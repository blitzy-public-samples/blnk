/*
	Copyright 2024 Blnk Finance Authors.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

// This file holds the RELAY half of the ledger event pipeline: the background
// processor that drains blnk.event_outbox to Kafka.
//
// The pipeline splits in two on purpose, and the split is what makes the whole thing
// reliable:
//
//	domain action ─┬─▶ ledger mutation  ┐
//	               └─▶ event outbox row ┴─ ONE database transaction (event_outbox.go)
//	                                        │
//	                        EventRelayProcessor claims the row, publishes it to Kafka,
//	                        marks it dispatched — and, during the dual-delivery window,
//	                        enqueues the legacy HTTP webhook from the SAME row.
//
// The capture half cannot publish, because a publish inside a ledger transaction would
// either hold the transaction open across a network round trip or lose the event when
// the transaction rolled back. The relay half cannot capture, because it runs on its own
// clock long after the mutation committed. This file is only ever the second half.
//
// # The guarantee, stated honestly
//
// The outbox gives EXACTLY-ONCE SEMANTICS ON THE WRITE SIDE: the mutation and its event
// commit together or not at all, and blnk.event_outbox.event_id is uniquely indexed, so
// one domain action can never record two events.
//
// DELIVERY REMAINS AT-LEAST-ONCE, and nothing in this file pretends otherwise. The relay
// publishes and then marks the row dispatched, and those are two operations against two
// systems with no transaction spanning them: a crash in between leaves a row that was
// published but not marked, and the next claim publishes it again. That window is
// deliberately not closed, because the alternative — marking first — would lose events
// instead of duplicating them, and a duplicate is recoverable at the subscriber while a
// loss is recoverable nowhere.
//
// The recovery mechanism for the subscriber is event_id: it is the message's idempotency
// key, unique per event and stable across redeliveries. docs/event-streaming.md states
// that obligation explicitly rather than implying end-to-end exactly-once.
//
// # Crash recovery
//
// A claim takes a LEASE (locked_until) rather than removing the row, so a relay that dies
// mid-batch strands nothing: once the lease expires the rows re-enter the claimable set
// and another instance — or the same one after a restart — picks them up. Every transition
// this file drives leaves the row in a state it can leave again, so there is no state from
// which a row becomes permanently unclaimable:
//
//   - processing with a live lease  → claimable again when the lease expires
//   - pending after a failed attempt → claimable again when next_attempt_at arrives
//   - dispatched / dead_lettered     → terminal, and terminal only after the event exists
//     somewhere durable (the category topic, or its `<topic>.dlt` sibling)
//
// # Retry is DURABLE, not a sleep
//
// The configured schedule (base 1s, doubling, capped at 30s, five attempts) is applied by
// persisting the next due instant, not by sleeping. One claim performs ONE publish
// attempt; a failed attempt records now + the computed delay in next_attempt_at and
// releases the lease, and the claim predicate refuses the row until it is due. See
// relayRetryPolicy and processRow for why the in-process alternative is not merely slower
// but incorrect.
package blnk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

const (
	// defaultEventRelayBatchSize is how many outbox rows one claim takes. It matches
	// LineageOutboxProcessor's batch size, and the two are the same number for the same
	// reason: a batch large enough to amortise the claim round trip, small enough that a
	// crash mid-batch leaves little to redo, and small enough that the whole batch
	// comfortably finishes inside the lease.
	defaultEventRelayBatchSize = 100

	// defaultEventRelayPollInterval is how often the relay looks for work when it has
	// none. It matches LineageOutboxProcessor. It is NOT the throughput limit — see
	// maxEventRelayBatchesPerTick, which is what lets one tick drain a backlog rather
	// than one batch per second.
	defaultEventRelayPollInterval = 1 * time.Second

	// defaultEventRelayLockDuration is the lease a claim takes on its rows, matching
	// LineageOutboxProcessor.
	//
	// It is the recovery latency after a crash: rows a dead relay was holding become
	// claimable again this long after it stopped. Thirty seconds is comfortably longer
	// than a batch takes (the writer's own produce timeout is 10s per attempt) and short
	// enough that a restart does not look like an outage.
	defaultEventRelayLockDuration = 30 * time.Second

	// defaultEventRelayConcurrency is how many partition-key groups of one batch are
	// published at once.
	//
	// Sequential publishing cannot meet the throughput requirement, and the arithmetic is
	// not close: a publish waits for every in-sync replica to acknowledge, so at a
	// realistic 10ms per acknowledgement one goroutine sustains about 100 events per
	// second, against a requirement of 500. Eight goroutines over the publisher's SHARED
	// writers reach it with headroom, which is exactly the usage kafka-go's own
	// documentation recommends — one writer, many callers — rather than more writers.
	//
	// ORDERING IS NOT AT RISK, and it is worth being precise about why, because
	// "concurrent publishing" and "ordered delivery" sound incompatible. Kafka orders
	// within a PARTITION, the partition is chosen by the message key, and the key is the
	// row's partition key. Two events that must stay ordered therefore share a key — and
	// this file never publishes two rows with the same key at the same time. See
	// groupEventRowsByPartitionKey for the two independent reasons that holds.
	defaultEventRelayConcurrency = 8

	// maxEventRelayBatchesPerTick bounds how many batches one tick chains, so a large
	// backlog drains promptly without a single tick starving the stop signal or the
	// ticker itself. Modelled on ChainProcessor's maxBatchesPerTick, which exists for the
	// same reason.
	//
	// Without chaining the relay's ceiling is batchSize per pollInterval — 100 events per
	// second at the defaults — regardless of how fast the broker is, and a backlog of a
	// million rows would take nearly three hours to clear no matter how idle the process
	// was. With it, the ceiling is what the broker and the concurrency limit allow, and
	// the tick becomes what it reads like: a poll for work, not a quota.
	maxEventRelayBatchesPerTick = 50

	// eventRelayBackoffMultiplier is the factor the retry delay grows by on each attempt.
	// Requirement R-4 fixes it at 2, so it is a constant rather than configuration: the
	// base delay, the cap and the attempt count are all tunable, the doubling is not.
	eventRelayBackoffMultiplier = 2

	// eventRelayBookkeepingTimeout bounds the bookkeeping transitions the relay performs
	// on a DETACHED context. See detachedBookkeepingContext for why they are detached at
	// all; this is what stops "finish what you owe" becoming "hang forever on a database
	// that has gone away during shutdown".
	eventRelayBookkeepingTimeout = 5 * time.Second

	// The two fallback delays below mirror config's relay defaults, and they are stated
	// here rather than read from it because the config package keeps its default block
	// unexported. That is a mirror of the REQUIREMENT rather than of an implementation
	// detail: R-4 fixes the base delay at one second and the cap at thirty, and
	// config.setRelayDefaults encodes the same two numbers. They are reached only by a
	// Configuration built directly, which is how the existing test suite constructs one —
	// the loader has always defaulted these before the relay sees them.
	//
	// The attempt-count fallback is NOT mirrored: config.MaxRelayRetryAttempts is exported
	// and is both the default and the ceiling, so the real value is used.
	fallbackRelayRetryBaseBackoff = 1000 * time.Millisecond
	fallbackRelayRetryMaxBackoff  = 30000 * time.Millisecond
)

// relayRetryPolicy is the bounded exponential backoff schedule of requirement R-4,
// resolved once from configuration into a value that can be reasoned about on its own.
//
// It is a plain value with a pure method rather than logic inlined in the relay, because
// the schedule is the part of this file most worth testing exactly — the mutation gate
// will not accept "roughly increasing" — and a pure function is testable without a
// database, a broker or a clock.
type relayRetryPolicy struct {
	// maxAttempts is the retry budget: how many publish attempts an event gets before it
	// is dead-lettered. It is informational HERE — the budget is enforced in SQL by
	// MarkEventFailed, which is what stops two relay instances both concluding they were
	// the last attempt — and it is carried so the relay can report "attempt 3 of 5" and
	// populate the request's MaxAttempts for the publisher's own telemetry.
	maxAttempts int

	// baseBackoff is the delay after the FIRST failed attempt, and the value every later
	// delay doubles from.
	baseBackoff time.Duration

	// maxBackoff caps the delay however many attempts have failed.
	maxBackoff time.Duration
}

// newRelayRetryPolicy resolves the schedule from the relay configuration block.
//
// Configuration has already defaulted and clamped these values (config.setRelayDefaults,
// bounded by config.MaxRelayRetryAttempts). The fallbacks here are therefore a second line
// of defence for a Configuration built directly — which is how the whole existing test
// suite constructs one — rather than a duplicate of that logic: a zero or negative value
// becomes the documented default instead of a schedule of zero-length delays, which would
// spend the entire retry budget in one poll interval and dead-letter an event during a
// broker restart that would have cleared on its own.
//
// An inverted window (base above cap) is not rejected. backoffFor applies the cap last, so
// a base of 60s against a cap of 30s yields 30s on every attempt: the operator gets the
// slower schedule they asked for, bounded as configured, rather than a refusal to start.
//
// Parameters:
//   - cfg config.RelayConfig: the resolved relay configuration block.
//
// Returns:
//   - relayRetryPolicy: the schedule, with every value positive.
func newRelayRetryPolicy(cfg config.RelayConfig) relayRetryPolicy {
	policy := relayRetryPolicy{
		maxAttempts: cfg.MaxRetryAttempts,
		baseBackoff: time.Duration(cfg.RetryBaseBackoffMS) * time.Millisecond,
		maxBackoff:  time.Duration(cfg.RetryMaxBackoffMS) * time.Millisecond,
	}

	if policy.maxAttempts < 1 {
		policy.maxAttempts = config.MaxRelayRetryAttempts
	}
	if policy.baseBackoff <= 0 {
		policy.baseBackoff = fallbackRelayRetryBaseBackoff
	}
	if policy.maxBackoff <= 0 {
		policy.maxBackoff = fallbackRelayRetryMaxBackoff
	}

	return policy
}

// backoffFor returns the delay to wait before the attempt AFTER the given one.
//
// attempt is 1-based and names the attempt that just failed, so backoffFor(1) is the
// pause after the first failure. With the configured defaults — base 1s, multiplier 2,
// cap 30s, five attempts — the schedule is exactly:
//
//	attempt 1 → 1s    attempt 2 → 2s    attempt 3 → 4s
//	attempt 4 → 8s    attempt 5 → 16s
//
// THE 30-SECOND CAP IS NEVER REACHED WITHIN FIVE ATTEMPTS, and that is correct rather
// than a bug to fix. The cap bounds a schedule whose configured base or attempt count
// would otherwise exceed it; with the mandated parameters the sixth delay would be the
// first to be capped, and there is no sixth attempt. Neither the multiplier nor the cap
// may be "corrected" to make the cap engage.
//
// Growth is computed by repeated doubling with an early return at the cap rather than by
// exponentiation, so there is no overflow to reason about: the value can never double
// past the cap, let alone past the range of a Duration, however large an attempt number
// arrives from a row whose max_attempts was raised directly in the database.
//
// Parameters:
//   - attempt int: the 1-based number of the attempt that failed. Values below 1 are
//     treated as 1, because there is no delay before the first attempt to describe and
//     silently returning zero would collapse the schedule.
//
// Returns:
//   - time.Duration: the delay, always positive and never above maxBackoff.
func (p relayRetryPolicy) backoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := p.baseBackoff
	for i := 1; i < attempt; i++ {
		if delay >= p.maxBackoff {
			return p.maxBackoff
		}

		delay *= eventRelayBackoffMultiplier
	}

	if delay > p.maxBackoff {
		return p.maxBackoff
	}

	return delay
}

// eventRelayStore is the repository surface the relay needs, and deliberately no more of
// it.
//
// Depending on a four-method interface rather than on the whole ten-sub-interface
// IDataSource is what makes every path in this file testable without a database, and it
// documents the blast radius precisely: the relay claims rows and drives them to a
// terminal state. It cannot insert an event (that belongs inside the ledger transaction,
// in event_outbox.go), cannot read the dead-letter inventory, and cannot replay — so it
// cannot accidentally take part in anybody else's job.
//
// MarkEventDeadLettered is POINTEDLY ABSENT. Recording a dead-letter is only legitimate
// once the event is actually on its `<topic>.dlt` sibling, and only the dead-letter writer
// knows whether it got there; the relay hands off instead. See eventRelayDeadLetterer.
type eventRelayStore interface {
	// ClaimPendingEventOutbox claims a batch, oldest occurrence first, taking a lease and
	// stamping every row with a claim token. It returns at most one row per partition key
	// across all concurrent relay instances, which is half of the ordering guarantee.
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	// MarkEventDispatched moves a claimed row to its success terminal state. It is
	// conditional on the claim token and CLEARS it, so it must be the last transition the
	// relay performs on a row.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string) error

	// MarkEventFailed records one failed attempt, schedules the row's next due instant
	// from retryAfter, and reports — decided in SQL, so two instances cannot both conclude
	// they were last — whether the budget is now spent.
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration) (model.EventFailureOutcome, error)

	// MarkWebhookDispatched records the legacy leg of the dual-delivery window. It is
	// conditional on the claim token, so it must run BEFORE MarkEventDispatched clears it.
	// This method disappears at the webhook sunset along with the branch that calls it.
	MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error
}

// eventRelayDeadLetterer is the hand-off the relay makes when a row has spent its retry
// budget.
//
// It is one method wide because that is the whole of the relay's involvement in
// dead-lettering: the relay decides that an event is FINISHED, and the dead-letter writer
// decides everything else — which `<topic>.dlt` sibling it goes to, what the failure
// metadata says, how the message is composed so the original bytes stay recoverable, and
// when the row may be recorded as dead-lettered. None of that is reimplemented here, and
// the interface is narrow precisely so it cannot be.
type eventRelayDeadLetterer interface {
	// DeadLetter writes the exhausted row to its dead-letter topic with failure metadata
	// attached and records the terminal state. It requires the row to carry the claim
	// token MarkEventFailed retained on its exhaustion arm.
	DeadLetter(ctx context.Context, row model.EventOutbox, cause error) (DeadLetterOutcome, error)
}

// eventRelayLegacyTransport is the legacy HTTP webhook leg of the dual-delivery window.
//
// SUNSET NOTICE — this interface and every use of it are DELETED at the webhook sunset.
// See deliverLegacyWebhook for the full description of what goes and what must stay.
type eventRelayLegacyTransport interface {
	// EnqueueLegacyWebhookDelivery enqueues the legacy delivery of one event, carrying the
	// stored bytes VERBATIM and using the event id as the task identity so a re-claim
	// cannot double-enqueue.
	EnqueueLegacyWebhookDelivery(eventID string, body []byte) error
}

// Compile-time proofs that the three seams are faithful subsets of the real types. Each
// fails the build here, on the line that states the contract, rather than at a call site.
var (
	_ eventRelayStore           = (database.IDataSource)(nil)
	_ eventRelayDeadLetterer    = (*EventDeadLetterService)(nil)
	_ eventRelayLegacyTransport = (*Blnk)(nil)
)

// EventRelayProcessor drains the transactional event outbox to Kafka.
//
// It polls blnk.event_outbox for rows that are due, publishes each one to its category
// topic, marks it dispatched, dead-letters it when its retry budget is spent, and — until
// the webhook sunset — additionally enqueues the legacy HTTP delivery from the same row.
//
// # It is LineageOutboxProcessor's shape, deliberately
//
// The struct fields, the defaults, the fluent configurators and the whole Start / Stop /
// IsRunning / run / processBatch lifecycle are the ones LineageOutboxProcessor
// established and ChainProcessor already reuses — read the three side by side and they are
// recognisably the same processor. Blnk operates two transactional outboxes over two
// separate tables served by two separate relays, and they are not merged; what they share
// is the pattern, not the state.
//
// Two additions earn their place, and both are documented where they are declared:
// concurrency across partition-key groups, without which the throughput requirement is
// arithmetically unreachable, and collaborator fields resolved once at construction so
// that the hot path never rebuilds a publisher or a dead-letter service.
//
// One instance per process. It is safe for concurrent use — the lifecycle state is
// mutex-guarded and the collaborators are read-only after construction — and several
// instances across several processes are safe too, which is the point of claiming with
// FOR UPDATE SKIP LOCKED.
type EventRelayProcessor struct {
	// blnk is the service container. It is held for the same reason
	// LineageOutboxProcessor holds it — configuration and the datasource — and the
	// collaborators below are derived from it once at construction.
	blnk *Blnk

	batchSize    int
	pollInterval time.Duration
	lockDuration time.Duration

	// concurrency is how many partition-key groups of one batch publish at once. See
	// defaultEventRelayConcurrency.
	concurrency int

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex

	// store is the outbox repository, narrowed to the four methods the relay drives.
	store eventRelayStore

	// publisher is the SHARED Kafka publisher this process built at start-up. The relay is
	// the only production caller of a kafka.Writer, and it borrows the process publisher
	// rather than building one so that every write reuses one connection pool and one SASL
	// session per broker.
	publisher TopicEventPublisher

	// deadLetters is built ONCE with the publisher above and kept for the relay's
	// lifetime, which is what the dead-letter service's own documentation requires of a
	// caller in a loop: the alternative resolves a short-lived publisher per call, which is
	// right for an operator-triggered replay and wrong here.
	deadLetters eventRelayDeadLetterer

	// legacy is the dual-delivery leg. Nil after the sunset, when the whole branch goes.
	legacy eventRelayLegacyTransport

	// retry is the resolved backoff schedule.
	retry relayRetryPolicy

	// sunsetPassed is THE sunset decision, and it is a function field only so a test can
	// pin the clock. In production it is event_sunset.go's WebhookSunsetPassed and nothing
	// else: this file never parses the configured date and never compares instants itself.
	// Two comparisons could disagree, and the pair that would then disagree is "stop
	// dual-writing" and "answer 410 Gone" — a system that had done one but not the other.
	sunsetPassed func(now time.Time) bool

	// now is the clock, replaceable in-package so the sunset boundary is exact in tests.
	// It follows EventDeadLetterService's now field.
	now func() time.Time
}

// NewEventRelayProcessor creates the event outbox relay for a Blnk instance.
//
// Everything the relay needs is derived from the instance here, once: the datasource, the
// process-wide Kafka publisher, a dead-letter service bound to both, the legacy webhook
// transport, and the retry schedule from configuration. That is what makes the start-up
// call site in the server role three lines — construct, start, defer stop — with no
// additional wiring and nothing for a caller to remember to pass.
//
// It performs NO I/O and never fails. A nil instance, a missing datasource or a publisher
// that is absent or is the no-op yields a processor that refuses to start and says why,
// rather than a constructor error the call site would have to handle. See Start.
//
// Parameters:
//   - blnk *Blnk: the service container. May be nil.
//
// Returns:
//   - *EventRelayProcessor: the configured processor, with the house defaults — batch size
//     100, poll interval 1 second, lock duration 30 seconds.
func NewEventRelayProcessor(blnk *Blnk) *EventRelayProcessor {
	processor := &EventRelayProcessor{
		blnk:         blnk,
		batchSize:    defaultEventRelayBatchSize,
		pollInterval: defaultEventRelayPollInterval,
		lockDuration: defaultEventRelayLockDuration,
		concurrency:  defaultEventRelayConcurrency,
		stopCh:       make(chan struct{}),
		sunsetPassed: WebhookSunsetPassed,
		now:          time.Now,
	}

	if blnk == nil {
		// Still usable as a value: Start refuses, Stop is a no-op, IsRunning is false.
		// NewBlnk(nil) is a supported construction in this codebase, so a processor built
		// from a bare instance must fail legibly rather than panic on a ledger process.
		processor.retry = newRelayRetryPolicy(config.RelayConfig{})

		return processor
	}

	processor.retry = newRelayRetryPolicy(blnk.Config().Relay)
	processor.legacy = blnk

	if blnk.datasource != nil {
		processor.store = blnk.datasource
	}

	// The publisher is adopted only if it can be given a DESTINATION TOPIC. A stored row
	// records the topic it was always meant for, and the minimal EventPublisher contract
	// cannot express one — so a publisher that only satisfies it could not honour the
	// row's own routing. Both implementations in event_publisher.go satisfy the fuller
	// contract, so this narrowing rejects nothing real; a nil field fails the assertion,
	// which is what routes it to Start's refusal.
	if publisher, ok := blnk.events.(TopicEventPublisher); ok {
		processor.publisher = publisher

		// One service, built here, kept for the relay's lifetime, sharing the relay's
		// publisher. It is NOT closed by the relay: the publisher belongs to the Blnk
		// instance, whose Close releases it, and a service handed a publisher explicitly
		// does not own it.
		processor.deadLetters = NewEventDeadLetterService(blnk.datasource, publisher)
	}

	return processor
}

// WithBatchSize sets how many outbox rows one claim takes.
//
// A non-positive size is ignored with a warning rather than applied. The repository
// rejects a non-positive batch outright, so applying one would turn every poll into a
// logged error and publish nothing at all — a relay that looks alive and delivers nothing
// is the one failure mode worth guarding against explicitly.
//
// Parameters:
//   - size int: the number of rows to claim per batch.
//
// Returns:
//   - *EventRelayProcessor: the processor, for chaining.
func (p *EventRelayProcessor) WithBatchSize(size int) *EventRelayProcessor {
	if size <= 0 {
		logrus.WithField("requested_batch_size", size).
			Warn("event relay: ignoring a non-positive batch size; keeping the configured value")

		return p
	}

	p.batchSize = size

	return p
}

// WithPollInterval sets how often the relay looks for work.
//
// A non-positive interval is ignored with a warning: time.NewTicker panics on one, and a
// panic in a background loop takes the whole process down.
//
// Parameters:
//   - interval time.Duration: the poll interval.
//
// Returns:
//   - *EventRelayProcessor: the processor, for chaining.
func (p *EventRelayProcessor) WithPollInterval(interval time.Duration) *EventRelayProcessor {
	if interval <= 0 {
		logrus.WithField("requested_poll_interval", interval.String()).
			Warn("event relay: ignoring a non-positive poll interval; keeping the configured value")

		return p
	}

	p.pollInterval = interval

	return p
}

// WithLockDuration sets the lease a claim takes on its rows.
//
// A non-positive duration is ignored with a warning. An expired-on-arrival lease is a
// correctness problem rather than a tuning mistake: a second relay instance could reclaim
// and republish a row this one is still publishing.
//
// Parameters:
//   - duration time.Duration: the lease duration.
//
// Returns:
//   - *EventRelayProcessor: the processor, for chaining.
func (p *EventRelayProcessor) WithLockDuration(duration time.Duration) *EventRelayProcessor {
	if duration <= 0 {
		logrus.WithField("requested_lock_duration", duration.String()).
			Warn("event relay: ignoring a non-positive lock duration; keeping the configured value")

		return p
	}

	p.lockDuration = duration

	return p
}

// WithConcurrency sets how many partition-key groups of one batch publish at once.
//
// One means fully sequential, which is a legitimate configuration — it is the right choice
// for a single-broker development stack — and anything above one is bounded by the
// semaphore in processBatch. A non-positive value is ignored with a warning, because zero
// would acquire a permit that never exists and stall the relay silently.
//
// Parameters:
//   - workers int: the maximum number of concurrent publishes.
//
// Returns:
//   - *EventRelayProcessor: the processor, for chaining.
func (p *EventRelayProcessor) WithConcurrency(workers int) *EventRelayProcessor {
	if workers <= 0 {
		logrus.WithField("requested_concurrency", workers).
			Warn("event relay: ignoring a non-positive concurrency; keeping the configured value")

		return p
	}

	p.concurrency = workers

	return p
}

// startupObstacle reports why the relay cannot run, or nil when it can.
//
// It exists because the ONE thing this processor must never do is drain the outbox
// without publishing anything. Two of the three conditions below would do exactly that:
//
//   - NO PUBLISHER AT ALL, or one that cannot be given a destination topic. Every publish
//     would fail, every event would burn its retry budget against a transport that does
//     not exist, and a whole backlog would be dead-lettered by a deployment that was
//     simply misconfigured.
//   - THE NO-OP PUBLISHER, which is far worse and is the reason this check is not merely
//     defensive. The no-op reports every publish as dispatched, so the relay would mark
//     every row dispatched having sent nothing at all: an entire outbox silently retired
//     with no error, no dead letter and nothing left to replay from. The no-op is a
//     legitimate steady state for the PRODUCER side — a deployment with no brokers
//     captures events and publishes none, exactly as SendWebhook no-ops without a URL —
//     and it is never a legitimate transport for the relay. The publisher's own
//     documentation states that this path is unreachable from the relay; this is what
//     makes that true.
//   - NO DATASOURCE, which cannot claim and so would log an error every poll forever.
//
// Refusing in Start rather than returning an error from the constructor is what keeps the
// server-role call site three lines: the caller may start the relay unconditionally and
// this decides, so the conditional in cmd/server.go is a second line of defence rather
// than the only one.
//
// Returns:
//   - error: the reason the relay must not run, or nil.
func (p *EventRelayProcessor) startupObstacle() error {
	if p == nil {
		return errors.New("the event relay processor is nil")
	}

	if p.blnk == nil {
		return errors.New("the event relay has no Blnk instance")
	}

	if p.store == nil {
		return errors.New("the event relay has no datasource, so it cannot claim outbox rows")
	}

	if p.publisher == nil {
		return errors.New(
			"the event relay has no Kafka publisher that accepts a destination topic, so no outbox " +
				"row could be published to the topic it recorded",
		)
	}

	if IsNoopEventPublisher(p.publisher) {
		return errors.New(
			"the event relay was given the no-op event publisher, which reports every publish as " +
				"dispatched without sending anything; running it would mark the whole outbox " +
				"dispatched and lose every event. Configure KAFKA_BROKERS, or do not start the relay",
		)
	}

	if p.deadLetters == nil {
		return errors.New(
			"the event relay has no dead-letter service, so an event that exhausted its retry " +
				"budget could not be preserved on its dead-letter topic",
		)
	}

	return nil
}

// Start begins draining the event outbox in the background.
//
// The processor polls for due rows at the configured interval until the context is
// cancelled or Stop is called. It is idempotent: a second Start while running is ignored
// rather than spawning a second loop, and stopCh is RE-CREATED here, which is what makes
// Start after Stop work rather than returning immediately on a channel that is already
// closed.
//
// It refuses to start when it could not publish — see startupObstacle — logging the
// reason. IsRunning then stays false and Stop remains safe, so an unconditional call site
// is correct.
//
// Parameters:
//   - ctx context.Context: the context for the operation. When cancelled, processing
//     stops; rows already claimed keep their lease and become claimable again when it
//     expires.
func (p *EventRelayProcessor) Start(ctx context.Context) {
	if err := p.startupObstacle(); err != nil {
		logrus.WithError(err).Error("Event outbox relay not started")

		return
	}

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()

		return
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(ctx)
	}()

	logrus.WithFields(logrus.Fields{
		"batch_size":    p.batchSize,
		"poll_interval": p.pollInterval.String(),
		"lock_duration": p.lockDuration.String(),
		"concurrency":   p.concurrency,
		"max_attempts":  p.retry.maxAttempts,
		"base_backoff":  p.retry.baseBackoff.String(),
		"max_backoff":   p.retry.maxBackoff.String(),
	}).Info("Event outbox relay started")
}

// Stop gracefully stops the relay.
//
// It signals the loop to exit and WAITS for the batch in flight to finish, so a shutdown
// does not abandon rows mid-publish. Calling it when the relay is not running — including
// before it was ever started, and after a refused Start — is safe and does nothing.
func (p *EventRelayProcessor) Stop() {
	if p == nil {
		return
	}

	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()

		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	logrus.Info("Event outbox relay stopped")
}

// IsRunning returns whether the relay's loop is active.
//
// Returns:
//   - bool: true when the loop is running.
func (p *EventRelayProcessor) IsRunning() bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.running
}

// run is the main processing loop: LineageOutboxProcessor's ticker and select, with an
// informative line on each exit path so a stopped relay always says why it stopped.
//
// Parameters:
//   - ctx context.Context: cancelling it ends the loop.
func (p *EventRelayProcessor) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Event outbox relay context cancelled")

			return
		case <-p.stopCh:
			logrus.Info("Event outbox relay stop signal received")

			return
		case <-ticker.C:
			p.processTick(ctx)
		}
	}
}

// processTick drains as many batches as are ready, bounded by maxEventRelayBatchesPerTick.
//
// It is the one structural addition to LineageOutboxProcessor's loop, and it is
// ChainProcessor's processTick rather than an invention: chain as much as is ready, stop
// as soon as a batch comes back short, and let the next tick continue.
//
// The bound matters for both directions. Without chaining the relay's ceiling is one batch
// per poll interval whatever the broker can take, which is 100 events per second at the
// defaults against a requirement of 500, and a backlog would drain at that rate no matter
// how idle the process was. Without the bound a large backfill would keep one tick
// running indefinitely, starving the ticker and delaying the stop signal.
//
// Between batches it re-checks cancellation and the stop signal, so a shutdown is honoured
// promptly instead of after up to fifty more batches.
//
// Parameters:
//   - ctx context.Context: cancelling it abandons the remaining batches; claimed rows keep
//     their lease and become claimable again when it expires.
func (p *EventRelayProcessor) processTick(ctx context.Context) {
	for chained := 0; chained < maxEventRelayBatchesPerTick; chained++ {
		if !p.shouldClaimAnotherBatch(ctx) {
			return
		}

		claimed := p.processBatch(ctx)
		if claimed < p.batchSize {
			// A short batch means the claimable set is drained — or that the rows left are
			// not due yet, which is the same thing for this tick.
			return
		}
	}
}

// shouldClaimAnotherBatch reports whether the relay may CLAIM MORE WORK.
//
// It answers no to either shutdown signal, because claiming a new batch while stopping is
// pointless in both cases: the rows would be leased and then left.
//
// The select is non-blocking on purpose: it asks "has anything asked me to stop?" without
// waiting for anything to.
//
// Parameters:
//   - ctx context.Context: the loop context.
//
// Returns:
//   - bool: false when the context is done or Stop has been called.
func (p *EventRelayProcessor) shouldClaimAnotherBatch(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	default:
		return true
	}
}

// publishingMayProceed reports whether the relay may keep publishing rows it has ALREADY
// CLAIMED. It consults the CONTEXT ONLY, and the asymmetry with
// shouldClaimAnotherBatch is deliberate.
//
// The two shutdown signals mean different things, and treating them alike gets one of them
// wrong:
//
//   - Stop is a GRACEFUL DRAIN. Its documented promise is to wait for work in flight, and a
//     batch this relay has already leased is exactly that. Abandoning it would strand those
//     events until their lease expired — thirty seconds of avoidable delivery latency, paid
//     on every deploy — so Stop stops the relay claiming more and lets the claimed batch
//     finish. The wait it costs is bounded by the batch, and the batch is bounded by the
//     writer's own produce timeout.
//   - Context cancellation is an ABORT. The process is going away, so starting another
//     publish is not something to insist on: the rows keep their lease and the next
//     instance picks them up, which is the same mechanism that recovers a crash.
//
// Parameters:
//   - ctx context.Context: the batch context.
//
// Returns:
//   - bool: false once the context is done.
func publishingMayProceed(ctx context.Context) bool {
	return ctx.Err() == nil
}

// processBatch claims one batch of due outbox rows and publishes every one of them.
//
// The claim is FIFO by occurrence — the repository orders by occurred_at ascending, and
// re-sorts the rows the UPDATE returned because `UPDATE … RETURNING` does not preserve the
// inner ORDER BY — and it takes a lease plus one claim token for the batch. Nothing is
// deleted and nothing is read outside that claim: FOR UPDATE SKIP LOCKED is what lets
// several relay instances work disjoint batches without blocking each other, and no
// additional lock is taken here, because an external lock would serialise the very
// instances that exist to run in parallel.
//
// The dual-delivery decision is taken ONCE per batch rather than per row. It comes from
// event_sunset.go either way; taking it once means a batch of five hundred events costs one
// decision instead of five hundred, and the sub-second imprecision that introduces at the
// boundary of a thirty-day window is not a distinction anything can observe.
//
// Parameters:
//   - ctx context.Context: cancels the claim and, between rows, the rest of the batch.
//
// Returns:
//   - int: how many rows were claimed, which is what lets processTick decide whether the
//     backlog is drained. Zero on a claim error, so a failing database cannot make a tick
//     spin through fifty empty batches.
func (p *EventRelayProcessor) processBatch(ctx context.Context) int {
	rows, err := p.store.ClaimPendingEventOutbox(ctx, p.batchSize, p.lockDuration)
	if err != nil {
		logrus.WithError(err).Error("failed to claim event outbox entries")

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	// The claim instant, captured from the relay's own clock rather than read back from the
	// row's last_attempted_at. The publish-duration histogram is documented as measuring
	// claim to broker acknowledgement, and a duration must not be computed across two
	// clocks: subtracting a database timestamp from a local one reports the skew between
	// them as latency.
	claimedAt := p.now()
	dualDelivery := !p.sunsetPassed(claimedAt)

	logrus.WithFields(logrus.Fields{
		"claimed":       len(rows),
		"dual_delivery": dualDelivery,
	}).Infof("Processing %d event outbox entries", len(rows))

	groups := groupEventRowsByPartitionKey(rows)
	permits := semaphore.NewWeighted(int64(p.concurrency))

	var batch sync.WaitGroup

	for _, group := range groups {
		// Checked before EVERY group, not only at the top of the batch, so an ABORT stops
		// dispatching work that has not started yet. The rows left behind keep their lease
		// and re-enter the claimable set when it expires, which is the same mechanism that
		// recovers a crashed relay. A graceful Stop deliberately does NOT stop here — see
		// publishingMayProceed for why the two signals are treated differently.
		//
		// The semaphore also fails an acquisition on a cancelled context, but only as a
		// race against a permit becoming free at the same instant; this check is what makes
		// the behaviour deterministic rather than a coin flip.
		if !publishingMayProceed(ctx) {
			logrus.WithField("event_id", group[0].EventID).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		// Acquire before spawning, so the number of in-flight publishes is bounded by the
		// permit count rather than by the number of groups.
		if acquireErr := permits.Acquire(ctx, 1); acquireErr != nil {
			logrus.WithError(acquireErr).WithField("event_id", group[0].EventID).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		batch.Add(1)

		go func(rows []model.EventOutbox) {
			defer batch.Done()
			defer permits.Release(1)

			// SEQUENTIALLY WITHIN THE GROUP. Every row here shares one partition key, so
			// this loop is the per-aggregate ordering guarantee in code.
			for _, row := range rows {
				// Cancellation is honoured BETWEEN rows as well as between groups. A group
				// is normally one row — the claim returns at most one per partition key — but
				// it is not bounded to one, and a group that kept publishing through an abort
				// would be the one place cancellation did not reach.
				if !publishingMayProceed(ctx) {
					return
				}

				p.processRow(ctx, row, claimedAt, dualDelivery)
			}
		}(group)
	}

	// Waiting here is what makes Stop's promise true: the loop goroutine cannot return —
	// and so p.wg cannot drain — until the batch in flight has finished.
	batch.Wait()

	return len(rows)
}

// groupEventRowsByPartitionKey partitions a claimed batch into per-partition-key groups,
// preserving the claim's order both within each group and between groups.
//
// # Why grouping is what makes concurrency safe
//
// Kafka orders messages within a PARTITION only, the partition is chosen from the message
// key, and the key is the row's partition key. Two events whose relative order matters
// therefore share a partition key — so ordering survives concurrency exactly as long as no
// two rows with the same key are ever published at the same time, or out of occurrence
// order.
//
// That holds here for TWO INDEPENDENT REASONS, and the redundancy is deliberate:
//
//  1. The claim already guarantees it. ClaimPendingEventOutbox returns at most one row per
//     partition key — across every concurrent relay instance, not just this one — because a
//     candidate is claimable only when no earlier row sharing its key is still pending or
//     processing. So in practice every group returned here has exactly one row.
//  2. This function guarantees it locally. If a future claim ever returned two rows sharing
//     a key, they would land in the same group and be published in claim order by one
//     goroutine, rather than racing in two.
//
// Relying on the first alone would put the ordering guarantee in a SQL predicate in another
// package, where a well-intentioned change to that query could break message ordering with
// nothing in this file to show it. Rows are never round-robined across workers, which is
// the one arrangement that would break ordering outright.
//
// Parameters:
//   - rows []model.EventOutbox: the claimed batch, in occurrence order.
//
// Returns:
//   - [][]model.EventOutbox: one slice per distinct partition key, each in claim order,
//     ordered by the first appearance of the key so the overall FIFO shape is kept.
func groupEventRowsByPartitionKey(rows []model.EventOutbox) [][]model.EventOutbox {
	groups := make([][]model.EventOutbox, 0, len(rows))
	indexByKey := make(map[string]int, len(rows))

	for _, row := range rows {
		// A blank key would collapse every unkeyed row into one group and serialise them.
		// It cannot occur on a persisted row — the capture path guarantees a value through
		// a documented fallback chain — so the row id is used to keep such a row in a group
		// of its own rather than inventing a shared bucket for a case that means the data
		// is already wrong.
		key := row.PartitionKey
		if key == "" {
			key = fmt.Sprintf("id:%d", row.ID)
		}

		if index, seen := indexByKey[key]; seen {
			groups[index] = append(groups[index], row)

			continue
		}

		indexByKey[key] = len(groups)
		groups = append(groups, []model.EventOutbox{row})
	}

	return groups
}

// processRow performs ONE publish attempt for one claimed row and records the outcome.
//
// # One attempt per claim, and why the retry is not a loop here
//
// There is no retry loop in this function, and that is the design rather than an omission.
// A failed attempt records now + the computed backoff in the row's next_attempt_at and
// releases the lease; the claim predicate then refuses the row until it is due, so the
// NEXT attempt is a later claim — by this instance or another.
//
// Sleeping the delay in process instead would be worse than having no backoff at all. The
// configured schedule sums to 31 seconds, which outlives the 30-second lease, so a second
// instance would reclaim the row mid-sleep and publish it twice; and the sleeping goroutine
// would hold a permit while doing nothing. Persisting the decision applies it to every
// instance at once and costs nothing while it waits.
//
// # The order of the three steps
//
// The legacy webhook leg goes FIRST, and both reasons matter. It needs the claim token,
// which MarkEventDispatched deliberately clears as it moves the row to its terminal state.
// And it must not be conditional on the Kafka publish succeeding: during the dual-delivery
// window the legacy transport is still the one existing subscribers are consuming, so
// suppressing it during a broker outage would turn a Kafka problem into an outage for
// subscribers who have not migrated yet — the exact failure the window exists to prevent.
//
// Parameters:
//   - ctx context.Context: cancels the publish. Bookkeeping the relay already owes is
//     completed on a detached context — see detachedBookkeepingContext.
//   - row model.EventOutbox: the claimed row, carrying its claim token.
//   - claimedAt time.Time: when the batch was claimed, for the publish-duration histogram.
//   - dualDelivery bool: whether the webhook sunset is still in the future.
func (p *EventRelayProcessor) processRow(
	ctx context.Context,
	row model.EventOutbox,
	claimedAt time.Time,
	dualDelivery bool,
) {
	// The attempt this publish represents. attempts counts the failures RECORDED so far —
	// the claim does not increment it, MarkEventFailed does — so the attempt now under way
	// is one past it. Getting this wrong would misreport the backoff schedule and the
	// attempt metric label together.
	attempt := row.Attempts + 1

	if dualDelivery {
		p.deliverLegacyWebhook(ctx, row)
	}

	result, err := p.publisher.PublishToTopic(ctx, PublishRequest{
		Event: model.LedgerEvent{
			EventID:       row.EventID,
			EventType:     row.EventType,
			AggregateID:   row.AggregateID,
			OccurredAt:    row.OccurredAt,
			Payload:       row.Payload,
			SchemaVersion: row.SchemaVersion,
		},
		// The topic and the key come from the ROW, through the publisher's own resolver, so
		// a stored row is published to the destination it recorded even if the topic-naming
		// configuration has changed since, and the key on the wire is the same value the
		// claim serialised dispatch on.
		Topic:   row.Topic,
		Key:     resolvePartitionKey(PublishRequestFromOutbox(row, attempt)),
		Attempt: attempt,
		// The budget is passed so the publisher can tell a failure that still has attempts
		// left from the one that spent the last of them, and so its log line reads
		// "attempt 3 of 5".
		MaxAttempts: p.rowMaxAttempts(row),
		Purpose:     PublishPurposeOriginal,
		ClaimedAt:   claimedAt,
	})

	p.logAttempt(row, attempt, result, err)

	if err != nil {
		p.recordFailedAttempt(ctx, row, attempt, err)

		return
	}

	// AT-LEAST-ONCE, EXPLICITLY. The broker has the event; the row does not yet say so. A
	// crash here — or a failure of this single statement — leaves the row claimable once its
	// lease expires, and the event is published a second time. That duplicate is suppressed
	// at the subscriber on event_id, which is unique per event and is the documented
	// idempotency key. The reverse order would lose events instead, which nothing can
	// recover.
	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if markErr := p.store.MarkEventDispatched(bookkeeping, row.ID, row.ClaimToken); markErr != nil {
		logrus.WithFields(p.rowFields(row, attempt)).WithError(markErr).Error(
			"event relay: the event was published but its outbox row could not be marked dispatched; " +
				"it will be republished when its lease expires and must be suppressed on event_id",
		)
	}
}

// rowMaxAttempts returns the retry budget in force for a row.
//
// The ROW's own budget wins, because it is what MarkEventFailed's in-SQL exhaustion
// decision is measured against — reporting the configured value while the database enforced
// a different one would make the logs and the metric label disagree with the actual
// outcome. The configured value is the fallback for a row that states none.
//
// Parameters:
//   - row model.EventOutbox: the claimed row.
//
// Returns:
//   - int: the budget, at least 1.
func (p *EventRelayProcessor) rowMaxAttempts(row model.EventOutbox) int {
	if row.MaxAttempts > 0 {
		return row.MaxAttempts
	}

	if p.retry.maxAttempts > 0 {
		return p.retry.maxAttempts
	}

	return 1
}

// recordFailedAttempt records one failed publish attempt, schedules the retry, and hands
// off to the dead-letter path when the budget is spent.
//
// The retry-versus-exhaustion decision is NOT taken here. MarkEventFailed takes it inside
// its UPDATE and reports it back, which is what stops two relay instances racing on one row
// from both concluding they were the last attempt — a race that would put two copies of the
// event on the dead-letter topic. This function supplies the delay and acts on the answer.
//
// A row whose claim was lost is abandoned rather than retried: another instance owns it now,
// and touching it further would overwrite state that is no longer ours. A row whose
// bookkeeping simply failed keeps its lease and returns to the claimable set when it
// expires, so nothing is dropped in either case.
//
// Parameters:
//   - ctx context.Context: used for the dead-letter publish. The bookkeeping transition
//     runs on a detached context.
//   - row model.EventOutbox: the claimed row.
//   - attempt int: the 1-based attempt that just failed.
//   - cause error: the publish failure.
func (p *EventRelayProcessor) recordFailedAttempt(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
) {
	retryAfter := p.retry.backoffFor(attempt)
	reason := relayFailureReason(cause)

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	outcome, err := p.store.MarkEventFailed(bookkeeping, row.ID, row.ClaimToken, reason, retryAfter)
	if err != nil {
		logrus.WithFields(p.rowFields(row, attempt)).WithError(err).Error(
			"event relay: recording a failed publish attempt failed; the row keeps its lease and " +
				"becomes claimable again when it expires",
		)

		return
	}

	if !outcome.Exhausted {
		logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
			"attempts":       outcome.Attempts,
			"retry_after":    retryAfter.String(),
			"next_attempt":   p.now().UTC().Add(retryAfter).Format(time.RFC3339),
			"error":          reason,
			"row_status":     outcome.Status,
			"max_attempts":   p.rowMaxAttempts(row),
			"backoff_capped": retryAfter >= p.retry.maxBackoff,
		}).Warn("event relay: publish failed and the event is scheduled for another attempt")

		return
	}

	// The budget is spent. The row is already failed, and the claim token MarkEventFailed
	// RETAINED on that arm travels on the row so the dead-letter write and the transition
	// that records it stay the exclusive property of the worker that spent the last
	// attempt — which is what stops two workers writing the same event to the dead-letter
	// topic. The attempt count and status are carried across too, so the failure metadata
	// reports the number the database actually recorded.
	row.ClaimToken = outcome.ClaimToken
	row.Attempts = outcome.Attempts
	row.Status = outcome.Status

	p.deadLetter(ctx, row, attempt, cause)
}

// deadLetter hands an exhausted row to the dead-letter writer.
//
// Nothing about dead-lettering is implemented here: the destination `<topic>.dlt`, the
// failure metadata, the additive message composition that keeps the original bytes
// recoverable, and the transition that records the terminal state all belong to
// event_dlt.go. This function exists so the relay's exhaustion branch reads as one call and
// so a failure to preserve the event is reported at the right severity.
//
// A failed hand-off leaves the row in the failed state, which the dead-letter inventory
// covers deliberately: the event stays visible to an operator instead of being reported as
// safely dead-lettered when its message never left the process.
//
// Parameters:
//   - ctx context.Context: cancels the dead-letter publish.
//   - row model.EventOutbox: the exhausted row, carrying the retained claim token.
//   - attempt int: the attempt that exhausted the budget, for the log line.
//   - cause error: the final publish failure.
func (p *EventRelayProcessor) deadLetter(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
) {
	if p.deadLetters == nil {
		// Unreachable: Start refuses without a dead-letter service. Logged rather than
		// dereferenced, because the alternative is a nil panic in a ledger process.
		logrus.WithFields(p.rowFields(row, attempt)).Error(
			"event relay: the retry budget is spent but no dead-letter service is configured; " +
				"the row stays failed and remains in the dead-letter inventory",
		)

		return
	}

	outcome, err := p.deadLetters.DeadLetter(ctx, row, cause)
	if err != nil {
		logrus.WithFields(p.rowFields(row, attempt)).WithError(err).Error(
			"event relay: the event exhausted its retry budget but could not be written to its " +
				"dead-letter topic; the row stays failed and remains in the dead-letter inventory",
		)

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"dlt_topic": outcome.DeadLetterTopic,
		"attempts":  outcome.Metadata.AttemptCount,
		"error":     relayFailureReason(cause),
	}).Warn("event relay: event dead-lettered after exhausting its retry budget")
}

// ---------------------------------------------------------------------------
// DUAL-DELIVERY BRANCH — DELETED AT THE WEBHOOK SUNSET
//
// Everything between this banner and its closing one is temporary by design. It exists for
// the 30-day window in which Kafka publishing and legacy HTTP webhook delivery run side by
// side, and it is removed on the sunset date together with:
//
//   - webhooks.go itself, and with it SendWebhook, processHTTP, ProcessWebhook and
//     EnqueueLegacyWebhookDelivery — but ONLY AFTER NewWebhook and getEventFromStatus have
//     been relocated into the event package. Those two symbols are not part of the legacy
//     transport: NewWebhook IS the payload contract that outlives it, and
//     getEventFromStatus defines seven of the event-type strings. Deleting them would break
//     the payload guarantee the sunset is supposed to leave standing.
//   - the WebhookQueue → ProcessWebhook HANDLER MAPPING in cmd/workers.go, and nothing else
//     from that file. THE QUEUE ITSELF MUST SURVIVE: conf.Queue.WebhookQueue,
//     initializeWebhookQueues and the webhook worker server are shared with the transaction
//     hooks subsystem, which enqueues onto that queue by name, and with the two TypeSense
//     index handlers registered on the same mux. Removing the queue would silently disable
//     hooks and search indexing, neither of which has anything to do with webhooks.
//   - MarkWebhookDispatched and the webhook_dispatched column, whose only purpose is this
//     branch.
//
// What must NOT be removed with it is everything above: claiming, publishing, the retry
// schedule, dead-lettering and replay all outlive the sunset unchanged.
// ---------------------------------------------------------------------------

// deliverLegacyWebhook enqueues the legacy HTTP delivery of one claimed row.
//
// # This is what makes payload identity STRUCTURAL rather than procedural
//
// The bytes handed to the legacy transport are row.Payload — the exact bytes recorded in
// the ledger transaction and the exact bytes published to Kafka. Nothing is rebuilt from the
// original domain object and nothing is re-marshalled, so the two transports cannot drift
// apart: there is no second serialisation to drift from. A round trip through
// NewWebhook.Payload interface{} would look identical and not be — object keys come back
// alphabetised and every number comes back as a float64 — which is exactly the difference a
// reviewer's eye passes over and a byte comparison does not. That is why this calls
// EnqueueLegacyWebhookDelivery and not SendWebhook.
//
// # Idempotency, twice over
//
// The row's webhook_dispatched flag skips a row whose legacy leg is already done, so a row
// republished to Kafka after a crash does not enqueue a second webhook. Because enqueuing
// and recording are two operations with no transaction spanning them, the enqueue ALSO
// carries the event id as its asynq task identity, and a duplicate under an identity already
// present is refused by the queue rather than accepted. The flag makes the common case
// cheap; the task identity makes the crash case correct.
//
// # A legacy failure never fails the Kafka path
//
// Every failure here is logged and swallowed. The Kafka publish, the row's dispatched state
// and the retry budget are all unaffected, because the legacy leg has its own retry
// machinery — asynq owns HTTP retry and always has — and because letting a webhook receiver
// being down consume a Kafka retry attempt would make the deprecated transport able to
// dead-letter events on the new one.
//
// Parameters:
//   - ctx context.Context: the batch context. The recording transition runs on a detached
//     context so a shutdown cannot leave an enqueued webhook unrecorded.
//   - row model.EventOutbox: the claimed row, carrying its stored payload and claim token.
func (p *EventRelayProcessor) deliverLegacyWebhook(ctx context.Context, row model.EventOutbox) {
	if p.legacy == nil {
		return
	}

	if row.WebhookDispatched {
		return
	}

	if err := p.legacy.EnqueueLegacyWebhookDelivery(row.EventID, row.Payload); err != nil {
		logrus.WithFields(p.rowFields(row, row.Attempts+1)).WithError(err).Warn(
			"event relay: enqueuing the legacy webhook delivery failed; the Kafka publish is " +
				"unaffected and the legacy leg will be retried on the next claim of this row",
		)

		return
	}

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if err := p.store.MarkWebhookDispatched(bookkeeping, row.ID, row.ClaimToken); err != nil {
		// The task is enqueued and will be delivered; only the marker is missing. A later
		// claim re-enqueues under the same task identity, which the queue refuses as a
		// duplicate, so this is an observability gap rather than a delivery defect.
		logrus.WithFields(p.rowFields(row, row.Attempts+1)).WithError(err).Warn(
			"event relay: the legacy webhook was enqueued but the row could not be marked; " +
				"a re-enqueue is suppressed by the task identity",
		)
	}
}

// ---------------------------------------------------------------------------
// END OF THE DUAL-DELIVERY BRANCH — DELETED AT THE WEBHOOK SUNSET
// ---------------------------------------------------------------------------

// logAttempt logs EVERY publish attempt, not only the last one.
//
// Requirement R-4 is explicit about this, and about the field set: the attempt number, the
// maximum attempts, the error, the event id and the topic. All five are present on a failed
// attempt, which is the line an operator reads when an event is not arriving — "attempt 2 of
// 5, connection refused, event X, topic blnk.transactions" answers what happened, how much
// budget is left and where to look, from one line.
//
// The levels are chosen so the requirement is met without drowning the log at 500 events per
// second. A FAILURE is logged unconditionally at warning: it is rare, it is what the
// requirement is about, and losing it to a level filter would defeat the purpose. A SUCCESS
// is logged at debug and the fields are built only if debug is enabled, because a line per
// published event is a line five hundred times a second whose content is "it worked".
//
// This is the relay's own line and it is deliberately not the publisher's. The publisher
// reports the transport's view — duration, transient classification, hashed partition key —
// while this reports the RELAY's: which attempt of which budget, and what the retry decision
// will be. Both are wanted, and neither is a substitute for the other.
//
// Parameters:
//   - row model.EventOutbox: the row that was published.
//   - attempt int: the 1-based attempt number.
//   - result PublishResult: the publisher's record of the attempt.
//   - err error: the publish failure, or nil.
func (p *EventRelayProcessor) logAttempt(
	row model.EventOutbox,
	attempt int,
	result PublishResult,
	err error,
) {
	if err == nil {
		if !logrus.IsLevelEnabled(logrus.DebugLevel) {
			return
		}

		logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
			"status":      string(result.Status),
			"duration_ms": result.Duration.Milliseconds(),
		}).Debug("event relay: published a ledger event")

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"error":       relayFailureReason(err),
		"status":      string(result.Status),
		"transient":   result.Transient,
		"retryable":   result.Retryable,
		"duration_ms": result.Duration.Milliseconds(),
	}).Warn("event relay: publishing a ledger event failed")
}

// rowFields renders the identity of a row and its attempt as logrus fields.
//
// It is one function so that every line this file emits about one event carries the SAME
// field names, which is what makes the log searchable — and so that the four fields
// requirement R-4 names besides the error reason cannot be omitted from one line by
// accident.
//
// The partition key is deliberately ABSENT rather than printed: it is a financial
// identifier, and the publisher already reports a hash of it for correlation. The event type
// is present because it costs nothing and answers "which producer is failing" without a
// second lookup.
//
// Parameters:
//   - row model.EventOutbox: the row to describe.
//   - attempt int: the 1-based attempt number.
//
// Returns:
//   - logrus.Fields: a fresh map the caller may extend.
func (p *EventRelayProcessor) rowFields(row model.EventOutbox, attempt int) logrus.Fields {
	return logrus.Fields{
		"event_id":     row.EventID,
		"event_type":   row.EventType,
		"topic":        row.Topic,
		"attempt":      attempt,
		"max_attempts": p.rowMaxAttempts(row),
		"outbox_id":    row.ID,
	}
}

// relayFailureReason renders a publish failure as text that is safe to log AND safe to store
// in the row's last_error column.
//
// Both destinations need the same treatment, which is why there is one function. The string
// comes from a broker or a client library, so its length and its content are not this
// codebase's to choose: newlines would forge log structure, control characters corrupt
// structured-log parsers, and an unbounded value written once per attempt per event —
// multiplied by the retry budget and the event rate — is how a log pipeline gets throttled
// and a text column grows without limit. sanitizeLogValue is shared with the rest of the
// event pipeline so the bounds are identical wherever the same error surfaces.
//
// A nil error yields a fixed, non-empty string rather than "": last_error is what an
// operator reads to find out what went wrong, and an empty reason recorded against a failed
// attempt is indistinguishable from a row nobody has tried yet.
//
// Parameters:
//   - cause error: the failure. May be nil.
//
// Returns:
//   - string: bounded, single-line, never empty.
func relayFailureReason(cause error) string {
	if cause == nil {
		return "the publish failed without reporting a reason"
	}

	reason := sanitizeLogValue(cause.Error(), maxLoggedErrorLength)
	if reason == "" {
		return "the publish failed without reporting a reason"
	}

	return reason
}

// detachedBookkeepingContext derives a short-lived context for a transition the relay
// ALREADY OWES, so that a shutdown cannot leave the database disagreeing with what has
// already happened on the wire.
//
// The three transitions it covers — marked dispatched, marked failed, webhook marked
// dispatched — all describe work that is already complete: the broker has the message, or
// the queue has the task. Cancelling them would not undo any of it; it would only leave the
// row saying otherwise, and a row that says otherwise is republished on its next claim. So
// on a graceful shutdown the caller's context is cancelled precisely at the moment when
// abandoning the bookkeeping converts a clean stop into a batch of duplicates.
//
// It is BOUNDED rather than simply detached, because "finish what you owe" must not become
// "block shutdown indefinitely on a database that has gone away". If the timeout is reached
// the transition fails, it is logged, and the row's lease expiry brings it back — the
// ordinary at-least-once path.
//
// The claim and the publishes deliberately keep the CALLER's context: they are work not yet
// started, and a shutdown should stop starting work.
//
// Parameters:
//   - ctx context.Context: the batch context, used only for its values.
//
// Returns:
//   - context.Context: a context carrying ctx's values, cancelled only by its own timeout.
//   - context.CancelFunc: must be called, conventionally by defer.
func detachedBookkeepingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), eventRelayBookkeepingTimeout)
}

// ---------------------------------------------------------------------------
// A NOTE ON THE METRICS THIS FILE DOES NOT RECORD
//
// The relay records no instrument directly, and that is deliberate on both counts:
//
//   - The three PER-ATTEMPT instruments — blnk.events.published.total,
//     blnk.events.publish.attempts.total and blnk.events.publish.duration — are recorded
//     inside the publisher, on the attempt itself. The relay's contribution is to populate
//     the request fields those recordings are attributed BY: Attempt and MaxAttempts, so the
//     sub-two-second p99 target can be read from first-attempt publishes alone, ClaimedAt,
//     so the duration measures claim to acknowledgement rather than just the write, and
//     Purpose, so an operator-triggered replay never contaminates the first-delivery
//     population. Recording them here as well would double every count.
//   - The three GAUGES — blnk.outbox.pending, blnk.dlt.oldest_message_age_seconds and
//     blnk.kafka.consumer_lag — have exactly one owner, the periodic EventMetricsCollector
//     in event_metrics.go, and internal/metrics documents that single ownership as a
//     correctness requirement rather than tidiness. A gauge retains its last value, so each
//     must be re-recorded from authoritative state on every tick INCLUDING an explicit zero
//     — zero is the measurement that clears the alert — and a gauge written from the code
//     path that causes the condition measures the wrong thing. The dead-letter age is the
//     clearest case: recorded where an event is dead-lettered it would report the age of
//     something that just happened, always near zero, and the "stuck for 15 minutes" alert
//     could never fire while looking healthy throughout. The collector also needs the Kafka
//     admin client to difference committed offsets against end offsets for consumer lag,
//     which the relay does not hold; the server role builds both and starts the collector
//     beside this processor.
// ---------------------------------------------------------------------------
