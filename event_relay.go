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

// This file holds the RELAY half of the ledger event pipeline: the background processor that
// drains blnk.event_outbox to Kafka.
//
//	domain action ─┬─▶ ledger mutation
//	               └─▶ event outbox row   (event_outbox.go — inside the mutation's
//	                                       transaction when the caller shares one,
//	                                       otherwise its own committed insert)
//	                                        │
//	                        EventRelayProcessor claims the row, publishes it to Kafka,
//	                        marks it dispatched — and, during the dual-delivery window,
//	                        enqueues the legacy HTTP webhook from the SAME row.
//
// # The guarantee, stated honestly
//
// EXACTLY-ONCE APPLIES TO THE WRITE SIDE, and its strength depends on the capture. A caller
// that threads the row through PublishEventInTx or an atomic writer commits mutation and event
// together. A standalone capture — PublishEvent, which the domain post-action call sites use —
// commits on its own afterwards and depends instead on model.DeriveEventID making the id a
// function of the mutation, so a replayed mutation collides on the unique index on event_id.
// Events with no stable identity (balance.monitor, system.error) take a random id and have no
// such protection, by design: they are repeatable.
//
// DELIVERY REMAINS AT-LEAST-ONCE either way. The relay publishes and then marks the row
// dispatched — two systems, no transaction spanning them — so a crash in between leaves a row
// published but not marked, and the next claim publishes it again. Marking first would lose
// events instead, and a duplicate is recoverable at the subscriber on event_id while a loss is
// recoverable nowhere.
//
// # Crash recovery, and durable retry
//
// A claim takes a LEASE (locked_until) rather than removing the row, so a relay that dies
// mid-batch strands nothing: processing with a live lease becomes claimable when the lease
// expires, pending after a failed attempt becomes claimable when next_attempt_at arrives, and
// dispatched or dead_lettered is terminal only once the event exists somewhere durable.
//
// Retry is applied by PERSISTING the next due instant, not by sleeping: one claim performs one
// publish attempt, and a failed attempt records now + the computed delay and releases the
// lease. See relayRetryPolicy and processRow.
package blnk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

const (
	// defaultEventRelayBatchSize is how many outbox rows one claim takes, matching
	// LineageOutboxProcessor: large enough to amortise the claim round trip, small enough
	// that a crash mid-batch leaves little to redo.
	defaultEventRelayBatchSize = 100

	// defaultEventRelayPollInterval is how often the relay looks for work when it has none,
	// matching LineageOutboxProcessor. It is NOT the throughput limit — see
	// maxEventRelayBatchesPerTick, which lets one tick drain a backlog.
	defaultEventRelayPollInterval = 1 * time.Second

	// defaultEventRelayLockDuration is the lease a claim takes on its rows, matching
	// LineageOutboxProcessor. It is the recovery latency after a crash: rows a dead relay
	// held become claimable again this long after it stopped.
	//
	// IT IS NOT A CERTIFICATE THAT A BATCH FITS INSIDE IT, and there is no lease renewal. A
	// default batch of 100 rows across 8 concurrent groups, each attempt bounded by the
	// writer's 10s produce timeout, can in the worst case need appreciably longer than 30s —
	// a broker that is slow rather than down is exactly that case. The consequence is
	// bounded but real: another instance may reclaim a row this one is still publishing,
	// producing a redelivery the subscriber suppresses on event_id, while the late
	// MarkEventDispatched finds its claim token gone and logs rather than corrupting the row.
	// A deployment that sees that should lengthen the lease or shorten the batch.
	defaultEventRelayLockDuration = 30 * time.Second

	// defaultEventRelayConcurrency is how many partition-key groups of one batch are
	// published at once.
	//
	// Sequential publishing cannot meet the throughput requirement: a publish waits for
	// every in-sync replica to acknowledge, so at a realistic 10ms per acknowledgement one
	// goroutine sustains about 100 events per second against a requirement of 500. Eight
	// goroutines over the publisher's SHARED writers and their shared transport reach it
	// with headroom, which is the usage kafka-go documents — one writer, many callers.
	//
	// ORDERING IS NOT AT RISK. Kafka orders within a PARTITION, the partition is chosen by
	// the message key, and the key is the row's EFFECTIVE key — its ledger where it has one,
	// its stored partition key where it does not (model.EffectivePartitionKey); two events
	// that must stay ordered therefore share a key, and this file never publishes two rows
	// with the same key at the same time. See groupEventRowsByPartitionKey for the two
	// independent reasons that holds.
	defaultEventRelayConcurrency = 8

	// maxEventRelayBatchesPerTick bounds how many batches one tick chains, so a large
	// backlog drains promptly without starving the stop signal. Modelled on
	// ChainProcessor's maxBatchesPerTick. Without chaining the ceiling would be batchSize
	// per pollInterval — 100 events per second at the defaults — however fast the broker is.
	maxEventRelayBatchesPerTick = 50

	// eventRelayRowPublishBudget is the WORST-CASE wall time ONE row's publish attempt may
	// take, and it is the only thing that bounds a publish by arithmetic.
	//
	// It is eventWriterWriteTimeout — the writer's own produce timeout, 10s — because that is
	// the longest a single PublishToTopic can block before it gives up. Expressing it here as
	// well is belt to that brace: an EventTransport is an interface, and one that ignored its
	// own timeout would otherwise park a semaphore permit for the life of the process.
	//
	// It is deliberately NOT derived from the lease. See leaseDeadline for why a lease-derived
	// publish deadline causes the duplicate it is meant to prevent once the lease is renewed.
	//
	// Keep it in step with eventWriterWriteTimeout: a writer allowed to block longer than this
	// would be cut off mid-produce by a bound that claims to be its worst case.
	eventRelayRowPublishBudget = eventWriterWriteTimeout

	// eventRelayBackoffMultiplier is the factor the retry delay grows by on each attempt.
	// Requirement R-4 fixes it at 2, so it is a constant rather than configuration: the
	// base delay, the cap and the attempt count are all tunable, the doubling is not.
	eventRelayBackoffMultiplier = 2

	// eventRelayLeaseRenewalDivisor sets how often an in-flight batch renews its lease, as
	// a fraction of the lease itself.
	//
	// A third is the standard choice for a heartbeat and the reason is arithmetic: two
	// consecutive renewals may be missed — one to a slow database round trip, one to
	// scheduling — and the lease still has a third of its life left when the third
	// succeeds. Renewing at half the lease survives one miss; renewing at the lease itself
	// survives none.
	eventRelayLeaseRenewalDivisor = 3

	// eventRelayMinLeaseRenewalInterval floors the renewal interval so a very short lease
	// cannot turn the heartbeat into a busy loop against the database.
	//
	// A test lease of a few hundred milliseconds is legitimate — the recovery tests use one
	// to make expiry observable — and dividing it by three would produce renewals tens of
	// times a second for no benefit.
	eventRelayMinLeaseRenewalInterval = 250 * time.Millisecond

	// The REPAIR fallbacks (PERF-M06), reached only by a relay built without a readable
	// configuration. config.DefaultRelayRepair* are the shipped values and the single source
	// of the numbers; these mirror them so a relay that cannot read a configuration still
	// recovers at a usable rate rather than at a token one.
	//
	// # What replaced the fixed 20 rows a tick, and why it had to
	//
	// Both repair passes used to claim 20 rows, once per tick, sequentially, un-chained — and
	// the number looked adequate because these sets are EMPTY in normal operation, so an idle
	// pass costs one indexed query whatever the batch size is. They fill during an outage, all
	// at once: 15 minutes at the 500 events per second acceptance rate produces about 450,000
	// rows, and 20 rows a second drains that in roughly SIX AND A QUARTER HOURS on one replica
	// with the dead-letter age alert firing throughout.
	//
	// The three knobs multiply into a drain rate — see config.RelayConfig's repair block for
	// the arithmetic — and each chained batch takes its OWN claim and its own lease, so
	// chaining does not widen the window in which any row is held.
	defaultEventRelayRepairBatchSize      = config.DefaultRelayRepairBatchSize
	defaultEventRelayRepairBatchesPerTick = config.DefaultRelayRepairMaxBatchesPerTick
	defaultEventRelayRepairConcurrency    = config.DefaultRelayRepairConcurrency

	// eventRelayBookkeepingTimeout bounds the bookkeeping transitions the relay performs
	// on a DETACHED context. See detachedBookkeepingContext for why they are detached at
	// all; this is what stops "finish what you owe" becoming "hang forever on a database
	// that has gone away during shutdown".
	eventRelayBookkeepingTimeout = 5 * time.Second

	// The two fallback delays below mirror config's relay defaults, which are unexported.
	// They mirror the REQUIREMENT rather than an implementation detail: R-4 fixes the base
	// delay at one second and the cap at thirty. They are reached only by a Configuration
	// built directly, which is how the existing test suite constructs one. The attempt-count
	// fallback is not mirrored: config.MaxRelayRetryAttempts is exported.
	fallbackRelayRetryBaseBackoff = 1000 * time.Millisecond
	fallbackRelayRetryMaxBackoff  = 30000 * time.Millisecond
)

// relayRetryPolicy is the bounded exponential backoff schedule of requirement R-4,
// resolved once from configuration into a value with a pure method, so the part of this file
// most worth testing exactly is testable without a database, a broker or a clock.
type relayRetryPolicy struct {
	// maxAttempts is the retry budget: how many publish attempts an event gets before it is
	// dead-lettered. It is informational HERE — the budget is enforced in SQL by
	// MarkEventFailed, which is what stops two instances both concluding they were the last
	// attempt — and is carried so the relay can report "attempt 3 of 5".
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
// bounded by config.MaxRelayRetryAttempts); the fallbacks here are a second line of defence
// for a Configuration built directly. A zero or negative value becomes the documented
// default rather than a schedule of zero-length delays, which would spend the whole retry
// budget in one poll interval and dead-letter an event during a broker restart that would
// have cleared on its own.
//
// An inverted window (base above cap) is not rejected: backoffFor applies the cap last, so a
// base of 60s against a cap of 30s yields 30s on every attempt — the slower schedule the
// operator asked for, bounded as configured, rather than a refusal to start.
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

// backoffFor returns the delay recorded against the attempt AFTER the given one.
//
// attempt is 1-based and names the attempt that just failed. With the mandated parameters —
// base 1s, multiplier 2, cap 30s, five attempts — the schedule is exactly
//
//	1s, 2s, 4s, 8s, 16s
//
// and THE LIVE PATH PRODUCES EVERY ONE OF THOSE FIVE VALUES. recordFailedAttempt calls this
// for each failed attempt from the first through the fifth and hands the result to
// MarkEventFailed, which stamps it onto the row's next_attempt_at in the same statement that
// decides whether the budget is spent. So the fifth delay, 16s, is computed and durably
// recorded exactly like the other four; it is observable on the row and in the
// scheduling log line, and event_relay_test.go asserts the whole sequence against a live
// processor rather than against this function in isolation.
//
// The one thing the fifth delay does NOT do is separate two publishes. Five attempts have
// four gaps between them, so 1s, 2s, 4s and 8s — 15 seconds in total — are the delays a
// retried event actually waits, and the fifth failure spends the budget and dead-letters the
// row rather than scheduling a sixth publish. Both facts matter and neither replaces the
// other: the SCHEDULE is five values, the WAITS a five-attempt event experiences are the
// first four of them, and a deployment that raises RELAY_MAX_RETRY_ATTEMPTS is what turns the
// fifth into a wait as well.
//
// THE 30-SECOND CAP IS OUT OF REACH at the mandated parameters, which is correct rather than a
// bug: the cap bounds a schedule whose configured base or attempt count would otherwise
// exceed it. Neither the multiplier nor the cap may be "corrected" to make it engage.
//
// Growth is repeated doubling with an early return at the cap rather than exponentiation, so
// there is no overflow to reason about however large an attempt number arrives from a row whose
// max_attempts was raised directly in the database. An attempt below 1 is treated as 1.
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
// Depending on a four-method interface rather than on the whole IDataSource is what makes
// every path in this file testable without a database, and it documents the blast radius:
// the relay claims rows and drives them to a terminal state. It cannot insert an event (that
// belongs to event_outbox.go), cannot read the dead-letter inventory, and cannot replay.
//
// MarkEventDeadLettered is POINTEDLY ABSENT. Recording a dead-letter is only legitimate once
// the event is actually on its `<topic>.dlt` sibling, and only the dead-letter writer knows
// whether it got there; the relay hands off instead. See eventRelayDeadLetterer.
type eventRelayStore interface {
	// ClaimPendingEventOutbox claims a batch, oldest occurrence first, taking a lease and
	// stamping every row with a claim token. It returns at most one row per EFFECTIVE key —
	// the same key the publisher hashes — across all concurrent relay instances, which is
	// half of the ordering guarantee.
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	// MarkEventDispatched moves a claimed row to its success terminal state. It is
	// conditional on the claim token and CLEARS it, so it must be the last transition the
	// relay performs on a row.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error

	// MarkEventFailed records one failed attempt, schedules the row's next due instant
	// from retryAfter, and reports — decided in SQL, so two instances cannot both conclude
	// they were last — whether the budget is now spent.
	//
	// terminal carries the PUBLISHER'S VERDICT that no further attempt can succeed, which is
	// what makes the durable state agree with what the publisher already reported and
	// metered. A permanent failure exhausts the row on whichever attempt it happened rather
	// than spending the remaining budget rediscovering it.
	//
	// deadLetterLease is how long the EXHAUSTION ARM holds the row for the dead-letter
	// hand-off. The relay passes its own lockDuration, so the hand-off is owned for exactly
	// the window every other claim uses. It is not a detail: the repair claim selects on the
	// lease alone, so a released lease lets it stamp a fresh token over a hand-off that is
	// still in flight and two workers write the same event to the same .dlt topic.
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, terminal bool, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// ClaimPendingWebhookDeliveries claims rows whose KAFKA leg has finished — successfully
	// or terminally — and whose LEGACY WEBHOOK leg is still owed, so a leg that failed
	// alongside a Kafka publish is not lost behind a state the ordinary claim never
	// revisits. Deleted at the sunset with the leg itself.
	ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	// MarkEventLegacyWebhookAttempted records one failed legacy enqueue against a row whose
	// Kafka leg has already finished, WITHOUT touching that leg's terminal state, and reports
	// whether the legacy budget is now spent. Deleted at the sunset with the leg itself.
	MarkEventLegacyWebhookAttempted(ctx context.Context, id int64, claimToken string, retryAfter time.Duration) (model.EventWebhookOutcome, error)

	// MarkEventPermanentlyFailed records an attempt whose failure was PERMANENT, taking the
	// row to failed on this attempt whatever budget remained and retaining the claim token
	// AND THE LEASE for the dead-letter hand-off. It is what lets the relay act on the
	// publisher's verdict instead of spending four more attempts on a condition none of them
	// can change.
	MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// MarkWebhookDispatched records the legacy leg of the dual-delivery window. It is
	// conditional on the claim token, so it must run BEFORE MarkEventDispatched clears it.
	// This method disappears at the webhook sunset along with the branch that calls it.
	MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error

	// MarkEventWebhookPending records a Kafka leg that is DONE alongside a legacy webhook
	// leg that is still OWED, and reports whether another webhook attempt is owed or the
	// leg has been abandoned. It is what keeps a failed enqueue recoverable instead of
	// being lost behind a terminal state. Deleted at the sunset with the leg itself.
	MarkEventWebhookPending(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, record model.BrokerRecord) (model.EventWebhookOutcome, error)

	// RenewEventOutboxLease extends the lease on every row still in flight under one claim
	// token. It is what lets a batch outlive its lease safely instead of having rows
	// reclaimed and published twice while this instance was still working on them.
	RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error)

	// ClaimFailedEventOutboxForDeadLetter claims rows whose retry budget is spent and whose
	// dead-letter write did not succeed, so the preservation can be retried. Without it such
	// a row is unclaimable, unreplayable and the only copy of the event.
	ClaimFailedEventOutboxForDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)
}

// eventRelayDeadLetterer is the hand-off the relay makes when a row has spent its retry
// budget. The relay decides that an event is FINISHED; the dead-letter writer decides
// everything else — which `<topic>.dlt` sibling, what the failure metadata says, how the
// message keeps the original bytes recoverable, and when the terminal state is recorded. The
// interface is one method wide so none of that can be reimplemented here.
type eventRelayDeadLetterer interface {
	// DeadLetter writes the exhausted row to its dead-letter topic with failure metadata
	// attached and records the terminal state. It requires the row to carry the claim
	// token MarkEventFailed retained on its exhaustion arm.
	DeadLetter(ctx context.Context, row model.EventOutbox, cause error) (DeadLetterOutcome, error)
}

// eventRelayLegacyTransport is the legacy HTTP webhook leg of the dual-delivery window.
//
// It exists only for the dual-delivery window; see deliverLegacyWebhook for what the window
// guarantees, and the sunset block at the foot of webhooks.go for the removal procedure.
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
// It polls blnk.event_outbox for rows that are due, publishes each to its category topic, marks
// it dispatched, dead-letters it when its retry budget is spent, and — until the webhook sunset
// — additionally enqueues the legacy HTTP delivery from the same row.
//
// The struct fields, the defaults, the fluent configurators and the whole Start / Stop /
// IsRunning / run / processBatch lifecycle are LineageOutboxProcessor's, which ChainProcessor
// already reuses; Blnk operates two outboxes over two tables served by two relays, sharing the
// pattern and not the state. Two additions earn their place and are documented where they are
// declared: concurrency across partition-key groups, and collaborators resolved once at
// construction.
//
// CONFIGURE FIRST, THEN USE. The fluent With* setters write their fields WITHOUT
// synchronisation, so they must be called before Start and never while the relay is running.
// Once configured the lifecycle is safe for concurrent use — the running state is mutex-guarded
// and the collaborators are read-only — and several instances across several processes are safe
// too, which is the point of claiming with FOR UPDATE SKIP LOCKED.
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

	// The REPAIR capacity (PERF-M06), resolved from configuration at construction and
	// overridable through WithRepairCapacity. It governs the two passes that clear work the
	// publish claim cannot reach — outstanding dead-letter writes and outstanding legacy
	// webhook enqueues — and it is separate from the publish batch because the two fill under
	// different conditions: the publish backlog grows with ingress, these grow with an outage.
	repairBatchSize      int
	repairBatchesPerTick int
	repairConcurrency    int

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex

	// store is the outbox repository, narrowed to the four methods the relay drives.
	store eventRelayStore

	// publisher is the SHARED Kafka publisher this process built at start-up. The relay is
	// the only production caller of a kafka.Writer, and it borrows the process publisher
	// rather than building one so that every write reuses the publisher's writers and their
	// one shared transport, with its connection pool and its authentication configuration.
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

	// dualDeliveryActive is THE dual-delivery decision, and it is a function field only
	// so a test can pin the clock. In production it is event_sunset.go's
	// WebhookDualDeliveryActive and nothing else: this file never parses a configured
	// date and never compares instants itself. Two comparisons could disagree, and the
	// pair that would then disagree is "stop dual-writing" and "answer 410 Gone" — a
	// system that had done one but not the other.
	//
	// It consults BOTH ends of the window, not only the sunset. Requirement R-12 is
	// about the span between them: the legacy leg runs for exactly the 30 days of
	// [start, sunset), and a predicate reading the sunset alone could not express that.
	dualDeliveryActive func(now time.Time) bool

	// windowState is the same resolution in its four-valued form, used where the REASON
	// dual delivery is off changes what happens: the startup obstacle refuses to run a
	// relay whose window has not opened yet, which is a misconfiguration, while it starts
	// happily after the sunset, which is the intended end state.
	windowState func(now time.Time) WebhookWindowState

	// now is the clock, replaceable in-package so the sunset boundary is exact in tests.
	// It follows EventDeadLetterService's now field.
	now func() time.Time

	// catalogue gates CLAIMING on the destination topics actually existing. Nil means
	// ungated, which is what every existing test and every construction without
	// WithCatalogueGate gets — those relays publish to a broker a test controls, and gating
	// them on a real metadata read would be a dependency they do not have.
	//
	// In production cmd/server.go always supplies one. See WithCatalogueGate for why a gate
	// on the CLAIM is the right shape rather than a gate on Start.
	catalogue eventRelayCatalogueGate
}

// eventRelayCatalogueGate answers whether the relay may claim work yet.
//
// One method, so a test can gate a relay with a closure and so the relay's dependency on the
// Kafka admin surface stays exactly this wide.
type eventRelayCatalogueGate interface {
	// Ready returns nil when every topic the relay may need is known to exist. A non-nil
	// error means the relay must not claim, and the gate has already reported why — the
	// relay stays silent so a 1-second poll cannot turn one condition into a log flood.
	Ready(ctx context.Context) error
}

// NewEventRelayProcessor creates the event outbox relay for a Blnk instance.
//
// Everything the relay needs is derived from the instance once: the datasource, the
// process-wide Kafka publisher, a dead-letter service bound to both, the legacy webhook
// transport, and the retry schedule from configuration. That is what makes the server-role
// start-up three lines — construct, start, defer stop.
//
// It performs NO I/O and never fails. A nil instance, a missing datasource or a publisher
// that is absent or is the no-op yields a processor that refuses to start and says why. See
// Start.
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
		now:          time.Now,

		repairBatchSize:      defaultEventRelayRepairBatchSize,
		repairBatchesPerTick: defaultEventRelayRepairBatchesPerTick,
		repairConcurrency:    defaultEventRelayRepairConcurrency,

		dualDeliveryActive: WebhookDualDeliveryActive,
		windowState:        WebhookDualDeliveryWindowState,
	}

	if blnk == nil {
		// Still usable as a value: Start refuses, Stop is a no-op, IsRunning is false.
		// NewBlnk(nil) is a supported construction in this codebase, so a processor built
		// from a bare instance must fail legibly rather than panic on a ledger process.
		processor.retry = newRelayRetryPolicy(config.RelayConfig{})

		return processor
	}

	processor.retry = newRelayRetryPolicy(blnk.Config().Relay)
	processor.applyRepairCapacity(blnk.Config().Relay)
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

// WithCatalogueGate makes the relay refuse to CLAIM until every topic it may need is known to
// exist. Call it before Start.
//
// # Why claiming is what is gated, rather than starting
//
// The topics are assured once at boot, and that pass was allowed to fail and be stepped past —
// deliberately, because the usual cause is a broker that is not listening yet (a compose stack
// coming up, a rolling restart), and refusing to start the relay would turn a transient
// condition into an outage that needs a human to end.
//
// But starting anyway had its own cost, and it is not small. Claiming a row LEASES it, and the
// relay then publishes to a topic that does not exist. Auto-creation is disabled, so the
// publish fails; it retries on the schedule and burns the row's whole attempt budget; and when
// the budget is spent the dead-letter write fails FOR THE SAME REASON, because the dead-letter
// sibling is missing too. The row ends failed with no dead-letter topic recorded — the state
// the repair pass exists to mop up — and a boot against an unprovisioned broker could spend
// every pending row's budget before anybody noticed the topics were absent.
//
// Gating the CLAIM instead keeps both properties. The relay starts, so recovery is automatic
// the moment the broker answers; and until then it leases nothing, so no row spends an attempt
// on a destination that cannot accept it. The rows stay exactly where they are, which is the
// one thing the outbox is for.
//
// A nil gate is ignored rather than treated as closed: an ungated relay is the pre-existing
// behaviour and is what tests against a controlled broker need.
//
// Parameters:
//   - gate eventRelayCatalogueGate: the readiness gate. Nil leaves the relay ungated.
//
// Returns:
//   - *EventRelayProcessor: the same processor, for chaining.
func (p *EventRelayProcessor) WithCatalogueGate(gate eventRelayCatalogueGate) *EventRelayProcessor {
	if p == nil {
		return p
	}

	p.catalogue = gate

	return p
}

// WithBatchSize sets how many outbox rows one claim takes. Call it before Start.
//
// A non-positive size is ignored with a warning: the repository rejects one outright, so
// applying it would turn every poll into a logged error and publish nothing — a relay that
// looks alive and delivers nothing.
func (p *EventRelayProcessor) WithBatchSize(size int) *EventRelayProcessor {
	if size <= 0 {
		logrus.WithField("requested_batch_size", size).
			Warn("event relay: ignoring a non-positive batch size; keeping the configured value")

		return p
	}

	p.batchSize = size

	return p
}

// WithPollInterval sets how often the relay looks for work. Call it before Start.
//
// A non-positive interval is ignored with a warning: time.NewTicker panics on one, and a
// panic in a background loop takes the process down.
func (p *EventRelayProcessor) WithPollInterval(interval time.Duration) *EventRelayProcessor {
	if interval <= 0 {
		logrus.WithField("requested_poll_interval", interval.String()).
			Warn("event relay: ignoring a non-positive poll interval; keeping the configured value")

		return p
	}

	p.pollInterval = interval

	return p
}

// WithLockDuration sets the lease a claim takes on its rows, and the period it is renewed by.
//
// The duration is honoured exactly as given, and is also the crash-recovery latency: rows a
// dead relay was holding become claimable again this long after it stopped. A short lease is
// therefore a legitimate choice rather than a mistake, and it is never silently lengthened.
// A batch that needs longer than one lease RENEWS it — see renewLeaseWhileInFlight — so a
// batch can never outlive the claim it is running under, whatever the lease is set to.
//
// A non-positive duration is ignored with a warning. An expired-on-arrival lease is a
// correctness problem rather than a tuning mistake.
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

// WithConcurrency sets how many partition-key groups of one batch publish at once. Call it
// before Start.
//
// One means fully sequential, which is the right choice for a single-broker development
// stack; anything above one is bounded by the semaphore in processBatch. A non-positive value
// is ignored with a warning, because zero would wait for a permit that never exists.
func (p *EventRelayProcessor) WithConcurrency(workers int) *EventRelayProcessor {
	if workers <= 0 {
		logrus.WithField("requested_concurrency", workers).
			Warn("event relay: ignoring a non-positive concurrency; keeping the configured value")

		return p
	}

	p.concurrency = workers

	return p
}

// applyRepairCapacity resolves the two repair passes' capacity from configuration.
//
// It normalises rather than trusts, and it does so for the same reason config.setRelayDefaults
// does: a relay is constructible from a configuration this process did not load — a test's
// MockConfig, a value assembled in code — so a zero or negative here would silently disable the
// only path that ever revisits an event which reached no topic at all. Each non-positive value
// falls back to the shipped default, which is the behaviour every other capacity field in this
// file already has.
//
// Parameters:
//   - relay config.RelayConfig: the resolved relay configuration.
func (p *EventRelayProcessor) applyRepairCapacity(relay config.RelayConfig) {
	if relay.RepairBatchSize > 0 {
		p.repairBatchSize = relay.RepairBatchSize
	}
	if relay.RepairMaxBatchesPerTick > 0 {
		p.repairBatchesPerTick = relay.RepairMaxBatchesPerTick
	}
	if relay.RepairConcurrency > 0 {
		p.repairConcurrency = relay.RepairConcurrency
	}
}

// WithRepairCapacity sets how fast the two repair passes clear their backlogs. Call it before
// Start.
//
// ONE configurator for the three numbers rather than three, because they are one decision:
// batch size times batches per tick is the rows a tick may repair, and the concurrency is what
// decides how long those rows take. Setting one without the others is how a capacity change
// comes to have no effect — raising the batch alone leaves the per-tick bound binding, and
// raising both without the width leaves every row waiting on the previous acknowledgement.
//
// A non-positive value LEAVES THAT FIELD ALONE with a warning, rather than being applied. Zero
// rows a tick is a silently disabled recovery path, and the rows it would abandon are the only
// copies of events that reached no topic at all; zero concurrency would wait for a semaphore
// permit that never exists.
//
// Parameters:
//   - batchSize int: rows one repair claim takes.
//   - batchesPerTick int: how many such claims one tick chains.
//   - concurrency int: how many message-key groups of a batch are written at once.
//
// Returns:
//   - *EventRelayProcessor: the receiver, for chaining.
func (p *EventRelayProcessor) WithRepairCapacity(batchSize, batchesPerTick, concurrency int) *EventRelayProcessor {
	for name, value := range map[string]int{
		"repair_batch_size":       batchSize,
		"repair_batches_per_tick": batchesPerTick,
		"repair_concurrency":      concurrency,
	} {
		if value <= 0 {
			logrus.WithFields(logrus.Fields{"setting": name, "requested": value}).
				Warn("event relay: ignoring a non-positive repair capacity value; keeping the configured value")
		}
	}

	if batchSize > 0 {
		p.repairBatchSize = batchSize
	}
	if batchesPerTick > 0 {
		p.repairBatchesPerTick = batchesPerTick
	}
	if concurrency > 0 {
		p.repairConcurrency = concurrency
	}

	return p
}

// startupObstacle reports why the relay cannot run, or nil when it can.
//
// Three of the four refusals are absences — no instance, no datasource, no publisher that
// accepts a destination topic — and the fourth is the important one: THE NO-OP PUBLISHER IS
// REFUSED. It reports every publish as dispatched without sending anything, so running the
// relay on it would mark the whole outbox dispatched and lose every event. An empty broker
// list is a legitimate steady state for the PRODUCER side, and never a legitimate transport
// for the relay.
//
// Refusing in Start rather than from the constructor is what keeps the server-role call site
// three lines: the caller may start the relay unconditionally and this decides.
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

	// THE WINDOW HAS NOT OPENED YET — a misconfiguration, and refusing is what stops it
	// becoming a silent one.
	//
	// SUNSET: this check goes with the legacy leg.
	//
	// The dual-delivery decision gates on both ends of the window, so a relay running
	// before the configured start would publish to Kafka while enqueuing NO legacy
	// webhooks — subscribers who have not migrated would simply stop receiving events,
	// with nothing failing to say so. That is the same class of failure as the
	// webhook-only deployment that captured undrainable rows.
	//
	// The alternative — dual-delivering anyway and warning — would mean the concurrent
	// period ran longer than the 30 days published to subscribers while the configured
	// start claimed otherwise. Neither behaviour is acceptable silently, so the process
	// refuses and names the two ways out. Time only moves forwards, so this can be
	// reached only at start-up or after a configuration reload, never mid-run.
	//
	// The message is event_sunset.go's, because that file owns the window and the
	// vocabulary for explaining it; composing it here would put the names of the
	// configured dates in a second place.
	//
	// BOTH refusable states are checked, not only the pending one. Reaching this line means
	// a REAL publisher was accepted above — the no-op is refused, so Kafka is configured —
	// and for such a deployment WebhookWindowUnavailable means the window is missing or
	// unparseable, which the sunset helper fails closed on: it answers "already sunset", so
	// the relay would publish to Kafka and enqueue NO legacy webhooks while configuration
	// claimed a migration was still under way. That is the same silent loss as the pending
	// case, arrived at from the other end of the window. config.resolveWebhookDeprecationWindow
	// refuses to LOAD the combination, so this is a second line of defence for configuration
	// that arrived some other way — a test writing to the store, or a future reload path.
	if err := WebhookWindowObstacle(p.windowStateAt(p.now())); err != nil {
		return err
	}

	return nil
}

// Start begins draining the event outbox in the background.
//
// The processor polls for due rows at the configured interval until the context is cancelled
// or Stop is called. It is idempotent: a second Start while running is ignored rather than
// spawning a second loop, and stopCh is RE-CREATED here, which is what makes Start after
// Stop work rather than returning immediately on a closed channel.
//
// It refuses to start when it could not publish — see startupObstacle — logging the reason;
// IsRunning then stays false and Stop remains safe, so an unconditional call site is correct.
//
// Parameters:
//   - ctx context.Context: when cancelled, processing stops; rows already claimed keep their
//     lease and become claimable again when it expires.
func (p *EventRelayProcessor) Start(ctx context.Context) {
	if err := p.startupObstacle(); err != nil {
		withLoggableCause(nil, err).Error("Event outbox relay not started")

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
		// The running flag is cleared on EVERY exit from the loop, not only the one Stop
		// takes. run returns on parent-context cancellation too, and leaving the flag set
		// there made IsRunning report a relay that had already stopped and made a later
		// Start refuse to spawn a new loop — a process that cancelled its context and then
		// restarted the relay silently ran without one. Clearing it here, under the mutex,
		// covers both paths from one place; Stop's own clear stays because it must take
		// effect before the loop notices the closed channel.
		defer p.markStopped()

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

// markStopped clears the running flag, whatever ended the loop.
//
// It is deferred by the goroutine Start spawns, so it runs for the stop signal, for parent
// context cancellation, and for a panic that unwinds the loop. Stop also clears the flag,
// before closing the channel, so the two overlap by design: Stop needs the flag down at the
// instant it decides to close, and this needs it down for every exit Stop was not involved
// in. Clearing an already-clear flag is a no-op, so the overlap costs nothing.
//
// Start re-creates stopCh, which is what makes a restart after either kind of exit work
// rather than returning immediately on a channel that is already closed.
func (p *EventRelayProcessor) markStopped() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()
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
// Chaining is what makes the batch size a throughput lever rather than a ceiling, which is
// why the lease race is answered by bounding each PUBLISH to the lease rather than by
// shrinking the batch to fit it: the batch stays at whatever the operator configured, and
// this loop claims again until the backlog is drained.
//
// Between batches it re-checks cancellation and the stop signal, so a shutdown is honoured
// promptly instead of after up to fifty more batches.
//
// Each tick also runs the dead-letter REPAIR pass, which retries the preservation of events
// whose dead-letter write failed. Those rows are outside the publish claim's reach by
// design — their retry budget is spent — and before the pass existed nothing ever acted on
// them again, even though each was the only copy of an event that reached no topic at all.
//
// Parameters:
//   - ctx context.Context: cancelling it abandons the remaining batches; claimed rows keep
//     their lease and become claimable again when it expires.
func (p *EventRelayProcessor) processTick(ctx context.Context) {
	// THE CATALOGUE GATE COMES FIRST, before the repair passes and before any claim.
	//
	// Every piece of work in this tick ends in a write to a Blnk-owned topic: the publish
	// loop to the category topics, and BOTH repair passes to the dead-letter siblings. If
	// those topics do not exist, none of it can succeed — and each attempt is not free, it
	// spends a row's retry budget and leaves the row worse off than untouched. So the tick
	// does nothing at all rather than doing something harmful.
	//
	// Silent on refusal, deliberately: the gate owns the reporting and rate-limits its own
	// broker probes, whereas this runs every poll interval — a log line here would be one per
	// second for as long as a broker was unprovisioned.
	if p.catalogue != nil {
		if err := p.catalogue.Ready(ctx); err != nil {
			return
		}
	}

	// The REPAIR passes first, and deliberately so. Each queries a set that is empty in normal
	// operation, so an idle pass costs one indexed query; and when a set is NOT empty those rows
	// are the only copies of events that reached no topic at all, which makes them the most
	// urgent work in the tick rather than the least. Putting them after the publish loop would
	// also mean a sustained backlog — fifty chained batches — starved the repair entirely.
	//
	// CHAINED, exactly as the publish loop below is (PERF-M06). One batch per tick was a drain
	// rate of batchSize per poll interval however fast the broker was, which could not clear
	// what an outage produces; see the repair constants for the arithmetic. The bound is what
	// keeps a recovery from starving the publish loop it shares this tick with.
	p.repairChained(ctx, repairLegDeadLetter, p.recoverUnpreservedDeadLetters)

	// The LEGACY leg's own repair pass, for the same reasons and with the same shape. Its
	// candidate set is likewise empty in normal operation and index-backed, and when it is not
	// empty those rows are webhooks promised for the migration window that no other statement
	// will ever pick up. SUNSET: deleted with the dual-delivery branch.
	p.repairChained(ctx, repairLegLegacyWebhook, p.recoverOwedLegacyWebhooks)

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

// The two repair legs, named so the metrics attribute and the log field cannot disagree about
// which backlog a reading describes.
//
// SUNSET: repairLegLegacyWebhook goes with the dual-delivery branch.
const (
	repairLegDeadLetter    = "dead_letter"
	repairLegLegacyWebhook = "legacy_webhook"
)

// repairChained runs one repair pass repeatedly until it drains, up to the per-tick bound, and
// publishes what the pass achieved (PERF-M06).
//
// # Why chaining, and why it is bounded
//
// A repair pass claims at most repairBatchSize rows. Run once per tick, the drain rate is that
// batch per poll interval — 20 rows a second under the previous fixed batch — which cannot clear
// what an outage produces: 15 minutes at 500 events a second is about 450,000 rows, roughly six
// and a quarter hours at that rate, with the dead-letter age alert firing throughout.
//
// The bound remains because the alternative is worse in the opposite direction. An unbounded loop
// would let one tick attempt an entire historical backlog, holding the publish loop — which this
// tick also owes — behind it for as long as that took. Bounded chaining drains fast AND keeps new
// events flowing, which matters because a repair backlog exists precisely while the pipeline is
// recovering.
//
// A SHORT BATCH ENDS THE CHAIN, which is the same drained-set signal processTick uses for the
// publish loop: a pass that repaired fewer rows than it could claim has nothing left to claim.
//
// # What it publishes
//
// blnk.events.repair.saturated is the reading that could not be derived from the backlog and the
// drain rate: both fall while a recovery is merely slow, so only "the budget was spent and work
// remained" distinguishes "recovering as configured" from "configured too slowly to recover".
//
// Parameters:
//   - ctx context.Context: cancelling it ends the chain; rows already claimed keep their lease.
//   - leg string: repairLegDeadLetter or repairLegLegacyWebhook, for the metric attribute.
//   - pass func(context.Context) int: one bounded pass, returning how many rows it repaired.
func (p *EventRelayProcessor) repairChained(ctx context.Context, leg string, pass func(context.Context) int) {
	budget := p.repairBatchesPerTick
	if budget < 1 {
		budget = 1
	}

	batches := 0
	repaired := 0
	saturated := false

	for chained := 0; chained < budget; chained++ {
		// The same non-blocking check the publish loop makes before every batch: claiming new
		// work while stopping would only lease rows and leave them.
		if !p.shouldClaimAnotherBatch(ctx) {
			break
		}

		done := pass(ctx)
		batches++
		repaired += done

		if done < p.repairBatchSize {
			// Drained, or every row of the batch failed — either way there is nothing this tick
			// can usefully chain onto. A pass that failed every row has already logged per row.
			break
		}

		// A full batch on the LAST permitted iteration means the budget, not the backlog, ended
		// the chain.
		if chained == budget-1 {
			saturated = true
		}
	}

	p.recordRepairOutcome(ctx, leg, repaired, batches, saturated)
}

// recordRepairOutcome publishes what one tick's chained repair achieved for one leg.
//
// Recorded on EVERY tick including the idle ones, and the zeros are the point: a saturation gauge
// left at 1 after the backlog cleared would keep an operator looking at a capacity problem that
// no longer exists, and a drain rate is only readable if the counter is known not to have stopped
// being incremented for want of a call.
//
// The BACKLOG gauge is deliberately not published here. It is a count of rows the relay has not
// claimed, which this function cannot know without a query the event-metrics collector already
// makes for blnk.outbox.pending — so the collector publishes it from the per-status counts it
// already holds, and this publishes only what the pass itself observed.
//
// Parameters:
//   - ctx context.Context: the tick context, used only as the metric recording context.
//   - leg string: which repair leg this describes.
//   - repaired int: rows that reached their destination this tick.
//   - batches int: how many chained passes ran.
//   - saturated bool: whether the per-tick budget ended the chain with a full batch.
func (p *EventRelayProcessor) recordRepairOutcome(ctx context.Context, leg string, repaired, batches int, saturated bool) {
	attributes := otelmetric.WithAttributes(attribute.String("leg", leg))

	if metrics.EventRepairsCompletedTotal != nil && repaired > 0 {
		metrics.EventRepairsCompletedTotal.Add(ctx, int64(repaired), attributes)
	}

	if metrics.EventRepairSaturated != nil {
		saturatedValue := int64(0)
		if saturated {
			saturatedValue = 1
		}

		metrics.EventRepairSaturated.Record(ctx, saturatedValue, attributes)
	}

	// Logged only when something happened, because this runs every poll interval and an idle
	// pass has nothing to say. Saturation IS something happening even at zero repaired: it means
	// the budget was spent on rows that all failed.
	if repaired == 0 && !saturated {
		return
	}

	fields := logrus.Fields{
		"leg":       leg,
		"repaired":  repaired,
		"batches":   batches,
		"saturated": saturated,
	}

	if saturated {
		logrus.WithFields(fields).Warn(
			"event relay: the repair budget for this leg was spent with work still outstanding; " +
				"raise RELAY_REPAIR_MAX_BATCHES_PER_TICK or RELAY_REPAIR_BATCH_SIZE if the backlog " +
				"is not falling fast enough",
		)

		return
	}

	logrus.WithFields(fields).Info("event relay: repaired outstanding rows for this leg")
}

// repairRowsConcurrently writes one claimed repair batch, bounded by repairConcurrency.
//
// # Ordering is preserved by construction, not by luck
//
// The rows are grouped by the SAME effective key the publisher hashes, through the same
// groupEventRowsByPartitionKey the publish loop uses, and each group is written sequentially by
// one goroutine. Two rows that must stay ordered therefore share a group and are never written at
// the same time — which matters for the legacy leg, whose enqueues carry a subscriber-visible
// order, and costs nothing for the dead-letter leg.
//
// Cancellation is honoured before every group AND between rows within a group, exactly as
// processBatch does: rows not yet attempted keep their lease and are re-claimed once it expires,
// which is the same mechanism that recovers a crashed relay.
//
// Parameters:
//   - ctx context.Context: cancelling it stops dispatching further rows.
//   - rows []model.EventOutbox: the claimed batch.
//   - repair func(context.Context, model.EventOutbox) bool: one row's repair, returning whether
//     it reached its destination. It must be safe for concurrent use across groups.
//
// Returns:
//   - int: how many rows were repaired.
func (p *EventRelayProcessor) repairRowsConcurrently(
	ctx context.Context,
	rows []model.EventOutbox,
	repair func(context.Context, model.EventOutbox) bool,
) int {
	width := p.repairConcurrency
	if width < 1 {
		width = 1
	}

	groups := groupEventRowsByPartitionKey(rows)
	permits := semaphore.NewWeighted(int64(width))

	var (
		wait     sync.WaitGroup
		mu       sync.Mutex
		repaired int
	)

	for _, group := range groups {
		if !publishingMayProceed(ctx) {
			break
		}

		// Acquired before spawning, so the number of in-flight writes is bounded by the permit
		// count rather than by the number of groups.
		if acquireErr := permits.Acquire(ctx, 1); acquireErr != nil {
			break
		}

		wait.Add(1)

		go func(group []model.EventOutbox) {
			defer wait.Done()
			defer permits.Release(1)

			done := 0
			for _, row := range group {
				if !publishingMayProceed(ctx) {
					break
				}

				if repair(ctx, row) {
					done++
				}
			}

			if done == 0 {
				return
			}

			mu.Lock()
			repaired += done
			mu.Unlock()
		}(group)
	}

	// Waiting here is what makes Stop's promise true: the tick cannot return, and so p.wg cannot
	// drain, until the writes in flight have finished.
	wait.Wait()

	return repaired
}

// shouldClaimAnotherBatch reports whether the relay may CLAIM MORE WORK.
//
// It answers no to either shutdown signal, because claiming a new batch while stopping would
// only lease rows and leave them. The select is non-blocking on purpose.
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
// CLAIMED. It consults the CONTEXT ONLY, and the asymmetry with shouldClaimAnotherBatch is
// deliberate:
//
//   - Stop is a GRACEFUL DRAIN. Its documented promise is to wait for work in flight, and an
//     already-leased batch is exactly that; abandoning it would strand those events until
//     their lease expired — 30 seconds of avoidable latency on every deploy.
//   - Context cancellation is an ABORT. The rows keep their lease and the next instance picks
//     them up, which is the mechanism that recovers a crash.
func publishingMayProceed(ctx context.Context) bool {
	return ctx.Err() == nil
}

// leaseDeadline is the instant this batch's claim lapses if nothing renews it, and its rows
// become claimable by anyone else.
//
// It is the plain expiry of the configured lease. The lease is honoured exactly as configured
// — it is also the crash-recovery latency, so lengthening it to suit the relay's own
// convenience would strand a dead relay's rows for longer than the operator asked for.
//
// # Why nothing derives a publish deadline from it
//
// A claim takes ONE lease over a whole batch and the rows are published concurrency-at-a-time,
// so a batch can outlast this instant: at the house defaults a hundred rows over eight workers
// is thirteen rounds of a produce call that may block for the writer's full ten-second timeout,
// against a thirty-second lease. A row still queued when the lease lapsed would be reclaimed by
// another instance and published by BOTH — a duplicate that arrives with no error recorded
// anywhere, suppressed only if the subscriber is doing its own idempotency. Three answers to
// that present themselves and only one of them holds.
//
// LENGTHENING THE LEASE to cover the worst-case batch is wrong twice over: the lease is the
// crash-recovery latency, so sizing it to a full batch strands a dead relay's rows for minutes
// instead of the configured seconds, and it silently overrules an operator who chose a short
// lease precisely to get fast reclaim.
//
// BOUNDING THE CLAIM SIZE to what one lease can publish is equally wrong: it makes throughput a
// function of the lease, and at concurrency 1 a thirty-second lease would permit two rows per
// claim — a hundred events per second against a requirement of five hundred.
//
// DEADLINING EACH PUBLISH at this instant looks like the answer and is not, because the lease
// does not stand still: renewLeaseWhileInFlight extends it for as long as the batch is in
// flight, so a deadline computed from claimedAt is stale as soon as the first renewal lands. A
// publish that would have completed safely is then cut short with a deadline error, its row
// returns to the claimable set, and another instance publishes it — the same duplicate, reached
// from the opposite direction, and now caused by the guard rather than prevented by it.
//
// So this instant SEEDS the heartbeat and bounds nothing directly. One produce round trip is
// bounded by eventRelayRowPublishBudget — the writer's own produce timeout, the honest worst
// case for a single publish — and the claim is watched rather than computed: the heartbeat is
// the only part of the relay that knows how long the batch has really been held, so it is what
// stops the batch publishing when the hold ends.
//
// Parameters:
//   - claimedAt time.Time: when the batch was claimed and its lease began.
//
// Returns:
//   - time.Time: the instant the lease lapses if it is never renewed.
func (p *EventRelayProcessor) leaseDeadline(claimedAt time.Time) time.Time {
	return claimedAt.Add(p.lockDuration)
}

// processBatch claims one batch of due outbox rows and publishes every one of them.
//
// The claim is FIFO by occurrence — the repository orders by occurred_at ascending, and
// re-sorts the rows the UPDATE returned because `UPDATE … RETURNING` does not preserve the
// inner ORDER BY — and it takes a lease plus one claim token for the batch. FOR UPDATE SKIP
// LOCKED is what lets several relay instances work disjoint batches without blocking each
// other, and no additional lock is taken here.
//
// The lease is RENEWED for as long as the batch is in flight. A batch may legitimately take
// longer than its own lease, and a lease that lapses under a relay still holding the rows
// invites a second instance to publish them again — see renewLeaseWhileInFlight.
//
// The dual-delivery decision is taken PER ROW, immediately before each legacy enqueue, by
// deliverLegacyWebhook. What is read here is only the window's state at the claim instant, and
// only so the batch log line says which regime the relay believes it is in. Deciding once for
// the whole batch would be wrong at the boundary: a batch claimed a second before the sunset
// takes time to publish, and every legacy enqueue after that instant would then be made on a
// decision that had already expired.
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
		withLoggableCause(nil, err).Error("failed to claim event outbox entries")

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

	// REPORTED, NOT DECIDED. This is the window's state at the moment the batch was
	// claimed, and it exists so the batch log line says which regime the relay believes
	// it is in. The DECISION is taken per row, immediately before each enqueue, by
	// deliverLegacyWebhook — see F12's reasoning there. A batch claimed one second
	// before the sunset takes time to publish, so a decision taken here would enqueue
	// legacy deliveries after the boundary had passed.
	windowAtClaim := p.windowStateAt(claimedAt)

	logrus.WithFields(logrus.Fields{
		"claimed":      len(rows),
		"window_state": windowAtClaim.String(),
	}).Infof("Processing %d event outbox entries", len(rows))

	// THE LEASE IS RENEWED WHILE THE BATCH IS IN FLIGHT, and this is not an optimisation.
	//
	// At the shipped defaults a claim takes 100 rows and publishes 8 at a time, so the batch
	// runs in 13 waves and one wave can occupy the writer's entire 10-second produce timeout.
	// The rows in the later waves therefore had their 30-second lease expire BEFORE their
	// publish was attempted — while this relay still held them and still intended to publish
	// them. Another instance claimed and published those rows, this one published them again
	// afterwards, and every transition this one attempted failed as a lost claim. The topic
	// got duplicates and neither process logged a defect.
	//
	// Renewal is preferred over the two alternatives because neither is free: a lease long
	// enough for the worst-case batch would leave a crashed relay's rows unclaimable for
	// minutes, and a batch small enough for the lease would cap throughput below the required
	// 500 events per second. A heartbeat keeps both.
	//
	// Recovery latency is UNCHANGED by it. Renewal stops the instant this batch finishes, and
	// a process that dies stops renewing by definition, so a crashed relay's rows still
	// become claimable one lease after it stopped.
	//
	// AND THE HEARTBEAT IS ALSO THE STOP SIGNAL. A renewal that keeps failing until the lease
	// has actually lapsed means these rows now belong to whoever claims them next, so the
	// publishing context below is cancelled and this batch stops mid-flight. Deriving a
	// deadline per publish from the lease instead cannot work once the lease is renewed: it
	// is computed before the renewals happen, so it aborts the very publishes the heartbeat
	// is keeping safe and hands their rows to another instance — the duplicate, reached from
	// the other side. See renewLeaseWhileInFlight.
	publishing, stopPublishing := context.WithCancel(ctx)
	defer stopPublishing()

	stopRenewals := p.renewLeaseWhileInFlight(
		ctx, rows[0].ClaimToken, len(rows), p.leaseDeadline(claimedAt), stopPublishing,
	)
	defer stopRenewals()

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
		if !publishingMayProceed(publishing) {
			logrus.WithField("event_id", group[0].EventID).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		// Acquire before spawning, so the number of in-flight publishes is bounded by the
		// permit count rather than by the number of groups.
		if acquireErr := permits.Acquire(publishing, 1); acquireErr != nil {
			withLoggableCause(logrus.WithField("event_id", group[0].EventID), acquireErr).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		batch.Add(1)

		go func(rows []model.EventOutbox) {
			defer batch.Done()
			defer permits.Release(1)

			// SEQUENTIALLY WITHIN THE GROUP. Every row here shares one partition key, so
			// this loop is the per-partition-key ordering guarantee in code.
			for _, row := range rows {
				// Cancellation is honoured BETWEEN rows as well as between groups. A group
				// is normally one row — the claim returns at most one per partition key — but
				// it is not bounded to one, and a group that kept publishing through an abort
				// would be the one place cancellation did not reach.
				if !publishingMayProceed(publishing) {
					return
				}

				p.processRow(publishing, row, claimedAt)
			}
		}(group)
	}

	// Waiting here is what makes Stop's promise true: the loop goroutine cannot return —
	// and so p.wg cannot drain — until the batch in flight has finished.
	batch.Wait()

	return len(rows)
}

// renewLeaseWhileInFlight starts the heartbeat that keeps a claimed batch's lease alive, and
// returns the function that stops it.
//
// # What it is for
//
// See the call site for the failure it removes. In short: a batch can legitimately take
// longer than its lease, and a lease that expires under a relay still holding the rows
// invites a second instance to publish them a second time.
//
// # Why it stops rather than being cancelled by the context
//
// The batch context is honoured too — a cancelled context ends the loop — but the returned
// stop function is what guarantees the goroutine cannot outlive the batch even on the paths
// where the context stays live for the rest of the process's life. It is idempotent, so the
// deferred call is safe however the batch ended.
//
// # Why the count matters
//
// A renewal that extends nothing means every row has reached a terminal state, so there is
// nothing left to protect and the heartbeat retires itself. That is the ordinary end of a
// batch, and stopping on it means the common case costs exactly one renewal round trip per
// lease-third rather than one per tick forever.
//
// A renewal FAILURE is logged at warning and the loop continues. One failure means nothing:
// the interval is a third of the lease, so two consecutive misses still leave a third of it to
// recover in. Aborting the batch on the first would abandon publishes that are in flight for
// what is usually a slow round trip.
//
// # Why it also decides when the claim is LOST
//
// The heartbeat is the only part of the relay that knows how long this batch has actually been
// held, so it is also the only part that can say when the hold has ended. Once the lease this
// relay last secured has elapsed, the rows are claimable by anyone and a publish still in
// flight would put a second copy of an event on the topic that another instance has already
// published — with no error recorded anywhere, because the bookkeeping is conditional on the
// claim token and simply fails.
//
// So on the tick where the held-until instant has passed with no successful renewal behind it,
// the heartbeat cancels the batch's publishing context and retires. In-flight publishes stop,
// no further rows are dispatched from that batch, and the rows return to the claimable set on
// their own. That is strictly better than the alternative of computing a deadline per publish
// from the lease, which cannot see the renewals and therefore aborts publishes the heartbeat is
// successfully protecting.
//
// The held-until instant is sampled BEFORE each renewal round trip rather than after, so the
// round trip's own latency shortens what this relay believes it holds instead of lengthening
// it. The database stamps locked_until from its own clock, which is at or after that sample.
//
// Parameters:
//   - ctx context.Context: the batch context. Cancellation ends the heartbeat.
//   - claimToken string: the token the claim issued — the identity of the whole batch, since
//     every terminal transition clears it from the rows that are done.
//   - claimed int: how many rows the batch started with, for the log line only.
//   - heldUntil time.Time: the instant the claim's ORIGINAL lease lapses, from leaseDeadline.
//     Each successful renewal moves it forward.
//   - onLeaseLost func(): called once, when the claim can no longer be held, to stop this
//     batch publishing. Must be safe to call from another goroutine; a context cancel is.
//
// Returns:
//   - func(): stops the heartbeat and waits for its goroutine to exit. Never nil, and safe to
//     call more than once.
func (p *EventRelayProcessor) renewLeaseWhileInFlight(
	ctx context.Context,
	claimToken string,
	claimed int,
	heldUntil time.Time,
	onLeaseLost func(),
) func() {
	if strings.TrimSpace(claimToken) == "" {
		// Nothing to renew against. Reachable only from a store that returned rows without
		// a token, which the repository never does; a no-op stop keeps the call site
		// unconditional.
		return func() {}
	}

	interval := p.lockDuration / eventRelayLeaseRenewalDivisor
	if interval < eventRelayMinLeaseRenewalInterval {
		interval = eventRelayMinLeaseRenewalInterval
	}

	done := make(chan struct{})

	var (
		stopOnce sync.Once
		finished sync.WaitGroup
	)

	finished.Add(1)

	go func() {
		defer finished.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				// Sampled before the round trip, so its latency shortens rather than
				// lengthens the hold this relay claims to have.
				issuedAt := p.now()

				// A detached context, for the same reason every other bookkeeping write
				// uses one: a renewal is work the relay already owes on rows it is holding,
				// and a shutdown mid-batch must not be the reason those rows lose their
				// lease early.
				renewal, cancel := detachedBookkeepingContext(ctx)
				renewed, err := p.store.RenewEventOutboxLease(renewal, claimToken, p.lockDuration)
				cancel()

				if err != nil {
					withLoggableCause(logrus.WithFields(logrus.Fields{
						"claim_token": claimToken,
						"claimed":     claimed,
						"lease":       p.lockDuration.String(),
						"held_until":  heldUntil.UTC().Format(time.RFC3339Nano),
					}), err).Warn(
						"event relay: renewing the lease on a batch in flight failed; the rows may be " +
							"reclaimed and republished by another instance, which is suppressed at the " +
							"subscriber on event_id",
					)

					if remaining := heldUntil.Sub(p.now()); remaining > 0 {
						// Still inside the lease this relay last secured, so the rows are
						// still its own and there are further ticks to recover in.
						//
						// Written as a subtraction rather than Before/After on purpose. This
						// file compares no instants the way a boundary decision does, so that
						// the one boundary in this pipeline — the deprecation window — keeps
						// exactly one home in event_sunset.go and cannot acquire a second
						// reading here. A lease is arithmetic on a duration this relay owns
						// outright, not a boundary anything else also decides.
						continue
					}

					logrus.WithFields(logrus.Fields{
						"claim_token": claimToken,
						"claimed":     claimed,
						"lease":       p.lockDuration.String(),
						"held_until":  heldUntil.UTC().Format(time.RFC3339Nano),
					}).Warn(
						"event relay: the lease on a batch in flight could not be held and has lapsed; " +
							"publishing is being stopped so this instance cannot put a second copy on " +
							"the topic behind whoever reclaims the rows",
					)

					onLeaseLost()

					return
				}

				if renewed == 0 {
					// Every row is terminal. Nothing is left to hold.
					return
				}

				heldUntil = issuedAt.Add(p.lockDuration)
			}
		}
	}()

	return func() {
		stopOnce.Do(func() { close(done) })
		finished.Wait()
	}
}

// recoverUnpreservedDeadLetters retries the dead-letter write for rows whose retry budget is
// spent and whose preservation did not succeed, and reports how many it repaired.
//
// # The limbo this ends
//
// When the dead-letter write fails — no transport, a broker outage, a topic that does not
// exist yet — the row is left at 'failed' with no dlt_topic. That state was a dead end in
// three directions at once: the ordinary claim excludes 'failed', replay accepts only
// 'dead_lettered', and the worker holding the claim token had already moved on. The event
// existed only as that row, listed in the dead-letter inventory and acted on by nothing.
//
// This pass is the route forward. It re-claims such rows with a fresh token and hands each one
// back to the dead-letter writer, which composes the same message and records the same
// terminal state it would have on the first attempt.
//
// # It WAITS for the hand-off lease, and that wait is the correctness property (PERF-P27)
//
// A row arrives here in one of two states that look identical in the table and are not: its
// hand-off has been ABANDONED, or its hand-off is IN FLIGHT in another replica right now.
// Nothing in the row distinguishes them — the claim token this pass replaces is the same token
// the live worker is holding — so the only thing that can tell them apart is the LEASE. The
// terminal transitions therefore retain one for a bounded hand-off window
// (deadLetterHandOffLease), and this claim's predicate skips a leased row. Without that, this
// pass raced the very worker it exists to replace: both published the same event to the same
// dead-letter topic, and the loser's MarkEventDeadLettered then failed on a superseded token,
// which reads in the log as a lost claim rather than as the duplicate it was. Waiting one lease
// costs a bounded delay on a genuinely abandoned hand-off and removes the duplicate entirely.
//
// # The cause it reports
//
// The row's own last_error, which the exhaustion arm recorded, wrapped so the metadata says
// what actually ended the event's retry budget rather than "recovered". A row with no recorded
// error — which should not happen, since the exhaustion arm always writes one — reports that
// its preservation is being retried, which is at least true.
//
// # Why it runs every tick
//
// The set it queries is empty in normal operation and the predicate is index-backed, so the
// cost of an idle pass is one cheap query per poll. Running it on a longer cycle would only
// delay the repair, and the delay is what the 15-minute dead-letter age alert measures.
//
// Parameters:
//   - ctx context.Context: cancels the claim and the writes.
//
// Returns:
//   - int: how many rows were successfully preserved on this pass.
func (p *EventRelayProcessor) recoverUnpreservedDeadLetters(ctx context.Context) int {
	if p.deadLetters == nil {
		return 0
	}

	rows, err := p.store.ClaimFailedEventOutboxForDeadLetter(ctx, p.repairBatchSize, p.lockDuration)
	if err != nil {
		withLoggableCause(nil, err).Error(
			"event relay: could not claim events awaiting dead-letter preservation; they stay in the " +
				"dead-letter inventory and are retried on a later poll",
		)

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	logrus.WithField("claimed", len(rows)).Info(
		"event relay: retrying the dead-letter preservation of events whose earlier write failed",
	)

	// Written repairConcurrency groups at a time rather than one row after another (PERF-M06).
	// Every row here needs its own Kafka write, so a sequential loop bounded the whole recovery
	// at one broker acknowledgement at a time — about 100 rows a second — which is an order of
	// magnitude under what an outage's backlog needs. Ordering is preserved by grouping on the
	// same effective key the publisher hashes; see repairRowsConcurrently.
	return p.repairRowsConcurrently(ctx, rows, p.preserveOneDeadLetter)
}

// preserveOneDeadLetter retries the `<topic>.dlt` write for one claimed row.
//
// Split out of recoverUnpreservedDeadLetters so the row's work is a value the bounded writer can
// call, and so the two repair legs have the same shape: claim, then write the batch through
// repairRowsConcurrently.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - row model.EventOutbox: the claimed row, carrying a fresh claim token.
//
// Returns:
//   - bool: true when the event is now preserved on its dead-letter topic and replayable.
func (p *EventRelayProcessor) preserveOneDeadLetter(ctx context.Context, row model.EventOutbox) bool {
	outcome, dltErr := p.deadLetters.DeadLetter(ctx, row, unpreservedDeadLetterCause(row))
	if dltErr != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts)), dltErr).Warn(
			"event relay: retrying the dead-letter preservation of this event failed again; it " +
				"stays in the dead-letter inventory and is retried once its lease expires",
		)

		return false
	}

	logrus.WithFields(p.rowFields(row, row.Attempts)).WithFields(logrus.Fields{
		"dlt_topic": outcome.DeadLetterTopic,
		"attempts":  outcome.Metadata.AttemptCount,
	}).Warn(
		"event relay: an event whose dead-letter write had failed is now preserved on its " +
			"dead-letter topic and is replayable",
	)

	return true
}

// recoverOwedLegacyWebhooks finishes the LEGACY leg of rows whose Kafka leg has finished and
// whose webhook enqueue never succeeded.
//
// SUNSET: deleted with the rest of the dual-delivery branch.
//
// # The events this exists for, and why nothing else reaches them
//
// The relay enqueues the legacy webhook before it publishes, and a failed enqueue is
// deliberately swallowed so a webhook receiver being down cannot spend a Kafka retry attempt.
// The caller then settles the row. Three settlements are possible and only ONE of them used to
// carry the outstanding webhook forward:
//
//   - The publish SUCCEEDED. settleAfterKafkaSuccess routes the row through
//     MarkEventWebhookPending, which moves it to webhook_pending — inside the main claim
//     predicate — so the relay comes back for the webhook alone. This case was covered.
//   - The publish FAILED with budget left. The row returns to pending and the whole row is
//     retried, webhook included. Also covered.
//   - The publish FAILED on the attempt that spent the budget. The row goes failed and then
//     dead_lettered: terminal, token cleared, outside every claim predicate, with
//     webhook_dispatched still FALSE. THE WEBHOOK WAS DISCARDED, silently, with one warning
//     line as the only trace — and in the one circumstance where the webhook is the only
//     transport that might still work, because the broker being unreachable is why the Kafka
//     leg failed at all.
//
// This pass closes the third case. ClaimPendingWebhookDeliveries admits all three Kafka end
// states, so the legacy obligation is durable and independently reclaimable no matter what the
// Kafka leg did, which is what "both transports run for thirty days" has to mean to be worth
// promising.
//
// # It does not touch the Kafka leg's state, ever
//
// MarkWebhookDispatched sets a flag; MarkEventLegacyWebhookAttempted increments a counter and
// a due instant. Neither moves the status, so a dead-lettered event stays dead-lettered and
// replayable, and a dispatched event is never re-published. That separation is the whole
// reason the two legs have their own columns.
//
// # The window is consulted here, exactly as it is per row in deliverLegacyWebhook
//
// Outside the window there is no leg to finish: before the start the relay refuses to run, and
// from the sunset onwards the promise has ended. Skipping the pass entirely is cheaper than
// claiming rows and discarding them, and it means the sunset silences this pass on the same
// boundary it silences the inline enqueue.
//
// Parameters:
//   - ctx context.Context: cancels the claim and the enqueues.
//
// Returns:
//   - int: how many outstanding webhook legs were enqueued on this pass.
func (p *EventRelayProcessor) recoverOwedLegacyWebhooks(ctx context.Context) int {
	if p.legacy == nil || !p.legacyLegDue(p.now()) {
		return 0
	}

	rows, err := p.store.ClaimPendingWebhookDeliveries(ctx, p.repairBatchSize, p.lockDuration)
	if err != nil {
		withLoggableCause(nil, err).Error(
			"event relay: could not claim events whose legacy webhook leg is still owed; the legs " +
				"stay recorded on their rows and are retried on a later poll",
		)

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	logrus.WithField("claimed", len(rows)).Info(
		"event relay: finishing the legacy webhook leg of events whose earlier enqueue failed",
	)

	// Bounded-concurrent for the same reason the dead-letter leg is (PERF-M06), and grouped on
	// the effective key so two enqueues of one aggregate keep their order — which matters more
	// here than on the dead-letter leg, because a subscriber still receiving webhooks sees this
	// order directly.
	return p.repairRowsConcurrently(ctx, rows, func(ctx context.Context, row model.EventOutbox) bool {
		return p.recoverOneOwedLegacyWebhook(ctx, row)
	})
}

// recoverOneOwedLegacyWebhook enqueues one outstanding legacy delivery and records the outcome.
//
// SUNSET: deleted with the rest of the dual-delivery branch.
//
// The bytes handed to the transport are row.Payload — the stored bytes, and the same bytes the
// Kafka leg carried — so payload identity holds on the recovery path exactly as it does on the
// ordinary one. The enqueue carries the event id as its task identity, so a re-claim after a
// lost lease is refused by the queue as a duplicate rather than delivered twice.
//
// Parameters:
//   - ctx context.Context: the pass context. The recording transitions run detached, so a
//     shutdown cannot leave an enqueued webhook unrecorded.
//   - row model.EventOutbox: the claimed row, carrying its stored payload and a fresh token.
//
// Returns:
//   - bool: true when the delivery was enqueued.
func (p *EventRelayProcessor) recoverOneOwedLegacyWebhook(ctx context.Context, row model.EventOutbox) bool {
	// The attempt label is the WEBHOOK's count, not the Kafka one: this row's Kafka leg is
	// finished and its attempt number would mislead an operator reading the line.
	fields := p.rowFields(row, row.Attempts)
	fields["webhook_attempts"] = row.WebhookAttempts
	fields["webhook_max_attempts"] = p.rowMaxAttempts(row)
	fields["row_status"] = row.Status

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if err := p.legacy.EnqueueLegacyWebhookDelivery(row.EventID, row.Payload); err != nil {
		// The backoff is indexed by the WEBHOOK attempt count so the two legs walk the same
		// curve while spending separate budgets, exactly as deferLegacyLeg does.
		retryAfter := p.retry.backoffFor(row.WebhookAttempts + 1)

		outcome, markErr := p.store.MarkEventLegacyWebhookAttempted(
			bookkeeping, row.ID, row.ClaimToken, retryAfter,
		)
		if markErr != nil {
			// The row keeps its lease and returns to this pass's candidate set when it
			// expires. Nothing is lost, and nothing about the Kafka leg is affected.
			withLoggableCause(logrus.WithFields(fields), markErr).Error(
				"event relay: a failed legacy webhook recovery could not be recorded; the row keeps " +
					"its lease and the leg is retried when it expires",
			)

			return false
		}

		fields["webhook_attempts"] = outcome.WebhookAttempts
		fields["reason"] = relayLogReason(err)

		if outcome.Abandoned {
			logrus.WithFields(fields).Error(
				"event relay: the legacy webhook leg for this event is ABANDONED after exhausting its " +
					"own enqueue budget. The webhook will never be delivered; any subscriber still " +
					"consuming webhooks has missed this event",
			)

			return false
		}

		fields["retry_after"] = retryAfter.String()
		logrus.WithFields(fields).Warn(
			"event relay: recovering the legacy webhook leg of this event failed again; it is " +
				"retried once its backoff elapses",
		)

		return false
	}

	if err := p.store.MarkWebhookDispatched(bookkeeping, row.ID, row.ClaimToken); err != nil {
		// The task IS enqueued and will be delivered; only the marker is missing. A later
		// pass re-enqueues under the same task identity, which the queue refuses as a
		// duplicate, so this is an observability gap rather than a delivery defect.
		withLoggableCause(logrus.WithFields(fields), err).Warn(
			"event relay: a recovered legacy webhook was enqueued but the row could not be marked; " +
				"a re-enqueue is suppressed by the task identity",
		)
	}

	logrus.WithFields(fields).Info(
		"event relay: the legacy webhook leg of an event whose earlier enqueue had failed is now " +
			"enqueued; the event's Kafka state is untouched",
	)

	return true
}

// unpreservedDeadLetterCause reconstructs the failure to report for a row being repaired.
//
// The row's last_error is what ended its retry budget, and the failure metadata attached to
// the dead-letter message must say that rather than describing the repair. The error is
// rebuilt from the stored text because the original error value is long gone — this row may
// have been recorded by a different process entirely.
//
// Parameters:
//   - row model.EventOutbox: the row being repaired.
//
// Returns:
//   - error: never nil, so the metadata always states a reason.
func unpreservedDeadLetterCause(row model.EventOutbox) error {
	if reason := strings.TrimSpace(row.LastError); reason != "" {
		return errors.New(reason)
	}

	return errors.New("the retry budget was exhausted and the dead-letter write is being retried")
}

// groupEventRowsByPartitionKey partitions a claimed batch into per-partition-key groups,
// preserving the claim's order within each group and between groups.
//
// Kafka orders messages within a PARTITION only, and the partition is chosen from the message
// key, so two events whose relative order matters share a partition key. Ordering therefore
// survives concurrency exactly as long as no two rows with the same key are published at the
// same time or out of occurrence order — which holds here for TWO INDEPENDENT REASONS:
//
//  1. The claim already guarantees it. ClaimPendingEventOutbox returns at most one row per
//     EFFECTIVE key, across every concurrent instance, so in practice each group has one row.
//     Its predicate resolves that key by the SAME rule this function does — eventOutboxEffectiveKeySQL
//     is model.EffectivePartitionKey in SQL — so the two agree by construction rather than by
//     coincidence.
//  2. This function guarantees it locally. Two rows sharing a key would land in the same group
//     and be published in claim order by one goroutine rather than racing in two.
//
// The redundancy is deliberate, and it is what limits the blast radius of a divergence between
// the two: reason 2 holds WITHIN one process whatever the SQL does, so a claim that serialised
// on the wrong key could only ever reorder rows across SEPARATE replicas. That is exactly the
// defect PERF-C02 was — invisible on a single-relay test and real in production — which is why
// relying on reason 2 alone is not enough either.
func groupEventRowsByPartitionKey(rows []model.EventOutbox) [][]model.EventOutbox {
	groups := make([][]model.EventOutbox, 0, len(rows))
	indexByKey := make(map[string]int, len(rows))

	for _, row := range rows {
		// GROUPED BY THE KEY THE PUBLISH ACTUALLY USES, not by the stored column.
		//
		// The two differ on a row whose partition key was derived before its ledger was known,
		// and the publisher prefers the ledger (requirement R-6). Grouping on the column there
		// would put two rows destined for ONE Kafka partition into two groups and publish them
		// concurrently — losing the ordering this function exists to preserve, on exactly the
		// rows where the discrepancy lives. See model.EffectivePartitionKey.
		//
		// A blank key would collapse every unkeyed row into one group and serialise them.
		// It cannot occur on a persisted row — the capture path guarantees a value through
		// a documented fallback chain — so the row id is used to keep such a row in a group
		// of its own rather than inventing a shared bucket for a case that means the data
		// is already wrong.
		key := row.EffectiveKey()
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
// There is no retry loop here, by design: a failed attempt records now + the computed backoff
// in next_attempt_at and releases the lease, and the claim predicate refuses the row until it
// is due, so the next attempt is a later claim by this instance or another. Sleeping the delay
// instead would hold a semaphore permit and half the 30-second lease doing nothing, and a
// longer configured schedule would outlive the lease and let a second instance reclaim the row
// mid-sleep and publish it twice.
//
// THE LEGACY WEBHOOK LEG GOES FIRST, for two reasons. It needs the claim token, which
// MarkEventDispatched clears as it moves the row to its terminal state. And it must not be
// conditional on the Kafka publish succeeding: during the window the legacy transport is what
// unmigrated subscribers are consuming, so suppressing it during a broker outage would turn a
// Kafka problem into an outage for them.
//
// # THE TWO LEGS REACH THEIR TERMINAL STATE INDEPENDENTLY
//
// A row is finished when BOTH legs are, and the two can finish on different claims. Three
// combinations are possible after one pass, and each has its own transition:
//
//   - Kafka ok, legacy ok (or not owed) → MarkEventDispatched. Terminal.
//   - Kafka ok, legacy enqueue FAILED → MarkEventWebhookPending. The Kafka leg is recorded
//     in kafka_dispatched_at, the row stays claimable, and the next claim delivers ONLY
//     the webhook because that column tells this function to skip the publish. Before this
//     existed the row was marked dispatched regardless, and since dispatched is outside
//     the claim predicate the webhook was never retried and never delivered — silently,
//     for a transport subscribers had been told would keep working.
//   - Kafka FAILED → recordFailedAttempt, whatever the legacy leg did. The legacy leg is
//     retried on the next claim through webhook_dispatched, and it costs no Kafka attempt.
//
// A row whose kafka_dispatched_at is already set publishes NOTHING: its position in the
// partition is fixed, and republishing to retry a deprecated-transport failure would put a
// duplicate on the topic for no benefit.
//
// Parameters:
//   - ctx context.Context: the batch's PUBLISHING context. It cancels on shutdown and on the
//     heartbeat losing the claim, and either way the publish stops. Bookkeeping the relay
//     already owes runs on a detached context — see detachedBookkeepingContext.
//   - row model.EventOutbox: the claimed row, carrying its claim token.
//   - claimedAt time.Time: when the batch was claimed, for the publish-duration histogram.
func (p *EventRelayProcessor) processRow(
	ctx context.Context,
	row model.EventOutbox,
	claimedAt time.Time,
) {
	// THE RELAY'S OWN SPAN over everything one claimed row costs: the legacy leg, the Kafka
	// publish, the retry decision, the dead-letter hand-off and the durable transition. The
	// publisher's producer span nests inside it, so a trace shows the publish in the context of
	// the row handling around it rather than on its own.
	//
	// It is LINKED to the trace that captured the event, not parented by it, for the reasons
	// linkToCapturedTrace sets out — chiefly that this work happens a poll interval and up to
	// five backoff waits after the request ended, so parenting would report a millisecond
	// request as a half-minute one.
	//
	// Named for the operation rather than the destination, unlike the producer span: this row
	// may reach two transports and may reach neither, so naming it after the topic would
	// describe only part of what the span covers.
	ctx, span := tracer.Start(ctx, "relay.process_event_outbox_row", linkToCapturedTrace(row)...)
	defer span.End()

	// The attempt this publish represents. attempts counts the failures RECORDED so far —
	// the claim does not increment it, MarkEventFailed does — so the attempt now under way
	// is one past it. Getting this wrong would misreport the backoff schedule and the
	// attempt metric label together.
	attempt := row.Attempts + 1

	// Bounded and hashed on the same terms as the producer span's: the topic through the
	// bounded label, the event type through its own, the event id in full because it is
	// Blnk-generated and is what an operator searches by. The attempt number makes a retry
	// distinguishable from a first delivery without opening the row.
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", boundedTopicLabel(row.Topic)),
		attribute.String("messaging.message.id", row.EventID),
		attribute.String("blnk.event.type", boundedEventTypeLabel(row.EventType)),
		attribute.Int("blnk.publish.attempt", attempt),
		attribute.Int("blnk.publish.max_attempts", p.rowMaxAttempts(row)),
	)

	// The legacy leg first, and its outcome kept: whether the webhook is still owed
	// decides which terminal transition this row takes below. The sunset boundary is
	// evaluated inside, per row, immediately before the enqueue.
	legacyLeg, legacyErr := p.deliverLegacyWebhook(ctx, row)

	// A row whose Kafka leg is already recorded is here ONLY for its outstanding webhook.
	// Republishing it would duplicate a message that is already on the topic — the whole
	// reason the two legs are tracked separately — so the publish is skipped and the row
	// is driven straight to its terminal decision.
	if row.KafkaDispatchedAt != nil {
		// NO COORDINATE is supplied on this pass, and that is deliberate: nothing was
		// published, so there is no new record to name — and the row already carries the
		// coordinate of the record its earlier successful publish produced. Both marking
		// transitions COALESCE the coordinate for exactly this case, so passing the zero
		// value leaves the stored one intact rather than erasing it.
		//
		// published is FALSE for the same reason, and it is what keeps EventsPublishedTotal
		// at one increment per event: this row's Kafka delivery was counted on the pass that
		// recorded it, and counting it again on the pass that finally settles the webhook
		// would report one event twice. See recordDurableEventDelivery.
		p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, model.BrokerRecord{}, false)

		return
	}

	// The publish is bounded so it cannot complete on a claim this relay no longer holds, and
	// the bound has TWO parts because there are two different questions to answer.
	//
	// The timeout here is the worst case for ONE produce round trip, and nothing more. It is
	// deliberately NOT derived from the lease: the lease is renewed for as long as the batch is
	// in flight (renewLeaseWhileInFlight), so a lease-derived deadline computed once at the
	// start of a publish is stale the moment the first renewal lands — and it would abort
	// exactly the publish the heartbeat exists to protect, releasing the row for another
	// instance to publish a second time. That is the duplicate, arrived at from the opposite
	// direction.
	//
	// The other part is the claim itself, and it is WATCHED rather than computed: ctx is the
	// batch's publishing context, which the heartbeat cancels the moment it can no longer hold
	// the lease. So a relay that loses its claim stops publishing within a renewal interval,
	// whatever the lease is set to, and one that keeps it publishes for as long as it takes.
	publishCtx, cancelPublish := context.WithTimeout(ctx, eventRelayRowPublishBudget)
	defer cancelPublish()

	// The request is built by the row-to-request conversion the dead-letter write and the
	// replay both use, rather than assembled here. That is what puts THE STORED CANONICAL
	// ENVELOPE on the wire: PublishRequestFromOutbox carries event_raw, so the bytes this
	// publish sends are the bytes composed once at capture, and a retry, the dead-letter copy
	// and a replay of the same row are byte-identical by construction rather than by the
	// serialiser happening not to have changed between them. Assembling the envelope here
	// re-composed it on every attempt, which produced the same bytes today and silently
	// different bytes the first time an envelope member was added or reordered.
	//
	// It also settles the topic and the key from the ROW, so a stored row is published to the
	// destination it recorded even if the topic-naming configuration has changed since, and
	// the key on the wire is the same value the claim serialised dispatch on.
	request := PublishRequestFromOutbox(row, attempt)
	// The budget is passed so the publisher can tell a failure that still has attempts left
	// from the one that spent the last of them, and so its log line reads "attempt 3 of 5".
	request.MaxAttempts = p.rowMaxAttempts(row)
	request.Purpose = PublishPurposeOriginal
	request.ClaimedAt = claimedAt

	result, err := p.publisher.PublishToTopic(publishCtx, request)

	p.logAttempt(row, attempt, result, err)

	if err != nil {
		// The Kafka leg failed, so this row keeps its Kafka retry budget and is retried as
		// a whole. Whatever the legacy leg did is deliberately NOT settled here: an
		// enqueue that succeeded is recorded by webhook_dispatched and will not repeat,
		// and one that failed is retried on the next claim without having cost a Kafka
		// attempt. Recording a webhook failure against a row that is going to be
		// re-published anyway would spend the legacy budget on a Kafka outage.
		//
		// THE PUBLISHER'S OWN VERDICT travels with the failure. It has already decided
		// whether another attempt could succeed, and a permanent failure — an oversized
		// envelope, bytes that are not valid JSON, a destination outside the namespace Blnk
		// owns — must exhaust the row now rather than after the whole backoff schedule has
		// rediscovered it. A webhook still owed when the row goes terminal is not abandoned
		// by that: recoverOwedLegacyWebhooks reaches the row in every Kafka end state.
		p.recordFailedAttempt(ctx, row, attempt, result, err)

		return
	}

	p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, result.Record, true)
}

// settleAfterKafkaSuccess drives a row whose KAFKA leg is complete to the right state,
// which depends entirely on what the legacy leg did.
//
// SUNSET: this function collapses to a single MarkEventDispatched call when the legacy leg
// is deleted, and the legacyLegOutcome parameter goes with it.
//
// # AT-LEAST-ONCE, EXPLICITLY
//
// The broker has the event; the row does not yet say so. A crash here — or a failure of
// the single statement below — leaves the row claimable once its lease expires and the
// event is published a second time. That duplicate is suppressed at the subscriber on
// event_id, which is unique per event and is the documented idempotency key. The reverse
// order would lose events instead, which nothing can recover.
//
// # Why an owed webhook does not simply mark the row dispatched
//
// dispatched is outside the claim predicate. A row marked dispatched with its webhook
// still owed is a webhook nobody will ever enqueue again, and the only trace is one
// warning line. MarkEventWebhookPending instead records the Kafka leg separately and
// leaves the row claimable for the webhook alone, with its own bounded budget.
//
// Parameters:
//   - ctx context.Context: the batch context; the transition itself runs detached.
//   - row model.EventOutbox: the claimed row, carrying its claim token.
//   - attempt int: the 1-based Kafka attempt this pass represents, for the log fields.
//   - legacyLeg legacyLegOutcome: what the legacy enqueue did on this pass.
//   - legacyErr error: why it failed, when it did. Recorded in last_error.
//   - record model.BrokerRecord: where the broker put the message, persisted so the row names
//     the record it produced (OBS-02). The zero value on a webhook-only pass, where nothing
//     was published and the row already carries its coordinate.
//   - published bool: whether THIS pass published to Kafka. True from the publishing path,
//     false on a webhook-only pass. It decides one thing only — whether the durable transition
//     below counts a delivered event — and it is what keeps EventsPublishedTotal at exactly
//     one increment per event across a row that finishes its two legs on different claims.
func (p *EventRelayProcessor) settleAfterKafkaSuccess(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	legacyLeg legacyLegOutcome,
	legacyErr error,
	record model.BrokerRecord,
	published bool,
) {
	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if legacyLeg == legacyLegOwed {
		p.deferLegacyLeg(bookkeeping, row, attempt, legacyErr, record, published)

		return
	}

	if markErr := p.store.MarkEventDispatched(bookkeeping, row.ID, row.ClaimToken, record); markErr != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), markErr).Error(
			"event relay: the event was published but its outbox row could not be marked dispatched; " +
				"it will be republished when its lease expires and must be suppressed on event_id",
		)

		// NOT COUNTED. The broker has the event but nothing durable says so, and this row is
		// going to be published again when its lease expires — counting here and again after
		// that republish would report one event twice. See recordDurableEventDelivery.
		return
	}

	// THE DURABLE TRANSITION HAS COMMITTED, which is the moment the delivery is recorded and
	// therefore the only moment it may be counted. Conditional on this pass having actually
	// published, so a webhook-only pass settling an earlier delivery does not count it twice.
	if published {
		recordDurableEventDelivery(bookkeeping, row.Topic, row.EventType)
	}

	// THE ONE PER-EVENT DELIVERY COUNT (PERF-P21), recorded here and only here for this arm.
	//
	// After the transition, never before it: the transition is conditional on the claim token
	// and clears it, so it succeeds for one worker once in an event's whole life. A republish
	// after a crash writes to the broker again — and increments EventsPublishedTotal again —
	// but cannot reach this line, because the row it would have to move is already dispatched.
	// Incrementing before the transition, or on the broker acknowledgement, is exactly what
	// made the published counter a count of writes rather than of events.
	recordEventDispatched(bookkeeping, row)
}

// recordEventDispatched increments the per-event delivery count for a row that has just
// reached the dispatched state.
//
// It exists as one function called from the two arms that reach that state — the ordinary
// MarkEventDispatched and MarkEventWebhookPending's abandon arm — so the attribute set and the
// once-per-event rule are stated once. A third caller would be a second increment for one
// event and would silently restore the defect this counter exists to fix; there is deliberately
// no other.
//
// Parameters:
//   - ctx context.Context: the detached bookkeeping context, so the increment is not lost to a
//     cancelled batch context after the row has already been moved.
//   - row model.EventOutbox: the row that reached dispatched, read for its topic and type.
func recordEventDispatched(ctx context.Context, row model.EventOutbox) {
	metrics.EventsDispatchedTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(row.Topic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(row.EventType)),
	))
}

// deferLegacyLeg records a Kafka leg that is done alongside a legacy webhook leg that is
// still owed, and reports the arm the datasource chose.
//
// SUNSET: deleted with the legacy leg.
//
// The backoff reuses the Kafka retry schedule, indexed by the WEBHOOK attempt count rather
// than the Kafka one, so the two legs back off on the same curve while spending separate
// budgets. Reusing the curve is deliberate: a queue that is unreachable and a broker that
// is unreachable fail for the same kinds of reason and recover on the same kinds of
// timescale, and a second configurable schedule for a transport that is being retired
// would be a knob nobody should have to learn.
//
// The abandon arm is logged at ERROR because it is the only notice that a webhook promised
// for the migration window will never be delivered. The retry arm is a warning, matching
// the enqueue failure it follows.
//
// Parameters:
//   - ctx context.Context: the detached bookkeeping context.
//   - row model.EventOutbox: the claimed row, carrying its claim token.
//   - attempt int: the Kafka attempt number, for the log fields.
//   - cause error: the enqueue failure. May be nil, in which case the reason is unstated.
//   - record model.BrokerRecord: where the broker put the message. Persisted alongside the
//     Kafka leg's completion, so a row waiting on its webhook still names its record and the
//     zero-loss audit can account for it.
//   - published bool: whether THIS pass published to Kafka. This arm is a durable transition
//     that records the Kafka leg, so a delivered event is counted here as well as on the
//     dispatched arm — an event whose webhook is still owed HAS been delivered to Kafka, and a
//     counter that waited for the webhook would under-report every event during a queue
//     outage, and never count one whose webhook is ultimately abandoned.
func (p *EventRelayProcessor) deferLegacyLeg(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
	record model.BrokerRecord,
	published bool,
) {
	reason := "the legacy webhook enqueue failed"
	if cause != nil {
		reason = relayFailureReason(cause)
	}

	// row.WebhookAttempts is the count BEFORE this failure, so the attempt now being
	// recorded is one past it — the same relationship attempt has to row.Attempts.
	retryAfter := p.retry.backoffFor(row.WebhookAttempts + 1)

	outcome, err := p.store.MarkEventWebhookPending(
		ctx, row.ID, row.ClaimToken, reason, retryAfter, record,
	)
	if err != nil {
		// The row keeps its lease and returns to the claimable set when it expires, so
		// nothing is lost: the Kafka publish is not repeated, because kafka_dispatched_at
		// was already stamped by whichever earlier pass succeeded, or will be stamped by
		// the next pass's own success.
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
			"event relay: the event was published to Kafka but its outstanding legacy webhook " +
				"leg could not be recorded; the row keeps its lease and is retried when it expires",
		)

		// NOT COUNTED: nothing durable records this delivery yet, and the row will be
		// re-settled on a later claim which is where the increment belongs.
		return
	}

	// THE DURABLE TRANSITION HAS COMMITTED. kafka_dispatched_at now names this delivery, so
	// the event is counted here — once — whether the webhook that follows it succeeds, is
	// retried or is ultimately abandoned.
	if published {
		recordDurableEventDelivery(ctx, row.Topic, row.EventType)
	}

	fields := p.rowFields(row, attempt)
	fields["webhook_attempts"] = outcome.WebhookAttempts
	fields["webhook_max_attempts"] = p.rowMaxAttempts(row)
	fields["reason"] = reason

	if outcome.Abandoned {
		// THE SECOND ARM THAT REACHES dispatched (PERF-P21). The abandon arm moves the row to
		// dispatched with the webhook given up on, so this event's delivery is now durably
		// recorded and it must be counted — omitting it here would under-report deliveries by
		// exactly the number of events whose legacy leg was abandoned, and make the
		// dead-letter rate look higher than it is.
		recordEventDispatched(ctx, row)

		logrus.WithFields(fields).Error(
			"event relay: the legacy webhook leg for this event is ABANDONED after exhausting its " +
				"own enqueue budget. The event IS on its Kafka topic; the webhook will never be " +
				"delivered. Any subscriber still consuming webhooks has missed this event",
		)

		return
	}

	fields["retry_after"] = retryAfter.String()
	logrus.WithFields(fields).Warn(
		"event relay: the event is published to Kafka and its legacy webhook leg is still owed; " +
			"the row stays claimable for the webhook alone and will not be republished",
	)
}

// legacyLegDue reports whether the legacy webhook leg must run for an event being
// dispatched at instant, through the injectable seam so a test can pin the clock.
//
// A nil seam resolves to FALSE — no dual delivery — rather than panicking. That is the
// fail-closed answer: a processor with no window decision cannot be trusted to know
// whether the sunset has passed, and enqueuing onto a retired transport is worse than
// not enqueuing onto a live one, which the relay's startup obstacle already prevents.
//
// SUNSET: deleted with the legacy leg.
//
// Parameters:
//   - instant time.Time: the instant to evaluate.
//
// Returns:
//   - bool: true only when the instant is inside the configured window.
func (p *EventRelayProcessor) legacyLegDue(instant time.Time) bool {
	if p == nil || p.dualDeliveryActive == nil {
		return false
	}

	return p.dualDeliveryActive(instant)
}

// windowStateAt resolves where an instant falls relative to the dual-delivery window,
// through the injectable seam so a test can pin it.
//
// A nil seam resolves to WebhookWindowUnavailable rather than panicking: a processor built
// as a struct literal — which several tests do — must still be able to log a batch.
//
// Parameters:
//   - instant time.Time: the instant to place.
//
// Returns:
//   - WebhookWindowState: the window's state at that instant.
func (p *EventRelayProcessor) windowStateAt(instant time.Time) WebhookWindowState {
	if p == nil || p.windowState == nil {
		return WebhookWindowUnavailable
	}

	return p.windowState(instant)
}

// rowMaxAttempts returns the retry budget in force for a row.
//
// The ROW's own budget wins, because it is what MarkEventFailed's in-SQL exhaustion decision
// is measured against; reporting the configured value while the database enforced another
// would make the logs and the metric label disagree with the outcome. The configured value is
// the fallback for a row that states none.
func (p *EventRelayProcessor) rowMaxAttempts(row model.EventOutbox) int {
	if row.MaxAttempts > 0 {
		return row.MaxAttempts
	}

	if p.retry.maxAttempts > 0 {
		return p.retry.maxAttempts
	}

	return 1
}

// recordFailedAttempt records one failed publish attempt, schedules the retry, and hands off
// to the dead-letter path when nothing further will be tried.
//
// # TWO GATES, in this order, answering different questions
//
// The FIRST gate is the publisher's verdict, read through PublishResult.PermanentFailure. It
// answers "can any further attempt succeed?", which only the code that saw the broker's answer
// can know: an unauthorised principal, a destination outside the topic catalogue, bytes that
// will never parse, a message over the size limit. None of those change with time, so the row
// goes straight to its terminal state and the event appears in the dead-letter inventory now
// rather than after the whole backoff schedule. This gate used to be MISSING: the verdict was
// computed, logged and then ignored, so a permanent failure cost five broker round trips and
// ~17 seconds per event while the log said "retryable=false" on one line and "scheduled for
// another attempt" on the next.
//
// The SECOND gate is MarkEventFailed's in-SQL budget check, unchanged and still authoritative
// for a retryable failure: the retry-versus-exhaustion decision is taken inside its UPDATE,
// which is what stops two instances racing on one row from both concluding they were the last
// attempt and putting two copies on the dead-letter topic.
//
// The order is the point. The first gate is about the event's PROSPECTS and the second about
// its BUDGET, and a failure that can never succeed must not have to exhaust a budget before it
// is recognised as one.
//
// A row whose claim was lost is abandoned rather than retried — another instance owns it now —
// and a row whose bookkeeping merely failed keeps its lease and returns to the claimable set
// when it expires, so nothing is dropped either way.
//
// Parameters:
//   - ctx context.Context: used for the dead-letter publish. The bookkeeping transition runs
//     on a detached context.
//   - row model.EventOutbox: the claimed row.
//   - attempt int: the 1-based attempt that just failed.
//   - result PublishResult: the attempt's observability record, read ONLY for its permanence
//     verdict. A zero value — which is what a publisher that classifies nothing produces —
//     falls through to the budgeted path, and that conservative default is deliberate: the
//     publisher is an interface seam, so "no verdict" must never be read as "give up". Both
//     gates below enforce that: PermanentFailure and publishFailureIsTerminal are each
//     affirmative, so neither can be satisfied by an unpopulated result.
//   - cause error: the publish failure.
func (p *EventRelayProcessor) recordFailedAttempt(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	result PublishResult,
	cause error,
) {
	if result.PermanentFailure() {
		p.recordPermanentFailure(ctx, row, attempt, cause)

		return
	}

	retryAfter := p.retry.backoffFor(attempt)
	reason := relayFailureReason(cause)
	terminal := publishFailureIsTerminal(result, cause)

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	// THE RELAY'S OWN LOCK DURATION IS THE HAND-OFF LEASE. On the exhaustion arm the row is
	// held under it while this worker writes the event to its `<topic>.dlt` sibling, so the
	// repair pass cannot reclaim the row and write a second copy; on the retry arm it is
	// ignored and the lease is released. Using the same duration the claim uses keeps one
	// value governing the whole ownership window.
	outcome, err := p.store.MarkEventFailed(bookkeeping, row.ID, row.ClaimToken, reason, retryAfter, terminal, p.lockDuration)
	if err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
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
			"error":          relayLogReason(cause),
			"row_status":     outcome.Status,
			"max_attempts":   p.rowMaxAttempts(row),
			"backoff_capped": retryAfter >= p.retry.maxBackoff,
		}).Warn("event relay: publish failed and the event is scheduled for another attempt")

		return
	}

	// WHY the budget is spent, stated on the line rather than left to be inferred from the
	// numbers. "attempt 1 of 5, exhausted" reads as a bookkeeping defect unless the line also
	// says the failure was permanent, which is the one case where it is the correct outcome.
	//
	// retry_after IS REPORTED ON THIS ARM TOO, and it is not noise. It is the delay the
	// schedule produced for the attempt that spent the budget — 16s at the mandated
	// parameters, the fifth value of 1s/2s/4s/8s/16s — and it is the same value
	// MarkEventFailed has just stamped on the row. Omitting it made the exhausting attempt
	// the one attempt whose scheduled delay never appeared anywhere, which is precisely
	// what made the full mandated sequence look as though the live path never produced it.
	// The line says plainly that nothing waits it: the budget is spent and the row is
	// dead-lettered instead of being published a sixth time.
	exhaustionFields := p.rowFields(row, attempt)
	exhaustionFields["terminal"] = outcome.Terminal
	exhaustionFields["retry_after"] = retryAfter.String()
	exhaustionFields["retry_after_waited"] = false
	exhaustionFields["attempts"] = outcome.Attempts
	exhaustionFields["max_attempts"] = p.rowMaxAttempts(row)
	exhaustionFields["error"] = relayLogReason(cause)

	// ONE line on BOTH arms, with the wording selected by the verdict. The permanent arm
	// already had one; the budget-spent arm had none at all, so the attempt that ended a
	// retried event's life was the one attempt the relay said nothing about — the schedule's
	// final delay went unreported and the transition itself was only visible indirectly,
	// through the dead-letter write that followed it.
	if outcome.Terminal {
		logrus.WithFields(exhaustionFields).Warn(
			"event relay: the publish failed PERMANENTLY, so the remaining retry budget is " +
				"abandoned and the event goes straight to its dead-letter topic; no retry could " +
				"change this outcome",
		)
	} else {
		logrus.WithFields(exhaustionFields).Warn(
			"event relay: publish failed on the last attempt this row's budget allowed, so the " +
				"schedule's final delay is recorded on the row but never waited and the event is " +
				"handed to its dead-letter topic instead of being published again",
		)
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

	p.deadLetter(ctx, row, attempt, cause, relayTerminalBudgetSpent)
}

// recordPermanentFailure records an attempt whose failure can never succeed and hands the row
// to the dead-letter writer on that attempt.
//
// It is the first of recordFailedAttempt's two gates. The transition is a different one from
// the retryable path's — MarkEventPermanentlyFailed rather than MarkEventFailed — because the
// decision was taken by the publisher against the broker's answer rather than by the database
// against a counter. Its documentation in database/event_outbox.go covers why the row may end
// failed with budget to spare and why that is safe.
//
// The log line names the reason the event stopped, because "permanent" and "out of attempts"
// send an operator to different places: the first to an ACL, a topic that does not exist or a
// message that is too large, the second to a broker that was unreachable for the whole
// schedule. It reports what the row's own history now says — the attempt number and the
// attempt count the database recorded — so the line and the row cannot disagree.
//
// Parameters:
//   - ctx context.Context: used for the dead-letter publish; the transition runs detached.
//   - row model.EventOutbox: the claimed row.
//   - attempt int: the 1-based attempt that just failed permanently.
//   - cause error: the publish failure, recorded in last_error and in the failure metadata.
func (p *EventRelayProcessor) recordPermanentFailure(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
) {
	reason := relayFailureReason(cause)

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	// The hand-off lease, for the same reason it is passed on the exhaustion arm above.
	outcome, err := p.store.MarkEventPermanentlyFailed(bookkeeping, row.ID, row.ClaimToken, reason, p.lockDuration)
	if err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
			"event relay: recording a permanently failed publish attempt failed; the row keeps its " +
				"lease and becomes claimable again when it expires",
		)

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"attempts":   outcome.Attempts,
		"error":      relayLogReason(cause),
		"row_status": outcome.Status,
	}).Warn(
		"event relay: publish failed permanently, so the remaining retry budget is not spent and " +
			"the event goes straight to its dead-letter topic",
	)

	// Exactly as the exhaustion arm does: the retained claim token, the recorded attempt
	// count and the new status travel on the row, so the dead-letter write and the
	// transition that records it stay the exclusive property of this worker and the failure
	// metadata reports the attempt count the database actually holds.
	row.ClaimToken = outcome.ClaimToken
	row.Attempts = outcome.Attempts
	row.Status = outcome.Status

	p.deadLetter(ctx, row, attempt, cause, relayTerminalPermanentFailure)
}

// relayTerminalReason says WHY a row reached the dead-letter hand-off, and it exists so the
// two log lines that describe the hand-off cannot claim the wrong one.
//
// Before the relay honoured a permanent failure there was only one way to get here, so every
// line said "after exhausting its retry budget". That sentence is now false for the commonest
// terminal case — a permanent failure spends one attempt, not the budget — and an operator
// reading it would look for a broker outage that never happened.
type relayTerminalReason int

const (
	// relayTerminalBudgetSpent means the row used every attempt its budget allowed.
	relayTerminalBudgetSpent relayTerminalReason = iota

	// relayTerminalPermanentFailure means the publisher reported a failure no further
	// attempt could change, so the budget was deliberately left unspent.
	relayTerminalPermanentFailure
)

// String renders the reason as the bounded log-field value, so the two terminal paths are
// separable with a field match rather than by parsing a message.
//
// Returns:
//   - string: "budget_spent" or "permanent_failure". An unrecognised value renders as
//     "unspecified" rather than a number, because a log field that reads "2" tells a reader
//     nothing at all.
func (r relayTerminalReason) String() string {
	switch r {
	case relayTerminalBudgetSpent:
		return "budget_spent"
	case relayTerminalPermanentFailure:
		return "permanent_failure"
	default:
		return "unspecified"
	}
}

// description renders the clause the dead-letter log line ends with, so the sentence an
// operator reads matches what actually happened to the event.
//
// Returns:
//   - string: the reason clause, always non-empty.
func (r relayTerminalReason) description() string {
	switch r {
	case relayTerminalBudgetSpent:
		return "after exhausting its retry budget"
	case relayTerminalPermanentFailure:
		return "after a permanent publish failure, with its retry budget deliberately unspent"
	default:
		return "for an unspecified terminal reason"
	}
}

// deadLetter hands a terminal row to the dead-letter writer.
//
// Nothing about dead-lettering is implemented here — destination, failure metadata, message
// composition and the terminal transition all belong to event_dlt.go. A failed hand-off
// leaves the row in the failed state deliberately, so the event stays visible to an operator
// instead of being reported as safely dead-lettered when its message never left the process.
//
// Parameters:
//   - ctx context.Context: cancels the dead-letter publish.
//   - row model.EventOutbox: the row, carrying the claim token its terminal transition retained.
//   - attempt int: the 1-based attempt that ended the event's life.
//   - cause error: the publish failure.
//   - reason relayTerminalReason: why the row is terminal, which selects the log wording.
func (p *EventRelayProcessor) deadLetter(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
	reason relayTerminalReason,
) {
	if p.deadLetters == nil {
		// Unreachable: Start refuses without a dead-letter service. Logged rather than
		// dereferenced, because the alternative is a nil panic in a ledger process.
		logrus.WithFields(p.rowFields(row, attempt)).WithField("terminal_reason", reason.String()).Error(
			"event relay: the event is terminal but no dead-letter service is configured; " +
				"the row stays failed and remains in the dead-letter inventory",
		)

		return
	}

	outcome, err := p.deadLetters.DeadLetter(ctx, row, cause)
	if err != nil {
		withLoggableCause(
			logrus.WithFields(p.rowFields(row, attempt)).
				WithField("terminal_reason", reason.String()), err).Error(
			"event relay: the event is terminal but could not be written to its dead-letter topic; " +
				"the row stays failed, remains in the dead-letter inventory, and its hand-off is " +
				"retried by recoverUnpreservedDeadLetters once the lease the terminal transition " +
				"retained has lapsed",
		)

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"dlt_topic":       outcome.DeadLetterTopic,
		"attempts":        outcome.Metadata.AttemptCount,
		"error":           relayLogReason(cause),
		"terminal_reason": reason.String(),
	}).Warn("event relay: event dead-lettered " + reason.description())
}

// ---------------------------------------------------------------------------
// DUAL-DELIVERY BRANCH — temporary by design
//
// Everything between this banner and its closing one exists for the 30-day window in which
// Kafka publishing and legacy HTTP webhook delivery run side by side from the same outbox
// row. Retiring it is operator work rather than something the configured date performs: the
// ordered procedure — and in particular which parts of the shared webhook queue must survive
// because transaction hooks and search indexing use it — is recorded once, in the sunset block
// at the foot of webhooks.go. That procedure's first step, relocating the two payload-contract
// symbols out of the file being deleted, is already done: NewWebhook now lives in
// event_outbox.go and getEventFromStatus in event_topics.go.
//
// Nothing above this banner is part of it: claiming, publishing, the retry schedule,
// dead-lettering and replay all outlive the sunset unchanged.
// ---------------------------------------------------------------------------

// deliverLegacyWebhook enqueues the legacy HTTP delivery of one claimed row.
//
// PAYLOAD IDENTITY IS STRUCTURAL HERE, not procedural. The bytes handed to the legacy transport
// are row.Payload — the stored bytes, and the same bytes published to Kafka — so there is no
// second serialisation to drift from. A round trip through NewWebhook.Payload interface{} would
// look identical and not be: object keys come back alphabetised and every number comes back as
// a float64. Hence EnqueueLegacyWebhookDelivery, the byte-oriented entry point, rather than
// SendWebhook.
//
// NOT DELIVERING TWICE takes two mechanisms. The row's webhook_dispatched flag skips a row
// whose legacy leg is done, so a row republished to Kafka after a crash does not enqueue a
// second webhook; and because enqueuing and recording are two operations with no transaction
// spanning them, the enqueue carries the event id as its asynq task identity, which the queue
// refuses as a duplicate. The flag makes the common case cheap, the identity makes the crash
// case correct.
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
// # A legacy failure never fails the KAFKA path — but it is no longer forgotten
//
// A failure here never touches the Kafka publish, the Kafka retry budget or the dead-letter
// decision: letting a webhook receiver being down consume a Kafka retry attempt would make
// the deprecated transport able to dead-letter events on the new one. What it DOES do is
// report the failure to the caller, which used to be missing and was the whole defect. The
// failure was logged and swallowed, the caller then marked the row dispatched, and because
// dispatched is outside the claim predicate the "will be retried on the next claim" the log
// line promised never happened. The caller now records the outstanding leg instead — see
// settleAfterKafkaSuccess.
//
// # The window is evaluated HERE, per row, immediately before the enqueue
//
// Not once per batch, which is what it used to be. A batch of a hundred rows claimed one
// second before the sunset takes longer than a second to publish, so a decision taken at
// the top of the batch enqueues legacy deliveries AFTER the boundary — the one behaviour
// the sunset is defined to prevent. The predicate consulted is the authoritative one in
// event_sunset.go, which gates on both ends of the window; this file never compares a
// clock against a configured date itself.
//
// Parameters:
//   - ctx context.Context: the batch context. The recording transition runs on a detached
//     context so a shutdown cannot leave an enqueued webhook unrecorded.
//   - row model.EventOutbox: the claimed row, carrying its stored payload and claim token.
//
// Returns:
//   - legacyLegOutcome: whether the legacy leg is settled or still owed.
//   - error: the enqueue failure, when the outcome is legacyLegOwed. Recorded in the row's
//     last_error by the caller.
func (p *EventRelayProcessor) deliverLegacyWebhook(
	ctx context.Context,
	row model.EventOutbox,
) (legacyLegOutcome, error) {
	if p.legacy == nil {
		return legacyLegSettled, nil
	}

	// Already enqueued on an earlier claim. The flag makes the common case cheap; the task
	// identity carried by the enqueue makes the crash case correct.
	if row.WebhookDispatched {
		return legacyLegSettled, nil
	}

	// The boundary, immediately before the enqueue and no earlier. Outside the window the
	// leg is not owed at all: before the start because the relay refuses to run there
	// (see startupObstacle), and from the sunset onwards because Kafka is then the only
	// transport. A row that reaches the sunset with its webhook still owed is settled
	// rather than stranded — the window has ended, and the promise with it.
	if !p.legacyLegDue(p.now()) {
		return legacyLegSettled, nil
	}

	if err := p.legacy.EnqueueLegacyWebhookDelivery(row.EventID, row.Payload); err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts+1)), err).Warn(
			"event relay: enqueuing the legacy webhook delivery failed; the Kafka publish is " +
				"unaffected and the row stays claimable for the webhook leg alone",
		)

		return legacyLegOwed, err
	}

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if err := p.store.MarkWebhookDispatched(bookkeeping, row.ID, row.ClaimToken); err != nil {
		// The task is enqueued and will be delivered; only the marker is missing. A later
		// claim re-enqueues under the same task identity, which the queue refuses as a
		// duplicate, so this is an observability gap rather than a delivery defect — and
		// the leg is reported as SETTLED, because it is: the webhook is on the queue.
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts+1)), err).Warn(
			"event relay: the legacy webhook was enqueued but the row could not be marked; " +
				"a re-enqueue is suppressed by the task identity",
		)
	}

	return legacyLegSettled, nil
}

// legacyLegOutcome is what one pass of the legacy webhook leg achieved.
//
// SUNSET: deleted with the leg it describes.
//
// It is two states rather than three because the caller only ever needs to know whether a
// webhook is STILL OWED. "Not applicable", "already done on an earlier claim", "outside the
// window" and "enqueued just now" are all the same thing to the transition that follows:
// this row owes nothing more to the legacy transport.
type legacyLegOutcome int

const (
	// legacyLegSettled means no webhook is owed: none was applicable, one was already
	// enqueued, the window has closed, or the enqueue just succeeded.
	legacyLegSettled legacyLegOutcome = iota

	// legacyLegOwed means the enqueue was attempted and FAILED, so this row still owes a
	// webhook delivery and must stay claimable for it.
	legacyLegOwed
)

// ---------------------------------------------------------------------------
// END OF THE DUAL-DELIVERY BRANCH
// ---------------------------------------------------------------------------

// logAttempt logs a SUCCESSFUL publish attempt at debug, and deliberately says nothing at all
// about a failed one.
//
// # Why the failure arm was removed (OBS-15)
//
// It used to log every failed attempt at warning, immediately after the publisher had already
// logged the same attempt at error. One attempt, two lines, two levels, two field
// vocabularies. That is not redundancy an operator can ignore:
//
//   - It DOUBLES the log volume of an incident, on the path whose volume is already the
//     reason the success arm is level-guarded. A broker outage at 500 events per second
//     produced two lines per attempt for every event in flight.
//   - It makes LINE COUNTING WRONG. "How many publish attempts failed" is the question the
//     log is read for during triage, and grep produced twice the answer — while an operator
//     who deduplicated by message text got the right number from either line and had no way
//     to know the other existed.
//   - The two lines DISAGREED ON SEVERITY for the same event, so a level-filtered view showed
//     the failure and a slightly stricter one showed it as merely a warning.
//
// Requirement R-4 — the attempt number and the error reason on EVERY attempt, not only the
// last — is met by the PUBLISHER'S line, which is unconditional, carries the attempt number,
// the budget, the error reason, the topic and the transient classification, and is structurally
// pinned unguarded by an assertion in event_publisher_test.go. Keeping one canonical
// per-attempt record is what makes that requirement verifiable rather than merely satisfied
// twice.
//
// What the relay still logs about a failure is the part the publisher cannot know, and each of
// those is a DURABLE state change rather than a restatement of the attempt:
//
//   - "scheduled for another attempt", carrying the delay the schedule produced and the
//     instant the row is next due — see recordFailedAttempt.
//   - the exhausting attempt, on both its arms, carrying the final delay and why nothing waits
//     it.
//   - the dead-letter write, carrying the destination and the terminal reason — see deadLetter.
//
// The success arm stays here because there is no other candidate: the publisher's success line
// is also debug-guarded, and this one adds the row's attempt-of-budget view.
func (p *EventRelayProcessor) logAttempt(
	row model.EventOutbox,
	attempt int,
	result PublishResult,
	err error,
) {
	if err != nil {
		// Silence, by design. The publisher has already emitted the canonical per-attempt
		// record for this failure; the durable consequences are logged by the transitions
		// that perform them.
		return
	}

	if !logrus.IsLevelEnabled(logrus.DebugLevel) {
		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"status":      string(result.Status),
		"duration_ms": result.Duration.Milliseconds(),
	}).Debug("event relay: published a ledger event")
}

// rowFields renders the identity of a row and its attempt as logrus fields.
//
// One function, so every line this file emits about one event carries the SAME field names —
// which is what makes the log searchable — and so the fields requirement R-4 names cannot be
// omitted from one line by accident.
//
// The partition key is deliberately ABSENT rather than printed: it is a financial identifier,
// and the publisher already reports a hash of it for correlation.
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

// publishFailureIsTerminal reads the publisher's verdict on whether another attempt could
// succeed, and reports the answer the durable transition needs.
//
// # Why the verdict is the publisher's and not the relay's
//
// The publisher is the only thing that knows what failed. It classifies a failure at the
// moment it happens and reports PublishStatusDeadLettered for one no retry can fix — an
// envelope over the size ceiling, bytes that are not valid JSON, a destination outside the
// topic namespace Blnk owns. Re-deriving that here would mean re-classifying broker errors in
// a second place, and the two copies would disagree the first time either changed.
//
// # Both signals are read, and BOTH ARE AFFIRMATIVE. "Unknown" is NOT terminal
//
// The result carries the classification for every publish that came out of this pipeline; the
// error carries it too, for a caller holding only an error. Terminal is reported only when one
// of them SAYS SO — PublishResult.PermanentFailure, which requires an error, the failed status
// this pipeline sets only after classifying, and a non-transient classification; or
// IsPermanentPublishError, which requires the error to be a PublishError that was classified
// as non-recoverable. Anything else is unknown, and unknown falls through to the budgeted
// retry that MarkEventFailed owns.
//
// This used to be `!result.Transient && !IsTransientPublishError(cause)`, and the double
// negation was the defect. Both halves report "not transient" for input they never
// classified: a zero-valued PublishResult — which is exactly what a publisher that classifies
// nothing produces — and any error that did not come out of this file's publish path. So a
// bare error from a borrowed writer, a wrapped context expiry, or a substitute publisher in a
// test was read as "no further attempt can succeed" and the row was dead-lettered on ATTEMPT
// ONE with four attempts unspent. The log said so too, one line claiming the event was
// scheduled for another attempt and the next recording it terminal, which is how a semantic
// defect hides in a system whose observability is otherwise good.
//
// The publisher is an INTERFACE SEAM — the relay borrows whichever implementation the process
// built — so "no verdict" is a normal input rather than a defect to punish, and it must read
// as "not permanent". That is the judgement PublishResult.PermanentFailure already documents
// and enforces for its own three facts; this predicate now agrees with it instead of
// contradicting it two hundred lines away.
//
// # Why erring toward RETRY is right here, having previously erred the other way
//
// Neither direction loses the event: the row is durable either way, and both a dead-letter and
// an exhausted budget end with the event preserved on its `<topic>.dlt` sibling, listable and
// replayable. What differs is which mistake is recoverable WITHOUT AN OPERATOR. Spending the
// budget on a failure that can never succeed costs four more attempts and about seventeen
// seconds, after which the event dead-letters exactly as it would have; dead-lettering a
// TRANSIENT failure on the first attempt turns a broker hiccup into an operator-triggered
// replay for every event in flight during it. The first mistake is absorbed by the retry
// schedule. The second becomes a queue of manual work, at exactly the moment the system is
// least healthy.
//
// Parameters:
//   - result PublishResult: the publisher's report of the attempt. A zero value states
//     nothing and is therefore NOT terminal.
//   - cause error: the failure returned alongside it. An error this pipeline did not classify
//     is likewise not terminal.
//
// Returns:
//   - bool: true only when the publisher or its error affirmatively reports that no further
//     attempt can succeed.
func publishFailureIsTerminal(result PublishResult, cause error) bool {
	// The result's own verdict, which requires an affirmative Classified marker.
	if result.PermanentFailure() {
		return true
	}

	// The error's verdict, for a caller holding only an error. A *PublishError is the
	// only carrier of a classification, so its absence is the "nobody classified this"
	// case and must NOT be read as permanent.
	var publishErr *PublishError
	if errors.As(cause, &publishErr) {
		return !publishErr.Transient
	}

	return false
}

// relayFailureReason renders a publish failure as text safe to STORE in the row's
// last_error column and in the dead-letter failure metadata.
//
// The string comes from a broker or a client library, so neither its length nor its content
// is this codebase's to choose: newlines would forge structure in anything that later renders
// the column, control characters corrupt parsers, and an unbounded value written once per
// attempt per event is how a text column grows without limit. sanitizeLogValue is shared with
// the rest of the event pipeline so the bounds are identical everywhere.
//
// # This is the DURABLE rendering, and it deliberately keeps the broker's own words
//
// It is NOT what goes into a log line — relayLogReason is — and the difference is the reader.
// This value is reachable in exactly two places, and both are already privileged: the
// last_error column and the failure_metadata written to a `<topic>.dlt` sibling. Dead-letter
// topics are not grantable to subscribers (IsSubscriberGrantableTopic excludes every `.dlt`
// name), and the dead-letter inventory API is gated on the master key, so the audience for
// this string is an operator who is already entitled to the deployment's internals.
//
// For that audience the address that failed is the most useful part of the message, and
// redacting it would make a dead-letter triage — the one workflow this column exists for —
// strictly harder while protecting nothing that is not already exposed to the reader.
//
// A nil error yields a fixed, non-empty string rather than "": an empty reason recorded
// against a failed attempt is indistinguishable from a row nobody has tried yet.
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

// relayLogReason renders a publish failure as text safe to write to a LOG at a normal level:
// everything relayFailureReason does, plus network topology redacted.
//
// The split from relayFailureReason exists because the two sinks have different readers. A log
// is read by whoever can reach the log aggregator, which in most deployments is a far wider
// group than the holders of the master key, and it is retained and shipped onwards. A broker
// error names the broker's address and port, a resolver failure names the internal DNS server,
// and a wrapped database error can quote its connection string; none of that is needed to know
// that a publish failed, and all of it is reconnaissance once it is sitting in a log.
//
// The diagnosis survives. Redaction removes address-shaped tokens and secret values only, so
// "connection refused", "i/o timeout" and "Cluster Authorization Failed" still reach the line —
// and the unredacted text is one debug level away, both through the cause_verbatim field
// withLoggableCause attaches and through the row's own last_error column.
//
// Parameters:
//   - cause error: the failure. A nil error yields the same fixed string relayFailureReason
//     uses, so the two renderings agree about the absence of a reason.
//
// Returns:
//   - string: the redacted, sanitized, bounded rendering.
func relayLogReason(cause error) string {
	if cause == nil {
		return "the publish failed without reporting a reason"
	}

	reason := loggableCause(cause)
	if reason == "" {
		return "the publish failed without reporting a reason"
	}

	return reason
}

// detachedBookkeepingContext derives a short-lived context for a transition the relay ALREADY
// OWES, so a shutdown cannot leave the database disagreeing with what has already happened on
// the wire.
//
// The three transitions it covers — marked dispatched, marked failed, webhook marked dispatched
// — all describe completed work: the broker has the message, or the queue has the task.
// Cancelling them would not undo any of it, only leave the row saying otherwise, and a row that
// says otherwise is republished on its next claim.
//
// It is BOUNDED rather than simply detached, so "finish what you owe" cannot become "block
// shutdown on a database that has gone away": on timeout the transition fails, is logged, and
// the row's lease expiry brings it back. The claim and the publishes deliberately keep the
// CALLER's context, being work not yet started.
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
// The relay records no instrument directly, on both counts deliberately. The PER-ATTEMPT
// instruments (blnk.events.published.total, blnk.events.publish.attempts.total,
// blnk.events.publish.duration) are recorded inside the publisher, on the attempt itself; the
// relay's contribution is to populate the fields they are attributed by — Attempt and
// MaxAttempts, ClaimedAt so the duration measures claim to acknowledgement, and Purpose so a
// replay never contaminates the first-delivery population. Recording them here too would double
// every count.
//
// The GAUGES (blnk.outbox.pending, blnk.dlt.oldest_message_age_seconds,
// blnk.kafka.consumer_lag) have one owner, the periodic EventMetricsCollector in
// event_metrics.go, because a gauge retains its last value and must be re-recorded from
// authoritative state on every tick, including the explicit zero that clears an alert.
// Dead-letter age is the clearest case: recorded where an event is dead-lettered it would
// always report something that just happened, and the "stuck for 15 minutes" alert could never
// fire. The collector also needs the admin client for consumer lag, which the relay does not
// hold.
// ---------------------------------------------------------------------------
