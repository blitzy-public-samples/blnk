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

// This file holds the RELAY half of the ledger event pipeline: the background processor
// that drains blnk.event_outbox to Kafka.
package blnk

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
)

const (
	// defaultEventRelayBatchSize is how many outbox rows one claim takes, matching
	// LineageOutboxProcessor: large enough to amortise the claim round trip, small enough
	// that a crash mid-batch leaves little to redo.
	defaultEventRelayBatchSize = 100

	// defaultEventRelayPollInterval is how often the relay looks for work when it has
	// none, matching LineageOutboxProcessor. It is NOT the throughput limit — see
	// maxEventRelayBatchesPerTick, which lets one tick drain a backlog.
	defaultEventRelayPollInterval = 1 * time.Second

	// defaultEventRelayLockDuration is the lease a claim takes on its rows, matching
	// LineageOutboxProcessor. It is the recovery latency after a crash: rows a dead relay
	// held become claimable again this long after it stopped.
	defaultEventRelayLockDuration = 30 * time.Second

	// defaultEventRelayConcurrency is how many partition-key groups of one batch are
	// published at once.
	defaultEventRelayConcurrency = 8

	// maxEventRelayBatchesPerTick bounds how many batches one tick chains, so a large
	// backlog drains promptly without starving the stop signal. Modelled on
	// ChainProcessor's maxBatchesPerTick. Without chaining the ceiling would be batchSize
	// per pollInterval — 100 events per second at the defaults — however fast the broker
	// is.
	maxEventRelayBatchesPerTick = 50

	// eventRelayRowPublishBudget is the WORST-CASE wall time ONE row's publish attempt may
	// take, and it is the only thing that bounds a publish by arithmetic.
	eventRelayRowPublishBudget = eventWriterWriteTimeout

	// eventRelayBackoffMultiplier is the factor the retry delay grows by on each attempt.
	eventRelayBackoffMultiplier = 2

	// eventRelayLeaseRenewalDivisor sets how often an in-flight batch renews its lease, as
	// a fraction of the lease itself.
	eventRelayLeaseRenewalDivisor = 3

	// eventRelayMinLeaseRenewalInterval floors the renewal interval so a very short lease
	// cannot turn the heartbeat into a busy loop against the database.
	eventRelayMinLeaseRenewalInterval = 250 * time.Millisecond

	// The REPAIR fallbacks, reached only by a relay built without a readable
	// configuration. config.DefaultRelayRepair* are the shipped values and the single
	// source of the numbers; these mirror them so a relay that cannot read a configuration
	// still recovers at a usable rate rather than at a token one.
	defaultEventRelayRepairBatchSize      = config.DefaultRelayRepairBatchSize
	defaultEventRelayRepairBatchesPerTick = config.DefaultRelayRepairMaxBatchesPerTick
	defaultEventRelayRepairConcurrency    = config.DefaultRelayRepairConcurrency

	// eventRelayBookkeepingTimeout bounds the bookkeeping transitions the relay performs
	// on a DETACHED context. See detachedBookkeepingContext for why they are detached at
	// all; this is what stops "finish what you owe" becoming "hang forever on a database
	// that has gone away during shutdown".
	eventRelayBookkeepingTimeout = 5 * time.Second

	// The two fallback delays below mirror config's relay defaults, which are unexported.
	fallbackRelayRetryBaseBackoff = 1000 * time.Millisecond
	fallbackRelayRetryMaxBackoff  = 30000 * time.Millisecond
)

// resolved once from configuration into a value with a pure method, so the part of this
// file most worth testing exactly is testable without a database, a broker or a clock.
type relayRetryPolicy struct {
	// maxAttempts is the retry budget: how many publish attempts an event gets before it
	// is dead-lettered. It is informational HERE — the budget is enforced in SQL by
	// MarkEventFailed, which is what stops two instances both concluding they were the
	// last attempt — and is carried so the relay can report "attempt 3 of 5".
	maxAttempts int

	// baseBackoff is the delay after the FIRST failed attempt, and the value every later
	// delay doubles from.
	baseBackoff time.Duration

	// maxBackoff caps the delay however many attempts have failed.
	maxBackoff time.Duration
}

// newRelayRetryPolicy resolves the schedule from the relay configuration block.
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

// eventRelayStore is the repository surface the relay needs, and deliberately no more
// of it.
type eventRelayStore interface {
	// ClaimPendingEventOutbox claims a batch, oldest occurrence first, taking a lease and
	// stamping every row with a claim token. It returns at most one row per EFFECTIVE key
	// — the same key the publisher hashes — across all concurrent relay instances, which
	// is half of the ordering guarantee.
	ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	// MarkEventDispatched moves a claimed row to its success terminal state. It is
	// conditional on the claim token and CLEARS it, so it must be the last transition the
	// relay performs on a row.
	MarkEventDispatched(ctx context.Context, id int64, claimToken string, record model.BrokerRecord) error

	// MarkEventFailed records one failed attempt, schedules the row's next due instant
	// from retryAfter, and reports — decided in SQL, so two instances cannot both conclude
	// they were last — whether the budget is now spent.
	MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration, terminal bool, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// ClaimPendingWebhookDeliveries claims rows whose KAFKA leg has finished —
	// successfully or terminally — and whose LEGACY WEBHOOK leg is still owed, so a leg
	// that failed alongside a Kafka publish is not lost behind a state the ordinary claim
	// never revisits. Deleted at the sunset with the leg itself.
	ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error)

	// MarkEventLegacyWebhookAttempted records one failed legacy enqueue against a row
	// whose Kafka leg has already finished, WITHOUT touching that leg's terminal state,
	// and reports whether the legacy budget is now spent. Deleted at the sunset with the
	// leg itself.
	MarkEventLegacyWebhookAttempted(ctx context.Context, id int64, claimToken string, retryAfter time.Duration) (model.EventWebhookOutcome, error)

	// MarkEventPermanentlyFailed records an attempt whose failure was PERMANENT, taking
	// the row to failed on this attempt whatever budget remained and retaining the claim
	// token AND THE LEASE for the dead-letter hand-off. It is what lets the relay act on
	// the publisher's verdict instead of spending four more attempts on a condition none
	// of them can change.
	MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string, deadLetterLease time.Duration) (model.EventFailureOutcome, error)

	// MarkWebhookDispatched records the legacy leg of the dual-delivery window. It is
	// conditional on the claim token, so it must run BEFORE MarkEventDispatched clears it.
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

	// ClaimFailedEventOutboxForDeadLetter claims rows whose retry budget is spent and
	// whose dead-letter write did not succeed, so the preservation can be retried. Without
	// it such a row is unclaimable, unreplayable and the only copy of the event.
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
type EventRelayProcessor struct {
	// blnk is the service container. It is held for the same reason LineageOutboxProcessor
	// holds it — configuration and the datasource — and the collaborators below are
	// derived from it once at construction.
	blnk *Blnk

	batchSize    int
	pollInterval time.Duration
	lockDuration time.Duration

	// concurrency is how many partition-key groups of one batch publish at once. See
	// defaultEventRelayConcurrency.
	concurrency int

	// The REPAIR capacity, resolved from configuration at construction and overridable
	// through WithRepairCapacity. It governs the two passes that clear work the publish
	// claim cannot reach — outstanding dead-letter writes and outstanding legacy webhook
	// enqueues — and it is separate from the publish batch because the two fill under
	// different conditions: the publish backlog grows with ingress, these grow with an
	// outage.
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
	// rather than building one so that every write reuses the publisher's writers and
	// their one shared transport, with its connection pool and its authentication
	// configuration.
	publisher TopicEventPublisher

	// deadLetters is built ONCE with the publisher above and kept for the relay's
	// lifetime, which is what the dead-letter service's own documentation requires of a
	// caller in a loop: the alternative resolves a short-lived publisher per call, which
	// is right for an operator-triggered replay and wrong here.
	deadLetters eventRelayDeadLetterer

	// legacy is the dual-delivery leg. Nil after the sunset, when the whole branch goes.
	legacy eventRelayLegacyTransport

	// retry is the resolved backoff schedule.
	retry relayRetryPolicy

	// dualDeliveryActive is THE dual-delivery decision, and it is a function field only so
	// a test can pin the clock. In production it is event_sunset.go's
	// WebhookDualDeliveryActive and nothing else: this file never parses a configured date
	// and never compares instants itself. Two comparisons could disagree, and the pair
	// that would then disagree is "stop dual-writing" and "answer 410 Gone" — a system
	// that had done one but not the other.
	dualDeliveryActive func(now time.Time) bool

	// windowState is the same resolution in its four-valued form, used where the REASON
	// dual delivery is off changes what happens: the startup obstacle refuses to run a
	// relay whose window has not opened yet, which is a misconfiguration, while it starts
	// happily after the sunset, which is the intended end state.
	windowState func(now time.Time) WebhookWindowState

	// now is the clock, replaceable in-package so the sunset boundary is exact in tests.
	now func() time.Time

	// catalogue gates CLAIMING on the destination topics actually existing. Nil means
	// ungated, which is what every existing test and every construction without
	// WithCatalogueGate gets — those relays publish to a broker a test controls, and
	// gating them on a real metadata read would be a dependency they do not have.
	catalogue eventRelayCatalogueGate
}

// eventRelayCatalogueGate answers whether the relay may claim work yet.
type eventRelayCatalogueGate interface {
	// Ready returns nil when every topic the relay may need is known to exist. A non-nil
	// error means the relay must not claim, and the gate has already reported why — the
	// relay stays silent so a 1-second poll cannot turn one condition into a log flood.
	Ready(ctx context.Context) error
}

// NewEventRelayProcessor creates the event outbox relay for a Blnk instance.
//
// Parameters:
//   - blnk *Blnk: the service container. May be nil.
//
// Returns:
//   - *EventRelayProcessor: the configured processor, with the house defaults — batch
//     size 100, poll interval 1 second, lock duration 30 seconds.
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
		// instance, whose Close releases it, and a service handed a publisher explicitly does
		// not own it.
		processor.deadLetters = NewEventDeadLetterService(blnk.datasource, publisher)
	}

	return processor
}

// WithCatalogueGate makes the relay refuse to CLAIM until every topic it may need is
// known to exist. Call it before Start.
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
func (p *EventRelayProcessor) WithPollInterval(interval time.Duration) *EventRelayProcessor {
	if interval <= 0 {
		logrus.WithField("requested_poll_interval", interval.String()).
			Warn("event relay: ignoring a non-positive poll interval; keeping the configured value")

		return p
	}

	p.pollInterval = interval

	return p
}

// WithLockDuration sets the lease a claim takes on its rows, and the period it is
// renewed by.
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

// WithConcurrency sets how many partition-key groups of one batch publish at once. Call
// it before Start.
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

// WithRepairCapacity sets how fast the two repair passes clear their backlogs. Call it
// before Start.
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
	if err := WebhookWindowObstacle(p.windowStateAt(p.now())); err != nil {
		return err
	}

	return nil
}

// Start begins draining the event outbox in the background.
//
// Parameters:
//   - ctx context.Context: when cancelled, processing stops; rows already claimed keep
//     their lease and become claimable again when it expires.
//
// Returns:
//   - error: the startup obstacle, already logged, or nil when the relay is running.
func (p *EventRelayProcessor) Start(ctx context.Context) error {
	if err := p.startupObstacle(); err != nil {
		withLoggableCause(nil, err).Error("Event outbox relay not started")

		return err
	}

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()

		// NIL, not an error. Idempotency is the contract: the caller asked for a draining
		// relay and there is one.
		return nil
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// The running flag is cleared on EVERY exit from the loop, not only the one Stop
		// takes. run returns on parent-context cancellation too, and leaving the flag set
		// there made IsRunning report a relay that had already stopped and made a later Start
		// refuse to spawn a new loop — a process that cancelled its context and then
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

	return nil
}

// Stop gracefully stops the relay.
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
func (p *EventRelayProcessor) markStopped() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()
}

// run is the main processing loop: LineageOutboxProcessor's ticker and select, with an
// informative line on each exit path so a stopped relay always says why it stopped.
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

// processTick drains as many batches as are ready, bounded by
// maxEventRelayBatchesPerTick.
func (p *EventRelayProcessor) processTick(ctx context.Context) {
	// THE CATALOGUE GATE COMES FIRST, before the repair passes and before any claim.
	if p.catalogue != nil {
		if err := p.catalogue.Ready(ctx); err != nil {
			return
		}
	}

	// The REPAIR passes first, and deliberately so. Each queries a set that is empty in
	// normal operation, so an idle pass costs one indexed query; and when a set is NOT
	// empty those rows are the only copies of events that reached no topic at all, which
	// makes them the most urgent work in the tick rather than the least. Putting them
	// after the publish loop would also mean a sustained backlog — fifty chained batches —
	// starved the repair entirely.
	p.repairChained(ctx, repairLegDeadLetter, p.recoverUnpreservedDeadLetters)

	// The LEGACY leg's own repair pass, for the same reasons and with the same shape. Its
	// candidate set is likewise empty in normal operation and index-backed, and when it is
	// not empty those rows are webhooks promised for the migration window that no other
	// statement will ever pick up. SUNSET: deleted with the dual-delivery branch.
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

// --------------------------------------------------------------------------- A NOTE ON
// THE METRICS THIS FILE DOES NOT RECORD
