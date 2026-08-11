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

// event_monitor_handoff.go is the evaluation half of the balance-monitor handoff: the
// processor that drains blnk.balance_monitor_handoff, judges each snapshot against its
// monitors, and captures the resulting alerts transactionally.
//
// # THIS IS NO LONGER THE ORDINARY PATH FOR A NEW MOVEMENT
//
// The atomic writers now decide a crossing and insert the canonical blnk.event_outbox row
// inside the transaction that moved the balance, which is requirement R-2 met literally —
// see database.captureBalanceMonitorAlertsInTx and, for the builder they share with the
// pre-write pass, blnk.prepareBalanceMonitorAlertRow. A movement made by a process with the
// alert capture registered, which every process built through NewBlnk is, writes NO handoff.
//
// This processor remains, and is still started by the server, for two populations that are
// real and finite:
//
//   - Handoff rows written by releases that PREDATE the in-transaction capture. They are
//     durable, they carry both decision inputs, and an upgrade must not strand them.
//   - Handoff rows written by a process with NO capture registered — a Datasource constructed
//     directly, without the root service, which is what this repository's own tests do.
//     database.recordBalanceMonitorEvaluation falls back to the handoff there rather than
//     leaving the movement's alerts to be decided later against a monitors table an operator
//     can edit in the meantime.
//
// # The gap it closed, and why the fallback is still sound
//
// Requirement R-2 puts every event in the same database transaction as the ledger
// mutation that produced it. `balance.monitor` was the hardest producer to bring under it,
// because a monitor fires on a balance a transaction has ALREADY committed as far as the
// original post-commit design was concerned. The capture was therefore a standalone insert
// taken after the commit, and a process that died in the window, or a database outage that
// outlasted a small retry budget, destroyed the alert outright: the balance movement stood,
// the low-balance or overdraft notification never existed, and nothing was left to replay
// because no row had ever been written.
//
// The handoff closed that in two halves, and both halves still hold for the rows it drains.
// The half that CAN be atomic is EVERY INPUT THE ALERT IS A FUNCTION OF.
// database.insertBalanceMonitorHandoffsInTx writes one handoff row per monitored balance inside
// the balance's own transaction, carrying the balance as written AND the monitor definitions
// read in that same transaction — so a committed movement always carries its pending
// evaluation together with the complete decision it is pending on, and a rolled-back
// movement carries none.
//
// Both snapshots are load-bearing. Re-reading the monitors at drain time left one input live
// on a table operators edit through PUT and DELETE /balance-monitors, so which events existed
// could change after the mutation committed and two attempts at one row could disagree. The
// snapshot is what makes this processor a pure function of the row it claimed.
//
// The half that is not atomic with the mutation is made atomic with the intent's
// COMPLETION. This processor claims a handoff, evaluates it, and hands the alerts and the
// completion to CompleteBalanceMonitorHandoffWithEvents, which writes both in one
// transaction. So the sequence is at-least-once evaluation feeding an atomic capture, and
// the only way to lose an alert is for its condition never to have been met. What it does not
// give, and what the in-transaction capture does, is the canonical event row in the mutation's
// own transaction: until the conversion commits, the event R-2 names does not exist.
//
// # The condition evaluation itself is UNTOUCHED
//
// model.BalanceMonitor.CheckCondition is called exactly as the post-commit path called
// it, on a balance value with the same contents. AAP §0.6.2 freezes monitor condition
// evaluation, and nothing here changes what a monitor decides — only when the decision is
// taken and how its result is made durable.
//
// # Structure
//
// The processor is LineageOutboxProcessor's shape: the same fields, the same defaults of
// 100 / 1 second / 30 seconds, the same fluent configurators, the same double-start
// guard, the same channel-closing Stop, the same ticker/select loop and the same
// claim-then-mark batch. chain_worker.go documents itself as modelled on the same
// processor, so this is the house pattern for a background relay rather than one file's
// preference.
package blnk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

// The processor's defaults, matching LineageOutboxProcessor's so the two background
// relays behave alike under load and an operator has one set of numbers to reason about.
//
// The poll interval deserves a word, because it is the only latency this mechanism adds.
// An alert used to be published from a goroutine spawned immediately after the commit;
// it is now published up to one poll later. That is immaterial in context: the event
// still has to be claimed by the event relay, which polls on the same interval, and then
// published to Kafka and consumed. One second on a path already measured in seconds
// buys the difference between an alert that is occasionally lost and one that never is.
const (
	defaultMonitorHandoffBatchSize    = 100
	defaultMonitorHandoffPollInterval = 1 * time.Second
	defaultMonitorHandoffLockDuration = 30 * time.Second
)

// balanceMonitorEventType is the event name a fired monitor publishes under.
//
// It is a constant here because every route that can decide a crossing must agree on it —
// this processor, the pre-write pass and the writer's in-transaction capture, which both reach
// it through blnk.prepareBalanceMonitorAlertRow, and the legacy post-commit path in
// balance.go — and a typo in any of them would route the alert to the wrong topic while every
// test that only checks "an event was captured" still passed.
const balanceMonitorEventType = "balance.monitor"

// BalanceMonitorHandoffProcessor drains blnk.balance_monitor_handoff.
//
// It holds no state beyond its configuration and its lifecycle: everything about a unit
// of work travels in the claimed row, which is what lets several processors run
// concurrently and what lets a crashed one's work be picked up by another.
type BalanceMonitorHandoffProcessor struct {
	blnk         *Blnk
	batchSize    int
	pollInterval time.Duration
	lockDuration time.Duration
	stopCh       chan struct{}
	wg           sync.WaitGroup
	running      bool
	mu           sync.Mutex
}

// NewBalanceMonitorHandoffProcessor creates a processor with the house defaults.
//
// Parameters:
//   - blnk *Blnk: the service handle, used for the datasource, the monitor lookup and
//     the event preparation.
//
// Returns:
//   - *BalanceMonitorHandoffProcessor: the configured processor, not yet started.
func NewBalanceMonitorHandoffProcessor(blnk *Blnk) *BalanceMonitorHandoffProcessor {
	return &BalanceMonitorHandoffProcessor{
		blnk:         blnk,
		batchSize:    defaultMonitorHandoffBatchSize,
		pollInterval: defaultMonitorHandoffPollInterval,
		lockDuration: defaultMonitorHandoffLockDuration,
		stopCh:       make(chan struct{}),
	}
}

// WithBatchSize sets how many handoffs one poll claims.
//
// Parameters:
//   - size int: the batch size. Values below one are ignored, because a processor that
//     claims nothing would run forever without draining anything.
//
// Returns:
//   - *BalanceMonitorHandoffProcessor: the processor, for chaining.
func (p *BalanceMonitorHandoffProcessor) WithBatchSize(size int) *BalanceMonitorHandoffProcessor {
	if size > 0 {
		p.batchSize = size
	}

	return p
}

// WithPollInterval sets the interval between polls.
//
// Parameters:
//   - interval time.Duration: the poll interval. Non-positive values are ignored,
//     because a zero-interval ticker panics.
//
// Returns:
//   - *BalanceMonitorHandoffProcessor: the processor, for chaining.
func (p *BalanceMonitorHandoffProcessor) WithPollInterval(interval time.Duration) *BalanceMonitorHandoffProcessor {
	if interval > 0 {
		p.pollInterval = interval
	}

	return p
}

// WithLockDuration sets how long a claim is leased.
//
// A lease that is too short lets a second processor claim a handoff the first is still
// evaluating. That is tolerated rather than fatal — the derived event ids make the
// duplicate collide with the unique index — but it wastes work, so the default is
// generous relative to the evaluation, which is a cached read and some comparisons.
//
// Parameters:
//   - duration time.Duration: the lease. Non-positive values are ignored.
//
// Returns:
//   - *BalanceMonitorHandoffProcessor: the processor, for chaining.
func (p *BalanceMonitorHandoffProcessor) WithLockDuration(duration time.Duration) *BalanceMonitorHandoffProcessor {
	if duration > 0 {
		p.lockDuration = duration
	}

	return p
}

// Start begins draining handoffs in the background.
//
// The double-start guard is not decoration. Two loops on one processor would double every
// claim attempt and halve the effective lease, and Stop would close a channel one of them
// no longer reads.
//
// Parameters:
//   - ctx context.Context: cancelled to stop the loop.
func (p *BalanceMonitorHandoffProcessor) Start(ctx context.Context) {
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
	}).Info("balance monitor handoff processor started")
}

// Stop signals the loop and waits for the in-flight batch to finish.
//
// Waiting matters: a batch abandoned mid-way leaves claimed handoffs whose lease has to
// expire before anything else will touch them, which delays every alert in it.
func (p *BalanceMonitorHandoffProcessor) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	logrus.Info("balance monitor handoff processor stopped")
}

// IsRunning reports whether the loop is active.
//
// Returns:
//   - bool: true between Start and Stop.
func (p *BalanceMonitorHandoffProcessor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.running
}

// run is the poll loop.
func (p *BalanceMonitorHandoffProcessor) run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("balance monitor handoff processor context cancelled")
			return
		case <-p.stopCh:
			logrus.Info("balance monitor handoff processor stop signal received")
			return
		case <-ticker.C:
			p.processBatch(ctx)
		}
	}
}

// processBatch claims a batch and evaluates each handoff in it.
//
// A claim failure is logged and the poll returns: the rows are untouched, so the next
// tick tries again, and there is nothing to compensate for.
//
// Each handoff is evaluated independently. One that fails does not abandon the rest,
// because its failure is recorded against its own row and the remainder are unrelated
// balances whose alerts have no reason to wait.
func (p *BalanceMonitorHandoffProcessor) processBatch(ctx context.Context) {
	if p.blnk == nil || p.blnk.datasource == nil {
		return
	}

	handoffs, err := p.blnk.datasource.ClaimPendingBalanceMonitorHandoffs(ctx, p.batchSize, p.lockDuration)
	if err != nil {
		logrus.WithError(err).Error("failed to claim balance monitor handoffs")
		return
	}

	if len(handoffs) == 0 {
		return
	}

	logrus.WithField("count", len(handoffs)).Debug("evaluating balance monitor handoffs")

	for index := range handoffs {
		handoff := handoffs[index]
		if err := p.processHandoff(ctx, handoff); err != nil {
			p.recordHandoffFailure(ctx, handoff, err)
		}
	}
}

// processHandoff evaluates one handoff and captures its result atomically.
//
// # The sequence, and why each step is where it is
//
//  1. Decode the BALANCE snapshot. The condition is judged against the balance AS THE
//     TRANSACTION WROTE IT, not against the balance now: re-reading would judge whatever
//     later transactions had done to it, so a threshold crossed by this movement and
//     uncrossed by the next would produce no alert, and two attempts could disagree.
//  2. Decode the MONITOR snapshot, for the same reason applied to the other input. A row
//     written before sql/1781252100.sql carries none, and only such a row falls back to the
//     live cached read — see monitorsForHandoff.
//  3. Evaluate each condition with the UNCHANGED CheckCondition.
//  4. Prepare an event row per fired monitor, with a DERIVED id.
//  5. Write the rows and the completion in ONE transaction.
//
// Step 4's derived id is what makes step 5 safe to repeat. The identity is the pair
// (handoff, monitor): distinct across firings because each firing has its own handoff, and
// stable across re-evaluations of one handoff. A lapsed lease, a retry or a restart
// therefore produces the same ids and collides with the unique index rather than
// delivering the alert twice.
//
// Zero fired monitors is the common case and is NOT a no-op: the handoff is completed with
// an empty event list, which records "evaluated, nothing fired". Skipping the completion
// would leave the row claimable and re-evaluate it until its budget ran out, and the
// distinction between "evaluated, nothing fired" and "never evaluated" — the only question
// worth asking when an expected alert did not arrive — would be lost.
//
// Parameters:
//   - ctx context.Context: the context for the evaluation and the write.
//   - handoff model.BalanceMonitorHandoff: the claimed row.
//
// Returns:
//   - error: the failure to record against the row; nil when the handoff is completed.
func (p *BalanceMonitorHandoffProcessor) processHandoff(ctx context.Context, handoff model.BalanceMonitorHandoff) error {
	balance, err := handoff.Balance()
	if err != nil {
		// PERMANENT. The stored bytes do not change, so no further attempt can decode
		// them, and spending the remaining budget only delays the same conclusion.
		return newPermanentMonitorHandoffError(err)
	}

	monitors, err := p.monitorsForHandoff(ctx, handoff)
	if err != nil {
		return err
	}

	events := make([]*model.EventOutbox, 0, len(monitors))
	for index := range monitors {
		monitor := monitors[index]
		// UNCHANGED CONDITION EVALUATION. This is the same call the post-commit path
		// made, on a balance with the same contents. AAP §0.6.2 freezes what a monitor
		// decides; only the durability of the decision's result has changed.
		if !monitor.CheckCondition(balance) {
			continue
		}

		event, prepareErr := p.blnk.PrepareEventOutbox(ctx, NewWebhook{
			Event:   balanceMonitorEventType,
			Payload: monitor,
		},
			// THE LEDGER IS SUPPLIED EXPLICITLY, from the handoff rather than from the
			// monitor. model.BalanceMonitor carries a balance and a condition and no
			// ledger, so without this the event's ledger column would be NULL — and R-6
			// partitions by ledger id. The handoff carries the ledger of the balance whose
			// movement created it, which is the authoritative answer.
			WithEventLedgerID(handoff.LedgerID),
			// THE DERIVED IDENTITY, and it is what makes a repeated evaluation idempotent.
			WithEventIdentity(model.BalanceMonitorEventIdentity(handoff.HandoffID, monitor.MonitorID)),
		)
		if prepareErr != nil {
			return fmt.Errorf("failed to prepare the balance.monitor event for monitor %s: %w", monitor.MonitorID, prepareErr)
		}

		// A nil row means publishing is not configured. The processor only runs when it
		// is, so this is unreachable in a healthy deployment; skipping rather than
		// failing means a configuration change mid-flight drains the backlog to
		// completion instead of failing every row in it.
		if event == nil {
			continue
		}

		events = append(events, event)
	}

	if err := p.blnk.datasource.CompleteBalanceMonitorHandoffWithEvents(ctx, handoff.HandoffID, events); err != nil {
		return fmt.Errorf("failed to capture the balance monitor evaluation: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"handoff_id":      handoff.HandoffID,
		"balance_id":      handoff.BalanceID,
		"monitors":        len(monitors),
		"events_captured": len(events),
	}).Debug("balance monitor handoff evaluated")

	return nil
}

// monitorsForHandoff resolves the monitor definitions this handoff is to be evaluated
// against, preferring the snapshot the mutation captured.
//
// # The snapshot is the answer, and the fallback is a migration artefact
//
// A row written by this release carries the definitions that were in force when the
// balance's transaction committed, read inside that transaction. Using them is what makes
// the evaluation a pure function of the row: the same row evaluated twice, or evaluated an
// hour later, reaches the same verdict, and no edit to blnk.balance_monitors between the
// commit and the drain can add, remove or reshape an alert for a movement that has already
// happened.
//
// A row written BEFORE sql/1781252100.sql carries no snapshot. Such a row is still
// evaluable — by reading the monitors live, which is exactly what this processor used to do
// — and it must be, because refusing it would strand the backlog an upgrade inherits at the
// moment that backlog is largest. So the fallback exists, it is taken only for those rows,
// and it is logged at WARN so the older guarantee is never applied silently.
//
// The fallback population is finite and shrinking: every row written from this release
// forward carries a snapshot, and a handoff is written only for a balance that HAS a
// monitor, so an empty snapshot can only mean "predates the column" and never "no
// monitors".
//
// # A DECODE FAILURE IS PERMANENT
//
// Stored bytes do not change, so no further attempt can decode them and spending the
// remaining budget only delays the same conclusion. It is reported as permanent, exactly as
// a corrupt balance snapshot is. A failure of the live fallback read is NOT permanent: a
// database that refused this read may answer the next one.
//
// Parameters:
//   - ctx context.Context: the context for the fallback read, unused on the snapshot path.
//   - handoff model.BalanceMonitorHandoff: the claimed row.
//
// Returns:
//   - []model.BalanceMonitor: the definitions to evaluate. Empty is a legitimate answer and
//     completes the handoff with no events.
//   - error: a permanent error for an undecodable snapshot, a retryable one for a failed
//     fallback read.
func (p *BalanceMonitorHandoffProcessor) monitorsForHandoff(
	ctx context.Context, handoff model.BalanceMonitorHandoff,
) ([]model.BalanceMonitor, error) {
	monitors, snapshotted, err := handoff.Monitors()
	if err != nil {
		return nil, newPermanentMonitorHandoffError(err)
	}

	if snapshotted {
		return monitors, nil
	}

	logrus.WithFields(logrus.Fields{
		"handoff_id": handoff.HandoffID,
		"balance_id": handoff.BalanceID,
		"created_at": handoff.CreatedAt.UTC().Format(time.RFC3339),
	}).Warn(
		"balance monitor handoff carries no monitor snapshot, so it predates sql/1781252100.sql: " +
			"falling back to a LIVE read of blnk.balance_monitors for this row. Its verdict therefore " +
			"depends on the monitor definitions as they stand now rather than as they stood when the " +
			"balance's transaction committed. Every row written since that migration carries the " +
			"snapshot, so this population only shrinks",
	)

	live, err := p.blnk.getBalanceMonitorsCached(ctx, handoff.BalanceID)
	if err != nil {
		// RETRYABLE, unlike a decode failure: the database refused this read, and the next
		// attempt may succeed.
		return nil, fmt.Errorf("failed to load monitors for balance %s: %w", handoff.BalanceID, err)
	}

	return live, nil
}

// recordHandoffFailure writes an evaluation failure against the row and reports it.
//
// The failure is logged at ERROR with the attempt count on EVERY attempt, not only the
// last, because a handoff that is failing repeatedly is the signal that alerts are being
// delayed — and by the time the budget is spent the delay has already happened.
//
// A failure to record the failure is itself logged and otherwise ignored: the lease will
// expire, the row will be re-claimed, and there is nothing else this process can do about
// a database it cannot write to.
//
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - handoff model.BalanceMonitorHandoff: the row that failed.
//   - cause error: the failure.
func (p *BalanceMonitorHandoffProcessor) recordHandoffFailure(ctx context.Context, handoff model.BalanceMonitorHandoff, cause error) {
	permanent := isPermanentMonitorHandoffError(cause)

	logrus.WithError(cause).WithFields(logrus.Fields{
		"handoff_id":   handoff.HandoffID,
		"balance_id":   handoff.BalanceID,
		"attempt":      handoff.Attempts,
		"max_attempts": handoff.MaxAttempts,
		"permanent":    permanent,
	}).Error("failed to evaluate a balance monitor handoff; the alerts it would have produced are delayed")

	if err := p.blnk.datasource.MarkBalanceMonitorHandoffFailed(ctx, handoff.HandoffID, cause.Error(), permanent); err != nil {
		logrus.WithError(err).WithField("handoff_id", handoff.HandoffID).
			Error("failed to record a balance monitor handoff failure")
	}
}

// permanentMonitorHandoffError marks a failure no retry can resolve.
//
// It is a distinct type rather than a sentinel value because the underlying cause must
// survive for the log line and for last_error: an operator reading "the snapshot did not
// decode" needs the decoder's own message, not a category name.
type permanentMonitorHandoffError struct {
	cause error
}

// newPermanentMonitorHandoffError wraps a cause as permanently unrecoverable.
//
// Parameters:
//   - cause error: the underlying failure.
//
// Returns:
//   - error: the wrapped failure.
func newPermanentMonitorHandoffError(cause error) error {
	return &permanentMonitorHandoffError{cause: cause}
}

// Error renders the underlying cause, so the stored last_error and the log line carry the
// detail rather than a category.
func (e *permanentMonitorHandoffError) Error() string {
	if e == nil || e.cause == nil {
		return "the balance monitor handoff cannot be evaluated"
	}

	return e.cause.Error()
}

// Unwrap exposes the cause to errors.Is and errors.As.
func (e *permanentMonitorHandoffError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.cause
}

// isPermanentMonitorHandoffError reports whether a failure should skip the retry budget.
//
// Parameters:
//   - err error: the failure to classify.
//
// Returns:
//   - bool: true only for a failure explicitly marked permanent. Everything else is
//     treated as retryable, which is the conservative direction: a retryable failure
//     wrongly called permanent loses the alert, while a permanent one wrongly called
//     retryable only wastes the budget.
func isPermanentMonitorHandoffError(err error) bool {
	var permanent *permanentMonitorHandoffError

	return errors.As(err, &permanent)
}

// balanceMonitorHandoffEnabled reports whether the WRITER owns monitor evaluation on this
// deployment, rather than the post-commit hook.
//
// # This is a SINGLE decision read from two places, and it has to be
//
// Two call sites depend on the answer and must never disagree:
//
//   - the atomic writers, which evaluate the monitors inside the balance transaction and insert
//     the canonical alert rows there — or, with no capture registered, commit the handoff
//     instead (both through database.recordBalanceMonitorEvaluation), and
//   - the post-commit hook in transaction_execution.go, which evaluates monitors inline.
//
// If the writer captured the alert and the post-commit path also evaluated, every alert
// would be delivered twice. If neither did, a movement's monitors would be evaluated by
// nobody and the alert would be lost with nothing failing to say so. Both sides read the
// same predicate — config.Configuration.EventPublishingConfigured — so the two faults are
// unrepresentable rather than merely unlikely.
//
// # Why "Kafka is configured" is the right condition
//
// A captured alert is only useful if something publishes it, and what publishes it is the event
// relay, which refuses to run without Kafka. A deployment with no broker has neither the relay
// nor this processor, so a row written there would be one nothing can ever act on — the alert
// would simply never be delivered. Such a deployment keeps the post-commit path, which
// publishes down the legacy webhook transport exactly as it did before this feature
// existed (AAP §0.5.4).
//
// The name predates the in-transaction capture and is kept because the predicate is unchanged:
// it still answers "does the writer own this movement's monitor evaluation", which is what both
// call sites ask.
//
// Returns:
//   - bool: true when the writer owns evaluation, false when the post-commit path does.
func (l *Blnk) balanceMonitorHandoffEnabled() bool {
	if l == nil {
		return false
	}

	return l.eventConfiguration().EventPublishingConfigured()
}
