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
// relays behave alike under load and an operator has one set of numbers to reason
// about.
const (
	defaultMonitorHandoffBatchSize    = 100
	defaultMonitorHandoffPollInterval = 1 * time.Second
	defaultMonitorHandoffLockDuration = 30 * time.Second
)

// balanceMonitorEventType is the event name a fired monitor publishes under.
const balanceMonitorEventType = "balance.monitor"

// BalanceMonitorHandoffProcessor drains blnk.balance_monitor_handoff.
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
		// UNCHANGED CONDITION EVALUATION. This is the same call the post-commit path made, on
		// a balance with the same contents. only the durability of the decision's result has
		// changed.
		if !monitor.CheckCondition(balance) {
			continue
		}

		event, prepareErr := p.blnk.PrepareEventOutbox(ctx, NewWebhook{
			Event:   balanceMonitorEventType,
			Payload: monitor,
		},
			// THE LEDGER IS SUPPLIED EXPLICITLY, from the handoff rather than from the monitor.
			WithEventLedgerID(handoff.LedgerID),
			// THE DERIVED IDENTITY, and it is what makes a repeated evaluation idempotent.
			WithEventIdentity(model.BalanceMonitorEventIdentity(handoff.HandoffID, monitor.MonitorID)),
		)
		if prepareErr != nil {
			return fmt.Errorf("failed to prepare the balance.monitor event for monitor %s: %w", monitor.MonitorID, prepareErr)
		}

		// A nil row means publishing is not configured. The processor only runs when it is,
		// so this is unreachable in a healthy deployment; skipping rather than failing means
		// a configuration change mid-flight drains the backlog to completion instead of
		// failing every row in it.
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
type permanentMonitorHandoffError struct {
	cause error
}

// newPermanentMonitorHandoffError wraps a cause as permanently unrecoverable.
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

// isPermanentMonitorHandoffError reports whether a failure should skip the retry
// budget.
func isPermanentMonitorHandoffError(err error) bool {
	var permanent *permanentMonitorHandoffError

	return errors.As(err, &permanent)
}

// balanceMonitorHandoffEnabled reports whether the WRITER owns monitor evaluation on
// this deployment, rather than the post-commit hook.
func (l *Blnk) balanceMonitorHandoffEnabled() bool {
	if l == nil {
		return false
	}

	return l.eventConfiguration().EventPublishingConfigured()
}
