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

// event_retention.go holds the production caller of the event outbox's retention purge.
package blnk

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/sirupsen/logrus"
)

const (
	// defaultEventRetentionInterval is how often the sweep runs. Hourly: retention periods
	// are measured in days, so this is already far more frequent than the shortest
	// sensible period, and running it faster would add load to a table the relay is
	// claiming from for no benefit.
	defaultEventRetentionInterval = time.Hour

	// defaultEventRetentionBatchSize is how many rows one DELETE removes: the fallback for
	// a sweeper built before configuration could be read. It keeps each statement's lock
	// footprint and WAL contribution small on a table under concurrent claim.
	defaultEventRetentionBatchSize = config.DefaultEventRetentionBatchSize

	// defaultEventRetentionMaxBatchesPerSweep bounds ONE sweep by default, and the bound
	// is what makes the operation safe to run beside a live relay: without it, the first
	// sweep after retention is enabled on a long-running deployment would try to delete
	// the entire historical backlog in a single pass, holding locks and generating WAL for
	// as long as that took. With it, the backlog drains over successive sweeps.
	defaultEventRetentionMaxBatchesPerSweep = config.DefaultEventRetentionMaxBatchesPerSweep

	// eventRetentionSweepTimeout bounds one sweep. Generous, because it may issue
	// thousands of bounded deletes; bounded all the same, so a sweep against a struggling
	// database ends and is retried on the next tick rather than overlapping the one after
	// it.
	eventRetentionSweepTimeout = 10 * time.Minute
)

// eventRetentionStore is the repository surface the sweeper needs, and nothing more.
type eventRetentionStore interface {
	// PurgeTerminalEventsBefore deletes at most limit TERMINAL rows older than cutoff and
	// returns how many it removed. A returned count below the limit means the eligible set
	// is drained.
	PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// EventRetentionSweeper periodically deletes DELIVERED event rows older
// than the configured retention period.
type EventRetentionSweeper struct {
	store eventRetentionStore

	// retention is the period resolved from configuration at construction. Zero means
	// retention is disabled and the sweeper will decline to start.
	retention time.Duration

	interval  time.Duration
	batchSize int

	// maxBatches bounds one sweep. Non-positive means no ceiling, which the sweep loop reads
	// directly; the unset-versus-unbounded distinction is resolved before it reaches here, by
	// applyPurgeCapacity and WithMaxBatches.
	maxBatches int

	// now is the clock, replaceable in-package so a test can assert the cutoff exactly.
	now func() time.Time

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// NewEventRetentionSweeper builds a sweeper from a Blnk instance's datasource and
// configuration.
//
// Parameters:
//   - b *Blnk: the service container. A nil instance, or one with no datasource, yields
//     a sweeper that declines to start rather than a nil pointer the caller must guard.
//
// Returns:
//   - *EventRetentionSweeper: ready to Start. Never nil.
func NewEventRetentionSweeper(b *Blnk) *EventRetentionSweeper {
	sweeper := &EventRetentionSweeper{
		interval:   defaultEventRetentionInterval,
		batchSize:  defaultEventRetentionBatchSize,
		maxBatches: defaultEventRetentionMaxBatchesPerSweep,
		now:        time.Now,
	}

	if b == nil {
		return sweeper
	}

	if b.datasource != nil {
		sweeper.store = b.datasource
	}

	sweeper.retention = retentionPeriodFor(b.Config())
	sweeper.applyPurgeCapacity(b.Config())

	return sweeper
}

// applyPurgeCapacity reads the configured purge capacity onto the sweeper.
func (s *EventRetentionSweeper) applyPurgeCapacity(cnf *config.Configuration) {
	if cnf == nil {
		fetched, err := config.Fetch()
		if err != nil {
			return
		}

		cnf = fetched
	}

	if cnf.Relay.EventRetentionBatchSize > 0 {
		s.batchSize = cnf.Relay.EventRetentionBatchSize
	}

	// Zero is UNSET and leaves the constructor's bound in place; a negative is
	// config.EventRetentionUnboundedSweep, the explicit request for no ceiling. The
	// distinction is made in config.setRelayDefaults and repeated here rather than
	// assumed, because this method also runs against configurations that never passed
	// through it — a hand-built Configuration, or one published before the defaults were
	// applied — and in those the zero value must not silently remove the bound.
	switch {
	case cnf.Relay.EventRetentionMaxBatchesPerSweep > 0:
		s.maxBatches = cnf.Relay.EventRetentionMaxBatchesPerSweep
	case cnf.Relay.EventRetentionMaxBatchesPerSweep < 0:
		s.maxBatches = config.EventRetentionUnboundedSweep
	}
}

// retentionPeriodFor reads the configured retention period, falling back to the process
// configuration when the instance carries none.
func retentionPeriodFor(cnf *config.Configuration) time.Duration {
	if cnf != nil {
		return cnf.EventRetentionPeriod()
	}

	fetched, err := config.Fetch()
	if err != nil {
		return 0
	}

	return fetched.EventRetentionPeriod()
}

// WithInterval sets how often the sweep runs.
//
// Parameters:
//   - interval time.Duration: the sweep interval.
//
// Returns:
//   - *EventRetentionSweeper: the receiver, for chaining.
func (s *EventRetentionSweeper) WithInterval(interval time.Duration) *EventRetentionSweeper {
	if interval <= 0 {
		logrus.WithField("requested_interval", interval.String()).
			Warnf("Non-positive event retention interval; falling back to %s", defaultEventRetentionInterval)

		return s
	}

	s.interval = interval

	return s
}

// WithBatchSize sets how many rows one delete removes.
//
// Parameters:
//   - size int: the batch size. Non-positive falls back to the default.
//
// Returns:
//   - *EventRetentionSweeper: the receiver, for chaining.
func (s *EventRetentionSweeper) WithBatchSize(size int) *EventRetentionSweeper {
	if size <= 0 {
		logrus.WithField("requested_batch_size", size).
			Warnf("Non-positive event retention batch size; falling back to %d", defaultEventRetentionBatchSize)

		return s
	}

	s.batchSize = size

	return s
}

// WithMaxBatches sets how many batches one sweep may issue.
//
// Parameters:
//   - batches int: the per-sweep batch ceiling. Negative means unbounded; zero keeps
//     the default.
//
// Returns:
//   - *EventRetentionSweeper: the receiver, for chaining.
func (s *EventRetentionSweeper) WithMaxBatches(batches int) *EventRetentionSweeper {
	switch {
	case batches > 0:
		s.maxBatches = batches
	case batches < 0:
		s.maxBatches = config.EventRetentionUnboundedSweep
	default:
		logrus.WithField("requested_max_batches", batches).
			Warnf(
				"A zero event retention batch ceiling is unset rather than unbounded; keeping %d. "+
					"Pass a negative value to remove the ceiling deliberately",
				s.maxBatches,
			)
	}

	return s
}

// StartupObstacle reports why the sweeper must not run, or nil when it may.
//
// Returns:
//   - error: the reason the sweeper must not run, or nil.
func (s *EventRetentionSweeper) StartupObstacle() error {
	if s == nil {
		return errEventRetentionSweeperNil
	}

	if s.retention <= 0 {
		return ErrEventRetentionDisabled
	}

	if s.store == nil {
		return errEventRetentionNoDatasource
	}

	return nil
}

// Start begins the periodic sweep. It is a no-op when the sweeper must not run, and calling
// it twice is a no-op the second time.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the loop.
func (s *EventRetentionSweeper) Start(ctx context.Context) {
	if obstacle := s.StartupObstacle(); obstacle != nil {
		return
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()

		return
	}
	s.running = true
	stop := make(chan struct{})
	s.stopCh = stop
	// Under the same lock that publishes running, so a Stop landing here cannot return from
	// a Wait with nothing to wait for while the loop goroutine has yet to start.
	s.wg.Add(1)
	s.mu.Unlock()

	logrus.WithFields(logrus.Fields{
		"retention":  s.retention.String(),
		"interval":   s.interval.String(),
		"batch_size": s.batchSize,
	}).Info("event outbox retention sweeper started")

	go func() {
		defer s.wg.Done()
		defer s.clearRunning()

		// The channel is PASSED IN, captured here from the same locked section that created
		// it, rather than read from the field inside the loop. The field is cleared by Stop,
		// and a Stop that lands between this goroutine being scheduled and its first read of
		// the field would hand the loop a nil channel — which in a select blocks forever, so
		// the loop would never see the stop signal and Stop's Wait would never return. That
		// is not a hypothetical: it deadlocked the lifecycle test on the first run.
		s.run(ctx, stop)
	}()
}

// Stop halts the loop and waits for the sweep in flight.
func (s *EventRetentionSweeper) Stop() {
	s.mu.Lock()
	// Nilled as well as closed, so a second Stop cannot close an already-closed channel and
	// panic. Safe to nil because the loop holds its own reference and never reads the field.
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
	s.mu.Unlock()

	// Outside the lock, and called unconditionally: a Stop that raced a Start must still
	// wait for whatever that Start began.
	s.wg.Wait()
}

// clearRunning marks the sweeper stopped. It runs when the loop exits for ANY reason,
// including context cancellation, so IsRunning cannot report a cancelled sweeper as live
// and a later Start is not refused by the idempotence guard.
func (s *EventRetentionSweeper) clearRunning() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
}

// IsRunning reports whether the loop is live.
//
// Returns:
//   - bool: true between a successful Start and the loop's exit.
func (s *EventRetentionSweeper) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.running
}

// run is the ticker loop.
func (s *EventRetentionSweeper) run(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("event outbox retention sweeper stopping: context cancelled")

			return
		case <-stop:
			logrus.Info("event outbox retention sweeper stopping")

			return
		case <-ticker.C:
			s.Sweep(ctx)
		}
	}
}

// Sweep performs ONE bounded retention sweep and returns how many rows it deleted.
//
// Parameters:
//   - ctx context.Context: cancels the sweep. A deadline of its own is applied on top.
//
// Returns:
//   - int64: how many rows were deleted.
func (s *EventRetentionSweeper) Sweep(ctx context.Context) int64 {
	if obstacle := s.StartupObstacle(); obstacle != nil {
		return 0
	}

	sweepCtx, cancel := context.WithTimeout(ctx, eventRetentionSweepTimeout)
	defer cancel()

	cutoff := s.now().UTC().Add(-s.retention)

	var (
		deleted int64

		// drained records WHY the loop ended, which is the difference between a sweep that
		// finished its work and one that ran out of the capacity it was given. Only the
		// second is worth an operator's attention, and without this the two are
		// indistinguishable in the logs.
		drained bool
	)

	for batch := 0; s.maxBatches <= 0 || batch < s.maxBatches; batch++ {
		if sweepCtx.Err() != nil {
			break
		}

		purged, err := s.store.PurgeTerminalEventsBefore(sweepCtx, cutoff, s.batchSize)
		if err != nil {
			withLoggableCause(logrus.WithFields(logrus.Fields{
				"cutoff":  cutoff.Format(time.RFC3339),
				"deleted": deleted,
			}), err).Error(
				"event outbox retention: a purge batch failed; the rows remain and the sweep is " +
					"retried on the next tick",
			)

			break
		}

		deleted += purged

		if purged < int64(s.batchSize) {
			// The eligible set is drained. This is the ordinary exit.
			drained = true

			break
		}
	}

	// The condition the capacity ceiling exists to make visible. A sweep that deleted a full batch on
	// its last permitted iteration left eligible rows behind, which means this
	// deployment's purge capacity is at or below its arrival rate and the retention period
	// is consequently NOT being enforced however it is configured. It is reported at
	// warning level naming the setting to raise, because the alternative — the previous
	// behaviour — was a table that grew without anything ever saying so.
	if !drained && s.maxBatches > 0 && deleted >= int64(s.maxBatches)*int64(s.batchSize) {
		logrus.WithFields(logrus.Fields{
			"deleted":                     deleted,
			"cutoff":                      cutoff.Format(time.RFC3339),
			"max_batches_per_sweep":       s.maxBatches,
			"batch_size":                  s.batchSize,
			"purge_capacity_rows_per_arm": int64(s.maxBatches) * int64(s.batchSize),
		}).Warn(
			"event outbox retention: the sweep exhausted its per-sweep batch ceiling with eligible " +
				"rows remaining, so the retention period is not being enforced; raise " +
				"RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP (or RELAY_EVENT_RETENTION_BATCH_SIZE) " +
				"until purge capacity exceeds the event arrival rate",
		)
	}

	if deleted == 0 {
		// Nothing to delete is the healthy steady state on a deployment whose retention
		// period exceeds its event age, so it is logged at debug rather than adding an
		// hourly line to every operator's log.
		logrus.WithField("cutoff", cutoff.Format(time.RFC3339)).
			Debug("event outbox retention: no terminal rows were older than the retention cutoff")

		return 0
	}

	// RECORDED HERE, on the code path that performs the deletion, because this is a
	// COUNTER and not a gauge: it only accumulates, so incrementing it where the thing it
	// counts happens is exactly right. The gauges in event_metrics.go are the opposite
	// case and have their own single owner for that reason.
	if metrics.EventsPurgedTotal != nil {
		metrics.EventsPurgedTotal.Add(ctx, deleted)
	}

	logrus.WithFields(logrus.Fields{
		"deleted":   deleted,
		"cutoff":    cutoff.Format(time.RFC3339),
		"retention": s.retention.String(),
	}).Info("event outbox retention: deleted terminal event rows older than the retention cutoff")

	return deleted
}

// The three obstacles, as values rather than freshly-built errors.
var (
	// ErrEventRetentionDisabled means RELAY_EVENT_RETENTION_DAYS is unset or zero. Not a
	// fault: retention ships switched off because deleting ledger-adjacent records is an
	// operator's decision, not a default.
	ErrEventRetentionDisabled = errors.New(
		"blnk: event outbox retention is disabled; set RELAY_EVENT_RETENTION_DAYS to a positive " +
			"number of days to have delivered events deleted after that period",
	)

	errEventRetentionSweeperNil = errors.New("blnk: the event retention sweeper is nil")

	errEventRetentionNoDatasource = errors.New(
		"blnk: event outbox retention is configured but this Blnk instance has no datasource, so " +
			"nothing can be purged",
	)
)
