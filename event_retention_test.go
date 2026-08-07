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

package blnk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retentionFixedNow pins the sweeper's clock so the cutoff can be asserted exactly rather
// than within a tolerance. A tolerance would also accept a cutoff computed in the wrong
// unit, which is the mistake that matters here: hours instead of days deletes rows the
// operator expected to keep for a month.
var retentionFixedNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// retentionPurgeCall is one recorded PurgeTerminalEventsBefore call.
type retentionPurgeCall struct {
	cutoff time.Time
	limit  int
}

// retentionFakeStore records purge calls and serves a scripted sequence of results, so a
// multi-batch sweep and a mid-sweep failure are both reachable without a database.
type retentionFakeStore struct {
	mu sync.Mutex

	calls []retentionPurgeCall

	// results is consumed one entry per call. When it runs out the store reports zero
	// deletions, which ends a sweep the way a drained eligible set does.
	results []int64
	err     error
	errAt   int
}

func (s *retentionFakeStore) PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	index := len(s.calls)
	s.calls = append(s.calls, retentionPurgeCall{cutoff: cutoff, limit: limit})

	if s.err != nil && index == s.errAt {
		return 0, s.err
	}

	if index < len(s.results) {
		return s.results[index], nil
	}

	return 0, nil
}

func (s *retentionFakeStore) snapshot() []retentionPurgeCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]retentionPurgeCall(nil), s.calls...)
}

// newRetentionSweeper builds a sweeper over the fake store with the clock pinned.
func newRetentionSweeper(store *retentionFakeStore, retention time.Duration) *EventRetentionSweeper {
	return &EventRetentionSweeper{
		store:     store,
		retention: retention,
		interval:  defaultEventRetentionInterval,
		batchSize: defaultEventRetentionBatchSize,
		now:       func() time.Time { return retentionFixedNow },
	}
}

// TestEventRetentionSweeper_IsDisabledUntilAPeriodIsConfigured asserts the default, and
// asserts it as a SAFETY property rather than a convenience.
//
// Deleting ledger-adjacent records is a decision only an operator can take: the period a
// jurisdiction, an audit programme or a legal hold requires is not something a default can
// guess, and a default that silently deleted evidence would be far worse than one that keeps
// too much. So the mechanism ships switched off, and the disabled state is reported as a
// distinct, exported error so the caller can log it as the unremarkable default it is.
func TestEventRetentionSweeper_IsDisabledUntilAPeriodIsConfigured(t *testing.T) {
	store := &retentionFakeStore{}

	for name, retention := range map[string]time.Duration{
		"unset":    0,
		"negative": -24 * time.Hour,
	} {
		t.Run(name, func(t *testing.T) {
			sweeper := newRetentionSweeper(store, retention)

			obstacle := sweeper.StartupObstacle()
			require.Error(t, obstacle)
			assert.ErrorIs(t, obstacle, ErrEventRetentionDisabled,
				"the disabled state must be recognisable by errors.Is, so the caller can log it at "+
					"info rather than as a warning nobody should act on")
			assert.Contains(t, obstacle.Error(), "RELAY_EVENT_RETENTION_DAYS",
				"the message must name the variable, because an operator who believes retention is "+
					"on needs to be able to discover that it is not")

			assert.Zero(t, sweeper.Sweep(context.Background()),
				"a disabled sweeper must delete nothing")
			assert.Empty(t, store.snapshot(),
				"and must issue no statement at all — not even one bounded by a cutoff so old it "+
					"happens to match nothing")
		})
	}
}

// TestEventRetentionSweeper_RefusesToRunWithoutADatasource asserts the other obstacle, and
// that it is NOT the disabled one.
//
// The distinction is what lets the caller log them differently: retention switched off is the
// default, while retention configured and not running means the control the operator asked for
// is absent and the table is quietly growing.
func TestEventRetentionSweeper_RefusesToRunWithoutADatasource(t *testing.T) {
	sweeper := &EventRetentionSweeper{retention: 30 * 24 * time.Hour}

	obstacle := sweeper.StartupObstacle()
	require.Error(t, obstacle)
	assert.NotErrorIs(t, obstacle, ErrEventRetentionDisabled,
		"a missing datasource is a wiring defect, not the disabled default, and conflating them "+
			"would have the caller log a real problem at info")
	assert.Contains(t, obstacle.Error(), "datasource")
}

// TestEventRetentionSweeper_ComputesTheCutoffFromTheConfiguredPeriodInDays is the arithmetic
// assertion, and it is the one with teeth.
//
// The configured value is DAYS and the cutoff is an instant, so the conversion is where an
// order-of-magnitude error hides — hours instead of days would delete rows an operator
// expected to keep for a month, irreversibly and without any error to notice. The clock is
// pinned so the expected instant is exact.
func TestEventRetentionSweeper_ComputesTheCutoffFromTheConfiguredPeriodInDays(t *testing.T) {
	for name, testCase := range map[string]struct {
		days       int
		wantCutoff time.Time
	}{
		"thirty days":  {days: 30, wantCutoff: retentionFixedNow.AddDate(0, 0, -30)},
		"ninety days":  {days: 90, wantCutoff: retentionFixedNow.AddDate(0, 0, -90)},
		"a single day": {days: 1, wantCutoff: retentionFixedNow.AddDate(0, 0, -1)},
	} {
		t.Run(name, func(t *testing.T) {
			cnf := &config.Configuration{Relay: config.RelayConfig{EventRetentionDays: testCase.days}}
			store := &retentionFakeStore{}
			sweeper := newRetentionSweeper(store, cnf.EventRetentionPeriod())

			require.NoError(t, sweeper.StartupObstacle())
			sweeper.Sweep(context.Background())

			calls := store.snapshot()
			require.Len(t, calls, 1)
			assert.Equal(t, testCase.wantCutoff.UTC(), calls[0].cutoff,
				"the cutoff must be exactly the configured number of DAYS before now; a unit error "+
					"here deletes ledger evidence the operator expected to keep")
			assert.Equal(t, defaultEventRetentionBatchSize, calls[0].limit,
				"and every delete must be bounded, so it cannot lock a table the relay is claiming from")
		})
	}
}

// TestEventRetentionSweeper_SweepsInBoundedBatchesUntilTheEligibleSetIsDrained asserts the
// loop's two exits.
//
// The bound is what makes this safe to run beside a live relay: an unbounded DELETE over a
// large backlog would hold locks for its whole duration and bloat the WAL in one transaction.
// Stopping on the first SHORT batch is what stops the sweep issuing a pointless extra
// statement every hour once the backlog is gone.
func TestEventRetentionSweeper_SweepsInBoundedBatchesUntilTheEligibleSetIsDrained(t *testing.T) {
	t.Run("a full batch continues and a short batch ends the sweep", func(t *testing.T) {
		store := &retentionFakeStore{results: []int64{
			defaultEventRetentionBatchSize,
			defaultEventRetentionBatchSize,
			7,
		}}
		sweeper := newRetentionSweeper(store, 30*24*time.Hour)

		assert.Equal(t, int64(2*defaultEventRetentionBatchSize+7), sweeper.Sweep(context.Background()),
			"the sweep must report every row it deleted across all its batches")

		calls := store.snapshot()
		assert.Len(t, calls, 3,
			"a short batch means the eligible set is drained, so no fourth statement may be issued")

		cutoff := calls[0].cutoff
		for _, call := range calls {
			assert.Equal(t, cutoff, call.cutoff,
				"every batch in one sweep must use the SAME cutoff; recomputing it per batch would "+
					"let the boundary drift forward mid-sweep")
		}
	})

	t.Run("one sweep is bounded even when the backlog is not", func(t *testing.T) {
		// Every batch comes back full, so the eligible set never drains. Without the
		// per-sweep bound this loop would run until the timeout, holding a table the relay
		// is claiming from for ten minutes on the first sweep after retention is enabled.
		results := make([]int64, maxEventRetentionBatchesPerSweep+50)
		for i := range results {
			results[i] = defaultEventRetentionBatchSize
		}

		store := &retentionFakeStore{results: results}
		sweeper := newRetentionSweeper(store, 30*24*time.Hour)

		sweeper.Sweep(context.Background())

		assert.Len(t, store.snapshot(), maxEventRetentionBatchesPerSweep,
			"a single sweep must stop at the batch bound and let the next tick continue; the "+
				"backlog drains over successive sweeps rather than in one pass")
	})
}

// TestEventRetentionSweeper_StopsOnAPurgeFailureAndReportsWhatItDeleted asserts a mid-sweep
// failure is reported and does not lose the count of what already succeeded.
//
// Each delete is its own committed statement, so an interrupted sweep has no partial state to
// repair — the rows that were deleted are gone and the rest are simply still there. Returning
// the partial count matters because it is what a caller records as the metric, and reporting
// zero would make a partially-successful sweep look like a sweep that did nothing.
func TestEventRetentionSweeper_StopsOnAPurgeFailureAndReportsWhatItDeleted(t *testing.T) {
	store := &retentionFakeStore{
		results: []int64{defaultEventRetentionBatchSize, defaultEventRetentionBatchSize},
		err:     errors.New("retention test: database unavailable"),
		errAt:   1,
	}
	sweeper := newRetentionSweeper(store, 30*24*time.Hour)

	assert.Equal(t, int64(defaultEventRetentionBatchSize), sweeper.Sweep(context.Background()),
		"the rows the first batch deleted are committed and gone, so the sweep must report them")
	assert.Len(t, store.snapshot(), 2,
		"the sweep must stop at the failure rather than hammering a database that is already "+
			"struggling; the remaining rows are eligible again on the next tick")
}

// TestEventRetentionSweeper_HonoursCancellationMidSweep asserts a shutdown ends the sweep
// promptly.
//
// Unlike the relay's bookkeeping, retention owes nothing: a row not yet deleted is simply a
// row that is still there, and it will be eligible on the next sweep. So cancellation is
// honoured rather than detached from, which is the opposite of the dead-letter hand-off's
// treatment and correct for the same underlying reason — what is owed, and what is not.
func TestEventRetentionSweeper_HonoursCancellationMidSweep(t *testing.T) {
	results := make([]int64, 10)
	for i := range results {
		results[i] = defaultEventRetentionBatchSize
	}

	store := &retentionFakeStore{results: results}
	sweeper := newRetentionSweeper(store, 30*24*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Zero(t, sweeper.Sweep(ctx),
		"a cancelled sweep must delete nothing further")
	assert.Empty(t, store.snapshot(),
		"and must not issue a statement after cancellation")
}

// TestEventRetentionSweeper_LifecycleStartsStopsAndIsRestartable mirrors the relay's
// lifecycle assertions, because the same defects are available to any worker built on this
// shape: a Stop that returns before the loop is accounted for, and a running flag that
// survives the loop and makes a later Start a silent no-op.
func TestEventRetentionSweeper_LifecycleStartsStopsAndIsRestartable(t *testing.T) {
	store := &retentionFakeStore{}
	sweeper := newRetentionSweeper(store, 30*24*time.Hour).WithInterval(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sweeper.Start(ctx)
	require.True(t, sweeper.IsRunning())

	// A second Start is a no-op rather than a second loop.
	sweeper.Start(ctx)
	assert.True(t, sweeper.IsRunning())

	sweeper.Stop()
	assert.False(t, sweeper.IsRunning(),
		"the running flag must be cleared when the loop exits, or a health check lies and the "+
			"next Start is refused by the idempotence guard")

	// Restartable in the same process.
	restartCtx, restartCancel := context.WithCancel(context.Background())
	defer restartCancel()

	sweeper.Start(restartCtx)
	assert.True(t, sweeper.IsRunning())
	sweeper.Stop()
	assert.False(t, sweeper.IsRunning())
}

// TestEventRetentionSweeper_ADisabledSweeperNeverStarts asserts the guard is in Start and not
// only in the caller, so a caller that forgot to check cannot begin deleting.
func TestEventRetentionSweeper_ADisabledSweeperNeverStarts(t *testing.T) {
	store := &retentionFakeStore{}
	sweeper := newRetentionSweeper(store, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sweeper.Start(ctx)

	assert.False(t, sweeper.IsRunning(),
		"a disabled sweeper must not start even when Start is called directly; the guard cannot "+
			"live only in the caller when what it guards is a destructive operation")

	assert.NotPanics(t, sweeper.Stop, "and stopping one that never started must be safe")
}

// TestEventRetentionSweeper_ConfiguratorsRejectNonPositiveValues asserts a misconfigured
// cadence or batch size cannot switch retention off.
//
// The failure mode of accepting them is silent: an interval of zero would make time.NewTicker
// panic, and a batch size of zero would have the repository substitute its own default while
// the sweeper's short-batch test compared against zero and ended every sweep after one
// statement. Falling back is the safe direction for a control whose absence has no symptom.
func TestEventRetentionSweeper_ConfiguratorsRejectNonPositiveValues(t *testing.T) {
	store := &retentionFakeStore{}
	sweeper := newRetentionSweeper(store, 30*24*time.Hour)

	sweeper.WithInterval(0).WithInterval(-time.Second)
	assert.Equal(t, defaultEventRetentionInterval, sweeper.interval)

	sweeper.WithBatchSize(0).WithBatchSize(-10)
	assert.Equal(t, defaultEventRetentionBatchSize, sweeper.batchSize)

	// And a valid value is applied, so the fallback is not simply ignoring the setter.
	sweeper.WithInterval(5 * time.Minute).WithBatchSize(250)
	assert.Equal(t, 5*time.Minute, sweeper.interval)
	assert.Equal(t, 250, sweeper.batchSize)
}

// TestNewEventRetentionSweeper_ReadsTheConfiguredPeriodFromTheInstance asserts the
// constructor resolves retention from configuration rather than leaving it to the caller.
func TestNewEventRetentionSweeper_ReadsTheConfiguredPeriodFromTheInstance(t *testing.T) {
	t.Run("a nil instance yields a sweeper that declines rather than a nil pointer", func(t *testing.T) {
		sweeper := NewEventRetentionSweeper(nil)
		require.NotNil(t, sweeper)
		assert.Error(t, sweeper.StartupObstacle())
		assert.NotPanics(t, func() { sweeper.Sweep(context.Background()) })
	})

	t.Run("the configured period is read from the instance", func(t *testing.T) {
		instance := &Blnk{config: &config.Configuration{
			Relay: config.RelayConfig{EventRetentionDays: 45},
		}}

		sweeper := NewEventRetentionSweeper(instance)
		assert.Equal(t, 45*24*time.Hour, sweeper.retention,
			"the constructor must resolve the period from configuration; a caller computing it "+
				"separately is how two places end up disagreeing about how much to delete")
	})
}
