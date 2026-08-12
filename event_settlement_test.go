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

// This file owns the SETTLEMENT LOOP: the worker that finishes broker-side subscriber
// work no request could complete.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// settlementFixedNow pins the processor's clock so the retry bound can be asserted exactly
// rather than within a tolerance. A tolerance would also accept a bound computed in the wrong
// direction — now PLUS the interval instead of minus — which would make every row eligible on
// every pass and remove the pacing altogether.
var settlementFixedNow = time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)

// settlementListCall is one recorded ListSubscriberSettlementObligations call, so the bound and
// the batch size can be asserted on the REQUEST rather than inferred from the result.
type settlementListCall struct {
	limit     int
	notBefore time.Time
}

// settlementAttempt is one recorded MarkSubscriberSettlementAttempt call.
type settlementAttempt struct {
	subscriberID string
	attemptedAt  time.Time
	failure      string
}

// settlementFakeStore serves a scripted backlog and records what the loop asked of it.
type settlementFakeStore struct {
	mu sync.Mutex

	outstanding []model.SubscriberSettlementObligation
	listErr     error

	lists    []settlementListCall
	attempts []settlementAttempt
	markErr  error
}

func (s *settlementFakeStore) ListSubscriberSettlementObligations(
	ctx context.Context,
	limit int,
	notBefore time.Time,
) ([]model.SubscriberSettlementObligation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lists = append(s.lists, settlementListCall{limit: limit, notBefore: notBefore})

	if s.listErr != nil {
		return nil, s.listErr
	}

	return s.outstanding, nil
}

func (s *settlementFakeStore) MarkSubscriberSettlementAttempt(
	_ context.Context,
	subscriberID string,
	attemptedAt time.Time,
	failure string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attempts = append(s.attempts, settlementAttempt{
		subscriberID: subscriberID,
		attemptedAt:  attemptedAt,
		failure:      failure,
	})

	return s.markErr
}

// recordedLists returns a copy of the recorded enumeration requests.
func (s *settlementFakeStore) recordedLists() []settlementListCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]settlementListCall(nil), s.lists...)
}

// recordedAttempts returns a copy of the recorded attempts.
func (s *settlementFakeStore) recordedAttempts() []settlementAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]settlementAttempt(nil), s.attempts...)
}

// settlementFakeSettler records which subscribers the loop asked it to settle, and can fail for
// named ones — which is how a partially successful pass is driven.
type settlementFakeSettler struct {
	mu sync.Mutex

	settled  []string
	failures map[string]error

	// blockFor makes one subscriber's remedy hang until its context expires, which is the only
	// way to reach the per-subscriber budget.
	blockFor string
}

func (f *settlementFakeSettler) SettleSubscriber(ctx context.Context, subscriberID string) error {
	f.mu.Lock()
	f.settled = append(f.settled, subscriberID)
	blocking := f.blockFor == subscriberID
	failure := f.failures[subscriberID]
	f.mu.Unlock()

	if blocking {
		<-ctx.Done()

		return ctx.Err()
	}

	return failure
}

// settledSubscribers returns a copy of the recorded remedies.
func (f *settlementFakeSettler) settledSubscribers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.settled...)
}

// newSettlementProcessor builds a processor over the two doubles, with the clock pinned and the
// broker reported as configured. Constructing it directly rather than through
// NewSubscriberSettlementProcessor is deliberate: the constructor's own configuration reading has
// its own test, and every test here is about the loop.
func newSettlementProcessor(
	store subscriberSettlementStore,
	settler subscriberSettler,
) *SubscriberSettlementProcessor {
	return &SubscriberSettlementProcessor{
		store:         store,
		settler:       settler,
		configured:    true,
		interval:      defaultSubscriberSettlementInterval,
		batchSize:     defaultSubscriberSettlementBatchSize,
		retryInterval: defaultSubscriberSettlementRetryInterval,
		now:           func() time.Time { return settlementFixedNow },
	}
}

// settlementOwing builds one outstanding obligation.
func settlementOwing(subscriberID string, grant, credential bool) model.SubscriberSettlementObligation {
	return model.SubscriberSettlementObligation{
		SubscriberID:             subscriberID,
		GrantReconcilePending:    grant,
		CredentialCleanupPending: credential,
	}
}

// TestSubscriberSettlementProcessor_IsInactiveWithoutABroker keeps the no-Kafka steady
// state working.
//
// A deployment with no broker has no broker-side subscriber state, so there is nothing
// that could diverge from the registry.
func TestSubscriberSettlementProcessor_IsInactiveWithoutABroker(t *testing.T) {
	processor := newSettlementProcessor(&settlementFakeStore{}, &settlementFakeSettler{})
	processor.configured = false

	require.ErrorIs(t, processor.StartupObstacle(), ErrSubscriberSettlementDisabled)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor.Start(ctx)
	assert.False(t, processor.IsRunning(),
		"the guard must be in Start and not only in the caller")

	assert.Zero(t, processor.Pass(ctx),
		"and a directly invoked pass must do nothing too")
	assert.NotPanics(t, processor.Stop, "stopping one that never started must be safe")
}

// TestSubscriberSettlementProcessor_RefusesToRunWithoutItsCollaborators covers the two
// wiring defects.
func TestSubscriberSettlementProcessor_RefusesToRunWithoutItsCollaborators(t *testing.T) {
	t.Run("no registry", func(t *testing.T) {
		processor := newSettlementProcessor(nil, &settlementFakeSettler{})

		obstacle := processor.StartupObstacle()
		require.Error(t, obstacle)
		assert.NotErrorIs(t, obstacle, ErrSubscriberSettlementDisabled,
			"a missing datasource is a wiring defect, not the legitimate no-broker steady state")
	})

	t.Run("no settler", func(t *testing.T) {
		processor := newSettlementProcessor(&settlementFakeStore{}, nil)

		obstacle := processor.StartupObstacle()
		require.Error(t, obstacle)
		assert.NotErrorIs(t, obstacle, ErrSubscriberSettlementDisabled)
	})

	t.Run("nil processor", func(t *testing.T) {
		var processor *SubscriberSettlementProcessor

		require.Error(t, processor.StartupObstacle(),
			"a nil processor must report an obstacle rather than panic, so a caller can defer "+
				"unconditionally")
	})
}

// TestSubscriberSettlementProcessor_PacesRetriesFromItsOwnClock is the pacing contract.
//
// Settlement talks to the same broker that just failed, so a pass that re-attempted
// every row every minute would hammer it and bury the log in one subscriber.
func TestSubscriberSettlementProcessor_PacesRetriesFromItsOwnClock(t *testing.T) {
	store := &settlementFakeStore{}
	processor := newSettlementProcessor(store, &settlementFakeSettler{}).
		WithRetryInterval(5 * time.Minute).
		WithBatchSize(7)

	processor.Pass(context.Background())

	lists := store.recordedLists()
	require.Len(t, lists, 1, "one enumeration per pass, so the cost does not grow with the backlog")
	assert.Equal(t, 7, lists[0].limit)
	assert.Equal(t, settlementFixedNow.Add(-5*time.Minute).UTC(), lists[0].notBefore,
		"rows attempted WITHIN the retry interval are skipped; a bound in the future would "+
			"re-attempt everything on every pass")
}

// TestSubscriberSettlementProcessor_SettlesEveryOutstandingObligation is the happy path.
func TestSubscriberSettlementProcessor_SettlesEveryOutstandingObligation(t *testing.T) {
	store := &settlementFakeStore{outstanding: []model.SubscriberSettlementObligation{
		settlementOwing("sub_a", true, false),
		settlementOwing("sub_b", false, true),
		settlementOwing("sub_c", true, true),
	}}
	settler := &settlementFakeSettler{}

	assert.Equal(t, 3, newSettlementProcessor(store, settler).Pass(context.Background()))
	assert.Equal(t, []string{"sub_a", "sub_b", "sub_c"}, settler.settledSubscribers())

	attempts := store.recordedAttempts()
	require.Len(t, attempts, 3, "every attempt is recorded, successful or not")

	for _, attempt := range attempts {
		assert.Empty(t, attempt.failure, "a successful remedy records no failure text")
		assert.Equal(t, settlementFixedNow.UTC(), attempt.attemptedAt,
			"the instant comes from the processor's clock, which is what makes the pacing testable")
	}
}

// TestSubscriberSettlementProcessor_RecordsAFailedAttemptAndKeepsGoing is the property that
// stops one stuck subscriber from stalling the whole backlog.
func TestSubscriberSettlementProcessor_RecordsAFailedAttemptAndKeepsGoing(t *testing.T) {
	store := &settlementFakeStore{outstanding: []model.SubscriberSettlementObligation{
		settlementOwing("sub_stuck", true, false),
		settlementOwing("sub_ok", true, false),
	}}
	settler := &settlementFakeSettler{failures: map[string]error{
		"sub_stuck": errors.New("broker still unreachable"),
	}}

	assert.Equal(t, 1, newSettlementProcessor(store, settler).Pass(context.Background()),
		"only the remedy that completed counts as settled")
	assert.Equal(t, []string{"sub_stuck", "sub_ok"}, settler.settledSubscribers(),
		"a failure must not abandon the rest of the batch")

	attempts := store.recordedAttempts()
	require.Len(t, attempts, 2)
	assert.Equal(t, "broker still unreachable", attempts[0].failure,
		"the failure is recorded on the row, so the next pass paces itself and an operator can "+
			"see WHY without reading a log")
	assert.Empty(t, attempts[1].failure)
}

// TestSubscriberSettlementProcessor_RecordsTheAttemptEvenWhenTheRemedyExhaustedItsBudget
// is why the pacing write runs on a detached context.
func TestSubscriberSettlementProcessor_RecordsTheAttemptEvenWhenTheRemedyExhaustedItsBudget(t *testing.T) {
	store := &settlementFakeStore{outstanding: []model.SubscriberSettlementObligation{
		settlementOwing("sub_hanging", true, false),
	}}
	settler := &settlementFakeSettler{blockFor: "sub_hanging"}

	processor := newSettlementProcessor(store, settler)

	// The remedy blocks until its context ends, and the pass's own deadline is what ends it.
	passCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	assert.Zero(t, processor.Pass(passCtx))

	attempts := store.recordedAttempts()
	require.Len(t, attempts, 1,
		"the attempt is recorded on a context DETACHED from the one that expired; recorded on "+
			"the expired context it would be lost, and the row would be retried without pacing")
	assert.NotEmpty(t, attempts[0].failure)
}

// TestSubscriberSettlementProcessor_ContinuesWhenThePacingWriteFails keeps a
// bookkeeping problem from becoming a settlement problem.
//
// The remedy succeeded, so the obligation is discharged and the pass reports it.
func TestSubscriberSettlementProcessor_ContinuesWhenThePacingWriteFails(t *testing.T) {
	store := &settlementFakeStore{
		outstanding: []model.SubscriberSettlementObligation{settlementOwing("sub_a", true, false)},
		markErr:     errors.New("registry write path unavailable"),
	}

	assert.Equal(t, 1, newSettlementProcessor(store, &settlementFakeSettler{}).Pass(context.Background()),
		"the remedy completed, so the obligation is settled whatever happened to the pacing write")
}

// TestSubscriberSettlementProcessor_AttemptsNothingWhenTheBacklogCannotBeRead is the
// fail-safe direction.
//
// An unreadable backlog is not an empty one.
func TestSubscriberSettlementProcessor_AttemptsNothingWhenTheBacklogCannotBeRead(t *testing.T) {
	store := &settlementFakeStore{listErr: errors.New("registry read path unavailable")}
	settler := &settlementFakeSettler{}

	assert.Zero(t, newSettlementProcessor(store, settler).Pass(context.Background()))
	assert.Empty(t, settler.settledSubscribers())
	assert.Empty(t, store.recordedAttempts(),
		"nothing was attempted, so nothing may be recorded as attempted")
}

// TestSubscriberSettlementProcessor_AnEmptyBacklogIsTheHealthySteadyState asserts the ordinary
// case costs one query and nothing else.
func TestSubscriberSettlementProcessor_AnEmptyBacklogIsTheHealthySteadyState(t *testing.T) {
	store := &settlementFakeStore{}
	settler := &settlementFakeSettler{}

	assert.Zero(t, newSettlementProcessor(store, settler).Pass(context.Background()))
	assert.Empty(t, settler.settledSubscribers())
	assert.Len(t, store.recordedLists(), 1)
}

// TestSubscriberSettlementProcessor_LifecycleStartsStopsAndIsRestartable pins the
// lifecycle this worker shares with every other background worker here.
func TestSubscriberSettlementProcessor_LifecycleStartsStopsAndIsRestartable(t *testing.T) {
	processor := newSettlementProcessor(&settlementFakeStore{}, &settlementFakeSettler{}).
		WithInterval(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor.Start(ctx)
	require.True(t, processor.IsRunning())

	// A second Start is a no-op rather than a second loop claiming the same rows.
	processor.Start(ctx)
	assert.True(t, processor.IsRunning())

	processor.Stop()
	assert.False(t, processor.IsRunning(),
		"the running flag must be cleared when the loop exits, or a health check lies and the "+
			"next Start is refused by the idempotence guard")

	restartCtx, restartCancel := context.WithCancel(context.Background())
	defer restartCancel()

	processor.Start(restartCtx)
	assert.True(t, processor.IsRunning())
	processor.Stop()
	assert.False(t, processor.IsRunning())

	assert.NotPanics(t, processor.Stop, "a second Stop must not close an already-closed channel")
}

// TestSubscriberSettlementProcessor_TheLoopSettlesOnEveryTick proves the ticker is wired to the
// pass rather than merely running.
func TestSubscriberSettlementProcessor_TheLoopSettlesOnEveryTick(t *testing.T) {
	store := &settlementFakeStore{outstanding: []model.SubscriberSettlementObligation{
		settlementOwing("sub_a", true, false),
	}}
	settler := &settlementFakeSettler{}

	processor := newSettlementProcessor(store, settler).WithInterval(2 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor.Start(ctx)

	require.Eventually(t, func() bool {
		return len(settler.settledSubscribers()) > 0
	}, 2*time.Second, 5*time.Millisecond,
		"the ticker must drive the pass; a loop that ticks and settles nothing is the failure "+
			"mode where the backlog grows with a healthy-looking worker")

	processor.Stop()
}

// TestSubscriberSettlementProcessor_HonoursCancellationMidPass asserts the pass stops when its
// context ends and leaves the remaining markers for the next one.
func TestSubscriberSettlementProcessor_HonoursCancellationMidPass(t *testing.T) {
	store := &settlementFakeStore{outstanding: []model.SubscriberSettlementObligation{
		settlementOwing("sub_a", true, false),
		settlementOwing("sub_b", true, false),
		settlementOwing("sub_c", true, false),
	}}
	settler := &settlementFakeSettler{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Zero(t, newSettlementProcessor(store, settler).Pass(ctx))
	assert.Empty(t, settler.settledSubscribers(),
		"an interrupted pass has no partial state to repair, because every obligation it did "+
			"not reach still carries its marker")
}

// TestSubscriberSettlementProcessor_ConfiguratorsRejectNonPositiveValues asserts a
// misconfigured cadence cannot switch settlement off or turn it into a busy loop.
func TestSubscriberSettlementProcessor_ConfiguratorsRejectNonPositiveValues(t *testing.T) {
	processor := newSettlementProcessor(&settlementFakeStore{}, &settlementFakeSettler{})

	processor.WithInterval(0).WithInterval(-time.Second)
	assert.Equal(t, defaultSubscriberSettlementInterval, processor.interval)

	processor.WithBatchSize(0).WithBatchSize(-5)
	assert.Equal(t, defaultSubscriberSettlementBatchSize, processor.batchSize)

	processor.WithRetryInterval(0).WithRetryInterval(-time.Minute)
	assert.Equal(t, defaultSubscriberSettlementRetryInterval, processor.retryInterval,
		"zero is NOT read as 'retry immediately'; that is the one setting that would turn a "+
			"broker outage into a busy loop")
}

// TestNewSubscriberSettlementProcessor_IsSafeOnAnUnwiredInstance keeps the constructor
// defensive, so the server role can call it unconditionally.
func TestNewSubscriberSettlementProcessor_IsSafeOnAnUnwiredInstance(t *testing.T) {
	processor := NewSubscriberSettlementProcessor(nil)

	require.NotNil(t, processor, "a nil instance must yield a processor that declines to start, "+
		"not a nil pointer the caller has to guard")
	require.Error(t, processor.StartupObstacle())
	assert.Equal(t, defaultSubscriberSettlementInterval, processor.interval)
	assert.Equal(t, defaultSubscriberSettlementBatchSize, processor.batchSize)
	assert.Equal(t, defaultSubscriberSettlementRetryInterval, processor.retryInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor.Start(ctx)
	assert.False(t, processor.IsRunning())
	assert.NotPanics(t, processor.Stop)
}

// TestSubscriberSettlementConfigured_ReadsTheBrokerListTheSameWayEverythingElseDoes
// keeps the worker's view of "is Kafka configured" identical to the publisher's.
//
// A worker that disagreed would either poll a table forever on a deployment without
// Kafka, or stay silent on one with it.
func TestSubscriberSettlementConfigured_ReadsTheBrokerListTheSameWayEverythingElseDoes(t *testing.T) {
	t.Run("no brokers", func(t *testing.T) {
		assert.False(t, subscriberSettlementConfigured(&config.Configuration{}))
	})

	t.Run("brokers configured", func(t *testing.T) {
		assert.True(t, subscriberSettlementConfigured(&config.Configuration{
			Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
		}))
	})

	t.Run("the constructor reads it from the instance", func(t *testing.T) {
		processor := NewSubscriberSettlementProcessor(&Blnk{config: &config.Configuration{
			Kafka: config.KafkaConfig{Brokers: []string{"localhost:9092"}},
		}})

		assert.True(t, processor.configured,
			"the constructor must resolve it from configuration; a caller computing it separately "+
				"is how two places end up disagreeing about whether Kafka exists")
	})
}
