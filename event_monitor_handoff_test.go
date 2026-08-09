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

// event_monitor_handoff_test.go covers the evaluation half of the balance-monitor handoff:
// the processor that turns a durable intent into a transactionally captured alert.
//
// The assertions are grouped around the two properties the mechanism exists for, because a
// test that only checked "an alert was produced" would pass against the post-commit capture
// this replaces and would prove nothing:
//
//   - THE CAPTURE IS ATOMIC WITH THE COMPLETION. The alerts and the handoff's terminal
//     transition must reach the repository as ONE call, never as an insert followed by a
//     mark. Two calls are two transactions, and a crash between them is precisely the window
//     the handoff removes.
//   - A REPEATED EVALUATION IS IDEMPOTENT. A lapsed claim lease, a retry or a restart must
//     derive the SAME event ids, so the duplicate collides with the unique index instead of
//     delivering the alert twice.
package blnk

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/cache"
	"github.com/blnkfinance/blnk/model"
)

// handoffProcessorHarness returns a processor wired to a mock datasource and a real cache.
//
// The cache is required rather than incidental: getBalanceMonitorsCached reads it before it
// reaches the datasource, and an instance without one panics inside the evaluation.
func handoffProcessorHarness(t *testing.T) (*BalanceMonitorHandoffProcessor, *mocks.MockDataSource) {
	t.Helper()

	datasource := new(mocks.MockDataSource)
	instance := newOutboxBlnk(t, outboxPublishingConfiguration(), datasource)

	cacheServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: cacheServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	instance.cache = cache.NewCacheWithClient(client)

	return NewBalanceMonitorHandoffProcessor(instance), datasource
}

// monitoredHandoff returns a claimed handoff carrying the snapshot of a balance that
// satisfies crossedMonitor's condition.
//
// The snapshot is built from the same fixture the post-commit tests use, so the two describe
// one balance rather than two plausible-looking ones.
func monitoredHandoff(t *testing.T, handoffID string) model.BalanceMonitorHandoff {
	t.Helper()

	handoffs, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()})
	require.NoError(t, err)
	require.Len(t, handoffs, 1)

	handoff := *handoffs[0]
	handoff.HandoffID = handoffID
	handoff.Status = model.OutboxStatusProcessing
	handoff.Attempts = 1
	handoff.MaxAttempts = 5

	return handoff
}

func TestPrepareBalanceMonitorHandoffs_SnapshotsEachBalanceAsWritten(t *testing.T) {
	handoffs, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()})

	require.NoError(t, err)
	require.Len(t, handoffs, 1)

	assert.Equal(t, "bln_monitored", handoffs[0].BalanceID)
	assert.Equal(t, "ldg_monitored", handoffs[0].LedgerID,
		"the ledger travels on the row so the alert can be attributed without a second read")
	assert.Equal(t, model.OutboxStatusPending, handoffs[0].Status)

	// The snapshot must be the balance's own serialised form, or the evaluation judges a
	// different value from the one the transaction wrote.
	snapshot, err := handoffs[0].Balance()
	require.NoError(t, err)
	assert.Equal(t, "bln_monitored", snapshot.BalanceID)
	require.NotNil(t, snapshot.Balance)
	assert.Equal(t, 0, snapshot.Balance.Cmp(big.NewInt(700)),
		"the snapshotted value is what the condition is judged against")
}

// TestPrepareBalanceMonitorHandoffs_SkipsWhatItCannotDescribe keeps a bookkeeping row from
// failing a ledger movement.
//
// The atomic writers call this for every balance they update. A nil entry or a blank id is a
// caller's slip, and refusing the whole batch for one would refuse money movement because an
// alerting side effect could not be described.
func TestPrepareBalanceMonitorHandoffs_SkipsWhatItCannotDescribe(t *testing.T) {
	handoffs, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{
		nil,
		{BalanceID: "   "},
		monitoredBalance(),
	})

	require.NoError(t, err)
	require.Len(t, handoffs, 1, "only the usable balance yields a handoff, and nothing is rejected")
	assert.Equal(t, "bln_monitored", handoffs[0].BalanceID)
}

// TestPrepareBalanceMonitorHandoffs_MintsAFreshIDPerMovement is the guard on the id that makes
// repeated firings distinguishable.
//
// One balance legitimately produces many handoffs — one per movement — so a derived handoff id
// would collapse every movement after the first into a duplicate the unique index rejects, and
// the alerts for exactly the balances that move most would silently stop.
func TestPrepareBalanceMonitorHandoffs_MintsAFreshIDPerMovement(t *testing.T) {
	first, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()})
	require.NoError(t, err)
	second, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()})
	require.NoError(t, err)

	assert.NotEqual(t, first[0].HandoffID, second[0].HandoffID,
		"two movements of one balance must be two handoffs, or the second alert is suppressed")
}

// TestBalanceMonitorEventIdentity_IsStablePerHandoffAndMonitor is the property the whole
// idempotence argument rests on.
//
// Stable across re-evaluations of ONE handoff, so a lapsed lease or a retry collides with the
// unique index instead of duplicating the alert. Distinct across handoffs, so a monitor that
// fires on every movement keeps producing new alerts.
func TestBalanceMonitorEventIdentity_IsStablePerHandoffAndMonitor(t *testing.T) {
	first := model.BalanceMonitorEventIdentity("bmh_1", "mon_crossed")

	assert.Equal(t, first, model.BalanceMonitorEventIdentity("bmh_1", "mon_crossed"),
		"the same handoff and monitor must always name the same alert")
	assert.NotEqual(t, first, model.BalanceMonitorEventIdentity("bmh_2", "mon_crossed"),
		"a later movement of the same monitor is a different alert")
	assert.NotEqual(t, first, model.BalanceMonitorEventIdentity("bmh_1", "mon_other"),
		"two monitors on one movement are two alerts")
	assert.Empty(t, model.BalanceMonitorEventIdentity("bmh_1", ""),
		"half an identity must be reported as no identity, never derived from")
	assert.Empty(t, model.BalanceMonitorEventIdentity("", "mon_crossed"))
}

// TestBalanceMonitorHandoff_CapturesTheAlertAtomicallyWithTheCompletion is the R-2 assertion
// for balance.monitor.
//
// The alerts and the handoff's terminal transition must arrive as ONE repository call. A
// version that inserted the events and then marked the handoff would pass any assertion about
// the alert existing while leaving exactly the window the handoff was built to close: a crash
// between the two either loses the alert or re-evaluates a handoff whose alerts are already
// stored.
func TestBalanceMonitorHandoff_CapturesTheAlertAtomicallyWithTheCompletion(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)
	handoff := monitoredHandoff(t, "bmh_atomic")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{handoff}, nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)

	var captured []*model.EventOutbox
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_atomic", mock.Anything).
		Run(func(args mock.Arguments) {
			captured, _ = args.Get(2).([]*model.EventOutbox)
		}).Return(nil).Once()

	processor.processBatch(context.Background())

	require.Len(t, captured, 1, "a met condition must produce exactly one alert")
	assert.Equal(t, "balance.monitor", captured[0].EventType)
	assert.Equal(t, "ldg_monitored", captured[0].LedgerID,
		"the ledger comes from the handoff, so the alert is keyed on the aggregate R-6 partitions by")

	event, data := capturedEventPayload(t, captured[0])
	assert.Equal(t, "balance.monitor", event)
	assert.Equal(t, "mon_crossed", data["monitor_id"],
		"the payload is the monitor object the legacy transport carried, unchanged")

	// The standalone insert is the pre-handoff capture. Reaching it would mean the alert was
	// written outside the completion's transaction, which is the defect, not the fix.
	datasource.AssertNotCalled(t, "InsertEventOutbox", mock.Anything, mock.Anything)
	datasource.AssertNotCalled(t, "MarkBalanceMonitorHandoffFailed",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestBalanceMonitorHandoff_DerivesAStableEventIDAcrossEvaluations proves the duplicate a
// lapsed lease can cause is absorbed by the unique index rather than delivered.
//
// This is what lets the completion skip a fence: two evaluations of one handoff offer the same
// event id, so the second insert resolves to the stored row.
func TestBalanceMonitorHandoff_DerivesAStableEventIDAcrossEvaluations(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)
	handoff := monitoredHandoff(t, "bmh_stable")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{handoff}, nil).Twice()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)

	ids := make([]string, 0, 2)
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_stable", mock.Anything).
		Run(func(args mock.Arguments) {
			rows, _ := args.Get(2).([]*model.EventOutbox)
			require.Len(t, rows, 1)
			ids = append(ids, rows[0].EventID)
		}).Return(nil).Twice()

	processor.processBatch(context.Background())
	processor.processBatch(context.Background())

	require.Len(t, ids, 2)
	assert.Equal(t, ids[0], ids[1],
		"a re-evaluated handoff must derive the same event id, or a lapsed lease delivers the alert twice")

	expected := model.DeriveEventID(
		model.BalanceMonitorEventIdentity("bmh_stable", "mon_crossed"),
		"balance.monitor",
		model.SchemaVersionV1,
	)
	assert.Equal(t, expected, ids[0],
		"the id must come from the (handoff, monitor) identity, which is what makes it both stable and per-firing")
}

// TestBalanceMonitorHandoff_CompletesWithNoEventsWhenNoConditionIsMet keeps the common case
// from being mistaken for the failure case.
//
// "Evaluated, nothing fired" and "never evaluated" are indistinguishable from the event outbox
// alone, and the difference is the only question worth asking when an expected alert did not
// arrive. Skipping the completion would also leave the row claimable and re-evaluate it until
// its budget ran out.
func TestBalanceMonitorHandoff_CompletesWithNoEventsWhenNoConditionIsMet(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	unmet := crossedMonitor()
	unmet.Condition.Operator = "<"

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_quiet")}, nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{unmet}, nil)

	completed := false
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_quiet", mock.Anything).
		Run(func(args mock.Arguments) {
			rows, _ := args.Get(2).([]*model.EventOutbox)
			assert.Empty(t, rows, "a condition that was not met must produce no alert at all")
			completed = true
		}).Return(nil).Once()

	processor.processBatch(context.Background())

	assert.True(t, completed,
		"the handoff must still be completed, or it is re-evaluated until its budget is spent")
}

// TestBalanceMonitorHandoff_FailsAnUndecodableSnapshotPermanently keeps the retry budget for
// failures a retry can actually resolve.
//
// Stored bytes do not change, so a snapshot that does not decode will not decode on the sixth
// attempt either. Spending the budget on it costs five more claims and five more poll intervals
// to reach the conclusion the first attempt already had.
func TestBalanceMonitorHandoff_FailsAnUndecodableSnapshotPermanently(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	broken := monitoredHandoff(t, "bmh_broken")
	broken.BalanceSnapshot = []byte("{not json")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{broken}, nil).Once()

	permanent := false
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_broken", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { permanent = args.Bool(3) }).Return(nil).Once()

	processor.processBatch(context.Background())

	assert.True(t, permanent, "an undecodable snapshot must be failed immediately, not retried five times")
	datasource.AssertNotCalled(t, "CompleteBalanceMonitorHandoffWithEvents",
		mock.Anything, mock.Anything, mock.Anything)
}

// TestBalanceMonitorHandoff_RetriesATransientEvaluationFailure is the other half of the
// classification: a failure a retry CAN resolve must keep its budget.
func TestBalanceMonitorHandoff_RetriesATransientEvaluationFailure(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_transient")}, nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor(nil), errors.New("connection reset"))

	permanent := true
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_transient", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { permanent = args.Bool(3) }).Return(nil).Once()

	processor.processBatch(context.Background())

	assert.False(t, permanent,
		"a transient database fault must keep the budget, or one blip discards the alert")
}

// TestBalanceMonitorHandoff_RecordsTheFailureWhenTheCaptureCannotCommit asserts the handoff
// stays claimable when the atomic write itself fails.
//
// Nothing was written, so the alert is not lost — it is owed. Marking the row is what makes the
// next poll retry it rather than leaving it leased until expiry.
func TestBalanceMonitorHandoff_RecordsTheFailureWhenTheCaptureCannotCommit(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_uncommitted")}, nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_uncommitted", mock.Anything).
		Return(errors.New("could not commit")).Once()
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_uncommitted", mock.Anything, false).
		Return(nil).Once()

	processor.processBatch(context.Background())

	datasource.AssertExpectations(t)
}

// TestBalanceMonitorHandoff_EvaluatesEveryHandoffInABatch keeps one bad row from stranding the
// rest.
//
// Each handoff is a different balance whose alerts have no reason to wait on an unrelated
// failure, and each failure is recorded against its own row.
func TestBalanceMonitorHandoff_EvaluatesEveryHandoffInABatch(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	broken := monitoredHandoff(t, "bmh_first_broken")
	broken.BalanceSnapshot = []byte("{not json")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{broken, monitoredHandoff(t, "bmh_second_ok")}, nil).Once()
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_first_broken", mock.Anything, true).
		Return(nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil)
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_second_ok", mock.Anything).
		Return(nil).Once()

	processor.processBatch(context.Background())

	datasource.AssertExpectations(t)
}

// TestBalanceMonitorHandoffProcessor_LifecycleIsIdempotent pins the two guards that keep the
// lifecycle honest.
//
// Two loops on one processor would double every claim and halve the effective lease, and Stop
// would close a channel one of them no longer reads. A second Stop must not close a closed
// channel.
func TestBalanceMonitorHandoffProcessor_LifecycleIsIdempotent(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)
	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{}, nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor.WithPollInterval(10 * time.Millisecond)
	processor.Start(ctx)
	assert.True(t, processor.IsRunning())

	processor.Start(ctx)
	assert.True(t, processor.IsRunning(), "a second Start must be a no-op, not a second loop")

	processor.Stop()
	assert.False(t, processor.IsRunning())

	assert.NotPanics(t, processor.Stop, "a second Stop must not close an already-closed channel")
}

// TestBalanceMonitorHandoffProcessor_RejectsUnusableConfiguration keeps a miswired configurator
// from producing a processor that cannot work.
//
// A zero poll interval panics time.NewTicker, and a batch size of zero claims nothing for ever.
// Both are ignored in favour of the default rather than accepted.
func TestBalanceMonitorHandoffProcessor_RejectsUnusableConfiguration(t *testing.T) {
	processor, _ := handoffProcessorHarness(t)

	processor.WithBatchSize(0).WithPollInterval(0).WithLockDuration(-time.Second)

	assert.Equal(t, defaultMonitorHandoffBatchSize, processor.batchSize)
	assert.Equal(t, defaultMonitorHandoffPollInterval, processor.pollInterval)
	assert.Equal(t, defaultMonitorHandoffLockDuration, processor.lockDuration)
}

// TestBalanceMonitorHandoffEnabled_TracksTheOneSharedPredicate is the guard on the decision the
// writer and the post-commit path must agree about.
//
// If they disagreed, one of two silent faults follows: every alert delivered twice, or a
// movement whose monitors nobody evaluates. Both sides read
// config.Configuration.EventPublishingConfigured, and this asserts the service-side reader
// tracks it.
func TestBalanceMonitorHandoffEnabled_TracksTheOneSharedPredicate(t *testing.T) {
	withBrokers := newOutboxBlnk(t, outboxPublishingConfiguration(), new(mocks.MockDataSource))
	assert.True(t, withBrokers.balanceMonitorHandoffEnabled(),
		"with a broker configured the handoff owns monitor capture")

	brokerless := newOutboxBlnk(t, &config.Configuration{}, new(mocks.MockDataSource))
	assert.False(t, brokerless.balanceMonitorHandoffEnabled(),
		"with no broker there is nothing to drain a handoff, so the post-commit path must own it")
	assert.False(t, brokerless.bulkBatchCoordinationEnabled(),
		"and the batch coordinator is gated on exactly the same fact")

	var absent *Blnk
	assert.False(t, absent.balanceMonitorHandoffEnabled(),
		"a nil receiver must answer false rather than panic; these are called from post-action goroutines")
	assert.False(t, absent.bulkBatchCoordinationEnabled())
}
