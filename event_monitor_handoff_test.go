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

// event_monitor_handoff_test.go covers the evaluation half of the balance-monitor
// handoff: the processor that turns a durable intent into a transactionally captured
// alert.
//
// The assertions are grouped around the two properties the mechanism exists for,
// because a test that only checked "an alert was produced" would pass against the
// post-commit capture this replaces and would prove nothing:
//
//   - THE CAPTURE IS ATOMIC WITH THE COMPLETION. The alerts and the handoff's terminal
//     transition must reach the repository as ONE call, never as an insert followed by
//     a mark.
//   - A REPEATED EVALUATION IS IDEMPOTENT. A lapsed claim lease, a retry or a restart
//     must derive the SAME event ids, so the duplicate collides with the unique index
//     instead of delivering the alert twice.
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

// monitoredHandoff returns a claimed handoff carrying BOTH snapshots: the balance that
// satisfies crossedMonitor's condition, and crossedMonitor itself.
//
// Both are built from the same fixtures the post-commit tests use, so the two describe
// one balance and one monitor rather than plausible-looking copies of them.
func monitoredHandoff(t *testing.T, handoffID string) model.BalanceMonitorHandoff {
	t.Helper()

	return monitoredHandoffWithMonitors(t, handoffID, crossedMonitor())
}

// monitoredHandoffWithMonitors is monitoredHandoff with the snapshotted definitions chosen by
// the caller, for a test that needs a condition other than "met".
func monitoredHandoffWithMonitors(
	t *testing.T, handoffID string, monitors ...model.BalanceMonitor,
) model.BalanceMonitorHandoff {
	t.Helper()

	balance := monitoredBalance()
	handoffs, err := model.PrepareBalanceMonitorHandoffs(
		[]*model.Balance{balance},
		map[string][]model.BalanceMonitor{balance.BalanceID: monitors},
	)
	require.NoError(t, err)
	require.Len(t, handoffs, 1)

	handoff := *handoffs[0]
	handoff.HandoffID = handoffID
	handoff.Status = model.OutboxStatusProcessing
	handoff.Attempts = 1
	handoff.MaxAttempts = 5

	return handoff
}

// legacyMonitoredHandoff returns a claimed handoff written BEFORE sql/1781252100.sql:
// the balance snapshot is present and the monitor snapshot is absent.
func legacyMonitoredHandoff(t *testing.T, handoffID string) model.BalanceMonitorHandoff {
	t.Helper()

	handoff := monitoredHandoff(t, handoffID)
	handoff.MonitorSnapshot = nil

	return handoff
}

func TestPrepareBalanceMonitorHandoffs_SnapshotsBothDecisionInputsAsWritten(t *testing.T) {
	balance := monitoredBalance()
	handoffs, err := model.PrepareBalanceMonitorHandoffs(
		[]*model.Balance{balance},
		map[string][]model.BalanceMonitor{balance.BalanceID: {crossedMonitor()}},
	)

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

	// AND THE OTHER HALF OF THE DECISION. A row carrying only the balance leaves the
	// monitor definitions to be re-read after the commit, from a table operators edit —
	// which is what let a deleted monitor suppress an alert for a movement that had
	// already crossed it.
	monitors, snapshotted, err := handoffs[0].Monitors()
	require.NoError(t, err)
	require.True(t, snapshotted,
		"every row this release writes must carry a monitor snapshot; without one the evaluator "+
			"falls back to a live read and the guarantee is the old one")
	require.Len(t, monitors, 1)
	assert.Equal(t, "mon_crossed", monitors[0].MonitorID)
	assert.Equal(t, ">=", monitors[0].Condition.Operator,
		"the CONDITION is snapshotted too, not just the monitor's identity: an edited threshold "+
			"must not change the verdict on a movement that already committed")
	require.NotNil(t, monitors[0].Condition.PreciseValue)
	assert.Equal(t, 0, monitors[0].Condition.PreciseValue.Cmp(big.NewInt(100)),
		"and the precise value survives the JSON round trip, or the payload bytes change")
}

// TestPrepareBalanceMonitorHandoffs_SkipsWhatItCannotDescribe keeps a bookkeeping row
// from failing a ledger movement.
//
// The atomic writers call this for every balance they update.
func TestPrepareBalanceMonitorHandoffs_SkipsWhatItCannotDescribe(t *testing.T) {
	balance := monitoredBalance()
	handoffs, err := model.PrepareBalanceMonitorHandoffs(
		[]*model.Balance{
			nil,
			{BalanceID: "   "},
			{BalanceID: "bln_unmonitored", LedgerID: "ldg_monitored"},
			balance,
		},
		map[string][]model.BalanceMonitor{balance.BalanceID: {crossedMonitor()}},
	)

	require.NoError(t, err)
	require.Len(t, handoffs, 1, "only the usable MONITORED balance yields a handoff, and nothing is rejected")
	assert.Equal(t, "bln_monitored", handoffs[0].BalanceID)
}

// TestPrepareBalanceMonitorHandoffs_WritesNothingForAnUnmonitoredBalance pins the guard
// that replaced a SQL EXISTS clause, and it is the one that keeps this affordable on
// the money path.
//
// The overwhelming majority of balances carry no monitor.
func TestPrepareBalanceMonitorHandoffs_WritesNothingForAnUnmonitoredBalance(t *testing.T) {
	balance := monitoredBalance()

	for name, monitors := range map[string]map[string][]model.BalanceMonitor{
		"a nil map":         nil,
		"an empty map":      {},
		"another balance":   {"bln_someone_else": {crossedMonitor()}},
		"an empty entry":    {"bln_monitored": {}},
		"a nil slice entry": {"bln_monitored": nil},
	} {
		t.Run(name, func(t *testing.T) {
			handoffs, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{balance}, monitors)

			require.NoError(t, err, "an unmonitored balance is not an error; it is the common case")
			assert.Empty(t, handoffs,
				"no monitor means no evaluation to hand off, so no row may be written")
		})
	}
}

// TestPrepareBalanceMonitorHandoffs_MintsAFreshIDPerMovement is the guard on the id
// that makes repeated firings distinguishable.
func TestPrepareBalanceMonitorHandoffs_MintsAFreshIDPerMovement(t *testing.T) {
	monitors := map[string][]model.BalanceMonitor{"bln_monitored": {crossedMonitor()}}

	first, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()}, monitors)
	require.NoError(t, err)
	second, err := model.PrepareBalanceMonitorHandoffs([]*model.Balance{monitoredBalance()}, monitors)
	require.NoError(t, err)

	assert.NotEqual(t, first[0].HandoffID, second[0].HandoffID,
		"two movements of one balance must be two handoffs, or the second alert is suppressed")
}

// TestBalanceMonitorEventIdentity_IsStablePerHandoffAndMonitor is the property the
// whole idempotence argument rests on.
//
// Stable across re-evaluations of ONE handoff, so a lapsed lease or a retry collides
// with the unique index instead of duplicating the alert.
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

// TestBalanceMonitorHandoff_CapturesTheAlertAtomicallyWithTheCompletion is the
// assertion for balance.monitor.
//
// The alerts and the handoff's terminal transition must arrive as ONE repository call.
func TestBalanceMonitorHandoff_CapturesTheAlertAtomicallyWithTheCompletion(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)
	handoff := monitoredHandoff(t, "bmh_atomic")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{handoff}, nil).Once()
	// NO GetBalanceMonitors STUB, deliberately: the row carries its monitor snapshot, so
	// the evaluation reads no table at all. An unstubbed call would panic inside the mock,
	// which is how this test would report a regression to the live read — and the explicit
	// AssertNotCalled below says so rather than leaving it to a panic.

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

	// AND THE MONITOR DEFINITIONS CAME FROM THE ROW, not from the table. This is the half
	// the snapshot added: a live read here would make the alert depend on the definitions
	// as they stand when the row is drained rather than as they stood when the balance's
	// transaction committed.
	datasource.AssertNotCalled(t, "GetBalanceMonitors", mock.Anything)
}

// TestBalanceMonitorHandoff_VerdictIsUnchangedByALaterEditToTheMonitors is the handoff-verdict
// property stated directly: the decision is a function of the ROW, and nothing else.
//
// So which events existed depended on when the row happened to be drained:
//
//  1. DELETED between commit and drain: no alert, for a movement that had crossed the
//     threshold.
//  2. CREATED in that window: an alert, for a movement the monitor was never registered
//     to watch.
//  3. EDITED in that window: an alert for a condition that was not the condition in
//     force.
func TestBalanceMonitorHandoff_VerdictIsUnchangedByALaterEditToTheMonitors(t *testing.T) {
	t.Run("a monitor deleted after the commit still fires", func(t *testing.T) {
		processor, datasource := handoffProcessorHarness(t)

		datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
			Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_deleted")}, nil).Once()
		// THE TABLE NOW SAYS THERE IS NO MONITOR. Under the live read this produced no alert for
		// a movement that had already crossed the threshold.
		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{}, nil).Maybe()

		var captured []*model.EventOutbox
		datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_deleted", mock.Anything).
			Run(func(args mock.Arguments) { captured, _ = args.Get(2).([]*model.EventOutbox) }).
			Return(nil).Once()

		processor.processBatch(context.Background())

		require.Len(t, captured, 1,
			"the movement crossed a threshold that WAS in force when it committed, so deleting the "+
				"monitor afterwards must not retract the alert")
		assert.Equal(t, "balance.monitor", captured[0].EventType)
	})

	t.Run("a monitor created after the commit does not fire", func(t *testing.T) {
		processor, datasource := handoffProcessorHarness(t)

		// A handoff whose snapshot carries a condition that is NOT met, standing in for the
		// monitors that existed at commit time.
		unmet := crossedMonitor()
		unmet.Condition.Operator = "<"

		datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
			Return([]model.BalanceMonitorHandoff{
				monitoredHandoffWithMonitors(t, "bmh_created", unmet),
			}, nil).Once()
		// THE TABLE NOW CARRIES AN EXTRA, MET MONITOR. Under the live read this produced an alert
		// for a movement the monitor was never registered to watch.
		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{unmet, crossedMonitor()}, nil).Maybe()

		var captured []*model.EventOutbox
		datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_created", mock.Anything).
			Run(func(args mock.Arguments) { captured, _ = args.Get(2).([]*model.EventOutbox) }).
			Return(nil).Once()

		processor.processBatch(context.Background())

		assert.Empty(t, captured,
			"a monitor registered after a movement was never intended to alert on it, and the "+
				"handoff must be completed rather than left claimable")
	})

	t.Run("an edited threshold does not change the verdict", func(t *testing.T) {
		processor, datasource := handoffProcessorHarness(t)

		datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
			Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_edited")}, nil).Once()
		// THE SAME MONITOR ID, REVERSED CONDITION. Under the live read this suppressed the alert.
		edited := crossedMonitor()
		edited.Condition.Operator = "<"
		datasource.On("GetBalanceMonitors", "bln_monitored").
			Return([]model.BalanceMonitor{edited}, nil).Maybe()

		var captured []*model.EventOutbox
		datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_edited", mock.Anything).
			Run(func(args mock.Arguments) { captured, _ = args.Get(2).([]*model.EventOutbox) }).
			Return(nil).Once()

		processor.processBatch(context.Background())

		require.Len(t, captured, 1,
			"the condition in force at commit time was met, so an edit afterwards must not decide "+
				"the alert")

		// AND THE PAYLOAD IS THE SNAPSHOTTED MONITOR, not the edited one. The event body is the
		// monitor object, so a live read would also have changed what a subscriber received.
		_, data := capturedEventPayload(t, captured[0])
		condition, ok := data["condition"].(map[string]interface{})
		require.True(t, ok, "the payload must carry the monitor's condition object")
		assert.Equal(t, ">=", condition["operator"],
			"the alert must describe the condition that actually fired, not the one an operator "+
				"substituted afterwards")
	})
}

// TestBalanceMonitorHandoff_FallsBackToALiveReadForALegacyRow keeps the upgrade path
// drainable.
//
// A row written before sql/1781252100.sql carries no monitor snapshot.
func TestBalanceMonitorHandoff_FallsBackToALiveReadForALegacyRow(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{legacyMonitoredHandoff(t, "bmh_legacy")}, nil).Once()
	// REQUIRED, not Maybe: the whole point of this test is that the legacy row DOES reach the
	// live read, so an implementation that refused or skipped it fails here.
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor{crossedMonitor()}, nil).Once()

	var captured []*model.EventOutbox
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_legacy", mock.Anything).
		Run(func(args mock.Arguments) { captured, _ = args.Get(2).([]*model.EventOutbox) }).
		Return(nil).Once()

	processor.processBatch(context.Background())

	require.Len(t, captured, 1,
		"a row that predates the snapshot column must still be evaluated, or an upgrade strands "+
			"every handoff already in the table")
	assert.Equal(t, "balance.monitor", captured[0].EventType)
	datasource.AssertExpectations(t)
}

// TestBalanceMonitorHandoff_FailsAnUndecodableMonitorSnapshotPermanently mirrors the
// balance snapshot's treatment, and for the same reason.
//
// Stored bytes do not change, so a snapshot that does not decode will not decode on the
// sixth attempt either.
func TestBalanceMonitorHandoff_FailsAnUndecodableMonitorSnapshotPermanently(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	broken := monitoredHandoff(t, "bmh_broken_monitors")
	broken.MonitorSnapshot = []byte("{not json")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{broken}, nil).Once()

	permanent := false
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_broken_monitors", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { permanent = args.Bool(3) }).Return(nil).Once()

	processor.processBatch(context.Background())

	assert.True(t, permanent,
		"undecodable bytes cannot decode later, so the remaining budget only delays the same "+
			"conclusion")
	datasource.AssertNotCalled(t, "CompleteBalanceMonitorHandoffWithEvents",
		mock.Anything, mock.Anything, mock.Anything)
	// AND IT IS NOT TREATED AS A LEGACY ROW. Falling back here would evaluate definitions the
	// mutation never saw and report success.
	datasource.AssertNotCalled(t, "GetBalanceMonitors", mock.Anything)
}

// TestBalanceMonitorHandoff_DerivesAStableEventIDAcrossEvaluations proves the duplicate
// a lapsed lease can cause is absorbed by the unique index rather than delivered.
//
// This is what lets the completion skip a fence: two evaluations of one handoff offer
// the same event id, so the second insert resolves to the stored row.
func TestBalanceMonitorHandoff_DerivesAStableEventIDAcrossEvaluations(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)
	handoff := monitoredHandoff(t, "bmh_stable")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{handoff}, nil).Twice()
	// No live read on either evaluation: both judge the same snapshotted definitions, which is
	// what makes the two verdicts comparable at all.

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

// TestBalanceMonitorHandoff_CompletesWithNoEventsWhenNoConditionIsMet keeps the common
// case from being mistaken for the failure case.
func TestBalanceMonitorHandoff_CompletesWithNoEventsWhenNoConditionIsMet(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	unmet := crossedMonitor()
	unmet.Condition.Operator = "<"

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{
			monitoredHandoffWithMonitors(t, "bmh_quiet", unmet),
		}, nil).Once()

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

// TestBalanceMonitorHandoff_FailsAnUndecodableSnapshotPermanently keeps the retry
// budget for failures a retry can actually resolve.
//
// Stored bytes do not change, so a snapshot that does not decode will not decode on the
// sixth attempt either.
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

	// THE TRANSIENT FAULT IS THE LEGACY FALLBACK READ, and it is the only read left on
	// this path: a row carrying a monitor snapshot needs no database at all to be
	// evaluated. A row written before sql/1781252100.sql does, so it is the one that can
	// still fail this way — and the assertion below is that such a failure keeps the
	// budget.
	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{legacyMonitoredHandoff(t, "bmh_transient")}, nil).Once()
	datasource.On("GetBalanceMonitors", "bln_monitored").
		Return([]model.BalanceMonitor(nil), errors.New("connection reset"))

	permanent := true
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_transient", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { permanent = args.Bool(3) }).Return(nil).Once()

	processor.processBatch(context.Background())

	assert.False(t, permanent,
		"a transient database fault must keep the budget, or one blip discards the alert")
}

// TestBalanceMonitorHandoff_RecordsTheFailureWhenTheCaptureCannotCommit asserts the
// handoff stays claimable when the atomic write itself fails.
//
// Nothing was written, so the alert is not lost — it is owed.
func TestBalanceMonitorHandoff_RecordsTheFailureWhenTheCaptureCannotCommit(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{monitoredHandoff(t, "bmh_uncommitted")}, nil).Once()
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_uncommitted", mock.Anything).
		Return(errors.New("could not commit")).Once()
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_uncommitted", mock.Anything, false).
		Return(nil).Once()

	processor.processBatch(context.Background())

	datasource.AssertExpectations(t)
}

// TestBalanceMonitorHandoff_EvaluatesEveryHandoffInABatch keeps one bad row from
// stranding the rest.
//
// Each handoff is a different balance whose alerts have no reason to wait on an
// unrelated failure, and each failure is recorded against its own row.
func TestBalanceMonitorHandoff_EvaluatesEveryHandoffInABatch(t *testing.T) {
	processor, datasource := handoffProcessorHarness(t)

	broken := monitoredHandoff(t, "bmh_first_broken")
	broken.BalanceSnapshot = []byte("{not json")

	datasource.On("ClaimPendingBalanceMonitorHandoffs", mock.Anything, mock.Anything, mock.Anything).
		Return([]model.BalanceMonitorHandoff{broken, monitoredHandoff(t, "bmh_second_ok")}, nil).Once()
	datasource.On("MarkBalanceMonitorHandoffFailed", mock.Anything, "bmh_first_broken", mock.Anything, true).
		Return(nil).Once()
	datasource.On("CompleteBalanceMonitorHandoffWithEvents", mock.Anything, "bmh_second_ok", mock.Anything).
		Return(nil).Once()

	processor.processBatch(context.Background())

	datasource.AssertExpectations(t)
}

// TestBalanceMonitorHandoffProcessor_LifecycleIsIdempotent pins the two guards that
// keep the lifecycle honest.
//
// Two loops on one processor would double every claim and halve the effective lease,
// and Stop would close a channel one of them no longer reads.
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

// TestBalanceMonitorHandoffProcessor_RejectsUnusableConfiguration keeps a miswired
// configurator from producing a processor that cannot work.
//
// A zero poll interval panics time.NewTicker, and a batch size of zero claims nothing
// for ever.
func TestBalanceMonitorHandoffProcessor_RejectsUnusableConfiguration(t *testing.T) {
	processor, _ := handoffProcessorHarness(t)

	processor.WithBatchSize(0).WithPollInterval(0).WithLockDuration(-time.Second)

	assert.Equal(t, defaultMonitorHandoffBatchSize, processor.batchSize)
	assert.Equal(t, defaultMonitorHandoffPollInterval, processor.pollInterval)
	assert.Equal(t, defaultMonitorHandoffLockDuration, processor.lockDuration)
}

// TestBalanceMonitorHandoffEnabled_TracksTheOneSharedPredicate is the guard on the
// decision the writer and the post-commit path must agree about.
//
// If they disagreed, one of two silent faults follows: every alert delivered twice, or
// a movement whose monitors nobody evaluates.
func TestBalanceMonitorHandoffEnabled_TracksTheOneSharedPredicate(t *testing.T) {
	withBrokers := newOutboxBlnk(t, outboxPublishingConfiguration(), new(mocks.MockDataSource))
	assert.True(t, withBrokers.balanceMonitorHandoffEnabled(),
		"with a broker configured the writer owns monitor capture")

	brokerless := newOutboxBlnk(t, &config.Configuration{}, new(mocks.MockDataSource))
	assert.False(t, brokerless.balanceMonitorHandoffEnabled(),
		"with no broker there is no outbox to capture into, so the post-commit path must own it")
	assert.False(t, brokerless.bulkBatchCoordinationEnabled(),
		"and the batch coordinator is gated on exactly the same fact")

	var absent *Blnk
	assert.False(t, absent.balanceMonitorHandoffEnabled(),
		"a nil receiver must answer false rather than panic; these are called from post-action goroutines")
	assert.False(t, absent.bulkBatchCoordinationEnabled())
}
