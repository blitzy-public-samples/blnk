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

// event_producer_atomicity_test.go exercises the two tables that bring the last two event
// families under requirement R-2, AGAINST A REAL POSTGRESQL.
//
// A mock cannot answer the questions that matter here. The monitor-handoff insert is one
// statement whose VALUES list is filtered by an EXISTS against blnk.balance_monitors — whether
// it writes a row depends on the planner resolving that join and on the jsonb casts being in
// the right place, neither of which a mock evaluates. The batch finalise's guarantee is that a
// guarded UPDATE and an INSERT commit together, which only a real transaction can demonstrate.
// The schema's own CHECK constraints are part of the contract too, and a mock has none.
//
// Every test skips when no database is reachable, matching the convention the other real-database
// tests in this package use.
package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// monitoredTestBalance creates a balance, and optionally a monitor on it, returning the
// balance id.
//
// The monitor is what the handoff insert's EXISTS guard looks for, so a test that wants a
// handoff written must ask for one and a test that wants none must not.
func monitoredTestBalance(t *testing.T, ds Datasource, withMonitor bool) (string, string) {
	t.Helper()

	ledger, err := ds.CreateLedger(model.Ledger{Name: "handoff-" + gofakeit.UUID()})
	require.NoError(t, err)

	balance := &model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"}
	balance.InitializeBalanceFields()
	created, err := ds.CreateBalance(*balance)
	require.NoError(t, err)

	if withMonitor {
		_, err = ds.CreateMonitor(model.BalanceMonitor{
			BalanceID: created.BalanceID,
			Condition: model.AlertCondition{Field: "balance", Operator: ">=", Value: 1, PreciseValue: big.NewInt(100), Precision: 100},
		})
		require.NoError(t, err)
	}

	retireHandoffFixtures(t, ds, created.BalanceID, ledger.LedgerID)

	return created.BalanceID, ledger.LedgerID
}

// retireHandoffFixtures deletes every row this test caused, in both tables, once it ends.
//
// # Why a leaked row here is not inert
//
// The database is SHARED with sibling clones and with the root package's own tiers, which run
// in another process. A handoff or event row left behind is therefore not merely clutter:
//
//   - A leaked event_outbox row is left PENDING, so the first relay to look — a live tier in
//     another process — claims it, publishes it for real and stamps it with the broker
//     coordinate it landed on. That coordinate then sits under the unique index on
//     (kafka_topic, kafka_partition, kafka_offset) for ever, and it is read by the
//     reconciliation audit, whose per-partition extremes are asserted for EXACT equality.
//     One leaked row from this file is enough to fail
//     TestAuditEventRecordCoordinates_ReportsPerPartitionClaims_RealDB in a way that reads as
//     a defect in the audit rather than as pollution from here.
//   - A leaked handoff row is claim-visible, and the claim is global and oldest-first, so it
//     is served AHEAD of the fixtures of any later test that reasons about batch composition.
//
// # Why it matches on four columns
//
// The two families this file exercises are keyed differently and neither is keyed on the
// balance: a monitor alert carries the LEDGER as its aggregate and partition key, and the
// handoff carries the balance. Matching all four covers both without assuming which of them a
// given test produced.
func retireHandoffFixtures(t *testing.T, ds Datasource, balanceID, ledgerID string) {
	t.Helper()

	t.Cleanup(func() {
		if _, err := ds.Conn.Exec(
			`DELETE FROM blnk.balance_monitor_handoff WHERE balance_id = $1`, balanceID); err != nil {
			t.Logf("failed to remove balance monitor handoff fixtures for %q: %v", balanceID, err)
		}

		if _, err := ds.Conn.Exec(`
			DELETE FROM blnk.event_outbox
			WHERE ledger_id = $1 OR aggregate_id = $1 OR partition_key = $1 OR partition_key = $2
		`, ledgerID, balanceID); err != nil {
			t.Logf("failed to remove event outbox fixtures for ledger %q: %v", ledgerID, err)
		}
	})
}

// coordinatedTestBatchID mints a batch id and registers the cleanup for everything a batch
// leaves behind: the coordinator row and the outcome event captured with it.
//
// The event's aggregate id and partition key are both the batch id — the batch is the
// aggregate a bulk outcome belongs to — so one predicate covers the event, and the coordinator
// row is keyed on the same value.
func coordinatedTestBatchID(t *testing.T, ds Datasource) string {
	t.Helper()

	batchID := "bulk_" + gofakeit.UUID()

	t.Cleanup(func() {
		if _, err := ds.Conn.Exec(
			`DELETE FROM blnk.event_outbox WHERE aggregate_id = $1 OR partition_key = $1`, batchID); err != nil {
			t.Logf("failed to remove the outcome event for batch %q: %v", batchID, err)
		}

		if _, err := ds.Conn.Exec(
			`DELETE FROM blnk.bulk_transaction_batches WHERE batch_id = $1`, batchID); err != nil {
			t.Logf("failed to remove the coordinator row for batch %q: %v", batchID, err)
		}
	})

	return batchID
}

// TestBalancesAwaitingMonitorEvaluation_SuppressesTheHandoffForABalanceEvaluatedWithTheMutation
// is the guard on the duplicate two independently-built R-2 mechanisms create together.
//
// # The defect
//
// `balance.monitor` is brought inside the mutation's transaction by two routes. The caller-side
// pass evaluates a moved balance's monitors before the write and hands the alerts to the writer;
// the writer-side handoff records the intent to evaluate, which the handoff processor drains.
// With both live and neither aware of the other, a crossing on the single-transaction path is
// captured TWICE — once by the pass, once by the processor — under two different event ids. A
// `balance.monitor` id is a fresh UUID by design, so no subscriber-side idempotency can collapse
// the pair, and the duplicate is indistinguishable from two genuine crossings.
//
// # Why these two functions need no database
//
// They are pure: one reads balance ids out of event payloads, the other subtracts a set. The
// statement they gate is exercised against real PostgreSQL above; the DECISION is exercised
// here, deterministically, because a planner cannot be asked whether it would have been right
// to write the row at all. This is the one test in this file that does not skip.
func TestBalancesAwaitingMonitorEvaluation_SuppressesTheHandoffForABalanceEvaluatedWithTheMutation(t *testing.T) {
	monitorRow := func(balanceID string) *model.EventOutbox {
		return &model.EventOutbox{
			EventType: model.EventTypeBalanceMonitor,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"event":"balance.monitor","data":{"monitor_id":"mon_1","balance_id":%q}}`, balanceID)),
		}
	}
	source := &model.Balance{BalanceID: "bln_source"}
	destination := &model.Balance{BalanceID: "bln_destination"}
	both := []*model.Balance{source, destination}

	t.Run("a balance whose alert travelled with the mutation is not handed off", func(t *testing.T) {
		rows := []*model.EventOutbox{
			{EventType: "transaction.applied", Payload: json.RawMessage(`{"event":"transaction.applied","data":{}}`)},
			monitorRow("bln_source"),
		}

		awaiting := balancesAwaitingMonitorEvaluation(both, balancesAlreadyEvaluatedInTx(rows))

		require.Len(t, awaiting, 1,
			"the evaluated balance must be dropped, or the processor publishes its crossing again")
		assert.Equal(t, "bln_destination", awaiting[0].BalanceID,
			"and the balance nobody evaluated must survive, or its crossing is evaluated by neither route")
	})

	t.Run("a balance with no captured alert is still handed off", func(t *testing.T) {
		// The read-failure case, and the reason coverage is taken from the rows rather than
		// assumed from the path: a monitor read that failed before the write captures nothing,
		// so the handoff is the only thing that will ever evaluate that balance.
		awaiting := balancesAwaitingMonitorEvaluation(both, balancesAlreadyEvaluatedInTx(
			[]*model.EventOutbox{monitorRow("bln_source")}))

		assert.Len(t, awaiting, 1)

		assert.Equal(t, both, balancesAwaitingMonitorEvaluation(both, nil),
			"with nothing evaluated the input set is returned unchanged, which is the coalesced "+
				"path's case and the common one")
	})

	t.Run("only balance.monitor rows count as coverage", func(t *testing.T) {
		// The transaction's own event carries an aggregate id too. Reading coverage from
		// anything but the monitor alert would suppress a handoff on the strength of an
		// unrelated event and lose the crossing outright.
		evaluated := balancesAlreadyEvaluatedInTx([]*model.EventOutbox{
			{EventType: "transaction.applied", Payload: json.RawMessage(`{"event":"transaction.applied","data":{"balance_id":"bln_source"}}`)},
			{EventType: "balance.created", Payload: json.RawMessage(`{"event":"balance.created","data":{"balance_id":"bln_source"}}`)},
		})

		assert.Empty(t, evaluated)
	})

	t.Run("an unreadable or balance-less payload yields no coverage", func(t *testing.T) {
		// The safe direction: the handoff is written, the processor evaluates, and the worst
		// case is the duplicate rather than an alert nobody evaluates.
		evaluated := balancesAlreadyEvaluatedInTx([]*model.EventOutbox{
			nil,
			{EventType: model.EventTypeBalanceMonitor, Payload: json.RawMessage(`{`)},
			{EventType: model.EventTypeBalanceMonitor, Payload: json.RawMessage(`{"event":"balance.monitor","data":{"monitor_id":"mon_1","balance_id":"   "}}`)},
		})

		assert.Empty(t, evaluated)
	})
}

// insertHandoffsInOwnTx runs the writer's in-transaction insert on its own, so a test can
// exercise the statement without driving a whole ledger mutation through it.
//
// It commits, because the point of every assertion below is what is DURABLE afterwards.
func insertHandoffsInOwnTx(t *testing.T, ds Datasource, balances []*model.Balance) {
	t.Helper()

	tx, err := ds.Conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	require.NoError(t, insertBalanceMonitorHandoffsInTx(context.Background(), tx, balances))
	require.NoError(t, tx.Commit())
}

// countHandoffsForBalance returns how many handoff rows exist for a balance.
func countHandoffsForBalance(t *testing.T, ds Datasource, balanceID string) int {
	t.Helper()

	var count int
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT COUNT(*) FROM blnk.balance_monitor_handoff WHERE balance_id = $1`, balanceID).Scan(&count))

	return count
}

// TestInsertBalanceMonitorHandoffsInTx_WritesOnlyForAMonitoredBalance is the assertion that
// makes this affordable on the money path.
//
// The overwhelming majority of balances carry no monitor. A row per balance per transaction
// would be pure write amplification at the system's throughput target, every row destined to be
// evaluated to "nothing fired". The EXISTS guard is what avoids that, and it has to be part of
// the same statement — a Go-side check would need its own query and its own round trip inside
// the ledger transaction.
func TestInsertBalanceMonitorHandoffsInTx_WritesOnlyForAMonitoredBalance(t *testing.T) {
	ds := openRealTestDB(t)

	monitored, monitoredLedger := monitoredTestBalance(t, ds, true)
	unmonitored, _ := monitoredTestBalance(t, ds, false)

	insertHandoffsInOwnTx(t, ds, []*model.Balance{
		{BalanceID: monitored, LedgerID: monitoredLedger},
		{BalanceID: unmonitored},
	})

	assert.Equal(t, 1, countHandoffsForBalance(t, ds, monitored),
		"a monitored balance must get its evaluation intent recorded")
	assert.Equal(t, 0, countHandoffsForBalance(t, ds, unmonitored),
		"an unmonitored balance must write nothing at all, or the money path pays for every transaction")
}

// TestInsertBalanceMonitorHandoffsInTx_StoresTheSnapshotAndTheLedger asserts the row carries
// everything the evaluation needs, so the evaluator can run in another process and judge the
// state the transaction actually wrote.
func TestInsertBalanceMonitorHandoffsInTx_StoresTheSnapshotAndTheLedger(t *testing.T) {
	ds := openRealTestDB(t)

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	written := &model.Balance{BalanceID: balanceID, LedgerID: ledgerID}
	written.InitializeBalanceFields()
	written.Balance = big.NewInt(4242)

	insertHandoffsInOwnTx(t, ds, []*model.Balance{written})

	var storedLedger sql.NullString
	var snapshot []byte
	var status string
	var attempts, maxAttempts, captured int
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT ledger_id, balance_snapshot, status, attempts, max_attempts, events_captured
		FROM blnk.balance_monitor_handoff WHERE balance_id = $1
	`, balanceID).Scan(&storedLedger, &snapshot, &status, &attempts, &maxAttempts, &captured))

	assert.Equal(t, ledgerID, storedLedger.String,
		"the ledger travels on the row so the alert is attributable without a second read")
	assert.Equal(t, model.OutboxStatusPending, status)
	assert.Zero(t, attempts)
	assert.Positive(t, maxAttempts, "the column default must supply a budget")
	assert.Zero(t, captured)

	decoded := &model.Balance{}
	require.NoError(t, json.Unmarshal(snapshot, decoded))
	require.NotNil(t, decoded.Balance)
	assert.Equal(t, 0, decoded.Balance.Cmp(big.NewInt(4242)),
		"the snapshot must be the value the transaction wrote, not the value read back later")
}

// TestInsertBalanceMonitorHandoffsInTx_RollsBackWithItsTransaction is the atomicity assertion.
//
// The whole mechanism rests on the intent sharing the balance's transaction. If the row survived
// a rollback, an alert would be evaluated for a movement that never happened.
func TestInsertBalanceMonitorHandoffsInTx_RollsBackWithItsTransaction(t *testing.T) {
	ds := openRealTestDB(t)
	balanceID, ledgerID := monitoredTestBalance(t, ds, true)

	tx, err := ds.Conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault})
	require.NoError(t, err)
	require.NoError(t, insertBalanceMonitorHandoffsInTx(context.Background(), tx,
		[]*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}}))
	require.NoError(t, tx.Rollback())

	assert.Equal(t, 0, countHandoffsForBalance(t, ds, balanceID),
		"a rolled-back movement must leave no evaluation intent behind")
}

// TestInsertBalanceMonitorHandoffsInTx_RefusesToWriteOutsideATransaction protects the guarantee
// from a caller that would silently break it.
//
// A handoff written on its own connection would look exactly like a working one and would
// reintroduce the crash window the mechanism exists to close.
func TestInsertBalanceMonitorHandoffsInTx_RefusesToWriteOutsideATransaction(t *testing.T) {
	err := insertBalanceMonitorHandoffsInTx(context.Background(), nil,
		[]*model.Balance{{BalanceID: "bln_anything"}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to record balance monitor handoffs")
}

// TestBalanceMonitorHandoffClaim_LeasesFIFOAndSkipsHeldRows covers the three properties the
// claim query is built for: oldest first, an incremented attempt, and a lease that another
// claimer respects.
func TestBalanceMonitorHandoffClaim_LeasesFIFOAndSkipsHeldRows(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	// DRAINED FIRST, and this is not tidying. The claim is global and strictly oldest-first, so
	// pending rows left by earlier tests in this package would be claimed ahead of this test's
	// and a batch-size assertion would see none of its own rows. Leasing them out of the way
	// for an hour makes the batch below deterministic without deleting anything.
	drainPendingHandoffs(t, ds)

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	for i := 0; i < 3; i++ {
		insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})
	}

	first, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 2, time.Minute)
	require.NoError(t, err)
	claimed := handoffsForBalance(first, balanceID)
	require.Len(t, claimed, 2, "the batch size must bound the claim")

	assert.False(t, claimed[1].CreatedAt.Before(claimed[0].CreatedAt),
		"the batch must be returned oldest first, or a balance's evaluations run out of order")
	assert.Equal(t, 1, claimed[0].Attempts, "the claim itself counts as the attempt")
	assert.Equal(t, model.OutboxStatusProcessing, claimed[0].Status)
	require.NotNil(t, claimed[0].LockedUntil)
	assert.True(t, claimed[0].LockedUntil.After(time.Now()), "the lease must be in the future")

	second, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 10, time.Minute)
	require.NoError(t, err)
	remaining := handoffsForBalance(second, balanceID)
	require.Len(t, remaining, 1, "a leased row must not be claimed again while the lease holds")
	assert.NotEqual(t, claimed[0].HandoffID, remaining[0].HandoffID)
}

// TestClaimPendingBalanceMonitorHandoffQuery_KeepsTheShapeItsCorrectnessDependsOn asserts the
// two clauses whose absence is invisible until the conditions that need them occur.
//
// Both were defects in the first version of this statement, and neither showed up as a wrong
// answer in a way a normal test would catch:
//
//   - Without MATERIALIZED, the candidate selection is a semi-join subplan the planner may
//     re-evaluate, and because this UPDATE writes `attempts` — the very column that subplan
//     filters on — each re-evaluation applies the LIMIT again. Measured on PostgreSQL 16, the
//     inline form claimed three rows for a batch size of two. It is PLAN-DEPENDENT, so it
//     appears on a small table and hides on a large one.
//   - Without FOR UPDATE SKIP LOCKED, two processors block on each other instead of taking
//     disjoint batches, which converts horizontal scaling into serialisation.
//
// A behavioural test cannot reliably reach either state, so the shape is asserted directly.
func TestClaimPendingBalanceMonitorHandoffQuery_KeepsTheShapeItsCorrectnessDependsOn(t *testing.T) {
	assert.Contains(t, claimPendingBalanceMonitorHandoffQuery, "AS MATERIALIZED",
		"the candidate selection must be materialised, or the batch size is a hint rather than a bound")
	assert.Contains(t, claimPendingBalanceMonitorHandoffQuery, "FOR UPDATE SKIP LOCKED",
		"two processors must take disjoint batches rather than block on one another")
	assert.Contains(t, claimPendingBalanceMonitorHandoffQuery, "attempts = attempts + 1",
		"the claim itself must spend an attempt, or a row that kills its processor is retried for ever")
	assert.Contains(t, claimPendingBalanceMonitorHandoffQuery, "ORDER BY created_at ASC, id ASC",
		"UPDATE ... RETURNING does not preserve the inner order, so the batch is re-sorted to stay FIFO")
}

// drainPendingHandoffs leases every currently claimable handoff far into the future.
//
// The claim query is global and oldest-first, so a test that wants to reason about ITS rows has
// to move everything older out of the way. A long lease does that without deleting rows another
// assertion may depend on.
func drainPendingHandoffs(t *testing.T, ds Datasource) {
	t.Helper()

	for {
		claimed, err := ds.ClaimPendingBalanceMonitorHandoffs(context.Background(), 500, time.Hour)
		require.NoError(t, err)
		if len(claimed) == 0 {
			return
		}
	}
}

// handoffsForBalance filters a claim batch down to one balance's rows.
//
// The database is shared with other tests and with sibling clones, so a claim legitimately
// returns rows this test knows nothing about. Filtering is what makes the assertions about
// THIS test's rows rather than about whatever else is pending.
func handoffsForBalance(handoffs []model.BalanceMonitorHandoff, balanceID string) []model.BalanceMonitorHandoff {
	filtered := make([]model.BalanceMonitorHandoff, 0, len(handoffs))
	for _, handoff := range handoffs {
		if handoff.BalanceID == balanceID {
			filtered = append(filtered, handoff)
		}
	}

	return filtered
}

// TestCompleteBalanceMonitorHandoffWithEvents_WritesTheAlertAndTheCompletionTogether is the
// R-2 assertion at the repository boundary.
func TestCompleteBalanceMonitorHandoffWithEvents_WritesTheAlertAndTheCompletionTogether(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})

	claimed, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 50, time.Minute)
	require.NoError(t, err)
	handoff := handoffsForBalance(claimed, balanceID)
	require.Len(t, handoff, 1)

	event := monitorAlertRow(handoff[0].HandoffID, ledgerID)
	require.NoError(t, ds.CompleteBalanceMonitorHandoffWithEvents(ctx, handoff[0].HandoffID,
		[]*model.EventOutbox{event}))

	status, captured, processed := handoffState(t, ds, handoff[0].HandoffID)
	assert.Equal(t, model.OutboxStatusCompleted, status)
	assert.Equal(t, 1, captured, "the count distinguishes an alert that fired from one that did not")
	assert.True(t, processed.Valid, "a terminal row must record when it became terminal")

	var storedType string
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT event_type FROM blnk.event_outbox WHERE event_id = $1`, event.EventID).Scan(&storedType))
	assert.Equal(t, "balance.monitor", storedType,
		"the alert must be durable in the same transaction that completed the handoff")
}

// TestCompleteBalanceMonitorHandoffWithEvents_IsIdempotentForARepeatedEvaluation is what lets
// the completion skip a fence.
//
// A lapsed lease can let two processors evaluate one handoff. Because the alert's id is derived
// from (handoff, monitor), the second completion offers the same row: the unique index resolves
// it to the stored one and the second call succeeds without writing a second alert.
func TestCompleteBalanceMonitorHandoffWithEvents_IsIdempotentForARepeatedEvaluation(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})

	claimed, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 50, time.Minute)
	require.NoError(t, err)
	handoff := handoffsForBalance(claimed, balanceID)
	require.Len(t, handoff, 1)

	event := monitorAlertRow(handoff[0].HandoffID, ledgerID)
	require.NoError(t, ds.CompleteBalanceMonitorHandoffWithEvents(ctx, handoff[0].HandoffID,
		[]*model.EventOutbox{event}))

	// The SAME derived row again, exactly as a second evaluation would produce it.
	repeat := monitorAlertRow(handoff[0].HandoffID, ledgerID)
	require.NoError(t, ds.CompleteBalanceMonitorHandoffWithEvents(ctx, handoff[0].HandoffID,
		[]*model.EventOutbox{repeat}),
		"a repeated evaluation must be absorbed, not reported as a failure that schedules a third")

	var alerts int
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT COUNT(*) FROM blnk.event_outbox WHERE event_id = $1`, event.EventID).Scan(&alerts))
	assert.Equal(t, 1, alerts, "one firing must be one alert however many times it is evaluated")
}

// TestCompleteBalanceMonitorHandoffWithEvents_CompletesWithNoAlerts records the common case.
//
// "Evaluated, nothing fired" is a result. Without it, that state is indistinguishable from
// "never evaluated", which is the only question worth asking when an expected alert is missing.
func TestCompleteBalanceMonitorHandoffWithEvents_CompletesWithNoAlerts(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})

	claimed, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 50, time.Minute)
	require.NoError(t, err)
	handoff := handoffsForBalance(claimed, balanceID)
	require.Len(t, handoff, 1)

	require.NoError(t, ds.CompleteBalanceMonitorHandoffWithEvents(ctx, handoff[0].HandoffID, nil))

	status, captured, _ := handoffState(t, ds, handoff[0].HandoffID)
	assert.Equal(t, model.OutboxStatusCompleted, status)
	assert.Zero(t, captured)
}

// TestMarkBalanceMonitorHandoffFailed_KeepsTheBudgetUnlessThePermanentFlagIsSet covers both
// arms of the classification the evaluator supplies.
func TestMarkBalanceMonitorHandoffFailed_KeepsTheBudgetUnlessThePermanentFlagIsSet(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})

	claimed, err := ds.ClaimPendingBalanceMonitorHandoffs(ctx, 50, time.Minute)
	require.NoError(t, err)
	rows := handoffsForBalance(claimed, balanceID)
	require.Len(t, rows, 2)

	require.NoError(t, ds.MarkBalanceMonitorHandoffFailed(ctx, rows[0].HandoffID, "connection reset", false))
	status, _, processed := handoffState(t, ds, rows[0].HandoffID)
	assert.Equal(t, model.OutboxStatusPending, status,
		"a retryable failure must return the row to pending so the next poll re-claims it")
	assert.False(t, processed.Valid, "a row that is coming back is not processed")

	require.NoError(t, ds.MarkBalanceMonitorHandoffFailed(ctx, rows[1].HandoffID, "snapshot will not decode", true))
	status, _, processed = handoffState(t, ds, rows[1].HandoffID)
	assert.Equal(t, model.OutboxStatusFailed, status,
		"a permanent failure must not spend five more polls reaching the same conclusion")
	assert.True(t, processed.Valid)
}

// TestCountBalanceMonitorHandoffByStatus_ReportsEveryPresentStatus underpins the operational
// question the event outbox cannot answer: were a movement's monitors evaluated at all.
func TestCountBalanceMonitorHandoffByStatus_ReportsEveryPresentStatus(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	balanceID, ledgerID := monitoredTestBalance(t, ds, true)
	insertHandoffsInOwnTx(t, ds, []*model.Balance{{BalanceID: balanceID, LedgerID: ledgerID}})

	before, err := ds.CountBalanceMonitorHandoffByStatus(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, before[model.OutboxStatusPending], int64(1),
		"the pending row just written must be counted")
}

// handoffState reads the three fields the assertions above care about.
func handoffState(t *testing.T, ds Datasource, handoffID string) (string, int, sql.NullTime) {
	t.Helper()

	var status string
	var captured int
	var processed sql.NullTime
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT status, events_captured, processed_at
		FROM blnk.balance_monitor_handoff WHERE handoff_id = $1
	`, handoffID).Scan(&status, &captured, &processed))

	return status, captured, processed
}

// monitorAlertRow builds the alert row an evaluation would produce, with the DERIVED id.
//
// The id is derived here exactly as the evaluator derives it, which is what makes the
// idempotence test above a test of the real mechanism rather than of a restated copy of it.
func monitorAlertRow(handoffID, ledgerID string) *model.EventOutbox {
	identity := model.BalanceMonitorEventIdentity(handoffID, "mon_"+handoffID)
	payload := json.RawMessage(fmt.Sprintf(`{"event":"balance.monitor","data":{"monitor_id":%q}}`, "mon_"+handoffID))

	return &model.EventOutbox{
		EventID:       model.DeriveEventID(identity, "balance.monitor", model.SchemaVersionV1),
		EventType:     "balance.monitor",
		AggregateID:   ledgerID,
		PartitionKey:  ledgerID,
		LedgerID:      ledgerID,
		Topic:         "blnk.balances",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payload,
		EventRaw:      payload,
		OccurredAt:    time.Now().UTC(),
		Status:        model.EventOutboxStatusPending,
		MaxAttempts:   5,
	}
}

// TestFinalizeBulkTransactionBatchWithEvent_CommitsTheOutcomeWithItsEvent is the R-2 assertion
// for bulk_transaction.<status>.
//
// The batch has no batch-spanning transaction, so the summary is made atomic with the
// coordinator's terminal transition instead. Both facts must be durable after one call, and the
// coordinator must record which event carried the outcome.
func TestFinalizeBulkTransactionBatchWithEvent_CommitsTheOutcomeWithItsEvent(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, &model.BulkTransactionBatch{
		BatchID: batchID, TransactionCount: 4, Atomic: true,
	}))

	event := bulkOutcomeRow(batchID, "applied")
	performed, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusApplied, TransactionCount: 4}, event)

	require.NoError(t, err)
	assert.True(t, performed, "the first finalise performs the transition")

	stored := bulkBatchState(t, ds, batchID)
	assert.Equal(t, model.BulkBatchStatusApplied, stored.Status)
	assert.Equal(t, event.EventID, stored.EventID,
		"the coordinator must record which event carried the outcome, which is the proof of the pairing")
	require.NotNil(t, stored.FinalizedAt)
	assert.True(t, stored.IsTerminal())

	var storedType string
	require.NoError(t, ds.Conn.QueryRow(
		`SELECT event_type FROM blnk.event_outbox WHERE event_id = $1`, event.EventID).Scan(&storedType))
	assert.Equal(t, "bulk_transaction.applied", storedType)
}

// TestFinalizeBulkTransactionBatchWithEvent_IsIdempotentForTheSameOutcome is what makes the
// retry safe.
//
// An attempt whose commit succeeded but whose acknowledgement was lost must be recognised, not
// followed by a second differently-identified event for one batch outcome.
func TestFinalizeBulkTransactionBatchWithEvent_IsIdempotentForTheSameOutcome(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, &model.BulkTransactionBatch{BatchID: batchID}))

	outcome := &model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusInflight}
	event := bulkOutcomeRow(batchID, "inflight")

	performed, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID, outcome, event)
	require.NoError(t, err)
	require.True(t, performed)

	performed, err = ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID, outcome, event)
	require.NoError(t, err, "an already-recorded outcome must be reported as success")
	assert.False(t, performed, "and it must say it did not perform the transition, so the caller stops")
}

// TestFinalizeBulkTransactionBatchWithEvent_RefusesAConflictingOutcome keeps two answers for
// one batch from being silently reduced to one.
func TestFinalizeBulkTransactionBatchWithEvent_RefusesAConflictingOutcome(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, &model.BulkTransactionBatch{BatchID: batchID}))

	_, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusApplied},
		bulkOutcomeRow(batchID, "applied"))
	require.NoError(t, err)

	_, err = ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusFailed},
		bulkOutcomeRow(batchID, "failed"))

	require.Error(t, err)
	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"a second, different outcome must be refused rather than overwriting the first")
}

// TestFinalizeBulkTransactionBatchWithEvent_AdoptsAnUncoordinatedBatch is the replacement for
// the last standalone capture on a producer that has state to be atomic with.
//
// A batch whose start-of-batch coordinator write failed used to be reported as ErrNotFound
// here, and the producer then captured its outcome with a single insert standing outside any
// transaction — so bulk_transaction.<status> was at-most-once for the whole life of a
// deployment that had suffered one database blip at the wrong moment. The finalise now ADOPTS
// such a batch: it writes the terminal coordinator row inside the same transaction as the
// outcome event, so the two commit together and requirement R-2 holds for this producer
// unconditionally.
func TestFinalizeBulkTransactionBatchWithEvent_AdoptsAnUncoordinatedBatch(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	event := bulkOutcomeRow(batchID, "applied")

	performed, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{
			BatchID:          batchID,
			Status:           model.BulkBatchStatusApplied,
			TransactionCount: 7,
		},
		event)

	require.NoError(t, err,
		"an unrecorded batch is adopted rather than refused: refusing it sends the producer to a "+
			"capture that stands outside every transaction")
	assert.True(t, performed, "the adoption recorded the outcome, so it counts as work performed")

	// BOTH ROWS, or neither. The point of adoption is that the outcome record and its event
	// share one commit, so the assertion has to be that both landed.
	var storedStatus string
	var storedCount int
	var storedEventID string
	require.NoError(t, ds.Conn.QueryRowContext(ctx, `
		SELECT status, transaction_count, event_id
		FROM blnk.bulk_transaction_batches WHERE batch_id = $1
	`, batchID).Scan(&storedStatus, &storedCount, &storedEventID))

	assert.Equal(t, model.BulkBatchStatusApplied, storedStatus,
		"the adopted row is written already-terminal")
	assert.Equal(t, 7, storedCount, "and carries the outcome the producer computed")
	assert.Equal(t, event.EventID, storedEventID,
		"the event id is recorded so the outcome and its event stay joinable")

	var events int
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		`SELECT count(*) FROM blnk.event_outbox WHERE event_id = $1`, event.EventID).Scan(&events))
	assert.Equal(t, 1, events, "the outcome event committed with the adopted row")
}

// TestFinalizeBulkTransactionBatchWithEvent_AdoptionIsIdempotent covers the retry of an
// adoption whose acknowledgement was lost.
//
// The event id is derived from the batch, so a second attempt must recognise the outcome it
// already recorded and stop rather than write a second, differently-identified event for one
// batch outcome.
func TestFinalizeBulkTransactionBatchWithEvent_AdoptionIsIdempotent(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	outcome := &model.BulkTransactionBatch{
		BatchID: batchID, Status: model.BulkBatchStatusApplied, TransactionCount: 2,
	}

	performed, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID, outcome,
		bulkOutcomeRow(batchID, "applied"))
	require.NoError(t, err)
	require.True(t, performed)

	performed, err = ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID, outcome,
		bulkOutcomeRow(batchID, "applied"))
	require.NoError(t, err, "the repeat finds the outcome already recorded and reports success")
	assert.False(t, performed, "and reports that it performed no transition")

	var events int
	require.NoError(t, ds.Conn.QueryRowContext(ctx,
		`SELECT count(*) FROM blnk.event_outbox WHERE event_id = $1`,
		bulkOutcomeRow(batchID, "applied").EventID).Scan(&events))
	assert.Equal(t, 1, events, "one batch outcome must produce exactly one event")
}

// TestFinalizeBulkTransactionBatchWithEvent_RefusesAConflictingAdoptedOutcome keeps adoption
// from overwriting an answer somebody else already recorded.
func TestFinalizeBulkTransactionBatchWithEvent_RefusesAConflictingAdoptedOutcome(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	_, err := ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusApplied},
		bulkOutcomeRow(batchID, "applied"))
	require.NoError(t, err)

	_, err = ds.FinalizeBulkTransactionBatchWithEvent(ctx, batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusFailed},
		bulkOutcomeRow(batchID, "failed"))

	require.Error(t, err, "two different outcomes for one batch must not both be accepted")
	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, apierror.ErrConflict, apiErr.Code,
		"the caller is told the state disagrees rather than silently having its answer discarded")
}

// TestFinalizeBulkTransactionBatchWithEvent_RefusesANonTerminalOutcome keeps the coordinator's
// one invariant enforceable.
//
// A finalise to 'processing' would move the row nowhere while stamping finalized_at, which the
// schema's CHECK forbids — and the stuck-batch query would then miss a batch that never
// finished.
func TestFinalizeBulkTransactionBatchWithEvent_RefusesANonTerminalOutcome(t *testing.T) {
	ds := openRealTestDB(t)

	batchID := coordinatedTestBatchID(t, ds)
	_, err := ds.FinalizeBulkTransactionBatchWithEvent(context.Background(), batchID,
		&model.BulkTransactionBatch{BatchID: batchID, Status: model.BulkBatchStatusProcessing},
		bulkOutcomeRow(batchID, "processing"))

	require.Error(t, err)

	// APIError.Error() renders only "CODE: message" — the Details, which carry the offending
	// status, are a separate field. Asserting on the code and the message is therefore the
	// assertion that actually holds; a Contains on Error() would silently pass or fail for the
	// wrong reason.
	var apiErr apierror.APIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
	assert.Contains(t, apiErr.Message, "Failed to finalize the bulk transaction batch")
	require.NotNil(t, apiErr.Details)
	detail, ok := apiErr.Details.(error)
	require.True(t, ok, "the offending status must travel in the details")
	assert.Contains(t, detail.Error(), "not a terminal batch outcome")
}

// TestInsertBulkTransactionBatch_IsIdempotentOnTheBatchID keeps a retried request from failing a
// batch because its coordinator was already recorded.
func TestInsertBulkTransactionBatch_IsIdempotentOnTheBatchID(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batch := &model.BulkTransactionBatch{BatchID: coordinatedTestBatchID(t, ds), TransactionCount: 2}
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, batch))
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, batch),
		"a repeated start must not fail the batch")
}

// TestCountUnfinalizedBulkTransactionBatches_CountsTheResidueBeyondTheGrace is the visibility
// assertion for the one window the coordinator cannot close.
//
// A batch that began and never reported an outcome is not silent loss — it is a countable state.
// The grace period is what separates a batch still legitimately running, which a large batch
// may be for minutes, from one that was abandoned.
func TestCountUnfinalizedBulkTransactionBatches_CountsTheResidueBeyondTheGrace(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	batchID := coordinatedTestBatchID(t, ds)
	require.NoError(t, ds.InsertBulkTransactionBatch(ctx, &model.BulkTransactionBatch{BatchID: batchID}))

	count, oldest, err := ds.CountUnfinalizedBulkTransactionBatches(ctx, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(1), "the batch just started is outstanding")
	require.NotNil(t, oldest, "an outstanding batch must report when the oldest of them began")

	// A grace period longer than the row's age must exclude it, or every in-flight batch is
	// reported as stuck the moment it starts.
	future, _, err := ds.CountUnfinalizedBulkTransactionBatches(ctx, time.Hour)
	require.NoError(t, err)
	assert.Less(t, future, count,
		"a batch younger than the grace period must not be counted as abandoned")
}

// bulkBatchState reads a coordinator row directly.
//
// The columns are read here rather than through a repository getter, because no production code
// reads a coordinator row by id — the finalise conditions its UPDATE instead, and an operator
// queries the table — and adding an exported accessor with no caller would be adding API for a
// test's convenience.
func bulkBatchState(t *testing.T, ds Datasource, batchID string) model.BulkTransactionBatch {
	t.Helper()

	batch := model.BulkTransactionBatch{BatchID: batchID}
	var errorMessage, eventID sql.NullString
	var finalizedAt sql.NullTime
	require.NoError(t, ds.Conn.QueryRow(`
		SELECT status, transaction_count, error_message, atomic, inflight, event_id, created_at, finalized_at
		FROM blnk.bulk_transaction_batches WHERE batch_id = $1
	`, batchID).Scan(&batch.Status, &batch.TransactionCount, &errorMessage, &batch.Atomic,
		&batch.Inflight, &eventID, &batch.CreatedAt, &finalizedAt))

	batch.ErrorMessage = errorMessage.String
	batch.EventID = eventID.String
	if finalizedAt.Valid {
		at := finalizedAt.Time
		batch.FinalizedAt = &at
	}

	return batch
}

// bulkOutcomeRow builds the outcome event row for a batch, with the derived id the producer
// derives.
func bulkOutcomeRow(batchID, status string) *model.EventOutbox {
	eventType := "bulk_transaction." + status
	payload := json.RawMessage(fmt.Sprintf(`{"event":%q,"data":{"batch_id":%q,"status":%q}}`,
		eventType, batchID, status))

	return &model.EventOutbox{
		EventID:       model.DeriveEventID(batchID, eventType, model.SchemaVersionV1),
		EventType:     eventType,
		AggregateID:   batchID,
		PartitionKey:  batchID,
		Topic:         "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payload,
		EventRaw:      payload,
		OccurredAt:    time.Now().UTC(),
		Status:        model.EventOutboxStatusPending,
		MaxAttempts:   5,
	}
}
