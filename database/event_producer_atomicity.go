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

// event_producer_atomicity.go is the repository implementation for the last two event families
// to be brought under requirement R-2: balance.monitor and bulk_transaction.<status>.
//
// Every other event type is captured inside the database transaction that performs
// its mutation — see the three atomic writers in transaction.go. These two looked as though
// they could not be, and for two different structural reasons:
//
//   - balance.monitor fires because a CONDITION was met on a balance, and the original design
//     evaluated that condition after the movement had committed. By the time the alert existed
//     there was no open transaction to enrol it in.
//   - bulk_transaction.<status> summarises a batch executed one transaction at a
//     time, each under its own transaction. There is no batch-spanning transaction
//     for the summary to join.
//
// Neither is solved by retrying the capture, because retrying cannot close a
// process-crash window. They are solved differently from each other, and the difference matters:
//
//   - balance.monitor is now DECIDED INSIDE the mutation's transaction. The writer reads the
//     monitor definitions there, applies model.BalanceMonitor.CheckCondition, and inserts the
//     canonical blnk.event_outbox row before the COMMIT — see recordBalanceMonitorEvaluation and
//     captureBalanceMonitorAlertsInTx. There is no intent and no conversion.
//   - bulk_transaction.<status> is given something durable to be atomic WITH. The batch
//     coordinator records, before any member transaction runs, that a batch began; its
//     transition to a terminal outcome and the outcome event then commit together.
//
// blnk.balance_monitor_handoff remains, and this file still implements it, for a finite
// population: rows written by releases that predate the in-transaction capture, and rows written
// by a process with no BalanceMonitorAlertCapture registered, which cannot build an event row at
// all. For those the pattern is the coordinator's — a pre-recorded intent carrying both decision
// inputs, plus an atomic completion — so what is left over is never a lost event but an
// unfinished intent, which is visible, countable and finishable.
//
// The claim query, the apierror wrapping and the status vocabulary are lifted from
// lineage.go and event_outbox.go rather than reinvented, for the same reason those two
// agree with each other: a third shape would be a maintenance liability.
package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// balanceMonitorHandoffColumns is the projection every read of the handoff table
// uses, declared once so the column order and the Scan order cannot drift apart.
const balanceMonitorHandoffColumns = `id, handoff_id, balance_id, ledger_id, balance_snapshot, ` +
	`monitor_snapshot, status, attempts, max_attempts, last_error, events_captured, created_at, ` +
	`processed_at, locked_until`

// selectBalanceMonitorsInTx reads the monitor definitions of the supplied balances
// INSIDE the caller's transaction, keyed by balance id.
//
// # This read is the other half of R-2, and the transaction is why
//
// A `balance.monitor` alert is decided by two inputs: the balance's post-mutation state and
// the monitor definitions in force at that moment. The balance was already snapshotted in
// this transaction. Reading the definitions here puts BOTH inside the same commit, so the
// evaluation the handoff processor performs later is a pure function of the row it claimed.
//
// Reading them afterwards — which is what the handoff processor used to do — left one input
// live. blnk.balance_monitors is operator-editable through PUT and DELETE
// /balance-monitors, so a monitor deleted between the commit and the drain suppressed an
// alert for a movement that had already crossed its threshold; one created in that window
// produced an alert for a movement it was never registered to watch; an edited threshold
// produced an alert for a condition that was not the condition in force. None of it was
// visible from the outbox, because the row looked identical either way.
//
// It also makes a RETRY deterministic. The claim lease can lapse and a row can be evaluated
// twice; model.BalanceMonitorEventIdentity makes the second attempt collide with the first
// on purpose so it is a no-op — which only holds if the second reaches the same verdict.
//
// # One statement, one index lookup per balance
//
// The read is a single `balance_id = ANY($1)` served by idx_balance_monitors_balance_id, the
// index the handoff table's own migration added for exactly this access pattern. It REPLACES
// the per-balance correlated EXISTS the insert used to carry, so a deployment with no
// monitors configured now pays one indexed probe for the whole statement rather than one per
// balance, and still writes nothing.
//
// # The scan tolerates NULL where GetBalanceMonitors does not, deliberately
//
// blnk.balance_monitors.precision and precise_value are nullable columns.
// Datasource.GetBalanceMonitors scans them into a bare float64 and int64, so a NULL there is
// a scan error on that path. Here it must not be: this statement runs inside a MONEY
// TRANSACTION, and a monitor row with an unset precision must never roll back a ledger
// movement. The nullable columns are read through sql.Null* and flattened to the same zero
// values the non-null case produces, so a row readable by both paths decodes identically —
// which is what keeps the event payload bytes identical to the pre-snapshot behaviour — and
// a row readable by only one is still evaluated rather than dropped.
//
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - tx *sql.Tx: the caller's open transaction. Required.
//   - balanceIDs []string: the balances to read monitors for. Empty yields no read.
//
// Returns:
//   - map[string][]model.BalanceMonitor: the definitions, keyed by balance id. A balance
//     with no monitor is absent from the map rather than present with an empty slice.
//   - error: the wrapped statement or scan error, which correctly rolls the caller back.
func selectBalanceMonitorsInTx(
	ctx context.Context, tx *sql.Tx, balanceIDs []string,
) (map[string][]model.BalanceMonitor, error) {
	if len(balanceIDs) == 0 {
		return nil, nil
	}

	if tx == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to read balance monitors",
			errors.New("balance monitors must be snapshotted inside the balance's own transaction"),
		)
	}

	// The column list and its order mirror Datasource.GetBalanceMonitors exactly, so the two
	// reads of this table cannot decode the same row into different values.
	rows, err := tx.QueryContext(ctx, `
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url,
		       created_at, precision, precise_value
		FROM blnk.balance_monitors
		WHERE balance_id = ANY($1)
	`, pq.Array(balanceIDs))
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read balance monitors", err)
	}
	defer func() { _ = rows.Close() }()

	monitors := make(map[string][]model.BalanceMonitor, len(balanceIDs))
	for rows.Next() {
		monitor := model.BalanceMonitor{}
		condition := model.AlertCondition{}

		var description, callBackURL sql.NullString
		var precision sql.NullFloat64
		var preciseValue sql.NullInt64

		if err := rows.Scan(
			&monitor.MonitorID,
			&monitor.BalanceID,
			&condition.Field,
			&condition.Operator,
			&condition.Value,
			&description,
			&callBackURL,
			&monitor.CreatedAt,
			&precision,
			&preciseValue,
		); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan a balance monitor", err)
		}

		monitor.Description = description.String
		monitor.CallBackURL = callBackURL.String
		condition.Precision = precision.Float64
		// ALWAYS NON-NIL, matching GetBalanceMonitors, which assigns big.NewInt
		// unconditionally. A nil pointer here would marshal as JSON null and CheckCondition
		// would have to guard it, so the zero is carried explicitly.
		condition.PreciseValue = big.NewInt(preciseValue.Int64)
		monitor.Condition = condition

		monitors[monitor.BalanceID] = append(monitors[monitor.BalanceID], monitor)
	}

	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read balance monitors", err)
	}

	return monitors, nil
}

// insertBalanceMonitorHandoffsInTx records, inside the caller's transaction, the pending
// evaluation of every supplied balance's monitors — together with both inputs that
// evaluation depends on.
//
// # THIS IS THE FALLBACK, not the ordinary path — read captureBalanceMonitorAlertsInTx first
//
// A writer that can build an event row inserts the CANONICAL blnk.event_outbox row for each
// crossing in this transaction and writes no handoff at all; that is what requirement R-2 asks
// for and what recordBalanceMonitorEvaluation does whenever a BalanceMonitorAlertCapture is
// registered, which the service constructor always does. This function is reached when NO
// capture is registered — a Datasource built directly, without the root service, which is what
// the repository's own tests do — and it is what stops such a process from committing a movement
// whose alerts would be decided later against whatever the monitors table says then.
// BalanceMonitorHandoffProcessor converts the row, and it remains the drain for handoff rows
// written by releases that predate the in-transaction capture.
//
// # This is the atomicity, and it is why the function takes a *sql.Tx
//
// The transaction belongs to the atomic writer that is updating these balances. Writing
// the row here — after the balance UPDATEs and before the COMMIT — is what makes a
// committed movement inseparable from its pending evaluation. Move this after the
// commit and the guarantee is gone while every happy-path test still passes, which is
// precisely the failure the handoff exists to remove.
//
// # BOTH DECISION INPUTS ARE CAPTURED, which is what makes the deferred insert safe
//
// selectBalanceMonitorsInTx reads the monitor definitions in this same transaction, and
// model.PrepareBalanceMonitorHandoffs stores them beside the balance snapshot. So a
// committed movement carries the complete decision: the balance as written, and the
// definitions in force when it was. The evaluator is then a pure function of the row —
// see the note on selectBalanceMonitorsInTx for the three ways a live read diverged. What it
// does NOT carry is the canonical event row itself, which is the one respect in which this path
// falls short of the in-transaction capture above.
//
// # A row is written only for a balance that HAS a monitor
//
// That guard used to be a per-balance correlated EXISTS in the insert's WHERE clause, and
// it is now a consequence of the read: a balance absent from the monitor map produces no
// handoff in model.PrepareBalanceMonitorHandoffs, so no row is built. One statement
// replaces one probe per balance, and the economics that motivated the guard are unchanged
// — the overwhelming majority of balances carry no monitor, and a row per balance per
// transaction would be pure write amplification at the system's throughput target, every
// one destined to be evaluated to "nothing fired" and deleted. A deployment with no
// monitors configured still writes nothing.
//
// The guard is sound because a monitor cannot fire retroactively. A monitor registered
// AFTER a movement was never intended to alert on it, which is exactly the behaviour of
// the post-commit evaluation this replaces, so deciding at write time changes nothing an
// operator can observe. Deciding it from a snapshot rather than at drain time is what makes
// that statement TRUE rather than merely intended.
//
// idx_balance_monitors_balance_id, added by the same migration as this table, is what
// keeps the read an index lookup. blnk.balance_monitors has a foreign key on
// balance_id but PostgreSQL does not index a foreign-key column automatically, so
// before that index this read would have been a sequential scan holding the balance
// locks for its duration.
//
// # Nothing here fails the caller for a reason of its own
//
// A nil balance, a balance with a blank id and an empty list are all skipped rather than
// rejected. The writers call this unconditionally, and a movement must never be refused
// because an alerting side effect could not be described. A genuine statement error IS
// returned, because at that point the transaction is aborted anyway and the caller's
// rollback is the correct outcome.
//
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - tx *sql.Tx: the caller's open transaction. Required; a nil tx is a programming
//     error and returns an error rather than silently writing outside the transaction.
//   - balances []*model.Balance: the balances the transaction is updating, in their
//     POST-mutation state. Each is snapshotted as written.
//
// Returns:
//   - error: nil when nothing needed writing or the write succeeded; the wrapped
//     statement error otherwise.
func insertBalanceMonitorHandoffsInTx(ctx context.Context, tx *sql.Tx, balances []*model.Balance) error {
	if len(balances) == 0 {
		return nil
	}

	if tx == nil {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to record balance monitor handoffs",
			errors.New("a balance monitor handoff must be written inside the balance's own transaction"),
		)
	}

	// THE MONITOR DEFINITIONS, READ IN THIS TRANSACTION. This is the R-2 half that was
	// missing: without it the row carries only the balance and the evaluator has to read
	// the definitions live, after the commit, from a table an operator can edit.
	balanceIDs := make([]string, 0, len(balances))
	for _, balance := range balances {
		if balance == nil {
			continue
		}
		if trimmed := strings.TrimSpace(balance.BalanceID); trimmed != "" {
			balanceIDs = append(balanceIDs, trimmed)
		}
	}

	monitors, err := selectBalanceMonitorsInTx(ctx, tx, balanceIDs)
	if err != nil {
		return err
	}

	// NOTHING MONITORED, NOTHING WRITTEN, and the read is the whole guard: this is the
	// common case for a deployment with no monitors configured, and it costs one indexed
	// statement rather than a row per balance.
	if len(monitors) == 0 {
		return nil
	}

	handoffs, err := model.PrepareBalanceMonitorHandoffs(balances, monitors)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to prepare balance monitor handoffs", err)
	}

	if len(handoffs) == 0 {
		return nil
	}

	var query strings.Builder
	query.WriteString(`
		INSERT INTO blnk.balance_monitor_handoff
		(handoff_id, balance_id, ledger_id, balance_snapshot, monitor_snapshot)
		VALUES
	`)

	args := make([]interface{}, 0, len(handoffs)*5)
	for index, handoff := range handoffs {
		if index > 0 {
			query.WriteString(",")
		}
		base := len(args) + 1
		// The casts stay on every tuple now that this is a plain VALUES list on the INSERT
		// rather than a joined subquery: they cost nothing, and they keep the two jsonb
		// placeholders unambiguous whichever tuple the planner types the list from.
		fmt.Fprintf(&query, "($%d::text, $%d::text, $%d::text, $%d::jsonb, $%d::jsonb)",
			base, base+1, base+2, base+3, base+4)

		ledgerID := interface{}(nil)
		if handoff.LedgerID != "" {
			ledgerID = handoff.LedgerID
		}
		args = append(args,
			handoff.HandoffID,
			handoff.BalanceID,
			ledgerID,
			[]byte(handoff.BalanceSnapshot),
			[]byte(handoff.MonitorSnapshot),
		)
	}

	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record balance monitor handoffs", err)
	}

	return nil
}

// balancesAlreadyEvaluatedInTx reports which balances the CALLER has already evaluated
// the monitors of, read from the event rows this same transaction is inserting.
//
// # The duplicate this exists to prevent
//
// Two mechanisms in this repository bring `balance.monitor` under requirement R-2, and
// they were built independently:
//
//   - the CALLER-SIDE pass — blnk.prepareBalanceMonitorEvents — reads a moved balance's
//     monitors before the write and hands the resulting `balance.monitor` rows to the
//     writer, so the alert is inserted in the very transaction that moved the balance.
//     It runs on the single-transaction path only.
//   - the WRITER-SIDE capture — recordBalanceMonitorEvaluation below — reads those
//     definitions inside the writer's own transaction, applies the same
//     model.BalanceMonitor.CheckCondition, and inserts the canonical alert row there. It
//     runs on every atomic writer, including the coalescing path the plan freezes.
//
// Both are correct in isolation. Run together on one balance they publish the SAME
// crossing twice, under two different event ids — and a `balance.monitor` id is a fresh
// UUID by design, precisely so a monitor that fires repeatedly is not collapsed into one
// event, so no subscriber-side idempotency could ever collapse the pair. The duplicate
// would be indistinguishable from two genuine crossings.
//
// # Why the two compose rather than one replacing the other
//
// They insert the SAME ROW, built by the same code, in the SAME transaction; they differ only
// in where the monitor definitions were read. The caller-side pass reads them from the monitor
// cache before the write, so it costs no statement inside the transaction and it is the hot
// path. The writer-side capture reads them from blnk.balance_monitors inside the transaction,
// which is one indexed lookup, and it reaches what the caller cannot: the coalesced batch,
// whose argument list is frozen, and any balance whose monitors could not be read before the
// write. Keeping both, with the writer-side capture suppressed exactly where the caller already
// evaluated, is strictly better than either alone: every path is covered, nothing is published
// twice, and the hot path keeps the cheaper read.
//
// # Reading the coverage from the rows rather than from a new parameter
//
// The balance is taken from the event's own payload, which carries the marshaled
// BalanceMonitor and therefore its balance_id. AggregateID is the MONITOR id for this
// event type (see blnk.eventAggregateID) and so cannot answer the question. Threading a
// coverage set down as a new writer argument would change three exported repository
// signatures, IDataSource and every mock of it, to communicate something the rows the
// writer is already inserting state exactly.
//
// A payload that will not parse yields no coverage, which is the safe direction: the
// writer-side capture evaluates the balance as well, and the worst case is the duplicate this
// function exists to avoid rather than an alert nobody evaluates. It cannot happen in
// practice — the row was produced by marshalling a BalanceMonitor — and a bookkeeping
// read must not be the thing that refuses a money write.
//
// Parameters:
//   - rows []*model.EventOutbox: the event rows this transaction is inserting. Nil
//     entries and every event type other than balance.monitor are ignored.
//
// Returns:
//   - map[string]struct{}: the balances whose monitors the caller evaluated. Nil when
//     none did, so the caller can test it with a plain length check.
func balancesAlreadyEvaluatedInTx(rows []*model.EventOutbox) map[string]struct{} {
	var evaluated map[string]struct{}

	for _, row := range rows {
		if row == nil || row.EventType != model.EventTypeBalanceMonitor {
			continue
		}

		// The legacy two-key envelope: {"event": "balance.monitor", "data": {…}}. Only
		// the balance id is read, so the rest of the monitor is left untouched — this
		// must not become a second definition of the payload's shape.
		var envelope struct {
			Data struct {
				BalanceID string `json:"balance_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(row.Payload, &envelope); err != nil {
			continue
		}

		balanceID := strings.TrimSpace(envelope.Data.BalanceID)
		if balanceID == "" {
			continue
		}

		if evaluated == nil {
			evaluated = make(map[string]struct{}, len(rows))
		}
		evaluated[balanceID] = struct{}{}
	}

	return evaluated
}

// balancesAwaitingMonitorEvaluation is the balance set the handoff is written for: the
// ones the caller did not already evaluate.
//
// # Coverage is per BALANCE, and that is exactly right
//
// The caller-side pass evaluates ALL of a balance's monitors in one read. So a balance
// that produced even one alert had every one of its monitors judged, and the ones that
// did not fire need no second look — a later movement will bring its own handoff. A
// balance that produced NO alert is left in the set: it either has no monitors, in which
// case the insert's own EXISTS clause skips it, or its monitor read failed before the
// write, which is the case the handoff is the durable answer to.
//
// Parameters:
//   - balances []*model.Balance: the balances this transaction is updating.
//   - evaluated map[string]struct{}: the coverage from balancesAlreadyEvaluatedInTx.
//
// Returns:
//   - []*model.Balance: the balances still needing evaluation. The input slice is
//     returned unchanged when nothing was covered, which is the common case.
func balancesAwaitingMonitorEvaluation(balances []*model.Balance, evaluated map[string]struct{}) []*model.Balance {
	if len(evaluated) == 0 {
		return balances
	}

	awaiting := make([]*model.Balance, 0, len(balances))
	for _, balance := range balances {
		if balance == nil {
			continue
		}
		if _, covered := evaluated[balance.BalanceID]; covered {
			continue
		}
		awaiting = append(awaiting, balance)
	}

	return awaiting
}

// captureBalanceMonitorAlertsInTx evaluates the monitors of the supplied balances inside the
// caller's transaction and inserts the CANONICAL balance.monitor event rows for the crossings
// it decides.
//
// # This is requirement R-2 for balance.monitor, met literally
//
// Everything the verdict depends on is available here: the balance in its post-mutation state
// because the writer has just updated it, and the monitor definitions because
// selectBalanceMonitorsInTx reads them in this transaction. So the crossing is DECIDED here and
// the blnk.event_outbox row announcing it is INSERTED here, before the commit. A committed
// movement therefore carries its alerts, and a rolled-back one carries none, with no second
// transaction in between.
//
// This is what replaced the deferred conversion. The writer used to commit a
// balance_monitor_handoff row carrying the same two inputs and leave the evaluation and the
// canonical insert to BalanceMonitorHandoffProcessor, so the event R-2 names did not exist until
// that second transaction succeeded. See recordBalanceMonitorEvaluation for the one shape that
// still takes the handoff.
//
// # The condition is evaluated by the frozen model method, unchanged
//
// model.BalanceMonitor.CheckCondition is the single evaluator in this repository and AAP §0.6.2
// freezes monitor condition evaluation. It is called here exactly as the caller-side pass
// (blnk.prepareBalanceMonitorEvents) and the handoff drain (BalanceMonitorHandoffProcessor) call
// it, against the same balance value, so which alerts exist does not depend on which route
// decided them. The row is built by the registered capture, which is the same construction all
// three routes use, so what each alert SAYS does not depend on the route either.
//
// # Writing only what fired is strictly less write amplification than the handoff
//
// The handoff wrote one row per MONITORED balance whether or not anything fired, to be
// evaluated and then deleted. This writes one row per CROSSING. A balance carrying ten monitors
// that meets none of them now costs one indexed read and no write at all, where it previously
// cost a jsonb row round trip through a second transaction.
//
// # Failure is fatal to the movement, deliberately
//
// A capture error — a payload that will not serialise, a partition key that cannot be resolved —
// is a producer defect rather than a transient condition, and it aborts the writer. That is the
// same answer resolveBatchEventOutboxes gives for a transaction event and the same answer the
// caller-side pass gives, and it is the answer R-2 requires: a mutation whose event cannot be
// captured must not commit. A nil row is NOT a failure; it is the unconfigured case, and it is
// unreachable from here because recordBalanceMonitorEvaluation has already checked the same
// predicate, so it is skipped rather than inserted.
//
// Parameters:
//   - ctx context.Context: the writer's context.
//   - d Datasource: the datasource whose InsertEventOutboxInTx performs the insert, passed
//     explicitly for the same reason captureEntityEvent takes it — one exported in-transaction
//     insert serves every producer.
//   - tx *sql.Tx: the caller's open transaction. Required.
//   - balances []*model.Balance: the balances this transaction is updating, POST-mutation.
//     Nil entries and blank ids are skipped.
//   - capture BalanceMonitorAlertCapture: the registered row builder. Must not be nil.
//
// Returns:
//   - int: the number of balances that carried at least one monitor and were evaluated.
//   - int: the number of canonical alert rows inserted.
//   - error: the monitor read's error, the capture's error, or the insert's error. Each
//     correctly rolls the writer back.
func captureBalanceMonitorAlertsInTx(
	ctx context.Context,
	d Datasource,
	tx *sql.Tx,
	balances []*model.Balance,
	capture BalanceMonitorAlertCapture,
) (int, int, error) {
	if len(balances) == 0 {
		return 0, 0, nil
	}

	if tx == nil {
		return 0, 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to capture balance monitor alerts",
			errors.New("a balance monitor alert must be written inside the balance's own transaction"),
		)
	}

	balanceIDs := make([]string, 0, len(balances))
	for _, balance := range balances {
		if balance == nil {
			continue
		}
		if trimmed := strings.TrimSpace(balance.BalanceID); trimmed != "" {
			balanceIDs = append(balanceIDs, trimmed)
		}
	}

	monitors, err := selectBalanceMonitorsInTx(ctx, tx, balanceIDs)
	if err != nil {
		return 0, 0, err
	}

	// NOTHING MONITORED, NOTHING EVALUATED, and the read is the whole guard: this is the
	// common case for a deployment with no monitors configured, and it costs one indexed
	// statement rather than a row per balance.
	if len(monitors) == 0 {
		return 0, 0, nil
	}

	evaluatedBalances, capturedAlerts := 0, 0

	for _, balance := range balances {
		if balance == nil {
			continue
		}

		balanceMonitors, monitored := monitors[strings.TrimSpace(balance.BalanceID)]
		if !monitored || len(balanceMonitors) == 0 {
			continue
		}

		evaluatedBalances++

		for _, monitor := range balanceMonitors {
			if !monitor.CheckCondition(balance) {
				continue
			}

			row, captureErr := capture(ctx, balance, monitor)
			if captureErr != nil {
				return evaluatedBalances, capturedAlerts, apierror.NewAPIError(
					apierror.ErrInternalServer,
					"Failed to capture the balance monitor alert for a moved balance",
					fmt.Errorf("blnk: capturing the balance.monitor event for monitor %q on balance %q: %w",
						monitor.MonitorID, balance.BalanceID, captureErr),
				)
			}

			// Unreachable from the gate, which has already established that publishing is
			// configured, and skipped rather than inserted so a capture that answers nil for a
			// reason of its own cannot become a nil-pointer dereference inside a money write.
			if row == nil {
				continue
			}

			if err := d.InsertEventOutboxInTx(ctx, tx, row); err != nil {
				return evaluatedBalances, capturedAlerts, err
			}

			capturedAlerts++
		}
	}

	return evaluatedBalances, capturedAlerts, nil
}

// recordBalanceMonitorEvaluation is the gate the atomic writers call.
//
// It answers two questions — does this deployment capture events at all, and did the
// caller already evaluate these monitors inside this very transaction — and evaluates only
// what is left. The first gate is not an optimisation; it is what keeps two deployment
// shapes correct at once:
//
//   - With Kafka configured, the monitors are evaluated here and the canonical alert rows are
//     inserted in this transaction. The post-commit evaluation stands down, so the alert is
//     captured exactly once, transactionally.
//   - With no Kafka configured there is no relay and no outbox to drain, so a row written here
//     would be one nothing can ever publish. The gate writes nothing, and the post-commit path
//     publishes down the legacy transport exactly as it did before this feature existed
//     (AAP §0.5.4).
//
// The predicate is config.Configuration.EventPublishingConfigured, deliberately shared
// with the root package's eventPublishingConfigured so the writer and the post-commit
// path cannot reach opposite conclusions and leave a movement evaluated by neither.
//
// The SECOND gate is what keeps the two R-2 mechanisms from publishing one crossing
// twice — see balancesAlreadyEvaluatedInTx for why both exist and how they compose.
//
// # The handoff is the fallback, and what it is still for
//
// When a balance monitor alert capture is registered — which the service constructor always
// does — the canonical blnk.event_outbox row is inserted here and no handoff is written. With
// NO capture registered the writer cannot build a row at all, so it falls back to committing the
// handoff: both decision inputs, frozen inside this transaction, for
// BalanceMonitorHandoffProcessor to convert. That reaches a Datasource constructed directly
// without the root service, which is what the repository's own tests do, and it keeps such a
// process from committing a movement whose alerts are decided against whatever the monitors
// table says later. The processor also remains the drain for handoff rows written by releases
// that predate this change, so an upgrade loses nothing that was in flight.
//
// A configuration read failure is NOT fatal to the ledger write. It is logged by
// config.Fetch's own path and treated here as "not configured", which is the safe
// direction: the post-commit path still evaluates, so the alert is delayed at worst,
// never lost, and a money movement is never refused because a configuration lookup
// failed.
//
// Parameters:
//   - ctx context.Context: the context for the statements.
//   - d Datasource: the datasource whose InsertEventOutboxInTx inserts the alert rows.
//   - tx *sql.Tx: the writer's open transaction.
//   - span trace.Span: the writer's span, annotated with what was evaluated and captured.
//   - balances []*model.Balance: the balances this transaction is updating.
//   - capturedEvents []*model.EventOutbox: the event rows this transaction is inserting,
//     read for the balances whose monitors the caller already evaluated.
//
// Returns:
//   - error: only a genuine capture or statement failure, which correctly rolls the writer back.
func recordBalanceMonitorEvaluation(ctx context.Context, d Datasource, tx *sql.Tx, span trace.Span, balances []*model.Balance, capturedEvents []*model.EventOutbox) error {
	cnf, err := config.Fetch()
	if err != nil || !cnf.EventPublishingConfigured() {
		return nil
	}

	evaluated := balancesAlreadyEvaluatedInTx(capturedEvents)
	awaiting := balancesAwaitingMonitorEvaluation(balances, evaluated)
	if len(awaiting) == 0 {
		// Every moved balance was evaluated with the mutation, so there is nothing left to
		// evaluate here. Recorded on the span rather than passed silently: "nothing to do" and
		// "suppressed because the caller covered every balance" are different facts, and only
		// one of them means the alerts are already durable.
		span.AddEvent("Balance monitor evaluation not needed; every balance was evaluated with the mutation", trace.WithAttributes(
			attribute.Int("balance_monitor_handoff.balances_evaluated_by_caller", len(evaluated)),
		))

		return nil
	}

	if capture := registeredBalanceMonitorAlertCapture(); capture != nil {
		evaluatedHere, capturedAlerts, captureErr := captureBalanceMonitorAlertsInTx(ctx, d, tx, awaiting, capture)
		if captureErr != nil {
			return captureErr
		}

		span.AddEvent("Balance monitor alerts captured with the mutation", trace.WithAttributes(
			attribute.Int("balance_monitor_alert.balances", len(awaiting)),
			attribute.Int("balance_monitor_alert.balances_monitored", evaluatedHere),
			attribute.Int("balance_monitor_alert.alerts_captured", capturedAlerts),
			attribute.Int("balance_monitor_alert.balances_evaluated_by_caller", len(evaluated)),
		))

		return nil
	}

	if err := insertBalanceMonitorHandoffsInTx(ctx, tx, awaiting); err != nil {
		return err
	}

	span.AddEvent("Balance monitor handoffs recorded", trace.WithAttributes(
		attribute.Int("balance_monitor_handoff.balances", len(awaiting)),
		attribute.Int("balance_monitor_handoff.balances_evaluated_by_caller", len(evaluated)),
	))

	return nil
}

// claimPendingBalanceMonitorHandoffQuery is the claim statement, declared at package level
// so a test can assert on its text. The two properties a test pins here — MATERIALIZED and
// FOR UPDATE SKIP LOCKED — are both invisible in behaviour until the exact conditions that
// break them occur, so asserting the shape is what keeps them.
const claimPendingBalanceMonitorHandoffQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.balance_monitor_handoff candidate
			WHERE candidate.status IN ('pending', 'processing')
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.attempts < candidate.max_attempts
			ORDER BY candidate.created_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.balance_monitor_handoff
			SET status = $1, locked_until = NOW() + $2::interval, attempts = attempts + 1
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + balanceMonitorHandoffColumns + `
		)
		SELECT * FROM claimed ORDER BY created_at ASC, id ASC
	`

// ClaimPendingBalanceMonitorHandoffs claims a batch of handoffs for evaluation.
//
// It is the event outbox claim query with this table's columns: a MATERIALIZED CTE
// selects a bounded, creation-ordered, FOR UPDATE SKIP LOCKED set of ids, a second CTE
// UPDATEs exactly those ids, and the outer SELECT re-sorts the returned rows because
// UPDATE ... RETURNING does not preserve the inner ORDER BY. SKIP LOCKED is what lets
// several processors run without blocking each other, and the re-sort is what keeps a
// batch FIFO.
//
// # The candidate selection MUST stay a MATERIALIZED CTE
//
// The obvious spelling — `UPDATE … WHERE id IN (SELECT … LIMIT $3 FOR UPDATE SKIP
// LOCKED)`, which is what ClaimPendingOutboxEntries in database/lineage.go still uses —
// does NOT reliably honour the limit here, and the difference is this statement's
// `attempts = attempts + 1`. The subquery filters on `attempts < max_attempts`, so the
// UPDATE writes a column its own semi-join subplan reads; the planner is then free to
// re-evaluate that subplan, and each evaluation applies the LIMIT afresh. Measured against
// PostgreSQL 16 with three claimable rows and a batch size of two, the inline form claimed
// all THREE, while the lineage query — which does not touch `attempts` — claimed two from
// the identical table.
//
// It is worse than a wrong number, because it is PLAN-DEPENDENT: it appears on a small
// table and hides on a large one, so it survives a full test suite and surfaces on an empty
// deployment. Over-claiming leases rows the processor will not reach in this pass, and every
// one of them sits unevaluated until its lease expires — the exact delay to a monitor alert
// that the handoff exists to prevent.
//
// A MATERIALIZED CTE is evaluated exactly once by definition, which makes the limit a fact
// about the statement rather than about the plan the planner happened to choose.
//
// A claim is a LEASE, not a lock: it sets status to processing and locked_until to
// now plus the lease, so a processor that dies mid-evaluation has its rows re-claimed
// once the lease expires. That is the at-least-once half of the guarantee. The
// exactly-once half is not enforced here — it is enforced by the DERIVED event ids the
// evaluation produces, which make a re-evaluation of the same handoff insert the same
// rows and collide with the unique index rather than duplicate the alert.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//   - batchSize int: the maximum number of handoffs to claim.
//   - lockDuration time.Duration: how long the claim is held.
//
// Returns:
//   - []model.BalanceMonitorHandoff: the claimed handoffs, oldest first.
//   - error: the wrapped query error.
func (d Datasource) ClaimPendingBalanceMonitorHandoffs(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.BalanceMonitorHandoff, error) {
	rows, err := d.Conn.QueryContext(ctx, claimPendingBalanceMonitorHandoffQuery,
		model.OutboxStatusProcessing, lockDuration.String(), batchSize)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to claim pending balance monitor handoffs", err)
	}
	defer func() { _ = rows.Close() }()

	handoffs := make([]model.BalanceMonitorHandoff, 0, batchSize)
	for rows.Next() {
		handoff, scanErr := scanBalanceMonitorHandoff(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		handoffs = append(handoffs, *handoff)
	}

	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read claimed balance monitor handoffs", err)
	}

	return handoffs, nil
}

// CompleteBalanceMonitorHandoffWithEvents writes the evaluation's result and the
// handoff's completion in ONE database transaction.
//
// # This single transaction is the whole point of the handoff
//
// The alert events and the record that this balance's monitors have been evaluated
// commit together. There is therefore no state in which the evaluation is marked done
// and its alerts are missing, and none in which the alerts exist and the handoff is
// still claimable — the two failure modes that a "publish then mark" sequence permits.
//
// A zero-length event list is the COMMON case and is not a no-op: it means the monitors
// were evaluated and no condition was met, which is a result worth recording. Recording
// it is what distinguishes "evaluated, nothing fired" from "never evaluated", and those
// are indistinguishable from the event outbox alone.
//
// # Why the UPDATE is not conditioned on the lease
//
// A lapsed lease can let two processors evaluate one handoff concurrently. That is
// tolerated deliberately rather than fenced, because the events they produce carry
// DERIVED ids — a function of the handoff id and the monitor id — so the second
// insert collides with the unique index on event_id and the repository reports the
// existing row as success. The duplicate is absorbed by the schema instead of by a
// lock, which is both cheaper and more robust: it also absorbs a duplicate produced by
// a retry, a replay or a restart, none of which a lease would catch.
//
// Parameters:
//   - ctx context.Context: the context for the transaction.
//   - handoffID string: the handoff being completed.
//   - events []*model.EventOutbox: the balance.monitor rows the evaluation produced,
//     possibly empty.
//
// Returns:
//   - error: nil on commit; the wrapped failure otherwise, in which case nothing was
//     written and the handoff remains claimable.
func (d Datasource) CompleteBalanceMonitorHandoffWithEvents(ctx context.Context, handoffID string, events []*model.EventOutbox) error {
	ctx, span := otel.Tracer("database.balance_monitor_handoff").Start(ctx, "CompleteBalanceMonitorHandoffWithEvents")
	defer span.End()

	trimmed := strings.TrimSpace(handoffID)
	if trimmed == "" {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to complete the balance monitor handoff",
			errors.New("a handoff id is required"),
		)
	}

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	// ALREADY-STORED ALERTS ARE FILTERED OUT BEFORE THE INSERT, and this is what makes a
	// repeated evaluation succeed rather than abort.
	//
	// PostgreSQL marks a transaction ABORTED after any error, so a unique violation here could
	// not be caught and recovered from inside this transaction — the way the standalone insert
	// recovers from it. The duplicate therefore has to be removed BEFORE the statement runs.
	fresh, err := filterAlreadyStoredMonitorAlerts(ctx, tx, events)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := insertEventOutboxesInTx(ctx, tx, fresh); err != nil {
		span.RecordError(err)
		return err
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE blnk.balance_monitor_handoff
		SET status = $1,
		    processed_at = NOW(),
		    locked_until = NULL,
		    last_error = NULL,
		    events_captured = $2
		WHERE handoff_id = $3
		  AND status <> $1
	`, model.OutboxStatusCompleted, len(events), trimmed)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to complete the balance monitor handoff", err)
	}

	// A row count of zero means the handoff was ALREADY completed, by a concurrent
	// evaluation whose lease overlapped this one. That is not an error: the events this
	// transaction inserted carry the same derived ids as the ones already stored, so the
	// insert above resolved to the existing rows and committing changes nothing. The
	// commit still runs, because rolling back here would leave the caller believing the
	// evaluation failed and schedule a third attempt at work that is finished.
	affected, err := result.RowsAffected()
	if err == nil && affected == 0 {
		span.AddEvent("Balance monitor handoff was already completed", trace.WithAttributes(
			attribute.String("handoff.id", trimmed),
		))
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit the balance monitor handoff", err)
	}

	span.SetAttributes(
		attribute.String("handoff.id", trimmed),
		attribute.Int("handoff.events_captured", len(events)),
	)

	return nil
}

// filterAlreadyStoredMonitorAlerts drops the alerts that are already durable.
//
// # Why this is needed at all
//
// A handoff can legitimately be evaluated twice — a lapsed claim lease, a retry after a failed
// commit, a restart — and both evaluations derive the SAME event ids, because the identity is
// the pair (handoff, monitor). That collision is the mechanism working: it is what stops the
// alert being delivered twice. But inside a transaction a unique violation ABORTS everything,
// so the duplicate cannot be recognised after the fact the way the standalone insert
// recognises it. It has to be removed first.
//
// # Why existence is compared on the TYPE AND AGGREGATE and not on the bytes
//
// The standalone path discriminates a benign duplicate from a genuine id reuse by comparing
// event_raw byte for byte. That test is unavailable here: event_raw carries occurred_at, which
// is minted when the row is prepared, so a re-evaluation of one handoff produces the same id
// with different bytes and a byte comparison would call it a collision.
//
// event_type and aggregate_id ARE stable across re-evaluations, and they are enough to keep a
// real collision loud: a stored row under this id describing a different event type or a
// different aggregate is NOT this alert, so it is left in the list, the insert conflicts, and
// the completion fails visibly instead of discarding an event.
//
// # The residual race, and why it converges
//
// Two processors can pass this filter simultaneously and then collide on the insert. The loser's
// transaction aborts, its handoff returns to pending, and the retry's filter finds the row and
// excludes it. No alert is lost and none is duplicated; one evaluation is simply repeated.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//   - tx *sql.Tx: the completion's transaction, so the read shares its snapshot.
//   - events []*model.EventOutbox: the alerts the evaluation produced.
//
// Returns:
//   - []*model.EventOutbox: the alerts not yet stored, in input order.
//   - error: the wrapped query error.
func filterAlreadyStoredMonitorAlerts(ctx context.Context, tx *sql.Tx, events []*model.EventOutbox) ([]*model.EventOutbox, error) {
	if len(events) == 0 {
		return events, nil
	}

	ids := make([]string, 0, len(events))
	for _, event := range events {
		if event != nil {
			ids = append(ids, event.EventID)
		}
	}

	if len(ids) == 0 {
		return events, nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, event_type, aggregate_id
		FROM blnk.event_outbox
		WHERE event_id = ANY($1)
	`, pq.Array(ids))
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check for already-stored alerts", err)
	}
	defer func() { _ = rows.Close() }()

	type storedAlert struct {
		eventType   string
		aggregateID string
	}
	stored := make(map[string]storedAlert, len(ids))
	for rows.Next() {
		var eventID string
		var alert storedAlert
		if err := rows.Scan(&eventID, &alert.eventType, &alert.aggregateID); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read already-stored alerts", err)
		}
		stored[eventID] = alert
	}
	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read already-stored alerts", err)
	}

	if len(stored) == 0 {
		return events, nil
	}

	fresh := make([]*model.EventOutbox, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}

		if existing, ok := stored[event.EventID]; ok &&
			existing.eventType == event.EventType && existing.aggregateID == event.AggregateID {
			logrus.WithFields(logrus.Fields{
				"event_id":   event.EventID,
				"event_type": event.EventType,
			}).Info(
				"the balance monitor alert is already durable, so this evaluation is a repeat; " +
					"the handoff is completed without recording a second copy",
			)

			continue
		}

		fresh = append(fresh, event)
	}

	return fresh, nil
}

// MarkBalanceMonitorHandoffFailed records an evaluation failure against a handoff.
//
// The row returns to pending while attempts remain, so the next poll re-claims it, and
// is marked failed once the budget is spent. That mirrors how both existing outboxes
// treat a failed unit of work, and it means an exhausted handoff stays in the table as
// the record of an evaluation that never happened rather than disappearing.
//
// The claim already incremented attempts, so this does not increment it again —
// counting a failure twice would halve the effective budget.
//
// # Why permanent is a parameter rather than a guess
//
// Some failures cannot be retried into success. A snapshot that does not decode will not
// decode on the sixth attempt either: the bytes are fixed, and re-reading them costs five
// more claims, five more log lines and five more poll intervals to reach the conclusion
// the first attempt already had. The evaluator knows which kind of failure it hit and this
// layer does not, so it says. A retryable failure passes false and keeps the budget.
//
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - handoffID string: the handoff that failed.
//   - reason string: the failure detail, stored verbatim in last_error.
//   - permanent bool: true when no further attempt can succeed, which fails the row
//     immediately instead of spending the remaining budget on a known-lost cause.
//
// Returns:
//   - error: the wrapped statement error.
func (d Datasource) MarkBalanceMonitorHandoffFailed(ctx context.Context, handoffID, reason string, permanent bool) error {
	trimmed := strings.TrimSpace(handoffID)
	if trimmed == "" {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to record the balance monitor handoff failure",
			errors.New("a handoff id is required"),
		)
	}

	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.balance_monitor_handoff
		SET status = CASE WHEN $6 OR attempts >= max_attempts THEN $1 ELSE $2 END,
		    processed_at = CASE WHEN $6 OR attempts >= max_attempts THEN NOW() ELSE NULL END,
		    locked_until = NULL,
		    last_error = $3
		WHERE handoff_id = $4
		  AND status <> $5
	`, model.OutboxStatusFailed, model.OutboxStatusPending, reason, trimmed, model.OutboxStatusCompleted, permanent)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record the balance monitor handoff failure", err)
	}

	return nil
}

// CountBalanceMonitorHandoffByStatus returns the number of handoffs in each status.
//
// It answers the operational question the event outbox cannot: whether a balance
// movement's monitors were evaluated at all. A non-zero failed count means alerts were
// never judged, which is a materially different fact from an alert that was judged and
// did not fire.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//
// Returns:
//   - map[string]int64: status to count, containing only the statuses present.
//   - error: the wrapped query error.
func (d Datasource) CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT status, COUNT(*) FROM blnk.balance_monitor_handoff GROUP BY status
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to count balance monitor handoffs", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[string]int64, 4)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read balance monitor handoff counts", err)
		}
		counts[status] = count
	}

	if err := rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read balance monitor handoff counts", err)
	}

	return counts, nil
}

// scanBalanceMonitorHandoff reads one row of balanceMonitorHandoffColumns.
//
// The nullable columns are read through sql.Null* and flattened, so the caller never
// has to distinguish "SQL NULL" from "zero" for a field where the two mean the same
// thing.
//
// Parameters:
//   - rows *sql.Rows: positioned on a row.
//
// Returns:
//   - *model.BalanceMonitorHandoff: the decoded row.
//   - error: the wrapped scan error.
func scanBalanceMonitorHandoff(rows *sql.Rows) (*model.BalanceMonitorHandoff, error) {
	handoff := &model.BalanceMonitorHandoff{}
	var ledgerID, lastError sql.NullString
	var snapshot, monitorSnapshot []byte
	var processedAt, lockedUntil sql.NullTime

	if err := rows.Scan(
		&handoff.ID,
		&handoff.HandoffID,
		&handoff.BalanceID,
		&ledgerID,
		&snapshot,
		// NULL on a row written before sql/1781252100.sql, which scans into a nil slice and
		// is what model.BalanceMonitorHandoff.Monitors reports as "no snapshot carried".
		&monitorSnapshot,
		&handoff.Status,
		&handoff.Attempts,
		&handoff.MaxAttempts,
		&lastError,
		&handoff.EventsCaptured,
		&handoff.CreatedAt,
		&processedAt,
		&lockedUntil,
	); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read a balance monitor handoff", err)
	}

	handoff.LedgerID = ledgerID.String
	handoff.LastError = lastError.String
	handoff.BalanceSnapshot = append([]byte(nil), snapshot...)
	// Left NIL when the column was NULL, rather than an empty non-nil slice: Monitors
	// distinguishes "no snapshot carried" from "a snapshot that decoded to nothing", and a
	// zero-length non-nil slice would make a legacy row indistinguishable from a new one.
	if monitorSnapshot != nil {
		handoff.MonitorSnapshot = append([]byte(nil), monitorSnapshot...)
	}
	if processedAt.Valid {
		at := processedAt.Time
		handoff.ProcessedAt = &at
	}
	if lockedUntil.Valid {
		at := lockedUntil.Time
		handoff.LockedUntil = &at
	}

	return handoff, nil
}

// InsertBulkTransactionBatch records that a bulk batch has begun.
//
// It is written BEFORE any member transaction runs, and that ordering is the point: the
// batch is durable and enumerable from the moment it starts, so a process that dies
// half way through leaves a row saying "this batch began and never reported an outcome"
// rather than leaving nothing at all.
//
// The insert is idempotent on the primary key. A batch id is minted fresh per request,
// so a conflict means the same batch is being started twice — a retry of the enclosing
// request — and the correct answer is to accept the existing row rather than to fail a
// batch because its coordinator was already recorded.
//
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - batch *model.BulkTransactionBatch: the coordinator row. Status is forced to
//     processing and finalized_at left NULL, because a batch cannot be inserted
//     already-finished.
//
// Returns:
//   - error: the wrapped statement error.
func (d Datasource) InsertBulkTransactionBatch(ctx context.Context, batch *model.BulkTransactionBatch) error {
	if batch == nil || strings.TrimSpace(batch.BatchID) == "" {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to record the bulk transaction batch",
			errors.New("a batch id is required"),
		)
	}

	_, err := d.Conn.ExecContext(ctx, `
		INSERT INTO blnk.bulk_transaction_batches
		(batch_id, status, transaction_count, atomic, inflight)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (batch_id) DO NOTHING
	`,
		strings.TrimSpace(batch.BatchID),
		model.BulkBatchStatusProcessing,
		batch.TransactionCount,
		batch.Atomic,
		batch.Inflight,
	)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record the bulk transaction batch", err)
	}

	return nil
}

// FinalizeBulkTransactionBatchWithEvent moves a batch to its terminal outcome AND
// inserts the outcome event, in ONE database transaction.
//
// # The guarantee, stated as the two states it makes unreachable
//
//   - a terminal coordinator row whose outcome event was never written, and
//   - an outcome event describing a batch the coordinator still calls in-progress.
//
// Both are reachable when the outcome and the event are written separately, and the
// first is exactly what used to happen: the outcome was computed, the event insert
// failed, and the summary was gone with only a log line to show for it.
//
// # Why the UPDATE is guarded on the row being non-terminal
//
// The guard is what makes the whole finalise idempotent, and idempotence is required
// because this is retried. A first attempt whose COMMIT succeeded but whose
// acknowledgement was lost must not be followed by a second, differently-identified
// event for one batch outcome. On the retry the guard matches nothing, this function
// reports the already-recorded outcome, and the caller stops.
//
// An already-terminal row is therefore reported as SUCCESS rather than as a conflict —
// but only when it agrees. A row already terminal with a DIFFERENT status is a genuine
// conflict: something else declared a different outcome for this batch, and silently
// discarding either answer would be worse than reporting it.
//
// Parameters:
//   - ctx context.Context: the context for the transaction.
//   - batchID string: the batch being finalised.
//   - outcome *model.BulkTransactionBatch: the terminal status, error message and
//     transaction count to record.
//   - event *model.EventOutbox: the prepared outcome event. Required — a finalise with
//     no event would defeat the atomicity this function exists for.
//
// Returns:
//   - bool: true when this call performed the transition, false when it found the
//     batch already finalised with the same outcome.
//   - error: the wrapped failure, including the conflicting-outcome case.
func (d Datasource) FinalizeBulkTransactionBatchWithEvent(
	ctx context.Context,
	batchID string,
	outcome *model.BulkTransactionBatch,
	event *model.EventOutbox,
) (bool, error) {
	ctx, span := otel.Tracer("database.bulk_transaction_batches").Start(ctx, "FinalizeBulkTransactionBatchWithEvent")
	defer span.End()

	trimmed := strings.TrimSpace(batchID)
	if trimmed == "" || outcome == nil || event == nil {
		return false, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to finalize the bulk transaction batch",
			errors.New("a batch id, an outcome and a prepared outcome event are all required"),
		)
	}

	if !model.IsTerminalBulkBatchStatus(outcome.Status) {
		return false, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to finalize the bulk transaction batch",
			fmt.Errorf("%q is not a terminal batch outcome", outcome.Status),
		)
	}

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	errorMessage := interface{}(nil)
	if strings.TrimSpace(outcome.ErrorMessage) != "" {
		errorMessage = outcome.ErrorMessage
	}

	var finalizedStatus string
	err = tx.QueryRowContext(ctx, `
		UPDATE blnk.bulk_transaction_batches
		SET status = $1,
		    transaction_count = $2,
		    error_message = $3,
		    event_id = $4,
		    finalized_at = NOW()
		WHERE batch_id = $5
		  AND status = $6
		RETURNING status
	`,
		outcome.Status,
		outcome.TransactionCount,
		errorMessage,
		event.EventID,
		trimmed,
		model.BulkBatchStatusProcessing,
	).Scan(&finalizedStatus)

	if errors.Is(err, sql.ErrNoRows) {
		// Either the batch is already terminal, or it was never recorded. Both answers
		// come from one read so the two cannot be confused, and neither is decided by a
		// second round trip that could observe a different instant.
		//
		// A MISSING ROW IS ADOPTED RATHER THAN REFUSED, and that is what removes the last
		// standalone capture from a producer that has state to be atomic with. It used to
		// be reported as ErrNotFound, the caller fell back to a single-shot insert outside
		// any transaction, and the batch summary was then at-most-once for the whole life
		// of a deployment whose coordinator write had failed once at batch start. Writing
		// the coordinator row here, in its terminal state, inside THIS transaction gives
		// the event a mutation to commit with after all: the outcome record and its event
		// are inserted together or not at all.
		//
		// AN ADOPTION FALLS THROUGH to the event insert and the commit below rather than
		// returning: the whole point is that both rows share one commit, so returning here
		// would leave the adopted row inside a transaction the deferred rollback discards.
		adopted, resolveErr := d.adoptOrExplainUnfinalizableBulkBatch(ctx, tx, trimmed, outcome, event)
		if resolveErr != nil || !adopted {
			return false, resolveErr
		}

		finalizedStatus = outcome.Status
	} else if err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to finalize the bulk transaction batch", err)
	}

	if err := insertEventOutboxesInTx(ctx, tx, []*model.EventOutbox{event}); err != nil {
		span.RecordError(err)
		return false, err
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit the bulk transaction batch outcome", err)
	}

	span.SetAttributes(
		attribute.String("batch.id", trimmed),
		attribute.String("batch.status", finalizedStatus),
		attribute.String("event.id", event.EventID),
	)

	return true, nil
}

// adoptOrExplainUnfinalizableBulkBatch resolves a finalise that matched no row, either by
// adopting the batch or by explaining why it cannot be finalised.
//
// It runs inside the caller's still-open transaction so the row it reads is the row the
// UPDATE failed to match, rather than whatever a later connection would see — and, in the
// adoption case, so the row it writes commits with the event the caller is inserting.
//
// Three answers are possible and they are genuinely different:
//
//   - no row at all: the coordinator write at batch start did not happen. The batch is
//     ADOPTED — its coordinator row is inserted here, already terminal, and the caller's
//     event insert and commit then proceed exactly as they would have. This is the case
//     that used to be reported as ErrNotFound and to send the caller down a standalone,
//     at-most-once capture; requirement R-2 asks for the event to share a transaction with
//     the state it describes, and this is the transaction that records that state.
//   - a terminal row with the SAME outcome: the finalise already happened. Reported as
//     success so a retry after a lost acknowledgement stops instead of recording a
//     second event for one outcome.
//   - a terminal row with a DIFFERENT outcome: two answers exist for one batch. Neither
//     is discarded silently.
//
// A non-terminal row cannot reach here: the guarded UPDATE would have matched it.
//
// # Why adoption is safe rather than a way to invent history
//
// The row it writes is not a guess. Every column comes from the outcome the caller
// computed from the batch it just ran — the same values the start-of-batch row would have
// carried, plus the terminal status the batch actually reached — so the adopted row is
// indistinguishable from one written at batch start and finalised normally, except that
// its start timestamp is the adoption instant. It is written with ON CONFLICT DO NOTHING
// and its effect re-checked, so a concurrent finaliser that inserted first is detected
// rather than overwritten, and the conflicting-outcome answer above is what that
// concurrent case gets.
//
// Parameters:
//   - ctx context.Context: the context for the statements.
//   - tx *sql.Tx: the caller's open transaction. Both the read and any adoption insert
//     run inside it.
//   - batchID string: the trimmed batch id.
//   - outcome *model.BulkTransactionBatch: the terminal outcome the caller is recording.
//   - event *model.EventOutbox: the prepared outcome event, whose id the adopted row
//     records so the outcome and its event stay joinable.
//
// Returns:
//   - bool: true when this call recorded the outcome — including by adoption — and false
//     when it found the batch already finalised with the same outcome. The caller commits
//     in both cases, so an adoption is reported as work performed.
//   - error: nil when the outcome is recorded or already present with the same value.
func (d Datasource) adoptOrExplainUnfinalizableBulkBatch(
	ctx context.Context,
	tx *sql.Tx,
	batchID string,
	outcome *model.BulkTransactionBatch,
	event *model.EventOutbox,
) (bool, error) {
	var stored string
	err := tx.QueryRowContext(ctx, `
		SELECT status FROM blnk.bulk_transaction_batches WHERE batch_id = $1
	`, batchID).Scan(&stored)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return d.adoptBulkTransactionBatch(ctx, tx, batchID, outcome, event)
	case err != nil:
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read the bulk transaction batch", err)
	case stored == outcome.Status:
		return false, nil
	default:
		return false, apierror.NewAPIError(
			apierror.ErrConflict,
			"The bulk transaction batch already reported a different outcome",
			fmt.Errorf("batch %q is recorded as %q and cannot be finalized as %q", batchID, stored, outcome.Status),
		)
	}
}

// adoptBulkTransactionBatch inserts an already-terminal coordinator row for a batch whose
// start was never recorded.
//
// Called only from adoptOrExplainUnfinalizableBulkBatch, and only when the read inside the
// caller's transaction found no row at all. See that function for why the values are the
// caller's computed outcome rather than a reconstruction.
//
// ON CONFLICT DO NOTHING, with the effect re-checked, because two finalisers can race for
// one batch: whichever loses must not overwrite the winner's outcome, and must be told
// whether the stored answer agrees with its own.
//
// Parameters:
//   - ctx context.Context: the context for the statements.
//   - tx *sql.Tx: the caller's open transaction.
//   - batchID string: the trimmed batch id.
//   - outcome *model.BulkTransactionBatch: the terminal outcome being adopted.
//   - event *model.EventOutbox: the prepared outcome event, recorded on the row.
//
// Returns:
//   - bool: true when the adoption inserted the row.
//   - error: the wrapped statement error, or a conflict when a concurrent finaliser
//     inserted a different outcome first.
func (d Datasource) adoptBulkTransactionBatch(
	ctx context.Context,
	tx *sql.Tx,
	batchID string,
	outcome *model.BulkTransactionBatch,
	event *model.EventOutbox,
) (bool, error) {
	errorMessage := interface{}(nil)
	if strings.TrimSpace(outcome.ErrorMessage) != "" {
		errorMessage = outcome.ErrorMessage
	}

	var adoptedStatus string
	err := tx.QueryRowContext(ctx, `
		INSERT INTO blnk.bulk_transaction_batches
		(batch_id, status, transaction_count, atomic, inflight, error_message, event_id, finalized_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (batch_id) DO NOTHING
		RETURNING status
	`,
		batchID,
		outcome.Status,
		outcome.TransactionCount,
		outcome.Atomic,
		outcome.Inflight,
		errorMessage,
		event.EventID,
	).Scan(&adoptedStatus)

	if errors.Is(err, sql.ErrNoRows) {
		// A concurrent finaliser inserted first. Its outcome decides, and the caller is
		// told whether that outcome is the one it was recording.
		return false, d.reconcileAdoptedBulkBatch(ctx, tx, batchID, outcome.Status)
	}
	if err != nil {
		return false, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to record the bulk transaction batch outcome",
			fmt.Errorf("adopting the unrecorded batch %q: %w", batchID, err),
		)
	}

	logrus.WithFields(logrus.Fields{
		"batch_id": batchID,
		"status":   adoptedStatus,
		"event_id": event.EventID,
	}).Warn(
		"the bulk transaction batch had no coordinator record, so its terminal row is being written " +
			"in the same transaction as its outcome event; the summary stays atomic with the outcome, " +
			"but the batch was not enumerable while it ran — investigate why the start-of-batch write failed",
	)

	return true, nil
}

// reconcileAdoptedBulkBatch reports whether the row a concurrent finaliser inserted agrees
// with the outcome this caller was adopting.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//   - tx *sql.Tx: the caller's open transaction.
//   - batchID string: the trimmed batch id.
//   - attempted string: the outcome this caller was recording.
//
// Returns:
//   - error: nil when the stored outcome matches, a conflict when it does not, and the
//     wrapped read error when the row cannot be read at all.
func (d Datasource) reconcileAdoptedBulkBatch(ctx context.Context, tx *sql.Tx, batchID, attempted string) error {
	var stored string
	if err := tx.QueryRowContext(ctx, `
		SELECT status FROM blnk.bulk_transaction_batches WHERE batch_id = $1
	`, batchID).Scan(&stored); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to read the bulk transaction batch", err)
	}

	if stored == attempted {
		return nil
	}

	return apierror.NewAPIError(
		apierror.ErrConflict,
		"The bulk transaction batch already reported a different outcome",
		fmt.Errorf("batch %q is recorded as %q and cannot be finalized as %q", batchID, stored, attempted),
	)
}

// CountUnfinalizedBulkTransactionBatches counts batches that began and never reported
// an outcome, and returns the oldest one's start time.
//
// This is the one window the coordinator cannot close: a process that dies before the
// finalising transaction leaves its batch here. Counting it is what makes that residue
// a visible operational fact instead of an absence nobody can query — and the age is
// what distinguishes a batch still legitimately running from one that was abandoned,
// since a large batch may take minutes by design.
//
// Parameters:
//   - ctx context.Context: the context for the query.
//   - olderThan time.Duration: ignore batches younger than this, so batches still in
//     flight are not reported as stuck. Zero counts every non-terminal batch.
//
// Returns:
//   - int64: how many batches are outstanding beyond the grace period.
//   - *time.Time: when the oldest of them began, nil when there are none.
//   - error: the wrapped query error.
func (d Datasource) CountUnfinalizedBulkTransactionBatches(ctx context.Context, olderThan time.Duration) (int64, *time.Time, error) {
	if olderThan < 0 {
		olderThan = 0
	}

	var count int64
	var oldest sql.NullTime
	err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(created_at)
		FROM blnk.bulk_transaction_batches
		WHERE status = $1
		  AND created_at < NOW() - $2::interval
	`, model.BulkBatchStatusProcessing, olderThan.String()).Scan(&count, &oldest)
	if err != nil {
		return 0, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to count unfinalized bulk transaction batches", err)
	}

	if !oldest.Valid {
		return count, nil, nil
	}

	at := oldest.Time

	return count, &at, nil
}
