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

// event_producer_atomicity.go is the repository implementation for the last two event
// families to be brought under the requirement: balance.monitor and
// bulk_transaction.<status>.
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

// insertBalanceMonitorHandoffsInTx records, inside the caller's transaction, the
// pending evaluation of every supplied balance's monitors — together with both inputs
// that evaluation depends on.
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

	// THE MONITOR DEFINITIONS, READ IN THIS TRANSACTION. This is the half that was
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

// captureBalanceMonitorAlertsInTx evaluates the monitors of the supplied balances
// inside the caller's transaction and inserts the CANONICAL balance.monitor event rows
// for the crossings it decides.
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
func recordBalanceMonitorEvaluation(ctx context.Context, d Datasource, tx *sql.Tx, span trace.Span, balances []*model.Balance, capturedEvents []*model.EventOutbox) error {
	cnf, err := config.Fetch()
	if err != nil || !cnf.EventPublishingConfigured() {
		return nil
	}

	evaluated := balancesAlreadyEvaluatedInTx(capturedEvents)
	awaiting := balancesAwaitingMonitorEvaluation(balances, evaluated)
	if len(awaiting) == 0 {
		// Every moved balance was evaluated with the mutation, so there is nothing left to
		// evaluate here. Recorded on the span rather than passed silently: "nothing to do"
		// and "suppressed because the caller covered every balance" are different facts, and
		// only one of them means the alerts are already durable.
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
// Parameters:
//   - ctx context.Context: the context for the statement.
//   - batch *model.BulkTransactionBatch: the coordinator row.
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
// Parameters:
//   - ctx context.Context: the context for the transaction.
//   - batchID string: the batch being finalised.
//   - outcome *model.BulkTransactionBatch: the terminal status, error message and
//     transaction count to record.
//   - event *model.EventOutbox: the prepared outcome event. Required — a finalise with
//     no event would defeat the atomicity this function exists for.
//
// Returns:
//   - bool: true when this call performed the transition, false when it found the batch
//     already finalised with the same outcome.
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
		// Either the batch is already terminal, or it was never recorded. Both answers come
		// from one read so the two cannot be confused, and neither is decided by a second
		// round trip that could observe a different instant.
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

// adoptOrExplainUnfinalizableBulkBatch resolves a finalise that matched no row, either
// by adopting the batch or by explaining why it cannot be finalised.
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

// adoptBulkTransactionBatch inserts an already-terminal coordinator row for a batch
// whose start was never recorded.
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

// reconcileAdoptedBulkBatch reports whether the row a concurrent finaliser inserted
// agrees with the outcome this caller was adopting.
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
// Parameters:
//   - ctx context.Context: the context for the query.
//   - olderThan time.Duration: ignore batches younger than this, so batches still in
//     flight are not reported as stuck.
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
