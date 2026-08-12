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

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// resolveBatchEventOutboxes returns the event rows the batch writer must insert inside its
// transaction: the caller's rows when it supplied any, otherwise rows derived from the
// registered transaction event capture.
func resolveBatchEventOutboxes(
	ctx context.Context,
	txns []*model.Transaction,
	balances []*model.Balance,
	supplied []*model.EventOutbox,
) ([]*model.EventOutbox, error) {
	rows, err := resolveEventOutboxes(len(txns), supplied)
	if err != nil || len(rows) > 0 {
		return rows, err
	}

	return deriveBatchEventOutboxes(ctx, txns, balances)
}

// deriveBatchEventOutboxes builds one event row per transaction using the registered
// capture.
func deriveBatchEventOutboxes(
	ctx context.Context,
	txns []*model.Transaction,
	balances []*model.Balance,
) ([]*model.EventOutbox, error) {
	capture := registeredTransactionEventCapture()
	if capture == nil || len(txns) == 0 {
		return nil, nil
	}

	ledgers := ledgerIDsByBalanceID(balances)

	rows := make([]*model.EventOutbox, 0, len(txns))
	captured, skipped := 0, 0

	for _, txn := range txns {
		if txn == nil {
			continue
		}

		row, err := capture(ctx, txn, transactionLedgerIDFromSet(txn, ledgers))
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer,
				"Failed to capture the ledger event for a batched transaction",
				fmt.Errorf("blnk: capturing the event for transaction %q: %w", txn.TransactionID, err))
		}

		if row == nil {
			skipped++

			continue
		}

		captured++
		rows = append(rows, row)
	}

	if captured == 0 {
		return nil, nil
	}

	if skipped > 0 {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer,
			"Failed to capture the ledger event for every batched transaction",
			fmt.Errorf("blnk: %d of %d batched transactions produced no event row",
				skipped, captured+skipped))
	}

	return rows, nil
}

// resolveEventOutboxes settles which event rows an atomic writer will insert, and enforces
// the transaction-to-event cardinality.
func resolveEventOutboxes(txnCount int, supplied []*model.EventOutbox) ([]*model.EventOutbox, error) {
	present := make([]*model.EventOutbox, 0, len(supplied))
	for _, row := range supplied {
		if row != nil {
			present = append(present, row)
		}
	}

	if len(present) == 0 {
		return nil, nil
	}

	// Counted over the rows that DESCRIBE a mutation. A repeatable event —
	// balance.monitor, system.error — describes a condition rather than the mutation it
	// travels with, so it rides along without being counted. See the note above.
	describing := 0
	for _, row := range present {
		if !model.EventTypeIsRepeatable(row.EventType) {
			describing++
		}
	}

	// The equality is required only when the writer is actually committing transactions. A
	// call carrying event rows and NO transactions is the batch-level case — one event
	// describing a whole operation rather than one event per ledger mutation — and refusing it
	// here would make that event unrepresentable.
	if txnCount > 0 && describing != txnCount {
		return nil, apierror.NewAPIError(apierror.ErrBadRequest,
			"Each committed transaction must carry exactly one event",
			fmt.Errorf("blnk: %d mutation-describing event rows supplied for %d transactions (%d rows in total)",
				describing, txnCount, len(present)))
	}

	return present, nil
}

// utcOrNil normalizes an optional timestamp to UTC so the naive value stored
// in timestamp-without-time-zone columns is timezone-independent.
func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// RecordTransaction persists one transaction row, optionally together with the event that
// describes it.
//
// Parameters:
//   - ctx: The context for the operation.
//   - txn: The transaction to record.
//   - eventOutbox: Optional. The prepared event row to commit with the transaction.
//
// Returns:
//   - *model.Transaction: The recorded transaction.
//   - error: A typed error if the marshal, the insert, the event insert or the commit
//     fails.
func (d Datasource) RecordTransaction(ctx context.Context, txn *model.Transaction, eventOutbox ...*model.EventOutbox) (*model.Transaction, error) {
	// Start a new tracing span for the database operation
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PersistTransaction")
	defer span.End()

	eventRows, err := resolveEventOutboxes(1, eventOutbox)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if len(eventRows) > 0 {
		return d.recordTransactionWithEvents(ctx, span, txn, eventRows)
	}

	// Marshal transaction metadata into JSON format
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err) // Record the error in the tracing span
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Execute the SQL insert statement to record the transaction
	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.AmountString, txn.PreciseAmount.String(), txn.Precision, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt.UTC(), metaDataJSON, txn.ScheduledFor.UTC(), txn.Hash, utcOrNil(txn.EffectiveDate),
	)
	// Handle errors that may occur during the execution of the query
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	// Log the successful transaction recording as an event in the tracing span
	span.AddEvent("Transaction recorded", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
	))

	return txn, nil
}

// recordTransactionWithEvents inserts one transaction row and its event rows inside a
// single transaction.
func (d Datasource) recordTransactionWithEvents(ctx context.Context, span trace.Span, txn *model.Transaction, eventRows []*model.EventOutbox) (*model.Transaction, error) {
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	for _, e := range eventRows {
		if err := d.InsertEventOutboxInTx(ctx, tx, e); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to insert event outbox: %w", err)
		}
		span.AddEvent("Event outbox entry inserted", trace.WithAttributes(
			attribute.String("event.id", e.EventID),
			attribute.String("event.type", e.EventType),
			attribute.String("event.topic", e.Topic),
		))
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction and event recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("transaction.reference", txn.Reference),
		attribute.Int("event_outbox.count", len(eventRows)),
	))

	return txn, nil
}

// recordTransactionInTx inserts a transaction record within an existing database
// transaction. This is a helper function used by RecordTransactionWithBalances for atomic
// operations. RecordTransactionWithEvent records ONE transaction and its event in ONE
// transaction.
//
// Parameters:
//   - ctx context.Context: cancels the transaction.
//   - txn *model.Transaction: the transaction to record.
//   - event *model.EventOutbox: the prepared event row, or nil.
//
// Returns:
//   - *model.Transaction: the recorded transaction, unchanged from the argument.
//   - error: the insert, event-capture or commit failure. On any error NOTHING was
//     committed — neither the transaction nor the event.
func (d Datasource) RecordTransactionWithEvent(ctx context.Context, txn *model.Transaction, event *model.EventOutbox) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PersistTransactionWithEvent")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if event != nil {
		if err := d.InsertEventOutboxInTx(ctx, tx, event); err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction and event recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.Bool("event.captured", event != nil),
	))

	return txn, nil
}

func recordTransactionInTx(ctx context.Context, tx *sql.Tx, txn *model.Transaction) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "recordTransactionInTx")
	defer span.End()

	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data, scheduled_for, hash, effective_date)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.AmountString, txn.PreciseAmount.String(), txn.Precision, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt.UTC(), metaDataJSON, txn.ScheduledFor.UTC(), txn.Hash, utcOrNil(txn.EffectiveDate),
	)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	span.AddEvent("Transaction recorded in transaction", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
	))

	return nil
}

// recordTransactionsInTx inserts multiple transaction records within an existing database
// transaction using PostgreSQL's COPY protocol to reduce SQL parsing and roundtrips.
func recordTransactionsInTx(ctx context.Context, tx *sql.Tx, txns []*model.Transaction) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "recordTransactionsInTx")
	defer span.End()

	if len(txns) == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, pq.CopyInSchema(
		"blnk",
		"transactions",
		"transaction_id",
		"parent_transaction",
		"source",
		"reference",
		"amount",
		"precise_amount",
		"precision",
		"currency",
		"destination",
		"description",
		"status",
		"created_at",
		"meta_data",
		"scheduled_for",
		"hash",
		"effective_date",
	))
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to prepare transaction copy", err)
	}

	defer func() {
		_ = stmt.Close()
	}()

	for _, txn := range txns {
		metaDataJSON, err := json.Marshal(txn.MetaData)
		if err != nil {
			span.RecordError(err)
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
		}

		if _, err := stmt.ExecContext(ctx,
			txn.TransactionID,
			txn.ParentTransaction,
			txn.Source,
			txn.Reference,
			txn.AmountString,
			txn.PreciseAmount.String(),
			txn.Precision,
			txn.Currency,
			txn.Destination,
			txn.Description,
			txn.Status,
			txn.CreatedAt.UTC(),
			string(metaDataJSON),
			txn.ScheduledFor.UTC(),
			txn.Hash,
			utcOrNil(txn.EffectiveDate),
		); err != nil {
			span.RecordError(err)
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to stream transaction copy row", err)
		}
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to flush transaction copy", err)
	}

	if err := stmt.Close(); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to finalize transaction copy", err)
	}

	span.AddEvent("Transactions recorded in transaction", trace.WithAttributes(
		attribute.Int("transaction.count", len(txns)),
	))

	return nil
}

// RecordTransactionWithBalances atomically records a transaction and updates both source and destination balances
// within a single database transaction. This ensures that either all operations succeed together,
// or none of them are committed, preventing inconsistent ledger states.
//
// Parameters:
// - ctx: Context for managing the request and tracing.
// - txn: The transaction object containing details to be recorded.
// - sourceBalance: The source balance to be updated.
// - destinationBalance: The destination balance to be updated.
//
// Returns:
// - The recorded transaction if successful, or an error if any operation fails.
func (d Datasource) RecordTransactionWithBalances(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionWithBalances")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction and balances recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID),
	))

	return txn, nil
}

// RecordTransactionWithBalancesAndOutbox atomically records a transaction, updates
// balances, and optionally inserts a lineage outbox entry and event outbox entries within a
// single database transaction. This ensures that the lineage processing intent is captured
// atomically with the main transaction, guaranteeing no lineage work is lost even if
// subsequent async operations fail. The event outbox rows are captured under the same
// guarantee, which is what makes a published event and the ledger mutation it describes
// inseparable: both are written by this one transaction, so a rollback takes the event with
// it and a commit can never leave the event behind.
//
// Parameters:
//   - ctx: Context for managing the request and tracing.
//   - txn: The transaction object containing details to be recorded.
//   - sourceBalance: The source balance to be updated.
//   - destinationBalance: The destination balance to be updated.
//   - outbox: Optional lineage outbox entry to insert atomically (can be nil if no lineage
//     processing needed).
//   - eventOutbox: Optional event outbox entries to insert atomically.
//
// Returns:
// - The recorded transaction if successful, or an error if any operation fails.
func (d Datasource) RecordTransactionWithBalancesAndOutbox(ctx context.Context, txn *model.Transaction, sourceBalance, destinationBalance *model.Balance, outbox *model.LineageOutbox, eventOutbox ...*model.EventOutbox) (*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionWithBalancesAndOutbox")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionInTx(ctx, tx, txn); err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Insert lineage outbox entry atomically if provided
	if outbox != nil {
		if err := d.InsertLineageOutboxInTx(ctx, tx, outbox); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to insert lineage outbox: %w", err)
		}
		span.AddEvent("Lineage outbox entry inserted", trace.WithAttributes(
			attribute.String("outbox.transaction_id", outbox.TransactionID),
			attribute.String("outbox.lineage_type", outbox.LineageType),
		))
	}

	// Insert event outbox entries atomically, immediately after the lineage outbox and before
	// the commit. This placement is the whole mechanism behind the transactional-outbox
	// guarantee: the event rows share this transaction with the balance updates and the
	// transaction record above, so the mutation and its events commit or roll back together.
	eventRows, err := resolveEventOutboxes(1, eventOutbox)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	for _, e := range eventRows {
		if err := d.InsertEventOutboxInTx(ctx, tx, e); err != nil {
			span.RecordError(err)
			return nil, fmt.Errorf("failed to insert event outbox: %w", err)
		}
		span.AddEvent("Event outbox entry inserted", trace.WithAttributes(
			attribute.String("event.id", e.EventID),
			attribute.String("event.type", e.EventType),
			attribute.String("event.topic", e.Topic),
			attribute.String("event.ledger_id", e.LedgerID),
		))
	}

	// The balance-monitor alerts, and they go in HERE for the same reason the event rows do:
	// the judgement of a balance's monitors belongs to the transaction that moved the balance,
	// so a committed movement always carries its alerts and a rolled-back one carries none.
	if err := recordBalanceMonitorEvaluation(ctx, d, tx, span,
		[]*model.Balance{sourceBalance, destinationBalance}, eventRows); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transaction, balances, and outbox recorded atomically", trace.WithAttributes(
		attribute.String("transaction.id", txn.TransactionID),
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID),
		attribute.Bool("outbox.included", outbox != nil),
		attribute.Int("event_outbox.count", len(eventRows)),
	))

	return txn, nil
}

// RecordTransactionsWithBalancesAndOutboxes atomically records multiple transactions,
// updates the source and destination balances once, and inserts any lineage and event
// outbox entries in the same database transaction.
func (d Datasource) RecordTransactionsWithBalancesAndOutboxes(ctx context.Context, txns []*model.Transaction, sourceBalance, destinationBalance *model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error) {
	return d.RecordTransactionsWithBalanceSetAndOutboxes(ctx, txns, []*model.Balance{sourceBalance, destinationBalance}, outboxes, eventOutboxes...)
}

// RecordTransactionsWithBalanceSetAndOutboxes atomically records multiple transactions,
// updates all changed balances, and inserts any lineage and event outbox entries in the
// same database transaction.
func (d Datasource) RecordTransactionsWithBalanceSetAndOutboxes(ctx context.Context, txns []*model.Transaction, balances []*model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordTransactionsWithBalancesAndOutboxes")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalanceSet(ctx, tx, balances); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := recordTransactionsInTx(ctx, tx, txns); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := insertLineageOutboxesInTx(ctx, tx, outboxes); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to insert lineage outboxes: %w", err)
	}

	// Event outbox entries go in at the same point as in the single-transaction writer above —
	// after the lineage outbox, before the commit — so the batch's events share the fate of
	// the batch's balance updates.
	eventRows, err := resolveBatchEventOutboxes(ctx, txns, balances, eventOutboxes)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if err := insertEventOutboxesInTx(ctx, tx, eventRows); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to insert event outboxes: %w", err)
	}

	// The balance-monitor alerts for every balance this batch moved that was not already
	// evaluated with it. Deriving it from the balances the writer is already updating covers
	// both. The coalesced caller supplies no monitor alerts, so every balance here is
	// evaluated here.
	if err := recordBalanceMonitorEvaluation(ctx, d, tx, span, balances, eventRows); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	span.AddEvent("Transactions, balances, and outboxes recorded atomically", trace.WithAttributes(
		attribute.Int("transaction.count", len(txns)),
		attribute.Int("balance.count", len(balances)),
		attribute.Int("outbox.count", len(outboxes)),
		attribute.Int("event_outbox.count", len(eventRows)),
	))

	return txns, nil
}

// GetTransaction retrieves a transaction by its ID from the database.
