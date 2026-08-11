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
//
// # Why derivation belongs here (requirement R-2)
//
// R-2 is that a ledger mutation and the event announcing it commit together. The
// single-transaction path satisfies it by preparing the row before the write and handing it
// in. The COALESCED path cannot: it commits many transactions in one database transaction, and
// the file that assembles that call is frozen by the plan, so the rows can only appear from
// below. Without this, a coalesced batch committed balance updates and transaction rows while
// publishing nothing — durable money movement with no event, which is the exact failure R-2
// names.
//
// SUPPLIED ROWS ALWAYS WIN. A caller that prepared its own events is authoritative, and
// re-deriving over it would replace a row whose payload the caller may have shaped.
//
// # Failure is fatal, and that is the point
//
// A capture error abandons the whole write. The alternative — commit the mutation and log the
// capture failure — is the pre-R-2 behaviour: it produces exactly the silent, unrecoverable
// gap between money moved and event published that this function exists to close. A payload
// that will not serialise is a producer defect, not a transient condition, so failing the
// mutation surfaces it immediately instead of losing one event per batch indefinitely.
//
// Parameters:
//   - ctx context.Context: the writer's context, forwarded to the capture for tracing.
//   - txns []*model.Transaction: the transactions about to be committed.
//   - balances []*model.Balance: the balance set being updated, used to resolve each
//     transaction's ledger.
//   - supplied []*model.EventOutbox: the caller's event rows, usually empty on this path.
//
// Returns:
//   - []*model.EventOutbox: the rows to insert, nil when publishing is not configured.
//   - error: a cardinality mismatch in the supplied rows, or a capture failure.
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

// deriveBatchEventOutboxes builds one event row per transaction using the registered capture.
//
// # The all-or-nothing rule on nil rows
//
// A nil row means event publishing is not configured, which is a PROCESS-WIDE condition: the
// capture consults one configuration, so either every row is nil or none is. All-nil is
// therefore the ordinary unconfigured case and returns no rows at all, preserving the
// no-op-when-unconfigured contract inherited from SendWebhook.
//
// A MIXED result cannot happen for that reason and is refused rather than trimmed, because
// trimming would insert fewer events than transactions and hand the cardinality check a set it
// would then reject with a misleading message. Refusing here names the real problem.
//
// Parameters:
//   - ctx context.Context: forwarded to the capture.
//   - txns []*model.Transaction: the transactions about to be committed. Nils are skipped.
//   - balances []*model.Balance: the balance set, for ledger resolution.
//
// Returns:
//   - []*model.EventOutbox: one row per non-nil transaction, or nil when publishing is off or
//     no capture is registered.
//   - error: a capture failure, or a mixed nil/non-nil result.
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

// resolveEventOutboxes settles which event rows an atomic writer will insert, and
// enforces the transaction-to-event cardinality.
//
// Nil entries are dropped rather than rejected, matching insertLineageOutboxesInTx,
// so a producer that assembles its slice conditionally does not have to compact it.
// A caller that supplies NO row at all is accepted: PrepareEventOutbox returns nil
// when publishing is unconfigured, and that no-op-when-unconfigured contract is the
// one this pipeline inherited from SendWebhook. But a caller that supplies SOME rows
// must supply exactly one per committed transaction — a count mismatch means either a
// duplicate publication or a silently dropped event, and neither is recoverable once
// the mutation has committed.
//
// # The cardinality is counted over MUTATION-DESCRIBING rows, not over every row
//
// The invariant being protected is "a committed transaction carries exactly one event
// DESCRIBING IT". Some mutations legitimately commit alongside events that describe
// something else: a balance movement that crosses a monitor threshold commits its
// transaction event AND the balance.monitor alerts the movement triggered, in one
// transaction, because requirement R-2 admits no exception for an alert whose trigger
// is already durable.
//
// Those two classes are already distinguished, once, in model: an event that fires
// repeatedly for one subject is REPEATABLE (model.EventTypeIsRepeatable — balance.monitor
// and system.error), and everything else describes a mutation that happens once. Counting
// only the non-repeatable rows against txnCount therefore makes the check MORE precise
// rather than looser: two transaction events for one transaction is still refused, a batch
// of fifty that supplies forty-nine is still refused, and an alert riding along with its
// trigger is no longer misread as a duplicate publication.
//
// Deriving the class from the event type rather than from a flag on the row is deliberate.
// A flag would be a second statement of the same fact, settable independently, and the two
// would drift — at which point a genuine duplicate could be waved through by mislabelling
// it.
//
// Parameters:
//   - txnCount: the number of transactions being committed by this writer.
//   - supplied: the caller's rows, possibly empty or containing nils.
//
// Returns:
//   - []*model.EventOutbox: the rows to insert, never containing a nil.
//   - error: a typed bad request when the mutation-describing count does not match
//     txnCount.
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

	// The equality is required only when the writer is actually committing
	// transactions. A call carrying event rows and NO transactions is the batch-level
	// case — one event describing a whole operation rather than one event per ledger
	// mutation — and refusing it here would make that event unrepresentable.
	//
	// What the check does catch is the two mismatches that are silent and
	// unrecoverable once the mutation has committed: more mutation-describing rows than
	// transactions is a duplicate publication, and fewer is a batch that publishes one
	// event and loses the rest. Neither is detectable afterwards, because the
	// transaction rows are all there and the missing events exist nowhere to be counted.
	//
	// NOT caught, and deliberately so: supplying NO events at all. That is the
	// no-op-when-unconfigured contract this pipeline inherited from SendWebhook —
	// PrepareEventOutbox returns nil when publishing is off — so it cannot be
	// distinguished here from a producer that forgot to capture. Capture itself is
	// asserted at the producer call sites, which are the only place that knows an
	// event was due.
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
// # The variadic event tail, and what it is for (requirement R-2)
//
// Supplying an event row moves this method onto a TRANSACTION: the transaction row and the
// event row are inserted together and committed together, so a status mutation can no
// longer be durable while the event announcing it is lost. The rejection path is the caller
// that needs this — RejectTransaction records a REJECTED transaction through this method,
// and its transaction.rejected event used to be captured only after the insert had
// committed, which left exactly that window on the one path that exists to report a failure.
//
// Supplying nothing keeps the single-statement path unchanged, which is what the frozen
// transaction-queue callers in transaction_queue.go and transaction_inflight.go continue to
// use. The tail is variadic for that reason: a positional parameter would require editing
// files this change may not touch.
//
// Exactly one event per transaction is permitted; see resolveEventOutboxes.
//
// Parameters:
//   - ctx: The context for the operation.
//   - txn: The transaction to record.
//   - eventOutbox: Optional. The prepared event row to commit with the transaction.
//
// Returns:
//   - *model.Transaction: The recorded transaction.
//   - error: A typed error if the marshal, the insert, the event insert or the commit fails.
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
//
// It is the atomic arm of RecordTransaction and reuses recordTransactionInTx and
// InsertEventOutboxInTx rather than restating either statement, so this path and the
// balance-updating writers insert a transaction identically and an event identically.
//
// Parameters:
//   - ctx: The context for the operation.
//   - span: The caller's span, so the two paths report under one operation name.
//   - txn: The transaction to record.
//   - eventRows: The resolved event rows, never empty and never containing a nil.
//
// Returns:
//   - *model.Transaction: The recorded transaction.
//   - error: A typed error if the begin, either insert, or the commit fails. Nothing is
//     persisted in that case, which is the guarantee this arm exists to provide.
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

// recordTransactionInTx inserts a transaction record within an existing database transaction.
// This is a helper function used by RecordTransactionWithBalances for atomic operations.
// RecordTransactionWithEvent records ONE transaction and its event in ONE transaction.
//
// # Why it exists separately from the balance-bearing writers
//
// A REJECTED transaction moves no money: RejectTransaction persists the row with its
// rejection reason and no balance is touched, so the balance-bearing atomic writers below
// — which update two balances unconditionally — cannot serve it. Without this method the
// rejection path had no transaction to enrol its event in and captured transaction.rejected
// afterwards, from a post-commit goroutine. A crash or a cancelled context in that window
// left the rejection recorded and the event that tells subscribers about it non-existent,
// which is exactly the case a subscriber most needs: a payment that will never settle.
//
// The insert is the SAME recordTransactionInTx body every other writer uses, so there is no
// second copy of the transaction INSERT.
//
// A nil event records the transaction alone, which is the unconfigured deployment's correct
// behaviour rather than a special case.
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

// RecordTransactionWithBalancesAndOutbox atomically records a transaction, updates balances,
// and optionally inserts a lineage outbox entry and event outbox entries within a single
// database transaction.
// This ensures that the lineage processing intent is captured atomically with the main transaction,
// guaranteeing no lineage work is lost even if subsequent async operations fail. The event outbox
// rows are captured under the same guarantee, which is what makes a published event and the ledger
// mutation it describes inseparable: both are written by this one transaction, so a rollback takes
// the event with it and a commit can never leave the event behind.
//
// Parameters:
// - ctx: Context for managing the request and tracing.
// - txn: The transaction object containing details to be recorded.
// - sourceBalance: The source balance to be updated.
// - destinationBalance: The destination balance to be updated.
// - outbox: Optional lineage outbox entry to insert atomically (can be nil if no lineage processing needed).
// - eventOutbox: Optional event outbox entries to insert atomically. Variadic so
// every pre-existing caller stays source-compatible; omitting it means this
// mutation captures no Kafka event, exactly as a nil lineage outbox means it
// captures no lineage work. See the note on the declaration in repository.go for
// why this must not become a positional parameter.
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

	// Insert event outbox entries atomically, immediately after the lineage
	// outbox and before the commit. This placement is the whole mechanism behind
	// the transactional-outbox guarantee: the event rows share this transaction
	// with the balance updates and the transaction record above, so the mutation
	// and its events commit or roll back together. There is no window in which a
	// balance moved but its event was lost, and none in which an event describes a
	// mutation that was rolled back. Move this below tx.Commit() and the guarantee
	// is gone while every happy-path test still passes, which is exactly why it
	// sits here and not there.
	//
	// THE CARDINALITY IS ENFORCED RATHER THAN TOLERATED. Capture itself happens at
	// the producer call site, which is the only place that knows the event type and
	// the payload; what this layer owns is the invariant that a committed mutation
	// carries exactly one event. A caller supplying two rows for one transaction
	// publishes a duplicate, and one row for a batch of fifty silently loses
	// forty-nine — neither is detectable afterwards, because the mutation succeeds
	// and the missing events exist nowhere to be counted. See resolveEventOutboxes.
	//
	// This single-transaction writer inserts row by row through the exported
	// InsertEventOutboxInTx, deliberately mirroring the single-row lineage idiom
	// immediately above rather than borrowing the batch helper the bulk writer
	// uses: this path carries exactly one event per ledger mutation, and a
	// per-entry insert is what lets each event be traced individually.
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

	// The balance-monitor handoff, and it goes in HERE for the same reason the event
	// rows do: the judgement of a balance's monitors belongs to the transaction that
	// moved the balance, so a committed movement always carries its pending evaluation
	// and a rolled-back one carries none.
	//
	// What is written is not merely a marker to go and look later. The handoff carries BOTH
	// inputs the judgement depends on — the balance exactly as this transaction wrote it, and
	// the monitor definitions read inside this transaction — so which alerts exist, and what
	// each says, is decided here and cannot be changed by an edit to blnk.balance_monitors
	// afterwards. Only the alert's INSERT is deferred, to the processor that drains the row.
	//
	// The event rows are passed in because they answer whether it is needed. A caller
	// that evaluated a balance's monitors BEFORE the write hands the resulting alerts to
	// this writer, and those alerts are already inside this transaction — writing a
	// handoff for that balance as well would have the processor publish the same crossing
	// a second time under a different event id. See recordBalanceMonitorHandoffs and
	// balancesAlreadyEvaluatedInTx.
	if err := recordBalanceMonitorHandoffs(ctx, tx, span,
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

// RecordTransactionsWithBalancesAndOutboxes atomically records multiple transactions, updates
// the source and destination balances once, and inserts any lineage and event outbox entries in
// the same database transaction.
//
// The variadic event outbox entries are forwarded verbatim with eventOutboxes...
// so this delegation stays a pure pass-through: an omitted variadic arrives as an
// empty slice and is forwarded as one, which is why a caller that captures no
// events needs no change here.
func (d Datasource) RecordTransactionsWithBalancesAndOutboxes(ctx context.Context, txns []*model.Transaction, sourceBalance, destinationBalance *model.Balance, outboxes []*model.LineageOutbox, eventOutboxes ...*model.EventOutbox) ([]*model.Transaction, error) {
	return d.RecordTransactionsWithBalanceSetAndOutboxes(ctx, txns, []*model.Balance{sourceBalance, destinationBalance}, outboxes, eventOutboxes...)
}

// RecordTransactionsWithBalanceSetAndOutboxes atomically records multiple transactions, updates
// all changed balances, and inserts any lineage and event outbox entries in the same database
// transaction.
//
// eventOutboxes is variadic for the same source-compatibility reason documented on
// the declaration in repository.go: the transaction coalescing path calls this
// method with four arguments and belongs to a frozen pipeline.
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

	// Event outbox entries go in at the same point as in the single-transaction
	// writer above — after the lineage outbox, before the commit — so the batch's
	// events share the fate of the batch's balance updates.
	//
	// One event PER TRANSACTION, count-checked whenever the caller supplies any, and
	// DERIVED HERE when the caller supplies none. The coalescing path reaches this
	// writer without event rows — that is how coalesced mutations used to commit
	// without events: fifty transactions, one commit, and nothing published — and its
	// argument list cannot be extended, because the file that builds it belongs to the
	// frozen transaction pipeline. Deriving from the registered capture closes that gap
	// at the only layer both the transactions and their balances are available in.
	eventRows, err := resolveBatchEventOutboxes(ctx, txns, balances, eventOutboxes)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if err := insertEventOutboxesInTx(ctx, tx, eventRows); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to insert event outboxes: %w", err)
	}

	// The balance-monitor handoff for every balance this batch moved that was not already
	// evaluated with it. THIS is the path the coalescing pipeline reaches, and it is why
	// the handoff is written by the writer rather than supplied by the caller:
	// transaction_coalescing.go is frozen by AAP §0.6.2 and cannot be given a new
	// argument, so a caller-supplied handoff would have covered the single-transaction
	// path and silently missed every coalesced batch. Deriving it from the balances the
	// writer is already updating covers both. The coalesced caller supplies no monitor
	// alerts, so every balance here is handed off.
	if err := recordBalanceMonitorHandoffs(ctx, tx, span, balances, eventRows); err != nil {
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
// It logs the transaction retrieval using OpenTelemetry tracing.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The unique transaction ID.
// Returns:
// - The retrieved transaction if successful, or an error if retrieval fails.
