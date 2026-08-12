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
	"fmt"
	"math/big"
	"strings"
	"time"

	blnkhooks "github.com/blnkfinance/blnk/internal/hooks"
	"github.com/blnkfinance/blnk/internal/hotpairs"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/internal/search"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

func (l *Blnk) getSourceAndDestination(ctx context.Context, transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {
	ctx, span := tracer.Start(ctx, "GetSourceAndDestination")
	defer span.End()

	var sourceBalance, destinationBalance *model.Balance

	cfg := l.config

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Source, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("source balance lookup failed")
			return nil, nil, err
		}
		// Update transaction source with the balance ID
		transaction.Source = sourceBalance.BalanceID
		span.SetAttributes(attribute.String("source.balance_id", sourceBalance.BalanceID))
	} else {
		// Use GetBalanceByID with queued checks if enabled, otherwise use lite version
		if cfg.Transaction.EnableQueuedChecks {
			sourceBalance, err = l.datasource.GetBalanceByID(transaction.Source, []string{}, true)
		} else {
			sourceBalance, err = l.datasource.GetBalanceByIDLite(transaction.Source)
		}
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("source balance lookup failed")
			return nil, nil, err
		}
	}

	// Check if Destination starts with "@"
	if strings.HasPrefix(transaction.Destination, "@") {
		destinationBalance, err = l.getOrCreateBalanceByIndicator(ctx, transaction.Destination, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("destination balance lookup failed")
			return nil, nil, err
		}
		// Update transaction destination with the balance ID
		transaction.Destination = destinationBalance.BalanceID
		span.SetAttributes(attribute.String("destination.balance_id", destinationBalance.BalanceID))
	} else {
		// Use GetBalanceByID with queued checks if enabled, otherwise use lite version
		if cfg.Transaction.EnableQueuedChecks {
			destinationBalance, err = l.datasource.GetBalanceByID(transaction.Destination, []string{}, true)
		} else {
			destinationBalance, err = l.datasource.GetBalanceByIDLite(transaction.Destination)
		}
		if err != nil {
			span.RecordError(err)
			logrus.WithError(err).Error("destination balance lookup failed")
			return nil, nil, err
		}
	}
	span.AddEvent("Retrieved source and destination balances")
	return sourceBalance, destinationBalance, nil
}

// resolveBalanceIDs resolves source and destination to actual balance IDs.
// If the source or destination starts with "@", it indicates a balance indicator (like @world)
// that needs to be resolved to an actual balance ID. This function creates the balance if needed.
// This should be called BEFORE acquiring locks to ensure we lock on the correct balance IDs.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction containing source and destination.
//
// Returns:
// - sourceBalanceID string: The resolved source balance ID.
// - destinationBalanceID string: The resolved destination balance ID.
// - error: An error if the balance IDs could not be resolved.
func (l *Blnk) resolveBalanceIDs(ctx context.Context, transaction *model.Transaction) (string, string, error) {
	ctx, span := tracer.Start(ctx, "ResolveBalanceIDs")
	defer span.End()

	sourceID := transaction.Source
	destID := transaction.Destination

	// Resolve source if it's an indicator (starts with "@")
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err := l.getOrCreateBalanceByIndicator(ctx, transaction.Source, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			return "", "", fmt.Errorf("failed to resolve source balance indicator: %w", err)
		}
		sourceID = sourceBalance.BalanceID
		span.SetAttributes(attribute.String("source.resolved_id", sourceID))
	}

	// Resolve destination if it's an indicator (starts with "@")
	if strings.HasPrefix(transaction.Destination, "@") {
		destBalance, err := l.getOrCreateBalanceByIndicator(ctx, transaction.Destination, transaction.Currency)
		if err != nil {
			span.RecordError(err)
			return "", "", fmt.Errorf("failed to resolve destination balance indicator: %w", err)
		}
		destID = destBalance.BalanceID
		span.SetAttributes(attribute.String("destination.resolved_id", destID))
	}

	span.AddEvent("Balance IDs resolved")
	return sourceID, destID, nil
}

// acquireLock acquires distributed locks for a transaction to ensure exclusive access to both
// source and destination balances. It uses a MultiLocker with deterministic ordering to prevent
// deadlocks when multiple transactions target the same balances.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - sourceBalanceID string: The ID of the source balance to lock.
// - destinationBalanceID string: The ID of the destination balance to lock.
//
// Returns:
// - *redlock.MultiLocker: A pointer to the acquired MultiLocker if successful.
// - error: An error if the locks could not be acquired.
func (l *Blnk) acquireLock(ctx context.Context, sourceBalanceID, destinationBalanceID string) (*redlock.MultiLocker, error) {
	ctx, span := tracer.Start(ctx, "Acquiring Lock")
	defer span.End()

	// Create a MultiLocker with both balance IDs
	// MultiLocker handles deduplication (if source == destination) and sorts keys lexicographically
	locker := redlock.NewMultiLocker(l.redis, []string{sourceBalanceID, destinationBalanceID}, model.GenerateUUIDWithSuffix("loc"))
	err := l.acquireTransactionLocker(ctx, locker)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Locks acquired", trace.WithAttributes(
		attribute.StringSlice("locked.keys", locker.Keys()),
	))
	return locker, nil
}

// updateTransactionDetails updates the details of a transaction, including source and destination balances and status.
// It starts a tracing span, creates a new transaction object with updated details, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The original transaction to be updated.
// - sourceBalance *model.Balance: The source balance for the transaction.
// - destinationBalance *model.Balance: The destination balance for the transaction.
//
// Returns:
// - *model.Transaction: A pointer to the new transaction object with updated details.
func (l *Blnk) updateTransactionDetails(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	_, span := tracer.Start(ctx, "Updating Transaction Details")
	defer span.End()

	// Create a new transaction object with updated details (immutable pattern)
	newTransaction := *transaction // Copy the original transaction
	newTransaction.Source = sourceBalance.BalanceID
	newTransaction.Destination = destinationBalance.BalanceID

	// Update the status based on the current status and inflight flag
	applicableStatus := map[string]string{
		StatusQueued:    StatusApplied,
		StatusApplied:   StatusApplied,
		StatusScheduled: StatusApplied,
		StatusCommit:    StatusApplied,
		StatusVoid:      StatusVoid,
	}
	newTransaction.Status = applicableStatus[transaction.Status]
	if transaction.Inflight {
		newTransaction.Status = StatusInflight
	}

	span.AddEvent("Transaction details updated")
	return &newTransaction
}

// postTransactionActions performs post-processing actions for a transaction. It starts
// a tracing span, queues the transaction and balance data for indexing in dependency
// order, captures the transaction's event in the transactional outbox when it was not
// already captured atomically, and processes fund lineage if applicable.
//
// It is therefore published here only when nothing captured it, which is three cases:
//
//   - A path that does not use the single-transaction atomic writer: a rejection,
//     recorded by RecordTransaction, which captures its own event and reports so.
//   - The COALESCED BATCH path. into the same outbox, with the same bounded retry
//     budget the other standalone captures spend, a moment after the commit rather than
//     inside it.
//   - A deployment with no Kafka brokers, where PublishEvent routes the event to the
//     legacy webhook transport instead.
//
// Parameters:
//   - ctx context.Context: The context for the operation.
//   - transaction *model.Transaction: The transaction for which to perform
//     post-processing actions.
//   - sourceBalance *model.Balance: The source balance (can be nil for rejected
//     transactions).
//   - destinationBalance *model.Balance: The destination balance (can be nil for rejected
//     transactions).
//   - eventCaptured bool: true when the transaction's event was already inserted inside
//     the ledger transaction, in which case no event is published here.
func (l *Blnk) postTransactionActions(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance, eventCaptured bool) {
	_, span := tracer.Start(ctx, "Post Transaction Actions")
	defer span.End()
	go func() {
		// Create an index batch to ensure balances are indexed before the transaction
		batch := search.NewIndexBatch(transaction.TransactionID)

		// Add balances as dependencies (indexed first) if they exist
		if sourceBalance != nil {
			batch.AddDependency("balances", sourceBalance.BalanceID, sourceBalance)
		}
		if destinationBalance != nil {
			batch.AddDependency("balances", destinationBalance.BalanceID, destinationBalance)
		}

		// Set transaction as the primary item (indexed after dependencies)
		batch.SetPrimary(l.Config().Transaction.IndexQueuePrefix, transaction.TransactionID, transaction)

		// Queue the batch for indexing
		err := l.queue.queueIndexBatch(batch)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}

		// PRODUCER CALL SITE for the seven status-derived transaction events, and the
		// FALLBACK one: the atomic writers capture this event inside the ledger transaction,
		// so this branch runs only for a transaction that was not persisted through one, or
		// for a deployment with no Kafka brokers.
		if !eventCaptured {
			err = l.PublishEventDurably(context.WithoutCancel(ctx), NewWebhook{
				Event:   getEventFromStatus(transaction.Status),
				Payload: transaction,
			}, WithEventLedgerID(transactionLedgerID(sourceBalance, destinationBalance)))
			if err != nil {
				span.RecordError(err)
				notification.NotifyError(err)
			}
		}

		span.AddEvent("Post-transaction actions completed", trace.WithAttributes(
			attribute.Bool("event.captured_atomically", eventCaptured),
		))
	}()
}

// validateTxn validates a transaction by checking if its reference has already been used.
// It starts a tracing span, checks the existence of the transaction reference, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated.
//
// Returns:
// - error: An error if the transaction reference has already been used or if there was an issue checking the reference.
func (l *Blnk) validateTxn(ctx context.Context, transaction *model.Transaction) error {
	ctx, span := tracer.Start(ctx, "Validating Transaction Reference")
	defer span.End()

	// Check if a transaction with the same reference already exists
	txn, err := l.datasource.TransactionExistsByRef(ctx, transaction.Reference)
	if err != nil {
		return err
	}

	// If the transaction reference already exists, return an error
	if txn {
		err := fmt.Errorf("reference %s has already been used", transaction.Reference)
		span.RecordError(err)
		notification.NotifyError(err)
		return err
	}

	span.AddEvent("Transaction validated")
	return nil
}

func IsDuplicateReferenceError(err error) bool {
	if err == nil {
		return false
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505" && strings.Contains(strings.ToLower(pqErr.Constraint), "reference")
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "duplicate key value violates unique constraint") && strings.Contains(msg, "reference") {
		return true
	}
	return strings.Contains(msg, "reference") && strings.Contains(msg, "already been used")
}

// applyTransactionToBalances applies a transaction to the provided balances.
// It starts a tracing span, calculates new balances, and updates the balances based on the transaction status.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balances []*model.Balance: A slice of Balance models to be updated. The first balance is the source, and the second is the destination.
// - transaction *model.Transaction: The transaction to be applied to the balances.
//
// Returns:
// - error: An error if the balances could not be updated.
func (l *Blnk) applyTransactionToBalances(ctx context.Context, balances []*model.Balance, transaction *model.Transaction) error {
	_, span := tracer.Start(ctx, "Applying Transaction to Balances")
	defer span.End()

	span.AddEvent("Calculating new balances")

	// Handle committed inflight transactions.
	if transaction.Status == StatusCommit {
		if err := balances[0].CommitInflightDebit(transaction); err != nil {
			span.RecordError(err)
			return fmt.Errorf("commit inflight debit failed: %w", err)
		}
		if err := balances[1].CommitInflightCredit(transaction); err != nil {
			span.RecordError(err)
			return fmt.Errorf("commit inflight credit failed: %w", err)
		}
		span.AddEvent("Committed inflight balances")
		return nil
	}

	transactionAmount := transaction.PreciseAmount

	// Handle voided transactions.
	if transaction.Status == StatusVoid {
		if err := balances[0].RollbackInflightDebit(transactionAmount); err != nil {
			span.RecordError(err)
			return fmt.Errorf("void inflight debit failed: %w", err)
		}
		if err := balances[1].RollbackInflightCredit(transactionAmount); err != nil {
			span.RecordError(err)
			return fmt.Errorf("void inflight credit failed: %w", err)
		}
		span.AddEvent("Rolled back inflight balances")
		return nil
	}

	// Update balances for other transaction statuses
	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Balances updated")
	return nil
}

// GetRefundableTransactionsByParentID retrieves refundable transactions by their parent transaction ID.
// It starts a tracing span, fetches the transactions from the datasource, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - parentTransactionID string: The ID of the parent transaction.
// - batchSize int: The number of transactions to retrieve in a batch.
// - offset int64: The offset for pagination.
//
// Returns:
// - []*model.Transaction: A slice of pointers to the retrieved Transaction models.
// - error: An error if the transactions could not be retrieved.

func (r transactionExecutionResult) usedCoalescing() bool {
	return r.mode == transactionExecutionModeQueuedBatch || r.mode == transactionExecutionModeHotQueuedBatch
}

// planTransactionExecution selects the internal execution mode for a transaction based on
// whether queued batching is allowed and whether hot-lane execution should be used.
func (l *Blnk) planTransactionExecution(transaction *model.Transaction, allowQueuedBatch, hotLane bool) transactionExecutionPlan {
	if allowQueuedBatch {
		if hotLane {
			return transactionExecutionPlan{mode: transactionExecutionModeHotQueuedBatch, transaction: transaction}
		}
		return transactionExecutionPlan{mode: transactionExecutionModeQueuedBatch, transaction: transaction}
	}

	return transactionExecutionPlan{mode: transactionExecutionModeSingle, transaction: transaction}
}

// executeTransactionPlan runs the selected internal execution mode and fails open from queued
// batching back to the single-transaction path when batching does not handle the work.
func (l *Blnk) executeTransactionPlan(ctx context.Context, plan transactionExecutionPlan) (transactionExecutionResult, error) {
	switch plan.mode {
	case transactionExecutionModeQueuedBatch:
		handled, err := l.TryRecordQueuedTransactionBatch(ctx, plan.transaction)
		if err != nil {
			logrus.WithError(err).Warnf("coalesced processing attempt failed for transaction %s", plan.transaction.TransactionID)
		}
		if handled {
			return transactionExecutionResult{
				mode:        transactionExecutionModeQueuedBatch,
				transaction: plan.transaction,
			}, nil
		}
		return l.executeTransactionPlan(ctx, l.planTransactionExecution(plan.transaction, false, false))
	case transactionExecutionModeHotQueuedBatch:
		handled, err := l.TryRecordQueuedTransactionBatchForHotLane(ctx, plan.transaction)
		if err != nil {
			logrus.WithError(err).Warnf("coalesced hot-lane processing attempt failed for transaction %s", plan.transaction.TransactionID)
		}
		if handled {
			return transactionExecutionResult{
				mode:        transactionExecutionModeHotQueuedBatch,
				transaction: plan.transaction,
			}, nil
		}
		return l.executeTransactionPlan(ctx, l.planTransactionExecution(plan.transaction, false, false))
	case transactionExecutionModeSingle:
		transaction, err := l.recordTransactionSingle(ctx, plan.transaction)
		if err != nil {
			return transactionExecutionResult{}, err
		}
		return transactionExecutionResult{
			mode:        transactionExecutionModeSingle,
			transaction: transaction,
		}, nil
	default:
		transaction, err := l.recordTransactionSingle(ctx, plan.transaction)
		if err != nil {
			return transactionExecutionResult{}, err
		}
		return transactionExecutionResult{
			mode:        transactionExecutionModeSingle,
			transaction: transaction,
		}, nil
	}
}

// processQueuedTransaction routes queued work through the shared executor so the planner can
// choose between normal queued batching, hot-lane batching, and single-transaction fallback.
func (l *Blnk) processQueuedTransaction(ctx context.Context, transaction *model.Transaction, hotLane bool) (transactionExecutionResult, error) {
	return l.executeTransactionPlan(ctx, l.planTransactionExecution(transaction, true, hotLane))
}

// RefundWorker processes refund transactions from the jobs channel and sends the results to the results channel.
// It starts a tracing span, processes each transaction, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - jobs <-chan *model.Transaction: A channel from which transactions are received for processing.
// - results chan<- BatchJobResult: A channel to which the results of the processing are sent.
// - wg *sync.WaitGroup: A wait group to synchronize the completion of the worker.
// - amount float64: The amount to be processed in the transaction.

func (l *Blnk) ProcessQueuedTransaction(ctx context.Context, transaction *model.Transaction, hotLane bool) (*model.Transaction, error) {
	result, err := l.processQueuedTransaction(ctx, transaction, hotLane)
	if err != nil {
		return nil, err
	}
	return result.transaction, nil
}

// RecordTransaction records a transaction by validating, processing balances, and finalizing the transaction.
// It starts a tracing span, acquires a lock, and performs the necessary steps to record the transaction.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be recorded.
//
// Returns:
// - *model.Transaction: A pointer to the recorded Transaction model.
// - error: An error if the transaction could not be recorded.
func (l *Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	result, err := l.executeTransactionPlan(ctx, l.planTransactionExecution(transaction, false, false))
	if err != nil {
		return nil, err
	}
	return result.transaction, nil
}

// recordTransactionSingle preserves the existing direct transaction-processing semantics by
// running the single-transaction flow under the balance lock.
func (l *Blnk) recordTransactionSingle(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RecordTransaction")
	defer span.End()

	startTime := time.Now()

	result, err := l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		// Execute pre-transaction hooks
		if err := l.Hooks.ExecutePreHooks(ctx, transaction.TransactionID, transaction); err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Validate and prepare the transaction, including retrieving source and destination balances
		transaction, sourceBalance, destinationBalance, err := l.validateAndPrepareTransaction(ctx, transaction)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Process the balances by applying the transaction
		if err := l.processBalances(ctx, transaction, sourceBalance, destinationBalance); err != nil {
			span.RecordError(err)
			return nil, err
		}

		work, skipPersist := l.buildTransactionExecutionWork(ctx, transaction, sourceBalance, destinationBalance)
		if skipPersist {
			span.AddEvent("Transaction with zero amount discarded, not persisted", trace.WithAttributes(attribute.String("transaction.id", work.transaction.TransactionID)))
			return work.transaction, nil
		}

		work, eventCaptured, monitorCapture, err := l.persistSingleTransactionExecutionWork(ctx, work)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// The captured set carries what the frozen work shape cannot: which transactions had
		// their event committed inside their own write, so the post-commit hook captures the
		// rest and duplicates none. See runTransactionPostCommitWorkForCapturedEvents.
		var capturedEvents map[string]struct{}
		if eventCaptured {
			capturedEvents = map[string]struct{}{work.transaction.TransactionID: {}}
		}

		l.runTransactionPostCommitWorkForCapturedEvents(ctx, span,
			[]*model.Balance{sourceBalance, destinationBalance},
			[]queuedBatchPostCommitWork{work}, nil, capturedEvents, monitorCapture)

		span.AddEvent("Transaction processed", trace.WithAttributes(attribute.String("transaction.id", work.transaction.TransactionID)))
		logrus.Infof("Transaction %s processed successfully", work.transaction.TransactionID)
		return work.transaction, nil
	})

	// Record metrics regardless of success or failure.
	duration := time.Since(startTime).Seconds()
	status := "error"
	currency := transaction.Currency
	if result != nil {
		status = result.Status
		currency = result.Currency
	}
	metrics.TransactionDuration.Record(ctx, duration,
		otelmetric.WithAttributes(attribute.String("status", status)),
	)
	if err == nil {
		metrics.TransactionTotal.Add(ctx, 1,
			otelmetric.WithAttributes(
				attribute.String("status", status),
				attribute.String("currency", currency),
			),
		)
	}

	return result, err
}

func (l *Blnk) runTransactionPostCommitWork(ctx context.Context, span trace.Span, orderedBalances []*model.Balance, postCommitWork []queuedBatchPostCommitWork) {
	l.runTransactionPostCommitWorkWithHooks(ctx, span, orderedBalances, postCommitWork, nil)
}

// runTransactionPostCommitWorkWithHooks executes monitor checks, post-hooks, and
// post-transaction actions for persisted work items, optionally reusing a preloaded
// hook set.
//
// so the captured-event set is carried by the wider function below rather than by a
// parameter here or a field on queuedBatchPostCommitWork.
func (l *Blnk) runTransactionPostCommitWorkWithHooks(ctx context.Context, span trace.Span, orderedBalances []*model.Balance, postCommitWork []queuedBatchPostCommitWork, postHooks []*blnkhooks.Hook) {
	// The captured set is RESOLVED FROM THE TABLE rather than passed in, because the
	// caller cannot supply it. The coalescing path calls the atomic writer and then this
	// function with the same work list, and the file doing so is frozen, so it cannot
	// report that the writer captured the batch's events inside the mutation. Asking the
	// outbox which events are already durable is the one answer that stays correct however
	// this function is reached.
	l.runTransactionPostCommitWorkForCapturedEvents(ctx, span, orderedBalances, postCommitWork, postHooks,
		l.durableTransactionEvents(ctx, postCommitWork), balanceMonitorCapture{})
}

// durableTransactionEvents returns the ids of the transactions whose lifecycle event is
// already recorded in the outbox.
//
// Since the atomic writers capture the event with the mutation, the post-commit
// fallback is a fallback in fact as well as in name: for most transactions the event is
// already durable, and publishing it again would prepare a second row carrying a fresh
// occurred_at. The unique index on the deterministic event id refuses that row, so
// nothing is duplicated — but the refusal arrives as an error the caller reports, and
// on a coalesced batch that is one spurious error per transaction, permanently.
// Suppressing the fallback where it is not owed removes the noise at its source instead
// of teaching the log to ignore it.
//
// Parameters:
//   - ctx context.Context: request context. A cancellation surfaces as a lookup error
//     and therefore as "not captured".
//   - postCommitWork []queuedBatchPostCommitWork: the committed work items.
//
// Returns:
//   - map[string]struct{}: transaction ids whose event is durable; nil when unknown or
//     none.
func (l *Blnk) durableTransactionEvents(ctx context.Context, postCommitWork []queuedBatchPostCommitWork) map[string]struct{} {
	if len(postCommitWork) == 0 || l.datasource == nil {
		return nil
	}

	if !eventPublishingConfigured(l.eventConfiguration()) {
		return nil
	}

	// One event id per transaction, derived rather than looked up. model.EventIdentityFor
	// is what makes transaction events derivable at all, and it is consulted here —
	// instead of assuming the transaction id is the identity — so this cannot drift from
	// the derivation PrepareEventOutbox performed when the row was written.
	transactionsByEventID := make(map[string]string, len(postCommitWork))
	eventIDs := make([]string, 0, len(postCommitWork))

	for _, work := range postCommitWork {
		transaction := work.transaction
		if transaction == nil {
			continue
		}

		eventType := getEventFromStatus(transaction.Status)
		identity, derivable := model.EventIdentityFor(eventType, transaction)
		if !derivable {
			continue
		}

		eventID := model.DeriveEventID(identity, eventType, model.SchemaVersionV1)
		transactionsByEventID[eventID] = transaction.TransactionID
		eventIDs = append(eventIDs, eventID)
	}

	if len(eventIDs) == 0 {
		return nil
	}

	present, err := l.datasource.ExistingEventIDs(ctx, eventIDs)
	if err != nil {
		logrus.WithError(err).Debug(
			"could not determine which transaction events were already captured; " +
				"the post-commit path will attempt capture and rely on the unique event id")

		return nil
	}

	captured := make(map[string]struct{}, len(present))
	for eventID := range present {
		if transactionID, ok := transactionsByEventID[eventID]; ok {
			captured[transactionID] = struct{}{}
		}
	}

	return captured
}

// runTransactionPostCommitWorkForCapturedEvents is
// runTransactionPostCommitWorkWithHooks with one extra fact: which of the work items
// already had their ledger event captured inside the database transaction that
// persisted them.
//
// Parameters:
//   - ctx context.Context: the request context. Monitor checks and event capture detach
//     from its cancellation, because both outlive the request that spawned them.
//   - span trace.Span: the caller's span, used to record hook failures.
//   - orderedBalances []*model.Balance: the balances to run monitor checks against.
//   - postCommitWork []queuedBatchPostCommitWork: the persisted work items.
//   - postHooks []*blnkhooks.Hook: a preloaded post-transaction hook set, or nil to
//     look one up.
//   - capturedEvents map[string]struct{}: the transaction ids whose event is already
//     durable.
//   - monitorCapture balanceMonitorCapture: which balances' monitors were already
//     evaluated with the mutation and which crossings are already durable.
func (l *Blnk) runTransactionPostCommitWorkForCapturedEvents(
	ctx context.Context,
	span trace.Span,
	orderedBalances []*model.Balance,
	postCommitWork []queuedBatchPostCommitWork,
	postHooks []*blnkhooks.Hook,
	capturedEvents map[string]struct{},
	monitorCapture balanceMonitorCapture,
) {
	// THE POST-COMMIT MONITOR CHECK IS SKIPPED WHEN THE DURABLE HANDOFF OWNS IT.
	if !l.balanceMonitorHandoffEnabled() {
		monitorCtx := context.WithoutCancel(ctx)
		for _, balance := range orderedBalances {
			balance := balance
			// Bound concurrent monitor checks instead of spawning one unbounded
			// goroutine per balance; acquiring the semaphore applies backpressure
			// outside the locked persistence path.
			balanceMonitorSem <- struct{}{}
			go func() {
				defer func() { <-balanceMonitorSem }()
				l.checkBalanceMonitors(monitorCtx, balance, monitorCapture)
			}()
		}
	}

	for _, work := range postCommitWork {
		if l.Hooks != nil {
			var err error
			if postHooks != nil {
				err = l.Hooks.ExecuteHooks(ctx, postHooks, blnkhooks.PostTransaction, work.transaction.TransactionID, work.transaction)
			} else {
				err = l.Hooks.ExecutePostHooks(ctx, work.transaction.TransactionID, work.transaction)
			}
			if err != nil {
				span.RecordError(err)
				logrus.WithError(err).Error("post-transaction hooks failed")
			}
		}
		l.postTransactionActions(ctx, work.transaction, work.sourceBalance, work.destinationBalance,
			transactionEventAlreadyCaptured(capturedEvents, work.transaction))
	}
}

// transactionEventAlreadyCaptured reports whether a transaction's event is already
// durable.
//
// A nil set and a nil transaction both answer false, which is the conservative
// direction: the event is then captured by the post-commit hook, and the worst case is
// an event stored a moment later than it could have been. Answering true wrongly would
// LOSE the event, because nothing else would capture it.
//
// Parameters:
//   - captured map[string]struct{}: the transaction ids captured inside their own
//     database transaction.
//   - transaction *model.Transaction: the work item's transaction. May be nil.
//
// Returns:
//   - bool: true only when this transaction's id is in the set.
func transactionEventAlreadyCaptured(captured map[string]struct{}, transaction *model.Transaction) bool {
	if len(captured) == 0 || transaction == nil {
		return false
	}

	_, ok := captured[transaction.TransactionID]

	return ok
}

// listHooksForExecution returns the current hook set for the requested type, or nil when
// hook execution is not configured for the current Blnk instance.
func (l *Blnk) listHooksForExecution(ctx context.Context, hookType blnkhooks.HookType) ([]*blnkhooks.Hook, error) {
	if l.Hooks == nil {
		return nil, nil
	}

	return l.Hooks.ListHooks(ctx, hookType)
}

// executeWithLock executes a function with distributed locks to ensure exclusive access to both
// source and destination balances. It resolves balance IDs first (handling @world indicators),
// then acquires locks in deterministic order to prevent deadlocks.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction for which to acquire the locks.
// - fn func(context.Context) (*model.Transaction, error): The function to execute with the locks.
//
// Returns:
// - *model.Transaction: A pointer to the Transaction model returned by the function.
// - error: An error if the locks could not be acquired or if the function execution fails.
func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "ExecuteWithLock")
	defer span.End()

	// First resolve balance IDs (handle @ indicators) BEFORE acquiring locks
	// This ensures we lock on the correct balance IDs
	sourceID, destID, err := l.resolveBalanceIDs(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to resolve balance IDs: %w", err)
	}

	// Update transaction with resolved IDs
	transaction.Source = sourceID
	transaction.Destination = destID

	// Acquire distributed locks for both source and destination balances
	// MultiLocker handles deduplication (if source == destination) and sorts keys lexicographically
	locker, err := l.acquireLock(ctx, sourceID, destID)
	if err != nil {
		hotpairs.RecordContention(ctx, l.hotPairs, sourceID, destID, transaction.Currency, err)
		metrics.HotpairsContentionTotal.Add(ctx, 1)
		span.RecordError(err)
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	// Execute the provided function with the locks held
	// The function will re-fetch balances to get fresh state after lock acquisition
	return fn(ctx)
}

// validateAndPrepareTransaction validates the transaction and prepares it by retrieving the source and destination balances.
// It starts a tracing span, validates the transaction, retrieves the balances, and updates the transaction with the balance IDs.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be validated and prepared.
//
// Returns:
// - *model.Transaction: A pointer to the new transaction object with updated details.
// - *model.Balance: A pointer to the source Balance model.
// - *model.Balance: A pointer to the destination Balance model.
// - error: An error if the transaction validation or balance retrieval fails.
func (l *Blnk) validateAndPrepareTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, *model.Balance, *model.Balance, error) {
	ctx, span := tracer.Start(ctx, "ValidateAndPrepareTransaction")
	defer span.End()

	// Validate the transaction
	if err := l.validateTxn(ctx, transaction); err != nil {
		span.RecordError(err)
		return nil, nil, nil, l.logAndRecordError(span, "transaction validation failed", err)
	}

	// Retrieve the source and destination balances
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(ctx, transaction)
	if err != nil {
		span.RecordError(err)
		return nil, nil, nil, l.logAndRecordError(span, "failed to get source and destination balances", err)
	}

	// Create a copy of the transaction and update it (immutable)
	newTransaction := *transaction // Copy the original transaction
	newTransaction.Source = sourceBalance.BalanceID
	newTransaction.Destination = destinationBalance.BalanceID

	span.AddEvent("Transaction validated and prepared", trace.WithAttributes(
		attribute.String("source.balance_id", sourceBalance.BalanceID),
		attribute.String("destination.balance_id", destinationBalance.BalanceID)))

	// Return the new transaction, source, and destination balances
	return &newTransaction, sourceBalance, destinationBalance, nil
}

// processBalances processes the source and destination balances by applying the transaction in-memory.
// It starts a tracing span, applies the transaction to the balances, and records relevant events and errors.
// Note: The actual database update of balances is done atomically with the transaction persistence
// step to ensure consistency.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - transaction *model.Transaction: The transaction to be applied to the balances.
// - sourceBalance *model.Balance: The source balance to be updated.
// - destinationBalance *model.Balance: The destination balance to be updated.
//
// Returns:
// - error: An error if the transaction could not be applied to the balances.
func (l *Blnk) processBalances(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	ctx, span := tracer.Start(ctx, "ProcessBalances")
	defer span.End()

	// Apply the transaction to the source and destination balances (in-memory only)
	if err := l.applyTransactionToBalances(ctx, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		span.RecordError(err)
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	span.AddEvent("Balances calculated")
	return nil
}

// buildTransactionExecutionWork converts an in-memory-applied transaction into the
// shared persistence and post-commit work shape used by both single and batched
// execution paths.
func (l *Blnk) buildTransactionExecutionWork(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (queuedBatchPostCommitWork, bool) {
	transaction = l.updateTransactionDetails(ctx, transaction, sourceBalance, destinationBalance)
	if transaction.PreciseAmount != nil && transaction.PreciseAmount.Cmp(big.NewInt(0)) == 0 {
		return queuedBatchPostCommitWork{
			transaction:        transaction,
			sourceBalance:      sourceBalance,
			destinationBalance: destinationBalance,
		}, true
	}

	return queuedBatchPostCommitWork{
		transaction:        transaction,
		sourceBalance:      sourceBalance,
		destinationBalance: destinationBalance,
		outbox:             l.prepareTransactionOutbox(ctx, transaction, sourceBalance, destinationBalance),
	}, false
}

// transactionLedgerID resolves THE LEDGER a transaction belongs to, from the balances
// it moves value between.
//
// They do NOT agree about the partition. The queue hashes the source BALANCE id into
// its own shard space; this returns the source balance's LEDGER, which Kafka hashes
// with murmur2 into a partition.
//
// Parameters:
//   - sourceBalance, destinationBalance *model.Balance: the loaded balances.
//
// Returns:
//   - string: the ledger id, or "" when neither balance is available.
func transactionLedgerID(sourceBalance, destinationBalance *model.Balance) string {
	if sourceBalance != nil && strings.TrimSpace(sourceBalance.LedgerID) != "" {
		return sourceBalance.LedgerID
	}

	if destinationBalance != nil {
		return destinationBalance.LedgerID
	}

	return ""
}

// persistSingleTransactionExecutionWork atomically persists one prepared transaction,
// its updated balances, any lineage outbox AND the transaction's Kafka event row, using
// the shared execution work shape.
//
// A nil row with no error is the unconfigured case and is simply passed through; the
// writer skips nil entries, which is what keeps a deployment with no brokers — and
// every existing test — working unchanged.
//
// Parameters:
//   - ctx context.Context: the context for the write.
//   - work queuedBatchPostCommitWork: the prepared transaction, its balances and its
//     lineage row.
//
// Returns:
//   - queuedBatchPostCommitWork: the work item carrying the persisted transaction.
//   - bool: true when the transaction's ledger event was committed inside this write,
//     which is what tells the post-commit hook not to capture it a second time.
//   - balanceMonitorCapture: which balances' monitors were evaluated with this write
//     and which crossings it committed, which is what tells the post-commit monitor
//     check what is already durable.
//   - error: the preparation or persistence failure. On error nothing was written.
func (l *Blnk) persistSingleTransactionExecutionWork(ctx context.Context, work queuedBatchPostCommitWork) (queuedBatchPostCommitWork, bool, balanceMonitorCapture, error) {
	ctx, span := tracer.Start(ctx, "PersistSingleTransactionExecutionWork")
	defer span.End()

	// PREPARED BEFORE THE WRITE, AND A FAILURE STOPS THE WRITE. Preparing costs one JSON
	// marshal and no I/O — nothing here contacts a broker or a database — so the only
	// failure it can report is a payload that will not serialise, which is a producer
	// defect the caller must see rather than a transient condition to work around.
	eventOutbox, err := l.prepareTransactionEventOutbox(ctx, work.transaction, work.sourceBalance, work.destinationBalance)
	if err != nil {
		span.RecordError(err)

		return queuedBatchPostCommitWork{}, false, balanceMonitorCapture{}, l.logAndRecordError(span,
			"refusing to persist a transaction whose ledger event could not be prepared", err)
	}

	// THE MONITOR ALERTS TRAVEL WITH THE MUTATION TOO.
	monitorEvents, monitorCapture, err := l.prepareBalanceMonitorEvents(ctx,
		[]*model.Balance{work.sourceBalance, work.destinationBalance})
	if err != nil {
		span.RecordError(err)

		return queuedBatchPostCommitWork{}, false, balanceMonitorCapture{}, l.logAndRecordError(span,
			"refusing to persist a transaction whose balance monitor alert could not be prepared", err)
	}

	// THE EVENT ROWS TRAVEL WITH THE MUTATION. The writer inserts them inside the same
	// database transaction as the two balance updates and the transaction row, so an event
	// cannot outlive a rollback and the mutation cannot outlive a lost event. The
	// transaction's own event leads; the monitor alerts follow it, and the writer skips
	// nil entries so an unconfigured deployment passes a single nil exactly as before.
	transaction, err := l.datasource.RecordTransactionWithBalancesAndOutbox(ctx, work.transaction, work.sourceBalance, work.destinationBalance, work.outbox,
		append([]*model.EventOutbox{eventOutbox}, monitorEvents...)...)
	if err != nil {
		span.RecordError(err)
		return queuedBatchPostCommitWork{}, false, balanceMonitorCapture{}, l.logAndRecordError(span, "failed to persist transaction with balances", err)
	}

	work.transaction = transaction
	// Reported only on the success path: the commit is what makes the events durable, so a
	// failed write must leave the post-commit hook free to capture both the transaction event
	// and the monitor alerts on the standalone path rather than believing they are stored.
	eventCaptured := eventOutbox != nil

	span.AddEvent("Transaction and balances persisted atomically", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.Bool("lineage.outbox_created", work.outbox != nil),
		attribute.Bool("event.captured_in_transaction", eventCaptured),
		attribute.Int("event.monitor_alerts_captured", len(monitorEvents)),
	))

	return work, eventCaptured, monitorCapture, nil
}

// prepareTransactionEventOutbox builds the outbox row for a transaction's lifecycle
// event, ready to be inserted inside the mutation's own database transaction.
//
// Parameters:
//   - ctx context.Context: the context for the operation, used for tracing.
//   - transaction *model.Transaction: the transaction as it will be persisted, with its
//     final status already assigned by updateTransactionDetails.
//   - sourceBalance, destinationBalance *model.Balance: the balances this transaction
//     moves value between.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not
//     configured.
//   - error: only when the payload cannot be serialised.
func (l *Blnk) prepareTransactionEventOutbox(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.EventOutbox, error) {
	if transaction == nil {
		return nil, nil
	}

	event := NewWebhook{
		Event:   getEventFromStatus(transaction.Status),
		Payload: transaction,
	}

	// The SAME resolver the post-commit fallback uses, so an event captured atomically and
	// one captured after the commit are keyed identically. WithEventLedgerID ignores an empty
	// value, so the rejection case needs no branch of its own.
	return l.PrepareEventOutbox(ctx, event,
		WithEventLedgerID(transactionLedgerID(sourceBalance, destinationBalance)))
}

func (l *Blnk) prepareTransactionOutbox(ctx context.Context, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.LineageOutbox {
	if transaction.Status != StatusApplied && transaction.Status != StatusInflight {
		return nil
	}

	// Skip lineage for commits of inflight transactions - lineage was already created for the original.
	isInflightCommit := transaction.ParentTransaction != "" && transaction.MetaData != nil && transaction.MetaData["inflight"] == true
	if isInflightCommit {
		return nil
	}

	return l.PrepareLineageOutbox(ctx, transaction, sourceBalance, destinationBalance)
}

func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.MultiLocker) {
	ctx, span := tracer.Start(ctx, "ReleaseLock")
	defer span.End()

	// Attempt to release all locks
	if err := locker.Unlock(ctx); err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to release lock")
	}
	span.AddEvent("Locks released")
}

// logAndRecordError logs an error message and records the error in the tracing span.
// It returns a formatted error message combining the provided message and the original error.
//
// Parameters:
// - span trace.Span: The tracing span to record the error.
// - msg string: The error message to log and include in the formatted error.
// - err error: The original error to be logged and recorded.
//
// Returns:
// - error: A formatted error message combining the provided message and the original error.
func (l *Blnk) logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.WithError(err).Error(msg)
	return fmt.Errorf("%s: %w", msg, err)
}
