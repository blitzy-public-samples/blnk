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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

func (l *Blnk) processBulkTransactions(ctx context.Context, transactions []*model.Transaction, batchID string, inflight bool, skipQueue bool) error {
	for i, txn := range transactions {
		// Set transaction properties
		txn.Inflight = inflight
		txn.SkipQueue = skipQueue // Process synchronously within the batch context first
		txn.ParentTransaction = batchID

		// Add sequence number to metadata
		if txn.MetaData == nil {
			txn.MetaData = make(map[string]interface{})
		}
		txn.MetaData["sequence"] = i + 1

		// Queue the transaction (which will record it if SkipQueue is true)
		if _, err := l.QueueTransaction(ctx, txn); err != nil {
			// Create a more descriptive error that includes transaction reference details
			return fmt.Errorf("failed to queue transaction %d (Reference: %s, Source: %s, Destination: %s, Amount: %.2f): %w",
				i+1, txn.Reference, txn.Source, txn.Destination, txn.Amount, err)
		}
	}
	return nil
}

// rollbackBatchTransactions performs a rollback of transactions in a batch
// Returns the action performed (voided/refunded) and any error that occurred
func (l *Blnk) rollbackBatchTransactions(ctx context.Context, batchID string, isInflight bool) (string, error) {
	var action string
	var rollbackErr error

	if isInflight {
		action, rollbackErr = l.voidInflightBatchTransactions(ctx, batchID)
	} else {
		action, rollbackErr = l.refundNonInflightBatchTransactions(ctx, batchID)
	}

	l.logRollbackResult(batchID, action, rollbackErr)
	return action, rollbackErr
}

// voidInflightBatchTransactions voids all inflight transactions in a batch
func (l *Blnk) voidInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetInflightTransactionsByParentID,
		l.VoidWorker,
	)
	return "voided", err
}

// refundNonInflightBatchTransactions refunds all non-inflight transactions in a batch
func (l *Blnk) refundNonInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetRefundableTransactionsByParentID,
		l.RefundWorker,
	)
	return "refunded", err
}

// logRollbackResult logs the outcome of a rollback operation
func (l *Blnk) logRollbackResult(batchID string, action string, err error) {
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to rollback batch transactions")
	} else {
		logrus.WithFields(logrus.Fields{
			"batch_id": batchID,
			"action":   action,
		}).Info("successfully rolled back atomic batch")
	}
}

// sendBulkTransactionWebhook captures the bulk_transaction.<status> event for a batch result.
//
// This is the producer call site for the bulk_transaction.* family. The relay claims the row
// it writes, publishes it to blnk.transactions with bounded retry, dead-letters it to
// blnk.transactions.dlt if the retry budget is spent, and — during the dual-delivery window
// — enqueues the legacy webhook task from that same row, which is why this function keeps
// its name and its log message.
//
// THE PAYLOAD MAP IS PASSED THROUGH UNCHANGED. The same map value is what is marshaled into
// the outbox payload, so re-shaping it into a struct or normalising its keys would change
// what subscribers receive. It has THREE shapes, not one, and all three are part of the
// contract:
//
//	status != "failed"                   → batch_id, status, timestamp, transaction_count
//	status == "failed", errorMsg != ""   → batch_id, status, timestamp, error
//	status == "failed", errorMsg == ""   → batch_id, status, timestamp
//
// batch_id is what makes the family coherent downstream: it is the aggregate id the outbox
// derives for a map payload, so the events describing one batch's progress group and order
// by the batch they belong to.
//
// The event string stays a RUNTIME CONCATENATION of "bulk_transaction." and the status.
// This is the only event name in the catalogue with an open suffix set, and it is routed
// by prefix in model.EventCategory precisely for that reason; spelling it as a lookup or
// a format string would invite a literal that no longer shares the prefix, which would
// misroute the event to the internal system topic with nothing failing to say so.
//
// THE CALLER'S CONTEXT IS THREADED IN AND ITS CANCELLATION IS DROPPED — both halves,
// through the context.WithoutCancel idiom postBalanceActions and
// runTransactionPostCommitWorkWithHooks already apply.
//
// Threading it is what keeps the capture attached to the operation. Two of the four paths
// that reach this function arrive from a live traced operation — transaction_rejection.go
// rejecting an atomic member, and transaction_queue.go failing to record a split
// transaction — and both hold a context carrying that operation's span. Publishing under
// context.Background() severed the outbox span from the batch it describes, so the one
// event that says a batch failed appeared in no trace at all, which is precisely the
// event an operator goes looking for.
//
// Dropping the cancellation is what keeps the capture durable, and it is the reason a
// bare ctx will not do. PublishEvent writes the outbox row with the context it is given,
// and the failure path reaches this line only AFTER a full batch rollback has run; a
// context that a long rollback exhausted, or that the API request behind it has already
// abandoned, would abort the insert and lose the event with nothing but a log line to
// show it — the single failure mode the outbox exists to rule out. The row must be
// written on the strength of the batch being finished, not of its context still being
// alive.
//
// # This is the ONE event that cannot be enrolled in its mutation's transaction, and why
//
// Requirement R-2 puts every event in the same database transaction as the ledger mutation
// that produced it, and every other producer in this codebase now does exactly that. This one
// cannot, because THERE IS NO BATCH-SPANNING TRANSACTION for it to join. A bulk request is
// executed one transaction at a time through QueueTransaction, with compensating void or
// refund as its rollback — see processBulkTransactions and rollbackBatchTransactions — so at
// the moment the batch's outcome becomes known, every mutation it describes has already
// committed under its own transaction. There is no row this event could be atomic with.
//
// The per-transaction events ARE atomic: each transaction in the batch carries its own
// status-derived event, inserted inside its own persistence transaction by
// persistSingleTransactionExecutionWork. What this event adds is the BATCH SUMMARY, which
// belongs to no single mutation. Creating a batch-spanning transaction would mean restructuring
// the transaction-processing pipeline, which this change is explicitly forbidden to modify
// (AAP §0.6.2 freezes queue.go, transaction_queue.go and transaction_coalescing.go).
//
// So the guarantee available here is a weaker one, and it is made as strong as it can be
// rather than left as a log line:
//
//   - The capture is SYNCHRONOUS. The caller does not return believing the outcome was
//     recorded while the write is still in flight.
//   - It is RETRIED with bounded backoff, because the realistic failure is a transient
//     database error and a single attempt turned that into permanent loss of the outcome.
//   - Exhaustion is reported as an ERROR RETURN, so callers can act on it, in addition to
//     being logged at error level with the batch id — which is the only handle an operator
//     has for reconstructing the outcome by hand.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for the retry sleeps only. The insert
//     itself uses a detached context for the reason given above.
//   - batchID string: the parent transaction id of the batch, and the event's aggregate.
//   - status string: the batch outcome — "applied", "inflight" or "failed" today. It is
//     both a payload field and the event name's suffix.
//   - errorMsg string: the failure detail, including rollback status. Empty on success.
//   - transactionCount int: the number of transactions in the batch. Omitted from the
//     payload on the failure path, where callers pass 0.
//
// Returns:
//   - error: the last persistence error when every attempt failed; nil on success and on
//     every no-op, including the unconfigured case.
func (l *Blnk) sendBulkTransactionWebhook(ctx context.Context, batchID, status, errorMsg string, transactionCount int) error {
	// Create payload with or without error info depending on status
	payload := map[string]interface{}{
		"batch_id":  batchID,
		"status":    status,
		"timestamp": time.Now(),
	}

	// Only include transaction count for success cases
	if status != "failed" {
		payload["transaction_count"] = transactionCount
	}

	// Include error details for failure cases
	if status == "failed" && errorMsg != "" {
		payload["error"] = errorMsg
	}

	event := NewWebhook{
		Event:   "bulk_transaction." + status,
		Payload: payload,
	}

	// DETACHED FROM CANCELLATION, NOT FROM THE TRACE, and derived once so every attempt
	// shares it. The outcome has already happened, so its capture must not be abandoned
	// because a long rollback exhausted the caller's deadline — and context.Background()
	// would buy that at the cost of the trace, leaving the outbox span an unparented root.
	// The one event that says a batch FAILED would then appear in no trace at all, which is
	// precisely the batch an operator goes looking for.
	publishCtx := context.WithoutCancel(ctx)

	var lastErr error
	for attempt := 1; attempt <= bulkOutcomeCaptureAttempts; attempt++ {
		lastErr = l.PublishEvent(publishCtx, event)
		if lastErr == nil {
			return nil
		}

		logrus.WithError(lastErr).WithFields(logrus.Fields{
			"batch_id":     batchID,
			"status":       status,
			"attempt":      attempt,
			"max_attempts": bulkOutcomeCaptureAttempts,
		}).Warn("failed to capture the bulk transaction outcome event; retrying")

		if attempt == bulkOutcomeCaptureAttempts {
			break
		}

		// Cancellation is honoured BETWEEN attempts. The caller going away is a reason to
		// stop retrying, not a reason to keep sleeping; the failure is still reported.
		select {
		case <-ctx.Done():
			logrus.WithError(lastErr).WithField("batch_id", batchID).Error(
				"the bulk transaction outcome event was not captured and the context was cancelled " +
					"before the retry budget was spent; this batch's outcome is not in the outbox",
			)

			return lastErr
		case <-time.After(bulkOutcomeCaptureBackoff * time.Duration(attempt)):
		}
	}

	logrus.WithError(lastErr).WithFields(logrus.Fields{
		"batch_id": batchID,
		"status":   status,
		"attempts": bulkOutcomeCaptureAttempts,
	}).Error(
		"the bulk transaction outcome event could not be captured after every attempt; this " +
			"batch's outcome is NOT in the outbox and will not be published to any subscriber",
	)

	return lastErr
}

// The retry budget for the bulk outcome capture.
//
// Three attempts at 200ms, 400ms is deliberately far smaller than the relay's own budget and
// is not trying to be it. This is one INSERT against the local database, retried to survive a
// transient error — a momentary connection reset, a brief pool exhaustion — and nothing more.
// A budget large enough to ride out a real outage would hold the batch goroutine open for
// minutes without improving the outcome, since a database that is down will still be down.
const (
	bulkOutcomeCaptureAttempts = 3
	bulkOutcomeCaptureBackoff  = 200 * time.Millisecond
)

// handleAsyncBulkTransactionFailure handles failures in asynchronous processing
// and builds a detailed error message including rollback status
func (l *Blnk) handleAsyncBulkTransactionFailure(ctx context.Context, err error, batchID string, isAtomic bool, isInflight bool) {
	logrus.WithError(err).WithField("batch_id", batchID).Error("async bulk transaction error")

	var errorMessage string

	if isAtomic {
		action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, isInflight)

		if rollbackErr != nil {
			errorMessage = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			logrus.WithError(rollbackErr).WithField("batch_id", batchID).Error("failed to roll back batch")
		} else {
			errorMessage = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			logrus.WithFields(logrus.Fields{
				"batch_id": batchID,
				"action":   action,
			}).Info("successfully rolled back async batch")
		}
	} else {
		// If not atomic, just include the original error and note about no rollback
		errorMessage = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
	}

	// Send webhook with the complete error message including rollback status
	// The error is logged inside the capture and deliberately not propagated here: this
	// function's callers are handling a batch failure that has already happened, and a
	// failure to RECORD it must not mask the failure itself.
	_ = l.sendBulkTransactionWebhook(ctx, batchID, "failed", errorMessage, 0) // 0 count for failed batch
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
// If atomic is true: Any failure will cause all transactions to be rolled back (or voided if inflight).
// If atomic is false: Failures will stop processing but previous transactions remain unaffected.
// If run_async is true: Processing happens in background with webhook notifications.
func (l *Blnk) CreateBulkTransactions(ctx context.Context, req *model.BulkTransactionRequest) (*model.BulkTransactionResult, error) {
	ctx, span := tracer.Start(ctx, "Blnk.CreateBulkTransactions")
	defer span.End()

	// Generate batch ID (parent transaction ID)
	batchID := model.GenerateUUIDWithSuffix("bulk")
	span.SetAttributes(attribute.String("batch.id", batchID))

	// Check if this should be run asynchronously
	if req.RunAsync {
		if !asyncBulkSemaphore.TryAcquire(1) {
			return nil, apierror.NewAPIError(
				apierror.ErrRateLimited,
				"too many async bulk operations in progress, try again later",
				nil,
			)
		}

		// processBulkTransactions mutates each transaction (status, metadata,
		// parent); clone them so the background goroutine never races the caller
		// still holding the request's transactions.
		asyncTxns := cloneTransactionsForAsync(req.Transactions)

		// Start processing in background
		go func() {
			defer asyncBulkSemaphore.Release(1)

			// Create a background context with timeout
			bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			logrus.Infof("Starting async bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
				batchID, len(asyncTxns), req.Atomic, req.Inflight)

			// Process transactions in batch
			err := l.processBulkTransactions(bgCtx, asyncTxns, batchID, req.Inflight, req.SkipQueue)

			if err != nil {
				// Handle failure (rollback if atomic, send webhook)
				l.handleAsyncBulkTransactionFailure(bgCtx, err, batchID, req.Atomic, req.Inflight)
			} else {
				// Send webhook notification for success
				status := "inflight"
				if !req.Inflight {
					status = "applied"
				}
				if captureErr := l.sendBulkTransactionWebhook(bgCtx, batchID, status, "", len(req.Transactions)); captureErr != nil {
					// Logged inside the capture with the batch id. The batch itself
					// succeeded and its per-transaction events were captured atomically, so
					// this is a missing SUMMARY rather than a missing outcome, and the
					// goroutine has nobody to return an error to.
					span.RecordError(captureErr)
				}
				logrus.Infof("Completed async bulk transaction batch %s successfully", batchID)
			}
		}()

		// Return immediate response indicating async processing started
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "processing", // Indicate that it's running in the background
		}, nil
	}

	// Synchronous processing
	logrus.Infof("Starting sync bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
		batchID, len(req.Transactions), req.Atomic, req.Inflight)

	// Process transactions in batch
	if err := l.processBulkTransactions(ctx, req.Transactions, batchID, req.Inflight, req.SkipQueue); err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithField("batch_id", batchID).Error("sync bulk transaction error")

		var responseError string
		if req.Atomic {
			action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, req.Inflight)
			if rollbackErr != nil {
				responseError = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			} else {
				responseError = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			}
		} else {
			responseError = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
		}

		// Return error result for synchronous failure
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "failed",
			Error:   responseError,
		}, errors.New(responseError) // Return the error itself as well
	}

	// Synchronous success
	status := "inflight"
	if !req.Inflight {
		status = "applied"
	}

	logrus.Infof("Completed sync bulk transaction batch %s successfully", batchID)
	return &model.BulkTransactionResult{
		BatchID:          batchID,
		Status:           status,
		TransactionCount: len(req.Transactions),
	}, nil
}
