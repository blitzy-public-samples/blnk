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
	"github.com/blnkfinance/blnk/internal/notification"
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

// sendBulkTransactionWebhook captures the bulk_transaction.<status> event for a batch
// result.
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
	publishCtx := context.WithoutCancel(ctx)

	// THE ATOMIC PATH. When the coordinator record exists, the outcome and its event are
	// written by ONE database transaction, so neither can exist without the other. This is
	// what brings bulk_transaction.<status> under the requirement despite there being no
	// batch-spanning transaction to enrol the event in: the event is atomic with the
	// coordinator's terminal transition instead.
	if l.bulkBatchCoordinationEnabled() {
		err := l.finalizeBulkBatchOutcome(ctx, publishCtx, batchID, status, errorMsg, transactionCount, event)
		if !errors.Is(err, errBulkBatchNotCoordinated) {
			return err
		}

		// Capture became unconfigured between the guard above and the finalise, so there is
		// no event row for a transaction to carry. A MISSING COORDINATOR ROW NO LONGER
		// REACHES HERE: the repository adopts such a batch and writes its terminal row inside
		// the transaction that inserts the event, so the outcome and its event still commit
		// together. See errBulkBatchNotCoordinated.
		logrus.WithError(err).WithFields(logrus.Fields{
			"batch_id": batchID,
			"status":   status,
		}).Warn(
			"bulk transaction outcome capture is unavailable, so this batch's outcome goes " +
				"down the legacy transport instead of the event outbox",
		)
	}

	// THE LEGACY PATH. Reached only when event capture is unconfigured — no Kafka broker —
	// in which case this routes to the legacy webhook transport exactly as it did before
	// the event pipeline existed. There is no outbox row on this path for anything to be
	// atomic with, and no relay to drain one.
	return l.PublishEventDurably(publishCtx, event)
}

// bulkBatchCoordinationEnabled reports whether the coordinator record backs this
// deployment's batch outcomes.
func (l *Blnk) bulkBatchCoordinationEnabled() bool {
	if l == nil {
		return false
	}

	return l.eventConfiguration().EventPublishingConfigured()
}

// errBulkBatchNotCoordinated marks a batch whose outcome cannot be captured atomically.
var errBulkBatchNotCoordinated = errors.New("bulk transaction outcome capture is unavailable")

// finalizeBulkBatchOutcome records the batch outcome and its event in ONE transaction,
// retrying a transient failure with the SAME prepared event.
func (l *Blnk) finalizeBulkBatchOutcome(
	ctx context.Context,
	publishCtx context.Context,
	batchID, status, errorMsg string,
	transactionCount int,
	event NewWebhook,
) error {
	// PREPARED ONCE, OUTSIDE THE LOOP. The row carries the event id, and re-preparing per
	// attempt would be wasted work at best. It is also what makes the derived id visible
	// in the coordinator row: the finalise stores it, so the outcome and the event it
	// produced are joinable afterwards.
	outbox, err := l.PrepareEventOutbox(publishCtx, event)
	if err != nil {
		return err
	}
	if outbox == nil {
		// Publishing became unconfigured between the guard and here. Treated as
		// uncoordinated so the caller takes the legacy path rather than failing the batch.
		return errBulkBatchNotCoordinated
	}

	outcome := &model.BulkTransactionBatch{
		BatchID:          batchID,
		Status:           status,
		TransactionCount: transactionCount,
		ErrorMessage:     errorMsg,
	}

	var lastErr error
	for attempt := 1; attempt <= bulkOutcomeCaptureAttempts; attempt++ {
		var performed bool
		performed, lastErr = l.datasource.FinalizeBulkTransactionBatchWithEvent(publishCtx, batchID, outcome, outbox)
		if lastErr == nil {
			logrus.WithFields(logrus.Fields{
				"batch_id":  batchID,
				"status":    status,
				"event_id":  outbox.EventID,
				"performed": performed,
			}).Debug("the bulk transaction outcome and its event were committed together")

			return nil
		}

		// A NOT-FOUND FROM THE FINALISE IS NO LONGER EXPECTED. The repository adopts a batch
		// whose coordinator row is missing rather than reporting it absent, so this arm can
		// only be reached by a future repository answer nobody has written yet. It is kept as
		// the defensive route to the legacy transport — a captured outcome by the weaker
		// route beats none — and it is reported at ERROR rather than silently, because
		// reaching it means the atomicity this producer is documented to have was not
		// obtained.
		if isNotFoundError(lastErr) {
			logrus.WithError(lastErr).WithFields(logrus.Fields{
				"batch_id": batchID,
				"status":   status,
			}).Error(
				"the bulk transaction outcome could not be recorded atomically because the " +
					"repository reported the batch absent, which adoption is supposed to make " +
					"impossible; falling back to the legacy transport for this outcome",
			)

			return fmt.Errorf("%w: %s", errBulkBatchNotCoordinated, lastErr.Error())
		}

		if isConflictError(lastErr) {
			logrus.WithError(lastErr).WithFields(logrus.Fields{
				"batch_id": batchID,
				"status":   status,
				"attempt":  attempt,
			}).Error(
				"the bulk transaction outcome could not be recorded because the batch already " +
					"reports a different outcome, or its event id has been reused; no retry can " +
					"resolve either",
			)

			escalateLostBulkOutcome(batchID, status, "the event id was reused, so no retry can resolve it", lastErr)

			return lastErr
		}

		logrus.WithError(lastErr).WithFields(logrus.Fields{
			"batch_id":     batchID,
			"status":       status,
			"attempt":      attempt,
			"max_attempts": bulkOutcomeCaptureAttempts,
		}).Warn("failed to commit the bulk transaction outcome with its event; retrying")

		if attempt == bulkOutcomeCaptureAttempts {
			break
		}

		// Cancellation is honoured BETWEEN attempts. The caller going away is a reason to
		// stop retrying, not a reason to keep sleeping; the failure is still reported.
		select {
		case <-ctx.Done():
			logrus.WithError(lastErr).WithField("batch_id", batchID).Error(
				"the bulk transaction outcome was not recorded and the context was cancelled " +
					"before the retry budget was spent; this batch's outcome is not in the outbox",
			)

			escalateLostBulkOutcome(
				batchID, status,
				"the retry was abandoned when the caller's context was cancelled", lastErr,
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
		"the bulk transaction outcome could not be committed with its event after every " +
			"attempt; the batch remains unfinalized and its outcome is NOT in the outbox",
	)

	escalateLostBulkOutcome(
		batchID, status,
		fmt.Sprintf("every one of %d attempts failed", bulkOutcomeCaptureAttempts), lastErr,
	)

	return lastErr
}

// escalateLostBulkOutcome raises a batch summary that will never reach the outbox.
func escalateLostBulkOutcome(batchID, status, reason string, cause error) {
	notification.NotifyError(fmt.Errorf(
		"blnk: the bulk transaction outcome event for batch %s (status %s) was not captured — %s: %w",
		batchID, status, reason, cause,
	))
}

// recordBulkBatchStart writes the coordinator record for an asynchronous batch.
func (l *Blnk) recordBulkBatchStart(ctx context.Context, batchID string, req *model.BulkTransactionRequest) {
	if !l.bulkBatchCoordinationEnabled() || l.datasource == nil || req == nil {
		return
	}

	err := l.datasource.InsertBulkTransactionBatch(ctx, &model.BulkTransactionBatch{
		BatchID:          batchID,
		Status:           model.BulkBatchStatusProcessing,
		TransactionCount: len(req.Transactions),
		Atomic:           req.Atomic,
		Inflight:         req.Inflight,
	})
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"batch_id":          batchID,
			"transaction_count": len(req.Transactions),
		}).Error(
			"the bulk transaction batch coordinator record could not be written, so this batch is " +
				"not enumerable while it runs and will not appear among unfinalized batches; its " +
				"outcome event is still captured atomically, because the finalise adopts a batch " +
				"whose start was never recorded and writes the terminal row with the event",
		)
	}
}

// The retry budget for the bulk outcome finalise.
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

	// Send webhook with the complete error message including rollback status The error is
	// logged inside the capture and deliberately not propagated here: this function's
	// callers are handling a batch failure that has already happened, and a failure to
	// RECORD it must not mask the failure itself. The error is INSPECTED rather than
	// discarded. sendBulkTransactionWebhook escalates an exhausted capture through
	// notification.NotifyError itself, so nothing depends on this branch to be observable,
	// but discarding the value outright said the opposite — that the outcome of the one
	// capture describing a FAILED batch did not matter here.
	if captureErr := l.sendBulkTransactionWebhook(ctx, batchID, "failed", errorMessage, 0); captureErr != nil {
		logrus.WithError(captureErr).WithField("batch_id", batchID).Error(
			"the failed-batch outcome event was not captured; the rollback status above is the only " +
				"record of this batch's outcome",
		)
	}
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
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

		// THE COORDINATOR RECORD, WRITTEN BEFORE ANY MEMBER TRANSACTION RUNS.
		l.recordBulkBatchStart(ctx, batchID, req)

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
				// THE OUTCOME IS PERSISTED BEFORE COMPLETION IS ANNOUNCED. The capture is
				// synchronous and its result decides what is announced, because a completion line
				// written over an unrecorded outcome is how a missing summary stays invisible: the
				// batch reads as finished in the log while no subscriber will ever be told it
				// finished.
				if captureErr := l.sendBulkTransactionWebhook(bgCtx, batchID, status, "", len(req.Transactions)); captureErr != nil {
					span.RecordError(captureErr)
					logrus.WithError(captureErr).WithFields(logrus.Fields{
						"batch_id":          batchID,
						"status":            status,
						"transaction_count": len(req.Transactions),
					}).Error(
						"async bulk transaction batch finished but its outcome event was NOT captured; " +
							"every transaction in it is applied and individually published, and no " +
							"bulk_transaction summary will reach any subscriber for this batch",
					)
				} else {
					logrus.Infof("Completed async bulk transaction batch %s successfully", batchID)
				}
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
