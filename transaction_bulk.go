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
// # NO LEDGER IS SUPPLIED, and the batch is the ordering domain by necessity
//
// Requirement R-6 partitions by ledger id, and every ledger-scoped capture in this codebase
// threads one. This event is not ledger-scoped: a bulk request may name transactions whose
// balances belong to DIFFERENT ledgers, so "the ledger of this batch" is not a value that
// exists. Picking one of them — the first transaction's, say — would key a batch's summary on a
// ledger that describes only part of it, and two summaries for the same batch could land on
// different partitions as the batch's membership changed.
//
// The BATCH is the aggregate this event describes, so batch_id is the ordering domain, and it is
// the right one: the events describing one batch's progress are mutually ordered, which is the
// guarantee a consumer of batch progress needs. ledger_id is stored as SQL NULL, honestly
// recording that this event belonged to no single ledger. Both facts are stated in the
// partition-key table in docs/event-streaming.md and pinned by test, rather than left as an
// implicit consequence of the fallback chain.
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
// # This event cannot be enrolled in its mutation's transaction, and it is one of THREE
//
// Requirement R-2 puts every event in the same database transaction as the ledger mutation
// that produced it, and most producers in this codebase do exactly that. This one cannot,
// because THERE IS NO BATCH-SPANNING TRANSACTION for it to join. A bulk request is executed
// one transaction at a time through QueueTransaction, with compensating void or refund as its
// rollback — see processBulkTransactions and rollbackBatchTransactions — so at the moment the
// batch's outcome becomes known, every mutation it describes has already committed under its
// own transaction. There is no row this event could be atomic with.
//
// It is NOT the only such capture, and an earlier revision of this comment claimed it was.
// The complete set is three, each post-commit and each at-most-once for its own structural
// reason: this one; `balance.monitor` in checkBalanceMonitors, whose condition is met on a
// balance another transaction already committed; and the status-derived `transaction.*`
// events of a COALESCED batch, captured by postTransactionActions' fallback because that
// batch's writer is called from a file AAP §0.6.2 freezes. All three spend the same bounded
// retry budget, and docs/event-streaming.md publishes the set so a subscriber knows which
// event types carry the weaker guarantee. Every OTHER event type is captured inside its
// mutation's transaction.
//
// A bulk request is executed one transaction at a time through QueueTransaction, with
// compensating void or refund as its rollback — see processBulkTransactions and
// rollbackBatchTransactions — so by the moment the batch's outcome becomes known, every
// mutation it describes has already committed under its own transaction, each carrying its own
// status-derived event inserted inside that transaction by
// persistSingleTransactionExecutionWork. Every ledger mutation in the batch is therefore
// covered by R-2 already. What is left over is the SUMMARY, and there is no row for it to be
// atomic with: the only candidate would be a batch-spanning transaction, which means
// restructuring the transaction-processing pipeline that AAP §0.6.2 freezes (queue.go,
// transaction_queue.go, transaction_coalescing.go).
//
// This is a statement about what the event IS, not a licence to record it loosely. The
// guarantee is made as strong as it can be rather than left as a log line, and the caller does
// not announce the batch complete until it holds:
//
//   - The capture is SYNCHRONOUS. The caller does not return believing the outcome was
//     recorded while the write is still in flight.
//   - It is RETRIED with bounded backoff, because the realistic failure is a transient
//     database error and a single attempt turned that into permanent loss of the outcome.
//   - Failure is reported as an ERROR RETURN, so callers can act on it, AND it is ESCALATED
//     through notification.NotifyError so that a lost batch summary raises a system.error event
//     exactly as a lost monitor alert does. All THREE unrecoverable exits escalate — a reused
//     event id, a retry abandoned by a cancelled caller, and a fully spent budget — because all
//     three lose the summary; see escalateLostBulkOutcome. Escalating here rather than at the
//     call sites is what makes the two post-commit producers behave identically: one caller
//     discarded this function's error entirely, so a lost summary produced a log line and
//     nothing an alert could fire on.
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

	// THE ATOMIC PATH. When the coordinator record exists, the outcome and its event are
	// written by ONE database transaction, so neither can exist without the other. This is
	// what brings bulk_transaction.<status> under requirement R-2 despite there being no
	// batch-spanning transaction to enrol the event in: the event is atomic with the
	// coordinator's terminal transition instead.
	if l.bulkBatchCoordinationEnabled() {
		err := l.finalizeBulkBatchOutcome(ctx, publishCtx, batchID, status, errorMsg, transactionCount, event)
		if !errors.Is(err, errBulkBatchNotCoordinated) {
			return err
		}

		// The coordinator row is absent, so the atomic finalise is unavailable for this
		// batch. That happens when the row could not be written at batch start — a
		// database fault at exactly that moment — and it is reported at ERROR because the
		// batch has silently lost its atomicity guarantee and an operator should know
		// which batch it was. The outcome is still captured below, with the pre-coordinator
		// behaviour, because a weaker capture is strictly better than none.
		logrus.WithError(err).WithFields(logrus.Fields{
			"batch_id": batchID,
			"status":   status,
		}).Error(
			"the bulk transaction batch has no coordinator record, so its outcome event " +
				"cannot be captured atomically; falling back to a standalone capture, which " +
				"can still be lost if the process dies before it completes",
		)
	}

	// THE FALLBACK AND LEGACY PATH. Reached when coordination is unavailable — either no
	// Kafka broker is configured, in which case this routes to the legacy webhook transport
	// exactly as it did before the event pipeline existed, or the coordinator row is
	// missing. The durable variant is used because the batch outcome is already durable and
	// this insert is its only chance, so a transient database fault must not destroy it.
	return l.PublishEventDurably(publishCtx, event)
}

// bulkBatchCoordinationEnabled reports whether the coordinator record backs this
// deployment's batch outcomes.
//
// It reads the SAME predicate as the outbox capture and the balance-monitor handoff, and
// it must: the coordinator row is inserted at batch start by one code path and finalised by
// another, and if the two disagreed a batch would either be finalised with no row to
// finalise or carry a row nothing ever closes. The condition is "Kafka is configured",
// because a deployment with no broker captures nothing in the outbox at all and its batch
// outcome goes straight down the legacy transport, where there is no event row for the
// coordinator to be atomic with.
//
// Returns:
//   - bool: true when batch outcomes are recorded and finalised atomically.
func (l *Blnk) bulkBatchCoordinationEnabled() bool {
	if l == nil {
		return false
	}

	return l.eventConfiguration().EventPublishingConfigured()
}

// errBulkBatchNotCoordinated marks a batch that has no coordinator record.
//
// It is a distinct sentinel rather than a bare not-found error because the caller must
// treat it differently from every other failure: every other failure means the outcome was
// not captured and must be reported, while this one means the ATOMIC capture is unavailable
// and the weaker one should be attempted. Conflating them would either lose the outcome or
// fall back on failures where falling back is wrong.
var errBulkBatchNotCoordinated = errors.New("the bulk transaction batch has no coordinator record")

// finalizeBulkBatchOutcome records the batch outcome and its event in ONE transaction,
// retrying a transient failure with the SAME prepared event.
//
// # Why the retry re-runs the whole transaction rather than just the insert
//
// The unit of work here is the pair — the terminal transition and the event row — so a
// partial retry would be meaningless. Re-running the whole transaction is safe because the
// transition is guarded on the row still being non-terminal and the event id is DERIVED from
// the batch id: an attempt whose COMMIT succeeded but whose acknowledgement was lost is
// recognised on the next attempt, which finds the batch already finalised with this outcome
// and reports success. That is the difference between an idempotent retry and one that
// records a second, differently-identified event for one batch outcome.
//
// # The three ways this returns
//
//   - nil: the outcome and its event are durable, either written here or already written.
//   - errBulkBatchNotCoordinated: no coordinator row exists, so the caller should fall
//     back to the standalone capture.
//   - anything else: the outcome is NOT captured, and the caller reports it.
//
// A CONFLICT IS NOT RETRIED. It means either the unique index refused the event id — a
// genuine collision, since an identical stored event is reported as success by the
// repository — or the batch is already recorded with a DIFFERENT outcome. Neither is
// resolvable by trying again, and spending the remaining attempts and their backoff on it
// only delays the error the caller needs.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for the retry sleeps only, so a
//     caller going away stops the retry rather than the write.
//   - publishCtx context.Context: the cancellation-detached context the write itself uses.
//   - batchID string: the batch being finalised.
//   - status string: the terminal outcome.
//   - errorMsg string: the failure detail, empty on success.
//   - transactionCount int: the number of transactions in the batch.
//   - event NewWebhook: the outcome event, unchanged from the legacy body.
//
// Returns:
//   - error: as enumerated above.
func (l *Blnk) finalizeBulkBatchOutcome(
	ctx context.Context,
	publishCtx context.Context,
	batchID, status, errorMsg string,
	transactionCount int,
	event NewWebhook,
) error {
	// PREPARED ONCE, OUTSIDE THE LOOP. The row carries the event id, and re-preparing per
	// attempt would be wasted work at best. It is also what makes the derived id visible in
	// the coordinator row: the finalise stores it, so the outcome and the event it produced
	// are joinable afterwards.
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

		if isNotFoundError(lastErr) {
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
//
// ESCALATED, NOT ONLY LOGGED. A lost batch summary is an event that now exists nowhere: there
// is no outbox row to claim, nothing to dead-letter and nothing to replay, so unless it is
// raised the only trace is a log line nobody is alerted on. Routing it through NotifyError
// emits a system.error event, which is the same escalation a lost balance.monitor alert takes
// — so both of the post-commit producers described at PostCommitEventCaptureContract are
// observable through one signal, instead of one of them depending on whether its caller
// happened to inspect a returned error. One caller discarded this function's error outright.
//
// ONE HELPER FOR ALL THREE UNRECOVERABLE EXITS, because all three lose the same thing. A
// reused event id, a retry abandoned by a cancelled caller, and a fully spent budget differ in
// why the outcome is gone, not in whether it is: escalating only the third would have left two
// silent ways to lose a batch summary. The reason is carried in the message so an operator can
// tell them apart without reading this code, and each exit keeps its own specific log line.
//
// There is no recursion risk. NotifyError dispatches system.error through the registered
// sender, whose own failure it logs rather than re-notifying, so an outbox that is refusing
// writes produces one escalation attempt per lost outcome and not a cascade.
//
// Parameters:
//   - batchID string: the batch whose summary was lost, so the escalation names the batch an
//     operator has to reconcile by hand.
//   - status string: the outcome that was being captured — "applied", "inflight" or "failed".
//   - reason string: which of the three exits was taken, in words.
//   - cause error: the last persistence error, wrapped so %w unwrapping still reaches it.
func escalateLostBulkOutcome(batchID, status, reason string, cause error) {
	notification.NotifyError(fmt.Errorf(
		"blnk: the bulk transaction outcome event for batch %s (status %s) was not captured — %s: %w",
		batchID, status, reason, cause,
	))
}

// recordBulkBatchStart writes the coordinator record for an asynchronous batch.
//
// # Why this is a separate function rather than four inline lines
//
// It owns the whole decision — whether coordination applies, what the row says, and what a
// failure means — so the call site reads as one intent and the gate cannot be duplicated
// slightly differently later. The gate is the same predicate the finalise reads, which is
// what stops a batch being finalised against a row that was never written.
//
// # Why the synchronous path does not call it
//
// Only the asynchronous path emits bulk_transaction.<status>: the synchronous path returns
// its outcome in the HTTP response and has always published nothing. A coordinator row there
// would have no event to be atomic with, and minting one would add an event to the catalogue
// that no subscriber has ever received.
//
// Parameters:
//   - ctx context.Context: the caller's context. The write is a single statement and is
//     bounded by whatever deadline the request carries.
//   - batchID string: the batch's parent transaction id.
//   - req *model.BulkTransactionRequest: read for the transaction count and the atomic and
//     inflight flags, which are recorded so a stuck row tells an operator what the batch was
//     attempting — the fact that decides how to finish it by hand.
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
			"the bulk transaction batch coordinator record could not be written, so this batch's " +
				"outcome event cannot be captured atomically with its outcome; the batch proceeds " +
				"and its outcome will be captured on the weaker standalone path",
		)
	}
}

// The retry budget for the bulk outcome finalise.
//
// Three attempts at 200ms, 400ms is deliberately far smaller than the relay's own budget and
// is not trying to be it. This is one short transaction against the local database — a
// guarded UPDATE and one INSERT — retried to survive a transient error such as a momentary
// connection reset or a brief pool exhaustion, and nothing more. A budget large enough to
// ride out a real outage would hold the batch goroutine open for minutes without improving
// the outcome, since a database that is down will still be down. What survives a real outage
// is the coordinator row: the batch stays non-terminal and countable rather than silently
// finished, which is the whole reason the row is written before the batch begins.
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
	// The error is INSPECTED rather than discarded. sendBulkTransactionWebhook escalates an
	// exhausted capture through notification.NotifyError itself, so nothing depends on this
	// branch to be observable, but discarding the value outright said the opposite — that the
	// outcome of the one capture describing a FAILED batch did not matter here.
	if captureErr := l.sendBulkTransactionWebhook(ctx, batchID, "failed", errorMessage, 0); captureErr != nil {
		logrus.WithError(captureErr).WithField("batch_id", batchID).Error(
			"the failed-batch outcome event was not captured; the rollback status above is the only " +
				"record of this batch's outcome",
		)
	}
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

		// THE COORDINATOR RECORD, WRITTEN BEFORE ANY MEMBER TRANSACTION RUNS.
		//
		// This is what gives the batch's outcome somewhere durable to be. Without it the
		// outcome existed only in the local variables of the goroutine below: a failed
		// capture, or a process that died, destroyed the summary permanently and left no
		// automatic way to reconstruct it. With it, the outcome and its event are written by
		// one transaction (see finalizeBulkBatchOutcome), so neither can exist without the
		// other — and a crash before that transaction leaves a row that says "this batch
		// began and never reported an outcome", which is countable rather than invisible.
		//
		// ORDER MATTERS: before the goroutine, not inside it. A row written concurrently with
		// the processing could lose the race against a batch that finishes immediately, and
		// the finalise would then find no row to finalise.
		//
		// A FAILURE HERE DOES NOT REFUSE THE BATCH. The insert hits the same database the
		// member transactions are about to use, so a fault here means the batch is going to
		// fail anyway on its own terms, and refusing it for a bookkeeping write would turn a
		// recoverable database blip into a rejected ledger request. It is logged at ERROR and
		// the finalise falls back to the standalone capture, which is the pre-coordinator
		// behaviour.
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
				// synchronous and its result decides what is announced, because a completion
				// line written over an unrecorded outcome is how a missing summary stays
				// invisible: the batch reads as finished in the log while no subscriber will
				// ever be told it finished.
				//
				// The batch itself succeeded either way and its per-transaction events were
				// captured inside their own transactions, so this is a missing SUMMARY rather
				// than a missing ledger event — and the goroutine has nobody to return an
				// error to, which is precisely why the distinction has to be visible here.
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
