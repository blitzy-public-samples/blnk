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
	"strings"

	"github.com/blnkfinance/blnk/internal/hotpairs"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// RejectTransaction records a transaction as REJECTED together with the event that announces
// it, atomically.
//
// # The event is prepared before persistence and committed with the status mutation
//
// The transaction.rejected event used to be captured after RecordTransaction had already
// committed — and captured TWICE, because the worker's rejection handler published it a second
// time on its own. Both halves were wrong in the same way: a crash between the commit and the
// capture left a transaction recorded as rejected with nothing telling any subscriber it had
// been, and the duplicate insert relied on the event-id unique index to refuse it, which made a
// logged conflict part of normal operation.
//
// The row is now prepared here, before anything is persisted, and handed to RecordTransaction,
// which commits it in the same database transaction as the REJECTED row (requirement R-2).
// postTransactionActions is then told the event is already captured, so it does not insert it
// again, and cmd/workers.go no longer publishes its own copy.
//
// Nothing about the event itself changed: getEventFromStatus(StatusRejected) is the same
// "transaction.rejected" string, and the payload is the same transaction object the legacy
// webhook carried, with the rejection reason already written into its metadata — so the bytes
// recorded are the bytes that used to be the HTTP body.
//
// NO LEDGER IS CLAIMED. A rejected transaction never loaded its balances — that is usually WHY
// it was rejected — so this call site does not know the ledger, and inventing one would be
// worse than storing none. The partition key falls back to the transaction's own source and
// destination through PrepareEventOutbox's documented chain, which still gives one
// transaction's events a stable partition.
//
// A payload that will not serialise fails the rejection rather than being swallowed. Nothing
// has been persisted at that point, so the caller's retry is safe, and the alternative is a
// rejection nobody is told about.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - transaction *model.Transaction: the transaction to reject. Its status and metadata are
//     mutated before persistence.
//   - reason string: the free-text rejection reason, recorded in metadata and categorised for
//     the rejection metric.
//
// Returns:
//   - *model.Transaction: the persisted, rejected transaction.
//   - error: a typed error if the event could not be prepared or the persistence failed.
func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "RejectTransaction")
	defer span.End()

	// Update the transaction status to rejected
	transaction.Status = StatusRejected

	// Initialize MetaData if it's nil and add the rejection reason
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	// Prepared BEFORE persistence, so the writer can commit it with the status mutation. A nil
	// row means publishing is not configured, and RecordTransaction's variadic tail accepts
	// that as "no event", so an unconfigured deployment takes exactly the path it did before.
	eventOutbox, err := l.PrepareEventOutbox(ctx, NewWebhook{
		Event:   getEventFromStatus(transaction.Status),
		Payload: transaction,
	})
	if err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithField("transaction_id", transaction.TransactionID).
			Error("failed to prepare the rejection event; the rejection was not persisted")
		return nil, err
	}

	// Persist the transaction with the updated status and metadata, and its event, atomically
	transaction, err = l.datasource.RecordTransaction(ctx, transaction, eventOutbox)
	if err != nil {
		span.RecordError(err)
		logrus.WithError(err).Error("failed to save transaction to db")
		return nil, err
	}

	span.AddEvent("Transaction rejected", trace.WithAttributes(attribute.String("transaction.id", transaction.TransactionID)))

	// Record rejection metrics.
	rejectionReason := categorizeRejectionReason(reason)
	metrics.TransactionRejectedTotal.Add(ctx, 1,
		otelmetric.WithAttributes(attribute.String("reason", rejectionReason)),
	)
	metrics.TransactionTotal.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("status", StatusRejected),
			attribute.String("currency", transaction.Currency),
		),
	)

	if transaction.Atomic {
		logrus.Info(transaction.ParentTransaction, "parent transaction", transaction.Atomic, "atomic", transaction.Inflight, "inflight")
		parentTransactionID, ok := transaction.MetaData["QUEUED_PARENT_TRANSACTION"].(string)
		if !ok {
			// Note the consequence of capturing atomically: the rejection event is ALREADY
			// durable at this point, so it is delivered even though this function returns an
			// error. Under the previous ordering the event was produced after this line and
			// a malformed batch reference suppressed it. Delivering it is the more honest
			// outcome — the rejection genuinely happened and is committed — and it is what
			// keeps the outbox's count reconcilable against the transactions table.
			return nil, fmt.Errorf("parent transaction ID not found in meta data")
		}
		l.handleAsyncBulkTransactionFailure(ctx, errors.New("transaction rejected"), parentTransactionID, transaction.Atomic, transaction.Inflight)
	}
	// For rejected transactions, no balances were updated, so pass nil.
	//
	// eventCaptured REPORTS THE ROW ABOVE, and it has to be derived rather than stated.
	// RecordTransaction takes the event row and, when given one, inserts it inside the
	// transaction that inserts the rejection — so on a deployment with publishing
	// configured the event is ALREADY durable here, and telling postTransactionActions
	// otherwise would make it publish a second time. The event id is derived from the
	// transaction's identity, so that second insert is refused by the unique index: the
	// duplicate is suppressed, but the conflict is reported through
	// notification.NotifyError, which emits a system.error of its own for every rejection
	// Blnk processes. A stream of spurious errors about a rejection that succeeded.
	//
	// eventOutbox is nil exactly when nothing was captured — an unconfigured deployment,
	// or a webhook-only one — and false is then correct, because the post-commit branch is
	// what delivers the event over the legacy transport on those deployments.
	//
	// This remains the SINGLE producer of transaction.rejected either way: the worker's
	// rejection handler deliberately does not publish it again, for the same
	// derived-id reason.
	l.postTransactionActions(ctx, transaction, nil, nil, eventOutbox != nil)
	return transaction, nil
}

// prepareRejectionEventOutbox builds the transaction.rejected event row for a transaction
// that is about to be recorded as REJECTED.
//
// The event name is derived through getEventFromStatus from the status this function's
// caller has already set, rather than written as a literal, so the rejection event stays
// spelled the way every other transaction event is spelled and cannot drift from the
// status-to-event table.
//
// A preparation failure is logged and yields nil, which degrades to post-commit capture
// rather than refusing the rejection. The transaction has already failed for a business
// reason; refusing to record that because its notification would not serialise would leave
// the caller with neither an outcome nor a reason.
//
// Parameters:
//   - ctx context.Context: the context for the operation; used for tracing only.
//   - transaction *model.Transaction: the transaction, status already set to REJECTED.
//
// Returns:
//   - *model.EventOutbox: the row to commit with the transaction, or nil when publishing is
//     unconfigured or the row could not be prepared.
func (l *Blnk) prepareRejectionEventOutbox(ctx context.Context, transaction *model.Transaction) *model.EventOutbox {
	row, err := l.PrepareEventOutbox(ctx, NewWebhook{
		Event:   getEventFromStatus(transaction.Status),
		Payload: transaction,
	})
	if err != nil {
		logrus.WithError(err).WithField("transaction_id", transaction.TransactionID).
			Error("failed to prepare the rejection event for atomic capture; it will be captured after the commit instead")

		return nil
	}

	return row
}

// categorizeRejectionReason maps a free-text rejection reason to a bounded set of metric labels
// to keep Prometheus cardinality under control.
func categorizeRejectionReason(reason string) string {
	lower := strings.ToLower(reason)
	switch {
	case strings.Contains(lower, "insufficient funds"):
		return "insufficient_funds"
	case strings.Contains(lower, "overdraft limit"):
		return "overdraft_limit"
	case hotpairs.IsLockContentionError(errors.New(reason)):
		return "lock_contention"
	case strings.Contains(lower, "exceeded max"):
		return "max_retries"
	default:
		return "other"
	}
}
