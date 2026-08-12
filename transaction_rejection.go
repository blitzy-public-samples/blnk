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

// RejectTransaction records a transaction as REJECTED together with the event that
// announces it, atomically.
//
// The status change and the transaction.rejected event are written in ONE database
// transaction, so a crash can never leave a transaction recorded as rejected with
// nothing telling a subscriber it was. The event is prepared once and reused across
// retries, so a retried write cannot lean on the event-id unique index to deduplicate
// and turn a logged conflict into part of normal operation.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - transaction *model.Transaction: the transaction to reject. Its status and
//     metadata are mutated before persistence.
//   - reason string: the free-text rejection reason, recorded in metadata and
//     categorised for the rejection metric.
//
// Returns:
//   - *model.Transaction: the persisted, rejected transaction.
//   - error: a typed error if the event could not be prepared or the persistence
//     failed.
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

	// Prepared BEFORE persistence, so the writer can commit it with the status mutation. A
	// nil row means publishing is not configured, and RecordTransaction's variadic tail
	// accepts that as "no event", so an unconfigured deployment takes exactly the path it
	// did before.
	eventOutbox, err := l.PrepareEventOutbox(ctx, NewWebhook{
		Event:   getEventFromStatus(transaction.Status),
		Payload: transaction,
	}, WithEventLedgerID(l.transactionRejectionLedgerID(transaction)))
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
			// error. Under the previous ordering the event was produced after this line and a
			// malformed batch reference suppressed it. Delivering it is the more honest outcome
			// — the rejection genuinely happened and is committed — and it is what keeps the
			// outbox's count reconcilable against the transactions table.
			return nil, fmt.Errorf("parent transaction ID not found in meta data")
		}
		l.handleAsyncBulkTransactionFailure(ctx, errors.New("transaction rejected"), parentTransactionID, transaction.Atomic, transaction.Inflight)
	}
	// For rejected transactions, no balances were updated, so pass nil.
	l.postTransactionActions(ctx, transaction, nil, nil, eventOutbox != nil)
	return transaction, nil
}

// transactionRejectionLedgerID resolves the ledger a rejected transaction belongs to,
// so its event is keyed on the same dimension as every other event in that
// transaction's lifecycle.
//
// The SOURCE balance is preferred and the destination is the fallback, matching
// transactionLedgerID in transaction_execution.go so that the two producers of one
// transaction's events cannot disagree about which balance names the ledger.
//
// Parameters:
//   - transaction *model.Transaction: the transaction being rejected.
//
// Returns:
//   - string: the ledger id, or the empty string when it cannot be resolved.
func (l *Blnk) transactionRejectionLedgerID(transaction *model.Transaction) string {
	if l == nil || l.datasource == nil || transaction == nil {
		return ""
	}

	for _, balanceID := range []string{transaction.Source, transaction.Destination} {
		trimmed := strings.TrimSpace(balanceID)
		if trimmed == "" {
			continue
		}

		balance, err := l.datasource.GetBalanceByIDLite(trimmed)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"transaction_id": transaction.TransactionID,
				"balance_id":     trimmed,
			}).Debug(
				"could not resolve the ledger of a rejected transaction from its balance; the " +
					"rejection event will be keyed on the balance instead of the ledger",
			)

			continue
		}

		if balance != nil && strings.TrimSpace(balance.LedgerID) != "" {
			return strings.TrimSpace(balance.LedgerID)
		}
	}

	return ""
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
