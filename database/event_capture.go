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
	"sync"

	"github.com/blnkfinance/blnk/model"
)

// event_capture.go lets the atomic writers capture an event row INSIDE the transaction
// that commits the mutation it describes, so the mutation and its event commit or roll
// back together.
//
// It holds TWO registered captures, for the two producers whose row cannot be handed in
// by the caller:
//
//   - TransactionEventCapture — the transaction.* event for a COALESCED batch, whose
//     caller assembles its argument list in a frozen file and so cannot supply rows.
//   - BalanceMonitorAlertCapture — the balance.monitor alert for a balance the
//     transaction moved, decided against the monitor definitions read in that same
//     transaction.

// TransactionEventCapture builds the event-outbox row that describes one transaction,
// for a writer that is about to commit it.
//
// It is called INSIDE the atomic writer and BEFORE the commit, once per transaction,
// and it must not perform I/O: everything it needs is already in hand, and a network
// call here would hold a database transaction open across it. The registered
// implementation marshals a payload and returns.
//
// Parameters:
//   - ctx context.Context: the writer's context, for tracing only.
//   - txn *model.Transaction: the finalized transaction about to be committed.
//   - ledgerID string: the ledger the transaction belongs to, resolved from the balance
//     set the writer is updating, or "" when no balance named one.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not
//     configured.
//   - error: only a real capture failure, such as a payload that will not serialise.
type TransactionEventCapture func(ctx context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error)

var (
	transactionEventCaptureMu sync.RWMutex
	transactionEventCapture   TransactionEventCapture
)

// RegisterTransactionEventCapture installs the process-wide transaction event capture
// used by the atomic batch writer.
//
// Called once from the service constructor, in the same place and for the same reason
// as notification.RegisterWebhookSender. Passing nil clears the registration, which is
// what a test that must observe the pre-registration behaviour uses to restore it.
//
// Parameters:
//   - capture TransactionEventCapture: the capture to install, or nil to clear it.
func RegisterTransactionEventCapture(capture TransactionEventCapture) {
	transactionEventCaptureMu.Lock()
	defer transactionEventCaptureMu.Unlock()

	transactionEventCapture = capture
}

// registeredTransactionEventCapture returns the installed capture, or nil when none is.
//
// Returns:
//   - TransactionEventCapture: the current capture, nil when unregistered.
func registeredTransactionEventCapture() TransactionEventCapture {
	transactionEventCaptureMu.RLock()
	defer transactionEventCaptureMu.RUnlock()

	return transactionEventCapture
}

// BalanceMonitorAlertCapture builds the canonical balance.monitor event-outbox row for
// ONE crossing: one monitor, on one balance, as the writer is about to commit it.
//
// This replaces a deferred conversion. The canonical event therefore did not exist
// until that second transaction succeeded.
//
// Parameters:
//   - ctx context.Context: the writer's context, for tracing only.
//   - balance *model.Balance: the balance in its POST-mutation state, exactly as the
//     transaction is writing it.
//   - monitor model.BalanceMonitor: the monitor whose condition that balance met.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not
//     configured.
//   - error: only a real capture failure, such as a payload that will not serialise or
//     a partition key that cannot be resolved.
type BalanceMonitorAlertCapture func(ctx context.Context, balance *model.Balance, monitor model.BalanceMonitor) (*model.EventOutbox, error)

var (
	balanceMonitorAlertCaptureMu sync.RWMutex
	balanceMonitorAlertCapture   BalanceMonitorAlertCapture
)

// RegisterBalanceMonitorAlertCapture installs the process-wide balance monitor alert
// capture used by the atomic writers.
//
// Called once from the service constructor, beside RegisterTransactionEventCapture and
// for the same structural reason. Passing nil clears the registration, which is what a
// test asserting the handoff fallback uses to reach it.
//
// Parameters:
//   - capture BalanceMonitorAlertCapture: the capture to install, or nil to clear it.
func RegisterBalanceMonitorAlertCapture(capture BalanceMonitorAlertCapture) {
	balanceMonitorAlertCaptureMu.Lock()
	defer balanceMonitorAlertCaptureMu.Unlock()

	balanceMonitorAlertCapture = capture
}

// registeredBalanceMonitorAlertCapture returns the installed capture, or nil when none is.
//
// Returns:
//   - BalanceMonitorAlertCapture: the current capture, nil when unregistered.
func registeredBalanceMonitorAlertCapture() BalanceMonitorAlertCapture {
	balanceMonitorAlertCaptureMu.RLock()
	defer balanceMonitorAlertCaptureMu.RUnlock()

	return balanceMonitorAlertCapture
}

// ledgerIDsByBalanceID indexes a balance set by balance id so the writer can resolve
// each transaction's ledger with a map lookup instead of a scan per transaction.
//
// Balances carrying no ledger id are omitted rather than indexed to an empty string, so
// a lookup miss and a blank ledger are the same answer and the caller needs one branch.
//
// Parameters:
//   - balances []*model.Balance: the balance set the writer is updating.
//
// Returns:
//   - map[string]string: balance id to ledger id, for every balance that names one.
func ledgerIDsByBalanceID(balances []*model.Balance) map[string]string {
	ledgers := make(map[string]string, len(balances))
	for _, balance := range balances {
		if balance == nil || balance.BalanceID == "" || balance.LedgerID == "" {
			continue
		}
		ledgers[balance.BalanceID] = balance.LedgerID
	}

	return ledgers
}

// transactionLedgerIDFromSet resolves the ledger of one transaction from an indexed
// balance set.
//
// Parameters:
//   - txn *model.Transaction: the transaction to resolve. Never nil.
//   - ledgers map[string]string: balance id to ledger id, from ledgerIDsByBalanceID.
//
// Returns:
//   - string: the ledger id, or "" when neither balance named one.
func transactionLedgerIDFromSet(txn *model.Transaction, ledgers map[string]string) string {
	if ledger, ok := ledgers[txn.Source]; ok {
		return ledger
	}

	return ledgers[txn.Destination]
}
