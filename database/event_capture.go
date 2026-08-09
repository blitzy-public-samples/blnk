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

// event_capture.go lets the atomic batch writer capture a transaction's ledger event INSIDE
// the transaction that commits the mutation, which is what requirement R-2 asks for.
//
// # Why a registered callback and not a parameter
//
// The batch writer's only caller is the coalescing path, and that path builds its argument
// list without event rows. Adding them there is the obvious fix and it is not available: the
// coalescing file belongs to the transaction-processing pipeline the plan freezes, so the
// event has to be captured from the layer BELOW the caller rather than by the caller.
//
// The writer cannot build the row itself either. Doing so needs the legacy webhook envelope,
// the status-to-event-name mapping and the outbox construction that own the payload contract,
// and all three live in the root package, which imports this one. A callback registered
// downward is the only direction that compiles.
//
// This mirrors internal/notification.RegisterWebhookSender, which solves the identical
// problem for system.error: a lower layer that must reach a higher one, resolved by the
// higher layer registering a function at construction. Following the established shape keeps
// one pattern in the codebase for this rather than two.
//
// # What registration does and does not change
//
// A process with no capture registered behaves exactly as before — the writer inserts the
// rows it was handed and nothing more. That is what keeps every existing test, and every
// deployment that constructs a Datasource directly, working untouched.
//
// The capture is process-wide because the Datasource is: database.GetDBConnection returns one
// shared instance behind a sync.Once, so per-instance state would be shared state wearing a
// different name. Access is guarded by a read-write mutex, and reads outnumber writes by the
// transaction rate to one, so the write side is a single call during construction.

// TransactionEventCapture builds the event-outbox row that describes one transaction, for a
// writer that is about to commit it.
//
// It is called INSIDE the atomic writer and BEFORE the commit, once per transaction, and it
// must not perform I/O: everything it needs is already in hand, and a network call here would
// hold a database transaction open across it. The registered implementation marshals a
// payload and returns.
//
// Parameters:
//   - ctx context.Context: the writer's context, for tracing only.
//   - txn *model.Transaction: the finalized transaction about to be committed. Never nil.
//   - ledgerID string: the ledger the transaction belongs to, resolved from the balance set
//     the writer is updating, or "" when no balance named one. Empty is passed through rather
//     than substituted, so the row records what was actually known.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not configured.
//     Nil is a legitimate, expected answer and is not an error — it is the same
//     no-op-when-unconfigured contract the pipeline inherited from SendWebhook.
//   - error: only a real capture failure, such as a payload that will not serialise. The
//     writer treats it as fatal and abandons the mutation, because a mutation that commits
//     without its event is precisely what R-2 exists to prevent.
type TransactionEventCapture func(ctx context.Context, txn *model.Transaction, ledgerID string) (*model.EventOutbox, error)

var (
	transactionEventCaptureMu sync.RWMutex
	transactionEventCapture   TransactionEventCapture
)

// RegisterTransactionEventCapture installs the process-wide transaction event capture used by
// the atomic batch writer.
//
// Called once from the service constructor, in the same place and for the same reason as
// notification.RegisterWebhookSender. Passing nil clears the registration, which is what a
// test that must observe the pre-registration behaviour uses to restore it.
//
// The last registration wins. Two different services in one process would therefore share the
// most recently registered capture, which is correct for the only case that exists — one
// service per process — and harmless for the case that does not, because every registered
// implementation builds the same row from the same transaction.
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

// ledgerIDsByBalanceID indexes a balance set by balance id so the writer can resolve each
// transaction's ledger with a map lookup instead of a scan per transaction.
//
// Balances carrying no ledger id are omitted rather than indexed to an empty string, so a
// lookup miss and a blank ledger are the same answer and the caller needs one branch.
//
// Parameters:
//   - balances []*model.Balance: the balance set the writer is updating. May contain nils.
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

// transactionLedgerIDFromSet resolves the ledger of one transaction from an indexed balance
// set.
//
// THE SOURCE WINS AND THE DESTINATION IS THE FALLBACK, which is the same rule the execution
// path applies in transactionLedgerID. That agreement is the point rather than a coincidence:
// an event captured here and the same event captured on the post-commit fallback path must
// carry the same ledger and therefore hash to the same partition, or one transaction's events
// would order against each other differently depending on which path recorded them.
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
