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

// event_capture.go lets the atomic writers capture an event row INSIDE the transaction that
// commits the mutation it describes, which is what requirement R-2 asks for.
//
// It holds TWO registered captures, for the two producers whose row cannot be handed in by
// the caller:
//
//   - TransactionEventCapture — the transaction.* event for a COALESCED batch, whose caller
//     assembles its argument list in a frozen file and so cannot supply rows.
//   - BalanceMonitorAlertCapture — the balance.monitor alert for a balance the transaction
//     moved, decided against the monitor definitions read in that same transaction.
//
// # Why a registered callback and not a parameter
//
// The batch writer's only caller is the coalescing path, and that path builds its argument
// list without event rows. Adding them there is the obvious fix and it is not available: the
// coalescing file belongs to the transaction-processing pipeline the plan freezes, so the
// event has to be captured from the layer BELOW the caller rather than by the caller. The
// monitor alert has that problem twice over: the coalesced caller cannot supply it, and a
// balance whose monitors the caller could not READ yields no alert to supply on any path.
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

// BalanceMonitorAlertCapture builds the canonical balance.monitor event-outbox row for ONE
// crossing: one monitor, on one balance, as the writer is about to commit it.
//
// # Why the writer needs this to satisfy R-2 for balance.monitor
//
// A balance monitor alert has two decision inputs — the balance as written, and the monitor
// definitions in force — and R-2 requires the row announcing the crossing to commit with the
// mutation that caused it. Both inputs are available inside the writer's transaction: the
// balance because the writer just updated it, the definitions because selectBalanceMonitorsInTx
// reads them there. What the writer cannot do is BUILD the row. That needs the legacy webhook
// envelope, the partition-key and aggregate resolution and the topic binding that own the
// payload contract, and all three live in the root package, which imports this one. So the
// writer evaluates and this callback constructs, and the row lands inside the transaction.
//
// This replaces a deferred conversion. The writer used to commit a balance_monitor_handoff row
// — an intent carrying both inputs, durable and inseparable from the movement, but not the
// blnk.event_outbox row R-2 names — and a second transaction, run by
// BalanceMonitorHandoffProcessor, turned it into one. The canonical event therefore did not
// exist until that second transaction succeeded. With a capture registered the canonical row is
// written in the first transaction and there is no conversion to wait for; the handoff remains
// as the path for rows written before this change and for a process with no capture registered.
//
// # The contract this function must honour
//
// It runs while a database transaction is open, holding that transaction's balance locks, so it
// must do NO I/O and must not block — the registered implementation marshals a payload and
// reads process configuration, and nothing else. It is called once per CROSSING, not once per
// balance: the writer has already applied model.BalanceMonitor.CheckCondition and only calls
// this for a condition that is met.
//
// Parameters:
//   - ctx context.Context: the writer's context, for tracing only.
//   - balance *model.Balance: the balance in its POST-mutation state, exactly as the
//     transaction is writing it. Never nil.
//   - monitor model.BalanceMonitor: the monitor whose condition that balance met. It is the
//     event's payload, which is what makes the row identical to the one the caller-side pass
//     and the handoff drain produce for the same crossing.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not configured.
//     Nil is legitimate and not an error; the writer inserts nothing and commits the movement,
//     which is the no-op-when-unconfigured contract inherited from SendWebhook.
//   - error: only a real capture failure, such as a payload that will not serialise or a
//     partition key that cannot be resolved. The writer treats it as fatal and abandons the
//     mutation, for the same reason TransactionEventCapture's error is fatal.
type BalanceMonitorAlertCapture func(ctx context.Context, balance *model.Balance, monitor model.BalanceMonitor) (*model.EventOutbox, error)

var (
	balanceMonitorAlertCaptureMu sync.RWMutex
	balanceMonitorAlertCapture   BalanceMonitorAlertCapture
)

// RegisterBalanceMonitorAlertCapture installs the process-wide balance monitor alert capture
// used by the atomic writers.
//
// Called once from the service constructor, beside RegisterTransactionEventCapture and for the
// same structural reason. Passing nil clears the registration, which is what a test asserting
// the handoff fallback uses to reach it.
//
// The last registration wins, which is correct for the only case that exists — one service per
// process — and harmless otherwise, because every registered implementation builds the same row
// from the same crossing.
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
