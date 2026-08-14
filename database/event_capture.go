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
	"errors"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/model"
)

// event_capture.go lets the atomic writers capture an event row INSIDE the transaction
// that commits the mutation it describes, so the mutation and its event commit or roll
// back together.

// TransactionEventCapture builds the event-outbox row that describes one transaction,
// for a writer that is about to commit it.
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
// Parameters:
//   - capture TransactionEventCapture: the capture to install, or nil to clear it.
func RegisterTransactionEventCapture(capture TransactionEventCapture) {
	transactionEventCaptureMu.Lock()
	defer transactionEventCaptureMu.Unlock()

	transactionEventCapture = capture
}

// registeredTransactionEventCapture returns the installed capture, or nil when none is.
func registeredTransactionEventCapture() TransactionEventCapture {
	transactionEventCaptureMu.RLock()
	defer transactionEventCaptureMu.RUnlock()

	return transactionEventCapture
}

// BalanceMonitorAlertCapture builds the canonical balance.monitor event-outbox row for
// ONE crossing: one monitor, on one balance, as the writer is about to commit it.
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
// Parameters:
//   - capture BalanceMonitorAlertCapture: the capture to install, or nil to clear it.
func RegisterBalanceMonitorAlertCapture(capture BalanceMonitorAlertCapture) {
	balanceMonitorAlertCaptureMu.Lock()
	defer balanceMonitorAlertCaptureMu.Unlock()

	balanceMonitorAlertCapture = capture
}

// registeredBalanceMonitorAlertCapture returns the installed capture, or nil when none is.
func registeredBalanceMonitorAlertCapture() BalanceMonitorAlertCapture {
	balanceMonitorAlertCaptureMu.RLock()
	defer balanceMonitorAlertCaptureMu.RUnlock()

	return balanceMonitorAlertCapture
}

// ledgerIDsByBalanceID indexes a balance set by balance id so the writer can resolve
// each transaction's ledger with a map lookup instead of a scan per transaction.
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
func transactionLedgerIDFromSet(txn *model.Transaction, ledgers map[string]string) string {
	if ledger, ok := ledgers[txn.Source]; ok {
		return ledger
	}

	return ledgers[txn.Destination]
}

// handoffLifecycleEventType names the lifecycle event of a transaction that is being
// persisted on its OWN, without a balance movement beside it — which is to say a
// transaction the queue has accepted but not yet executed.
//
// Those are the only two statuses this file captures for. Every other status belongs to a
// transaction that moved money, and its event is captured by the atomic writer alongside
// the balance updates; capturing it here as well would attempt a second row under the
// same derived event id, and blnk.event_outbox's unique index on event_id would then roll
// back the ledger write itself. The guard is therefore load-bearing rather than tidy.
//
// Parameters:
//   - txn *model.Transaction: the transaction about to be recorded. May be nil.
//
// Returns:
//   - string: the event name to capture, or "" when this transaction's event belongs to
//     another writer.
func handoffLifecycleEventType(txn *model.Transaction) string {
	if txn == nil {
		return ""
	}

	switch eventType := model.EventTypeForTransactionStatus(txn.Status); eventType {
	case model.EventTypeTransactionQueued, model.EventTypeTransactionScheduled:
		return eventType
	default:
		return ""
	}
}

// captureHandoffTransactionEvent builds the event-outbox row for a transaction being
// recorded in the QUEUED or SCHEDULED state, so the queue's acceptance of it is announced
// with the same durability as its eventual execution.
//
// Parameters:
//   - ctx context.Context: the writer's context. Used for tracing and for the ledger
//     lookup below.
//   - txn *model.Transaction: the transaction about to be recorded.
//
// Returns:
//   - *model.EventOutbox: the row to commit with the transaction, or nil when this
//     transaction's event is another writer's to capture or event publishing is
//     unconfigured.
//   - error: a real capture failure, such as a payload that will not serialise. The
//     caller must refuse the write rather than commit a movement no subscriber is told
//     about — the posture persistSingleTransactionExecutionWork already takes.
func (d Datasource) captureHandoffTransactionEvent(ctx context.Context, txn *model.Transaction) (*model.EventOutbox, error) {
	if handoffLifecycleEventType(txn) == "" {
		return nil, nil
	}

	capture := registeredTransactionEventCapture()
	if capture == nil {
		return nil, nil
	}

	return capture(ctx, txn, d.transactionLedgerID(ctx, txn))
}

// transactionLedgerID resolves the ledger a transaction belongs to by reading it off the
// transaction's own balances.
//
// The atomic writers do not need this: they are already holding the balance rows they are
// updating, so they resolve the ledger from the set in memory. A transaction recorded on
// its own has no such set — the balances were fetched while it was being prepared and
// discarded — and the ledger is what the event's partition key and ledger_id column are
// built from, so a lookup is the only way to key the event on the dimension its type
// declares. One indexed read, on the acceptance path rather than the relay's, and a miss
// is not an error: the capture then keys the event on its aggregate instead.
//
// Parameters:
//   - ctx context.Context: cancels the lookup.
//   - txn *model.Transaction: the transaction whose ledger is wanted.
//
// Returns:
//   - string: the ledger id, preferring the source balance's, or "" when neither balance
//     names one.
func (d Datasource) transactionLedgerID(ctx context.Context, txn *model.Transaction) string {
	if txn == nil {
		return ""
	}

	source := strings.TrimSpace(txn.Source)
	destination := strings.TrimSpace(txn.Destination)
	if source == "" && destination == "" {
		return ""
	}

	var ledgerID sql.NullString

	// ORDERED SO THE SOURCE WINS, matching transactionLedgerIDFromSet's preference above:
	// the two resolutions must agree, or one transaction's queued event and its applied
	// event would be keyed on different ledgers and could be published out of order
	// against each other.
	err := d.Conn.QueryRowContext(ctx, `
		SELECT balance.ledger_id
		FROM blnk.balances balance
		WHERE balance.balance_id IN ($1, $2)
		ORDER BY CASE WHEN balance.balance_id = $1 THEN 0 ELSE 1 END
		LIMIT 1
	`, source, destination).Scan(&ledgerID)
	if err != nil {
		// A miss is the ordinary outcome for an internal balance reference that names no
		// stored row, so it is not reported as a fault. Logged at debug so a systematic miss
		// is still discoverable.
		if !errors.Is(err, sql.ErrNoRows) {
			logrus.WithField("cause", databaseErrorClass(err)).Debug(
				"the ledger of a queued transaction could not be read, so its event is keyed on " +
					"its aggregate rather than its ledger")
		}

		return ""
	}

	return strings.TrimSpace(ledgerID.String)
}
