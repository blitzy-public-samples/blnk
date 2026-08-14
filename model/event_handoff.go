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

package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// BalanceMonitorHandoff is one row of blnk.balance_monitor_handoff: the durable,
// self-contained record that a balance moved and its monitors have not been evaluated
// yet, carrying both inputs that evaluation depends on.
type BalanceMonitorHandoff struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database.
	ID int64 `json:"id"`
	// HandoffID is the business key, and it carries a unique index.
	HandoffID string `json:"handoff_id"`
	// BalanceID is the balance whose monitors are to be evaluated.
	BalanceID string `json:"balance_id"`
	// LedgerID is the ledger the balance belongs to, carried so the resulting event can be
	// attributed to it without a second read. A balance always has one, so this is empty
	// only on a row written from an incomplete snapshot.
	LedgerID string `json:"ledger_id,omitempty"`
	// BalanceSnapshot is the balance as the transaction wrote it, marshalled.
	BalanceSnapshot json.RawMessage `json:"balance_snapshot"`
	// MonitorSnapshot is the monitor definitions in force when that transaction committed,
	// read inside it and marshalled as a JSON array.
	MonitorSnapshot json.RawMessage `json:"monitor_snapshot,omitempty"`
	// Status is the relay state machine: pending, processing, completed or failed. The
	// values are the OutboxStatus* constants, shared with the two existing outboxes rather
	// than duplicated, so one vocabulary describes all three.
	Status string `json:"status"`
	// Attempts is how many evaluation attempts this row has had.
	Attempts int `json:"attempts"`
	// MaxAttempts is the budget beyond which the row is marked failed.
	MaxAttempts int `json:"max_attempts"`
	// LastError is the most recent failure reason, empty when there has been none.
	LastError string `json:"last_error,omitempty"`
	// EventsCaptured is how many balance.monitor events the completed evaluation produced.
	EventsCaptured int `json:"events_captured"`
	// CreatedAt is when the balance's transaction committed this row.
	CreatedAt time.Time `json:"created_at"`
	// ProcessedAt is when the row reached a terminal status, nil before that.
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	// LockedUntil is the claim lease expiry, nil when unclaimed.
	LockedUntil *time.Time `json:"locked_until,omitempty"`
}

// Balance decodes the snapshot this handoff carries.
//
// Returns:
//   - *Balance: the decoded snapshot.
//   - error: a decode failure, which means the row is not evaluable and should be
//     failed rather than retried; re-reading the same bytes cannot succeed later.
func (h *BalanceMonitorHandoff) Balance() (*Balance, error) {
	if h == nil || len(h.BalanceSnapshot) == 0 {
		return nil, fmt.Errorf("balance monitor handoff carries no balance snapshot")
	}

	balance := &Balance{}
	if err := json.Unmarshal(h.BalanceSnapshot, balance); err != nil {
		return nil, fmt.Errorf("failed to decode the balance snapshot: %w", err)
	}

	return balance, nil
}

// Monitors decodes the monitor definitions this handoff carries.
//
// Returns:
//   - []BalanceMonitor: the snapshotted definitions, nil when none was carried.
//   - bool: true when this row carries a snapshot at all.
//   - error: a decode failure, which means the row is not evaluable and should be
//     failed rather than retried; re-reading the same bytes cannot succeed later.
func (h *BalanceMonitorHandoff) Monitors() ([]BalanceMonitor, bool, error) {
	if h == nil || len(h.MonitorSnapshot) == 0 {
		return nil, false, nil
	}

	// A JSON `null` is a carried snapshot that decodes to nothing, and it is distinguished
	// here because json.Unmarshal into a slice leaves it nil without error — which would
	// otherwise be indistinguishable from a decode that produced an empty array.
	if string(bytes.TrimSpace(h.MonitorSnapshot)) == "null" {
		return nil, true, nil
	}

	monitors := []BalanceMonitor{}
	if err := json.Unmarshal(h.MonitorSnapshot, &monitors); err != nil {
		return nil, true, fmt.Errorf("failed to decode the balance monitor snapshot: %w", err)
	}

	return monitors, true, nil
}

// balanceMonitorHandoffPrefix is the identifier prefix for a handoff, following the
// repository's `<module>_<uuid>` convention so an id is self-describing in a log line.
const balanceMonitorHandoffPrefix = "bmh"

// PrepareBalanceMonitorHandoffs builds one handoff per supplied balance that HAS a
// monitor.
//
// Parameters:
//   - balances []*Balance: the balances being updated, in post-mutation state.
//   - monitors map[string][]BalanceMonitor: the monitor definitions read inside the
//     caller's transaction, keyed by balance id.
//
// Returns:
//   - []*BalanceMonitorHandoff: one handoff per monitored balance, in input order.
//   - error: a marshal failure on any balance or monitor set.
func PrepareBalanceMonitorHandoffs(
	balances []*Balance,
	monitors map[string][]BalanceMonitor,
) ([]*BalanceMonitorHandoff, error) {
	if len(balances) == 0 || len(monitors) == 0 {
		return nil, nil
	}

	handoffs := make([]*BalanceMonitorHandoff, 0, len(balances))
	for _, balance := range balances {
		if balance == nil {
			continue
		}

		balanceID := strings.TrimSpace(balance.BalanceID)
		if balanceID == "" {
			continue
		}

		// NO MONITOR, NO ROW. This is the write-amplification guard, and it is also what
		// makes an empty MonitorSnapshot on a stored row mean "written before the column
		// existed" rather than "no monitors": a row is never written for an empty set.
		balanceMonitors := monitors[balanceID]
		if len(balanceMonitors) == 0 {
			continue
		}

		snapshot, err := json.Marshal(balance)
		if err != nil {
			return nil, fmt.Errorf("failed to snapshot balance %s for monitor evaluation: %w", balanceID, err)
		}

		monitorSnapshot, err := json.Marshal(balanceMonitors)
		if err != nil {
			return nil, fmt.Errorf("failed to snapshot the monitors of balance %s: %w", balanceID, err)
		}

		handoffs = append(handoffs, &BalanceMonitorHandoff{
			HandoffID:       GenerateUUIDWithSuffix(balanceMonitorHandoffPrefix),
			BalanceID:       balanceID,
			LedgerID:        strings.TrimSpace(balance.LedgerID),
			BalanceSnapshot: snapshot,
			MonitorSnapshot: monitorSnapshot,
			Status:          OutboxStatusPending,
		})
	}

	return handoffs, nil
}

// BalanceMonitorEventIdentity is the stable identity of the alert one monitor produces
// for one balance movement.
//
// Parameters:
//   - handoffID string: the handoff the evaluation belongs to.
//   - monitorID string: the monitor whose condition was met.
//
// Returns:
//   - string: the identity to pass to DeriveEventID, empty when either part is missing,
//     which the caller must treat as "not derivable" rather than deriving from half an
//     identity.
func BalanceMonitorEventIdentity(handoffID, monitorID string) string {
	handoff := strings.TrimSpace(handoffID)
	monitor := strings.TrimSpace(monitorID)
	if handoff == "" || monitor == "" {
		return ""
	}

	// A colon cannot appear in a UUID or in one of this repository's prefixed
	// identifiers, so no two different pairs can produce the same joined string.
	return handoff + ":" + monitor
}

// Bulk batch coordinator statuses.
const (
	// BulkBatchStatusProcessing means the batch began and has not reported an outcome. A
	// row that stays here is the one residue the coordinator cannot remove: the process
	// handling the batch died before it could finalise.
	BulkBatchStatusProcessing = "processing"
	// BulkBatchStatusApplied is a batch whose transactions were all applied.
	BulkBatchStatusApplied = "applied"
	// BulkBatchStatusInflight is a batch whose transactions were all left inflight.
	BulkBatchStatusInflight = "inflight"
	// BulkBatchStatusFailed is a batch that failed, with error_message carrying the
	// failure and any rollback detail.
	BulkBatchStatusFailed = "failed"
)

// BulkTransactionBatch is one row of blnk.bulk_transaction_batches: the durable
// coordinator record for an asynchronous bulk transaction batch.
type BulkTransactionBatch struct {
	// BatchID is the parent transaction id of the batch and the primary key. It is also
	// the event's aggregate id, so the coordinator row and the event it produced are
	// joinable on it.
	BatchID string `json:"batch_id"`
	// Status is one of the BulkBatchStatus* constants.
	Status string `json:"status"`
	// TransactionCount is the number of transactions in the batch. It is zero on a
	// failed batch, matching the payload the failure path has always sent.
	TransactionCount int `json:"transaction_count"`
	// ErrorMessage is the failure detail including rollback status, empty on success.
	ErrorMessage string `json:"error_message,omitempty"`
	// Atomic records whether a failure rolls the whole batch back, and Inflight whether
	// its transactions are left inflight. Both are carried so a stuck row tells an
	// operator what the batch was ATTEMPTING, which is what decides how to finish it by
	// hand.
	Atomic   bool `json:"atomic"`
	Inflight bool `json:"inflight"`
	// EventID is the outbox event that recorded this outcome, empty until finalised. It is
	// the proof of the atomicity: a terminal row without one would mean the outcome was
	// recorded without its event, which the finalising transaction makes unreachable.
	EventID string `json:"event_id,omitempty"`
	// CreatedAt is when the batch began.
	CreatedAt time.Time `json:"created_at"`
	// FinalizedAt is when the outcome became terminal, nil before that. The schema
	// constrains this to be non-nil exactly when Status is terminal.
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
}

// IsTerminal reports whether this batch has reported an outcome.
//
// Returns:
//   - bool: false only for BulkBatchStatusProcessing.
func (b *BulkTransactionBatch) IsTerminal() bool {
	if b == nil {
		return false
	}

	return IsTerminalBulkBatchStatus(b.Status)
}

// IsTerminalBulkBatchStatus reports whether a coordinator status is an outcome.
//
// Parameters:
//   - status string: the status to classify.
//
// Returns:
//   - bool: true for applied, inflight and failed.
func IsTerminalBulkBatchStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case BulkBatchStatusApplied, BulkBatchStatusInflight, BulkBatchStatusFailed:
		return true
	default:
		return false
	}
}
