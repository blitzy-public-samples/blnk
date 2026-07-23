// Package blnk is the SOLE package in the recon-agent module permitted to
// communicate with the Blnk ledger service, and it does so EXCLUSIVELY over
// HTTP (Rule 5.1). It never imports any github.com/blnkfinance/blnk/... package.
//
// Because importing Blnk's model package is forbidden, the request/response
// contracts below are re-declared locally. Their JSON tags are byte-for-byte
// identical to Blnk's model/reconciliation_model.go so the wire format matches
// exactly.
package blnk

import "time"

// ExternalTransaction mirrors Blnk's model.ExternalTransaction JSON shape.
// It is the external-statement line submitted to the reconciliation engine.
type ExternalTransaction struct {
	ID          string    `json:"id"`
	Amount      float64   `json:"amount"`
	Reference   string    `json:"reference"`
	Currency    string    `json:"currency"`
	Description string    `json:"description"`
	Date        time.Time `json:"date"`
	Source      string    `json:"source"`
}

// Reconciliation mirrors Blnk's model.Reconciliation JSON shape returned by
// GET /reconciliation/:id.
//
// IMPORTANT: MatchedTransactions and UnmatchedTransactions are INTEGER COUNTS,
// not lists of transaction IDs. Blnk exposes no list of unmatched IDs over
// HTTP, which is precisely why Client.ProbeBreak submits a single transaction
// through a dry-run and inspects UnmatchedTransactions (0 => cleared, 1 =>
// still a break).
type Reconciliation struct {
	ReconciliationID      string     `json:"reconciliation_id"`
	UploadID              string     `json:"upload_id"`
	Status                string     `json:"status"`
	MatchedTransactions   int        `json:"matched_transactions"`
	UnmatchedTransactions int        `json:"unmatched_transactions"`
	IsDryRun              bool       `json:"is_dry_run"`
	StartedAt             time.Time  `json:"started_at"`
	CompletedAt           *time.Time `json:"completed_at"`
}

// MatchingRule mirrors Blnk's model.MatchingRule JSON shape. It is the body of
// POST /reconciliation/matching-rules and PUT /reconciliation/matching-rules/:id
// and is returned by both.
type MatchingRule struct {
	RuleID      string             `json:"rule_id"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Criteria    []MatchingCriteria `json:"criteria"`
}

// MatchingCriteria mirrors Blnk's model.MatchingCriteria JSON shape.
//
// Blnk accepts only a fixed domain of Field and Operator values:
//
//	Field    ∈ {amount, date, description, reference, currency}
//	Operator ∈ {equals, greater_than, less_than, contains}
//
// These domains are documented here for reference only. Enforcement of the
// grammar for agent-PROPOSED rules lives in internal/classifier/grammar.go
// (Rule 5.2); this package performs no validation and simply transports the
// rule to Blnk.
type MatchingCriteria struct {
	Field          string  `json:"field"`
	Operator       string  `json:"operator"`
	Value          string  `json:"value"`
	Pattern        string  `json:"pattern"`
	AllowableDrift float64 `json:"allowable_drift"`
}

// StartReconciliationRequest is the JSON body for POST /reconciliation/start.
// UploadID, Strategy and MatchingRuleIDs are required by Blnk.
type StartReconciliationRequest struct {
	UploadID         string   `json:"upload_id"`
	Strategy         string   `json:"strategy"`
	GroupingCriteria string   `json:"grouping_criteria,omitempty"`
	DryRun           bool     `json:"dry_run"`
	MatchingRuleIDs  []string `json:"matching_rule_ids"`
}

// InstantReconciliationRequest is the JSON body for
// POST /reconciliation/start-instant. ExternalTransactions, Strategy and
// MatchingRuleIDs are required by Blnk.
type InstantReconciliationRequest struct {
	ExternalTransactions []ExternalTransaction `json:"external_transactions"`
	Strategy             string                `json:"strategy"`
	GroupingCriteria     string                `json:"grouping_criteria,omitempty"`
	DryRun               bool                  `json:"dry_run"`
	MatchingRuleIDs      []string              `json:"matching_rule_ids"`
}

// UploadResponse is the JSON returned by POST /reconciliation/upload.
type UploadResponse struct {
	UploadID    string `json:"upload_id"`
	RecordCount int    `json:"record_count"`
	Source      string `json:"source"`
}

// StartReconciliationResponse is the JSON returned by both
// POST /reconciliation/start and POST /reconciliation/start-instant.
type StartReconciliationResponse struct {
	ReconciliationID string `json:"reconciliation_id"`
}
