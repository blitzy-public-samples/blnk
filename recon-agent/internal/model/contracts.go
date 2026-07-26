// Package model declares the recon-agent's OWN domain contracts — the shared
// vocabulary that flows through the pipeline
// (classifier -> remediator -> store -> audit -> hitl).
//
// These types are deliberately distinct from the Blnk wire DTOs declared in
// internal/blnk/types.go: this package models the agent's internal state, while
// internal/blnk models Blnk's HTTP JSON shapes. The ONLY cross-package
// dependency here is on the recon-agent module's own internal/blnk package,
// referenced by BreakClassification.ProposedRule. Per Rule 5.1, this package
// imports NOTHING from Blnk's own internal Go packages; it depends only on the
// recon-agent module's own internal/blnk. That package must never import this
// one, so there is no import cycle.
package model

import "github.com/blnkfinance/recon-agent/internal/blnk"

// RootCause is the closed set of root-cause labels the classifier may assign to
// a reconciliation break. The classifier's structured LLM output MUST resolve to
// exactly one of these constants, and the ">= 5 of 6" seed-accuracy success
// criterion is scored against this enumeration.
type RootCause string

// The complete, closed enumeration of root-cause labels. Do not add values
// without also updating the classifier prompt and the evaluation corpus.
const (
	RootCauseTiming            RootCause = "timing"
	RootCauseAmountDrift       RootCause = "amount_drift"
	RootCauseReferenceMismatch RootCause = "reference_mismatch"
	RootCauseDuplicate         RootCause = "duplicate"
	RootCauseMissingInternal   RootCause = "missing_internal"
	RootCauseCurrencyMismatch  RootCause = "currency_mismatch"
	RootCauseUnknown           RootCause = "unknown"
)

// BreakClassification is the classifier's verdict for a single reconciliation
// break. It is produced by internal/classifier, consumed by internal/remediator
// (which applies the confidence + regulation gate, Rule 5.4), and surfaced on
// the HITL status page.
type BreakClassification struct {
	// ExternalTxnID is the id of the external transaction that broke.
	ExternalTxnID string `json:"external_txn_id"`
	// RootCause is the classified root cause; one of the RootCause constants.
	RootCause RootCause `json:"root_cause"`
	// Confidence is the calibrated confidence in the classification, in [0,1].
	Confidence float64 `json:"confidence"`
	// Regulated marks the break as touching a regulated flow. A regulated break
	// is NEVER auto-remediated regardless of confidence (Rule 5.4).
	Regulated bool `json:"regulated"`
	// ProposedRule is an optional Blnk-native matching rule the classifier
	// proposes to clear the break. It is nil when no safe rule was proposed.
	ProposedRule *blnk.MatchingRule `json:"proposed_rule,omitempty"`
	// Rationale is the classifier's short natural-language justification.
	Rationale string `json:"rationale"`
}

// Provenance captures the origin metadata stamped onto every AuditEvent so the
// append-only trail is fully self-describing.
type Provenance struct {
	// Model is the LLM_MODEL used for inference (e.g. "kimi-k3").
	Model string `json:"model"`
	// ReconID is the id of the Blnk dry-run reconciliation that confirmed a
	// resolution (Rule 5.3). Empty when the event is not a confirmed resolution.
	ReconID string `json:"recon_id"`
	// UploadID is the Blnk upload batch the break originated from.
	UploadID string `json:"upload_id"`
	// Source is the external-statement source label (e.g. the bank name).
	Source string `json:"source"`

	// The following identity fields (finding M-13) let the append-only trail
	// prove and reconstruct the EXACT transaction lifecycle end-to-end, not just
	// the final clearance. They are optional (omitempty) so an event that does
	// not concern a given stage simply omits the corresponding id.

	// MainReconID is the id of the batch reconciliation run over the persisted
	// upload that surfaced this break. It is DISTINCT from ReconID, which is the
	// per-break single-transaction dry-run that confirms an individual
	// clearance: MainReconID anchors the break to the run that produced it,
	// while ReconID proves that one break cleared (Rule 5.3).
	MainReconID string `json:"main_recon_id,omitempty"`
	// RunID correlates every event emitted during a single agent pipeline run,
	// so the complete set of actions for one execution can be reconstructed even
	// across many breaks.
	RunID string `json:"run_id,omitempty"`
	// ProbeTxnID is the ephemeral external-transaction id the agent submitted to
	// Blnk's single-transaction dry-run to probe/confirm this break. The agent
	// uses a fresh probe-<uuid> id to avoid colliding with Blnk's
	// external_transactions primary key, so recording it is the ONLY way to tie
	// a probe/confirmation back to the real break it stands in for.
	ProbeTxnID string `json:"probe_txn_id,omitempty"`

	// The following fields carry the machine-readable audit evidence stamped by
	// the internal/audit builders so the append-only trail is fully
	// self-describing and queryable (not buried in free-text rationale). They
	// are persisted transparently: agent.agent_audit stores Provenance as a
	// single JSONB column, so store.InsertAudit/ListAudit marshal and restore
	// these fields with no schema change. All are optional (omitempty): an event
	// that does not concern a classification or rule simply omits them.

	// RootCause is the classified root cause (one of the RootCause constants)
	// for the break the event concerns. Stamped on classified/escalated events.
	RootCause string `json:"root_cause,omitempty"`
	// Regulated marks the break as touching a regulated flow (Rule 5.4). Stamped
	// on classification-derived events so a regulated break is auditable as such.
	Regulated bool `json:"regulated,omitempty"`
	// RuleID identifies the Blnk matching rule the event concerns (a proposed or
	// created rule). Empty when the event does not concern a rule.
	RuleID string `json:"rule_id,omitempty"`
	// RuleField / RuleOperator record the primary criterion of that rule, giving
	// the audit trail machine-readable rule identity without a Blnk lookup.
	RuleField    string `json:"rule_field,omitempty"`
	RuleOperator string `json:"rule_operator,omitempty"`
}

// AuditEvent is a single immutable entry in the append-only audit trail
// (agent_audit). Every action the pipeline takes emits exactly one AuditEvent
// (Rule 5.5: this record is append-only and is never updated or deleted).
type AuditEvent struct {
	// EventID is a unique id for this event (a UUID).
	EventID string `json:"event_id"`
	// ExternalTxnID is the external transaction the event concerns.
	ExternalTxnID string `json:"external_txn_id"`
	// Actor is who performed the action: "agent" or a human reviewer id.
	Actor string `json:"actor"`
	// Action is what happened; one of: classified, resolved, escalated,
	// accepted, re_driven, rejected.
	Action string `json:"action"`
	// Timestamp is the RFC3339 time the event was recorded.
	Timestamp string `json:"timestamp"`
	// Rationale is a short human-readable reason for the action.
	Rationale string `json:"rationale"`
	// Confidence is the classification confidence associated with the action.
	Confidence float64 `json:"confidence"`
	// Provenance is the origin metadata for the action.
	Provenance Provenance `json:"provenance"`
}

// HITLDecision is a human reviewer's decision on a queued break, submitted via
// POST /decisions on the HITL server.
type HITLDecision struct {
	// ExternalTxnID identifies the queued break being decided.
	ExternalTxnID string `json:"external_txn_id"`
	// Decision is the reviewer's choice; one of: accept, re_drive, reject.
	Decision string `json:"decision"`
	// Reviewer is the id of the human making the decision.
	Reviewer string `json:"reviewer"`
	// Note is an optional free-text note from the reviewer.
	Note string `json:"note"`
}
