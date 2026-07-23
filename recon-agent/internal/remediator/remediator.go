// Package remediator is the safe auto-remediation orchestrator and decision
// core of the recon-agent pipeline.
//
// For each external "break" it classifies a root cause, applies the
// confidence/regulation gate (Rule 5.4), and — only for high-confidence,
// non-regulated breaks — performs the deterministic
// propose-rule -> dry-run -> confirm-cleared loop in which Blnk (never the LLM)
// is the sole arbiter of clearance (Rule 5.3). Every action emits an
// append-only AuditEvent (Rule 5.5). Anything not auto-cleared — including any
// classifier failure (fail-closed, Rule 5.7) — is routed to the
// human-in-the-loop (HITL) queue and is never dropped or silently resolved.
//
// This package reaches Blnk only through the recon-agent's own internal/blnk
// HTTP client; it imports no github.com/blnkfinance/blnk/... internal package
// (Rule 5.1).
package remediator

import (
	"context"
	"errors"
	"fmt"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// Break lifecycle status strings persisted through the store. They MUST match
// the values the store and HITL status page understand.
const (
	statusClassified   = "classified"
	statusAutoResolved = "auto-resolved"
	statusQueued       = "queued"
)

// Human-readable escalation reasons recorded on the HITL queue entry and on the
// rationale of the escalated AuditEvent.
const (
	reasonRegulated        = "regulated break requires human review"
	reasonLowConfidence    = "classification confidence below auto-remediation threshold"
	reasonClassifierFailed = "classifier failed; routed to HITL (fail-closed)"
	reasonMissingRule      = "no matching rule proposed; cannot auto-remediate"
	reasonGrammarReject    = "proposed matching rule failed grammar validation"
	reasonRuleCreateFailed = "failed to create matching rule in Blnk"
	reasonProbeFailed      = "Blnk dry-run probe failed"
	reasonNotCleared       = "Blnk dry-run did not confirm clearance"
)

// classifierPort is the subset of the classifier used by the remediator.
type classifierPort interface {
	Classify(ctx context.Context, txn blnk.ExternalTransaction) (model.BreakClassification, error)
}

// blnkPort is the subset of the Blnk HTTP client used by the remediator. It is
// the ONLY channel through which the remediator touches Blnk (Rule 5.1).
type blnkPort interface {
	CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error)
	ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (cleared bool, reconID string, err error)
}

// auditPort is the append-only audit writer used by the remediator (Rule 5.5).
type auditPort interface {
	Record(ctx context.Context, ev model.AuditEvent) error
}

// storePort is the subset of the persistence layer used by the remediator.
type storePort interface {
	UpsertBreak(ctx context.Context, c model.BreakClassification, status string) error
	SetBreakStatus(ctx context.Context, externalTxnID, status string) error
	EnqueueHITL(ctx context.Context, externalTxnID, reason string) error
}

// Remediator orchestrates classification, gating, deterministic remediation and
// HITL escalation for one break at a time.
type Remediator struct {
	cls         classifierPort
	blnkClient  blnkPort
	auditWriter auditPort
	store       storePort

	// threshold is CONF_AUTO_THRESHOLD; a break may only be auto-remediated
	// when its confidence is >= threshold and it is not regulated (Rule 5.4).
	threshold float64

	// llmModel is recorded as provenance on every AuditEvent (Rule 5.6).
	llmModel string
}

// New wires the remediator with its collaborators. The parameters are
// consumer-side interfaces so tests can inject fakes; the concrete
// *classifier.Classifier, *blnk.Client, *audit.Writer and *store.Store returned
// by the module's own constructors satisfy them structurally.
func New(cls classifierPort, blnkClient blnkPort, auditWriter auditPort, store storePort, threshold float64, llmModel string) *Remediator {
	return &Remediator{
		cls:         cls,
		blnkClient:  blnkClient,
		auditWriter: auditWriter,
		store:       store,
		threshold:   threshold,
		llmModel:    llmModel,
	}
}

// Handle processes exactly one external break end to end.
//
// It returns nil once the break has reached a terminal, safely-recorded outcome
// (auto-resolved or escalated to HITL). It returns a non-nil error only when an
// agent-side infrastructure operation (persistence or audit write) fails; such
// errors never leave a break auto-resolved.
func (r *Remediator) Handle(ctx context.Context, txn blnk.ExternalTransaction) error {
	prov := model.Provenance{
		Model:  r.llmModel,
		Source: txn.Source,
	}

	// 1. Classify. Rule 5.7 (fail-closed): ANY classifier error/timeout — the
	// classifier signals terminal failure with ErrClassificationFailed after
	// exhausting its retry cap — routes the break straight to HITL. The break
	// is never dropped and never auto-resolved, and no "classified" event is
	// emitted because classification did not succeed.
	classification, err := r.cls.Classify(ctx, txn)
	if err != nil {
		stub := classification
		stub.ExternalTxnID = txn.ID
		if stub.RootCause == "" {
			stub.RootCause = model.RootCauseUnknown
		}
		reason := reasonClassifierFailed
		if !errors.Is(err, classifier.ErrClassificationFailed) {
			reason = fmt.Sprintf("%s (unexpected error: %v)", reasonClassifierFailed, err)
		}
		return r.escalate(ctx, stub, prov, reason)
	}

	// Defensive: guarantee the break is keyed by the transaction under
	// management regardless of what the classifier populated.
	classification.ExternalTxnID = txn.ID

	// 2. Persist the classified break and emit the classified AuditEvent
	// (Rule 5.5 — classification is an auditable action).
	if err := r.store.UpsertBreak(ctx, classification, statusClassified); err != nil {
		return fmt.Errorf("remediator: persist classified break %q: %w", txn.ID, err)
	}
	if err := r.auditWriter.Record(ctx, audit.Classified(classification, prov)); err != nil {
		return fmt.Errorf("remediator: audit classified break %q: %w", txn.ID, err)
	}

	// 3. Confidence/regulation gate (Rule 5.4). A regulated break, or one below
	// the confidence threshold, is NEVER auto-remediated: it is escalated to
	// HITL with no resolved event.
	if classification.Regulated {
		return r.escalate(ctx, classification, prov, reasonRegulated)
	}
	if classification.Confidence < r.threshold {
		return r.escalate(ctx, classification, prov, reasonLowConfidence)
	}

	// 4. Auto path. The agent only PROPOSES; Blnk DECIDES (Rule 5.3).
	if classification.ProposedRule == nil {
		return r.escalate(ctx, classification, prov, reasonMissingRule)
	}

	// Rule 5.2: never POST a rule whose Field/Operator falls outside Blnk's
	// accepted grammar. The classifier already grammar-gates proposals; this is
	// defense-in-depth immediately before the POST.
	if err := classifier.ValidateRule(*classification.ProposedRule); err != nil {
		return r.escalate(ctx, classification, prov, fmt.Sprintf("%s: %v", reasonGrammarReject, err))
	}

	created, err := r.blnkClient.CreateMatchingRule(ctx, *classification.ProposedRule)
	if err != nil {
		return r.escalate(ctx, classification, prov, fmt.Sprintf("%s: %v", reasonRuleCreateFailed, err))
	}

	// Re-drive a single-transaction Blnk dry-run using the freshly created
	// rule. Blnk's InstantReconciliation requires a non-empty matching_rule_ids
	// set, so the created rule's ID is passed explicitly.
	cleared, reconID, err := r.blnkClient.ProbeBreak(ctx, txn, []string{created.RuleID})
	if err != nil {
		return r.escalate(ctx, classification, prov, fmt.Sprintf("%s: %v", reasonProbeFailed, err))
	}
	if !cleared {
		return r.escalate(ctx, classification, prov, reasonNotCleared)
	}

	// 5. Blnk confirmed clearance — and only Blnk can (Rule 5.3). Build the
	// clearance proof from the confirming reconciliation id BEFORE marking the
	// break resolved: audit.Resolved takes a ClearanceProof (never a bare id),
	// so a resolution is impossible to record without evidence of an actual
	// dry-run clearance. If the arbiter reported cleared but returned no usable
	// reconciliation id, fail closed and escalate rather than record an
	// unprovable resolution (Rule 5.7). LLM confidence alone never resolves.
	proof, err := audit.NewClearanceProof(reconID, cleared)
	if err != nil {
		return r.escalate(ctx, classification, prov, fmt.Sprintf("%s: %v", reasonNotCleared, err))
	}
	if err := r.store.SetBreakStatus(ctx, classification.ExternalTxnID, statusAutoResolved); err != nil {
		return fmt.Errorf("remediator: mark break %q auto-resolved: %w", txn.ID, err)
	}
	// The proof's confirming recon_id is stamped onto the resolved AuditEvent's
	// provenance, making every resolution traceable to the dry-run that cleared it.
	if err := r.auditWriter.Record(ctx, audit.Resolved(classification.ExternalTxnID, classification.Rationale, classification.Confidence, prov, proof)); err != nil {
		return fmt.Errorf("remediator: audit resolved break %q: %w", txn.ID, err)
	}
	return nil
}

// escalate routes a break to the human-in-the-loop queue: it marks the break
// queued, enqueues it with the given reason, and emits an escalated AuditEvent
// (Rule 5.5). It never records a resolved event, guaranteeing that a break the
// agent could not auto-clear is surfaced to a human rather than left silently
// unresolved (Rules 5.4 / 5.7).
func (r *Remediator) escalate(ctx context.Context, classification model.BreakClassification, prov model.Provenance, reason string) error {
	if err := r.store.UpsertBreak(ctx, classification, statusQueued); err != nil {
		return fmt.Errorf("remediator: persist queued break %q: %w", classification.ExternalTxnID, err)
	}
	if err := r.store.EnqueueHITL(ctx, classification.ExternalTxnID, reason); err != nil {
		return fmt.Errorf("remediator: enqueue break %q for HITL: %w", classification.ExternalTxnID, err)
	}
	if err := r.auditWriter.Record(ctx, audit.Escalated(classification.ExternalTxnID, reason, classification.Confidence, prov)); err != nil {
		return fmt.Errorf("remediator: audit escalated break %q: %w", classification.ExternalTxnID, err)
	}
	return nil
}
