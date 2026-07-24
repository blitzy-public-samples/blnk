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
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

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
	reasonRegulated                = "regulated break requires human review"
	reasonLowConfidence            = "classification confidence below auto-remediation threshold"
	reasonRootCauseNotAutoEligible = "root cause is not eligible for automatic remediation; requires human review"
	reasonClassifierFailed         = "classifier failed; routed to HITL (fail-closed)"
	reasonInvalidConfidence        = "classifier returned a malformed confidence; routed to HITL (fail-closed)"
	reasonMissingRule              = "no matching rule proposed; cannot auto-remediate"
	reasonGrammarReject            = "proposed matching rule failed grammar validation"
	reasonOverbroadRule            = "proposed matching rule is too broad to auto-apply safely"
	reasonRuleCreateFailed         = "failed to create matching rule in Blnk"
	reasonEmptyRuleID              = "Blnk returned an empty matching-rule id; cannot probe"
	reasonProbeFailed              = "Blnk dry-run probe failed"
	reasonNotCleared               = "Blnk dry-run did not confirm clearance"
	reasonOrphanedRuleResume       = "prior rule proposal recorded without a persisted created rule; possible orphaned Blnk rule from an interrupted attempt — routed to HITL to avoid creating a duplicate"
)

// operatorEquals is the only matching operator tight enough to auto-apply to a
// single-transaction dry-run (finding C3). Blnk compares the external transaction
// against internal transactions FIELD-TO-FIELD and IGNORES criteria.Value for all
// operators (see Blnk reconciliation.go matchesString / matchesGroupAmount /
// matchesCurrency), so a range/substring operator (greater_than / less_than /
// contains) can match an UNINTENDED internal counterpart and make a single-txn
// probe falsely report "cleared". Equality pins external == internal on a field.
const operatorEquals = "equals"

// autoEligibleRootCauses is the closed allow-list of root causes the agent may
// auto-remediate (finding C3). It is defense-in-depth ABOVE the classifier's soft
// "omit a rule for unsafe causes" behavior: even if a high-confidence
// classification arrives WITH a grammar-valid rule attached, only these causes may
// proceed down the auto path. duplicate, missing_internal, currency_mismatch and
// unknown are deliberately excluded and ALWAYS route to HITL regardless of
// confidence, because a single-transaction Blnk dry-run can falsely "clear" them
// (e.g. a duplicate double-matches the sole internal counterpart of the original
// line; missing_internal has no counterpart to match; currency_mismatch is
// treated as regulated). This mirrors the evaluation corpus, which auto-resolves
// only timing / amount_drift / reference_mismatch and escalates the rest.
var autoEligibleRootCauses = map[model.RootCause]bool{
	model.RootCauseTiming:            true,
	model.RootCauseAmountDrift:       true,
	model.RootCauseReferenceMismatch: true,
}

// classifierPort is the subset of the classifier used by the remediator.
type classifierPort interface {
	Classify(ctx context.Context, txn blnk.ExternalTransaction) (model.BreakClassification, error)
}

// blnkPort is the subset of the Blnk HTTP client used by the remediator. It is
// the ONLY channel through which the remediator touches Blnk (Rule 5.1).
type blnkPort interface {
	CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error)
	// DeleteMatchingRule removes a matching rule from Blnk's shared catalog. The
	// remediator uses it to roll back a rule it created for an auto-remediation
	// attempt that did not clear, so a failed attempt leaves no orphan rule
	// behind (finding F5).
	DeleteMatchingRule(ctx context.Context, ruleID string) error
	ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (cleared bool, reconID string, err error)
}

// auditPort is the append-only audit writer used by the remediator (Rule 5.5).
// RecordTx enrolls an append in a caller-supplied transaction so a state change
// and its audit event commit atomically (Rule 5.3); Record is used for the
// advisory rule_proposed/probed events that accompany no state change.
type auditPort interface {
	Record(ctx context.Context, ev model.AuditEvent) error
	RecordTx(ctx context.Context, tx *sql.Tx, ev model.AuditEvent) error
}

// storePort is the subset of the persistence layer used by the remediator. All
// writes are transaction-bound so each break state transition commits together
// with its audit event (the atomicity the QA fault-injection findings require);
// WithTx supplies the transaction and LoadBreak drives idempotency/resume.
type storePort interface {
	// LoadBreak returns the persisted classification, status, and created rule
	// id for a break (found=false when absent), enabling idempotent re-runs and
	// crash-resume without re-classifying or duplicating a Blnk rule.
	LoadBreak(ctx context.Context, externalTxnID string) (model.BreakClassification, string, string, bool, error)
	// WithTx runs fn in one transaction, committing on nil and rolling back on
	// error so partial writes are never durable.
	WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error
	UpsertBreakTx(ctx context.Context, tx *sql.Tx, c model.BreakClassification, txn blnk.ExternalTransaction, status string) error
	SetBreakStatusTx(ctx context.Context, tx *sql.Tx, externalTxnID, status string) error
	EnqueueHITLTx(ctx context.Context, tx *sql.Tx, externalTxnID, reason string) error
	SetCreatedRuleTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error
	// SetCreatedRule records (or, with an empty ruleID, CLEARS) the persisted
	// Blnk rule id for a break in autocommit mode. The remediator clears it as
	// part of rolling back a rule it created for an auto-remediation attempt
	// that did not clear (finding F5), so a subsequent HITL re_drive does not
	// try to reuse a now-deleted Blnk rule.
	SetCreatedRule(ctx context.Context, externalTxnID, ruleID string) error
	// CountAuditByAction returns how many append-only audit events a break has
	// recorded for the given action (SELECT only — no write). It drives the
	// fail-closed resume guard against duplicate Blnk rule creation
	// (SEAM-MIN-1): a rule_proposed event is durably recorded before the Blnk
	// CreateMatchingRule POST, so a positive count with no persisted created
	// rule id signals a prior attempt may have orphaned a rule in Blnk.
	CountAuditByAction(ctx context.Context, externalTxnID, action string) (int, error)
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

	// locks serializes concurrent Handle calls for the SAME break so a break is
	// never classified/remediated twice by racing goroutines (finding M-5).
	// Distinct breaks acquire distinct keys and still run in parallel.
	locks keyedMutex
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
//
// Handle is safe to call concurrently and is idempotent: concurrent calls for
// the same break are serialized (M-5), an already-terminal break is a no-op so
// a re-run never double-records a resolution or escalation (M-6), and a break
// left "classified" by a crashed prior attempt is resumed — reusing its stored
// classification and any created Blnk rule — without re-classifying or creating
// a duplicate rule (M-3). Every state transition commits atomically with its
// audit event, so no partial outcome is ever durable (C-1, C-2, M-2, M-4).
//
// uploadID identifies the reconciliation upload batch this break originated
// from. It is stamped into the provenance of EVERY audit event the break emits
// (finding m-2), so the append-only trail attributes each action to its source
// upload and per-run summaries can be scoped correctly (finding M-6). Blnk's
// own external-transaction JSON carries no upload id (it is a property of the
// reconciliation run, not the transaction line), so it is threaded here as an
// explicit parameter rather than read off txn.
func (r *Remediator) Handle(ctx context.Context, txn blnk.ExternalTransaction, uploadID string) error {
	// M-5: serialize concurrent Handle calls for the SAME break. Released after
	// the break reaches a terminal, committed state (or an error), so a racing
	// caller observes that terminal state and no-ops below rather than
	// re-processing.
	unlock := r.locks.lock(txn.ID)
	defer unlock()

	prov := model.Provenance{
		Model:    r.llmModel,
		Source:   txn.Source,
		UploadID: uploadID,
	}

	// M-6 idempotency / crash-resume: consult the persisted state first.
	loaded, status, createdRuleID, found, err := r.store.LoadBreak(ctx, txn.ID)
	if err != nil {
		return fmt.Errorf("remediator: load break %q: %w", txn.ID, err)
	}

	var classification model.BreakClassification
	switch {
	case found && (status == statusAutoResolved || status == statusQueued):
		// Terminal outcome already recorded. A re-run (or the loser of a race)
		// is a no-op, so a resolved/escalated event is never double-counted.
		return nil

	case found && status == statusClassified:
		// Resume a break a prior attempt persisted+audited as classified but did
		// not carry to a terminal state. Reuse the stored classification (and any
		// created rule id) WITHOUT re-classifying or re-emitting a classified
		// event; fall through to the gate and auto path below.
		classification = loaded
		classification.ExternalTxnID = txn.ID

	default:
		// Fresh break: classify (fail-closed on error), validate confidence,
		// then persist + audit the classification atomically (M-2).
		c, cerr := r.cls.Classify(ctx, txn)
		if cerr != nil {
			// Rule 5.7 (fail-closed): ANY classifier error/timeout routes the
			// break straight to HITL. It is never dropped or auto-resolved, and
			// no classified event is emitted because classification did not
			// succeed.
			stub := c
			stub.ExternalTxnID = txn.ID
			if stub.RootCause == "" {
				stub.RootCause = model.RootCauseUnknown
			}
			reason := reasonClassifierFailed
			if !errors.Is(cerr, classifier.ErrClassificationFailed) {
				reason = fmt.Sprintf("%s (unexpected error: %v)", reasonClassifierFailed, cerr)
			}
			return r.escalate(ctx, txn, stub, prov, reason)
		}

		// Defensive: guarantee the break is keyed by the transaction under
		// management regardless of what the classifier populated.
		c.ExternalTxnID = txn.ID

		// m-4: a malformed confidence (NaN/Inf or outside [0,1]) must fail closed
		// to HITL, never silently flow into the auto path (NaN comparisons make
		// the gate below ineffective). No classified event is emitted; escalate
		// sanitizes the confidence so the escalated event is itself well-formed.
		if !validConfidence(c.Confidence) {
			return r.escalate(ctx, txn, c, prov, fmt.Sprintf("%s (confidence=%v)", reasonInvalidConfidence, c.Confidence))
		}

		// M-2: persist the classified break and emit the classified AuditEvent in
		// ONE transaction, so a classified break can never exist without its
		// audit event (nor an audit event without the break).
		if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
			if e := r.store.UpsertBreakTx(ctx, tx, c, txn, statusClassified); e != nil {
				return e
			}
			return r.auditWriter.RecordTx(ctx, tx, audit.Classified(c, prov))
		}); err != nil {
			return fmt.Errorf("remediator: persist+audit classified break %q: %w", txn.ID, err)
		}
		classification = c
	}

	id := classification.ExternalTxnID

	// Confidence/regulation gate (Rule 5.4). A regulated break, or one below the
	// confidence threshold, is NEVER auto-remediated: it is escalated to HITL
	// with no resolved event. (On resume this re-applies the gate to a break a
	// crashed attempt left classified.)
	if classification.Regulated {
		return r.escalate(ctx, txn, classification, prov, reasonRegulated)
	}
	if classification.Confidence < r.threshold {
		return r.escalate(ctx, txn, classification, prov, reasonLowConfidence)
	}

	// Root-cause auto-eligibility (finding C3). Confidence and the regulated flag
	// alone are NOT sufficient to auto-remediate: the root cause itself must be
	// one that a single-transaction dry-run can safely clear. duplicate,
	// missing_internal, currency_mismatch and unknown are escalated here
	// REGARDLESS of confidence, so a high-confidence classification of an unsafe
	// cause — even one that (incorrectly) carries a grammar-valid proposed rule —
	// can never be auto-applied. This is a hard, model-independent backstop above
	// the classifier's soft guidance to omit a rule for these causes.
	if !autoEligibleRootCauses[classification.RootCause] {
		return r.escalate(ctx, txn, classification, prov, reasonRootCauseNotAutoEligible)
	}

	// Auto path. The agent only PROPOSES; Blnk DECIDES (Rule 5.3).
	if classification.ProposedRule == nil {
		return r.escalate(ctx, txn, classification, prov, reasonMissingRule)
	}

	// provEv carries the classification evidence (root cause, regulated flag,
	// rule identity) on every auto-path audit event.
	provEv := audit.WithClassificationEvidence(prov, classification)

	// M-3: reuse a rule a prior attempt already created for this break rather
	// than creating a duplicate in Blnk. When none exists yet, propose → create
	// → persist the created rule id atomically with its rule_created event.
	ruleID := createdRuleID
	// createdHere is set true only when THIS invocation creates a new Blnk rule
	// (as opposed to reusing a rule a prior attempt already persisted). It gates
	// the finding-F5 rollback: if the auto-remediation attempt does not clear, a
	// rule created here is deleted so a failed attempt leaves no orphan rule in
	// Blnk's shared catalog. A reused (already-persisted) rule is never deleted
	// here — it is tracked on the break and managed by the HITL re_drive path.
	createdHere := false
	if ruleID == "" {
		// SEAM-MIN-1 (fail-closed resume guard against a DUPLICATE Blnk rule).
		// The rule_proposed audit event below is recorded durably BEFORE the
		// CreateMatchingRule POST, and the created rule id is persisted only
		// AFTER the POST, atomically with the rule_created event. So if we reach
		// this block for a break that has NO persisted created rule id yet
		// already has a rule_proposed event, a prior attempt crashed in the
		// window between the Blnk POST and that atomic commit — Blnk may hold an
		// orphaned rule (deterministically named agent-proposed-<id>). Blnk
		// exposes no list/get matching-rule route (only POST/PUT/DELETE), so the
		// orphan cannot be reconciled over the native HTTP surface (Rule 5.1),
		// and re-POSTing would create a DUPLICATE. Escalate to HITL for human
		// reconciliation rather than auto-recreating the rule. A fresh break
		// (no prior rule_proposed event) is unaffected and proceeds normally.
		proposedCount, err := r.store.CountAuditByAction(ctx, id, audit.ActionRuleProposed)
		if err != nil {
			return fmt.Errorf("remediator: check prior rule proposal for break %q: %w", id, err)
		}
		if proposedCount > 0 {
			return r.escalate(ctx, txn, classification, prov, reasonOrphanedRuleResume)
		}

		// F7: narrow the proposed rule before any POST. The classifier's
		// {amount,equals} proposal carries an empty (or model-suggested) Value,
		// and Blnk's field-to-field matcher IGNORES MatchingCriteria.Value
		// entirely — it compares each external field against the internal
		// candidate's same field — so an amount-only rule can clear a break by
		// coincidentally matching an internal booking of the SAME amount in a
		// DIFFERENT currency. We deep-copy the proposal (Criteria is a slice, so
		// copying the struct alone would alias it and mutate the persisted
		// classification's rule), populate each equality criterion's Value from
		// the transaction for auditability/intent, and append a {currency,equals}
		// criterion when the break carries a currency so clearance requires the
		// internal booking to match BOTH amount and currency. A residual
		// limitation remains — two internal bookings of the same amount AND
		// currency are still indistinguishable field-to-field — which is inherent
		// to matching over Blnk's native HTTP surface (Rule 5.1 forbids reading
		// Blnk's tables to disambiguate further).
		rule := narrowProposedRule(*classification.ProposedRule, txn)

		// Rule 5.2: never POST a rule whose Field/Operator falls outside Blnk's
		// accepted grammar. The classifier already grammar-gates proposals; this
		// is defense-in-depth immediately before the POST, validating the ACTUAL
		// (narrowed) rule that will be sent.
		if err := classifier.ValidateRule(rule); err != nil {
			return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonGrammarReject, err))
		}

		// Semantic safety before any POST (finding C3): reject proposed rules that
		// are too broad to auto-apply to a single-transaction dry-run. A
		// grammar-valid rule can still be unsafe — a range/substring operator can
		// match an unintended internal counterpart and falsely clear the probe —
		// so only tight equality criteria may proceed; anything else routes to
		// HITL. The appended {currency,equals} criterion is an equality criterion
		// and therefore compatible with this check.
		if err := autoApplySafe(rule); err != nil {
			return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonOverbroadRule, err))
		}

		// m-3: record the proposal (previously dead-code audit builder) before
		// any POST to Blnk.
		if err := r.auditWriter.Record(ctx, audit.RuleProposed(id, ruleRefOf(rule), classification.Confidence, provEv)); err != nil {
			return fmt.Errorf("remediator: audit proposed rule for break %q: %w", id, err)
		}

		created, err := r.blnkClient.CreateMatchingRule(ctx, rule)
		if err != nil {
			return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonRuleCreateFailed, err))
		}
		// m-7: guard an empty rule id BEFORE probing — Blnk's instant
		// reconciliation requires a non-empty matching_rule_ids set, and probing
		// with [""] would be meaningless. Fail closed to HITL.
		if created.RuleID == "" {
			return r.escalate(ctx, txn, classification, prov, reasonEmptyRuleID)
		}
		ruleID = created.RuleID
		createdHere = true

		// M-3 / m-3: persist the created rule id ATOMICALLY with its rule_created
		// audit event. Committing them together means a crash cannot leave an
		// orphaned Blnk rule the agent has forgotten; on resume the persisted id
		// is reused instead of creating another rule.
		if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
			if e := r.store.SetCreatedRuleTx(ctx, tx, id, ruleID); e != nil {
				return e
			}
			return r.auditWriter.RecordTx(ctx, tx, audit.RuleCreated(id, ruleRefOf(created), classification.Confidence, provEv))
		}); err != nil {
			return fmt.Errorf("remediator: persist+audit created rule for break %q: %w", id, err)
		}
	}

	// Deterministic re-drive: a single-transaction Blnk dry-run with the rule.
	cleared, reconID, err := r.blnkClient.ProbeBreak(ctx, txn, []string{ruleID})
	if err != nil {
		// F5: the probe transport/validation failed, so this rule will not be
		// used to clear the break. If we created it in this call, roll it back
		// (delete from Blnk + clear the persisted id) before escalating so a
		// failed auto-attempt leaves no orphan rule behind.
		if createdHere {
			r.rollbackCreatedRule(ctx, id, ruleID)
		}
		return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonProbeFailed, err))
	}
	// m-3: record the probe outcome (previously dead-code audit builder).
	if err := r.auditWriter.Record(ctx, audit.Probed(id, reconID, cleared, provEv)); err != nil {
		return fmt.Errorf("remediator: audit probe for break %q: %w", id, err)
	}
	if !cleared {
		// F5: Blnk did not confirm clearance, so this rule did not do its job.
		// Roll back a rule created in this call before escalating.
		if createdHere {
			r.rollbackCreatedRule(ctx, id, ruleID)
		}
		return r.escalate(ctx, txn, classification, prov, reasonNotCleared)
	}

	// Blnk confirmed clearance — and only Blnk can (Rule 5.3). Build the
	// clearance proof from the confirming reconciliation id: audit.Resolved
	// takes a ClearanceProof (never a bare id), so a resolution is impossible to
	// record without evidence of an actual dry-run clearance. If the arbiter
	// reported cleared but returned no usable reconciliation id, fail closed and
	// escalate rather than record an unprovable resolution (Rule 5.7). LLM
	// confidence alone never resolves.
	proof, err := audit.NewClearanceProof(reconID, cleared)
	if err != nil {
		// F5: Blnk reported cleared but returned no usable reconciliation id, so
		// we fail closed and escalate rather than record an unprovable
		// resolution. Roll back a rule created in this call so the aborted
		// attempt leaves no orphan rule behind.
		if createdHere {
			r.rollbackCreatedRule(ctx, id, ruleID)
		}
		return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonNotCleared, err))
	}

	// C-1: mark the break auto-resolved and record the resolved AuditEvent in
	// ONE transaction, so an auto-resolved status can never be durable without
	// its resolved event (nor vice versa). The proof's confirming recon_id is
	// stamped onto the event's provenance, making every resolution traceable to
	// the dry-run that cleared it.
	if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if e := r.store.SetBreakStatusTx(ctx, tx, id, statusAutoResolved); e != nil {
			return e
		}
		return r.auditWriter.RecordTx(ctx, tx, audit.Resolved(id, classification.Rationale, classification.Confidence, provEv, proof))
	}); err != nil {
		return fmt.Errorf("remediator: resolve break %q: %w", id, err)
	}
	return nil
}

// escalate routes a break to the human-in-the-loop queue: it marks the break
// queued, enqueues it with the given reason, and emits an escalated AuditEvent
// — all in ONE transaction (C-2, M-4), so a queued break can never exist
// without both its queue entry AND its audit event. It never records a resolved
// event, guaranteeing that a break the agent could not auto-clear is surfaced
// to a human rather than left silently unresolved (Rules 5.4 / 5.7).
// The txn argument carries the original transaction's matchable fields so the
// queued break persists them and can later be re-driven from the HITL surface
// (finding L2).
func (r *Remediator) escalate(ctx context.Context, txn blnk.ExternalTransaction, classification model.BreakClassification, prov model.Provenance, reason string) error {
	// Sanitize a possibly-malformed confidence so both the persisted break and
	// the escalated audit event are well-formed (the audit validator rejects a
	// non-finite or out-of-range confidence).
	classification.Confidence = safeConfidence(classification.Confidence)

	// Enrich the escalation provenance with the classification evidence (root
	// cause, regulated flag, proposed-rule identity) so the HITL/audit trail is
	// self-describing rather than free-text only.
	provEv := audit.WithClassificationEvidence(prov, classification)

	if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if e := r.store.UpsertBreakTx(ctx, tx, classification, txn, statusQueued); e != nil {
			return e
		}
		if e := r.store.EnqueueHITLTx(ctx, tx, classification.ExternalTxnID, reason); e != nil {
			return e
		}
		return r.auditWriter.RecordTx(ctx, tx, audit.Escalated(classification.ExternalTxnID, reason, classification.Confidence, provEv))
	}); err != nil {
		return fmt.Errorf("remediator: escalate break %q to HITL: %w", classification.ExternalTxnID, err)
	}
	return nil
}

// ruleRefOf projects a Blnk matching rule to the audit RuleRef (id + primary
// criterion) stamped onto rule_proposed/rule_created events.
func ruleRefOf(rule blnk.MatchingRule) audit.RuleRef {
	ref := audit.RuleRef{ID: rule.RuleID}
	if len(rule.Criteria) > 0 {
		ref.Field = rule.Criteria[0].Field
		ref.Operator = rule.Criteria[0].Operator
	}
	return ref
}

// Blnk matching-criteria field names the remediator populates or appends when
// narrowing a proposed rule (finding F7). They mirror Blnk's accepted field
// grammar (classifier.allowedFields) — validated defensively by
// classifier.ValidateRule before any POST.
const (
	fieldAmount      = "amount"
	fieldCurrency    = "currency"
	fieldReference   = "reference"
	fieldDescription = "description"
	fieldDate        = "date"
)

// narrowProposedRule returns a defensive DEEP COPY of the classifier's proposed
// rule, tightened for the specific transaction under remediation (finding F7:
// "non-deterministic clearance depends on polluted shared DB state").
//
// It (1) copies the rule and its Criteria slice so the persisted
// classification's rule is never mutated, (2) fills each equality criterion's
// Value from the corresponding transaction field (the fix explicitly suggested
// by the QA finding — for auditability and human intent), and (3) appends a
// {currency,equals} criterion when the transaction carries a currency and the
// proposal does not already constrain it, so clearance requires the candidate
// to match BOTH amount and currency (this reduces cross-currency spurious
// matches and still clears the real same-currency seed counterparts).
//
// RESIDUAL LIMITATION (documented, bounded by explicit AAP rules — see finding
// F7 and AAP §0.1.1/§0.4.1). Full, deterministic clearance-against-the-specific
// -intended-counterpart is NOT achievable over Blnk's native HTTP surface,
// because Blnk-core behavior — which Rule 5.8 forbids modifying and Rule 5.1
// forbids reading around — determines matching:
//   - Blnk's matcher (reconciliation.go matchesRules/matchesString/
//     matchesGroupAmount) compares each EXTERNAL field against the INTERNAL
//     candidate's same field and IGNORES MatchingCriteria.Value; populating
//     Value therefore records intent but cannot itself change what Blnk matches.
//   - Blnk's start-instant PERSISTS every probed external transaction even under
//     dry_run, and matches against that shared, growing transaction set, so an
//     amount-only equality can clear by coincidentally matching a stray
//     same-amount transaction left by an earlier run rather than the intended
//     seed booking.
//
// This narrowing applies the maximum tightening the HTTP-only contract permits
// (populated Value + currency pinning); the remaining non-determinism is a
// property of the off-limits Blnk engine on a shared database, not of the agent.
func narrowProposedRule(rule blnk.MatchingRule, txn blnk.ExternalTransaction) blnk.MatchingRule {
	out := rule
	out.Criteria = append([]blnk.MatchingCriteria(nil), rule.Criteria...)

	hasCurrency := false
	for i := range out.Criteria {
		c := &out.Criteria[i]
		if c.Field == fieldCurrency {
			hasCurrency = true
		}
		// Only populate the Value of tight equality criteria; leave any
		// range/substring operator (which autoApplySafe rejects anyway) untouched.
		if c.Operator != operatorEquals {
			continue
		}
		switch c.Field {
		case fieldAmount:
			c.Value = strconv.FormatFloat(txn.Amount, 'f', -1, 64)
		case fieldCurrency:
			c.Value = txn.Currency
		case fieldReference:
			c.Value = txn.Reference
		case fieldDescription:
			c.Value = txn.Description
		case fieldDate:
			if !txn.Date.IsZero() {
				c.Value = txn.Date.Format(time.RFC3339)
			}
		}
	}

	// Append a currency equality criterion when the break carries a currency and
	// the proposal does not already constrain it, so an amount-only proposal
	// cannot clear across a currency boundary.
	if !hasCurrency && txn.Currency != "" {
		out.Criteria = append(out.Criteria, blnk.MatchingCriteria{
			Field:    fieldCurrency,
			Operator: operatorEquals,
			Value:    txn.Currency,
		})
	}
	return out
}

// rollbackCreatedRule best-effort deletes a Blnk matching rule the remediator
// created in the current invocation for an auto-remediation attempt that did
// not clear, and clears the persisted created-rule id on the break (finding
// F5). Without this, every non-clearing auto-attempt would leave an orphan rule
// in Blnk's shared catalog (observed accumulating 30 -> 88 during QA).
//
// Both steps are best-effort by design: their failures never abort the caller,
// because the break MUST still route to HITL (Rule 5.7 fail-closed) even when
// catalog cleanup cannot complete. Only a rule created in THIS call is ever
// passed here (createdHere gate) — a reused, already-persisted rule is never
// deleted.
//
// The persisted created-rule id is cleared ONLY when the Blnk delete actually
// succeeded: on success, clearing it prevents a later HITL re_drive from
// reusing a now-deleted rule; on failure, the rule likely still exists in Blnk,
// so we KEEP our reference to it (rather than clearing it and forgetting an
// orphan) — a re_drive can still reuse it or a human can reconcile it.
func (r *Remediator) rollbackCreatedRule(ctx context.Context, externalTxnID, ruleID string) {
	if ruleID == "" {
		return
	}
	if err := r.blnkClient.DeleteMatchingRule(ctx, ruleID); err != nil {
		return
	}
	_ = r.store.SetCreatedRule(ctx, externalTxnID, "")
}

// autoApplySafe reports whether a grammar-valid proposed rule is tight enough to
// be auto-applied to a single-transaction Blnk dry-run (finding C3). Blnk matches
// the external transaction against internal transactions field-to-field and
// ignores criteria.Value, so a non-equality operator (greater_than, less_than,
// contains) can match an unintended internal counterpart and make the probe
// falsely report the break cleared. Only equality criteria pin external ==
// internal on a field, so every criterion must use the equals operator; any other
// operator returns an error and the break is escalated to HITL instead of
// auto-applied. The rule's grammar (non-empty criteria, allowed fields/operators)
// is validated separately by classifier.ValidateRule before this check.
func autoApplySafe(rule blnk.MatchingRule) error {
	for _, c := range rule.Criteria {
		if c.Operator != operatorEquals {
			return fmt.Errorf("criterion on field %q uses non-equality operator %q; only %q may be auto-applied", c.Field, c.Operator, operatorEquals)
		}
	}
	return nil
}

// validConfidence reports whether c is a finite probability in [0,1]. A
// confidence that is NaN, ±Inf, or out of range is treated as malformed and
// fails the break closed to HITL (finding m-4).
//
// This is the CONSUMER half of recon-agent's two-layer confidence policy
// (finding SEAM-INFO-1); the PRODUCER half is classifier.clampConfidence, which
// normalizes every raw model confidence into [0,1] before it leaves the
// classifier. The two layers form ONE coherent policy, not two competing
// philosophies: a confidence produced by the classifier is already clamped, so
// this check passes it through untouched (a legitimately clamped value — e.g. a
// model over-confidence normalized to 1.0 — is never falsely escalated, and is
// then decided by Blnk's deterministic arbiter per Rule 5.3). The check exists
// as defense in depth for confidences that did NOT pass through the clamp:
// values reconstructed from the store on resume, injected by tests, or emitted
// by any future non-clamping producer. If such a value reaches the gate
// malformed, failing closed here prevents a NaN comparison from silently
// defeating the confidence gate (Rule 5.4).
func validConfidence(c float64) bool {
	return !math.IsNaN(c) && !math.IsInf(c, 0) && c >= 0 && c <= 1
}

// safeConfidence clamps a possibly-malformed confidence to a value the audit
// validator accepts (finite, in [0,1]); malformed inputs collapse to 0 so an
// escalated event can still be recorded for the break. It mirrors the
// producer-side classifier.clampConfidence for the escalation path (finding
// SEAM-INFO-1), keeping the two layers of the confidence policy consistent.
func safeConfidence(c float64) float64 {
	if !validConfidence(c) {
		return 0
	}
	return c
}

// keyedMutex provides per-key mutual exclusion. It serializes concurrent Handle
// calls for the SAME break (keyed by external transaction id) so a break is
// never processed twice concurrently (finding M-5), while distinct breaks
// acquire distinct keys and proceed in parallel. Entries are reference-counted
// and deleted when the last holder releases, so the map does not grow without
// bound over a long-running pipeline.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*refCountedLock
}

type refCountedLock struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the per-key mutex and returns a release function. Callers must
// invoke the returned function exactly once (typically via defer).
func (k *keyedMutex) lock(key string) func() {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = make(map[string]*refCountedLock)
	}
	rl, ok := k.locks[key]
	if !ok {
		rl = &refCountedLock{}
		k.locks[key] = rl
	}
	// Register interest BEFORE releasing the guard so the entry cannot be
	// deleted out from under a waiter.
	rl.refs++
	k.mu.Unlock()

	rl.mu.Lock()
	return func() {
		rl.mu.Unlock()
		k.mu.Lock()
		rl.refs--
		if rl.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
