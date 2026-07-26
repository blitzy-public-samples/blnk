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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Break lifecycle status strings persisted through the store. They MUST match
// the values the store and HITL status page understand, so each name aliases
// the canonical literal in internal/model (finding m-01) — the single
// module-wide source of truth — while local use sites keep their concise
// unqualified names.
const (
	statusClassified   = model.StatusClassified
	statusAutoResolved = model.StatusAutoResolved
	statusQueued       = model.StatusQueued
)

// Human-readable escalation reasons recorded on the HITL queue entry and on the
// rationale of the escalated AuditEvent.
const (
	reasonRegulated                = "regulated break requires human review"
	reasonForeignCurrency          = "break currency differs from the ledger base currency; treated as regulated (independent non-LLM backstop) and routed to human review"
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
	reasonUnsafeProfile            = "no safe root-cause-specific rule profile could be built for this break; cannot auto-remediate"
)

// Auto-remediation safety and durability tuning constants.
const (
	// maxAmountDriftFraction caps the AllowableDrift the agent will auto-apply on
	// an amount_drift break's amount criterion (finding C-06). Blnk's
	// matchesGroupAmount treats an equality criterion's AllowableDrift as a
	// FRACTION of the internal amount (|ext-int| <= |int|*drift), so 0.05 permits
	// at most a 5% amount difference to auto-clear. A classifier-proposed drift
	// above this cap is clamped DOWN to the cap (never up); a missing, non-finite,
	// or non-positive proposal collapses to 0 (exact amount match). This bounds
	// financial exposure so a large amount discrepancy can never be silently
	// auto-cleared as "drift".
	maxAmountDriftFraction = 0.05

	// escalateTimeout bounds the independent fail-closed context used to route a
	// break to HITL (finding M-10). Escalation must not be defeated by a canceled
	// or deadline-exceeded parent context (e.g. an LLM timeout), so it runs on a
	// context that keeps the parent's values but drops its cancellation, bounded
	// by this timeout.
	escalateTimeout = 10 * time.Second

	// compensateTimeout bounds the independent fail-closed context used to delete
	// a non-clearing created rule from Blnk and audit the outcome (finding M-11),
	// for the same reason as escalateTimeout.
	compensateTimeout = 10 * time.Second

	// remediationLeaseTTL is how long a database processing lease is held while a
	// break undergoes external remediation (finding M-15). It bounds how long a
	// crashed holder blocks another instance: after it elapses the lease becomes
	// reclaimable. It is a durable, cross-instance complement to the in-process
	// keyedMutex.
	remediationLeaseTTL = 2 * time.Minute
)

// DefaultClassifyBudget bounds a SINGLE break's classification (INFO#3). The
// classifier retries up to defaultMaxRetries (2) after the initial attempt, each
// attempt self-bounded by defaultPerAttemptTimeout (30s), so a HUNG endpoint
// could otherwise make one break's classification consume up to ~90s — the whole
// pipelineTimeout — starving every remaining break. propose() wraps Classify in
// a context bounded by this budget when it is > 0, so a single hung inference
// fails THAT break closed to HITL (Rule 5.7) after ~one attempt's worth of hang
// without aborting or starving the run. It is generous for a healthy Kimi K3
// response yet finite. cmd passes this to remediator.New; the unit tests pass 0
// to leave the caller's context unwrapped (the fake classifier returns
// immediately, so no budget is needed there).
const DefaultClassifyBudget = 30 * time.Second

// operatorEquals is the only matching operator tight enough to auto-apply to a
// single-transaction dry-run (finding C3). Blnk compares the external transaction
// against internal transactions FIELD-TO-FIELD and IGNORES criteria.Value for all
// operators (see Blnk reconciliation.go matchesString / matchesGroupAmount /
// matchesCurrency), so a range/substring operator (greater_than / less_than /
// contains) can match an UNINTENDED internal counterpart and make a single-txn
// probe falsely report "cleared". Equality pins external == internal on a field.
// Aliases the canonical grammar operator in internal/model (finding m-01).
const operatorEquals = model.OperatorEquals

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
	// EstablishMainReconciliation runs the pipeline's INITIAL dry-run
	// reconciliation over the uploaded statement and returns its reconciliation
	// id (the main_recon_id). It implements the mandated Upload -> start -> read
	// topology (finding F02); the remediator stamps the returned id onto every
	// break's provenance so each break points at the batch reconciliation that
	// surfaced it. It is a dry run (Rule 5.3): it never mutates Blnk state.
	EstablishMainReconciliation(ctx context.Context, uploadID string, matchingRuleIDs []string) (reconID string, err error)
	// ProbeBreak is the PER-BREAK DETERMINISTIC ARBITER (Rule 5.3). It submits a
	// SINGLE external transaction as its own dry-run (many_to_one, cache-safe) and
	// reports cleared iff Blnk matched that transaction (matched>=1). Adjudicating
	// each break with its OWN probe lets a confirmed break resolve independently
	// even when a sibling in the same run remains unmatched (finding F18),
	// replacing the earlier all-or-nothing whole-cohort dry-run. The returned
	// reconciliation id is the clearance proof recorded on the resolved break; a
	// probe error fails the break closed to HITL (Rule 5.7).
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
	UpsertBreakTx(ctx context.Context, tx *sql.Tx, c model.BreakClassification, txn blnk.ExternalTransaction, prov model.Provenance, status string) error
	// StampMainReconID records the run's initial (batch) reconciliation id as the
	// main_recon_id of every listed break, but ONLY where that column is still
	// blank, so it fills the provenance the initial run establishes (finding F02)
	// without ever overwriting a value already recorded. Autocommit; safe to call
	// once per run after the initial reconciliation completes.
	StampMainReconID(ctx context.Context, mainReconID string, externalTxnIDs []string) error
	// MarkResolvedTx transitions a break to auto-resolved AND stamps the
	// confirming Blnk dry-run reconciliation id onto the row in one statement.
	// The store's agent_break_resolved_proof CHECK makes an auto-resolved row
	// without that proof impossible, so the remediator can never record a
	// resolution the arbiter did not confirm (Rule 5.3). It replaces a bare
	// status write for the resolve path.
	MarkResolvedTx(ctx context.Context, tx *sql.Tx, externalTxnID, reconID string) error
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

	// ClaimBreak acquires (or renews) a durable, cross-instance processing lease
	// on a break before the remediator performs external remediation (finding
	// M-15). It returns granted=false with a nil error when another live owner
	// currently holds the lease, so the caller yields rather than racing. This
	// is the database-level complement to the in-process keyedMutex.
	ClaimBreak(ctx context.Context, externalTxnID, owner string, ttl time.Duration) (granted bool, err error)
	// ReleaseBreak releases a processing lease this instance holds, letting a
	// retry or another instance reclaim the break (finding M-15).
	ReleaseBreak(ctx context.Context, externalTxnID, owner string) error

	// RecordRuleOutboxTx records, in the caller's transaction, that a Blnk rule
	// was created for a break and now owes confirmation or compensation (finding
	// M-11). Committed atomically with the created-rule id + rule_created audit
	// event so a crash can never create a rule without a durable cleanup
	// obligation.
	RecordRuleOutboxTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error
	// ConfirmRuleOutboxTx retires a rule's compensation obligation in the
	// caller's transaction because the rule legitimately cleared its break
	// (finding M-11). Committed atomically with MarkResolvedTx.
	ConfirmRuleOutboxTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error
	// CompensateRuleOutbox marks the pending obligation for (externalTxnID,
	// ruleID) compensated (ok) or compensation_failed (!ok) after an in-call rule
	// deletion attempt (finding M-11). Idempotent and tolerant of a missing row.
	CompensateRuleOutbox(ctx context.Context, externalTxnID, ruleID string, ok bool, errMsg string) error
	// ListPendingRuleOutbox returns pending compensation obligations left by a
	// crashed attempt, for the startup recovery sweep (finding M-11).
	ListPendingRuleOutbox(ctx context.Context, limit int) ([]store.RuleOutboxItem, error)
	// MarkRuleOutboxCompensated records, by row id, the outcome of compensating a
	// pending obligation during the startup recovery sweep (finding M-11).
	MarkRuleOutboxCompensated(ctx context.Context, id int64, ok bool, errMsg string) error
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

	// baseCurrency is the ledger's settlement currency (config AGENT_BASE_CURRENCY;
	// may be empty). When non-empty it is an INDEPENDENT, non-LLM regulated
	// backstop (INFO#2): a break whose currency differs is treated as regulated
	// and escalated regardless of the classifier's (LLM-sourced, thus
	// prompt-injectable) regulated flag, so a foreign-currency break can never be
	// auto-remediated on the strength of a flipped flag alone. Empty disables the
	// backstop; the deterministic per-break dry-run probe (which cannot match
	// across currencies) remains the always-on independent arbiter (Rule 5.3).
	baseCurrency string

	// classifyBudget bounds a single break's classification (INFO#3). See
	// DefaultClassifyBudget. Zero leaves the caller's context unwrapped.
	classifyBudget time.Duration

	// owner uniquely identifies THIS remediator instance when it claims a
	// database processing lease on a break (finding M-15). It is generated once
	// per instance in New so that leases this instance takes can be told apart
	// from those of any other instance (or a crashed prior incarnation).
	owner string

	// locks serializes concurrent Handle calls for the SAME break so a break is
	// never classified/remediated twice by racing goroutines (finding M-5).
	// Distinct breaks acquire distinct keys and still run in parallel. It is the
	// in-process fast path; the database lease (owner + storePort.ClaimBreak) is
	// the durable, cross-instance complement (finding M-15).
	locks keyedMutex
}

// New wires the remediator with its collaborators. The parameters are
// consumer-side interfaces so tests can inject fakes; the concrete
// *classifier.Classifier, *blnk.Client, *audit.Writer and *store.Store returned
// by the module's own constructors satisfy them structurally.
//
// baseCurrency (config AGENT_BASE_CURRENCY, may be empty) is the independent
// non-LLM regulated backstop of INFO#2; classifyBudget (see DefaultClassifyBudget)
// bounds a single break's classification per INFO#3. Tests pass baseCurrency=""
// and classifyBudget=0 to exercise the pre-INFO behavior unchanged.
func New(cls classifierPort, blnkClient blnkPort, auditWriter auditPort, store storePort, threshold float64, llmModel string, baseCurrency string, classifyBudget time.Duration) *Remediator {
	return &Remediator{
		cls:            cls,
		blnkClient:     blnkClient,
		auditWriter:    auditWriter,
		store:          store,
		threshold:      threshold,
		llmModel:       llmModel,
		baseCurrency:   strings.TrimSpace(baseCurrency),
		classifyBudget: classifyBudget,
		// A unique owner identity per instance so database processing leases
		// (finding M-15) taken by this remediator are distinguishable from any
		// other instance's.
		owner: "recon-agent-" + uuid.NewString(),
	}
}

// foreignCurrency reports whether the break's currency differs from the ledger
// base currency, implementing the INFO#2 independent (non-LLM) regulated
// backstop. It returns false when no base currency is configured (the backstop
// is disabled) or when the currencies match case-insensitively; a mismatch means
// the break must be treated as regulated and routed to human review regardless
// of the classifier's LLM-sourced regulated flag. Comparing on the transaction's
// OWN currency field keeps this backstop entirely independent of model output,
// so a prompt-injection that flips regulated=false cannot open the auto path for
// a cross-currency break.
func (r *Remediator) foreignCurrency(txn blnk.ExternalTransaction) bool {
	if r.baseCurrency == "" {
		return false
	}
	return !strings.EqualFold(strings.TrimSpace(txn.Currency), r.baseCurrency)
}

// Handle processes exactly one external break end to end. It is the
// single-break convenience wrapper over ProcessCohort, preserving the original
// single-break API for callers (and the unit suite) that triage one break at a
// time. A batch of one still gets its own per-break dry-run probe, so a
// matched>=1 verdict proves THAT break cleared — the deterministic-arbiter
// guarantee (Rule 5.3) holds exactly as for a multi-break batch.
//
// It returns nil once the break has reached a terminal, safely-recorded outcome
// (auto-resolved or escalated to HITL). It returns a non-nil error only when an
// agent-side infrastructure operation (persistence or audit write) fails; such
// errors never leave a break auto-resolved.
//
// uploadID identifies the reconciliation upload batch this break originated
// from. It is stamped into the provenance of EVERY audit event the break emits
// (finding m-2), so the append-only trail attributes each action to its source
// upload and per-run summaries can be scoped correctly (finding M-6).
func (r *Remediator) Handle(ctx context.Context, txn blnk.ExternalTransaction, uploadID string) error {
	return r.ProcessCohort(ctx, []blnk.ExternalTransaction{txn}, uploadID)
}

// pendingRemediation carries a break that propose() advanced to the point of
// having a created (or reused) Blnk matching rule awaiting its per-break
// clearance confirmation. finalize() consumes it after that break's OWN
// single-transaction dry-run probe. Its release closure holds the break's M-5
// in-process mutex AND M-15 durable lease and MUST be invoked exactly once —
// finalize defers it — so both are released only after the break reaches its
// terminal outcome.
type pendingRemediation struct {
	txn            blnk.ExternalTransaction
	classification model.BreakClassification
	prov           model.Provenance // base provenance (model, source, upload)
	provEv         model.Provenance // prov + classification evidence (auto-path events)
	id             string           // classification.ExternalTxnID (== txn.ID)
	ruleID         string           // the Blnk rule created/reused for this break
	createdHere    bool             // true iff THIS run created the rule (gates rollback)
	release        func()           // releases M-5 mutex + M-15 lease; call exactly once
}

// ProcessCohort triages a batch of breaks with the deterministic-arbiter
// guarantee (Rule 5.3) and the per-break independence that fixes findings
// F02/F18. It runs in three phases:
//
//  1. PROPOSE every break independently (classify -> gate -> build+create the
//     safe rule), collecting those that reached the point of a created/reused
//     rule awaiting confirmation. Breaks that terminate during propose
//     (already-terminal no-op, classifier fail-closed, gate escalation, unsafe
//     profile, rule-create failure, ...) are fully resolved there and are not
//     carried forward.
//  2. ESTABLISH the run's INITIAL reconciliation over the uploaded statement
//     (finding F02): a single dry-run POST /reconciliation/start followed by a
//     GET read, yielding the main_recon_id. That id is stamped onto EVERY
//     break's persisted row (StampMainReconID) and onto each pending break's
//     provenance, so every break points at the batch reconciliation that
//     surfaced it. The initial run needs at least one matching rule id (Blnk
//     rejects an empty set), so it uses the ids of the rules just created for
//     the auto-eligible breaks; its counts are not consulted for any per-break
//     decision. When no break is auto-eligible there is no rule to run it with
//     and nothing to auto-remediate, so the initial run is skipped.
//  3. CONFIRM + FINALIZE each pending break INDEPENDENTLY (finding F18): each
//     break gets its OWN single-transaction dry-run probe (ProbeBreak, cache-safe
//     many_to_one) and is finalized with its OWN verdict. A break Blnk confirms
//     matched is resolved (stamped with that probe's reconciliation id, Rule
//     5.3) EVEN IF a sibling break in the same run remains unmatched; an
//     unmatched or errored probe fails only THAT break closed to HITL and
//     compensates only its own rule (Rule 5.7). This replaces the earlier
//     all-or-nothing whole-cohort dry-run under which one unmatched member
//     suppressed every valid resolution (F18).
//
// It returns the joined error of any per-break infrastructure failures (nil when
// every break reached a terminal, safely-recorded outcome). Each break's M-5
// mutex and M-15 lease are held from its propose() through its finalize().
func (r *Remediator) ProcessCohort(ctx context.Context, breaks []blnk.ExternalTransaction, uploadID string) error {
	var (
		pending []*pendingRemediation
		errs    []error
	)

	// Phase 1: propose each break independently. A propose error is collected but
	// does not abort the cohort — other breaks must still be triaged.
	for _, txn := range breaks {
		p, err := r.propose(ctx, txn, uploadID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if p != nil {
			pending = append(pending, p)
		}
	}

	// No break reached the confirmation phase: every one terminated in propose.
	// With nothing auto-eligible there is no rule to run an initial reconciliation
	// with and nothing to auto-remediate, so the initial run is skipped.
	if len(pending) == 0 {
		return errors.Join(errs...)
	}

	// Phase 2 (F02): establish the run's initial reconciliation over the upload
	// and stamp its id as every break's main_recon_id provenance. A failure here
	// is recorded but does NOT abort triage: the per-break probes below remain the
	// authoritative clearance signal, and a break simply carries no main_recon_id
	// rather than being dropped. The initial run uses the auto-eligible breaks'
	// rule ids because Blnk requires a non-empty rule set.
	ruleIDs := make([]string, len(pending))
	for i, p := range pending {
		ruleIDs[i] = p.ruleID
	}
	mainReconID, mainErr := r.blnkClient.EstablishMainReconciliation(ctx, uploadID, ruleIDs)
	if mainErr != nil {
		errs = append(errs, fmt.Errorf("remediator: establish main reconciliation: %w", mainErr))
	}
	if mainReconID != "" {
		// Stamp the main reconciliation id onto EVERY break in this run (eligible
		// and already-escalated), so no break's persisted row leaves main_recon_id
		// blank (finding F02). StampMainReconID only fills a blank column, so it
		// never overwrites a resolution's proof id.
		breakIDs := make([]string, len(breaks))
		for i, txn := range breaks {
			breakIDs[i] = txn.ID
		}
		if serr := r.store.StampMainReconID(ctx, mainReconID, breakIDs); serr != nil {
			errs = append(errs, fmt.Errorf("remediator: stamp main reconciliation id: %w", serr))
		}
		// Thread the id into each pending break's provenance so its probe/resolve
		// audit events also carry it.
		for _, p := range pending {
			p.prov.MainReconID = mainReconID
			p.provEv.MainReconID = mainReconID
		}
	}

	// Phase 3 (F18): adjudicate EACH pending break with its OWN single-transaction
	// dry-run probe and finalize it with its OWN verdict, so a confirmed break
	// resolves independently of any unmatched sibling. A probe transport/validation
	// error yields a false verdict + error so finalize fails only THAT break closed
	// (Rule 5.7).
	for _, p := range pending {
		cleared, reconID, probeErr := r.blnkClient.ProbeBreak(ctx, p.txn, []string{p.ruleID})
		if err := r.finalize(ctx, p, cleared, reconID, probeErr); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// propose advances one break from classification through creating (or reusing)
// its safe Blnk matching rule, up to — but NOT including — its per-break
// clearance confirmation. It returns:
//
//   - (nil, nil)  when the break reached a terminal outcome here (already
//     terminal, classifier fail-closed, gate escalation, unsafe profile,
//     orphaned-rule resume, empty/failed rule creation, ...): it performed the
//     escalation itself and released the break's mutex/lease.
//   - (nil, err)  when an agent-side persistence/audit operation failed; the
//     mutex/lease are released.
//   - (&pendingRemediation, nil)  when a rule was created/reused and the break
//     awaits its per-break confirmation probe; the returned release closure
//     still holds the break's mutex AND lease and MUST be invoked by finalize.
//
// It preserves every durability guarantee of the original monolithic Handle:
// M-5 in-process serialization, M-6 idempotency/crash-resume, M-2 atomic
// classify+audit, M-15 durable lease, the Rule 5.4 confidence/regulation gate,
// the INFO#2 currency backstop, the C3 root-cause allow-list, the SEAM-MIN-1
// orphaned-rule guard, and the M-3/M-11 atomic rule-create + outbox.
func (r *Remediator) propose(ctx context.Context, txn blnk.ExternalTransaction, uploadID string) (*pendingRemediation, error) {
	// M-5: serialize concurrent processing of the SAME break. Unlike the original
	// Handle this is NOT deferred: the mutex must stay held across the cohort
	// confirmation and finalize, so it is released explicitly on every terminal
	// path here and, on the success path, by finalize via the release closure.
	unlockMutex := r.locks.lock(txn.ID)

	prov := model.Provenance{
		Model:    r.llmModel,
		Source:   txn.Source,
		UploadID: uploadID,
	}

	// M-6 idempotency / crash-resume: consult the persisted state first.
	loaded, status, createdRuleID, found, err := r.store.LoadBreak(ctx, txn.ID)
	if err != nil {
		unlockMutex()
		return nil, fmt.Errorf("remediator: load break %q: %w", txn.ID, err)
	}

	var classification model.BreakClassification
	switch {
	case found && (status == statusAutoResolved || status == statusQueued):
		// Terminal outcome already recorded. A re-run (or the loser of a race)
		// is a no-op, so a resolved/escalated event is never double-counted.
		unlockMutex()
		return nil, nil

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
		//
		// INFO#3: bound this single break's classification by r.classifyBudget so
		// a HUNG LLM endpoint cannot let one break consume the whole run's budget
		// (the classifier retries up to the Rule 5.7 cap, each attempt self-bounded
		// but summing to ~90s). When the budget elapses, Classify returns and the
		// break fails closed to HITL below. A zero budget (unit tests) leaves the
		// caller's context unwrapped.
		classifyCtx := ctx
		if r.classifyBudget > 0 {
			var cancelClassify context.CancelFunc
			classifyCtx, cancelClassify = context.WithTimeout(ctx, r.classifyBudget)
			defer cancelClassify()
		}
		c, cerr := r.cls.Classify(classifyCtx, txn)
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
			escErr := r.escalate(ctx, txn, stub, prov, reason)
			unlockMutex()
			return nil, escErr
		}

		// Defensive: guarantee the break is keyed by the transaction under
		// management regardless of what the classifier populated.
		c.ExternalTxnID = txn.ID

		// m-4: a malformed confidence (NaN/Inf or outside [0,1]) must fail closed
		// to HITL, never silently flow into the auto path (NaN comparisons make
		// the gate below ineffective). No classified event is emitted; escalate
		// sanitizes the confidence so the escalated event is itself well-formed.
		if !validConfidence(c.Confidence) {
			escErr := r.escalate(ctx, txn, c, prov, fmt.Sprintf("%s (confidence=%v)", reasonInvalidConfidence, c.Confidence))
			unlockMutex()
			return nil, escErr
		}

		// M-2: persist the classified break and emit the classified AuditEvent in
		// ONE transaction, so a classified break can never exist without its
		// audit event (nor an audit event without the break).
		if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
			if e := r.store.UpsertBreakTx(ctx, tx, c, txn, prov, statusClassified); e != nil {
				return e
			}
			return r.auditWriter.RecordTx(ctx, tx, audit.Classified(c, prov))
		}); err != nil {
			unlockMutex()
			return nil, fmt.Errorf("remediator: persist+audit classified break %q: %w", txn.ID, err)
		}
		classification = c
	}

	id := classification.ExternalTxnID

	// M-15: acquire a durable, cross-instance processing lease before performing
	// any external remediation. The in-process keyedMutex above serializes racing
	// goroutines WITHIN this process; the lease additionally guarantees that at
	// most one AGENT INSTANCE drives a given break's Blnk rule creation / dry-run
	// probe / resolution at a time — something a process-local mutex cannot. The
	// break row now exists (a fresh break was just persisted as classified; a
	// resumed one was loaded), so the lease is claimable. If another live
	// instance already holds it, we YIELD: the break is being handled elsewhere,
	// so this call is a safe no-op rather than a competing (and potentially
	// rule-duplicating) second attempt.
	//
	// A fresh break may be classified by two instances in the brief window before
	// either claims (both upsert the same row and append a classified event); the
	// only artifact is a duplicate classified audit event, never a duplicate
	// resolution or Blnk rule, because exactly one instance wins the lease that
	// gates all external work below.
	granted, claimErr := r.store.ClaimBreak(ctx, id, r.owner, remediationLeaseTTL)
	if claimErr != nil {
		unlockMutex()
		return nil, fmt.Errorf("remediator: claim processing lease for break %q: %w", id, claimErr)
	}
	if !granted {
		unlockMutex()
		return nil, nil
	}
	// release retires the break's durable lease (M-15) and then its in-process
	// mutex (M-5). Unlike the original monolithic Handle's deferred cleanup it is
	// NOT fired here: the lease and mutex must stay held across the per-break
	// clearance confirmation, so every terminal path in propose() invokes
	// release() explicitly, and the success path hands it to finalize(), which
	// defers it. The lease release runs on an independent context so a canceled
	// parent cannot leave the lease dangling; if it still fails, the lease
	// self-expires after remediationLeaseTTL. It is invoked exactly once.
	release := func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), escalateTimeout)
		defer cancel()
		_ = r.store.ReleaseBreak(relCtx, id, r.owner)
		unlockMutex()
	}

	// Confidence/regulation gate (Rule 5.4). A regulated break, or one below the
	// confidence threshold, is NEVER auto-remediated: it is escalated to HITL
	// with no resolved event. (On resume this re-applies the gate to a break a
	// crashed attempt left classified.)
	if classification.Regulated {
		escErr := r.escalate(ctx, txn, classification, prov, reasonRegulated)
		release()
		return nil, escErr
	}
	// INFO#2 independent, non-LLM regulated backstop. The classifier's regulated
	// flag is entirely LLM-sourced and thus prompt-injectable; this backstop
	// treats a break whose currency differs from the ledger base currency as
	// regulated REGARDLESS of that flag, so a flipped regulated=false can never
	// open the auto path for a foreign-currency break. It is defense-in-depth
	// ABOVE the always-on deterministic per-break dry-run probe (which
	// independently cannot match across currencies, Rule 5.3). Disabled when
	// baseCurrency is empty; a single break whose currency equals itself is
	// unaffected.
	if r.foreignCurrency(txn) {
		escErr := r.escalate(ctx, txn, classification, prov, reasonForeignCurrency)
		release()
		return nil, escErr
	}
	if classification.Confidence < r.threshold {
		escErr := r.escalate(ctx, txn, classification, prov, reasonLowConfidence)
		release()
		return nil, escErr
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
		escErr := r.escalate(ctx, txn, classification, prov, reasonRootCauseNotAutoEligible)
		release()
		return nil, escErr
	}

	// Auto path. The agent only PROPOSES; Blnk DECIDES (Rule 5.3).
	if classification.ProposedRule == nil {
		escErr := r.escalate(ctx, txn, classification, prov, reasonMissingRule)
		release()
		return nil, escErr
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
			release()
			return nil, fmt.Errorf("remediator: check prior rule proposal for break %q: %w", id, err)
		}
		if proposedCount > 0 {
			escErr := r.escalate(ctx, txn, classification, prov, reasonOrphanedRuleResume)
			release()
			return nil, escErr
		}

		// C-06: build the EXACT, deterministic safe rule profile for this break's
		// (already gated, auto-eligible) root cause, rather than trusting the
		// classifier's advisory rule STRUCTURE. The classifier's proposal only
		// SIGNALS that the break looked auto-remediable — its nil-ness gated us
		// above — but the rule the agent actually POSTs is constructed here from
		// the root cause and the transaction's OWN identity fields, so clearance
		// pins the intended counterpart and can never be steered by weakly-bounded
		// model output. safeRuleForBreak returns an error when no safe profile
		// applies (e.g. a required identity field is missing), failing the break
		// closed to HITL. See safeRuleForBreak for the per-root-cause profiles.
		rule, ruleErr := safeRuleForBreak(classification, txn)
		if ruleErr != nil {
			escErr := r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonUnsafeProfile, ruleErr))
			release()
			return nil, escErr
		}

		// Rule 5.2: never POST a rule whose Field/Operator falls outside Blnk's
		// accepted grammar. safeRuleForBreak only ever emits allowed fields with
		// the equals operator, so this is a defensive assertion immediately before
		// the POST, validating the ACTUAL rule that will be sent.
		if err := classifier.ValidateRule(rule); err != nil {
			escErr := r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonGrammarReject, err))
			release()
			return nil, escErr
		}

		// Semantic safety before any POST (finding C-06): every criterion the
		// agent auto-applies must be a tight equality pin. Blnk's field-to-field
		// matcher ignores criteria.Value, and a range/substring operator could
		// match an unintended internal counterpart and falsely "clear" the probe,
		// so a non-equality operator routes to HITL. safeRuleForBreak builds only
		// equality criteria, so this is a defensive assertion that the built
		// profile conforms.
		if err := autoApplySafe(rule); err != nil {
			escErr := r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonOverbroadRule, err))
			release()
			return nil, escErr
		}

		// m-3: record the proposal (previously dead-code audit builder) before
		// any POST to Blnk.
		if err := r.auditWriter.Record(ctx, audit.RuleProposed(id, ruleRefOf(rule), classification.Confidence, provEv)); err != nil {
			release()
			return nil, fmt.Errorf("remediator: audit proposed rule for break %q: %w", id, err)
		}

		created, err := r.blnkClient.CreateMatchingRule(ctx, rule)
		if err != nil {
			escErr := r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonRuleCreateFailed, err))
			release()
			return nil, escErr
		}
		// m-7: guard an empty rule id BEFORE probing — Blnk's instant
		// reconciliation requires a non-empty matching_rule_ids set, and probing
		// with [""] would be meaningless. Fail closed to HITL.
		if created.RuleID == "" {
			escErr := r.escalate(ctx, txn, classification, prov, reasonEmptyRuleID)
			release()
			return nil, escErr
		}
		ruleID = created.RuleID
		createdHere = true

		// M-3 / m-3 / M-11: persist the created rule id, record the durable
		// compensation obligation (a 'pending' outbox row), and emit the
		// rule_created audit event — all ATOMICALLY in ONE transaction. Committing
		// them together means a crash can never leave an orphaned Blnk rule
		// WITHOUT also recording the pending obligation to clean it up (the
		// outbox) and its audit event; on resume the persisted id is reused
		// instead of creating another rule.
		if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
			if e := r.store.SetCreatedRuleTx(ctx, tx, id, ruleID); e != nil {
				return e
			}
			if e := r.store.RecordRuleOutboxTx(ctx, tx, id, ruleID); e != nil {
				return e
			}
			return r.auditWriter.RecordTx(ctx, tx, audit.RuleCreated(id, ruleRefOf(created), classification.Confidence, provEv))
		}); err != nil {
			// M-11: the local commit failed but the Blnk rule EXISTS (the POST
			// already succeeded), so without cleanup it is an orphan the agent has
			// no durable record of. Compensate immediately — delete it from Blnk
			// and audit the outcome (including a delete failure) — then fail the
			// break closed to HITL. Compensation runs on an independent fail-closed
			// context so a canceled parent cannot skip it. (The outbox row was part
			// of THIS failed transaction and so was never committed; the
			// compensation's outbox mark is therefore a harmless no-op.)
			r.compensateCreatedRule(ctx, id, ruleID, provEv, classification.Confidence, "created-rule local commit failed")
			escErr := r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonRuleCreateFailed, err))
			release()
			return nil, escErr
		}
	}

	// propose has created (or reused) the break's safe rule and durably recorded
	// its compensation obligation. The break now awaits its per-break clearance
	// probe (ProcessCohort phase 3), so hand it forward with its M-5 mutex AND
	// M-15 lease STILL HELD — release is deliberately NOT called here; finalize()
	// invokes it exactly once. Blnk remains the sole arbiter of clearance (Rule
	// 5.3): propose never marks a resolution, it only prepares the break for its
	// deterministic single-transaction dry-run probe.
	return &pendingRemediation{
		txn:            txn,
		classification: classification,
		prov:           prov,
		provEv:         provEv,
		id:             id,
		ruleID:         ruleID,
		createdHere:    createdHere,
		release:        release,
	}, nil
}

// finalize applies THIS break's OWN clearance verdict to one pending break and
// releases its M-5 mutex + M-15 lease exactly once (deferred). It reproduces the
// tail of the original monolithic Handle — probe audit, fail-closed
// compensation, and the atomic auto-resolve — keyed off the PER-BREAK
// (cleared, reconID, confirmErr) that ProcessCohort obtained from THIS break's
// single-transaction dry-run probe (ProbeBreak). Because that verdict comes from
// a probe over exactly this one transaction, a matched>=1 result is attributable
// to a dry-run that genuinely cleared THIS break, and a sibling break's verdict
// never affects it (finding F18, Rule 5.3). On a false or error verdict it fails
// only THIS break closed to HITL (Rule 5.7) and compensates only its own rule, so
// a failed attempt leaves neither an orphan rule nor an unprovable resolution.
func (r *Remediator) finalize(ctx context.Context, p *pendingRemediation, cleared bool, reconID string, confirmErr error) error {
	// Release the break's mutex + lease exactly once, however finalize returns.
	defer p.release()

	txn := p.txn
	classification := p.classification
	prov := p.prov
	provEv := p.provEv
	id := p.id
	ruleID := p.ruleID
	createdHere := p.createdHere

	if confirmErr != nil {
		// M-11: this break's probe transport/validation failed, so its rule cannot
		// be proven to clear the break. If we created it in this run, durably
		// compensate it (delete from Blnk, mark the outbox obligation, audit the
		// outcome — including a delete failure) before escalating, so a failed
		// auto-attempt leaves no unaudited orphan rule behind.
		if createdHere {
			r.compensateCreatedRule(ctx, id, ruleID, provEv, classification.Confidence, "dry-run probe failed before clearance")
		}
		return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonProbeFailed, confirmErr))
	}
	// m-3: record the probe outcome (previously dead-code audit builder). This
	// break's OWN probe reconciliation id is stamped on its probe event, so its
	// resolution traces to the dry-run that cleared exactly this break.
	if err := r.auditWriter.Record(ctx, audit.Probed(id, reconID, cleared, provEv)); err != nil {
		return fmt.Errorf("remediator: audit probe for break %q: %w", id, err)
	}
	if !cleared {
		// M-11: Blnk did not confirm THIS break cleared, so its rule did not do
		// its job. Durably compensate a rule created in this run before escalating.
		// Only this break is failed closed — an unmatched sibling never suppresses
		// another break's valid resolution (finding F18 / Rule 5.7).
		if createdHere {
			r.compensateCreatedRule(ctx, id, ruleID, provEv, classification.Confidence, "dry-run probe did not confirm clearance")
		}
		return r.escalate(ctx, txn, classification, prov, reasonNotCleared)
	}

	// Blnk confirmed THIS break cleared — and only Blnk can (Rule 5.3).
	// Build the clearance proof from the confirming reconciliation id:
	// audit.Resolved takes a ClearanceProof (never a bare id), so a resolution is
	// impossible to record without evidence of an actual dry-run clearance. If the
	// arbiter reported cleared but returned no usable reconciliation id, fail
	// closed and escalate rather than record an unprovable resolution (Rule 5.7).
	// LLM confidence alone never resolves.
	proof, err := audit.NewClearanceProof(reconID, cleared)
	if err != nil {
		// M-11: cleared but no usable reconciliation id — fail closed and escalate
		// rather than record an unprovable resolution. Durably compensate a rule
		// created in this run so the aborted attempt leaves no unaudited orphan.
		if createdHere {
			r.compensateCreatedRule(ctx, id, ruleID, provEv, classification.Confidence, "clearance reported without a usable reconciliation id")
		}
		return r.escalate(ctx, txn, classification, prov, fmt.Sprintf("%s: %v", reasonNotCleared, err))
	}

	// C-1 / M-11: mark the break auto-resolved, retire the rule's compensation
	// obligation (confirm the outbox row), and record the resolved AuditEvent —
	// all in ONE transaction. An auto-resolved status can never be durable
	// without its resolved event (nor vice versa); and confirming the outbox in
	// the SAME commit means a rule that legitimately cleared its break is marked
	// 'confirmed' rather than being mistaken for an orphan by the recovery sweep.
	// MarkResolvedTx stamps the SAME confirming per-break recon_id the proof
	// carries onto the break row, and audit.Resolved stamps it onto the event's
	// provenance — so the row's resolved_recon_id and the audit trail agree, and
	// the store's resolved-proof CHECK guarantees the row cannot be auto-resolved
	// without it (Rule 5.3). Every resolution is thus traceable to the probe
	// dry-run that cleared it. ConfirmRuleOutboxTx targets only a still-'pending'
	// row, so on the resume path (where the outbox may already be confirmed) it is
	// a harmless no-op.
	if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if e := r.store.MarkResolvedTx(ctx, tx, id, proof.ReconID()); e != nil {
			return e
		}
		if e := r.store.ConfirmRuleOutboxTx(ctx, tx, id, ruleID); e != nil {
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
	// M-10: fail-closed durable routing must NOT be defeated by a canceled or
	// deadline-exceeded parent context. A frequent escalation trigger is an LLM
	// timeout (Rule 5.7), whose context is already past its deadline; reusing it
	// here would make the very database writes that record the escalation
	// (persist queued + enqueue HITL + escalated audit) fail, silently DROPPING
	// the break instead of routing it. Derive an independent context that keeps
	// the parent's values but drops its cancellation/deadline, bounded by its own
	// short timeout, so the escalation always commits.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), escalateTimeout)
	defer cancel()

	// Sanitize a possibly-malformed confidence so both the persisted break and
	// the escalated audit event are well-formed (the audit validator rejects a
	// non-finite or out-of-range confidence).
	classification.Confidence = safeConfidence(classification.Confidence)

	// Enrich the escalation provenance with the classification evidence (root
	// cause, regulated flag, proposed-rule identity) so the HITL/audit trail is
	// self-describing rather than free-text only.
	provEv := audit.WithClassificationEvidence(prov, classification)

	if err := r.store.WithTx(ctx, func(tx *sql.Tx) error {
		if e := r.store.UpsertBreakTx(ctx, tx, classification, txn, provEv, statusQueued); e != nil {
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

// Blnk matching-criteria field names the remediator uses when building a safe
// root-cause-specific rule profile (finding C-06). They alias the canonical
// grammar vocabulary in internal/model (finding m-01) — the same domain the
// classifier's allowedFields validator enforces via classifier.ValidateRule
// before any POST — so the profile builder and the validator share one source.
const (
	fieldAmount    = model.FieldAmount
	fieldCurrency  = model.FieldCurrency
	fieldReference = model.FieldReference
)

// safeRuleForBreak builds the EXACT, deterministic Blnk matching rule the agent
// is permitted to auto-apply for an auto-eligible break, keyed on its (already
// gated) root cause (finding C-06). It does NOT trust the classifier's advisory
// rule STRUCTURE; it constructs the rule from the root cause and the
// transaction's OWN fields, so the semantic safety of an auto-remediation is a
// deterministic property of the code rather than of weakly-bounded model output.
//
// Each profile pins the intended counterpart's IDENTITY with equality criteria
// (which Blnk matches field-to-field) and omits exactly the field that
// legitimately differs for that root cause. Because Blnk IGNORES criteria.Value
// and matches field-to-field, the equals OPERATOR — not the Value — is what pins
// external == internal on a field; Value is still populated from the transaction
// for auditability and human intent.
//
//   - timing: amount, currency and reference all match the internal booking;
//     only the settlement DATE differs. Profile = {amount==, currency==,
//     reference==}; the date is intentionally omitted so the lag cannot defeat
//     the match. Requires a reference (the strongest available identity).
//   - amount_drift: reference and currency match; only the AMOUNT drifted
//     slightly. Profile = {amount== within a bounded AllowableDrift, currency==,
//     reference==}. The drift is derived from the classifier's advisory proposal
//     and hard-capped by boundedAmountDrift. Requires a reference so a
//     bounded-amount match stays pinned to a stable identity.
//   - reference_mismatch: amount and currency match; only the REFERENCE differs
//     (e.g. a vendor ref vs an invoice number). Profile = {amount==, currency==};
//     the reference is intentionally omitted because it is the differing field.
//     This is the narrowest safe identity the HTTP-only surface allows for this
//     cause.
//
// Every profile requires a currency (to forbid a cross-currency match). A break
// missing a field its profile requires — or any root cause outside the auto path
// (a programming error, since the gate escalates all others) — returns an error
// so the break fails closed to HITL rather than being auto-applied unsafely.
//
// RESIDUAL LIMITATION (documented, bounded by explicit AAP rules — AAP
// §0.1.1/§0.4.1, Rules 5.1/5.8): two internal bookings identical on a profile's
// pinned fields remain indistinguishable field-to-field, because Blnk-core
// matching may not be modified (Rule 5.8) or read around (Rule 5.1). The profiles
// apply the tightest identity the native HTTP surface permits; the remaining
// non-determinism is a property of the off-limits Blnk engine, not the agent, and
// clearance is ALWAYS confirmed by Blnk's own dry-run arbiter (Rule 5.3) before a
// resolution is recorded.
func safeRuleForBreak(c model.BreakClassification, txn blnk.ExternalTransaction) (blnk.MatchingRule, error) {
	if strings.TrimSpace(txn.Currency) == "" {
		return blnk.MatchingRule{}, fmt.Errorf("break %q carries no currency; cannot pin currency identity for a safe %s rule", txn.ID, c.RootCause)
	}
	amountEq := blnk.MatchingCriteria{Field: fieldAmount, Operator: operatorEquals, Value: formatAmount(txn.Amount)}
	currencyEq := blnk.MatchingCriteria{Field: fieldCurrency, Operator: operatorEquals, Value: txn.Currency}
	referenceEq := blnk.MatchingCriteria{Field: fieldReference, Operator: operatorEquals, Value: txn.Reference}

	var criteria []blnk.MatchingCriteria
	switch c.RootCause {
	case model.RootCauseTiming:
		if strings.TrimSpace(txn.Reference) == "" {
			return blnk.MatchingRule{}, fmt.Errorf("timing break %q carries no reference; cannot pin identity for a safe rule", txn.ID)
		}
		criteria = []blnk.MatchingCriteria{amountEq, currencyEq, referenceEq}
	case model.RootCauseAmountDrift:
		if strings.TrimSpace(txn.Reference) == "" {
			return blnk.MatchingRule{}, fmt.Errorf("amount_drift break %q carries no reference; cannot pin identity for a safe rule", txn.ID)
		}
		amountEq.AllowableDrift = boundedAmountDrift(c.ProposedRule)
		criteria = []blnk.MatchingCriteria{amountEq, currencyEq, referenceEq}
	case model.RootCauseReferenceMismatch:
		criteria = []blnk.MatchingCriteria{amountEq, currencyEq}
	default:
		return blnk.MatchingRule{}, fmt.Errorf("root cause %q has no safe auto-remediation rule profile", c.RootCause)
	}

	return blnk.MatchingRule{
		// Deterministic name so an orphaned rule from an interrupted attempt is
		// identifiable and the SEAM-MIN-1 resume guard can reason about it. It
		// mirrors the name the classifier stamps on its advisory proposal.
		Name:        fmt.Sprintf("agent-proposed-%s", txn.ID),
		Description: fmt.Sprintf("agent safe auto-remediation rule for %s break %s", c.RootCause, txn.ID),
		Criteria:    criteria,
	}, nil
}

// boundedAmountDrift extracts a safe amount AllowableDrift from the classifier's
// advisory proposed rule, capped at maxAmountDriftFraction (finding C-06). It
// takes the largest finite, positive AllowableDrift on any amount criterion of
// the proposal and clamps it DOWN to the cap; a nil, non-finite, or non-positive
// proposal yields 0 (exact amount match). Clamping never INCREASES the drift, so
// a model cannot widen the tolerance beyond the hard cap, bounding how large an
// amount discrepancy may auto-clear as "drift".
func boundedAmountDrift(proposed *blnk.MatchingRule) float64 {
	if proposed == nil {
		return 0
	}
	best := 0.0
	for _, c := range proposed.Criteria {
		if strings.ToLower(strings.TrimSpace(c.Field)) != fieldAmount {
			continue
		}
		d := c.AllowableDrift
		if math.IsNaN(d) || math.IsInf(d, 0) || d <= 0 {
			continue
		}
		if d > best {
			best = d
		}
	}
	if best > maxAmountDriftFraction {
		best = maxAmountDriftFraction
	}
	return best
}

// formatAmount renders an amount as the canonical string stamped into a
// criterion's Value for auditability (Blnk itself ignores the Value).
func formatAmount(a float64) string {
	return strconv.FormatFloat(a, 'f', -1, 64)
}

// compensateCreatedRule durably compensates a Blnk matching rule the agent
// created in THIS invocation for an auto-remediation attempt that did not
// resolve the break (finding M-11), replacing the earlier best-effort rollback
// whose delete error was silently ignored. It:
//  1. deletes the rule from Blnk's shared catalog so no orphan accumulates
//     (observed growing 30 -> 88 during QA);
//  2. records the compensation OUTCOME to the append-only audit trail — INCLUDING
//     a delete failure, so an orphan that could not be removed stays durably
//     visible for human cleanup (M-11's "audit cleanup failures" obligation);
//  3. marks the durable outbox obligation compensated / compensation_failed
//     (idempotent; tolerant of a missing row when the create-commit itself
//     failed and no pending row was ever written); and
//  4. on a successful delete, clears the persisted created-rule id so a later
//     HITL re_drive does not reuse a now-deleted rule.
//
// It never returns an error: the break MUST still route to HITL (Rule 5.7
// fail-closed) even when catalog cleanup cannot complete, and any un-removed
// orphan is both audited and left as a 'compensation_failed' outbox row for the
// startup sweep to retry. It runs on an independent fail-closed context (finding
// M-10) so a canceled parent cannot skip compensation. Only a rule created in
// THIS call is ever passed here (createdHere gate).
func (r *Remediator) compensateCreatedRule(ctx context.Context, externalTxnID, ruleID string, prov model.Provenance, confidence float64, why string) {
	if ruleID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensateTimeout)
	defer cancel()

	delErr := r.blnkClient.DeleteMatchingRule(ctx, ruleID)
	ok := delErr == nil
	errMsg := ""
	rationale := fmt.Sprintf("compensation (%s): deleted non-clearing rule %s from Blnk", why, ruleID)
	if delErr != nil {
		errMsg = delErr.Error()
		rationale = fmt.Sprintf("compensation (%s): FAILED to delete rule %s from Blnk (%v); it may still exist and needs manual cleanup", why, ruleID, delErr)
	}

	// M-11: durable failure evidence — record the outcome in the append-only
	// audit trail regardless of success.
	_ = r.auditWriter.Record(ctx, audit.RuleCompensated(externalTxnID, ruleID, ok, rationale, confidence, prov))
	// Retire (or fail) the durable outbox obligation for this rule.
	_ = r.store.CompensateRuleOutbox(ctx, externalTxnID, ruleID, ok, errMsg)

	if ok {
		// Deletion succeeded: forget the rule so a re_drive cannot reuse it.
		_ = r.store.SetCreatedRule(ctx, externalTxnID, "")
	}
}

// RecoverPendingRules is the startup compensation sweep for the durable rule
// outbox (finding M-11). A 'pending' outbox row is a Blnk matching rule the agent
// created whose owning attempt did not durably CONFIRM (break resolved) or
// COMPENSATE (rule deleted) — the signature of a crash mid-attempt. For each
// pending row it consults the break's CURRENT persisted status and reconciles:
//
//   - break auto-resolved -> the rule legitimately cleared the break, so it is
//     NOT an orphan; CONFIRM the obligation and leave the rule in Blnk.
//   - otherwise            -> the attempt never resolved; COMPENSATE: delete the
//     orphaned rule from Blnk, mark the obligation, and audit the outcome
//     (including a delete failure, so an un-removable orphan stays visible).
//
// It runs on an independent fail-closed context so a canceled parent cannot
// interrupt recovery. It returns the number of pending rows it processed and a
// non-nil error only when the pending set itself could not be listed; a per-row
// failure is recorded (marked compensation_failed + audited) and never aborts
// recovery of the remaining rows.
func (r *Remediator) RecoverPendingRules(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensateTimeout)
	defer cancel()

	pending, err := r.store.ListPendingRuleOutbox(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("remediator: list pending rule outbox: %w", err)
	}

	for _, item := range pending {
		// Was the created rule the one that legitimately cleared its break? If so
		// it is NOT an orphan and must not be deleted.
		_, status, _, found, lerr := r.store.LoadBreak(ctx, item.ExternalTxnID)
		if lerr != nil {
			// Could not read the break's status; leave the row pending for a later
			// sweep rather than risk deleting a legitimately-resolving rule.
			continue
		}
		if found && status == statusAutoResolved {
			// The rule did its job; confirm the obligation (idempotent) and keep
			// the rule. Wrap the tx-bound confirm in a trivial transaction.
			_ = r.store.WithTx(ctx, func(tx *sql.Tx) error {
				return r.store.ConfirmRuleOutboxTx(ctx, tx, item.ExternalTxnID, item.RuleID)
			})
			continue
		}

		// Orphan: the attempt never resolved. Delete the rule from Blnk and record
		// the outcome durably in BOTH the outbox and the append-only audit trail.
		delErr := r.blnkClient.DeleteMatchingRule(ctx, item.RuleID)
		ok := delErr == nil
		errMsg := ""
		rationale := fmt.Sprintf("startup recovery sweep: deleted orphaned rule %s from Blnk", item.RuleID)
		if delErr != nil {
			errMsg = delErr.Error()
			rationale = fmt.Sprintf("startup recovery sweep: FAILED to delete orphaned rule %s from Blnk (%v); it may still exist and needs manual cleanup", item.RuleID, delErr)
		}
		_ = r.auditWriter.Record(ctx, audit.RuleCompensated(item.ExternalTxnID, item.RuleID, ok, rationale, 0, model.Provenance{Model: r.llmModel}))
		_ = r.store.MarkRuleOutboxCompensated(ctx, item.ID, ok, errMsg)
		if ok {
			// Forget the rule so a later HITL re_drive cannot reuse a deleted one.
			_ = r.store.SetCreatedRule(ctx, item.ExternalTxnID, "")
		}
	}
	return len(pending), nil
}

// autoApplySafe reports whether a rule is tight enough to be auto-applied to a
// single-transaction Blnk dry-run (finding C-06). Blnk matches the external
// transaction against internal transactions field-to-field and ignores
// criteria.Value, so a non-equality operator (greater_than, less_than, contains)
// can match an unintended internal counterpart and make the probe falsely report
// the break cleared. Only equality criteria pin external == internal on a field,
// so every criterion must use the equals operator; any other operator returns an
// error and the break is escalated to HITL instead of auto-applied.
//
// Since safeRuleForBreak now BUILDS the auto-applied rule deterministically from
// a per-root-cause profile (which uses only equality criteria), this is a
// defensive post-build assertion that the constructed profile conforms — a
// belt-and-braces gate immediately before the POST, not the primary safety
// mechanism. The rule's grammar (non-empty criteria, allowed fields/operators) is
// validated separately by classifier.ValidateRule before this check.
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
// fails the break closed to HITL (finding M-09).
//
// The classifier REJECTS a malformed model confidence at parse time (a
// non-finite or out-of-range value makes classification fail, which itself
// fails the break closed per Rule 5.7) — it does NOT silently clamp one into
// range. This check is therefore the remediator's independent, defense-in-depth
// guard for confidences that did NOT originate from a fresh classifier parse:
// values reconstructed from the store on a crash-resume, injected by tests, or
// produced by any future alternative source. If such a value reaches the gate
// malformed, failing closed here prevents a NaN comparison from silently
// defeating the confidence gate (Rule 5.4); a well-formed value passes through
// untouched and is then decided by Blnk's deterministic arbiter (Rule 5.3).
func validConfidence(c float64) bool {
	return !math.IsNaN(c) && !math.IsInf(c, 0) && c >= 0 && c <= 1
}

// safeConfidence coerces a possibly-malformed confidence to a value the audit
// validator accepts (finite, in [0,1]); a malformed input collapses to 0 so an
// escalated event can still be recorded for the break. It exists only for the
// escalation path, where a break carrying a store-reconstructed or test-injected
// out-of-range confidence must still be routed to HITL with a well-formed audit
// event rather than being dropped.
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
