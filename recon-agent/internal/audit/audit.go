// Package audit is the recon-agent's append-only audit writer: the single path
// through which every pipeline action (classify, propose/create a rule, probe,
// resolve, escalate, and each HITL decision) is recorded to the immutable
// agent.agent_audit ledger. This realizes the measurable success criterion that
// 100% of actions emit an AuditEvent.
//
// Design: the Writer does not talk to PostgreSQL directly. Its persistence
// dependency is the sink interface, whose only method appends one event via an
// INSERT into agent.agent_audit. In production the sink is *store.Store (the
// module's single DB gateway), so all SQL lives in one place and this package
// introduces no second write path. Because the interface offers no update or
// delete method, the writer is append-only by construction (Rule 5.5): there is
// no code path here that could mutate or remove a recorded event.
//
// Atomicity (finding C-06): a Writer also accepts a transaction-bound sink so an
// audit event can be committed in the SAME *sql.Tx as the state change it
// records. When the sink additionally implements TxSink, RecordTx enrolls the
// append in the caller's transaction; the state mutation and its audit event
// then commit or roll back together, guaranteeing action-to-audit parity.
//
// Integrity (findings C-07, M-14, Rule 5.3): every event is validated before it
// is written (required fields, closed action set, finite confidence in [0,1]),
// its timestamp is always stamped from the trusted server clock at RFC3339Nano
// precision (a caller-supplied timestamp is never trusted), and a "resolved"
// event can only be produced from a ClearanceProof — evidence that a Blnk
// dry-run reconciliation actually cleared the break — so LLM confidence alone
// can never mark a resolution.
//
// Rule 5.1 (native-API-only): this package imports only the standard library,
// github.com/google/uuid, and the recon-agent module's own internal/model. It
// never imports any Blnk internal package.
package audit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blnkfinance/recon-agent/internal/model"
)

// ActorAgent is the actor recorded for actions the agent performs on its own
// (classification, rule proposal/creation, probing, auto-remediation,
// escalation). Human decisions record the reviewer's id as the actor instead.
const ActorAgent = "agent"

// Action constants are the closed set of values written to agent_audit.action.
// Any event whose Action is outside this set is rejected by Record/RecordTx
// (finding M-13: no arbitrary action may reach the ledger). Each name aliases
// the canonical literal in internal/model (finding m-01), so the value is
// declared exactly once module-wide while this package's public audit.Action*
// API — referenced by the remediator, hitl, cmd, and their tests — stays
// stable.
const (
	ActionClassified   = model.ActionClassified
	ActionRuleProposed = model.ActionRuleProposed
	ActionRuleCreated  = model.ActionRuleCreated
	ActionProbed       = model.ActionProbed
	ActionResolved     = model.ActionResolved
	ActionEscalated    = model.ActionEscalated
	ActionAccepted     = model.ActionAccepted
	ActionReDriven     = model.ActionReDriven
	ActionRejected     = model.ActionRejected
	// ActionRuleCompensated records the OUTCOME of compensating a Blnk matching
	// rule the agent created but whose auto-remediation attempt did not resolve
	// the break — the rule is deleted from Blnk's shared catalog so no orphan
	// survives (finding M-11). It is emitted by the remediator's in-call
	// rollback and by its startup recovery sweep, and — crucially — is recorded
	// even when the deletion FAILED, so an orphaned rule that could not be
	// removed is durably visible in the append-only trail for human cleanup.
	ActionRuleCompensated = model.ActionRuleCompensated
)

// Decision verbs, as submitted on model.HITLDecision.Decision. DecisionAction
// maps each to its past-tense Action constant.
const (
	DecisionAccept  = "accept"
	DecisionReDrive = "re_drive"
	DecisionReject  = "reject"
)

// Sentinel errors returned by the constructor, the recording methods, and the
// verified-probe constructor. Callers can match them with errors.Is.
var (
	// ErrNilSink is returned by New when the sink is nil (finding C-07): a
	// Writer with no persistence dependency could silently drop events.
	ErrNilSink = errors.New("audit: sink must not be nil")
	// ErrNilTx is returned by RecordTx when the caller passes a nil transaction.
	ErrNilTx = errors.New("audit: transaction must not be nil")
	// ErrTxSinkUnsupported is returned by RecordTx when the configured sink does
	// not implement TxSink and therefore cannot enroll the append in a caller
	// transaction.
	ErrTxSinkUnsupported = errors.New("audit: sink does not support transaction-bound writes")
	// ErrEmptyExternalTxnID is returned when an event has no external txn id.
	ErrEmptyExternalTxnID = errors.New("audit: external_txn_id must not be empty")
	// ErrEmptyActor is returned when an event has no actor (finding M-13: an
	// empty/arbitrary decision must not pass through as a nameless actor).
	ErrEmptyActor = errors.New("audit: actor must not be empty")
	// ErrInvalidAction is returned when an event's action is outside the closed
	// set (finding M-13).
	ErrInvalidAction = errors.New("audit: action is not in the allowed set")
	// ErrInvalidConfidence is returned when confidence is NaN, infinite, or
	// outside [0,1] (finding C-07).
	ErrInvalidConfidence = errors.New("audit: confidence must be a finite number in [0,1]")
	// ErrEmptyRationale is returned when an event carries no rationale (finding
	// M-12). Every action must record WHY it happened so the append-only trail
	// is self-describing; this mirrors the agent_audit_rationale_nonempty DB
	// CHECK, failing fast at the application boundary with a clear error. The
	// evidence-stamping builders synthesize a sensible default rationale when a
	// caller supplies none, so a well-formed event never trips this.
	ErrEmptyRationale = errors.New("audit: rationale must not be empty")
	// ErrMissingProvenance is returned when an event lacks the action-specific
	// evidence its action requires (finding M-12): a classification must name the
	// model, a proposed rule must carry its field, a created rule its id, and a
	// probe its reconciliation id. Without this, an action would be recorded
	// without the machine-readable proof that makes it independently meaningful.
	ErrMissingProvenance = errors.New("audit: action is missing required provenance evidence")
	// ErrInvalidEventID is returned when a caller-supplied event id is not a
	// valid UUID.
	ErrInvalidEventID = errors.New("audit: event_id must be a valid UUID")
	// ErrEmptyReconID is returned when a resolved event has no confirming
	// reconciliation id, or when NewClearanceProof is given an empty id
	// (Rule 5.3: a resolution must be provably traceable to a Blnk dry-run).
	ErrEmptyReconID = errors.New("audit: a resolution requires a non-empty confirming reconciliation id")
	// ErrNotCleared is returned by NewClearanceProof when the arbiter did not
	// report the break cleared (Rule 5.3: LLM confidence alone cannot resolve).
	ErrNotCleared = errors.New("audit: a resolution requires a confirmed (cleared) Blnk dry-run")
)

// sink is the append-only persistence dependency of the Writer. It is
// deliberately minimal — a single INSERT method — so the audit package has no
// way to update or delete a recorded event (Rule 5.5). *store.Store satisfies
// this interface via its InsertAudit method, so New is wired as
// audit.New(store).
type sink interface {
	InsertAudit(ctx context.Context, e model.AuditEvent) error
}

// TxSink is the optional transaction-bound extension of sink (finding C-06).
// A sink that also implements TxSink lets the Writer append an audit event
// inside a caller-supplied *sql.Tx, so the event commits atomically with the
// state change it records. Like sink, TxSink exposes only an INSERT and no
// update/delete, preserving append-only writes (Rule 5.5). In production
// *store.Store's transaction-bound insert satisfies this interface.
type TxSink interface {
	InsertAuditTx(ctx context.Context, tx *sql.Tx, e model.AuditEvent) error
}

// Writer records audit events to the append-only ledger. A single Writer is
// safe to share across the pipeline (it holds no mutable state of its own).
type Writer struct {
	sink   sink
	txSink TxSink // non-nil when sink also implements TxSink (enables RecordTx)
}

// New returns a Writer backed by the given sink, or ErrNilSink if the sink is
// nil (finding C-07). When the sink also implements TxSink, the returned Writer
// supports transaction-bound recording via RecordTx (finding C-06); otherwise
// RecordTx returns ErrTxSinkUnsupported. In production pass the module's
// *store.Store, whose InsertAudit satisfies the sink interface.
func New(s sink) (*Writer, error) {
	if s == nil {
		return nil, ErrNilSink
	}
	w := &Writer{sink: s}
	if ts, ok := s.(TxSink); ok {
		w.txSink = ts
	}
	return w, nil
}

// Record validates and appends a single event to the audit ledger via the
// sink's InsertAudit (an INSERT). Validation (required fields, closed action
// set, finite confidence, proven resolution) and timestamp/id normalization are
// performed by prepare; on any validation error nothing is written (fail-safe).
// Record never updates or deletes: it is an append-only choke point through
// which pipeline actions are recorded (Rule 5.5).
func (w *Writer) Record(ctx context.Context, ev model.AuditEvent) error {
	prepared, err := w.prepare(ev)
	if err != nil {
		return err
	}
	return w.sink.InsertAudit(ctx, prepared)
}

// RecordTx validates and appends a single event inside the caller-supplied
// transaction so it commits atomically with the state change it records
// (finding C-06). It requires a non-nil tx and a sink that implements TxSink;
// otherwise it returns ErrNilTx or ErrTxSinkUnsupported and writes nothing. Like
// Record it only ever performs an INSERT (Rule 5.5).
func (w *Writer) RecordTx(ctx context.Context, tx *sql.Tx, ev model.AuditEvent) error {
	if tx == nil {
		return ErrNilTx
	}
	if w.txSink == nil {
		return ErrTxSinkUnsupported
	}
	prepared, err := w.prepare(ev)
	if err != nil {
		return err
	}
	return w.txSink.InsertAuditTx(ctx, tx, prepared)
}

// prepare validates the event and normalizes its identity and chronology. It
// assigns a fresh UUID when EventID is empty (and rejects a malformed
// caller-supplied id), and ALWAYS stamps Timestamp from the trusted server
// clock at RFC3339Nano precision, overriding any caller-supplied value so
// arbitrary chronology can never be injected into the immutable ledger
// (finding M-14). It returns the normalized event, or an error (in which case
// the caller writes nothing).
func (w *Writer) prepare(ev model.AuditEvent) (model.AuditEvent, error) {
	if err := validateEvent(ev); err != nil {
		return model.AuditEvent{}, err
	}
	if ev.EventID == "" {
		ev.EventID = uuid.NewString()
	} else if _, err := uuid.Parse(ev.EventID); err != nil {
		return model.AuditEvent{}, fmt.Errorf("%w: %v", ErrInvalidEventID, err)
	}
	// M-14: the record time is authoritative — never the caller's clock.
	ev.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	return ev, nil
}

// validateEvent enforces the required-field and domain invariants every audit
// event must satisfy before it is written (findings C-07, M-12, M-13, Rule 5.3).
func validateEvent(ev model.AuditEvent) error {
	if strings.TrimSpace(ev.ExternalTxnID) == "" {
		return ErrEmptyExternalTxnID
	}
	if strings.TrimSpace(ev.Actor) == "" {
		return ErrEmptyActor
	}
	if !isAllowedAction(ev.Action) {
		return fmt.Errorf("%w: %q", ErrInvalidAction, ev.Action)
	}
	if math.IsNaN(ev.Confidence) || math.IsInf(ev.Confidence, 0) || ev.Confidence < 0 || ev.Confidence > 1 {
		return ErrInvalidConfidence
	}
	// M-12: every event must record a non-empty rationale so the append-only
	// trail explains WHY each action happened, not just that it did. The
	// builders synthesize a default when a caller supplies none, so this only
	// rejects a genuinely empty direct Record call.
	if strings.TrimSpace(ev.Rationale) == "" {
		return ErrEmptyRationale
	}
	// M-12: action-specific provenance — each action must carry the
	// machine-readable evidence that makes it independently meaningful and
	// queryable in the trail.
	return validateActionProvenance(ev)
}

// validateActionProvenance enforces the per-action evidence each audit action
// must carry in its provenance (finding M-12). The required evidence is exactly
// what every legitimate emission site of that action populates, so this rejects
// under-attributed events without ever rejecting a well-formed one:
//
//   - classified   → the model that produced the classification;
//   - rule_proposed → the proposed rule's primary criterion field (a proposed
//     rule has no Blnk id yet, so the field — not the id — is its evidence);
//   - rule_created → the Blnk-assigned rule id;
//   - rule_compensated → the Blnk rule id that was compensated (deleted, or
//     attempted): the compensation event is only meaningful if it names WHICH
//     orphaned rule it concerns, so a failed cleanup is traceable to the exact
//     rule still needing manual removal (finding M-11);
//   - probed       → the dry-run reconciliation id the probe ran under;
//   - resolved     → the confirming reconciliation id (Rule 5.3): a resolution
//     must be provably traceable to the Blnk dry-run that cleared it. Kept as a
//     distinct ErrEmptyReconID sentinel so the deterministic-arbiter proof has
//     its own matchable error.
//
// escalated, accepted, re_driven, and rejected intentionally require no extra
// provenance: a probe-error escalation precedes any LLM call so carries no
// model (cmd/main.go), and a "not cleared" re_driven event deliberately carries
// no recon_id so a recon_id can never imply a false clearance (finding m-04).
// For those, the required non-empty actor and rationale are sufficient evidence.
func validateActionProvenance(ev model.AuditEvent) error {
	switch ev.Action {
	case ActionClassified:
		if strings.TrimSpace(ev.Provenance.Model) == "" {
			return fmt.Errorf("%w: %q requires provenance.model", ErrMissingProvenance, ev.Action)
		}
	case ActionRuleProposed:
		if strings.TrimSpace(ev.Provenance.RuleField) == "" {
			return fmt.Errorf("%w: %q requires provenance.rule_field", ErrMissingProvenance, ev.Action)
		}
	case ActionRuleCreated:
		if strings.TrimSpace(ev.Provenance.RuleID) == "" {
			return fmt.Errorf("%w: %q requires provenance.rule_id", ErrMissingProvenance, ev.Action)
		}
	case ActionRuleCompensated:
		if strings.TrimSpace(ev.Provenance.RuleID) == "" {
			return fmt.Errorf("%w: %q requires provenance.rule_id", ErrMissingProvenance, ev.Action)
		}
	case ActionProbed:
		if strings.TrimSpace(ev.Provenance.ReconID) == "" {
			return fmt.Errorf("%w: %q requires provenance.recon_id", ErrMissingProvenance, ev.Action)
		}
	case ActionResolved:
		if strings.TrimSpace(ev.Provenance.ReconID) == "" {
			return ErrEmptyReconID
		}
	}
	return nil
}

// isAllowedAction reports whether action is a member of the closed action set.
// A switch (rather than a mutable package-level map) keeps the set immutable
// and the check allocation-free.
func isAllowedAction(action string) bool {
	switch action {
	case ActionClassified, ActionRuleProposed, ActionRuleCreated, ActionProbed,
		ActionResolved, ActionEscalated, ActionAccepted, ActionReDriven, ActionRejected,
		ActionRuleCompensated:
		return true
	default:
		return false
	}
}

// ClearanceProof is verified evidence that a Blnk dry-run reconciliation
// confirmed a break cleared (Rule 5.3 — Blnk is the deterministic arbiter). It
// carries the confirming reconciliation id and can ONLY be constructed via
// NewClearanceProof, which requires an explicit cleared==true result and a
// non-empty reconciliation id. Because Resolved requires a ClearanceProof, no
// code path can mark a break resolved on LLM confidence alone.
type ClearanceProof struct {
	reconID string
}

// NewClearanceProof constructs proof that a Blnk dry-run cleared a break. It
// returns ErrNotCleared unless the arbiter reported cleared==true, and
// ErrEmptyReconID unless a non-empty reconciliation id is supplied, so an
// unproven or fabricated resolution cannot be recorded (finding C-07, Rule 5.3).
func NewClearanceProof(reconID string, cleared bool) (ClearanceProof, error) {
	if !cleared {
		return ClearanceProof{}, ErrNotCleared
	}
	reconID = strings.TrimSpace(reconID)
	if reconID == "" {
		return ClearanceProof{}, ErrEmptyReconID
	}
	return ClearanceProof{reconID: reconID}, nil
}

// ReconID returns the confirming reconciliation id captured by the proof.
func (p ClearanceProof) ReconID() string { return p.reconID }

// RuleRef is the minimal identity of a Blnk matching rule as recorded in the
// audit trail: the rule id plus the field and operator of its primary
// criterion. Callers build it from a blnk.MatchingRule at the call site so the
// audit package stays free of any Blnk coupling (Rule 5.1).
type RuleRef struct {
	ID       string
	Field    string
	Operator string
}

// WithClassificationEvidence returns a copy of prov enriched with the
// machine-readable classification evidence (root cause, regulated flag, and the
// proposed rule's identity when present) drawn from c (finding M-13). The input
// prov is not modified. Use it to enrich the provenance of any event derived
// from a classification (classified, escalated, resolved) so the append-only
// trail is fully queryable.
func WithClassificationEvidence(prov model.Provenance, c model.BreakClassification) model.Provenance {
	prov.RootCause = string(c.RootCause)
	prov.Regulated = c.Regulated
	if c.ProposedRule != nil {
		prov.RuleID = c.ProposedRule.RuleID
		if len(c.ProposedRule.Criteria) > 0 {
			prov.RuleField = c.ProposedRule.Criteria[0].Field
			prov.RuleOperator = c.ProposedRule.Criteria[0].Operator
		}
	}
	return prov
}

// withRuleEvidence returns a copy of prov stamped with the identity of a
// specific rule.
func withRuleEvidence(prov model.Provenance, rule RuleRef) model.Provenance {
	prov.RuleID = rule.ID
	prov.RuleField = rule.Field
	prov.RuleOperator = rule.Operator
	return prov
}

// Classified builds the audit event for a completed classification. The actor
// is the agent; the event carries the classification's confidence and rationale
// plus the given provenance enriched with machine-readable classification
// evidence (root cause, regulated flag, proposed-rule identity — finding M-13).
func Classified(c model.BreakClassification, prov model.Provenance) model.AuditEvent {
	rationale := c.Rationale
	if strings.TrimSpace(rationale) == "" {
		// M-12: synthesize a non-empty rationale from the classification's own
		// evidence when the classifier returned none, so the classified event is
		// still self-describing rather than being rejected as under-attributed.
		rootCause := string(c.RootCause)
		if rootCause == "" {
			rootCause = string(model.RootCauseUnknown)
		}
		rationale = "agent classified break; root cause: " + rootCause
	}
	return model.AuditEvent{
		ExternalTxnID: c.ExternalTxnID,
		Actor:         ActorAgent,
		Action:        ActionClassified,
		Rationale:     rationale,
		Confidence:    c.Confidence,
		Provenance:    WithClassificationEvidence(prov, c),
	}
}

// RuleProposed builds the audit event recording that the agent proposed a
// Blnk-native matching rule for a break, before any POST to Blnk (finding M-13:
// rule actions are audited). The rule's identity is stamped into provenance.
func RuleProposed(externalTxnID string, rule RuleRef, confidence float64, prov model.Provenance) model.AuditEvent {
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionRuleProposed,
		Rationale:     "agent proposed a Blnk matching rule",
		Confidence:    confidence,
		Provenance:    withRuleEvidence(prov, rule),
	}
}

// RuleCreated builds the audit event recording that a proposed rule was created
// in Blnk (finding M-13). The created rule's identity is stamped into
// provenance so the trail links the action to the concrete Blnk rule id.
func RuleCreated(externalTxnID string, rule RuleRef, confidence float64, prov model.Provenance) model.AuditEvent {
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionRuleCreated,
		Rationale:     "agent created the matching rule in Blnk",
		Confidence:    confidence,
		Provenance:    withRuleEvidence(prov, rule),
	}
}

// RuleCompensated builds the audit event recording the OUTCOME of compensating a
// Blnk matching rule the agent created for an auto-remediation attempt that did
// not resolve the break (finding M-11). deleted reports whether the rule was
// successfully removed from Blnk's shared catalog; the compensated rule's id is
// stamped into provenance so the trail names the exact rule. When deleted is
// false the event is a DURABLE record that an orphaned rule could NOT be removed
// and still exists in Blnk, needing human cleanup — the "audit cleanup failures"
// obligation of M-11. A caller-supplied rationale is preserved; an empty one is
// synthesized from the outcome so the event is always self-describing.
func RuleCompensated(externalTxnID, ruleID string, deleted bool, rationale string, confidence float64, prov model.Provenance) model.AuditEvent {
	if strings.TrimSpace(rationale) == "" {
		if deleted {
			rationale = "agent compensated a non-clearing matching rule: deleted rule " + ruleID + " from Blnk"
		} else {
			rationale = "agent FAILED to compensate matching rule " + ruleID + "; it may still exist in Blnk and needs manual cleanup"
		}
	}
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionRuleCompensated,
		Rationale:     rationale,
		Confidence:    confidence,
		Provenance:    withRuleEvidence(prov, RuleRef{ID: ruleID}),
	}
}

// Probed builds the audit event recording a Blnk dry-run probe of a break
// (finding M-13: probe actions are audited). The confirming reconciliation id
// is stamped into provenance and the outcome (cleared or still unmatched) is
// captured in the rationale. A probe is informational and does not itself mark
// a resolution — only Resolved does that, and only from a ClearanceProof.
func Probed(externalTxnID, reconID string, cleared bool, prov model.Provenance) model.AuditEvent {
	prov.ReconID = reconID
	rationale := "Blnk dry-run probe: break still unmatched"
	if cleared {
		rationale = "Blnk dry-run probe: break cleared"
	}
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionProbed,
		Rationale:     rationale,
		Provenance:    prov,
	}
}

// Resolved builds the audit event for a break that a Blnk dry-run reconciliation
// confirmed as cleared. It takes a ClearanceProof rather than a bare id, so a
// resolved event is impossible to build without evidence of an actual dry-run
// clearance (finding C-07, Rule 5.3). The proof's confirming reconciliation id
// is stamped into the event's provenance (overriding any prior ReconID), making
// every resolved event provably traceable to the dry-run that cleared it.
func Resolved(externalTxnID, rationale string, confidence float64, prov model.Provenance, proof ClearanceProof) model.AuditEvent {
	prov.ReconID = proof.reconID
	if strings.TrimSpace(rationale) == "" {
		// M-12: a resolution is always self-describing — default to naming the
		// confirming dry-run so the event explains WHY the break is resolved.
		rationale = "Blnk dry-run reconciliation " + proof.reconID + " confirmed the break cleared"
	}
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionResolved,
		Rationale:     rationale,
		Confidence:    confidence,
		Provenance:    prov,
	}
}

// Escalated builds the audit event for a break routed to the HITL queue (low
// confidence, regulated, or a fail-closed LLM error). The actor is the agent.
// Enrich prov via WithClassificationEvidence before calling to record the root
// cause and regulated flag that drove the escalation (finding M-13).
func Escalated(externalTxnID, rationale string, confidence float64, prov model.Provenance) model.AuditEvent {
	if strings.TrimSpace(rationale) == "" {
		// M-12: never record a bare escalation — always explain that the break
		// was routed to human review even when the caller passed no reason.
		rationale = "break routed to human review (HITL)"
	}
	return model.AuditEvent{
		ExternalTxnID: externalTxnID,
		Actor:         ActorAgent,
		Action:        ActionEscalated,
		Rationale:     rationale,
		Confidence:    confidence,
		Provenance:    prov,
	}
}

// Decision builds the audit event for a human reviewer's HITL decision. The
// actor is the reviewer; the decision verb (accept / re_drive / reject) is
// mapped to its past-tense action (accepted / re_driven / rejected) by
// DecisionAction, and the reviewer's note becomes the rationale. An unrecognized
// verb maps to an empty action, which Record/RecordTx reject with
// ErrInvalidAction, so an arbitrary decision can never be recorded (finding
// M-13). Callers should gate on IsValidDecision first for a clear early error.
func Decision(d model.HITLDecision, prov model.Provenance) model.AuditEvent {
	rationale := d.Note
	if strings.TrimSpace(rationale) == "" {
		// M-12: a human decision always records WHAT was decided even when the
		// reviewer left no note, so the trail is never a bare, noteless verb.
		rationale = "human reviewer decision: " + d.Decision
	}
	return model.AuditEvent{
		ExternalTxnID: d.ExternalTxnID,
		Actor:         d.Reviewer,
		Action:        DecisionAction(d.Decision),
		Rationale:     rationale,
		Provenance:    prov,
	}
}

// DecisionAction maps a HITL decision verb to its audit action:
// accept -> accepted, re_drive -> re_driven, reject -> rejected. An
// unrecognized verb maps to "" (not the raw verb) so that it is rejected by
// event validation rather than silently recorded (finding M-13: arbitrary
// decisions must not pass through).
func DecisionAction(decision string) string {
	switch decision {
	case DecisionAccept:
		return ActionAccepted
	case DecisionReDrive:
		return ActionReDriven
	case DecisionReject:
		return ActionRejected
	default:
		return ""
	}
}

// IsValidDecision reports whether decision is one of the closed HITL verbs
// (accept / re_drive / reject). HITL handlers should reject other values before
// building an event.
func IsValidDecision(decision string) bool {
	return DecisionAction(decision) != ""
}
