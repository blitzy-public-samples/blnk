// Package audit is the recon-agent's append-only audit writer: the single path
// through which every pipeline action (classify, resolve, escalate, and each
// HITL decision) is recorded to the immutable agent.agent_audit ledger. This
// realizes the measurable success criterion that 100% of actions emit an
// AuditEvent.
//
// Design: the Writer does not talk to PostgreSQL directly. Its sole persistence
// dependency is the sink interface, which exposes exactly one method,
// InsertAudit — an INSERT into agent.agent_audit. In production the sink is
// *store.Store (the module's single DB gateway), so all SQL lives in one place
// and this package introduces no second write path. Because the interface
// offers no update or delete method, the writer is append-only by construction
// (Rule 5.5): there is simply no code path here that could mutate or remove a
// recorded event.
//
// Rule 5.1 (native-API-only): this package imports only the standard library,
// github.com/google/uuid, and the recon-agent module's own internal/model. It
// never imports any Blnk internal package.
package audit

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/blnkfinance/recon-agent/internal/model"
)

// ActorAgent is the actor recorded for actions the agent performs on its own
// (classification, auto-remediation, escalation). Human decisions record the
// reviewer's id as the actor instead.
const ActorAgent = "agent"

// Action constants are the closed set of values written to agent_audit.action;
// they match the actions documented on model.AuditEvent.
const (
	ActionClassified = "classified"
	ActionResolved   = "resolved"
	ActionEscalated  = "escalated"
	ActionAccepted   = "accepted"
	ActionReDriven   = "re_driven"
	ActionRejected   = "rejected"
)

// Decision verbs, as submitted on model.HITLDecision.Decision. DecisionAction
// maps each to its past-tense Action constant.
const (
	DecisionAccept  = "accept"
	DecisionReDrive = "re_drive"
	DecisionReject  = "reject"
)

// sink is the append-only persistence dependency of the Writer. It is
// deliberately minimal — a single INSERT method — so the audit package has no
// way to update or delete a recorded event (Rule 5.5). *store.Store satisfies
// this interface via its InsertAudit method, so New is wired as
// audit.New(store).
type sink interface {
	InsertAudit(ctx context.Context, e model.AuditEvent) error
}

// Writer records audit events to the append-only ledger. A single Writer is
// safe to share across the pipeline (it holds no mutable state of its own).
type Writer struct {
	sink sink
}

// New returns a Writer backed by the given sink. In production pass the
// module's *store.Store, whose InsertAudit satisfies the sink interface.
func New(s sink) *Writer {
	return &Writer{sink: s}
}

// Record appends a single event to the audit ledger. It fills EventID with a
// fresh UUID and Timestamp with the current time in UTC RFC3339 when those
// fields are empty (preserving any caller-supplied values), then performs the
// append via the sink's InsertAudit — an INSERT. Record never updates or
// deletes: it is the single append-only choke point through which 100% of
// pipeline actions are recorded (Rule 5.5).
func (w *Writer) Record(ctx context.Context, ev model.AuditEvent) error {
	if ev.EventID == "" {
		ev.EventID = uuid.NewString()
	}
	if ev.Timestamp == "" {
		ev.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	return w.sink.InsertAudit(ctx, ev)
}

// Classified builds the audit event for a completed classification. The actor
// is the agent; the event carries the classification's confidence and rationale
// plus the given provenance (model / upload / source).
func Classified(c model.BreakClassification, prov model.Provenance) model.AuditEvent {
	return model.AuditEvent{
		ExternalTxnID: c.ExternalTxnID,
		Actor:         ActorAgent,
		Action:        ActionClassified,
		Rationale:     c.Rationale,
		Confidence:    c.Confidence,
		Provenance:    prov,
	}
}

// Resolved builds the audit event for a break that a Blnk dry-run reconciliation
// confirmed as cleared. Per Rule 5.3 the confirming reconciliation id is stamped
// into the event's provenance (overriding any prior ReconID), so every resolved
// event is provably traceable to the dry-run that demonstrated clearance.
func Resolved(externalTxnID, reconID, rationale string, confidence float64, prov model.Provenance) model.AuditEvent {
	prov.ReconID = reconID
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
func Escalated(externalTxnID, rationale string, confidence float64, prov model.Provenance) model.AuditEvent {
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
// DecisionAction, and the reviewer's note becomes the rationale.
func Decision(d model.HITLDecision, prov model.Provenance) model.AuditEvent {
	return model.AuditEvent{
		ExternalTxnID: d.ExternalTxnID,
		Actor:         d.Reviewer,
		Action:        DecisionAction(d.Decision),
		Rationale:     d.Note,
		Provenance:    prov,
	}
}

// DecisionAction maps a HITL decision verb to its audit action:
// accept -> accepted, re_drive -> re_driven, reject -> rejected. An
// unrecognized verb is returned unchanged so no information is silently lost.
func DecisionAction(decision string) string {
	switch decision {
	case DecisionAccept:
		return ActionAccepted
	case DecisionReDrive:
		return ActionReDriven
	case DecisionReject:
		return ActionRejected
	default:
		return decision
	}
}
