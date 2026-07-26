// Package model — agent-domain vocabulary (finding m-01).
//
// This file is the SINGLE source of truth for the agent's own lifecycle and
// grammar strings that previously lived, duplicated, across the remediator,
// hitl, audit, and cmd packages. Centralizing them here — alongside the
// RootCause enumeration already declared in contracts.go — guarantees the wire
// values that flow through agent_break.status, agent_audit.action, and the
// Rule 5.2 matching-rule grammar can never silently diverge between the
// component that writes a value and the component that reads it back.
//
// Placement rationale (import-cycle safety): this package imports ONLY
// internal/blnk (see contracts.go), and nothing in the module imports it back
// except its downstream consumers (audit, remediator, hitl, cmd, classifier),
// so hosting these constants here introduces no cycle. Values that describe
// Blnk's own HTTP contract rather than the agent's domain — the reconciliation
// strategy name and Blnk's run-status strings — deliberately live in
// internal/blnk instead (blnk.StrategyOneToOne, blnk.ReconStatusCompleted,
// blnk.ReconStatusFailed), because internal/blnk must never import this package
// (that WOULD cycle, since model -> blnk).
//
// All constants are intentionally UNTYPED string constants so they remain
// drop-in assignable everywhere the pre-existing untyped local constants were
// used (string columns, string comparisons, string-typed struct fields, and
// map keys) without forcing a type conversion at every call site.
package model

// Break lifecycle statuses — the closed set of values persisted to
// agent_break.status. The remediator writes classified/auto-resolved/queued; a
// human decision transitions a queued break to accepted/re_driven/rejected via
// the HITL handler. The store, the HITL status page, and cmd's scoped summary
// all read these back, so every producer and consumer MUST reference exactly
// these constants.
const (
	// StatusClassified marks a break the classifier has labeled but that has not
	// yet been auto-resolved or escalated.
	StatusClassified = "classified"
	// StatusAutoResolved marks a break the remediator cleared via a Blnk-confirmed
	// dry-run (Rule 5.3); it is a terminal, non-actionable state.
	StatusAutoResolved = "auto-resolved"
	// StatusQueued marks a break awaiting a human decision in agent_hitl_queue. It
	// is the ONLY state from which accept/re_drive/reject may transition.
	StatusQueued = "queued"
	// StatusAccepted is the terminal state after a human accepts the break as a
	// legitimate, acknowledged exception.
	StatusAccepted = "accepted"
	// StatusReDriven is the terminal state after a human-triggered re_drive whose
	// Blnk dry-run confirmed clearance.
	StatusReDriven = "re_driven"
	// StatusRejected is the terminal state after a human rejects the break.
	StatusRejected = "rejected"
)

// Audit actions — the closed set of values written to agent_audit.action. The
// audit writer re-exports these (see internal/audit) so its public
// audit.Action* API and every existing reference remain stable while this
// package holds the canonical literal. Any event whose action is outside this
// set is rejected by the audit writer.
const (
	// ActionClassified records that the classifier assigned a root cause.
	ActionClassified = "classified"
	// ActionRuleProposed records that a Blnk-native matching rule was proposed.
	ActionRuleProposed = "rule_proposed"
	// ActionRuleCreated records that a proposed rule was POSTed to Blnk.
	ActionRuleCreated = "rule_created"
	// ActionProbed records a Blnk dry-run probe of a break's match status.
	ActionProbed = "probed"
	// ActionResolved records a break cleared with Blnk confirmation (Rule 5.3).
	ActionResolved = "resolved"
	// ActionEscalated records a break routed to the HITL queue.
	ActionEscalated = "escalated"
	// ActionAccepted records a human accept decision.
	ActionAccepted = "accepted"
	// ActionReDriven records a human re_drive decision. It remains the value of
	// StatusReDriven (the terminal break status after a confirmed-clearing
	// re_drive) and stays in the closed action set for backward compatibility
	// with any historical audit rows, but the re_drive HANDLER no longer emits
	// it: a re_drive now records the finer-grained, outcome-specific actions
	// below (finding F16) so cleared / still-unmatched / failed attempts are
	// each an unambiguous, immutable action rather than one 're_driven' value
	// disambiguated only by free-text rationale.
	ActionReDriven = "re_driven"
	// ActionRejected records a human reject decision.
	ActionRejected = "rejected"
	// ActionRuleCompensated records that a previously created Blnk matching rule
	// was compensated (deleted) after its remediation attempt did not clear.
	ActionRuleCompensated = "rule_compensated"
	// ActionReDriveAttempted records that a human INITIATED a re_drive of a
	// queued break (finding F16). It is written once, immediately after the
	// processing lease is acquired and before any Blnk interaction, so every
	// re_drive attempt is durably visible on the append-only trail regardless of
	// its eventual outcome — including an attempt that then fails against Blnk
	// and emits no probe. It carries no reconciliation id (nothing has cleared).
	ActionReDriveAttempted = "re_drive_attempted"
	// ActionReDriveCleared records the terminal outcome of a re_drive whose Blnk
	// dry-run CONFIRMED the break moved out of the unmatched set (finding F16,
	// Rule 5.3). It is the ONLY re_drive outcome action that carries a confirming
	// reconciliation id in its provenance (the durable clearance proof), and it
	// accompanies the break's transition to StatusReDriven.
	ActionReDriveCleared = "re_drive_cleared"
	// ActionReDriveUnmatched records the terminal outcome of a re_drive whose
	// Blnk dry-run ran but did NOT confirm clearance (finding F16). The break is
	// left queued for further review. It carries NO reconciliation id so a
	// reconciliation id can never imply a false clearance (finding m-04); the
	// preceding `probed` event already records the dry-run id and verdict.
	ActionReDriveUnmatched = "re_drive_unmatched"
	// ActionReDriveFailed records the terminal outcome of a re_drive that could
	// not complete because a Blnk interaction failed — the ephemeral rule
	// creation or the dry-run probe errored (finding F16, fail-closed Rule 5.7).
	// The break is left queued; the event carries a failure rationale and no
	// reconciliation id, so a Blnk-down attempt is a first-class, unambiguous
	// action rather than a 're_driven' value distinguishable only by its text.
	ActionReDriveFailed = "re_drive_failed"
)

// Matching-rule grammar (Rule 5.2) — the closed field and operator domains
// Blnk accepts, confirmed in Blnk's own reconciliation.go (validateField /
// validateOperator). The classifier validates every LLM-proposed rule against
// these sets BEFORE it can be POSTed to Blnk, and cmd builds its exact-match
// detection rule from the same field/operator constants, so both the validator
// and the rule builder share one vocabulary.
const (
	// FieldAmount matches on the transaction amount.
	FieldAmount = "amount"
	// FieldDate matches on the transaction date.
	FieldDate = "date"
	// FieldDescription matches on the transaction description.
	FieldDescription = "description"
	// FieldReference matches on the transaction reference.
	FieldReference = "reference"
	// FieldCurrency matches on the transaction currency.
	FieldCurrency = "currency"
)

const (
	// OperatorEquals requires exact equality between the external and internal
	// values of the criterion's field.
	OperatorEquals = "equals"
	// OperatorGreaterThan requires the external value to exceed the internal value.
	OperatorGreaterThan = "greater_than"
	// OperatorLessThan requires the external value to be below the internal value.
	OperatorLessThan = "less_than"
	// OperatorContains requires the external value to contain the internal value.
	OperatorContains = "contains"
)
