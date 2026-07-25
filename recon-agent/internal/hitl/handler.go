package hitl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"html"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Break statuses set by human decisions. statusQueued is the ONLY state a
// decision is eligible from (finding M-01); a decision transitions the break to
// a terminal state (accepted/re_driven/rejected). re_driven is set ONLY when
// Blnk confirms clearance; a non-clearing re_drive leaves the break queued
// (finding m-04).
// Each name aliases the canonical literal in internal/model (finding m-01), the
// single module-wide source of truth, while local use sites keep their concise
// unqualified names.
const (
	statusQueued   = model.StatusQueued
	statusAccepted = model.StatusAccepted
	statusReDriven = model.StatusReDriven
	statusRejected = model.StatusRejected
)

// Grammar-conformant field/operator constants for the rule re_drive builds to
// probe a queued break (Rule 5.2). They alias the canonical grammar vocabulary
// in internal/model (finding m-01) so the classifier's validator, cmd's
// detection rule, and this re_drive rule builder all share one domain.
const (
	fieldAmount    = model.FieldAmount
	fieldCurrency  = model.FieldCurrency
	operatorEquals = model.OperatorEquals
)

// Field bounds for a submitted decision (findings C-05, M-23). They keep the
// unauthenticated endpoint from accepting an unattributable or unbounded
// payload even after the router-level body/content-type/origin guard.
const (
	// maxExternalTxnIDLen bounds the external txn id a decision may target.
	maxExternalTxnIDLen = 256
	// maxReviewerLen bounds the reviewer identity string.
	maxReviewerLen = 128
	// maxNoteLen bounds the optional free-text reviewer note.
	maxNoteLen = 2048
)

// leaseReleaseTimeout bounds the independent context used to release a re_drive
// processing lease and to compensate an ephemeral probe rule, so a canceled or
// expired request context cannot skip that cleanup (findings M-15/M-11/M-12).
const leaseReleaseTimeout = 10 * time.Second

// errBreakBusy is returned by re_drive when another instance/request currently
// holds the break's processing lease (finding M-15). handleDecision maps it to
// 409 Conflict so the caller can retry rather than double-driving Blnk.
var errBreakBusy = errors.New("hitl: break is currently being processed")

// upstreamError marks a failure that originated in the Blnk dependency (an
// upstream service), so handleDecision maps it to 502 Bad Gateway instead of
// 500 (finding L2) — distinguishing a dependency fault from an internal agent
// fault. It unwraps to the underlying error for logging/inspection.
type upstreamError struct{ err error }

func (e *upstreamError) Error() string { return e.err.Error() }
func (e *upstreamError) Unwrap() error { return e.err }

// asUpstream wraps err as an upstreamError (nil-safe).
func asUpstream(err error) error {
	if err == nil {
		return nil
	}
	return &upstreamError{err: err}
}

// handleDecision binds a model.HITLDecision (JSON body or HTML form) and
// dispatches it to the accept/re_drive/reject handlers. Each successful decision
// writes exactly one AuditEvent via the append-only audit writer, committed
// atomically with the state change it records (Gate 13; Rule 5.5; findings
// M-01/M-02). Invalid input (bad payload, blank id, unknown decision) is
// rejected with a sanitized 400 before any state change or audit write; internal
// and upstream errors are logged server-side and returned as stable, sanitized
// messages (M-03) with the right status — 404 unknown break, 409 already-decided
// break, 502 Blnk (upstream) fault, 500 otherwise.
func (s *Server) handleDecision(c *gin.Context) {
	d, err := bindDecision(c)
	if err != nil {
		// M-03: never leak the framework/JSON parse error (it names Go struct
		// internals); log it and return a stable message.
		log.Printf("hitl: decision bind error: %v", err)
		s.respondDecisionError(c, http.StatusBadRequest, "invalid request payload")
		return
	}
	if msg, ok := validateDecisionFields(&d); !ok {
		// The offending value is client-supplied; return a stable, non-reflecting
		// message (M-03) — never echo the raw decision verb or payload back.
		s.respondDecisionError(c, http.StatusBadRequest, msg)
		return
	}

	ctx := c.Request.Context()
	var handleErr error
	switch d.Decision {
	case audit.DecisionAccept:
		handleErr = s.accept(ctx, d)
	case audit.DecisionReDrive:
		handleErr = s.reDrive(ctx, d)
	case audit.DecisionReject:
		handleErr = s.reject(ctx, d)
	default:
		// Unreachable: IsValidDecision gated the verb above. Defensive only.
		s.respondDecisionError(c, http.StatusBadRequest, "unknown decision")
		return
	}

	if handleErr != nil {
		status, msg := publicError(handleErr)
		// M-03: log the concrete error server-side; return only the sanitized
		// message to the client.
		log.Printf("hitl: decision %q for break %q failed: %v", d.Decision, d.ExternalTxnID, handleErr)
		s.respondDecisionError(c, status, msg)
		return
	}

	s.respondDecision(c, d)
}

// publicError maps an internal error to a stable, client-safe (status, message)
// pair (M-03). It never exposes the underlying pq:/framework/upstream/Go error
// text — the caller logs that separately. A missing break is 404 (finding
// m-01), a non-queued (already-decided) break is 409 (finding M-01), and a Blnk
// (upstream) fault is 502 so the operator can distinguish a dependency fault
// from an agent bug and safely retry (finding L2). Every other error collapses
// to a generic 500 so no internal detail leaks by default.
func publicError(err error) (int, string) {
	var upstream *upstreamError
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "break not found"
	case errors.Is(err, store.ErrNotQueued):
		return http.StatusConflict, "break is not awaiting review"
	case errors.Is(err, errBreakBusy):
		// M-15: another instance/request holds the break's processing lease.
		// 409 (not 500) tells the caller this is a transient conflict to retry.
		return http.StatusConflict, "break is currently being processed"
	case errors.As(err, &upstream):
		return http.StatusBadGateway, "reconciliation service temporarily unavailable"
	default:
		return http.StatusInternalServerError, "internal error processing decision"
	}
}

// validateDecisionFields normalizes and validates a bound decision (findings
// C-05, M-23). It trims and requires a non-empty, length-bounded external txn
// id; requires a recognized decision verb; and — critically — REQUIRES an
// attributable reviewer identity: a blank reviewer is REJECTED rather than
// silently defaulted to a shared pseudo-identity, so every audit event names a
// real reviewer (M-23). The optional note is length-bounded. On failure it
// returns a stable, client-safe message and false; the pointer's fields are
// normalized in place so the caller audits the trimmed values.
func validateDecisionFields(d *model.HITLDecision) (string, bool) {
	d.ExternalTxnID = strings.TrimSpace(d.ExternalTxnID)
	if d.ExternalTxnID == "" {
		return "external_txn_id is required", false
	}
	if len(d.ExternalTxnID) > maxExternalTxnIDLen {
		return "external_txn_id is too long", false
	}
	// Finding #5: reject a control character (e.g. an embedded NUL U+0000) in the
	// identifier BEFORE it reaches the store. PostgreSQL's text type cannot hold a
	// NUL, so lib/pq would otherwise fail the query and the request would surface
	// as a confusing HTTP 500 instead of a clean 400. Identifiers carry no control
	// characters, so ALL of them are rejected here.
	if containsDisallowedControlChar(d.ExternalTxnID, false) {
		return "external_txn_id contains invalid control characters", false
	}
	if !audit.IsValidDecision(d.Decision) {
		return "unknown decision", false
	}
	// M-23: reviewer identity is mandatory and attributable. A blank reviewer is
	// an unattributable decision on this no-auth surface and is rejected outright
	// (the append-only writer's ErrEmptyActor is the defense-in-depth backstop).
	d.Reviewer = strings.TrimSpace(d.Reviewer)
	if d.Reviewer == "" {
		return "reviewer is required", false
	}
	if len(d.Reviewer) > maxReviewerLen {
		return "reviewer is too long", false
	}
	// Finding #5: the reviewer identity is an identifier — no control characters
	// (including an embedded NUL) are valid; reject them with a 400 rather than
	// letting a NUL reach lib/pq and surface as a 500.
	if containsDisallowedControlChar(d.Reviewer, false) {
		return "reviewer contains invalid control characters", false
	}
	if len(d.Note) > maxNoteLen {
		return "note is too long", false
	}
	// Finding #5: the free-text note may legitimately contain ordinary text
	// whitespace (tab / newline / carriage return) — a multi-line note is
	// preserved — but an embedded NUL (unstorable in a PostgreSQL text column) and
	// every other C0/C1/DEL control character are rejected with a 400 rather than
	// surfacing as a 500 from the persistence layer.
	if containsDisallowedControlChar(d.Note, true) {
		return "note contains invalid control characters", false
	}
	return "", true
}

// containsDisallowedControlChar reports whether s contains a control character
// that must cause a 400 rather than propagate to the persistence layer. An
// embedded NUL (U+0000) is the motivating case (finding #5): PostgreSQL's text
// type cannot store it, so lib/pq rejects the query and the request would
// otherwise return a confusing HTTP 500 instead of a clean client-side 400.
//
// For identifier fields (external_txn_id, reviewer) EVERY control character is
// disallowed — they carry none legitimately. For the free-text note,
// allowTextWhitespace permits ordinary text whitespace (tab, newline, carriage
// return) so a legitimate multi-line note is preserved, while NUL and every
// other C0/C1/DEL control character (unicode.IsControl) are still rejected.
func containsDisallowedControlChar(s string, allowTextWhitespace bool) bool {
	for _, r := range s {
		if !unicode.IsControl(r) {
			continue
		}
		if allowTextWhitespace && (r == '\t' || r == '\n' || r == '\r') {
			continue
		}
		return true
	}
	return false
}

// bindDecision reads a HITLDecision from a JSON body (Content-Type
// application/json) or, otherwise, from posted form fields. The request body was
// already bounded and its content type allow-listed by the router's
// mutationGuard (findings C-05/M-23). For the JSON path it uses a STRICT decoder
// that rejects unknown fields and trailing data, so a payload carrying
// unexpected keys (a sign of a confused or hostile client) is refused rather
// than silently accepted (M-23). The form path reads only the known fields,
// which is inherently strict.
func bindDecision(c *gin.Context) (model.HITLDecision, error) {
	var d model.HITLDecision
	if contentTypeOf(c) == "application/json" {
		dec := json.NewDecoder(c.Request.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&d); err != nil {
			return model.HITLDecision{}, err
		}
		// Reject trailing garbage / a second JSON value after the object.
		if dec.More() {
			return model.HITLDecision{}, errors.New("unexpected trailing data after JSON body")
		}
		return d, nil
	}
	d.ExternalTxnID = c.PostForm("external_txn_id")
	d.Decision = c.PostForm("decision")
	d.Reviewer = c.PostForm("reviewer")
	d.Note = c.PostForm("note")
	return d, nil
}

// respondDecision replies with JSON for API clients and a 303 redirect back to
// the status page for browser form submissions.
func (s *Server) respondDecision(c *gin.Context, d model.HITLDecision) {
	if c.ContentType() == "application/json" {
		c.JSON(http.StatusOK, gin.H{
			"external_txn_id": d.ExternalTxnID,
			"decision":        d.Decision,
			"status":          "recorded",
		})
		return
	}
	c.Redirect(http.StatusSeeOther, "/")
}

// respondDecisionError returns a sanitized error to the client: JSON for API
// clients and a minimal, friendly HTML panel for browser/form submissions
// (M-03). It never includes the underlying internal error text.
func (s *Server) respondDecisionError(c *gin.Context, status int, msg string) {
	if c.ContentType() == "application/json" {
		c.JSON(status, gin.H{"error": msg})
		return
	}
	c.Data(status, "text/html; charset=utf-8", errorPageHTML(msg))
}

// errorPageHTML renders a minimal, friendly HTML error panel (embedded CSS only,
// AAP 0.5.3) carrying a sanitized message and a link back to the status page.
// The message is HTML-escaped defensively even though callers pass only fixed,
// internal constants — never raw error text (M-03). The status page reuses this
// via renderStatusError so page-load and form-submit errors share one surface.
func errorPageHTML(msg string) []byte {
	return []byte(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>recon-agent &mdash; error</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1a1a1a; }
  .panel { border: 1px solid #f0c0c0; background: #fdf2f2; color: #b00020; padding: 1rem 1.2rem; border-radius: 6px; max-width: 40rem; }
  a { color: #0b5cad; }
</style>
</head>
<body>
  <div class="panel">` + html.EscapeString(msg) + `</div>
  <p><a href="/">&larr; Back to review</a></p>
</body>
</html>
`)
}

// accept marks a queued break resolved-by-human, drains it from the HITL queue,
// and records an `accepted` audit event — all atomically (findings M-01/M-02).
func (s *Server) accept(ctx context.Context, d model.HITLDecision) error {
	return s.decide(ctx, d, statusAccepted)
}

// decide is the shared accept/reject path: in ONE transaction it (1) transitions
// the break out of the queued state via the queued-only guard
// (SetBreakStatusFromQueuedTx — so a terminal/already-decided break yields
// ErrNotQueued -> 409 and a missing break yields ErrNotFound -> 404, never a
// silent overwrite: findings M-01/m-01), (2) drains the HITL queue, and (3)
// records the decision audit event via the tx-bound append-only writer (Rule
// 5.5). Because all three commit or roll back together, a decision can never
// report success while leaving status, queue, and audit inconsistent (M-02).
func (s *Server) decide(ctx context.Context, d model.HITLDecision, status string) error {
	return s.st.WithTx(ctx, func(tx *sql.Tx) error {
		if err := s.st.SetBreakStatusFromQueuedTx(ctx, tx, d.ExternalTxnID, status); err != nil {
			return err
		}
		if err := s.st.DequeueHITLTx(ctx, tx, d.ExternalTxnID); err != nil {
			return err
		}
		return s.aud.RecordTx(ctx, tx, audit.Decision(d, model.Provenance{Model: s.llmModel}))
	})
}

// reDrive re-tests clearance by re-invoking a Blnk dry-run probe (the
// deterministic arbiter, Rule 5.3), updates status, drains the queue only when
// Blnk confirms clearance, and records a `re_driven` audit event carrying the
// recon_id.
//
// Finding L2: the probe must carry the break's FULL external transaction (not
// just its id) and a NON-EMPTY matching_rule_ids set — Blnk's start-instant
// rejects an empty rule set with 400. We therefore load the persisted
// transaction, reuse any matching rule a prior auto-attempt created for it, and
// otherwise build a grammar-conformant {amount,equals}(+{currency,equals})
// rule (Rule 5.2) just to probe. A rule created here is a throwaway probe
// artifact and is always deleted afterwards so no orphan rule accumulates in
// Blnk (finding F5 parity) — the clearance verdict recorded on the audit event
// (recon_id) is the durable proof, not the rule.
//
// Finding M-15 (race/idempotency): the external Blnk work (rule creation +
// dry-run probe) is guarded by a durable, cross-instance processing LEASE
// (ClaimBreak) acquired BEFORE any external call and released afterwards. Two
// concurrent re_drive requests for the same queued break can therefore no longer
// both hit Blnk or race the terminal state change: exactly one wins the lease
// and does the work; the other is told the break is being processed (409). The
// queued-only terminal CAS (SetBreakStatusFromQueuedTx) is the second, durable
// idempotency backstop — a replayed re_drive after the break already reached a
// terminal state yields ErrNotQueued (409), never a double transition.
//
// Finding M-12 (audit every attempt and compensation outcome): every Blnk
// interaction is now recorded on the append-only trail — a successful ephemeral
// rule creation (`rule_created`), the probe outcome (`probed`, with its recon
// id), the compensation of the ephemeral rule (`rule_compensated`, deleted or
// not), and a FAILED rule-creation/probe attempt (a `re_driven` event carrying a
// failure rationale and no recon id). Nothing that touches Blnk is silent.
//
// Failures originating in Blnk (rule creation or probe) are wrapped as
// upstreamError so handleDecision returns 502, never 500, and the break is left
// in the queue unchanged (fail-closed, Rule 5.7). Only a successful CLEARING
// probe updates status / drains the queue / writes the terminal re_driven event.
func (s *Server) reDrive(ctx context.Context, d model.HITLDecision) error {
	id := d.ExternalTxnID

	// (1) Existence + eligibility BEFORE any lease or Blnk call: a missing break
	//     is 404 and a non-queued (already-decided) break is 409, both decided
	//     before any external work (findings m-01/M-01).
	_, status, _, found, err := s.st.LoadBreak(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		return store.ErrNotFound
	}
	if status != statusQueued {
		return store.ErrNotQueued
	}

	// (2) M-15: acquire a durable, cross-instance processing lease BEFORE any
	//     external Blnk work. A concurrent re_drive that cannot get the lease is
	//     rejected as busy (409) rather than duplicating rule creation / probes.
	//     The lease is released when this path completes (or fails) on an
	//     independent context so a canceled request cannot leave it dangling; a
	//     crashed holder's lease self-expires after reDriveLeaseTTL.
	granted, claimErr := s.st.ClaimBreak(ctx, id, s.owner, reDriveLeaseTTL)
	if claimErr != nil {
		if errors.Is(claimErr, store.ErrNotFound) {
			return store.ErrNotFound
		}
		return claimErr
	}
	if !granted {
		return errBreakBusy
	}
	defer func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
		defer cancel()
		_ = s.st.ReleaseBreak(relCtx, id, s.owner)
	}()

	// (3) Load the FULL external transaction (and any rule a prior auto-attempt
	//     created) so the clearance probe carries a real transaction and a
	//     non-empty rule set — Blnk's start-instant rejects an empty rule set
	//     (findings L2/C-01).
	txn, createdRuleID, found, err := s.st.LoadBreakTxn(ctx, id)
	if err != nil {
		return err
	}
	if !found {
		// Raced with a concurrent delete between the two reads.
		return store.ErrNotFound
	}

	ruleID := createdRuleID
	createdHere := false
	if ruleID == "" {
		// No rule persisted for this break — build a throwaway rule from the
		// transaction's own fields so Blnk has a non-empty rule set to probe.
		created, cerr := s.bc.CreateMatchingRule(ctx, buildReDriveRule(txn))
		if cerr != nil {
			// M-12: record the failed create attempt; break stays queued.
			s.auditReDriveFailure(ctx, d, "re_drive rule creation failed (Blnk upstream)")
			return asUpstream(cerr)
		}
		if created.RuleID == "" {
			s.auditReDriveFailure(ctx, d, "re_drive rule creation returned an empty rule id (Blnk upstream)")
			return asUpstream(errors.New("blnk returned an empty rule id for re_drive probe"))
		}
		ruleID = created.RuleID
		createdHere = true
		// M-12: audit the successful ephemeral-rule creation. If the audit write
		// itself fails, compensate the just-created rule so we never leak an
		// unaudited rule into Blnk, then fail closed.
		if aerr := s.aud.Record(ctx, audit.RuleCreated(id, audit.RuleRef{ID: ruleID}, 0, model.Provenance{Model: s.llmModel})); aerr != nil {
			s.compensateReDriveRule(ctx, id, ruleID)
			return aerr
		}
	}
	// A rule created purely to probe is always removed afterwards, and the
	// compensation OUTCOME (deleted or orphaned) is audited (findings M-11/M-12).
	// A rule REUSED from a prior auto-attempt is left intact (it is not ours to
	// delete).
	if createdHere {
		defer s.compensateReDriveRule(ctx, id, ruleID)
	}

	// (4) Deterministic arbiter: probe Blnk with the full txn + rule id (Rule
	//     5.3). Any error fails closed, leaving the break queued (Rule 5.7/L2).
	cleared, reconID, perr := s.bc.ProbeBreak(ctx, txn, []string{ruleID})
	if perr != nil {
		// M-12: record the failed probe attempt; break stays queued (the
		// deferred compensation still removes any ephemeral rule).
		s.auditReDriveFailure(ctx, d, "re_drive probe failed (Blnk upstream)")
		return asUpstream(perr)
	}
	// M-12: audit the probe outcome (cleared or still unmatched) with its recon
	// id. A failure to record the probe fails the decision closed (500) rather
	// than proceeding with an unaudited probe.
	if aerr := s.aud.Record(ctx, audit.Probed(id, reconID, cleared, model.Provenance{Model: s.llmModel})); aerr != nil {
		return aerr
	}

	// (5a) Not cleared: keep the break queued (no dequeue, no terminal status),
	//      record a re_driven decision WITHOUT a recon_id so a recon_id never
	//      implies a false clearance (finding m-04; only `resolved` proves
	//      clearance, and HITL never writes `resolved` — Rule 5.3).
	if !cleared {
		return s.aud.Record(ctx, audit.Decision(d, model.Provenance{Model: s.llmModel}))
	}

	// (5b) Cleared: mark re_driven, drain the queue, and audit WITH the
	//      confirming recon_id — atomically and queued-only (findings M-01/M-02,
	//      m-04). The queued-only CAS also makes a replayed re_drive idempotent:
	//      a second attempt after this transition yields ErrNotQueued (409).
	return s.st.WithTx(ctx, func(tx *sql.Tx) error {
		if err := s.st.SetBreakStatusFromQueuedTx(ctx, tx, id, statusReDriven); err != nil {
			return err
		}
		if err := s.st.DequeueHITLTx(ctx, tx, id); err != nil {
			return err
		}
		return s.aud.RecordTx(ctx, tx, audit.Decision(d, model.Provenance{Model: s.llmModel, ReconID: reconID}))
	})
}

// auditReDriveFailure records a best-effort `re_driven` audit event capturing a
// re_drive attempt that failed against Blnk — a failed rule creation or a failed
// probe (finding M-12). The break is left queued (fail-closed, Rule 5.7); the
// reviewer can retry. The event carries a failure-specific rationale and NO
// recon_id (no clearance was proven, finding m-04). An audit-write failure here
// is logged rather than surfaced, because the caller is already returning the
// originating upstream error to the client — we do not want a secondary audit
// hiccup to mask the real (502) upstream cause.
func (s *Server) auditReDriveFailure(ctx context.Context, d model.HITLDecision, reason string) {
	ev := audit.Decision(d, model.Provenance{Model: s.llmModel})
	ev.Rationale = reason
	if aerr := s.aud.Record(ctx, ev); aerr != nil {
		log.Printf("hitl: failed to audit re_drive failure for break %q: %v", d.ExternalTxnID, aerr)
	}
}

// compensateReDriveRule deletes the ephemeral rule re_drive created solely to
// probe, and records the compensation OUTCOME as a durable audit event
// (findings M-11/M-12): `rule_compensated` with deleted=true when Blnk removed
// the rule, or deleted=false when it could NOT be removed — a durable record
// that an orphaned rule still exists in Blnk and needs manual cleanup. It runs
// on an INDEPENDENT context so a canceled/expired request context cannot skip
// the cleanup, mirroring the remediator's compensation discipline. A delete
// failure or audit-write failure is logged; a completed re_drive outcome is not
// reversed by a cleanup hiccup — the recon_id, not the ephemeral rule, is the
// durable clearance proof.
func (s *Server) compensateReDriveRule(ctx context.Context, externalTxnID, ruleID string) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
	defer cancel()

	derr := s.bc.DeleteMatchingRule(cctx, ruleID)
	deleted := derr == nil
	if derr != nil {
		log.Printf("hitl: re_drive could not delete ephemeral probe rule %q for break %q (orphaned in Blnk): %v", ruleID, externalTxnID, derr)
	}
	if aerr := s.aud.Record(cctx, audit.RuleCompensated(externalTxnID, ruleID, deleted, "", 0, model.Provenance{Model: s.llmModel})); aerr != nil {
		log.Printf("hitl: failed to audit re_drive rule compensation for break %q rule %q: %v", externalTxnID, ruleID, aerr)
	}
}

// buildReDriveRule constructs a grammar-conformant (Rule 5.2) matching rule from
// a break's own transaction so re_drive can probe with a non-empty rule set. It
// mirrors the remediator's rule narrowing (amount, plus currency when present)
// without importing remediator (Rule 5.1). Blnk matches external-vs-internal
// field values and ignores criteria.Value, but Value is still populated for
// intent/audit fidelity.
func buildReDriveRule(txn blnk.ExternalTransaction) blnk.MatchingRule {
	criteria := []blnk.MatchingCriteria{{
		Field:    fieldAmount,
		Operator: operatorEquals,
		Value:    strconv.FormatFloat(txn.Amount, 'f', -1, 64),
	}}
	if txn.Currency != "" {
		criteria = append(criteria, blnk.MatchingCriteria{
			Field:    fieldCurrency,
			Operator: operatorEquals,
			Value:    txn.Currency,
		})
	}
	return blnk.MatchingRule{
		Name:        "hitl-redrive-" + txn.ID,
		Description: "ephemeral re_drive probe rule (auto-deleted)",
		Criteria:    criteria,
	}
}

// reject marks a queued break closed, drains it from the HITL queue, and records
// a `rejected` audit event — all atomically (findings M-01/M-02).
func (s *Server) reject(ctx context.Context, d model.HITLDecision) error {
	return s.decide(ctx, d, statusRejected)
}
