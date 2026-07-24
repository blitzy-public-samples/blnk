package hitl

import (
	"context"
	"database/sql"
	"errors"
	"html"
	"log"
	"net/http"
	"strconv"

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
const (
	statusQueued   = "queued"
	statusAccepted = "accepted"
	statusReDriven = "re_driven"
	statusRejected = "rejected"
)

// Grammar-conformant field/operator constants for the rule re_drive builds to
// probe a queued break (Rule 5.2). They mirror the domain Blnk accepts.
const (
	fieldAmount    = "amount"
	fieldCurrency  = "currency"
	operatorEquals = "equals"
)

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
	if d.ExternalTxnID == "" {
		s.respondDecisionError(c, http.StatusBadRequest, "external_txn_id is required")
		return
	}
	if !audit.IsValidDecision(d.Decision) {
		// The decision verb is client-supplied; do not echo it back (avoids
		// reflecting arbitrary input onto the page/response). A generic message
		// suffices (M-03).
		s.respondDecisionError(c, http.StatusBadRequest, "unknown decision")
		return
	}
	if d.Reviewer == "" {
		d.Reviewer = defaultReviewer
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
	case errors.As(err, &upstream):
		return http.StatusBadGateway, "reconciliation service temporarily unavailable"
	default:
		return http.StatusInternalServerError, "internal error processing decision"
	}
}

// bindDecision reads a HITLDecision from a JSON body (Content-Type
// application/json) or, otherwise, from posted form fields.
func bindDecision(c *gin.Context) (model.HITLDecision, error) {
	var d model.HITLDecision
	if c.ContentType() == "application/json" {
		if err := c.ShouldBindJSON(&d); err != nil {
			return model.HITLDecision{}, err
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
// Failures originating in Blnk (rule creation or probe) are wrapped as
// upstreamError so handleDecision returns 502, never 500, and the break is left
// in the queue unchanged (fail-closed, Rule 5.7). Only a successful probe
// updates status / drains the queue / writes the audit event.
func (s *Server) reDrive(ctx context.Context, d model.HITLDecision) error {
	id := d.ExternalTxnID

	// (1) Existence + eligibility BEFORE any Blnk call: a missing break is 404
	//     and a non-queued (already-decided) break is 409, both decided before
	//     any external work (findings m-01/M-01).
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

	// (2) Load the FULL external transaction (and any rule a prior auto-attempt
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
			return asUpstream(cerr)
		}
		if created.RuleID == "" {
			return asUpstream(errors.New("blnk returned an empty rule id for re_drive probe"))
		}
		ruleID = created.RuleID
		createdHere = true
	}
	// A rule created purely to probe is always removed afterwards (F5 parity).
	if createdHere {
		defer func() { _ = s.bc.DeleteMatchingRule(ctx, ruleID) }()
	}

	// (3) Deterministic arbiter: probe Blnk with the full txn + rule id (Rule
	//     5.3). Any error fails closed, leaving the break queued (Rule 5.7/L2).
	cleared, reconID, perr := s.bc.ProbeBreak(ctx, txn, []string{ruleID})
	if perr != nil {
		return asUpstream(perr)
	}

	// (4a) Not cleared: keep the break queued (no dequeue, no terminal status),
	//      record a re_driven decision WITHOUT a recon_id so a recon_id never
	//      implies a false clearance (finding m-04; only `resolved` proves
	//      clearance, and HITL never writes `resolved` — Rule 5.3).
	if !cleared {
		return s.aud.Record(ctx, audit.Decision(d, model.Provenance{Model: s.llmModel}))
	}

	// (4b) Cleared: mark re_driven, drain the queue, and audit WITH the
	//      confirming recon_id — atomically and queued-only (findings M-01/M-02,
	//      m-04).
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
