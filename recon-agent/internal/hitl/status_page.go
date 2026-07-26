package hitl

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
)

// statusTemplate is the single, server-rendered HTML status page for the HITL
// review surface (AAP 0.5.3). It uses only minimal embedded CSS: no component
// library, no CSS framework, and no client-side JS framework are introduced.
var statusTemplate = template.Must(template.New("status").Parse(statusPageHTML))

// ruleCriterionView renders one fully-escaped criterion of a proposed matching
// rule so the reviewer sees the exact operator semantics that would be applied
// (finding M-22: "full escaped operator context"). Field/Operator are the Blnk
// grammar tokens (Rule 5.2); Detail carries any drift/pattern/value qualifier.
type ruleCriterionView struct {
	Field    string
	Operator string
	Detail   string
}

// breakView is the per-break row rendered on the status page.
type breakView struct {
	ExternalTxnID string
	RootCause     string
	Confidence    float64
	Regulated     bool
	Status        string
	// Queued reports whether this break is still awaiting a human decision. Only
	// queued rows render the accept/re_drive/reject controls (finding C-07);
	// terminal rows (auto-resolved/accepted/re_driven/rejected/classified) render
	// their status text only, with no actionable form.
	Queued bool
	// Rationale is the classifier's short justification for the root cause — the
	// per-break decision context a reviewer needs (finding M-22).
	Rationale string
	RuleName  string
	HasRule   bool
	// RuleCriteria is the full, escaped criteria list of the proposed rule so the
	// reviewer can see exactly what Blnk would match on (finding M-22). Empty when
	// no rule was proposed.
	RuleCriteria []ruleCriterionView
	// BelowThreshold marks a break whose confidence is below the auto-remediation
	// threshold; the row renders an explicit "below auto-threshold" tag so a
	// human-gated break can never read as auto-eligible (finding F-A).
	BelowThreshold bool
	// Reason is the human-readable routing reason recorded on the HITL queue for a
	// currently-queued break (finding F-I); empty (em-dash) for non-queued rows.
	Reason string
	// ResolvedReconID is the confirming Blnk dry-run reconciliation id that proves
	// this break cleared — displayed as first-class evidence next to a cleared
	// break (finding F16). It is populated for an auto-resolved break and for a
	// re_driven break whose re_drive Blnk-confirmed clearance; empty (em-dash) for
	// every non-cleared status, so a re_driven row can never show a blank proof.
	ResolvedReconID string
}

// auditView is a single append-only audit row rendered on the status page.
type auditView struct {
	Timestamp     string
	ExternalTxnID string
	Actor         string
	Action        string
	Confidence    float64
	// ConfidenceNA marks a human-decision audit row (accepted/re_driven/rejected)
	// whose stored Confidence is a zero value meaning "not applicable"; it renders
	// as an em-dash rather than a misleading "0.0000" (finding F-J). Agent actions
	// always carry a real confidence and stay numeric, including a genuine 0.0000.
	ConfidenceNA bool
	ReconID      string
	// Source and UploadID surface the break's origin — the external-statement
	// source label and the Blnk upload batch id — from the event provenance, so a
	// reviewer can trace which statement/batch a break came from (finding F-K).
	Source    string
	UploadID  string
	Rationale string
}

// pageData is the view model handed to statusTemplate.
type pageData struct {
	Title string
	Model string
	// CSRFToken is the per-render double-submit CSRF token embedded as a hidden
	// field in every decision form; it must equal the csrf_token cookie the page
	// set for the POST to be accepted by the mutation guard (finding C-05).
	CSRFToken string
	// AutoThreshold is the auto-remediation confidence gate surfaced in the page
	// header and used to render the per-break below-threshold tag (finding F-A).
	AutoThreshold float64
	// ReviewerMaxLen and NoteMaxLen are the reviewer/note length bounds (in runes,
	// finding F21) rendered into the decision form's HTML maxlength attributes.
	// They are set from the handler's maxReviewerLen/maxNoteLen constants so the
	// client-side limit is derived from — and can never drift from — the exact
	// server-side rune bound enforced by validateDecisionFields.
	ReviewerMaxLen int
	NoteMaxLen     int
	Breaks         []breakView
	// BreaksNav is the bounded pagination state for the breaks table so rows
	// beyond the first page — including still-queued ones — are reachable via
	// Prev/Next traversal controls instead of being silently hidden (finding
	// F06).
	BreaksNav breaksNav
	Audits    []auditView
}

// breaksNav carries the breaks-table pagination window and precomputed traversal
// targets for the status page (finding F06). From/To are 1-based inclusive row
// positions of the current page within the full set of Total breaks; HasPrev/
// HasNext gate the Prev/Next links and PrevOffset/NextOffset are their offsets.
// The Limit is echoed into the links so the chosen page size is preserved while
// traversing.
type breaksNav struct {
	Total      int
	Limit      int
	Offset     int
	From       int
	To         int
	HasPrev    bool
	PrevOffset int
	HasNext    bool
	NextOffset int
}

// statusPage renders the operator/demo status page: the breaks table (with the
// proposed MatchingRule when present) and the append-only audit trail. Simple
// <form> controls on each row POST an accept/re_drive/reject decision.
func (s *Server) statusPage(c *gin.Context) {
	ctx := c.Request.Context()
	data := pageData{
		Title: "recon-agent — HITL review", Model: s.llmModel, AutoThreshold: s.autoThreshold,
		// F21: derive the form's HTML maxlength from the server-side rune bounds so
		// the browser and the server enforce the SAME documented limit.
		ReviewerMaxLen: maxReviewerLen, NoteMaxLen: maxNoteLen,
	}

	// C-05: mint a fresh double-submit CSRF token, set it as a SameSite=Strict,
	// HttpOnly cookie, and embed the same value in every decision form below. The
	// mutation guard rejects a form POST whose token field does not match this
	// cookie, defeating cross-site form submission. HttpOnly is safe here because
	// the server embeds the token into the form itself (no client JS reads it);
	// SameSite=Strict alone already blocks the cookie on cross-site requests, and
	// the origin check is the third, independent layer.
	if token := newCSRFToken(); token != "" {
		http.SetCookie(c.Writer, &http.Cookie{
			Name:     csrfCookieName,
			Value:    token,
			Path:     "/",
			MaxAge:   csrfCookieMaxAge,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
		data.CSRFToken = token
	}

	// F06: read ONE bounded page of breaks (honoring ?limit=/?offset=) plus the
	// total, so rows beyond the first defaultPageLimit page — including
	// still-queued ones — are reachable through the Prev/Next controls rendered
	// below rather than silently truncated. The default (no query params) still
	// shows the first page, preserving the common single-page view.
	page := pageParams(c).Normalize()
	total, err := s.st.CountBreaks(ctx)
	if err != nil {
		log.Printf("hitl: status page — count breaks failed: %v", err)
		s.renderStatusError(c)
		return
	}
	breaks, err := s.st.ListBreaksPage(ctx, page)
	if err != nil {
		log.Printf("hitl: status page — list breaks failed: %v", err)
		s.renderStatusError(c)
		return
	}
	// Precompute the traversal window. From/To are 1-based inclusive positions;
	// an empty page yields From=0 (the template then shows "0 breaks"). HasNext
	// uses len(breaks) (not the requested limit) so a short final page ends the
	// traversal correctly; HasPrev is simply a nonzero offset.
	nav := breaksNav{Total: total, Limit: page.Limit, Offset: page.Offset}
	if len(breaks) > 0 {
		nav.From = page.Offset + 1
		nav.To = page.Offset + len(breaks)
	}
	if page.Offset > 0 {
		nav.HasPrev = true
		nav.PrevOffset = page.Offset - page.Limit
		if nav.PrevOffset < 0 {
			nav.PrevOffset = 0
		}
	}
	if page.Offset+len(breaks) < total {
		nav.HasNext = true
		nav.NextOffset = page.Offset + page.Limit
	}
	data.BreaksNav = nav

	// F-I: fetch the human-review queue so each queued break can display WHY it
	// was routed to a human (regulated, low confidence, fail-closed, ...). The
	// reason is persisted on agent.agent_hitl_queue and is meaningful only while a
	// break is still queued; a decided/auto-resolved break has been drained and
	// carries no reason (it renders an em-dash). A queue-read failure is handled
	// like a breaks/audit-read failure: log server-side and show the sanitized
	// error panel rather than a partially-populated, misleading page.
	queue, err := s.st.ListHITL(ctx)
	if err != nil {
		log.Printf("hitl: status page — list HITL queue failed: %v", err)
		s.renderStatusError(c)
		return
	}
	reasonByID := make(map[string]string, len(queue))
	for _, q := range queue {
		reasonByID[q.ExternalTxnID] = q.Reason
	}

	for _, b := range breaks {
		bv := breakView{
			ExternalTxnID: b.Classification.ExternalTxnID,
			RootCause:     string(b.Classification.RootCause),
			Confidence:    b.Classification.Confidence,
			Regulated:     b.Classification.Regulated,
			Status:        b.Status,
			// C-07: decision controls render ONLY for persisted queued rows.
			Queued:    b.Status == statusQueued,
			Rationale: b.Classification.Rationale,
			// F-A: flag a below-threshold confidence explicitly so the human
			// safety surface never presents it as auto-eligible.
			BelowThreshold: b.Classification.Confidence < s.autoThreshold,
			// F-I: the routing reason for a currently-queued break (empty for a
			// non-queued break, which renders an em-dash).
			Reason: reasonByID[b.Classification.ExternalTxnID],
			// F16: the confirming dry-run reconciliation id proving a cleared
			// break (auto-resolved or re_driven); empty for a non-cleared status.
			ResolvedReconID: b.ResolvedReconID,
		}
		if r := b.Classification.ProposedRule; r != nil {
			bv.HasRule = true
			bv.RuleName = r.Name
			// M-22: surface the FULL, escaped operator context (every criterion),
			// not just the rule name, so the reviewer can judge the proposed match.
			bv.RuleCriteria = ruleCriteriaViews(r.Criteria)
		}
		data.Breaks = append(data.Breaks, bv)
	}

	audits, err := s.st.ListAudit(ctx)
	if err != nil {
		log.Printf("hitl: status page — list audit failed: %v", err)
		s.renderStatusError(c)
		return
	}
	for _, a := range audits {
		// F-J / F16: a human decision or a re_drive outcome (accepted / rejected /
		// re_driven and the finer re_drive_attempted / re_drive_cleared /
		// re_drive_unmatched / re_drive_failed) is recorded with no classification
		// confidence, so its stored Confidence is a zero value meaning "not
		// applicable" — render it as an em-dash rather than a misleading "0.0000".
		// Agent actions (classified/resolved/escalated/...) always carry a real
		// confidence and stay numeric, including a genuine 0.
		confidenceNA := a.Action == audit.ActionAccepted ||
			a.Action == audit.ActionReDriven ||
			a.Action == audit.ActionRejected ||
			a.Action == audit.ActionReDriveAttempted ||
			a.Action == audit.ActionReDriveCleared ||
			a.Action == audit.ActionReDriveUnmatched ||
			a.Action == audit.ActionReDriveFailed
		data.Audits = append(data.Audits, auditView{
			Timestamp:     a.Timestamp,
			ExternalTxnID: a.ExternalTxnID,
			Actor:         a.Actor,
			Action:        a.Action,
			Confidence:    a.Confidence,
			ConfidenceNA:  confidenceNA,
			ReconID:       a.Provenance.ReconID,
			// F-K: surface the break's origin (source label + Blnk upload batch id)
			// from the event provenance so a reviewer can trace its statement/batch.
			Source:    a.Provenance.Source,
			UploadID:  a.Provenance.UploadID,
			Rationale: a.Rationale,
		})
	}

	var buf bytes.Buffer
	if err := s.tmpl.Execute(&buf, data); err != nil {
		log.Printf("hitl: status page — render failed: %v", err)
		s.renderStatusError(c)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// renderStatusError renders the friendly, sanitized HTML error panel for the
// status page when a store query or template render fails (finding M-03). The
// concrete error is logged server-side by the caller; the operator sees only a
// stable, non-disclosing message — never a raw pq:/driver/Go error string that
// would reveal the DB engine, schema, or table names on this unauthenticated
// surface. It reuses errorPageHTML so the status-page and form-submit error
// panels are visually identical.
func (s *Server) renderStatusError(c *gin.Context) {
	c.Data(http.StatusInternalServerError, "text/html; charset=utf-8",
		errorPageHTML("unable to load the review page"))
}

// ruleCriteriaViews flattens a proposed rule's criteria into fully-renderable
// rows for the status page (finding M-22). Each criterion is shown as
// "field operator" plus a Detail qualifier describing any drift tolerance,
// pattern, or literal value. All values are rendered through html/template, so
// escaping is automatic; this helper only formats the human-readable qualifier.
func ruleCriteriaViews(criteria []blnk.MatchingCriteria) []ruleCriterionView {
	if len(criteria) == 0 {
		return nil
	}
	views := make([]ruleCriterionView, 0, len(criteria))
	for _, cr := range criteria {
		cv := ruleCriterionView{Field: cr.Field, Operator: cr.Operator}
		var parts []string
		// A non-zero drift is meaningful for amount/date tolerances; render it at
		// fixed precision so "0" tolerances are not shown as noise.
		if cr.AllowableDrift != 0 {
			parts = append(parts, fmt.Sprintf("drift %.3f", cr.AllowableDrift))
		}
		if strings.TrimSpace(cr.Pattern) != "" {
			parts = append(parts, fmt.Sprintf("pattern %q", cr.Pattern))
		}
		if strings.TrimSpace(cr.Value) != "" {
			parts = append(parts, fmt.Sprintf("value %q", cr.Value))
		}
		cv.Detail = strings.Join(parts, ", ")
		views = append(views, cv)
	}
	return views
}

// statusPageHTML is the html/template source for the status page. Styling is
// intentionally minimal embedded CSS only (AAP 0.5.3).
const statusPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="recon-agent human-in-the-loop review console: reconciliation breaks, proposed matching rules, and the append-only audit trail.">
<link rel="icon" href="data:,">
<title>{{.Title}}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; }
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1a1a1a; }
  main { max-width: 100%; }
  h1 { font-size: 1.4rem; margin-bottom: .2rem; }
  h2 { font-size: 1.1rem; margin-top: 2rem; }
  /* M-22: confine wide tables to a horizontally scrollable region so the PAGE
     itself never overflows the viewport (previously ~450px of body overflow on
     mobile). The <body>/<html> stay at viewport width; only the table scrolls. */
  .table-wrap { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; margin-top: .5rem; }
  /* F20 (WCAG 2.1.1): each horizontally-scrollable table wrapper is a focusable,
     named region (role=region + tabindex=0 + aria-labelledby its section heading),
     so a keyboard-only user can Tab to it and arrow-scroll to reveal off-screen
     columns (e.g. the audit trail's Recon ID / Source / Upload ID / Rationale) at
     narrow widths. A clearly visible focus ring marks the focused region. */
  .table-wrap:focus { outline: 3px solid #0b5cad; outline-offset: 2px; }
  .table-wrap:focus-visible { outline: 3px solid #0b5cad; outline-offset: 2px; }
  table { border-collapse: collapse; width: 100%; }
  caption { text-align: left; font-weight: 600; padding: .3rem 0; }
  th, td { border: 1px solid #ccc; padding: .4rem .6rem; text-align: left; font-size: .9rem; vertical-align: top; }
  th { background: #f2f2f2; }
  .regulated { color: #b00020; font-weight: 600; }
  .status-auto-resolved { color: #0a7d28; font-weight: 600; }
  .status-rejected { color: #b00020; font-weight: 600; }
  .status-re_driven { color: #0b5cad; font-weight: 600; }
  .status-queued { color: #8a5a00; font-weight: 600; }
  .status-accepted { color: #0a7d28; font-weight: 600; }
  /* F19: the transient 'classified' lifecycle state (StatusClassified) — a break
     the classifier has labeled but that has not yet been auto-remediated or
     escalated — MUST receive the SAME explicit semantic treatment (status color +
     weight 600) as every other status, rather than falling back to ordinary body
     text. A muted indigo distinguishes this in-progress state from the terminal
     green/red/blue/amber statuses while meeting WCAG AA contrast on white. */
  .status-classified { color: #5a4b8b; font-weight: 600; }
  .rationale { display: block; margin-top: .2rem; }
  .criteria { margin: .2rem 0 0; padding-left: 1.1rem; }
  .criteria li { font-size: .85rem; }
  /* M-22: the decision form lays its controls out with wrapping so they never
     force horizontal overflow, and every interactive target meets the WCAG AA
     minimum (>=24px; we use 44px, the preferred size). */
  .decision-form { display: flex; flex-wrap: wrap; gap: .4rem; align-items: center; margin: 0; }
  .reviewer-input { min-height: 44px; padding: .4rem .6rem; font-size: .9rem; min-width: 8rem; }
  .decision-form button { min-height: 44px; min-width: 44px; padding: .5rem .75rem; cursor: pointer; font-size: .9rem; }
  .no-action { color: #666; font-size: .85rem; }
  .muted { color: #666; font-size: .8rem; }
  /* F16: the confirming dry-run reconciliation id (clearance proof) rendered in
     a monospace face so the identifier is easy to read/compare next to a cleared break. */
  .proof { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .8rem; color: #0a7d28; word-break: break-all; }
  /* F-A: flag a below-threshold confidence with an explicit textual tag so it
     can never be misread as auto-eligible on this human safety surface. */
  .thresh-tag { color: #8a4b00; font-size: .75rem; font-weight: 600; white-space: nowrap; }
  /* F-M: the optional note input sits alongside the reviewer id on the form. */
  .note-input { min-height: 44px; padding: .4rem .6rem; font-size: .9rem; min-width: 10rem; }
  /* F06: bounded pagination controls for the breaks table. The page is
     server-rendered with no client JS (strict CSP), so traversal is plain
     <a> links that carry the chosen offset+limit; the total and 1-based
     From/To are shown so every break (including queued rows past the first
     page) is discoverable. Links are ≥44px tall for a comfortable tap target. */
  .pagination { display: flex; flex-wrap: wrap; align-items: center; gap: .75rem; margin: .5rem 0 1rem; }
  .page-link { display: inline-block; min-height: 44px; line-height: 44px; padding: 0 .9rem; border: 1px solid #ccc; border-radius: 4px; text-decoration: none; color: #0a4b8a; font-size: .85rem; }
  .page-link:hover, .page-link:focus { background: #eef4fb; }
  /* M-22: small-screen reflow — reduce chrome so the content fits a phone. */
  @media (max-width: 640px) {
    body { margin: .75rem; }
    h1 { font-size: 1.2rem; }
    th, td { padding: .35rem .45rem; }
    .reviewer-input { min-width: 100%; }
    .note-input { min-width: 100%; }
    .decision-form button { flex: 1 1 auto; }
  }
</style>
</head>
<body>
  <main>
  <h1>{{.Title}}</h1>
  <p class="muted">LLM model: {{.Model}} &middot; auto-remediation threshold: {{printf "%.4f" .AutoThreshold}}</p>

  <h2 id="breaks-heading">Breaks</h2>
  <div class="table-wrap" tabindex="0" role="region" aria-labelledby="breaks-heading">
  <table aria-labelledby="breaks-heading">
    <caption>External breaks under management, their classification, the proposed matching rule, and the available human decision.</caption>
    <thead>
      <tr>
        <th scope="col">External Txn ID</th>
        <th scope="col">Root Cause</th>
        <th scope="col">Confidence</th>
        <th scope="col">Regulated</th>
        <th scope="col">Status</th>
        <th scope="col">Proof Recon ID</th>
        <th scope="col">Reason</th>
        <th scope="col">Proposed Rule</th>
        <th scope="col">Decision</th>
      </tr>
    </thead>
    <tbody>
    {{range .Breaks}}
      <tr>
        <td>{{.ExternalTxnID}}</td>
        <td>
          {{.RootCause}}
          {{if .Rationale}}<span class="rationale muted">{{.Rationale}}</span>{{end}}
        </td>
        <td>{{printf "%.4f" .Confidence}}{{if .BelowThreshold}} <span class="thresh-tag" title="below the auto-remediation threshold">below auto-threshold</span>{{end}}</td>
        <td>{{if .Regulated}}<span class="regulated">yes</span>{{else}}no{{end}}</td>
        <td class="status-{{.Status}}">{{.Status}}</td>
        <td>{{if .ResolvedReconID}}<span class="proof" title="confirming Blnk dry-run reconciliation id">{{.ResolvedReconID}}</span>{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{if .Reason}}{{.Reason}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>
          {{if .HasRule}}
            {{if .RuleName}}{{.RuleName}}{{else}}<span class="muted">(unnamed rule)</span>{{end}}
            {{if .RuleCriteria}}
            <ul class="criteria">
              {{range .RuleCriteria}}<li>{{.Field}} {{.Operator}}{{if .Detail}} &mdash; {{.Detail}}{{end}}</li>{{end}}
            </ul>
            {{end}}
          {{else}}<span class="muted">&mdash;</span>{{end}}
        </td>
        <td>
          {{if .Queued}}
          <form class="decision-form" method="post" action="/decisions">
            <input type="hidden" name="external_txn_id" value="{{.ExternalTxnID}}">
            <input type="hidden" name="csrf_token" value="{{$.CSRFToken}}">
            <input class="reviewer-input" type="text" name="reviewer" placeholder="reviewer id" required maxlength="{{$.ReviewerMaxLen}}" autocomplete="off" aria-label="reviewer id for break {{.ExternalTxnID}}">
            <input class="note-input" type="text" name="note" placeholder="note (optional)" maxlength="{{$.NoteMaxLen}}" autocomplete="off" aria-label="optional note for break {{.ExternalTxnID}}">
            <button type="submit" name="decision" value="accept" aria-label="accept break {{.ExternalTxnID}}">accept</button>
            <button type="submit" name="decision" value="re_drive" aria-label="re_drive break {{.ExternalTxnID}}">re_drive</button>
            <button type="submit" name="decision" value="reject" aria-label="reject break {{.ExternalTxnID}}">reject</button>
          </form>
          {{else}}
          <span class="no-action">no action &mdash; {{.Status}}</span>
          {{end}}
        </td>
      </tr>
    {{else}}
      <tr><td colspan="9" class="muted">No breaks under management.</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
  {{if .BreaksNav.Total}}
  <nav class="pagination" aria-label="Breaks pagination">
    <span class="muted">Showing {{.BreaksNav.From}}&ndash;{{.BreaksNav.To}} of {{.BreaksNav.Total}} breaks</span>
    {{if .BreaksNav.HasPrev}}<a class="page-link" href="/?offset={{.BreaksNav.PrevOffset}}&amp;limit={{.BreaksNav.Limit}}" rel="prev" aria-label="previous page of breaks">&larr; Prev</a>{{end}}
    {{if .BreaksNav.HasNext}}<a class="page-link" href="/?offset={{.BreaksNav.NextOffset}}&amp;limit={{.BreaksNav.Limit}}" rel="next" aria-label="next page of breaks">Next &rarr;</a>{{end}}
  </nav>
  {{end}}

  <h2 id="audit-heading">Audit trail</h2>
  <div class="table-wrap" tabindex="0" role="region" aria-labelledby="audit-heading">
  <table aria-labelledby="audit-heading">
    <caption>Append-only record of every action taken on each break, in commit order.</caption>
    <thead>
      <tr>
        <th scope="col">Timestamp</th>
        <th scope="col">External Txn ID</th>
        <th scope="col">Actor</th>
        <th scope="col">Action</th>
        <th scope="col">Confidence</th>
        <th scope="col">Recon ID</th>
        <th scope="col">Source</th>
        <th scope="col">Upload ID</th>
        <th scope="col">Rationale</th>
      </tr>
    </thead>
    <tbody>
    {{range .Audits}}
      <tr>
        <td>{{.Timestamp}}</td>
        <td>{{.ExternalTxnID}}</td>
        <td>{{.Actor}}</td>
        <td>{{.Action}}</td>
        <td>{{if .ConfidenceNA}}<span class="muted" title="not applicable for a human decision">&mdash;</span>{{else}}{{printf "%.4f" .Confidence}}{{end}}</td>
        <td>{{if .ReconID}}{{.ReconID}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{if .Source}}{{.Source}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{if .UploadID}}{{.UploadID}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{.Rationale}}</td>
      </tr>
    {{else}}
      <tr><td colspan="9" class="muted">No audit events recorded yet.</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
  </main>
</body>
</html>
`
