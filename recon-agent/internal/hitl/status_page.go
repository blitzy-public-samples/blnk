package hitl

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

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
}

// auditView is a single append-only audit row rendered on the status page.
type auditView struct {
	Timestamp     string
	ExternalTxnID string
	Actor         string
	Action        string
	Confidence    float64
	ReconID       string
	Rationale     string
}

// pageData is the view model handed to statusTemplate.
type pageData struct {
	Title string
	Model string
	// CSRFToken is the per-render double-submit CSRF token embedded as a hidden
	// field in every decision form; it must equal the csrf_token cookie the page
	// set for the POST to be accepted by the mutation guard (finding C-05).
	CSRFToken string
	Breaks    []breakView
	Audits    []auditView
}

// statusPage renders the operator/demo status page: the breaks table (with the
// proposed MatchingRule when present) and the append-only audit trail. Simple
// <form> controls on each row POST an accept/re_drive/reject decision.
func (s *Server) statusPage(c *gin.Context) {
	ctx := c.Request.Context()
	data := pageData{Title: "recon-agent — HITL review", Model: s.llmModel}

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

	breaks, err := s.st.ListBreaks(ctx)
	if err != nil {
		log.Printf("hitl: status page — list breaks failed: %v", err)
		s.renderStatusError(c)
		return
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
		data.Audits = append(data.Audits, auditView{
			Timestamp:     a.Timestamp,
			ExternalTxnID: a.ExternalTxnID,
			Actor:         a.Actor,
			Action:        a.Action,
			Confidence:    a.Confidence,
			ReconID:       a.Provenance.ReconID,
			Rationale:     a.Rationale,
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
  table { border-collapse: collapse; width: 100%; }
  caption { text-align: left; font-weight: 600; padding: .3rem 0; }
  th, td { border: 1px solid #ccc; padding: .4rem .6rem; text-align: left; font-size: .9rem; vertical-align: top; }
  th { background: #f2f2f2; }
  .regulated { color: #b00020; font-weight: 600; }
  .status-auto-resolved { color: #0a7d28; font-weight: 600; }
  .status-rejected { color: #b00020; }
  .status-re_driven { color: #0b5cad; }
  .status-queued { color: #8a5a00; font-weight: 600; }
  .status-accepted { color: #0a7d28; font-weight: 600; }
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
  /* M-22: small-screen reflow — reduce chrome so the content fits a phone. */
  @media (max-width: 640px) {
    body { margin: .75rem; }
    h1 { font-size: 1.2rem; }
    th, td { padding: .35rem .45rem; }
    .reviewer-input { min-width: 100%; }
    .decision-form button { flex: 1 1 auto; }
  }
</style>
</head>
<body>
  <main>
  <h1>{{.Title}}</h1>
  <p class="muted">LLM model: {{.Model}}</p>

  <h2 id="breaks-heading">Breaks</h2>
  <div class="table-wrap">
  <table aria-labelledby="breaks-heading">
    <caption>External breaks under management, their classification, the proposed matching rule, and the available human decision.</caption>
    <thead>
      <tr>
        <th scope="col">External Txn ID</th>
        <th scope="col">Root Cause</th>
        <th scope="col">Confidence</th>
        <th scope="col">Regulated</th>
        <th scope="col">Status</th>
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
        <td>{{printf "%.4f" .Confidence}}</td>
        <td>{{if .Regulated}}<span class="regulated">yes</span>{{else}}no{{end}}</td>
        <td class="status-{{.Status}}">{{.Status}}</td>
        <td>
          {{if .HasRule}}
            {{.RuleName}}
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
            <input class="reviewer-input" type="text" name="reviewer" placeholder="reviewer id" required maxlength="128" autocomplete="off" aria-label="reviewer id for break {{.ExternalTxnID}}">
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
      <tr><td colspan="7" class="muted">No breaks under management.</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>

  <h2 id="audit-heading">Audit trail</h2>
  <div class="table-wrap">
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
        <td>{{printf "%.4f" .Confidence}}</td>
        <td>{{if .ReconID}}{{.ReconID}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{.Rationale}}</td>
      </tr>
    {{else}}
      <tr><td colspan="7" class="muted">No audit events recorded yet.</td></tr>
    {{end}}
    </tbody>
  </table>
  </div>
  </main>
</body>
</html>
`
