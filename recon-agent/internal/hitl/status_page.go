package hitl

import (
	"bytes"
	"html/template"
	"net/http"

	"github.com/gin-gonic/gin"
)

// statusTemplate is the single, server-rendered HTML status page for the HITL
// review surface (AAP 0.5.3). It uses only minimal embedded CSS: no component
// library, no CSS framework, and no client-side JS framework are introduced.
var statusTemplate = template.Must(template.New("status").Parse(statusPageHTML))

// breakView is the per-break row rendered on the status page.
type breakView struct {
	ExternalTxnID string
	RootCause     string
	Confidence    float64
	Regulated     bool
	Status        string
	RuleName      string
	HasRule       bool
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
	Title  string
	Model  string
	Breaks []breakView
	Audits []auditView
}

// statusPage renders the operator/demo status page: the breaks table (with the
// proposed MatchingRule when present) and the append-only audit trail. Simple
// <form> controls on each row POST an accept/re_drive/reject decision.
func (s *Server) statusPage(c *gin.Context) {
	ctx := c.Request.Context()
	data := pageData{Title: "recon-agent — HITL review", Model: s.llmModel}

	breaks, err := s.st.ListBreaks(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to load breaks: %v", err)
		return
	}
	for _, b := range breaks {
		bv := breakView{
			ExternalTxnID: b.Classification.ExternalTxnID,
			RootCause:     string(b.Classification.RootCause),
			Confidence:    b.Classification.Confidence,
			Regulated:     b.Classification.Regulated,
			Status:        b.Status,
		}
		if b.Classification.ProposedRule != nil {
			bv.HasRule = true
			bv.RuleName = b.Classification.ProposedRule.Name
		}
		data.Breaks = append(data.Breaks, bv)
	}

	audits, err := s.st.ListAudit(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to load audit trail: %v", err)
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
		c.String(http.StatusInternalServerError, "failed to render status page: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// statusPageHTML is the html/template source for the status page. Styling is
// intentionally minimal embedded CSS only (AAP 0.5.3).
const statusPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1a1a1a; }
  h1 { font-size: 1.4rem; margin-bottom: .2rem; }
  h2 { font-size: 1.1rem; margin-top: 2rem; }
  table { border-collapse: collapse; width: 100%; margin-top: .5rem; }
  th, td { border: 1px solid #ccc; padding: .4rem .6rem; text-align: left; font-size: .9rem; vertical-align: top; }
  th { background: #f2f2f2; }
  .regulated { color: #b00020; font-weight: 600; }
  .status-auto-resolved { color: #0a7d28; font-weight: 600; }
  .status-rejected { color: #b00020; }
  .status-re_driven { color: #0b5cad; }
  form { display: inline; margin: 0; }
  button { margin: 0 .1rem; cursor: pointer; }
  .muted { color: #666; font-size: .8rem; }
</style>
</head>
<body>
  <h1>{{.Title}}</h1>
  <p class="muted">LLM model: {{.Model}}</p>

  <h2>Breaks</h2>
  <table>
    <thead>
      <tr>
        <th>External Txn ID</th>
        <th>Root Cause</th>
        <th>Confidence</th>
        <th>Regulated</th>
        <th>Status</th>
        <th>Proposed Rule</th>
        <th>Decision</th>
      </tr>
    </thead>
    <tbody>
    {{range .Breaks}}
      <tr>
        <td>{{.ExternalTxnID}}</td>
        <td>{{.RootCause}}</td>
        <td>{{printf "%.2f" .Confidence}}</td>
        <td>{{if .Regulated}}<span class="regulated">yes</span>{{else}}no{{end}}</td>
        <td class="status-{{.Status}}">{{.Status}}</td>
        <td>{{if .HasRule}}{{.RuleName}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>
          <form method="post" action="/decisions">
            <input type="hidden" name="external_txn_id" value="{{.ExternalTxnID}}">
            <input type="hidden" name="reviewer" value="operator">
            <button type="submit" name="decision" value="accept">accept</button>
            <button type="submit" name="decision" value="re_drive">re_drive</button>
            <button type="submit" name="decision" value="reject">reject</button>
          </form>
        </td>
      </tr>
    {{else}}
      <tr><td colspan="7" class="muted">No breaks under management.</td></tr>
    {{end}}
    </tbody>
  </table>

  <h2>Audit trail</h2>
  <table>
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>External Txn ID</th>
        <th>Actor</th>
        <th>Action</th>
        <th>Confidence</th>
        <th>Recon ID</th>
        <th>Rationale</th>
      </tr>
    </thead>
    <tbody>
    {{range .Audits}}
      <tr>
        <td>{{.Timestamp}}</td>
        <td>{{.ExternalTxnID}}</td>
        <td>{{.Actor}}</td>
        <td>{{.Action}}</td>
        <td>{{printf "%.2f" .Confidence}}</td>
        <td>{{if .ReconID}}{{.ReconID}}{{else}}<span class="muted">&mdash;</span>{{end}}</td>
        <td>{{.Rationale}}</td>
      </tr>
    {{else}}
      <tr><td colspan="7" class="muted">No audit events recorded yet.</td></tr>
    {{end}}
    </tbody>
  </table>
</body>
</html>
`
