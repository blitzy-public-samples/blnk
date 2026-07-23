// Package hitl — status page.
//
// This file provides the single server-rendered HTML status page referenced by
// server.go: the package-level statusTemplate and the (*Server).statusPage
// handler bound to GET "/". Per AAP 0.5.3 the page is a minimal operator/demo
// surface with embedded CSS only — no component library, CSS framework, or
// client-side JS framework is introduced, and there is no auth/RBAC. It reads
// only the recon-agent's own persistence and never touches any blnk.* table.
package hitl

import (
	"html/template"
	"net/http"

	"github.com/gin-gonic/gin"
)

// statusTemplate is the compiled single status page. html/template auto-escapes
// all interpolated values, so break and audit text render safely.
var statusTemplate = template.Must(template.New("status").Parse(statusPageHTML))

// statusPageHTML is the operator/demo status page: a table of breaks under
// management (external txn id, root cause, confidence, regulated flag, current
// status, and the proposed matching rule when present) with inline
// accept / re_drive / reject controls that POST to /decisions, followed by the
// append-only audit trail.
const statusPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>recon-agent — HITL review</title>
<style>
  body { font-family: system-ui, Arial, sans-serif; margin: 1.5rem; color: #1a1a1a; }
  h1 { font-size: 1.3rem; } h2 { font-size: 1.05rem; margin-top: 1.75rem; }
  .muted { color: #666; font-size: 0.85rem; }
  table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }
  th, td { border: 1px solid #d0d0d0; padding: 0.35rem 0.5rem; text-align: left; font-size: 0.85rem; vertical-align: top; }
  th { background: #f4f4f4; }
  .reg { color: #b00020; font-weight: 600; }
  .status { font-weight: 600; }
  form.decision { display: inline; }
  form.decision button { margin-right: 0.15rem; }
  input[name=reviewer] { width: 7rem; }
  code { background: #f4f4f4; padding: 0 0.2rem; }
</style>
</head>
<body>
<h1>recon-agent — human-in-the-loop review</h1>
<p class="muted">Inference model: <code>{{.Model}}</code>. Blnk is the deterministic arbiter; re_drive re-tests clearance via a Blnk dry-run.</p>

<h2>Breaks ({{len .Breaks}})</h2>
<table>
  <thead>
    <tr><th>External txn</th><th>Root cause</th><th>Confidence</th><th>Regulated</th><th>Status</th><th>Proposed rule</th><th>Decision</th></tr>
  </thead>
  <tbody>
  {{range .Breaks}}
    <tr>
      <td>{{.Classification.ExternalTxnID}}</td>
      <td>{{.Classification.RootCause}}</td>
      <td>{{printf "%.2f" .Classification.Confidence}}</td>
      <td>{{if .Classification.Regulated}}<span class="reg">yes</span>{{else}}no{{end}}</td>
      <td class="status">{{.Status}}</td>
      <td>
        {{with .Classification.ProposedRule}}
          <code>{{.RuleID}}</code>{{if .Criteria}} ({{(index .Criteria 0).Field}} {{(index .Criteria 0).Operator}}){{end}}
        {{else}}<span class="muted">—</span>{{end}}
      </td>
      <td>
        <form class="decision" method="post" action="/decisions">
          <input type="hidden" name="external_txn_id" value="{{.Classification.ExternalTxnID}}">
          <input type="text" name="reviewer" placeholder="reviewer">
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

<h2>Audit trail ({{len .Audit}})</h2>
<table>
  <thead>
    <tr><th>Timestamp</th><th>Action</th><th>External txn</th><th>Actor</th><th>Confidence</th><th>Rationale</th></tr>
  </thead>
  <tbody>
  {{range .Audit}}
    <tr>
      <td>{{.Timestamp}}</td>
      <td>{{.Action}}</td>
      <td>{{.ExternalTxnID}}</td>
      <td>{{.Actor}}</td>
      <td>{{printf "%.2f" .Confidence}}</td>
      <td>{{.Rationale}}</td>
    </tr>
  {{else}}
    <tr><td colspan="6" class="muted">No audit events yet.</td></tr>
  {{end}}
  </tbody>
</table>
</body>
</html>`

// statusPage renders the single server-rendered HTML status page (GET "/"). It
// reads the current breaks and the append-only audit trail from the store and
// renders them through statusTemplate. It is read-only and performs no state
// mutation.
func (s *Server) statusPage(c *gin.Context) {
	ctx := c.Request.Context()
	breaks, err := s.st.ListBreaks(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, "list breaks: %v", err)
		return
	}
	events, err := s.st.ListAudit(ctx)
	if err != nil {
		c.String(http.StatusInternalServerError, "list audit: %v", err)
		return
	}
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	if err := s.tmpl.Execute(c.Writer, gin.H{
		"Breaks": breaks,
		"Audit":  events,
		"Model":  s.llmModel,
	}); err != nil {
		c.String(http.StatusInternalServerError, "render status page: %v", err)
	}
}
