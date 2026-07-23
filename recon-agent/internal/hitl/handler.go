// Package hitl — decision handler.
//
// This file provides the (*Server).handleDecision handler referenced by
// server.go and bound to POST "/decisions". It applies a human reviewer's
// decision (accept / re_drive / reject) to a queued break. Every decision —
// whatever its verb — writes exactly one append-only AuditEvent (Rule 5.5,
// Gate 13). accept and reject drain the break from the HITL queue; re_drive
// re-tests clearance through a Blnk dry-run (start-instant, dry_run=true) so
// that Blnk, never the agent, decides whether the break actually cleared
// (Rule 5.3). It reaches Blnk only through the injected prober (internal/blnk),
// never a Blnk internal package (Rule 5.1).
package hitl

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// Break lifecycle statuses written by HITL decisions. They align with the
// statuses the remediator produces and the status page renders.
const (
	statusAccepted     = "accepted"
	statusRejected     = "rejected"
	statusReDriven     = "re_driven"
	statusAutoResolved = "auto-resolved"
)

// handleDecision applies a reviewer's accept / re_drive / reject decision to a
// queued break and records exactly one append-only AuditEvent for it.
func (s *Server) handleDecision(c *gin.Context) {
	d := s.bindDecision(c)

	if d.ExternalTxnID == "" {
		s.respondError(c, http.StatusBadRequest, "external_txn_id is required")
		return
	}
	// Reject any verb outside the closed HITL set before mutating state or
	// building an event, so an arbitrary decision can never be recorded.
	if !audit.IsValidDecision(d.Decision) {
		s.respondError(c, http.StatusBadRequest, "decision must be one of: accept, re_drive, reject")
		return
	}
	if d.Reviewer == "" {
		d.Reviewer = defaultReviewer
	}

	ctx := c.Request.Context()
	// llmModel is config-driven (Rule 5.6) and recorded on the decision's provenance.
	prov := model.Provenance{Model: s.llmModel}
	result := gin.H{"external_txn_id": d.ExternalTxnID, "decision": d.Decision, "reviewer": d.Reviewer}

	switch d.Decision {
	case audit.DecisionAccept:
		if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusAccepted); err != nil {
			s.respondError(c, http.StatusInternalServerError, "set status: "+err.Error())
			return
		}
		if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
			s.respondError(c, http.StatusInternalServerError, "dequeue: "+err.Error())
			return
		}
	case audit.DecisionReject:
		if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusRejected); err != nil {
			s.respondError(c, http.StatusInternalServerError, "set status: "+err.Error())
			return
		}
		if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
			s.respondError(c, http.StatusInternalServerError, "dequeue: "+err.Error())
			return
		}
	case audit.DecisionReDrive:
		// Re-test clearance via a Blnk dry-run. Blnk is the sole arbiter of
		// clearance (Rule 5.3); the confirming reconciliation id is recorded on
		// the decision's provenance.
		cleared, reconID, err := s.bc.ProbeBreak(ctx, blnk.ExternalTransaction{ID: d.ExternalTxnID}, nil)
		if err != nil {
			s.respondError(c, http.StatusBadGateway, "re_drive probe failed: "+err.Error())
			return
		}
		prov.ReconID = reconID
		result["cleared"] = cleared
		result["recon_id"] = reconID
		if cleared {
			// Only a Blnk dry-run may clear a break; drain it from the queue.
			if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusAutoResolved); err != nil {
				s.respondError(c, http.StatusInternalServerError, "set status: "+err.Error())
				return
			}
			if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
				s.respondError(c, http.StatusInternalServerError, "dequeue: "+err.Error())
				return
			}
		} else {
			// Still unmatched: keep the break under human review, recording the
			// re_drive attempt in its status.
			if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusReDriven); err != nil {
				s.respondError(c, http.StatusInternalServerError, "set status: "+err.Error())
				return
			}
		}
	}

	// Every decision emits exactly one append-only AuditEvent (Rule 5.5,
	// Gate 13). audit.Decision maps the verb to its past-tense action and
	// records the reviewer as the actor; IsValidDecision above guarantees the
	// verb is recordable.
	if err := s.aud.Record(ctx, audit.Decision(d, prov)); err != nil {
		s.respondError(c, http.StatusInternalServerError, "record decision audit: "+err.Error())
		return
	}

	// Browser form posts are redirected back to the status page; JSON API
	// clients receive a JSON acknowledgement.
	if isJSON(c) {
		result["status"] = "ok"
		c.JSON(http.StatusOK, result)
		return
	}
	c.Redirect(http.StatusSeeOther, "/")
}

// bindDecision reads a HITLDecision from a JSON body or from posted form fields,
// so both the status-page form controls and JSON API clients are supported.
func (s *Server) bindDecision(c *gin.Context) model.HITLDecision {
	var d model.HITLDecision
	if isJSON(c) {
		_ = c.ShouldBindJSON(&d)
		return d
	}
	d.ExternalTxnID = c.PostForm("external_txn_id")
	d.Decision = c.PostForm("decision")
	d.Reviewer = c.PostForm("reviewer")
	d.Note = c.PostForm("note")
	return d
}

// respondError writes an error as JSON for API clients or as plain text for
// browser posts.
func (s *Server) respondError(c *gin.Context, code int, msg string) {
	if isJSON(c) {
		c.JSON(code, gin.H{"error": msg})
		return
	}
	c.String(code, msg)
}

// isJSON reports whether the request carries a JSON body.
func isJSON(c *gin.Context) bool { return c.ContentType() == "application/json" }
