package hitl

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Break statuses set by human decisions.
const (
	statusAccepted = "accepted"
	statusReDriven = "re_driven"
	statusRejected = "rejected"
)

// handleDecision binds a model.HITLDecision (JSON body or HTML form) and
// dispatches it to the accept/re_drive/reject handlers. Each successful decision
// writes exactly one AuditEvent via the append-only audit writer (Gate 13,
// Rule 5.5). Unknown decisions and blank ids are rejected before any state
// change or audit write.
func (s *Server) handleDecision(c *gin.Context) {
	d, err := bindDecision(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if d.ExternalTxnID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "external_txn_id is required"})
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown decision: " + d.Decision})
		return
	}

	if handleErr != nil {
		if errors.Is(handleErr, store.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": handleErr.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": handleErr.Error()})
		return
	}

	s.respondDecision(c, d)
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

// accept marks the break resolved-by-human, drains it from the HITL queue, and
// records an `accepted` audit event.
func (s *Server) accept(ctx context.Context, d model.HITLDecision) error {
	if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusAccepted); err != nil {
		return err
	}
	if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
		return err
	}
	return s.aud.Record(ctx, audit.Decision(d, model.Provenance{Model: s.llmModel}))
}

// reDrive re-tests clearance by re-invoking a Blnk dry-run probe (the
// deterministic arbiter, Rule 5.3), updates status, drains the queue only when
// Blnk confirms clearance, and records a `re_driven` audit event carrying the
// recon_id. On probe error it fails closed (Rule 5.7): no state change, no audit
// event, error surfaced to the caller.
func (s *Server) reDrive(ctx context.Context, d model.HITLDecision) error {
	cleared, reconID, err := s.bc.ProbeBreak(ctx, blnk.ExternalTransaction{ID: d.ExternalTxnID}, nil)
	if err != nil {
		return err
	}
	if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusReDriven); err != nil {
		return err
	}
	if cleared {
		if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
			return err
		}
	}
	return s.aud.Record(ctx, audit.Decision(d, model.Provenance{Model: s.llmModel, ReconID: reconID}))
}

// reject marks the break closed, drains it from the HITL queue, and records a
// `rejected` audit event.
func (s *Server) reject(ctx context.Context, d model.HITLDecision) error {
	if err := s.st.SetBreakStatus(ctx, d.ExternalTxnID, statusRejected); err != nil {
		return err
	}
	if err := s.st.DequeueHITL(ctx, d.ExternalTxnID); err != nil {
		return err
	}
	return s.aud.Record(ctx, audit.Decision(d, model.Provenance{Model: s.llmModel}))
}
