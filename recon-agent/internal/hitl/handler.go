package hitl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Break statuses relevant to HITL decisions. statusQueued is the ONLY decidable
// state: accept/re_drive/reject transition a queued break, and every other
// status (accepted / rejected / re_driven / auto-resolved) is terminal and
// rejected as a conflict (finding C-04).
const (
	statusQueued   = "queued"
	statusAccepted = "accepted"
	statusReDriven = "re_driven"
	statusRejected = "rejected"
)

// errNoReDriveContext is returned when a queued break lacks the durable context
// a contract-valid Blnk dry-run requires (finding C-03): the full external
// transaction and at least one applicable matching-rule id. Rather than submit
// an id-only, rule-less probe that Blnk's start-instant rejects, re_drive fails
// closed and the handler maps this to 422 Unprocessable Entity.
var errNoReDriveContext = errors.New("hitl: break has no durable re-drive context (missing transaction or applicable rule)")

// errProbeFailed wraps a Blnk dry-run (ProbeBreak) failure during re_drive. Per
// the fail-closed rule (Rule 5.7) a probe error causes NO state change and NO
// audit event; the handler maps it to 502 Bad Gateway so the operator can retry
// once Blnk is reachable, and the break stays queued and re-drivable.
var errProbeFailed = errors.New("hitl: blnk dry-run probe failed")

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
		s.respondDecisionError(c, handleErr)
		return
	}

	s.respondDecision(c, d)
}

// respondDecisionError maps a decision handler error to the HTTP status that
// tells the operator precisely what happened, and — critically — guarantees that
// the failure path never reports success for an unmutated break (finding C-04):
//   - ErrNotFound       -> 404: no such break;
//   - ErrConflict       -> 409: the break is no longer queued (already decided,
//     re-driven, or auto-resolved) or the decision was replayed — a terminal or
//     duplicate transition is refused, not applied;
//   - errNoReDriveContext -> 422: a queued break cannot be re-driven because its
//     durable transaction / applicable rule set is missing (fail-closed, C-03);
//   - errProbeFailed    -> 502: the Blnk dry-run was unreachable/failed, so the
//     break stayed queued (fail-closed, Rule 5.7);
//   - anything else      -> 500.
//
// Because every mutating handler runs in a single WithTx, a non-nil error here
// means the transaction rolled back and NO partial state (status, queue drain,
// or audit event) was committed.
func (s *Server) respondDecisionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, store.ErrConflict):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, errNoReDriveContext):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
	case errors.Is(err, errProbeFailed):
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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

// accept settles a queued break as resolved-by-human. Its status change, its
// removal from the HITL queue, and its `accepted` audit event all commit inside
// a single transaction (finding C-04): either the break moves queued->accepted,
// is drained, and the audit row lands together, or nothing does. The queued-only
// compare-and-set guarantees a terminal break (already decided/re-driven/
// auto-resolved) is refused with ErrConflict rather than overwritten, which also
// makes a replayed accept idempotent — the second attempt matches no queued row
// and returns 409.
func (s *Server) accept(ctx context.Context, d model.HITLDecision) error {
	return s.settle(ctx, d, statusAccepted, model.Provenance{Model: s.llmModel})
}

// reject settles a queued break as closed-by-human. Like accept, its status
// change, queue drain, and `rejected` audit event commit atomically under a
// queued-only CAS, so a terminal or replayed reject is refused (409), never
// applied (finding C-04).
func (s *Server) reject(ctx context.Context, d model.HITLDecision) error {
	return s.settle(ctx, d, statusRejected, model.Provenance{Model: s.llmModel})
}

// settle is the shared accept/reject core: a single transaction that (1)
// compare-and-sets the break from queued to the terminal status, (2) drains it
// from the HITL queue, and (3) appends the decision's audit event — all or
// nothing (finding C-04). SetBreakStatusIfQueuedTx returns ErrNotFound when no
// such break exists and ErrConflict when it is no longer queued, so terminal and
// replayed decisions never mutate settled state; any error rolls the whole
// transaction back, leaving no partial outcome.
func (s *Server) settle(ctx context.Context, d model.HITLDecision, terminalStatus string, prov model.Provenance) error {
	return s.st.WithTx(ctx, func(tx *sql.Tx) error {
		if err := s.st.SetBreakStatusIfQueuedTx(ctx, tx, d.ExternalTxnID, terminalStatus); err != nil {
			return err
		}
		if err := s.st.DequeueHITLTx(ctx, tx, d.ExternalTxnID); err != nil {
			return err
		}
		return s.aud.RecordTx(ctx, tx, audit.Decision(d, prov))
	})
}

// reDrive re-tests a queued break against Blnk (the deterministic arbiter,
// Rule 5.3) and settles it only if Blnk confirms clearance. It first loads the
// break's durable re-drive context (finding C-03) — the FULL external
// transaction and the applicable matching-rule id set — so the dry-run probe is
// a contract-valid start-instant payload rather than an id-only, rule-less call.
//
// Pre-transaction guards (no state mutated on any of these paths):
//   - break missing            -> ErrNotFound (404);
//   - break not queued          -> ErrConflict (409): a terminal break is not
//     re-drivable, and a replayed re_drive against a settled break is refused;
//   - no transaction / no rule  -> errNoReDriveContext (422): fail closed rather
//     than probe Blnk with an invalid rule-less request (C-03);
//   - probe error/timeout       -> errProbeFailed (502): fail closed (Rule 5.7),
//     the break stays queued and re-drivable.
//
// Then, inside one transaction (finding C-04):
//   - cleared:     queued->re_driven via the queued-only CAS + queue drain, so a
//     racing decision cannot be clobbered;
//   - not cleared: the break stays queued (still re-drivable); RequireQueuedTx
//     re-confirms it is queued under a row lock before the audit event is
//     appended, so no re-drive is recorded against a break another decision
//     settled between the probe and this commit.
//
// In both branches exactly one `re_driven` audit event (carrying the dry-run's
// recon_id, Rule 5.3) commits atomically with the status/queue effect.
func (s *Server) reDrive(ctx context.Context, d model.HITLDecision) error {
	status, txn, ruleIDs, found, err := s.st.LoadBreakContext(ctx, d.ExternalTxnID)
	if err != nil {
		return err
	}
	if !found {
		return store.ErrNotFound
	}
	if status != statusQueued {
		return store.ErrConflict
	}
	if txn == nil || len(ruleIDs) == 0 {
		return errNoReDriveContext
	}

	cleared, reconID, err := s.bc.ProbeBreak(ctx, *txn, ruleIDs)
	if err != nil {
		return fmt.Errorf("%w: %v", errProbeFailed, err)
	}

	prov := model.Provenance{Model: s.llmModel, ReconID: reconID}
	return s.st.WithTx(ctx, func(tx *sql.Tx) error {
		if cleared {
			if err := s.st.SetBreakStatusIfQueuedTx(ctx, tx, d.ExternalTxnID, statusReDriven); err != nil {
				return err
			}
			if err := s.st.DequeueHITLTx(ctx, tx, d.ExternalTxnID); err != nil {
				return err
			}
		} else if err := s.st.RequireQueuedTx(ctx, tx, d.ExternalTxnID); err != nil {
			return err
		}
		return s.aud.RecordTx(ctx, tx, audit.Decision(d, prov))
	})
}
