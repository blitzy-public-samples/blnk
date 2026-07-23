// Package hitl implements the human-in-the-loop (HITL) review surface for the
// recon-agent sidecar: a minimal Gin HTTP server exposing the review queue and a
// single server-rendered HTML status page. Every human decision writes an
// append-only AuditEvent (Gate 13). There is no auth/RBAC — this is a local
// operator/demo surface (AAP 0.5.3).
//
// Rule 5.1 (native-API-only) is honored: this package imports only sibling
// modules (config, model, store, audit, blnk) plus gin and the standard library.
// It never imports any Blnk internal package, and it never imports remediator —
// re_drive re-tests clearance directly via blnk.ProbeBreak (the deterministic
// arbiter, Rule 5.3).
package hitl

import (
	"context"
	"html/template"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// breakStore is the read/update surface hitl needs from the persistence layer.
// The concrete *store.Store satisfies it (asserted below); tests inject a mock.
type breakStore interface {
	ListBreaks(ctx context.Context) ([]store.Break, error)
	ListAudit(ctx context.Context) ([]model.AuditEvent, error)
	SetBreakStatus(ctx context.Context, externalTxnID, status string) error
	DequeueHITL(ctx context.Context, externalTxnID string) error
}

// recorder is the append-only audit surface (Rule 5.5). *audit.Writer satisfies it.
type recorder interface {
	Record(ctx context.Context, ev model.AuditEvent) error
}

// prober re-tests break clearance via a Blnk dry-run (start-instant, dry_run=true).
// The concrete *blnk.Client satisfies it. Keeping this as an interface both
// enables mocking in tests and avoids importing remediator.
type prober interface {
	ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (cleared bool, reconID string, err error)
}

// Compile-time guarantees that the concrete sibling types satisfy the consumer
// interfaces, so cmd/main.go can wire them directly (Gate 9 — integration wiring).
var (
	_ breakStore = (*store.Store)(nil)
	_ recorder   = (*audit.Writer)(nil)
	_ prober     = (*blnk.Client)(nil)
)

// defaultReviewer is attributed to a decision when no reviewer is supplied.
const defaultReviewer = "operator"

// Server hosts the HITL review API and the single server-rendered status page.
type Server struct {
	st       breakStore
	aud      recorder
	bc       prober
	llmModel string
	tmpl     *template.Template
	engine   *gin.Engine
}

// NewServer wires the HITL server from config and its collaborators. The
// concrete *store.Store, *audit.Writer, and *blnk.Client all satisfy the
// interface parameters. llmModel (config-driven, Rule 5.6) is recorded on the
// provenance of every decision audit event.
func NewServer(cfg config.Config, st breakStore, aud recorder, bc prober) *Server {
	gin.SetMode(gin.ReleaseMode)
	s := &Server{
		st:       st,
		aud:      aud,
		bc:       bc,
		llmModel: cfg.LLMModel,
		tmpl:     statusTemplate,
	}
	s.engine = s.buildRouter()
	return s
}

// buildRouter registers all HITL routes. No auth middleware is applied (AAP 0.5.3).
func (s *Server) buildRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", s.healthz)
	r.GET("/", s.statusPage)
	r.GET("/breaks", s.listBreaks)
	r.POST("/decisions", s.handleDecision)
	return r
}

// Router returns the configured gin engine (used by cmd/main.go and tests).
func (s *Server) Router() *gin.Engine { return s.engine }

// Run serves the HITL API + status page, blocking until the server stops.
// cmd/main.go invokes it as srv.Run(":" + cfg.HitlPort).
func (s *Server) Run(addr string) error { return s.engine.Run(addr) }

// healthz is a liveness probe.
func (s *Server) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// listBreaks returns the machine-readable JSON view of all breaks under management.
func (s *Server) listBreaks(c *gin.Context) {
	breaks, err := s.st.ListBreaks(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"breaks": breaks})
}
