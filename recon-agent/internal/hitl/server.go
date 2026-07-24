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
	"database/sql"
	"errors"
	"html/template"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

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
	// LoadBreak returns the persisted classification, current status, and any
	// created rule id for a break. re_drive uses the status to reject a missing
	// break (404) or an already-decided break (409) BEFORE any Blnk call
	// (findings m-01/M-01).
	LoadBreak(ctx context.Context, externalTxnID string) (c model.BreakClassification, status, createdRuleID string, found bool, err error)
	// LoadBreakTxn returns the full external transaction (and any Blnk matching
	// rule id previously created for it) persisted for a queued break. re_drive
	// needs the matchable fields — not just the id — so its clearance probe
	// carries a real transaction (finding L2).
	LoadBreakTxn(ctx context.Context, externalTxnID string) (txn blnk.ExternalTransaction, createdRuleID string, found bool, err error)
	// WithTx runs fn inside a single transaction so a HITL decision's guarded
	// status change, queue drain, and audit append commit or roll back together
	// (finding M-02).
	WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error
	// SetBreakStatusFromQueuedTx is the queued-only compare-and-set that
	// transitions a break out of the queued state, returning store.ErrNotQueued
	// for an already-decided break and store.ErrNotFound for a missing one so a
	// decision never silently overwrites a terminal status (finding M-01).
	SetBreakStatusFromQueuedTx(ctx context.Context, tx *sql.Tx, externalTxnID, status string) error
	// DequeueHITLTx drains a break from the review queue inside the caller's
	// transaction (finding M-02).
	DequeueHITLTx(ctx context.Context, tx *sql.Tx, externalTxnID string) error
	// Ping verifies the persistence layer is reachable; /healthz and /readyz use
	// it to reflect real dependency health rather than reporting a fixed value
	// (findings M3/m-02).
	Ping(ctx context.Context) error
}

// recorder is the append-only audit surface (Rule 5.5). *audit.Writer satisfies
// it. RecordTx enrolls the audit append in the caller's transaction so a
// decision's state change and its audit event commit atomically (finding M-02).
type recorder interface {
	Record(ctx context.Context, ev model.AuditEvent) error
	RecordTx(ctx context.Context, tx *sql.Tx, ev model.AuditEvent) error
}

// prober re-tests break clearance via a Blnk dry-run (start-instant, dry_run=true).
// The concrete *blnk.Client satisfies it. Keeping this as an interface both
// enables mocking in tests and avoids importing remediator.
//
// re_drive (finding L2) needs a non-empty matching_rule_ids set for the probe —
// Blnk's start-instant rejects an empty set — so the prober also exposes rule
// creation and deletion. A rule built just to probe is deleted afterwards so no
// orphan rule accumulates in Blnk (finding F5 parity).
type prober interface {
	ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (cleared bool, reconID string, err error)
	CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error)
	DeleteMatchingRule(ctx context.Context, ruleID string) error
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

	// mu guards httpServer, which Run publishes and Shutdown reads, so a signal
	// handler can gracefully drain the server from another goroutine (finding M4).
	mu         sync.Mutex
	httpServer *http.Server
	// ready reflects overall service readiness for /healthz (finding M3). It
	// defaults to true so a freshly constructed server (e.g. the integration
	// test) reports healthy; cmd/main.go flips it via SetReady while serve-mode
	// awaits dependency/pipeline readiness (finding L1).
	ready atomic.Bool
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
	// Default to ready so a directly-constructed server reports healthy on
	// /healthz (finding M3); serve-mode callers gate this via SetReady (L1).
	s.ready.Store(true)
	s.engine = s.buildRouter()
	return s
}

// buildRouter registers all HITL routes. No auth middleware is applied (AAP 0.5.3).
func (s *Server) buildRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", s.healthz)
	r.GET("/readyz", s.readyz)
	r.GET("/", s.statusPage)
	r.GET("/breaks", s.listBreaks)
	r.POST("/decisions", s.handleDecision)
	return r
}

// Router returns the configured gin engine (used by cmd/main.go and tests).
func (s *Server) Router() *gin.Engine { return s.engine }

// Run serves the HITL API + status page on addr, blocking until the server is
// shut down (via Shutdown) or a listen error occurs. Unlike gin's Engine.Run,
// it is backed by an *http.Server so cmd/main.go can drain it gracefully on
// SIGINT/SIGTERM (finding M4). A clean shutdown returns nil, not
// http.ErrServerClosed.
func (s *Server) Run(addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.engine}
	s.mu.Lock()
	s.httpServer = srv
	s.mu.Unlock()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully drains in-flight requests and stops the HTTP server
// within ctx's deadline (finding M4). It is safe to call from a different
// goroutine than Run, and is a no-op if Run has not started the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpServer
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// SetReady toggles the readiness flag reported by /healthz (findings M3/L1).
// cmd/main.go marks the service not-ready until its dependencies are reachable
// and (in serve mode) the pipeline has completed.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// healthz is a combined liveness+readiness probe (finding M3). It returns:
//   - 503 not_ready   — the service has been marked not-ready (deps/pipeline), or
//   - 503 unhealthy   — the persistence layer is unreachable (Ping failed), or
//   - 200 ok          — ready and the store is reachable.
//
// It still returns 200 while healthy, so callers that only assert reachability
// continue to pass.
func (s *Server) healthz(c *gin.Context) {
	if !s.ready.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
		return
	}
	if err := s.st.Ping(c.Request.Context()); err != nil {
		// M-03: log the concrete dependency error server-side; never return the
		// raw pq:/driver string to the client.
		log.Printf("hitl: healthz ping failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz is a dedicated READINESS probe (finding m-02). It reports 200
// {"status":"ready"} only when the service has been marked ready AND the
// dependency every decision requires — the agent database — is reachable via
// store.Ping; otherwise 503 {"status":"not ready"}. The concrete error is
// logged server-side, never returned to the client (M-03).
//
// Blnk reachability is deliberately NOT probed here: the agent may reach Blnk
// only through /reconciliation/* routes (Rule 5.1), and re_drive already fails
// closed if Blnk is unavailable at decision time (Rule 5.7). Accept/reject do
// not touch Blnk at all, so DB readiness is the meaningful signal for the
// decision surface as a whole.
func (s *Server) readyz(c *gin.Context) {
	if !s.ready.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}
	if err := s.st.Ping(c.Request.Context()); err != nil {
		log.Printf("hitl: readiness check failed: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// listBreaks returns the machine-readable JSON view of all breaks under
// management. On a store failure it logs the concrete error server-side and
// returns a stable, sanitized message — never the raw pq:/driver string (M-03).
func (s *Server) listBreaks(c *gin.Context) {
	breaks, err := s.st.ListBreaks(c.Request.Context())
	if err != nil {
		log.Printf("hitl: list breaks failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"breaks": breaks})
}
