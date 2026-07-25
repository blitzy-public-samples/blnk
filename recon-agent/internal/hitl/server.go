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
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

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
	// ClaimBreak acquires (or renews) a durable, cross-instance processing lease
	// on a break before re_drive performs any external Blnk work (rule creation /
	// dry-run probe), so two concurrent re_drive requests for the same break can
	// never both hit Blnk or race the terminal state change (finding M-15). It
	// returns granted=false with a nil error when another live owner holds the
	// lease, and store.ErrNotFound when the break does not exist.
	ClaimBreak(ctx context.Context, externalTxnID, owner string, ttl time.Duration) (granted bool, err error)
	// ReleaseBreak releases a lease this server holds once re_drive completes (or
	// fails), letting a retry or another instance reclaim the break (finding
	// M-15). A lease left unreleased self-expires after its TTL.
	ReleaseBreak(ctx context.Context, externalTxnID, owner string) error
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

// Request-hardening bounds for the state-changing decision endpoint (findings
// C-05, M-23). They keep the unauthenticated local surface from being abused by
// an oversized, mislabeled, or cross-site browser submission.
const (
	// maxDecisionBodyBytes caps the /decisions request body. A HITL decision is
	// a handful of short fields; 64 KiB is generous while bounding memory and
	// preventing a slurp-the-body abuse of the unauthenticated endpoint.
	maxDecisionBodyBytes = 64 << 10 // 64 KiB

	// csrfCookieName is the double-submit CSRF cookie the status page sets and
	// the mutation guard verifies against the posted form field (finding C-05).
	csrfCookieName = "csrf_token"
	// csrfFieldName is the hidden form field carrying the double-submit token.
	csrfFieldName = "csrf_token"
	// csrfTokenBytes is the entropy of a freshly minted CSRF token.
	csrfTokenBytes = 32
	// csrfCookieMaxAge bounds how long a minted CSRF cookie is offered before the
	// status page mints a fresh one.
	csrfCookieMaxAge = 3600 // seconds (1h)

	// reDriveLeaseTTL bounds how long a re_drive holds the DB processing lease on
	// a break while it performs external Blnk work (finding M-15). It mirrors the
	// remediator's lease TTL; a crashed holder's lease self-expires after it.
	reDriveLeaseTTL = 2 * time.Minute
)

// Server hosts the HITL review API and the single server-rendered status page.
type Server struct {
	st       breakStore
	aud      recorder
	bc       prober
	llmModel string
	tmpl     *template.Template
	engine   *gin.Engine

	// owner uniquely identifies THIS server instance when it claims a
	// per-break processing lease before re_drive's external Blnk work, so the
	// lease is a genuine cross-instance mutual-exclusion primitive rather than a
	// process-local guard (finding M-15).
	owner string

	// mu guards httpServer and listener, which Listen/Run publish and Shutdown
	// reads, so a signal handler can gracefully drain the server from another
	// goroutine (finding M4). listener is the already-bound socket produced by
	// Listen; Serve consumes it. Binding synchronously in Listen — BEFORE any
	// long-running startup work — lets cmd/main.go fail fast on a bind error
	// instead of discovering it asynchronously after the pipeline has run
	// (finding M-16).
	mu         sync.Mutex
	httpServer *http.Server
	listener   net.Listener
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
		// A unique owner identity per instance so re_drive's DB processing lease
		// is a cross-instance mutual-exclusion primitive (finding M-15).
		owner: "recon-agent-hitl-" + uuid.NewString(),
	}
	// Default to ready so a directly-constructed server reports healthy on
	// /healthz (finding M3); serve-mode callers gate this via SetReady (L1).
	s.ready.Store(true)
	s.engine = s.buildRouter()
	return s
}

// buildRouter registers all HITL routes and the request-hardening middleware
// (finding C-05). There is still no authentication/RBAC — that remains out of
// scope for this local operator/demo surface (AAP 0.5.3) — but the surface is no
// longer defenseless against browser-driven abuse:
//
//   - securityHeaders runs on EVERY response and sets a restrictive baseline
//     (nosniff, frame/clickjacking denial via CSP frame-ancestors + legacy
//     X-Frame-Options, an inline-style-only CSP with form-action 'self', a
//     no-referrer policy, and no-store caching) so the page cannot be framed,
//     MIME-sniffed, or have its forms retargeted cross-origin.
//   - mutationGuard runs ONLY on the state-changing POST /decisions and enforces
//     the local boundary and same-origin/CSRF protections the finding requires:
//     a bounded body, an allow-listed content type, an Origin/Referer same-origin
//     check, and a double-submit CSRF token for browser form submissions.
//
// The listener itself is expected to bind to a loopback/local boundary — the
// Compose deployment publishes it as 127.0.0.1:${HITL_PORT} (Phase-scoped
// orchestration wiring) — so the surface is not reachable off-host; the
// same-origin/CSRF controls here defend against a browser on the operator's own
// machine being used as a cross-site confused deputy.
func (s *Server) buildRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(s.securityHeaders())
	r.GET("/healthz", s.healthz)
	r.GET("/readyz", s.readyz)
	r.GET("/", s.statusPage)
	r.GET("/breaks", s.listBreaks)
	// The mutation guard is applied ONLY to the state-changing endpoint so
	// read-only GETs and non-browser API clients are unaffected (finding C-05).
	r.POST("/decisions", s.mutationGuard, s.handleDecision)
	return r
}

// securityHeaders sets a restrictive security-header baseline on every response
// (finding C-05). The Content-Security-Policy permits only same-origin
// resources and the page's own inline <style> (no external or inline scripts,
// no framing), restricts form submissions to the same origin, and disables the
// <base> element; X-Frame-Options mirrors frame-ancestors for legacy browsers.
// Cache-Control: no-store keeps the unauthenticated review data and the CSRF
// token out of shared/browser caches.
func (s *Server) securityHeaders() gin.HandlerFunc {
	const csp = "default-src 'self'; style-src 'self' 'unsafe-inline'; " +
		"script-src 'none'; object-src 'none'; base-uri 'none'; " +
		"form-action 'self'; frame-ancestors 'none'"
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", csp)
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		c.Next()
	}
}

// mutationGuard hardens the unauthenticated state-changing POST /decisions
// endpoint (findings C-05, M-23). In order, it:
//
//  1. Bounds the request body to maxDecisionBodyBytes so a hostile client cannot
//     force an unbounded read on this endpoint.
//  2. Requires an allow-listed content type (application/json or
//     application/x-www-form-urlencoded); anything else is 415. This blocks
//     text/plain form posts a cross-site page could send without a CORS
//     preflight.
//  3. Enforces same-origin: when an Origin (or, as a fallback, Referer) header is
//     present it MUST match this server's host, so a cross-site browser
//     submission is rejected 403. Requests with NEITHER header are non-browser
//     API clients (curl, server-to-server) that carry no ambient cookies and are
//     therefore not a CSRF vector, so they pass — preserving the JSON API.
//  4. For browser FORM submissions, requires a double-submit CSRF token: the
//     posted csrf_token field must equal the csrf_token cookie the status page
//     set (constant-time compare). JSON submissions are exempt from the token
//     (they cannot be forged cross-site without a CORS preflight this server
//     never grants) but still undergo the origin check above.
func (s *Server) mutationGuard(c *gin.Context) {
	// (1) Bound the body before anything reads it.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxDecisionBodyBytes)

	// (2) Allow-list the content type (bare media type, ignoring parameters).
	ct := contentTypeOf(c)
	if ct != "application/json" && ct != "application/x-www-form-urlencoded" {
		c.AbortWithStatusJSON(http.StatusUnsupportedMediaType, gin.H{"error": "unsupported content type"})
		return
	}

	// (3) Same-origin enforcement (present Origin/Referer must match our host).
	if !s.sameOrigin(c) {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "cross-origin request rejected"})
		return
	}

	// (4) Double-submit CSRF token for browser form submissions.
	if ct == "application/x-www-form-urlencoded" {
		if err := c.Request.ParseForm(); err != nil {
			// A MaxBytesReader overflow surfaces here as a parse error.
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request payload too large"})
			return
		}
		cookie, cerr := c.Cookie(csrfCookieName)
		field := c.Request.PostForm.Get(csrfFieldName)
		if cerr != nil || cookie == "" || field == "" ||
			subtle.ConstantTimeCompare([]byte(cookie), []byte(field)) != 1 {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid or missing CSRF token"})
			return
		}
	}

	c.Next()
}

// contentTypeOf returns the bare media type of the request (lower-cased, without
// charset/boundary parameters) so the content-type allow-list matches
// "application/json; charset=utf-8" as "application/json".
func contentTypeOf(c *gin.Context) string {
	ct := c.GetHeader("Content-Type")
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

// sameOrigin reports whether a state-changing request is same-origin. It trusts
// a request with NO Origin and NO Referer (a non-browser API client that cannot
// be a CSRF vector) and otherwise requires the header's host to equal the
// request host. Only the Origin header is used when present; Referer is a
// fallback for the rare browser that omits Origin on same-origin POSTs.
func (s *Server) sameOrigin(c *gin.Context) bool {
	host := c.Request.Host
	if origin := c.GetHeader("Origin"); origin != "" {
		u, err := url.Parse(origin)
		return err == nil && u.Host == host
	}
	if referer := c.GetHeader("Referer"); referer != "" {
		u, err := url.Parse(referer)
		return err == nil && u.Host == host
	}
	// Neither header present: not a browser-originated cross-site request.
	return true
}

// newCSRFToken mints a cryptographically random double-submit CSRF token
// (finding C-05). On the (astronomically unlikely) failure of the system RNG it
// returns an empty string; the caller then does not set a token and browser form
// submissions will be rejected by the guard — fail-closed rather than issuing a
// predictable token.
func newCSRFToken() string {
	b := make([]byte, csrfTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// Router returns the configured gin engine (used by cmd/main.go and tests).
func (s *Server) Router() *gin.Engine { return s.engine }

// Listen SYNCHRONOUSLY binds the HTTP server's listening socket on addr and
// returns any bind error immediately, without yet accepting connections
// (finding M-16). Binding first — before cmd/main.go performs any long-running
// startup work (dependency wait, migration, the reconciliation pipeline) — means
// a fatal, non-retryable condition such as an already-in-use port surfaces
// instantly and deterministically to the caller, instead of racing inside a
// background goroutine and being discovered only after minutes of work. On
// success the bound listener and its backing *http.Server are published under mu
// so Serve can begin accepting and Shutdown can drain gracefully from another
// goroutine (finding M4). Callers that have called Listen MUST subsequently call
// either Serve (to accept connections) or Shutdown (to release the socket).
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: s.engine}
	s.mu.Lock()
	s.httpServer = srv
	s.listener = ln
	s.mu.Unlock()
	return nil
}

// Serve accepts connections on the socket already bound by Listen, blocking
// until the server is shut down (via Shutdown) or an unexpected serve error
// occurs (finding M-16). It is an error to call Serve before Listen. A clean
// shutdown returns nil, not http.ErrServerClosed.
func (s *Server) Serve() error {
	s.mu.Lock()
	srv := s.httpServer
	ln := s.listener
	s.mu.Unlock()
	if srv == nil || ln == nil {
		return errors.New("hitl: Serve called before Listen")
	}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Run serves the HITL API + status page on addr, blocking until the server is
// shut down (via Shutdown) or a listen error occurs. It is the synchronous
// Listen-then-Serve convenience used by tests and any caller that does not need
// to observe the bind result separately from the serve loop. Because it now
// delegates to Listen, the socket is bound BEFORE Serve begins accepting, so a
// bind failure is returned directly rather than surfacing asynchronously
// (finding M-16). Like Serve, it is backed by an *http.Server so cmd/main.go can
// drain it gracefully on SIGINT/SIGTERM (finding M4); a clean shutdown returns
// nil, not http.ErrServerClosed.
func (s *Server) Run(addr string) error {
	if err := s.Listen(addr); err != nil {
		return err
	}
	return s.Serve()
}

// Shutdown gracefully drains in-flight requests and stops the HTTP server
// within ctx's deadline (finding M4). It is safe to call from a different
// goroutine than Run/Serve, and is a no-op if the server was never started.
// If Listen bound a socket that Serve never consumed, the listener is closed
// explicitly so the bound port is never leaked (finding M-16); when Serve did
// consume it, srv.Shutdown already closes it and the redundant Close is
// harmless.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpServer
	ln := s.listener
	s.mu.Unlock()
	if srv == nil {
		if ln != nil {
			_ = ln.Close()
		}
		return nil
	}
	err := srv.Shutdown(ctx)
	if ln != nil {
		_ = ln.Close()
	}
	return err
}

// SetReady toggles the readiness flag reported by /healthz (findings M3/L1).
// cmd/main.go marks the service not-ready until its dependencies are reachable
// and (in serve mode) the pipeline has completed.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// healthz is a pure LIVENESS probe (finding M-16). It reports 200 {"status":
// "ok"} whenever the process is alive and the HTTP server can service a request
// — it deliberately does NOT check readiness or ping any dependency. Liveness
// answers only "is this process running and not wedged?"; a transient dependency
// outage or a not-yet-ready pipeline must NOT flip liveness, or an orchestrator's
// liveness probe would needlessly restart a healthy process that is simply
// waiting on a dependency. Readiness (deps reachable + pipeline complete) is a
// SEPARATE signal served by /readyz, which orchestration consumes to gate
// traffic. Separating the two is exactly what M-16 requires.
func (s *Server) healthz(c *gin.Context) {
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
