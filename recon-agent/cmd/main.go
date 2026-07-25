// Command recon-agent is the executable entrypoint for the reconciliation
// sidecar. It composes the runtime object graph, awaits dependency readiness,
// drives the break-triage pipeline through Blnk's native reconciliation HTTP
// flow, and then either exits (one-shot mode) or serves the human-in-the-loop
// (HITL) API and status page with a bounded, graceful shutdown.
//
// Modes are selected by the -once flag:
//
//	-once=false (default) SERVE mode: start the HITL/status server (initially
//	                      reporting not-ready on /healthz), await dependency
//	                      readiness, run the pipeline once, mark ready on
//	                      success, and block until SIGINT/SIGTERM triggers a
//	                      graceful shutdown. This is what the docker-compose
//	                      recon-agent service runs.
//	-once=true            ONE-SHOT mode: await readiness, run the pipeline once,
//	                      print the summary table, and exit — nonzero on any
//	                      failure (CSV/Blnk/LLM/dependency). This is what
//	                      `make demo` (go run ./cmd -once) invokes.
//
// Pipeline (the required Blnk-native flow, findings F1/F2): load the external
// statement -> UploadExternalData (consume the real upload_id) -> create a
// detection matching rule -> StartReconciliation (dry-run) -> poll
// GetReconciliation to completion -> derive per-break identity via a valid
// single-transaction dry-run ProbeBreak (a probe transport/validation error is
// fail-closed to HITL, never counted as "unmatched", finding F2) -> hand each
// break to the remediator (classify -> gate -> auto-remediate or escalate).
// Resolved-run artifacts are written to a SEPARATE, idempotent file — never the
// committed ground-truth corpus (finding F6). Summary counts are scoped to this
// run's id set (finding M1). Any failure exits nonzero in one-shot (finding F4).
//
// The agent reaches Blnk exclusively over HTTP through internal/blnk; this file
// imports only recon-agent's own internal packages plus the standard library
// and never any Blnk internal package (Rule 5.1).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/hitl"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/remediator"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Default filesystem locations. defaultCSVPath is the seed statement (resolved
// relative to the module directory, which is `make demo`'s working dir).
// defaultResolvedPath is the resolved-run artifact log: a SEPARATE, idempotent
// file, deliberately NOT the committed eval corpus (finding F6).
const (
	defaultCSVPath      = "../seed/external_transactions.csv"
	defaultResolvedPath = "recon_resolved.jsonl"
	defaultSource       = "seed-bank"

	// committedCorpusBase is the basename of the committed ground-truth eval
	// corpus. The resolved-artifact writer refuses to target any file with this
	// name so the SUT can never mutate the committed corpus (finding F6).
	committedCorpusBase = "recon_corpus.jsonl"

	// summaryReportBase is the basename of the machine-readable one-shot run
	// report written alongside the resolved artifact in `-once` (demo) mode
	// (findings M-01, M-17). It carries the same run-scoped counts printSummary
	// renders (breaks in / auto-resolved / escalated / audit count) in a stable
	// JSON shape the standalone eval scorer (eval/scorer) consumes to enforce the
	// AAP's acceptance criteria and exit nonzero on any failed invariant. It is a
	// SEPARATE, idempotent artifact (truncated each run), never the committed
	// corpus, and is emitted ONLY in one-shot mode — serve mode never writes it.
	summaryReportBase = "recon_summary.json"
)

// Agent-domain lifecycle/grammar vocabulary and Blnk-wire status/strategy
// values are deliberately NOT redeclared in cmd (finding m-01). cmd consumes
// the single module-wide sources of truth directly: break statuses and the
// Rule 5.2 detection-rule grammar from internal/model (model.Status*,
// model.Field*, model.Operator*), and Blnk's reconciliation strategy and
// terminal run-status strings from internal/blnk (blnk.StrategyOneToOne,
// blnk.ReconStatusCompleted, blnk.ReconStatusFailed). This removes the former
// local duplicates that risked silently diverging from the packages that
// actually write and read these values.
//
// The strict detection rule (built in detectionRule below) still pins ALL
// matchable fields with the equals operator so a transaction counts as MATCHED
// only when it aligns exactly on amount, date, reference AND currency with an
// internal booking; anything differing in any field is surfaced as a break
// (finding C-02).

// Lifecycle bounds.
const (
	readinessTimeout  = 30 * time.Second
	reconPollInterval = 200 * time.Millisecond
	pipelineTimeout   = 90 * time.Second
	shutdownTimeout   = 10 * time.Second
	// migrateTimeout bounds the whole boot migration — advisory-lock acquisition
	// (itself capped at the store's migrateLockTimeout) plus the additive DDL — so
	// startup can never hang unbounded on a stuck lock or a slow statement
	// (finding M-16 "bounded startup"). It is comfortably larger than the store's
	// lock ceiling so a legitimately queued migration still completes.
	migrateTimeout = 60 * time.Second
)

// External-statement CSV bounds (finding M-07). The demo entrypoint reads the
// SAME statement the seed uploads, so its parser enforces the SAME bounded,
// streaming schema the seed's validateExternalCSV does: a malformed or hostile
// statement fails fast instead of exhausting memory or letting non-finite /
// empty / duplicate data reach Blnk (which silently coerces an unparseable
// amount to 0 and an unparseable date to the zero time). These mirror the seed's
// constants exactly so both code paths agree on what a valid statement is.
const (
	maxCSVFileBytes  int64 = 8 << 20  // 8 MiB cap on the whole statement file
	maxCSVRows             = 100_000  // cap on data rows (excludes the header)
	maxCSVFieldBytes       = 64 << 10 // 64 KiB cap on any single field
	maxAbsAmount           = 1e12     // reject non-finite / absurd-magnitude amounts
)

// requiredCSVColumns is the full column set every external-statement row must
// carry (finding M-07), mirroring the seed's requiredCSVColumns and the columns
// governed by Blnk's files.go. Requiring the whole schema — rather than only
// id/amount — is what makes the parser reject a truncated or wrong statement
// up front instead of silently building partial transactions.
var requiredCSVColumns = []string{"id", "amount", "currency", "reference", "description", "date"}

// readinessInterval is the delay between dependency-readiness polls. It is a
// var (not a const) solely so the readiness unit test can shrink it to keep the
// timeout-path test fast; production code never mutates it.
var readinessInterval = 500 * time.Millisecond

// maxPipelineAttempts bounds how many times serve mode retries the startup
// pipeline before giving up and serving in the not-ready state (finding M-16).
// A transient dependency hiccup or Blnk error at boot should not permanently
// wedge the run, but an unbounded retry storm is equally undesirable; three
// attempts with a backoff between them is the bounded restart policy. Durable
// run idempotency (finding M-15) makes each retry safe: a resumed run reuses the
// original scoped id set instead of duplicating history.
const maxPipelineAttempts = 3

// pipelineRetryBackoff is the delay between failed serve-mode pipeline attempts
// (finding M-16). It is a var (not a const) solely so the serve/retry unit test
// can shrink it; production code never mutates it.
var pipelineRetryBackoff = 2 * time.Second

// summary captures the demo scorecard printed after the pipeline runs. Every
// count is scoped to the current run's id set (finding M1).
type summary struct {
	breaksIn     int
	autoResolved int
	escalated    int
	auditCount   int
}

// Consumer interfaces used by the pipeline. The concrete sibling types satisfy
// them (asserted below), so cmd/main.go wires them directly (Gate 9); the unit
// test injects lightweight fakes / a mock Blnk endpoint (finding M5).
type (
	// reconClient is the subset of *blnk.Client the pipeline drives. Break
	// detection reads Blnk's authoritative integer unmatched count from the
	// run's single detection reconciliation (GetReconciliation) rather than
	// probing per transaction, so the pipeline no longer calls ProbeBreak
	// (findings C-02/C-03).
	reconClient interface {
		UploadExternalData(ctx context.Context, source, filename string, file io.Reader) (blnk.UploadResponse, error)
		CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error)
		DeleteMatchingRule(ctx context.Context, ruleID string) error
		StartReconciliation(ctx context.Context, req blnk.StartReconciliationRequest) (string, error)
		GetReconciliation(ctx context.Context, reconciliationID string) (blnk.Reconciliation, error)
	}
	// remediatorPort triages a single break (classify -> gate -> auto/escalate).
	// baselineUnmatched is the run's detection unmatched count; the auto path
	// confirms clearance only when a dry-run over the break's upload drives the
	// unmatched count strictly below it (findings C-03, Rule 5.3).
	remediatorPort interface {
		Handle(ctx context.Context, txn blnk.ExternalTransaction, uploadID string, baselineUnmatched int) error
	}
	// pipelineStore is the persistence surface the pipeline uses directly. It is
	// READ/idempotency-only: every break STATE change (classify/resolve/escalate)
	// and its append-only audit event are owned by the remediator, which commits
	// them atomically (finding M-12) — the pipeline never performs a direct,
	// un-audited or multi-commit break write.
	//
	// BeginRun/CompleteRun implement durable run idempotency (finding M-15): a
	// serve restart over the same fixture reuses the recorded run rather than
	// reprocessing it under fresh ids and duplicating history. The summary scopes
	// every tally to the current run's id set database-side (finding M-14) rather
	// than listing the entire historical table and discarding rows in Go:
	// ListBreaksForIDs returns only this run's break rows (bounded) for the
	// resolved-artifact writer, and the Count*ForIDs helpers tally auto-resolved /
	// escalated / audit counts with a COUNT(*).
	pipelineStore interface {
		BeginRun(ctx context.Context, fixtureKey, runID string) (alreadyCompleted bool, existingRunID string, err error)
		CompleteRun(ctx context.Context, fixtureKey string) error
		ListBreaksForIDs(ctx context.Context, ids []string, page store.Page) ([]store.Break, error)
		CountBreaksByStatusForIDs(ctx context.Context, ids []string, status string) (int, error)
		CountHITLForIDs(ctx context.Context, ids []string) (int, error)
		CountAuditForIDs(ctx context.Context, ids []string) (int, error)
	}
	// readinessProbe / pinger back the startup dependency-readiness wait (L1).
	readinessProbe interface {
		Ready(ctx context.Context) error
	}
	pinger interface {
		Ping(ctx context.Context) error
	}

	// blnkPort and storePort are the combined interfaces the serve-mode startup
	// pipeline needs: the Blnk client is both the reconciliation driver and the
	// readiness probe, and the store is both the pipeline persistence surface and
	// the readiness pinger. Defining them as embedded unions lets runStartupPipeline
	// take interface parameters (so the retry policy of finding M-16 is unit-testable
	// with in-memory fakes) while runServe passes the concrete *blnk.Client /
	// *store.Store, which satisfy them.
	blnkPort interface {
		reconClient
		readinessProbe
	}
	storePort interface {
		pipelineStore
		pinger
	}
	// readySetter is the one method runStartupPipeline needs from the HITL server:
	// flipping readiness true once the pipeline succeeds (findings M3/M-16).
	// *hitl.Server satisfies it.
	readySetter interface {
		SetReady(ready bool)
	}
)

// Compile-time guarantees the concrete sibling types satisfy the consumer
// interfaces the pipeline requires.
var (
	_ reconClient    = (*blnk.Client)(nil)
	_ remediatorPort = (*remediator.Remediator)(nil)
	_ pipelineStore  = (*store.Store)(nil)
	_ readinessProbe = (*blnk.Client)(nil)
	_ pinger         = (*store.Store)(nil)
	// The combined ports the serve-mode startup pipeline depends on: the Blnk
	// client is both the reconciliation driver and the readiness probe, the store
	// is both the pipeline persistence surface and the readiness pinger, and the
	// HITL server is the readiness setter (Gate 9 / findings M-16, M3).
	_ blnkPort    = (*blnk.Client)(nil)
	_ storePort   = (*store.Store)(nil)
	_ readySetter = (*hitl.Server)(nil)
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	once := flag.Bool("once", false, "run the pipeline once, print the summary table, and exit nonzero on failure (used by `make demo`); when false, serve the HITL API and status page")
	csvPath := flag.String("csv", defaultCSVPath, "path to the external-transactions CSV to ingest")
	resolvedPath := flag.String("resolved", defaultResolvedPath, "path to the idempotent JSONL log of this run's resolved/escalated breaks (never the committed eval corpus)")
	source := flag.String("source", defaultSource, "source label attached to ingested external transactions")
	flag.Parse()

	if err := run(*once, *csvPath, *resolvedPath, *source); err != nil {
		log.Fatalf("recon-agent: fatal: %v", err)
	}
}

// run composes the object graph (Gate 9: every component reachable from main)
// and dispatches to one-shot or serve mode.
func run(once bool, csvPath, resolvedPath, source string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Fail fast on the finding-F6 misconfiguration before opening any connection
	// or doing any work: refuse to target the committed corpus.
	if err := validateResolvedPath(resolvedPath); err != nil {
		return err
	}

	// Persistence: open the agent store and apply the additive agent schema.
	st, err := store.New(cfg.AgentDatabaseURL)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Printf("recon-agent: closing store: %v", cerr)
		}
	}()
	// M-16 (bounded startup): apply the additive agent schema under a bounded
	// context so a stuck advisory lock or slow DDL fails fast instead of hanging
	// boot indefinitely.
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), migrateTimeout)
	err = st.Migrate(migrateCtx)
	migrateCancel()
	if err != nil {
		return fmt.Errorf("migrate store: %w", err)
	}

	// Wire the remaining components in dependency order.
	auditWriter, err := audit.New(st)
	if err != nil {
		return fmt.Errorf("init audit writer: %w", err)
	}
	blnkClient := blnk.NewClient(cfg.BlnkBaseURL, cfg.BlnkApiKey)
	cls := classifier.New(cfg)
	rem := remediator.New(cls, blnkClient, auditWriter, st, cfg.ConfAutoThreshold, cfg.LLMModel)

	// M-11: on startup, run the durable rule-outbox recovery sweep BEFORE any
	// pipeline work. It compensates (deletes + audits) any Blnk matching rule a
	// prior crashed run created but never durably confirmed or compensated, so no
	// orphaned rule survives across restarts, and confirms any rule that did
	// legitimately clear its break. A sweep failure is logged but not fatal: it
	// must never block the agent from starting, and each per-row outcome is
	// itself audited and left retryable in the outbox.
	if n, rerr := rem.RecoverPendingRules(context.Background()); rerr != nil {
		log.Printf("recon-agent: rule-outbox recovery sweep failed: %v", rerr)
	} else if n > 0 {
		log.Printf("recon-agent: rule-outbox recovery sweep processed %d pending obligation(s)", n)
	}

	srv := hitl.NewServer(cfg, st, auditWriter, blnkClient)

	if once {
		return runOnce(blnkClient, rem, st, csvPath, resolvedPath, source)
	}
	return runServe(cfg, srv, blnkClient, rem, st, csvPath, resolvedPath, source)
}

// runOnce awaits readiness, runs the pipeline once, prints the summary, and
// exits. Any failure returns a non-nil error so main exits nonzero (finding F4).
//
// It takes the same combined interface ports as runStartupPipeline
// (blnkPort/remediatorPort/storePort) rather than the concrete *blnk.Client /
// *remediator.Remediator / *store.Store so the one-shot path is unit-testable
// with the in-memory fakes / mock Blnk endpoint (finding M5, Gate 9); run()
// passes the concrete siblings, which satisfy the ports.
func runOnce(blnkClient blnkPort, rem remediatorPort, st storePort, csvPath, resolvedPath, source string) error {
	ctx, cancel := context.WithTimeout(context.Background(), pipelineTimeout)
	defer cancel()

	// L1: never triage against not-yet-ready dependencies.
	if err := awaitReadiness(ctx, blnkClient, st, readinessTimeout); err != nil {
		return fmt.Errorf("dependencies not ready: %w", err)
	}

	s, err := runPipeline(ctx, blnkClient, rem, st, csvPath, resolvedPath, source)
	if err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}
	printSummary(os.Stdout, s)
	// Emit the machine-readable run report next to the resolved artifact so the
	// standalone eval scorer can enforce the acceptance criteria (findings M-01,
	// M-17). A report-write failure must fail the one-shot run: `make demo`
	// depends on this artifact to score, so a missing/partial report is a real
	// failure, not a soft warning.
	if err := writeSummaryReport(resolvedPath, s); err != nil {
		return fmt.Errorf("write summary report: %w", err)
	}
	return nil
}

// runServe binds the HITL server, serves it (initially not-ready), retries the
// startup pipeline under a bounded policy (finding M-16), marks the service
// ready on the first success, and blocks until SIGINT/SIGTERM triggers a bounded
// graceful shutdown (finding M4). While dependencies are unready or the pipeline
// has not yet succeeded, /readyz reports not-ready (finding M3) — never a
// false-green.
//
// Finding M-16 — startup lifecycle — is addressed in three ways:
//   - BIND FIRST, SYNCHRONOUSLY. srv.Listen binds the socket before any
//     long-running work and returns a bind error directly, so a fatal condition
//     such as an already-in-use port fails the process fast and deterministically
//     instead of racing inside a background goroutine and surfacing minutes later.
//   - BOUNDED STARTUP. The store migration ran under a bounded context in run(),
//     and each pipeline attempt runs under its own pipelineTimeout child context,
//     so no startup step can hang unbounded.
//   - RETRY / FATAL RESTART POLICY. A failed startup pipeline is retried up to
//     maxPipelineAttempts with a backoff rather than being abandoned after a
//     single try; durable run idempotency (finding M-15) makes each retry reuse
//     the original run instead of duplicating history. After the attempts are
//     exhausted the server keeps running in the not-ready state so an operator
//     can still reach the HITL surface — the fail-safe posture, never a crash
//     loop and never a false-green.
func runServe(cfg config.Config, srv *hitl.Server, blnkClient *blnk.Client, rem *remediator.Remediator, st *store.Store, csvPath, resolvedPath, source string) error {
	// M4: intercept termination signals so deferred cleanup (store close, server
	// drain) actually runs instead of the process being killed outright.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Report not-ready until the pipeline succeeds (M3/L1).
	srv.SetReady(false)
	addr := ":" + cfg.HitlPort

	// M-16: BIND FIRST. Bind the listening socket synchronously BEFORE any
	// long-running startup work so a bind failure (e.g. port in use) is fatal and
	// immediate rather than discovered asynchronously after the pipeline has run.
	if err := srv.Listen(addr); err != nil {
		return fmt.Errorf("bind HITL server on %s: %w", addr, err)
	}
	log.Printf("recon-agent: bound HITL server on %s (initially not-ready)", addr)

	// Serve on the already-bound socket.
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	// M-16: run the startup pipeline under a bounded retry policy in the
	// background so shutdown signals and fatal serve errors remain responsive
	// while retries/backoffs are in flight. The goroutine honors ctx, so a
	// shutdown during a backoff or an in-flight attempt cancels promptly.
	go runStartupPipeline(ctx, srv, blnkClient, rem, st, csvPath, resolvedPath, source)

	// Block until a shutdown signal or a fatal server error.
	select {
	case <-ctx.Done():
		log.Printf("recon-agent: shutdown signal received; draining HITL server")
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("hitl server: %w", err)
		}
		return nil
	}

	// M4: bounded graceful shutdown; the deferred st.Close() in run() then runs.
	shCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Printf("recon-agent: HITL server drained; exiting")
	return nil
}

// runStartupPipeline drives the serve-mode startup pipeline under the bounded
// retry policy of finding M-16. It awaits dependency readiness (finding L1) and
// then runs the pipeline; on failure it retries up to maxPipelineAttempts with a
// backoff between attempts. On the first success it prints the summary and marks
// the server ready (M3). If every attempt fails it returns having left the server
// not-ready, so the process keeps serving the HITL surface for operator review
// rather than crash-looping or reporting a false-green. It honors ctx throughout,
// so a shutdown signal during an attempt or a backoff ends it promptly.
func runStartupPipeline(ctx context.Context, srv readySetter, blnkClient blnkPort, rem remediatorPort, st storePort, csvPath, resolvedPath, source string) {
	for attempt := 1; attempt <= maxPipelineAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if rerr := awaitReadiness(ctx, blnkClient, st, readinessTimeout); rerr != nil {
			log.Printf("recon-agent: dependencies not ready (attempt %d/%d): %v", attempt, maxPipelineAttempts, rerr)
		} else {
			// Each attempt runs under its own bounded context so a wedged
			// pipeline can never hang the startup sequence (M-16 bounded startup).
			pctx, cancel := context.WithTimeout(ctx, pipelineTimeout)
			s, perr := runPipeline(pctx, blnkClient, rem, st, csvPath, resolvedPath, source)
			cancel()
			if perr == nil {
				printSummary(os.Stdout, s)
				srv.SetReady(true)
				log.Printf("recon-agent: pipeline complete (attempt %d/%d); serving in ready state", attempt, maxPipelineAttempts)
				return
			}
			log.Printf("recon-agent: pipeline failed (attempt %d/%d): %v", attempt, maxPipelineAttempts, perr)
		}
		// Back off before the next attempt, unless this was the last attempt or a
		// shutdown was requested (M-16 bounded restart, ctx-responsive).
		if attempt < maxPipelineAttempts {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pipelineRetryBackoff):
			}
		}
	}
	log.Printf("recon-agent: pipeline did not succeed after %d attempt(s); serving in not-ready state for operator review", maxPipelineAttempts)
}

// awaitReadiness polls the Blnk endpoint and the agent store until both are
// reachable or the timeout elapses (finding L1). It returns an error on timeout
// (one-shot treats this as fatal) or when ctx is cancelled.
func awaitReadiness(ctx context.Context, rp readinessProbe, pg pinger, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var lastErr error
		if err := rp.Ready(ctx); err != nil {
			lastErr = fmt.Errorf("blnk not ready: %w", err)
		} else if err := pg.Ping(ctx); err != nil {
			lastErr = fmt.Errorf("store not ready: %w", err)
		} else {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readinessInterval):
		}
	}
}

// runPipeline drives the required Blnk-native reconciliation flow and returns a
// run-scoped summary. It returns an error on any failure so callers can exit
// nonzero (finding F4). See the package doc for the step-by-step flow.
func runPipeline(ctx context.Context, bc reconClient, rem remediatorPort, st pipelineStore, csvPath, resolvedPath, source string) (summary, error) {
	// F6: refuse a dangerous resolved-artifact path (e.g. the committed eval
	// corpus) BEFORE doing any work, so a misconfiguration fails fast instead of
	// running the whole pipeline and only then discovering the output is
	// unwritable. run() performs the same check even earlier; this makes
	// runPipeline self-protecting for direct callers (including tests).
	if err := validateResolvedPath(resolvedPath); err != nil {
		return summary{}, err
	}

	// F4: load and validate the statement; any problem is a hard error, never a
	// silent zero-break "success".
	txns, err := loadExternalTransactions(csvPath, source)
	if err != nil {
		return summary{}, fmt.Errorf("load external transactions from %q: %w", csvPath, err)
	}
	if len(txns) == 0 {
		return summary{}, fmt.Errorf("no external transactions parsed from %q", csvPath)
	}
	log.Printf("recon-agent: loaded %d external transaction(s) from %q", len(txns), csvPath)

	// M-15: durable run idempotency. A fixture is identified by a deterministic
	// content hash of its canonical rows plus the source label, so the SAME
	// statement always maps to the SAME fixture key regardless of the random
	// per-run id suffix. BeginRun records the run once and tells us what to do:
	//
	//   - fresh fixture  -> proceed under a newly-generated run id;
	//   - already done   -> a prior boot fully processed this fixture; rebuild the
	//                        summary from the recorded run's persisted rows and
	//                        return WITHOUT touching Blnk or reprocessing, so a
	//                        serve restart can no longer duplicate history or
	//                        inflate the demo scorecard;
	//   - crashed run     -> reuse the ORIGINAL run id so the retry converges on
	//                        the same scoped id set (upsert-idempotent) instead of
	//                        forking a new duplicate history under a fresh id.
	fixtureKey := fixtureKeyFor(source, txns)
	alreadyCompleted, runID, err := st.BeginRun(ctx, fixtureKey, shortRunID())
	if err != nil {
		return summary{}, fmt.Errorf("begin run for fixture %s: %w", fixtureKey, err)
	}

	// Reconstruct the run-scoped id set (id + "-" + runID) that this fixture used.
	// Run-scoped ids make the upload rerun-safe (no external_transactions_pkey
	// collisions, findings F3/F8) and scope the summary/artifacts to THIS run
	// (finding M1). The reconstruction is deterministic, so it is identical on a
	// fresh run and on an idempotent replay of a completed/crashed run.
	scoped := make([]blnk.ExternalTransaction, len(txns))
	txnByID := make(map[string]blnk.ExternalTransaction, len(txns))
	runIDs := make(map[string]bool, len(txns))
	for i, t := range txns {
		t.ID = fmt.Sprintf("%s-%s", t.ID, runID)
		scoped[i] = t
		txnByID[t.ID] = t
		runIDs[t.ID] = true
	}

	// M-15: a fixture that already completed on a prior boot is NOT reprocessed.
	// Rebuild the summary from its persisted rows so serve mode still reports the
	// scorecard, then return — Blnk is never touched and no history is duplicated.
	if alreadyCompleted {
		log.Printf("recon-agent: fixture %s already completed under run %s; rebuilding summary without reprocessing (finding M-15)", fixtureKey, runID)
		s, runBreaks, serr := scopedSummary(ctx, st, runIDs)
		if serr != nil {
			return summary{}, serr
		}
		s.breaksIn = len(runBreaks)
		if werr := writeResolvedArtifacts(resolvedPath, runBreaks, txnByID); werr != nil {
			log.Printf("recon-agent: could not write resolved artifacts to %q: %v", resolvedPath, werr)
		}
		return s, nil
	}

	// F1: upload the statement and consume the real upload id.
	upload, err := bc.UploadExternalData(ctx, source, "recon_statement.csv", strings.NewReader(buildStatementCSV(scoped)))
	if err != nil {
		return summary{}, fmt.Errorf("upload external data: %w", err)
	}
	if upload.UploadID == "" {
		return summary{}, fmt.Errorf("upload returned an empty upload_id")
	}
	if upload.RecordCount != len(scoped) {
		return summary{}, fmt.Errorf("upload record_count %d does not match %d uploaded rows", upload.RecordCount, len(scoped))
	}
	log.Printf("recon-agent: uploaded %d record(s); upload_id=%s", upload.RecordCount, upload.UploadID)

	// C-02: create the strict all-field detection matching rule (amount + date +
	// reference + currency, all equals). Under it Blnk treats an uploaded row as
	// matched only when it aligns EXACTLY with an internal booking on every
	// field, so every genuine break differs in at least one field and is surfaced
	// as unmatched — a live-proven detector whose authoritative unmatched count
	// (read from GetReconciliation below) is the exact break set. This replaces
	// the earlier reference-only detector and its sequential per-transaction
	// ProbeBreak loop, which could neither satisfy exact-six semantics (C-02) nor
	// avoid Blnk's upload-agnostic pagination cache (C-03). The rule is the
	// required non-empty matching_rule_ids set for the run and is deleted
	// afterwards so no detection rule accumulates in Blnk's catalog (finding F5
	// parity).
	detect, err := bc.CreateMatchingRule(ctx, detectionRule(runID))
	if err != nil {
		return summary{}, fmt.Errorf("create detection rule: %w", err)
	}
	if detect.RuleID == "" {
		return summary{}, fmt.Errorf("create detection rule returned an empty rule id")
	}
	defer func() {
		if derr := bc.DeleteMatchingRule(context.Background(), detect.RuleID); derr != nil {
			log.Printf("recon-agent: could not delete detection rule %s: %v", detect.RuleID, derr)
		}
	}()

	// F1: start a dry-run reconciliation over the uploaded batch and poll it to
	// completion (the required upload -> start -> get flow).
	reconID, err := bc.StartReconciliation(ctx, blnk.StartReconciliationRequest{
		UploadID:        upload.UploadID,
		Strategy:        blnk.StrategyOneToOne,
		DryRun:          true,
		MatchingRuleIDs: []string{detect.RuleID},
	})
	if err != nil {
		return summary{}, fmt.Errorf("start reconciliation: %w", err)
	}
	if err := pollReconciliation(ctx, bc, reconID); err != nil {
		return summary{}, fmt.Errorf("await reconciliation %s: %w", reconID, err)
	}

	// C-02: read Blnk's AUTHORITATIVE integer unmatched count from the completed
	// detection reconciliation. This is the live-proven detection signal (and the
	// baseline against which each break's clearance is later confirmed, C-03),
	// derived entirely from Blnk's own result over the persisted upload rather
	// than from sequential per-transaction probes that would trip Blnk's
	// upload-agnostic pagination cache (Rule 5.8).
	detectRecon, err := bc.GetReconciliation(ctx, reconID)
	if err != nil {
		return summary{}, fmt.Errorf("read reconciliation %s result: %w", reconID, err)
	}
	baselineUnmatched := detectRecon.UnmatchedTransactions
	log.Printf("recon-agent: reconciliation %s completed; %d unmatched of %d uploaded", reconID, baselineUnmatched, len(scoped))

	// C-02: derive the exact break set from the authoritative unmatched count.
	breaks := deriveBreaks(scoped, baselineUnmatched)
	log.Printf("recon-agent: derived %d break(s) to triage", len(breaks))

	// Triage each break via the remediator, threading the detection upload and
	// baseline so the auto path confirms clearance with a cache-safe,
	// upload-scoped dry-run (findings C-03, Rule 5.3). A per-break agent-side
	// infra failure aborts the run nonzero (finding F4).
	for _, txn := range breaks {
		if err := rem.Handle(ctx, txn, upload.UploadID, baselineUnmatched); err != nil {
			return summary{}, fmt.Errorf("remediate break %q: %w", txn.ID, err)
		}
	}

	// M-15: mark this fixture's run durably complete so a serve restart over the
	// same statement is reported alreadyCompleted by BeginRun and skips
	// reprocessing. This runs only after every break triaged successfully, so a
	// mid-pipeline crash leaves the run 'running' and the retry resumes it under
	// the same scoped id set rather than being falsely treated as done.
	if err := st.CompleteRun(ctx, fixtureKey); err != nil {
		return summary{}, fmt.Errorf("complete run for fixture %s: %w", fixtureKey, err)
	}

	// M1: scope every tally to THIS run's id set.
	s, runBreaks, err := scopedSummary(ctx, st, runIDs)
	if err != nil {
		return summary{}, err
	}
	s.breaksIn = len(breaks)

	// F6: write this run's resolved/escalated artifacts to a SEPARATE, idempotent
	// file using the documented corpus schema — never the committed corpus.
	// Best-effort: an I/O error is logged, not fatal (the triage already
	// succeeded), but the committed-corpus guard is enforced up front in run().
	if werr := writeResolvedArtifacts(resolvedPath, runBreaks, txnByID); werr != nil {
		log.Printf("recon-agent: could not write resolved artifacts to %q: %v", resolvedPath, werr)
	}

	return s, nil
}

// fixtureKeyFor computes the deterministic run-idempotency key for a fixture
// (finding M-15): a SHA-256 over the source label and the canonical CSV
// rendering of the UNSCOPED transactions. Because it is derived from the
// fixture's content — not the random per-run id suffix — the same statement
// always maps to the same key across restarts, which is exactly what lets
// BeginRun recognize an already-processed fixture and skip reprocessing it.
func fixtureKeyFor(source string, txns []blnk.ExternalTransaction) string {
	h := sha256.New()
	_, _ = io.WriteString(h, source)
	_, _ = h.Write([]byte{0})
	_, _ = io.WriteString(h, buildStatementCSV(txns))
	return hex.EncodeToString(h.Sum(nil))
}

// deriveBreaks derives the exact set of breaks to triage from Blnk's
// AUTHORITATIVE detection result (findings C-02/C-03) instead of sequential
// per-transaction dry-run probes. The run's single detection reconciliation over
// the persisted upload applies the strict all-field rule, under which a genuine
// match requires amount, date, reference AND currency to align exactly; every
// uploaded transaction that differs in any field is surfaced as unmatched.
//
// baselineUnmatched is that reconciliation's authoritative unmatched count. When
// it equals the uploaded cardinality, EVERY uploaded transaction is a break — the
// exact-six live proof the demo relies on. When it is smaller (an uploaded row
// perfectly matched an internal booking and is therefore NOT a break), Blnk's
// count-only HTTP surface cannot name WHICH rows matched (the unmatched id list
// is never exposed, and reading Blnk's tables is forbidden, Rule 5.1); rather
// than risk DROPPING a genuine break, the agent conservatively triages ALL
// uploaded rows and logs the discrepancy. A perfectly-matched row that is
// triaged simply clears immediately on its own confirmation and is recorded
// auto-resolved — never lost. This per-row-identity imprecision is the residual
// of Blnk's protected-core cache/list surface (Rule 5.8 / AAP §0.6.2) and is the
// strongest derivation obtainable over the native HTTP contract.
//
// A zero unmatched count means Blnk matched every uploaded row: there are no
// breaks to triage, which is a legitimate empty-result success (not an error).
func deriveBreaks(txns []blnk.ExternalTransaction, baselineUnmatched int) []blnk.ExternalTransaction {
	if baselineUnmatched == 0 {
		log.Printf("recon-agent: detection reported 0 unmatched of %d uploaded; no breaks to triage", len(txns))
		return nil
	}
	if baselineUnmatched != len(txns) {
		// Defensive: Blnk matched some uploaded rows, but count-only HTTP cannot
		// identify which. Triage all rows conservatively so no genuine break is
		// dropped; a truly-matched row clears immediately and is auto-resolved.
		log.Printf("recon-agent: WARNING detection unmatched count %d != %d uploaded; count-only HTTP cannot name matched rows (Rule 5.8), triaging all rows conservatively", baselineUnmatched, len(txns))
	}
	breaks := make([]blnk.ExternalTransaction, len(txns))
	copy(breaks, txns)
	return breaks
}

// detectionRule builds the run-scoped, grammar-conformant (Rule 5.2) detection
// rule used to derive breaks from Blnk's authoritative unmatched count (finding
// C-02). It pins ALL matchable fields (amount, date, reference, currency) with
// the equals operator, so Blnk treats an uploaded transaction as MATCHED only
// when it aligns EXACTLY with an internal booking on every field. A single-field
// (e.g. reference-only) rule would spuriously match genuine breaks that share a
// reference but differ in amount/date/currency — the defect this replaces — and
// could not satisfy the exact-six break semantics. Under this strict rule every
// genuine break (timing/amount/reference/currency mismatch, duplicate, or
// missing internal counterpart) differs in at least one field and is surfaced as
// unmatched, so the detection reconciliation's unmatched count equals the number
// of breaks. The rule is deleted after the run (runPipeline's defer) so no
// detection rule accumulates in Blnk's catalog (finding F5 parity).
func detectionRule(runID string) blnk.MatchingRule {
	return blnk.MatchingRule{
		Name:        "recon-agent-detect-" + runID,
		Description: "strict all-field detection rule (amount+date+reference+currency equals; surfaces every non-exact-match upload row as a break; auto-deleted after the run)",
		Criteria: []blnk.MatchingCriteria{
			{Field: model.FieldAmount, Operator: model.OperatorEquals},
			{Field: model.FieldDate, Operator: model.OperatorEquals},
			{Field: model.FieldReference, Operator: model.OperatorEquals},
			{Field: model.FieldCurrency, Operator: model.OperatorEquals},
		},
	}
}

// pollReconciliation polls GET /reconciliation/:id until the run completes or
// fails, or ctx is cancelled/timed out.
func pollReconciliation(ctx context.Context, bc reconClient, reconID string) error {
	ticker := time.NewTicker(reconPollInterval)
	defer ticker.Stop()
	for {
		rec, err := bc.GetReconciliation(ctx, reconID)
		if err != nil {
			return err
		}
		switch rec.Status {
		case blnk.ReconStatusCompleted:
			return nil
		case blnk.ReconStatusFailed:
			return fmt.Errorf("reconciliation reported status %q", rec.Status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// runBreaksPageSize bounds each page fetched while collecting the current run's
// break rows for the resolved-artifact writer. It is well under the store's
// maxPageLimit so every fetch is bounded (finding M-14).
const runBreaksPageSize = 1000

// scopedSummary tallies auto-resolved / escalated / audit counts restricted to
// the current run's id set (finding M1) and returns the run's break rows for the
// resolved-artifact writer. Every tally is computed DATABASE-SIDE and scoped to
// the run's id set (finding M-14): rather than listing the entire historical
// break/HITL/audit tables and discarding every row outside the run in Go, it
// issues bounded, id-scoped COUNT(*) queries and fetches only the run's break
// rows in bounded pages.
func scopedSummary(ctx context.Context, st pipelineStore, runIDs map[string]bool) (summary, []store.Break, error) {
	ids := make([]string, 0, len(runIDs))
	for id := range runIDs {
		ids = append(ids, id)
	}

	// Fetch ONLY this run's break rows (id-scoped, bounded) for the resolved
	// artifact writer, paging so an arbitrarily large run is still read in
	// bounded chunks rather than one unbounded query.
	var runBreaks []store.Break
	for offset := 0; ; offset += runBreaksPageSize {
		page, err := st.ListBreaksForIDs(ctx, ids, store.Page{Limit: runBreaksPageSize, Offset: offset})
		if err != nil {
			return summary{}, nil, fmt.Errorf("list run breaks: %w", err)
		}
		runBreaks = append(runBreaks, page...)
		if len(page) < runBreaksPageSize {
			break
		}
	}

	autoResolved, err := st.CountBreaksByStatusForIDs(ctx, ids, model.StatusAutoResolved)
	if err != nil {
		return summary{}, nil, fmt.Errorf("count auto-resolved breaks: %w", err)
	}
	escalated, err := st.CountHITLForIDs(ctx, ids)
	if err != nil {
		return summary{}, nil, fmt.Errorf("count escalated breaks: %w", err)
	}
	auditCount, err := st.CountAuditForIDs(ctx, ids)
	if err != nil {
		return summary{}, nil, fmt.Errorf("count audit events: %w", err)
	}

	return summary{autoResolved: autoResolved, escalated: escalated, auditCount: auditCount}, runBreaks, nil
}

// loadExternalTransactions parses the seed CSV. The header is matched
// case-insensitively and expects the columns dictated by Blnk's external-file
// contract: ID, Amount, Currency, Reference, Description, Date. The source is
// supplied by the caller (the CSV carries no source column).
//
// Finding F4: a missing/unreadable/empty/header-only file, a missing required
// column, or an unparseable amount/date is returned as an ERROR — never coerced
// into a zero-break "success".
// loadExternalTransactions reads and strictly validates the external statement
// CSV into run transactions (finding M-07). Because the demo reads the SAME
// statement the seed uploads, this parser enforces the SAME bounded, streaming
// schema as the seed's validateExternalCSV rather than the earlier lenient
// "id+amount only, ReadAll" parser:
//
//   - BOUNDED & STREAMING: the file is size-checked before opening, read through
//     an io.LimitReader capped at maxCSVFileBytes, parsed row-by-row (never
//     ReadAll), with the row count capped at maxCSVRows and each field at
//     maxCSVFieldBytes — so a malformed or hostile statement fails fast instead
//     of buffering an unbounded amount into memory.
//   - FULL SCHEMA: all of requiredCSVColumns must be present; the matchable
//     identity fields (id, amount, currency, reference, date) must be non-empty.
//   - FINITE, RANGED, NON-ZERO AMOUNTS: the amount must parse as a finite float
//     (NaN/±Inf rejected), be non-zero, and be within ±maxAbsAmount. Blnk would
//     otherwise silently coerce an unparseable amount to 0.
//   - UNIQUE IDS: Blnk keys external transactions by id, so a duplicate id in the
//     statement is rejected rather than colliding on upload/reconcile.
//
// A fully-blank line (e.g. a trailing newline) is tolerated and skipped; any
// non-blank row that violates the schema is a hard error with a row-numbered
// message, never a silently-dropped or partial transaction.
func loadExternalTransactions(path, source string) ([]blnk.ExternalTransaction, error) {
	// Fast pre-check on file size before opening, so an obviously oversized
	// statement is rejected without streaming any of it.
	if info, statErr := os.Stat(path); statErr == nil && info.Size() > maxCSVFileBytes {
		return nil, fmt.Errorf("csv %s is %d bytes, exceeding the %d-byte limit", path, info.Size(), maxCSVFileBytes)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Defense-in-depth byte bound (+1 so a file exactly at the limit stays
	// valid). Reading is row-by-row; the whole file is never buffered.
	r := csv.NewReader(io.LimitReader(f, maxCSVFileBytes+1))
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("csv is empty")
		}
		return nil, fmt.Errorf("read csv header: %w", err)
	}

	col := make(map[string]int, len(header))
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, required := range requiredCSVColumns {
		if _, ok := col[required]; !ok {
			return nil, fmt.Errorf("csv is missing required column %q (need: %s)", required, strings.Join(requiredCSVColumns, ", "))
		}
	}
	field := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}

	txns := make([]blnk.ExternalTransaction, 0, 16)
	seenIDs := make(map[string]int) // id -> first line it appeared on
	line := 1                       // header already consumed
	for {
		row, readErr := r.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		line++
		if readErr != nil {
			return nil, fmt.Errorf("read csv row %d: %w", line, readErr)
		}
		if isBlankRow(row) {
			continue // tolerate trailing blank lines
		}
		if len(txns) >= maxCSVRows {
			return nil, fmt.Errorf("csv exceeds the %d-row limit", maxCSVRows)
		}
		// Bound each field so a single monster cell cannot balloon memory.
		for i, cell := range row {
			if len(cell) > maxCSVFieldBytes {
				return nil, fmt.Errorf("csv row %d: field %d is %d bytes, exceeding the %d-byte limit", line, i+1, len(cell), maxCSVFieldBytes)
			}
		}

		id := field(row, "id")
		if id == "" {
			return nil, fmt.Errorf("row %d has data but an empty id", line)
		}
		if first, dup := seenIDs[id]; dup {
			return nil, fmt.Errorf("row %d: duplicate id %q (first seen on row %d)", line, id, first)
		}
		seenIDs[id] = line

		amountStr := field(row, "amount")
		if amountStr == "" {
			return nil, fmt.Errorf("row %d (%q) has an empty amount", line, id)
		}
		amount, aerr := strconv.ParseFloat(amountStr, 64)
		if aerr != nil {
			return nil, fmt.Errorf("row %d (%q) has an unparseable amount %q: %w", line, id, amountStr, aerr)
		}
		if math.IsNaN(amount) || math.IsInf(amount, 0) {
			return nil, fmt.Errorf("row %d (%q) has a non-finite amount %q", line, id, amountStr)
		}
		if amount == 0 {
			return nil, fmt.Errorf("row %d (%q) has a zero amount", line, id)
		}
		if math.Abs(amount) > maxAbsAmount {
			return nil, fmt.Errorf("row %d (%q) amount %q exceeds the maximum magnitude %g", line, id, amountStr, float64(maxAbsAmount))
		}

		currency := field(row, "currency")
		if currency == "" {
			return nil, fmt.Errorf("row %d (%q) has an empty currency", line, id)
		}
		reference := field(row, "reference")
		if reference == "" {
			return nil, fmt.Errorf("row %d (%q) has an empty reference", line, id)
		}
		dateStr := field(row, "date")
		if dateStr == "" {
			return nil, fmt.Errorf("row %d (%q) has an empty date", line, id)
		}
		date, derr := parseDate(dateStr)
		if derr != nil {
			return nil, fmt.Errorf("row %d (%q) has an unparseable date: %w", line, id, derr)
		}

		txns = append(txns, blnk.ExternalTransaction{
			ID:          id,
			Amount:      amount,
			Currency:    currency,
			Reference:   reference,
			Description: field(row, "description"),
			Date:        date,
			Source:      source,
		})
	}
	if len(txns) == 0 {
		return nil, fmt.Errorf("csv contains a header but no data rows")
	}
	return txns, nil
}

// isBlankRow reports whether every field in a CSV row is empty (a trailing blank
// line), so such rows are tolerated rather than treated as malformed.
func isBlankRow(row []string) bool {
	for _, c := range row {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

// parseDate accepts an empty value (zero time), an RFC3339 timestamp, or a bare
// calendar date. A non-empty, unparseable value is an error (finding F4).
func parseDate(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date %q", s)
}

// buildStatementCSV renders the run-scoped transactions back into the Blnk
// external-file CSV format for upload.
func buildStatementCSV(txns []blnk.ExternalTransaction) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	_ = w.Write([]string{"ID", "Amount", "Currency", "Reference", "Description", "Date"})
	for _, t := range txns {
		date := ""
		if !t.Date.IsZero() {
			date = t.Date.Format(time.RFC3339)
		}
		_ = w.Write([]string{
			t.ID,
			strconv.FormatFloat(t.Amount, 'f', -1, 64),
			t.Currency,
			t.Reference,
			t.Description,
			date,
		})
	}
	w.Flush()
	return b.String()
}

// shortRunID returns a short, unique run identifier (the first uuid segment).
func shortRunID() string {
	return strings.SplitN(uuid.NewString(), "-", 2)[0]
}

// evalRecord is one JSONL line, using the required five top-level keys:
// id, scenario, input, expected_output, judging_criteria. It matches the
// committed corpus schema so the resolved-run artifact is directly comparable.
type evalRecord struct {
	ID              string       `json:"id"`
	Scenario        string       `json:"scenario"`
	Input           evalInput    `json:"input"`
	ExpectedOutput  evalExpected `json:"expected_output"`
	JudgingCriteria string       `json:"judging_criteria"`
}

type evalInput struct {
	ExternalTransaction blnk.ExternalTransaction `json:"external_transaction"`
	InternalCandidate   any                      `json:"internal_candidate"`
	ContextNote         string                   `json:"context_note"`
}

type evalExpected struct {
	RootCause      model.RootCause    `json:"root_cause"`
	Regulated      bool               `json:"regulated"`
	ExpectedAction string             `json:"expected_action"`
	ProposedRule   *blnk.MatchingRule `json:"proposed_rule"`
}

// validateResolvedPath refuses to target the committed ground-truth corpus so
// the SUT can never mutate it (finding F6).
func validateResolvedPath(path string) error {
	if filepath.Base(filepath.Clean(path)) == committedCorpusBase {
		return fmt.Errorf("refusing to write resolved-run artifacts to the committed eval corpus %q; choose a separate output path", path)
	}
	return nil
}

// writeResolvedArtifacts writes one JSONL record per run-scoped break to a
// SEPARATE, idempotent file (finding F6): the file is truncated on each run so
// reruns do not accumulate, the documented schema is used, and expected_action
// is auto_resolve / escalate (never the drifted "auto_remediate").
func writeResolvedArtifacts(path string, breaks []store.Break, txnByID map[string]blnk.ExternalTransaction) error {
	if err := validateResolvedPath(path); err != nil {
		return err
	}
	if len(breaks) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, b := range breaks {
		c := b.Classification
		rec := evalRecord{
			ID:       c.ExternalTxnID,
			Scenario: string(c.RootCause),
			Input: evalInput{
				ExternalTransaction: txnByID[c.ExternalTxnID],
				InternalCandidate:   nil,
				ContextNote:         c.Rationale,
			},
			ExpectedOutput: evalExpected{
				RootCause:      c.RootCause,
				Regulated:      c.Regulated,
				ExpectedAction: expectedActionFor(b.Status),
				ProposedRule:   c.ProposedRule,
			},
			JudgingCriteria: "root_cause label matches and, for auto_resolve, a Blnk dry-run reconciliation confirms the external transaction left the unmatched set",
		}
		if err := enc.Encode(&rec); err != nil {
			return err
		}
	}
	return nil
}

// expectedActionFor maps a break's terminal status to the corpus's
// expected_action vocabulary (auto_resolve / escalate).
func expectedActionFor(status string) string {
	if status == model.StatusAutoResolved {
		return "auto_resolve"
	}
	return "escalate"
}

// printSummary renders the demo scorecard: breaks in / auto-resolved /
// escalated / audit count (all scoped to this run, finding M1).
func printSummary(w io.Writer, s summary) {
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "=== recon-agent pipeline summary ===")
	fmt.Fprintf(w, "  %-16s %d\n", "breaks in", s.breaksIn)
	fmt.Fprintf(w, "  %-16s %d\n", "auto-resolved", s.autoResolved)
	fmt.Fprintf(w, "  %-16s %d\n", "escalated", s.escalated)
	fmt.Fprintf(w, "  %-16s %d\n", "audit events", s.auditCount)
	fmt.Fprintln(w, "====================================")
}

// summaryReport is the stable, machine-readable JSON shape of a one-shot run's
// scoped counts (findings M-01, M-17). The standalone eval scorer (eval/scorer)
// decodes exactly these fields to enforce the AAP acceptance invariants
// (exactly-six, >=1 clearance, terminal+audited parity) and cross-check them
// against the immutable corpus. The JSON keys are a committed contract with the
// scorer; do not rename them without updating eval/scorer.
type summaryReport struct {
	BreaksIn     int `json:"breaks_in"`
	AutoResolved int `json:"auto_resolved"`
	Escalated    int `json:"escalated"`
	AuditCount   int `json:"audit_count"`
}

// writeSummaryReport writes the machine-readable one-shot run report as
// summaryReportBase in the SAME directory as the resolved artifact (findings
// M-01, M-17). Co-locating it with resolvedPath means a demo that redirects its
// artifacts to a temp dir keeps the report beside them, and a unit test that
// points resolvedPath at t.TempDir() never litters the module directory. The
// file is created/truncated each run (idempotent), and — like the resolved
// artifact — must never be the committed corpus.
func writeSummaryReport(resolvedPath string, s summary) error {
	// The report's basename is the fixed summaryReportBase, which is structurally
	// distinct from committedCorpusBase, so this artifact can never target the
	// committed corpus (no validateResolvedPath guard is needed as it is for the
	// caller-supplied resolvedPath).
	path := filepath.Join(filepath.Dir(resolvedPath), summaryReportBase)
	rep := summaryReport{
		BreaksIn:     s.breaksIn,
		AutoResolved: s.autoResolved,
		Escalated:    s.escalated,
		AuditCount:   s.auditCount,
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(&rep)
}
