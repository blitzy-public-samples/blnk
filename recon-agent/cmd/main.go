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
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
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
)

// statusAutoResolved / statusQueued mirror the break statuses the remediator
// writes to agent_break. They are duplicated here (rather than imported) to keep
// main decoupled from the remediator's unexported constants.
const (
	statusAutoResolved = "auto-resolved"
	statusQueued       = "queued"
)

// Blnk reconciliation status values and the detection-rule grammar (Rule 5.2).
const (
	reconStatusCompleted = "completed"
	reconStatusFailed    = "failed"

	detectionStrategy = "one_to_one"
	fieldReference    = "reference"
	operatorEquals    = "equals"
)

// Lifecycle bounds.
const (
	readinessTimeout  = 30 * time.Second
	reconPollInterval = 200 * time.Millisecond
	pipelineTimeout   = 90 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// readinessInterval is the delay between dependency-readiness polls. It is a
// var (not a const) solely so the readiness unit test can shrink it to keep the
// timeout-path test fast; production code never mutates it.
var readinessInterval = 500 * time.Millisecond

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
	// reconClient is the subset of *blnk.Client the pipeline drives.
	reconClient interface {
		UploadExternalData(ctx context.Context, source, filename string, file io.Reader) (blnk.UploadResponse, error)
		CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error)
		DeleteMatchingRule(ctx context.Context, ruleID string) error
		StartReconciliation(ctx context.Context, req blnk.StartReconciliationRequest) (string, error)
		GetReconciliation(ctx context.Context, reconciliationID string) (blnk.Reconciliation, error)
		ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, matchingRuleIDs []string) (bool, string, error)
	}
	// remediatorPort triages a single break (classify -> gate -> auto/escalate).
	remediatorPort interface {
		Handle(ctx context.Context, txn blnk.ExternalTransaction, uploadID string) error
	}
	// auditRecorder is the append-only audit sink (Rule 5.5).
	auditRecorder interface {
		Record(ctx context.Context, ev model.AuditEvent) error
	}
	// pipelineStore is the persistence surface the pipeline uses directly (the
	// remediator owns its own richer store access).
	pipelineStore interface {
		UpsertBreak(ctx context.Context, c model.BreakClassification, txn blnk.ExternalTransaction, status string) error
		EnqueueHITL(ctx context.Context, externalTxnID, reason string) error
		ListBreaks(ctx context.Context) ([]store.Break, error)
		ListHITL(ctx context.Context) ([]store.HITLItem, error)
		ListAudit(ctx context.Context) ([]model.AuditEvent, error)
	}
	// readinessProbe / pinger back the startup dependency-readiness wait (L1).
	readinessProbe interface {
		Ready(ctx context.Context) error
	}
	pinger interface {
		Ping(ctx context.Context) error
	}
)

// Compile-time guarantees the concrete sibling types satisfy the consumer
// interfaces the pipeline requires.
var (
	_ reconClient    = (*blnk.Client)(nil)
	_ remediatorPort = (*remediator.Remediator)(nil)
	_ auditRecorder  = (*audit.Writer)(nil)
	_ pipelineStore  = (*store.Store)(nil)
	_ readinessProbe = (*blnk.Client)(nil)
	_ pinger         = (*store.Store)(nil)
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
	if err := st.Migrate(context.Background()); err != nil {
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
	srv := hitl.NewServer(cfg, st, auditWriter, blnkClient)

	if once {
		return runOnce(blnkClient, rem, auditWriter, st, csvPath, resolvedPath, source)
	}
	return runServe(cfg, srv, blnkClient, rem, auditWriter, st, csvPath, resolvedPath, source)
}

// runOnce awaits readiness, runs the pipeline once, prints the summary, and
// exits. Any failure returns a non-nil error so main exits nonzero (finding F4).
func runOnce(blnkClient *blnk.Client, rem *remediator.Remediator, aud *audit.Writer, st *store.Store, csvPath, resolvedPath, source string) error {
	ctx, cancel := context.WithTimeout(context.Background(), pipelineTimeout)
	defer cancel()

	// L1: never triage against not-yet-ready dependencies.
	if err := awaitReadiness(ctx, blnkClient, st, readinessTimeout); err != nil {
		return fmt.Errorf("dependencies not ready: %w", err)
	}

	s, err := runPipeline(ctx, blnkClient, rem, aud, st, csvPath, resolvedPath, source)
	if err != nil {
		return fmt.Errorf("pipeline failed: %w", err)
	}
	printSummary(os.Stdout, s)
	return nil
}

// runServe starts the HITL server (initially not-ready), awaits dependency
// readiness before running the pipeline once (finding L1), marks the service
// ready on success, and blocks until SIGINT/SIGTERM triggers a bounded graceful
// shutdown (finding M4). While dependencies are unready or the pipeline has not
// yet succeeded, /healthz reports not-ready (finding M3) — never a false-green.
func runServe(cfg config.Config, srv *hitl.Server, blnkClient *blnk.Client, rem *remediator.Remediator, aud *audit.Writer, st *store.Store, csvPath, resolvedPath, source string) error {
	// M4: intercept termination signals so deferred cleanup (store close, server
	// drain) actually runs instead of the process being killed outright.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Serve immediately but report not-ready until the pipeline succeeds (M3/L1).
	srv.SetReady(false)
	addr := ":" + cfg.HitlPort
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("recon-agent: serving HITL API and status page on %s (initially not-ready)", addr)
		serveErr <- srv.Run(addr)
	}()

	// L1: await dependency readiness, then run the pipeline once. If deps never
	// become ready (or the pipeline fails), keep serving in the not-ready state
	// rather than reporting a false-green success.
	if rerr := awaitReadiness(ctx, blnkClient, st, readinessTimeout); rerr != nil {
		log.Printf("recon-agent: dependencies not ready after %s: %v; serving in not-ready state", readinessTimeout, rerr)
	} else if s, perr := runPipeline(ctx, blnkClient, rem, aud, st, csvPath, resolvedPath, source); perr != nil {
		log.Printf("recon-agent: pipeline failed: %v; serving in not-ready state", perr)
	} else {
		printSummary(os.Stdout, s)
		srv.SetReady(true)
		log.Printf("recon-agent: pipeline complete; serving in ready state")
	}

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
func runPipeline(ctx context.Context, bc reconClient, rem remediatorPort, aud auditRecorder, st pipelineStore, csvPath, resolvedPath, source string) (summary, error) {
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

	// Run-scoped ids make the upload rerun-safe (no external_transactions_pkey
	// collisions, findings F3/F8) and scope the summary/artifacts to THIS run
	// (finding M1).
	runID := shortRunID()
	scoped := make([]blnk.ExternalTransaction, len(txns))
	txnByID := make(map[string]blnk.ExternalTransaction, len(txns))
	runIDs := make(map[string]bool, len(txns))
	for i, t := range txns {
		t.ID = fmt.Sprintf("%s-%s", t.ID, runID)
		scoped[i] = t
		txnByID[t.ID] = t
		runIDs[t.ID] = true
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

	// F1/F2: create a detection matching rule. A string-only {reference,equals}
	// rule never bridges an amount/date window in this deployment, so Blnk
	// surfaces every uploaded transaction as unmatched — a deterministic
	// detector. StartReconciliation/GetReconciliation exercise the real run, and
	// the per-txn ProbeBreak below reuses this rule as the required non-empty
	// matching_rule_ids set (finding F2). The rule is deleted after the run so no
	// orphan rule accumulates in Blnk (finding F5 parity).
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
		Strategy:        detectionStrategy,
		DryRun:          true,
		MatchingRuleIDs: []string{detect.RuleID},
	})
	if err != nil {
		return summary{}, fmt.Errorf("start reconciliation: %w", err)
	}
	if err := pollReconciliation(ctx, bc, reconID); err != nil {
		return summary{}, fmt.Errorf("await reconciliation %s: %w", reconID, err)
	}
	log.Printf("recon-agent: reconciliation %s completed", reconID)

	// F1/F2: derive per-break identity via valid single-txn dry-run probes.
	breaks, probeEscalations, err := deriveBreaks(ctx, bc, aud, st, scoped, detect.RuleID, upload.UploadID, source)
	if err != nil {
		return summary{}, err
	}
	log.Printf("recon-agent: derived %d break(s) to triage (%d probe-error escalation(s))", len(breaks), probeEscalations)

	// Triage each break via the remediator. A per-break agent-side infra failure
	// aborts the run nonzero (finding F4).
	for _, txn := range breaks {
		if err := rem.Handle(ctx, txn, upload.UploadID); err != nil {
			return summary{}, fmt.Errorf("remediate break %q: %w", txn.ID, err)
		}
	}

	// M1: scope every tally to THIS run's id set.
	s, runBreaks, err := scopedSummary(ctx, st, runIDs)
	if err != nil {
		return summary{}, err
	}
	s.breaksIn = len(breaks) + probeEscalations

	// F6: write this run's resolved/escalated artifacts to a SEPARATE, idempotent
	// file using the documented corpus schema — never the committed corpus.
	// Best-effort: an I/O error is logged, not fatal (the triage already
	// succeeded), but the committed-corpus guard is enforced up front in run().
	if werr := writeResolvedArtifacts(resolvedPath, runBreaks, txnByID); werr != nil {
		log.Printf("recon-agent: could not write resolved artifacts to %q: %v", resolvedPath, werr)
	}

	return s, nil
}

// deriveBreaks probes each transaction with the detection rule to determine
// break status. A probe transport/validation error is fail-closed to HITL
// (finding F2): the transaction is queued for human review — never counted as
// "unmatched" and never dropped. A clean "cleared" result is a match (skipped);
// a clean "not cleared" result is a break to triage.
func deriveBreaks(ctx context.Context, bc reconClient, aud auditRecorder, st pipelineStore, txns []blnk.ExternalTransaction, ruleID, uploadID, source string) ([]blnk.ExternalTransaction, int, error) {
	breaks := make([]blnk.ExternalTransaction, 0, len(txns))
	escalations := 0
	for _, txn := range txns {
		cleared, _, perr := bc.ProbeBreak(ctx, txn, []string{ruleID})
		if perr != nil {
			if eerr := escalateProbeError(ctx, aud, st, txn, uploadID, source, perr); eerr != nil {
				return nil, 0, fmt.Errorf("escalate probe error for %q: %w", txn.ID, eerr)
			}
			log.Printf("recon-agent: probe for %q errored; routed to HITL (fail-closed): %v", txn.ID, perr)
			escalations++
			continue
		}
		if cleared {
			continue
		}
		breaks = append(breaks, txn)
	}
	return breaks, escalations, nil
}

// escalateProbeError fail-closes a transaction whose detection probe errored to
// the HITL queue with an append-only escalation audit event (findings F2/5.7).
func escalateProbeError(ctx context.Context, aud auditRecorder, st pipelineStore, txn blnk.ExternalTransaction, uploadID, source string, cause error) error {
	rationale := "detection probe failed; routed to HITL (fail-closed): " + cause.Error()
	cls := model.BreakClassification{
		ExternalTxnID: txn.ID,
		RootCause:     model.RootCauseUnknown,
		Confidence:    0,
		Regulated:     false,
		Rationale:     rationale,
	}
	if err := st.UpsertBreak(ctx, cls, txn, statusQueued); err != nil {
		return err
	}
	if err := st.EnqueueHITL(ctx, txn.ID, "probe_error"); err != nil {
		return err
	}
	return aud.Record(ctx, audit.Escalated(txn.ID, rationale, 0, model.Provenance{UploadID: uploadID, Source: source}))
}

// detectionRule builds the run-scoped, grammar-conformant (Rule 5.2) detection
// rule. See runPipeline for why a reference-only rule surfaces every uploaded
// transaction as a break in this deployment.
func detectionRule(runID string) blnk.MatchingRule {
	return blnk.MatchingRule{
		Name:        "recon-agent-detect-" + runID,
		Description: "detection rule (reference-only; surfaces uploaded transactions as breaks; auto-deleted after the run)",
		Criteria: []blnk.MatchingCriteria{{
			Field:    fieldReference,
			Operator: operatorEquals,
		}},
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
		case reconStatusCompleted:
			return nil
		case reconStatusFailed:
			return fmt.Errorf("reconciliation reported status %q", rec.Status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// scopedSummary tallies auto-resolved / escalated / audit counts restricted to
// the current run's id set (finding M1) and returns the run's break rows for the
// resolved-artifact writer.
func scopedSummary(ctx context.Context, st pipelineStore, runIDs map[string]bool) (summary, []store.Break, error) {
	breakRows, err := st.ListBreaks(ctx)
	if err != nil {
		return summary{}, nil, fmt.Errorf("list breaks: %w", err)
	}
	runBreaks := make([]store.Break, 0, len(runIDs))
	autoResolved := 0
	for _, b := range breakRows {
		if !runIDs[b.Classification.ExternalTxnID] {
			continue
		}
		runBreaks = append(runBreaks, b)
		if b.Status == statusAutoResolved {
			autoResolved++
		}
	}

	hitlRows, err := st.ListHITL(ctx)
	if err != nil {
		return summary{}, nil, fmt.Errorf("list hitl queue: %w", err)
	}
	escalated := 0
	for _, h := range hitlRows {
		if runIDs[h.ExternalTxnID] {
			escalated++
		}
	}

	auditRows, err := st.ListAudit(ctx)
	if err != nil {
		return summary{}, nil, fmt.Errorf("list audit trail: %w", err)
	}
	auditCount := 0
	for _, a := range auditRows {
		if runIDs[a.ExternalTxnID] {
			auditCount++
		}
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
func loadExternalTransactions(path, source string) ([]blnk.ExternalTransaction, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse csv: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("csv is empty")
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("csv has a header but no data rows")
	}

	col := make(map[string]int, len(records[0]))
	for i, h := range records[0] {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, required := range []string{"id", "amount"} {
		if _, ok := col[required]; !ok {
			return nil, fmt.Errorf("csv is missing required column %q", required)
		}
	}
	field := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		return ""
	}

	txns := make([]blnk.ExternalTransaction, 0, len(records)-1)
	for i, row := range records[1:] {
		lineNo := i + 2 // 1-based, accounting for the header row
		if isBlankRow(row) {
			continue // tolerate trailing blank lines
		}
		id := field(row, "id")
		if id == "" {
			return nil, fmt.Errorf("row %d has data but an empty id", lineNo)
		}
		amountStr := field(row, "amount")
		amount, aerr := strconv.ParseFloat(amountStr, 64)
		if aerr != nil {
			return nil, fmt.Errorf("row %d (%q) has an unparseable amount %q: %w", lineNo, id, amountStr, aerr)
		}
		date, derr := parseDate(field(row, "date"))
		if derr != nil {
			return nil, fmt.Errorf("row %d (%q) has an unparseable date: %w", lineNo, id, derr)
		}
		txns = append(txns, blnk.ExternalTransaction{
			ID:          id,
			Amount:      amount,
			Currency:    field(row, "currency"),
			Reference:   field(row, "reference"),
			Description: field(row, "description"),
			Date:        date,
			Source:      source,
		})
	}
	if len(txns) == 0 {
		return nil, fmt.Errorf("csv contains no data rows with a valid id")
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
	if status == statusAutoResolved {
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
