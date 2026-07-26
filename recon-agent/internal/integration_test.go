//go:build integration

package internal_test

// Package internal_test hosts recon-agent's module-level, build-tag-guarded
// END-TO-END integration test (Gate 9: >=1 integration test; Gate 10: run by CI
// with PostgreSQL + Blnk + recon-agent stood up).
//
// WHY A BUILD TAG (`//go:build integration`):
//   - This is intentionally the ONLY .go file directly under recon-agent/internal/
//     (every other unit of code lives in a sub-package). Under the DEFAULT build
//     tags a plain `make test` (`go test ./...`) SILENTLY SKIPS this directory,
//     so the fast, service-free unit suites in the sub-packages (which carry the
//     >=80% coverage floor, Rule 5.9) run without any external services.
//   - Under `go test -tags integration ./...` this file compiles as the external
//     test package `internal_test` and TestReconAgentPipeline runs. That tagged
//     run is what `make test` step 2 and CI execute against a live stack.
//
// M-18 (final-delivery review). The previous revision drove SEVEN synthetic
// scenarios directly through remediator.Handle — bypassing cmd/main.go's batch
// upload -> start -> derive -> summary path — ran under a 120s budget, carried a
// stale flat proposed-rule shape and a stale 3-arg Handle call, and accepted an
// HTTP 502 from re_drive. This revision instead exercises the EXACT production
// demo path — `go run ./cmd -once`, precisely what `make demo` invokes — over
// the CANONICAL SIX breaks in seed/external_transactions.csv, under a hard
// <=60s deadline, and requires every measurable AAP success criterion:
//   * exactly six breaks ingested; exactly three auto-resolved and three
//     escalated (auto + escalated == 6);
//   * >=5/6 correct root-cause labels (the deterministic stub yields 6/6);
//   * >=1 auto-remediation confirmed by a Blnk dry-run (Rule 5.3: every
//     `resolved` audit event carries a confirming recon_id);
//   * routing safety (Rule 5.4): every regulated / low-confidence break is
//     escalated and NEVER auto-actioned;
//   * 100% audit coverage: every break emits >=1 append-only audit event and the
//     persisted audit-row count equals the run summary's audit_count;
//   * each HITL decision — accept / re_drive / reject (Gate 13) — is exercised
//     over HTTP and MUST return 200 (no 502 accepted).
//
// DESIGN — why subprocess the real binary:
//   cmd/main.go's pipeline lives in `package main` and is not importable, so the
//   most faithful "exact main/demo path" is to run the compiled command itself.
//   The test hosts a deterministic, offline, OpenAI-compatible stub LLM
//   (httptest) that the subprocess's real go-openai classifier calls
//   (LLM_BASE_URL points at it); the stub keys its canonical classification off
//   each break's STABLE base id (EXT-00N), which survives cmd's run-scoped id
//   rewrite (EXT-00N-<runID>). After the run, the test opens the agent store for
//   DB parity and stands up the REAL hitl server in-process (httptest) to drive
//   the three decisions over HTTP, exactly as cmd/main.go serve mode wires it.
//
// RULE 5.1 (native-API-only): this file imports NO github.com/blnkfinance/blnk
// package. It reaches Blnk only through recon-agent's own internal/blnk HTTP
// client (indirectly, via the subprocess and the in-process hitl server). The
// only direct HTTP it performs itself is (a) its own stub LLM and (b) an
// optional Blnk /health readiness poll in TestMain (readiness gating only).

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/hitl"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

const (
	// integrationReviewer is the human reviewer id attributed to the HITL
	// decisions this test submits over HTTP.
	integrationReviewer = "integration-reviewer"

	// uploadSourcePrefix seeds the per-run unique external-statement source
	// label. A UNIQUE source per run yields a unique run fixture key (cmd's
	// BeginRun), so every invocation processes a FRESH set of run-scoped breaks
	// against the shared Blnk database rather than replaying a prior run.
	uploadSourcePrefix = "integration-seed-bank"

	// demoDeadline is the hard wall-clock budget for the demo subprocess. M-18
	// requires the canonical run to complete in <=60s (matching `make demo`).
	demoDeadline = 60 * time.Second

	// blnkReadyTimeout bounds TestMain's best-effort Blnk readiness poll.
	blnkReadyTimeout = 90 * time.Second
)

// canonicalSpec is the deterministic classification the in-process stub LLM
// returns for a break, keyed by its STABLE base id (EXT-00N), plus the pipeline
// outcome the assertions expect. It mirrors seed/external_transactions.csv,
// seed/internal_ledger.json and eval/recon_corpus.jsonl, so the stub (what the
// model "says") and the assertions (what the pipeline must therefore do) share
// one source of truth and can never silently drift.
type canonicalSpec struct {
	// baseID is the stable external id (EXT-00N) the stub matches in the prompt.
	baseID string
	// rootCause is the label the stub returns (matches the eval corpus verbatim).
	rootCause string
	// confidence is the calibrated confidence the stub returns, in [0,1].
	confidence float64
	// regulated marks a break touching a regulated flow; it must NEVER be
	// auto-remediated regardless of confidence (Rule 5.4).
	regulated bool
	// expectAuto is true when the pipeline must auto-resolve this break and false
	// when it must escalate to the HITL queue.
	expectAuto bool
	// proposedRule is the Blnk-native matching rule the stub proposes. It MUST be
	// non-nil for an auto-eligible break (the remediator escalates a break whose
	// classification carries no proposed rule) and nil for a break that escalates.
	proposedRule *stubProposedRule
}

// stubCriterion / stubProposedRule / stubClassification mirror the classifier's
// strict on-the-wire decode types (classifier.rawCriterion / rawProposedRule /
// rawClassification) EXACTLY — same JSON field names, same Go types. The
// classifier decodes the stub's content with DisallowUnknownFields, so an extra
// or mistyped field would fail classification and route the break to HITL.
type stubCriterion struct {
	Field          string  `json:"field"`
	Operator       string  `json:"operator"`
	Value          string  `json:"value"`
	Pattern        string  `json:"pattern"`
	AllowableDrift float64 `json:"allowable_drift"`
}

type stubProposedRule struct {
	Criteria []stubCriterion `json:"criteria"`
}

type stubClassification struct {
	RootCause    string            `json:"root_cause"`
	Confidence   float64           `json:"confidence"`
	Regulated    bool              `json:"regulated"`
	ProposedRule *stubProposedRule `json:"proposed_rule,omitempty"`
	Rationale    string            `json:"rationale"`
}

// crit builds an in-grammar matching criterion (Rule 5.2:
// field in {amount,date,description,reference,currency}, operator in
// {equals,greater_than,less_than,contains}). drift is set only on an amount
// criterion; leave it 0 elsewhere.
func crit(field, operator, value string, drift float64) stubCriterion {
	return stubCriterion{Field: field, Operator: operator, Value: value, AllowableDrift: drift}
}

func autoRule(crits ...stubCriterion) *stubProposedRule {
	return &stubProposedRule{Criteria: crits}
}

// canonicalSpecs is the six-break ground truth. The three auto-eligible causes
// (timing / amount_drift / reference_mismatch) carry a valid proposed rule and
// must auto-resolve; duplicate / missing_internal escalate; currency_mismatch
// is BOTH high-confidence AND regulated and must escalate anyway (Rule 5.4).
var canonicalSpecs = []canonicalSpec{
	{
		baseID: "EXT-001", rootCause: "timing", confidence: 0.95, regulated: false, expectAuto: true,
		proposedRule: autoRule(
			crit(model.FieldAmount, model.OperatorEquals, "1500.00", 0),
			crit(model.FieldCurrency, model.OperatorEquals, "USD", 0),
			crit(model.FieldReference, model.OperatorEquals, "INV-1001", 0),
		),
	},
	{
		baseID: "EXT-002", rootCause: "amount_drift", confidence: 0.95, regulated: false, expectAuto: true,
		proposedRule: autoRule(
			crit(model.FieldAmount, model.OperatorEquals, "250.75", 0.01),
			crit(model.FieldCurrency, model.OperatorEquals, "USD", 0),
			crit(model.FieldReference, model.OperatorEquals, "INV-1002", 0),
		),
	},
	{
		baseID: "EXT-003", rootCause: "reference_mismatch", confidence: 0.92, regulated: false, expectAuto: true,
		proposedRule: autoRule(
			crit(model.FieldAmount, model.OperatorEquals, "980.00", 0),
			crit(model.FieldCurrency, model.OperatorEquals, "USD", 0),
		),
	},
	{baseID: "EXT-004", rootCause: "duplicate", confidence: 0.90, regulated: false, expectAuto: false, proposedRule: nil},
	{baseID: "EXT-005", rootCause: "missing_internal", confidence: 0.90, regulated: false, expectAuto: false, proposedRule: nil},
	{baseID: "EXT-006", rootCause: "currency_mismatch", confidence: 0.92, regulated: true, expectAuto: false, proposedRule: nil},
}

// baseIDRe extracts a stable canonical base id (EXT-001..EXT-006) from a
// run-scoped id (EXT-00N-<runID>) or from a classifier prompt. The run id
// segment is 8 lowercase-hex chars and can never contain "EXT-", and no CSV
// field carries an "EXT-00N" token, so the match is unambiguous.
var baseIDRe = regexp.MustCompile(`EXT-00[1-6]`)

func baseID(runScoped string) string {
	if m := baseIDRe.FindString(runScoped); m != "" {
		return m
	}
	return runScoped
}

func specForPrompt(prompt string) (canonicalSpec, bool) {
	m := baseIDRe.FindString(prompt)
	if m == "" {
		return canonicalSpec{}, false
	}
	for _, s := range canonicalSpecs {
		if s.baseID == m {
			return s, true
		}
	}
	return canonicalSpec{}, false
}

// newStubLLM starts an in-process, deterministic, OpenAI-compatible Chat
// Completions stub. The demo subprocess's real go-openai classifier POSTs to it
// (its LLM_BASE_URL points here); the stub extracts the break's stable base id
// from the prompt and returns the canonical classification as the completion
// content. Because a real, reachable endpoint always answers, the fail-closed
// path never fires and the run is fully deterministic and offline (Rule 5.6).
func newStubLLM(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	// Catch-all: any POST (e.g. /v1/chat/completions) is a classification call.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var cls stubClassification
		if spec, ok := specForPrompt(string(body)); ok {
			cls = stubClassification{
				RootCause:    spec.rootCause,
				Confidence:   spec.confidence,
				Regulated:    spec.regulated,
				ProposedRule: spec.proposedRule,
				Rationale:    "deterministic stub classification for " + spec.baseID,
			}
		} else {
			// Unrecognized break: fail closed (unknown / zero confidence) so the
			// remediator escalates rather than auto-acting on an unmapped break.
			cls = stubClassification{RootCause: "unknown", Confidence: 0, Rationale: "unrecognized break"}
		}
		content, _ := json.Marshal(cls)
		resp := map[string]any{
			"id":      "stub-cmpl",
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   "kimi-k3",
			"choices": []map[string]any{{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": string(content)},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// moduleRoot resolves the repository root and the recon-agent module directory
// from this test file's own location, so the subprocess calls are independent
// of the caller's working directory.
func moduleRoot(t *testing.T) (repoRoot, reconAgentDir string) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller could not locate the test source file")
	internalDir := filepath.Dir(thisFile)     // <root>/recon-agent/internal
	reconAgentDir = filepath.Dir(internalDir) // <root>/recon-agent
	repoRoot = filepath.Dir(reconAgentDir)    // <root>
	return repoRoot, reconAgentDir
}

// mergeEnv returns os.Environ() with every key present in overrides REMOVED and
// then re-appended from overrides, guaranteeing exactly one occurrence per
// overridden key. This avoids the ambiguity of duplicate env entries (glibc
// getenv returns the first match; some runtimes the last), so the child process
// deterministically observes the values this test intends.
func mergeEnv(overrides map[string]string) []string {
	base := os.Environ()
	out := make([]string, 0, len(base)+len(overrides))
	for _, kv := range base {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		if _, shadowed := overrides[key]; shadowed {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range overrides {
		out = append(out, k+"="+v)
	}
	return out
}

// runGo runs `go <args>` in dir with the given environment overrides, capturing
// combined stdout+stderr for diagnostics.
func runGo(ctx context.Context, dir string, overrides map[string]string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	cmd.Env = mergeEnv(overrides)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return buf.String(), err
}

// resolvedRecord is the subset of the per-break JSONL artifact this test reads.
// The artifact's `expected_output` records the ACTUAL realized outcome of the
// run (root cause, regulated flag, and the action the pipeline took).
type resolvedRecord struct {
	ID             string `json:"id"`
	ExpectedOutput struct {
		RootCause      string `json:"root_cause"`
		Regulated      bool   `json:"regulated"`
		ExpectedAction string `json:"expected_action"`
	} `json:"expected_output"`
}

// summaryReport mirrors cmd's machine-readable one-shot run summary.
type summaryReport struct {
	BreaksIn     int `json:"breaks_in"`
	AutoResolved int `json:"auto_resolved"`
	Escalated    int `json:"escalated"`
	AuditCount   int `json:"audit_count"`
}

func readSummary(t *testing.T, path string) summaryReport {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoErrorf(t, err, "reading run summary %s", path)
	var s summaryReport
	require.NoErrorf(t, json.Unmarshal(raw, &s), "decoding run summary %s", path)
	return s
}

func readResolved(t *testing.T, path string) []resolvedRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoErrorf(t, err, "reading resolved artifact %s", path)
	var out []resolvedRecord
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec resolvedRecord
		require.NoErrorf(t, json.Unmarshal([]byte(line), &rec), "decoding resolved record %q", line)
		out = append(out, rec)
	}
	return out
}

// postDecision POSTs a JSON HITL decision to <baseURL>/decisions and asserts the
// response status. A JSON content-type with no cross-origin header satisfies the
// server's mutation guard (same-origin JSON needs no CSRF token).
func postDecision(ctx context.Context, t *testing.T, baseURL string, d model.HITLDecision, want int) {
	t.Helper()
	body, err := json.Marshal(d)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/decisions", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	require.Equalf(t, want, resp.StatusCode,
		"POST /decisions %s/%s -> %d (want %d); body=%s", d.ExternalTxnID, d.Decision, resp.StatusCode, want, string(msg))
}

func assertGet(ctx context.Context, t *testing.T, url string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equalf(t, want, resp.StatusCode, "GET %s -> %d (want %d)", url, resp.StatusCode, want)
}

func requireStatus(ctx context.Context, t *testing.T, st *store.Store, id, want string) {
	t.Helper()
	_, status, _, found, err := st.LoadBreak(ctx, id)
	require.NoError(t, err)
	require.Truef(t, found, "break %s not found in store", baseID(id))
	require.Equalf(t, want, status, "break %s status", baseID(id))
}

func requireAudit(ctx context.Context, t *testing.T, st *store.Store, id, action string) {
	t.Helper()
	n, err := st.CountAuditByAction(ctx, id, action)
	require.NoError(t, err)
	require.GreaterOrEqualf(t, n, 1, "break %s must have a %q audit event", baseID(id), action)
}

// assertResolvedCarryReconID proves Rule 5.3 at the DB level: every `resolved`
// audit event for this run carries a non-empty confirming Blnk recon_id. It uses
// a scoped raw query (lib/pq, already a store dependency) because the store's
// paginated audit list orders ASC and cannot cheaply target the newest run in a
// shared database.
func assertResolvedCarryReconID(ctx context.Context, t *testing.T, dsn string, runIDs []string) {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx,
		`SELECT external_txn_id, provenance->>'recon_id'
		   FROM agent.agent_audit
		  WHERE action = $1 AND external_txn_id = ANY($2)`,
		model.ActionResolved, pq.Array(runIDs))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var id string
		var reconID sql.NullString
		require.NoError(t, rows.Scan(&id, &reconID))
		require.Truef(t, reconID.Valid && strings.TrimSpace(reconID.String) != "",
			"resolved audit event for %s must carry a Blnk recon_id (Rule 5.3)", baseID(id))
		n++
	}
	require.NoError(t, rows.Err())
	require.GreaterOrEqual(t, n, 3, "all three auto-resolved breaks must have a resolved event with a recon_id")
}

// TestMain optionally waits for the live Blnk daemon to become reachable before
// running the suite (readiness gating only): a best-effort GET of
// <BLNK_BASE_URL>/health, treating ANY HTTP response as "up". When BLNK_BASE_URL
// is unset the test itself skips, so this is a no-op.
func TestMain(m *testing.M) {
	if base := strings.TrimSpace(os.Getenv("BLNK_BASE_URL")); base != "" {
		waitForBlnk(base, blnkReadyTimeout)
	}
	os.Exit(m.Run())
}

func waitForBlnk(baseURL string, timeout time.Duration) {
	url := strings.TrimRight(baseURL, "/") + "/health"
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestReconAgentPipeline drives the canonical six breaks through the exact
// production demo path and asserts every AAP success criterion, DB parity, and
// the three HITL decisions end-to-end. It requires a live Blnk daemon and the
// agent PostgreSQL database (BLNK_BASE_URL + AGENT_DATABASE_URL); otherwise it
// skips (the unit suites carry the coverage floor without external services).
func TestReconAgentPipeline(t *testing.T) {
	blnkBase := strings.TrimSpace(os.Getenv("BLNK_BASE_URL"))
	agentDSN := strings.TrimSpace(os.Getenv("AGENT_DATABASE_URL"))
	if blnkBase == "" || agentDSN == "" {
		t.Skip("integration env not configured (requires BLNK_BASE_URL and AGENT_DATABASE_URL)")
	}
	blnkKey := strings.TrimSpace(os.Getenv("BLNK_API_KEY"))

	repoRoot, reconAgentDir := moduleRoot(t)

	// (1) Deterministic, offline LLM the subprocess classifier will call.
	stub := newStubLLM(t)
	llmBaseURL := stub.URL + "/v1"

	// A unique per-run source => unique run fixture key => a FRESH set of
	// run-scoped breaks (never a replay of a prior run) against the shared Blnk DB.
	source := fmt.Sprintf("%s-%d", uploadSourcePrefix, time.Now().UnixNano())

	// (2) Establish internal ledger state (idempotent) via the seed program over
	//     Blnk's public HTTP API — exactly as `make seed` does. Without this the
	//     external statement has nothing to reconcile against.
	seedCtx, cancelSeed := context.WithTimeout(context.Background(), demoDeadline)
	defer cancelSeed()
	if out, err := runGo(seedCtx, repoRoot, map[string]string{
		"BLNK_BASE_URL": blnkBase,
		"BLNK_API_KEY":  blnkKey,
	}, "run", "./seed"); err != nil {
		t.Fatalf("seed (go run ./seed) failed: %v\n%s", err, out)
	}

	// (3) Run the EXACT demo path: `go run ./cmd -once` (what `make demo`
	//     invokes) under a hard <=60s deadline, with artifacts isolated to a temp
	//     dir so the shared working tree is untouched. -csv defaults to
	//     ../seed/external_transactions.csv (the canonical six).
	artifactDir := t.TempDir()
	resolvedPath := filepath.Join(artifactDir, "recon_resolved.jsonl")
	summaryPath := filepath.Join(artifactDir, "recon_summary.json")

	demoCtx, cancelDemo := context.WithTimeout(context.Background(), demoDeadline)
	defer cancelDemo()
	start := time.Now()
	out, err := runGo(demoCtx, reconAgentDir, map[string]string{
		"AGENT_DATABASE_URL":  agentDSN,
		"BLNK_BASE_URL":       blnkBase,
		"BLNK_API_KEY":        blnkKey,
		"LLM_BASE_URL":        llmBaseURL,
		"LLM_API_KEY":         "stub-key",
		"LLM_MODEL":           "kimi-k3",
		"CONF_AUTO_THRESHOLD": "0.85",
		"HITL_PORT":           "8088",
	}, "run", "./cmd", "-once", "-source", source, "-resolved", resolvedPath)
	elapsed := time.Since(start)
	t.Logf("demo (go run ./cmd -once) completed in %s; output:\n%s", elapsed, out)
	require.NoErrorf(t, demoCtx.Err(), "demo did not complete within the %s budget (M-18)", demoDeadline)
	require.NoErrorf(t, err, "demo `go run ./cmd -once` failed:\n%s", out)
	require.Lessf(t, elapsed, demoDeadline, "demo exceeded the %s budget (M-18)", demoDeadline)

	// (4) Machine-readable acceptance: the run summary + per-break artifact.
	summary := readSummary(t, summaryPath)
	records := readResolved(t, resolvedPath)

	require.Equal(t, 6, summary.BreaksIn, "exactly six canonical breaks must be ingested")
	require.Equal(t, 3, summary.AutoResolved, "exactly three breaks must auto-resolve")
	require.Equal(t, 3, summary.Escalated, "exactly three breaks must escalate")
	require.Equal(t, summary.BreaksIn, summary.AutoResolved+summary.Escalated,
		"every break must be either auto-resolved or escalated")
	require.GreaterOrEqual(t, summary.AuditCount, summary.BreaksIn,
		"every break must emit at least one audit event")
	require.Len(t, records, 6, "resolved artifact must carry one record per break")

	byBase := make(map[string]resolvedRecord, 6)
	runIDs := make([]string, 0, 6)
	for _, r := range records {
		byBase[baseID(r.ID)] = r
		runIDs = append(runIDs, r.ID)
	}
	require.Len(t, byBase, 6, "resolved records must cover all six distinct canonical breaks")

	// (5) Label accuracy (AAP: >=5/6) and routing safety (Rule 5.4).
	correct, autoCount := 0, 0
	for _, spec := range canonicalSpecs {
		rec, ok := byBase[spec.baseID]
		require.Truef(t, ok, "break %s missing from resolved artifact", spec.baseID)
		if rec.ExpectedOutput.RootCause == spec.rootCause {
			correct++
		}
		if spec.expectAuto {
			require.Equalf(t, "auto_resolve", rec.ExpectedOutput.ExpectedAction, "break %s must auto-resolve", spec.baseID)
			autoCount++
		} else {
			require.Equalf(t, "escalate", rec.ExpectedOutput.ExpectedAction, "break %s must escalate", spec.baseID)
		}
		if spec.regulated {
			require.Equalf(t, "escalate", rec.ExpectedOutput.ExpectedAction,
				"regulated break %s must NEVER auto-resolve (Rule 5.4)", spec.baseID)
			require.Truef(t, rec.ExpectedOutput.Regulated, "regulated break %s must be flagged regulated", spec.baseID)
		}
	}
	require.GreaterOrEqual(t, correct, 5, "at least five of six root-cause labels must be correct (AAP)")
	require.GreaterOrEqual(t, autoCount, 1, "at least one break must be auto-resolved (AAP)")

	// (6) DB parity via the agent store, scoped to THIS run's ids.
	st, err := store.New(agentDSN)
	require.NoError(t, err)
	defer func() { _ = st.Close() }()
	ctx := context.Background()

	nAuto, err := st.CountBreaksByStatusForIDs(ctx, runIDs, model.StatusAutoResolved)
	require.NoError(t, err)
	require.Equal(t, 3, nAuto, "three breaks must be persisted as auto-resolved")
	nQueued, err := st.CountBreaksByStatusForIDs(ctx, runIDs, model.StatusQueued)
	require.NoError(t, err)
	require.Equal(t, 3, nQueued, "three breaks must be persisted as queued")

	nHITL, err := st.CountHITLForIDs(ctx, runIDs)
	require.NoError(t, err)
	require.Equal(t, 3, nHITL, "three breaks must be routed to the HITL queue")

	nAudit, err := st.CountAuditForIDs(ctx, runIDs)
	require.NoError(t, err)
	require.Equal(t, summary.AuditCount, nAudit,
		"the run summary audit_count must equal the persisted audit rows for this run")
	for _, id := range runIDs { // 100% coverage: every break has >=1 audit event.
		n, err := st.CountAuditForIDs(ctx, []string{id})
		require.NoError(t, err)
		require.GreaterOrEqualf(t, n, 1, "break %s must have at least one audit event", baseID(id))
	}

	// (7) Rule 5.3 (deterministic arbiter): each auto-resolved break has a
	//     `resolved` audit event carrying a confirming Blnk recon_id.
	for _, spec := range canonicalSpecs {
		if !spec.expectAuto {
			continue
		}
		requireAudit(ctx, t, st, byBase[spec.baseID].ID, model.ActionResolved)
	}
	assertResolvedCarryReconID(ctx, t, agentDSN, runIDs)

	// (8) HITL invocation over HTTP (Gate 13): accept / re_drive / reject are
	//     each exercised against a distinct queued break and MUST return 200
	//     (no 502 accepted — M-18). The real hitl server is stood up in-process,
	//     wired exactly as cmd/main.go serve mode wires it.
	cfg := config.Config{
		LLMModel:          "kimi-k3",
		BlnkBaseURL:       blnkBase,
		BlnkApiKey:        blnkKey,
		ConfAutoThreshold: 0.85,
		HitlPort:          "0",
		AgentDatabaseURL:  agentDSN,
	}
	aud, err := audit.New(st)
	require.NoError(t, err)
	bc := blnk.NewClient(blnkBase, blnkKey)
	srv := hitl.NewServer(cfg, st, aud, bc)
	hitlHTTP := httptest.NewServer(srv.Router())
	defer hitlHTTP.Close()

	assertGet(ctx, t, hitlHTTP.URL+"/healthz", http.StatusOK)
	assertGet(ctx, t, hitlHTTP.URL+"/breaks", http.StatusOK)

	// Distinct queued breaks for the three decisions (all escalated above).
	reDriveID := byBase["EXT-004"].ID // duplicate
	acceptID := byBase["EXT-006"].ID  // regulated — a human may still accept it
	rejectID := byBase["EXT-005"].ID  // missing_internal — no internal booking

	// re_drive first (needs the break queued). Requires 200: the probe completes
	// (buildReDriveRule matches amount+currency), no Blnk upstream error, and the
	// Blnk dry-run CONFIRMS clearance. Post-F16 the handler no longer emits the
	// generic `re_driven` action: a confirmed-clearing re_drive transitions the
	// break to StatusReDriven and records BOTH the durable early
	// `re_drive_attempted` event and the terminal `re_drive_cleared` outcome
	// (the latter carrying the confirming Blnk recon_id — Rule 5.3 / finding F16).
	postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{
		ExternalTxnID: reDriveID, Decision: "re_drive", Reviewer: integrationReviewer,
	}, http.StatusOK)
	requireStatus(ctx, t, st, reDriveID, model.StatusReDriven)
	requireAudit(ctx, t, st, reDriveID, model.ActionReDriveAttempted)
	requireAudit(ctx, t, st, reDriveID, model.ActionReDriveCleared)

	postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{
		ExternalTxnID: acceptID, Decision: "accept", Reviewer: integrationReviewer,
	}, http.StatusOK)
	requireStatus(ctx, t, st, acceptID, model.StatusAccepted)
	requireAudit(ctx, t, st, acceptID, model.ActionAccepted)

	postDecision(ctx, t, hitlHTTP.URL, model.HITLDecision{
		ExternalTxnID: rejectID, Decision: "reject", Reviewer: integrationReviewer,
	}, http.StatusOK)
	requireStatus(ctx, t, st, rejectID, model.StatusRejected)
	requireAudit(ctx, t, st, rejectID, model.ActionRejected)
}
