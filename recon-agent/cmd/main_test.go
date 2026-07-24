package main

// Unit tests for the recon-agent entrypoint pipeline. These are TRUE unit tests:
// they drive runPipeline (and its helpers) against an in-process httptest Blnk
// mock — exercising the REAL *blnk.Client (its JSON/multipart serialization,
// ephemeral-id probing, and reconciliation polling) — plus lightweight in-memory
// fakes for the remediator, audit sink, and store. No PostgreSQL, no live Blnk,
// and no LLM are required (finding M5). The tests assert the Blnk-native flow
// order (F1), fail-closed probe-error escalation (F2), strict CSV validation and
// nonzero-error propagation (F4), and the separate/idempotent resolved-artifact
// file with the committed-corpus guard (F6), plus dependency-readiness waiting
// (L1) and the CSV/date/summary helpers.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// ---------------------------------------------------------------------------
// In-process Blnk mock (drives the REAL *blnk.Client)
// ---------------------------------------------------------------------------

// mockBlnk is an httptest-backed stand-in for Blnk's /reconciliation/* surface.
// It records the ordered sequence of (method, normalized-path) requests so tests
// can assert the upload -> create-rule -> start -> get -> probe -> delete flow,
// and it decides each single-transaction probe's cleared/break verdict from the
// submitted transaction's Reference prefix:
//
//	Reference "MATCH*"    -> unmatched=0 (cleared, a match)
//	Reference "PROBEERR*" -> HTTP 500    (probe transport error -> fail-closed)
//	otherwise             -> unmatched=1 (a break)
type mockBlnk struct {
	srv *httptest.Server

	mu            sync.Mutex
	seq           []string
	probeDecision map[string]int
	ruleSeq       int
	probeSeq      int
	probeErrCount int
	deletedRules  []string

	// knobs (override default happy-path behavior)
	uploadStatus     int    // if non-zero, POST /upload returns this status
	uploadCountDelta int    // added to the true record_count
	startStatus      int    // if non-zero, POST /start returns this status
	createRuleStatus int    // if non-zero, POST /matching-rules returns this status
	mainReconStatus  string // status returned for the main reconciliation GET (default "completed")
}

func newMockBlnk() *mockBlnk {
	m := &mockBlnk{probeDecision: map[string]int{}}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockBlnk) close() { m.srv.Close() }

func (m *mockBlnk) record(method, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq = append(m.seq, method+" "+normalizePath(path))
}

func (m *mockBlnk) sequence() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.seq))
	copy(out, m.seq)
	return out
}

func normalizePath(p string) string {
	switch {
	case p == "/reconciliation/upload",
		p == "/reconciliation/start",
		p == "/reconciliation/start-instant",
		p == "/reconciliation/matching-rules",
		p == "/health":
		return p
	case strings.HasPrefix(p, "/reconciliation/matching-rules/"):
		return "/reconciliation/matching-rules/{id}"
	case strings.HasPrefix(p, "/reconciliation/"):
		return "/reconciliation/{id}"
	default:
		return p
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (m *mockBlnk) handle(w http.ResponseWriter, r *http.Request) {
	m.record(r.Method, r.URL.Path)
	path := r.URL.Path

	switch {
	case r.Method == http.MethodGet && path == "/health":
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPost && path == "/reconciliation/upload":
		if m.uploadStatus != 0 {
			w.WriteHeader(m.uploadStatus)
			return
		}
		count := m.countUploaded(r)
		writeJSON(w, http.StatusOK, blnk.UploadResponse{
			UploadID:    "upload-1",
			RecordCount: count + m.uploadCountDelta,
			Source:      r.FormValue("source"),
		})

	case r.Method == http.MethodPost && path == "/reconciliation/matching-rules":
		if m.createRuleStatus != 0 {
			w.WriteHeader(m.createRuleStatus)
			return
		}
		var rule blnk.MatchingRule
		_ = json.NewDecoder(r.Body).Decode(&rule)
		m.mu.Lock()
		m.ruleSeq++
		rule.RuleID = fmt.Sprintf("rule-%d", m.ruleSeq)
		m.mu.Unlock()
		writeJSON(w, http.StatusCreated, rule)

	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/reconciliation/matching-rules/"):
		m.mu.Lock()
		m.deletedRules = append(m.deletedRules, strings.TrimPrefix(path, "/reconciliation/matching-rules/"))
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{"message": "deleted"})

	case r.Method == http.MethodPost && path == "/reconciliation/start":
		if m.startStatus != 0 {
			w.WriteHeader(m.startStatus)
			return
		}
		writeJSON(w, http.StatusOK, blnk.StartReconciliationResponse{ReconciliationID: "main-recon-1"})

	case r.Method == http.MethodPost && path == "/reconciliation/start-instant":
		var req blnk.InstantReconciliationRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		ref := ""
		if len(req.ExternalTransactions) > 0 {
			ref = req.ExternalTransactions[0].Reference
		}
		if strings.HasPrefix(ref, "PROBEERR") {
			m.mu.Lock()
			m.probeErrCount++
			m.mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		unmatched := 1
		if strings.HasPrefix(ref, "MATCH") {
			unmatched = 0
		}
		m.mu.Lock()
		m.probeSeq++
		id := fmt.Sprintf("probe-%d", m.probeSeq)
		m.probeDecision[id] = unmatched
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, blnk.StartReconciliationResponse{ReconciliationID: id})

	case r.Method == http.MethodGet && strings.HasPrefix(path, "/reconciliation/"):
		id := strings.TrimPrefix(path, "/reconciliation/")
		if id == "main-recon-1" {
			status := m.mainReconStatus
			if status == "" {
				status = "completed"
			}
			writeJSON(w, http.StatusOK, blnk.Reconciliation{ReconciliationID: id, Status: status})
			return
		}
		m.mu.Lock()
		un := m.probeDecision[id]
		m.mu.Unlock()
		writeJSON(w, http.StatusOK, blnk.Reconciliation{
			ReconciliationID:      id,
			Status:                "completed",
			UnmatchedTransactions: un,
		})

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// countUploaded parses the multipart upload and returns the number of CSV data
// rows (excluding the header), mirroring Blnk's real record_count.
func (m *mockBlnk) countUploaded(r *http.Request) int {
	f, _, err := r.FormFile("file")
	if err != nil {
		return -1
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil || len(recs) < 1 {
		return 0
	}
	return len(recs) - 1
}

func (m *mockBlnk) client() *blnk.Client { return blnk.NewClient(m.srv.URL, "") }

// ---------------------------------------------------------------------------
// In-memory backend (implements pipelineStore + auditRecorder)
// ---------------------------------------------------------------------------

type fakeBackend struct {
	mu     sync.Mutex
	breaks map[string]store.Break
	hitl   []store.HITLItem
	audits []model.AuditEvent

	upsertErr  error
	enqueueErr error
	listErr    error
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{breaks: map[string]store.Break{}}
}

func (f *fakeBackend) UpsertBreak(_ context.Context, c model.BreakClassification, _ blnk.ExternalTransaction, status string) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.breaks[c.ExternalTxnID] = store.Break{Classification: c, Status: status}
	return nil
}

func (f *fakeBackend) EnqueueHITL(_ context.Context, externalTxnID, reason string) error {
	if f.enqueueErr != nil {
		return f.enqueueErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hitl = append(f.hitl, store.HITLItem{ExternalTxnID: externalTxnID, Reason: reason})
	return nil
}

func (f *fakeBackend) ListBreaks(context.Context) ([]store.Break, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.Break, 0, len(f.breaks))
	for _, b := range f.breaks {
		out = append(out, b)
	}
	return out, nil
}

func (f *fakeBackend) ListHITL(context.Context) ([]store.HITLItem, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.HITLItem, len(f.hitl))
	copy(out, f.hitl)
	return out, nil
}

func (f *fakeBackend) ListAudit(context.Context) ([]model.AuditEvent, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.AuditEvent, len(f.audits))
	copy(out, f.audits)
	return out, nil
}

func (f *fakeBackend) Record(_ context.Context, ev model.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, ev)
	return nil
}

// ---------------------------------------------------------------------------
// Fake remediator (models the real remediator's store/audit side effects)
// ---------------------------------------------------------------------------

type fakeRemediator struct {
	backend *fakeBackend
	handled []string
	err     error
}

func (r *fakeRemediator) Handle(ctx context.Context, txn blnk.ExternalTransaction, uploadID string) error {
	if r.err != nil {
		return r.err
	}
	r.handled = append(r.handled, txn.ID)
	// Simulate the real remediator: auto-resolve the break and emit a resolved
	// audit event so scopedSummary reflects it.
	cls := model.BreakClassification{ExternalTxnID: txn.ID, RootCause: model.RootCauseTiming, Confidence: 0.99}
	_ = r.backend.UpsertBreak(ctx, cls, txn, statusAutoResolved)
	_ = r.backend.Record(ctx, model.AuditEvent{
		ExternalTxnID: txn.ID,
		Actor:         "agent",
		Action:        "resolved",
		Provenance:    model.Provenance{UploadID: uploadID, ReconID: "recon-x"},
	})
	return nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func writeTempCSV(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "ext.csv")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	return p
}

const happyCSV = `ID,Amount,Currency,Reference,Description,Date
T1,100.00,USD,BREAK-1,timing break,2025-01-01
T2,200.00,USD,MATCH-1,matched,2025-01-02
T3,300.00,USD,BREAK-2,another break,2025-01-03T00:00:00Z
T4,400.00,USD,MATCH-2,matched,
`

// assertSubsequence asserts want appears, in order, as a subsequence of seq.
func assertSubsequence(t *testing.T, seq, want []string) {
	t.Helper()
	i := 0
	for _, s := range seq {
		if i < len(want) && s == want[i] {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("request sequence did not contain expected subsequence.\n want: %v\n got:  %v", want, seq)
	}
}

func originalPrefix(id string) string { return strings.SplitN(id, "-", 2)[0] }

// ---------------------------------------------------------------------------
// F1/F2/M1: the Blnk-native pipeline flow
// ---------------------------------------------------------------------------

func TestRunPipeline_DrivesBlnkNativeFlow(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}
	resolved := filepath.Join(t.TempDir(), "recon_resolved.jsonl")

	s, err := runPipeline(context.Background(), m.client(), rem, backend, backend, writeTempCSV(t, happyCSV), resolved, "seed-bank")
	if err != nil {
		t.Fatalf("runPipeline returned error: %v", err)
	}

	// F1: the required upload -> create-rule -> start -> get -> probe -> cleanup flow occurred, in order.
	assertSubsequence(t, m.sequence(), []string{
		"POST /reconciliation/upload",
		"POST /reconciliation/matching-rules",
		"POST /reconciliation/start",
		"GET /reconciliation/{id}",
		"POST /reconciliation/start-instant",
		"DELETE /reconciliation/matching-rules/{id}",
	})

	// The detection rule was deleted (finding F5 parity: no orphan rule).
	if len(m.deletedRules) != 1 {
		t.Fatalf("expected exactly 1 detection rule deleted, got %d (%v)", len(m.deletedRules), m.deletedRules)
	}

	// Exactly the two BREAK-* transactions were handed to the remediator (matches skipped).
	if len(rem.handled) != 2 {
		t.Fatalf("expected 2 breaks remediated, got %d (%v)", len(rem.handled), rem.handled)
	}
	got := map[string]bool{}
	for _, id := range rem.handled {
		got[originalPrefix(id)] = true
		if !strings.Contains(id, "-") {
			t.Fatalf("handled id %q is not run-scoped", id)
		}
	}
	if !got["T1"] || !got["T3"] || got["T2"] || got["T4"] {
		t.Fatalf("remediated the wrong transactions: %v", rem.handled)
	}

	// M1: summary scoped to this run.
	if s.breaksIn != 2 || s.autoResolved != 2 || s.escalated != 0 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	if s.auditCount != 2 {
		t.Fatalf("expected 2 run-scoped audit events, got %d", s.auditCount)
	}

	// F6: artifacts written to the separate file, committed corpus untouched.
	data, err := os.ReadFile(resolved)
	if err != nil {
		t.Fatalf("resolved artifacts not written: %v", err)
	}
	lines := nonEmptyLines(string(data))
	if len(lines) != 2 {
		t.Fatalf("expected 2 resolved-artifact lines, got %d", len(lines))
	}
	for _, ln := range lines {
		var rec evalRecord
		if err := json.Unmarshal([]byte(ln), &rec); err != nil {
			t.Fatalf("artifact line is not valid JSON: %v", err)
		}
		if rec.ID == "" || rec.ExpectedOutput.ExpectedAction != "auto_resolve" {
			t.Fatalf("unexpected artifact record: %+v", rec)
		}
	}
}

func TestRunPipeline_ProbeErrorFailsClosedToHITL(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}
	resolved := filepath.Join(t.TempDir(), "out.jsonl")

	csvBody := `ID,Amount,Currency,Reference,Description,Date
E1,100,USD,BREAK-1,real break,2025-01-01
E2,200,USD,PROBEERR-1,probe blows up,2025-01-02
`
	s, err := runPipeline(context.Background(), m.client(), rem, backend, backend, writeTempCSV(t, csvBody), resolved, "seed-bank")
	if err != nil {
		t.Fatalf("runPipeline returned error: %v", err)
	}

	// F2: the probe-error txn was routed to HITL (fail-closed), NOT remediated, NOT dropped.
	if len(rem.handled) != 1 || originalPrefix(rem.handled[0]) != "E1" {
		t.Fatalf("expected only E1 remediated, got %v", rem.handled)
	}
	if len(backend.hitl) != 1 || backend.hitl[0].Reason != "probe_error" || originalPrefix(backend.hitl[0].ExternalTxnID) != "E2" {
		t.Fatalf("expected E2 escalated to HITL with reason probe_error, got %+v", backend.hitl)
	}
	// An append-only escalation audit event was written for the fail-closed break.
	foundEscalation := false
	for _, a := range backend.audits {
		if a.Action == "escalated" && originalPrefix(a.ExternalTxnID) == "E2" {
			foundEscalation = true
		}
	}
	if !foundEscalation {
		t.Fatalf("expected an escalated audit event for E2, got %+v", backend.audits)
	}
	if s.breaksIn != 2 || s.autoResolved != 1 || s.escalated != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
}

// ---------------------------------------------------------------------------
// F4: strict validation & nonzero-error propagation
// ---------------------------------------------------------------------------

func TestRunPipeline_MissingCSVReturnsErrorAndCallsNoBlnk(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		filepath.Join(t.TempDir(), "does-not-exist.csv"), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error for a missing CSV, got nil")
	}
	if len(m.sequence()) != 0 {
		t.Fatalf("expected zero Blnk calls when the CSV is missing, got %v", m.sequence())
	}
}

func TestRunPipeline_UploadFailurePropagates(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	m.uploadStatus = http.StatusInternalServerError
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error when upload fails, got nil")
	}
	if len(rem.handled) != 0 {
		t.Fatalf("no breaks should be remediated when upload fails, got %v", rem.handled)
	}
}

func TestRunPipeline_UploadCountMismatchIsFatal(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	m.uploadCountDelta = 1 // report one more record than uploaded
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error on upload record_count mismatch, got nil")
	}
}

func TestRunPipeline_StartFailurePropagates(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	m.startStatus = http.StatusBadRequest
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error when start reconciliation fails, got nil")
	}
}

func TestRunPipeline_ReconFailedStatusIsFatal(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	m.mainReconStatus = "failed"
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error when the reconciliation reports failed, got nil")
	}
}

func TestRunPipeline_CreateRuleFailurePropagates(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	m.createRuleStatus = http.StatusBadRequest
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error when detection-rule creation fails, got nil")
	}
}

func TestRunPipeline_RemediatorErrorPropagates(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend, err: fmt.Errorf("boom")}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), "o.jsonl"), "seed-bank")
	if err == nil {
		t.Fatalf("expected error to propagate from the remediator, got nil")
	}
}

func TestRunPipeline_RejectsCommittedCorpusPath(t *testing.T) {
	m := newMockBlnk()
	defer m.close()
	backend := newFakeBackend()
	rem := &fakeRemediator{backend: backend}

	_, err := runPipeline(context.Background(), m.client(), rem, backend, backend,
		writeTempCSV(t, happyCSV), filepath.Join(t.TempDir(), committedCorpusBase), "seed-bank")
	if err == nil {
		t.Fatalf("expected runPipeline to refuse the committed corpus basename, got nil")
	}
}

func TestLoadExternalTransactions(t *testing.T) {
	valid := `ID,Amount,Currency,Reference,Description,Date
A1,10.50,USD,REF-1,desc,2025-01-01
A2,20,EUR,REF-2,desc,2025-01-02T10:00:00Z
`
	tests := []struct {
		name    string
		body    string
		wantErr bool
		wantN   int
	}{
		{"valid", valid, false, 2},
		{"empty file", "", true, 0},
		{"header only", "ID,Amount,Currency,Reference,Description,Date\n", true, 0},
		{"missing required column", "Amount,Currency\n10,USD\n", true, 0},
		{"unparseable amount", "ID,Amount\nA1,notanumber\n", true, 0},
		{"unparseable date", "ID,Amount,Date\nA1,10,31-13-2025\n", true, 0},
		{"empty id with data", "ID,Amount\n,10\n", true, 0},
		{"trailing blank line tolerated", "ID,Amount\nA1,10\n\n", false, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempCSV(t, tc.body)
			txns, err := loadExternalTransactions(path, "seed-bank")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (txns=%v)", txns)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(txns) != tc.wantN {
				t.Fatalf("expected %d txns, got %d", tc.wantN, len(txns))
			}
			for _, tx := range txns {
				if tx.Source != "seed-bank" {
					t.Fatalf("expected source stamped, got %q", tx.Source)
				}
			}
		})
	}
}

func TestLoadExternalTransactions_MissingFile(t *testing.T) {
	if _, err := loadExternalTransactions(filepath.Join(t.TempDir(), "nope.csv"), "s"); err == nil {
		t.Fatalf("expected error for a missing file")
	}
}

// ---------------------------------------------------------------------------
// F6: committed-corpus guard & idempotent resolved-artifact writer
// ---------------------------------------------------------------------------

func TestValidateResolvedPath(t *testing.T) {
	bad := []string{
		"recon_corpus.jsonl",
		"../eval/recon_corpus.jsonl",
		"/abs/path/eval/recon_corpus.jsonl",
		"./recon_corpus.jsonl",
	}
	for _, p := range bad {
		if err := validateResolvedPath(p); err == nil {
			t.Fatalf("expected %q to be rejected", p)
		}
	}
	good := []string{"recon_resolved.jsonl", "../out/run.jsonl", "/tmp/x.jsonl"}
	for _, p := range good {
		if err := validateResolvedPath(p); err != nil {
			t.Fatalf("expected %q to be allowed, got %v", p, err)
		}
	}
}

func TestWriteResolvedArtifacts_IdempotentAndSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recon_resolved.jsonl")
	txnByID := map[string]blnk.ExternalTransaction{
		"X1": {ID: "X1", Amount: 100, Currency: "USD", Reference: "R1"},
		"X2": {ID: "X2", Amount: 200, Currency: "EUR", Reference: "R2"},
	}
	breaks := []store.Break{
		{Classification: model.BreakClassification{ExternalTxnID: "X1", RootCause: model.RootCauseTiming}, Status: statusAutoResolved},
		{Classification: model.BreakClassification{ExternalTxnID: "X2", RootCause: model.RootCauseCurrencyMismatch, Regulated: true}, Status: statusQueued},
	}

	if err := writeResolvedArtifacts(path, breaks, txnByID); err != nil {
		t.Fatalf("first write: %v", err)
	}
	first, _ := os.ReadFile(path)
	if err := writeResolvedArtifacts(path, breaks, txnByID); err != nil {
		t.Fatalf("second write: %v", err)
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Fatalf("resolved-artifact writer is not idempotent")
	}

	lines := nonEmptyLines(string(second))
	if len(lines) != 2 {
		t.Fatalf("expected 2 records, got %d", len(lines))
	}
	actions := map[string]string{}
	for _, ln := range lines {
		var rec evalRecord
		if err := json.Unmarshal([]byte(ln), &rec); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		actions[rec.ID] = rec.ExpectedOutput.ExpectedAction
		if rec.JudgingCriteria == "" || rec.Scenario == "" {
			t.Fatalf("record missing required fields: %+v", rec)
		}
	}
	if actions["X1"] != "auto_resolve" || actions["X2"] != "escalate" {
		t.Fatalf("unexpected expected_action mapping: %v", actions)
	}
}

func TestWriteResolvedArtifacts_RefusesCommittedCorpus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, committedCorpusBase)
	err := writeResolvedArtifacts(path, []store.Break{
		{Classification: model.BreakClassification{ExternalTxnID: "X1"}, Status: statusAutoResolved},
	}, map[string]blnk.ExternalTransaction{"X1": {ID: "X1"}})
	if err == nil {
		t.Fatalf("expected refusal to write the committed corpus basename")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("committed-corpus file must not be created")
	}
}

func TestWriteResolvedArtifacts_NoBreaksNoFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recon_resolved.jsonl")
	if err := writeResolvedArtifacts(path, nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("no file should be created when there are no breaks")
	}
}

func TestExpectedActionFor(t *testing.T) {
	if expectedActionFor(statusAutoResolved) != "auto_resolve" {
		t.Fatalf("auto-resolved should map to auto_resolve")
	}
	for _, s := range []string{statusQueued, "rejected", "re_driven", ""} {
		if expectedActionFor(s) != "escalate" {
			t.Fatalf("status %q should map to escalate", s)
		}
	}
}

// ---------------------------------------------------------------------------
// L1: dependency-readiness waiting
// ---------------------------------------------------------------------------

type fakeProbe struct {
	failFirst int
	calls     int
	alwaysErr bool
}

func (p *fakeProbe) Ready(context.Context) error {
	p.calls++
	if p.alwaysErr {
		return fmt.Errorf("blnk down")
	}
	if p.calls <= p.failFirst {
		return fmt.Errorf("blnk warming up")
	}
	return nil
}

type fakePinger struct{ err error }

func (p *fakePinger) Ping(context.Context) error { return p.err }

func TestAwaitReadiness_SucceedsAfterRetry(t *testing.T) {
	old := readinessInterval
	readinessInterval = time.Millisecond
	defer func() { readinessInterval = old }()

	probe := &fakeProbe{failFirst: 2}
	if err := awaitReadiness(context.Background(), probe, &fakePinger{}, time.Second); err != nil {
		t.Fatalf("expected readiness after retries, got %v", err)
	}
	if probe.calls < 3 {
		t.Fatalf("expected at least 3 readiness attempts, got %d", probe.calls)
	}
}

func TestAwaitReadiness_TimesOut(t *testing.T) {
	old := readinessInterval
	readinessInterval = time.Millisecond
	defer func() { readinessInterval = old }()

	err := awaitReadiness(context.Background(), &fakeProbe{alwaysErr: true}, &fakePinger{}, 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected a timeout error, got %v", err)
	}
}

func TestAwaitReadiness_StoreNotReady(t *testing.T) {
	old := readinessInterval
	readinessInterval = time.Millisecond
	defer func() { readinessInterval = old }()

	err := awaitReadiness(context.Background(), &fakeProbe{}, &fakePinger{err: fmt.Errorf("db down")}, 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "store not ready") {
		t.Fatalf("expected a store-not-ready timeout error, got %v", err)
	}
}

func TestAwaitReadiness_ContextCancelled(t *testing.T) {
	old := readinessInterval
	readinessInterval = 50 * time.Millisecond
	defer func() { readinessInterval = old }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := awaitReadiness(ctx, &fakeProbe{alwaysErr: true}, &fakePinger{}, time.Second); err == nil {
		t.Fatalf("expected context cancellation error")
	}
}

// ---------------------------------------------------------------------------
// Helpers: date parsing, CSV round-trip, summary printing, config validation
// ---------------------------------------------------------------------------

func TestParseDate(t *testing.T) {
	if d, err := parseDate(""); err != nil || !d.IsZero() {
		t.Fatalf("empty should be zero time, got %v %v", d, err)
	}
	if _, err := parseDate("2025-03-04"); err != nil {
		t.Fatalf("date-only should parse: %v", err)
	}
	if _, err := parseDate("2025-03-04T12:30:00Z"); err != nil {
		t.Fatalf("rfc3339 should parse: %v", err)
	}
	if _, err := parseDate("not-a-date"); err == nil {
		t.Fatalf("garbage should error")
	}
}

func TestBuildStatementCSVRoundTrips(t *testing.T) {
	in := []blnk.ExternalTransaction{
		{ID: "T1", Amount: 100.5, Currency: "USD", Reference: "R1", Description: "d1", Date: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)},
		{ID: "T2", Amount: 200, Currency: "EUR", Reference: "R2", Description: "d2"},
	}
	csvText := buildStatementCSV(in)
	path := writeTempCSV(t, csvText)
	out, err := loadExternalTransactions(path, "seed-bank")
	if err != nil {
		t.Fatalf("round-trip load: %v", err)
	}
	if len(out) != 2 || out[0].ID != "T1" || out[0].Amount != 100.5 || out[0].Reference != "R1" {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
	if !out[0].Date.Equal(in[0].Date) {
		t.Fatalf("date not preserved: got %v want %v", out[0].Date, in[0].Date)
	}
}

func TestDetectionRuleGrammar(t *testing.T) {
	r := detectionRule("abc123")
	if len(r.Criteria) != 1 || r.Criteria[0].Field != fieldReference || r.Criteria[0].Operator != operatorEquals {
		t.Fatalf("detection rule violates the expected grammar: %+v", r)
	}
	if !strings.Contains(r.Name, "abc123") {
		t.Fatalf("detection rule name should be run-scoped: %q", r.Name)
	}
}

func TestShortRunIDUnique(t *testing.T) {
	a, b := shortRunID(), shortRunID()
	if a == "" || a == b {
		t.Fatalf("expected distinct non-empty run ids, got %q and %q", a, b)
	}
	if strings.Contains(a, "-") {
		t.Fatalf("short run id should have no dashes: %q", a)
	}
}

func TestPrintSummary(t *testing.T) {
	var sb strings.Builder
	printSummary(&sb, summary{breaksIn: 6, autoResolved: 2, escalated: 4, auditCount: 12})
	out := sb.String()
	for _, want := range []string{"breaks in", "auto-resolved", "escalated", "audit events", "6", "2", "4", "12"} {
		if !strings.Contains(out, want) {
			t.Fatalf("summary output missing %q:\n%s", want, out)
		}
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, ln := range strings.Split(strings.TrimSpace(s), "\n") {
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}
