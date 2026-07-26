package blnk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "test-blnk-key"

// requireBlnkKey fails the test unless the X-Blnk-Key header carries testKey.
func requireBlnkKey(t *testing.T, r *http.Request) {
	t.Helper()
	if got := r.Header.Get(keyHeader); got != testKey {
		t.Fatalf("missing/incorrect %s header: got %q want %q", keyHeader, got, testKey)
	}
}

// errReader is an io.Reader that always fails, used to exercise the upload
// file-copy error branch.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestNewClientTrimsTrailingSlash(t *testing.T) {
	c := NewClient("http://example.test/", testKey)
	if c.baseURL != "http://example.test" {
		t.Fatalf("baseURL not trimmed: %q", c.baseURL)
	}
	if c.apiKey != testKey {
		t.Fatalf("apiKey mismatch: %q", c.apiKey)
	}
	if c.probeInterval != defaultProbeInterval || c.probeTimeout != defaultProbeTimeout {
		t.Fatalf("probe defaults not set: %v %v", c.probeInterval, c.probeTimeout)
	}
	if c.httpClient == nil {
		t.Fatal("httpClient is nil")
	}
}

func TestUploadExternalData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodPost || r.URL.Path != routeUpload {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatalf("parse multipart: %v", err)
		}
		if got := r.FormValue("source"); got != "bank-x" {
			t.Fatalf("source field: got %q", got)
		}
		f, hdr, err := r.FormFile("file")
		if err != nil {
			t.Fatalf("form file: %v", err)
		}
		defer func() { _ = f.Close() }()
		if hdr.Filename != "stmt.csv" {
			t.Fatalf("filename: got %q", hdr.Filename)
		}
		b, _ := io.ReadAll(f)
		if string(b) != "ID,Amount\n1,10\n" {
			t.Fatalf("file body: got %q", string(b))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(UploadResponse{UploadID: "up_1", RecordCount: 6, Source: "bank-x"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	out, err := c.UploadExternalData(context.Background(), "bank-x", "stmt.csv", strings.NewReader("ID,Amount\n1,10\n"))
	if err != nil {
		t.Fatalf("UploadExternalData: %v", err)
	}
	if out.UploadID != "up_1" || out.RecordCount != 6 || out.Source != "bank-x" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestUploadExternalDataErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.UploadExternalData(context.Background(), "s", "f.csv", strings.NewReader("x")); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestUploadExternalDataFileReadError(t *testing.T) {
	c := NewClient("http://example.test", testKey)
	_, err := c.UploadExternalData(context.Background(), "s", "f.csv", errReader{})
	if err == nil || !strings.Contains(err.Error(), "copy file contents") {
		t.Fatalf("expected copy error, got %v", err)
	}
}

func TestUploadExternalDataRequestBuildError(t *testing.T) {
	c := NewClient("http://%zz", testKey) // invalid URL escaping
	if _, err := c.UploadExternalData(context.Background(), "s", "f.csv", strings.NewReader("x")); err == nil {
		t.Fatal("expected request-build error")
	}
}

func TestStartReconciliation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodPost || r.URL.Path != routeStart {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type: %q", ct)
		}
		var req StartReconciliationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if req.UploadID != "up_1" || req.Strategy != "one_to_one" || !req.DryRun {
			t.Fatalf("bad request: %+v", req)
		}
		if len(req.MatchingRuleIDs) != 1 || req.MatchingRuleIDs[0] != "rule_1" {
			t.Fatalf("bad matching_rule_ids: %+v", req.MatchingRuleIDs)
		}
		_ = json.NewEncoder(w).Encode(StartReconciliationResponse{ReconciliationID: "recon_1"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	id, err := c.StartReconciliation(context.Background(), StartReconciliationRequest{
		UploadID: "up_1", Strategy: "one_to_one", DryRun: true, MatchingRuleIDs: []string{"rule_1"},
	})
	if err != nil {
		t.Fatalf("StartReconciliation: %v", err)
	}
	if id != "recon_1" {
		t.Fatalf("id: got %q", id)
	}
}

func TestStartReconciliationErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.StartReconciliation(context.Background(), StartReconciliationRequest{}); err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestGetReconciliation(t *testing.T) {
	completed := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodGet || r.URL.Path != routeReconByID+"recon_1" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(Reconciliation{
			ReconciliationID:      "recon_1",
			Status:                statusCompleted,
			MatchedTransactions:   5,
			UnmatchedTransactions: 1,
			IsDryRun:              true,
			CompletedAt:           &completed,
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	recon, err := c.GetReconciliation(context.Background(), "recon_1")
	if err != nil {
		t.Fatalf("GetReconciliation: %v", err)
	}
	if recon.MatchedTransactions != 5 || recon.UnmatchedTransactions != 1 || recon.Status != statusCompleted {
		t.Fatalf("unexpected recon: %+v", recon)
	}
}

func TestGetReconciliationErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.GetReconciliation(context.Background(), "nope"); err == nil {
		t.Fatal("expected error on 404")
	}
}

func TestGetReconciliationBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "{not-json")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.GetReconciliation(context.Background(), "recon_1"); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestGetReconciliationRequestBuildError(t *testing.T) {
	c := NewClient("http://%zz", testKey)
	if _, err := c.GetReconciliation(context.Background(), "recon_1"); err == nil {
		t.Fatal("expected request-build error")
	}
}

func TestConnectionError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // server is now down; the Do call must fail

	c := NewClient(url, testKey)
	if _, err := c.GetReconciliation(context.Background(), "recon_1"); err == nil {
		t.Fatal("expected connection error")
	}
}

func TestCreateMatchingRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodPost || r.URL.Path != routeMatchingRules {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var rule MatchingRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if rule.Name != "amount-eq" || len(rule.Criteria) != 1 {
			t.Fatalf("bad rule: %+v", rule)
		}
		if rule.Criteria[0].Field != "amount" || rule.Criteria[0].Operator != "equals" {
			t.Fatalf("bad criteria: %+v", rule.Criteria[0])
		}
		rule.RuleID = "rule_1"
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(rule)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	out, err := c.CreateMatchingRule(context.Background(), MatchingRule{
		Name:     "amount-eq",
		Criteria: []MatchingCriteria{{Field: "amount", Operator: "equals", Value: "10"}},
	})
	if err != nil {
		t.Fatalf("CreateMatchingRule: %v", err)
	}
	if out.RuleID != "rule_1" {
		t.Fatalf("rule id: got %q", out.RuleID)
	}
}

func TestCreateMatchingRuleWrongStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 200 instead of the required 201.
		_ = json.NewEncoder(w).Encode(MatchingRule{RuleID: "rule_1"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.CreateMatchingRule(context.Background(), MatchingRule{}); err == nil {
		t.Fatal("expected error when status is not 201")
	}
}

func TestUpdateMatchingRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodPut || r.URL.Path != routeMatchingRules+"/rule_1" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var rule MatchingRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			t.Fatalf("decode: %v", err)
		}
		rule.RuleID = "rule_1"
		_ = json.NewEncoder(w).Encode(rule)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	out, err := c.UpdateMatchingRule(context.Background(), "rule_1", MatchingRule{Name: "updated"})
	if err != nil {
		t.Fatalf("UpdateMatchingRule: %v", err)
	}
	if out.RuleID != "rule_1" || out.Name != "updated" {
		t.Fatalf("unexpected rule: %+v", out)
	}
}

func TestUpdateMatchingRuleErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.UpdateMatchingRule(context.Background(), "rule_1", MatchingRule{}); err == nil {
		t.Fatal("expected error on 500")
	}
}

// probeServer builds a mock Blnk exposing start-instant + GET, returning the
// supplied status/matched-count for the reconciliation and honoring an optional
// number of "started" polls before completion. It ALWAYS reports a nonzero
// UnmatchedTransactions (contamination by unrelated internal bookings, as a real
// many_to_one run does) so the tests prove ProbeBreak's verdict keys off
// matched>=1 and IGNORES unmatched.
func probeServer(t *testing.T, status string, matched int, startedPolls int32) *httptest.Server {
	var gets int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == routeStartInstant:
			var req InstantReconciliationRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode instant: %v", err)
			}
			if !req.DryRun {
				t.Fatal("ProbeBreak must set dry_run=true")
			}
			if len(req.ExternalTransactions) != 1 {
				t.Fatalf("ProbeBreak must submit exactly one txn, got %d", len(req.ExternalTransactions))
			}
			// F3/F8: ProbeBreak MUST submit a FRESH ephemeral id (never the
			// caller's real external id) so the same break can be probed
			// repeatedly — during detection then auto-remediation confirm, and
			// across reruns against a shared Blnk database — without colliding on
			// Blnk's external_transactions_pkey (which would return HTTP 500).
			if got := req.ExternalTransactions[0].ID; !strings.HasPrefix(got, "probe-") {
				t.Fatalf("ProbeBreak must submit an ephemeral probe- id, got %q", got)
			}
			// F02/F18/Rule 5.8: the per-break probe MUST use the cache-safe
			// many_to_one strategy with reference grouping so sequential per-break
			// probes never collide on Blnk's upload-agnostic pagination cache.
			// probeStrategy is defined as StrategyManyToOne in client.go; asserting
			// the wire value equals the exported many_to_one literal pins that.
			if req.Strategy != StrategyManyToOne {
				t.Fatalf("strategy: got %q want %q (many_to_one)", req.Strategy, StrategyManyToOne)
			}
			if req.GroupingCriteria != probeGroupingCriteria {
				t.Fatalf("grouping_criteria: got %q want %q", req.GroupingCriteria, probeGroupingCriteria)
			}
			if len(req.MatchingRuleIDs) != 1 || req.MatchingRuleIDs[0] != "rule_1" {
				t.Fatalf("matching rule ids: %+v", req.MatchingRuleIDs)
			}
			_ = json.NewEncoder(w).Encode(StartReconciliationResponse{ReconciliationID: "recon_probe"})
		case r.Method == http.MethodGet && r.URL.Path == routeReconByID+"recon_probe":
			n := atomic.AddInt32(&gets, 1)
			st := status
			if n <= startedPolls {
				st = "started" // not yet terminal
			}
			_ = json.NewEncoder(w).Encode(Reconciliation{
				ReconciliationID:      "recon_probe",
				Status:                st,
				MatchedTransactions:   matched,
				UnmatchedTransactions: 3, // contamination: proves verdict ignores unmatched
				IsDryRun:              true,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
}

func TestProbeBreakCleared(t *testing.T) {
	srv := probeServer(t, statusCompleted, 1, 0) // matched=1 => cleared
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}
	if !cleared {
		t.Fatal("expected cleared=true for matched>=1")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID: got %q", reconID)
	}
}

func TestProbeBreakStillBreak(t *testing.T) {
	srv := probeServer(t, statusCompleted, 0, 0) // matched=0 => still a break
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}
	if cleared {
		t.Fatal("expected cleared=false for matched=0")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID: got %q", reconID)
	}
}

func TestProbeBreakPollsUntilComplete(t *testing.T) {
	srv := probeServer(t, statusCompleted, 1, 2) // matched=1; two "started" polls, then completed
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	c.probeInterval = time.Millisecond // speed up polling for the test
	cleared, _, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}
	if !cleared {
		t.Fatal("expected cleared=true after polling to completion")
	}
}

func TestProbeBreakFailed(t *testing.T) {
	srv := probeServer(t, statusFailed, 0, 0)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err == nil {
		t.Fatal("expected error on failed reconciliation")
	}
	if cleared {
		t.Fatal("cleared must be false on failure")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID should still be returned: got %q", reconID)
	}
}

func TestProbeBreakTimeout(t *testing.T) {
	srv := probeServer(t, statusCompleted, 1, 1<<30) // never terminal within the window
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	c.probeInterval = time.Millisecond
	c.probeTimeout = 20 * time.Millisecond
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err == nil || !strings.Contains(err.Error(), "did not complete") {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if cleared {
		t.Fatal("cleared must be false on timeout")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID should still be returned: got %q", reconID)
	}
}

func TestProbeBreakInstantError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err == nil || !strings.Contains(err.Error(), "probe start-instant") {
		t.Fatalf("expected start-instant error, got %v", err)
	}
	if cleared || reconID != "" {
		t.Fatalf("expected zero values on start error: cleared=%v reconID=%q", cleared, reconID)
	}
}

// TestProbeBreakUsesEphemeralID pins the F3/F8 double-persist elimination: the
// probe submitted to Blnk must carry a FRESH ephemeral id distinct from the
// caller's real external id, while preserving every matchable field so Blnk's
// field-to-field verdict is unchanged. It also asserts the caller's own txn
// value is left unmodified (the ephemeral id is applied to a copy).
func TestProbeBreakUsesEphemeralID(t *testing.T) {
	txnDate := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	input := ExternalTransaction{
		ID:          "EXT-001",
		Amount:      1500,
		Reference:   "INV-1001",
		Currency:    "USD",
		Description: "wire settlement",
		Date:        txnDate,
		Source:      "bank_statement",
	}

	var submitted ExternalTransaction
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == routeStartInstant:
			var req InstantReconciliationRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode instant: %v", err)
			}
			if len(req.ExternalTransactions) != 1 {
				t.Fatalf("expected exactly one txn, got %d", len(req.ExternalTransactions))
			}
			submitted = req.ExternalTransactions[0]
			_ = json.NewEncoder(w).Encode(StartReconciliationResponse{ReconciliationID: "recon_probe"})
		case r.Method == http.MethodGet && r.URL.Path == routeReconByID+"recon_probe":
			_ = json.NewEncoder(w).Encode(Reconciliation{
				ReconciliationID:      "recon_probe",
				Status:                statusCompleted,
				MatchedTransactions:   1,
				UnmatchedTransactions: 0,
				IsDryRun:              true,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, _, err := c.ProbeBreak(context.Background(), input, []string{"rule_1"}); err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}

	// The submitted id must be a fresh ephemeral id, never the caller's id.
	if submitted.ID == input.ID {
		t.Fatalf("probe reused the caller's external id %q; expected a fresh ephemeral id", input.ID)
	}
	if !strings.HasPrefix(submitted.ID, "probe-") {
		t.Fatalf("expected ephemeral probe- id, got %q", submitted.ID)
	}
	// Every matchable field must be preserved so the verdict is invariant.
	if submitted.Amount != input.Amount ||
		submitted.Reference != input.Reference ||
		submitted.Currency != input.Currency ||
		submitted.Description != input.Description ||
		!submitted.Date.Equal(input.Date) ||
		submitted.Source != input.Source {
		t.Fatalf("matchable fields not preserved: got %+v want (matchable of) %+v", submitted, input)
	}
	// The caller's original txn value must be untouched.
	if input.ID != "EXT-001" {
		t.Fatalf("caller txn id was mutated: %q", input.ID)
	}
}

func TestDeleteMatchingRule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if r.Method != http.MethodDelete || r.URL.Path != routeMatchingRules+"/rule_1" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "deleted"})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if err := c.DeleteMatchingRule(context.Background(), "rule_1"); err != nil {
		t.Fatalf("DeleteMatchingRule: %v", err)
	}
}

func TestDeleteMatchingRuleErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if err := c.DeleteMatchingRule(context.Background(), "rule_1"); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestJSONMethodsRequestBuildError(t *testing.T) {
	c := NewClient("http://%zz", testKey) // invalid URL escaping trips request build
	ctx := context.Background()
	if _, err := c.StartReconciliation(ctx, StartReconciliationRequest{}); err == nil {
		t.Fatal("StartReconciliation: expected request-build error")
	}
	if _, err := c.InstantReconciliation(ctx, InstantReconciliationRequest{}); err == nil {
		t.Fatal("InstantReconciliation: expected request-build error")
	}
	if _, err := c.CreateMatchingRule(ctx, MatchingRule{}); err == nil {
		t.Fatal("CreateMatchingRule: expected request-build error")
	}
	if _, err := c.UpdateMatchingRule(ctx, "rule_1", MatchingRule{}); err == nil {
		t.Fatal("UpdateMatchingRule: expected request-build error")
	}
	if err := c.DeleteMatchingRule(ctx, "rule_1"); err == nil {
		t.Fatal("DeleteMatchingRule: expected request-build error")
	}
}

func TestNewJSONRequestMarshalError(t *testing.T) {
	c := NewClient("http://example.test", testKey)
	if _, err := c.newJSONRequest(context.Background(), http.MethodPost, "/x", make(chan int)); err == nil {
		t.Fatal("expected marshal error for unsupported type")
	}
}

func TestNewJSONRequestBadMethod(t *testing.T) {
	c := NewClient("http://example.test", testKey)
	if _, err := c.newJSONRequest(context.Background(), "bad method", "/x", nil); err == nil {
		t.Fatal("expected error for invalid method")
	}
}

// TestReadyProbesHealthAndAuthenticates asserts F13: Ready probes Blnk's
// purpose-built GET /health liveness endpoint (which never logs an error on the
// Blnk side, unlike the former sentinel-reconciliation-id probe) with the
// X-Blnk-Key attached, and treats 200 + an "UP" status body as ready.
func TestReadyProbesHealthAndAuthenticates(t *testing.T) {
	var sawHealth, sawKey bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == routeHealth {
			sawHealth = true
		}
		// F13: Ready must NOT request a (nonexistent) reconciliation id, which is
		// what made Blnk log a spurious ERROR on every boot.
		if strings.HasPrefix(r.URL.Path, routeReconByID) && r.URL.Path != routeHealth {
			t.Errorf("Ready must NOT probe a reconciliation id (finding F13); got %s", r.URL.Path)
		}
		if r.Header.Get(keyHeader) == testKey {
			sawKey = true
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if err := c.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() = %v, want nil for a healthy Blnk", err)
	}
	if !sawHealth {
		t.Error("Ready did not probe GET /health")
	}
	if !sawKey {
		t.Error("Ready did not attach the X-Blnk-Key header")
	}
}

// TestReadyRejectsUnhealthyAndErrors asserts F13/F08 posture: a degraded Blnk
// (503 {"status":"DOWN"}), an auth failure (401/403), a bad request (400), a
// server error (5xx), or a 200 whose body is not UP is NOT treated as ready.
func TestReadyRejectsUnhealthyAndErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"down", http.StatusServiceUnavailable, `{"status":"DOWN"}`},
		{"unauthorized", http.StatusUnauthorized, ``},
		{"forbidden", http.StatusForbidden, ``},
		{"badRequest", http.StatusBadRequest, ``},
		{"serverError", http.StatusInternalServerError, ``},
		{"badGateway", http.StatusBadGateway, ``},
		{"okButNotUp", http.StatusOK, `{"status":"STARTING"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			c := NewClient(srv.URL, testKey)
			if err := c.Ready(context.Background()); err == nil {
				t.Fatalf("Ready() = nil, want error for %s (status %d body %q)", tc.name, tc.status, tc.body)
			}
		})
	}
}

// TestReadyTransportError asserts a down Blnk is reported not-ready.
func TestReadyTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()

	c := NewClient(url, testKey)
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("Ready() = nil, want transport error against a down server")
	}
}

// TestClientRefusesCrossOriginRedirect asserts M-06: a cross-origin redirect is
// refused and the custom X-Blnk-Key is never delivered to the foreign origin.
func TestClientRefusesCrossOriginRedirect(t *testing.T) {
	var foreignHit int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&foreignHit, 1)
		if r.Header.Get(keyHeader) != "" {
			t.Errorf("cross-origin request leaked %s header", keyHeader)
		}
		_ = json.NewEncoder(w).Encode(Reconciliation{ReconciliationID: "recon_1"})
	}))
	defer foreign.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL+r.URL.Path, http.StatusFound)
	}))
	defer origin.Close()

	c := NewClient(origin.URL, testKey)
	if _, err := c.GetReconciliation(context.Background(), "recon_1"); err == nil {
		t.Fatal("expected error refusing cross-origin redirect")
	}
	if n := atomic.LoadInt32(&foreignHit); n != 0 {
		t.Fatalf("foreign origin received %d request(s); redirect must not be followed", n)
	}
}

// TestClientFollowsSameOriginRedirect asserts M-06: a same-origin redirect is
// still followed to completion.
func TestClientFollowsSameOriginRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		if strings.HasSuffix(r.URL.Path, "final") {
			_ = json.NewEncoder(w).Encode(Reconciliation{ReconciliationID: "recon_1", Status: statusCompleted})
			return
		}
		http.Redirect(w, r, r.URL.Path+"-final", http.StatusFound)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	recon, err := c.GetReconciliation(context.Background(), "recon_1")
	if err != nil {
		t.Fatalf("same-origin redirect not followed: %v", err)
	}
	if recon.ReconciliationID != "recon_1" {
		t.Fatalf("unexpected recon after redirect: %+v", recon)
	}
}

// TestResponseBodyBounded asserts M-06: the client reads/decodes at most
// maxRespBytes, so an over-limit body is truncated (here forcing a decode error)
// rather than fully buffered.
func TestResponseBodyBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A valid, large JSON object whose byte length far exceeds the tiny cap.
		_, _ = io.WriteString(w, `{"reconciliation_id":"`+strings.Repeat("x", 4096)+`"}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	c.maxRespBytes = 16 // truncate well before the object closes
	if _, err := c.GetReconciliation(context.Background(), "recon_1"); err == nil {
		t.Fatal("expected decode error from truncated (bounded) body")
	}
}

// -----------------------------------------------------------------------------
// EstablishMainReconciliation (finding F02, Rule 5.3).
//
// This exercises the pipeline's INITIAL reconciliation over the already-uploaded
// statement — the mandated Upload -> start -> read topology (F02). It POSTs
// /reconciliation/start for the upload with the cache-safe many_to_one strategy,
// reference grouping and dry_run=true (never mutating Blnk state, Rule 5.3),
// polls GET /reconciliation/:id to a terminal status, and returns the completed
// reconciliation's id (the "main_recon_id") that callers persist as each break's
// batch-reconciliation provenance. Blnk requires >=1 matching rule id for both
// /reconciliation/start and /reconciliation/start-instant, so an empty upload id
// or an empty rule set is rejected before any HTTP call, and a start error, a
// failed run or a timeout surfaces as an error rather than a silent success.
// -----------------------------------------------------------------------------

// mainReconMock returns an httptest server answering the two routes
// EstablishMainReconciliation drives (POST /reconciliation/start, then GET
// /reconciliation/<id>). Successive GET calls return statuses[i] (the last
// element repeats once exhausted). It captures the decoded start request into
// *gotReq for assertion. The reconciliation always reports NONZERO counts to
// prove EstablishMainReconciliation ignores them (it returns purely on terminal
// status, F02), unlike the per-break ProbeBreak verdict.
func mainReconMock(t *testing.T, reconID string, statuses []string, gotReq *StartReconciliationRequest) *httptest.Server {
	t.Helper()
	var gets int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == routeStart:
			if gotReq != nil {
				if err := json.NewDecoder(r.Body).Decode(gotReq); err != nil {
					t.Fatalf("decode start body: %v", err)
				}
			}
			_ = json.NewEncoder(w).Encode(StartReconciliationResponse{ReconciliationID: reconID})
		case r.Method == http.MethodGet && r.URL.Path == routeReconByID+reconID:
			n := atomic.AddInt32(&gets, 1)
			st := statuses[len(statuses)-1]
			if int(n) <= len(statuses) {
				st = statuses[n-1]
			}
			_ = json.NewEncoder(w).Encode(Reconciliation{
				ReconciliationID:      reconID,
				Status:                st,
				MatchedTransactions:   2,
				UnmatchedTransactions: 4, // counts are irrelevant to Establish; only status matters
				IsDryRun:              true,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
}

func TestEstablishMainReconciliationSuccess(t *testing.T) {
	// The initial run must POST /reconciliation/start for the upload as a DRY RUN
	// carrying the cache-safe many_to_one strategy, reference grouping and the
	// supplied rule ids, poll GET to a terminal status, and return the completed
	// reconciliation id (the main_recon_id, F02).
	var gotReq StartReconciliationRequest
	srv := mainReconMock(t, "recon_main_ok", []string{statusCompleted}, &gotReq)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	reconID, err := c.EstablishMainReconciliation(context.Background(), "up_main", []string{"rule_a", "rule_b"})
	if err != nil {
		t.Fatalf("EstablishMainReconciliation: %v", err)
	}
	if reconID != "recon_main_ok" {
		t.Fatalf("main recon id: got %q want recon_main_ok", reconID)
	}
	if !gotReq.DryRun {
		t.Fatal("EstablishMainReconciliation must run a DRY RUN (Rule 5.3), never mutate Blnk state")
	}
	if gotReq.UploadID != "up_main" {
		t.Fatalf("upload id: got %q want up_main", gotReq.UploadID)
	}
	// F02/F18/Rule 5.8: the initial run MUST use the SAME cache-safe many_to_one
	// strategy + reference grouping as the per-break probes, so the real upload's
	// run cannot poison the per-break probes' disjoint upload-scoped cache entries.
	// probeStrategy is defined as StrategyManyToOne in client.go; asserting the
	// wire value equals the exported many_to_one literal pins that invariant.
	if gotReq.Strategy != StrategyManyToOne {
		t.Fatalf("strategy: got %q want %q (many_to_one)", gotReq.Strategy, StrategyManyToOne)
	}
	if gotReq.GroupingCriteria != probeGroupingCriteria {
		t.Fatalf("grouping_criteria: got %q want %q", gotReq.GroupingCriteria, probeGroupingCriteria)
	}
	if len(gotReq.MatchingRuleIDs) != 2 {
		t.Fatalf("matching rule ids: got %v want 2 ids", gotReq.MatchingRuleIDs)
	}
}

func TestEstablishMainReconciliationPollsUntilTerminal(t *testing.T) {
	// First GET is non-terminal ("processing"); the poll loop must try again and
	// only return once Blnk reports a terminal "completed".
	srv := mainReconMock(t, "recon_main_poll", []string{"processing", statusCompleted}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	c.probeInterval = time.Millisecond // speed up polling for the test
	reconID, err := c.EstablishMainReconciliation(context.Background(), "up_main", []string{"rule_a"})
	if err != nil {
		t.Fatalf("EstablishMainReconciliation: %v", err)
	}
	if reconID != "recon_main_poll" {
		t.Fatalf("main recon id: got %q want recon_main_poll", reconID)
	}
}

func TestEstablishMainReconciliationEmptyUploadErrors(t *testing.T) {
	// An empty (whitespace-only) upload id is rejected BEFORE any HTTP call: a
	// missing initial reconciliation must surface, never be silently skipped (F02).
	c := NewClient("http://unused.invalid", testKey)
	if _, err := c.EstablishMainReconciliation(context.Background(), "  ", []string{"rule_a"}); err == nil {
		t.Fatal("an empty upload id must be rejected with an error")
	}
}

func TestEstablishMainReconciliationEmptyRulesErrors(t *testing.T) {
	// Blnk rejects /reconciliation/start with no matching_rule_ids, so the client
	// fails fast (before any HTTP call) when the caller supplies none.
	c := NewClient("http://unused.invalid", testKey)
	if _, err := c.EstablishMainReconciliation(context.Background(), "up_main", nil); err == nil {
		t.Fatal("an empty matching_rule_ids set must be rejected with an error")
	}
}

func TestEstablishMainReconciliationFailedStatusErrors(t *testing.T) {
	// A Blnk run that reports status=failed must surface an error while still
	// returning the reconciliation id for diagnostics.
	srv := mainReconMock(t, "recon_main_fail", []string{statusFailed}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	reconID, err := c.EstablishMainReconciliation(context.Background(), "up_main", []string{"rule_a"})
	if err == nil {
		t.Fatal("a reconciliation that reports status=failed must surface an error")
	}
	if reconID != "recon_main_fail" {
		t.Fatalf("recon id should still be returned on failure: got %q", reconID)
	}
}

func TestEstablishMainReconciliationStartError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	if _, err := c.EstablishMainReconciliation(context.Background(), "up_main", []string{"rule_a"}); err == nil {
		t.Fatal("a failed /reconciliation/start must abort EstablishMainReconciliation with an error")
	}
}

func TestEstablishMainReconciliationContextCancel(t *testing.T) {
	// GET never reaches a terminal status, so the bounded poll must give up when
	// the caller's context deadline elapses rather than blocking forever.
	srv := mainReconMock(t, "recon_main_hang", []string{"processing"}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	c.probeInterval = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.EstablishMainReconciliation(ctx, "up_main", []string{"rule_a"}); err == nil {
		t.Fatal("EstablishMainReconciliation must error when the context deadline elapses before completion")
	}
}
