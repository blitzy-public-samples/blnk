package blnk

import (
	"context"
	"encoding/json"
	"fmt"
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
// supplied status/unmatched for the reconciliation and honoring an optional
// number of "started" polls before completion.
func probeServer(t *testing.T, status string, unmatched int, startedPolls int32) *httptest.Server {
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
			if req.Strategy != probeStrategy {
				t.Fatalf("strategy: got %q want %q", req.Strategy, probeStrategy)
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
				UnmatchedTransactions: unmatched,
				IsDryRun:              true,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
}

func TestProbeBreakCleared(t *testing.T) {
	srv := probeServer(t, statusCompleted, 0, 0)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}
	if !cleared {
		t.Fatal("expected cleared=true for unmatched=0")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID: got %q", reconID)
	}
}

func TestProbeBreakStillBreak(t *testing.T) {
	srv := probeServer(t, statusCompleted, 1, 0)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ProbeBreak(context.Background(), ExternalTransaction{ID: "ext_1"}, []string{"rule_1"})
	if err != nil {
		t.Fatalf("ProbeBreak: %v", err)
	}
	if cleared {
		t.Fatal("expected cleared=false for unmatched=1")
	}
	if reconID != "recon_probe" {
		t.Fatalf("reconID: got %q", reconID)
	}
}

func TestProbeBreakPollsUntilComplete(t *testing.T) {
	srv := probeServer(t, statusCompleted, 0, 2) // two "started" polls, then completed
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
	srv := probeServer(t, statusCompleted, 0, 1<<30) // never terminal within the window
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

// TestReadyStaysOnReconRouteAndAuthenticates asserts M-05: Ready probes the
// mandated /reconciliation/* surface (never /health) with the X-Blnk-Key
// attached, and treats 200 and 404 as ready.
func TestReadyStaysOnReconRouteAndAuthenticates(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var sawReconRoute, sawKey bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/reconciliation/") {
					sawReconRoute = true
				}
				if r.URL.Path == "/health" {
					t.Errorf("Ready must NOT call /health (Rule 5.1); got %s", r.URL.Path)
				}
				if r.Header.Get(keyHeader) == testKey {
					sawKey = true
				}
				w.WriteHeader(status)
			}))
			defer srv.Close()

			c := NewClient(srv.URL, testKey)
			if err := c.Ready(context.Background()); err != nil {
				t.Fatalf("Ready() = %v, want nil for status %d", err, status)
			}
			if !sawReconRoute {
				t.Error("Ready did not probe a /reconciliation/* route")
			}
			if !sawKey {
				t.Error("Ready did not attach the X-Blnk-Key header")
			}
		})
	}
}

// TestReadyRejectsAuthAndServerErrors asserts M-05: an auth failure (401/403), a
// bad request (400), or a server error (5xx) is NOT treated as ready.
func TestReadyRejectsAuthAndServerErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusUnauthorized, http.StatusForbidden,
		http.StatusBadRequest, http.StatusInternalServerError, http.StatusBadGateway,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			c := NewClient(srv.URL, testKey)
			if err := c.Ready(context.Background()); err == nil {
				t.Fatalf("Ready() = nil, want error for status %d", status)
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
// ConfirmCohortCleared (finding F-1, Rule 5.3).
//
// This exercises the per-break-attributable, cache-safe deterministic-arbiter
// seam that replaced the earlier aggregate ConfirmClearedOverUpload. It submits
// the eligible cohort inline via POST /reconciliation/start-instant with FRESH
// per-run "cohort-" ids and dry_run=true (never mutating Blnk), polls GET to a
// terminal status, and reports cleared ONLY when Blnk's authoritative counts show
// unmatched==0 AND matched+unmatched==len(cohort). Because the dedicated upload
// contains ONLY the cohort, unmatched==0 proves EVERY member left the unmatched
// set, so a resolution is attributable per break (Rule 5.3); the cardinality
// guard fails closed if a stale-cache read (protected-core defect INFO#4)
// returns a different-cardinality page.
// -----------------------------------------------------------------------------

// cohortMock returns an httptest server answering the two routes
// ConfirmCohortCleared drives (POST /reconciliation/start-instant, then GET
// /reconciliation/<id>). Successive GET calls return statuses[i] (the last
// element repeats once exhausted); the reconciliation reports matched/unmatched.
// It captures the decoded start-instant request into *gotReq for assertion.
func cohortMock(t *testing.T, reconID string, matched, unmatched int, statuses []string, gotReq *InstantReconciliationRequest) *httptest.Server {
	t.Helper()
	var getCalls int
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requireBlnkKey(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == routeStartInstant:
			if gotReq != nil {
				if err := json.NewDecoder(r.Body).Decode(gotReq); err != nil {
					t.Fatalf("decode start-instant body: %v", err)
				}
			}
			_ = json.NewEncoder(w).Encode(StartReconciliationResponse{ReconciliationID: reconID})
		case r.Method == http.MethodGet && r.URL.Path == routeReconByID+reconID:
			st := statuses[len(statuses)-1]
			if getCalls < len(statuses) {
				st = statuses[getCalls]
			}
			getCalls++
			_ = json.NewEncoder(w).Encode(Reconciliation{
				ReconciliationID:      reconID,
				Status:                st,
				MatchedTransactions:   matched,
				UnmatchedTransactions: unmatched,
				IsDryRun:              true,
			})
		default:
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
	}))
}

func cohortOf(n int) []ExternalTransaction {
	cohort := make([]ExternalTransaction, n)
	for i := range cohort {
		cohort[i] = ExternalTransaction{ID: fmt.Sprintf("ext_%d", i), Amount: 100, Currency: "USD"}
	}
	return cohort
}

func TestConfirmCohortClearedAllCleared(t *testing.T) {
	// A 3-member cohort whose dedicated dry-run reports matched=3, unmatched=0:
	// every member left the unmatched set => cleared (Rule 5.3), attributable per
	// break because the dedicated upload contained ONLY these three rows.
	var gotReq InstantReconciliationRequest
	srv := cohortMock(t, "recon_cohort_ok", 3, 0, []string{statusCompleted}, &gotReq)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, reconID, err := c.ConfirmCohortCleared(context.Background(), cohortOf(3), []string{"rule_a", "rule_b", "rule_c"})
	if err != nil {
		t.Fatalf("ConfirmCohortCleared: %v", err)
	}
	if !cleared {
		t.Fatal("unmatched==0 and matched+unmatched==len(cohort) must report cleared=true")
	}
	if reconID != "recon_cohort_ok" {
		t.Fatalf("confirming recon id: got %q want recon_cohort_ok", reconID)
	}
	// Rule 5.3 / F3/F8: the cohort dry-run MUST be a DRY RUN carrying the proposed
	// rules and FRESH ephemeral "cohort-" ids (never the caller's real external
	// ids, which would collide on Blnk's external_transactions_pkey across reruns).
	if !gotReq.DryRun {
		t.Fatal("ConfirmCohortCleared must run a DRY RUN (Rule 5.3), never mutate Blnk state")
	}
	if gotReq.Strategy != probeStrategy {
		t.Fatalf("strategy: got %q want %q", gotReq.Strategy, probeStrategy)
	}
	if len(gotReq.ExternalTransactions) != 3 {
		t.Fatalf("cohort size submitted: got %d want 3", len(gotReq.ExternalTransactions))
	}
	for _, txn := range gotReq.ExternalTransactions {
		if !strings.HasPrefix(txn.ID, "cohort-") {
			t.Fatalf("cohort member must carry a fresh cohort- id, got %q", txn.ID)
		}
	}
	if len(gotReq.MatchingRuleIDs) != 3 {
		t.Fatalf("matching rule ids: got %v want 3 ids", gotReq.MatchingRuleIDs)
	}
}

func TestConfirmCohortClearedNotClearedWhenUnmatched(t *testing.T) {
	// A misclassified no-counterpart break in the cohort cannot clear: the
	// dedicated dry-run reports unmatched>0, so the WHOLE cohort fails closed
	// (cleared=false). LLM confidence must never substitute for this Blnk verdict.
	srv := cohortMock(t, "recon_cohort_no", 2, 1, []string{statusCompleted}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, _, err := c.ConfirmCohortCleared(context.Background(), cohortOf(3), []string{"rule_a", "rule_b", "rule_c"})
	if err != nil {
		t.Fatalf("ConfirmCohortCleared: %v", err)
	}
	if cleared {
		t.Fatal("unmatched(1) > 0 must report cleared=false (fail closed)")
	}
}

func TestConfirmCohortClearedCardinalityGuardFailsClosed(t *testing.T) {
	// INFO#4 defense: a stale-cache read returns a DIFFERENT-cardinality page
	// (matched=6, unmatched=0 for a 3-member cohort). Even though unmatched==0,
	// matched+unmatched(6) != len(cohort)(3), so the cardinality guard fails
	// closed rather than trusting an unattributable result.
	srv := cohortMock(t, "recon_cohort_stale", 6, 0, []string{statusCompleted}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, _, err := c.ConfirmCohortCleared(context.Background(), cohortOf(3), []string{"rule_a"})
	if err != nil {
		t.Fatalf("ConfirmCohortCleared: %v", err)
	}
	if cleared {
		t.Fatal("a different-cardinality (stale-cache) read must fail the guard closed (cleared=false)")
	}
}

func TestConfirmCohortClearedPollsUntilTerminal(t *testing.T) {
	// First GET is non-terminal ("processing"); the loop must poll again on the
	// ticker and only return once Blnk reports a terminal "completed".
	srv := cohortMock(t, "recon_cohort_poll", 2, 0, []string{"processing", statusCompleted}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, _, err := c.ConfirmCohortCleared(context.Background(), cohortOf(2), []string{"rule_a"})
	if err != nil {
		t.Fatalf("ConfirmCohortCleared: %v", err)
	}
	if !cleared {
		t.Fatal("matched=2 unmatched=0 for a 2-member cohort must report cleared=true")
	}
}

func TestConfirmCohortClearedEmptyCohortErrors(t *testing.T) {
	c := NewClient("http://unused.invalid", testKey)
	if _, _, err := c.ConfirmCohortCleared(context.Background(), nil, []string{"rule_a"}); err == nil {
		t.Fatal("an empty cohort must be rejected with an error, never treated as cleared")
	}
}

func TestConfirmCohortClearedFailedStatusErrors(t *testing.T) {
	srv := cohortMock(t, "recon_cohort_fail", 0, 0, []string{statusFailed}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, _, err := c.ConfirmCohortCleared(context.Background(), cohortOf(1), []string{"rule_a"})
	if err == nil {
		t.Fatal("a Blnk reconciliation that reports status=failed must surface an error")
	}
	if cleared {
		t.Fatal("a failed reconciliation must never report cleared=true")
	}
}

func TestConfirmCohortClearedStartError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	cleared, _, err := c.ConfirmCohortCleared(context.Background(), cohortOf(1), nil)
	if err == nil {
		t.Fatal("a failed start-instant must abort ConfirmCohortCleared with an error")
	}
	if cleared {
		t.Fatal("an errored confirmation must never report cleared=true")
	}
}

func TestConfirmCohortClearedContextCancel(t *testing.T) {
	// GET never reaches a terminal status, so the bounded poll must give up when
	// the caller's context deadline elapses rather than blocking forever.
	srv := cohortMock(t, "recon_cohort_hang", 1, 0, []string{"processing"}, nil)
	defer srv.Close()

	c := NewClient(srv.URL, testKey)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := c.ConfirmCohortCleared(ctx, cohortOf(1), nil); err == nil {
		t.Fatal("ConfirmCohortCleared must error when the context deadline elapses before completion")
	}
}
