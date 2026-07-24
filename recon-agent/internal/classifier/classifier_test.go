package classifier

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	openai "github.com/sashabaranov/go-openai"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
)

const testModel = "kimi-k3-unit-test"

func sampleTxn() blnk.ExternalTransaction {
	return blnk.ExternalTransaction{
		ID:          "ext-001",
		Amount:      100.25,
		Currency:    "USD",
		Reference:   "INV-1001",
		Description: "Vendor payment",
		Date:        time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		Source:      "bank-a",
	}
}

// chatResponseBody marshals an OpenAI-compatible chat-completion response whose
// single choice carries the given assistant content.
func chatResponseBody(t *testing.T, content string) []byte {
	t.Helper()
	resp := openai.ChatCompletionResponse{
		ID:    "cmpl-test",
		Model: testModel,
		Choices: []openai.ChatCompletionChoice{
			{Index: 0, Message: openai.ChatCompletionMessage{Role: openai.ChatMessageRoleAssistant, Content: content}},
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return b
}

// newTestClassifier wires a Classifier at an httptest server via the real
// constructor, so the genuine go-openai client is exercised end to end.
func newTestClassifier(serverURL string) *Classifier {
	return New(config.Config{
		LLMBaseURL: serverURL,
		LLMApiKey:  "test-key",
		LLMModel:   testModel,
	})
}

func TestClassify_SuccessParsesAndAttachesValidRule(t *testing.T) {
	content := `{
		"root_cause": "reference_mismatch",
		"confidence": 0.92,
		"regulated": false,
		"proposed_rule": {"field": "Reference", "operator": "Equals", "value": "INV-1001"},
		"rationale": "reference differs only by casing"
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, content))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	txn := sampleTxn()
	got, err := c.Classify(context.Background(), txn)
	if err != nil {
		t.Fatalf("Classify returned error: %v", err)
	}
	if got.ExternalTxnID != txn.ID {
		t.Errorf("ExternalTxnID = %q, want %q (must come from txn, not model)", got.ExternalTxnID, txn.ID)
	}
	if got.RootCause != model.RootCauseReferenceMismatch {
		t.Errorf("RootCause = %q, want %q", got.RootCause, model.RootCauseReferenceMismatch)
	}
	if got.Confidence != 0.92 {
		t.Errorf("Confidence = %v, want 0.92", got.Confidence)
	}
	if got.Regulated {
		t.Errorf("Regulated = true, want false")
	}
	if got.ProposedRule == nil {
		t.Fatalf("ProposedRule = nil, want attached valid rule")
	}
	if len(got.ProposedRule.Criteria) != 1 {
		t.Fatalf("ProposedRule.Criteria len = %d, want 1", len(got.ProposedRule.Criteria))
	}
	// Mixed-case field/operator must be normalized to Blnk's lower-case grammar.
	if f := got.ProposedRule.Criteria[0].Field; f != "reference" {
		t.Errorf("normalized Field = %q, want %q", f, "reference")
	}
	if op := got.ProposedRule.Criteria[0].Operator; op != "equals" {
		t.Errorf("normalized Operator = %q, want %q", op, "equals")
	}
}

func TestClassify_ForwardsConfiguredModel(t *testing.T) {
	var gotModel atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req openai.ChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		gotModel.Store(req.Model)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, `{"root_cause":"timing","confidence":0.5,"regulated":false,"rationale":"x"}`))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	if _, err := c.Classify(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	// Rule 5.6: the model string on the wire must equal the configured LLM_MODEL,
	// proving it is config-driven and never hardcoded.
	if m, _ := gotModel.Load().(string); m != testModel {
		t.Fatalf("request Model = %q, want configured %q", m, testModel)
	}
}

func TestClassify_DropsOutOfGrammarRule(t *testing.T) {
	content := `{
		"root_cause": "amount_drift",
		"confidence": 0.9,
		"regulated": false,
		"proposed_rule": {"field": "vendor", "operator": "equals", "value": "x"},
		"rationale": "bad field must be dropped"
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, content))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	got, err := c.Classify(context.Background(), sampleTxn())
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	// Rule 5.2: an out-of-grammar rule is rejected BEFORE it can be POSTed; the
	// classification is still returned but with no ProposedRule.
	if got.ProposedRule != nil {
		t.Fatalf("ProposedRule = %+v, want nil (out-of-grammar rule must be dropped)", got.ProposedRule)
	}
	if got.RootCause != model.RootCauseAmountDrift {
		t.Errorf("RootCause = %q, want %q", got.RootCause, model.RootCauseAmountDrift)
	}
}

func TestClassify_HandlesCodeFencedJSONAndUnknownEnum(t *testing.T) {
	// Reply wrapped in a Markdown code fence, with an unrecognized root cause and
	// an out-of-range confidence that must be clamped.
	content := "```json\n{\"root_cause\":\"martian_interference\",\"confidence\":1.7,\"regulated\":true,\"rationale\":\"weird\"}\n```"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, content))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	got, err := c.Classify(context.Background(), sampleTxn())
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if got.RootCause != model.RootCauseUnknown {
		t.Errorf("RootCause = %q, want %q (unknown enum normalizes to unknown)", got.RootCause, model.RootCauseUnknown)
	}
	if got.Confidence != 1.0 {
		t.Errorf("Confidence = %v, want clamped 1.0", got.Confidence)
	}
	if !got.Regulated {
		t.Errorf("Regulated = false, want true")
	}
}

func TestClassify_FailClosedAfterRetries(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	txn := sampleTxn()
	got, err := c.Classify(context.Background(), txn)
	if err == nil {
		t.Fatalf("Classify returned nil error, want fail-closed error")
	}
	// Rule 5.7: after the retry cap the break must fail closed via the sentinel.
	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed", err)
	}
	// retry cap of 2 => 1 initial + 2 retries = 3 attempts.
	if n := atomic.LoadInt32(&calls); n != int32(defaultMaxRetries+1) {
		t.Fatalf("upstream calls = %d, want %d", n, defaultMaxRetries+1)
	}
	// The stub result must never look auto-resolvable.
	if got.ExternalTxnID != txn.ID {
		t.Errorf("stub ExternalTxnID = %q, want %q", got.ExternalTxnID, txn.ID)
	}
	if got.RootCause != model.RootCauseUnknown || got.Confidence != 0 {
		t.Errorf("stub = {%q, %v}, want {unknown, 0}", got.RootCause, got.Confidence)
	}
}

func TestClassify_FailClosedOnUnparseableReply(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		// 200 OK, but the assistant content has no JSON object at all.
		_, _ = w.Write(chatResponseBody(t, "I am unable to analyze this transaction because the statement line is ambiguous and contains no structured content whatsoever for parsing here."))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	_, err := c.Classify(context.Background(), sampleTxn())
	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed", err)
	}
	if n := atomic.LoadInt32(&calls); n != int32(defaultMaxRetries+1) {
		t.Fatalf("upstream calls = %d, want %d (parse failure must also retry then fail closed)", n, defaultMaxRetries+1)
	}
}

func TestClassify_RecoversAfterTransientError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			http.Error(w, "transient", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, `{"root_cause":"duplicate","confidence":0.88,"regulated":false,"rationale":"dup"}`))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	got, err := c.Classify(context.Background(), sampleTxn())
	if err != nil {
		t.Fatalf("Classify error: %v", err)
	}
	if got.RootCause != model.RootCauseDuplicate {
		t.Errorf("RootCause = %q, want %q", got.RootCause, model.RootCauseDuplicate)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("calls = %d, want 2 (one transient failure then success)", atomic.LoadInt32(&calls))
	}
}

func TestNewUsesConfiguredValues(t *testing.T) {
	c := New(config.Config{LLMBaseURL: "http://example.test", LLMApiKey: "k", LLMModel: "kimi-k3"})
	if c.model != "kimi-k3" {
		t.Errorf("model = %q, want kimi-k3", c.model)
	}
	if c.maxRetries != defaultMaxRetries {
		t.Errorf("maxRetries = %d, want %d", c.maxRetries, defaultMaxRetries)
	}
	if c.client == nil {
		t.Errorf("client = nil, want constructed openai client")
	}
}

func TestClassify_FailClosedOnEmptyChoices(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		// Valid JSON envelope but zero choices.
		_, _ = w.Write([]byte(`{"id":"x","model":"` + testModel + `","choices":[]}`))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	_, err := c.Classify(context.Background(), sampleTxn())
	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed", err)
	}
	if n := atomic.LoadInt32(&calls); n != int32(defaultMaxRetries+1) {
		t.Fatalf("calls = %d, want %d (empty choices must retry then fail closed)", n, defaultMaxRetries+1)
	}
}

func TestClassify_FailClosedOnMalformedJSONObject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Contains braces (so a candidate object is extracted) but is not valid JSON.
		_, _ = w.Write(chatResponseBody(t, "here you go: {root_cause: timing, confidence: high}"))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	_, err := c.Classify(context.Background(), sampleTxn())
	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed", err)
	}
}

func TestClampConfidence(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want float64
	}{
		{"in range", 0.5, 0.5},
		{"zero", 0, 0},
		{"one", 1, 1},
		{"above one", 1.7, 1},
		{"negative", -0.3, 0},
		{"nan", math.NaN(), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clampConfidence(tt.in); got != tt.want {
				t.Fatalf("clampConfidence(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseClassificationNoRuleWhenAbsent(t *testing.T) {
	bc, err := parseClassification(`{"root_cause":"timing","confidence":0.7,"regulated":false,"rationale":"ok"}`, sampleTxn())
	if err != nil {
		t.Fatalf("parseClassification error: %v", err)
	}
	if bc.ProposedRule != nil {
		t.Fatalf("ProposedRule = %+v, want nil when omitted", bc.ProposedRule)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 120); got != "short" {
		t.Errorf("truncate short = %q, want unchanged", got)
	}
	long := make([]rune, 130)
	for i := range long {
		long[i] = 'a'
	}
	got := truncate(string(long), 120)
	if len([]rune(got)) != 123 { // 120 runes + "..."
		t.Errorf("truncate long rune len = %d, want 123", len([]rune(got)))
	}
}

// hangingServer returns an httptest server whose handler blocks (never sends a
// response) so the classifier's own per-attempt timeout — not the server — is
// what unblocks Classify. It records how many requests reached it. A handler
// unblocks either when the client aborts the request (per-attempt timeout /
// caller cancel fires r.Context().Done()) or when the test's registered cleanup
// closes the release channel; the cleanup closes release BEFORE srv.Close() so
// any straggler handler goroutine returns promptly and Close never blocks.
func hangingServer(t *testing.T, calls *int32) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(calls, 1)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

// TestClassify_PerAttemptTimeoutBoundsHungEndpoint reproduces finding C1: a hung
// endpoint under a NON-deadline context (context.Background) previously blocked
// Classify forever. With the per-attempt timeout the classifier now self-bounds
// each attempt, exhausts the retry cap, and fails closed (Rule 5.7) instead of
// hanging.
func TestClassify_PerAttemptTimeoutBoundsHungEndpoint(t *testing.T) {
	var calls int32
	srv := hangingServer(t, &calls)

	c := newTestClassifier(srv.URL)
	c.perAttemptTimeout = 120 * time.Millisecond // small explicit per-attempt bound for the test

	start := time.Now()
	got, err := c.Classify(context.Background(), sampleTxn()) // NO caller deadline
	elapsed := time.Since(start)

	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed (fail-closed under a hung endpoint)", err)
	}
	// The classifier must bound itself; total ~ (maxRetries+1) * perAttemptTimeout,
	// comfortably under this generous ceiling. Before the fix this blocked forever.
	if elapsed > 2*time.Second {
		t.Fatalf("Classify blocked %v under a hung endpoint with context.Background(); want bounded by its own per-attempt timeout", elapsed)
	}
	// Each attempt is bounded then retried up to the cap: 1 initial + 2 retries.
	if n := atomic.LoadInt32(&calls); n != int32(defaultMaxRetries+1) {
		t.Fatalf("attempts reaching server = %d, want %d", n, defaultMaxRetries+1)
	}
	// The stub result must never look auto-resolvable.
	if got.ExternalTxnID != sampleTxn().ID || got.RootCause != model.RootCauseUnknown || got.Confidence != 0 {
		t.Errorf("stub = {%q,%q,%v}, want {%q,unknown,0}", got.ExternalTxnID, got.RootCause, got.Confidence, sampleTxn().ID)
	}
}

// TestClassify_CallerDeadlineWinsAndShortCircuits verifies finding C1 repro
// step 3: a caller-supplied deadline shorter than the per-attempt bound governs,
// and once the caller's context is done the retry loop stops early (does not burn
// the full retry budget) yet still fails closed.
func TestClassify_CallerDeadlineWinsAndShortCircuits(t *testing.T) {
	var calls int32
	srv := hangingServer(t, &calls)

	c := newTestClassifier(srv.URL)
	c.perAttemptTimeout = 10 * time.Second // large; the caller deadline below must win

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.Classify(ctx, sampleTxn())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want errors.Is ErrClassificationFailed", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Classify took %v; the caller's 150ms deadline should bound it", elapsed)
	}
	// The caller's context expired, so the loop must short-circuit rather than
	// retry to the cap. Exactly one request should have reached the server.
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("attempts reaching server = %d, want 1 (retry loop must stop once the caller context is done)", n)
	}
}

// TestClassify_SlowAttemptsAreBoundedThenFailClosed reproduces finding C1
// repro step 4: three slow attempts are each bounded by the per-attempt timeout
// (they do not run unbounded) and the pipeline fails closed after the cap.
func TestClassify_SlowAttemptsAreBoundedThenFailClosed(t *testing.T) {
	var calls int32
	srv := hangingServer(t, &calls)

	c := newTestClassifier(srv.URL)
	c.perAttemptTimeout = 100 * time.Millisecond

	start := time.Now()
	_, err := c.Classify(context.Background(), sampleTxn())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want ErrClassificationFailed", err)
	}
	// At least the per-attempt bound elapsed (attempts really were bounded, not
	// instantaneous), and the total stayed well under the runaway ceiling.
	if elapsed < c.perAttemptTimeout {
		t.Fatalf("elapsed %v < per-attempt timeout %v; attempts were not actually bounded", elapsed, c.perAttemptTimeout)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("elapsed %v exceeds a sane ceiling for 3 bounded attempts", elapsed)
	}
	if n := atomic.LoadInt32(&calls); n != int32(defaultMaxRetries+1) {
		t.Fatalf("attempts = %d, want %d", n, defaultMaxRetries+1)
	}
}

// TestNewInstallsPerAttemptTimeout verifies the real constructor wires the C1
// per-attempt bound default (go-openai's DefaultConfig otherwise ships an
// unbounded http.Client, which is why New must install its own bounds). The
// bounded behavior itself is exercised end-to-end by the hung-endpoint tests.
func TestNewInstallsPerAttemptTimeout(t *testing.T) {
	c := New(config.Config{LLMBaseURL: "http://example.test", LLMApiKey: "k", LLMModel: testModel})
	if c.perAttemptTimeout != defaultPerAttemptTimeout {
		t.Errorf("perAttemptTimeout = %v, want %v", c.perAttemptTimeout, defaultPerAttemptTimeout)
	}
	if c.perAttemptTimeout <= 0 || c.perAttemptTimeout > defaultHTTPClientTimeout {
		t.Errorf("perAttemptTimeout %v must be a positive bound no greater than the HTTP backstop %v", c.perAttemptTimeout, defaultHTTPClientTimeout)
	}
}

// TestClassify_DisabledPerAttemptTimeoutHonorsCallerCancel verifies the
// perAttemptTimeout<=0 branch: the per-attempt bound is disabled, but a caller
// cancellation still returns promptly and fails closed (no hang, no auto-resolve).
func TestClassify_DisabledPerAttemptTimeoutHonorsCallerCancel(t *testing.T) {
	var calls int32
	srv := hangingServer(t, &calls)

	c := newTestClassifier(srv.URL)
	c.perAttemptTimeout = 0 // disable per-attempt bound; rely on caller context

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.Classify(ctx, sampleTxn())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrClassificationFailed) {
		t.Fatalf("error = %v, want ErrClassificationFailed", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Classify took %v; caller cancellation should return promptly", elapsed)
	}
}

// TestClassify_PromptCarriesReconciliationReasoning verifies finding C2: the
// prompt now grounds classification in reconciliation reality. The captured
// on-the-wire request must (a) keep exactly two messages [system, user] and
// (b) carry reconciliation-reasoning markers — per-root-cause internal-ledger
// definitions and the counterpart-state checklist — so the label derives from
// ledger reasoning, not description prose alone.
func TestClassify_PromptCarriesReconciliationReasoning(t *testing.T) {
	var captured atomic.Value // openai.ChatCompletionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req openai.ChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captured.Store(req)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(chatResponseBody(t, `{"root_cause":"timing","confidence":0.9,"regulated":false,"rationale":"x"}`))
	}))
	defer srv.Close()

	c := newTestClassifier(srv.URL)
	if _, err := c.Classify(context.Background(), sampleTxn()); err != nil {
		t.Fatalf("Classify: %v", err)
	}

	req, ok := captured.Load().(openai.ChatCompletionRequest)
	if !ok {
		t.Fatalf("no request captured")
	}
	// Invariant preserved: exactly two messages [system, user].
	if len(req.Messages) != 2 {
		t.Fatalf("messages = %d, want 2 [system,user]", len(req.Messages))
	}
	if req.Messages[0].Role != openai.ChatMessageRoleSystem {
		t.Errorf("message[0].Role = %q, want system", req.Messages[0].Role)
	}
	if req.Messages[1].Role != openai.ChatMessageRoleUser {
		t.Errorf("message[1].Role = %q, want user", req.Messages[1].Role)
	}
	// Marker checks are case-insensitive: they assert the presence of the
	// reconciliation-reasoning CONCEPTS, not any particular casing.
	sys := strings.ToLower(req.Messages[0].Content)
	// Reconciliation-reality markers: internal-counterpart reasoning and the
	// discriminating semantics for the labels the seed cannot separate on
	// external fields alone (timing vs duplicate vs missing_internal).
	for _, marker := range []string{
		"internal", "counterpart", "already", "consumed",
		"duplicate", "missing_internal", "timing",
	} {
		if !strings.Contains(sys, marker) {
			t.Errorf("system prompt missing reconciliation-reasoning marker %q", marker)
		}
	}
	// The system prompt must forbid proposing a rule for the escalate-only
	// causes, hardening classification against over-eager auto-remediation.
	if !strings.Contains(sys, "omit") {
		t.Errorf("system prompt does not instruct omitting proposed_rule for escalate-only causes")
	}
	// The user turn must restate the counterpart-state checklist so the label is
	// derived from ledger reasoning per break.
	user := strings.ToLower(req.Messages[1].Content)
	for _, marker := range []string{"internal counterpart", "duplicate", "missing_internal"} {
		if !strings.Contains(user, marker) {
			t.Errorf("user prompt missing reconciliation-reasoning marker %q", marker)
		}
	}
}
