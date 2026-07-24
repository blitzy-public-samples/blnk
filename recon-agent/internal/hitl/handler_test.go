package hitl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// -------------------- hand-rolled mocks (stdlib only) --------------------

type mockStore struct {
	breaks       []store.Break
	audits       []model.AuditEvent
	setCalls     [][2]string // recorded (id, status) pairs
	dequeueCalls []string
	setErr       error
	dequeueErr   error
	listErr      error
	auditErr     error
}

func (m *mockStore) ListBreaks(ctx context.Context) ([]store.Break, error) {
	return m.breaks, m.listErr
}

func (m *mockStore) ListAudit(ctx context.Context) ([]model.AuditEvent, error) {
	return m.audits, m.auditErr
}

func (m *mockStore) SetBreakStatus(ctx context.Context, id, status string) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.setCalls = append(m.setCalls, [2]string{id, status})
	return nil
}

func (m *mockStore) DequeueHITL(ctx context.Context, id string) error {
	if m.dequeueErr != nil {
		return m.dequeueErr
	}
	m.dequeueCalls = append(m.dequeueCalls, id)
	return nil
}

type mockRecorder struct {
	events []model.AuditEvent
	err    error
}

func (m *mockRecorder) Record(ctx context.Context, ev model.AuditEvent) error {
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, ev)
	return nil
}

type mockProber struct {
	cleared bool
	reconID string
	err     error
	calls   int
	lastTxn blnk.ExternalTransaction
	lastIDs []string
}

func (m *mockProber) ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, ids []string) (bool, string, error) {
	m.calls++
	m.lastTxn = txn
	m.lastIDs = ids
	return m.cleared, m.reconID, m.err
}

// -------------------- helpers --------------------

func newTestServer(st breakStore, aud recorder, bc prober) *Server {
	return NewServer(config.Config{LLMModel: "kimi-k3", HitlPort: "8088"}, st, aud, bc)
}

func doForm(t *testing.T, s *Server, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)
	return w
}

func doJSON(t *testing.T, s *Server, d model.HITLDecision) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)
	return w
}

// -------------------- route smoke tests --------------------

func TestHealthz(t *testing.T) {
	s := newTestServer(&mockStore{}, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body not json: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("healthz status field = %q, want ok", body["status"])
	}
}

func TestListBreaks(t *testing.T) {
	st := &mockStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "ext-1", RootCause: model.RootCauseTiming, Confidence: 0.4},
		Status:         "queued",
	}}}
	s := newTestServer(st, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/breaks", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("breaks status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ext-1") {
		t.Fatalf("breaks body missing ext-1: %s", w.Body.String())
	}
}

func TestListBreaksError(t *testing.T) {
	st := &mockStore{listErr: errors.New("db down")}
	s := newTestServer(st, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/breaks", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("breaks error status = %d, want 500", w.Code)
	}
}

func TestStatusPage(t *testing.T) {
	rule := &blnk.MatchingRule{Name: "amount-drift-rule"}
	st := &mockStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "ext-42",
				RootCause:     model.RootCauseAmountDrift,
				Confidence:    0.72,
				Regulated:     true,
				ProposedRule:  rule,
			},
			Status: "queued",
		}},
		audits: []model.AuditEvent{{
			Timestamp:     "2024-01-01T00:00:00Z",
			ExternalTxnID: "ext-42",
			Actor:         "agent",
			Action:        "escalated",
			Confidence:    0.72,
			Provenance:    model.Provenance{Model: "kimi-k3", ReconID: "recon-9"},
			Rationale:     "low confidence",
		}},
	}
	s := newTestServer(st, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status page code = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("status page content-type = %q, want text/html", ct)
	}
	body := w.Body.String()
	for _, want := range []string{"ext-42", "amount_drift", "amount-drift-rule", "recon-9", "accept", "re_drive", "reject", "kimi-k3"} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing %q; body=%s", want, body)
		}
	}
}

func TestStatusPageBreaksError(t *testing.T) {
	st := &mockStore{listErr: errors.New("db down")}
	s := newTestServer(st, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status page breaks-error code = %d, want 500", w.Code)
	}
}

func TestStatusPageAuditError(t *testing.T) {
	st := &mockStore{auditErr: errors.New("audit read fail")}
	s := newTestServer(st, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status page audit-error code = %d, want 500", w.Code)
	}
}

// -------------------- Gate 13: accept / re_drive / reject invocation tests --------------------

func TestDecisionAcceptForm(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doForm(t, s, url.Values{
		"external_txn_id": {"ext-1"},
		"decision":        {"accept"},
	})

	if w.Code != http.StatusSeeOther {
		t.Fatalf("accept(form) code = %d, want 303", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/" {
		t.Fatalf("accept redirect location = %q, want /", loc)
	}
	if len(st.setCalls) != 1 || st.setCalls[0] != [2]string{"ext-1", statusAccepted} {
		t.Fatalf("accept setCalls = %v, want one (ext-1, accepted)", st.setCalls)
	}
	if len(st.dequeueCalls) != 1 || st.dequeueCalls[0] != "ext-1" {
		t.Fatalf("accept dequeueCalls = %v, want [ext-1]", st.dequeueCalls)
	}
	if len(aud.events) != 1 {
		t.Fatalf("accept wrote %d audit events, want exactly 1", len(aud.events))
	}
	ev := aud.events[0]
	if ev.Action != "accepted" || ev.ExternalTxnID != "ext-1" {
		t.Fatalf("accept audit event = %+v, want action=accepted id=ext-1", ev)
	}
	if ev.Actor != defaultReviewer {
		t.Fatalf("accept default reviewer = %q, want %q", ev.Actor, defaultReviewer)
	}
	if ev.Provenance.Model != "kimi-k3" {
		t.Fatalf("accept provenance model = %q, want kimi-k3", ev.Provenance.Model)
	}
}

func TestDecisionReDriveClearedJSON(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	bc := &mockProber{cleared: true, reconID: "recon-123"}
	s := newTestServer(st, aud, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-2", Decision: "re_drive", Reviewer: "alice", Note: "retry"})

	if w.Code != http.StatusOK {
		t.Fatalf("re_drive(json) code = %d, want 200", w.Code)
	}
	// Gate 13: the dry-run probe MUST be invoked, with the break id.
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if bc.lastTxn.ID != "ext-2" {
		t.Fatalf("re_drive probed txn id = %q, want ext-2", bc.lastTxn.ID)
	}
	if len(st.setCalls) != 1 || st.setCalls[0] != [2]string{"ext-2", statusReDriven} {
		t.Fatalf("re_drive setCalls = %v, want one (ext-2, re_driven)", st.setCalls)
	}
	if len(st.dequeueCalls) != 1 {
		t.Fatalf("re_drive(cleared) dequeueCalls = %v, want [ext-2]", st.dequeueCalls)
	}
	if len(aud.events) != 1 {
		t.Fatalf("re_drive wrote %d audit events, want exactly 1", len(aud.events))
	}
	ev := aud.events[0]
	if ev.Action != "re_driven" {
		t.Fatalf("re_drive audit action = %q, want re_driven", ev.Action)
	}
	if ev.Provenance.ReconID != "recon-123" {
		t.Fatalf("re_drive audit recon_id = %q, want recon-123", ev.Provenance.ReconID)
	}
	if ev.Actor != "alice" {
		t.Fatalf("re_drive actor = %q, want alice", ev.Actor)
	}
}

func TestDecisionReDriveNotCleared(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	bc := &mockProber{cleared: false, reconID: "recon-x"}
	s := newTestServer(st, aud, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-3", Decision: "re_drive"})

	if w.Code != http.StatusOK {
		t.Fatalf("re_drive(not cleared) code = %d, want 200", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if len(st.setCalls) != 1 || st.setCalls[0] != [2]string{"ext-3", statusReDriven} {
		t.Fatalf("re_drive setCalls = %v, want one (ext-3, re_driven)", st.setCalls)
	}
	if len(st.dequeueCalls) != 0 {
		t.Fatalf("re_drive(not cleared) dequeueCalls = %v, want none", st.dequeueCalls)
	}
	if len(aud.events) != 1 {
		t.Fatalf("re_drive wrote %d audit events, want exactly 1", len(aud.events))
	}
}

// Rule 5.7 (fail-closed): a probe error must NOT change state or write audit.
func TestDecisionReDriveProbeErrorFailsClosed(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	bc := &mockProber{err: errors.New("llm/blnk timeout")}
	s := newTestServer(st, aud, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-4", Decision: "re_drive"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("re_drive(probe error) code = %d, want 500", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if len(st.setCalls) != 0 {
		t.Fatalf("re_drive(fail-closed) setCalls = %v, want none", st.setCalls)
	}
	if len(aud.events) != 0 {
		t.Fatalf("re_drive(fail-closed) wrote %d audit events, want 0", len(aud.events))
	}
}

// re_drive: a SetBreakStatus failure after a successful probe surfaces as 500
// and writes no audit event.
func TestDecisionReDriveSetStatusError(t *testing.T) {
	st := &mockStore{setErr: errors.New("set boom")}
	aud := &mockRecorder{}
	bc := &mockProber{cleared: true, reconID: "recon-e"}
	s := newTestServer(st, aud, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-30", Decision: "re_drive"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("re_drive set-status error code = %d, want 500", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if len(aud.events) != 0 {
		t.Fatalf("re_drive set-status error must not audit; events=%v", aud.events)
	}
}

// re_drive: when Blnk confirms clearance but the queue drain fails, the error
// surfaces as 500 and no audit event is written.
func TestDecisionReDriveClearedDequeueError(t *testing.T) {
	st := &mockStore{dequeueErr: errors.New("dequeue boom")}
	aud := &mockRecorder{}
	bc := &mockProber{cleared: true, reconID: "recon-f"}
	s := newTestServer(st, aud, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-31", Decision: "re_drive"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("re_drive cleared+dequeue error code = %d, want 500", w.Code)
	}
	if len(st.setCalls) != 1 || st.setCalls[0] != [2]string{"ext-31", statusReDriven} {
		t.Fatalf("re_drive setCalls = %v, want one (ext-31, re_driven)", st.setCalls)
	}
	if len(aud.events) != 0 {
		t.Fatalf("re_drive cleared+dequeue error must not audit; events=%v", aud.events)
	}
}

func TestDecisionRejectJSON(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-5", Decision: "reject", Reviewer: "bob"})

	if w.Code != http.StatusOK {
		t.Fatalf("reject(json) code = %d, want 200", w.Code)
	}
	if len(st.setCalls) != 1 || st.setCalls[0] != [2]string{"ext-5", statusRejected} {
		t.Fatalf("reject setCalls = %v, want one (ext-5, rejected)", st.setCalls)
	}
	if len(st.dequeueCalls) != 1 {
		t.Fatalf("reject dequeueCalls = %v, want [ext-5]", st.dequeueCalls)
	}
	if len(aud.events) != 1 || aud.events[0].Action != "rejected" {
		t.Fatalf("reject audit events = %+v, want one action=rejected", aud.events)
	}
}

// -------------------- validation / error branches --------------------

func TestDecisionUnknown(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doForm(t, s, url.Values{"external_txn_id": {"ext-6"}, "decision": {"frobnicate"}})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown decision code = %d, want 400", w.Code)
	}
	if len(aud.events) != 0 || len(st.setCalls) != 0 {
		t.Fatalf("unknown decision must not mutate/audit; setCalls=%v events=%v", st.setCalls, aud.events)
	}
}

func TestDecisionMissingID(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doForm(t, s, url.Values{"decision": {"accept"}})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing id code = %d, want 400", w.Code)
	}
	if len(aud.events) != 0 {
		t.Fatalf("missing id must not audit; events=%v", aud.events)
	}
}

func TestDecisionBadJSON(t *testing.T) {
	s := newTestServer(&mockStore{}, &mockRecorder{}, &mockProber{})
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad json code = %d, want 400", w.Code)
	}
}

func TestDecisionNotFound(t *testing.T) {
	st := &mockStore{setErr: store.ErrNotFound}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "missing", Decision: "accept"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("not-found code = %d, want 404", w.Code)
	}
	if len(aud.events) != 0 {
		t.Fatalf("not-found must not audit; events=%v", aud.events)
	}
}

func TestDecisionInternalError(t *testing.T) {
	st := &mockStore{setErr: errors.New("boom")}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-7", Decision: "reject"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("internal error code = %d, want 500", w.Code)
	}
}

func TestDecisionDequeueError(t *testing.T) {
	st := &mockStore{dequeueErr: errors.New("dequeue boom")}
	aud := &mockRecorder{}
	s := newTestServer(st, aud, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-8", Decision: "accept"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("dequeue error code = %d, want 500", w.Code)
	}
	if len(aud.events) != 0 {
		t.Fatalf("dequeue error must not audit; events=%v", aud.events)
	}
}

func TestDecisionAuditError(t *testing.T) {
	st := &mockStore{}
	aud := &mockRecorder{err: errors.New("audit write failed")}
	s := newTestServer(st, aud, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-9", Decision: "accept"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("audit error code = %d, want 500", w.Code)
	}
}

// -------------------- server surface --------------------

func TestRouterNotNil(t *testing.T) {
	s := newTestServer(&mockStore{}, &mockRecorder{}, &mockProber{})
	if s.Router() == nil {
		t.Fatal("Router() returned nil")
	}
}

func TestRunInvalidAddr(t *testing.T) {
	s := newTestServer(&mockStore{}, &mockRecorder{}, &mockProber{})
	if err := s.Run("bad-address-without-port"); err == nil {
		t.Fatal("Run(invalid) returned nil error, want non-nil")
	}
}
