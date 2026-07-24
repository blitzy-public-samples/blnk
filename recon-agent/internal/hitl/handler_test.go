package hitl

import (
	"context"
	"database/sql"
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
//
// mockStore is a single fake that implements BOTH breakStore AND recorder. That
// dual role is deliberate and load-bearing for the C-04 findings: production
// threads one *sql.Tx from store.WithTx into audit.RecordTx so a decision's
// status change, its HITL-queue drain, and its audit event commit together or
// not at all. A faithful fake must therefore model that shared transaction — so
// mockStore buffers every transaction-bound write and only applies it to
// committed state when WithTx commits, discarding the whole buffer on rollback.
// The decision tests inject the SAME *mockStore as both the store and the
// recorder (see newDecisionServer) precisely so RecordTx enrolls in the same
// buffered transaction the store mutations do, letting the tests prove that no
// partial outcome (status without audit, dequeue without status, …) is ever
// durable when any step fails.
//
// It also models the queued-only compare-and-set that makes decisions
// terminal-safe and idempotent: SetBreakStatusIfQueuedTx / RequireQueuedTx read
// committed status and return store.ErrConflict for any non-queued break, so a
// decision against an already-settled break — or a replay of a decision already
// applied — is refused, never re-applied.

// mockBreak is one break's committed decision state.
type mockBreak struct {
	status  string
	queued  bool // present in the agent_hitl_queue
	txn     *blnk.ExternalTransaction
	ruleIDs []string
}

type mockStore struct {
	// Reads for the list / status-page tests.
	breaks   []store.Break
	listErr  error
	auditErr error

	// Committed decision state, keyed by external txn id.
	items map[string]*mockBreak

	// Committed append-only audit ledger. ListAudit returns it; RecordTx appends
	// to it (but only when the enclosing transaction commits).
	audits []model.AuditEvent

	// Fault-injection hooks (nil => success). A non-nil error returned from a
	// transaction-bound write aborts the closure, forcing WithTx to roll back
	// and discard every buffered mutation of that transaction.
	loadErr    error // LoadBreakContext
	beginErr   error // WithTx (simulated begin failure)
	casErr     error // SetBreakStatusIfQueuedTx non-sentinel failure
	dequeueErr error // DequeueHITLTx
	requireErr error // RequireQueuedTx non-sentinel failure
	recordErr  error // RecordTx

	// Per-request transaction buffer. HITL handlers open exactly one WithTx and
	// gin serves each request on a single goroutine, so a single buffer suffices.
	pending []func()

	// Observability: successful WithTx commits, so a test can assert that a
	// fault-injected decision committed nothing (commits stays 0).
	commits int
}

// seed preloads a break's committed decision state.
func (m *mockStore) seed(id, status string, queued bool, txn *blnk.ExternalTransaction, ruleIDs []string) {
	if m.items == nil {
		m.items = map[string]*mockBreak{}
	}
	m.items[id] = &mockBreak{status: status, queued: queued, txn: txn, ruleIDs: ruleIDs}
}

func (m *mockStore) statusOf(id string) (string, bool) {
	b, ok := m.items[id]
	if !ok {
		return "", false
	}
	return b.status, true
}

func (m *mockStore) isQueued(id string) bool {
	b, ok := m.items[id]
	return ok && b.queued
}

func (m *mockStore) ListBreaks(_ context.Context) ([]store.Break, error) {
	return m.breaks, m.listErr
}

func (m *mockStore) ListAudit(_ context.Context) ([]model.AuditEvent, error) {
	return m.audits, m.auditErr
}

// LoadBreakContext returns the durable re-drive context (finding C-03): the
// break's status, the FULL persisted transaction, and the applicable rule ids.
func (m *mockStore) LoadBreakContext(_ context.Context, id string) (string, *blnk.ExternalTransaction, []string, bool, error) {
	if m.loadErr != nil {
		return "", nil, nil, false, m.loadErr
	}
	b, ok := m.items[id]
	if !ok {
		return "", nil, nil, false, nil
	}
	return b.status, b.txn, b.ruleIDs, true, nil
}

// WithTx runs fn against a buffered transaction: on success it applies every
// buffered mutation atomically; on any error it discards the buffer (rollback).
// The *sql.Tx is nil because the buffered mocks never touch a real driver.
func (m *mockStore) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	if m.beginErr != nil {
		return m.beginErr
	}
	m.pending = nil
	if err := fn(nil); err != nil {
		m.pending = nil // rollback: nothing becomes durable
		return err
	}
	for _, mut := range m.pending {
		mut()
	}
	m.pending = nil
	m.commits++
	return nil
}

// SetBreakStatusIfQueuedTx is the queued-only CAS: it buffers a status change
// only when the break is currently queued, and otherwise returns the sentinel
// the handler maps to 404 (missing) or 409 (terminal / replay).
func (m *mockStore) SetBreakStatusIfQueuedTx(_ context.Context, _ *sql.Tx, id, status string) error {
	if m.casErr != nil {
		return m.casErr
	}
	b, ok := m.items[id]
	if !ok {
		return store.ErrNotFound
	}
	if b.status != statusQueued {
		return store.ErrConflict
	}
	m.pending = append(m.pending, func() { b.status = status })
	return nil
}

// RequireQueuedTx confirms the break is still queued without mutating it, used
// by re_drive's "not cleared" path.
func (m *mockStore) RequireQueuedTx(_ context.Context, _ *sql.Tx, id string) error {
	if m.requireErr != nil {
		return m.requireErr
	}
	b, ok := m.items[id]
	if !ok {
		return store.ErrNotFound
	}
	if b.status != statusQueued {
		return store.ErrConflict
	}
	return nil
}

// DequeueHITLTx buffers removal from the HITL queue (idempotent).
func (m *mockStore) DequeueHITLTx(_ context.Context, _ *sql.Tx, id string) error {
	if m.dequeueErr != nil {
		return m.dequeueErr
	}
	m.pending = append(m.pending, func() {
		if b, ok := m.items[id]; ok {
			b.queued = false
		}
	})
	return nil
}

// RecordTx buffers the decision's audit event so it commits with the status /
// queue effects. It is the recorder half of the dual role: injecting the same
// *mockStore as both store and recorder is what makes the audit event share the
// store mutations' transaction (finding C-04).
func (m *mockStore) RecordTx(_ context.Context, _ *sql.Tx, ev model.AuditEvent) error {
	if m.recordErr != nil {
		return m.recordErr
	}
	m.pending = append(m.pending, func() { m.audits = append(m.audits, ev) })
	return nil
}

// mockRecorder is a trivial recorder for the non-decision tests (route smoke,
// list, status page) that never drive a decision and so never call RecordTx.
type mockRecorder struct {
	events []model.AuditEvent
	err    error
}

func (m *mockRecorder) RecordTx(_ context.Context, _ *sql.Tx, ev model.AuditEvent) error {
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

func (m *mockProber) ProbeBreak(_ context.Context, txn blnk.ExternalTransaction, ids []string) (bool, string, error) {
	m.calls++
	m.lastTxn = txn
	m.lastIDs = ids
	return m.cleared, m.reconID, m.err
}

// -------------------- helpers --------------------

func newTestServer(st breakStore, aud recorder, bc prober) *Server {
	return NewServer(config.Config{LLMModel: "kimi-k3", HitlPort: "8088"}, st, aud, bc)
}

// newDecisionServer wires the SAME *mockStore as both the store and the audit
// recorder so a decision's audit event shares the store's buffered transaction —
// the setup that lets the decision tests assert atomic all-or-nothing commit
// (finding C-04).
func newDecisionServer(m *mockStore, bc prober) *Server {
	return newTestServer(m, m, bc)
}

// queuedTxn is a complete external transaction planted on a queued break so
// re_drive can prove it probes with the FULL line, not an id-only stub (C-03).
func queuedTxn(id string) *blnk.ExternalTransaction {
	return &blnk.ExternalTransaction{
		ID:          id,
		Amount:      1234.56,
		Currency:    "USD",
		Reference:   "INV-" + id,
		Description: "vendor payment " + id,
		Source:      "bankA",
	}
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
//
// Every decision below drives a real HITP request through the router into the
// handler, so each of the three HITL verbs has an invocation test (Gate 13),
// and each asserts the C-03 / C-04 guarantees rather than the old permissive
// behavior.

// accept (happy path): a queued break moves queued->accepted, is drained from
// the queue, and gets exactly one `accepted` audit event — all in one commit.
func TestDecisionAcceptForm(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-1", statusQueued, true, queuedTxn("ext-1"), []string{"rule-1"})
	s := newDecisionServer(st, &mockProber{})

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
	if got, _ := st.statusOf("ext-1"); got != statusAccepted {
		t.Fatalf("accept committed status = %q, want %q", got, statusAccepted)
	}
	if st.isQueued("ext-1") {
		t.Fatal("accept must drain the break from the HITL queue")
	}
	if st.commits != 1 {
		t.Fatalf("accept commits = %d, want exactly 1", st.commits)
	}
	if len(st.audits) != 1 {
		t.Fatalf("accept wrote %d audit events, want exactly 1", len(st.audits))
	}
	ev := st.audits[0]
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

// reject (happy path): queued->rejected, drained, one `rejected` event.
func TestDecisionRejectJSON(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-5", statusQueued, true, queuedTxn("ext-5"), []string{"rule-5"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-5", Decision: "reject", Reviewer: "bob"})

	if w.Code != http.StatusOK {
		t.Fatalf("reject(json) code = %d, want 200", w.Code)
	}
	if got, _ := st.statusOf("ext-5"); got != statusRejected {
		t.Fatalf("reject committed status = %q, want %q", got, statusRejected)
	}
	if st.isQueued("ext-5") {
		t.Fatal("reject must drain the break from the HITL queue")
	}
	if len(st.audits) != 1 || st.audits[0].Action != "rejected" {
		t.Fatalf("reject audit events = %+v, want one action=rejected", st.audits)
	}
	if st.audits[0].Actor != "bob" {
		t.Fatalf("reject actor = %q, want bob", st.audits[0].Actor)
	}
}

// re_drive (cleared) — finding C-03: the probe MUST receive the FULL persisted
// transaction AND a NON-EMPTY applicable-rule-id set, not an id-only, rule-less
// stub. Blnk confirming clearance then settles the break re_driven and drains it.
func TestDecisionReDriveClearedJSON(t *testing.T) {
	st := &mockStore{}
	full := queuedTxn("ext-2")
	st.seed("ext-2", statusQueued, true, full, []string{"rule-9"})
	bc := &mockProber{cleared: true, reconID: "recon-123"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-2", Decision: "re_drive", Reviewer: "alice", Note: "retry"})

	if w.Code != http.StatusOK {
		t.Fatalf("re_drive(json) code = %d, want 200", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	// C-03: the probe payload is the COMPLETE transaction, not just the id.
	if bc.lastTxn.ID != "ext-2" || bc.lastTxn.Amount != full.Amount ||
		bc.lastTxn.Currency != full.Currency || bc.lastTxn.Reference != full.Reference {
		t.Fatalf("re_drive probed txn = %+v, want the full persisted txn %+v", bc.lastTxn, *full)
	}
	// C-03: the probe carries a non-empty applicable-rule-id set.
	if len(bc.lastIDs) != 1 || bc.lastIDs[0] != "rule-9" {
		t.Fatalf("re_drive probe rule ids = %v, want [rule-9]", bc.lastIDs)
	}
	if got, _ := st.statusOf("ext-2"); got != statusReDriven {
		t.Fatalf("re_drive(cleared) committed status = %q, want %q", got, statusReDriven)
	}
	if st.isQueued("ext-2") {
		t.Fatal("re_drive(cleared) must drain the break from the HITL queue")
	}
	if len(st.audits) != 1 {
		t.Fatalf("re_drive wrote %d audit events, want exactly 1", len(st.audits))
	}
	ev := st.audits[0]
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

// re_drive (not cleared): the dry-run did not clear, so the break STAYS queued
// (still re-drivable) — the earlier behavior of stamping it re_driven regardless
// was wrong. Exactly one `re_driven` audit event is still recorded.
func TestDecisionReDriveNotCleared(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-3", statusQueued, true, queuedTxn("ext-3"), []string{"rule-3"})
	bc := &mockProber{cleared: false, reconID: "recon-x"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-3", Decision: "re_drive"})

	if w.Code != http.StatusOK {
		t.Fatalf("re_drive(not cleared) code = %d, want 200", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if got, _ := st.statusOf("ext-3"); got != statusQueued {
		t.Fatalf("re_drive(not cleared) status = %q, want it to STAY %q", got, statusQueued)
	}
	if !st.isQueued("ext-3") {
		t.Fatal("re_drive(not cleared) must leave the break in the HITL queue")
	}
	if len(st.audits) != 1 || st.audits[0].Action != "re_driven" {
		t.Fatalf("re_drive(not cleared) audits = %+v, want one re_driven event", st.audits)
	}
	if st.commits != 1 {
		t.Fatalf("re_drive(not cleared) commits = %d, want 1", st.commits)
	}
}

// re_drive fail-closed on missing durable context — finding C-03. A queued break
// with no persisted transaction, or with no applicable rule, must NOT be probed
// with an invalid rule-less request; it fails closed with 422 and changes nothing.
func TestDecisionReDriveNoContextFailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		txn     *blnk.ExternalTransaction
		ruleIDs []string
	}{
		{name: "no transaction", txn: nil, ruleIDs: []string{"rule-1"}},
		{name: "no applicable rule", txn: queuedTxn("ext-nc"), ruleIDs: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &mockStore{}
			st.seed("ext-nc", statusQueued, true, tc.txn, tc.ruleIDs)
			bc := &mockProber{cleared: true, reconID: "should-not-happen"}
			s := newDecisionServer(st, bc)

			w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-nc", Decision: "re_drive"})

			if w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("re_drive(no context) code = %d, want 422", w.Code)
			}
			if bc.calls != 0 {
				t.Fatalf("re_drive(no context) must NOT probe Blnk; calls = %d", bc.calls)
			}
			if got, _ := st.statusOf("ext-nc"); got != statusQueued {
				t.Fatalf("re_drive(no context) status = %q, want unchanged %q", got, statusQueued)
			}
			if len(st.audits) != 0 || st.commits != 0 {
				t.Fatalf("re_drive(no context) must not audit/commit; audits=%d commits=%d", len(st.audits), st.commits)
			}
		})
	}
}

// re_drive fail-closed on probe error — Rule 5.7 / finding C-04. A probe failure
// maps to 502, changes no state, writes no audit, and leaves the break queued.
func TestDecisionReDriveProbeErrorFailsClosed(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-4", statusQueued, true, queuedTxn("ext-4"), []string{"rule-4"})
	bc := &mockProber{err: errors.New("llm/blnk timeout")}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-4", Decision: "re_drive"})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("re_drive(probe error) code = %d, want 502", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive probe calls = %d, want 1", bc.calls)
	}
	if got, _ := st.statusOf("ext-4"); got != statusQueued {
		t.Fatalf("re_drive(fail-closed) status = %q, want unchanged %q", got, statusQueued)
	}
	if !st.isQueued("ext-4") {
		t.Fatal("re_drive(fail-closed) must leave the break queued")
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(fail-closed) must not audit/commit; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// -------------------- finding C-04: atomic rollback, no partial state --------------------

// accept with an audit-write failure must roll the WHOLE transaction back: the
// status stays queued, the break stays in the queue, and no audit is durable.
// The previous fire-and-forget handlers left the status changed and the break
// dequeued even though the audit never landed.
func TestDecisionAcceptAuditErrorRollsBack(t *testing.T) {
	st := &mockStore{recordErr: errors.New("audit write failed")}
	st.seed("ext-9", statusQueued, true, queuedTxn("ext-9"), []string{"rule-9"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-9", Decision: "accept"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("accept(audit error) code = %d, want 500", w.Code)
	}
	if got, _ := st.statusOf("ext-9"); got != statusQueued {
		t.Fatalf("accept(audit error) status = %q, want rolled back to %q", got, statusQueued)
	}
	if !st.isQueued("ext-9") {
		t.Fatal("accept(audit error) must NOT dequeue the break (rollback)")
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("accept(audit error) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// accept with a queue-drain failure must likewise roll back the status change.
func TestDecisionAcceptDequeueErrorRollsBack(t *testing.T) {
	st := &mockStore{dequeueErr: errors.New("dequeue boom")}
	st.seed("ext-8", statusQueued, true, queuedTxn("ext-8"), []string{"rule-8"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-8", Decision: "accept"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("accept(dequeue error) code = %d, want 500", w.Code)
	}
	if got, _ := st.statusOf("ext-8"); got != statusQueued {
		t.Fatalf("accept(dequeue error) status = %q, want rolled back to %q", got, statusQueued)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("accept(dequeue error) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// re_drive that clears but then fails to drain the queue must roll back: the
// status must NOT be left re_driven with the break still queued and no audit.
func TestDecisionReDriveClearedDequeueErrorRollsBack(t *testing.T) {
	st := &mockStore{dequeueErr: errors.New("dequeue boom")}
	st.seed("ext-31", statusQueued, true, queuedTxn("ext-31"), []string{"rule-31"})
	bc := &mockProber{cleared: true, reconID: "recon-f"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-31", Decision: "re_drive"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("re_drive cleared+dequeue error code = %d, want 500", w.Code)
	}
	if got, _ := st.statusOf("ext-31"); got != statusQueued {
		t.Fatalf("re_drive cleared+dequeue error status = %q, want rolled back to %q", got, statusQueued)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive cleared+dequeue error must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// re_drive against a break that does not exist is a 404 before any probe.
func TestDecisionReDriveNotFound(t *testing.T) {
	st := &mockStore{} // no seeded breaks
	bc := &mockProber{cleared: true, reconID: "nope"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "missing", Decision: "re_drive"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("re_drive(not found) code = %d, want 404", w.Code)
	}
	if bc.calls != 0 {
		t.Fatalf("re_drive(not found) must NOT probe; calls = %d", bc.calls)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(not found) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// A failure loading the re-drive context surfaces as 500 and probes nothing.
func TestDecisionReDriveLoadError(t *testing.T) {
	st := &mockStore{loadErr: errors.New("context read failed")}
	st.seed("ext-le", statusQueued, true, queuedTxn("ext-le"), []string{"rule-le"})
	bc := &mockProber{cleared: true}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-le", Decision: "re_drive"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("re_drive(load error) code = %d, want 500", w.Code)
	}
	if bc.calls != 0 {
		t.Fatalf("re_drive(load error) must NOT probe; calls = %d", bc.calls)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(load error) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// Concurrency (finding C-04): the dry-run cleared, but between the probe and the
// commit another decision settled the break, so the queued-only CAS fails with a
// conflict inside the transaction. The whole re_drive rolls back — 409, no audit.
func TestDecisionReDriveClearedRaceConflict(t *testing.T) {
	st := &mockStore{casErr: store.ErrConflict}
	st.seed("ext-race", statusQueued, true, queuedTxn("ext-race"), []string{"rule-r"})
	bc := &mockProber{cleared: true, reconID: "recon-race"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-race", Decision: "re_drive"})

	if w.Code != http.StatusConflict {
		t.Fatalf("re_drive(cleared race) code = %d, want 409", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive(cleared race) probe calls = %d, want 1", bc.calls)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(cleared race) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// Concurrency (finding C-04): the dry-run did NOT clear, and RequireQueuedTx
// discovers the break was settled by a concurrent decision between the probe and
// the commit, so no re_drive audit is recorded against a now-terminal break — 409.
func TestDecisionReDriveNotClearedRaceConflict(t *testing.T) {
	st := &mockStore{requireErr: store.ErrConflict}
	st.seed("ext-race2", statusQueued, true, queuedTxn("ext-race2"), []string{"rule-r2"})
	bc := &mockProber{cleared: false, reconID: "recon-race2"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-race2", Decision: "re_drive"})

	if w.Code != http.StatusConflict {
		t.Fatalf("re_drive(not-cleared race) code = %d, want 409", w.Code)
	}
	if bc.calls != 1 {
		t.Fatalf("re_drive(not-cleared race) probe calls = %d, want 1", bc.calls)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(not-cleared race) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// -------------------- finding C-04: terminal-state rejection & idempotency --------------------

// Rejecting an already auto-resolved break must be REFUSED with 409 — this is
// the exact regression the review caught in the browser (an auto-resolved row
// being flipped to rejected through the UI). No mutation, no audit.
func TestDecisionRejectAutoResolvedConflict(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-auto", "auto-resolved", false, queuedTxn("ext-auto"), []string{"rule-a"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-auto", Decision: "reject"})

	if w.Code != http.StatusConflict {
		t.Fatalf("reject(auto-resolved) code = %d, want 409", w.Code)
	}
	if got, _ := st.statusOf("ext-auto"); got != "auto-resolved" {
		t.Fatalf("reject(auto-resolved) status = %q, want unchanged auto-resolved", got)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("reject(auto-resolved) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// Accepting a break that is already terminal (accepted) must be refused with 409.
func TestDecisionAcceptTerminalConflict(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-term", statusAccepted, false, queuedTxn("ext-term"), []string{"rule-t"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-term", Decision: "accept"})

	if w.Code != http.StatusConflict {
		t.Fatalf("accept(terminal) code = %d, want 409", w.Code)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("accept(terminal) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// re_drive against a terminal break must be refused with 409 BEFORE probing.
func TestDecisionReDriveTerminalConflict(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-rt", statusReDriven, false, queuedTxn("ext-rt"), []string{"rule-rt"})
	bc := &mockProber{cleared: true, reconID: "nope"}
	s := newDecisionServer(st, bc)

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-rt", Decision: "re_drive"})

	if w.Code != http.StatusConflict {
		t.Fatalf("re_drive(terminal) code = %d, want 409", w.Code)
	}
	if bc.calls != 0 {
		t.Fatalf("re_drive(terminal) must NOT probe; calls = %d", bc.calls)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("re_drive(terminal) must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// Replaying a decision that already succeeded is idempotent: the first accept
// settles the break; the SECOND accept matches no queued row and is refused with
// 409, leaving the single original audit event untouched (no double-processing).
func TestDecisionAcceptReplayIdempotent(t *testing.T) {
	st := &mockStore{}
	st.seed("ext-1", statusQueued, true, queuedTxn("ext-1"), []string{"rule-1"})
	s := newDecisionServer(st, &mockProber{})

	first := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-1", Decision: "accept"})
	if first.Code != http.StatusOK {
		t.Fatalf("first accept code = %d, want 200", first.Code)
	}
	if got, _ := st.statusOf("ext-1"); got != statusAccepted {
		t.Fatalf("after first accept status = %q, want %q", got, statusAccepted)
	}
	if len(st.audits) != 1 || st.commits != 1 {
		t.Fatalf("after first accept audits=%d commits=%d, want 1/1", len(st.audits), st.commits)
	}

	second := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-1", Decision: "accept"})
	if second.Code != http.StatusConflict {
		t.Fatalf("replayed accept code = %d, want 409", second.Code)
	}
	if len(st.audits) != 1 {
		t.Fatalf("replayed accept must not append a second audit; audits=%d", len(st.audits))
	}
	if st.commits != 1 {
		t.Fatalf("replayed accept must not commit again; commits=%d", st.commits)
	}
}

// -------------------- validation / error branches --------------------

func TestDecisionUnknown(t *testing.T) {
	st := &mockStore{}
	s := newDecisionServer(st, &mockProber{})

	w := doForm(t, s, url.Values{"external_txn_id": {"ext-6"}, "decision": {"frobnicate"}})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown decision code = %d, want 400", w.Code)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("unknown decision must not mutate/audit; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

func TestDecisionMissingID(t *testing.T) {
	st := &mockStore{}
	s := newDecisionServer(st, &mockProber{})

	w := doForm(t, s, url.Values{"decision": {"accept"}})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing id code = %d, want 400", w.Code)
	}
	if len(st.audits) != 0 {
		t.Fatalf("missing id must not audit; audits=%d", len(st.audits))
	}
}

func TestDecisionBadJSON(t *testing.T) {
	s := newDecisionServer(&mockStore{}, &mockProber{})
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Router().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad json code = %d, want 400", w.Code)
	}
}

// A decision against a non-existent break is a 404 (the CAS matches no row),
// and writes no audit.
func TestDecisionNotFound(t *testing.T) {
	st := &mockStore{} // no seeded breaks
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "missing", Decision: "accept"})

	if w.Code != http.StatusNotFound {
		t.Fatalf("not-found code = %d, want 404", w.Code)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("not-found must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// A non-sentinel store error from the CAS surfaces as 500 with no audit.
func TestDecisionInternalError(t *testing.T) {
	st := &mockStore{casErr: errors.New("boom")}
	st.seed("ext-7", statusQueued, true, queuedTxn("ext-7"), []string{"rule-7"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-7", Decision: "reject"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("internal error code = %d, want 500", w.Code)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("internal error must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
	}
}

// A WithTx begin failure surfaces as 500 and commits nothing.
func TestDecisionBeginTxError(t *testing.T) {
	st := &mockStore{beginErr: errors.New("cannot begin tx")}
	st.seed("ext-b", statusQueued, true, queuedTxn("ext-b"), []string{"rule-b"})
	s := newDecisionServer(st, &mockProber{})

	w := doJSON(t, s, model.HITLDecision{ExternalTxnID: "ext-b", Decision: "accept"})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("begin-tx error code = %d, want 500", w.Code)
	}
	if got, _ := st.statusOf("ext-b"); got != statusQueued {
		t.Fatalf("begin-tx error status = %q, want unchanged %q", got, statusQueued)
	}
	if len(st.audits) != 0 || st.commits != 0 {
		t.Fatalf("begin-tx error must commit nothing; audits=%d commits=%d", len(st.audits), st.commits)
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
