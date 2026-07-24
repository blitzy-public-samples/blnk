package hitl

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/config"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// The fakes below satisfy the exact consumer interfaces the HITL server depends
// on, so the handlers can be exercised through the real Router() without a live
// Blnk daemon or Postgres. Compile-time assertions guard the contracts.
var (
	_ breakStore = (*fakeStore)(nil)
	_ recorder   = (*fakeAudit)(nil)
	_ prober     = (*fakeProber)(nil)
)

// ----- fakeStore ------------------------------------------------------------

type statusCall struct{ id, status string }

type fakeStore struct {
	mu sync.Mutex

	breaks []store.Break
	events []model.AuditEvent

	// LoadBreak (re_drive eligibility gate) programmed result. status defaults
	// to "queued" when empty so happy-path tests need not set it; found is
	// shared with LoadBreakTxn. loadErr is returned by LoadBreak (the first,
	// eligibility read) and surfaces as a generic 500.
	status  string
	found   bool
	loadErr error

	// LoadBreakTxn programmed result.
	txn         blnk.ExternalTransaction
	createdRule string
	loadTxnErr  error

	pingErr       error
	setStatusErr  error // returned by SetBreakStatusFromQueuedTx (e.g. ErrNotQueued / ErrNotFound)
	dequeueErr    error // returned by DequeueHITLTx
	listBreaksErr error
	listAuditErr  error

	// committed calls (rolled back by WithTx when its fn returns an error).
	statusCalls    []statusCall
	dequeueCalls   []string
	loadCalls      []string // LoadBreakTxn ids
	loadBreakCalls []string // LoadBreak ids
}

func (f *fakeStore) ListBreaks(ctx context.Context) ([]store.Break, error) {
	return f.breaks, f.listBreaksErr
}
func (f *fakeStore) ListAudit(ctx context.Context) ([]model.AuditEvent, error) {
	return f.events, f.listAuditErr
}

// LoadBreak is the re_drive eligibility read: it returns the break's current
// status so re_drive can 404 a missing break and 409 an already-decided one
// BEFORE any Blnk call (findings m-01/M-01). status defaults to "queued".
func (f *fakeStore) LoadBreak(ctx context.Context, id string) (model.BreakClassification, string, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadBreakCalls = append(f.loadBreakCalls, id)
	if f.loadErr != nil {
		return model.BreakClassification{}, "", "", false, f.loadErr
	}
	st := f.status
	if st == "" {
		st = "queued"
	}
	return model.BreakClassification{}, st, f.createdRule, f.found, nil
}

func (f *fakeStore) LoadBreakTxn(ctx context.Context, id string) (blnk.ExternalTransaction, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls = append(f.loadCalls, id)
	return f.txn, f.createdRule, f.found, f.loadTxnErr
}

// WithTx models a real transaction's atomicity (finding M-02): it snapshots the
// committed effects, runs fn, and on error rolls them back so a partial failure
// (a blocked/failed status change, a failed dequeue, or a failed audit append)
// leaves NOTHING committed. The tx-bound store methods append to the same
// slices only on success; the audit writer's RecordTx appends to fakeAudit only
// on success — so a rolled-back decision leaves status, queue, and audit all
// unchanged.
func (f *fakeStore) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	f.mu.Lock()
	nStatus := len(f.statusCalls)
	nDequeue := len(f.dequeueCalls)
	f.mu.Unlock()

	err := fn(nil) // the mock's tx-bound methods ignore the *sql.Tx
	if err != nil {
		f.mu.Lock()
		f.statusCalls = f.statusCalls[:nStatus]
		f.dequeueCalls = f.dequeueCalls[:nDequeue]
		f.mu.Unlock()
	}
	return err
}

// SetBreakStatusFromQueuedTx is the queued-only guard (finding M-01). It records
// the (id, status) transition on success; a programmed setStatusErr (e.g.
// store.ErrNotQueued for an already-decided break, store.ErrNotFound for a
// missing one) is returned WITHOUT recording, so a guarded/blocked decision
// commits no status change.
func (f *fakeStore) SetBreakStatusFromQueuedTx(ctx context.Context, tx *sql.Tx, id, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setStatusErr != nil {
		return f.setStatusErr
	}
	f.statusCalls = append(f.statusCalls, statusCall{id, status})
	return nil
}

func (f *fakeStore) DequeueHITLTx(ctx context.Context, tx *sql.Tx, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dequeueErr != nil {
		return f.dequeueErr
	}
	f.dequeueCalls = append(f.dequeueCalls, id)
	return nil
}

func (f *fakeStore) Ping(ctx context.Context) error { return f.pingErr }

func (f *fakeStore) lastStatus() (statusCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.statusCalls) == 0 {
		return statusCall{}, false
	}
	return f.statusCalls[len(f.statusCalls)-1], true
}
func (f *fakeStore) dequeueCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dequeueCalls)
}

// ----- fakeAudit ------------------------------------------------------------

type fakeAudit struct {
	mu     sync.Mutex
	events []model.AuditEvent
	err    error
}

func (f *fakeAudit) Record(ctx context.Context, ev model.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, ev)
	return nil
}

// RecordTx is the transaction-bound append used by atomic decisions (finding
// M-02). It appends only on success; a programmed err is returned WITHOUT
// appending, so when it fails inside WithTx the whole decision rolls back and
// no audit event is committed.
func (f *fakeAudit) RecordTx(ctx context.Context, tx *sql.Tx, ev model.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeAudit) recorded() []model.AuditEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.AuditEvent, len(f.events))
	copy(out, f.events)
	return out
}

// ----- fakeProber -----------------------------------------------------------

type probeCall struct {
	txn   blnk.ExternalTransaction
	rules []string
}

type fakeProber struct {
	mu sync.Mutex

	cleared  bool
	reconID  string
	probeErr error

	createID  string // rule id CreateMatchingRule echoes back
	createErr error
	deleteErr error

	probeCalls  []probeCall
	createCalls []blnk.MatchingRule
	deleteCalls []string
}

func (f *fakeProber) ProbeBreak(ctx context.Context, txn blnk.ExternalTransaction, rules []string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeCalls = append(f.probeCalls, probeCall{txn: txn, rules: rules})
	if f.probeErr != nil {
		return false, "", f.probeErr
	}
	return f.cleared, f.reconID, nil
}
func (f *fakeProber) CreateMatchingRule(ctx context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, rule)
	if f.createErr != nil {
		return blnk.MatchingRule{}, f.createErr
	}
	rule.RuleID = f.createID
	return rule, nil
}
func (f *fakeProber) DeleteMatchingRule(ctx context.Context, ruleID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, ruleID)
	return f.deleteErr
}
func (f *fakeProber) probeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.probeCalls)
}
func (f *fakeProber) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.createCalls)
}
func (f *fakeProber) deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deleteCalls))
	copy(out, f.deleteCalls)
	return out
}
func (f *fakeProber) lastProbe() (probeCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.probeCalls) == 0 {
		return probeCall{}, false
	}
	return f.probeCalls[len(f.probeCalls)-1], true
}

// ----- helpers --------------------------------------------------------------

func newTestServer(st breakStore, aud recorder, bc prober) *Server {
	return NewServer(config.Config{LLMModel: "kimi-k3"}, st, aud, bc)
}

func postDecisionJSON(t *testing.T, srv *Server, d model.HITLDecision) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	return w
}

func postDecisionForm(t *testing.T, srv *Server, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	return w
}

func doGET(t *testing.T, srv *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	return w
}

func onlyAction(t *testing.T, aud *fakeAudit) model.AuditEvent {
	t.Helper()
	evs := aud.recorded()
	if len(evs) != 1 {
		t.Fatalf("expected exactly 1 audit event, got %d", len(evs))
	}
	return evs[0]
}

// ----- accept / reject (Gate 13) --------------------------------------------

func TestHandleDecisionAccept(t *testing.T) {
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	bc := &fakeProber{}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "dup-1", Decision: audit.DecisionAccept, Reviewer: "alice"})

	if w.Code != http.StatusOK {
		t.Fatalf("accept status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	last, ok := st.lastStatus()
	if !ok || last.id != "dup-1" || last.status != statusAccepted {
		t.Fatalf("SetBreakStatus = %+v, want {dup-1 accepted}", last)
	}
	if st.dequeueCount() != 1 {
		t.Fatalf("accept must dequeue the break; dequeueCount=%d", st.dequeueCount())
	}
	ev := onlyAction(t, aud)
	if ev.Action != audit.ActionAccepted {
		t.Fatalf("audit action = %q, want %q", ev.Action, audit.ActionAccepted)
	}
	if ev.Actor != "alice" || ev.ExternalTxnID != "dup-1" {
		t.Fatalf("audit actor/id = %q/%q, want alice/dup-1", ev.Actor, ev.ExternalTxnID)
	}
	// accept must not touch Blnk at all.
	if bc.probeCount() != 0 || bc.createCount() != 0 {
		t.Fatalf("accept must not call Blnk; probes=%d creates=%d", bc.probeCount(), bc.createCount())
	}
}

func TestHandleDecisionReject(t *testing.T) {
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	bc := &fakeProber{}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "missing-1", Decision: audit.DecisionReject, Reviewer: "bob"})

	if w.Code != http.StatusOK {
		t.Fatalf("reject status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	last, ok := st.lastStatus()
	if !ok || last.status != statusRejected {
		t.Fatalf("SetBreakStatus = %+v, want status rejected", last)
	}
	if st.dequeueCount() != 1 {
		t.Fatalf("reject must dequeue the break; dequeueCount=%d", st.dequeueCount())
	}
	if ev := onlyAction(t, aud); ev.Action != audit.ActionRejected {
		t.Fatalf("audit action = %q, want %q", ev.Action, audit.ActionRejected)
	}
}

func TestHandleDecisionDefaultReviewer(t *testing.T) {
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	// No reviewer supplied -> handler applies defaultReviewer before auditing.
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "x", Decision: audit.DecisionAccept})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ev := onlyAction(t, aud); ev.Actor != defaultReviewer {
		t.Fatalf("audit actor = %q, want default %q", ev.Actor, defaultReviewer)
	}
}

// ----- re_drive: success paths (L2) -----------------------------------------

func TestHandleReDriveBuildsRuleProbesClearsAndDeletes(t *testing.T) {
	st := &fakeStore{
		found:       true,
		createdRule: "", // no persisted rule -> build a throwaway one
		txn:         blnk.ExternalTransaction{ID: "low-1", Amount: 250.75, Currency: "USD", Reference: "INV-1002"},
	}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_x", cleared: true, reconID: "rec_1"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "low-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})

	if w.Code != http.StatusOK {
		t.Fatalf("re_drive status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// Built exactly one rule from the txn fields (amount + currency).
	if bc.createCount() != 1 {
		t.Fatalf("expected one CreateMatchingRule, got %d", bc.createCount())
	}
	rule := bc.createCalls[0]
	if len(rule.Criteria) != 2 {
		t.Fatalf("expected amount+currency criteria, got %+v", rule.Criteria)
	}
	if rule.Criteria[0].Field != fieldAmount || rule.Criteria[0].Operator != operatorEquals || rule.Criteria[0].Value != "250.75" {
		t.Fatalf("amount criterion = %+v, want {amount equals 250.75}", rule.Criteria[0])
	}
	if rule.Criteria[1].Field != fieldCurrency || rule.Criteria[1].Value != "USD" {
		t.Fatalf("currency criterion = %+v, want {currency equals USD}", rule.Criteria[1])
	}
	// Probed with the FULL txn and the created rule id (finding L2), not {ID only}+nil.
	pc, ok := bc.lastProbe()
	if !ok {
		t.Fatalf("expected a probe call")
	}
	if pc.txn.ID != "low-1" || pc.txn.Amount != 250.75 {
		t.Fatalf("probe txn = %+v, want full txn low-1/250.75", pc.txn)
	}
	if len(pc.rules) != 1 || pc.rules[0] != "rule_x" {
		t.Fatalf("probe rules = %v, want [rule_x]", pc.rules)
	}
	// Cleared -> status re_driven + dequeued + audit carries the recon id.
	if last, _ := st.lastStatus(); last.status != statusReDriven {
		t.Fatalf("status = %q, want re_driven", last.status)
	}
	if st.dequeueCount() != 1 {
		t.Fatalf("cleared re_drive must dequeue; dequeueCount=%d", st.dequeueCount())
	}
	ev := onlyAction(t, aud)
	if ev.Action != audit.ActionReDriven || ev.Provenance.ReconID != "rec_1" {
		t.Fatalf("audit = action %q recon %q, want re_driven/rec_1", ev.Action, ev.Provenance.ReconID)
	}
	// Throwaway rule deleted afterwards (F5 parity).
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_x" {
		t.Fatalf("expected throwaway rule_x deleted, got %v", got)
	}
}

func TestHandleReDriveNotClearedStaysQueued(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "low-2", Amount: 10, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_y", cleared: false, reconID: "rec_2"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "low-2", Decision: audit.DecisionReDrive})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// m-04: a non-clearing re_drive leaves the break QUEUED — no terminal status
	// change and no dequeue — so it stays eligible for a later decision.
	if st.dequeueCount() != 0 {
		t.Fatalf("uncleared re_drive must NOT dequeue; dequeueCount=%d", st.dequeueCount())
	}
	if last, ok := st.lastStatus(); ok {
		t.Fatalf("uncleared re_drive must NOT change status (stays queued); got %+v", last)
	}
	// The re_drive action is still audited (Rule 5.5), but WITHOUT a recon_id:
	// only Blnk-confirmed clearance carries a recon_id, so one never implies a
	// false clearance (m-04).
	ev := onlyAction(t, aud)
	if ev.Action != audit.ActionReDriven {
		t.Fatalf("audit action = %q, want re_driven", ev.Action)
	}
	if ev.Provenance.ReconID != "" {
		t.Fatalf("uncleared re_drive audit must carry NO recon_id, got %q", ev.Provenance.ReconID)
	}
	// Even when not cleared, the throwaway rule is removed (F5 parity).
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_y" {
		t.Fatalf("expected throwaway rule_y deleted, got %v", got)
	}
}

func TestHandleReDriveReusesPersistedRule(t *testing.T) {
	st := &fakeStore{found: true, createdRule: "existing_rule", txn: blnk.ExternalTransaction{ID: "b3", Amount: 5, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{cleared: true, reconID: "rec_3"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b3", Decision: audit.DecisionReDrive})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	// Reused the persisted rule -> no create, and it must NOT be deleted (F5:
	// never delete a reused/persisted rule).
	if bc.createCount() != 0 {
		t.Fatalf("expected reuse (0 creates), got %d", bc.createCount())
	}
	pc, _ := bc.lastProbe()
	if len(pc.rules) != 1 || pc.rules[0] != "existing_rule" {
		t.Fatalf("probe rules = %v, want [existing_rule]", pc.rules)
	}
	if got := bc.deletes(); len(got) != 0 {
		t.Fatalf("must not delete a reused/persisted rule, got %v", got)
	}
}

func TestHandleReDriveOmitsCurrencyCriterionWhenAbsent(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b4", Amount: 42}} // no currency
	bc := &fakeProber{createID: "r", cleared: true}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b4", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	rule := bc.createCalls[0]
	if len(rule.Criteria) != 1 || rule.Criteria[0].Field != fieldAmount {
		t.Fatalf("expected a single amount criterion when currency absent, got %+v", rule.Criteria)
	}
}

// ----- re_drive: failure paths map to 502 (L2) ------------------------------

func TestHandleReDriveCreateRuleErrorReturns502(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b5", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createErr: errors.New("blnk unreachable")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b5", Decision: audit.DecisionReDrive})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("create-error status = %d, want 502; body=%s", w.Code, w.Body.String())
	}
	// Fail-closed: no probe, no status change, no audit.
	if bc.probeCount() != 0 {
		t.Fatalf("must not probe when rule creation failed")
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status on upstream failure")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("must not audit on upstream failure")
	}
}

func TestHandleReDriveProbeErrorReturns502AndDeletesThrowawayRule(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b6", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_z", probeErr: errors.New("start-instant 400")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b6", Decision: audit.DecisionReDrive})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("probe-error status = %d, want 502; body=%s", w.Code, w.Body.String())
	}
	// The rule we created just to probe must be cleaned up even on probe failure.
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_z" {
		t.Fatalf("expected rule_z cleanup on probe error, got %v", got)
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status on probe failure")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("must not audit on probe failure")
	}
}

func TestHandleReDriveEmptyRuleIDReturns502(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b7", Amount: 1, Currency: "USD"}}
	bc := &fakeProber{createID: ""} // Blnk echoes an empty rule id
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b7", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusBadGateway {
		t.Fatalf("empty-rule-id status = %d, want 502", w.Code)
	}
	if bc.probeCount() != 0 {
		t.Fatalf("must not probe with an empty rule id")
	}
}

// ----- re_drive: not-found / internal errors -------------------------------

func TestHandleReDriveNotFoundReturns404(t *testing.T) {
	st := &fakeStore{found: false} // break has no persisted txn
	bc := &fakeProber{}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "ghost", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("must not touch Blnk for an unknown break")
	}
}

func TestHandleReDriveLoadErrorReturns500(t *testing.T) {
	st := &fakeStore{loadErr: errors.New("db down")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b8", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("load-error status = %d, want 500", w.Code)
	}
}

// ----- decision validation --------------------------------------------------

func TestHandleDecisionUnknownVerbReturns400(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "x", Decision: "frobnicate"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown-decision status = %d, want 400", w.Code)
	}
}

func TestHandleDecisionBlankIDReturns400(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "", Decision: audit.DecisionAccept})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("blank-id status = %d, want 400", w.Code)
	}
}

func TestHandleAcceptNotFoundReturns404(t *testing.T) {
	st := &fakeStore{found: true, setStatusErr: store.ErrNotFound}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "ghost", Decision: audit.DecisionAccept})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestHandleDecisionFormRedirects(t *testing.T) {
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	form := url.Values{}
	form.Set("external_txn_id", "b9")
	form.Set("decision", audit.DecisionAccept)
	w := postDecisionForm(t, srv, form)

	// Browser form submissions get a 303 redirect back to the status page.
	if w.Code != http.StatusSeeOther {
		t.Fatalf("form status = %d, want 303; body=%s", w.Code, w.Body.String())
	}
	if ev := onlyAction(t, aud); ev.Action != audit.ActionAccepted {
		t.Fatalf("audit action = %q, want accepted", ev.Action)
	}
}

// ----- /healthz (M3) --------------------------------------------------------

func TestHealthzReadyAndReachableReturns200(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200 (default ready + ping ok)", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"ok"`) {
		t.Fatalf("healthz body = %s, want status ok", w.Body.String())
	}
}

func TestHealthzNotReadyReturns503(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	srv.SetReady(false)
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz status = %d, want 503 when not ready", w.Code)
	}
	if !strings.Contains(w.Body.String(), "not_ready") {
		t.Fatalf("healthz body = %s, want not_ready", w.Body.String())
	}
}

func TestHealthzPingFailureReturns503(t *testing.T) {
	st := &fakeStore{pingErr: errors.New("postgres unreachable")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz status = %d, want 503 when store ping fails", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unhealthy") {
		t.Fatalf("healthz body = %s, want unhealthy", w.Body.String())
	}
}

// ----- /breaks --------------------------------------------------------------

func TestListBreaksReturnsJSON(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "b10", RootCause: model.RootCause("timing"), Confidence: 0.9},
		Status:         "queued",
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/breaks")
	if w.Code != http.StatusOK {
		t.Fatalf("breaks status = %d, want 200", w.Code)
	}
	var resp struct {
		Breaks []store.Break `json:"breaks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode /breaks: %v", err)
	}
	if len(resp.Breaks) != 1 || resp.Breaks[0].Classification.ExternalTxnID != "b10" {
		t.Fatalf("breaks = %+v, want one break b10", resp.Breaks)
	}
}

// ----- error paths on other verbs ------------------------------------------

func TestHandleRejectDequeueErrorReturns500(t *testing.T) {
	st := &fakeStore{found: true, dequeueErr: errors.New("db write failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b", Decision: audit.DecisionReject})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 on dequeue error", w.Code)
	}
}

func TestListBreaksErrorReturns500(t *testing.T) {
	st := &fakeStore{listBreaksErr: errors.New("query failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/breaks")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// ----- status page (AAP 0.5.3) ---------------------------------------------

func TestStatusPageRenders(t *testing.T) {
	rule := &blnk.MatchingRule{Name: "auto-rule-1"}
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-status",
				RootCause:     model.RootCauseTiming,
				Confidence:    0.91,
				ProposedRule:  rule,
			},
			Status: "auto-resolved",
		}},
		events: []model.AuditEvent{{
			ExternalTxnID: "b-status",
			Actor:         "agent",
			Action:        audit.ActionResolved,
			Provenance:    model.Provenance{ReconID: "rec-9"},
		}},
	}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusOK {
		t.Fatalf("status page = %d, want 200", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{"HITL review", "b-status", "timing", "auto-rule-1", "rec-9", "kimi-k3"} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing %q; body len=%d", want, len(body))
		}
	}
}

func TestStatusPageListBreaksErrorReturns500(t *testing.T) {
	st := &fakeStore{listBreaksErr: errors.New("breaks query failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when ListBreaks fails", w.Code)
	}
}

func TestStatusPageListAuditErrorReturns500(t *testing.T) {
	st := &fakeStore{listAuditErr: errors.New("audit query failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when ListAudit fails", w.Code)
	}
}

// ----- server lifecycle: Run + graceful Shutdown (finding M4) ---------------

func TestServerRunAndGracefulShutdown(t *testing.T) {
	// Obtain a free port, then let Run bind it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(addr) }()

	// Poll until the server is serving /healthz.
	deadline := time.Now().Add(3 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	var served bool
	for time.Now().Before(deadline) {
		resp, gerr := client.Get("http://" + addr + "/healthz")
		if gerr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				served = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !served {
		t.Fatalf("server did not begin serving /healthz on %s", addr)
	}

	// Graceful shutdown must let Run return nil (not http.ErrServerClosed).
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error after graceful shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Run did not return after Shutdown")
	}
}

func TestServerShutdownBeforeRunIsNoop(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Run should be a no-op, got %v", err)
	}
}

func TestAsUpstreamNilReturnsNil(t *testing.T) {
	if err := asUpstream(nil); err != nil {
		t.Fatalf("asUpstream(nil) = %v, want nil", err)
	}
}

// ----- queued-only guard: already-decided breaks -> 409 (findings M-01/M-02) --

func TestHandleAcceptAlreadyDecidedReturns409(t *testing.T) {
	// A break no longer queued (already accepted/rejected/re_driven): the
	// queued-only guard returns ErrNotQueued -> 409, and NOTHING is committed —
	// no status overwrite, no dequeue, no audit (findings M-01/M-02).
	st := &fakeStore{found: true, setStatusErr: store.ErrNotQueued}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-1", Decision: audit.DecisionAccept})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("guarded accept must not overwrite status")
	}
	if st.dequeueCount() != 0 {
		t.Fatalf("guarded accept must not dequeue; got %d", st.dequeueCount())
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("guarded accept must not audit; got %d events", len(aud.recorded()))
	}
	// M-03: the 409 body must not leak the raw sentinel text.
	if strings.Contains(w.Body.String(), "store:") {
		t.Fatalf("409 body leaked internal error text: %s", w.Body.String())
	}
}

func TestHandleRejectAlreadyDecidedReturns409(t *testing.T) {
	st := &fakeStore{found: true, setStatusErr: store.ErrNotQueued}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-1", Decision: audit.DecisionReject})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("guarded reject must not overwrite status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("guarded reject must not audit")
	}
}

func TestHandleReDriveAlreadyDecidedReturns409BeforeBlnk(t *testing.T) {
	// A non-queued break must be rejected with 409 BEFORE any Blnk call
	// (findings m-01/M-01): re_drive checks eligibility via LoadBreak first.
	st := &fakeStore{found: true, status: statusAccepted,
		txn: blnk.ExternalTransaction{ID: "term-2", Amount: 5, Currency: "USD"}}
	bc := &fakeProber{createID: "rule_q", cleared: true}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-2", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("re_drive on a decided break must not touch Blnk; creates=%d probes=%d", bc.createCount(), bc.probeCount())
	}
}

// ----- atomicity: a partial failure rolls the whole decision back (M-02) -----

func TestHandleAcceptAtomicRollbackOnAuditFailure(t *testing.T) {
	// If the audit append fails, the whole decision rolls back atomically: the
	// status change is NOT committed and the queue is NOT drained (finding M-02).
	st := &fakeStore{found: true}
	aud := &fakeAudit{err: errors.New("pq: audit insert failed")}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "roll-1", Decision: audit.DecisionAccept})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("audit failure must roll back the status change")
	}
	if st.dequeueCount() != 0 {
		t.Fatalf("audit failure must roll back the dequeue; got %d", st.dequeueCount())
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("failed audit must record nothing; got %d", len(aud.recorded()))
	}
	// M-03: the 500 body must not leak the raw pq: error.
	if strings.Contains(w.Body.String(), "pq:") {
		t.Fatalf("500 body leaked raw db error: %s", w.Body.String())
	}
}

func TestHandleRejectDequeueErrorRollsBack(t *testing.T) {
	// A dequeue failure after the status change must roll the status change back
	// (finding M-02) and audit nothing.
	st := &fakeStore{found: true, dequeueErr: errors.New("pq: dequeue failed")}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "roll-2", Decision: audit.DecisionReject})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("dequeue failure must roll back the status change")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("dequeue failure must not audit; got %d", len(aud.recorded()))
	}
}

// ----- M-03: error sanitization (never leak pq:/driver/upstream text) --------

func TestHandleAcceptInternalErrorSanitized(t *testing.T) {
	// A raw (non-sentinel) store error collapses to a generic 500 message; the
	// underlying pq:/driver text is never returned to the client (M-03).
	st := &fakeStore{found: true, setStatusErr: errors.New(`pq: relation "agent.agent_break" does not exist`)}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "s-1", Decision: audit.DecisionAccept})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "pq:") || strings.Contains(body, "agent_break") {
		t.Fatalf("500 body leaked internal detail: %s", body)
	}
	if !strings.Contains(body, "internal error processing decision") {
		t.Fatalf("500 body = %s, want stable sanitized message", body)
	}
}

func TestHandleReDriveProbeErrorSanitized(t *testing.T) {
	// A Blnk (upstream) probe failure maps to 502 with a stable message; the raw
	// upstream error text is never returned (findings L2 + M-03).
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "s-2", Amount: 1, Currency: "USD"}}
	bc := &fakeProber{createID: "rule_s", probeErr: errors.New("start-instant 400: bad reference detail")}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "s-2", Decision: audit.DecisionReDrive})
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "start-instant") || strings.Contains(body, "bad reference") {
		t.Fatalf("502 body leaked raw upstream error: %s", body)
	}
	if !strings.Contains(body, "reconciliation service temporarily unavailable") {
		t.Fatalf("502 body = %s, want stable sanitized message", body)
	}
	// Throwaway rule still cleaned up on probe failure (F5 parity).
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_s" {
		t.Fatalf("expected rule_s cleanup on probe error, got %v", got)
	}
}

func TestListBreaksErrorSanitized(t *testing.T) {
	st := &fakeStore{listBreaksErr: errors.New("pq: connection refused")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/breaks")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "pq:") || strings.Contains(body, "connection refused") {
		t.Fatalf("/breaks 500 leaked raw error: %s", body)
	}
}

// ----- /readyz readiness probe (finding m-02) --------------------------------

func TestReadyzReadyAndReachableReturns200(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ready") {
		t.Fatalf("readyz body = %s, want ready", w.Body.String())
	}
}

func TestReadyzPingFailureReturns503Sanitized(t *testing.T) {
	st := &fakeStore{pingErr: errors.New("pq: postgres unreachable")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "not ready") {
		t.Fatalf("readyz body = %s, want 'not ready'", body)
	}
	if strings.Contains(body, "pq:") || strings.Contains(body, "unreachable") {
		t.Fatalf("readyz 503 leaked raw ping error: %s", body)
	}
}

func TestReadyzNotReadyReturns503(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	srv.SetReady(false)
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503 when not ready", w.Code)
	}
}
