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

// testCSRF is the fixed double-submit token the happy-path form helper places in
// BOTH the cookie and the hidden field so the mutation guard accepts it. Negative
// tests deliberately omit or mismatch it to prove the guard rejects the POST.
const testCSRF = "test-csrf-token-0123456789abcdef"

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

	// LoadBreakTxn programmed result. txnNotFound makes LoadBreakTxn report the
	// break as gone (found=false) even though LoadBreak found it, modeling a
	// concurrent delete racing between the two reads.
	txn         blnk.ExternalTransaction
	createdRule string
	loadTxnErr  error
	txnNotFound bool

	pingErr       error
	setStatusErr  error // returned by SetBreakStatusFromQueuedTx (e.g. ErrNotQueued / ErrNotFound)
	dequeueErr    error // returned by DequeueHITLTx
	listBreaksErr error
	listAuditErr  error

	// Claim/lease programming for the re_drive processing lease (finding M-15).
	// claimBusy makes ClaimBreak report "another owner holds it" (granted=false,
	// nil error); claimErr makes it fail (e.g. store.ErrNotFound). claims tracks
	// the current lease holder per break so ReleaseBreak can verify ownership.
	claimBusy  bool
	claimErr   error
	releaseErr error
	claims     map[string]string

	// committed calls (rolled back by WithTx when its fn returns an error).
	statusCalls    []statusCall
	dequeueCalls   []string
	loadCalls      []string // LoadBreakTxn ids
	loadBreakCalls []string // LoadBreak ids
	claimCalls     []string
	releaseCalls   []string
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
	found := f.found && !f.txnNotFound
	return f.txn, f.createdRule, found, f.loadTxnErr
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

// ClaimBreak models the durable, cross-instance processing lease re_drive
// acquires before any external Blnk work (finding M-15). It records the call,
// then: returns the programmed claimErr if set; reports "busy" (granted=false,
// nil) when claimBusy is set (another owner holds the lease); otherwise grants
// the lease and remembers the owner so ReleaseBreak can verify ownership.
func (f *fakeStore) ClaimBreak(ctx context.Context, id, owner string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls = append(f.claimCalls, id)
	if f.claimErr != nil {
		return false, f.claimErr
	}
	if f.claimBusy {
		return false, nil
	}
	if f.claims == nil {
		f.claims = map[string]string{}
	}
	f.claims[id] = owner
	return true, nil
}

// ReleaseBreak clears a lease this owner holds; a mismatch yields ErrLeaseNotHeld
// so a caller can never believe it released a lease it did not own (finding M-15).
func (f *fakeStore) ReleaseBreak(ctx context.Context, id, owner string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls = append(f.releaseCalls, id)
	if f.releaseErr != nil {
		return f.releaseErr
	}
	if f.claims != nil && f.claims[id] == owner {
		delete(f.claims, id)
		return nil
	}
	return store.ErrLeaseNotHeld
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
func (f *fakeStore) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claimCalls)
}
func (f *fakeStore) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.releaseCalls)
}

// heldLeases reports how many breaks still have an un-released lease — used to
// assert re_drive always releases what it claimed (finding M-15).
func (f *fakeStore) heldLeases() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claims)
}

// ----- fakeAudit ------------------------------------------------------------

type fakeAudit struct {
	mu     sync.Mutex
	events []model.AuditEvent
	err    error
	// failAction, when set, restricts err to a SINGLE action so a test can fail
	// (say) only the `probed` audit while letting `rule_created` succeed. When
	// empty, a non-nil err fails EVERY append (the original blanket behavior).
	failAction string
}

func (f *fakeAudit) shouldFail(ev model.AuditEvent) bool {
	return f.err != nil && (f.failAction == "" || ev.Action == f.failAction)
}

func (f *fakeAudit) Record(ctx context.Context, ev model.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.shouldFail(ev) {
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
	if f.shouldFail(ev) {
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

// firstWithAction returns the first recorded event with the given action.
func firstWithAction(evs []model.AuditEvent, action string) (model.AuditEvent, bool) {
	for _, e := range evs {
		if e.Action == action {
			return e, true
		}
	}
	return model.AuditEvent{}, false
}

// countAction counts recorded events with the given action.
func countAction(evs []model.AuditEvent, action string) int {
	n := 0
	for _, e := range evs {
		if e.Action == action {
			n++
		}
	}
	return n
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

func serve(srv *Server, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	return w
}

func postDecisionJSON(t *testing.T, srv *Server, d model.HITLDecision) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal decision: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	return serve(srv, req)
}

// postRawJSON posts an arbitrary JSON string (for unknown-field / trailing /
// oversized-body negative tests). It uses the JSON content type, which the
// mutation guard exempts from the CSRF token (origin-checked only).
func postRawJSON(srv *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return serve(srv, req)
}

// newFormReq builds a bare urlencoded form POST (no Origin/CSRF). Negative tests
// add or omit Origin/cookie/field to exercise the guard precisely.
func newFormReq(form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

// postDecisionForm posts a well-formed, SAME-ORIGIN, CSRF-valid browser form
// (the happy path): it injects a reviewer when absent, sets the matching
// csrf_token cookie+field, and a same-origin Origin header.
func postDecisionForm(t *testing.T, srv *Server, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	if form.Get("reviewer") == "" {
		form.Set("reviewer", "test-reviewer")
	}
	form.Set("csrf_token", testCSRF)
	req := newFormReq(form)
	req.Header.Set("Origin", "http://"+req.Host)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	return serve(srv, req)
}

func doGET(t *testing.T, srv *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	return serve(srv, req)
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
	// accept must not touch Blnk at all, and (M-15) must NOT claim a lease —
	// leasing is only for re_drive's external work.
	if bc.probeCount() != 0 || bc.createCount() != 0 {
		t.Fatalf("accept must not call Blnk; probes=%d creates=%d", bc.probeCount(), bc.createCount())
	}
	if st.claimCount() != 0 {
		t.Fatalf("accept must not claim a processing lease; claims=%d", st.claimCount())
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
	if st.claimCount() != 0 {
		t.Fatalf("reject must not claim a processing lease; claims=%d", st.claimCount())
	}
}

// ----- M-23: reviewer identity is REQUIRED (never defaulted) ----------------

func TestHandleDecisionBlankReviewerRejected(t *testing.T) {
	// M-23: a decision with no reviewer is UNATTRIBUTABLE and must be rejected
	// with 400 — NOT silently defaulted to a shared "operator" identity. Nothing
	// is committed.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "x", Decision: audit.DecisionAccept})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("blank-reviewer status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reviewer is required") {
		t.Fatalf("body = %s, want 'reviewer is required'", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("blank-reviewer decision must change no status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("blank-reviewer decision must audit nothing; got %d", len(aud.recorded()))
	}
}

func TestHandleDecisionWhitespaceReviewerRejected(t *testing.T) {
	// M-23: a whitespace-only reviewer trims to empty and is rejected (not
	// stored as a blank-looking actor).
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "   "})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("whitespace-reviewer status = %d, want 400", w.Code)
	}
}

func TestHandleDecisionBlankReviewerFormRejected(t *testing.T) {
	// Same M-23 rule on the browser form path: a same-origin, CSRF-valid form
	// with a blank reviewer is still rejected 400.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "b")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "") // explicit blank
	form.Set("csrf_token", testCSRF)
	req := newFormReq(form)
	req.Header.Set("Origin", "http://"+req.Host)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	w := serve(srv, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("blank-reviewer form status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleDecisionReviewerTooLong(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: strings.Repeat("a", maxReviewerLen+1)})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-long reviewer status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "reviewer is too long") {
		t.Fatalf("body = %s, want 'reviewer is too long'", w.Body.String())
	}
}

func TestHandleDecisionNoteTooLong(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice", Note: strings.Repeat("n", maxNoteLen+1)})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-long note status = %d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "note is too long") {
		t.Fatalf("body = %s, want 'note is too long'", w.Body.String())
	}
}

func TestHandleDecisionExternalIDTooLong(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: strings.Repeat("i", maxExternalTxnIDLen+1), Decision: audit.DecisionAccept, Reviewer: "alice"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-long external_txn_id status = %d, want 400", w.Code)
	}
}

// ----- C-05 / M-23: strict bounded request decoding -------------------------

func TestHandleDecisionUnknownJSONFieldRejected(t *testing.T) {
	// M-23: a JSON payload carrying an unexpected key is refused (strict
	// DisallowUnknownFields), not silently accepted.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})
	w := postRawJSON(srv, `{"external_txn_id":"x","decision":"accept","reviewer":"alice","is_admin":true}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown-field status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("rejected payload must audit nothing")
	}
}

func TestHandleDecisionTrailingJSONRejected(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postRawJSON(srv, `{"external_txn_id":"x","decision":"accept","reviewer":"a"}{"x":1}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("trailing-data status = %d, want 400", w.Code)
	}
}

func TestHandleDecisionOversizedJSONRejected(t *testing.T) {
	// C-05: the body is bounded; an oversized JSON body is rejected rather than
	// read unbounded.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	huge := strings.Repeat("A", maxDecisionBodyBytes+1024)
	w := postRawJSON(srv, `{"external_txn_id":"x","decision":"accept","reviewer":"a","note":"`+huge+`"}`)
	if w.Code < 400 || w.Code >= 500 {
		t.Fatalf("oversized JSON status = %d, want a 4xx rejection; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleDecisionOversizedFormRejected(t *testing.T) {
	// C-05: an oversized urlencoded body trips the MaxBytesReader during form
	// parsing and is rejected 413.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "b")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "alice")
	form.Set("csrf_token", testCSRF)
	form.Set("note", strings.Repeat("A", maxDecisionBodyBytes+1024))
	req := newFormReq(form)
	req.Header.Set("Origin", "http://"+req.Host)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	w := serve(srv, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized form status = %d, want 413; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleDecisionUnsupportedContentTypeRejected(t *testing.T) {
	// C-05/M-23: only application/json and x-www-form-urlencoded are accepted; a
	// text/plain POST (which a cross-site page can send without a CORS preflight)
	// is rejected 415.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader("external_txn_id=b&decision=accept&reviewer=a"))
	req.Header.Set("Content-Type", "text/plain")
	w := serve(srv, req)
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain status = %d, want 415; body=%s", w.Code, w.Body.String())
	}
}

// ----- C-05: CSRF / same-origin protection ----------------------------------

func TestHandleDecisionFormMissingCSRFRejected(t *testing.T) {
	// C-05: a same-origin form WITHOUT the double-submit CSRF token is rejected
	// 403 — the token is mandatory for browser form submissions.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "b")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "alice")
	req := newFormReq(form) // no csrf field, no cookie
	req.Header.Set("Origin", "http://"+req.Host)
	w := serve(srv, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("missing-CSRF status = %d, want 403; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("CSRF-rejected form must change no status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("CSRF-rejected form must audit nothing")
	}
}

func TestHandleDecisionFormMismatchedCSRFRejected(t *testing.T) {
	// C-05: cookie token and form token must MATCH; a mismatch is rejected 403.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "b")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "alice")
	form.Set("csrf_token", "field-token")
	req := newFormReq(form)
	req.Header.Set("Origin", "http://"+req.Host)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "different-cookie-token"})
	w := serve(srv, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("mismatched-CSRF status = %d, want 403", w.Code)
	}
}

func TestHandleDecisionCrossOriginFormRejected(t *testing.T) {
	// C-05: a cross-site Origin is rejected 403 even with a (forged) matching
	// token, because the same-origin check runs first.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "b")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "alice")
	form.Set("csrf_token", testCSRF)
	req := newFormReq(form)
	req.Header.Set("Origin", "http://evil.example.net")
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	w := serve(srv, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-origin form status = %d, want 403; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleDecisionCrossOriginJSONRejected(t *testing.T) {
	// C-05: the same-origin check also guards the JSON API when an Origin header
	// is present and mismatched.
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	b, _ := json.Marshal(model.HITLDecision{ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "a"})
	req := httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://evil.example.net")
	w := serve(srv, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-origin JSON status = %d, want 403", w.Code)
	}
}

func TestSecurityHeadersPresent(t *testing.T) {
	// C-05: every response carries the restrictive security-header baseline.
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	h := w.Result().Header
	checks := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
		"Cache-Control":          "no-store",
	}
	for k, want := range checks {
		if got := h.Get(k); got != want {
			t.Fatalf("header %s = %q, want %q", k, got, want)
		}
	}
	if csp := h.Get("Content-Security-Policy"); !strings.Contains(csp, "frame-ancestors 'none'") || !strings.Contains(csp, "form-action 'self'") {
		t.Fatalf("CSP = %q, want frame-ancestors 'none' + form-action 'self'", csp)
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
	// Cleared -> status re_driven + dequeued.
	if last, _ := st.lastStatus(); last.status != statusReDriven {
		t.Fatalf("status = %q, want re_driven", last.status)
	}
	if st.dequeueCount() != 1 {
		t.Fatalf("cleared re_drive must dequeue; dequeueCount=%d", st.dequeueCount())
	}
	// M-12: every Blnk interaction is audited — rule_created, probed(recon), the
	// terminal re_driven(recon), and rule_compensated(deleted).
	evs := aud.recorded()
	rc, ok := firstWithAction(evs, audit.ActionRuleCreated)
	if !ok || rc.Provenance.RuleID != "rule_x" {
		t.Fatalf("expected rule_created audit for rule_x; events=%+v", evs)
	}
	pb, ok := firstWithAction(evs, audit.ActionProbed)
	if !ok || pb.Provenance.ReconID != "rec_1" {
		t.Fatalf("expected probed audit with recon rec_1; events=%+v", evs)
	}
	rd, ok := firstWithAction(evs, audit.ActionReDriven)
	if !ok || rd.Provenance.ReconID != "rec_1" {
		t.Fatalf("expected terminal re_driven audit with recon rec_1; events=%+v", evs)
	}
	comp, ok := firstWithAction(evs, audit.ActionRuleCompensated)
	if !ok || comp.Provenance.RuleID != "rule_x" {
		t.Fatalf("expected rule_compensated audit for rule_x; events=%+v", evs)
	}
	// Throwaway rule deleted afterwards (F5 parity).
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_x" {
		t.Fatalf("expected throwaway rule_x deleted, got %v", got)
	}
	// M-15: the lease was claimed and released (nothing left held).
	if st.claimCount() != 1 || st.releaseCount() != 1 || st.heldLeases() != 0 {
		t.Fatalf("lease lifecycle = claim %d/release %d/held %d, want 1/1/0",
			st.claimCount(), st.releaseCount(), st.heldLeases())
	}
}

func TestHandleReDriveNotClearedStaysQueued(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "low-2", Amount: 10, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_y", cleared: false, reconID: "rec_2"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "low-2", Decision: audit.DecisionReDrive, Reviewer: "alice"})

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
	// The re_drive action is still audited, but the terminal re_driven event
	// carries NO recon_id — only Blnk-confirmed clearance carries one (m-04).
	evs := aud.recorded()
	rd, ok := firstWithAction(evs, audit.ActionReDriven)
	if !ok {
		t.Fatalf("expected a re_driven audit; events=%+v", evs)
	}
	if rd.Provenance.ReconID != "" {
		t.Fatalf("uncleared re_drive re_driven audit must carry NO recon_id, got %q", rd.Provenance.ReconID)
	}
	// M-12: the probe outcome (not cleared) is still audited with its recon id.
	if pb, ok := firstWithAction(evs, audit.ActionProbed); !ok || pb.Provenance.ReconID != "rec_2" {
		t.Fatalf("expected probed audit with recon rec_2; events=%+v", evs)
	}
	// Even when not cleared, the throwaway rule is removed and compensation audited.
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_y" {
		t.Fatalf("expected throwaway rule_y deleted, got %v", got)
	}
	if _, ok := firstWithAction(evs, audit.ActionRuleCompensated); !ok {
		t.Fatalf("expected rule_compensated audit; events=%+v", evs)
	}
	if st.releaseCount() != 1 || st.heldLeases() != 0 {
		t.Fatalf("lease not released: release %d/held %d", st.releaseCount(), st.heldLeases())
	}
}

func TestHandleReDriveReusesPersistedRule(t *testing.T) {
	st := &fakeStore{found: true, createdRule: "existing_rule", txn: blnk.ExternalTransaction{ID: "b3", Amount: 5, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{cleared: true, reconID: "rec_3"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b3", Decision: audit.DecisionReDrive, Reviewer: "alice"})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	// Reused the persisted rule -> no create, and it must NOT be deleted (F5:
	// never delete a reused/persisted rule) — hence no rule_created/compensated.
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
	evs := aud.recorded()
	if countAction(evs, audit.ActionRuleCreated) != 0 || countAction(evs, audit.ActionRuleCompensated) != 0 {
		t.Fatalf("reused rule must not audit create/compensate; events=%+v", evs)
	}
	if rd, ok := firstWithAction(evs, audit.ActionReDriven); !ok || rd.Provenance.ReconID != "rec_3" {
		t.Fatalf("expected terminal re_driven with recon rec_3; events=%+v", evs)
	}
}

func TestHandleReDriveOmitsCurrencyCriterionWhenAbsent(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b4", Amount: 42}} // no currency
	bc := &fakeProber{createID: "r", cleared: true}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b4", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	rule := bc.createCalls[0]
	if len(rule.Criteria) != 1 || rule.Criteria[0].Field != fieldAmount {
		t.Fatalf("expected a single amount criterion when currency absent, got %+v", rule.Criteria)
	}
}

// ----- re_drive: failure paths map to 502 AND audit the attempt (M-12) ------

func TestHandleReDriveCreateRuleErrorReturns502AndAuditsAttempt(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b5", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createErr: errors.New("blnk unreachable")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b5", Decision: audit.DecisionReDrive, Reviewer: "alice"})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("create-error status = %d, want 502; body=%s", w.Code, w.Body.String())
	}
	// Fail-closed: no probe, no status change.
	if bc.probeCount() != 0 {
		t.Fatalf("must not probe when rule creation failed")
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status on upstream failure")
	}
	// M-12: the FAILED create attempt is audited (a re_driven event with a
	// failure rationale and no recon_id), not silently dropped. Because creation
	// failed there is no rule_created and nothing to compensate.
	evs := aud.recorded()
	rd, ok := firstWithAction(evs, audit.ActionReDriven)
	if !ok || rd.Provenance.ReconID != "" {
		t.Fatalf("expected an audited failed re_drive attempt with no recon_id; events=%+v", evs)
	}
	if !strings.Contains(strings.ToLower(rd.Rationale), "creation failed") {
		t.Fatalf("failure rationale = %q, want it to name the failed creation", rd.Rationale)
	}
	if countAction(evs, audit.ActionRuleCreated) != 0 || countAction(evs, audit.ActionRuleCompensated) != 0 {
		t.Fatalf("no rule created/compensated when creation failed; events=%+v", evs)
	}
	// The lease is still claimed then released around the failed attempt.
	if st.claimCount() != 1 || st.releaseCount() != 1 || st.heldLeases() != 0 {
		t.Fatalf("lease lifecycle = %d/%d/%d, want 1/1/0", st.claimCount(), st.releaseCount(), st.heldLeases())
	}
}

func TestHandleReDriveProbeErrorReturns502AuditsAttemptAndCompensates(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b6", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_z", probeErr: errors.New("start-instant 400")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b6", Decision: audit.DecisionReDrive, Reviewer: "alice"})

	if w.Code != http.StatusBadGateway {
		t.Fatalf("probe-error status = %d, want 502; body=%s", w.Code, w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status on probe failure")
	}
	// The rule we created just to probe must be cleaned up even on probe failure.
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_z" {
		t.Fatalf("expected rule_z cleanup on probe error, got %v", got)
	}
	// M-12: rule_created (the successful create), the FAILED probe attempt
	// (re_driven, no recon), and the compensation outcome are all audited.
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionRuleCreated); !ok {
		t.Fatalf("expected rule_created audit; events=%+v", evs)
	}
	rd, ok := firstWithAction(evs, audit.ActionReDriven)
	if !ok || rd.Provenance.ReconID != "" || !strings.Contains(strings.ToLower(rd.Rationale), "probe failed") {
		t.Fatalf("expected audited failed probe attempt; events=%+v", evs)
	}
	if _, ok := firstWithAction(evs, audit.ActionRuleCompensated); !ok {
		t.Fatalf("expected rule_compensated audit; events=%+v", evs)
	}
}

func TestHandleReDriveEmptyRuleIDReturns502AndAuditsAttempt(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b7", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: ""} // Blnk echoes an empty rule id
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b7", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusBadGateway {
		t.Fatalf("empty-rule-id status = %d, want 502", w.Code)
	}
	if bc.probeCount() != 0 {
		t.Fatalf("must not probe with an empty rule id")
	}
	// M-12: the failed attempt is audited.
	if _, ok := firstWithAction(aud.recorded(), audit.ActionReDriven); !ok {
		t.Fatalf("expected an audited failed re_drive attempt")
	}
}

func TestHandleReDriveDeleteFailureLeavesDurableEvidence(t *testing.T) {
	// M-11/M-12: when the ephemeral probe rule cannot be deleted, the re_drive
	// still succeeds (the recon_id, not the rule, is the durable clearance proof)
	// and a rule_compensated event with deleted=false is recorded as DURABLE
	// evidence that an orphaned rule remains in Blnk needing manual cleanup.
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "b-del", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{}
	bc := &fakeProber{createID: "rule_d", cleared: true, reconID: "rec_d", deleteErr: errors.New("blnk delete 500")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b-del", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (delete failure must not fail the decision)", w.Code)
	}
	if last, _ := st.lastStatus(); last.status != statusReDriven {
		t.Fatalf("status = %q, want re_driven despite delete failure", last.status)
	}
	comp, ok := firstWithAction(aud.recorded(), audit.ActionRuleCompensated)
	if !ok || comp.Provenance.RuleID != "rule_d" {
		t.Fatalf("expected a rule_compensated event for rule_d; got %+v", aud.recorded())
	}
	// deleted=false is captured in the rationale as durable manual-cleanup evidence.
	if !strings.Contains(strings.ToLower(comp.Rationale), "manual cleanup") {
		t.Fatalf("compensation rationale = %q, want durable 'manual cleanup' evidence", comp.Rationale)
	}
}

// ----- re_drive: eligibility errors BEFORE any lease / Blnk work ------------

func TestHandleReDriveNotFoundReturns404BeforeClaim(t *testing.T) {
	st := &fakeStore{found: false} // break has no persisted txn
	bc := &fakeProber{}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "ghost", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("must not touch Blnk for an unknown break")
	}
	if st.claimCount() != 0 {
		t.Fatalf("must not claim a lease for an unknown break; claims=%d", st.claimCount())
	}
}

func TestHandleReDriveLoadErrorReturns500(t *testing.T) {
	st := &fakeStore{loadErr: errors.New("db down")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b8", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("load-error status = %d, want 500", w.Code)
	}
	if st.claimCount() != 0 {
		t.Fatalf("must not claim a lease when eligibility read failed")
	}
}

// ----- M-15: distributed re_drive races are prevented by the DB lease -------

func TestHandleReDriveBusyReturns409WithoutExternalWork(t *testing.T) {
	// M-15: when another instance/request holds the break's processing lease,
	// re_drive is told the break is busy (409) and performs NO external Blnk
	// work — defeating the duplicate/racing re_drive the finding describes.
	st := &fakeStore{found: true, claimBusy: true, txn: blnk.ExternalTransaction{ID: "busy-1", Amount: 1, Currency: "USD"}}
	bc := &fakeProber{createID: "r", cleared: true}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "busy-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusConflict {
		t.Fatalf("busy re_drive status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if st.claimCount() != 1 {
		t.Fatalf("expected one claim attempt, got %d", st.claimCount())
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("a busy break must not do external Blnk work; creates=%d probes=%d", bc.createCount(), bc.probeCount())
	}
	if st.releaseCount() != 0 {
		t.Fatalf("must not release a lease it never acquired; releases=%d", st.releaseCount())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("busy re_drive must change no status")
	}
}

func TestHandleReDriveClaimNotFoundReturns404(t *testing.T) {
	st := &fakeStore{found: true, claimErr: store.ErrNotFound, txn: blnk.ExternalTransaction{ID: "gone", Amount: 1}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "gone", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("claim-not-found status = %d, want 404", w.Code)
	}
}

func TestHandleReDriveClaimErrorReturns500(t *testing.T) {
	st := &fakeStore{found: true, claimErr: errors.New("lease table unreachable"), txn: blnk.ExternalTransaction{ID: "e", Amount: 1}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "e", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("claim-error status = %d, want 500", w.Code)
	}
}

// ----- decision validation --------------------------------------------------

func TestHandleDecisionUnknownVerbReturns400(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "x", Decision: "frobnicate", Reviewer: "alice"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown-decision status = %d, want 400", w.Code)
	}
}

func TestHandleDecisionBlankIDReturns400(t *testing.T) {
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "", Decision: audit.DecisionAccept, Reviewer: "alice"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("blank-id status = %d, want 400", w.Code)
	}
}

func TestHandleAcceptNotFoundReturns404(t *testing.T) {
	st := &fakeStore{found: true, setStatusErr: store.ErrNotFound}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "ghost", Decision: audit.DecisionAccept, Reviewer: "alice"})
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
	form.Set("reviewer", "carol")
	w := postDecisionForm(t, srv, form)

	// A well-formed, same-origin, CSRF-valid browser form gets a 303 redirect
	// back to the status page.
	if w.Code != http.StatusSeeOther {
		t.Fatalf("form status = %d, want 303; body=%s", w.Code, w.Body.String())
	}
	ev := onlyAction(t, aud)
	if ev.Action != audit.ActionAccepted {
		t.Fatalf("audit action = %q, want accepted", ev.Action)
	}
	if ev.Actor != "carol" {
		t.Fatalf("audit actor = %q, want the operator-supplied reviewer 'carol'", ev.Actor)
	}
}

// ----- /healthz is a PURE LIVENESS probe (finding M-16) ---------------------

func TestHealthzAlwaysOKWhileServing(t *testing.T) {
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Fatalf("healthz = %d %s, want 200 ok", w.Code, w.Body.String())
	}
}

func TestHealthzLivenessIgnoresReadiness(t *testing.T) {
	// M-16: liveness must NOT flip when the service is merely not-ready — that is
	// readiness's job (/readyz). A not-ready process is still alive.
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	srv.SetReady(false)
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200 even when not ready (liveness != readiness)", w.Code)
	}
}

func TestHealthzLivenessIgnoresPingFailure(t *testing.T) {
	// M-16: a dependency (DB) outage must NOT flip liveness — otherwise an
	// orchestrator would needlessly restart a healthy process. Dependency health
	// is surfaced by /readyz.
	st := &fakeStore{pingErr: errors.New("postgres unreachable")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200 even when the store ping fails", w.Code)
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
	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "b", Decision: audit.DecisionReject, Reviewer: "alice"})
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
	// A QUEUED break is what renders the decision form (finding C-07): a terminal
	// row would correctly suppress the controls, so the form-presence assertions
	// below require a queued fixture.
	rule := &blnk.MatchingRule{
		Name: "auto-rule-1",
		Criteria: []blnk.MatchingCriteria{
			{Field: fieldAmount, Operator: operatorEquals, AllowableDrift: 0.05},
			{Field: fieldCurrency, Operator: operatorEquals},
		},
	}
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-status",
				RootCause:     model.RootCauseTiming,
				Confidence:    0.91,
				ProposedRule:  rule,
				Rationale:     "date lag of two days",
			},
			Status: statusQueued,
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
	// M-22: the FULL proposed-rule operator context is rendered, not just the
	// rule name — the reviewer sees each criterion's field/operator and drift.
	for _, want := range []string{`class="criteria"`, "amount equals", "currency equals", "drift 0.050"} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing rule-criteria context %q", want)
		}
	}
	// M-22: the classifier rationale is surfaced as per-break decision context.
	if !strings.Contains(body, "date lag of two days") {
		t.Fatalf("status page must surface the classifier rationale")
	}
	// C-05: the form embeds a CSRF token, sets the matching cookie, and requires
	// an operator-entered reviewer — the hardcoded client-controlled reviewer is
	// gone.
	if !strings.Contains(body, `name="csrf_token"`) {
		t.Fatalf("status page form missing hidden csrf_token field")
	}
	if strings.Contains(body, `value="operator"`) {
		t.Fatalf("status page still hardcodes the client-controlled reviewer 'operator'")
	}
	if !strings.Contains(body, `name="reviewer"`) || !strings.Contains(body, "required") {
		t.Fatalf("status page must require an operator-entered reviewer input")
	}
	// The CSRF cookie must be set for the double-submit check to succeed later.
	var sawCookie bool
	for _, ck := range w.Result().Cookies() {
		if ck.Name == csrfCookieName && ck.Value != "" {
			sawCookie = true
		}
	}
	if !sawCookie {
		t.Fatalf("status page did not set a %s cookie", csrfCookieName)
	}
}

// TestStatusPageC07RendersControlsOnlyForQueuedRows proves finding C-07: only a
// persisted queued break renders the accept/re_drive/reject controls; every
// terminal row (auto-resolved/accepted/re_driven/rejected) shows its status with
// NO actionable form. Regression here would re-expose already-decided breaks to
// a second, racy decision.
func TestStatusPageC07RendersControlsOnlyForQueuedRows(t *testing.T) {
	terminal := []string{"auto-resolved", "accepted", "re_driven", "rejected", "classified"}
	for _, status := range terminal {
		t.Run("terminal_"+status, func(t *testing.T) {
			st := &fakeStore{breaks: []store.Break{{
				Classification: model.BreakClassification{ExternalTxnID: "term-only", RootCause: model.RootCauseDuplicate},
				Status:         status,
			}}}
			srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
			body := doGET(t, srv, "/").Body.String()
			if strings.Contains(body, `action="/decisions"`) {
				t.Fatalf("terminal status %q must NOT render a decision form", status)
			}
			for _, verb := range []string{`value="accept"`, `value="re_drive"`, `value="reject"`} {
				if strings.Contains(body, verb) {
					t.Fatalf("terminal status %q must NOT render decision button %s", status, verb)
				}
			}
			if !strings.Contains(body, "no action") || !strings.Contains(body, status) {
				t.Fatalf("terminal status %q should show a non-actionable status cell", status)
			}
		})
	}

	// A queued row DOES render exactly the three controls and the form.
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "q-1", RootCause: model.RootCauseTiming},
		Status:         statusQueued,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()
	if !strings.Contains(body, `action="/decisions"`) {
		t.Fatalf("queued row must render a decision form")
	}
	for _, verb := range []string{`value="accept"`, `value="re_drive"`, `value="reject"`} {
		if !strings.Contains(body, verb) {
			t.Fatalf("queued row missing decision control %s", verb)
		}
	}
}

// TestStatusPageM22SemanticStructure proves the finding-M-22 accessibility
// structure: a main landmark, table captions, column header scopes, and
// per-row contextual accessible names on the interactive controls.
func TestStatusPageM22SemanticStructure(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "EXT-77", RootCause: model.RootCauseTiming},
		Status:         statusQueued,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()

	for _, want := range []string{
		"<main>",             // landmark
		"<caption>",          // table captions
		`scope="col"`,        // header cell scope
		`class="table-wrap"`, // responsive overflow container (no page overflow)
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing semantic structure %q", want)
		}
	}
	// Contextual accessible names carry the row's txn id so a screen-reader user
	// knows WHICH break a control acts on.
	for _, want := range []string{
		`aria-label="accept break EXT-77"`,
		`aria-label="re_drive break EXT-77"`,
		`aria-label="reject break EXT-77"`,
		`aria-label="reviewer id for break EXT-77"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing contextual accessible name %q", want)
		}
	}
}

// TestStatusPageEscapesHostileContext proves that untrusted, break-derived
// context (ids, rationale, rule fields) is HTML-escaped on the page — a
// classifier-proposed or Blnk-returned value can never inject markup into this
// unauthenticated surface (findings M-22 "escaped context" / C-05).
func TestStatusPageEscapesHostileContext(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{
			ExternalTxnID: `<script>x</script>`,
			RootCause:     model.RootCauseUnknown,
			Rationale:     `<img src=x onerror=alert(1)>`,
			ProposedRule:  &blnk.MatchingRule{Name: `"><b>evil</b>`},
		},
		Status: statusQueued,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()
	for _, raw := range []string{"<script>x</script>", "<img src=x onerror=", "<b>evil</b>"} {
		if strings.Contains(body, raw) {
			t.Fatalf("status page rendered unescaped hostile markup %q", raw)
		}
	}
	// The escaped form is present instead.
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Fatalf("status page did not HTML-escape the external txn id")
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

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-1", Decision: audit.DecisionAccept, Reviewer: "alice"})
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

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-1", Decision: audit.DecisionReject, Reviewer: "alice"})
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
	// A non-queued break must be rejected with 409 BEFORE any Blnk call and
	// BEFORE any lease claim (findings m-01/M-01): re_drive checks eligibility
	// via LoadBreak first.
	st := &fakeStore{found: true, status: statusAccepted,
		txn: blnk.ExternalTransaction{ID: "term-2", Amount: 5, Currency: "USD"}}
	bc := &fakeProber{createID: "rule_q", cleared: true}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "term-2", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body.String())
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("re_drive on a decided break must not touch Blnk; creates=%d probes=%d", bc.createCount(), bc.probeCount())
	}
	if st.claimCount() != 0 {
		t.Fatalf("re_drive on a decided break must not claim a lease; claims=%d", st.claimCount())
	}
}

// ----- atomicity: a partial failure rolls the whole decision back (M-02) -----

func TestHandleAcceptAtomicRollbackOnAuditFailure(t *testing.T) {
	// If the audit append fails, the whole decision rolls back atomically: the
	// status change is NOT committed and the queue is NOT drained (finding M-02).
	st := &fakeStore{found: true}
	aud := &fakeAudit{err: errors.New("pq: audit insert failed")}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "roll-1", Decision: audit.DecisionAccept, Reviewer: "alice"})
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

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "roll-2", Decision: audit.DecisionReject, Reviewer: "alice"})
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

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "s-1", Decision: audit.DecisionAccept, Reviewer: "alice"})
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

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "s-2", Decision: audit.DecisionReDrive, Reviewer: "alice"})
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

// ----- M-12: audit-failure paths in re_drive fail closed --------------------

func TestHandleReDriveRuleCreatedAuditFailureCompensatesAndFailsClosed(t *testing.T) {
	// M-12: if recording the ephemeral rule's creation fails, we must not leak an
	// unaudited rule — the rule is compensated (deleted) and the decision fails
	// closed (500) BEFORE any probe.
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "ba-1", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{err: errors.New("pq: audit down")} // failAction "" -> all audits fail
	bc := &fakeProber{createID: "rule_af", cleared: true, reconID: "rec_af"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "ba-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when the rule_created audit fails", w.Code)
	}
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_af" {
		t.Fatalf("expected rule_af compensated on audit failure, got %v", got)
	}
	if bc.probeCount() != 0 {
		t.Fatalf("must not probe after a rule_created audit failure")
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status when failing closed")
	}
}

func TestHandleReDriveCreateErrorWithAuditFailureStillReturns502(t *testing.T) {
	// M-12/M-03: even if auditing the FAILED create attempt itself fails, the
	// original upstream (502) cause is surfaced — the audit hiccup is logged, not
	// allowed to mask the real dependency fault.
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "bb-1", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{err: errors.New("pq: audit down")}
	bc := &fakeProber{createErr: errors.New("blnk unreachable")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bb-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (upstream cause preserved despite audit failure)", w.Code)
	}
}

func TestHandleReDriveProbedAuditFailureFailsClosed(t *testing.T) {
	// M-12: if recording the probe OUTCOME fails, the decision fails closed (500)
	// rather than proceeding with an unaudited probe; the ephemeral rule is still
	// compensated.
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "bc-1", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{err: errors.New("pq: audit down"), failAction: audit.ActionProbed}
	bc := &fakeProber{createID: "rule_pf", cleared: true, reconID: "rec_pf"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bc-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when the probed audit fails", w.Code)
	}
	if _, ok := firstWithAction(aud.recorded(), audit.ActionRuleCreated); !ok {
		t.Fatalf("expected rule_created audit before the failing probed audit")
	}
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_pf" {
		t.Fatalf("expected rule_pf compensated, got %v", got)
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status when the probe audit fails")
	}
}

// ----- M-15 / atomicity: raced terminal transition + dequeue error ----------

func TestHandleReDriveClearedButRacedTerminalReturns409(t *testing.T) {
	// M-15: if a concurrent decision transitions the break out of queued between
	// the eligibility read and the terminal CAS, the queued-only guard yields 409
	// — the re_drive does not overwrite the terminal state.
	st := &fakeStore{found: true, setStatusErr: store.ErrNotQueued,
		txn: blnk.ExternalTransaction{ID: "bd-1", Amount: 1, Currency: "USD"}}
	bc := &fakeProber{createID: "rule_r", cleared: true, reconID: "rec_r"}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bd-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 on raced terminal transition", w.Code)
	}
	if got := bc.deletes(); len(got) != 1 || got[0] != "rule_r" {
		t.Fatalf("expected rule_r cleanup, got %v", got)
	}
}

func TestHandleReDriveClearedDequeueErrorReturns500(t *testing.T) {
	st := &fakeStore{found: true, dequeueErr: errors.New("pq: dequeue failed"),
		txn: blnk.ExternalTransaction{ID: "be-1", Amount: 1, Currency: "USD"}}
	bc := &fakeProber{createID: "rule_dq", cleared: true, reconID: "rec_dq"}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "be-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 on dequeue error in cleared re_drive", w.Code)
	}
}

func TestHandleReDriveLoadTxnErrorReturns500(t *testing.T) {
	st := &fakeStore{found: true, loadTxnErr: errors.New("pq: txn load failed"),
		txn: blnk.ExternalTransaction{ID: "bf-1"}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bf-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 on LoadBreakTxn error", w.Code)
	}
	// The lease was claimed then released around the failed load (M-15).
	if st.claimCount() != 1 || st.releaseCount() != 1 {
		t.Fatalf("lease not claimed+released around load-txn error: %d/%d", st.claimCount(), st.releaseCount())
	}
}

func TestHandleReDriveTxnRacedDeleteReturns404(t *testing.T) {
	// The break passed the eligibility read but was deleted before LoadBreakTxn
	// (a race); re_drive returns 404 rather than probing a phantom.
	st := &fakeStore{found: true, txnNotFound: true, txn: blnk.ExternalTransaction{ID: "bg-1"}}
	bc := &fakeProber{}
	srv := newTestServer(st, &fakeAudit{}, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bg-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 on raced delete", w.Code)
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("must not touch Blnk after a raced delete")
	}
}

// ----- C-05 mutation guard: content-type parameters + Referer fallback ------

func TestMutationGuardAcceptsJSONWithCharset(t *testing.T) {
	// contentTypeOf strips parameters so "application/json; charset=utf-8" is
	// accepted — the guard must not reject legitimate charset-qualified JSON.
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	b, _ := json.Marshal(model.HITLDecision{ExternalTxnID: "cs-1", Decision: audit.DecisionAccept, Reviewer: "alice"})
	req := httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := serve(srv, req)
	if w.Code != http.StatusOK {
		t.Fatalf("charset-qualified JSON status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
}

func TestMutationGuardRefererFallbackCrossOriginRejected(t *testing.T) {
	// When Origin is absent, the Referer host is used for the same-origin check;
	// a cross-site Referer is rejected 403 (finding C-05).
	srv := newTestServer(&fakeStore{found: true}, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "rf-1")
	form.Set("decision", audit.DecisionAccept)
	form.Set("reviewer", "alice")
	form.Set("csrf_token", testCSRF)
	req := newFormReq(form)
	req.Header.Set("Referer", "http://evil.example.net/x") // no Origin; cross-site Referer
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	w := serve(srv, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-site Referer status = %d, want 403", w.Code)
	}
}

func TestServerRunListenErrorReturnsError(t *testing.T) {
	// A listen failure (invalid port) surfaces as a non-nil error from Run — it
	// is NOT swallowed as a clean shutdown.
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	if err := srv.Run("127.0.0.1:99999"); err == nil {
		t.Fatalf("Run on an invalid address must return an error")
	}
}

// TestRuleCriteriaViews covers the M-22 operator-context formatter directly:
// the empty case, and each qualifier branch (drift / pattern / value) plus a
// combined criterion, so the reviewer-facing Detail string is exercised end to
// end.
func TestRuleCriteriaViews(t *testing.T) {
	if got := ruleCriteriaViews(nil); got != nil {
		t.Fatalf("nil criteria should yield nil, got %v", got)
	}

	crit := []blnk.MatchingCriteria{
		{Field: "amount", Operator: "equals", AllowableDrift: 0.05},
		{Field: "reference", Operator: "contains", Pattern: "INV-*"},
		{Field: "currency", Operator: "equals", Value: "USD"},
		{Field: "description", Operator: "contains", Pattern: "wire", Value: "ACME", AllowableDrift: 0.1},
		{Field: "currency", Operator: "equals"}, // no qualifier -> empty Detail
	}
	got := ruleCriteriaViews(crit)
	if len(got) != len(crit) {
		t.Fatalf("expected %d views, got %d", len(crit), len(got))
	}
	wantDetail := []string{
		"drift 0.050",
		`pattern "INV-*"`,
		`value "USD"`,
		`drift 0.100, pattern "wire", value "ACME"`,
		"",
	}
	for i, w := range wantDetail {
		if got[i].Detail != w {
			t.Fatalf("criterion %d Detail = %q, want %q", i, got[i].Detail, w)
		}
		if got[i].Field != crit[i].Field || got[i].Operator != crit[i].Operator {
			t.Fatalf("criterion %d field/operator not carried through", i)
		}
	}
}
