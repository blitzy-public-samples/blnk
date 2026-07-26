package hitl

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

// reconStamp records a (break id, confirming reconciliation id) pair the cleared
// re_drive path durably stamps via MarkReDrivenClearedFromQueuedTx (finding
// F16). Tests assert the proof recon id was persisted with the re_driven status.
type reconStamp struct{ id, reconID string }

type fakeStore struct {
	mu sync.Mutex

	breaks []store.Break
	events []model.AuditEvent
	hitl   []store.HITLItem // rows returned by ListHITL (status-page reason column)

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

	pingErr        error
	setStatusErr   error // returned by SetBreakStatusFromQueuedTx (e.g. ErrNotQueued / ErrNotFound)
	dequeueErr     error // returned by DequeueHITLTx
	listBreaksErr  error
	countBreaksErr error // returned by CountBreaks (finding F06 pagination total)
	listAuditErr   error
	listHITLErr    error // returned by ListHITL (status-page queue read)

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
	reconStamps    []reconStamp // (id, reconID) stamped by MarkReDrivenClearedFromQueuedTx (finding F16)
	dequeueCalls   []string
	loadCalls      []string // LoadBreakTxn ids
	loadBreakCalls []string // LoadBreak ids
	claimCalls     []string
	releaseCalls   []string
}

func (f *fakeStore) ListBreaks(ctx context.Context) ([]store.Break, error) {
	return f.breaks, f.listBreaksErr
}

// ListBreaksPage returns one bounded in-memory page of f.breaks honoring the
// normalized Limit/Offset, so the /breaks and / handlers can be tested walking
// past the first page (finding F06). It reuses listBreaksErr for fault
// injection so existing error-path tests keep exercising the breaks read.
func (f *fakeStore) ListBreaksPage(ctx context.Context, page store.Page) ([]store.Break, error) {
	if f.listBreaksErr != nil {
		return nil, f.listBreaksErr
	}
	p := page.Normalize()
	if p.Offset >= len(f.breaks) {
		return nil, nil
	}
	end := p.Offset + p.Limit
	if end > len(f.breaks) {
		end = len(f.breaks)
	}
	return f.breaks[p.Offset:end], nil
}

// CountBreaks returns the total number of breaks (finding F06). countBreaksErr
// injects a count-read failure independently of the page read.
func (f *fakeStore) CountBreaks(ctx context.Context) (int, error) {
	if f.countBreaksErr != nil {
		return 0, f.countBreaksErr
	}
	return len(f.breaks), nil
}
func (f *fakeStore) ListAudit(ctx context.Context) ([]model.AuditEvent, error) {
	return f.events, f.listAuditErr
}

// ListHITL is the status-page read of the pending HITL queue; its rows supply
// the per-break routing reason surfaced in the "Reason" column (finding F-I).
func (f *fakeStore) ListHITL(ctx context.Context) ([]store.HITLItem, error) {
	return f.hitl, f.listHITLErr
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
	nRecon := len(f.reconStamps)
	nDequeue := len(f.dequeueCalls)
	f.mu.Unlock()

	err := fn(nil) // the mock's tx-bound methods ignore the *sql.Tx
	if err != nil {
		f.mu.Lock()
		f.statusCalls = f.statusCalls[:nStatus]
		f.reconStamps = f.reconStamps[:nRecon]
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

// MarkReDrivenClearedFromQueuedTx models the cleared-re_drive queued-only CAS
// that ALSO stamps the confirming reconciliation id (finding F16). It reuses the
// programmed setStatusErr (both are the same queued-only compare-and-set, so an
// ErrNotQueued/ErrNotFound test programs one field). On success it records BOTH
// the re_driven status transition (so lastStatus still observes it) and the
// (id, reconID) proof stamp, letting tests assert the proof was persisted
// atomically with the status change.
func (f *fakeStore) MarkReDrivenClearedFromQueuedTx(ctx context.Context, tx *sql.Tx, id, reconID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setStatusErr != nil {
		return f.setStatusErr
	}
	f.statusCalls = append(f.statusCalls, statusCall{id, statusReDriven})
	f.reconStamps = append(f.reconStamps, reconStamp{id, reconID})
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

// lastReconStamp returns the most recent (id, reconID) proof stamp committed by
// the cleared re_drive path (finding F16), and whether any stamp was committed.
func (f *fakeStore) lastReconStamp() (reconStamp, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reconStamps) == 0 {
		return reconStamp{}, false
	}
	return f.reconStamps[len(f.reconStamps)-1], true
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
	// readyErr controls the Blnk reachability dimension probed by /readyz
	// (finding F08): nil => Blnk reachable, non-nil => Blnk unreachable.
	readyErr error

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

// Ready satisfies the prober interface's Blnk-reachability probe used by /readyz
// (finding F08). It returns f.readyErr so a test can simulate Blnk up (nil) or
// down (non-nil) independently of the DB (fakeStore.pingErr) dimension.
func (f *fakeProber) Ready(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readyErr
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

// ----- Finding #5: control-character (NUL) input rejected with 400 ----------

func TestHandleDecisionControlCharExternalIDRejected(t *testing.T) {
	// Finding #5: an embedded NUL (U+0000) in the identifier must be rejected with
	// a clean 400 BEFORE the store is touched — never surface as an HTTP 500 from
	// lib/pq (which cannot store a NUL in a PostgreSQL text column). No mutation,
	// no audit.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "EXT-\x00001", Decision: audit.DecisionAccept, Reviewer: "alice"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("NUL external_txn_id status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "external_txn_id contains invalid control characters") {
		t.Fatalf("body = %s, want external_txn_id control-character message", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("a control-char decision must change no status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("a control-char decision must audit nothing; got %d", len(aud.recorded()))
	}
}

func TestHandleDecisionControlCharReviewerRejected(t *testing.T) {
	// Finding #5: a NUL / control character in the reviewer identity → 400.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "ali\x00ce"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("NUL reviewer status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reviewer contains invalid control characters") {
		t.Fatalf("body = %s, want reviewer control-character message", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("a control-char decision must change no status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("a control-char decision must audit nothing; got %d", len(aud.recorded()))
	}
}

func TestHandleDecisionControlCharNoteRejected(t *testing.T) {
	// Finding #5: a NUL in the free-text note → 400. A NUL is unstorable in a
	// PostgreSQL text column regardless of the note being free-text, so it is
	// rejected even though ordinary text whitespace in a note is allowed.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice", Note: "bad\x00note"})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("NUL note status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "note contains invalid control characters") {
		t.Fatalf("body = %s, want note control-character message", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("a control-char decision must change no status")
	}
}

func TestHandleDecisionNoteWithNewlineAccepted(t *testing.T) {
	// Finding #5 preserves legitimate free-text whitespace: a multi-line note
	// (containing \n / \r / \t) is still accepted and drives the decision — the
	// runtime-accepted "CRLF stored as a literal newline" behavior is not
	// regressed; only NUL and other control characters are rejected.
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})

	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "dup-1", Decision: audit.DecisionAccept, Reviewer: "alice",
		Note: "line one\nline two\twith tab\r\n"})

	if w.Code != http.StatusOK {
		t.Fatalf("multi-line note status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	last, ok := st.lastStatus()
	if !ok || last.status != statusAccepted {
		t.Fatalf("multi-line note decision must accept; lastStatus=%+v ok=%v", last, ok)
	}
}

func TestContainsDisallowedControlChar(t *testing.T) {
	// Direct coverage of both branches of the Finding #5 guard: identifiers reject
	// ALL control characters; the free-text note (allowTextWhitespace=true) permits
	// tab/newline/CR but still rejects NUL and every other control character.
	cases := []struct {
		name                string
		s                   string
		allowTextWhitespace bool
		want                bool
	}{
		{"plain identifier", "EXT-001-abc", false, false},
		{"nul in identifier", "EXT-\x00", false, true},
		{"bell in identifier", "EXT\x07", false, true},
		{"newline in identifier", "EXT\n", false, true},
		{"tab in identifier", "EXT\t", false, true},
		{"del in identifier", "EXT\x7f", false, true},
		{"plain note", "a normal note", true, false},
		{"newline note allowed", "line1\nline2", true, false},
		{"tab and cr note allowed", "a\tb\r\nc", true, false},
		{"nul note rejected", "a\x00b", true, true},
		{"bell note rejected", "a\x07b", true, true},
		{"empty", "", false, false},
	}
	for _, tc := range cases {
		if got := containsDisallowedControlChar(tc.s, tc.allowTextWhitespace); got != tc.want {
			t.Errorf("%s: containsDisallowedControlChar(%q, %v) = %v, want %v",
				tc.name, tc.s, tc.allowTextWhitespace, got, tc.want)
		}
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
		// F-CRIT-1: same-origin (not no-referrer) so a same-origin form POST
		// carries a real Origin and is not rejected 403 by the mutation guard.
		"Referrer-Policy": "same-origin",
		"Cache-Control":   "no-store",
	}
	for k, want := range checks {
		if got := h.Get(k); got != want {
			t.Fatalf("header %s = %q, want %q", k, got, want)
		}
	}
	csp := h.Get("Content-Security-Policy")
	if !strings.Contains(csp, "frame-ancestors 'none'") || !strings.Contains(csp, "form-action 'self'") {
		t.Fatalf("CSP = %q, want frame-ancestors 'none' + form-action 'self'", csp)
	}
	// F-INFO-1: the inline data: favicon must be permitted so no CSP error is
	// logged on every page load, WITHOUT relaxing the script restriction that
	// keeps injected markup inert (the XSS defense).
	if !strings.Contains(csp, "img-src 'self' data:") {
		t.Fatalf("CSP = %q, want img-src 'self' data: (favicon)", csp)
	}
	if !strings.Contains(csp, "script-src 'none'") {
		t.Fatalf("CSP = %q, want script-src 'none' preserved (XSS defense)", csp)
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
	// F16: the confirming recon id is durably STAMPED onto the break row as the
	// first-class clearance proof (resolved_recon_id), atomically with the status
	// change — not left blank as it was before.
	stamp, ok := st.lastReconStamp()
	if !ok || stamp.id != "low-1" || stamp.reconID != "rec_1" {
		t.Fatalf("expected proof recon rec_1 stamped on break low-1, got %+v (ok=%v)", stamp, ok)
	}
	// F16/M-12: every Blnk interaction is audited — the uniform re_drive_attempted,
	// rule_created, probed(recon), the DISTINCT terminal re_drive_cleared(recon),
	// and rule_compensated(deleted). The ambiguous legacy re_driven action is NOT
	// emitted.
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionReDriveAttempted); !ok {
		t.Fatalf("expected re_drive_attempted audit; events=%+v", evs)
	}
	rc, ok := firstWithAction(evs, audit.ActionRuleCreated)
	if !ok || rc.Provenance.RuleID != "rule_x" {
		t.Fatalf("expected rule_created audit for rule_x; events=%+v", evs)
	}
	pb, ok := firstWithAction(evs, audit.ActionProbed)
	if !ok || pb.Provenance.ReconID != "rec_1" {
		t.Fatalf("expected probed audit with recon rec_1; events=%+v", evs)
	}
	rd, ok := firstWithAction(evs, audit.ActionReDriveCleared)
	if !ok || rd.Provenance.ReconID != "rec_1" {
		t.Fatalf("expected terminal re_drive_cleared audit with recon rec_1; events=%+v", evs)
	}
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("cleared re_drive must emit re_drive_cleared, never the ambiguous re_driven; events=%+v", evs)
	}
	comp, ok := firstWithAction(evs, audit.ActionRuleCompensated)
	if !ok || comp.Provenance.RuleID != "rule_x" {
		t.Fatalf("expected rule_compensated audit for rule_x; events=%+v", evs)
	}
	// F16 (Bug B): the ephemeral probe rule's compensation must NOT be mislabeled
	// "non-clearing" on a re_drive that DID clear the break.
	if strings.Contains(strings.ToLower(comp.Rationale), "non-clearing") {
		t.Fatalf("compensation of a cleared re_drive's ephemeral rule mislabeled non-clearing: %q", comp.Rationale)
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
	// F16: the uniform attempt is audited, and the terminal outcome is a DISTINCT
	// re_drive_unmatched action (not the ambiguous re_driven) carrying NO recon_id
	// — only a Blnk-confirmed clearance carries one (m-04). No proof is stamped.
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionReDriveAttempted); !ok {
		t.Fatalf("expected re_drive_attempted audit; events=%+v", evs)
	}
	rd, ok := firstWithAction(evs, audit.ActionReDriveUnmatched)
	if !ok {
		t.Fatalf("expected a re_drive_unmatched audit; events=%+v", evs)
	}
	if rd.Provenance.ReconID != "" {
		t.Fatalf("uncleared re_drive terminal audit must carry NO recon_id, got %q", rd.Provenance.ReconID)
	}
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("uncleared re_drive must emit re_drive_unmatched, never the ambiguous re_driven; events=%+v", evs)
	}
	if _, ok := st.lastReconStamp(); ok {
		t.Fatalf("an uncleared re_drive must NOT stamp a clearance proof onto the break")
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
	if rd, ok := firstWithAction(evs, audit.ActionReDriveCleared); !ok || rd.Provenance.ReconID != "rec_3" {
		t.Fatalf("expected terminal re_drive_cleared with recon rec_3; events=%+v", evs)
	}
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("cleared re_drive must emit re_drive_cleared, never the ambiguous re_driven; events=%+v", evs)
	}
	// F16: the proof recon id is stamped on the break even when reusing a rule.
	if stamp, ok := st.lastReconStamp(); !ok || stamp.id != "b3" || stamp.reconID != "rec_3" {
		t.Fatalf("expected proof recon rec_3 stamped on break b3, got %+v (ok=%v)", stamp, ok)
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
	// F16/M-12: the FAILED create attempt is audited as a DISTINCT re_drive_failed
	// event (with a failure rationale and no recon_id), not silently dropped and
	// not the ambiguous re_driven. Because creation failed there is no
	// rule_created and nothing to compensate.
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionReDriveAttempted); !ok {
		t.Fatalf("expected re_drive_attempted audit; events=%+v", evs)
	}
	rd, ok := firstWithAction(evs, audit.ActionReDriveFailed)
	if !ok || rd.Provenance.ReconID != "" {
		t.Fatalf("expected an audited failed re_drive attempt with no recon_id; events=%+v", evs)
	}
	if !strings.Contains(strings.ToLower(rd.Rationale), "creation failed") {
		t.Fatalf("failure rationale = %q, want it to name the failed creation", rd.Rationale)
	}
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("re_drive failure must emit re_drive_failed, never the ambiguous re_driven; events=%+v", evs)
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
	// F16/M-12: the uniform ATTEMPT, rule_created (the successful create), the
	// FAILED probe attempt (a DISTINCT re_drive_failed action, no recon), and the
	// compensation outcome are all audited.
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionReDriveAttempted); !ok {
		t.Fatalf("expected re_drive_attempted audit; events=%+v", evs)
	}
	if _, ok := firstWithAction(evs, audit.ActionRuleCreated); !ok {
		t.Fatalf("expected rule_created audit; events=%+v", evs)
	}
	// F16: the Blnk-down failure is a first-class re_drive_failed action (never a
	// generic re_driven), carries the failure rationale, and no clearance recon.
	rd, ok := firstWithAction(evs, audit.ActionReDriveFailed)
	if !ok || rd.Provenance.ReconID != "" || !strings.Contains(strings.ToLower(rd.Rationale), "probe failed") {
		t.Fatalf("expected audited failed probe attempt; events=%+v", evs)
	}
	// The failure must NOT be recorded as the ambiguous legacy re_driven action.
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("re_drive failure must not emit the ambiguous re_driven action; events=%+v", evs)
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
	// F16/M-12: the uniform attempt is audited, and the failed attempt is a
	// DISTINCT re_drive_failed action (not the ambiguous legacy re_driven).
	evs := aud.recorded()
	if _, ok := firstWithAction(evs, audit.ActionReDriveAttempted); !ok {
		t.Fatalf("expected re_drive_attempted audit; events=%+v", evs)
	}
	if _, ok := firstWithAction(evs, audit.ActionReDriveFailed); !ok {
		t.Fatalf("expected an audited failed re_drive attempt (re_drive_failed); events=%+v", evs)
	}
	if countAction(evs, audit.ActionReDriven) != 0 {
		t.Fatalf("re_drive failure must not emit the ambiguous re_driven action; events=%+v", evs)
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

func mkPageBreak(id string) store.Break {
	return store.Break{
		Classification: model.BreakClassification{ExternalTxnID: id, RootCause: model.RootCause("timing"), Confidence: 0.9},
		Status:         "queued",
	}
}

// TestListBreaksPaginationMetadata asserts finding F06: /breaks returns ONE
// bounded page plus continuation metadata so a client can page past the first
// defaultPageLimit rows. With 3 breaks and ?limit=2 the first page returns 2
// rows with next_offset=2; the second (final) page returns the last row with
// next_offset=null — an unambiguous stop condition. This is the exact mechanism
// that makes a queued row beyond row 500 discoverable.
func TestListBreaksPaginationMetadata(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{mkPageBreak("b1"), mkPageBreak("b2"), mkPageBreak("b3")}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	type pag struct {
		Total      int  `json:"total"`
		Limit      int  `json:"limit"`
		Offset     int  `json:"offset"`
		Returned   int  `json:"returned"`
		NextOffset *int `json:"next_offset"`
	}
	decode := func(w *httptest.ResponseRecorder) ([]store.Break, pag) {
		t.Helper()
		var resp struct {
			Breaks     []store.Break `json:"breaks"`
			Pagination pag           `json:"pagination"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode /breaks: %v", err)
		}
		return resp.Breaks, resp.Pagination
	}

	// Page 1.
	w := doGET(t, srv, "/breaks?limit=2&offset=0")
	if w.Code != http.StatusOK {
		t.Fatalf("page1 status = %d, want 200", w.Code)
	}
	breaks, p := decode(w)
	if len(breaks) != 2 || breaks[0].Classification.ExternalTxnID != "b1" || breaks[1].Classification.ExternalTxnID != "b2" {
		t.Fatalf("page1 breaks = %+v, want b1,b2", breaks)
	}
	if p.Total != 3 || p.Limit != 2 || p.Offset != 0 || p.Returned != 2 {
		t.Fatalf("page1 pagination = %+v, want {total:3 limit:2 offset:0 returned:2}", p)
	}
	if p.NextOffset == nil || *p.NextOffset != 2 {
		t.Fatalf("page1 next_offset = %v, want 2", p.NextOffset)
	}

	// Page 2 (final): the last row, next_offset null.
	w = doGET(t, srv, "/breaks?limit=2&offset=2")
	breaks, p = decode(w)
	if len(breaks) != 1 || breaks[0].Classification.ExternalTxnID != "b3" {
		t.Fatalf("page2 breaks = %+v, want b3", breaks)
	}
	if p.Total != 3 || p.Returned != 1 {
		t.Fatalf("page2 pagination = %+v, want {total:3 returned:1}", p)
	}
	if p.NextOffset != nil {
		t.Fatalf("page2 next_offset = %v, want null (last page)", p.NextOffset)
	}
}

// TestListBreaksCountErrorReturns500 asserts a count-read failure is sanitized
// to a 500 rather than silently under-reporting the total (finding F06).
func TestListBreaksCountErrorReturns500(t *testing.T) {
	st := &fakeStore{countBreaksErr: errors.New("count failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/breaks")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 on count error", w.Code)
	}
	if strings.Contains(w.Body.String(), "count failed") {
		t.Fatalf("/breaks 500 leaked raw error: %s", w.Body.String())
	}
}

// TestStatusPagePaginationNav asserts finding F06 on the browser surface: the
// status page pages the breaks table and renders a "Showing X–Y of N" summary
// plus Prev/Next traversal links, so queued rows beyond the first page are
// reachable. With 3 breaks and ?limit=2, page 1 shows b1,b2 with a Next link
// (no Prev) and hides b3; page 2 shows b3 with a Prev link (no Next).
func TestStatusPagePaginationNav(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{mkPageBreak("b1"), mkPageBreak("b2"), mkPageBreak("b3")}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})

	// Page 1.
	body := doGET(t, srv, "/?limit=2&offset=0").Body.String()
	if !strings.Contains(body, "of 3 breaks") {
		t.Fatalf("page1 missing total summary; body:\n%s", body)
	}
	if !strings.Contains(body, `rel="next"`) || !strings.Contains(body, "offset=2") {
		t.Fatalf("page1 missing Next link to offset=2; body:\n%s", body)
	}
	if strings.Contains(body, `rel="prev"`) {
		t.Fatalf("page1 must NOT render a Prev link on the first page")
	}
	// Match the exact External-Txn-ID table cell (>b3<) rather than the bare
	// substring "b3", which would spuriously match the random hex CSRF token.
	if strings.Contains(body, ">b3<") {
		t.Fatalf("page1 must not include b3 (it belongs to page 2) — pagination not applied")
	}

	// Page 2 (final).
	body = doGET(t, srv, "/?limit=2&offset=2").Body.String()
	if !strings.Contains(body, "of 3 breaks") {
		t.Fatalf("page2 missing total summary; body:\n%s", body)
	}
	if !strings.Contains(body, `rel="prev"`) || !strings.Contains(body, "offset=0") {
		t.Fatalf("page2 missing Prev link to offset=0; body:\n%s", body)
	}
	if strings.Contains(body, `rel="next"`) {
		t.Fatalf("page2 must NOT render a Next link on the last page")
	}
	if !strings.Contains(body, ">b3<") {
		t.Fatalf("page2 must include b3 — the row hidden on page 1 is now discoverable")
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

// TestListenConfiguresHardeningTimeouts verifies the HITL http.Server is
// constructed with the bounded read/write/idle deadlines and the header-size cap
// (finding F09), rather than the former Addr+Handler-only server that left a
// slowloris client able to hold partial-request sockets open indefinitely.
func TestListenConfiguresHardeningTimeouts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	if err := srv.Listen(addr); err != nil {
		t.Fatalf("Listen(%s): %v", addr, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	srv.mu.Lock()
	hs := srv.httpServer
	srv.mu.Unlock()
	if hs == nil {
		t.Fatal("Listen did not publish an *http.Server")
	}
	if hs.ReadHeaderTimeout != readHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", hs.ReadHeaderTimeout, readHeaderTimeout)
	}
	if hs.ReadTimeout != readTimeout {
		t.Errorf("ReadTimeout = %v, want %v", hs.ReadTimeout, readTimeout)
	}
	if hs.WriteTimeout != writeTimeout {
		t.Errorf("WriteTimeout = %v, want %v", hs.WriteTimeout, writeTimeout)
	}
	if hs.IdleTimeout != idleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", hs.IdleTimeout, idleTimeout)
	}
	if hs.MaxHeaderBytes != maxHeaderBytes {
		t.Errorf("MaxHeaderBytes = %d, want %d", hs.MaxHeaderBytes, maxHeaderBytes)
	}
	// The slowloris defense that actually matters is a bounded header-read
	// deadline: assert it is positive AND strictly under the 8s partial-header
	// window the finding's reproduction uses, so an abusive socket is terminated
	// before then.
	if hs.ReadHeaderTimeout <= 0 || hs.ReadHeaderTimeout >= 8*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want a positive bound < 8s (slowloris defense)", hs.ReadHeaderTimeout)
	}
	// WriteTimeout must comfortably exceed the ~30s Blnk dry-run probe budget so a
	// legitimate long-running re_drive response is never truncated mid-flight.
	if hs.WriteTimeout < 60*time.Second {
		t.Errorf("WriteTimeout = %v, want >= 60s so re_drive's ~30s Blnk probe is never truncated", hs.WriteTimeout)
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
	// Both dimensions healthy: DB reachable (fakeStore ping ok) AND Blnk
	// reachable (fakeProber readyErr nil). /readyz reports 200 with both
	// dimensions "ok" (finding F08).
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"ready"`) {
		t.Fatalf("readyz body = %s, want status ready", body)
	}
	if !strings.Contains(body, `"database":"ok"`) || !strings.Contains(body, `"blnk":"ok"`) {
		t.Fatalf("readyz body = %s, want database:ok AND blnk:ok dimensions", body)
	}
}

func TestReadyzPingFailureReturns503Sanitized(t *testing.T) {
	// The raw driver error carries sensitive tokens ("pq:", "postgres") that must
	// never reach the client (M-03). The generic word "unreachable" is now a
	// legitimate dimension LABEL ("database":"unreachable"), so the leak assertion
	// targets the sensitive tokens themselves, not that label.
	st := &fakeStore{pingErr: errors.New("pq: dial tcp 10.0.0.5:5432: postgres connection refused")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "not ready") {
		t.Fatalf("readyz body = %s, want 'not ready'", body)
	}
	if !strings.Contains(body, `"database":"unreachable"`) {
		t.Fatalf("readyz body = %s, want database dimension labeled unreachable", body)
	}
	if strings.Contains(body, "pq:") || strings.Contains(body, "postgres") || strings.Contains(body, "10.0.0.5") {
		t.Fatalf("readyz 503 leaked raw ping error: %s", body)
	}
}

// TestReadyzBlnkUnreachableReturns503Degraded is the core F08 assertion: when the
// database is healthy but Blnk is unreachable, /readyz must NO LONGER report a
// false-green 200 {"status":"ready"} — it reports 503 {"status":"degraded"} with
// the Blnk dimension explicitly named, so a consumer sees the required Blnk
// dependency is down (re_drive cannot succeed until it returns).
func TestReadyzBlnkUnreachableReturns503Degraded(t *testing.T) {
	bc := &fakeProber{readyErr: errors.New("blnk not ready: GET /health returned status 503")}
	srv := newTestServer(&fakeStore{}, &fakeAudit{}, bc)
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503 when Blnk is unreachable (F08)", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"degraded"`) {
		t.Fatalf("readyz body = %s, want status degraded (F08)", body)
	}
	if !strings.Contains(body, `"database":"ok"`) || !strings.Contains(body, `"blnk":"unreachable"`) {
		t.Fatalf("readyz body = %s, want database:ok AND blnk:unreachable dimensions (F08)", body)
	}
	// The degraded response must NOT be the old false-green "ready".
	if strings.Contains(body, `"status":"ready"`) {
		t.Fatalf("readyz reported ready while Blnk is down (F08 regression): %s", body)
	}
	// The raw Blnk error must not leak to the client (M-03).
	if strings.Contains(body, "GET /health") || strings.Contains(body, "status 503") {
		t.Fatalf("readyz degraded response leaked raw blnk error: %s", body)
	}
}

// TestReadyzDatabaseDownTakesPrecedenceOverBlnk verifies the hard dependency is
// evaluated first: when BOTH the DB and Blnk are down, /readyz reports the DB as
// the not-ready cause (flat "not ready"), because without the DB no decision —
// not even accept/reject — can be served.
func TestReadyzDatabaseDownTakesPrecedenceOverBlnk(t *testing.T) {
	st := &fakeStore{pingErr: errors.New("db down")}
	bc := &fakeProber{readyErr: errors.New("blnk down")}
	srv := newTestServer(st, &fakeAudit{}, bc)
	w := doGET(t, srv, "/readyz")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"not ready"`) {
		t.Fatalf("readyz body = %s, want flat 'not ready' when DB is down", body)
	}
	if !strings.Contains(body, `"database":"unreachable"`) {
		t.Fatalf("readyz body = %s, want database dimension unreachable", body)
	}
	// Blnk must not even be consulted / reported once the hard DB dependency
	// fails, so the degraded-Blnk label must be absent.
	if strings.Contains(body, "degraded") {
		t.Fatalf("readyz reported degraded (Blnk) while DB is the real cause: %s", body)
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
	// F16: fail ONLY the rule_created audit — the uniform re_drive_attempted
	// audit (now written first) succeeds, so this exercises exactly the
	// rule_created-audit-failure path this test targets.
	aud := &fakeAudit{err: errors.New("pq: audit down"), failAction: audit.ActionRuleCreated}
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
	// F16: fail ONLY the re_drive_failed audit (the audit of the FAILED create
	// attempt) — the uniform re_drive_attempted audit succeeds — so this
	// exercises the "audit of the failure itself fails, but the 502 upstream
	// cause is still surfaced" path.
	aud := &fakeAudit{err: errors.New("pq: audit down"), failAction: audit.ActionReDriveFailed}
	bc := &fakeProber{createErr: errors.New("blnk unreachable")}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "bb-1", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (upstream cause preserved despite audit failure)", w.Code)
	}
}

// TestHandleReDriveAttemptAuditFailureFailsClosed asserts the NEW uniform
// re_drive_attempted audit fails closed (finding F16): if the very first audit —
// recording that a human initiated the re_drive — cannot be written, the
// decision returns 500 and NO Blnk work happens (no rule created, no probe, no
// status change), so an unaudited re_drive can never touch Blnk.
func TestHandleReDriveAttemptAuditFailureFailsClosed(t *testing.T) {
	st := &fakeStore{found: true, txn: blnk.ExternalTransaction{ID: "attempt-fail", Amount: 1, Currency: "USD"}}
	aud := &fakeAudit{err: errors.New("pq: audit down"), failAction: audit.ActionReDriveAttempted}
	bc := &fakeProber{createID: "rule_x", cleared: true, reconID: "rec_x"}
	srv := newTestServer(st, aud, bc)

	w := postDecisionJSON(t, srv, model.HITLDecision{ExternalTxnID: "attempt-fail", Decision: audit.DecisionReDrive, Reviewer: "alice"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when the re_drive_attempted audit fails", w.Code)
	}
	if bc.createCount() != 0 || bc.probeCount() != 0 {
		t.Fatalf("no Blnk work must happen when the attempt cannot be audited: creates=%d probes=%d", bc.createCount(), bc.probeCount())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("must not change status when the attempt audit fails closed")
	}
	// The lease is still claimed then released around the fail-closed attempt.
	if st.claimCount() != 1 || st.releaseCount() != 1 || st.heldLeases() != 0 {
		t.Fatalf("lease lifecycle = %d/%d/%d, want 1/1/0", st.claimCount(), st.releaseCount(), st.heldLeases())
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

// serverWithThreshold builds a status server whose auto-remediation threshold is
// set, so the below-threshold tag (finding F-A) can be exercised — newTestServer
// leaves the threshold at its zero value.
func serverWithThreshold(st breakStore, thr float64) *Server {
	return NewServer(config.Config{LLMModel: "kimi-k3", ConfAutoThreshold: thr}, st, &fakeAudit{}, &fakeProber{})
}

// TestStatusPageBelowThresholdTagAndHeader proves finding F-A: the page header
// surfaces the auto-remediation threshold, and a queued break whose confidence
// is below that threshold renders an explicit "below auto-threshold" tag so a
// human-gated break can never read as auto-eligible.
func TestStatusPageBelowThresholdTagAndHeader(t *testing.T) {
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-low",
				RootCause:     model.RootCauseTiming,
				Confidence:    0.40,
			},
			Status: statusQueued,
		}},
	}
	srv := serverWithThreshold(st, 0.85)
	w := doGET(t, srv, "/")
	if w.Code != http.StatusOK {
		t.Fatalf("status page = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "below auto-threshold") {
		t.Fatalf("below-threshold break must render the below-auto-threshold tag (F-A)")
	}
	if !strings.Contains(body, "0.8500") {
		t.Fatalf("status page header must surface the auto-remediation threshold 0.8500 (F-A)")
	}
}

// TestStatusPageAtOrAboveThresholdNoTag proves the F-A tag is confined to breaks
// strictly below the threshold: a confident break carries no tag.
func TestStatusPageAtOrAboveThresholdNoTag(t *testing.T) {
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-high",
				RootCause:     model.RootCauseTiming,
				Confidence:    0.95,
			},
			Status: statusQueued,
		}},
	}
	srv := serverWithThreshold(st, 0.85)
	w := doGET(t, srv, "/")
	body := w.Body.String()
	if strings.Contains(body, "below auto-threshold") {
		t.Fatalf("a break at/above the threshold must NOT render the below-auto-threshold tag (F-A)")
	}
}

// TestStatusPageReasonColumn proves finding F-I: the status page shows a Reason
// column, and a queued break's HITL routing reason (from ListHITL) is surfaced
// against its row.
func TestStatusPageReasonColumn(t *testing.T) {
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-reg",
				RootCause:     model.RootCauseUnknown,
				Confidence:    0.10,
			},
			Status: statusQueued,
		}},
		hitl: []store.HITLItem{{ExternalTxnID: "b-reg", Reason: "regulated flow requires human review"}},
	}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusOK {
		t.Fatalf("status page = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `<th scope="col">Reason</th>`) {
		t.Fatalf("breaks table must include a Reason column header (F-I)")
	}
	if !strings.Contains(body, "regulated flow requires human review") {
		t.Fatalf("queued break must surface its HITL routing reason (F-I)")
	}
}

// TestStatusPageUnnamedRulePlaceholder proves finding F-E: a proposed rule with
// an empty Name renders a "(unnamed rule)" placeholder rather than a blank cell,
// so the reviewer always sees that a rule was proposed.
func TestStatusPageUnnamedRulePlaceholder(t *testing.T) {
	st := &fakeStore{
		breaks: []store.Break{{
			Classification: model.BreakClassification{
				ExternalTxnID: "b-unnamed",
				RootCause:     model.RootCauseAmountDrift,
				Confidence:    0.50,
				ProposedRule: &blnk.MatchingRule{
					Name: "",
					Criteria: []blnk.MatchingCriteria{
						{Field: fieldAmount, Operator: operatorEquals, AllowableDrift: 0.01},
					},
				},
			},
			Status: statusQueued,
		}},
	}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	body := w.Body.String()
	if !strings.Contains(body, "(unnamed rule)") {
		t.Fatalf("an unnamed proposed rule must render the (unnamed rule) placeholder (F-E)")
	}
}

// TestStatusPageAuditProvenanceColumns proves finding F-K: the audit table
// surfaces the break origin (source label + Blnk upload batch id) from the event
// provenance so a reviewer can trace which statement/batch a break came from.
func TestStatusPageAuditProvenanceColumns(t *testing.T) {
	st := &fakeStore{
		events: []model.AuditEvent{{
			ExternalTxnID: "b-src",
			Actor:         "agent",
			Action:        audit.ActionClassified,
			Confidence:    0.72,
			Provenance:    model.Provenance{Model: "kimi-k3", Source: "acme-bank-stmt", UploadID: "upload-4242"},
		}},
	}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusOK {
		t.Fatalf("status page = %d, want 200", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`<th scope="col">Source</th>`, `<th scope="col">Upload ID</th>`, "acme-bank-stmt", "upload-4242"} {
		if !strings.Contains(body, want) {
			t.Fatalf("audit table must surface provenance origin %q (F-K)", want)
		}
	}
}

// TestStatusPageHumanDecisionConfidenceEmDash proves finding F-J: a human
// decision (accepted / re_driven / rejected) is recorded with no classification
// confidence, so the audit row renders an em-dash rather than a misleading
// "0.0000" — while an agent action keeps its genuine numeric confidence.
func TestStatusPageHumanDecisionConfidenceEmDash(t *testing.T) {
	st := &fakeStore{
		events: []model.AuditEvent{
			{ExternalTxnID: "b-1", Actor: "operator", Action: audit.ActionAccepted, Confidence: 0},
			{ExternalTxnID: "b-1", Actor: "agent", Action: audit.ActionClassified, Confidence: 0.7300},
		},
	}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	body := w.Body.String()
	if !strings.Contains(body, "&mdash;") {
		t.Fatalf("a human-decision audit row must render an em-dash for confidence (F-J)")
	}
	if !strings.Contains(body, "0.7300") {
		t.Fatalf("an agent audit row must keep its genuine numeric confidence (F-J)")
	}
}

// TestStatusPageListHITLErrorReturns500 covers the queue-read failure path added
// for the Reason column (F-I): a ListHITL error is handled like a breaks/audit
// read failure — the sanitized error panel with a 500, never a partial page.
func TestStatusPageListHITLErrorReturns500(t *testing.T) {
	st := &fakeStore{listHITLErr: errors.New("hitl queue query failed")}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := doGET(t, srv, "/")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 when ListHITL fails", w.Code)
	}
}

// ============================================================================
// Group G — HITL handler validation & response contract (F21/F22/F23/F24)
// ============================================================================

// ----- F21: one documented length unit (runes); browser-valid is server-valid --

// TestValidateDecisionMultibyteWithinRuneBoundAccepted proves the byte-vs-UTF-16
// mismatch (F21) is gone: values the browser's maxlength (UTF-16 code units)
// admits — but that EXCEEDED the old UTF-8 BYTE limit — are now accepted, because
// the server bounds by RUNES. Each case previously returned a spurious 400.
func TestValidateDecisionMultibyteWithinRuneBoundAccepted(t *testing.T) {
	cases := []struct{ name, reviewer, note string }{
		// 33 emoji: 33 runes (<=128) but 132 UTF-8 bytes (>128, the old limit).
		{"33-emoji-reviewer", strings.Repeat("😀", 33), ""},
		// 65 precomposed "é": 65 runes (<=128) but 130 UTF-8 bytes.
		{"65-eacute-reviewer", strings.Repeat("é", 65), ""},
		// 513 emoji note: 513 runes (<=2048) but 2052 UTF-8 bytes.
		{"513-emoji-note", "alice", strings.Repeat("😀", 513)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{found: true}
			srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
			w := postDecisionJSON(t, srv, model.HITLDecision{
				ExternalTxnID: "ext-1", Decision: audit.DecisionAccept, Reviewer: tc.reviewer, Note: tc.note})
			if w.Code != http.StatusOK {
				t.Fatalf("%s: status = %d, want 200 (browser-valid multibyte must not be rejected server-side); body=%s",
					tc.name, w.Code, w.Body.String())
			}
			if _, ok := st.lastStatus(); !ok {
				t.Fatalf("%s: expected the decision to commit a status change", tc.name)
			}
		})
	}
}

// TestValidateDecisionReviewerRuneUpperBound proves the reviewer bound is enforced
// in RUNES: exactly maxReviewerLen runes is accepted and one more is rejected —
// using emoji so the assertion cannot pass under a byte or UTF-16 interpretation
// (maxReviewerLen emoji is 4x the bytes and 2x the UTF-16 units of the limit).
func TestValidateDecisionReviewerRuneUpperBound(t *testing.T) {
	// exactly the limit, in runes → accepted
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: strings.Repeat("😀", maxReviewerLen)})
	if w.Code != http.StatusOK {
		t.Fatalf("reviewer of exactly %d runes: status = %d, want 200; body=%s", maxReviewerLen, w.Code, w.Body.String())
	}
	// one rune over the limit → rejected
	st2 := &fakeStore{found: true}
	aud2 := &fakeAudit{}
	srv2 := newTestServer(st2, aud2, &fakeProber{})
	w2 := postDecisionJSON(t, srv2, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: strings.Repeat("😀", maxReviewerLen+1)})
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("reviewer of %d runes: status = %d, want 400", maxReviewerLen+1, w2.Code)
	}
	if !strings.Contains(w2.Body.String(), "reviewer is too long") {
		t.Fatalf("body = %s, want 'reviewer is too long'", w2.Body.String())
	}
	if _, ok := st2.lastStatus(); ok {
		t.Fatalf("over-long reviewer must change no status")
	}
	if len(aud2.recorded()) != 0 {
		t.Fatalf("over-long reviewer must audit nothing")
	}
}

// TestValidateDecisionNoteRuneUpperBound proves the note bound is likewise
// enforced in runes at its exact boundary.
func TestValidateDecisionNoteRuneUpperBound(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice", Note: strings.Repeat("😀", maxNoteLen)})
	if w.Code != http.StatusOK {
		t.Fatalf("note of exactly %d runes: status = %d, want 200; body=%s", maxNoteLen, w.Code, w.Body.String())
	}
	st2 := &fakeStore{found: true}
	srv2 := newTestServer(st2, &fakeAudit{}, &fakeProber{})
	w2 := postDecisionJSON(t, srv2, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice", Note: strings.Repeat("😀", maxNoteLen+1)})
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("note of %d runes: status = %d, want 400", maxNoteLen+1, w2.Code)
	}
	if !strings.Contains(w2.Body.String(), "note is too long") {
		t.Fatalf("body = %s, want 'note is too long'", w2.Body.String())
	}
}

// TestStatusPageFormMaxlengthMatchesRuneBounds proves F21's single source of
// truth: the decision form's HTML maxlength attributes are rendered FROM the
// server-side rune bounds (maxReviewerLen/maxNoteLen), so the client-side limit
// can never drift from what validateDecisionFields enforces.
func TestStatusPageFormMaxlengthMatchesRuneBounds(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "EXT-1", RootCause: model.RootCauseTiming},
		Status:         statusQueued,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()
	for _, want := range []string{
		fmt.Sprintf(`name="reviewer" placeholder="reviewer id" required maxlength="%d"`, maxReviewerLen),
		fmt.Sprintf(`name="note" placeholder="note (optional)" maxlength="%d"`, maxNoteLen),
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page form missing rune-bound maxlength %q", want)
		}
	}
}

// ----- F23: unspoofable actor — reject bidi controls & malformed UTF-8 --------

// TestValidateDecisionReviewerBidiOverrideRejected proves a right-to-left override
// in the reviewer (the Trojan-Source spoof that renders "qa-<RLO>nimda" as the
// plausible "qa-admin") is REJECTED before it can become the immutable audit
// actor (F23) — no status change, no audit event.
func TestValidateDecisionReviewerBidiOverrideRejected(t *testing.T) {
	st := &fakeStore{found: true}
	aud := &fakeAudit{}
	srv := newTestServer(st, aud, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "qa-\u202Enimda"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bidi reviewer status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reviewer contains disallowed formatting characters") {
		t.Fatalf("body = %s, want reviewer formatting-character message", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("bidi reviewer must change no status")
	}
	if len(aud.recorded()) != 0 {
		t.Fatalf("bidi reviewer must audit nothing")
	}
}

// TestValidateDecisionReviewerFormatCharsRejected sweeps the invisible/format
// codepoints an identifier must never carry (F23): bidi controls, isolates,
// directional marks, zero-width space, soft hyphen, and the BOM — all category
// Cf — are each rejected in the reviewer.
func TestValidateDecisionReviewerFormatCharsRejected(t *testing.T) {
	cases := []struct {
		name string
		r    rune
	}{
		{"RLO-U+202E", '\u202E'},
		{"LRO-U+202D", '\u202D'},
		{"LRI-U+2066", '\u2066'},
		{"PDI-U+2069", '\u2069'},
		{"LRM-U+200E", '\u200E'},
		{"ZWSP-U+200B", '\u200B'},
		{"soft-hyphen-U+00AD", '\u00AD'},
		{"BOM-U+FEFF", '\uFEFF'},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{found: true}
			srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
			w := postDecisionJSON(t, srv, model.HITLDecision{
				ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "qa" + string(tc.r) + "bob"})
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s reviewer status = %d, want 400; body=%s", tc.name, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "reviewer contains disallowed formatting characters") {
				t.Fatalf("%s: body = %s, want formatting-character message", tc.name, w.Body.String())
			}
		})
	}
}

// TestValidateDecisionReviewerReplacementCharRejected proves a reviewer carrying
// U+FFFD — the artifact Go's form parser leaves when it silently "repairs"
// undecodable request bytes — is rejected rather than persisted as a lossy,
// unattributable actor (F23).
func TestValidateDecisionReviewerReplacementCharRejected(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "qa-\uFFFD-user"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("U+FFFD reviewer status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reviewer contains invalid text encoding") {
		t.Fatalf("body = %s, want reviewer encoding message", w.Body.String())
	}
}

// TestValidateDecisionReviewerRawInvalidUTF8FormRejected proves the OTHER
// malformed-UTF-8 path (F23): a raw undecodable byte (0xFF) posted in a urlencoded
// form value is rejected as invalid text encoding — exercising the
// utf8.ValidString guard, since url-unescaping delivers the byte UNREPAIRED
// (unlike the U+FFFD form).
func TestValidateDecisionReviewerRawInvalidUTF8FormRejected(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := "external_txn_id=x&decision=accept&reviewer=qa-%FF-user&csrf_token=" + testCSRF
	req := httptest.NewRequest(http.MethodPost, "/decisions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "http://"+req.Host)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: testCSRF})
	w := serve(srv, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("raw-invalid-UTF8 reviewer status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "invalid text encoding") {
		t.Fatalf("body = %s, want 'invalid text encoding'", w.Body.String())
	}
	if _, ok := st.lastStatus(); ok {
		t.Fatalf("raw-invalid-UTF8 reviewer must change no status")
	}
}

// TestValidateDecisionNoteBidiRejectedButEmojiZWJAllowed proves the note path is
// strict on the visual-spoofing bidi controls yet still permits a legitimate
// emoji zero-width-joiner sequence (identifier=false) (F23).
func TestValidateDecisionNoteBidiRejectedButEmojiZWJAllowed(t *testing.T) {
	// bidi control in the note → rejected
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice", Note: "see \u202Ethis"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bidi note status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "note contains disallowed formatting characters") {
		t.Fatalf("body = %s, want note formatting-character message", w.Body.String())
	}
	// legitimate emoji ZWJ sequence in the note → accepted (man+ZWJ+woman+ZWJ+girl)
	st2 := &fakeStore{found: true}
	srv2 := newTestServer(st2, &fakeAudit{}, &fakeProber{})
	w2 := postDecisionJSON(t, srv2, model.HITLDecision{
		ExternalTxnID: "x", Decision: audit.DecisionAccept, Reviewer: "alice",
		Note: "family \U0001F468\u200D\U0001F469\u200D\U0001F467"})
	if w2.Code != http.StatusOK {
		t.Fatalf("emoji-ZWJ note status = %d, want 200 (legitimate ZWJ must be allowed); body=%s", w2.Code, w2.Body.String())
	}
}

// TestValidateDecisionExternalTxnIDFormatCharRejected proves an identifier field
// other than the reviewer is held to the same F23 rule: a bidi control in the
// external txn id is rejected.
func TestValidateDecisionExternalTxnIDFormatCharRejected(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postDecisionJSON(t, srv, model.HITLDecision{
		ExternalTxnID: "EXT\u202E1", Decision: audit.DecisionAccept, Reviewer: "alice"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bidi txn id status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "external_txn_id contains disallowed formatting characters") {
		t.Fatalf("body = %s, want external_txn_id formatting-character message", w.Body.String())
	}
}

// ----- F24: mixed-case JSON media type is negotiated consistently -------------

// postJSONWithContentType posts a JSON body under an explicit (possibly
// mixed-case) Content-Type, with no Origin header (a trusted non-browser API
// client), so the response-format negotiation can be asserted in isolation.
func postJSONWithContentType(srv *Server, ct string, d model.HITLDecision) *httptest.ResponseRecorder {
	b, _ := json.Marshal(d)
	req := httptest.NewRequest(http.MethodPost, "/decisions", bytes.NewReader(b))
	req.Header.Set("Content-Type", ct)
	return serve(srv, req)
}

// TestRespondDecisionMixedCaseJSONReturnsJSONSuccess proves F24: a valid decision
// posted with a mixed-case media type ("APPLICATION/JSON") — which is bound and
// COMMITTED as JSON — also RECEIVES the JSON success schema (200 + recorded),
// not the browser 303 redirect it previously fell through to.
func TestRespondDecisionMixedCaseJSONReturnsJSONSuccess(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postJSONWithContentType(srv, "APPLICATION/JSON", model.HITLDecision{
		ExternalTxnID: "dup-1", Decision: audit.DecisionAccept, Reviewer: "alice"})
	if w.Code != http.StatusOK {
		t.Fatalf("mixed-case JSON status = %d, want 200 (JSON success, not 303); body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("response Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(w.Body.String(), `"status":"recorded"`) {
		t.Fatalf("body = %s, want the JSON recorded schema", w.Body.String())
	}
	if _, ok := st.lastStatus(); !ok {
		t.Fatalf("mixed-case JSON decision must commit the status change")
	}
}

// TestRespondDecisionErrorMixedCaseJSONReturnsJSONError proves the error path is
// negotiated the same way (F24): a mixed-case JSON request for a MISSING break
// gets the JSON error schema (404 + {"error":...}), never the HTML error panel.
func TestRespondDecisionErrorMixedCaseJSONReturnsJSONError(t *testing.T) {
	st := &fakeStore{setStatusErr: store.ErrNotFound}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	w := postJSONWithContentType(srv, "Application/Json", model.HITLDecision{
		ExternalTxnID: "missing-1", Decision: audit.DecisionAccept, Reviewer: "bob"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("mixed-case JSON missing-break status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("error response Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(w.Body.String(), `"error"`) {
		t.Fatalf("body = %s, want the JSON error schema", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "<!DOCTYPE html>") {
		t.Fatalf("mixed-case JSON error must NOT return the HTML error page")
	}
}

// ----- F22: accessible error page --------------------------------------------

// TestErrorPageHTMLAccessibleLandmarks asserts the standalone error surface
// carries the semantics F22 requires: exactly one <main> landmark (so
// landmark-one-main passes), a top-level <h1>, an announced alert region
// (role="alert" + aria-live), a deterministic focus target (tabindex + autofocus,
// no client JS under the strict CSP), and a >=44px Back tap target. The message
// is still HTML-escaped (M-03).
func TestErrorPageHTMLAccessibleLandmarks(t *testing.T) {
	out := string(errorPageHTML("boom <x>"))
	if n := strings.Count(out, "<main>"); n != 1 {
		t.Fatalf("error page has %d <main> landmarks, want exactly 1 (landmark-one-main)", n)
	}
	for _, want := range []string{
		"<h1>",
		`role="alert"`,
		`aria-live="assertive"`,
		`tabindex="-1"`,
		"autofocus",
		`class="back"`,
		"min-height: 44px",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("error page missing accessibility affordance %q", want)
		}
	}
	if !strings.Contains(out, "boom &lt;x&gt;") || strings.Contains(out, "boom <x>") {
		t.Fatalf("error page did not HTML-escape the message: %s", out)
	}
}

// TestFormValidationErrorRendersAccessibleErrorPage proves the wired path (F22):
// a browser FORM submission that fails validation renders the accessible error
// page (with <main>, <h1>, and the alert region), not a bare panel.
func TestFormValidationErrorRendersAccessibleErrorPage(t *testing.T) {
	st := &fakeStore{found: true}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	form := url.Values{}
	form.Set("external_txn_id", "x")
	form.Set("decision", "not-a-decision") // invalid verb → 400 validation error
	form.Set("reviewer", "alice")
	w := postDecisionForm(t, srv, form)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid-decision form status = %d, want 400; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"<main>", `role="alert"`, "<h1>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("form error page missing %q", want)
		}
	}
}

// ============================================================================
// Group H — HITL status-page visual & accessibility (F19/F20)
// ============================================================================

// TestStatusPageClassifiedStatusStyled proves F19: the transient `classified`
// lifecycle state receives the SAME explicit semantic treatment (a dedicated
// status color + font-weight 600) as every other status, instead of rendering as
// ordinary, unstyled body text. It asserts both that a classified row is emitted
// with the status-classified class AND that the stylesheet defines that class.
func TestStatusPageClassifiedStatusStyled(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "EXT-CL", RootCause: model.RootCauseTiming},
		Status:         model.StatusClassified,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()

	// the row is emitted with the status-specific class (same pattern as siblings)
	if !strings.Contains(body, `class="status-classified">classified<`) {
		t.Fatalf("classified row not rendered with the status-classified class")
	}
	// the stylesheet defines the class with an explicit color and weight 600,
	// consistent with the other .status-* rules (never default body text).
	if !strings.Contains(body, ".status-classified { color: #5a4b8b; font-weight: 600; }") {
		t.Fatalf("stylesheet missing a consistent .status-classified rule (color + weight 600)")
	}
	// guard the consistency invariant: every status class the template can emit
	// has a matching styled rule, so no lifecycle state is ever left unstyled.
	for _, cls := range []string{
		".status-auto-resolved", ".status-rejected", ".status-re_driven",
		".status-queued", ".status-accepted", ".status-classified",
	} {
		if !strings.Contains(body, cls+" { color:") {
			t.Fatalf("stylesheet missing a color rule for %s", cls)
		}
	}
}

// TestStatusPageScrollableRegionsFocusable proves F20 (WCAG 2.1.1 Keyboard): each
// horizontally-scrollable table wrapper is a focusable, named region so a
// keyboard-only user can Tab to it and arrow-scroll to reveal off-screen columns
// (e.g. the audit trail's Recon ID / Source / Upload ID / Rationale) at narrow
// widths — and a visible focus ring marks it. Both the breaks and audit wrappers
// carry role="region", tabindex="0", and an aria-labelledby naming them.
func TestStatusPageScrollableRegionsFocusable(t *testing.T) {
	st := &fakeStore{breaks: []store.Break{{
		Classification: model.BreakClassification{ExternalTxnID: "EXT-1", RootCause: model.RootCauseTiming},
		Status:         statusQueued,
	}}}
	srv := newTestServer(st, &fakeAudit{}, &fakeProber{})
	body := doGET(t, srv, "/").Body.String()

	for _, want := range []string{
		// audit overflow region (the finding's primary target): focusable + named
		`<div class="table-wrap" tabindex="0" role="region" aria-labelledby="audit-heading">`,
		// breaks overflow region: same treatment so neither region is a keyboard trap
		`<div class="table-wrap" tabindex="0" role="region" aria-labelledby="breaks-heading">`,
		// a visible focus indicator for the focused scroll region
		".table-wrap:focus { outline:",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("status page missing scrollable-region-focusable affordance %q", want)
		}
	}
	// the section headings that name the regions must exist as label targets.
	for _, id := range []string{`id="breaks-heading"`, `id="audit-heading"`} {
		if !strings.Contains(body, id) {
			t.Fatalf("status page missing region label target %q", id)
		}
	}
}
