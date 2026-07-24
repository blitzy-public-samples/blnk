package remediator

// Comprehensive unit tests for the auto-remediation decision core. This is the
// boundary file whose absence drove finding M-1 (remediator at 0% coverage,
// module aggregate below the Rule 5.9 floor).
//
// The suite uses ONLY the standard library plus the recon-agent's own packages
// (no testify, matching the store/audit test convention and the pinned-deps
// constraint). Its centerpiece is fakeBackend, a single fake that implements
// BOTH storePort AND auditPort. Because production threads one *sql.Tx through
// store.WithTx into audit.RecordTx so a state change and its audit event commit
// together, a faithful fake must model that shared transaction: fakeBackend
// buffers every transaction-bound write and only applies it to committed state
// when the transaction commits, discarding the buffer on rollback. That lets
// the tests reproduce the QA fault-injection scenarios (FI1/FI3/FI5/FI6) at the
// decision-core level and assert that NO partial outcome is ever durable
// (findings C-1, C-2, M-2, M-4, Rule 5.3).

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/classifier"
	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// Compile-time proof that the module's own concrete types satisfy the ports the
// remediator depends on. This is the Gate 9 wiring guarantee: the object graph
// cmd/main.go builds (real classifier, Blnk client, audit writer, store) is
// structurally injectable into New, even though New has no runtime caller yet.
var (
	_ storePort      = (*store.Store)(nil)
	_ auditPort      = (*audit.Writer)(nil)
	_ classifierPort = (*classifier.Classifier)(nil)
	_ blnkPort       = (*blnk.Client)(nil)
)

const testThreshold = 0.85

// testUploadID is the reconciliation upload batch id threaded through Handle in
// the tests. It is asserted onto the provenance of emitted audit events to lock
// in finding m-2 (provenance.upload_id is populated, no longer always empty).
const testUploadID = "upload_test"

// -----------------------------------------------------------------------------
// Minimal database/sql driver used ONLY to mint real *sql.Tx values so
// fakeBackend can key its per-transaction buffers by the *sql.Tx pointer and
// exercise real Begin/Commit/Rollback. No statement is ever executed against it.
// -----------------------------------------------------------------------------

type rdriver struct{}

func (rdriver) Open(string) (driver.Conn, error) { return rconn{}, nil }

type rconn struct{}

func (rconn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("no prepare") }
func (rconn) Close() error                        { return nil }
func (rconn) Begin() (driver.Tx, error)           { return rtx{}, nil }

type rtx struct{}

func (rtx) Commit() error   { return nil }
func (rtx) Rollback() error { return nil }

func init() { sql.Register("remediatorfake", rdriver{}) }

func newFakeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("remediatorfake", "")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	return db
}

// -----------------------------------------------------------------------------
// Fake classifier
// -----------------------------------------------------------------------------

type fakeClassifier struct {
	mu     sync.Mutex
	result model.BreakClassification
	err    error
	calls  int
}

func (f *fakeClassifier) Classify(_ context.Context, _ blnk.ExternalTransaction) (model.BreakClassification, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.result, f.err
}

func (f *fakeClassifier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// -----------------------------------------------------------------------------
// Fake Blnk client
// -----------------------------------------------------------------------------

type fakeBlnk struct {
	mu           sync.Mutex
	created      blnk.MatchingRule
	createErr    error
	cleared      bool
	reconID      string
	probeErr     error
	createCalls  int
	probeCalls   int
	probeRuleIDs [][]string
}

func (f *fakeBlnk) CreateMatchingRule(_ context.Context, _ blnk.MatchingRule) (blnk.MatchingRule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.createErr != nil {
		return blnk.MatchingRule{}, f.createErr
	}
	return f.created, nil
}

func (f *fakeBlnk) ProbeBreak(_ context.Context, _ blnk.ExternalTransaction, ids []string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeCalls++
	f.probeRuleIDs = append(f.probeRuleIDs, ids)
	if f.probeErr != nil {
		return false, "", f.probeErr
	}
	return f.cleared, f.reconID, nil
}

func (f *fakeBlnk) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls
}

func (f *fakeBlnk) probeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probeCalls
}

func (f *fakeBlnk) lastProbeRuleIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.probeRuleIDs) == 0 {
		return nil
	}
	return f.probeRuleIDs[len(f.probeRuleIDs)-1]
}

// -----------------------------------------------------------------------------
// fakeBackend: implements BOTH storePort and auditPort, modelling the shared
// transaction so atomicity can be asserted.
// -----------------------------------------------------------------------------

type fakeBreak struct {
	classification model.BreakClassification
	status         string
	createdRuleID  string
}

type fakeBackend struct {
	mu  sync.Mutex
	db  *sql.DB
	seq int // monotonically increases so tests can detect double-processing

	breaks map[string]*fakeBreak
	hitl   map[string]string
	audits []model.AuditEvent

	// per-transaction buffered mutations, keyed by the *sql.Tx WithTx minted.
	pending map[*sql.Tx][]func()

	// Fault-injection hooks (nil => success). Returning a non-nil error from a
	// transaction-bound write aborts fn, forcing WithTx to roll back and discard
	// every buffered mutation of that transaction.
	loadErr              error
	countAuditErr        error
	beginErr             error
	recordHook           func(model.AuditEvent) error
	recordTxHook         func(model.AuditEvent) error
	upsertBreakTxHook    func(model.BreakClassification, string) error
	setBreakStatusTxHook func(string, string) error
	enqueueHITLTxHook    func(string, string) error
	setCreatedRuleTxHook func(string, string) error
}

func newBackend(t *testing.T) *fakeBackend {
	return &fakeBackend{
		db:      newFakeDB(t),
		breaks:  map[string]*fakeBreak{},
		hitl:    map[string]string{},
		pending: map[*sql.Tx][]func(){},
	}
}

// seedBreak preloads committed state, simulating a break left by a prior
// (possibly crashed) attempt — used by the idempotency/resume tests.
func (f *fakeBackend) seedBreak(id string, c model.BreakClassification, status, createdRuleID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c.ExternalTxnID = id
	f.breaks[id] = &fakeBreak{classification: c, status: status, createdRuleID: createdRuleID}
}

// seedAudit preloads a committed audit event for a break, simulating an event a
// prior (possibly crashed) attempt durably recorded before completing its side
// effect. Used by the SEAM-MIN-1 resume test to plant the rule_proposed event
// that is written BEFORE the CreateMatchingRule POST.
func (f *fakeBackend) seedAudit(id, action string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audits = append(f.audits, model.AuditEvent{ExternalTxnID: id, Action: action})
}

func (f *fakeBackend) buffer(tx *sql.Tx, m func()) {
	f.mu.Lock()
	f.pending[tx] = append(f.pending[tx], m)
	f.mu.Unlock()
}

// --- storePort ---

func (f *fakeBackend) LoadBreak(_ context.Context, id string) (model.BreakClassification, string, string, bool, error) {
	if f.loadErr != nil {
		return model.BreakClassification{}, "", "", false, f.loadErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.breaks[id]
	if b == nil {
		return model.BreakClassification{}, "", "", false, nil
	}
	return b.classification, b.status, b.createdRuleID, true, nil
}

func (f *fakeBackend) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	if f.beginErr != nil {
		return f.beginErr
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.pending[tx] = nil
	f.mu.Unlock()

	if ferr := fn(tx); ferr != nil {
		_ = tx.Rollback()
		f.mu.Lock()
		delete(f.pending, tx)
		f.mu.Unlock()
		return ferr
	}
	if cerr := tx.Commit(); cerr != nil {
		f.mu.Lock()
		delete(f.pending, tx)
		f.mu.Unlock()
		return cerr
	}
	// Commit succeeded: apply the buffered mutations atomically.
	f.mu.Lock()
	muts := f.pending[tx]
	delete(f.pending, tx)
	f.seq++
	for _, m := range muts {
		m()
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) UpsertBreakTx(_ context.Context, tx *sql.Tx, c model.BreakClassification, status string) error {
	if f.upsertBreakTxHook != nil {
		if err := f.upsertBreakTxHook(c, status); err != nil {
			return err
		}
	}
	f.buffer(tx, func() {
		b := f.breaks[c.ExternalTxnID]
		if b == nil {
			b = &fakeBreak{}
			f.breaks[c.ExternalTxnID] = b
		}
		// Preserve createdRuleID across an upsert, matching the store's
		// ON CONFLICT DO UPDATE that never overwrites created_rule_id.
		b.classification = c
		b.status = status
	})
	return nil
}

func (f *fakeBackend) SetBreakStatusTx(_ context.Context, tx *sql.Tx, id, status string) error {
	if f.setBreakStatusTxHook != nil {
		if err := f.setBreakStatusTxHook(id, status); err != nil {
			return err
		}
	}
	f.buffer(tx, func() {
		if b := f.breaks[id]; b != nil {
			b.status = status
		}
	})
	return nil
}

func (f *fakeBackend) EnqueueHITLTx(_ context.Context, tx *sql.Tx, id, reason string) error {
	if f.enqueueHITLTxHook != nil {
		if err := f.enqueueHITLTxHook(id, reason); err != nil {
			return err
		}
	}
	f.buffer(tx, func() { f.hitl[id] = reason })
	return nil
}

func (f *fakeBackend) SetCreatedRuleTx(_ context.Context, tx *sql.Tx, id, ruleID string) error {
	if f.setCreatedRuleTxHook != nil {
		if err := f.setCreatedRuleTxHook(id, ruleID); err != nil {
			return err
		}
	}
	f.buffer(tx, func() {
		if b := f.breaks[id]; b != nil {
			b.createdRuleID = ruleID
		}
	})
	return nil
}

// CountAuditByAction counts committed audit events matching id+action, mirroring
// the store's append-only SELECT COUNT(*). It reads only already-applied events
// (never the per-transaction buffer), matching the real store which sees only
// committed rows. Backs the remediator's SEAM-MIN-1 fail-closed resume guard.
func (f *fakeBackend) CountAuditByAction(_ context.Context, id, action string) (int, error) {
	if f.countAuditErr != nil {
		return 0, f.countAuditErr
	}
	return f.countAction(id, action), nil
}

// --- auditPort ---

func (f *fakeBackend) Record(_ context.Context, ev model.AuditEvent) error {
	if f.recordHook != nil {
		if err := f.recordHook(ev); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.audits = append(f.audits, ev)
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) RecordTx(_ context.Context, tx *sql.Tx, ev model.AuditEvent) error {
	if f.recordTxHook != nil {
		if err := f.recordTxHook(ev); err != nil {
			return err
		}
	}
	f.buffer(tx, func() { f.audits = append(f.audits, ev) })
	return nil
}

// --- inspection helpers ---

func (f *fakeBackend) statusOf(id string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b := f.breaks[id]; b != nil {
		return b.status, true
	}
	return "", false
}

func (f *fakeBackend) createdRuleOf(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b := f.breaks[id]; b != nil {
		return b.createdRuleID
	}
	return ""
}

func (f *fakeBackend) inHITL(id string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.hitl[id]
	return r, ok
}

func (f *fakeBackend) countAction(id, action string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.audits {
		if e.ExternalTxnID == id && e.Action == action {
			n++
		}
	}
	return n
}

func (f *fakeBackend) resolvedReconID(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.audits {
		if e.ExternalTxnID == id && e.Action == audit.ActionResolved {
			return e.Provenance.ReconID
		}
	}
	return ""
}

// auditProvenance returns the provenance of the first committed audit event
// matching id+action, so tests can assert provenance stamping (finding m-2).
func (f *fakeBackend) auditProvenance(id, action string) (model.Provenance, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.audits {
		if e.ExternalTxnID == id && e.Action == action {
			return e.Provenance, true
		}
	}
	return model.Provenance{}, false
}

func (f *fakeBackend) totalAudits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.audits)
}

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

func validRule() *blnk.MatchingRule {
	return &blnk.MatchingRule{
		Name:     "amount-eq",
		Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: "equals", Value: "10.00"}},
	}
}

func txnFor(id string) blnk.ExternalTransaction {
	return blnk.ExternalTransaction{ID: id, Source: "bank-x", Amount: 10.0, Currency: "USD"}
}

// autoClassification is a high-confidence, non-regulated classification with a
// valid proposed rule — the input that SHOULD reach the auto path.
func autoClassification(id string) model.BreakClassification {
	return model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     model.RootCauseAmountDrift,
		Confidence:    0.95,
		Regulated:     false,
		ProposedRule:  validRule(),
		Rationale:     "amount drift within fee tolerance",
	}
}

// harness wires a Remediator to the fakes and returns everything a test needs.
type harness struct {
	rem  *Remediator
	cls  *fakeClassifier
	bnk  *fakeBlnk
	back *fakeBackend
}

func newHarness(t *testing.T) *harness {
	cls := &fakeClassifier{}
	bnk := &fakeBlnk{}
	back := newBackend(t)
	rem := New(cls, bnk, back, back, testThreshold, "kimi-k3")
	return &harness{rem: rem, cls: cls, bnk: bnk, back: back}
}

// autoHarness configures the fakes for a successful end-to-end auto-resolution.
func autoHarness(t *testing.T, id string) *harness {
	h := newHarness(t)
	h.cls.result = autoClassification(id)
	h.bnk.created = blnk.MatchingRule{RuleID: "rule_1", Criteria: validRule().Criteria}
	h.bnk.cleared = true
	h.bnk.reconID = "recon_1"
	return h
}

// -----------------------------------------------------------------------------
// Happy path (Rule 5.3): agent proposes, Blnk decides, resolution is proven.
// -----------------------------------------------------------------------------

func TestHandleAutoResolvesHighConfidenceBreak(t *testing.T) {
	const id = "ext_auto"
	h := autoHarness(t, id)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	status, found := h.back.statusOf(id)
	if !found || status != statusAutoResolved {
		t.Fatalf("break must be auto-resolved, got found=%v status=%q", found, status)
	}
	// Full, granular audit trail (m-3: previously dead-code builders now fire).
	for _, action := range []string{
		audit.ActionClassified, audit.ActionRuleProposed,
		audit.ActionRuleCreated, audit.ActionProbed, audit.ActionResolved,
	} {
		if got := h.back.countAction(id, action); got != 1 {
			t.Fatalf("expected exactly 1 %q audit event, got %d", action, got)
		}
	}
	// Rule 5.3: the resolved event is traceable to the confirming dry-run.
	if rid := h.back.resolvedReconID(id); rid != "recon_1" {
		t.Fatalf("resolved event must carry the confirming recon_id, got %q", rid)
	}
	if h.bnk.createCount() != 1 {
		t.Fatalf("expected exactly 1 Blnk rule creation, got %d", h.bnk.createCount())
	}
	if h.back.createdRuleOf(id) != "rule_1" {
		t.Fatalf("created rule id must be persisted, got %q", h.back.createdRuleOf(id))
	}
	if _, queued := h.back.inHITL(id); queued {
		t.Fatal("an auto-resolved break must NOT be in the HITL queue")
	}
}

// -----------------------------------------------------------------------------
// Confidence / regulation gate (Rule 5.4) and the confidence boundary.
// -----------------------------------------------------------------------------

func TestHandleGateMatrix(t *testing.T) {
	cases := []struct {
		name          string
		mutate        func(c *model.BreakClassification)
		wantStatus    string
		wantResolved  int
		wantEscalated int
		wantHITL      bool
		wantCreate    int
	}{
		{
			name:          "regulated break is escalated, never auto-remediated",
			mutate:        func(c *model.BreakClassification) { c.Regulated = true },
			wantStatus:    statusQueued,
			wantResolved:  0,
			wantEscalated: 1,
			wantHITL:      true,
			wantCreate:    0,
		},
		{
			name:          "confidence just below threshold is escalated",
			mutate:        func(c *model.BreakClassification) { c.Confidence = testThreshold - 0.0001 },
			wantStatus:    statusQueued,
			wantResolved:  0,
			wantEscalated: 1,
			wantHITL:      true,
			wantCreate:    0,
		},
		{
			name:          "confidence exactly at threshold reaches the auto path",
			mutate:        func(c *model.BreakClassification) { c.Confidence = testThreshold },
			wantStatus:    statusAutoResolved,
			wantResolved:  1,
			wantEscalated: 0,
			wantHITL:      false,
			wantCreate:    1,
		},
		{
			name:          "missing proposed rule is escalated",
			mutate:        func(c *model.BreakClassification) { c.ProposedRule = nil },
			wantStatus:    statusQueued,
			wantResolved:  0,
			wantEscalated: 1,
			wantHITL:      true,
			wantCreate:    0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const id = "ext_gate"
			h := autoHarness(t, id)
			c := autoClassification(id)
			tc.mutate(&c)
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			status, _ := h.back.statusOf(id)
			if status != tc.wantStatus {
				t.Fatalf("status: want %q got %q", tc.wantStatus, status)
			}
			if got := h.back.countAction(id, audit.ActionResolved); got != tc.wantResolved {
				t.Fatalf("resolved events: want %d got %d", tc.wantResolved, got)
			}
			if got := h.back.countAction(id, audit.ActionEscalated); got != tc.wantEscalated {
				t.Fatalf("escalated events: want %d got %d", tc.wantEscalated, got)
			}
			if _, queued := h.back.inHITL(id); queued != tc.wantHITL {
				t.Fatalf("in HITL: want %v got %v", tc.wantHITL, queued)
			}
			if got := h.bnk.createCount(); got != tc.wantCreate {
				t.Fatalf("Blnk create calls: want %d got %d", tc.wantCreate, got)
			}
			// A classified event is always emitted before the gate for a
			// well-formed classification.
			if got := h.back.countAction(id, audit.ActionClassified); got != 1 {
				t.Fatalf("classified events: want 1 got %d", got)
			}
		})
	}
}

// TestHandleMalformedConfidenceFailsClosed covers finding m-4: a NaN/Inf or
// out-of-range confidence must fail closed to HITL, never silently flow into
// the auto path, and no classified event is emitted for the malformed input.
func TestHandleMalformedConfidenceFailsClosed(t *testing.T) {
	for _, conf := range []float64{
		math.NaN(), math.Inf(1), math.Inf(-1), 1.5, -0.1,
	} {
		t.Run(fmt.Sprintf("confidence=%v", conf), func(t *testing.T) {
			const id = "ext_badconf"
			h := autoHarness(t, id)
			c := autoClassification(id)
			c.Confidence = conf
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			status, _ := h.back.statusOf(id)
			if status != statusQueued {
				t.Fatalf("malformed-confidence break must be queued, got %q", status)
			}
			if _, queued := h.back.inHITL(id); !queued {
				t.Fatal("malformed-confidence break must be in HITL")
			}
			if got := h.back.countAction(id, audit.ActionClassified); got != 0 {
				t.Fatalf("no classified event should be emitted for a malformed classification, got %d", got)
			}
			if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
				t.Fatalf("expected exactly 1 escalated event, got %d", got)
			}
			if h.bnk.createCount() != 0 {
				t.Fatal("auto path must not run for a malformed confidence")
			}
			if h.back.countAction(id, audit.ActionResolved) != 0 {
				t.Fatal("a malformed-confidence break must never be resolved")
			}
		})
	}
}

// TestHandleClampedCeilingConfidenceDefersToArbiter is the SEAM-INFO-1
// consumer-side companion to TestHandleMalformedConfidenceFailsClosed. Confidence
// 1.0 is exactly what the classifier's producer-side clamp yields for an
// OVER-confident model answer (e.g. a percentage or a >1 float). It must NOT be
// rejected by the remediator's validConfidence backstop; instead it flows into
// the auto path where Blnk's deterministic dry-run — not the confidence — decides
// resolution (Rule 5.3). Together the two tests show the clamp/validate pair is
// one coherent policy: a legitimately clamped value proceeds, only a value that
// bypassed the clamp and is still malformed fails closed.
func TestHandleClampedCeilingConfidenceDefersToArbiter(t *testing.T) {
	const id = "ext_clamp_ceiling"
	h := autoHarness(t, id)
	c := autoClassification(id)
	c.Confidence = 1.0 // the clamp ceiling for any over-confident model answer
	h.cls.result = c

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Not falsely escalated: the clamped ceiling confidence reaches the auto path.
	if h.bnk.createCount() != 1 {
		t.Fatalf("clamped ceiling confidence must reach the auto path (1 rule create), got %d", h.bnk.createCount())
	}
	if h.bnk.probeCount() != 1 {
		t.Fatalf("expected exactly 1 deterministic Blnk probe, got %d", h.bnk.probeCount())
	}
	// Resolution is Blnk's decision (the probe cleared), never the confidence itself.
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("Blnk-confirmed clearance should auto-resolve, got %q", status)
	}
	if got := h.back.resolvedReconID(id); got == "" {
		t.Fatal("resolved event must carry the confirming Blnk recon_id (Rule 5.3), got empty")
	}
}

// -----------------------------------------------------------------------------
// Fail-closed classifier (Rule 5.7).
// -----------------------------------------------------------------------------

func TestHandleClassifierFailureFailsClosed(t *testing.T) {
	const id = "ext_clsfail"
	h := newHarness(t)
	h.cls.err = fmt.Errorf("boom: %w", classifier.ErrClassificationFailed)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	status, found := h.back.statusOf(id)
	if !found || status != statusQueued {
		t.Fatalf("classifier failure must route to HITL (queued), got found=%v status=%q", found, status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("classifier failure must enqueue the break for HITL")
	}
	if got := h.back.countAction(id, audit.ActionClassified); got != 0 {
		t.Fatalf("no classified event when classification failed, got %d", got)
	}
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("expected exactly 1 escalated event, got %d", got)
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("a classifier failure must never resolve a break")
	}
}

func TestHandleClassifierUnexpectedErrorFailsClosed(t *testing.T) {
	const id = "ext_clsunexpected"
	h := newHarness(t)
	// A non-sentinel error still fails closed.
	h.cls.err = errors.New("network reset")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("unexpected classifier error must still escalate, got %q", status)
	}
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("expected 1 escalated event, got %d", got)
	}
}

// -----------------------------------------------------------------------------
// Auto-path failure branches — each escalates, none resolves.
// -----------------------------------------------------------------------------

func TestHandleAutoPathEscalations(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(h *harness)
		wantCreate int
		wantProbe  int
	}{
		{
			name: "grammar-invalid rule never reaches Blnk",
			setup: func(h *harness) {
				c := autoClassification("ext_ap")
				c.ProposedRule = &blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{{Field: "bogus", Operator: "equals"}}}
				h.cls.result = c
			},
			wantCreate: 0,
			wantProbe:  0,
		},
		{
			name: "Blnk rule-create failure escalates",
			setup: func(h *harness) {
				h.bnk.createErr = errors.New("blnk 500")
			},
			wantCreate: 1,
			wantProbe:  0,
		},
		{
			name: "empty created rule id is guarded before probing (m-7)",
			setup: func(h *harness) {
				h.bnk.created = blnk.MatchingRule{RuleID: ""} // empty id
			},
			wantCreate: 1,
			wantProbe:  0,
		},
		{
			name: "Blnk probe failure escalates",
			setup: func(h *harness) {
				h.bnk.probeErr = errors.New("probe timeout")
			},
			wantCreate: 1,
			wantProbe:  1,
		},
		{
			name: "dry-run that does not clear escalates",
			setup: func(h *harness) {
				h.bnk.cleared = false
				h.bnk.reconID = "recon_x"
			},
			wantCreate: 1,
			wantProbe:  1,
		},
		{
			name: "cleared but empty recon id cannot prove resolution (Rule 5.3)",
			setup: func(h *harness) {
				h.bnk.cleared = true
				h.bnk.reconID = "" // no proof
			},
			wantCreate: 1,
			wantProbe:  1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const id = "ext_ap"
			h := autoHarness(t, id)
			tc.setup(h)

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if status, _ := h.back.statusOf(id); status != statusQueued {
				t.Fatalf("auto-path failure must escalate to queued, got %q", status)
			}
			if _, queued := h.back.inHITL(id); !queued {
				t.Fatal("auto-path failure must enqueue for HITL")
			}
			if h.back.countAction(id, audit.ActionResolved) != 0 {
				t.Fatal("auto-path failure must NEVER resolve the break (Rule 5.3)")
			}
			if h.back.countAction(id, audit.ActionEscalated) != 1 {
				t.Fatalf("expected exactly 1 escalated event, got %d", h.back.countAction(id, audit.ActionEscalated))
			}
			if got := h.bnk.createCount(); got != tc.wantCreate {
				t.Fatalf("Blnk create calls: want %d got %d", tc.wantCreate, got)
			}
			if got := h.bnk.probeCount(); got != tc.wantProbe {
				t.Fatalf("Blnk probe calls: want %d got %d", tc.wantProbe, got)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Atomicity fault injection (findings C-1, C-2, M-2, M-4; Rule 5.3).
// Each reproduces a QA fault: an audit/queue write fails mid-transaction, and
// the whole transaction must roll back so NO partial outcome is durable.
// -----------------------------------------------------------------------------

// FI1 / M-2: the classified audit write fails inside the classify transaction.
func TestHandleAtomicity_ClassifyAuditFailure(t *testing.T) {
	const id = "ext_fi1"
	h := autoHarness(t, id)
	h.back.recordTxHook = func(ev model.AuditEvent) error {
		if ev.Action == audit.ActionClassified {
			return errors.New("audit down")
		}
		return nil
	}

	err := h.rem.Handle(context.Background(), txnFor(id), testUploadID)
	if err == nil {
		t.Fatal("expected the classified-audit failure to surface as an error")
	}
	// Nothing durable: no break row, no classified audit (M-2).
	if _, found := h.back.statusOf(id); found {
		t.Fatal("a classified break must NOT be committed when its audit write fails")
	}
	if h.back.countAction(id, audit.ActionClassified) != 0 {
		t.Fatal("no classified audit event may be durable after rollback")
	}
	if h.bnk.createCount() != 0 {
		t.Fatal("the pipeline must not proceed past a failed classify commit")
	}
}

// FI3 / C-1: the resolved audit write fails inside the resolve transaction. The
// break must NOT be left auto-resolved without its resolved event.
func TestHandleAtomicity_ResolveAuditFailure(t *testing.T) {
	const id = "ext_fi3"
	h := autoHarness(t, id)
	h.back.recordTxHook = func(ev model.AuditEvent) error {
		if ev.Action == audit.ActionResolved {
			return errors.New("audit down")
		}
		return nil
	}

	err := h.rem.Handle(context.Background(), txnFor(id), testUploadID)
	if err == nil {
		t.Fatal("expected the resolved-audit failure to surface as an error")
	}
	// The status update rolled back with the failed audit: the break is NOT
	// auto-resolved and NO resolved event exists (C-1).
	status, _ := h.back.statusOf(id)
	if status == statusAutoResolved {
		t.Fatal("break must NOT be auto-resolved when the resolved audit write fails (C-1)")
	}
	if status != statusClassified {
		t.Fatalf("break should remain in its last committed state (classified), got %q", status)
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("no resolved audit event may be durable after rollback")
	}
	// The earlier, independently-committed steps remain: the rule was created
	// and probed before the resolve transaction.
	if h.back.countAction(id, audit.ActionRuleCreated) != 1 || h.back.countAction(id, audit.ActionProbed) != 1 {
		t.Fatal("rule_created and probed events (committed earlier) must persist")
	}
	if h.back.createdRuleOf(id) != "rule_1" {
		t.Fatal("the created rule id (committed earlier) must persist for reuse on retry")
	}
}

// FI5 / C-2: the escalated audit write fails inside the escalate transaction.
func TestHandleAtomicity_EscalateAuditFailure(t *testing.T) {
	const id = "ext_fi5"
	h := autoHarness(t, id)
	c := autoClassification(id)
	c.Regulated = true // force the escalate path
	h.cls.result = c
	h.back.recordTxHook = func(ev model.AuditEvent) error {
		if ev.Action == audit.ActionEscalated {
			return errors.New("audit down")
		}
		return nil
	}

	err := h.rem.Handle(context.Background(), txnFor(id), testUploadID)
	if err == nil {
		t.Fatal("expected the escalated-audit failure to surface as an error")
	}
	// The escalate transaction rolled back: the break is NOT queued, is NOT in
	// the HITL queue, and has NO escalated event (C-2). It remains classified
	// (its last good commit).
	status, _ := h.back.statusOf(id)
	if status == statusQueued {
		t.Fatal("break must NOT be queued when the escalated audit write fails (C-2)")
	}
	if status != statusClassified {
		t.Fatalf("break should remain classified after escalate rollback, got %q", status)
	}
	if _, queued := h.back.inHITL(id); queued {
		t.Fatal("break must NOT be in the HITL queue after escalate rollback (C-2)")
	}
	if h.back.countAction(id, audit.ActionEscalated) != 0 {
		t.Fatal("no escalated audit event may be durable after rollback")
	}
}

// FI6 / M-4: the HITL-queue insert fails inside the escalate transaction. The
// break must not end up queued-without-a-queue-entry, nor with an escalated
// event.
func TestHandleAtomicity_EscalateEnqueueFailure(t *testing.T) {
	const id = "ext_fi6"
	h := autoHarness(t, id)
	c := autoClassification(id)
	c.Regulated = true
	h.cls.result = c
	h.back.enqueueHITLTxHook = func(_, _ string) error { return errors.New("queue down") }

	err := h.rem.Handle(context.Background(), txnFor(id), testUploadID)
	if err == nil {
		t.Fatal("expected the HITL-enqueue failure to surface as an error")
	}
	status, _ := h.back.statusOf(id)
	if status == statusQueued {
		t.Fatal("break must NOT be marked queued when the HITL enqueue fails (M-4)")
	}
	if _, queued := h.back.inHITL(id); queued {
		t.Fatal("no HITL-queue entry may be durable after rollback")
	}
	if h.back.countAction(id, audit.ActionEscalated) != 0 {
		t.Fatal("no escalated audit event may be durable after rollback")
	}
}

// The same atomicity guarantee viewed from the STORE side: when the persistence
// write (rather than the audit write) fails inside a transaction, the whole
// transaction still rolls back and no partial outcome is durable.

func TestHandleAtomicity_ClassifyStoreFailure(t *testing.T) {
	const id = "ext_clsstore"
	h := autoHarness(t, id)
	h.back.upsertBreakTxHook = func(_ model.BreakClassification, status string) error {
		if status == statusClassified {
			return errors.New("store down")
		}
		return nil
	}
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected the classify store-write failure to surface")
	}
	if _, found := h.back.statusOf(id); found {
		t.Fatal("no classified break may be committed when its store write fails (M-2)")
	}
	if h.back.countAction(id, audit.ActionClassified) != 0 {
		t.Fatal("no classified audit may be durable after rollback")
	}
	if h.bnk.createCount() != 0 {
		t.Fatal("pipeline must not proceed past a failed classify commit")
	}
}

func TestHandleAtomicity_RuleStoreFailure(t *testing.T) {
	const id = "ext_rulestore"
	h := autoHarness(t, id)
	h.back.setCreatedRuleTxHook = func(_, _ string) error { return errors.New("store down") }
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected the created-rule store-write failure to surface")
	}
	// The Blnk rule was created (external side effect) but the local created
	// rule id was rolled back with its rule_created event, and the break was
	// NOT resolved.
	if h.bnk.createCount() != 1 {
		t.Fatalf("expected exactly 1 Blnk create attempt, got %d", h.bnk.createCount())
	}
	if h.back.createdRuleOf(id) != "" {
		t.Fatal("created rule id must NOT be durable after the rule transaction rolled back")
	}
	if h.back.countAction(id, audit.ActionRuleCreated) != 0 {
		t.Fatal("no rule_created audit may be durable after rollback")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("break must not be resolved when the rule commit failed")
	}
}

func TestHandleAtomicity_ResolveStoreFailure(t *testing.T) {
	const id = "ext_resstore"
	h := autoHarness(t, id)
	h.back.setBreakStatusTxHook = func(_, status string) error {
		if status == statusAutoResolved {
			return errors.New("store down")
		}
		return nil
	}
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected the resolve store-write failure to surface")
	}
	if status, _ := h.back.statusOf(id); status == statusAutoResolved {
		t.Fatal("break must NOT be auto-resolved when the status write fails (C-1)")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("no resolved audit may be durable after rollback")
	}
}

func TestHandleAtomicity_EscalateStoreFailure(t *testing.T) {
	const id = "ext_escstore"
	h := autoHarness(t, id)
	c := autoClassification(id)
	c.Regulated = true
	h.cls.result = c
	h.back.upsertBreakTxHook = func(_ model.BreakClassification, status string) error {
		if status == statusQueued {
			return errors.New("store down")
		}
		return nil
	}
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected the escalate store-write failure to surface")
	}
	if status, _ := h.back.statusOf(id); status == statusQueued {
		t.Fatal("break must NOT be queued when the queued-status write fails (C-2)")
	}
	if _, queued := h.back.inHITL(id); queued {
		t.Fatal("no HITL entry may be durable after rollback")
	}
	if h.back.countAction(id, audit.ActionEscalated) != 0 {
		t.Fatal("no escalated audit may be durable after rollback")
	}
}

// The infrastructure error paths that are NOT escalations but hard errors: a
// LoadBreak failure and a WithTx begin failure must surface as errors and leave
// nothing resolved.
func TestHandleLoadBreakError(t *testing.T) {
	const id = "ext_loaderr"
	h := autoHarness(t, id)
	h.back.loadErr = errors.New("db unreachable")
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected LoadBreak error to surface")
	}
	if h.cls.callCount() != 0 {
		t.Fatal("classification must not run when the initial load fails")
	}
}

func TestHandleClassifyBeginError(t *testing.T) {
	const id = "ext_beginerr"
	h := autoHarness(t, id)
	h.back.beginErr = errors.New("cannot begin")
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected the transaction-begin error to surface")
	}
	if _, found := h.back.statusOf(id); found {
		t.Fatal("no break may be committed when the transaction cannot begin")
	}
}

// A failure to record the advisory rule_proposed / probed events surfaces as an
// error (every action must be auditable, Rule 5.5) and does not resolve.
func TestHandleProposedAuditFailure(t *testing.T) {
	const id = "ext_propaudit"
	h := autoHarness(t, id)
	h.back.recordHook = func(ev model.AuditEvent) error {
		if ev.Action == audit.ActionRuleProposed {
			return errors.New("audit down")
		}
		return nil
	}
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected rule_proposed audit failure to surface")
	}
	if h.bnk.createCount() != 0 {
		t.Fatal("must not create a Blnk rule after failing to audit the proposal")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("must not resolve")
	}
}

func TestHandleProbeAuditFailure(t *testing.T) {
	const id = "ext_probeaudit"
	h := autoHarness(t, id)
	h.back.recordHook = func(ev model.AuditEvent) error {
		if ev.Action == audit.ActionProbed {
			return errors.New("audit down")
		}
		return nil
	}
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("expected probed audit failure to surface")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("must not resolve when the probe audit write fails")
	}
}

// -----------------------------------------------------------------------------
// Idempotency and crash-resume (findings M-6, M-3).
// -----------------------------------------------------------------------------

// A terminal break is a no-op on re-run: no double classification, no duplicate
// Blnk rule, no second resolved/escalated event.
func TestHandleIdempotentRerunOnResolvedBreak(t *testing.T) {
	const id = "ext_rerun"
	h := autoHarness(t, id)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	// Re-run the exact same break.
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("second Handle: %v", err)
	}

	if h.cls.callCount() != 1 {
		t.Fatalf("classifier must run once, not on the idempotent re-run: got %d", h.cls.callCount())
	}
	if h.bnk.createCount() != 1 {
		t.Fatalf("Blnk rule must be created once, not duplicated on re-run: got %d", h.bnk.createCount())
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
		t.Fatalf("resolved event must NOT be double-counted on re-run: got %d (M-6)", got)
	}
	if got := h.back.countAction(id, audit.ActionClassified); got != 1 {
		t.Fatalf("classified event must NOT be double-counted on re-run: got %d", got)
	}
}

func TestHandleIdempotentRerunOnQueuedBreak(t *testing.T) {
	const id = "ext_rerun_q"
	h := autoHarness(t, id)
	// Seed an already-queued (escalated) break.
	h.back.seedBreak(id, autoClassification(id), statusQueued, "")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if h.cls.callCount() != 0 {
		t.Fatal("a queued break must not be re-classified")
	}
	if h.bnk.createCount() != 0 {
		t.Fatal("a queued break must not trigger Blnk work")
	}
	if h.back.totalAudits() != 0 {
		t.Fatal("a terminal queued break produces no new audit events on re-run")
	}
}

// Resume a break a crashed attempt left "classified" WITH a created rule id:
// the rule must be reused (no duplicate Blnk creation), the classifier must not
// re-run, and the probe must use the persisted rule id (M-3).
func TestHandleResumeClassifiedWithCreatedRule(t *testing.T) {
	const id = "ext_resume_withrule"
	h := autoHarness(t, id)
	h.back.seedBreak(id, autoClassification(id), statusClassified, "rule_existing")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if h.cls.callCount() != 0 {
		t.Fatal("resume must NOT re-classify a break already persisted as classified")
	}
	if h.bnk.createCount() != 0 {
		t.Fatalf("resume must reuse the persisted rule, not create a duplicate (M-3): got %d creates", h.bnk.createCount())
	}
	if got := h.bnk.lastProbeRuleIDs(); len(got) != 1 || got[0] != "rule_existing" {
		t.Fatalf("probe must use the persisted rule id, got %v", got)
	}
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("resume should carry the break to auto-resolved, got %q", status)
	}
	// No duplicate classified event on resume.
	if got := h.back.countAction(id, audit.ActionClassified); got != 0 {
		t.Fatalf("resume must not emit a classified event, got %d", got)
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
		t.Fatalf("resume should resolve exactly once, got %d", got)
	}
}

// Resume a break left "classified" WITHOUT a created rule id: the classifier is
// still skipped (classification reused) but the rule is created now.
func TestHandleResumeClassifiedWithoutCreatedRule(t *testing.T) {
	const id = "ext_resume_norule"
	h := autoHarness(t, id)
	h.back.seedBreak(id, autoClassification(id), statusClassified, "")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if h.cls.callCount() != 0 {
		t.Fatal("resume reuses the persisted classification; classifier must not run")
	}
	if h.bnk.createCount() != 1 {
		t.Fatalf("no rule was persisted, so exactly one must be created on resume, got %d", h.bnk.createCount())
	}
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("resume should reach auto-resolved, got %q", status)
	}
}

// SEAM-MIN-1 (fail-closed resume guard against a DUPLICATE Blnk rule). A break a
// crashed attempt left "classified" WITHOUT a persisted created rule id but WITH
// a durable rule_proposed audit event indicates the prior attempt crashed in the
// window between the CreateMatchingRule POST and the atomic created-rule commit,
// so Blnk may hold an orphaned rule. Because Blnk exposes no list/get
// matching-rule route to reconcile it (Rule 5.1), re-POSTing would create a
// duplicate. The break MUST escalate to HITL with NO new Blnk rule creation, NO
// probe, and NO resolution. Contrast TestHandleResumeClassifiedWithoutCreatedRule
// above, where no rule_proposed event exists and the rule is (correctly) created.
func TestHandleResumeRuleProposedWithoutCreatedRuleEscalates(t *testing.T) {
	const id = "ext_resume_orphan"
	h := autoHarness(t, id)
	// The exact state a crash in that window leaves behind: classified, no
	// persisted created rule id, but a rule_proposed event already recorded.
	h.back.seedBreak(id, autoClassification(id), statusClassified, "")
	h.back.seedAudit(id, audit.ActionRuleProposed)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if h.cls.callCount() != 0 {
		t.Fatal("resume reuses the persisted classification; classifier must not run")
	}
	// Core SEAM-MIN-1 assertion: no second Blnk rule is created for the orphan.
	if h.bnk.createCount() != 0 {
		t.Fatalf("must NOT re-create a rule when a prior rule_proposed exists (would duplicate an orphan): got %d creates (SEAM-MIN-1)", h.bnk.createCount())
	}
	if h.bnk.probeCount() != 0 {
		t.Fatalf("must NOT probe (no rule id to probe with): got %d probes", h.bnk.probeCount())
	}
	// Fail-closed to HITL.
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("break must be escalated to HITL (queued), got %q", status)
	}
	if _, ok := h.back.inHITL(id); !ok {
		t.Fatal("break must be enqueued to the HITL queue")
	}
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("exactly one escalated event expected, got %d", got)
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
		t.Fatalf("an orphan-resume break must NEVER be resolved, got %d resolved", got)
	}
	// m-2: the escalation audit event also carries the upload id in provenance.
	if prov, ok := h.back.auditProvenance(id, audit.ActionEscalated); !ok || prov.UploadID != testUploadID {
		t.Fatalf("escalated event must carry upload_id=%q in provenance, got %+v (ok=%v)", testUploadID, prov, ok)
	}
}

// The SEAM-MIN-1 resume guard reads the audit trail (CountAuditByAction). If
// that read fails, Handle must fail closed: return an infrastructure error and
// NEVER create a Blnk rule or resolve the break off an unverified assumption.
func TestHandleResumeCountAuditErrorFailsClosed(t *testing.T) {
	const id = "ext_resume_counterr"
	h := autoHarness(t, id)
	h.back.seedBreak(id, autoClassification(id), statusClassified, "")
	h.back.countAuditErr = errors.New("audit count query failed")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("Handle must return an error when the resume audit-count read fails")
	}
	if h.bnk.createCount() != 0 {
		t.Fatalf("must NOT create a Blnk rule when the resume guard read fails: got %d", h.bnk.createCount())
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
		t.Fatalf("must NOT resolve when the resume guard read fails: got %d", got)
	}
}

// m-2: the reconciliation upload batch id threaded through Handle is stamped into
// the provenance of the emitted audit events (previously provenance.upload_id was
// always empty). Asserted across both an early event (classified) and a terminal
// event (resolved) so the stamp is proven to propagate through the whole auto
// path, not just at construction.
func TestHandleStampsUploadIDIntoProvenance(t *testing.T) {
	const id = "ext_upload_prov"
	h := autoHarness(t, id)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	for _, action := range []string{audit.ActionClassified, audit.ActionRuleProposed, audit.ActionRuleCreated, audit.ActionProbed, audit.ActionResolved} {
		prov, ok := h.back.auditProvenance(id, action)
		if !ok {
			t.Fatalf("expected a %q audit event on the auto path", action)
		}
		if prov.UploadID != testUploadID {
			t.Fatalf("%q event provenance upload_id = %q, want %q (m-2)", action, prov.UploadID, testUploadID)
		}
	}
}

// A break a crashed attempt left classified that is actually regulated must be
// escalated on resume (the gate is re-applied), not auto-resolved.
func TestHandleResumeClassifiedRegulatedEscalates(t *testing.T) {
	const id = "ext_resume_reg"
	h := autoHarness(t, id)
	c := autoClassification(id)
	c.Regulated = true
	h.back.seedBreak(id, c, statusClassified, "")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("resumed regulated break must be escalated, got %q", status)
	}
	if h.bnk.createCount() != 0 {
		t.Fatal("a regulated break must never create a Blnk rule")
	}
	if h.back.countAction(id, audit.ActionEscalated) != 1 {
		t.Fatal("resumed regulated break must emit exactly one escalated event")
	}
}

// -----------------------------------------------------------------------------
// Concurrency (finding M-5). Uses -race to catch data races.
// -----------------------------------------------------------------------------

// Concurrent Handle calls for the SAME break must be serialized so the break is
// classified once, its rule created once, and it is resolved exactly once.
func TestHandleConcurrentSameBreakResolvesOnce(t *testing.T) {
	const id = "ext_conc_same"
	h := autoHarness(t, id)

	const n = 12
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Errorf("concurrent Handle: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := h.cls.callCount(); got != 1 {
		t.Fatalf("same break must be classified exactly once under concurrency, got %d (M-5)", got)
	}
	if got := h.bnk.createCount(); got != 1 {
		t.Fatalf("same break must create exactly one Blnk rule under concurrency, got %d (M-5)", got)
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
		t.Fatalf("same break must be resolved exactly once under concurrency, got %d (M-5)", got)
	}
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("break must end auto-resolved, got %q", status)
	}
}

// Concurrent Handle calls for DISTINCT breaks all proceed and each resolves.
func TestHandleConcurrentDistinctBreaks(t *testing.T) {
	h := autoHarness(t, "unused")
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("ext_conc_%d", i)
		go func(id string) {
			defer wg.Done()
			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Errorf("Handle(%s): %v", id, err)
			}
		}(id)
	}
	wg.Wait()

	if got := h.bnk.createCount(); got != n {
		t.Fatalf("each distinct break should create its own rule: want %d got %d", n, got)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("ext_conc_%d", i)
		if status, _ := h.back.statusOf(id); status != statusAutoResolved {
			t.Fatalf("distinct break %s must be auto-resolved, got %q", id, status)
		}
		if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
			t.Fatalf("distinct break %s must resolve exactly once, got %d", id, got)
		}
	}
}

// -----------------------------------------------------------------------------
// New / wiring.
// -----------------------------------------------------------------------------

func TestNewReturnsWiredRemediator(t *testing.T) {
	h := newHarness(t)
	if h.rem == nil {
		t.Fatal("New returned nil")
	}
	if h.rem.threshold != testThreshold {
		t.Fatalf("threshold not wired: got %v", h.rem.threshold)
	}
	if h.rem.llmModel != "kimi-k3" {
		t.Fatalf("llmModel not wired: got %q", h.rem.llmModel)
	}
}

// -----------------------------------------------------------------------------
// Classification-safety gate (finding C3): confidence + the regulated flag are
// NOT sufficient to auto-remediate. The root cause must be auto-eligible AND the
// proposed rule must be tight enough (equality-only) to auto-apply to a
// single-transaction dry-run. Everything else escalates to HITL with zero Blnk
// side effects, regardless of how high the confidence is.
// -----------------------------------------------------------------------------

// TestHandleRootCauseNotAutoEligibleEscalates covers finding C3: a high-confidence,
// non-regulated break whose root cause is NOT on the auto-eligible allow-list
// (duplicate, missing_internal, currency_mismatch, unknown) must escalate to HITL
// even when it carries a grammar-valid proposed rule — it is never auto-applied,
// and no rule is created or probed in Blnk.
func TestHandleRootCauseNotAutoEligibleEscalates(t *testing.T) {
	ineligible := []model.RootCause{
		model.RootCauseDuplicate,
		model.RootCauseMissingInternal,
		model.RootCauseCurrencyMismatch,
		model.RootCauseUnknown,
	}
	for _, rc := range ineligible {
		t.Run(string(rc), func(t *testing.T) {
			id := "ext_ineligible_" + string(rc)
			h := autoHarness(t, id)
			c := autoClassification(id) // high confidence + valid equals rule
			c.RootCause = rc
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if status, _ := h.back.statusOf(id); status != statusQueued {
				t.Fatalf("status: want %q got %q", statusQueued, status)
			}
			if _, queued := h.back.inHITL(id); !queued {
				t.Fatal("break should be queued to HITL")
			}
			if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
				t.Fatalf("resolved events: want 0 got %d", got)
			}
			if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
				t.Fatalf("escalated events: want 1 got %d", got)
			}
			if got := h.back.countAction(id, audit.ActionClassified); got != 1 {
				t.Fatalf("classified events: want 1 got %d", got)
			}
			if got := h.bnk.createCount(); got != 0 {
				t.Fatalf("Blnk create calls: want 0 got %d", got)
			}
			if got := h.bnk.probeCount(); got != 0 {
				t.Fatalf("Blnk probe calls: want 0 got %d", got)
			}
		})
	}
}

// TestHandleAutoEligibleRootCausesReachAutoPath is the positive control for C3:
// each root cause ON the allow-list (timing, amount_drift, reference_mismatch),
// at high confidence with a valid equals rule, still auto-resolves.
func TestHandleAutoEligibleRootCausesReachAutoPath(t *testing.T) {
	eligible := []model.RootCause{
		model.RootCauseTiming,
		model.RootCauseAmountDrift,
		model.RootCauseReferenceMismatch,
	}
	for _, rc := range eligible {
		t.Run(string(rc), func(t *testing.T) {
			id := "ext_eligible_" + string(rc)
			h := autoHarness(t, id)
			c := autoClassification(id)
			c.RootCause = rc
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if status, _ := h.back.statusOf(id); status != statusAutoResolved {
				t.Fatalf("status: want %q got %q", statusAutoResolved, status)
			}
			if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
				t.Fatalf("resolved events: want 1 got %d", got)
			}
		})
	}
}

// TestHandleOverbroadOperatorEscalates covers finding C3: even for an auto-eligible
// root cause at high confidence, a grammar-valid proposed rule that uses a
// non-equality operator (greater_than / less_than / contains) is too broad to
// auto-apply to a single-transaction dry-run and must escalate to HITL BEFORE any
// rule is created or probed in Blnk.
func TestHandleOverbroadOperatorEscalates(t *testing.T) {
	for _, op := range []string{"greater_than", "less_than", "contains"} {
		t.Run(op, func(t *testing.T) {
			id := "ext_overbroad_" + op
			h := autoHarness(t, id)
			c := autoClassification(id) // auto-eligible root cause, high confidence
			c.ProposedRule = &blnk.MatchingRule{
				Name:     "overbroad",
				Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: op, Value: "10.00"}},
			}
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if status, _ := h.back.statusOf(id); status != statusQueued {
				t.Fatalf("status: want %q got %q", statusQueued, status)
			}
			if _, queued := h.back.inHITL(id); !queued {
				t.Fatal("break should be queued to HITL")
			}
			if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
				t.Fatalf("resolved events: want 0 got %d", got)
			}
			if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
				t.Fatalf("escalated events: want 1 got %d", got)
			}
			if got := h.bnk.createCount(); got != 0 {
				t.Fatalf("Blnk create calls: want 0 got %d", got)
			}
			if got := h.bnk.probeCount(); got != 0 {
				t.Fatalf("Blnk probe calls: want 0 got %d", got)
			}
		})
	}
}

// TestAutoApplySafe unit-tests the semantic-safety predicate that backs finding
// C3: an equality-only rule is safe to auto-apply, while any criterion using a
// non-equality operator is rejected.
func TestAutoApplySafe(t *testing.T) {
	safe := blnk.MatchingRule{
		Criteria: []blnk.MatchingCriteria{
			{Field: "amount", Operator: "equals", Value: "10.00"},
			{Field: "currency", Operator: "equals", Value: "USD"},
		},
	}
	if err := autoApplySafe(safe); err != nil {
		t.Fatalf("equals-only rule should be safe, got error: %v", err)
	}
	for _, op := range []string{"greater_than", "less_than", "contains"} {
		unsafe := blnk.MatchingRule{
			Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: op, Value: "10.00"}},
		}
		if err := autoApplySafe(unsafe); err == nil {
			t.Fatalf("operator %q should be rejected as overbroad", op)
		}
	}
}
