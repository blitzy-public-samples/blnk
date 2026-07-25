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
	"strings"
	"sync"
	"testing"
	"time"

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
	// delay, when > 0, makes Classify block for that long OR until the caller's
	// context is cancelled — whichever comes first — returning the context error
	// on cancellation. It models a slow/hung LLM endpoint so the remediator's
	// per-break classify budget (INFO#3) can be exercised: a budget shorter than
	// delay cancels the call and the break fails closed to HITL (Rule 5.7).
	delay time.Duration
}

func (f *fakeClassifier) Classify(ctx context.Context, _ blnk.ExternalTransaction) (model.BreakClassification, error) {
	f.mu.Lock()
	f.calls++
	delay := f.delay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return model.BreakClassification{}, ctx.Err()
		}
	}
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
	mu          sync.Mutex
	created     blnk.MatchingRule
	createErr   error
	cleared     bool
	reconID     string
	probeErr    error
	createCalls int
	probeCalls  int
	// probeRuleIDs captures the matching-rule id set threaded into each
	// ConfirmCohortCleared call (one call per cohort; for the cohort-of-one
	// Handle wrapper that is a single rule id), so a test can assert the
	// confirmation targeted the created/persisted rule(s).
	probeRuleIDs [][]string
	// createdRules records every rule passed to CreateMatchingRule (finding F7),
	// so tests can assert the ACTUAL narrowed rule that was POSTed (Value
	// populated, {currency,equals} appended) rather than the classifier's raw
	// proposal.
	createdRules []blnk.MatchingRule
	// deleteCalls / deletedRuleIDs record DeleteMatchingRule invocations (finding
	// F5), so tests can assert a rule created for a non-clearing attempt is
	// rolled back — and, crucially, that a rule for a CLEARED attempt is NOT.
	deleteCalls    int
	deletedRuleIDs []string
	deleteErr      error
}

func (f *fakeBlnk) CreateMatchingRule(_ context.Context, rule blnk.MatchingRule) (blnk.MatchingRule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.createdRules = append(f.createdRules, rule)
	if f.createErr != nil {
		return blnk.MatchingRule{}, f.createErr
	}
	return f.created, nil
}

// DeleteMatchingRule records the rollback of a created rule (finding F5). It
// returns f.deleteErr so tests can assert the remediator still escalates
// fail-closed (Rule 5.7) even when catalog cleanup fails.
func (f *fakeBlnk) DeleteMatchingRule(_ context.Context, ruleID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.deletedRuleIDs = append(f.deletedRuleIDs, ruleID)
	return f.deleteErr
}

// ConfirmCohortCleared is the cache-safe, per-break-attributable
// deterministic-arbiter seam the remediator uses after creating the eligible
// cohort's rules (finding F-1, Rule 5.3): ONE dry-run reconciliation over a
// dedicated upload containing ONLY the cohort, reporting cleared when Blnk's
// authoritative unmatched count is 0 (every member left the unmatched set). The
// fake records the cohort's rule id set (so a test can assert the confirmation
// targeted the created/persisted rules) and returns the test-configurable
// cleared / reconID / error verdict. It is invoked exactly once per cohort; the
// cohort-of-one Handle wrapper therefore records a single rule id, matching the
// per-attempt semantics the earlier ConfirmClearedOverUpload modelled.
func (f *fakeBlnk) ConfirmCohortCleared(_ context.Context, cohort []blnk.ExternalTransaction, matchingRuleIDs []string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probeCalls++
	f.probeRuleIDs = append(f.probeRuleIDs, append([]string(nil), matchingRuleIDs...))
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

func (f *fakeBlnk) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteCalls
}

func (f *fakeBlnk) deletedRules() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletedRuleIDs...)
}

// lastCreatedRule returns the most recent rule passed to CreateMatchingRule
// (finding F7), so tests can assert the narrowed rule that was actually POSTed.
func (f *fakeBlnk) lastCreatedRule() (blnk.MatchingRule, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.createdRules) == 0 {
		return blnk.MatchingRule{}, false
	}
	return f.createdRules[len(f.createdRules)-1], true
}

// -----------------------------------------------------------------------------
// fakeBackend: implements BOTH storePort and auditPort, modelling the shared
// transaction so atomicity can be asserted.
// -----------------------------------------------------------------------------

type fakeBreak struct {
	classification model.BreakClassification
	status         string
	createdRuleID  string
	// resolvedReconID captures the confirming Blnk dry-run reconciliation id
	// stamped onto the row by MarkResolvedTx, mirroring the store's
	// resolved_recon_id column so tests can assert the resolve proof is durable.
	resolvedReconID string
	// txn captures the matchable transaction fields persisted alongside the
	// verdict (finding L2), so tests can assert the break remembers the
	// transaction it was managing for a later re_drive.
	txn blnk.ExternalTransaction
	// prov captures the provenance the upsert persisted (findings M-13/M-12), so
	// tests can assert the break row remembers its upload/source correlation.
	prov model.Provenance
}

// fakeOutboxRow models one durable rule-compensation obligation
// (agent.agent_rule_outbox) that finding M-11 introduced: a Blnk rule the agent
// created that now owes either CONFIRMation (its break resolved) or
// COMPENSATION (the rule was deleted as an orphan). Tests inspect and
// fault-inject against these rows to prove that no created rule can survive a
// failed local commit without a durable cleanup obligation.
type fakeOutboxRow struct {
	id            int64
	externalTxnID string
	ruleID        string
	status        string // store.Outbox{Pending,Confirmed,Compensated,CompensationFailed}
	attempts      int
	lastError     string
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

	// outbox is the committed set of durable rule-compensation obligations
	// (finding M-11). outboxSeq mints their monotonic row ids. Rows are written
	// through the transaction buffer (RecordRuleOutboxTx / ConfirmRuleOutboxTx)
	// or in autocommit mode (CompensateRuleOutbox / MarkRuleOutboxCompensated),
	// exactly mirroring the store.
	outbox    []*fakeOutboxRow
	outboxSeq int64

	// claims is the committed durable per-break processing lease (finding M-15):
	// externalTxnID -> current owner. An empty/absent entry means unclaimed.
	claims map[string]string

	// Fault-injection hooks (nil => success). Returning a non-nil error from a
	// transaction-bound write aborts fn, forcing WithTx to roll back and discard
	// every buffered mutation of that transaction.
	loadErr              error
	countAuditErr        error
	beginErr             error
	recordHook           func(model.AuditEvent) error
	recordTxHook         func(model.AuditEvent) error
	upsertBreakTxHook    func(model.BreakClassification, string) error
	markResolvedTxHook   func(id, reconID string) error
	enqueueHITLTxHook    func(string, string) error
	setCreatedRuleTxHook func(string, string) error
	setCreatedRuleHook   func(string, string) error

	// M-11/M-15 fault-injection hooks for the durable outbox + lease surface.
	claimBreakHook          func(id, owner string) (bool, error)
	releaseBreakHook        func(id, owner string) error
	recordRuleOutboxTxHook  func(id, ruleID string) error
	confirmRuleOutboxTxHook func(id, ruleID string) error
	compensateOutboxHook    func(id, ruleID string, ok bool) error
	listPendingOutboxErr    error
	markOutboxCompHook      func(rowID int64, ok bool) error
}

func newBackend(t *testing.T) *fakeBackend {
	return &fakeBackend{
		db:      newFakeDB(t),
		breaks:  map[string]*fakeBreak{},
		hitl:    map[string]string{},
		pending: map[*sql.Tx][]func(){},
		claims:  map[string]string{},
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

func (f *fakeBackend) UpsertBreakTx(_ context.Context, tx *sql.Tx, c model.BreakClassification, txn blnk.ExternalTransaction, prov model.Provenance, status string) error {
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
		// Persist the matchable transaction fields (finding L2) so a queued
		// break can be re-driven without reading any blnk.* table.
		b.txn = txn
		// Persist the correlation provenance (findings M-13/M-12).
		b.prov = prov
	})
	return nil
}

// MarkResolvedTx models the store's resolve write: it transitions the break to
// auto-resolved AND records the confirming reconciliation id, mirroring the
// agent_break_resolved_proof CHECK. A markResolvedTxHook lets a test fault-inject
// a resolve-path store failure (C-1 atomicity).
func (f *fakeBackend) MarkResolvedTx(_ context.Context, tx *sql.Tx, id, reconID string) error {
	if f.markResolvedTxHook != nil {
		if err := f.markResolvedTxHook(id, reconID); err != nil {
			return err
		}
	}
	f.buffer(tx, func() {
		if b := f.breaks[id]; b != nil {
			b.status = statusAutoResolved
			b.resolvedReconID = reconID
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

// SetCreatedRule mirrors the store's AUTOCOMMIT setter: it applies immediately
// (no transaction buffer). The remediator uses it to CLEAR the persisted
// created-rule id while rolling back a non-clearing rule (finding F5).
func (f *fakeBackend) SetCreatedRule(_ context.Context, id, ruleID string) error {
	if f.setCreatedRuleHook != nil {
		if err := f.setCreatedRuleHook(id, ruleID); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if b := f.breaks[id]; b != nil {
		b.createdRuleID = ruleID
	}
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

// --- storePort: durable processing lease (finding M-15) ---

// ClaimBreak models the store's DB-level lease acquisition: it grants the lease
// unless another live owner currently holds it, in which case it returns
// granted=false with a nil error so the caller yields rather than racing. A
// claimBreakHook lets a test force a busy (not-granted) or error outcome.
func (f *fakeBackend) ClaimBreak(_ context.Context, id, owner string, _ time.Duration) (bool, error) {
	if f.claimBreakHook != nil {
		return f.claimBreakHook(id, owner)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, held := f.claims[id]; held && cur != "" && cur != owner {
		return false, nil
	}
	f.claims[id] = owner
	return true, nil
}

// ReleaseBreak models the store's lease release: it clears the lease only when
// this owner holds it, matching the store's owner-scoped release. The remediator
// invokes it in a deferred, best-effort fashion and ignores its error, so the
// fake stays lenient unless a releaseBreakHook forces a fault.
func (f *fakeBackend) ReleaseBreak(_ context.Context, id, owner string) error {
	if f.releaseBreakHook != nil {
		if err := f.releaseBreakHook(id, owner); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claims[id] == owner {
		delete(f.claims, id)
	}
	return nil
}

// --- storePort: durable rule-compensation outbox (finding M-11) ---

// RecordRuleOutboxTx models the store's transaction-bound INSERT of a 'pending'
// compensation obligation. It is buffered on the shared *sql.Tx so it commits
// ATOMICALLY with SetCreatedRuleTx + the rule_created audit event; a rollback
// discards it, exactly like the store. It rejects an empty rule id (mirroring
// the store's NOT NULL/CHECK) so a created rule can never be recorded without an
// id to compensate.
func (f *fakeBackend) RecordRuleOutboxTx(_ context.Context, tx *sql.Tx, id, ruleID string) error {
	if f.recordRuleOutboxTxHook != nil {
		if err := f.recordRuleOutboxTxHook(id, ruleID); err != nil {
			return err
		}
	}
	if strings.TrimSpace(ruleID) == "" {
		return fmt.Errorf("fake: RecordRuleOutboxTx requires a non-empty rule id")
	}
	f.buffer(tx, func() {
		f.outboxSeq++
		f.outbox = append(f.outbox, &fakeOutboxRow{
			id:            f.outboxSeq,
			externalTxnID: id,
			ruleID:        ruleID,
			status:        store.OutboxPending,
		})
	})
	return nil
}

// ConfirmRuleOutboxTx models the store's transaction-bound flip of a still
// 'pending' obligation to 'confirmed' because the rule legitimately cleared its
// break. Buffered on the shared *sql.Tx so it commits atomically with
// MarkResolvedTx. It targets ONLY a pending row (idempotent no-op otherwise),
// matching the store's WHERE status = 'pending' guard.
func (f *fakeBackend) ConfirmRuleOutboxTx(_ context.Context, tx *sql.Tx, id, ruleID string) error {
	if f.confirmRuleOutboxTxHook != nil {
		if err := f.confirmRuleOutboxTxHook(id, ruleID); err != nil {
			return err
		}
	}
	f.buffer(tx, func() {
		if row := f.pendingOutboxRow(id, ruleID); row != nil {
			row.status = store.OutboxConfirmed
		}
	})
	return nil
}

// CompensateRuleOutbox models the store's autocommit mark of a pending
// obligation as 'compensated' (ok) or 'compensation_failed' (!ok), keyed by
// (externalTxnID, ruleID) after an in-call delete attempt. Idempotent and
// tolerant of a missing row (returns nil, no error) — the create-commit-failed
// path compensates a rule whose pending row was never committed.
func (f *fakeBackend) CompensateRuleOutbox(_ context.Context, id, ruleID string, ok bool, errMsg string) error {
	if f.compensateOutboxHook != nil {
		if err := f.compensateOutboxHook(id, ruleID, ok); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, row := range f.outbox {
		if row.externalTxnID == id && row.ruleID == ruleID && row.status == store.OutboxPending {
			row.attempts++
			row.lastError = errMsg
			if ok {
				row.status = store.OutboxCompensated
			} else {
				row.status = store.OutboxCompensationFailed
			}
		}
	}
	return nil
}

// ListPendingRuleOutbox models the store's SELECT of pending obligations for the
// startup recovery sweep. A zero/negative limit returns all pending rows.
func (f *fakeBackend) ListPendingRuleOutbox(_ context.Context, limit int) ([]store.RuleOutboxItem, error) {
	if f.listPendingOutboxErr != nil {
		return nil, f.listPendingOutboxErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.RuleOutboxItem
	for _, row := range f.outbox {
		if row.status != store.OutboxPending {
			continue
		}
		out = append(out, store.RuleOutboxItem{
			ID:            row.id,
			ExternalTxnID: row.externalTxnID,
			RuleID:        row.ruleID,
			Status:        row.status,
			Attempts:      row.attempts,
			LastError:     row.lastError,
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// MarkRuleOutboxCompensated models the store's autocommit mark BY ROW ID used by
// the startup recovery sweep. Missing row => ErrNotFound-equivalent so a test
// can assert the sweep addressed a specific row.
func (f *fakeBackend) MarkRuleOutboxCompensated(_ context.Context, rowID int64, ok bool, errMsg string) error {
	if f.markOutboxCompHook != nil {
		if err := f.markOutboxCompHook(rowID, ok); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, row := range f.outbox {
		if row.id == rowID {
			row.attempts++
			row.lastError = errMsg
			if ok {
				row.status = store.OutboxCompensated
			} else {
				row.status = store.OutboxCompensationFailed
			}
			return nil
		}
	}
	return fmt.Errorf("fake: outbox row %d not found", rowID)
}

// pendingOutboxRow returns the pending outbox row for (id, ruleID), or nil.
// Caller must hold f.mu (buffered mutations run under the lock).
func (f *fakeBackend) pendingOutboxRow(id, ruleID string) *fakeOutboxRow {
	for _, row := range f.outbox {
		if row.externalTxnID == id && row.ruleID == ruleID && row.status == store.OutboxPending {
			return row
		}
	}
	return nil
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

// outboxStatusOf returns the committed status of the outbox obligation for
// (id, ruleID), or ("", false) when no such row exists. When multiple rows
// exist for the pair (e.g. a re-created rule), it returns the LAST one, which is
// the most recent obligation.
func (f *fakeBackend) outboxStatusOf(id, ruleID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	status, found := "", false
	for _, row := range f.outbox {
		if row.externalTxnID == id && row.ruleID == ruleID {
			status, found = row.status, true
		}
	}
	return status, found
}

// outboxCount returns the number of outbox rows recorded for a break.
func (f *fakeBackend) outboxCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, row := range f.outbox {
		if row.externalTxnID == id {
			n++
		}
	}
	return n
}

// seedOutbox preloads a committed pending outbox obligation, simulating a rule a
// prior (possibly crashed) attempt created without confirming or compensating.
// Used by the RecoverPendingRules startup-sweep tests.
func (f *fakeBackend) seedOutbox(id, ruleID string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outboxSeq++
	f.outbox = append(f.outbox, &fakeOutboxRow{
		id:            f.outboxSeq,
		externalTxnID: id,
		ruleID:        ruleID,
		status:        store.OutboxPending,
	})
	return f.outboxSeq
}

// claimOwnerOf returns the current durable lease owner for a break ("", false
// when unclaimed), so a test can assert the lease was released on completion.
func (f *fakeBackend) claimOwnerOf(id string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	owner, held := f.claims[id]
	return owner, held
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
	// A reference is included because the safe auto-remediation profiles for the
	// timing and amount_drift root causes pin the counterpart's identity on the
	// reference field (finding C-06); a break missing it fails closed to HITL.
	return blnk.ExternalTransaction{ID: id, Source: "bank-x", Amount: 10.0, Currency: "USD", Reference: "REF-" + id}
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
	rem := New(cls, bnk, back, back, testThreshold, "kimi-k3", "", 0)
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

// newHarnessCfg is newHarness with the two INFO-hardening knobs made explicit:
// the independent non-LLM base currency (INFO#2) and the per-break classify
// budget (INFO#3). The default newHarness passes ("", 0) so those backstops are
// dormant and every pre-existing test observes unchanged behavior; these two
// tests arm them to cover the NEW code paths.
func newHarnessCfg(t *testing.T, baseCurrency string, classifyBudget time.Duration) *harness {
	cls := &fakeClassifier{}
	bnk := &fakeBlnk{}
	back := newBackend(t)
	rem := New(cls, bnk, back, back, testThreshold, "kimi-k3", baseCurrency, classifyBudget)
	return &harness{rem: rem, cls: cls, bnk: bnk, back: back}
}

// TestHandleForeignCurrencyBackstopEscalates covers INFO#2: the independent,
// non-LLM regulated backstop. When a base currency is configured, a break whose
// currency differs from it must be routed to HITL as regulated REGARDLESS of the
// classifier's (LLM-sourced, prompt-injectable) regulated flag — so a flipped
// regulated=false can never open the auto path for a foreign-currency break.
// The disarmed contrast proves the currency backstop is the ONLY thing gating
// the otherwise fully auto-eligible break here.
func TestHandleForeignCurrencyBackstopEscalates(t *testing.T) {
	t.Run("armed backstop escalates a foreign-currency break despite regulated=false", func(t *testing.T) {
		const id = "ext_fx"
		// Base currency USD; classify budget dormant (0) to isolate INFO#2.
		h := newHarnessCfg(t, "USD", 0)
		// A fully auto-eligible classification: high confidence, NOT regulated,
		// valid proposed rule. Nothing in the classification would gate it.
		h.cls.result = autoClassification(id)
		h.bnk.created = blnk.MatchingRule{RuleID: "rule_1", Criteria: validRule().Criteria}
		h.bnk.cleared = true
		h.bnk.reconID = "recon_1"

		txn := txnFor(id)    // USD by default...
		txn.Currency = "EUR" // ...flipped to a foreign currency.

		if err := h.rem.Handle(context.Background(), txn, testUploadID); err != nil {
			t.Fatalf("Handle: %v", err)
		}

		status, _ := h.back.statusOf(id)
		if status != statusQueued {
			t.Fatalf("foreign-currency break must be queued, got %q", status)
		}
		reason, queued := h.back.inHITL(id)
		if !queued {
			t.Fatal("foreign-currency break must be in the HITL queue")
		}
		if reason != reasonForeignCurrency {
			t.Fatalf("HITL reason must be the currency backstop, got %q", reason)
		}
		// A well-formed classification still emits its classified event before the gate...
		if got := h.back.countAction(id, audit.ActionClassified); got != 1 {
			t.Fatalf("classified events: want 1 got %d", got)
		}
		// ...then exactly one escalation, and NOTHING on the auto path.
		if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
			t.Fatalf("escalated events: want 1 got %d", got)
		}
		if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
			t.Fatalf("a foreign-currency break must never be resolved, got %d resolved events", got)
		}
		if h.bnk.createCount() != 0 {
			t.Fatalf("auto path must not create a rule for a foreign-currency break, got %d", h.bnk.createCount())
		}
		if h.bnk.probeCount() != 0 {
			t.Fatalf("auto path must not probe for a foreign-currency break, got %d", h.bnk.probeCount())
		}
	})

	t.Run("disarmed backstop lets the same break reach the auto path", func(t *testing.T) {
		const id = "ext_fx_off"
		// baseCurrency "" (autoHarness default) => backstop OFF.
		h := autoHarness(t, id)
		txn := txnFor(id)
		txn.Currency = "EUR" // identical foreign currency as the armed case

		if err := h.rem.Handle(context.Background(), txn, testUploadID); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		status, found := h.back.statusOf(id)
		if !found || status != statusAutoResolved {
			t.Fatalf("with the backstop disarmed the break must auto-resolve, got found=%v status=%q", found, status)
		}
		if _, queued := h.back.inHITL(id); queued {
			t.Fatal("with the backstop disarmed the break must NOT be in HITL")
		}
	})
}

// TestHandleClassifyBudgetTimeoutFailsClosed covers INFO#3: a per-break classify
// budget so one hung LLM call cannot consume the whole run's budget. When the
// budget elapses the break fails closed to HITL (Rule 5.7) — never dropped,
// never auto-resolved — and the call returns without waiting for the full,
// slower classifier delay. The disarmed contrast (budget 0) proves a fast call
// within budget still flows to the auto path unchanged.
func TestHandleClassifyBudgetTimeoutFailsClosed(t *testing.T) {
	t.Run("a classify call exceeding the budget fails closed", func(t *testing.T) {
		const id = "ext_slow"
		// Tiny 20ms budget; classifier blocks 2s (a hung endpoint). The budget
		// must cancel the call long before the delay elapses.
		h := newHarnessCfg(t, "", 20*time.Millisecond)
		h.cls.delay = 2 * time.Second
		h.cls.result = autoClassification(id) // never observed: the call times out first

		start := time.Now()
		if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		elapsed := time.Since(start)
		if elapsed >= time.Second {
			t.Fatalf("classify budget must cut the hung call short; waited %v (>= 1s)", elapsed)
		}

		status, _ := h.back.statusOf(id)
		if status != statusQueued {
			t.Fatalf("a classify-timeout break must be queued, got %q", status)
		}
		reason, queued := h.back.inHITL(id)
		if !queued {
			t.Fatal("a classify-timeout break must be in the HITL queue")
		}
		// Fail-closed reason with the underlying deadline surfaced.
		if !strings.HasPrefix(reason, reasonClassifierFailed) {
			t.Fatalf("HITL reason must be the fail-closed classifier reason, got %q", reason)
		}
		if !strings.Contains(reason, "deadline exceeded") {
			t.Fatalf("HITL reason should surface the classify-budget timeout, got %q", reason)
		}
		// Classification did not succeed => no classified event, exactly one escalation.
		if got := h.back.countAction(id, audit.ActionClassified); got != 0 {
			t.Fatalf("no classified event must be emitted on a classify timeout, got %d", got)
		}
		if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
			t.Fatalf("escalated events: want 1 got %d", got)
		}
		if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
			t.Fatalf("a classify-timeout break must never be resolved, got %d resolved events", got)
		}
		if h.bnk.createCount() != 0 || h.bnk.probeCount() != 0 {
			t.Fatalf("auto path must not run on a classify timeout, got creates=%d probes=%d", h.bnk.createCount(), h.bnk.probeCount())
		}
	})

	t.Run("a fast classify within budget reaches the auto path", func(t *testing.T) {
		const id = "ext_fast"
		// Generous 2s budget; classifier returns almost immediately.
		h := newHarnessCfg(t, "", 2*time.Second)
		h.cls.delay = 1 * time.Millisecond
		h.cls.result = autoClassification(id)
		h.bnk.created = blnk.MatchingRule{RuleID: "rule_1", Criteria: validRule().Criteria}
		h.bnk.cleared = true
		h.bnk.reconID = "recon_1"

		if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		status, found := h.back.statusOf(id)
		if !found || status != statusAutoResolved {
			t.Fatalf("a fast classify within budget must auto-resolve, got found=%v status=%q", found, status)
		}
		if _, queued := h.back.inHITL(id); queued {
			t.Fatal("a fast classify within budget must NOT be in HITL")
		}
	})
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

// TestHandleEscalatesOnExpiredContext reproduces finding MINOR-1: under an LLM
// black-hole the one-shot pipeline context can elapse WHILE classifying earlier
// breaks, so a later break reaches escalate with an ALREADY-EXPIRED caller
// context. The fail-closed escalation write MUST still succeed — the break is
// never dropped — because Handle's idempotency read and escalate's persistence
// both run on a context detached from the caller's deadline (finding MINOR-1).
//
// Before the fix, escalate reused the expired caller context: BeginTx failed
// with "context deadline exceeded", Handle returned an error, the break was
// persisted NOWHERE (0 break / 0 hitl / 0 audit rows), and the pipeline aborted,
// dropping this break and every remaining one. This test fails against that old
// behavior and passes against the detached-context fix, guarding the regression.
func TestHandleEscalatesOnExpiredContext(t *testing.T) {
	const id = "ext_expiredctx"
	h := newHarness(t)
	// The LLM black-hole ultimately surfaces as a classifier failure after the
	// retry cap (Rule 5.7). Wrapping the sentinel yields the clean
	// classifier-failed escalation reason.
	h.cls.err = fmt.Errorf("llm black-hole: %w", classifier.ErrClassificationFailed)

	// An already-expired DEADLINE context — exactly the state the pipeline
	// context is in for a break reached after the inference budget elapsed under
	// a hung LLM. fakeBackend.WithTx threads this through sql.DB.BeginTx, which
	// honors an expired context, so this faithfully exercises the failure the
	// detached persistence context must survive.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel()
	if ctx.Err() == nil {
		t.Fatal("precondition: context must already be expired")
	}

	// The break must be ESCALATED, not dropped: Handle returns nil despite the
	// expired context.
	if err := h.rem.Handle(ctx, txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle must escalate (never drop) a break reached with an expired context, got error: %v", err)
	}

	// It is durably queued for HITL...
	status, found := h.back.statusOf(id)
	if !found || status != statusQueued {
		t.Fatalf("break must be persisted as queued despite expired ctx, got found=%v status=%q", found, status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("break must be enqueued for HITL despite expired ctx (never dropped)")
	}
	// ...with exactly one escalated audit event, and never a classified or
	// resolved event (classification failed; the LLM never marks a match).
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("expected exactly 1 escalated audit event, got %d", got)
	}
	if got := h.back.countAction(id, audit.ActionClassified); got != 0 {
		t.Fatalf("no classified event when classification failed, got %d", got)
	}
	if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
		t.Fatalf("an expired-context escalation must never resolve a break, got %d resolved", got)
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
		// wantDelete asserts findings F5/M-11: a rule this invocation created for
		// an attempt that did not clear MUST be durably compensated (deleted)
		// before the break escalates, so a failed auto-attempt leaves no orphan
		// Blnk rule.
		wantDelete int
	}{
		{
			name: "Blnk rule-create failure escalates",
			setup: func(h *harness) {
				h.bnk.createErr = errors.New("blnk 500")
			},
			wantCreate: 1,
			wantProbe:  0,
			wantDelete: 0, // creation failed => no rule id to delete
		},
		{
			name: "empty created rule id is guarded before probing (m-7)",
			setup: func(h *harness) {
				h.bnk.created = blnk.MatchingRule{RuleID: ""} // empty id
			},
			wantCreate: 1,
			wantProbe:  0,
			wantDelete: 0, // no usable rule id to delete
		},
		{
			name: "Blnk probe failure escalates",
			setup: func(h *harness) {
				h.bnk.probeErr = errors.New("probe timeout")
			},
			wantCreate: 1,
			wantProbe:  1,
			wantDelete: 1, // F5: created rule rolled back on probe failure
		},
		{
			name: "dry-run that does not clear escalates",
			setup: func(h *harness) {
				h.bnk.cleared = false
				h.bnk.reconID = "recon_x"
			},
			wantCreate: 1,
			wantProbe:  1,
			wantDelete: 1, // F5: created rule rolled back when not cleared
		},
		{
			name: "cleared but empty recon id cannot prove resolution (Rule 5.3)",
			setup: func(h *harness) {
				h.bnk.cleared = true
				h.bnk.reconID = "" // no proof
			},
			wantCreate: 1,
			wantProbe:  1,
			wantDelete: 1, // F5: created rule rolled back when clearance unprovable
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
			// F5/M-11: verify durable compensation of a rule created for a
			// non-clearing attempt.
			if got := h.bnk.deleteCount(); got != tc.wantDelete {
				t.Fatalf("Blnk delete calls: want %d got %d", tc.wantDelete, got)
			}
			if tc.wantDelete == 1 {
				if dels := h.bnk.deletedRules(); len(dels) != 1 || dels[0] != "rule_1" {
					t.Fatalf("F5: expected the created rule %q to be deleted, got %v", "rule_1", dels)
				}
				// M-11: the compensation OUTCOME must be durably recorded to the
				// append-only audit trail — a best-effort delete whose result is
				// discarded is exactly the behavior this finding forbids.
				if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 1 {
					t.Fatalf("M-11: expected exactly 1 rule_compensated audit event, got %d", got)
				}
				// M-11: and the durable outbox obligation must be retired
				// (compensated), so the startup sweep does not re-process it.
				if st, ok := h.back.outboxStatusOf(id, "rule_1"); !ok || st != store.OutboxCompensated {
					t.Fatalf("M-11: created rule's outbox obligation must be marked compensated, got %q found=%v", st, ok)
				}
			} else {
				// No rule was durably created here, so no compensation obligation
				// or event should exist.
				if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 0 {
					t.Fatalf("M-11: no rule was created, expected 0 rule_compensated events, got %d", got)
				}
			}
			// F5: whichever branch escalated, the break must retain NO persisted
			// created-rule id — either none was ever set, or the compensation
			// cleared it — so a later HITL re_drive cannot reuse a deleted rule.
			if crid := h.back.createdRuleOf(id); crid != "" {
				t.Fatalf("F5: escalated auto-path break must not retain a created-rule id, got %q", crid)
			}
			// M-15: the durable processing lease acquired for this attempt must be
			// released on completion so a retry or another instance can reclaim it.
			if owner, held := h.back.claimOwnerOf(id); held {
				t.Fatalf("M-15: processing lease must be released after handling, still held by %q", owner)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Finding C-06 (semantic safety): the agent does NOT trust the classifier's
// advisory rule STRUCTURE. Before any POST it builds a deterministic,
// root-cause-specific safe rule from the transaction's OWN fields
// (safeRuleForBreak) so the semantic safety of an auto-remediation is a property
// of the code, not of weakly-bounded model output. These tests assert the
// through-Handle behavior for each auto-eligible root cause; the pure-function
// profile shapes are additionally locked in by TestSafeRuleForBreakProfiles.
// -----------------------------------------------------------------------------

// C-06: for an amount_drift break the safe profile pins {amount==(bounded drift),
// currency==, reference==}, each Value populated from the transaction. A
// deliberately unsafe advisory rule (amount-only, empty Value) is IGNORED and
// left unmutated, and the break still auto-resolves.
func TestHandleC6BuildsDeterministicAmountDriftProfile(t *testing.T) {
	const id = "ext_c6"
	h := newHarness(t)
	txn := blnk.ExternalTransaction{ID: id, Source: "bank-x", Amount: 1500.5, Currency: "USD", Reference: "INV-1001"}
	// The classifier proposes a DELIBERATELY unsafe amount-only rule with an
	// empty Value — exactly the shape C-06 says must NOT be trusted. The agent
	// must ignore this STRUCTURE and build its own safe profile.
	origRule := &blnk.MatchingRule{
		Name:     "amount-eq",
		Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: "equals", Value: ""}},
	}
	h.cls.result = model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     model.RootCauseAmountDrift,
		Confidence:    0.95,
		ProposedRule:  origRule,
		Rationale:     "amount drift within fee tolerance",
	}
	h.bnk.created = blnk.MatchingRule{RuleID: "rule_1"}
	h.bnk.cleared = true
	h.bnk.reconID = "recon_1"

	if err := h.rem.Handle(context.Background(), txn, testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	posted, ok := h.bnk.lastCreatedRule()
	if !ok {
		t.Fatal("C-06: expected a safe profile rule to be POSTed to Blnk")
	}
	got := map[string]blnk.MatchingCriteria{}
	for _, c := range posted.Criteria {
		got[c.Field] = c
	}
	// The amount_drift profile pins amount, currency AND reference.
	if len(posted.Criteria) != 3 {
		t.Fatalf("C-06: amount_drift profile must pin exactly 3 fields, got %d: %+v", len(posted.Criteria), posted.Criteria)
	}
	if c, ok := got["amount"]; !ok || c.Operator != "equals" || c.Value != "1500.5" {
		t.Fatalf("C-06: amount criterion must be {equals,1500.5}, got %+v (present=%v)", c, ok)
	}
	if c, ok := got["currency"]; !ok || c.Operator != "equals" || c.Value != "USD" {
		t.Fatalf("C-06: currency criterion must be {equals,USD}, got %+v (present=%v)", c, ok)
	}
	if c, ok := got["reference"]; !ok || c.Operator != "equals" || c.Value != "INV-1001" {
		t.Fatalf("C-06: reference criterion must be {equals,INV-1001}, got %+v (present=%v)", c, ok)
	}
	// C-06: the agent builds a FRESH rule; the classifier's advisory rule is
	// neither consulted for structure nor mutated.
	if len(origRule.Criteria) != 1 || origRule.Criteria[0].Value != "" {
		t.Fatalf("C-06: advisory rule must be left untouched, got %+v", origRule.Criteria)
	}
	// The safe profile is a valid equality rule, so the break still auto-resolves.
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("C-06: safe profile should still auto-resolve, got %q", status)
	}
}

// C-06: the reference_mismatch profile pins {amount==, currency==} and OMITS the
// reference (the field that legitimately differs for this cause), emitting the
// currency exactly once and never a duplicate.
func TestHandleC6ReferenceMismatchProfileOmitsReference(t *testing.T) {
	const id = "ext_c6_ref"
	h := newHarness(t)
	txn := blnk.ExternalTransaction{ID: id, Source: "bank-x", Amount: 42.0, Currency: "EUR", Reference: "VENDOR-REF-9"}
	h.cls.result = model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     model.RootCauseReferenceMismatch,
		Confidence:    0.95,
		ProposedRule:  validRule(),
		Rationale:     "reference mismatch",
	}
	h.bnk.created = blnk.MatchingRule{RuleID: "rule_1"}
	h.bnk.cleared = true
	h.bnk.reconID = "recon_1"

	if err := h.rem.Handle(context.Background(), txn, testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	posted, _ := h.bnk.lastCreatedRule()
	amtCount, curCount, refCount := 0, 0, 0
	for _, c := range posted.Criteria {
		switch c.Field {
		case "amount":
			amtCount++
		case "currency":
			curCount++
			if c.Value != "EUR" {
				t.Fatalf("C-06: currency criterion must be populated from txn, got %q", c.Value)
			}
		case "reference":
			refCount++
		}
	}
	if amtCount != 1 || curCount != 1 {
		t.Fatalf("C-06: reference_mismatch profile must pin exactly one amount and one currency, got amount=%d currency=%d", amtCount, curCount)
	}
	if refCount != 0 {
		t.Fatalf("C-06: reference_mismatch profile must OMIT the reference (the differing field), got %d reference criteria", refCount)
	}
}

// -----------------------------------------------------------------------------
// Finding F5: a rule this invocation created for an auto-attempt that did not
// clear is rolled back (deleted + persisted id cleared) so no orphan Blnk rule
// accumulates. The rollback is best-effort and never blocks the fail-closed
// escalation; a reused rule and a successfully-resolving rule are never deleted.
// -----------------------------------------------------------------------------

// A successful resolution must NOT delete the rule that cleared the break, and
// its durable compensation obligation must be CONFIRMED (not compensated) so the
// startup recovery sweep never mistakes a legitimately-clearing rule for an
// orphan (findings F5, M-11).
func TestHandleF5NoRollbackOnSuccess(t *testing.T) {
	const id = "ext_f5_ok"
	h := autoHarness(t, id)

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("expected auto-resolved, got %q", status)
	}
	if h.bnk.deleteCount() != 0 {
		t.Fatalf("F5: a rule that cleared the break must NOT be deleted, got %d deletes", h.bnk.deleteCount())
	}
	if crid := h.back.createdRuleOf(id); crid != "rule_1" {
		t.Fatalf("F5: a resolving rule id must be retained, got %q", crid)
	}
	// M-11: the outbox obligation recorded at create time must be CONFIRMED in the
	// SAME commit that resolved the break, so a rule that did its job is never
	// swept as an orphan.
	if st, ok := h.back.outboxStatusOf(id, "rule_1"); !ok || st != store.OutboxConfirmed {
		t.Fatalf("M-11: resolving rule's outbox obligation must be confirmed, got %q found=%v", st, ok)
	}
	// M-11: a resolution is a normal (non-compensating) outcome; no compensation
	// event should exist.
	if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 0 {
		t.Fatalf("M-11: a resolved break must record 0 rule_compensated events, got %d", got)
	}
}

// M-11: when the compensating Blnk delete FAILS, the break still escalates
// fail-closed (Rule 5.7) — but the failure is no longer silently swallowed.
// A rule_compensated audit event and a 'compensation_failed' outbox obligation
// are durably recorded, so an orphaned rule that could not be removed stays
// visible for manual cleanup. Because the delete failed the rule likely still
// exists, so the persisted reference is retained rather than blindly cleared.
func TestHandleM11CompensationFailureLeavesDurableEvidence(t *testing.T) {
	const id = "ext_f5_beffort"
	h := autoHarness(t, id)
	h.bnk.cleared = false // probe does not clear => compensation path
	h.bnk.reconID = "recon_x"
	h.bnk.deleteErr = errors.New("blnk delete 500")

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle must not fail when rule compensation fails (fail-closed): %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("break must still escalate despite compensation failure, got %q", status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("break must be enqueued for HITL despite compensation failure (Rule 5.7)")
	}
	if h.bnk.deleteCount() != 1 {
		t.Fatalf("M-11: compensating delete must be ATTEMPTED once, got %d", h.bnk.deleteCount())
	}
	// M-11: the FAILED cleanup must be durably audited — the exact obligation the
	// finding says the old best-effort rollback dropped on the floor.
	if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 1 {
		t.Fatalf("M-11: a failed compensation must record exactly 1 rule_compensated event, got %d", got)
	}
	prov, ok := h.back.auditProvenance(id, audit.ActionRuleCompensated)
	if !ok || prov.RuleID != "rule_1" {
		t.Fatalf("M-11: rule_compensated event must name the failed rule in provenance, got %+v found=%v", prov, ok)
	}
	// M-11: the durable outbox obligation must record the compensation FAILURE so
	// the orphan stays visible (compensation_failed is a terminal needs-cleanup
	// state, not a swept-away 'compensated').
	if st, ok := h.back.outboxStatusOf(id, "rule_1"); !ok || st != store.OutboxCompensationFailed {
		t.Fatalf("M-11: failed compensation's outbox obligation must be compensation_failed, got %q found=%v", st, ok)
	}
	// Delete failed, so the rule likely still exists — keep the reference.
	if crid := h.back.createdRuleOf(id); crid != "rule_1" {
		t.Fatalf("M-11: on delete failure the created-rule id must be retained, got %q", crid)
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("a non-clearing break must never resolve")
	}
}

// A REUSED (already-persisted) rule is not this call's to roll back: only a rule
// created in THIS invocation is deleted on failure.
func TestHandleF5DoesNotDeleteReusedRuleOnFailure(t *testing.T) {
	const id = "ext_f5_reused"
	h := autoHarness(t, id)
	// A prior attempt already created & persisted this rule (resume-classified).
	h.back.seedBreak(id, autoClassification(id), statusClassified, "rule_prior")
	h.bnk.cleared = false // this attempt's probe does not clear
	h.bnk.reconID = "recon_x"

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if h.bnk.createCount() != 0 {
		t.Fatalf("resume must reuse the persisted rule, got %d creates", h.bnk.createCount())
	}
	if h.bnk.deleteCount() != 0 {
		t.Fatalf("F5: a reused rule must NOT be deleted on failure, got %d deletes", h.bnk.deleteCount())
	}
	if crid := h.back.createdRuleOf(id); crid != "rule_prior" {
		t.Fatalf("F5: a reused rule id must be retained, got %q", crid)
	}
	// M-11: a rule NOT created in this call carries no compensation obligation for
	// this call, so no rule_compensated event may fire (the createdHere gate).
	if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 0 {
		t.Fatalf("M-11: a reused rule must NOT be compensated by this call, got %d rule_compensated events", got)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("break must escalate, got %q", status)
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

// M-11: when the create-rule transaction (SetCreatedRuleTx + RecordRuleOutboxTx
// + rule_created audit) rolls back, the Blnk rule POST already succeeded, so the
// rule EXISTS in Blnk with no durable local record — the orphan scenario the
// finding targets. The remediator must immediately COMPENSATE (delete the rule
// from Blnk, audit the outcome) and escalate the break fail-closed to HITL,
// rather than surfacing a bare error and leaking the orphan.
func TestHandleAtomicity_RuleStoreFailure(t *testing.T) {
	const id = "ext_rulestore"
	h := autoHarness(t, id)
	h.back.setCreatedRuleTxHook = func(_, _ string) error { return errors.New("store down") }
	// M-11: the failure is handled durably (compensation + escalation), NOT
	// surfaced as a raw error — Handle returns nil having routed the break to HITL.
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("create-tx failure must be handled fail-closed, not surfaced: %v", err)
	}
	// The Blnk rule was created (external side effect)...
	if h.bnk.createCount() != 1 {
		t.Fatalf("expected exactly 1 Blnk create attempt, got %d", h.bnk.createCount())
	}
	// ...and then COMPENSATED: deleted from Blnk with the outcome durably audited.
	if h.bnk.deleteCount() != 1 {
		t.Fatalf("M-11: the orphaned rule must be deleted from Blnk exactly once, got %d", h.bnk.deleteCount())
	}
	if dels := h.bnk.deletedRules(); len(dels) != 1 || dels[0] != "rule_1" {
		t.Fatalf("M-11: the created rule must be the one deleted, got %v", dels)
	}
	if got := h.back.countAction(id, audit.ActionRuleCompensated); got != 1 {
		t.Fatalf("M-11: compensation of the orphaned rule must be audited exactly once, got %d", got)
	}
	// The local created-rule id and rule_created event were rolled back and never
	// became durable...
	if h.back.createdRuleOf(id) != "" {
		t.Fatal("created rule id must NOT be durable after the rule transaction rolled back")
	}
	if h.back.countAction(id, audit.ActionRuleCreated) != 0 {
		t.Fatal("no rule_created audit may be durable after rollback")
	}
	// ...the pending outbox row was part of the SAME rolled-back transaction, so it
	// never committed (the compensation's outbox mark is a harmless no-op here).
	if got := h.back.outboxCount(id); got != 0 {
		t.Fatalf("M-11: the outbox row was rolled back with the failed tx, expected 0 rows, got %d", got)
	}
	// ...and the break is escalated fail-closed, never resolved.
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("break must be escalated fail-closed, got %q", status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("break must be enqueued for HITL after create-tx failure (Rule 5.7)")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("break must not be resolved when the rule commit failed")
	}
}

func TestHandleAtomicity_ResolveStoreFailure(t *testing.T) {
	const id = "ext_resstore"
	h := autoHarness(t, id)
	h.back.markResolvedTxHook = func(_, _ string) error {
		return errors.New("store down")
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

// TestHandleC6IgnoresOverbroadAdvisoryOperator is the C-06 inversion of the old
// "overbroad operator escalates" test. Under the pre-fix design the agent
// VALIDATED and applied the classifier's advisory rule, so a non-equality
// operator escalated. Under C-06 the agent IGNORES the advisory rule STRUCTURE
// entirely and builds a deterministic, all-equality safe profile from the
// transaction's own fields — so an overbroad advisory operator is simply
// discarded, the safe profile reaches Blnk, and (Blnk confirming clearance) the
// break auto-resolves. The semantic safety is now a property of the code, not of
// the model's operator choice.
func TestHandleC6IgnoresOverbroadAdvisoryOperator(t *testing.T) {
	for _, op := range []string{"greater_than", "less_than", "contains"} {
		t.Run(op, func(t *testing.T) {
			id := "ext_overbroad_" + op
			h := autoHarness(t, id)
			c := autoClassification(id) // auto-eligible root cause, high confidence
			// A grammar-valid but OVERBROAD advisory rule. C-06 must not trust it.
			c.ProposedRule = &blnk.MatchingRule{
				Name:     "overbroad",
				Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: op, Value: "10.00"}},
			}
			h.cls.result = c

			if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			// The advisory operator is ignored; the safe profile auto-resolves.
			if status, _ := h.back.statusOf(id); status != statusAutoResolved {
				t.Fatalf("status: want %q got %q", statusAutoResolved, status)
			}
			if got := h.back.countAction(id, audit.ActionResolved); got != 1 {
				t.Fatalf("resolved events: want 1 got %d", got)
			}
			if got := h.back.countAction(id, audit.ActionEscalated); got != 0 {
				t.Fatalf("escalated events: want 0 got %d", got)
			}
			if got := h.bnk.createCount(); got != 1 {
				t.Fatalf("Blnk create calls: want 1 got %d", got)
			}
			if got := h.bnk.probeCount(); got != 1 {
				t.Fatalf("Blnk probe calls: want 1 got %d", got)
			}
			// C-06 proof: the rule the agent actually POSTed uses ONLY the equals
			// operator — the overbroad advisory operator never reached Blnk.
			posted, ok := h.bnk.lastCreatedRule()
			if !ok {
				t.Fatal("expected a safe profile rule to be POSTed")
			}
			for _, cr := range posted.Criteria {
				if cr.Operator != "equals" {
					t.Fatalf("C-06: safe profile must use only equals, found operator %q", cr.Operator)
				}
			}
		})
	}
}

// TestAutoApplySafe unit-tests the semantic-safety predicate that now serves as a
// DEFENSIVE post-build assertion (finding C-06): safeRuleForBreak only ever emits
// equality criteria, and autoApplySafe re-checks the built rule so any future
// regression that introduced a non-equality operator would fail closed rather
// than auto-apply an overbroad rule. An equality-only rule is safe; any
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

// -----------------------------------------------------------------------------
// Finding C-06: pure-function coverage of the deterministic safe-rule profiles.
// safeRuleForBreak is the heart of the C-06 fix; these tests lock in the exact
// criteria each auto-eligible root cause emits and prove every unsafe input
// fails closed (an error the caller turns into a HITL escalation).
// -----------------------------------------------------------------------------

func TestSafeRuleForBreakProfiles(t *testing.T) {
	// fields returns the {field->criterion} map of a rule for compact assertions.
	fields := func(r blnk.MatchingRule) map[string]blnk.MatchingCriteria {
		m := map[string]blnk.MatchingCriteria{}
		for _, c := range r.Criteria {
			m[c.Field] = c
		}
		return m
	}

	txn := blnk.ExternalTransaction{ID: "EXT-1", Source: "bank-x", Amount: 250.75, Currency: "USD", Reference: "INV-1002"}

	t.Run("timing pins amount+currency+reference and omits date", func(t *testing.T) {
		c := model.BreakClassification{RootCause: model.RootCauseTiming}
		rule, err := safeRuleForBreak(c, txn)
		if err != nil {
			t.Fatalf("timing profile must build, got %v", err)
		}
		f := fields(rule)
		if len(rule.Criteria) != 3 {
			t.Fatalf("timing profile must have 3 criteria, got %d", len(rule.Criteria))
		}
		if f["amount"].Operator != "equals" || f["amount"].Value != "250.75" || f["amount"].AllowableDrift != 0 {
			t.Fatalf("timing amount criterion wrong: %+v", f["amount"])
		}
		if f["currency"].Operator != "equals" || f["currency"].Value != "USD" {
			t.Fatalf("timing currency criterion wrong: %+v", f["currency"])
		}
		if f["reference"].Operator != "equals" || f["reference"].Value != "INV-1002" {
			t.Fatalf("timing reference criterion wrong: %+v", f["reference"])
		}
		if _, hasDate := f["date"]; hasDate {
			t.Fatal("timing profile must OMIT date (the field that legitimately differs)")
		}
	})

	t.Run("amount_drift pins amount(bounded)+currency+reference", func(t *testing.T) {
		c := model.BreakClassification{
			RootCause:    model.RootCauseAmountDrift,
			ProposedRule: &blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: 0.02}}},
		}
		rule, err := safeRuleForBreak(c, txn)
		if err != nil {
			t.Fatalf("amount_drift profile must build, got %v", err)
		}
		f := fields(rule)
		if len(rule.Criteria) != 3 {
			t.Fatalf("amount_drift profile must have 3 criteria, got %d", len(rule.Criteria))
		}
		if f["amount"].AllowableDrift != 0.02 {
			t.Fatalf("amount_drift must carry the bounded drift 0.02, got %v", f["amount"].AllowableDrift)
		}
		if _, hasRef := f["reference"]; !hasRef {
			t.Fatal("amount_drift profile must pin reference")
		}
	})

	t.Run("reference_mismatch pins amount+currency and omits reference", func(t *testing.T) {
		c := model.BreakClassification{RootCause: model.RootCauseReferenceMismatch}
		rule, err := safeRuleForBreak(c, txn)
		if err != nil {
			t.Fatalf("reference_mismatch profile must build, got %v", err)
		}
		f := fields(rule)
		if len(rule.Criteria) != 2 {
			t.Fatalf("reference_mismatch profile must have 2 criteria, got %d", len(rule.Criteria))
		}
		if _, hasRef := f["reference"]; hasRef {
			t.Fatal("reference_mismatch profile must OMIT reference (the differing field)")
		}
		if _, hasAmt := f["amount"]; !hasAmt {
			t.Fatal("reference_mismatch profile must pin amount")
		}
	})

	t.Run("missing currency fails closed for any cause", func(t *testing.T) {
		noCur := blnk.ExternalTransaction{ID: "EXT-2", Amount: 10, Reference: "R"}
		for _, rc := range []model.RootCause{model.RootCauseTiming, model.RootCauseAmountDrift, model.RootCauseReferenceMismatch} {
			if _, err := safeRuleForBreak(model.BreakClassification{RootCause: rc}, noCur); err == nil {
				t.Fatalf("%s with no currency must fail closed", rc)
			}
		}
	})

	t.Run("timing/amount_drift missing reference fail closed", func(t *testing.T) {
		noRef := blnk.ExternalTransaction{ID: "EXT-3", Amount: 10, Currency: "USD"}
		if _, err := safeRuleForBreak(model.BreakClassification{RootCause: model.RootCauseTiming}, noRef); err == nil {
			t.Fatal("timing with no reference must fail closed")
		}
		if _, err := safeRuleForBreak(model.BreakClassification{RootCause: model.RootCauseAmountDrift}, noRef); err == nil {
			t.Fatal("amount_drift with no reference must fail closed")
		}
	})

	t.Run("non-eligible root causes have no safe profile", func(t *testing.T) {
		for _, rc := range []model.RootCause{
			model.RootCauseDuplicate, model.RootCauseMissingInternal,
			model.RootCauseCurrencyMismatch, model.RootCauseUnknown,
		} {
			if _, err := safeRuleForBreak(model.BreakClassification{RootCause: rc}, txn); err == nil {
				t.Fatalf("root cause %q must have no safe auto-remediation profile", rc)
			}
		}
	})
}

// TestBoundedAmountDrift covers finding C-06's hard cap on the auto-applied
// amount tolerance: the drift is taken from the classifier's advisory proposal
// but can only ever be clamped DOWN, never widened, and any non-finite or
// non-positive value is ignored.
func TestBoundedAmountDrift(t *testing.T) {
	amt := func(d float64) *blnk.MatchingRule {
		return &blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{{Field: "amount", Operator: "equals", AllowableDrift: d}}}
	}
	cases := []struct {
		name string
		in   *blnk.MatchingRule
		want float64
	}{
		{"nil proposal yields exact match", nil, 0},
		{"no amount criterion yields 0", &blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{{Field: "currency", Operator: "equals", AllowableDrift: 0.99}}}, 0},
		{"within cap is preserved", amt(0.02), 0.02},
		{"above cap is clamped down", amt(0.10), maxAmountDriftFraction},
		{"exactly at cap", amt(maxAmountDriftFraction), maxAmountDriftFraction},
		{"NaN is ignored", amt(math.NaN()), 0},
		{"positive infinity is ignored", amt(math.Inf(1)), 0},
		{"negative is ignored", amt(-0.03), 0},
		{"zero is ignored", amt(0), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedAmountDrift(tc.in); got != tc.want {
				t.Fatalf("boundedAmountDrift = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("largest finite positive is taken then capped", func(t *testing.T) {
		multi := &blnk.MatchingRule{Criteria: []blnk.MatchingCriteria{
			{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
			{Field: "amount", Operator: "equals", AllowableDrift: 0.20}, // largest, over cap
			{Field: "amount", Operator: "equals", AllowableDrift: math.NaN()},
		}}
		if got := boundedAmountDrift(multi); got != maxAmountDriftFraction {
			t.Fatalf("boundedAmountDrift = %v, want the cap %v", got, maxAmountDriftFraction)
		}
	})
}

// TestFormatAmount locks in the canonical amount rendering stamped into a
// criterion Value (Blnk ignores the Value, but the audit trail reads it).
func TestFormatAmount(t *testing.T) {
	cases := map[float64]string{
		1500.5: "1500.5",
		250.0:  "250",
		0:      "0",
		0.01:   "0.01",
	}
	for in, want := range cases {
		if got := formatAmount(in); got != want {
			t.Fatalf("formatAmount(%v) = %q, want %q", in, got, want)
		}
	}
}

// -----------------------------------------------------------------------------
// Finding M-15: the durable processing lease gates external remediation. A break
// whose lease is held by another live instance is a safe no-op; a claim error
// fails the call rather than proceeding unguarded.
// -----------------------------------------------------------------------------

// TestHandleClaimNotGrantedYields: when another instance holds the lease, this
// call classifies+persists the fresh break (that happens before the claim) but
// then YIELDS — it performs no external Blnk work and records no terminal event.
func TestHandleClaimNotGrantedYields(t *testing.T) {
	const id = "ext_claim_busy"
	h := autoHarness(t, id)
	h.back.claimBreakHook = func(_, _ string) (bool, error) { return false, nil }

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("a not-granted lease must be a safe no-op, got error: %v", err)
	}
	// The fresh break was classified+persisted before the claim...
	if status, found := h.back.statusOf(id); !found || status != statusClassified {
		t.Fatalf("break must remain classified after yielding, got found=%v status=%q", found, status)
	}
	if got := h.back.countAction(id, audit.ActionClassified); got != 1 {
		t.Fatalf("expected exactly 1 classified event, got %d", got)
	}
	// ...but NO external remediation happened, and no terminal outcome recorded.
	if h.bnk.createCount() != 0 || h.bnk.probeCount() != 0 {
		t.Fatalf("a yielded break must do no Blnk work, got create=%d probe=%d", h.bnk.createCount(), h.bnk.probeCount())
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 || h.back.countAction(id, audit.ActionEscalated) != 0 {
		t.Fatal("a yielded break must record neither a resolved nor an escalated event")
	}
	if _, queued := h.back.inHITL(id); queued {
		t.Fatal("a yielded break must NOT be enqueued for HITL")
	}
}

// TestHandleClaimErrorSurfaces: a lease-acquisition ERROR (vs a clean not-granted)
// surfaces as a Handle error so the caller can retry, and no external work runs.
func TestHandleClaimErrorSurfaces(t *testing.T) {
	const id = "ext_claim_err"
	h := autoHarness(t, id)
	h.back.claimBreakHook = func(_, _ string) (bool, error) { return false, errors.New("lease table down") }

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("a claim error must surface as a Handle error")
	}
	if h.bnk.createCount() != 0 {
		t.Fatalf("no Blnk work may run when the lease could not be claimed, got %d creates", h.bnk.createCount())
	}
}

// TestHandleReleasesLeaseOnResolve: the happy path releases the lease it acquired
// so a later re_drive (or another instance) can reclaim the break.
func TestHandleReleasesLeaseOnResolve(t *testing.T) {
	const id = "ext_lease_release"
	h := autoHarness(t, id)
	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusAutoResolved {
		t.Fatalf("expected auto-resolved, got %q", status)
	}
	if owner, held := h.back.claimOwnerOf(id); held {
		t.Fatalf("lease must be released after resolution, still held by %q", owner)
	}
}

// -----------------------------------------------------------------------------
// Finding M-11: startup recovery sweep for the durable rule-compensation outbox.
// A 'pending' obligation left by a crashed attempt is CONFIRMED if its break
// resolved (the rule did its job) or COMPENSATED (the orphan is deleted +
// audited) otherwise. A per-row failure never aborts the sweep.
// -----------------------------------------------------------------------------

func TestRecoverPendingRules(t *testing.T) {
	t.Run("empty outbox is a no-op", func(t *testing.T) {
		h := newHarness(t)
		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 0 {
			t.Fatalf("empty sweep: got n=%d err=%v, want 0,nil", n, err)
		}
	})

	t.Run("resolved break confirms the obligation, keeps the rule", func(t *testing.T) {
		const id = "ext_rec_ok"
		h := newHarness(t)
		h.back.seedBreak(id, autoClassification(id), statusAutoResolved, "rule_r")
		h.back.seedOutbox(id, "rule_r")

		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("sweep: got n=%d err=%v, want 1,nil", n, err)
		}
		if st, ok := h.back.outboxStatusOf(id, "rule_r"); !ok || st != store.OutboxConfirmed {
			t.Fatalf("a resolved break's obligation must be confirmed, got %q found=%v", st, ok)
		}
		if h.bnk.deleteCount() != 0 {
			t.Fatalf("a legitimately-clearing rule must NOT be deleted, got %d", h.bnk.deleteCount())
		}
		if h.back.countAction(id, audit.ActionRuleCompensated) != 0 {
			t.Fatal("a confirmed rule must not be audited as compensated")
		}
	})

	t.Run("unresolved break compensates the orphan and audits it", func(t *testing.T) {
		const id = "ext_rec_orphan"
		h := newHarness(t)
		h.back.seedBreak(id, autoClassification(id), statusQueued, "rule_o")
		rowID := h.back.seedOutbox(id, "rule_o")

		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("sweep: got n=%d err=%v, want 1,nil", n, err)
		}
		if h.bnk.deleteCount() != 1 {
			t.Fatalf("the orphaned rule must be deleted, got %d", h.bnk.deleteCount())
		}
		if dels := h.bnk.deletedRules(); len(dels) != 1 || dels[0] != "rule_o" {
			t.Fatalf("the orphan rule id must be the one deleted, got %v", dels)
		}
		if st, ok := h.back.outboxStatusOf(id, "rule_o"); !ok || st != store.OutboxCompensated {
			t.Fatalf("the orphan obligation must be compensated, got %q found=%v", st, ok)
		}
		if h.back.countAction(id, audit.ActionRuleCompensated) != 1 {
			t.Fatalf("the compensation must be audited once, got %d", h.back.countAction(id, audit.ActionRuleCompensated))
		}
		_ = rowID
	})

	t.Run("orphan with no break row is compensated", func(t *testing.T) {
		const id = "ext_rec_nobreak"
		h := newHarness(t)
		h.back.seedOutbox(id, "rule_nb") // pending obligation but no break row

		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("sweep: got n=%d err=%v, want 1,nil", n, err)
		}
		if h.bnk.deleteCount() != 1 {
			t.Fatalf("an obligation with no resolved break must be compensated, got %d deletes", h.bnk.deleteCount())
		}
		if st, ok := h.back.outboxStatusOf(id, "rule_nb"); !ok || st != store.OutboxCompensated {
			t.Fatalf("obligation must be compensated, got %q found=%v", st, ok)
		}
	})

	t.Run("delete failure marks compensation_failed and audits it", func(t *testing.T) {
		const id = "ext_rec_delfail"
		h := newHarness(t)
		h.back.seedBreak(id, autoClassification(id), statusQueued, "rule_df")
		h.back.seedOutbox(id, "rule_df")
		h.bnk.deleteErr = errors.New("blnk delete 500")

		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("sweep: got n=%d err=%v, want 1,nil", n, err)
		}
		if st, ok := h.back.outboxStatusOf(id, "rule_df"); !ok || st != store.OutboxCompensationFailed {
			t.Fatalf("a failed delete must mark compensation_failed, got %q found=%v", st, ok)
		}
		if h.back.countAction(id, audit.ActionRuleCompensated) != 1 {
			t.Fatalf("a failed compensation must still be audited (durable evidence), got %d", h.back.countAction(id, audit.ActionRuleCompensated))
		}
	})

	t.Run("load error leaves the row pending for a later sweep", func(t *testing.T) {
		const id = "ext_rec_loaderr"
		h := newHarness(t)
		h.back.seedOutbox(id, "rule_le")
		h.back.loadErr = errors.New("break load down")

		n, err := h.rem.RecoverPendingRules(context.Background())
		if err != nil || n != 1 {
			t.Fatalf("sweep: got n=%d err=%v, want 1,nil", n, err)
		}
		if h.bnk.deleteCount() != 0 {
			t.Fatalf("a row whose break status is unknown must NOT be deleted, got %d", h.bnk.deleteCount())
		}
		if st, ok := h.back.outboxStatusOf(id, "rule_le"); !ok || st != store.OutboxPending {
			t.Fatalf("the row must stay pending for a later sweep, got %q found=%v", st, ok)
		}
	})

	t.Run("list error surfaces and aborts the sweep", func(t *testing.T) {
		h := newHarness(t)
		h.back.listPendingOutboxErr = errors.New("outbox list down")
		if _, err := h.rem.RecoverPendingRules(context.Background()); err == nil {
			t.Fatal("a list error must surface")
		}
	})
}

// TestHandleM10EscalationSurvivesCanceledContext is the finding M-10 anchor: a
// fail-closed escalation must route the break to HITL even when the parent
// context is ALREADY canceled (the exact condition an LLM-timeout escalation
// hits — its context is past its deadline). escalate() detaches via
// context.WithoutCancel + its own short timeout, so the durable writes (persist
// queued + enqueue HITL + escalated audit) still commit instead of being
// silently dropped. The fake's transaction begin honors context cancellation
// (verified: database/sql BeginTx returns ctx.Err() on a canceled context), so
// this test would FAIL if escalate reused the canceled parent context.
func TestHandleM10EscalationSurvivesCanceledContext(t *testing.T) {
	const id = "ext_m10"
	h := newHarness(t)
	// A regulated break already persisted as classified by a prior step; on resume
	// the gate escalates it (Rule 5.4) without re-classifying — isolating the
	// escalation writes as the only transaction, driven with a canceled parent.
	c := autoClassification(id)
	c.Regulated = true
	h.back.seedBreak(id, c, statusClassified, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // parent context is already canceled

	if err := h.rem.Handle(ctx, txnFor(id), testUploadID); err != nil {
		t.Fatalf("M-10: escalation must succeed despite a canceled parent context, got %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("M-10: break must be durably queued despite cancellation, got %q", status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("M-10: break must be enqueued for HITL despite cancellation")
	}
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("M-10: exactly 1 escalated event must be durable despite cancellation, got %d", got)
	}
}

// TestHandleUnsafeProfileEscalates: an auto-eligible root cause whose transaction
// lacks a field its safe profile REQUIRES (e.g. amount_drift with no reference)
// cannot build a safe rule and must fail closed to HITL BEFORE any Blnk work.
func TestHandleUnsafeProfileEscalates(t *testing.T) {
	const id = "ext_unsafe_profile"
	h := autoHarness(t, id)
	// amount_drift requires a reference; strip it so safeRuleForBreak errors.
	txn := blnk.ExternalTransaction{ID: id, Source: "bank-x", Amount: 10.0, Currency: "USD"}

	if err := h.rem.Handle(context.Background(), txn, testUploadID); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("an unbuildable safe profile must escalate, got %q", status)
	}
	if _, queued := h.back.inHITL(id); !queued {
		t.Fatal("break must be enqueued for HITL")
	}
	if h.bnk.createCount() != 0 || h.bnk.probeCount() != 0 {
		t.Fatalf("no Blnk work may run when no safe profile can be built, got create=%d probe=%d", h.bnk.createCount(), h.bnk.probeCount())
	}
	if h.back.countAction(id, audit.ActionEscalated) != 1 {
		t.Fatalf("expected exactly 1 escalated event, got %d", h.back.countAction(id, audit.ActionEscalated))
	}
}

// TestHandleOutboxRecordFailureCompensates: finding M-11 requires the durable
// outbox write to participate in the SAME atomic transaction as the created-rule
// id and the rule_created audit event. If RecordRuleOutboxTx fails, the whole
// create-tx rolls back and the already-created Blnk rule is compensated
// (deleted + audited) before the break escalates fail-closed — proving the
// outbox is not a best-effort side write.
func TestHandleOutboxRecordFailureCompensates(t *testing.T) {
	const id = "ext_outbox_recfail"
	h := autoHarness(t, id)
	h.back.recordRuleOutboxTxHook = func(_, _ string) error { return errors.New("outbox insert down") }

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("an outbox-record failure must be handled fail-closed, not surfaced: %v", err)
	}
	if h.bnk.createCount() != 1 {
		t.Fatalf("expected exactly 1 Blnk create, got %d", h.bnk.createCount())
	}
	// The create-tx rolled back (no durable created-rule id, no rule_created
	// event, no committed outbox row), and the orphan was compensated + audited.
	if h.back.createdRuleOf(id) != "" {
		t.Fatal("created rule id must NOT be durable after the create-tx rolled back")
	}
	if h.back.countAction(id, audit.ActionRuleCreated) != 0 {
		t.Fatal("no rule_created audit may be durable after rollback")
	}
	if h.back.outboxCount(id) != 0 {
		t.Fatalf("the outbox row was rolled back with the tx, expected 0 rows, got %d", h.back.outboxCount(id))
	}
	if h.bnk.deleteCount() != 1 || h.back.countAction(id, audit.ActionRuleCompensated) != 1 {
		t.Fatalf("orphan must be compensated+audited, got delete=%d compensated=%d", h.bnk.deleteCount(), h.back.countAction(id, audit.ActionRuleCompensated))
	}
	if status, _ := h.back.statusOf(id); status != statusQueued {
		t.Fatalf("break must escalate fail-closed, got %q", status)
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("break must not resolve when the outbox record failed")
	}
}

// TestHandleOutboxConfirmFailureRollsBackResolve: the outbox CONFIRM is committed
// atomically with the resolve write (MarkResolvedTx + resolved audit). If
// ConfirmRuleOutboxTx fails, the entire resolve transaction rolls back so a break
// is never marked resolved without its obligation being confirmed in the same
// commit (findings M-11, C-1; Rule 5.3). The failure surfaces so the caller can
// retry.
func TestHandleOutboxConfirmFailureRollsBackResolve(t *testing.T) {
	const id = "ext_outbox_conffail"
	h := autoHarness(t, id)
	h.back.confirmRuleOutboxTxHook = func(_, _ string) error { return errors.New("outbox confirm down") }

	if err := h.rem.Handle(context.Background(), txnFor(id), testUploadID); err == nil {
		t.Fatal("a resolve-tx confirm failure must surface as an error")
	}
	// Atomicity: neither the resolved status nor the resolved event became durable.
	if status, found := h.back.statusOf(id); found && status == statusAutoResolved {
		t.Fatal("break must NOT be auto-resolved when the resolve transaction rolled back")
	}
	if h.back.countAction(id, audit.ActionResolved) != 0 {
		t.Fatal("no resolved audit may be durable after the resolve transaction rolled back")
	}
}

// -----------------------------------------------------------------------------
// Finding #5 / Rule 5.7 durability: the fail-closed escalation write MUST persist
// even when the pipeline's parent context is already cancelled/expired (e.g. a
// fully hung LLM consumed the entire pipeline budget before the break reached
// this path). escalate() runs its persistence on a context DETACHED from the
// parent (context.WithoutCancel) so the break is durably queued + audited
// regardless. These tests reproduce that fault injection through escalate()
// directly and through the public Handle() fail-closed path.
// -----------------------------------------------------------------------------

func deadContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func TestEscalatePersistsUnderCancelledParentContext(t *testing.T) {
	const id = "ext_hung_llm"
	h := newHarness(t)

	// A regulated classification is a representative fail-closed escalation.
	cls := model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     model.RootCauseCurrencyMismatch,
		Confidence:    0.90,
		Regulated:     true,
		Rationale:     "regulated cross-border flow; must be reviewed by a human",
	}
	prov := model.Provenance{UploadID: testUploadID, Source: "bank-x"}

	// The parent context is already dead — as it would be after a hung LLM
	// consumed the whole pipeline budget.
	if err := h.rem.escalate(deadContext(), txnFor(id), cls, prov, "regulated break requires human review"); err != nil {
		t.Fatalf("escalate under a cancelled parent context must still persist the break, got error: %v", err)
	}

	// The break must be durably queued to HITL (never dropped, Rule 5.7).
	if reason, ok := h.back.inHITL(id); !ok {
		t.Fatalf("break %q was DROPPED — no HITL queue entry after escalate under a dead context", id)
	} else if reason == "" {
		t.Fatalf("break %q enqueued to HITL with an empty reason", id)
	}

	// The break must be persisted in the queued state.
	if status, ok := h.back.statusOf(id); !ok || status != statusQueued {
		t.Fatalf("break %q status = %q found=%v, want %q", id, status, ok, statusQueued)
	}

	// Exactly one append-only escalated audit event must have been recorded
	// (100%-of-actions-emit-an-AuditEvent, even on the fail-closed path).
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("expected exactly 1 %q audit event for a break escalated under a dead context, got %d", audit.ActionEscalated, got)
	}
}
func TestHandleFailClosedIsDurableUnderCancelledContext(t *testing.T) {
	const id = "ext_hung_llm_e2e"
	h := newHarness(t)
	// The classifier failed (as a hung endpoint does after the retry cap); the
	// returned stub carries no usable classification.
	h.cls.result = model.BreakClassification{ExternalTxnID: id, RootCause: model.RootCauseUnknown}
	h.cls.err = fmt.Errorf("classify: %w", classifier.ErrClassificationFailed)

	if err := h.rem.Handle(deadContext(), txnFor(id), testUploadID); err != nil {
		t.Fatalf("Handle must durably fail-close (escalate) a break whose classifier failed even under a cancelled context, got error: %v", err)
	}

	if reason, ok := h.back.inHITL(id); !ok {
		t.Fatalf("break %q was DROPPED under a cancelled context — no HITL entry (Rule 5.7 violated)", id)
	} else if reason == "" {
		t.Fatalf("break %q enqueued to HITL with an empty reason", id)
	}
	if got := h.back.countAction(id, audit.ActionEscalated); got != 1 {
		t.Fatalf("expected exactly 1 %q audit event for the fail-closed break, got %d", audit.ActionEscalated, got)
	}
	// A fail-closed break must NEVER be auto-resolved (Rule 5.7).
	if got := h.back.countAction(id, audit.ActionResolved); got != 0 {
		t.Fatalf("fail-closed break %q must have 0 %q audit events, got %d", id, audit.ActionResolved, got)
	}
}
