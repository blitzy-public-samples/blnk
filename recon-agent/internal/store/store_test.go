package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/recon-agent/internal/audit"
	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// Compile-time proof that *Store satisfies both the audit sink (InsertAudit)
// and the transaction-bound audit.TxSink (InsertAuditTx). The latter is what
// makes audit.Writer.RecordTx live instead of returning ErrTxSinkUnsupported,
// closing the dead-code path that findings C-1/C-2/M-2/M-4 depend on.
var _ audit.TxSink = (*Store)(nil)

// -----------------------------------------------------------------------------
// Hand-rolled database/sql/driver fake.
//
// The recon-agent go.mod pins EXACTLY five dependencies and does NOT include a
// SQL-mock library, so these tests may not import one. Instead they implement a
// minimal in-process driver on top of the stdlib database/sql/driver contract.
// The fake records every statement (query text + args) the store issues and
// returns programmable results/rows/errors, letting the tests assert both the
// SQL surface (Rule 5.1 agent-only, Rule 5.5 audit append-only) and the
// scan/round-trip behaviour without any external PostgreSQL.
// -----------------------------------------------------------------------------

// recordedCall captures one statement executed against the fake driver.
type recordedCall struct {
	query string
	args  []driver.NamedValue
}

// fakeDB is the programmable state shared by every connection the fake hands
// out. Tests set the responses the next call should yield and inspect the
// recorded calls afterwards.
type fakeDB struct {
	execs   []recordedCall
	queries []recordedCall

	execResult driver.Result
	execErr    error
	// execHook, when non-nil, is consulted on every Exec (autocommit or
	// transaction-scoped). Returning a non-nil error fails that specific
	// statement, letting a test inject a fault at a chosen point in a WithTx
	// sequence (e.g. let the status UPDATE succeed but fail the audit INSERT) to
	// prove the whole transaction rolls back and nothing partial is committed.
	execHook func(query string) error

	cols     []string
	rows     [][]driver.Value
	queryErr error

	// Transaction instrumentation. WithTx exercises BeginTx/Commit/Rollback;
	// these fields let tests inject failures and count what actually happened.
	beginErr  error
	commitErr error
	begins    int
	commits   int
	rollbacks int
}

// execQueries returns just the SQL text of every recorded Exec call.
func (f *fakeDB) execQueries() []string {
	out := make([]string, len(f.execs))
	for i, c := range f.execs {
		out[i] = c.query
	}
	return out
}

type fakeConnector struct{ db *fakeDB }

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{db: c.db}, nil
}
func (c *fakeConnector) Driver() driver.Driver { return &fakeDriver{db: c.db} }

type fakeDriver struct{ db *fakeDB }

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{db: d.db}, nil }

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported by fake")
}
func (c *fakeConn) Close() error { return nil }

// Begin delegates to BeginTx so both the legacy and context-aware transaction
// entry points share one code path. database/sql invokes BeginTx (this conn
// implements driver.ConnBeginTx), so Begin is retained only for completeness.
func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx so *sql.DB.BeginTx (used by store.WithTx)
// hands back a fakeTx whose Commit/Rollback are counted and can be made to fail.
func (c *fakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.db.begins++
	if c.db.beginErr != nil {
		return nil, c.db.beginErr
	}
	return &fakeTx{db: c.db}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.db.execs = append(c.db.execs, recordedCall{query: query, args: args})
	if c.db.execHook != nil {
		if err := c.db.execHook(query); err != nil {
			return nil, err
		}
	}
	if c.db.execErr != nil {
		return nil, c.db.execErr
	}
	if c.db.execResult != nil {
		return c.db.execResult, nil
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.db.queries = append(c.db.queries, recordedCall{query: query, args: args})
	if c.db.queryErr != nil {
		return nil, c.db.queryErr
	}
	return &fakeRows{cols: c.db.cols, data: c.db.rows}, nil
}

// fakeTx is the fake driver's transaction. It records whether the store
// committed or rolled back so atomicity tests can assert the outcome, and can
// be made to fail its Commit to exercise WithTx's commit-error path.
type fakeTx struct{ db *fakeDB }

func (t *fakeTx) Commit() error {
	t.db.commits++
	if t.db.commitErr != nil {
		return t.db.commitErr
	}
	return nil
}

func (t *fakeTx) Rollback() error {
	t.db.rollbacks++
	return nil
}

type fakeRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

// errResult is a driver.Result whose RowsAffected always fails, used to cover
// SetBreakStatus's RowsAffected error branch.
type errResult struct{}

func (errResult) LastInsertId() (int64, error) { return 0, errors.New("no last insert id") }
func (errResult) RowsAffected() (int64, error) { return 0, errors.New("rows affected failure") }

// newTestStore builds a Store backed by the fake driver and returns the shared
// state so the test can program responses and inspect recorded calls.
func newTestStore() (*Store, *fakeDB) {
	fdb := &fakeDB{}
	return &Store{db: sql.OpenDB(&fakeConnector{db: fdb})}, fdb
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// -----------------------------------------------------------------------------
// New / Close
// -----------------------------------------------------------------------------

func TestNewEmptyDSN(t *testing.T) {
	for _, dsn := range []string{"", "   ", "\t\n"} {
		if _, err := New(dsn); err == nil {
			t.Fatalf("New(%q): expected error for empty DSN", dsn)
		}
	}
}

func TestNewOpensLazily(t *testing.T) {
	s, err := New("postgres://user:pass@localhost:5432/blnk?sslmode=disable")
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	if s == nil || s.db == nil {
		t.Fatal("New returned nil store/db")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseNilDB(t *testing.T) {
	s := &Store{}
	if err := s.Close(); err != nil {
		t.Fatalf("Close on nil db: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Migrate / upSection
// -----------------------------------------------------------------------------

func TestMigrateExecutesUpSectionOnly(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// M2: Migrate runs inside ONE transaction guarded by a Postgres advisory
	// lock, so it issues exactly two execs — the advisory-lock acquisition, then
	// the DDL — and commits exactly once.
	if len(fdb.execs) != 2 {
		t.Fatalf("expected exactly 2 execs (advisory lock + DDL), got %d", len(fdb.execs))
	}
	if !strings.Contains(fdb.execs[0].query, "pg_advisory_xact_lock") {
		t.Fatalf("first exec must acquire the migration advisory lock, got:\n%s", fdb.execs[0].query)
	}
	if fdb.begins != 1 || fdb.commits != 1 || fdb.rollbacks != 0 {
		t.Fatalf("Migrate must run in exactly one committed transaction: begins=%d commits=%d rollbacks=%d", fdb.begins, fdb.commits, fdb.rollbacks)
	}
	up := fdb.execs[1].query
	for _, want := range []string{
		"CREATE SCHEMA IF NOT EXISTS agent",
		"agent.agent_break",
		"agent.agent_audit",
		"agent.agent_hitl_queue",
	} {
		if !strings.Contains(up, want) {
			t.Fatalf("migration missing %q:\n%s", want, up)
		}
	}
	// Up-only: the destructive Down section must never be executed.
	if strings.Contains(up, "DROP ") {
		t.Fatalf("migration must not contain DROP (Up-only):\n%s", up)
	}
	// Rule 5.1: no blnk.* object anywhere in the migration.
	if strings.Contains(strings.ToLower(up), "blnk.") {
		t.Fatalf("migration must not reference blnk.*:\n%s", up)
	}
}

func TestMigrateExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.Migrate(context.Background()); err == nil {
		t.Fatal("expected Migrate error when exec fails")
	}
}

func TestUpSection(t *testing.T) {
	// Whole-line matching: markers mentioned in prose must be ignored.
	in := strings.Join([]string{
		"-- talks about -- +migrate Up in prose",
		"-- +migrate Up",
		"CREATE SCHEMA IF NOT EXISTS agent;",
		"-- +migrate Down",
		"DROP SCHEMA agent;",
	}, "\n")
	got := upSection(in)
	if !strings.Contains(got, "CREATE SCHEMA IF NOT EXISTS agent;") {
		t.Fatalf("upSection missing Up body: %q", got)
	}
	if strings.Contains(got, "DROP SCHEMA") {
		t.Fatalf("upSection leaked Down body: %q", got)
	}
	if strings.Contains(got, "in prose") {
		t.Fatalf("upSection captured pre-marker prose: %q", got)
	}
	if s := upSection("no markers here"); s != "" {
		t.Fatalf("upSection without markers must be empty, got %q", s)
	}
}

// -----------------------------------------------------------------------------
// Breaks
// -----------------------------------------------------------------------------

func sampleRule() *blnk.MatchingRule {
	return &blnk.MatchingRule{
		Name:        "amount-eq",
		Description: "match on amount",
		Criteria: []blnk.MatchingCriteria{
			{Field: "amount", Operator: "equals", Value: "10.00"},
		},
	}
}

func TestUpsertBreakWithRule(t *testing.T) {
	s, fdb := newTestStore()
	c := model.BreakClassification{
		ExternalTxnID: "ext_1",
		RootCause:     model.RootCauseAmountDrift,
		Confidence:    0.91,
		Regulated:     false,
		ProposedRule:  sampleRule(),
		Rationale:     "amount drifted by fees",
	}
	txn := blnk.ExternalTransaction{
		ID:          "ext_1",
		Amount:      250.75,
		Currency:    "USD",
		Reference:   "INV-1002",
		Description: "card settlement",
		Date:        time.Date(2024, 5, 2, 9, 30, 0, 0, time.UTC),
	}
	if err := s.UpsertBreak(context.Background(), c, txn, "classified"); err != nil {
		t.Fatalf("UpsertBreak: %v", err)
	}
	if len(fdb.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(fdb.execs))
	}
	q := fdb.execs[0].query
	if !strings.Contains(q, "INSERT INTO agent.agent_break") || !strings.Contains(q, "ON CONFLICT (external_txn_id) DO UPDATE") {
		t.Fatalf("unexpected upsert SQL:\n%s", q)
	}
	args := fdb.execs[0].args
	// finding L2: 7 verdict args + 5 matchable-txn args (amount/currency/
	// reference/description/txn_date) = 12.
	if len(args) != 12 {
		t.Fatalf("expected 12 args, got %d", len(args))
	}
	if args[0].Value != "ext_1" || args[1].Value != "amount_drift" || args[2].Value != 0.91 || args[3].Value != false || args[4].Value != "classified" {
		t.Fatalf("unexpected args: %+v", args)
	}
	ruleArg, ok := args[5].Value.(string)
	if !ok || !strings.Contains(ruleArg, "\"field\":\"amount\"") {
		t.Fatalf("proposed_rule arg should be JSON string with the rule, got %#v", args[5].Value)
	}
	// finding m-1: the classifier's rationale is persisted (arg 7).
	if args[6].Value != "amount drifted by fees" {
		t.Fatalf("rationale arg should round-trip, got %#v", args[6].Value)
	}
	// finding L2: the matchable transaction fields are persisted (args 8-12) so
	// a queued break can be re-driven without reading any blnk.* table.
	if args[7].Value != 250.75 || args[8].Value != "USD" || args[9].Value != "INV-1002" || args[10].Value != "card settlement" {
		t.Fatalf("txn fields not persisted: amount=%#v currency=%#v reference=%#v description=%#v",
			args[7].Value, args[8].Value, args[9].Value, args[10].Value)
	}
	if _, ok := args[11].Value.(time.Time); !ok {
		t.Fatalf("txn_date should round-trip as time.Time, got %#v", args[11].Value)
	}
}

func TestUpsertBreakNilRuleStoresNull(t *testing.T) {
	s, fdb := newTestStore()
	c := model.BreakClassification{
		ExternalTxnID: "ext_2",
		RootCause:     model.RootCauseTiming,
		Confidence:    0.4,
	}
	if err := s.UpsertBreak(context.Background(), c, blnk.ExternalTransaction{ID: "ext_2"}, "queued"); err != nil {
		t.Fatalf("UpsertBreak: %v", err)
	}
	if got := fdb.execs[0].args[5].Value; got != nil {
		t.Fatalf("nil ProposedRule must map to SQL NULL, got %#v", got)
	}
	// A transaction with no date must map txn_date (arg 12) to SQL NULL so the
	// column faithfully round-trips a zero date.
	if got := fdb.execs[0].args[11].Value; got != nil {
		t.Fatalf("zero txn date must map to SQL NULL, got %#v", got)
	}
}

func TestUpsertBreakExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("db down")
	if err := s.UpsertBreak(context.Background(), model.BreakClassification{ExternalTxnID: "x"}, blnk.ExternalTransaction{ID: "x"}, "classified"); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestSetBreakStatusOK(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.SetBreakStatus(context.Background(), "ext_1", "auto-resolved"); err != nil {
		t.Fatalf("SetBreakStatus: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.HasPrefix(q, "UPDATE agent.agent_break SET status") {
		t.Fatalf("unexpected update SQL:\n%s", q)
	}
	if fdb.execs[0].args[0].Value != "ext_1" || fdb.execs[0].args[1].Value != "auto-resolved" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

func TestSetBreakStatusNotFound(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = driver.RowsAffected(0)
	err := s.SetBreakStatus(context.Background(), "missing", "rejected")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSetBreakStatusExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.SetBreakStatus(context.Background(), "x", "y"); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestSetBreakStatusRowsAffectedError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = errResult{}
	err := s.SetBreakStatus(context.Background(), "x", "y")
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("expected RowsAffected error (not ErrNotFound), got %v", err)
	}
}

func TestListBreaks(t *testing.T) {
	s, fdb := newTestStore()
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id", "created_at", "updated_at"}
	fdb.rows = [][]driver.Value{
		{"ext_1", "amount_drift", 0.91, false, "auto-resolved", mustMarshal(t, sampleRule()), "amount drifted", "rule_abc", now, now},
		{"ext_2", "timing", 0.42, true, "queued", nil, "", nil, now, now},
	}
	breaks, err := s.ListBreaks(context.Background())
	if err != nil {
		t.Fatalf("ListBreaks: %v", err)
	}
	if len(breaks) != 2 {
		t.Fatalf("expected 2 breaks, got %d", len(breaks))
	}
	if breaks[0].Classification.ExternalTxnID != "ext_1" || breaks[0].Classification.RootCause != model.RootCauseAmountDrift {
		t.Fatalf("break0 mismatch: %+v", breaks[0])
	}
	if breaks[0].Classification.ProposedRule == nil || breaks[0].Classification.ProposedRule.Criteria[0].Field != "amount" {
		t.Fatalf("break0 rule not unmarshaled: %+v", breaks[0].Classification.ProposedRule)
	}
	// finding m-1 + M-3: rationale and created_rule_id round-trip out of storage.
	if breaks[0].Classification.Rationale != "amount drifted" {
		t.Fatalf("break0 rationale mismatch: %q", breaks[0].Classification.Rationale)
	}
	if breaks[0].CreatedRuleID != "rule_abc" {
		t.Fatalf("break0 created_rule_id mismatch: %q", breaks[0].CreatedRuleID)
	}
	if !breaks[0].CreatedAt.Equal(now) {
		t.Fatalf("break0 created_at mismatch: %v", breaks[0].CreatedAt)
	}
	if breaks[1].Classification.ProposedRule != nil {
		t.Fatalf("break1 should have nil rule, got %+v", breaks[1].Classification.ProposedRule)
	}
	if breaks[1].CreatedRuleID != "" {
		t.Fatalf("break1 created_rule_id should be empty (NULL), got %q", breaks[1].CreatedRuleID)
	}
	if !breaks[1].Classification.Regulated {
		t.Fatal("break1 should be regulated")
	}
}

func TestListBreaksQueryError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.queryErr = errors.New("query fail")
	if _, err := s.ListBreaks(context.Background()); err == nil {
		t.Fatal("expected query error")
	}
}

func TestListBreaksScanError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id", "created_at", "updated_at"}
	// confidence is a non-numeric string => Scan into *float64 fails.
	fdb.rows = [][]driver.Value{
		{"ext_1", "amount_drift", "not-a-float", false, "queued", nil, "", nil, time.Now(), time.Now()},
	}
	if _, err := s.ListBreaks(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
}

// TestListBreaksBadRuleJSON asserts finding m-5's per-row isolation: a single
// row whose proposed_rule column is not valid JSON leaves that row's
// ProposedRule nil but does NOT fail the whole listing, so every healthy row
// (and the HITL status page) stays readable.
func TestListBreaksBadRuleJSON(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id", "created_at", "updated_at"}
	fdb.rows = [][]driver.Value{
		{"ext_bad", "amount_drift", 0.9, false, "queued", []byte("{not-json"), "", nil, time.Now(), time.Now()},
		{"ext_ok", "timing", 0.5, false, "classified", mustMarshal(t, sampleRule()), "", nil, time.Now(), time.Now()},
	}
	breaks, err := s.ListBreaks(context.Background())
	if err != nil {
		t.Fatalf("ListBreaks must not fail on one bad rule row (per-row isolation): %v", err)
	}
	if len(breaks) != 2 {
		t.Fatalf("expected 2 breaks, got %d", len(breaks))
	}
	if breaks[0].Classification.ExternalTxnID != "ext_bad" || breaks[0].Classification.ProposedRule != nil {
		t.Fatalf("bad-rule row must have nil ProposedRule, got %+v", breaks[0].Classification.ProposedRule)
	}
	if breaks[1].Classification.ProposedRule == nil || breaks[1].Classification.ProposedRule.Criteria[0].Field != "amount" {
		t.Fatalf("healthy row must still parse its rule, got %+v", breaks[1].Classification.ProposedRule)
	}
}

// -----------------------------------------------------------------------------
// HITL queue
// -----------------------------------------------------------------------------

func TestEnqueueHITL(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.EnqueueHITL(context.Background(), "ext_9", "regulated flow"); err != nil {
		t.Fatalf("EnqueueHITL: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.Contains(q, "INSERT INTO agent.agent_hitl_queue") {
		t.Fatalf("unexpected enqueue SQL:\n%s", q)
	}
	if fdb.execs[0].args[0].Value != "ext_9" || fdb.execs[0].args[1].Value != "regulated flow" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

func TestEnqueueHITLExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.EnqueueHITL(context.Background(), "x", "y"); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestListHITL(t *testing.T) {
	s, fdb := newTestStore()
	now := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	fdb.cols = []string{"external_txn_id", "reason", "enqueued_at"}
	fdb.rows = [][]driver.Value{
		{"ext_1", "low confidence", now},
		{"ext_2", "regulated", now},
	}
	items, err := s.ListHITL(context.Background())
	if err != nil {
		t.Fatalf("ListHITL: %v", err)
	}
	if len(items) != 2 || items[0].ExternalTxnID != "ext_1" || items[1].Reason != "regulated" {
		t.Fatalf("unexpected items: %+v", items)
	}
	if !items[0].EnqueuedAt.Equal(now) {
		t.Fatalf("enqueued_at mismatch: %v", items[0].EnqueuedAt)
	}
}

func TestListHITLQueryError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.queryErr = errors.New("fail")
	if _, err := s.ListHITL(context.Background()); err == nil {
		t.Fatal("expected query error")
	}
}

func TestListHITLScanError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"external_txn_id", "reason", "enqueued_at"}
	// enqueued_at as an int64 cannot scan into time.Time.
	fdb.rows = [][]driver.Value{{"ext_1", "reason", int64(123)}}
	if _, err := s.ListHITL(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
}

func TestDequeueHITL(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.DequeueHITL(context.Background(), "ext_1"); err != nil {
		t.Fatalf("DequeueHITL: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.HasPrefix(q, "DELETE FROM agent.agent_hitl_queue") {
		t.Fatalf("unexpected dequeue SQL:\n%s", q)
	}
	if fdb.execs[0].args[0].Value != "ext_1" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

func TestDequeueHITLExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.DequeueHITL(context.Background(), "x"); err == nil {
		t.Fatal("expected exec error")
	}
}

// -----------------------------------------------------------------------------
// Queued-only compare-and-set (finding M-01) + transactional dequeue (M-02).
// -----------------------------------------------------------------------------

// TestSetBreakStatusFromQueuedTxOK proves the guard transitions a currently
// queued break (one row affected) and issues a status='queued'-guarded UPDATE.
func TestSetBreakStatusFromQueuedTxOK(t *testing.T) {
	s, fdb := newTestStore() // default execResult => RowsAffected 1
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(context.Background(), tx, "ext_1", "accepted")
	})
	if err != nil {
		t.Fatalf("SetBreakStatusFromQueuedTx: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.Contains(q, "status = 'queued'") {
		t.Fatalf("CAS must guard on the queued state:\n%s", q)
	}
	if fdb.execs[0].args[0].Value != "ext_1" || fdb.execs[0].args[1].Value != "accepted" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
	if fdb.commits != 1 || fdb.rollbacks != 0 {
		t.Fatalf("a successful guarded update must commit: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
}

// TestSetBreakStatusFromQueuedTxNotQueued proves that when no queued row is
// transitioned but the break exists in another (terminal) state, the guard
// returns ErrNotQueued so the caller can answer 409 — never a silent overwrite.
func TestSetBreakStatusFromQueuedTxNotQueued(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = driver.RowsAffected(0)   // no queued row transitioned
	fdb.cols = []string{"status"}             // the follow-up existence read
	fdb.rows = [][]driver.Value{{"accepted"}} // break exists, already decided
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(context.Background(), tx, "ext_1", "accepted")
	})
	if !errors.Is(err, ErrNotQueued) {
		t.Fatalf("expected ErrNotQueued, got %v", err)
	}
}

// TestSetBreakStatusFromQueuedTxNotFound proves that when no row is affected and
// the break does not exist at all, the guard returns ErrNotFound (404).
func TestSetBreakStatusFromQueuedTxNotFound(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = driver.RowsAffected(0)
	fdb.cols = []string{"status"}
	fdb.rows = nil // no row => sql.ErrNoRows => ErrNotFound
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(context.Background(), tx, "missing", "accepted")
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestSetBreakStatusFromQueuedTxExecError proves a raw UPDATE failure surfaces
// as-is (neither ErrNotQueued nor ErrNotFound).
func TestSetBreakStatusFromQueuedTxExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(context.Background(), tx, "x", "accepted")
	})
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotQueued) {
		t.Fatalf("expected raw exec error, got %v", err)
	}
}

// TestDequeueHITLTx proves the transactional dequeue issues the agent-schema
// DELETE inside the caller's transaction (finding M-02).
func TestDequeueHITLTx(t *testing.T) {
	s, fdb := newTestStore()
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.DequeueHITLTx(context.Background(), tx, "ext_1")
	})
	if err != nil {
		t.Fatalf("DequeueHITLTx: %v", err)
	}
	if !strings.HasPrefix(fdb.execs[0].query, "DELETE FROM agent.agent_hitl_queue") {
		t.Fatalf("unexpected dequeue SQL:\n%s", fdb.execs[0].query)
	}
	if fdb.execs[0].args[0].Value != "ext_1" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

// -----------------------------------------------------------------------------
// Audit (append-only, Rule 5.5)
// -----------------------------------------------------------------------------

func sampleAudit() model.AuditEvent {
	return model.AuditEvent{
		EventID:       "11111111-1111-1111-1111-111111111111",
		ExternalTxnID: "ext_1",
		Actor:         "agent",
		Action:        "resolved",
		Timestamp:     "2024-01-02T03:04:05Z",
		Rationale:     "dry-run cleared the break",
		Confidence:    0.93,
		Provenance:    model.Provenance{Model: "kimi-k3", ReconID: "recon_1", UploadID: "up_1", Source: "bank-x"},
	}
}

func TestInsertAudit(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.InsertAudit(context.Background(), sampleAudit()); err != nil {
		t.Fatalf("InsertAudit: %v", err)
	}
	if len(fdb.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(fdb.execs))
	}
	q := fdb.execs[0].query
	if !strings.HasPrefix(q, "INSERT INTO agent.agent_audit") {
		t.Fatalf("audit write must be an INSERT:\n%s", q)
	}
	upper := strings.ToUpper(q)
	if strings.Contains(upper, "UPDATE") || strings.Contains(upper, "DELETE") {
		t.Fatalf("Rule 5.5: audit statement must not UPDATE/DELETE:\n%s", q)
	}
	args := fdb.execs[0].args
	if len(args) != 8 {
		t.Fatalf("expected 8 args, got %d", len(args))
	}
	if args[0].Value != "11111111-1111-1111-1111-111111111111" || args[3].Value != "resolved" {
		t.Fatalf("unexpected audit args: %+v", args)
	}
	prov, ok := args[7].Value.(string)
	if !ok || !strings.Contains(prov, "\"recon_id\":\"recon_1\"") {
		t.Fatalf("provenance arg should be JSON string, got %#v", args[7].Value)
	}
}

func TestInsertAuditExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.InsertAudit(context.Background(), sampleAudit()); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestListAudit(t *testing.T) {
	s, fdb := newTestStore()
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	fdb.cols = []string{"event_id", "external_txn_id", "actor", "action", "timestamp", "rationale", "confidence", "provenance"}
	fdb.rows = [][]driver.Value{
		{"ev1", "ext_1", "agent", "classified", ts, "why", 0.5, mustMarshal(t, model.Provenance{Model: "kimi-k3", Source: "bank-x"})},
	}
	events, err := s.ListAudit(context.Background())
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventID != "ev1" || events[0].Action != "classified" {
		t.Fatalf("unexpected event: %+v", events[0])
	}
	if events[0].Timestamp != "2024-01-02T03:04:05Z" {
		t.Fatalf("timestamp not RFC3339: %q", events[0].Timestamp)
	}
	if events[0].Provenance.Model != "kimi-k3" || events[0].Provenance.Source != "bank-x" {
		t.Fatalf("provenance not unmarshaled: %+v", events[0].Provenance)
	}
}

func TestListAuditQueryError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.queryErr = errors.New("fail")
	if _, err := s.ListAudit(context.Background()); err == nil {
		t.Fatal("expected query error")
	}
}

func TestListAuditScanError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"event_id", "external_txn_id", "actor", "action", "timestamp", "rationale", "confidence", "provenance"}
	// timestamp as a string cannot scan into time.Time.
	fdb.rows = [][]driver.Value{
		{"ev1", "ext_1", "agent", "classified", "not-a-time", "why", 0.5, []byte("{}")},
	}
	if _, err := s.ListAudit(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
}

func TestListAuditBadProvenanceJSON(t *testing.T) {
	s, fdb := newTestStore()
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	fdb.cols = []string{"event_id", "external_txn_id", "actor", "action", "timestamp", "rationale", "confidence", "provenance"}
	fdb.rows = [][]driver.Value{
		{"ev1", "ext_1", "agent", "classified", ts, "why", 0.5, []byte("{bad")},
	}
	if _, err := s.ListAudit(context.Background()); err == nil {
		t.Fatal("expected provenance unmarshal error")
	}
}

// CountAuditByAction returns the number of audit events for a break+action and
// must do so with a READ-ONLY SELECT COUNT scoped by both parameters (Rule 5.5:
// SELECT is permitted on agent_audit; UPDATE/DELETE are not). Backs the
// remediator's SEAM-MIN-1 fail-closed resume guard.
func TestCountAuditByAction(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"count"}
	fdb.rows = [][]driver.Value{{int64(3)}}

	n, err := s.CountAuditByAction(context.Background(), "ext_1", audit.ActionRuleProposed)
	if err != nil {
		t.Fatalf("CountAuditByAction: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected count 3, got %d", n)
	}
	if len(fdb.queries) != 1 {
		t.Fatalf("expected exactly one query, got %d", len(fdb.queries))
	}
	q := fdb.queries[0].query
	if !strings.Contains(q, "count(*)") || !strings.Contains(q, "agent.agent_audit") ||
		!strings.Contains(q, "external_txn_id = $1") || !strings.Contains(q, "action = $2") {
		t.Fatalf("unexpected count query: %q", q)
	}
	if up := strings.ToUpper(q); strings.Contains(up, "UPDATE") || strings.Contains(up, "DELETE") {
		t.Fatalf("CountAuditByAction must be read-only, got: %q", q)
	}
	args := fdb.queries[0].args
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d: %+v", len(args), args)
	}
	if got, _ := args[0].Value.(string); got != "ext_1" {
		t.Fatalf("arg0 = %v, want external txn id ext_1", args[0].Value)
	}
	if got, _ := args[1].Value.(string); got != audit.ActionRuleProposed {
		t.Fatalf("arg1 = %v, want action %q", args[1].Value, audit.ActionRuleProposed)
	}
}

func TestCountAuditByActionQueryError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.queryErr = errors.New("count fail")
	if _, err := s.CountAuditByAction(context.Background(), "ext_1", audit.ActionRuleProposed); err == nil {
		t.Fatal("expected query error")
	}
}

// -----------------------------------------------------------------------------
// Rule 5.1 + Rule 5.5 whole-surface assertions
// -----------------------------------------------------------------------------

// TestSQLSurfaceRules drives every write path once and asserts the aggregate
// SQL surface honours the agent-only (Rule 5.1) and audit-append-only (Rule
// 5.5) constraints.
func TestSQLSurfaceRules(t *testing.T) {
	s, fdb := newTestStore()
	ctx := context.Background()

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.UpsertBreak(ctx, model.BreakClassification{ExternalTxnID: "ext_1", RootCause: model.RootCauseTiming, ProposedRule: sampleRule()}, blnk.ExternalTransaction{ID: "ext_1", Amount: 100, Currency: "USD"}, "classified"); err != nil {
		t.Fatalf("UpsertBreak: %v", err)
	}
	if err := s.SetBreakStatus(ctx, "ext_1", "auto-resolved"); err != nil {
		t.Fatalf("SetBreakStatus: %v", err)
	}
	if err := s.EnqueueHITL(ctx, "ext_2", "regulated"); err != nil {
		t.Fatalf("EnqueueHITL: %v", err)
	}
	if err := s.DequeueHITL(ctx, "ext_2"); err != nil {
		t.Fatalf("DequeueHITL: %v", err)
	}
	if err := s.InsertAudit(ctx, sampleAudit()); err != nil {
		t.Fatalf("InsertAudit: %v", err)
	}
	// reads (empty result sets)
	if _, err := s.ListBreaks(ctx); err != nil {
		t.Fatalf("ListBreaks: %v", err)
	}
	if _, err := s.ListHITL(ctx); err != nil {
		t.Fatalf("ListHITL: %v", err)
	}
	if _, err := s.ListAudit(ctx); err != nil {
		t.Fatalf("ListAudit: %v", err)
	}

	all := append(append([]recordedCall{}, fdb.execs...), fdb.queries...)
	if len(all) == 0 {
		t.Fatal("no statements recorded")
	}
	for _, c := range all {
		// Strip SQL line comments first: schema.sql documents Rule 5.5 in a
		// comment that literally names UPDATE/DELETE, which must not be mistaken
		// for real DML.
		lower := strings.ToLower(stripSQLComments(c.query))
		// Rule 5.1: never touch a blnk.* object.
		if strings.Contains(lower, "blnk.") {
			t.Fatalf("Rule 5.1 violation: statement references blnk.*:\n%s", c.query)
		}
		// Rule 5.5: no UPDATE/DELETE may target agent_audit. Match the specific
		// DML forms (UPDATE <table> / DELETE FROM <table>) rather than the bare
		// keywords, so the append-only ENFORCEMENT trigger — whose definition
		// legitimately reads "BEFORE UPDATE OR DELETE ON agent.agent_audit" — is
		// not itself misread as a mutation of the table.
		if strings.Contains(lower, "update agent.agent_audit") ||
			strings.Contains(lower, "delete from agent.agent_audit") {
			t.Fatalf("Rule 5.5 violation: mutating statement targets agent_audit:\n%s", c.query)
		}
	}

	// Positive check: the only statement mentioning agent_audit as a write is
	// the INSERT (plus the CREATE TABLE inside Migrate); there is exactly one
	// INSERT into agent_audit.
	var auditInserts int
	for _, q := range fdb.execQueries() {
		if strings.HasPrefix(q, "INSERT INTO agent.agent_audit") {
			auditInserts++
		}
	}
	if auditInserts != 1 {
		t.Fatalf("expected exactly 1 audit INSERT, got %d", auditInserts)
	}
}

// stripSQLComments removes whole-line SQL comments ("-- ...") so that keywords
// documented in prose (e.g. schema.sql's Rule 5.5 note) are not mistaken for
// executable DML by the SQL-surface assertions.
func stripSQLComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// -----------------------------------------------------------------------------
// Transaction API (WithTx + *Tx write methods)
//
// These cover the atomicity primitive the remediator uses so a break's state
// transition and its audit event commit-or-roll-back together (findings C-1,
// C-2, M-2, M-4 and Rule 5.3).
// -----------------------------------------------------------------------------

func TestWithTxCommitsOnSuccess(t *testing.T) {
	s, fdb := newTestStore()
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := s.UpsertBreakTx(context.Background(), tx, model.BreakClassification{ExternalTxnID: "ext_1", RootCause: model.RootCauseTiming}, blnk.ExternalTransaction{ID: "ext_1"}, "classified"); err != nil {
			return err
		}
		return s.InsertAuditTx(context.Background(), tx, sampleAudit())
	})
	if err != nil {
		t.Fatalf("WithTx: %v", err)
	}
	if fdb.begins != 1 {
		t.Fatalf("expected 1 begin, got %d", fdb.begins)
	}
	if fdb.commits != 1 || fdb.rollbacks != 0 {
		t.Fatalf("success must commit exactly once and never roll back: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
	// Both writes were recorded and both are agent-schema statements.
	if len(fdb.execs) != 2 {
		t.Fatalf("expected 2 tx execs, got %d", len(fdb.execs))
	}
	if !strings.HasPrefix(fdb.execs[0].query, "INSERT INTO agent.agent_break") {
		t.Fatalf("first tx exec should be the break upsert:\n%s", fdb.execs[0].query)
	}
	if !strings.HasPrefix(fdb.execs[1].query, "INSERT INTO agent.agent_audit") {
		t.Fatalf("second tx exec should be the audit insert:\n%s", fdb.execs[1].query)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	s, fdb := newTestStore()
	sentinel := errors.New("fn failed")
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		// A write happens, then fn returns an error: the whole tx must roll back.
		if e := s.SetBreakStatusTx(context.Background(), tx, "ext_1", "auto-resolved"); e != nil {
			return e
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx must surface fn's error, got %v", err)
	}
	if fdb.commits != 0 || fdb.rollbacks != 1 {
		t.Fatalf("fn error must roll back and never commit: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
}

func TestWithTxBeginError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.beginErr = errors.New("cannot begin")
	called := false
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected begin error")
	}
	if called {
		t.Fatal("fn must not be called when BeginTx fails")
	}
	if fdb.commits != 0 || fdb.rollbacks != 0 {
		t.Fatalf("no commit/rollback expected on begin failure: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
}

func TestWithTxCommitError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.commitErr = errors.New("commit failed")
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.InsertAuditTx(context.Background(), tx, sampleAudit())
	})
	if err == nil {
		t.Fatal("expected commit error to surface")
	}
	if fdb.commits != 1 {
		t.Fatalf("expected exactly 1 commit attempt, got %d", fdb.commits)
	}
}

// TestWithTxAtomicityFaultInjection reproduces the QA fault-injection scenario
// (FI3 / C-1): the break status UPDATE succeeds but the accompanying audit
// INSERT fails inside the same transaction. WithTx must roll back so NEITHER
// write is committed — a terminal status can never be persisted without its
// audit event.
func TestWithTxAtomicityFaultInjection(t *testing.T) {
	s, fdb := newTestStore()
	// Let the status update succeed, but fail the audit insert.
	fdb.execHook = func(query string) error {
		if strings.HasPrefix(query, "INSERT INTO agent.agent_audit") {
			return errors.New("audit insert failed")
		}
		return nil
	}
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		if e := s.SetBreakStatusTx(context.Background(), tx, "ext_1", "auto-resolved"); e != nil {
			return e
		}
		return s.InsertAuditTx(context.Background(), tx, sampleAudit())
	})
	if err == nil {
		t.Fatal("expected the audit-insert failure to abort the transaction")
	}
	if fdb.commits != 0 || fdb.rollbacks != 1 {
		t.Fatalf("partial failure must roll back everything: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
	// Both statements were attempted (the status update and the failing audit
	// insert), but because the tx rolled back neither is durable.
	if len(fdb.execs) != 2 {
		t.Fatalf("expected 2 attempted execs, got %d", len(fdb.execs))
	}
}

func TestUpsertBreakTx(t *testing.T) {
	s, fdb := newTestStore()
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.UpsertBreakTx(context.Background(), tx, model.BreakClassification{
			ExternalTxnID: "ext_1",
			RootCause:     model.RootCauseAmountDrift,
			Confidence:    0.9,
			Rationale:     "why",
		}, blnk.ExternalTransaction{ID: "ext_1", Amount: 980, Currency: "USD", Reference: "INV-1003"}, "classified")
	})
	if err != nil {
		t.Fatalf("UpsertBreakTx: %v", err)
	}
	if len(fdb.execs) != 1 || !strings.HasPrefix(fdb.execs[0].query, "INSERT INTO agent.agent_break") {
		t.Fatalf("unexpected tx exec: %+v", fdb.execs)
	}
	// finding L2: 7 verdict args + 5 matchable-txn args = 12; rationale is arg 7
	// and the persisted amount/currency/reference follow it.
	if len(fdb.execs[0].args) != 12 || fdb.execs[0].args[6].Value != "why" {
		t.Fatalf("UpsertBreakTx must pass 12 args incl. rationale, got %+v", fdb.execs[0].args)
	}
	if fdb.execs[0].args[7].Value != float64(980) || fdb.execs[0].args[8].Value != "USD" || fdb.execs[0].args[9].Value != "INV-1003" {
		t.Fatalf("UpsertBreakTx must persist matchable txn fields, got %+v", fdb.execs[0].args[7:10])
	}
}

func TestSetBreakStatusTx(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusTx(context.Background(), tx, "ext_1", "auto-resolved")
	}); err != nil {
		t.Fatalf("SetBreakStatusTx: %v", err)
	}
	if !strings.HasPrefix(fdb.execs[0].query, "UPDATE agent.agent_break SET status") {
		t.Fatalf("unexpected SQL:\n%s", fdb.execs[0].query)
	}
}

func TestSetBreakStatusTxNotFound(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = driver.RowsAffected(0)
	err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetBreakStatusTx(context.Background(), tx, "missing", "auto-resolved")
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound to propagate out of WithTx, got %v", err)
	}
	// ErrNotFound from the update forces a rollback.
	if fdb.rollbacks != 1 || fdb.commits != 0 {
		t.Fatalf("ErrNotFound must roll back: commits=%d rollbacks=%d", fdb.commits, fdb.rollbacks)
	}
}

func TestEnqueueHITLTx(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.EnqueueHITLTx(context.Background(), tx, "ext_2", "regulated")
	}); err != nil {
		t.Fatalf("EnqueueHITLTx: %v", err)
	}
	if !strings.HasPrefix(fdb.execs[0].query, "INSERT INTO agent.agent_hitl_queue") {
		t.Fatalf("unexpected SQL:\n%s", fdb.execs[0].query)
	}
	if fdb.execs[0].args[0].Value != "ext_2" || fdb.execs[0].args[1].Value != "regulated" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

func TestInsertAuditTx(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.InsertAuditTx(context.Background(), tx, sampleAudit())
	}); err != nil {
		t.Fatalf("InsertAuditTx: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.HasPrefix(q, "INSERT INTO agent.agent_audit") {
		t.Fatalf("audit tx write must be an INSERT:\n%s", q)
	}
	upper := strings.ToUpper(q)
	if strings.Contains(upper, "UPDATE") || strings.Contains(upper, "DELETE") {
		t.Fatalf("Rule 5.5: audit tx statement must not UPDATE/DELETE:\n%s", q)
	}
	if len(fdb.execs[0].args) != 8 {
		t.Fatalf("expected 8 audit args, got %d", len(fdb.execs[0].args))
	}
}

// -----------------------------------------------------------------------------
// SetCreatedRule / SetCreatedRuleTx (finding M-3)
// -----------------------------------------------------------------------------

func TestSetCreatedRuleOK(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.SetCreatedRule(context.Background(), "ext_1", "rule_123"); err != nil {
		t.Fatalf("SetCreatedRule: %v", err)
	}
	q := fdb.execs[0].query
	if !strings.HasPrefix(q, "UPDATE agent.agent_break SET created_rule_id") {
		t.Fatalf("unexpected SQL:\n%s", q)
	}
	if fdb.execs[0].args[0].Value != "ext_1" || fdb.execs[0].args[1].Value != "rule_123" {
		t.Fatalf("unexpected args: %+v", fdb.execs[0].args)
	}
}

func TestSetCreatedRuleNotFound(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execResult = driver.RowsAffected(0)
	if err := s.SetCreatedRule(context.Background(), "missing", "r"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSetCreatedRuleExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("boom")
	if err := s.SetCreatedRule(context.Background(), "ext_1", "r"); err == nil {
		t.Fatal("expected exec error")
	}
}

func TestSetCreatedRuleTx(t *testing.T) {
	s, fdb := newTestStore()
	if err := s.WithTx(context.Background(), func(tx *sql.Tx) error {
		return s.SetCreatedRuleTx(context.Background(), tx, "ext_1", "rule_123")
	}); err != nil {
		t.Fatalf("SetCreatedRuleTx: %v", err)
	}
	if !strings.HasPrefix(fdb.execs[0].query, "UPDATE agent.agent_break SET created_rule_id") {
		t.Fatalf("unexpected SQL:\n%s", fdb.execs[0].query)
	}
}

// -----------------------------------------------------------------------------
// LoadBreak (findings M-3, M-6 idempotency/resume)
// -----------------------------------------------------------------------------

func TestLoadBreakFound(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{"amount_drift", 0.91, false, "auto-resolved", mustMarshal(t, sampleRule()), "why", "rule_abc"},
	}
	c, status, ruleID, found, err := s.LoadBreak(context.Background(), "ext_1")
	if err != nil || !found {
		t.Fatalf("LoadBreak: found=%v err=%v", found, err)
	}
	if c.ExternalTxnID != "ext_1" || c.RootCause != model.RootCauseAmountDrift || c.Confidence != 0.91 {
		t.Fatalf("classification mismatch: %+v", c)
	}
	if c.Rationale != "why" {
		t.Fatalf("rationale mismatch: %q", c.Rationale)
	}
	if c.ProposedRule == nil || c.ProposedRule.Criteria[0].Field != "amount" {
		t.Fatalf("proposed rule not loaded: %+v", c.ProposedRule)
	}
	if status != "auto-resolved" {
		t.Fatalf("status mismatch: %q", status)
	}
	if ruleID != "rule_abc" {
		t.Fatalf("created_rule_id mismatch: %q", ruleID)
	}
}

func TestLoadBreakNotFound(t *testing.T) {
	s, fdb := newTestStore()
	// No rows programmed => the underlying QueryRow.Scan returns sql.ErrNoRows,
	// which LoadBreak maps to found=false with a nil error.
	fdb.cols = []string{"root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id"}
	fdb.rows = nil
	c, status, ruleID, found, err := s.LoadBreak(context.Background(), "missing")
	if err != nil {
		t.Fatalf("not-found must be a nil error, got %v", err)
	}
	if found {
		t.Fatal("found must be false for a missing break")
	}
	if status != "" || ruleID != "" || c.ExternalTxnID != "" {
		t.Fatalf("zero values expected when not found: %+v %q %q", c, status, ruleID)
	}
}

func TestLoadBreakNullCreatedRule(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{"timing", 0.5, true, "queued", nil, "", nil},
	}
	c, status, ruleID, found, err := s.LoadBreak(context.Background(), "ext_2")
	if err != nil || !found {
		t.Fatalf("LoadBreak: found=%v err=%v", found, err)
	}
	if !c.Regulated || status != "queued" {
		t.Fatalf("unexpected load: regulated=%v status=%q", c.Regulated, status)
	}
	if c.ProposedRule != nil {
		t.Fatalf("nil proposed_rule expected, got %+v", c.ProposedRule)
	}
	if ruleID != "" {
		t.Fatalf("NULL created_rule_id must map to empty string, got %q", ruleID)
	}
}

func TestLoadBreakScanError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{"timing", "not-a-float", true, "queued", nil, "", nil},
	}
	if _, _, _, _, err := s.LoadBreak(context.Background(), "ext_2"); err == nil {
		t.Fatal("expected scan error")
	}
}

func TestLoadBreakBadRuleJSON(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"root_cause", "confidence", "regulated", "status", "proposed_rule", "rationale", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{"timing", 0.5, false, "classified", []byte("{not-json"), "", nil},
	}
	// LoadBreak feeds the resume/idempotency path; a corrupt persisted rule must
	// surface as an error there (unlike ListBreaks, which isolates per row for
	// display), so the remediator never resumes from a corrupt rule.
	if _, _, _, _, err := s.LoadBreak(context.Background(), "ext_2"); err == nil {
		t.Fatal("expected unmarshal error for bad proposed_rule JSON")
	}
}

// -----------------------------------------------------------------------------
// LoadBreakTxn (finding L2 — re_drive support) + Ping (finding M3 — healthz)
// -----------------------------------------------------------------------------

func TestLoadBreakTxnFound(t *testing.T) {
	s, fdb := newTestStore()
	date := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	fdb.cols = []string{"amount", "currency", "reference", "description", "txn_date", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{1500.0, "USD", "INV-1001", "wire settlement", date, "rule_xyz"},
	}
	txn, ruleID, found, err := s.LoadBreakTxn(context.Background(), "EXT-001")
	if err != nil || !found {
		t.Fatalf("LoadBreakTxn: found=%v err=%v", found, err)
	}
	if txn.ID != "EXT-001" {
		t.Fatalf("txn id must be the break id, got %q", txn.ID)
	}
	if txn.Amount != 1500.0 || txn.Currency != "USD" || txn.Reference != "INV-1001" || txn.Description != "wire settlement" {
		t.Fatalf("matchable fields not reconstructed: %+v", txn)
	}
	if !txn.Date.Equal(date) {
		t.Fatalf("txn date mismatch: got %v want %v", txn.Date, date)
	}
	if ruleID != "rule_xyz" {
		t.Fatalf("created_rule_id mismatch: %q", ruleID)
	}
	// The query must target only the agent schema (Rule 5.1).
	if len(fdb.queries) != 1 || !strings.Contains(fdb.queries[0].query, "agent.agent_break") {
		t.Fatalf("LoadBreakTxn must SELECT from agent.agent_break, got %+v", fdb.queries)
	}
}

func TestLoadBreakTxnNullColumns(t *testing.T) {
	s, fdb := newTestStore()
	// A pre-upgrade row (or a break with no date) yields NULLs; LoadBreakTxn must
	// return a well-formed transaction with the corresponding zero values.
	fdb.cols = []string{"amount", "currency", "reference", "description", "txn_date", "created_rule_id"}
	fdb.rows = [][]driver.Value{
		{nil, nil, nil, nil, nil, nil},
	}
	txn, ruleID, found, err := s.LoadBreakTxn(context.Background(), "EXT-002")
	if err != nil || !found {
		t.Fatalf("LoadBreakTxn: found=%v err=%v", found, err)
	}
	if txn.ID != "EXT-002" || txn.Amount != 0 || txn.Currency != "" || txn.Reference != "" || txn.Description != "" || !txn.Date.IsZero() {
		t.Fatalf("NULL columns must map to zero values, got %+v", txn)
	}
	if ruleID != "" {
		t.Fatalf("NULL created_rule_id must map to empty string, got %q", ruleID)
	}
}

func TestLoadBreakTxnNotFound(t *testing.T) {
	s, fdb := newTestStore()
	// No rows programmed => QueryRow.Scan returns sql.ErrNoRows, which
	// LoadBreakTxn maps to found=false with a nil error.
	fdb.cols = []string{"amount", "currency", "reference", "description", "txn_date", "created_rule_id"}
	fdb.rows = nil
	txn, ruleID, found, err := s.LoadBreakTxn(context.Background(), "missing")
	if err != nil {
		t.Fatalf("LoadBreakTxn must return nil error when absent, got %v", err)
	}
	if found || ruleID != "" || txn.ID != "" {
		t.Fatalf("absent break must yield found=false and zero values, got found=%v ruleID=%q txn=%+v", found, ruleID, txn)
	}
}

func TestPingOK(t *testing.T) {
	s, _ := newTestStore()
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping against a live (fake) connection must succeed, got %v", err)
	}
}

func TestPingNilDB(t *testing.T) {
	s := &Store{}
	if err := s.Ping(context.Background()); err == nil {
		t.Fatal("Ping on an uninitialized store must error")
	}
}
