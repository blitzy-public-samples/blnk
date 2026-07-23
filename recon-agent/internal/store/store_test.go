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

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

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

	cols     []string
	rows     [][]driver.Value
	queryErr error
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
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("begin not supported by fake") }

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.db.execs = append(c.db.execs, recordedCall{query: query, args: args})
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
	if len(fdb.execs) != 1 {
		t.Fatalf("expected exactly 1 exec, got %d", len(fdb.execs))
	}
	up := fdb.execs[0].query
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
	if err := s.UpsertBreak(context.Background(), c, "classified"); err != nil {
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
	if len(args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(args))
	}
	if args[0].Value != "ext_1" || args[1].Value != "amount_drift" || args[2].Value != 0.91 || args[3].Value != false || args[4].Value != "classified" {
		t.Fatalf("unexpected args: %+v", args)
	}
	ruleArg, ok := args[5].Value.(string)
	if !ok || !strings.Contains(ruleArg, "\"field\":\"amount\"") {
		t.Fatalf("proposed_rule arg should be JSON string with the rule, got %#v", args[5].Value)
	}
}

func TestUpsertBreakNilRuleStoresNull(t *testing.T) {
	s, fdb := newTestStore()
	c := model.BreakClassification{
		ExternalTxnID: "ext_2",
		RootCause:     model.RootCauseTiming,
		Confidence:    0.4,
	}
	if err := s.UpsertBreak(context.Background(), c, "queued"); err != nil {
		t.Fatalf("UpsertBreak: %v", err)
	}
	if got := fdb.execs[0].args[5].Value; got != nil {
		t.Fatalf("nil ProposedRule must map to SQL NULL, got %#v", got)
	}
}

func TestUpsertBreakExecError(t *testing.T) {
	s, fdb := newTestStore()
	fdb.execErr = errors.New("db down")
	if err := s.UpsertBreak(context.Background(), model.BreakClassification{ExternalTxnID: "x"}, "classified"); err == nil {
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
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "created_at", "updated_at"}
	fdb.rows = [][]driver.Value{
		{"ext_1", "amount_drift", 0.91, false, "auto-resolved", mustMarshal(t, sampleRule()), now, now},
		{"ext_2", "timing", 0.42, true, "queued", nil, now, now},
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
	if !breaks[0].CreatedAt.Equal(now) {
		t.Fatalf("break0 created_at mismatch: %v", breaks[0].CreatedAt)
	}
	if breaks[1].Classification.ProposedRule != nil {
		t.Fatalf("break1 should have nil rule, got %+v", breaks[1].Classification.ProposedRule)
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
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "created_at", "updated_at"}
	// confidence is a non-numeric string => Scan into *float64 fails.
	fdb.rows = [][]driver.Value{
		{"ext_1", "amount_drift", "not-a-float", false, "queued", nil, time.Now(), time.Now()},
	}
	if _, err := s.ListBreaks(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
}

func TestListBreaksBadRuleJSON(t *testing.T) {
	s, fdb := newTestStore()
	fdb.cols = []string{"external_txn_id", "root_cause", "confidence", "regulated", "status", "proposed_rule", "created_at", "updated_at"}
	fdb.rows = [][]driver.Value{
		{"ext_1", "amount_drift", 0.9, false, "queued", []byte("{not-json"), time.Now(), time.Now()},
	}
	if _, err := s.ListBreaks(context.Background()); err == nil {
		t.Fatal("expected unmarshal error for bad proposed_rule JSON")
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
	if err := s.UpsertBreak(ctx, model.BreakClassification{ExternalTxnID: "ext_1", RootCause: model.RootCauseTiming, ProposedRule: sampleRule()}, "classified"); err != nil {
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
		// Rule 5.5: no UPDATE/DELETE may target agent_audit.
		if strings.Contains(lower, "agent_audit") {
			if strings.Contains(lower, "update ") || strings.Contains(lower, "delete ") {
				t.Fatalf("Rule 5.5 violation: mutating statement targets agent_audit:\n%s", c.query)
			}
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
