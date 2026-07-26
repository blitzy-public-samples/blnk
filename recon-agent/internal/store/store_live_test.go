//go:build integration

// store_live_test.go exercises the Store against a REAL PostgreSQL database
// (the shared blnk database, agent schema), complementing store_test.go which
// uses a hand-rolled database/sql/driver fake. The fake proves the SQL *surface*
// (which statements are issued, Rule 5.1/5.5 text invariants, scan behaviour)
// but cannot prove that PostgreSQL actually ENFORCES those invariants — most
// importantly the Rule 5.5 append-only trigger on agent.agent_audit and the
// real transactional atomicity of WithTx (QA finding F3). These tests close that
// gap by asserting behaviour that only a live engine can demonstrate:
//
//   - the append-only trigger REJECTS UPDATE and DELETE on agent.agent_audit at
//     the database boundary while permitting INSERT and SELECT (Rule 5.5);
//   - WithTx is truly atomic on real PG: a mid-transaction failure rolls back
//     BOTH the break status change and its audit append, and the success path
//     commits both together (Rule 5.3 action-to-audit parity);
//   - UpsertBreak's ON CONFLICT clause is idempotent on a real unique key: a
//     duplicate external_txn_id updates in place to the latest values, never
//     inserting a second row;
//   - SetBreakStatusFromQueuedTx is a real queued-only compare-and-set: it
//     transitions a queued break exactly once and thereafter returns ErrNotQueued
//     (never silently overwriting a terminal status), and ErrNotFound for an
//     unknown break (finding M-01).
//
// The whole file is guarded by the `integration` build tag AND skips at runtime
// unless AGENT_DATABASE_URL is set, so it never runs in the default,
// service-free `go test ./...` used for the Rule 5.9 coverage floor. It is bound
// in CI (which stands up PostgreSQL) via `go test -tags=integration ./...`.
//
// Isolation: every test uses UUID-unique external_txn_ids so concurrent or
// repeated runs against the shared agent schema never collide, and so the
// append-only agent_audit table (whose rows can never be deleted, by design)
// accumulates only orthogonal rows. Migrate is called at setup and is
// idempotent, so the tests are self-contained regardless of prior schema state.
package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// liveStore opens a Store against AGENT_DATABASE_URL and applies the (idempotent)
// migration so the agent schema, tables and the append-only trigger exist. It
// skips the calling test when AGENT_DATABASE_URL is unset so the default
// service-free run is unaffected.
func liveStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("AGENT_DATABASE_URL")
	if dsn == "" {
		t.Skip("AGENT_DATABASE_URL not set; skipping live-PostgreSQL store test")
	}
	s, err := New(dsn)
	require.NoError(t, err, "open store against AGENT_DATABASE_URL")
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.Migrate(ctx), "migrate must create the agent schema, tables and append-only trigger")
	return s
}

// liveBreakID returns a UUID-unique external transaction id so live tests never
// collide on the shared agent schema.
func liveBreakID() string { return "LIVE-" + uuid.NewString() }

// liveClassification builds a minimal, valid BreakClassification for a live row.
func liveClassification(id string, rc model.RootCause, confidence float64, regulated bool) model.BreakClassification {
	return model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     rc,
		Confidence:    confidence,
		Regulated:     regulated,
		Rationale:     "live store test fixture",
	}
}

// liveTxn builds the matchable external-transaction fields persisted alongside a
// break row.
func liveTxn(id string, amount float64, currency string) blnk.ExternalTransaction {
	return blnk.ExternalTransaction{
		ID:          id,
		Amount:      amount,
		Currency:    currency,
		Reference:   "REF-" + id,
		Description: "live store test txn",
		Date:        time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		Source:      "live-test",
	}
}

// liveAuditEvent builds a valid raw audit row (as store.InsertAudit persists it,
// without going through the audit.Writer validator) for the given break.
func liveAuditEvent(id string) model.AuditEvent {
	return model.AuditEvent{
		EventID:       uuid.NewString(),
		ExternalTxnID: id,
		Actor:         "agent",
		Action:        "classified",
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		Rationale:     "live audit fixture",
		Confidence:    0.9,
		Provenance:    model.Provenance{Model: "kimi-k3"},
	}
}

// liveCountAudit returns how many audit rows exist for a break id (a SELECT,
// which Rule 5.5 permits).
func liveCountAudit(t *testing.T, s *Store, id string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	require.NoError(t, s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM agent.agent_audit WHERE external_txn_id = $1`, id).Scan(&n))
	return n
}

// liveCountBreak returns how many agent_break rows exist for a break id.
func liveCountBreak(t *testing.T, s *Store, id string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	require.NoError(t, s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM agent.agent_break WHERE external_txn_id = $1`, id).Scan(&n))
	return n
}

// TestLiveAuditAppendOnlyTrigger proves the Rule 5.5 append-only invariant is
// enforced by PostgreSQL itself (the agent_audit_no_mutate trigger), not merely
// by application convention: a real UPDATE and a real DELETE issued directly
// against the table — bypassing every store method — are both rejected, while
// INSERT and SELECT continue to work. The QA fake driver can only prove the
// store never *issues* an UPDATE/DELETE; only this live test proves the engine
// would *refuse* one.
func TestLiveAuditAppendOnlyTrigger(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := liveBreakID()
	ev := liveAuditEvent(id)

	// INSERT is permitted (append is the only allowed mutation).
	require.NoError(t, s.InsertAudit(ctx, ev), "INSERT into agent_audit must be permitted")
	require.Equal(t, 1, liveCountAudit(t, s, id), "the appended event must be present")

	// A raw UPDATE, issued directly against the connection (not via any store
	// method), must be rejected by the append-only trigger.
	_, updErr := s.db.ExecContext(ctx,
		`UPDATE agent.agent_audit SET rationale = 'tampered' WHERE event_id = $1`, ev.EventID)
	require.Error(t, updErr, "UPDATE on agent_audit must be rejected by the append-only trigger")
	require.ErrorContains(t, updErr, "append-only", "the trigger must identify the Rule 5.5 violation")
	require.ErrorContains(t, updErr, "UPDATE", "the trigger message must name the rejected operation")

	// A raw DELETE must likewise be rejected.
	_, delErr := s.db.ExecContext(ctx,
		`DELETE FROM agent.agent_audit WHERE event_id = $1`, ev.EventID)
	require.Error(t, delErr, "DELETE on agent_audit must be rejected by the append-only trigger")
	require.ErrorContains(t, delErr, "append-only", "the trigger must identify the Rule 5.5 violation")
	require.ErrorContains(t, delErr, "DELETE", "the trigger message must name the rejected operation")

	// Neither the rejected UPDATE nor the rejected DELETE took effect: the row is
	// intact and unchanged.
	require.Equal(t, 1, liveCountAudit(t, s, id), "the event must survive the rejected UPDATE/DELETE")
	var rationale string
	require.NoError(t, s.db.QueryRowContext(ctx,
		`SELECT rationale FROM agent.agent_audit WHERE event_id = $1`, ev.EventID).Scan(&rationale))
	require.Equal(t, "live audit fixture", rationale, "the original rationale must be unchanged (the UPDATE was rejected)")
}

// TestLiveWithTxAtomicity proves WithTx is genuinely transactional on real
// PostgreSQL: when fn fails after issuing writes, PostgreSQL rolls BOTH the
// status change and the audit append back (nothing is persisted); when fn
// succeeds, both commit together. This is the atomicity primitive the remediator
// relies on so a terminal status can never be persisted without its audit event
// (Rule 5.3).
func TestLiveWithTxAtomicity(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := liveBreakID()
	// Seed a queued break in autocommit mode.
	require.NoError(t, s.UpsertBreak(ctx, liveClassification(id, model.RootCauseTiming, 0.9, false), liveTxn(id, 1500, "USD"), model.Provenance{}, "queued"))
	require.Equal(t, 0, liveCountAudit(t, s, id), "no audit rows before any transaction")

	// Failure path: change status and append an audit event, then fail. Both
	// writes must roll back.
	errInjected := errors.New("injected mid-transaction failure")
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		if e := s.MarkResolvedTx(ctx, tx, id, "recon-rollback-fixture"); e != nil {
			return e
		}
		if e := s.InsertAuditTx(ctx, tx, liveAuditEvent(id)); e != nil {
			return e
		}
		return errInjected
	})
	require.ErrorIs(t, err, errInjected, "WithTx must surface fn's error")

	_, status, _, found, loadErr := s.LoadBreak(ctx, id)
	require.NoError(t, loadErr)
	require.True(t, found)
	require.Equal(t, "queued", status, "the rolled-back transaction must NOT have changed the status")
	require.Equal(t, 0, liveCountAudit(t, s, id), "the rolled-back transaction must NOT have appended an audit event")

	// Success path: the same two writes must both commit.
	require.NoError(t, s.WithTx(ctx, func(tx *sql.Tx) error {
		if e := s.SetBreakStatusTx(ctx, tx, id, "accepted"); e != nil {
			return e
		}
		return s.InsertAuditTx(ctx, tx, liveAuditEvent(id))
	}))

	_, status, _, found, loadErr = s.LoadBreak(ctx, id)
	require.NoError(t, loadErr)
	require.True(t, found)
	require.Equal(t, "accepted", status, "the committed transaction must have changed the status")
	require.Equal(t, 1, liveCountAudit(t, s, id), "the committed transaction must have appended exactly one audit event")
}

// TestLiveUpsertBreakIdempotent proves UpsertBreak's ON CONFLICT
// (external_txn_id) DO UPDATE clause is idempotent against the REAL primary-key
// constraint: upserting the same id twice leaves exactly one row carrying the
// latest classification and status — never a duplicate row.
func TestLiveUpsertBreakIdempotent(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := liveBreakID()

	// First upsert.
	require.NoError(t, s.UpsertBreak(ctx,
		liveClassification(id, model.RootCauseTiming, 0.50, false),
		liveTxn(id, 100, "USD"), model.Provenance{}, "queued"))
	require.Equal(t, 1, liveCountBreak(t, s, id), "the first upsert inserts exactly one row")

	// Second upsert of the SAME id with different values must UPDATE in place.
	require.NoError(t, s.UpsertBreak(ctx,
		liveClassification(id, model.RootCauseAmountDrift, 0.95, true),
		liveTxn(id, 250, "EUR"), model.Provenance{}, "accepted"))
	require.Equal(t, 1, liveCountBreak(t, s, id), "the duplicate-id upsert must NOT insert a second row")

	// The single surviving row carries the latest values.
	c, status, _, found, err := s.LoadBreak(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, model.RootCauseAmountDrift, c.RootCause, "root_cause must reflect the latest upsert")
	require.InDelta(t, 0.95, c.Confidence, 1e-9, "confidence must reflect the latest upsert")
	require.True(t, c.Regulated, "regulated must reflect the latest upsert")
	require.Equal(t, "accepted", status, "status must reflect the latest upsert")
}

// TestLiveSetBreakStatusFromQueuedCAS proves SetBreakStatusFromQueuedTx is a real
// queued-only compare-and-set on PostgreSQL (finding M-01): the first transition
// out of queued succeeds, a second transition attempt returns ErrNotQueued
// (never silently overwriting the now-terminal status), and an unknown break
// returns ErrNotFound.
func TestLiveSetBreakStatusFromQueuedCAS(t *testing.T) {
	s := liveStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := liveBreakID()
	require.NoError(t, s.UpsertBreak(ctx, liveClassification(id, model.RootCauseTiming, 0.9, false), liveTxn(id, 1500, "USD"), model.Provenance{}, "queued"))

	// First decision: queued -> accepted succeeds.
	require.NoError(t, s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(ctx, tx, id, "accepted")
	}), "the first queued-only transition must succeed")

	_, status, _, found, err := s.LoadBreak(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "accepted", status, "the break must have transitioned to accepted")

	// Second decision on the SAME (now non-queued) break must fail closed with
	// ErrNotQueued, and must not change the terminal status.
	err = s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(ctx, tx, id, "rejected")
	})
	require.ErrorIs(t, err, ErrNotQueued, "a replayed decision on an already-decided break must return ErrNotQueued")

	_, status, _, found, err = s.LoadBreak(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "accepted", status, "the terminal status must be preserved (no silent overwrite)")

	// An unknown break returns ErrNotFound.
	err = s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.SetBreakStatusFromQueuedTx(ctx, tx, liveBreakID(), "accepted")
	})
	require.ErrorIs(t, err, ErrNotFound, "an unknown break must return ErrNotFound")
}
