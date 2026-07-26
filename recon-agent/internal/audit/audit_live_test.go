//go:build integration

// audit_live_test.go exercises the audit Writer against a REAL PostgreSQL
// database wired through its production sink, *store.Store (the shared blnk
// database, agent schema). audit_test.go already proves the Writer's logic with
// in-memory fake sinks — validation, id/timestamp normalization, the
// append-only-by-construction API surface, and the Rule 5.3 ClearanceProof gate.
// What a fake sink CANNOT prove is that those guarantees survive a real
// Writer -> store -> PostgreSQL round trip (QA finding F3): that a recorded event
// actually persists and reads back with its provenance JSONB and normalized
// RFC3339 timestamp intact, that the Rule 5.3 resolution gate blocks an unproven
// resolution BEFORE any row reaches the live ledger, and that RecordTx genuinely
// enrolls the append in the caller's transaction so it commits or rolls back
// atomically with the state change it records.
//
// The database-boundary enforcement of Rule 5.5 append-only immutability (the
// agent_audit_no_mutate trigger rejecting UPDATE/DELETE) is proved in the store
// package's TestLiveAuditAppendOnlyTrigger; here we focus on the Writer's own
// end-to-end contract against live PostgreSQL.
//
// Like store_live_test.go this file carries the `integration` build tag and
// skips at runtime unless AGENT_DATABASE_URL is set, so it never runs in the
// default service-free suite behind the Rule 5.9 coverage floor. It is bound in
// CI via `go test -tags=integration ./...`. Every event uses a UUID-unique
// external txn id so runs against the shared (append-only, never-cleaned) ledger
// stay orthogonal.
package audit

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/recon-agent/internal/model"
	"github.com/blnkfinance/recon-agent/internal/store"
)

// liveWriter opens the production *store.Store sink against AGENT_DATABASE_URL,
// migrates it (idempotent), and returns a Writer backed by it. It skips the
// calling test when AGENT_DATABASE_URL is unset so the default service-free run
// is unaffected. Because *store.Store implements both InsertAudit and
// InsertAuditTx, the returned Writer supports RecordTx.
func liveWriter(t *testing.T) (*Writer, *store.Store) {
	t.Helper()
	dsn := os.Getenv("AGENT_DATABASE_URL")
	if dsn == "" {
		t.Skip("AGENT_DATABASE_URL not set; skipping live-PostgreSQL audit test")
	}
	s, err := store.New(dsn)
	require.NoError(t, err, "open store against AGENT_DATABASE_URL")
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, s.Migrate(ctx), "migrate must create the agent schema and audit table")

	w, err := New(s)
	require.NoError(t, err, "New must accept *store.Store as the sink")
	return w, s
}

// liveAuditID returns a UUID-unique external transaction id for a live audit row.
func liveAuditID() string { return "LIVE-AUD-" + uuid.NewString() }

// TestLiveRecordRoundTrip proves a classified event survives the full
// Writer -> store -> PostgreSQL -> ListAudit round trip with its identity,
// normalized timestamp, and provenance JSONB (including the classification
// evidence stamped by audit.Classified) intact.
func TestLiveRecordRoundTrip(t *testing.T) {
	w, s := liveWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := liveAuditID()
	c := model.BreakClassification{
		ExternalTxnID: id,
		RootCause:     model.RootCauseTiming,
		Confidence:    0.91,
		Regulated:     false,
		Rationale:     "live round-trip classification",
	}
	prov := model.Provenance{Model: "kimi-k3", UploadID: "up-live-1", Source: "live-bank"}

	require.NoError(t, w.Record(ctx, Classified(c, prov)), "recording a classified event must persist to live PG")

	events, err := s.ListAudit(ctx)
	require.NoError(t, err)

	var got []model.AuditEvent
	for _, e := range events {
		if e.ExternalTxnID == id {
			got = append(got, e)
		}
	}
	require.Len(t, got, 1, "exactly one persisted event must exist for the unique break id")
	ev := got[0]

	require.Equal(t, ActorAgent, ev.Actor)
	require.Equal(t, ActionClassified, ev.Action)
	require.InDelta(t, 0.91, ev.Confidence, 1e-9, "confidence must round-trip")
	require.Equal(t, "live round-trip classification", ev.Rationale)

	// EventID was assigned by prepare() and must be a valid UUID after the round trip.
	_, perr := uuid.Parse(ev.EventID)
	require.NoError(t, perr, "the persisted event_id must be a valid UUID")

	// Timestamp was stamped from the server clock and re-formatted by ListAudit;
	// it must parse as RFC3339Nano.
	_, terr := time.Parse(time.RFC3339Nano, ev.Timestamp)
	require.NoError(t, terr, "the persisted timestamp must be RFC3339Nano")

	// Provenance JSONB must restore every field, including the classification
	// evidence audit.Classified stamps (root_cause) — proving the JSONB
	// marshal/restore path is intact end-to-end.
	require.Equal(t, "kimi-k3", ev.Provenance.Model)
	require.Equal(t, "up-live-1", ev.Provenance.UploadID)
	require.Equal(t, "live-bank", ev.Provenance.Source)
	require.Equal(t, string(model.RootCauseTiming), ev.Provenance.RootCause, "classification evidence must be persisted in provenance")
}

// TestLiveResolvedGateEndToEnd proves the Rule 5.3 deterministic-arbiter gate is
// enforced end-to-end against live PostgreSQL: a resolution backed by a
// ClearanceProof persists with its confirming recon_id, while a resolved event
// lacking a confirming recon_id is rejected by the Writer BEFORE any row reaches
// the ledger (so LLM confidence alone can never mark a resolution).
func TestLiveResolvedGateEndToEnd(t *testing.T) {
	w, s := liveWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Proven resolution: a ClearanceProof (cleared==true, non-empty recon id) is
	// required to even construct a recordable resolved event.
	provenID := liveAuditID()
	proof, err := NewClearanceProof("recon-live-xyz", true)
	require.NoError(t, err)
	require.NoError(t, w.Record(ctx,
		Resolved(provenID, "cleared by Blnk dry-run", 0.99, model.Provenance{Model: "kimi-k3"}, proof)),
		"a proven resolution must persist")

	events, err := s.ListAudit(ctx)
	require.NoError(t, err)
	var resolved *model.AuditEvent
	for i := range events {
		if events[i].ExternalTxnID == provenID && events[i].Action == ActionResolved {
			resolved = &events[i]
			break
		}
	}
	require.NotNil(t, resolved, "the proven resolution must be present in the live ledger")
	require.Equal(t, "recon-live-xyz", resolved.Provenance.ReconID, "the confirming recon_id must be persisted (Rule 5.3)")

	// Unproven resolution: a resolved event with no confirming recon_id must be
	// rejected before any INSERT, so the live ledger gains no such row.
	unprovenID := liveAuditID()
	err = w.Record(ctx, model.AuditEvent{
		ExternalTxnID: unprovenID,
		Actor:         ActorAgent,
		Action:        ActionResolved,
		Rationale:     "attempted resolution without a confirming reconciliation",
		Confidence:    0.99,
		Provenance:    model.Provenance{Model: "kimi-k3"}, // ReconID deliberately empty
	})
	require.ErrorIs(t, err, ErrEmptyReconID, "an unproven resolution must be rejected (Rule 5.3)")

	n, err := s.CountAuditByAction(ctx, unprovenID, ActionResolved)
	require.NoError(t, err)
	require.Equal(t, 0, n, "the rejected resolution must NOT have reached the live ledger")
}

// TestLiveRecordTxAtomic proves RecordTx genuinely enrolls the audit append in
// the caller's transaction against live PostgreSQL: when the surrounding
// transaction fails the appended event rolls back (no row persists), and when it
// succeeds the event commits (exactly one row persists).
func TestLiveRecordTxAtomic(t *testing.T) {
	w, s := liveWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Rollback path.
	rollbackID := liveAuditID()
	cRollback := model.BreakClassification{ExternalTxnID: rollbackID, RootCause: model.RootCauseTiming, Confidence: 0.9, Rationale: "tx rollback"}
	errInjected := errors.New("injected post-record failure")
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		if e := w.RecordTx(ctx, tx, Classified(cRollback, model.Provenance{Model: "kimi-k3"})); e != nil {
			return e
		}
		return errInjected
	})
	require.ErrorIs(t, err, errInjected, "WithTx must surface the injected failure")

	n, err := s.CountAuditByAction(ctx, rollbackID, ActionClassified)
	require.NoError(t, err)
	require.Equal(t, 0, n, "the audit append must roll back with its transaction")

	// Commit path.
	commitID := liveAuditID()
	cCommit := model.BreakClassification{ExternalTxnID: commitID, RootCause: model.RootCauseTiming, Confidence: 0.9, Rationale: "tx commit"}
	require.NoError(t, s.WithTx(ctx, func(tx *sql.Tx) error {
		return w.RecordTx(ctx, tx, Classified(cCommit, model.Provenance{Model: "kimi-k3"}))
	}))

	n, err = s.CountAuditByAction(ctx, commitID, ActionClassified)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the audit append must commit with its transaction")
}
