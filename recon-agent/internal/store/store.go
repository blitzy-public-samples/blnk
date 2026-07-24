// Package store owns ALL of recon-agent's database access. It creates and
// manages three additive tables in a dedicated "agent" schema inside the shared
// blnk database and applies its own boot-time migration. It NEVER reads or
// writes any blnk.* table (Rule 5.1): every statement is fully qualified
// against the agent schema, and the audit ledger is append-only (Rule 5.5).
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"strings"
	"time"

	// lib/pq registers the "postgres" database/sql driver via its init side
	// effect. It is the ONLY external dependency of this package.
	_ "github.com/lib/pq"

	"github.com/blnkfinance/recon-agent/internal/blnk"
	"github.com/blnkfinance/recon-agent/internal/model"
)

// schemaSQL is the embedded migration file. store.Migrate executes only its
// "Up" section (extracted by upSection), so the destructive teardown is never
// run at boot.
//
//go:embed schema.sql
var schemaSQL string

// ErrNotFound is returned when a status update targets a break that does not
// exist.
var ErrNotFound = errors.New("store: record not found")

// ErrNotQueued is returned by SetBreakStatusFromQueuedTx when the target break
// exists but is no longer in the queued state, so a human decision must not
// overwrite its terminal (already-decided) status. It is the compare-and-set
// failure signal backing the queued-only guard (finding M-01); handleDecision
// maps it to 409 Conflict.
var ErrNotQueued = errors.New("store: break is not awaiting review")

// Break is one external break under management as persisted in
// agent.agent_break. It carries the classifier's verdict plus the agent's
// current lifecycle status and timestamps.
type Break struct {
	Classification model.BreakClassification `json:"classification"`
	Status         string                    `json:"status"`
	// CreatedRuleID is the id of the Blnk matching rule the agent created while
	// auto-remediating this break (empty when none was created). It is persisted
	// so a retry after a partial failure can reuse the already-created rule
	// instead of creating a duplicate one in Blnk.
	CreatedRuleID string    `json:"created_rule_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// sqlExecer is the subset of *sql.DB and *sql.Tx used by the write helpers.
// Defining each write once against this interface lets the store expose both an
// autocommit method (backed by s.db) and a transaction-bound *Tx variant
// (backed by a caller's *sql.Tx) without duplicating any SQL. Both *sql.DB and
// *sql.Tx satisfy it.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// HITLItem is one break awaiting a human decision, persisted in
// agent.agent_hitl_queue.
type HITLItem struct {
	ExternalTxnID string    `json:"external_txn_id"`
	Reason        string    `json:"reason"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
}

// Store is the sole PostgreSQL gateway for the recon-agent. Every statement it
// issues targets the agent schema; no blnk.* table is ever read or written.
type Store struct {
	db *sql.DB
}

// New opens a Store against the given PostgreSQL DSN (from AGENT_DATABASE_URL)
// using the lib/pq "postgres" driver. The connection is opened lazily; call
// Migrate (or any method) to force a real connection.
func New(dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("store: empty DSN")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Ping verifies the agent database is reachable by forcing a round-trip to
// PostgreSQL. It backs the HITL /healthz dependency check (finding M3): a
// healthy agent requires a live database connection, so /healthz reports 503
// when Ping fails.
func (s *Store) Ping(ctx context.Context) error {
	if s.db == nil {
		return errors.New("store: not initialized")
	}
	return s.db.PingContext(ctx)
}

// migrateAdvisoryLockKey is the fixed key every recon-agent instance uses for
// the boot-migration Postgres advisory lock (finding M2). Its exact value is
// arbitrary; the ONLY requirement is that all instances sharing a database use
// the SAME key so their migrations serialize on one lock. 0x7265636F6E is the
// ASCII bytes of "recon".
const migrateAdvisoryLockKey int64 = 0x7265636F6E

// Migrate applies the embedded schema. It executes ONLY the Up section of
// schema.sql, so the destructive Down section is never run. The DDL uses
// IF NOT EXISTS throughout, making Migrate idempotent and safe to call on every
// boot.
//
// M2 (concurrent cold-start race): `CREATE SCHEMA IF NOT EXISTS` — and the
// other IF NOT EXISTS catalog inserts — are NOT race-safe across backends.
// Two instances booting at once can both observe the schema as absent and then
// collide inserting into pg_namespace (duplicate key on
// pg_namespace_nspname_index), which previously FATAL'd one of the two
// recon-agent processes. Migrate therefore runs the whole DDL inside a single
// transaction guarded by a transaction-scoped Postgres advisory lock: the lock
// makes the DDL block mutually exclusive across every connection and instance
// sharing the database, and pg_advisory_xact_lock releases automatically on
// COMMIT/ROLLBACK. Running the lock acquisition and the DDL in the same
// transaction guarantees they execute on the same backend connection (a pooled
// s.db.ExecContext could otherwise run them on different connections). The loser
// of the race simply re-runs the idempotent DDL after acquiring the lock, which
// no-ops.
func (s *Store) Migrate(ctx context.Context) error {
	up := upSection(schemaSQL)
	if strings.TrimSpace(up) == "" {
		return errors.New("store: migration has no Up section")
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", migrateAdvisoryLockKey); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, up); err != nil {
			return err
		}
		return nil
	})
}

// upSection returns the text of a migration's Up block: everything between a
// line that is exactly "-- +migrate Up" and the next line that is exactly
// "-- +migrate Down". Whole-line matching (rather than a raw substring search)
// means the markers may also be mentioned in prose comments without tripping
// the extraction.
func upSection(schema string) string {
	const (
		upMarker   = "-- +migrate Up"
		downMarker = "-- +migrate Down"
	)
	var out []string
	inUp := false
	for _, line := range strings.Split(schema, "\n") {
		switch strings.TrimSpace(line) {
		case upMarker:
			inUp = true
			continue
		case downMarker:
			if inUp {
				return strings.Join(out, "\n")
			}
		}
		if inUp {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

// UpsertBreak inserts or updates the agent_break row for c.ExternalTxnID with
// the classified fields and the given lifecycle status. A proposed rule (when
// present) is stored as JSONB; a nil rule stores SQL NULL.
// WithTx runs fn inside a single database transaction. It commits when fn
// returns nil and rolls back — discarding every write fn performed — when fn
// returns an error (or when Commit itself fails). This is the atomicity
// primitive the remediator relies on to make a break's state transition and its
// audit event commit-or-roll-back together, so a terminal status can never be
// persisted without its audit event (Rule 5.3) and a queued break can never be
// persisted without both its HITL-queue entry and its escalation audit event.
//
// fn must perform all of its writes through the *sql.Tx it receives (via the
// *Tx-suffixed methods and InsertAuditTx); writes issued against the Store's
// autocommit connection from within fn are NOT part of the transaction.
func (s *Store) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		// Roll back and surface fn's error. The Rollback error is intentionally
		// ignored: fn's failure is the meaningful one, and a rollback failure on
		// an already-failed tx does not change the (uncommitted) outcome.
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// upsertBreakSQL inserts or refreshes a break row. created_rule_id is
// deliberately absent from the ON CONFLICT SET list so a re-upsert (for example
// when a partially-processed break is resumed and re-classified) preserves any
// rule id a prior attempt already recorded.
const upsertBreakSQL = `INSERT INTO agent.agent_break
	(external_txn_id, root_cause, confidence, regulated, status, proposed_rule, rationale,
	 amount, currency, reference, description, txn_date)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (external_txn_id) DO UPDATE SET
	root_cause = EXCLUDED.root_cause,
	confidence = EXCLUDED.confidence,
	regulated = EXCLUDED.regulated,
	status = EXCLUDED.status,
	proposed_rule = EXCLUDED.proposed_rule,
	rationale = EXCLUDED.rationale,
	amount = EXCLUDED.amount,
	currency = EXCLUDED.currency,
	reference = EXCLUDED.reference,
	description = EXCLUDED.description,
	txn_date = EXCLUDED.txn_date,
	updated_at = now()`

// upsertBreak is the single definition of the break-upsert write, shared by the
// autocommit UpsertBreak and the transactional UpsertBreakTx via sqlExecer. The
// txn argument carries the matchable fields of the original external transaction
// (amount/currency/reference/description/date); they are persisted so a queued
// break can be re-driven from the HITL surface without reading any blnk.* table
// (finding L2, Rule 5.1). txn_date is stored as SQL NULL when the transaction
// carries no date, so the column faithfully round-trips a zero date.
func upsertBreak(ctx context.Context, ex sqlExecer, c model.BreakClassification, txn blnk.ExternalTransaction, status string) error {
	var rule any
	if c.ProposedRule != nil {
		encoded, err := json.Marshal(c.ProposedRule)
		if err != nil {
			return err
		}
		rule = string(encoded)
	}
	var txnDate any
	if !txn.Date.IsZero() {
		txnDate = txn.Date
	}
	_, err := ex.ExecContext(ctx, upsertBreakSQL,
		c.ExternalTxnID,
		string(c.RootCause),
		c.Confidence,
		c.Regulated,
		status,
		rule,
		c.Rationale,
		txn.Amount,
		txn.Currency,
		txn.Reference,
		txn.Description,
		txnDate,
	)
	return err
}

// UpsertBreak inserts or updates a break in autocommit mode. txn supplies the
// original transaction's matchable fields to persist alongside the verdict.
func (s *Store) UpsertBreak(ctx context.Context, c model.BreakClassification, txn blnk.ExternalTransaction, status string) error {
	return upsertBreak(ctx, s.db, c, txn, status)
}

// UpsertBreakTx inserts or updates a break inside the caller's transaction so it
// commits atomically with the accompanying audit event. txn supplies the
// original transaction's matchable fields to persist alongside the verdict.
func (s *Store) UpsertBreakTx(ctx context.Context, tx *sql.Tx, c model.BreakClassification, txn blnk.ExternalTransaction, status string) error {
	return upsertBreak(ctx, tx, c, txn, status)
}

const setBreakStatusSQL = `UPDATE agent.agent_break SET status = $2, updated_at = now() WHERE external_txn_id = $1`

// setBreakStatus is the single definition of the status-update write, shared by
// SetBreakStatus and SetBreakStatusTx via sqlExecer.
func setBreakStatus(ctx context.Context, ex sqlExecer, externalTxnID, status string) error {
	res, err := ex.ExecContext(ctx, setBreakStatusSQL, externalTxnID, status)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetBreakStatus updates the lifecycle status of an existing break, returning
// ErrNotFound when no row matches externalTxnID.
func (s *Store) SetBreakStatus(ctx context.Context, externalTxnID, status string) error {
	return setBreakStatus(ctx, s.db, externalTxnID, status)
}

// SetBreakStatusTx updates a break's status inside the caller's transaction so
// the terminal status commits atomically with its audit event (Rule 5.3).
func (s *Store) SetBreakStatusTx(ctx context.Context, tx *sql.Tx, externalTxnID, status string) error {
	return setBreakStatus(ctx, tx, externalTxnID, status)
}

const setBreakStatusFromQueuedSQL = `UPDATE agent.agent_break
SET status = $2, updated_at = now()
WHERE external_txn_id = $1 AND status = 'queued'`

// SetBreakStatusFromQueuedTx transitions a break out of the queued state inside
// the caller's transaction, but ONLY when it is currently queued — a queued-only
// compare-and-set (finding M-01). It returns:
//   - nil          when exactly one queued break was transitioned,
//   - ErrNotQueued when the break exists but is no longer queued (already
//     decided/terminal), so a replayed or concurrent decision can never silently
//     overwrite a terminal status, and
//   - ErrNotFound  when no such break exists.
//
// Wrapping this guarded status change together with the HITL queue drain
// (DequeueHITLTx) and the decision audit append (audit.Writer.RecordTx) in one
// WithTx makes a human decision atomic: status, queue, and audit commit or roll
// back together (finding M-02).
func (s *Store) SetBreakStatusFromQueuedTx(ctx context.Context, tx *sql.Tx, externalTxnID, status string) error {
	res, err := tx.ExecContext(ctx, setBreakStatusFromQueuedSQL, externalTxnID, status)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	// No row transitioned: distinguish a missing break (ErrNotFound) from a
	// break that exists but is no longer queued (ErrNotQueued) so the HITL
	// handler can return 404 vs 409. The follow-up read runs inside the same
	// transaction, so it observes a consistent snapshot.
	var existing string
	qerr := tx.QueryRowContext(ctx,
		`SELECT status FROM agent.agent_break WHERE external_txn_id = $1`, externalTxnID).Scan(&existing)
	if errors.Is(qerr, sql.ErrNoRows) {
		return ErrNotFound
	}
	if qerr != nil {
		return qerr
	}
	return ErrNotQueued
}

const setCreatedRuleSQL = `UPDATE agent.agent_break SET created_rule_id = $2, updated_at = now() WHERE external_txn_id = $1`

// setCreatedRule is the single definition of the created-rule-id write, shared
// by SetCreatedRule and SetCreatedRuleTx via sqlExecer.
func setCreatedRule(ctx context.Context, ex sqlExecer, externalTxnID, ruleID string) error {
	res, err := ex.ExecContext(ctx, setCreatedRuleSQL, externalTxnID, ruleID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetCreatedRule records the id of the Blnk matching rule created for a break in
// autocommit mode.
func (s *Store) SetCreatedRule(ctx context.Context, externalTxnID, ruleID string) error {
	return setCreatedRule(ctx, s.db, externalTxnID, ruleID)
}

// SetCreatedRuleTx records the created Blnk rule id inside the caller's
// transaction so it commits atomically with the rule_created audit event. This
// lets a retry after a partial failure reuse the already-created rule instead
// of creating a duplicate one in Blnk (finding M-3).
func (s *Store) SetCreatedRuleTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error {
	return setCreatedRule(ctx, tx, externalTxnID, ruleID)
}

// LoadBreak returns the persisted classification, current lifecycle status, and
// created Blnk rule id for the break identified by externalTxnID. found is
// false (with a nil error) when no such break exists. The remediator uses it to
// make Handle idempotent — a break already in a terminal agent state is not
// reprocessed — and to resume a partially-processed break without creating a
// duplicate Blnk rule.
func (s *Store) LoadBreak(ctx context.Context, externalTxnID string) (c model.BreakClassification, status, createdRuleID string, found bool, err error) {
	const q = `SELECT root_cause, confidence, regulated, status, proposed_rule, rationale, created_rule_id
FROM agent.agent_break
WHERE external_txn_id = $1`
	var (
		cause    string
		ruleJSON []byte
		crid     sql.NullString
	)
	row := s.db.QueryRowContext(ctx, q, externalTxnID)
	if err = row.Scan(&cause, &c.Confidence, &c.Regulated, &status, &ruleJSON, &c.Rationale, &crid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.BreakClassification{}, "", "", false, nil
		}
		return model.BreakClassification{}, "", "", false, err
	}
	c.ExternalTxnID = externalTxnID
	c.RootCause = model.RootCause(cause)
	if len(ruleJSON) > 0 {
		var rule blnk.MatchingRule
		if err = json.Unmarshal(ruleJSON, &rule); err != nil {
			return model.BreakClassification{}, "", "", false, err
		}
		c.ProposedRule = &rule
	}
	if crid.Valid {
		createdRuleID = crid.String
	}
	return c, status, createdRuleID, true, nil
}

// LoadBreakTxn returns the persisted matchable fields of the original external
// transaction for a break (its amount/currency/reference/description/date) plus
// any Blnk matching-rule id the agent already created for it. found is false
// (with a nil error) when no such break exists.
//
// It backs the HITL re_drive action (finding L2): re-driving a queued break
// requires re-submitting the ORIGINAL transaction to a Blnk single-transaction
// dry-run, but the native Blnk HTTP surface exposes no route that returns an
// unmatched transaction's fields, and Rule 5.1 forbids reading blnk.* tables
// directly. The agent therefore reconstructs the transaction from the fields it
// persisted when the break was first managed. The returned transaction's ID is
// the break's external id; the caller (ProbeBreak) substitutes a fresh
// ephemeral id before submission so a re_drive never collides on Blnk's
// external_transactions primary key (finding F3).
func (s *Store) LoadBreakTxn(ctx context.Context, externalTxnID string) (txn blnk.ExternalTransaction, createdRuleID string, found bool, err error) {
	const q = `SELECT amount, currency, reference, description, txn_date, created_rule_id
FROM agent.agent_break
WHERE external_txn_id = $1`
	var (
		amount   sql.NullFloat64
		currency sql.NullString
		ref      sql.NullString
		desc     sql.NullString
		date     sql.NullTime
		crid     sql.NullString
	)
	row := s.db.QueryRowContext(ctx, q, externalTxnID)
	if err = row.Scan(&amount, &currency, &ref, &desc, &date, &crid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blnk.ExternalTransaction{}, "", false, nil
		}
		return blnk.ExternalTransaction{}, "", false, err
	}
	txn.ID = externalTxnID
	if amount.Valid {
		txn.Amount = amount.Float64
	}
	if currency.Valid {
		txn.Currency = currency.String
	}
	if ref.Valid {
		txn.Reference = ref.String
	}
	if desc.Valid {
		txn.Description = desc.String
	}
	if date.Valid {
		txn.Date = date.Time
	}
	if crid.Valid {
		createdRuleID = crid.String
	}
	return txn, createdRuleID, true, nil
}

// ListBreaks returns every break under management, oldest first.
func (s *Store) ListBreaks(ctx context.Context) ([]Break, error) {
	const q = `SELECT external_txn_id, root_cause, confidence, regulated, status, proposed_rule, rationale, created_rule_id, created_at, updated_at
FROM agent.agent_break
ORDER BY created_at ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var breaks []Break
	for rows.Next() {
		var (
			b        Break
			cause    string
			ruleJSON []byte
			crid     sql.NullString
		)
		if err := rows.Scan(
			&b.Classification.ExternalTxnID,
			&cause,
			&b.Classification.Confidence,
			&b.Classification.Regulated,
			&b.Status,
			&ruleJSON,
			&b.Classification.Rationale,
			&crid,
			&b.CreatedAt,
			&b.UpdatedAt,
		); err != nil {
			return nil, err
		}
		b.Classification.RootCause = model.RootCause(cause)
		if crid.Valid {
			b.CreatedRuleID = crid.String
		}
		if len(ruleJSON) > 0 {
			// Per-row isolation (finding m-5): a single row whose proposed_rule
			// column fails to unmarshal (only reachable via out-of-band DB
			// tampering, since this store is the sole writer and always persists
			// a well-formed rule) must not fail the entire listing and blank the
			// HITL status page. Leave that one row's ProposedRule nil and keep
			// every healthy row readable.
			var rule blnk.MatchingRule
			if err := json.Unmarshal(ruleJSON, &rule); err == nil {
				b.Classification.ProposedRule = &rule
			}
		}
		breaks = append(breaks, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return breaks, nil
}

// EnqueueHITL adds (or refreshes) a break in the human-review queue with the
// given reason.
func (s *Store) EnqueueHITL(ctx context.Context, externalTxnID, reason string) error {
	return enqueueHITL(ctx, s.db, externalTxnID, reason)
}

// EnqueueHITLTx enqueues a break for human review inside the caller's
// transaction so the queue entry commits atomically with the break's queued
// status and its escalation audit event (findings C-2 / M-4).
func (s *Store) EnqueueHITLTx(ctx context.Context, tx *sql.Tx, externalTxnID, reason string) error {
	return enqueueHITL(ctx, tx, externalTxnID, reason)
}

const enqueueHITLSQL = `INSERT INTO agent.agent_hitl_queue (external_txn_id, reason)
VALUES ($1, $2)
ON CONFLICT (external_txn_id) DO UPDATE SET reason = EXCLUDED.reason, enqueued_at = now()`

// enqueueHITL is the single definition of the HITL-enqueue write, shared by
// EnqueueHITL and EnqueueHITLTx via sqlExecer.
func enqueueHITL(ctx context.Context, ex sqlExecer, externalTxnID, reason string) error {
	_, err := ex.ExecContext(ctx, enqueueHITLSQL, externalTxnID, reason)
	return err
}

// ListHITL returns the breaks currently awaiting a human decision, oldest
// first.
func (s *Store) ListHITL(ctx context.Context) ([]HITLItem, error) {
	const q = `SELECT external_txn_id, reason, enqueued_at
FROM agent.agent_hitl_queue
ORDER BY enqueued_at ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []HITLItem
	for rows.Next() {
		var it HITLItem
		if err := rows.Scan(&it.ExternalTxnID, &it.Reason, &it.EnqueuedAt); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// DequeueHITL removes a break from the human-review queue. Removing a break
// that is not queued is a no-op (not an error), so HITL draining is idempotent.
func (s *Store) DequeueHITL(ctx context.Context, externalTxnID string) error {
	const q = `DELETE FROM agent.agent_hitl_queue WHERE external_txn_id = $1`
	_, err := s.db.ExecContext(ctx, q, externalTxnID)
	return err
}

// DequeueHITLTx removes a break from the human-review queue inside the caller's
// transaction, so the drain commits atomically with the guarded status change
// and audit append it accompanies (finding M-02). Like DequeueHITL, removing a
// break that is not queued is a no-op (not an error), so draining is idempotent.
func (s *Store) DequeueHITLTx(ctx context.Context, tx *sql.Tx, externalTxnID string) error {
	const q = `DELETE FROM agent.agent_hitl_queue WHERE external_txn_id = $1`
	_, err := tx.ExecContext(ctx, q, externalTxnID)
	return err
}

// InsertAudit appends one event to the append-only audit ledger. Per Rule 5.5
// this is the ONLY write path for agent.agent_audit: the statement is an
// INSERT, and this package never issues an UPDATE or DELETE against that table.
func (s *Store) InsertAudit(ctx context.Context, e model.AuditEvent) error {
	return insertAudit(ctx, s.db, e)
}

// InsertAuditTx appends one event to the append-only audit ledger inside the
// caller's transaction. It makes *Store satisfy audit.TxSink, enabling
// audit.Writer.RecordTx so a break's state transition and its audit event
// commit atomically (Rule 5.3 / findings C-1, C-2, M-2, M-4). Like InsertAudit
// this is INSERT-only, preserving append-only immutability (Rule 5.5).
func (s *Store) InsertAuditTx(ctx context.Context, tx *sql.Tx, e model.AuditEvent) error {
	return insertAudit(ctx, tx, e)
}

const insertAuditSQL = `INSERT INTO agent.agent_audit
	(event_id, external_txn_id, actor, action, "timestamp", rationale, confidence, provenance)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

// insertAudit is the single definition of the append-only audit write, shared
// by InsertAudit and InsertAuditTx via sqlExecer. It is INSERT-only: this
// package NEVER issues an UPDATE or DELETE against agent.agent_audit (Rule 5.5),
// an invariant additionally enforced at the database boundary by a trigger.
func insertAudit(ctx context.Context, ex sqlExecer, e model.AuditEvent) error {
	prov, err := json.Marshal(e.Provenance)
	if err != nil {
		return err
	}
	_, err = ex.ExecContext(ctx, insertAuditSQL,
		e.EventID,
		e.ExternalTxnID,
		e.Actor,
		e.Action,
		e.Timestamp,
		e.Rationale,
		e.Confidence,
		string(prov),
	)
	return err
}

// ListAudit returns the full append-only audit trail, oldest first. SELECT is
// permitted on agent_audit (Rule 5.5 forbids only UPDATE/DELETE); the HITL
// status page renders this trail.
func (s *Store) ListAudit(ctx context.Context) ([]model.AuditEvent, error) {
	const q = `SELECT event_id, external_txn_id, actor, action, "timestamp", rationale, confidence, provenance
FROM agent.agent_audit
ORDER BY "timestamp" ASC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var events []model.AuditEvent
	for rows.Next() {
		var (
			e        model.AuditEvent
			ts       time.Time
			provJSON []byte
		)
		if err := rows.Scan(
			&e.EventID,
			&e.ExternalTxnID,
			&e.Actor,
			&e.Action,
			&ts,
			&e.Rationale,
			&e.Confidence,
			&provJSON,
		); err != nil {
			return nil, err
		}
		e.Timestamp = ts.UTC().Format(time.RFC3339Nano)
		if len(provJSON) > 0 {
			if err := json.Unmarshal(provJSON, &e.Provenance); err != nil {
				return nil, err
			}
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// CountAuditByAction returns how many append-only audit events a break has
// recorded for the given action. It is a read only (SELECT COUNT(*); Rule 5.5
// forbids only UPDATE/DELETE against agent_audit, never a SELECT) and issues no
// write, preserving append-only immutability.
//
// It backs the remediator's fail-closed resume guard (SEAM-MIN-1): a
// rule_proposed event is durably recorded BEFORE the Blnk CreateMatchingRule
// POST. On resume, a positive rule_proposed count for a break that still has no
// persisted created-rule id means a prior attempt crashed in the window between
// the POST and the atomic created-rule commit, so Blnk may hold an orphaned
// matching rule. Blnk exposes no list/get matching-rule route (only
// POST/PUT/DELETE), so the orphan cannot be reconciled over the native HTTP
// surface (Rule 5.1); the remediator therefore escalates to HITL rather than
// re-creating a duplicate rule.
func (s *Store) CountAuditByAction(ctx context.Context, externalTxnID, action string) (int, error) {
	const q = `SELECT count(*) FROM agent.agent_audit WHERE external_txn_id = $1 AND action = $2`
	var n int
	if err := s.db.QueryRowContext(ctx, q, externalTxnID, action).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
