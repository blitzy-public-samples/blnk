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

// Break is one external break under management as persisted in
// agent.agent_break. It carries the classifier's verdict plus the agent's
// current lifecycle status and timestamps.
type Break struct {
	Classification model.BreakClassification `json:"classification"`
	Status         string                    `json:"status"`
	CreatedAt      time.Time                 `json:"created_at"`
	UpdatedAt      time.Time                 `json:"updated_at"`
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

// Migrate applies the embedded schema. It executes ONLY the Up section of
// schema.sql, so the destructive Down section is never run. The DDL uses
// IF NOT EXISTS throughout, making Migrate idempotent and safe to call on every
// boot.
func (s *Store) Migrate(ctx context.Context) error {
	up := upSection(schemaSQL)
	if strings.TrimSpace(up) == "" {
		return errors.New("store: migration has no Up section")
	}
	if _, err := s.db.ExecContext(ctx, up); err != nil {
		return err
	}
	return nil
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
func (s *Store) UpsertBreak(ctx context.Context, c model.BreakClassification, status string) error {
	var rule any
	if c.ProposedRule != nil {
		encoded, err := json.Marshal(c.ProposedRule)
		if err != nil {
			return err
		}
		rule = string(encoded)
	}
	const q = `INSERT INTO agent.agent_break
	(external_txn_id, root_cause, confidence, regulated, status, proposed_rule)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (external_txn_id) DO UPDATE SET
	root_cause = EXCLUDED.root_cause,
	confidence = EXCLUDED.confidence,
	regulated = EXCLUDED.regulated,
	status = EXCLUDED.status,
	proposed_rule = EXCLUDED.proposed_rule,
	updated_at = now()`
	_, err := s.db.ExecContext(ctx, q,
		c.ExternalTxnID,
		string(c.RootCause),
		c.Confidence,
		c.Regulated,
		status,
		rule,
	)
	return err
}

// SetBreakStatus updates the lifecycle status of an existing break, returning
// ErrNotFound when no row matches externalTxnID.
func (s *Store) SetBreakStatus(ctx context.Context, externalTxnID, status string) error {
	const q = `UPDATE agent.agent_break SET status = $2, updated_at = now() WHERE external_txn_id = $1`
	res, err := s.db.ExecContext(ctx, q, externalTxnID, status)
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

// ListBreaks returns every break under management, oldest first.
func (s *Store) ListBreaks(ctx context.Context) ([]Break, error) {
	const q = `SELECT external_txn_id, root_cause, confidence, regulated, status, proposed_rule, created_at, updated_at
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
		)
		if err := rows.Scan(
			&b.Classification.ExternalTxnID,
			&cause,
			&b.Classification.Confidence,
			&b.Classification.Regulated,
			&b.Status,
			&ruleJSON,
			&b.CreatedAt,
			&b.UpdatedAt,
		); err != nil {
			return nil, err
		}
		b.Classification.RootCause = model.RootCause(cause)
		if len(ruleJSON) > 0 {
			var rule blnk.MatchingRule
			if err := json.Unmarshal(ruleJSON, &rule); err != nil {
				return nil, err
			}
			b.Classification.ProposedRule = &rule
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
	const q = `INSERT INTO agent.agent_hitl_queue (external_txn_id, reason)
VALUES ($1, $2)
ON CONFLICT (external_txn_id) DO UPDATE SET reason = EXCLUDED.reason, enqueued_at = now()`
	_, err := s.db.ExecContext(ctx, q, externalTxnID, reason)
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

// InsertAudit appends one event to the append-only audit ledger. Per Rule 5.5
// this is the ONLY write path for agent.agent_audit: the statement is an
// INSERT, and this package never issues an UPDATE or DELETE against that table.
func (s *Store) InsertAudit(ctx context.Context, e model.AuditEvent) error {
	prov, err := json.Marshal(e.Provenance)
	if err != nil {
		return err
	}
	const q = `INSERT INTO agent.agent_audit
	(event_id, external_txn_id, actor, action, "timestamp", rationale, confidence, provenance)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err = s.db.ExecContext(ctx, q,
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
