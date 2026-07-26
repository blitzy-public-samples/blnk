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
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	// lib/pq registers the "postgres" database/sql driver via its init side
	// effect and provides pq.Array for binding a text[] parameter (used by the
	// database-side current-run aggregation queries). It is the ONLY external
	// dependency of this package.
	"github.com/lib/pq"

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

// ErrLeaseNotHeld is returned by ReleaseBreak (and Claim-release paths) when the
// caller tries to release a processing lease it does not currently own — either
// because the row is unclaimed, was claimed by a different owner, or the lease
// already expired and was reclaimed. It is part of the durable, cross-instance
// concurrency control that replaces the process-local mutex (finding M-15).
var ErrLeaseNotHeld = errors.New("store: processing lease is not held by this owner")

// Pagination bounds for the list endpoints (finding M-14). Every list query is
// bounded so an unbounded audit/break history can never be streamed into memory
// in a single call. A caller may request a specific page via the *Page methods;
// the zero Page (or any Limit <= 0) yields defaultPageLimit rows, and any
// requested Limit is capped at maxPageLimit.
const (
	defaultPageLimit = 500
	maxPageLimit     = 5000
)

// Page is a bounded pagination request. Limit <= 0 selects defaultPageLimit;
// a Limit above maxPageLimit is clamped to maxPageLimit. Offset < 0 is treated
// as 0. Normalize applies these rules so no caller can request an unbounded or
// negative window.
type Page struct {
	Limit  int
	Offset int
}

// Normalize clamps a Page to the enforced bounds. It is total: every input maps
// to a valid (Limit in [1,maxPageLimit], Offset >= 0) window. It is exported so
// the HITL surfaces can report the EFFECTIVE limit/offset actually applied and
// compute continuation metadata (next offset) consistently with the bounds the
// store enforces (finding F06).
func (p Page) Normalize() Page {
	out := p
	if out.Limit <= 0 {
		out.Limit = defaultPageLimit
	}
	if out.Limit > maxPageLimit {
		out.Limit = maxPageLimit
	}
	if out.Offset < 0 {
		out.Offset = 0
	}
	return out
}

// defaultLeaseTTL is the default duration a processing lease is held before it
// is considered expired and eligible for reclamation by another instance. It
// bounds how long a crashed instance can block reprocessing of a break.
const defaultLeaseTTL = 2 * time.Minute

// migrateLockTimeout bounds how long Migrate waits to acquire the boot-migration
// advisory lock before giving up (finding M-16). A try-lock retry loop honors
// both this ceiling and the caller's context deadline, so a stuck peer holding
// the lock can never make Migrate block indefinitely.
const migrateLockTimeout = 30 * time.Second

// migrateLockRetryInterval is how long the try-lock loop sleeps between failed
// acquisition attempts.
const migrateLockRetryInterval = 100 * time.Millisecond

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
	CreatedRuleID string `json:"created_rule_id,omitempty"`
	// ResolvedReconID is the confirming Blnk dry-run reconciliation id that
	// proves this break cleared — the durable clearance proof (Rule 5.3). It is
	// populated for an auto-resolved break (MarkResolvedTx) and for a re_driven
	// break whose re_drive Blnk-confirmed clearance (MarkReDrivenClearedFromQueuedTx,
	// finding F16); it is empty for every non-cleared status. Surfacing it here
	// lets the HITL status page display the exact proof next to a cleared break
	// rather than leaving a re_driven row with a blank proof.
	ResolvedReconID string    `json:"resolved_recon_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
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

// RuleOutboxItem is one durable compensation-ledger entry from
// agent.agent_rule_outbox (findings M-12 / M-11): a Blnk matching rule the agent
// created that must be either confirmed (the break resolved) or compensated (the
// rule deleted from Blnk). ListPendingRuleOutbox returns the 'pending' rows a
// crashed attempt left behind so the remediator can compensate them on startup.
type RuleOutboxItem struct {
	ID            int64     `json:"id"`
	ExternalTxnID string    `json:"external_txn_id"`
	RuleID        string    `json:"rule_id"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
	LastError     string    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Rule-outbox lifecycle status values (findings M-12 / M-11). They mirror the
// agent_rule_outbox_status_domain CHECK constraint in schema.sql.
const (
	// OutboxPending marks a created Blnk rule whose owning remediation attempt
	// has not yet confirmed (break resolved) or compensated (rule deleted). A
	// pending row surviving a restart indicates a crash mid-attempt.
	OutboxPending = "pending"
	// OutboxConfirmed marks a rule that legitimately cleared its break: the
	// break reached auto-resolved in the same transaction that confirmed it.
	OutboxConfirmed = "confirmed"
	// OutboxCompensated marks a rule the agent successfully deleted from Blnk
	// after its attempt aborted, leaving no orphan behind.
	OutboxCompensated = "compensated"
	// OutboxCompensationFailed marks a rule whose deletion from Blnk repeatedly
	// failed; last_error/attempts capture why, and the failure is also audited.
	OutboxCompensationFailed = "compensation_failed"
)

const (
	// RunRunning marks an agent_run row whose pipeline has begun but not yet
	// finished. A running row surviving a restart indicates a crash mid-pipeline
	// (finding M-15); BeginRun hands the original run_id back so the retry reuses
	// the same scoped id set instead of forking a duplicate history.
	RunRunning = "running"
	// RunCompleted marks a fixture whose pipeline finished. BeginRun reports
	// alreadyCompleted for such a fixture so a serve restart rebuilds the summary
	// from the recorded run rather than reprocessing the fixture (finding M-15).
	RunCompleted = "completed"
)

// Store is the sole PostgreSQL gateway for the recon-agent. Every statement it
// issues targets the agent schema; no blnk.* table is ever read or written.
type Store struct {
	db *sql.DB
	// dsn is the connection string this Store was opened with. It is retained
	// SOLELY so error surfaces that could echo it (lib/pq forces a lazy connect
	// on the first query and Go's net/url parse error embeds the RAW DSN — see
	// sanitizeDBError) can be scrubbed of any embedded credential before the
	// error propagates to a log (finding F07). It is never itself logged.
	dsn string
}

// dsnURLUserinfoPassword matches the password segment of a URL-form DSN's
// userinfo (scheme://user:PASSWORD@host...). It is used only to extract/redact
// the secret when net/url cannot parse the DSN (e.g. an invalid port), which is
// exactly the malformed-DSN case whose parse error would otherwise leak the
// password verbatim (finding F07).
var dsnURLUserinfoPassword = regexp.MustCompile(`://[^:@/]+:([^@/\s]+)@`)

// dsnKeywordPassword matches the value of a libpq keyword/value DSN's password
// field (host=... password=SECRET ...), quoted or bare.
var dsnKeywordPassword = regexp.MustCompile(`(?i)password\s*=\s*('[^']*'|"[^"]*"|[^\s'"]+)`)

// redactDSN returns a display-safe rendering of dsn with any embedded password
// masked. It first tries net/url (the common URL form) and, whether or not that
// succeeds, additionally applies the regex fallbacks so a DSN that net/url
// cannot parse — the precise malformed case that leaks in finding F07 — is
// still redacted. An empty dsn yields "(empty)".
func redactDSN(dsn string) string {
	if strings.TrimSpace(dsn) == "" {
		return "(empty)"
	}
	out := dsn
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if _, hasPw := u.User.Password(); hasPw {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
			out = u.String()
		}
	}
	// Replace the captured userinfo password with the mask (handles the malformed
	// DSN that net/url could not parse above).
	out = dsnURLUserinfoPassword.ReplaceAllStringFunc(out, func(m string) string {
		sub := dsnURLUserinfoPassword.FindStringSubmatch(m)
		if len(sub) == 2 {
			return strings.Replace(m, sub[1], "xxxxx", 1)
		}
		return m
	})
	out = dsnKeywordPassword.ReplaceAllString(out, "password=xxxxx")
	return out
}

// dsnSecrets returns every literal secret token embedded in dsn (the URL
// userinfo password and/or a keyword password value). These exact substrings
// are stripped from any error message that might echo them (finding F07),
// which defends against error text that reproduces the raw DSN verbatim even
// when the DSN itself is malformed and cannot be parsed structurally.
func dsnSecrets(dsn string) []string {
	var secrets []string
	seen := map[string]struct{}{}
	add := func(s string) {
		s = strings.Trim(s, `'"`)
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		secrets = append(secrets, s)
	}
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok {
			add(pw)
		}
	}
	if m := dsnURLUserinfoPassword.FindStringSubmatch(dsn); len(m) == 2 {
		add(m[1])
	}
	if m := dsnKeywordPassword.FindStringSubmatch(dsn); len(m) == 2 {
		add(m[1])
	}
	return secrets
}

// sanitizeDBError returns an error whose message is guaranteed to contain no
// credential embedded in dsn (finding F07). It replaces every secret token from
// the DSN with a mask and, if the raw DSN appears verbatim, replaces it with its
// redacted form. A brand-new error is returned (the original is NOT wrapped)
// because lib/pq / net/url attach the leaking text to the error's own message,
// so preserving the chain via %w would re-expose the secret through Error().
// nil in yields nil out.
func sanitizeDBError(err error, dsn string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	for _, secret := range dsnSecrets(dsn) {
		msg = strings.ReplaceAll(msg, secret, "xxxxx")
	}
	if dsn != "" && strings.Contains(msg, dsn) {
		msg = strings.ReplaceAll(msg, dsn, redactDSN(dsn))
	}
	return errors.New(msg)
}

// New opens a Store against the given PostgreSQL DSN (from AGENT_DATABASE_URL)
// using the lib/pq "postgres" driver. The connection is opened lazily; call
// Migrate (or any method) to force a real connection. Any error is scrubbed of
// DSN credentials before it is returned (finding F07).
func New(dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("store: empty DSN")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, sanitizeDBError(err, dsn)
	}
	return &Store{db: db, dsn: dsn}, nil
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
	// lib/pq forces the lazy connection here; a malformed DSN surfaces a
	// net/url parse error that embeds the RAW DSN (password included). Scrub it
	// before it reaches /healthz logging (finding F07).
	return sanitizeDBError(s.db.PingContext(ctx), s.dsn)
}

// SelectOne verifies the database can actually SERVE a query, not merely accept
// a TCP connection, by executing a real `SELECT 1` round-trip (finding F01). A
// container orchestrator's TCP/`pg_isready` gate can report PostgreSQL "healthy"
// during the window between accepting connections and being able to run queries
// (startup, recovery, a brief reset); running the boot migration in that window
// fails with an opaque "connection reset". cmd/main.go calls SelectOne in a
// bounded retry BEFORE migrating so that transient window is waited out instead
// of turning into a fatal boot error. Any driver error is scrubbed of DSN
// secrets first (finding F07).
func (s *Store) SelectOne(ctx context.Context) error {
	if s.db == nil {
		return errors.New("store: not initialized")
	}
	var one int
	if err := s.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		return sanitizeDBError(err, s.dsn)
	}
	if one != 1 {
		return fmt.Errorf("store: readiness probe returned unexpected value %d", one)
	}
	return nil
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
	return s.migrate(ctx, "")
}

// MigrateAndGrant applies the schema AS THE OWNER/MIGRATOR role this Store is
// connected as, and then grants the restricted RUNTIME role (identified by the
// username embedded in runtimeDSN) exactly the least privileges it needs
// (finding F05, Rule 5.5): USAGE on the agent schema; SELECT+INSERT on the
// append-only agent_audit (NO update/delete/truncate); the specific DML each
// mutable table requires; and USAGE on the outbox sequence. It deliberately
// grants NO ownership, NO database CREATE, and NO privilege that would let the
// runtime role disable the append-only trigger or mutate/drop audit history.
//
// This is the two-role production path: cmd/main.go connects this Store as the
// migrator (AGENT_MIGRATE_DATABASE_URL) purely to run MigrateAndGrant, while the
// long-lived application Store connects as the runtime role (AGENT_DATABASE_URL).
// When runtimeDSN is empty (single-role dev/test) it degrades to a plain
// Migrate with no cross-role grants. Granting to a role identical to the
// migrator (a misconfiguration) is harmless (a grant to self).
func (s *Store) MigrateAndGrant(ctx context.Context, runtimeDSN string) error {
	return s.migrate(ctx, runtimeRoleFromDSN(runtimeDSN))
}

// migrate runs the Up section (and, when runtimeRole is non-empty, the runtime
// privilege grants) inside ONE advisory-locked transaction, then scrubs any DSN
// credential from the returned error (finding F07).
func (s *Store) migrate(ctx context.Context, runtimeRole string) error {
	up := upSection(schemaSQL)
	if strings.TrimSpace(up) == "" {
		return errors.New("store: migration has no Up section")
	}
	// M-16: bound the wait for the migration lock. If the caller's context has
	// no deadline, impose migrateLockTimeout so that a peer holding the lock (or
	// a stuck migration) can never make Migrate block forever.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, migrateLockTimeout)
		defer cancel()
	}
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		// Acquire the transaction-scoped advisory lock with a bounded try-lock
		// retry loop rather than the blocking pg_advisory_xact_lock, so the wait
		// is capped by the context deadline above (finding M-16).
		if err := acquireMigrateLock(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, up); err != nil {
			return err
		}
		// Least-privilege grants to the restricted runtime role (finding F05).
		// Runs in the SAME transaction as the DDL so the schema and its grants
		// commit atomically; GRANT is transactional in PostgreSQL.
		if runtimeRole != "" {
			if err := grantRuntimePrivileges(ctx, tx, runtimeRole); err != nil {
				return err
			}
		}
		return nil
	})
	return sanitizeDBError(err, s.dsn)
}

// runtimeRoleFromDSN extracts the login role name from a PostgreSQL DSN so the
// migrator can grant privileges to the runtime role without a separate config
// field. It understands both the URL form (postgres://ROLE:pw@host/db) and the
// libpq keyword form (user=ROLE ...). It returns "" when no role can be
// determined (in which case the caller performs no cross-role grants).
func runtimeRoleFromDSN(dsn string) string {
	if strings.TrimSpace(dsn) == "" {
		return ""
	}
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if name := u.User.Username(); name != "" {
			return name
		}
	}
	if m := regexp.MustCompile(`(?i)\buser\s*=\s*('[^']*'|"[^"]*"|[^\s'"]+)`).FindStringSubmatch(dsn); len(m) == 2 {
		return strings.Trim(m[1], `'"`)
	}
	return ""
}

// grantRuntimePrivileges issues the exact least-privilege GRANTs the runtime
// role needs against the migrator-owned agent objects (finding F05). The role
// name is interpolated via pq.QuoteIdentifier so it cannot be used for SQL
// injection. The privilege set mirrors the DML in this package: agent_audit is
// SELECT+INSERT only (append-only, Rule 5.5); agent_break / agent_rule_outbox /
// agent_run get their required UPDATE; agent_hitl_queue gets UPDATE for its
// re-enqueue upsert (INSERT ... ON CONFLICT DO UPDATE, which PostgreSQL checks
// as an UPDATE) plus DELETE for draining; and the outbox's BIGSERIAL sequence
// gets USAGE for nextval on INSERT. No TRUNCATE, no schema/database CREATE, and
// no ownership are granted, so the runtime role can neither disable the
// append-only trigger nor create unrelated objects.
func grantRuntimePrivileges(ctx context.Context, tx *sql.Tx, runtimeRole string) error {
	for _, stmt := range runtimeGrantStatements(runtimeRole) {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("store: grant runtime privileges: %w", err)
		}
	}
	return nil
}

// runtimeGrantStatements returns the exact least-privilege GRANT statements the
// runtime role receives, as a pure (side-effect-free) function so the privilege
// set is unit-testable without a live database (finding F05 regression guard).
// Each statement mirrors the DML this package actually issues per object: the
// privilege set MUST include UPDATE for every table written via an
// "INSERT ... ON CONFLICT DO UPDATE" upsert (agent_break, agent_hitl_queue),
// because PostgreSQL checks the DO UPDATE arm as an UPDATE — omitting it makes
// the upsert fail at runtime with "permission denied". agent_audit stays
// SELECT+INSERT only to preserve append-only immutability (Rule 5.5).
func runtimeGrantStatements(runtimeRole string) []string {
	role := pq.QuoteIdentifier(runtimeRole)
	return []string{
		"GRANT USAGE ON SCHEMA agent TO " + role,
		"GRANT SELECT, INSERT ON agent.agent_audit TO " + role,
		"GRANT SELECT, INSERT, UPDATE ON agent.agent_break TO " + role,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON agent.agent_hitl_queue TO " + role,
		"GRANT SELECT, INSERT, UPDATE ON agent.agent_rule_outbox TO " + role,
		"GRANT USAGE ON SEQUENCE agent.agent_rule_outbox_id_seq TO " + role,
		"GRANT SELECT, INSERT, UPDATE ON agent.agent_run TO " + role,
	}
}

// acquireMigrateLock takes the transaction-scoped advisory lock, retrying on
// contention until it succeeds or ctx is done (finding M-16). It uses the
// NON-blocking pg_try_advisory_xact_lock so a stuck holder cannot make this
// block past the caller's deadline; the lock releases automatically on the
// enclosing transaction's COMMIT/ROLLBACK. Retrying in the same transaction is
// safe because the lock is not yet held by this backend until the call returns
// true.
func acquireMigrateLock(ctx context.Context, tx *sql.Tx) error {
	for {
		var got bool
		if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", migrateAdvisoryLockKey).Scan(&got); err != nil {
			return err
		}
		if got {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("store: timed out acquiring migration advisory lock: %w", ctx.Err())
		case <-time.After(migrateLockRetryInterval):
		}
	}
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
	 amount, currency, reference, description, txn_date, source, upload_id, main_recon_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
	source = COALESCE(EXCLUDED.source, agent.agent_break.source),
	upload_id = COALESCE(EXCLUDED.upload_id, agent.agent_break.upload_id),
	main_recon_id = COALESCE(EXCLUDED.main_recon_id, agent.agent_break.main_recon_id),
	status_version = agent.agent_break.status_version + 1,
	updated_at = now()`

// nullIfEmpty returns SQL NULL (nil) for an empty/whitespace-only string, else
// the string itself. It keeps optional provenance columns NULL rather than
// storing empty strings, so the COALESCE-on-conflict logic in upsertBreakSQL
// can preserve a previously-persisted value across a later upsert that carries
// no provenance (e.g. a crash-resume re-upsert).
func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// upsertBreak is the single definition of the break-upsert write, shared by the
// autocommit UpsertBreak and the transactional UpsertBreakTx via sqlExecer. The
// txn argument carries the matchable fields of the original external transaction
// (amount/currency/reference/description/date) AND its source label; they are
// persisted so a queued break can be re-driven from the HITL surface without
// reading any blnk.* table (finding L2/M-13, Rule 5.1). prov supplies the
// break's durable correlation identity — the upload batch and the batch
// reconciliation run that surfaced it (finding M-13). txn_date and the optional
// provenance columns are stored as SQL NULL when empty, so the columns
// faithfully round-trip absence and a later upsert cannot wipe a value a prior
// one recorded. Every ON CONFLICT update bumps status_version (finding M-15) so
// concurrent processors observe a monotonically-advancing optimistic counter.
func upsertBreak(ctx context.Context, ex sqlExecer, c model.BreakClassification, txn blnk.ExternalTransaction, prov model.Provenance, status string) error {
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
		nullIfEmpty(txn.Source),
		nullIfEmpty(prov.UploadID),
		nullIfEmpty(prov.MainReconID),
	)
	return err
}

// UpsertBreak inserts or updates a break in autocommit mode. txn supplies the
// original transaction's matchable fields and source; prov supplies the upload
// and main-reconciliation correlation identity persisted alongside the verdict.
func (s *Store) UpsertBreak(ctx context.Context, c model.BreakClassification, txn blnk.ExternalTransaction, prov model.Provenance, status string) error {
	return upsertBreak(ctx, s.db, c, txn, prov, status)
}

// UpsertBreakTx inserts or updates a break inside the caller's transaction so it
// commits atomically with the accompanying audit event. txn supplies the
// original transaction's matchable fields and source; prov supplies the upload
// and main-reconciliation correlation identity persisted alongside the verdict.
func (s *Store) UpsertBreakTx(ctx context.Context, tx *sql.Tx, c model.BreakClassification, txn blnk.ExternalTransaction, prov model.Provenance, status string) error {
	return upsertBreak(ctx, tx, c, txn, prov, status)
}

const setBreakStatusSQL = `UPDATE agent.agent_break SET status = $2, status_version = status_version + 1, updated_at = now() WHERE external_txn_id = $1`

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

const markResolvedSQL = `UPDATE agent.agent_break
SET status = 'auto-resolved', resolved_recon_id = $2, status_version = status_version + 1, updated_at = now()
WHERE external_txn_id = $1`

// MarkResolvedTx transitions a break to the auto-resolved terminal state inside
// the caller's transaction, durably recording the confirming Blnk dry-run
// reconciliation id (Rule 5.3, finding M-13) that proves clearance. The
// reconciliation id MUST be non-empty: the agent_break_resolved_proof CHECK
// constraint rejects an auto-resolved row without it, and the remediator only
// ever calls this with a proven ClearanceProof reconciliation id. It bumps
// status_version (finding M-15) and returns ErrNotFound when no row matches.
// Using this dedicated transition (rather than SetBreakStatusTx) guarantees the
// durable resolved-proof column is populated atomically with the status change
// and its accompanying resolved audit event.
func (s *Store) MarkResolvedTx(ctx context.Context, tx *sql.Tx, externalTxnID, reconID string) error {
	if strings.TrimSpace(reconID) == "" {
		return fmt.Errorf("store: MarkResolvedTx requires a non-empty confirming reconciliation id")
	}
	res, err := tx.ExecContext(ctx, markResolvedSQL, externalTxnID, reconID)
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

// stampMainReconIDSQL records the run's initial (batch) reconciliation id on a
// set of breaks, but ONLY where main_recon_id is still blank. The
// COALESCE-style guard (NULL or all-whitespace) makes the write idempotent and
// non-destructive: re-running it never overwrites an id already recorded, and it
// only ever FILLS the provenance the initial reconciliation establishes (finding
// F02). It intentionally does NOT bump status_version — main_recon_id is
// provenance metadata, not a lifecycle transition — so it cannot disturb the
// M-15 optimistic-concurrency accounting.
const stampMainReconIDSQL = `UPDATE agent.agent_break
SET main_recon_id = $1, updated_at = now()
WHERE external_txn_id = ANY($2)
  AND (main_recon_id IS NULL OR length(btrim(main_recon_id)) = 0)`

// StampMainReconID stamps mainReconID as the main_recon_id of every break in
// externalTxnIDs whose column is still blank (see stampMainReconIDSQL). It is an
// autocommit write called once per run after the initial reconciliation
// completes (finding F02), so every break — auto-resolved or escalated — points
// at the batch reconciliation that surfaced it. An empty id or empty id list is
// a no-op (nothing to stamp), never an error.
func (s *Store) StampMainReconID(ctx context.Context, mainReconID string, externalTxnIDs []string) error {
	if strings.TrimSpace(mainReconID) == "" || len(externalTxnIDs) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, stampMainReconIDSQL, mainReconID, pq.Array(externalTxnIDs))
	return err
}

const setBreakStatusFromQueuedSQL = `UPDATE agent.agent_break
SET status = $2, status_version = status_version + 1, updated_at = now()
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

const markReDrivenClearedFromQueuedSQL = `UPDATE agent.agent_break
SET status = 're_driven', resolved_recon_id = $2, status_version = status_version + 1, updated_at = now()
WHERE external_txn_id = $1 AND status = 'queued'`

// MarkReDrivenClearedFromQueuedTx transitions a queued break to the re_driven
// terminal state AND durably stamps the confirming Blnk dry-run reconciliation
// id into resolved_recon_id — atomically, inside the caller's transaction
// (finding F16, Rule 5.3). It is to a cleared re_drive what MarkResolvedTx is to
// an auto-resolution: it makes the durable clearance proof a first-class column
// on the break row (not merely a value buried in one audit event's provenance),
// so a reviewer or query can read WHICH dry-run cleared a re_driven break, and
// the agent_break_resolved_proof CHECK can require that proof for a re_driven
// row exactly as it does for an auto-resolved one.
//
// Like SetBreakStatusFromQueuedTx it is a queued-only compare-and-set: it
// transitions ONLY a currently-queued break (findings M-01/m-04) and returns
// ErrNotQueued for an already-decided break and ErrNotFound for a missing one,
// so a replayed or concurrent re_drive can never overwrite a terminal status
// nor mark a non-queued break cleared. The reconciliation id MUST be non-empty:
// the re_drive handler only ever calls this after a ProbeBreak returned a real
// confirming id, and the CHECK constraint refuses a blank proof.
func (s *Store) MarkReDrivenClearedFromQueuedTx(ctx context.Context, tx *sql.Tx, externalTxnID, reconID string) error {
	if strings.TrimSpace(reconID) == "" {
		return fmt.Errorf("store: MarkReDrivenClearedFromQueuedTx requires a non-empty confirming reconciliation id")
	}
	res, err := tx.ExecContext(ctx, markReDrivenClearedFromQueuedSQL, externalTxnID, reconID)
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
	// break that exists but is no longer queued (ErrNotQueued), mirroring
	// SetBreakStatusFromQueuedTx so the HITL handler maps them to 404 vs 409.
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

const claimBreakSQL = `UPDATE agent.agent_break
SET claimed_by = $2, lease_expires_at = $3, updated_at = now()
WHERE external_txn_id = $1
  AND (claimed_by IS NULL OR claimed_by = $2 OR lease_expires_at IS NULL OR lease_expires_at < now())`

// ClaimBreak acquires (or renews) a durable processing lease on an existing
// break row for owner, valid for ttl (finding M-15). It is a cross-instance
// mutual-exclusion primitive: at most one owner holds a break's lease at a time,
// so at most one agent instance performs that break's external remediation
// (rule creation / dry-run probe) — something a process-local mutex cannot
// guarantee. A lease is grantable when the row is unclaimed, already owned by
// the SAME owner (renewal — idempotent), or the prior lease has expired (crash
// recovery). It returns:
//   - (true, nil)            when the lease was granted or renewed,
//   - (false, nil)           when another live owner currently holds the lease,
//   - (false, ErrNotFound)   when no such break exists.
//
// ttl <= 0 falls back to defaultLeaseTTL. The expiry is computed from the
// application clock and stored as an absolute timestamp, so a crashed holder's
// lease becomes reclaimable after ttl without any background sweeper.
func (s *Store) ClaimBreak(ctx context.Context, externalTxnID, owner string, ttl time.Duration) (bool, error) {
	if strings.TrimSpace(owner) == "" {
		return false, fmt.Errorf("store: ClaimBreak requires a non-empty owner")
	}
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	res, err := s.db.ExecContext(ctx, claimBreakSQL, externalTxnID, owner, time.Now().Add(ttl))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 1 {
		return true, nil
	}
	// Zero rows updated: distinguish a missing break from one currently held by
	// another live owner, so the caller can tell "does not exist" from "busy".
	var exists bool
	qerr := s.db.QueryRowContext(ctx,
		`SELECT true FROM agent.agent_break WHERE external_txn_id = $1`, externalTxnID).Scan(&exists)
	if errors.Is(qerr, sql.ErrNoRows) {
		return false, ErrNotFound
	}
	if qerr != nil {
		return false, qerr
	}
	return false, nil // exists, but a live owner holds the lease
}

const releaseBreakSQL = `UPDATE agent.agent_break
SET claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
WHERE external_txn_id = $1 AND claimed_by = $2`

// ReleaseBreak releases a processing lease held by owner (finding M-15),
// letting another instance (or a retry) reclaim the break. It returns
// ErrLeaseNotHeld when the row is not currently claimed by owner — whether it
// was never claimed, already released/reclaimed, held by someone else, or does
// not exist — so a caller can never mistakenly believe it released a lease it
// did not own.
func (s *Store) ReleaseBreak(ctx context.Context, externalTxnID, owner string) error {
	res, err := s.db.ExecContext(ctx, releaseBreakSQL, externalTxnID, owner)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseNotHeld
	}
	return nil
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
// persisted when the break was first managed. The persisted source label is
// reloaded into txn.Source (finding M-13): Blnk's matching engine keys on the
// source, so omitting it would submit a mislabeled transaction and silently
// change which internal candidates a re_drive can match. The returned
// transaction's ID is the break's external id; the caller (ProbeBreak)
// substitutes a fresh ephemeral id before submission so a re_drive never
// collides on Blnk's external_transactions primary key (finding F3).
func (s *Store) LoadBreakTxn(ctx context.Context, externalTxnID string) (txn blnk.ExternalTransaction, createdRuleID string, found bool, err error) {
	const q = `SELECT amount, currency, reference, description, txn_date, source, created_rule_id
FROM agent.agent_break
WHERE external_txn_id = $1`
	var (
		amount   sql.NullFloat64
		currency sql.NullString
		ref      sql.NullString
		desc     sql.NullString
		date     sql.NullTime
		source   sql.NullString
		crid     sql.NullString
	)
	row := s.db.QueryRowContext(ctx, q, externalTxnID)
	if err = row.Scan(&amount, &currency, &ref, &desc, &date, &source, &crid); err != nil {
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
	if source.Valid {
		txn.Source = source.String
	}
	if crid.Valid {
		createdRuleID = crid.String
	}
	return txn, createdRuleID, true, nil
}

// breakSelectColumns is the shared column list for every query that scans into a
// Break, so ListBreaks, ListBreaksPage, and ListBreaksForIDs decode identically.
const breakSelectColumns = `external_txn_id, root_cause, confidence, regulated, status, proposed_rule, rationale, created_rule_id, resolved_recon_id, created_at, updated_at`

// scanBreaks decodes a *sql.Rows opened over breakSelectColumns into a slice of
// Break, applying the per-row proposed_rule isolation described on ListBreaks.
// It is shared by every break-listing query so decoding stays consistent.
func scanBreaks(rows *sql.Rows) ([]Break, error) {
	var breaks []Break
	for rows.Next() {
		var (
			b        Break
			cause    string
			ruleJSON []byte
			crid     sql.NullString
			rrid     sql.NullString
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
			&rrid,
			&b.CreatedAt,
			&b.UpdatedAt,
		); err != nil {
			return nil, err
		}
		b.Classification.RootCause = model.RootCause(cause)
		if crid.Valid {
			b.CreatedRuleID = crid.String
		}
		if rrid.Valid {
			b.ResolvedReconID = rrid.String
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

// ListBreaks returns a bounded page of breaks under management, oldest first. It
// is equivalent to ListBreaksPage(ctx, Page{}) and therefore returns at most
// defaultPageLimit rows (finding M-14): no caller can stream an unbounded break
// history into memory. Use ListBreaksPage to walk further pages.
func (s *Store) ListBreaks(ctx context.Context) ([]Break, error) {
	return s.ListBreaksPage(ctx, Page{})
}

// ListBreaksPage returns one bounded page of breaks, oldest first (finding
// M-14). The page is normalized: Limit defaults to defaultPageLimit and is
// capped at maxPageLimit, and a negative Offset is treated as 0.
func (s *Store) ListBreaksPage(ctx context.Context, page Page) ([]Break, error) {
	p := page.Normalize()
	q := `SELECT ` + breakSelectColumns + `
FROM agent.agent_break
ORDER BY created_at ASC
LIMIT $1 OFFSET $2`
	rows, err := s.db.QueryContext(ctx, q, p.Limit, p.Offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanBreaks(rows)
}

// ListBreaksForIDs returns, in one bounded query, only the break rows whose
// external_txn_id is in ids — the database-side scoping the pipeline summary
// needs so it never lists the entire historical break table only to discard
// every row outside the current run (finding M-14). ids are bound as a single
// text[] parameter via pq.Array. The result is bounded by page like
// ListBreaksPage; callers scoping to a known id set pass Page{Limit: len(ids)}.
func (s *Store) ListBreaksForIDs(ctx context.Context, ids []string, page Page) ([]Break, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	p := page.Normalize()
	q := `SELECT ` + breakSelectColumns + `
FROM agent.agent_break
WHERE external_txn_id = ANY($1)
ORDER BY created_at ASC
LIMIT $2 OFFSET $3`
	rows, err := s.db.QueryContext(ctx, q, pq.Array(ids), p.Limit, p.Offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanBreaks(rows)
}

// CountBreaks returns the total number of breaks under management, database-side
// (finding F06). The HITL surfaces page the break list at defaultPageLimit rows;
// without a total the JSON `/breaks` view and the `/` status page silently
// stopped at the first page and later (e.g. queued) rows were invisible with no
// total, cursor, or next link. Callers pair this count with ListBreaksPage to
// emit continuation metadata (total + next offset) and render traversal
// controls, so every break — including those beyond the first page — is
// discoverable. It is a bounded COUNT(*) that never materializes any row.
func (s *Store) CountBreaks(ctx context.Context) (int, error) {
	const q = `SELECT count(*) FROM agent.agent_break`
	var n int
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CountBreaksByStatusForIDs counts, database-side, how many of the given breaks
// currently hold status — used by the pipeline summary to tally auto-resolved
// breaks for the current run without materializing every row (finding M-14).
func (s *Store) CountBreaksByStatusForIDs(ctx context.Context, ids []string, status string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	const q = `SELECT count(*) FROM agent.agent_break WHERE external_txn_id = ANY($1) AND status = $2`
	var n int
	if err := s.db.QueryRowContext(ctx, q, pq.Array(ids), status).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CountHITLForIDs counts, database-side, how many of the given breaks are
// currently in the human-review queue — the current-run "escalated" tally
// (finding M-14).
func (s *Store) CountHITLForIDs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	const q = `SELECT count(*) FROM agent.agent_hitl_queue WHERE external_txn_id = ANY($1)`
	var n int
	if err := s.db.QueryRowContext(ctx, q, pq.Array(ids)).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// CountAuditForIDs counts, database-side, how many append-only audit events
// belong to the given breaks — the current-run "audit count" (finding M-14).
// It is a SELECT COUNT(*), permitted on the append-only ledger (Rule 5.5 forbids
// only UPDATE/DELETE).
func (s *Store) CountAuditForIDs(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	const q = `SELECT count(*) FROM agent.agent_audit WHERE external_txn_id = ANY($1)`
	var n int
	if err := s.db.QueryRowContext(ctx, q, pq.Array(ids)).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
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

// ListHITL returns a bounded page of breaks awaiting a human decision, oldest
// first. It is equivalent to ListHITLPage(ctx, Page{}) and returns at most
// defaultPageLimit rows (finding M-14).
func (s *Store) ListHITL(ctx context.Context) ([]HITLItem, error) {
	return s.ListHITLPage(ctx, Page{})
}

// ListHITLPage returns one bounded page of the human-review queue, oldest first
// (finding M-14). The page is normalized to the enforced Limit/Offset bounds.
func (s *Store) ListHITLPage(ctx context.Context, page Page) ([]HITLItem, error) {
	p := page.Normalize()
	const q = `SELECT external_txn_id, reason, enqueued_at
FROM agent.agent_hitl_queue
ORDER BY enqueued_at ASC
LIMIT $1 OFFSET $2`
	rows, err := s.db.QueryContext(ctx, q, p.Limit, p.Offset)
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

// ListAudit returns a bounded page of the append-only audit trail, oldest
// first. It is equivalent to ListAuditPage(ctx, Page{}) and returns at most
// defaultPageLimit rows (finding M-14). SELECT is permitted on agent_audit
// (Rule 5.5 forbids only UPDATE/DELETE); the HITL status page renders this
// trail.
func (s *Store) ListAudit(ctx context.Context) ([]model.AuditEvent, error) {
	return s.ListAuditPage(ctx, Page{})
}

// ListAuditPage returns one bounded page of the append-only audit trail, oldest
// first (finding M-14). The page is normalized to the enforced Limit/Offset
// bounds so no caller can stream the entire audit history in a single query.
func (s *Store) ListAuditPage(ctx context.Context, page Page) ([]model.AuditEvent, error) {
	p := page.Normalize()
	const q = `SELECT event_id, external_txn_id, actor, action, "timestamp", rationale, confidence, provenance
FROM agent.agent_audit
ORDER BY "timestamp" ASC
LIMIT $1 OFFSET $2`
	rows, err := s.db.QueryContext(ctx, q, p.Limit, p.Offset)
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

const recordRuleOutboxSQL = `INSERT INTO agent.agent_rule_outbox (external_txn_id, rule_id, status)
VALUES ($1, $2, 'pending')`

// RecordRuleOutboxTx records, inside the caller's transaction, that a Blnk
// matching rule was created for a break and now requires either confirmation or
// compensation (findings M-12 / M-11). The remediator calls it in the SAME
// transaction that persists the created rule id and appends the rule_created
// audit event, so the durable "a rule exists in Blnk" fact and its compensation
// obligation commit atomically — a crash can never create a rule without also
// recording the pending obligation to clean it up.
func (s *Store) RecordRuleOutboxTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error {
	if strings.TrimSpace(ruleID) == "" {
		return fmt.Errorf("store: RecordRuleOutboxTx requires a non-empty rule id")
	}
	_, err := tx.ExecContext(ctx, recordRuleOutboxSQL, externalTxnID, ruleID)
	return err
}

const confirmRuleOutboxSQL = `UPDATE agent.agent_rule_outbox
SET status = 'confirmed', updated_at = now()
WHERE external_txn_id = $1 AND rule_id = $2 AND status = 'pending'`

// ConfirmRuleOutboxTx marks a created rule's outbox obligation confirmed inside
// the caller's transaction (findings M-12 / M-11): the rule legitimately cleared
// its break, so no compensation is owed. The remediator calls it in the SAME
// transaction that marks the break auto-resolved, so a confirmed resolution and
// the retirement of its compensation obligation commit atomically. It targets
// only the still-'pending' row, so a replay or double-confirm is a harmless
// no-op rather than an error.
func (s *Store) ConfirmRuleOutboxTx(ctx context.Context, tx *sql.Tx, externalTxnID, ruleID string) error {
	_, err := tx.ExecContext(ctx, confirmRuleOutboxSQL, externalTxnID, ruleID)
	return err
}

// ListPendingRuleOutbox returns up to limit oldest 'pending' outbox rows —
// created Blnk rules whose owning remediation attempt never confirmed or
// compensated them (findings M-12 / M-11). The remediator scans these on startup
// to delete orphaned rules left by a crash. limit <= 0 selects defaultPageLimit
// and is capped at maxPageLimit, so recovery can never stream an unbounded set
// into memory (finding M-14).
func (s *Store) ListPendingRuleOutbox(ctx context.Context, limit int) ([]RuleOutboxItem, error) {
	p := Page{Limit: limit}.Normalize()
	const q = `SELECT id, external_txn_id, rule_id, status, attempts, last_error, created_at, updated_at
FROM agent.agent_rule_outbox
WHERE status = 'pending'
ORDER BY created_at ASC
LIMIT $1`
	rows, err := s.db.QueryContext(ctx, q, p.Limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var items []RuleOutboxItem
	for rows.Next() {
		var (
			it      RuleOutboxItem
			lastErr sql.NullString
		)
		if err := rows.Scan(&it.ID, &it.ExternalTxnID, &it.RuleID, &it.Status,
			&it.Attempts, &lastErr, &it.CreatedAt, &it.UpdatedAt); err != nil {
			return nil, err
		}
		if lastErr.Valid {
			it.LastError = lastErr.String
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const markRuleOutboxCompensatedSQL = `UPDATE agent.agent_rule_outbox
SET status = $2, attempts = attempts + 1, last_error = $3, updated_at = now()
WHERE id = $1`

// MarkRuleOutboxCompensated records the outcome of a compensation attempt on a
// pending outbox row (findings M-12 / M-11). On success (ok) the row becomes
// 'compensated' — the orphaned Blnk rule was deleted; on failure it becomes
// 'compensation_failed' and errMsg is stored in last_error for diagnosis. Every
// call increments attempts. It returns ErrNotFound when no such outbox row
// exists. The compensation OUTCOME is additionally written to the append-only
// audit trail by the caller, so this mutable ledger and the immutable audit stay
// consistent.
func (s *Store) MarkRuleOutboxCompensated(ctx context.Context, id int64, ok bool, errMsg string) error {
	status := OutboxCompensated
	if !ok {
		status = OutboxCompensationFailed
	}
	res, err := s.db.ExecContext(ctx, markRuleOutboxCompensatedSQL, id, status, nullIfEmpty(errMsg))
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

const compensateRuleOutboxSQL = `UPDATE agent.agent_rule_outbox
SET status = $3, attempts = attempts + 1, last_error = $4, updated_at = now()
WHERE external_txn_id = $1 AND rule_id = $2 AND status = 'pending'`

// CompensateRuleOutbox records the outcome of an IN-CALL compensation attempt on
// the still-'pending' outbox obligation for (externalTxnID, ruleID), in
// autocommit mode (findings M-12 / M-11). On success (ok) the row becomes
// 'compensated' — the orphaned Blnk rule was deleted; on failure it becomes
// 'compensation_failed' and errMsg is stored in last_error for diagnosis. Every
// matching call increments attempts.
//
// It is the by-(externalTxnID, ruleID) counterpart of MarkRuleOutboxCompensated
// (which addresses a row by its numeric id during the startup recovery sweep,
// where the caller holds that id). It mirrors ConfirmRuleOutboxTx's
// (externalTxnID, ruleID) addressing so the remediator's in-call compensation
// path — which holds the rule identity but not the row id — can retire the
// obligation atomically-enough without a prior lookup.
//
// It targets ONLY the still-'pending' row, so it is idempotent and, crucially,
// tolerant of the "no row" case: when a create-commit itself failed the outbox
// row was never durably written, so there is nothing to compensate; a replay or
// double-compensation likewise matches no pending row. In all of these it is a
// harmless no-op that returns nil rather than ErrNotFound. The compensation
// OUTCOME is additionally written to the append-only audit trail by the caller,
// so this mutable ledger and the immutable audit stay consistent.
func (s *Store) CompensateRuleOutbox(ctx context.Context, externalTxnID, ruleID string, ok bool, errMsg string) error {
	status := OutboxCompensated
	if !ok {
		status = OutboxCompensationFailed
	}
	_, err := s.db.ExecContext(ctx, compensateRuleOutboxSQL, externalTxnID, ruleID, status, nullIfEmpty(errMsg))
	return err
}

const beginRunInsertSQL = `INSERT INTO agent.agent_run (run_id, fixture_key, status)
VALUES ($1, $2, 'running')
ON CONFLICT (run_id) DO NOTHING
RETURNING run_id, status`

const beginRunSelectSQL = `SELECT run_id, status FROM agent.agent_run WHERE run_id = $1`

// BeginRun implements durable per-INVOCATION run idempotency (findings M-15,
// F17). runID is THIS invocation's identity (the caller generates exactly one
// per process invocation and reuses it across in-process retries); fixtureKey is
// the immutable content hash of the ingested fixture (CSV bytes plus source
// label), recorded for provenance and deliberately non-unique so many
// invocations may share one fixture. BeginRun records the invocation exactly
// once per runID and reports what the caller should do:
//
//   - First time this runID is seen (a fresh invocation): it inserts a 'running'
//     row recording runID + fixtureKey and returns
//     (alreadyCompleted=false, existingRunID=runID, nil) — the caller proceeds.
//     A genuinely NEW invocation of the SAME fixture supplies a NEW runID and so
//     always lands here, which is what lets a legitimate rerun/recovery
//     reprocess eligible work under a fresh run-scoped id set instead of being
//     refused as "already done" (finding F17).
//   - Same runID already 'running' (an in-process M-16 retry of the SAME
//     invocation): it returns (alreadyCompleted=false, existingRunID=runID, nil)
//     — the retry reuses the original scoped id set (upsert-idempotent break
//     rows) rather than forking duplicate history under a fresh id.
//   - Same runID already 'completed': it returns
//     (alreadyCompleted=true, existingRunID=runID, nil) — an idempotent replay of
//     an invocation that already finished (the caller rebuilds the summary from
//     that run's rows without reprocessing).
//
// The insert-then-select pattern tolerates a concurrent same-runID race: the PK
// on run_id guarantees exactly one INSERT wins; the loser reads the winner's
// committed row, and per-break claim leases (ClaimBreak) provide the actual
// mutual exclusion for individual break processing.
func (s *Store) BeginRun(ctx context.Context, fixtureKey, runID string) (bool, string, error) {
	if strings.TrimSpace(fixtureKey) == "" {
		return false, "", fmt.Errorf("store: BeginRun requires a non-empty fixture key")
	}
	if strings.TrimSpace(runID) == "" {
		return false, "", fmt.Errorf("store: BeginRun requires a non-empty run id")
	}
	var (
		gotRunID string
		status   string
	)
	err := s.db.QueryRowContext(ctx, beginRunInsertSQL, runID, fixtureKey).Scan(&gotRunID, &status)
	if err == nil {
		// A row was inserted: this is a fresh invocation under the caller's runID.
		return false, gotRunID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, "", err
	}
	// ON CONFLICT DO NOTHING returned no row: this runID already exists (an
	// in-process retry or an idempotent replay of the same invocation). Read the
	// recorded run to decide whether to skip (completed) or resume (running).
	if err := s.db.QueryRowContext(ctx, beginRunSelectSQL, runID).Scan(&gotRunID, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Extremely unlikely TOCTOU (row removed between the two statements);
			// surface it rather than silently reprocessing.
			return false, "", ErrNotFound
		}
		return false, "", err
	}
	return status == RunCompleted, gotRunID, nil
}

const completeRunSQL = `UPDATE agent.agent_run
SET status = 'completed', updated_at = now()
WHERE run_id = $1`

// CompleteRun flips THIS invocation's run row (by run_id) to 'completed'
// (findings M-15, F17) once the pipeline has finished, so an in-process replay of
// the same invocation is reported alreadyCompleted by BeginRun and skips
// reprocessing, while a genuinely new invocation (new run_id) is unaffected and
// runs fresh. It returns ErrNotFound when no run row exists for runID (BeginRun
// must have created it first).
func (s *Store) CompleteRun(ctx context.Context, runID string) error {
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("store: CompleteRun requires a non-empty run id")
	}
	res, err := s.db.ExecContext(ctx, completeRunSQL, runID)
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
