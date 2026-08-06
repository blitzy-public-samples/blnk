/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// event_outbox.go is the repository implementation of the eventOutbox contract
// declared in repository.go: persistence for blnk.event_outbox, the transactional
// outbox that carries every ledger mutation's event to Kafka.
//
// It is modelled directly on lineage.go's outbox half — the same in-transaction
// insert, the same CTE claim with FOR UPDATE SKIP LOCKED, and the same
// apierror-wrapped failures — because that is the proven house pattern for a
// relay-backed outbox in this repository, and a second, differently shaped one
// would be a maintenance liability rather than an improvement. blnk.event_outbox
// and blnk.lineage_outbox are nonetheless SEPARATE TABLES served by SEPARATE
// RELAYS: they are never merged, nothing here joins across them, and the lineage
// relay's behaviour is untouched.
//
// Three differences from the lineage outbox are deliberate and load-bearing:
//
//   - Ordering is by occurred_at, not created_at. occurred_at is the instant the
//     domain action happened; created_at is merely when the row was written.
//     Claiming in occurred_at order is one of the two mechanisms that jointly
//     deliver the per-aggregate ordering guarantee — the other being the Kafka
//     message key pinning one aggregate to one partition.
//   - The terminal success state is dispatched, and there is an additional
//     dead_lettered terminal state. The lineage vocabulary has neither, so every
//     status here comes from model.EventOutboxStatus* and never from
//     model.OutboxStatus*.
//   - Every standalone method opens an OpenTelemetry span on the
//     transaction.database tracer, following chain.go. The in-transaction insert
//     deliberately does not: it runs inside a caller-owned transaction whose span
//     is already open in transaction.go, and a second span per row would be noise.
//
// # What "byte-for-byte payload fidelity" actually means here — READ THIS
//
// The payload column is JSONB, and JSONB is a PARSED representation: PostgreSQL
// sorts object keys, renormalises whitespace and expands exponent notation. A
// payload marshaled as {"event":"x","data":{...}} therefore comes back as
// {"data": {...}, "event": "x"}. This was verified against a live database, not
// assumed, and it is NOT a defect to be fixed — the JSONB column type is
// specified, and switching it to TEXT to preserve the input spelling would give up
// JSONB indexing and containment queries for a guarantee nothing actually needs.
//
// The guarantees that DO matter are unaffected, because both are relative to the
// STORED row rather than to the pre-insert spelling:
//
//   - Dual-delivery consistency holds structurally. The relay publishes to Kafka
//     and enqueues the legacy webhook from the SAME claimed row, so both
//     transports carry the same bytes by construction and cannot drift apart.
//   - Replay fidelity holds. A replay re-publishes the STORED bytes rather than
//     re-marshalling a struct, so a replayed event is identical to the event
//     originally published, aside from the added failure metadata.
//
// Repeated reads of one row return identical bytes — normalisation happens once on
// write and the stored form is stable thereafter — which is what makes both
// guarantees hold in practice. Nothing in this file transforms those bytes: the
// insert binds model.EventOutbox.Payload straight through, and every read scans it
// back into a json.RawMessage without re-marshalling.
//
// The practical consequence for anyone writing a test: compare payloads by JSON
// EQUIVALENCE against the pre-insert value, or by byte equality against a value
// that has been round-tripped through the database. Comparing pre-insert bytes to
// post-read bytes fails on key order alone and looks like payload corruption when
// nothing is wrong.

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// defaultEventMaxAttempts mirrors the max_attempts column default and the
	// default of RELAY_MAX_RETRY_ATTEMPTS. It is the fallback for a row inserted
	// without an explicit retry budget: a zero budget would make the row both
	// un-retryable AND un-claimable, because the claim predicate requires
	// attempts < max_attempts, so it would silently never publish.
	defaultEventMaxAttempts = 5

	// defaultEventClaimLease is the lease applied when a caller asks for a
	// non-positive one. A zero-length lease would expire the instant it was
	// taken, letting a second relay instance reclaim a row that is still being
	// published and delivering the event twice; falling back to the value the
	// outbox relays already use is safer than honouring an obviously wrong
	// argument.
	defaultEventClaimLease = 30 * time.Second

	// Page-size bounds for the dead-letter listing. The default keeps an
	// unqualified request cheap; the maximum stops a caller turning a triage
	// endpoint into a full table scan.
	defaultDeadLetterPageSize = 50
	maxDeadLetterPageSize     = 500
)

// eventOutboxColumns is the column list every read of blnk.event_outbox
// projects, in the exact order scanEventOutbox consumes it.
//
// It is declared once because the claim, the single-row fetch and the dead-letter
// listing must project identically: a column added to one query and not the
// others is a scan-order bug that no compiler catches and that surfaces only as
// mis-assigned field values at runtime.
//
// created_at is deliberately absent. It exists on the table for forensics, but
// model.EventOutbox has no field for it and nothing orders by it — all ordering
// in this file is by occurred_at — so projecting it would only add a column with
// nowhere to go.
const eventOutboxColumns = `id, event_id, event_type, aggregate_id, ledger_id, topic, schema_version, payload, occurred_at, ` +
	`status, attempts, max_attempts, last_error, first_attempted_at, last_attempted_at, dispatched_at, locked_until, ` +
	`webhook_dispatched, dlt_topic, failure_metadata`

// eventOutboxScanner is the minimum surface scanEventOutbox needs, satisfied by
// both *sql.Row and *sql.Rows. Sharing one scanner between the single-row and
// multi-row paths is what keeps their column handling from drifting apart.
type eventOutboxScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventOutbox decodes one blnk.event_outbox row into a model.EventOutbox.
//
// Every nullable column is read through a sql.Null* wrapper and then normalised:
// nullable text becomes the empty string, and nullable timestamps become nil
// pointers rather than zero times. That distinction matters — a nil
// FirstAttemptedAt means "never attempted", which a zero time.Time would silently
// misreport as the year 1.
//
// Both JSONB columns are decoded as raw JSON. payload is scanned straight into
// the json.RawMessage field so the stored bytes reach the caller untransformed,
// and failure_metadata is scanned into a byte slice so that SQL NULL stays a nil
// RawMessage rather than becoming the four bytes "null".
func scanEventOutbox(s eventOutboxScanner) (model.EventOutbox, error) {
	var e model.EventOutbox
	var lastError, dltTopic sql.NullString
	var firstAttemptedAt, lastAttemptedAt, dispatchedAt, lockedUntil sql.NullTime
	var failureMetadata []byte

	if err := s.Scan(
		&e.ID,
		&e.EventID,
		&e.EventType,
		&e.AggregateID,
		&e.LedgerID,
		&e.Topic,
		&e.SchemaVersion,
		&e.Payload,
		&e.OccurredAt,
		&e.Status,
		&e.Attempts,
		&e.MaxAttempts,
		&lastError,
		&firstAttemptedAt,
		&lastAttemptedAt,
		&dispatchedAt,
		&lockedUntil,
		&e.WebhookDispatched,
		&dltTopic,
		&failureMetadata,
	); err != nil {
		return model.EventOutbox{}, err
	}

	e.LastError = lastError.String
	e.DLTTopic = dltTopic.String
	if len(failureMetadata) > 0 {
		e.FailureMetadata = failureMetadata
	}
	if firstAttemptedAt.Valid {
		e.FirstAttemptedAt = &firstAttemptedAt.Time
	}
	if lastAttemptedAt.Valid {
		e.LastAttemptedAt = &lastAttemptedAt.Time
	}
	if dispatchedAt.Valid {
		e.DispatchedAt = &dispatchedAt.Time
	}
	if lockedUntil.Valid {
		e.LockedUntil = &lockedUntil.Time
	}

	return e, nil
}

// eventOutboxInsertQuery is shared by the in-transaction insert, the standalone
// insert and the batch insert, so none of the three can diverge in which columns
// it populates. The batch insert reuses the same column list and repeats the same
// value tuple once per row; every other detail is identical.
//
// status is bound explicitly from model.EventOutboxStatusPending rather than left
// to the column default, mirroring InsertLineageOutboxInTx: the initial state
// belongs to this layer and not to the caller, and binding it means a caller that
// sets Status to anything else cannot smuggle a row into the middle of the relay
// state machine. The bound value and the column default agree, so the row is
// pending either way.
//
// occurred_at, by contrast, IS the caller's value: it is the domain instant that
// was observed, it is what the claim orders by, and replacing it with the insert
// time would destroy the ordering guarantee built on it. created_at and the
// remaining relay-state columns are left to their defaults.
const eventOutboxInsertColumns = `event_id, event_type, aggregate_id, ledger_id, topic, schema_version, payload, occurred_at, status, max_attempts`

const eventOutboxInsertQuery = `
		INSERT INTO blnk.event_outbox
		(` + eventOutboxInsertColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`

// eventOutboxInsertValueCount is the number of bound values per inserted row. It
// keeps the batch insert's placeholder arithmetic tied to the column list above
// rather than to a literal that a later column addition would leave stale.
const eventOutboxInsertValueCount = 10

// maxEventOutboxInsertRows bounds how many rows one multi-row INSERT may carry,
// and it is a hard requirement rather than tuning.
//
// PostgreSQL's extended protocol accepts at most 65535 bound parameters per
// statement. At eventOutboxInsertValueCount parameters per row, a single statement
// therefore tops out around 6553 rows — and the transaction coalescing path is
// allowed to present up to 10000 transactions in one batch, so an unbounded batch
// would fail outright on a large enough coalesced write and roll the whole ledger
// transaction back with it. Chunking at 1000 rows keeps every statement an order of
// magnitude clear of the limit while still costing only one round trip per
// thousand events, and every chunk runs inside the caller's transaction, so the
// batch remains all-or-nothing.
const maxEventOutboxInsertRows = 1000

// validateEventOutboxEntry rejects the three ways a caller can hand over an entry
// that cannot become a valid row, converting each into a typed bad-request rather
// than an opaque driver error raised deep inside the caller's ledger transaction.
//
//   - A nil entry is a programming error; dereferencing it would panic.
//   - An empty event_id would insert a row whose idempotency key is the empty
//     string. The first such row succeeds and every later one collides on the
//     unique index, so the failure would surface far from its cause.
//   - An empty payload would send NULL into a NOT NULL JSONB column. The payload
//     IS the event body; a row without one has nothing to publish.
func validateEventOutboxEntry(e *model.EventOutbox) error {
	if e == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry is required", nil)
	}
	if e.EventID == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires an event ID", nil)
	}
	if len(e.Payload) == 0 {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires a payload", nil)
	}
	return nil
}

// normalizeEventOutboxEntry applies the defaults a caller may legitimately leave
// unset and pins the initial status, mutating the entry so that it describes
// exactly what is about to be persisted. Callers read these fields back after the
// insert — the relay and the dual-delivery leg both work from the in-memory row —
// so leaving the struct disagreeing with the stored row would be a trap.
//
// A non-positive schema_version is raised to model.SchemaVersionV1 because the
// envelope version reaches subscribers on the wire, where a zero would be read as
// an unknown schema.
func normalizeEventOutboxEntry(e *model.EventOutbox) {
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now()
	}
	if e.MaxAttempts <= 0 {
		e.MaxAttempts = defaultEventMaxAttempts
	}
	if e.SchemaVersion <= 0 {
		e.SchemaVersion = model.SchemaVersionV1
	}
	e.Status = model.EventOutboxStatusPending
}

// eventOutboxInsertArgs builds the bound values for one row, in eventOutboxInsertColumns order.
//
// The payload is passed through as its raw bytes. It is neither re-marshalled nor
// round-tripped through a map, because those bytes are the legacy webhook body
// verbatim and both delivery guarantees are written against them.
func eventOutboxInsertArgs(e *model.EventOutbox) []interface{} {
	return []interface{}{
		e.EventID,
		e.EventType,
		e.AggregateID,
		e.LedgerID,
		e.Topic,
		e.SchemaVersion,
		[]byte(e.Payload),
		e.OccurredAt,
		model.EventOutboxStatusPending,
		e.MaxAttempts,
	}
}

// wrapEventOutboxInsertError turns a driver failure into the typed error the rest
// of the system can act on, following the discrimination UpsertLineageMapping
// performs on *pq.Error.
//
// The unique-violation case is the one that matters. event_id carries
// event_outbox_event_id_uidx, which is what makes "one event is recorded once" a
// schema-enforced invariant, so a duplicate insert is a CONFLICT and not a server
// fault: a caller retrying a mutation whose event was already captured must be
// able to recognise that and carry on. errors.As is used rather than a bare type
// assertion so the discrimination still works if the driver error arrives wrapped.
func wrapEventOutboxInsertError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code.Name() {
		case "unique_violation":
			return apierror.NewAPIError(apierror.ErrConflict, "Event outbox entry already exists", err)
		case "foreign_key_violation":
			return apierror.NewAPIError(apierror.ErrBadRequest, "Invalid event outbox entry reference", err)
		case "not_null_violation":
			return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry is missing a required field", err)
		default:
			return apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
		}
	}
	return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert event outbox entry", err)
}

// InsertEventOutboxInTx inserts an event outbox entry within an existing database
// transaction, so the event is committed atomically with the ledger mutation that
// produced it.
//
// This is the method that makes the transactional-outbox guarantee real: the
// caller has already begun a transaction and applied its balance updates, and
// passing that same *sql.Tx here is what ties the event's fate to theirs. Roll the
// transaction back and the event row is gone with it; commit and neither can exist
// without the other. The generated id is written back onto the supplied entry so
// the caller can correlate it after the commit.
//
// No span is opened. This runs inside a caller-owned transaction whose span is
// already open in transaction.go, and one span per inserted row would bury that
// trace in noise for no diagnostic gain.
func (d Datasource) InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	if err := validateEventOutboxEntry(e); err != nil {
		return err
	}
	normalizeEventOutboxEntry(e)

	if err := tx.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		return wrapEventOutboxInsertError(err)
	}
	return nil
}

// InsertEventOutbox inserts an event outbox entry directly, outside any ledger
// transaction.
//
// It exists for events that have no accompanying ledger mutation to be atomic
// with — internal error notifications, for instance, and ledger and identity
// creation, which do not go through the atomic transaction writers. Such events
// still belong in the outbox so they get the same durable retry, dead-letter and
// replay treatment as every other event; there is simply no wider transaction to
// enrol them in.
func (d Datasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	if err := validateEventOutboxEntry(e); err != nil {
		return err
	}
	normalizeEventOutboxEntry(e)

	ctx, span := otel.Tracer("transaction.database").Start(ctx, "InsertEventOutbox")
	defer span.End()
	span.SetAttributes(
		attribute.String("event_outbox.event_id", e.EventID),
		attribute.String("event_outbox.event_type", e.EventType),
		attribute.String("event_outbox.topic", e.Topic),
	)

	if err := d.Conn.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		span.RecordError(err)
		return wrapEventOutboxInsertError(err)
	}
	return nil
}

// insertEventOutboxesInTx inserts a batch of event outbox entries inside an
// existing transaction, in a single round trip.
//
// It is the entry point the atomic transaction writers use, including the
// coalescing path, which is why it takes a slice and tolerates an empty one: the
// event rows are threaded through those writers as a variadic argument, so a
// caller that captures no events passes nothing and this is a no-op rather than an
// error. nil elements are skipped for the same reason, mirroring
// insertLineageOutboxesInTx.
//
// One multi-row INSERT is used rather than a loop of single inserts because the
// coalescing path can present a large batch inside an already-open ledger
// transaction, where every extra round trip lengthens the window during which the
// transaction holds its balance row locks.
//
// Every value is bound. The only string formatting is placeholder NUMBERING,
// which is generated from eventOutboxInsertValueCount so it cannot fall out of
// step with the column list, and never from caller data.
func insertEventOutboxesInTx(ctx context.Context, tx *sql.Tx, entries []*model.EventOutbox) error {
	if len(entries) == 0 {
		return nil
	}

	// Compact and validate up front so an invalid entry fails the whole batch
	// before any statement runs, rather than after some rows are already written.
	pending := make([]*model.EventOutbox, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if err := validateEventOutboxEntry(entry); err != nil {
			return err
		}
		normalizeEventOutboxEntry(entry)
		pending = append(pending, entry)
	}

	// Every element was nil, so there is nothing to insert and no statement to
	// run. Reaching PostgreSQL with a VALUES list containing no tuples would be a
	// syntax error.
	if len(pending) == 0 {
		return nil
	}

	for start := 0; start < len(pending); start += maxEventOutboxInsertRows {
		end := start + maxEventOutboxInsertRows
		if end > len(pending) {
			end = len(pending)
		}
		if err := insertEventOutboxChunkInTx(ctx, tx, pending[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// insertEventOutboxChunkInTx writes one bounded chunk of entries as a single
// multi-row INSERT and back-fills the generated ids. The caller has already
// validated and normalised every entry and guaranteed the chunk is non-empty and
// within maxEventOutboxInsertRows.
func insertEventOutboxChunkInTx(ctx context.Context, tx *sql.Tx, chunk []*model.EventOutbox) error {
	var query strings.Builder
	query.WriteString(`
		INSERT INTO blnk.event_outbox
		(` + eventOutboxInsertColumns + `)
		VALUES
	`)

	args := make([]interface{}, 0, len(chunk)*eventOutboxInsertValueCount)
	entriesByEventID := make(map[string]*model.EventOutbox, len(chunk))
	for _, entry := range chunk {
		if len(args) > 0 {
			query.WriteString(",")
		}
		base := len(args) + 1
		query.WriteString("(")
		for offset := 0; offset < eventOutboxInsertValueCount; offset++ {
			if offset > 0 {
				query.WriteString(",")
			}
			query.WriteString(fmt.Sprintf("$%d", base+offset))
		}
		query.WriteString(")")

		args = append(args, eventOutboxInsertArgs(entry)...)
		entriesByEventID[entry.EventID] = entry
	}

	query.WriteString(" RETURNING id, event_id")
	rows, err := tx.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return wrapEventOutboxInsertError(err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	inserted := 0
	for rows.Next() {
		var id int64
		var eventID string
		if scanErr := rows.Scan(&id, &eventID); scanErr != nil {
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan inserted event outbox entry", scanErr)
		}
		// The generated ids come back keyed by event_id rather than by position,
		// because RETURNING makes no promise about row order.
		if entry, ok := entriesByEventID[eventID]; ok {
			entry.ID = id
		}
		inserted++
	}

	if err := rows.Err(); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed while iterating inserted event outbox entries", err)
	}

	// A short count means at least one event was silently not persisted, which
	// would be a lost event the moment the surrounding transaction committed.
	// Returning an error rolls that transaction back instead.
	if inserted != len(entriesByEventID) {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert all event outbox entries", nil)
	}

	return nil
}

// claimPendingEventOutboxQuery claims a batch of publishable rows and takes a
// lease on them in one statement. It is a package-level constant rather than a
// local so its text is reachable from tests, which assert that FOR UPDATE SKIP
// LOCKED and the occurred_at ordering are both still present — the two properties
// a well-meaning refactor is most likely to drop.
//
// UPDATE ... RETURNING does not preserve the inner ORDER BY, so the claimed rows
// are re-ordered through a CTE to guarantee FIFO delivery within the batch.
// Without the outer sort the batch would come back in arbitrary order and
// per-aggregate FIFO would be lost inside a batch even though the partition key
// was correct. The CTE therefore is NOT redundant and must not be collapsed.
//
// Ordering is by occurred_at, NOT created_at as the lineage claim uses. This is
// the one deliberate divergence from that query, and it carries the per-aggregate
// ordering guarantee: occurred_at is the instant the domain action happened,
// whereas created_at is when the row happened to be written. A copy-paste "fix"
// back to created_at would leave every unit test green and silently break
// ordering. id breaks ties, so rows that share an occurred_at instant — bulk
// events stamped from one clock read, for instance — are still claimed in the
// order they were recorded rather than in whatever order the planner returns.
//
// FOR UPDATE SKIP LOCKED is not optional and must not be replaced by an advisory
// lock, by NOWAIT, or by a status flag alone: it is what lets several relay
// instances claim disjoint batches without blocking each other or losing ordering.
//
// The lease in locked_until, rather than an in-memory marker, is what makes a
// crashed relay's in-flight rows recoverable: once the lease expires the rows
// re-enter the claimable set instead of being stranded. attempts < max_attempts
// keeps rows that have spent their retry budget out of the claimable set, so an
// exhausted row is left for the dead-letter path rather than being retried
// forever.
//
// first_attempted_at and last_attempted_at are stamped here, on the claim, which
// is what model.EventOutbox documents ("nil until the row is first claimed").
// COALESCE pins the first to the first claim and leaves it alone thereafter, while
// the last moves with every claim; together they bound the retry window that the
// dead-letter failure metadata reports.
const claimPendingEventOutboxQuery = `
		WITH claimed AS (
			UPDATE blnk.event_outbox
			SET status = $1,
				locked_until = NOW() + $2::interval,
				first_attempted_at = COALESCE(first_attempted_at, NOW()),
				last_attempted_at = NOW()
			WHERE id IN (
				SELECT id FROM blnk.event_outbox
				WHERE status IN ('pending', 'processing')
				  AND (locked_until IS NULL OR locked_until < NOW())
				  AND attempts < max_attempts
				ORDER BY occurred_at ASC, id ASC
				LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingEventOutbox claims a batch of pending entries for publishing and
// takes a lease on them for lockDuration. The returned entries are ordered oldest
// occurrence first, which is the order they must be published in.
//
// A non-positive batchSize is rejected rather than passed through. A zero LIMIT
// claims nothing and returns no error, which is indistinguishable from an empty
// backlog: the relay would look healthy while publishing nothing at all. A
// non-positive lockDuration is instead normalised to defaultEventClaimLease,
// because an expired-on-arrival lease is a correctness problem — a second relay
// instance could reclaim and republish a row still being published — and failing
// the poll outright would stop the relay rather than protect it.
//
// lockDuration is bound as its Go duration string ("30s"), which PostgreSQL parses
// directly as an interval; the value is never hand-formatted into the statement.
func (d Datasource) ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingEventOutbox")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox claim batch size must be greater than zero", nil)
		span.RecordError(err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive event outbox lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, lockDuration.String(), batchSize)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to claim pending event outbox entries", err)
	}
	defer func() {
		// A close failure is logged rather than returned: the rows have already
		// been read, and replacing a successful claim with an error here would
		// leave those rows leased but unpublished until the lease expired.
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			span.RecordError(scanErr)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan event outbox entry", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over event outbox entries", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// logEventOutboxRowsUnaffected reports an UPDATE that matched no row.
//
// None of the transitions below treats this as an error, and that is deliberate:
// a concurrent relay instance whose lease expired may legitimately have moved the
// row on already, and failing the caller would turn a benign race into a spurious
// retry. It is logged rather than ignored because the other explanation — an id
// that never existed — is a real defect, and an operator needs to be able to see
// it. DeleteLineageMapping's ErrNotFound treatment is the right choice for a
// caller-visible delete and the wrong one here.
func logEventOutboxRowsUnaffected(result sql.Result, id int64, transition string) {
	if result == nil {
		// database/sql guarantees a non-nil Result alongside a nil error, but a
		// transition that has already succeeded must not be turned into a panic by
		// its own bookkeeping.
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// Not every driver reports this. It is not worth failing a completed
		// UPDATE over, but it is worth a line.
		logrus.WithError(err).WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
		}).Debug("Could not determine rows affected for event outbox transition")
		return
	}
	if affected == 0 {
		logrus.WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
		}).Warn("Event outbox transition matched no row")
	}
}

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged
// the publish.
//
// The lease is released and dispatched_at is stamped in the same statement.
// dispatched is a terminal state: the row is no longer claimable, because the
// claim predicate admits only pending and processing rows.
//
// The gap between the broker's acknowledgement and this call is where at-least-once
// delivery comes from. A relay that dies in that gap has published the event but
// not recorded it, so the row is reclaimed once its lease expires and published
// again. That duplicate is by design and is suppressed at the consumer's
// idempotency boundary on event_id; nothing here tries to close it, because closing
// it would require a distributed transaction with the broker.
func (d Datasource) MarkEventDispatched(ctx context.Context, id int64) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dispatched_at = NOW(), locked_until = NULL
		WHERE id = $2
	`, model.EventOutboxStatusDispatched, id)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dispatched", err)
	}

	logEventOutboxRowsUnaffected(result, id, model.EventOutboxStatusDispatched)
	return nil
}

// MarkEventFailed records a failed publish attempt and its reason, and decides in
// the same statement whether the entry is retried or has spent its budget.
//
// The status is chosen in SQL rather than in Go: if the increment exhausts
// max_attempts the row becomes failed, otherwise it returns to pending for another
// attempt. Deciding it inside the UPDATE keeps the read and the write atomic, so
// two relay instances racing on one row cannot both conclude they were the last
// attempt. The lease is released either way, which is what lets the retry actually
// be picked up.
//
// The exhaustion arm sets failed and NOT dead_lettered, following the state
// machine model.EventOutbox documents: failed means "we gave up publishing", and
// dead_lettered means "and the event is now preserved on its `<topic>.dlt`
// sibling, from which it can be replayed". Only the dead-letter publisher knows
// whether that second step actually happened, so only it may declare it — see
// MarkEventDeadLettered. ListDeadLetteredEvents covers both literals, so an event
// stuck between the two states is still visible to an operator rather than
// invisible until its dead-letter write succeeds.
//
// first_attempted_at is stamped through COALESCE so it survives as the first
// attempt even on the retry path, and last_attempted_at moves with every attempt.
// Both are stamped here as well as on the claim, because a publish attempt can be
// recorded against a row this process did not itself claim, and the dead-letter
// failure metadata needs the window bounded either way.
func (d Datasource) MarkEventFailed(ctx context.Context, id int64, errMsg string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventFailed")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = NULL
		WHERE id = $4
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, id)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark event outbox entry as failed", err)
	}

	logEventOutboxRowsUnaffected(result, id, model.EventOutboxStatusFailed)
	return nil
}

// MarkEventDeadLettered records that an entry whose retry budget was exhausted has
// been written to its dead-letter topic, moving it to the dead_lettered terminal
// state and storing the dead-letter record.
//
// It is the second half of the failure path, and it exists as a separate method
// because only the dead-letter publisher can supply what it stores: the resolved
// `<topic>.dlt` name and the marshaled failure metadata. Composing the dead-letter
// envelope belongs to that publisher — deriving the topic name or building the
// metadata in SQL here would duplicate the topic-naming rules and the metadata
// schema in a second place, and they would drift.
//
// Recording it matters beyond bookkeeping: dlt_topic and failure_metadata are what
// the dead-letter API displays and what an operator triages from, and
// dead_lettered is the state a replay checks for before re-publishing. Leaving
// either NULL would give the dead-letter inventory nothing to show.
//
// An empty dltTopic or empty failureMetadata is stored as SQL NULL rather than as
// an empty string or an empty JSON document, so "not recorded" stays distinct from
// "recorded as empty". The caller drives the state machine: no precondition on the
// current status is enforced here, and a call that matches no row is logged rather
// than failed, exactly as the other transitions behave.
func (d Datasource) MarkEventDeadLettered(ctx context.Context, id int64, dltTopic string, failureMetadata json.RawMessage) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDeadLettered")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dlt_topic", dltTopic),
	)

	var dltTopicArg interface{}
	if dltTopic != "" {
		dltTopicArg = dltTopic
	}

	var failureMetadataArg interface{}
	if len(failureMetadata) > 0 {
		failureMetadataArg = []byte(failureMetadata)
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dlt_topic = $2, failure_metadata = $3, locked_until = NULL
		WHERE id = $4
	`, model.EventOutboxStatusDeadLettered, dltTopicArg, failureMetadataArg, id)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dead-lettered", err)
	}

	logEventOutboxRowsUnaffected(result, id, model.EventOutboxStatusDeadLettered)
	return nil
}

// MarkWebhookDispatched records that the legacy HTTP webhook leg was dispatched
// for this entry.
//
// It serves the dual-delivery window only. The relay publishes to Kafka and
// enqueues the legacy webhook task from the SAME claimed row — which is what makes
// the two transports carry byte-identical payloads structurally rather than by
// careful coding — and this flag makes the legacy leg individually idempotent: a
// row republished to Kafka after a crash does not enqueue a second webhook.
//
// This method, the webhook_dispatched column and the relay branch that calls it are
// all removed at the webhook sunset, together with webhooks.go. Every other method
// in this file outlives it.
func (d Datasource) MarkWebhookDispatched(ctx context.Context, id int64) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkWebhookDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_dispatched = TRUE
		WHERE id = $1
	`, id)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark webhook dispatched for event outbox entry", err)
	}

	logEventOutboxRowsUnaffected(result, id, "webhook_dispatched")
	return nil
}

// GetEventByID retrieves an entry by its business event_id UUID.
//
// The lookup is by event_id and not by the BIGSERIAL id because that is what the
// dead-letter replay route carries and what a subscriber quotes when it asks for an
// event to be resent; the surrogate key is never exposed outside this layer. The
// lookup is index-backed by event_outbox_event_id_uidx, and the unique index also
// guarantees at most one row can match.
//
// A missing row is returned as a typed not-found error rather than as (nil, nil)
// the way GetOutboxByTransactionID does. That deviation is deliberate: the replay
// handler has to tell "no such event" apart from "found it" to answer correctly,
// and a nil-nil result forces every caller to re-derive that distinction and
// invites the nil dereference that follows from forgetting to.
func (d Datasource) GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventByID")
	defer span.End()
	span.SetAttributes(attribute.String("event_outbox.event_id", eventID))

	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE event_id = $1
	`, eventID)

	entry, err := scanEventOutbox(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Not recorded on the span as an error: a lookup that finds nothing
			// is an ordinary outcome of a caller-supplied id, not a fault.
			return nil, apierror.NewAPIError(apierror.ErrNotFound, "Event not found", err)
		}
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve event outbox entry", err)
	}

	span.SetAttributes(attribute.String("event_outbox.status", entry.Status))
	return &entry, nil
}

// ListDeadLetteredEvents pages the dead-letter inventory behind the dead-letter
// API.
//
// Both terminal failure states are included. A row becomes failed the moment its
// retry budget is spent and dead_lettered only once the event has additionally been
// written to its `<topic>.dlt` sibling; listing only the latter would hide events
// that exhausted their retries but whose dead-letter publication itself failed —
// exactly the events an operator most needs to see. The partial index on
// (status, occurred_at) covers both literals for this reason.
//
// Ordering is by occurred_at descending, newest first, because triage starts from
// the most recent failures; id descending breaks ties so paging cannot show or skip
// a row twice when several share an instant. limit is defaulted and capped, and a
// negative offset is clamped to zero, so a malformed page request degrades to a
// sane one instead of scanning the table or failing.
func (d Datasource) ListDeadLetteredEvents(ctx context.Context, limit, offset int) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListDeadLetteredEvents")
	defer span.End()

	if limit <= 0 {
		limit = defaultDeadLetterPageSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}
	if offset < 0 {
		offset = 0
	}
	span.SetAttributes(
		attribute.Int("event_outbox.limit", limit),
		attribute.Int("event_outbox.offset", offset),
	)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		ORDER BY occurred_at DESC, id DESC
		LIMIT $3 OFFSET $4
	`, model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to list dead-lettered events", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			span.RecordError(scanErr)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan dead-lettered event", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over dead-lettered events", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.dead_lettered_count", len(entries)))
	return entries, nil
}

// CountEventOutboxByStatus returns a status-keyed count of every row in
// blnk.event_outbox.
//
// IT IS NOT UNUSED — do not delete it. A grep for callers inside this package
// alone finds none, which is exactly the trap this comment exists to prevent. It
// exists for one named purpose: the daily zero-loss reconciliation, which passes
// when the dispatched plus dead-lettered counts equal the sum of the main-topic and
// dead-letter-topic end offsets reported by the broker. Three things consume it —
// the event statistics endpoint, the reconciliation runbook in the Kafka operations
// documentation, and the pending-backlog gauge exported to the metrics pipeline.
//
// A status with no rows is ABSENT from the returned map rather than present with a
// zero, because GROUP BY only produces rows that exist. Callers must therefore read
// it with the two-value form or accept the zero value — which is what the backlog
// gauge does when nothing is pending. The map is keyed by the
// model.EventOutboxStatus* values; any other key would mean a row carries a status
// this code does not know about, which the column deliberately permits so the state
// machine can be extended without a migration.
//
// The map is never nil on success: an empty table yields an empty map, so a caller
// can range over the result without a nil check.
func (d Datasource) CountEventOutboxByStatus(ctx context.Context) (map[string]int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountEventOutboxByStatus")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT status, COUNT(*)
		FROM blnk.event_outbox
		GROUP BY status
	`)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to count event outbox entries by status", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	counts := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			span.RecordError(scanErr)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan event outbox status count", scanErr)
		}
		counts[status] = count
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over event outbox status counts", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.status_count", len(counts)))
	return counts, nil
}
