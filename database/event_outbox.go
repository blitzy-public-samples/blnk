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
// relay-backed outbox in this repository, and a second, differently-shaped one
// would be a maintenance liability rather than an improvement.
//
// Two differences from the lineage outbox are deliberate and load-bearing:
//
//   - Ordering is by occurred_at, not created_at. The event envelope's occurred_at
//     is the instant the domain action happened, and claiming in that order is
//     half of what delivers the per-aggregate ordering guarantee (the other half
//     being the Kafka message key pinning an aggregate to one partition).
//   - The terminal success state is dispatched, and there is an additional
//     dead_lettered terminal state. The lineage vocabulary has neither.
//
// # What "byte-for-byte payload fidelity" actually means here — READ THIS
//
// The payload column is JSONB, and JSONB is a PARSED representation: PostgreSQL
// sorts object keys, drops insignificant whitespace and normalises number
// literals. A payload marshaled as {"event":"x","data":{...}} therefore comes back
// as {"data": {...}, "event": "x"}. This was verified against a live database, not
// assumed, and it is NOT a defect to be fixed — the JSONB column type is
// specified, and switching it to TEXT to preserve the input spelling would give up
// JSONB indexing and containment queries for a guarantee nothing actually needs.
//
// The guarantees that DO matter are unaffected, because both of them are relative
// to the STORED row rather than to the pre-insert spelling:
//
//   - Dual-delivery consistency holds structurally. The relay publishes to Kafka
//     and enqueues the legacy webhook from the SAME claimed row, so both transports
//     carry the same bytes by construction and cannot drift apart.
//   - Replay fidelity holds. A replay re-publishes the STORED bytes rather than
//     re-marshalling a struct, so a replayed event is identical to the event that
//     was originally published, aside from the added failure metadata.
//
// Repeated reads of one row return identical bytes — the normalisation happens once
// on write, and the stored form is stable thereafter — which is what makes both
// guarantees hold in practice.
//
// The practical consequence for anyone writing a test: compare payloads by JSON
// EQUIVALENCE against the pre-insert value, or by byte equality against a value
// that has been round-tripped through the database. Comparing pre-insert bytes to
// post-read bytes will fail on key order alone and will look like a payload
// corruption bug when nothing is wrong.
package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// eventOutboxColumns is the column list every read of blnk.event_outbox
// projects, in the exact order scanEventOutbox consumes it.
//
// It is declared once because the claim, the single-row fetch and the
// dead-letter listing must project identically: a column added to one query and
// not the others is a scan-order bug that no compiler catches and that surfaces
// only as mis-assigned field values at runtime.
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
// FirstAttemptedAt means "never attempted", which a zero time.Time would
// silently misreport as the year 1.
func scanEventOutbox(s eventOutboxScanner) (model.EventOutbox, error) {
	var e model.EventOutbox
	var ledgerID, lastError, dltTopic sql.NullString
	var firstAttemptedAt, lastAttemptedAt, dispatchedAt, lockedUntil sql.NullTime
	var failureMetadata []byte

	if err := s.Scan(
		&e.ID,
		&e.EventID,
		&e.EventType,
		&e.AggregateID,
		&ledgerID,
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

	e.LedgerID = ledgerID.String
	e.LastError = lastError.String
	e.DLTTopic = dltTopic.String
	if failureMetadata != nil {
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

// eventOutboxInsertQuery is shared by the in-transaction and standalone inserts
// so the two cannot diverge in which columns they populate.
//
// status is left to the column default ('pending') rather than being passed
// explicitly: the row's initial state belongs to the schema, and passing it here
// would create a second place for it to be defined. occurred_at, by contrast, IS
// passed, because it is the domain instant the caller observed and must not be
// replaced by the insert time.
const eventOutboxInsertQuery = `
		INSERT INTO blnk.event_outbox
		(event_id, event_type, aggregate_id, ledger_id, topic, schema_version, payload, occurred_at, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`

// eventOutboxInsertArgs builds the argument list for eventOutboxInsertQuery,
// applying the two defaults a caller may legitimately leave unset.
func eventOutboxInsertArgs(e *model.EventOutbox) []interface{} {
	occurredAt := e.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now()
	}

	// A zero retry budget would make the row un-retryable AND un-claimable,
	// because the claim predicate requires attempts < max_attempts. Falling back
	// to the schema default keeps a caller that forgot to set it from producing a
	// row that silently never publishes.
	maxAttempts := e.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultEventMaxAttempts
	}

	return []interface{}{
		e.EventID,
		e.EventType,
		e.AggregateID,
		e.LedgerID,
		e.Topic,
		e.SchemaVersion,
		[]byte(e.Payload),
		occurredAt,
		maxAttempts,
	}
}

// defaultEventMaxAttempts mirrors the max_attempts column default and the
// configured relay attempt limit. It is the fallback for a row inserted without
// an explicit budget.
const defaultEventMaxAttempts = 5

// InsertEventOutboxInTx inserts an event outbox entry within an existing database
// transaction, so the event is committed atomically with the ledger mutation that
// produced it.
//
// This is the method that makes the transactional-outbox guarantee real: the
// caller has already begun a transaction and applied its balance updates, and
// passing that same *sql.Tx here is what ties the event's fate to theirs. The
// generated id is written back onto the supplied entry so the caller can
// correlate it after the commit.
func (d Datasource) InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	if e == nil {
		return nil
	}

	if err := tx.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert event outbox entry", err)
	}
	return nil
}

// InsertEventOutbox inserts an event outbox entry directly, outside any ledger
// transaction.
//
// It exists for events that have no accompanying ledger mutation to be atomic
// with — internal error notifications, for instance, and ledger and identity
// creation, which do not go through the atomic transaction writers. Such events
// still belong in the outbox so that they get the same durable retry, dead-letter
// and replay treatment as every other event; there is simply no wider transaction
// to enrol them in.
func (d Datasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	if e == nil {
		return nil
	}

	ctx, span := otel.Tracer("transaction.database").Start(ctx, "InsertEventOutbox")
	defer span.End()

	if err := d.Conn.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert event outbox entry", err)
	}

	span.AddEvent("Event outbox entry inserted", trace.WithAttributes(
		attribute.String("event.id", e.EventID),
		attribute.String("event.type", e.EventType),
		attribute.String("event.topic", e.Topic),
	))
	return nil
}

// ClaimPendingEventOutbox claims a batch of pending entries for publishing and
// takes a lease on them for lockDuration.
//
// Concurrency and ordering are both handled in one statement:
//
//   - FOR UPDATE SKIP LOCKED lets several relay instances claim disjoint batches
//     without blocking each other, so horizontal scaling needs no coordination.
//   - The lease in locked_until, rather than an in-memory marker, is what makes a
//     crashed relay's in-flight rows reclaimable: once the lease expires the rows
//     re-enter the claimable set instead of being stranded.
//   - UPDATE ... RETURNING does not preserve the inner ORDER BY, so the claimed
//     rows are re-sorted through a CTE. Without that the batch would come back in
//     arbitrary order and per-aggregate FIFO would be lost inside a batch even
//     though the partition key was correct.
//
// Ordering is by occurred_at ascending — the domain instant — not by insert time.
func (d Datasource) ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingEventOutbox")
	defer span.End()

	query := `
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
				ORDER BY occurred_at ASC
				LIMIT $3
				FOR UPDATE SKIP LOCKED
			)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC
	`

	rows, err := d.Conn.QueryContext(ctx, query, model.EventOutboxStatusProcessing, lockDuration.String(), batchSize)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to claim pending event outbox entries", err)
	}
	defer func() { _ = rows.Close() }()

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

	span.AddEvent("Event outbox entries claimed", trace.WithAttributes(
		attribute.Int("event.claimed_count", len(entries)),
	))
	return entries, nil
}

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged
// the publish.
//
// The lease is released and dispatched_at is stamped in the same statement.
// dispatched is a terminal state: the row is no longer claimable, because the
// claim predicate only admits pending and processing rows.
func (d Datasource) MarkEventDispatched(ctx context.Context, id int64) error {
	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dispatched_at = NOW(), locked_until = NULL
		WHERE id = $2
	`, model.EventOutboxStatusDispatched, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dispatched", err)
	}
	return nil
}

// MarkEventFailed records a failed publish attempt and its reason.
//
// The attempt counter is incremented and the status is decided in SQL rather than
// in Go: if the increment exhausts the budget the row becomes failed, otherwise it
// returns to pending for another attempt. Deciding it in the UPDATE keeps the
// read and the write in one atomic statement, so two relay instances racing on
// the same row cannot both conclude they were the last attempt. The lease is
// released either way, which is what lets the retry actually be picked up.
func (d Datasource) MarkEventFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			last_attempted_at = NOW(),
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			locked_until = NULL
		WHERE id = $4
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark event outbox entry as failed", err)
	}
	return nil
}

// MarkWebhookDispatched records that the legacy HTTP webhook leg was dispatched
// for this entry.
//
// It serves the dual-delivery window only. The relay publishes to Kafka and
// enqueues the legacy webhook task from the SAME claimed row, and this flag makes
// the legacy leg individually idempotent: a row republished to Kafka after a crash
// does not enqueue a second webhook. After the sunset date the legacy leg is gone
// and the flag is simply never set.
func (d Datasource) MarkWebhookDispatched(ctx context.Context, id int64) error {
	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_dispatched = TRUE
		WHERE id = $1
	`, id)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark webhook dispatched for event outbox entry", err)
	}
	return nil
}

// GetEventByID retrieves an entry by its business event_id UUID.
//
// The lookup is by event_id and not by the BIGSERIAL id because that is what the
// dead-letter replay route carries and what a subscriber reports when it asks for
// an event to be resent — the surrogate key is never exposed outside this layer.
// A missing row is returned as a typed not-found error rather than a bare
// sql.ErrNoRows, so the API layer can map it to a 404 without inspecting driver
// sentinels.
func (d Datasource) GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventByID")
	defer span.End()

	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE event_id = $1
	`, eventID)

	entry, err := scanEventOutbox(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, "Event not found", err)
		}
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve event outbox entry", err)
	}

	span.AddEvent("Event outbox entry retrieved", trace.WithAttributes(
		attribute.String("event.id", entry.EventID),
		attribute.String("event.status", entry.Status),
	))
	return &entry, nil
}

// ListDeadLetteredEvents pages the dead-letter inventory behind the dead-letter
// API.
//
// Both terminal failure states are included. A row becomes failed the moment its
// retry budget is spent and dead_lettered only once the event has additionally
// been written to its `<topic>.dlt` sibling; listing only the latter would hide
// events that exhausted their retries but whose dead-letter publication itself
// failed — exactly the events an operator most needs to see.
//
// Ordering is by occurred_at descending, newest first, because triage starts from
// the most recent failures. The limit is bounded so a caller cannot ask for the
// whole table.
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

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		ORDER BY occurred_at DESC
		LIMIT $3 OFFSET $4
	`, model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to list dead-lettered events", err)
	}
	defer func() { _ = rows.Close() }()

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

	span.AddEvent("Dead-lettered events listed", trace.WithAttributes(
		attribute.Int("event.dead_lettered_count", len(entries)),
	))
	return entries, nil
}

// Page-size bounds for the dead-letter listing. The default keeps an unqualified
// request cheap; the maximum stops a caller turning a triage endpoint into a full
// table scan.
const (
	defaultDeadLetterPageSize = 50
	maxDeadLetterPageSize     = 500
)

// CountEventOutboxByStatus returns a status-keyed count of every row in
// blnk.event_outbox.
//
// It backs the daily zero-loss reconciliation: the check passes when the
// dispatched plus dead-lettered counts equal the sum of the main-topic and
// dead-letter-topic end offsets reported by the broker. It also feeds the
// pending-backlog gauge and the event statistics endpoint.
//
// A status with no rows is ABSENT from the returned map rather than present with
// a zero, because GROUP BY only produces rows that exist. Callers must therefore
// read it with the two-value form or accept the zero value — which is what the
// backlog gauge does when nothing is pending.
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
	defer func() { _ = rows.Close() }()

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

	span.AddEvent("Event outbox status counts computed", trace.WithAttributes(
		attribute.Int("event.status_count", len(counts)),
	))
	return counts, nil
}

// insertEventOutboxesInTx inserts a batch of event outbox entries inside an
// existing transaction, skipping nil entries.
//
// The bulk atomic writers hand their variadic event rows straight to this
// helper, so an empty or all-nil batch is a no-op rather than an error — which is
// what lets a caller that is not capturing events omit the variadic argument
// entirely.
func insertEventOutboxesInTx(ctx context.Context, tx *sql.Tx, entries []*model.EventOutbox) error {
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if err := tx.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(entry)...).Scan(&entry.ID); err != nil {
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert event outbox entry", err)
		}
	}
	return nil
}
