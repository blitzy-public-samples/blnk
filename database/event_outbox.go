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

// event_outbox.go is the repository implementation of the eventOutbox contract declared
// in repository.go: persistence for blnk.event_outbox, the transactional outbox that
// carries every ledger mutation's event to Kafka.

package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// defaultEventMaxAttempts mirrors the max_attempts column default and the default of
	// RELAY_MAX_RETRY_ATTEMPTS. It is the fallback for a row inserted without an explicit
	// retry budget: a zero budget would make the row both un-retryable AND un-claimable,
	// because the claim predicate requires attempts < max_attempts, so it would silently
	// never publish.
	defaultEventMaxAttempts = 5

	// defaultEventClaimLease is the lease applied when a caller asks for a non-positive
	// one. A zero-length lease would expire the instant it was taken, letting a second
	// relay instance reclaim a row that is still being published and delivering the event
	// twice; falling back to the value the outbox relays already use is safer than
	// honouring an obviously wrong argument.
	defaultEventClaimLease = 30 * time.Second

	// eventDeadLetterHandoffLease is the lease a terminal transition LEAVES ON THE ROW so
	// that the dead-letter write it owes is exclusively its own.
	eventDeadLetterHandoffLease = 30 * time.Second

	// Page-size bounds for the dead-letter listing. The default keeps an
	// unqualified request cheap; the maximum stops a caller turning a triage
	// endpoint into a full table scan.
	defaultDeadLetterPageSize = 50
	maxDeadLetterPageSize     = 500

	// Slice-size bounds for the retention purge. The default keeps one sweep short enough
	// that it never holds locks long against a table the relay is concurrently claiming
	// from; the maximum stops a caller asking for a delete large enough to block the relay
	// and bloat the WAL in one transaction. A caller sweeps in a loop until fewer than its
	// limit are returned.
	defaultEventPurgeBatchSize = 1000
	maxEventPurgeBatchSize     = 10000
)

// eventOutboxColumns is the column list every read of blnk.event_outbox projects, in
// the exact order scanEventOutbox consumes it.
const eventOutboxColumns = `id, event_id, event_type, aggregate_id, partition_key, ledger_id, topic, schema_version, ` +
	`payload_raw, event_raw, occurred_at, status, attempts, max_attempts, next_attempt_at, last_error, first_attempted_at, ` +
	`last_attempted_at, dispatched_at, locked_until, claim_token, webhook_dispatched, kafka_dispatched_at, ` +
	`webhook_attempts, kafka_topic, kafka_partition, kafka_offset, dlt_topic, failure_metadata, traceparent, tracestate`

// eventOutboxScanner is the minimum surface scanEventOutbox needs, satisfied by
// both *sql.Row and *sql.Rows. Sharing one scanner between the single-row and
// multi-row paths is what keeps their column handling from drifting apart.
type eventOutboxScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventOutbox decodes one blnk.event_outbox row into a model.EventOutbox.
func scanEventOutbox(s eventOutboxScanner) (model.EventOutbox, error) {
	var e model.EventOutbox
	var ledgerID, lastError, claimToken, dltTopic, kafkaTopic sql.NullString
	var traceparent, tracestate sql.NullString
	var firstAttemptedAt, lastAttemptedAt, dispatchedAt, lockedUntil, kafkaDispatchedAt sql.NullTime
	var kafkaPartition sql.NullInt32
	var kafkaOffset sql.NullInt64
	var failureMetadata []byte

	if err := s.Scan(
		&e.ID,
		&e.EventID,
		&e.EventType,
		&e.AggregateID,
		&e.PartitionKey,
		&ledgerID,
		&e.Topic,
		&e.SchemaVersion,
		&e.Payload,
		// The CANONICAL event value, read back as the bytes the broker was given. Every
		// publish, dead-letter write and replay takes it from here rather than rebuilding
		// the envelope from the columns around it — see model.EventOutbox.EventRaw.
		&e.EventRaw,
		&e.OccurredAt,
		&e.Status,
		&e.Attempts,
		&e.MaxAttempts,
		&e.NextAttemptAt,
		&lastError,
		&firstAttemptedAt,
		&lastAttemptedAt,
		&dispatchedAt,
		&lockedUntil,
		&claimToken,
		&e.WebhookDispatched,
		&kafkaDispatchedAt,
		&e.WebhookAttempts,
		&kafkaTopic,
		&kafkaPartition,
		&kafkaOffset,
		&dltTopic,
		&failureMetadata,
		// The trace context of the request that captured this event. NULL for every event
		// captured without an active trace, which is a legitimate and common state — see
		// model.EventOutbox.Traceparent.
		&traceparent,
		&tracestate,
	); err != nil {
		return model.EventOutbox{}, err
	}

	// ledger_id is NULLABLE and NULL means "this event has no ledger" as a positive fact —
	// see the column comment in the migration. It collapses to the empty string on the Go
	// side because model.EventOutbox.LedgerID is a plain string tagged omitempty, so both
	// spellings render identically on the wire and no caller has to branch on a pointer.
	e.LedgerID = ledgerID.String
	e.LastError = lastError.String
	// A non-empty claim token means some worker holds this row right now, and it is
	// the value every conditional transition must present to be allowed to write.
	e.ClaimToken = claimToken.String
	e.DLTTopic = dltTopic.String
	// Collapsed to the empty string for the same reason ledger_id is: the model fields are
	// plain strings tagged omitempty, so "no trace was recorded" has one spelling on the Go
	// side and no reader has to branch on a pointer to ask the question.
	e.Traceparent = traceparent.String
	e.Tracestate = tracestate.String
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
	// SUNSET: this field and the column behind it go with the legacy transport. Until
	// then a non-nil value is what tells the relay this row's Kafka leg is already
	// done, so a re-claim delivers only the outstanding webhook.
	if kafkaDispatchedAt.Valid {
		e.KafkaDispatchedAt = &kafkaDispatchedAt.Time
	}
	if lockedUntil.Valid {
		e.LockedUntil = &lockedUntil.Time
	}
	// The broker coordinate is all-or-nothing, which the check constraint enforces in the
	// schema and this reads back the same way. Assigning the partition or the offset
	// without the topic would produce a row that looks confirmed to a reader testing only
	// the offset and has nothing to look the record up with — see
	// model.EventOutbox.BrokerRecord.
	if kafkaTopic.Valid && kafkaPartition.Valid && kafkaOffset.Valid {
		partition := int(kafkaPartition.Int32)
		offset := kafkaOffset.Int64
		e.KafkaTopic = kafkaTopic.String
		e.KafkaPartition = &partition
		e.KafkaOffset = &offset
	}

	return e, nil
}

// eventOutboxInsertQuery is shared by the in-transaction insert, the standalone insert
// and the batch insert, so none of the three can diverge in which columns it populates.
const eventOutboxInsertColumns = `event_id, event_type, aggregate_id, partition_key, ledger_id, topic, ` +
	`schema_version, payload, payload_raw, event_raw, occurred_at, status, max_attempts, traceparent, tracestate`

const eventOutboxInsertQuery = `
		INSERT INTO blnk.event_outbox
		(` + eventOutboxInsertColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		RETURNING id
	`

// eventOutboxInsertValueCount is the number of bound values per inserted row. It
// keeps the batch insert's placeholder arithmetic tied to the column list above
// rather than to a literal that a later column addition would leave stale.
const eventOutboxInsertValueCount = 15

// maxEventOutboxInsertRows bounds how many rows one multi-row INSERT may carry, and it
// is a hard requirement rather than tuning.
const maxEventOutboxInsertRows = 1000

// validateEventOutboxEntry is the persistence-boundary gate on every field of an event
// row, and it runs before ANY insert path — in-transaction, standalone or batched. It
// converts each rejection into a typed bad-request rather than letting it surface as an
// opaque driver error raised deep inside the caller's ledger transaction, or worse, as
// a row that is accepted and then cannot be published.
func validateEventOutboxEntry(e *model.EventOutbox) error {
	if e == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry is required", nil)
	}
	if !model.IsCanonicalUUID(e.EventID) {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox entry requires an event ID in canonical UUID form", nil)
	}
	if strings.TrimSpace(e.EventType) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires an event type", nil)
	}
	if strings.TrimSpace(e.AggregateID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires an aggregate ID", nil)
	}
	if strings.TrimSpace(e.PartitionKey) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires a partition key", nil)
	}
	if !model.IsBlnkEventTopicUnderAnyPrefix(e.Topic, expectedEventTopicPrefixes()) {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox entry requires a Blnk-owned topic", nil)
	}
	if e.SchemaVersion != model.SchemaVersionV1 {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox entry requires schema version %d", model.SchemaVersionV1), nil)
	}
	if len(e.Payload) == 0 {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry requires a payload", nil)
	}
	if len(e.Payload) > model.MaxEventMessageBytes {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox payload exceeds the %d byte maximum", model.MaxEventMessageBytes), nil)
	}
	if !json.Valid(e.Payload) {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox payload is not valid JSON", nil)
	}

	// THE CANONICAL ENVELOPE IS REQUIRED, and reaching here without one is a defect in
	// this file rather than in the caller: normalizeEventOutboxEntry derives it for every
	// entry it is handed. It is checked anyway because the column is NOT NULL, and a
	// constraint violation raised inside a caller's ledger transaction is a far worse way
	// to learn about it than a named failure — the whole reason this function exists.
	if len(bytes.TrimSpace(e.EventRaw)) == 0 {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox entry requires its canonical event envelope", nil)
	}
	if len(e.EventRaw) > model.MaxEventMessageBytes {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox canonical envelope exceeds the %d byte maximum", model.MaxEventMessageBytes), nil)
	}
	if !json.Valid(e.EventRaw) {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox canonical envelope is not valid JSON", nil)
	}

	return nil
}

// expectedEventTopicPrefixes resolves every topic namespace this deployment owns, for
// the topic-membership check above.
func expectedEventTopicPrefixes() []string {
	cnf, err := config.Fetch()
	if err != nil || cnf == nil {
		return []string{model.DefaultEventTopicPrefix}
	}

	prefixes := cnf.Kafka.OwnedTopicPrefixes()
	if len(prefixes) == 0 {
		return []string{model.DefaultEventTopicPrefix}
	}

	return prefixes
}

// configuredEventTopicPrefix resolves the CURRENT topic namespace only —
// KAFKA_TOPIC_PREFIX, falling back to model.DefaultEventTopicPrefix.
func configuredEventTopicPrefix() string {
	return expectedEventTopicPrefixes()[0]
}

// prepareEventOutboxEntry normalises then validates one entry, in that order, and is
// the single sequence every insert path uses.
func prepareEventOutboxEntry(e *model.EventOutbox) error {
	if e == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry is required", nil)
	}
	if err := normalizeEventOutboxEntry(e); err != nil {
		return err
	}

	return validateEventOutboxEntry(e)
}

// normalizeEventOutboxEntry applies the defaults a caller may legitimately leave unset
// and pins the initial status, mutating the entry so that it describes exactly what is
// about to be persisted. Callers read these fields back after the insert — the relay
// and the dual-delivery leg both work from the in-memory row — so leaving the struct
// disagreeing with the stored row would be a trap.
func normalizeEventOutboxEntry(e *model.EventOutbox) error {
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

	// THE TRACE CONTEXT IS SANITISED, NEVER VALIDATED INTO A REJECTION.
	e.Traceparent, e.Tracestate = model.SanitizeTraceContext(e.Traceparent, e.Tracestate)

	// THE CANONICAL ENVELOPE IS DERIVED HERE WHEN A CALLER LEFT IT EMPTY, which is what
	// makes "every row carries the value the broker was given" an invariant rather than a
	// convention. The producer sets it (PrepareEventOutbox), and a row assembled anywhere
	// else — a test fixture, a future caller, a migration backfill — gets the same bytes
	// this version would have stored, composed from the envelope fields already normalised
	// above. Deriving it AFTER the schema version and the occurrence instant is not
	// optional: both are members of the envelope, so composing first would freeze a zero
	// version or a zero timestamp into the stored value.
	if len(bytes.TrimSpace(e.EventRaw)) == 0 {
		canonical, err := e.CanonicalEvent().CanonicalBytes()
		if err != nil {
			return apierror.NewAPIError(
				apierror.ErrBadRequest,
				"The event payload could not be serialized into its canonical envelope",
				fmt.Errorf("database: composing the canonical envelope for event %q: %w", e.EventID, err),
			)
		}

		e.EventRaw = canonical
	}

	return nil
}

// eventOutboxInsertArgs builds the bound values for one row, in eventOutboxInsertColumns order.
func eventOutboxInsertArgs(e *model.EventOutbox) []interface{} {
	// ledger_id is bound as SQL NULL when the event genuinely has no ledger, not as
	// the empty string. The column is nullable precisely so that "no ledger" is a
	// statable fact, and two spellings of one meaning is how a query that filters on
	// one of them silently misses rows. The non-blank CHECK constraint on the column
	// rejects '' as well, so this is required and not merely tidy.
	var ledgerIDArg interface{}
	if trimmed := strings.TrimSpace(e.LedgerID); trimmed != "" {
		ledgerIDArg = trimmed
	}

	payload := []byte(e.Payload)

	return []interface{}{
		e.EventID,
		e.EventType,
		e.AggregateID,
		e.PartitionKey,
		ledgerIDArg,
		e.Topic,
		e.SchemaVersion,
		// ONE slice serves BOTH body columns, and that is the mechanism by which the JSONB
		// projection and the byte-preserving column cannot disagree: PostgreSQL parses this
		// value on its way into payload and stores it verbatim in payload_raw. Binding them
		// from two expressions — or re-marshalling for one of them — is how they would drift,
		// and the drift would be invisible until a replay was compared byte for byte.
		payload,
		payload,
		// THE CANONICAL ENVELOPE, stored rather than rebuilt on every publish. It is never
		// nil here: prepareEventOutboxEntry derives it from the envelope fields when a caller
		// left it empty, and validateEventOutboxEntry refuses a row without one, so this
		// binding cannot violate the column's NOT NULL.
		[]byte(e.EventRaw),
		e.OccurredAt,
		model.EventOutboxStatusPending,
		e.MaxAttempts,
		// SQL NULL rather than '' when there was no trace, so the CHECK constraints on the
		// two columns never see an empty string and a query asking "which events carry a
		// trace" has one answer to test for. normalizeEventOutboxEntry has already dropped
		// anything unusable, so what reaches here is either a legal value or nothing.
		nullableTraceValue(e.Traceparent),
		nullableTraceValue(e.Tracestate),
	}
}

// nullableTraceValue binds a trace-context value, or SQL NULL when it is absent.
func nullableTraceValue(value string) interface{} {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}

	return nil
}

// hashedEventIdentifier turns a financial identifier into a stable, non-reversible
// token for a log field.
func hashedEventIdentifier(value string) string {
	return model.HashIdentifier(value)
}

// wrapEventOutboxInsertError turns a driver failure into the typed error the rest of
// the system can act on, following the discrimination UpsertLineageMapping performs on
// *pq.Error.
func wrapEventOutboxInsertError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code.Name() {
		case "unique_violation":
			return loggedDatabaseError(apierror.ErrConflict, "Event outbox entry already exists", "wrap_event_outbox_insert_error", err)
		case "foreign_key_violation":
			return loggedDatabaseError(apierror.ErrBadRequest, "Invalid event outbox entry reference", "wrap_event_outbox_insert_error", err)
		case "not_null_violation":
			return loggedDatabaseError(apierror.ErrBadRequest, "Event outbox entry is missing a required field", "wrap_event_outbox_insert_error", err)
		case "check_violation":
			// The schema's CHECK constraints are the backstop beneath validateEventOutboxEntry.
			return loggedDatabaseError(apierror.ErrBadRequest, "Event outbox entry violates a field constraint", "wrap_event_outbox_insert_error", err)
		default:
			return loggedDatabaseError(apierror.ErrInternalServer, "Database error occurred", "wrap_event_outbox_insert_error", err)
		}
	}
	return loggedDatabaseError(apierror.ErrInternalServer, "Failed to insert event outbox entry", "wrap_event_outbox_insert_error", err)
}

// InsertEventOutboxInTx inserts an event outbox entry within an existing database
// transaction, so the event is committed atomically with the ledger mutation that
// produced it.
func (d Datasource) InsertEventOutboxInTx(ctx context.Context, tx *sql.Tx, e *model.EventOutbox) error {
	if err := prepareEventOutboxEntry(e); err != nil {
		return err
	}

	if err := tx.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		return wrapEventOutboxInsertError(err)
	}
	return nil
}

// sqlExecer is the minimum surface an entity INSERT needs, satisfied by both *sql.DB
// and *sql.Tx.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// sqlQueryer is the minimum surface a READ needs, satisfied by both *sql.DB and
// *sql.Tx.
type sqlQueryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// EventPreparer builds the outbox row for an entity that has JUST BEEN INSERTED, and is
// called from inside the transaction that inserted it.
type EventPreparer[T any] func(entity T) (*model.EventOutbox, error)

// firstEventPreparer resolves a variadic preparer tail to the single preparer to use,
// or nil when none was supplied.
func firstEventPreparer[T any](preparers []EventPreparer[T]) EventPreparer[T] {
	for _, prepare := range preparers {
		if prepare != nil {
			return prepare
		}
	}

	return nil
}

// captureEntityEvent runs a preparer against a just-inserted entity and inserts the row
// it returns inside the caller's transaction.
func captureEntityEvent[T any](ctx context.Context, d Datasource, tx *sql.Tx, entity T, prepare EventPreparer[T]) error {
	row, err := prepare(entity)
	if err != nil {
		return err
	}

	if row == nil {
		// Publishing is not configured. The entity insert stands on its own, which is the
		// no-op-when-unconfigured contract this pipeline inherited from SendWebhook.
		return nil
	}

	return d.InsertEventOutboxInTx(ctx, tx, row)
}

// InsertEventOutbox inserts an event outbox entry directly, outside any ledger
// transaction.
func (d Datasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	if err := prepareEventOutboxEntry(e); err != nil {
		return err
	}

	ctx, span := otel.Tracer("transaction.database").Start(ctx, "InsertEventOutbox")
	defer span.End()
	span.SetAttributes(
		attribute.String("event_outbox.event_id", e.EventID),
		attribute.String("event_outbox.event_type", e.EventType),
		attribute.String("event_outbox.topic", e.Topic),
	)

	if err := d.Conn.QueryRowContext(ctx, eventOutboxInsertQuery, eventOutboxInsertArgs(e)...).Scan(&e.ID); err != nil {
		// AN IDENTICAL EVENT ALREADY RECORDED IS SUCCESS, not loss, and it must be resolved
		// before anything below runs. See resolveDuplicateEventOutboxInsert.
		if adopted := d.resolveDuplicateEventOutboxInsert(ctx, e, err); adopted {
			span.SetAttributes(attribute.Bool("event_outbox.idempotent_insert", true))

			return nil
		}

		failDatabaseSpan(span, err)
		// Loud by design. This is the non-atomic path, so a failure here means the domain
		// mutation that produced this event is already committed and the event is gone.
		logrus.WithFields(logrus.Fields{
			"event_id":          e.EventID,
			"event_type":        e.EventType,
			"topic":             e.Topic,
			"aggregate_id_hash": hashedEventIdentifier(e.AggregateID),
			"atomic":            false,
			"error_class":       databaseErrorClass(err),
			"sqlstate":          postgresSQLState(err),
		}).Error("event lost: non-atomic outbox insert failed after its mutation was committed")
		logDatabaseDiagnostic("insert_event_outbox_non_atomic", err)

		return wrapEventOutboxInsertError(err)
	}
	return nil
}

// resolveDuplicateEventOutboxInsert decides whether a failed insert is actually the
// event ALREADY BEING RECORDED, and adopts the stored row's identity when it is.
func (d Datasource) resolveDuplicateEventOutboxInsert(
	ctx context.Context,
	e *model.EventOutbox,
	cause error,
) bool {
	var pqErr *pq.Error
	if !errors.As(cause, &pqErr) || pqErr.Code.Name() != "unique_violation" {
		return false
	}

	stored, err := d.GetEventByID(ctx, e.EventID)
	if err != nil || stored == nil {
		// Nothing established. The caller reports the original failure, which is the safe
		// direction: a possible loss that could not be ruled out must stay visible.
		return false
	}

	// The canonical envelope is the whole event, so byte equality here is identity.
	if len(stored.EventRaw) == 0 || !bytes.Equal(stored.EventRaw, e.EventRaw) {
		return false
	}

	e.ID = stored.ID

	logrus.WithFields(logrus.Fields{
		"event_id":          e.EventID,
		"event_type":        e.EventType,
		"topic":             e.Topic,
		"aggregate_id_hash": hashedEventIdentifier(e.AggregateID),
		"outbox_id":         stored.ID,
	}).Info(
		"event outbox insert was already durable: the unique index refused a duplicate of an " +
			"identical event, so the capture is complete and the insert is idempotent",
	)

	return true
}

// insertEventOutboxesInTx inserts a batch of event outbox entries inside an existing
// transaction, in a single round trip.
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
		if err := prepareEventOutboxEntry(entry); err != nil {
			return err
		}
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
			return loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan inserted event outbox entry", "insert_event_outbox_chunk_in_tx", scanErr)
		}
		// The generated ids come back keyed by event_id rather than by position,
		// because RETURNING makes no promise about row order.
		if entry, ok := entriesByEventID[eventID]; ok {
			entry.ID = id
		}
		inserted++
	}

	if err := rows.Err(); err != nil {
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed while iterating inserted event outbox entries", "insert_event_outbox_chunk_in_tx", err)
	}

	// A short count means at least one event was silently not persisted, which
	// would be a lost event the moment the surrounding transaction committed.
	// Returning an error rolls that transaction back instead.
	if inserted != len(entriesByEventID) {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to insert all event outbox entries", nil)
	}

	return nil
}

// eventOutboxEffectiveKeySQL renders model.EffectivePartitionKey as SQL for one
// relation alias, and it is the ONLY place that spelling exists in this package.
func eventOutboxEffectiveKeySQL(alias string) string {
	return `COALESCE(NULLIF(btrim(` + alias + `.ledger_id), ''), btrim(` + alias + `.partition_key))`
}
