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
//
// It is modelled directly on lineage.go's outbox half — the same in-transaction insert,
// the same CTE claim with FOR UPDATE SKIP LOCKED, and the same apierror-wrapped
// failures — because that is the proven house pattern for a relay-backed outbox in this
// repository, and a second, differently shaped one would be a maintenance liability
// rather than an improvement. blnk.event_outbox and blnk.lineage_outbox are nonetheless
// SEPARATE TABLES served by SEPARATE RELAYS: they are never merged, nothing here joins
// across them, and the lineage relay's behaviour is untouched.
//
// Three differences from the lineage outbox are deliberate and load-bearing:
//
//   - Ordering is by occurred_at, not created_at. occurred_at is the instant the domain
//     action happened; created_at is merely when the row was written.
//   - The terminal success state is dispatched, and there is an additional
//     dead_lettered terminal state.
//   - Every standalone method opens an OpenTelemetry span on the transaction.database
//     tracer, following chain.go.
//
// The event body is stored TWICE, and which column a reader picks decides whether the
// fidelity guarantee holds:
//
//   - payload is JSONB, a PARSED representation. PostgreSQL sorts object keys,
//     renormalises whitespace, expands exponent notation and collapses a duplicate key,
//     so a body marshaled as {"event":"x","data":{...}} comes back as {"data": {...},
//     "event": "x"}.
//   - payload_raw is BYTEA and holds THE EXACT BYTES the producer marshaled.
//
// EVERY READ IN THIS FILE PROJECTS payload_raw INTO model.EventOutbox.Payload, never
// the JSONB column — see eventOutboxColumns. That is what makes the two delivery
// guarantees structural rather than a matter of careful coding:
//
//   - Dual-delivery consistency. The relay publishes to Kafka and enqueues the legacy
//     webhook from the SAME claimed row, and the body it carries is the producer's own
//     bytes, so the two transports cannot differ from each other OR from what the HTTP
//     webhook body would have been.
//   - Replay fidelity. A replay re-publishes the STORED bytes rather than
//     re-marshalling a struct, so a replayed event is byte-identical to the event
//     originally published, aside from the added failure metadata.

package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	//
	// MarkEventFailed's exhaustion arm and MarkEventPermanentlyFailed both retain the
	// claim token, precisely so that only the worker that spent the last attempt may
	// perform the dead-letter write and the MarkEventDeadLettered that records it. So a
	// second relay instance could reclaim the row in the same instant, overwrite the token
	// the first worker was about to present, and publish to the dead-letter topic
	// concurrently with it — two copies of one event on the dead-letter topic, and the
	// first worker's transition then failing with a lost claim it never actually lost.
	//
	// Holding a lease across the hand-off is what makes the retained token mean something:
	// the repair path cannot see the row until the lease lapses, by which time the owning
	// worker has either recorded the dead-letter write — MarkEventDeadLettered clears both
	// the lease and the token — or died, which is exactly when the repair path SHOULD take
	// it.
	//
	// It equals defaultEventClaimLease deliberately. The hand-off is one broker write plus
	// one UPDATE, the same shape of work an ordinary claim covers, and a longer lease
	// would only delay repair after a crash while a shorter one would reintroduce the race
	// under load.
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
//
// It is declared once because the claim, the single-row fetch and the dead-letter
// listing must project identically: a column added to one query and not the others is a
// scan-order bug that no compiler catches and that surfaces only as mis-assigned field
// values at runtime.
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
//
// Every nullable column is read through a sql.Null* wrapper and then normalised:
// nullable text becomes the empty string, and nullable timestamps become nil pointers
// rather than zero times. That distinction matters — a nil FirstAttemptedAt means
// "never attempted", which a zero time.Time would silently misreport as the year 1.
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
// The batch insert reuses the same column list and repeats the same value tuple once
// per row; every other detail is identical.
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
//
// Each rule, and the specific failure it prevents:
//
//   - Nil entry: a programming error; dereferencing it would panic.
//   - event_id must be a canonical hyphenated UUID. It is the SUBSCRIBER'S IDEMPOTENCY
//     KEY, so a non-UUID or alternatively-spelled value is a key a consumer cannot
//     deduplicate on reliably.
//   - event_type must be non-blank. It is what subscribers filter on and what the relay
//     routes by; blank is unroutable.
//   - aggregate_id must be non-blank. It is what consumers group by and what the
//     ordering audit queries on.
//   - partition_key must be non-blank. A blank key makes Kafka scatter the event
//     round-robin, which silently destroys the per-aggregate ordering guarantee with
//     nothing in the row to show it happened.
//   - topic must be non-blank AND lie inside the Blnk-owned topic namespace.
//   - schema_version must be a version this build actually emits. It reaches
//     subscribers on the wire, where an unknown value is an unknown envelope shape.
//   - payload must be present, must be valid JSON, and must fit within
//     model.MaxEventMessageBytes.
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
//
// The widening reaches nothing a producer can steer. A new row's topic is composed by
// TopicForEvent from the CONFIGURED prefix alone, so a historical name cannot arrive on
// an insert in the first place; what the shared definition buys is that the two layers
// cannot disagree, not extra reach for a caller.
//
// Returns:
//   - []string: one or more distinct, non-blank prefixes, the configured one first.
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
//
// Returns:
//   - string: a non-empty prefix.
func configuredEventTopicPrefix() string {
	return expectedEventTopicPrefixes()[0]
}

// prepareEventOutboxEntry normalises then validates one entry, in that order, and is
// the single sequence every insert path uses.
//
// The order is not interchangeable. Normalisation supplies the schema version, the
// retry budget and the occurrence instant that a caller may legitimately leave unset;
// validating first would reject a perfectly ordinary entry for missing a value this
// layer is responsible for providing.
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
	//
	// SanitizeTraceContext owns the rules, including that a tracestate cannot outlive the
	// traceparent it describes.
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
//
// The payload is passed through as its raw bytes. It is neither re-marshalled nor
// round-tripped through a map, because those bytes are the legacy webhook body
// verbatim and both delivery guarantees are written against them.
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
//
// Its own function rather than two inline conditionals because the two columns need
// identical treatment and an asymmetry between them would be invisible: a tracestate
// stored as ” beside a NULL traceparent reads as "vendor state for no trace", which is
// not a state that exists.
//
// Parameters:
//   - value string: the trimmed trace value, or empty when there is none.
//
// Returns:
//   - interface{}: the value, or nil so the driver binds SQL NULL.
func nullableTraceValue(value string) interface{} {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}

	return nil
}

// hashedEventIdentifier turns a financial identifier into a stable, non-reversible
// token for a log field.
//
// It keeps the one property a log needs — the same identifier always produces the same
// token — and gives up the one it does not need, the identifier itself. An empty input
// returns an empty string rather than the digest of the empty string, so "no aggregate"
// and "some aggregate" stay distinguishable.
//
// Parameters:
//   - value string: the identifier. May be empty.
//
// Returns:
//   - string: a short hex token of model.LogIdentifierHashLength characters, or "" for
//     an empty input.
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
			// Reaching one means a value slipped past that gate, so this is a bad request and
			// not a server fault — but it is also worth noticing, because the two layers are
			// supposed to agree.
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
//
// No span is opened. This runs inside a caller-owned transaction whose span is already
// open in transaction.go, and one span per inserted row would bury that trace in noise
// for no diagnostic gain.
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
//
// So the caller passes a function instead of a row: the writer inserts the entity,
// hands the finished value to this function, and inserts whatever row comes back — all
// inside one transaction, which is the requirement for these three producers.
type EventPreparer[T any] func(entity T) (*model.EventOutbox, error)

// firstEventPreparer resolves a variadic preparer tail to the single preparer to use,
// or nil when none was supplied.
//
// Only the FIRST non-nil preparer is used. One entity produces one creation event; a
// second preparer would mean two events for one mutation, which is a duplicate
// publication the subscriber cannot distinguish from a genuine repeat.
//
// Parameters:
//   - preparers []EventPreparer[T]: the caller's variadic tail, possibly empty.
//
// Returns:
//   - EventPreparer[T]: the preparer to run, or nil when there is nothing to capture.
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
//
// It is a free function rather than a method because Go does not permit type parameters
// on methods; the datasource is passed explicitly so the insert still goes through the
// one exported in-transaction insert every atomic writer uses.
//
// Parameters:
//   - ctx context.Context: the writer's context.
//   - d Datasource: the datasource whose InsertEventOutboxInTx performs the insert.
//   - tx *sql.Tx: the open transaction the entity was inserted in.
//   - entity T: the finished entity, exactly as it will be returned to the caller.
//   - prepare EventPreparer[T]: the caller's preparer. Must not be nil.
//
// Returns:
//   - error: the preparer's error, or the insert's error. Either aborts the
//     transaction, so the entity is not created without its event.
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
//
// The transactional-outbox guarantee belongs to InsertEventOutboxInTx and to it alone.
// This method opens no transaction and joins none, so it provides durable retry,
// dead-lettering and replay for the event, and NOTHING about atomicity.
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
		// Returning the error alone would make that invisible at any call site that logs and
		// continues.
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
//
// event_outbox_event_id_uidx exists to make "one event is recorded once" an invariant
// the database enforces. When it rejects an insert it is reporting that the invariant
// HOLDS — the event is durable, exactly once, and every downstream guarantee built on
// it is intact.
//
// The stored row's surrogate id is adopted so the caller ends up with the same
// fully-identified row a first-time insert would have produced. Without that, an
// idempotent success would hand back a row with ID 0, which the dead-letter path
// explicitly refuses.
//
// Parameters:
//   - ctx context.Context: the insert's context, reused for the lookup.
//   - e *model.EventOutbox: the row that failed to insert. Its ID is populated on
//     adoption.
//   - cause error: the driver error the insert returned.
//
// Returns:
//   - bool: true when the event is already recorded identically and the caller may
//     treat the insert as having succeeded.
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
	// Compared as bytes rather than field by field because that is exactly what a
	// subscriber received, and because the columns cannot be compared directly —
	// occurred_at comes back truncated to the microsecond PostgreSQL stores, while these
	// bytes round-trip unchanged.
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
//
// One multi-row INSERT is used rather than a loop of single inserts because the
// coalescing path can present a large batch inside an already-open ledger transaction,
// where every extra round trip lengthens the window during which the transaction holds
// its balance row locks.
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
//
// The Kafka message key is NOT the stored column. Partitioning is by ledger id, so the
// publish path keys on ledger_id when the row carries one and falls back to
// partition_key only when it does not — model.EffectivePartitionKey, applied by
// model.EventOutbox.EffectiveKey, by the publisher and by the relay's grouping.
//
//	COALESCE(NULLIF(btrim(ledger_id), <empty>), btrim(partition_key))
//
// Parameters:
//   - alias string: the relation alias the columns are qualified with.
//
// Returns:
//   - string: the SQL expression, parenthesised so it composes into any predicate.
func eventOutboxEffectiveKeySQL(alias string) string {
	return `COALESCE(NULLIF(btrim(` + alias + `.ledger_id), ''), btrim(` + alias + `.partition_key))`
}

// claimPendingEventOutboxQuery claims a batch of publishable rows, takes a lease on
// them and stamps a fresh claim token, all in one statement. It is a package-level
// variable rather than a local so its text is reachable from tests, which assert that
// FOR UPDATE SKIP LOCKED, the occurred_at ordering and the earlier-same-key exclusion
// are all still present — the three properties a well-meaning refactor is most likely
// to drop. A variable and not a constant only because the effective-key expression is
// composed from eventOutboxEffectiveKeySQL rather than written twice; nothing assigns
// to it after initialisation.
//
// UPDATE... RETURNING does not preserve the inner ORDER BY, so the claimed rows are
// re-ordered through a CTE to guarantee FIFO delivery within the batch.
//
// Ordering is by occurred_at, NOT created_at as the lineage claim uses. This is the one
// deliberate divergence from that query, and it carries the per-aggregate ordering
// guarantee: occurred_at is the instant the domain action happened, whereas created_at
// is when the row happened to be written.
//
// The NOT EXISTS clause is the correctness heart of this query. Read it before changing
// anything here.
//
// SUNSET: this paragraph and the literal it describes go with the legacy transport.
//
// Nothing else about the candidate selection changes: the WHERE clause, the literal
// status list, the ordering and FOR UPDATE SKIP LOCKED are all preserved verbatim, so
// every partial index this query depends on remains usable and the plan assertions in
// the tests still hold.
var claimPendingEventOutboxQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('pending', 'processing', 'webhook_pending')
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.attempts < candidate.max_attempts
			  AND candidate.next_attempt_at <= NOW()
			  AND NOT EXISTS (
				SELECT 1 FROM blnk.event_outbox earlier
				WHERE ` + eventOutboxEffectiveKeySQL("earlier") + ` = ` + eventOutboxEffectiveKeySQL("candidate") + `
				  AND earlier.status IN ('pending', 'processing')
				  AND (earlier.occurred_at, earlier.id) < (candidate.occurred_at, candidate.id)
			  )
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET status = $1,
				locked_until = NOW() + $2::interval,
				claim_token = $3,
				first_attempted_at = COALESCE(first_attempted_at, NOW()),
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingEventOutbox claims a batch of pending entries for publishing, takes a
// lease on them for lockDuration and stamps every claimed row with one fresh claim
// token. The returned entries are ordered oldest occurrence first, which is the order
// they must be published in, and each carries the token in its ClaimToken field — every
// transition the caller subsequently performs must present it.
func (d Datasource) ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingEventOutbox")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive event outbox lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending event outbox entries", "claim_pending_event_outbox", err)
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
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_event_outbox", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_event_outbox", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimFailedEventOutboxForDeadLetterQuery claims rows whose retry budget is spent and
// whose dead-letter PRESERVATION has not happened, so it can be attempted again.
//
// The exhaustion arm of MarkEventFailed sets status = 'failed' and hands the claim
// token to the worker that spent the last attempt, which then writes the event to its
// `<topic>.dlt` sibling and records the result. When that write fails — no transport, a
// broker outage, a topic that does not exist yet — the row is left at 'failed' with
// dlt_topic still NULL, and that state was a dead end in three directions at once: the
// ordinary claim predicate excludes 'failed', replay accepts only 'dead_lettered', and
// the worker holding the token had already moved on.
//
// dlt_topic IS NULL is the whole definition of "not preserved": the column is set by
// the same statement that moves the row to 'dead_lettered', so its absence on a failed
// row means the message never reached a topic.
//
// THE STATUS IS NOT CHANGED. The row stays 'failed' for two reasons.
//
// The ordering columns match the rest of the file (occurred_at, then id) and the
// predicate is served by idx_event_outbox_failed, whose partial WHERE covers exactly
// the failed and dead-lettered set. FOR UPDATE SKIP LOCKED is required for the same
// reason it is on the ordinary claim: several relay instances must be able to repair
// disjoint subsets.
const claimFailedEventOutboxForDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status = 'failed'
			  AND candidate.dlt_topic IS NULL
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimFailedEventOutboxForDeadLetter claims up to batchSize rows whose retry budget is
// spent and whose dead-letter write has not yet succeeded, taking a lease on them and
// stamping one fresh claim token, so the preservation can be attempted again.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: the maximum number of rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest first. Empty when nothing needs
//     repair, which is the normal state.
//   - error: a bad-request error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimFailedEventOutboxForDeadLetter(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimFailedEventOutboxForDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox dead-letter recovery batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)

		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter recovery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimFailedEventOutboxForDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to claim event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
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
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox entry awaiting dead-letter preservation",
				"claim_failed_event_outbox_for_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))

	return entries, nil
}

// brokerRecordBindings renders a broker coordinate as the three nullable SQL parameters
// the marking statements bind, so all three transitions agree on one definition.
//
// An unconfirmed record is a legitimate input, not an error: the broker acknowledged
// the write and the library reported no coordinate, which leaves the publication real
// but unnameable. The existing values are left in place by COALESCE at the call sites,
// so a later confirmed write can still fill them in.
//
// Parameters:
//   - record model.BrokerRecord: the coordinate, possibly unconfirmed.
//
// Returns:
//   - topic any: the topic name, or nil.
//   - partition any: the partition, or nil.
//   - offset any: the offset, or nil.
func brokerRecordBindings(record model.BrokerRecord) (topic, partition, offset any) {
	if !record.Confirmed() {
		return nil, nil, nil
	}

	return strings.TrimSpace(record.Topic), record.Partition, record.Offset
}

// requireEventOutboxClaimToken rejects a transition attempted without a claim token.
//
// Every conditional transition below matches on (id, claim_token), so an empty token
// could only ever match a row whose token is NULL — which is to say a row nobody holds,
// in pending or a terminal state. Letting the call through would therefore either match
// nothing (and be reported as a lost claim, which is misleading) or, worse, match a row
// that is not the caller's to move.
func requireEventOutboxClaimToken(claimToken, transition string) error {
	if strings.TrimSpace(claimToken) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox transition %q requires the claim token held for the row", transition), nil)
	}
	return nil
}

// normalizeDeadLetterHandoffLease resolves the lease a terminal transition holds the
// row under while its dead-letter write is owed.
//
// A NON-POSITIVE LEASE MUST NOT BE HONOURED, and the reason is the same one the claim
// paths already normalise for: `NOW() + '0s'::interval` is an instant that has already
// passed by the time the next poll runs, so the row satisfies
// claimFailedEventOutboxForDeadLetter's `locked_until < NOW()` immediately and is
// re-claimed with a fresh token while the first worker's dead-letter write is still in
// flight. That is the exact race the retained lease exists to prevent, reintroduced by
// a zero-valued argument — which is what a caller that forgot to plumb its lock
// duration through would pass.
//
// Falling back to defaultEventClaimLease matches what every claim in this file does
// with a non-positive lockDuration, so the two halves of the ownership window are
// governed by one value rather than by whichever caller happened to supply something.
//
// The fallback is LOGGED rather than silent: a relay that reaches this has a
// configuration or wiring defect, and it will otherwise behave correctly enough that
// nobody looks.
//
// Parameters:
//   - lease time.Duration: the caller's requested lease.
//   - transition string: the transition name, for the diagnostic.
//
// Returns:
//   - time.Duration: lease when positive, otherwise defaultEventClaimLease.
func normalizeDeadLetterHandoffLease(lease time.Duration, transition string) time.Duration {
	if lease > 0 {
		return lease
	}

	logrus.WithFields(logrus.Fields{
		"transition":      transition,
		"requested_lease": lease.String(),
		"applied_lease":   defaultEventClaimLease.String(),
	}).Warn(
		"Non-positive dead-letter hand-off lease; falling back to the default. A lease that has " +
			"already expired lets the dead-letter repair pass reclaim the row while this worker's " +
			"dead-letter write is still in flight, which is how one event reaches a .dlt topic twice",
	)

	return defaultEventClaimLease
}

// eventOutboxClaimLost is the typed error every conditional transition returns when its
// UPDATE matched no row, and it is a DELIBERATE behaviour change from the previous
// warn-and-continue treatment.
//
// A transition that matches nothing means one of three things, and all three are facts
// the caller must act on rather than facts to log and forget:
//
//   - The lease expired and another relay instance reclaimed the row.
//   - The row is no longer in the state this transition moves out of, so somebody else
//     has already moved it on.
//   - The id does not exist, which is a defect.
func eventOutboxClaimLost(id int64, transition string) error {
	logrus.WithFields(logrus.Fields{
		"event_outbox_id": id,
		"transition":      transition,
	}).Warn("Event outbox transition matched no row: the claim was lost or the row already moved on")

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event outbox row is no longer claimed for transition %q", transition), nil)
}

// requireEventOutboxRowAffected converts an UPDATE that matched no row into the typed
// lost-claim conflict above.
//
// A driver that cannot report the affected count is treated as success rather than
// failure: the UPDATE itself did not error, so the transition did happen, and failing
// the caller over missing bookkeeping would turn a completed write into a spurious
// retry. Every driver this repository uses does report it.
func requireEventOutboxRowAffected(result sql.Result, id int64, transition string) error {
	if result == nil {
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
			"error_class":     databaseErrorClass(err),
			"sqlstate":        postgresSQLState(err),
		}).Debug("Could not determine rows affected for event outbox transition")
		logDatabaseDiagnostic("require_event_outbox_row_affected", err)
		return nil
	}
	if affected == 0 {
		return eventOutboxClaimLost(id, transition)
	}

	return nil
}

// RenewEventOutboxLease extends the lease on every row still being worked under one
// claim token, and reports how many it extended.
//
// The lease was a fixed 30 seconds while a batch could take far longer. At the shipped
// defaults a claim takes 100 rows and publishes them 8 at a time, so the batch runs in
// 13 waves; a single wave can occupy the writer's whole 10-second produce timeout
// before it fails.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - claimToken string: the token the claim issued. Required; without it the statement
//     could only match rows nobody holds.
//   - lease time.Duration: how long from NOW the extended lease should run.
//
// Returns:
//   - int64: how many rows were extended. Zero is a legitimate answer.
//   - error: a bad-request error for a missing token, or a wrapped driver error.
func (d Datasource) RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RenewEventOutboxLease")
	defer span.End()

	if err := requireEventOutboxClaimToken(claimToken, "renew_lease"); err != nil {
		failDatabaseSpan(span, err)
		return 0, err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive event outbox lease renewal; falling back to %s", defaultEventClaimLease)
		lease = defaultEventClaimLease
	}

	span.SetAttributes(
		attribute.String("event_outbox.claim_token", claimToken),
		attribute.String("event_outbox.lease", lease.String()),
	)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET locked_until = NOW() + $1::interval
		WHERE claim_token = $2 AND status = $3
	`, lease.String(), claimToken, model.EventOutboxStatusProcessing)
	if err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the event outbox lease", "renew_event_outbox_lease", err)
	}

	renewed, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The renewal itself succeeded, so this is
		// reported as zero rather than as a failure: the caller uses the count only to
		// decide whether to keep renewing, and one uncounted round is harmless.
		logrus.WithFields(logrus.Fields{
			"claim_token": claimToken,
			"error_class": databaseErrorClass(err),
			"sqlstate":    postgresSQLState(err),
		}).Debug("Could not determine how many event outbox leases were renewed")
		logDatabaseDiagnostic("renew_event_outbox_lease", err)

		return 0, nil
	}

	span.SetAttributes(attribute.Int64("event_outbox.renewed_count", renewed))

	return renewed, nil
}

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged the
// publish, and does so ONLY IF the caller still holds the claim.
//
// The lease and the claim token are released and dispatched_at is stamped in the same
// statement. dispatched is a terminal state: the row is no longer claimable, because it
// is outside the claim predicate's status list.
//
// The gap between the broker's acknowledgement and this call is where at-least-once
// delivery comes from. A relay that dies in that gap has published the event but not
// recorded it, so the row is reclaimed once its lease expires and published again.
func (d Datasource) MarkEventDispatched(
	ctx context.Context,
	id int64,
	claimToken string,
	record model.BrokerRecord,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDispatched")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusDispatched); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			dispatched_at = NOW(),
			kafka_dispatched_at = COALESCE(kafka_dispatched_at, NOW()),
			kafka_topic = COALESCE($6, kafka_topic),
			kafka_partition = COALESCE($7, kafka_partition),
			kafka_offset = COALESCE($8, kafka_offset),
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $2 AND claim_token = $3 AND status IN ($4, $5)
	`, model.EventOutboxStatusDispatched, id, claimToken,
		model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying,
		recordTopic, recordPartition, recordOffset)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dispatched", "mark_event_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDispatched); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// MarkEventFailed records one failed publish attempt against a claimed row and settles
// it: back to pending with a next-attempt instant, or into the terminal failed state
// with a dead-letter hand-off lease when the retry budget is spent.
//
// A non-positive deadLetterLease is corrected to defaultEventClaimLease rather than
// honoured, and there is deliberately no second lease constant. The window a terminal
// transition holds a row under while its dead-letter write is owed is the same kind of
// window the CLAIM took, and two constants for one window is how the two halves of an
// ownership window come to be governed by different numbers.
func (d Datasource) MarkEventFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	terminal bool,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventFailed")
	defer span.End()
	deadLetterLease = normalizeDeadLetterHandoffLease(deadLetterLease, "mark_event_failed")
	if retryAfter < 0 {
		retryAfter = 0
	}
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
		attribute.Bool("event_outbox.terminal", terminal),
		attribute.String("event_outbox.dead_letter_lease", deadLetterLease.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, err
	}

	// $8 is the caller's terminal verdict, ORed into every arm of the decision so the
	// status, the due instant, the retained token AND the retained lease all agree about
	// which arm was taken. Repeating the condition rather than computing it once is what
	// keeps the whole decision inside the single statement.
	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN NOW() + $9::interval ELSE NULL END,
			next_attempt_at = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN next_attempt_at ELSE NOW() + $4::interval END,
			claim_token = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN claim_token ELSE NULL END
		WHERE id = $5 AND claim_token = $6 AND status = $7
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, retryAfter.String(), id, claimToken,
		model.EventOutboxStatusProcessing, terminal,
		eventDeadLetterHandoffLease.String()).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row matched, so the claim was lost or the row is no longer
			// processing. RETURNING makes this reachable as ErrNoRows rather than as
			// a zero affected count.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			failDatabaseSpan(span, lost)
			return model.EventFailureOutcome{}, lost
		}
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as failed", "mark_event_failed", err)
	}

	outcome.Exhausted = outcome.Status == model.EventOutboxStatusFailed
	// Echoed back from what the caller passed rather than inferred from the attempt count,
	// so a row that exhausted on its budget alone reports false even when its last attempt
	// happened to be permanent. The two causes need to stay distinguishable in the log:
	// "attempt 1 of 5, exhausted" reads as a bookkeeping defect unless the line also says
	// the failure was permanent.
	outcome.Terminal = terminal

	if outcome.Exhausted {
		// Retained by the UPDATE above, and returned so the caller need not
		// remember which arm keeps it.
		outcome.ClaimToken = claimToken
	}

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.attempts", outcome.Attempts),
		attribute.Bool("event_outbox.exhausted", outcome.Exhausted),
		attribute.Bool("event_outbox.terminal", outcome.Terminal),
	)
	return outcome, nil
}

// MarkEventPermanentlyFailed records a publish attempt that failed PERMANENTLY: the row
// becomes failed on this attempt, whatever budget it had left, and the dead-letter
// write is owed immediately.
//
// The two answer different questions and one of them is decided in a different place.
// MarkEventFailed asks the DATABASE whether the budget is spent, because two relay
// instances racing on one row must not both conclude they were the last attempt.
//
// Before this existed the relay had no way to act on that verdict. A
// topic-authorisation failure spent all five attempts and ~17 seconds of backoff
// proving the broker meant it, five times per event, at whatever rate events were being
// produced — and the log said "retryable=false" on each attempt and then "scheduled for
// another attempt" immediately after, which is a log contradicting itself about the
// decision it just took.
//
// attempts is INCREMENTED, so the failure metadata reports the number of attempts that
// were really made — 1 for a permanent failure on the first attempt, which is the
// honest figure and is what tells an operator triaging the dead-letter topic that this
// event never had a chance rather than that it fought for thirty seconds.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: the failure reason, stored in last_error.
//   - deadLetterLease time.Duration: how long this worker holds the row for the
//     dead-letter hand-off.
//
// Returns:
//   - model.EventFailureOutcome: Status failed, the new attempt count, Exhausted true —
//     because no further attempt will be made whatever the count says — and the token
//     for the dead-letter hand-off.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventPermanentlyFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventPermanentlyFailed")
	defer span.End()
	deadLetterLease = normalizeDeadLetterHandoffLease(deadLetterLease, "mark_event_permanently_failed")
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dead_letter_lease", deadLetterLease.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, err
	}

	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			attempts = attempts + 1,
			last_error = $2,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = NOW() + $6::interval
		WHERE id = $3 AND claim_token = $4 AND status = $5
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, errMsg, id, claimToken,
		model.EventOutboxStatusProcessing,
		eventDeadLetterHandoffLease.String()).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The claim was lost, or the row is no longer processing. Reachable as
			// ErrNoRows rather than as a zero affected count because of RETURNING.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			failDatabaseSpan(span, lost)
			return model.EventFailureOutcome{}, lost
		}
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to mark event outbox entry as permanently failed", "mark_event_permanently_failed", err)
	}

	// Always true on this path: the caller established that no further attempt is
	// possible, which is what Exhausted means to the caller — "the dead-letter write is
	// now owed" — rather than "the counter reached max_attempts".
	outcome.Exhausted = true
	outcome.ClaimToken = claimToken

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.attempts", outcome.Attempts),
		attribute.Bool("event_outbox.exhausted", outcome.Exhausted),
	)
	return outcome, nil
}

// MarkEventDeadLettered records that an entry has been written to its dead-letter
// topic, moving it to the dead_lettered terminal state and storing the dead-letter
// record — and does so ONLY IF the caller still holds the claim.
//
// An empty dltTopic or empty failureMetadata is stored as SQL NULL rather than as an
// empty string or an empty JSON document, so "not recorded" stays distinct from
// "recorded as empty".
func (d Datasource) MarkEventDeadLettered(
	ctx context.Context,
	id int64,
	claimToken, dltTopic string,
	failureMetadata json.RawMessage,
	record model.BrokerRecord,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDeadLettered")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dlt_topic", dltTopic),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusDeadLettered); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	var dltTopicArg interface{}
	if dltTopic != "" {
		dltTopicArg = dltTopic
	}

	var failureMetadataArg interface{}
	if len(failureMetadata) > 0 {
		failureMetadataArg = []byte(failureMetadata)
	}

	// The coordinate here names the record on the DEAD-LETTER topic, not on the original
	// one: the original publish is what failed, so there is no record of it to name. It is
	// assigned rather than COALESCEd, because a dead-letter write supersedes whatever a
	// failed original attempt may have left behind — a coordinate on the main topic would
	// send an operator looking for a record the retries never produced.
	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dlt_topic = $2, failure_metadata = $3,
			kafka_topic = $8, kafka_partition = $9, kafka_offset = $10,
			locked_until = NULL, claim_token = NULL
		WHERE id = $4 AND claim_token = $5 AND status IN ($6, $7)
	`, model.EventOutboxStatusDeadLettered, dltTopicArg, failureMetadataArg, id, claimToken,
		model.EventOutboxStatusFailed, model.EventOutboxStatusProcessing,
		recordTopic, recordPartition, recordOffset)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dead-lettered", "mark_event_dead_lettered", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDeadLettered); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// ClaimEventForReplay atomically claims a dead-lettered event for replay, moving it
// from dead_lettered to replaying and stamping a fresh claim token, and returns the
// claimed row.
//
// Without this, a replay was a read followed by a check followed by a publish: fetch
// the row, confirm it is dead-lettered, then publish. Two concurrent replays of one
// event both read a dead_lettered row, both pass the check, and both publish — so an
// operator clicking twice, or two operators triaging the same backlog, put two copies
// of the event on the topic.
//
// Making the transition the claim removes the window entirely: only the caller whose
// UPDATE actually changed a row proceeds to publish, and it is handed the token that
// authorises the follow-up transition. The row is not claimable by the relay while it
// is replaying, because replaying is outside the claim predicate's state set.
//
// The lease exists for the same reason it does on an ordinary claim: a process that
// dies mid-replay must not strand the row in replaying forever. ReleaseEventReplay
// returns the row to dead_lettered on every failure path, so a replay that merely could
// not publish stays replayable.
func (d Datasource) ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventForReplay")
	defer span.End()
	span.SetAttributes(attribute.String("event_outbox.event_id", eventID))

	if lockDuration <= 0 {
		lockDuration = defaultEventClaimLease
	}
	claimToken := uuid.NewString()

	// TWO CLAIMABLE STATES, and $1 appears in both the SET and the WHERE deliberately: it
	// is the replaying literal, so the second arm reads "a replay whose lease has run out".
	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, claim_token = $2, locked_until = NOW() + $3::interval
		WHERE event_id = $4
		  AND (status = $5
		       OR status = $1 AND (locked_until IS NULL OR locked_until < NOW()))
		RETURNING `+eventOutboxColumns, model.EventOutboxStatusReplaying, claimToken,
		lockDuration.String(), eventID, model.EventOutboxStatusDeadLettered)

	entry, err := scanEventOutbox(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, d.describeUnclaimableReplay(ctx, eventID)
		}
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim event for replay", "claim_event_for_replay", err)
	}

	span.SetAttributes(attribute.String("event_outbox.claim_token", claimToken))
	return &entry, nil
}

// describeUnclaimableReplay explains why ClaimEventForReplay matched nothing, so the
// caller can answer "no such event" and "that event is not dead-lettered" differently
// instead of conflating them into one unhelpful failure.
//
// A read error here is deliberately not propagated. The replay claim has already failed
// and this call exists only to explain why; returning a database error instead of the
// explanation would replace a precise answer with a vague one.
func (d Datasource) describeUnclaimableReplay(ctx context.Context, eventID string) error {
	existing, lookupErr := d.GetEventByID(ctx, eventID)
	if lookupErr != nil || existing == nil {
		return apierror.NewAPIError(apierror.ErrNotFound, "Event not found", nil)
	}

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event is not available for replay: its status is %q", existing.Status), nil)
}

// ReleaseEventReplay returns a replaying row to dead_lettered, releasing the claim.
//
// It is the rollback half of ClaimEventForReplay and the reason a failed replay does
// not cost an event its replayability. Without it a replay that claimed a row and then
// failed to publish would leave the row stuck in replaying — outside the relay's
// claimable set and outside the dead-letter inventory's own terminal state — where
// nothing would ever pick it up again.
func (d Datasource) ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ReleaseEventReplay")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusReplaying); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			claim_token = NULL,
			locked_until = NULL,
			last_error = COALESCE(NULLIF($2, ''), last_error)
		WHERE id = $3 AND claim_token = $4 AND status = $5
	`, model.EventOutboxStatusDeadLettered, replayErr, id, claimToken, model.EventOutboxStatusReplaying)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to release event replay claim", "release_event_replay", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusReplaying); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// claimPendingWebhookDeliveriesQuery claims rows whose Kafka leg has FINISHED and whose
// LEGACY WEBHOOK leg is still owed, taking a lease and stamping a fresh claim token
// WITHOUT changing the row's status.
//
// It is a package-level constant for the same reason the main claim query is: tests
// assert that FOR UPDATE SKIP LOCKED, the occurred_at ordering and the untouched status
// are all still present.
const claimPendingWebhookDeliveriesQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('dispatched', 'failed', 'dead_lettered')
			  AND candidate.webhook_dispatched = FALSE
			  AND candidate.webhook_attempts < candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingWebhookDeliveries claims rows whose Kafka leg has finished and whose
// legacy HTTP webhook leg has not been recorded, so the dual-delivery window can finish
// a leg that failed alongside the Kafka publish — whether that publish then succeeded
// or was itself given up on.
//
// The lease and token this stamps are left in place once MarkWebhookDispatched
// succeeds, and that is deliberate: the flag alone removes the row from this query's
// candidate set permanently, and clearing the token here would break
// MarkWebhookDispatched's documented idempotency for the main dual-delivery path, where
// the token must survive until MarkEventDispatched consumes it.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first, each carrying the
//     claim token MarkWebhookDispatched requires.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingWebhookDeliveries")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Webhook delivery claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive webhook delivery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingWebhookDeliveriesQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending webhook deliveries", "claim_pending_webhook_deliveries", err)
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
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_webhook_deliveries", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_webhook_deliveries", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// MarkWebhookDispatched records that the legacy HTTP webhook leg was dispatched for
// this entry, and does so ONLY IF the caller still holds the claim.
//
// It serves the dual-delivery window only. The relay publishes to Kafka and enqueues
// the legacy webhook task from the SAME claimed row — which is what makes the two
// transports carry byte-identical payloads structurally rather than by careful coding —
// and this flag makes the legacy leg individually idempotent: a row republished to
// Kafka after a crash does not enqueue a second webhook.
//
// The claim token requirement is what makes that idempotency hold under concurrency.
// Without it a worker whose lease had expired could set the flag for a row the current
// holder was about to enqueue for, so the current holder would skip the enqueue and the
// webhook would be recorded as dispatched having never been sent.
func (d Datasource) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkWebhookDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, "webhook_dispatched"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_dispatched = TRUE
		WHERE id = $1 AND claim_token = $2
	`, id, claimToken)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark webhook dispatched for event outbox entry", "mark_webhook_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, "webhook_dispatched"); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// MarkEventWebhookPending records that this row's KAFKA leg is complete while its
// LEGACY WEBHOOK leg is still owed, and decides in the same statement whether another
// webhook attempt is made or the legacy leg is abandoned.
//
// SUNSET: this method goes with the rest of the legacy transport.
//
// Both arms stamp kafka_dispatched_at through COALESCE, so the first acknowledgement
// instant is preserved across repeated webhook retries.
//
// The accepted prior states are processing and webhook_pending. processing is the
// ordinary path — the claim moves every row it takes to processing, including rows it
// took out of webhook_pending — and webhook_pending is accepted so that an honest
// re-call from the claim holder is idempotent rather than reported as a lost claim.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: why the enqueue failed, stored in last_error.
//   - retryAfter time.Duration: how long before the row is due again.
//
// Returns:
//   - model.EventWebhookOutcome: the resulting status, the new webhook attempt count,
//     and whether the legacy leg has been abandoned.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventWebhookPending(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	record model.BrokerRecord,
) (model.EventWebhookOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventWebhookPending")
	defer span.End()

	if retryAfter < 0 {
		retryAfter = 0
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusWebhookPending); err != nil {
		failDatabaseSpan(span, err)
		return model.EventWebhookOutcome{}, err
	}

	// COALESCEd for the reason MarkEventDispatched documents: this row's Kafka leg may
	// already have completed on an earlier claim, and a webhook-only retry carries no
	// coordinate. A straight assignment would erase the record the first successful
	// publish named.
	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	var outcome model.EventWebhookOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_attempts = webhook_attempts + 1,
			kafka_dispatched_at = COALESCE(kafka_dispatched_at, NOW()),
			kafka_topic = COALESCE($9, kafka_topic),
			kafka_partition = COALESCE($10, kafka_partition),
			kafka_offset = COALESCE($11, kafka_offset),
			last_error = $1,
			status = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN $2
				ELSE $3
			END,
			dispatched_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN NOW()
				ELSE dispatched_at
			END,
			next_attempt_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN next_attempt_at
				ELSE NOW() + $4::interval
			END,
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $5 AND claim_token = $6 AND status IN ($7, $8)
		RETURNING status, webhook_attempts
	`,
		errMsg,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusWebhookPending,
		retryAfter.String(),
		id,
		claimToken,
		model.EventOutboxStatusProcessing,
		model.EventOutboxStatusWebhookPending,
		recordTopic,
		recordPartition,
		recordOffset,
	).Scan(&outcome.Status, &outcome.WebhookAttempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusWebhookPending)
			failDatabaseSpan(span, lost)

			return model.EventWebhookOutcome{}, lost
		}

		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the pending legacy webhook leg for event outbox entry", "mark_event_webhook_pending", err)
	}

	// Derived from the status the database chose, never recomputed, so the two cannot
	// disagree about which arm was taken.
	outcome.Abandoned = outcome.Status == model.EventOutboxStatusDispatched

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.webhook_attempts", outcome.WebhookAttempts),
		attribute.Bool("event_outbox.legacy_leg_abandoned", outcome.Abandoned),
	)

	return outcome, nil
}

// MarkEventLegacyWebhookAttempted records one failed legacy webhook enqueue against a
// row whose KAFKA leg has already finished, and decides in the same statement whether
// another webhook attempt is made or the legacy leg is abandoned.
//
// SUNSET: this method goes with the rest of the legacy transport.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token ClaimPendingWebhookDeliveries issued; refused
//     without it.
//   - retryAfter time.Duration: how long before the legacy leg is due again.
//
// Returns:
//   - model.EventWebhookOutcome: the row's UNCHANGED status, the new webhook attempt
//     count, and whether the legacy leg has been abandoned.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventLegacyWebhookAttempted(
	ctx context.Context,
	id int64,
	claimToken string,
	retryAfter time.Duration,
) (model.EventWebhookOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventLegacyWebhookAttempted")
	defer span.End()

	if retryAfter < 0 {
		retryAfter = 0
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, "legacy_webhook_attempt"); err != nil {
		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, err
	}

	var outcome model.EventWebhookOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_attempts = webhook_attempts + 1,
			next_attempt_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN next_attempt_at
				ELSE NOW() + $1::interval
			END,
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $2 AND claim_token = $3 AND status IN ($4, $5, $6)
		RETURNING status, webhook_attempts, webhook_attempts >= max_attempts
	`,
		retryAfter.String(),
		id,
		claimToken,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusFailed,
		model.EventOutboxStatusDeadLettered,
	).Scan(&outcome.Status, &outcome.WebhookAttempts, &outcome.Abandoned)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			lost := eventOutboxClaimLost(id, "legacy_webhook_attempt")
			failDatabaseSpan(span, lost)

			return model.EventWebhookOutcome{}, lost
		}

		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the legacy webhook attempt for event outbox entry",
			"mark_event_legacy_webhook_attempted", err)
	}

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.webhook_attempts", outcome.WebhookAttempts),
		attribute.Bool("event_outbox.legacy_leg_abandoned", outcome.Abandoned),
	)

	return outcome, nil
}

// PurgeTerminalEventsBefore deletes terminal event rows whose occurrence is older than
// cutoff, and returns how many it removed. It is the retention primitive behind the
// outbox's data-minimisation contract.
//
// WHAT THIS TABLE STORES IS SENSITIVE. payload is the webhook body verbatim, so a
// transaction event carries amounts and balance identifiers and an identity event
// carries names, email addresses, phone numbers, postal addresses and dates of birth.
// last_error and failure_metadata carry broker and driver text. None of it has any
// operational value once the event has been delivered, so keeping it indefinitely turns
// a delivery buffer into an unbounded secondary copy of the ledger's most sensitive
// data — with none of the access controls the primary tables have around them, and with
// an ever-growing blast radius if the database is ever exposed.
func (d Datasource) PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PurgeTerminalEventsBefore")
	defer span.End()

	if cutoff.IsZero() {
		// A zero cutoff would read as "delete everything older than the year 1", which
		// deletes nothing — but it is far more likely to be an unset field than an intention,
		// and silently doing nothing would hide a broken retention job that looks like it is
		// running.
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox retention cutoff is required", nil)
		failDatabaseSpan(span, err)
		return 0, err
	}
	if limit <= 0 {
		limit = defaultEventPurgeBatchSize
	}
	if limit > maxEventPurgeBatchSize {
		limit = maxEventPurgeBatchSize
	}

	span.SetAttributes(
		attribute.String("event_outbox.retention_cutoff", cutoff.UTC().Format(time.RFC3339)),
		attribute.Int("event_outbox.retention_limit", limit),
	)

	// This is the one IN-subquery LIMIT in this file that is NOT wrapped in a MATERIALIZED
	// CTE, and the omission is deliberate rather than an oversight.
	row := d.Conn.QueryRowContext(ctx, `
		WITH removed AS (
			DELETE FROM blnk.event_outbox
			WHERE id IN (
				SELECT id FROM blnk.event_outbox
				WHERE status = $1
				  AND occurred_at < $2
				ORDER BY occurred_at ASC
				LIMIT $3
			)
			RETURNING occurred_at, kafka_offset
		), logged AS (
			INSERT INTO blnk.event_outbox_purge_log
				(cutoff, rows_removed, confirmed_removed, oldest_occurred_at, newest_occurred_at)
			SELECT $2, COUNT(*), COUNT(kafka_offset), MIN(occurred_at), MAX(occurred_at)
			FROM removed
			HAVING COUNT(*) > 0
			RETURNING rows_removed
		)
		SELECT COUNT(*) FROM removed
	`, model.EventOutboxStatusDispatched, cutoff, limit)

	var purged int64
	if err := row.Scan(&purged); err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer, "Failed to purge terminal event outbox entries", "purge_terminal_events_before", err)
	}

	span.SetAttributes(attribute.Int64("event_outbox.purged_count", purged))
	return purged, nil
}

// SumPurgedTerminalEvents reports what retention has removed from blnk.event_outbox
// over the table's whole life.
//
// A broker's end offset counts every record ever appended and never decreases. The
// outbox side of the reconciliation counts rows that still exist.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - model.EventOutboxPurgeTotals: the totals.
//   - error: the repository's typed error.
func (d Datasource) SumPurgedTerminalEvents(ctx context.Context) (model.EventOutboxPurgeTotals, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "SumPurgedTerminalEvents")
	defer span.End()

	totals := model.EventOutboxPurgeTotals{Recorded: true}

	var lastPurgedAt sql.NullTime
	var newestPurgedOccurrence sql.NullTime

	err := d.Conn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(rows_removed), 0)      AS rows_removed,
			COALESCE(SUM(confirmed_removed), 0) AS confirmed_removed,
			COUNT(*)                            AS batches,
			MAX(purged_at)                      AS last_purged_at,
			MAX(newest_occurred_at)             AS newest_purged_occurrence
		FROM blnk.event_outbox_purge_log
	`).Scan(
		&totals.RowsRemoved,
		&totals.ConfirmedRemoved,
		&totals.Batches,
		&lastPurgedAt,
		&newestPurgedOccurrence,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventOutboxPurgeTotals{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to total the event outbox purge log", "sum_purged_terminal_events", err)
	}

	if lastPurgedAt.Valid {
		instant := lastPurgedAt.Time.UTC()
		totals.LastPurgedAt = &instant
	}
	if newestPurgedOccurrence.Valid {
		instant := newestPurgedOccurrence.Time.UTC()
		totals.NewestPurgedOccurrence = &instant
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.purged_rows_total", totals.RowsRemoved),
		attribute.Int64("event_outbox.purged_confirmed_total", totals.ConfirmedRemoved),
		attribute.Int64("event_outbox.purge_batches", totals.Batches),
	)

	return totals, nil
}

// AuditEventRecordCoordinates reports, per (topic, partition), how many terminal rows
// claim a broker record there and what the extreme claimed offsets are.
//
// A coordinate can be checked rather than inferred. Compared against the live
// per-partition bounds the broker reports, the claims divide into three cases, and only
// the first is benign:
//
//   - within [first, end) — the record exists and the claim is verified.
//   - at or beyond end — the row claims a record the log does not contain.
//   - below first — the record has aged out under retention, so the claim can no longer
//     be verified either way and the verdict must say so.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - model.EventRecordCoordinateAudit: one entry per claimed partition, ordered by
//     topic then partition.
//   - error: the repository's typed error.
func (d Datasource) AuditEventRecordCoordinates(ctx context.Context) (model.EventRecordCoordinateAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditEventRecordCoordinates")
	defer span.End()

	audit := model.EventRecordCoordinateAudit{
		Coordinates: []model.EventRecordCoordinate{},
		MeasuredAt:  time.Now().UTC(),
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT kafka_topic, kafka_partition, COUNT(*), MIN(kafka_offset), MAX(kafka_offset)
		FROM blnk.event_outbox
		WHERE kafka_offset IS NOT NULL
		  AND kafka_topic IS NOT NULL
		  AND kafka_partition IS NOT NULL
		GROUP BY kafka_topic, kafka_partition
		ORDER BY kafka_topic ASC, kafka_partition ASC
	`)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker coordinates of published event outbox entries",
			"audit_event_record_coordinates", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	for rows.Next() {
		var coordinate model.EventRecordCoordinate
		if scanErr := rows.Scan(
			&coordinate.Topic,
			&coordinate.Partition,
			&coordinate.Rows,
			&coordinate.MinOffset,
			&coordinate.MaxOffset,
		); scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox broker coordinate",
				"audit_event_record_coordinates", scanErr)
		}
		audit.Coordinates = append(audit.Coordinates, coordinate)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox broker coordinates",
			"audit_event_record_coordinates", err)
	}

	span.SetAttributes(
		attribute.Int("event_outbox.claimed_partitions", len(audit.Coordinates)),
		attribute.Int64("event_outbox.claimed_rows", audit.TotalRows()),
	)

	return audit, nil
}

// GetEventByID retrieves an entry by its business event_id UUID.
//
// The lookup is by event_id and not by the BIGSERIAL id because that is what the
// dead-letter replay route carries and what a subscriber quotes when it asks for an
// event to be resent; the surrogate key is never exposed outside this layer. The lookup
// is index-backed by event_outbox_event_id_uidx, and the unique index also guarantees
// at most one row can match.
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
			return nil, loggedDatabaseError(apierror.ErrNotFound, "Event not found", "get_event_by_id", err)
		}
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to retrieve event outbox entry", "get_event_by_id", err)
	}

	span.SetAttributes(attribute.String("event_outbox.status", entry.Status))
	return &entry, nil
}

// ListDeadLetteredEvents pages the dead-letter inventory, applying EVERY narrowing the
// query expresses IN SQL.
//
// It is the single entry point every caller uses — the operator listing, the
// dead-letter age scan and the recovery audit — and it delegates to
// ListDeadLetterInventory so a filtered page and an unfiltered one cannot be drawn from
// different statements. The zero-valued query is the whole inventory at the
// repository's default page size.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterQuery: the narrowing and the page. The zero value is valid.
//
// Returns:
//   - []model.EventOutbox: the matching page, newest occurrence first.
//   - error: the repository's typed error.
func (d Datasource) ListDeadLetteredEvents(
	ctx context.Context,
	query model.DeadLetterQuery,
) ([]model.EventOutbox, error) {
	return d.ListDeadLetteredEventsFiltered(ctx, query, query.Limit, query.Offset)
}

// ListDeadLetterInventory returns one page of the dead-letter inventory an operator
// triages from, narrowed and paged entirely in SQL.
//
// Both terminal failure states are included by default. A row becomes failed the moment
// its retry budget is spent and dead_lettered only once the event has additionally
// reached its `<topic>.dlt` sibling, so listing only the latter would hide the events
// whose dead-letter write itself failed — the ones most in need of attention.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterInventoryQuery: the page and its narrowing.
//
// Returns:
//   - model.DeadLetterInventoryPage: the entries, plus the cursor for the next page
//     when one exists.
//   - error: a logged internal error.
func (d Datasource) ListDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListDeadLetterInventory")
	defer span.End()

	return listDeadLetterInventory(ctx, d.Conn, span, query)
}

// listDeadLetterInventory is the page read, parameterised by the connection it runs on.
//
// It exists so that the standalone read and the SNAPSHOT-CONSISTENT read that pairs the
// page with its total run the identical statement over the identical bindings.
// Duplicating the statement for the two paths is how the cursor predicate, the probe
// row and the ordering drift apart, and that drift would show up as a page that
// differed depending on whether a total had been asked for.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - conn sqlQueryer: *sql.DB for a standalone read, *sql.Tx for the paired read.
//   - span trace.Span: the caller's span, annotated here.
//   - query model.DeadLetterInventoryQuery: the narrowing and the page.
//
// Returns:
//   - model.DeadLetterInventoryPage: as ListDeadLetterInventory.
//   - error: as ListDeadLetterInventory.
func listDeadLetterInventory(
	ctx context.Context,
	conn sqlQueryer,
	span trace.Span,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = defaultDeadLetterPageSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}

	// The cursor's two halves are bound as a nullable pair, because a nil cursor and a
	// cursor at the zero instant must be the same request — "start at the newest entry" —
	// and binding a zero timestamp would instead exclude everything older than year one,
	// which is every row.
	var cursorInstant interface{}
	var cursorID int64
	if query.Cursor != nil {
		cursorInstant = query.Cursor.OccurredAt.UTC()
		cursorID = query.Cursor.ID
	}

	span.SetAttributes(
		attribute.Int("event_outbox.limit", limit),
		attribute.String("event_outbox.status_filter", query.Status),
		attribute.Bool("event_outbox.cursor_present", query.Cursor != nil),
	)

	// limit+1: the extra row is read to establish that another page exists and is then
	// discarded, which answers "is there more" without a COUNT over the inventory.
	// The window's two ends are bound the same nullable way as the cursor, and for the same
	// reason: a zero instant means "unbounded", not "year one", and binding it literally would
	// exclude every row at one end and none at the other.
	var occurredFrom, occurredTo interface{}
	if !query.OccurredFrom.IsZero() {
		occurredFrom = query.OccurredFrom.UTC()
	}

	if !query.OccurredTo.IsZero() {
		occurredTo = query.OccurredTo.UTC()
	}

	rows, err := conn.QueryContext(ctx, listDeadLetterInventoryQuery,
		model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
		query.Status, query.EventType, query.Topic,
		cursorInstant, cursorID, occurredFrom, occurredTo, limit+1)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list the dead-letter inventory", "list_dead_letter_inventory", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	page := model.DeadLetterInventoryPage{
		Entries: make([]model.DeadLetterInventoryEntry, 0, limit),
	}

	for rows.Next() {
		entry, scanErr := scanDeadLetterInventoryEntry(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan a dead-letter inventory entry", "list_dead_letter_inventory", scanErr)
		}

		if len(page.Entries) == limit {
			// The probe row. Its existence is the answer, and the cursor is taken from the
			// LAST RETURNED entry rather than from this one, so the next page resumes
			// exactly where this one stopped.
			page.HasMore = true

			break
		}

		page.Entries = append(page.Entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over the dead-letter inventory", "list_dead_letter_inventory", err)
	}

	if page.HasMore && len(page.Entries) > 0 {
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}

	span.SetAttributes(
		attribute.Int("event_outbox.dead_lettered_count", len(page.Entries)),
		attribute.Bool("event_outbox.has_more", page.HasMore),
	)

	return page, nil
}

// deadLetterFilterClause renders a dead-letter filter as a SQL predicate and its
// arguments.
//
// EVERY VALUE IS A BOUND PARAMETER. Nothing from the caller is interpolated into the
// SQL text, so an event type or topic containing a quote is a value that matches
// nothing rather than a fragment of the statement — the property that makes an
// operator-facing filter safe to expose.
//
// Parameters:
//   - filter model.DeadLetterFilter: the requested narrowing. The zero value selects
//     the whole inventory.
//   - next int: the 1-based index of the first placeholder to allocate, so the caller
//     can append LIMIT/OFFSET parameters after the predicate's own.
//
// Returns:
//   - string: the WHERE clause body, never empty — it always at least bounds the status
//     set.
//   - []interface{}: the arguments, in placeholder order.
//   - int: the next unused placeholder index.
func deadLetterFilterClause(filter model.DeadLetterFilter, next int) (string, []interface{}, int) {
	clauses := make([]string, 0, 3)
	args := make([]interface{}, 0, 4)

	if status := strings.TrimSpace(filter.Status); status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", next))
		args = append(args, status)
		next++
	} else {
		clauses = append(clauses, fmt.Sprintf("status IN ($%d, $%d)", next, next+1))
		args = append(args,
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed)
		next += 2
	}

	if eventType := strings.TrimSpace(filter.EventType); eventType != "" {
		clauses = append(clauses, fmt.Sprintf("event_type = $%d", next))
		args = append(args, eventType)
		next++
	}

	if topic := strings.TrimSpace(filter.Topic); topic != "" {
		clauses = append(clauses, fmt.Sprintf("topic = $%d", next))
		args = append(args, topic)
		next++
	}

	// THE OCCURRENCE WINDOW, bound inclusively at both ends and omitted when either end is
	// zero: zero means "unbounded", not "year one", and binding it literally would exclude
	// every row at one end and none at the other. occurred_at is the right column for a
	// triage window rather than the row's creation or last-attempt instant — it is when
	// the ledger mutation happened, which is what an operator correlating a backlog
	// against an incident timeline holds, and it is the column the listing is ordered by,
	// so the window and the paging agree about what "newest first" selects.
	if !filter.OccurredFrom.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at >= $%d", next))
		args = append(args, filter.OccurredFrom.UTC())
		next++
	}

	if !filter.OccurredTo.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at <= $%d", next))
		args = append(args, filter.OccurredTo.UTC())
		next++
	}

	return strings.Join(clauses, " AND "), args, next
}

// ListDeadLetteredEventsFiltered pages the dead-letter inventory with the caller's
// filters applied IN SQL.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - filter model.DeadLetterFilter: the narrowing.
//   - limit int: page size. Non-positive selects the default; oversized is capped.
//   - offset int: how many matching rows to skip. Negative is clamped to zero.
//
// Returns:
//   - []model.EventOutbox: the matching page, nil when nothing matches.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) ListDeadLetteredEventsFiltered(
	ctx context.Context,
	filter model.DeadLetterFilter,
	limit, offset int,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListDeadLetteredEventsFiltered")
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

	predicate, args, next := deadLetterFilterClause(filter, 1)
	args = append(args, limit, offset)

	span.SetAttributes(
		attribute.Int("event_outbox.limit", limit),
		attribute.Int("event_outbox.offset", offset),
		attribute.Bool("event_outbox.filtered", filter.Narrows()),
	)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE `+predicate+`
		ORDER BY occurred_at DESC, id DESC
		LIMIT $`+strconv.Itoa(next)+` OFFSET $`+strconv.Itoa(next+1), args...)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list dead-lettered events", "list_dead_lettered_events_filtered", err)
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
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan dead-lettered event", "list_dead_lettered_events_filtered", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over dead-lettered events", "list_dead_lettered_events_filtered", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.dead_lettered_count", len(entries)))

	return entries, nil
}

// CountDeadLetteredEvents counts the dead-letter inventory THE SAME FILTER selects.
//
// It is the other half of a usable paginated triage list. Without it a filtered page
// could report no total at all — the API refused `include_count` alongside a filter for
// exactly that reason — and a client had no way to know how many matches it was paging
// through, or whether the short page it just received was the end of the result or the
// end of a scan budget.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - filter model.DeadLetterFilter: the narrowing. The zero value counts the whole
//     inventory.
//
// Returns:
//   - int64: how many rows match. Zero is a legitimate answer and not an error.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) CountDeadLetteredEvents(
	ctx context.Context,
	filter model.DeadLetterFilter,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountDeadLetteredEvents")
	defer span.End()

	predicate, args, _ := deadLetterFilterClause(filter, 1)

	var total int64
	if err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_outbox
		WHERE `+predicate, args...).Scan(&total); err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count dead-lettered events", "count_dead_lettered_events", err)
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.dead_lettered_total", total),
		attribute.Bool("event_outbox.filtered", filter.Narrows()),
	)

	return total, nil
}

// CountUnresolvedEventOutbox returns a status-keyed count of every row that has NOT
// reached its terminal dispatched state, and touches no dispatched row at all.
//
// Dropping the second arm is all it takes, because the arm that MATTERS to those
// callers was never windowed. The population counted here is bounded by OPERATION
// rather than by history — the pending backlog, the rows in flight, the dead-letter
// inventory, the two repair legs.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - map[string]int64: counts by status for every non-dispatched status, never nil on
//     success.
//   - error: a logged internal error.
func (d Datasource) CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountUnresolvedEventOutbox")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, countUnresolvedEventOutboxQuery,
		model.EventOutboxStatusDispatched)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count unresolved event outbox entries by status",
			"count_unresolved_event_outbox", err)
	}

	return collectEventOutboxStatusCounts(span, rows, "count_unresolved_event_outbox")
}

// CountEventOutboxByStatus returns a status-keyed count of blnk.event_outbox rows,
// INCLUDING the dispatched history inside the caller's window.
//
// IT IS NOT UNUSED — do not delete it. A grep for callers inside this package alone
// finds none, which is exactly the trap this comment exists to prevent.
//
// The map is never nil on success: an empty table yields an empty map, so a caller can
// range over the result without a nil check.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - since time.Time: the earliest occurrence instant a dispatched row must have to be
//     counted.
//
// Returns:
//   - map[string]int64: counts by status, never nil on success.
//   - error: a logged internal error.
func (d Datasource) CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountEventOutboxByStatus")
	defer span.End()

	since = normalizeEventCountWindow(since)
	span.SetAttributes(attribute.String("event_outbox.window_start", since.Format(time.RFC3339)))

	rows, err := d.Conn.QueryContext(ctx, countEventOutboxByStatusQuery,
		model.EventOutboxStatusDispatched, since)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to count event outbox entries by status", "count_event_outbox_by_status", err)
	}

	return collectEventOutboxStatusCounts(span, rows, "count_event_outbox_by_status")
}

// collectEventOutboxStatusCounts drains a `(status, row_count)` result set into a map.
//
// Shared by the two status aggregates so that the one thing a reader must be able to
// trust about both — that a scan or iteration failure is reported rather than silently
// yielding a SHORT count — is written once. Two copies of this loop is how one of them
// ends up returning a partially-drained map on a mid-iteration error, and a zero-loss
// reconciliation compared against a short total reports loss that has not happened.
//
// Parameters:
//   - span trace.Span: the caller's span, marked failed and annotated with the status
//     count.
//   - rows *sql.Rows: the open result set. Closed before this returns.
//   - operation string: the operation label for the logged error.
//
// Returns:
//   - map[string]int64: counts by status, never nil on success and never partial.
//   - error: a logged internal error.
func collectEventOutboxStatusCounts(
	span trace.Span,
	rows *sql.Rows,
	operation string,
) (map[string]int64, error) {
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
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan event outbox status count", operation, scanErr)
		}
		counts[status] = count
	}

	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox status counts", operation, err)
	}

	span.SetAttributes(attribute.Int("event_outbox.status_count", len(counts)))

	return counts, nil
}

// AuditEventRecordsInIntervals classifies every row that claims a Kafka record against
// the MEASURED offset windows of the partitions those records live in.
//
// Those two numbers describe different populations, and no amount of care in the
// arithmetic fixes that:
//
//   - Records are a LOWER BOUND on events — a redelivery, a replay or a dead-letter
//     copy writes its own record — so the comparison is directional and a surplus is
//     expected.
//   - Outbox retention prunes rows, so the left side shrinks over time while the right
//     side only climbs, and the gap that hides loss grows on its own.
//   - Kafka retention deletes records the end offset still counts, so "the sum says it
//     was written" is not "a consumer could still read it".
//   - Recreating a topic resets its offsets, so the right side collapses to a number
//     smaller than the left and a healthy system reports catastrophic loss.
//   - Foreign or pre-existing traffic on a shared topic inflates the right side by an
//     unknown amount, masking loss by exactly that much.
//
// Each row lands in exactly one bucket and the buckets sum to PublishedRows:
//
//   - corroborated — offset inside [first_offset, end_offset) of a measured partition.
//   - unconfirmed — no coordinate at all. COUNT of a NULL-able expression ignores
//     NULLs, and the all-or-nothing check constraint means a non-NULL offset implies a
//     complete coordinate, so testing the offset alone is sufficient.
//   - unmeasured — names a topic or partition absent from the measurement: a missing
//     topic, an unavailable partition, or a partition count that has since shrunk.
//   - aged_out — offset BELOW first_offset. Written, then deleted by Kafka retention.
//   - beyond_end — offset AT OR ABOVE end_offset. Impossible on an intact log, because
//     the broker assigned that offset when it accepted the write; it means the
//     partition was truncated or the topic recreated.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - intervals []model.PartitionOffsetInterval: the measured windows, from
//     TopicOffsetReport.PartitionIntervals().
//
// Returns:
//   - model.EventRecordIntervalAudit: the classification and the instants it covers.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) AuditEventRecordsInIntervals(
	ctx context.Context,
	intervals []model.PartitionOffsetInterval,
) (model.EventRecordIntervalAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditEventRecordsInIntervals")
	defer span.End()

	audit := model.EventRecordIntervalAudit{MeasuredAt: time.Now().UTC()}

	// The windows are passed as four parallel arrays and zipped by unnest, so one
	// statement serves any number of partitions with a fixed parameter count. Built with
	// make rather than left nil, because lib/pq renders a nil slice as SQL NULL and
	// unnest(NULL) yields no rows at all — which would classify correctly by accident
	// here, but would break the moment the join was used in the other direction.
	topics := make([]string, 0, len(intervals))
	partitions := make([]int64, 0, len(intervals))
	firstOffsets := make([]int64, 0, len(intervals))
	endOffsets := make([]int64, 0, len(intervals))
	for _, interval := range intervals {
		topic := strings.TrimSpace(interval.Topic)
		if topic == "" || interval.Partition < 0 {
			// A window with no topic or a negative partition cannot match any stored
			// coordinate, and passing it would only make the arrays longer. Dropping it here
			// keeps "unmeasured" meaning what it says.
			continue
		}

		first := interval.FirstOffset
		if first < 0 {
			// A broker that could not report the earliest retained offset is reported as zero
			// rather than as a negative sentinel, which is the widest window the measurement
			// supports and therefore the reading that cannot manufacture an aged-out row that is
			// not one.
			first = 0
		}

		topics = append(topics, topic)
		partitions = append(partitions, int64(interval.Partition))
		firstOffsets = append(firstOffsets, first)
		endOffsets = append(endOffsets, interval.EndOffset)
	}

	span.SetAttributes(attribute.Int("event_outbox.measured_partitions", len(topics)))

	var oldestTerminalAt, corroboratedFrom, corroboratedTo sql.NullTime

	err := d.Conn.QueryRowContext(ctx, `
		WITH measured AS (
			SELECT *
			FROM unnest($2::text[], $3::bigint[], $4::bigint[], $5::bigint[])
				AS m(topic_name, partition_id, first_offset, end_offset)
		),
		terminal AS (
			SELECT
				kafka_topic,
				kafka_partition,
				kafka_offset,
				COALESCE(kafka_dispatched_at, occurred_at) AS published_at
			FROM blnk.event_outbox
			WHERE kafka_dispatched_at IS NOT NULL OR status = $1
		),
		classified AS (
			SELECT
				t.kafka_topic,
				t.kafka_partition,
				t.kafka_offset,
				t.published_at,
				CASE
					WHEN t.kafka_offset IS NULL              THEN 'unconfirmed'
					WHEN m.topic_name IS NULL                THEN 'unmeasured'
					WHEN t.kafka_offset >= m.end_offset      THEN 'beyond_end'
					WHEN t.kafka_offset <  m.first_offset    THEN 'aged_out'
					ELSE 'corroborated'
				END AS classification
			FROM terminal t
			LEFT JOIN measured m
				ON m.topic_name = t.kafka_topic
				AND m.partition_id = t.kafka_partition
		)
		SELECT
			COUNT(*)                                                       AS published_rows,
			COUNT(*) FILTER (WHERE classification = 'corroborated')        AS corroborated_rows,
			COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
				FILTER (WHERE classification = 'corroborated'
					AND kafka_offset IS NOT NULL)                          AS distinct_corroborated,
			COUNT(*) FILTER (WHERE classification = 'unconfirmed')         AS unconfirmed_rows,
			COUNT(*) FILTER (WHERE classification = 'unmeasured')          AS unmeasured_rows,
			COUNT(*) FILTER (WHERE classification = 'aged_out')            AS aged_out_rows,
			COUNT(*) FILTER (WHERE classification = 'beyond_end')          AS beyond_end_rows,
			MIN(published_at)                                              AS oldest_terminal_at,
			MIN(published_at) FILTER (WHERE classification = 'corroborated') AS corroborated_from,
			MAX(published_at) FILTER (WHERE classification = 'corroborated') AS corroborated_to
		FROM classified
	`,
		model.EventOutboxStatusDeadLettered,
		pq.Array(topics), pq.Array(partitions), pq.Array(firstOffsets), pq.Array(endOffsets),
	).Scan(
		&audit.PublishedRows,
		&audit.CorroboratedRows,
		&audit.DistinctCorroboratedRecords,
		&audit.UnconfirmedRows,
		&audit.UnmeasuredRows,
		&audit.AgedOutRows,
		&audit.BeyondEndRows,
		&oldestTerminalAt,
		&corroboratedFrom,
		&corroboratedTo,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordIntervalAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker records of published event outbox entries",
			"audit_event_records_in_intervals", err)
	}

	if oldestTerminalAt.Valid {
		audit.OldestTerminalAt = oldestTerminalAt.Time.UTC()
	}
	if corroboratedFrom.Valid {
		audit.CorroboratedFrom = corroboratedFrom.Time.UTC()
	}
	if corroboratedTo.Valid {
		audit.CorroboratedTo = corroboratedTo.Time.UTC()
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.published_rows", audit.PublishedRows),
		attribute.Int64("event_outbox.corroborated_rows", audit.CorroboratedRows),
		attribute.Int64("event_outbox.uncorroborated_rows", audit.UncorroboratedRows()),
		attribute.Int64("event_outbox.beyond_end_rows", audit.BeyondEndRows),
	)

	return audit, nil
}

// ListUndrainedEventTopics groups every row that still owes a publish by the
// destination topic recorded on it. See database.eventOutboxRepository for what the
// audit is for and why "undrained" is the two sets it is rather than simply the
// non-terminal statuses.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//
// Returns:
//   - []model.EventTopicBacklog: one entry per distinct destination topic that still
//     owes work, ordered by the oldest outstanding occurrence first, so the most
//     overdue generation is read first.
//   - error: a logged internal error.
func (d Datasource) ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListUndrainedEventTopics")
	defer span.End()

	// The status sets are spelled from the model's own literals through parameters, so a
	// status renamed there cannot leave this statement silently matching nothing.
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT
			topic,
			COUNT(*) FILTER (WHERE status NOT IN ($1, $2))          AS undelivered_rows,
			COUNT(*) FILTER (WHERE status = $2)                     AS replayable_rows,
			MIN(occurred_at)                                        AS oldest_occurred_at
		FROM blnk.event_outbox
		WHERE status NOT IN ($1, $3)
		GROUP BY topic
		ORDER BY oldest_occurred_at ASC, topic ASC
	`, model.EventOutboxStatusDispatched, model.EventOutboxStatusDeadLettered,
		model.EventOutboxStatusWebhookPending)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list the undrained destination topics of the event outbox",
			"list_undrained_event_topics", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	backlogs := make([]model.EventTopicBacklog, 0, len(model.AllEventCategories())*2)
	for rows.Next() {
		var (
			backlog model.EventTopicBacklog
			oldest  sql.NullTime
		)
		if err := rows.Scan(
			&backlog.Topic, &backlog.UndeliveredRows, &backlog.ReplayableRows, &oldest,
		); err != nil {
			failDatabaseSpan(span, err)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to read an undrained destination topic of the event outbox",
				"list_undrained_event_topics", err)
		}
		if strings.TrimSpace(backlog.Topic) == "" {
			continue
		}
		if oldest.Valid {
			backlog.OldestOccurredAt = oldest.Time.UTC()
		}

		backlogs = append(backlogs, backlog)
	}
	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to iterate the undrained destination topics of the event outbox",
			"list_undrained_event_topics", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.undrained_topics", len(backlogs)))

	return backlogs, nil
}

// ---------------------------------------------------------------------------------------
// Driver errors must not become API response bodies
//
// apierror.APIError serialises its Details field into the response, and lib/pq's
// *pq.Error is a struct of exported fields — schema, table, column, violated constraint,
// even the PostgreSQL source file that raised it. Passing one as Details hands out a map
// of the database one failed request at a time.
//
// Every driver-origin failure therefore goes through the helpers below: the full error
// is LOGGED with its SQLSTATE, and the API error carries a code and a message and no
// details at all. Each audience gets what it can act on — the operator a structured log
// line naming the operation and the SQLSTATE class, the caller the typed code.
// ---------------------------------------------------------------------------------------

// postgresSQLState extracts the five-character SQLSTATE from a driver error.
//
// It is a bounded, non-revealing value — a standardised error class such as "23505",
// not a message — which makes it safe to put in a log field and useful for grouping
// failures. It returns "unknown" for anything that is not a *pq.Error, including a
// wrapped one, so a log line always has the field.
//
// Parameters:
//   - err error: the error to inspect. May be nil or wrapped.
//
// Returns:
//   - string: the SQLSTATE, or "unknown".
func postgresSQLState(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}

	return "unknown"
}

// ---------------------------------------------------------------------------------------
// The driver's own words must not reach the standard log or a span either
//
// Keeping the driver error out of the RESPONSE is only the first half. It also does not
// belong in either of the two other places that leave the process:
//
//   - a log entry given the cause directly renders the full text into an "error" field.
//   - failDatabaseSpan(span, cause) writes that same text as an exception.message
//     attribute on a span.
//
// Both take the bounded class and the SQLSTATE instead, which is why every helper below
// classifies before it reports.
// ---------------------------------------------------------------------------------------

// The bounded classes a database failure is reported as. Every value is a fixed literal
// and the set is closed, which is what makes them safe as a log field and as a span
// status.
const (
	databaseErrorClassUnknown       = "unknown"
	databaseErrorClassNoRows        = "no_rows"
	databaseErrorClassCancelled     = "context_cancelled"
	databaseErrorClassDeadline      = "context_deadline_exceeded"
	databaseErrorClassConnection    = "connection"
	databaseErrorClassData          = "data_exception"
	databaseErrorClassIntegrity     = "integrity_constraint"
	databaseErrorClassContention    = "serialization_or_deadlock"
	databaseErrorClassSchemaOrGrant = "syntax_or_access_rule"
	databaseErrorClassResources     = "insufficient_resources"
	databaseErrorClassIntervention  = "operator_intervention"
	databaseErrorClassServer        = "internal_server_error"
	databaseErrorClassDriver        = "driver"
	databaseErrorClassApplication   = "application"
)

// databaseErrorClass maps a failure to one of the bounded classes above.
//
// An apierror.APIError is classified by its CODE and never by its message. That is not
// timidity about Blnk's own prose: several of these values carry a Details error that
// interpolates a subscriber or event identifier, so the message is caller data, while
// the code vocabulary is closed and is already the value clients switch on.
//
// Parameters:
//   - err error: the failure to classify. May be nil or wrapped.
//
// Returns:
//   - string: one of the databaseErrorClass* constants, or "api:<CODE>" for a typed
//     application error.
func databaseErrorClass(err error) string {
	var apiErr apierror.APIError

	switch {
	case err == nil:
		return databaseErrorClassUnknown
	case errors.Is(err, sql.ErrNoRows):
		return databaseErrorClassNoRows
	case errors.Is(err, context.Canceled):
		return databaseErrorClassCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return databaseErrorClassDeadline
	case errors.As(err, &apiErr):
		return "api:" + string(apiErr.Code)
	}

	state := postgresSQLState(err)
	if len(state) < 2 {
		return databaseErrorClassDriver
	}

	switch state[:2] {
	case "08":
		return databaseErrorClassConnection
	case "22":
		return databaseErrorClassData
	case "23":
		return databaseErrorClassIntegrity
	case "40":
		return databaseErrorClassContention
	case "42":
		return databaseErrorClassSchemaOrGrant
	case "53":
		return databaseErrorClassResources
	case "57":
		return databaseErrorClassIntervention
	case "XX":
		return databaseErrorClassServer
	default:
		return databaseErrorClassDriver
	}
}

// ErrorClass is the exported form of databaseErrorClass, for a caller OUTSIDE this
// package that has to log a failure this package returned.
//
// The API layer is the caller that needs it. A handler receives an error from a
// repository method and cannot assume anything about its text: IDataSource is an
// interface, so the value may be the bounded apierror.APIError loggedDatabaseError
// returns, a driver error from a different implementation, or a mock's own error.
//
// Parameters:
//   - err error: the failure to classify. May be nil or wrapped.
//
// Returns:
//   - string: a bounded class. Never empty.
func ErrorClass(err error) string {
	return databaseErrorClass(err)
}

// SQLState is the exported form of postgresSQLState, so a caller outside this package
// can carry the standardised five-character error class beside the bounded class from
// ErrorClass.
//
// Parameters:
//   - err error: the failure to inspect. May be nil or wrapped.
//
// Returns:
//   - string: the SQLSTATE, or "unknown".
func SQLState(err error) string {
	return postgresSQLState(err)
}

// LogDiagnostic is the exported form of logDatabaseDiagnostic, so a caller outside this package
// routes a raw cause to the same trace-level sink rather than to its own log line.
//
// Parameters:
//   - operation string: a fixed literal naming what was attempted.
//   - cause error: the raw failure. A nil cause is a no-op.
func LogDiagnostic(operation string, cause error) {
	logDatabaseDiagnostic(operation, cause)
}

// logDatabaseDiagnostic sends the raw cause to the trace-level diagnostic sink.
//
// Parameters:
//   - operation string: what was being attempted, for correlation with the bounded
//     line.
//   - cause error: the raw failure. A nil cause is a no-op.
func logDatabaseDiagnostic(operation string, cause error) {
	if cause == nil || !logrus.IsLevelEnabled(logrus.TraceLevel) {
		return
	}

	// THE RAW CAUSE IS ATTACHED VERBATIM HERE, and only here. This sink exists precisely
	// so the driver's or client's own text is reachable when an operator asks for it
	// explicitly, which is why it is gated on the trace level and why it does NOT go
	// through the redacting helper every other site uses — redacting the one place the
	// full text is supposed to be available would leave it available nowhere.
	logrus.WithField("operation", operation).WithField(logrus.ErrorKey, cause).Trace(
		"database diagnostic: the driver's own error text, which may name schema objects, " +
			"constraints and broker or host addresses and is therefore emitted at trace only",
	)
}

// failDatabaseSpan marks a span failed with a bounded class instead of recording the
// raw error.
//
// span.RecordError is deliberately not used, for the reason the banner above gives: it
// writes the error's own text as an exception.message attribute, and both the driver's
// text and an apierror's interpolated details leave the deployment on a span.
//
// Parameters:
//   - span trace.Span: the span to mark. A nil or non-recording span is tolerated.
//   - cause error: the failure. May be nil.
func failDatabaseSpan(span trace.Span, cause error) {
	if span == nil || cause == nil {
		return
	}

	class := databaseErrorClass(cause)
	span.SetAttributes(
		attribute.String("db.error_class", class),
		attribute.String("db.sqlstate", postgresSQLState(cause)),
	)
	span.SetStatus(codes.Error, class)
}

// loggedDatabaseError logs a driver-origin failure as a bounded class and returns a
// typed API error that carries NO details.
//
// It is the only way an event repository should report a database failure to a caller.
// The alternative — passing the driver error as details — is the disclosure this file's
// error-reporting banner refuses, and it is easy to reintroduce because it reads like
// helpfulness.
//
// Parameters:
//   - code apierror.ErrorCode: the typed code the caller receives.
//   - message string: the caller-facing message. Must not interpolate the cause.
//   - operation string: what was being attempted, for the log field.
//   - cause error: the driver error. Classified, never rendered; the text itself goes
//     only to the trace-level diagnostic sink.
//
// Returns:
//   - error: an apierror.APIError value, so errors.As continues to work at every call
//     site.
func loggedDatabaseError(code apierror.ErrorCode, message, operation string, cause error) error {
	logrus.WithFields(logrus.Fields{
		"operation":   operation,
		"error_class": databaseErrorClass(cause),
		"sqlstate":    postgresSQLState(cause),
		"code":        string(code),
	}).Error(message)
	logDatabaseDiagnostic(operation, cause)

	// The code is carried through UNNORMALIZED, exactly as apierror.NewAPIError does.
	// Normalizing here would quietly rewrite INTERNAL_SERVER_ERROR to GEN_INTERNAL on
	// every repository error, changing the code clients receive — a wire-contract change
	// smuggled in by a logging helper. Whether the legacy codes should be normalized is a
	// real question, and it belongs to the API layer that owns the mapping, not to this.
	return apierror.APIError{Code: code, Message: message}
}

// claimEventsOwedDeadLetterQuery claims rows whose retry budget is spent and whose
// dead-letter write has NOT been recorded, taking a lease and stamping a fresh claim
// token WITHOUT changing their status.
//
// It is a package-level constant so tests can assert that FOR UPDATE SKIP LOCKED, the
// occurred_at ordering, the untouched status and the dlt_topic IS NULL restriction are
// all still present.
const claimEventsOwedDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('failed', 'processing')
			  AND candidate.dlt_topic IS NULL
			  AND candidate.attempts >= candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.next_attempt_at ASC, candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimEventsOwedDeadLetter claims events whose retry budget is spent and whose
// dead-letter write is still owed, so the hand-off can be retried instead of the event
// being stranded in the only table that holds it.
//
// The returned rows carry a FRESH claim token, which is what authorises
// MarkEventDeadLettered, and their attempt count still shows the budget spent, which is
// how a caller knows to retry the hand-off rather than the publish.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimEventsOwedDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventsOwedDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Dead-letter hand-off claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter hand-off lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimEventsOwedDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim events owed a dead-letter write", "claim_events_owed_dead_letter", err)
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
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_events_owed_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_events_owed_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// deadLetterInventoryPredicate builds the WHERE clause shared by the dead-letter
// listing and its count, and appends the values it binds.
//
// The listing and the count MUST select exactly the same set. If they can disagree, an
// operator pages through entries while the total tells them a different number exists,
// and neither figure can be trusted — the sort of discrepancy that is discovered during
// an incident, at the worst possible moment.
//
// Parameters:
//   - query model.DeadLetterQuery: the requested narrowing. Blank filters are skipped,
//     so the zero value yields the unfiltered inventory.
//   - args []interface{}: the argument slice to append to, normally empty.
//
// Returns:
//   - string: the WHERE clause, including the leading "WHERE".
//   - []interface{}: args with the bound filter values appended, in placeholder order.
func deadLetterInventoryPredicate(
	query model.DeadLetterQuery,
	args []interface{},
) (string, []interface{}) {
	var clause strings.Builder

	args = append(args, model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed)
	clause.WriteString(fmt.Sprintf("WHERE status IN ($%d, $%d)", len(args)-1, len(args)))

	if query.Status != "" {
		args = append(args, query.Status)
		clause.WriteString(fmt.Sprintf(" AND status = $%d", len(args)))
	}
	if query.EventType != "" {
		args = append(args, query.EventType)
		clause.WriteString(fmt.Sprintf(" AND event_type = $%d", len(args)))
	}
	if query.Topic != "" {
		// The ORIGINAL category topic, compared against the stored column. That comparison is
		// exact rather than a best effort: topic is NOT NULL and carries CHECK (btrim(topic)
		// <> ''), so no stored row can hold a blank the service would have to substitute an
		// event-type derivation for.
		args = append(args, query.Topic)
		clause.WriteString(fmt.Sprintf(" AND topic = $%d", len(args)))
	}

	// THE SAME OCCURRENCE WINDOW THE LISTING APPLIES. A count drawn from a narrowing the page
	// was not is worse than no count at all: a paging client comparing the two would never
	// terminate, so the window is applied here or in neither place.
	if !query.OccurredFrom.IsZero() {
		args = append(args, query.OccurredFrom.UTC())
		clause.WriteString(fmt.Sprintf(" AND occurred_at >= $%d", len(args)))
	}

	if !query.OccurredTo.IsZero() {
		args = append(args, query.OccurredTo.UTC())
		clause.WriteString(fmt.Sprintf(" AND occurred_at <= $%d", len(args)))
	}

	return clause.String(), args
}

// CountDeadLetterInventory counts the entries a listing query matches, ignoring its
// page. Both dead-letter listings clamp their own limit against
// defaultDeadLetterPageSize and maxDeadLetterPageSize inline, and the inventory listing
// pages by CURSOR rather than by offset.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterQuery: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: how many entries match. Zero is a valid, successful answer.
//   - error: the repository's typed error.
func (d Datasource) CountDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterQuery,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountDeadLetterInventory")
	defer span.End()

	return countDeadLetterInventory(ctx, d.Conn, span, query)
}

// countDeadLetterInventory is the count, parameterised by the connection it runs on,
// for the same reason listDeadLetterInventory is: the paired read must count with the
// predicate the page was drawn with, not with one that resembles it.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - conn sqlQueryer: *sql.DB for a standalone count, *sql.Tx for the paired read.
//   - span trace.Span: the caller's span, annotated here.
//   - query model.DeadLetterQuery: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: as CountDeadLetterInventory.
//   - error: as CountDeadLetterInventory.
func countDeadLetterInventory(
	ctx context.Context,
	conn sqlQueryer,
	span trace.Span,
	query model.DeadLetterQuery,
) (int64, error) {
	span.SetAttributes(attribute.Bool("event_outbox.filtered", query.HasFilters()))

	where, args := deadLetterInventoryPredicate(query, make([]interface{}, 0, 5))

	var total int64
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_outbox
		`+where, args...).Scan(&total)
	if err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count the dead-letter inventory", "count_dead_letter_inventory", err)
	}

	span.SetAttributes(attribute.Int64("event_outbox.dead_letter_total", total))

	return total, nil
}

// ListAndCountDeadLetterInventory returns one page of the inventory AND how many
// entries the same narrowing matches, both read from a single snapshot.
//
// On a triage endpoint that reads as a different amount of stuck work than there is —
// the operator sees 41 entries and a total of 40, or pages to the total and finds the
// backlog is not empty.
//
// Parameters:
//   - ctx context.Context: cancels the transaction.
//   - query model.DeadLetterInventoryQuery: the narrowing and the page.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page, as ListDeadLetterInventory.
//   - int64: how many entries the narrowing matches in the same snapshot.
//   - error: the repository's typed error.
func (d Datasource) ListAndCountDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListAndCountDeadLetterInventory")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the dead-letter inventory", "list_and_count_dead_letter_inventory", err)
	}

	// ROLLED BACK UNCONDITIONALLY, never committed. Nothing was written, so there is
	// nothing to commit, and a rollback releases the snapshot on every path including the
	// error ones. The result is already in hand by then, so a rollback failure is logged
	// rather than returned: discarding a correct answer over a condition the caller cannot
	// act on would be the worse outcome.
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			withLoggableCause(nil, rollbackErr).Error(
				"failed to roll back the dead-letter inventory snapshot")
		}
	}()

	page, err := listDeadLetterInventory(ctx, tx, span, query)
	if err != nil {
		return model.DeadLetterInventoryPage{}, 0, err
	}

	// The page's own narrowing, with the cursor and the limit dropped: which matches to return is
	// not a question about how many there are.
	total, err := countDeadLetterInventory(ctx, tx, span, query.FilterQuery())
	if err != nil {
		return model.DeadLetterInventoryPage{}, 0, err
	}

	return page, total, nil
}

// AuditTerminalEventRecords counts, over a window, how many terminal rows claim a
// broker record and how many of those records are distinct. It is the repository half of
// the zero-loss reconciliation: the three counts are what a comparison against the
// broker's own offsets is drawn from.
//
//   - PublishedRows counts every row whose Kafka leg completed (kafka_dispatched_at is
//     stamped, which covers dispatched AND webhook_pending) plus every dead-lettered
//     row, whose record is on the dead-letter topic.
//
//   - ConfirmedRows counts the subset naming a coordinate. COUNT(kafka_offset) does
//     this directly: SQL COUNT of an expression ignores NULLs, and the all-or-nothing
//     check constraint means a non-NULL offset implies a complete coordinate.
//
//   - DistinctRecords counts the DISTINCT coordinates, FILTERED to the rows that name
//     one.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//
// Returns:
//   - model.EventOutboxAudit: the counts and the instant they were read.
//   - error: a logged internal error.
func (d Datasource) AuditTerminalEventRecords(ctx context.Context, since time.Time) (model.EventOutboxAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditTerminalEventRecords")
	defer span.End()

	since = normalizeEventCountWindow(since)
	span.SetAttributes(attribute.String("event_outbox.window_start", since.Format(time.RFC3339)))

	audit := model.EventOutboxAudit{MeasuredAt: time.Now().UTC(), WindowStart: since}

	// THE FILTER ON THE DISTINCT COUNT IS REQUIRED, not defensive.
	err := d.Conn.QueryRowContext(ctx, auditTerminalEventRecordsQuery,
		model.EventOutboxStatusDeadLettered, since).Scan(
		&audit.PublishedRows, &audit.ConfirmedRows, &audit.DistinctRecords,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventOutboxAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker records of published event outbox entries",
			"audit_terminal_event_records", err)
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.published_rows", audit.PublishedRows),
		attribute.Int64("event_outbox.confirmed_rows", audit.ConfirmedRows),
		attribute.Int64("event_outbox.unconfirmed_rows", audit.UnconfirmedRows()),
	)

	return audit, nil
}

// scanDeadLetterInventoryEntry decodes one narrow inventory row.
//
// It is separate from scanEventOutbox because the projections differ and sharing one
// scanner between them would mean either reading columns the listing does not select or
// silently leaving fields zero — the second of which is how a listing starts reporting
// "this event had no partition key" for every row.
//
// Parameters:
//   - s eventOutboxScanner: *sql.Rows or *sql.Row.
//
// Returns:
//   - model.DeadLetterInventoryEntry: the decoded entry.
//   - error: the driver's scan error, unwrapped, so the caller can classify it.
func scanDeadLetterInventoryEntry(s eventOutboxScanner) (model.DeadLetterInventoryEntry, error) {
	var entry model.DeadLetterInventoryEntry
	var ledgerID, lastError, dltTopic sql.NullString
	var firstAttemptedAt, lastAttemptedAt sql.NullTime
	var failureMetadata []byte
	var payloadBytes sql.NullInt64

	if err := s.Scan(
		&entry.ID,
		&entry.EventID,
		&entry.EventType,
		&entry.AggregateID,
		&entry.PartitionKey,
		&ledgerID,
		&entry.Topic,
		&entry.SchemaVersion,
		&entry.OccurredAt,
		&entry.Status,
		&entry.Attempts,
		&lastError,
		&firstAttemptedAt,
		&lastAttemptedAt,
		&dltTopic,
		&failureMetadata,
		&payloadBytes,
	); err != nil {
		return model.DeadLetterInventoryEntry{}, err
	}

	entry.LedgerID = ledgerID.String
	entry.LastError = lastError.String
	entry.DLTTopic = dltTopic.String
	entry.PayloadBytes = int(payloadBytes.Int64)

	if firstAttemptedAt.Valid {
		instant := firstAttemptedAt.Time
		entry.FirstAttemptedAt = &instant
	}
	if lastAttemptedAt.Valid {
		instant := lastAttemptedAt.Time
		entry.LastAttemptedAt = &instant
	}

	// Assigned only when present, so SQL NULL stays a nil RawMessage rather than becoming
	// the four bytes "null" — which a reader would then decode into a zero-valued record and
	// report as metadata that was captured and happened to be empty.
	if len(failureMetadata) > 0 {
		entry.FailureMetadata = append(json.RawMessage(nil), failureMetadata...)
	}

	return entry, nil
}

// oldestDeadLetterAgeByTopicQuery reports the oldest outstanding entry per dead-letter
// topic as ONE grouped aggregate.
//
// Counting the whole inventory, deriving a tail offset from that count and then reading
// whole rows — bodies included — from that deep offset every collection interval is
// three compounding costs for one number per topic, and it is not even exact: past any
// scan cap it yields a lower bound, and an alert on a lower bound cannot fire when the
// true age crosses the threshold and the bound does not.
//
// This reads the partial index that already covers the two terminal failure states,
// groups by the dead-letter topic and takes MIN of the age instant. No row is fetched,
// the answer is exact, and the cost is set by the number of topics rather than by the
// size of the backlog — which is what matters, because the backlog is largest exactly
// when the gauge matters most.
//
// EVERY OUTSTANDING ENTRY COUNTS, with no exemption. A dead-lettered row leaves this
// aggregate by being REPLAYED — a re-publish the broker acknowledges makes it
// dispatched, and dispatched is not one of the two states counted here.
//
// It now reads CountUnresolvedEventOutbox instead, whose plan is an Index Only Scan of
// idx_event_outbox_status_open.
//
// Two candidate index changes were built and measured against a 1,000,000-row fixture
// with 100,000 outstanding failure rows — a 10% inventory, an order of magnitude worse
// than the criterion allows — and BOTH WERE REJECTED. Recorded here so the experiment
// is not repeated:
//
//   - Adding dlt_topic and last_attempted_at to
//     idx_event_outbox_dead_letter_inventory's INCLUDE list does NOT make this
//     index-only.
//   - An expression index keyed on the two COALESCEs is 36% smaller (4656 kB against
//     7280 kB) and no faster (30.9ms against 31.2ms) — and it cannot REPLACE the
//     inventory index, whose keyset ordering the dead-letter listing pages by, so it
//     would be a second index adding write amplification to every dead-letter
//     transition for no measurable read gain.
const oldestDeadLetterAgeByTopicQuery = `
		SELECT
			COALESCE(NULLIF(dlt_topic, ''), topic || $3) AS age_topic,
			MIN(COALESCE(last_attempted_at, occurred_at)) AS oldest,
			COUNT(*)                                     AS outstanding
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		GROUP BY age_topic
	`

// OldestDeadLetterAgeByTopic reports, per dead-letter topic, the age instant of the
// oldest entry still outstanding and how many are outstanding there.
//
// It is what the dead-letter age gauge and the 15-minute alert are computed from, so it
// is EXACT: no scan cap, no lower bound, no truncation warning to interpret.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - deadLetterSuffix string: the `.dlt` suffix used to name the sibling topic of a
//     row whose dead-letter write has not happened yet.
//
// Returns:
//   - []model.DeadLetterTopicAge: one entry per topic holding something, empty when
//     nothing is outstanding — which is the healthy state and must be reported as a
//     reading rather than as an absence.
//   - error: a logged internal error.
func (d Datasource) OldestDeadLetterAgeByTopic(
	ctx context.Context,
	deadLetterSuffix string,
) ([]model.DeadLetterTopicAge, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "OldestDeadLetterAgeByTopic")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, oldestDeadLetterAgeByTopicQuery,
		model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, deadLetterSuffix)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the oldest dead-letter age per topic", "oldest_dead_letter_age_by_topic", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	ages := make([]model.DeadLetterTopicAge, 0, len(model.AllEventCategories()))
	for rows.Next() {
		var age model.DeadLetterTopicAge
		var oldest sql.NullTime

		if scanErr := rows.Scan(&age.Topic, &oldest, &age.Outstanding); scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan a dead-letter age row", "oldest_dead_letter_age_by_topic", scanErr)
		}

		// MIN over a group that exists cannot be NULL here, because the COALESCE in the
		// query falls back to a NOT NULL column. Scanned through a nullable time all the
		// same, so a future column change cannot turn a reading into a scan error.
		if oldest.Valid {
			age.Oldest = oldest.Time.UTC()
		}

		ages = append(ages, age)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over dead-letter age rows", "oldest_dead_letter_age_by_topic", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.dead_letter_topics", len(ages)))

	return ages, nil
}

// countEventOutboxByStatusQuery counts every row that is NOT terminally delivered
// exactly, and the terminally delivered rows only inside the caller's window.
//
// It is a package-level constant so a test can assert the two-armed shape, which is the
// whole substance of the fix and is invisible from the method's behaviour on a small
// table.
const countEventOutboxByStatusQuery = `
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status <> $1
		GROUP BY status
		UNION ALL
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status = $1 AND occurred_at >= $2
		GROUP BY status
	`

// countUnresolvedEventOutboxQuery is countEventOutboxByStatusQuery's FIRST ARM ALONE:
// every status except the terminal dispatched one, counted exactly and for all time.
const countUnresolvedEventOutboxQuery = `
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status <> $1
		GROUP BY status
	`

// defaultEventCountWindow is the window applied when a caller asks for none.
//
// One day, because that is the period the zero-loss reconciliation runbook reconciles over
// and the period an operator asks "what happened today" about. It is a FALLBACK and not a
// policy: every production caller passes its own window, and this exists so that a caller
// which forgot to gets a bounded reading rather than a whole-table scan.
const defaultEventCountWindow = 24 * time.Hour

// normalizeEventCountWindow turns a caller's window start into a usable one.
//
// Two inputs are corrected rather than honoured, because both would defeat the bound
// this parameter exists to impose:
//
//   - THE ZERO INSTANT would select the whole history — the unbounded scan this window
//     exists to prevent — and it is what a caller that simply forgot the parameter
//     passes.
//   - A FUTURE INSTANT would select nothing, reporting an empty outbox on a busy one,
//     which is worse than expensive: it reads as a healthy system.
//
// Parameters:
//   - since time.Time: the requested window start.
//
// Returns:
//   - time.Time: a UTC instant in the past, never zero.
func normalizeEventCountWindow(since time.Time) time.Time {
	now := time.Now().UTC()
	if since.IsZero() || since.After(now) {
		return now.Add(-defaultEventCountWindow)
	}

	return since.UTC()
}

// auditTerminalEventRecordsQuery counts the rows inside the window that claim a broker
// record, and how many of them name it.
//
// A package-level constant so a test can assert the windowed predicate and the FILTER,
// both of which are load-bearing and neither of which is observable from the returned
// numbers on a small table.
const auditTerminalEventRecordsQuery = `
		SELECT
			COUNT(*)            AS published_rows,
			COUNT(kafka_offset) AS confirmed_rows,
			COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
				FILTER (WHERE kafka_offset IS NOT NULL) AS distinct_records
		FROM blnk.event_outbox
		WHERE kafka_dispatched_at >= $2
		   OR (status = $1 AND last_attempted_at >= $2)
	`

// listDeadLetterInventoryQuery pages the dead-letter inventory with every predicate
// applied in SQL and the page bounded by a KEYSET rather than an offset.
//
// A package-level constant so a test can assert the keyset predicate, the SQL-side
// filters and the ordering, none of which is observable from the rows a small fixture
// returns.
//
// failure_metadata stays, because it is five fields and the API projection fills gaps
// in the row's own columns from it. last_error stays because the projection CLASSIFIES
// it; nothing publishes it verbatim.
const deadLetterInventoryColumns = `id, event_id, event_type, aggregate_id, partition_key, ledger_id, topic, ` +
	`schema_version, occurred_at, status, attempts, last_error, first_attempted_at, last_attempted_at, ` +
	`dlt_topic, failure_metadata, octet_length(payload_raw) AS payload_bytes`

const listDeadLetterInventoryQuery = `
		SELECT ` + deadLetterInventoryColumns + `
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		  AND ($3 = '' OR status = $3)
		  AND ($4 = '' OR event_type = $4)
		  AND ($5 = '' OR topic = $5)
		  AND ($6::timestamptz IS NULL OR (occurred_at, id) < ($6::timestamptz, $7::bigint))
		  AND ($8::timestamptz IS NULL OR occurred_at >= $8::timestamptz)
		  AND ($9::timestamptz IS NULL OR occurred_at <= $9::timestamptz)
		ORDER BY occurred_at DESC, id DESC
		LIMIT $10
	`

// withLoggableCause attaches a driver or dependency error to a log entry in the two
// renderings an operator needs, and it is the ONLY way the event repositories should
// put an error into a line.
//
// The "cause" field is redacted — network topology and secret values removed, control
// characters stripped, length bounded — and is what a deployment writes at info, warn
// and error. The "cause_verbatim" field carries the unredacted text and is attached
// ONLY when the standard logger is at debug, which is the restricted sink.
//
// Parameters:
//   - entry *logrus.Entry: the entry to extend. A nil entry is treated as a fresh one.
//   - err error: the error to attach. A nil error leaves the entry untouched.
//
// Returns:
//   - *logrus.Entry: the entry with the cause fields attached.
func withLoggableCause(entry *logrus.Entry, err error) *logrus.Entry {
	if entry == nil {
		entry = logrus.NewEntry(logrus.StandardLogger())
	}

	if err == nil {
		return entry
	}

	entry = entry.WithField("cause", logsafe.Cause(err))

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		entry = entry.WithField("cause_verbatim", logsafe.CauseVerbatim(err))
	}

	return entry
}

// ListDeadLetteredEvents pages the dead-letter inventory behind the dead-letter API.
//
// Both terminal failure states are included. A row becomes failed the moment its retry
// budget is spent and dead_lettered only once the event has additionally been written
// to its `<topic>.dlt` sibling; listing only the latter would hide events that
// exhausted their retries but whose dead-letter publication itself failed — exactly the
// events an operator most needs to see.
//
// Parameters:
//   - ctx context.Context: request context.
//   - eventIDs []string: the ids to test. Empty or all-blank returns an empty map with
//     no query.
//
// Returns:
//   - map[string]struct{}: the ids that exist. Never nil on success.
//   - error: a typed internal error when the query or scan fails.
func (d Datasource) ExistingEventIDs(ctx context.Context, eventIDs []string) (map[string]struct{}, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ExistingEventIDs")
	defer span.End()

	wanted := make([]string, 0, len(eventIDs))
	for _, id := range eventIDs {
		if strings.TrimSpace(id) != "" {
			wanted = append(wanted, id)
		}
	}

	present := make(map[string]struct{}, len(wanted))
	if len(wanted) == 0 {
		return present, nil
	}

	span.SetAttributes(attribute.Int("event_outbox.requested", len(wanted)))

	// pq.Array keeps this ONE statement with ONE parameter however many ids arrive, so a
	// hundred-transaction batch neither builds a hundred placeholders nor issues a hundred
	// queries. The unique index on event_id serves the lookup.
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT event_id
		FROM blnk.event_outbox
		WHERE event_id = ANY($1)
	`, pq.Array(wanted))
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to look up existing event outbox entries", "existing_event_ids", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			failDatabaseSpan(span, err)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an existing event outbox entry", "existing_event_ids", err)
		}
		present[eventID] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read existing event outbox entries", "existing_event_ids", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.present", len(present)))

	return present, nil
}
