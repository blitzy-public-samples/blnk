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
// # Byte-for-byte payload fidelity, and the two columns that deliver it — READ THIS
//
// The event body is stored TWICE, and which column a reader picks decides whether the
// fidelity guarantee holds:
//
//   - payload is JSONB, a PARSED representation. PostgreSQL sorts object keys,
//     renormalises whitespace, expands exponent notation and collapses a duplicate
//     key, so a body marshaled as {"event":"x","data":{...}} comes back as
//     {"data": {...}, "event": "x"}. Verified against a live database rather than
//     assumed. The column is kept because that parsed form is what makes containment
//     (@>) and member extraction (->, ->>) work, which is how an operator triages a
//     stuck event in SQL.
//   - payload_raw is BYTEA and holds THE EXACT BYTES the producer marshaled. Nothing
//     about a bytea column can validate, reject, transcode or re-render its input,
//     which is what makes it a byte contract rather than a hope.
//
// EVERY READ IN THIS FILE PROJECTS payload_raw INTO model.EventOutbox.Payload, never
// the JSONB column — see eventOutboxColumns. That is what makes the two delivery
// guarantees structural rather than a matter of careful coding:
//
//   - Dual-delivery consistency. The relay publishes to Kafka and enqueues the legacy
//     webhook from the SAME claimed row, and the body it carries is the producer's own
//     bytes, so the two transports cannot differ from each other OR from what the HTTP
//     webhook body would have been (acceptance criterion V-8).
//   - Replay fidelity. A replay re-publishes the STORED bytes rather than re-marshalling
//     a struct, so a replayed event is byte-identical to the event originally published,
//     aside from the added failure metadata (acceptance criterion V-9).
//
// The two columns cannot disagree, and that is a property of the write path rather than
// a convention: eventOutboxInsertArgs binds BOTH from one in-memory slice, and no
// statement anywhere UPDATEs either of them. Nothing else in this file transforms those
// bytes either — the insert binds model.EventOutbox.Payload straight through, and every
// read scans payload_raw back into that json.RawMessage field without re-marshalling.
//
// The practical consequence for anyone writing a test: compare payloads by BYTE equality
// against the pre-insert value. JSON equivalence would also pass against a projection
// that had silently gone back to the JSONB column, which is the one regression this
// design exists to prevent.

package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
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

	// Slice-size bounds for the retention purge. The default keeps one sweep
	// short enough that it never holds locks long against a table the relay is
	// concurrently claiming from; the maximum stops a caller asking for a delete
	// large enough to block the relay and bloat the WAL in one transaction. A
	// caller sweeps in a loop until fewer than its limit are returned.
	defaultEventPurgeBatchSize = 1000
	maxEventPurgeBatchSize     = 10000
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
// payload_raw, NOT payload, is the body column projected here. The BYTEA column holds
// the producer's exact bytes; the JSONB column holds PostgreSQL's normalised
// re-rendering of them, and model.EventOutbox.Payload is what the Kafka publish, the
// legacy webhook leg and a dead-letter replay all serialise. Projecting the JSONB
// column would silently substitute normalised bytes for the producer's own and break
// acceptance criteria V-8 and V-9 without any read failing.
const eventOutboxColumns = `id, event_id, event_type, aggregate_id, partition_key, ledger_id, topic, schema_version, ` +
	`payload_raw, event_raw, occurred_at, status, attempts, max_attempts, next_attempt_at, last_error, first_attempted_at, ` +
	`last_attempted_at, dispatched_at, locked_until, claim_token, webhook_dispatched, kafka_dispatched_at, ` +
	`webhook_attempts, kafka_topic, kafka_partition, kafka_offset, dlt_topic, failure_metadata`

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
	var ledgerID, lastError, claimToken, dltTopic, kafkaTopic sql.NullString
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
	); err != nil {
		return model.EventOutbox{}, err
	}

	// ledger_id is NULLABLE and NULL means "this event has no ledger" as a
	// positive fact — see the column comment in the migration. It collapses to the
	// empty string on the Go side because model.EventOutbox.LedgerID is a plain
	// string tagged omitempty, so both spellings render identically on the wire and
	// no caller has to branch on a pointer.
	e.LedgerID = ledgerID.String
	e.LastError = lastError.String
	// A non-empty claim token means some worker holds this row right now, and it is
	// the value every conditional transition must present to be allowed to write.
	e.ClaimToken = claimToken.String
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
	// schema and this reads back the same way. Assigning the partition or the offset without
	// the topic would produce a row that looks confirmed to a reader testing only the offset
	// and has nothing to look the record up with — see model.EventOutbox.BrokerRecord.
	if kafkaTopic.Valid && kafkaPartition.Valid && kafkaOffset.Valid {
		partition := int(kafkaPartition.Int32)
		offset := kafkaOffset.Int64
		e.KafkaTopic = kafkaTopic.String
		e.KafkaPartition = &partition
		e.KafkaOffset = &offset
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
// Both body columns are populated, from the same bytes: payload for SQL-side
// containment and extraction queries, payload_raw for every byte-level guarantee.
// Populating one without the other is not an option — a reader takes the body from
// payload_raw, so a missing payload_raw is a missing event body.
const eventOutboxInsertColumns = `event_id, event_type, aggregate_id, partition_key, ledger_id, topic, ` +
	`schema_version, payload, payload_raw, event_raw, occurred_at, status, max_attempts`

const eventOutboxInsertQuery = `
		INSERT INTO blnk.event_outbox
		(` + eventOutboxInsertColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id
	`

// eventOutboxInsertValueCount is the number of bound values per inserted row. It
// keeps the batch insert's placeholder arithmetic tied to the column list above
// rather than to a literal that a later column addition would leave stale.
const eventOutboxInsertValueCount = 13

// maxEventOutboxInsertRows bounds how many rows one multi-row INSERT may carry,
// and it is a hard requirement rather than tuning.
//
// PostgreSQL's extended protocol accepts at most 65535 bound parameters per
// statement. At eventOutboxInsertValueCount parameters per row, a single statement
// therefore tops out around 5957 rows — and the transaction coalescing path is
// allowed to present up to 10000 transactions in one batch, so an unbounded batch
// would fail outright on a large enough coalesced write and roll the whole ledger
// transaction back with it. Chunking at 1000 rows keeps every statement an order of
// magnitude clear of the limit while still costing only one round trip per
// thousand events, and every chunk runs inside the caller's transaction, so the
// batch remains all-or-nothing.
const maxEventOutboxInsertRows = 1000

// validateEventOutboxEntry is the persistence-boundary gate on every field of an
// event row, and it runs before ANY insert path — in-transaction, standalone or
// batched. It converts each rejection into a typed bad-request rather than letting
// it surface as an opaque driver error raised deep inside the caller's ledger
// transaction, or worse, as a row that is accepted and then cannot be published.
//
// # Why the persistence layer validates at all
//
// A stored row is a COMMITMENT. Once it is in the table, the relay will try to
// publish it, retry it, spend its budget on it, and finally preserve it on a
// dead-letter topic — and every one of those steps costs work and produces alert
// noise. A row that could never have been published is therefore not a harmless
// bad value; it is a guaranteed dead-letter entry that an operator has to triage.
// Validating here means the failure is reported to the caller that caused it, at
// the moment it is caused, with the field named.
//
// It also validates what the schema's CHECK constraints enforce, one layer above
// them, on purpose. The constraints are the backstop that protects the table from
// any writer; these checks are what produce a diagnosable message instead of
// "violates check constraint event_outbox_partition_key_not_blank".
//
// Each rule, and the specific failure it prevents:
//
//   - Nil entry: a programming error; dereferencing it would panic.
//   - event_id must be a canonical hyphenated UUID. It is the SUBSCRIBER'S
//     IDEMPOTENCY KEY, so a non-UUID or alternatively-spelled value is a key a
//     consumer cannot deduplicate on reliably. An empty one is worse: the first row
//     succeeds and every later one collides on the unique index, surfacing far from
//     its cause.
//   - event_type must be non-blank. It is what subscribers filter on and what the
//     relay routes by; blank is unroutable.
//   - aggregate_id must be non-blank. It is what consumers group by and what the
//     ordering audit queries on.
//   - partition_key must be non-blank. A blank key makes Kafka scatter the event
//     round-robin, which silently destroys the per-aggregate ordering guarantee
//     with nothing in the row to show it happened.
//   - topic must be non-blank AND lie inside the Blnk-owned topic namespace.
//     Without the namespace check, a row could name an arbitrary topic and the
//     relay would lazily create a writer for it and publish there — a write to a
//     destination Blnk does not own, driven by data.
//   - schema_version must be a version this build actually emits. It reaches
//     subscribers on the wire, where an unknown value is an unknown envelope shape.
//   - payload must be present, must be valid JSON, and must fit within
//     model.MaxEventMessageBytes. Absent means there is nothing to publish; invalid
//     JSON would be rejected by the JSONB column anyway but with a driver error
//     instead of a field name; and oversize means the row is accepted here and then
//     rejected by the broker on every one of its attempts until it dead-letters.
//
// The entry must already have been normalised — normalizeEventOutboxEntry supplies
// the defaults a caller may legitimately omit — so that this function sees exactly
// the values that are about to be bound.
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
	if !model.IsBlnkEventTopic(e.Topic, expectedEventTopicPrefix()) {
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
	//
	// The size ceiling applies to the ENVELOPE and not only to the payload: the envelope is
	// what Kafka is asked to accept, and a dead-letter copy of it is larger still. A row
	// over the limit could therefore be published on no attempt and dead-lettered on none
	// either, which is an unbounded backlog from one event. Refusing it at the boundary is
	// what keeps that out of the table.
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

// expectedEventTopicPrefix resolves the topic namespace this deployment owns, for the
// topic-membership check above.
//
// It reads KAFKA_TOPIC_PREFIX and falls back to model.DefaultEventTopicPrefix when
// configuration is unset, blank or not loaded — which mirrors the root package's
// TopicPrefix exactly, so the name a row is validated against is the name that row's
// topic was composed from.
//
// THE FALLBACK IS THE STRICT ANSWER, not a permissive one. Defaulting to "blnk" means a
// deployment that renamed its namespace but has not loaded configuration has its inserts
// REFUSED rather than accepted; the failure is visible and safe. Defaulting to "accept
// anything" would have turned a configuration problem into an open namespace.
//
// The value is re-read per call rather than cached, because the configuration store is
// an atomic value that is re-published whenever configuration is reloaded; a cached
// prefix would keep validating against configuration that no longer exists. The cost is
// one atomic load and a trim of a short string, and nothing pays it on a hot loop —
// validation runs once per inserted row.
func expectedEventTopicPrefix() string {
	cnf, err := config.Fetch()
	if err != nil || cnf == nil {
		return model.DefaultEventTopicPrefix
	}

	if prefix := strings.TrimSpace(cnf.Kafka.TopicPrefix); prefix != "" {
		return prefix
	}

	return model.DefaultEventTopicPrefix
}

// prepareEventOutboxEntry normalises then validates one entry, in that order, and
// is the single sequence every insert path uses.
//
// The order is not interchangeable. Normalisation supplies the schema version, the
// retry budget and the occurrence instant that a caller may legitimately leave
// unset; validating first would reject a perfectly ordinary entry for missing a
// value this layer is responsible for providing. Sharing one function means the
// three insert paths cannot end up applying the two steps in different orders.
func prepareEventOutboxEntry(e *model.EventOutbox) error {
	if e == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox entry is required", nil)
	}
	if err := normalizeEventOutboxEntry(e); err != nil {
		return err
	}

	return validateEventOutboxEntry(e)
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

	// THE CANONICAL ENVELOPE IS DERIVED HERE WHEN A CALLER LEFT IT EMPTY, which is what
	// makes "every row carries the value the broker was given" an invariant rather than a
	// convention. The producer sets it (PrepareEventOutbox), and a row assembled anywhere
	// else — a test fixture, a future caller, a migration backfill — gets the same bytes
	// this version would have stored, composed from the envelope fields already normalised
	// above. Deriving it AFTER the schema version and the occurrence instant is not
	// optional: both are members of the envelope, so composing first would freeze a zero
	// version or a zero timestamp into the stored value.
	//
	// A supplied value is never recomputed. It is the authority, and overwriting it would
	// defeat the entire purpose of storing it.
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
		// ONE slice serves BOTH body columns, and that is the mechanism by which the
		// JSONB projection and the byte-preserving column cannot disagree: PostgreSQL
		// parses this value on its way into payload and stores it verbatim in
		// payload_raw. Binding them from two expressions — or re-marshalling for one of
		// them — is how they would drift, and the drift would be invisible until a
		// replay was compared byte for byte.
		payload,
		payload,
		// THE CANONICAL ENVELOPE, stored rather than rebuilt on every publish. It is
		// never nil here: prepareEventOutboxEntry derives it from the envelope fields
		// when a caller left it empty, and validateEventOutboxEntry refuses a row
		// without one, so this binding cannot violate the column's NOT NULL.
		[]byte(e.EventRaw),
		e.OccurredAt,
		model.EventOutboxStatusPending,
		e.MaxAttempts,
	}
}

// eventIdentifierHashLength is how many hex characters of a digest a log line carries.
//
// Sixteen is 64 bits, which is far more than enough to keep two distinct identifiers apart
// in one deployment's logs while being short enough to read and to grep. It matches the
// producer-side hashLogIdentifier in the root package, so the same identifier produces the
// same token on both sides and the two lines can be joined.
const eventIdentifierHashLength = 16

// hashedEventIdentifier turns a financial identifier into a stable, non-reversible token
// for a log field.
//
// It keeps the one property a log needs — the same identifier always produces the same token
// — and gives up the one it does not need, the identifier itself. An empty input returns an
// empty string rather than the digest of the empty string, so "no aggregate" and "some
// aggregate" stay distinguishable.
//
// Parameters:
//   - value string: the identifier. May be empty.
//
// Returns:
//   - string: a short hex token, or "" for an empty input.
func hashedEventIdentifier(value string) string {
	if value == "" {
		return ""
	}

	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])[:eventIdentifierHashLength]
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
			return loggedDatabaseError(apierror.ErrConflict, "Event outbox entry already exists", "wrap_event_outbox_insert_error", err)
		case "foreign_key_violation":
			return loggedDatabaseError(apierror.ErrBadRequest, "Invalid event outbox entry reference", "wrap_event_outbox_insert_error", err)
		case "not_null_violation":
			return loggedDatabaseError(apierror.ErrBadRequest, "Event outbox entry is missing a required field", "wrap_event_outbox_insert_error", err)
		case "check_violation":
			// The schema's CHECK constraints are the backstop beneath
			// validateEventOutboxEntry. Reaching one means a value slipped past that
			// gate, so this is a bad request and not a server fault — but it is also
			// worth noticing, because the two layers are supposed to agree.
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
//
// It exists so that one insert function serves both the direct path and the
// transactional path of a create-and-capture writer. Duplicating the statement for the
// two paths is how the column list, the generated id and the driver-error mapping drift
// apart, and the drift would show up as a create that behaves differently depending on
// whether an event was being captured alongside it.
type sqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// EventPreparer builds the outbox row for an entity that has JUST BEEN INSERTED, and is
// called from inside the transaction that inserted it.
//
// # Why a create-and-capture writer needs a callback rather than a prepared row
//
// The three creation events — ledger.created, identity.created, balance.created — describe
// an entity whose identity and creation instant are minted INSIDE the repository:
// CreateLedger assigns ldg_<uuid>, CreateIdentity assigns idt_<uuid> unless the caller
// supplied one, CreateBalance assigns bln_<uuid>, and each stamps created_at. The event's
// payload is that finished entity, and its aggregate id and partition key are derived from
// the very id that does not exist until the insert has run. A row prepared BEFORE the call
// would therefore describe an entity that does not exist yet, and moving id generation up
// into the service layer to avoid that would change domain behaviour this change is not
// permitted to touch.
//
// So the caller passes a function instead of a row: the writer inserts the entity, hands
// the finished value to this function, and inserts whatever row comes back — all inside one
// transaction, which is requirement R-2 for these three producers.
//
// # The contract this function must honour
//
// It runs while a database transaction is open, so it must do NO I/O and must not block:
// blnk's implementation marshals the payload and reads process configuration, and nothing
// else. Returning (nil, nil) is legitimate and means "no event to capture" — which is what
// PrepareEventOutbox returns when publishing is not configured, and it leaves the entity
// insert to commit on its own. Returning an error ABORTS the whole transaction, entity
// included, because a mutation whose event cannot be built is exactly the half-committed
// state the outbox exists to rule out.
type EventPreparer[T any] func(entity T) (*model.EventOutbox, error)

// firstEventPreparer resolves a variadic preparer tail to the single preparer to use, or
// nil when none was supplied.
//
// The tail is variadic so that adding event capture to CreateLedger, CreateIdentity and
// CreateBalance keeps every pre-existing caller source-compatible — the API layer, the
// reconciliation and account paths, and a long tail of tests all call them with one
// argument — which is the same reason the atomic transaction writers take their event rows
// variadically. Nils are skipped rather than honoured, because a caller assembling the
// argument conditionally would otherwise take a create down with it.
//
// Only the FIRST non-nil preparer is used. One entity produces one creation event; a second
// preparer would mean two events for one mutation, which is a duplicate publication the
// subscriber cannot distinguish from a genuine repeat.
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

// captureEntityEvent runs a preparer against a just-inserted entity and inserts the row it
// returns inside the caller's transaction.
//
// It is a free function rather than a method because Go does not permit type parameters on
// methods; the datasource is passed explicitly so the insert still goes through the one
// exported in-transaction insert every atomic writer uses.
//
// Parameters:
//   - ctx context.Context: the writer's context.
//   - d Datasource: the datasource whose InsertEventOutboxInTx performs the insert.
//   - tx *sql.Tx: the open transaction the entity was inserted in.
//   - entity T: the finished entity, exactly as it will be returned to the caller.
//   - prepare EventPreparer[T]: the caller's preparer. Must not be nil.
//
// Returns:
//   - error: the preparer's error, or the insert's error. Either aborts the transaction,
//     so the entity is not created without its event.
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
// # THIS PATH IS NOT ATOMIC WITH ITS MUTATION — read this before using it
//
// The transactional-outbox guarantee belongs to InsertEventOutboxInTx and to it
// alone. This method opens no transaction and joins none, so it provides durable
// retry, dead-lettering and replay for the event, and NOTHING about atomicity. A
// caller that has already committed a domain mutation and then calls this has a real
// window: if the process dies, or this insert fails, in between, the domain state is
// committed and its event does not exist. No amount of retrying here closes that
// window, because the mutation is already durable.
//
// It is used by the producers that have NO SINGLE MUTATION to be atomic with:
// balance monitor alerts, bulk transaction batch progress, system.error, and the
// coalesced transaction batch whose writer is called from a file this change may not
// edit. For those, there is no transaction to enrol the event in, so this path is the
// correct one rather than a compromise.
//
// The three ordinary creation events NO LONGER COME HERE. Ledger, identity and balance
// creation each insert their event inside the transaction that inserts the entity —
// see CreateLedger, CreateIdentity and CreateBalance, which take an EventPreparer for
// exactly that purpose — so the window described above is closed for them rather than
// merely documented.
//
// A failure is logged at ERROR with the full event identity, not merely returned, so
// that a lost event is visible in the log even at a call site that discards the
// error. That is the difference between a gap that can be found and one that cannot.
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

		span.RecordError(err)
		// Loud by design. This is the non-atomic path, so a failure here means the
		// domain mutation that produced this event is already committed and the event
		// is gone. Returning the error alone would make that invisible at any call
		// site that logs and continues.
		//
		// The aggregate is reported as a HASH rather than in plaintext. It is a ledger,
		// balance, transaction or identity id — a financial identifier naming whose money
		// this event describes — and this line is emitted on a failure path that a database
		// problem can make high-volume, into a log with a wider audience and a longer life
		// than the database itself. Correlation loses nothing: event_id identifies the event
		// uniquely and is already here, and the token still lets an operator see that several
		// failures share one aggregate, which is all the identifier was contributing. The
		// producer-side log in event_outbox.go hashes the same field for the same reason, so
		// the two lines can be joined on the token.
		logrus.WithFields(logrus.Fields{
			"event_id":          e.EventID,
			"event_type":        e.EventType,
			"topic":             e.Topic,
			"aggregate_id_hash": hashedEventIdentifier(e.AggregateID),
			"atomic":            false,
		}).WithError(err).Error("event lost: non-atomic outbox insert failed after its mutation was committed")

		return wrapEventOutboxInsertError(err)
	}
	return nil
}

// resolveDuplicateEventOutboxInsert decides whether a failed insert is actually the event
// ALREADY BEING RECORDED, and adopts the stored row's identity when it is.
//
// # Why a duplicate here is success and not loss
//
// event_outbox_event_id_uidx exists to make "one event is recorded once" an invariant the
// database enforces. When it rejects an insert it is reporting that the invariant HOLDS — the
// event is durable, exactly once, and every downstream guarantee built on it is intact. Treating
// that as a failure inverted the meaning of the constraint: the caller was told the event was
// lost, an ERROR line said so in the log, and a retrying caller — the bulk-outcome capture is
// one — kept retrying a write that could only ever fail again, then reported the outcome as
// uncaptured when it was sitting in the table.
//
// The stored row's surrogate id is adopted so the caller ends up with the same
// fully-identified row a first-time insert would have produced. Without that, an idempotent
// success would hand back a row with ID 0, which the dead-letter path explicitly refuses.
//
// # What is NOT accepted
//
// Only a unique violation whose stored row is THE SAME EVENT. The comparison is on
// event_raw — the canonical envelope bytes, which carry every envelope member — so two
// genuinely different events that collided on one id stay a conflict and stay loud. That is
// the case worth failing over: it means an id was reused, and silently discarding the second
// event would lose it for real.
//
// A row that cannot be re-read is not accepted either. If the lookup fails, nothing has been
// established, and the caller falls through to the loud path — reporting a possible loss it
// could not rule out is the safe direction.
//
// # This applies to the NON-TRANSACTIONAL path only, and that is deliberate
//
// InsertEventOutboxInTx has no equivalent and must not: PostgreSQL marks a transaction
// ABORTED after any error, so the re-read below could not run, and — more importantly — a
// duplicate inside a ledger transaction means the caller is retrying a mutation whose event was
// already captured. Aborting is then the correct outcome, because the mutation must roll back
// with it.
//
// Parameters:
//   - ctx context.Context: the insert's context, reused for the lookup.
//   - e *model.EventOutbox: the row that failed to insert. Its ID is populated on adoption.
//   - cause error: the driver error the insert returned.
//
// Returns:
//   - bool: true when the event is already recorded identically and the caller may treat the
//     insert as having succeeded.
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

	// The canonical envelope is the whole event, so byte equality here is identity. Compared as
	// bytes rather than field by field because that is exactly what a subscriber received, and
	// because the columns cannot be compared directly — occurred_at comes back truncated to the
	// microsecond PostgreSQL stores, while these bytes round-trip unchanged.
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

// claimPendingEventOutboxQuery claims a batch of publishable rows, takes a lease on
// them and stamps a fresh claim token, all in one statement. It is a package-level
// constant rather than a local so its text is reachable from tests, which assert
// that FOR UPDATE SKIP LOCKED, the occurred_at ordering and the earlier-same-key
// exclusion are all still present — the three properties a well-meaning refactor is
// most likely to drop.
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
// # THE EARLIER-SAME-KEY EXCLUSION, and why SKIP LOCKED alone is not enough
//
// The NOT EXISTS clause is the correctness heart of this query. Read it before
// changing anything here.
//
// FOR UPDATE SKIP LOCKED makes it impossible for two relay instances to claim the
// SAME row. It does NOT make it impossible for them to claim two rows of the same
// aggregate out of order, and that is the failure it hides: relay B skips the
// earlier row relay A holds and claims a LATER row with the same partition key.
// Kafka preserves APPEND order, not occurred_at, so relay B's message can be
// appended first and a subscriber then observes transaction.applied before
// transaction.queued for one transaction. There is no error, no log line and no
// row state to show it happened — the ordering guarantee that the whole
// partitioning scheme exists to provide is simply gone.
//
// NOT EXISTS closes it by making at most ONE row per partition key claimable at any
// instant: a candidate is claimable only when no earlier row sharing its key is
// still pending or processing. It is race-safe without any additional locking,
// because the blocking read runs in the same snapshot in which an
// uncommitted-but-locked earlier row still reads as pending — so a relay that skips
// a locked row also declines every later row of that key.
//
// The blocking set is pending and processing ONLY, and it is deliberately narrower
// than the claimable set. A row that has spent its retry budget (failed) or been
// preserved on its dead-letter topic (dead_lettered) does NOT block its key
// forever. That is an explicit trade: strict ordering would demand it block, but one
// permanently undeliverable event would then stall every subsequent event for that
// aggregate indefinitely, which is a worse failure than a gap. Retries DO preserve
// order, because MarkEventFailed's retry arm returns the row to pending, where it
// blocks its key again.
//
// A row that ClaimFailedEventOutboxForDeadLetter has taken does not block its key
// either, and
// that is the same trade rather than a new one: such a row will never reach the MAIN
// topic under any outcome — the only question left is whether its event is preserved
// on the dead-letter sibling — so a later event of the same aggregate overtaking it
// changes nothing about the main topic's order. Blocking it would stall the
// aggregate for as long as the dead-letter topic was unreachable, which is precisely
// the failure this trade exists to avoid.
//
// The predicate is index-backed by idx_event_outbox_partition_key_inflight, whose
// partial WHERE clause is exactly the blocking set. Widening the blocking set here
// without widening that index turns each candidate check into a scan of the key's
// entire history.
//
// FOR UPDATE SKIP LOCKED is still not optional and must not be replaced by an
// advisory lock, by NOWAIT, or by a status flag alone: it is what lets several relay
// instances claim disjoint batches without blocking each other.
//
// # The claim token
//
// claim_token is stamped here and is what every subsequent transition must present
// to be allowed to write. It converts each transition from "update row 42" into
// "update row 42 IF I still hold it", which is what stops a worker whose lease
// expired from overwriting the newer state of a row another instance has since
// taken. One token is issued per CLAIM BATCH rather than per row: the transitions
// match on (id, token) together, so a shared token is still exact per row, and two
// different claims of the same row necessarily carry different tokens.
//
// The lease in locked_until, rather than an in-memory marker, is what makes a
// crashed relay's in-flight rows recoverable: once the lease expires the rows
// re-enter the claimable set instead of being stranded. attempts < max_attempts
// keeps rows that have spent their retry budget out of the claimable set, so an
// exhausted row is left for the dead-letter path rather than being retried
// forever.
//
// A row whose budget IS spent and whose dead-letter write is still owed is
// therefore invisible to this query, and it is reached by
// ClaimFailedEventOutboxForDeadLetter instead. Keeping the two sets in two statements is
// deliberate: folding the second into an OR here made this predicate
// unsatisfiable by idx_event_outbox_claim's partial index — a partial index is
// usable only when its predicate is implied by the query's — and the planner fell
// back to a sequential scan of the whole table on every poll.
//
// # next_attempt_at <= NOW() is the DUE predicate, and it is what makes the backoff real
//
// The lease answers "has whoever claimed this row abandoned it". It does not answer
// "may anybody claim it yet", and those are different questions for a row that has
// just failed: MarkEventFailed releases the lease, so without this predicate the row
// would be claimable again on the very next poll. At a 1-second poll interval the
// configured 1s/2s/4s/8s/16s schedule then collapses into about five seconds of
// consecutive attempts against a broker that has barely begun to fail.
//
// The alternative — sleeping the computed delay inside the relay — is worse than no
// backoff at all: the schedule sums to 31 seconds, which outlives the 30-second
// lease, so a second instance reclaims the row mid-sleep and publishes it twice.
//
// Filtering on a persisted due instant removes both options, and it does so for every
// instance at once rather than per process. The schedule itself is not computed here;
// the caller supplies the delay to MarkEventFailed, which records the instant.
//
// first_attempted_at and last_attempted_at are stamped here, on the claim, which
// is what model.EventOutbox documents ("nil until the row is first claimed").
// COALESCE pins the first to the first claim and leaves it alone thereafter, while
// the last moves with every claim; together they bound the retry window that the
// dead-letter failure metadata reports.
//
// # webhook_pending is CLAIMABLE but not BLOCKING — the asymmetry is deliberate
//
// SUNSET: this paragraph and the literal it describes go with the legacy transport.
//
// A webhook_pending row has been published to Kafka and recorded in
// kafka_dispatched_at; what it still owes is a legacy webhook enqueue. It is in the
// claimable set so that outstanding enqueue is actually retried — before it existed,
// such a row was marked dispatched and the webhook was never delivered — and the
// caller skips the publish for any claimed row carrying kafka_dispatched_at, so a
// re-claim cannot put a duplicate on the topic.
//
// It is NOT in the blocking set of the NOT EXISTS predicate below. Ordering is a
// property of the Kafka partition, and such a row's position in that partition is
// already fixed, so nothing it does subsequently can reorder anything. Blocking its
// key would instead let a failing LEGACY enqueue stall Kafka delivery for the entire
// aggregate — the deprecated transport interfering with the new one, which is exactly
// backwards. idx_event_outbox_claim's partial predicate covers the wider claimable
// set and idx_event_outbox_partition_key_inflight's covers the narrower blocking one;
// they must keep matching these two lists respectively.
// AS MATERIALIZED IS WHAT MAKES THE LIMIT BINDING. It is not a hint and not an
// optimisation, and removing it reintroduces a defect that is invisible in a small
// table.
//
// Written as `WHERE id IN (SELECT … LIMIT $4 FOR UPDATE SKIP LOCKED)` — which is the
// shape the fund-lineage claim this file is modelled on still uses — the planner is
// free to implement the semi-join as a NESTED LOOP that RE-EXECUTES the subquery once
// per candidate row of the outer scan. EXPLAIN ANALYZE against a six-row backlog with
// a batch size of two showed exactly that:
//
//	Update on blnk.event_outbox (actual rows=6 loops=1)
//	  -> Nested Loop Semi Join (actual rows=6 loops=1)
//	       -> Seq Scan on blnk.event_outbox (actual rows=7 loops=1)
//	       -> Subquery Scan on "ANY_subquery" (actual rows=1 loops=7)
//	            -> Limit (actual rows=1 loops=7)
//
// Seven executions of a LIMIT 2 subquery, each returning a DIFFERENT row because the
// previous execution had already locked its own, so a claim for two rows leased and
// stamped six. The bound the caller asked for was silently discarded.
//
// That is not a cosmetic overshoot. The relay sizes its batch so one poll's work fits
// inside one lease and one process's memory; an unbounded claim leases the entire
// pending backlog under a single claim token, and every row it cannot publish before
// the lease expires is republished by whoever claims it next. It is also PLAN
// DEPENDENT, so it appears and disappears with table statistics — which is why it
// surfaced as an intermittent test failure rather than as an outage.
//
// A MATERIALIZED CTE is evaluated exactly once (PostgreSQL 12+; this schema requires
// 14+), so the candidate set is fixed before the UPDATE runs. The same plan then reads:
//
//	CTE candidates -> Limit (actual rows=2 loops=1)
//	CTE claimed    -> Update on event_outbox (actual rows=2 loops=1)
//
// The CTE scan is still re-scanned by the semi-join, but it returns the same two rows
// every time, so the UPDATE matches exactly those two.
//
// Nothing else about the candidate selection changes: the WHERE clause, the literal
// status list, the ordering and FOR UPDATE SKIP LOCKED are all preserved verbatim, so
// every partial index this query depends on remains usable and the plan assertions in
// the tests still hold.
const claimPendingEventOutboxQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('pending', 'processing', 'webhook_pending')
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.attempts < candidate.max_attempts
			  AND candidate.next_attempt_at <= NOW()
			  AND NOT EXISTS (
				SELECT 1 FROM blnk.event_outbox earlier
				WHERE earlier.partition_key = candidate.partition_key
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
// token. The returned entries are ordered oldest occurrence first, which is the
// order they must be published in, and each carries the token in its ClaimToken
// field — every transition the caller subsequently performs must present it.
//
// At most one row per partition key is ever returned across all concurrent relay
// instances, which is what preserves per-aggregate ordering; see the query comment.
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

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, lockDuration.String(), claimToken, batchSize)
	if err != nil {
		span.RecordError(err)
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
			span.RecordError(scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_event_outbox", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_event_outbox", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimFailedEventOutboxForDeadLetterQuery claims rows whose retry budget is spent and
// whose dead-letter PRESERVATION has not happened, so it can be attempted again.
//
// # The limbo this exists to end
//
// The exhaustion arm of MarkEventFailed sets status = 'failed' and hands the claim token to
// the worker that spent the last attempt, which then writes the event to its `<topic>.dlt`
// sibling and records the result. When that write fails — no transport, a broker outage, a
// topic that does not exist yet — the row is left at 'failed' with dlt_topic still NULL, and
// that state was a dead end in three directions at once: the ordinary claim predicate
// excludes 'failed', replay accepts only 'dead_lettered', and the worker holding the token
// had already moved on. The event existed ONLY as that row. It was visible in the
// dead-letter inventory, which lists both literals precisely so it would be, and nothing in
// the system would ever act on it again.
//
// # What it claims, and what it deliberately leaves alone
//
// dlt_topic IS NULL is the whole definition of "not preserved": the column is set by the
// same statement that moves the row to 'dead_lettered', so its absence on a failed row means
// the message never reached a topic.
//
// THE STATUS IS NOT CHANGED. The row stays 'failed' for two reasons. It keeps the row in
// the dead-letter inventory for the whole repair attempt, so an operator watching the
// backlog does not see events flicker out of it; and MarkEventDeadLettered accepts 'failed'
// as a prior state, so the recovered row can complete through exactly the same transition
// the original attempt would have used. A fresh claim token IS stamped, because the original
// token belonged to a worker that may no longer exist and every transition is conditional on
// the token the caller holds.
//
// # Why the retry is not attempt-bounded
//
// Every other retry in this table is bounded, and this one is not, because there is nowhere
// further to fall back to: the dead-letter topic IS the last resort, and abandoning the
// write would delete the only copy of the event. What bounds it instead is FREQUENCY — a row
// is re-claimable only once its lease has expired — and VISIBILITY: the row stays in the
// dead-letter inventory and feeds blnk.dlt.oldest_message_age_seconds, whose 15-minute alert
// is the escalation path for a preservation that is not succeeding.
//
// The ordering columns match the rest of the file (occurred_at, then id) and the predicate is
// served by idx_event_outbox_failed, whose partial WHERE covers exactly the failed and
// dead-lettered set. FOR UPDATE SKIP LOCKED is required for the same reason it is on the
// ordinary claim: several relay instances must be able to repair disjoint subsets.
// The candidate selection is a MATERIALIZED CTE for the reason given at length on
// claimPendingEventOutboxQuery: inside an IN subquery the LIMIT can be re-executed per
// outer row and the batch bound becomes advisory. All four claims in this file share
// the shape, so all four take the same precaution — fixing one and leaving the others
// would mean the bound holds for the ordinary claim and not for the repair paths, which
// are the ones that run when the system is already unhealthy.
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
// The returned rows are oldest occurrence first and each carries the token in its ClaimToken
// field; MarkEventDeadLettered must be presented that token. See the query comment for why
// the status is deliberately left at 'failed' and why this retry is bounded by frequency
// rather than by an attempt count.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: the maximum number of rows to claim. Must be positive — a zero LIMIT
//     claims nothing and returns no error, which is indistinguishable from an empty repair
//     backlog and would make a broken caller look healthy.
//   - lockDuration time.Duration: the lease. Non-positive is normalised to
//     defaultEventClaimLease rather than rejected, because an expired-on-arrival lease is a
//     correctness problem while failing the poll would stop the repair entirely.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest first. Empty when nothing needs repair,
//     which is the normal state.
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
		span.RecordError(err)

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
		span.RecordError(err)

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
			span.RecordError(scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox entry awaiting dead-letter preservation",
				"claim_failed_event_outbox_for_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))

	return entries, nil
}

// brokerRecordBindings renders a broker coordinate as the three nullable SQL parameters the
// marking statements bind, so all three transitions agree on one definition.
//
// # Why NULL rather than zero
//
// Partition 0 and offset 0 are a perfectly ordinary location — the first record on a fresh
// partition — so binding zeroes for an absent coordinate would write a row that claims to name
// a record it never produced, and the audit would count it as confirmed while an operator
// looking there would find somebody else's event. An absent coordinate must therefore be SQL
// NULL, which is also what the all-or-nothing check constraint requires.
//
// An unconfirmed record is a legitimate input, not an error: the broker acknowledged the write
// and the library reported no coordinate, which leaves the publication real but unnameable. The
// existing values are left in place by COALESCE at the call sites, so a later confirmed write
// can still fill them in.
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

// requireEventOutboxClaimToken rejects a transition attempted without a claim
// token.
//
// Every conditional transition below matches on (id, claim_token), so an empty
// token could only ever match a row whose token is NULL — which is to say a row
// nobody holds, in pending or a terminal state. Letting the call through would
// therefore either match nothing (and be reported as a lost claim, which is
// misleading) or, worse, match a row that is not the caller's to move. Naming the
// omission is the only honest outcome.
func requireEventOutboxClaimToken(claimToken, transition string) error {
	if strings.TrimSpace(claimToken) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox transition %q requires the claim token held for the row", transition), nil)
	}
	return nil
}

// eventOutboxClaimLost is the typed error every conditional transition returns when
// its UPDATE matched no row, and it is a DELIBERATE behaviour change from the
// previous warn-and-continue treatment.
//
// A transition that matches nothing means one of three things, and all three are
// facts the caller must act on rather than facts to log and forget:
//
//   - The lease expired and another relay instance reclaimed the row. The caller is
//     a zombie worker; whatever it just did to the broker has already been, or will
//     be, redone by the current holder, and it must NOT go on to record a terminal
//     state over the top of that holder's work.
//   - The row is no longer in the state this transition moves out of, so somebody
//     else has already moved it on.
//   - The id does not exist, which is a defect.
//
// The old behaviour logged a warning and returned nil, so a caller that had lost its
// claim carried on as though it had succeeded. That is precisely how a stalled
// worker overwrote newer state, how two workers each recorded an attempt against the
// same claim and double-spent the retry budget, and how a terminal row was moved back
// out of its terminal state by a call that arrived late. Returning ErrConflict makes
// the lost claim visible at the point it matters.
//
// It is a CONFLICT and not a server fault: nothing is broken, the caller simply no
// longer owns what it is trying to change. A relay treats it as "stop working on
// this row and move on".
func eventOutboxClaimLost(id int64, transition string) error {
	logrus.WithFields(logrus.Fields{
		"event_outbox_id": id,
		"transition":      transition,
	}).Warn("Event outbox transition matched no row: the claim was lost or the row already moved on")

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event outbox row is no longer claimed for transition %q", transition), nil)
}

// requireEventOutboxRowAffected converts an UPDATE that matched no row into the
// typed lost-claim conflict above.
//
// A driver that cannot report the affected count is treated as success rather than
// failure: the UPDATE itself did not error, so the transition did happen, and
// failing the caller over missing bookkeeping would turn a completed write into a
// spurious retry. Every driver this repository uses does report it.
func requireEventOutboxRowAffected(result sql.Result, id int64, transition string) error {
	if result == nil {
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
		}).Debug("Could not determine rows affected for event outbox transition")
		return nil
	}
	if affected == 0 {
		return eventOutboxClaimLost(id, transition)
	}

	return nil
}

// RenewEventOutboxLease extends the lease on every row still being worked under one claim
// token, and reports how many it extended.
//
// # The defect it exists to remove
//
// The lease was a fixed 30 seconds while a batch could take far longer. At the shipped
// defaults a claim takes 100 rows and publishes them 8 at a time, so the batch runs in 13
// waves; a single wave can occupy the writer's whole 10-second produce timeout before it
// fails. The rows in the last waves therefore had their lease expire BEFORE their publish
// was even attempted — while this relay still held them and still intended to publish them.
// A second instance then claimed and published those rows, and this instance published them
// again afterwards, so the topic received duplicates and every transition this instance
// attempted failed as a lost claim. Nothing in either process reported a defect.
//
// Renewal fixes it without lengthening the recovery latency. The alternatives both cost
// something real: deriving a lease long enough for the worst-case batch would make a
// crashed relay's rows unclaimable for minutes, and shrinking the batch to fit the lease
// would cap throughput below the 500 events per second the pipeline is required to sustain.
//
// # Why the claim token alone identifies the work
//
// Every terminal and near-terminal transition CLEARS claim_token, so a row that has been
// dispatched, dead-lettered, returned to pending or moved to webhook_pending is
// automatically outside this statement's reach. What remains under the token is exactly the
// set still in flight. That is why no id list is passed: the token IS the batch, and the
// database already knows which of its rows are unfinished.
//
// The status guard is processing only — the state the claim itself set. A row in any other
// state is either finished or owned by somebody else, and extending a lease on it would be
// this instance asserting a hold it no longer has.
//
// # Reporting rather than failing
//
// A renewal that extends nothing is not an error: it is the ordinary end of a batch, where
// every row has already reached a terminal state. The count is returned so the caller can
// stop renewing when it reaches zero and can log the difference when it is smaller than the
// batch it expected.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - claimToken string: the token the claim issued. Required; without it the statement
//     could only match rows nobody holds.
//   - lease time.Duration: how long from NOW the extended lease should run. Non-positive is
//     replaced with defaultEventClaimLease, because an expired-on-arrival renewal is worse
//     than no renewal at all.
//
// Returns:
//   - int64: how many rows were extended. Zero is a legitimate answer.
//   - error: a bad-request error for a missing token, or a wrapped driver error.
func (d Datasource) RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RenewEventOutboxLease")
	defer span.End()

	if err := requireEventOutboxClaimToken(claimToken, "renew_lease"); err != nil {
		span.RecordError(err)
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
		span.RecordError(err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the event outbox lease", "renew_event_outbox_lease", err)
	}

	renewed, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The renewal itself succeeded, so this is
		// reported as zero rather than as a failure: the caller uses the count only to
		// decide whether to keep renewing, and one uncounted round is harmless.
		logrus.WithError(err).WithField("claim_token", claimToken).
			Debug("Could not determine how many event outbox leases were renewed")

		return 0, nil
	}

	span.SetAttributes(attribute.Int64("event_outbox.renewed_count", renewed))

	return renewed, nil
}

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged
// the publish, and does so ONLY IF the caller still holds the claim.
//
// The lease and the claim token are released and dispatched_at is stamped in the
// same statement. dispatched is a terminal state: the row is no longer claimable,
// because it is outside the claim predicate's status list.
//
// kafka_dispatched_at is stamped through COALESCE in the same statement, so the
// column answers "was this event published to the broker, and when" for EVERY
// dispatched row rather than only for rows that passed through webhook_pending. The
// COALESCE is what makes it the FIRST acknowledgement rather than the last write: a
// row whose Kafka leg completed on an earlier claim keeps that instant, which is the
// instant the message actually reached the broker.
//
// Both processing and replaying are accepted as the prior state. processing is the
// ordinary relay path; replaying is the successful-replay path, where a
// dead-lettered row that has just been re-published to its original topic becomes
// dispatched and so leaves the dead-letter inventory. Nothing else is accepted, so
// a late call cannot resurrect a row out of a terminal state.
//
// # The broker coordinate (OBS-02)
//
// The topic, partition and offset the broker assigned are persisted here, which is what turns
// "this row claims a publication" into "this row IS that record". They are written through
// COALESCE for the same reason kafka_dispatched_at is: a row whose Kafka leg completed on an
// earlier claim already names its record, and a later marking must not overwrite that with the
// nil coordinate a webhook-only pass carries. An unconfirmed coordinate therefore leaves the
// existing values alone rather than erasing them.
//
// A caller that has lost its claim receives ErrConflict and must not treat the
// publish as recorded — see eventOutboxClaimLost.
//
// The gap between the broker's acknowledgement and this call is where at-least-once
// delivery comes from. A relay that dies in that gap has published the event but
// not recorded it, so the row is reclaimed once its lease expires and published
// again. That duplicate is by design and is suppressed at the consumer's
// idempotency boundary on event_id; nothing here tries to close it, because closing
// it would require a distributed transaction with the broker.
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
		span.RecordError(err)
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
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dispatched", "mark_event_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDispatched); affectedErr != nil {
		span.RecordError(affectedErr)
		return affectedErr
	}
	return nil
}

// MarkEventFailed records a failed publish attempt and its reason, decides in the
// same statement whether the entry is retried or has spent its budget, and reports
// that decision back to the caller.
//
// The status is chosen in SQL rather than in Go: if the increment exhausts
// max_attempts the row becomes failed, otherwise it returns to pending for another
// attempt. Deciding it inside the UPDATE keeps the read and the write atomic, so
// two relay instances racing on one row cannot both conclude they were the last
// attempt. In a SET list every right-hand reference to attempts reads the OLD value,
// so both arms of the CASE agree on the same arithmetic.
//
// # THE CALLER'S VERDICT IS THE OTHER HALF OF THAT DECISION
//
// terminal is the publisher's own answer to "can another attempt succeed?", and it
// short-circuits the arithmetic: a terminal failure exhausts the row on whichever
// attempt it happened, budget remaining or not.
//
// Without it the durable state contradicted what the publisher had already reported
// and metered. The publisher classifies a failure as transient or permanent and
// reports PublishStatusDeadLettered for a permanent one — an oversized envelope,
// bytes that are not valid JSON, a destination outside the topic namespace Blnk owns
// — because no retry can change the answer. That verdict was then thrown away here,
// and the row returned to pending to spend its whole schedule rediscovering it. The
// consequences were all in the same direction: 31 seconds of backoff before the event
// reached the dead-letter topic where an operator could see it, four extra attempts of
// relay throughput spent on a message that can never be published, and a permanently
// stuck event reported as a busy one in the status counter for the whole interval.
//
// The decision still happens in SQL, and it must: the parameter is one input to the
// CASE rather than a Go-side branch, so two instances racing on one row still cannot
// both conclude they were the last attempt.
//
// The arithmetic arm is NOT removed. A transient failure is still bounded by
// max_attempts, which is what stops an unreachable broker keeping a row claimable for
// ever, and terminal only ever brings that boundary forward.
//
// # Why it returns an outcome instead of just an error
//
// The caller has to know which arm was taken, because only the exhaustion arm hands
// off to the dead-letter path. Deriving that in Go from a re-read of the row would
// reintroduce exactly the race the in-SQL decision exists to remove: between the
// UPDATE and the re-read, another instance could have moved the row again. The
// outcome carries the resulting status, the new attempt count, whether the budget is
// spent, and the token to use for the hand-off.
//
// # What happens to the claim, and why the two arms differ
//
// On the RETRY arm the lease and the token are both released, which is what lets the
// row be re-claimed — by this instance or another — on a later poll.
//
// On the EXHAUSTION arm the token is RETAINED. The row is now failed and owes one
// more step: the dead-letter write, followed by MarkEventDeadLettered. Retaining the
// token means only the worker that spent the last attempt can perform that step, so
// two instances cannot both dead-letter the same event and put two copies on the
// dead-letter topic. The lease is released either way, which is harmless here
// because failed is outside the claimable set.
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
// Both are stamped here as well as on the claim, because the dead-letter failure
// metadata needs the retry window bounded either way.
//
// # retryAfter is what makes the backoff real
//
// The row's next due instant becomes NOW() + retryAfter on the retry arm, and the
// claim predicate is next_attempt_at <= NOW(). That is the whole reason this
// parameter exists. Before it, the lease was simply released and the row was
// claimable again on the very next poll: the relay either retried immediately —
// hammering a broker that was already failing — or slept in process, which blocks
// the batch and, at the configured schedule (1s + 2s + 4s + 8s + 16s = 31s),
// outlives the 30-second lease so a second instance reclaims the row mid-sleep and
// publishes it twice. Persisting the decision removes both options, for every
// instance at once.
//
// THE SCHEDULE ITSELF IS THE CALLER'S. RELAY_RETRY_BASE_BACKOFF_MS, the doubling and
// the RELAY_RETRY_MAX_BACKOFF_MS cap are configuration, and computing them in SQL
// here would freeze them into a migration and put them out of reach of the
// configuration meant to govern them. This method records only the instant the
// caller decided on. A negative retryAfter is treated as zero — due immediately —
// because a caller that computed a negative delay meant "no delay", and reaching
// into the past would be indistinguishable from it while looking deliberate.
//
// THE EXHAUSTION ARM LEAVES next_attempt_at ALONE, and the CASE expression that does
// so is the point rather than a flourish.
//
// A row whose budget is spent is not waiting for a retry, and nothing reads a due
// instant on it. The ordinary claim requires attempts < max_attempts, so it can never
// reach an exhausted row; and the dead-letter repair claim
// (claimFailedEventOutboxForDeadLetter) selects on status, dlt_topic and the LEASE
// only — it is throttled by frequency, not by a due instant. Writing a future instant
// there would therefore be read by exactly one audience, an operator triaging a
// dead-letter backlog, and it would tell them a retry was still coming when the budget
// that would have paid for it is gone. Leaving the value as the last retry set it keeps
// the row's own history readable instead.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: the failure reason, stored in last_error.
//   - retryAfter time.Duration: how long the row must wait before its next publish
//     attempt. Applied on the retry arm only. Non-positive means due immediately.
//   - terminal bool: the caller's verdict that no further attempt can succeed. True
//     exhausts the row on this attempt whatever the budget says; false leaves the
//     decision to the attempt arithmetic.
//
// Returns:
//   - model.EventFailureOutcome: the resulting status, the new attempt count, whether
//     the budget is spent, whether the caller declared the failure permanent, and the
//     token for the dead-letter hand-off.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	terminal bool,
) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventFailed")
	defer span.End()
	if retryAfter < 0 {
		retryAfter = 0
	}
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
		attribute.Bool("event_outbox.terminal", terminal),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		span.RecordError(err)
		return model.EventFailureOutcome{}, err
	}

	// $8 is the caller's terminal verdict, ORed into every arm of the decision so the
	// status, the due instant and the retained token all agree about which arm was taken.
	// Repeating the condition rather than computing it once is what keeps the whole
	// decision inside the single statement.
	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = NULL,
			next_attempt_at = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN next_attempt_at ELSE NOW() + $4::interval END,
			claim_token = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN claim_token ELSE NULL END
		WHERE id = $5 AND claim_token = $6 AND status = $7
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, retryAfter.String(), id, claimToken,
		model.EventOutboxStatusProcessing, terminal).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row matched, so the claim was lost or the row is no longer
			// processing. RETURNING makes this reachable as ErrNoRows rather than as
			// a zero affected count.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			span.RecordError(lost)
			return model.EventFailureOutcome{}, lost
		}
		span.RecordError(err)
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
// becomes failed on this attempt, whatever budget it had left, and the dead-letter write
// is owed immediately.
//
// # Why a separate transition rather than a flag on MarkEventFailed
//
// The two answer different questions and one of them is decided in a different place.
// MarkEventFailed asks the DATABASE whether the budget is spent, because two relay
// instances racing on one row must not both conclude they were the last attempt. This one
// carries a decision the PUBLISHER already made — the broker refused the write for a
// reason no further attempt can change: an unauthorised principal, a destination outside
// the topic catalogue, bytes that are not valid JSON, a message over the size limit.
// Bolting that onto the same method would mean one statement whose exhaustion test is
// sometimes SQL and sometimes a parameter, which is how the atomicity the other arm
// depends on gets lost in a later edit.
//
// Before this existed the relay had no way to act on that verdict. A topic-authorisation
// failure spent all five attempts and ~17 seconds of backoff proving the broker meant it,
// five times per event, at whatever rate events were being produced — and the log said
// "retryable=false" on each attempt and then "scheduled for another attempt" immediately
// after, which is a log contradicting itself about the decision it just took.
//
// # What it does, and what it deliberately leaves alone
//
// attempts is INCREMENTED, so the failure metadata reports the number of attempts that
// were really made — 1 for a permanent failure on the first attempt, which is the honest
// figure and is what tells an operator triaging the dead-letter topic that this event
// never had a chance rather than that it fought for thirty seconds.
//
// The row may therefore end up failed with attempts < max_attempts, and that is correct
// and safe: ClaimPendingEventOutbox filters on status IN ('pending','processing',
// 'webhook_pending'), so an unspent budget cannot make a failed row claimable again, while
// ClaimFailedEventOutboxForDeadLetter selects on status, dlt_topic IS NULL and the lease
// alone — so the dead-letter repair path still reaches it if the write below it fails.
//
// The CLAIM TOKEN IS RETAINED, exactly as MarkEventFailed's exhaustion arm retains it and
// for exactly the same reason: the dead-letter write and the MarkEventDeadLettered that
// records it are still owed, and only the worker that took this decision may perform them.
// That is what stops two workers putting two copies of one event on a dead-letter topic.
//
// The LEASE is released, which is harmless because failed is outside the claimable set,
// and next_attempt_at is left as the last retry set it — there is no next attempt to
// describe, and writing a future instant would tell an operator a retry was still coming.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: the failure reason, stored in last_error.
//
// Returns:
//   - model.EventFailureOutcome: Status failed, the new attempt count, Exhausted true —
//     because no further attempt will be made whatever the count says — and the token for
//     the dead-letter hand-off.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventPermanentlyFailed(ctx context.Context, id int64, claimToken, errMsg string) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventPermanentlyFailed")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		span.RecordError(err)
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
			locked_until = NULL
		WHERE id = $3 AND claim_token = $4 AND status = $5
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, errMsg, id, claimToken,
		model.EventOutboxStatusProcessing).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The claim was lost, or the row is no longer processing. Reachable as
			// ErrNoRows rather than as a zero affected count because of RETURNING.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			span.RecordError(lost)
			return model.EventFailureOutcome{}, lost
		}
		span.RecordError(err)
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

// claimEventsOwedDeadLetterQuery claims rows whose retry budget is spent and whose
// dead-letter write has NOT been recorded, taking a lease and stamping a fresh claim
// token WITHOUT changing their status.
//
// It is a package-level constant so tests can assert that FOR UPDATE SKIP LOCKED, the
// occurred_at ordering, the untouched status and the dlt_topic IS NULL restriction are
// all still present.
//
// # The set, and why it must be reachable at all
//
// MarkEventFailed's exhaustion arm records 'failed' and RETAINS the claim token so the
// worker that spent the last attempt is the only one permitted to write the event to its
// `<topic>.dlt` sibling. When that write — or the MarkEventDeadLettered that records it —
// fails, the row is left failed with no dlt_topic, and at that point it was reachable by
// nothing at all: the main claim excludes it twice over (wrong status, no budget) and
// ClaimEventForReplay accepts only dead_lettered. This table is the ONLY copy of that
// event — the retention purge deliberately refuses to delete a failed row for exactly this
// reason — so an operator's only recourse was a hand-written UPDATE.
//
// 'processing' is included alongside 'failed' because a relay can die between this claim
// and the dead-letter record; such a row is recovered by the same predicate once its lease
// expires. dlt_topic IS NULL is what makes the set SELF-CLEARING: recording the
// dead-letter takes the row out of it permanently.
//
// # Why the status is deliberately NOT changed
//
// Moving the row to 'processing' would put it back into the main claim's blocking set, so
// a row whose dead-letter topic was unreachable would stall every later event of its
// aggregate for as long as the outage lasted. Leaving it failed keeps the trade the main
// claim's anti-join documents: an event that can never reach the main topic must not hold
// its key back. MarkEventDeadLettered accepts 'failed' as a prior state precisely so this
// works.
//
// # next_attempt_at is the retry pacing
//
// MarkEventFailed's exhaustion arm schedules it exactly as its retry arm does, so a
// dead-letter topic that is unreachable is retried on the same bounded backoff the publish
// attempts used rather than once per poll interval per stranded row.
// MATERIALIZED for the reason given on claimPendingEventOutboxQuery.
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
// In a healthy system this returns nothing: the hand-off happens in the same batch as the
// attempt that exhausted the budget. It matters when the dead-letter topic is unreachable,
// when the broker rejects the write, or when the relay dies between the write and the
// record — and in every one of those cases the alternative is an event that exists nowhere
// else and can be neither published, replayed nor purged.
//
// The returned rows carry a FRESH claim token, which is what authorises
// MarkEventDeadLettered, and their attempt count still shows the budget spent, which is
// how a caller knows to retry the hand-off rather than the publish.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim. Non-positive is rejected, because a zero LIMIT
//     claims nothing and returns no error, which is indistinguishable from "nothing is owed".
//   - lockDuration time.Duration: the lease. Non-positive is normalised to
//     defaultEventClaimLease, since an expired-on-arrival lease lets two relays write the
//     same event to the dead-letter topic.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimEventsOwedDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventsOwedDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Dead-letter hand-off claim batch size must be greater than zero", nil)
		span.RecordError(err)
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
		span.RecordError(err)
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
			span.RecordError(scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_events_owed_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_events_owed_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// MarkEventDeadLettered records that an entry has been written to its dead-letter
// topic, moving it to the dead_lettered terminal state and storing the dead-letter
// record — and does so ONLY IF the caller still holds the claim.
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
// # The claim requirement, and which prior states are accepted
//
// The token is the one MarkEventFailed returned on its exhaustion arm, or the one
// from the original claim when a non-retryable failure dead-letters a row directly.
// Both failed and processing are therefore accepted as the prior state, and nothing
// else — a row already dead_lettered cannot be dead-lettered again, so a duplicate
// call is reported as a lost claim rather than silently overwriting the existing
// dead-letter record with a second one.
//
// This is what stops two workers each publishing the event to the dead-letter topic:
// only one of them holds the token, so only one gets past this transition, and the
// caller that fails it knows not to have published. THE ORDER MATTERS — publish to
// the dead-letter topic first and record it here second, so a row is never marked
// dead-lettered without a message behind it.
//
// An empty dltTopic or empty failureMetadata is stored as SQL NULL rather than as
// an empty string or an empty JSON document, so "not recorded" stays distinct from
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
		span.RecordError(err)
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

	// The coordinate here names the record on the DEAD-LETTER topic, not on the original one:
	// the original publish is what failed, so there is no record of it to name. It is assigned
	// rather than COALESCEd, because a dead-letter write supersedes whatever a failed original
	// attempt may have left behind — a coordinate on the main topic would send an operator
	// looking for a record the retries never produced.
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
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dead-lettered", "mark_event_dead_lettered", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDeadLettered); affectedErr != nil {
		span.RecordError(affectedErr)
		return affectedErr
	}
	return nil
}

// ClaimEventForReplay atomically claims a dead-lettered event for replay, moving it
// from dead_lettered to replaying and stamping a fresh claim token, and returns the
// claimed row.
//
// # Why a replay must be a claim and not a read
//
// Without this, a replay was a read followed by a check followed by a publish: fetch
// the row, confirm it is dead-lettered, then publish. Two concurrent replays of one
// event both read a dead_lettered row, both pass the check, and both publish — so an
// operator clicking twice, or two operators triaging the same backlog, put two copies
// of the event on the topic. Because a replay re-publishes the STORED bytes, those
// two copies are byte-identical and a consumer deduplicating on event_id will discard
// one, but the duplicate is real, it costs a partition slot, and relying on consumer
// behaviour to correct a defect on the publishing side is not a guarantee.
//
// Making the transition the claim removes the window entirely: only the caller whose
// UPDATE actually changed a row proceeds to publish, and it is handed the token that
// authorises the follow-up transition. The row is not claimable by the relay while it
// is replaying, because replaying is outside the claim predicate's state set.
//
// The lease exists for the same reason it does on an ordinary claim: a process that
// dies mid-replay must not strand the row in replaying forever. ReleaseEventReplay
// returns the row to dead_lettered on every failure path, so a replay that merely
// could not publish stays replayable.
//
// # AND AN EXPIRED REPLAY LEASE IS CLAIMABLE TOO, which is what makes that lease mean
// something
//
// Admitting dead_lettered alone left the lease written down and nothing reading it. A
// process that died mid-replay — or one whose ReleaseEventReplay failed because the
// request context it was running under had already been cancelled — left the row in
// replaying for ever: outside the relay's claimable states, outside the dead-letter
// inventory's terminal state, and refused by every subsequent replay attempt with a
// conflict naming a status the operator cannot clear. The event was preserved on its
// dead-letter topic and permanently unreplayable at the same time.
//
// So the claim admits a replaying row whose lease has lapsed, on exactly the predicate
// the ordinary claim uses for the same purpose. The concurrency guarantee is unchanged:
// a replay holding a LIVE lease is still refused, so two operators triaging one backlog
// still cannot both publish, and the row is reclaimable only once the holder can no
// longer be holding it.
//
// A failure to match is discriminated rather than collapsed, because the two causes
// need different answers: an unknown event_id is a not-found, while a row that exists
// but is not dead-lettered is a conflict whose message names the state it is actually
// in — a replay of an already-dispatched event and a replay of a still-pending one
// are very different operator mistakes. That costs one extra query on the failure
// path only.
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
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim event for replay", "claim_event_for_replay", err)
	}

	span.SetAttributes(attribute.String("event_outbox.claim_token", claimToken))
	return &entry, nil
}

// describeUnclaimableReplay explains why ClaimEventForReplay matched nothing, so the
// caller can answer "no such event" and "that event is not dead-lettered" differently
// instead of conflating them into one unhelpful failure.
//
// A read error here is deliberately not propagated. The replay claim has already
// failed and this call exists only to explain why; returning a database error instead
// of the explanation would replace a precise answer with a vague one.
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
// not cost an event its replayability. Without it a replay that claimed a row and
// then failed to publish would leave the row stuck in replaying — outside the relay's
// claimable set and outside the dead-letter inventory's own terminal state — where
// nothing would ever pick it up again. Every path out of a replay, success or
// failure, must therefore end in either MarkEventDispatched or this method.
//
// replayErr is recorded in last_error when non-empty so the reason the replay failed
// survives for the next operator; an empty string leaves whatever was already there,
// which is the original publish failure, rather than blanking it.
func (d Datasource) ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ReleaseEventReplay")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusReplaying); err != nil {
		span.RecordError(err)
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
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to release event replay claim", "release_event_replay", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusReplaying); affectedErr != nil {
		span.RecordError(affectedErr)
		return affectedErr
	}
	return nil
}

// claimPendingWebhookDeliveriesQuery claims rows whose Kafka leg has FINISHED and
// whose LEGACY WEBHOOK leg is still owed, taking a lease and stamping a fresh claim
// token WITHOUT changing the row's status.
//
// It is a package-level constant for the same reason the main claim query is: tests
// assert that FOR UPDATE SKIP LOCKED, the occurred_at ordering and the untouched
// status are all still present.
//
// # "Finished" means all three of its end states, not only the successful one
//
// The set was 'dispatched' alone, and that lost events. The relay enqueues the legacy
// webhook first and publishes to Kafka second, so when BOTH fail on the attempt that
// spends the retry budget the row travels failed → dead_lettered with
// webhook_dispatched still FALSE. Neither state was in this predicate, neither is in
// the main claim's, and the token is cleared — so the outstanding HTTP delivery was
// discarded silently, in exactly the circumstance where the webhook is the only
// transport that might still work, because the broker being unreachable is why the
// Kafka leg failed at all.
//
// All three literals are therefore candidates. What they have in common is the
// property that matters: the Kafka leg will not be attempted again, so no other
// statement in this file will ever return to the row, and if this one does not carry
// the legacy leg nothing will.
//
// # Why the status must NOT change, and why this is a second query rather than an arm
//
// For a dispatched row the event IS on its Kafka topic; returning the row to
// processing would put it back in the main claimable set and publish it a second time.
// For a dead-lettered row it would be worse still: the row is terminal by design and
// the publisher would take it as unpublished work. So this claim takes only the two
// things a conditional transition needs — a lease, so two relays cannot enqueue the
// same webhook at once, and a claim token, because MarkWebhookDispatched is conditional
// on one and every terminal transition deliberately cleared the row's previous token.
//
// The candidate statuses are LITERALS rather than bound parameters, exactly as in the
// main claim and the dead-letter hand-off claim. That is not styling: a partial index
// is usable only when its predicate is implied by the query's WHERE clause, and the
// planner can only prove implication from a value it can see while planning. Binding
// them would make idx_event_outbox_webhook_pending unusable for any generic plan, and
// dispatched is the largest status in the table. The index's own predicate lists the
// same three literals for that reason.
//
// # The two bounds
//
// webhook_attempts < max_attempts is the legacy leg's own budget, so an abandoned leg
// leaves this candidate set permanently instead of being re-enqueued for ever against a
// receiver that is never coming back. next_attempt_at <= NOW() is the backoff
// MarkEventLegacyWebhookAttempted persisted, so a failing receiver is retried on the
// configured schedule rather than once per poll interval.
//
// SUNSET NOTICE: this query, ClaimPendingWebhookDeliveries, MarkWebhookDispatched,
// MarkEventLegacyWebhookAttempted, the webhook_dispatched and webhook_attempts columns
// and the partial index are all DELETED at the webhook sunset, together with
// webhooks.go and the relay's dual-delivery branch. Nothing else in this file depends
// on them.
//
// MATERIALIZED for the reason given on claimPendingEventOutboxQuery.
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

// ClaimPendingWebhookDeliveries claims rows whose Kafka leg has finished and whose legacy
// HTTP webhook leg has not been recorded, so the dual-delivery window can finish a leg
// that failed alongside the Kafka publish — whether that publish then succeeded or was
// itself given up on.
//
// # The defect this closes
//
// The relay enqueues the legacy webhook task and then publishes to Kafka. A failed
// enqueue was logged and swallowed — correctly, because a webhook receiver being down
// must not consume a Kafka retry attempt or dead-letter an event on the new transport —
// and the log line promised the leg would be retried "on the next claim of this row".
// There was no next claim: the row reached a state the main claim predicate never looks
// at again — dispatched when the publish succeeded, or failed and then dead_lettered when
// it did not — and every one of those transitions cleared the token. The legacy leg was
// lost silently, for exactly the subscribers the 30-day window exists to protect: the ones
// that have not migrated yet.
//
// The both-legs-failed case is the sharper of the two, because it loses the webhook in
// precisely the situation where the webhook is the only transport that might still work.
// The broker being unreachable is why the Kafka leg failed; the receiver may be perfectly
// healthy.
//
// This makes the promise true. The rows are durable and independently reclaimable: the
// webhook_dispatched flag is the outstanding-work marker, the lease serialises two
// relays, and the asynq task identity (the event id) refuses a duplicate even if a
// lease expires mid-enqueue.
//
// In a healthy window this returns nothing: the flag is set microseconds after the
// enqueue. The partial index idx_event_outbox_webhook_pending is what keeps that cheap —
// without it this would scan every dispatched row, which is the largest set in the
// table.
//
// The lease and token this stamps are left in place once MarkWebhookDispatched succeeds,
// and that is deliberate: the flag alone removes the row from this query's candidate set
// permanently, and clearing the token here would break MarkWebhookDispatched's documented
// idempotency for the main dual-delivery path, where the token must survive until
// MarkEventDispatched consumes it.
//
// The one interaction worth stating: a FAILED row can also be claimed by
// ClaimFailedEventOutboxForDeadLetter, which owes it a dead-letter write. The two cannot
// hold it at once — both require the lease to be free and both stamp a fresh token — so a
// claim by either invalidates the other's token and that transition is refused as a lost
// claim. Nothing is lost by it: both sets are self-clearing on the work they exist for
// (dlt_topic being recorded, webhook_dispatched being set), so whichever lost simply
// re-claims once the lease lapses.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim. Non-positive is rejected, because a zero
//     LIMIT claims nothing and returns no error, which is indistinguishable from "no
//     webhook legs are outstanding".
//   - lockDuration time.Duration: the lease. Non-positive is normalised to
//     defaultEventClaimLease, since an expired-on-arrival lease lets two relays enqueue
//     the same webhook.
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
		span.RecordError(err)
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
		span.RecordError(err)
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
			span.RecordError(scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_webhook_deliveries", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_webhook_deliveries", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// MarkWebhookDispatched records that the legacy HTTP webhook leg was dispatched
// for this entry, and does so ONLY IF the caller still holds the claim.
//
// It serves the dual-delivery window only. The relay publishes to Kafka and
// enqueues the legacy webhook task from the SAME claimed row — which is what makes
// the two transports carry byte-identical payloads structurally rather than by
// careful coding — and this flag makes the legacy leg individually idempotent: a
// row republished to Kafka after a crash does not enqueue a second webhook.
//
// The claim token requirement is what makes that idempotency hold under
// concurrency. Without it a worker whose lease had expired could set the flag for a
// row the current holder was about to enqueue for, so the current holder would skip
// the enqueue and the webhook would be recorded as dispatched having never been
// sent. Setting an already-true flag is left as a matching, no-op UPDATE rather than
// excluded by the predicate, so an honest re-call from the claim holder is idempotent
// instead of being reported as a lost claim.
//
// This method, the webhook_dispatched column and the relay branch that calls it are
// all removed at the webhook sunset, together with webhooks.go. Every other method
// in this file outlives it.
func (d Datasource) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkWebhookDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, "webhook_dispatched"); err != nil {
		span.RecordError(err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_dispatched = TRUE
		WHERE id = $1 AND claim_token = $2
	`, id, claimToken)
	if err != nil {
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark webhook dispatched for event outbox entry", "mark_webhook_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, "webhook_dispatched"); affectedErr != nil {
		span.RecordError(affectedErr)
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
// # The defect it exists to remove
//
// The two legs of the dual-delivery window used to share one terminal state. A row
// whose Kafka publish succeeded was marked dispatched even when the webhook enqueue
// alongside it had failed, and because dispatched is outside the claim predicate that
// webhook was never retried and never delivered — permanently, for a transport
// subscribers had been told would keep working until the sunset, with one warning line
// as the only trace. The relay's own comment claimed the leg "will be retried on the
// next claim of this row"; there was no next claim.
//
// So the two legs are now tracked independently. kafka_dispatched_at records the Kafka
// leg, this transition moves the row to webhook_pending, and the claim predicate
// includes that state so the row IS picked up again. The caller skips the publish for
// any claimed row carrying kafka_dispatched_at, so retrying the webhook cannot put a
// duplicate on the Kafka topic.
//
// # The budget, and why it is a separate one
//
// webhook_attempts is incremented here, never attempts. A webhook receiver being down
// or a queue being unreachable must not consume a KAFKA retry attempt, because that
// would let the deprecated transport dead-letter events on the new one. The budget
// ceiling is nonetheless the row's own max_attempts, so one knob governs both legs
// and a permanently unreachable queue cannot keep a row claimable forever.
//
// The decision is taken in SQL, in the CASE, for the same reason MarkEventFailed's is:
// two relay instances working one row cannot both conclude they spent the last webhook
// attempt. In a SET list every right-hand reference reads the OLD value, so both arms
// agree on the same arithmetic.
//
// # What each arm does
//
// RETRY arm: status becomes webhook_pending, the lease and token are released so the
// row can be re-claimed, and next_attempt_at becomes NOW() + retryAfter so the backoff
// is honoured by every instance rather than by a sleeping goroutine.
//
// ABANDON arm: the budget is spent, so the row becomes dispatched and terminal on the
// strength of its Kafka delivery alone. dispatched_at is stamped, the lease and token
// are released, and last_error carries the reason — this is a webhook that will never
// be delivered, and last_error plus the caller's error-level log line are the only
// record of it.
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
//   - retryAfter time.Duration: how long before the row is due again. Applied on the
//     retry arm only. Non-positive means due immediately.
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
		span.RecordError(err)
		return model.EventWebhookOutcome{}, err
	}

	// COALESCEd for the reason MarkEventDispatched documents: this row's Kafka leg may already
	// have completed on an earlier claim, and a webhook-only retry carries no coordinate. A
	// straight assignment would erase the record the first successful publish named.
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
			span.RecordError(lost)

			return model.EventWebhookOutcome{}, lost
		}

		span.RecordError(err)

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

// MarkEventLegacyWebhookAttempted records one failed legacy webhook enqueue against a row
// whose KAFKA leg has already finished, and decides in the same statement whether another
// webhook attempt is made or the legacy leg is abandoned.
//
// SUNSET: this method goes with the rest of the legacy transport.
//
// # Why this exists alongside MarkEventWebhookPending, which looks like the same thing
//
// The two cover disjoint halves of one problem, and collapsing them would break whichever
// half lost. MarkEventWebhookPending handles a row whose Kafka leg SUCCEEDED: it may move
// the status, because webhook_pending and dispatched are both truthful descriptions of such
// a row, and the claim predicate includes webhook_pending precisely so the row comes back.
//
// This one handles a row whose Kafka leg has reached a state the ordinary claim will never
// revisit — dispatched, failed, or dead_lettered — and it MUST NOT MOVE THE STATUS. Moving a
// dead-lettered row to webhook_pending would revive an event that is terminal by design and
// hand it back to the publisher, and marking it dispatched would assert a Kafka delivery that
// never happened, in the one column the zero-loss audit reads. So the status is left exactly
// as it is, and the row's re-claimability comes from the legacy leg's own columns instead:
// webhook_dispatched FALSE and webhook_attempts < max_attempts, which is what
// claimPendingWebhookDeliveriesQuery selects on.
//
// # THE DEFECT THIS CLOSES
//
// The relay enqueues the legacy webhook first and publishes to Kafka second. When BOTH fail
// on the attempt that spends the retry budget, the row goes to failed and then dead_lettered
// — terminal, token cleared, outside every claim predicate — while webhook_dispatched is
// still FALSE. The outstanding HTTP delivery was silently discarded, for exactly the
// subscribers the 30-day window exists to protect, and in exactly the circumstance where the
// webhook is the ONLY transport that might still work: the broker being unreachable is why
// the Kafka leg failed in the first place.
//
// # last_error is deliberately NOT overwritten
//
// On a failed or dead-lettered row, last_error holds the KAFKA failure reason. It is what
// BuildFailureMetadata records, what the dead-letter API projection classifies, and what an
// operator triaging a dead-lettered event reads. Replacing it with a webhook enqueue error
// would destroy the diagnosis of the failure that actually stranded the event, to record a
// transient queue problem that is already logged with the row's fields and already counted
// in webhook_attempts. The durable trail for this leg is the counter; the reason is the log.
//
// # The decision is in SQL, and abandoned is RETURNED rather than derived
//
// For the reason MarkEventFailed's is: two relay instances working one row must not both
// conclude they spent the last webhook attempt. And because the status deliberately does not
// move, the caller cannot infer the arm from it — the comparison is therefore evaluated in
// the RETURNING clause, against the incremented value, and reported.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token ClaimPendingWebhookDeliveries issued; refused without it.
//   - retryAfter time.Duration: how long before the legacy leg is due again. Applied on the
//     retry arm only. Non-positive means due immediately.
//
// Returns:
//   - model.EventWebhookOutcome: the row's UNCHANGED status, the new webhook attempt count,
//     and whether the legacy leg has been abandoned.
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
		span.RecordError(err)

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
			span.RecordError(lost)

			return model.EventWebhookOutcome{}, lost
		}

		span.RecordError(err)

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

// PurgeTerminalEventsBefore deletes terminal event rows whose occurrence is older
// than cutoff, and returns how many it removed. It is the retention primitive
// behind the outbox's data-minimisation contract.
//
// # Why retention is a requirement here and not an optimisation
//
// WHAT THIS TABLE STORES IS SENSITIVE. payload is the webhook body verbatim, so a
// transaction event carries amounts and balance identifiers and an identity event
// carries names, email addresses, phone numbers, postal addresses and dates of
// birth. last_error and failure_metadata carry broker and driver text. None of it has
// any operational value once the event has been delivered, so keeping it
// indefinitely turns a delivery buffer into an unbounded secondary copy of the
// ledger's most sensitive data — with none of the access controls the primary tables
// have around them, and with an ever-growing blast radius if the database is ever
// exposed.
//
// # The safety property: terminal states only
//
// Only dispatched and dead_lettered rows are eligible, taken from
// model.TerminalEventOutboxStatuses so the set is defined in one place. A pending,
// processing, replaying or failed row is still owed a delivery attempt and CANNOT be
// deleted here however old it is. failed is excluded for a specific reason that is
// easy to get wrong: its retry budget is spent but its dead-letter write is still
// owed, so this table is the only copy of the event in existence and deleting it
// would destroy that copy.
//
// # Bounded, resumable deletion
//
// The delete is bounded by limit and driven off idx_event_outbox_terminal_retention,
// so a caller sweeps in slices rather than taking one enormous lock on a table the
// relay is concurrently claiming from. A caller loops until the returned count is
// less than the limit. An unbounded DELETE on a large backlog would block the relay
// for the duration and bloat the WAL in a single transaction.
//
// Choosing the cutoff, and honouring any legal or audit hold that requires a longer
// one, is an operator decision: this method provides the mechanism and takes no view
// on the period.
func (d Datasource) PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PurgeTerminalEventsBefore")
	defer span.End()

	if cutoff.IsZero() {
		// A zero cutoff would read as "delete everything older than the year 1",
		// which deletes nothing — but it is far more likely to be an unset field
		// than an intention, and silently doing nothing would hide a broken
		// retention job that looks like it is running.
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox retention cutoff is required", nil)
		span.RecordError(err)
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

	// This is the one IN-subquery LIMIT in this file that is NOT wrapped in a
	// MATERIALIZED CTE, and the omission is deliberate rather than an oversight.
	//
	// The hazard the four claims guard against — documented at length on
	// claimPendingEventOutboxQuery — is that the planner may re-execute the subquery
	// once per outer row, and that each execution then returns a DIFFERENT row because
	// FOR UPDATE SKIP LOCKED has already taken the previous one. What makes the bound
	// slip is the locking, not the re-execution.
	//
	// There is no locking here. Every execution runs in one statement snapshot over a
	// candidate set nothing is removing, with the same plan and the same ORDER BY, so
	// it returns the same rows and the LIMIT holds however many times it is evaluated.
	// Add FOR UPDATE or FOR UPDATE SKIP LOCKED to this statement and that stops being
	// true, at which point it needs the CTE too.
	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.event_outbox
		WHERE id IN (
			SELECT id FROM blnk.event_outbox
			WHERE status = ANY($1) AND occurred_at < $2
			ORDER BY occurred_at ASC
			LIMIT $3
		)
	`, pq.Array(model.TerminalEventOutboxStatuses()), cutoff, limit)
	if err != nil {
		span.RecordError(err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer, "Failed to purge terminal event outbox entries", "purge_terminal_events_before", err)
	}

	purged, err := result.RowsAffected()
	if err != nil {
		// The delete succeeded; only the count is unavailable. Reporting zero would
		// stop a caller's sweep loop early, so the failure is surfaced instead.
		span.RecordError(err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer, "Failed to count purged event outbox entries", "purge_terminal_events_before", err)
	}

	span.SetAttributes(attribute.Int64("event_outbox.purged_count", purged))
	return purged, nil
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
			return nil, loggedDatabaseError(apierror.ErrNotFound, "Event not found", "get_event_by_id", err)
		}
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to retrieve event outbox entry", "get_event_by_id", err)
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
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to list dead-lettered events", "list_dead_lettered_events", err)
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
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan dead-lettered event", "list_dead_lettered_events", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over dead-lettered events", "list_dead_lettered_events", err)
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
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to count event outbox entries by status", "count_event_outbox_by_status", err)
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
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox status count", "count_event_outbox_by_status", scanErr)
		}
		counts[status] = count
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox status counts", "count_event_outbox_by_status", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.status_count", len(counts)))
	return counts, nil
}

// AuditTerminalEventRecords reports how many rows claim a Kafka record and how many of those
// name the record they produced.
//
// # Why counting alone could never prove zero loss
//
// The reconciliation compares the outbox against the broker's summed end offsets. Records are a
// LOWER BOUND on events — a redelivery, a replay or a dead-letter copy writes a second record
// for one event — so the comparison is directional and a surplus is expected. Its weakness is
// that the surplus is INDISTINGUISHABLE FROM COMPENSATED LOSS: ten lost events plus ten
// redeliveries produce exactly the totals of a healthy pipeline, and the verdict reads "no loss"
// while ten events are genuinely missing.
//
// This query reports the mapping instead of a total. Every row that claims a publication either
// names a coordinate or does not, and a row that does not is counted separately rather than
// being absorbed. ReconcileAgainstOutbox then refuses to call a verdict conclusive while any
// remain — which is what makes an undetectable loss impossible rather than merely unlikely.
//
// # The three counts, and why each is needed
//
//   - PublishedRows counts every row whose Kafka leg completed (kafka_dispatched_at is
//     stamped, which covers dispatched AND webhook_pending) plus every dead-lettered row,
//     whose record is on the dead-letter topic. The unique index on event_id is what makes
//     each row exactly one event.
//   - ConfirmedRows counts the subset naming a coordinate. COUNT(kafka_offset) does this
//     directly: SQL COUNT of an expression ignores NULLs, and the all-or-nothing check
//     constraint means a non-NULL offset implies a complete coordinate.
//   - DistinctRecords counts the DISTINCT coordinates, FILTERED to the rows that name one. It
//     equals ConfirmedRows unless two rows name the same record, which the partial unique index
//     forbids — so a discrepancy means that index is absent, and the audit reports the fact
//     rather than assuming the schema is intact. Two rows sharing one record's corroboration is
//     the same double-counting the whole mechanism exists to remove. The filter is what makes
//     that equality true at all; see the query for why.
//
// # Why the webhook_pending row is included
//
// It HAS been published to Kafka; what is outstanding is the deprecated HTTP leg. Its record is
// on the topic and contributes to the broker's end offsets, so excluding it would inflate the
// apparent surplus and loosen the reconciliation during exactly the dual-delivery window when it
// matters most.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//
// Returns:
//   - model.EventOutboxAudit: the counts and the instant they were read.
//   - error: a logged internal error.
func (d Datasource) AuditTerminalEventRecords(ctx context.Context) (model.EventOutboxAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditTerminalEventRecords")
	defer span.End()

	audit := model.EventOutboxAudit{MeasuredAt: time.Now().UTC()}

	// THE FILTER ON THE DISTINCT COUNT IS REQUIRED, not defensive.
	//
	// COUNT(DISTINCT expr) ignores a NULL expr, but a ROW CONSTRUCTOR whose every field is NULL
	// is not itself NULL in PostgreSQL — so without the filter every row that completed its
	// Kafka leg WITHOUT a coordinate collapsed into one extra "record", (NULL, NULL, NULL), and
	// was counted. A publication the library reported no coordinate for is precisely a row that
	// names NO record, which is what ConfirmedRows already excludes it from.
	//
	// The consequence was not cosmetic. DistinctRecords could EXCEED ConfirmedRows for a reason
	// that has nothing to do with duplication, which breaks both readers of this audit:
	// FullyConfirmed requires the two to be equal and would report an inconclusive verdict on a
	// perfectly healthy outbox, and ReconcileAgainstOutbox derives duplication from their
	// difference and had to clamp a negative it should never have been able to see. The
	// documented contract — DistinctRecords equals ConfirmedRows unless two rows name the same
	// record — is only true with the filter in place.
	err := d.Conn.QueryRowContext(ctx, `
		SELECT
			COUNT(*)            AS published_rows,
			COUNT(kafka_offset) AS confirmed_rows,
			COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
				FILTER (WHERE kafka_offset IS NOT NULL) AS distinct_records
		FROM blnk.event_outbox
		WHERE kafka_dispatched_at IS NOT NULL OR status = $1
	`, model.EventOutboxStatusDeadLettered).Scan(
		&audit.PublishedRows, &audit.ConfirmedRows, &audit.DistinctRecords,
	)
	if err != nil {
		span.RecordError(err)

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

// ---------------------------------------------------------------------------------------
// DATA-01: driver errors must not become API response bodies
//
// apierror.APIError carries its cause in an interface{} Details field that is serialised into
// the response. lib/pq's *pq.Error is a STRUCT WITH EXPORTED FIELDS — Severity, Code, Message,
// Detail, Hint, Schema, Table, Column, Constraint, File, Line, Routine — so passing one as
// Details published the schema name, the table, the column, the violated constraint and even
// the PostgreSQL source file and function that raised it, to whoever called the endpoint. That
// is a map of the database handed out one failed request at a time.
//
// Both event repositories therefore route every driver-origin failure through the helpers
// below: the full error is LOGGED, with its SQLSTATE, and the API error carries a code and a
// message and NO details at all. Details is `omitempty`, so it simply does not appear.
//
// Nothing is lost by that. The operator gets more than before — a structured log line naming
// the operation and the SQLSTATE class — while the caller gets what a caller can act on, which
// is the typed code. The two audiences were previously served by one field, and it was the
// wrong field for one of them.
// ---------------------------------------------------------------------------------------

// postgresSQLState extracts the five-character SQLSTATE from a driver error.
//
// It is a bounded, non-revealing value — a standardised error class such as "23505", not a
// message — which makes it safe to put in a log field and useful for grouping failures. It
// returns "unknown" for anything that is not a *pq.Error, including a wrapped one, so a log
// line always has the field.
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

// loggedDatabaseError logs a driver-origin failure in full and returns a typed API error that
// carries NO details.
//
// It is the only way an event repository should report a database failure to a caller. The
// alternative — passing the driver error as details — is the DATA-01 disclosure, and it is
// easy to reintroduce because it reads like helpfulness.
//
// The error is constructed directly rather than through apierror.NewAPIError so that the
// logging here, which carries the operation and the SQLSTATE, is the only log line for the
// failure rather than a second one alongside a details-only entry.
//
// Parameters:
//   - code apierror.ErrorCode: the typed code the caller receives. Normalised.
//   - message string: the caller-facing message. Must not interpolate the cause.
//   - operation string: what was being attempted, for the log field. A fixed literal.
//   - cause error: the driver error. Logged in full; never returned.
//
// Returns:
//   - error: an apierror.APIError value, so errors.As continues to work at every call site.
func loggedDatabaseError(code apierror.ErrorCode, message, operation string, cause error) error {
	logrus.WithError(cause).WithFields(logrus.Fields{
		"operation": operation,
		"sqlstate":  postgresSQLState(cause),
		"code":      string(code),
	}).Error(message)

	// The code is carried through UNNORMALIZED, exactly as apierror.NewAPIError does.
	// Normalizing here would quietly rewrite INTERNAL_SERVER_ERROR to GEN_INTERNAL on
	// every repository error, changing the code clients receive — a wire-contract change
	// smuggled in by a logging helper. Whether the legacy codes should be normalized is a
	// real question, and it belongs to the API layer that owns the mapping, not to this.
	return apierror.APIError{Code: code, Message: message}
}
