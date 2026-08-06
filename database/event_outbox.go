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
	`payload_raw, occurred_at, status, attempts, max_attempts, next_attempt_at, last_error, first_attempted_at, ` +
	`last_attempted_at, dispatched_at, locked_until, claim_token, webhook_dispatched, dlt_topic, failure_metadata`

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
	var ledgerID, lastError, claimToken, dltTopic sql.NullString
	var firstAttemptedAt, lastAttemptedAt, dispatchedAt, lockedUntil sql.NullTime
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
// Both body columns are populated, from the same bytes: payload for SQL-side
// containment and extraction queries, payload_raw for every byte-level guarantee.
// Populating one without the other is not an option — a reader takes the body from
// payload_raw, so a missing payload_raw is a missing event body.
const eventOutboxInsertColumns = `event_id, event_type, aggregate_id, partition_key, ledger_id, topic, ` +
	`schema_version, payload, payload_raw, occurred_at, status, max_attempts`

const eventOutboxInsertQuery = `
		INSERT INTO blnk.event_outbox
		(` + eventOutboxInsertColumns + `)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id
	`

// eventOutboxInsertValueCount is the number of bound values per inserted row. It
// keeps the batch insert's placeholder arithmetic tied to the column list above
// rather than to a literal that a later column addition would leave stale.
const eventOutboxInsertValueCount = 12

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
	normalizeEventOutboxEntry(e)
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
// It is used by the producers whose mutation repositories do not accept an event row:
// ledger creation, identity creation, balance creation, balance monitor alerts, bulk
// transaction batch progress and system.error. The first three are ordinary domain
// mutations and therefore genuinely carry the gap described above; the last three
// have no single mutation to be atomic with in the first place. Closing the gap for
// the first three requires their repository write paths to accept and insert the
// event row inside their own transactions, exactly as
// RecordTransactionWithBalancesAndOutbox does — those files are outside this
// change's scope, so the gap is documented here rather than described as closed.
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
		span.RecordError(err)
		// Loud by design. This is the non-atomic path, so a failure here means the
		// domain mutation that produced this event is already committed and the event
		// is gone. Returning the error alone would make that invisible at any call
		// site that logs and continues.
		logrus.WithFields(logrus.Fields{
			"event_id":     e.EventID,
			"event_type":   e.EventType,
			"topic":        e.Topic,
			"aggregate_id": e.AggregateID,
			"atomic":       false,
		}).WithError(err).Error("event lost: non-atomic outbox insert failed after its mutation was committed")

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
const claimPendingEventOutboxQuery = `
		WITH claimed AS (
			UPDATE blnk.event_outbox
			SET status = $1,
				locked_until = NOW() + $2::interval,
				claim_token = $3,
				first_attempted_at = COALESCE(first_attempted_at, NOW()),
				last_attempted_at = NOW()
			WHERE id IN (
				SELECT candidate.id FROM blnk.event_outbox candidate
				WHERE candidate.status IN ('pending', 'processing')
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
			)
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

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged
// the publish, and does so ONLY IF the caller still holds the claim.
//
// The lease and the claim token are released and dispatched_at is stamped in the
// same statement. dispatched is a terminal state: the row is no longer claimable,
// because the claim predicate admits only pending and processing rows.
//
// Both processing and replaying are accepted as the prior state. processing is the
// ordinary relay path; replaying is the successful-replay path, where a
// dead-lettered row that has just been re-published to its original topic becomes
// dispatched and so leaves the dead-letter inventory. Nothing else is accepted, so
// a late call cannot resurrect a row out of a terminal state.
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
func (d Datasource) MarkEventDispatched(ctx context.Context, id int64, claimToken string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusDispatched); err != nil {
		span.RecordError(err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dispatched_at = NOW(), locked_until = NULL, claim_token = NULL
		WHERE id = $2 AND claim_token = $3 AND status IN ($4, $5)
	`, model.EventOutboxStatusDispatched, id, claimToken,
		model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying)
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
// On the EXHAUSTION arm next_attempt_at is deliberately left ALONE: a row whose
// budget is spent is not waiting for anything, and a future due instant on a
// terminal row would read as though a retry were still coming.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: the failure reason, stored in last_error.
//   - retryAfter time.Duration: how long the row must wait before it is due again.
//     Applied on the retry arm only. Non-positive means due immediately.
//
// Returns:
//   - model.EventFailureOutcome: the resulting status, the new attempt count, whether
//     the budget is spent, and the token for the dead-letter hand-off.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventFailed(ctx context.Context, id int64, claimToken, errMsg string, retryAfter time.Duration) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventFailed")
	defer span.End()
	if retryAfter < 0 {
		retryAfter = 0
	}
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		span.RecordError(err)
		return model.EventFailureOutcome{}, err
	}

	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = NULL,
			next_attempt_at = CASE WHEN attempts + 1 >= max_attempts
				THEN next_attempt_at ELSE NOW() + $4::interval END,
			claim_token = CASE WHEN attempts + 1 >= max_attempts THEN claim_token ELSE NULL END
		WHERE id = $5 AND claim_token = $6 AND status = $7
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, retryAfter.String(), id, claimToken,
		model.EventOutboxStatusProcessing).Scan(&outcome.Status, &outcome.Attempts)
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
	if outcome.Exhausted {
		// Retained by the UPDATE above, and returned so the caller need not
		// remember which arm keeps it.
		outcome.ClaimToken = claimToken
	}

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
func (d Datasource) MarkEventDeadLettered(ctx context.Context, id int64, claimToken, dltTopic string, failureMetadata json.RawMessage) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDeadLettered")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dlt_topic", dltTopic),
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

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dlt_topic = $2, failure_metadata = $3, locked_until = NULL, claim_token = NULL
		WHERE id = $4 AND claim_token = $5 AND status IN ($6, $7)
	`, model.EventOutboxStatusDeadLettered, dltTopicArg, failureMetadataArg, id, claimToken,
		model.EventOutboxStatusFailed, model.EventOutboxStatusProcessing)
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
// dies mid-replay must not strand the row in replaying forever. Once the lease
// expires an operator can see from locked_until that the claim is stale, and
// ReleaseEventReplay returns the row to dead_lettered so it stays replayable.
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

	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, claim_token = $2, locked_until = NOW() + $3::interval
		WHERE event_id = $4 AND status = $5
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
