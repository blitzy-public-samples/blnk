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

// event.go is the single declaration site for Blnk's Kafka event-publishing
// contract (requirement R-8, the canonical JSON event schema). It holds the
// LedgerEvent wire envelope, the EventOutbox persisted row, the FailureMetadata
// attached to dead-lettered events, the PublishStatus and EventOutboxStatus
// vocabularies, and the one event-type-to-category mapping table in the
// repository.
//
// The file is deliberately dependency-free — it imports nothing but the
// standard library — so that every package taking part in the event pipeline
// can consume it without creating an import cycle. In particular the root
// `blnk` package imports `model`, so `model` can never import `blnk` back;
// keeping this file free of first-party imports is what guarantees the
// producers (root package), the repository layer (`database`), the API layer
// (`api`) and the observability layer (`internal/metrics`) all agree on one
// contract declared in exactly one place.

package model

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SchemaVersionV1 is the initial value of LedgerEvent.SchemaVersion and the
// version every event published today carries.
//
// The envelope is versioned so a subscriber can branch on its shape rather than
// guess: a future additive change that a v1 consumer can safely ignore keeps
// this value, while a breaking reshaping of the envelope increments it, letting
// old and new consumers coexist during a migration.
//
// Every producer MUST reference this constant rather than the literal 1, so
// that a version bump is a one-line change here instead of a repository-wide
// search for stray literals.
const SchemaVersionV1 = 1

// MaxEventMessageBytes is the largest a serialised event message may be, in
// bytes, and it is the ONE limit the whole pipeline validates against.
//
// # Why a limit is required rather than prudent
//
// Two independent sizes were free to disagree. Blnk accepts request bodies up to
// DEFAULT_MAX_REQUEST_BODY_SIZE_MB (5 MiB), and a large request produces a
// proportionally large event payload, because the payload IS the webhook body
// verbatim. kafka-go's writer, meanwhile, caps a message at 1 MiB by default, and
// a Kafka broker's own max.message.bytes defaults to roughly the same. So an
// oversized event was accepted, committed to the outbox, and then failed EVERY
// publish attempt — including the dead-letter write, which is strictly larger
// because it appends failure metadata. The row could reach neither terminal state,
// so it sat in the table being retried forever: unbounded backlog growth from a
// single request, with the relay's throughput spent on an event that can never
// leave.
//
// # Why this value
//
// 768 KiB. It sits below the 1 MiB floor with about 25% of headroom, and the
// headroom is what makes the guarantee hold end to end rather than at the moment
// of validation:
//
//   - The stored payload is wrapped in the LedgerEvent envelope, which adds the
//     five sibling keys and their values.
//   - A dead-letter copy adds the whole failure_metadata object on top of that,
//     including an error string of unbounded length from the broker.
//   - Kafka accounts for the record's key, headers and framing as well as its
//     value.
//
// Validating the message against this limit therefore leaves a dead-letter copy of
// the same event comfortably inside the writer's cap, which is the property that
// matters: an event that can be published can also be dead-lettered.
//
// It is a constant rather than configuration on purpose. Raising it would require
// raising the writer limit, the broker's max.message.bytes and every topic's
// per-topic override together, and a value that can be raised in one place alone is
// a value that will be.
const MaxEventMessageBytes = 768 * 1024

// MaxTraceparentLength and MaxTracestateLength bound the two W3C trace-context values an
// outbox row may carry.
//
// Both come from the W3C Trace Context specification rather than being chosen here. A
// traceparent is exactly 55 characters in version 00 and the specification requires
// implementations to accept longer future versions, so 255 leaves generous room while still
// refusing an unbounded value; tracestate is capped by the specification itself at 512.
//
// The bound matters because these values originate in a CALLER-SUPPLIED HTTP HEADER and are
// stored on a table that takes one row per ledger mutation at 500 events per second. Without
// a ceiling, a caller could append arbitrary bytes to every event row in the ledger, and the
// cost would surface as bloat on the relay's hottest table rather than as a rejected request.
const (
	MaxTraceparentLength = 255
	MaxTracestateLength  = 512
)

// SanitizeTraceContext returns the trace context that may be stored on an outbox row,
// dropping anything unusable.
//
// # Why it drops rather than truncates or rejects
//
// TRUNCATING a traceparent would produce a syntactically invalid one — a mangled trace and
// span id that correlates the event with nothing and that a tracing backend may reject
// outright — so an oversized traceparent is dropped whole. Truncating a tracestate would be
// safe in principle, since it is a list of independent vendor entries, but a partial list is
// a partial claim about which vendors saw the request, and the correlation that matters lives
// entirely in the traceparent. So both are dropped.
//
// REJECTING is not an option. This value is telemetry attached to a ledger mutation, and
// refusing the mutation because a caller sent an oversized header would let a header break
// the ledger. Dropping loses a trace link; rejecting loses money movement.
//
// A tracestate without a traceparent is meaningless — it is vendor state ABOUT a trace whose
// identity has been discarded — so dropping the traceparent drops the tracestate with it.
//
// Parameters:
//   - traceparent string: the W3C traceparent header value, or empty when there is no trace.
//   - tracestate string: the W3C tracestate header value, or empty.
//
// Returns:
//   - string: the traceparent to store, empty when there is none or it was unusable.
//   - string: the tracestate to store, empty when there is none, it was unusable, or the
//     traceparent was dropped.
func SanitizeTraceContext(traceparent, tracestate string) (string, string) {
	parent := strings.TrimSpace(traceparent)
	if parent == "" || len(parent) > MaxTraceparentLength {
		return "", ""
	}

	state := strings.TrimSpace(tracestate)
	if len(state) > MaxTracestateLength {
		state = ""
	}

	return parent, state
}

// LedgerEvent is the canonical JSON envelope published to Kafka for every
// ledger mutation Blnk emits (requirement R-8). Its field set and JSON tags are
// the subscriber-facing contract: they are stable, and none of them is
// `omitempty`, so a subscriber can rely on all six keys being present on every
// message even when a value is at its zero value.
//
// Serialised shape:
//
//	{
//	  "event_id":       "b0f6e2c8-...",
//	  "event_type":     "transaction.applied",
//	  "aggregate_id":   "txn_...",
//	  "occurred_at":    "2024-05-01T12:34:56.789Z",
//	  "payload":        {"event": "transaction.applied", "data": { ... }},
//	  "schema_version": 1
//	}
type LedgerEvent struct {
	// EventID is a UUID that uniquely identifies this event, and it doubles as
	// the subscriber idempotency key.
	//
	// Deduplicating on EventID is a subscriber obligation, not an optional
	// optimisation. The transactional outbox gives exactly-once semantics on
	// the WRITE SIDE, and only for a capture that shares the mutation's
	// transaction: a caller that threads the row through PublishEventInTx or
	// the atomic writers commits event and mutation together, so neither can
	// exist without the other. A standalone capture (PublishEvent, which is
	// what the domain post-action call sites use) commits on its own, after the
	// mutation, and relies instead on the id being DERIVED from the mutation —
	// see DeriveEventID — so that a replayed mutation collides on the unique
	// index rather than admitting a second row. Events with no stable identity
	// (balance.monitor, system.error) take a random id and have no such
	// protection, by design: they are repeatable by nature.
	//
	// Kafka delivery itself remains at-least-once, and the relay can crash in
	// the window between a successfully acknowledged publish and the outbox row
	// being marked dispatched — after which the row is reclaimed and
	// republished. That is why blnk.event_outbox carries a unique index on
	// event_id and why the published documentation states the
	// duplicate-suppression obligation explicitly.
	EventID string `json:"event_id"`

	// EventType is the event name, for example "transaction.applied" or
	// "balance.monitor". It duplicates the event name that already appears
	// inside Payload, hoisted to the envelope level so subscribers can route
	// and filter without parsing the payload at all. The redundant string per
	// message is a deliberate trade for cheap, allocation-free routing.
	EventType string `json:"event_type"`

	// AggregateID identifies the aggregate the event belongs to — the
	// transaction, balance, identity or ledger the mutation acted on.
	//
	// It is distinct from the Kafka message key. The key is the row's STORED
	// PARTITION KEY — a LEDGER ID for every ledger-scoped event, including
	// transactions, and otherwise an identity id, a batch id or the event type;
	// see EventOutbox.PartitionKey — and keying with a stable hash balancer is
	// what pins every event sharing a key to one partition, delivering ordering
	// per partition key. AggregateID is what a consumer groups by once the
	// messages arrive.
	AggregateID string `json:"aggregate_id"`

	// OccurredAt is the instant the domain action happened, RFC3339 on the
	// wire. time.Time's standard JSON encoding already emits RFC3339 with
	// nanosecond precision, so no custom marshaller is defined or wanted here —
	// adding one would be a way to accidentally break the documented format.
	OccurredAt time.Time `json:"occurred_at"`

	// Payload carries the legacy webhook body verbatim, byte for byte.
	//
	// Today's HTTP webhook body is the marshaled NewWebhook struct
	// (`Event string json:"event"` and `Payload interface{} json:"data"`), i.e.
	// the two-key object {"event": ..., "data": ...}. The entire object is
	// carried here, BOTH keys included, so an existing subscriber's body parser
	// keeps working unchanged and only the transport differs. This is the
	// resolved reading of "payload matching today's webhook body
	// field-for-field".
	//
	// The type is json.RawMessage — not map[string]interface{} and not a typed
	// struct — precisely so the bytes pass through untransformed. Decoding into
	// a map and re-encoding would reorder keys and renormalise number literals,
	// which would make both the dual-delivery byte-equality guarantee (the
	// Kafka message and the legacy webhook body are identical because they are
	// produced from the same stored bytes) and the byte-for-byte dead-letter
	// replay guarantee unachievable.
	//
	// It is PERSISTED IN AND READ FROM the payload_raw BYTEA column, not the
	// payload JSONB column beside it. Both columns are written from these same
	// bytes, but only bytea returns them unchanged: JSONB is a parsed
	// representation and reorders members, renormalises whitespace, expands
	// exponent notation and collapses a duplicate key. The JSONB column exists
	// for SQL-side containment and extraction queries during triage and is
	// never read as the body.
	Payload json.RawMessage `json:"payload"`

	// SchemaVersion is the envelope version, starting at SchemaVersionV1.
	SchemaVersion int `json:"schema_version"`
}

// envelopeScaffoldBytes is a sizing hint for CanonicalBytes: the member names, quotes,
// separators, braces and encoded scalars of a typical envelope. It only pre-sizes the
// buffer, so an imprecise value costs at most one reallocation and can never affect the
// output.
const envelopeScaffoldBytes = 256

// canonicalNullPayload is what an empty payload serialises to, so the envelope stays
// parseable JSON rather than carrying an empty member.
var canonicalNullPayload = []byte("null")

// CanonicalBytes serialises the envelope EXACTLY as it goes on the wire, splicing the
// payload bytes in VERBATIM.
//
// # This is the authoritative event value, and it is meant to be STORED
//
// Requirement R-5 asks for a dead-lettered event to be replayable byte-for-byte aside from
// its failure metadata. That is only achievable if the bytes exist somewhere: reconstructing
// the envelope from a row's columns at publish time gives byte equality only for as long as
// this function's output never changes, and any later change — a member added, a member
// reordered, an encoder upgraded — silently breaks the promise for every row already
// captured. So the bytes this returns are persisted once, at capture, in
// blnk.event_outbox.event_raw, and the publish, dead-letter and replay paths reuse THAT value
// rather than calling this again. This function is the single producer of it.
//
// # Why the object is composed member by member rather than marshalled as a struct
//
// Handing the struct to encoding/json would route Payload through the encoder's compactor,
// which strips insignificant whitespace, and — because HTML escaping is on by default —
// rewrites `<`, `>` and `&` inside the raw payload as escape sequences. Either transformation
// breaks the byte-identity the dual-delivery and replay guarantees are asserted on, while
// leaving the two bodies semantically equal, which is exactly the kind of difference a
// reviewer's eye passes over and a byte comparison does not.
//
// The scalar members ARE encoded through encoding/json, so escaping and timestamp formatting
// stay identical to what a struct marshal would produce. Only the payload bypasses it, and
// only because it is already JSON.
//
// Member order is the order LedgerEvent declares: event_id, event_type, aggregate_id,
// occurred_at, payload, schema_version. It is part of the stored value and must not change.
//
// An EMPTY payload is published as JSON null with no error, matching what a nil
// json.RawMessage means, and an INVALID payload is refused: splicing bytes that are not JSON
// would produce a message that breaks every subscriber's parser, and no number of retries
// makes them valid.
//
// Returns:
//   - []byte: the canonical envelope bytes.
//   - error: when the payload is present but is not valid JSON, or a scalar member cannot be
//     encoded.
func (e LedgerEvent) CanonicalBytes() ([]byte, error) {
	payload := e.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = canonicalNullPayload
	} else if !json.Valid(payload) {
		return nil, fmt.Errorf(
			"model: the payload of event %s (%s) is not valid JSON and cannot be published",
			e.EventID, e.EventType,
		)
	}

	scalars := [4]struct {
		key   string
		value interface{}
	}{
		{"event_id", e.EventID},
		{"event_type", e.EventType},
		{"aggregate_id", e.AggregateID},
		{"occurred_at", e.OccurredAt},
	}

	var buf bytes.Buffer
	buf.Grow(len(payload) + envelopeScaffoldBytes)
	buf.WriteByte('{')

	for i, scalar := range scalars {
		encoded, err := json.Marshal(scalar.value)
		if err != nil {
			return nil, fmt.Errorf(
				"model: encoding %s for event %s (%s): %w",
				scalar.key, e.EventID, e.EventType, err,
			)
		}

		if i > 0 {
			buf.WriteByte(',')
		}

		buf.WriteByte('"')
		buf.WriteString(scalar.key)
		buf.WriteString(`":`)
		buf.Write(encoded)
	}

	buf.WriteString(`,"payload":`)
	buf.Write(payload)
	buf.WriteString(`,"schema_version":`)
	buf.WriteString(strconv.Itoa(e.SchemaVersion))
	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// EventOutbox is one row of blnk.event_outbox: a durable record of an event that
// must reach Kafka, together with the relay state machine that gets it there.
//
// The row is inserted inside the caller's own database transaction whenever the
// caller has one to offer: PublishEventInTx and the atomic writers in
// database/transaction.go take the row and insert it before COMMIT, which is the
// transactional-outbox guarantee of requirement R-2 — the mutation and its event
// commit or roll back together. That is the path almost every producer takes, and
// for those events the write side is EXACTLY-ONCE: the event cannot be lost while
// its mutation stands, and cannot exist for a mutation that rolled back.
//
// A caller with no transaction to share captures standalone through PublishEvent
// or PublishEventDurably, and for THREE event classes that is at-most-once rather
// than exactly-once, because their mutation is already committed by the time the
// row is written: balance.monitor, bulk_transaction.<status>, and the
// status-derived transaction.* events of a coalesced batch. A bounded retry makes
// a transient database fault survivable, but a process death in that window loses
// the event with nothing left to replay. PublishEventDurably enumerates the set
// and docs/event-streaming.md publishes it, so a subscriber knows which event
// types carry the weaker guarantee. system.error is standalone too but is not in
// that set: it describes no mutation, so there is nothing it could be atomic with.
//
// Kafka delivery downstream is AT-LEAST-ONCE for every event regardless of which
// path captured it: a relay that crashes between a successfully acknowledged
// publish and marking this row dispatched will reclaim the row after its lock
// expires and publish it again. Duplicate suppression on LedgerEvent.EventID is
// consequently a documented subscriber obligation rather than an implicit promise.
//
// Construction: callers set the event-envelope fields and MaxAttempts
// explicitly in Go and leave ID, Status, Attempts and the creation timestamp to
// the database column defaults, mirroring how the lineage outbox row is
// prepared. No constructor is declared here — the root package owns
// PrepareEventOutbox, which is what marshals the legacy webhook object into
// Payload. Keeping this type a plain data carrier is what lets the repository
// layer, the relay and the dead-letter API all share it without pulling in
// behaviour.
//
// The JSON tags below correspond one-to-one with the blnk.event_outbox columns,
// in the same four groups the table declares them. The repository layer scans
// rows into this struct, so the two must stay aligned.
type EventOutbox struct {
	// --- Event envelope ---
	// These fields reproduce the LedgerEvent that will be published, plus the
	// routing information the relay needs to publish it.

	// ID is the BIGSERIAL surrogate primary key, assigned by the database.
	ID int64 `json:"id"`
	// EventID is the event UUID. It carries a unique index, which is what makes
	// the outbox's exactly-once write-side guarantee enforceable in the schema
	// rather than merely intended in code.
	EventID string `json:"event_id"`
	// EventType is the event name, copied to LedgerEvent.EventType on publish.
	EventType string `json:"event_type"`
	// AggregateID is the aggregate the event belongs to.
	AggregateID string `json:"aggregate_id"`

	// PartitionKey is the Kafka message key, and it is ALWAYS SET.
	//
	// Keying with a stable hash balancer pins every event sharing a key to a
	// single partition, which is what delivers ordering PER PARTITION KEY. An
	// empty key would let Kafka scatter the event round-robin and destroy that
	// ordering with nothing in the data to show it, so the construction path
	// guarantees a value through a documented fallback chain and this field is
	// never blank on a persisted row.
	//
	// # THE CONTRACT IS ONE RULE: an event is keyed on ITS LEDGER
	//
	// Requirement R-6 partitions by ledger id, and that is what this field
	// carries for every event whose ledger can be established at capture time —
	// which is every ledger-scoped event Blnk emits: transaction.* (the ledger
	// resolved from the loaded source balance, falling back to the destination),
	// balance.created and balance.monitor (the balance's ledger), and
	// ledger.created (the ledger itself). Every event of one ledger therefore
	// lands on ONE partition, which is the strongest ordering guarantee
	// available and is the one the requirement asks for.
	//
	// A ledger-scoped producer supplies the ledger explicitly through
	// blnk.WithEventLedgerID, because model.Transaction has no ledger field of
	// its own; the supplied value populates BOTH this field and LedgerID.
	//
	// # The fallback chain, and the events that genuinely have no ledger
	//
	// Four cases have no ledger to key on, and for them the chain applies: the
	// payload's own aggregate, then AggregateID, then the event type, then a
	// fixed sentinel. They are identity.created (an identity is not scoped to a
	// ledger), bulk_transaction.<status> (a batch is a runtime grouping, not a
	// ledger object), system.error (no aggregate of any kind, so it keys on the
	// event type and gets a total order), and a REJECTED transaction persisted
	// with no balances (no balance moved, so no ledger is named).
	//
	// It remains a SEPARATE FIELD FROM LedgerID even though the two carry the
	// same value on a ledger-scoped event: LedgerID answers "which ledger is
	// this about" and may legitimately be empty, whereas this field answers
	// "where does this message go" and never may. Conflating them is how an
	// empty ledger becomes an unkeyed, unordered message.
	//
	// Because one key groups every event of one ledger, the guarantee is stated
	// per partition key rather than per aggregate — the stronger reading.
	PartitionKey string `json:"partition_key"`

	// LedgerID is the AUTHORITATIVE ledger this event belongs to, or empty when
	// the event genuinely has no ledger. It takes no part in partitioning: it
	// answers "which ledger is this about", never "where does this message go".
	//
	// It is populated from a payload that carries a ledger identifier — a
	// ledger, or a balance, which belongs to exactly one ledger — or from a
	// ledger the PRODUCER supplies through blnk.WithEventLedgerID, which is how
	// the two event families whose payload cannot yield one are still recorded
	// against their ledger:
	//
	//   - Transactions. model.Transaction HAS NO LEDGER FIELD; a transaction's
	//     ledger association is indirect, through the balances it moves value
	//     between. Transaction execution has those balances loaded already, so
	//     it supplies the ledger and this field is populated for every
	//     transaction event EXCEPT a rejection, which is persisted with no
	//     balances at all and therefore names no ledger.
	//   - Balance monitors. BalanceMonitor carries no ledger field either, but
	//     the balance whose update triggered the check does, and the monitor
	//     call site holds it. Populated.
	//
	// The events that legitimately leave it empty, enumerated so that "empty" is
	// a documented fact rather than an omission:
	//
	//   - Identities. An identity is not scoped to a ledger in this model.
	//   - Bulk transaction batches. A batch is a runtime grouping, not a ledger
	//     object.
	//   - system.error. No aggregate of any kind.
	//   - A rejected transaction, for the reason above.
	//
	// Because it is nullable in the schema, an empty value here means "this
	// event has no ledger", never "we did not look".
	LedgerID string `json:"ledger_id,omitempty"`
	// Topic is the fully-resolved destination topic, recorded at insert time so
	// the relay never has to re-derive it and so a stored row remains
	// replayable to its original destination even if the topic-naming
	// configuration changes later.
	Topic string `json:"topic"`
	// SchemaVersion is the envelope version, copied to
	// LedgerEvent.SchemaVersion on publish.
	SchemaVersion int `json:"schema_version"`
	// Payload holds the legacy webhook body bytes verbatim, as json.RawMessage
	// so they are neither reordered nor renormalised between the insert and the
	// publish.
	//
	// The row stores them TWICE, in two columns with two jobs: payload_raw
	// (BYTEA) is the byte-exact copy and the only column ever projected back
	// into this field, while payload (JSONB) is a queryable projection for
	// containment and extraction queries — JSONB is parsed and re-rendered, so
	// it preserves the object's meaning and not its bytes. Both transports
	// during the dual-delivery window read these same bytes, which is what makes
	// their payloads identical structurally rather than by careful coding.
	Payload json.RawMessage `json:"payload"`

	// EventRaw is THE CANONICAL EVENT VALUE: the complete LedgerEvent envelope
	// bytes, exactly as they go onto the Kafka topic, produced once at capture by
	// LedgerEvent.CanonicalBytes and persisted in the event_raw BYTEA column.
	//
	// # Why the whole envelope is stored and not just the payload
	//
	// Requirement R-5 promises that a dead-lettered event replays byte-for-byte
	// aside from its failure metadata, and R-12 promises that both transports carry
	// identical bytes during the dual-delivery window. Payload alone cannot deliver
	// either: the five envelope members around it were re-serialised on every
	// publish, so byte identity held only for as long as the serialiser never
	// changed. Add a member, reorder one, or upgrade the encoder, and every row
	// already captured replays as different bytes — silently, because the two
	// values stay semantically equal. Storing the value the broker was given makes
	// the promise a property of the data instead of a property of the code's
	// stability across versions.
	//
	// # What reads it
	//
	// Everything that puts an event on a topic. The relay's publish, the
	// dead-letter write (which splices failure_metadata onto these bytes rather
	// than rebuilding the envelope), and replay (which republishes them unchanged)
	// all take this value. Nothing re-marshals from the columns.
	//
	// # It is NOT NULL in the schema, and it cannot be forgotten
	//
	// The repository derives it from the row's own envelope fields when a caller
	// leaves it empty — see prepareEventOutboxEntry — so a row cannot exist without
	// one, and a hand-built row in a test behaves like a production one. The publish
	// path keeps a documented fallback to composing the envelope for the same
	// reason: an empty value must degrade to correct behaviour rather than to no
	// behaviour.
	//
	// It is bytes rather than json.RawMessage because it is stored in a BYTEA column
	// and is never a member of another JSON document: it IS the document.
	EventRaw []byte `json:"event_raw,omitempty"`

	// OccurredAt is the instant the domain action happened. The relay claims
	// rows in ascending OccurredAt order, so FIFO holds within a partition key.
	OccurredAt time.Time `json:"occurred_at"`

	// CreatedAt is the instant this ROW became durable — when the capturing
	// transaction committed — as opposed to OccurredAt, which is when the domain
	// action happened. The two normally coincide and deliberately may not: a
	// backfilled or replayed mutation carries an earlier OccurredAt than its
	// CreatedAt.
	//
	// It is the START of the interval acceptance criterion V-1 is stated over,
	// "outbox-to-Kafka publish latency", and it is carried on the row for exactly
	// that reason. The relay's alternative — timing from the moment it claimed the
	// row — excludes the poll delay and the backlog, so a relay running an hour
	// behind reports the same sub-second latency as an idle one. It is never used
	// for ordering; OccurredAt owns that, and using this instead would publish a
	// backfilled event out of its domain order.
	//
	// It is zero only on a row assembled in Go that has not been read back from the
	// database, in which case the publisher falls back to the claim instant.
	CreatedAt time.Time `json:"created_at"`

	// --- Relay state machine ---
	// These fields track the row's progress from pending to dispatched, failed
	// or dead-lettered, and carry the bounded-retry bookkeeping.

	// Status is one of the EventOutboxStatus* values below.
	Status string `json:"status"`
	// Attempts counts publish attempts made so far.
	Attempts int `json:"attempts"`
	// MaxAttempts is the retry budget for this row, set explicitly by the
	// caller at construction from the configured relay attempt limit.
	MaxAttempts int `json:"max_attempts"`
	// NextAttemptAt is the instant this row is next DUE for a publish attempt.
	//
	// It is the DURABLE form of the configured exponential backoff. A retryable
	// failure records NOW() + the caller's computed delay here, and the claim
	// predicate is next_attempt_at <= NOW(), so a row inside its backoff window
	// is not claimed by any relay instance and nothing has to sleep to honour
	// the delay. Persisting it is what makes the schedule survive a restart:
	// without it a failed row is claimable again on the very next poll, so the
	// configured waits — 1s, 2s, 4s and 8s between the five attempts — collapse
	// into consecutive attempts against a broker that is already failing.
	//
	// It is NOT the lease. Lease expiry (locked_until) answers "has whoever
	// claimed this row abandoned it"; this answers "may anybody claim it yet",
	// and a row can be unleased and still not due.
	//
	// Never nil-valued: a freshly inserted row defaults to NOW() because a first
	// publish is not a retry and must not wait.
	NextAttemptAt time.Time `json:"next_attempt_at"`
	// LastError is the most recent publish failure reason, retained for
	// operator triage and copied into FailureMetadata.ErrorReason when the row
	// is finally dead-lettered.
	LastError string `json:"last_error,omitempty"`
	// FirstAttemptedAt is when the first publish attempt was made; nil until
	// the row is first claimed. It becomes
	// FailureMetadata.FirstAttemptedAt on dead-lettering.
	FirstAttemptedAt *time.Time `json:"first_attempted_at,omitempty"`
	// LastAttemptedAt is when the most recent publish attempt was made; nil
	// until the row is first claimed. It becomes
	// FailureMetadata.LastAttemptedAt on dead-lettering.
	LastAttemptedAt *time.Time `json:"last_attempted_at,omitempty"`
	// DispatchedAt is when the broker acknowledged the publish; nil until then.
	// It is set together with the dispatched status.
	DispatchedAt *time.Time `json:"dispatched_at,omitempty"`
	// LockedUntil is the lease expiry held by the relay instance that claimed
	// this row. Concurrent relay instances skip locked rows, and an expired
	// lease makes a row claimable again — which is what lets a crashed relay's
	// in-flight work be picked up rather than stranded.
	LockedUntil *time.Time `json:"locked_until,omitempty"`

	// ClaimToken identifies the CLAIM the row is currently under, and is what
	// makes every state transition safe against a worker whose lease expired.
	//
	// A fresh token is minted on each claim. Every transition — dispatched,
	// failed, dead-lettered, webhook-dispatched — then names the token it
	// believes it holds, and the UPDATE matches on it as well as on the row id
	// and the expected status. A relay that stalled past its lease, and whose
	// row has since been claimed and moved on by another instance, therefore
	// matches nothing and is told its claim was lost, instead of overwriting
	// newer state, double-incrementing the attempt count, or resurrecting a row
	// that is already terminal.
	//
	// Empty on a freshly inserted row and cleared when the row reaches a
	// terminal state, so a non-empty value means "some worker holds this row
	// right now".
	ClaimToken string `json:"claim_token,omitempty"`

	// --- Dual-delivery marker ---

	// WebhookDispatched records that the legacy HTTP webhook leg was dispatched
	// for this row.
	//
	// During the 30-day dual-delivery window the relay publishes to Kafka AND
	// enqueues the legacy webhook task FROM THE SAME CLAIMED ROW. Because both
	// transports read this row's Payload bytes, payload identity between them
	// is structural rather than procedural — they cannot drift apart. This flag
	// makes the legacy leg individually idempotent, so a row republished to
	// Kafka after a crash does not also re-enqueue a duplicate webhook. After
	// the sunset date the legacy leg is gone and the flag is simply never set.
	WebhookDispatched bool `json:"webhook_dispatched"`

	// KafkaDispatchedAt is when the broker acknowledged the Kafka publish,
	// recorded INDEPENDENTLY of DispatchedAt, and the field that keeps the two
	// legs' fates separate.
	//
	// DispatchedAt means "this row is finished". This means "the Kafka leg of
	// this row is finished", which during the dual-delivery window is a strictly
	// weaker statement because the legacy leg may still be owed. Collapsing the
	// two was a real defect and not a tidiness one: a row whose Kafka publish
	// succeeded and whose webhook enqueue failed was marked dispatched anyway,
	// and since the claim predicate excludes dispatched rows, the webhook was
	// never retried and never delivered.
	//
	// Its second job is to make a re-claim safe. A claimed row carrying a
	// non-nil value here has already been published, so the relay skips the
	// publish and delivers only the outstanding webhook — which is what stops
	// retrying a deprecated-transport failure from putting a duplicate on the
	// Kafka topic.
	//
	// Set on the ordinary success path too, so it answers "was this published,
	// and when" for every row rather than only for rows that took the unusual
	// path. Vestigial after the sunset, exactly like WebhookDispatched.
	KafkaDispatchedAt *time.Time `json:"kafka_dispatched_at,omitempty"`

	// WebhookAttempts counts legacy webhook ENQUEUE attempts made so far,
	// separately from Attempts.
	//
	// Separate because the two legs must not spend each other's budget: a
	// webhook receiver being down, or the queue being unreachable, must never
	// consume a Kafka retry attempt, because that would let the deprecated
	// transport dead-letter events on the new one. Bounded by this row's own
	// MaxAttempts so a permanently unreachable queue cannot keep the row
	// claimable indefinitely — once the budget is spent the Kafka delivery is
	// recorded as final, the legacy leg is abandoned, and the reason is written
	// to LastError. Vestigial after the sunset.
	WebhookAttempts int `json:"webhook_attempts"`

	// --- Broker coordinate (OBS-02) ---

	// KafkaTopic, KafkaPartition and KafkaOffset are WHERE THIS ROW'S RECORD
	// ACTUALLY LANDED: the coordinate the broker assigned to the write that made
	// this row's publication real. All three are nil until a write is
	// acknowledged, and they are set or cleared together.
	//
	// # Why the coordinate is stored rather than derived
	//
	// The zero-loss criterion (V-2) reconciles the outbox against the broker. Done
	// by COUNTING — records written against rows claiming a publication — it
	// cannot detect loss that duplicate surplus happens to offset: ten lost events
	// and ten redeliveries produce exactly the totals of a healthy system, and the
	// reconciliation reports no loss while ten events are genuinely missing.
	//
	// A stored coordinate replaces that arithmetic with a MAPPING. Every row that
	// claims a publication names the record it produced, the schema refuses two
	// rows the same coordinate, and a row claiming a publication with no
	// coordinate is visible as exactly that — unconfirmed — instead of being
	// absorbed into a surplus.
	//
	// The coordinate is also what makes the reconciliation BOUNDED rather than
	// global: it is checked against the measured [first, end) window of its own
	// partition, so retention, topic recreation and foreign traffic on a shared
	// topic each become a stated fact about specific rows instead of a distortion
	// of one total. See AuditEventRecordsInIntervals and ReconcileAgainstOutbox.
	//
	// # Which topic the coordinate belongs to
	//
	// KafkaTopic is recorded explicitly rather than inferred from Status, because
	// the destination differs by outcome: a dispatched row's record is on Topic
	// and a dead-lettered row's is on DLTTopic. Storing the name makes the
	// coordinate self-describing, which is what lets an operator paste it
	// straight into a console consumer.
	KafkaTopic string `json:"kafka_topic,omitempty"`
	// KafkaPartition is the partition the broker assigned. Nil when no write has
	// been acknowledged. A pointer rather than an int because partition 0 is a
	// perfectly ordinary partition, and a zero value would be indistinguishable
	// from "not recorded".
	KafkaPartition *int `json:"kafka_partition,omitempty"`
	// KafkaOffset is the offset within that partition. Nil when no write has been
	// acknowledged, and a pointer for the same reason as KafkaPartition: offset 0
	// is the first record on a fresh partition.
	KafkaOffset *int64 `json:"kafka_offset,omitempty"`

	// --- Dead-letter record ---

	// DLTTopic is the dead-letter topic the event was written to once its retry
	// budget was exhausted — the `<topic>.dlt` sibling of Topic. Empty until
	// the row is dead-lettered.
	DLTTopic string `json:"dlt_topic,omitempty"`
	// FailureMetadata stores the marshaled FailureMetadata struct declared
	// below. It is kept as raw JSON, not a decoded struct, so the stored bytes
	// are handed back to the dead-letter API exactly as they were written.
	FailureMetadata json.RawMessage `json:"failure_metadata,omitempty"`

	// Traceparent and Tracestate carry the W3C trace context of the request that CAPTURED
	// this event, so that publishing it can be correlated with the mutation that produced it.
	//
	// # Why they are stored rather than propagated
	//
	// The capture and the publish are decoupled by design: the request commits its
	// transaction and returns, and the relay claims the row up to a poll interval later,
	// possibly in a different process. There is no in-memory context to hand over, so a trace
	// that is not written on the row cannot be recovered from anything afterwards — which is
	// why every request and database trace used to END at the outbox insert, and publishing,
	// retrying, dead-lettering and replaying one event each produced spans in unrelated
	// traces.
	//
	// # They are a LINK, never a parent
	//
	// The publish span links to this context rather than being parented by it. The capturing
	// span has already ended, so parenting would attach a span to a completed trace and
	// stretch that trace's duration across the poll interval and every retry — reporting a
	// request that took milliseconds as one that took minutes. A link states "caused by"
	// without making the claim about duration, which is what the OpenTelemetry messaging
	// conventions specify for a producer decoupled in time from its trigger.
	//
	// # Empty is a legitimate, common state
	//
	// Both are empty for every event captured with no active trace: a CLI-driven mutation, a
	// worker-initiated rejection, or any deployment running with observability disabled. The
	// publish path then produces an ordinary unlinked span rather than treating the absence as
	// a fault.
	//
	// Both are BOUNDED at the persistence boundary — 255 and 512 characters, from the W3C
	// specification's own limits — because they originate in a caller-supplied HTTP header on
	// a table that takes one row per ledger mutation.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

// IsPurgeableByRetention reports whether the retention sweep is permitted to delete
// this row.
//
// # It must agree with the SQL exactly
//
// The authoritative predicate is the WHERE clause of PurgeTerminalEventsBefore and the
// partial index idx_event_outbox_purgeable that serves it. This method is the same rule
// expressed in Go so that a caller, a test and a runbook can evaluate eligibility
// without a database — and TestEventOutboxRetention_GoAndSQLAgreeOnEligibility pins the
// two together, because a divergence here would be silent and would either delete
// evidence or stop deleting anything.
//
// The rule, in words: ONLY A DISPATCHED ROW MAY BE DELETED BY AGE. It is a receipt for an
// event a subscriber has already had, so nothing is lost by removing it once it is old
// enough. Every other state is still owed something, and two of them are owed it until an
// operator acts:
//
//   - A DEAD-LETTERED row is the record of an event NO SUBSCRIBER EVER RECEIVED, together
//     with the failure metadata explaining why and the bytes a replay is driven from. It is
//     NEVER eligible, however old it is. The workflow that ends its life is REPLAY: a
//     re-publish the broker acknowledges makes the row dispatched, and a receipt is what age
//     may then remove.
//   - A FAILED row is worse still: its `<topic>.dlt` write has not landed, so the outbox row
//     is the only copy of the event in existence.
//
// Returns:
//   - bool: true when the row may be deleted by age.
func (e EventOutbox) IsPurgeableByRetention() bool {
	return e.Status == EventOutboxStatusDispatched
}

// DeadLetterInventoryFilter narrows the dead-letter inventory IN SQL.
//
// # Why the narrowing is pushed into SQL at all
//
// The filtered listing used to be served by walking the inventory in repository-sized pages
// and applying the predicates in Go, bounded at five thousand scanned rows. That bound made
// the endpoint's answer silently wrong in the one direction that matters: an operator
// filtering for a stuck event type received a short page with no indication that the scan had
// given up, and "nothing more is stuck" is the worst possible thing to tell someone triaging
// a loss. The warning it logged was in a place the operator was not looking.
//
// Pushing the predicates into SQL removes the failure mode rather than reporting it. The
// database applies them across the whole table, the page is taken from the FILTERED set, and
// there is no scan bound to reach because no rows are read and discarded in Go. It is also
// what makes a filter-aware COUNT possible, so a page can always be checked against a total.
//
// # It is an ALIAS of DeadLetterQuery, deliberately
//
// A filter over this inventory and a query over it are the same thing — the predicates, plus
// the occurrence window — and two types would be two places for one rule to be stated. The
// alias keeps the name that reads correctly at a filtering call site while there remains
// exactly one set of fields, one Filtered/IsEmpty answer, and one SQL rendering. Every field
// is an EXACT, case-sensitive match, which is how event types and topics are compared
// everywhere in this pipeline: producers emit fixed literals, and case-folding would make the
// filtered and unfiltered paths disagree for no benefit. The zero value selects the whole
// inventory.
type DeadLetterInventoryFilter = DeadLetterQuery

// InventoryEntry projects a full outbox row onto the dead-letter listing shape.
//
// It exists so the two places a dead-lettered event is rendered — the paged inventory,
// which the repository projects in SQL, and the single event a resolve or a fetch returns
// as a whole row — produce the SAME response body. Without it the resolve response would
// be assembled field by field at the handler, which is how one of the two ends up missing
// the partition key or reporting a payload size of zero.
//
// PayloadBytes is measured from the body actually held here, matching what the SQL
// projection measures with octet_length, so a caller cannot tell which path served it.
//
// Returns:
//   - DeadLetterInventoryEntry: the same row in the listing's narrow shape.
func (e EventOutbox) InventoryEntry() DeadLetterInventoryEntry {
	return DeadLetterInventoryEntry{
		ID:               e.ID,
		EventID:          e.EventID,
		EventType:        e.EventType,
		AggregateID:      e.AggregateID,
		PartitionKey:     e.PartitionKey,
		LedgerID:         e.LedgerID,
		Topic:            e.Topic,
		SchemaVersion:    e.SchemaVersion,
		OccurredAt:       e.OccurredAt,
		Status:           e.Status,
		Attempts:         e.Attempts,
		LastError:        e.LastError,
		FirstAttemptedAt: e.FirstAttemptedAt,
		LastAttemptedAt:  e.LastAttemptedAt,
		DLTTopic:         e.DLTTopic,
		FailureMetadata:  e.FailureMetadata,
		PayloadBytes:     len(e.Payload),
	}
}

// BrokerRecord is the coordinate of one record on one Kafka topic: the value that
// turns "this event was published" into "this event is THAT record".
//
// # OBS-02, and why a coordinate rather than a count
//
// The zero-loss criterion (V-2) used to be checked by counting: sum the topics' end
// offsets and compare against the number of outbox rows claiming a publication. That
// comparison is directional — records are a lower bound on events, because a
// redelivery or a replay writes a second record for one event — and its weakness is
// that the surplus is INDISTINGUISHABLE FROM COMPENSATED LOSS. Ten redeliveries and
// ten lost events produce exactly the totals of a healthy pipeline.
//
// A coordinate fixes that by making the relationship a mapping rather than a
// subtraction. Each row names the record it produced; the schema forbids two rows the
// same coordinate; and a row claiming a publication with no coordinate is reported as
// unconfirmed instead of quietly increasing the surplus.
//
// It is also the value an operator actually needs during triage: given
// "blnk.transactions/3@148291" they can read the exact record back with a console
// consumer, which is a materially different position from knowing only that the event
// was published at some point.
type BrokerRecord struct {
	// Topic is the topic the record is on. For a dispatched event this is the
	// category topic; for a dead-lettered one it is the `.dlt` sibling.
	Topic string

	// Partition is the partition the broker assigned, which for a keyed message is
	// determined by the partition key and is therefore stable across redeliveries of
	// the same event.
	Partition int

	// Offset is the record's position within that partition. It is assigned by the
	// broker and is unique per partition, so the three fields together identify one
	// record in the cluster.
	Offset int64
}

// Confirmed reports whether this coordinate actually names a record.
//
// A zero BrokerRecord is what an unacknowledged or unrecorded write leaves behind, and
// it must not be mistaken for "partition 0, offset 0" — which is a real and very
// ordinary location, being the first record on a fresh partition. The topic name is
// what distinguishes them: the broker always reports one, and nothing else sets it.
//
// Returns:
//   - bool: true when the coordinate names a record.
func (r BrokerRecord) Confirmed() bool {
	return strings.TrimSpace(r.Topic) != "" && r.Offset >= 0
}

// String renders the coordinate in the `topic/partition@offset` form used in logs, the
// dead-letter API and the operations runbook.
//
// An unconfirmed coordinate renders as a named absence rather than as
// "/0@0", because the latter reads as data and would send an operator looking for a
// record that was never written.
//
// Returns:
//   - string: the coordinate, or "unconfirmed".
func (r BrokerRecord) String() string {
	if !r.Confirmed() {
		return "unconfirmed"
	}

	return fmt.Sprintf("%s/%d@%d", r.Topic, r.Partition, r.Offset)
}

// BrokerRecord returns the coordinate this row recorded, and whether it has one.
//
// The three columns are written and cleared together, so a row is either fully
// confirmed or fully unconfirmed; this accessor states that invariant in one place
// rather than leaving every reader to test three pointers.
//
// Returns:
//   - BrokerRecord: the coordinate, zero-valued when the row has none.
//   - bool: whether the row names a record.
//
// CanonicalEvent rebuilds the LedgerEvent this row describes from its envelope columns.
//
// It exists for the ONE case that needs a struct rather than bytes: composing a publish
// request, whose result fields (event id, event type, topic) are read for metrics, logs and
// the API response. The bytes on the wire come from EventRaw, never from re-serialising what
// this returns — see CanonicalEventBytes.
//
// Returns:
//   - LedgerEvent: the envelope, with Payload sharing this row's payload bytes.
func (e EventOutbox) CanonicalEvent() LedgerEvent {
	return LedgerEvent{
		EventID:       e.EventID,
		EventType:     e.EventType,
		AggregateID:   e.AggregateID,
		OccurredAt:    e.OccurredAt,
		Payload:       e.Payload,
		SchemaVersion: e.SchemaVersion,
	}
}

// CanonicalEventBytes returns the STORED canonical envelope, or composes one when the row
// carries none.
//
// The stored value is preferred always, and that preference is the whole of requirement R-5's
// byte-fidelity guarantee: it is the value the broker was given, so replaying it cannot drift
// from the original however the serialiser changes afterwards.
//
// The fallback exists because degrading to correct behaviour beats degrading to none. A row
// with no stored envelope is one written before the column existed, or a value assembled in
// Go by a caller that only filled the envelope fields; composing from those fields yields the
// same bytes THIS version would have stored, which is right for both cases. The second return
// value reports which happened, so a caller that cares — the replay-fidelity assertion does —
// can tell a stored value from a reconstructed one.
//
// Returns:
//   - []byte: the canonical envelope bytes.
//   - bool: true when the bytes came from the row's stored value.
//   - error: only from composition, when the payload is not valid JSON.
func (e EventOutbox) CanonicalEventBytes() ([]byte, bool, error) {
	if len(bytes.TrimSpace(e.EventRaw)) > 0 {
		return e.EventRaw, true, nil
	}

	composed, err := e.CanonicalEvent().CanonicalBytes()
	if err != nil {
		return nil, false, err
	}

	return composed, false, nil
}

func (e EventOutbox) BrokerRecord() (BrokerRecord, bool) {
	if e.KafkaPartition == nil || e.KafkaOffset == nil || strings.TrimSpace(e.KafkaTopic) == "" {
		return BrokerRecord{}, false
	}

	record := BrokerRecord{
		Topic:     e.KafkaTopic,
		Partition: *e.KafkaPartition,
		Offset:    *e.KafkaOffset,
	}

	return record, record.Confirmed()
}

// DeadLetterFilter narrows the dead-letter inventory at the REPOSITORY, which is the only
// layer that can narrow it correctly.
//
// # Why this type exists
//
// The inventory listing used to accept these filters at the API and satisfy them by walking
// unfiltered repository pages in the service, applying the predicates in Go and stopping after
// a fixed 5,000 rows. Three things were wrong with that, and only the last is obvious:
//
//   - Matching entries beyond the scan bound were omitted from an operator-facing triage list
//     that returned HTTP 200 with no truncation marker, so "nothing else is stuck" and "I gave
//     up looking" were indistinguishable.
//   - No filter-aware COUNT existed, so a filtered page could not report a total at all and the
//     API refused `include_count` whenever a filter was set.
//   - Every filtered request read up to 5,000 rows out of the database to return at most 500.
//
// Pushing the predicates into SQL fixes all three at once: the page is exactly the requested
// slice of the matching set, the count is over the same predicate, and the index does the work.
//
// Every field is optional and an empty field means "no narrowing". The zero value therefore
// selects the whole inventory, which is what an unfiltered listing asks for.
type DeadLetterFilter = DeadLetterQuery

// Narrows reports whether the filter constrains anything at all.
//
// It exists so a caller can tell an unfiltered request from a filtered one without inspecting
// three fields and getting one of them wrong.
//
// Returns:
//   - bool: true when at least one field is set.
func (q DeadLetterQuery) Narrows() bool {
	return q.Filtered()
}

// PartitionOffsetInterval is one partition's MEASURED, currently-readable offset window:
// the half-open range [FirstOffset, EndOffset) the broker reported for it.
//
// # Why the zero-loss reconciliation needs an interval at all
//
// The reconciliation used to compare two numbers that describe different populations: the
// outbox's currently-retained rows against a topic's CUMULATIVE end offset. Those have no
// common baseline, no common time window and no common topic incarnation, so the comparison
// could be wrong in either direction while reporting itself as conclusive:
//
//   - Outbox retention prunes rows, so the left side shrinks while the right keeps climbing,
//     and the growing "surplus" hides real loss.
//   - Kafka retention deletes records the end offset still counts, so a record the sum
//     asserts exists may be unreadable.
//   - Recreating a topic resets its offsets to zero, so the right side collapses and the
//     comparison reports catastrophic loss — or, once rows are pruned too, reports nothing.
//   - Foreign or pre-existing traffic on a shared topic inflates the right side, masking
//     loss by exactly as much as it contributes.
//
// An interval fixes all four, because it turns the question from "do these two totals
// agree" into "is the record THIS ROW NAMED inside the window the broker can currently
// serve". That question is answerable per row, is unaffected by anything else on the topic,
// and yields a verdict whose scope is stated rather than assumed.
type PartitionOffsetInterval struct {
	// Topic is the fully-qualified topic name, exactly as a row's kafka_topic records it.
	Topic string

	// Partition is the partition ID.
	Partition int

	// FirstOffset is the earliest offset still retained. INCLUSIVE.
	FirstOffset int64

	// EndOffset is the log end offset: one past the last record written. EXCLUSIVE, which
	// is why a coordinate at or above it cannot be on the log at all.
	EndOffset int64
}

// Contains reports whether an offset lies inside the measured window.
//
// The bounds are asymmetric on purpose, matching Kafka's own semantics: FirstOffset is the
// earliest RETAINED record and EndOffset is one past the last WRITTEN one, so the window is
// [first, end).
//
// Parameters:
//   - offset int64: the coordinate's offset.
//
// Returns:
//   - bool: true when the offset is readable within this window.
func (i PartitionOffsetInterval) Contains(offset int64) bool {
	return offset >= i.FirstOffset && offset < i.EndOffset
}

// Records is how many records the window holds, never negative.
//
// An empty partition reports first equal to end, and a partition whose every record has
// aged out reports the same at a non-zero offset; both are legitimately zero rather than
// negative.
//
// Returns:
//   - int64: the number of readable records.
func (i PartitionOffsetInterval) Records() int64 {
	if i.EndOffset <= i.FirstOffset {
		return 0
	}

	return i.EndOffset - i.FirstOffset
}

// EventRecordIntervalAudit is the outbox side of the zero-loss reconciliation, classified
// AGAINST THE MEASURED BROKER WINDOWS rather than counted in aggregate.
//
// # Why "published" is not the same as "terminal"
//
// PublishedRows deliberately counts a wider set than the terminal statuses. A row in
// webhook_pending HAS been published to Kafka — its Kafka leg completed and
// KafkaDispatchedAt is stamped; what remains outstanding is the deprecated HTTP leg.
// Counting only dispatched and dead_lettered rows would leave those records unaccounted for
// on the broker side, inflating the apparent surplus and making the reconciliation looser
// precisely during the dual-delivery window, which is when it is most needed.
//
// # Every row lands in exactly one bucket, and the buckets are the verdict
//
// PublishedRows equals CorroboratedRows + UnconfirmedRows + UnmeasuredRows + AgedOutRows +
// BeyondEndRows, always. That identity is what makes the verdict a MAPPING instead of
// arithmetic: a green result means every single claim of publication was individually
// placed inside a window the broker can serve, so there is no aggregate for a surplus of
// redeliveries to hide a loss inside.
//
// Each non-corroborated bucket is a distinct operational fact, and collapsing any two of
// them would destroy the distinction an operator acts on:
//
//   - UnconfirmedRows — claims publication, names no record. Either the client returned no
//     coordinate or the row predates coordinate recording. Nothing corroborates it.
//   - UnmeasuredRows — names a topic or partition the measurement did not cover: a missing
//     topic, an unavailable partition, or a partition count that has since shrunk.
//   - AgedOutRows — names an offset BELOW the retained window. The record was written; Kafka
//     retention has since deleted it. Not loss, but no longer corroborable, and a consumer
//     that has not read it never will.
//   - BeyondEndRows — names an offset AT OR ABOVE the log end. This is impossible on an
//     intact log, because the broker assigned that offset when it accepted the write. It
//     means the partition was TRUNCATED or the topic was RECREATED, so the records are gone.
//     This is the topic-incarnation signal the whole interval design exists to surface.
type EventRecordIntervalAudit struct {
	// PublishedRows is how many rows claim a record on the broker: every row whose Kafka
	// leg completed, plus every dead-lettered row, each counted exactly once by virtue of
	// the unique index on event_id.
	PublishedRows int64

	// CorroboratedRows is how many of those name a record inside a measured window. It is
	// the only bucket a green verdict may contain.
	CorroboratedRows int64

	// DistinctCorroboratedRecords is how many DISTINCT coordinates the corroborated rows
	// name. It equals CorroboratedRows unless two rows claim the same record, which the
	// partial unique index on the coordinate makes impossible — so a discrepancy means that
	// index is missing or has been dropped, and the audit reports it rather than assuming
	// the schema is intact.
	DistinctCorroboratedRecords int64

	// UnconfirmedRows claim a publication without naming any record.
	UnconfirmedRows int64

	// UnmeasuredRows name a topic or partition the measurement did not cover.
	UnmeasuredRows int64

	// AgedOutRows name an offset below the retained window: written, then deleted by
	// retention.
	AgedOutRows int64

	// BeyondEndRows name an offset at or above the log end: evidence of truncation or topic
	// recreation.
	BeyondEndRows int64

	// OldestTerminalAt is the earliest publication instant among ALL terminal rows still
	// retained in the outbox, which is the floor of what any verdict can speak about.
	//
	// It is reported because the outbox is pruned: events published before this instant have
	// no row left to reconcile, so a verdict is a statement about [OldestTerminalAt, now]
	// and nothing earlier. Presenting a verdict without that bound is what let a
	// reconciliation over an aggressively pruned outbox look complete.
	OldestTerminalAt time.Time

	// CorroboratedFrom and CorroboratedTo bound the publication instants of the
	// CORROBORATED population: the window the green verdict actually covers.
	CorroboratedFrom time.Time
	CorroboratedTo   time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	//
	// # PERF-P05: the audit is WINDOWED, and it has to be
	//
	// The counts are one half of a comparison whose other half is a Kafka offset
	// reading, and the two halves must describe the SAME population or the comparison
	// means nothing. Over the whole history they cannot: broker end offsets are
	// cumulative for the life of a topic and are unaffected by Kafka retention, while
	// the outbox's retention sweep DELETES terminal rows — so the longer a deployment
	// runs with retention enabled, the further the outbox side falls behind a broker
	// side that never forgets, and the "surplus" the verdict tolerates grows without
	// bound until it can hide any amount of loss.
	//
	// A shared window fixes both directions at once: rows whose publication instant
	// falls inside it, against records the broker wrote inside it. Retention shorter
	// than the window is then the only thing that can invalidate the reading, and it is
	// detectable rather than silent — see the truncation flag on the offset report.
	//
	// It is also what makes the query bounded. At 500 events per second the table grows
	// by 43.2 million rows a day, so an exact whole-history aggregate is a scan whose
	// cost rises for ever while answering a question about the last day.
	//
	// The zero value means the counts are whole-history, which is a diagnostic reading
	// only: no reconciliation verdict may be drawn from it.
	WindowStart time.Time

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time
}

// EventTopicBacklog is how much work an outbox topic still owes, for ONE topic name as it
// is stored on the rows.
//
// # What it is for
//
// A row records its fully-resolved destination topic at insert time, so a deployment that
// changes KAFKA_TOPIC_PREFIX keeps rows naming the previous generation's topics. Those rows
// are only publishable while the previous prefix is declared in
// KAFKA_HISTORICAL_TOPIC_PREFIXES; if it is not, they are stranded — safe in the table, and
// carried by no transport. Nothing about that state is visible from a status count, because
// the rows look like ordinary pending and dead-lettered work.
//
// Grouping the undrained rows BY TOPIC is what makes it visible: a topic name outside every
// owned prefix is a stranded generation, and the count and the oldest instant say how much
// and how long. That is the audit behind the start-up warning that names the prefix an
// operator has to declare.
//
// # Why "undrained" rather than "non-terminal"
//
// Two kinds of row still owe a publish to the topic named here, and they must both be
// counted or the audit says a generation has drained when it has not:
//
//   - Rows that have never been dispatched — pending, processing, failed, replaying. The
//     relay owes each of them a publish to this exact topic.
//   - Rows that are dead_lettered. That state is terminal for delivery, but a dead-lettered
//     event is REPLAYABLE, and a replay publishes to the ORIGINAL topic. So a prefix with
//     dead-lettered rows under it is a prefix whose replays would be refused.
//
// A dispatched row owes nothing and is excluded. So is webhook_pending: its Kafka leg is
// complete and only the deprecated HTTP leg is outstanding, which does not involve a topic.
type EventTopicBacklog struct {
	// Topic is the destination as recorded on the rows, verbatim.
	Topic string

	// UndeliveredRows is how many rows still owe a first successful publish to Topic —
	// pending, processing, failed and replaying.
	UndeliveredRows int64

	// ReplayableRows is how many dead-lettered rows could be replayed to Topic.
	ReplayableRows int64

	// OldestOccurredAt is the occurrence instant of the oldest row counted here, which is
	// what turns "some rows are stranded" into "events from three days ago are stranded".
	OldestOccurredAt time.Time
}

// TotalRows is how many rows this topic still owes something for.
//
// Returns:
//   - int64: the sum of the two counts.
func (b EventTopicBacklog) TotalRows() int64 {
	return b.UndeliveredRows + b.ReplayableRows
}

// UnconfirmedRows is how many rows claim a publication they cannot name a record for.
//
// Returns:
//   - int64: never negative.
func (a EventRecordIntervalAudit) DuplicatedRecords() int64 {
	duplicated := a.CorroboratedRows - a.DistinctCorroboratedRecords
	if duplicated < 0 {
		return 0
	}

	return duplicated
}

// FullyCorroborated reports whether every row claiming a publication names a distinct
// record inside a measured window.
//
// It is the precondition for a conclusive zero-loss verdict, and it is deliberately
// stricter than the count-based predicate it replaces: a row whose record has aged out or
// whose partition was not measured no longer counts as confirmed, because nothing available
// today corroborates it.
//
// Returns:
//   - bool: true when nothing is uncorroborated and no two rows share a coordinate.
func (a EventRecordIntervalAudit) FullyCorroborated() bool {
	return a.UncorroboratedRows() == 0 && a.DuplicatedRecords() == 0
}

// ---------------------------------------------------------------------------------------
// PERF-P06/P07/P08: the dead-letter inventory, projected narrow and paged by key
//
// The inventory is what an operator triages a dead-letter backlog from, and it used to be
// served by reading WHOLE outbox rows — payload bytes and canonical envelope included, up
// to 768 KiB of each per row — through LIMIT/OFFSET, and then narrowing and filtering them
// in Go. Three costs followed from that, and none of them was visible in the response:
//
//   - A filtered page walked up to 5,000 of those rows to fill at most 100 small items, so
//     one triage request could move several gibibytes between PostgreSQL and the API for a
//     result measured in kilobytes — and returned a SUCCESSFUL but silently incomplete page
//     once the scan bound was reached.
//   - An OFFSET grows a page's cost with its depth: PostgreSQL reads and discards every
//     row before the offset, so the deepest page of a large inventory is the most expensive
//     one, and the depth was caller-supplied and unbounded.
//   - The age gauge sized the whole inventory with a COUNT and then read its oldest rows
//     from a deep tail offset, on every collection interval.
//
// The types below are what let all three be answered in SQL: a projection that names only
// the columns the triage view shows, a keyset cursor so a page's cost is independent of
// its depth, and a grouped age reading that never reads a row at all.
// ---------------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------------
// The legacy webhook URL policy — ONE definition, for every layer that records the column
// ---------------------------------------------------------------------------------------

// ValidateWebhookURL judges a legacy webhook URL against the single policy every layer that
// writes blnk.event_subscribers.webhook_url must apply.
//
// # Why it lives here
//
// The policy existed in TWO independent implementations — one in the repository, one in the
// request DTO — each with its own destination classifier and its own wording. One column, two
// rules, and the drift between them was not hypothetical: a service, CLI or migration caller
// reaching the repository directly was judged by a different standard from an HTTP caller, and a
// reason phrase differed between them for the same rejected host, so the same mistake read as two
// different problems depending on which door it came through.
//
// This file is deliberately dependency-free, and both callers already import it, so the policy can
// live in exactly one place without an import cycle. Each caller keeps its OWN error type — the
// repository answers with a typed apierror, the DTO with a plain validation error — because the
// two surfaces answer to different contracts; what they must not each own is the rule.
//
// # What the policy is
//
// An https URL, with a host, that is not visibly internal, carrying no surrounding whitespace.
// Each clause earns its place:
//
//   - HTTPS ONLY, because the payload is a ledger, identity or balance event and pushing it in
//     cleartext is a disclosure whatever the destination.
//   - NO SURROUNDING WHITESPACE, refused rather than trimmed, because the column is stored
//     VERBATIM: trimming for validation and storing the original would persist a destination that
//     never passed the check. It also keeps the repository and the DTO honest about one value.
//   - NOT AN INTERNAL DESTINATION, because a webhook URL is third-party input that Blnk itself
//     dials, which makes it a server-side request forgery vector straight at the cloud metadata
//     endpoint and at every service that trusts the network rather than the caller.
//
// An empty or all-whitespace value is ACCEPTABLE and means "clear the record": the column is
// nullable precisely so "no endpoint" and "this endpoint" stay distinguishable, and every caller
// maps nil/empty/value the same three ways.
//
// Parameters:
//   - raw string: the URL exactly as it will be stored. Not trimmed by this function.
//
// Returns:
//   - message string: a short caller-facing statement of what is wrong, echoing no caller input.
//     Empty when the URL is acceptable.
//   - reason string: why, for the log or the error cause. It may name the offending HOST, which is
//     the caller's own value and the one thing they need to see; it never echoes the whole URL or a
//     parser's rendering of it.
func ValidateWebhookURL(raw string) (message, reason string) {
	if strings.TrimSpace(raw) == "" {
		return "", ""
	}

	if raw != strings.TrimSpace(raw) {
		return "The webhook URL must not have surrounding whitespace",
			"a URL differing from another only by whitespace is a copy-paste artefact, and this " +
				"column is stored verbatim, so trimming it would persist a destination the caller " +
				"did not supply"
	}

	// THE PARSER'S ERROR IS NOT RETURNED. url.Parse quotes the input back, and the input is a
	// third party's endpoint that has no business in Blnk's error responses or logs.
	parsed, err := url.Parse(raw)
	if err != nil {
		return "The webhook URL is not a valid URL", "the value could not be parsed as a URL"
	}

	if parsed.Scheme != "https" {
		return "The webhook URL must use https",
			fmt.Sprintf(
				"scheme %q is not permitted; ledger and identity payloads must not be pushed in cleartext",
				parsed.Scheme,
			)
	}

	host := parsed.Hostname()
	if host == "" {
		return "The webhook URL must name a host", "the URL carries no host"
	}

	if internal := InternalWebhookDestinationReason(host); internal != "" {
		return "The webhook URL must not address an internal destination",
			fmt.Sprintf("host %q is refused: %s", host, internal)
	}

	return "", ""
}

// InternalWebhookDestinationReason reports why a host is an internal destination, or "" when it is
// not visibly internal.
//
// It returns a REASON rather than a boolean so a caller can say which rule was hit. "Not allowed"
// sends an operator looking for a policy document; "the range the cloud metadata service lives on"
// tells them what they just pointed Blnk at.
//
// Literal addresses are classified through net.IP and never as text, so every spelling is covered —
// IPv4, IPv6, and the IPv4-mapped IPv6 form "::ffff:127.0.0.1" that a denylist of strings always
// misses. A name with no dot is refused because it can only resolve through a search domain or a
// hosts entry, both of which are inside the deployment.
//
// Parameters:
//   - host string: the hostname or literal address from the URL, without a port.
//
// Returns:
//   - string: a short reason, or "" when the host is acceptable.
func InternalWebhookDestinationReason(host string) string {
	if address := net.ParseIP(host); address != nil {
		switch {
		case address.IsLoopback():
			return "it is a loopback address, which would make Blnk call itself"
		case address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast():
			return "it is a link-local address, the range the cloud metadata service lives on"
		case address.IsPrivate():
			return "it is a private address, which reaches services that trust the network rather than the caller"
		case address.IsUnspecified():
			return "it is the unspecified address"
		case address.IsInterfaceLocalMulticast(), address.IsMulticast():
			return "it is a multicast address"
		default:
			return ""
		}
	}

	lowered := strings.ToLower(host)
	switch {
	case lowered == "localhost", strings.HasSuffix(lowered, ".localhost"):
		return "it resolves to loopback"
	// .local is mDNS and .internal is the conventional private zone — metadata.google.internal is
	// one of the two best-known metadata endpoints.
	case strings.HasSuffix(lowered, ".local"), strings.HasSuffix(lowered, ".internal"):
		return "it is an internal-only name"
	case !strings.Contains(lowered, "."):
		return "it is unqualified, so it can only resolve inside this deployment"
	default:
		return ""
	}
}

// EffectivePartitionKey resolves the Kafka message key a row is ACTUALLY published under.
//
// # Why this is not simply the partition_key column
//
// Requirement R-6 partitions by ledger ID, so the publish path keys on the ledger when the row
// carries one and falls back to the stored partition key when it does not. partition_key is
// therefore the key for a ledger-less event — an identity, a bulk batch, a system error — and the
// SECOND choice for everything else.
//
// The two values agree on almost every row, because PrepareEventOutbox derives the partition key
// from the ledger when a ledger is known. They diverge on exactly the rows where the answer
// matters: one written before the ledger was threaded through, or one whose partition key was
// derived from the payload before the ledger was resolved. Reading the column alone reports the
// key those events were NOT routed by, and an operator answering "why are these two events out of
// order" from it reaches the wrong conclusion with nothing to indicate that they might have.
//
// It lives here, on the model, so that the publish path and every projection that REPORTS the key
// resolve it through one rule. Two copies of a fallback chain is how a response starts describing
// a routing decision the publisher did not make.
//
// Parameters:
//   - ledgerID string: the row's authoritative ledger, empty for a ledger-less event.
//   - partitionKey string: the row's stored partition key.
//
// Returns:
//   - string: the key the message is published under. Empty only when both inputs are blank,
//     which is an event belonging to no aggregate at all.
func EffectivePartitionKey(ledgerID, partitionKey string) string {
	if trimmed := strings.TrimSpace(ledgerID); trimmed != "" {
		return trimmed
	}

	return strings.TrimSpace(partitionKey)
}

// EffectiveKey is the Kafka message key this row is published under. See
// EffectivePartitionKey for why it is not simply the stored column.
//
// Returns:
//   - string: the ledger id when the row has one, otherwise the stored partition key.
func (e EventOutbox) EffectiveKey() string {
	return EffectivePartitionKey(e.LedgerID, e.PartitionKey)
}

// DeadLetterInventoryEntry is one row of the dead-letter inventory, projected to exactly
// what a triage listing shows.
//
// THE PAYLOAD IS NOT HERE, and its absence is the point. Neither the stored body nor the
// canonical envelope is projected — only PayloadBytes, the body's size, which is the one
// thing a listing legitimately reports about it. A caller that needs the bytes themselves
// is replaying the event, and replay reads the full row through its own claim so the bytes
// it publishes are the bytes that were stored.
//
// FailureMetadata IS projected, because it is small — five fields — and the projection the
// API builds fills gaps in the row's own columns from it.
type DeadLetterInventoryEntry struct {
	// ID is the surrogate key, carried so a keyset cursor can break ties on it.
	ID int64

	// EventID is the event's UUID and the subscriber's idempotency key. It is what a
	// replay is addressed by.
	EventID string

	// EventType is the event's type string, as published.
	EventType string

	// AggregateID is the entity the event is about.
	AggregateID string

	// PartitionKey is the STORED key the original publish used, so an ordering question
	// can be answered from the listing without re-deriving it.
	PartitionKey string

	// LedgerID is the ledger the event belongs to, when it has one.
	LedgerID string

	// Topic is the original category topic the event was destined for.
	Topic string

	// SchemaVersion is the envelope version the event was published under.
	SchemaVersion int

	// OccurredAt is when the event happened. It is the first component of the keyset
	// cursor and the ordering key of the listing.
	OccurredAt time.Time

	// Status is the row's terminal failure state: failed, or dead_lettered.
	Status string

	// Attempts is how many publish attempts were made.
	Attempts int

	// LastError is the raw failure text the last attempt recorded. It is CLASSIFIED
	// before it reaches a response body; nothing publishes it verbatim.
	LastError string

	// FirstAttemptedAt and LastAttemptedAt bound the retry window.
	FirstAttemptedAt *time.Time
	LastAttemptedAt  *time.Time

	// DLTTopic is the `<topic>.dlt` sibling the event was preserved on, empty while the
	// dead-letter write is still owed.
	DLTTopic string

	// FailureMetadata is the stored failure record, or nil when none was written.
	FailureMetadata json.RawMessage

	// PayloadBytes is the size of the stored body in bytes, measured in SQL so the bytes
	// themselves never leave the database.
	PayloadBytes int
}

// EffectiveKey is the Kafka message key this entry was published under, resolved through the
// same rule the publish path uses. See EffectivePartitionKey.
//
// It is what an ordering question has to be answered from. The stored partition_key alone is the
// SECOND choice on any row carrying a ledger, so a listing reporting it can name a key the event
// was not routed by — on precisely the rows where the two disagree, which are the rows an
// operator is investigating.
//
// Returns:
//   - string: the ledger id when the entry has one, otherwise the stored partition key.
func (e DeadLetterInventoryEntry) EffectiveKey() string {
	return EffectivePartitionKey(e.LedgerID, e.PartitionKey)
}

// DeadLetterCursor is a position in the dead-letter inventory, expressed as the ordering
// key of the last row a page returned rather than as a row count.
//
// The inventory is ordered by occurred_at descending with id descending as the tie-break,
// so a page resumes at "strictly older than this instant, or the same instant with a lower
// id". That predicate is index-backed, which is what makes every page cost the same — and
// it is also STABLE under concurrent writes: rows arriving while a caller pages do not
// shift the positions of the rows behind the cursor, whereas an OFFSET silently repeats or
// skips rows whenever the set changes beneath it.
type DeadLetterCursor struct {
	// OccurredAt is the occurrence instant of the last row returned.
	OccurredAt time.Time

	// ID is that row's surrogate key, which breaks ties between rows sharing an instant.
	ID int64
}

// eventCursorSeparator joins a cursor's two components. A colon cannot appear in either —
// one is a decimal nanosecond count, the other a decimal integer — so the split is
// unambiguous.
const eventCursorSeparator = ":"

// Encode renders the cursor as the opaque token an API hands back to a caller.
//
// It is base64url of "<unix-nanoseconds>:<id>". Opaque rather than structured on purpose:
// a caller that parses it is depending on the ordering key, which is an implementation
// detail of the listing and must stay changeable. Base64url — no padding — so the token is
// safe in a query string without escaping.
//
// Returns:
//   - string: the token, or "" for a zero cursor, which means "start at the beginning".
func (c DeadLetterCursor) Encode() string {
	if c.OccurredAt.IsZero() && c.ID == 0 {
		return ""
	}

	return base64.RawURLEncoding.EncodeToString([]byte(
		strconv.FormatInt(c.OccurredAt.UTC().UnixNano(), 10) + eventCursorSeparator +
			strconv.FormatInt(c.ID, 10),
	))
}

// ParseDeadLetterCursor decodes a token produced by Encode.
//
// Every malformed shape is rejected rather than coerced: a token that does not decode, does
// not split, or whose halves are not integers cannot be honoured, and defaulting it to the
// beginning of the inventory would silently restart a caller's pagination at page one — an
// infinite loop for any client that pages until it sees an empty page.
//
// Parameters:
//   - token string: the opaque cursor. Empty yields a nil cursor and no error, which means
//     "start at the beginning".
//
// Returns:
//   - *DeadLetterCursor: the decoded position, or nil for an empty token.
//   - error: ErrInvalidDeadLetterCursor for anything that does not decode.
func ParseDeadLetterCursor(token string) (*DeadLetterCursor, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	instant, identifier, found := strings.Cut(string(raw), eventCursorSeparator)
	if !found {
		return nil, ErrInvalidDeadLetterCursor
	}

	nanos, err := strconv.ParseInt(instant, 10, 64)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	id, err := strconv.ParseInt(identifier, 10, 64)
	if err != nil {
		return nil, ErrInvalidDeadLetterCursor
	}

	return &DeadLetterCursor{OccurredAt: time.Unix(0, nanos).UTC(), ID: id}, nil
}

// ErrInvalidDeadLetterCursor reports a cursor token that cannot be decoded. It is a
// sentinel so the API layer can answer it as a validation error naming the parameter
// rather than as an internal fault.
var ErrInvalidDeadLetterCursor = errors.New("model: the dead-letter cursor is not a token this inventory issued")

// DeadLetterInventoryQuery narrows and pages the dead-letter inventory.
//
// Every field is applied IN SQL. That is the whole difference from what this replaced: the
// filters used to be applied in Go over pages of whole rows, so narrowing a listing made it
// more expensive rather than less, and the result could be silently short.
type DeadLetterInventoryQuery struct {
	// Limit is the maximum number of entries to return. The repository defaults and caps
	// it, so a malformed request degrades to a cheap page.
	Limit int

	// EventType narrows to one event type exactly. Empty means every type.
	EventType string

	// Topic narrows to one ORIGINAL category topic. The `.dlt` spelling is resolved to
	// the original by the API before it reaches here, so this compares one value.
	Topic string

	// Status narrows to one terminal failure state. Empty means both — which is the
	// default, because a row that exhausted its retries but whose dead-letter write also
	// failed is the one an operator most needs to see.
	Status string

	// Cursor resumes a previous page. Nil starts at the newest entry.
	Cursor *DeadLetterCursor
	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant inclusively, and either
	// may be zero to leave that end unbounded.
	//
	// Triage is nearly always scoped to an incident — "what is stuck from the twenty minutes the
	// broker was down" — and the window is what makes that question expressible without paging
	// the whole inventory. occurred_at is the column the inventory is ordered and paged by, so
	// the window and the cursor agree about what "newest first" selects.
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// FilterQuery is this page's narrowing with the PAGE dropped: the same event type, topic,
// status and occurrence window, without the cursor or the limit.
//
// It exists so that a count accompanying a page is derived FROM the page's own query rather
// than assembled beside it. Two structures built independently from one request is how a filter
// gets applied to the page and not to its total, and a total describing a wider set than the
// page is not a harmless discrepancy on a triage endpoint — a paging client comparing the two
// never terminates, and an operator reads a backlog that is the wrong size.
//
// The page bounds are dropped rather than carried because which matches to return has no
// bearing on how many there are.
//
// Returns:
//   - DeadLetterQuery: the same narrowing, page-unbounded.
func (q DeadLetterInventoryQuery) FilterQuery() DeadLetterQuery {
	return DeadLetterQuery{
		EventType:    q.EventType,
		Topic:        q.Topic,
		Status:       q.Status,
		OccurredFrom: q.OccurredFrom,
		OccurredTo:   q.OccurredTo,
	}
}

// DeadLetterInventoryPage is one page of the inventory, plus what a caller needs to ask for
// the next one.
type DeadLetterInventoryPage struct {
	// Entries are the matching rows, newest occurrence first. Never nil on success.
	Entries []DeadLetterInventoryEntry

	// NextCursor is the position to resume from, nil when this page is the last one.
	//
	// It is derived from the last entry returned and is present ONLY when the repository
	// established that more rows match — it reads one row beyond the page to find out —
	// so a caller paging until NextCursor is nil terminates exactly once, without an extra
	// empty request and without ever stopping early.
	NextCursor *DeadLetterCursor

	// HasMore mirrors NextCursor != nil, so a response can report the fact without
	// exposing the token to a reader that does not need it.
	HasMore bool
}

// SubscriberCursor is a position in the subscriber registry, expressed as the ordering key
// of the last row a page returned.
//
// The registry is ordered by created_at descending with id descending as the tie-break, for
// the same reasons the dead-letter inventory is: created_at is stamped in Go, so two
// subscribers registered in the same microsecond would otherwise page in arbitrary relative
// order, and a cursor keyed on both is stable under concurrent registration where an offset
// is not.
type SubscriberCursor struct {
	// CreatedAt is the registration instant of the last row returned.
	CreatedAt time.Time

	// ID is that row's surrogate key.
	ID int64
}

// Encode renders the cursor as the opaque token an API hands back. See
// DeadLetterCursor.Encode for the format and for why it is opaque.
//
// Returns:
//   - string: the token, or "" for a zero cursor.
func (c SubscriberCursor) Encode() string {
	if c.CreatedAt.IsZero() && c.ID == 0 {
		return ""
	}

	return base64.RawURLEncoding.EncodeToString([]byte(
		strconv.FormatInt(c.CreatedAt.UTC().UnixNano(), 10) + eventCursorSeparator +
			strconv.FormatInt(c.ID, 10),
	))
}

// ParseSubscriberCursor decodes a token produced by SubscriberCursor.Encode.
//
// Parameters:
//   - token string: the opaque cursor. Empty yields a nil cursor and no error.
//
// Returns:
//   - *SubscriberCursor: the decoded position, or nil for an empty token.
//   - error: ErrInvalidSubscriberCursor for anything that does not decode.
func ParseSubscriberCursor(token string) (*SubscriberCursor, error) {
	position, err := ParseDeadLetterCursor(token)
	if err != nil {
		return nil, ErrInvalidSubscriberCursor
	}
	if position == nil {
		return nil, nil
	}

	return &SubscriberCursor{CreatedAt: position.OccurredAt, ID: position.ID}, nil
}

// ErrInvalidSubscriberCursor reports a subscriber cursor token that cannot be decoded.
var ErrInvalidSubscriberCursor = errors.New("model: the subscriber cursor is not a token this registry issued")

// SubscriberPageQuery pages the subscriber registry by key rather than by depth.
type SubscriberPageQuery struct {
	// Limit is the maximum number of subscribers to return. The repository defaults and
	// caps it.
	Limit int

	// Cursor resumes a previous page. Nil starts at the most recently registered
	// subscriber.
	Cursor *SubscriberCursor
}

// SubscriberPage is one page of the registry plus the position to resume from.
//
// It is what both the management API and the consumer-lag collector enumerate through — the
// API because a caller-supplied OFFSET made a page's cost its caller's choice, and the
// collector because it MUST be able to resume: reading from the top on every tick measured
// the newest subscribers over and over and never reached the rest of the registry at all
// (PERF-P22).
type SubscriberPage struct {
	// Subscribers are the rows, newest registration first. Never nil on success.
	Subscribers []EventSubscriber

	// NextCursor is the position to resume from, nil when this page is the last one.
	NextCursor *SubscriberCursor

	// HasMore mirrors NextCursor != nil.
	HasMore bool
}

// DeadLetterTopicAge is the oldest outstanding dead-letter entry on one topic, and how many
// are outstanding there.
//
// # PERF-P07: why this exists instead of a scan
//
// The dead-letter age gauge answers one question per topic — how old is the oldest thing
// still stuck — and it used to answer it by counting the whole inventory and then reading
// its oldest 5,000 rows, whole rows, from a deep tail offset, on every collection interval.
// That is the most expensive possible way to compute a MIN. The grouped aggregate below
// reads an index and returns one row per topic, so the cost of observing the backlog no
// longer grows with the backlog — which matters most precisely when the backlog is growing.
//
// It is also EXACT. The scan reported a lower bound once it hit its cap and warned about it;
// an alert on a lower bound cannot fire when the true value crosses the threshold and the
// bound does not.
type DeadLetterTopicAge struct {
	// Topic is the dead-letter topic the entries belong to, or the original topic's
	// `.dlt` sibling for a row whose dead-letter write has not happened yet.
	Topic string

	// Oldest is the age instant of the oldest outstanding entry on that topic.
	Oldest time.Time

	// Outstanding is how many entries the topic holds.
	Outstanding int64
}

// SubscriberRevocationBacklog is how much broker-side credential revocation is still
// OWED, and for how long the oldest debt has been outstanding.
//
// # What is being counted
//
// Deregistration revokes at the broker and only then deletes the registry row, so a row
// still carrying RevocationPendingAt is a principal that may still be able to
// authenticate and read while nothing in Blnk records an issuance for it. It is a durable
// to-do item rather than a lost one — a retried deregistration finishes the job — but
// until it is finished it is live access nobody is watching.
//
// # Why the two figures, and why the age is the one to alert on
//
// Pending answers "how much", which is what tells an operator whether they are looking at
// one stuck subscriber or a broker that has been unreachable for an hour. OldestPendingAt
// answers "for how long", and that is the alertable quantity: a marker cleared within a
// minute by a retry is routine, while one outstanding for an hour means the automatic
// settlement paths are not running and a human has to revoke by hand.
//
// The age is measured from when the obligation was FIRST recorded and is deliberately not
// reset by a later failed attempt, so it reports the age of the EXPOSURE rather than the
// age of the last try.
//
// # No per-subscriber breakdown
//
// Deliberately absent. Subscriber identifiers are unbounded in cardinality and would
// export a tenant identifier into every metric series and every alert notification. The
// question these two answer is how much is outstanding and for how long, not which; the
// rows themselves are the per-subscriber detail, listed through the registry.
type SubscriberRevocationBacklog struct {
	// Pending is how many subscribers carry an unsettled revocation marker. Zero is the
	// healthy steady state and is a meaningful reading rather than an absent one.
	Pending int64

	// OldestPendingAt is when the OLDEST outstanding marker was recorded. It is the zero
	// value when nothing is outstanding, which is why callers must test it rather than
	// subtracting blindly — an epoch-zero instant would otherwise render as an age of
	// fifty-odd years and pin every alert on it.
	OldestPendingAt time.Time
}

// OldestAge is how long the oldest outstanding revocation has been owed, measured
// against the supplied instant.
//
// It returns ZERO when nothing is outstanding, and zero rather than a negative value when
// the marker is stamped in the future — which spans two clocks by necessity, since the
// deregistering process stamps the marker and the collecting process reads it.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its own
//     clock and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (b SubscriberRevocationBacklog) OldestAge(now time.Time) time.Duration {
	if b.OldestPendingAt.IsZero() {
		return 0
	}

	age := now.Sub(b.OldestPendingAt)
	if age < 0 {
		return 0
	}

	return age
}

// SubscriberAccessResidue is how much broker-side access is UNACCOUNTED FOR: credentials
// that outlived their registry record, and revocations the broker refused.
//
// # Why these are separate from the revocation backlog
//
// SubscriberRevocationBacklog counts rows carrying RevocationPendingAt, which is stamped
// before the broker is touched. It therefore answers "how many deregistrations are
// unfinished". Neither figure here is inside that answer:
//
//   - AN ORPHANED CREDENTIAL is created by a FAILED ISSUANCE, not by a deregistration. The
//     issuance path never stamps RevocationPendingAt, so the revocation backlog and its
//     alert were structurally unable to see an orphan — the exposure existed and the only
//     representation of it was a log line.
//   - A FAILED REVOCATION is a strict subset of the pending rows, distinguished because a
//     pending row whose attempt actually FAILED needs the broker's authorization or
//     reachability fixed before any retry can work, while one that merely started needs
//     nothing but the retry. A single count cannot tell an operator which they have.
//
// # Why the ages, and why they are the alertable quantities
//
// The counts say how much; they are what tells an operator whether they are looking at one
// stuck subscriber or a broker that has been unreachable for an hour. The ages say for how
// long, and that is what a rule should fire on: a marker settled within a minute is
// routine, because both automatic settlement paths — re-issuing, which replaces an orphan
// by construction, and deprovisioning, which removes it — clear it as a side effect. One
// outstanding for an hour means neither has been exercised and a human must revoke by hand.
//
// # No per-subscriber breakdown
//
// Deliberately absent, for the reason SubscriberRevocationBacklog gives: subscriber
// identifiers are unbounded in cardinality and would export a tenant identifier into every
// metric series and every alert notification. The rows themselves are the per-subscriber
// detail, and they are now projected through the registry API so reading them does not
// require a psql session.
type SubscriberAccessResidue struct {
	// OrphanedCredentials is how many rows carry an unsettled CredentialOrphanedAt. Zero
	// is the healthy steady state and is a meaningful reading rather than an absent one.
	OrphanedCredentials int64

	// OldestOrphanedAt is when the OLDEST unsettled orphan was recorded, or the zero
	// instant when there is none. Callers must test it rather than subtracting blindly —
	// an epoch-zero instant renders as an age of fifty-odd years and would pin every
	// alert built on it.
	OldestOrphanedAt time.Time

	// FailedRevocations is how many rows carry an unsettled RevocationFailedAt.
	FailedRevocations int64

	// OldestFailedRevocationAt is when the OLDEST of those attempts failed, or the zero
	// instant when none has.
	OldestFailedRevocationAt time.Time
}

// OldestOrphanAge is how long the oldest orphaned credential has been outstanding.
//
// It returns ZERO when nothing is outstanding, and zero rather than a negative value when
// the marker is stamped in the future — which spans two clocks by necessity, since the
// issuing process stamps the marker and the collecting process reads it.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its own
//     clock and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (r SubscriberAccessResidue) OldestOrphanAge(now time.Time) time.Duration {
	return nonNegativeAgeSince(r.OldestOrphanedAt, now)
}

// OldestFailedRevocationAge is how long the oldest refused revocation has stood.
//
// Parameters:
//   - now time.Time: the instant to measure against.
//
// Returns:
//   - time.Duration: never negative, and zero when no attempt has failed.
func (r SubscriberAccessResidue) OldestFailedRevocationAge(now time.Time) time.Duration {
	return nonNegativeAgeSince(r.OldestFailedRevocationAt, now)
}

// Settled reports whether there is no unaccounted broker-side access at all.
//
// It is the reading an operator wants a single answer for, and it is deliberately a
// conjunction of both states rather than of the counts alone: either one being non-zero
// means something at the broker is not described by the registry.
//
// Returns:
//   - bool: true when both counts are zero.
func (r SubscriberAccessResidue) Settled() bool {
	return r.OrphanedCredentials == 0 && r.FailedRevocations == 0
}

// nonNegativeAgeSince measures an age from a possibly-zero instant.
//
// The zero instant answers zero rather than fifty-odd years, and an instant in the future
// answers zero rather than a negative duration. Both cases are real: a marker is absent
// far more often than present, and the process that stamps it is not the process that
// reads it, so their clocks can disagree by a little.
//
// Parameters:
//   - at time.Time: the instant the marker was recorded. The zero value means "absent".
//   - now time.Time: the instant to measure against.
//
// Returns:
//   - time.Duration: never negative.
func nonNegativeAgeSince(at, now time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}

	age := now.Sub(at)
	if age < 0 {
		return 0
	}

	return age
}

// FullyConfirmed reports whether every row claiming a publication names a distinct
// record.
//
// Outstanding answers "is anything owed", which is the figure to graph. The two component counts
// answer "which KIND", and that distinction changes what an operator does: a rising
// credential-cleanup count is a security matter — access that may exist beyond what is recorded —
// while a rising grant-reconciliation count is an availability matter, a subscriber whose access
// may be narrower or wider than intended.
//
// # The age is the alertable quantity
//
// OldestPendingAt is measured from when the obligation was FIRST recorded and is deliberately not
// reset by a later failed attempt, so it reports the age of the divergence rather than the age of
// the last try. An obligation settled within a minute is routine; one outstanding for an hour
// means the settlement pass is not running, or the broker has been unreachable throughout, and
// either needs a human.
//
// # No per-subscriber breakdown
//
// Deliberately absent, for the reason SubscriberRevocationBacklog gives: subscriber identifiers
// are unbounded in cardinality and would export a tenant identifier into every metric series and
// alert notification. The rows themselves are the per-subscriber detail, listed oldest first
// through the registry.
type SubscriberSettlementBacklog struct {
	// Outstanding is how many subscribers owe EITHER obligation. It is not the sum of the two
	// counts below, because one subscriber can owe both.
	Outstanding int64

	// GrantReconcilePending is how many owe a broker-side grant reconciliation.
	GrantReconcilePending int64

	// CredentialCleanupPending is how many owe a credential cleanup.
	CredentialCleanupPending int64

	// OldestPendingAt is when the oldest outstanding obligation of EITHER kind was recorded. It
	// is the zero value when nothing is outstanding, which is why callers must test it rather
	// than subtracting blindly — an epoch-zero instant would render as an age of fifty-odd years
	// and pin every alert on it.
	OldestPendingAt time.Time
}

// OldestAge is how long the oldest outstanding obligation has been owed, measured against the
// supplied instant.
//
// It returns ZERO when nothing is outstanding, and zero rather than a negative value when the
// marker is stamped in the future — which spans two clocks by necessity, since the operation that
// stamps the marker and the collector that reads it are different processes.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its own clock
//     and a test can be exact.
//
// Returns:
//   - time.Duration: never negative.
func (b SubscriberSettlementBacklog) OldestAge(now time.Time) time.Duration {
	if b.OldestPendingAt.IsZero() {
		return 0
	}

	age := now.Sub(b.OldestPendingAt)
	if age < 0 {
		return 0
	}

	return age
}

// EventOutboxPurgeTotals is what retention has removed from the event outbox over the
// table's whole life, and it is what lets the zero-loss reconciliation compare two
// quantities that describe the same interval.
//
// # Why a purged row has to be remembered
//
// A Kafka end offset counts every record ever appended to a partition and never
// decreases — not when the log segments age out, and certainly not when the outbox row
// that produced a record is deleted. The outbox side of the reconciliation counts rows
// that still exist. Those two quantities agree about what they are measuring only until
// retention deletes its first terminal row; afterwards the broker counts the whole of
// history and the outbox counts a suffix of it.
//
// That difference is invisible, because the comparison already EXPECTS a surplus:
// records are a lower bound on events, since a redelivery, a replay and a dead-letter
// copy each append a record no single row claims. So a purge-inflated surplus is
// indistinguishable from that expected overhead, and it can offset a genuine shortfall
// exactly — at which point the endpoint answers a confident "no loss detected" while
// events are missing, with nothing in the numbers to suggest otherwise.
//
// Adding these totals back yields an ALL-TIME terminal count, which is the quantity an
// all-time offset sum can honestly be compared against.
type EventOutboxPurgeTotals struct {
	// Recorded reports whether the totals could be read at all.
	//
	// It exists because "retention has removed nothing" and "we cannot tell what
	// retention has removed" are different statements, and only the first of them
	// supports a conclusive verdict. Both would otherwise present as zeroes, and the
	// zero value of this struct — Recorded false — is deliberately the unknown case so
	// that a caller which forgets to set it cannot accidentally claim knowledge.
	Recorded bool

	// RowsRemoved is how many terminal rows retention has deleted in total. It is added
	// to the surviving terminal rows to restore the all-time count.
	RowsRemoved int64

	// ConfirmedRemoved is how many of those rows carried a broker coordinate, and
	// therefore how many records the broker still counts have lost the row that named
	// them.
	//
	// It is tracked separately from RowsRemoved because the two restore different
	// baselines: RowsRemoved restores the terminal count that the offset sum is compared
	// against, while this restores the CONFIRMED count that the row-to-record mapping is
	// measured against. The mapping is the part of the verdict that carries its
	// soundness, so conflating the two would corrupt exactly the wrong number.
	ConfirmedRemoved int64

	// Batches is how many purge sweeps have recorded a deletion. Zero with Recorded true
	// means retention has never removed a terminal row, which is the one state in which
	// the arithmetic needs no correction at all.
	Batches int64

	// LastPurgedAt is when the most recent recorded batch committed, or nil if none has.
	LastPurgedAt *time.Time

	// NewestPurgedOccurrence is the latest occurrence instant among all purged rows, or
	// nil when unknown. It bounds the purged window from above, which is what an operator
	// needs in order to tell whether a measurement window overlaps a purge.
	NewestPurgedOccurrence *time.Time
}

// PurgeHasOccurred reports whether retention is known to have removed terminal rows.
//
// Returns:
//   - bool: true only when the log was read AND it records at least one deletion. An
//     unread log answers false, so callers must consult Recorded before treating that
//     as "nothing was purged".
func (t EventOutboxPurgeTotals) PurgeHasOccurred() bool {
	return t.Recorded && t.RowsRemoved > 0
}

// EventRecordCoordinate is what one outbox row claims about the broker: the exact record
// its publish produced.
//
// It is the unit the only sound part of the reconciliation is built from. Counting alone
// cannot distinguish a surplus of redeliveries from a surplus that is concealing an equal
// number of losses, whereas a coordinate can be CHECKED: a claimed offset at or beyond the
// partition's end offset names a record that does not exist, which is direct evidence
// rather than an inference from totals.
type EventRecordCoordinate struct {
	// Topic and Partition identify the log the claims belong to.
	Topic     string
	Partition int

	// Rows is how many terminal rows claim a record in this partition.
	Rows int64

	// MinOffset and MaxOffset bound the claimed offsets. They are compared against the
	// partition's live bounds: MaxOffset at or beyond the end offset means rows claim
	// records the log does not contain, and MinOffset below the first retained offset
	// means the oldest claims can no longer be verified because those records have aged
	// out.
	MinOffset int64
	MaxOffset int64
}

// EventRecordCoordinateAudit is every partition the outbox claims a record in.
//
// Rows without a coordinate are absent by construction: they are already counted as
// unconfirmed by EventOutboxAudit, and that count is what makes the verdict inconclusive
// while any exist.
type EventRecordCoordinateAudit struct {
	// Coordinates is one entry per (topic, partition) the outbox names, in topic then
	// partition order so a report reads deterministically.
	Coordinates []EventRecordCoordinate

	// MeasuredAt is when the outbox was read.
	MeasuredAt time.Time
}

// TotalRows sums the claims across every partition.
//
// Returns:
//   - int64: how many terminal rows name a broker record.
func (a EventRecordCoordinateAudit) TotalRows() int64 {
	var total int64
	for _, coordinate := range a.Coordinates {
		total += coordinate.Rows
	}

	return total
}

// HasFilters reports whether any narrowing was requested.
//
// Returns:
//   - bool: true when at least one of the three filters is set.
func (q DeadLetterQuery) HasFilters() bool {
	return q.EventType != "" || q.Topic != "" || q.Status != ""
}

// FailureMetadata is the diagnostic record appended to an event when its retry
// budget is exhausted and it is written to a dead-letter topic (requirement
// R-5). It answers the three questions an operator triaging a dead-lettered
// event always has: where was this event meant to go, why did it not get there,
// and over what window did we try.
//
// # Additive-attachment contract
//
// The failure metadata is attached to the dead-letter message as a SIBLING
// TOP-LEVEL KEY named "failure_metadata", alongside the LedgerEvent keys:
//
//	{
//	  "event_id": "...", "event_type": "...", "aggregate_id": "...",
//	  "occurred_at": "...", "payload": { ... }, "schema_version": 1,
//	  "failure_metadata": {
//	    "original_topic": "blnk.transactions",
//	    "error_reason": "write tcp ...: broken pipe",
//	    "attempt_count": 5,
//	    "first_attempted_at": "2024-05-01T12:34:56Z",
//	    "last_attempted_at":  "2024-05-01T12:35:27Z"
//	  }
//	}
//
// The attachment is strictly additive. It is NEVER nested inside payload, it
// NEVER replaces payload, and it NEVER mutates the original envelope: not one
// of the six LedgerEvent keys is rewritten, reordered or removed. That is
// precisely what leaves the original event bytes recoverable unchanged, so a
// replay can re-publish the STORED BYTES to the original topic — rather than
// re-marshalling from a struct, which would renormalise the JSON — and match the
// original byte-for-byte aside from the failure metadata.
//
// This struct is the metadata only. The combined dead-letter envelope that
// composes it with the LedgerEvent keys is owned by the dead-letter
// implementation in the root package, which also owns listing and replay.
type FailureMetadata struct {
	// OriginalTopic is the topic the event was originally destined for and the
	// topic a replay must send it back to.
	OriginalTopic string `json:"original_topic"`
	// ErrorReason is the failure reason from the final attempt, taken from
	// EventOutbox.LastError.
	ErrorReason string `json:"error_reason"`
	// AttemptCount is how many publish attempts were made before giving up.
	AttemptCount int `json:"attempt_count"`
	// FirstAttemptedAt is when the first publish attempt was made.
	FirstAttemptedAt time.Time `json:"first_attempted_at"`
	// LastAttemptedAt is when the final publish attempt was made. Together with
	// FirstAttemptedAt it bounds the window over which the failure persisted,
	// which is what distinguishes a momentary broker blip from a sustained
	// outage.
	LastAttemptedAt time.Time `json:"last_attempted_at"`
}

// PublishStatus is the outcome of ONE publish attempt, reported for
// observability. The publisher's result type carries it, and the metrics layer
// uses it verbatim as a metric attribute value — the publish-attempts counter is
// attributed by outcome. Declaring the vocabulary once, here, is what stops the
// code and the metric label set from silently drifting apart.
//
// # THREE VALUES, AND THE VOCABULARY IS FROZEN
//
// dispatched, retrying, dead_lettered. That is the publish-outcome contract the
// publisher abstraction is required to report, and it is closed: a fourth public
// value is a change to a contract subscribers' dashboards, the metrics reference and
// the alert rules are all written against.
//
// A fourth value, `failed`, was previously declared here to express "this attempt
// failed and nothing further will be tried". It has been removed. That statement is a
// PROPERTY OF THE ATTEMPT rather than a fourth kind of outcome, so it now travels as
// its own boolean on the publisher's result — see PublishResult.Terminal — and as its
// own `terminal` metric attribute beside the outcome. Nothing is lost by the change:
// "how many events are actually stuck" is still answerable, from
// outcome="retrying" narrowed by terminal="true", and the two facts are now
// independent instead of one collapsing the other.
//
// It is deliberately distinct from the EventOutboxStatus* values below:
// PublishStatus describes a single attempt and is never persisted, whereas the
// outbox status describes the row's durable state machine. An event can report
// PublishStatusRetrying several times while its row stays in the processing
// state.
//
// # THE VOCABULARY IS EXACTLY THREE VALUES, AND THAT IS A FROZEN CONTRACT
//
// Requirement R-3 names dispatched, retrying and dead-lettered, and no more. A
// fourth value, "failed", was added here to express "this attempt failed and
// nothing further will be tried"; it was removed because the status vocabulary is
// a PUBLISHED metric label domain, so widening it changes every recorded query and
// every dashboard selection an operator wrote against the documented set.
//
// The distinction that value carried is real and is still made — it just lives
// somewhere a metric label domain does not: PublishResult.Transient carries the
// classification, PublishResult.Classified says whether a classification was made
// at all, and PublishError.Transient carries the same verdict on the error itself.
// A caller asking "can another attempt succeed?" reads PermanentFailure(), not the
// status. Keeping the two apart is what lets the classification grow — a reason
// code, a retry-after hint — without touching a published label domain.
//
// So: a failed attempt reports PublishStatusRetrying whether or not a further
// attempt will actually be made, because "retrying" describes the attempt's place
// in the sequence rather than a decision the publisher took. Whether the sequence
// continues is the relay's call, made against the row's own budget in SQL, and
// PublishStatusDeadLettered is reported only once a broker has ACKNOWLEDGED a write
// to a `<topic>.dlt` sibling — never on the strength of a failed original publish.
type PublishStatus string

const (
	// PublishStatusDispatched means the broker acknowledged the write. With
	// RequiredAcks set to all in-sync replicas, this is a durable acknowledgement,
	// not merely a successful socket write.
	PublishStatusDispatched PublishStatus = "dispatched"
	// PublishStatusRetrying means the attempt FAILED. It is the single failure outcome,
	// and it covers both a failure another attempt may recover from and one nothing can:
	// whether anything further will be tried is reported separately, by
	// PublishResult.Terminal and by the `terminal` metric attribute, because that is a
	// property of the attempt rather than a different kind of outcome.
	//
	// Read the name as "this attempt did not deliver the event, and the row is still in
	// the relay's hands" rather than as a promise that a retry is scheduled — the retry
	// decision belongs to the relay and to the row's own budget in SQL, never to the
	// publisher.
	PublishStatusRetrying PublishStatus = "retrying"
	// PublishStatusDeadLettered means the event was written to its dead-letter topic
	// with FailureMetadata attached AND A BROKER ACKNOWLEDGED THAT WRITE.
	//
	// The acknowledgement is the whole condition. Reporting this on the strength of a
	// failed original publish claimed a preservation that had not happened — and for an
	// event whose destination lies outside the topic catalogue, never would — so the
	// counter operations reads to answer "how much are we dead-lettering?" counted
	// events that were nowhere at all. Only the dead-letter writer may set it.
	PublishStatusDeadLettered PublishStatus = "dead_lettered"
)

// AllPublishStatuses returns the complete per-attempt outcome vocabulary, in the
// order the metric documentation lists it.
//
// It exists so that the metrics layer, the load harness and the tests enumerate the
// label domain from one declaration rather than each rebuilding it — a fourth value
// appearing in one copy and not another is how a dashboard selection silently starts
// missing a population.
//
// Returns:
//   - []PublishStatus: a fresh slice; the caller may sort or filter it freely.
func AllPublishStatuses() []PublishStatus {
	return []PublishStatus{
		PublishStatusDispatched,
		PublishStatusRetrying,
		PublishStatusDeadLettered,
	}
}

// EventOutboxStatus* are the durable states of an EventOutbox row — the values
// stored in the blnk.event_outbox status column and the values the relay's claim
// query and partial indexes filter on.
//
// # State machine
//
//	pending ──claimed by a relay──▶ processing ──broker ack──▶ dispatched
//	   │                                │           │          (dispatched_at set)
//	   │                                │           └──legacy leg owed──▶ webhook_pending
//	   │                                │                                      │
//	   │                                │              (re-claimed; Kafka NOT republished)
//	   └──────retry budget exhausted────┴──▶ failed ──written to <topic>.dlt──▶ dead_lettered
//
// A row is inserted as pending by the database column default. A relay claims it
// — moving it to processing and taking a lease in locked_until — then publishes.
// A broker acknowledgement moves the row to dispatched and stamps dispatched_at.
// A failed attempt within budget returns the row to the claimable set so it is
// retried after a backoff; once the budget in max_attempts is spent the row
// becomes failed with dlt_topic still NULL, and once the event has additionally
// been written to its `<topic>.dlt` sibling it becomes dead_lettered. dispatched and dead_lettered
// are the terminal states; only a dead_lettered row is eligible for replay.
//
// webhook_pending is the one state that exists solely for the dual-delivery
// window: the Kafka leg is published and recorded in kafka_dispatched_at while
// the legacy HTTP leg is still owed. It is claimable — so the outstanding
// webhook is actually retried — and a row re-claimed out of it publishes NOTHING
// to Kafka, because its Kafka leg is already recorded. It disappears with the
// rest of the legacy transport at the sunset.
//
// # Relationship to the lineage outbox vocabulary
//
// These are event-specific constants, intentionally NOT the lineage outbox's
// OutboxStatus* constants, even though three of the five values coincide:
//
//   - pending and processing share their VALUES with OutboxStatusPending and
//     OutboxStatusProcessing, and failed shares its value with
//     OutboxStatusFailed. That value-level overlap is deliberate: it lets the
//     partial-index convention proven on blnk.lineage_outbox — a partial index on
//     pending rows, another on failed rows, and a composite claim index
//     restricted to pending and processing — carry over to blnk.event_outbox
//     unchanged.
//   - completed has no counterpart here; it is replaced by dispatched, which
//     names the same idea in the language of this pipeline and matches the
//     dispatched_at column.
//   - dead_lettered has no lineage equivalent at all. The event relay needs a
//     terminal state that distinguishes "we gave up and preserved the event on a
//     dead-letter topic, from which it can be replayed" from a plain failure,
//     and the lineage machine has no constant for it.
//
// The lineage declarations are NOT modified and are NOT reused by name, so the
// two state machines can evolve independently: blnk.lineage_outbox and
// blnk.event_outbox are separate tables served by separate relays, and they are
// not merged.
const (
	// EventOutboxStatusPending is the initial state, set by the column default
	// when the row is inserted alongside the ledger mutation.
	EventOutboxStatusPending = "pending"
	// EventOutboxStatusProcessing means a relay instance has claimed the row and
	// holds a lease on it in locked_until.
	EventOutboxStatusProcessing = "processing"
	// EventOutboxStatusWebhookPending means the Kafka leg is published and
	// recorded, and only the legacy HTTP webhook leg is still owed.
	//
	// It exists because the two legs of the dual-delivery window used to share
	// one terminal state, so a Kafka publish that succeeded marked the row
	// dispatched even when the webhook enqueue alongside it had failed — and
	// dispatched is outside the claim predicate, so that webhook was never
	// retried. This state is INSIDE the claimable set and outside the
	// key-blocking set: the outstanding webhook is picked up again, while later
	// events for the same aggregate keep flowing, because a row that is already
	// on its Kafka topic can no longer affect Kafka ordering.
	//
	// A row claimed out of this state carries a non-nil KafkaDispatchedAt, which
	// is what tells the relay to deliver only the webhook and publish nothing.
	//
	// SUNSET: removed with the rest of the legacy transport.
	EventOutboxStatusWebhookPending = "webhook_pending"
	// EventOutboxStatusDispatched is the success terminal state: the broker
	// acknowledged the publish, dispatched_at is set, and — during the
	// dual-delivery window — the legacy leg is either done or has been
	// deliberately abandoned after spending its own budget.
	EventOutboxStatusDispatched = "dispatched"
	// EventOutboxStatusFailed means the retry budget in max_attempts was
	// exhausted without a successful publish.
	//
	// IT IS WHAT THE RELAY'S EXHAUSTION ARM WRITES, and a failed row with dlt_topic
	// still NULL is precisely "the dead-letter write is owed". That pair — the status
	// and the coordinate's absence — is what ClaimFailedEventOutboxForDeadLetter
	// claims, so the hand-off is re-claimable after a worker dies or its dead-letter
	// write fails rather than the event being stranded in a state nothing selects
	// for. Once the write lands the row moves to EventOutboxStatusDeadLettered.
	//
	// It is NOT terminal and NOT purgeable: while a row sits here the event exists
	// only in this table.
	EventOutboxStatusFailed = "failed"
	// EventOutboxStatusDeadLettered is the failure terminal state: the event has
	// been written to its `<topic>.dlt` sibling with failure metadata attached,
	// and is now listable and replayable through the dead-letter API.
	EventOutboxStatusDeadLettered = "dead_lettered"
	// EventOutboxStatusReplaying means a replay has CLAIMED this dead-lettered
	// row and is re-publishing it to its original topic.
	//
	// It exists so that a replay is a claim rather than a read. Without it, two
	// concurrent replay requests for one event both read a dead_lettered row,
	// both pass the "is it dead-lettered?" check, and both publish — so an
	// operator clicking twice, or two operators triaging the same backlog, put
	// two copies of the event on the topic. The replay path instead transitions
	// dead_lettered → replaying conditionally, and only the transition that
	// actually changed a row goes on to publish. The row returns to
	// dead_lettered when the replay finishes or fails, so it stays replayable.
	EventOutboxStatusReplaying = "replaying"
)

// eventOutboxStatuses is the closed set of literals the status column accepts,
// declared once so the Go vocabulary and the schema's CHECK constraint cannot
// drift. A row carrying anything else is not merely untidy: the claim predicate,
// both partial indexes and every transition select on specific literals, so a row
// in an unrecognised state is an event nothing will ever look at again.
var eventOutboxStatuses = map[string]struct{}{
	EventOutboxStatusPending:        {},
	EventOutboxStatusProcessing:     {},
	EventOutboxStatusWebhookPending: {},
	EventOutboxStatusDispatched:     {},
	EventOutboxStatusFailed:         {},
	EventOutboxStatusDeadLettered:   {},
	EventOutboxStatusReplaying:      {},
}

// terminalEventOutboxStatuses is the subset of states from which no further
// delivery attempt is owed, and it is what a caller checks before concluding an
// event's delivery lifecycle is over.
//
// IT IS NOT THE RETENTION PREDICATE, and the distinction is the whole of SEC-08.
// Terminal means "Blnk will not try again". Purgeable means "the row may be
// destroyed", and for a dead-lettered row those are not the same claim: the event
// reached no subscriber, so the row is the only record that it went undelivered and
// the only thing a replay can be driven from. Deleting it on an age timer destroys
// that evidence unrecoverably. Retention therefore reads
// EventOutbox.IsPurgeableByRetention, which additionally requires an operator
// resolution on a dead-lettered row; this set answers the narrower question it is
// named for.
//
// failed is deliberately NOT terminal even though the retry budget is spent: the
// dead-letter write is still owed, and until it lands the event exists only in
// this table. replaying is not terminal for the obvious reason that a publish is in
// flight. webhook_pending is not terminal because a delivery is still owed on the
// legacy leg — treating such a row as finished would drop that webhook silently,
// which is the exact loss the state was introduced to stop.
var terminalEventOutboxStatuses = map[string]struct{}{
	EventOutboxStatusDispatched:   {},
	EventOutboxStatusDeadLettered: {},
}

// IsKnownEventOutboxStatus reports whether status is one of the
// EventOutboxStatus* literals.
//
// Parameters:
//   - status string: the status literal to test. Compared exactly; no trimming
//     or case folding is applied, because the column stores exact literals and
//     accepting a variant here would let one through that no query matches.
//
// Returns:
//   - bool: true when the literal is part of the state machine.
func IsKnownEventOutboxStatus(status string) bool {
	_, ok := eventOutboxStatuses[status]
	return ok
}

// IsTerminalEventOutboxStatus reports whether status is a state from which no
// further delivery attempt is owed.
//
// It does NOT answer whether the row may be deleted — a dead-lettered row is
// terminal and is not purgeable until an operator resolves it. Use
// EventOutbox.IsPurgeableByRetention for that question.
//
// Parameters:
//   - status string: the status literal to test.
//
// Returns:
//   - bool: true for dispatched and dead_lettered only.
func IsTerminalEventOutboxStatus(status string) bool {
	_, ok := terminalEventOutboxStatuses[status]
	return ok
}

// TerminalEventOutboxStatuses returns the terminal states, in state-machine
// order, as a fresh slice the caller may retain or reorder.
//
// It exists so a caller reasoning about which states are finished enumerates them
// from one authoritative place rather than restating the pair. It is NOT what the
// retention purge selects on: that predicate is narrower, because dead_lettered is
// terminal without being purgeable. See EventOutbox.IsPurgeableByRetention.
//
// Returns:
//   - []string: dispatched then dead_lettered.
func TerminalEventOutboxStatuses() []string {
	return []string{EventOutboxStatusDispatched, EventOutboxStatusDeadLettered}
}

// DeadLetterInventoryStatuses returns the two states the dead-letter inventory is
// composed of, as a fresh slice.
//
// 'failed' is in it alongside 'dead_lettered', and its presence is the more urgent
// half: a failed row has spent its retry budget and, while dlt_topic is still NULL,
// no copy of the event exists on any topic — the outbox row IS the event. An
// inventory that showed only dead_lettered would hide exactly the rows an operator
// most needs to see.
//
// It is declared here so the list query, the count query and the status filter's
// validation all read one list. Two of them agreeing and a third not is a filter
// that returns an empty page for a state the inventory actually holds, which reads
// to an operator as "nothing is stuck".
//
// Returns:
//   - []string: dead_lettered then failed.
func DeadLetterInventoryStatuses() []string {
	return []string{EventOutboxStatusDeadLettered, EventOutboxStatusFailed}
}

// DeadLetterQuery is the WHOLE narrowing a dead-letter inventory read may express, and
// it is the single contract the list and the count are both driven from.
//
// # Why one struct rather than a widening parameter list
//
// The inventory used to be read by `ListDeadLetteredEvents(ctx, limit, offset)` and
// filtered IN MEMORY afterwards, by paging the whole inventory and applying the
// predicates in Go up to a fixed scan ceiling. That produced a page that looked
// ordinary and was silently incomplete once the ceiling was reached, and it made an
// exact total impossible: the count came from a whole-table per-status aggregate that
// knew nothing about the event-type or topic filter, so `include_count` had to be
// refused for any filtered request. A paging client cannot detect either failure.
//
// Pushing the narrowing into SQL fixes both at once, and it has to be ONE type for the
// list and the count or the two can disagree about what they are describing — a total
// computed over a different predicate than the page is worse than no total at all.
//
// # The zero value is the whole inventory
//
// Every field is optional and an unset field narrows nothing, so `DeadLetterQuery{}`
// is "the whole inventory, default page". That is what lets the age scan and the
// operator listing share one method rather than justifying a second one.
type DeadLetterQuery struct {
	// EventType narrows to one event name, for example "transaction.applied".
	// Surrounding whitespace is ignored; empty narrows nothing.
	EventType string

	// Topic narrows to one ORIGINAL category topic, for example "blnk.transactions".
	// Filtering on the original topic rather than on the `.dlt` sibling is what makes
	// "show me the transaction events that are stuck" expressible without the caller
	// having to know the suffix convention.
	Topic string

	// Status narrows to one of DeadLetterInventoryStatuses. Anything else must be
	// refused by the caller rather than passed through: a status the inventory cannot
	// contain would match nothing, and an empty page reads as "nothing is stuck".
	Status string

	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant, inclusively,
	// and either may be zero to leave that end unbounded.
	//
	// occurred_at is the right column for a triage window rather than created_at or
	// last_attempted_at: it is the instant the ledger mutation happened, which is what
	// an operator correlating a dead-letter backlog against an incident timeline has.
	// It is also the column the inventory is ordered by, so a window and the paging
	// agree about what "newest first" means.
	OccurredFrom time.Time
	OccurredTo   time.Time

	// Limit is the maximum number of rows to return. Zero or negative selects the
	// repository default; anything above its maximum is clamped there.
	Limit int

	// Offset is how many MATCHING rows to skip. Negative is clamped to zero. Because
	// the narrowing is applied in SQL, this is an offset into the FILTERED set, which
	// is what makes a filtered page behave like a page of the filtered inventory
	// rather than a filtered page of the unfiltered one.
	Offset int
}

// Filtered reports whether the query narrows the inventory at all.
//
// It exists for observability rather than for control flow — there is one query path
// now, filtered or not — so a span or a log line can say whether a page was narrowed
// without restating the field list.
//
// Returns:
//   - bool: true when at least one narrowing field is set.
func (q DeadLetterQuery) Filtered() bool {
	return strings.TrimSpace(q.EventType) != "" ||
		strings.TrimSpace(q.Topic) != "" ||
		strings.TrimSpace(q.Status) != "" ||
		!q.OccurredFrom.IsZero() ||
		!q.OccurredTo.IsZero()
}

// IsEmpty reports whether the query narrows nothing, so a caller can tell a page of
// everything from a page of a subset without re-deriving the rule.
//
// The inverse of Filtered, named for the filtering call sites that read better in the
// negative. One rule, two spellings — not two rules.
//
// Returns:
//   - bool: true for the zero value.
func (q DeadLetterQuery) IsEmpty() bool {
	return !q.Filtered()
}

// EventOutboxStatuses returns EVERY state in the outbox state machine, in
// state-machine order, as a fresh slice the caller may retain or reorder.
//
// It exists so that a caller which must account for all of them — the statistics
// response behind the daily zero-loss reconciliation, and the test that proves
// that response is complete — enumerates them from this one authoritative place
// rather than restating the list. A status added here and not added there is
// exactly the failure the reconciliation cannot survive: the reported per-status
// counts then sum to less than the table's row count, and a short total is
// indistinguishable from a lost event.
//
// The order is the order a row moves through, not alphabetical, because that is
// how the runbook reads the counts.
//
// Returns:
//   - []string: pending, processing, dispatched, failed, dead_lettered, replaying.
func EventOutboxStatuses() []string {
	return []string{
		EventOutboxStatusPending,
		EventOutboxStatusProcessing,
		EventOutboxStatusWebhookPending,
		EventOutboxStatusDispatched,
		EventOutboxStatusFailed,
		EventOutboxStatusDeadLettered,
		EventOutboxStatusReplaying,
	}
}

// EventFailureOutcome is what a recorded publish failure decided, returned by
// Datasource.MarkEventFailed.
//
// # Why the decision is returned instead of re-read
//
// The retry-versus-exhaustion decision is made inside the UPDATE, in SQL, so that
// two relay instances racing on one row cannot both conclude they were the last
// attempt. Returning the decision preserves that atomicity for the caller. Deriving
// it instead from a fresh read of the row would reintroduce exactly the race the
// in-SQL decision exists to remove: between the update and the read, another
// instance could have moved the row again, and the caller would act on a state that
// is no longer current.
//
// The caller needs it because only the exhaustion arm hands off to the dead-letter
// path. A relay that cannot tell the two arms apart either dead-letters events that
// still have retries left, or never dead-letters at all and leaves them stranded in
// failed.
type EventFailureOutcome struct {
	// Status is the state the row is now in: EventOutboxStatusPending when another
	// attempt is owed, or EventOutboxStatusFailed when the budget is spent and the
	// dead-letter write is owed.
	Status string

	// Attempts is the attempt count AFTER this failure was recorded, which is the
	// number the dead-letter failure metadata reports.
	Attempts int

	// Exhausted is true when no further publish attempt will be made and the
	// dead-letter write is now owed — that is, when Status is
	// EventOutboxStatusFailed. It is derived from Status rather than computed
	// independently, so the two can never disagree.
	//
	// It becomes true for EITHER of two reasons, and the distinction is Terminal's:
	// the retry budget ran out, or the publisher reported a failure no retry can
	// fix.
	Exhausted bool

	// Terminal reports that the caller declared this failure PERMANENT — the
	// publisher's own verdict that no further attempt can succeed — rather than the
	// attempt count having run out.
	//
	// It exists so the two causes of exhaustion stay distinguishable in the log and
	// in the failure metadata. "attempt 1 of 5, exhausted" is otherwise indis-
	// tinguishable from a bookkeeping defect, when in fact it is the correct and
	// intended response to a message that can never be published: an oversized
	// envelope, bytes that are not valid JSON, or a destination outside the topic
	// namespace Blnk owns. Spending four more attempts to rediscover that would
	// delay preservation by the whole backoff schedule and report a permanently
	// stuck event as a busy one throughout.
	//
	// It is echoed back from what the caller passed rather than inferred, so a row
	// that exhausted on its budget alone reports false even when the last attempt
	// happened to be permanent.
	Terminal bool

	// ClaimToken is the token to present to MarkEventDeadLettered, and it is set
	// ONLY when Exhausted is true.
	//
	// The retry arm releases the claim so the row can be re-claimed, so there is no
	// token to carry. The exhaustion arm retains it, because the dead-letter write
	// and the transition that records it are still owed and only the worker that
	// spent the last attempt may perform them — which is what stops two workers both
	// writing the event to the dead-letter topic. It is echoed back here so the
	// caller does not have to remember which arm keeps it.
	ClaimToken string
}

// EventWebhookOutcome is what a recorded LEGACY WEBHOOK enqueue failure decided,
// returned by Datasource.MarkEventWebhookPending.
//
// SUNSET: this type, the transition that returns it and the state it reports go with
// the rest of the legacy transport.
//
// # Why the decision is returned instead of re-read
//
// For exactly the reason EventFailureOutcome is: the retry-versus-abandon decision is
// taken inside the UPDATE, so two relay instances working the same row cannot both
// conclude they spent the last webhook attempt. Deriving it from a fresh read would
// reintroduce that race.
//
// The caller needs it because the two arms differ in what the operator is told. The
// retry arm is routine and logs at debug: an enqueue failed, the row stays claimable,
// the webhook will be attempted again. The ABANDON arm is not routine — a webhook the
// system promised during the migration window is never going to be delivered — and it
// is logged at error with the event's identity, because it is the only notice anybody
// gets.
type EventWebhookOutcome struct {
	// Status is the state the row is now in: EventOutboxStatusWebhookPending when
	// another webhook attempt is owed, or EventOutboxStatusDispatched when the legacy
	// leg has been abandoned and the row is terminal on the strength of its Kafka
	// delivery alone.
	Status string

	// WebhookAttempts is the legacy enqueue count AFTER this failure was recorded.
	WebhookAttempts int

	// Abandoned is true when the legacy leg's budget is spent and no further webhook
	// attempt will be made. Derived from Status rather than computed independently, so
	// the two cannot disagree.
	Abandoned bool
}

// eventTopicDLTSuffix is the dead-letter suffix, duplicated from the root
// package's topic-naming layer for the structural check below ONLY.
//
// The root package remains the owner of topic NAMING: it holds the configured
// prefix and composes every name. This package cannot import it — the root
// package imports this one — and a structural namespace check is nonetheless
// needed at the persistence boundary, which sits below both. See
// IsBlnkEventTopic for why the same check is applied at two layers rather than one.
const eventTopicDLTSuffix = ".dlt"

// DefaultEventTopicPrefix is the topic namespace assumed when none is configured.
//
// It duplicates the root package's DefaultTopicPrefix and config's own Kafka default
// by value, because this package sits below both and cannot import either. The three
// are pinned equal by test. Duplicating the value is what lets the persistence layer
// validate a topic without a configuration dependency it has no business having.
const DefaultEventTopicPrefix = "blnk"

// IsBlnkEventTopic reports whether topic is a topic BLNK OWNS under the given prefix:
// exactly `<prefix>.<category>` or `<prefix>.<category>.dlt`, where category is one of
// the EventCategory* tokens.
//
// # What this defends against, and why the prefix is a parameter
//
// A stored row names the topic the relay will publish to. Without this check the relay
// would lazily create a writer for whatever name the row carried and PUBLISH TO A
// DESTINATION BLNK DOES NOT OWN, driven entirely by stored data — so a row that reached
// the table with the topic "attacker.transactions" would be delivered to the attacker's
// topic on the same broker, using Blnk's own producer credentials.
//
// The prefix must therefore be MATCHED EXACTLY, not merely be well formed. A check that
// accepted any prefix with a recognised category would accept "attacker.transactions",
// which has exactly the same shape as "blnk.transactions" and none of the same meaning.
// That is why the caller supplies the prefix rather than this function guessing: only
// the caller knows which namespace this deployment owns.
//
// # Where it is applied
//
// At the persistence boundary, so an unpublishable row cannot be stored, and again at
// publish, so a row stored under a prefix that has since been reconfigured is refused
// rather than sent somewhere unexpected. Neither check subsumes the other: the first
// stops the row existing, the second stops a stale row being acted on.
//
// The comparison is exact on both segments, and a name with surrounding whitespace is
// refused rather than trimmed, because Kafka would treat the untrimmed form as a
// different topic than the one that reads correctly here.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns. Trimmed; a blank prefix falls
//     back to DefaultEventTopicPrefix, which is the STRICTEST available answer rather
//     than a permissive one — an unconfigured caller must not accidentally widen the
//     namespace.
//
// Returns:
//   - bool: true when the name is a Blnk-owned category or dead-letter topic.
func IsBlnkEventTopic(topic, prefix string) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultEventTopicPrefix
	}

	// The dead-letter sibling of an owned category topic is itself owned, and
	// stripping the suffix first means the category check below is written once.
	base := strings.TrimSuffix(topic, eventTopicDLTSuffix)

	remainder, ok := strings.CutPrefix(base, prefix+".")
	if !ok || remainder == "" {
		return false
	}

	for _, known := range eventCategoryOrder {
		if remainder == known {
			return true
		}
	}

	return false
}

// IsBlnkEventTopicUnderAnyPrefix reports whether topic is a topic Blnk owns under ANY of
// the given prefixes.
//
// # Why more than one prefix is a real case
//
// An outbox row records its fully-resolved destination topic at INSERT time, so a
// deployment that changes KAFKA_TOPIC_PREFIX still holds rows naming the previous
// generation's topics. Those rows are committed events that must still be published, must
// still be able to dead-letter, and must still be replayable. A single-prefix test refuses
// every one of them after a restart, so nothing drains them — which is a delivery outage
// caused by a rename, with the events safe in the table and no transport willing to carry
// them.
//
// The set of prefixes is an EXPLICIT ALLOWLIST supplied by the caller — the configured
// prefix plus KAFKA_HISTORICAL_TOPIC_PREFIXES — and not a relaxation to "any prefix with a
// recognised category". The looser rule would admit "attacker.transactions", which has the
// same shape as a real name and none of the same meaning, and the callers of this predicate
// are the two places a stored string becomes either a persisted destination or an outbound
// connection carrying Blnk's own producer credentials. Only an operator can say which
// namespaces a deployment has actually owned.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefixes []string: the namespaces this deployment owns. Blank entries are skipped. An
//     empty or all-blank list falls back to DefaultEventTopicPrefix, matching
//     IsBlnkEventTopic's single-prefix behaviour, which is the STRICTEST available answer
//     rather than a permissive one.
//
// Returns:
//   - bool: true when the name is a Blnk-owned category or dead-letter topic under at least
//     one of the prefixes.
func IsBlnkEventTopicUnderAnyPrefix(topic string, prefixes []string) bool {
	matched := false
	for _, prefix := range prefixes {
		if strings.TrimSpace(prefix) == "" {
			continue
		}
		matched = true
		if IsBlnkEventTopic(topic, prefix) {
			return true
		}
	}

	if matched {
		return false
	}

	// No usable prefix was supplied. Delegating with a blank prefix resolves to
	// DefaultEventTopicPrefix inside IsBlnkEventTopic, so the two functions cannot disagree
	// about what an unconfigured caller owns.
	return IsBlnkEventTopic(topic, "")
}

// uuidCanonicalLength is the length of a canonical hyphenated UUID,
// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
const uuidCanonicalLength = 36

// IsCanonicalUUID reports whether s is a UUID in canonical hyphenated form.
//
// It is deliberately STRICTER than uuid.Parse, which also accepts the braced,
// URN-prefixed and unhyphenated spellings. Those alternatives matter here because
// event_id is the subscriber's idempotency key: two spellings of one UUID are two
// distinct keys to a consumer deduplicating on the string, and they are two
// distinct rows to the unique index that is supposed to make "one event is
// recorded once" an invariant. Admitting only one spelling is what keeps the key
// canonical end to end.
//
// The CASE check is part of that, and it is not pedantry. uuid.Parse accepts
// "3F2504E0-..." and "3f2504e0-..." as the same value, but the outbox stores and
// the unique index compares STRINGS: the two spellings are two rows for one event,
// and a subscriber deduplicating on event_id sees two keys. RFC 4122 §3 specifies
// lowercase on output, every id this package mints is lowercase, and admitting the
// other spelling would let a hand-supplied id defeat the one invariant the column
// exists to hold.
//
// Parameters:
//   - s string: the candidate identifier.
//
// Returns:
//   - bool: true only for the 36-character lowercase hyphenated form.
func IsCanonicalUUID(s string) bool {
	if len(s) != uuidCanonicalLength {
		return false
	}
	if s != strings.ToLower(s) {
		return false
	}
	if _, err := uuid.Parse(s); err != nil {
		return false
	}
	return true
}

// EventCategory* are BARE CATEGORY TOKENS, not topic names.
//
// They are the middle segment of a topic name and nothing more. The topic-naming
// layer in the root package composes `<prefix>.<category>` and
// `<prefix>.<category>.dlt` from them, applying the configured topic prefix
// (KAFKA_TOPIC_PREFIX, default "blnk"), and it is that layer — never this one —
// that owns the resulting names:
//
//	transactions → blnk.transactions → blnk.transactions.dlt
//	balances     → blnk.balances     → blnk.balances.dlt
//	identities   → blnk.identities   → blnk.identities.dlt
//	system       → blnk.system       → blnk.system.dlt         (internal)
//
// FOUR CATEGORIES, EIGHT TOPICS, AND THE LIST IS CLOSED. It is the agreed topic
// contract: subscribers, the provisioning script, the Kubernetes configuration and
// the local stack all enumerate exactly these names, so adding a fifth category here
// silently obliges every one of them to be changed too. A new category is a
// deliberate contract change, never an implementation detail, and
// TestEventCatalogue_CategorySetIsClosed fails the build if one appears without that
// change being made deliberately.
//
// Nothing in this file knows the prefix, builds a topic name, or appends the
// `.dlt` suffix. Treating a value returned from here as a topic is a bug.
//
// # Why there is one category beyond the three the requirements name
//
// Two event types that really are emitted belong to none of the three categories
// the requirements name: "ledger.created", raised by the post-ledger-creation
// actions, and "system.error", raised through the registered webhook-sender
// indirection when an internal error is notified. At the same time the coverage
// requirement is absolute — every event type that reaches the legacy webhook
// sender must be published, with zero exceptions.
//
// BOTH GO TO ONE EXTRA CATEGORY, `system`, following the identical naming
// convention so nothing about the scheme is special-cased. That is AMBIGUITY-2's
// resolution as the agreed plan states it — three named category topics plus
// blnk.system and its blnk.system.dlt sibling — and the plan is the frozen
// contract here, not a starting point. The alternatives it rejects are worse:
// forcing ledger events onto, say, the transactions topic corrupts that topic's
// semantics for every subscriber that filters on it, and dropping them violates the
// coverage requirement outright.
//
// A FIFTH `ledgers` CATEGORY WAS TRIED AND REMOVED. The argument for it was that
// `system` is internal, so routing `ledger.created` there leaves it unreachable by
// any subscriber credential — which is true. It was still the wrong change, for two
// reasons that outrank it. The topic catalogue is a PUBLISHED contract: subscribers,
// the provisioning script, the ACL allowlist, the local stack and the Kubernetes
// configuration all enumerate it, so a fifth subscriber-facing topic obliges every
// subscriber wanting universal coverage to hold an extra grant it was never told
// about. And the catalogue is frozen at four in the agreed plan, so widening it is a
// contract change that belongs to a deliberate revision of that plan rather than to
// this implementation. Making `ledger.created` consumable is therefore an
// ACCESS-MODEL question, and it is answered by granting the system category to the
// subscriber that needs ledger events rather than by minting a topic the plan does not
// name.
//
// `system` IS grantable, and granting it is a deliberate per-subscriber decision
// rather than a category-wide one. That matters most for `system.error`, whose payload
// is the frozen legacy body and therefore still carries the error text as it renders:
// narrowing it would break the payload-preservation guarantee, so the disclosure is
// contained by AUDIENCE — by which subscribers are granted the topic — instead of by
// redaction. See EventCategorySystem, which states what such a grant discloses. Coverage and reachability are different questions, and only the
// first is absolute — every event, `ledger.created` included, is durably captured,
// published, observable and replayable.
//
// # Where an event type this table does not recognise goes
//
// To the SYSTEM category, which is the catch-all as well as the home of
// system.error and ledger.created. An uncatalogued event is therefore published —
// the coverage guarantee is absolute and the row is already committed by the time
// routing happens, so it can be neither dropped nor refused — and it is published to
// the one topic whose audience is already the narrowest an operator grants.
//
// Anything arriving there under an unrecognised name is a defect to fix by
// extending EventCategory, not a state to design around, which is why the publisher
// says so at warning level when it routes one.
const (
	// EventCategoryTransactions covers all transaction lifecycle events,
	// including the runtime-composed bulk transaction events.
	EventCategoryTransactions = "transactions"
	// EventCategoryBalances covers balance creation and balance monitor alerts.
	EventCategoryBalances = "balances"
	// EventCategoryIdentities covers identity events.
	EventCategoryIdentities = "identities"
	// EventCategorySystem covers `ledger.created` and internal-error events, and it is
	// also the CATCH-ALL for an event type EventCategory does not recognise.
	//
	// It is the ONE category AMBIGUITY-2 adds to the requirement's three named ones.
	// Neither `ledger.created` nor `system.error` belongs to transactions, balances or
	// identities, and the coverage requirement admits no exceptions, so they need a
	// category rather than being forced into an unrelated one — which would corrupt
	// that topic's meaning for the subscribers filtering it — or dropped, which would
	// breach coverage outright.
	//
	// # `ledger.created` lives here, and a fifth `ledgers` category was removed
	//
	// The agreed plan's topic table places `ledger.created` on this topic, and the
	// catalogue is frozen at four categories. An implementation that gave ledger
	// events a subscriber-facing `blnk.ledgers` topic of their own was reverted: the
	// catalogue is published, so a fifth topic obliges every subscriber that wants
	// universal coverage to hold an additional grant, and widening a frozen contract
	// is a revision of the plan rather than an implementation detail.
	//
	// That does leave `ledger.created` unreachable by a subscriber credential, because
	// this category is internal. That is a real consequence and it is recorded rather
	// than hidden — in docs/event-streaming.md, where a subscriber reads it. It is an
	// ACCESS-MODEL decision (does the system category, or some subset of it, become
	// grantable?) and it is owned by the plan, in one place, instead of being answered
	// here by minting a topic.
	//
	// They share ONE category rather than getting one each. A separate grantable
	// `ledgers` category was tried and removed: it added a fifth category and two more
	// topics to a public contract the provisioning script, the Kubernetes
	// configuration, the local stack and the subscriber documentation all enumerate,
	// and it widened what a subscriber credential can reach beyond the agreed layout.
	// The reachability argument that motivated it is real and is answered in the
	// documentation instead — `ledger.created` is captured, published, observable and
	// replayable; what it does not have is a subscriber ACL.
	//
	// # `ledger.created` lives here, and that is the frozen contract
	//
	// Its payload is a *model.Ledger — a name, an id, a creation instant and the
	// caller's own metadata — and on its own merits it is ordinary ledger data. It
	// shares this category with `system.error` because the agreed catalogue is four
	// categories and eight topics, and a fifth category invented to separate them
	// would be an implementation quietly rewriting a contract that the provisioning
	// script, both compose stacks, the Kubernetes configuration and the operator
	// documentation all enumerate.
	//
	// The consequence is stated plainly rather than left to be discovered: because
	// this category is internal, `ledger.created` is captured, published, observable
	// and replayable like every other event, and it is NOT consumable by any
	// subscriber credential Blnk issues. Making the category grantable to recover that
	// reachability would hand the same subscribers the verbatim internal error text
	// `system.error` carries, which cannot be narrowed without breaking the
	// payload-preservation guarantee. Recovering it therefore requires a decision from
	// whoever owns the topic catalogue — a fifth grantable category, or a redaction
	// exception for the system payload — and not a change here.
	//
	// # It is GRANTABLE, and granting it is a DELIBERATE operator decision
	//
	// This category is not withheld at the allowlist, because withholding it would
	// withhold `ledger.created` — an event the legacy webhook transport delivers
	// today — from every subscriber, and the four-category layout is what put that
	// event here. A migration whose stated promise is that no event is lost cannot
	// make one of them unreachable as a side effect of a topic assignment. See
	// SubscriberGrantableEventCategories.
	//
	// What a grant of this category discloses is stated here so the decision is made
	// with the facts:
	//
	//   - system.error's payload is the FROZEN LEGACY BODY, {"error": <text>,
	//     "time": <now>}, and the <text> is the error as it renders. A PostgreSQL
	//     error names schema, table, column and routine; a broker error names
	//     internal addresses. That body cannot be narrowed without breaking the
	//     payload-preservation guarantee the whole migration rests on. So a
	//     subscriber granted this topic reads Blnk's own error text.
	//   - It is also the catalogue's CATCH-ALL. An uncatalogued event type is routed
	//     here, so a routing omission places a payload of unknown provenance on this
	//     topic. The publisher logs at warning level when it does, which is what
	//     makes the omission visible rather than silent.
	//
	// The consequence for an operator is a rule, not a prohibition: grant this
	// category only to a subscriber that needs `ledger.created`, and treat what else
	// arrives on it as operator-visible detail. Because authorized_topics is chosen
	// per subscriber, the narrow default is available to every deployment without the
	// allowlist having to deny the category outright — and denying it there would take
	// the choice away from the operator while quietly dropping an event.
	//
	// Coverage and reachability are both absolute here: every event, including this
	// one, is durably captured, published, observable and replayable, and every
	// category can be granted. docs/event-streaming.md states what this topic carries
	// so a subscriber planning its migration reads it rather than discovers it.
	//
	EventCategorySystem = "system"
)

// SubscriberGrantableEventCategories returns the categories a subscriber may be
// authorised to consume, in a stable order.
//
// It is the allowlist the subscriber DTO validation and the Kafka ACL provisioning
// both check against, so that "which topics may a subscriber be granted?" has one
// answer rather than one per caller.
//
// EVERY CATEGORY IS GRANTABLE. There is no internal category and no category-level
// exclusion: each of the four carries event types the legacy webhook transport
// already delivered, so withholding one would lose a migrating subscriber an event
// it receives today. What a grant of the system category discloses, and why the
// choice belongs to the operator rather than to this list, is documented on
// EventCategorySystem.
//
// The function remains distinct from AllEventCategories rather than being replaced by
// it, because the two answer different questions — "what categories exist" and "what
// may be granted" — and only one of them may ever narrow. A caller that means the
// second and asks the first would keep working today and silently over-grant the day
// an internal category is introduced.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, in canonical order.
func SubscriberGrantableEventCategories() []string {
	grantable := make([]string, 0, len(eventCategoryOrder))
	grantable = append(grantable, eventCategoryOrder[:]...)

	return grantable
}

// SubscriberGrantableTopics returns the fully-resolved topic names a subscriber may be
// authorised for under prefix, in the canonical category order.
//
// # One answer to "which topics may a subscriber be granted?"
//
// Three layers ask that question — the API request DTO, the persistence boundary and the
// Kafka ACL provisioner — and each of them refuses a grant that is not on this list. Three
// independent reconstructions of the same list is three chances for one of them to drift,
// and the failure mode of drift is a topic that one layer refuses and another grants. The
// list is therefore composed HERE, once, and the answer is identical everywhere by
// construction rather than by review.
//
// What the list EXCLUDES is the security-relevant part: DEAD-LETTER topics. A
// `<topic>.dlt` holds events that already failed, together with failure metadata naming
// broker addresses and internal error reasons. It is Blnk's operational surface, read under
// the master key through GET /events/dead-letter, and granting one to a subscriber would
// hand it every other subscriber's failed events. The list composes main topics only, so no
// `.dlt` name can appear on it.
//
// So the list is exactly `<prefix>.transactions`, `<prefix>.balances` and
// `<prefix>.identities`. Two of the thirteen migrated event types — `ledger.created` and
// `system.error` — consequently have no authorized subscriber path, which is a REAL
// limitation of the frozen four-category catalogue and is documented as one in
// docs/event-streaming.md rather than worked around here by minting a fifth topic.
// Coverage is unaffected: both are captured, published, observable and replayable, and
// both remain readable operationally under the master key.
//
// Parameters:
//   - prefix string: the namespace this deployment owns. Trimmed; a blank prefix falls back
//     to DefaultEventTopicPrefix, the strictest available answer, so that an unconfigured
//     caller cannot accidentally widen the namespace it grants over.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names, safe for the caller to retain
//     or sort.
func SubscriberGrantableTopics(prefix string) []string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultEventTopicPrefix
	}

	categories := SubscriberGrantableEventCategories()
	topics := make([]string, 0, len(categories))
	for _, category := range categories {
		topics = append(topics, prefix+"."+category)
	}

	return topics
}

// IsSubscriberGrantableTopicName reports whether topic is one a subscriber may be granted
// Read and Describe on under prefix.
//
// It is the membership test over SubscriberGrantableTopics, and it is EXACT: a name is
// compared byte-for-byte against the composed list rather than pattern-matched. That is what
// makes "*" a plain non-member instead of a special case somebody has to remember to write —
// Kafka reads the resource name "*" as matching every resource, so a wildcard reaching an
// ACL binding converts a per-topic grant into a cluster-wide one, and the safest handling is
// a membership test that no wildcard can satisfy.
//
// A name carrying surrounding whitespace is refused rather than trimmed, for the same reason
// IsBlnkEventTopic refuses one: Kafka would treat the untrimmed form as a different topic
// from the one that reads correctly here, so accepting it would grant a name nobody intended.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true only for an exact member of the grantable list.
func IsSubscriberGrantableTopicName(topic, prefix string) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	for _, grantable := range SubscriberGrantableTopics(prefix) {
		if topic == grantable {
			return true
		}
	}

	return false
}

// eventCategoryOrder is the canonical order every category listing uses, so that
// topic inventories, reports and log lines are deterministic.
//
// It lives here rather than in the topic-naming layer because this file owns the
// category vocabulary; the naming layer composes topic names from it.
// The three categories the requirement names come first, in the order it names them,
// and the fourth that AMBIGUITY-2 adds comes last, so a reader of any inventory —
// a topic listing, a reconciliation report, a log line — sees the requirement's own
// order first and the addition where it was added.
var eventCategoryOrder = [...]string{
	EventCategoryTransactions,
	EventCategoryBalances,
	EventCategoryIdentities,
	EventCategorySystem,
}

// AllEventCategories returns every category token, in canonical order.
//
// Returns:
//   - []string: a fresh slice; the caller may sort or filter it freely.
func AllEventCategories() []string {
	categories := make([]string, 0, len(eventCategoryOrder))
	categories = append(categories, eventCategoryOrder[:]...)

	return categories
}

// bulkTransactionEventPrefix is the fixed prefix of every bulk transaction event
// name. The full name is composed at runtime as this prefix plus the batch
// status, so the prefix — and never the whole string — is what can be matched.
const bulkTransactionEventPrefix = "bulk_transaction."

// eventTypeCategories is THE CATALOGUE: every event type Blnk emits under a fixed
// name, mapped to the category whose topic it is published to.
//
// It is a table rather than a switch so that EventCategory and IsCataloguedEventType
// read the SAME list. Two copies of this vocabulary would drift, and the drift is not
// cosmetic: an event type present in one copy and absent from the other is routed to a
// real category while being reported as unrecognised, or the reverse, so the metric
// label and the topic would disagree about the same event.
//
// The bulk transaction family is deliberately ABSENT. Its names are composed at
// runtime as bulkTransactionEventPrefix plus the batch status, so the suffix set is
// open and no table can enumerate it; both readers match it by prefix before
// consulting this map.
//
// "transaction.unknown" is a real, reachable member rather than a placeholder — see
// EventCategory for why the COMMIT status falls through to it.
var eventTypeCategories = map[string]string{
	"transaction.queued":    EventCategoryTransactions,
	"transaction.applied":   EventCategoryTransactions,
	"transaction.scheduled": EventCategoryTransactions,
	"transaction.inflight":  EventCategoryTransactions,
	"transaction.void":      EventCategoryTransactions,
	"transaction.rejected":  EventCategoryTransactions,
	"transaction.unknown":   EventCategoryTransactions,
	"balance.created":       EventCategoryBalances,
	"balance.monitor":       EventCategoryBalances,
	"identity.created":      EventCategoryIdentities,
	// ledger.created routes to the SYSTEM category, which is where the agreed plan's
	// topic table places it. That category IS grantable — see
	// SubscriberGrantableEventCategories, which returns all four — and ledger.created is
	// the reason it has to be: withholding the category would make a currently-delivered
	// event unreachable to every subscriber, turning a disclosure question into a lost
	// event. EventCategorySystem states what the grant discloses, and why the fifth
	// `ledgers` category that used to appear here was removed instead.
	"ledger.created": EventCategorySystem,
	"system.error":   EventCategorySystem,
}

// IsCataloguedEventType reports whether an event type is one this repository is known
// to emit.
//
// It answers a different question from EventCategory, and the difference is the whole
// point of it existing. EventCategory always returns a category, because every event
// must be published somewhere; this says whether that category was CHOSEN for the event
// type or merely inherited from the catch-all. Two callers need the distinction: the
// publisher, which warns when it routes an event type nobody catalogued, and the metrics
// layer, which must collapse uncatalogued names to one label rather than let an
// arbitrary string become a metric dimension.
//
// Parameters:
//   - eventType string: the event name, for example "transaction.applied".
//
// Returns:
//   - bool: true for a named member of the catalogue and for any
//     bulk_transaction.<status>.
func IsCataloguedEventType(eventType string) bool {
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return true
	}

	_, catalogued := eventTypeCategories[eventType]

	return catalogued
}

// EventCategory resolves an event type to its category token. It is THE single
// event-type-to-category mapping table in the repository: the topic-naming layer
// delegates to it rather than keeping a second table, because two tables that
// drift produce a routing bug that is invisible until a subscriber notices
// missing events.
//
// It returns a bare category token — "transactions", "balances", "identities" or
// "system" — never a topic name. Applying the configured prefix and the `.dlt`
// suffix is the caller's job.
//
// The mapping covers every event type Blnk emits:
//
//	transactions: transaction.queued, transaction.applied, transaction.scheduled,
//	              transaction.inflight, transaction.void, transaction.rejected,
//	              transaction.unknown, and any bulk_transaction.<status>
//	balances:     balance.created, balance.monitor
//	identities:   identity.created
//	system:       ledger.created, system.error
//
// "transaction.unknown" is a real, reachable value rather than a placeholder: the
// transaction status-to-event mapping has no case for the COMMIT status, which is
// genuinely assigned when an inflight transaction is committed, so it falls
// through to that event name. This is pre-existing behaviour that is preserved
// deliberately — changing it would make the dual-delivery payload-equivalence
// comparison fail for a reason unrelated to the transport. It is documented for
// correction as a separate, intentional change, and must not be "fixed" here.
//
// Comparison is plain and exact. The input is deliberately not lower-cased or
// otherwise normalised: producers pass the literals above, and normalising would
// add branches that then have to be mutation-tested for no behavioural benefit.
func EventCategory(eventType string) string {
	// Bulk transaction event names are composed at runtime as
	// bulkTransactionEventPrefix + status, so they must be matched by PREFIX and
	// never by exact equality — the suffix set is open. Three statuses are
	// emitted today ("failed", "inflight" and "applied"); any status added later
	// routes correctly with no change here.
	//
	// The prefix test runs first, ahead of the exact-match arms, so it cannot be
	// shadowed by them and is reached for every input.
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return EventCategoryTransactions
	}

	if category, catalogued := eventTypeCategories[eventType]; catalogued {
		return category
	}

	{
		// THE CATCH-ALL, and it is the system category.
		//
		// An unrecognised or empty event type is still routed rather than
		// rejected or dropped, which is what keeps the "zero exceptions"
		// coverage guarantee true for any event type introduced later: the row
		// is committed before routing happens, so refusing it here would strand
		// a durable event, and dropping it would lose one.
		//
		// Where it goes matters as much as that it goes somewhere, and the
		// system category is the narrowest destination available: it is the one
		// topic an operator grants deliberately rather than as a matter of
		// course, so a producer added without extending the table above does
		// not deliver its payload — a balance record, an identity record — to
		// the audience of a domain topic that never asked for it. The event
		// stays durable, observable and replayable, and the omission stays
		// visible: the publisher logs at warning level when it routes an event
		// type it does not recognise, and the fix is always to add the type
		// above rather than to design around this arm.
		return EventCategorySystem
	}
}

// credentialReferenceScheme is the fixed, self-describing prefix every credential
// reference carries.
//
// The scheme is part of the stored value rather than implied by the column, so a
// reference is recognisable on sight — in a log line, in an API response, in a psql
// session — and so that a future second derivation can be introduced without
// guessing which scheme an existing row used.
const credentialReferenceScheme = "scram-sha-512-ref-v1"

// credentialReferenceSeparator divides the scheme from the digest.
const credentialReferenceSeparator = "$"

// credentialReferenceDigestHexLen is the length of the hex-encoded digest: SHA-256
// produces 32 bytes, so 64 hex characters.
const credentialReferenceDigestHexLen = 64

// CredentialFingerprintLen is how many leading digest characters a fingerprint
// shows. Twelve hex characters is 48 bits — ample for a human to tell two
// issuances apart in a log or a response, and far too little to attack the digest
// with.
const CredentialFingerprintLen = 12

// ErrInvalidCredentialReference reports a value that is not a credential reference
// this package produced.
//
// It exists so the repository layer can refuse such a value rather than storing it.
// The column is TEXT and would accept anything, INCLUDING A PLAINTEXT PASSWORD
// handed over by a caller who misunderstood the field — and once a secret has been
// written to a column that was documented as never holding one, removing it is an
// incident rather than a migration. Validating the format is what makes the
// "no secret is stored here" property structural instead of a convention.
var ErrInvalidCredentialReference = errors.New(
	"model: value is not a credential reference derived by DeriveCredentialReference",
)

// DeriveCredentialReference produces the NON-REVERSIBLE reference persisted for an
// issued SASL credential.
//
// # What it is for
//
// The registry has to be able to answer "which credential does this row correspond
// to?" — to correlate an issuance with broker state, and to tell a reissue apart
// from the credential it replaced — without being able to answer "what is the
// secret?". A digest over the principal and the secret gives exactly that: the
// same pair always derives the same reference, a different pair derives a different
// one, and the secret is not recoverable from it.
//
// # Why HMAC-SHA-256 and not bcrypt
//
// This value is never used to VERIFY a presented secret. Nothing authenticates
// against it; the broker holds the SCRAM verifier and does that job. So the
// deliberate slowness bcrypt buys — which api_keys.key needs, because an API key IS
// verified against its hash on the request path — buys nothing here, and would make
// the 5-second credential-issuance budget carry a cost for no security benefit.
// What IS required is that the reference be stable (so a reissue can be detected)
// and non-reversible, and a keyed digest gives both. The principal is mixed in as
// the HMAC key so that two subscribers who happen to be issued the same generated
// secret still derive different references.
//
// # The HMAC key is PUBLIC, deliberately, and what that does and does not buy
//
// The key is the Kafka principal, which is derived from the subscriber's identifier
// and appears in API responses, broker ACL listings and logs. So this is a keyed
// digest whose key is known — it is domain separation, not secrecy, and it is worth
// stating plainly because a reader who assumes otherwise would over-trust the value.
//
// What the public key buys is exactly the property named above: two principals
// issued the same secret derive different references, so a reference collision
// cannot be read as "these two subscribers hold the same credential". What it does
// NOT buy is resistance to an offline guessing attack by someone holding the
// database — and that resistance comes instead from the secret itself.
// generateSubscriberPassword draws 48 characters uniformly from a 62-symbol
// alphabet through the system CSPRNG, which is roughly 285 bits of entropy: a
// salted, iterated KDF, or an additional server-side pepper, would multiply an
// already unreachable search space and would add a real failure mode — a
// configuration value whose loss or rotation changes every future derivation. The
// reference is also never returned to a client; only CredentialFingerprint's
// twelve-character fragment is.
//
// This is therefore a considered choice rather than an omission. If the posture ever
// has to change — because the secret's generation weakens, or because a compliance
// regime requires a KDF regardless of the arithmetic — the scheme prefix on the
// stored value is what makes that a versioned migration rather than a guess: see
// credentialReferenceScheme, whose "-v1" exists for this.
//
// # What the caller must do
//
// Call this at the moment of issuance, persist ONLY the result, and hand the secret
// itself to the subscriber exactly once. Never persist, log or return the secret,
// and never construct a reference by hand.
//
// Parameters:
//   - principal string: the Kafka principal the credential belongs to. Required.
//   - secret string: the generated SASL secret. Required. Consumed here and not
//     retained; it never appears in the result or in any error.
//
// Returns:
//   - string: the reference, in the form "scram-sha-512-ref-v1$<64 hex chars>".
//   - error: when either input is empty, which would derive a reference that
//     several rows could share.
func DeriveCredentialReference(principal, secret string) (string, error) {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return "", errors.New("model: deriving a credential reference requires a principal")
	}
	if secret == "" {
		return "", errors.New("model: deriving a credential reference requires a secret")
	}

	mac := hmac.New(sha256.New, []byte(principal))
	// hash.Hash.Write is documented never to return an error, so there is nothing to
	// check here; the linter's errcheck is satisfied by the explicit discard.
	_, _ = mac.Write([]byte(secret))

	return credentialReferenceScheme + credentialReferenceSeparator +
		hex.EncodeToString(mac.Sum(nil)), nil
}

// ValidateCredentialReference reports whether a value has the exact shape
// DeriveCredentialReference produces.
//
// The check is strict — exact scheme, exact separator, exact digest length, lower
// case hex only — because its whole purpose is to refuse anything that is not a
// derived reference. A lenient check would let the very thing it exists to prevent
// through: a plaintext secret, or a truncated digest, stored in the reference
// column.
//
// Parameters:
//   - reference string: the candidate value.
//
// Returns:
//   - error: nil when the value is a well-formed reference, otherwise
//     ErrInvalidCredentialReference.
func ValidateCredentialReference(reference string) error {
	scheme, digest, found := strings.Cut(reference, credentialReferenceSeparator)
	if !found || scheme != credentialReferenceScheme || len(digest) != credentialReferenceDigestHexLen {
		return ErrInvalidCredentialReference
	}

	// hex.DecodeString accepts upper case, which DeriveCredentialReference never
	// produces, so the case is checked as well as the alphabet. Two references over
	// one credential differing only in case would defeat the equality comparison the
	// reference exists to support.
	if strings.ToLower(digest) != digest {
		return ErrInvalidCredentialReference
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ErrInvalidCredentialReference
	}

	return nil
}

// CredentialFingerprint reduces a credential reference to the short, non-sensitive
// form that is safe to put in an API response or a log line.
//
// The full reference is not a secret, but it is not information a client needs
// either: it is internal correlation state, and returning it invites a caller to
// treat it as an identifier to send back, or to compare against something. A
// fingerprint carries the only thing a client legitimately wants — "is this the same
// issuance I saw last time?" — and nothing else.
//
// Parameters:
//   - reference string: a reference as produced by DeriveCredentialReference. Any
//     value failing validation yields the empty string rather than a partial
//     rendering of itself, so a mis-stored value cannot leak through this function.
//
// Returns:
//   - string: the first CredentialFingerprintLen characters of the digest, or "".
func CredentialFingerprint(reference string) string {
	if ValidateCredentialReference(reference) != nil {
		return ""
	}

	_, digest, _ := strings.Cut(reference, credentialReferenceSeparator)

	return digest[:CredentialFingerprintLen]
}

// LogIdentifierHashLength is how many hex characters of a SHA-256 digest an identifier
// pseudonym keeps.
//
// Sixteen hex characters is 64 bits: ample to keep every identifier in one deployment
// distinguishable, short enough to read off a dashboard and to grep for, and not reversible.
const LogIdentifierHashLength = 16

// HashIdentifier is THE canonical pseudonym rule for an identifier that must be correlated
// without being disclosed.
//
// # Why it lives here rather than in the package that first needed it
//
// The pseudonym has to be a PIVOT ACROSS THREE LAYERS to be worth anything. A metric label
// carries it, a log line carries it, and the subscriber resource publishes it as
// subscriber_id_hash so an operator holding a token from an alert can resolve it to a
// subscriber. Those are the root package, the database package and the api/model package —
// three packages, and the ONLY one all three already import is this one.
//
// A second implementation in any of them would be a second thing to keep in step, and drift
// would be silent in the worst way: two tokens for one subscriber, an alert nobody can trace,
// and no error anywhere. One function, imported by all three, makes the pivot true by
// construction rather than by discipline.
//
// The rule is deliberately reproducible outside Go, which is the other half of being usable:
// SHA-256 over the exact identifier bytes, hex-encoded, first LogIdentifierHashLength
// characters. docs/kafka-operations.md publishes the shell equivalent.
//
// An empty input returns an empty string rather than the digest of the empty string, so
// "no identifier" and "some identifier" stay distinguishable — a keyless message was balanced
// across partitions rather than pinned to one, which is exactly what an ordering investigation
// needs to see.
//
// Parameters:
//   - value string: the identifier. Hashed verbatim, with no trimming or case folding, so the
//     caller decides what the canonical form is. May be empty.
//
// Returns:
//   - string: a short hex token, or "" for an empty input.
func HashIdentifier(value string) string {
	if value == "" {
		return ""
	}

	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])[:LogIdentifierHashLength]
}

// EventSubscriber is one row of blnk.event_subscribers: a registered Kafka
// subscriber, the access boundary provisioned for it, and — during the dual-run
// only — the legacy webhook URL it is being migrated away from (requirement R-7).
//
// # The access model this type carries, stated exactly
//
// There are no per-tenant topics: every subscriber reads the same shared category
// topics. Isolation is achieved by making each subscriber a distinct Kafka
// principal and scoping that principal with ACLs.
//
// THE BOUNDARY IS EXACTLY THREE SCOPES, and each one names where it is checked:
//
//	Topic  <each entry of AuthorizedTopics>  LITERAL   Read + Describe   BROKER
//	Group  <ConsumerGroupID>                 PREFIXED  Read              BROKER
//	Key    <PartitionKeyPrefix>              prefix    consume           CONSUMER
//
// KafkaPrincipal, ConsumerGroupID and AuthorizedTopics are the two BROKER-CHECKED
// scopes, and a provisioning call translates them into one SCRAM credential plus
// that set of ACL bindings. KafkaPrincipal is the join key between a registry row
// and the broker's own authorization state, because every binding names it.
//
// # The key scope is a DELIVERED CONTRACT, not a broker binding
//
// Kafka's authorizer has no message-key dimension. Its resource types are Topic,
// Group, Cluster, TransactionalId, DelegationToken and User — there is no
// partition-scoped or key-scoped resource, so no ACL, pattern type or operation
// can confine a consumer to the records whose key carries a given prefix. That is
// a property of Kafka rather than of this system, and no amount of care in this
// package changes it: a principal granted Read on a shared category topic can read
// every record on it, whatever the key.
//
// The two designs that COULD enforce a key boundary at the broker are both closed
// off deliberately. A resource per authorization domain — a topic per key scope —
// is ruled out by the access model's own first line: there are no per-tenant
// topics, because the whole point of the shared category topics is that a new
// subscriber costs no new topics. An interposed filtering gateway that re-emits
// already-isolated streams is subscriber-side consumer machinery, which Blnk
// explicitly does not build. What remains is the one place the key is visible to
// somebody entitled to act on it: the consumer.
//
// SO THE PREFIX IS ISSUED, RETURNED AND STATED, rather than refused. It is part of
// the scope the credential endpoint hands back, alongside the broker endpoint, the
// topic list and the consumer group, so it reaches the party that can apply it;
// and it is returned together with an explicit statement of which scopes the
// broker enforces, so nobody has to infer that the topic grant is whole-topic.
// EffectiveKeyScope is that statement in code, and HasKeyAccess is the one
// authoritative predicate the prefix means, so any component that can see a
// record's key decides with the registry's rule rather than re-deriving it.
//
// # Why disclosure replaced refusal
//
// Refusing issuance for any row carrying a prefix was the previous behaviour. It
// kept the registry honest, but it did so by DECLINING the access model rather
// than implementing it: a subscriber that recorded a key scope got no credential
// at all, so the scope-by-topics-group-and-key model was unavailable in exactly
// the case it was written for. Refusal also fixed nothing a reader could see — the
// row still recorded a prefix, and the only thing that changed was that the
// subscriber could not consume.
//
// Calling the field "advisory" in a comment was the earlier framing and was worse
// than either, because a comment is not what somebody reads when they read a
// database row. What replaces both is a boundary that says, at the moment a
// credential is issued and in the response that carries it, exactly which of its
// three scopes the broker checks and which one the consumer must apply. An
// operator answering "can this subscriber see that ledger?" reads two enforced
// scopes and one delivered obligation, and each is labelled.
//
// This is the same posture the event contract already takes for duplicate
// suppression: Kafka delivery is at-least-once, so event_id is a documented
// subscriber obligation rather than a promise the broker keeps. A key scope is
// that pattern applied to authorization.
//
// Nothing in this package silently widens or narrows the prefix:
// NewSubscriberProvisioningRequest carries it so the credential contract can
// report it and deliberately maps it onto NO ACL binding, and HasTopicAccess —
// which answers a question about topics — ignores it.
//
// # A deregistration in progress is a state of its own
//
// RevocationPendingAt marks a subscriber whose broker-side access is being taken
// away. The row survives a revocation that failed precisely so the failure stays
// recoverable: a deregistration that deleted the row first would destroy the only
// record of which principal still has to be revoked. A pending row is NOT an
// active subscriber — issuance refuses for it too — and it is removed only once
// the broker-side cleanup has been confirmed.
//
// # No secret is stored here, and none can be
//
// The SASL secret is generated during provisioning, returned to the caller
// EXACTLY ONCE, and persisted nowhere — not in this struct and not in the table
// behind it. It cannot be recovered afterwards, only replaced by issuing a new
// one. Only CredentialReference, a non-reversible reference sufficient to prove
// which credential a row corresponds to and insufficient to authenticate with,
// and CredentialIssuedAt are retained. This is the same posture as APIKey, whose
// Key column holds a bcrypt hash rather than the raw key.
//
// There is deliberately NO field capable of holding a plaintext or reversibly
// encrypted secret, and none may be added: blnk.event_subscribers has no column
// to store one in, so such a field would be unimplementable as well as unsafe.
//
// # Nullable fields and what their absence means
//
// Four fields are pointers because for each of them NULL carries information that
// a zero value would destroy:
//
//   - PartitionKeyPrefix nil means NO key constraint was recorded, as opposed to a
//     constraint on the empty prefix — which would invert the intent. Non-nil is
//     returned to the subscriber as the consumer-enforced third scope; see the
//     access-model note above.
//   - CredentialReference nil is the reliable test for "no credential has ever
//     been issued", the real "registered, not yet provisioned" state.
//   - CredentialIssuedAt nil accompanies it; the two are set and overwritten
//     together as one issuance record.
//   - MigratedAt nil means NOT YET MIGRATED, which is precisely what
//     migration-progress reporting counts during the dual-delivery window.
//
// AuthorizedTopics, by contrast, is a plain slice because the column is NOT NULL
// with a '{}' default: a freshly registered subscriber is authorised for NOTHING
// rather than for NULL, so the registry fails closed and no authorisation check
// has to guess whether an absent grant meant "none" or "not yet known".
//
// The Group 4 fields, WebhookURL and MigratedAt, are temporary by design: they
// exist only for the 30-day window in which Kafka publishing and legacy HTTP
// webhook delivery run side by side, and they are what a post-sunset migration
// removes.
type EventSubscriber struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database. The
	// key callers use is SubscriberID.
	ID int64 `json:"id"`

	// --- Subscriber identity ---

	// SubscriberID is the business key and the {id} in
	// POST /subscribers/{id}/kafka-credentials. It is a '<prefix>_<uuid>' string
	// produced by GenerateUUIDWithSuffix, exactly as every other business key in
	// this schema is.
	SubscriberID string `json:"subscriber_id"`
	// Name is the human label an operator recognises the subscriber by. It is
	// required, because an unnamed principal cannot be triaged — being able to
	// answer "who is this principal?" months later is most of the reason the
	// registry exists.
	Name string `json:"name"`

	// --- The Kafka access model ---

	// KafkaPrincipal is the SASL/SCRAM username the ACLs are granted to.
	KafkaPrincipal string `json:"kafka_principal"`
	// ConsumerGroupID is the consumer group the subscriber reads under, returned
	// verbatim by the credential endpoint. The provisioned ACL grants Read on it
	// with a prefixed pattern type, reserving the subscriber's whole group
	// namespace without enumerating every group it might create.
	ConsumerGroupID string `json:"consumer_group_id"`
	// AuthorizedTopics is the exact set of topics the subscriber may Read and
	// Describe, and the set the ACLs are granted over. Empty means authorised for
	// nothing — the registry fails closed.
	AuthorizedTopics []string `json:"authorized_topics"`
	// PartitionKeyPrefix records a key-scoped authorization constraint that KAFKA
	// CANNOT ENFORCE, so a non-nil value makes the subscriber UNPROVISIONABLE:
	// credential issuance refuses rather than mint a credential whose real scope
	// is every record on every authorised topic.
	//
	// Kafka ACLs are topic-level and group-level; the broker has no message-key
	// dimension to restrict on. Nil means no such constraint was recorded, which
	// is the only state a credential can be issued in.
	//
	// NOTHING IN THE SERVICE WRITES A NON-NIL VALUE HERE. Registration and update
	// both refuse a non-blank prefix outright — a stored one would state a tenancy
	// boundary no credential for the row can have, and a row is what an operator,
	// a migration report or a support answer is read from. A non-nil value
	// therefore identifies a LEGACY row: one written before that refusal, or by a
	// psql session, a data migration or a restored backup. The struct still models
	// the column because such rows exist and must be readable, reportable and
	// clearable; clearing is the one write to it the service performs.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// --- The credential record ---

	// CredentialReference is a non-reversible reference to the issued
	// credential. It is NOT the secret and nothing can be authenticated with it.
	// Nil means no credential has ever been issued.
	CredentialReference *string `json:"credential_reference,omitempty"`
	// CredentialIssuedAt is when the credential was issued, set together with
	// CredentialReference. A reissue overwrites both.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// --- Dual-run migration tracking (temporary by design) ---

	// WebhookURL is a MIGRATION RECORD, not a delivery destination. NOTHING SENDS
	// TO IT.
	//
	// It records the legacy HTTP webhook URL a subscriber was receiving pushes on
	// before it moved to Kafka, so that an operator can tell which subscribers are
	// still to be migrated and confirm which URL each one is coming off. Nil for a
	// subscriber onboarded after the cutover, which never had one.
	//
	// Dual-run delivery does NOT read this column. During the 30-day window the
	// relay delivers the legacy leg to the single global Notification.Webhook.Url,
	// which is the entire webhook subscription surface Blnk has ever had — there
	// was never per-subscriber HTTP delivery to reproduce, and inventing one during
	// a deprecation window would be building the transport being retired. Anything
	// that treated a value here as a sink would silently deliver nothing.
	WebhookURL *string `json:"webhook_url,omitempty"`
	// MigratedAt is when the subscriber completed its move to Kafka consumption.
	// Nil means not yet migrated.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// --- Deregistration in progress ---

	// RevocationPendingAt is when deregistration began taking this subscriber's
	// broker-side access away. Nil for every ordinary subscriber.
	//
	// It is the TOMBSTONE that makes a failed revocation recoverable. Deregistration
	// marks the row, revokes at the broker, and deletes the row only once the
	// revocation is confirmed — so a row still carrying this timestamp names a
	// principal that may still authenticate, and retrying the deregistration
	// finishes the job. Deleting the row first, as this once did, destroyed the
	// principal and topic list that revocation needs and left access live with
	// nothing in Blnk able to see it.
	//
	// A pending row is not an active subscriber: credential issuance refuses for it.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt failed at the
	// broker. Nil when no attempt has failed since the last one started.
	//
	// It exists because RevocationPendingAt is stamped BEFORE the broker is touched
	// and therefore covers two situations that need different work: the broker
	// REFUSED the revocation, or the process died between the stamp and the attempt.
	// The first needs the administrative principal's authorization or the broker's
	// reachability fixed before any retry can succeed; the second needs nothing but
	// the retry. This value is cleared at the start of every new attempt, so it
	// always describes the latest one rather than accumulating history — while
	// RevocationPendingAt keeps the FIRST instant, because the age of the exposure
	// is what an operator alerts on.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// --- A credential that outlived its record ---

	// CredentialOrphanedAt is when an issuance left a credential at the broker that
	// Blnk could neither record nor revoke. Nil for every healthy subscriber.
	//
	// Provisioning writes the SCRAM credential BEFORE the ACL bindings, because a
	// binding for a principal that does not exist is inert while a credential with no
	// bindings still AUTHENTICATES. So a failure to record the issuance is compensated
	// by revoking the credential — and when that compensation also fails, a means of
	// authenticating exists for a principal the registry records no issuance for.
	// Before this column, the only trace of that was a log line, which meant the
	// outstanding-revocation alert could not see it: that rule reads
	// RevocationPendingAt, which this path never sets.
	//
	// IT IS NOT THE SAME STATE AS RevocationPendingAt AND THE REMEDIES ARE OPPOSITE. A
	// row marked pending revocation is on its way out, is not an active subscriber, and
	// is settled by RETRYING THE DEREGISTRATION. A row marked orphaned IS an active
	// subscriber whose recorded credential is untrustworthy, and is settled by
	// RE-ISSUING — which replaces the orphan by construction, since Kafka stores one
	// credential per principal — or by deprovisioning if it should have no access at
	// all. Collapsing the two would have made the re-issue remedy unreachable, because
	// issuance refuses a row marked pending revocation.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// --- Settlement obligations the background pass owes ---

	// GrantReconcilePendingAt and CredentialCleanupPendingAt are the two settlement markers,
	// stamped when a broker-side step could not be completed and cleared only once it has been.
	//
	// They were already columns, already written by the settlement paths and already counted by
	// the residue aggregate — but they were NOT on this struct, and so were invisible to every
	// predicate a lifecycle path consults. A row whose ACL grant was never created, or whose
	// credential Blnk intended to destroy and could not, therefore read as an ordinary row to
	// the guard that decides whether broker work can be confirmed.
	//
	// GrantReconcilePendingAt means the recorded authorization is WIDER than the broker's, so a
	// grant this row describes may not exist. CredentialCleanupPendingAt means a SCRAM credential
	// may exist that Blnk intended to destroy, or the recorded reference names one that no longer
	// works — which is the direction that matters, because a credential nothing accounts for
	// still authenticates.
	//
	// Both keep their FIRST instant on repeated stamping, because the age of the obligation is
	// what an operator alerts on rather than the age of the latest attempt.
	GrantReconcilePendingAt    *time.Time `json:"grant_reconcile_pending_at,omitempty"`
	CredentialCleanupPendingAt *time.Time `json:"credential_cleanup_pending_at,omitempty"`

	// --- Row bookkeeping ---

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasTopicAccess reports whether the subscriber's RECORDED GRANT covers the given
// topic.
//
// The comparison is exact and the empty-grant case falls out of it naturally: a
// subscriber with no authorised topics matches nothing, so the registry fails
// closed rather than open. PartitionKeyPrefix is deliberately not consulted —
// this answers a TOPIC question, and folding a key scope into it would blur the
// broker-enforced boundary with the consumer-side one. HasKeyAccess is where the
// key scope is answered, and KeyScopeEnforcement is where the difference is named.
//
// This is a read over the recorded grant and is NOT a substitute for broker-side
// ACL enforcement. The broker is the authority; this method exists so the service
// layer can reject an obviously out-of-boundary request before spending a round
// trip to find out.
func (s *EventSubscriber) HasTopicAccess(topic string) bool {
	for _, t := range s.AuthorizedTopics {
		if t == topic {
			return true
		}
	}
	return false
}

// RequestedKeyScope is the key scope recorded for this subscriber, flattening the
// nullable column to a plain string.
//
// "" means no scope was requested, which is the same thing as "every key on the
// authorised topics" — the two readings are identical and the empty string is used
// for both so no caller has to handle a third state.
//
// The scope is RECORDED rather than derived, and it must be: a message key on Blnk's
// topics is the LEDGER ID for every ledger-scoped event and otherwise an identity id,
// a batch id or the event type — never anything derived from a subscriber — so a scope
// computed from the subscriber's own identifier would match no record ever produced.
// Only the caller registering the subscriber knows which ledgers it is entitled to.
//
// # Why it trims
//
// The presence predicates — RequiresClientSideKeyFiltering and DeclaresKeyScope —
// trim before deciding whether a scope is recorded at all, and this accessor did not.
// The two therefore disagreed about exactly one value: a whitespace-only prefix, which
// the predicates read as "no scope" while HasKeyAccess and EffectiveKeyScope read as a
// real one. No message key begins with a space, so a subscriber holding such a row was
// told it had all-keys access and would then have discarded EVERY record it consumed —
// silently, with nothing in the registry looking wrong. Trimming here makes the whole
// family agree by construction, and it costs nothing: a legitimate ledger-id prefix has
// no surrounding whitespace, and the write path refuses one that does rather than
// trimming it, so a stored value only ever needs trimming if it predates that refusal.
func (s *EventSubscriber) RequestedKeyScope() string {
	if s == nil || s.PartitionKeyPrefix == nil {
		return ""
	}

	return strings.TrimSpace(*s.PartitionKeyPrefix)
}

// HasKeyAccess reports whether this subscriber is entitled to a record carrying the
// given message key.
//
// This is the AUTHORIZATION RULE the key scope is expressed in, and it exists so
// that any component able to see a record's key — a Blnk-controlled filtering layer,
// a replay path, an administrative export — decides with one authoritative
// predicate instead of re-deriving prefix logic that could drift from what the
// registry recorded.
//
// With no scope recorded every key is in bounds, which is why an unprovisioned
// prefix and an explicitly whole-topic entitlement read the same here. With a scope
// recorded only keys carrying it are in bounds, and the comparison is a byte-exact
// prefix test: keys are opaque identifiers rather than text, so no case folding, no
// trimming and no normalisation is applied — a caller whose key differs from the
// recorded scope by whitespace is asking about a different key.
//
// IT IS NOT WHAT THE BROKER CHECKS. Kafka's authorizer has no message-key dimension,
// so this rule is honoured wherever a record's key is visible to something willing to
// act on it: the subscriber's own consumer, to which the scope is delivered by the
// credential endpoint, and any Blnk-side path that reads keys — a replay, an
// administrative export. Keeping the rule here rather than at those call sites is what
// stops two of them disagreeing about what a recorded prefix means.
func (s *EventSubscriber) HasKeyAccess(key string) bool {
	// DeclaresKeyScope, not `RequestedKeyScope() != ""`. The two differ on a whitespace-only
	// column, and here that difference decides whether EVERY record is excluded: no ledger id
	// begins with a tab, so a scope of "\t" would make this rule refuse the subscriber's whole
	// stream without an error anywhere. All three of this predicate, EffectiveKeyScope and the
	// credential contract read the same presence test for that reason.
	if !s.DeclaresKeyScope() {
		return true
	}

	// The recorded value VERBATIM, not the trimmed one: keys are opaque identifiers, so
	// trimming here would test a different prefix than the registry recorded. Only the
	// question "is there a scope at all" tolerates trimming.
	return strings.HasPrefix(key, s.RequestedKeyScope())
}

// SubscriberKeyScopeAllKeys is the key scope of a credential issued to a subscriber
// that recorded no prefix, and the value the credential contract reports for it:
// EVERY key on the authorised topics.
//
// It is a descriptive word rather than "*" deliberately. "*" is Kafka's own
// match-anything resource name, so a response field carrying it would read as an ACL
// pattern a client might send back somewhere, and a client comparing against it
// would be comparing against a value with a second meaning.
const SubscriberKeyScopeAllKeys = "all-keys"

// EffectiveKeyScope reports the key scope a credential issued for this subscriber
// would ACTUALLY carry, together with whether that scope is enforced.
//
// The pair exists because the requested scope and the delivered scope are not always
// the same value, and a contract that reported only one of them would be the defect
// this method closes: a response echoing a requested prefix beside a credential that
// can read the whole topic states a boundary that does not exist.
//
// Returns:
//   - scope: SubscriberKeyScopeAllKeys when no scope was requested, which is the
//     honest description of what a topic ACL grants on its own; otherwise the
//     recorded prefix, which is the boundary the consumer must apply.
//   - enforced: whether THE BROKER enforces the scope. True only for the all-keys
//     case, where the topic ACL is the whole boundary and nothing is left to the
//     consumer. A recorded prefix is reported as broker-UNENFORCED, and the pair is
//     handed to the subscriber precisely so that it knows which half is its own to
//     keep — see the access-model note on EventSubscriber.
func (s *EventSubscriber) EffectiveKeyScope() (scope string, enforced bool) {
	// DeclaresKeyScope is the presence test, NOT `RequestedKeyScope() != ""`, and the
	// difference is a whitespace-only prefix. RequestedKeyScope returns the column
	// verbatim — correct, because message keys are opaque and normalising one would
	// select a different set of records — so a column holding a tab would otherwise be
	// reported here as a real scope. This pair is what the credential endpoint delivers
	// to a consumer, and a consumer applying a scope of "\t" discards its ENTIRE stream
	// silently. Sharing the one presence test is what makes that unreachable rather than
	// merely unlikely; normalizeSubscriberKeyScope refusing such a value at persistence
	// is the outer layer of the same defence.
	if s.DeclaresKeyScope() {
		return s.RequestedKeyScope(), false
	}

	return SubscriberKeyScopeAllKeys, true
}

// SubscriberSettlementObligation is one subscriber's outstanding broker-side obligation, as the
// settlement pass sees it.
//
// It is a NARROW projection rather than a whole EventSubscriber, and deliberately so. The
// obligation is an operational fact about work Blnk still owes the broker; it is not part of a
// subscriber's public representation, so putting these columns on EventSubscriber would have
// added them to every registry response, every row fixture and the wire contracts that pin those
// responses field-for-field — for the benefit of one background worker.
//
// The worker re-reads the full row when it acts, because both remedies need the derived
// principal, the consumer group and the authorized topic set, and deriving those twice is how
// the broker-side grant and the registry drift apart in the first place.
type SubscriberSettlementObligation struct {
	// SubscriberID identifies the row that owes the work.
	SubscriberID string

	// GrantReconcilePending means the broker's ACL bindings may not match the row's recorded
	// authorization, so the grant must be reconciled to the row.
	GrantReconcilePending bool

	// CredentialCleanupPending means a SCRAM credential may exist that Blnk intended to
	// destroy, or the row names one that no longer works. Settlement revokes and then clears.
	CredentialCleanupPending bool

	// Attempts is how many settlement passes have already tried this row, and LastError is what
	// the most recent one said.
	//
	// They are carried so a pass can log the history rather than only the present failure: an
	// obligation on its twentieth attempt with the same broker error is a different operational
	// situation from one on its first, and nothing else distinguishes them.
	Attempts  int
	LastError string
}

// Outstanding reports whether this obligation still requires work.
//
// It exists so no caller has to remember that an obligation is the OR of two independent flags.
// A row is listed by the settlement scan only when one of them is set, but a pass that discharges
// one and fails the other must be able to ask whether anything remains, and spelling that
// condition out at each site is how the two drift apart.
func (o SubscriberSettlementObligation) Outstanding() bool {
	return o.GrantReconcilePending || o.CredentialCleanupPending
}

// IsProvisioned reports whether a credential has ever been issued to this
// subscriber.
//
// It tests the credential reference rather than the issuance timestamp because
// the reference is the value the repository writes first and the two are always
// written together; either would do, and testing the reference keeps the
// "registered, not yet provisioned" state readable at the call site.
//
// A NIL RECEIVER answers false, matching RequiresClientSideKeyFiltering. It has to: the repository's not-found
// representation for a subscriber is a nil pointer, so every caller that reads a
// row and then asks a question about it can hold one, and a predicate that
// panicked there would turn a missing subscriber into a crashed ledger process
// rather than a 404.
func (s *EventSubscriber) IsProvisioned() bool {
	return s != nil && s.CredentialReference != nil && *s.CredentialReference != ""
}

// MayHaveBrokerCredential reports whether a means of authenticating as this row's principal
// MIGHT exist at a broker.
//
// # Why the question is "might" and not "does"
//
// This is the guard that decides whether an operation which cannot reach a broker is allowed to
// proceed as though there were nothing at a broker to act on. The only safe answer to that
// question is drawn from the row's own evidence, and the evidence is one-sided: Blnk can be
// certain a credential was written, but it can never be certain one was not, because the failure
// modes that lose the record are exactly the ones that leave the credential behind.
//
// So this predicate is the UNION of every state that can coexist with a live credential, and it
// is deliberately conservative. A false positive costs a retryable refusal; a false negative
// costs a live SASL credential with live ACL bindings, left at a broker with nothing anywhere
// naming the principal it belongs to and no row to retry from.
//
// # The three states, and why each one has to be here
//
//   - A CREDENTIAL REFERENCE: a credential was written for this principal and has not been
//     confirmed removed. This is the certain case and was the only one IsProvisioned tested.
//   - AN ORPHAN MARKER: an issuance wrote the credential at the broker and then could neither
//     record nor revoke it, so the reference is NIL while a means of authenticating exists. This
//     is the state the reference test gets exactly backwards — the absence of the record IS the
//     evidence — and it is how a deletion once removed the only row naming a live principal.
//   - A CLEANUP OBLIGATION: a credential Blnk intended to destroy may still exist, or the
//     recorded reference names one that no longer works. Either way the broker's state is
//     unconfirmed, and the direction of the doubt is that something authenticates.
//
// # The two markers deliberately NOT included, and why each exclusion is safe
//
// GrantReconcilePendingAt means the recorded authorization is WIDER than the broker's — a grant
// that may not exist. That is the safe direction: no authentication follows from a missing ACL
// binding, and blocking a deletion on it would strand rows whose only defect is that they promise
// less access than they were given.
//
// RevocationPendingAt is NOT credential evidence, and including it would be actively harmful.
// It is a tombstone stamped by deregistration BEFORE the broker is touched, so a deregistration
// consults this predicate on a row it has just marked — and a predicate that answered true for
// its own tombstone would make deregistration impossible in a deployment that never configured
// Kafka, which is a supported steady state. It also adds nothing: a tombstoned row that ever held
// a credential still carries its reference, because the reference is cleared only after a
// confirmed revocation. Grants and widenings for a tombstoned row are refused separately, by the
// active-subscriber guard, which is where that concern belongs.
//
// # A nil receiver answers false
//
// Matching IsProvisioned and IsRevocationPending, for the reason all three share: the
// repository's not-found representation is a nil pointer, and a guard that panics where it is
// supposed to refuse is worse than no guard at all. A subscriber that does not exist has no
// credential.
//
// Returns:
//   - bool: true when any of the three states above holds.
func (s *EventSubscriber) MayHaveBrokerCredential() bool {
	if s == nil {
		return false
	}

	return s.IsProvisioned() ||
		s.CredentialOrphanedAt != nil ||
		s.CredentialCleanupPendingAt != nil
}

// BrokerCredentialEvidence names WHY MayHaveBrokerCredential answered true, for the log line and
// the error detail that accompany a refusal.
//
// A refusal that says only "this subscriber may hold a credential" leaves an operator to work out
// which of four states they are looking at, and the remedies differ: an orphan is settled by
// RE-ISSUING, a tombstone by retrying the DEREGISTRATION, a cleanup obligation by letting the
// settlement pass run or forcing a revocation. Naming the state is what makes the refusal
// actionable rather than merely correct.
//
// Returns:
//   - string: a short phrase naming the strongest evidence, or "" when there is none. The order
//     is by certainty: a recorded reference is a fact, the markers are possibilities.
func (s *EventSubscriber) BrokerCredentialEvidence() string {
	switch {
	case s == nil:
		return ""
	case s.IsProvisioned():
		return "the registry records an issued credential for this principal"
	case s.CredentialOrphanedAt != nil:
		return "an issuance left a credential at the broker that Blnk could neither record nor revoke"
	case s.CredentialCleanupPendingAt != nil:
		return "a credential this row intended to destroy has not been confirmed destroyed"
	default:
		return ""
	}
}

// IsMigrated reports whether the subscriber has completed its move from legacy
// HTTP webhook delivery to Kafka consumption. A nil MigratedAt means not yet
// migrated, which is what dual-window migration-progress reporting counts.
//
// A nil RECEIVER answers false for the same reason IsProvisioned does: a
// subscriber that does not exist has not migrated, and saying so is better than
// panicking in a process that moves money.
func (s *EventSubscriber) IsMigrated() bool {
	return s != nil && s.MigratedAt != nil
}

// IsRevocationPending reports whether deregistration has begun taking this
// subscriber's broker-side access away and has not yet confirmed it.
//
// Such a row is not an active subscriber. It exists only so the outstanding
// revocation stays recoverable, and issuing a credential for it would re-arm a
// principal that is in the middle of being taken out of service.
// A nil receiver answers false. Both this predicate and RequiresClientSideKeyFiltering are read
// as GUARDS on a row that may not have been found, so they must be answerable on nothing: a
// guard that panics where it is supposed to refuse is worse than no guard at all.
func (s *EventSubscriber) IsRevocationPending() bool {
	return s != nil && s.RevocationPendingAt != nil
}

// RequiresClientSideKeyFiltering reports whether the subscriber records a partition key prefix
// whose narrowing the SUBSCRIBER has to apply itself, because the broker will not.
//
// # It states a fact, and it must not be read as a verdict
//
// The name says what the SUBSCRIBER must do, not what Blnk refuses to do, and the distinction is
// the whole reason it is worded this way. An earlier predicate over this same column was named
// for unenforceability, and credential issuance failed closed on it: a subscriber recording a
// prefix could never obtain a credential, permanently, and the mandatory credential endpoint
// answered 409 for it forever. The intent was honesty — a topic-level credential is wider than a
// key-scoped row appears to describe — but the effect was to withdraw a required capability, and
// it withdrew it for a state the registry is explicitly designed to hold. That predicate has been
// removed rather than merely re-documented, so no future caller can reach for a name that
// implies a refusal this package no longer performs.
//
// Kafka's authorizer has FIVE resource types — Topic, Group, Cluster, TransactionalId and
// DelegationToken — and none of them is a message key. There is no binding, pattern type or
// operation that confines a consumer to the records whose key carries a given prefix, and the
// only architecture that would enforce a per-key boundary is a topic per key space, which is
// ruled out: no per-tenant topics. So a key-enforcing credential is not something Blnk declines
// to mint; it is not something that exists.
//
// What is therefore required is not a refusal but a STATEMENT. Issuance succeeds, and the
// response says outright which dimensions the broker enforces, echoes the recorded prefix, and
// answers the one question whose wrong answer is a data-disclosure bug — see
// api/model.SubscriberEnforcedAccess. A reader of the registry or the credential response is
// told that key filtering is the subscriber's own obligation, which is the strongest guarantee
// available and a far better one than a comment that says the field is advisory.
//
// The empty string is treated as absent for the same reason the column is nullable: "a
// constraint on the empty prefix" is not an intent anybody has.
//
// A nil receiver answers false, for the reason given on IsRevocationPending.
//
// Returns:
//   - bool: true when a non-blank partition key prefix is recorded.
func (s *EventSubscriber) RequiresClientSideKeyFiltering() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// ---------------------------------------------------------------------------------------
// Canonical subscriber identity — SEC-03 and SEC-04
//
// A subscriber's Kafka PRINCIPAL and CONSUMER GROUP NAMESPACE are the two names its whole
// access boundary is expressed in: the SCRAM credential is minted for the principal, and
// every ACL binding names it and the group namespace. So whoever chooses those two strings
// chooses the boundary, and the strings must therefore be DERIVED from the subscriber's
// immutable identifier rather than accepted from a request.
//
// # The two defects this closes
//
// SEC-03 — an accepted value was only TRIMMED. A request could ask for the principal or
// group "*", and Kafka treats the resource name "*" as matching ANY resource, so a prefixed
// or literal binding on it is a cluster-wide grant. A group namespace could also be chosen
// to OVERLAP another subscriber's — asking for "acme-" reserves every group beginning with
// those characters, including a different subscriber's — which is a cross-domain grant
// obtained through a perfectly ordinary, authorized request.
//
// SEC-04 — trimming happened at provisioning but not at persistence, so the rows "alice"
// and " alice " both satisfied the unique index while provisioning the SAME Kafka
// principal. Two registry subscribers then shared one credential and one ACL set, and
// reissuing for either silently rewrote the other's.
//
// # The shape, and why each part of it
//
// Principal:            blnk-sub-<identifier>
// Group namespace:      blnk-sub-<identifier>.        <- the PREFIXED ACL resource name
// Default consumer group: blnk-sub-<identifier>.default
//
//   - The NAMESPACE PREFIX makes every Blnk-issued principal recognisable at the broker and
//     keeps it from colliding with an operator's own principals.
//   - The TERMINATOR on the group namespace is what makes prefixed grants disjoint. Without
//     it, a subscriber whose identifier is a leading substring of another's would reserve
//     the other's namespace too; the terminator cannot occur inside a validated identifier,
//     so "blnk-sub-abc." and "blnk-sub-abcd." can never overlap however the identifiers
//     relate.
//   - A LEAF under the namespace is the subscriber's own to choose, which is the entire
//     point of granting a prefixed rather than a literal group: a replay group can run
//     beside a live one with no administrative round trip, and still inside the boundary.
//
// # Canonicalization rejects rather than folds
//
// An identifier that is not already canonical is REFUSED, not rewritten. Case is the
// instructive example: Kafka principals are case-sensitive, so "Alice" and "alice" are two
// different identities at the broker. Folding case would merge two distinct registry rows
// onto one principal — which is SEC-04 again, arrived at from the other direction. Refusing
// leaves exactly one spelling of any identifier and no merging.
// ---------------------------------------------------------------------------------------

// SubscriberPrincipalNamespace prefixes every Kafka principal and consumer group Blnk
// derives, so a Blnk-issued identity is recognisable at the broker and cannot collide with
// an operator's own principals.
const SubscriberPrincipalNamespace = "blnk-sub-"

// SubscriberGroupTerminator ends a subscriber's consumer group namespace.
//
// It is the character that makes two prefixed group grants disjoint, and it is deliberately
// one that CanonicalizeSubscriberIdentifier forbids inside an identifier — that exclusion is
// the whole guarantee, so the two rules must be read together.
const SubscriberGroupTerminator = "."

// SubscriberDefaultGroupLeaf is the leaf of the consumer group a subscriber is given when it
// has not chosen one. It is a leaf inside the subscriber's own namespace, so using it is
// already inside the grant.
const SubscriberDefaultGroupLeaf = "default"

// maxSubscriberIdentifierLen bounds the identifier so the derived principal stays inside
// Kafka's own 255-character limit on a SCRAM user name with generous room for the namespace
// prefix and the group leaf.
const maxSubscriberIdentifierLen = 128

// minSubscriberIdentifierLen keeps an identifier long enough to be meaningful. A
// one-character identifier is almost certainly a mistake, and it maximises the chance of one
// identifier being a leading substring of another.
const minSubscriberIdentifierLen = 3

// ErrInvalidSubscriberIdentifier reports an identifier that cannot be used to derive a Kafka
// identity.
//
// It is a sentinel so the API layer, the repository and the provisioning path all recognise
// the same refusal without matching message text, and so that "this identifier is unusable"
// is provably one condition rather than three similar checks.
var ErrInvalidSubscriberIdentifier = errors.New(
	"model: subscriber identifier cannot be used to derive a Kafka identity",
)

// CanonicalizeSubscriberIdentifier returns the identifier in its one permitted spelling, or
// refuses it.
//
// The accepted alphabet is lowercase ASCII letters, digits, underscore and hyphen, starting
// with a letter or digit. Everything else is refused, and the refusal names the reason:
//
//   - WILDCARDS ("*", "?") because Kafka's resource name "*" matches any resource, making a
//     binding on it a cluster-wide grant.
//   - WHITESPACE, including leading and trailing, because a value that differs only by
//     whitespace is the SEC-04 duplicate: two rows, one principal.
//   - CONTROL CHARACTERS and anything outside printable ASCII, because they survive
//     round trips through broker configuration, JAAS files and shell arguments
//     unpredictably, and because SASLprep rewrites or rejects parts of that range — a
//     principal that authenticates for nobody.
//   - The SASL and ACL METACHARACTERS ":", ",", ";", "=", "/" and the group terminator ".",
//     because they carry meaning in the principal syntax ("User:name"), in JAAS
//     configuration and in the group namespace scheme.
//   - UPPERCASE, because folding it would merge two case-distinct broker identities onto
//     one, and refusing leaves exactly one spelling.
//
// Parameters:
//   - identifier string: the subscriber's business identifier, normally its
//     '<prefix>_<uuid>' subscriber_id.
//
// Returns:
//   - string: the identifier, unchanged, when it is already canonical.
//   - error: wrapping ErrInvalidSubscriberIdentifier, naming the specific rule broken.
func CanonicalizeSubscriberIdentifier(identifier string) (string, error) {
	if identifier == "" {
		return "", fmt.Errorf("%w: it is empty", ErrInvalidSubscriberIdentifier)
	}

	if identifier != strings.TrimSpace(identifier) {
		// Reported separately from the alphabet rule because this is the SEC-04 case, and
		// an operator who sees "surrounding whitespace" fixes it immediately.
		return "", fmt.Errorf(
			"%w: %q has surrounding whitespace, and a value that differs from another only by "+
				"whitespace would provision the same Kafka principal twice",
			ErrInvalidSubscriberIdentifier, identifier,
		)
	}

	if len(identifier) < minSubscriberIdentifierLen || len(identifier) > maxSubscriberIdentifierLen {
		return "", fmt.Errorf(
			"%w: %q is %d characters, outside the permitted %d to %d",
			ErrInvalidSubscriberIdentifier, identifier, len(identifier),
			minSubscriberIdentifierLen, maxSubscriberIdentifierLen,
		)
	}

	for i := 0; i < len(identifier); i++ {
		character := identifier[i]

		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case (character == '_' || character == '-') && i > 0:
		default:
			return "", fmt.Errorf(
				"%w: %q contains %q at position %d; only lowercase ASCII letters, digits, "+
					"underscore and hyphen are permitted, and the first character must be a letter or digit",
				ErrInvalidSubscriberIdentifier, identifier, string(character), i,
			)
		}
	}

	return identifier, nil
}

// CanonicalKafkaPrincipal derives the SASL/SCRAM username for a subscriber.
//
// This is the ONLY way a principal may be produced. A principal supplied by a caller is not
// a request for a name, it is a request for a boundary, so provisioning compares what it was
// given against what this derives and refuses a mismatch.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalKafkaPrincipal(identifier string) (string, error) {
	canonical, err := CanonicalizeSubscriberIdentifier(identifier)
	if err != nil {
		return "", err
	}

	return SubscriberPrincipalNamespace + canonical, nil
}

// CanonicalConsumerGroupNamespace derives the PREFIXED ACL resource name that reserves a
// subscriber's consumer group namespace.
//
// The trailing terminator is the disjointness guarantee: it cannot appear inside a canonical
// identifier, so no subscriber's namespace can be a prefix of another's.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>.".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalConsumerGroupNamespace(identifier string) (string, error) {
	canonical, err := CanonicalizeSubscriberIdentifier(identifier)
	if err != nil {
		return "", err
	}

	return SubscriberPrincipalNamespace + canonical + SubscriberGroupTerminator, nil
}

// CanonicalConsumerGroupID derives the consumer group a subscriber reads under by default.
//
// It is a leaf inside the namespace CanonicalConsumerGroupNamespace reserves, so a
// subscriber using it is already inside its grant, and a subscriber that wants a second
// group picks another leaf without an administrative round trip.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>.default".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalConsumerGroupID(identifier string) (string, error) {
	namespace, err := CanonicalConsumerGroupNamespace(identifier)
	if err != nil {
		return "", err
	}

	return namespace + SubscriberDefaultGroupLeaf, nil
}

// IsInSubscriberGroupNamespace reports whether a consumer group lies inside a namespace.
//
// It is what lets a subscriber choose its own group leaf while the boundary stays checkable:
// the group must start with the namespace AND add something to it, so the bare namespace —
// which is not a usable group name — is not mistaken for a group inside itself.
//
// Parameters:
//   - group string: the consumer group to test.
//   - namespace string: a namespace from CanonicalConsumerGroupNamespace.
//
// Returns:
//   - bool: true when group is a proper leaf of namespace.
func IsInSubscriberGroupNamespace(group, namespace string) bool {
	if namespace == "" || group == "" {
		return false
	}

	return len(group) > len(namespace) && strings.HasPrefix(group, namespace)
}

// Transaction lifecycle event names. These are the strings subscribers filter on, so
// each is a published contract and none may be renamed without a subscriber-visible
// migration.
const (
	EventTypeTransactionQueued    = "transaction.queued"
	EventTypeTransactionApplied   = "transaction.applied"
	EventTypeTransactionScheduled = "transaction.scheduled"
	EventTypeTransactionInflight  = "transaction.inflight"
	EventTypeTransactionVoid      = "transaction.void"
	EventTypeTransactionRejected  = "transaction.rejected"
	// EventTypeTransactionUnknown is a REAL, REACHABLE value and not a placeholder.
	// The COMMIT status has no case in EventTypeForTransactionStatus and falls
	// through to it. See that function for why that is preserved deliberately.
	EventTypeTransactionUnknown = "transaction.unknown"
)

// The two REPEATABLE event names, declared so the one place that has to distinguish them
// from the rest — EventTypeIsRepeatable, which decides whether an id may be derived —
// names them rather than spelling them again. They are the same published strings
// EventCategory routes on.
const (
	EventTypeBalanceMonitor = "balance.monitor"
	EventTypeSystemError    = "system.error"
)

// Transaction status literals, as assigned by the root package's Status* constants.
//
// They are restated here because `model` cannot import the root package, and
// TestEventTypeForTransactionStatus_AgreesWithTheLegacyMapping in the root package
// asserts this mapping against getEventFromStatus for every status constant — so the
// duplication cannot drift undetected.
const (
	transactionStatusQueued    = "QUEUED"
	transactionStatusApplied   = "APPLIED"
	transactionStatusScheduled = "SCHEDULED"
	transactionStatusInflight  = "INFLIGHT"
	transactionStatusVoid      = "VOID"
	transactionStatusRejected  = "REJECTED"
)

// EventTypeForTransactionStatus maps a transaction status to its event name.
//
// It is the AAP-mandated relocation of the root package's getEventFromStatus: that
// function IS the event-string vocabulary for the transactions category, and it has
// to outlive webhooks.go, which is deleted at the webhook sunset. The root function
// delegates here so there is one table rather than two that can drift.
//
// Comparison is case-insensitive, exactly as the original is, so a status arriving in
// any casing resolves the same way.
//
// # The COMMIT fall-through is deliberate and must not be given a case
//
// The COMMIT status is genuinely assigned when an inflight transaction is committed,
// and it has NO case here, so it falls through to EventTypeTransactionUnknown. That
// is pre-existing behaviour, not a regression introduced by the Kafka work, and it is
// preserved on purpose: the dual-delivery comparison asserts that the Kafka message
// and the legacy webhook carry identical bytes for the same event, and adding a
// transaction.commit case would change one side of that comparison and fail it for a
// reason that has nothing to do with the transport. It is documented in
// docs/event-streaming.md for correction later as a deliberate, separately reviewed
// change — with the subscriber-facing event-name change that implies.
//
// Parameters:
//   - status string: the transaction status, in any casing.
//
// Returns:
//   - string: the event name; EventTypeTransactionUnknown for an unmapped status.
func EventTypeForTransactionStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case transactionStatusQueued:
		return EventTypeTransactionQueued
	case transactionStatusApplied:
		return EventTypeTransactionApplied
	case transactionStatusScheduled:
		return EventTypeTransactionScheduled
	case transactionStatusInflight:
		return EventTypeTransactionInflight
	case transactionStatusVoid:
		return EventTypeTransactionVoid
	case transactionStatusRejected:
		return EventTypeTransactionRejected
	default:
		// COMMIT lands here. See the note above; this fall-through is intentional.
		return EventTypeTransactionUnknown
	}
}

// Subscriber identifier generation and topic-grant bounds.
//
// The registry ISSUES a subscriber's business identifier rather than accepting whatever a
// caller supplies, and these are the two halves of that: the generator, and the bounds a
// grant recorded against a generated identity has to stay inside.
const (
	// SubscriberIDPrefix is the module prefix of a generated subscriber business key,
	// following the repository's GenerateUUIDWithSuffix convention so a subscriber id is
	// recognisable at a glance in a log line or a broker ACL listing.
	SubscriberIDPrefix = "sub"

	// ConsumerGroupIDPrefix is the prefix every consumer group Blnk issues carries. It is
	// SubscriberPrincipalNamespace, because a subscriber's consumer group and its Kafka
	// principal are derived from the same identifier and share one namespace — the ACL
	// grant that reserves the group namespace is what makes them one boundary rather than
	// two.
	ConsumerGroupIDPrefix = SubscriberPrincipalNamespace

	// MaxSubscriberTopics bounds an authorised-topic grant.
	//
	// An unbounded list is an unbounded allocation, an unbounded row and an unbounded burst
	// of broker requests, all reachable from one request body. The grantable ALLOWLIST is
	// the primary narrowing — see SubscriberGrantableTopics — and this is the resource
	// bound that holds even if the allowlist ever grows: a bound that generous
	// accommodation cannot exceed is the only kind worth having.
	MaxSubscriberTopics = 16

	// MaxTopicNameLength is Kafka's own limit on the length of a topic name.
	//
	// Kafka rejects a longer name, so accepting one only defers the failure to the point
	// where it is expensive and confusing: the registry row is already written, the ACL
	// request already in flight, and the error arrives from the broker with no reference to
	// the field that caused it.
	MaxTopicNameLength = 249
)

// eventIDNamespace is the fixed UUID namespace every derived event id is generated
// under.
//
// It must never change. Every derived id is a function of this value, so a new
// namespace would give the same logical event a different id — and the unique index
// on event_id, which is what makes a logical retry idempotent, would stop
// recognising it. The value is arbitrary; its stability is not.
var eventIDNamespace = uuid.MustParse("6f2d1a55-9f6b-4a1e-8d2c-0f43a1b7c9e1")

// DeriveEventID derives the deterministic event id for one logical event.
//
// # Why determinism matters here
//
// event_id carries TWO contracts at once: it is the subscriber's idempotency key,
// and it is the unique index that makes the outbox's write side exactly-once. A
// freshly random id per preparation satisfies neither for a RETRY. A mutation that
// is retried after an ambiguous failure — a commit whose acknowledgement was lost,
// a request replayed by a client, a worker that restarted mid-flight — prepares its
// event a second time, and with a random id the unique index sees a different key,
// admits a second row, and the subscriber receives the same business event twice
// with no way to tell. Deriving the id from what the event IS rather than from when
// it happened to be prepared makes the second insert collide, which is exactly what
// the index is for.
//
// # The inputs, and why each is part of the identity
//
//   - mutationID: the identity of the thing that changed — a transaction id, a
//     ledger id, an identity id, a balance id, a batch id. Two events about
//     different things must never share an id.
//   - eventType: two events about the SAME thing are still different events.
//     transaction.queued and transaction.applied for one transaction must have
//     distinct ids, or the second would be suppressed as a duplicate of the first.
//   - schemaVersion: a v2 envelope carrying the same business event is a different
//     message to a subscriber that decodes by version. Including it means a schema
//     migration can re-emit an event without colliding with its v1 self.
//
// The parts are joined with a separator that cannot occur in a UUID or in Blnk's
// prefixed identifiers, so two different part lists cannot produce the same input
// string — without it, ("ab", "c") and ("a", "bc") would derive the same id.
//
// # Version 5, not version 4
//
// A version-5 (name-based, SHA-1) UUID is a pure function of its namespace and name,
// which is precisely the property required. The hash is used as an identifier and
// never as a security primitive.
//
// Parameters:
//   - mutationID string: the identity of the mutation the event describes.
//   - eventType string: the event name.
//   - schemaVersion int: the envelope schema version.
//
// Returns:
//   - string: a canonical, textual version-5 UUID.
func DeriveEventID(mutationID, eventType string, schemaVersion int) string {
	name := strings.Join([]string{
		strings.TrimSpace(mutationID),
		strings.TrimSpace(eventType),
		strconv.Itoa(schemaVersion),
	}, "\x1f")

	return uuid.NewSHA1(eventIDNamespace, []byte(name)).String()
}

// NewEventID mints a fresh, random event id for an event that has NO stable identity.
//
// Not every event can be derived, and pretending otherwise would be worse than
// randomness. Two events are repeatable by nature:
//
//   - balance.monitor fires every time its condition is met, so deriving from the
//     monitor id would collapse every firing after the first into a duplicate the
//     unique index rejects — the pipeline would silently stop delivering alerts,
//     which is the exact opposite of what a monitor is for.
//   - system.error is emitted per occurrence; two identical messages a second apart
//     are two events an operator needs to see twice.
//
// For those, a random id is the CORRECT answer and a derived one would be a defect.
// EventIdentityFor is what decides between them.
//
// Returns:
//   - string: a canonical, textual version-4 UUID.
func NewEventID() string {
	return uuid.New().String()
}

// EventIdentityFor reports the mutation identity a deterministic event id may be
// derived from, and whether the event has one at all.
//
// The second return value is the load-bearing half. Determinism is CORRECT only for
// an event that describes a mutation happening once; applying it to a repeatable
// event would collapse every later occurrence into a duplicate the unique index
// rejects, and the pipeline would stop delivering those events with no error
// anywhere. So this function's job is to say "yes, and here is the identity" or
// "no, this event has no stable identity" — never to guess.
//
// # Derivable, because the mutation happens once
//
//	*model.Transaction / model.Transaction   TransactionID
//	    A status transition happens once per transaction row. A partial commit of an
//	    inflight transaction creates a CHILD transaction with its own id, so its
//	    events carry that child's identity and cannot collide with the parent's.
//	*model.Ledger / model.Ledger             LedgerID
//	*model.Identity / model.Identity         IdentityID
//	*model.Balance / model.Balance           BalanceID
//	    A create happens once per record.
//	map[string]interface{}                   batch_id, for bulk_transaction.* only
//	    One terminal status per batch.
//
// # NOT derivable, because the event repeats
//
//	*model.BalanceMonitor / model.BalanceMonitor
//	    A monitor fires every time its condition is met. Deriving from the monitor id
//	    would deliver the first alert and silently discard every one after it.
//	everything else, including system.error and an unrecognised payload
//	    Emitted per occurrence. Two identical error messages a second apart are two
//	    events an operator needs to see twice.
//
// Parameters:
//   - eventType string: the event name, which selects the map interpretation.
//   - payload interface{}: the domain object the event carries. May be nil.
//
// Returns:
//   - string: the mutation identity, trimmed. Empty when there is none.
//   - bool: true when a deterministic id may be derived from it.
func EventIdentityFor(eventType string, payload interface{}) (string, bool) {
	identity := ""

	switch typed := payload.(type) {
	case *Transaction:
		if typed != nil {
			identity = typed.TransactionID
		}
	case Transaction:
		identity = typed.TransactionID
	case *Ledger:
		if typed != nil {
			identity = typed.LedgerID
		}
	case Ledger:
		identity = typed.LedgerID
	case *Identity:
		if typed != nil {
			identity = typed.IdentityID
		}
	case Identity:
		identity = typed.IdentityID
	case *Balance:
		if typed != nil {
			identity = typed.BalanceID
		}
	case Balance:
		identity = typed.BalanceID
	case map[string]interface{}:
		// A map payload is only a batch when the event type says so. Reading
		// batch_id out of some other map-shaped payload would derive an id from a
		// field that means something else entirely.
		if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
			if value, ok := typed["batch_id"].(string); ok {
				identity = value
			}
		}
	}

	identity = strings.TrimSpace(identity)

	return identity, identity != ""
}

// EventTypeIsRepeatable reports whether an event type is emitted more than once for one
// subject, and therefore must never carry a derived id.
//
// It is the event-type half of the same decision EventIdentityFor makes from the payload,
// exposed for the callers that have only the name — a metrics label, a triage query, a
// test. The two must agree, and they do because both are stated from the same list:
//
//   - balance.monitor fires every time its condition is met.
//   - system.error is emitted per occurrence.
//
// Everything else describes a mutation that happens once. A caller must not read a false
// answer here as "derivable", though: whether an id CAN be derived also depends on the
// payload actually carrying an identity, which only EventIdentityFor can say.
//
// Parameters:
//   - eventType string: the event name.
//
// Returns:
//   - bool: true when the event repeats for one subject.
func EventTypeIsRepeatable(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case EventTypeBalanceMonitor, EventTypeSystemError:
		return true
	default:
		return false
	}
}

// GenerateSubscriberID mints a subscriber business identifier.
//
// The value it produces is canonical by construction — lowercase, prefixed, and otherwise a
// UUID — so it passes CanonicalizeSubscriberIdentifier and therefore the schema's
// event_subscribers_subscriber_id_chk. That matters more than it looks: the principal and the
// consumer group are DERIVED from this value, so an identifier that cannot be canonicalized
// cannot be provisioned at all.
//
// It is opaque on purpose. A human-readable identifier ends up naming the customer, and the
// identifier is copied verbatim into a Kafka principal and a consumer group id, both of which
// are visible to every operator with broker access and appear in broker logs.
//
// Returns:
//   - string: "sub_<uuid>".
func GenerateSubscriberID() string {
	return GenerateUUIDWithSuffix(SubscriberIDPrefix)
}

// GenerateConsumerGroupID mints a consumer group id inside a freshly generated subscriber's
// namespace.
//
// Prefer CanonicalConsumerGroupID when the subscriber identifier is already known: a group
// derived from the SAME identifier as the principal is what the schema's
// event_subscribers_group_derived_chk requires of a stored row. This no-argument form exists
// for the callers that need a well-shaped group id without a subscriber to derive it from —
// a lag-measurement fixture, a smoke check — and its shape is identical.
//
// Returns:
//   - string: "blnk-sub-<generated identifier>.default".
func GenerateConsumerGroupID() string {
	group, err := CanonicalConsumerGroupID(GenerateSubscriberID())
	if err != nil {
		// Unreachable: GenerateSubscriberID is canonical by construction. Falling back to
		// the namespace of a fixed leaf keeps the shape valid rather than returning "",
		// which would read as "no group" to every caller.
		return ConsumerGroupIDPrefix + SubscriberIDPrefix + SubscriberGroupTerminator + SubscriberDefaultGroupLeaf
	}

	return group
}

// isLegalKafkaTopicName reports whether every character is one Kafka permits in a topic name.
//
// Kafka permits only [a-zA-Z0-9._-]. Anything else cannot name a real topic, so it is either
// a mistake or an attempt to smuggle a separator, a quote or a brace into a value that is
// about to be composed into an array literal and an authorization rule.
//
// Parameters:
//   - name string: the candidate topic name.
//
// Returns:
//   - bool: true when every character is legal. An empty name is vacuously legal and is
//     rejected by the blank check instead, which produces a better message.
func isLegalKafkaTopicName(name string) bool {
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}

	return true
}

// ValidateSubscriberTopics checks a topic grant against the bounds the registry stores it
// under, and is the SINGLE definition of those bounds.
//
// It is the RESOURCE half of grant validation; IsSubscriberGrantableTopicName is the
// AUTHORIZATION half. Both are needed and neither subsumes the other: the allowlist decides
// which names may be granted at all, and these bounds decide how much of anything one
// request may ask for, which still has to hold if the allowlist ever grows.
//
// The empty grant is VALID and must stay valid: the column is NOT NULL DEFAULT '{}' precisely
// so that a registered-but-unprovisioned subscriber is authorised for nothing, and rejecting
// it here would make that legitimate state unrepresentable.
//
// Parameters:
//   - topics []string: the grant to check. Nil and empty are both accepted.
//
// Returns:
//   - error: nil when the grant is within every bound, otherwise a plain error naming the
//     offending rule and, where it helps, the offending element. The caller wraps it in
//     whichever typed error its layer answers with.
func ValidateSubscriberTopics(topics []string) error {
	if len(topics) > MaxSubscriberTopics {
		return fmt.Errorf(
			"a subscriber may be authorised for at most %d topics, got %d",
			MaxSubscriberTopics, len(topics))
	}

	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return errors.New("an authorised topic must not be blank")
		}

		if len(topic) > MaxTopicNameLength {
			return fmt.Errorf(
				"an authorised topic must be at most %d characters, got %d",
				MaxTopicNameLength, len(topic))
		}

		if !isLegalKafkaTopicName(topic) {
			return fmt.Errorf(
				"authorised topic %q contains a character Kafka does not permit in a topic name; "+
					"only letters, digits, dots, underscores and hyphens are allowed", topic)
		}

		if _, duplicate := seen[topic]; duplicate {
			return fmt.Errorf("authorised topic %q is listed more than once", topic)
		}

		seen[topic] = struct{}{}
	}

	return nil
}

// -----------------------------------------------------------------------------
// Webhook destination policy (SSRF-01)
// -----------------------------------------------------------------------------
//
// ONE implementation of "is this address somewhere Blnk must not send to", shared by
// every layer that asks the question.
//
// There are two layers, and they ask at different moments about different things:
//
//   - ADMISSION TIME, in api/model, about the TEXT an operator supplied. It can only
//     judge a literal — a hostname's resolution is not knowable when a row is written.
//   - SEND TIME, in the legacy webhook transport, about the RESOLVED ADDRESS the dialer
//     is about to connect to. That is the only place DNS rebinding is visible, because
//     it is the only place the actual address is known.
//
// They live in different packages and neither can import the other, so the policy lives
// here in the package both already depend on. That placement is the whole point: two
// copies of an address deny-list drift, and the drift is silent. A range added to the
// admission check and forgotten at send time reads as defence-in-depth and is in fact a
// hole, because the send-time check is the one an attacker actually has to get past.
//
// # What is refused, and why each range is on the list
//
// The stdlib predicates cover most of it (loopback, link-local, private, unspecified,
// multicast — including the IPv4-mapped IPv6 spellings, since net.IP normalises those).
// Four ranges the stdlib does NOT classify are added explicitly because each is a
// documented SSRF bypass rather than a theoretical one:
//
//   - 100.64.0.0/10 (RFC 6598 carrier-grade NAT). Cloud providers put internal
//     infrastructure here, notably the GKE metadata proxy, precisely because it is
//     neither public nor RFC1918 and so slips past deny-lists built from IsPrivate.
//   - 192.0.0.0/24 (IETF protocol assignments) and 198.18.0.0/15 (benchmarking).
//     Neither is routable on the public internet, so nothing legitimate is reachable
//     there and anything answering is inside the network.
//   - 64:ff9b::/96 (RFC 6052 NAT64 well-known prefix). This EMBEDS an IPv4 address in
//     the low 32 bits, so 64:ff9b::7f00:1 reaches 127.0.0.1 through a NAT64 gateway
//     while every IPv6 predicate reports a perfectly ordinary global unicast address.
//     The embedded address is unwrapped and judged on its own merits.
//
// IsGlobalUnicast is then used as a closing backstop, so an address family the list
// above does not enumerate is refused rather than allowed by omission.

// nat64WellKnownPrefixLen is the length in bytes of the RFC 6052 well-known prefix
// (64:ff9b::/96 — twelve bytes) after which an embedded IPv4 address begins.
const nat64WellKnownPrefixLen = 12

// nat64WellKnownPrefix is the first twelve bytes of 64:ff9b::/96.
var nat64WellKnownPrefix = [nat64WellKnownPrefixLen]byte{
	0x00, 0x64, 0xff, 0x9b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

// extraInternalRanges are the CIDRs the net.IP predicates do not classify but that are
// nonetheless unreachable on the public internet, each paired with the reason reported
// when an address falls inside it.
var extraInternalRanges = []struct {
	cidr   string
	reason string
	parsed *net.IPNet
}{
	{
		cidr: "100.64.0.0/10",
		reason: "it is in the carrier-grade NAT range, where cloud providers host " +
			"internal infrastructure such as the metadata proxy",
	},
	{cidr: "192.0.0.0/24", reason: "it is in the IETF protocol-assignment range, which is not publicly routable"},
	{cidr: "198.18.0.0/15", reason: "it is in the benchmarking range, which is not publicly routable"},
}

// init parses the extra ranges once, at package load, so the hot path never parses a
// CIDR string. A malformed constant here would be a programming error rather than a
// runtime condition, so parse failure panics at load rather than degrading the policy
// silently at send time — a deny-list that quietly stopped covering a range is the one
// failure mode this whole section exists to prevent.
func init() {
	for i := range extraInternalRanges {
		_, network, err := net.ParseCIDR(extraInternalRanges[i].cidr)
		if err != nil {
			panic("model: malformed internal destination CIDR " + extraInternalRanges[i].cidr + ": " + err.Error())
		}

		extraInternalRanges[i].parsed = network
	}
}

// InternalIPReason reports why an IP address is an internal destination, or "" when it
// is one Blnk may send to.
//
// A REASON rather than a boolean, because the caller has to be able to say what it
// refused: "not allowed" sends an operator hunting for a policy document, while "it is a
// link-local address, which reaches the cloud instance metadata endpoint" tells them
// exactly what they just pointed Blnk at.
//
// Parameters:
//   - address net.IP: the address, as parsed. A nil or malformed address is refused,
//     because a caller that could not parse what it is about to dial must not proceed.
//
// Returns:
//   - string: the reason, or "" when the address is acceptable.
func InternalIPReason(address net.IP) string {
	if address == nil {
		return "it is not a valid IP address"
	}

	// NAT64 first, and deliberately so: the embedded address is what the packet
	// ultimately reaches, and every predicate below would describe the wrapper as
	// ordinary global unicast.
	if embedded := nat64EmbeddedIPv4(address); embedded != nil {
		if reason := InternalIPReason(embedded); reason != "" {
			return "it embeds an internal IPv4 address in the NAT64 well-known prefix, and " + reason
		}
	}

	// Ordered so that the reason REPORTED is the accurate one, not merely a refusal. The
	// pairs overlap: 224.0.0.0/24 and ff02::/16 are both link-local AND multicast, and
	// describing a multicast group as "the cloud instance metadata endpoint" would send an
	// operator to investigate the wrong thing. Unicast link-local is checked on its own, and
	// every remaining multicast form — link-local, interface-local, global — falls to the
	// multicast arm. Coverage is identical either way; only the diagnosis changes.
	switch {
	case address.IsLoopback():
		return "it is a loopback address"
	case address.IsLinkLocalUnicast():
		return "it is a link-local address, which reaches the cloud instance metadata endpoint"
	case address.IsMulticast():
		return "it is a multicast address"
	case address.IsPrivate():
		return "it is a private address inside Blnk's own network"
	case address.IsUnspecified():
		return "it is the unspecified address"
	}

	for _, candidate := range extraInternalRanges {
		if candidate.parsed != nil && candidate.parsed.Contains(address) {
			return candidate.reason
		}
	}

	// The backstop. Anything that is not global unicast after the enumerated checks is
	// refused on the strength of not being an address the public internet can route.
	if !address.IsGlobalUnicast() {
		return "it is not a globally routable address"
	}

	return ""
}

// nat64EmbeddedIPv4 returns the IPv4 address embedded in an RFC 6052 well-known-prefix
// NAT64 address, or nil when the address is not one.
//
// Parameters:
//   - address net.IP: the address to unwrap.
//
// Returns:
//   - net.IP: the embedded four-byte address, or nil.
func nat64EmbeddedIPv4(address net.IP) net.IP {
	// Only a true 16-byte form can carry the prefix. To4 returning non-nil means the
	// value is an IPv4 address (or its mapped spelling), which cannot be NAT64.
	if address.To4() != nil {
		return nil
	}

	sixteen := address.To16()
	if sixteen == nil {
		return nil
	}

	for i := 0; i < nat64WellKnownPrefixLen; i++ {
		if sixteen[i] != nat64WellKnownPrefix[i] {
			return nil
		}
	}

	return net.IPv4(sixteen[12], sixteen[13], sixteen[14], sixteen[15])
}

// InternalDestinationReason reports why a hostname or IP literal is an internal
// destination, or "" when it is not.
//
// IP literals are decided on the parsed address by InternalIPReason, never on the text,
// so "::ffff:127.0.0.1" is recognised as loopback rather than passing as an
// unfamiliar-looking string. Names are decided on the four shapes that can only resolve
// inside the network Blnk runs in.
//
// This is a LITERAL check and is deliberately not sold as more than one: a public
// hostname resolving to an internal address is not detectable here, which is exactly why
// the send-time dialer judges the resolved address as well.
//
// Parameters:
//   - host string: the hostname or IP literal, without a port.
//
// Returns:
//   - string: the reason, or "" when the host is acceptable.
func InternalDestinationReason(host string) string {
	if address := net.ParseIP(host); address != nil {
		return InternalIPReason(address)
	}

	lowered := strings.ToLower(host)

	if lowered == "localhost" || strings.HasSuffix(lowered, ".localhost") {
		return "it resolves to loopback"
	}

	// .local is mDNS and .internal is the conventional private zone — and the name
	// metadata.google.internal is one of the two best-known metadata endpoints.
	if strings.HasSuffix(lowered, ".local") || strings.HasSuffix(lowered, ".internal") {
		return "it is an internal-only hostname"
	}

	// An unqualified single-label name can only resolve through a local search domain,
	// which is by definition inside the network Blnk runs in.
	if !strings.Contains(lowered, ".") {
		return "it is an unqualified hostname that can only resolve inside Blnk's own network"
	}

	return ""
}

// OperatorOwnableInternalIP reports whether an internal address is one an operator can
// plausibly own and legitimately point a webhook at.
//
// # Why the deny-list is not uniform
//
// Refusing every internal address outright would be simpler and wrong. An on-premise
// deployment delivering to https://webhooks.corp:8443 behind RFC1918 is a completely
// legitimate configuration, and so is a developer delivering to a loopback test sink.
// Those are the operator's own network, and the operator is the one who wrote the
// configuration.
//
// The metadata endpoint is not. Nobody legitimately webhooks to 169.254.169.254, the
// unspecified address, a multicast group, the benchmarking range or a NAT64-wrapped
// loopback address — every one of those is either an attack or a mistake. So the
// destination policy has TWO tiers: ranges an operator may opt back into by asserting
// ownership of the network, and ranges no configuration can re-enable.
//
// This function draws that line. It says nothing about whether the address is allowed;
// it says whether an operator's explicit assertion is capable of allowing it.
//
// Parameters:
//   - address net.IP: the address already known to be internal.
//
// Returns:
//   - bool: true when an explicit operator assertion may permit this address.
func OperatorOwnableInternalIP(address net.IP) bool {
	if address == nil {
		return false
	}

	// A NAT64-wrapped internal address is never operator-ownable. An operator who owns
	// 10.0.0.0/8 writes 10.x.y.z; reaching it through 64:ff9b:: is a bypass attempt
	// dressed as a global unicast address.
	if nat64EmbeddedIPv4(address) != nil {
		return false
	}

	return address.IsLoopback() || address.IsPrivate()
}

// BalanceMonitorHandoff is one row of blnk.balance_monitor_handoff: the durable
// intent that a balance moved and its monitors have not been evaluated yet.
//
// # What it is for
//
// It is the mechanism that brings `balance.monitor` under requirement R-2. A monitor
// fires because a CONDITION was met on a balance a transaction already committed, so
// the alert cannot be captured inside the mutation that caused it — by the time the
// alert exists, that transaction is gone. What CAN be captured inside the mutation is
// the intent to evaluate, and that is this row. It is written by the same database
// transaction that writes the balance, so a committed movement always carries its
// pending evaluation and a rolled-back movement carries none.
//
// The evaluation's RESULT is then captured transactionally too: the event rows it
// produces and this row's transition to a terminal status commit together. So the
// sequence is at-least-once evaluation feeding an atomic capture, and the only way to
// lose an alert is for the condition never to have been met.
//
// # Why the balance is carried as a snapshot
//
// BalanceSnapshot holds the balance exactly as the transaction wrote it. The
// evaluation must judge THAT state: re-reading the balance later would judge whatever
// subsequent transactions had done to it, so a threshold crossed by this movement and
// uncrossed by the next would produce no alert, and two attempts at the same handoff
// could reach different verdicts. Snapshotting makes the evaluation deterministic,
// makes a retry idempotent in outcome, and lets the evaluator run in a different
// process from the writer.
//
// The field is json.RawMessage rather than a *Balance so the row can be carried,
// claimed and requeued without paying an unmarshal that only the evaluator needs. Use
// Balance to decode it.
//
// The field names correspond one-to-one with the table's columns; the repository scans
// rows directly into this struct, so the two must stay aligned.
type BalanceMonitorHandoff struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database.
	ID int64 `json:"id"`
	// HandoffID is the business key, and it carries a unique index.
	//
	// It is GENERATED per handoff rather than derived from the balance, because one
	// balance legitimately produces many handoffs — one per movement — and a derived
	// key would collapse every movement after the first into a duplicate the index
	// rejects, silently stopping the alerts for exactly the balances that move most.
	HandoffID string `json:"handoff_id"`
	// BalanceID is the balance whose monitors are to be evaluated.
	BalanceID string `json:"balance_id"`
	// LedgerID is the ledger the balance belongs to, carried so the resulting event
	// can be attributed to it without a second read. A balance always has one, so
	// this is empty only on a row written from an incomplete snapshot.
	LedgerID string `json:"ledger_id,omitempty"`
	// BalanceSnapshot is the balance as the transaction wrote it, marshalled.
	BalanceSnapshot json.RawMessage `json:"balance_snapshot"`
	// Status is the relay state machine: pending, processing, completed or failed.
	// The values are the OutboxStatus* constants, shared with the two existing
	// outboxes rather than duplicated, so one vocabulary describes all three.
	Status string `json:"status"`
	// Attempts is how many evaluation attempts this row has had.
	Attempts int `json:"attempts"`
	// MaxAttempts is the budget beyond which the row is marked failed.
	MaxAttempts int `json:"max_attempts"`
	// LastError is the most recent failure reason, empty when there has been none.
	LastError string `json:"last_error,omitempty"`
	// EventsCaptured is how many balance.monitor events the completed evaluation
	// produced. Zero is the common and correct answer: it means the monitors were
	// evaluated and none of their conditions was met.
	//
	// It is recorded rather than inferred because "evaluated, nothing fired" and
	// "never evaluated" are indistinguishable from the event outbox alone, and the
	// difference is the whole question an operator asks when an expected alert did
	// not arrive.
	EventsCaptured int `json:"events_captured"`
	// CreatedAt is when the balance's transaction committed this row.
	CreatedAt time.Time `json:"created_at"`
	// ProcessedAt is when the row reached a terminal status, nil before that.
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	// LockedUntil is the claim lease expiry, nil when unclaimed.
	LockedUntil *time.Time `json:"locked_until,omitempty"`
}

// Balance decodes the snapshot this handoff carries.
//
// The returned balance is the state the monitor conditions must be evaluated against —
// see the note on BalanceSnapshot for why it is this state and not the current one.
//
// Returns:
//   - *Balance: the decoded snapshot.
//   - error: a decode failure, which means the row is not evaluable and should be
//     failed rather than retried; re-reading the same bytes cannot succeed later.
func (h *BalanceMonitorHandoff) Balance() (*Balance, error) {
	if h == nil || len(h.BalanceSnapshot) == 0 {
		return nil, fmt.Errorf("balance monitor handoff carries no balance snapshot")
	}

	balance := &Balance{}
	if err := json.Unmarshal(h.BalanceSnapshot, balance); err != nil {
		return nil, fmt.Errorf("failed to decode the balance snapshot: %w", err)
	}

	return balance, nil
}

// balanceMonitorHandoffPrefix is the identifier prefix for a handoff, following the
// repository's `<module>_<uuid>` convention so an id is self-describing in a log line.
const balanceMonitorHandoffPrefix = "bmh"

// PrepareBalanceMonitorHandoffs builds one handoff per supplied balance.
//
// It is the model-layer half of the atomic capture: the repository writes what this
// returns inside the balance's own transaction. It lives here rather than in the
// repository because the snapshot IS the balance's serialised form, and the model owns
// what a balance serialises to.
//
// # What is skipped, and why nothing is rejected
//
// A nil balance and a balance with a blank id are skipped. The writers call this
// unconditionally for every balance they update, and a ledger movement must never be
// refused because an alerting side effect could not be described. A marshal failure IS
// returned, because it means the balance itself does not serialise — a fact the caller
// needs, and one that cannot be worked around by omitting the handoff.
//
// # The snapshot is the balance AS PASSED
//
// The writers hand over balances in their post-mutation state, which is the state the
// monitor conditions must be judged against. Marshalling here — before the transaction
// commits — captures exactly that state, so the evaluation later judges the movement
// that produced the handoff rather than whatever the balance has become since.
//
// Parameters:
//   - balances []*model.Balance: the balances being updated, in post-mutation state.
//
// Returns:
//   - []*BalanceMonitorHandoff: one handoff per usable balance, in input order.
//   - error: a marshal failure on any balance.
func PrepareBalanceMonitorHandoffs(balances []*Balance) ([]*BalanceMonitorHandoff, error) {
	if len(balances) == 0 {
		return nil, nil
	}

	handoffs := make([]*BalanceMonitorHandoff, 0, len(balances))
	for _, balance := range balances {
		if balance == nil || strings.TrimSpace(balance.BalanceID) == "" {
			continue
		}

		snapshot, err := json.Marshal(balance)
		if err != nil {
			return nil, fmt.Errorf("failed to snapshot balance %s for monitor evaluation: %w", balance.BalanceID, err)
		}

		handoffs = append(handoffs, &BalanceMonitorHandoff{
			HandoffID:       GenerateUUIDWithSuffix(balanceMonitorHandoffPrefix),
			BalanceID:       strings.TrimSpace(balance.BalanceID),
			LedgerID:        strings.TrimSpace(balance.LedgerID),
			BalanceSnapshot: snapshot,
			Status:          OutboxStatusPending,
		})
	}

	return handoffs, nil
}

// BalanceMonitorEventIdentity is the stable identity of the alert one monitor produces
// for one balance movement.
//
// # Why balance.monitor can be derived after all, given the right identity
//
// The event catalogue records balance.monitor as NOT derivable, and that is correct for
// the identity that was available before the handoff existed: a monitor id alone. A
// monitor fires every time its condition is met, so deriving from the monitor would
// collapse every firing after the first into a duplicate the unique index rejects, and
// the alerts would silently stop.
//
// The handoff supplies the missing half. Each balance movement gets its own handoff id,
// so (handoff, monitor) names exactly one alert: distinct across firings, because each
// firing has a different handoff, and stable across re-evaluations of the same handoff,
// because the handoff id does not change.
//
// That stability is what makes the evaluation safe to repeat. A lapsed claim lease can
// let two processors evaluate one handoff, and a retry can re-run one that already
// committed; in both cases the second attempt derives the SAME event ids and collides
// with the unique index instead of duplicating the alert. The duplicate is absorbed by
// the schema rather than by a lock.
//
// Parameters:
//   - handoffID string: the handoff the evaluation belongs to.
//   - monitorID string: the monitor whose condition was met.
//
// Returns:
//   - string: the identity to pass to DeriveEventID, empty when either part is missing,
//     which the caller must treat as "not derivable" rather than deriving from half an
//     identity.
func BalanceMonitorEventIdentity(handoffID, monitorID string) string {
	handoff := strings.TrimSpace(handoffID)
	monitor := strings.TrimSpace(monitorID)
	if handoff == "" || monitor == "" {
		return ""
	}

	// A colon cannot appear in a UUID or in one of this repository's prefixed
	// identifiers, so no two different pairs can produce the same joined string.
	return handoff + ":" + monitor
}

// Bulk batch coordinator statuses.
//
// 'processing' is the single non-terminal value and is what a row is inserted with,
// before any member transaction runs. The other three are the outcome names the
// `bulk_transaction.<status>` event already uses as its suffix, so the coordinator's
// status and the event's name are the same word rather than two vocabularies that can
// drift apart.
const (
	// BulkBatchStatusProcessing means the batch began and has not reported an
	// outcome. A row that stays here is the one residue the coordinator cannot
	// remove: the process handling the batch died before it could finalise.
	BulkBatchStatusProcessing = "processing"
	// BulkBatchStatusApplied is a batch whose transactions were all applied.
	BulkBatchStatusApplied = "applied"
	// BulkBatchStatusInflight is a batch whose transactions were all left inflight.
	BulkBatchStatusInflight = "inflight"
	// BulkBatchStatusFailed is a batch that failed, with error_message carrying the
	// failure and any rollback detail.
	BulkBatchStatusFailed = "failed"
)

// BulkTransactionBatch is one row of blnk.bulk_transaction_batches: the durable
// coordinator record for an asynchronous bulk transaction batch.
//
// # What it is for
//
// It is the mechanism that brings `bulk_transaction.<status>` under requirement R-2,
// and it works differently from the monitor handoff because the problem is different.
// A bulk batch has no batch-spanning database transaction — every member transaction
// commits under its own — so there is no mutation the summary event could join. The
// outcome therefore had nowhere durable to live: it existed only in the local
// variables of the goroutine that computed it, and a failed capture destroyed it
// permanently.
//
// This row gives the outcome a home, and the finalising transaction writes the outcome
// and the event TOGETHER. Two facts follow, and they are the guarantee:
//
//   - the event cannot be missing while the outcome is recorded, and
//   - the outcome cannot be recorded while the event is missing.
//
// A crash before the finalise leaves Status at 'processing', which is a visible,
// queryable, counted state meaning "this batch began and never reported an outcome" —
// materially different from an outcome that was declared and then evaporated.
type BulkTransactionBatch struct {
	// BatchID is the parent transaction id of the batch and the primary key. It is
	// also the event's aggregate id, so the coordinator row and the event it
	// produced are joinable on it.
	BatchID string `json:"batch_id"`
	// Status is one of the BulkBatchStatus* constants.
	Status string `json:"status"`
	// TransactionCount is the number of transactions in the batch. It is zero on a
	// failed batch, matching the payload the failure path has always sent.
	TransactionCount int `json:"transaction_count"`
	// ErrorMessage is the failure detail including rollback status, empty on success.
	ErrorMessage string `json:"error_message,omitempty"`
	// Atomic records whether a failure rolls the whole batch back, and Inflight
	// whether its transactions are left inflight. Both are carried so a stuck row
	// tells an operator what the batch was ATTEMPTING, which is what decides how to
	// finish it by hand.
	Atomic   bool `json:"atomic"`
	Inflight bool `json:"inflight"`
	// EventID is the outbox event that recorded this outcome, empty until finalised.
	// It is the proof of the atomicity: a terminal row without one would mean the
	// outcome was recorded without its event, which the finalising transaction makes
	// unreachable.
	EventID string `json:"event_id,omitempty"`
	// CreatedAt is when the batch began.
	CreatedAt time.Time `json:"created_at"`
	// FinalizedAt is when the outcome became terminal, nil before that. The schema
	// constrains this to be non-nil exactly when Status is terminal.
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
}

// IsTerminal reports whether this batch has reported an outcome.
//
// Returns:
//   - bool: false only for BulkBatchStatusProcessing.
func (b *BulkTransactionBatch) IsTerminal() bool {
	if b == nil {
		return false
	}

	return IsTerminalBulkBatchStatus(b.Status)
}

// IsTerminalBulkBatchStatus reports whether a coordinator status is an outcome.
//
// It exists as a free function because the repository's conditional finalise needs the
// answer for a status it has read but not yet built a struct around.
//
// Parameters:
//   - status string: the status to classify. Unknown values answer false, which is the
//     conservative direction: an unrecognised status is treated as still in progress
//     rather than as an outcome that may be overwritten.
//
// Returns:
//   - bool: true for applied, inflight and failed.
func IsTerminalBulkBatchStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case BulkBatchStatusApplied, BulkBatchStatusInflight, BulkBatchStatusFailed:
		return true
	default:
		return false
	}
}

// UncorroboratedRows is how many claims of publication could not be placed inside a
// measured window, for any reason.
//
// Returns:
//   - int64: never negative.
func (a EventRecordIntervalAudit) UncorroboratedRows() int64 {
	uncorroborated := a.UnconfirmedRows + a.UnmeasuredRows + a.AgedOutRows + a.BeyondEndRows
	if uncorroborated < 0 {
		return 0
	}

	return uncorroborated
}

// EventOutboxAudit is the outbox side of the zero-loss reconciliation: how many rows
// claim a Kafka record, and how many of those can actually name the record they
// produced.
//
// # Why "published" is not the same as "terminal"
//
// PublishedRows deliberately counts a wider set than the terminal statuses. A row in
// webhook_pending HAS been published to Kafka — its Kafka leg completed and
// KafkaDispatchedAt is stamped; what remains outstanding is the deprecated HTTP leg.
// Counting only dispatched and dead_lettered rows would leave those records
// unaccounted for on the broker side, inflating the apparent surplus and making the
// reconciliation looser precisely during the dual-delivery window, which is when it is
// most needed.
//
// # What the split is for
//
// ConfirmedRows is the count that can be MATCHED to a record. UnconfirmedRows is the
// count that cannot, and its existence is the finding this type closes: an unconfirmed
// row is a claim of publication that nothing corroborates, and under a pure count it
// was invisible because a redelivery elsewhere could make the totals balance.
type EventOutboxAudit struct {
	// PublishedRows is how many rows claim a record on the broker: every row whose
	// Kafka leg completed, plus every dead-lettered row, each counted exactly once
	// by virtue of the unique index on event_id.
	PublishedRows int64

	// ConfirmedRows is how many of those name the record they produced.
	ConfirmedRows int64

	// DistinctRecords is how many DISTINCT coordinates those rows name. It equals
	// ConfirmedRows unless two rows claim the same record, which the partial unique
	// index on the coordinate makes impossible — so a discrepancy here means the
	// index is missing or has been dropped, and the audit says so rather than
	// assuming the schema is intact.
	DistinctRecords int64

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	//
	// # PERF-P05: the audit is WINDOWED, and it has to be
	//
	// The counts are one half of a comparison whose other half is a Kafka offset
	// reading, and the two halves must describe the SAME population or the comparison
	// means nothing. Over the whole history they cannot: broker end offsets are
	// cumulative for the life of a topic and are unaffected by Kafka retention, while
	// the outbox's retention sweep DELETES terminal rows — so the longer a deployment
	// runs with retention enabled, the further the outbox side falls behind a broker
	// side that never forgets, and the "surplus" the verdict tolerates grows without
	// bound until it can hide any amount of loss.
	//
	// A shared window fixes both directions at once: rows whose publication instant
	// falls inside it, against records the broker wrote inside it. Retention shorter
	// than the window is then the only thing that can invalidate the reading, and it is
	// detectable rather than silent — see the truncation flag on the offset report.
	//
	// It is also what makes the query bounded. At 500 events per second the table grows
	// by 43.2 million rows a day, so an exact whole-history aggregate is a scan whose
	// cost rises for ever while answering a question about the last day.
	//
	// The zero value means the counts are whole-history, which is a diagnostic reading
	// only: no reconciliation verdict may be drawn from it.
	WindowStart time.Time
}

// UnconfirmedRows is how many rows claim a publication they cannot name a record for.
//
// Returns:
//   - int64: never negative.
func (a EventOutboxAudit) UnconfirmedRows() int64 {
	if a.ConfirmedRows >= a.PublishedRows {
		return 0
	}

	return a.PublishedRows - a.ConfirmedRows
}

// FullyConfirmed reports whether every row claiming a publication names a distinct
// record.
//
// It is the precondition for a conclusive zero-loss verdict: with it true, a shortfall
// in broker records cannot be hidden by a surplus, because each claim is individually
// accounted for.
//
// Returns:
//   - bool: true when nothing is unconfirmed and no two rows share a coordinate.
func (a EventOutboxAudit) FullyConfirmed() bool {
	return a.UnconfirmedRows() == 0 && a.DistinctRecords == a.ConfirmedRows
}

// DeclaresUnenforceableIsolation reports whether this subscriber's record asks for an
// access boundary that nothing in the system can enforce.
//
// # The boundary that does not exist
//
// A non-nil PartitionKeyPrefix says "this subscriber may see only the records whose
// partition key starts with this". Kafka has no mechanism for that: an ACL names a
// topic, and a principal granted a topic reads every record on it. So a credential
// issued to a subscriber that declares a prefix hands out access strictly wider than
// the record it was issued against describes — and on a shared category topic that
// wider access is every other subscriber's transactions, balances and identities.
//
// The failure mode is not a missing feature, it is a false assurance. An operator reads
// the registry row, sees the prefix, and concludes the subscriber is confined to its own
// records. Nothing anywhere contradicts them, because the value is accepted, stored,
// echoed back and never enforced.
//
// So the value is still STORED — it is a real statement of intent, and erasing it would
// destroy the record of what an operator asked for — and credential issuance REFUSES
// while it is present. The remedy is explicit rather than silent: clear the prefix to
// accept topic-level scope, which is the boundary Kafka ACLs can actually hold, or narrow
// authorized_topics until topic-level scope IS the isolation required.
//
// Returns:
//   - bool: true when a partition-key prefix is recorded.
func (s *EventSubscriber) DeclaresUnenforceableIsolation() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// DeclaresKeyScope reports whether this subscriber records a partition-key scope.
//
// # What the property is, and where it is checked
//
// A non-nil PartitionKeyPrefix says "this subscriber is entitled only to the records
// whose partition key starts with this". Because Blnk keys every event by ledger id,
// that is a ledger boundary expressed in the value the transport already carries.
//
// THE BROKER DOES NOT CHECK IT. Kafka's authorizer names a topic or a group; it has no
// message-key dimension, so a principal granted a shared category topic reads every
// record on it. The scope is therefore delivered to the consumer — returned by the
// credential endpoint alongside the enforced scopes and labelled as the one the consumer
// applies — and HasKeyAccess is the rule it means.
//
// This predicate is what reporting, the credential contract and the operations runbook
// read to tell a key-scoped subscriber from a whole-topic one. IT IS NOT A REFUSAL.
// Issuance once failed closed on it, which kept the registry honest by declining the
// access model instead of implementing it; see the access-model note on EventSubscriber
// for why disclosure at issuance replaced that.
//
// The empty string and whitespace are treated as absent, for the same reason the column
// is nullable: "a constraint on the empty prefix" is not an intent anybody has, and
// reading it as one would attach a boundary to a subscriber that asked for none.
//
// A nil receiver answers false. It is read as a guard on a row that may not have been
// found, so it must be answerable on nothing.
//
// Returns:
//   - bool: true when a non-blank partition-key prefix is recorded.
func (s *EventSubscriber) DeclaresKeyScope() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// There is no internalEventCategories set and no IsInternalEventCategory predicate, and the
// absence is a decision rather than an omission.
//
// A category-level exclusion was introduced to keep system.error away from subscribers, and it
// would also have withheld `ledger.created`, because the four-category layout puts that event on
// the same topic. That trades a disclosure for a lost event: `ledger.created` is delivered by the
// legacy webhook transport today, so a subscriber migrating to Kafka would simply stop receiving
// it, with an allowlist saying nothing was wrong.
//
// The concern it was reaching for is real and is answered where the decision belongs — per
// subscriber, in authorized_topics, with what a grant of the system category discloses documented
// on EventCategorySystem. Every category is grantable; see SubscriberGrantableEventCategories.

// KeyScopeEnforcementStatus names WHERE a subscriber's key scope is enforced.
//
// It is a string rather than a bool because the honest answer is a place and not a yes or
// no, and because it is serialised into the credential response, where a client reads it
// to decide whether it has filtering of its own to do. A bool called "enforced" was the
// shape that preceded it and it could only ever be false, which is how it came to be read
// as "unenforceable, therefore refuse".
type KeyScopeEnforcementStatus string

const (
	// KeyScopeEnforcementNone is reported when no key scope is recorded: the
	// broker-enforced topic and consumer-group ACLs are the subscriber's entire
	// boundary and there is nothing left for a consumer to filter.
	KeyScopeEnforcementNone KeyScopeEnforcementStatus = "none"

	// KeyScopeEnforcementConsumerSide is reported when a key scope IS recorded. The
	// broker grants Read on whole topics — it has no message-key dimension — so the
	// scope is honoured by the consumer applying HasKeyAccess to each record's key.
	// A subscriber that ignores it will see records outside its scope, which is why
	// this value is disclosed rather than inferred.
	KeyScopeEnforcementConsumerSide KeyScopeEnforcementStatus = "consumer_side"
)

// KeyScopeEnforcement reports where this subscriber's key scope is enforced.
//
// This is the accessor that replaced a boolean predicate named for unenforceability, which
// has since been deleted outright. The difference is not cosmetic: the old name asserted
// that a recorded prefix could not be honoured, and three separate barriers — issuance,
// update and a CHECK constraint — read it as licence to deny the subscriber a credential
// entirely. Reporting the enforcement POINT instead lets the credential be issued with the
// boundary the broker really keeps, while every surface that shows the prefix shows this
// value beside it.
//
// A nil receiver answers KeyScopeEnforcementNone, because a subscriber that does not
// exist has recorded nothing — and because this is read on rows loaded from a repository
// whose not-found representation is a nil pointer.
//
// Returns:
//   - KeyScopeEnforcementStatus: ConsumerSide when a non-blank prefix is recorded,
//     otherwise None.
func (s *EventSubscriber) KeyScopeEnforcement() KeyScopeEnforcementStatus {
	if s.DeclaresKeyScope() {
		return KeyScopeEnforcementConsumerSide
	}

	return KeyScopeEnforcementNone
}
