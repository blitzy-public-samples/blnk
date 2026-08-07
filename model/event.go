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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
	// PARTITION KEY — a ledger id when the payload yields one, otherwise a
	// balance, identity, monitor or batch id, or the event type; see
	// EventOutbox.PartitionKey — and keying with a stable hash balancer is what
	// pins every event sharing a key to one partition, delivering ordering per
	// partition key. AggregateID is what a consumer groups by once the messages
	// arrive.
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
// commit or roll back together. A caller with no transaction to share captures
// standalone through PublishEvent, which is what the domain post-action call
// sites do today: the row is then committed on its own, after the mutation, and
// idempotence rests on the derived event id and the unique index rather than on
// atomicity.
//
// Either way the guarantee is EXACTLY-ONCE ON THE WRITE SIDE ONLY. Kafka
// delivery downstream remains AT-LEAST-ONCE: a relay that crashes between a
// successfully acknowledged publish and marking this row dispatched will reclaim
// the row after its lock expires and publish it again. Duplicate suppression on
// LedgerEvent.EventID is consequently a documented subscriber obligation rather
// than an implicit promise.
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
	// It is a SEPARATE FIELD FROM LedgerID, and the separation is the point.
	// This value is whatever aggregate the event's ordering should follow: a
	// ledger id when the payload carries one, otherwise a balance id, an
	// identity id, a monitor id, a batch id or the event type. It is therefore
	// not "the ledger id", and one key may deliberately group several
	// aggregates — every event of one ledger, for instance — which is why the
	// guarantee is stated per partition key rather than per aggregate.
	PartitionKey string `json:"partition_key"`

	// LedgerID is the AUTHORITATIVE ledger this event belongs to, or empty when
	// the event genuinely has no ledger. It takes no part in partitioning.
	//
	// It is populated only from a payload that actually carries a ledger
	// identifier — a ledger, or a balance, which belongs to exactly one ledger.
	// The exceptions, enumerated so that "empty" is a documented fact rather
	// than an omission:
	//
	//   - Transactions. model.Transaction HAS NO LEDGER FIELD; a transaction's
	//     ledger association is indirect, through the balances it moves value
	//     between, and resolving it would require a database read on the
	//     capture path. Left empty.
	//   - Balance monitors. BalanceMonitor carries no ledger field either.
	//     Left empty.
	//   - Identities. An identity is not scoped to a ledger in this model.
	//     Left empty.
	//   - Bulk transaction batches. A batch is a runtime grouping, not a ledger
	//     object. Left empty.
	//   - system.error. No aggregate of any kind. Left empty.
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
	// absorbed into a surplus. See AuditTerminalEventRecords and
	// ReconcileAgainstOutbox.
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
// It is deliberately distinct from the EventOutboxStatus* values below:
// PublishStatus describes a single attempt and is never persisted, whereas the
// outbox status describes the row's durable state machine. An event can report
// PublishStatusRetrying several times while its row stays in the processing
// state.
type PublishStatus string

const (
	// PublishStatusDispatched means the broker acknowledged the write. With
	// RequiredAcks set to all in-sync replicas, this is a durable acknowledgement,
	// not merely a successful socket write.
	PublishStatusDispatched PublishStatus = "dispatched"
	// PublishStatusRetrying means the attempt failed but the retry budget is not
	// yet exhausted, so the event will be attempted again after a backoff delay.
	PublishStatusRetrying PublishStatus = "retrying"
	// PublishStatusDeadLettered means the retry budget was exhausted and the
	// event was written to the dead-letter topic with FailureMetadata attached.
	PublishStatusDeadLettered PublishStatus = "dead_lettered"

	// PublishStatusFailed means the attempt failed and NO further attempt will be made for
	// it: either the failure is permanent, or it was the last attempt the row's budget
	// allowed. It is an ATTEMPT outcome and is never persisted as a row's status — the
	// row's terminal state is PublishStatusDeadLettered.
	//
	// It exists because none of the other three can express "this attempt failed and
	// nothing further will be tried", and reporting that as PublishStatusRetrying made a
	// permanently-stuck event indistinguishable from a busy one in both the logs and the
	// attempts counter.
	PublishStatusFailed PublishStatus = "failed"
)

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
// delivery attempt is owed. It is what the retention purge is allowed to delete
// and what a caller checks before concluding an event's lifecycle is over.
//
// failed is deliberately NOT terminal even though the retry budget is spent: the
// dead-letter write is still owed, and until it lands the event exists only in
// this table. Purging a failed row would therefore destroy the only copy.
// replaying is not terminal for the obvious reason that a publish is in flight.
// webhook_pending is not terminal because a delivery is still owed on the legacy
// leg — purging such a row would drop that webhook silently, which is the exact
// loss the state was introduced to stop.
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
// further delivery attempt is owed, and is therefore eligible for retention
// purging.
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
// It exists so the retention purge can enumerate exactly what it is permitted to
// delete from one authoritative place rather than restating the pair at the call
// site, where an addition here would not reach it.
//
// Returns:
//   - []string: dispatched then dead_lettered.
func TerminalEventOutboxStatuses() []string {
	return []string{EventOutboxStatusDispatched, EventOutboxStatusDeadLettered}
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
//	ledgers      → blnk.ledgers      → blnk.ledgers.dlt
//	system       → blnk.system       → blnk.system.dlt         (internal)
//
// FIVE CATEGORIES, TEN TOPICS, AND THE LIST IS CLOSED. It is the agreed topic
// contract: subscribers, the provisioning script, the Kubernetes configuration and
// the local stack all enumerate exactly these names, so adding a sixth category here
// silently obliges every one of them to be changed too. A new category is a
// deliberate contract change, never an implementation detail.
//
// Nothing in this file knows the prefix, builds a topic name, or appends the
// `.dlt` suffix. Treating a value returned from here as a topic is a bug.
//
// # Why there are two categories beyond the three the requirements name
//
// Two event types that really are emitted belong to none of the three categories
// the requirements name: "ledger.created", raised by the post-ledger-creation
// actions, and "system.error", raised through the registered webhook-sender
// indirection when an internal error is notified. At the same time the coverage
// requirement is absolute — every event type that reaches the legacy webhook
// sender must be published, with zero exceptions — and so is the migration's
// promise that a subscriber can consume what the webhook used to deliver it.
//
// THEY GET ONE CATEGORY EACH, `ledgers` and `system`, both following the identical
// naming convention so nothing about the scheme is special-cased. That is
// AMBIGUITY-2's resolution, and every alternative is worse. Forcing ledger events
// onto, say, the transactions topic corrupts that topic's semantics for every
// subscriber that filters on it. Dropping them violates the coverage requirement
// outright. And putting BOTH into one extra category — which is what this file used
// to do — is worse than either, because the two have opposite access requirements:
// system.error must be ungrantable and ledger.created must be grantable, so a shared
// topic had to choose, and it chose to make ordinary ledger data unreachable by
// every subscriber credential Blnk can issue.
//
// Only `system` is INTERNAL, so no subscriber principal may be granted its topic.
// That is a deliberate consequence and not an oversight: system.error's payload is
// the frozen legacy body, so it still carries the error text as it renders, and
// narrowing it would break the payload-preservation guarantee. The disclosure is
// therefore contained by audience rather than by redaction, and it costs a
// subscriber nothing it used to receive, because the only event type left inside
// the internal category is the one whose audience was always the operator alone.
//
// # Where an event type this table does not recognise goes
//
// To the SYSTEM category, which is the catch-all as well as the home of
// system.error. That placement is safe for the one reason that
// matters: the system category is INTERNAL, so no subscriber can be granted its
// topic (see IsInternalEventCategory and SubscriberGrantableEventCategories). An
// uncatalogued event is therefore published — the coverage guarantee is absolute
// and the row is already committed by the time routing happens, so it can be
// neither dropped nor refused — while remaining unreachable by any subscriber.
// Containment does not need a category of its own; it needs a topic no grant
// covers, and the system topic already is one.
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
	// EventCategoryLedgers covers ledger events, and it is SUBSCRIBER-FACING.
	//
	// It exists because `ledger.created` belongs to none of the three categories the
	// requirement names, and because it is ordinary ledger data that a subscriber has
	// every right to consume. Its payload is a *model.Ledger — a name, an id, a
	// creation instant and the caller's own metadata — which is exactly the shape of
	// `identity.created` and no more sensitive than it.
	//
	// # Why it is not the system category, which is where it used to live
	//
	// Putting it there made it UNREACHABLE. Every event in this catalogue reached the
	// legacy webhook, so a subscriber consuming webhooks today receives
	// `ledger.created`; the system category is internal by design, so no principal can
	// be granted its topic. A subscriber migrating to Kafka would therefore have LOST
	// this event at the sunset with nothing offered in its place — a silent regression
	// in the one direction the migration promises not to regress, and a breach of the
	// coverage requirement read as it is meant to be read: coverage of a transport
	// nobody can subscribe to is not coverage.
	//
	// Sharing a topic with `system.error` was also the reason it could not simply be
	// made grantable: that would have exposed internal error detail to every
	// subscriber granted ledger events. Splitting the two is what lets each get the
	// audience it should have.
	EventCategoryLedgers = "ledgers"
	// EventCategorySystem covers internal-error events, and it is also the CATCH-ALL
	// for an event type EventCategory does not recognise.
	//
	// It is one of the two categories AMBIGUITY-2 resolves the requirement's three
	// named ones into. `system.error` belongs to none of transactions, balances or
	// identities, and the coverage requirement admits no exceptions, so it needs a
	// category of its own rather than being forced into an unrelated one — which would
	// corrupt that topic's meaning for the subscribers filtering it — or dropped,
	// which would breach coverage outright.
	//
	// # It is INTERNAL: no subscriber principal may be granted it
	//
	// Two independent reasons, either of which is sufficient on its own:
	//
	//   - system.error's payload is the FROZEN LEGACY BODY, {"error": <text>,
	//     "time": <now>}, and the <text> is the error as it renders. A PostgreSQL
	//     error names schema, table, column and routine; a broker error names
	//     internal addresses. That body cannot be narrowed without breaking the
	//     payload-preservation guarantee the whole migration rests on, so the
	//     disclosure is contained by AUDIENCE instead: the bounded, classified
	//     diagnosis goes to the operator log (see internal/notification), and the
	//     verbatim body goes to a topic no grant covers.
	//   - Being internal is exactly what makes it a safe catch-all. An uncatalogued
	//     event type lands on a topic no subscriber can be granted, so a routing
	//     omission cannot deliver a domain payload — a balance record, an identity
	//     record — to an audience that never asked for it.
	//
	// Coverage is still absolute and is not the same question as reachability. Every
	// event, including this one, is durably captured, published, observable and
	// replayable; the publisher logs at warning level when it routes an
	// unrecognised type here so the omission is visible rather than silent. What an
	// internal category withholds is a subscriber ACL, not the event.
	//
	// The ONE event type deliberately left without a subscriber route is system.error,
	// and that is a security decision rather than an oversight. Every event describing
	// LEDGER STATE — transactions, balances, identities and now ledgers — has a
	// grantable topic.
	EventCategorySystem = "system"
)

// internalEventCategories are the categories no subscriber may be granted access
// to, keyed by category token.
//
// The system category holds Blnk-internal material rather than subscriber-facing
// ledger data — internal error detail, and any event type the catalogue does not
// recognise. Declaring the set here, once, is what lets the subscriber
// authorization path and the provisioning script apply the same rule without
// either of them keeping its own list.
//
// It is the ONLY internal category, and deliberately narrow. Every event that
// describes ledger state has a grantable topic: transactions, balances, identities
// and ledgers. Widening this set is how an event ends up captured, published and
// unreachable by the subscribers it was captured for.
var internalEventCategories = map[string]struct{}{
	EventCategorySystem: {},
}

// IsInternalEventCategory reports whether a category is Blnk-internal and
// therefore not grantable to a subscriber.
//
// Parameters:
//   - category string: a bare category token, not a topic name.
//
// Returns:
//   - bool: true for the system category, which is the only internal one.
func IsInternalEventCategory(category string) bool {
	_, internal := internalEventCategories[category]

	return internal
}

// SubscriberGrantableEventCategories returns the categories a subscriber may be
// authorised to consume, in a stable order.
//
// It is the allowlist the subscriber DTO validation and the Kafka ACL provisioning
// both check against, so that "which topics may a subscriber be granted?" has one
// answer rather than one per caller.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, excluding every internal
//     category.
func SubscriberGrantableEventCategories() []string {
	grantable := make([]string, 0, len(eventCategoryOrder))
	for _, category := range eventCategoryOrder {
		if IsInternalEventCategory(category) {
			continue
		}
		grantable = append(grantable, category)
	}

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
// What the list EXCLUDES is the security-relevant part, and each exclusion is deliberate:
//
//   - DEAD-LETTER topics. A `<topic>.dlt` holds events that already failed, together with
//     failure metadata naming broker addresses and internal error reasons. It is Blnk's
//     operational surface, read under the master key through GET /events/dead-letter, and
//     granting one to a subscriber would hand it every other subscriber's failed events.
//   - The SYSTEM category, which carries `ledger.created` and `system.error` — Blnk's own
//     diagnostics, including error text from inside the process — and, as the catch-all,
//     any uncatalogued event type, whose contents and audience nothing has yet decided.
//
// What it deliberately INCLUDES is the system category, which carries `ledger.created` and
// `system.error`. Both were delivered by the legacy webhook transport, so excluding them
// left two of the thirteen migrated event types with no authorized path — and the payload
// that motivated the exclusion is sanitized where it is produced rather than withheld here.
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
// The grantable categories come first and the internal ones last, so that
// SubscriberGrantableEventCategories — which filters this list — yields a
// contiguous prefix of it and a reader can see at a glance where the boundary
// between subscriber-facing and internal topics falls.
var eventCategoryOrder = [...]string{
	EventCategoryTransactions,
	EventCategoryBalances,
	EventCategoryIdentities,
	EventCategoryLedgers,
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
	// ledger.created routes to its OWN grantable category rather than to system, which
	// is what gives a migrating subscriber a Kafka route to an event it receives over
	// the legacy webhook today. See EventCategoryLedgers.
	"ledger.created": EventCategoryLedgers,
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
		// system category answers that safely because it is INTERNAL: no
		// subscriber can be granted its topic, so a producer added without
		// extending the table above cannot deliver its payload — a balance
		// record, an identity record — to an audience that never asked for it.
		// The event stays durable, observable and replayable, and the omission
		// stays visible: the publisher logs at warning level when it routes an
		// event type it does not recognise, and the fix is always to add the
		// type above rather than to design around this arm.
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
// THE BOUNDARY IS EXACTLY THREE SCOPES, and the broker can check only the first
// two:
//
//	Topic  <each entry of AuthorizedTopics>  LITERAL   Read + Describe
//	Group  <ConsumerGroupID>                 PREFIXED  Read
//	Key    <PartitionKeyPrefix>              enforced by REFUSAL — see below
//
// KafkaPrincipal, ConsumerGroupID and AuthorizedTopics are the two broker-checked
// scopes, and a provisioning call translates them into one SCRAM credential plus
// that set of ACL bindings. KafkaPrincipal is the join key between a registry row
// and the broker's own authorization state, because every binding names it.
//
// PARTITIONKEYPREFIX IS NOT PART OF THE ENFORCED BOUNDARY, and cannot be. Kafka's
// authorizer has no message-key dimension: there is no ACL that restricts a
// consumer to a slice of a topic by key, and no broker-side mechanism of any kind
// that could apply one. A subscriber granted a category topic can read EVERY
// record on that topic, whatever its key.
//
// SO THE FIELD IS A CONSTRAINT THIS SYSTEM WILL NOT PRETEND TO HONOUR, AND
// ISSUANCE FAILS CLOSED ON IT. A row carrying a non-nil prefix records an
// authorization narrower than any credential Blnk could mint, so
// EventSubscriberService.IssueSubscriberCredential REFUSES to issue for that row
// rather than handing back a credential whose real scope is the whole topic. The
// refusal names both ways forward: clear the prefix to accept whole-topic access,
// or narrow the topic grant, which IS enforceable.
//
// Calling it "advisory" — which this documentation and the column comment both
// once did — was the more dangerous framing, however carefully qualified. The
// field's presence in a struct describing "the access model" implies an isolation
// guarantee the system cannot deliver, and an operator reading it would grant a
// shared topic believing the key prefix confined the subscriber to its own
// records. A qualification in a comment does not survive that reading; a refused
// issuance does. If per-key isolation is genuinely required it has to come from a
// different design — a topic per authorization domain, or a filtering gateway that
// emits already-isolated streams — and not from this field.
//
// Nothing in this package or the provisioning path treats the prefix as a grant:
// NewSubscriberProvisioningRequest deliberately does not map it onto any binding,
// and HasTopicAccess ignores it.
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
//     constraint on the empty prefix — which would invert the intent. Non-nil
//     blocks credential issuance; see the access-model note above.
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

	// --- Row bookkeeping ---

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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

// HasTopicAccess reports whether the subscriber's RECORDED GRANT covers the given
// topic.
//
// The comparison is exact and the empty-grant case falls out of it naturally: a
// subscriber with no authorised topics matches nothing, so the registry fails
// closed rather than open. PartitionKeyPrefix is deliberately not consulted —
// folding it in here would suggest the system can decide access per key, which it
// cannot; a row carrying one is refused a credential outright instead.
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
// The scope is RECORDED rather than derived, and it must be: message keys on Blnk's
// topics are the aggregate's ledger partition key, so a scope derived from the
// subscriber's own identifier would match no record ever produced. Only the caller
// registering the subscriber knows which ledgers it is entitled to.
func (s *EventSubscriber) RequestedKeyScope() string {
	if s == nil || s.PartitionKeyPrefix == nil {
		return ""
	}

	return *s.PartitionKeyPrefix
}

// RequiresKeyScopeEnforcement reports whether this subscriber asked for a boundary
// narrower than a whole topic.
//
// It is the FAIL-CLOSED TEST that credential issuance and provisioning both consult:
// true means the recorded boundary cannot be expressed as a Kafka ACL, so a direct
// broker credential must be refused rather than issued with the wider access the
// broker would really grant. See the access-model note on EventSubscriber.
//
// It is deliberately not the negation of "is provisionable": a subscriber can be
// unprovisionable for other reasons (no authorised topics, no consumer group), and
// each of those has its own test so a refusal can name its own cause.
func (s *EventSubscriber) RequiresKeyScopeEnforcement() bool {
	return s.RequestedKeyScope() != ""
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
// IT IS NOT WHAT THE BROKER CHECKS. Kafka cannot filter by key, which is exactly why
// a subscriber with a recorded scope is refused a direct credential: until an
// enforcement layer calls this rule on every record, the only place the scope can be
// honoured is a refusal.
func (s *EventSubscriber) HasKeyAccess(key string) bool {
	scope := s.RequestedKeyScope()
	if scope == "" {
		return true
	}

	return strings.HasPrefix(key, scope)
}

// SubscriberKeyScopeAllKeys is the effective key scope of every credential Blnk can
// issue today, and the value the credential contract reports for it: EVERY key on
// the authorised topics.
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
//     honest description of what a topic ACL grants; otherwise the requested scope,
//     which is the boundary a credential would have to keep and cannot.
//   - enforced: true only for the all-keys case. A requested prefix is reported as
//     UNENFORCED, and that is precisely why issuance refuses it instead of handing
//     this pair to a subscriber.
func (s *EventSubscriber) EffectiveKeyScope() (scope string, enforced bool) {
	if requested := s.RequestedKeyScope(); requested != "" {
		return requested, false
	}

	return SubscriberKeyScopeAllKeys, true
}

// IsProvisioned reports whether a credential has ever been issued to this
// subscriber.
//
// It tests the credential reference rather than the issuance timestamp because
// the reference is the value the repository writes first and the two are always
// written together; either would do, and testing the reference keeps the
// "registered, not yet provisioned" state readable at the call site.
//
// A NIL RECEIVER answers false, matching DeclaresUnenforceableIsolation. It has to: the repository's not-found
// representation for a subscriber is a nil pointer, so every caller that reads a
// row and then asks a question about it can hold one, and a predicate that
// panicked there would turn a missing subscriber into a crashed ledger process
// rather than a 404.
func (s *EventSubscriber) IsProvisioned() bool {
	return s != nil && s.CredentialReference != nil && *s.CredentialReference != ""
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
// A nil receiver answers false. Both this predicate and KeyScopeUnenforceable are read as
// GUARDS on a row that may not have been found, so they must be answerable on nothing: a guard
// that panics where it is supposed to refuse is worse than no guard at all.
func (s *EventSubscriber) IsRevocationPending() bool {
	return s != nil && s.RevocationPendingAt != nil
}

// KeyScopeUnenforceable reports whether the subscriber records a key-scoped
// authorization constraint that Kafka cannot enforce.
//
// It is the predicate credential issuance fails closed on. A non-empty prefix means
// the recorded authorization is narrower than any credential Blnk can mint, so the
// honest answer is to refuse rather than to issue whole-topic access under a row
// that says otherwise. The empty string is treated as absent for the same reason the
// column is nullable: "a constraint on the empty prefix" is not an intent anybody
// has, and reading it as one would refuse a subscriber that asked for nothing.
// A nil receiver answers false, for the reason given on IsRevocationPending.
func (s *EventSubscriber) KeyScopeUnenforceable() bool {
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
