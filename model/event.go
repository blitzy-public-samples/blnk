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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	// the write side only: the event row is committed atomically with the
	// ledger mutation, so it can never be lost. Kafka delivery itself remains
	// at-least-once, and the relay can crash in the window between a
	// successfully acknowledged publish and the outbox row being marked
	// dispatched — after which the row is reclaimed and republished. That is
	// why blnk.event_outbox carries a unique index on event_id (one event is
	// recorded once) and why the published documentation states the
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
	// It is distinct from the Kafka message key: the key is the ledger ID, and
	// keying by ledger ID with a stable hash balancer is what pins every event
	// for one ledger to one partition and therefore delivers the per-aggregate
	// ordering guarantee. AggregateID is what a consumer groups by once the
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

// EventOutbox is one row of blnk.event_outbox: a durable record of an event that
// must reach Kafka, together with the relay state machine that gets it there.
//
// The row is inserted INSIDE THE SAME DATABASE TRANSACTION as the ledger
// mutation that produced it (requirement R-2, the transactional-outbox
// guarantee). The mutation and its event therefore commit or roll back together
// and can never disagree: there is no window in which a balance moved but the
// event was lost, and no window in which an event describes a mutation that was
// rolled back.
//
// The guarantee this buys is exactly-once ON THE WRITE SIDE ONLY. Each event is
// recorded exactly once, which is why event_id carries a unique index. Kafka
// delivery downstream remains AT-LEAST-ONCE: a relay that crashes between a
// successfully acknowledged publish and marking this row dispatched will
// reclaim the row after its lock expires and publish it again. Duplicate
// suppression on LedgerEvent.EventID is consequently a documented subscriber
// obligation rather than an implicit promise.
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
	// single partition, which is what delivers the per-aggregate ordering
	// guarantee. An empty key would let Kafka scatter the event round-robin and
	// destroy that ordering with nothing in the data to show it, so the
	// construction path guarantees a value through a documented fallback chain
	// and this field is never blank on a persisted row.
	//
	// It is a SEPARATE FIELD FROM LedgerID, and the separation is the point.
	// This value is whatever aggregate the event's ordering should follow: a
	// ledger ID when the payload carries one, otherwise a balance ID, an
	// identity ID, a monitor ID, a batch ID or the event type. Calling that
	// "the ledger ID" — which is what this struct used to do — made the name
	// lie about most of its values, and it made the subscriber-facing claim
	// that a key prefix identifies a ledger unfounded.
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
	// Payload holds the legacy webhook body bytes verbatim, stored as JSONB and
	// kept as json.RawMessage so it is neither reordered nor renormalised
	// between the insert and the publish. Both transports during the
	// dual-delivery window read these same bytes, which is what makes their
	// payloads identical structurally rather than by careful coding.
	Payload json.RawMessage `json:"payload"`
	// OccurredAt is the instant the domain action happened. The relay claims
	// rows in ascending OccurredAt order, so FIFO holds within a partition key.
	OccurredAt time.Time `json:"occurred_at"`

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
	// before it, a failed row was claimable again on the very next poll, so the
	// 1s/2s/4s/8s/16s schedule collapsed into roughly five seconds of
	// consecutive attempts against a broker that was already failing.
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
//	   │                                │                     (dispatched_at set)
//	   │                                │
//	   └──────retry budget exhausted────┴──▶ failed ──written to <topic>.dlt──▶ dead_lettered
//
// A row is inserted as pending by the database column default. A relay claims it
// — moving it to processing and taking a lease in locked_until — then publishes.
// A broker acknowledgement moves the row to dispatched and stamps dispatched_at.
// A failed attempt within budget returns the row to the claimable set so it is
// retried after a backoff; once the budget in max_attempts is spent the row
// becomes failed, and once the event has additionally been written to its
// `<topic>.dlt` sibling it becomes dead_lettered. dispatched and dead_lettered
// are the terminal states; only a dead_lettered row is eligible for replay.
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
	// EventOutboxStatusDispatched is the success terminal state: the broker
	// acknowledged the publish and dispatched_at is set.
	EventOutboxStatusDispatched = "dispatched"
	// EventOutboxStatusFailed means the retry budget in max_attempts was
	// exhausted without a successful publish.
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
	EventOutboxStatusPending:      {},
	EventOutboxStatusProcessing:   {},
	EventOutboxStatusDispatched:   {},
	EventOutboxStatusFailed:       {},
	EventOutboxStatusDeadLettered: {},
	EventOutboxStatusReplaying:    {},
}

// terminalEventOutboxStatuses is the subset of states from which no further
// delivery attempt is owed. It is what the retention purge is allowed to delete
// and what a caller checks before concluding an event's lifecycle is over.
//
// failed is deliberately NOT terminal even though the retry budget is spent: the
// dead-letter write is still owed, and until it lands the event exists only in
// this table. Purging a failed row would therefore destroy the only copy.
// replaying is not terminal for the obvious reason that a publish is in flight.
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
	// attempt is owed, or EventOutboxStatusFailed when the budget is spent.
	Status string

	// Attempts is the attempt count AFTER this failure was recorded, which is the
	// number the dead-letter failure metadata reports.
	Attempts int

	// Exhausted is true when the retry budget is spent and the dead-letter write is
	// now owed. It is derived from Status rather than computed independently, so the
	// two can never disagree.
	Exhausted bool

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
// Parameters:
//   - s string: the candidate identifier.
//
// Returns:
//   - bool: true only for the 36-character hyphenated form.
func IsCanonicalUUID(s string) bool {
	if len(s) != uuidCanonicalLength {
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
//	quarantine   → blnk.quarantine   → blnk.quarantine.dlt     (internal)
//
// Nothing in this file knows the prefix, builds a topic name, or appends the
// `.dlt` suffix. Treating a value returned from here as a topic is a bug.
//
// # Why there is a fourth, `system`, category
//
// Two event types that really are emitted belong to none of the three categories
// the requirements name: "ledger.created", raised by the post-ledger-creation
// actions, and "system.error", raised through the registered webhook-sender
// indirection when an internal error is notified. At the same time the coverage
// requirement is absolute — every event type that reaches the legacy webhook
// sender must be published, with zero exceptions.
//
// The fourth category resolves that tension while following the identical naming
// convention, so nothing about the scheme is special-cased. Do NOT collapse it
// into another category: forcing ledger and system-error events onto, say, the
// transactions topic corrupts that topic's semantics for every subscriber that
// filters on it, and dropping them violates the coverage requirement outright.
//
// # Why there is a fifth, `quarantine`, category
//
// Because "every event must be published" and "an unmapped event must not reach
// the wrong audience" are both true, and one topic cannot satisfy both.
//
// The system category used to double as the catch-all for any event type this
// table did not recognise. That made a forgotten mapping into a disclosure: a new
// producer emitting, say, a balance or identity payload under a name nobody had
// added here delivered it to whoever consumes the system topic. Quarantine
// separates the two jobs — system carries the events that genuinely belong to it,
// quarantine carries the ones we cannot classify — and both are INTERNAL, so
// neither can be granted to a subscriber. See IsInternalEventCategory.
//
// Anything arriving in quarantine is a defect to fix by extending EventCategory,
// not a state to design around.
const (
	// EventCategoryTransactions covers all transaction lifecycle events,
	// including the runtime-composed bulk transaction events.
	EventCategoryTransactions = "transactions"
	// EventCategoryBalances covers balance creation and balance monitor alerts.
	EventCategoryBalances = "balances"
	// EventCategoryIdentities covers identity events.
	EventCategoryIdentities = "identities"
	// EventCategorySystem covers ledger and internal-error events.
	//
	// It is INTERNAL: system.error carries Blnk's own diagnostic detail, which is
	// not subscriber-facing data, so this category is not offered to subscribers.
	// IsInternalEventCategory is the single test for that, and the subscriber
	// authorization path refuses a grant over its topic.
	//
	// It is NOT the catch-all. It used to be, and that was the defect: an event
	// type nobody had mapped landed on a topic whose consumers expect ledger and
	// error records, so a missing mapping silently exposed a domain payload to the
	// wrong audience instead of failing where somebody would see it.
	EventCategorySystem = "system"
	// EventCategoryQuarantine is where an event type that this table does not
	// recognise is routed, and it is the catch-all EventCategorySystem used to be.
	//
	// # Why a quarantine category rather than the system topic
	//
	// The coverage requirement is absolute: every event that reaches the publisher
	// must be published, with zero exceptions, so an unrecognised type cannot be
	// dropped and cannot be rejected at the publisher — the row is already
	// committed by then, and refusing it would strand a durable event.
	//
	// But "published somewhere" is not the same as "published to the right
	// audience". Routing an unknown type to blnk.system meant a new producer that
	// forgot to extend this table delivered its payload — potentially a balance
	// or an identity record — to whoever consumes the system topic. Quarantine
	// keeps the event durable, observable and replayable while sending it to a
	// topic that is INTERNAL and that no subscriber can be granted, so a routing
	// omission is contained rather than turned into a disclosure.
	//
	// The correct fix for anything landing here is always to add the event type to
	// EventCategory. A non-empty quarantine topic is a defect signal, which is why
	// the publisher logs at warning level when it routes there.
	EventCategoryQuarantine = "quarantine"
)

// internalEventCategories are the categories no subscriber may be granted access
// to, keyed by category token.
//
// Both entries hold Blnk-internal material rather than subscriber-facing ledger
// data: the system category carries internal error detail, and quarantine carries
// events whose audience is by definition unknown. Declaring them here, once, is
// what lets the subscriber authorization path and the provisioning script apply
// the same rule without either of them keeping its own list.
var internalEventCategories = map[string]struct{}{
	EventCategorySystem:     {},
	EventCategoryQuarantine: {},
}

// IsInternalEventCategory reports whether a category is Blnk-internal and
// therefore not grantable to a subscriber.
//
// Parameters:
//   - category string: a bare category token, not a topic name.
//
// Returns:
//   - bool: true for the system and quarantine categories.
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
//     diagnostics, including error text from inside the process.
//   - The QUARANTINE category, which is where an uncatalogued event type lands precisely
//     because nothing has yet decided what it contains or who may see it.
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
var eventCategoryOrder = [...]string{
	EventCategoryTransactions,
	EventCategoryBalances,
	EventCategoryIdentities,
	EventCategorySystem,
	EventCategoryQuarantine,
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

	switch eventType {
	case "transaction.queued",
		"transaction.applied",
		"transaction.scheduled",
		"transaction.inflight",
		"transaction.void",
		"transaction.rejected",
		"transaction.unknown":
		return EventCategoryTransactions
	case "balance.created",
		"balance.monitor":
		return EventCategoryBalances
	case "identity.created":
		return EventCategoryIdentities
	case "ledger.created",
		"system.error":
		return EventCategorySystem
	default:
		// The catch-all, and it is QUARANTINE rather than the system category.
		//
		// An unrecognised or empty event type is still routed rather than
		// rejected or dropped, which is what keeps the "zero exceptions"
		// coverage guarantee true for any event type introduced later: the row
		// is committed before routing happens, so refusing it here would strand
		// a durable event, and dropping it would lose one.
		//
		// Where it goes matters as much as that it goes somewhere. This arm used
		// to return EventCategorySystem, which meant a new producer that forgot
		// to extend the table above delivered its payload — a balance record, an
		// identity record — to whoever consumes the system topic. Quarantine is
		// internal and cannot be granted to a subscriber, so the same omission
		// is contained instead of becoming a disclosure, and it stays visible:
		// events on the quarantine topic are a defect signal, and the publisher
		// logs at warning level when it routes there.
		return EventCategoryQuarantine
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
// THE ENFORCED BOUNDARY IS EXACTLY THREE THINGS, and they are all the broker can
// check:
//
//	Topic  <each entry of AuthorizedTopics>  LITERAL   Read + Describe
//	Group  <ConsumerGroupID>                 PREFIXED  Read
//
// KafkaPrincipal, ConsumerGroupID and AuthorizedTopics therefore ARE the boundary,
// and a provisioning call translates them into one SCRAM credential plus that set
// of ACL bindings. KafkaPrincipal is the join key between a registry row and the
// broker's own authorization state, because every binding names it.
//
// PARTITIONKEYPREFIX IS NOT PART OF THE ENFORCED BOUNDARY, and cannot be. Kafka's
// authorizer has no message-key dimension: there is no ACL that restricts a
// consumer to a slice of a topic by key, and no broker-side mechanism of any kind
// that could apply one. A subscriber granted a category topic can read EVERY
// record on that topic, whatever its key. The prefix is an advisory filter hint
// that the registry records and the credential response reports so a subscriber can
// discard the records it does not care about CLIENT-SIDE — and client-side
// filtering is not authorization.
//
// This is stated at length because the field's mere presence in a struct called
// "the access model" previously implied an isolation guarantee the system cannot
// deliver, which is worse than having no field at all: an operator reading it would
// grant a shared topic believing the key prefix confined the subscriber to its own
// records. If per-key isolation is genuinely required, it has to come from a
// different design — a topic per authorization domain, or a filtering gateway that
// emits already-isolated streams — and not from this field.
//
// Nothing in this package or the provisioning path treats the prefix as
// authorization: NewSubscriberProvisioningRequest deliberately does not map it onto
// any binding, and HasTopicAccess ignores it.
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
//   - PartitionKeyPrefix nil means the subscriber has asked for no client-side key
//     filter at all, as opposed to a filter on the empty prefix — which would
//     invert the intent. It is not a grant either way; see the access-model note
//     above for why the prefix is advisory.
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
	// PartitionKeyPrefix is an ADVISORY, CLIENT-SIDE filter hint. It is NOT an
	// authorization boundary and is not enforced by anything.
	//
	// Kafka ACLs are topic-level; the broker has no message-key dimension to
	// restrict on. A subscriber granted a topic can read every record on it
	// regardless of this value, so nothing may treat a non-nil prefix as
	// narrowing what the subscriber CAN read — only as describing what it WANTS
	// to read. Nil means no filter was requested.
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

	// WebhookURL is the legacy HTTP webhook URL this subscriber received pushes
	// on before moving to Kafka. Nil for a subscriber onboarded after the
	// cutover, which never had one.
	WebhookURL *string `json:"webhook_url,omitempty"`
	// MigratedAt is when the subscriber completed its move to Kafka consumption.
	// Nil means not yet migrated.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// --- Row bookkeeping ---

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasTopicAccess reports whether the subscriber's RECORDED GRANT covers the given
// topic.
//
// The comparison is exact and the empty-grant case falls out of it naturally: a
// subscriber with no authorised topics matches nothing, so the registry fails
// closed rather than open. PartitionKeyPrefix is deliberately not consulted — it is
// advisory (see the type documentation) and folding it in here would suggest the
// system can decide access per key, which it cannot.
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

// IsProvisioned reports whether a credential has ever been issued to this
// subscriber.
//
// It tests the credential reference rather than the issuance timestamp because
// the reference is the value the repository writes first and the two are always
// written together; either would do, and testing the reference keeps the
// "registered, not yet provisioned" state readable at the call site.
func (s *EventSubscriber) IsProvisioned() bool {
	return s.CredentialReference != nil && *s.CredentialReference != ""
}

// IsMigrated reports whether the subscriber has completed its move from legacy
// HTTP webhook delivery to Kafka consumption. A nil MigratedAt means not yet
// migrated, which is what dual-window migration-progress reporting counts.
func (s *EventSubscriber) IsMigrated() bool {
	return s.MigratedAt != nil
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
