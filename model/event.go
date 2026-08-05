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
	"encoding/json"
	"strings"
	"time"
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
	// LedgerID is the Kafka message key. Keying by ledger ID with a stable hash
	// balancer pins every event for one ledger to a single partition, which is
	// what delivers the per-aggregate ordering guarantee. It is optional
	// because a few event types (system errors, for instance) belong to no
	// ledger; such events are published without a key.
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
)

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
//	system       → blnk.system       → blnk.system.dlt
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
const (
	// EventCategoryTransactions covers all transaction lifecycle events,
	// including the runtime-composed bulk transaction events.
	EventCategoryTransactions = "transactions"
	// EventCategoryBalances covers balance creation and balance monitor alerts.
	EventCategoryBalances = "balances"
	// EventCategoryIdentities covers identity events.
	EventCategoryIdentities = "identities"
	// EventCategorySystem covers ledger and internal-error events, and is also
	// the catch-all for any event type not explicitly mapped.
	EventCategorySystem = "system"
)

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
		// The catch-all. An unrecognised or empty event type is routed to the
		// system category rather than rejected or dropped, which is what keeps
		// the "zero exceptions" coverage guarantee true for any event type
		// introduced later: a new producer that forgets to extend this table
		// still gets its events published and observable on a real topic instead
		// of losing them silently.
		return EventCategorySystem
	}
}
