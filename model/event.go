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

// event.go is the single declaration site for Blnk's Kafka event-publishing contract.

package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SchemaVersionV1 is the initial value of LedgerEvent.SchemaVersion and the version
// every event published today carries.
const SchemaVersionV1 = 1

// MaxEventMessageBytes is the largest a serialised event message may be, in bytes, and
// it is the ONE limit the whole pipeline validates against.
const MaxEventMessageBytes = 768 * 1024

// MaxTraceparentLength and MaxTracestateLength bound the two W3C trace-context values
// an outbox row may carry.
const (
	MaxTraceparentLength = 255
	MaxTracestateLength  = 512
)

// SanitizeTraceContext returns the trace context that may be stored on an outbox row,
// dropping anything unusable.
//
// Parameters:
//   - traceparent string: the W3C traceparent header value, or empty when there is no
//     trace.
//   - tracestate string: the W3C tracestate header value, or empty.
//
// Returns:
//   - string: the traceparent to store, empty when there is none or it was unusable.
//   - string: the tracestate to store, empty when there is none, it was unusable, or
//     the traceparent was dropped.
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

// LedgerEvent is the canonical JSON envelope published to Kafka for every ledger
// mutation Blnk emits. Its field set and JSON tags are the subscriber-facing contract:
type LedgerEvent struct {
	// EventID is a UUID that uniquely identifies this event, and it doubles as the
	// subscriber idempotency key.
	EventID string `json:"event_id"`

	// EventType is the event name, for example "transaction.applied" or "balance.monitor".
	EventType string `json:"event_type"`

	// AggregateID identifies the aggregate the event belongs to — the transaction,
	// balance, identity or ledger the mutation acted on.
	AggregateID string `json:"aggregate_id"`

	// OccurredAt is the instant the domain action happened, RFC3339 on the wire.
	OccurredAt time.Time `json:"occurred_at"`

	// Payload carries the legacy webhook body verbatim, byte for byte.
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
// Returns:
//   - []byte: the canonical envelope bytes.
//   - error: when the payload is present but is not valid JSON, or a scalar member
//     cannot be encoded.
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

// EventOutbox is one row of blnk.event_outbox: a durable record of an event that must
// reach Kafka, together with the relay state machine that gets it there.
type EventOutbox struct {
	// --- Event envelope ---
	// These fields reproduce the LedgerEvent that will be published, plus the
	// routing information the relay needs to publish it.

	// ID is the BIGSERIAL surrogate primary key, assigned by the database.
	ID int64 `json:"id"`
	// EventID is the event UUID. It carries a unique index, which is what makes the
	// outbox's exactly-once write-side guarantee enforceable in the schema rather than
	// merely intended in code.
	EventID string `json:"event_id"`
	// EventType is the event name, copied to LedgerEvent.EventType on publish.
	EventType string `json:"event_type"`
	// AggregateID is the aggregate the event belongs to.
	AggregateID string `json:"aggregate_id"`

	// PartitionKey is the Kafka message key, and it is ALWAYS SET.
	PartitionKey string `json:"partition_key"`

	// LedgerID is the AUTHORITATIVE ledger this event belongs to, or empty when the event
	// genuinely has no ledger. It takes no part in partitioning: it answers "which ledger
	// is this about", never "where does this message go".
	LedgerID string `json:"ledger_id,omitempty"`
	// Topic is the fully-resolved destination topic, recorded at insert time so the relay
	// never has to re-derive it and so a stored row remains replayable to its original
	// destination even if the topic-naming configuration changes later.
	Topic string `json:"topic"`
	// SchemaVersion is the envelope version, copied to
	// LedgerEvent.SchemaVersion on publish.
	SchemaVersion int `json:"schema_version"`
	// Payload holds the legacy webhook body bytes verbatim, as json.RawMessage so they are
	// neither reordered nor renormalised between the insert and the publish.
	Payload json.RawMessage `json:"payload"`

	// EventRaw is THE CANONICAL EVENT VALUE: the complete LedgerEvent envelope bytes,
	// exactly as they go onto the Kafka topic, produced once at capture by
	// LedgerEvent.CanonicalBytes and persisted in the event_raw BYTEA column.
	EventRaw []byte `json:"event_raw,omitempty"`

	// OccurredAt is the instant the domain action happened. The relay claims
	// rows in ascending OccurredAt order, so FIFO holds within a partition key.
	OccurredAt time.Time `json:"occurred_at"`

	// CreatedAt is the instant this ROW became durable — when the capturing transaction
	// committed — as opposed to OccurredAt, which is when the domain action happened.
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
	NextAttemptAt time.Time `json:"next_attempt_at"`
	// LastError is the most recent publish failure reason, retained for operator triage
	// and copied into FailureMetadata.ErrorReason when the row is finally dead-lettered.
	LastError string `json:"last_error,omitempty"`
	// FirstAttemptedAt is when the first publish attempt was made; nil until the row is
	// first claimed. It becomes FailureMetadata.FirstAttemptedAt on dead-lettering.
	FirstAttemptedAt *time.Time `json:"first_attempted_at,omitempty"`
	// LastAttemptedAt is when the most recent publish attempt was made; nil until the row
	// is first claimed. It becomes FailureMetadata.LastAttemptedAt on dead-lettering.
	LastAttemptedAt *time.Time `json:"last_attempted_at,omitempty"`
	// DispatchedAt is when the broker acknowledged the publish; nil until then.
	DispatchedAt *time.Time `json:"dispatched_at,omitempty"`
	// LockedUntil is the lease expiry held by the relay instance that claimed this row.
	LockedUntil *time.Time `json:"locked_until,omitempty"`

	// ClaimToken identifies the CLAIM the row is currently under, and is what makes every
	// state transition safe against a worker whose lease expired.
	ClaimToken string `json:"claim_token,omitempty"`

	// --- Dual-delivery marker ---

	// WebhookDispatched records that the legacy HTTP webhook leg was dispatched for this
	// row.
	WebhookDispatched bool `json:"webhook_dispatched"`

	// KafkaDispatchedAt is when the broker acknowledged the Kafka publish, recorded
	// INDEPENDENTLY of DispatchedAt, and the field that keeps the two legs' fates
	// separate.
	KafkaDispatchedAt *time.Time `json:"kafka_dispatched_at,omitempty"`

	// WebhookAttempts counts legacy webhook ENQUEUE attempts made so far, separately from
	// Attempts.
	WebhookAttempts int `json:"webhook_attempts"`

	// KafkaTopic, KafkaPartition and KafkaOffset are WHERE THIS ROW'S RECORD ACTUALLY
	// LANDED: the coordinate the broker assigned to the write that made this row's
	// publication real. All three are nil until a write is acknowledged, and they are set
	// or cleared together.
	KafkaTopic string `json:"kafka_topic,omitempty"`
	// KafkaPartition is the partition the broker assigned. Nil when no write has been
	// acknowledged. A pointer rather than an int because partition 0 is a perfectly
	// ordinary partition, and a zero value would be indistinguishable from "not recorded".
	KafkaPartition *int `json:"kafka_partition,omitempty"`
	// KafkaOffset is the offset within that partition. Nil when no write has been
	// acknowledged, and a pointer for the same reason as KafkaPartition: offset 0 is the
	// first record on a fresh partition.
	KafkaOffset *int64 `json:"kafka_offset,omitempty"`

	// --- Dead-letter record ---

	// DLTTopic is the dead-letter topic the event was written to once its retry budget was
	// exhausted — the `<topic>.dlt` sibling of Topic. Empty until the row is
	// dead-lettered.
	DLTTopic string `json:"dlt_topic,omitempty"`
	// FailureMetadata stores the marshaled FailureMetadata struct declared below. It is
	// kept as raw JSON, not a decoded struct, so the stored bytes are handed back to the
	// dead-letter API exactly as they were written.
	FailureMetadata json.RawMessage `json:"failure_metadata,omitempty"`

	// Traceparent and Tracestate carry the W3C trace context of the request that CAPTURED
	// this event, so that publishing it can be correlated with the mutation that produced
	// it.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

// IsPurgeableByRetention reports whether the retention sweep is permitted to delete
// this row.
//
// Returns:
//   - bool: true when the row may be deleted by age.
func (e EventOutbox) IsPurgeableByRetention() bool {
	return e.Status == EventOutboxStatusDispatched
}

// DeadLetterInventoryFilter narrows the dead-letter inventory IN SQL.
type DeadLetterInventoryFilter = DeadLetterQuery

// InventoryEntry projects a full outbox row onto the dead-letter listing shape.
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

// BrokerRecord is the coordinate of one record on one Kafka topic: the value that turns
// "this event was published" into "this event is THAT record".
type BrokerRecord struct {
	// Topic is the topic the record is on. For a dispatched event this is the
	// category topic; for a dead-lettered one it is the `.dlt` sibling.
	Topic string

	// Partition is the partition the broker assigned, which for a keyed message is
	// determined by the partition key and is therefore stable across redeliveries of the
	// same event.
	Partition int

	// Offset is the record's position within that partition. It is assigned by the broker
	// and is unique per partition, so the three fields together identify one record in the
	// cluster.
	Offset int64
}

// Confirmed reports whether this coordinate actually names a record.
//
// Returns:
//   - bool: true when the coordinate names a record.
func (r BrokerRecord) Confirmed() bool {
	return strings.TrimSpace(r.Topic) != "" && r.Offset >= 0
}

// String renders the coordinate in the `topic/partition@offset` form used in logs, the
// dead-letter API and the operations runbook.
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
// Returns:
//   - BrokerRecord: the coordinate, zero-valued when the row has none.
//   - bool: whether the row names a record.
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

// CanonicalEventBytes returns the STORED canonical envelope, or composes one when the
// row carries none.
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

// EffectivePartitionKey resolves the Kafka message key a row is ACTUALLY published
// under.
//
// Parameters:
//   - ledgerID string: the row's authoritative ledger, empty for a ledger-less event.
//   - partitionKey string: the row's stored partition key.
//
// Returns:
//   - string: the key the message is published under. Empty only when both inputs are
//     blank, which is an event belonging to no aggregate at all.
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

// FailureMetadata is the diagnostic record appended to an event when its retry budget
// is exhausted and it is written to a dead-letter topic. It answers the three questions
// an operator triaging a dead-lettered event always has: where was this event meant to
// go, why did it not get there, and over what window did we try.
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
	// FirstAttemptedAt it bounds the window over which the failure persisted, which is
	// what distinguishes a momentary broker blip from a sustained outage.
	LastAttemptedAt time.Time `json:"last_attempted_at"`
}

// PublishStatus is the outcome of ONE publish attempt, reported for observability. The
// publisher's result type carries it, and the metrics layer uses it verbatim as a
// metric attribute value — the publish-attempts counter is attributed by outcome.
type PublishStatus string

const (
	// PublishStatusDispatched means the broker acknowledged the write. With RequiredAcks
	// set to all in-sync replicas, this is a durable acknowledgement, not merely a
	// successful socket write.
	PublishStatusDispatched PublishStatus = "dispatched"
	// PublishStatusRetrying means the attempt FAILED. It is the single failure outcome,
	// and it covers both a failure another attempt may recover from and one nothing can:
	PublishStatusRetrying PublishStatus = "retrying"
	// PublishStatusDeadLettered means the event was written to its dead-letter topic with
	// FailureMetadata attached AND A BROKER ACKNOWLEDGED THAT WRITE.
	PublishStatusDeadLettered PublishStatus = "dead_lettered"
)

// AllPublishStatuses returns the complete per-attempt outcome vocabulary, in the order
// the metric documentation lists it.
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

// EventOutboxStatus* are the durable states of an EventOutbox row — the values stored
// in the blnk.event_outbox status column and the values the relay's claim query and
// partial indexes filter on.
const (
	// EventOutboxStatusPending is the initial state, set by the column default
	// when the row is inserted alongside the ledger mutation.
	EventOutboxStatusPending = "pending"
	// EventOutboxStatusProcessing means a relay instance has claimed the row and
	// holds a lease on it in locked_until.
	EventOutboxStatusProcessing = "processing"
	// EventOutboxStatusWebhookPending means the Kafka leg is published and recorded, and
	// only the legacy HTTP webhook leg is still owed.
	EventOutboxStatusWebhookPending = "webhook_pending"
	// EventOutboxStatusDispatched is the success terminal state: the broker acknowledged
	// the publish, dispatched_at is set, and — during the dual-delivery window — the
	// legacy leg is either done or has been deliberately abandoned after spending its own
	// budget.
	EventOutboxStatusDispatched = "dispatched"
	// EventOutboxStatusFailed means the retry budget in max_attempts was exhausted without
	// a successful publish.
	EventOutboxStatusFailed = "failed"
	// EventOutboxStatusDeadLettered is the failure terminal state: the event has been
	// written to its `<topic>.dlt` sibling with failure metadata attached, and is now
	// listable and replayable through the dead-letter API.
	EventOutboxStatusDeadLettered = "dead_lettered"
	// EventOutboxStatusReplaying means a replay has CLAIMED this dead-lettered row and is
	// re-publishing it to its original topic.
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

// terminalEventOutboxStatuses is the subset of states from which no further delivery
// attempt is owed, and it is what a caller checks before concluding an event's delivery
// lifecycle is over.
var terminalEventOutboxStatuses = map[string]struct{}{
	EventOutboxStatusDispatched:   {},
	EventOutboxStatusDeadLettered: {},
}

// IsKnownEventOutboxStatus reports whether status is one of the EventOutboxStatus*
// literals.
//
// Parameters:
//   - status string: the status literal to test.
//
// Returns:
//   - bool: true when the literal is part of the state machine.
func IsKnownEventOutboxStatus(status string) bool {
	_, ok := eventOutboxStatuses[status]
	return ok
}

// IsTerminalEventOutboxStatus reports whether status is a state from which no further
// delivery attempt is owed.
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

// TerminalEventOutboxStatuses returns the terminal states, in state-machine order, as a
// fresh slice the caller may retain or reorder.
//
// Returns:
//   - []string: dispatched then dead_lettered.
func TerminalEventOutboxStatuses() []string {
	return []string{EventOutboxStatusDispatched, EventOutboxStatusDeadLettered}
}

// DeadLetterInventoryStatuses returns the two states the dead-letter inventory is
// composed of, as a fresh slice.
//
// Returns:
//   - []string: dead_lettered then failed.
func DeadLetterInventoryStatuses() []string {
	return []string{EventOutboxStatusDeadLettered, EventOutboxStatusFailed}
}

// DeadLetterQuery is the WHOLE narrowing a dead-letter inventory read may express, and
// it is the single contract the list and the count are both driven from.
type DeadLetterQuery struct {
	// EventType narrows to one event name, for example "transaction.applied".
	EventType string

	// Topic narrows to one ORIGINAL category topic, for example "blnk.transactions".
	Topic string

	// Status narrows to one of DeadLetterInventoryStatuses. Anything else must be refused
	// by the caller rather than passed through: a status the inventory cannot contain
	// would match nothing, and an empty page reads as "nothing is stuck".
	Status string

	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant, inclusively, and
	// either may be zero to leave that end unbounded.
	OccurredFrom time.Time
	OccurredTo   time.Time

	// Limit is the maximum number of rows to return. Zero or negative selects the
	// repository default; anything above its maximum is clamped there.
	Limit int

	// Offset is how many MATCHING rows to skip. Negative is clamped to zero. Because the
	// narrowing is applied in SQL, this is an offset into the FILTERED set, which is what
	// makes a filtered page behave like a page of the filtered inventory rather than a
	// filtered page of the unfiltered one.
	Offset int
}

// Filtered reports whether the query narrows the inventory at all.
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
// Returns:
//   - bool: true for the zero value.
func (q DeadLetterQuery) IsEmpty() bool {
	return !q.Filtered()
}

// EventOutboxStatuses returns EVERY state in the outbox state machine, in state-machine
// order, as a fresh slice the caller may retain or reorder.
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
type EventFailureOutcome struct {
	// Status is the state the row is now in: EventOutboxStatusPending when another attempt
	// is owed, or EventOutboxStatusFailed when the budget is spent and the dead-letter
	// write is owed.
	Status string

	// Attempts is the attempt count AFTER this failure was recorded, which is the
	// number the dead-letter failure metadata reports.
	Attempts int

	// Exhausted is true when no further publish attempt will be made and the dead-letter
	// write is now owed — that is, when Status is EventOutboxStatusFailed. It is derived
	// from Status rather than computed independently, so the two can never disagree.
	Exhausted bool

	// Terminal reports that the caller declared this failure PERMANENT — the publisher's
	// own verdict that no further attempt can succeed — rather than the attempt count
	// having run out.
	Terminal bool

	// ClaimToken is the token to present to MarkEventDeadLettered, and it is set ONLY when
	// Exhausted is true.
	ClaimToken string
}

// EventWebhookOutcome is what a recorded LEGACY WEBHOOK enqueue failure decided,
// returned by Datasource.MarkEventWebhookPending.
type EventWebhookOutcome struct {
	// Status is the state the row is now in: EventOutboxStatusWebhookPending when another
	// webhook attempt is owed, or EventOutboxStatusDispatched when the legacy leg has been
	// abandoned and the row is terminal on the strength of its Kafka delivery alone.
	Status string

	// WebhookAttempts is the legacy enqueue count AFTER this failure was recorded.
	WebhookAttempts int

	// Abandoned is true when the legacy leg's budget is spent and no further webhook
	// attempt will be made. Derived from Status rather than computed independently, so the
	// two cannot disagree.
	Abandoned bool
}
