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
// It holds the LedgerEvent wire envelope, the EventOutbox persisted row, the
// FailureMetadata attached to dead-lettered events, the PublishStatus and
// EventOutboxStatus vocabularies, and the one event-type-to-category mapping table in
// the repository.
//
// The file is deliberately dependency-free — it imports nothing but the standard
// library — so that every package taking part in the event pipeline can consume it
// without creating an import cycle. In particular the root `blnk` package imports
// `model`, so `model` can never import `blnk` back; keeping this file free of
// first-party imports is what guarantees the producers (root package), the repository
// layer (`database`), the API layer (`api`) and the observability layer
// (`internal/metrics`) all agree on one contract declared in exactly one place.

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

// SchemaVersionV1 is the initial value of LedgerEvent.SchemaVersion and the version
// every event published today carries.
//
// Every producer MUST reference this constant rather than the literal 1, so that a
// version bump is a one-line change here instead of a repository-wide search for stray
// literals.
const SchemaVersionV1 = 1

// MaxEventMessageBytes is the largest a serialised event message may be, in bytes, and
// it is the ONE limit the whole pipeline validates against.
//
// 768 KiB. It sits below the 1 MiB floor with about 25% of headroom, and the headroom
// is what makes the guarantee hold end to end rather than at the moment of validation:
//
//   - The stored payload is wrapped in the LedgerEvent envelope, which adds the
//     five sibling keys and their values.
//   - A dead-letter copy adds the whole failure_metadata object on top of that,
//     including an error string of unbounded length from the broker.
//   - Kafka accounts for the record's key, headers and framing as well as its
//     value.
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
// REJECTING is not an option. This value is telemetry attached to a ledger mutation,
// and refusing the mutation because a caller sent an oversized header would let a
// header break the ledger.
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
// they are stable, and none of them is `omitempty`, so a subscriber can rely on all six
// keys being present on every message even when a value is at its zero value.
type LedgerEvent struct {
	// EventID is a UUID that uniquely identifies this event, and it doubles as the
	// subscriber idempotency key.
	EventID string `json:"event_id"`

	// EventType is the event name, for example "transaction.applied" or "balance.monitor".
	// It duplicates the event name that already appears inside Payload, hoisted to the
	// envelope level so subscribers can route and filter without parsing the payload at
	// all. The redundant string per message is a deliberate trade for cheap,
	// allocation-free routing.
	EventType string `json:"event_type"`

	// AggregateID identifies the aggregate the event belongs to — the transaction,
	// balance, identity or ledger the mutation acted on.
	AggregateID string `json:"aggregate_id"`

	// OccurredAt is the instant the domain action happened, RFC3339 on the wire.
	// time.Time's standard JSON encoding already emits RFC3339 with nanosecond precision,
	// so no custom marshaller is defined or wanted here — adding one would be a way to
	// accidentally break the documented format.
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
// The scalar members ARE encoded through encoding/json, so escaping and timestamp
// formatting stay identical to what a struct marshal would produce. Only the payload
// bypasses it, and only because it is already JSON.
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
//
// That is the path almost every producer takes, and for those events the write side is
// EXACTLY-ONCE: the event cannot be lost while its mutation stands, and cannot exist
// for a mutation that rolled back.
//
// EVERY PRODUCER THAT HAS A MUTATION IS TRANSACTIONAL, and that includes the ones a
// reader might expect to be exceptions. The status-derived transaction.* events of a
// COALESCED batch are captured by the coalescing writer itself, which derives the rows
// and inserts them before its own COMMIT (see resolveBatchEventOutboxes in
// database/transaction.go), so a coalesced batch carries the full guarantee — it is not
// an exception and must not be described as one.
//
// THREE EVENT TYPES STILL CARRY A WEAKER GUARANTEE, each through one narrow named
// window rather than as standing behaviour, and a subscriber that needs to reconcile
// should know which:
//
//   - balance.monitor — on a deployment with no Kafka broker, where no event is
//     captured at all. A pre-write evaluation that could not read the monitors is NOT
//     one of these windows: the writer evaluates that balance itself, inside the
//     transaction.
//   - bulk_transaction.<status> — when the finalising transaction cannot commit
//     after its retry budget. The batch is then left non-terminal and countable, but
//     no event row exists.
//   - system.error — always, and for a different reason: it describes no mutation,
//     so there has never been a transaction it could have joined.
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
	// It is set together with the dispatched status.
	DispatchedAt *time.Time `json:"dispatched_at,omitempty"`
	// LockedUntil is the lease expiry held by the relay instance that claimed this row.
	// Concurrent relay instances skip locked rows, and an expired lease makes a row
	// claimable again — which is what lets a crashed relay's in-flight work be picked up
	// rather than stranded.
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
//
// Pushing the predicates into SQL removes the failure mode rather than reporting it.
// The database applies them across the whole table, the page is taken from the FILTERED
// set, and there is no scan bound to reach because no rows are read and discarded in
// Go.
type DeadLetterInventoryFilter = DeadLetterQuery

// InventoryEntry projects a full outbox row onto the dead-letter listing shape.
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
// An unconfirmed coordinate renders as a named absence rather than as "/0@0", because
// the latter reads as data and would send an operator looking for a record that was
// never written.
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
// CanonicalEvent rebuilds the LedgerEvent this row describes from its envelope columns.
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

// DeadLetterFilter narrows the dead-letter inventory at the REPOSITORY, which is the
// only layer that can narrow it correctly.
//
// Three things were wrong with that, and only the last is obvious:
//
//   - Matching entries beyond the scan bound were omitted from an operator-facing triage list
//     that returned HTTP 200 with no truncation marker, so "nothing else is stuck" and "I gave
//     up looking" were indistinguishable.
//   - No filter-aware COUNT existed, so a filtered page could not report a total at all and the
//     API refused `include_count` whenever a filter was set.
//   - Every filtered request read up to 5,000 rows out of the database to return at most 500.
type DeadLetterFilter = DeadLetterQuery

// Narrows reports whether the filter constrains anything at all.
//
// It exists so a caller can tell an unfiltered request from a filtered one without
// inspecting three fields and getting one of them wrong.
//
// Returns:
//   - bool: true when at least one field is set.
func (q DeadLetterQuery) Narrows() bool {
	return q.Filtered()
}

// PartitionOffsetInterval is one partition's MEASURED, currently-readable offset
// window: the half-open range [FirstOffset, EndOffset) the broker reported for it.
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
// The bounds are asymmetric on purpose, matching Kafka's own semantics: FirstOffset is
// the earliest RETAINED record and EndOffset is one past the last WRITTEN one, so the
// window is [first, end).
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
// aged out reports the same at a non-zero offset; both are legitimately zero rather
// than negative.
//
// Returns:
//   - int64: the number of readable records.
func (i PartitionOffsetInterval) Records() int64 {
	if i.EndOffset <= i.FirstOffset {
		return 0
	}

	return i.EndOffset - i.FirstOffset
}

// EventRecordIntervalAudit is the outbox side of the zero-loss reconciliation,
// classified AGAINST THE MEASURED BROKER WINDOWS rather than counted in aggregate.
//
// PublishedRows deliberately counts a wider set than the terminal statuses. A row in
// webhook_pending HAS been published to Kafka — its Kafka leg completed and
// KafkaDispatchedAt is stamped; what remains outstanding is the deprecated HTTP leg.
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
	// partial unique index on the coordinate makes impossible — so a discrepancy means
	// that index is missing or has been dropped, and the audit reports it rather than
	// assuming the schema is intact.
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
	OldestTerminalAt time.Time

	// CorroboratedFrom and CorroboratedTo bound the publication instants of the
	// CORROBORATED population: the window the green verdict actually covers.
	CorroboratedFrom time.Time
	CorroboratedTo   time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	//
	// The counts are one half of a comparison whose other half is a Kafka offset reading,
	// and the two halves must describe the SAME population or the comparison means
	// nothing. Over the whole history they cannot: broker end offsets are cumulative for
	// the life of a topic and are unaffected by Kafka retention, while the outbox's
	// retention sweep DELETES terminal rows — so the longer a deployment runs with
	// retention enabled, the further the outbox side falls behind a broker side that never
	// forgets, and the "surplus" the verdict tolerates grows without bound until it can
	// hide any amount of loss.
	//
	// A shared window fixes both directions at once: rows whose publication instant falls
	// inside it, against records the broker wrote inside it. Retention shorter than the
	// window is then the only thing that can invalidate the reading, and it is detectable
	// rather than silent — see the truncation flag on the offset report.
	//
	// It is also what makes the query bounded. At 500 events per second the table grows by
	// 43.2 million rows a day, so an exact whole-history aggregate is a scan whose cost
	// rises for ever while answering a question about the last day.
	//
	// The zero value means the counts are whole-history, which is a diagnostic reading
	// only: no reconciliation verdict may be drawn from it.
	WindowStart time.Time

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time
}

// EventTopicBacklog is how much work an outbox topic still owes, for ONE topic name as
// it is stored on the rows.
//
// Two kinds of row still owe a publish to the topic named here, and they must both be
// counted or the audit says a generation has drained when it has not:
//
//   - Rows that have never been dispatched — pending, processing, failed, replaying. The
//     relay owes each of them a publish to this exact topic.
//   - Rows that are dead_lettered. That state is terminal for delivery, but a dead-lettered
//     event is REPLAYABLE, and a replay publishes to the ORIGINAL topic. So a prefix with
//     dead-lettered rows under it is a prefix whose replays would be refused.
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
// Returns:
//   - bool: true when nothing is uncorroborated and no two rows share a coordinate.
func (a EventRecordIntervalAudit) FullyCorroborated() bool {
	return a.UncorroboratedRows() == 0 && a.DuplicatedRecords() == 0
}

// Three costs followed from that, and none of them was visible in the response:
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

// ---------------------------------------------------------------------------------------
// The legacy webhook URL policy — ONE definition, for every layer that records the column
// ---------------------------------------------------------------------------------------

// MaxWebhookURLLength bounds blnk.event_subscribers.webhook_url, in bytes.
//
// It is measured in BYTES rather than runes, deliberately: the limit exists to bound
// what is stored and transmitted, and a rune count would let a multi-byte URL occupy
// several times the intended space. The refusal states both numbers so a caller can see
// by how much.
const MaxWebhookURLLength = 2048

// ValidateWebhookURL judges a legacy webhook URL against the single policy every layer
// that writes blnk.event_subscribers.webhook_url must apply.
//
// An https URL, with a host, no longer than MaxWebhookURLLength, that is not visibly
// internal and carries no surrounding whitespace. Each clause earns its place:
//
//   - HTTPS ONLY, because the payload is a ledger, identity or balance event and pushing it in
//     cleartext is a disclosure whatever the destination.
//   - NO SURROUNDING WHITESPACE, refused rather than trimmed, because the column is stored
//     VERBATIM: trimming for validation and storing the original would persist a destination that
//     never passed the check. It also keeps the repository and the DTO honest about one value.
//   - BOUNDED LENGTH, because every other caller-supplied text on this table is bounded and this
//     one was not. See MaxWebhookURLLength.
//   - NOT AN INTERNAL DESTINATION, because a webhook URL is third-party input that Blnk itself
//     dials, which makes it a server-side request forgery vector straight at the cloud metadata
//     endpoint and at every service that trusts the network rather than the caller.
//
// Parameters:
//   - raw string: the URL exactly as it will be stored. Not trimmed by this function.
//
// Returns:
//   - message string: a short caller-facing statement of what is wrong, echoing no
//     caller input.
//   - reason string: why, for the log or the error cause.
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

	// LENGTH BEFORE PARSING. url.Parse on a 100 KB string is work done on a value that was
	// never going to be accepted, and the check is cheaper than the parse.
	if len(raw) > MaxWebhookURLLength {
		return "The webhook URL is too long",
			fmt.Sprintf(
				"the URL is %d bytes, over the %d byte maximum",
				len(raw), MaxWebhookURLLength,
			)
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

// InternalWebhookDestinationReason reports why a host is an internal destination, or ""
// when it is not visibly internal.
//
// It returns a REASON rather than a boolean so a caller can say which rule was hit.
// "Not allowed" sends an operator looking for a policy document; "the range the cloud
// metadata service lives on" tells them what they just pointed Blnk at.
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

// EffectivePartitionKey resolves the Kafka message key a row is ACTUALLY published
// under.
//
// It lives here, on the model, so that the publish path and every projection that
// REPORTS the key resolve it through one rule. Two copies of a fallback chain is how a
// response starts describing a routing decision the publisher did not make.
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

// DeadLetterInventoryEntry is one row of the dead-letter inventory, projected to
// exactly what a triage listing shows.
//
// THE PAYLOAD IS NOT HERE, and its absence is the point. Neither the stored body nor
// the canonical envelope is projected — only PayloadBytes, the body's size, which is
// the one thing a listing legitimately reports about it.
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

// EffectiveKey is the Kafka message key this entry was published under, resolved
// through the same rule the publish path uses. See EffectivePartitionKey.
//
// Returns:
//   - string: the ledger id when the entry has one, otherwise the stored partition key.
func (e DeadLetterInventoryEntry) EffectiveKey() string {
	return EffectivePartitionKey(e.LedgerID, e.PartitionKey)
}

// DeadLetterCursor is a position in the dead-letter inventory, expressed as the
// ordering key of the last row a page returned rather than as a row count.
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
// It is base64url of "<unix-nanoseconds>:<id>". Opaque rather than structured on
// purpose: a caller that parses it is depending on the ordering key, which is an
// implementation detail of the listing and must stay changeable.
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
// Every malformed shape is rejected rather than coerced: a token that does not decode,
// does not split, or whose halves are not integers cannot be honoured, and defaulting
// it to the beginning of the inventory would silently restart a caller's pagination at
// page one — an infinite loop for any client that pages until it sees an empty page.
//
// Parameters:
//   - token string: the opaque cursor. Empty yields a nil cursor and no error, which
//     means "start at the beginning".
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
// Every field is applied IN SQL.
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
	// OccurredFrom and OccurredTo bound the event's OCCURRENCE instant inclusively, and
	// either may be zero to leave that end unbounded.
	OccurredFrom time.Time
	OccurredTo   time.Time
}

// FilterQuery is this page's narrowing with the PAGE dropped: the same event type,
// topic, status and occurrence window, without the cursor or the limit.
//
// The page bounds are dropped rather than carried because which matches to return has
// no bearing on how many there are.
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
	NextCursor *DeadLetterCursor

	// HasMore mirrors NextCursor != nil, so a response can report the fact without
	// exposing the token to a reader that does not need it.
	HasMore bool
}

// SubscriberCursor is a position in the subscriber registry, expressed as the ordering
// key of the last row a page returned.
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
type SubscriberPage struct {
	// Subscribers are the rows, newest registration first. Never nil on success.
	Subscribers []EventSubscriber

	// NextCursor is the position to resume from, nil when this page is the last one.
	NextCursor *SubscriberCursor

	// HasMore mirrors NextCursor != nil.
	HasMore bool
}

// DeadLetterTopicAge is the oldest outstanding dead-letter entry on one topic, and how
// many are outstanding there.
//
// It is also EXACT. The scan reported a lower bound once it hit its cap and warned
// about it; an alert on a lower bound cannot fire when the true value crosses the
// threshold and the bound does not.
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
// The age is measured from when the obligation was FIRST recorded and is deliberately
// not reset by a later failed attempt, so it reports the age of the EXPOSURE rather
// than the age of the last try.
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
// It returns ZERO when nothing is outstanding, and zero rather than a negative value
// when the marker is stamped in the future — which spans two clocks by necessity, since
// the deregistering process stamps the marker and the collecting process reads it.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
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

// SubscriberAccessResidue is how much broker-side access is UNACCOUNTED FOR:
// credentials that outlived their registry record, and revocations the broker refused.
type SubscriberAccessResidue struct {
	// OrphanedCredentials is how many rows carry an unsettled CredentialOrphanedAt. Zero
	// is the healthy steady state and is a meaningful reading rather than an absent one.
	OrphanedCredentials int64

	// OldestOrphanedAt is when the OLDEST unsettled orphan was recorded, or the zero
	// instant when there is none. Callers must test it rather than subtracting blindly —
	// an epoch-zero instant renders as an age of fifty-odd years and would pin every alert
	// built on it.
	OldestOrphanedAt time.Time

	// FailedRevocations is how many rows carry an unsettled RevocationFailedAt.
	FailedRevocations int64

	// OldestFailedRevocationAt is when the OLDEST of those attempts failed, or the zero
	// instant when none has.
	OldestFailedRevocationAt time.Time
}

// OldestOrphanAge is how long the oldest orphaned credential has been outstanding.
//
// It returns ZERO when nothing is outstanding, and zero rather than a negative value
// when the marker is stamped in the future — which spans two clocks by necessity, since
// the issuing process stamps the marker and the collecting process reads it.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
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
// The zero instant answers zero rather than fifty-odd years, and an instant in the
// future answers zero rather than a negative duration. Both cases are real: a marker is
// absent far more often than present, and the process that stamps it is not the process
// that reads it, so their clocks can disagree by a little.
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
type SubscriberSettlementBacklog struct {
	// Outstanding is how many subscribers owe EITHER obligation. It is not the sum of the two
	// counts below, because one subscriber can owe both.
	Outstanding int64

	// GrantReconcilePending is how many owe a broker-side grant reconciliation.
	GrantReconcilePending int64

	// CredentialCleanupPending is how many owe a credential cleanup.
	CredentialCleanupPending int64

	// OldestPendingAt is when the oldest outstanding obligation of EITHER kind was
	// recorded. It is the zero value when nothing is outstanding, which is why callers
	// must test it rather than subtracting blindly — an epoch-zero instant would render as
	// an age of fifty-odd years and pin every alert on it.
	OldestPendingAt time.Time
}

// OldestAge is how long the oldest outstanding obligation has been owed, measured
// against the supplied instant.
//
// Parameters:
//   - now time.Time: the instant to measure against, supplied so a caller can use its
//     own clock and a test can be exact.
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
// Adding these totals back yields an ALL-TIME terminal count, which is the quantity an
// all-time offset sum can honestly be compared against.
type EventOutboxPurgeTotals struct {
	// Recorded reports whether the totals could be read at all.
	Recorded bool

	// RowsRemoved is how many terminal rows retention has deleted in total. It is added
	// to the surviving terminal rows to restore the all-time count.
	RowsRemoved int64

	// ConfirmedRemoved is how many of those rows carried a broker coordinate, and
	// therefore how many records the broker still counts have lost the row that named
	// them.
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
//   - bool: true only when the log was read AND it records at least one deletion.
func (t EventOutboxPurgeTotals) PurgeHasOccurred() bool {
	return t.Recorded && t.RowsRemoved > 0
}

// EventRecordCoordinate is what one outbox row claims about the broker: the exact
// record its publish produced.
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

// FailureMetadata is the diagnostic record appended to an event when its retry budget
// is exhausted and it is written to a dead-letter topic. It answers the three questions
// an operator triaging a dead-lettered event always has: where was this event meant to
// go, why did it not get there, and over what window did we try.
//
// The failure metadata is attached to the dead-letter message as a SIBLING TOP-LEVEL
// KEY named "failure_metadata", alongside the LedgerEvent keys:
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
// Declaring the vocabulary once, here, is what stops the code and the metric label set
// from silently drifting apart.
type PublishStatus string

const (
	// PublishStatusDispatched means the broker acknowledged the write. With RequiredAcks
	// set to all in-sync replicas, this is a durable acknowledgement, not merely a
	// successful socket write.
	PublishStatusDispatched PublishStatus = "dispatched"
	// PublishStatusRetrying means the attempt FAILED. It is the single failure outcome,
	// and it covers both a failure another attempt may recover from and one nothing can:
	// whether anything further will be tried is reported separately, by
	// PublishResult.Terminal and by the `terminal` metric attribute, because that is a
	// property of the attempt rather than a different kind of outcome.
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
//
// A row is inserted as pending by the database column default. A relay claims it —
// moving it to processing and taking a lease in locked_until — then publishes.
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
	//
	// It exists so that a replay is a claim rather than a read. Without it, two concurrent
	// replay requests for one event both read a dead_lettered row, both pass the "is it
	// dead-lettered?" check, and both publish — so an operator clicking twice, or two
	// operators triaging the same backlog, put two copies of the event on the topic.
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
// It does NOT answer whether the row may be deleted — a dead-lettered row is terminal
// and is not purgeable until an operator resolves it. Use
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
// 'failed' is in it alongside 'dead_lettered', and its presence is the more urgent
// half: a failed row has spent its retry budget and, while dlt_topic is still NULL, no
// copy of the event exists on any topic — the outbox row IS the event. An inventory
// that showed only dead_lettered would hide exactly the rows an operator most needs to
// see.
//
// It is declared here so the list query, the count query and the status filter's
// validation all read one list. Two of them agreeing and a third not is a filter that
// returns an empty page for a state the inventory actually holds, which reads to an
// operator as "nothing is stuck".
//
// Returns:
//   - []string: dead_lettered then failed.
func DeadLetterInventoryStatuses() []string {
	return []string{EventOutboxStatusDeadLettered, EventOutboxStatusFailed}
}

// DeadLetterQuery is the WHOLE narrowing a dead-letter inventory read may express, and
// it is the single contract the list and the count are both driven from.
//
// Pushing the narrowing into SQL fixes both at once, and it has to be ONE type for the
// list and the count or the two can disagree about what they are describing — a total
// computed over a different predicate than the page is worse than no total at all.
type DeadLetterQuery struct {
	// EventType narrows to one event name, for example "transaction.applied".
	// Surrounding whitespace is ignored; empty narrows nothing.
	EventType string

	// Topic narrows to one ORIGINAL category topic, for example "blnk.transactions".
	// Filtering on the original topic rather than on the `.dlt` sibling is what makes
	// "show me the transaction events that are stuck" expressible without the caller
	// having to know the suffix convention.
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

// EventOutboxStatuses returns EVERY state in the outbox state machine, in state-machine
// order, as a fresh slice the caller may retain or reorder.
//
// The order is the order a row moves through, not alphabetical, because that is how the
// runbook reads the counts.
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
// The retry-versus-exhaustion decision is made inside the UPDATE, in SQL, so that two
// relay instances racing on one row cannot both conclude they were the last attempt.
// Returning the decision preserves that atomicity for the caller.
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
	//
	// It becomes true for EITHER of two reasons, and the distinction is Terminal's: the
	// retry budget ran out, or the publisher reported a failure no retry can fix.
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
//
// SUNSET: this type, the transition that returns it and the state it reports go with
// the rest of the legacy transport.
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

// eventTopicDLTSuffix is the dead-letter suffix, duplicated from the root package's
// topic-naming layer for the structural check below ONLY.
const eventTopicDLTSuffix = ".dlt"

// DefaultEventTopicPrefix is the topic namespace assumed when none is configured.
//
// It duplicates the root package's DefaultTopicPrefix and config's own Kafka default by
// value, because this package sits below both and cannot import either. The three are
// pinned equal by test.
const DefaultEventTopicPrefix = "blnk"

// IsBlnkEventTopic reports whether topic is a topic BLNK OWNS under the given prefix:
// exactly `<prefix>.<category>` or `<prefix>.<category>.dlt`, where category is one of
// the EventCategory* tokens.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
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

// IsBlnkEventTopicUnderAnyPrefix reports whether topic is a topic Blnk owns under ANY
// of the given prefixes.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefixes []string: the namespaces this deployment owns. Blank entries are
//     skipped.
//
// Returns:
//   - bool: true when the name is a Blnk-owned category or dead-letter topic under at
//     least one of the prefixes.
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
	// DefaultEventTopicPrefix inside IsBlnkEventTopic, so the two functions cannot
	// disagree about what an unconfigured caller owns.
	return IsBlnkEventTopic(topic, "")
}

// uuidCanonicalLength is the length of a canonical hyphenated UUID,
// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
const uuidCanonicalLength = 36

// IsCanonicalUUID reports whether s is a UUID in canonical hyphenated form.
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
// They are the middle segment of a topic name and nothing more. The topic-naming layer
// in the root package composes `<prefix>.<category>` and `<prefix>.<category>.dlt` from
// them, applying the configured topic prefix (KAFKA_TOPIC_PREFIX, default "blnk"), and
// it is that layer — never this one — that owns the resulting names:
//
//	transactions → blnk.transactions → blnk.transactions.dlt
//	balances     → blnk.balances     → blnk.balances.dlt
//	identities   → blnk.identities   → blnk.identities.dlt
//	system       → blnk.system       → blnk.system.dlt         (internal)
//
// FOUR CATEGORIES, EIGHT TOPICS, AND THE LIST IS CLOSED. It is the published topic
// contract: subscribers, the provisioning script, the Kubernetes configuration and the
// local stack all enumerate exactly these names, so a further category here silently
// obliges every one of them to be changed too — and obliges every subscriber that has
// already built against the published catalogue to change with them.
//
// Nothing in this file knows the prefix, builds a topic name, or appends the `.dlt`
// suffix. Treating a value returned from here as a topic is a bug.
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
	EventCategorySystem = "system"
)

// SubscriberGrantableEventCategories returns the categories a subscriber may be
// authorised to consume, in a stable order.
//
// It is the allowlist the subscriber DTO validation and the Kafka ACL provisioning both
// check against, so that "which topics may a subscriber be granted?" has one answer
// rather than one per caller.
//
// THE THREE TENANT CATEGORIES ARE GRANTABLE — transactions, balances and identities.
// EventCategorySystem IS NOT, and that exclusion is the point of this function existing
// at all: `<prefix>.system` carries system.error's frozen verbatim-error body and is
// the catalogue's catch-all, so it is an OPERATOR topic in the same class as every
// `<topic>.dlt`.
//
// EVERY MIGRATED EVENT TYPE HAS A ROUTE THROUGH ONE OF THE TWO LISTS, and for two of
// them that route is the privileged one. `ledger.created` shares `<prefix>.system` with
// `system.error` because the published topic catalogue puts it there, so a subscriber
// that consumed it over the legacy webhook transport needs the deployment-level
// acknowledgement and an explicit grant to keep receiving it after the cutover — with
// the disclosure that comes attached.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, in canonical order.
func SubscriberGrantableEventCategories() []string {
	grantable := make([]string, 0, len(eventCategoryOrder))
	for _, category := range eventCategoryOrder {
		// The ONE exclusion, expressed as a filter over the canonical order rather than as a
		// second hand-written list. A second list would drift the day a category is added:
		// the addition would appear in the catalogue and silently not here, or here and not
		// in the catalogue. Filtering keeps the order and the membership single-sourced and
		// makes the exclusion the only local decision.
		if isSubscriberPrivilegedEventCategory(category) {
			continue
		}

		grantable = append(grantable, category)
	}

	return grantable
}

// SubscriberPrivilegedEventCategories returns the categories a subscriber may be
// authorised to consume ONLY under an explicit deployment-level acknowledgement, in
// canonical order.
//
// A category on this list has a real subscriber audience and a disclosure that audience
// must be accepted on purpose.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, in canonical order.
func SubscriberPrivilegedEventCategories() []string {
	privileged := make([]string, 0, len(eventCategoryOrder))
	for _, category := range eventCategoryOrder {
		if isSubscriberPrivilegedEventCategory(category) {
			privileged = append(privileged, category)
		}
	}

	return privileged
}

// isSubscriberPrivilegedEventCategory is the single membership test behind both lists,
// so a category cannot be absent from the default list and absent from the privileged
// one as well — which is how a category would become ungrantable by accident.
func isSubscriberPrivilegedEventCategory(category string) bool {
	return category == EventCategorySystem
}

// SubscriberGrantableTopics returns the fully-resolved topic names a subscriber may be
// authorised for under prefix, in the canonical category order.
//
// Three layers ask that question — the API request DTO, the persistence boundary and
// the Kafka ACL provisioner — and each of them refuses a grant that is not on this
// list. Three independent reconstructions of the same list is three chances for one of
// them to drift, and the failure mode of drift is a topic that one layer refuses and
// another grants.
//
// What the list EXCLUDES is the security-relevant part, and there are TWO exclusions.
//
// DEAD-LETTER topics. A `<topic>.dlt` holds events that already failed, together with
// failure metadata naming broker addresses and internal error reasons.
//
// `<prefix>.system`. The exclusion comes from SubscriberGrantableEventCategories, which
// is where the full reasoning lives.
//
// So the list is exactly the THREE TENANT category topics: `<prefix>.transactions`,
// `<prefix>.balances` and `<prefix>.identities`. Eleven of the thirteen migrated event
// types have an authorized subscriber path through it, and the remaining two —
// `ledger.created` and `system.error`, which the published catalogue places together on
// `<prefix>.system` — have one through the privileged list, so every event type that
// ever reached a webhook subscriber has a credential-reachable route after the cutover.
//
// Parameters:
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names, safe for the caller to
//     retain or sort.
func SubscriberGrantableTopics(prefix string) []string {
	return composeCategoryTopics(prefix, SubscriberGrantableEventCategories())
}

// SubscriberPrivilegedTopics returns the topic names a subscriber may be authorised for
// ONLY in a deployment that has acknowledged the disclosure, in canonical order.
//
// Parameters:
//   - prefix string: the namespace this deployment owns, trimmed, defaulted as above.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names. Never nil.
func SubscriberPrivilegedTopics(prefix string) []string {
	return composeCategoryTopics(prefix, SubscriberPrivilegedEventCategories())
}

// SubscriberAuthorizableTopics returns every topic a subscriber may be authorised for
// in THIS deployment: the default grantable names, plus the privileged ones when the
// deployment has acknowledged them.
//
// Parameters:
//   - prefix string: the namespace this deployment owns, trimmed, defaulted as above.
//   - includePrivileged bool: whether the deployment has acknowledged the privileged
//     categories.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names, safe to retain or sort.
func SubscriberAuthorizableTopics(prefix string, includePrivileged bool) []string {
	topics := SubscriberGrantableTopics(prefix)
	if !includePrivileged {
		return topics
	}

	return append(topics, SubscriberPrivilegedTopics(prefix)...)
}

// composeCategoryTopics applies prefix to categories, and is the ONE place a category token
// becomes a topic name in this file, so the prefix defaulting cannot differ between the
// default list and the privileged one.
func composeCategoryTopics(prefix string, categories []string) []string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultEventTopicPrefix
	}

	topics := make([]string, 0, len(categories))
	for _, category := range categories {
		topics = append(topics, prefix+"."+category)
	}

	return topics
}

// IsSubscriberGrantableTopicName reports whether topic is one a subscriber may be
// granted Read and Describe on under prefix WITHOUT a deployment-level acknowledgement.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true only for an exact member of the default grantable list.
func IsSubscriberGrantableTopicName(topic, prefix string) bool {
	return IsSubscriberAuthorizableTopicName(topic, prefix, false)
}

// IsSubscriberPrivilegedTopicName reports whether topic is one of the names that
// require the deployment-level acknowledgement.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true only for an exact member of the privileged list.
func IsSubscriberPrivilegedTopicName(topic, prefix string) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	for _, privileged := range SubscriberPrivilegedTopics(prefix) {
		if topic == privileged {
			return true
		}
	}

	return false
}

// IsSubscriberAuthorizableTopicName is the membership test over
// SubscriberAuthorizableTopics: whether topic may be granted in a deployment whose
// acknowledgement state is includePrivileged.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//   - includePrivileged bool: the deployment's acknowledgement state.
//
// Returns:
//   - bool: true only for an exact member of the resolved list.
func IsSubscriberAuthorizableTopicName(topic, prefix string, includePrivileged bool) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	for _, authorizable := range SubscriberAuthorizableTopics(prefix, includePrivileged) {
		if topic == authorizable {
			return true
		}
	}

	return false
}

// eventCategoryOrder is the canonical order every category listing uses, so that topic
// inventories, reports and log lines are deterministic.
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

// eventTypeCategories is THE CATALOGUE: every event type Blnk emits under a fixed name,
// mapped to the category whose topic it is published to.
//
// The bulk transaction family is deliberately ABSENT. Its names are composed at runtime
// as bulkTransactionEventPrefix plus the batch status, so the suffix set is open and no
// table can enumerate it; both readers match it by prefix before consulting this map.
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
	// ledger.created and system.error share the fourth category, which is what the agreed
	// plan's topic table specifies. A subscriber therefore reaches ledger.created only
	// through the PRIVILEGED grant of `<prefix>.system` — the same grant that discloses
	// system.error's verbatim internal error text — and that cost is published in
	// docs/event-streaming.md rather than worked around by adding a category to the
	// contract. See EventCategorySystem and SubscriberPrivilegedEventCategories.
	"ledger.created": EventCategorySystem,
	// system.error renders Blnk's own error text verbatim, and this category is the
	// catalogue's catch-all, which together are why the category is privileged rather than
	// ordinary.
	"system.error": EventCategorySystem,
}

// EventKeyDimension names WHICH identifier an event type's Kafka message key is taken
// from, and it is a DECLARATION rather than a description of whatever the payload
// happened to yield.
type EventKeyDimension string

const (
	// EventKeyDimensionLedger is declared for every event type that describes ledger
	// state.
	EventKeyDimensionLedger EventKeyDimension = "ledger"

	// EventKeyDimensionAggregate is declared for event types that genuinely have no ledger
	// and whose own aggregate is the correct ordering unit. An identity is not scoped to a
	// ledger in this model and may be referenced by balances in several, so per-identity
	// ordering is the strongest guarantee that is true; a bulk batch is a runtime grouping
	// whose members may span ledgers, so the batch is its own unit and each member still
	// carries its own ledger-keyed transaction event.
	EventKeyDimensionAggregate EventKeyDimension = "aggregate"

	// EventKeyDimensionEventType is declared for event types with no aggregate of any
	// kind. Keying on the type gives the stream a single partition and therefore a total
	// order, which is what an error stream wants.
	EventKeyDimensionEventType EventKeyDimension = "event_type"
)

// eventKeyDimensions declares the key dimension of every catalogued event type.
//
// The bulk transaction family is absent for the same reason it is absent from
// eventTypeCategories — its names are composed at runtime — and
// KeyDimensionForEventType matches it by prefix.
var eventKeyDimensions = map[string]EventKeyDimension{
	"transaction.queued":    EventKeyDimensionLedger,
	"transaction.applied":   EventKeyDimensionLedger,
	"transaction.scheduled": EventKeyDimensionLedger,
	"transaction.inflight":  EventKeyDimensionLedger,
	"transaction.void":      EventKeyDimensionLedger,
	"transaction.rejected":  EventKeyDimensionLedger,
	"transaction.unknown":   EventKeyDimensionLedger,
	"balance.created":       EventKeyDimensionLedger,
	"balance.monitor":       EventKeyDimensionLedger,
	"ledger.created":        EventKeyDimensionLedger,
	"identity.created":      EventKeyDimensionAggregate,
	"system.error":          EventKeyDimensionEventType,
}

// KeyDimensionForEventType returns the declared key dimension of an event type.
//
// An event type this repository does not yet know about is routed to the system
// category by EventCategory, and nothing can be assumed about whether it describes a
// ledger. Adding a producer therefore means adding a row to this table in the same
// change, which is what the contract test enforces.
//
// Parameters:
//   - eventType string: the event name, exactly as the producer passes it.
//
// Returns:
//   - EventKeyDimension: the declared dimension; never empty.
func KeyDimensionForEventType(eventType string) EventKeyDimension {
	// Prefix first, so it cannot be shadowed by the exact-match arm below.
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return EventKeyDimensionAggregate
	}

	if dimension, declared := eventKeyDimensions[eventType]; declared {
		return dimension
	}

	return EventKeyDimensionAggregate
}

// CataloguedEventTypes returns every event type this repository emits under a fixed
// name.
//
// Returns:
//   - []string: a fresh slice in no guaranteed order; the caller may sort it freely.
func CataloguedEventTypes() []string {
	catalogued := make([]string, 0, len(eventTypeCategories))
	for eventType := range eventTypeCategories {
		catalogued = append(catalogued, eventType)
	}

	return catalogued
}

// EventKeyDimensionsByType returns a copy of the declared key-dimension table.
//
// Exported so the contract tests can assert that every catalogued event type carries a
// declaration, without the table itself becoming writable from outside this package.
//
// Returns:
//   - map[string]EventKeyDimension: a fresh map; the caller may mutate it freely.
func EventKeyDimensionsByType() map[string]EventKeyDimension {
	declared := make(map[string]EventKeyDimension, len(eventKeyDimensions))
	for eventType, dimension := range eventKeyDimensions {
		declared[eventType] = dimension
	}

	return declared
}

// IsCataloguedEventType reports whether an event type is one this repository is known
// to emit.
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
// delegates to it rather than keeping a second table, because two tables that drift
// produce a routing bug that is invisible until a subscriber notices missing events.
func EventCategory(eventType string) string {
	// Bulk transaction event names are composed at runtime as bulkTransactionEventPrefix +
	// status, so they must be matched by PREFIX and never by exact equality — the suffix
	// set is open. Three statuses are emitted today ("failed", "inflight" and "applied");
	// any status added later routes correctly with no change here.
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return EventCategoryTransactions
	}

	if category, catalogued := eventTypeCategories[eventType]; catalogued {
		return category
	}

	{
		// THE CATCH-ALL, and it is the system category.
		return EventCategorySystem
	}
}

// credentialReferenceScheme is the fixed, self-describing prefix every credential
// reference carries.
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

// ErrInvalidCredentialReference reports a value that is not a credential reference this
// package produced.
var ErrInvalidCredentialReference = errors.New(
	"model: value is not a credential reference derived by DeriveCredentialReference",
)

// DeriveCredentialReference produces the NON-REVERSIBLE reference persisted for an
// issued SASL credential.
//
// Call this at the moment of issuance, persist ONLY the result, and hand the secret
// itself to the subscriber exactly once. Never persist, log or return the secret, and
// never construct a reference by hand.
//
// HMAC-SHA-256 rather than bcrypt because the reference is NEVER used to verify a
// presented secret: nothing authenticates against it, the broker holds the SCRAM
// verifier and does that job. Bcrypt's deliberate slowness would spend the 5-second
// issuance budget for no security benefit.
//
// Parameters:
//   - principal string: the Kafka principal the credential belongs to.
//   - secret string: the generated SASL secret. Required.
//
// Returns:
//   - string: the reference, in the form "scram-sha-512-ref-v1$<64 hex chars>".
//   - error: when either input is empty, which would derive a reference that several
//     rows could share.
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

	// hex.DecodeString accepts upper case, which DeriveCredentialReference never produces,
	// so the case is checked as well as the alphabet. Two references over one credential
	// differing only in case would defeat the equality comparison the reference exists to
	// support.
	if strings.ToLower(digest) != digest {
		return ErrInvalidCredentialReference
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return ErrInvalidCredentialReference
	}

	return nil
}

// CredentialFingerprint reduces a credential reference to the short, non-sensitive form
// that is safe to put in an API response or a log line.
//
// Parameters:
//   - reference string: a reference as produced by DeriveCredentialReference.
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

// HashIdentifier is THE canonical pseudonym rule for an identifier that must be
// correlated without being disclosed.
//
// The pseudonym has to be a PIVOT ACROSS THREE LAYERS to be worth anything. A metric
// label carries it, a log line carries it, and the subscriber resource publishes it as
// subscriber_id_hash so an operator holding a token from an alert can resolve it to a
// subscriber.
//
// A second implementation in any of them would be a second thing to keep in step, and
// drift would be silent in the worst way: two tokens for one subscriber, an alert
// nobody can trace, and no error anywhere. One function, imported by all three, makes
// the pivot true by construction rather than by discipline.
//
// The rule is deliberately reproducible outside Go, which is the other half of being
// usable: SHA-256 over the exact identifier bytes, hex-encoded, first
// LogIdentifierHashLength characters. docs/kafka-operations.md publishes the shell
// equivalent.
//
// An empty input returns an empty string rather than the digest of the empty string, so
// "no identifier" and "some identifier" stay distinguishable — a keyless message was
// balanced across partitions rather than pinned to one, which is exactly what an
// ordering investigation needs to see.
//
// Parameters:
//   - value string: the identifier. Hashed verbatim, with no trimming or case folding,
//     so the caller decides what the canonical form is.
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

// EventSubscriber is one row of blnk.event_subscribers: a registered Kafka subscriber,
// the access boundary provisioned for it, and — during the dual-run only — the legacy
// webhook URL it is being migrated away from.
//
// There are no per-tenant topics: every subscriber reads the same shared category
// topics. Isolation is achieved by making each subscriber a distinct Kafka principal
// and scoping that principal with ACLs.
//
// THE BOUNDARY IS EXACTLY THREE SCOPES, and each one names where it is ENFORCED. Which
// bindings a row implies depends on whether it declares a key scope, because the two
// cases are enforced in different places:
//
//	NO KEY SCOPE — the topic grant IS the boundary, and the broker keeps all of it:
//	  Topic  <each entry of AuthorizedTopics>  LITERAL   Read + Describe   BROKER
//	  Group  <ConsumerGroupID>                 PREFIXED  Read              BROKER
//
//	KEY SCOPE RECORDED — record access is withheld at the broker, and the key
//	boundary is kept by the component the deployment DECLARED:
//	  Topic  <each entry of AuthorizedTopics>  LITERAL   Describe only     BROKER
//	  Group  <ConsumerGroupID>                 PREFIXED  Read              BROKER
//	  Key    <PartitionKeyPrefix>              prefix    consume           DECLARED GATEWAY
type EventSubscriber struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database. The
	// key callers use is SubscriberID.
	ID int64 `json:"id"`

	// --- Subscriber identity ---

	// SubscriberID is the business key and the {id} in POST
	// /subscribers/{id}/kafka-credentials. It is a '<prefix>_<uuid>' string produced by
	// GenerateUUIDWithSuffix, exactly as every other business key in this schema is.
	SubscriberID string `json:"subscriber_id"`
	// Name is the human label an operator recognises the subscriber by. It is required,
	// because an unnamed principal cannot be triaged — being able to answer "who is this
	// principal?" months later is most of the reason the registry exists.
	Name string `json:"name"`

	// --- The Kafka access model ---

	// KafkaPrincipal is the SASL/SCRAM username the ACLs are granted to.
	KafkaPrincipal string `json:"kafka_principal"`
	// ConsumerGroupID is the consumer group the subscriber reads under, returned verbatim
	// by the credential endpoint. The provisioned ACL grants Read on it with a prefixed
	// pattern type, reserving the subscriber's whole group namespace without enumerating
	// every group it might create.
	ConsumerGroupID string `json:"consumer_group_id"`
	// AuthorizedTopics is the exact set of topics the subscriber may Read and Describe,
	// and the set the ACLs are granted over. Empty means authorised for nothing — the
	// registry fails closed.
	AuthorizedTopics []string `json:"authorized_topics"`
	// PartitionKeyPrefix records a key-scoped authorization constraint that KAFKA CANNOT
	// ENFORCE, so a non-nil value makes the subscriber UNPROVISIONABLE: credential
	// issuance refuses rather than mint a credential whose real scope is every record on
	// every authorised topic.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// --- The credential record ---

	// CredentialReference is a non-reversible reference to the issued credential. It is
	// NOT the secret and nothing can be authenticated with it. Nil means no credential has
	// ever been issued.
	CredentialReference *string `json:"credential_reference,omitempty"`
	// CredentialIssuedAt is when the credential was issued, set together with
	// CredentialReference. A reissue overwrites both.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// --- Dual-run migration tracking (temporary by design) ---

	// WebhookURL is a MIGRATION RECORD, not a delivery destination. NOTHING SENDS TO IT.
	WebhookURL *string `json:"webhook_url,omitempty"`
	// MigratedAt is when the subscriber completed its move to Kafka consumption.
	// Nil means not yet migrated.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// --- Deregistration in progress ---

	// RevocationPendingAt is when deregistration began taking this subscriber's
	// broker-side access away. Nil for every ordinary subscriber.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt failed at the broker.
	// Nil when no attempt has failed since the last one started.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// --- A credential that outlived its record ---

	// CredentialOrphanedAt is when an issuance left a credential at the broker that Blnk
	// could neither record nor revoke. Nil for every healthy subscriber.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// --- Settlement obligations the background pass owes ---

	// GrantReconcilePendingAt and CredentialCleanupPendingAt are the two settlement
	// markers, stamped when a broker-side step could not be completed and cleared only
	// once it has been.
	GrantReconcilePendingAt    *time.Time `json:"grant_reconcile_pending_at,omitempty"`
	CredentialCleanupPendingAt *time.Time `json:"credential_cleanup_pending_at,omitempty"`

	// --- Row bookkeeping ---

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasTopicAccess reports whether the subscriber's RECORDED GRANT covers the given
// topic.
//
// This is a read over the recorded grant and is NOT a substitute for broker-side ACL
// enforcement. The broker is the authority; this method exists so the service layer can
// reject an obviously out-of-boundary request before spending a round trip to find out.
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
// authorised topics" — the two readings are identical and the empty string is used for
// both so no caller has to handle a third state.
func (s *EventSubscriber) RequestedKeyScope() string {
	if s == nil || s.PartitionKeyPrefix == nil {
		return ""
	}

	return strings.TrimSpace(*s.PartitionKeyPrefix)
}

// HasKeyAccess reports whether this subscriber is entitled to a record carrying the
// given message key.
func (s *EventSubscriber) HasKeyAccess(key string) bool {
	// DeclaresKeyScope, not `RequestedKeyScope() != ""`. The two differ on a
	// whitespace-only column, and here that difference decides whether EVERY record is
	// excluded: no ledger id begins with a tab, so a scope of "\t" would make this rule
	// refuse the subscriber's whole stream without an error anywhere. All three of this
	// predicate, EffectiveKeyScope and the credential contract read the same presence test
	// for that reason.
	if !s.DeclaresKeyScope() {
		return true
	}

	// The recorded value VERBATIM, not the trimmed one: keys are opaque identifiers, so
	// trimming here would test a different prefix than the registry recorded. Only the
	// question "is there a scope at all" tolerates trimming.
	return strings.HasPrefix(key, s.RequestedKeyScope())
}

// SubscriberKeyScopeAllKeys is the key scope of a credential issued to a subscriber
// that recorded no prefix, and the value the credential contract reports for it: EVERY
// key on the authorised topics.
const SubscriberKeyScopeAllKeys = "all-keys"

// EffectiveKeyScope reports the key scope a credential issued for this subscriber would
// ACTUALLY carry, together with whether that scope is enforced.
//
// Returns:
//   - scope: SubscriberKeyScopeAllKeys when no scope was requested, which is the honest
//     description of what a topic ACL grants on its own; otherwise the recorded prefix,
//     which is the boundary the consumer must apply.
//   - enforced: whether THE BROKER enforces the scope.
func (s *EventSubscriber) EffectiveKeyScope() (scope string, enforced bool) {
	// DeclaresKeyScope is the presence test, NOT `RequestedKeyScope() != ""`, and the
	// difference is a whitespace-only prefix. RequestedKeyScope returns the column
	// verbatim — correct, because message keys are opaque and normalising one would select
	// a different set of records — so a column holding a tab would otherwise be reported
	// here as a real scope. This pair is what the credential endpoint delivers to a
	// consumer, and a consumer applying a scope of "\t" discards its ENTIRE stream
	// silently.
	if s.DeclaresKeyScope() {
		return s.RequestedKeyScope(), false
	}

	return SubscriberKeyScopeAllKeys, true
}

// SubscriberSettlementObligation is one subscriber's outstanding broker-side
// obligation, as the settlement pass sees it.
//
// The worker re-reads the full row when it acts, because both remedies need the derived
// principal, the consumer group and the authorized topic set, and deriving those twice
// is how the broker-side grant and the registry drift apart in the first place.
type SubscriberSettlementObligation struct {
	// SubscriberID identifies the row that owes the work.
	SubscriberID string

	// GrantReconcilePending means the broker's ACL bindings may not match the row's recorded
	// authorization, so the grant must be reconciled to the row.
	GrantReconcilePending bool

	// CredentialCleanupPending means a SCRAM credential may exist that Blnk intended to
	// destroy, or the row names one that no longer works. Settlement revokes and then clears.
	CredentialCleanupPending bool

	// Attempts is how many settlement passes have already tried this row, and LastError is
	// what the most recent one said.
	Attempts  int
	LastError string
}

// Outstanding reports whether this obligation still requires work.
func (o SubscriberSettlementObligation) Outstanding() bool {
	return o.GrantReconcilePending || o.CredentialCleanupPending
}

// IsProvisioned reports whether a credential has ever been issued to this subscriber.
//
// It tests the credential reference rather than the issuance timestamp because the
// reference is the value the repository writes first and the two are always written
// together; either would do, and testing the reference keeps the "registered, not yet
// provisioned" state readable at the call site.
func (s *EventSubscriber) IsProvisioned() bool {
	return s != nil && s.CredentialReference != nil && *s.CredentialReference != ""
}

// MayHaveBrokerCredential reports whether a means of authenticating as this row's
// principal MIGHT exist at a broker.
//
// This is the guard that decides whether an operation which cannot reach a broker is
// allowed to proceed as though there were nothing at a broker to act on. The only safe
// answer to that question is drawn from the row's own evidence, and the evidence is
// one-sided: Blnk can be certain a credential was written, but it can never be certain
// one was not, because the failure modes that lose the record are exactly the ones that
// leave the credential behind.
//
// So this predicate is the UNION of every state that can coexist with a live
// credential, and it is deliberately conservative. A false positive costs a retryable
// refusal; a false negative costs a live SASL credential with live ACL bindings, left
// at a broker with nothing anywhere naming the principal it belongs to and no row to
// retry from.
//
// GrantReconcilePendingAt means the recorded authorization is WIDER than the broker's —
// a grant that may not exist. That is the safe direction: no authentication follows
// from a missing ACL binding, and blocking a deletion on it would strand rows whose
// only defect is that they promise less access than they were given.
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

// BrokerCredentialEvidence names WHY MayHaveBrokerCredential answered true, for the log
// line and the error detail that accompany a refusal.
//
// A refusal that says only "this subscriber may hold a credential" leaves an operator
// to work out which of four states they are looking at, and the remedies differ: an
// orphan is settled by RE-ISSUING, a tombstone by retrying the DEREGISTRATION, a
// cleanup obligation by letting the settlement pass run or forcing a revocation. Naming
// the state is what makes the refusal actionable rather than merely correct.
//
// Returns:
//   - string: a short phrase naming the strongest evidence, or "" when there is none.
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

// IsMigrated reports whether the subscriber has completed its move from legacy HTTP
// webhook delivery to Kafka consumption. A nil MigratedAt means not yet migrated, which
// is what dual-window migration-progress reporting counts.
func (s *EventSubscriber) IsMigrated() bool {
	return s != nil && s.MigratedAt != nil
}

// IsRevocationPending reports whether deregistration has begun taking this subscriber's
// broker-side access away and has not yet confirmed it.
//
// Such a row is not an active subscriber. It exists only so the outstanding revocation
// stays recoverable, and issuing a credential for it would re-arm a principal that is
// in the middle of being taken out of service.
func (s *EventSubscriber) IsRevocationPending() bool {
	return s != nil && s.RevocationPendingAt != nil
}

// RequiresGatewayDelivery reports whether the subscriber records a partition key
// prefix, and therefore that its records may only be delivered through the
// key-authorising component an operator has declared in front of the brokers.
//
// The empty string is treated as absent for the same reason the column is nullable: "a
// constraint on the empty prefix" is not an intent anybody has.
//
// Returns:
//   - bool: true when a non-blank partition key prefix is recorded.
func (s *EventSubscriber) RequiresGatewayDelivery() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// GrantsBrokerRecordAccess reports whether this subscriber's credential may fetch
// records DIRECTLY from the broker.
//
// False means the only boundary the row asks for is one the broker cannot evaluate, so
// record access is withheld and the declared key-authorising component is the only path
// records can take.
//
// Returns:
//   - bool: true when no key scope is recorded.
func (s *EventSubscriber) GrantsBrokerRecordAccess() bool {
	return !s.RequiresGatewayDelivery()
}

// Two registry subscribers then shared one credential and one ACL set, and reissuing
// for either silently rewrote the other's.

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

// ErrInvalidSubscriberIdentifier reports an identifier that cannot be used to derive a
// Kafka identity.
//
// It is a sentinel so the API layer, the repository and the provisioning path all
// recognise the same refusal without matching message text, and so that "this
// identifier is unusable" is provably one condition rather than three similar checks.
var ErrInvalidSubscriberIdentifier = errors.New(
	"model: subscriber identifier cannot be used to derive a Kafka identity",
)

// CanonicalizeSubscriberIdentifier returns the identifier in its one permitted
// spelling, or refuses it.
//
// The accepted alphabet is lowercase ASCII letters, digits, underscore and hyphen,
// starting with a letter or digit. Everything else is refused, and the refusal names
// the reason:
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
		// The message names the exact rule broken, and an operator who sees "surrounding
		// whitespace" fixes it immediately.
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
// This is the ONLY way a principal may be produced. A principal supplied by a caller is
// not a request for a name, it is a request for a boundary, so provisioning compares
// what it was given against what this derives and refuses a mismatch.
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

// CanonicalConsumerGroupNamespace derives the PREFIXED ACL resource name that reserves
// a subscriber's consumer group namespace.
//
// The trailing terminator is the disjointness guarantee: it cannot appear inside a
// canonical identifier, so no subscriber's namespace can be a prefix of another's.
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

// CanonicalConsumerGroupID derives the consumer group a subscriber reads under by
// default.
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

// IsInSubscriberGroupNamespace reports whether a consumer group lies inside a
// namespace.
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
	// EventTypeTransactionUnknown is a REAL, REACHABLE value and not a placeholder. The
	// COMMIT status has no case in EventTypeForTransactionStatus and falls through to it.
	// See that function for why that is preserved deliberately.
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
// The root function delegates here so there is one table rather than two that can
// drift.
//
// Comparison is case-insensitive, exactly as the original is, so a status arriving in
// any casing resolves the same way.
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
	MaxSubscriberTopics = 16

	// MaxTopicNameLength is Kafka's own limit on the length of a topic name.
	MaxTopicNameLength = 249
)

// eventIDNamespace is the fixed UUID namespace every derived event id is generated
// under.
//
// It must never change. Every derived id is a function of this value, so a new
// namespace would give the same logical event a different id — and the unique index on
// event_id, which is what makes a logical retry idempotent, would stop recognising it.
var eventIDNamespace = uuid.MustParse("6f2d1a55-9f6b-4a1e-8d2c-0f43a1b7c9e1")

// DeriveEventID derives the deterministic event id for one logical event.
//
// event_id carries TWO contracts at once: it is the subscriber's idempotency key, and
// it is the unique index that makes the outbox's write side exactly-once. A freshly
// random id per preparation satisfies neither for a RETRY.
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
// Returns:
//   - string: a canonical, textual version-4 UUID.
func NewEventID() string {
	return uuid.New().String()
}

// EventIdentityFor reports the mutation identity a deterministic event id may be
// derived from, and whether the event has one at all.
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
		// A map payload is only a batch when the event type says so. Reading batch_id out of
		// some other map-shaped payload would derive an id from a field that means something
		// else entirely.
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
// Everything else describes a mutation that happens once. A caller must not read a
// false answer here as "derivable", though: whether an id CAN be derived also depends
// on the payload actually carrying an identity, which only EventIdentityFor can say.
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
// It is opaque on purpose. A human-readable identifier ends up naming the customer, and
// the identifier is copied verbatim into a Kafka principal and a consumer group id,
// both of which are visible to every operator with broker access and appear in broker
// logs.
//
// Returns:
//   - string: "sub_<uuid>".
func GenerateSubscriberID() string {
	return GenerateUUIDWithSuffix(SubscriberIDPrefix)
}

// GenerateConsumerGroupID mints a consumer group id inside a freshly generated
// subscriber's namespace.
//
// Returns:
//   - string: "blnk-sub-<generated identifier>.default".
func GenerateConsumerGroupID() string {
	group, err := CanonicalConsumerGroupID(GenerateSubscriberID())
	if err != nil {
		// Unreachable: GenerateSubscriberID is canonical by construction. Falling back to the
		// namespace of a fixed leaf keeps the shape valid rather than returning "", which
		// would read as "no group" to every caller.
		return ConsumerGroupIDPrefix + SubscriberIDPrefix + SubscriberGroupTerminator + SubscriberDefaultGroupLeaf
	}

	return group
}

// isLegalKafkaTopicName reports whether every character is one Kafka permits in a topic
// name.
//
// Kafka permits only [a-zA-Z0-9._-]. Anything else cannot name a real topic, so it is
// either a mistake or an attempt to smuggle a separator, a quote or a brace into a
// value that is about to be composed into an array literal and an authorization rule.
//
// Parameters:
//   - name string: the candidate topic name.
//
// Returns:
//   - bool: true when every character is legal.
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

// ValidateSubscriberTopics checks a topic grant against the bounds the registry stores
// it under, and is the SINGLE definition of those bounds.
//
// The empty grant is VALID and must stay valid: the column is NOT NULL DEFAULT '{}'
// precisely so that a registered-but-unprovisioned subscriber is authorised for
// nothing, and rejecting it here would make that legitimate state unrepresentable.
//
// Parameters:
//   - topics []string: the grant to check. Nil and empty are both accepted.
//
// Returns:
//   - error: nil when the grant is within every bound, otherwise a plain error naming
//     the offending rule and, where it helps, the offending element.
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

// ----------------------------------------------------------------------------- Webhook
// destination policy
// -----------------------------------------------------------------------------

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
// Parameters:
//   - address net.IP: the address, as parsed.
//
// Returns:
//   - string: the reason, or "" when the address is acceptable.
func InternalIPReason(address net.IP) string {
	if address == nil {
		return "it is not a valid IP address"
	}

	// NAT64 first, and deliberately so: the embedded address is what the packet ultimately
	// reaches, and every predicate below would describe the wrapper as ordinary global
	// unicast.
	if embedded := nat64EmbeddedIPv4(address); embedded != nil {
		if reason := InternalIPReason(embedded); reason != "" {
			return "it embeds an internal IPv4 address in the NAT64 well-known prefix, and " + reason
		}
	}

	// Ordered so that the reason REPORTED is the accurate one, not merely a refusal. The
	// pairs overlap: 224.0.0.0/24 and ff02::/16 are both link-local AND multicast, and
	// describing a multicast group as "the cloud instance metadata endpoint" would send an
	// operator to investigate the wrong thing. Unicast link-local is checked on its own,
	// and every remaining multicast form — link-local, interface-local, global — falls to
	// the multicast arm.
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
// This is a LITERAL check and is deliberately not sold as more than one: a public
// hostname resolving to an internal address is not detectable here, which is exactly
// why the send-time dialer judges the resolved address as well.
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
// Refusing every internal address outright would be simpler and wrong. An on-premise
// deployment delivering to https://webhooks.corp:8443 behind RFC1918 is a completely
// legitimate configuration, and so is a developer delivering to a loopback test sink.
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

// BalanceMonitorHandoff is one row of blnk.balance_monitor_handoff: the durable,
// self-contained record that a balance moved and its monitors have not been evaluated
// yet, carrying both inputs that evaluation depends on.
//
// TOGETHER THEY ALSO MAKE A RETRY IDEMPOTENT IN OUTCOME.
type BalanceMonitorHandoff struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database.
	ID int64 `json:"id"`
	// HandoffID is the business key, and it carries a unique index.
	HandoffID string `json:"handoff_id"`
	// BalanceID is the balance whose monitors are to be evaluated.
	BalanceID string `json:"balance_id"`
	// LedgerID is the ledger the balance belongs to, carried so the resulting event can be
	// attributed to it without a second read. A balance always has one, so this is empty
	// only on a row written from an incomplete snapshot.
	LedgerID string `json:"ledger_id,omitempty"`
	// BalanceSnapshot is the balance as the transaction wrote it, marshalled.
	BalanceSnapshot json.RawMessage `json:"balance_snapshot"`
	// MonitorSnapshot is the monitor definitions in force when that transaction committed,
	// read inside it and marshalled as a JSON array.
	MonitorSnapshot json.RawMessage `json:"monitor_snapshot,omitempty"`
	// Status is the relay state machine: pending, processing, completed or failed. The
	// values are the OutboxStatus* constants, shared with the two existing outboxes rather
	// than duplicated, so one vocabulary describes all three.
	Status string `json:"status"`
	// Attempts is how many evaluation attempts this row has had.
	Attempts int `json:"attempts"`
	// MaxAttempts is the budget beyond which the row is marked failed.
	MaxAttempts int `json:"max_attempts"`
	// LastError is the most recent failure reason, empty when there has been none.
	LastError string `json:"last_error,omitempty"`
	// EventsCaptured is how many balance.monitor events the completed evaluation produced.
	// Zero is the common and correct answer: it means the monitors were evaluated and none
	// of their conditions was met.
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

// Monitors decodes the monitor definitions this handoff carries.
//
// The monitors are the definitions in force when the balance's transaction committed —
// see the note on MonitorSnapshot for why the evaluation must judge those and not
// whatever blnk.balance_monitors holds now.
//
// Returns:
//   - []BalanceMonitor: the snapshotted definitions, nil when none was carried.
//   - bool: true when this row carries a snapshot at all.
//   - error: a decode failure, which means the row is not evaluable and should be
//     failed rather than retried; re-reading the same bytes cannot succeed later.
func (h *BalanceMonitorHandoff) Monitors() ([]BalanceMonitor, bool, error) {
	if h == nil || len(h.MonitorSnapshot) == 0 {
		return nil, false, nil
	}

	// A JSON `null` is a carried snapshot that decodes to nothing, and it is distinguished
	// here because json.Unmarshal into a slice leaves it nil without error — which would
	// otherwise be indistinguishable from a decode that produced an empty array.
	if string(bytes.TrimSpace(h.MonitorSnapshot)) == "null" {
		return nil, true, nil
	}

	monitors := []BalanceMonitor{}
	if err := json.Unmarshal(h.MonitorSnapshot, &monitors); err != nil {
		return nil, true, fmt.Errorf("failed to decode the balance monitor snapshot: %w", err)
	}

	return monitors, true, nil
}

// balanceMonitorHandoffPrefix is the identifier prefix for a handoff, following the
// repository's `<module>_<uuid>` convention so an id is self-describing in a log line.
const balanceMonitorHandoffPrefix = "bmh"

// PrepareBalanceMonitorHandoffs builds one handoff per supplied balance that HAS a
// monitor.
//
// The monitors are keyed BY BALANCE ID rather than passed as one flat slice, because a
// handoff is per balance and a flat slice would make every evaluator re-derive the
// split.
//
// Parameters:
//   - balances []*Balance: the balances being updated, in post-mutation state.
//   - monitors map[string][]BalanceMonitor: the monitor definitions read inside the
//     caller's transaction, keyed by balance id.
//
// Returns:
//   - []*BalanceMonitorHandoff: one handoff per monitored balance, in input order.
//   - error: a marshal failure on any balance or monitor set.
func PrepareBalanceMonitorHandoffs(
	balances []*Balance,
	monitors map[string][]BalanceMonitor,
) ([]*BalanceMonitorHandoff, error) {
	if len(balances) == 0 || len(monitors) == 0 {
		return nil, nil
	}

	handoffs := make([]*BalanceMonitorHandoff, 0, len(balances))
	for _, balance := range balances {
		if balance == nil {
			continue
		}

		balanceID := strings.TrimSpace(balance.BalanceID)
		if balanceID == "" {
			continue
		}

		// NO MONITOR, NO ROW. This is the write-amplification guard, and it is also what
		// makes an empty MonitorSnapshot on a stored row mean "written before the column
		// existed" rather than "no monitors": a row is never written for an empty set.
		balanceMonitors := monitors[balanceID]
		if len(balanceMonitors) == 0 {
			continue
		}

		snapshot, err := json.Marshal(balance)
		if err != nil {
			return nil, fmt.Errorf("failed to snapshot balance %s for monitor evaluation: %w", balanceID, err)
		}

		monitorSnapshot, err := json.Marshal(balanceMonitors)
		if err != nil {
			return nil, fmt.Errorf("failed to snapshot the monitors of balance %s: %w", balanceID, err)
		}

		handoffs = append(handoffs, &BalanceMonitorHandoff{
			HandoffID:       GenerateUUIDWithSuffix(balanceMonitorHandoffPrefix),
			BalanceID:       balanceID,
			LedgerID:        strings.TrimSpace(balance.LedgerID),
			BalanceSnapshot: snapshot,
			MonitorSnapshot: monitorSnapshot,
			Status:          OutboxStatusPending,
		})
	}

	return handoffs, nil
}

// BalanceMonitorEventIdentity is the stable identity of the alert one monitor produces
// for one balance movement.
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
const (
	// BulkBatchStatusProcessing means the batch began and has not reported an outcome. A
	// row that stays here is the one residue the coordinator cannot remove: the process
	// handling the batch died before it could finalise.
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
// This row gives the outcome a home, and the finalising transaction writes the outcome
// and the event TOGETHER. Two facts follow, and they are the guarantee:
//
//   - the event cannot be missing while the outcome is recorded, and
//   - the outcome cannot be recorded while the event is missing.
type BulkTransactionBatch struct {
	// BatchID is the parent transaction id of the batch and the primary key. It is also
	// the event's aggregate id, so the coordinator row and the event it produced are
	// joinable on it.
	BatchID string `json:"batch_id"`
	// Status is one of the BulkBatchStatus* constants.
	Status string `json:"status"`
	// TransactionCount is the number of transactions in the batch. It is zero on a
	// failed batch, matching the payload the failure path has always sent.
	TransactionCount int `json:"transaction_count"`
	// ErrorMessage is the failure detail including rollback status, empty on success.
	ErrorMessage string `json:"error_message,omitempty"`
	// Atomic records whether a failure rolls the whole batch back, and Inflight whether
	// its transactions are left inflight. Both are carried so a stuck row tells an
	// operator what the batch was ATTEMPTING, which is what decides how to finish it by
	// hand.
	Atomic   bool `json:"atomic"`
	Inflight bool `json:"inflight"`
	// EventID is the outbox event that recorded this outcome, empty until finalised. It is
	// the proof of the atomicity: a terminal row without one would mean the outcome was
	// recorded without its event, which the finalising transaction makes unreachable.
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
//   - status string: the status to classify.
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
type EventOutboxAudit struct {
	// PublishedRows is how many rows claim a record on the broker: every row whose Kafka
	// leg completed, plus every dead-lettered row, each counted exactly once by virtue of
	// the unique index on event_id.
	PublishedRows int64

	// ConfirmedRows is how many of those name the record they produced.
	ConfirmedRows int64

	// DistinctRecords is how many DISTINCT coordinates those rows name. It equals
	// ConfirmedRows unless two rows claim the same record, which the partial unique index
	// on the coordinate makes impossible — so a discrepancy here means the index is
	// missing or has been dropped, and the audit says so rather than assuming the schema
	// is intact.
	DistinctRecords int64

	// MeasuredAt is when the outbox side was read.
	MeasuredAt time.Time

	// WindowStart is the earliest publication instant the three counts above include.
	//
	// The counts are one half of a comparison whose other half is a Kafka offset reading,
	// and the two halves must describe the SAME population or the comparison means
	// nothing. Over the whole history they cannot: broker end offsets are cumulative for
	// the life of a topic and are unaffected by Kafka retention, while the outbox's
	// retention sweep DELETES terminal rows — so the longer a deployment runs with
	// retention enabled, the further the outbox side falls behind a broker side that never
	// forgets, and the "surplus" the verdict tolerates grows without bound until it can
	// hide any amount of loss.
	//
	// A shared window fixes both directions at once: rows whose publication instant falls
	// inside it, against records the broker wrote inside it. Retention shorter than the
	// window is then the only thing that can invalidate the reading, and it is detectable
	// rather than silent — see the truncation flag on the offset report.
	//
	// It is also what makes the query bounded. At 500 events per second the table grows by
	// 43.2 million rows a day, so an exact whole-history aggregate is a scan whose cost
	// rises for ever while answering a question about the last day.
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

// DeclaresKeyScope reports whether this subscriber records a partition-key scope.
//
// A non-nil PartitionKeyPrefix says "this subscriber is entitled only to the records
// whose partition key starts with this". Because Blnk keys every event by ledger id,
// that is a ledger boundary expressed in the value the transport already carries.
//
// Returns:
//   - bool: true when a non-blank partition-key prefix is recorded.
func (s *EventSubscriber) DeclaresKeyScope() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// There is no internalEventCategories set and no exported IsInternalEventCategory
// predicate, and the absence is deliberate now that the membership has ONE holder.

// KeyScopeEnforcementStatus names WHERE a subscriber's key scope is enforced.
type KeyScopeEnforcementStatus string

const (
	// KeyScopeEnforcementNone is reported when no key scope is recorded: the
	// broker-enforced topic and consumer-group ACLs are the subscriber's entire boundary
	// and there is nothing left for a consumer to filter.
	KeyScopeEnforcementNone KeyScopeEnforcementStatus = "none"

	// KeyScopeEnforcementGateway is reported when a key scope IS recorded AND the
	// deployment has declared a key-authorising component in front of the brokers: that
	// component applies the recorded prefix to every record's key before returning it.
	// Blnk does not ship it, and where none is declared no credential exists to report a
	// status for, because issuance fails closed.
	KeyScopeEnforcementGateway KeyScopeEnforcementStatus = "broker_gateway"
)

// KeyScopeEnforcement reports where this subscriber's key scope is enforced.
//
// A nil receiver answers KeyScopeEnforcementNone, because a subscriber that does not
// exist has recorded nothing — and because this is read on rows loaded from a
// repository whose not-found representation is a nil pointer.
//
// Returns:
//   - KeyScopeEnforcementStatus: Gateway when a non-blank prefix is recorded, otherwise
//     None.
func (s *EventSubscriber) KeyScopeEnforcement() KeyScopeEnforcementStatus {
	if s.DeclaresKeyScope() {
		return KeyScopeEnforcementGateway
	}

	return KeyScopeEnforcementNone
}

// SubscriberKeyScopeState is how far a subscriber's key scope has actually got, from a
// prefix somebody typed into a registration body to a boundary a component confirmed it
// is keeping.
type SubscriberKeyScopeState string

const (
	// SubscriberKeyScopeStateNotRequested means no prefix is recorded, so there is no key
	// scope to enforce and none is claimed. The subscriber's boundary is its topic grant
	// and its consumer-group namespace, both of which the broker keeps in full.
	SubscriberKeyScopeStateNotRequested SubscriberKeyScopeState = "not_requested"

	// SubscriberKeyScopeStateRequested means a prefix IS recorded and the deployment
	// declares nothing that can keep it.
	SubscriberKeyScopeStateRequested SubscriberKeyScopeState = "requested"

	// SubscriberKeyScopeStateAvailable means a prefix is recorded AND the deployment
	// declares a key-authorising component with a reachable attestation endpoint, so a
	// credential for this row can be minted.
	SubscriberKeyScopeStateAvailable SubscriberKeyScopeState = "available"

	// SubscriberKeyScopeStateAttested means the declared component answered an
	// authenticated request confirming it is applying this exact prefix for this exact
	// principal.
	SubscriberKeyScopeStateAttested SubscriberKeyScopeState = "attested"
)

// SubscriberAccessDeployment is the DEPLOYMENT STATE a subscriber projection has to
// know before it can describe that subscriber's access truthfully.
type SubscriberAccessDeployment struct {
	// KeyScopeEnforcement is where this deployment can enforce a partition-key scope,
	// resolved from config.KafkaConfig.KeyScopeGateway — so it is Gateway only when the
	// mode is declared, the gateway address list is non-empty and distinct from
	// KAFKA_BROKERS, AND an attestation endpoint is configured.
	KeyScopeEnforcement KeyScopeEnforcementStatus

	// SubscriberBrokersAdvertised reports whether KAFKA_SUBSCRIBER_BROKERS names an
	// externally advertised bootstrap list, from
	// config.KafkaConfig.SubscriberFacingBrokers.
	SubscriberBrokersAdvertised bool

	// WholeTopicAccessPermitted reports whether this deployment may be issued a credential
	// that reads a granted topic in full: true outside secure mode, or in secure mode once
	// KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS declares the model.
	WholeTopicAccessPermitted bool
}

// KeyScopeStateFor resolves how far a recorded prefix has got in THIS deployment.
//
// One derivation, so a boolean, a place name and a state in the same response body
// cannot describe three different subscribers. Callers that have also obtained an
// attestation report SubscriberKeyScopeStateAttested themselves; this answers
// everything knowable without a round trip.
//
// Parameters:
//   - partitionKeyPrefix string: the prefix recorded on the row. Blank means none.
//
// Returns:
//   - SubscriberKeyScopeState: NotRequested for a blank prefix, Available when the
//     deployment declares a usable enforcement point, otherwise Requested.
func (d SubscriberAccessDeployment) KeyScopeStateFor(partitionKeyPrefix string) SubscriberKeyScopeState {
	if strings.TrimSpace(partitionKeyPrefix) == "" {
		return SubscriberKeyScopeStateNotRequested
	}

	if d.KeyScopeEnforcement == KeyScopeEnforcementGateway {
		return SubscriberKeyScopeStateAvailable
	}

	return SubscriberKeyScopeStateRequested
}
