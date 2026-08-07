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

package blnk

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// This file is the publishing layer of the Kafka event pipeline. It answers exactly
// one question — how does a model.LedgerEvent get onto a Kafka topic and what
// happened when it did — and it owns nothing else:
//
//   - The EventPublisher contract every producer of events depends on.
//   - The Kafka-backed implementation, holding one writer per topic.
//   - The no-op implementation selected when no brokers are configured.
//   - PublishResult, the observability record of a single attempt.
//
// Four responsibilities that live NEXT DOOR are deliberately absent, because
// duplicating any of them would create two sources of truth for one decision:
//
//   - Topic and dead-letter NAMING belongs to event_topics.go. This file resolves a
//     destination by calling TopicForEvent and enumerates its writers from
//     AllTopicsWithDeadLetters; it never composes a name from a prefix itself.
//   - RETRY belongs to the relay. Nothing here sleeps, counts attempts against a
//     budget, or decides that an event has failed for the last time. The writer's own
//     internal retry loop is switched OFF (see eventWriterMaxAttempts) precisely so
//     the relay's bounded exponential backoff is the only retry in the system.
//   - DEAD-LETTERING belongs to event_dlt.go. This file can publish TO a dead-letter
//     topic — it is just another topic with a writer — but it never decides that an
//     event should be dead-lettered and never composes the failure metadata.
//   - TOPIC CREATION belongs to event_admin.go's topic assurance, which applies the
//     configured partition count and replication factor. Auto-creation is explicitly
//     disabled on every writer here (see eventWriterAllowAutoTopicCreation).

// Writer and transport tuning. Every value below is set EXPLICITLY on every writer
// rather than left to kafka-go's defaults, and each one is load-bearing: three of the
// library defaults are actively wrong for this pipeline, and relying on the remaining
// ones would let a library upgrade change ledger-event durability without a code
// change.
const (
	// eventWriterRequiredAcks makes the broker acknowledge a write only once every
	// in-sync replica has it, which is the strongest durability setting Kafka offers
	// and the at-least-once guarantee this pipeline is built on.
	//
	// This MUST be set explicitly. kafka-go substitutes RequireAll for a zero value
	// only inside its deprecated NewWriter(WriterConfig) constructor; a Writer built
	// as a struct literal — which is the supported construction style, and what this
	// file uses — keeps RequiredAcks at its zero value, and that zero value is
	// kafka.RequireNone. Under RequireNone the produce call returns as soon as the
	// request is written to the socket, with no acknowledgement of any kind, so
	// WriteMessages would report success for events the cluster never stored and the
	// relay would mark their outbox rows dispatched. Leaving this field out is
	// therefore not a stylistic choice; it silently converts the whole pipeline to
	// fire-and-forget.
	eventWriterRequiredAcks = kafka.RequireAll

	// eventWriterMaxAttempts is 1, which disables the writer's own retry loop.
	//
	// kafka-go retries a failed produce up to 10 times by default, with its own
	// backoff. That is one retry policy too many: requirement R-4 puts the schedule
	// (base 1s, multiplier 2, cap 30s, 5 attempts) and the per-attempt logging in the
	// relay, and a hidden second policy underneath it would multiply the real attempt
	// count by ten, make the logged attempt number a fiction, and stretch a single
	// "attempt" far past the publish-latency budget. One attempt per call means the
	// error the relay sees is the error the broker produced.
	eventWriterMaxAttempts = 1

	// eventWriterBatchSize is 1 so a synchronous write flushes immediately.
	//
	// kafka-go batches to 100 messages by default and a synchronous WriteMessages
	// blocks until the batch is full OR the batch timeout expires — a one-second
	// floor on the latency of every publish that is not part of a burst. The relay
	// publishes and then marks each row individually, so batching across rows buys
	// nothing here while costing a second of latency against the sub-two-second p99
	// target. Throughput comes from the relay's concurrency over a shared writer,
	// which is the usage kafka-go's own documentation recommends.
	eventWriterBatchSize = 1

	// eventWriterBatchTimeout bounds how long a partially-filled batch waits. It is
	// unreachable while eventWriterBatchSize is 1 — every message completes a batch on
	// arrival — and is pinned to a small value anyway so that raising the batch size
	// later cannot silently reintroduce kafka-go's one-second default flush delay.
	eventWriterBatchTimeout = 10 * time.Millisecond

	// eventWriterWriteTimeout bounds the produce round trip. It matters more than it
	// looks: kafka-go bounds the produce call with its own background context and this
	// timeout, NOT with the context passed to WriteMessages, so this value — not the
	// caller's deadline — is what stops a single attempt hanging on an unresponsive
	// broker. It matches kafka-go's own default, so behaviour is pinned rather than
	// changed.
	eventWriterWriteTimeout = 10 * time.Second

	// eventWriterReadTimeout bounds the metadata read that resolves a topic's
	// partition count before balancing. Unlike the produce call, that read does honour
	// the caller's context, so this is a ceiling rather than the only bound.
	eventWriterReadTimeout = 10 * time.Second

	// eventWriterAsync is false: WriteMessages must block until the broker
	// acknowledges. An asynchronous writer returns nil the moment a message is
	// queued, so every publish would look successful, every outbox row would be
	// marked dispatched, and a broker outage would be invisible until subscribers
	// reported missing events. Set explicitly for the same reason as the acks.
	eventWriterAsync = false

	// eventWriterBatchBytes is the writer's own byte ceiling, and it is set EXPLICITLY
	// rather than left to kafka-go's 1 MiB default.
	//
	// The default is the problem. A dead-letter copy of an event is the ORIGINAL
	// envelope plus its failure metadata — original topic, error reason, attempt count,
	// and two timestamps — so it is always larger than the message that failed. An
	// event that fits under a 1 MiB producer limit on its first publish can therefore
	// exceed it on the dead-letter write, at which point the event cannot be preserved
	// at the exact moment preservation is the only thing left to do.
	//
	// The ceiling here is deliberately ABOVE model.MaxEventMessageBytes, which is what
	// the application enforces, so the two limits cannot fight: an event is refused by
	// application validation with a diagnosable message long before the writer would
	// refuse it with a produce error, and the headroom between them is what absorbs the
	// failure metadata. Both remain comfortably under Kafka's own default
	// message.max.bytes of roughly 1 MiB.
	eventWriterBatchBytes = 1000000

	// eventWriterAllowAutoTopicCreation is false so that writing to a topic which
	// does not exist FAILS instead of creating one.
	//
	// Broker-side auto-creation would use the cluster's default partition count and
	// replication factor — typically one partition and one replica — quietly
	// discarding both guarantees this pipeline is built on: six partitions for
	// parallelism and the configured replication factor for durability. A loud failure
	// against an unprovisioned topic is the correct outcome, because provisioning is
	// event_admin.go's job and it applies the configured geometry.
	eventWriterAllowAutoTopicCreation = false

	// eventTransportDialTimeout bounds establishing a broker connection, including the
	// SASL handshake.
	eventTransportDialTimeout = 5 * time.Second

	// eventTransportIdleTimeout is how long an unused broker connection is kept in the
	// pool. It matches the 90 seconds the shared HTTP client uses for the legacy
	// webhook transport, so both transports age connections alike.
	eventTransportIdleTimeout = 90 * time.Second

	// eventTransportClientID identifies this producer to the broker. It surfaces in
	// broker logs, request metrics and quota accounting, which is what makes a
	// misbehaving producer attributable to Blnk rather than anonymous.
	eventTransportClientID = "blnk"
)

// Metric attribute keys, declared once so the publisher, its tests and the metrics
// reference cannot drift apart. They match the attribute sets documented on the
// instruments in internal/metrics: published events are attributed by topic and event
// type, attempts by outcome, and duration by topic and attempt number.
const (
	publishAttrTopic     = "topic"
	publishAttrEventType = "event_type"
	publishAttrOutcome   = "outcome"
	publishAttrAttempt   = "attempt"
)

// The two replacement labels below bound metric cardinality. See boundedTopicLabel and
// boundedEventTypeLabel for why an unbounded label is a defect rather than a detail.
const (
	// unownedTopicLabel replaces a topic name that is not in the Blnk-owned namespace.
	unownedTopicLabel = "unowned"

	// unrecognisedEventTypeLabel replaces an event type that has no entry in the
	// event-type catalogue.
	unrecognisedEventTypeLabel = "unrecognised"
)

// boundedTopicLabel renders a topic name as a metric label with BOUNDED cardinality.
//
// # DATA-01: a stored string must not become a metric dimension
//
// Every value that reaches these labels arrives from an OUTBOX ROW, and the failure path
// records an attempt for a topic that was rejected — which is precisely the case where the
// name is not one of Blnk's own. An unbounded label is two problems at once: each distinct
// value creates a new time series, so a stream of odd names is a memory and storage attack
// on the metrics pipeline with no request rate to rate-limit; and the label itself is a
// disclosure channel, publishing whatever string was stored to anyone who can read
// /metrics.
//
// A Blnk-owned name is a member of a small, enumerable set — prefix times four categories,
// times the optional `.dlt` suffix — so it is safe to report verbatim, and reporting it is
// what makes the dead-letter-rate and per-topic queries in docs/metrics.md work. Anything
// else collapses to one fixed label: the series count stays bounded, and the anomaly is
// still visible as a non-zero count on that label. The name itself remains available in the
// log line and on the outbox row, which is where an operator triaging one event looks.
//
// Parameters:
//   - topic string: the topic name to report.
//
// Returns:
//   - string: the topic verbatim when Blnk owns it, unownedTopicLabel otherwise.
func boundedTopicLabel(topic string) string {
	if model.IsBlnkEventTopic(topic, TopicPrefix()) {
		return topic
	}

	return unownedTopicLabel
}

// boundedEventTypeLabel renders an event type as a metric label with BOUNDED cardinality.
//
// The catalogue in model.EventCategory is the bound: an event type it recognises is one of
// a fixed set that this repository emits, so it is reported verbatim. An event type it does
// not recognise is one whose name could be arbitrary — a stored row from a producer that was
// never catalogued, or a value that reached the table before validation existed. They
// collapse to one label for the same two reasons boundedTopicLabel exists.
//
// Collapsing does not hide the condition. A non-zero count on the unrecognised label is the
// signal that something is publishing an uncatalogued event, and the publisher's warning
// names the specific event.
//
// Parameters:
//   - eventType string: the event type to report.
//
// Returns:
//   - string: the event type verbatim when catalogued, unrecognisedEventTypeLabel
//     otherwise.
func boundedEventTypeLabel(eventType string) string {
	if !model.IsCataloguedEventType(eventType) {
		return unrecognisedEventTypeLabel
	}

	return eventType
}

// EventTransportErrorDetail is the detail attached to a typed API error whose cause came
// from the Kafka client rather than from Blnk.
//
// # DATA-01: a broker error object must not become a response body
//
// A typed apierror.APIError carries its cause in an interface{} Details field that is
// serialised into the response. The Kafka client's errors are STRUCTS WITH EXPORTED FIELDS —
// *net.OpError carries Op, Net and Addr, so marshalling one publishes the broker's address
// and port; kafka.Error is an integer protocol code; kafka.WriteErrors is a slice of either.
// Attaching such a value directly would hand a caller the deployment's internal broker
// topology and the client's protocol state, from an endpoint whose job is to report that one
// event did not publish.
//
// This type is what is attached instead. Every field is a BOUNDED value the caller either
// supplied or already holds: the event's own identifiers, the topic (bounded by
// boundedTopicLabel), a reason drawn from a fixed vocabulary declared beside the call, and
// the transient classification the retry decision was made on — which is the one thing a
// caller can act on, because it says whether waiting will help.
//
// The full cause is not discarded, only redirected. It is logged, with the error attached,
// at the site that builds this detail, so an operator retains the broker's exact words while
// the caller receives the diagnosis and nothing else.
type EventTransportErrorDetail struct {
	// Reason states what failed, in fixed wording chosen at the call site. It never
	// interpolates the cause.
	Reason string `json:"reason"`

	// EventID is the event's UUID: the caller's own correlation handle, and what an
	// operator needs to find the row and the log line.
	EventID string `json:"event_id"`

	// EventType is the event name, bounded to the catalogue.
	EventType string `json:"event_type,omitempty"`

	// Topic is the destination, bounded to the Blnk-owned namespace.
	Topic string `json:"topic,omitempty"`

	// Transient reports whether the failure looks recoverable, which is the actionable
	// half of the diagnosis: a transient failure will clear, a permanent one will not.
	Transient bool `json:"transient"`
}

// NewEventTransportErrorDetail builds a bounded transport-failure detail from a cause,
// keeping the cause itself out of the result.
//
// The cause is used for exactly one thing — the transient classification — so the returned
// value cannot contain any of its text or structure however the caller renders it: as JSON,
// with %v, or field by field.
//
// Parameters:
//   - reason string: fixed wording describing what failed. Supply a literal, never a
//     formatted string containing the cause.
//   - eventID, eventType, topic string: the event's identifiers and destination. The last
//     two are bounded here so a caller cannot forget to.
//   - cause error: the underlying failure, read only for its transient classification.
//
// Returns:
//   - EventTransportErrorDetail: the safe detail.
func NewEventTransportErrorDetail(reason, eventID, eventType, topic string, cause error) EventTransportErrorDetail {
	return EventTransportErrorDetail{
		Reason:    reason,
		EventID:   eventID,
		EventType: boundedEventTypeLabel(eventType),
		Topic:     boundedTopicLabel(topic),
		Transient: cause != nil && classifyTransientPublishError(cause),
	}
}

// jsonNull is what a LedgerEvent with no payload bytes serialises its payload member
// to. Emitting the JSON null literal keeps the envelope structurally valid and the
// event published, observable and replayable; the alternative — omitting the member —
// would break the contract that all six envelope keys are always present.
var jsonNull = []byte("null")

// ErrEventPublisherClosed is returned by a publish attempt made after Close. It is a
// permanent error for the attempt in hand: this publisher will never accept another
// write. The event itself is not lost, because its outbox row is only marked
// dispatched after a successful publish and becomes claimable again once the relay's
// lease on it expires.
var ErrEventPublisherClosed = errors.New("blnk: event publisher is closed")

// ErrEventMessageTooLarge is returned when a fully-marshalled event envelope exceeds
// model.MaxEventMessageBytes.
//
// It is a sentinel so the relay, the dead-letter path and a test can all recognise the
// same condition without matching message text, and so that "too large" is provably
// distinct from a transport failure that happens to mention a size.
var ErrEventMessageTooLarge = errors.New("blnk: serialised event exceeds the maximum message size")

// ErrKafkaProducerCredentialsRequired is returned when a deployment configures Kafka
// brokers and administrative SASL credentials but no dedicated producer principal, and
// has not explicitly opted in to publishing as the administrator.
//
// It is a sentinel so that the refusal is provably distinct from the half-configured-pair
// errors that share the same code path, and so a test can assert the refusal without
// matching message text.
var ErrKafkaProducerCredentialsRequired = errors.New(
	"blnk: a dedicated Kafka producer principal is required; set KAFKA_SASL_USER and KAFKA_SASL_SECRET",
)

// EventPublisher publishes a canonical ledger event.
//
// The signature is fixed by the deployment contract and is reproduced here verbatim.
// It takes a context and a model.LedgerEvent and returns an error, and it must not
// grow parameters, return a tuple, or be renamed. Callers that need the richer
// per-attempt record — the relay and the dead-letter writer — use the separate
// PublishToTopic method declared on TopicEventPublisher below, which is how extra
// information is exposed without touching this contract.
//
// Two implementations live in this file and there are never any others in a running
// process: a Kafka-backed publisher, and a no-op selected when no brokers are
// configured.
type EventPublisher interface {
	Publish(ctx context.Context, event model.LedgerEvent) error
}

// TopicEventPublisher is the full publisher contract used inside Blnk: the mandated
// Publish, the explicit result-returning form the relay and the dead-letter writer
// need, and lifecycle.
//
// BOTH implementations in this file satisfy it, which is the point. A caller holding
// one never has to feature-detect, never has to keep a nil branch for "this publisher
// only does the minimal contract", and can shut the publisher down without knowing
// which one it holds. PublishToTopic exists because three things cannot be derived
// from a LedgerEvent alone and must therefore be passed in:
//
//   - The DESTINATION TOPIC. A stored outbox row records its topic at insert time, so
//     a row remains publishable — and a dead-lettered event replayable — to the topic
//     it was always meant for, even if the naming configuration changed since.
//   - The PARTITION KEY. The key is the row's partition key, which is not one of the
//     six envelope fields; it lives on the outbox row. See resolvePartitionKey.
//   - The ATTEMPT NUMBER, which the publish-duration metric is attributed by so that
//     first-attempt latency can be read separately from retried latency.
type TopicEventPublisher interface {
	EventPublisher

	// PublishToTopic publishes one event and reports what happened. The returned
	// error is always identical to the Err field of the returned result, so a caller
	// may branch on either.
	PublishToTopic(ctx context.Context, req PublishRequest) (PublishResult, error)

	// Close flushes and releases every resource the publisher holds. It is
	// idempotent and safe on the no-op.
	Close() error
}

// PublishRequest is the explicit form of a publish: the event, where it goes, how it
// is keyed, and which attempt this is.
//
// Every field except Event is optional and has a documented fallback, so the zero
// value plus an Event is a valid request — that is exactly what the mandated
// Publish method submits.
type PublishRequest struct {
	// Event is the envelope to publish. Its Payload bytes are written through
	// untransformed; see marshalLedgerEvent.
	Event model.LedgerEvent

	// Topic is the fully-resolved destination. When empty it is derived with
	// TopicForEvent from the event type, which is the correct behaviour for an event
	// that never passed through the outbox. The relay always sets it explicitly from
	// the topic recorded on the outbox row, and the dead-letter writer sets it to the
	// `<topic>.dlt` sibling.
	Topic string

	// Key is the Kafka message key, which is the outbox row's PARTITION KEY —
	// model.EventOutbox.PartitionKey, the value the row was stored with and the value
	// ClaimPendingEventOutbox serialises dispatch on. Keying with a stable hash
	// balancer pins every event sharing a key to a single partition, and that
	// partition affinity is what makes per-aggregate ordering hold: Kafka orders
	// within a partition only.
	//
	// It is the ledger ID exactly when the event has one — PrepareEventOutbox stores
	// a known ledger in the partition-key column as well, which is how requirement
	// R-6's ledger partitioning is honoured — and the next-best stable aggregate
	// otherwise. Populate it with PublishRequestFromOutbox rather than by hand, so the
	// key on the wire cannot diverge from the key the database ordered on.
	//
	// When empty, the event's aggregate ID is used instead — see resolvePartitionKey
	// for why that fallback is safe and what it does and does not preserve.
	Key string

	// Attempt is the 1-based attempt number, used only as a metric attribute. Any
	// value below 1 is treated as 1. The publisher never interprets it as a budget:
	// it does not compare it against a maximum and does not change behaviour as it
	// grows, because retry is the relay's responsibility alone.
	Attempt int

	// MaxAttempts is the retry budget the row states, and it is what lets a single
	// attempt report whether it was the LAST one.
	//
	// Without it the publisher can only say "this attempt failed", and every failure is
	// then reported as retrying — including the one that spent the budget, which is the
	// single most important failure to be able to see.
	//
	// Zero means "not stated". A transient failure with no stated budget is reported as
	// retrying, because whether another attempt follows is genuinely unknown here; a
	// permanent failure is reported as failed regardless, because no budget makes
	// corrupt bytes valid.
	MaxAttempts int

	// Purpose says which of the three publish paths this is, and it exists to keep three
	// different populations out of each other's telemetry. The zero value is
	// PublishPurposeOriginal, so an unstated purpose is a first delivery — which is what
	// the mandated envelope-only Publish method submits. See PublishPurpose.
	Purpose PublishPurpose

	// ClaimedAt is when the relay claimed the outbox row, and it exists so the
	// publish-duration histogram measures what its documentation says it measures:
	// claim to broker acknowledgement, not just the time inside WriteMessages. Leave
	// it zero — as the envelope-only path does — and the duration covers the write
	// alone.
	ClaimedAt time.Time
}

// PublishResult is the observability record of ONE publish attempt. It exists because
// an error alone cannot answer the questions operations asks — which topic, which
// event, which attempt, how long, and is this worth retrying — and because the metrics
// layer is keyed on the outcome vocabulary it carries.
//
// The status vocabulary is model.PublishStatus, reused rather than redeclared so the
// code and the metric label set cannot drift. The publisher itself only ever reports
// two of the three values:
//
//   - PublishStatusDispatched when the broker acknowledged the write.
//   - PublishStatusRetrying when the attempt failed. "Retrying" describes what the
//     attempt makes possible, not a decision this file took: whether another attempt
//     actually happens is the relay's call, made against its own budget.
//
// PublishStatusDeadLettered is never produced here, because retry exhaustion is not
// something a single attempt can observe. The dead-letter writer stamps it with the
// DeadLettered method below.
type PublishResult struct {
	// Status is the outcome of this attempt, and the value the publish-attempts
	// counter is attributed by.
	Status model.PublishStatus

	// EventID is the event's UUID, which is also the subscriber idempotency key. It
	// is carried here so a log line about a failed attempt names the event an
	// operator has to go and look at.
	EventID string

	// EventType is the event name, and the value the published-events counter is
	// attributed by alongside the topic.
	EventType string

	// Topic is the resolved destination this attempt targeted, after the fallback in
	// PublishRequest.Topic was applied.
	Topic string

	// PartitionKey is the key the message was written with, after the fallback in
	// resolvePartitionKey was applied. For an outbox-backed publish it is the row's
	// stored partition key, which is the same value ClaimPendingEventOutbox serialised
	// dispatch on. An empty value means the message was written without a key and was
	// therefore balanced across partitions rather than pinned to one — correct for an
	// event that belongs to no ledger and no aggregate, and a red flag for anything
	// else.
	PartitionKey string

	// Attempt is the 1-based attempt number this result describes.
	Attempt int

	// MaxAttempts is the budget the attempt was measured against, carried through from the
	// request so a log line can state "attempt 3 of 5" rather than "attempt 3". Zero means
	// no budget was stated.
	MaxAttempts int

	// Purpose is the resolved publish purpose, never the empty string. It is what keeps an
	// operator-triggered replay and a dead-letter write out of the first-attempt latency
	// population the sub-2-second target is read from.
	Purpose PublishPurpose

	// Retryable states whether ANOTHER attempt is possible for this event: the failure
	// looked transient AND the attempt did not spend the stated budget. It is false for a
	// success and false for a terminal failure, so it answers exactly one question and is
	// not a synonym for Transient — a transient failure on the last permitted attempt is
	// not retryable.
	Retryable bool

	// Duration is how long the attempt took: from PublishRequest.ClaimedAt when it
	// was set, otherwise the time spent in the write itself.
	Duration time.Duration

	// CapturedAt is the instant the event was CAPTURED in the transactional outbox, copied
	// from the envelope's OccurredAt, and it is what makes acceptance criterion V-1
	// measurable.
	//
	// Duration cannot answer V-1. Its clock starts at the claim, so it excludes the row
	// waiting for the next poll tick, the poll interval itself, and the claim query's
	// latency — the three intervals that dominate a backlog. A relay stalled for a minute
	// would report a five-millisecond publish, so the figure would look healthiest exactly
	// when subscribers were furthest behind. Both are recorded: the difference between them
	// is the queue wait, which is what tells an operator whether a slow end-to-end figure is
	// the broker or the relay.
	//
	// It spans two clocks by necessity — the capturing process stamped occurred_at, this
	// process reads the acknowledgement — so a NEGATIVE reading is possible under skew and
	// is DROPPED rather than clamped. See recordPublishAttempt.
	//
	// Zero when the envelope carried no occurred_at, in which case no end-to-end observation
	// is recorded at all.
	CapturedAt time.Time

	// Record is WHERE the broker put this event: the topic, partition and offset it
	// assigned. Confirmed only on a successful attempt, and the zero value on every
	// failure — a write that was not acknowledged produced no record to name.
	//
	// OBS-02. It is captured because the zero-loss reconciliation needs a MAPPING from
	// each published row to the record it produced, not a count of each side: counting
	// cannot distinguish a surplus of redeliveries from a surplus that is masking an
	// equal number of losses. The relay persists this coordinate when it marks the row,
	// so a row claiming a publication either names its record or is reported as
	// unconfirmed.
	//
	// It is also the value an operator wants during triage — given
	// "blnk.transactions/3@148291" the exact record can be read back with a console
	// consumer, which is a different position from knowing only that a publish happened.
	Record model.BrokerRecord

	// Transient reports whether the failure looks recoverable — a broker that is
	// down, a leader election in flight, a timeout — as opposed to permanent, such as
	// a message that exceeds the topic's size limit or a principal that is not
	// authorised. It is meaningful only when Err is non-nil, and it is advice for the
	// relay's retry decision, never a decision taken here.
	Transient bool

	// Err is the failure, or nil on success. It is the same value PublishToTopic
	// returns as its error, and it wraps the underlying broker or library error so
	// errors.Is and errors.As reach it.
	Err error
}

// Dispatched reports whether the attempt succeeded, meaning the broker acknowledged
// the write under the all-replicas acknowledgement setting. It reads the status rather
// than testing Err for nil so that there is one definition of success.
//
// Returns:
//   - bool: true when the event reached the broker durably.
func (r PublishResult) Dispatched() bool {
	return r.Status == model.PublishStatusDispatched
}

// DeadLettered returns a copy of the result with its status set to dead-lettered.
//
// It is how the dead-letter writer records the terminal outcome without redeclaring
// the status vocabulary or rebuilding the result: the publish that wrote the event to
// its `<topic>.dlt` sibling reports dispatched — because from the broker's point of
// view it plainly was — and this converts that into the pipeline-level statement that
// the ORIGINAL event ended its life dead-lettered.
//
// The receiver is a value, so the original result is unchanged and remains available
// for the log line describing the dead-letter write itself.
//
// Returns:
//   - PublishResult: a copy whose Status is model.PublishStatusDeadLettered.
func (r PublishResult) DeadLettered() PublishResult {
	r.Status = model.PublishStatusDeadLettered

	return r
}

// LogFields renders the result as logrus fields.
//
// It exists so that every log line about a publish — here, in the relay, in the
// dead-letter writer — carries the SAME field names, which is what makes the log
// searchable at all. Requirement R-4 requires the attempt count and the error reason
// on every attempt rather than only on the final failure, and this is the shape those
// two are reported in.
//
// The error field is present only when there is an error, so a successful attempt does
// not log an empty reason.
//
// Returns:
//   - logrus.Fields: a fresh map the caller may extend.
func (r PublishResult) LogFields() logrus.Fields {
	// The partition key is HASHED rather than printed. It is a ledger id — a financial
	// identifier naming the account an event belongs to — and a log stream is routinely
	// shipped somewhere with a weaker access boundary than the ledger itself. The hash
	// keeps the one property a log needs, that two lines about the same ledger are
	// recognisable as such, and gives up the one it does not.
	fields := logrus.Fields{
		"event_id":           r.EventID,
		"event_type":         r.EventType,
		"topic":              r.Topic,
		"partition_key_hash": hashLogIdentifier(r.PartitionKey),
		"attempt":            r.Attempt,
		"status":             string(r.Status),
		"duration_ms":        r.Duration.Milliseconds(),
	}

	// The budget accompanies the attempt on EVERY attempt, not only the last: "attempt 3"
	// is unreadable on its own, while "attempt 3 of 5" says how much of the budget is
	// left, which is the difference between an event that is progressing and one that is
	// about to be dead-lettered. Omitted when unstated, so an envelope-only publish does
	// not carry a fabricated zero.
	if r.MaxAttempts > 0 {
		fields["max_attempts"] = r.MaxAttempts
	}

	// The purpose is named only when it is NOT the default. An original publish is the
	// overwhelming majority of lines, and a field whose value is the same on all of them
	// is noise; a replay or a dead-letter write is the line an operator is looking for.
	if r.Purpose != "" && r.Purpose != PublishPurposeOriginal {
		fields["purpose"] = string(r.Purpose)
	}

	if r.Err != nil {
		// BOUNDED, because this string came from a broker or a library rather than from
		// this codebase: its length is not ours to choose, and it is emitted once per
		// attempt per event, so an unbounded value multiplied by the retry budget and the
		// event rate is how a log pipeline gets throttled for being over quota.
		fields["error"] = sanitizeLogValue(r.Err.Error(), maxLoggedErrorLength)
		fields["transient"] = r.Transient
		// transient says what the failure LOOKED like; retryable says whether anything
		// further will actually be tried. They differ on the last permitted attempt, and
		// that is the case an operator most needs to be able to see.
		fields["retryable"] = r.Retryable
	}

	return fields
}

// PublishError is the error a failed publish returns. It carries the same context
// PublishResult does, so the retry decision survives the narrowing to a bare error
// that the mandated Publish signature forces.
//
// Without it, a caller holding only an EventPublisher would have to guess whether a
// failure is worth retrying. With it, IsTransientPublishError answers from the error
// alone, and the underlying broker error remains reachable through errors.Is and
// errors.As because Unwrap exposes it.
type PublishError struct {
	// Topic is the destination the failed attempt targeted.
	Topic string
	// EventID is the UUID of the event that failed to publish.
	EventID string
	// EventType is the name of the event that failed to publish.
	EventType string
	// Attempt is the 1-based attempt number that failed.
	Attempt int
	// Transient reports whether the failure looks recoverable. See
	// PublishResult.Transient.
	Transient bool
	// Err is the underlying broker, library or context error.
	Err error
}

// Error renders the failure with the context needed to act on it: which event, which
// topic, which attempt, whether a retry is worth attempting, and the underlying
// reason.
//
// Returns:
//   - string: the formatted message.
func (e *PublishError) Error() string {
	classification := "permanent"
	if e.Transient {
		classification = "transient"
	}

	return fmt.Sprintf(
		"blnk: publishing event %s (%s) to topic %s failed on attempt %d (%s): %v",
		e.EventID, e.EventType, e.Topic, e.Attempt, classification, e.Err,
	)
}

// Unwrap exposes the underlying error so errors.Is and errors.As see through this
// wrapper — that is what lets a caller test for context.DeadlineExceeded, for
// ErrEventPublisherClosed, or for a specific kafka.Error without knowing that a
// PublishError sits in between.
//
// Returns:
//   - error: the wrapped cause, which may be nil.
func (e *PublishError) Unwrap() error {
	return e.Err
}

// IsTransientPublishError reports whether an error from a publish looks recoverable.
//
// It is the retry-decision input for a caller that has only an error to work with,
// and it is deliberately conservative in one direction: an error that is not a
// PublishError at all — anything that did not come out of this file's publish path —
// is reported as NOT transient. Guessing "retryable" for an unrecognised error would
// invite an unbounded retry of something that can never succeed.
//
// The classification itself is made once, at the moment of failure, by
// classifyTransientPublishError; this function only reads the verdict back.
//
// Parameters:
//   - err error: the error returned by Publish or PublishToTopic. May be nil.
//
// Returns:
//   - bool: true only when err is (or wraps) a PublishError classified as transient.
func IsTransientPublishError(err error) bool {
	var publishErr *PublishError
	if errors.As(err, &publishErr) {
		return publishErr.Transient
	}

	return false
}

// IsBrokerUnavailableError reports whether a failed publish is the BROKER being unable to
// accept the write, as opposed to a defect in the event or in this service.
//
// It exists because those two are different answers to an operator's question and, at the
// API boundary, different HTTP statuses. An unreachable, leaderless or under-replicated
// broker is a retryable upstream condition and must surface as
// apierror.ErrKafkaUnavailable, which resolves to 503; a message that can never be
// published — bytes that are not valid JSON, an event whose destination does not resolve —
// is this service's problem and must surface as a 500. Reporting an outage as a 500 sends
// an operator hunting for a defect in Blnk and tells a client that retrying is pointless
// at the exact moment retrying is the only correct response.
//
// The verdict is taken from the first of these that applies:
//
//  1. ErrEventPublisherClosed. A closed publisher has no transport at all, which is the
//     same operator situation as no broker being configured: nothing can be published from
//     this process now, the event stays safe in its outbox row, and the honest answer is
//     "unavailable, try again" rather than "internal error".
//  2. A PublishError's OWN classification. Transient was decided at the moment of failure
//     by classifyTransientPublishError against the broker's real error, so it is the most
//     informed answer available and it is not second-guessed here.
//  3. The raw error, for a failure that reached the caller unwrapped — a borrowed writer's
//     WriteMessages error, or a double reporting the broker's own error code.
//
// Anything unrecognised is reported as false. That is the conservative direction: an
// unknown failure stays visible as a fault in this service rather than being written off
// as somebody else's outage.
//
// Parameters:
//   - err error: the error returned by Publish, PublishToTopic or a raw writer. May be
//     nil.
//
// Returns:
//   - bool: true when the failure is the broker being unavailable.
func IsBrokerUnavailableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrEventPublisherClosed) {
		return true
	}

	var publishErr *PublishError
	if errors.As(err, &publishErr) {
		return publishErr.Transient
	}

	return brokerUnavailable(err)
}

// brokerUnavailable classifies a RAW publish failure — one that has not been through
// kafkaPublisher.fail and so carries no verdict of its own.
//
// It starts from the transient classification, because every recoverable failure that
// function recognises (a Temporary or Timeout error, a context expiry, a refused or reset
// connection) is by definition the broker not being reachable or not being ready. Three
// additions cover what it deliberately does not:
//
//   - kafka.WriteErrors, so a batch is judged by its members exactly as the transient
//     classification judges it.
//   - BrokerNotAvailable and ReplicaNotAvailable. kafka-go does NOT list these in
//     Error.Temporary, yet both are the broker stating plainly that it cannot serve the
//     partition right now. They are the two codes a caller most expects to see during a
//     rolling restart, so leaving them out would report the commonest planned outage as an
//     internal error.
//   - *net.OpError and *net.DNSError, which cover a dial, route or resolution failure
//     whose underlying errno is outside the small set classifyTransientPublishError names.
//
// The net checks are deliberately made against those CONCRETE types and never against the
// net.Error interface: kafka.Error implements Error, Timeout and Temporary, so it satisfies
// net.Error, and an interface test would silently reclassify every Kafka protocol error —
// including genuine defects such as an invalid topic or an oversized message — as a
// broker outage.
//
// Parameters:
//   - err error: the raw failure. May be nil.
//
// Returns:
//   - bool: true when the failure is the broker being unavailable.
func brokerUnavailable(err error) bool {
	if err == nil {
		return false
	}

	var writeErrors kafka.WriteErrors
	if errors.As(err, &writeErrors) {
		for _, writeErr := range writeErrors {
			if brokerUnavailable(writeErr) {
				return true
			}
		}

		return false
	}

	if classifyTransientPublishError(err) {
		return true
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr == kafka.BrokerNotAvailable || kafkaErr == kafka.ReplicaNotAvailable
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	var dnsErr *net.DNSError

	return errors.As(err, &dnsErr)
}

// NoopEventPublisher is the publisher selected when no Kafka brokers are configured.
// It accepts every event, does nothing with it, and reports success.
//
// This is a CORRECTNESS requirement, not a convenience. An empty broker list is a
// legitimate steady state rather than an error, and it reproduces exactly the contract
// the legacy transport already had: the webhook sender returned nil without doing
// anything when no webhook URL was configured, so a Blnk deployment has always been
// able to run with no notification sink at all. Every existing test and every existing
// deployment therefore keeps working with no Kafka anywhere in sight — construction
// dials nothing, resolves nothing, blocks on nothing and, above all, returns no error.
//
// It satisfies the whole TopicEventPublisher contract, not just the mandated method,
// so a relay or dead-letter writer holding one needs no special case.
type NoopEventPublisher struct{}

// NewNoopEventPublisher returns the no-op publisher.
//
// It is exported so that tests and callers which must be explicit about wanting no
// publishing — rather than relying on configuration being absent — can say so. The
// type carries no state, so the returned value is safe to share across goroutines and
// cheap to create.
//
// Returns:
//   - *NoopEventPublisher: a ready publisher that never performs I/O.
func NewNoopEventPublisher() *NoopEventPublisher {
	return &NoopEventPublisher{}
}

// Publish accepts the event and discards it, reporting success.
//
// Parameters:
//   - _ context.Context: unused; nothing is dispatched, so nothing can be cancelled.
//   - _ model.LedgerEvent: unused.
//
// Returns:
//   - error: always nil.
func (p *NoopEventPublisher) Publish(_ context.Context, _ model.LedgerEvent) error {
	return nil
}

// PublishToTopic accepts the event, discards it, and reports a dispatched result.
//
// Reporting dispatched rather than a failure is the same no-op contract Publish
// honours: with no broker configured there is nothing to fail against, and a caller
// that treated the absence of Kafka as an error would break every deployment that runs
// without it.
//
// THE RELAY NEVER CALLS THIS, and that is enforced rather than assumed. A relay holding the
// no-op runs in legacy-only mode when a webhook URL is configured — where it drains the
// outbox over the legacy leg and never touches a publisher — and refuses to start at all when
// one is not. Either way this method's dispatched result can never become a row marked
// dispatched, which is precisely the silent loss that refusal exists to prevent. It is
// implemented completely regardless, so the contract holds for any other caller.
//
// The result echoes the request's own topic, key and attempt — resolved through the
// same fallbacks the Kafka implementation applies — so that a caller logging the result
// sees the same fields either way.
//
// Parameters:
//   - _ context.Context: unused.
//   - req PublishRequest: the request whose event, topic and key are echoed back.
//
// Returns:
//   - PublishResult: a dispatched result with a zero duration and no error.
//   - error: always nil.
func (p *NoopEventPublisher) PublishToTopic(_ context.Context, req PublishRequest) (PublishResult, error) {
	return PublishResult{
		Status:       model.PublishStatusDispatched,
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
		MaxAttempts:  resolveMaxAttempts(req),
		Purpose:      resolvePurpose(req),
	}, nil
}

// Close releases nothing, because nothing was ever acquired.
//
// It is nil-safe as well as no-op: calling it on a nil receiver is fine, which is what
// lets the Blnk shutdown path stay as simple as its existing nil-guarded close of the
// asynq client.
//
// Returns:
//   - error: always nil.
func (p *NoopEventPublisher) Close() error {
	return nil
}

// IsNoopEventPublisher reports whether a publisher is the no-op implementation.
//
// It exists so that startup validation and tests can assert WHICH implementation
// configuration selected — the property under test is "an empty broker list selects the
// no-op", and that is not observable through the EventPublisher interface itself. A nil
// publisher counts as a no-op: it publishes nothing, which is the same observable
// behaviour.
//
// Parameters:
//   - p EventPublisher: the publisher to classify. May be nil.
//
// Returns:
//   - bool: true when p is nil or is the no-op publisher.
func IsNoopEventPublisher(p EventPublisher) bool {
	if p == nil {
		return true
	}

	_, isNoop := p.(*NoopEventPublisher)

	return isNoop
}

// kafkaPublisher is the Kafka-backed publisher. It holds ONE writer per topic and one
// shared transport, all constructed once and reused for the process lifetime —
// mirroring how a single HTTP client is constructed once and shared for the legacy
// webhook transport rather than rebuilt per request.
//
// Building a writer per publish would be a serious regression, not merely wasteful: a
// fresh writer has an empty partition-metadata cache and, with its own transport, an
// empty connection pool, so every event would pay a metadata round trip and a TCP plus
// SASL handshake before it could be produced. Sharing one writer per topic is also what
// kafka-go's documentation recommends, and *kafka.Writer is explicitly safe for
// concurrent use, which the relay depends on when it publishes a claimed batch
// concurrently.
type kafkaPublisher struct {
	// brokers is the normalised bootstrap list, retained for logging and for building
	// writers lazily.
	brokers []string

	// addr is the pre-built broker address shared by every writer. kafka.TCP performs
	// no name resolution, so building it is pure string work.
	addr net.Addr

	// transport is shared by every writer so that all of them draw on ONE connection
	// pool and ONE authentication and TLS configuration; kafka.Transport establishes
	// connections lazily per broker and reuses them, authenticating each as it is
	// opened. Per-writer transports would multiply connections and SASL handshakes by
	// the number of topics for no benefit.
	transport *kafka.Transport

	// mu guards writers and closed. *kafka.Writer is itself safe for concurrent use;
	// what needs guarding is the map that hands them out and the shutdown flag.
	mu sync.RWMutex

	// writers is keyed by topic. It is pre-populated with every topic Blnk owns and
	// grows lazily — see writerFor for the two real cases that require growth.
	writers map[string]*kafka.Writer

	// lazyTopics is the topics added AFTER construction, in creation order.
	//
	// It exists to make the writer cache boundable. The map alone cannot answer either
	// question retirement needs — which entries were added lazily, and which of those is
	// oldest — because a map has no order and no record of how an entry arrived. The
	// pre-created inventory is deliberately absent from this list: those writers are the
	// deployment's current topics and must never be retired.
	lazyTopics []string

	// closed makes Close idempotent and turns a publish after shutdown into a clear
	// error rather than a panic on a closed writer.
	closed bool
}

// Compile-time proof that both implementations satisfy both contracts. These
// assertions are the cheapest possible guard against the interface and its
// implementations drifting apart, and they fail the build rather than a test.
var (
	_ EventPublisher      = (*kafkaPublisher)(nil)
	_ EventPublisher      = (*NoopEventPublisher)(nil)
	_ TopicEventPublisher = (*kafkaPublisher)(nil)
	_ TopicEventPublisher = (*NoopEventPublisher)(nil)

	// The MANDATED SIGNATURE itself, pinned at compile time. "We documented it" is not
	// a guarantee: assigning each implementation's method to a variable of the exact
	// function type means any drift — an added parameter, a tuple return, a renamed
	// method — stops the build here, on the lines that state the contract, rather than
	// surfacing as a puzzling failure somewhere downstream. The assignment is the whole
	// point, so the values are discarded.
	_ func(context.Context, model.LedgerEvent) error = (&NoopEventPublisher{}).Publish
	_ func(context.Context, model.LedgerEvent) error = (&kafkaPublisher{}).Publish
)

// NewEventPublisher builds the event publisher from configuration.
//
// It is the single decision point for which implementation a process runs with, and the
// rule is exactly one line long: NO BROKERS MEANS THE NO-OP, and that is not an error.
// A nil configuration, an unset KAFKA_BROKERS, and a broker list that contains nothing
// but whitespace all resolve the same way, and all three are reachable — the first from
// a process whose configuration has not been loaded, the second from every deployment
// that does not run Kafka, and the third from a stray separator in an environment file.
//
// CONSTRUCTION PERFORMS NO I/O AT ALL. No connection is dialled, no name is resolved,
// no metadata is fetched, and nothing blocks. kafka.TCP only canonicalises address
// strings, a SCRAM mechanism is pure computation, and a kafka.Writer connects lazily on
// its first write. That property is load-bearing: existing tests construct a Blnk
// instance with nothing but a Redis DSN configured, and this must not turn that into a
// network call, a delay, or an error.
//
// The returned publisher owns writers for every topic Blnk owns — the four category topics
// and their five dead-letter siblings, ten in all — enumerated from AllTopicsWithDeadLetters
// so that this file and the provisioning path work from one list.
//
// The errors it can return both come from the administrative SASL credential: the pair
// is half-configured (exactly one of KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET
// set), or the credentials cannot be prepared because SASLprep rejects the username or
// password. Both ARE fatal misconfigurations and must not be silently downgraded to the
// no-op: silently publishing nothing because a password was malformed, or publishing
// anonymously because a username was missing, are precisely the failure modes the loud
// error prevents.
//
// Parameters:
//   - cnf *config.Configuration: the loaded configuration. May be nil.
//
// Returns:
//   - EventPublisher: the Kafka publisher when brokers are configured, otherwise the
//     no-op. Never nil when the error is nil.
//   - error: non-nil only when configured SASL credentials cannot be prepared.
func NewEventPublisher(cnf *config.Configuration) (EventPublisher, error) {
	if cnf == nil {
		logrus.Debug(
			"no configuration available for the event publisher; " +
				"selecting the no-op publisher so no ledger event is published",
		)

		return NewNoopEventPublisher(), nil
	}

	brokers := normalizeBrokers(cnf.Kafka.Brokers)
	if len(brokers) == 0 {
		logrus.Debug(
			"KAFKA_BROKERS is not configured; selecting the no-op event publisher. " +
				"Ledger events are not published to Kafka and no error is raised",
		)

		return NewNoopEventPublisher(), nil
	}

	publisher, err := newKafkaPublisher(brokers, cnf.Kafka)
	if err != nil {
		return nil, err
	}

	// The principal and the encryption state are the two facts an operator needs to
	// confirm the producer connected as intended, so both are named explicitly rather
	// than inferred from which variables happen to be set.
	producerUser, _, credErr := kafkaTransportCredentials(cnf.Kafka, KafkaTransportRoleProducer)
	if credErr != nil {
		// Unreachable: newKafkaPublisher resolved the same pair a moment ago and would
		// have returned the error. Logged rather than ignored so a future divergence
		// between the two calls is visible instead of silent.
		logrus.WithError(credErr).Warn(
			"could not re-resolve the producer SASL principal for the initialisation log",
		)
	}

	logrus.WithFields(logrus.Fields{
		"brokers":         brokers,
		"topics":          len(publisher.writers),
		"sasl":            producerUser != "",
		"sasl_principal":  producerUser,
		"dedicated_sasl":  cnf.Kafka.SASLUser != "",
		"tls":             cnf.Kafka.TLS.Enabled,
		"required_acks":   "all",
		"balancer":        "murmur2",
		"topic_prefix":    TopicPrefix(),
		"internal_retry":  false,
		"max_event_bytes": model.MaxEventMessageBytes,
	}).Info("kafka event publisher initialised")

	return publisher, nil
}

// newKafkaPublisher assembles the transport and the per-topic writers.
//
// It is separated from NewEventPublisher so that the assembly is reachable from a test
// without going through configuration selection, and so that the selection logic above
// reads as the single decision it is.
//
// The transport it builds is the SECURITY BOUNDARY of the producer path — TLS, credential
// selection and the plaintext refusal all live in NewKafkaTransport, which the admin path
// shares so the two cannot diverge.
//
// Parameters:
//   - brokers []string: a non-empty, normalised bootstrap list.
//   - cfg config.KafkaConfig: the Kafka configuration block, read for credentials and TLS.
//
// Returns:
//   - *kafkaPublisher: the assembled publisher, with one writer per owned topic.
//   - error: non-nil when the transport cannot be built — malformed credentials, an
//     unreadable or invalid TLS material, or plaintext without the explicit local-dev
//     acknowledgement.
func newKafkaPublisher(brokers []string, cfg config.KafkaConfig) (*kafkaPublisher, error) {
	if err := cfg.ValidateSASLAdminCredentials(); err != nil {
		return nil, fmt.Errorf("blnk: cannot build the Kafka event publisher: %w", err)
	}

	addr := kafka.TCP(brokers...)

	transport, err := NewKafkaTransport(cfg, KafkaTransportRoleProducer)
	if err != nil {
		return nil, err
	}

	publisher := &kafkaPublisher{
		brokers:   brokers,
		addr:      addr,
		transport: transport,
		writers:   make(map[string]*kafka.Writer),
	}

	// Pre-create a writer for every topic Blnk owns, taken from the one inventory the
	// admin path and the provisioning script also work from. Pre-creating costs
	// nothing — a writer performs no I/O until its first write — and it means the
	// steady-state publish path never takes the write lock.
	for _, topic := range AllTopicsWithDeadLetters() {
		publisher.writers[topic] = publisher.newWriter(topic)
	}

	return publisher, nil
}

// KafkaTransportRole names which principal a transport authenticates as.
//
// It exists because the producer and the administrator are DIFFERENT PRINCIPALS with
// deliberately different privileges, and a single "the Kafka credentials" notion is what
// erased that distinction. The role is an explicit argument so a caller cannot pick up the
// wrong one by omission.
type KafkaTransportRole string

const (
	// KafkaTransportRoleProducer is the steady-state event-publishing principal. It needs
	// Write and Describe on the Blnk-owned topics and NOTHING ELSE — no topic creation, no
	// credential alteration, no ACL management.
	KafkaTransportRoleProducer KafkaTransportRole = "producer"

	// KafkaTransportRoleAdmin is the provisioning principal. It creates topics, alters
	// SCRAM credentials and manages ACLs, which is the most privileged identity in the
	// system and is why it must not be the one used to publish every ledger event.
	KafkaTransportRoleAdmin KafkaTransportRole = "admin"
)

// NewKafkaTransport builds the shared kafka.Transport for one role, applying the TLS and
// credential policy.
//
// # TLS, and why plaintext must be asked for explicitly
//
// SASL/SCRAM authenticates the CLIENT to the BROKER; it does not encrypt the connection and
// it does not authenticate the broker to the client. Over SASL_PLAINTEXT the SCRAM exchange
// and every subsequent produce request travel in the clear, so anything on the path can read
// the ledger events — which carry amounts, balance identifiers and, on identity events,
// names, email addresses, phone numbers, postal addresses and dates of birth — and can
// impersonate the broker to harvest the SCRAM handshake.
//
// So TLS is the default posture and plaintext is an OPT-IN that has to be stated:
// KAFKA_TLS_ENABLED turns on a verified TLS transport, and KAFKA_INSECURE_LOCAL_DEV is the
// only thing that permits running without it. Neither being set is a CONFIGURATION ERROR
// rather than a silent downgrade, because a silent downgrade is indistinguishable from a
// working deployment right up to the moment someone reads a packet capture. The local
// single-broker stack runs SASL_PLAINTEXT, which is what the escape hatch is for, and it
// names itself so it cannot be mistaken for a production setting.
//
// InsecureSkipVerify is honoured but refused outside local-dev for the same reason: TLS
// without verification stops a passive reader and does nothing about an active one, so a
// production deployment that set it would believe it had a protection it does not have.
//
// # Credentials
//
// The role decides which principal authenticates. Both pairs go through
// Configuration.ValidateSASLPair, so a half-configured pair — a username with no secret, or
// a secret with no username — is refused at construction rather than producing an
// authentication failure on the first publish, hours later, in a log nobody is watching.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block, read for TLS material and both credential
//     pairs.
//   - role KafkaTransportRole: which principal to authenticate as.
//
// Returns:
//   - *kafka.Transport: the assembled transport. No I/O is performed: TLS material is read
//     from disk, which is local, and a SCRAM mechanism is pure computation.
//   - error: a malformed or half-configured credential pair, unreadable or invalid TLS
//     material, or plaintext without the explicit local-dev acknowledgement.
func NewKafkaTransport(cfg config.KafkaConfig, role KafkaTransportRole) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		DialTimeout: eventTransportDialTimeout,
		IdleTimeout: eventTransportIdleTimeout,
		ClientID:    eventTransportClientID,
	}

	// Both halves are resolved before either is judged, and their errors are JOINED.
	//
	// A deployment that has neither enabled TLS nor finished configuring its credentials has
	// two problems, and reporting one of them sends the operator round the loop twice: they
	// fix the encryption, restart, and only then learn about the credential. Joining costs
	// nothing and turns two boot failures into one.
	tlsConfig, tlsErr := kafkaTLSConfig(cfg)
	user, secret, credErr := kafkaTransportCredentials(cfg, role)
	if err := errors.Join(tlsErr, credErr); err != nil {
		return nil, err
	}

	transport.TLS = tlsConfig

	if user != "" {
		mechanism, mechErr := scram.Mechanism(scram.SHA512, user, secret)
		if mechErr != nil {
			return nil, saslCredentialError(role, user)
		}

		transport.SASL = mechanism
	} else if tlsConfig == nil {
		// Neither authenticated nor encrypted. That is only ever acceptable on a local
		// broker, and only when the operator has said so.
		logrus.WithField("role", string(role)).Warn(
			"the Kafka transport is neither authenticated nor encrypted; " +
				"this is only supported under KAFKA_INSECURE_LOCAL_DEV",
		)
	}

	return transport, nil
}

// kafkaTLSConfig builds the verified TLS configuration, or returns nil when plaintext has
// been explicitly permitted.
//
// A nil *tls.Config makes kafka-go dial in the clear, so returning nil is the plaintext
// decision and it is reachable from exactly one place: TLS disabled AND local-dev
// acknowledged. Every other combination either builds a verified configuration or fails.
//
// Returns:
//   - *tls.Config: the verified configuration, or nil for explicitly-permitted plaintext.
//   - error: TLS disabled without the local-dev acknowledgement, InsecureSkipVerify
//     outside local dev, or unreadable/invalid CA or client key material.
func kafkaTLSConfig(cfg config.KafkaConfig) (*tls.Config, error) {
	if !cfg.TLS.Enabled {
		if !cfg.InsecureLocalDev {
			return nil, errors.New(
				"blnk: KAFKA_TLS_ENABLED is false, so ledger events and the SASL/SCRAM handshake " +
					"would travel in the clear. Enable TLS, or set KAFKA_INSECURE_LOCAL_DEV=true to " +
					"acknowledge that this is a local development broker",
			)
		}

		logrus.Warn(
			"KAFKA_TLS_ENABLED is false and KAFKA_INSECURE_LOCAL_DEV is set: " +
				"connecting to Kafka WITHOUT TLS. Ledger event payloads and SASL credentials are " +
				"not encrypted in transit. This configuration must never be used outside local development",
		)

		return nil, nil
	}

	if cfg.TLS.InsecureSkipVerify && !cfg.InsecureLocalDev {
		return nil, errors.New(
			"blnk: KAFKA_TLS_INSECURE_SKIP_VERIFY disables broker certificate verification, which " +
				"leaves the connection open to an active attacker while appearing encrypted. It is " +
				"permitted only alongside KAFKA_INSECURE_LOCAL_DEV=true",
		)
	}

	tlsConfig := &tls.Config{
		// TLS 1.2 is the floor. Anything earlier has known weaknesses and no reason to be
		// offered to a broker that this deployment controls.
		MinVersion:         tls.VersionTLS12,
		ServerName:         strings.TrimSpace(cfg.TLS.ServerName),
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify, //nolint:gosec // refused above unless KAFKA_INSECURE_LOCAL_DEV is set
	}

	if caFile := strings.TrimSpace(cfg.TLS.CAFile); caFile != "" {
		pem, readErr := os.ReadFile(caFile)
		if readErr != nil {
			return nil, fmt.Errorf("blnk: reading KAFKA_TLS_CA_FILE %q: %w", caFile, readErr)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf(
				"blnk: KAFKA_TLS_CA_FILE %q contains no usable PEM certificate; "+
					"an empty trust pool would fall back to the system roots and silently trust "+
					"a broker this deployment never intended to", caFile)
		}

		tlsConfig.RootCAs = pool
	}

	certFile := strings.TrimSpace(cfg.TLS.CertFile)
	keyFile := strings.TrimSpace(cfg.TLS.KeyFile)

	switch {
	case certFile != "" && keyFile != "":
		certificate, certErr := tls.LoadX509KeyPair(certFile, keyFile)
		if certErr != nil {
			// The paths are named; the key material is not rendered.
			return nil, fmt.Errorf(
				"blnk: loading the Kafka client certificate from KAFKA_TLS_CERT_FILE %q and "+
					"KAFKA_TLS_KEY_FILE %q: %w", certFile, keyFile, certErr)
		}

		tlsConfig.Certificates = []tls.Certificate{certificate}

	case certFile != "" || keyFile != "":
		// Half a client certificate is not a weaker mutual-TLS setup; it is no mutual TLS
		// at all, silently, while the operator believes otherwise.
		return nil, errors.New(
			"blnk: KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must be set together; " +
				"one without the other yields no client certificate and mutual TLS would not be in effect",
		)
	}

	return tlsConfig, nil
}

// kafkaTransportCredentials selects and validates the credential pair for a role.
//
// # PRIV-01: the producer must not be the administrator
//
// The steady-state publisher used to authenticate as KAFKA_SASL_ADMIN_USER — the principal
// that creates topics, alters SCRAM credentials and manages ACLs. Every ledger event was
// therefore produced by the most privileged identity in the system, so a leaked producer
// credential handed an attacker the ability to rewrite the access model rather than merely
// to publish events, and nothing in the broker's audit trail could distinguish routine
// publishing from administration.
//
// The producer role therefore requires the DEDICATED pair, KAFKA_SASL_USER /
// KAFKA_SASL_SECRET.
//
// # Why the admin fallback is REFUSED rather than warned about
//
// This used to fall back to the admin pair with a warning, on the reasoning that an
// existing deployment mid-upgrade must keep publishing rather than stop dead. That
// reasoning is sound but the default was backwards: a deployment reaches the fallback by
// leaving two variables UNSET, which is the state every deployment starts in, so the
// warning was the only thing standing between an ordinary rollout and running the data
// plane as the cluster administrator — and a warning changes nothing about what a leaked
// credential can then do.
//
// So the fallback is now gated on KAFKA_ALLOW_ADMIN_PRODUCER, default false. A deployment
// with brokers and admin credentials but no producer principal FAILS at publisher
// construction, naming the two variables to set, which is a defect an operator fixes in
// minutes. The escape hatch remains for the upgrade case that justified it, but it must be
// asked for, and asking for it is recorded in the configuration where a reviewer can see
// it rather than inferred from an absence.
//
// # What is deliberately still permitted
//
// No SASL at all. When neither pair is configured this returns two empty strings and no
// error: the local single-broker stack can run unauthenticated, and refusing that here
// would break it. The refusal below is specifically about REACHING FOR THE ADMIN
// CREDENTIAL, not about the absence of a producer one.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block.
//   - role KafkaTransportRole: which principal to resolve.
//
// Returns:
//   - user, secret string: the resolved pair. Both empty means no SASL is configured,
//     which is legitimate for an unauthenticated local broker.
//   - error: a half-configured pair, for either role; or
//     ErrKafkaProducerCredentialsRequired when the producer role would have to borrow the
//     administrative credential and that has not been explicitly allowed.
func kafkaTransportCredentials(cfg config.KafkaConfig, role KafkaTransportRole) (string, string, error) {
	if role == KafkaTransportRoleAdmin {
		if err := cfg.ValidateSASLAdminCredentials(); err != nil {
			return "", "", err
		}

		// Resolved through the ONE named reading rather than by trimming the fields here.
		// Both values are trimmed by it, so a secret carrying a trailing newline from a
		// secret store cannot reach the SASL mechanism, and "exactly one set" cannot
		// produce a half-authenticated transport: it is refused above.
		user, secret, _ := cfg.SASLAdminCredentials()

		return user, secret, nil
	}

	if err := config.ValidateSASLPair("producer", cfg.SASLUser, cfg.SASLSecret); err != nil {
		return "", "", err
	}

	if producer := strings.TrimSpace(cfg.SASLUser); producer != "" {
		return producer, cfg.SASLSecret, nil
	}

	// No dedicated producer principal. The administrative pair is validated first so
	// that a half-configured one is reported as the configuration defect it is, rather
	// than being silently read as "no admin credentials" and turned into the different
	// diagnosis below.
	if err := cfg.ValidateSASLAdminCredentials(); err != nil {
		return "", "", err
	}

	admin, adminSecret, enabled := cfg.SASLAdminCredentials()
	if !enabled {
		// Neither role is configured. An unauthenticated broker, which the local stack
		// is entitled to be.
		return "", "", nil
	}

	if !cfg.AllowAdminProducer {
		// THE PRINCIPAL IS NAMED AND THE SECRET IS NOT, and the asymmetry is deliberate. A
		// caller logs this error, so the secret must never be in it — but the principal is an
		// identifier, and naming it turns the message from "configure a producer" into
		// "configure a producer INSTEAD OF blnk-kafka-admin", which is what tells an operator
		// with several credentials in play which one the refusal is about.
		return "", "", fmt.Errorf(
			"%w. Kafka brokers and administrative credentials are configured but no producer "+
				"principal is, and publishing as the administrator %q is refused: that principal "+
				"can create topics, alter SCRAM credentials and grant or revoke ACLs, so a leaked "+
				"producer credential would compromise the cluster's authorization state rather "+
				"than merely permit publishing, and the broker's audit trail could no longer tell "+
				"routine publishing from administration. Provision a principal with Write and "+
				"Describe on the Blnk-owned topics — scripts/kafka-provision.sh does this for the "+
				"local stack — and set KAFKA_SASL_USER and KAFKA_SASL_SECRET. To keep publishing "+
				"as the administrator while that is arranged, set KAFKA_ALLOW_ADMIN_PRODUCER=true "+
				"deliberately",
			ErrKafkaProducerCredentialsRequired, admin,
		)
	}

	logrus.WithFields(logrus.Fields{
		"principal": admin,
		"setting":   "KAFKA_ALLOW_ADMIN_PRODUCER",
	}).Warn(
		"SECURITY: the event publisher is authenticating with the KAFKA ADMIN credentials " +
			"because KAFKA_ALLOW_ADMIN_PRODUCER is set and no dedicated producer principal is " +
			"configured. The admin principal can create topics, alter SCRAM credentials and " +
			"manage ACLs, so every ledger event is being published at the privilege level that " +
			"controls the cluster's authorization state. This is an upgrade-window compatibility " +
			"setting, not a configuration to run on: provision a producer principal with Write " +
			"and Describe on the Blnk-owned topics, set KAFKA_SASL_USER and KAFKA_SASL_SECRET, " +
			"and clear KAFKA_ALLOW_ADMIN_PRODUCER",
	)

	return admin, adminSecret, nil
}

// saslProbeValue is a fixed, non-secret placeholder used ONLY to establish which of the two
// configured SASL values is malformed. It never leaves the process: it is not sent to a
// broker, not stored, and not logged. It is deliberately a plain ASCII literal, so
// preparing it can only ever succeed and the probe's verdict is unambiguous.
const saslProbeValue = "sasl-probe"

// saslCredentialError reports that the configured SASL credentials cannot be prepared, and
// does so WITHOUT wrapping the underlying library error.
//
// Not wrapping is a SECURITY decision, not an oversight. The SCRAM client constructs its
// failure as `Error SASLprepping password '<password>': ...` — it embeds the PLAINTEXT
// PASSWORD in the message text. Wrapping that error would carry KAFKA_SASL_ADMIN_SECRET into
// every log line, error response and trace that ever renders it, which is precisely the leak
// the credential-handling posture exists to prevent. The library error is therefore
// discarded on purpose, and the diagnosis is rebuilt from values that are safe to print.
//
// The username IS safe to print: it is an identifier rather than a credential, and it is
// what an operator needs in order to match the failure to their configuration. Which of the
// two values is at fault is established by re-preparing the username alone against
// saslProbeValue, so the answer is precise without the secret being rendered anywhere.
//
// The ROLE decides which variable names appear in the message, because telling an operator
// to check KAFKA_SASL_ADMIN_USER when the producer pair is at fault sends them to the wrong
// line of their configuration.
//
// Parameters:
//   - role KafkaTransportRole: which principal failed to prepare.
//   - username string: the configured username for that role.
//
// Returns:
//   - error: a message naming the offending variable, never containing the secret.
func saslCredentialError(role KafkaTransportRole, username string) error {
	userVar, secretVar := "KAFKA_SASL_USER", "KAFKA_SASL_SECRET"
	if role == KafkaTransportRoleAdmin {
		userVar, secretVar = "KAFKA_SASL_ADMIN_USER", "KAFKA_SASL_ADMIN_SECRET"
	}

	if _, err := scram.Mechanism(scram.SHA512, username, saslProbeValue); err != nil {
		return fmt.Errorf(
			"blnk: %s %q cannot be prepared as a SASL/SCRAM-SHA-512 credential; "+
				"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point",
			userVar, username,
		)
	}

	return fmt.Errorf(
		"blnk: %s cannot be prepared as a SASL/SCRAM-SHA-512 credential for user %q; "+
			"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point. "+
			"The secret is deliberately omitted from this message",
		secretVar, username,
	)
}

// newWriter builds one writer for one topic, with every load-bearing setting stated
// explicitly. See the tuning constants for why each value is what it is and why none of
// them is left to a library default.
//
// Parameters:
//   - topic string: the fully-resolved topic this writer publishes to.
//
// Returns:
//   - *kafka.Writer: a ready writer that has performed no I/O.
//
// publishAcknowledgement receives the broker coordinate of one message.
//
// # Why a per-message carrier rather than a per-writer field
//
// kafka-go reports offsets through Writer.Completion, which is a property of the WRITER —
// and writers here are pooled per topic and shared by every concurrent publish to it. A
// callback writing into publisher state could not tell which publish a message belonged
// to, and one batch legitimately carries messages from several of them.
//
// kafka.Message.WriterData is the library's own correlation seam: it is carried through the
// write and handed back on the completion, and it never touches the wire. So each publish
// attaches its own carrier, the callback fills in the one it is given, and no coordination
// between concurrent publishes is needed at all.
//
// The mutex is not ceremony. Completion runs on the writer's own goroutines, so the write
// and the read genuinely cross goroutine boundaries — with Async false, WriteMessages blocks
// on Completion, which orders them but does not by itself make the access race-free under
// the memory model.
type publishAcknowledgement struct {
	mu     sync.Mutex
	record model.BrokerRecord
	filled bool
}

// accept records the coordinate the broker assigned.
//
// The first completion wins. kafka-go calls Completion once per batch and a message belongs
// to exactly one batch, so a second call for the same carrier would mean an internal retry
// re-reporting the message — in which case the first coordinate is the one the offset series
// was assigned from.
//
// Parameters:
//   - message kafka.Message: the completed message, carrying the broker's topic, partition
//     and offset.
func (a *publishAcknowledgement) accept(message kafka.Message) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.filled {
		return
	}

	// A message the broker did not answer for carries no usable coordinate. kafka-go leaves
	// the fields at their zero values in that case and reports the error separately, and
	// partition 0 / offset 0 is a REAL location — so accepting it would manufacture a
	// coordinate for a write that never landed, which is worse than recording nothing.
	if strings.TrimSpace(message.Topic) == "" || message.Offset < 0 {
		return
	}

	a.record = model.BrokerRecord{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
	a.filled = true
}

// coordinate returns the recorded location, and whether one was received.
//
// Returns:
//   - model.BrokerRecord: the coordinate, zero-valued when none arrived.
//   - bool: whether the broker reported one.
func (a *publishAcknowledgement) coordinate() (model.BrokerRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.record, a.filled
}

// completeWrite is the writers' shared Completion callback.
//
// It fans each completed message out to its own carrier and does nothing else. Keeping it
// this small is deliberate: kafka-go documents that a panic in a completion function
// terminates the program, because the panic bubbles up a writer goroutine that nothing
// recovers — so this function must be incapable of panicking. It therefore performs a
// checked type assertion, tolerates a nil carrier, and never dereferences anything the
// caller did not put there.
//
// The batch error is IGNORED on purpose. A failed write is reported to the caller by
// WriteMessages, which is where the failure is classified, logged and counted; the only job
// here is the coordinate, and a failed batch simply has none to report.
//
// Parameters:
//   - messages []kafka.Message: the batch, with topic, partition and offset set from the
//     produce response.
//   - _ error: the batch outcome, handled by the caller instead.
func (p *kafkaPublisher) completeWrite(messages []kafka.Message, _ error) {
	for _, message := range messages {
		acknowledgement, ok := message.WriterData.(*publishAcknowledgement)
		if !ok || acknowledgement == nil {
			continue
		}

		acknowledgement.accept(message)
	}
}

func (p *kafkaPublisher) newWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:  p.addr,
		Topic: topic,

		// The broker coordinate seam. With Async false, WriteMessages blocks on this
		// callback, so a coordinate is available by the time the write returns — which is
		// what lets the result carry it synchronously rather than the relay having to
		// correlate it afterwards.
		Completion: p.completeWrite,

		// A STABLE HASH BALANCER. Murmur2 is chosen over kafka-go's other stable
		// hashes because it reproduces the Java client's default partitioner exactly,
		// so a message Blnk produces for a given key lands on the same partition a
		// Java or librdkafka producer would choose for it. That matters the moment
		// anything other than Blnk writes to these topics, or a subscriber reasons
		// about partition assignment from the key. Consistent is left false, which is
		// also the Java behaviour: a message with no key is spread across partitions
		// instead of being pinned to one, so keyless events cannot create a hot
		// partition.
		Balancer: kafka.Murmur2Balancer{},

		RequiredAcks:           eventWriterRequiredAcks,
		MaxAttempts:            eventWriterMaxAttempts,
		BatchSize:              eventWriterBatchSize,
		BatchBytes:             eventWriterBatchBytes,
		BatchTimeout:           eventWriterBatchTimeout,
		WriteTimeout:           eventWriterWriteTimeout,
		ReadTimeout:            eventWriterReadTimeout,
		Async:                  eventWriterAsync,
		AllowAutoTopicCreation: eventWriterAllowAutoTopicCreation,
		Transport:              p.transport,
		Logger:                 kafkaLogger{level: logrus.DebugLevel, topic: topic},
		ErrorLogger:            kafkaLogger{level: logrus.ErrorLevel, topic: topic},
	}
}

// ErrTopicNotOwned is returned when a publish names a topic outside the Blnk-owned
// namespace.
//
// It is a sentinel rather than a formatted error so a caller can classify it without
// matching on message text, and so the topic-membership refusal is provably the same
// condition wherever it is checked.
var ErrTopicNotOwned = errors.New("blnk: refusing to publish to a topic Blnk does not own")

// writerFor returns the writer for a topic, creating and caching one only for topics
// inside the Blnk-owned namespace.
//
// # VALID-01: lazy growth had no membership check, and that was a data-driven write
//
// The destination of a publish comes from a STORED OUTBOX ROW. Lazy writer creation with no
// membership test therefore meant that any topic name which reached the table would be
// created as a writer and published to — using Blnk's own producer credentials, on Blnk's
// own broker, at the direction of stored data. A row carrying "attacker.transactions", or a
// name with an injected segment, would be delivered exactly as asked. The persistence layer
// now refuses such a row at insert, and this is the second half of that defence: the two
// together mean a row would have to bypass validation AND survive here to reach a foreign
// topic.
//
// # What is admitted, and why the cache is not a hole in it
//
// Two sets of topics are served, and they have different provenance:
//
//   - THE PRE-CREATED SET, built at construction from AllTopicsWithDeadLetters — that is,
//     from BLNK'S OWN CONFIGURATION. Under a fixed prefix this is already every owned name,
//     since the owned namespace is exactly prefix.<category> and its `.dlt` sibling for the
//     five known categories. These are served from the fast path with no membership test,
//     which is safe precisely because stored data had no say in which ones exist.
//   - LAZILY GROWN NAMES, which are the only ones a stored row can influence, and the only
//     ones the membership test governs. It uses model.IsBlnkEventTopic against the CURRENTLY
//     CONFIGURED prefix, so a name is admitted only if it is prefix.<known-category>
//     optionally suffixed `.dlt`. Nothing else is.
//
// Growth is reachable in exactly one situation: a TOPIC PREFIX CHANGE. The prefix is re-read
// from live configuration on every naming call, so a reload starts resolving events to names
// this publisher was not constructed with; those names are owned under the new prefix and are
// admitted and cached.
//
// A STORED ROW FROM BEFORE SUCH A CHANGE keeps working, because its topic is in the
// pre-created set — the process built a writer for it at startup, from the prefix that was in
// force then. So the committed event still reaches the topic it was always bound for, which
// is what the row recorded its destination for in the first place. A row naming a prefix this
// process never had is the one case that is refused, and refusing it loses nothing: the row
// stays claimable, its failure names the reason, and an operator can re-point it.
//
// The fast path takes only a read lock, so concurrent publishes to the pre-created topics
// never serialise, and no membership test is paid on it. Growth double-checks under the
// write lock so two goroutines racing on a new topic share one writer rather than orphaning
// one.
//
// Parameters:
//   - topic string: a non-empty, fully-resolved topic name.
//
// Returns:
//   - *kafka.Writer: the writer for that topic.
//   - error: ErrEventPublisherClosed when the publisher has been closed, or
//     ErrTopicNotOwned when the topic lies outside the Blnk-owned namespace.
func (p *kafkaPublisher) writerFor(topic string) (*kafka.Writer, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()

		return nil, ErrEventPublisherClosed
	}

	writer, found := p.writers[topic]
	p.mu.RUnlock()

	if found {
		return writer, nil
	}

	// Checked BEFORE the write lock is taken, so a rejected topic never contends with
	// live publishes and never enters the map.
	// Pinned to the CONFIGURED prefix, deliberately, and not to the owned FORM. A form test
	// would also admit '<someone else>.transactions', and this is the one place a topic name
	// turns into an outbound connection, so the narrower test is the right one here.
	//
	// It does not strand an event stored before a KAFKA_TOPIC_PREFIX change: the publisher
	// pre-creates the whole inventory of the prefix it was BUILT with, so such a row names a
	// topic already in the map and is served by the fast path above without reaching this
	// check at all. What is refused is a generation that predates this process, which is an
	// operator action — restore the prefix, or drain the old topics — rather than something
	// to admit silently. IsOwnedTopicForm is the wider form test, for callers that need it.
	if !model.IsBlnkEventTopic(topic, TopicPrefix()) {
		logrus.WithFields(logrus.Fields{
			"topic":          topic,
			"owned_prefix":   TopicPrefix(),
			"owned_topics":   len(p.writers),
			"refusal_reason": "topic is not in the Blnk-owned namespace",
		}).Error("refusing to create a Kafka writer for a topic Blnk does not own")

		return nil, fmt.Errorf("%w: %q is not %s.<category> or %s.<category>.dlt",
			ErrTopicNotOwned, topic, TopicPrefix(), TopicPrefix())
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrEventPublisherClosed
	}

	// Re-check: another goroutine may have created this writer between the read
	// unlock and the write lock.
	if writer, found = p.writers[topic]; found {
		return writer, nil
	}

	logrus.WithField("topic", topic).Info(
		"creating a Kafka writer for a Blnk-owned topic outside the pre-created inventory; " +
			"this is expected after a topic-prefix change",
	)

	writer = p.newWriter(topic)
	p.writers[topic] = writer
	p.lazyTopics = append(p.lazyTopics, topic)

	retired, retiredTopic := p.retireOldestLazyWriterLocked()

	if retired != nil {
		lazyCount := len(p.lazyTopics)

		// Closed in a goroutine so it happens OUTSIDE this function's deferred unlock.
		// Close flushes whatever the writer still holds, and doing that under the write
		// lock would stall every concurrent publish — including publishes to the eight
		// known topics, which have nothing to do with this retirement.
		go func() {
			if err := retired.Close(); err != nil {
				logrus.WithError(err).WithField("topic", retiredTopic).Warn(
					"failed to close a retired Kafka writer while bounding the writer cache",
				)
			}

			logrus.WithFields(logrus.Fields{
				"retired_topic": retiredTopic,
				"new_topic":     topic,
				"lazy_writers":  lazyCount,
				"bound":         maxLazyTopicWriters,
			}).Info(
				"retired the oldest lazily-created Kafka writer to stay within the writer cache bound",
			)
		}()
	}

	return writer, nil
}

// retireOldestLazyWriterLocked evicts the oldest lazily-created writer once the cache is
// over its bound, and returns it for the caller to close.
//
// This is a CACHE EVICTION and not a refusal. A retired topic that is published to again
// simply gets a new writer on the next call, so no event is ever stranded by an eviction —
// which is what makes bounding the cache a cost decision rather than a correctness one. What
// eviction costs is one reconnection for a topic that has not been used recently; what it
// prevents is a writer pool, and its connections, that only ever grows.
//
// The caller must hold the write lock. The returned writer is NOT closed here, because
// closing flushes and would stall every concurrent publish if done under the lock.
//
// Returns:
//   - *kafka.Writer: the retired writer, or nil when the cache is within its bound.
//   - string: the topic the retired writer served, or "".
func (p *kafkaPublisher) retireOldestLazyWriterLocked() (*kafka.Writer, string) {
	if len(p.lazyTopics) <= maxLazyTopicWriters {
		return nil, ""
	}

	oldest := p.lazyTopics[0]
	p.lazyTopics = p.lazyTopics[1:]

	retired, found := p.writers[oldest]
	if !found {
		return nil, ""
	}

	delete(p.writers, oldest)

	return retired, oldest
}

// Publish publishes an event to the topic its type routes to.
//
// This is the mandated contract, and it is deliberately the thin one: it submits a
// PublishRequest carrying nothing but the event and returns the error, so there is
// exactly one publish implementation underneath and no second code path that could
// behave differently. Callers that need the destination, the key, the attempt number or
// the result use PublishToTopic.
//
// The destination is resolved with TopicForEvent, and the message key falls back to the
// event's aggregate ID because the stored partition key is not carried in the envelope —
// see resolvePartitionKey, which documents exactly what that fallback preserves.
//
// Parameters:
//   - ctx context.Context: cancels the partition-metadata lookup and the wait for the
//     broker acknowledgement.
//   - event model.LedgerEvent: the envelope to publish.
//
// Returns:
//   - error: nil when the broker acknowledged the write, otherwise a *PublishError.
func (p *kafkaPublisher) Publish(ctx context.Context, event model.LedgerEvent) error {
	_, err := p.PublishToTopic(ctx, PublishRequest{Event: event})

	return err
}

// PublishToTopic publishes one event and reports what happened.
//
// It makes EXACTLY ONE attempt. It never sleeps, never loops, and never decides that an
// event has failed for good: retry is the relay's responsibility, bounded by the relay's
// own budget, and the writer's internal retry loop is switched off so that one call here
// is one produce request. An error is returned in full, unswallowed, with a transient or
// permanent classification attached so the relay can act on it.
//
// Ordering. The message carries the resolved partition key and the writer uses a stable
// hash balancer, so every event for one key lands on one partition. Kafka orders within
// a partition, so that pairing — and nothing else — is what delivers the per-aggregate
// ordering guarantee.
//
// Bytes. The envelope is serialised by marshalLedgerEvent, which splices the payload
// bytes through untransformed. That is what makes the dual-delivery and replay
// byte-equality guarantees achievable rather than approximate.
//
// Observability. Three instruments are recorded on every call: the attempt counter
// attributed by outcome, the duration histogram attributed by topic and attempt, and —
// on success only — the published-events counter attributed by topic and event type.
// Recording the duration for failures as well as successes is deliberate: a broker that
// times out is exactly the case where latency data matters, and the attempt attribute
// keeps first-attempt latency readable on its own.
//
// Parameters:
//   - ctx context.Context: cancels the metadata lookup and the acknowledgement wait.
//   - req PublishRequest: the event and its optional destination, key and attempt.
//
// Returns:
//   - PublishResult: the record of this attempt, always populated, even on failure.
//   - error: identical to the result's Err field; nil on success.
func (p *kafkaPublisher) PublishToTopic(ctx context.Context, req PublishRequest) (PublishResult, error) {
	started := time.Now()

	// EVERY field is resolved through the same helpers the no-op uses, and the last two
	// are not decoration: they are read by code below and in recordPublishAttempt.
	//
	// MaxAttempts is what lets fail() distinguish "this attempt failed and another will
	// follow" from "this attempt spent the budget", which is the difference between
	// retrying and failed on the attempts counter. Left unset it is always zero, so a
	// terminal failure is reported as retry pressure that no longer exists.
	//
	// Purpose is read twice: the published-events counter below increments only for an
	// ORIGINAL publish, and attemptLabel turns a replay or a dead-letter write into its
	// own fixed attempt token. Left unset it is the empty string, which is neither
	// PublishPurposeOriginal nor either of the other two — so the counter that is the
	// denominator of the dead-letter rate would never increment at all, and a replay
	// would be labelled with a retry-sequence attempt number it does not belong to.
	result := PublishResult{
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
		// Carried from the envelope so that EVERY exit path below reports it, exactly as
		// the topic and key are. It is the capture instant the relay persisted with the
		// row; see PublishResult.CapturedAt for why the interval it opens is the one V-1
		// is stated over.
		CapturedAt: req.Event.OccurredAt,
		// RESOLVED HERE, on the same line of reasoning as the topic and the key above, and
		// for the same reason the no-op resolves them: every one of the five is a request
		// field with a documented fallback, and a result that omits one silently changes
		// what the pipeline reports.
		//
		// These two in particular are load-bearing rather than cosmetic. The PURPOSE gates
		// the EventsPublishedTotal increment below, which is the DENOMINATOR of the
		// dead-letter rate; left at its zero value it never equals PublishPurposeOriginal,
		// so the counter would never move and the rate would be undefined. The BUDGET is
		// what lets fail() tell a failure that still has attempts left from the one that
		// spent the last of them, and it is the "of 5" in the "attempt 3 of 5" that
		// requirement R-4 requires on every attempt.
		MaxAttempts: resolveMaxAttempts(req),
		Purpose:     resolvePurpose(req),
	}

	// elapsed measures from the outbox claim when the relay supplied that instant, so
	// the histogram reports what its documentation promises: claim to acknowledgement.
	// Otherwise it measures the write alone.
	elapsed := func() time.Duration {
		if req.ClaimedAt.IsZero() {
			return time.Since(started)
		}

		return time.Since(req.ClaimedAt)
	}

	value, err := marshalLedgerEvent(req.Event)
	if err != nil {
		// A payload that is not valid JSON cannot be spliced into an envelope without
		// producing a message that breaks every subscriber's parser, so this is
		// permanent by construction: no number of retries turns corrupt bytes into
		// valid ones, and the dead-letter topic would reject them for the same reason.
		// The durable record is the outbox row itself, whose last_error names the
		// problem for triage.
		result.Duration = elapsed()
		failed := p.fail(ctx, result, err, false)

		return failed, failed.Err
	}

	// SIZE-01: the size ceiling is enforced HERE, on the fully-marshalled envelope, and
	// not only on the stored payload.
	//
	// Persistence validates the payload it is handed, which is the right place to reject a
	// caller's oversized data. But what Kafka is asked to accept is this envelope: the
	// payload plus event_id, event_type, aggregate_id, occurred_at and schema_version, and
	// for a dead-letter copy the whole failure_metadata object as well. So a payload that
	// passed validation can still produce a message over the limit, and without a check
	// here that message reaches the writer, is rejected by it or by the broker on every
	// attempt, and — because the dead-letter copy is strictly larger — cannot be
	// dead-lettered either. The row would then never reach a terminal state: an unbounded,
	// self-inflicted backlog from one event.
	//
	// The failure is PERMANENT. Retrying cannot shrink a message, and the broker's answer
	// will not change, so classifying it transient would spend the whole retry budget
	// establishing what is already known. Marked permanent, the relay exhausts it
	// immediately and the operator sees the real reason in last_error.
	if len(value) > model.MaxEventMessageBytes {
		result.Duration = elapsed()
		failed := p.fail(ctx, result, fmt.Errorf(
			"%w: the serialised event is %d bytes, over the %d byte maximum; "+
				"retrying cannot shrink it and a dead-letter copy would be larger still",
			ErrEventMessageTooLarge, len(value), model.MaxEventMessageBytes,
		), false)

		return failed, failed.Err
	}

	writer, err := p.writerFor(result.Topic)
	if err != nil {
		result.Duration = elapsed()

		// A closed publisher is permanent for this attempt: this instance will not
		// accept another write. The event is safe, because its row is only marked
		// dispatched on success and becomes claimable again when the lease expires.
		//
		// A refused topic (ErrTopicNotOwned) is permanent for a different and stronger
		// reason: the destination itself is not one Blnk may write to, so no retry and no
		// broker state can make the write legitimate.
		failed := p.fail(ctx, result, err, false)

		return failed, failed.Err
	}

	// The carrier the broker's coordinate comes back on. It rides with the message through
	// WriterData, which never reaches the wire, and is filled by completeWrite.
	acknowledgement := &publishAcknowledgement{}

	message := kafka.Message{
		// Topic is intentionally left empty. kafka-go rejects a message whose topic is
		// set when the writer already has one, and the writer here is per-topic.
		Key:        partitionKeyBytes(result.PartitionKey),
		Value:      value,
		Time:       req.Event.OccurredAt,
		WriterData: acknowledgement,
	}

	if err := writer.WriteMessages(ctx, message); err != nil {
		result.Duration = elapsed()
		failed := p.fail(ctx, result, err, classifyTransientPublishError(err))

		return failed, failed.Err
	}

	result.Duration = elapsed()
	result.Status = model.PublishStatusDispatched

	// The coordinate, read AFTER the write returned. Async is false, so WriteMessages has
	// already blocked on the completion callback and the carrier is filled.
	//
	// Its absence is not an error and does not fail the publish: the broker acknowledged the
	// write, so the record exists whether or not the library reported where. What it does
	// mean is that this row will be counted as an UNCONFIRMED publication by the zero-loss
	// audit, which is the honest outcome — and the log line says so, because a systematic
	// absence here would quietly render the reconciliation inconclusive for every event.
	if record, confirmed := acknowledgement.coordinate(); confirmed {
		result.Record = record
	} else {
		logrus.WithFields(logrus.Fields{
			"event_id": hashLogIdentifier(result.EventID),
			"topic":    boundedTopicLabel(result.Topic),
		}).Warn(
			"the broker acknowledged this event but reported no partition or offset, so the row " +
				"cannot name the record it produced; it will count as an unconfirmed publication in " +
				"the zero-loss reconciliation",
		)
	}

	// ONLY an original publish increments this counter. It is the denominator of the
	// dead-letter rate, so a replay or a dead-letter write counted here would make that
	// rate depend on how much triage happened that day rather than on how the pipeline is
	// behaving.
	if result.Purpose == PublishPurposeOriginal {
		metrics.EventsPublishedTotal.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
			attribute.String(publishAttrEventType, boundedEventTypeLabel(result.EventType)),
		))
	}

	recordPublishAttempt(ctx, result)

	// GUARDED, and the failure log deliberately is not. LogFields builds a map, hashes the
	// partition key and bounds the error string on every call, and this one runs once per
	// published event at 500 events per second — work whose result is discarded whenever
	// debug is off, which is every production deployment. The failure log stays unguarded
	// because requirement R-4 mandates the attempt number and error reason on EVERY
	// attempt.
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(result.LogFields()).Debug("ledger event published to kafka")
	}

	return result, nil
}

// fail completes a failed result: it attaches the classified error, records the attempt
// metrics, and logs the attempt.
//
// It exists so that every failure exit from PublishToTopic reports identically. The
// alternative — repeating the wrap, the two metric calls and the log line at each exit —
// is exactly how one of them ends up subtly different from the others.
//
// Requirement R-4 requires the attempt number and the error reason to be logged on EVERY
// attempt rather than only on the final one, which is why the log line is here, on the
// per-attempt path, and not in the relay's exhaustion branch.
//
// Parameters:
//   - ctx context.Context: used for metric recording.
//   - result PublishResult: the partially-populated result, with its duration already
//     set.
//   - cause error: the underlying failure.
//   - transient bool: whether the failure looks recoverable.
//
// Returns:
//   - PublishResult: the completed result, with Status retrying and Err populated.
func (p *kafkaPublisher) fail(ctx context.Context, result PublishResult, cause error, transient bool) PublishResult {
	// RETRYING and DEAD-LETTERED are not the same outcome, and reporting every failure as
	// retrying made a permanently-stuck event indistinguishable from a busy one in both
	// the logs and the attempts counter. Another attempt is possible only when the failure
	// looked transient AND the attempt did not spend the budget the row stated; a
	// permanent error is terminal whatever the budget says, because no number of further
	// attempts makes corrupt bytes valid or an oversized message small.
	//
	// A terminal attempt reports model.PublishStatusDeadLettered — the destination the
	// relay takes such a row to — rather than a fourth "failed" value. The observable
	// vocabulary is exactly three values (see model.PublishStatus): dashboards, alert
	// rules and the requirement all share that set, so a terminal outcome names the state
	// the event is bound for instead of widening the contract to describe the step. The
	// distinction that mattered survives: PublishStatusRetrying still means "this will be
	// attempted again" and nothing else does.
	result.Retryable = transient && !attemptBudgetSpent(result.Attempt, result.MaxAttempts)
	if result.Retryable {
		result.Status = model.PublishStatusRetrying
	} else {
		result.Status = model.PublishStatusDeadLettered
	}

	result.Transient = transient
	result.Err = &PublishError{
		Topic:     result.Topic,
		EventID:   result.EventID,
		EventType: result.EventType,
		Attempt:   result.Attempt,
		Transient: transient,
		Err:       cause,
	}

	recordPublishAttempt(ctx, result)

	logrus.WithFields(result.LogFields()).Error("publishing a ledger event to kafka failed")

	return result
}

// Close flushes and closes every writer and releases the shared transport's pooled
// connections.
//
// It is idempotent, so a second call is harmless, and NIL-SAFE, so it can be called on a
// publisher that was never assigned — which is what lets the Blnk shutdown path keep the
// simple nil-guarded shape it already uses for the asynq client. After it returns, every
// publish attempt fails with ErrEventPublisherClosed rather than panicking on a closed
// writer.
//
// Errors from individual writers are joined rather than short-circuited, because
// abandoning the loop on the first failure would leak the remaining writers'
// connections. Closing the shared transport is done explicitly here: a kafka.Writer only
// closes a transport it created itself, and this one is supplied to all of them.
//
// Returns:
//   - error: the joined close errors, or nil when every writer closed cleanly.
func (p *kafkaPublisher) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return nil
	}

	p.closed = true

	writers := make([]*kafka.Writer, 0, len(p.writers))
	for _, writer := range p.writers {
		writers = append(writers, writer)
	}

	p.writers = make(map[string]*kafka.Writer)
	transport := p.transport
	p.mu.Unlock()

	var errs []error
	for _, writer := range writers {
		if err := writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("blnk: closing the kafka writer for topic %q: %w", writer.Topic, err))
		}
	}

	if transport != nil {
		transport.CloseIdleConnections()
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	logrus.WithField("writers", len(writers)).Debug("kafka event publisher closed")

	return nil
}

// PublishRequestFromOutbox builds the publish request for a claimed outbox row.
//
// It is THE single place the load-bearing routing rule is applied — the Kafka message key is
// the row's LEDGER, falling back to its partition key, and the destination is the topic
// recorded on the row — so that the relay, the dead-letter writer and a replay cannot each
// apply it slightly differently. A relay that composed the request inline would be one
// refactor away from dropping the key and silently losing per-aggregate ordering, a defect
// that no unit test of the relay would notice.
//
// # Why the key is the ledger, and how the claim still composes with it
//
// Requirement R-6 partitions by LEDGER ID, and that is what this function keys on. The reason
// it composes with the database's ordering work is that the two values AGREE wherever a ledger
// exists: WithEventLedgerID writes the supplied ledger into ledger_id AND partition_key, and
// every payload that yields a ledger of its own does the same. So the value
// ClaimPendingEventOutbox serialises dispatch on — at most ONE row per partition_key in flight,
// which is the purpose of the NOT EXISTS anti-join in claimPendingEventOutboxQuery and of the
// idx_event_outbox_partition_key_inflight index behind it — is the same value Kafka partitions
// on, and the database's ordering guarantee reaches the subscriber intact.
//
// This is why populating the ledger AT CAPTURE TIME is not optional. Transaction payloads carry
// no ledger field, so the producer must state it: transaction execution takes it from the
// source balance it has already loaded, and the ledger and balance hooks hold the entity
// itself. Without that, ledger_id is NULL, the key falls through to partition_key, and
// whole-ledger affinity is lost silently — which is exactly the failure model
// model.EventOutbox.PartitionKey's own documentation warns about, in the other direction.
//
// Events that genuinely have NO ledger keep their partition-key affinity through the fallback:
// balance monitors key on the monitored balance, identities on the identity, bulk batches on
// the batch, and system errors on the event type. Each is stable per aggregate, so per-aggregate
// ordering holds for all of them.
//
// # The fallback chain, and why ledger_id comes FIRST
//
// The chain is ledger_id → partition_key → aggregate_id, and that order is requirement R-6
// stated in code: "partitioned by ledger ID". Wherever the row records a ledger, that ledger
// IS the key, so every event of one ledger lands on one partition and a subscriber reading
// that partition observes the ledger's whole event sequence in order.
//
// Reading partition_key first was the previous order and it was wrong in exactly one case,
// which is also the most common one. Every production capture path now supplies the ledger
// through WithEventLedgerID — transaction execution takes it from the loaded source balance,
// the ledger and balance hooks from the entity itself — and that option sets BOTH columns, so
// the two agree and the order makes no difference. But a row written by a caller that set only
// ledger_id, or one whose partition_key was derived from the payload before the ledger was
// known, would have been keyed on the weaker value with nothing to show it. Preferring the
// ledger removes that case rather than documenting it.
//
// partition_key remains the next rung and is the one that carries the events with NO ledger:
// balance monitors, identities, bulk batches and system errors. It is NOT NULL in the schema,
// has a not-blank CHECK, and PrepareEventOutbox guarantees a value through its own chain, so a
// row read back from the database always carries one. resolvePartitionKey supplies the final
// rung, the aggregate id, for a request assembled in Go rather than read from a row. Every
// rung is stable per aggregate, so ordering survives all of them; only an event belonging to
// no aggregate at all ends up unkeyed.
//
// The row's own topic is used rather than re-deriving one from the event type, because the
// row recorded its destination at insert time precisely so it stays publishable to the
// topic it was always meant for. The row's claim instant is not stored on the row, so the
// caller passes it separately by setting ClaimedAt on the returned request when it wants
// claim-to-acknowledgement latency measured.
//
// Parameters:
//   - row model.EventOutbox: the claimed row.
//   - attempt int: the 1-based attempt number this publish represents. Values below 1 are
//     normalised to 1 when the request is served.
//
// Returns:
//   - PublishRequest: a request whose event is reconstructed from the row's envelope
//     columns, keyed by the row's partition key and targeted at the row's topic.
func PublishRequestFromOutbox(row model.EventOutbox, attempt int) PublishRequest {
	return PublishRequest{
		Event: model.LedgerEvent{
			EventID:       row.EventID,
			EventType:     row.EventType,
			AggregateID:   row.AggregateID,
			OccurredAt:    row.OccurredAt,
			Payload:       row.Payload,
			SchemaVersion: row.SchemaVersion,
		},
		Topic: row.Topic,
		// LEDGER FIRST — requirement R-6 partitions by ledger ID. See the fallback chain above.
		Key:     firstNonBlank(row.LedgerID, row.PartitionKey),
		Attempt: attempt,
	}
}

// marshalLedgerEvent serialises the envelope for the wire, splicing the payload bytes in
// VERBATIM.
//
// The envelope is composed member by member rather than handed to encoding/json as a
// struct, and that is a correctness decision, not a micro-optimisation. Marshalling the
// struct would route the payload through the encoder's compactor, which strips
// insignificant whitespace and — because HTML escaping is on by default — rewrites `<`,
// `>` and `&` inside the raw payload as escape sequences. Either transformation breaks the
// two guarantees the payload column exists to provide: that the Kafka message and the
// legacy webhook body are identical during the dual-delivery window because both are
// produced from the same stored bytes, and that replaying a dead-lettered event
// reproduces the original message. Splicing preserves the bytes exactly, whatever they
// contain.
//
// Every scalar member is still encoded with encoding/json, so string escaping and the
// RFC3339 rendering of the timestamp are byte-identical to what marshalling the struct
// would produce. Member order matches the order the struct declares its fields, which is
// the order the documented serialised shape shows.
//
// Two payload conditions are handled deliberately:
//
//   - EMPTY bytes serialise as the JSON null literal, with a warning. The envelope stays
//     structurally valid and the event stays published, observable and replayable, which
//     is strictly better than losing it over a producer defect.
//   - INVALID bytes are rejected. Splicing them would emit a message that breaks every
//     subscriber's parser, so the publish fails permanently instead. The event is not
//     lost: the outbox row is the durable record, and its error column names the reason.
//
// Parameters:
//   - event model.LedgerEvent: the envelope to serialise.
//
// Returns:
//   - []byte: the message value.
//   - error: non-nil when the payload is not valid JSON, or when the timestamp cannot be
//     rendered as RFC3339 (a year outside the four-digit range).
func marshalLedgerEvent(event model.LedgerEvent) ([]byte, error) {
	payload := event.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		logrus.WithFields(logrus.Fields{
			"event_id":   event.EventID,
			"event_type": event.EventType,
		}).Warn("ledger event has an empty payload; publishing it as a JSON null payload")

		payload = jsonNull
	} else if !json.Valid(payload) {
		return nil, fmt.Errorf(
			"blnk: the payload of event %s (%s) is not valid JSON and cannot be published",
			event.EventID, event.EventType,
		)
	}

	// The scalar members, in the order the envelope declares them. Encoding each through
	// encoding/json is what keeps escaping and timestamp formatting identical to a
	// struct marshal.
	scalars := [4]struct {
		key   string
		value interface{}
	}{
		{"event_id", event.EventID},
		{"event_type", event.EventType},
		{"aggregate_id", event.AggregateID},
		{"occurred_at", event.OccurredAt},
	}

	var buf bytes.Buffer
	buf.Grow(len(payload) + envelopeScaffoldBytes)
	buf.WriteByte('{')

	for i, scalar := range scalars {
		encoded, err := json.Marshal(scalar.value)
		if err != nil {
			return nil, fmt.Errorf(
				"blnk: encoding %s for event %s (%s): %w",
				scalar.key, event.EventID, event.EventType, err,
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
	buf.WriteString(strconv.Itoa(event.SchemaVersion))
	buf.WriteByte('}')

	return buf.Bytes(), nil
}

// envelopeScaffoldBytes is a sizing hint: the member names, quotes, separators, braces and
// the encoded scalars of a typical envelope. It only pre-sizes the buffer, so an imprecise
// value costs at most one reallocation and can never affect the output.
const envelopeScaffoldBytes = 256

// resolveTopic returns the destination for a request: the topic it names, or the topic the
// event type routes to when it names none.
//
// The fallback is what makes the mandated single-argument Publish work at all, and it is
// correct for its one real caller — an event that never passed through the outbox and so
// has no recorded destination, such as an internal-error notification. Anything read from
// the outbox names its topic explicitly.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - string: a non-empty topic name; TopicForEvent never returns empty.
func resolveTopic(req PublishRequest) string {
	if topic := strings.TrimSpace(req.Topic); topic != "" {
		return topic
	}

	return TopicForEvent(req.Event.EventType)
}

// resolvePartitionKey returns the Kafka message key for a request.
//
// THE KEY IS WHAT THE CALLER SUPPLIED — for an outbox-backed publish that is the row's ledger
// ID, or its stored partition key when the event has no ledger; see PublishRequestFromOutbox,
// which resolves that precedence in one place.
// Every event sharing a key hashes to one partition, Kafka orders within a partition,
// and that is the whole mechanism behind the per-aggregate ordering guarantee. It is the
// same idea the transaction queue already applies when it shards by hashing the source
// balance ID, applied to the transport that now carries the events. It is also the value
// ClaimPendingEventOutbox serialises dispatch on, so keying on it is what makes the
// database's ordering domain and the broker's partitioning domain the same domain.
//
// The partition key is NOT one of the six envelope fields — the envelope is a fixed
// subscriber-facing contract and carries the aggregate ID instead — so it reaches this
// function only when a caller supplies it, which PublishRequestFromOutbox does from the
// ledger-id and partition-key columns of the claimed outbox row. When it is absent, the
// aggregate ID is used, and the consequences of that fallback are worth stating precisely:
//
//   - It PRESERVES per-aggregate ordering, which is the property acceptance requires.
//     Every event for one aggregate still shares a key and therefore a partition.
//   - It does NOT preserve whole-ledger partition affinity, so events for different
//     aggregates in the same ledger may land on different partitions. Nothing requires
//     ordering across aggregates, so nothing is lost.
//   - It cannot split ONE aggregate's stream across two partitions, which would be the
//     only genuinely harmful outcome, because outbox-backed events always arrive with the
//     recorded partition key and non-outbox events never share an aggregate with them.
//
// An empty result means the message is written with no key at all and is spread across
// partitions by the balancer. That is the right answer for an event belonging to neither a
// ledger nor an aggregate — an internal-error notification, for instance — where there is
// nothing to order it against and pinning every such event to one partition would only
// create a hot spot. It is unreachable for an outbox-backed event, whose partition key is
// NOT NULL, non-blank by CHECK, and guaranteed by PrepareEventOutbox's own fallback chain.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - string: the partition key, or the empty string when the request carries neither a
//     supplied key nor an aggregate ID.
func resolvePartitionKey(req PublishRequest) string {
	if key := strings.TrimSpace(req.Key); key != "" {
		return key
	}

	return strings.TrimSpace(req.Event.AggregateID)
}

// partitionKeyBytes converts a resolved key to the message key.
//
// An empty key becomes NIL rather than an empty byte slice, and the distinction is
// material: kafka-go's Murmur2 balancer — like the Java partitioner it reproduces — treats
// a nil key as absent and spreads the message across partitions, while an empty non-nil
// slice is a defined value that hashes to one fixed partition. Sending keyless events to a
// single partition would concentrate them needlessly.
//
// Parameters:
//   - key string: the resolved partition key. May be empty.
//
// Returns:
//   - []byte: the key bytes, or nil when key is empty.
func partitionKeyBytes(key string) []byte {
	if key == "" {
		return nil
	}

	return []byte(key)
}

// resolveAttempt normalises the attempt number to a 1-based value.
//
// The zero value of PublishRequest.Attempt means "not stated", which is the first attempt;
// a negative value can only be a caller mistake and is treated the same way. Normalising
// here keeps the metric's attempt attribute a small, stable label set instead of admitting
// "0" and negative values into it.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - int: the attempt number, never below 1.
func resolveAttempt(req PublishRequest) int {
	if req.Attempt < 1 {
		return 1
	}

	return req.Attempt
}

// normalizeBrokers cleans a configured broker list.
//
// Both transformations are needed rather than defensive. Broker lists arrive from
// KAFKA_BROKERS as a comma-separated value, and envconfig splits on the comma without
// trimming, so "host-a:9092, host-b:9092" yields an entry with a leading space that would
// be dialled verbatim and fail. An entry that is empty or whitespace-only comes from a
// trailing comma or a stray newline in an environment file, and dropping it is what makes
// KAFKA_BROKERS=" " resolve to "no brokers configured" — the no-op publisher — rather than
// to one unusable broker.
//
// Order is preserved, since the bootstrap order is the operator's stated preference, and
// duplicates are left alone because a repeated bootstrap address is harmless.
//
// Parameters:
//   - brokers []string: the configured list. May be nil.
//
// Returns:
//   - []string: a fresh slice of non-empty, trimmed addresses. Empty when nothing usable
//     was configured.
func normalizeBrokers(brokers []string) []string {
	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}

	return normalized
}

// KafkaBrokersConfigured reports whether a broker list holds anything usable.
//
// It is the exported form of the same question normalizeBrokers answers, for callers OUTSIDE
// this package — the server role, deciding whether to start the relay at all. Exporting the
// predicate rather than the normalisation keeps one definition of "configured": a caller
// testing len(brokers) > 0 for itself would treat KAFKA_BROKERS="," as configured, because
// envconfig splits it into a slice of blanks, and would then start a relay that could never
// connect to an address that is not an address.
//
// Parameters:
//   - brokers []string: the configured list. May be nil.
//
// Returns:
//   - bool: true when at least one entry is a non-blank address.
func KafkaBrokersConfigured(brokers []string) bool {
	return len(normalizeBrokers(brokers)) > 0
}

// classifyTransientPublishError decides whether a write failure looks recoverable.
//
// The verdict is ADVICE FOR THE RELAY, not a decision taken here: this file makes one
// attempt and reports, and the relay owns whether another attempt happens. What the
// classification buys is that a relay need not burn its whole budget on a failure that can
// never succeed, and need not give up on one that plainly can.
//
// Four sources of truth are consulted, in the order a failure is most likely to arrive:
//
//   - kafka.WriteErrors, the per-message slice a failed batch returns. It exposes no
//     Unwrap, so errors.As cannot see inside it and its elements must be inspected
//     directly. A batch is transient when any element is, which errs toward retrying:
//     retrying a permanent failure is bounded by the relay's budget and ends on the
//     dead-letter topic, whereas refusing to retry a transient one loses a recoverable
//     event.
//   - The library's own retriability judgement, through the Temporary and Timeout methods
//     that kafka.Error and the net package's errors expose. This is what classifies leader
//     elections, in-flight metadata changes, not-enough-replicas and request timeouts as
//     retriable, and authorisation and record-size failures as not.
//   - Context expiry. A deadline or cancellation says nothing about the broker's health,
//     so the same write may well succeed on the next attempt.
//   - Raw connection failures, which reach the caller unwrapped often enough to be worth
//     naming: a refused, reset or broken connection is the ordinary signature of a broker
//     restart.
//
// Anything unrecognised is reported as permanent. That is the conservative direction for an
// unknown failure, because an unbounded retry of something that can never succeed is worse
// than a dead-letter entry an operator can see and replay.
//
// Parameters:
//   - err error: the error returned by WriteMessages. May be nil.
//
// Returns:
//   - bool: true when the failure looks recoverable.
func classifyTransientPublishError(err error) bool {
	if err == nil {
		return false
	}

	var writeErrors kafka.WriteErrors
	if errors.As(err, &writeErrors) {
		for _, writeErr := range writeErrors {
			if writeErr != nil && classifyTransientPublishError(writeErr) {
				return true
			}
		}

		return false
	}

	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return true
	}

	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF)
}

// recordPublishAttempt records the two per-attempt instruments.
//
// Both are recorded for successes AND failures, which is what makes them useful: the
// attempt counter attributed by outcome is how retry pressure becomes visible independently
// of delivery volume, and the duration histogram attributed by attempt number is how
// first-attempt latency — the figure the latency target is stated against — is read
// separately from the latency of retried publishes. Recording duration only on success
// would blind the histogram to exactly the timeouts that matter most.
//
// The instruments themselves live in internal/metrics, declared as package-level variables
// and assigned in its single Init, which the package's own initialiser runs. Nothing here
// creates an instrument.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - result PublishResult: the completed attempt.
func recordPublishAttempt(ctx context.Context, result PublishResult) {
	metrics.EventPublishAttemptsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrOutcome, string(result.Status)),
	))

	// attemptLabel and not strconv: the attribute sits on a HISTOGRAM, so its cardinality
	// is multiplied by the bucket count and the domain has to stay closed at its eight
	// declared values. See attemptLabel for the three inputs that would otherwise widen it.
	//
	// The OUTCOME accompanies the attempt, because the latency target is stated over
	// first-attempt SUCCESSFUL publishes and the instrument's declaration in
	// internal/metrics spells that query out as
	// {attempt="1",outcome="dispatched"}. Recording the attempt alone would leave that
	// query matching nothing at all, so the p99 the acceptance criterion is read from
	// would be unreadable — while every dashboard still looked populated, because the
	// series exist under a shorter label set. Its domain is the same closed
	// model.PublishStatus vocabulary the attempts counter uses, so it adds no unbounded
	// dimension.
	metrics.EventPublishDuration.Record(ctx, result.Duration.Seconds(), otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrAttempt, attemptLabel(result.Purpose, result.Attempt)),
		attribute.String(publishAttrOutcome, string(result.Status)),
	))

	recordCaptureToDispatch(ctx, result)
}

// recordCaptureToDispatch records the END-TO-END age of an ACKNOWLEDGED event: from its
// capture in the transactional outbox to the broker's acknowledgement.
//
// This is the instrument acceptance criterion V-1 is read from, and it is recorded here — beside
// the per-attempt instruments, in the publisher — rather than in the relay, because the
// publisher owns every per-attempt instrument write. The relay deliberately does not import
// internal/metrics at all: the collector owns the gauges, and a second writer would make them
// disagree with themselves between ticks.
//
// It needs nothing from the relay to do this. The envelope the relay hands it already carries
// occurred_at, so the capture instant travels with the event.
//
// # Three rules, each from the instrument's declaration
//
// ONLY ACKNOWLEDGED PUBLISHES. A failed or retrying attempt has no end-to-end latency to
// report — the event has not arrived — and recording one would credit the histogram with a
// short duration for an event that is still waiting, which lowers the very quantile the
// criterion is read from.
//
// NO CAPTURE INSTANT, NO OBSERVATION. The envelope-only Publish path and any caller that omits
// occurred_at have no interval to measure, and a zero-valued time would render as an age of
// several decades.
//
// A NEGATIVE READING IS DROPPED, NOT CLAMPED. occurred_at is stamped by the process that
// captured the event and the acknowledgement is read from this one, so the figure carries
// whatever clock skew exists between them. Clamping a capture stamped in this process's future
// to zero would be indistinguishable from a genuinely instant publish and would quietly improve
// the quantile; a gap is the honest outcome.
//
// The attribute set is topic and attempt, and deliberately NOT outcome: only one outcome is
// ever recorded here, so the label would be a constant that multiplied the series count by
// nothing.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - result PublishResult: the completed attempt.
func recordCaptureToDispatch(ctx context.Context, result PublishResult) {
	if result.Status != model.PublishStatusDispatched || result.CapturedAt.IsZero() {
		return
	}

	age := time.Since(result.CapturedAt)
	if age < 0 {
		return
	}

	metrics.EventCaptureToDispatchDuration.Record(ctx, age.Seconds(), otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrAttempt, attemptLabel(result.Purpose, result.Attempt)),
	))
}

// kafkaLogger adapts kafka-go's logging hook to logrus, so that anything the client has to
// say about a broker arrives in the same structured stream as the rest of Blnk instead of
// on stderr, unstructured and unattributed.
//
// One value is attached per writer at each of two levels: the informational hook at debug,
// because it is chatty enough to drown a production log, and the error hook at error. The
// topic is captured so a message names the writer that produced it.
type kafkaLogger struct {
	level logrus.Level
	topic string
}

// Printf satisfies kafka-go's logger interface, forwarding the formatted message to logrus
// at this logger's level with the topic attached.
//
// Parameters:
//   - format string: the format string kafka-go supplies.
//   - args ...interface{}: its arguments.
func (l kafkaLogger) Printf(format string, args ...interface{}) {
	logrus.WithFields(logrus.Fields{
		"component": "kafka-writer",
		"topic":     l.topic,
	}).Logf(l.level, format, args...)
}
