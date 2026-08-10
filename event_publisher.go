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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

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
// instruments in internal/metrics: published events and broker acknowledgements are
// attributed by topic and event type, acknowledgements additionally by purpose, attempts by
// outcome, and duration by topic and attempt number.
const (
	publishAttrTopic     = "topic"
	publishAttrEventType = "event_type"
	publishAttrOutcome   = "outcome"
	publishAttrAttempt   = "attempt"

	// publishAttrPurpose separates an original publish from a replay and from a dead-letter
	// write on EventBrokerAcknowledgementsTotal. The value set is PublishPurpose's, which is
	// fixed and small, so it bounds cardinality by construction.
	publishAttrPurpose = "purpose"

	// publishAttrTerminal reports whether a failed attempt was the LAST one for its
	// event. It is a separate dimension rather than a fourth `outcome` value because the
	// publish-outcome vocabulary is a frozen three-value contract — dispatched, retrying,
	// dead_lettered — and "nothing further will be tried" is a property of the attempt,
	// not a different kind of outcome.
	//
	// Its domain is closed at "true" and "false", so it adds no unbounded dimension, and
	// it keeps "how many events are actually stuck" answerable as
	// {outcome="retrying",terminal="true"} — the selection that used to be
	// {outcome="failed"}.
	publishAttrTerminal = "terminal"
)

// The closed domain of publishAttrTerminal, spelled as constants so the publisher, the
// dead-letter writer, the tests and docs/metrics.md cannot disagree about the literals.
const (
	publishTerminalTrue  = "true"
	publishTerminalFalse = "false"
)

// terminalAttributeValue renders a result's terminal classification as its metric label.
//
// Parameters:
//   - result PublishResult: the completed attempt.
//
// Returns:
//   - string: publishTerminalTrue when no further attempt will be made, otherwise
//     publishTerminalFalse. A success is never terminal, so it reports false.
func terminalAttributeValue(result PublishResult) string {
	if result.Terminal {
		return publishTerminalTrue
	}

	return publishTerminalFalse
}

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
	// Every owned prefix, not only the configured one. A row captured before a prefix rename
	// still names the previous generation's topic and is still published, so collapsing its
	// name to the "unowned" label would report a legitimate delivery as an anomaly — and
	// would hide, behind one shared series, exactly the traffic an operator draining that
	// generation needs to watch. The set stays bounded because the allowlist is bounded by
	// config.MaxHistoricalTopicPrefixes: at most (1 + 4) prefixes times four categories
	// times the optional `.dlt` suffix.
	if IsOwnedTopicUnderAnyConfiguredPrefix(topic) {
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
	//
	// It is read for the result fields — event id, event type — and as the SOURCE OF
	// LAST RESORT for the message value. When Raw below is set, that is what goes on
	// the wire and this struct is not serialised at all.
	Event model.LedgerEvent

	// Raw is THE STORED CANONICAL ENVELOPE, and when present it is written to the topic
	// VERBATIM.
	//
	// # Why the bytes travel rather than the struct
	//
	// blnk.event_outbox.event_raw holds the envelope produced once at capture. Publishing
	// those stored bytes — rather than re-serialising Event on every attempt — is what
	// makes requirement R-5's byte-for-byte replay a property of the data instead of a
	// property of this file's serialiser never changing. A replay of a dead-lettered event
	// is then the same bytes the first attempt sent, whatever version of Blnk performs it.
	//
	// PublishRequestFromOutbox populates it from the row, so the relay, the dead-letter
	// writer and replay all get it without asking. It is EMPTY for a request assembled in
	// Go that never passed through the outbox — the mandated envelope-only Publish — and
	// for a row written before the column existed; both fall back to composing the envelope
	// from Event, which yields the bytes this version would have stored.
	//
	// It must be a complete JSON object: the dead-letter writer splices a failure_metadata
	// member onto it, and the size ceiling is enforced against it.
	Raw []byte

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

	// Traceparent and Tracestate are the W3C trace context recorded on the outbox row at
	// capture, carried here so the publish span can LINK back to the request that produced the
	// event.
	//
	// They are plain strings rather than a context or a trace.SpanContext deliberately: this
	// struct is a data carrier that a test builds by hand and that the no-op publisher accepts
	// unchanged, and a tracing type in it would make both of those depend on the tracing
	// packages. The publisher turns them into a link; see linkToCapturedTrace.
	//
	// Both are empty for an event captured with no active trace, which is a legitimate and
	// common state — the publish then produces an unlinked span rather than reporting a fault.
	Traceparent string
	Tracestate  string
}

// PublishResult is the observability record of ONE publish attempt. It exists because
// an error alone cannot answer the questions operations asks — which topic, which
// event, which attempt, how long, and is this worth retrying — and because the metrics
// layer is keyed on the outcome vocabulary it carries.
//
// The status vocabulary is model.PublishStatus, reused rather than redeclared so the
// code and the metric label set cannot drift. It is exactly three values, and the
// publisher itself reports two of them:
//
//   - PublishStatusDispatched when the broker acknowledged the write.
//   - PublishStatusRetrying for EVERY failed attempt. "Retrying" describes the
//     attempt's place in the sequence, not a decision this file took — whether
//     another attempt actually happens is the relay's call, made against the row's
//     own budget in SQL.
//
// # WHERE "nothing further will be tried" LIVES, now that it is not a status
//
// On three fields of this struct, never on Status: Transient carries the
// classification, Classified says a classification was made at all, and Retryable
// says whether the stated budget also permits another attempt. PermanentFailure()
// combines them, and it is what the relay reads. Requirement R-3 fixes the status
// vocabulary at three values and the vocabulary is a published metric label domain,
// so the distinction is kept as a property — where it can gain a reason code or a
// retry-after hint later — rather than as a fourth label nobody's dashboard selects.
//
// PublishStatusDeadLettered is never produced here, because dead-lettering is an
// acknowledged write to a `<topic>.dlt` sibling and this file never performs one.
// Reporting it from a failed original publish claimed a preservation that had not
// happened — and for an event whose destination is outside the topic catalogue, never
// would. The dead-letter writer stamps it with the DeadLettered method below, on the
// strength of a write the broker actually acknowledged.
type PublishResult struct {
	// Status is the outcome of this attempt, and the value the publish-attempts
	// counter is attributed by.
	Status model.PublishStatus

	// EventID is the event's UUID, which is also the subscriber idempotency key. It
	// is carried here so a log line about a failed attempt names the event an
	// operator has to go and look at.
	EventID string

	// EventType is the event name, and the value the broker-acknowledgement counter is
	// attributed by alongside the topic. The relay attributes the durable
	// published-events counter by the same pair, read off the outbox row, so the two are
	// directly comparable.
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
	//
	// It is ADVICE THE RELAY ACTS ON, not a field only the log reads. The relay reads it
	// through TerminalFailure and, when it is false on a failed attempt, records the row's
	// terminal state and hands it to the dead-letter writer immediately instead of
	// scheduling a retry. It used to be computed, logged and then ignored, which produced
	// a log that contradicted itself one line later — "retryable=false" followed by
	// "scheduled for another attempt" — and cost five broker round trips and seventeen
	// seconds of backoff per event on a condition no attempt could change, such as a
	// principal that is not authorised for the topic.
	Retryable bool

	// Terminal states that this attempt failed and NO FURTHER ATTEMPT WILL BE MADE for the
	// event: the failure is permanent, or it was the last attempt the stated budget allowed.
	// It is false on every success and false on a failure another attempt may recover from.
	//
	// It exists because the publish-outcome vocabulary is a frozen three-value contract —
	// dispatched, retrying, dead_lettered — and none of those three can carry this fact.
	// Widening the vocabulary with a fourth `failed` value was how it used to be carried,
	// and that put an implementation detail into a contract that subscribers' dashboards,
	// the metrics reference and the alert rules are all written against. A boolean beside the
	// status says the same thing without touching the contract.
	//
	// It is ALSO the affirmative verdict the relay's retry decision reads, through
	// PermanentFailure. That affirmativeness is a safety property: a result nobody populated
	// has Terminal false, so a publisher implementation that classifies nothing can never
	// end an event's life on its first attempt. "No verdict" must read as "not permanent".
	//
	// Distinguish it from Retryable, which is its near-complement but not its negation:
	// Retryable is false on a SUCCESS too, whereas Terminal describes failures only.
	Terminal bool

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
	// when subscribers were furthest behind. Both are recorded: for a SINGLE event the
	// difference between its two observations is that event's queue wait, which is what tells
	// an operator whether a slow end-to-end figure is the broker or the relay. Per event only
	// — subtracting the two p99 FIGURES is not a queue-wait percentile, because quantiles are
	// not subtractive; docs/metrics.md states that where the two series are catalogued.
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
	// authorised. It is meaningful only when Err is non-nil and Classified is true,
	// and it is advice for the relay's retry decision, never a decision taken here.
	Transient bool

	// Classified states that a publisher actually REACHED A VERDICT about Transient,
	// as opposed to leaving it at its zero value.
	//
	// It is the safety property that keeps a bare error from ending an event's life on
	// its first attempt, and it exists because `!Transient` is true on a result nobody
	// populated. The publisher is an interface seam — the relay borrows whichever
	// implementation the process built, and a double or a future implementation may
	// return an error with an empty result — so "no verdict" must read as "not
	// permanent" and fall through to the budgeted retry.
	//
	// It replaces the role model.PublishStatusFailed used to play in that test. Using
	// the status for it tied a safety check to a metric label domain: the check could
	// not be strengthened without widening a published label set, and requirement R-3
	// fixes that set at three values. A dedicated boolean says the same thing without
	// that coupling, and it is set in exactly one place — kafkaPublisher.fail, after
	// classifyTransientPublishError has run.
	Classified bool

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

// PermanentFailure reports whether this attempt failed for a reason NO FURTHER ATTEMPT
// COULD CHANGE: an unauthorised principal, a destination outside the topic catalogue,
// bytes that will never parse, a message over the size limit.
//
// It is the signal the relay's retry decision reads, and it answers a different question
// from "will there be another attempt". A transient failure on the last permitted attempt
// is also terminal, but it is terminal because the BUDGET ran out — a fact the database
// owns and decides inside MarkEventFailed's UPDATE, so that two instances racing on one
// row cannot both conclude they were last. That decision is deliberately left where it is;
// this predicate covers only the case the database cannot see.
//
// It is AFFIRMATIVE rather than a negation, and that is a safety property rather than a
// style: `!Retryable` and `!Transient` are both true on a result nobody populated, so a
// publisher implementation that returned a bare error with an empty result would have every
// failure treated as permanent and dead-lettered on the first attempt. The publisher is an
// interface seam — the relay borrows whichever implementation the process built — so
// "no verdict" must read as "not permanent". Requiring all three facts (an error, an
// explicit Classified verdict, and a non-transient classification) means only a publisher
// that actually classified the failure can end an event's life early. Anything else falls
// through to the budgeted retry.
//
// Classified is what carries that third fact. It used to be carried by the status —
// `Status == model.PublishStatusFailed` — which tied this safety check to a published
// metric label domain that requirement R-3 fixes at three values. The check is identical;
// only the field it reads changed, and it now reads a field that exists for this purpose
// alone and can be strengthened without touching a label set.
//
// Returns:
//   - bool: true when no further attempt can succeed, so the relay must take the row to its
//     terminal state now instead of scheduling a retry.
func (r PublishResult) PermanentFailure() bool {
	return r.Err != nil && r.Classified && !r.Transient
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
		// BOUNDED AND REDACTED, because this string came from a broker or a library rather
		// than from this codebase. Bounded: its length is not ours to choose, and it is
		// emitted once per attempt per event, so an unbounded value multiplied by the retry
		// budget and the event rate is how a log pipeline gets throttled for being over
		// quota. Redacted: a client error renders with the broker's address and port, and a
		// log is read by a wider audience than the deployment's operators. The verbatim text
		// stays reachable at debug through withLoggableCause's companion field.
		fields["error"] = loggableCause(r.Err)
		fields["transient"] = r.Transient
		// transient says what the failure LOOKED like; retryable says whether anything
		// further will actually be tried. They differ on the last permitted attempt, and
		// that is the case an operator most needs to be able to see.
		fields["retryable"] = r.Retryable
		// terminal is what the STATUS no longer carries. The publish-outcome vocabulary is
		// frozen at three values, so every failure logs status=retrying; this field is how
		// "and nothing further will be tried" reaches the log at all, and it is the field to
		// grep for when the question is "which events are actually stuck".
		fields["terminal"] = r.Terminal
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

// IsPermanentPublishError reports whether an error from a publish was AFFIRMATIVELY
// classified as one no further attempt could succeed.
//
// # It is not the negation of IsTransientPublishError, and that is the whole point
//
// `!IsTransientPublishError(err)` is true for an error this file never saw — a bare error
// from a borrowed writer, a wrapped context expiry, anything a caller constructed itself —
// because that function reports "not transient" for everything it does not recognise. Using
// the negation as a terminal verdict therefore reads "unclassified" as "give up", and the
// cost of that mistake is an event dead-lettered on its FIRST attempt with its whole retry
// budget unspent. That is the same trap PublishResult.PermanentFailure documents and avoids
// by requiring three positive facts rather than one negation.
//
// So this requires the error to have come out of this file's publish path — where
// classifyTransientPublishError made a decision against the broker's real answer — and to
// have been classified there as NOT recoverable. Anything else is "unknown", which is
// reported as false so the caller falls through to its budgeted retry.
//
// The two predicates are therefore deliberately NOT exhaustive: an error can be neither
// transient nor permanent, and that third state is the one every unrecognised failure
// occupies. A caller wanting a two-way split must choose which side "unknown" falls on, and
// the safe side is the budgeted one.
//
// Parameters:
//   - err error: the error returned by Publish or PublishToTopic. May be nil.
//
// Returns:
//   - bool: true only when err is (or wraps) a PublishError that was classified as
//     non-transient. False for nil, for an unrecognised error, and for a transient one.
func IsPermanentPublishError(err error) bool {
	var publishErr *PublishError
	if errors.As(err, &publishErr) {
		return !publishErr.Transient
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
//  2. A PublishError's OWN classification, with ONE correction applied to it. A permanent
//     failure is never an outage, and that direction is taken verbatim. A TRANSIENT failure
//     is an outage unless its only cause is this process giving up — see TAXONOMY-01 below.
//  3. The raw error, for a failure that reached the caller unwrapped — a borrowed writer's
//     WriteMessages error, or a double reporting the broker's own error code — classified by
//     brokerUnavailable.
//
// # TAXONOMY-01: a cancelled write is retryable, and it is NOT an outage
//
// This function used to return a PublishError's Transient flag unchanged, and to delegate the
// raw case to the retry classifier. Both readings answered "will another attempt help?" when
// the question asked is "is the broker the reason?", and for one class of failure those have
// opposite answers: a write abandoned because the process is shutting down, because the relay
// lost its outbox lease, or because a request budget expired is worth attempting again and
// says nothing whatever about Kafka.
//
// Reported as an outage it reached the replay endpoint's mapper, which answered
// EVENT_KAFKA_UNAVAILABLE / 503 with a message naming the broker and advising a retry once it
// recovers — for a broker that was never unhealthy. So context termination with no network or
// protocol signature anywhere in its chain is reported false here, and the retry verdict is
// left exactly as wide as it was: nothing about the relay's budget changes, because
// classifyTransientPublishError still calls such a failure transient.
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
		// A publisher that called the failure permanent is describing a defect in the event or
		// in this service, and no reading here overrides that.
		if !publishErr.Transient {
			return false
		}

		// TAXONOMY-01. The publisher's transient verdict is kept for every cause EXCEPT a bare
		// cancellation or expiry, which is this process's own decision rather than the broker's
		// condition.
		return !localContextTermination(publishErr)
	}

	return brokerUnavailable(err)
}

// localContextTermination reports that a failure is THIS PROCESS giving up — a cancelled
// context, a spent deadline, a lost lease, a shutdown — rather than anything observed about the
// broker.
//
// The concrete-signature test comes first and it is what makes the predicate safe. Go's net
// package maps a dial cancelled or timed out by its context onto errors that satisfy
// errors.Is(err, context.Canceled) and errors.Is(err, context.DeadlineExceeded), wrapped in a
// *net.OpError — so a broker that blackholes connections produces a real outage whose chain
// also looks like cancellation. Asking "does this carry an outage signature?" before "does this
// look cancelled?" keeps that case an outage; the reverse order would report an unreachable
// broker as an internal defect.
//
// Parameters:
//   - err error: the failure. A nil is reported false.
//
// Returns:
//   - bool: true only for a context termination with no broker or network signature at all.
func localContextTermination(err error) bool {
	if err == nil || brokerOutageSignature(err) {
		return false
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// brokerUnavailable classifies a RAW publish failure — one that has not been through
// kafkaPublisher.fail and so carries no verdict of its own.
//
// # TWO QUESTIONS, AND THEY ARE NOT THE SAME QUESTION (TAXONOMY-01)
//
// "Will another attempt help?" is classifyTransientPublishError. "Is the BROKER the reason?"
// is this. Every failure this reports is also retryable, so the set here is a SUBSET of the
// retryable set — but it is a strict subset, and the difference is the whole point of this
// function existing separately.
//
// This used to delegate wholesale to classifyTransientPublishError, and that made one class of
// failure lie about its cause. Context termination is retryable — a write abandoned because
// the process is shutting down, because the relay lost its outbox lease, or because a request
// budget expired says nothing at all about the event, so another attempt in a live process
// publishes it — but it is a LOCAL decision, not an outage. Reported as one it reached
// event_dlt.go's replay mapper, which answered EVENT_KAFKA_UNAVAILABLE / 503 with a message
// naming the broker as the problem and telling the operator to retry once it recovers. The
// broker was healthy; the caller had cancelled, or the deadline had passed. An operator
// reading that went looking at Kafka for a fault that was on this side of the connection.
//
// So the two verdicts are two predicates again, and the DISAGREEMENT the delegation existed
// to prevent is prevented structurally instead: every signature named below is one
// classifyTransientPublishError also reports, which is what preserves RETRY-01's invariant —
// A FAILURE CLASSIFIED broker_unavailable IS NEVER TERMINAL ON AN UNSPENT BUDGET — while
// letting the retry verdict stay strictly wider than the outage verdict.
//
// # Why the concrete signatures are tested BEFORE context termination
//
// The order is load-bearing, not stylistic. net.Dialer.DialContext maps an expired or
// cancelled dial onto errors that satisfy errors.Is(err, context.DeadlineExceeded) and
// errors.Is(err, context.Canceled) respectively, wrapped in a *net.OpError. A broker that
// blackholes SYNs therefore produces a genuine outage whose error chain also matches context
// termination. Testing the concrete network signature first keeps that case an outage;
// testing context first would have reported an unreachable broker as an internal defect,
// which is the same misclassification as the one being fixed, in the opposite direction.
//
// A bare context error — one that arrives with no network or protocol signature anywhere in
// its chain — is the only shape this reports false for, and it is exactly the shape that means
// "this process gave up", never "the broker is down".
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

	// The per-message slice a failed batch returns. It exposes no Unwrap, so errors.As cannot
	// see inside it and its elements must be inspected directly — the same reason
	// classifyTransientPublishError recurses by hand. A batch is an outage when ANY element
	// is: one message failing on an unavailable partition leader is the broker's condition,
	// not the batch's.
	var writeErrors kafka.WriteErrors
	if errors.As(err, &writeErrors) {
		for _, writeErr := range writeErrors {
			if writeErr != nil && brokerUnavailable(writeErr) {
				return true
			}
		}

		return false
	}

	return brokerOutageSignature(err)
}

// brokerOutageSignature recognises the failures that are EVIDENCE OF THE BROKER, as opposed to
// evidence of this process, this event, or this caller's deadline.
//
// It is the leaf brokerUnavailable is written in terms of, and it is deliberately a closed list
// of concrete signatures rather than a reach for an interface:
//
//   - A KAFKA PROTOCOL ERROR answers from its own code and never falls through. kafka.Error
//     implements Error, Timeout and Temporary, so it satisfies net.Error — testing the
//     interface would classify MessageSizeTooLarge, InvalidTopic and RecordListTooLarge, which
//     are defects in the event, as broker outages. Temporary() is what covers the broker-side
//     conditions kafka-go already knows are retriable (a leader election, not-enough-replicas,
//     a request the broker did not acknowledge in time), and the two codes it omits are named
//     explicitly: BrokerNotAvailable and ReplicaNotAvailable are the broker stating plainly
//     that it cannot serve the partition, and they are the pair a caller sees most during a
//     rolling restart.
//   - RAW CONNECTION FAILURES — refused, reset, a broken pipe, a truncated or closed stream.
//     These reach the caller unwrapped often enough to be worth naming, and each is the
//     ordinary signature of a broker that has just gone away.
//   - *net.OpError and *net.DNSError, which cover a dial, route or resolution failure whose
//     underlying cause is outside the errno set above — a resolution failure has no errno at
//     all, which is exactly how it once escaped classification entirely.
//
// Anything else is reported false, which is the conservative direction for this question: an
// unrecognised failure stays visible as a fault in Blnk rather than being written off as
// somebody else's outage.
//
// Parameters:
//   - err error: the failure. A nil is reported false.
//
// Returns:
//   - bool: true when the failure carries one of the signatures above.
func brokerOutageSignature(err error) bool {
	if err == nil {
		return false
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Temporary() ||
			kafkaErr == kafka.BrokerNotAvailable ||
			kafkaErr == kafka.ReplicaNotAvailable
	}

	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) {
		return true
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
// and their four dead-letter siblings, eight in all — enumerated from AllTopicsWithDeadLetters
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
		withLoggableCause(nil, credErr).Warn(
			"could not re-resolve the producer SASL principal for the initialisation log",
		)
	}

	// broker_count and auth_mode rather than the endpoint list and the principal. The
	// address list is topology: it tells a reader of the log where to aim, and it tells an
	// operator nothing they cannot get from their own configuration. The principal is the
	// other half of a SCRAM credential whose mechanism this same line publishes, so naming
	// it at info narrows a guess to one unknown. Both are available at debug below, which
	// is the sink for detail an operator has explicitly asked for.
	logrus.WithFields(logrus.Fields{
		"broker_count":    len(brokers),
		"topics":          len(publisher.writers),
		"sasl":            producerUser != "",
		"auth_mode":       publisherAuthMode(cnf.Kafka),
		"dedicated_sasl":  cnf.Kafka.SASLUser != "",
		"tls":             cnf.Kafka.TLS.Enabled,
		"required_acks":   "all",
		"balancer":        "murmur2",
		"topic_prefix":    TopicPrefix(),
		"internal_retry":  false,
		"max_event_bytes": model.MaxEventMessageBytes,
	}).Info("kafka event publisher initialised")

	// The identity-confirmation detail, at the level an operator turns on when they are
	// confirming exactly this. sanitizeLogValue because both values come from configuration
	// and neither their length nor their content is this codebase's to assume.
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(logrus.Fields{
			"brokers":        sanitizeLogValue(strings.Join(brokers, ","), maxLoggedErrorLength),
			"sasl_principal": sanitizeLogValue(producerUser, maxLoggedFilterLength),
		}).Debug("kafka event publisher endpoints and principal")
	}

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
	//
	// ACROSS EVERY OWNED PREFIX, not only the configured one. An outbox row records its
	// destination at insert time, so a deployment that changed KAFKA_TOPIC_PREFIX still
	// holds rows naming the previous generation's topics. Building the inventory from the
	// configured prefix alone meant those rows were served by a RUNNING process (which had
	// pre-created them before the change) and refused by a RESTARTED one — so a rename plus
	// a rolling restart silently stopped draining every event captured before it, and
	// stranded their dead-letter writes and replays with them. Each prefix an operator lists
	// in KAFKA_HISTORICAL_TOPIC_PREFIXES contributes its inventory here, which puts those
	// rows back on the fast path.
	for _, topic := range AllOwnedTopicsAcrossPrefixes() {
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

// requireLocalBrokersForPlaintext refuses plaintext to any broker that is not local.
//
// It is the enforcement half of KAFKA_INSECURE_LOCAL_DEV. That variable is an assertion
// about the ENVIRONMENT — "this broker is a local development broker" — and an assertion
// nobody checks is indistinguishable from a switch that disables encryption outright.
//
// # What counts as local, and why the list is not longer
//
// model.InternalDestinationReason decides, and it recognises exactly the shapes that
// cannot resolve outside the network Blnk runs in: loopback IP literals (including
// IPv4-mapped and NAT64-wrapped forms, which is why the decision is made on the parsed
// address rather than on the text), localhost and its subdomains, the .local and
// .internal zones, and unqualified single-label names. That last shape is what makes
// the local stack work unchanged — a Compose service name like "kafka" and an in-cluster
// Kubernetes Service name are both single-label — while "kafka.example.com" and a public
// IP are not.
//
// An unrecognised host is treated as REMOTE. That is deliberate: the cost of wrongly
// refusing an exotic local address is a clear error message and one configuration
// change, and the cost of wrongly permitting a remote one is ledger data and SASL
// credentials on the wire in cleartext.
//
// # Why this does not weaken the production path
//
// It runs only on the branch where TLS is already disabled AND the operator has already
// acknowledged local development. A deployment with TLS enabled never reaches it, so
// nothing about a TLS-enabled cluster's broker naming is constrained by this.
//
// Parameters:
//   - brokers: the configured broker list, each entry host:port or a bare host.
//
// Returns:
//   - error: naming every remote broker found, or nil when all are local.
func requireLocalBrokersForPlaintext(brokers []string) error {
	var remote []string

	for _, broker := range brokers {
		candidate := strings.TrimSpace(broker)
		if candidate == "" {
			continue
		}

		// SplitHostPort fails on a bare host, which is a legitimate way to write a
		// broker, so fall back to the whole value rather than rejecting it.
		host, _, err := net.SplitHostPort(candidate)
		if err != nil {
			host = candidate
		}

		// A non-empty reason means the classifier recognised the host as internal,
		// which is what this branch requires. An empty reason means it did not, and
		// that is the refusal.
		if model.InternalDestinationReason(host) == "" {
			remote = append(remote, candidate)
		}
	}

	if len(remote) == 0 {
		return nil
	}

	return fmt.Errorf(
		"blnk: KAFKA_INSECURE_LOCAL_DEV asserts that Kafka is a local development broker, but "+
			"%s not local: %s. Plaintext is refused. Ledger amounts, identity records and the "+
			"SASL/SCRAM handshake would all travel unencrypted to a remote host, and no later "+
			"rotation undoes a credential that has already crossed the network in the clear. "+
			"Either set KAFKA_TLS_ENABLED=true and configure KAFKA_TLS_CA_FILE for this broker, "+
			"or point KAFKA_BROKERS back at the local stack. KAFKA_INSECURE_LOCAL_DEV is not a "+
			"way to disable TLS for a remote cluster",
		map[bool]string{true: "this broker is", false: "these brokers are"}[len(remote) == 1],
		strings.Join(remote, ", "),
	)
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

		// THE ACKNOWLEDGEMENT IS SCOPED TO WHAT IT CLAIMS TO BE (F-24).
		//
		// KAFKA_INSECURE_LOCAL_DEV says "this is a local development broker". Until
		// now nothing checked that it was, so the flag permitted plaintext to ANY
		// broker — and because the local compose stack defaulted it to true, pointing
		// KAFKA_BROKERS at a remote host was enough to send every ledger amount, every
		// identity record and the SASL/SCRAM handshake itself across the public
		// internet in the clear. The only trace was a warning in a log, which is not a
		// control.
		//
		// A warning cannot be the control here because of WHERE the mistake happens:
		// the flag is set once, correctly, for local development, and the broker list
		// is changed later by someone doing something else entirely. Nobody re-reads
		// the flag at that moment, and nothing else in the stack objects.
		//
		// So the claim is now verified against the broker list it is being used to
		// reach. model.InternalDestinationReason is the same literal classifier the
		// webhook destination guard uses — loopback addresses, localhost, .local and
		// .internal zones, and unqualified single-label names, which is what a Docker
		// Compose service name and an in-cluster Kubernetes Service name both are. A
		// host it does not recognise as internal is treated as remote, which is the
		// safe direction for an unrecognised value.
		if err := requireLocalBrokersForPlaintext(cfg.Brokers); err != nil {
			return nil, err
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
//     four known categories. These are served from the fast path with no membership test,
//     which is safe precisely because stored data had no say in which ones exist.
//   - LAZILY GROWN NAMES, which are the only ones a stored row can influence, and the only
//     ones the membership test governs. It uses IsOwnedTopicUnderAnyConfiguredPrefix, so a
//     name is admitted only if it is <declared-prefix>.<known-category> optionally suffixed
//     `.dlt`. Nothing else is.
//
// Growth is reachable in exactly one situation: a PREFIX SET CHANGE. The prefixes are re-read
// from live configuration on every naming call, so a reload starts resolving events to names
// this publisher was not constructed with; those names are owned under the new set and are
// admitted and cached.
//
// A STORED ROW FROM BEFORE A PREFIX RENAME keeps working, provided the previous prefix is
// DECLARED. That declaration is the whole mechanism, and it was the gap here: the pre-created
// set used to be the configured prefix alone, so such a row was served by a RUNNING process —
// which had built a writer for it before the change — and refused by a RESTARTED one, whose
// inventory held only the new generation. A rename plus a rolling restart therefore stopped
// draining every event captured before it, and stranded its dead-letter writes and replays
// too. With the previous prefix listed, the inventory spans both generations and the committed
// event still reaches the topic it was always bound for.
//
// A row naming a prefix nobody declared is the one case that is refused, and refusing it loses
// nothing: the row stays claimable, its failure names the topic and the owned namespaces, and
// AuditStrandedTopicPrefixes reports the prefix at start-up with the variable to set.
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
	//
	// Pinned to the DECLARED prefixes, deliberately, and not to the owned FORM. A form test
	// would also admit '<someone else>.transactions', and this is the one place a topic name
	// turns into an outbound connection carrying Blnk's own producer credentials, so the
	// narrower test is the right one here. IsOwnedTopicForm is the wider form test, for
	// callers that need it.
	//
	// The declared set is the configured prefix plus KAFKA_HISTORICAL_TOPIC_PREFIXES, which
	// is what keeps a row captured before a prefix rename publishable. Those rows are
	// normally served by the fast path above — the pre-created inventory now spans every
	// owned prefix — and reach this check only when a prefix was declared after this process
	// started, which a configuration reload can do. What is still refused is a generation
	// nobody declared: an operator adds it to the allowlist, or drains and re-points the
	// rows. Refusing loses nothing in the meantime, because the row stays claimable and each
	// failure names the topic and the namespaces that were owned.
	if !IsOwnedTopicUnderAnyConfiguredPrefix(topic) {
		owned := OwnedTopicPrefixes()
		logrus.WithFields(logrus.Fields{
			"topic":          topic,
			"owned_prefix":   owned[0],
			"owned_prefixes": owned,
			"owned_topics":   len(p.writers),
			"refusal_reason": "topic is not in the Blnk-owned namespace",
			"remedy": "if this names a namespace this deployment used to own, add it to " +
				"KAFKA_HISTORICAL_TOPIC_PREFIXES so the rows captured under it can drain",
		}).Error("refusing to create a Kafka writer for a topic Blnk does not own")

		return nil, fmt.Errorf("%w: %q is not <prefix>.<category> or <prefix>.<category>.dlt "+
			"for any owned prefix (%s)",
			ErrTopicNotOwned, topic, strings.Join(owned, ", "))
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
				withLoggableCause(logrus.WithField("topic", retiredTopic), err).Warn(
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
// Observability. Four instruments are recorded here. On EVERY call: the attempt counter
// attributed by outcome, and the per-attempt duration histogram attributed by topic,
// attempt and outcome. On SUCCESS only: the broker-acknowledgement counter attributed by
// topic, event type and purpose, and the end-to-end capture-to-dispatch histogram the V-1
// latency target is read from. Recording the per-attempt duration for failures as well as
// successes is deliberate: a broker that times out is exactly the case where latency data
// matters, and the outcome attribute keeps those observations out of the success
// population while the attempt attribute keeps first-attempt latency readable on its own.
//
// What is NOT recorded here is the published-events counter. An acknowledgement is not a
// delivery: delivery is at-least-once, so an event acknowledged by the broker and then
// left unmarked by a dying relay is published again, and a counter incremented here
// counted it twice. The relay increments it once, after the transition that makes the
// delivery durable — see EventRelayProcessor.recordDurableEventPublication.
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

	// THE PRODUCER SPAN, and the point at which a request's trace resumes after the outbox.
	//
	// It is opened before anything else so that every exit below — an unserialisable payload, an
	// oversized envelope, a refused topic, a closed publisher, a broker that will not answer —
	// is inside it. A span that covered only the successful path would be absent from exactly
	// the traces an operator opens.
	//
	// The link, not the parent, is what ties it to the capture: see linkToCapturedTrace for why
	// parenting would misreport the request's duration, attach spans to an exported trace and
	// collapse a coalesced fan-out into one span. traceRequestLinks reads the two carried
	// strings and yields nothing when the event was captured untraced.
	//
	// The span name is bounded by construction. It carries the topic, which is what makes a
	// trace list readable, and the topic is passed through the same bounded label the metrics
	// use — so an event addressed to a topic Blnk does not own cannot mint a new span name.
	ctx, span := tracer.Start(ctx, "publish "+boundedTopicLabel(resolveTopic(req)),
		append(
			traceRequestLinks(req),
			trace.WithSpanKind(trace.SpanKindProducer),
		)...,
	)
	defer span.End()

	// EVERY field is resolved through the same helpers the no-op uses, and the last two
	// are not decoration: they are read by code below and in recordPublishAttempt.
	//
	// MaxAttempts is what lets fail() distinguish "this attempt failed and another will
	// follow" from "this attempt spent the budget", which is the difference between
	// retrying and failed on the attempts counter. Left unset it is always zero, so a
	// terminal failure is reported as retry pressure that no longer exists.
	//
	// Purpose is read twice: it is an attribute of the broker-acknowledgement counter
	// below, which counts every purpose and keeps them separable, and attemptLabel turns
	// a replay or a dead-letter write into its own fixed attempt token. Left unset it is
	// the empty string — which resolvePurpose normalises to PublishPurposeOriginal, so an
	// acknowledgement is never attributed to an empty label and a replay is never given a
	// retry-sequence attempt number it does not belong to.
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
		// These two in particular are load-bearing rather than cosmetic. The PURPOSE selects
		// the ATTEMPT LABEL on both latency histograms — a replay carries the fixed "replay"
		// token rather than a number, because it belongs to no retry sequence — so leaving it
		// at its zero value would file every replay into the first-attempt population the
		// V-1 p99 is read from and lower that quantile with re-deliveries. The BUDGET is
		// what lets fail() tell a failure that still has attempts left from the one that
		// spent the last of them, and it is the "of 5" in the "attempt 3 of 5" that
		// requirement R-4 requires on every attempt.
		MaxAttempts: resolveMaxAttempts(req),
		Purpose:     resolvePurpose(req),
	}

	// THE SEMANTIC MESSAGING ATTRIBUTES, set once from the resolved result rather than from the
	// request, so the span describes what was actually attempted — the resolved topic and key —
	// and not what the caller asked for.
	//
	// Every value is BOUNDED or HASHED. The topic goes through the same bounded label the
	// metrics use, so a span cannot carry an arbitrary destination name; the partition key is
	// hashed, because it is a ledger, balance or identity identifier and a span attribute is
	// rendered into trace viewers and incident tickets the same way a metric label is. The event
	// id is carried in full, matching the capture span in event_outbox.go: it is Blnk-generated,
	// it is the subscriber idempotency key, and it is the value an operator searches by.
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "publish"),
		attribute.String("messaging.destination.name", boundedTopicLabel(result.Topic)),
		attribute.String("messaging.message.id", result.EventID),
		attribute.String("messaging.kafka.message.key_hash", hashLogIdentifier(result.PartitionKey)),
		attribute.String("blnk.event.type", boundedEventTypeLabel(result.EventType)),
		attribute.String("blnk.publish.purpose", string(result.Purpose)),
		attribute.Int("blnk.publish.attempt", result.Attempt),
	)

	// elapsed measures from the outbox claim when the relay supplied that instant, so
	// the histogram reports what its documentation promises: claim to acknowledgement.
	// Otherwise it measures the write alone.
	elapsed := func() time.Duration {
		if req.ClaimedAt.IsZero() {
			return time.Since(started)
		}

		return time.Since(req.ClaimedAt)
	}

	// recordSpanFailure marks the span failed with a BOUNDED class rather than the error text.
	//
	// span.RecordError(err) is deliberately not used. A Kafka or driver error carries broker
	// hostnames, internal addresses and library internals, and a span attribute is exported to
	// whatever backend is configured and rendered into incident tickets — so the raw cause would
	// leave infrastructure detail somewhere it cannot be recalled from. The class plus the
	// transient verdict is what an operator acts on; the full cause stays on the outbox row's
	// last_error and in the publisher's own log line, which are both access-controlled.
	recordSpanFailure := func(cause error, transient bool) {
		span.SetAttributes(attribute.Bool("blnk.publish.transient", transient))
		span.SetStatus(codes.Error, publishSpanErrorClass(cause))
	}

	value, err := resolveEventValue(req)
	if err != nil {
		// A payload that is not valid JSON cannot be spliced into an envelope without
		// producing a message that breaks every subscriber's parser, so this is
		// permanent by construction: no number of retries turns corrupt bytes into
		// valid ones, and the dead-letter topic would reject them for the same reason.
		// The durable record is the outbox row itself, whose last_error names the
		// problem for triage.
		result.Duration = elapsed()
		failed := p.fail(ctx, result, err, false)
		recordSpanFailure(failed.Err, false)

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
		recordSpanFailure(failed.Err, false)

		return failed, failed.Err
	}

	writer, err := p.writerFor(result.Topic)
	if err != nil {
		result.Duration = elapsed()

		// THE TWO WRITER FAILURES ARE CLASSIFIED DIFFERENTLY, and the difference is the
		// event's prospects rather than this attempt's.
		//
		// A CLOSED PUBLISHER is permanent for this INSTANCE and fully recoverable for the
		// EVENT: the process is shutting down, and the next attempt — in this process
		// after a restart, or in another replica right now — has a live transport and will
		// publish it. So it is reported RECOVERABLE. Reporting it permanent was safe only
		// while the relay ignored the verdict and retried everything within budget; now
		// that the relay acts on it, "permanent" here would mean a shutdown landing
		// mid-batch dead-lettered perfectly deliverable events, which is the opposite of
		// what a graceful shutdown must do. IsBrokerUnavailableError already answers true
		// for this error at the API boundary, so this brings the two into agreement.
		//
		// A REFUSED TOPIC (ErrTopicNotOwned) is permanent for the event itself: the
		// destination is not one Blnk may write to, so no retry and no broker state can
		// make the write legitimate, and the row belongs in the dead-letter inventory
		// where an operator can see it now rather than in five attempts' time.
		transient := errors.Is(err, ErrEventPublisherClosed)
		failed := p.fail(ctx, result, err, transient)
		recordSpanFailure(failed.Err, transient)

		return failed, failed.Err
	}

	// The carrier the broker's coordinate comes back on. It rides with the message through
	// WriterData, which never reaches the wire, and is filled by completeWrite.
	acknowledgement := &publishAcknowledgement{}

	message := kafka.Message{
		// Topic is intentionally left empty. kafka-go rejects a message whose topic is
		// set when the writer already has one, and the writer here is per-topic.
		Key:   partitionKeyBytes(result.PartitionKey),
		Value: value,
		Time:  req.Event.OccurredAt,
		// THE TRACE TRAVELS AS RECORD HEADERS, and never in the value.
		//
		// Headers are where the OpenTelemetry messaging conventions put trace context, and
		// keeping it out of the value is what preserves the byte-equality guarantees: dual
		// delivery (V-8) compares the Kafka payload against the legacy webhook body, and a
		// replay (V-9) compares against the stored envelope. A trace member inside the
		// envelope would differ between the original publish and its replay by construction,
		// so both comparisons would fail on telemetry rather than on anything that matters.
		//
		// Injected from the PRODUCER SPAN's context, not from the captured one: a subscriber
		// that continues this trace should attach to the publish it consumed, and the publish
		// is in turn linked to the capture — so the whole chain is reachable while each span
		// has the parent that reflects real causality.
		Headers:    kafkaTraceHeaders(ctx),
		WriterData: acknowledgement,
	}

	if err := writer.WriteMessages(ctx, message); err != nil {
		result.Duration = elapsed()
		transient := classifyTransientPublishError(err)
		failed := p.fail(ctx, result, err, transient)
		recordSpanFailure(failed.Err, transient)

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
		// The coordinate on the span is what turns "this event was published" into "this event
		// is THAT record" for someone reading a trace rather than the outbox row. Both values
		// are broker-assigned integers, so neither is caller data and neither needs bounding.
		span.SetAttributes(
			attribute.Int("messaging.kafka.destination.partition", record.Partition),
			attribute.Int64("messaging.kafka.message.offset", record.Offset),
		)
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

	// EventsPublishedTotal IS NOT INCREMENTED HERE, and its absence is the point (OBS-05).
	//
	// This line is the BROKER ACKNOWLEDGEMENT, which is one step short of the delivery being
	// recorded. Delivery is at-least-once by construction: the relay publishes, then marks the
	// outbox row dispatched, and a crash or a failed bookkeeping statement between the two
	// deliberately leaves the row claimable so the event is published AGAIN — losing it is
	// unrecoverable while a duplicate is suppressed at the subscriber on event_id. Counted
	// here, that second publish increments the counter a second time for ONE event, so a
	// counter documented as "one per event" silently becomes "one per successful write"
	// exactly when the pipeline is having trouble. It is the denominator of the dead-letter
	// rate acceptance criterion V-3 is stated over, so over-counting it understates that rate
	// precisely during an incident.
	//
	// The increment lives on the DURABLE TRANSITION instead, in event_relay.go: the
	// claim-token-conditional statement that records the Kafka leg succeeds for one worker
	// once per event, which is what makes the increment unique. See
	// recordDurableEventDelivery and metrics.EventsPublishedTotal's own declaration, which
	// documents that contract. The acknowledgement is counted on
	// metrics.EventBrokerAcknowledgementsTotal below, so nothing about the wire is lost.
	//
	// The ACK itself is not lost from the telemetry: it is counted RIGHT HERE, on its own
	// instrument, and recordPublishAttempt below additionally counts this attempt under
	// outcome="dispatched" and records both latency histograms. So
	// acknowledged-writes-per-second remains readable and is simply no longer conflated with
	// unique delivered events.
	//
	// EVERY purpose is counted here, unlike EventsPublishedTotal: this measures traffic the
	// broker accepted, so a replay and a dead-letter write are both real acknowledgements. The
	// purpose attribute keeps them separable, and its domain is PublishPurpose's — fixed and
	// small — so it adds no cardinality risk. The gap between this counter and
	// EventsPublishedTotal is the leading indicator of duplicate delivery: acks running ahead
	// of durable deliveries means rows are not being marked and will be republished.
	metrics.EventBrokerAcknowledgementsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(result.EventType)),
		attribute.String(publishAttrPurpose, string(result.Purpose)),
	))

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
//   - PublishResult: the completed result, with Status retrying, Terminal set from the
//     classification, and Err populated.
func (p *kafkaPublisher) fail(ctx context.Context, result PublishResult, cause error, transient bool) PublishResult {
	// EVERY FAILED ATTEMPT REPORTS RETRYING, and the terminal distinction is carried on
	// the result's classification fields instead of on its status.
	//
	// Requirement R-3 fixes the status vocabulary at dispatched, retrying and
	// dead-lettered, and that vocabulary is a PUBLISHED METRIC LABEL DOMAIN: the
	// instrument documentation, the alert rules, the load harness and every operator
	// dashboard select on those three values. A fourth value ("failed") briefly lived here
	// to express "this attempt failed and nothing further will be tried"; it was removed
	// because widening the domain silently changes what every existing selection matches,
	// and because the distinction does not need to be a label to be made.
	//
	// So the three facts are recorded where a consumer of them actually looks:
	//
	//   - Retryable — another attempt is possible: the failure looked transient AND the
	//     attempt did not spend the budget the row stated.
	//   - Transient — the classification itself, from classifyTransientPublishError.
	//   - Classified — that a classification was reached at all, which is what stops a
	//     bare error from an unclassifying implementation reading as permanent. This is
	//     the ONLY place it is set.
	//
	// PermanentFailure() combines them and is what the relay reads; nothing reads the
	// status to decide an event's fate.
	//
	// model.PublishStatusDeadLettered is likewise NOT reported here, and for a reason that
	// outlasts the vocabulary question: dead-lettering is an acknowledged WRITE to a
	// `<topic>.dlt` sibling, which is a publish this function never performs. Reporting it
	// here claimed a preservation that had not occurred, and sometimes never would — an
	// event whose destination lies outside the topic catalogue has no `.dlt` sibling to be
	// written to, so the row ended failed with no dead letter anywhere while the
	// dead-letter counter reported one per attempt. Only the dead-letter writer knows
	// whether that write happened, so only it may declare it: see
	// PublishResult.DeadLettered and event_dlt.go.
	result.Retryable = transient && !attemptBudgetSpent(result.Attempt, result.MaxAttempts)
	result.Status = model.PublishStatusRetrying

	result.Transient = transient
	result.Classified = true

	// TERMINAL IS THE COMPLEMENT OF RETRYABLE ON A FAILURE, and it is set here because this is
	// the only place that knows both halves of it: the classification, and whether the attempt
	// spent the budget the row stated. Both of Terminal's documented cases fall out of the one
	// expression — a non-transient failure is not retryable, and neither is a transient one on
	// the last permitted attempt.
	//
	// It is affirmative rather than inferred, which is the safety property the field exists for:
	// Classified is true on this line, so nothing an unclassifying publisher returns can arrive
	// here and read as terminal.
	//
	// Leaving it unassigned was not harmless. The field is read by the `terminal` metric
	// attribute and by the publisher's own log fields, so every failed attempt — permanent ones
	// and the budget-spending one included — was reported as terminal=false, which made
	// {outcome="retrying",terminal="true"} an empty series and therefore a silent answer to
	// "which events are stuck". The relay's retry decision reads PermanentFailure rather than
	// this field, so the routing was correct throughout; what was wrong was everything an
	// operator could see.
	result.Terminal = !result.Retryable
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
// Requirement R-6 partitions by LEDGER ID, and that is what this function keys on. It composes
// with the database's ordering work because the claim serialises on THE SAME rule rather than
// on a column: ClaimPendingEventOutbox admits at most ONE row per EFFECTIVE key in flight —
// the purpose of the NOT EXISTS anti-join in claimPendingEventOutboxQuery and of the
// idx_event_outbox_effective_key_inflight expression index behind it — and both sides of that
// predicate are rendered from eventOutboxEffectiveKeySQL, which is model.EffectivePartitionKey
// in SQL. The value the database serialises on is therefore the value Kafka partitions on by
// construction, and the ordering guarantee reaches the subscriber intact even with several
// relay replicas running.
//
// Serialising on the partition_key COLUMN instead was the previous shape and it was unsound
// (PERF-C02): the two values agree only wherever a ledger reached the row's partition key, so
// two same-ledger rows with divergent stored keys went unserialised while both hashed to one
// Kafka partition — two replicas could then append them in either order.
//
// Populating the ledger AT CAPTURE TIME is still not optional, for a different reason: it is
// what gives whole-ledger affinity in the first place. Transaction payloads carry no ledger
// field, so the producer must state it: transaction execution takes it from the source balance
// it has already loaded, and the ledger and balance hooks hold the entity itself. Without
// that, ledger_id is NULL, the key falls back to partition_key, and events of one ledger
// spread across partitions — which is exactly the failure model
// model.EventOutbox.PartitionKey's own documentation warns about, in the other direction.
//
// Events that genuinely have NO ledger keep their partition-key affinity through the fallback:
// identities key on the identity, bulk batches on the batch, system errors on the event type,
// and a rejected transaction whose balances were never loaded on its own source, destination or
// id. Each is stable per aggregate, so per-aggregate ordering holds for all of them. Balance
// MONITORS are not in that list: checkBalanceMonitors supplies the ledger of the balance whose
// update fired the condition, so a monitor event is ledger-keyed like the balance event beside
// it.
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
// identities, bulk batches, system errors and rejected transactions. It is NOT NULL in the schema,
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
		Event: row.CanonicalEvent(),
		// THE STORED BYTES, carried so the publish does not re-serialise the envelope. A row
		// that carries none — one written before the column existed — leaves this empty and
		// the publisher composes, which is the documented fallback rather than a second
		// source of truth. See PublishRequest.Raw.
		Raw:   row.EventRaw,
		Topic: row.Topic,
		// LEDGER FIRST — requirement R-6 partitions by ledger ID. See the fallback chain above.
		//
		// Resolved through model.EffectivePartitionKey rather than inline, because the API
		// REPORTS this key on the dead-letter listing and an operator answers ordering questions
		// from what it says. Two copies of the fallback chain is how the reported key and the
		// routed key start disagreeing on exactly the rows where they matter.
		Key:     row.EffectiveKey(),
		Attempt: attempt,
		// THE CAPTURED TRACE, carried from the row so the publish span links to the request
		// that produced the event. This conversion is used by the relay, the dead-letter write
		// and the replay alike, so all three inherit the link from one place — which is the
		// same reason the canonical bytes and the topic are resolved here rather than at each
		// call site.
		Traceparent: row.Traceparent,
		Tracestate:  row.Tracestate,
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
	return event.CanonicalBytes()
}

// resolveEventValue returns the bytes to put on the topic for one request.
//
// THE STORED VALUE WINS, ALWAYS. blnk.event_outbox.event_raw is the envelope the broker was
// given at capture, so publishing it means a retry, a dead-letter copy and a replay all carry
// the identical bytes — which is requirement R-5's byte fidelity expressed as data rather than
// as a hope that the serialiser never changes.
//
// Composition is the fallback for the two requests that legitimately have no stored value: the
// mandated envelope-only Publish, whose event never passed through the outbox, and a row
// written before the column existed. Both get the bytes this version would have stored, and
// both are still validated, because splicing bytes that are not JSON would produce a message
// that breaks every subscriber's parser.
//
// A stored value is NOT re-validated. It was validated at the persistence boundary, it is the
// authority, and re-parsing it on every attempt would be a per-publish cost for a check that
// cannot change its answer.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - []byte: the message value.
//   - error: only from composition, when the payload is not valid JSON.
func resolveEventValue(req PublishRequest) ([]byte, error) {
	if len(bytes.TrimSpace(req.Raw)) > 0 {
		return req.Raw, nil
	}

	return marshalLedgerEvent(req.Event)
}

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
// Five sources of truth are consulted, in the order a failure is most likely to arrive:
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
//   - brokerOutageSignature, the broker-outage shapes none of the four rules above
//     recognises: BrokerNotAvailable, ReplicaNotAvailable, and a *net.OpError or
//     *net.DNSError whose cause is outside the small errno set named above.
//
// # RETRY-01: why the fifth rule is here and not only in brokerUnavailable
//
// It used to be only there, and a DNS failure was the consequence. A broker whose NAME stops
// resolving — the ordinary shape of a container or pod replacement, a Service recreation or a
// resolver restart — surfaces as a *net.DNSError, which carries no errno, is not Temporary
// and is not a Timeout, so all four rules above reported it permanent. The relay then
// dead-lettered the event IMMEDIATELY with its whole budget unspent, on a condition that
// clears by itself in seconds, while the very next log line classified the same error
// failure_class=broker_unavailable. A rolling restart converted every event in flight into a
// manual replay and a DeadLetterMessageStuck page.
//
// The two verdicts share that fifth rule as ONE leaf, which is what keeps them from drifting
// apart again: every failure brokerUnavailable reports is a failure this function reports, so
// the invariant a failure classified broker_unavailable is never terminal on an unspent budget
// holds by construction rather than by two functions being maintained in step.
//
// # TAXONOMY-01: the retryable set is WIDER than the outage set, and stays that way
//
// The two are not the same question, and this one answers the wider of them. Context expiry
// above is the case that separates them: a write abandoned by a cancelled context or a spent
// budget IS worth attempting again, and it is NOT evidence of anything about the broker — so it
// is transient here and not an outage in brokerUnavailable. Collapsing the two verdicts into
// this function is what once let a shutdown be reported to an operator as a broker failure.
//
// Anything unrecognised is still reported as permanent. That is the conservative direction
// for an unknown failure, because an unbounded retry of something that can never succeed is
// worse than a dead-letter entry an operator can see and replay — and it is why this widening
// names concrete signatures rather than reaching for the net.Error interface, which
// kafka.Error also satisfies.
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

	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) {
		return true
	}

	return brokerOutageSignature(err)
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
	// THE TERMINAL DIMENSION ACCOMPANIES THE OUTCOME, and it has to. The outcome
	// vocabulary is frozen at three values, so a failed attempt that will never be retried
	// and one that will are both outcome="retrying"; without this attribute the counter
	// could no longer answer "how many events are actually stuck", which is the question the
	// dead-letter triage runbook opens with. Its domain is closed at two literals, so it
	// multiplies the series count by two and nothing more.
	metrics.EventPublishAttemptsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrOutcome, string(result.Status)),
		attribute.String(publishAttrTerminal, terminalAttributeValue(result)),
	))

	// attemptLabel and not strconv: the attribute sits on a HISTOGRAM, so its cardinality
	// is multiplied by the bucket count and the domain has to stay closed at its eight
	// declared values. See attemptLabel for the three inputs that would otherwise widen it.
	//
	// The OUTCOME accompanies the attempt because this instrument's usable population is
	// first-attempt SUCCESSFUL writes, which its declaration in internal/metrics spells out
	// as {attempt="1",outcome="dispatched"}: that is the broker write in isolation, and the
	// second term of the queue-wait split. Recording the attempt alone would leave that
	// query matching nothing at all — while every dashboard still looked populated, because
	// the series exist under a shorter label set.
	//
	// It is NOT the series acceptance criterion V-1 is read from. V-1 spans outbox capture
	// to acknowledgement and is read from EventCaptureToDispatchDuration, recorded below;
	// this clock starts at the claim and so omits the queue wait. Its domain is the same
	// closed model.PublishStatus vocabulary the attempts counter uses, so it adds no
	// unbounded dimension.
	metrics.EventPublishDuration.Record(ctx, result.Duration.Seconds(), otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrAttempt, attemptLabel(result.Purpose, result.Attempt)),
		attribute.String(publishAttrOutcome, string(result.Status)),
	))

	recordCaptureToDispatch(ctx, result)
}

// recordDurableEventDelivery increments EventsPublishedTotal for ONE event whose delivery is
// now DURABLY RECORDED: the broker acknowledged the write AND the claim-token-conditional
// statement that records the Kafka leg has committed.
//
// # Why the relay calls this and the publisher does not (OBS-05)
//
// The increment used to sit immediately after the broker acknowledgement. That reads as the
// natural place and is the wrong one, because acknowledgement is not the last step:
//
//	publish → ack → mark the row's Kafka leg recorded
//
// Delivery is at-least-once by design. A crash, a cancelled context or a failed statement
// between the second and third steps leaves the row claimable so the event is published
// AGAIN — losing it is unrecoverable, a duplicate is suppressed at the subscriber on event_id.
// Counted at the ack, that republish increments the counter a SECOND TIME for one event. The
// counter is documented as one increment per delivered event and is the denominator acceptance
// criterion V-3's dead-letter rate is stated over, so over-counting it understates the rate
// exactly when the pipeline is struggling and the rate is being read.
//
// The transitions that record the Kafka leg — MarkEventDispatched and MarkEventWebhookPending —
// are both conditional on the claim token and both clear or consume it, so each succeeds for
// one worker once per event. Counting there is what makes the increment unique. The cost is
// that this counter LAGS THE WIRE by one bookkeeping statement, and an event on the topic whose
// row could not be marked is not counted until the republish completes. That is the correct
// trade: the alternative over-counts, and the acknowledged-write rate remains readable on
// EventPublishAttemptsTotal{outcome="dispatched"}.
//
// # It counts ORIGINAL first deliveries only
//
// The relay is the only caller and it publishes only originals, so replays and dead-letter
// writes cannot reach here. That is deliberate rather than incidental: both are visible on the
// per-attempt instruments under their own fixed attempt attribute, and counting them here would
// make the dead-letter rate depend on how much triage happened that day.
//
// It lives in this file, beside the other metrics writes for the publish pipeline, so that
// event_relay.go still imports no instrument package of its own — the collector owns the gauges
// and a second writer would make them disagree between ticks.
//
// Parameters:
//   - ctx context.Context: the recording context. A cancelled one is harmless; the OpenTelemetry
//     synchronous counter does not block on it.
//   - topic string: the CATEGORY topic the event was published to. Bounded to the closed topic
//     vocabulary here, so a row carrying an unexpected destination cannot mint a label value.
//   - eventType string: the event name, bounded to the closed event-type vocabulary.
func recordDurableEventDelivery(ctx context.Context, topic, eventType string) {
	if metrics.EventsPublishedTotal == nil {
		// Observability is optional, and this is the one metric write on the relay's
		// BOOKKEEPING path rather than on a publish path. Every other writer in this file
		// runs after a publish that already required the instruments; this one runs after
		// the row has already been moved, so a nil instrument must not take a settled
		// event's bookkeeping down with it.
		return
	}

	metrics.EventsPublishedTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(topic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(eventType)),
	))
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
// # DATA-02: the client's message is a dependency's own words, so it is redacted at a
// normal level
//
// kafka-go writes its failures for a developer at a terminal: "kafka.(*Client).Produce: dial
// tcp 10.0.3.14:9092: connect: connection refused" and "dial tcp: lookup kafka on
// 127.0.0.11:53: no such host" are both verbatim renderings of what it tried and where. This
// hook is installed at ERROR level, so forwarding the string unchanged put the broker's
// address, its port and the internal resolver's address into every deployment's log at the
// default level — the same disclosure the event pipeline redacts everywhere it renders an
// error itself, arriving through the one path that was not this codebase's own text.
//
// So a line at Warn or worse is redacted through redactLogValue, which removes
// address-shaped tokens and secret values and keeps the prose: the diagnosis an operator
// acts on survives, and the topic that failed is already carried as its own field. The
// verbatim text is not lost — when the standard logger is at debug it is attached as
// message_verbatim, exactly the asymmetry withLoggableCause applies to an error, so the
// detail a broker investigation needs stays one explicit, auditable act away.
//
// The DEBUG hook is forwarded verbatim and deliberately so: it is only ever emitted when a
// deployment has asked for debug, which is the same consent gate cause_verbatim sits behind.
//
// Parameters:
//   - format string: the format string kafka-go supplies.
//   - args ...interface{}: its arguments.
func (l kafkaLogger) Printf(format string, args ...interface{}) {
	entry := logrus.WithFields(logrus.Fields{
		"component": "kafka-writer",
		"topic":     l.topic,
	})

	// logrus orders its levels from Panic (0) to Trace (6), so "at Warn or worse" is a
	// numeric comparison against WarnLevel. Anything below it — debug and trace — is already
	// gated on a deployment having asked for that detail.
	if l.level > logrus.WarnLevel {
		entry.Logf(l.level, format, args...)

		return
	}

	message := fmt.Sprintf(format, args...)

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		entry = entry.WithField("message_verbatim", sanitizeLogValue(message, maxLoggedErrorLength))
	}

	entry.Log(l.level, redactLogValue(message, maxLoggedErrorLength))
}
