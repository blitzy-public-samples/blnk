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

// This file is the publishing layer of the Kafka event pipeline. It answers exactly one
// question — how does a model.LedgerEvent get onto a Kafka topic and what happened when
// it did — and it owns nothing else:
//
//   - The EventPublisher contract every producer of events depends on.
//   - The Kafka-backed implementation, holding one writer per topic.
//   - The no-op implementation selected when no brokers are configured.
//   - PublishResult, the observability record of a single attempt.

// Writer and transport tuning. Every value below is set EXPLICITLY on every writer
// rather than left to kafka-go's defaults, and each one is load-bearing: three of the
// library defaults are actively wrong for this pipeline, and relying on the remaining
// ones would let a library upgrade change ledger-event durability without a code
// change.
const (
	// eventWriterRequiredAcks makes the broker acknowledge a write only once every in-sync
	// replica has it, which is the strongest durability setting Kafka offers and the
	// at-least-once guarantee this pipeline is built on.
	eventWriterRequiredAcks = kafka.RequireAll

	// eventWriterMaxAttempts is 1, which disables the writer's own retry loop.
	eventWriterMaxAttempts = 1

	// eventWriterBatchSize is 1 so a synchronous write flushes immediately.
	eventWriterBatchSize = 1

	// eventWriterBatchTimeout bounds how long a partially-filled batch waits. It is
	// unreachable while eventWriterBatchSize is 1 — every message completes a batch on
	// arrival — and is pinned to a small value anyway so that raising the batch size later
	// cannot silently reintroduce kafka-go's one-second default flush delay.
	eventWriterBatchTimeout = 10 * time.Millisecond

	// eventWriterWriteTimeout bounds the produce round trip. It matters more than it
	// looks: kafka-go bounds the produce call with its own background context and this
	// timeout, NOT with the context passed to WriteMessages, so this value — not the
	// caller's deadline — is what stops a single attempt hanging on an unresponsive
	// broker. It matches kafka-go's own default, so behaviour is pinned rather than
	// changed.
	eventWriterWriteTimeout = 10 * time.Second

	// eventWriterReadTimeout bounds the metadata read that resolves a topic's partition
	// count before balancing. Unlike the produce call, that read does honour the caller's
	// context, so this is a ceiling rather than the only bound.
	eventWriterReadTimeout = 10 * time.Second

	// eventWriterAsync is false: WriteMessages must block until the broker acknowledges.
	// An asynchronous writer returns nil the moment a message is queued, so every publish
	// would look successful, every outbox row would be marked dispatched, and a broker
	// outage would be invisible until subscribers reported missing events. Set explicitly
	// for the same reason as the acks.
	eventWriterAsync = false

	// eventWriterBatchBytes is the writer's own byte ceiling, and it is set EXPLICITLY
	// rather than left to kafka-go's 1 MiB default.
	eventWriterBatchBytes = 1000000

	// eventWriterAllowAutoTopicCreation is false so that writing to a topic which does not
	// exist FAILS instead of creating one.
	eventWriterAllowAutoTopicCreation = false

	// eventTransportDialTimeout bounds establishing a broker connection, including the
	// SASL handshake.
	eventTransportDialTimeout = 5 * time.Second

	// eventTransportIdleTimeout is how long an unused broker connection is kept in the
	// pool. It matches the 90 seconds the shared HTTP client uses for the legacy webhook
	// transport, so both transports age connections alike.
	eventTransportIdleTimeout = 90 * time.Second

	// eventTransportClientID identifies this producer to the broker. It surfaces in broker
	// logs, request metrics and quota accounting, which is what makes a misbehaving
	// producer attributable to Blnk rather than anonymous.
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

	// publishAttrPurpose separates an original publish from a replay and from a
	// dead-letter write on EventBrokerAcknowledgementsTotal. The value set is
	// PublishPurpose's, which is fixed and small, so it bounds cardinality by
	// construction.
	publishAttrPurpose = "purpose"

	// publishAttrTerminal reports whether a failed attempt was the LAST one for its event.
	// It is a separate dimension rather than a fourth `outcome` value because the
	// publish-outcome vocabulary is a frozen three-value contract — dispatched, retrying,
	// dead_lettered — and "nothing further will be tried" is a property of the attempt,
	// not a different kind of outcome.
	publishAttrTerminal = "terminal"
)

// The closed domain of publishAttrTerminal, spelled as constants so the publisher, the
// dead-letter writer, the tests and docs/metrics.md cannot disagree about the literals.
const (
	publishTerminalTrue  = "true"
	publishTerminalFalse = "false"
)

// terminalAttributeValue renders a result's terminal classification as its metric
// label.
//
// Parameters:
//   - result PublishResult: the completed attempt.
//
// Returns:
//   - string: publishTerminalTrue when no further attempt will be made, otherwise
//     publishTerminalFalse.
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
// Parameters:
//   - topic string: the topic name to report.
//
// Returns:
//   - string: the topic verbatim when Blnk owns it, unownedTopicLabel otherwise.
func boundedTopicLabel(topic string) string {
	// Every owned prefix, not only the configured one. A row captured before a prefix
	// rename still names the previous generation's topic and is still published, so
	// collapsing its name to the "unowned" label would report a legitimate delivery as an
	// anomaly — and would hide, behind one shared series, exactly the traffic an operator
	// draining that generation needs to watch. The set stays bounded because the allowlist
	// is bounded by config.MaxHistoricalTopicPrefixes: at most (1 + 4) prefixes times the
	// four categories times the optional `.dlt` suffix.
	if IsOwnedTopicUnderAnyConfiguredPrefix(topic) {
		return topic
	}

	return unownedTopicLabel
}

// boundedEventTypeLabel renders an event type as a metric label with BOUNDED
// cardinality.
//
// Collapsing does not hide the condition. A non-zero count on the unrecognised label is
// the signal that something is publishing an uncatalogued event, and the publisher's
// warning names the specific event.
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

// EventTransportErrorDetail is the detail attached to a typed API error whose cause
// came from the Kafka client rather than from Blnk.
//
// The full cause is not discarded, only redirected. It is logged, with the error
// attached, at the site that builds this detail, so an operator retains the broker's
// exact words while the caller receives the diagnosis and nothing else.
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
// The cause is used for exactly one thing — the transient classification — so the
// returned value cannot contain any of its text or structure however the caller renders
// it: as JSON, with %v, or field by field.
//
// Parameters:
//   - reason string: fixed wording describing what failed. Supply a literal, never a
//     formatted string containing the cause.
//   - eventID, eventType, topic string: the event's identifiers and destination.
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
var ErrKafkaProducerCredentialsRequired = errors.New(
	"blnk: a dedicated Kafka producer principal is required; set KAFKA_SASL_USER and KAFKA_SASL_SECRET",
)

// EventPublisher publishes a canonical ledger event.
//
// The signature is fixed by the deployment contract and is reproduced here verbatim. It
// takes a context and a model.LedgerEvent and returns an error, and it must not grow
// parameters, return a tuple, or be renamed.
type EventPublisher interface {
	Publish(ctx context.Context, event model.LedgerEvent) error
}

// TopicEventPublisher is the full publisher contract used inside Blnk: the mandated
// Publish, the explicit result-returning form the relay and the dead-letter writer
// need, and lifecycle.
type TopicEventPublisher interface {
	EventPublisher

	// PublishToTopic publishes one event and reports what happened. The returned error is
	// always identical to the Err field of the returned result, so a caller may branch on
	// either.
	PublishToTopic(ctx context.Context, req PublishRequest) (PublishResult, error)

	// Close flushes and releases every resource the publisher holds. It is
	// idempotent and safe on the no-op.
	Close() error
}

// PublishRequest is the explicit form of a publish: the event, where it goes, how it is
// keyed, and which attempt this is.
//
// Every field except Event is optional and has a documented fallback, so the zero value
// plus an Event is a valid request — that is exactly what the mandated Publish method
// submits.
type PublishRequest struct {
	// Event is the envelope to publish. Its Payload bytes are written through
	// untransformed; see marshalLedgerEvent.
	Event model.LedgerEvent

	// Raw is THE STORED CANONICAL ENVELOPE, and when present it is written to the topic
	// VERBATIM.
	Raw []byte

	// Topic is the fully-resolved destination. When empty it is derived with TopicForEvent
	// from the event type, which is the correct behaviour for an event that never passed
	// through the outbox. The relay always sets it explicitly from the topic recorded on
	// the outbox row, and the dead-letter writer sets it to the `<topic>.dlt` sibling.
	Topic string

	// Key is the Kafka message key, which is the outbox row's PARTITION KEY —
	// model.EventOutbox.PartitionKey, the value the row was stored with and the value
	// ClaimPendingEventOutbox serialises dispatch on. Keying with a stable hash balancer
	// pins every event sharing a key to a single partition, and that partition affinity is
	// what makes per-aggregate ordering hold: Kafka orders within a partition only.
	Key string

	// Attempt is the 1-based attempt number, used only as a metric attribute. Any value
	// below 1 is treated as 1. The publisher never interprets it as a budget: it does not
	// compare it against a maximum and does not change behaviour as it grows, because
	// retry is the relay's responsibility alone.
	Attempt int

	// MaxAttempts is the retry budget the row states, and it is what lets a single attempt
	// report whether it was the LAST one.
	MaxAttempts int

	// Purpose says which of the three publish paths this is, and it exists to keep three
	// different populations out of each other's telemetry. The zero value is
	// PublishPurposeOriginal, so an unstated purpose is a first delivery — which is what
	// the mandated envelope-only Publish method submits. See PublishPurpose.
	Purpose PublishPurpose

	// ClaimedAt is when the relay claimed the outbox row, and it exists so the
	// publish-duration histogram measures what its documentation says it measures: claim
	// to broker acknowledgement, not just the time inside WriteMessages. Leave it zero —
	// as the envelope-only path does — and the duration covers the write alone.
	ClaimedAt time.Time

	// Traceparent and Tracestate are the W3C trace context recorded on the outbox row at
	// capture, carried here so the publish span can LINK back to the request that produced
	// the event.
	Traceparent string
	Tracestate  string
}

// PublishResult is the observability record of ONE publish attempt. It exists because
// an error alone cannot answer the questions operations asks — which topic, which
// event, which attempt, how long, and is this worth retrying — and because the metrics
// layer is keyed on the outcome vocabulary it carries.
type PublishResult struct {
	// Status is the outcome of this attempt, and the value the publish-attempts
	// counter is attributed by.
	Status model.PublishStatus

	// EventID is the event's UUID, which is also the subscriber idempotency key. It is
	// carried here so a log line about a failed attempt names the event an operator has to
	// go and look at.
	EventID string

	// EventType is the event name, and the value the broker-acknowledgement counter is
	// attributed by alongside the topic. The relay attributes the durable published-events
	// counter by the same pair, read off the outbox row, so the two are directly
	// comparable.
	EventType string

	// Topic is the resolved destination this attempt targeted, after the fallback in
	// PublishRequest.Topic was applied.
	Topic string

	// PartitionKey is the key the message was written with, after the fallback in
	// resolvePartitionKey was applied. For an outbox-backed publish it is the row's stored
	// partition key, which is the same value ClaimPendingEventOutbox serialised dispatch
	// on. An empty value means the message was written without a key and was therefore
	// balanced across partitions rather than pinned to one — correct for an event that
	// belongs to no ledger and no aggregate, and a red flag for anything else.
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

	// Terminal states that this attempt failed and NO FURTHER ATTEMPT WILL BE MADE for the
	// event: the failure is permanent, or it was the last attempt the stated budget
	// allowed. It is false on every success and false on a failure another attempt may
	// recover from.
	Terminal bool

	// Duration is how long the attempt took: from PublishRequest.ClaimedAt when it
	// was set, otherwise the time spent in the write itself.
	Duration time.Duration

	// CapturedAt is the instant the event was CAPTURED in the transactional outbox, copied
	// from the envelope's OccurredAt.
	CapturedAt time.Time

	// Record is WHERE the broker put this event: the topic, partition and offset it
	// assigned. Confirmed only on a successful attempt, and the zero value on every
	// failure — a write that was not acknowledged produced no record to name.
	Record model.BrokerRecord

	// Transient reports whether the failure looks recoverable — a broker that is down, a
	// leader election in flight, a timeout — as opposed to permanent, such as a message
	// that exceeds the topic's size limit or a principal that is not authorised. It is
	// meaningful only when Err is non-nil and Classified is true, and it is advice for the
	// relay's retry decision, never a decision taken here.
	Transient bool

	// Classified states that a publisher actually REACHED A VERDICT about Transient, as
	// opposed to leaving it at its zero value.
	Classified bool

	// Err is the failure, or nil on success. It is the same value PublishToTopic returns
	// as its error, and it wraps the underlying broker or library error so errors.Is and
	// errors.As reach it.
	Err error
}

// Dispatched reports whether the attempt succeeded, meaning the broker acknowledged the
// write under the all-replicas acknowledgement setting. It reads the status rather than
// testing Err for nil so that there is one definition of success.
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
// Returns:
//   - bool: true when no further attempt can succeed, so the relay must take the row to
//     its terminal state now instead of scheduling a retry.
func (r PublishResult) PermanentFailure() bool {
	return r.Err != nil && r.Classified && !r.Transient
}

// DeadLettered returns a copy of the result with its status set to dead-lettered.
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
// searchable at all.
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
		// log is read by a wider audience than the deployment's operators.
		fields["error"] = loggableCause(r.Err)
		fields["transient"] = r.Transient
		// transient says what the failure LOOKED like; retryable says whether anything
		// further will actually be tried. They differ on the last permitted attempt, and that
		// is the case an operator most needs to be able to see.
		fields["retryable"] = r.Retryable
		// terminal is what the STATUS cannot carry. The publish-outcome vocabulary is frozen
		// at three values, so every failure logs status=retrying; this field is how "and
		// nothing further will be tried" reaches the log at all, and it is the field to grep
		// for when the question is "which events are actually stuck".
		fields["terminal"] = r.Terminal
	}

	return fields
}

// PublishError is the error a failed publish returns. It carries the same context
// PublishResult does, so the retry decision survives the narrowing to a bare error that
// the mandated Publish signature forces.
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
// topic, which attempt, whether a retry is worth attempting, and the underlying reason.
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
// Parameters:
//   - err error: the error returned by Publish or PublishToTopic. May be nil.
//
// Returns:
//   - bool: true only when err is (or wraps) a PublishError that was classified as
//     non-transient.
func IsPermanentPublishError(err error) bool {
	var publishErr *PublishError
	if errors.As(err, &publishErr) {
		return !publishErr.Transient
	}

	return false
}

// IsBrokerUnavailableError reports whether a failed publish is the BROKER being unable
// to accept the write, as opposed to a defect in the event or in this service.
//
// The verdict is taken from the first of these that applies:
//
//  1. ErrEventPublisherClosed. A closed publisher has no transport at all, which is the
//     same operator situation as no broker being configured: nothing can be published
//     from this process now, the event stays safe in its outbox row, and the honest
//     answer is "unavailable, try again" rather than "internal error".
//  2. A PublishError's OWN classification, with one correction: a permanent failure is
//     never an outage and is taken verbatim, while a TRANSIENT failure is an outage
//     unless its only cause is this process giving up — a write abandoned to shutdown, a
//     lost outbox lease or an expired request budget is worth another attempt and says
//     nothing about Kafka.
//  3. The raw error, for a failure that reached the caller unwrapped — a borrowed
//     writer's WriteMessages error, or a double reporting the broker's own error code —
//     classified by brokerUnavailable.
//
// Parameters:
//   - err error: the error returned by Publish, PublishToTopic or a raw writer.
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

		// The publisher's transient verdict is kept for every cause EXCEPT a bare
		// cancellation or expiry, which is this process's own decision rather than the
		// broker's condition.
		return !localContextTermination(publishErr)
	}

	return brokerUnavailable(err)
}

// localContextTermination reports that a failure is THIS PROCESS giving up — a
// cancelled context, a spent deadline, a lost lease, a shutdown — rather than anything
// observed about the broker.
//
// Parameters:
//   - err error: the failure. A nil is reported false.
//
// Returns:
//   - bool: true only for a context termination with no broker or network signature at
//     all.
func localContextTermination(err error) bool {
	if err == nil || brokerOutageSignature(err) {
		return false
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// brokerUnavailable classifies a RAW publish failure — one that has not been through
// kafkaPublisher.fail and so carries no verdict of its own.
//
// "Will another attempt help?" is classifyTransientPublishError. "Is the BROKER the
// reason?" is this.
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

// brokerOutageSignature recognises the failures that are EVIDENCE OF THE BROKER, as
// opposed to evidence of this process, this event, or this caller's deadline.
//
// It is the leaf brokerUnavailable is written in terms of, and it is deliberately a
// closed list of concrete signatures rather than a reach for an interface:
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
//     all, so an errno-only test cannot see it.
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

// NoopEventPublisher is the publisher selected when no Kafka brokers are configured. It
// accepts every event, does nothing with it, and reports success.
//
// It satisfies the whole TopicEventPublisher contract, not just the mandated method, so
// a relay or dead-letter writer holding one needs no special case.
type NoopEventPublisher struct{}

// NewNoopEventPublisher returns the no-op publisher.
//
// It is exported so that tests and callers which must be explicit about wanting no
// publishing — rather than relying on configuration being absent — can say so. The type
// carries no state, so the returned value is safe to share across goroutines and cheap
// to create.
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
type kafkaPublisher struct {
	// brokers is the normalised bootstrap list, retained for logging and for building
	// writers lazily.
	brokers []string

	// addr is the pre-built broker address shared by every writer. kafka.TCP performs
	// no name resolution, so building it is pure string work.
	addr net.Addr

	// transport is shared by every writer so that all of them draw on ONE connection pool
	// and ONE authentication and TLS configuration; kafka.Transport establishes
	// connections lazily per broker and reuses them, authenticating each as it is opened.
	// Per-writer transports would multiply connections and SASL handshakes by the number
	// of topics for no benefit.
	transport *kafka.Transport

	// mu guards writers and closed. *kafka.Writer is itself safe for concurrent use;
	// what needs guarding is the map that hands them out and the shutdown flag.
	mu sync.RWMutex

	// writers is keyed by topic. It is pre-populated with every topic Blnk owns and
	// grows lazily — see writerFor for the two real cases that require growth.
	writers map[string]*kafka.Writer

	// lazyTopics is the topics added AFTER construction, in creation order.
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

	// The MANDATED SIGNATURE itself, pinned at compile time. "We documented it" is not a
	// guarantee: assigning each implementation's method to a variable of the exact
	// function type means any drift — an added parameter, a tuple return, a renamed method
	// — stops the build here, on the lines that state the contract, rather than surfacing
	// as a puzzling failure somewhere downstream. The assignment is the whole point, so
	// the values are discarded.
	_ func(context.Context, model.LedgerEvent) error = (&NoopEventPublisher{}).Publish
	_ func(context.Context, model.LedgerEvent) error = (&kafkaPublisher{}).Publish
)

// NewEventPublisher builds the event publisher from configuration.
//
// CONSTRUCTION PERFORMS NO I/O AT ALL. No connection is dialled, no name is resolved,
// no metadata is fetched, and nothing blocks. kafka.TCP only canonicalises address
// strings, a SCRAM mechanism is pure computation, and a kafka.Writer connects lazily on
// its first write.
//
// Parameters:
//   - cnf *config.Configuration: the loaded configuration. May be nil.
//
// Returns:
//   - EventPublisher: the Kafka publisher when brokers are configured, otherwise the
//     no-op.
//   - error: non-nil when the transport cannot be assembled from the configuration —
//     administrative or producer SASL credentials, TLS material, or the plaintext
//     policy.
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
	// confirm the producer connected as intended, so both are named explicitly rather than
	// inferred from which variables happen to be set.
	producerUser, _, credErr := kafkaTransportCredentials(cnf.Kafka, KafkaTransportRoleProducer)
	if credErr != nil {
		// Unreachable: newKafkaPublisher resolved the same pair a moment ago and would have
		// returned the error. Logged rather than ignored so a future divergence between the
		// two calls is visible instead of silent.
		withLoggableCause(nil, credErr).Warn(
			"could not re-resolve the producer SASL principal for the initialisation log",
		)
	}

	// broker_count and auth_mode rather than the endpoint list and the principal. The
	// address list is topology: it tells a reader of the log where to aim, and it tells an
	// operator nothing they cannot get from their own configuration. The principal is the
	// other half of a SCRAM credential whose mechanism this same line publishes, so naming
	// it at info narrows a guess to one unknown.
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
	// confirming exactly this. sanitizeLogValue because both values come from
	// configuration and neither their length nor their content is this codebase's to
	// assume.
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
// Parameters:
//   - brokers []string: a non-empty, normalised bootstrap list.
//   - cfg config.KafkaConfig: the Kafka configuration block, read for credentials and
//     TLS.
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
	// admin path and the provisioning script also work from. Pre-creating costs nothing —
	// a writer performs no I/O until its first write — and it means the steady-state
	// publish path never takes the write lock.
	for _, topic := range AllOwnedTopicsAcrossPrefixes() {
		publisher.writers[topic] = publisher.newWriter(topic)
	}

	return publisher, nil
}

// KafkaTransportRole names which principal a transport authenticates as.
//
// It exists because the producer and the administrator are DIFFERENT PRINCIPALS with
// deliberately different privileges, and a single "the Kafka credentials" notion is
// what erased that distinction. The role is an explicit argument so a caller cannot
// pick up the wrong one by omission.
type KafkaTransportRole string

const (
	// KafkaTransportRoleProducer is the steady-state event-publishing principal. It needs
	// Write and Describe on the Blnk-owned topics and NOTHING ELSE — no topic creation, no
	// credential alteration, no ACL management.
	KafkaTransportRoleProducer KafkaTransportRole = "producer"

	// KafkaTransportRoleAdmin is the provisioning principal.
	KafkaTransportRoleAdmin KafkaTransportRole = "admin"
)

// NewKafkaTransport builds the shared kafka.Transport for one role, applying the TLS
// and credential policy.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block, read for TLS material and both
//     credential pairs.
//   - role KafkaTransportRole: which principal to authenticate as.
//
// Returns:
//   - *kafka.Transport: the assembled transport.
//   - error: a malformed or half-configured credential pair, unreadable or invalid TLS
//     material, or plaintext without the explicit local-dev acknowledgement.
func NewKafkaTransport(cfg config.KafkaConfig, role KafkaTransportRole) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		DialTimeout: eventTransportDialTimeout,
		IdleTimeout: eventTransportIdleTimeout,
		ClientID:    eventTransportClientID,
	}

	// Both halves are resolved before either is judged, and their errors are JOINED.
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
// about the ENVIRONMENT — "this broker is a local development broker" — and an
// assertion nobody checks is indistinguishable from a switch that disables encryption
// outright.
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

		// A non-empty reason means the classifier recognised the host as internal, which is
		// what this branch requires. An empty reason means it did not, and that is the
		// refusal.
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

// kafkaTLSConfig builds the verified TLS configuration, or returns nil when plaintext
// has been explicitly permitted.
//
// Returns:
//   - *tls.Config: the verified configuration, or nil for explicitly-permitted
//     plaintext.
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

		// THE ACKNOWLEDGEMENT IS SCOPED TO WHAT IT CLAIMS TO BE.
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
// Every ledger event was therefore produced by the most privileged identity in the
// system, so a leaked producer credential handed an attacker the ability to rewrite the
// access model rather than merely to publish events, and nothing in the broker's audit
// trail could distinguish routine publishing from administration.
//
// Parameters:
//   - cfg config.KafkaConfig: the Kafka block.
//   - role KafkaTransportRole: which principal to resolve.
//
// Returns:
//   - user, secret string: the resolved pair. Both empty means no SASL is configured,
//     which is legitimate for an unauthenticated local broker.
//   - error: a half-configured pair, for either role; or
//     ErrKafkaProducerCredentialsRequired when the producer role would have to borrow
//     the administrative credential and that has not been explicitly allowed.
func kafkaTransportCredentials(cfg config.KafkaConfig, role KafkaTransportRole) (string, string, error) {
	if role == KafkaTransportRoleAdmin {
		if err := cfg.ValidateSASLAdminCredentials(); err != nil {
			return "", "", err
		}

		// Resolved through the ONE named reading rather than by trimming the fields here.
		// Both values are trimmed by it, so a secret carrying a trailing newline from a
		// secret store cannot reach the SASL mechanism, and "exactly one set" cannot produce
		// a half-authenticated transport: it is refused above.
		user, secret, _ := cfg.SASLAdminCredentials()

		return user, secret, nil
	}

	if err := config.ValidateSASLPair("producer", cfg.SASLUser, cfg.SASLSecret); err != nil {
		return "", "", err
	}

	if producer := strings.TrimSpace(cfg.SASLUser); producer != "" {
		return producer, cfg.SASLSecret, nil
	}

	// No dedicated producer principal. The administrative pair is validated first so that
	// a half-configured one is reported as the configuration defect it is, rather than
	// being silently read as "no admin credentials" and turned into the different
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

// saslCredentialError reports that the configured SASL credentials cannot be prepared,
// and does so WITHOUT wrapping the underlying library error.
//
// Not wrapping is a SECURITY decision, not an oversight. The SCRAM client constructs
// its failure as `Error SASLprepping password '<password>':...` — it embeds the
// PLAINTEXT PASSWORD in the message text.
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
// publishAcknowledgement receives the broker coordinate of one message.
//
// Parameters:
//   - topic string: the fully-resolved topic this writer publishes to.
//
// Returns:
//   - *kafka.Writer: a ready writer that has performed no I/O.
type publishAcknowledgement struct {
	mu     sync.Mutex
	record model.BrokerRecord
	filled bool
}

// accept records the coordinate the broker assigned.
//
// The first completion wins. kafka-go calls Completion once per batch and a message
// belongs to exactly one batch, so a second call for the same carrier would mean an
// internal retry re-reporting the message — in which case the first coordinate is the
// one the offset series was assigned from.
//
// Parameters:
//   - message kafka.Message: the completed message, carrying the broker's topic,
//     partition and offset.
func (a *publishAcknowledgement) accept(message kafka.Message) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.filled {
		return
	}

	// A message the broker did not answer for carries no usable coordinate. kafka-go
	// leaves the fields at their zero values in that case and reports the error
	// separately, and partition 0 / offset 0 is a REAL location — so accepting it would
	// manufacture a coordinate for a write that never landed, which is worse than
	// recording nothing.
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
// It fans each completed message out to its own carrier and does nothing else. Keeping
// it this small is deliberate: kafka-go documents that a panic in a completion function
// terminates the program, because the panic bubbles up a writer goroutine that nothing
// recovers — so this function must be incapable of panicking.
//
// Parameters:
//   - messages []kafka.Message: the batch, with topic, partition and offset set from
//     the produce response.
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

		// A STABLE HASH BALANCER. Murmur2 is chosen over kafka-go's other stable hashes
		// because it reproduces the Java client's default partitioner exactly, so a message
		// Blnk produces for a given key lands on the same partition a Java or librdkafka
		// producer would choose for it. That matters the moment anything other than Blnk
		// writes to these topics, or a subscriber reasons about partition assignment from the
		// key.
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

	// Checked BEFORE the write lock is taken, so a rejected topic never contends with live
	// publishes and never enters the map.
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

		// Closed in a goroutine so it happens OUTSIDE this function's deferred unlock. Close
		// flushes whatever the writer still holds, and doing that under the write lock would
		// stall every concurrent publish — including publishes to the eight known topics,
		// which have nothing to do with this retirement.
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

// retireOldestLazyWriterLocked evicts the oldest lazily-created writer once the cache
// is over its bound, and returns it for the caller to close.
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
// The destination is resolved with TopicForEvent, and the message key falls back to the
// event's aggregate ID because the stored partition key is not carried in the envelope
// — see resolvePartitionKey, which documents exactly what that fallback preserves.
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
// event has failed for good: retry is the relay's responsibility, bounded by the
// relay's own budget, and the writer's internal retry loop is switched off so that one
// call here is one produce request.
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

	// THE PRODUCER SPAN, and the point at which a request's trace resumes after the
	// outbox.
	ctx, span := tracer.Start(ctx, "publish "+boundedTopicLabel(resolveTopic(req)),
		append(
			traceRequestLinks(req),
			trace.WithSpanKind(trace.SpanKindProducer),
		)...,
	)
	defer span.End()

	// EVERY field is resolved through the same helpers the no-op uses, and the last two
	// are not decoration: they are read by code below and in recordPublishAttempt.
	result := PublishResult{
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
		// Carried from the envelope so that EVERY exit path below reports it, exactly as the
		// topic and key are.
		CapturedAt: req.Event.OccurredAt,
		// RESOLVED HERE, on the same line of reasoning as the topic and the key above, and
		// for the same reason the no-op resolves them: every one of the five is a request
		// field with a documented fallback, and a result that omits one silently changes what
		// the pipeline reports.
		MaxAttempts: resolveMaxAttempts(req),
		Purpose:     resolvePurpose(req),
	}

	// THE SEMANTIC MESSAGING ATTRIBUTES, set once from the resolved result rather than
	// from the request, so the span describes what was actually attempted — the resolved
	// topic and key — and not what the caller asked for.
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

	// elapsed measures from the outbox claim when the relay supplied that instant, so the
	// histogram reports what its documentation promises: claim to acknowledgement.
	// Otherwise it measures the write alone.
	elapsed := func() time.Duration {
		if req.ClaimedAt.IsZero() {
			return time.Since(started)
		}

		return time.Since(req.ClaimedAt)
	}

	// recordSpanFailure marks the span failed with a BOUNDED class rather than the error
	// text.
	recordSpanFailure := func(cause error, transient bool) {
		span.SetAttributes(attribute.Bool("blnk.publish.transient", transient))
		span.SetStatus(codes.Error, publishSpanErrorClass(cause))
	}

	value, err := resolveEventValue(req)
	if err != nil {
		// A payload that is not valid JSON cannot be spliced into an envelope without
		// producing a message that breaks every subscriber's parser, so this is permanent by
		// construction: no number of retries turns corrupt bytes into valid ones, and the
		// dead-letter topic would reject them for the same reason. The durable record is the
		// outbox row itself, whose last_error names the problem for triage.
		result.Duration = elapsed()
		failed := p.fail(ctx, result, err, false)
		recordSpanFailure(failed.Err, false)

		return failed, failed.Err
	}

	// the size ceiling is enforced HERE, on the fully-marshalled envelope, and
	// not only on the stored payload.
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
		// EVENT: the process is shutting down, and the next attempt — in this process after a
		// restart, or in another replica right now — has a live transport and will publish
		// it. So it is reported RECOVERABLE.
		//
		// A REFUSED TOPIC (ErrTopicNotOwned) is permanent for the event itself: the
		// destination is not one Blnk may write to, so no retry and no broker state can make
		// the write legitimate, and the row belongs in the dead-letter inventory where an
		// operator can see it now rather than in five attempts' time.
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
	if record, confirmed := acknowledgement.coordinate(); confirmed {
		result.Record = record
		// The coordinate on the span is what turns "this event was published" into "this
		// event is THAT record" for someone reading a trace rather than the outbox row. Both
		// values are broker-assigned integers, so neither is caller data and neither needs
		// bounding.
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

	// EventsPublishedTotal IS NOT INCREMENTED HERE, and its absence is the point.
	metrics.EventBrokerAcknowledgementsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(result.EventType)),
		attribute.String(publishAttrPurpose, string(result.Purpose)),
	))

	recordPublishAttempt(ctx, result)

	// GUARDED, and the failure log deliberately is not. LogFields builds a map, hashes the
	// partition key and bounds the error string on every call, and this one runs once per
	// published event at 500 events per second — work whose result is discarded whenever
	// debug is off, which is every production deployment.
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		logrus.WithFields(result.LogFields()).Debug("ledger event published to kafka")
	}

	return result, nil
}

// fail completes a failed result: it attaches the classified error, records the attempt
// metrics, and logs the attempt.
//
// It exists so that every failure exit from PublishToTopic reports identically. The
// alternative — repeating the wrap, the two metric calls and the log line at each exit
// — is exactly how one of them ends up subtly different from the others.
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
	result.Retryable = transient && !attemptBudgetSpent(result.Attempt, result.MaxAttempts)
	result.Status = model.PublishStatusRetrying

	result.Transient = transient
	result.Classified = true

	// TERMINAL IS THE COMPLEMENT OF RETRYABLE ON A FAILURE, and it is set here because
	// this is the only place that knows both halves of it: the classification, and whether
	// the attempt spent the budget the row stated. Both of Terminal's documented cases
	// fall out of the one expression — a non-transient failure is not retryable, and
	// neither is a transient one on the last permitted attempt.
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
// Wherever the row records a ledger, that ledger IS the key, so every event of one
// ledger lands on one partition and a subscriber reading that partition observes the
// ledger's whole event sequence in order.
//
// Parameters:
//   - row model.EventOutbox: the claimed row.
//   - attempt int: the 1-based attempt number this publish represents.
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
		// LEDGER FIRST. See the fallback chain above.
		Key:     row.EffectiveKey(),
		Attempt: attempt,
		// THE CAPTURED TRACE, carried from the row so the publish span links to the request
		// that produced the event. This conversion is used by the relay, the dead-letter
		// write and the replay alike, so all three inherit the link from one place — which is
		// the same reason the canonical bytes and the topic are resolved here rather than at
		// each call site.
		Traceparent: row.Traceparent,
		Tracestate:  row.Tracestate,
	}
}

// marshalLedgerEvent serialises the envelope for the wire, splicing the payload bytes
// in VERBATIM.
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
//   - error: non-nil when the payload is not valid JSON, or when the timestamp cannot
//     be rendered as RFC3339 (a year outside the four-digit range).
func marshalLedgerEvent(event model.LedgerEvent) ([]byte, error) {
	return event.CanonicalBytes()
}

// resolveEventValue returns the bytes to put on the topic for one request.
//
// A stored value is NOT re-validated. It was validated at the persistence boundary, it
// is the authority, and re-parsing it on every attempt would be a per-publish cost for
// a check that cannot change its answer.
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

// resolveTopic returns the destination for a request: the topic it names, or the topic
// the event type routes to when it names none.
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
// The zero value of PublishRequest.Attempt means "not stated", which is the first
// attempt; a negative value can only be a caller mistake and is treated the same way.
// Normalising here keeps the metric's attempt attribute a small, stable label set
// instead of admitting "0" and negative values into it.
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
// trimming, so "host-a:9092, host-b:9092" yields an entry with a leading space that
// would be dialled verbatim and fail.
//
// Parameters:
//   - brokers []string: the configured list. May be nil.
//
// Returns:
//   - []string: a fresh slice of non-empty, trimmed addresses. Empty when nothing
//     usable was configured.
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
// classification buys is that a relay need not burn its whole budget on a failure that
// can never succeed, and need not give up on one that plainly can.
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
// The instruments themselves live in internal/metrics, declared as package-level
// variables and assigned in its single Init, which the package's own initialiser runs.
// Nothing here creates an instrument.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - result PublishResult: the completed attempt.
func recordPublishAttempt(ctx context.Context, result PublishResult) {
	// THE TERMINAL DIMENSION ACCOMPANIES THE OUTCOME, and it has to. The outcome
	// vocabulary is frozen at three values, so a failed attempt that will never be retried
	// and one that will are both outcome="retrying"; without this attribute the counter
	// could no longer answer "how many events are actually stuck", which is the question
	// the dead-letter triage runbook opens with. Its domain is closed at two literals, so
	// it multiplies the series count by two and nothing more.
	metrics.EventPublishAttemptsTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrOutcome, string(result.Status)),
		attribute.String(publishAttrTerminal, terminalAttributeValue(result)),
	))

	// attemptLabel and not strconv: the attribute sits on a HISTOGRAM, so its cardinality
	// is multiplied by the bucket count and the domain has to stay closed at its eight
	// declared values. See attemptLabel for the three inputs that would otherwise widen
	// it.
	metrics.EventPublishDuration.Record(ctx, result.Duration.Seconds(), otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(result.Topic)),
		attribute.String(publishAttrAttempt, attemptLabel(result.Purpose, result.Attempt)),
		attribute.String(publishAttrOutcome, string(result.Status)),
	))

	recordCaptureToDispatch(ctx, result)
}

// recordDurableEventDelivery increments EventsPublishedTotal for ONE event whose
// delivery is now DURABLY RECORDED: the broker acknowledged the write AND the
// claim-token-conditional statement that records the Kafka leg has committed.
//
// Parameters:
//   - ctx context.Context: the recording context. A cancelled one is harmless; the
//     OpenTelemetry synchronous counter does not block on it.
//   - topic string: the CATEGORY topic the event was published to.
//   - eventType string: the event name, bounded to the closed event-type vocabulary.
func recordDurableEventDelivery(ctx context.Context, topic, eventType string) {
	if metrics.EventsPublishedTotal == nil {
		// Observability is optional, and this is the one metric write on the relay's
		// BOOKKEEPING path rather than on a publish path. Every other writer in this file
		// runs after a publish that already required the instruments; this one runs after the
		// row has already been moved, so a nil instrument must not take a settled event's
		// bookkeeping down with it.
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
// The relay deliberately does not import internal/metrics at all: the collector owns
// the gauges, and a second writer would make them disagree with themselves between
// ticks.
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

// kafkaLogger adapts kafka-go's logging hook to logrus, so that anything the client has
// to say about a broker arrives in the same structured stream as the rest of Blnk
// instead of on stderr, unstructured and unattributed.
type kafkaLogger struct {
	level logrus.Level
	topic string
}

// Printf satisfies kafka-go's logger interface, forwarding the formatted message to
// logrus at this logger's level with the topic attached.
//
// The DEBUG hook is forwarded verbatim and deliberately so: it is only ever emitted
// when a deployment has asked for debug, which is the same consent gate cause_verbatim
// sits behind.
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
	// numeric comparison against WarnLevel. Anything below it — debug and trace — is
	// already gated on a deployment having asked for that detail.
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
