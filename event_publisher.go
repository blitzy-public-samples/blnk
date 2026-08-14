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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/model"
)

// This file is the publishing layer of the Kafka event pipeline. It answers exactly one
// question — how does a model.LedgerEvent get onto a Kafka topic and what happened when
// it did — and it owns nothing else:

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
func boundedEventTypeLabel(eventType string) string {
	if !model.IsCataloguedEventType(eventType) {
		return unrecognisedEventTypeLabel
	}

	return eventType
}

// EventTransportErrorDetail is the detail attached to a typed API error whose cause
// came from the Kafka client rather than from Blnk.
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
var ErrEventMessageTooLarge = errors.New("blnk: serialised event exceeds the maximum message size")

// ErrKafkaProducerCredentialsRequired is returned when a deployment configures Kafka
// brokers and administrative SASL credentials but no dedicated producer principal, and
// has not explicitly opted in to publishing as the administrator.
var ErrKafkaProducerCredentialsRequired = errors.New(
	"blnk: a dedicated Kafka producer principal is required; set KAFKA_SASL_USER and KAFKA_SASL_SECRET",
)

// EventPublisher publishes a canonical ledger event.
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
// Returns:
//   - PublishResult: a copy whose Status is model.PublishStatusDeadLettered.
func (r PublishResult) DeadLettered() PublishResult {
	r.Status = model.PublishStatusDeadLettered

	return r
}

// LogFields renders the result as logrus fields.
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
func localContextTermination(err error) bool {
	if err == nil || brokerOutageSignature(err) {
		return false
	}

	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// brokerUnavailable classifies a RAW publish failure — one that has not been through
// kafkaPublisher.fail and so carries no verdict of its own.
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
type NoopEventPublisher struct{}

// NewNoopEventPublisher returns the no-op publisher.
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
