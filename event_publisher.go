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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
//   - The PARTITION KEY. The key is the ledger ID, and the ledger ID is not one of
//     the six envelope fields; it lives on the outbox row. See resolvePartitionKey.
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

	// Key is the Kafka message key, which is the LEDGER ID. Keying by ledger ID with
	// a stable hash balancer pins every event for one ledger to a single partition,
	// and that partition affinity is what makes per-aggregate ordering hold: Kafka
	// orders within a partition only.
	//
	// When empty, the event's aggregate ID is used instead — see resolvePartitionKey
	// for why that fallback is safe and what it does and does not preserve.
	Key string

	// Attempt is the 1-based attempt number, used only as a metric attribute. Any
	// value below 1 is treated as 1. The publisher never interprets it as a budget:
	// it does not compare it against a maximum and does not change behaviour as it
	// grows, because retry is the relay's responsibility alone.
	Attempt int

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
	// resolvePartitionKey was applied. An empty value means the message was written
	// without a key and was therefore balanced across partitions rather than pinned
	// to one — correct for an event that belongs to no ledger and no aggregate, and a
	// red flag for anything else.
	PartitionKey string

	// Attempt is the 1-based attempt number this result describes.
	Attempt int

	// Duration is how long the attempt took: from PublishRequest.ClaimedAt when it
	// was set, otherwise the time spent in the write itself.
	Duration time.Duration

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
	fields := logrus.Fields{
		"event_id":      r.EventID,
		"event_type":    r.EventType,
		"topic":         r.Topic,
		"partition_key": r.PartitionKey,
		"attempt":       r.Attempt,
		"status":        string(r.Status),
		"duration_ms":   r.Duration.Milliseconds(),
	}

	if r.Err != nil {
		fields["error"] = r.Err.Error()
		fields["transient"] = r.Transient
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
// without it. In practice this method is unreachable from the relay, which is only
// started when brokers are configured; it is implemented completely regardless so that
// the contract holds for any caller.
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
	// pool and ONE SASL session per broker. Per-writer transports would multiply
	// connections and SASL handshakes by the number of topics for no benefit.
	transport *kafka.Transport

	// mu guards writers and closed. *kafka.Writer is itself safe for concurrent use;
	// what needs guarding is the map that hands them out and the shutdown flag.
	mu sync.RWMutex

	// writers is keyed by topic. It is pre-populated with every topic Blnk owns and
	// grows lazily — see writerFor for the two real cases that require growth.
	writers map[string]*kafka.Writer

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
// The returned publisher owns writers for every topic Blnk owns — the four category
// topics and their four dead-letter siblings — enumerated from
// AllTopicsWithDeadLetters so that this file and the provisioning path work from one
// list.
//
// The only error it can return comes from building the SASL/SCRAM mechanism, which
// fails when the configured credentials cannot be prepared (SASLprep rejects the
// username or password). That IS a fatal misconfiguration and must not be silently
// downgraded to the no-op: silently publishing nothing because a password was malformed
// is precisely the failure mode the loud error prevents.
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

	logrus.WithFields(logrus.Fields{
		"brokers":        brokers,
		"topics":         len(publisher.writers),
		"sasl":           cnf.Kafka.SASLAdminUser != "",
		"required_acks":  "all",
		"balancer":       "murmur2",
		"topic_prefix":   TopicPrefix(),
		"internal_retry": false,
	}).Info("kafka event publisher initialised")

	return publisher, nil
}

// newKafkaPublisher assembles the transport and the per-topic writers.
//
// It is separated from NewEventPublisher so that the assembly is reachable from a test
// without going through configuration selection, and so that the selection logic above
// reads as the single decision it is.
//
// SASL is applied only when an administrative username is configured. That is a real
// branch, not a defensive one: the local single-broker stack can run a plaintext
// listener, while any cluster with the KRaft standard authorizer enabled requires
// SCRAM. SHA-512 is used because Kafka supports only SHA-256 and SHA-512 for SCRAM and
// SHA-512 is the stronger of the two.
//
// Parameters:
//   - brokers []string: a non-empty, normalised bootstrap list.
//   - cfg config.KafkaConfig: the Kafka configuration block, read for credentials.
//
// Returns:
//   - *kafkaPublisher: the assembled publisher, with one writer per owned topic.
//   - error: non-nil only when the SCRAM mechanism cannot be built.
func newKafkaPublisher(brokers []string, cfg config.KafkaConfig) (*kafkaPublisher, error) {
	addr := kafka.TCP(brokers...)

	transport := &kafka.Transport{
		DialTimeout: eventTransportDialTimeout,
		IdleTimeout: eventTransportIdleTimeout,
		ClientID:    eventTransportClientID,
	}

	if cfg.SASLAdminUser != "" {
		mechanism, err := scram.Mechanism(scram.SHA512, cfg.SASLAdminUser, cfg.SASLAdminSecret)
		if err != nil {
			return nil, saslCredentialError(cfg.SASLAdminUser)
		}

		transport.SASL = mechanism
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
// Parameters:
//   - username string: the configured KAFKA_SASL_ADMIN_USER.
//
// Returns:
//   - error: a message naming the offending variable, never containing the secret.
func saslCredentialError(username string) error {
	if _, err := scram.Mechanism(scram.SHA512, username, saslProbeValue); err != nil {
		return fmt.Errorf(
			"blnk: KAFKA_SASL_ADMIN_USER %q cannot be prepared as a SASL/SCRAM-SHA-512 credential; "+
				"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point",
			username,
		)
	}

	return fmt.Errorf(
		"blnk: KAFKA_SASL_ADMIN_SECRET cannot be prepared as a SASL/SCRAM-SHA-512 credential for user %q; "+
			"SASLprep rejects it, typically because of a prohibited or unassigned Unicode code point. "+
			"The secret is deliberately omitted from this message",
		username,
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
func (p *kafkaPublisher) newWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:  p.addr,
		Topic: topic,

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

// writerFor returns the writer for a topic, creating and caching one if this is a topic
// the publisher has not seen.
//
// Lazy growth is required for correctness rather than added for flexibility, because
// two reachable situations produce a topic that is not in the pre-created set:
//
//   - A TOPIC PREFIX CHANGE. The prefix is re-read from live configuration on every
//     naming call, so a reload can start resolving events to names this publisher was
//     not constructed with.
//   - A STORED ROW FROM BEFORE SUCH A CHANGE. An outbox row records its destination at
//     insert time, precisely so it stays publishable — and a dead-lettered event stays
//     replayable — to the topic it was always meant for. Refusing to publish it because
//     the name is no longer the one configuration would compose today would strand a
//     committed event.
//
// The fast path takes only a read lock, so concurrent publishes to the eight known
// topics never serialise. Growth double-checks under the write lock so two goroutines
// racing on a new topic share one writer rather than orphaning one.
//
// Parameters:
//   - topic string: a non-empty, fully-resolved topic name.
//
// Returns:
//   - *kafka.Writer: the writer for that topic.
//   - error: ErrEventPublisherClosed when the publisher has been closed.
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
		"creating a Kafka writer for a topic outside the configured inventory; " +
			"this is expected for an event stored before a topic-prefix change",
	)

	writer = p.newWriter(topic)
	p.writers[topic] = writer

	return writer, nil
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
// event's aggregate ID because the ledger ID is not carried in the envelope — see
// resolvePartitionKey, which documents exactly what that fallback preserves.
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

	result := PublishResult{
		EventID:      req.Event.EventID,
		EventType:    req.Event.EventType,
		Topic:        resolveTopic(req),
		PartitionKey: resolvePartitionKey(req),
		Attempt:      resolveAttempt(req),
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

	writer, err := p.writerFor(result.Topic)
	if err != nil {
		result.Duration = elapsed()

		// A closed publisher is permanent for this attempt: this instance will not
		// accept another write. The event is safe, because its row is only marked
		// dispatched on success and becomes claimable again when the lease expires.
		failed := p.fail(ctx, result, err, false)

		return failed, failed.Err
	}

	message := kafka.Message{
		// Topic is intentionally left empty. kafka-go rejects a message whose topic is
		// set when the writer already has one, and the writer here is per-topic.
		Key:   partitionKeyBytes(result.PartitionKey),
		Value: value,
		Time:  req.Event.OccurredAt,
	}

	if err := writer.WriteMessages(ctx, message); err != nil {
		result.Duration = elapsed()
		failed := p.fail(ctx, result, err, classifyTransientPublishError(err))

		return failed, failed.Err
	}

	result.Duration = elapsed()
	result.Status = model.PublishStatusDispatched

	metrics.EventsPublishedTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, result.Topic),
		attribute.String(publishAttrEventType, result.EventType),
	))
	recordPublishAttempt(ctx, result)

	logrus.WithFields(result.LogFields()).Debug("ledger event published to kafka")

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
	result.Status = model.PublishStatusRetrying
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
// It is THE single place the load-bearing routing rule is applied — the Kafka message key
// is the LEDGER ID recorded on the row, and the destination is the topic recorded on the
// row — so that the relay and the dead-letter writer cannot each apply it slightly
// differently. A relay that composed the request inline would be one refactor away from
// dropping the key and silently losing per-aggregate ordering, a defect that no unit test
// of the relay would notice.
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
//     columns, keyed by the row's ledger ID and targeted at the row's topic.
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
		Topic:   row.Topic,
		Key:     row.LedgerID,
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
// THE KEY IS THE LEDGER ID. Every event for one ledger hashes to one partition, Kafka
// orders within a partition, and that is the whole mechanism behind the per-aggregate
// ordering guarantee. It is the same idea the transaction queue already applies when it
// shards by hashing the source balance ID, applied to the transport that now carries the
// events.
//
// The ledger ID is NOT one of the six envelope fields — the envelope is a fixed
// subscriber-facing contract and carries the aggregate ID instead — so it reaches this
// function only when a caller supplies it, which the relay does from the ledger ID column
// of the claimed outbox row. When it is absent, the aggregate ID is used, and the
// consequences of that fallback are worth stating precisely:
//
//   - It PRESERVES per-aggregate ordering, which is the property acceptance requires.
//     Every event for one aggregate still shares a key and therefore a partition.
//   - It does NOT preserve whole-ledger partition affinity, so events for different
//     aggregates in the same ledger may land on different partitions. Nothing requires
//     ordering across aggregates, so nothing is lost.
//   - It cannot split ONE aggregate's stream across two partitions, which would be the
//     only genuinely harmful outcome, because outbox-backed events always arrive with the
//     recorded ledger ID and non-outbox events never share an aggregate with them.
//
// An empty result means the message is written with no key at all and is spread across
// partitions by the balancer. That is the right answer for an event belonging to neither a
// ledger nor an aggregate — an internal-error notification, for instance — where there is
// nothing to order it against and pinning every such event to one partition would only
// create a hot spot.
//
// Parameters:
//   - req PublishRequest: the request to resolve.
//
// Returns:
//   - string: the partition key, or the empty string when the request carries neither a
//     ledger ID nor an aggregate ID.
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

	metrics.EventPublishDuration.Record(ctx, result.Duration.Seconds(), otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, result.Topic),
		attribute.String(publishAttrAttempt, strconv.Itoa(result.Attempt)),
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
