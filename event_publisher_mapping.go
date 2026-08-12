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
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// PublishRequestFromOutbox builds the publish request for a claimed outbox row.
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
func marshalLedgerEvent(event model.LedgerEvent) ([]byte, error) {
	return event.CanonicalBytes()
}

// resolveEventValue returns the bytes to put on the topic for one request.
func resolveEventValue(req PublishRequest) ([]byte, error) {
	if len(bytes.TrimSpace(req.Raw)) > 0 {
		return req.Raw, nil
	}

	return marshalLedgerEvent(req.Event)
}

// resolveTopic returns the destination for a request: the topic it names, or the topic
// the event type routes to when it names none.
func resolveTopic(req PublishRequest) string {
	if topic := strings.TrimSpace(req.Topic); topic != "" {
		return topic
	}

	return TopicForEvent(req.Event.EventType)
}

// resolvePartitionKey returns the Kafka message key for a request.
func resolvePartitionKey(req PublishRequest) string {
	if key := strings.TrimSpace(req.Key); key != "" {
		return key
	}

	return strings.TrimSpace(req.Event.AggregateID)
}

// partitionKeyBytes converts a resolved key to the message key.
func partitionKeyBytes(key string) []byte {
	if key == "" {
		return nil
	}

	return []byte(key)
}

// resolveAttempt normalises the attempt number to a 1-based value.
func resolveAttempt(req PublishRequest) int {
	if req.Attempt < 1 {
		return 1
	}

	return req.Attempt
}

// normalizeBrokers cleans a configured broker list.
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
