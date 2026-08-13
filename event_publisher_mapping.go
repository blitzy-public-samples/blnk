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

// normalizeBrokers cleans a configured broker list: it trims each address, drops the blanks,
// and drops repeats, keeping the order of first appearance.
//
// It matches config.normalizeBrokers deliberately, because the two answer the same question
// for the same value at different layers, and a bootstrap list is a SET of discovery seeds:
// the same address named twice is one broker, and counting it twice is what made
// `broker_count=3` appear in this package's start-up line for a single-broker stack.
func normalizeBrokers(brokers []string) []string {
	seen := make(map[string]struct{}, len(brokers))
	normalized := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		trimmed := strings.TrimSpace(broker)
		if trimmed == "" {
			continue
		}
		if _, already := seen[trimmed]; already {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
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

// Claim outcome labels. They partition every claim the relay issues into four states, and
// the partition is exhaustive on purpose: an operator adding the four series together must
// get the claim count, or a state has gone unrecorded.
const (
	// claimOutcomeRows is a claim that returned work.
	claimOutcomeRows = "rows"
	// claimOutcomeEmpty is a claim that found nothing claimable, which is the normal
	// steady state of a drained outbox and is what distinguishes an IDLE relay from a
	// stopped one.
	claimOutcomeEmpty = "empty"
	// claimOutcomeError is a claim the repository rejected or the driver failed.
	claimOutcomeError = "error"
	// claimOutcomeTimeout is a claim cancelled by its own budget — the state that used to
	// be invisible, because a statement that never returns produces no error to log.
	claimOutcomeTimeout = "timeout"
)

// recordEventClaim records the outcome, duration and size of ONE outbox claim.
//
// It is called on EVERY claim, including the ones that fail, because the reading that
// matters most is taken while claims are going wrong: the duration climbs before any
// throughput counter falls, which makes it the leading indicator of a relay whose claim has
// outgrown its indexes.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - elapsed time.Duration: how long the claim took, measured across the repository call.
//   - claimed int: how many rows it returned.
//   - err error: the claim's error, or nil.
//   - budgetExpired bool: whether the claim's own deadline had passed, which is what
//     separates a timed-out claim from a rejected one.
func recordEventClaim(ctx context.Context, elapsed time.Duration, claimed int, err error, budgetExpired bool) {
	// Observability is optional throughout this pipeline, and a claim must not fail because
	// the instruments were never created. Guarded on the counter alone: Init assigns all
	// three together or returns, so one being present means all are.
	if metrics.EventRelayClaimsTotal == nil {
		return
	}

	outcome := claimOutcomeRows

	switch {
	case err != nil && budgetExpired:
		outcome = claimOutcomeTimeout
	case err != nil:
		outcome = claimOutcomeError
	case claimed == 0:
		outcome = claimOutcomeEmpty
	}

	attributes := otelmetric.WithAttributes(attribute.String(publishAttrOutcome, outcome))

	metrics.EventRelayClaimsTotal.Add(ctx, 1, attributes)

	if elapsed >= 0 {
		metrics.EventRelayClaimDuration.Record(ctx, elapsed.Seconds(), attributes)
	}

	if claimed > 0 {
		// Rows and claims are counted separately rather than one being derived from the
		// other, because their RATIO is the diagnosis: 100 rows per claim is a relay working
		// at its batch size, and one row per claim is a relay paying a claim's cost for every
		// event.
		metrics.EventRelayClaimedRows.Add(ctx, int64(claimed), attributes)
	}
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
