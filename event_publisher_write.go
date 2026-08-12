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
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// newWriter builds one writer for one topic, with every load-bearing setting stated
// explicitly. See the tuning constants for why each value is what it is and why none of
// them is left to a library default.
type publishAcknowledgement struct {
	mu     sync.Mutex
	record model.BrokerRecord
	filled bool
}

// accept records the coordinate the broker assigned.
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
func (a *publishAcknowledgement) coordinate() (model.BrokerRecord, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.record, a.filled
}

// completeWrite is the writers' shared Completion callback.
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
var ErrTopicNotOwned = errors.New("blnk: refusing to publish to a topic Blnk does not own")

// writerFor returns the writer for a topic, creating and caching one only for topics
// inside the Blnk-owned namespace.
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
