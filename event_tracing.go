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

// event_tracing.go carries a trace ACROSS the transactional outbox, which is the one
// place in the event pipeline where an in-memory context cannot reach.
package blnk

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/model"
)

// The W3C Trace Context header names, used both as outbox column contents and as Kafka record
// header keys.
const (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
)

// eventTraceCarrier is a propagation carrier over a two-key map.
type eventTraceCarrier map[string]string

// Get returns the value of a carrier key, or "" when absent.
func (c eventTraceCarrier) Get(key string) string { return c[key] }

// Set stores a carrier key.
func (c eventTraceCarrier) Set(key, value string) { c[key] = value }

// Keys returns the carrier's keys.
func (c eventTraceCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}

	return keys
}

// eventTracePropagator is the propagator used for every outbox column and Kafka header in this
// file. See the file comment for why it is W3C explicitly rather than the global composite.
var eventTracePropagator = propagation.TraceContext{}

// captureTraceContext serialises the trace context active in ctx into the two values an
// outbox row stores.
func captureTraceContext(ctx context.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}

	// A span context that is not valid injects nothing, so the guard is not strictly
	// required — but it makes the common untraced path free rather than allocating a
	// carrier to discover that it has nothing to put in it, and this runs once per
	// captured event at 500 events per second.
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return "", ""
	}

	carrier := eventTraceCarrier{}
	eventTracePropagator.Inject(ctx, carrier)

	return model.SanitizeTraceContext(carrier[traceparentHeader], carrier[tracestateHeader])
}

// capturedSpanContext rebuilds the span context an outbox row recorded at capture.
func capturedSpanContext(traceparent, tracestate string) trace.SpanContext {
	parent, state := model.SanitizeTraceContext(traceparent, tracestate)
	if parent == "" {
		return trace.SpanContext{}
	}

	carrier := eventTraceCarrier{traceparentHeader: parent}
	if state != "" {
		carrier[tracestateHeader] = state
	}

	// Extracted into a background context deliberately: what is wanted is the RECORDED
	// context alone, and extracting into a live context would return that context's own
	// span whenever the stored value failed to parse — silently linking the publish to
	// whatever happened to be in flight instead of to the capture.
	return trace.SpanContextFromContext(
		eventTracePropagator.Extract(context.Background(), carrier),
	)
}

// linkToCapturedTrace turns an outbox row's stored trace context into span-start
// options.
func linkToCapturedTrace(row model.EventOutbox) []trace.SpanStartOption {
	captured := capturedSpanContext(row.Traceparent, row.Tracestate)
	if !captured.IsValid() {
		return nil
	}

	return []trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: captured})}
}

// kafkaTraceHeaders serialises the trace context active in ctx into Kafka record
// headers.
func kafkaTraceHeaders(ctx context.Context) []kafka.Header {
	if ctx == nil || !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}

	carrier := eventTraceCarrier{}
	eventTracePropagator.Inject(ctx, carrier)

	headers := make([]kafka.Header, 0, len(carrier))
	// Emitted in a FIXED order rather than by ranging the map, because a record's headers
	// are part of what the byte-level dual-delivery and replay comparisons see, and Go's
	// map order is randomised per iteration. Two publishes of one event would otherwise
	// differ in header order for no reason anybody could explain.
	for _, key := range []string{traceparentHeader, tracestateHeader} {
		value := carrier[key]
		if value == "" {
			continue
		}

		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}

	if len(headers) == 0 {
		return nil
	}

	return headers
}

// traceRequestLinks turns a publish request's carried trace context into span-start
// options.
func traceRequestLinks(req PublishRequest) []trace.SpanStartOption {
	captured := capturedSpanContext(req.Traceparent, req.Tracestate)
	if !captured.IsValid() {
		return []trace.SpanStartOption{}
	}

	return []trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: captured})}
}

// The bounded classes a publish failure is reported as on a span.
const (
	publishSpanErrorUnknown        = "publish failed"
	publishSpanErrorPublisherState = "publisher unavailable"
	publishSpanErrorTopicRefused   = "topic not owned by blnk"
	publishSpanErrorMessageTooBig  = "message over the size limit"
	publishSpanErrorBroker         = "broker write failed"
)

// publishSpanErrorClass maps a publish failure to one of the bounded classes above.
func publishSpanErrorClass(cause error) string {
	switch {
	case cause == nil:
		return publishSpanErrorUnknown
	case errors.Is(cause, ErrEventPublisherClosed):
		return publishSpanErrorPublisherState
	case errors.Is(cause, ErrTopicNotOwned):
		return publishSpanErrorTopicRefused
	case errors.Is(cause, ErrEventMessageTooLarge):
		return publishSpanErrorMessageTooBig
	default:
		return publishSpanErrorBroker
	}
}

// The bounded classes a Kafka ADMIN failure is reported as on a span.
const (
	adminSpanErrorUnknown       = "kafka admin operation failed"
	adminSpanErrorNotConfigured = "kafka admin is not configured"
	adminSpanErrorAuthorizer    = "broker authorizer is not enforcing acls"
	adminSpanErrorGeometry      = "topic geometry could not be satisfied"
	adminSpanErrorBroker        = "broker refused the admin operation"
)

// adminSpanErrorClass maps a Kafka admin failure to one of the bounded classes above.
func adminSpanErrorClass(cause error) string {
	switch {
	case cause == nil:
		return adminSpanErrorUnknown
	case errors.Is(cause, ErrKafkaAdminNotConfigured):
		return adminSpanErrorNotConfigured
	case errors.Is(cause, ErrAuthorizerNotEnforcing):
		return adminSpanErrorAuthorizer
	case errors.Is(cause, ErrPartitionGrowthRefused), errors.Is(cause, ErrReplicationFactorInadequate):
		return adminSpanErrorGeometry
	default:
		return adminSpanErrorBroker
	}
}

// startKafkaAdminSpan opens a span over one Kafka administrative operation.
func startKafkaAdminSpan(
	ctx context.Context,
	operation string,
	attrs ...attribute.KeyValue,
) (context.Context, trace.Span) {
	ctx, span := tracer.Start(ctx, "kafka.admin "+operation, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(append([]attribute.KeyValue{
		attribute.String("messaging.system", "kafka"),
		attribute.String("blnk.kafka.admin.operation", operation),
	}, attrs...)...)

	return ctx, span
}

// failKafkaAdminSpan marks an admin span failed with a bounded class.
func failKafkaAdminSpan(span trace.Span, cause error) {
	if cause == nil {
		return
	}

	span.SetStatus(codes.Error, adminSpanErrorClass(cause))
}

// hashedSubscriberSpanAttribute renders a subscriber identifier as a bounded span
// attribute.
func hashedSubscriberSpanAttribute(subscriberID string) attribute.KeyValue {
	return attribute.String("blnk.subscriber.id_hash", hashLogIdentifier(subscriberID))
}

// subscriberSpanAttribute is the nil-safe form for the admin operations that take a
// whole subscriber, several of which legitimately receive nil and report it as a named
// error.
func subscriberSpanAttribute(subscriber *model.EventSubscriber) attribute.KeyValue {
	if subscriber == nil {
		return hashedSubscriberSpanAttribute("")
	}

	return hashedSubscriberSpanAttribute(subscriber.SubscriberID)
}
