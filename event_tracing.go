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

// event_tracing.go carries a trace ACROSS the transactional outbox, which is the one place in
// the event pipeline where an in-memory context cannot reach.
//
// # The gap this file closes
//
// Blnk's request path is instrumented end to end: otelgin opens a server span, every handler
// passes c.Request.Context() down, and the domain and database layers open child spans from it.
// All of that ENDED AT THE OUTBOX INSERT. The relay claims the row later — up to a poll interval
// later, in a different goroutine, usually in a different process, and after several retries —
// so there is no context to inherit, and publishing, retrying, dead-lettering and replaying one
// event each produced spans in a trace of their own. The question "show me everything that
// happened to this event" was answerable only by grepping logs for its identifier, and the
// question "which request produced this Kafka record" was not answerable at all.
//
// Three moves close it, and this file owns all three:
//
//  1. CAPTURE. The active trace context is serialised into the outbox row's traceparent and
//     tracestate columns at the moment of capture, inside the caller's transaction.
//  2. LINK. When the relay publishes the row, the stored context becomes a span LINK on the
//     producer span — not a parent. See linkToCapturedTrace for why that distinction is
//     load-bearing rather than stylistic.
//  3. PROPAGATE. The producer span's own context is injected into the Kafka record's headers, so
//     a subscriber that reads the record continues the same trace rather than starting a new one.
//
// # Why the W3C propagator is used explicitly rather than the global one
//
// The storage columns and the Kafka headers are named traceparent and tracestate: the format IS
// the contract, both for the database schema's CHECK constraints and for every subscriber that
// reads the headers. Going through otel.GetTextMapPropagator would make that format depend on
// deployment configuration — a deployment that registered only B3 would write B3 values into
// columns and headers named after W3C, and subscribers would silently see nothing they could
// parse. So propagation.TraceContext is used directly, and the composite global propagator
// remains what the HTTP layer uses for ingress.
//
// Baggage is deliberately NOT carried. It is caller-controlled arbitrary key-value data, and
// persisting it on one row per ledger mutation would put unbounded caller input on the relay's
// hottest table — the cost the trace columns' length bounds exist to prevent, reintroduced.
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
//
// Declared as constants rather than written at each use because they appear on both sides of the
// carrier — the map this file injects into and the assertions that read it back — and a typo in
// one of two string literals produces a header nothing reads, with no failure anywhere.
const (
	traceparentHeader = "traceparent"
	tracestateHeader  = "tracestate"
)

// eventTraceCarrier is a propagation carrier over a two-key map.
//
// propagation.MapCarrier from the OTel API would serve, and this is that type by another name;
// it exists so the two header keys above are the only ones this file's carriers can hold, which
// keeps a future propagator change from silently adding a third value to a schema that has two
// columns for it.
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

// captureTraceContext serialises the trace context active in ctx into the two values an outbox
// row stores.
//
// # What "no trace" means here
//
// It returns two empty strings whenever there is nothing to record, and that is a legitimate,
// common outcome rather than a failure: a CLI-driven mutation, a worker-initiated rejection, a
// deployment running with observability disabled, and any request whose span was not sampled all
// produce it. Nothing downstream treats the absence as a defect — the publish simply produces an
// unlinked span.
//
// The result is passed through model.SanitizeTraceContext before being returned, so a value that
// could not be stored legally never leaves this function. That is what keeps the database's
// length constraints a backstop rather than a live failure mode inside a caller's ledger
// transaction; see normalizeEventOutboxEntry.
//
// Parameters:
//   - ctx context.Context: the context whose span context is captured. A nil-safe caller is not
//     required; a context with no span yields empty values.
//
// Returns:
//   - string: the W3C traceparent to store, or "" when there is no trace.
//   - string: the W3C tracestate to store, or "" when there is none.
func captureTraceContext(ctx context.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}

	// A span context that is not valid injects nothing, so the guard is not strictly required —
	// but it makes the common untraced path free rather than allocating a carrier to discover
	// that it has nothing to put in it, and this runs once per captured event at 500 events per
	// second.
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return "", ""
	}

	carrier := eventTraceCarrier{}
	eventTracePropagator.Inject(ctx, carrier)

	return model.SanitizeTraceContext(carrier[traceparentHeader], carrier[tracestateHeader])
}

// capturedSpanContext rebuilds the span context an outbox row recorded at capture.
//
// Returns an INVALID span context — the zero value — when the row carries no trace or carries one
// the propagator cannot parse. Callers test IsValid rather than comparing against nil, which is
// what trace.SpanContext supports.
//
// Parameters:
//   - traceparent string: the stored traceparent, possibly empty.
//   - tracestate string: the stored tracestate, possibly empty.
//
// Returns:
//   - trace.SpanContext: the captured context, or an invalid one.
func capturedSpanContext(traceparent, tracestate string) trace.SpanContext {
	parent, state := model.SanitizeTraceContext(traceparent, tracestate)
	if parent == "" {
		return trace.SpanContext{}
	}

	carrier := eventTraceCarrier{traceparentHeader: parent}
	if state != "" {
		carrier[tracestateHeader] = state
	}

	// Extracted into a background context deliberately: what is wanted is the RECORDED context
	// alone, and extracting into a live context would return that context's own span whenever the
	// stored value failed to parse — silently linking the publish to whatever happened to be in
	// flight instead of to the capture.
	return trace.SpanContextFromContext(
		eventTracePropagator.Extract(context.Background(), carrier),
	)
}

// linkToCapturedTrace turns an outbox row's stored trace context into span-start options.
//
// # Why a LINK and not a parent
//
// Parenting the publish span to the capturing span would be wrong in three separate ways, and
// each of them corrupts data an operator reads directly.
//
// It would MISREPORT DURATION. A trace's duration is its root span's duration, so attaching a
// publish that happens a poll interval later — and after up to five retries spanning
// thirty-one seconds of backoff — would report a request that took four milliseconds as one that
// took half a minute. Every latency percentile computed from traces would be wrong, and wrong in
// the direction that hides fast requests behind slow publishes.
//
// It would ATTACH SPANS TO A COMPLETED TRACE. The capturing span has already ended and, with a
// batch exporter, has usually already been exported. Adding children afterwards produces traces
// that arrive incomplete and then change, which many backends handle by dropping the late spans.
//
// It would COLLAPSE A FAN-OUT. One coalesced ledger transaction captures many events, and each is
// published independently and retried independently. As children they would all hang off one
// request span; as links, each publish trace stands alone and names the capture it came from.
//
// A link states "caused by" without claiming containment, which is exactly what the
// OpenTelemetry messaging conventions specify for a producer decoupled in time from its trigger.
//
// Parameters:
//   - row model.EventOutbox: the claimed row, whose stored trace context is read.
//
// Returns:
//   - []trace.SpanStartOption: one link option, or nil when the row carries no usable trace so
//     the span starts with no link at all.
func linkToCapturedTrace(row model.EventOutbox) []trace.SpanStartOption {
	captured := capturedSpanContext(row.Traceparent, row.Tracestate)
	if !captured.IsValid() {
		return nil
	}

	return []trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: captured})}
}

// kafkaTraceHeaders serialises the trace context active in ctx into Kafka record headers.
//
// This is what lets a SUBSCRIBER continue the trace: it extracts these headers from the record it
// consumes and its own processing span becomes part of the same trace as the publish. Blnk does
// not build subscriber-side consumers — the requirements exclude that explicitly — so this is a
// contract offered outward rather than a facility Blnk uses itself, which is precisely why it has
// to be W3C standard rather than anything Blnk-specific.
//
// It returns nil when there is no trace, so a record produced by an untraced publish carries no
// headers at all rather than empty ones. An empty header value is worse than an absent header: a
// consumer that extracts it gets an invalid span context and, depending on its instrumentation,
// either drops the trace or starts a broken one.
//
// The headers are added to the record and never to the message VALUE, which is what keeps the
// byte-equality guarantees intact: dual delivery (V-8) and replay (V-9) both compare the stored
// envelope bytes, and those bytes are unaffected by anything here.
//
// Parameters:
//   - ctx context.Context: the context whose span context is injected — in practice the producer
//     span's own context, so subscribers link to the publish rather than to the capture.
//
// Returns:
//   - []kafka.Header: the trace headers, or nil when there is no valid trace to propagate.
func kafkaTraceHeaders(ctx context.Context) []kafka.Header {
	if ctx == nil || !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}

	carrier := eventTraceCarrier{}
	eventTracePropagator.Inject(ctx, carrier)

	headers := make([]kafka.Header, 0, len(carrier))
	// Emitted in a FIXED order rather than by ranging the map, because a record's headers are
	// part of what the byte-level dual-delivery and replay comparisons see, and Go's map order is
	// randomised per iteration. Two publishes of one event would otherwise differ in header
	// order for no reason anybody could explain.
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

// traceRequestLinks turns a publish request's carried trace context into span-start options.
//
// It is the request-shaped counterpart of linkToCapturedTrace, and both exist because the two
// callers hold the trace context in different shapes: the relay's dead-letter and retention paths
// work from a claimed row, while every publish works from the request built out of one. Routing
// both through capturedSpanContext is what keeps them from disagreeing about what a usable trace
// is.
//
// Parameters:
//   - req PublishRequest: the request, whose Traceparent and Tracestate are read.
//
// Returns:
//   - []trace.SpanStartOption: a slice holding one link option, or an EMPTY non-nil slice when
//     there is no usable trace. Non-nil so a caller can append its own options to the result
//     without a length check.
func traceRequestLinks(req PublishRequest) []trace.SpanStartOption {
	captured := capturedSpanContext(req.Traceparent, req.Tracestate)
	if !captured.IsValid() {
		return []trace.SpanStartOption{}
	}

	return []trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: captured})}
}

// The bounded classes a publish failure is reported as on a span.
//
// A CLOSED domain, and that is the whole purpose. A span's status description is exported to
// whatever tracing backend is configured and is rendered into trace viewers and incident tickets,
// so putting a Kafka or driver error text there would publish broker hostnames, internal
// addresses and library internals into places they cannot be recalled from. These five classes
// are what an operator acts on differently; the full cause stays on the outbox row's last_error
// and in the publisher's own log line, both of which are access-controlled.
const (
	publishSpanErrorUnknown        = "publish failed"
	publishSpanErrorPublisherState = "publisher unavailable"
	publishSpanErrorTopicRefused   = "topic not owned by blnk"
	publishSpanErrorMessageTooBig  = "message over the size limit"
	publishSpanErrorBroker         = "broker write failed"
)

// publishSpanErrorClass maps a publish failure to one of the bounded classes above.
//
// The order of the tests is the order of specificity, not of likelihood: the three sentinel
// errors identify themselves, so they are checked first, and everything that reaches the broker
// test is a transport or protocol failure by elimination.
//
// It never returns the error's own text. A caller that needs the full cause reads it from the
// outbox row or the log line.
//
// Parameters:
//   - cause error: the publish failure. May be nil.
//
// Returns:
//   - string: one of the publishSpanError* constants.
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
//
// Closed for the same reason the publish classes are: a span's status description leaves the
// process, and an admin error from kafka-go carries broker hostnames, listener addresses and — for
// a credential operation — the principal it was acting on. None of that belongs in a trace
// exported to a shared backend. The full cause stays in the caller's log line and in the error
// returned to the caller, both of which stay inside the deployment.
const (
	adminSpanErrorUnknown       = "kafka admin operation failed"
	adminSpanErrorNotConfigured = "kafka admin is not configured"
	adminSpanErrorAuthorizer    = "broker authorizer is not enforcing acls"
	adminSpanErrorGeometry      = "topic geometry could not be satisfied"
	adminSpanErrorBroker        = "broker refused the admin operation"
)

// adminSpanErrorClass maps a Kafka admin failure to one of the bounded classes above.
//
// The three sentinel conditions identify themselves and each sends an operator somewhere
// different: not-configured is a deployment that has no broker at all and is often a legitimate
// steady state; a non-enforcing authorizer means ACLs are being ACCEPTED AND IGNORED, so every
// isolation guarantee is void while it holds; a geometry failure means the topic exists but not
// with the durability or partitioning that was asked for. Everything else is the broker refusing
// the call.
//
// Parameters:
//   - cause error: the admin failure. May be nil.
//
// Returns:
//   - string: one of the adminSpanError* constants.
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
//
// # Why admin operations are traced at all
//
// They are the operations that CHANGE the broker: creating a topic, minting or revoking a SCRAM
// credential, granting or pruning ACLs. Each is a privileged act performed on behalf of an
// authenticated caller, and each was previously invisible outside a log line — so "when did Blnk
// grant this principal access, and as part of which request" was unanswerable. A span answers it,
// and because the API handlers pass their request context down, the span sits inside the trace of
// the request that caused it.
//
// SpanKindClient rather than Producer: these are calls to the broker's admin API, not records
// produced onto a topic. The messaging.system attribute is set on every one so that admin work is
// selectable alongside the produce spans.
//
// Parameters:
//   - ctx context.Context: the caller's context; the span is a child of whatever is in it.
//   - operation string: a FIXED literal naming the operation. Never interpolated from data — a
//     span name built from an identifier would give the trace backend one operation per
//     subscriber.
//   - attrs ...attribute.KeyValue: additional bounded attributes.
//
// Returns:
//   - context.Context: carrying the new span.
//   - trace.Span: the span. The caller must End it, conventionally by defer.
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
//
// span.RecordError is deliberately not used, for the reason adminSpanErrorClass documents: the
// raw cause carries broker and principal detail that must not leave the deployment on a span.
//
// A nil error is a no-op, so a caller can route both outcomes through one deferred call without
// branching.
//
// Parameters:
//   - span trace.Span: the span to mark.
//   - cause error: the failure, or nil when the operation succeeded.
func failKafkaAdminSpan(span trace.Span, cause error) {
	if cause == nil {
		return
	}

	span.SetStatus(codes.Error, adminSpanErrorClass(cause))
}

// hashedSubscriberSpanAttribute renders a subscriber identifier as a bounded span attribute.
//
// Hashed for the reason every subscriber label and log field in this package is: a span attribute
// is exported to whatever tracing backend is configured and is rendered into trace viewers and
// incident tickets, so a registry identifier there is a tenant identifier in a place it cannot be
// recalled from. The hash is the SAME truncated digest the metrics labels and the log fields use,
// so a trace, a metric series and a log line for one subscriber all carry the identical token and
// can be joined.
//
// A nil subscriber or an empty identifier yields the empty string rather than the digest of
// nothing, so "no subject" stays distinguishable from "some subject".
//
// Parameters:
//   - subscriberID string: the registry identifier, possibly empty.
//
// Returns:
//   - attribute.KeyValue: the hashed attribute. Carries "" when there was no identifier.
func hashedSubscriberSpanAttribute(subscriberID string) attribute.KeyValue {
	return attribute.String("blnk.subscriber.id_hash", hashLogIdentifier(subscriberID))
}

// subscriberSpanAttribute is the nil-safe form for the admin operations that take a whole
// subscriber, several of which legitimately receive nil and report it as a named error.
//
// Parameters:
//   - subscriber *model.EventSubscriber: may be nil.
//
// Returns:
//   - attribute.KeyValue: the hashed identifier attribute, empty-valued for a nil subscriber.
func subscriberSpanAttribute(subscriber *model.EventSubscriber) attribute.KeyValue {
	if subscriber == nil {
		return hashedSubscriberSpanAttribute("")
	}

	return hashedSubscriberSpanAttribute(subscriber.SubscriberID)
}
