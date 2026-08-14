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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/model"
)

// TestCaptureTraceContext_RecordsTheActiveTraceAndNothingElse pins the capture half of
// the cross-outbox correlation.
//
// This is the ONLY moment at which the trace can be recorded.
func TestCaptureTraceContext_RecordsTheActiveTraceAndNothingElse(t *testing.T) {
	t.Run("an active span is recorded as a parseable W3C traceparent", func(t *testing.T) {
		recordingTracerProvider(t)

		ctx, span := tracer.Start(context.Background(), "capture-test-request")
		defer span.End()

		traceparent, tracestate := captureTraceContext(ctx)

		require.NotEmpty(t, traceparent, "an active, recorded span must be captured")
		assert.Empty(t, tracestate, "no vendor state was set, so none must be invented")

		// The VALUE is asserted, not merely its presence: a traceparent that did not carry this
		// span's identifiers would correlate the event with something else entirely.
		spanContext := span.SpanContext()
		assert.Contains(t, traceparent, spanContext.TraceID().String(),
			"the captured traceparent must carry the trace id, which is what joins the publish to the request")
		assert.Contains(t, traceparent, spanContext.SpanID().String(),
			"and the span id, which is what names the exact operation that captured the event")
		assert.True(t, strings.HasPrefix(traceparent, "00-"),
			"version 00 is what the W3C propagator emits, and the stored value must be a "+
				"standard traceparent because subscribers and the schema's length bound both assume it")
	})

	t.Run("a context with no span records nothing", func(t *testing.T) {
		traceparent, tracestate := captureTraceContext(context.Background())

		assert.Empty(t, traceparent,
			"an untraced capture is the common case — a CLI mutation, a worker rejection, "+
				"observability disabled — and it must record nothing rather than a placeholder")
		assert.Empty(t, tracestate)
	})

	t.Run("a nil context records nothing rather than panicking", func(t *testing.T) {
		//nolint:staticcheck // A nil context is the point of this assertion.
		traceparent, tracestate := captureTraceContext(nil)

		assert.Empty(t, traceparent)
		assert.Empty(t, tracestate)
	})
}

// TestSanitizeTraceContext_DropsWhatCannotBeStored pins the rule that keeps a request
// header from being able to fail a ledger write.
//
// Both values originate in a caller-supplied HTTP header, and both columns carry a
// length CHECK.
func TestSanitizeTraceContext_DropsWhatCannotBeStored(t *testing.T) {
	const validParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	for name, testCase := range map[string]struct {
		traceparent string
		tracestate  string
		wantParent  string
		wantState   string
	}{
		"a valid pair survives intact": {
			traceparent: validParent, tracestate: "vendor=value",
			wantParent: validParent, wantState: "vendor=value",
		},
		"surrounding whitespace is trimmed": {
			traceparent: "  " + validParent + "\n", tracestate: " vendor=value ",
			wantParent: validParent, wantState: "vendor=value",
		},
		"an empty pair stays empty": {},
		"a whitespace-only traceparent is nothing": {
			traceparent: "   ", tracestate: "vendor=value",
		},
		"an oversized traceparent takes its tracestate with it": {
			traceparent: strings.Repeat("x", model.MaxTraceparentLength+1),
			tracestate:  "vendor=value",
		},
		"a traceparent at the limit is kept": {
			traceparent: strings.Repeat("x", model.MaxTraceparentLength),
			wantParent:  strings.Repeat("x", model.MaxTraceparentLength),
		},
		"an oversized tracestate is dropped alone": {
			traceparent: validParent,
			tracestate:  strings.Repeat("y", model.MaxTracestateLength+1),
			wantParent:  validParent,
		},
		"a tracestate at the limit is kept": {
			traceparent: validParent,
			tracestate:  strings.Repeat("y", model.MaxTracestateLength),
			wantParent:  validParent,
			wantState:   strings.Repeat("y", model.MaxTracestateLength),
		},
		"a tracestate with no traceparent is discarded": {
			tracestate: "vendor=value",
		},
	} {
		t.Run(name, func(t *testing.T) {
			parent, state := model.SanitizeTraceContext(testCase.traceparent, testCase.tracestate)

			assert.Equal(t, testCase.wantParent, parent)
			assert.Equal(t, testCase.wantState, state)
			assert.LessOrEqual(t, len(parent), model.MaxTraceparentLength,
				"nothing over the column's bound may leave this function, or the CHECK constraint "+
					"fires inside a caller's ledger transaction")
			assert.LessOrEqual(t, len(state), model.MaxTracestateLength)
			if parent == "" {
				assert.Empty(t, state, "vendor state cannot outlive the trace identity it describes")
			}
		})
	}
}

// TestCapturedSpanContext_RoundTripsTheStoredValue closes the loop: what capture wrote,
// the publish must be able to read back as the same span.
func TestCapturedSpanContext_RoundTripsTheStoredValue(t *testing.T) {
	t.Run("a captured context is rebuilt as the same span, marked remote", func(t *testing.T) {
		recordingTracerProvider(t)

		ctx, span := tracer.Start(context.Background(), "roundtrip-request")
		original := span.SpanContext()
		traceparent, tracestate := captureTraceContext(ctx)
		span.End()

		rebuilt := capturedSpanContext(traceparent, tracestate)

		require.True(t, rebuilt.IsValid())
		assert.Equal(t, original.TraceID(), rebuilt.TraceID())
		assert.Equal(t, original.SpanID(), rebuilt.SpanID())
		assert.True(t, rebuilt.IsRemote(),
			"the linked span was produced by another operation, and often another process; a "+
				"backend renders a local link differently from a remote one")
	})

	for name, stored := range map[string]string{
		"nothing stored":           "",
		"whitespace only":          "   ",
		"not a traceparent":        "definitely-not-a-traceparent",
		"an all-zero trace id":     "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"an oversized traceparent": strings.Repeat("x", model.MaxTraceparentLength+1),
	} {
		t.Run("an unusable stored value yields an invalid context: "+name, func(t *testing.T) {
			assert.False(t, capturedSpanContext(stored, "").IsValid(),
				"an unusable stored value must produce NO link rather than a link to whatever "+
					"span happens to be in flight, which would look entirely plausible in a trace")
		})
	}
}

// TestLinkToCapturedTrace_LinksRatherThanParents is the assertion the whole design
// rests on, and it is the one that cannot be made by reading the code.
//
// DURATION.
//
// COMPLETENESS.
func TestLinkToCapturedTrace_LinksRatherThanParents(t *testing.T) {
	recorder := recordingTracerProvider(t)

	// The capturing operation, ended before the publish begins — exactly as production does it.
	captureCtx, captureSpan := tracer.Start(context.Background(), "capture-request")
	captured := captureSpan.SpanContext()
	traceparent, tracestate := captureTraceContext(captureCtx)
	captureSpan.End()

	row := model.EventOutbox{Traceparent: traceparent, Tracestate: tracestate}

	// Started from a BACKGROUND context, which is what the relay has: a poll tick, no request.
	_, publishSpan := tracer.Start(context.Background(), "relay.publish", linkToCapturedTrace(row)...)
	publishSpan.End()

	var recorded trace.SpanContext
	var links []sdktrace.Link
	var parent trace.SpanContext
	for _, span := range recorder.Ended() {
		if span.Name() != "relay.publish" {
			continue
		}

		recorded = span.SpanContext()
		links = span.Links()
		parent = span.Parent()
	}

	require.True(t, recorded.IsValid(), "the publish span must have been recorded")

	require.Len(t, links, 1, "the publish must link to exactly the capture that produced the event")
	assert.Equal(t, captured.TraceID(), links[0].SpanContext.TraceID())
	assert.Equal(t, captured.SpanID(), links[0].SpanContext.SpanID())

	assert.NotEqual(t, captured.SpanID(), parent.SpanID(),
		"the publish must NOT be parented by the capture: a trace's duration is its root span's, "+
			"so parenting would report a millisecond request as one lasting the poll interval plus "+
			"every backoff wait")
	assert.NotEqual(t, captured.TraceID(), recorded.TraceID(),
		"and it must be its own trace, so a coalesced transaction's many publishes do not all hang "+
			"off one request span and so nothing is appended to a trace that has already been exported")

	t.Run("a row with no captured trace produces no link", func(t *testing.T) {
		assert.Empty(t, linkToCapturedTrace(model.EventOutbox{}),
			"an untraced capture must yield an unlinked publish span, not a link to nothing")
	})
}

// TestKafkaTraceHeaders_PropagateW3CToSubscribers pins the contract Blnk offers
// OUTWARD.
//
// Blnk does not build subscriber-side consumers — the requirements exclude that
// explicitly — so these headers are the whole of the subscriber-facing tracing story,
// and that is exactly why they have to be standard W3C rather than anything
// Blnk-specific: a subscriber's own instrumentation extracts them without knowing
// anything about Blnk.
func TestKafkaTraceHeaders_PropagateW3CToSubscribers(t *testing.T) {
	t.Run("an active span yields a W3C traceparent header", func(t *testing.T) {
		recordingTracerProvider(t)

		ctx, span := tracer.Start(context.Background(), "header-injection")
		defer span.End()

		headers := kafkaTraceHeaders(ctx)

		require.Len(t, headers, 1, "only the traceparent is set when there is no vendor state")
		assert.Equal(t, traceparentHeader, headers[0].Key,
			"the key must be the standard lowercase W3C name, because a subscriber's own "+
				"instrumentation looks for exactly that")
		assert.Contains(t, string(headers[0].Value), span.SpanContext().TraceID().String())
		assert.Contains(t, string(headers[0].Value), span.SpanContext().SpanID().String(),
			"the header must name the PUBLISH span, so a subscriber attaches to the publish it "+
				"consumed rather than to the capture")
	})

	t.Run("a tracestate is propagated after the traceparent, in that order", func(t *testing.T) {
		recordingTracerProvider(t)

		// A remote context carrying vendor state, which is how a real inbound request arrives.
		remote := trace.ContextWithSpanContext(context.Background(), capturedSpanContext(
			"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"vendor=value",
		))

		headers := kafkaTraceHeaders(remote)

		require.Len(t, headers, 2)
		assert.Equal(t, []string{traceparentHeader, tracestateHeader},
			[]string{headers[0].Key, headers[1].Key},
			"the order must be fixed, because a randomised header order makes two publishes of one "+
				"event differ for no reason a reader of a byte comparison could explain")
		assert.Equal(t, "vendor=value", string(headers[1].Value))
	})

	t.Run("no trace yields no headers at all", func(t *testing.T) {
		assert.Nil(t, kafkaTraceHeaders(context.Background()),
			"an EMPTY header is worse than an absent one: a consumer that extracts it gets an "+
				"invalid span context and either drops the trace or starts a broken one")
		//nolint:staticcheck // A nil context is the point of this assertion.
		assert.Nil(t, kafkaTraceHeaders(nil))
	})
}

// TestPublishSpanErrorClass_IsAClosedVocabulary pins that a raw broker error can never
// reach a span's status description.
//
// A span status leaves the process.
func TestPublishSpanErrorClass_IsAClosedVocabulary(t *testing.T) {
	const leaky = "dial tcp 10.4.2.17:9092: connect: connection refused (broker-3.internal)"

	for name, testCase := range map[string]struct {
		cause error
		want  string
	}{
		"no cause":           {cause: nil, want: publishSpanErrorUnknown},
		"a closed publisher": {cause: ErrEventPublisherClosed, want: publishSpanErrorPublisherState},
		"a refused topic":    {cause: ErrTopicNotOwned, want: publishSpanErrorTopicRefused},
		"an oversized event": {cause: ErrEventMessageTooLarge, want: publishSpanErrorMessageTooBig},
		"a wrapped sentinel": {cause: fmt.Errorf("publishing: %w", ErrTopicNotOwned), want: publishSpanErrorTopicRefused},
		"anything else":      {cause: errors.New(leaky), want: publishSpanErrorBroker},
	} {
		t.Run(name, func(t *testing.T) {
			got := publishSpanErrorClass(testCase.cause)

			assert.Equal(t, testCase.want, got)
			assert.NotContains(t, got, "10.4.2.17",
				"a broker address must never reach a span status; the raw cause belongs on the "+
					"outbox row and in the log, which stay inside the deployment")
			assert.NotContains(t, got, "broker-3.internal")
		})
	}
}

// TestAdminSpanErrorClass_SeparatesTheConditionsThatNeedDifferentActions pins the admin
// side of the same rule, and pins that the three sentinel conditions stay
// distinguishable.
func TestAdminSpanErrorClass_SeparatesTheConditionsThatNeedDifferentActions(t *testing.T) {
	classes := map[error]string{
		nil:                            adminSpanErrorUnknown,
		ErrKafkaAdminNotConfigured:     adminSpanErrorNotConfigured,
		ErrAuthorizerNotEnforcing:      adminSpanErrorAuthorizer,
		ErrPartitionGrowthRefused:      adminSpanErrorGeometry,
		ErrReplicationFactorInadequate: adminSpanErrorGeometry,
		errors.New("sasl authentication to broker-1.internal:9092 failed"): adminSpanErrorBroker,
	}

	seen := map[string]struct{}{}
	for cause, want := range classes {
		got := adminSpanErrorClass(cause)

		assert.Equal(t, want, got, "cause %v", cause)
		assert.NotContains(t, got, "broker-1.internal",
			"a broker address must never reach a span status")
		seen[got] = struct{}{}
	}

	// FIVE distinct classes out of six inputs — the two geometry failures share one class
	// deliberately, because the remedy is the same: fix the topic's partitioning or replication.
	assert.Len(t, seen, 5,
		"the sentinel conditions must stay distinguishable; a non-enforcing authorizer voids every "+
			"isolation guarantee and must not read like a broker that is merely unconfigured")
}

// TestPublishToTopic_ProducesALinkedSpanAndTraceHeadersOnTheWire is the end-to-end
// assertion: everything above is checked against the real publish path and the record
// the transport saw.
//
// It also pins the one thing the trace must NOT affect: the message VALUE.
func TestPublishToTopic_ProducesALinkedSpanAndTraceHeadersOnTheWire(t *testing.T) {
	recorder := recordingTracerProvider(t)

	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	// The capture, ended before the publish — the production shape, not a convenience.
	captureCtx, captureSpan := tracer.Start(context.Background(), "capture-request")
	captured := captureSpan.SpanContext()
	traceparent, tracestate := captureTraceContext(captureCtx)
	captureSpan.End()
	require.NotEmpty(t, traceparent)

	event := publisherEvent(model.EventTypeTransactionApplied, "txn_tracing_wire", publisherPayload)
	result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
		Event:       event,
		Topic:       TopicForEvent(event.EventType),
		Key:         publisherLedgerID,
		Purpose:     PublishPurposeOriginal,
		Attempt:     1,
		MaxAttempts: 5,
		Traceparent: traceparent,
		Tracestate:  tracestate,
	})
	require.NoError(t, err)
	require.True(t, result.Dispatched())

	_, record := transport.onlyRecord(t)

	t.Run("the record carries a W3C traceparent naming the publish span", func(t *testing.T) {
		value, present := record.headerValue(traceparentHeader)
		require.True(t, present,
			"without this header a subscriber cannot continue the trace, which is the whole of the "+
				"subscriber-facing tracing contract Blnk offers")
		assert.True(t, strings.HasPrefix(value, "00-"), "it must be a standard traceparent")

		// The PUBLISH span's identity, not the capture's: a subscriber attaches to the
		// publish it consumed, and the publish is in turn linked to the capture, so the whole
		// chain is reachable while every parent reflects real causality.
		publish := publishedSpan(t, recorder)
		assert.Contains(t, value, publish.SpanContext().TraceID().String())
		assert.Contains(t, value, publish.SpanContext().SpanID().String())
	})

	t.Run("the producer span links to the capture without being parented by it", func(t *testing.T) {
		publish := publishedSpan(t, recorder)

		require.Len(t, publish.Links(), 1, "the publish must name the capture that produced the event")
		assert.Equal(t, captured.TraceID(), publish.Links()[0].SpanContext.TraceID())
		assert.Equal(t, captured.SpanID(), publish.Links()[0].SpanContext.SpanID())
		assert.NotEqual(t, captured.SpanID(), publish.Parent().SpanID(),
			"parenting would stretch the capturing request's duration across the poll interval and "+
				"every backoff wait")
		assert.Equal(t, trace.SpanKindProducer, publish.SpanKind(),
			"a produce is a producer span; the admin operations are the client ones")
	})

	t.Run("the span carries bounded messaging attributes and no raw identifiers", func(t *testing.T) {
		attributes := spanAttributeMap(publishedSpan(t, recorder))

		assert.Equal(t, "kafka", attributes["messaging.system"])
		assert.Equal(t, "publish", attributes["messaging.operation"])
		assert.Equal(t, "blnk.transactions", attributes["messaging.destination.name"])
		assert.Equal(t, event.EventID, attributes["messaging.message.id"],
			"the event id is Blnk-generated, is the subscriber idempotency key, and is what an "+
				"operator searches by, so it is carried in full — matching the capture span")

		// THE PARTITION KEY IS HASHED. It is a ledger, balance or identity identifier, and a span
		// attribute is exported and rendered into incident tickets exactly as a metric label is.
		assert.NotContains(t, attributes["messaging.kafka.message.key_hash"], publisherLedgerID,
			"a span attribute must not carry a raw ledger identifier")
		assert.Equal(t, hashLogIdentifier(publisherLedgerID),
			attributes["messaging.kafka.message.key_hash"],
			"and the hash must be the SAME token the metric labels and the log fields use, or the "+
				"three cannot be joined for one aggregate")
	})

	t.Run("the message value is untouched by tracing", func(t *testing.T) {
		// The dual-delivery guarantee, and the trace travels as headers, so the bytes compared by
		// the dual-delivery and replay assertions cannot be perturbed by it.
		assert.NotContains(t, string(record.Value), "traceparent")
		assert.NotContains(t, string(record.Value), traceparent)
	})

	t.Run("an event captured untraced still propagates, but links to nothing", func(t *testing.T) {
		// THE DISTINCTION THAT MATTERS, and it is easy to get backwards.
		untracedTransport := newPublisherFakeTransport()
		untracedPublisher := publisherWithFakeTransport(t, untracedTransport)

		before := len(recorder.Ended())

		untraced := publisherEvent(model.EventTypeTransactionApplied, "txn_untraced", publisherPayload)
		_, publishErr := untracedPublisher.PublishToTopic(context.Background(), PublishRequest{
			Event: untraced,
			Topic: TopicForEvent(untraced.EventType),
			Key:   publisherLedgerID,
		})
		require.NoError(t, publishErr)

		_, untracedRecord := untracedTransport.onlyRecord(t)
		_, present := untracedRecord.headerValue(traceparentHeader)
		assert.True(t, present,
			"the publish is itself traced, so the record must still carry the publish's own context; "+
				"withholding it would leave a subscriber unable to trace anything at all for events "+
				"that happened to be captured outside a request")

		var unlinked sdktrace.ReadOnlySpan
		for _, span := range recorder.Ended()[before:] {
			if span.SpanKind() == trace.SpanKindProducer {
				unlinked = span
			}
		}
		require.NotNil(t, unlinked, "the untraced publish must still have produced a producer span")
		assert.Empty(t, unlinked.Links(),
			"there was no capturing request, so there is nothing to link to and none must be invented")
	})
}

// publishedSpan returns the single recorded producer span, failing the test when there
// is not exactly one.
func publishedSpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	var found []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.SpanKind() == trace.SpanKindProducer {
			found = append(found, span)
		}
	}

	require.Len(t, found, 1, "exactly one producer span must have been recorded")

	return found[0]
}

// spanAttributeMap flattens a recorded span's attributes into a map for assertion.
//
// Values are rendered with AsString because every attribute this pipeline sets on a
// span is a string or an integer, and comparing rendered values keeps the assertions
// readable without a type switch per attribute.
func spanAttributeMap(span sdktrace.ReadOnlySpan) map[string]string {
	attributes := make(map[string]string, len(span.Attributes()))
	for _, kv := range span.Attributes() {
		attributes[string(kv.Key)] = kv.Value.AsString()
	}

	return attributes
}

// TestPrepareEventOutbox_RecordsTheCapturingTraceOnTheRow is the assertion that the
// trace is written down at the ONLY moment it can be.
//
// Everything downstream — the link, the header, the whole correlation — depends on this
// one write.
func TestPrepareEventOutbox_RecordsTheCapturingTraceOnTheRow(t *testing.T) {
	recordingTracerProvider(t)

	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	t.Run("a capture inside a traced request records that trace", func(t *testing.T) {
		ctx, span := tracer.Start(context.Background(), "POST /transactions")
		defer span.End()

		row, err := blnk.PrepareEventOutbox(ctx, NewWebhook{
			Event:   model.EventTypeTransactionApplied,
			Payload: map[string]interface{}{"transaction_id": "txn_traced_capture"},
		})
		require.NoError(t, err)
		require.NotNil(t, row)

		require.NotEmpty(t, row.Traceparent,
			"a row captured inside a request must carry that request's trace; nothing downstream can "+
				"recover it afterwards, which is why every request trace used to end at the outbox")
		assert.Contains(t, row.Traceparent, span.SpanContext().TraceID().String())
		assert.LessOrEqual(t, len(row.Traceparent), model.MaxTraceparentLength,
			"the stored value must be inside the column's bound before it ever reaches the INSERT")

		// OUTSIDE THE ENVELOPE. The stored canonical bytes are what a replay republishes and
		// what the dual-delivery comparison reads, so a trace member in there would make an
		// original and its replay differ by construction.
		assert.NotContains(t, string(row.EventRaw), "traceparent",
			"the canonical envelope must not carry the trace: it is the basis of the byte-equality "+
				"guarantees, and a per-request member would change the bytes of every event")
		assert.NotContains(t, string(row.EventRaw), row.Traceparent)
	})

	t.Run("a capture outside any request records no trace", func(t *testing.T) {
		row, err := blnk.PrepareEventOutbox(context.Background(), NewWebhook{
			Event:   model.EventTypeTransactionApplied,
			Payload: map[string]interface{}{"transaction_id": "txn_untraced_capture"},
		})
		require.NoError(t, err)
		require.NotNil(t, row)

		assert.Empty(t, row.Traceparent,
			"a CLI mutation, a worker-initiated rejection or a deployment with observability off all "+
				"capture without a trace, and that must record nothing rather than a placeholder")
		assert.Empty(t, row.Tracestate)
	})
}

// TestPublishRequestFromOutbox_CarriesTheStoredTraceToThePublish pins the one hand-off
// between the stored row and the publish.
func TestPublishRequestFromOutbox_CarriesTheStoredTraceToThePublish(t *testing.T) {
	const (
		storedParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		storedState  = "vendor=value"
	)

	request := PublishRequestFromOutbox(model.EventOutbox{
		EventID:     "11111111-1111-4111-8111-111111111111",
		EventType:   model.EventTypeTransactionApplied,
		AggregateID: "txn_conversion",
		Topic:       TopicForEvent(model.EventTypeTransactionApplied),
		Traceparent: storedParent,
		Tracestate:  storedState,
	}, 1)

	assert.Equal(t, storedParent, request.Traceparent)
	assert.Equal(t, storedState, request.Tracestate)

	// And the conversion is what makes the link reachable, so the round trip is asserted through
	// the same helper the publisher uses rather than by inspecting the strings again.
	require.Len(t, traceRequestLinks(request), 1,
		"the converted request must produce a link, or the relay, the dead-letter write and every "+
			"replay all publish unlinked")
}

// TestKafkaAdminOperations_ProduceClientSpansWithoutLeakingCauses pins the admin half
// of F14.
//
// They are the operations that CHANGE the broker on behalf of an authenticated caller:
// creating a topic, minting or revoking a SCRAM credential, granting or pruning ACLs.
func TestKafkaAdminOperations_ProduceClientSpansWithoutLeakingCauses(t *testing.T) {
	t.Run("a successful topic assurance produces one client span", func(t *testing.T) {
		recorder := recordingTracerProvider(t)
		storeKafkaTopicPrefix(t, "")

		admin := newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 3)
		_, err := admin.EnsureTopics(context.Background())
		require.NoError(t, err)

		span := adminSpan(t, recorder, "kafka.admin ensure_topics")
		assert.Equal(t, trace.SpanKindClient, span.SpanKind(),
			"an admin call is a client call to the broker's admin API, not a produce")

		attributes := spanAttributeMap(span)
		assert.Equal(t, "kafka", attributes["messaging.system"])
		assert.Equal(t, "ensure_topics", attributes["blnk.kafka.admin.operation"])
		assert.Equal(t, codes.Unset, span.Status().Code, "a successful operation must not be marked failed")
	})

	t.Run("a subscriber operation carries the pseudonym, never the identifier", func(t *testing.T) {
		recorder := recordingTracerProvider(t)

		subscriber := &model.EventSubscriber{
			SubscriberID:     "sub_0f6e2c8a1b944106b4d6793e838afcbf",
			ConsumerGroupID:  "blnk-grp-0f6e2c8a1b944106b4d6793e838afcbf",
			KafkaPrincipal:   "blnk-sub-0f6e2c8a1b944106b4d6793e838afcbf",
			AuthorizedTopics: []string{TopicForEvent(model.EventTypeTransactionApplied)},
		}

		admin := newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 1)
		_, err := admin.GrantSubscriberAccess(context.Background(), subscriber)
		require.NoError(t, err)

		attributes := spanAttributeMap(adminSpan(t, recorder, "kafka.admin grant_subscriber_access"))

		assert.Equal(t, hashLogIdentifier(subscriber.SubscriberID), attributes["blnk.subscriber.id_hash"],
			"the pseudonym must be the SAME token the metric labels and the log fields carry, or a "+
				"trace, a series and a log line for one subscriber cannot be joined")
		for key, value := range attributes {
			assert.NotContains(t, value, subscriber.SubscriberID,
				"attribute %q must not carry the raw registry identifier", key)
			assert.NotContains(t, value, subscriber.KafkaPrincipal,
				"attribute %q must not carry the raw broker principal", key)
		}
	})

	t.Run("a failed operation is marked with a class, never with the cause", func(t *testing.T) {
		recorder := recordingTracerProvider(t)

		// A client that is not configured, which is the one admin failure reachable without a
		// broker — and it is also the one that is often a legitimate steady state, which is
		// why it has a class of its own rather than reading like a broker fault.
		unconfigured := &KafkaAdminClient{}
		_, err := unconfigured.EnsureTopics(context.Background())
		require.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

		span := adminSpan(t, recorder, "kafka.admin ensure_topics")
		assert.Equal(t, codes.Error, span.Status().Code, "the failure must be visible on the span")
		assert.Equal(t, adminSpanErrorNotConfigured, span.Status().Description,
			"the description must be the bounded class; a raw kafka-go cause would export broker "+
				"hostnames and listener addresses to whatever backend is configured")
		assert.Empty(t, span.Events(),
			"span.RecordError is deliberately not used: it would attach the full cause as an event, "+
				"which is the same leak by another route")
	})
}

// adminSpan returns the single recorded span with the given name, failing the test
// otherwise.
func adminSpan(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()

	var found []sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			found = append(found, span)
		}
	}

	require.Len(t, found, 1, "exactly one span named %q must have been recorded", name)

	return found[0]
}
