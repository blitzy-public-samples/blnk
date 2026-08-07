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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

// This file covers the CONTEXT the bulk-transaction producer captures its event under.
//
// It is one call site of eight, and it is the only one that had no context at all: it
// published under context.Background(), so the one event that says a batch finished — or
// failed — belonged to no trace. Every other producer threads its caller's context through
// the context.WithoutCancel idiom, and the two properties that idiom delivers pull in
// opposite directions, which is why both are asserted here rather than assumed from the
// shape of the call:
//
//   - The VALUES must propagate, or the capture is not attached to the operation.
//   - The CANCELLATION must not, or the capture is abandoned exactly when it matters most —
//     after a rollback that consumed the deadline.
//
// A test that only asserted the first would pass for a bare ctx, which loses events. A test
// that only asserted the second would pass for context.Background(), which is what was
// there. Neither alone is the fix.

// bulkCaptureDatasource records the CONTEXT each outbox insert was issued with, alongside the
// row.
//
// The context is the subject here, which is why event_outbox_test.go's outboxSpyDatasource
// will not do: it discards the context, because its own subject is which insert path was
// taken and with which transaction. Recording it is safe — nothing is done with it but
// inspection — and it is the only way to observe what the producer actually handed the
// repository.
//
// The embedded *mocks.MockDataSource carries no expectations, so any other datasource method
// this path touched would panic rather than pass unnoticed.
type bulkCaptureDatasource struct {
	*mocks.MockDataSource

	guard    sync.Mutex
	contexts []context.Context
	rows     []*model.EventOutbox
}

// newBulkCaptureDatasource returns a datasource that records inserts for inspection.
func newBulkCaptureDatasource() *bulkCaptureDatasource {
	return &bulkCaptureDatasource{MockDataSource: new(mocks.MockDataSource)}
}

// InsertEventOutbox records the standalone insert the bulk producer performs.
func (s *bulkCaptureDatasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.contexts = append(s.contexts, ctx)
	s.rows = append(s.rows, e)

	return nil
}

// captured returns the single recorded insert, failing the test unless there was exactly one.
//
// Insisting on exactly one is part of the assertion rather than convenience: a producer that
// captured an event twice would be publishing a duplicate that the outbox's unique event id
// would then reject on the second attempt, and reading only the first would hide it.
//
// Returns:
//   - context.Context: the context the insert was issued with.
//   - *model.EventOutbox: the captured row.
func (s *bulkCaptureDatasource) captured(t *testing.T) (context.Context, *model.EventOutbox) {
	t.Helper()

	s.guard.Lock()
	defer s.guard.Unlock()

	require.Len(t, s.rows, 1, "the bulk producer must capture exactly one event")

	return s.contexts[0], s.rows[0]
}

// newBulkCaptureBlnk builds the minimal *Blnk the bulk producer needs: a publishing
// configuration and a datasource to capture into.
//
// Returns:
//   - *Blnk: the instance to publish through.
//   - *bulkCaptureDatasource: the recorder.
func newBulkCaptureBlnk(t *testing.T) (*Blnk, *bulkCaptureDatasource) {
	t.Helper()

	datasource := newBulkCaptureDatasource()

	return newOutboxBlnk(t, outboxPublishingConfiguration(), datasource), datasource
}

// recordingTracerProvider installs a span-recording provider for the test and returns the
// recorder.
//
// otel.SetTracerProvider is process-global and the package's tracer resolves through it, so
// the previous provider is restored — and the recording one shut down — through t.Cleanup
// rather than a defer, so a require failure cannot leave the package tracing into a provider
// that has been torn down.
//
// Returns:
//   - *tracetest.SpanRecorder: the recorder holding every span ended during the test.
func recordingTracerProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Logf("failed to shut down the recording tracer provider: %v", err)
		}
	})

	return recorder
}

// TestSendBulkTransactionWebhook_CapturesInsideTheCallersTrace is the correlation half.
//
// Two of the four paths that reach this producer arrive from a live traced operation —
// transaction_rejection.go rejecting an atomic member, and transaction_queue.go failing to
// record a split transaction — and both hold a context carrying that operation's span.
// Publishing under context.Background() discarded it, so the event describing the batch
// outcome appeared as an unparented root span with nothing linking it to the batch. That is
// the one event an operator goes looking for when a batch fails, and it was the one event the
// trace could not lead them to.
//
// The trace id is asserted rather than the parent span id, because the chain from here to the
// repository passes through a span of its own (publishEvent starts "PublishEvent") and
// asserting a direct parent would freeze that intermediate structure. The trace id is what
// makes the capture findable from the operation, and it is invariant to how many spans lie
// between.
func TestSendBulkTransactionWebhook_CapturesInsideTheCallersTrace(t *testing.T) {
	recorder := recordingTracerProvider(t)
	blnk, datasource := newBulkCaptureBlnk(t)

	// A caller's operation span, standing in for the rejection and split-recording paths.
	callerCtx, callerSpan := otel.Tracer("blnk-test").Start(context.Background(), "CallersOperation")
	expected := callerSpan.SpanContext().TraceID()
	require.True(t, expected.IsValid(), "the fixture must have a real trace, or nothing below is exercised")

	blnk.sendBulkTransactionWebhook(callerCtx, "bulk_trace_probe", "applied", "", 3)
	callerSpan.End()

	insertCtx, row := datasource.captured(t)

	assert.Equal(t, expected, trace.SpanContextFromContext(insertCtx).TraceID(),
		"the capture must run inside the caller's trace; under context.Background() it was an unparented root nothing led to")

	require.NotEmpty(t, recorder.Ended(), "the recorder must have captured the capture's spans")
	for _, span := range recorder.Ended() {
		assert.Equal(t, expected, span.SpanContext().TraceID(),
			"every span the capture produced must belong to the caller's trace, including %q", span.Name())
	}

	// The transport substitution must not have touched the payload contract. The event name
	// is a runtime concatenation and the aggregate is the batch, which is what groups a
	// batch's progress events together.
	assert.Equal(t, "bulk_transaction.applied", row.EventType)
	assert.Equal(t, "bulk_trace_probe", row.AggregateID,
		"the batch id is the aggregate, so a batch's events group and order by the batch they describe")
}

// TestSendBulkTransactionWebhook_CapturesEvenWhenTheCallersContextIsDone is the durability
// half, and the reason a bare ctx would be the wrong fix.
//
// The failure path reaches this producer only AFTER a full batch rollback has run, and the
// rejection path reaches it from an asynq task that may already be draining. So the context
// it is handed can be, and routinely is, already done. PublishEvent issues the outbox insert
// with the context it is given, so inheriting that cancellation would abort the insert and
// lose the event — with nothing but a log line to show it, which is the single failure mode
// the outbox exists to rule out.
//
// The row is asserted captured, and the insert's context asserted uncancellable rather than
// merely not-yet-cancelled: context.WithoutCancel yields a context with no Done channel at
// all, so a cancellation arriving mid-insert cannot abort it either. A context that simply
// happened to be live when the insert began would satisfy a weaker assertion and still lose
// the row to a race.
func TestSendBulkTransactionWebhook_CapturesEvenWhenTheCallersContextIsDone(t *testing.T) {
	blnk, datasource := newBulkCaptureBlnk(t)

	callerCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, callerCtx.Err(), "the fixture's context must already be done, or this asserts nothing")

	blnk.sendBulkTransactionWebhook(callerCtx, "bulk_cancelled_probe", "failed", "rolled back", 0)

	insertCtx, row := datasource.captured(t)

	assert.NoError(t, insertCtx.Err(),
		"the insert must not inherit the caller's cancellation: the batch has already finished, and its outcome must be recorded")
	assert.Nil(t, insertCtx.Done(),
		"and it must be uncancellable rather than merely still live, so a cancellation arriving mid-insert cannot abort it")

	assert.Equal(t, "bulk_transaction.failed", row.EventType)
}
