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
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
)

// This file covers the CONTEXT the bulk-transaction producer captures its event under.
//
// It is one call site of eight, and it is the only one that had no context at all: it
// published under context.Background(), so the one event that says a batch finished —
// or failed — belonged to no trace. Every other producer threads its caller's context
// through the context.WithoutCancel idiom, and the two properties that idiom delivers
// pull in opposite directions, which is why both are asserted here rather than assumed
// from the shape of the call:
//
//   - The VALUES must propagate, or the capture is not attached to the operation.
//   - The CANCELLATION must not, or the capture is abandoned exactly when it matters
//     most — after a rollback that consumed the deadline.
//
// A test that only asserted the first would pass for a bare ctx, which loses events.

// bulkCaptureDatasource records the CONTEXT each outbox insert was issued with,
// alongside the row.
//
// The embedded *mocks.MockDataSource carries no expectations, so any other datasource
// method this path touched would panic rather than pass unnoticed.
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

// InsertEventOutbox records the standalone insert, which the bulk producer now reaches only
// on its FALLBACK path — when the batch has no coordinator record.
func (s *bulkCaptureDatasource) InsertEventOutbox(ctx context.Context, e *model.EventOutbox) error {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.contexts = append(s.contexts, ctx)
	s.rows = append(s.rows, e)

	return nil
}

// FinalizeBulkTransactionBatchWithEvent records the ATOMIC write the bulk producer now
// performs: the coordinator's terminal transition and the outcome event in one
// transaction.
func (s *bulkCaptureDatasource) FinalizeBulkTransactionBatchWithEvent(
	ctx context.Context,
	_ string,
	_ *model.BulkTransactionBatch,
	event *model.EventOutbox,
) (bool, error) {
	s.guard.Lock()
	defer s.guard.Unlock()

	s.contexts = append(s.contexts, ctx)
	s.rows = append(s.rows, event)

	return true, nil
}

// captured returns the single recorded insert, failing the test unless there was
// exactly one.
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

// ---------------------------------------------------------------------------
// Span recording for the whole package. WHY THE IDLE DELEGATE IS THE NOOP PROVIDER.
// ---------------------------------------------------------------------------

// spanSwitchboard is the one TracerProvider this package ever installs globally.
type spanSwitchboard struct {
	embedded.TracerProvider

	mu       sync.Mutex
	delegate trace.TracerProvider
}

// Tracer returns a tracer that resolves through the switchboard on every span it
// starts.
func (s *spanSwitchboard) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	return &switchboardTracer{switchboard: s, name: name, options: options}
}

// register makes recorder the destination for spans until deregister is called.
func (s *spanSwitchboard) register(delegate trace.TracerProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.delegate = delegate
}

// current returns the registered delegate, or the noop provider when none is registered.
func (s *spanSwitchboard) current() trace.TracerProvider {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.delegate == nil {
		return noop.NewTracerProvider()
	}

	return s.delegate
}

// switchboardTracer is one instrumentation scope, resolved late.
type switchboardTracer struct {
	embedded.Tracer

	switchboard *spanSwitchboard
	name        string
	options     []trace.TracerOption
}

// Start resolves the current delegate and starts the span on it.
func (t *switchboardTracer) Start(
	ctx context.Context,
	spanName string,
	options ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	return t.switchboard.current().Tracer(t.name, t.options...).Start(ctx, spanName, options...)
}

var (
	// packageSpanSwitchboard is installed once and never replaced.
	packageSpanSwitchboard     = &spanSwitchboard{}
	packageSpanSwitchboardOnce sync.Once
)

// recordingTracerProvider routes the package's spans into a fresh recorder for the
// duration of the test and returns it.
//
// The tests using it must not call t.Parallel: the registration is process-wide, so two
// concurrent tests would record into each other's recorders.
//
// Returns:
//   - *tracetest.SpanRecorder: the recorder holding every span ended during the test.
func recordingTracerProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	packageSpanSwitchboardOnce.Do(func() {
		otel.SetTracerProvider(packageSpanSwitchboard)
	})

	// Proof rather than assumption. If anything else in the test binary installed a
	// provider first, the switchboard was never delegated to and every assertion below
	// would read an empty recorder — a silent pass on tests that exercise nothing.
	require.Same(t, packageSpanSwitchboard, otel.GetTracerProvider(),
		"the package's span switchboard must be the installed global provider; OpenTelemetry "+
			"delegates the global tracer only ONCE, so a provider installed elsewhere in this test "+
			"binary would leave transaction.go's package-level tracer bound to it and every span "+
			"assertion in this package reading an empty recorder")

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	packageSpanSwitchboard.register(provider)
	t.Cleanup(func() {
		packageSpanSwitchboard.register(nil)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Logf("failed to shut down the recording tracer provider: %v", err)
		}
	})

	return recorder
}

// TestSendBulkTransactionWebhook_CapturesInsideTheCallersTrace is the correlation half.
func TestSendBulkTransactionWebhook_CapturesInsideTheCallersTrace(t *testing.T) {
	recorder := recordingTracerProvider(t)
	blnk, datasource := newBulkCaptureBlnk(t)

	// A caller's operation span, standing in for the rejection and split-recording paths.
	callerCtx, callerSpan := otel.Tracer("blnk-test").Start(context.Background(), "CallersOperation")
	expected := callerSpan.SpanContext().TraceID()
	require.True(t, expected.IsValid(), "the fixture must have a real trace, or nothing below is exercised")

	// The error is checked rather than discarded: the capture is asserted on below, so a
	// producer that failed outright would otherwise be reported as a missing span or a
	// missing row instead of as the failure it was.
	require.NoError(t, blnk.sendBulkTransactionWebhook(callerCtx, "bulk_trace_probe", "applied", "", 3),
		"the capture must succeed, or the spans asserted below describe a failure path instead")
	callerSpan.End()

	insertCtx, row := datasource.captured(t)

	assert.Equal(t, expected, trace.SpanContextFromContext(insertCtx).TraceID(),
		"the capture must run inside the caller's trace; under context.Background() it was an unparented root nothing led to")

	require.NotEmpty(t, recorder.Ended(), "the recorder must have captured the capture's spans")
	for _, span := range recorder.Ended() {
		assert.Equal(t, expected, span.SpanContext().TraceID(),
			"every span the capture produced must belong to the caller's trace, including %q", span.Name())
	}

	// The transport substitution must not have touched the payload contract. The event
	// name is a runtime concatenation and the aggregate is the batch, which is what groups
	// a batch's progress events together.
	assert.Equal(t, "bulk_transaction.applied", row.EventType)
	assert.Equal(t, "bulk_trace_probe", row.AggregateID,
		"the batch id is the aggregate, so a batch's events group and order by the batch they describe")
}

// TestSendBulkTransactionWebhook_CapturesEvenWhenTheCallersContextIsDone is the
// durability half, and the reason a bare ctx would be the wrong fix.
//
// The failure path reaches this producer only AFTER a full batch rollback has run, and
// the rejection path reaches it from an asynq task that may already be draining.
func TestSendBulkTransactionWebhook_CapturesEvenWhenTheCallersContextIsDone(t *testing.T) {
	blnk, datasource := newBulkCaptureBlnk(t)

	callerCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, callerCtx.Err(), "the fixture's context must already be done, or this asserts nothing")

	// The error is checked for the same reason as above, and it carries the point of this
	// test: a cancelled CALLER context must not make the capture fail, because the capture
	// is deliberately detached from it.
	require.NoError(t, blnk.sendBulkTransactionWebhook(callerCtx, "bulk_cancelled_probe", "failed", "rolled back", 0),
		"a caller context that is already done must not make the capture fail; that is the whole point")

	insertCtx, row := datasource.captured(t)

	assert.NoError(t, insertCtx.Err(),
		"the insert must not inherit the caller's cancellation: the batch has already finished, and its outcome must be recorded")
	assert.Nil(t, insertCtx.Done(),
		"and it must be uncancellable rather than merely still live, so a cancellation arriving mid-insert cannot abort it")

	assert.Equal(t, "bulk_transaction.failed", row.EventType)
}
