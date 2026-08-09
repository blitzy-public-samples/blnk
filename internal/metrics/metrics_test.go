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

package metrics

// This is the first test file for package metrics. It exists to guard a specific,
// easy-to-break structural property of the package rather than to exercise
// business logic — there is none here.
//
// metrics.go is deliberately flat: one package-level meter, an init() that calls
// Init() and log.Fatalf's if it returns an error, a flat set of exported instrument
// variables, and an Init() that assigns every one of them in declaration order and
// returns on the first error. Two consequences follow, and both are what the tests
// below assert:
//
//  1. Every instrument is constructed at package-IMPORT time. A meter that
//     rejected an instrument would therefore kill the process at load, and it
//     would take down every package that imports this one — the API server, the
//     workers, the CLI. "The test binary ran at all" is itself evidence that
//     construction succeeded; these tests turn that implicit evidence into
//     explicit assertions with failure messages that name the culprit.
//
//  2. A declaration whose matching assignment block is missing from Init() stays
//     nil forever. Nothing catches that: the variable exists, the package builds
//     cleanly, and the first production call site panics on a nil interface. The
//     non-nil tests are the only mechanical defence against such an orphaned
//     declaration — precisely the mistake that appending new instruments to an
//     existing list invites.
//
//     The two inventory functions below, preExistingInstruments and
//     eventStreamingInstruments, ARE that defence, and they are hand-maintained: an
//     instrument absent from both has no orphan guard at all. Adding an instrument
//     to metrics.go therefore means adding it to one of them.
//
// The tests are in package metrics (an internal test package) rather than
// metrics_test, so the unexported meter every instrument is built from is in
// scope and can be asserted on directly.
//
// Scope note: instrument names are asserted here only indirectly, through Init()
// returning a nil error. The instrument-name-to-Prometheus-series transform is the
// exporter's behaviour, not this package's, and is verified out-of-process against
// real /metrics output. Asserting a guessed series name here would produce a test
// that passes while the endpoint disagrees.
//
// None of these tests calls t.Parallel(). Init() writes the package-level
// instrument variables that the other tests read, and CI runs the suite under the
// race detector, so the tests are kept strictly serial.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Compile-time instrument-kind guards for the event-streaming instruments.
//
// DO NOT DELETE THESE AS "UNUSED". They are pure static type checks and they are
// the primary defence against an instrument-kind mix-up. Each metric.* kind embeds
// a distinct unexported marker interface — embedded.Int64Gauge declares
// int64Gauge(), embedded.Int64Histogram declares int64Histogram(), and so on — so
// no two kinds are mutually assignable, not even Int64Gauge and Int64Histogram,
// which expose an identical Record(context.Context, int64, ...RecordOption)
// method. Changing any declaration in metrics.go to the wrong kind therefore fails
// to COMPILE right here, long before a test could run and long before a dashboard
// silently renders a counter as a gauge.
//
// These are value assignments rather than type assertions on purpose: they check
// the declared static type and never touch the runtime value, so they hold whether
// or not Init() has run.
var (
	_ metric.Int64Counter     = EventsPublishedTotal
	_ metric.Int64Counter     = EventBrokerAcknowledgementsTotal
	_ metric.Int64Counter     = EventsDispatchedTotal
	_ metric.Int64Counter     = EventPublishAttemptsTotal
	_ metric.Float64Histogram = EventPublishDuration
	_ metric.Float64Histogram = EventCaptureToDispatchDuration
	_ metric.Int64Counter     = EventsDeadLetteredTotal
	_ metric.Float64Gauge     = DLTOldestMessageAgeSeconds
	_ metric.Int64Gauge       = OutboxPendingBacklog

	// The two lag instruments are ASYNCHRONOUS, and the distinction this guard enforces
	// is the whole cardinality fix rather than a stylistic preference. A synchronous
	// Int64Gauge is a write whose aggregator retains every attribute set it has ever
	// been written with, and it has no delete — so with a dynamic label set, subscriber
	// churn grows the series count without bound and no zero written afterwards can
	// retire a series. An Int64ObservableGauge exports exactly what its callback observes
	// per collection, so the series set IS the current inventory. The two kinds are not
	// mutually assignable, so reverting either declaration fails to compile here.
	_ metric.Int64ObservableGauge = SubscriberConsumerLag
	_ metric.Int64ObservableGauge = ConsumerLagUnmeasuredPartitions

	// The COLLECTION-HEALTH pair is asynchronous for a different reason from the lag
	// gauges, and it is the reason the kind matters most here: their subject is the
	// collector itself, which may have STOPPED. A synchronous gauge can only be written by
	// the component being observed, so a stalled collector would freeze its own freshness
	// gauge at whatever age it last reported and the series would read as permanently
	// current — the single failure these two instruments exist to make visible. Observed
	// from a stored timestamp at collection time, the age instead rises on every scrape
	// with no participation from the stalled component.
	_ metric.Float64ObservableGauge = EventMetricsLastCollectionAgeSeconds
	_ metric.Float64ObservableGauge = EventMetricsLastSuccessAgeSeconds

	_ metric.Int64Counter = EventMetricsCollectionFailuresTotal

	// Synchronous, deliberately: both describe the last SWEEP and each has exactly one
	// series (the unmeasured gauge's reason domain is closed and every value is written
	// every tick), so there is no churn for an observable gauge to bound.
	_ metric.Int64Gauge = ConsumerLagInventoryComplete
	_ metric.Int64Gauge = SubscribersUnmeasured
)

// publishOutcomes is the complete vocabulary of the "outcome" attribute carried by
// EventPublishAttemptsTotal and EventPublishDuration, matching the documented
// attribute list on their declarations in metrics.go.
//
// Note the UNDERSCORE in dead_lettered. Surrounding prose spells the concept
// "dead-lettered" with a hyphen; that is not the attribute value, and a metric
// consumer filtering on the hyphenated spelling would match nothing.
//
// The vocabulary is THREE values. "failed" is NOT one of them: a fourth outcome once
// carried "this attempt will not be retried", and it was removed because it widened
// the three-value publish-status vocabulary that model.PublishStatus, the API
// responses and docs/event-streaming.md all state. That fact is now reported on the
// separate `terminal` dimension below, so the three values remain mutually exclusive
// and exhaustive and their sum is still the total number of attempted writes.
//
// Spelled as literals deliberately: this test file stays as dependency-free as the
// package it covers, so it does not import the model package merely to obtain three
// strings.
var publishOutcomes = []string{"dispatched", "retrying", "dead_lettered"}

// publishTerminalValues is the closed domain of the "terminal" attribute that
// accompanies the outcome on EventPublishAttemptsTotal.
//
// It exists because the outcome vocabulary is frozen at three values, which makes a
// failure that will be retried and one that never will BOTH outcome="retrying". This
// dimension is what keeps "how many events are actually stuck" answerable —
// {outcome="retrying",terminal="true"} — and it is the reason the attempts counter's
// series count is the PRODUCT of two domains rather than the outcome domain alone.
//
// Two literals and no more. The values are the strings the publisher emits, not Go
// bools rendered by fmt, so that a query written against the exporter matches.
var publishTerminalValues = []string{"true", "false"}

// maxRelayRetryAttempts is the relay's attempt budget CEILING
// (config.MaxRelayRetryAttempts, which RELAY_MAX_RETRY_ATTEMPTS is clamped to),
// bounding the numeric values the "attempt" attribute can take: 1 through 5
// inclusive, rendered as strings. Mirrored as a literal for the same reason as
// publishOutcomes — this package reads no configuration, and neither does its test.
const maxRelayRetryAttempts = 5

// publishAttemptLabels is the COMPLETE, closed domain of the "attempt" attribute:
// the five numeric attempts, one overflow bucket, and one fixed token for each of the
// two publishes that are not part of a retry sequence.
//
// The domain has to be closed because this attribute is carried by a histogram, whose
// series count is its label cardinality multiplied by its bucket count. "over" is what
// a row whose per-row max_attempts was raised directly in the database collapses into,
// and "replay" and "dead_letter" keep operator-triggered replays and dead-letter writes
// out of the attempt="1" population the latency target is read from — rather than
// extending the numeric domain past the budget, which is what an attempt-count label
// on a replay would do.
var publishAttemptLabels = []string{"1", "2", "3", "4", "5", "over", "replay", "dead_letter"}

// namedInstrument pairs an instrument with the name of the variable holding it, so
// that a table-driven failure names the exact declaration at fault instead of
// reporting an anonymous slice index.
type namedInstrument struct {
	name  string
	value any
}

// eventStreamingInstruments returns EVERY instrument added for the Kafka
// event-publishing pipeline, each labelled with its variable name.
//
// It is exhaustive by intent, and the count is deliberately not stated in this comment: a
// number here would go stale the first time an instrument was added, and the assertions that
// matter — that every declared instrument is assigned in Init() and exported under the name an
// alert rule matches — are only true if this list is complete. An instrument omitted here is one
// nothing checks, which is the exact failure mode the event pipeline has already hit twice:
// gauges declared, initialised and never recorded, leaving a rule permanently inactive with
// healthy-looking rule health.
//
// This is a FUNCTION and not a package-level table for a load-bearing reason. Go
// initialises every package-level variable before it runs any init() function, and
// metrics.go declares its instruments with no initialiser — they are assigned
// inside Init(), which init() calls. A package-level table would therefore capture
// the pre-init() zero values and freeze nil into the fixtures, making every
// non-nil assertion either fail spuriously or assert nothing at all. Reading the
// variables inside a function body defers the read until the test runs, which is
// after all package initialisation has completed.
func eventStreamingInstruments() []namedInstrument {
	return []namedInstrument{
		{"EventsPublishedTotal", EventsPublishedTotal},
		{"EventsDispatchedTotal", EventsDispatchedTotal},
		{"EventPublishAttemptsTotal", EventPublishAttemptsTotal},
		{"EventPublishDuration", EventPublishDuration},
		{"EventCaptureToDispatchDuration", EventCaptureToDispatchDuration},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds},
		{"SubscriberConsumerLag", SubscriberConsumerLag},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions},
		{"OutboxPendingBacklog", OutboxPendingBacklog},
		{"EventsPurgedTotal", EventsPurgedTotal},
		{"SubscriberRevocationsPending", SubscriberRevocationsPending},
		{"OldestSubscriberRevocationAgeSeconds", OldestSubscriberRevocationAgeSeconds},
		{"SubscriberSettlementOutstanding", SubscriberSettlementOutstanding},
		{"SubscriberGrantReconcilePending", SubscriberGrantReconcilePending},
		{"SubscriberCredentialCleanupPending", SubscriberCredentialCleanupPending},
		{"OldestSubscriberSettlementAgeSeconds", OldestSubscriberSettlementAgeSeconds},
		{"SubscriberObligationsSettledTotal", SubscriberObligationsSettledTotal},
		{"EventBrokerAcknowledgementsTotal", EventBrokerAcknowledgementsTotal},
		{"SubscriberCredentialOrphans", SubscriberCredentialOrphans},
		{"OldestSubscriberCredentialOrphanAgeSeconds", OldestSubscriberCredentialOrphanAgeSeconds},
		{"SubscriberRevocationFailures", SubscriberRevocationFailures},
		{"OldestSubscriberRevocationFailureAgeSeconds", OldestSubscriberRevocationFailureAgeSeconds},
		{"SubscribersUnmeasured", SubscribersUnmeasured},
		{"SubscribersRegistered", SubscribersRegistered},
		{"ConsumerLagInventoryComplete", ConsumerLagInventoryComplete},
		{"SubscriberLagPassAgeSeconds", SubscriberLagPassAgeSeconds},
		{"SubscriberLagCoveredSubscribers", SubscriberLagCoveredSubscribers},
		{"EventMetricsCollectionFailuresTotal", EventMetricsCollectionFailuresTotal},
		{"EventMetricsLastCollectionAgeSeconds", EventMetricsLastCollectionAgeSeconds},
		{"EventMetricsLastSuccessAgeSeconds", EventMetricsLastSuccessAgeSeconds},
	}
}

// preExistingInstruments returns the sixteen instruments that predate the
// event-streaming work, each labelled with its variable name. Enumerated
// exhaustively so that a new assignment block spliced into the wrong place in
// Init() — above an existing block's early return, say — cannot silently orphan an
// instrument that used to work.
//
// See eventStreamingInstruments for why this is a function rather than a
// package-level table.
func preExistingInstruments() []namedInstrument {
	return []namedInstrument{
		{"TransactionTotal", TransactionTotal},
		{"TransactionDuration", TransactionDuration},
		{"TransactionRejectedTotal", TransactionRejectedTotal},
		{"QueueEnqueuedTotal", QueueEnqueuedTotal},
		{"QueueProcessingDuration", QueueProcessingDuration},
		{"BalanceCreatedTotal", BalanceCreatedTotal},
		{"InflightCommitTotal", InflightCommitTotal},
		{"InflightVoidTotal", InflightVoidTotal},
		{"TransactionBatchSize", TransactionBatchSize},
		{"TransactionBatchTotal", TransactionBatchTotal},
		{"HotpairsContentionTotal", HotpairsContentionTotal},
		{"HotpairsLaneRoutedTotal", HotpairsLaneRoutedTotal},
		{"WorkerRetriesTotal", WorkerRetriesTotal},
		{"ChainBacklog", ChainBacklog},
		{"ChainHeadSeq", ChainHeadSeq},
		{"ChainLagSeconds", ChainLagSeconds},
	}
}

// TestInit_ReturnsNilForEveryInstrument asserts that Init() walks its whole
// assignment chain — every instrument, the sixteen that predate the event pipeline and
// every event-streaming addition — without the meter refusing to build one.
//
// Init() has ALREADY run once, from the package's own init(), before this function
// was reached. Calling it a second time here is intentional and is not a bug:
// Init() keeps no state of its own and only re-assigns the package-level instrument
// variables from the same meter, so it is idempotent by construction. A repeat call
// produces an equivalent set of instruments and cannot leave the package in a worse
// state than it found it, which is also why no other test in this file depends on
// whether this one has run.
//
// What a nil error does and does not prove: it proves the chain completed and every
// variable was assigned, because Init() returns early on the first failure. It does
// not prove the names survive a real SDK meter's validation — that is checked
// out-of-process against the live /metrics endpoint, for the reason given in the
// scope note at the top of this file.
func TestInit_ReturnsNilForEveryInstrument(t *testing.T) {
	// The meter is the source of every instrument, so a nil meter would make the
	// rest of this test meaningless. Reachable only because these tests live in
	// package metrics rather than metrics_test.
	require.NotNil(t, meter, "the package meter must be constructed before Init runs")

	require.NoError(t, Init(),
		"Init must build every instrument without the meter rejecting one; a non-nil error here is fatal at import and would kill any process importing this package")

	// A nil error is necessary but not sufficient. Init() returns on the FIRST
	// error, so an assignment block that was never written at all also yields nil.
	// Re-reading the variables after the call proves each one was genuinely
	// assigned rather than merely not-failing.
	for _, instrument := range append(eventStreamingInstruments(), preExistingInstruments()...) {
		assert.NotNil(t, instrument.value,
			"%s returned from Init nil: its declaration has no matching assignment block", instrument.name)
	}
}

// TestEventStreamingInstruments_AreNonNilAfterPackageLoad asserts that importing
// this package is by itself enough to leave EVERY event-streaming instrument
// usable.
//
// This test deliberately does NOT call Init(). Its entire subject is the state the
// package reaches on its own, through init(), at import time — which is the state
// every production call site actually observes, because nothing in the codebase
// calls Init() a second time. A nil here means the declaration in metrics.go has no
// matching assignment block in Init(): an orphaned declaration that the compiler
// accepts happily and that becomes a nil-interface panic at the first call site.
//
// Reaching this function at all is itself the proof that init() did not
// log.Fatalf, since a fatal at load aborts the test binary before any test runs.
func TestEventStreamingInstruments_AreNonNilAfterPackageLoad(t *testing.T) {
	for _, instrument := range eventStreamingInstruments() {
		t.Run(instrument.name, func(t *testing.T) {
			assert.NotNil(t, instrument.value,
				"%s is declared but never assigned during package initialisation: every call site would panic on a nil instrument", instrument.name)
		})
	}
}

// TestPreExistingInstruments_RemainNonNilAfterEventStreamingAppend asserts that
// the event-streaming additions did not disturb any of the sixteen
// instruments that came before them.
//
// Init() is a single linear chain of assignments, each followed by an early return
// on error. Appending to that chain is safe; splicing into the middle of it is not,
// and an assignment block accidentally placed inside another block's error branch —
// or a declaration left behind when its block moved — orphans an instrument that
// used to work. Nothing else in the build catches that, so all sixteen are
// enumerated by name here.
func TestPreExistingInstruments_RemainNonNilAfterEventStreamingAppend(t *testing.T) {
	instruments := preExistingInstruments()
	require.Len(t, instruments, 16,
		"all sixteen pre-existing instruments must stay enumerated here; update this table only when metrics.go genuinely removes one")

	for _, instrument := range instruments {
		t.Run(instrument.name, func(t *testing.T) {
			assert.NotNil(t, instrument.value,
				"%s was orphaned: it is declared in metrics.go but no longer assigned during package initialisation", instrument.name)
		})
	}
}

// TestEventStreamingInstruments_DeclaredKindsMatchTheirInstrumentType makes the
// instrument-kind table readable and executable, and checks it one level deeper
// than the compile-time guards above do.
//
// The guards check the DECLARED static type. This test checks the DYNAMIC type the
// meter actually handed back, and labels each check with the variable name so a
// failure points straight at the offending declaration instead of at a build error
// on a blank identifier. Both matter: a counter recorded as a gauge produces a
// series that looks plausible and is arithmetically meaningless, and neither
// alerting rule in the event pipeline — dead-letter age and consumer lag — survives
// its gauge being built as anything else.
func TestEventStreamingInstruments_DeclaredKindsMatchTheirInstrumentType(t *testing.T) {
	tests := []struct {
		name string
		// instrument is the package variable under test.
		instrument any
		// expectedKind is a nil pointer to the interface the instrument must
		// satisfy; that is the shape assert.Implements requires.
		expectedKind any
	}{
		{"EventsPublishedTotal", EventsPublishedTotal, (*metric.Int64Counter)(nil)},
		{"EventsDispatchedTotal", EventsDispatchedTotal, (*metric.Int64Counter)(nil)},
		{"EventPublishAttemptsTotal", EventPublishAttemptsTotal, (*metric.Int64Counter)(nil)},
		{"EventPublishDuration", EventPublishDuration, (*metric.Float64Histogram)(nil)},
		{"EventCaptureToDispatchDuration", EventCaptureToDispatchDuration, (*metric.Float64Histogram)(nil)},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal, (*metric.Int64Counter)(nil)},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds, (*metric.Float64Gauge)(nil)},
		{"SubscriberConsumerLag", SubscriberConsumerLag, (*metric.Int64ObservableGauge)(nil)},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions, (*metric.Int64ObservableGauge)(nil)},
		{"OutboxPendingBacklog", OutboxPendingBacklog, (*metric.Int64Gauge)(nil)},
		{"EventsPurgedTotal", EventsPurgedTotal, (*metric.Int64Counter)(nil)},
		{"SubscriberRevocationsPending", SubscriberRevocationsPending, (*metric.Int64Gauge)(nil)},
		{
			"OldestSubscriberRevocationAgeSeconds",
			OldestSubscriberRevocationAgeSeconds,
			(*metric.Float64Gauge)(nil),
		},
		{"SubscriberSettlementOutstanding", SubscriberSettlementOutstanding, (*metric.Int64Gauge)(nil)},
		{"SubscriberGrantReconcilePending", SubscriberGrantReconcilePending, (*metric.Int64Gauge)(nil)},
		{
			"SubscriberCredentialCleanupPending",
			SubscriberCredentialCleanupPending,
			(*metric.Int64Gauge)(nil),
		},
		{
			"OldestSubscriberSettlementAgeSeconds",
			OldestSubscriberSettlementAgeSeconds,
			(*metric.Float64Gauge)(nil),
		},
		{
			// A COUNTER, and the kind is the point: SubscriberSettlementNotProgressing takes a
			// rate over it, and rate() over a gauge is arithmetically meaningless — it would
			// produce a plausible-looking series that could never distinguish a stuck pass from
			// a busy one, which is the only question the instrument exists to answer.
			"SubscriberObligationsSettledTotal",
			SubscriberObligationsSettledTotal,
			(*metric.Int64Counter)(nil),
		},
		{
			"EventBrokerAcknowledgementsTotal",
			EventBrokerAcknowledgementsTotal,
			(*metric.Int64Counter)(nil),
		},
		{"SubscriberCredentialOrphans", SubscriberCredentialOrphans, (*metric.Int64Gauge)(nil)},
		{
			"OldestSubscriberCredentialOrphanAgeSeconds",
			OldestSubscriberCredentialOrphanAgeSeconds,
			(*metric.Float64Gauge)(nil),
		},
		{"SubscriberRevocationFailures", SubscriberRevocationFailures, (*metric.Int64Gauge)(nil)},
		{
			"OldestSubscriberRevocationFailureAgeSeconds",
			OldestSubscriberRevocationFailureAgeSeconds,
			(*metric.Float64Gauge)(nil),
		},
		{"SubscribersUnmeasured", SubscribersUnmeasured, (*metric.Int64Gauge)(nil)},
		{"SubscribersRegistered", SubscribersRegistered, (*metric.Int64Gauge)(nil)},
		{"ConsumerLagInventoryComplete", ConsumerLagInventoryComplete, (*metric.Int64Gauge)(nil)},
		{"SubscriberLagPassAgeSeconds", SubscriberLagPassAgeSeconds, (*metric.Float64Gauge)(nil)},
		{
			"SubscriberLagCoveredSubscribers",
			SubscriberLagCoveredSubscribers,
			(*metric.Int64Gauge)(nil),
		},
		{
			"EventMetricsCollectionFailuresTotal",
			EventMetricsCollectionFailuresTotal,
			(*metric.Int64Counter)(nil),
		},
		{
			// ASYNCHRONOUS, and the kind is the point: the subject is a component that may have
			// STOPPED, and a synchronous gauge can only be written by the collector itself — so
			// it would freeze at its last reading and report a dead collector as permanently
			// fresh, which is the one state these two exist to detect.
			"EventMetricsLastCollectionAgeSeconds",
			EventMetricsLastCollectionAgeSeconds,
			(*metric.Float64ObservableGauge)(nil),
		},
		{
			"EventMetricsLastSuccessAgeSeconds",
			EventMetricsLastSuccessAgeSeconds,
			(*metric.Float64ObservableGauge)(nil),
		},
	}

	require.Len(t, tests, len(eventStreamingInstruments()),
		"every event-streaming instrument must have a declared kind asserted here")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.instrument,
				"%s is nil, so its kind cannot be checked", tt.name)
			assert.Implements(t, tt.expectedKind, tt.instrument,
				"%s was built as the wrong instrument kind", tt.name)
		})
	}
}

// TestEventStreamingInstruments_RecordWithoutPanic drives one representative
// measurement through every event-streaming instrument.
//
// Each call reproduces the shape the production call sites use — an attributed
// Add for counters (as in queue.go), an attributed Record for the histogram (as in
// cmd/workers.go), and a bare Record for the unattributed gauge (as in
// chain_worker.go) — so the intended attribute KEYS for each instrument are
// documented in executable form and drift from them shows up as a test edit rather
// than as a quietly relabelled series.
//
// Recording is expected to be safe here: with no global MeterProvider installed the
// instruments are delegating no-ops whose Add and Record are nil-delegate guarded.
// That makes NotPanics a real assertion rather than a formality — it fails loudly
// if an instrument is nil, which is the state an orphaned declaration leaves behind
// and the exact failure a production call site would hit.
func TestEventStreamingInstruments_RecordWithoutPanic(t *testing.T) {
	ctx := context.Background()

	t.Run("EventsPublishedTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			EventsPublishedTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("event_type", "transaction.applied"),
			))
		})
	})

	t.Run("EventBrokerAcknowledgementsTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			// The purpose attribute is what keeps a replay and a dead-letter write separable
			// from a first delivery here, since unlike EventsPublishedTotal this counter
			// records every acknowledgement the broker gave.
			EventBrokerAcknowledgementsTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("event_type", "transaction.applied"),
				attribute.String("purpose", "original"),
			))
		})
	})

	t.Run("EventPublishAttemptsTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("outcome", "dispatched"),
			))
		})
	})

	t.Run("EventPublishDuration", func(t *testing.T) {
		require.NotPanics(t, func() {
			EventPublishDuration.Record(ctx, 0.184, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("attempt", "1"),
			))
		})
	})

	t.Run("EventCaptureToDispatchDuration", func(t *testing.T) {
		require.NotPanics(t, func() {
			// 1.75 seconds: an event that waited for a poll tick and then published,
			// which is the shape this instrument exists to measure and the per-write
			// histogram above cannot see. No outcome attribute — only an acknowledged
			// publish has an end-to-end age at all.
			EventCaptureToDispatchDuration.Record(ctx, 1.75, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("attempt", "1"),
			))
		})
	})

	t.Run("EventsDeadLetteredTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			// The topic attribute deliberately carries the ORIGINAL category
			// topic rather than its .dlt sibling, so this counter stays directly
			// comparable with EventsPublishedTotal and their ratio is the
			// dead-letter rate.
			EventsDeadLetteredTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("event_type", "transaction.applied"),
			))
		})
	})

	t.Run("DLTOldestMessageAgeSeconds", func(t *testing.T) {
		require.NotPanics(t, func() {
			// This gauge measures a message sitting ON a dead-letter topic, so
			// here the topic attribute is the .dlt name. 930 seconds is just past
			// the 900-second alerting threshold, which is the value that matters
			// operationally.
			DLTOldestMessageAgeSeconds.Record(ctx, 930, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions.dlt"),
			))
		})
	})

	t.Run("the asynchronous lag gauges", func(t *testing.T) {
		// These two are OBSERVABLE, so there is no Record to drive. The equivalent
		// operation is publishing an inventory and letting the registered callback
		// observe it, which is what this exercises: 10001 messages is just past the
		// 10000-message alerting threshold, and the second sample is the incomplete
		// case whose lag must be withheld.
		require.NotPanics(t, func() {
			PublishConsumerLagInventory([]ConsumerLagSample{
				{
					Subscriber:  "sub_01HZX9K2Q4",
					Group:       "blnk-sub-01HZX9K2Q4",
					Topic:       "blnk.transactions",
					Lag:         10001,
					LagComplete: true,
				},
				{
					Subscriber:           "sub_01HZX9K2Q4",
					Group:                "blnk-sub-01HZX9K2Q4",
					Topic:                "blnk.balances",
					Lag:                  7,
					LagComplete:          false,
					UnmeasuredPartitions: 2,
				},
			})

			observer := &capturingObserver{}
			require.NoError(t, observeConsumerLagInventory(ctx, observer))

			// THE WITHHOLDING CONTRACT, asserted at the layer that decides it. Both samples
			// contribute an unmeasured-partition reading, and only the complete one
			// contributes a lag — because a partial sum is a lower bound and exporting it
			// would resolve the >10000 alert with a figure known to be too small.
			assert.Equal(t, []int64{10001}, observer.int64For(SubscriberConsumerLag),
				"only the completely measured topic may export a lag")
			assert.Equal(t, []int64{0, 2}, observer.int64For(ConsumerLagUnmeasuredPartitions),
				"every measured topic reports its unmeasured-partition count, zero included")
		})

		t.Cleanup(func() { PublishConsumerLagInventory(nil) })
	})

	t.Run("OutboxPendingBacklog", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Unattributed by design: the relay's backlog is a single global
			// number, recorded exactly the way chain_worker.go records
			// ChainBacklog.
			OutboxPendingBacklog.Record(ctx, 42)
		})
	})

	t.Run("EventsPurgedTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Unattributed, matching event_retention.go: the sweep reports how many rows
			// it deleted, and it deletes across every topic in one pass, so there is no
			// per-topic number to attribute.
			EventsPurgedTotal.Add(ctx, 5)
		})
	})

	t.Run("the revocation backlog gauges", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Both unattributed, matching event_metrics.go: the backlog is a single
			// global pair, and attributing it per subscriber would publish a series per
			// subscriber ever revoked — unbounded cardinality for a number whose only
			// consumer is one alert threshold.
			SubscriberRevocationsPending.Record(ctx, 4)
			OldestSubscriberRevocationAgeSeconds.Record(ctx, 7200)
		})
	})
}

// TestPublishConsumerLagInventory_ReplacesRatherThanMerges pins the property the whole
// cardinality fix rests on.
//
// A merging publication would reproduce the exact defect the asynchronous gauges were adopted
// to remove: series would accumulate across ticks and the count would become the union of every
// inventory the process had ever seen. REPLACEMENT is what makes the exported series set equal
// to the current one, and it is why the collector must pass its whole measured set on every tick
// rather than a delta.
func TestPublishConsumerLagInventory_ReplacesRatherThanMerges(t *testing.T) {
	t.Cleanup(func() { PublishConsumerLagInventory(nil) })

	first := []ConsumerLagSample{
		{Subscriber: "sub_a", Group: "blnk-sub-sub_a", Topic: "blnk.transactions", Lag: 11, LagComplete: true},
		{Subscriber: "sub_b", Group: "blnk-sub-sub_b", Topic: "blnk.balances", Lag: 22, LagComplete: true},
	}
	PublishConsumerLagInventory(first)
	require.Len(t, ConsumerLagInventory(), 2)

	// sub_b is gone. After replacement it must be absent, not present at zero: a zero stops an
	// alert but leaves the series in existence, which is what grew the count without bound.
	PublishConsumerLagInventory([]ConsumerLagSample{first[0]})

	remaining := ConsumerLagInventory()
	require.Len(t, remaining, 1, "the inventory is the current set and nothing else")
	assert.Equal(t, "sub_a", remaining[0].Subscriber)

	observer := &capturingObserver{}
	require.NoError(t, observeConsumerLagInventory(context.Background(), observer))
	assert.Equal(t, []int64{11}, observer.int64For(SubscriberConsumerLag),
		"only the surviving subscriber may be observed, so only its series is exported")

	t.Run("an empty publication retires everything", func(t *testing.T) {
		PublishConsumerLagInventory(nil)
		assert.Empty(t, ConsumerLagInventory())

		empty := &capturingObserver{}
		require.NoError(t, observeConsumerLagInventory(context.Background(), empty))
		assert.Empty(t, empty.int64For(SubscriberConsumerLag),
			"no registry, no broker or no subscribers must export no lag at all")
		assert.Empty(t, empty.int64For(ConsumerLagUnmeasuredPartitions))
	})

	t.Run("the published slice is copied, so a caller may reuse its buffer", func(t *testing.T) {
		buffer := []ConsumerLagSample{
			{Subscriber: "sub_c", Group: "blnk-sub-sub_c", Topic: "blnk.identities", Lag: 5, LagComplete: true},
		}
		PublishConsumerLagInventory(buffer)

		// The collector reuses its accumulator between ticks, so aliasing here would let a
		// later tick rewrite telemetry that has already been published.
		buffer[0].Lag = 999_999
		buffer[0].Subscriber = "mutated"

		published := ConsumerLagInventory()
		require.Len(t, published, 1)
		assert.Equal(t, int64(5), published[0].Lag, "a mutation after publication must not reach the export")
		assert.Equal(t, "sub_c", published[0].Subscriber)

		// And the reader copies too, for the same reason in the other direction.
		published[0].Lag = -1
		assert.Equal(t, int64(5), ConsumerLagInventory()[0].Lag,
			"ConsumerLagInventory must copy, or a reader could rewrite what is exported")
	})

	t.Run("publishing concurrently with observation is safe", func(t *testing.T) {
		// The SDK invokes the callback on its own collection goroutine, which is concurrent
		// with the collector's tick by construction. Under -race this is the assertion.
		var wg sync.WaitGroup
		for worker := range 8 {
			wg.Add(2)

			go func(worker int) {
				defer wg.Done()

				PublishConsumerLagInventory([]ConsumerLagSample{{
					Subscriber:  "sub_concurrent",
					Group:       "blnk-sub-sub_concurrent",
					Topic:       "blnk.transactions",
					Lag:         int64(worker),
					LagComplete: true,
				}})
			}(worker)

			go func() {
				defer wg.Done()

				assert.NoError(t, observeConsumerLagInventory(context.Background(), &capturingObserver{}))
			}()
		}
		wg.Wait()

		assert.Len(t, ConsumerLagInventory(), 1, "every publication replaces the whole inventory")
	})
}

// TestEventPublishAttemptsTotal_AcceptsEveryPublishOutcome records one attempt per
// value of the "outcome" attribute vocabulary.
//
// The point is not that the no-op instrument tolerates arbitrary strings — it
// tolerates anything — but that the vocabulary is pinned in one place, in the same
// spelling the relay emits and the alerting rules filter on. dead_lettered in
// particular is easy to get wrong: the hyphenated "dead-lettered" reads more
// naturally in prose and matches nothing at query time.
//
// The vocabulary is THREE values, and the accompanying `terminal` dimension is what
// carries the fourth fact the retired outcome used to carry. Both domains are pinned
// together here, because they are only meaningful as a pair: a failure that will be
// retried and one that never will are different operational states, and after the
// fourth outcome was removed the ONLY thing distinguishing them is `terminal`. Any
// change to either list is a change to a published attribute domain and has to be made
// deliberately here, on the instrument's declaration, and in anything querying it.
func TestEventPublishAttemptsTotal_AcceptsEveryPublishOutcome(t *testing.T) {
	ctx := context.Background()

	require.Len(t, publishOutcomes, 3,
		"the outcome vocabulary is dispatched, retrying and dead_lettered; "+
			"a fourth publish outcome needs a deliberate decision here and in the alerting rules")
	require.Contains(t, publishOutcomes, "dead_lettered",
		"the dead-letter outcome is spelled with an underscore, not a hyphen")
	require.NotContains(t, publishOutcomes, "failed",
		"`failed` was retired because it widened the documented three-value publish-status "+
			"vocabulary; a terminal failure is {outcome=\"retrying\",terminal=\"true\"}")

	require.Len(t, publishTerminalValues, 2,
		"the terminal domain is exactly true and false, so it multiplies the series count by two")

	for _, outcome := range publishOutcomes {
		for _, terminal := range publishTerminalValues {
			t.Run(outcome+"/terminal="+terminal, func(t *testing.T) {
				require.NotPanics(t, func() {
					EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
						attribute.String("outcome", outcome),
						attribute.String("terminal", terminal),
					))
				})
			})
		}
	}
}

// TestEventPublishDuration_AcceptsEveryAttemptInTheRetryBudget records a duration
// for every attempt number the relay's retry budget can produce.
//
// The attempt attribute carries the attempt NUMBER as a string, which is what lets
// a p99 latency query exclude retried publishes by filtering attempt="1" — the
// distinction the throughput-and-latency acceptance criterion is stated in terms
// of. Every value from the first attempt through the budget's last must therefore
// be a legal label.
func TestEventPublishDuration_AcceptsEveryAttemptInTheRetryBudget(t *testing.T) {
	ctx := context.Background()

	for attempt := 1; attempt <= maxRelayRetryAttempts; attempt++ {
		label := strconv.Itoa(attempt)
		t.Run("attempt="+label, func(t *testing.T) {
			require.NotPanics(t, func() {
				// The WHOLE declared tuple, outcome included, even though this test asserts
				// only that the attempt label is legal. Recording a subset would put a series
				// with two of the instrument's three keys into the process, and once a
				// MeterProvider is installed — which the tests below the SDK line do, once, for
				// the lifetime of the binary — that series is a second, off-contract shape of
				// an instrument whose attribute keys are a published contract. It is also what
				// the exported-attribute-keys assertion used to trip over.
				EventPublishDuration.Record(ctx, float64(attempt)*0.25, metric.WithAttributes(
					attribute.String("topic", "blnk.transactions"),
					attribute.String("attempt", label),
					attribute.String("outcome", "dispatched"),
				))
			})
		})
	}
}

// TestDLTOldestMessageAgeSeconds_AcceptsZeroWhenDeadLetterTopicIsEmpty asserts that
// zero is a legal recorded value for the dead-letter age gauge.
//
// This is not a redundant edge case. An age gauge that is only ever written when a
// dead-lettered message exists would keep reporting its last non-zero reading after
// the queue drained, and the 900-second alert would stay latched on a backlog that
// no longer exists. Returning the gauge to zero on an empty dead-letter topic is
// what clears the alert, so zero has to round-trip.
func TestDLTOldestMessageAgeSeconds_AcceptsZeroWhenDeadLetterTopicIsEmpty(t *testing.T) {
	ctx := context.Background()

	require.NotPanics(t, func() {
		DLTOldestMessageAgeSeconds.Record(ctx, 0, metric.WithAttributes(
			attribute.String("topic", "blnk.transactions.dlt"),
		))
	})
}

// TestOutboxPendingBacklog_AcceptsZeroWhenOutboxIsDrained asserts the same
// alert-clearing property for the relay's backlog gauge: a drained outbox must be
// recordable as zero, not merely left at its last non-zero reading.
func TestOutboxPendingBacklog_AcceptsZeroWhenOutboxIsDrained(t *testing.T) {
	ctx := context.Background()

	require.NotPanics(t, func() {
		OutboxPendingBacklog.Record(ctx, 0)
	})
}

// ---------------------------------------------------------------------------
// Exported-contract assertions, made against a real SDK reader
//
// Everything above this line runs against the no-op instruments a process gets
// when no MeterProvider is installed, and that is all those tests can do: a
// delegating no-op discards its measurements and exposes no descriptor, so
// "does not panic" is the strongest statement available. It is not enough for
// the properties an alert or a latency query actually depends on — the metric
// NAME the exporter publishes, the UNIT it publishes it in, the explicit
// BUCKET BOUNDARIES that decide whether a p99 near two seconds is measurable
// at all, and the ATTRIBUTE KEYS the alert annotations interpolate. A rename,
// a unit slip, or a silent fall back to OTel's millisecond-scale default
// buckets would leave every test above passing.
//
// The tests below therefore install a real SDK MeterProvider backed by a
// manual reader, re-run Init() so the instruments are constructed through it,
// record one representative measurement per instrument, collect, and assert on
// the collected metricdata. That is the same path the Prometheus exporter
// takes, so what is asserted here is what is exported.
//
// Global-state note: otel.SetMeterProvider delegates the package's meter
// permanently — the delegation is a sync.Once — so it is done exactly once, in
// installSDKReader, and the tests that share it are strictly serial (no
// t.Parallel anywhere in this file). Delegation is harmless to the tests above:
// their measurements simply land in a reader nobody collects.
// ---------------------------------------------------------------------------

// sharedReader and installOnce back installSDKReader.
//
// They are package-level and guarded by a sync.Once because the installation CANNOT be
// repeated: otel delegates the global meter to the first provider set and does so under
// its own sync.Once, so a second provider installed by a second test would never receive
// this package's measurements — the meter stays bound to the first. Installing per test
// therefore does not isolate the tests, it silently blinds all but the first of them.
// One installation, shared, with the tests kept serial, is the only arrangement that
// works.
//
// The provider is deliberately never shut down. A shut-down provider drops
// measurements while the meter remains delegated to it, which would leave every
// subsequent test collecting an empty snapshot.
var (
	sharedReader *sdkmetric.ManualReader
	installOnce  sync.Once
)

// perTestTemporality makes the shared reader report DELTA sums and histograms and cumulative
// gauges.
//
// # Why the shared reader cannot be cumulative
//
// A shared reader is forced on this file by the once-only meter delegation above, and a
// CUMULATIVE one accumulates for the lifetime of the process. Every assertion of the form "this
// measurement landed in exactly one bucket" therefore held only on the FIRST iteration:
// `go test -count=2` doubled the bucket counts and the file failed with `expected: 0x1, actual:
// 0x2`, and `-count=3` tripled them. Scoping a data point by a topic value of the test's own —
// which these tests already did — separates them from EACH OTHER but not from the previous
// iteration of THEMSELVES, so it could not fix this.
//
// Delta resets the accumulation on every collect and drops the series that had no measurements
// in the interval, which combined with the drain in installSDKReader gives each test a
// collection containing exactly what that test recorded. That is repeat-safe and
// order-independent by construction rather than by convention.
//
// # Why gauges stay cumulative
//
// A gauge reports a LAST VALUE, and delta would retire it from the collection as soon as it had
// been read once. The descriptor and attribute-key assertions need the gauge present, and its
// value is not an accumulation, so there is nothing for delta to fix.
func perTestTemporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	switch kind {
	case sdkmetric.InstrumentKindCounter,
		sdkmetric.InstrumentKindUpDownCounter,
		sdkmetric.InstrumentKindHistogram:
		return metricdata.DeltaTemporality
	default:
		return metricdata.CumulativeTemporality
	}
}

// installSDKReader returns the shared SDK-backed manual reader, installing the provider
// and rebuilding every instrument through it on first use, and DRAINING whatever was recorded
// before this test began.
//
// Init() is called AFTER the provider is installed, which is what makes the instruments
// real SDK instruments rather than delegating no-ops: an instrument created before
// delegation replays its creation, but creating it afterwards is direct and leaves no room
// for the replay to lose an option — and the option that matters most here, the explicit
// bucket boundaries, is exactly the kind of thing a replay could drop.
//
// # The drain is the isolation
//
// Every test above the SDK line records measurements too, and once the provider is installed
// those land in this reader as well — TestEventPublishDuration_AcceptsEveryAttemptInTheRetryBudget
// in particular records the publish-duration histogram with only two of its three attribute
// keys. From the second iteration onwards, or under any -shuffle order that ran it later, those
// measurements were still in the reader when a contract assertion collected, and a test reading
// "the exported attribute keys" could pick up that shape instead of its own.
//
// Discarding one collection here draws a line: everything before this test is gone, so the next
// collection holds this test's measurements and nothing else. Combined with delta temporality
// above, that makes the file idempotent across iterations.
//
// Returns:
//   - *sdkmetric.ManualReader: the reader to collect from, drained.
func installSDKReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	installOnce.Do(func() {
		sharedReader = sdkmetric.NewManualReader(
			sdkmetric.WithTemporalitySelector(perTestTemporality),
		)
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(sharedReader)))
	})

	require.NoError(t, Init(), "Init must rebuild every instrument through the installed provider")
	require.NotNil(t, sharedReader, "the shared manual reader was not installed")

	var discarded metricdata.ResourceMetrics
	require.NoError(t, sharedReader.Collect(context.Background(), &discarded),
		"draining the shared reader before the test records into it")

	return sharedReader
}

// capturingObserver is a metric.Observer that keeps what a callback observed, so the
// asynchronous gauges' contract can be asserted without an SDK.
//
// It is the asynchronous counterpart of a fake instrument: an observable gauge has no Record
// to intercept, so the interception point is the OBSERVER the callback is handed. What matters
// operationally is not only the values but WHICH instrument received them — a sample whose lag
// is withheld still observes its unmeasured-partition count — and that is a distinction only an
// observer-level capture can make.
//
// embedded.Observer is embedded, not implemented: that is how the OpenTelemetry API intends
// third-party implementations of its interfaces to be written.
type capturingObserver struct {
	embedded.Observer

	int64Observations   []observedInt64
	float64Observations []observedFloat64
}

// observedInt64 is one int64 observation and the instrument it was made against.
type observedInt64 struct {
	instrument metric.Int64Observable
	value      int64
}

// observedFloat64 is one float64 observation and the instrument it was made against.
type observedFloat64 struct {
	instrument metric.Float64Observable
	value      float64
}

var _ metric.Observer = (*capturingObserver)(nil)

func (o *capturingObserver) ObserveInt64(
	instrument metric.Int64Observable,
	value int64,
	_ ...metric.ObserveOption,
) {
	o.int64Observations = append(o.int64Observations, observedInt64{instrument: instrument, value: value})
}

func (o *capturingObserver) ObserveFloat64(
	instrument metric.Float64Observable,
	value float64,
	_ ...metric.ObserveOption,
) {
	o.float64Observations = append(o.float64Observations, observedFloat64{instrument: instrument, value: value})
}

// int64For returns the values observed against one instrument, in observation order.
//
// Returns:
//   - []int64: the values; empty when the instrument was never observed.
func (o *capturingObserver) int64For(instrument metric.Int64Observable) []int64 {
	values := []int64{}
	for _, observation := range o.int64Observations {
		if observation.instrument == instrument {
			values = append(values, observation.value)
		}
	}

	return values
}

// recordEveryEventInstrument drives one representative measurement through each of the
// event-streaming instruments, using the attribute keys their declarations
// document.
//
// It exists so the descriptor assertions below have something to collect: the SDK
// reports a metric only once it has a data point, so an instrument that is never
// recorded is indistinguishable from one that was never declared.
func recordEveryEventInstrument(ctx context.Context) {
	EventsPublishedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("event_type", "transaction.applied"),
	))
	EventsDispatchedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("event_type", "transaction.applied"),
	))

	EventBrokerAcknowledgementsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("event_type", "transaction.applied"),
		attribute.String("purpose", "original"),
	))
	EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", "dispatched"),
		attribute.String("terminal", "false"),
	))
	EventPublishDuration.Record(ctx, 0.42, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("attempt", "1"),
		attribute.String("outcome", "dispatched"),
	))
	EventCaptureToDispatchDuration.Record(ctx, 1.75, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("attempt", "1"),
	))
	EventsDeadLetteredTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("event_type", "transaction.applied"),
	))
	DLTOldestMessageAgeSeconds.Record(ctx, 901, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions.dlt"),
	))
	// The two lag gauges are ASYNCHRONOUS: publishing the inventory is the measurement, and
	// the registered callback observes it when the reader collects. Both instruments are fed
	// from this one sample, so a single complete entry produces a lag point and a zero
	// unmeasured-partition point.
	PublishConsumerLagInventory([]ConsumerLagSample{{
		Subscriber:  "sub_0f6e2c8a",
		Group:       "blnk-grp-0f6e2c8a1b944106b4d6793e838afcbf",
		Topic:       "blnk.transactions",
		Lag:         10001,
		LagComplete: true,
	}})

	OutboxPendingBacklog.Record(ctx, 7)

	EventsPurgedTotal.Add(ctx, 3)

	SubscriberRevocationsPending.Record(ctx, 2)
	OldestSubscriberRevocationAgeSeconds.Record(ctx, 3601)

	// The four settlement gauges are recorded together because they are published together, as
	// one reading of one aggregate. Recording only some of them here would let a rename of the
	// others past the descriptor assertions below.
	SubscriberSettlementOutstanding.Record(ctx, 3)
	SubscriberGrantReconcilePending.Record(ctx, 2)
	SubscriberCredentialCleanupPending.Record(ctx, 2)
	OldestSubscriberSettlementAgeSeconds.Record(ctx, 3601)
	SubscriberObligationsSettledTotal.Add(ctx, 5)

	// The credential-orphan and revocation-failure pairs. Each is a marker plus the age the
	// age-based alert is stated over, so both halves are recorded together: a rename of the age
	// alone would otherwise slip past the descriptor assertions.
	SubscriberCredentialOrphans.Record(ctx, 1)
	OldestSubscriberCredentialOrphanAgeSeconds.Record(ctx, 3601)
	SubscriberRevocationFailures.Record(ctx, 1)
	OldestSubscriberRevocationFailureAgeSeconds.Record(ctx, 3601)

	// Lag-sweep COVERAGE, which is what says whether the lag alert can fire at all. The
	// unmeasured gauge is attributed by reason, because the reason decides the remediation.
	SubscribersUnmeasured.Record(ctx, 1, metric.WithAttributes(
		attribute.String("reason", "budget"),
	))
	SubscribersRegistered.Record(ctx, 3)
	ConsumerLagInventoryComplete.Record(ctx, 0)
	SubscriberLagPassAgeSeconds.Record(ctx, 12)
	SubscriberLagCoveredSubscribers.Record(ctx, 2)

	// The collector's own health. The counter is synchronous; the two ages are ASYNCHRONOUS and
	// are published by observeEventMetricsCollectionAges, which returns nothing at all until a
	// collection has been recorded — so this call is what makes those two series exist for the
	// descriptor and series-name assertions to find.
	EventMetricsCollectionFailuresTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("collection", "outbox_backlog"),
	))
	RecordEventMetricsCollection(time.Now().Add(-30*time.Second), true)
}

// collectScopeMetrics collects one snapshot and returns the metrics of the "blnk"
// meter, indexed by metric name.
func collectScopeMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var snapshot metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &snapshot), "collecting the manual reader")

	collected := make(map[string]metricdata.Metrics)
	for _, scope := range snapshot.ScopeMetrics {
		if scope.Scope.Name != "blnk" {
			continue
		}
		for _, m := range scope.Metrics {
			collected[m.Name] = m
		}
	}

	require.NotEmpty(t, collected, "the blnk meter produced no metrics; is the scope name still \"blnk\"?")

	return collected
}

// TestEventStreamingInstruments_ExportedDescriptors asserts the name, description and
// unit of every event-streaming instrument as the exporter sees them.
//
// The names are the load-bearing part. Alert rules and dashboards reference the
// PROMETHEUS forms of these names — dots become underscores and the unit suffix is
// appended — so alerts/blnk-kafka-alerts.yml matching on
// blnk_dlt_oldest_message_age_seconds and blnk_kafka_consumer_lag depends on the OTel
// names below being exactly what they are. A rename here does not fail a build and does
// not fail promtool; it silently makes the rule evaluate against nothing, which cannot
// fire and cannot be distinguished from "no problem". The units matter for the same
// reason: the Prometheus exporter derives a series suffix from them, so a unit change is
// a rename.
func TestEventStreamingInstruments_ExportedDescriptors(t *testing.T) {
	reader := installSDKReader(t)
	recordEveryEventInstrument(context.Background())
	collected := collectScopeMetrics(t, reader)

	expected := []struct {
		name        string
		unit        string
		description string
	}{
		{
			// UNIT {event}, NOT {write}, and the description says events. The unit is part of
			// the exported name, so this pairing is a published contract.
			//
			// The increment is on the token-fenced transition that records the Kafka leg, NOT on
			// the broker acknowledgement — so a republish after a crash cannot count one event
			// twice, and this stays the per-event measure the V-1 throughput verdict and the
			// V-3 dead-letter rate are read from. The write-side signal has its own instrument
			// two entries below; it once shared this one, and the description promised a
			// contract the increment did not keep.
			name: "blnk.events.published.total",
			unit: "{event}",
			description: "Total original ledger events whose Kafka leg is durably recorded, " +
				"counted once each by topic and event type",
		},
		{
			// ALSO PER-EVENT, and deliberately not a second opinion on the row above. This one
			// is incremented at the terminal dispatched state, which for a row still owing a
			// legacy webhook happens on a later pass — so it answers "how many events are
			// completely settled" rather than "how many are on their topic". The two converge
			// at the sunset, when the webhook_pending state is removed.
			name: "blnk.events.dispatched.total",
			unit: "{event}",
			description: "Total ledger events whose delivery is durably recorded, counted once " +
				"each by topic and event type",
		},
		{
			// THE WRITE-SIDE SIGNAL, and the unit says so: {write}, not {event}. It counts what
			// the broker accepted, for EVERY purpose — original, replay, dead-letter — so a
			// republished event is counted again, which is the whole point. The gap between it
			// and published.total is rows that reached Kafka and could not be marked, which is
			// the leading indicator of duplicate delivery.
			name:        "blnk.events.broker_acknowledgements.total",
			unit:        "{write}",
			description: "Total event publishes acknowledged by the broker, by topic, event type and purpose",
		},
		{
			name:        "blnk.events.publish.attempts.total",
			unit:        "{attempt}",
			description: "Total number of event publish attempts by outcome, retries included",
		},
		{
			// THE CLAIM is the start of this interval, and the description says so. It is
			// the broker write in relative isolation, which is the useful thing for it to
			// be: subtracted from the end-to-end figure below, the difference is the queue
			// wait, and that is what distinguishes a slow cluster from an under-provisioned
			// relay. Either instrument alone sends an operator to the wrong system.
			name:        "blnk.events.publish.duration",
			unit:        "s",
			description: "Duration of a single event publish attempt, from outbox claim to broker acknowledgement",
		},
		{
			// THE ACCEPTANCE CRITERION IS READ FROM THIS ONE. V-1 is stated over
			// "outbox-to-Kafka publish latency", and only this instrument starts its clock
			// at the durable capture: timing from the claim excludes the poll delay and the
			// backlog, so a relay an hour behind would report the same sub-second p99 as an
			// idle one and would certify a target the system was missing.
			name:        "blnk.events.capture_to_dispatch.duration",
			unit:        "s",
			description: "End-to-end age of a published event, from its capture in the transactional outbox to broker acknowledgement",
		},
		{
			name:        "blnk.events.dead_lettered.total",
			unit:        "{event}",
			description: "Total number of events dead-lettered after retry exhaustion by topic and event type",
		},
		{
			name:        "blnk.dlt.oldest_message_age_seconds",
			unit:        "s",
			description: "Seconds the oldest unresolved dead-letter entry has been waiting, over rows in the failed or dead_lettered state, measured from the last publish attempt",
		},
		{
			name:        "blnk.kafka.consumer_lag",
			unit:        "{message}",
			description: "Number of messages a subscriber consumer group trails the log end offset by, for completely measured topics only",
		},
		{
			name:        "blnk.kafka.consumer_lag_unmeasured_partitions",
			unit:        "{partition}",
			description: "Number of partitions whose offsets could not be read when a subscriber's lag was last measured",
		},
		{
			name:        "blnk.outbox.pending",
			unit:        "{event}",
			description: "Number of event outbox rows not yet published to Kafka",
		},
		{
			name:        "blnk.events.purged.total",
			unit:        "{event}",
			description: "Terminal event outbox rows deleted by the retention sweep",
		},
		{
			name:        "blnk.subscribers.revocation_pending",
			unit:        "{subscriber}",
			description: "Subscribers whose broker-side credential revocation is still owed",
		},
		{
			// THE ALERT READS THIS ONE. SubscriberRevocationOutstanding matches
			// blnk_subscribers_oldest_revocation_age_seconds against 3600, and the "seconds"
			// suffix comes from the unit below — so a unit change is a rename that silently
			// leaves the rule evaluating nothing.
			name:        "blnk.subscribers.oldest_revocation_age_seconds",
			unit:        "s",
			description: "Age of the oldest outstanding subscriber credential revocation",
		},
		{
			name:        "blnk.subscribers.settlement_outstanding",
			unit:        "{subscriber}",
			description: "Subscribers owing broker-side reconciliation of either kind",
		},
		{
			name:        "blnk.subscribers.grant_reconcile_pending",
			unit:        "{subscriber}",
			description: "Subscribers whose broker-side ACL grant may not match the registry",
		},
		{
			name:        "blnk.subscribers.credential_cleanup_pending",
			unit:        "{subscriber}",
			description: "Subscribers owing a broker-side credential cleanup",
		},
		{
			// SubscriberSettlementOutstanding matches
			// blnk_subscribers_oldest_settlement_age_seconds against 3600, so the same
			// unit-is-a-rename caution applies here.
			name:        "blnk.subscribers.oldest_settlement_age_seconds",
			unit:        "s",
			description: "Age of the oldest outstanding subscriber settlement obligation",
		},
		{
			// SubscriberSettlementNotProgressing takes a rate over this counter, which is what
			// separates a stuck settlement pass from a busy one — a distinction the four gauges
			// above cannot make, because a flat backlog looks identical either way.
			name:        "blnk.subscribers.obligations_settled.total",
			unit:        "{obligation}",
			description: "Broker-side subscriber obligations discharged by the settlement pass",
		},
		{
			name:        "blnk.subscribers.credential_orphans",
			unit:        "{subscriber}",
			description: "Subscribers holding a Kafka credential Blnk could neither record nor revoke",
		},
		{
			name:        "blnk.subscribers.oldest_credential_orphan_age_seconds",
			unit:        "s",
			description: "Age of the oldest unaccounted subscriber Kafka credential",
		},
		{
			name:        "blnk.subscribers.revocation_failures",
			unit:        "{subscriber}",
			description: "Subscribers whose most recent broker-side revocation attempt was refused",
		},
		{
			name:        "blnk.subscribers.oldest_revocation_failure_age_seconds",
			unit:        "s",
			description: "Age of the oldest refused subscriber revocation attempt",
		},
		{
			// ATTRIBUTED BY REASON, which is why the aggregate alone is not enough: only the
			// 'budget' reason is answered by configuration.
			name:        "blnk.kafka.subscribers_unmeasured",
			unit:        "{subscriber}",
			description: "Registered subscribers the last lag sweep did not publish a complete reading for, by reason",
		},
		{
			name: "blnk.subscribers.registered",
			unit: "{subscriber}",
			description: "Subscribers the registry holds, so the unmeasured count can be read as a proportion " +
				"and the measurement budget's headroom is visible before it is exhausted",
		},
		{
			name:        "blnk.kafka.consumer_lag_inventory_complete",
			unit:        "{status}",
			description: "1 when the last lag sweep measured every registered subscriber, 0 when it did not",
		},
		{
			name:        "blnk.kafka.consumer_lag.pass_age_seconds",
			unit:        "s",
			description: "Age of the in-progress pass over the subscriber registry for lag measurement",
		},
		{
			name:        "blnk.kafka.consumer_lag.covered_subscribers",
			unit:        "{subscriber}",
			description: "Subscribers with a currently exported consumer-lag series",
		},
		{
			name:        "blnk.event_metrics.collection_failures.total",
			unit:        "{failure}",
			description: "Failures inside the periodic event-metrics collector, by which collection failed",
		},
		{
			// THE COLLECTOR'S OWN LIVENESS. EventMetricsCollectionStale matches
			// blnk_event_metrics_last_collection_age_seconds, so the "seconds" suffix comes from
			// this unit and a unit change is a rename that leaves the rule evaluating nothing.
			name:        "blnk.event_metrics.last_collection_age_seconds",
			unit:        "s",
			description: "Seconds since the periodic event-metrics collector last finished a collection",
		},
		{
			name:        "blnk.event_metrics.last_success_age_seconds",
			unit:        "s",
			description: "Seconds since the periodic event-metrics collector last completed a collection with no failures",
		},
	}

	require.Len(t, expected, len(eventStreamingInstruments()),
		"every declared event-streaming instrument must have a descriptor expectation here")

	for _, want := range expected {
		t.Run(want.name, func(t *testing.T) {
			got, ok := collected[want.name]
			require.True(t, ok, "%s was not exported; declared but never assigned in Init, or renamed", want.name)
			assert.Equal(t, want.unit, got.Unit, "%s carries the wrong unit, which renames its exported series", want.name)
			assert.Equal(t, want.description, got.Description, "%s carries the wrong description", want.name)
		})
	}
}

// TestEventPublishDuration_UsesExplicitSubTwoSecondBuckets is the assertion the
// latency acceptance criterion rests on.
//
// OTel's DEFAULT histogram boundaries are 0, 5, 10, 25, 50, 75, 100, 250, 500, 750,
// 1000, 2500, 5000, 7500, 10000 — chosen for milliseconds. This instrument records
// SECONDS, so under those defaults every realistic publish latency falls in the single
// bucket [0, 5] and histogram_quantile can only interpolate inside a five-second span:
// the sub-two-second p99 target would be unmeasurable, and a regression from 50 ms to
// 4 s would not move the reported quantile at all. Nothing about that failure is
// visible in a build, in promtool, or in a "does not panic" test.
//
// So this test asserts the boundaries actually in force on the collected data point,
// and asserts specifically that 2 is one of them: with a bucket edge exactly at the
// threshold, "is p99 under 2 seconds" is answered from bucket counts rather than
// estimated across it.
func TestEventPublishDuration_UsesExplicitSubTwoSecondBuckets(t *testing.T) {
	reader := installSDKReader(t)

	// A topic of this test's own. The shared reader is cumulative, so a data point
	// keyed on a topic other tests also use would carry their measurements too and the
	// bucket-placement assertion below would count them.
	const scopedTopic = "blnk.bucket-boundaries.test"

	EventPublishDuration.Record(context.Background(), 0.42, metric.WithAttributes(
		attribute.String("topic", scopedTopic),
		attribute.String("attempt", "1"),
		attribute.String("outcome", "dispatched"),
	))

	collected := collectScopeMetrics(t, reader)

	metricData, ok := collected["blnk.events.publish.duration"]
	require.True(t, ok, "the publish-duration histogram was not exported")

	histogram, ok := metricData.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "blnk.events.publish.duration must aggregate as a float64 histogram, got %T", metricData.Data)
	require.NotEmpty(t, histogram.DataPoints, "the histogram reported no data points")

	var point metricdata.HistogramDataPoint[float64]
	var found bool
	for _, candidate := range histogram.DataPoints {
		if topic, ok := candidate.Attributes.Value("topic"); ok && topic.AsString() == scopedTopic {
			point = candidate
			found = true

			break
		}
	}
	require.True(t, found, "the scoped measurement produced no data point")

	assert.Equal(t, EventPublishDurationBuckets, point.Bounds,
		"the histogram is not using its declared explicit boundaries; "+
			"OTel's millisecond-scale defaults make a sub-2s p99 unmeasurable on a second-valued instrument")
	assert.Contains(t, point.Bounds, float64(2),
		"2 must be a bucket EDGE so the two-second target is read from counts rather than interpolated across it")
	assert.NotContains(t, point.Bounds, float64(10000),
		"10000s is an OTel default millisecond boundary and has no meaning on a second-valued instrument")

	// The recorded 0.42s must land below the 0.5 edge, which is the cheapest possible
	// proof that the boundaries are in seconds rather than milliseconds.
	require.Len(t, point.BucketCounts, len(point.Bounds)+1, "bucket counts must be boundaries+1")
	edge := indexOfBound(point.Bounds, 0.5)
	require.GreaterOrEqual(t, edge, 0, "0.5 must be a boundary")
	assert.Equal(t, uint64(1), point.BucketCounts[edge],
		"a 0.42s measurement must fall in the bucket ending at 0.5s")
}

// TestEventCaptureToDispatchDuration_BracketsTheTargetAndKeepsABacklogOnScale is the
// bucket assertion for the instrument acceptance criterion V-1 is ACTUALLY read from.
//
// The per-write histogram cannot answer V-1 — its clock starts after the outbox claim, so it
// excludes the pending wait and the poll interval, and a relay stalled for a minute still
// reported a five-millisecond publish. This instrument spans capture to acknowledgement, and
// its boundaries therefore have to do two things the per-write set does not: put an edge
// exactly at the two-second target, and keep a genuinely backlogged pipeline on the scale
// instead of collapsing every stalled event into +Inf, where one minute behind and five are
// indistinguishable.
func TestEventCaptureToDispatchDuration_BracketsTheTargetAndKeepsABacklogOnScale(t *testing.T) {
	reader := installSDKReader(t)

	// A topic of this test's own: the shared reader is cumulative, so a data point keyed on a
	// topic another test also uses would carry its measurements into the placement assertion.
	const scopedTopic = "blnk.capture-to-dispatch.test"

	EventCaptureToDispatchDuration.Record(context.Background(), 1.75, metric.WithAttributes(
		attribute.String("topic", scopedTopic),
		attribute.String("attempt", "1"),
	))

	collected := collectScopeMetrics(t, reader)

	metricData, ok := collected["blnk.events.capture_to_dispatch.duration"]
	require.True(t, ok, "the capture-to-dispatch histogram was not exported")

	histogram, ok := metricData.Data.(metricdata.Histogram[float64])
	require.True(t, ok,
		"blnk.events.capture_to_dispatch.duration must aggregate as a float64 histogram, got %T", metricData.Data)
	require.NotEmpty(t, histogram.DataPoints, "the histogram reported no data points")

	var point metricdata.HistogramDataPoint[float64]
	var found bool
	for _, candidate := range histogram.DataPoints {
		if topic, ok := candidate.Attributes.Value("topic"); ok && topic.AsString() == scopedTopic {
			point = candidate
			found = true

			break
		}
	}
	require.True(t, found, "the scoped measurement produced no data point")

	assert.Equal(t, EventCaptureToDispatchDurationBuckets, point.Bounds,
		"the histogram is not using its declared explicit boundaries, so the p99 the acceptance "+
			"criterion is read from would be interpolated across a five-second default bucket")
	assert.Contains(t, point.Bounds, float64(2),
		"2 must be a bucket EDGE so the two-second target is answered from counts rather than interpolated")
	assert.Contains(t, point.Bounds, float64(31),
		"31s is the whole configured retry schedule (1+2+4+8+16), so an event that spent its entire "+
			"budget must land on an edge rather than be blended into a neighbour")
	assert.Contains(t, point.Bounds, float64(300),
		"a backlogged pipeline must stay on the scale: without a long edge, one minute behind and five "+
			"are indistinguishable in +Inf")

	// 1.75s must fall in the bucket ending at 2s: the cheapest possible proof that the
	// boundaries are seconds and that the target edge is where the target is.
	require.Len(t, point.BucketCounts, len(point.Bounds)+1, "bucket counts must be boundaries+1")
	edge := indexOfBound(point.Bounds, 2)
	require.GreaterOrEqual(t, edge, 0, "2 must be a boundary")
	assert.Equal(t, uint64(1), point.BucketCounts[edge],
		"a 1.75s end-to-end age must fall in the bucket ending at 2s, which is the bucket the target reads")
}

// indexOfBound returns the index of bound in bounds, or -1.
func indexOfBound(bounds []float64, bound float64) int {
	for i, candidate := range bounds {
		if candidate == bound {
			return i
		}
	}

	return -1
}

// TestEventStreamingInstruments_ExportedAttributeKeys asserts the attribute KEYS that
// reach the exporter on each instrument.
//
// These keys are a published contract, not an implementation detail: the two alert
// rules interpolate {{ $labels.topic }}, {{ $labels.subscriber }} and
// {{ $labels.group }} into the notification an operator is paged with, and the latency
// query selects on attempt and outcome. A renamed key leaves the alert firing with an
// empty interpolation, or leaves the query selecting nothing.
func TestEventStreamingInstruments_ExportedAttributeKeys(t *testing.T) {
	reader := installSDKReader(t)
	recordEveryEventInstrument(context.Background())
	collected := collectScopeMetrics(t, reader)

	cases := []struct {
		metric string
		keys   []string
	}{
		{metric: "blnk.events.published.total", keys: []string{"topic", "event_type"}},
		// The terminal counter, attributed exactly as the published counter is. The pair is what
		// makes "how many events are still mid-flight" a subtraction rather than a guess, and a
		// divergence in labels would make the two unsubtractable.
		{metric: "blnk.events.dispatched.total", keys: []string{"topic", "event_type"}},
		// Both dimensions. `terminal` is not decoration: with the outcome vocabulary frozen at
		// three values it is the ONLY thing separating a failure that will be retried from one
		// that never will, so a counter that lost it could no longer answer the question the
		// dead-letter triage runbook opens with.
		{metric: "blnk.events.publish.attempts.total", keys: []string{"outcome", "terminal"}},
		{metric: "blnk.events.publish.duration", keys: []string{"topic", "attempt", "outcome"}},
		// No outcome: only an acknowledged publish has an end-to-end age to report, so the
		// attribute would carry one value on every series and add nothing.
		{metric: "blnk.events.capture_to_dispatch.duration", keys: []string{"topic", "attempt"}},
		{metric: "blnk.events.dead_lettered.total", keys: []string{"topic", "event_type"}},
		{metric: "blnk.dlt.oldest_message_age_seconds", keys: []string{"topic"}},
		{metric: "blnk.kafka.consumer_lag", keys: []string{"subscriber", "group", "topic"}},
		// The measurement-health gauge shares the lag gauge's label tuple exactly, because
		// the two are observed from ONE sample: an operator correlating "this subscriber's
		// lag vanished" with "because two of its partitions are unreadable" joins on these
		// three labels, and a divergence would make that join impossible.
		{
			metric: "blnk.kafka.consumer_lag_unmeasured_partitions",
			keys:   []string{"subscriber", "group", "topic"},
		},
		// Deliberately unattributed: one process has one outbox backlog, so a label
		// would add cardinality without adding information.
		{metric: "blnk.outbox.pending", keys: nil},
		// The coverage boolean is one fact about one sweep, so it carries nothing. A
		// per-subscriber breakdown would defeat its purpose: the whole reason it exists is
		// that an unmeasured subscriber has no series of its own to attribute anything to.
		{metric: "blnk.kafka.consumer_lag_inventory_complete", keys: nil},
		// The reason is the entire value of this gauge — SubscriberLagCoverageIncomplete's
		// remediation branches on it, naming each value and its own action — so the key is a
		// published contract and not an incidental label.
		{metric: "blnk.kafka.subscribers_unmeasured", keys: []string{"reason"}},
		// Which collection failed, for the same reason: EventMetricsCollectionFailing's
		// remediation reads this attribute to separate a database fault from a broker one.
		{metric: "blnk.event_metrics.collection_failures.total", keys: []string{"collection"}},
		// The collection-health pair describes the collector itself, of which a process has
		// exactly one, so both are unattributed. EventMetricsCollectionAbsent selects on the
		// SCRAPE job label rather than on anything the instrument carries, which is why no
		// attribute is needed to scope it to the server role.
		{metric: "blnk.event_metrics.last_collection_age_seconds", keys: nil},
		{metric: "blnk.event_metrics.last_success_age_seconds", keys: nil},
		// PURPOSE, and only this instrument carries it. It is what separates an original
		// publish from a replay and from a dead-letter write, so a comparison against
		// published.total has to filter on it — an unfiltered comparison counts triage as
		// re-delivery.
		{
			metric: "blnk.events.broker_acknowledgements.total",
			keys:   []string{"topic", "event_type", "purpose"},
		},
		// The four unsettled-state markers describe the registry as a whole, of which a
		// deployment has one, so none of them carries a label. Their per-subscriber detail is
		// read from GET /subscribers, which is where a triage runbook sends an operator.
		{metric: "blnk.subscribers.credential_orphans", keys: nil},
		{metric: "blnk.subscribers.oldest_credential_orphan_age_seconds", keys: nil},
		{metric: "blnk.subscribers.revocation_failures", keys: nil},
		{metric: "blnk.subscribers.oldest_revocation_failure_age_seconds", keys: nil},
		// The registry size, unattributed, so the unmeasured count above reads as a
		// proportion of it.
		{metric: "blnk.subscribers.registered", keys: nil},
		// The lag sweep's own progress, one pass per process, so neither carries a label.
		{metric: "blnk.kafka.consumer_lag.pass_age_seconds", keys: nil},
		{metric: "blnk.kafka.consumer_lag.covered_subscribers", keys: nil},
		// The retention sweep's own count, unattributed: one process runs one sweep over one
		// table, and it is read against eligibility rather than broken down.
		{metric: "blnk.events.purged.total", keys: nil},
		// The revocation and settlement aggregates, each a marker paired with the age its
		// age-based alert is stated over. All unattributed for the same reason as the orphan
		// pair above: they describe the registry, and the per-subscriber detail is read from
		// GET /subscribers.
		{metric: "blnk.subscribers.revocation_pending", keys: nil},
		{metric: "blnk.subscribers.oldest_revocation_age_seconds", keys: nil},
		{metric: "blnk.subscribers.settlement_outstanding", keys: nil},
		{metric: "blnk.subscribers.grant_reconcile_pending", keys: nil},
		{metric: "blnk.subscribers.credential_cleanup_pending", keys: nil},
		{metric: "blnk.subscribers.oldest_settlement_age_seconds", keys: nil},
		// A COUNTER, and unattributed: SubscriberSettlementNotProgressing takes a rate over it
		// to separate a stuck pass from a busy one, and that question has no per-subject
		// breakdown.
		{metric: "blnk.subscribers.obligations_settled.total", keys: nil},
	}

	require.Len(t, cases, len(eventStreamingInstruments()),
		"every event-streaming instrument must have its attribute keys asserted here; this "+
			"table had drifted five instruments behind the declarations, and an unasserted "+
			"instrument is one whose labels can be renamed without anything noticing")

	for _, tc := range cases {
		t.Run(tc.metric, func(t *testing.T) {
			metricData, ok := collected[tc.metric]
			require.True(t, ok, "%s was not exported", tc.metric)

			keys := attributeKeysOf(t, metricData)
			assert.ElementsMatch(t, tc.keys, keys,
				"%s carries the wrong attribute keys; the alert rules and latency queries name these", tc.metric)
		})
	}
}

// attributeKeysOf returns the attribute keys a collected metric's data points carry, and
// requires that EVERY data point agrees on them.
//
// The agreement is asserted rather than assumed. Reading the first data point and trusting it
// was the original shape of this helper, and the order of data points in a collection is not
// defined — so with more than one series present the answer was whichever the SDK happened to
// emit first. That made the attribute-key contract assertions nondeterministic the moment
// anything else had recorded the same instrument with a different key set, which is exactly what
// happened from the second `-count` iteration onwards.
//
// Requiring one key set across every point is also the stronger statement. These keys are a
// PUBLISHED contract — the alert rules interpolate them and the latency queries select on them —
// and a second series of the same instrument carrying a different tuple means one of the two
// producers is off-contract. A helper that reads one point cannot see that at all.
//
// The four aggregation shapes are handled explicitly rather than through reflection so
// that an instrument whose KIND changed — a counter declared as a gauge, say — fails
// here with a message naming the aggregation it actually produced.
func attributeKeysOf(t *testing.T, m metricdata.Metrics) []string {
	t.Helper()

	var sets []attribute.Set
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		for _, point := range data.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Gauge[int64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		for _, point := range data.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Gauge[float64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		for _, point := range data.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Histogram[float64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		for _, point := range data.DataPoints {
			sets = append(sets, point.Attributes)
		}
	default:
		t.Fatalf("%s aggregated as an unexpected shape %T", m.Name, m.Data)
	}

	keys := attributeSetKeys(sets[0])
	for _, set := range sets[1:] {
		require.ElementsMatchf(t, keys, attributeSetKeys(set),
			"%s exported two series with DIFFERENT attribute keys, %v and %v. The keys are a "+
				"published contract the alert rules and latency queries name, so two producers "+
				"disagreeing about them means one of them is off-contract",
			m.Name, keys, attributeSetKeys(set))
	}

	return keys
}

// attributeSetKeys returns the keys of an attribute set, in the set's own order.
func attributeSetKeys(set attribute.Set) []string {
	keys := make([]string, 0, set.Len())
	for _, kv := range set.ToSlice() {
		keys = append(keys, string(kv.Key))
	}

	return keys
}

// TestEventPublishTelemetry_LabelDomainsStayClosed records every legal value of the two
// bounded attributes and asserts the resulting series count is exactly the size of the
// declared domains.
//
// This is a CARDINALITY BUDGET expressed as a test. The attempt attribute sits on a
// histogram, so each of its values costs a full set of buckets; letting an unbounded
// value in — an attempt number straight from a row whose max_attempts was raised in the
// database, or an attempt count invented for a replay — is how a single instrument comes
// to dominate the exporter. Recording the whole domain and counting the result is what
// makes an accidental widening visible: a new value shows up as this count being wrong,
// which is a test edit rather than a silent production cost.
func TestEventPublishTelemetry_LabelDomainsStayClosed(t *testing.T) {
	reader := installSDKReader(t)
	ctx := context.Background()

	// A topic value used by no other test, so the series counted below are exactly the
	// ones recorded here. The shared reader is cumulative, so counting every series
	// present would count whatever the other tests recorded too.
	const scopedTopic = "blnk.cardinality-budget.test"

	for _, attempt := range publishAttemptLabels {
		for _, outcome := range publishOutcomes {
			EventPublishDuration.Record(ctx, 0.1, metric.WithAttributes(
				attribute.String("topic", scopedTopic),
				attribute.String("attempt", attempt),
				attribute.String("outcome", outcome),
			))
		}
	}
	for _, outcome := range publishOutcomes {
		for _, terminal := range publishTerminalValues {
			EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("outcome", outcome),
				attribute.String("terminal", terminal),
			))
		}
	}

	collected := collectScopeMetrics(t, reader)

	histogram, ok := collected["blnk.events.publish.duration"].Data.(metricdata.Histogram[float64])
	require.True(t, ok, "the publish-duration histogram was not exported as a float64 histogram")

	scoped := 0
	for _, point := range histogram.DataPoints {
		if topic, found := point.Attributes.Value("topic"); found && topic.AsString() == scopedTopic {
			scoped++
		}
	}
	assert.Equal(t, len(publishAttemptLabels)*len(publishOutcomes), scoped,
		"one series per (attempt, outcome) pair for a single topic: %d attempt values x %d outcomes. "+
			"A larger number means a value outside the declared domains reached the histogram",
		len(publishAttemptLabels), len(publishOutcomes))

	attempts, ok := collected["blnk.events.publish.attempts.total"].Data.(metricdata.Sum[int64])
	require.True(t, ok, "the attempts counter was not exported as an int64 sum")
	// The PRODUCT of the two domains, not the outcome domain alone. The counter carries
	// `terminal` beside the outcome, so its budget is 3 x 2; asserting the smaller number
	// would understate the exporter's real series count by half and pass while a third
	// dimension crept in.
	assert.Len(t, attempts.DataPoints, len(publishOutcomes)*len(publishTerminalValues),
		"the attempts counter carries outcome AND terminal, so its series count is the product "+
			"of the two closed domains: %d outcomes x %d terminal values",
		len(publishOutcomes), len(publishTerminalValues))
	assert.True(t, attempts.IsMonotonic, "an attempts counter must be monotonic")
}

// TestSubscriberUnmeasuredReasons_IsAClosedNonDegenerateVocabulary pins the domain of the one
// attribute an alert's REMEDIATION branches on.
//
// SubscriberLagCoverageIncomplete does not tell an operator to go and investigate; it names each
// reason and gives each its own action, because the five mean five different faults in five
// different places — the budget, the registry row, the broker, the registry query and the topic
// provisioning. That makes this vocabulary a published contract rather than a set of log-ish
// strings, and it has two failure modes worth mechanically excluding.
//
// A reason ADDED here and not added to the remediation leaves an operator holding a series with a
// reason the runbook does not explain. A reason RENAMED leaves the remediation describing a value
// that no longer exists while saying nothing about the one that does. Neither breaks a build,
// neither fails promtool, and both are only ever discovered mid-incident — so the enumeration is
// asserted exactly, and event_metrics_test.go asserts that the alert text still names every
// member of it.
//
// The duplicate and empty checks are not padding. The collector writes the gauge once per value
// returned here, so a duplicate would record the same series twice per tick — the second write
// silently overwriting the first with a different number — and an empty string would export a
// series whose reason cannot be read at all.
func TestSubscriberUnmeasuredReasons_IsAClosedNonDegenerateVocabulary(t *testing.T) {
	reasons := SubscriberUnmeasuredReasons()

	assert.Equal(t, []string{
		SubscribersUnmeasuredReasonBudget,
		SubscribersUnmeasuredReasonUnprovisioned,
		SubscribersUnmeasuredReasonMeasureFailed,
		SubscribersUnmeasuredReasonRegistryFailed,
		SubscribersUnmeasuredReasonTopicMissing,
	}, reasons,
		"the reason domain is a published contract: SubscriberLagCoverageIncomplete's remediation "+
			"branches on every one of these values, so adding or renaming one without updating that "+
			"rule leaves an operator with a series the runbook cannot explain")

	seen := make(map[string]struct{}, len(reasons))
	for _, reason := range reasons {
		assert.NotEmpty(t, reason, "an empty reason exports a series nobody can interpret")

		_, duplicate := seen[reason]
		assert.False(t, duplicate,
			"reason %q appears twice, so the collector would record the same series twice per tick "+
				"and the second write would silently replace the first", reason)
		seen[reason] = struct{}{}
	}

	// A FRESH SLICE, so a caller cannot mutate the vocabulary for every other caller in the
	// process. The collector iterates this on every tick; a handler that sorted or truncated the
	// returned slice in place would change what is published from then on.
	first := SubscriberUnmeasuredReasons()
	first[0] = "mutated-by-a-caller"
	assert.Equal(t, reasons, SubscriberUnmeasuredReasons(),
		"the vocabulary must be returned as a copy; a caller mutating it would change what every "+
			"later tick publishes")
}

// TestEventStreamingInstruments_ExportedPrometheusSeriesNames asserts the names the ALERT RULES
// actually match on, as the Prometheus exporter produces them.
//
// # Why the OTel name is not enough
//
// Every other test in this file checks the OTel instrument name. The rules do not use that name:
// they use the exporter's translation of it, and the translation is not a mechanical
// dot-to-underscore substitution. The exporter also appends a suffix DERIVED FROM THE UNIT — a
// unit of "1" becomes "_ratio", "s" becomes "_seconds", a monotonic counter gains "_total" — so
// a unit is part of a series name whether or not anybody intended it to be.
//
// That is not hypothetical here. blnk.kafka.consumer_lag_inventory_complete was declared with the
// UCUM unit "1", which is correct for a dimensionless value and which the exporter renders as
// blnk_kafka_consumer_lag_inventory_complete_ratio. SubscriberLagCoverageIncomplete matched the
// unsuffixed name, so it evaluated against a series that did not exist — and a rule over a
// missing series cannot fire and is indistinguishable from a system with nothing wrong. The Go
// build passed, every OTel-name assertion passed, and promtool validated the rule.
//
// So the exporter is run for real and the exposition text is searched for the exact names the
// rule files use. A unit or name change that moves a series now fails HERE, at the boundary the
// alerting depends on, rather than silently in production.
func TestEventStreamingInstruments_ExportedPrometheusSeriesNames(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	require.NoError(t, err, "building the Prometheus exporter")

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	// A meter from THIS provider, and every instrument rebuilt on it: the package-level
	// instruments belong to whatever provider was installed at import time, so recording through
	// them would not reach this exporter.
	restore := swapMeter(t, provider.Meter("blnk"))
	defer restore()

	require.NoError(t, Init(), "re-initialising the instruments against the Prometheus exporter")
	recordEveryEventInstrument(context.Background())

	exposition := scrapeRegistry(t, registry)

	// Exactly the names alerts/blnk-kafka-alerts.yml and docs/metrics.md use. Written out as
	// literals rather than derived from the instrument names, because deriving them would
	// reproduce the very assumption that broke.
	for _, series := range []string{
		"blnk_events_published_total",
		"blnk_events_broker_acknowledgements_total",
		"blnk_events_dispatched_total",
		"blnk_events_publish_attempts_total",
		// HISTOGRAMS ARE ASSERTED ON THEIR _bucket SERIES, because that is the series the
		// documented p99 queries feed to histogram_quantile — a histogram exports
		// _bucket, _sum and _count and no bare sample at all, so asserting the bare name
		// would fail for a correctly exported instrument.
		"blnk_events_publish_duration_seconds_bucket",
		"blnk_events_capture_to_dispatch_duration_seconds_bucket",
		"blnk_events_dead_lettered_total",
		"blnk_dlt_oldest_message_age_seconds",
		"blnk_kafka_consumer_lag",
		"blnk_kafka_consumer_lag_unmeasured_partitions",
		"blnk_kafka_consumer_lag_inventory_complete",
		"blnk_kafka_subscribers_unmeasured",
		"blnk_outbox_pending",
		"blnk_events_purged_total",
		"blnk_subscribers_revocation_pending",
		"blnk_subscribers_oldest_revocation_age_seconds",
		"blnk_event_metrics_last_collection_age_seconds",
		"blnk_event_metrics_last_success_age_seconds",
		"blnk_event_metrics_collection_failures_total",
	} {
		t.Run(series, func(t *testing.T) {
			assert.True(t, expositionHasSeries(exposition, series),
				"no exported sample is named %q. The alert rules and the documented queries match on "+
					"this exact name, so if the exporter renamed it — most likely by appending a unit "+
					"suffix — every rule over it silently evaluates nothing and can never fire",
				series)
		})
	}

	t.Run("the success age is published even when no collection has ever succeeded", func(t *testing.T) {
		// The worst state there is, and the one absence would hide: failing since start-up. The
		// gauge must still carry a value, or EventMetricsCollectionFailing has no series to
		// evaluate for precisely the collector that has never worked.
		resetEventMetricsCollectionHealth(t)
		RecordEventMetricsCollection(time.Now().Add(-10*time.Minute), false)

		text := scrapeRegistry(t, registry)
		assert.True(t, expositionHasSeries(text, "blnk_event_metrics_last_success_age_seconds"),
			"a collector that has never completed a clean collection must still report a success age, "+
				"measured from when it started; reporting nothing makes the most degraded state the one "+
				"state a threshold rule cannot detect")
	})
}

// swapMeter points the package meter at a different one for the duration of a test.
//
// Returns:
//   - func(): restores the original meter. Also registered with t.Cleanup, so a test that
//     forgets to call it still cannot leak the swap into another test.
func swapMeter(t *testing.T, replacement metric.Meter) func() {
	t.Helper()

	original := meter
	restore := func() { meter = original }
	t.Cleanup(restore)
	meter = replacement

	return restore
}

// resetEventMetricsCollectionHealth clears the recorded collection timestamps.
//
// Necessary because they are process-global and monotonic: a success recorded by an earlier test
// cannot be undone by recording an older failure, so the "never succeeded" case is only reachable
// from a cleared state.
func resetEventMetricsCollectionHealth(t *testing.T) {
	t.Helper()

	eventMetricsCollectionHealth.mu.Lock()
	defer eventMetricsCollectionHealth.mu.Unlock()

	eventMetricsCollectionHealth.firstAttempt = time.Time{}
	eventMetricsCollectionHealth.lastAttempt = time.Time{}
	eventMetricsCollectionHealth.lastSuccess = time.Time{}
}

// scrapeRegistry gathers a Prometheus registry and renders it as exposition text.
//
// Returns:
//   - string: the exposition body, one sample per line.
func scrapeRegistry(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err, "gathering the Prometheus registry")
	require.NotEmpty(t, families, "the registry produced no metric families")

	var rendered strings.Builder
	encoder := expfmt.NewEncoder(&rendered, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		require.NoError(t, encoder.Encode(family), "encoding %s", family.GetName())
	}

	return rendered.String()
}

// expositionHasSeries reports whether exposition text carries a sample with the given name.
//
// Matched on the sample line's leading token so that a name which is a PREFIX of another cannot
// satisfy the assertion — which is the whole failure mode being guarded, since the suffix the
// exporter appends is what moves a series.
func expositionHasSeries(exposition, series string) bool {
	for _, line := range strings.Split(exposition, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}

		name := line
		if index := strings.IndexAny(line, "{ "); index >= 0 {
			name = line[:index]
		}
		if name == series {
			return true
		}
	}

	return false
}

// declaredInstrumentNames parses metrics.go and returns the name of every EXPORTED
// package-level variable whose declared type comes from the otel metric package.
//
// It reads the source rather than using reflection because the property under test is a
// property of the DECLARATIONS, not of the values. Reflection can only see variables a
// test already names, which is precisely the blind spot this helper exists to remove: an
// instrument nobody enumerated is invisible to a reflective walk for the same reason it is
// invisible to the enumeration tables.
func declaredInstrumentNames(t *testing.T) []string {
	t.Helper()

	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "metrics.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "metrics.go must be parseable for the declaration inventory to be trustworthy")

	names := make([]string, 0, 32)

	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}

		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || value.Type == nil {
				continue
			}

			selector, ok := value.Type.(*ast.SelectorExpr)
			if !ok {
				continue
			}

			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Name != "metric" {
				continue
			}

			for _, name := range value.Names {
				if name.IsExported() {
					names = append(names, name.Name)
				}
			}
		}
	}

	require.NotEmpty(t, names, "no instrument declarations were found, so this guard would pass vacuously")
	sort.Strings(names)

	return names
}

// TestInstrumentTables_CoverEveryDeclaredInstrumentExactlyOnce is the guard that keeps the
// two enumeration tables — and every count stated in this file's prose — honest.
//
// Both tables are written out by hand on purpose: that is what makes every non-nil
// assertion above a real assertion rather than a tautology against the thing under test.
// The cost of a hand-written table is that it can fall behind the code, and it did: an
// instrument was declared, assigned in Init() and used in production while appearing in
// NEITHER table, so the orphan guard silently covered one fewer instrument than its own
// documentation claimed. Nothing failed, because a table that omits an entry cannot notice
// the omission.
//
// This test closes that loop from the other direction. It derives the truth from the
// declarations in metrics.go and asserts a bidirectional match:
//
//   - every declared instrument appears in exactly one table, so a newly declared
//     instrument cannot be added without being brought under the non-nil guard, and
//   - every table entry corresponds to a real declaration, so a removed instrument cannot
//     leave a stale name behind that would fail to compile only after someone else's change.
//
// The failure messages name the specific instrument and the specific table to update,
// because "the counts disagree" is not actionable at the moment the test goes red.
func TestInstrumentTables_CoverEveryDeclaredInstrumentExactlyOnce(t *testing.T) {
	declared := declaredInstrumentNames(t)

	enumerated := make(map[string]int, len(declared))
	for _, instrument := range preExistingInstruments() {
		enumerated[instrument.name]++
	}

	for _, instrument := range eventStreamingInstruments() {
		enumerated[instrument.name]++
	}

	for _, name := range declared {
		assert.Equalf(t, 1, enumerated[name],
			"instrument %s is declared in metrics.go but appears %d times across preExistingInstruments() and "+
				"eventStreamingInstruments(); every declared instrument must appear in exactly one of them or it "+
				"is not covered by the non-nil orphan guard", name, enumerated[name])
	}

	declaredSet := make(map[string]struct{}, len(declared))
	for _, name := range declared {
		declaredSet[name] = struct{}{}
	}

	for name := range enumerated {
		_, ok := declaredSet[name]
		assert.Truef(t, ok,
			"%s is enumerated by a table in this file but is no longer declared as an otel instrument in "+
				"metrics.go; remove it from the table", name)
	}

	// The totals are asserted separately from the membership check so that a red test says
	// which of the two went wrong: a miscount, or a genuine mismatch.
	assert.Equalf(t, len(declared), len(preExistingInstruments())+len(eventStreamingInstruments()),
		"metrics.go declares %d instruments but the tables enumerate %d in total (%d pre-existing + %d "+
			"event-streaming); the prose counts in this file's header are derived from these numbers and must "+
			"be updated with them", len(declared), len(preExistingInstruments())+len(eventStreamingInstruments()),
		len(preExistingInstruments()), len(eventStreamingInstruments()))

	// A declaration that is never assigned stays nil forever and panics at its first
	// production call site. The tables above are what defend against that, so a table
	// entry naming a variable Init() never touches is itself a defect worth reporting.
	source, err := parser.ParseFile(token.NewFileSet(), "metrics.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)

	var initBody strings.Builder
	for _, declaration := range source.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "Init" || function.Body == nil {
			continue
		}

		for _, statement := range function.Body.List {
			assignment, ok := statement.(*ast.AssignStmt)
			if !ok {
				continue
			}

			for _, target := range assignment.Lhs {
				if identifier, ok := target.(*ast.Ident); ok {
					initBody.WriteString(identifier.Name)
					initBody.WriteString("\n")
				}
			}
		}
	}

	assigned := initBody.String()
	for _, name := range declared {
		assert.Containsf(t, assigned, name+"\n",
			"%s is declared as an instrument but Init() never assigns it, so it stays nil and the first "+
				"production call site panics on a nil interface", name)
	}
}
