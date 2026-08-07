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
// Init() and log.Fatalf's if it returns an error, twenty-three exported instrument
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
//     declaration — precisely the mistake that appending seven new instruments to
//     a list of sixteen invites.
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
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
)

// publishOutcomes is the complete vocabulary of the "outcome" attribute carried by
// EventPublishAttemptsTotal and EventPublishDuration, matching the documented
// attribute list on their declarations in metrics.go.
//
// Note the UNDERSCORE in dead_lettered. Surrounding prose spells the concept
// "dead-lettered" with a hyphen; that is not the attribute value, and a metric
// consumer filtering on the hyphenated spelling would match nothing.
//
// "failed" is here because it is a DISTINCT outcome from "retrying" and not a
// synonym for it: an attempt that failed permanently, or that spent the last of the
// retry budget, will never be retried, and reporting it as retrying would report
// retry pressure that does not exist. The four values are mutually exclusive and
// exhaustive, so their sum is the total number of attempted writes.
//
// Spelled as literals deliberately: this test file stays as dependency-free as the
// package it covers, so it does not import the model package merely to obtain four
// strings.
var publishOutcomes = []string{"dispatched", "retrying", "failed", "dead_lettered"}

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

// eventStreamingInstruments returns the eight instruments added for the Kafka
// event-publishing pipeline, each labelled with its variable name.
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
		{"EventPublishAttemptsTotal", EventPublishAttemptsTotal},
		{"EventPublishDuration", EventPublishDuration},
		{"EventCaptureToDispatchDuration", EventCaptureToDispatchDuration},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds},
		{"SubscriberConsumerLag", SubscriberConsumerLag},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions},
		{"OutboxPendingBacklog", OutboxPendingBacklog},
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
// assignment chain — all twenty-three instruments, the sixteen original ones and
// the seven event-streaming additions — without the meter refusing to build one.
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
// this package is by itself enough to leave all seven event-streaming instruments
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
// the seven event-streaming additions did not disturb any of the sixteen
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
		{"EventPublishAttemptsTotal", EventPublishAttemptsTotal, (*metric.Int64Counter)(nil)},
		{"EventPublishDuration", EventPublishDuration, (*metric.Float64Histogram)(nil)},
		{"EventCaptureToDispatchDuration", EventCaptureToDispatchDuration, (*metric.Float64Histogram)(nil)},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal, (*metric.Int64Counter)(nil)},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds, (*metric.Float64Gauge)(nil)},
		{"SubscriberConsumerLag", SubscriberConsumerLag, (*metric.Int64ObservableGauge)(nil)},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions, (*metric.Int64ObservableGauge)(nil)},
		{"OutboxPendingBacklog", OutboxPendingBacklog, (*metric.Int64Gauge)(nil)},
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
// The vocabulary is FOUR values, not three. "retrying" and "failed" are separate
// outcomes because a failure that will be retried and a failure that will not are
// different operational states: labelling a permanent failure, or the one that spent
// the last of the retry budget, as "retrying" reports retry pressure that no longer
// exists and hides the events that are actually stuck. Any change to this list is a
// change to a published attribute domain and has to be made deliberately here, on the
// instrument's declaration, and in anything querying it.
func TestEventPublishAttemptsTotal_AcceptsEveryPublishOutcome(t *testing.T) {
	ctx := context.Background()

	require.Len(t, publishOutcomes, 4,
		"the outcome vocabulary is dispatched, retrying, failed and dead_lettered; "+
			"a fifth publish outcome needs a deliberate decision here and in the alerting rules")
	require.Contains(t, publishOutcomes, "dead_lettered",
		"the dead-letter outcome is spelled with an underscore, not a hyphen")
	require.Contains(t, publishOutcomes, "failed",
		"a terminal failure must be distinguishable from one that will be retried")

	for _, outcome := range publishOutcomes {
		t.Run(outcome, func(t *testing.T) {
			require.NotPanics(t, func() {
				EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
					attribute.String("outcome", outcome),
				))
			})
		})
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
				EventPublishDuration.Record(ctx, float64(attempt)*0.25, metric.WithAttributes(
					attribute.String("topic", "blnk.transactions"),
					attribute.String("attempt", label),
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

// installSDKReader returns the shared SDK-backed manual reader, installing the provider
// and rebuilding every instrument through it on first use.
//
// Init() is called AFTER the provider is installed, which is what makes the instruments
// real SDK instruments rather than delegating no-ops: an instrument created before
// delegation replays its creation, but creating it afterwards is direct and leaves no room
// for the replay to lose an option — and the option that matters most here, the explicit
// bucket boundaries, is exactly the kind of thing a replay could drop.
//
// The reader accumulates cumulatively across the tests that share it, so a test asserting
// on SERIES COUNTS must scope itself with an attribute value of its own rather than
// counting everything present.
//
// Returns:
//   - *sdkmetric.ManualReader: the reader to collect from.
func installSDKReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	installOnce.Do(func() {
		sharedReader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(sharedReader)))
	})

	require.NoError(t, Init(), "Init must rebuild every instrument through the installed provider")
	require.NotNil(t, sharedReader, "the shared manual reader was not installed")

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
// seven event-streaming instruments, using the attribute keys their declarations
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
	EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", "dispatched"),
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
			name:        "blnk.events.published.total",
			unit:        "{event}",
			description: "Total number of ledger events durably recorded as dispatched by topic and event type",
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
		{metric: "blnk.events.publish.attempts.total", keys: []string{"outcome"}},
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
	}

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

// attributeKeysOf returns the attribute keys of a collected metric's first data point.
//
// The four aggregation shapes are handled explicitly rather than through reflection so
// that an instrument whose KIND changed — a counter declared as a gauge, say — fails
// here with a message naming the aggregation it actually produced.
func attributeKeysOf(t *testing.T, m metricdata.Metrics) []string {
	t.Helper()

	var set attribute.Set
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		set = data.DataPoints[0].Attributes
	case metricdata.Gauge[int64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		set = data.DataPoints[0].Attributes
	case metricdata.Gauge[float64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		set = data.DataPoints[0].Attributes
	case metricdata.Histogram[float64]:
		require.NotEmpty(t, data.DataPoints, "%s reported no data points", m.Name)
		set = data.DataPoints[0].Attributes
	default:
		t.Fatalf("%s aggregated as an unexpected shape %T", m.Name, m.Data)
	}

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
		EventPublishAttemptsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String("outcome", outcome),
		))
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
	assert.Len(t, attempts.DataPoints, len(publishOutcomes),
		"the attempts counter carries only the outcome attribute, so its series count is the outcome domain")
	assert.True(t, attempts.IsMonotonic, "an attempts counter must be monotonic")
}
