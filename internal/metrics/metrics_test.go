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
// easy-to-break structural property of the package rather than to exercise business
// logic — there is none here.
//
// metrics.go is deliberately flat: one package-level meter, an init() that calls Init()
// and log.Fatalf's if it returns an error, a flat set of exported instrument variables,
// and an Init() that assigns every one of them in declaration order and returns on the
// first error. Two consequences follow, and both are what the tests below assert:
//
//  1. Every instrument is constructed at package-IMPORT time. A meter that rejected an
//     instrument would therefore kill the process at load, and it would take down every
//     package that imports this one — the API server, the workers, the CLI.
//
//  2. A declaration whose matching assignment block is missing from Init() stays nil
//     forever.
//
//     The two inventory functions below, preExistingInstruments and
//     eventStreamingInstruments, ARE that defence, and they are hand-maintained: an
//     instrument absent from both has no orphan guard at all. Adding an instrument
//     to metrics.go therefore means adding it to one of them.

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
// DO NOT DELETE THESE AS "UNUSED".
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
	_ metric.Int64Gauge       = EventRepairBacklog
	_ metric.Int64Counter     = EventRepairsCompletedTotal
	_ metric.Int64Gauge       = EventRepairSaturated

	// The two lag instruments are ASYNCHRONOUS, and the distinction this guard enforces is
	// the whole cardinality fix rather than a stylistic preference. A synchronous
	// Int64Gauge is a write whose aggregator retains every attribute set it has ever been
	// written with, and it has no delete — so with a dynamic label set, subscriber churn
	// grows the series count without bound and no zero written afterwards can retire a
	// series.
	_ metric.Int64ObservableGauge = SubscriberConsumerLag
	_ metric.Int64ObservableGauge = ConsumerLagUnmeasuredPartitions

	// The COLLECTION-HEALTH pair is asynchronous for a different reason from the lag
	// gauges, and it is the reason the kind matters most here: their subject is the
	// collector itself, which may have STOPPED. A synchronous gauge can only be written by
	// the component being observed, so a stalled collector would freeze its own freshness
	// gauge at whatever age it last reported and the series would read as permanently
	// current — the single failure these two instruments exist to make visible.
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
// EventPublishAttemptsTotal and EventPublishDuration, matching the documented attribute
// list on their declarations in metrics.go.
//
// Note the UNDERSCORE in dead_lettered.
var publishOutcomes = []string{"dispatched", "retrying", "dead_lettered"}

// publishTerminalValues is the closed domain of the "terminal" attribute that
// accompanies the outcome on EventPublishAttemptsTotal.
//
// It exists because the outcome vocabulary is frozen at three values, which makes a
// failure that will be retried and one that never will BOTH outcome="retrying".
var publishTerminalValues = []string{"true", "false"}

// maxRelayRetryAttempts is the relay's attempt budget CEILING
// (config.MaxRelayRetryAttempts, which RELAY_MAX_RETRY_ATTEMPTS is clamped to),
// bounding the numeric values the "attempt" attribute can take: 1 through 5 inclusive,
// rendered as strings. Mirrored as a literal for the same reason as publishOutcomes —
// this package reads no configuration, and neither does its test.
const maxRelayRetryAttempts = 5

// publishAttemptLabels is the COMPLETE, closed domain of the "attempt" attribute: the
// five numeric attempts, one overflow bucket, and one fixed token for each of the two
// publishes that are not part of a retry sequence.
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
// This is a FUNCTION and not a package-level table for a load-bearing reason.
func eventStreamingInstruments() []namedInstrument {
	return []namedInstrument{
		{"EventsPublishedTotal", EventsPublishedTotal},
		{"EventsDispatchedTotal", EventsDispatchedTotal},
		{"EventPublishAttemptsTotal", EventPublishAttemptsTotal},
		{"EventPublishDuration", EventPublishDuration},
		{"EventCaptureToDispatchDuration", EventCaptureToDispatchDuration},
		{"EventRelayClaimsTotal", EventRelayClaimsTotal},
		{"EventRelayClaimDuration", EventRelayClaimDuration},
		{"EventRelayClaimedRows", EventRelayClaimedRows},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds},
		{"SubscriberConsumerLag", SubscriberConsumerLag},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions},
		{"OutboxPendingBacklog", OutboxPendingBacklog},
		{"EventRepairBacklog", EventRepairBacklog},
		{"EventRepairsCompletedTotal", EventRepairsCompletedTotal},
		{"EventRepairSaturated", EventRepairSaturated},
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
		{"SubscriberMeasurementBudget", SubscriberMeasurementBudget},
		{"ConsumerLagInventoryComplete", ConsumerLagInventoryComplete},
		{"SubscriberLagPassAgeSeconds", SubscriberLagPassAgeSeconds},
		{"SubscriberLagCoveredSubscribers", SubscriberLagCoveredSubscribers},
		{"EventMetricsCollectionFailuresTotal", EventMetricsCollectionFailuresTotal},
		{"EventMetricsLastCollectionAgeSeconds", EventMetricsLastCollectionAgeSeconds},
		{"EventMetricsLastSuccessAgeSeconds", EventMetricsLastSuccessAgeSeconds},
	}
}

// There are no subscriber-record-delivery instruments in this inventory, and their
// absence is the access model rather than an omission.

// preExistingInstruments returns the instruments OUTSIDE the event-streaming pipeline,
// each labelled with its variable name.
//
// Sixteen of them predate that work, which is where the name comes from; QueueBacklog was
// added later and belongs here rather than in the event-streaming table because it measures
// the worker fleet's queues, not the event pipeline.
//
// See eventStreamingInstruments for why this is a function rather than a package-level
// table.
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
		// Added after the event-streaming work but belonging to this table rather than the
		// other one: it measures the asynq worker fleet's queue depths, which is the same
		// non-event area as the queue and worker instruments above it.
		{"QueueBacklog", QueueBacklog},
	}
}

// TestInit_ReturnsNilForEveryInstrument asserts that Init() walks its whole assignment
// chain — every instrument, the sixteen that predate the event pipeline and every
// event-streaming addition — without the meter refusing to build one.
func TestInit_ReturnsNilForEveryInstrument(t *testing.T) {
	// The meter is the source of every instrument, so a nil meter would make the rest of
	// this test meaningless. Reachable only because these tests live in package metrics
	// rather than metrics_test.
	require.NotNil(t, meter, "the package meter must be constructed before Init runs")

	require.NoError(t, Init(),
		"Init must build every instrument without the meter rejecting one; a non-nil error here is fatal at import and would kill any process importing this package")

	// A nil error is necessary but not sufficient. Init() returns on the FIRST error, so
	// an assignment block that was never written at all also yields nil.
	for _, instrument := range append(eventStreamingInstruments(), preExistingInstruments()...) {
		assert.NotNil(t, instrument.value,
			"%s returned from Init nil: its declaration has no matching assignment block", instrument.name)
	}
}

// TestEventStreamingInstruments_AreNonNilAfterPackageLoad asserts that importing this
// package is by itself enough to leave EVERY event-streaming instrument usable.
//
// This test deliberately does NOT call Init().
func TestEventStreamingInstruments_AreNonNilAfterPackageLoad(t *testing.T) {
	for _, instrument := range eventStreamingInstruments() {
		t.Run(instrument.name, func(t *testing.T) {
			assert.NotNil(t, instrument.value,
				"%s is declared but never assigned during package initialisation: every call site would panic on a nil instrument", instrument.name)
		})
	}
}

// TestPreExistingInstruments_RemainNonNilAfterEventStreamingAppend asserts that the
// event-streaming additions did not disturb any of the sixteen instruments that came
// before them.
func TestPreExistingInstruments_RemainNonNilAfterEventStreamingAppend(t *testing.T) {
	instruments := preExistingInstruments()
	require.Len(t, instruments, 17,
		"every non-event-streaming instrument must stay enumerated here: the sixteen that predate "+
			"the event pipeline plus QueueBacklog; grow this table when metrics.go adds another "+
			"outside the pipeline, and shrink it only when metrics.go genuinely removes one")

	for _, instrument := range instruments {
		t.Run(instrument.name, func(t *testing.T) {
			assert.NotNil(t, instrument.value,
				"%s was orphaned: it is declared in metrics.go but no longer assigned during package initialisation", instrument.name)
		})
	}
}

// TestEventStreamingInstruments_DeclaredKindsMatchTheirInstrumentType makes the
// instrument-kind table readable and executable, and checks it one level deeper than
// the compile-time guards above do.
//
// The guards check the DECLARED static type.
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
		{"EventRelayClaimsTotal", EventRelayClaimsTotal, (*metric.Int64Counter)(nil)},
		{"EventRelayClaimDuration", EventRelayClaimDuration, (*metric.Float64Histogram)(nil)},
		{
			// A COUNTER beside the claim counter above, because the ratio of the two is the
			// reading: rows per claim. A gauge of "rows in the last claim" would answer a
			// different and much less useful question.
			"EventRelayClaimedRows",
			EventRelayClaimedRows,
			(*metric.Int64Counter)(nil),
		},
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal, (*metric.Int64Counter)(nil)},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds, (*metric.Float64Gauge)(nil)},
		{"SubscriberConsumerLag", SubscriberConsumerLag, (*metric.Int64ObservableGauge)(nil)},
		{"ConsumerLagUnmeasuredPartitions", ConsumerLagUnmeasuredPartitions, (*metric.Int64ObservableGauge)(nil)},
		{"OutboxPendingBacklog", OutboxPendingBacklog, (*metric.Int64Gauge)(nil)},
		{"EventRepairBacklog", EventRepairBacklog, (*metric.Int64Gauge)(nil)},
		{"EventRepairsCompletedTotal", EventRepairsCompletedTotal, (*metric.Int64Counter)(nil)},
		{"EventRepairSaturated", EventRepairSaturated, (*metric.Int64Gauge)(nil)},
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
		{
			// A LEVEL, not a total: the budget is reconfigurable, and the headroom query
			// subtracts the registry size from it, so a counter here would accumulate the
			// configured value on every tick and read as headroom that only ever grows.
			"SubscriberMeasurementBudget",
			SubscriberMeasurementBudget,
			(*metric.Int64Gauge)(nil),
		},
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
// Recording is expected to be safe here: with no global MeterProvider installed the
// instruments are delegating no-ops whose Add and Record are nil-delegate guarded.
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
			// from a first delivery here, since unlike EventsPublishedTotal this counter records
			// every acknowledgement the broker gave.
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
			// 1.75 seconds: an event that waited for a poll tick and then published, which is
			// the shape this instrument exists to measure and the per-write histogram above
			// cannot see. No outcome attribute — only an acknowledged publish has an end-to-end
			// age at all.
			EventCaptureToDispatchDuration.Record(ctx, 1.75, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("attempt", "1"),
			))
		})
	})

	t.Run("EventsDeadLetteredTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			// The topic attribute deliberately carries the ORIGINAL category topic rather than
			// its .dlt sibling, so this counter stays directly comparable with
			// EventsPublishedTotal and their ratio is the dead-letter rate.
			EventsDeadLetteredTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions"),
				attribute.String("event_type", "transaction.applied"),
			))
		})
	})

	t.Run("DLTOldestMessageAgeSeconds", func(t *testing.T) {
		require.NotPanics(t, func() {
			// This gauge measures a message sitting ON a dead-letter topic, so here the topic
			// attribute is the .dlt name. 930 seconds is just past the 900-second alerting
			// threshold, which is the value that matters operationally.
			DLTOldestMessageAgeSeconds.Record(ctx, 930, metric.WithAttributes(
				attribute.String("topic", "blnk.transactions.dlt"),
			))
		})
	})

	t.Run("the asynchronous lag gauges", func(t *testing.T) {
		// These two are OBSERVABLE, so there is no Record to drive. The equivalent operation
		// is publishing an inventory and letting the registered callback observe it, which is
		// what this exercises: 10001 messages is just past the 10000-message alerting
		// threshold, and the second sample is the incomplete case whose lag must be withheld.
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
			// contribute an unmeasured-partition reading, and only the complete one contributes
			// a lag — because a partial sum is a lower bound and exporting it would resolve the
			// >10000 alert with a figure known to be too small.
			assert.Equal(t, []int64{10001}, observer.int64For(SubscriberConsumerLag),
				"only the completely measured topic may export a lag")
			assert.Equal(t, []int64{0, 2}, observer.int64For(ConsumerLagUnmeasuredPartitions),
				"every measured topic reports its unmeasured-partition count, zero included")
		})

		t.Cleanup(func() { PublishConsumerLagInventory(nil) })
	})

	t.Run("OutboxPendingBacklog", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Unattributed by design: the relay's backlog is a single global number, recorded
			// exactly the way chain_worker.go records ChainBacklog.
			OutboxPendingBacklog.Record(ctx, 42)
		})
	})

	t.Run("EventsPurgedTotal", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Unattributed, matching event_retention.go: the sweep reports how many rows it
			// deleted, and it deletes across every topic in one pass, so there is no per-topic
			// number to attribute.
			EventsPurgedTotal.Add(ctx, 5)
		})
	})

	t.Run("the revocation backlog gauges", func(t *testing.T) {
		require.NotPanics(t, func() {
			// Both unattributed, matching event_metrics.go: the backlog is a single global pair,
			// and attributing it per subscriber would publish a series per subscriber ever
			// revoked — unbounded cardinality for a number whose only consumer is one alert
			// threshold.
			SubscriberRevocationsPending.Record(ctx, 4)
			OldestSubscriberRevocationAgeSeconds.Record(ctx, 7200)
		})
	})
}

// TestPublishConsumerLagInventory_ReplacesRatherThanMerges pins the property the whole
// cardinality fix rests on.
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
// The vocabulary is THREE values, and the accompanying `terminal` dimension carries the
// fourth fact: whether any further attempt is possible.
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

// TestEventPublishDuration_AcceptsEveryAttemptInTheRetryBudget records a duration for
// every attempt number the relay's retry budget can produce.
//
// The attempt attribute carries the attempt NUMBER as a string, which is what lets a
// p99 latency query exclude retried publishes by filtering attempt="1".
func TestEventPublishDuration_AcceptsEveryAttemptInTheRetryBudget(t *testing.T) {
	ctx := context.Background()

	for attempt := 1; attempt <= maxRelayRetryAttempts; attempt++ {
		label := strconv.Itoa(attempt)
		t.Run("attempt="+label, func(t *testing.T) {
			require.NotPanics(t, func() {
				// The WHOLE declared tuple, outcome included, even though this test asserts only
				// that the attempt label is legal. Recording a subset would put a series with two
				// of the instrument's three keys into the process, and once a MeterProvider is
				// installed — which the tests below the SDK line do, once, for the lifetime of the
				// binary — that series is a second, off-contract shape of an instrument whose
				// attribute keys are a published contract.
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
// This is not a redundant edge case.
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
// ---------------------------------------------------------------------------

// sharedReader and installOnce back installSDKReader.
//
// The provider is deliberately never shut down.
var (
	sharedReader *sdkmetric.ManualReader
	installOnce  sync.Once
)

// perTestTemporality makes the shared reader report DELTA sums and histograms and
// cumulative gauges.
//
// A shared reader is forced on this file by the once-only meter delegation above, and a
// CUMULATIVE one accumulates for the lifetime of the process.
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
// and rebuilding every instrument through it on first use, and DRAINING whatever was
// recorded before this test began.
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
// embedded.Observer is embedded, not implemented: that is how the OpenTelemetry API
// intends third-party implementations of its interfaces to be written.
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
// event-streaming instruments, using the attribute keys their declarations document.
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
	// The three claim instruments carry ONE attribute between them — the claim's outcome —
	// and no topic or event type, because a claim is issued against the outbox as a whole
	// and belongs to no single event.
	EventRelayClaimsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", "rows"),
	))
	EventRelayClaimDuration.Record(ctx, 0.0012, metric.WithAttributes(
		attribute.String("outcome", "rows"),
	))
	EventRelayClaimedRows.Add(ctx, 100, metric.WithAttributes(
		attribute.String("outcome", "rows"),
	))
	EventsDeadLetteredTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions"),
		attribute.String("event_type", "transaction.applied"),
	))
	DLTOldestMessageAgeSeconds.Record(ctx, 901, metric.WithAttributes(
		attribute.String("topic", "blnk.transactions.dlt"),
	))
	// The two lag gauges are ASYNCHRONOUS: publishing the inventory is the measurement,
	// and the registered callback observes it when the reader collects. Both instruments
	// are fed from this one sample, so a single complete entry produces a lag point and a
	// zero unmeasured-partition point.
	PublishConsumerLagInventory([]ConsumerLagSample{{
		Subscriber:  "sub_0f6e2c8a",
		Group:       "blnk-grp-0f6e2c8a1b944106b4d6793e838afcbf",
		Topic:       "blnk.transactions",
		Lag:         10001,
		LagComplete: true,
	}})

	OutboxPendingBacklog.Record(ctx, 7)

	// The three REPAIR instruments, recorded under BOTH legs because the attribute domain
	// is closed at two and the assertions below read the exported key set. Recording one
	// leg would let the other's spelling drift.
	for _, leg := range []string{"dead_letter", "legacy_webhook"} {
		attributes := metric.WithAttributes(attribute.String("leg", leg))
		EventRepairBacklog.Record(ctx, 12, attributes)
		EventRepairsCompletedTotal.Add(ctx, 4, attributes)
		EventRepairSaturated.Record(ctx, 1, attributes)
	}

	EventsPurgedTotal.Add(ctx, 3)

	SubscriberRevocationsPending.Record(ctx, 2)
	OldestSubscriberRevocationAgeSeconds.Record(ctx, 3601)

	// The four settlement gauges are recorded together because they are published
	// together, as one reading of one aggregate. Recording only some of them here would
	// let a rename of the others past the descriptor assertions below.
	SubscriberSettlementOutstanding.Record(ctx, 3)
	SubscriberGrantReconcilePending.Record(ctx, 2)
	SubscriberCredentialCleanupPending.Record(ctx, 2)
	OldestSubscriberSettlementAgeSeconds.Record(ctx, 3601)
	SubscriberObligationsSettledTotal.Add(ctx, 5)

	// The credential-orphan and revocation-failure pairs. Each is a marker plus the age
	// the age-based alert is stated over, so both halves are recorded together: a rename
	// of the age alone would otherwise slip past the descriptor assertions.
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
	// The configured budget, published beside the size so headroom is a difference of two
	// series rather than a literal that is wrong wherever the budget was raised.
	SubscriberMeasurementBudget.Record(ctx, 200)
	ConsumerLagInventoryComplete.Record(ctx, 0)
	SubscriberLagPassAgeSeconds.Record(ctx, 12)
	SubscriberLagCoveredSubscribers.Record(ctx, 2)

	// The collector's own health. The counter is synchronous; the two ages are
	// ASYNCHRONOUS and are published by observeEventMetricsCollectionAges, which returns
	// nothing at all until a collection has been recorded — so this call is what makes
	// those two series exist for the descriptor and series-name assertions to find.
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
// The names are the load-bearing part.
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
			name: "blnk.events.published.total",
			unit: "{event}",
			description: "Total original ledger events whose Kafka leg is durably recorded, " +
				"counted once each by topic and event type",
		},
		{
			// ALSO PER-EVENT, and deliberately not a second opinion on the row above. This one
			// is incremented at the terminal dispatched state, which for a row still owing a
			// legacy webhook happens on a later pass — so it answers "how many events are
			// completely settled" rather than "how many are on their topic".
			name: "blnk.events.dispatched.total",
			unit: "{event}",
			description: "Total ledger events whose delivery is durably recorded, counted once " +
				"each by topic and event type",
		},
		{
			// THE WRITE-SIDE SIGNAL, and the unit says so: {write}, not {event}. It counts what
			// the broker accepted, for EVERY purpose — original, replay, dead-letter — so a
			// republished event is counted again, which is the whole point.
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
			// THE CLAIM is the start of this interval, and the description says so. It is the
			// broker write in relative isolation, which is the useful thing for it to be:
			// subtracted from the end-to-end figure below, the difference is the queue wait, and
			// that is what distinguishes a slow cluster from an under-provisioned relay.
			name:        "blnk.events.publish.duration",
			unit:        "s",
			description: "Duration of a single event publish attempt, from outbox claim to broker acknowledgement",
		},
		{
			// THE PUBLISH-LATENCY OBJECTIVE IS READ FROM THIS ONE. It is stated over
			// "outbox-to-Kafka publish latency", and only this instrument starts its clock at
			// the durable capture: timing from the claim excludes the poll delay and the
			// backlog, so a relay an hour behind would report the same sub-second p99 as an idle
			// one and would certify a target the system was missing.
			name:        "blnk.events.capture_to_dispatch.duration",
			unit:        "s",
			description: "End-to-end age of a published event, from its capture in the transactional outbox to broker acknowledgement",
		},
		{
			// PER CLAIM, not per row, and recorded on EVERY claim including the empty ones —
			// which is what makes a stopped relay distinguishable from an idle one.
			name:        "blnk.events.relay.claims.total",
			unit:        "{claim}",
			description: "Total number of event outbox claims the relay issued, by outcome: rows, empty, error or timeout",
		},
		{
			// THE LEADING INDICATOR of a claim whose cost has grown with the backlog: it rises
			// long before any throughput counter falls, because a slow claim still returns rows.
			name:        "blnk.events.relay.claim.duration",
			unit:        "s",
			description: "Duration of one event outbox claim, by outcome",
		},
		{
			// UNIT {row}, against {claim} above, and the pairing is the point: their quotient is
			// rows per claim, which is the reading that separates a relay working at its batch
			// size from one paying a whole claim per event.
			name:        "blnk.events.relay.claimed_rows.total",
			unit:        "{row}",
			description: "Total number of outbox rows the relay's claims returned, so claim size is comparable with claim count",
		},
		{
			name:        "blnk.events.dead_lettered.total",
			unit:        "{event}",
			description: "Total number of events dead-lettered after retry exhaustion by topic and event type",
		},
		{
			name: "blnk.dlt.oldest_message_age_seconds",
			unit: "s",
			// THE ANCHOR IS PART OF THE DESCRIPTION, and it has to be: the two populations this
			// gauge covers are measured from different timestamps, and an operator reading the
			// exposition needs to know which — a `failed` entry's age is the age of its whole
			// retry-and-preserve debt, while a preserved entry's is how long it has waited for
			// triage. The repair pass rewrites last_attempted_at on every tick for exactly the
			// rows whose dead-letter write is still owed, so anchoring those there reported them
			// as seconds old however long they had been stranded.
			description: "Seconds the oldest unresolved dead-letter entry has been waiting, over rows in the failed or " +
				"dead_lettered state, measured from the last publish attempt once the entry is preserved on " +
				"its dead-letter topic and from the first attempt while that write is still owed",
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
			name:        "blnk.events.repair.backlog",
			unit:        "{event}",
			description: "Event outbox rows a repair leg still owes, by leg",
		},
		{
			name:        "blnk.events.repair.completed.total",
			unit:        "{event}",
			description: "Event outbox rows repaired to their destination, by leg",
		},
		{
			name:        "blnk.events.repair.saturated",
			unit:        "{state}",
			description: "1 when a repair pass spent its whole per-tick budget with work outstanding, by leg",
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
			// The other half of that headroom. Exported so the headroom query can name it as a
			// SERIES: a literal default is wrong on every deployment that raised the budget.
			name: "blnk.subscribers.measurement_budget",
			unit: "{subscriber}",
			description: "Subscribers one consumer-lag sweep may measure, as configured, so headroom is a " +
				"difference of two series rather than a literal that is wrong wherever the " +
				"budget was raised",
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
		// NO blnk.subscriber_stream.* PAIR. Blnk serves no subscriber records, so it counts
		// none — see the note on eventStreamingInstruments for where per-record evidence of a
		// key-scope filter has to come from instead.
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

// TestEventPublishDuration_UsesExplicitSubTwoSecondBuckets is the assertion the latency
// acceptance criterion rests on.
//
// OTel's DEFAULT histogram boundaries are 0, 5, 10, 25, 50, 75, 100, 250, 500, 750,
// 1000, 2500, 5000, 7500, 10000 — chosen for milliseconds.
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
// bucket assertion for the instrument the acceptance criterion is ACTUALLY read from.
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

	// THE BREACH MUST REPORT A NUMBER, NOT A FLOOR, and this is the assertion that pins it.
	//
	// A quantile is interpolable only INSIDE a finite bucket; a rank that falls past the
	// largest boundary is reported AS that boundary. With 300 as the top edge, a run whose
	// true p50, p95 and p99 were all minutes past it read as exactly 300.0000 across the
	// board — the same value whether the real answer was five minutes or five hours, which
	// is a floor pretending to be a measurement and is unusable for deciding whether a
	// recovery is progressing.
	//
	// An hour is the threshold asserted rather than the exact tail, so the boundaries above
	// can be re-spaced without failing here, while the property — the scale reaches far
	// enough past the objective for a breach to be a distinguishable number — cannot be
	// removed. Coarse resolution up there is correct: a breach of a two-second objective
	// needs its order of magnitude, not its milliseconds.
	require.NotEmpty(t, point.Bounds)
	assert.GreaterOrEqual(t, point.Bounds[len(point.Bounds)-1], float64(3600),
		"the top finite boundary is %v, so any age past it is reported as that boundary. A breach of "+
			"the two-second objective must come back as a NUMBER an operator can watch move, and a "+
			"scale that stops minutes past the target cannot tell a slow drain from a stalled one",
		point.Bounds[len(point.Bounds)-1])

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
func TestEventStreamingInstruments_ExportedAttributeKeys(t *testing.T) {
	reader := installSDKReader(t)
	recordEveryEventInstrument(context.Background())
	collected := collectScopeMetrics(t, reader)

	cases := []struct {
		metric string
		keys   []string
	}{
		{metric: "blnk.events.published.total", keys: []string{"topic", "event_type"}},
		// The terminal counter, attributed exactly as the published counter is. The pair is
		// what makes "how many events are still mid-flight" a subtraction rather than a
		// guess, and a divergence in labels would make the two unsubtractable.
		{metric: "blnk.events.dispatched.total", keys: []string{"topic", "event_type"}},
		// Both dimensions. `terminal` is not decoration: with the outcome vocabulary frozen
		// at three values it is the ONLY thing separating a failure that will be retried from
		// one that never will, so a counter that lost it could no longer answer the question
		// the dead-letter triage runbook opens with.
		{metric: "blnk.events.publish.attempts.total", keys: []string{"outcome", "terminal"}},
		{metric: "blnk.events.publish.duration", keys: []string{"topic", "attempt", "outcome"}},
		// No outcome: only an acknowledged publish has an end-to-end age to report, so the
		// attribute would carry one value on every series and add nothing.
		{metric: "blnk.events.capture_to_dispatch.duration", keys: []string{"topic", "attempt"}},
		// The three CLAIM instruments carry `outcome` and nothing else. No topic and no event
		// type: a claim is issued against the outbox as a whole, so it belongs to no topic and
		// to no event — attributing it to one would invent a dimension the measurement does
		// not have. `outcome` is closed at four values (rows, empty, error, timeout), and it is
		// necessary because a timed-out claim and a rejected one need different remedies.
		{metric: "blnk.events.relay.claims.total", keys: []string{"outcome"}},
		{metric: "blnk.events.relay.claim.duration", keys: []string{"outcome"}},
		{metric: "blnk.events.relay.claimed_rows.total", keys: []string{"outcome"}},
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
		// The three REPAIR instruments all carry `leg` and nothing else. The label is
		// necessary — the two legs fill under different conditions and have different
		// remedies, so a summed backlog would tell an operator neither which one is owed nor
		// which knob to reach for — and it is CLOSED at two values, so it adds no unbounded
		// cardinality.
		{metric: "blnk.events.repair.backlog", keys: []string{"leg"}},
		{metric: "blnk.events.repair.completed.total", keys: []string{"leg"}},
		{metric: "blnk.events.repair.saturated", keys: []string{"leg"}},
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
		// The configured sweep budget, one number per process, so it carries no label either.
		{metric: "blnk.subscribers.measurement_budget", keys: nil},
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
		// A COUNTER, and unattributed: SubscriberSettlementNotProgressing takes a rate over
		// it to separate a stuck pass from a busy one, and that question has no per-subject
		// breakdown.
		{metric: "blnk.subscribers.obligations_settled.total", keys: nil},
		// NO blnk.subscriber_stream.* PAIR, for the reason recorded on eventStreamingInstruments:
		// there is no Blnk-hosted subscriber read path to count records on.
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

// attributeKeysOf returns the attribute keys a collected metric's data points carry,
// and requires that EVERY data point agrees on them.
//
// The agreement is asserted rather than assumed.
//
// Requiring one key set across every point is also the stronger statement.
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
// This is a CARDINALITY BUDGET expressed as a test.
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

// TestSubscriberUnmeasuredReasons_IsAClosedNonDegenerateVocabulary pins the domain of
// the one attribute an alert's REMEDIATION branches on.
//
// SubscriberLagCoverageIncomplete does not tell an operator to go and investigate; it
// names each reason and gives each its own action, because the six mean six different
// faults in six different places — the budget, the registry row, the broker, the
// registry query, the topic provisioning, and a deployment that configured no broker at
// all.
//
// A reason ADDED here and not added to the remediation leaves an operator holding a
// series with a reason the runbook does not explain.
//
// The duplicate and empty checks are not padding.
func TestSubscriberUnmeasuredReasons_IsAClosedNonDegenerateVocabulary(t *testing.T) {
	reasons := SubscriberUnmeasuredReasons()

	assert.Equal(t, []string{
		SubscribersUnmeasuredReasonBudget,
		SubscribersUnmeasuredReasonUnprovisioned,
		SubscribersUnmeasuredReasonMeasureFailed,
		SubscribersUnmeasuredReasonRegistryFailed,
		SubscribersUnmeasuredReasonTopicMissing,
		// THE REASON A DEPLOYMENT WITH NO BROKER GETS ITS OWN VALUE. Those rows were counted
		// under `budget`, whose documented remedy is to raise the measurement budget — which
		// cannot produce an offset from a broker that was never configured, so the one series an
		// operator was told to act on named the one action that could not help.
		SubscribersUnmeasuredReasonBrokerUnconfigured,
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

	// A FRESH SLICE, so a caller cannot mutate the vocabulary for every other caller in
	// the process. The collector iterates this on every tick; a handler that sorted or
	// truncated the returned slice in place would change what is published from then on.
	first := SubscriberUnmeasuredReasons()
	first[0] = "mutated-by-a-caller"
	assert.Equal(t, reasons, SubscriberUnmeasuredReasons(),
		"the vocabulary must be returned as a copy; a caller mutating it would change what every "+
			"later tick publishes")
}

// TestEventStreamingInstruments_ExportedPrometheusSeriesNames asserts the names the
// ALERT RULES actually match on, as the Prometheus exporter produces them.
//
// Every other test in this file checks the OTel instrument name.
func TestEventStreamingInstruments_ExportedPrometheusSeriesNames(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := promexporter.New(promexporter.WithRegisterer(registry))
	require.NoError(t, err, "building the Prometheus exporter")

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() {
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	// A meter from THIS provider, and every instrument rebuilt on it: the package-level
	// instruments belong to whatever provider was installed at import time, so recording
	// through them would not reach this exporter.
	restore := swapMeter(t, provider.Meter("blnk"))
	defer restore()

	require.NoError(t, Init(), "re-initialising the instruments against the Prometheus exporter")
	recordEveryEventInstrument(context.Background())

	exposition := scrapeRegistry(t, registry)

	// Exactly the names alerts/blnk-kafka-alerts.yml and docs/metrics.md use. Written out
	// as literals rather than derived from the instrument names, because deriving them
	// would reproduce the very assumption that broke.
	for _, series := range []string{
		"blnk_events_published_total",
		"blnk_events_broker_acknowledgements_total",
		"blnk_events_dispatched_total",
		"blnk_events_publish_attempts_total",
		// HISTOGRAMS ARE ASSERTED ON THEIR _bucket SERIES, because that is the series the
		// documented p99 queries feed to histogram_quantile — a histogram exports _bucket,
		// _sum and _count and no bare sample at all, so asserting the bare name would fail
		// for a correctly exported instrument.
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
		// The worst state there is, and the one absence would hide: failing since start-up.
		// The gauge must still carry a value, or EventMetricsCollectionFailing has no series
		// to evaluate for precisely the collector that has never worked.
		resetEventMetricsCollectionHealth(t)
		RecordEventMetricsCollection(time.Now().Add(-10*time.Minute), false)

		exported := scrapeRegistry(t, registry)
		assert.True(t, expositionHasSeries(exported, "blnk_event_metrics_last_success_age_seconds"),
			"a collector that has never completed a clean collection must still report a success age, "+
				"measured from when it started; reporting nothing makes the most degraded state the one "+
				"state a threshold rule cannot detect")
	})
}

// swapMeter points the package meter at a different one for the duration of a test.
//
// Returns:
//   - func(): restores the original meter. Also registered with t.Cleanup, so a test
//     that forgets to call it still cannot leak the swap into another test.
func swapMeter(t *testing.T, replacement metric.Meter) func() {
	t.Helper()

	original := meter
	restore := func() { meter = original }
	t.Cleanup(restore)
	meter = replacement

	return restore
}

// resetEventMetricsCollectionHealth clears the recorded collection timestamps.
func resetEventMetricsCollectionHealth(t *testing.T) {
	t.Helper()

	eventMetricsCollectionHealth.mu.Lock()
	defer eventMetricsCollectionHealth.mu.Unlock()

	eventMetricsCollectionHealth.firstAttempt = time.Time{}
	eventMetricsCollectionHealth.lastAttempt = time.Time{}
	eventMetricsCollectionHealth.lastSuccess = time.Time{}
}

// scrapeRegistry gathers a Prometheus registry and returns the set of SERIES NAMES the
// exposition format would carry for it.
//
// Returns:
//   - map[string]bool: every series name the registry would export, ready for lookup.
func scrapeRegistry(t *testing.T, registry *prometheus.Registry) map[string]bool {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err, "gathering the Prometheus registry")
	require.NotEmpty(t, families, "the registry produced no metric families")

	series := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		require.NotEmpty(t, name, "the registry produced a metric family with no name")

		for _, sample := range family.GetMetric() {
			switch {
			case sample.GetHistogram() != nil:
				series[name+"_bucket"] = true
				series[name+"_sum"] = true
				series[name+"_count"] = true
			case sample.GetSummary() != nil:
				series[name] = true
				series[name+"_sum"] = true
				series[name+"_count"] = true
			default:
				// Counters, gauges and untyped samples export under the family name itself.
				series[name] = true
			}
		}
	}

	return series
}

// expositionHasSeries reports whether an exported series set carries the given name.
//
// The match is on the WHOLE name, so a name which is a PREFIX of another cannot satisfy
// the assertion — which is the whole failure mode being guarded, since the suffix the
// exporter appends is what moves a series.
func expositionHasSeries(exposition map[string]bool, series string) bool {
	return exposition[series]
}

// declaredInstrumentNames parses metrics.go and returns the name of every EXPORTED
// package-level variable whose declared type comes from the otel metric package.
//
// It reads the source rather than using reflection because the property under test is a
// property of the DECLARATIONS, not of the values.
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

// TestInstrumentTables_CoverEveryDeclaredInstrumentExactlyOnce is the guard that keeps
// the two enumeration tables — and every count stated in this file's prose — honest.
//
// This test closes that loop from the other direction. It derives the truth from the
// declarations in metrics.go and asserts a bidirectional match:
//
//   - every declared instrument appears in exactly one table, so a newly declared
//     instrument cannot be added without being brought under the non-nil guard, and
//   - every table entry corresponds to a real declaration, so a removed instrument
//     cannot leave a stale name behind that would fail to compile only after someone
//     else's change.
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
