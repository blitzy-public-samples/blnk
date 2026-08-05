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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
	_ metric.Int64Counter     = EventsDeadLetteredTotal
	_ metric.Float64Gauge     = DLTOldestMessageAgeSeconds
	_ metric.Int64Gauge       = SubscriberConsumerLag
	_ metric.Int64Gauge       = OutboxPendingBacklog
)

// publishOutcomes is the complete vocabulary of the "outcome" attribute carried by
// EventPublishAttemptsTotal, matching the documented attribute list on its
// declaration in metrics.go.
//
// Note the UNDERSCORE in dead_lettered. Surrounding prose spells the concept
// "dead-lettered" with a hyphen; that is not the attribute value, and a metric
// consumer filtering on the hyphenated spelling would match nothing.
//
// Spelled as literals deliberately: this test file stays as dependency-free as the
// package it covers, so it does not import the model package merely to obtain three
// strings.
var publishOutcomes = []string{"dispatched", "retrying", "dead_lettered"}

// maxRelayRetryAttempts is the relay's default attempt budget
// (RELAY_MAX_RETRY_ATTEMPTS), which bounds the values the "attempt" attribute on
// EventPublishDuration can take: 1 through 5 inclusive, rendered as strings.
// Mirrored as a literal for the same reason as publishOutcomes — this package reads
// no configuration, and neither does its test.
const maxRelayRetryAttempts = 5

// namedInstrument pairs an instrument with the name of the variable holding it, so
// that a table-driven failure names the exact declaration at fault instead of
// reporting an anonymous slice index.
type namedInstrument struct {
	name  string
	value any
}

// eventStreamingInstruments returns the seven instruments added for the Kafka
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
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds},
		{"SubscriberConsumerLag", SubscriberConsumerLag},
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
		{"EventsDeadLetteredTotal", EventsDeadLetteredTotal, (*metric.Int64Counter)(nil)},
		{"DLTOldestMessageAgeSeconds", DLTOldestMessageAgeSeconds, (*metric.Float64Gauge)(nil)},
		{"SubscriberConsumerLag", SubscriberConsumerLag, (*metric.Int64Gauge)(nil)},
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

	t.Run("SubscriberConsumerLag", func(t *testing.T) {
		require.NotPanics(t, func() {
			// 10001 messages is just past the 10000-message alerting threshold.
			SubscriberConsumerLag.Record(ctx, 10001, metric.WithAttributes(
				attribute.String("subscriber", "sub_01HZX9K2Q4"),
				attribute.String("group", "blnk-sub-01HZX9K2Q4"),
				attribute.String("topic", "blnk.transactions"),
			))
		})
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

// TestEventPublishAttemptsTotal_AcceptsEveryPublishOutcome records one attempt per
// value of the "outcome" attribute vocabulary.
//
// The point is not that the no-op instrument tolerates arbitrary strings — it
// tolerates anything — but that the vocabulary is pinned in one place, in the same
// spelling the relay emits and the alerting rules filter on. dead_lettered in
// particular is easy to get wrong: the hyphenated "dead-lettered" reads more
// naturally in prose and matches nothing at query time.
func TestEventPublishAttemptsTotal_AcceptsEveryPublishOutcome(t *testing.T) {
	ctx := context.Background()

	require.Len(t, publishOutcomes, 3,
		"the outcome vocabulary is dispatched, retrying and dead_lettered; a fourth publish status needs a deliberate decision here and in the alerting rules")
	require.Contains(t, publishOutcomes, "dead_lettered",
		"the dead-letter outcome is spelled with an underscore, not a hyphen")

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
