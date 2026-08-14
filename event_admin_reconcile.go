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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// OutboxReconciliation is the verdict of comparing the outbox against the broker.
type OutboxReconciliation struct {
	// TerminalEvents is how many outbox rows claim to have been published to the broker.
	TerminalEvents int64

	// CorroboratedEvents is how many of those rows name a record INSIDE the measured
	// [first, end) window of its partition — a record the broker can serve right now.
	CorroboratedEvents int64

	// UnconfirmedEvents claim a publication without naming any record at all.
	UnconfirmedEvents int64

	// UnmeasuredEvents name a topic or partition the measurement did not cover: a missing
	// topic, an unavailable partition, or a partition count that has since shrunk. Their
	// records may well be there; nothing in this measurement says so.
	UnmeasuredEvents int64

	// AgedOutEvents name an offset BELOW the retained window. The record was written and
	// Kafka retention has since deleted it.
	AgedOutEvents int64

	// BeyondEndEvents name an offset AT OR ABOVE the log end of their partition.
	BeyondEndEvents int64

	// DuplicatedRecords is how many corroborated rows share a coordinate with another row.
	DuplicatedRecords int64

	// MessagesWritten is how many records the broker has accepted across the measured
	// topics, from summed end offsets, and RecordsRetained how many of those it still
	// holds.
	MessagesWritten int64
	RecordsRetained int64

	// BlnkRecordShare is how many of the retained records this reconciliation attributed
	// to Blnk rows: CorroboratedEvents. Reported as its own field so the response states
	// plainly how much of a shared topic's traffic the verdict actually accounts for.
	BlnkRecordShare int64

	// LossDetected is true when specific records this outbox recorded are provably not on
	// the log: a row naming an offset at or above its partition's end.
	LossDetected bool

	// Conclusive reports whether the mapping accounted for EVERY retained claim.
	Conclusive bool

	// Caveats names, in plain words, every reason the result is inconclusive. Empty when
	// Conclusive is true.
	Caveats []string

	// CoveredFrom and CoveredTo bound the publication instants of the corroborated
	// population: the window a green verdict actually speaks about.
	CoveredFrom time.Time
	CoveredTo   time.Time

	// OldestTerminalAt is the earliest publication instant among all retained terminal
	// rows. Anything published before it has been pruned from the outbox and is outside
	// the reach of any verdict — which is why it is reported rather than left implicit.
	OldestTerminalAt time.Time

	// MeasuredAt is when the broker side was measured.
	MeasuredAt time.Time

	// WindowStart is the instant both sides were measured from, zero when the comparison was
	// whole-history.
	WindowStart time.Time

	// Overhead is MessagesWritten minus TerminalEvents: the redelivery, replay and
	// dead-letter copies. Its EXPECTED value is greater than or equal to zero, and a
	// healthy system's overhead is small but not zero.
	Overhead int64

	// Windowed reports whether both sides were bounded to a common window. Only a windowed
	// comparison can be conclusive — see the caveat ReconcileAgainstOutbox adds when it is
	// not — so this is the field to read before believing MessagesWritten or Overhead.
	Windowed bool
}

// Summary renders the verdict as one sentence for a log line or a runbook.
//
// Returns:
//   - string: the verdict, always naming the numbers it rests on so the sentence is
//     checkable against the fields.
func (r OutboxReconciliation) Summary() string {
	switch {
	case r.BeyondEndEvents > 0:
		// Stated ahead of the arithmetic because it is stronger evidence: an offset past the
		// end of a log cannot be explained by any amount of surplus.
		return fmt.Sprintf(
			"LOSS DETECTED: %d row(s) name a broker record at or beyond the end of the partition "+
				"they claim, so those records do not exist. That is only possible if the partition was "+
				"truncated or the topic was deleted and recreated beneath the ledger, or the events "+
				"were lost after being marked published",
			r.BeyondEndEvents,
		)
	case r.LossDetected:
		return fmt.Sprintf(
			"LOSS DETECTED: the broker holds %d record(s) inside the measured window against %d "+
				"outbox row(s) claiming a publication inside it, a shortfall of %d. Records are a "+
				"lower bound on events — every redelivery, replay and dead-letter copy adds one — so "+
				"a shortfall means rows claim a publication that never happened",
			r.MessagesWritten, r.TerminalEvents, -r.Overhead,
		)
	case !r.Conclusive:
		return fmt.Sprintf(
			"INCONCLUSIVE: %d of %d outbox rows were matched to a record inside the measured broker "+
				"windows, and the rest could not be (%s)",
			r.CorroboratedEvents, r.TerminalEvents, strings.Join(r.Caveats, "; "),
		)
	case r.TerminalEvents == 0:
		return "NO LOSS DETECTED: the outbox holds no rows claiming a publication, so there is " +
			"nothing to reconcile"
	case !r.Windowed:
		// THE SAME GREEN VERDICT, WITHOUT CALLING A CUMULATIVE TOTAL A SURPLUS.
		return fmt.Sprintf(
			"NO LOSS DETECTED: every one of the %d outbox rows published between %s and %s names the "+
				"distinct broker record it produced, inside the measured offset window of its own "+
				"partition, so no event this outbox still retains is missing from the broker. The "+
				"broker's %d record(s) are a CUMULATIVE total for the topics rather than a count "+
				"inside a shared window, so the difference against the row count is not a surplus "+
				"and no shortfall can be computed from it",
			r.CorroboratedEvents,
			r.CoveredFrom.Format(time.RFC3339),
			r.CoveredTo.Format(time.RFC3339),
			r.MessagesWritten,
		)
	default:
		// The SURPLUS is named, because a green verdict that only said "no loss detected"
		// left an operator unable to tell a healthy overhead from a shortfall that happened
		// to be hidden by one: what makes the surplus safe is that every row names the
		// distinct record it produced, so the extra records belong to redeliveries, replays
		// and dead-letter copies rather than to events nothing accounts for.
		return fmt.Sprintf(
			"NO LOSS DETECTED: every one of the %d outbox rows published between %s and %s names the "+
				"distinct broker record it produced, inside the measured offset window of its own "+
				"partition, so the %d record(s) of surplus are redelivery, replay and dead-letter "+
				"copies rather than unaccounted events, and no event this outbox still retains is "+
				"missing from the broker",
			r.CorroboratedEvents,
			r.CoveredFrom.Format(time.RFC3339),
			r.CoveredTo.Format(time.RFC3339),
			r.Overhead,
		)
	}
}

// ReconcileAgainstOutbox interprets an offset report against the outbox's interval
// audit.
//
// Parameters:
//   - report TopicOffsetReport: the broker-side measurement from TopicEndOffsets, whose
//     PartitionIntervals() must be the windows the audit was taken against.
//   - audit model.EventRecordIntervalAudit: the outbox-side classification from
//     AuditEventRecordsInIntervals.
//
// Returns:
//   - OutboxReconciliation: the verdict, always populated.
func ReconcileAgainstOutbox(
	report TopicOffsetReport,
	audit model.EventRecordIntervalAudit,
) OutboxReconciliation {
	// THE COMMON POPULATION. When both sides name a window the comparison is drawn INSIDE
	// it, because the cumulative sum counts a history the outbox no longer holds: a topic
	// that has accepted a million records over its life and a thousand inside the window
	// must reconcile against the rows the outbox holds for that window, or every
	// reconciliation reports a surplus that grows without bound. With no window on either
	// side the cumulative reading is the only one available, and the caveats say so.
	windowed := !report.WindowStart.IsZero() && !audit.WindowStart.IsZero()

	writtenRecords := report.EndOffsetSum
	if windowed {
		writtenRecords = report.WindowRecordCount
	}

	verdict := OutboxReconciliation{
		TerminalEvents:     audit.PublishedRows,
		CorroboratedEvents: audit.CorroboratedRows,
		UnconfirmedEvents:  audit.UnconfirmedRows,
		UnmeasuredEvents:   audit.UnmeasuredRows,
		AgedOutEvents:      audit.AgedOutRows,
		BeyondEndEvents:    audit.BeyondEndRows,
		DuplicatedRecords:  audit.DuplicatedRecords(),
		MessagesWritten:    writtenRecords,
		Windowed:           windowed,
		Overhead:           writtenRecords - audit.PublishedRows,
		RecordsRetained:    report.RetainedCount,
		BlnkRecordShare:    audit.CorroboratedRows,
		CoveredFrom:        audit.CorroboratedFrom,
		CoveredTo:          audit.CorroboratedTo,
		OldestTerminalAt:   audit.OldestTerminalAt,
		WindowStart:        report.WindowStart,
		MeasuredAt:         report.MeasuredAt,
	}

	// TWO INDEPENDENT SIGNALS, and either is enough.
	verdict.LossDetected = verdict.BeyondEndEvents > 0 || (windowed && verdict.Overhead < 0)

	caveats := make([]string, 0, 7)

	// AN UNWINDOWED COMPARISON IS NOT A CAVEAT, and the reason is worth stating because
	// the opposite is the intuitive answer.

	if verdict.BeyondEndEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name an offset at or beyond the end of their partition's log, which means the "+
				"partition was truncated or the topic was deleted and recreated after those records "+
				"were written",
			verdict.BeyondEndEvents,
		))
	}

	// THE CAVEAT THE COORDINATE MAPPING EXISTS FOR. A row claiming a publication it cannot
	// name a record for is not evidence of loss — the record may well be there — but it is
	// precisely what a surplus of redeliveries could be concealing, so no green verdict
	// may be reported while any remain.
	if verdict.UnconfirmedEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) claim a publication without naming the broker record they produced, so they "+
				"cannot be matched against any measured window",
			verdict.UnconfirmedEvents,
		))
	}

	if verdict.UnmeasuredEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a topic or partition this measurement did not cover, so their records "+
				"were neither confirmed nor ruled out",
			verdict.UnmeasuredEvents,
		))
	}

	// Retention is a statement about SPECIFIC EVENTS rather than a topic-wide subtraction.
	if verdict.AgedOutEvents > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a record Kafka retention has already deleted, so the write is evidenced "+
				"by the stored offset but the record can no longer be read",
			verdict.AgedOutEvents,
		))
	}

	// Impossible while the partial unique index on the coordinate exists, which is why its
	// appearance is reported as a schema problem rather than absorbed.
	if verdict.DuplicatedRecords > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d row(s) name a broker record another row also names, which the unique index on "+
				"(kafka_topic, kafka_partition, kafka_offset) should make impossible; verify that "+
				"index still exists before trusting any reconciliation",
			verdict.DuplicatedRecords,
		))
	}

	if len(report.MissingTopics) > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d measured topic(s) do not exist on the broker (%s), so no window could be measured "+
				"for them",
			len(report.MissingTopics), strings.Join(report.MissingTopics, ", "),
		))
	}

	if report.PartitionsUnavailable > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d partition(s) did not report offsets, so no window could be measured for them",
			report.PartitionsUnavailable,
		))
	}

	// RETENTION IS ONLY A CAVEAT WHEN IT TRUNCATES THE MEASURED WINDOW.
	if report.WindowTruncated {
		caveats = append(caveats,
			"retention has removed records that were written inside the measured window, so the "+
				"broker-side count is a lower bound; shorten the reconciliation window or lengthen the "+
				"broker's retention so the window fits inside it",
		)
	}

	if report.WindowPartitionsUnreadable > 0 {
		caveats = append(caveats, fmt.Sprintf(
			"%d partition(s) did not report a window-start offset, so the records they accepted inside "+
				"the window are missing from the total",
			report.WindowPartitionsUnreadable,
		))
	}

	verdict.Caveats = caveats
	verdict.Conclusive = len(caveats) == 0

	return verdict
}

// --------------------------------------------------------------------------- Event
// pipeline statistics — the orchestration behind GET /events/stats
//
// It lives here, in the root package, and not in the HTTP handler. Two costs followed.

// eventOffsetReadTimeout bounds the broker round trip a statistics read makes.
const eventOffsetReadTimeout = 10 * time.Second

// EventOffsetInclusion is how a statistics read wants the BROKER side treated — and,
// because the two are one decision, whether the DISPATCHED HISTORY is counted at all.
type EventOffsetInclusion int

const (
	// EventOffsetsSkipped makes no broker round trip at all and counts no dispatched
	// history: the answer is the exact unresolved inventory. It is the ZERO VALUE and the
	// posture every routine caller wants.
	EventOffsetsSkipped EventOffsetInclusion = iota

	// EventOffsetsBestEffort counts the dispatched history, reads the broker when one is
	// configured, and omits the broker-side fields on any failure, logging the reason. It
	// is the posture the daily reconciliation runbook relies on, and it must be asked for.
	EventOffsetsBestEffort

	// EventOffsetsRequired is EventOffsetsBestEffort except that a failure to read the
	// broker becomes a typed error, because the caller asked specifically for the half of
	// the reconciliation that failed.
	EventOffsetsRequired
)

// countsDispatchedHistory reports whether this posture counts the unbounded dispatched
// population as well as the unresolved inventory.
func (i EventOffsetInclusion) countsDispatchedHistory() bool {
	return i != EventOffsetsSkipped
}

// EventOutboxStatistics is the whole state of the event pipeline at one instant: the
// outbox side always, the broker side and the verdict when they could be measured.
type EventOutboxStatistics struct {
	// GeneratedAt is when the outbox side was read, in UTC.
	GeneratedAt time.Time

	// CountsByStatus is the per-status aggregate, keyed by the model.EventOutboxStatus*
	// values. A status with no rows is ABSENT rather than present with a zero, because
	// GROUP BY only produces rows that exist — read it with the two-value form or accept
	// the zero value.
	CountsByStatus map[string]int64

	// UnreportedStatuses names any status the table holds that model.EventOutboxStatuses
	// does not know about, sorted.
	UnreportedStatuses []string

	// WindowStart is the instant the WINDOWED figures are measured from, and Window is its
	// length. They are reported rather than implied because only one figure here is
	// windowed — the dispatched count — and a reader who cannot see the interval cannot
	// tell a quiet day from a short window.
	WindowStart time.Time
	Window      time.Duration

	// DispatchedHistoryCounted reports whether the dispatched population was counted at
	// all.
	DispatchedHistoryCounted bool

	// Audit is the outbox side of the reconciliation, classified against the very
	// partition windows the broker reported. Meaningful only when AuditRead.
	Audit model.EventRecordIntervalAudit

	// AuditRead reports whether the audit was actually read. False both when the posture
	// skipped the broker side entirely and when the audit query failed under a best-effort
	// posture.
	AuditRead bool

	// Offsets is the broker-side measurement. Meaningful only when OffsetsRead.
	Offsets TopicOffsetReport

	// OffsetsRead reports whether the broker was actually read.
	OffsetsRead bool

	// Reconciliation is the verdict comparing the two sides, or nil when it could not
	// be produced.
	Reconciliation *OutboxReconciliation

	// ProducerAtomicity is the outstanding half of the two pre-recorded intents, or nil
	// when neither census could be read.
	ProducerAtomicity *ProducerAtomicityCensus
}

// eventStatisticsStore is the repository surface the statistics read needs, and
// deliberately no more of it.
type eventStatisticsStore interface {
	// CountUnresolvedEventOutbox returns the per-status aggregate for every NON-DISPATCHED
	// status, exact and unwindowed.
	CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error)

	// CountEventOutboxByStatus returns the same aggregate PLUS the dispatched history. The
	// instant bounds the unbounded dispatched population only; every other status is
	// counted in full however short the window, because a row stuck for days must not
	// vanish from a one-day reading. It is read only when the broker side is being
	// measured, because the dispatched figure exists to be compared against the broker's
	// records.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)

	// AuditEventRecordsInIntervals returns the outbox side of the zero-loss
	// reconciliation, classified against the readable offset windows the broker reported.
	AuditEventRecordsInIntervals(
		ctx context.Context,
		intervals []model.PartitionOffsetInterval,
	) (model.EventRecordIntervalAudit, error)

	// CountBalanceMonitorHandoffByStatus and CountUnfinalizedBulkTransactionBatches are
	// the two PRE-RECORDED INTENT censuses, and they answer the one question the
	// per-status counts above cannot.
	CountBalanceMonitorHandoffByStatus(ctx context.Context) (map[string]int64, error)
	CountUnfinalizedBulkTransactionBatches(
		ctx context.Context,
		olderThan time.Duration,
	) (int64, *time.Time, error)
}

// eventCensusStamper is implemented by a statistics store whose per-status census may be
// a moment old, so the statistics can report WHEN the numbers were taken rather than when
// the response was assembled.
//
// Optional on purpose: a store that always reads live implements nothing and the
// statistics fall back to the wall clock, which is what every test double and every
// direct Datasource caller does.
type eventCensusStamper interface {
	// LastCensusAt is the instant the counts most recently returned were read at, or the
	// zero time when no census has been served yet.
	LastCensusAt() time.Time
}

// eventStatusCensusTTL is how long a per-status census may be reused.
//
// ONE SECOND, and the number is chosen against the consumers rather than picked for
// roundness. The census is O(non-dispatched rows) by nature — counting open work means
// visiting it — so the only way its cost stops tracking the backlog is to stop repeating
// it per request. Measured on a 400,000-row open backlog, one census is ~34ms and takes
// two parallel workers with it, so sixteen concurrent readers were costing 48 backend
// processes to answer one question that has one answer.
//
// The consumers set the ceiling. The load harness's settling gate polls this endpoint
// every 5 seconds by default and needs to see monotonic progress and then stability; the
// metrics collector reads the same census every 15 seconds. A one-second reuse window is
// invisible to both, and it is REPORTED rather than hidden: GeneratedAt becomes the
// instant the counts were read, which is what its documentation already says it is.
const eventStatusCensusTTL = time.Second

// eventStatusCensus memoises the per-status census behind a TTL and collapses concurrent
// readers onto one query.
//
// The two mechanisms do different jobs and both are needed. SINGLE FLIGHT is what stops
// concurrency multiplying the work: sixteen readers arriving together share the one census
// already in progress, so the database does the work once. The TTL is what stops a high
// request RATE doing the same thing sequentially. Neither makes the census cheaper; both
// make its cost independent of how often it is asked for, which is the property that was
// missing.
type eventStatusCensus struct {
	mu sync.Mutex

	// ttl is how long an entry may be reused. Zero selects eventStatusCensusTTL; a
	// negative value disables reuse entirely, which is what a test asserting on raw query
	// counts wants.
	ttl time.Duration

	// entries holds one memo per census variant: the unresolved census, and one per
	// truncated window start. The windowed key carries an instant, so it necessarily moves
	// on — pruneExpiredLocked is what keeps that from being a slow leak.
	entries map[string]*eventCensusEntry

	// takenAt is the instant of the census most recently SERVED, memo or fresh, which is
	// what LastCensusAt reports.
	takenAt time.Time

	// now is the clock, injectable so expiry is testable without sleeping.
	now func() time.Time
}

// eventCensusEntry is one variant's memo, and while loading is also the rendezvous the
// concurrent readers of that variant wait on.
type eventCensusEntry struct {
	// done is closed when the load finishes. A nil channel means the entry is settled.
	done chan struct{}

	// counts, takenAt and err are the load's outcome, written before done is closed and
	// only read afterwards.
	counts  map[string]int64
	takenAt time.Time
	err     error
}

// ProducerAtomicityCensus is how much of the two pre-recorded intents is outstanding.
type ProducerAtomicityCensus struct {
	// MonitorHandoffPending, MonitorHandoffProcessing, MonitorHandoffCompleted and
	// MonitorHandoffFailed are the handoff relay's four states, read straight from the
	// aggregate. A status with no rows is absent from that aggregate and reads as zero
	// here, which is the correct reading of "none in that state".
	MonitorHandoffPending    int64
	MonitorHandoffProcessing int64
	MonitorHandoffCompleted  int64
	MonitorHandoffFailed     int64

	// UnfinalizedBatches counts asynchronous bulk batches that began and never reported an
	// outcome, past the grace period so batches still legitimately running are excluded,
	// and OldestUnfinalizedBatchAt is when the oldest of them began — nil when there are
	// none. The age is what separates a large batch still running from one that was
	// abandoned, so the count alone is not actionable and the pair is.
	UnfinalizedBatches       int64
	OldestUnfinalizedBatchAt *time.Time
}

// unfinalizedBulkBatchGrace is how long a bulk batch may run before an unfinalized
// coordinator row counts as outstanding.
const unfinalizedBulkBatchGrace = 15 * time.Minute

// EventOutboxStatistics assembles the statistics for this instance.
const (
	defaultEventStatisticsWindow = 24 * time.Hour
	maxEventStatisticsWindow     = defaultEventStatisticsWindow
)

// normalizeEventStatisticsWindow bounds a requested window.
func normalizeEventStatisticsWindow(window time.Duration) time.Duration {
	if window <= 0 {
		return defaultEventStatisticsWindow
	}

	if window > maxEventStatisticsWindow {
		return maxEventStatisticsWindow
	}

	return window
}

func (b *Blnk) EventOutboxStatistics(
	ctx context.Context,
	inclusion EventOffsetInclusion,
	window time.Duration,
) (EventOutboxStatistics, error) {
	store, err := b.eventStatisticsStore()
	if err != nil {
		return EventOutboxStatistics{}, err
	}

	return eventOutboxStatistics(ctx, store, b.readEventTopicEndOffsets, inclusion, window)
}

// eventOutboxStatistics is the orchestration itself, with its two collaborators passed
// in.
func eventOutboxStatistics(
	ctx context.Context,
	store eventStatisticsStore,
	readOffsets func(context.Context, time.Time) (TopicOffsetReport, error),
	inclusion EventOffsetInclusion,
	window time.Duration,
) (EventOutboxStatistics, error) {
	ctx, span := tracer.Start(ctx, "EventOutboxStatistics")
	defer span.End()

	if store == nil {
		err := apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventStatisticsDataSourceMissing,
		)
		span.RecordError(err)

		return EventOutboxStatistics{}, err
	}

	if readOffsets == nil {
		// An absent reader is the same situation as an unconfigured broker, and reporting it
		// as that error rather than panicking is what keeps the two postures behaving
		// identically for a caller that has no broker at all.
		readOffsets = func(context.Context, time.Time) (TopicOffsetReport, error) {
			return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
		}
	}

	// ONE WINDOW START, DERIVED ONCE, AND REPORTED. So the request did not get the whole
	// history: it got twenty-four hours of dispatched rows, described to the operator as
	// though it were everything, with no window on the response to say otherwise.
	windowStart := time.Now().UTC().Add(-normalizeEventStatisticsWindow(window))

	// WHICH AGGREGATE, decided by the posture and by nothing else. Skipped takes the
	// unresolved inventory alone — exact, unwindowed, bounded by outstanding work — and
	// the two broker-reading postures additionally count the dispatched history, because
	// that figure exists only to be compared against what the broker recorded. See
	// EventOffsetInclusion for why this is one decision rather than two flags.
	countsHistory := inclusion.countsDispatchedHistory()

	counts, err := readEventStatusCounts(ctx, store, windowStart, countsHistory)
	if err != nil {
		span.RecordError(err)

		return EventOutboxStatistics{}, err
	}

	statistics := EventOutboxStatistics{
		// THE INSTANT THE COUNTS WERE READ, which is what this field's documentation says it
		// is and what a store serving a memoised census reports. A store that always reads
		// live implements nothing and this is the wall clock, unchanged.
		GeneratedAt:              eventCensusInstant(store),
		CountsByStatus:           counts,
		UnreportedStatuses:       unreportedEventOutboxStatuses(counts),
		WindowStart:              windowStart,
		Window:                   normalizeEventStatisticsWindow(window),
		DispatchedHistoryCounted: countsHistory,
		// THE OWED EVENTS, read from PostgreSQL alongside the counts and never from the
		// broker, so the figure is present in every posture — including the one that skips
		// Kafka entirely and the one where the broker is unreachable, which is exactly when
		// an operator is asking what is outstanding.
		ProducerAtomicity: producerAtomicityCensus(ctx, store),
	}

	if len(statistics.UnreportedStatuses) > 0 {
		// Logged HERE as well as returned, because the caller may render it and may
		// not, and this is a schema-drift warning an operator needs to see either way.
		logrus.WithField("statuses", strings.Join(statistics.UnreportedStatuses, ", ")).Warn(
			"the event outbox holds rows in states nothing reports a count for, so the reported " +
				"per-status counts sum to less than the table's row count; teach the statistics " +
				"projection the new state before trusting the zero-loss reconciliation",
		)
	}

	span.SetAttributes(
		attribute.Int("event_outbox.statuses_reported", len(counts)),
		attribute.Int("event_outbox.statuses_unreported", len(statistics.UnreportedStatuses)),
		attribute.Bool("event_outbox.dispatched_history_counted", countsHistory),
	)

	if !countsHistory {
		return statistics, nil
	}

	// MEASURED FROM THE SAME INSTANT THE OUTBOX SIDE WAS COUNTED FROM. Two populations,
	// one verdict — and the verdict could still report itself CONCLUSIVE, because the
	// arithmetic that would have caught the mismatch (a shortfall of records against rows)
	// is only available when both sides name a window, and with the broker side unbounded
	// it was silently switched off. Passing the window start here is what makes the
	// comparison a comparison.
	report, err := readOffsets(ctx, windowStart)
	if err != nil {
		if inclusion == EventOffsetsRequired {
			// The driver error carries transport detail, which renders with broker addresses and
			// topology, so it is logged here and a fixed message is returned in its place.
			withLoggableCause(nil, err).Error(
				"the Kafka topic end offsets could not be read for a statistics request that " +
					"required them",
			)
			span.RecordError(err)

			return statistics, apierror.NewAPIError(
				apierror.ErrKafkaUnavailable,
				"The Kafka broker could not be read, so the topic end offsets this request required "+
					"are unavailable; retry once the broker recovers or omit include_offsets to "+
					"receive the outbox counts alone",
				errors.New("blnk: reading the event topic end offsets failed"),
			)
		}

		withLoggableCause(nil, err).Warn(
			"the Kafka topic end offsets could not be read, so the statistics report the " +
				"per-status counts alone; this is the expected result when no brokers are configured",
		)

		return statistics, nil
	}
	statistics.Offsets, statistics.OffsetsRead = report, true

	// The audit is taken against the windows THIS report measured, never against the whole
	// table: an audit and an offset total that describe different populations cannot be
	// compared, which is the defect the interval form exists to close.
	audit, err := store.AuditEventRecordsInIntervals(ctx, report.PartitionIntervals())
	if err != nil {
		if inclusion == EventOffsetsRequired {
			span.RecordError(err)

			return statistics, err
		}

		withLoggableCause(nil, err).Warn(
			"the event outbox audit could not be read, so the statistics report the per-status " +
				"counts and the measured offsets without the zero-loss verdict",
		)

		return statistics, nil
	}
	statistics.Audit, statistics.AuditRead = audit, true

	// With nothing measured there is nothing to compare against, so no verdict is
	// produced. A verdict computed over zero topics would report a clean bill of health it
	// never established.
	if len(report.Topics) > 0 {
		verdict := ReconcileAgainstOutbox(report, audit)
		statistics.Reconciliation = &verdict

		span.SetAttributes(
			attribute.Bool("event_outbox.reconciliation.conclusive", verdict.Conclusive),
			attribute.Bool("event_outbox.reconciliation.loss_detected", verdict.LossDetected),
		)
	}

	return statistics, nil
}

// eventCensusInstant reports when the counts a store just served were read.
//
// Returns the wall clock for a store that reads live, and never a future instant: a memo's
// stamp can only be older than now, and a clock skewed the other way would make the
// statistics claim to describe a moment that has not happened.
func eventCensusInstant(store eventStatisticsStore) time.Time {
	now := time.Now().UTC()

	stamper, ok := store.(eventCensusStamper)
	if !ok {
		return now
	}

	taken := stamper.LastCensusAt()
	if taken.IsZero() || taken.After(now) {
		return now
	}

	return taken.UTC()
}

// readEventStatusCounts reads whichever per-status aggregate the posture calls for.
func readEventStatusCounts(
	ctx context.Context,
	store eventStatisticsStore,
	windowStart time.Time,
	includeHistory bool,
) (map[string]int64, error) {
	if includeHistory {
		return store.CountEventOutboxByStatus(ctx, windowStart)
	}

	return store.CountUnresolvedEventOutbox(ctx)
}

// producerAtomicityCensus reads the two pre-recorded intent censuses, and answers nil
// rather than zeros when either read fails.
func producerAtomicityCensus(
	ctx context.Context,
	store eventStatisticsStore,
) *ProducerAtomicityCensus {
	handoffs, err := store.CountBalanceMonitorHandoffByStatus(ctx)
	if err != nil {
		withLoggableCause(nil, err).Warn(
			"the balance-monitor handoff census could not be read, so the statistics omit the " +
				"producer-atomicity figures rather than reporting zeros for them; an outstanding " +
				"handoff is a balance movement whose monitor conditions have not been judged, and " +
				"reporting zero would say the opposite",
		)

		return nil
	}

	batches, oldest, err := store.CountUnfinalizedBulkTransactionBatches(ctx, unfinalizedBulkBatchGrace)
	if err != nil {
		withLoggableCause(nil, err).Warn(
			"the unfinalized bulk-batch census could not be read, so the statistics omit the " +
				"producer-atomicity figures rather than reporting a partial answer",
		)

		return nil
	}

	// The four handoff states use the shared outbox status vocabulary rather than one of
	// their own — see model.BalanceMonitorHandoff.Status — so they are read through the
	// same constants the outbox counts are. A status with no rows is absent from the
	// aggregate and indexes to zero, which is the correct reading of "none in that state".
	census := &ProducerAtomicityCensus{
		MonitorHandoffPending:    handoffs[model.OutboxStatusPending],
		MonitorHandoffProcessing: handoffs[model.OutboxStatusProcessing],
		MonitorHandoffCompleted:  handoffs[model.OutboxStatusCompleted],
		MonitorHandoffFailed:     handoffs[model.OutboxStatusFailed],
		UnfinalizedBatches:       batches,
		OldestUnfinalizedBatchAt: oldest,
	}

	// LOGGED ONLY WHEN SOMETHING IS OWED, and at warn, because these two numbers are the
	// ones nothing else in the response can reveal: an unjudged monitor movement and an
	// abandoned batch are both events that will never exist without intervention.
	if census.MonitorHandoffFailed > 0 || census.UnfinalizedBatches > 0 {
		logrus.WithFields(logrus.Fields{
			"monitor_handoff_failed": census.MonitorHandoffFailed,
			"unfinalized_batches":    census.UnfinalizedBatches,
		}).Warn(
			"the event pipeline owes events that no outbox row represents: a failed monitor " +
				"handoff is a balance movement whose conditions were never judged, and an " +
				"unfinalized bulk batch never reported its outcome",
		)
	}

	return census
}

// errEventStatisticsDataSourceMissing is the cause recorded when statistics are asked
// for before a datasource exists. It is a start-up or programming fault rather than a
// caller error, so the cause is kept out of any response body.
var errEventStatisticsDataSourceMissing = errors.New(
	"blnk: event statistics require a datasource",
)

// eventStatisticsStore resolves the repository the statistics read goes through.
func (b *Blnk) eventStatisticsStore() (eventStatisticsStore, error) {
	unavailable := func() error {
		return apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event outbox is unavailable because the service is not initialised",
			errEventStatisticsDataSourceMissing,
		)
	}

	if b == nil {
		return nil, unavailable()
	}

	datasource := b.GetDataSource()
	if datasource == nil {
		return nil, unavailable()
	}

	return &censusMemoStore{eventStatisticsStore: datasource, census: b.statusCensus()}, nil
}

// statusCensus returns the process-wide census memo, building it on first use.
//
// Lazily rather than in NewBlnk because a Blnk assembled as a struct literal — which is
// what several tests and the CLI paths do — must be as bounded as one the constructor
// returned.
func (b *Blnk) statusCensus() *eventStatusCensus {
	b.censusOnce.Do(func() {
		b.eventCensus = &eventStatusCensus{entries: map[string]*eventCensusEntry{}}
	})

	return b.eventCensus
}

// clock reads the census's clock, defaulting to time.Now.
func (c *eventStatusCensus) clock() time.Time {
	if c.now != nil {
		return c.now()
	}

	return time.Now()
}

// reuseWindow reports how long an entry may be reused for.
func (c *eventStatusCensus) reuseWindow() time.Duration {
	if c.ttl == 0 {
		return eventStatusCensusTTL
	}

	return c.ttl
}

// LastCensusAt reports the instant of the census most recently served.
func (c *eventStatusCensus) LastCensusAt() time.Time {
	if c == nil {
		return time.Time{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.takenAt
}

// read serves one census variant, reusing a fresh memo, joining a load already in flight,
// or performing the load itself.
//
// Parameters:
//   - ctx context.Context: bounds this reader. A reader that JOINS a load in flight
//     abandons the wait when its own context ends, so one slow census cannot hold a
//     request past its deadline — the load itself continues for whoever is still waiting.
//   - variant string: the memo key. Must be derived from a normalised input, because it
//     is what bounds the memo's size.
//   - load func(context.Context) (map[string]int64, error): performs the census.
//
// Returns:
//   - map[string]int64: the counts. Copied on the way out, so a caller cannot mutate a
//     memo that other readers are still going to be served.
//   - time.Time: the instant the counts were read.
//   - error: the load's error, shared by every reader that joined it.
func (c *eventStatusCensus) read(
	ctx context.Context,
	variant string,
	load func(context.Context) (map[string]int64, error),
) (map[string]int64, time.Time, error) {
	if c == nil {
		counts, err := load(ctx)

		return counts, time.Now(), err
	}

	for {
		c.mu.Lock()

		entry := c.entries[variant]

		switch {
		case entry != nil && entry.done != nil:
			// A load is in flight for this variant. Wait for it rather than starting a second.
			//
			// THE CHANNEL IS COPIED OUT WHILE THE MUTEX IS STILL HELD, and the wait below is on
			// that copy rather than on the entry's field. The owning reader nils the field when
			// its load settles, so reading entry.done after the unlock would be an
			// unsynchronised read of a field another goroutine writes — and not a harmless one:
			// a reader that observed the nil would select on a nil channel, which never becomes
			// ready, so it would wait out its whole context instead of being woken by the load
			// it was waiting for. The race detector caught this before a request ever did.
			waiting := entry.done
			c.mu.Unlock()

			select {
			case <-waiting:
			case <-ctx.Done():
				return nil, time.Time{}, ctx.Err()
			}

			continue

		case entry != nil && c.clock().Sub(entry.takenAt) < c.reuseWindow():
			// Fresh enough. Errors are never memoised — an entry that failed is left settled
			// with a zero takenAt, so this arm cannot serve one.
			counts, takenAt := copyEventStatusCounts(entry.counts), entry.takenAt
			c.takenAt = takenAt
			c.mu.Unlock()

			return counts, takenAt, nil
		}

		// This reader owns the load. The entry is published BEFORE the query starts so that
		// every reader arriving during it finds the rendezvous rather than starting its own.
		pending := &eventCensusEntry{done: make(chan struct{})}
		c.entries[variant] = pending
		c.mu.Unlock()

		counts, err := load(ctx)
		takenAt := c.clock()

		c.mu.Lock()
		pending.counts = counts
		pending.err = err

		if err == nil {
			pending.takenAt = takenAt
			c.takenAt = takenAt
		} else {
			// A failed census leaves no memo to serve and no memo to expire: the entry is
			// removed so the next reader loads afresh instead of waiting out a TTL on an answer
			// that does not exist.
			delete(c.entries, variant)
		}

		done := pending.done
		pending.done = nil

		// The windowed variant's key moves forward with the clock, so the map would otherwise
		// gain an entry a second and keep every one of them. Pruned here rather than on a timer
		// because this is the only place entries are added.
		c.pruneExpiredLocked(takenAt)

		c.mu.Unlock()

		close(done)

		if err != nil {
			return nil, time.Time{}, err
		}

		return copyEventStatusCounts(counts), takenAt, nil
	}
}

// pruneExpiredLocked drops every settled entry that can no longer be reused. The caller
// must hold the mutex.
func (c *eventStatusCensus) pruneExpiredLocked(now time.Time) {
	window := c.reuseWindow()

	for variant, entry := range c.entries {
		// A load in flight is never pruned: readers are waiting on its channel.
		if entry.done != nil {
			continue
		}

		if now.Sub(entry.takenAt) >= window {
			delete(c.entries, variant)
		}
	}
}

// copyEventStatusCounts copies a census so a memo cannot be mutated by a caller.
func copyEventStatusCounts(counts map[string]int64) map[string]int64 {
	if counts == nil {
		return nil
	}

	copied := make(map[string]int64, len(counts))
	for status, count := range counts {
		copied[status] = count
	}

	return copied
}

// censusMemoStore is an eventStatisticsStore whose two per-status censuses are served
// through the memo, leaving every other read live.
//
// A decorator rather than a change to Datasource, because the memo's lifetime is the
// PROCESS and Datasource is a value type constructed per call. It is also why the memo is
// held on Blnk: one memo per service instance, shared by every request that reaches it.
type censusMemoStore struct {
	eventStatisticsStore

	census *eventStatusCensus
}

// Compile-time proof the decorator can stamp the statistics with the census instant.
var _ eventCensusStamper = (*censusMemoStore)(nil)

// LastCensusAt reports the instant of the census most recently served.
func (s *censusMemoStore) LastCensusAt() time.Time {
	return s.census.LastCensusAt()
}

// CountUnresolvedEventOutbox serves the unresolved census through the memo.
func (s *censusMemoStore) CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error) {
	counts, _, err := s.census.read(ctx, "unresolved", s.eventStatisticsStore.CountUnresolvedEventOutbox)

	return counts, err
}

// CountEventOutboxByStatus serves the windowed census through the memo.
//
// The memo is keyed on the window's LENGTH rather than on the instant derived from it,
// because the instant moves every millisecond and would make every request a miss. The
// counts a reader gets are therefore measured from an instant up to one TTL earlier than
// its own window start — a second's difference in a window measured in hours, and the
// reader is told when the census was taken.
func (s *censusMemoStore) CountEventOutboxByStatus(
	ctx context.Context,
	since time.Time,
) (map[string]int64, error) {
	variant := "history:" + since.Truncate(s.census.reuseWindow()).UTC().Format(time.RFC3339)

	counts, _, err := s.census.read(ctx, variant, func(call context.Context) (map[string]int64, error) {
		return s.eventStatisticsStore.CountEventOutboxByStatus(call, since)
	})

	return counts, err
}

// unreportedEventOutboxStatuses names the statuses present in the aggregate that the
// state-machine enumeration does not know about, sorted.
func unreportedEventOutboxStatuses(counts map[string]int64) []string {
	if len(counts) == 0 {
		return nil
	}

	known := model.EventOutboxStatuses()

	var unreported []string
	for status := range counts {
		if !slices.Contains(known, status) {
			unreported = append(unreported, status)
		}
	}
	if len(unreported) == 0 {
		return nil
	}

	// Map iteration order is unspecified, so the names are sorted to keep the warning —
	// and any test asserting on it — deterministic.
	slices.Sort(unreported)

	return unreported
}

// readEventTopicEndOffsets measures the broker side of the reconciliation.
func (b *Blnk) readEventTopicEndOffsets(ctx context.Context, since time.Time) (TopicOffsetReport, error) {
	if b == nil {
		return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
	}

	configuration := b.Config()
	if configuration == nil || !KafkaBrokersConfigured(configuration.Kafka.Brokers) {
		return TopicOffsetReport{}, ErrKafkaAdminNotConfigured
	}

	admin, err := NewKafkaAdmin(configuration)
	if err != nil {
		return TopicOffsetReport{}, err
	}
	defer func() {
		if closeErr := admin.Close(); closeErr != nil {
			withLoggableCause(nil, closeErr).Warn(
				"closing the Kafka admin client after reading the event topic end offsets failed",
			)
		}
	}()

	measurement, cancel := context.WithTimeout(ctx, eventOffsetReadTimeout)
	defer cancel()

	// No topic list is passed, so the full inventory is measured — see the note on
	// EventOutboxStatistics for why narrowing it would invalidate the verdict.
	return admin.TopicEndOffsets(measurement, since)
}
