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
		GeneratedAt:              time.Now().UTC(),
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

	return datasource, nil
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
