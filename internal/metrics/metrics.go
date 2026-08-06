package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"log"
)

// meter is the package-level OTel meter used to create all instruments.
var meter = otel.Meter("blnk")

func init() {
	if err := Init(); err != nil {
		log.Fatalf("failed to initialize metrics instruments: %v", err)
	}
}

// TransactionTotal counts transactions that reach a terminal or significant state.
// Attributes: status (APPLIED, REJECTED, INFLIGHT, VOID, COMMIT), currency
var TransactionTotal metric.Int64Counter

// TransactionDuration records the wall-clock time of RecordTransaction().
// Attributes: status
var TransactionDuration metric.Float64Histogram

// TransactionRejectedTotal counts rejected transactions broken down by reason.
// Attributes: reason (insufficient_funds, overdraft_limit, lock_contention, max_retries)
var TransactionRejectedTotal metric.Int64Counter

// QueueEnqueuedTotal counts transactions enqueued for async processing.
// Attributes: queue_name
var QueueEnqueuedTotal metric.Int64Counter

// QueueProcessingDuration records the time spent processing a transaction in the worker.
// Attributes: result (success, error, retry)
var QueueProcessingDuration metric.Float64Histogram

// BalanceCreatedTotal counts newly created balances.
var BalanceCreatedTotal metric.Int64Counter

// InflightCommitTotal counts committed inflight transactions.
var InflightCommitTotal metric.Int64Counter

// InflightVoidTotal counts voided inflight transactions.
var InflightVoidTotal metric.Int64Counter

// TransactionBatchSize records the number of transactions in a coalesced batch.
var TransactionBatchSize metric.Int64Histogram

// TransactionBatchTotal counts coalescing attempts.
// Attributes: result (success, failure, skipped)
var TransactionBatchTotal metric.Int64Counter

// HotpairsContentionTotal counts lock contention events.
var HotpairsContentionTotal metric.Int64Counter

// HotpairsLaneRoutedTotal counts transactions routed to queue lanes.
// Attributes: lane (normal, hot)
var HotpairsLaneRoutedTotal metric.Int64Counter

// WorkerRetriesTotal counts worker retry events.
// Attributes: reason (insufficient_funds, lock_contention, other)
var WorkerRetriesTotal metric.Int64Counter

// ChainBacklog is the number of transactions still waiting to be sealed into the
// hash chain (the chainer's backlog).
var ChainBacklog metric.Int64Gauge

// ChainHeadSeq is the sequence number of the chain head — total transactions sealed.
var ChainHeadSeq metric.Int64Gauge

// ChainLagSeconds is the age of the chain head: seconds since the chainer last
// advanced it.
var ChainLagSeconds metric.Float64Gauge

// EventsPublishedTotal counts ledger events that were published to a Kafka category
// topic and acknowledged by the broker.
//
// It counts ORIGINAL events only — one increment per event that reached its category
// topic for the first time. Replays and dead-letter writes are deliberately excluded,
// and that exclusion is what makes the dead-letter rate meaningful: the rate is
// EventsDeadLetteredTotal over this counter, so the two have to count the same
// population of events. Letting an operator-triggered replay add to this denominator
// would dilute the rate by an amount that depends on how much triage happened, and
// counting a dead-letter write here as well as there would count one event twice.
//
// Replays are not invisible as a result — they are visible on
// EventPublishAttemptsTotal and EventPublishDuration under their own fixed attempt
// attribute, which is where re-delivery belongs.
//
// Attributes: topic, event_type
var EventsPublishedTotal metric.Int64Counter

// EventPublishAttemptsTotal counts individual relay publish attempts, retries included,
// so retry pressure stays visible independently of how many events were delivered.
//
// The outcome vocabulary is CLOSED and MUTUALLY EXCLUSIVE — exactly one value is recorded
// per attempted write, so summing the series yields the total number of attempts:
//
//   - dispatched: the broker acknowledged the write.
//   - retrying: the attempt failed and another attempt is possible under the stated
//     retry budget.
//   - failed: the attempt failed and NO further attempt is possible — either the failure
//     is permanent (a message the topic will never accept, an unauthorised principal) or
//     the attempt exhausted the budget. Distinguishing this from retrying is what makes
//     "how many events are actually stuck" answerable; labelling a final failure
//     "retrying" reports retry pressure that no longer exists.
//   - dead_lettered: an acknowledged write to a `<topic>.dlt` sibling, which is the
//     terminal outcome of the ORIGINAL event.
//
// Attributes: outcome (dispatched, retrying, failed, dead_lettered)
var EventPublishAttemptsTotal metric.Int64Counter

// EventPublishDuration records how long a single publish took, measured from the moment
// the pipeline started working on the event — the outbox claim for a relay publish, the
// request instant for a replay — to broker acknowledgement.
//
// # The SLO population
//
// The sub-two-second p99 target is stated for FIRST-ATTEMPT, SUCCESSFUL publishes of
// original events, so that population has to be selectable rather than blended. Two
// bounded attributes make it so, and the target reads exactly:
//
//	histogram_quantile(0.99, sum by (le) (rate(
//	  blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))
//
// Failed and retried attempts are recorded too — a broker that times out is precisely
// when latency data matters — but they carry a different outcome, so they cannot
// contaminate the figure above.
//
// # Why the attribute domains are closed
//
// Both attributes are drawn from fixed vocabularies, because a histogram multiplies its
// label cardinality by its bucket count: one unbounded label is enough to make this
// instrument the most expensive series in the exporter. attempt is the attempt number for
// an original publish, capped at the retry budget (see the note below), and a fixed token
// for the two publishes that are not part of a retry sequence. Configuration cannot widen
// it: config.setRelayDefaults clamps RELAY_MAX_RETRY_ATTEMPTS to 5, and the recording
// helper collapses anything above that into "over".
//
// Attributes:
//   - topic: the destination topic.
//   - attempt: 1, 2, 3, 4, 5, over, replay, dead_letter.
//   - outcome: dispatched, retrying, failed, dead_lettered (as above).
var EventPublishDuration metric.Float64Histogram

// EventsDeadLetteredTotal counts ledger events diverted to a dead-letter topic after the
// relay exhausted its retry budget. The topic attribute carries the original category
// topic, not the .dlt sibling, so it is comparable with EventsPublishedTotal.
// Attributes: topic, event_type
var EventsDeadLetteredTotal metric.Int64Counter

// The three gauges below are maintained by ONE production caller, the periodic
// EventMetricsCollector in event_metrics.go, and by nothing else. That single ownership is
// a correctness requirement rather than tidiness, for two reasons that apply to every
// gauge in an exporter and to these three in particular.
//
// A gauge RETAINS ITS LAST VALUE until it is written again or the process restarts, so a
// value recorded once at the moment something went wrong keeps alerting long after the
// condition has cleared. Each of these is therefore re-recorded on every collector tick
// from authoritative state, including an explicit ZERO when there is nothing outstanding:
// zero is the measurement that clears the alert, and omitting it is what leaves a stale
// one firing.
//
// A gauge written only from the code path that CAUSES the condition also measures the
// wrong thing. Recording the dead-letter age where an event is dead-lettered would report
// the age of something that just happened — always near zero, so the "stuck for 15
// minutes" alert could never fire while looking healthy throughout. The collector instead
// reads the oldest outstanding entry, which is what the alert is expressed against.
//
// The same reasoning gives the chain gauges above their chain_worker.go tick, so this is
// the house pattern for a gauge rather than a local invention.

// DLTOldestMessageAgeSeconds is the age of the oldest unresolved message sitting on a
// dead-letter topic: seconds since it was dead-lettered, measured from the outbox row's
// persisted dead_lettered_at. Alerting fires above 900s. Every dead-letter topic Blnk owns
// is recorded on every collector tick, zero included.
// Attributes: topic
var DLTOldestMessageAgeSeconds metric.Float64Gauge

// SubscriberConsumerLag is how many messages a subscriber's consumer group trails the log
// end offset by, computed in-process by differencing committed offsets against end offsets.
// Alerting fires above 10000 messages.
//
// Its three attributes are the only DYNAMIC label set among the event instruments, so they
// are bounded twice over: the collector measures a bounded number of registry subscribers
// per tick, and every value is validated to be an opaque generated identifier before it is
// recorded, so a human-chosen consumer group name can neither carry tenant-identifying
// text into an exported label nor grow the series count without limit. A series whose
// subscriber disappears from the registry is explicitly zeroed on the next tick rather
// than left at its last reading.
//
// Attributes: subscriber, group, topic
var SubscriberConsumerLag metric.Int64Gauge

// OutboxPendingBacklog is the number of event outbox rows still waiting to be published
// to Kafka: the relay's backlog, counted as pending plus processing.
//
// Processing rows are INCLUDED deliberately. A row in that state has been claimed by a
// relay instance but not yet acknowledged by the broker, so it is still un-published work;
// counting only pending rows would report a drained backlog at exactly the moment a stalled
// relay is holding every claimable row under a lease.
var OutboxPendingBacklog metric.Int64Gauge

// EventPublishDurationBuckets are the explicit bucket boundaries, in SECONDS, of
// EventPublishDuration.
//
// They are stated rather than inherited because the OTel default histogram boundaries —
// 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 — are chosen for
// MILLISECOND-scale measurements. Applied to a second-valued instrument they place every
// realistic publish latency in the first bucket, [0, 5], so histogram_quantile can only
// ever interpolate inside a five-second span: the sub-two-second p99 target becomes
// unmeasurable, and a regression from 50 ms to 4 s would not move the reported quantile at
// all.
//
// The boundaries below are dense either side of the values that decide the target and
// sparse beyond it:
//
//   - 5 ms to 500 ms covers a healthy local or same-zone broker, so normal p50/p90/p99
//     movement is visible rather than flattened into one bucket.
//   - 1 s, 1.5 s and 2 s bracket the target itself. 2 s is a boundary EXACTLY because the
//     requirement is stated at two seconds: with a bucket edge there, "is p99 under 2 s"
//     is answered by bucket counts alone and needs no interpolation across the threshold,
//     which is the one reading that must not be an estimate.
//   - 3 s to 30 s keeps a degraded broker on the scale instead of dumping it into +Inf,
//     where it would be invisible. 30 s is the retry backoff cap and the last edge, so
//     anything slower than the whole retry budget lands in +Inf, which is the correct
//     place for it.
//
// 16 boundaries means 17 buckets per attribute combination. With both attribute domains
// closed (see EventPublishDuration) the series count stays bounded and predictable.
var EventPublishDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5,
	0.75, 1, 1.5, 2, 3, 5, 10, 30,
}

// Init creates all metric instruments. It should be called once during application
// startup. Safe to call even when observability is disabled
func Init() error {
	var err error

	TransactionTotal, err = meter.Int64Counter("blnk.transaction.total",
		metric.WithDescription("Total number of transactions by status and currency"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionDuration, err = meter.Float64Histogram("blnk.transaction.duration",
		metric.WithDescription("Duration of RecordTransaction processing"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	TransactionRejectedTotal, err = meter.Int64Counter("blnk.transaction.rejected.total",
		metric.WithDescription("Total number of rejected transactions by reason"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	QueueEnqueuedTotal, err = meter.Int64Counter("blnk.queue.enqueued.total",
		metric.WithDescription("Total number of transactions enqueued for processing"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	QueueProcessingDuration, err = meter.Float64Histogram("blnk.queue.processing.duration",
		metric.WithDescription("Duration of worker transaction processing"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	BalanceCreatedTotal, err = meter.Int64Counter("blnk.balance.created.total",
		metric.WithDescription("Total number of balances created"),
		metric.WithUnit("{balance}"),
	)
	if err != nil {
		return err
	}

	InflightCommitTotal, err = meter.Int64Counter("blnk.inflight.commit.total",
		metric.WithDescription("Total number of inflight transactions committed"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	InflightVoidTotal, err = meter.Int64Counter("blnk.inflight.void.total",
		metric.WithDescription("Total number of inflight transactions voided"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionBatchSize, err = meter.Int64Histogram("blnk.transaction.batch.size",
		metric.WithDescription("Number of transactions in a coalesced batch"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionBatchTotal, err = meter.Int64Counter("blnk.transaction.batch.total",
		metric.WithDescription("Total number of batch coalescing attempts by result"),
		metric.WithUnit("{batch}"),
	)
	if err != nil {
		return err
	}

	HotpairsContentionTotal, err = meter.Int64Counter("blnk.hotpairs.contention.total",
		metric.WithDescription("Total number of lock contention events"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	HotpairsLaneRoutedTotal, err = meter.Int64Counter("blnk.hotpairs.lane.routed.total",
		metric.WithDescription("Total number of transactions routed to queue lanes"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	WorkerRetriesTotal, err = meter.Int64Counter("blnk.worker.retries.total",
		metric.WithDescription("Total number of worker retry events by reason"),
		metric.WithUnit("{retry}"),
	)
	if err != nil {
		return err
	}

	ChainBacklog, err = meter.Int64Gauge("blnk.chain.backlog",
		metric.WithDescription("Number of transactions not yet sealed into the hash chain"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	ChainHeadSeq, err = meter.Int64Gauge("blnk.chain.head_seq",
		metric.WithDescription("Sequence number of the hash-chain head"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	ChainLagSeconds, err = meter.Float64Gauge("blnk.chain.lag_seconds",
		metric.WithDescription("Seconds since the hash chain last advanced"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	EventsPublishedTotal, err = meter.Int64Counter("blnk.events.published.total",
		metric.WithDescription("Total number of ledger events acknowledged by Kafka by topic and event type"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	EventPublishAttemptsTotal, err = meter.Int64Counter("blnk.events.publish.attempts.total",
		metric.WithDescription("Total number of event publish attempts by outcome, retries included"),
		metric.WithUnit("{attempt}"),
	)
	if err != nil {
		return err
	}

	EventPublishDuration, err = meter.Float64Histogram("blnk.events.publish.duration",
		metric.WithDescription("Duration of a single event publish attempt, from outbox claim to broker acknowledgement"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(EventPublishDurationBuckets...),
	)
	if err != nil {
		return err
	}

	EventsDeadLetteredTotal, err = meter.Int64Counter("blnk.events.dead_lettered.total",
		metric.WithDescription("Total number of events dead-lettered after retry exhaustion by topic and event type"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	DLTOldestMessageAgeSeconds, err = meter.Float64Gauge("blnk.dlt.oldest_message_age_seconds",
		metric.WithDescription("Seconds since the oldest unresolved dead-letter message was dead-lettered"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	SubscriberConsumerLag, err = meter.Int64Gauge("blnk.kafka.consumer_lag",
		metric.WithDescription("Number of messages a subscriber consumer group trails the log end offset by"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return err
	}

	OutboxPendingBacklog, err = meter.Int64Gauge("blnk.outbox.pending",
		metric.WithDescription("Number of event outbox rows not yet published to Kafka"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	return nil
}
