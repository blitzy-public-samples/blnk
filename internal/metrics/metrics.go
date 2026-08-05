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
// topic and acknowledged by the broker. Paired with EventsDeadLetteredTotal, which is
// scoped identically, so the dead-letter rate is the ratio of the two.
// Attributes: topic, event_type
var EventsPublishedTotal metric.Int64Counter

// EventPublishAttemptsTotal counts individual relay publish attempts, retries included,
// so retry pressure stays visible independently of how many events were delivered.
// Attributes: outcome (dispatched, retrying, dead_lettered)
var EventPublishAttemptsTotal metric.Int64Counter

// EventPublishDuration records how long a single publish attempt took, measured from the
// moment the relay claimed the outbox row to broker acknowledgement. The attempt attribute
// carries the attempt number as a string, so publish latency excluding retries reads as
// histogram_quantile(0.99, rate(blnk_events_publish_duration_seconds_bucket{attempt="1"}[5m])).
// Attributes: topic, attempt (1, 2, 3, 4, 5)
var EventPublishDuration metric.Float64Histogram

// EventsDeadLetteredTotal counts ledger events diverted to a dead-letter topic after the
// relay exhausted its retry budget. The topic attribute carries the original category
// topic, not the .dlt sibling, so it is comparable with EventsPublishedTotal.
// Attributes: topic, event_type
var EventsDeadLetteredTotal metric.Int64Counter

// DLTOldestMessageAgeSeconds is the age of the oldest unresolved message sitting on a
// dead-letter topic: seconds since it was dead-lettered. Alerting fires above 900s.
// Attributes: topic
var DLTOldestMessageAgeSeconds metric.Float64Gauge

// SubscriberConsumerLag is how many messages a subscriber's consumer group trails the log
// end offset by, computed in-process by differencing committed offsets against end offsets.
// Alerting fires above 10000 messages.
// Attributes: subscriber, group, topic
var SubscriberConsumerLag metric.Int64Gauge

// OutboxPendingBacklog is the number of event outbox rows still waiting to be published
// to Kafka (the relay's backlog).
var OutboxPendingBacklog metric.Int64Gauge

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
