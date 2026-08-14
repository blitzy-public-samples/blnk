package metrics

import (
	"context"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meter is the package-level OTel meter used to create all instruments.
var meter = otel.Meter("blnk")

func init() {
	if err := Init(); err != nil {
		log.Fatalf("failed to initialize metrics instruments: %v", err)
	}
}

// TransactionTotal counts transactions that reach a terminal or significant state.
var TransactionTotal metric.Int64Counter

// TransactionDuration records the wall-clock time of RecordTransaction().
var TransactionDuration metric.Float64Histogram

// TransactionRejectedTotal counts rejected transactions broken down by reason.
var TransactionRejectedTotal metric.Int64Counter

// QueueEnqueuedTotal counts transactions enqueued for async processing.
var QueueEnqueuedTotal metric.Int64Counter

// QueueProcessingDuration records the time spent processing a transaction in the worker.
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
var TransactionBatchTotal metric.Int64Counter

// HotpairsContentionTotal counts lock contention events.
var HotpairsContentionTotal metric.Int64Counter

// HotpairsLaneRoutedTotal counts transactions routed to queue lanes.
var HotpairsLaneRoutedTotal metric.Int64Counter

// WorkerRetriesTotal counts worker retry events.
var WorkerRetriesTotal metric.Int64Counter

// ChainBacklog is the number of transactions still waiting to be sealed into the
// hash chain (the chainer's backlog).
var ChainBacklog metric.Int64Gauge

// ChainHeadSeq is the sequence number of the chain head — total transactions sealed.
var ChainHeadSeq metric.Int64Gauge

// ChainLagSeconds is the age of the chain head: seconds since the chainer last
// advanced it.
var ChainLagSeconds metric.Float64Gauge

// EventsPublishedTotal counts ORIGINAL LEDGER EVENTS ONCE EACH, at the moment their
// Kafka leg becomes durably recorded — the broker acknowledged the write AND the
// claim-token-conditional statement that records that leg has committed.
var EventsPublishedTotal metric.Int64Counter

// EventBrokerAcknowledgementsTotal counts publishes the BROKER ACKNOWLEDGED, whether or
// not the outbox row was subsequently marked.
var EventBrokerAcknowledgementsTotal metric.Int64Counter

// EventsDispatchedTotal counts ledger events ONCE EACH, at the moment their delivery
// becomes durably recorded: the outbox row reaches the dispatched state under the claim
// token that authorised the publish.
var EventsDispatchedTotal metric.Int64Counter

// EventPublishAttemptsTotal counts individual relay publish attempts, retries included,
// so retry pressure stays visible independently of how many events were delivered.
var EventPublishAttemptsTotal metric.Int64Counter

// EventPublishDuration records how long a single publish took, measured from the moment
// the pipeline started working on the event — the outbox claim for a relay publish, the
// request instant for a replay — to broker acknowledgement.
var EventPublishDuration metric.Float64Histogram

// EventCaptureToDispatchDuration records the END-TO-END age of a published event: the
// interval from the instant the event was CAPTURED in the transactional outbox, inside
// the ledger transaction that produced it, to the instant the broker acknowledged its
// publish.
var EventCaptureToDispatchDuration metric.Float64Histogram

// EventsDeadLetteredTotal counts ledger events diverted to a dead-letter topic after the
// relay exhausted its retry budget. The topic attribute carries the original category
// topic, not the .dlt sibling, so it is comparable with EventsDispatchedTotal — the two
// partition the terminal outcomes of a captured event, and their sum is the population the
// dead-letter rate is a fraction of.
var EventsDeadLetteredTotal metric.Int64Counter

// EventRelayClaimsTotal counts the relay's outbox CLAIMS by outcome — rows, empty, error
// or timeout — and EventRelayClaimDuration records how long each one took.
//
// THIS PAIR EXISTS BECAUSE A RELAY THAT HAS STOPPED CLAIMING LOOKS EXACTLY LIKE A RELAY
// WITH NOTHING TO DO. Every other instrument in this file is fed by a publish: they all
// read zero whether the relay is idle, wedged inside a claim, or dead. A claim that ran for
// 504 seconds without returning was observed producing no log line, no error and no counter
// movement, and the only way to see it was a goroutine dump. These two move on the claim
// itself, so an idle relay reads as a rising `empty` count with a millisecond duration,
// while a struggling one reads as a duration climbing into seconds and, past the relay's
// claim budget, as a rising `timeout` count.
var (
	EventRelayClaimsTotal   metric.Int64Counter
	EventRelayClaimDuration metric.Float64Histogram
	EventRelayClaimedRows   metric.Int64Counter
)

// EVERY GAUGE BELOW is maintained by ONE production caller, the periodic
// EventMetricsCollector in event_metrics.go, and by nothing else — the dead-letter age,
// the consumer-lag pair and its coverage gauges, the outbox backlog, the registry size,
// and the revocation, orphan and settlement gauges. The set has grown as the subscriber
// lifecycle did, so it is named by its OWNER rather than by a count that would drift
// the next time one is added; what matters is that no other caller writes any of them.

// DLTOldestMessageAgeSeconds is the age, in seconds, of the oldest UNRESOLVED
// dead-letter entry destined for each dead-letter topic. Alerting fires above 900s.
//
// THE ANCHOR DEPENDS ON WHETHER THE ENTRY REACHED ITS TOPIC, because only one of the two
// states has a timestamp nothing rewrites. An entry that has been preserved on its
// `<topic>.dlt` sibling is MEASURED FROM last_attempted_at, the moment the event was
// given up on and therefore the moment it started waiting for a human. An entry whose
// dead-letter write is still OWED is measured from first_attempted_at, because the repair
// pass re-claims exactly those rows every poll tick and stamps last_attempted_at as it
// does — anchoring them there let the process failing to preserve an event reset that
// event's own triage clock, so the entries that exist in no Kafka topic at all were the
// ones this gauge could not age. There is no dead_lettered_at column; when neither
// attempt timestamp is present the row's occurred_at is used instead, which is older and
// so errs toward reporting a problem rather than hiding one. The authoritative
// implementation is RefreshDeadLetterAgeGauge.
var DLTOldestMessageAgeSeconds metric.Float64Gauge

// SubscriberConsumerLag is how many messages a subscriber's consumer group trails the
// log end offset by, computed in-process by differencing committed offsets against end
// offsets. Alerting fires above 10000 messages.
var SubscriberConsumerLag metric.Int64ObservableGauge

// ConsumerLagUnmeasuredPartitions is how many of a topic's partitions could not be read
// when its lag was last measured. Zero is the healthy reading and is reported
// explicitly.
var ConsumerLagUnmeasuredPartitions metric.Int64ObservableGauge

// SubscriberLagPassAgeSeconds is how long the IN-PROGRESS pass over the subscriber
// registry has been running, and SubscriberLagCoveredSubscribers is how many
// subscribers currently have an exported lag series. Together they are the COVERAGE
// statement for consumer-lag measurement.
var (
	SubscriberLagPassAgeSeconds     metric.Float64Gauge
	SubscriberLagCoveredSubscribers metric.Int64Gauge
)

// ConsumerLagSample is one topic's entry in the consumer-lag inventory.
type ConsumerLagSample struct {
	// Subscriber, Group and Topic are the RESOLVED label values — already reduced to the
	// bounded vocabulary by the caller. The metrics package does not know the identifier
	// rules, so it cannot bound them itself; it exports what it is given, which is why the
	// caller resolving them is part of this instrument's contract.
	Subscriber string
	Group      string
	Topic      string

	// Lag is the number of messages the group trails the log end offset by. It is observed
	// only when LagComplete is true.
	Lag int64

	// LagComplete states whether every partition of this topic was readable. When it is
	// false the sample contributes NO lag observation at all — see
	// ConsumerLagUnmeasuredPartitions for why a partial total must not be published — and
	// only UnmeasuredPartitions is observed.
	LagComplete bool

	// UnmeasuredPartitions is how many partitions could not be read. It is observed
	// unconditionally, including as zero, because zero is the reading that says the
	// measurement is trustworthy.
	UnmeasuredPartitions int
}

// consumerLagInventory holds the snapshot the asynchronous gauges observe.
var consumerLagInventory struct {
	mu      sync.RWMutex
	samples []ConsumerLagSample
}

// PublishConsumerLagInventory replaces the consumer-lag inventory the asynchronous
// gauges report.
//
// Parameters:
//   - samples []ConsumerLagSample: the complete current inventory, with label values
//     already resolved to their bounded vocabularies.
func PublishConsumerLagInventory(samples []ConsumerLagSample) {
	copied := make([]ConsumerLagSample, len(samples))
	copy(copied, samples)

	consumerLagInventory.mu.Lock()
	consumerLagInventory.samples = copied
	consumerLagInventory.mu.Unlock()
}

// ConsumerLagInventory returns the currently published inventory.
//
// Returns:
//   - []ConsumerLagSample: a fresh slice; empty when nothing has been published.
func ConsumerLagInventory() []ConsumerLagSample {
	consumerLagInventory.mu.RLock()
	defer consumerLagInventory.mu.RUnlock()

	copied := make([]ConsumerLagSample, len(consumerLagInventory.samples))
	copy(copied, consumerLagInventory.samples)

	return copied
}

// observeConsumerLagInventory is the callback both asynchronous gauges are registered
// with.
func observeConsumerLagInventory(_ context.Context, observer metric.Observer) error {
	samples := ConsumerLagInventory()

	for _, sample := range samples {
		attributes := metric.WithAttributes(
			attribute.String("subscriber", sample.Subscriber),
			attribute.String("group", sample.Group),
			attribute.String("topic", sample.Topic),
		)

		// Observed unconditionally, zero included: zero is the measurement that says the
		// lag figure beside it can be trusted, and omitting it would make a healthy topic
		// indistinguishable from one that is not being measured at all.
		observer.ObserveInt64(ConsumerLagUnmeasuredPartitions, int64(sample.UnmeasuredPartitions), attributes)

		// WITHHELD when the measurement is incomplete. See
		// ConsumerLagUnmeasuredPartitions: a partial sum is a lower bound, and exporting it
		// would resolve the lag alert using a number known to be too small.
		if !sample.LagComplete {
			continue
		}

		observer.ObserveInt64(SubscriberConsumerLag, sample.Lag, attributes)
	}

	return nil
}

// EventMetricsLastCollectionAgeSeconds is how long ago the periodic event-metrics
// collector last FINISHED a collection, whatever that collection reported.
var EventMetricsLastCollectionAgeSeconds metric.Float64ObservableGauge

// EventMetricsLastSuccessAgeSeconds is how long ago the periodic event-metrics
// collector last completed a collection with NO failures.
var EventMetricsLastSuccessAgeSeconds metric.Float64ObservableGauge

// EventMetricsCollectionFailuresTotal counts individual collection failures inside the
// periodic event-metrics collector, attributed by WHICH collection failed.
var EventMetricsCollectionFailuresTotal metric.Int64Counter

// ConsumerLagInventoryComplete reports whether the last lag sweep measured EVERY
// registered subscriber: 1 for yes, 0 for no.
var ConsumerLagInventoryComplete metric.Int64Gauge

// SubscribersUnmeasured is how many registered subscribers the last sweep did not
// publish a complete lag reading for, attributed by why.
var SubscribersUnmeasured metric.Int64Gauge

// SubscribersRegistered is how many subscribers the registry holds.
var SubscribersRegistered metric.Int64Gauge

// SubscriberMeasurementBudget is how many subscribers one consumer-lag sweep may
// measure.
var SubscriberMeasurementBudget metric.Int64Gauge

// The closed vocabulary of SubscribersUnmeasured's "reason" attribute.
const (
	// SubscribersUnmeasuredReasonBudget is a subscriber the sweep's per-tick budget stopped it
	// from examining. Temporary: the sweep resumes from a rotating cursor, so the subscriber is
	// reached on a later tick.
	SubscribersUnmeasuredReasonBudget = "budget"

	// SubscribersUnmeasuredReasonUnprovisioned is a registry row with no authorised topics, or
	// with identifiers the registry did not issue. There is nothing to measure.
	SubscribersUnmeasuredReasonUnprovisioned = "unprovisioned"

	// SubscribersUnmeasuredReasonMeasureFailed is a subscriber whose measurement the broker
	// refused.
	SubscribersUnmeasuredReasonMeasureFailed = "measure_failed"

	// SubscribersUnmeasuredReasonRegistryFailed is the count not yet examined when the registry
	// enumeration itself failed, so the true number unmeasured is at least this.
	SubscribersUnmeasuredReasonRegistryFailed = "registry_failed"

	// SubscribersUnmeasuredReasonTopicMissing is a subscriber at least one of whose
	// authorised topics does not exist at the broker. The measurement itself SUCCEEDED,
	// which is why this needs its own reason: a topic that is absent contributes no lag
	// and no unreadable partitions, so the subscriber looks measured and the gap it leaves
	// is silent.
	SubscribersUnmeasuredReasonTopicMissing = "topic_missing"

	// SubscribersUnmeasuredReasonBrokerUnconfigured is a registry row on a deployment with
	// no KAFKA_BROKERS at all. There is no broker to difference offsets against, so nothing
	// about the row can be measured however healthy it is.
	//
	// IT NEEDS ITS OWN REASON BECAUSE EVERY OTHER ONE MISDIRECTS. These rows were reported
	// under `budget`, whose documented remedy is to raise the measurement budget — the one
	// action that cannot possibly help, since no amount of budget produces an offset from a
	// broker that was never configured. The remedy here is to configure KAFKA_BROKERS or to
	// remove registry rows the deployment is not using.
	SubscribersUnmeasuredReasonBrokerUnconfigured = "broker_unconfigured"
)

// SubscriberUnmeasuredReasons returns every value of SubscribersUnmeasured's reason
// attribute.
//
// Returns:
//   - []string: a fresh slice, so a caller cannot mutate the vocabulary.
func SubscriberUnmeasuredReasons() []string {
	return []string{
		SubscribersUnmeasuredReasonBudget,
		SubscribersUnmeasuredReasonUnprovisioned,
		SubscribersUnmeasuredReasonMeasureFailed,
		SubscribersUnmeasuredReasonRegistryFailed,
		SubscribersUnmeasuredReasonTopicMissing,
		SubscribersUnmeasuredReasonBrokerUnconfigured,
	}
}

// eventMetricsCollectionHealth holds the timestamps the asynchronous collection-health
// gauges are observed from.
var eventMetricsCollectionHealth struct {
	mu           sync.RWMutex
	firstAttempt time.Time
	lastAttempt  time.Time
	lastSuccess  time.Time
}

// RecordEventMetricsCollection records that a collection finished, and whether it was
// complete.
//
// Parameters:
//   - at time.Time: when the collection finished. A zero value is ignored, since it
//     would otherwise mark the collector as never having run.
//   - complete bool: true when the collection reported no failures at all.
func RecordEventMetricsCollection(at time.Time, complete bool) {
	if at.IsZero() {
		return
	}

	eventMetricsCollectionHealth.mu.Lock()
	defer eventMetricsCollectionHealth.mu.Unlock()

	// SET ONCE and never moved, including by an earlier-stamped straggler: it anchors the
	// "never succeeded" age, and an anchor that advanced would make that age shrink.
	if eventMetricsCollectionHealth.firstAttempt.IsZero() {
		eventMetricsCollectionHealth.firstAttempt = at
	}
	if at.After(eventMetricsCollectionHealth.lastAttempt) {
		eventMetricsCollectionHealth.lastAttempt = at
	}
	if complete && at.After(eventMetricsCollectionHealth.lastSuccess) {
		eventMetricsCollectionHealth.lastSuccess = at
	}
}

// EventMetricsCollectionHealth returns the recorded collection timestamps.
//
// Returns:
//   - firstAttempt time.Time: when the first collection finished. Zero before any has.
//   - lastAttempt time.Time: when a collection last finished. Zero when none ever has.
//   - lastSuccess time.Time: when a collection last finished with no failures.
func EventMetricsCollectionHealth() (firstAttempt, lastAttempt, lastSuccess time.Time) {
	eventMetricsCollectionHealth.mu.RLock()
	defer eventMetricsCollectionHealth.mu.RUnlock()

	return eventMetricsCollectionHealth.firstAttempt,
		eventMetricsCollectionHealth.lastAttempt,
		eventMetricsCollectionHealth.lastSuccess
}

// observeEventMetricsCollectionAges is the callback both collection-health gauges are
// registered with.
func observeEventMetricsCollectionAges(_ context.Context, observer metric.Observer) error {
	firstAttempt, lastAttempt, lastSuccess := EventMetricsCollectionHealth()
	now := time.Now()

	if lastAttempt.IsZero() {
		// Nothing has finished a collection yet, so neither age exists. This is the millisecond
		// window between Start and the first tick completing; a permanent absence here means no
		// collector, which absent() covers.
		return nil
	}

	observer.ObserveFloat64(EventMetricsLastCollectionAgeSeconds, nonNegativeAge(now, lastAttempt))

	since := lastSuccess
	if since.IsZero() {
		since = firstAttempt
	}
	observer.ObserveFloat64(EventMetricsLastSuccessAgeSeconds, nonNegativeAge(now, since))

	return nil
}

// nonNegativeAge is the age of an instant, floored at zero.
func nonNegativeAge(now, at time.Time) float64 {
	age := now.Sub(at).Seconds()
	if age < 0 {
		return 0
	}

	return age
}

// OutboxPendingBacklog is the number of event outbox rows still waiting to be published
// to Kafka: the relay's backlog, counted as pending plus processing.
var OutboxPendingBacklog metric.Int64Gauge

// QueueBacklog is the number of tasks waiting in an asynq queue, attributed by queue name
// and by the state the task is waiting in.
//
// It exists because a queue whose arrival rate exceeds its drain rate has no symptom until
// something else breaks. The index queue reached 609,673 pending tasks under a sustained
// load run while draining at 276/s against 550/s arriving, and nothing reported it: the
// dashboard would have shown it to anyone who opened the dashboard, and no alert could fire
// because no series existed. Publishing the depth is what makes the imbalance detectable
// while it is still only an imbalance.
var QueueBacklog metric.Int64Gauge

// The three REPAIR instruments. Together they answer the only two questions worth
// asking about a recovery in progress: how much is owed, and how fast it is being paid.

// EventRepairBacklog is how many rows each repair leg still owes.
var EventRepairBacklog metric.Int64Gauge

// EventRepairsCompletedTotal counts rows a repair pass actually repaired, attributed by
// leg.
var EventRepairsCompletedTotal metric.Int64Counter

// EventRepairSaturated reports whether the per-tick chaining bound stopped a repair
// pass with work still outstanding: 1 when the last tick used its whole budget, 0 when
// it drained.
var EventRepairSaturated metric.Int64Gauge

// EventsPurgedTotal counts event outbox rows DELETED by the retention sweep.
var EventsPurgedTotal metric.Int64Counter

// SubscriberRevocationsPending is the number of subscribers whose broker-side
// credential revocation is still OWED, and OldestSubscriberRevocationAgeSeconds is how
// long the oldest of them has been outstanding.
var (
	SubscriberRevocationsPending         metric.Int64Gauge
	OldestSubscriberRevocationAgeSeconds metric.Float64Gauge
)

// SubscriberSettlementOutstanding is how many subscribers owe broker-side
// reconciliation, SubscriberGrantReconcilePending and
// SubscriberCredentialCleanupPending split that by kind, and
// OldestSubscriberSettlementAgeSeconds is how long the oldest of them has been
// outstanding.
var (
	SubscriberSettlementOutstanding      metric.Int64Gauge
	SubscriberGrantReconcilePending      metric.Int64Gauge
	SubscriberCredentialCleanupPending   metric.Int64Gauge
	OldestSubscriberSettlementAgeSeconds metric.Float64Gauge
)

// SubscriberCredentialOrphans and OldestSubscriberCredentialOrphanAgeSeconds describe
// credentials that outlived their registry record; SubscriberRevocationFailures and
// OldestSubscriberRevocationFailureAgeSeconds describe revocations the broker REFUSED.
var (
	SubscriberCredentialOrphans                 metric.Int64Gauge
	OldestSubscriberCredentialOrphanAgeSeconds  metric.Float64Gauge
	SubscriberRevocationFailures                metric.Int64Gauge
	OldestSubscriberRevocationFailureAgeSeconds metric.Float64Gauge
)

// SubscriberObligationsSettledTotal counts the broker-side obligations the settlement
// pass discharged.
var SubscriberObligationsSettledTotal metric.Int64Counter

// EventPublishDurationBuckets are the explicit bucket boundaries, in SECONDS, of
// EventPublishDuration.
var EventPublishDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5,
	0.75, 1, 1.5, 2, 3, 5, 10, 30,
}

// EventCaptureToDispatchDurationBuckets are the explicit bucket boundaries, in SECONDS,
// of EventCaptureToDispatchDuration.
//
// THE BOUNDARIES ABOVE 300 SECONDS ARE NOT DECORATION. This histogram answers the
// acceptance question "is the p99 age of a first-attempt publish under 2 seconds", and a
// quantile can only be interpolated inside a finite bucket: everything past the largest
// boundary lands in (largest, +Inf] and reports AS the largest boundary. When the top
// boundary was 300, a backlog whose true p50, p95 and p99 were all minutes past it read as
// exactly 300.0000 s across the board — a floor pretending to be a measurement, and one
// that would keep reading 300 whether the real answer was five minutes or five hours.
//
// The tail is coarse on purpose. A breach of a 2-second objective does not need resolution
// past its order of magnitude; it needs to be a NUMBER, distinguishable from the next
// number, so an operator can tell a slow drain from a stalled one and see recovery move.
// The boundaries below 31 seconds are unchanged, so every reading that was previously
// interpolable still interpolates identically.
var EventCaptureToDispatchDurationBuckets = []float64{
	0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 20, 31, 60, 300,
	900, 1800, 3600, 21600, 86400,
}

// EventRelayClaimDurationBuckets are the explicit bucket boundaries, in SECONDS, of
// EventRelayClaimDuration.
//
// The lower end is sub-millisecond because a healthy claim IS sub-millisecond to
// single-digit-millisecond: it reads two bounded windows of the claim-order index, walks a
// bounded number of keys, and updates at most one batch of rows. The upper end runs past
// the relay's claim budget so a claim that is cancelled at the budget still lands in a
// finite bucket rather than in the overflow.
var EventRelayClaimDurationBuckets = []float64{
	0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30,
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
		metric.WithDescription(
			"Total original ledger events whose Kafka leg is durably recorded, counted once each "+
				"by topic and event type",
		),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	EventBrokerAcknowledgementsTotal, err = meter.Int64Counter(
		"blnk.events.broker_acknowledgements.total",
		metric.WithDescription(
			"Total event publishes acknowledged by the broker, by topic, event type and purpose",
		),
		metric.WithUnit("{write}"),
	)
	if err != nil {
		return err
	}

	EventsDispatchedTotal, err = meter.Int64Counter("blnk.events.dispatched.total",
		metric.WithDescription(
			"Total ledger events whose delivery is durably recorded, counted once each by topic "+
				"and event type",
		),
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

	EventCaptureToDispatchDuration, err = meter.Float64Histogram("blnk.events.capture_to_dispatch.duration",
		metric.WithDescription(
			"End-to-end age of a published event, from its capture in the transactional outbox to broker acknowledgement",
		),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(EventCaptureToDispatchDurationBuckets...),
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

	EventRelayClaimsTotal, err = meter.Int64Counter("blnk.events.relay.claims.total",
		metric.WithDescription("Total number of event outbox claims the relay issued, by outcome: rows, empty, error or timeout"),
		metric.WithUnit("{claim}"),
	)
	if err != nil {
		return err
	}

	EventRelayClaimDuration, err = meter.Float64Histogram("blnk.events.relay.claim.duration",
		metric.WithDescription("Duration of one event outbox claim, by outcome"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(EventRelayClaimDurationBuckets...),
	)
	if err != nil {
		return err
	}

	EventRelayClaimedRows, err = meter.Int64Counter("blnk.events.relay.claimed_rows.total",
		metric.WithDescription("Total number of outbox rows the relay's claims returned, so claim size is comparable with claim count"),
		metric.WithUnit("{row}"),
	)
	if err != nil {
		return err
	}

	DLTOldestMessageAgeSeconds, err = meter.Float64Gauge("blnk.dlt.oldest_message_age_seconds",
		metric.WithDescription(
			"Seconds the oldest unresolved dead-letter entry has been waiting, over rows in the failed or "+
				"dead_lettered state, measured from the last publish attempt once the entry is preserved on "+
				"its dead-letter topic and from the first attempt while that write is still owed",
		),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	// The two ASYNCHRONOUS gauges, created before the callback that feeds them because
	// RegisterCallback takes the instruments as arguments.
	SubscriberConsumerLag, err = meter.Int64ObservableGauge("blnk.kafka.consumer_lag",
		metric.WithDescription("Number of messages a subscriber consumer group trails the log end offset by, for completely measured topics only"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return err
	}

	ConsumerLagUnmeasuredPartitions, err = meter.Int64ObservableGauge("blnk.kafka.consumer_lag_unmeasured_partitions",
		metric.WithDescription("Number of partitions whose offsets could not be read when a subscriber's lag was last measured"),
		metric.WithUnit("{partition}"),
	)
	if err != nil {
		return err
	}

	// ONE registration over BOTH instruments, so every collection cycle observes them from
	// the same inventory snapshot. The Registration handle is deliberately discarded: the
	// callback lives for the life of the process, there is no metrics shutdown path that
	// would unregister it, and holding a handle nothing ever uses would imply otherwise.
	if _, err = meter.RegisterCallback(
		observeConsumerLagInventory,
		SubscriberConsumerLag,
		ConsumerLagUnmeasuredPartitions,
	); err != nil {
		return err
	}

	// THE LAG-SWEEP COVERAGE TRIO, assigned here beside the two asynchronous lag gauges
	// they qualify. All three were DECLARED and never assigned, which is not a cosmetic
	// omission: a nil instrument panics on the first Record, so the collector tick that
	// first published a coverage reading would have taken the whole collector down — and
	// these are precisely the instruments that say the lag alert cannot fire, so the
	// failure would have removed the signal that the signal was missing.
	ConsumerLagInventoryComplete, err = meter.Int64Gauge(
		"blnk.kafka.consumer_lag_inventory_complete",
		metric.WithDescription("1 when the last lag sweep measured every registered subscriber, 0 when it did not"),
		metric.WithUnit("{status}"),
	)
	if err != nil {
		return err
	}

	SubscriberLagPassAgeSeconds, err = meter.Float64Gauge(
		"blnk.kafka.consumer_lag.pass_age_seconds",
		metric.WithDescription(
			"Age of the in-progress pass over the subscriber registry for lag measurement",
		),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	SubscriberLagCoveredSubscribers, err = meter.Int64Gauge(
		"blnk.kafka.consumer_lag.covered_subscribers",
		metric.WithDescription("Subscribers with a currently exported consumer-lag series"),
		metric.WithUnit("{subscriber}"),
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

	QueueBacklog, err = meter.Int64Gauge("blnk.queue.backlog",
		metric.WithDescription("Tasks waiting in an asynq queue, by queue and wait state"),
		metric.WithUnit("{task}"),
	)
	if err != nil {
		return err
	}

	EventRepairBacklog, err = meter.Int64Gauge("blnk.events.repair.backlog",
		metric.WithDescription("Event outbox rows a repair leg still owes, by leg"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	EventRepairsCompletedTotal, err = meter.Int64Counter("blnk.events.repair.completed.total",
		metric.WithDescription("Event outbox rows repaired to their destination, by leg"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	EventRepairSaturated, err = meter.Int64Gauge("blnk.events.repair.saturated",
		metric.WithDescription("1 when a repair pass spent its whole per-tick budget with work outstanding, by leg"),
		metric.WithUnit("{state}"),
	)
	if err != nil {
		return err
	}

	EventsPurgedTotal, err = meter.Int64Counter("blnk.events.purged.total",
		metric.WithDescription("Terminal event outbox rows deleted by the retention sweep"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	SubscriberRevocationsPending, err = meter.Int64Gauge("blnk.subscribers.revocation_pending",
		metric.WithDescription("Subscribers whose broker-side credential revocation is still owed"),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	OldestSubscriberRevocationAgeSeconds, err = meter.Float64Gauge(
		"blnk.subscribers.oldest_revocation_age_seconds",
		metric.WithDescription("Age of the oldest outstanding subscriber credential revocation"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	SubscriberCredentialOrphans, err = meter.Int64Gauge("blnk.subscribers.credential_orphans",
		metric.WithDescription(
			"Subscribers holding a Kafka credential Blnk could neither record nor revoke",
		),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	// ATTRIBUTED BY REASON, and named blnk.kafka.subscribers_unmeasured. The reason
	// decides the remediation — only 'budget' is answered by configuration — so the
	// aggregate alone would send an operator to the wrong fix. Sum the reasons away for
	// the total: sum without(reason)(blnk_kafka_subscribers_unmeasured).
	SubscribersUnmeasured, err = meter.Int64Gauge(
		"blnk.kafka.subscribers_unmeasured",
		metric.WithDescription(
			"Registered subscribers the last lag sweep did not publish a complete reading for, by reason",
		),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	SubscribersRegistered, err = meter.Int64Gauge("blnk.subscribers.registered",
		metric.WithDescription(
			"Subscribers the registry holds, so the unmeasured count can be read as a proportion "+
				"and the measurement budget's headroom is visible before it is exhausted",
		),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	SubscriberMeasurementBudget, err = meter.Int64Gauge("blnk.subscribers.measurement_budget",
		metric.WithDescription(
			"Subscribers one consumer-lag sweep may measure, as configured, so headroom is a "+
				"difference of two series rather than a literal that is wrong wherever the "+
				"budget was raised",
		),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	OldestSubscriberCredentialOrphanAgeSeconds, err = meter.Float64Gauge(
		"blnk.subscribers.oldest_credential_orphan_age_seconds",
		metric.WithDescription("Age of the oldest unaccounted subscriber Kafka credential"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	SubscriberRevocationFailures, err = meter.Int64Gauge("blnk.subscribers.revocation_failures",
		metric.WithDescription(
			"Subscribers whose most recent broker-side revocation attempt was refused",
		),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	OldestSubscriberRevocationFailureAgeSeconds, err = meter.Float64Gauge(
		"blnk.subscribers.oldest_revocation_failure_age_seconds",
		metric.WithDescription("Age of the oldest refused subscriber revocation attempt"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	// THE SETTLEMENT GAUGES. Assigned immediately before the counter that reports the
	// obligations they count down, because they are published together as one reading of
	// one aggregate — SubscriberSettlementOutstanding is the total, the two Pending gauges
	// split it by kind, and the age gauge is what the age-based alert is stated over. All
	// four were declared and never assigned.
	SubscriberSettlementOutstanding, err = meter.Int64Gauge(
		"blnk.subscribers.settlement_outstanding",
		metric.WithDescription("Subscribers owing broker-side reconciliation of either kind"),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	SubscriberGrantReconcilePending, err = meter.Int64Gauge(
		"blnk.subscribers.grant_reconcile_pending",
		metric.WithDescription("Subscribers whose broker-side ACL grant may not match the registry"),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	SubscriberCredentialCleanupPending, err = meter.Int64Gauge(
		"blnk.subscribers.credential_cleanup_pending",
		metric.WithDescription("Subscribers owing a broker-side credential cleanup"),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		return err
	}

	OldestSubscriberSettlementAgeSeconds, err = meter.Float64Gauge(
		"blnk.subscribers.oldest_settlement_age_seconds",
		metric.WithDescription("Age of the oldest outstanding subscriber settlement obligation"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	SubscriberObligationsSettledTotal, err = meter.Int64Counter(
		"blnk.subscribers.obligations_settled.total",
		metric.WithDescription("Broker-side subscriber obligations discharged by the settlement pass"),
		metric.WithUnit("{obligation}"),
	)
	if err != nil {
		return err
	}

	// THE COLLECTOR'S OWN HEALTH, last because it describes the component every gauge above
	// depends on. All three were declared and never assigned.
	EventMetricsCollectionFailuresTotal, err = meter.Int64Counter(
		"blnk.event_metrics.collection_failures.total",
		metric.WithDescription("Failures inside the periodic event-metrics collector, by which collection failed"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return err
	}

	// The collection-health pair. ASYNCHRONOUS, because the subject is a component that
	// may have STOPPED: a synchronous gauge could only be written by the collector itself
	// and would freeze at its last reading, reading as permanently fresh — which makes a
	// dead collector indistinguishable from a healthy one. Created before the callback
	// that feeds them, because RegisterCallback takes the instruments as arguments.
	EventMetricsLastCollectionAgeSeconds, err = meter.Float64ObservableGauge(
		"blnk.event_metrics.last_collection_age_seconds",
		metric.WithDescription("Seconds since the periodic event-metrics collector last finished a collection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	EventMetricsLastSuccessAgeSeconds, err = meter.Float64ObservableGauge(
		"blnk.event_metrics.last_success_age_seconds",
		metric.WithDescription("Seconds since the periodic event-metrics collector last completed a collection with no failures"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	// ONE registration over BOTH, so every collection cycle observes them from the same
	// snapshot and the pair can never describe two different ticks. The Registration
	// handle is discarded for the reason the consumer-lag one is: the callback lives for
	// the life of the process and there is no metrics shutdown path that would unregister
	// it.
	if _, err = meter.RegisterCallback(
		observeEventMetricsCollectionAges,
		EventMetricsLastCollectionAgeSeconds,
		EventMetricsLastSuccessAgeSeconds,
	); err != nil {
		return err
	}

	return nil
}
