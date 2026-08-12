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

// EventsPublishedTotal counts ORIGINAL LEDGER EVENTS ONCE EACH, at the moment their
// Kafka leg becomes durably recorded — the broker acknowledged the write AND the
// claim-token-conditional statement that records that leg has committed.
//
// Delivery is at-least-once by construction: the relay publishes, then records the leg,
// and a crash or a failed statement between those two steps deliberately leaves the row
// claimable so the event is published AGAIN — losing an event is unrecoverable while a
// duplicate is suppressed at the subscriber on event_id. Incremented at the
// acknowledgement, that republish counted the same event twice, so a counter documented
// as one-per-event silently became one-per-successful-write exactly when the pipeline
// was struggling.
//
// The transitions that record the Kafka leg — MarkEventDispatched and
// MarkEventWebhookPending — are both conditional on the claim token and both clear or
// consume it, so each succeeds for one worker once in an event's whole life. Counting
// there makes the increment unique by construction.
//
// It counts ORIGINAL publishes only. A replay and a dead-letter write are separate
// purposes, visible on EventBrokerAcknowledgementsTotal under their own purpose
// attribute and on EventPublishAttemptsTotal and EventPublishDuration under their own
// fixed attempt attribute, which is where re-delivery belongs.
var EventsPublishedTotal metric.Int64Counter

// EventBrokerAcknowledgementsTotal counts publishes the BROKER ACKNOWLEDGED, whether or
// not the outbox row was subsequently marked.
//
// It exists because EventsPublishedTotal is incremented on the durable transition,
// which leaves the wire signal unrepresented. The two are deliberately different
// measurements and the gap between them is the diagnostic:
//
//   - ACKS ≈ PUBLISHED is the healthy steady state.
//   - ACKS > PUBLISHED means events are reaching Kafka but their rows are not being
//     marked — a database fault, a lost claim, or a relay dying between the two steps.
//   - PUBLISHED > ACKS cannot happen and indicates an instrumentation defect.
//
// Unlike EventsPublishedTotal it counts EVERY purpose — original publishes, replays and
// dead-letter writes — because it measures traffic the broker accepted rather than
// first deliveries of business events. Do not substitute it for EventsPublishedTotal in
// the dead-letter rate: it would count a dead-lettered event on both sides of the
// ratio.
//
// Attributes: topic, event_type, purpose
var EventBrokerAcknowledgementsTotal metric.Int64Counter

// EventsDispatchedTotal counts ledger events ONCE EACH, at the moment their delivery
// becomes durably recorded: the outbox row reaches the dispatched state under the claim
// token that authorised the publish.
//
// TWO statements reach that state and both increment here, which is what makes the
// count complete rather than merely unique:
//
//   - MarkEventDispatched, the ordinary arm — the Kafka write is acknowledged and the
//     legacy webhook leg is done or was never owed.
//   - MarkEventWebhookPending's ABANDON arm — the Kafka write is acknowledged and the
//     legacy leg has exhausted its own enqueue budget, so the row is moved to
//     dispatched with the webhook given up on.
//
// Both are per-event and both are incremented after a claim-token-conditional
// transition, so neither can double-count. They differ in WHICH transition, and
// therefore in WHEN:
//
//   - EventsPublishedTotal counts the KAFKA LEG becoming durable, including for a row
//     that then waits in webhook_pending for its legacy leg.
//   - This one counts the row reaching the TERMINAL dispatched state, so for a row that
//     owes a legacy webhook it lags by that leg's remaining budget.
var EventsDispatchedTotal metric.Int64Counter

// EventPublishAttemptsTotal counts individual relay publish attempts, retries included,
// so retry pressure stays visible independently of how many events were delivered.
//
// The outcome vocabulary is CLOSED AT THREE VALUES and mutually exclusive — exactly one
// value is recorded per attempted write, so summing the series yields the total number
// of attempts:
//
//   - dispatched: the broker acknowledged the write.
//   - retrying: the attempt did not deliver the event. This is the SINGLE failure
//     outcome, whether or not another attempt will follow.
//   - dead_lettered: an acknowledged write to a `<topic>.dlt` sibling, which is the
//     terminal outcome of the ORIGINAL event.
//
// A fourth outcome naming "no further attempt is possible" belongs on the `terminal`
// dimension below rather than here: it would widen the three-value publish-status
// vocabulary that model.PublishStatus, the API responses and docs/event-streaming.md all
// state, and a subscriber or dashboard reading the documented three values against a
// series carrying four gets an outcome it has no case for.
//
// Attributes:
//   - outcome: dispatched, retrying, dead_lettered.
//   - terminal: true, false. Describes FAILURES: true on a retrying attempt means no
//     further attempt will be made, because the failure is permanent or the budget is
//     spent.
var EventPublishAttemptsTotal metric.Int64Counter

// EventPublishDuration records how long a single publish took, measured from the moment
// the pipeline started working on the event — the outbox claim for a relay publish, the
// request instant for a replay — to broker acknowledgement.
//
//	histogram_quantile(0.99, sum by (le) (rate(
//	  blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))
//
// Attributes:
//   - topic: the destination topic.
//   - attempt: 1, 2, 3, 4, 5, over, replay, dead_letter.
//   - outcome: dispatched, retrying, dead_lettered — the same three-value vocabulary
//     EventPublishAttemptsTotal carries.
var EventPublishDuration metric.Float64Histogram

// EventCaptureToDispatchDuration records the END-TO-END age of a published event: the
// interval from the instant the event was CAPTURED in the transactional outbox, inside
// the ledger transaction that produced it, to the instant the broker acknowledged its
// publish.
//
// The end-to-end publish-latency objective is read from this instrument:
//
//	histogram_quantile(0.99, sum by (le) (rate(
//	  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))
//
// Attributes:
//   - topic: the destination topic, bounded to the Blnk-owned namespace.
//   - attempt: 1, 2, 3, 4, 5, over, replay — the same closed domain
//     EventPublishDuration uses, minus dead_letter, which is never an end-to-end
//     delivery.
var EventCaptureToDispatchDuration metric.Float64Histogram

// EventsDeadLetteredTotal counts ledger events diverted to a dead-letter topic after the
// relay exhausted its retry budget. The topic attribute carries the original category
// topic, not the .dlt sibling, so it is comparable with EventsDispatchedTotal — the two
// partition the terminal outcomes of a captured event, and their sum is the population the
// dead-letter rate is a fraction of.
// Attributes: topic, event_type
var EventsDeadLetteredTotal metric.Int64Counter

// EVERY GAUGE BELOW is maintained by ONE production caller, the periodic
// EventMetricsCollector in event_metrics.go, and by nothing else — the dead-letter age,
// the consumer-lag pair and its coverage gauges, the outbox backlog, the registry size,
// and the revocation, orphan and settlement gauges. The set has grown as the subscriber
// lifecycle did, so it is named by its OWNER rather than by a count that would drift
// the next time one is added; what matters is that no other caller writes any of them.
// That single ownership is a correctness requirement rather than tidiness, for two
// reasons that apply to every gauge in an exporter and to these in particular.
//
// A gauge RETAINS ITS LAST VALUE until it is written again or the process restarts, so
// a value recorded once at the moment something went wrong keeps alerting long after
// the condition has cleared. Each of these is therefore re-recorded on every collector
// tick from authoritative state, including an explicit ZERO when there is nothing
// outstanding: zero is the measurement that clears the alert, and omitting it is what
// leaves a stale one firing.
//
// A gauge written only from the code path that CAUSES the condition also measures the
// wrong thing. Recording the dead-letter age where an event is dead-lettered would
// report the age of something that just happened — always near zero, so the "stuck for
// 15 minutes" alert could never fire while looking healthy throughout.

// DLTOldestMessageAgeSeconds is the age, in seconds, of the oldest UNRESOLVED
// dead-letter entry destined for each dead-letter topic. Alerting fires above 900s.
//
// It counts outbox rows in EITHER of two states, and the second is the one that is easy
// to overlook:
//
//   - dead_lettered — the dead-letter message was written and acknowledged, and the
//     entry is waiting for an operator.
//   - failed — the retry budget is spent but the dead-letter WRITE ITSELF has not
//     completed, so there may be no message on the topic at all.
//
// Including the failed state is what makes the gauge honest. An event whose dead-letter
// write fails is the most stranded an event can be — out of the relay's claimable set
// with nothing on any topic behind it — and a gauge scoped to messages actually on the
// topic would have reported that as perfectly healthy.
//
// last_attempted_at, which is the moment the event was given up on and therefore the
// moment it started waiting. There is NO dead_lettered_at column; when
// last_attempted_at is absent the row's occurred_at is used instead, which is older and
// so errs toward reporting a problem rather than hiding one.
//
// The authoritative implementation is RefreshDeadLetterAgeGauge in event_dlt.go.
var DLTOldestMessageAgeSeconds metric.Float64Gauge

// SubscriberConsumerLag is how many messages a subscriber's consumer group trails the
// log end offset by, computed in-process by differencing committed offsets against end
// offsets. Alerting fires above 10000 messages.
//
// Its three attributes are the only DYNAMIC label set among the event instruments —
// every other instrument's attributes come from closed vocabularies — and that
// difference is what makes the instrument kind matter.
var SubscriberConsumerLag metric.Int64ObservableGauge

// ConsumerLagUnmeasuredPartitions is how many of a topic's partitions could not be read
// when its lag was last measured. Zero is the healthy reading and is reported
// explicitly.
//
// It shares SubscriberConsumerLag's inventory and callback, so the two are always
// observed from the same snapshot and cannot disagree about which topics were measured,
// and it inherits the same current-inventory cardinality bound.
var ConsumerLagUnmeasuredPartitions metric.Int64ObservableGauge

// SubscriberLagPassAgeSeconds is how long the IN-PROGRESS pass over the subscriber
// registry has been running, and SubscriberLagCoveredSubscribers is how many
// subscribers currently have an exported lag series. Together they are the COVERAGE
// statement for consumer-lag measurement.
//
// The pass age resets to zero each time a rotation completes, so its MAXIMUM over a
// window is the rotation latency:
var (
	SubscriberLagPassAgeSeconds     metric.Float64Gauge
	SubscriberLagCoveredSubscribers metric.Int64Gauge
)

// ConsumerLagSample is one topic's entry in the consumer-lag inventory.
//
// It is a value type with no methods so that a snapshot is trivially copyable and cannot be
// mutated through a shared pointer after it has been published.
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
//
// It is guarded rather than atomic-swapped through an interface value because the read
// side runs inside a collection callback that the SDK may invoke concurrently with a
// publish, and a mutex makes the whole-slice replacement obviously indivisible. The
// lock is held only for a slice-header assignment on the write side and for the
// iteration on the read side, both of which are bounded by the subscriber budget.
var consumerLagInventory struct {
	mu      sync.RWMutex
	samples []ConsumerLagSample
}

// PublishConsumerLagInventory replaces the consumer-lag inventory the asynchronous
// gauges report.
//
// Passing an empty or nil slice is meaningful and correct: it says nothing is currently
// measurable — no registry, no broker, or no subscribers — and it retires every series.
//
// The slice is COPIED, so a caller may reuse or mutate its buffer afterwards without
// rewriting live telemetry.
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
// It exists so the publication can be asserted without a metric reader, and so an
// operator endpoint could report what is being exported. The result is a copy for the
// same reason PublishConsumerLagInventory copies.
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
//
// It never returns an error: there is no failure mode in reading a slice, and returning
// one would only cause the SDK to log. An empty inventory legitimately observes
// nothing.
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
//
// It sits near zero and never exceeds the collection interval plus one collection's
// duration (15 seconds plus the tick budget by default). A value materially above that
// means the loop is not ticking.
//
// Attributes: none
var EventMetricsLastCollectionAgeSeconds metric.Float64ObservableGauge

// EventMetricsLastSuccessAgeSeconds is how long ago the periodic event-metrics
// collector last completed a collection with NO failures.
//
// It is the second half of the collection-health pair, and the two answer different
// questions. EventMetricsLastCollectionAgeSeconds says whether the loop is running;
// this says whether it is achieving anything.
//
// Attributes: none
var EventMetricsLastSuccessAgeSeconds metric.Float64ObservableGauge

// EventMetricsCollectionFailuresTotal counts individual collection failures inside the
// periodic event-metrics collector, attributed by WHICH collection failed.
//
// The attribute domain is CLOSED at the five collections plus the registry enumeration,
// all fixed literals declared in event_metrics.go. No error text and no identifier ever
// reaches it.
//
// Attributes: collection
var EventMetricsCollectionFailuresTotal metric.Int64Counter

// ConsumerLagInventoryComplete reports whether the last lag sweep measured EVERY
// registered subscriber: 1 for yes, 0 for no.
//
// So completeness is published as its own fact. Zero means at least one registered
// subscriber's lag is not being measured, whatever the reason — budget exhausted,
// registry unreadable, or a measurement that failed — and
// SubscriberLagCoverageIncomplete fires on it.
var ConsumerLagInventoryComplete metric.Int64Gauge

// SubscribersUnmeasured is how many registered subscribers the last sweep did not
// publish a complete lag reading for, attributed by why.
//
// It is the quantity behind ConsumerLagInventoryComplete, and it is written for EVERY
// reason on every tick — zeros included — because a reason that disappears from the
// export is indistinguishable from a reason that has been resolved.
//
// The reason vocabulary is closed and each value means something an operator would act
// on differently:
//
//   - budget: the sweep's per-tick budget was reached, so these subscribers were not
//     examined.
//   - unprovisioned: the row carries no authorised topics, or identifiers the registry
//     did not issue.
//   - measure_failed: the broker refused the measurement for this subscriber.
//   - registry_failed: the registry enumeration itself failed, so an unknown number of
//     subscribers were never reached.
//   - topic_missing: the measurement succeeded but named at least one authorised topic
//     that does not exist at the broker, so that topic contributes no lag and no
//     partition health.
var SubscribersUnmeasured metric.Int64Gauge

// SubscribersRegistered is how many subscribers the registry holds.
//
// SubscribersUnmeasured above says how many subscribers have NO consumer-lag series,
// which is the worst property a monitoring system can have — silence and health are
// indistinguishable, because SubscriberConsumerLagHigh has nothing to evaluate for a
// subscriber that was never measured. That count alone cannot be judged: three
// unmeasured out of five is a different situation from three out of three thousand.
var SubscribersRegistered metric.Int64Gauge

// SubscriberMeasurementBudget is how many subscribers one consumer-lag sweep may
// measure.
//
// 200 - blnk_subscribers_registered
//
// So the CONFIGURED value is published, from the same call as the registry size and on
// every tick, and the portable query is a difference of two series:
//
// blnk_subscribers_measurement_budget - blnk_subscribers_registered
var SubscriberMeasurementBudget metric.Int64Gauge

// The closed vocabulary of SubscribersUnmeasured's "reason" attribute.
//
// Declared here, beside the instrument, rather than in the collector that writes them: the
// attribute domain is part of the instrument's contract, and a producer that minted its own
// value would widen a gauge's series set without anything failing. See SubscribersUnmeasured
// for what each one means operationally.
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
	}
}

// eventMetricsCollectionHealth holds the timestamps the asynchronous collection-health
// gauges are observed from.
//
// firstAttempt closes that: until a clean collection happens, the success age is
// reported from when the collector STARTED collecting, which is the honest answer to
// "how long has it been since this was last known good". It is set once and never
// moves, so the age rises monotonically for as long as the condition holds.
var eventMetricsCollectionHealth struct {
	mu           sync.RWMutex
	firstAttempt time.Time
	lastAttempt  time.Time
	lastSuccess  time.Time
}

// RecordEventMetricsCollection records that a collection finished, and whether it was
// complete.
//
// It is called once per tick by the periodic collector and by nothing else. Both
// timestamps are written under one lock so the pair the gauges observe is always
// self-consistent.
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
// It exists so the recording can be asserted without a metric reader, and so an
// operational endpoint could report collection freshness directly.
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
//
// ONE callback over both, from ONE snapshot, so the pair always describes the same tick
// — see eventMetricsCollectionHealth. A future timestamp is clamped to zero rather than
// reported as a negative age.
//
// It never returns an error: reading three timestamps has no failure mode.
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
//
// The floor is not defensive dressing: `at` is stamped by this process and `now` is
// read at collection time, so a clock adjustment between the two produces a negative
// interval, and a negative age on a freshness gauge would read as the freshest possible
// value — the exact opposite of the truth — and would satisfy every threshold rule
// written over it.
//
// Parameters:
//   - now time.Time: the reference instant.
//   - at time.Time: the instant being aged.
//
// Returns:
//   - float64: the age in seconds, never below zero.
func nonNegativeAge(now, at time.Time) float64 {
	age := now.Sub(at).Seconds()
	if age < 0 {
		return 0
	}

	return age
}

// OutboxPendingBacklog is the number of event outbox rows still waiting to be published
// to Kafka: the relay's backlog, counted as pending plus processing.
//
// Processing rows are INCLUDED deliberately. A row in that state has been claimed by a
// relay instance but not yet acknowledged by the broker, so it is still un-published
// work; counting only pending rows would report a drained backlog at exactly the moment
// a stalled relay is holding every claimable row under a lease.
var OutboxPendingBacklog metric.Int64Gauge

// The three REPAIR instruments. Together they answer the only two questions worth
// asking about a recovery in progress: how much is owed, and how fast it is being paid.
//
// Two populations of outbox row are outside the publish claim's reach by design and are
// reached by their own passes each relay tick:
//
//   - leg="dead_letter" — a row whose retry budget is spent and whose `<topic>.dlt`
//     write failed.
//   - leg="legacy_webhook" — a row whose Kafka leg finished and whose legacy webhook
//     enqueue never succeeded.
//
// blnk_events_repair_backlog{leg="dead_letter"} -- rows still owed
// rate(blnk_events_repair_completed_total{leg="dead_letter"}[5m]) -- the DRAIN RATE,
// rows/sec backlog / drain rate -- seconds to clear
//
// A non-zero backlog with a zero drain rate is the condition to act on: the pass is
// claiming nothing or every write is failing. A non-zero backlog with a healthy drain
// rate and blnk_events_repair_saturated at 1 is the other one: the per-tick bound is
// the binding constraint, and RELAY_REPAIR_MAX_BATCHES_PER_TICK or
// RELAY_REPAIR_BATCH_SIZE is the knob.

// EventRepairBacklog is how many rows each repair leg still owes.
//
// Published by the event-metrics collector from the per-status counts it ALREADY reads
// for blnk.outbox.pending, so it costs no additional query: the dead-letter leg is the
// `failed` count and the legacy leg is the `webhook_pending` count. Attributed by leg,
// and recorded including zero — a cleared backlog must publish 0 rather than leave the
// previous reading standing in the exporter.
var EventRepairBacklog metric.Int64Gauge

// EventRepairsCompletedTotal counts rows a repair pass actually repaired, attributed by
// leg.
var EventRepairsCompletedTotal metric.Int64Counter

// EventRepairSaturated reports whether the per-tick chaining bound stopped a repair
// pass with work still outstanding: 1 when the last tick used its whole budget, 0 when
// it drained.
var EventRepairSaturated metric.Int64Gauge

// EventsPurgedTotal counts event outbox rows DELETED by the retention sweep.
//
// It is the observability half of a destructive operation, and it exists because the
// alternative is a retention job whose two failure modes are both silent. A sweep that
// has stopped running leaves this counter flat while the table grows without bound — an
// ever-growing second copy of the ledger's most sensitive data, which is the outcome
// retention exists to prevent.
//
// A COUNTER rather than a gauge, so a rate over it answers "is retention keeping up
// with the arrival rate", and so a restart cannot be mistaken for a sweep that deleted
// nothing.
//
// Attributes: none. The rows deleted are terminal events of every type and category,
// and attributing by type would put unbounded-cardinality event names on a counter
// whose only question is how much was removed.
var EventsPurgedTotal metric.Int64Counter

// SubscriberRevocationsPending is the number of subscribers whose broker-side
// credential revocation is still OWED, and OldestSubscriberRevocationAgeSeconds is how
// long the oldest of them has been outstanding.
//
// These two gauges are what make it actionable. The COUNT answers "is anything
// outstanding", which is normally zero and any non-zero value is worth a look.
var (
	SubscriberRevocationsPending         metric.Int64Gauge
	OldestSubscriberRevocationAgeSeconds metric.Float64Gauge
)

// SubscriberSettlementOutstanding is how many subscribers owe broker-side
// reconciliation, SubscriberGrantReconcilePending and
// SubscriberCredentialCleanupPending split that by kind, and
// OldestSubscriberSettlementAgeSeconds is how long the oldest of them has been
// outstanding.
//
// The total is NOT the sum of the two, because one subscriber can owe both.
var (
	SubscriberSettlementOutstanding      metric.Int64Gauge
	SubscriberGrantReconcilePending      metric.Int64Gauge
	SubscriberCredentialCleanupPending   metric.Int64Gauge
	OldestSubscriberSettlementAgeSeconds metric.Float64Gauge
)

// SubscriberCredentialOrphans and OldestSubscriberCredentialOrphanAgeSeconds describe
// credentials that outlived their registry record; SubscriberRevocationFailures and
// OldestSubscriberRevocationFailureAgeSeconds describe revocations the broker REFUSED.
//
// The revocation gauges above cannot see either state: they count rows carrying the
// revocation tombstone, which a DEREGISTRATION stamps, while an orphaned credential is
// created by a failed ISSUANCE that never stamps it. The two states also demand
// opposite remedies — an orphan is settled by re-issuing or deprovisioning, a refused
// revocation by retrying the deregistration once the broker's refusal is understood —
// so they are reported separately rather than folded together.
//
// The orphan age is the alertable quantity, measured from when the exposure was FIRST
// recorded and never reset by a later attempt, because it measures how long a
// credential nobody accounts for has been able to authenticate.
//
// GAUGES, because each is a point-in-time fact about outstanding work that legitimately
// returns to zero. Zero is published explicitly rather than left stale, and NOTHING is
// published when the read fails — four zeroes on a failed read would assert that every
// credential is accounted for.
var (
	SubscriberCredentialOrphans                 metric.Int64Gauge
	OldestSubscriberCredentialOrphanAgeSeconds  metric.Float64Gauge
	SubscriberRevocationFailures                metric.Int64Gauge
	OldestSubscriberRevocationFailureAgeSeconds metric.Float64Gauge
)

// SubscriberObligationsSettledTotal counts the broker-side obligations the settlement
// pass discharged.
//
// A COUNTER rather than a gauge, so a restart cannot be mistaken for a pass that
// settled nothing.
//
// Attributes: none, for the cardinality reason above.
var SubscriberObligationsSettledTotal metric.Int64Counter

// EventPublishDurationBuckets are the explicit bucket boundaries, in SECONDS, of
// EventPublishDuration.
//
// The boundaries below are dense either side of the values that decide the target and
// sparse beyond it:
//
//   - 5 ms to 500 ms covers a healthy local or same-zone broker, so normal p50/p90/p99
//     movement is visible rather than flattened into one bucket.
//   - 1 s, 1.5 s and 2 s bracket the target itself. 2 s is a boundary EXACTLY because
//     the requirement is stated at two seconds: with a bucket edge there, "is p99 under
//     2 s" is answered by bucket counts alone and needs no interpolation across the
//     threshold, which is the one reading that must not be an estimate.
//   - 3 s to 30 s keeps a degraded broker on the scale instead of dumping it into +Inf,
//     where it would be invisible. 30 s is the retry backoff cap and the last edge, so
//     anything slower than the whole retry budget lands in +Inf, which is the correct
//     place for it.
var EventPublishDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5,
	0.75, 1, 1.5, 2, 3, 5, 10, 30,
}

// EventCaptureToDispatchDurationBuckets are the explicit bucket boundaries, in SECONDS,
// of EventCaptureToDispatchDuration.
//
//   - 0.05 s to 0.5 s is the sub-poll-interval region, reachable when a row is captured
//     just before a tick.
//   - 1 s, 1.5 s and 2 s bracket the acceptance target. 2 s is an edge EXACTLY because
//     the requirement is stated at two seconds: "is p99 under 2 s" is then answered
//     from bucket counts alone, with no interpolation across the threshold.
//   - 3 s to 31 s covers a row that needed retries; 31 s is the whole configured retry
//     schedule (1 + 2 + 4 + 8 + 16), so an event that spent its entire budget lands on
//     an edge rather than being blended into a neighbouring bucket.
//   - 60 s and 300 s keep a genuinely backlogged pipeline on the scale.
var EventCaptureToDispatchDurationBuckets = []float64{
	0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 20, 31, 60, 300,
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

	DLTOldestMessageAgeSeconds, err = meter.Float64Gauge("blnk.dlt.oldest_message_age_seconds",
		metric.WithDescription("Seconds the oldest unresolved dead-letter entry has been waiting, over rows in the failed or dead_lettered state, measured from the last publish attempt"),
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
