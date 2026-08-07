package metrics

import (
	"context"
	"log"
	"sync"

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

// EventsPublishedTotal counts ledger events whose DELIVERY IS DURABLY RECORDED: the
// broker acknowledged the write AND the event's outbox row was moved to the dispatched
// state under the claim token that authorised the publish.
//
// # Why the durable transition and not the broker acknowledgement
//
// Delivery is at-least-once by construction. The relay publishes, then marks the row
// dispatched, and a crash or a database failure between those two steps deliberately
// leaves the row claimable so the event is published AGAIN — losing it is unrecoverable
// while a duplicate is suppressed at the subscriber on event_id. Counted at the
// acknowledgement, that second publish increments this counter a second time for one
// event, so a counter documented as "one per event" silently becomes "one per successful
// write" exactly when the pipeline is having trouble. The dispatched transition is
// conditional on the claim token and clears it, so it succeeds for one worker once per
// event: counting there is what makes the increment unique.
//
// The consequence to know when reading this counter: it lags the wire by the duration of
// one bookkeeping statement, and an event that reached Kafka but whose row could not be
// marked is not counted until the republish completes. That is the correct trade — the
// alternative over-counts.
//
// It counts ORIGINAL events only — one increment per event that reached its category
// topic. Replays and dead-letter writes are deliberately excluded so this counter stays a
// count of first deliveries; both are visible on EventPublishAttemptsTotal and
// EventPublishDuration under their own fixed attempt attribute, which is where
// re-delivery belongs.
//
// # The dead-letter rate
//
// The rate acceptance criterion V-3 is stated against is
// EventsDeadLetteredTotal / (EventsDeadLetteredTotal + EventsPublishedTotal). The two
// counters partition the TERMINAL outcomes of a captured event — an event is either
// delivered or dead-lettered, never both — so their sum is the population and this
// counter alone is not. Dividing by this counter alone would report dead-letters as a
// fraction of successes, which exceeds the true rate and diverges without bound as the
// failure rate rises (every event failing gives a denominator of zero).
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

// EventCaptureToDispatchDuration records the END-TO-END age of a published event: the
// interval from the instant the event was CAPTURED in the transactional outbox, inside the
// ledger transaction that produced it, to the instant the broker acknowledged its publish.
//
// # Why this exists alongside EventPublishDuration
//
// The sub-two-second p99 target in acceptance criterion V-1 is stated for
// outbox-to-Kafka latency, and EventPublishDuration cannot answer it: its clock starts
// when the relay's claim query RETURNS, so it excludes the three intervals that dominate
// the wait — the row sitting pending until the next poll tick, the poll interval itself,
// and the claim query's own latency. A pipeline whose relay was stalled for a minute would
// still report a five-millisecond publish, so the figure would look healthy precisely when
// subscribers were an eternity behind. Both instruments are kept because they answer
// different questions: this one is the SUBSCRIBER'S wait, and EventPublishDuration is the
// broker write in isolation, which is what tells an operator whether a slow end-to-end
// figure is the broker or the backlog.
//
// V-1 is therefore read from this instrument:
//
//	histogram_quantile(0.99, sum by (le) (rate(
//	  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))
//
// # What the measurement is, honestly stated
//
// The capture instant is the outbox row's persisted occurred_at, stamped by the process
// that captured the event; the acknowledgement instant is read from the relay's clock. The
// two therefore come from different processes, so the figure carries whatever clock skew
// exists between them. Within one NTP-synchronised deployment that term is milliseconds
// against a two-second target, and no alternative avoids it: any measurement that spans
// the durable handover has to span two clocks. A reading that would be NEGATIVE — a
// capture stamped in the relay's future — is not recorded at all rather than clamped to
// zero, because a zero is indistinguishable from a genuinely instant publish and would
// quietly improve the very quantile the criterion is read from.
//
// Only ACKNOWLEDGED publishes are recorded. A failed attempt has no end-to-end latency to
// report — the event has not arrived — and including one would credit the histogram with a
// short duration for an event that is still waiting. Retries are recorded under their own
// attempt number, so the first-attempt population the target is stated over stays
// selectable, and a republished event legitimately contributes a second, longer
// observation: both writes really did take that long from capture.
//
// Attributes:
//   - topic: the destination topic, bounded to the Blnk-owned namespace.
//   - attempt: 1, 2, 3, 4, 5, over, replay — the same closed domain
//     EventPublishDuration uses, minus dead_letter, which is never an end-to-end delivery.
var EventCaptureToDispatchDuration metric.Float64Histogram

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

// DLTOldestMessageAgeSeconds is the age, in seconds, of the oldest UNRESOLVED dead-letter
// entry destined for each dead-letter topic. Alerting fires above 900s.
//
// # The population is wider than "messages on the topic", deliberately
//
// It counts outbox rows in EITHER of two states, and the second is the one that is easy to
// overlook:
//
//   - dead_lettered — the dead-letter message was written and acknowledged, and the entry is
//     waiting for an operator. This one is replayable.
//   - failed — the retry budget is spent but the dead-letter WRITE ITSELF has not completed,
//     so there may be no message on the topic at all. This one is NOT replayable yet; it
//     needs the dead-letter write to succeed first.
//
// Including the failed state is what makes the gauge honest. An event whose dead-letter write
// fails is the most stranded an event can be — out of the relay's claimable set with nothing
// on any topic behind it — and a gauge scoped to messages actually on the topic would have
// reported that as perfectly healthy. The consequence for the alert rule is that a firing
// alert does not imply a replayable entry, which is why the rule's remediation is stated in
// two steps rather than one.
//
// # What the age is measured FROM
//
// last_attempted_at, which is the moment the event was given up on and therefore the moment
// it started waiting. There is NO dead_lettered_at column; when last_attempted_at is absent
// the row's occurred_at is used instead, which is older and so errs toward reporting a problem
// rather than hiding one. A negative age — a future-dated row, or clock skew — is reported as
// zero rather than allowed to lower the maximum.
//
// # Coverage and its one bound
//
// Every dead-letter topic Blnk owns is recorded on every collector tick, ZERO INCLUDED: zero
// is the reading that clears the alert, and omitting it would leave a resolved incident
// alerting for ever. The walk is bounded at a fixed row limit, so an inventory larger than
// that limit yields a LOWER BOUND drawn from the oldest entries examined — which is safe for a
// threshold alert, since a lower bound above the threshold still fires, and the truncation is
// logged rather than silently swallowed.
//
// The topic attribute is the `.dlt` sibling name, not the original category topic, because
// this measures what is waiting on a dead-letter topic. That is the opposite convention from
// EventsDeadLetteredTotal, which carries the ORIGINAL topic so it stays comparable with
// EventsPublishedTotal, and the difference is deliberate in both cases.
//
// The authoritative implementation is RefreshDeadLetterAgeGauge in event_dlt.go.
//
// Attributes: topic (the `<category>.dlt` name)
var DLTOldestMessageAgeSeconds metric.Float64Gauge

// SubscriberConsumerLag is how many messages a subscriber's consumer group trails the log
// end offset by, computed in-process by differencing committed offsets against end offsets.
// Alerting fires above 10000 messages.
//
// # Why this one is ASYNCHRONOUS when the other gauges are synchronous
//
// Its three attributes are the only DYNAMIC label set among the event instruments — every
// other instrument's attributes come from closed vocabularies — and that difference is what
// makes the instrument kind matter.
//
// A synchronous gauge is a WRITE: the SDK's last-value aggregator remembers every attribute
// set ever written to it and keeps exporting each one until the process restarts. There is no
// delete in the synchronous gauge API, so a subscriber that is deregistered, has its grant
// narrowed or has its consumer group reissued leaves its label tuple behind permanently. The
// previous implementation wrote a ZERO to such a tuple, which correctly stopped it alerting
// but did nothing about the retention: the series still existed, still occupied memory in the
// SDK, and was still scraped and stored for ever. Subscriber churn therefore grew the series
// count and the process's memory without bound and with no ceiling anywhere — no request rate
// to throttle, and no SDK cardinality limit configured.
//
// An asynchronous gauge inverts the ownership. It is a READ: the SDK invokes a callback at
// collection time and exports exactly the attribute sets that callback observes, and nothing
// else. A subscriber that leaves the registry simply stops being observed, so its series stops
// being exported — no zero to write, nothing retained, and the series count is by construction
// the size of the CURRENT INVENTORY rather than the history of every inventory there has ever
// been. That is the property the cardinality bound needs, and it cannot be had from a
// synchronous gauge at all.
//
// The inventory is supplied by PublishConsumerLagInventory, which the periodic collector calls
// once per tick with the complete measured set. See ConsumerLagSample for what one entry is
// and for why an incompletely measured topic contributes no lag reading.
//
// Attributes: subscriber, group, topic
var SubscriberConsumerLag metric.Int64ObservableGauge

// ConsumerLagUnmeasuredPartitions is how many of a topic's partitions could not be read when
// its lag was last measured. Zero is the healthy reading and is reported explicitly.
//
// # Why measurement health is a separate signal
//
// Lag is a sum over partitions, so a partition whose offsets the broker will not report
// contributes nothing and the total silently becomes a LOWER BOUND. Publishing that lower
// bound as the lag is worse than publishing nothing: a subscriber genuinely 50,000 messages
// behind on six partitions reads as 8,000 behind when five of them go unreadable, which is
// under the alert threshold — so a broker or metadata fault does not merely blind the
// measurement, it actively RESOLVES a firing alert and reports the system as healthy at the
// moment it is least able to tell.
//
// So the two facts are separated. SubscriberConsumerLag carries only complete measurements and
// is what the >10000 rule reads, and this instrument carries the count of partitions that
// could not be measured, with its own rule firing on any non-zero value. Degraded measurement
// then surfaces as degraded measurement rather than as good news. The partial total is still
// returned to the caller and logged for diagnosis; it is simply not published as a number an
// alert is allowed to trust.
//
// It shares SubscriberConsumerLag's inventory and callback, so the two are always observed
// from the same snapshot and cannot disagree about which topics were measured, and it inherits
// the same current-inventory cardinality bound.
//
// Attributes: subscriber, group, topic
var ConsumerLagUnmeasuredPartitions metric.Int64ObservableGauge

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
// It is guarded rather than atomic-swapped through an interface value because the read side
// runs inside a collection callback that the SDK may invoke concurrently with a publish, and
// a mutex makes the whole-slice replacement obviously indivisible. The lock is held only for
// a slice-header assignment on the write side and for the iteration on the read side, both of
// which are bounded by the subscriber budget.
var consumerLagInventory struct {
	mu      sync.RWMutex
	samples []ConsumerLagSample
}

// PublishConsumerLagInventory replaces the consumer-lag inventory the asynchronous gauges
// report.
//
// REPLACEMENT, not merging, is the entire point. The published set becomes the complete set
// of series exported until the next call, so a subscriber absent from it stops being exported
// rather than lingering at its last reading — which is what bounds the series count to the
// current inventory instead of the union of every inventory the process has ever seen. A
// caller must therefore pass the WHOLE measured set on every tick, never a delta.
//
// Passing an empty or nil slice is meaningful and correct: it says nothing is currently
// measurable — no registry, no broker, or no subscribers — and it retires every series.
//
// The slice is COPIED, so a caller may reuse or mutate its buffer afterwards without
// rewriting live telemetry.
//
// Parameters:
//   - samples []ConsumerLagSample: the complete current inventory, with label values already
//     resolved to their bounded vocabularies.
func PublishConsumerLagInventory(samples []ConsumerLagSample) {
	copied := make([]ConsumerLagSample, len(samples))
	copy(copied, samples)

	consumerLagInventory.mu.Lock()
	consumerLagInventory.samples = copied
	consumerLagInventory.mu.Unlock()
}

// ConsumerLagInventory returns the currently published inventory.
//
// It exists so the publication can be asserted without a metric reader, and so an operator
// endpoint could report what is being exported. The result is a copy for the same reason
// PublishConsumerLagInventory copies.
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

// observeConsumerLagInventory is the callback both asynchronous gauges are registered with.
//
// ONE callback for both instruments, rather than one each, so that a single collection cycle
// always observes both from the SAME snapshot. Two callbacks could straddle a publish and
// report a lag for a topic whose unmeasured-partition count came from a different tick, which
// is precisely the kind of inconsistency the separation of the two signals exists to avoid.
//
// It never returns an error: there is no failure mode in reading a slice, and returning one
// would only cause the SDK to log. An empty inventory legitimately observes nothing.
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

// OutboxPendingBacklog is the number of event outbox rows still waiting to be published
// to Kafka: the relay's backlog, counted as pending plus processing.
//
// Processing rows are INCLUDED deliberately. A row in that state has been claimed by a
// relay instance but not yet acknowledged by the broker, so it is still un-published work;
// counting only pending rows would report a drained backlog at exactly the moment a stalled
// relay is holding every claimable row under a lease.
var OutboxPendingBacklog metric.Int64Gauge

// EventsPurgedTotal counts event outbox rows DELETED by the retention sweep.
//
// It is the observability half of a destructive operation, and it exists because the
// alternative is a retention job whose two failure modes are both silent. A sweep that has
// stopped running leaves this counter flat while the table grows without bound — an
// ever-growing second copy of the ledger's most sensitive data, which is the outcome
// retention exists to prevent. A sweep that is deleting far more than expected shows as a
// step change here, which is the earliest signal that a cutoff was misconfigured.
//
// A COUNTER rather than a gauge, so a rate over it answers "is retention keeping up with
// the arrival rate", and so a restart cannot be mistaken for a sweep that deleted nothing.
//
// Attributes: none. The rows deleted are terminal events of every type and category, and
// attributing by type would put unbounded-cardinality event names on a counter whose only
// question is how much was removed.
var EventsPurgedTotal metric.Int64Counter

// SubscriberRevocationsPending is the number of subscribers whose broker-side credential
// revocation is still OWED, and OldestSubscriberRevocationAgeSeconds is how long the oldest
// of them has been outstanding.
//
// # AUTH-01: an unrevoked credential has to be visible, not merely recorded
//
// Provisioning writes a SCRAM credential before its ACL bindings, so a binding failure is
// compensated by revoking the credential. When that compensation ALSO fails, a live means of
// authenticating to the event bus exists for a principal the registry records no issuance
// for. The registry now records that obligation durably — but a row nobody reads is not an
// alert, and the exposure it describes does not expire on its own.
//
// These two gauges are what make it actionable. The COUNT answers "is anything outstanding",
// which is normally zero and any non-zero value is worth a look. The AGE is the one to alert
// on: a marker cleared within a minute by a retry is routine, and one outstanding for hours
// means the automatic settlement paths — a re-issue or a deprovisioning — are not being
// exercised and a human has to revoke by hand.
//
// The age is measured from when the obligation was FIRST recorded and is deliberately not
// reset by a later failed attempt, so it reports the age of the exposure rather than the age
// of the last try.
//
// GAUGES rather than counters: both are point-in-time facts about outstanding work, and both
// legitimately return to zero when the work is settled. Zero is published explicitly rather
// than left stale, so "nothing outstanding" is distinguishable from "the collector stopped".
//
// Attributes: none, on both. The subscriber a marker belongs to is deliberately NOT an
// attribute — subscriber identifiers are unbounded in cardinality, and the question these
// answer is how much is outstanding and for how long, not which. The rows themselves are the
// per-subscriber detail, listed oldest first through the registry.
var (
	SubscriberRevocationsPending         metric.Int64Gauge
	OldestSubscriberRevocationAgeSeconds metric.Float64Gauge
)

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

// EventCaptureToDispatchDurationBuckets are the explicit bucket boundaries, in SECONDS, of
// EventCaptureToDispatchDuration.
//
// They are a DIFFERENT set from EventPublishDurationBuckets because the two instruments
// measure differently-shaped quantities. A broker write is milliseconds; an end-to-end
// capture-to-acknowledgement interval has a floor of the relay's poll interval (1 second)
// and a tail that includes the entire retry schedule, so its useful range starts later and
// runs much further.
//
//   - 0.05 s to 0.5 s is the sub-poll-interval region, reachable when a row is captured
//     just before a tick. Kept dense enough to show that it is happening at all.
//   - 1 s, 1.5 s and 2 s bracket the acceptance target. 2 s is an edge EXACTLY because the
//     requirement is stated at two seconds: "is p99 under 2 s" is then answered from bucket
//     counts alone, with no interpolation across the threshold.
//   - 3 s to 31 s covers a row that needed retries; 31 s is the whole configured retry
//     schedule (1 + 2 + 4 + 8 + 16), so an event that spent its entire budget lands on an
//     edge rather than being blended into a neighbouring bucket.
//   - 60 s and 300 s keep a genuinely backlogged pipeline on the scale. Without them every
//     stalled event would fall into +Inf together, and the difference between one minute
//     behind and five is exactly what an operator needs during an incident.
//
// 15 boundaries means 16 buckets per attribute combination, and the attribute domains are
// closed (topic × attempt), so the series count is bounded by construction.
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
		metric.WithDescription("Total number of ledger events durably recorded as dispatched by topic and event type"),
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

	OutboxPendingBacklog, err = meter.Int64Gauge("blnk.outbox.pending",
		metric.WithDescription("Number of event outbox rows not yet published to Kafka"),
		metric.WithUnit("{event}"),
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

	return nil
}
