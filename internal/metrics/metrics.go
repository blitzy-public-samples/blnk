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

// EventsPublishedTotal counts ORIGINAL LEDGER EVENTS ONCE EACH, at the moment their Kafka
// leg becomes durably recorded — the broker acknowledged the write AND the
// claim-token-conditional statement that records that leg has committed.
//
// # It is incremented on the durable transition, never on the acknowledgement (OBS-05)
//
// Delivery is at-least-once by construction: the relay publishes, then records the leg, and a
// crash or a failed statement between those two steps deliberately leaves the row claimable so
// the event is published AGAIN — losing an event is unrecoverable while a duplicate is
// suppressed at the subscriber on event_id. Incremented at the acknowledgement, that republish
// counted the same event twice, so a counter documented as one-per-event silently became
// one-per-successful-write exactly when the pipeline was struggling. Because it is the
// denominator acceptance criterion V-3's dead-letter rate is stated over, over-counting it
// UNDERSTATED that rate precisely during an incident.
//
// The transitions that record the Kafka leg — MarkEventDispatched and MarkEventWebhookPending —
// are both conditional on the claim token and both clear or consume it, so each succeeds for
// one worker once in an event's whole life. Counting there makes the increment unique by
// construction. The cost is that this counter LAGS THE WIRE by one bookkeeping statement, which
// is the correct trade for a per-event count; the acknowledged-write rate remains readable on
// EventBrokerAcknowledgementsTotal and on EventPublishAttemptsTotal{outcome="dispatched"}.
//
// It counts ORIGINAL publishes only. A replay and a dead-letter write are separate purposes,
// visible on EventBrokerAcknowledgementsTotal under their own purpose attribute and on
// EventPublishAttemptsTotal and EventPublishDuration under their own fixed attempt attribute,
// which is where re-delivery belongs.
//
// USE THIS ONE for anything that must be per-event: the throughput verdict for acceptance
// criterion V-1 and the denominator of the dead-letter rate for V-3. Use
// EventBrokerAcknowledgementsTotal to see how much the relay is WRITING, and the ratio between
// the two to see how much of that work is redelivery.
//
// Attributes: topic, event_type
var EventsPublishedTotal metric.Int64Counter

// EventBrokerAcknowledgementsTotal counts publishes the BROKER ACKNOWLEDGED, whether or not
// the outbox row was subsequently marked.
//
// It exists because EventsPublishedTotal is incremented on the durable transition, which
// leaves the wire signal unrepresented. The two are deliberately different measurements and
// the gap between them is the diagnostic:
//
//   - ACKS ≈ PUBLISHED is the healthy steady state.
//   - ACKS > PUBLISHED means events are reaching Kafka but their rows are not being marked —
//     a database fault, a lost claim, or a relay dying between the two steps. Those events
//     WILL be republished, so this gap is the leading indicator of duplicate delivery that
//     EventsPublishedTotal, by design, cannot show.
//   - PUBLISHED > ACKS cannot happen and indicates an instrumentation defect.
//
// Unlike EventsPublishedTotal it counts EVERY purpose — original publishes, replays and
// dead-letter writes — because it measures traffic the broker accepted rather than first
// deliveries of business events. Do not substitute it for EventsPublishedTotal in the
// dead-letter rate: it would count a dead-lettered event on both sides of the ratio.
//
// Attributes: topic, event_type, purpose
var EventBrokerAcknowledgementsTotal metric.Int64Counter

// EventsDispatchedTotal counts ledger events ONCE EACH, at the moment their delivery becomes
// durably recorded: the outbox row reaches the dispatched state under the claim token that
// authorised the publish.
//
// # PERF-P21: why a second counter, and why this is the one to build a verdict on
//
// `dispatched` is terminal and sits outside the claim predicate, and the transition into it
// is conditional on the claim token and clears it. So it succeeds for exactly one worker,
// exactly once, for the whole life of an event — which is what makes this increment unique
// where EventsPublishedTotal's is not. A republish after a crash writes to the broker again
// and increments that counter again; it cannot increment this one, because the row it would
// have to move is already dispatched.
//
// TWO statements reach that state and both increment here, which is what makes the count
// complete rather than merely unique:
//
//   - MarkEventDispatched, the ordinary arm — the Kafka write is acknowledged and the legacy
//     webhook leg is done or was never owed.
//   - MarkEventWebhookPending's ABANDON arm — the Kafka write is acknowledged and the legacy
//     leg has exhausted its own enqueue budget, so the row is moved to dispatched with the
//     webhook given up on. The event IS on its topic and must be counted as delivered.
//
// A row whose webhook is still owed is NOT counted yet: it is in webhook_pending, not
// dispatched, and it is counted on the later pass that settles it — once, by whichever of the
// two arms applies. So the count lags the wire by the duration of one bookkeeping statement,
// and by the legacy leg's remaining budget for the rows still owed one. That is the correct
// trade for a per-event count; the alternative over-counts.
//
// REPLAYS ARE EXCLUDED. A replayed event reaches dispatched through the dead-letter service
// rather than the relay, and it was already accounted for as dead-lettered. Counting it here
// too would put one event in both terminal counters and break the partition the dead-letter
// rate depends on.
//
// # The dead-letter rate
//
// The rate acceptance criterion V-3 is stated against is
// EventsDeadLetteredTotal / (EventsDeadLetteredTotal + EventsDispatchedTotal). The two
// counters partition the TERMINAL outcomes of a captured event — an event is either
// delivered or dead-lettered, never both — so their sum is the population and neither alone
// is. Dividing by the delivered count alone would report dead-letters as a fraction of
// successes, which exceeds the true rate and diverges without bound as the failure rate
// rises (every event failing gives a denominator of zero).
//
// # How it differs from EventsPublishedTotal, which is also once per event
//
// Both are per-event and both are incremented after a claim-token-conditional transition, so
// neither can double-count. They differ in WHICH transition, and therefore in WHEN:
//
//   - EventsPublishedTotal counts the KAFKA LEG becoming durable, including for a row that
//     then waits in webhook_pending for its legacy leg. It is the earliest honest per-event
//     count and is what the V-1 throughput verdict and the V-3 rate are read from.
//   - This one counts the row reaching the TERMINAL dispatched state, so for a row that owes a
//     legacy webhook it lags by that leg's remaining budget. It is what answers "how many
//     events are completely settled", which is the figure the outbox backlog is reconciled
//     against.
//
// The two converge at the sunset, when the legacy leg and the webhook_pending state are
// removed and every acknowledged event reaches dispatched in one step. Until then the
// difference between them is the population still owed a webhook.
//
// Attributes: topic, event_type — the same set EventsPublishedTotal carries, so the two are
// directly comparable.
var EventsDispatchedTotal metric.Int64Counter

// EventPublishAttemptsTotal counts individual relay publish attempts, retries included,
// so retry pressure stays visible independently of how many events were delivered.
//
// The outcome vocabulary is CLOSED AT THREE VALUES and mutually exclusive — exactly one
// value is recorded per attempted write, so summing the series yields the total number of
// attempts:
//
//   - dispatched: the broker acknowledged the write.
//   - retrying: the attempt did not deliver the event. This is the SINGLE failure outcome,
//     whether or not another attempt will follow.
//   - dead_lettered: an acknowledged write to a `<topic>.dlt` sibling, which is the
//     terminal outcome of the ORIGINAL event.
//
// # Why "how many events are stuck" is a separate dimension
//
// A fourth outcome, `failed`, once carried the fact that no further attempt was possible.
// It was removed because it widened the three-value publish-status vocabulary that
// model.PublishStatus, the API responses and docs/event-streaming.md all state — a
// subscriber or dashboard reading the documented three values against a series carrying
// four gets an outcome it has no case for.
//
// The fact itself is still reported, on its own bounded dimension instead: `terminal` says
// whether the event will be attempted again. Stuck events are therefore selected as
// {outcome="retrying",terminal="true"}, and a rule still selecting outcome="failed"
// matches nothing at all — which reads as "nothing is stuck" and is why the retirement is
// recorded here rather than only in the changelog.
//
// Attributes:
//   - outcome: dispatched, retrying, dead_lettered.
//   - terminal: true, false. Describes FAILURES: true on a retrying attempt means no
//     further attempt will be made, because the failure is permanent or the budget is
//     spent. A dispatched write and a dead-letter write both report false.
var EventPublishAttemptsTotal metric.Int64Counter

// EventPublishDuration records how long a single publish took, measured from the moment
// the pipeline started working on the event — the outbox claim for a relay publish, the
// request instant for a replay — to broker acknowledgement.
//
// # THIS IS NOT THE SERIES V-1 IS READ FROM
//
// The sub-two-second p99 target in acceptance criterion V-1 is stated over outbox capture
// to broker acknowledgement, and it is read from EventCaptureToDispatchDuration below. This
// instrument's clock starts when the relay's claim query returns, so it excludes the queue
// wait — the row sitting pending until the next poll tick, the poll interval, and the claim
// itself — which is exactly the component that grows when the relay falls behind. A stalled
// relay reports the same few milliseconds here as an idle one, so certifying V-1 from this
// series reports a pass for precisely the backlog the criterion exists to catch.
//
// What this instrument answers is the BROKER WRITE IN ISOLATION. Differenced against the
// end-to-end figure it yields the queue wait, which is how an operator tells a slow broker
// from a relay backlog:
//
//	histogram_quantile(0.99, sum by (le) (rate(
//	  blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))
//
// The two bounded attributes are what make that population selectable rather than blended:
// failed and retried attempts are recorded too — a broker that times out is precisely when
// latency data matters — but they carry a different outcome, so they cannot contaminate the
// figure above.
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
//   - outcome: dispatched, retrying, dead_lettered — the same three-value vocabulary
//     EventPublishAttemptsTotal carries. The retired `failed` value matches nothing.
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
// topic, not the .dlt sibling, so it is comparable with EventsDispatchedTotal — the two
// partition the terminal outcomes of a captured event, and their sum is the population the
// dead-letter rate is a fraction of.
// Attributes: topic, event_type
var EventsDeadLetteredTotal metric.Int64Counter

// EVERY GAUGE BELOW is maintained by ONE production caller, the periodic
// EventMetricsCollector in event_metrics.go, and by nothing else — the dead-letter age, the
// consumer-lag pair and its coverage gauges, the outbox backlog, the registry size, and the
// revocation, orphan and settlement gauges. The set has grown as the subscriber lifecycle
// did, so it is named by its OWNER rather than by a count that would drift the next time one
// is added; what matters is that no other caller writes any of them. That single ownership is
// a correctness requirement rather than tidiness, for two reasons that apply to every gauge in
// an exporter and to these in particular.
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
// alerting for ever.
//
// The age is EXACT, at any inventory size (PERF-P07). It used to be drawn from a bounded walk
// of the oldest entries, so past that bound it was a lower bound and said so — safe for a
// threshold alert only in the direction that already fires, and unable to fire at all in the
// case that matters if the bound cut above the threshold. One grouped MIN over a partial index
// replaced the walk, so there is no bound and nothing to truncate.
//
// The topic attribute is the `.dlt` sibling name, not the original category topic, because
// this measures what is waiting on a dead-letter topic. That is the opposite convention from
// EventsDeadLetteredTotal, which carries the ORIGINAL topic so it stays comparable with
// EventsDispatchedTotal, and the difference is deliberate in both cases.
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

// SubscriberLagPassAgeSeconds is how long the IN-PROGRESS pass over the subscriber registry
// has been running, and SubscriberLagCoveredSubscribers is how many subscribers currently have
// an exported lag series. Together they are the COVERAGE statement for consumer-lag
// measurement (PERF-P22).
//
// # Why lag alone is not enough
//
// Measuring every registered subscriber on every tick would spend a broker round trip per
// subscriber per tick, so the collector measures a bounded slice each tick and ROTATES through
// the registry, retaining each reading under a TTL so a subscriber's series stays exported
// between the ticks that refresh it. That makes the >10000 lag rule reliable only while the
// rotation returns to a subscriber before its reading expires.
//
// If it does not, the failure is INVISIBLE IN THE LAG SIGNAL ITSELF. The subscriber's series
// simply stops being exported, and an absent series does not breach a threshold — so a
// registry that has outgrown the measurement budget reports no lag problem for precisely the
// subscribers it can no longer see. That is the original defect in a new form: it moved from
// "the oldest subscribers are never measured" to "they are measured too rarely to alert on".
//
// # How to read them
//
// The pass age resets to zero each time a rotation completes, so its MAXIMUM over a window is
// the rotation latency:
//
//	max_over_time(blnk_kafka_consumer_lag_pass_age_seconds[1h])
//
// Compare that against the reading TTL. Exceeding it means readings expire before the rotation
// returns to them, and the remedy is a larger per-tick budget, a shorter tick, or fewer
// subscribers per instance — not a change to the lag rule.
//
// The covered count is the size of the exported inventory. Persistently below the registry
// size means subscribers are being skipped outright rather than merely late: a row with no
// authorized topics, or one whose identifiers were not generated by the registry, is not
// measurable and is counted as skipped instead.
//
// GAUGES rather than counters: both are point-in-time facts, and both legitimately fall — a
// completed pass takes the age back to zero, and a deprovisioned subscriber lowers the count.
//
// Attributes: none, on both. These are process-wide facts about the measurement itself, and
// attributing them per subscriber would restate the per-subscriber inventory that
// SubscriberConsumerLag already carries — at the same unbounded cardinality, for a question
// that is not per subscriber.
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

// EventMetricsLastCollectionAgeSeconds is how long ago the periodic event-metrics collector
// last FINISHED a collection, whatever that collection reported.
//
// # It is the freshness of every other event gauge in this file
//
// The backlog, the dead-letter age, the revocation gauges and the consumer-lag inventory are
// all maintained by one periodic collector. A gauge holds its last value until it is written
// again, so a collector that hangs — on a database round trip, on a broker that accepts the
// connection and never answers — leaves every one of them being scraped as though it were
// current. Nothing in the values themselves can express that: a stalled backlog of 12 and a
// live backlog of 12 are the same number. This gauge is what tells them apart.
//
// # Why it is ASYNCHRONOUS and not written by the collector
//
// A synchronous gauge could only be updated by the collector, which is the very thing that may
// have stopped: it would freeze at whatever age it last reported and read as permanently fresh.
// This is observed at COLLECTION TIME from a stored timestamp instead, so the age rises on
// every scrape for as long as nothing collects, without the stalled component having to
// participate.
//
// # Reading it
//
// It sits near zero and never exceeds the collection interval plus one collection's duration
// (15 seconds plus the tick budget by default). A value materially above that means the loop is
// not ticking. ABSENCE means no collector is running in the scraped process at all, which is
// the normal state of the WORKER role — the collector is deliberately server-only, so an
// absence rule must be scoped to the server job.
//
// It is fed by RecordEventMetricsCollection, called once per tick by the collector in
// event_metrics.go, and by nothing else.
//
// Attributes: none
var EventMetricsLastCollectionAgeSeconds metric.Float64ObservableGauge

// EventMetricsLastSuccessAgeSeconds is how long ago the periodic event-metrics collector last
// completed a collection with NO failures.
//
// It is the second half of the collection-health pair, and the two answer different questions.
// EventMetricsLastCollectionAgeSeconds says whether the loop is running; this says whether it is
// achieving anything. A collector whose database is refusing connections ticks perfectly on
// schedule and publishes nothing — the first gauge stays near zero throughout, and only this one
// rises. Alerting on the first alone would report a healthy monitoring pipeline while every
// event gauge went stale.
//
// ASYNCHRONOUS for the same reason as its sibling, and observed from the same snapshot by the
// same callback so the pair can never describe two different ticks.
//
// # It is never absent while a collector is running
//
// A collector that has NEVER succeeded reports the age from when it started collecting rather
// than reporting nothing. Reporting nothing made the most degraded state — failing since
// start-up — the one state a threshold rule over this gauge could not detect, because the rule
// would have had no series to evaluate. So a rising value with no prior success and a rising
// value after a long healthy period read the same way, which is the correct reading: neither is
// currently known good.
//
// Attributes: none
var EventMetricsLastSuccessAgeSeconds metric.Float64ObservableGauge

// EventMetricsCollectionFailuresTotal counts individual collection failures inside the periodic
// event-metrics collector, attributed by WHICH collection failed.
//
// # Why a per-collection counter and not one number
//
// The collector performs five independent collections per tick and deliberately continues past
// a failure in any of them, because the moment one dependency fails is when the gauges over the
// others matter most. That design has a cost this counter pays: a partially failed tick used to
// be visible only in one log line, so "the backlog is 0" and "the backlog could not be counted"
// were indistinguishable to anything reading metrics. The attribute names the failing
// collection, so a database fault and a broker fault are separable without reading logs.
//
// The attribute domain is CLOSED at the five collections plus the registry enumeration, all
// fixed literals declared in event_metrics.go. No error text and no identifier ever reaches it.
//
// Attributes: collection
var EventMetricsCollectionFailuresTotal metric.Int64Counter

// ConsumerLagInventoryComplete reports whether the last lag sweep measured EVERY registered
// subscriber: 1 for yes, 0 for no.
//
// # The gap it closes
//
// The consumer-lag inventory is whole-set: what the sweep publishes is exactly what is
// exported, so a subscriber the sweep never reached has NO SERIES rather than a wrong one. That
// is the honest representation of an unmeasured subject and it is also completely silent — a
// registry larger than the sweep's budget left every subscriber past the budget permanently
// unmeasured and permanently unalertable, and the only trace was one warning line per tick.
// Absence cannot be alerted on per-subscriber, because the labels of a series that does not
// exist are unknown.
//
// So completeness is published as its own fact. Zero means at least one registered subscriber's
// lag is not being measured, whatever the reason — budget exhausted, registry unreadable, or a
// measurement that failed — and SubscriberLagCoverageIncomplete fires on it. Read
// SubscribersUnmeasured beside it for how many and why.
//
// A synchronous gauge is correct here, unlike the staleness pair above: this describes the
// SWEEP, so a value that stops being refreshed is covered by the staleness gauges rather than by
// this one, and there is exactly one series so nothing can be left stale by churn.
//
// Attributes: none
var ConsumerLagInventoryComplete metric.Int64Gauge

// SubscribersUnmeasured is how many registered subscribers the last sweep did not publish a
// complete lag reading for, attributed by why.
//
// "Complete" rather than "any": four of the five reasons mean no series at all, and topic_missing
// means the series that were published omit a topic the subscriber is authorised on. Both are
// answers to the same operational question — is what I am looking at the whole picture — so they
// share one gauge rather than being split across two an operator has to remember to read
// together.
//
// It is the quantity behind ConsumerLagInventoryComplete, and it is written for EVERY reason on
// every tick — zeros included — because a reason that disappears from the export is
// indistinguishable from a reason that has been resolved.
//
// The reason vocabulary is closed and each value means something an operator would act on
// differently:
//
//   - budget: the sweep's per-tick budget was reached, so these subscribers were not examined.
//     They ARE reached on a later tick — the sweep resumes from a rotating cursor rather than
//     restarting at the newest row — so this is a lag in coverage, not a permanent hole. Raise
//     EVENT_METRICS_SUBSCRIBER_BUDGET to close it.
//   - unprovisioned: the row carries no authorised topics, or identifiers the registry did not
//     issue. There is nothing to measure and the row itself is what needs fixing.
//   - measure_failed: the broker refused the measurement for this subscriber. A broker or ACL
//     fault.
//   - registry_failed: the registry enumeration itself failed, so an unknown number of
//     subscribers were never reached. Reported as the count that had not been examined when the
//     enumeration broke.
//   - topic_missing: the measurement succeeded but named at least one authorised topic that does
//     not exist at the broker, so that topic contributes no lag and no partition health. The
//     registry row or the topic provisioning is what needs fixing, not the broker.
//
// Attributes: reason
var SubscribersUnmeasured metric.Int64Gauge

// SubscribersRegistered is how many subscribers the registry holds.
//
// # SEC-10: it exists so a monitoring GAP can be read as a proportion
//
// SubscribersUnmeasured above says how many subscribers have NO consumer-lag series, which is
// the worst property a monitoring system can have — silence and health are indistinguishable,
// because SubscriberConsumerLagHigh has nothing to evaluate for a subscriber that was never
// measured. That count alone cannot be judged: three unmeasured out of five is a different
// situation from three out of three thousand.
//
// This gauge supplies the denominator, and it also makes the measurement budget's HEADROOM
// visible before it is exhausted rather than only after, since the budget itself has no
// series of its own.
//
// A GAUGE, published on every tick including when nothing is unmeasured, so a stopped
// collector is distinguishable from a fully measured registry. Attributes: none — the
// question is how many, not which, and a subscriber label would put unbounded-cardinality
// tenant identifiers on it.
var SubscribersRegistered metric.Int64Gauge

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

	// SubscribersUnmeasuredReasonTopicMissing is a subscriber at least one of whose authorised
	// topics does not exist at the broker. The measurement itself SUCCEEDED, which is why this
	// needs its own reason: a topic that is absent contributes no lag and no unreadable
	// partitions, so the subscriber looks measured and the gap it leaves is silent.
	SubscribersUnmeasuredReasonTopicMissing = "topic_missing"
)

// SubscriberUnmeasuredReasons returns every value of SubscribersUnmeasured's reason attribute.
//
// The collector writes the gauge for ALL of them on every tick, zeros included, and this is
// what makes that enumerable rather than hand-maintained at the call site. Omitting a reason
// would leave its last non-zero reading exported indefinitely, so "the budget stopped being
// reached" and "the budget is still being reached" would look identical.
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

// eventMetricsCollectionHealth holds the timestamps the asynchronous collection-health gauges
// are observed from.
//
// A mutex rather than atomics, for the reason consumerLagInventory uses one: the read side runs
// inside a collection callback the SDK may invoke concurrently with a write, and the timestamps
// must be read as ONE snapshot — reporting a last-success from before a tick alongside a
// last-attempt from after it would make the pair describe a state that never existed.
//
// # Why firstAttempt exists
//
// A collector that has NEVER completed a clean collection is the most degraded state there is,
// and it is the state a broken dependency produces from start-up. With only lastSuccess, that
// state has a zero timestamp, the callback observes nothing, and the success-age series is
// ABSENT — so EventMetricsCollectionFailing could not fire for it. The rule would only ever
// catch a collector that succeeded once and then broke, and would stay silent on one that never
// worked at all.
//
// firstAttempt closes that: until a clean collection happens, the success age is reported from
// when the collector STARTED collecting, which is the honest answer to "how long has it been
// since this was last known good". It is set once and never moves, so the age rises
// monotonically for as long as the condition holds.
//
// The remaining absence — before the very first tick finishes — is the one case where absence is
// right: there is genuinely no age yet, and the collector runs its first collection immediately
// on Start, so the window is milliseconds. The "no collector at all" case is a PERMANENT absence
// and is alerted on with absent(), scoped to the role that runs one.
var eventMetricsCollectionHealth struct {
	mu           sync.RWMutex
	firstAttempt time.Time
	lastAttempt  time.Time
	lastSuccess  time.Time
}

// RecordEventMetricsCollection records that a collection finished, and whether it was complete.
//
// It is called once per tick by the periodic collector and by nothing else. Both timestamps are
// written under one lock so the pair the gauges observe is always self-consistent.
//
// A NON-MONOTONIC call is ignored rather than applied: an `at` older than what is already
// recorded would move an age BACKWARDS, and the only ways to produce one are a clock adjustment
// and a stale goroutine finishing after a newer tick. Both should leave the reported freshness
// alone rather than making it optimistic.
//
// Parameters:
//   - at time.Time: when the collection finished. A zero value is ignored, since it would
//     otherwise mark the collector as never having run.
//   - complete bool: true when the collection reported no failures at all. Only a complete
//     collection advances the success timestamp, because a tick that published half its gauges
//     has not established the freshness of the other half.
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
// It exists so the recording can be asserted without a metric reader, and so an operational
// endpoint could report collection freshness directly.
//
// Returns:
//   - firstAttempt time.Time: when the first collection finished. Zero before any has. It is
//     what the success age is reported from while no clean collection has ever happened.
//   - lastAttempt time.Time: when a collection last finished. Zero when none ever has.
//   - lastSuccess time.Time: when a collection last finished with no failures. Zero when none
//     ever has.
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
// ONE callback over both, from ONE snapshot, so the pair always describes the same tick — see
// eventMetricsCollectionHealth. A future timestamp is clamped to zero rather than reported as a
// negative age.
//
// # Both gauges are published from the first finished tick onwards
//
// The success age FALLS BACK to firstAttempt when no clean collection has ever happened, so a
// collector that has been failing since start-up reports a rising success age rather than no
// series at all. Reporting nothing was the earlier behaviour and it made the worst case the one
// case EventMetricsCollectionFailing could not detect: the rule matched only collectors that had
// once succeeded, and stayed silent on one that never had.
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
// The floor is not defensive dressing: `at` is stamped by this process and `now` is read at
// collection time, so a clock adjustment between the two produces a negative interval, and a
// negative age on a freshness gauge would read as the freshest possible value — the exact
// opposite of the truth — and would satisfy every threshold rule written over it.
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

// SubscriberSettlementOutstanding is how many subscribers owe broker-side reconciliation,
// SubscriberGrantReconcilePending and SubscriberCredentialCleanupPending split that by kind, and
// OldestSubscriberSettlementAgeSeconds is how long the oldest of them has been outstanding.
//
// # What they describe
//
// A subscriber's state lives in two systems that cannot be written atomically — the registry row
// and the broker's principal, credential and ACL bindings — so an operation that fails part-way
// leaves them disagreeing. Those disagreements are now recorded durably on the row instead of
// only in a log line, and a background pass discharges them. These gauges are what make the
// backlog of that pass visible.
//
// # Why the split as well as the total
//
// The total is the figure to graph. The split changes what an operator DOES about it: a rising
// credential-cleanup count is a security matter — a credential may exist that Blnk meant to
// destroy — while a rising grant-reconciliation count is an availability matter, a subscriber
// whose enforced access may be wider or narrower than the registry records. One number cannot
// say which.
//
// The total is NOT the sum of the two, because one subscriber can owe both.
//
// # The age is the one to alert on
//
// An obligation settled within a minute is routine — it is the mechanism working. One outstanding
// for an hour means either the settlement pass is not running or the broker has been unreachable
// throughout, and both need a human. The age is measured from when the obligation was FIRST
// recorded and is not reset by a later failed attempt, so it reports the age of the divergence
// rather than the age of the last try.
//
// GAUGES rather than counters: all four are point-in-time facts about outstanding work and all
// legitimately return to zero when it is settled. Zero is published explicitly rather than left
// stale, so "nothing outstanding" is distinguishable from "the collector stopped".
//
// Attributes: none, on any of them. The subscriber an obligation belongs to is deliberately NOT
// an attribute — subscriber identifiers are unbounded in cardinality — and the questions these
// answer are how much and for how long, not which. The rows themselves are the per-subscriber
// detail, listed oldest first through the registry.
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
// created by a failed ISSUANCE that never stamps it. The two states also demand opposite
// remedies — an orphan is settled by re-issuing or deprovisioning, a refused revocation by
// retrying the deregistration once the broker's refusal is understood — so they are reported
// separately rather than folded together.
//
// The orphan age is the alertable quantity, measured from when the exposure was FIRST
// recorded and never reset by a later attempt, because it measures how long a credential
// nobody accounts for has been able to authenticate.
//
// GAUGES, because each is a point-in-time fact about outstanding work that legitimately
// returns to zero. Zero is published explicitly rather than left stale, and NOTHING is
// published when the read fails — four zeroes on a failed read would assert that every
// credential is accounted for. No attributes: the subscriber identifier is unbounded in
// cardinality, and the registry rows are the per-subscriber detail.
var (
	SubscriberCredentialOrphans                 metric.Int64Gauge
	OldestSubscriberCredentialOrphanAgeSeconds  metric.Float64Gauge
	SubscriberRevocationFailures                metric.Int64Gauge
	OldestSubscriberRevocationFailureAgeSeconds metric.Float64Gauge
)

// SubscriberObligationsSettledTotal counts the broker-side obligations the settlement pass
// discharged.
//
// It is the throughput half of the four gauges above, and it answers the question they cannot: a
// backlog that stays flat looks identical whether the pass is settling nothing or settling as
// fast as new obligations arrive. A rate over this counter separates those, which is what tells
// an operator whether a non-zero backlog is a stuck pass or a busy one.
//
// A COUNTER rather than a gauge, so a restart cannot be mistaken for a pass that settled nothing.
//
// Attributes: none, for the cardinality reason above.
var SubscriberObligationsSettledTotal metric.Int64Counter

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

	// THE LAG-SWEEP COVERAGE TRIO, assigned here beside the two asynchronous lag gauges they
	// qualify. All three were DECLARED and never assigned, which is not a cosmetic omission: a
	// nil instrument panics on the first Record, so the collector tick that first published a
	// coverage reading would have taken the whole collector down — and these are precisely the
	// instruments that say the lag alert cannot fire, so the failure would have removed the
	// signal that the signal was missing.
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

	// ATTRIBUTED BY REASON, and named blnk.kafka.subscribers_unmeasured. The reason decides
	// the remediation — only 'budget' is answered by configuration — so the aggregate alone
	// would send an operator to the wrong fix. Sum the reasons away for the total:
	// sum without(reason)(blnk_kafka_subscribers_unmeasured).
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
	// obligations they count down, because they are published together as one reading of one
	// aggregate — SubscriberSettlementOutstanding is the total, the two Pending gauges split it
	// by kind, and the age gauge is what the age-based alert is stated over. All four were
	// declared and never assigned.
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

	// The collection-health pair. ASYNCHRONOUS, because the subject is a component that may
	// have STOPPED: a synchronous gauge could only be written by the collector itself and would
	// freeze at its last reading, reading as permanently fresh — which makes a dead collector
	// indistinguishable from a healthy one. Created before the callback that feeds them,
	// because RegisterCallback takes the instruments as arguments.
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
	// snapshot and the pair can never describe two different ticks. The Registration handle is
	// discarded for the reason the consumer-lag one is: the callback lives for the life of the
	// process and there is no metrics shutdown path that would unregister it.
	if _, err = meter.RegisterCallback(
		observeEventMetricsCollectionAges,
		EventMetricsLastCollectionAgeSeconds,
		EventMetricsLastSuccessAgeSeconds,
	); err != nil {
		return err
	}

	return nil
}
