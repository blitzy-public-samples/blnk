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

// event_metrics.go holds the ONE production maintainer of the event pipeline's three
// gauges: the outbox backlog, the dead-letter age, and subscriber consumer lag.
//
// # Why a periodic collector, and not a call from the code that causes each condition
//
// A COUNTER can be incremented where the thing it counts happens, because a counter only
// ever accumulates. A GAUGE cannot, and the difference is not stylistic:
//
//   - A gauge RETAINS ITS LAST VALUE until it is written again or the process restarts. A
//     value recorded once, at the moment something went wrong, keeps alerting long after
//     the condition has cleared — an alert nobody can clear, for a problem that is over.
//     Every one of the three is therefore re-recorded on every tick from authoritative
//     state, INCLUDING AN EXPLICIT ZERO when there is nothing outstanding. Zero is the
//     measurement that clears the alert; omitting it is what leaves a stale one firing.
//   - A gauge written from the code path that CAUSES the condition measures the wrong
//     quantity. Recording the dead-letter age where an event is dead-lettered would report
//     the age of something that just happened — always near zero — so the "stuck for 15
//     minutes" alert could never fire while every dashboard looked healthy. The collector
//     reads the OLDEST outstanding entry instead, which is what the alert is expressed
//     against.
//   - A series whose subject DISAPPEARS is never written again, so it too keeps its last
//     value. A deleted subscriber would go on alerting forever. The collector therefore
//     remembers the label sets it published on the previous tick and explicitly zeroes any
//     that are gone.
//
// Without this file every gauge the alert rules read is declared, initialised, documented
// and never recorded: `/metrics` carries no series for them, and NONE of the thirteen rules
// in alerts/blnk-kafka-alerts.yml can fire at all — including the two acceptance-criterion
// alerts, dead-letter age above 900 seconds and consumer lag above 10,000 messages —
// whatever the system is actually doing.
//
// # Shape
//
// EventMetricsCollector is modelled on LineageOutboxProcessor [lineage_worker.go:31-193],
// the house pattern for a background loop, and on chain_worker.go which is modelled on the
// same thing: a mutex-guarded running flag, a stop channel, a wait group, fluent With*
// configurators, a double-start-guarded Start(ctx), a Stop() that closes and waits,
// IsRunning(), and a ticker/select loop over context cancellation, the stop channel and the
// tick. Following it means the eventual wiring in the server role is the same two lines the
// lineage processor already gets, and an operator reasons about one lifecycle rather than
// three.
//
// The poll interval differs from that precedent on purpose. A relay polls every second
// because a claimable row is work waiting to be done; a gauge is an OBSERVATION, and its
// cadence is set by the Prometheus scrape interval — 15 seconds in prometheus.yml — and by
// the cost of the queries behind it. Collecting far more often than the scrape would spend
// database and broker round trips producing values nothing reads.
//
// # Where this must be started
//
// In the SERVER role, in cmd/server.go, immediately alongside the existing lineage outbox
// processor, using the same construct / start / defer-stop shape that file already applies
// to LineageOutboxProcessor and the chain processor:
//
//	collector := blnk.NewBlnkEventMetricsCollector(b.blnk, deadLetterService, kafkaAdmin)
//	collector.Start(ctx)
//	defer collector.Stop()
//
// The server role rather than the worker role, for the same reason the lineage relay lives
// there: it is where the outbox background work already runs, and hosting it here avoids
// adding a fourth asynq server. Starting it in BOTH roles would be actively wrong — two
// collectors would write the same gauges from two processes and each would zero the other's
// series as stale.
//
// Until that call exists those gauges have no maintainer and none of the thirteen rules in
// alerts/blnk-kafka-alerts.yml can fire, which is the precise defect this file was added
// to fix. NewBlnkEventMetricsCollector is deliberately a one-liner so the wiring cannot
// pair one instance's datasource with another's admin client.
package blnk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// Collector defaults.
const (
	// DefaultMetricsCollectionInterval is how often the collector recomputes every gauge.
	//
	// Fifteen seconds matches the Prometheus scrape interval in prometheus.yml, which is
	// the shortest cadence at which a new value can actually be observed: collecting
	// faster would spend a database round trip and a broker round trip per subscriber on
	// values that are overwritten before anything reads them, and collecting slower would
	// leave a cleared condition alerting for the remainder of the interval.
	DefaultMetricsCollectionInterval = 15 * time.Second

	// DefaultSubscriberMetricsBudget caps how many subscribers one tick measures lag for.
	//
	// It is a budget on TWO scarce things at once, which is why it is explicit rather than
	// left to the registry's size. Each subscriber costs an OffsetFetch and a ListOffsets
	// round trip per authorised topic, so an unbounded registry would make one tick
	// unboundedly long and could overrun the interval. And each subscriber-topic pair is a
	// GAUGE SERIES the exporter retains, so the registry's size is also the series count.
	//
	// Two hundred is far above any plausible subscriber count for a single ledger
	// deployment and far below the point at which either cost matters. Reaching it is
	// reported rather than silently applied.
	DefaultSubscriberMetricsBudget = 200

	// subscriberMetricsPageSize is how many registry rows one enumeration query reads.
	// The registry is small and read-rarely — the schema says so explicitly — so a single
	// modest page keeps the whole enumeration to one or two queries.
	subscriberMetricsPageSize = 50

	// lagMeasurementConcurrency is how many subscribers are measured AT ONCE within a page.
	//
	// Each measurement is one or more broker round trips, so sequentially a tick costs the
	// budget multiplied by the round-trip time — and that product, not the budget, is what
	// limits how much of the registry one tick covers on a remote or loaded broker. Measuring
	// concurrently spends the same number of round trips in a fraction of the wall time and
	// asks the broker for no more work than before (PERF-P22).
	//
	// EIGHT, and bounded rather than one goroutine per row, because the broker may already be
	// the thing failing: an unbounded fan-out would open a connection per subscriber at
	// exactly that moment, which is how an observability sweep becomes the cause of the outage
	// it is measuring. Eight keeps a full 200-subscriber budget to twenty-five sequential
	// waves, comfortably inside the collection window, while holding the peak connection count
	// independent of the page size and of the registry's growth.
	lagMeasurementConcurrency = 8

	// outboxBacklogCountWindow is the window the backlog gauge passes to the per-status
	// count. It bounds ONLY the dispatched count, which the gauge never reads; pending and
	// processing are always exact and complete, which is what a backlog gauge requires
	// (PERF-P04).
	outboxBacklogCountWindow = 24 * time.Hour

	// lagReadingTTL is how long a consumer-lag reading is exported after it was taken.
	//
	// It is the staleness bound on the rotating sweep (PERF-P22): a series is published while
	// something is still refreshing it and retired once nothing is, which is how a deleted
	// subscriber's series disappears without an explicit deletion.
	//
	// Ten minutes is set against what the rotation can actually keep current. At the default
	// interval and budget a tick measures 200 subscribers every 15 seconds, so ten minutes is
	// forty ticks and 8,000 subscribers — far above any plausible registry for one ledger
	// deployment, so a healthy rotation never expires a series it is about to refresh. It is
	// also comfortably longer than the lag alert's dwell, so a series cannot expire out from
	// under a firing alert and make it resolve while the condition holds.
	lagReadingTTL = 10 * time.Minute

	// DefaultMetricsCollectionTickBudget bounds ONE WHOLE COLLECTION.
	//
	// Without it the collection ran on the process context, so a dependency that accepted a
	// connection and then never answered — a database in the middle of a failover, a broker
	// whose TCP session is alive and whose responses are not — blocked the loop indefinitely.
	// The consequence is worse than the stall: every gauge keeps its last value, so the whole
	// event pipeline goes on being SCRAPED AS CURRENT while nothing is being measured, and
	// there is no number in any of those values that could reveal it.
	//
	// Thirty seconds is two collection intervals: comfortably longer than a healthy tick,
	// short enough that a hung one is abandoned before a second and third pile up behind it.
	// A ticker drops missed ticks, so the loop recovers on the next one rather than replaying
	// the backlog. It is paired with the last-collection-age gauge — the bound stops the stall
	// being permanent, and the gauge is what makes it visible.
	DefaultMetricsCollectionTickBudget = 30 * time.Second

	// DefaultMetricsCollectionCallBudget bounds ONE dependency call inside a collection.
	//
	// The tick budget alone is not sufficient. Four of the five collections are independent
	// and the collector deliberately continues past a failure in any of them, so a single
	// hung call must not consume the whole tick and starve the other four — the moment one
	// dependency fails is exactly when the gauges over the others matter most. Five seconds
	// is far above a healthy count-by-status or a single subscriber's two round trips, and
	// six of them still fit inside the tick budget.
	DefaultMetricsCollectionCallBudget = 5 * time.Second
)

// The "collection" attribute values of metrics.EventMetricsCollectionFailuresTotal.
//
// A CLOSED set of fixed literals, one per independent collection plus the registry
// enumeration, so the counter's cardinality is a property of this file rather than of whatever
// failed. No error text and no identifier ever reaches the attribute.
const (
	collectionOutboxBacklog     = "outbox_backlog"
	collectionDeadLetterAge     = "dead_letter_age"
	collectionRevocations       = "subscriber_revocations"
	collectionSubscriberLag     = "subscriber_lag"
	collectionSubscriberListing = "subscriber_listing"
)

// lagReading is one retained consumer-lag reading and when it was taken.
type lagReading struct {
	sample     metrics.ConsumerLagSample
	measuredAt time.Time
}

// eventMetricsOutboxStore is the repository surface the backlog gauge needs, and nothing
// else.
//
// One method. The backlog is a COUNT BY STATUS and not a scan, deliberately: the count is
// answered by an index-only aggregate, whereas walking rows to size a queue would make the
// cost of observing the backlog grow with the backlog — worst exactly when the system is
// already struggling.
type eventMetricsOutboxStore interface {
	// CountEventOutboxByStatus returns a status-keyed count of outbox rows. A status
	// with no rows is ABSENT from the map rather than present with a zero, which is what
	// makes the two-value read below mandatory.
	//
	// The window bounds ONLY the dispatched count (PERF-P04), which this collector never
	// reads: the backlog gauge is the sum of pending and processing, both of which are
	// counted exactly and in full however short the window — a row pending for three days
	// must still appear in the backlog it is the point of.
	CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error)
}

// eventMetricsSubscriberStore is the registry surface the lag gauge needs.
//
// Paging rather than "give me all of them": the collector applies a cardinality budget, and
// a paged read lets it stop at the budget instead of loading a registry it will not measure.
type eventMetricsSubscriberStore interface {
	// ListEventSubscribers pages the registry, resuming from a keyset cursor.
	//
	// The cursor is what makes rotating coverage possible (PERF-P22): the sweep measures a
	// bounded number of subscribers per tick, and reading from the top every time measured
	// the newest ones over and over while never reaching the rest of the registry at all.
	ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error)

	// CountSubscriberRevocationsPending returns the outstanding-revocation backlog as two
	// scalars.
	//
	// An AGGREGATE and not a second walk of the registry, for the reason the backlog gauge is
	// a count: observing how much is owed must not cost more as more is owed. It also keeps
	// this measurement independent of the lag sweep's cardinality budget and skip rules,
	// which exist for a different question — a subscriber with no authorised topics is
	// skipped for lag and still matters here.
	// CountEventSubscribers returns how many rows the registry holds (SEC-10).
	//
	// An AGGREGATE rather than a walk, and read independently of the lag sweep's budget and
	// skip rules: it answers how many subscribers EXIST, which is the denominator that turns
	// "the budget stopped me" into "this many subscribers have no lag series at all". A
	// subscriber with no authorised topics is skipped for lag and still counts here.
	CountEventSubscribers(ctx context.Context) (int64, error)

	CountSubscriberRevocationsPending(ctx context.Context) (model.SubscriberRevocationBacklog, error)

	// CountSubscriberSettlementObligations returns the broker-side settlement backlog as four
	// scalars.
	//
	// An AGGREGATE for the same reason as the revocation backlog, and read here for the same
	// reason too: the obligation lives in a row that outlives the request which created it, so
	// only a reading taken FROM the row is true after a restart. A gauge maintained by the code
	// that raises the obligation would report what that code saw on its way past and then go
	// stale.
	CountSubscriberSettlementObligations(ctx context.Context) (model.SubscriberSettlementBacklog, error)

	// CountSubscriberAccessResidue returns how much broker-side access is UNACCOUNTED FOR:
	// credentials that outlived their registry record, and revocations the broker refused.
	//
	// It is a second aggregate rather than more fields on the first because an ORPHANED
	// CREDENTIAL is not in the revocation backlog at all: it is created by a failed
	// ISSUANCE, which never stamps the revocation tombstone. The backlog gauge and its alert
	// were therefore structurally unable to see one, which is the gap this closes.
	CountSubscriberAccessResidue(ctx context.Context) (model.SubscriberAccessResidue, error)
}

// eventMetricsLagMeasurer is the broker surface the lag gauge needs: one method of
// KafkaAdmin, so a test needs no broker and the collector cannot reach any other
// administrative operation.
//
// It notably CANNOT create a topic, mint a credential or bind an ACL. A metrics collector
// that could provision would be one misplaced call away from changing the system it is
// supposed to be observing.
type eventMetricsLagMeasurer interface {
	// IsConfigured reports whether a broker is configured at all.
	IsConfigured() bool

	// ConsumerLag measures how far a consumer group trails the end of the log.
	ConsumerLag(ctx context.Context, req ConsumerLagRequest) (ConsumerLagReport, error)
}

// eventMetricsAgeRefresher is the dead-letter surface the age gauge needs.
//
// The collector delegates rather than reimplementing, because the age has to be measured
// from the persisted dead_lettered_at over dead-lettered rows only, and a second
// implementation of that rule would be a second thing to keep correct. See
// RefreshDeadLetterAgeGauge.
type eventMetricsAgeRefresher interface {
	// RefreshDeadLetterAgeGauge recomputes the per-topic dead-letter ages and records
	// them, zero included for every topic Blnk owns.
	RefreshDeadLetterAgeGauge(ctx context.Context) (DeadLetterAgeReport, error)
}

// Compile-time proof that the production types satisfy the three seams. A signature drift
// fails the build here, on the line that states the contract, rather than at a call site.
var (
	_ eventMetricsOutboxStore     = (database.IDataSource)(nil)
	_ eventMetricsSubscriberStore = (database.IDataSource)(nil)
	_ eventMetricsLagMeasurer     = (KafkaAdmin)(nil)
	_ eventMetricsAgeRefresher    = (*EventDeadLetterService)(nil)
)

// lagSeries identifies one consumer-lag series: the exact label tuple the gauge is
// attributed by.
//
// It is a comparable struct rather than a formatted string so it can be a map key without
// a delimiter that a label value could contain. The three fields are the RESOLVED label
// values — already reduced to the bounded vocabulary — because what has to be zeroed on a
// later tick is the series as EXPORTED, not the raw input it came from.
type lagSeries struct {
	subscriber string
	group      string
	topic      string
}

// EventMetricsCollector recomputes the event pipeline's gauges on a fixed interval.
//
// One instance per process is enough and it is safe for concurrent use: its dependencies
// are read-only after construction and its lifecycle state is mutex-guarded.
//
// The dead-letter service and the Kafka admin are both OPTIONAL. A deployment with no
// broker is a legitimate steady state — it reproduces the legacy webhook sender's
// no-op-when-unconfigured contract — and in that state the backlog gauge is still the
// meaningful one: events accumulate in the outbox and an operator needs to see it. Refusing
// to collect anything because there is no broker would hide exactly the deployment most
// likely to be accumulating a backlog.
type EventMetricsCollector struct {
	outbox      eventMetricsOutboxStore
	subscribers eventMetricsSubscriberStore
	deadLetters eventMetricsAgeRefresher
	admin       eventMetricsLagMeasurer

	interval         time.Duration
	subscriberBudget int

	// tickBudget bounds one whole collection and callBudget bounds one dependency call
	// inside it. Both exist so a hung dependency cannot freeze the refresh of gauges that
	// would otherwise keep being scraped as current. See their defaults for why the loop
	// alone is not enough.
	tickBudget time.Duration
	callBudget time.Duration

	// publishedLagSeries is the set of lag series written on the PREVIOUS tick. It is
	// what makes a disappeared subscriber's series clearable: without it, a deleted
	// subscriber's last reading would stay in the exporter and go on alerting forever.
	publishedLagSeries map[lagSeries]struct{}

	// lagReadings is the retained inventory: the last reading of every series, with when it
	// was taken. It exists because the sweep ROTATES (PERF-P22) — one tick measures a slice
	// of the registry, so publishing only that slice would retire every other subscriber's
	// series each tick and export them flapping in turn. Readings are held until they age
	// past lagReadingTTL, which is what retires a deleted subscriber's series without any
	// explicit deletion.
	lagReadings map[lagSeries]lagReading

	// lagCursor is where the NEXT lag sweep resumes, and it is what gives every registered
	// subscriber coverage rather than only the newest ones (PERF-P22).
	//
	// The sweep is budgeted — each subscriber costs broker round trips and one gauge series
	// per topic — and it used to start from the top of the registry on every tick. With a
	// registry larger than the budget that measured the same newest subscribers for ever and
	// NEVER reached the rest: their lag had no series at all, so the alert on lag above
	// 10,000 could not fire for them, and nothing said so. Carrying the position forward
	// turns the budget from a horizon into a rate: the whole registry is covered in
	// ceil(size / budget) ticks, and the collector reports how long a full pass takes.
	//
	// Nil means "start at the newest", which is both the first sweep and the wrap-around.
	lagCursor *model.SubscriberCursor

	// lagPassSeen is every series measured SO FAR IN THE CURRENT PASS, accumulated across the
	// ticks the pass spans and reset when a new pass begins.
	//
	// It is what makes retirement exact under rotation (PERF-P22). A single tick cannot tell a
	// deleted subscriber from one it has not reached yet, so a tick alone can only let a stale
	// reading expire. A COMPLETED pass can: it has visited the whole registry, so any retained
	// series it did not see belongs to a subscriber that is gone, or to a grant that has been
	// narrowed, and is retired at once rather than lingering for the TTL.
	lagPassSeen map[lagSeries]struct{}

	// lagPassStartedAt is when the current full pass over the registry began, so the
	// coverage figure the report carries is the age of the OLDEST reading in flight rather
	// than a claim about the whole registry being current.
	lagPassStartedAt time.Time

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// NewEventMetricsCollector builds a collector over the given dependencies.
//
// Parameters:
//   - outbox eventMetricsOutboxStore: the outbox repository. Required; without it the
//     backlog cannot be counted and Collect reports an error.
//   - subscribers eventMetricsSubscriberStore: the subscriber registry. Optional — a
//     deployment with no registered subscribers publishes no lag series, which is correct.
//   - deadLetters eventMetricsAgeRefresher: the dead-letter service. Optional.
//   - admin eventMetricsLagMeasurer: the Kafka admin. Optional; an unconfigured one is
//     skipped rather than treated as a failure.
//
// Returns:
//   - *EventMetricsCollector: configured with the default interval and budget.
func NewEventMetricsCollector(
	outbox eventMetricsOutboxStore,
	subscribers eventMetricsSubscriberStore,
	deadLetters eventMetricsAgeRefresher,
	admin eventMetricsLagMeasurer,
) *EventMetricsCollector {
	return &EventMetricsCollector{
		outbox:             outbox,
		subscribers:        subscribers,
		deadLetters:        deadLetters,
		admin:              admin,
		interval:           DefaultMetricsCollectionInterval,
		subscriberBudget:   DefaultSubscriberMetricsBudget,
		tickBudget:         DefaultMetricsCollectionTickBudget,
		callBudget:         DefaultMetricsCollectionCallBudget,
		publishedLagSeries: map[lagSeries]struct{}{},
		lagReadings:        map[lagSeries]lagReading{},
		lagPassSeen:        map[lagSeries]struct{}{},
		stopCh:             make(chan struct{}),
	}
}

// NewBlnkEventMetricsCollector builds the collector a running Blnk instance needs, wiring
// its datasource, its dead-letter service and its Kafka admin.
//
// It exists so that the eventual start-up call in the server role is one line and cannot
// pair one instance's datasource with another's admin client.
//
// Parameters:
//   - b *Blnk: the service container. A nil container, or one without a datasource, yields
//     a collector whose ticks report an error rather than panicking a ledger process.
//   - deadLetters *EventDeadLetterService: the dead-letter service, or nil.
//   - admin KafkaAdmin: the admin client, or nil.
//
// Returns:
//   - *EventMetricsCollector: ready to Start.
func NewBlnkEventMetricsCollector(
	b *Blnk,
	deadLetters *EventDeadLetterService,
	admin KafkaAdmin,
) *EventMetricsCollector {
	collector := NewEventMetricsCollector(nil, nil, nil, nil)

	if b != nil && b.datasource != nil {
		collector.outbox = b.datasource
		collector.subscribers = b.datasource
	}
	// Typed nils would satisfy the interfaces while being unusable, so each is assigned
	// only when it is genuinely present.
	if deadLetters != nil {
		collector.deadLetters = deadLetters
	}
	if admin != nil {
		collector.admin = admin
	}

	// The sweep budget is OPERATIONALLY CONFIGURABLE, read from the instance's configuration
	// rather than fixed in code, because the value decides whether a registry larger than it
	// is covered in one tick or across several. Configuration has already defaulted and
	// clamped it; a nil or zero value leaves the constructor's default in place, which is the
	// same number.
	if b != nil {
		if configuration := b.Config(); configuration != nil && configuration.Kafka.MetricsSubscriberBudget > 0 {
			collector.subscriberBudget = configuration.Kafka.MetricsSubscriberBudget
		}
	}

	return collector
}

// WithTickBudget sets the bound on ONE WHOLE COLLECTION.
//
// A non-positive value falls back to the default rather than being rejected, and — unlike the
// interval — an UNBOUNDED value is not offered at all. The whole purpose of the bound is that a
// hung dependency cannot freeze the refresh of gauges that go on being scraped as current, and
// a configuration knob that could switch that protection off would be a knob for reintroducing
// the defect.
//
// Parameters:
//   - budget time.Duration: the per-collection bound.
//
// Returns:
//   - *EventMetricsCollector: the collector, for chaining.
func (c *EventMetricsCollector) WithTickBudget(budget time.Duration) *EventMetricsCollector {
	if budget <= 0 {
		logrus.WithFields(logrus.Fields{
			"requested": budget.String(),
			"using":     DefaultMetricsCollectionTickBudget.String(),
		}).Warn("event metrics: a non-positive collection tick budget was requested; using the default")

		c.tickBudget = DefaultMetricsCollectionTickBudget

		return c
	}

	c.tickBudget = budget

	return c
}

// WithCallBudget sets the bound on ONE dependency call inside a collection.
//
// Parameters:
//   - budget time.Duration: the per-call bound. Non-positive falls back to the default, for
//     the same reason WithTickBudget does.
//
// Returns:
//   - *EventMetricsCollector: the collector, for chaining.
func (c *EventMetricsCollector) WithCallBudget(budget time.Duration) *EventMetricsCollector {
	if budget <= 0 {
		logrus.WithFields(logrus.Fields{
			"requested": budget.String(),
			"using":     DefaultMetricsCollectionCallBudget.String(),
		}).Warn("event metrics: a non-positive collection call budget was requested; using the default")

		c.callBudget = DefaultMetricsCollectionCallBudget

		return c
	}

	c.callBudget = budget

	return c
}

// WithInterval sets how often the collector recomputes the gauges.
//
// A non-positive interval falls back to the default rather than being rejected: a
// misconfigured cadence must not be able to stop the gauges being published, because the
// failure mode of that is silent — an alert that never fires.
//
// Parameters:
//   - interval time.Duration: the collection interval.
//
// Returns:
//   - *EventMetricsCollector: the collector, for chaining.
func (c *EventMetricsCollector) WithInterval(interval time.Duration) *EventMetricsCollector {
	if interval <= 0 {
		logrus.WithFields(logrus.Fields{
			"requested": interval.String(),
			"using":     DefaultMetricsCollectionInterval.String(),
		}).Warn("event metrics: a non-positive collection interval was requested; using the default")

		c.interval = DefaultMetricsCollectionInterval

		return c
	}

	c.interval = interval

	return c
}

// WithSubscriberBudget sets how many subscribers one tick may measure lag for.
//
// Parameters:
//   - budget int: the maximum. Non-positive falls back to the default, for the same
//     reason WithInterval does.
//
// Returns:
//   - *EventMetricsCollector: the collector, for chaining.
func (c *EventMetricsCollector) WithSubscriberBudget(budget int) *EventMetricsCollector {
	if budget <= 0 {
		logrus.WithFields(logrus.Fields{
			"requested": budget,
			"using":     DefaultSubscriberMetricsBudget,
		}).Warn("event metrics: a non-positive subscriber budget was requested; using the default")

		c.subscriberBudget = DefaultSubscriberMetricsBudget

		return c
	}

	c.subscriberBudget = budget

	return c
}

// Start begins collecting in the background.
//
// It collects ONCE IMMEDIATELY and then on every tick. Waiting a full interval before the
// first collection would leave the gauges absent from `/metrics` for that interval after
// every deployment, and an absent gauge is not the same as a zero one: a dashboard shows a
// gap and an alert expression over it evaluates to nothing at all.
//
// A second Start on a running collector is a no-op, matching LineageOutboxProcessor. Two
// loops writing the same gauges would interleave their measurements and their stale-series
// bookkeeping.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the loop.
func (c *EventMetricsCollector) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()

		return
	}
	c.running = true
	c.stopCh = make(chan struct{})
	interval := c.interval
	c.mu.Unlock()

	logrus.WithFields(logrus.Fields{
		"interval":          interval.String(),
		"subscriber_budget": c.subscriberBudget,
	}).Info("event metrics collector started")

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(ctx)
	}()
}

// Stop signals the loop to finish and waits for the in-flight tick to complete.
//
// Waiting matters rather than being tidy: a tick that is interrupted mid-way has published
// some gauges and not others, so the last values in the exporter would be a mix of two
// different instants.
func (c *EventMetricsCollector) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()

		return
	}
	c.running = false
	close(c.stopCh)
	c.mu.Unlock()

	c.wg.Wait()
	logrus.Info("event metrics collector stopped")
}

// IsRunning reports whether the loop is active.
//
// Returns:
//   - bool: true while the loop is running.
func (c *EventMetricsCollector) IsRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.running
}

// run is the collection loop.
func (c *EventMetricsCollector) run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// The first collection happens before the first tick; see Start.
	c.collectAndLog(ctx)

	for {
		select {
		case <-ctx.Done():
			logrus.Info("event metrics collector context cancelled")

			return
		case <-c.stopCh:
			logrus.Info("event metrics collector stop signal received")

			return
		case <-ticker.C:
			c.collectAndLog(ctx)
		}
	}
}

// collectAndLog runs one BOUNDED collection, publishes the collection-health signals, and logs
// whatever failed.
//
// # The bound is the point of this function
//
// The collection runs under a deadline of its own rather than on the process context. Without
// one, a dependency that accepts a connection and never answers blocks the loop for ever — and
// the damage is not the stall but its invisibility: every gauge holds its last value, so the
// whole event pipeline goes on being scraped as though it were current, and no value in any of
// those gauges could reveal it. The deadline abandons the tick, the ticker drops the ticks
// missed in the meantime, and the next one starts clean.
//
// # The health signals are recorded on EVERY tick, including a failed one
//
// The two are different facts and are recorded as such: the attempt timestamp always advances,
// so "is the loop running" is answerable; the success timestamp advances only when the tick had
// no failures at all, so "is it achieving anything" is answerable separately. A collector whose
// database is refusing connections ticks perfectly on schedule and publishes nothing — the
// first stays near zero and only the second rises, which is why one gauge could not carry both.
//
// The error is logged and discarded because there is nothing above this to handle it: the loop
// must continue whatever one tick reported, since stopping the collector on a transient failure
// would silence every gauge — including the backlog gauge that would have shown the problem.
func (c *EventMetricsCollector) collectAndLog(ctx context.Context) {
	bounded, cancel := context.WithTimeout(ctx, c.tickBudget)
	defer cancel()

	report, err := c.Collect(bounded)

	// RECORDED BEFORE THE LOGGING, and from the report rather than from err, so a tick that
	// timed out mid-way is still recorded as an attempt: the loop is demonstrably alive, and it
	// is the success age that is supposed to rise.
	metrics.RecordEventMetricsCollection(time.Now(), len(report.Failures) == 0)

	if err != nil {
		withLoggableCause(logrus.WithFields(report.LogFields()), err).Warn(
			"event metrics collection reported errors; the gauges it did reach were still published",
		)

		return
	}

	logrus.WithFields(report.LogFields()).Debug("event metrics collected")
}

// callContext derives the bound on ONE dependency call from the collection's own context.
//
// Derived rather than independent, so cancelling the collection cancels the call in flight;
// bounded separately, so one hung call cannot consume the whole tick and starve the four
// collections that do not depend on it. When the remaining tick budget is already shorter than
// the call budget, the parent's earlier deadline wins — context.WithTimeout takes the earlier of
// the two — which is the correct precedence and needs no arithmetic here.
//
// Parameters:
//   - ctx context.Context: the collection's context.
//
// Returns:
//   - context.Context: the bounded call context.
//   - context.CancelFunc: must be called by the caller, conventionally deferred.
func (c *EventMetricsCollector) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	budget := c.callBudget
	if budget <= 0 {
		budget = DefaultMetricsCollectionCallBudget
	}

	return context.WithTimeout(ctx, budget)
}

// recordCollectionFailure counts one failed collection under its fixed name.
//
// It exists so that a partially failed tick is visible in METRICS and not only in one log line.
// "The backlog is 0" and "the backlog could not be counted" are the same series otherwise, and
// the second is the one an operator has to act on.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - collection string: one of the closed collection literals declared in this file.
func recordCollectionFailure(ctx context.Context, collection string) {
	metrics.EventMetricsCollectionFailuresTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String("collection", collection),
	))
}

// EventMetricsReport is what one collection observed, for the caller's log line and for
// tests.
//
// It reports the FAILURES as well as the values, because a partially failed tick is the
// case that matters: knowing that the backlog was published and the lag was not is the
// difference between "consumer lag is zero" and "consumer lag is unknown", which no gauge
// can express on its own.
type EventMetricsReport struct {
	// CollectedAt is when the collection ran.
	CollectedAt time.Time

	// PendingBacklog is the value published to the backlog gauge: pending plus
	// processing rows.
	PendingBacklog int64

	// DeadLetterAge is the dead-letter age report, when that collection succeeded.
	DeadLetterAge DeadLetterAgeReport

	// SubscribersMeasured is how many registry rows had their lag measured.
	SubscribersMeasured int

	// SubscribersSkipped is how many rows were passed over because they have no
	// authorised topics or no usable identifiers — registered but not yet provisioned,
	// which is a legitimate state and not a failure.
	SubscribersSkipped int

	// LagSeriesPublished is how many lag series this tick published to the inventory the
	// asynchronous gauges observe.
	LagSeriesPublished int

	// LagSeriesCleared is how many series from the previous tick are absent from this
	// one, and so stop being exported because their subject is gone.
	//
	// It is a CHURN measurement rather than a description of work done. Retiring a series
	// takes no action under the asynchronous gauges — a series that is not observed is
	// simply not exported — so this counts what disappeared, which is the operationally
	// interesting figure and the one the previous synchronous implementation had to do
	// real work to achieve.
	LagSeriesCleared int

	// BudgetReached is true when the subscriber budget stopped the enumeration, meaning
	// some registered subscribers were not measured this tick. They are not lost: the next
	// tick resumes from the rotating cursor rather than restarting at the newest row.
	BudgetReached bool

	// SubscribersRegistered is how many rows the registry holds, read as an AGGREGATE at the
	// start of every sweep including the arms that measure nothing (SEC-10).
	//
	// It is the denominator the unmeasured count is judged against: three unmeasured out of
	// three thousand is a rounding error on coverage, three out of five means the lag signal
	// is mostly absent. Zero when the registry could not be counted, in which case a failure
	// is recorded — the count is an observation about an observation, so failing it must not
	// stop the measurement it describes.
	SubscribersRegistered int64

	// SubscribersUnmeasured is how many registered subscribers this tick published NO
	// consumer-lag reading for, summed across every reason.
	//
	// It exists for the LOG LINE and for a caller holding the report; the exported gauge is
	// attributed by reason, because the reason decides the remediation. The two agree by
	// construction: this is the sum of the same map publishSweepCoverage records.
	//
	// On the arms that measure nothing at all — no registry seam, or no configured broker — it
	// is the whole registry, because reporting zero there would be the same false reassurance
	// the coverage gauges exist to remove.
	SubscribersUnmeasured int64

	// LagPassComplete is true when this tick reached the END of the registry, which means
	// every registered subscriber has now been measured at least once since LagPassStartedAt.
	LagPassComplete bool

	// LagPassAgeSeconds is how long the IN-PROGRESS pass has been outstanding, and zero on a
	// tick that completed one. It is the value published to
	// metrics.SubscriberLagPassAgeSeconds, held here so a test can assert the number the
	// gauge received rather than infer it.
	LagPassAgeSeconds float64

	// LagPassStartedAt is when the current pass over the registry began. On a completed pass,
	// the interval between it and CollectedAt is how long a full sweep of the registry takes —
	// the staleness bound on the oldest lag reading being exported.
	LagPassStartedAt time.Time

	// SubscribersFailed is how many rows were examined and had their lag measurement REFUSED
	// by the broker. Kept apart from SubscribersSkipped because the two need different
	// actions: a skipped row is unprovisioned and the row is what needs fixing, while a
	// failed one is a broker or ACL fault.
	SubscribersFailed int

	// ListingFailed is true when the registry enumeration itself failed, so an unknown number
	// of subscribers were never reached. It is what stops a sweep claiming completeness on the
	// strength of rows it happened to see before the query broke.
	ListingFailed bool

	// SubscribersTopicMissing is how many rows were measured SUCCESSFULLY but name at least one
	// authorised topic that does not exist at the broker.
	//
	// Its own field rather than folded into SubscribersFailed, because the fault is somewhere
	// else entirely: the broker answered, the other topics measured normally, and what is wrong
	// is either the registry row's topic list or the topic provisioning. Untracked, this is the
	// quietest gap in the whole inventory — an absent topic contributes no lag AND no unreadable
	// partitions, so the subscriber is counted as measured and neither the lag rule nor the
	// partition-health rule has anything to fire on.
	SubscribersTopicMissing int

	// RegistrySize is how many rows the sweep observed in the registry, or -1 when it stopped
	// at the budget and never reached the end.
	RegistrySize int

	// SweepComplete is true only when this tick examined EVERY registered subscriber and
	// nothing was left unmeasured by a failure. It is the value published to
	// blnk.kafka.consumer_lag_inventory_complete, and it is the only thing that makes a
	// permanently unmeasured subscriber alertable — the lag inventory is whole-set, so such a
	// subscriber has no series to alert on at all.
	SweepComplete bool

	// RevocationsPending is how many subscribers still owe a broker-side credential
	// revocation — the value published to the revocation-count gauge.
	RevocationsPending int64

	// OldestRevocationAge is how long the OLDEST outstanding revocation has been owed, and
	// the value published to the revocation-age gauge. Zero when nothing is outstanding,
	// which is a reading rather than an absence.
	OldestRevocationAge time.Duration

	// SettlementBacklog is how much broker-side reconciliation subscribers owe, split by
	// kind, and when the oldest obligation was recorded — the values published to the four
	// settlement gauges. The zero value is a reading rather than an absence.
	SettlementBacklog model.SubscriberSettlementBacklog

	// AccessResidue is how much broker-side access is unaccounted for — orphaned credentials
	// and refused revocations — as published to the four residue gauges. Its zero value is a
	// reading (nothing outstanding) only when ResidueMeasured is true.
	AccessResidue model.SubscriberAccessResidue

	// ResidueMeasured distinguishes "nothing is outstanding" from "the residue could not be
	// read", which are the same four zeroes and must not be confused: publishing zeroes on
	// the strength of a failed read would assert that every credential is accounted for.
	ResidueMeasured bool

	// OldestSettlementAge is how long the OLDEST outstanding settlement obligation of either
	// kind has been owed, and the value published to the settlement-age gauge.
	OldestSettlementAge time.Duration

	// Failures are the per-collection errors. A non-empty slice accompanies the error
	// Collect returns.
	Failures []error
}

// LogFields renders the report as logrus fields.
//
// Returns:
//   - logrus.Fields: a fresh map the caller may extend.
func (r EventMetricsReport) LogFields() logrus.Fields {
	return logrus.Fields{
		"pending_backlog":           r.PendingBacklog,
		"dead_letter_age":           r.DeadLetterAge.OldestAge().String(),
		"dlt_outstanding":           r.DeadLetterAge.Outstanding,
		"dlt_rows_scanned":          r.DeadLetterAge.Scanned,
		"dlt_scan_truncated":        r.DeadLetterAge.Truncated,
		"subscribers_measured":      r.SubscribersMeasured,
		"subscribers_skipped":       r.SubscribersSkipped,
		"lag_series_published":      r.LagSeriesPublished,
		"lag_series_cleared":        r.LagSeriesCleared,
		"subscriber_budget_hit":     r.BudgetReached,
		"subscribers_registered":    r.SubscribersRegistered,
		"subscribers_unmeasured":    r.SubscribersUnmeasured,
		"subscribers_failed":        r.SubscribersFailed,
		"subscriber_listing_failed": r.ListingFailed,
		"subscribers_topic_missing": r.SubscribersTopicMissing,
		"registry_size":             r.RegistrySize,
		"sweep_complete":            r.SweepComplete,
		"revocations_pending":       r.RevocationsPending,
		"oldest_revocation_age":     r.OldestRevocationAge.String(),
		"failures":                  len(r.Failures),
	}
}

// Collect performs ONE collection of every gauge it owns and returns what it observed.
//
// It is exported so that a caller can drive a collection on demand — a test, or an
// operational endpoint — without owning a loop, and so that the loop itself has nothing in
// it but scheduling.
//
// # Every collection is attempted, whatever the others did
//
// The three are independent and are run independently: a database failure must not stop the
// consumer-lag measurement, and a broker failure must not stop the backlog count. This is
// the property that keeps the observability of a degraded system from degrading with it —
// the moment one dependency fails is precisely when the gauges over the others matter most.
// Failures are collected and returned together.
//
// Parameters:
//   - ctx context.Context: cancels every underlying query.
//
// Returns:
//   - EventMetricsReport: what was observed. Populated even alongside an error.
//   - error: a joined error naming every collection that failed, or nil.
func (c *EventMetricsCollector) Collect(ctx context.Context) (EventMetricsReport, error) {
	report := EventMetricsReport{CollectedAt: time.Now().UTC()}

	if c == nil {
		return report, errors.New("blnk: the event metrics collector is nil")
	}

	c.collectOutboxBacklog(ctx, &report)
	c.collectDeadLetterAge(ctx, &report)
	c.collectSubscriberRevocations(ctx, &report)
	c.collectSubscriberSettlement(ctx, &report)
	// THE UNACCOUNTED-ACCESS RESIDUE, and it is not a duplicate of the two steps above it. Those
	// report work the registry KNOWS is outstanding — a revocation pending, a settlement owed.
	// This reports the residue those two cannot see: a credential written at the broker whose
	// registry record could not be updated, and a revocation the broker REFUSED. Both leave a
	// principal that can still authenticate with nothing in the registry pointing at it, which is
	// the one failure in this subsystem that grants access rather than withholding it — so the
	// two orphan gauges and their alert thresholds are fed from here or from nowhere.
	c.collectSubscriberAccessResidue(ctx, &report)
	c.collectSubscriberLag(ctx, &report)

	if len(report.Failures) > 0 {
		return report, errors.Join(report.Failures...)
	}

	return report, nil
}

// collectOutboxBacklog records the relay's backlog: pending plus processing rows.
//
// PROCESSING ROWS ARE INCLUDED, and that is the whole reason this cannot be a count of the
// pending literal alone. A processing row has been claimed by a relay instance under a lease
// but not yet acknowledged by the broker, so it is still un-published work. Counting only
// pending rows would report a DRAINED BACKLOG at exactly the moment a stalled relay holds
// every claimable row under a lease — the failure this gauge exists to make visible would
// render as zero.
//
// A status with no rows is absent from the repository's map rather than present with a zero,
// so both reads are two-value reads and an empty outbox produces an explicit zero rather
// than no measurement at all.
func (c *EventMetricsCollector) collectOutboxBacklog(ctx context.Context, report *EventMetricsReport) {
	if c.outbox == nil {
		report.Failures = append(report.Failures,
			errors.New("blnk: the event metrics collector has no outbox datasource, so the backlog gauge cannot be published"))

		return
	}

	call, cancel := c.callContext(ctx)
	defer cancel()

	// The window bounds ONLY the dispatched count, which this gauge never reads: pending and
	// processing are always counted in full, so the backlog stays exact however much dispatched
	// history has accumulated behind it.
	counts, err := c.outbox.CountEventOutboxByStatus(call, time.Now().UTC().Add(-outboxBacklogCountWindow))
	if err != nil {
		report.Failures = append(report.Failures, fmt.Errorf("counting the event outbox by status: %w", err))
		recordCollectionFailure(ctx, collectionOutboxBacklog)

		return
	}

	backlog := counts[model.EventOutboxStatusPending] + counts[model.EventOutboxStatusProcessing]
	report.PendingBacklog = backlog

	if metrics.OutboxPendingBacklog == nil {
		return
	}

	// Recorded unconditionally, zero included: a drained outbox must publish 0 rather than
	// leave the previous backlog standing in the exporter.
	metrics.OutboxPendingBacklog.Record(ctx, backlog)
}

// collectDeadLetterAge refreshes the dead-letter age gauge by delegating to the dead-letter
// service.
//
// Delegation rather than reimplementation: the age has to be measured from the persisted
// dead_lettered_at, over dead-lettered rows only, with every topic Blnk owns published
// including the zeros. A second implementation of those three rules would be a second thing
// to keep correct, and the two would diverge exactly when one of them was fixed.
//
// A collector with no dead-letter service is not a failure. The service needs a datasource
// and, for its other operations, a transport; a deployment that has not wired one still
// wants its backlog gauge.
func (c *EventMetricsCollector) collectDeadLetterAge(ctx context.Context, report *EventMetricsReport) {
	if c.deadLetters == nil {
		return
	}

	call, cancel := c.callContext(ctx)
	defer cancel()

	ageReport, err := c.deadLetters.RefreshDeadLetterAgeGauge(call)
	if err != nil {
		report.Failures = append(report.Failures, fmt.Errorf("refreshing the dead-letter age gauge: %w", err))
		recordCollectionFailure(ctx, collectionDeadLetterAge)

		return
	}

	report.DeadLetterAge = ageReport
}

// collectSubscriberRevocations publishes how much broker-side credential revocation is
// outstanding and how old the oldest obligation is.
//
// # Why the collector, and why these were unreachable before
//
// Deregistration revokes at the broker and only then deletes the registry row, so a row still
// carrying revocation_pending_at is a principal that may still authenticate while nothing in
// Blnk records an issuance for it. Both gauges describing that were declared, initialised and
// NEVER RECORDED, which made the alert on the age of the oldest one permanently inactive with
// a healthy-looking rule — the precise failure mode where an alert that cannot fire is
// indistinguishable from a system with nothing wrong.
//
// Recording them here rather than in the deregistration path is what makes them true rather
// than merely written. A gauge maintained by the code that creates the obligation reports only
// what that code observed on its way past; the obligation persists in a row and outlives the
// request, so the reading has to come from the row. It is also the reading that survives a
// restart: the marker is durable and the gauge is not.
//
// # INDEPENDENT OF THE BROKER, deliberately
//
// Unlike consumer lag this needs no broker and is not skipped when none is configured. A
// marker is created by a deregistration whose revocation failed, and the commonest reason for
// that failure is precisely that the broker was unreachable — so refusing to measure it
// without a broker would hide the backlog exactly when it is growing.
//
// # A failed read publishes NOTHING
//
// Zero is published when the count really is zero, so that "nothing owed" is distinguishable
// from "the collector stopped". It is NOT published when the query failed, because a zero
// would then assert that everything is settled on the strength of a reading that does not
// exist. The failure is reported instead, and the previous value stands until the next tick.
func (c *EventMetricsCollector) collectSubscriberRevocations(ctx context.Context, report *EventMetricsReport) {
	if c.subscribers == nil {
		// No registry surface: there is nothing to read, and a zero would be an invention.
		c.publishCoverage(ctx, report)

		return
	}

	call, cancel := c.callContext(ctx)
	defer cancel()

	backlog, err := c.subscribers.CountSubscriberRevocationsPending(call)
	if err != nil {
		report.Failures = append(report.Failures,
			fmt.Errorf("counting outstanding subscriber revocations: %w", err))
		recordCollectionFailure(ctx, collectionRevocations)

		return
	}

	report.RevocationsPending = backlog.Pending
	report.OldestRevocationAge = backlog.OldestAge(report.CollectedAt)

	if metrics.SubscriberRevocationsPending != nil {
		metrics.SubscriberRevocationsPending.Record(ctx, backlog.Pending)
	}

	if metrics.OldestSubscriberRevocationAgeSeconds != nil {
		// Seconds, matching the instrument's declared unit and the alert expression's
		// threshold. The age is zero when nothing is outstanding — see
		// SubscriberRevocationBacklog.OldestAge for why the zero instant is tested rather
		// than subtracted, which would otherwise publish an age of fifty-odd years and pin
		// the alert permanently.
		metrics.OldestSubscriberRevocationAgeSeconds.Record(ctx, report.OldestRevocationAge.Seconds())
	}
}

// collectSubscriberSettlement publishes how much broker-side reconciliation subscribers owe and
// how old the oldest obligation is.
//
// # Why the collector owns this reading
//
// The obligation is a column on a row, raised by an operation that has since returned and
// possibly by a process that no longer exists. A gauge maintained where the obligation is CREATED
// would therefore report only what that code observed on its way past, and would be wrong the
// moment anything settled it, restarted, or raised one it never saw. Reading it from the row is
// the only measurement that is true after a restart — the marker is durable and the gauge is not.
//
// # INDEPENDENT OF THE BROKER, deliberately
//
// Like the revocation backlog and unlike consumer lag, this is not skipped when no broker is
// configured. Obligations are raised BY broker failures, and the commonest of those is the broker
// being unreachable — so declining to measure the backlog without a broker would hide it exactly
// when it is growing fastest.
//
// # A failed read publishes NOTHING
//
// Zero is published when the counts really are zero, so that "nothing owed" is distinguishable
// from "the collector stopped". It is NOT published when the query failed, because a zero would
// then assert that everything is settled on the strength of a reading that does not exist. The
// failure is reported and the previous value stands until the next tick.
func (c *EventMetricsCollector) collectSubscriberSettlement(ctx context.Context, report *EventMetricsReport) {
	if c.subscribers == nil {
		// No registry surface: there is nothing to read, and a zero would be an invention.
		return
	}

	backlog, err := c.subscribers.CountSubscriberSettlementObligations(ctx)
	if err != nil {
		report.Failures = append(report.Failures,
			fmt.Errorf("counting outstanding subscriber settlement obligations: %w", err))

		return
	}

	report.SettlementBacklog = backlog
	report.OldestSettlementAge = backlog.OldestAge(report.CollectedAt)

	if metrics.SubscriberSettlementOutstanding != nil {
		metrics.SubscriberSettlementOutstanding.Record(ctx, backlog.Outstanding)
	}

	if metrics.SubscriberGrantReconcilePending != nil {
		metrics.SubscriberGrantReconcilePending.Record(ctx, backlog.GrantReconcilePending)
	}

	if metrics.SubscriberCredentialCleanupPending != nil {
		metrics.SubscriberCredentialCleanupPending.Record(ctx, backlog.CredentialCleanupPending)
	}

	if metrics.OldestSubscriberSettlementAgeSeconds != nil {
		// Seconds, matching the instrument's declared unit and the alert expression's threshold.
		// The age is zero when nothing is outstanding — see
		// SubscriberSettlementBacklog.OldestAge for why the zero instant is tested rather than
		// subtracted, which would publish an age of fifty-odd years and pin the alert
		// permanently.
		metrics.OldestSubscriberSettlementAgeSeconds.Record(ctx, report.OldestSettlementAge.Seconds())
	}
}

// collectSubscriberLag measures consumer lag for every registered subscriber and publishes
// the result as the complete current inventory.
//
// # Why enumeration belongs here
//
// ConsumerLag measures ONE group when asked. Nothing asks it on a schedule, so without this
// the lag gauge only ever held whatever an operator's ad-hoc query happened to record —
// which is to say the consumer-lag alert could not fire for a subscriber nobody had thought
// to query. Enumerating the registry is what turns a diagnostic into a monitored quantity,
// and it is why this collector, alone, is allowed to publish: it is the only caller that
// knows the whole registry, and a whole-inventory publication from a caller that knows only
// part of it would retire every series it had not looked at.
//
// # Retirement is now a property of publishing, not a separate step
//
// The lag gauges are ASYNCHRONOUS: the SDK exports exactly the label tuples the callback
// observes on each collection, so publishing this tick's set is simultaneously the retirement
// of everything absent from it. A deleted subscriber, a narrowed grant or a reissued consumer
// group stops being observed and its series stops being exported — with nothing retained, no
// zero to write, and a series count equal to the current inventory rather than the union of
// every inventory the process has ever seen.
//
// That replaces an explicit zeroing pass. Zeroing stopped a departed subscriber's series
// ALERTING but could not stop it EXISTING, because a synchronous gauge has no delete and its
// aggregator retains every attribute set it has ever been written with. See
// metrics.SubscriberConsumerLag.
//
// An empty publication is meaningful and is made deliberately in every early return below: no
// registry, no broker, or no subscribers all mean nothing is currently measurable, and
// publishing nothing retires everything rather than leaving a deployment that has just lost
// its broker alerting on the last lag it happened to see.
func (c *EventMetricsCollector) collectSubscriberLag(ctx context.Context, report *EventMetricsReport) {
	if c.subscribers == nil {
		// No registry: nothing to enumerate, so the inventory is empty and every previously
		// exported series is retired.
		report.LagSeriesCleared = c.publishLagInventory(nil, true)

		// AND SAID SO. Nothing registered means nothing unmeasured, which is an honest zero and
		// is not the same fact as writing nothing: a gauge that only speaks when something is
		// wrong cannot distinguish this deployment from a collector that has stopped.
		//
		// The registry SIZE is not written here. collectSubscriberRevocations already publishes
		// it from its own no-registry arm on the same tick, and a second write of the same value
		// would double the series' sample count for no added fact.
		c.publishSweepCoverage(ctx, report, 0, 0)

		return
	}

	// READ BEFORE THE SWEEP, and read even on the paths that measure nothing.
	//
	// The registry total is what turns "the budget stopped me" into "this many subscribers
	// have no lag series at all", and the second is the only form an operator can act on. It
	// is also why this is not deferred to the arm that reaches the budget: a deployment with
	// no broker measures nothing, so EVERY registered subscriber is unmeasured, and reporting
	// zero there would be the same false reassurance in a different disguise.
	//
	// A failure to count is recorded and stepped past. The count is an observation about an
	// observation; refusing to measure lag because the registry could not be sized would
	// convert a reporting gap into a total one.
	registered, countErr := c.subscribers.CountEventSubscribers(ctx)
	if countErr != nil {
		report.Failures = append(report.Failures, fmt.Errorf("counting event subscribers: %w", countErr))
	} else {
		report.SubscribersRegistered = registered
	}

	if c.admin == nil || !c.admin.IsConfigured() {
		// No broker means no offsets to difference. This is the legitimate no-Kafka steady
		// state, not a failure — and the empty inventory is what stops a deployment that has
		// just lost its broker from continuing to export a stale lag.
		report.LagSeriesCleared = c.publishLagInventory(nil, true)

		// Nothing measured means the WHOLE registry is unmeasured, and saying so is the point:
		// reporting zero here would be the false reassurance the coverage gauges exist to remove.
		//
		// PUBLISHED, not merely tallied on the report. This arm used to set the field and return
		// without writing the gauge, so a deployment that lost its broker stopped writing the
		// one series that says the lag signal is gone — leaving whatever the last healthy tick
		// wrote standing, which reads as coverage. The inventory has just been cleared above, so
		// no subscriber has a series and the shortfall is the whole registry by construction.
		c.publishSweepCoverage(ctx, report, 0, int(report.SubscribersRegistered))
		c.publishCoverage(ctx, report)

		return
	}

	samples := []metrics.ConsumerLagSample{}

	// WHERE THIS TICK RESUMES. A nil cursor is the start of a pass, which is both the first
	// sweep and every wrap-around; a non-nil one is the KEYSET position the previous tick
	// stopped at, so the budget covers a different slice of the registry each time and every
	// subscriber is reached within one pass.
	//
	// Keyset rather than an offset, because the registry changes underneath a sweep that spans
	// several ticks: an offset silently skips a row when an older one is deregistered and
	// repeats one when a newer is registered, whereas resuming from the last row's
	// (created_at, id) coordinate is stable under both.
	cursor, passStartedAt := c.beginLagSweep()
	report.LagPassStartedAt = passStartedAt

	// A pass that begins at the top of the registry is the only one that can observe the
	// registry's SIZE, because a keyset resumption cannot know how much it has already walked.
	startedFresh := cursor == nil

	// The budget bounds rows EXAMINED, not rows successfully measured. Counting only
	// successes would let a registry whose every measurement fails page through itself
	// without limit — spending a broker round trip per row precisely when the broker is
	// the thing that is failing.
	examined := 0
	exhausted := false

	for examined < c.subscriberBudget {
		limit := subscriberMetricsPageSize
		if remaining := c.subscriberBudget - examined; remaining < limit {
			limit = remaining
		}

		call, cancel := c.callContext(ctx)
		page, err := c.subscribers.ListEventSubscribers(call, model.SubscriberPageQuery{
			Limit:  limit,
			Cursor: cursor,
		})
		cancel()

		if err != nil {
			report.Failures = append(report.Failures, fmt.Errorf("listing event subscribers: %w", err))
			recordCollectionFailure(ctx, collectionSubscriberListing)
			report.ListingFailed = true

			break
		}

		if len(page.Subscribers) == 0 {
			// Nothing left from here on: the pass has reached the end of the registry.
			exhausted = true

			break
		}

		examined += len(page.Subscribers)
		samples = c.measurePage(ctx, page.Subscribers, samples, report)

		cursor = page.NextCursor

		if !page.HasMore {
			exhausted = true

			break
		}
	}

	// A completed pass rewinds the cursor and restarts the clock, so the NEXT tick begins a
	// fresh pass from the newest subscriber — which is also what keeps a shrinking registry
	// from leaving the cursor stranded past its end. Advanced WHATEVER happened, including on
	// a listing failure, so a registry whose middle page is briefly unreadable is not swept
	// from the same place for ever.
	report.LagPassComplete = exhausted
	c.finishLagSweep(cursor, exhausted)

	if examined >= c.subscriberBudget && !exhausted {
		report.BudgetReached = true
		logrus.WithFields(logrus.Fields{
			"budget":     c.subscriberBudget,
			"examined":   examined,
			"measured":   report.SubscribersMeasured,
			"skipped":    report.SubscribersSkipped,
			"pass_since": passStartedAt.Format(time.RFC3339),
		}).Warn(
			"event metrics: the subscriber measurement budget was reached, so this tick covered part of " +
				"the registry; the next tick RESUMES from where it stopped, so coverage rotates and the " +
				"pass completes over several ticks — raise EVENT_METRICS_SUBSCRIBER_BUDGET to measure the " +
				"whole registry every tick",
		)
	}

	// COVERAGE IS PUBLISHED AS ITS OWN FACT, because absence cannot express it.
	//
	// The inventory is whole-set, so a subscriber this sweep did not reach has no series at
	// all — which is honest and completely silent. Nothing can alert per-subscriber on a
	// series that does not exist, so "every registered subscriber is being measured" is
	// published as one boolean with the shortfall beside it, broken down by reason.
	//
	// The registry size is knowable only on a tick that both STARTED a pass and reached its
	// end; -1 reports it as unknown, which is what makes the completeness gauge conservative
	// on every partial tick rather than optimistic on a number it cannot have.
	total := -1
	if startedFresh && exhausted && !report.ListingFailed {
		total = examined
	}

	report.RegistrySize = total
	report.SweepComplete = c.sweepWasComplete(report, examined, total)

	// PUBLISHED ONCE, AFTER THE WHOLE SWEEP, as the UNION of this tick's readings and the
	// still-fresh readings of the subscribers this tick did not reach.
	//
	// The union is what rotating coverage requires. Publishing only the slice just measured
	// would retire every other subscriber's series on every tick, so a registry larger than
	// one budget would export a lag series that appeared and vanished in turn — worse than the
	// gap it replaced, because a flapping series looks like a subscriber connecting and
	// disconnecting. Readings are retained under a TTL instead, so a series is exported while
	// it is still being refreshed and retired once it stops being, which is what a deleted
	// subscriber's series does on its own.
	report.LagSeriesPublished = len(samples)
	report.LagSeriesCleared = c.publishLagInventory(samples, exhausted)

	// COVERAGE IS PUBLISHED AFTER THE INVENTORY, and the order is load-bearing. The shortfall
	// is the registry size minus the DISTINCT SUBSCRIBERS CURRENTLY EXPORTED, so reading it
	// before the inventory was replaced would measure the PREVIOUS tick's export — which on the
	// first tick is empty, and so would report a whole registry as unmeasured on the one tick
	// that had just measured its first slice.
	c.publishSweepCoverage(ctx, report, examined, total)
	c.publishCoverage(ctx, report)

	c.publishLagCoverage(ctx, report, passStartedAt, exhausted)
}

// publishLagCoverage records how well the rotation is keeping up, alongside the lag readings
// themselves. PERF-P22.
//
// # Why coverage needs its own signal
//
// The lag gauges cannot report their own blind spots. A subscriber the rotation has not
// returned to within the reading TTL stops being exported, and an ABSENT series breaches no
// threshold — so the >10000 rule reports nothing wrong for exactly the subscribers it can no
// longer see. Rotation fixed the original defect, in which the oldest subscribers were never
// measured at all; without this it would leave a quieter version of the same thing, in which
// they are measured too rarely to alert on.
//
// # What the two numbers say
//
// The pass age is the age of the IN-PROGRESS rotation and resets to zero when one completes,
// so its maximum over a window is the rotation latency — the figure to compare against the
// reading TTL. Zero is published on a completed pass rather than the age it reached, because
// the fact being reported is "how long has the current rotation been outstanding", and a
// finished rotation has nothing outstanding.
//
// The covered count is the size of the exported inventory, so a value persistently below the
// registry size means subscribers are being skipped outright — no authorized topics, or
// identifiers the registry did not generate — rather than merely reached late.
//
// Recorded even when nothing was measurable, because the empty reading is the informative one:
// leaving the last value standing would report a healthy rotation for a deployment that has
// just lost its broker or its registry.
//
// Parameters:
//   - ctx context.Context: the collection context.
//   - report *EventMetricsReport: the tick's report, read for the published series count.
//   - passStartedAt time.Time: when the in-progress pass began.
//   - passComplete bool: whether this tick reached the end of the registry.
func (c *EventMetricsCollector) publishLagCoverage(
	ctx context.Context,
	report *EventMetricsReport,
	passStartedAt time.Time,
	passComplete bool,
) {
	var passAge float64
	if !passComplete && !passStartedAt.IsZero() {
		if age := time.Now().UTC().Sub(passStartedAt); age > 0 {
			passAge = age.Seconds()
		}
	}

	report.LagPassAgeSeconds = passAge

	if metrics.SubscriberLagPassAgeSeconds != nil {
		metrics.SubscriberLagPassAgeSeconds.Record(ctx, passAge)
	}
	if metrics.SubscriberLagCoveredSubscribers != nil {
		metrics.SubscriberLagCoveredSubscribers.Record(ctx, int64(c.coveredSubscriberCount()))
	}
}

// coveredSubscriberCount is how many DISTINCT subscribers currently have an exported lag
// series.
//
// Distinct SUBSCRIBERS rather than series, because the inventory carries one series per
// subscriber-topic pair: counting series would report a subscriber authorised on four topics
// as four covered subscribers and make the comparison against the registry size meaningless.
//
// Returns:
//   - int: the number of distinct subscribers in the currently exported inventory.
func (c *EventMetricsCollector) coveredSubscriberCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.publishedLagSeries) == 0 {
		return 0
	}

	subscribers := make(map[string]struct{}, len(c.publishedLagSeries))
	for series := range c.publishedLagSeries {
		subscribers[series.subscriber] = struct{}{}
	}

	return len(subscribers)
}

// beginLagSweep reads the position this tick resumes from and the instant the current pass
// began, starting a new pass when there is none in progress.
//
// Returns:
//   - *model.SubscriberCursor: the resume position, nil at the start of a pass.
//   - time.Time: when the current pass began.
func (c *EventMetricsCollector) beginLagSweep() (*model.SubscriberCursor, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lagPassStartedAt.IsZero() {
		c.lagPassStartedAt = time.Now().UTC()
	}

	if c.lagCursor == nil {
		// A pass is starting. Whatever the previous one saw is spent, and carrying it forward
		// would make the next completed pass retire nothing.
		c.lagPassSeen = map[lagSeries]struct{}{}
	}

	return c.lagCursor, c.lagPassStartedAt
}

// finishLagSweep records where the next tick resumes.
//
// A completed pass resets both the cursor and the pass clock, so coverage restarts from the
// newest subscriber. An incomplete one carries the cursor forward — and a nil cursor from an
// incomplete sweep, which is what a listing failure on the first page leaves behind, restarts
// the pass rather than silently pinning it.
//
// Parameters:
//   - cursor *model.SubscriberCursor: where the sweep stopped.
//   - complete bool: whether the sweep reached the end of the registry.
func (c *EventMetricsCollector) finishLagSweep(cursor *model.SubscriberCursor, complete bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if complete {
		c.lagCursor = nil
		c.lagPassStartedAt = time.Time{}

		return
	}

	c.lagCursor = cursor
}

// sweepWasComplete decides whether this tick measured EVERY registered subscriber.
//
// It is deliberately conservative: a sweep whose registry enumeration failed reports
// INCOMPLETE even if it happened to examine every row it did see, because the number of rows it
// never reached is unknown and claiming completeness on an unknown is the failure this signal
// exists to prevent.
//
// Parameters:
//   - report *EventMetricsReport: the report so far, read for its failure flags.
//   - covered int: rows this tick examined.
//   - total int: the registry size observed, or -1 when the sweep never reached the end.
//
// Returns:
//   - bool: true only when the whole registry was examined and nothing was left unmeasured by
//     a failure.
func (c *EventMetricsCollector) sweepWasComplete(report *EventMetricsReport, covered, total int) bool {
	if report.ListingFailed || report.BudgetReached || total < 0 {
		return false
	}

	// A measurement that FAILED leaves that subscriber with no series, and a measurement that
	// named a topic which does not exist leaves that topic with none, so the inventory is
	// incomplete in both cases even though the row was examined.
	return covered >= total && report.SubscribersFailed == 0 && report.SubscribersTopicMissing == 0
}

// publishSweepCoverage records the coverage gauges for this tick.
//
// EVERY reason value is written on every tick, zeros included. A reason that disappears from the
// export is indistinguishable from one that has been resolved, and the whole point of these two
// series is to make the unmeasured case observable rather than absent.
//
// Parameters:
//   - ctx context.Context: the recording context.
//   - report *EventMetricsReport: the tick's tallies.
//   - covered int: rows examined this tick.
//   - total int: the registry size observed, or -1 when unknown.
func (c *EventMetricsCollector) publishSweepCoverage(
	ctx context.Context,
	report *EventMetricsReport,
	covered, total int,
) {
	if metrics.ConsumerLagInventoryComplete != nil {
		complete := int64(0)
		if report.SweepComplete {
			complete = 1
		}
		metrics.ConsumerLagInventoryComplete.Record(ctx, complete)
	}

	// THE SHORTFALL IS MEASURED AGAINST THE EXPORTED INVENTORY, NOT AGAINST THIS TICK'S SLICE.
	//
	// The sweep ROTATES: one tick measures a slice of the registry and the readings for the
	// subscribers it did not reach are RETAINED under a TTL, so they still have a series and
	// SubscriberConsumerLagHigh can still fire for them. Subtracting this tick's coverage from
	// the registry size would therefore report every rotating deployment as missing most of its
	// lag signal, which is false and would train an operator to ignore the alert.
	//
	// What the coverage gauge is about is subscribers with NO SERIES AT ALL, so the figure is
	// the registry size minus the number of DISTINCT subscribers currently exported. In the
	// steady state that is zero however small the budget is; it becomes non-zero exactly when
	// the rotation cannot get back to a subscriber inside the reading TTL, which is the
	// condition worth paging on.
	//
	// The size compared against is the registry AGGREGATE, counted on this same tick, rather
	// than the row count the sweep happened to observe — the aggregate is knowable on every
	// tick, including one that stopped at its budget, and reporting the gap as unknown there
	// would blind the gauge on precisely the ticks that produce one. It is zero when the count
	// itself failed, and no shortfall is claimed from a size that was never read.
	//
	// # OBS-07: THE GAPS THIS TICK ALREADY EXPLAINED ARE SUBTRACTED, OR THEY ARE COUNTED TWICE
	//
	// registered minus exported is the count of subscribers with NO SERIES AT ALL — every one
	// of them, whatever the reason. The reasons below then count three of those populations
	// AGAIN by name: a row with no authorised topics (unprovisioned), a row whose measurement
	// the broker refused (measure_failed), and a row naming a topic that does not exist
	// (topic_missing). Assigning the whole difference to a fourth reason therefore
	// DOUBLE-COUNTED every explained gap, and the aggregate exceeded the registry it is
	// supposed to be a subset of: three registered subscribers with one unprovisioned exported
	// unprovisioned=1 AND budget=1 — two unmeasured subscribers out of three, for one gap — and
	// a broker outage with two registered subscribers exported measure_failed=2 AND budget=2,
	// a total of four against a registry of two.
	//
	// Both consequences were operational, not cosmetic. SubscriberLagCoverageIncomplete's
	// remediation branches on this label, so 'budget' sent an operator to raise
	// EVENT_METRICS_SUBSCRIBER_BUDGET for what was actually a broker fault or an ordinary
	// un-granted subscriber — with, in the observed case, 189 subscribers of headroom still
	// unused. And the documented proportion query, unmeasured over registered, read 200%.
	//
	// So what is left for 'budget' is the RESIDUE: gaps this tick could not explain any other
	// way, which is exactly the rotation not getting back to a subscriber inside the reading
	// TTL. The subtraction is safe because measurePage puts every examined row in exactly one
	// of skipped, failed or measured, and topic_missing is a subset of measured, so the three
	// subtrahends sum to at most the number of rows examined and therefore at most the registry
	// size. Flooring at zero holds the invariant Σ(unmeasured) ≤ registered on the one shape
	// where the terms disagree: a subscriber counted topic_missing whose OTHER topics still
	// export a series is both covered and explained, so the arithmetic can go negative without
	// any gap existing at all.
	//
	// The same rule the ListingFailed exclusion below already stated, applied to the three
	// reasons it did not cover.
	budgetShortfall := 0
	explained := report.SubscribersSkipped + report.SubscribersFailed + report.SubscribersTopicMissing
	if shortfall := int(report.SubscribersRegistered) - c.coveredSubscriberCount() - explained; shortfall > 0 {
		budgetShortfall = shortfall
	}

	// covered and total remain the SWEEP's figures and are what SweepComplete is derived from:
	// "did this tick reach the end of the registry" is a different question from "does every
	// subscriber have a series", and conflating them is what made one gauge answer neither.
	_ = covered
	_ = total

	// THE REASON THE SHORTFALL IS ATTRIBUTED TO, which is the whole value of the label: only
	// 'budget' is answered by configuration, so an operator who reads the aggregate alone is as
	// likely to raise a budget as to fix the broken query that actually caused it.
	shortfallReason := metrics.SubscribersUnmeasuredReasonBudget
	if report.ListingFailed {
		// The enumeration broke, so the rows past the failure were never reached — the same
		// consequence as the budget with a completely different remedy. The shortfall is
		// reported under THIS reason rather than added to the budget one, or one gap would be
		// counted twice and the aggregate would exceed the registry.
		shortfallReason = metrics.SubscribersUnmeasuredReasonRegistryFailed
	}

	unmeasured := map[string]int64{
		metrics.SubscribersUnmeasuredReasonBudget:         0,
		metrics.SubscribersUnmeasuredReasonUnprovisioned:  int64(report.SubscribersSkipped),
		metrics.SubscribersUnmeasuredReasonMeasureFailed:  int64(report.SubscribersFailed),
		metrics.SubscribersUnmeasuredReasonRegistryFailed: 0,
		metrics.SubscribersUnmeasuredReasonTopicMissing:   int64(report.SubscribersTopicMissing),
	}
	unmeasured[shortfallReason] = int64(budgetShortfall)

	// The report's single total and the attributed gauge are the SAME number by construction,
	// summed from the one map, so the log line and the exported series can never disagree
	// about how much of the lag signal is missing.
	report.SubscribersUnmeasured = 0
	for _, count := range unmeasured {
		report.SubscribersUnmeasured += count
	}

	// Nil-guarded AFTER the total, because the report is logged and returned to a caller
	// whether or not telemetry was ever initialised.
	if metrics.SubscribersUnmeasured == nil {
		return
	}

	for _, reason := range metrics.SubscriberUnmeasuredReasons() {
		metrics.SubscribersUnmeasured.Record(ctx, unmeasured[reason], otelmetric.WithAttributes(
			attribute.String("reason", reason),
		))
	}
}

// publishCoverage publishes how large the registry is, so the unmeasured count can be read as
// a proportion. SEC-10.
//
// # The condition it makes visible
//
// A subscriber the sweep did not reach has NO consumer-lag series. blnk_kafka_consumer_lag is
// absent for it, so the SubscriberConsumerLagHigh rule has nothing to evaluate and the
// subscriber can fall arbitrarily far behind while every dashboard and every alert reads
// clean. That is the worst property a monitoring system can have — silence and health are
// indistinguishable — and until this existed the only trace of it was a WARN log line, which
// nobody is watching at the moment it starts to matter.
//
// # Published UNCONDITIONALLY, from every arm
//
// It is called from every arm of collectSubscriberLag, including the two that measure nothing
// at all, because a registry size that is only written when a sweep ran cannot distinguish a
// small registry from a stopped collector. The shortfall then reads as a proportion, and the
// measurement budget's headroom is visible BEFORE it is exhausted rather than only after.
//
// The unmeasured COUNT is not published here: it is exported ATTRIBUTED BY REASON from
// publishSweepCoverage, because the reason decides the remediation and only 'budget' is
// answered by configuration. Summing the reasons away recovers this number.
//
// Parameters:
//   - ctx context.Context: passed to the instrument.
//   - report *EventMetricsReport: read for the registry size, already counted.
func (c *EventMetricsCollector) publishCoverage(ctx context.Context, report *EventMetricsReport) {
	// Nil-guarded because metrics.Init may not have run: every other recording site in this
	// file does the same, and a collector that panicked because telemetry was not initialised
	// would take down the process it is observing.
	if metrics.SubscribersRegistered != nil {
		metrics.SubscribersRegistered.Record(ctx, report.SubscribersRegistered)
	}
}

// publishLagInventory replaces the exported consumer-lag inventory and reports the churn.
//
// It is the ONE place the inventory is published, so the whole-set semantics cannot be
// bypassed by a caller holding only part of the registry, and the retained set is tracked here
// so the churn figure and the publication cannot disagree.
//
// # Why the union, and why a TTL (PERF-P22)
//
// The sweep is budgeted and ROTATES, so one tick measures a slice of the registry rather than
// all of it. Publishing only that slice would retire every other subscriber's series on every
// tick: a registry larger than one budget would export each series in turn, appearing and
// vanishing, which reads as subscribers connecting and disconnecting and is worse than the
// coverage gap it replaced.
//
// So a reading is RETAINED until it goes stale. The TTL is what keeps that honest: a series
// whose subscriber has been deleted simply stops being measured, and it drops out once its
// last reading ages past the TTL — no explicit deletion, and no series exported for ever on
// the strength of a reading nobody is refreshing. lagReadingTTL is set well above the time a
// full pass takes at the default budget, so a healthy rotation never expires a series it is
// about to refresh.
//
// Passing nil retires the WHOLE inventory immediately, discarding the retained set. That is
// the deliberate reading of "nothing is measurable": no registry, or no broker, means a
// deployment that has just lost its Kafka must not go on exporting the last lag it happened to
// see.
//
// # A COMPLETED pass retires exactly, and immediately
//
// The TTL is the fallback, not the mechanism. When the sweep has covered the WHOLE registry —
// which is every tick for any registry that fits inside one budget, and the end of a rotation
// for one that does not — the set of series measured during that pass is authoritative: a
// retained series it did not see belongs to a subscriber that has been deregistered, or to a
// topic that has been dropped from a grant, and it is retired on the spot. Waiting out the TTL
// there would keep exporting a lag for something that no longer exists, and an alert reading it
// would go on firing about a subscriber nobody can fix.
//
// Parameters:
//   - samples []metrics.ConsumerLagSample: the readings measured on THIS tick. Nil or empty
//     retires everything.
//   - passComplete bool: whether the sweep has now covered the whole registry. When true, every
//     retained series absent from the pass is retired without waiting for the TTL.
//
// Returns:
//   - int: how many series from the previous publication are absent from this one, and so stop
//     being exported.
func (c *EventMetricsCollector) publishLagInventory(
	samples []metrics.ConsumerLagSample,
	passComplete bool,
) int {
	now := time.Now().UTC()

	c.mu.Lock()

	previous := c.publishedLagSeries

	if len(samples) == 0 {
		// Nothing is measurable: discard the retained readings rather than keeping them
		// alive on a TTL that would outlast the condition.
		c.lagReadings = nil
		c.lagPassSeen = nil
		c.publishedLagSeries = map[lagSeries]struct{}{}
		c.mu.Unlock()

		metrics.PublishConsumerLagInventory(nil)

		return len(previous)
	}

	if c.lagReadings == nil {
		c.lagReadings = make(map[lagSeries]lagReading, len(samples))
	}
	if c.lagPassSeen == nil {
		c.lagPassSeen = make(map[lagSeries]struct{}, len(samples))
	}

	for _, sample := range samples {
		series := seriesOf(sample)
		c.lagReadings[series] = lagReading{sample: sample, measuredAt: now}
		c.lagPassSeen[series] = struct{}{}
	}

	published := make([]metrics.ConsumerLagSample, 0, len(c.lagReadings))
	current := make(map[lagSeries]struct{}, len(c.lagReadings))

	for series, reading := range c.lagReadings {
		if _, seen := c.lagPassSeen[series]; passComplete && !seen {
			// The pass covered the whole registry and never measured this series, so
			// whatever it described is gone. Retired now rather than at the TTL, because
			// the collector KNOWS rather than merely suspects.
			delete(c.lagReadings, series)

			continue
		}

		if now.Sub(reading.measuredAt) > lagReadingTTL {
			// Nobody is refreshing this series. Its subscriber has been deleted, or the
			// registry is larger than the rotation can keep current — either way, exporting
			// a reading this old as though it were live would be a claim the collector
			// cannot support.
			delete(c.lagReadings, series)

			continue
		}

		published = append(published, reading.sample)
		current[series] = struct{}{}
	}

	if passComplete {
		// The pass is spent. The next one starts from an empty set, so it can retire in turn.
		c.lagPassSeen = map[lagSeries]struct{}{}
	}

	c.publishedLagSeries = current
	c.mu.Unlock()

	metrics.PublishConsumerLagInventory(published)

	retired := 0
	for series := range previous {
		if _, still := current[series]; !still {
			retired++
		}
	}

	if retired > 0 {
		logrus.WithField("series", retired).Debug(
			"event metrics: consumer-lag series whose subscriber is no longer measured were retired; " +
				"an unobserved asynchronous gauge series stops being exported, so nothing is left alerting",
		)
	}

	return retired
}

// seriesOf is the label tuple a sample is exported under, in one place so the retained-reading
// key and the published attribute set cannot drift apart.
//
// Parameters:
//   - sample metrics.ConsumerLagSample: the reading.
//
// Returns:
//   - lagSeries: its identity.
func seriesOf(sample metrics.ConsumerLagSample) lagSeries {
	return lagSeries{
		subscriber: sample.Subscriber,
		group:      sample.Group,
		topic:      sample.Topic,
	}
}

// measureSubscriber measures one subscriber's lag and appends the samples it produced.
//
// It APPENDS to the caller's accumulator and returns it, rather than publishing, because the
// inventory the asynchronous gauges observe is replaced as a whole: publishing per subscriber
// would retire every other subscriber's series on each call. See collectSubscriberLag.
//
// A subscriber with no authorised topics is SKIPPED rather than measured. That is the
// fail-closed default of a freshly registered row — authorised for nothing — and measuring
// it would publish a zero-lag series for a subscriber that is consuming nothing, which reads
// as a healthy consumer rather than as an unprovisioned one.
//
// A subscriber whose identifiers are not registry-issued is also skipped, and its skip is
// LOUD. The gauge would collapse such a value to a token anyway, so measuring it would merge
// several unrelated subscribers into one series; the row itself is the thing to fix.
//
// Parameters:
//   - ctx context.Context
//   - subscriber model.EventSubscriber: the registry row to measure.
//   - samples []metrics.ConsumerLagSample: the accumulator so far.
//   - report *EventMetricsReport: updated with the measured, skipped and failure tallies.
//
// Returns:
//   - []metrics.ConsumerLagSample: the accumulator, with this subscriber's samples appended.
//     Returned unchanged when the row was skipped or its measurement failed.
func (c *EventMetricsCollector) measureSubscriber(
	ctx context.Context,
	subscriber model.EventSubscriber,
) lagMeasurement {
	if len(subscriber.AuthorizedTopics) == 0 {
		return lagMeasurement{skipped: true}
	}

	if !isRegistrySubscriberIdentifier(subscriber.SubscriberID) ||
		!isRegistryConsumerGroupID(subscriber.ConsumerGroupID) {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash":  subscriberLogLabel(subscriber.SubscriberID),
			"consumer_group_hash": consumerGroupLogLabel(subscriber.ConsumerGroupID),
		}).Warn(
			"event metrics: a registry row does not carry generated identifiers, so its lag is not measured; " +
				"measuring it would merge it with every other non-conforming row into one series",
		)

		return lagMeasurement{skipped: true}
	}

	call, cancel := c.callContext(ctx)
	defer cancel()

	lagReport, err := c.admin.ConsumerLag(call, ConsumerLagRequest{
		SubscriberID: subscriber.SubscriberID,
		GroupID:      subscriber.ConsumerGroupID,
		Topics:       subscriber.AuthorizedTopics,
	})
	if err != nil {
		// NO SAMPLES from a failed measurement, which is deliberate: the series is retired
		// rather than left standing at its last reading. A lag nobody could verify this tick
		// is not evidence of anything, and a missing series is visibly missing where a stale
		// one reads as current.
		return lagMeasurement{failure: fmt.Errorf(
			"measuring consumer lag for subscriber %s: %w", subscriber.SubscriberID, err,
		)}
	}

	// A TOPIC THAT DOES NOT EXIST is recorded as its own kind of incompleteness.
	//
	// The measurement succeeded, so this row is counted as measured and its other topics carry
	// real lag readings. But an absent topic yields neither a lag sample nor an unreadable
	// partition, so without this the omission is invisible to every rule: the subscriber appears
	// fully measured and the missing topic simply has no series. The admin client already logs
	// which topics they were; this is what makes the condition alertable.
	// Carried on the measurement rather than tallied here, because this runs on one of the
	// page's goroutines: the counters are folded into the report afterwards, on one goroutine.
	topicMissing := len(lagReport.MissingTopics) > 0

	// The report renders its own samples, resolving every label through the same helpers the
	// instrument contract requires — so the collector cannot label a series differently from
	// the way ConsumerLag would have. A topic whose partitions could not all be read yields a
	// sample marked incomplete, which contributes measurement health and NO lag reading.
	return lagMeasurement{measured: true, topicMissing: topicMissing, samples: lagReport.LagSamples()}
}

// lagMeasurement is one subscriber's measurement outcome, as a value.
//
// measureSubscriber returns this rather than mutating the report and the sample slice in
// place, which is what lets a page be measured CONCURRENTLY: the goroutines write only their
// own element of a pre-sized slice, and the merge back into the shared report happens on one
// goroutine afterwards. Sharing the report directly would need a mutex around every field of
// it, and would make the order failures are recorded in depend on which broker call finished
// first.
//
// Exactly one of skipped, measured and failure is set.
type lagMeasurement struct {
	// samples are the per-topic lag samples, present only on a successful measurement.
	samples []metrics.ConsumerLagSample

	// skipped is true for a row that is registered but not measurable: no authorized topics,
	// or identifiers the registry did not generate. A legitimate state, not a failure.
	skipped bool

	// measured is true when the broker answered.
	measured bool

	// topicMissing is true when the broker answered but one of the subscriber's authorized
	// topics does not exist. The row still counts as measured — its other topics carry real
	// readings — and this is what makes the omission alertable rather than merely absent.
	topicMissing bool

	// failure is the measurement error, if the broker did not answer.
	failure error
}

// measurePage measures one page of subscribers with BOUNDED CONCURRENCY and merges the results
// into the report in page order. PERF-P22.
//
// # Why this is concurrent
//
// Each measurement is one or more broker round trips, and sequentially the whole tick costs
// the budget multiplied by the round-trip time. That product is what decides how much of the
// registry one tick can cover, and therefore how long a full rotation takes — so on a
// remote or loaded broker a sequential sweep is the binding constraint on coverage, not the
// budget. Measuring a page concurrently spends the same number of round trips in a fraction of
// the wall time, without asking the broker for any more work than before.
//
// # Why it is bounded
//
// The concurrency is fixed and small, following the house semaphore pattern. An unbounded
// fan-out over a page would open one connection per subscriber against a broker that may
// already be the thing failing, which is how an observability sweep becomes the cause of the
// outage it is measuring. The bound also holds the peak connection count independent of the
// page size and of the registry's growth.
//
// # Why the merge is ordered
//
// Results are collected into a pre-sized slice indexed by position and merged afterwards on
// this goroutine, so the report's counters and its failure list come out identical to the
// sequential version regardless of which measurement finished first. Ordering that depended on
// completion order would make a failure list non-deterministic, and an assertion over it would
// pass or fail according to broker timing.
//
// Parameters:
//   - ctx context.Context: the collection context, shared by every measurement.
//   - subscribers []model.EventSubscriber: the page to measure.
//   - samples []metrics.ConsumerLagSample: the accumulating inventory.
//   - report *EventMetricsReport: the tick's report, updated with counts and failures.
//
// Returns:
//   - []metrics.ConsumerLagSample: samples with this page's readings appended.
func (c *EventMetricsCollector) measurePage(
	ctx context.Context,
	subscribers []model.EventSubscriber,
	samples []metrics.ConsumerLagSample,
	report *EventMetricsReport,
) []metrics.ConsumerLagSample {
	if len(subscribers) == 0 {
		return samples
	}

	results := make([]lagMeasurement, len(subscribers))

	var wait sync.WaitGroup

	semaphore := make(chan struct{}, lagMeasurementConcurrency)

	for i := range subscribers {
		wait.Add(1)

		go func(index int) {
			defer wait.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			results[index] = c.measureSubscriber(ctx, subscribers[index])
		}(i)
	}

	wait.Wait()

	for i := range results {
		switch {
		case results[i].failure != nil:
			report.Failures = append(report.Failures, results[i].failure)
			report.SubscribersFailed++
			recordCollectionFailure(ctx, collectionSubscriberLag)
		case results[i].skipped:
			report.SubscribersSkipped++
		case results[i].measured:
			report.SubscribersMeasured++

			if results[i].topicMissing {
				report.SubscribersTopicMissing++
			}

			samples = append(samples, results[i].samples...)
		}
	}

	return samples
}

// collectSubscriberAccessResidue publishes how much broker-side access is UNACCOUNTED FOR:
// credentials that outlived their registry record, and revocations the broker refused.
//
// # ORPHAN-01: the exposure the revocation gauge could not see
//
// Provisioning writes a subscriber's SCRAM credential BEFORE its ACL bindings, because a binding
// for a principal that does not exist is inert while a credential with no bindings still
// AUTHENTICATES. So a failure to record the issuance is compensated by revoking the credential —
// and when that compensation also fails, a means of authenticating to the event bus exists for a
// principal the registry records no issuance for.
//
// That state was previously represented by a log line and nothing else. It is not in the
// revocation backlog, because the issuance path never stamps revocation_pending_at, so the
// existing gauge and its critical alert were STRUCTURALLY unable to fire on it: an exposure with
// no metric is an exposure nobody is watching, and it is indistinguishable from a healthy system.
//
// The second figure — refused revocations — is a subset of the pending rows, separated because
// the remedies differ. A refusal means the administrative principal's grants or the broker's
// reachability must be fixed before ANY retry can succeed; a pending row that merely started
// needs nothing but the retry. One count cannot tell an operator which they are looking at.
//
// # Independent of the broker, deliberately
//
// Like the revocation backlog and unlike consumer lag, this needs no broker and is not skipped
// when none is configured. The commonest cause of both markers is precisely that the broker was
// unreachable, so declining to measure them without one would hide the backlog exactly while it
// grew.
//
// # A failed read publishes NOTHING
//
// Four zeroes are published when the counts really are zero, so "everything is accounted for" is
// distinguishable from "the collector stopped". They are NOT published when the query failed,
// because zeroes would then assert that no credential is unaccounted for on the strength of a
// reading that does not exist. ResidueMeasured on the report carries that distinction for a
// caller inspecting it directly.
func (c *EventMetricsCollector) collectSubscriberAccessResidue(ctx context.Context, report *EventMetricsReport) {
	if c.subscribers == nil {
		// No registry surface: there is nothing to read, and a zero would be an invention.
		return
	}

	residue, err := c.subscribers.CountSubscriberAccessResidue(ctx)
	if err != nil {
		report.Failures = append(report.Failures,
			fmt.Errorf("counting unaccounted subscriber access: %w", err))

		return
	}

	report.AccessResidue = residue
	report.ResidueMeasured = true

	if metrics.SubscriberCredentialOrphans != nil {
		metrics.SubscriberCredentialOrphans.Record(ctx, residue.OrphanedCredentials)
	}

	if metrics.OldestSubscriberCredentialOrphanAgeSeconds != nil {
		// Seconds, matching the instrument's unit and the alert threshold. The age is zero when
		// nothing is outstanding — the zero instant is tested rather than subtracted, which would
		// otherwise publish an age of fifty-odd years and pin the alert permanently.
		metrics.OldestSubscriberCredentialOrphanAgeSeconds.Record(
			ctx, residue.OldestOrphanAge(report.CollectedAt).Seconds(),
		)
	}

	if metrics.SubscriberRevocationFailures != nil {
		metrics.SubscriberRevocationFailures.Record(ctx, residue.FailedRevocations)
	}

	if metrics.OldestSubscriberRevocationFailureAgeSeconds != nil {
		metrics.OldestSubscriberRevocationFailureAgeSeconds.Record(
			ctx, residue.OldestFailedRevocationAge(report.CollectedAt).Seconds(),
		)
	}
}
