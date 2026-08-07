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
// Without this file the three gauges are declared, initialised, documented and never
// recorded: `/metrics` carries no series for them, and the two acceptance-criterion alerts
// — dead-letter age above 900 seconds and consumer lag above 10,000 messages — cannot fire
// at all, whatever the system is actually doing.
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
// Until that call exists the three gauges have no maintainer and the two alerts in
// alerts/blnk-kafka-alerts.yml cannot fire, which is the precise defect this file was added
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
)

// eventMetricsOutboxStore is the repository surface the backlog gauge needs, and nothing
// else.
//
// One method. The backlog is a COUNT BY STATUS and not a scan, deliberately: the count is
// answered by an index-only aggregate, whereas walking rows to size a queue would make the
// cost of observing the backlog grow with the backlog — worst exactly when the system is
// already struggling.
type eventMetricsOutboxStore interface {
	// CountEventOutboxByStatus returns a status-keyed count of every outbox row. A status
	// with no rows is ABSENT from the map rather than present with a zero, which is what
	// makes the two-value read below mandatory.
	CountEventOutboxByStatus(ctx context.Context) (map[string]int64, error)
}

// eventMetricsSubscriberStore is the registry surface the lag gauge needs.
//
// Paging rather than "give me all of them": the collector applies a cardinality budget, and
// a paged read lets it stop at the budget instead of loading a registry it will not measure.
type eventMetricsSubscriberStore interface {
	// ListEventSubscribers pages the registry.
	ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error)
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

// EventMetricsCollector recomputes the event pipeline's three gauges on a fixed interval.
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

	// publishedLagSeries is the set of lag series written on the PREVIOUS tick. It is
	// what makes a disappeared subscriber's series clearable: without it, a deleted
	// subscriber's last reading would stay in the exporter and go on alerting forever.
	publishedLagSeries map[lagSeries]struct{}

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
		publishedLagSeries: map[lagSeries]struct{}{},
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

	return collector
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

// collectAndLog runs one collection and logs whatever failed.
//
// The error is logged and discarded here because there is nothing above this to handle it:
// the loop must continue whatever one tick reported, since stopping the collector on a
// transient database or broker failure would silence every gauge — including the backlog
// gauge that would have shown the problem.
func (c *EventMetricsCollector) collectAndLog(ctx context.Context) {
	report, err := c.Collect(ctx)
	if err != nil {
		logrus.WithError(err).WithFields(report.LogFields()).Warn(
			"event metrics collection reported errors; the gauges it did reach were still published",
		)

		return
	}

	logrus.WithFields(report.LogFields()).Debug("event metrics collected")
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
	// some registered subscribers were not measured this tick.
	BudgetReached bool

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
		"pending_backlog":       r.PendingBacklog,
		"dead_letter_age":       r.DeadLetterAge.OldestAge().String(),
		"dlt_outstanding":       r.DeadLetterAge.Outstanding,
		"dlt_rows_scanned":      r.DeadLetterAge.Scanned,
		"dlt_scan_truncated":    r.DeadLetterAge.Truncated,
		"subscribers_measured":  r.SubscribersMeasured,
		"subscribers_skipped":   r.SubscribersSkipped,
		"lag_series_published":  r.LagSeriesPublished,
		"lag_series_cleared":    r.LagSeriesCleared,
		"subscriber_budget_hit": r.BudgetReached,
		"failures":              len(r.Failures),
	}
}

// Collect performs ONE collection of all three gauges and returns what it observed.
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

	counts, err := c.outbox.CountEventOutboxByStatus(ctx)
	if err != nil {
		report.Failures = append(report.Failures, fmt.Errorf("counting the event outbox by status: %w", err))

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

	ageReport, err := c.deadLetters.RefreshDeadLetterAgeGauge(ctx)
	if err != nil {
		report.Failures = append(report.Failures, fmt.Errorf("refreshing the dead-letter age gauge: %w", err))

		return
	}

	report.DeadLetterAge = ageReport
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
		report.LagSeriesCleared = c.publishLagInventory(nil)

		return
	}

	if c.admin == nil || !c.admin.IsConfigured() {
		// No broker means no offsets to difference. This is the legitimate no-Kafka steady
		// state, not a failure — and the empty inventory is what stops a deployment that has
		// just lost its broker from continuing to export a stale lag.
		report.LagSeriesCleared = c.publishLagInventory(nil)

		return
	}

	samples := []metrics.ConsumerLagSample{}

	// The budget bounds rows EXAMINED, not rows successfully measured. Counting only
	// successes would let a registry whose every measurement fails page through itself
	// without limit — spending a broker round trip per row precisely when the broker is
	// the thing that is failing.
	examined := 0

	for offset := 0; examined < c.subscriberBudget; {
		limit := subscriberMetricsPageSize
		if remaining := c.subscriberBudget - examined; remaining < limit {
			limit = remaining
		}

		page, err := c.subscribers.ListEventSubscribers(ctx, limit, offset)
		if err != nil {
			report.Failures = append(report.Failures, fmt.Errorf("listing event subscribers: %w", err))

			break
		}
		if len(page) == 0 {
			break
		}
		offset += len(page)

		for i := range page {
			examined++
			samples = c.measureSubscriber(ctx, page[i], samples, report)
		}

		if len(page) < limit {
			// A short page is the end of the registry: the repository fills a page
			// whenever it can.
			break
		}
	}

	if examined >= c.subscriberBudget {
		report.BudgetReached = true
		logrus.WithFields(logrus.Fields{
			"budget":   c.subscriberBudget,
			"examined": examined,
			"measured": report.SubscribersMeasured,
			"skipped":  report.SubscribersSkipped,
		}).Warn(
			"event metrics: the subscriber measurement budget was reached, so some registered subscribers " +
				"were not measured this tick; raise the budget or reduce the registry",
		)
	}

	// PUBLISHED ONCE, AFTER THE WHOLE SWEEP, and unconditionally — including when the sweep
	// broke early on a listing failure or stopped at the budget. A partial inventory retires
	// the series it did not reach, which is the correct trade: an un-refreshed series would
	// otherwise be exported at a reading that is no longer being verified, and a missing
	// series is visibly missing while a stale one is not.
	report.LagSeriesPublished = len(samples)
	report.LagSeriesCleared = c.publishLagInventory(samples)
}

// publishLagInventory replaces the exported consumer-lag inventory and reports the churn.
//
// It is the ONE place the inventory is published, so the whole-set semantics cannot be
// bypassed by a caller that holds only part of the registry, and the previous set is tracked
// here so the churn figure and the publication cannot disagree.
//
// Parameters:
//   - samples []metrics.ConsumerLagSample: the complete measured set for this tick. Nil or
//     empty retires every series, which is the correct reading when nothing is measurable.
//
// Returns:
//   - int: how many series from the previous inventory are absent from this one, and so stop
//     being exported.
func (c *EventMetricsCollector) publishLagInventory(samples []metrics.ConsumerLagSample) int {
	current := make(map[lagSeries]struct{}, len(samples))
	for _, sample := range samples {
		current[lagSeries{
			subscriber: sample.Subscriber,
			group:      sample.Group,
			topic:      sample.Topic,
		}] = struct{}{}
	}

	c.mu.Lock()
	previous := c.publishedLagSeries
	c.publishedLagSeries = current
	c.mu.Unlock()

	metrics.PublishConsumerLagInventory(samples)

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
	samples []metrics.ConsumerLagSample,
	report *EventMetricsReport,
) []metrics.ConsumerLagSample {
	if len(subscriber.AuthorizedTopics) == 0 {
		report.SubscribersSkipped++

		return samples
	}

	if !isRegistrySubscriberIdentifier(subscriber.SubscriberID) ||
		!isRegistryConsumerGroupID(subscriber.ConsumerGroupID) {
		report.SubscribersSkipped++

		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			"group":      sanitizeLogValue(subscriber.ConsumerGroupID, maxLoggedFilterLength),
		}).Warn(
			"event metrics: a registry row does not carry generated identifiers, so its lag is not measured; " +
				"measuring it would merge it with every other non-conforming row into one series",
		)

		return samples
	}

	lagReport, err := c.admin.ConsumerLag(ctx, ConsumerLagRequest{
		SubscriberID: subscriber.SubscriberID,
		GroupID:      subscriber.ConsumerGroupID,
		Topics:       subscriber.AuthorizedTopics,
	})
	if err != nil {
		// NO SAMPLES from a failed measurement, which is deliberate: the series is retired
		// rather than left standing at its last reading. A lag nobody could verify this tick
		// is not evidence of anything, and a missing series is visibly missing where a stale
		// one reads as current.
		report.Failures = append(report.Failures, fmt.Errorf(
			"measuring consumer lag for subscriber %s: %w", subscriber.SubscriberID, err,
		))

		return samples
	}

	report.SubscribersMeasured++

	// The report renders its own samples, resolving every label through the same helpers the
	// instrument contract requires — so the collector cannot label a series differently from
	// the way ConsumerLag would have. A topic whose partitions could not all be read yields a
	// sample marked incomplete, which contributes measurement health and NO lag reading.
	return append(samples, lagReport.LagSamples()...)
}
