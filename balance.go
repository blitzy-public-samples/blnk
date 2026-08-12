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
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// balanceTracer is an OpenTelemetry tracer for tracking balance-related transactions.
var (
	balanceTracer = otel.Tracer("blnk.transactions")
)

// NewBalanceTracker creates a new BalanceTracker instance.
// It initializes the Balances and Frequencies maps.
//
// Returns:
// - *model.BalanceTracker: A pointer to the newly created BalanceTracker instance.
func NewBalanceTracker() *model.BalanceTracker {
	return &model.BalanceTracker{
		Balances:    make(map[string]*model.Balance),
		Frequencies: make(map[string]int),
	}
}

// checkBalanceMonitors checks the balance monitors for a given updated balance. It
// starts a tracing span, fetches the monitors, and checks each monitor's condition. If
// a condition is met, it captures a balance.monitor event in the outbox through the
// DURABLE standalone path, because the balance movement that satisfied the condition
// has already been committed by the time this runs and the capture is therefore the
// alert's only chance.
//
// Parameters:
//   - ctx context.Context: The context for the operation.
//   - updatedBalance *model.Balance: A pointer to the updated Balance model.
//   - capture balanceMonitorCapture: what the pre-commit evaluation already covered.
func (l *Blnk) checkBalanceMonitors(ctx context.Context, updatedBalance *model.Balance, capture balanceMonitorCapture) {
	_, span := balanceTracer.Start(ctx, "CheckBalanceMonitors")
	defer span.End()

	// THIS IS NOW THE LEGACY-ONLY PATH, and the guard is what keeps it that way.
	if l.balanceMonitorHandoffEnabled() {
		span.AddEvent("Monitor evaluation deferred to the durable handoff")
		return
	}

	// Fetch monitors using cache (avoids DB query on every transaction)
	monitors, err := l.getBalanceMonitorsCached(ctx, updatedBalance.BalanceID)
	if err != nil {
		span.RecordError(err)
		notification.NotifyError(err)
		return
	}

	// Check each monitor's condition
	for _, monitor := range monitors {
		if monitor.CheckCondition(updatedBalance) {
			span.AddEvent(fmt.Sprintf("Condition met for balance: %s", monitor.MonitorID))

			// ALREADY DURABLE, so nothing to do. The alert was enrolled in the very transaction
			// that moved the balance across the threshold, which is what the requirement asks
			// for; capturing it again here would publish the same crossing twice under two
			// different event ids, and duplicate suppression at a subscriber keys on event_id,
			// so nothing downstream could collapse them.
			if capture.holds(updatedBalance.BalanceID, monitor.MonitorID) {
				continue
			}
			// BOUNDED, and acquired here rather than inside the goroutine so a database that
			// cannot keep up is felt as backpressure instead of absorbed as an unbounded pile-up
			// of goroutines. One balance can carry many monitors and many of them can fire on
			// one update, so the fan-out here is a product of two counts rather than one per
			// transaction — the shape most likely to exhaust memory first.
			postCommitEventPublishSem <- struct{}{}
			go func(monitor model.BalanceMonitor) {
				defer func() { <-postCommitEventPublishSem }()

				// PRODUCER CALL SITE FOR balance.monitor — THE FALLBACK ONE.
				//
				// THIS IS NOT THE ONLY SUCH SITE. Three places in this repository spend a bounded
				// budget on a capture whose
				// mutation has already committed — that is the SITE set, and it is NOT the same
				// thing as the set of event types a subscriber must treat as at-most-once.
				//
				//   1. balance.monitor — this call site. The balance movement that met the
				//      condition committed under another transaction.
				//   2. bulk_transaction.<status> — finalizeBulkBatchOutcome's retry loop.
				//   3. The status-derived transaction.* events of a COALESCED batch —
				//      postTransactionActions' fallback.
				err := l.PublishEventDurably(ctx, NewWebhook{
					Event:   "balance.monitor",
					Payload: monitor,
				}, WithEventLedgerID(updatedBalance.LedgerID))
				if err != nil {
					notification.NotifyError(err)
				}
			}(monitor)
		}
	}
}

// monitorCaptureKey identifies one balance-and-monitor alert.
//
// Both halves are needed. One balance can carry several monitors and one monitor names
// exactly one balance, so keying on either alone would let a second monitor's crossing
// on the same balance be mistaken for one already captured, and the alert would then be
// dropped by both routes.
//
// Parameters:
//   - balanceID string: the monitored balance.
//   - monitorID string: the monitor whose condition was met.
//
// Returns:
//   - string: the set key. The separator is a character neither identifier can contain.
func monitorCaptureKey(balanceID, monitorID string) string {
	return balanceID + "|" + monitorID
}

// balanceMonitorCapture records what the pre-commit monitor evaluation covered.
//
// It travels from the atomic writer to the post-commit monitor check and answers two
// different questions, which is why it is a pair of sets rather than one:
//
//   - balances: whose monitors were READ AND EVALUATED before the write.
//   - alerts: which crossings were CAPTURED, keyed by monitorCaptureKey.
type balanceMonitorCapture struct {
	balances map[string]struct{}
	alerts   map[string]struct{}
}

// covers reports whether a balance's monitors were already read and evaluated with the
// mutation.
//
// Parameters:
//   - balanceID string: the balance the post-commit check is about to examine.
//
// Returns:
//   - bool: true when the pre-commit pass owned this balance.
func (c balanceMonitorCapture) covers(balanceID string) bool {
	if len(c.balances) == 0 {
		return false
	}

	_, ok := c.balances[balanceID]

	return ok
}

// holds reports whether one crossing is already durable.
//
// It is the finer-grained companion to covers, used for the balance that WAS evaluated
// but whose monitor set the post-commit check re-read anyway — a path that exists only
// when a caller supplies alerts without the balance, and one that must not publish a
// crossing twice.
//
// Parameters:
//   - balanceID string: the monitored balance.
//   - monitorID string: the monitor whose condition was met.
//
// Returns:
//   - bool: true when this crossing was committed inside the mutation.
func (c balanceMonitorCapture) holds(balanceID, monitorID string) bool {
	if len(c.alerts) == 0 {
		return false
	}

	_, ok := c.alerts[monitorCaptureKey(balanceID, monitorID)]

	return ok
}

// prepareBalanceMonitorEvents evaluates the monitors of balances a mutation has just
// changed and returns the balance.monitor rows to enrol in that mutation's own
// transaction.
//
//   - A MONITOR READ that fails is reported and stepped past, and the affected balance
//     is left out of the capture so the OTHER route still evaluates it durably — the
//     writer's own in-transaction capture when publishing is configured, the
//     post-commit hook when it is not.
//   - A PAYLOAD that will not serialise is returned as an error, which fails the write.
//
// Parameters:
//   - ctx context.Context: the context for the reads; no write happens here.
//   - balances []*model.Balance: the balances the mutation has updated, as they will be
//     persisted.
//
// Returns:
//   - []*model.EventOutbox: the rows to hand to the atomic writer, nil when nothing
//     fired or when event publishing is not configured.
//   - balanceMonitorCapture: what this pass covered, for the post-commit hook to skip.
//   - error: only a payload that cannot be serialised.
func (l *Blnk) prepareBalanceMonitorEvents(
	ctx context.Context,
	balances []*model.Balance,
) ([]*model.EventOutbox, balanceMonitorCapture, error) {
	ctx, span := balanceTracer.Start(ctx, "PrepareBalanceMonitorEvents")
	defer span.End()

	var (
		rows    []*model.EventOutbox
		capture balanceMonitorCapture
	)

	if l == nil || l.datasource == nil {
		return nil, capture, nil
	}

	// NO CACHE IS NOT A REASON TO SKIP THIS. The monitor read below tolerates a nil cache
	// on both its read and its write-back, so the only thing a missing cache costs is the
	// cache.

	for _, balance := range balances {
		if balance == nil {
			continue
		}

		monitors, err := l.getBalanceMonitorsCached(ctx, balance.BalanceID)
		if err != nil {
			// Reported and stepped past, never returned: see the error policy above. The
			// post-commit hook evaluates this balance because it never enters the capture.
			span.RecordError(err)
			logrus.WithError(err).WithField("balance", balance.BalanceID).Warn(
				"balance monitors could not be read before the write, so any crossing they " +
					"describe is captured after the commit instead of with it",
			)

			continue
		}

		if capture.balances == nil {
			capture.balances = make(map[string]struct{}, len(balances))
		}
		capture.balances[balance.BalanceID] = struct{}{}

		for _, monitor := range monitors {
			if !monitor.CheckCondition(balance) {
				continue
			}

			row, prepareErr := l.prepareBalanceMonitorAlertRow(ctx, balance, monitor)
			if prepareErr != nil {
				span.RecordError(prepareErr)

				return nil, balanceMonitorCapture{}, prepareErr
			}

			// Nil is the unconfigured case: no brokers, so no event pipeline and no row to
			// capture. The balance still counts as evaluated, because a deployment with no
			// event pipeline has nothing for the post-commit route to capture either.
			if row == nil {
				continue
			}

			if capture.alerts == nil {
				capture.alerts = make(map[string]struct{}, len(monitors))
			}

			rows = append(rows, row)
			capture.alerts[monitorCaptureKey(balance.BalanceID, monitor.MonitorID)] = struct{}{}
		}
	}

	span.SetAttributes(
		attribute.Int("balance.monitors_evaluated_for", len(capture.balances)),
		attribute.Int("balance.monitor_events_captured", len(rows)),
	)

	return rows, capture, nil
}

// prepareBalanceMonitorAlertRow builds the canonical balance.monitor outbox row for ONE
// crossing: one monitor, on one balance, as that balance has just been written.
//
// Three routes can decide a monitor crossing, and all three build the row here:
//
//   - prepareBalanceMonitorEvents above, before the write, on the single-transaction
//     path.
//   - database.captureBalanceMonitorAlertsInTx, inside the writer's transaction, for
//     the balances that pass did not cover and for the coalesced batch it is not wired
//     into.
//   - BalanceMonitorHandoffProcessor, draining a handoff row written before the
//     in-transaction capture existed, which supplies the handoff's own event identity.
//
// Parameters:
//   - ctx context.Context: the caller's context, for tracing only. No I/O happens here.
//   - balance *model.Balance: the monitored balance in its post-mutation state.
//   - monitor model.BalanceMonitor: the monitor whose condition the balance met.
//
// Returns:
//   - *model.EventOutbox: the row to insert, or nil when event publishing is not
//     configured.
//   - error: a payload that cannot be serialised or a partition key that cannot be
//     resolved.
func (l *Blnk) prepareBalanceMonitorAlertRow(ctx context.Context, balance *model.Balance, monitor model.BalanceMonitor) (*model.EventOutbox, error) {
	return l.PrepareEventOutbox(ctx, NewWebhook{
		Event:   balanceMonitorEventType,
		Payload: monitor,
	}, WithEventLedgerID(balance.LedgerID))
}

// getBalanceMonitorsCached retrieves balance monitors with caching.
// It first checks the cache for monitors, and if not found, fetches from the database
// and caches the result with a 5-minute TTL.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance to get monitors for.
//
// Returns:
// - []model.BalanceMonitor: A slice of monitors for the balance.
// - error: An error if the monitors could not be retrieved.
func (l *Blnk) getBalanceMonitorsCached(ctx context.Context, balanceID string) ([]model.BalanceMonitor, error) {
	cacheKey := "monitors:" + balanceID

	var monitors []model.BalanceMonitor

	// GUARDED ON BOTH SIDES, because this function is reachable on an instance built
	// without a cache — production always has one, tests construct Blnk directly, and the
	// monitor capture path must behave the same either way. cache.Cache is an interface,
	// so a nil field is a nil interface and calling through it panics rather than
	// returning an error.
	if l.cache != nil {
		if err := l.cache.Get(ctx, cacheKey, &monitors); err == nil && monitors != nil {
			return monitors, nil
		}
	}

	monitors, err := l.datasource.GetBalanceMonitors(balanceID)
	if err != nil {
		return nil, err
	}

	if monitors == nil {
		monitors = []model.BalanceMonitor{}
	}

	// The write-back, guarded for the same reason as the read above.
	if l.cache != nil {
		_ = l.cache.Set(ctx, cacheKey, monitors, 5*time.Minute)
	}

	return monitors, nil
}

// getOrCreateBalanceByIndicator retrieves a balance by its indicator and currency.
// If the balance does not exist, it creates a new one.
// It starts a tracing span, fetches or creates the balance, and records relevant events.
// When EnableQueuedChecks is enabled in the transaction config, it will fetch the balance with queued data included.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - indicator string: The indicator for the balance.
// - currency string: The currency for the balance.
//
// Returns:
// - *model.Balance: A pointer to the Balance model.
// - error: An error if the balance could not be retrieved or created.
func (l *Blnk) getOrCreateBalanceByIndicator(ctx context.Context, indicator, currency string) (*model.Balance, error) {
	ctx, span := balanceTracer.Start(ctx, "GetOrCreateBalanceByIndicator")
	defer span.End()

	// Get configuration to check if queued checks are enabled
	cfg, err := config.Fetch()
	if err != nil {
		span.RecordError(err)
		logrus.Errorf("failed to fetch config: %v", err)
		return nil, err
	}

	balance, err := l.datasource.GetBalanceByIndicator(indicator, currency)
	if err != nil {
		span.AddEvent("Creating new balance")
		balance = &model.Balance{
			Indicator: indicator,
			LedgerID:  GeneralLedgerID,
			Currency:  currency,
		}
		_, err := l.CreateBalance(ctx, *balance)
		if err != nil && !strings.Contains(err.Error(), "Balance already exist") {
			span.RecordError(err)
			return nil, err
		}
		balance, err = l.datasource.GetBalanceByIndicator(indicator, currency)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		span.AddEvent("New balance created", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))

		// If queued checks are enabled, fetch the balance with queued data
		if cfg.Transaction.EnableQueuedChecks {
			balance, err = l.datasource.GetBalanceByID(balance.BalanceID, []string{}, true)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
		}

		return balance, nil
	}

	// If queued checks are enabled, fetch the balance with queued data
	if cfg.Transaction.EnableQueuedChecks {
		balance, err = l.datasource.GetBalanceByID(balance.BalanceID, []string{}, true)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	span.AddEvent("Balance found", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	return balance, nil
}

// postBalanceActions performs some actions after a balance has been created. It starts
// a tracing span and sends the balance to the search index queue.
//
// It does NOT capture balance.created on the normal path: the repository inserts that row
// inside the transaction that inserts the balance, from the preparer
// balanceCreatedEventPreparer supplies, so the event and the balance commit together.
// Capturing it here as well would attempt the same event_id twice and leave a logged
// conflict on every balance creation. Indexing stays because TypeSense is a separate
// system with its own retry queue and belongs nowhere near a ledger transaction.
//
// THE ONE CASE IT STILL PUBLISHES is a webhook-only deployment — a webhook URL and no
// KAFKA_BROKERS — which gets no preparer at all, because capturing rows no relay can
// drain is what eventCaptureEnabled exists to avoid. publishEntityEventWhenUncaptured is
// the fallback for that shape alone, and it declines an empty aggregate id, so the
// idempotent indicator conflict CreateBalance reports as success with an EMPTY balance
// announces nothing on either path.
//
// Parameters:
//   - ctx context.Context: The context for the operation, used for the span and,
//     detached from cancellation, for the fallback publish.
//   - balance *model.Balance: A pointer to the newly created Balance model.
func (l *Blnk) postBalanceActions(ctx context.Context, balance *model.Balance) {
	ctx, span := balanceTracer.Start(ctx, "PostBalanceActions")
	defer span.End()

	// The publish context is DETACHED FROM CANCELLATION but not from the trace, using the
	// same context.WithoutCancel idiom runTransactionPostCommitWorkWithHooks already
	// applies to the monitor goroutines in transaction_execution.go. It is derived here,
	// outside the goroutine, because ctx is still live at this point.
	publishCtx := context.WithoutCancel(ctx)

	go func() {
		err := l.queue.queueIndexData(balance.BalanceID, "balances", balance)
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		err = l.publishEntityEventWhenUncaptured(publishCtx, balance.BalanceID, NewWebhook{
			Event:   "balance.created",
			Payload: balance,
		}, WithEventLedgerID(balance.LedgerID))
		if err != nil {
			span.RecordError(err)
			notification.NotifyError(err)
		}
		span.AddEvent("Post balance actions completed", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	}()
}

// balanceCreatedEventPreparer returns the preparer that builds the balance.created
// outbox row, for the repository to insert INSIDE the transaction that inserts the
// balance.
//
// Capturing it here closes the window in which a balance exists and its event does not:
// the event commits with the balance or not at all.
//
// Parameters:
//   - ctx context.Context: the creating request's context, captured for tracing only.
//
// Returns:
//   - database.EventPreparer[model.Balance]: the preparer to hand to the repository.
func (l *Blnk) balanceCreatedEventPreparer(ctx context.Context) database.EventPreparer[model.Balance] {
	// A NIL PREPARER when nothing is configured, so the repository stays on its
	// single-statement path instead of opening a transaction to insert no event. See
	// eventCaptureEnabled.
	if !l.eventCaptureEnabled() {
		return nil
	}

	return func(created model.Balance) (*model.EventOutbox, error) {
		return l.PrepareEventOutbox(ctx, NewWebhook{
			Event:   "balance.created",
			Payload: &created,
		}, WithEventLedgerID(created.LedgerID))
	}
}

// CreateBalance creates a new balance. It starts a tracing span, creates the balance,
// and performs post-creation actions.
//
// The `balance.created` event is captured atomically with the balance row: the capture
// handed to the datasource is invoked with the finalised balance and its row is
// inserted inside the same database transaction, so the balance and its event commit or
// roll back together.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balance model.Balance: The Balance model to be created.
//
// Returns:
// - model.Balance: The created Balance model.
// - error: An error if the balance could not be created.
func (l *Blnk) CreateBalance(ctx context.Context, balance model.Balance) (model.Balance, error) {
	ctx, span := balanceTracer.Start(ctx, "CreateBalance")
	defer span.End()

	balance, err := l.datasource.CreateBalance(balance, l.balanceCreatedEventPreparer(ctx))
	if err != nil {
		span.RecordError(err)
		return model.Balance{}, err
	}
	l.postBalanceActions(ctx, &balance)
	metrics.BalanceCreatedTotal.Add(ctx, 1)
	span.AddEvent("Balance created", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	return balance, nil
}

// GetBalanceByID retrieves a balance by its ID.
// It starts a tracing span, fetches the balance, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the balance to retrieve.
// - include []string: A slice of strings specifying additional data to include.
//
// Returns:
// - *model.Balance: A pointer to the Balance model if found.
// - error: An error if the balance could not be retrieved.
func (l *Blnk) GetBalanceByID(ctx context.Context, id string, include []string, withQueued bool) (*model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceByID")
	defer span.End()

	balance, err := l.datasource.GetBalanceByID(id, include, withQueued)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Balance retrieved", trace.WithAttributes(attribute.String("balance.id", id)))
	return balance, nil
}

// GetAllBalances retrieves all balances.
// It starts a tracing span, fetches all balances, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
//
// Returns:
// - []model.Balance: A slice of Balance models.
// - error: An error if the balances could not be retrieved.
func (l *Blnk) GetAllBalances(ctx context.Context, limit, offset int) ([]model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetAllBalances")
	defer span.End()

	balances, err := l.datasource.GetAllBalances(limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("All balances retrieved", trace.WithAttributes(attribute.Int("balance.count", len(balances))))
	return balances, nil
}

// GetAllBalancesWithFilter retrieves balances using advanced filters.
// It starts a tracing span, fetches balances matching the filter criteria, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - filters *filter.QueryFilterSet: Filter conditions to apply.
// - limit int: Maximum number of balances to return.
// - offset int: Offset for pagination.
//
// Returns:
// - []model.Balance: A slice of Balance models matching the filter criteria.
// - error: An error if the balances could not be retrieved.
func (l *Blnk) GetAllBalancesWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetAllBalancesWithFilter")
	defer span.End()

	balances, err := l.datasource.GetAllBalancesWithFilter(ctx, filters, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Balances with filter retrieved", trace.WithAttributes(attribute.Int("balance.count", len(balances))))
	return balances, nil
}

// GetAllBalancesWithFilterAndOptions retrieves balances with advanced filters, sorting, and optional count.
func (l *Blnk) GetAllBalancesWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Balance, *int64, error) {
	_, span := balanceTracer.Start(ctx, "GetAllBalancesWithFilterAndOptions")
	defer span.End()

	balances, count, err := l.datasource.GetAllBalancesWithFilterAndOptions(ctx, filters, opts, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, nil, err
	}
	span.AddEvent("Balances with filter and options retrieved", trace.WithAttributes(attribute.Int("balance.count", len(balances))))
	return balances, count, nil
}

// CreateMonitor creates a new balance monitor.
// It starts a tracing span, applies precision to the monitor's condition value, and creates the monitor.
// It records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - monitor model.BalanceMonitor: The BalanceMonitor model to be created.
//
// Returns:
// - model.BalanceMonitor: The created BalanceMonitor model.
// - error: An error if the monitor could not be created.
func (l *Blnk) CreateMonitor(ctx context.Context, monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "CreateMonitor")
	defer span.End()

	amount := int64(monitor.Condition.Value * monitor.Condition.Precision) // apply precision to value
	amountBigInt := model.Int64ToBigInt(amount)
	monitor.Condition.PreciseValue = amountBigInt
	monitor, err := l.datasource.CreateMonitor(monitor)
	if err != nil {
		span.RecordError(err)
		return model.BalanceMonitor{}, err
	}

	_ = l.cache.Delete(ctx, "monitors:"+monitor.BalanceID)

	span.AddEvent("Monitor created", trace.WithAttributes(attribute.String("monitor.id", monitor.MonitorID)))
	return monitor, nil
}

// GetMonitorByID retrieves a balance monitor by its ID.
// It starts a tracing span, fetches the monitor, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the monitor to retrieve.
//
// Returns:
// - *model.BalanceMonitor: A pointer to the BalanceMonitor model if found.
// - error: An error if the monitor could not be retrieved.
func (l *Blnk) GetMonitorByID(ctx context.Context, id string) (*model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetMonitorByID")
	defer span.End()

	monitor, err := l.datasource.GetMonitorByID(id)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Monitor retrieved", trace.WithAttributes(attribute.String("monitor.id", id)))
	return monitor, nil
}

// GetAllMonitors retrieves all balance monitors.
// It starts a tracing span, fetches all monitors, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor models.
// - error: An error if the monitors could not be retrieved.
func (l *Blnk) GetAllMonitors(ctx context.Context) ([]model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetAllMonitors")
	defer span.End()

	monitors, err := l.datasource.GetAllMonitors()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("All monitors retrieved", trace.WithAttributes(attribute.Int("monitor.count", len(monitors))))
	return monitors, nil
}

// GetBalanceMonitors retrieves all monitors for a given balance ID.
// It starts a tracing span, fetches the monitors, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance for which to retrieve monitors.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor models.
// - error: An error if the monitors could not be retrieved.
func (l *Blnk) GetBalanceMonitors(ctx context.Context, balanceID string) ([]model.BalanceMonitor, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceMonitors")
	defer span.End()

	monitors, err := l.datasource.GetBalanceMonitors(balanceID)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Monitors retrieved for balance", trace.WithAttributes(attribute.String("balance.id", balanceID), attribute.Int("monitor.count", len(monitors))))
	return monitors, nil
}

// UpdateMonitor updates an existing balance monitor.
// It starts a tracing span, updates the monitor, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - monitor *model.BalanceMonitor: A pointer to the BalanceMonitor model to be updated.
//
// Returns:
// - error: An error if the monitor could not be updated.
func (l *Blnk) UpdateMonitor(ctx context.Context, monitor *model.BalanceMonitor) error {
	_, span := balanceTracer.Start(ctx, "UpdateMonitor")
	defer span.End()

	err := l.datasource.UpdateMonitor(monitor)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_ = l.cache.Delete(ctx, "monitors:"+monitor.BalanceID)

	span.AddEvent("Monitor updated", trace.WithAttributes(attribute.String("monitor.id", monitor.MonitorID)))
	return nil
}

// DeleteMonitor deletes a balance monitor by its ID.
// It starts a tracing span, deletes the monitor, and records relevant events and errors.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - id string: The ID of the monitor to delete.
//
// Returns:
// - error: An error if the monitor could not be deleted.
func (l *Blnk) DeleteMonitor(ctx context.Context, id string) error {
	_, span := balanceTracer.Start(ctx, "DeleteMonitor")
	defer span.End()

	monitor, err := l.datasource.GetMonitorByID(id)
	if err != nil {
		span.RecordError(err)
		return err
	}

	err = l.datasource.DeleteMonitor(id)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_ = l.cache.Delete(ctx, "monitors:"+monitor.BalanceID)

	span.AddEvent("Monitor deleted", trace.WithAttributes(attribute.String("monitor.id", id)))
	return nil
}

// TakeBalanceSnapshots creates daily snapshots of balances in batches.
// It accepts a batch size parameter to control the number of balances processed at once,
// helping to manage memory usage for large datasets.
//
// Parameters:
// - ctx context.Context: The context for managing the operation's lifecycle and cancellation
// - batchSize int: The number of balances to process in each batch
//
// Returns:
// - int: The total number of snapshots created
// - error: An error if the snapshot creation fails
func (l *Blnk) TakeBalanceSnapshots(ctx context.Context, batchSize int) {
	go func() {
		startTime := time.Now()

		// Log the start of snapshot operation
		logrus.WithFields(logrus.Fields{
			"batch_size": batchSize,
			"operation":  "balance_snapshots",
			"status":     "started",
			"timestamp":  startTime.Format(time.RFC3339),
		}).Info("Balance snapshot operation starting")

		_, span := balanceTracer.Start(ctx, "TakeBalanceSnapshots")
		defer span.End()

		// Call the datasource method to create snapshots
		total, err := l.datasource.TakeBalanceSnapshots(context.Background(), batchSize)

		// Calculate duration
		duration := time.Since(startTime)

		if err != nil {
			// Log error with details
			logrus.WithFields(logrus.Fields{
				"batch_size":  batchSize,
				"operation":   "balance_snapshots",
				"status":      "failed",
				"duration_ms": duration.Milliseconds(),
				"error":       err.Error(),
			}).Error("Balance snapshot operation failed")

			span.RecordError(err)
			return
		}

		// Log successful completion with metrics
		logrus.WithFields(logrus.Fields{
			"batch_size":           batchSize,
			"operation":            "balance_snapshots",
			"status":               "completed",
			"total_snapshots":      total,
			"duration_ms":          duration.Milliseconds(),
			"snapshots_per_second": float64(total) / duration.Seconds(),
			"timestamp":            time.Now().Format(time.RFC3339),
		}).Info("Balance snapshot operation completed successfully")

		span.AddEvent("Balance snapshots created", trace.WithAttributes(
			attribute.Int("total_snapshots", total),
			attribute.Int("batch_size", batchSize),
			attribute.Int64("duration_ms", duration.Milliseconds()),
			attribute.Float64("snapshots_per_second", float64(total)/duration.Seconds()),
		))
	}()
}

// GetBalanceAtTime retrieves a balance's state at a specific point in time.
// It can either use balance snapshots for efficiency or calculate from all source transactions
// based on the fromSource parameter.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - balanceID string: The ID of the balance to retrieve.
// - targetTime time.Time: The point in time for which to retrieve the balance state.
// - fromSource bool: If true, calculates balance from all transactions instead of using snapshots.
//
// Returns:
// - *model.Balance: A pointer to the Balance model representing the state at the given time.
// - error: An error if the historical balance state could not be retrieved.
func (l *Blnk) GetBalanceAtTime(ctx context.Context, balanceID string, targetTime time.Time, fromSource bool) (*model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceAtTime")
	defer span.End()

	span.SetAttributes(
		attribute.String("balance.id", balanceID),
		attribute.String("target.time", targetTime.String()),
		attribute.Bool("from.source", fromSource),
	)

	if fromSource {
		span.AddEvent("Calculating balance from source transactions")
	} else {
		span.AddEvent("Using snapshots to calculate balance")
	}

	balance, err := l.datasource.GetBalanceAtTime(ctx, balanceID, targetTime, fromSource)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to get balance at time: %w", err)
	}

	if balance == nil {
		span.AddEvent("No balance data found for the specified time")
		return nil, fmt.Errorf("no balance data found for time: %v", targetTime)
	}

	calculationMethod := "snapshot-based"
	if fromSource {
		calculationMethod = "transaction-based"
	}

	span.AddEvent("Historical balance state retrieved", trace.WithAttributes(
		attribute.String("balance.id", balance.BalanceID),
		attribute.String("snapshot.time", targetTime.String()),
		attribute.String("calculation.method", calculationMethod),
	))

	return balance, nil
}

// GetBalanceByIndicator retrieves a balance by its indicator and currency.
// It starts a tracing span, fetches the balance, and records relevant events.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - indicator string: The indicator of the balance to retrieve.
// - currency string: The currency of the balance to retrieve.
//
// Returns:
// - *model.Balance: A pointer to the Balance model if found.
// - error: An error if the balance could not be retrieved.
func (l *Blnk) GetBalanceByIndicator(ctx context.Context, indicator, currency string) (*model.Balance, error) {
	_, span := balanceTracer.Start(ctx, "GetBalanceByIndicator")
	defer span.End()

	span.SetAttributes(
		attribute.String("balance.indicator", indicator),
		attribute.String("balance.currency", currency),
	)

	balance, err := l.datasource.GetBalanceByIndicator(indicator, currency)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("Balance retrieved by indicator", trace.WithAttributes(attribute.String("balance.id", balance.BalanceID)))
	return balance, nil
}

// UpdateBalanceIdentity updates only the identity_id associated with a balance.
// It validates that both the balance and the identity exist before applying the change.
//
// Parameters:
// - balanceID string: The ID of the balance whose identity reference should be modified.
// - identityID string: The new identity ID to associate with the balance.
//
// Returns:
// - error: An error is returned if either the balance or identity records are not found or the update fails.
func (l *Blnk) UpdateBalanceIdentity(balanceID, identityID string) error {
	// Ensure the referenced identity exists
	_, err := l.datasource.GetIdentityByID(identityID)
	if err != nil {
		return fmt.Errorf("identity validation failed: %w", err)
	}

	// Ensure the balance exists (lite lookup)
	_, err = l.datasource.GetBalanceByIDLite(balanceID)
	if err != nil {
		return fmt.Errorf("balance validation failed: %w", err)
	}

	// Apply the update
	if err := l.datasource.UpdateBalanceIdentity(balanceID, identityID); err != nil {
		return err
	}

	return nil
}
