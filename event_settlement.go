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

// event_settlement.go finishes the broker-side work a subscriber operation could not.
//
// A subscriber's state lives in two systems that cannot be written atomically: the
// registry row in PostgreSQL, and the principal, credential and ACL bindings at the
// Kafka broker. Three operations span both — issuing a credential, changing an
// authorization, deregistering — and every one of them has intermediate states that are
// reachable in practice:
//
//   - an authorization change that pruned the broker and then failed to persist, or
//     persisted and then failed to grant, or whose process disappeared between the two;
//   - a credential written at the broker whose compensating revocation itself failed,
//     leaving a principal that can authenticate with no authorization boundary;
//   - a credential revoked and confirmed gone whose registry record could not be
//     cleared, leaving the registry reporting access that does not exist.
package blnk

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
)

const (
	// defaultSubscriberSettlementInterval is how often a pass runs.
	defaultSubscriberSettlementInterval = time.Minute

	// defaultSubscriberSettlementBatchSize is how many obligations one pass takes.
	defaultSubscriberSettlementBatchSize = 20

	// defaultSubscriberSettlementRetryInterval is how long a failed attempt is left alone.
	defaultSubscriberSettlementRetryInterval = 5 * time.Minute

	// subscriberSettlementPassTimeout bounds ONE pass.
	subscriberSettlementPassTimeout = 5 * time.Minute

	// subscriberSettlementBudget bounds ONE subscriber's remedy.
	subscriberSettlementBudget = 30 * time.Second
)

// The obstacles that stop the processor running, as values rather than freshly-built
// errors.
var (
	// ErrSubscriberSettlementDisabled means no Kafka broker is configured, so there is no
	// broker-side state to reconcile and nothing for this worker to do.
	ErrSubscriberSettlementDisabled = errors.New(
		"blnk: no Kafka broker is configured, so subscriber settlement has nothing to reconcile",
	)

	errSubscriberSettlementProcessorNil = errors.New(
		"blnk: the subscriber settlement processor is nil",
	)

	errSubscriberSettlementNoService = errors.New(
		"blnk: subscriber settlement has no subscriber registry service",
	)
)

// subscriberSettlementStore is the persistence surface the processor needs, and nothing more.
//
// Two methods: find the obligations, and record that they were attempted. Everything that
// DISCHARGES an obligation is deliberately absent, because discharging belongs to the remedy
// that satisfied it — a processor able to clear a marker without performing the remedy would be
// one mistake away from silently declaring the divergence settled.
type subscriberSettlementStore interface {
	// ListSubscriberSettlementObligations returns the subscribers owing broker-side work,
	// oldest attempt first, skipping any attempted at or after the caller's bound.
	ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error)

	// MarkSubscriberSettlementAttempt records that a pass tried a row and what happened. It
	// paces the next attempt and does not discharge anything.
	MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error
}

// subscriberSettler is the remedy surface the processor drives.
//
// One method, so the processor owns the loop and the registry service owns every decision about
// broker state. That separation is what keeps the fencing, the tombstone handling and the
// ordering between the two remedies in the one file that already implements them, rather than
// duplicated here where a second opinion could disagree with the first.
type subscriberSettler interface {
	// SettleSubscriber discharges whatever a subscriber owes, under its provisioning claim.
	SettleSubscriber(ctx context.Context, subscriberID string) error
}

// SubscriberSettlementProcessor periodically finishes the broker-side work that subscriber
// operations could not complete.
type SubscriberSettlementProcessor struct {
	store   subscriberSettlementStore
	settler subscriberSettler

	// configured records whether a broker is configured. Read once at construction, because a
	// deployment without Kafka has no broker-side state and this worker must decline to start
	// rather than poll a table forever to find nothing.
	configured bool

	interval      time.Duration
	batchSize     int
	retryInterval time.Duration

	// now is the clock, replaceable in-package so a test can assert the retry bound exactly.
	// It follows the retention sweeper's and the relay's now field.
	now func() time.Time

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// NewSubscriberSettlementProcessor builds a processor from a Blnk instance.
//
// Parameters:
//   - b *Blnk: the service container. A nil instance, or one with no datasource, yields
//     a processor that declines to start rather than a nil pointer the caller must
//     guard.
//
// Returns:
//   - *SubscriberSettlementProcessor: ready to Start. Never nil.
func NewSubscriberSettlementProcessor(b *Blnk) *SubscriberSettlementProcessor {
	processor := &SubscriberSettlementProcessor{
		interval:      defaultSubscriberSettlementInterval,
		batchSize:     defaultSubscriberSettlementBatchSize,
		retryInterval: defaultSubscriberSettlementRetryInterval,
		now:           time.Now,
	}

	if b == nil {
		return processor
	}

	if b.datasource != nil {
		processor.store = b.datasource
	}

	service := b.EventSubscribers()
	if service != nil {
		processor.settler = service
	}

	// Read from the instance's own configuration, falling back to the process
	// configuration when the instance carries none — the same fallback retentionPeriodFor
	// makes, and for the same reason: a Blnk built before configuration was published
	// would otherwise report the broker as absent and silently never settle anything.
	processor.configured = subscriberSettlementConfigured(b.Config())

	return processor
}

// subscriberSettlementConfigured reports whether a Kafka broker is configured.
//
// It asks the same question every other part of this feature asks — is the broker list
// non-empty — rather than a variant of it, because a worker that disagreed with the
// publisher about whether Kafka exists would either poll forever on a deployment
// without it or stay silent on one with it.
//
// Parameters:
//   - cnf *config.Configuration: the instance's configuration, possibly nil.
//
// Returns:
//   - bool: true when at least one broker is configured.
func subscriberSettlementConfigured(cnf *config.Configuration) bool {
	if cnf != nil {
		return len(cnf.Kafka.Brokers) > 0
	}

	fetched, err := config.Fetch()
	if err != nil {
		return false
	}

	return len(fetched.Kafka.Brokers) > 0
}

// WithInterval sets how often a pass runs.
//
// A non-positive interval falls back to the default rather than being rejected,
// matching every other worker here: a misconfigured cadence must not be able to stop
// settlement running, because the failure mode is a divergence nobody notices.
//
// Parameters:
//   - interval time.Duration: the pass interval.
//
// Returns:
//   - *SubscriberSettlementProcessor: the receiver, for chaining.
func (p *SubscriberSettlementProcessor) WithInterval(interval time.Duration) *SubscriberSettlementProcessor {
	if interval <= 0 {
		logrus.WithField("requested_interval", interval.String()).
			Warnf("Non-positive subscriber settlement interval; falling back to %s",
				defaultSubscriberSettlementInterval)

		return p
	}

	p.interval = interval

	return p
}

// WithBatchSize sets how many obligations one pass takes.
//
// Parameters:
//   - size int: the batch size. Non-positive falls back to the default.
//
// Returns:
//   - *SubscriberSettlementProcessor: the receiver, for chaining.
func (p *SubscriberSettlementProcessor) WithBatchSize(size int) *SubscriberSettlementProcessor {
	if size <= 0 {
		logrus.WithField("requested_batch_size", size).
			Warnf("Non-positive subscriber settlement batch size; falling back to %d",
				defaultSubscriberSettlementBatchSize)

		return p
	}

	p.batchSize = size

	return p
}

// WithRetryInterval sets how long a failed attempt is left alone.
//
// A non-positive value falls back to the default. Zero is NOT read as "retry
// immediately", because that is the one setting that would turn a broker outage into a
// busy loop.
//
// Parameters:
//   - interval time.Duration: the retry interval.
//
// Returns:
//   - *SubscriberSettlementProcessor: the receiver, for chaining.
func (p *SubscriberSettlementProcessor) WithRetryInterval(interval time.Duration) *SubscriberSettlementProcessor {
	if interval <= 0 {
		logrus.WithField("requested_retry_interval", interval.String()).
			Warnf("Non-positive subscriber settlement retry interval; falling back to %s",
				defaultSubscriberSettlementRetryInterval)

		return p
	}

	p.retryInterval = interval

	return p
}

// StartupObstacle reports why the processor must not run, or nil when it may.
//
// Returns:
//   - error: ErrSubscriberSettlementDisabled when no broker is configured, a wiring error when
//     the registry or the service is absent, or nil.
func (p *SubscriberSettlementProcessor) StartupObstacle() error {
	if p == nil {
		return errSubscriberSettlementProcessorNil
	}

	if !p.configured {
		return ErrSubscriberSettlementDisabled
	}

	if p.store == nil {
		return errEventRetentionNoDatasource
	}

	if p.settler == nil {
		return errSubscriberSettlementNoService
	}

	return nil
}

// Start begins the periodic pass. It is a no-op when the processor must not run, and calling it
// twice is a no-op the second time.
//
// Parameters:
//   - ctx context.Context: cancelling it stops the loop.
func (p *SubscriberSettlementProcessor) Start(ctx context.Context) {
	if obstacle := p.StartupObstacle(); obstacle != nil {
		return
	}

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()

		return
	}
	p.running = true
	stop := make(chan struct{})
	p.stopCh = stop
	// Under the same lock that publishes running, so a Stop landing here cannot return from a
	// Wait with nothing to wait for while the loop goroutine has yet to start.
	p.wg.Add(1)
	p.mu.Unlock()

	logrus.WithFields(logrus.Fields{
		"interval":       p.interval.String(),
		"batch_size":     p.batchSize,
		"retry_interval": p.retryInterval.String(),
	}).Info("subscriber settlement processor started")

	go func() {
		defer p.wg.Done()
		defer p.clearRunning()

		// The channel is PASSED IN, captured from the same locked section that created it,
		// rather than read from the field inside the loop. Stop nils the field, and a Stop
		// that lands between this goroutine being scheduled and its first read would hand the
		// loop a nil channel — which in a select blocks forever, so the loop would never see
		// the stop signal and Stop's Wait would never return.
		p.run(ctx, stop)
	}()
}

// Stop halts the loop and waits for the pass in flight.
func (p *SubscriberSettlementProcessor) Stop() {
	p.mu.Lock()
	// Nilled as well as closed, so a second Stop cannot close an already-closed channel and
	// panic. Safe to nil because the loop holds its own reference and never reads the field.
	if p.stopCh != nil {
		close(p.stopCh)
		p.stopCh = nil
	}
	p.mu.Unlock()

	// Outside the lock, and called unconditionally: a Stop that raced a Start must still wait
	// for whatever that Start began.
	p.wg.Wait()
}

// clearRunning marks the processor stopped. It runs when the loop exits for ANY reason,
// including context cancellation, so IsRunning cannot report a cancelled processor as live and a
// later Start is not refused by the idempotence guard.
func (p *SubscriberSettlementProcessor) clearRunning() {
	p.mu.Lock()
	p.running = false
	p.mu.Unlock()
}

// IsRunning reports whether the loop is live.
//
// Returns:
//   - bool: true between a successful Start and the loop's exit.
func (p *SubscriberSettlementProcessor) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.running
}

// run is the ticker loop.
//
// The first pass is on the first tick rather than at start-up, matching the retention
// sweeper. An obligation that has been outstanding since before this process existed
// can wait one more interval, and a pass during a rollout would have every replica
// making administrative calls to the broker at the same moment.
//
// Parameters:
//   - ctx context.Context: cancelling it ends the loop.
//   - stop <-chan struct{}: the stop channel, captured by Start rather than read from
//     the field.
func (p *SubscriberSettlementProcessor) run(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Info("subscriber settlement processor stopping: context cancelled")

			return
		case <-stop:
			logrus.Info("subscriber settlement processor stopping")

			return
		case <-ticker.C:
			p.Pass(ctx)
		}
	}
}

// Pass performs ONE bounded settlement pass and returns how many obligations it
// discharged.
//
// It is exported so an operator-facing path can run settlement on demand — the same
// code, the same bounds, the same ordering — rather than a second implementation able
// to disagree with this one.
//
// Parameters:
//   - ctx context.Context: cancels the pass. A deadline of its own is applied on top.
//
// Returns:
//   - int: how many obligations were fully discharged.
func (p *SubscriberSettlementProcessor) Pass(ctx context.Context) int {
	if obstacle := p.StartupObstacle(); obstacle != nil {
		return 0
	}

	passCtx, cancel := context.WithTimeout(ctx, subscriberSettlementPassTimeout)
	defer cancel()

	// The bound is computed once for the whole pass, so a long pass cannot start skipping rows
	// it would have taken when it began.
	notBefore := p.now().UTC().Add(-p.retryInterval)

	obligations, err := p.store.ListSubscriberSettlementObligations(passCtx, p.batchSize, notBefore)
	if err != nil {
		logrus.WithError(err).Error(
			"subscriber settlement: the outstanding obligations could not be read, so none were " +
				"attempted; the pass is retried on the next tick",
		)

		return 0
	}

	if len(obligations) == 0 {
		// The healthy steady state, so it is logged at debug rather than adding a line a minute
		// to every operator's log.
		logrus.Debug("subscriber settlement: no subscriber owes broker-side reconciliation")

		return 0
	}

	settled := 0

	for i := range obligations {
		if passCtx.Err() != nil {
			// Out of budget. The remaining obligations keep their markers and are taken by the
			// next pass, which is why nothing here needs to record where it stopped.
			break
		}

		if p.settle(passCtx, obligations[i]) {
			settled++
		}
	}

	logrus.WithFields(logrus.Fields{
		"outstanding": len(obligations),
		"settled":     settled,
	}).Info("subscriber settlement: pass complete")

	return settled
}

// settle discharges one subscriber's obligations and records the attempt either way.
//
// The attempt is recorded on a context DETACHED from the per-subscriber deadline,
// because the commonest failure is that deadline expiring and an attempt that could not
// be recorded would never pace the next one — turning a broker outage into a
// pass-per-minute busy loop against the same failing subscriber.
//
// Parameters:
//   - ctx context.Context: the pass context.
//   - obligation model.SubscriberSettlementObligation: what this subscriber owes.
//
// Returns:
//   - bool: true when the remedy completed and the obligations were discharged.
func (p *SubscriberSettlementProcessor) settle(
	ctx context.Context,
	obligation model.SubscriberSettlementObligation,
) bool {
	fields := logrus.Fields{
		"subscriber":                 sanitizeLogValue(obligation.SubscriberID, maxLoggedFilterLength),
		"grant_reconcile_pending":    obligation.GrantReconcilePending,
		"credential_cleanup_pending": obligation.CredentialCleanupPending,
		"prior_attempts":             obligation.Attempts,
	}

	remedy, cancel := context.WithTimeout(ctx, subscriberSettlementBudget)
	defer cancel()

	err := p.settler.SettleSubscriber(remedy, obligation.SubscriberID)

	failure := ""
	if err != nil {
		failure = sanitizeLogValue(err.Error(), maxLoggedErrorLength)
	}

	// Detached from the remedy's deadline AND from the pass's, so the pacing write survives the
	// expiry that is most likely to have caused the failure it is pacing.
	record, cancelRecord := subscriberCleanupContext(context.WithoutCancel(ctx))
	defer cancelRecord()

	if markErr := p.store.MarkSubscriberSettlementAttempt(
		record, obligation.SubscriberID, p.now().UTC(), failure,
	); markErr != nil {
		logrus.WithFields(fields).WithField(
			"pacing_error", sanitizeLogValue(markErr.Error(), maxLoggedErrorLength),
		).Warn(
			"subscriber settlement: the attempt could not be recorded, so the next pass may retry " +
				"this subscriber sooner than the retry interval",
		)
	}

	if err != nil {
		logrus.WithFields(fields).WithField("error", failure).Error(
			"subscriber settlement: this subscriber's broker-side reconciliation failed and its " +
				"obligation remains outstanding; it is retried after the retry interval",
		)

		return false
	}

	logrus.WithFields(fields).Info(
		"subscriber settlement: this subscriber's broker-side state was reconciled and its " +
			"obligation discharged",
	)

	if metrics.SubscriberObligationsSettledTotal != nil {
		metrics.SubscriberObligationsSettledTotal.Add(ctx, 1)
	}

	return true
}
