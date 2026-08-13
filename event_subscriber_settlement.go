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
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/model"
)

// SettleSubscriber discharges whatever broker-side work a subscriber still owes.
//
// Parameters:
//   - ctx context.Context: bounds the whole remedy. The caller applies its own budget.
//   - subscriberID string: the subscriber to settle.
//
// Returns:
//   - error: a typed conflict when another operation holds the subscriber's claim —
//     which is a SKIP rather than a fault, and is why the processor simply retries
//     later — a typed not-found when the subscriber has since been deregistered, or the
//     failure that stopped the remedy.
func (s *EventSubscriberService) SettleSubscriber(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED, like every other operation that changes broker state. A subscriber somebody
	// is actively issuing for is left alone: the conflict propagates, the processor
	// records the attempt, and the obligation is taken by a later pass. Settling
	// underneath a live issuance would revoke the credential it is in the middle of
	// writing.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer fence.release(ctx)

	// RE-READ UNDER THE CLAIM. The flags the processor scanned are advisory: between the
	// scan and this point a successful re-issuance can have discharged the
	// credential-cleanup obligation, and acting on the stale flag would revoke a
	// credential that had just been issued. This read is the first moment at which they
	// cannot change underneath the remedy.
	obligation, err := store.GetSubscriberSettlementObligation(ctx, subscriberID)
	if err != nil {
		return err
	}

	if !obligation.Outstanding() {
		logrus.WithField(
			"subscriber_id_hash", subscriberLogLabel(subscriberID),
		).Debug(
			"subscriber settlement: nothing is owed for this subscriber any more, so nothing was done",
		)

		return nil
	}

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return err
	}

	if err := s.settleCredentialCleanup(ctx, store, fence, subscriber, obligation); err != nil {
		return err
	}

	return s.settleGrantReconciliation(ctx, store, fence, subscriber, obligation)
}

// settleCredentialCleanup revokes a credential Blnk no longer accounts for and clears
// the row's record of it.
func (s *EventSubscriberService) settleCredentialCleanup(
	ctx context.Context,
	store eventSubscriberStore,
	fence *subscriberFence,
	subscriber *model.EventSubscriber,
	obligation model.SubscriberSettlementObligation,
) error {
	if !obligation.CredentialCleanupPending {
		return nil
	}

	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
	}

	if admin.IsConfigured() {
		phase, endPhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endPhase()

			return phaseErr
		}

		revokeErr := admin.RevokeSubscriber(phase, subscriber)
		endPhase()

		if revokeErr != nil {
			logrus.WithFields(fields).WithField(
				"revocation_error", sanitizeLogValue(revokeErr.Error(), maxLoggedErrorLength),
			).Error(
				"subscriber settlement: revoking this principal's Kafka access failed, so its " +
					"pending credential cleanup remains outstanding",
			)

			return revokeErr
		}
	}

	// The broker no longer holds anything for this principal, so the row must stop saying
	// it does. This is the write that ALSO clears the marker — see
	// ClearSubscriberCredential — which is why nothing else in this function touches it.
	if err := store.ClearSubscriberCredential(ctx, subscriber.SubscriberID, fence.token); err != nil {
		logrus.WithFields(fields).WithField(
			"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"subscriber settlement: the principal's Kafka access was revoked but clearing the " +
				"registry's credential record failed, so the registry still over-reports it",
		)

		return err
	}

	logrus.WithFields(fields).Info(
		"subscriber settlement: the principal's Kafka credential was revoked and the registry's " +
			"credential record cleared",
	)

	return nil
}

// settleGrantReconciliation brings the broker's ACL bindings back into line with the
// row.
func (s *EventSubscriberService) settleGrantReconciliation(
	ctx context.Context,
	store eventSubscriberStore,
	fence *subscriberFence,
	subscriber *model.EventSubscriber,
	obligation model.SubscriberSettlementObligation,
) error {
	if !obligation.GrantReconcilePending {
		return nil
	}

	fields := logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(subscriber.SubscriberID),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
	}

	// A tombstoned row is discharged without being acted on. Reconciling TO it would
	// re-create the grants its deregistration is removing, and the tombstone is already a
	// durable marker the deregistration retry finds — so this obligation would otherwise
	// be one no pass could ever satisfy.
	if subscriber.IsRevocationPending() {
		if err := store.ClearSubscriberGrantReconcilePending(
			ctx, subscriber.SubscriberID, fence.token,
		); err != nil {
			return err
		}

		logrus.WithFields(fields).Warn(
			"subscriber settlement: this subscriber is being deregistered, so its broker-side " +
				"grant was NOT reconciled to the row; the pending-reconciliation marker was " +
				"discharged and the revocation tombstone remains the outstanding work",
		)

		return nil
	}

	prunePhase, endPrunePhase, err := fence.brokerPhaseContext(ctx)
	if err != nil {
		endPrunePhase()

		return err
	}

	pruned, err := s.pruneBrokerAccess(prunePhase, subscriber)
	endPrunePhase()

	if err != nil {
		return err
	}

	grantPhase, endGrantPhase, err := fence.brokerPhaseContext(ctx)
	if err != nil {
		endGrantPhase()

		return err
	}

	granted, err := s.grantBrokerAccess(grantPhase, subscriber)
	endGrantPhase()

	if err != nil {
		return err
	}

	// Discharged only now that the broker and the row demonstrably agree.
	if err := store.ClearSubscriberGrantReconcilePending(
		ctx, subscriber.SubscriberID, fence.token,
	); err != nil {
		logrus.WithFields(fields).WithField(
			"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"subscriber settlement: the broker was reconciled to the registry row but the " +
				"pending-reconciliation marker could not be cleared, so the reconciliation will " +
				"be repeated harmlessly on a later pass",
		)

		return err
	}

	logrus.WithFields(fields).WithFields(logrus.Fields{
		"acl_bindings_removed": pruned,
		"acl_bindings_created": granted,
	}).Info(
		"subscriber settlement: the broker's grants for this subscriber were reconciled to the " +
			"registry row",
	)

	return nil
}

// markCredentialOrphaned persists the issuance-orphan state, best-effort.
func (s *EventSubscriberService) markCredentialOrphaned(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fields logrus.Fields,
	orphanedAt time.Time,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.MarkSubscriberCredentialOrphaned(ctx, subscriber.SubscriberID, orphanedAt); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the orphaned-credential marker could not be written, so this " +
				"exposure is recorded nowhere but the logs and no gauge or alert can see it",
		)

		return false
	}

	return true
}

// markRevocationFailed persists the failed-deregistration state, best-effort.
func (s *EventSubscriberService) markRevocationFailed(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fields logrus.Fields,
	failedAt time.Time,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.MarkSubscriberRevocationFailed(ctx, subscriber.SubscriberID, failedAt); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the failed-revocation marker could not be written, so the row records " +
				"only that a deregistration began and not that the broker refused it",
		)

		return false
	}

	return true
}

// WithKafkaAdminResolver installs the resolver this service asks for a PROCESS-WIDE
// administrative client the first time it needs one.
//
// Parameters:
//   - resolve func() (subscriberPrincipalProvisioner, error): the resolver, or nil.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithKafkaAdminResolver(
	resolve func() (subscriberPrincipalProvisioner, error),
) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resolveAdmin = resolve

	return s
}

// WithBackgroundScheduler installs the scheduler compensating work is handed to.
//
// Parameters:
//   - schedule func(func()): the scheduler, or nil for inline execution.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithBackgroundScheduler(schedule func(func())) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deferWork = schedule

	return s
}

// schedule hands a compensating task to the installed scheduler, or runs it inline when
// there is none.
func (s *EventSubscriberService) schedule(task func()) {
	if task == nil {
		return
	}

	if s == nil {
		task()

		return
	}

	s.mu.Lock()
	deferWork := s.deferWork
	s.mu.Unlock()

	if deferWork == nil {
		task()

		return
	}

	deferWork(task)
}

// scheduleSettlement hands the post-response settlement of one operation to the
// scheduler, on the context that work is entitled to.
//
// THE ONE PLACE THAT DECIDES WHICH CLOCK A SETTLEMENT RUNS ON, because the answer
// depends on something only this function knows: whether the settlement is going to run
// after the response or inside it.
//
//   - A scheduler is installed, so the settlement runs after the response has been
//     written and nobody is waiting on it. It is rebased onto a wall clock of its own —
//     see rebaseSubscriberSettlementClock for why that is not an extension of the
//     caller's promise but a recognition that the promise was already kept.
//   - No scheduler, so the settlement runs INLINE and the caller is still waiting. It
//     stays a slice of the caller's promise, which is what keeps the endpoint answering
//     inside its budget rather than inside its budget plus a fresh one per level of
//     cleanup.
//
// Either way the window is opened ONCE, by subscriberCleanupContext, so a phase derived
// inside the task shares this instant instead of carving a second slice out of it.
//
// Parameters:
//   - ctx context.Context: the operation's context, whose wall clock may be spent.
//   - task func(context.Context): the settlement, run with the context it is entitled
//     to. Not called when nil.
func (s *EventSubscriberService) scheduleSettlement(ctx context.Context, task func(context.Context)) {
	if task == nil {
		return
	}

	// Read BEFORE the task is handed over: the decision must describe the arm this
	// settlement is actually taking, and a scheduler installed moments later must not
	// retroactively change the clock an inline run already used.
	deferred := s.defersWork()

	s.schedule(func() {
		base := ctx
		if deferred {
			base = rebaseSubscriberSettlementClock(ctx)
		}

		settle, done := subscriberCleanupContext(base)
		defer done()

		task(settle)
	})
}

// defersWork reports whether a scheduler is installed, so a caller can ask the broker
// to defer work only when there is somewhere for it to be deferred TO.
func (s *EventSubscriberService) defersWork() bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deferWork != nil
}
