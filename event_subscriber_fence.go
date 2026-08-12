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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// subscriberBrokerPhaseBudget bounds ONE broker phase of a fenced operation.
const subscriberBrokerPhaseBudget = 3 * SubscriberCredentialIssuanceBudget

// SubscriberProvisioningFenceLease is how long an issuance or revocation holds its
// fence.
const SubscriberProvisioningFenceLease = subscriberBrokerPhaseBudget +
	subscriberCleanupBudget + SubscriberCredentialIssuanceBudget

// subscriberFence is a held provisioning claim, and the only way to reach the broker.
type subscriberFence struct {
	store        eventSubscriberStore
	subscriberID string

	// token is the claim this operation holds. It is a CONCURRENCY-CONTROL SECRET: it must
	// never reach an API response or a log line, because a caller holding another
	// operation's token could complete writes against a subscriber it does not own. It is
	// deliberately absent from model.EventSubscriber for the same reason.
	token string

	// lease is how long one claim renewal is good for, carried so the heartbeat renews on the
	// same term the claim was taken on.
	lease time.Duration

	// stop ends the heartbeat; done is closed once it has exited, so release can wait for it
	// and no renewal can land after the claim has been cleared.
	stop chan struct{}
	done chan struct{}

	stopOnce sync.Once
	released sync.Once
}

// fenceSubscriber claims a subscriber for one operation and returns the claim.
func fenceSubscriber(
	ctx context.Context,
	store eventSubscriberStore,
	subscriberID string,
) (*subscriberFence, error) {
	token, err := store.ClaimSubscriberForProvisioning(
		ctx, subscriberID, SubscriberProvisioningFenceLease)
	if err != nil {
		// NIL, NOT A FENCE WITH NO TOKEN. A half-built fence looks like a held claim to every
		// caller that does not inspect its fields: `defer fence.release(ctx)` on one would
		// issue a release for a claim this call never took, and `renew` has to carry an
		// explicit guard for a state that should not be constructible. Returning nil makes
		// the refusal unmistakable, and release is deliberately nil-safe — see
		// TestSubscriberFence_ReleaseIsIdempotentAndNilSafe — so a caller may still defer the
		// release before it knows whether the claim succeeded.
		return nil, err
	}

	fence := &subscriberFence{
		store:        store,
		subscriberID: strings.TrimSpace(subscriberID),
		token:        token,
	}

	// THE CLAIM IS RENEWED WHILE THE OPERATION RUNS. A provisioning claim is a LEASE, and
	// the operations that hold one make several broker round trips under it — so a slow
	// broker, or a compensation that outlives the request, could let the lease expire
	// underneath an operation still working. The heartbeat renews it on a fraction of its
	// term, which is what keeps the fence true for the whole operation rather than only
	// for its first few seconds.
	fence.lease = SubscriberProvisioningFenceLease
	fence.stop = make(chan struct{})
	fence.done = make(chan struct{})

	go fence.heartbeat()

	return fence, nil
}

// renew proves the claim is still this caller's and extends it by one lease.
func (f *subscriberFence) renew(ctx context.Context) error {
	if f == nil || f.token == "" {
		// Unreachable through fenceSubscriber, which returns the claim and the error
		// together. Reported rather than treated as "held" because a fence with no token
		// constrains nothing, and silently proceeding would restore the unfenced behaviour
		// exactly.
		return apierror.NewAPIError(apierror.ErrConflict,
			"No provisioning claim is held for this subscriber, so the operation was abandoned",
			errors.New("event subscriber: a broker phase was attempted without a provisioning claim"))
	}

	return f.store.RenewSubscriberProvisioningFence(
		ctx, f.subscriberID, f.token, SubscriberProvisioningFenceLease)
}

// consumed records that the claim went away with the row it was held on.
func (f *subscriberFence) consumed() {
	if f == nil {
		return
	}

	// The Once is CONSUMED rather than the token blanked, so an explicit release already made
	// before this point is still not repeated, and a deferred release after it does nothing.
	f.released.Do(func() {})
}

// release clears the claim so a retry need not wait out the lease.
func (f *subscriberFence) release(ctx context.Context) {
	if f == nil || f.token == "" {
		return
	}

	f.released.Do(func() {
		// STOPPED AND WAITED FOR FIRST, so no renewal can land after the claim has been cleared
		// and re-fence a subscriber this operation has finished with.
		f.stopHeartbeat()

		releaseCtx, cancel := subscriberCleanupContext(ctx)
		defer cancel()

		if err := f.store.ReleaseSubscriberProvisioningFence(
			releaseCtx, f.subscriberID, f.token); err != nil {
			withLoggableCause(logrus.WithField(
				"subscriber_id_hash", subscriberLogLabel(f.subscriberID),
			), err).Warn(
				"event subscriber: releasing the provisioning fence failed; the subscriber stays " +
					"fenced until its claim lease expires, after which the next attempt proceeds",
			)
		}
	})
}

// brokerPhaseContext bounds one broker phase and confirms the claim before it starts.
func (f *subscriberFence) brokerPhaseContext(
	ctx context.Context,
) (context.Context, context.CancelFunc, error) {
	if err := f.renew(ctx); err != nil {
		return ctx, func() {}, err
	}

	phase, cancel := context.WithTimeout(ctx, subscriberBrokerPhaseBudget)

	return phase, cancel, nil
}

// subscriberCleanupBudget bounds a compensating write when no absolute deadline is in
// force.
const subscriberCleanupBudget = SubscriberCredentialIssuanceBudget

// subscriberIssuanceDeadlineKey is the context key the absolute issuance deadline travels on.
type subscriberIssuanceDeadlineKey struct{}

// withSubscriberIssuanceDeadline attaches the ONE absolute deadline an issuance is
// bounded by.
func withSubscriberIssuanceDeadline(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, subscriberIssuanceDeadlineKey{}, deadline)
}

// subscriberIssuanceDeadline reads the absolute issuance deadline, if one is in force.
func subscriberIssuanceDeadline(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}

	deadline, ok := ctx.Value(subscriberIssuanceDeadlineKey{}).(time.Time)
	if !ok || deadline.IsZero() {
		return time.Time{}, false
	}

	return deadline, true
}

// subscriberCompensationFloor is the smallest window a compensating operation is ever
// given.
const subscriberCompensationFloor = subscriberCompensationReserve

// boundedCompensationDeadline resolves the instant a compensating operation must finish
// by.
func boundedCompensationDeadline(ctx context.Context, fallback time.Duration) time.Time {
	now := time.Now()
	ceiling := now.Add(fallback)

	absolute, ok := subscriberIssuanceDeadline(ctx)
	if !ok {
		return ceiling
	}

	// The forward path overran the absolute instant, so there is nothing left of the
	// reserve to compensate inside. Grant the floor: bounded, named, and once per request
	// because the cleanup helpers rebase the value they read.
	if !absolute.After(now) {
		absolute = now.Add(subscriberCompensationFloor)
	}

	if absolute.Before(ceiling) {
		return absolute
	}

	return ceiling
}

// subscriberCleanupContext derives the context a compensating write runs on.
func subscriberCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// ONE PHASE BUILDER, so this and kafkaCleanupContext cannot disagree about when a
	// compensation ends. Both were written for the same bound and arrived reading two
	// different context keys, which meant the wall clock one of them attached was
	// invisible to the other and every broker cleanup silently took a fresh ten seconds.
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, subscriberCleanupBudget, true)
}

// requireSubscriberIdentifier rejects a blank subscriber id before anything is spent on
// it.
func requireSubscriberIdentifier(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber ID is required",
			errors.New("event subscriber: the subscriber id is blank"),
		)
	}

	return nil
}

// maxSubscriberNameLength bounds the human label.
const maxSubscriberNameLength = 256

// subscriberFenceWasLost reports whether an error is the repository's lost-fence
// conflict.
func subscriberFenceWasLost(err error) bool {
	if err == nil {
		return false
	}

	var apiErr apierror.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Code != apierror.ErrConflict {
		return false
	}

	// Details is an interface{} carrying the wrapped cause, so it is rendered rather than
	// type-asserted: the marker must be found whether the producer wrapped an error, a
	// string or a struct that prints one.
	if detail, ok := apiErr.Details.(error); ok {
		return strings.Contains(detail.Error(), subscriberFenceLostMarker)
	}
	if apiErr.Details != nil {
		return strings.Contains(fmt.Sprint(apiErr.Details), subscriberFenceLostMarker)
	}

	return false
}

// subscriberFenceLostMarker is the phrase database.subscriberFenceLost puts in every
// lost-fence error, and the one subscriberFenceWasLost matches on.
const subscriberFenceLostMarker = "the provisioning claim was no longer held"

// THE ISSUANCE WALL CLOCK HAS ONE CONTEXT KEY, and it is subscriberIssuanceDeadlineKey.

// subscriberDurabilityReserve is the final slice of the SLA, held back so that the
// record of an unaccounted credential can always be written.
const subscriberDurabilityReserve = 500 * time.Millisecond

// withSubscriberSLA attaches the wall-clock instant the whole operation must finish by.
func withSubscriberSLA(ctx context.Context, deadline time.Time) context.Context {
	return withSubscriberIssuanceDeadline(ctx, deadline)
}

// subscriberSLADeadline reads the wall-clock instant back, if one was attached.
func subscriberSLADeadline(ctx context.Context) (time.Time, bool) {
	return subscriberIssuanceDeadline(ctx)
}

// subscriberCompensationWindowKey is the context key the RESOLVED compensation window
// travels on, as distinct from the wall clock it was carved out of.
type subscriberCompensationWindowKey struct{}

// withSubscriberCompensationWindow publishes the instant a compensation phase resolved
// to.
func withSubscriberCompensationWindow(ctx context.Context, window time.Time) context.Context {
	return context.WithValue(ctx, subscriberCompensationWindowKey{}, window)
}

// subscriberCompensationWindow reads back the open compensation window, if there is
// one.
func subscriberCompensationWindow(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}

	window, ok := ctx.Value(subscriberCompensationWindowKey{}).(time.Time)
	if !ok || window.IsZero() {
		return time.Time{}, false
	}

	return window, true
}

// subscriberPhaseContext derives one phase of the wall clock.
func subscriberPhaseContext(
	ctx context.Context,
	headroom, fallback time.Duration,
	detached bool,
) (context.Context, context.CancelFunc) {
	// THE WORK PHASE honours the caller's cancellation and gets no floor. Its whole
	// purpose is to keep the caller's promise, so extending it past a deadline that has
	// already passed would do work the caller has stopped waiting for — and it would hide
	// the expiry from the classifier that turns a spent budget into a timeout rather than
	// a server fault.
	if !detached {
		sla, ok := subscriberSLADeadline(ctx)
		if !ok {
			return context.WithTimeout(ctx, fallback)
		}

		return context.WithDeadline(ctx, sla.Add(-headroom))
	}

	// A COMPENSATION ALREADY IN PROGRESS IS SHARED, to the same instant.
	deadline, open := subscriberCompensationWindow(ctx)
	if !open {
		// THE FIRST COMPENSATION OF THE REQUEST. Its slice is carved off the wall clock by
		// subtracting the headroom this phase must leave behind it, and
		// boundedCompensationDeadline then applies the two rules a compensation is subject to
		// whatever it is compensating: it may never be given more than its own budget, and a
		// slice that has already elapsed yields the floor rather than a dead context. Passing
		// the reduced instant rather than duplicating that arithmetic here is what keeps ONE
		// function deciding when a compensation ends.
		phase := ctx
		if sla, hasSLA := subscriberSLADeadline(ctx); hasSLA && headroom > 0 {
			phase = withSubscriberSLA(ctx, sla.Add(-headroom))
		}

		deadline = boundedCompensationDeadline(phase, fallback)

		// THE RESERVE IS A SLICE OF THE PROMISE, NEVER AN EXTENSION OF IT.
		if sla, hasSLA := subscriberSLADeadline(ctx); hasSLA &&
			sla.After(time.Now()) && deadline.After(sla) {
			deadline = sla
		}
	}

	// DETACHED FROM CANCELLATION, STILL BOUNDED BY THE REQUEST'S OWN DEADLINE. This is the
	// only context.WithoutCancel in the subscriber lifecycle, together with the one in
	// event_admin.go, and it is written as this exact expression because WithoutCancel
	// strips a real deadline along with the cancellation — the absolute instant travels as
	// a context value precisely so it can be put back here.
	base := withSubscriberIssuanceDeadline(context.WithoutCancel(ctx), deadline)

	// The window is published as well as the deadline, so a phase derived inside this one shares
	// it exactly instead of carving a second slice out of what is left.
	return context.WithDeadline(withSubscriberCompensationWindow(base, deadline), deadline)
}

// It is granted ONCE per request rather than once per nested phase, which is what
// subscriberCompensationWindow enforces.

// subscriberDurabilityContext derives the context the durable exposure markers are
// written on.
func subscriberDurabilityContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return subscriberPhaseContext(ctx, 0, subscriberDurabilityReserve, true)
}

// abandonUpdateAfterLostFence records what an update leaves behind when its claim
// lapses, and returns the error the caller should see.
func (s *EventSubscriberService) abandonUpdateAfterLostFence(
	subscriber *model.EventSubscriber,
	stored subscriberAuthorization,
	pruned int,
	cause error,
) error {
	if pruned == 0 {
		// Nothing reached the broker, so there is no residue to describe and a conflict is the
		// whole story.
		return cause
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":   subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":       subscriberLogLabel(subscriber.KafkaPrincipal),
		"acl_bindings_removed": pruned,
		"recorded_topics":      len(stored.topics),
	}).Warn(
		"event subscriber: an authorization change lost its provisioning claim after pruning Kafka " +
			"grants, so the registry was NOT updated and the subscriber now has LESS access than the " +
			"registry records; this is fail-closed and the next successful authorization change " +
			"restores it, or retry this one",
	)

	return cause
}

// subscriberFenceRenewalDivisor sets how often a held fence is renewed, as a fraction
// of the lease.
const subscriberFenceRenewalDivisor = 3

// subscriberFenceMinRenewalInterval floors the renewal interval so a short test lease cannot
// turn the heartbeat into a busy loop against the database.
const subscriberFenceMinRenewalInterval = 250 * time.Millisecond

// heartbeat renews the claim until it is stopped, the claim is lost, or the process ends.
func (f *subscriberFence) heartbeat() {
	defer close(f.done)

	interval := f.lease / subscriberFenceRenewalDivisor
	if interval < subscriberFenceMinRenewalInterval {
		interval = subscriberFenceMinRenewalInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			renewal, cancel := context.WithTimeout(context.Background(), subscriberCleanupBudget)
			err := f.store.RenewSubscriberProvisioningFence(renewal, f.subscriberID, f.token, f.lease)
			cancel()

			if err == nil {
				continue
			}

			logrus.WithField(
				"subscriber_id_hash", subscriberLogLabel(f.subscriberID),
			).WithField(
				"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
			).Error(
				"event subscriber: this operation's provisioning claim could not be renewed, so it is " +
					"no longer fenced; a concurrent issuance or deregistration can now interleave with " +
					"it, and this operation's own conditional writes are what will report that as a " +
					"conflict",
			)

			return
		}
	}
}

// stopHeartbeat ends the renewal loop and waits for it to exit, so no renewal can be in flight
// when the claim is cleared. Idempotent.
func (f *subscriberFence) stopHeartbeat() {
	// A fence whose claim was never granted has no heartbeat to stop, and waiting on its
	// nil done channel would block for ever. fenceSubscriber returns exactly that fence
	// alongside its error so callers can defer the release unconditionally.
	if f == nil || f.stop == nil || f.done == nil {
		return
	}

	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
}
