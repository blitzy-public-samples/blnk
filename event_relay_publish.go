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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// publishingMayProceed reports whether the relay may keep publishing rows it has
// ALREADY CLAIMED. It consults the CONTEXT ONLY, and the asymmetry with
// shouldClaimAnotherBatch is deliberate:
func publishingMayProceed(ctx context.Context) bool {
	return ctx.Err() == nil
}

// leaseDeadline is the instant this batch's claim lapses if nothing renews it, and its
// rows become claimable by anyone else.
func (p *EventRelayProcessor) leaseDeadline(claimedAt time.Time) time.Time {
	return claimedAt.Add(p.lockDuration)
}

// processBatch claims one batch of due outbox rows and publishes every one of them.
func (p *EventRelayProcessor) processBatch(ctx context.Context) int {
	rows, err := p.store.ClaimPendingEventOutbox(ctx, p.batchSize, p.lockDuration)
	if err != nil {
		withLoggableCause(nil, err).Error("failed to claim event outbox entries")

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	// The claim instant, captured from the relay's own clock rather than read back from
	// the row's last_attempted_at. The publish-duration histogram is documented as
	// measuring claim to broker acknowledgement, and a duration must not be computed
	// across two clocks: subtracting a database timestamp from a local one reports the
	// skew between them as latency.
	claimedAt := p.now()

	// REPORTED, NOT DECIDED. This is the window's state at the moment the batch was
	// claimed, and it exists so the batch log line says which regime the relay believes it
	// is in. The DECISION is taken per row, immediately before each enqueue, by
	// deliverLegacyWebhook — see F12's reasoning there.
	windowAtClaim := p.windowStateAt(claimedAt)

	logrus.WithFields(logrus.Fields{
		"claimed":      len(rows),
		"window_state": windowAtClaim.String(),
	}).Infof("Processing %d event outbox entries", len(rows))

	// THE LEASE IS RENEWED WHILE THE BATCH IS IN FLIGHT, and this is not an optimisation.
	publishing, stopPublishing := context.WithCancel(ctx)
	defer stopPublishing()

	stopRenewals := p.renewLeaseWhileInFlight(
		ctx, rows[0].ClaimToken, len(rows), p.leaseDeadline(claimedAt), stopPublishing,
	)
	defer stopRenewals()

	groups := groupEventRowsByPartitionKey(rows)
	permits := semaphore.NewWeighted(int64(p.concurrency))

	var batch sync.WaitGroup

	for _, group := range groups {
		// Checked before EVERY group, not only at the top of the batch, so an ABORT stops
		// dispatching work that has not started yet. The rows left behind keep their lease
		// and re-enter the claimable set when it expires, which is the same mechanism that
		// recovers a crashed relay. A graceful Stop deliberately does NOT stop here — see
		// publishingMayProceed for why the two signals are treated differently.
		if !publishingMayProceed(publishing) {
			logrus.WithField("event_id", group[0].EventID).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		// Acquire before spawning, so the number of in-flight publishes is bounded by the
		// permit count rather than by the number of groups.
		if acquireErr := permits.Acquire(publishing, 1); acquireErr != nil {
			withLoggableCause(logrus.WithField("event_id", group[0].EventID), acquireErr).
				Debug("event relay: stopped dispatching this batch; the remaining rows keep their lease")

			break
		}

		batch.Add(1)

		go func(rows []model.EventOutbox) {
			defer batch.Done()
			defer permits.Release(1)

			// SEQUENTIALLY WITHIN THE GROUP. Every row here shares one partition key, so
			// this loop is the per-partition-key ordering guarantee in code.
			for _, row := range rows {
				// Cancellation is honoured BETWEEN rows as well as between groups. A group is
				// normally one row — the claim returns at most one per partition key — but it is
				// not bounded to one, and a group that kept publishing through an abort would be
				// the one place cancellation did not reach.
				if !publishingMayProceed(publishing) {
					return
				}

				p.processRow(publishing, row, claimedAt)
			}
		}(group)
	}

	// Waiting here is what makes Stop's promise true: the loop goroutine cannot return —
	// and so p.wg cannot drain — until the batch in flight has finished.
	batch.Wait()

	return len(rows)
}

// renewLeaseWhileInFlight starts the heartbeat that keeps a claimed batch's lease
// alive, and returns the function that stops it.
func (p *EventRelayProcessor) renewLeaseWhileInFlight(
	ctx context.Context,
	claimToken string,
	claimed int,
	heldUntil time.Time,
	onLeaseLost func(),
) func() {
	if strings.TrimSpace(claimToken) == "" {
		// Nothing to renew against. Reachable only from a store that returned rows without a
		// token, which the repository never does; a no-op stop keeps the call site
		// unconditional.
		return func() {}
	}

	interval := p.lockDuration / eventRelayLeaseRenewalDivisor
	if interval < eventRelayMinLeaseRenewalInterval {
		interval = eventRelayMinLeaseRenewalInterval
	}

	done := make(chan struct{})

	var (
		stopOnce sync.Once
		finished sync.WaitGroup
	)

	finished.Add(1)

	go func() {
		defer finished.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				// Sampled before the round trip, so its latency shortens rather than
				// lengthens the hold this relay claims to have.
				issuedAt := p.now()

				// A detached context, for the same reason every other bookkeeping write uses one: a
				// renewal is work the relay already owes on rows it is holding, and a shutdown
				// mid-batch must not be the reason those rows lose their lease early.
				renewal, cancel := detachedBookkeepingContext(ctx)
				renewed, err := p.store.RenewEventOutboxLease(renewal, claimToken, p.lockDuration)
				cancel()

				if err != nil {
					withLoggableCause(logrus.WithFields(logrus.Fields{
						"claim_token": claimToken,
						"claimed":     claimed,
						"lease":       p.lockDuration.String(),
						"held_until":  heldUntil.UTC().Format(time.RFC3339Nano),
					}), err).Warn(
						"event relay: renewing the lease on a batch in flight failed; the rows may be " +
							"reclaimed and republished by another instance, which is suppressed at the " +
							"subscriber on event_id",
					)

					if remaining := heldUntil.Sub(p.now()); remaining > 0 {
						// Still inside the lease this relay last secured, so the rows are still its own
						// and there are further ticks to recover in.
						continue
					}

					logrus.WithFields(logrus.Fields{
						"claim_token": claimToken,
						"claimed":     claimed,
						"lease":       p.lockDuration.String(),
						"held_until":  heldUntil.UTC().Format(time.RFC3339Nano),
					}).Warn(
						"event relay: the lease on a batch in flight could not be held and has lapsed; " +
							"publishing is being stopped so this instance cannot put a second copy on " +
							"the topic behind whoever reclaims the rows",
					)

					onLeaseLost()

					return
				}

				if renewed == 0 {
					// Every row is terminal. Nothing is left to hold.
					return
				}

				heldUntil = issuedAt.Add(p.lockDuration)
			}
		}
	}()

	return func() {
		stopOnce.Do(func() { close(done) })
		finished.Wait()
	}
}

// groupEventRowsByPartitionKey partitions a claimed batch into per-partition-key
// groups, preserving the claim's order within each group and between groups.
func groupEventRowsByPartitionKey(rows []model.EventOutbox) [][]model.EventOutbox {
	groups := make([][]model.EventOutbox, 0, len(rows))
	indexByKey := make(map[string]int, len(rows))

	for _, row := range rows {
		// GROUPED BY THE KEY THE PUBLISH ACTUALLY USES, not by the stored column.
		//
		// A blank key would collapse every unkeyed row into one group and serialise them.
		key := row.EffectiveKey()
		if key == "" {
			key = fmt.Sprintf("id:%d", row.ID)
		}

		if index, seen := indexByKey[key]; seen {
			groups[index] = append(groups[index], row)

			continue
		}

		indexByKey[key] = len(groups)
		groups = append(groups, []model.EventOutbox{row})
	}

	return groups
}

// processRow performs ONE publish attempt for one claimed row and records the outcome.
func (p *EventRelayProcessor) processRow(
	ctx context.Context,
	row model.EventOutbox,
	claimedAt time.Time,
) {
	// THE RELAY'S OWN SPAN over everything one claimed row costs: the legacy leg, the
	// Kafka publish, the retry decision, the dead-letter hand-off and the durable
	// transition. The publisher's producer span nests inside it, so a trace shows the
	// publish in the context of the row handling around it rather than on its own.
	ctx, span := tracer.Start(ctx, "relay.process_event_outbox_row", linkToCapturedTrace(row)...)
	defer span.End()

	// The attempt this publish represents. attempts counts the failures RECORDED so far —
	// the claim does not increment it, MarkEventFailed does — so the attempt now under way
	// is one past it. Getting this wrong would misreport the backoff schedule and the
	// attempt metric label together.
	attempt := row.Attempts + 1

	// Bounded and hashed on the same terms as the producer span's: the topic through the
	// bounded label, the event type through its own, the event id in full because it is
	// Blnk-generated and is what an operator searches by. The attempt number makes a retry
	// distinguishable from a first delivery without opening the row.
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", boundedTopicLabel(row.Topic)),
		attribute.String("messaging.message.id", row.EventID),
		attribute.String("blnk.event.type", boundedEventTypeLabel(row.EventType)),
		attribute.Int("blnk.publish.attempt", attempt),
		attribute.Int("blnk.publish.max_attempts", p.rowMaxAttempts(row)),
	)

	// The legacy leg first, and its outcome kept: whether the webhook is still owed
	// decides which terminal transition this row takes below. The sunset boundary is
	// evaluated inside, per row, immediately before the enqueue.
	legacyLeg, legacyErr := p.deliverLegacyWebhook(ctx, row)

	// A row whose Kafka leg is already recorded is here ONLY for its outstanding webhook.
	if row.KafkaDispatchedAt != nil {
		// NO COORDINATE is supplied on this pass, and that is deliberate: nothing was
		// published, so there is no new record to name — and the row already carries the
		// coordinate of the record its earlier successful publish produced. Both marking
		// transitions COALESCE the coordinate for exactly this case, so passing the zero
		// value leaves the stored one intact rather than erasing it.
		p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, model.BrokerRecord{}, false)

		return
	}

	// The publish is bounded so it cannot complete on a claim this relay no longer holds,
	// and the bound has TWO parts because there are two different questions to answer.
	publishCtx, cancelPublish := context.WithTimeout(ctx, eventRelayRowPublishBudget)
	defer cancelPublish()

	// The request is built by the row-to-request conversion the dead-letter write and the
	// replay both use, rather than assembled here. That is what puts THE STORED CANONICAL
	// ENVELOPE on the wire: PublishRequestFromOutbox carries event_raw, so the bytes this
	// publish sends are the bytes composed once at capture, and a retry, the dead-letter
	// copy and a replay of the same row are byte-identical by construction rather than by
	// the serialiser happening not to have changed between them. Assembling the envelope
	// here re-composed it on every attempt, which produced the same bytes today and
	// silently different bytes the first time an envelope member was added or reordered.
	request := PublishRequestFromOutbox(row, attempt)
	// The budget is passed so the publisher can tell a failure that still has attempts left
	// from the one that spent the last of them, and so its log line reads "attempt 3 of 5".
	request.MaxAttempts = p.rowMaxAttempts(row)
	request.Purpose = PublishPurposeOriginal
	request.ClaimedAt = claimedAt

	result, err := p.publisher.PublishToTopic(publishCtx, request)

	p.logAttempt(row, attempt, result, err)

	if err != nil {
		// The Kafka leg failed, so this row keeps its Kafka retry budget and is retried as a
		// whole. Whatever the legacy leg did is deliberately NOT settled here: an enqueue
		// that succeeded is recorded by webhook_dispatched and will not repeat, and one that
		// failed is retried on the next claim without having cost a Kafka attempt. Recording
		// a webhook failure against a row that is going to be re-published anyway would spend
		// the legacy budget on a Kafka outage.
		p.recordFailedAttempt(ctx, row, attempt, result, err)

		return
	}

	p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, result.Record, true)
}

// settleAfterKafkaSuccess drives a row whose KAFKA leg is complete to the right state,
// which depends entirely on what the legacy leg did.
func (p *EventRelayProcessor) settleAfterKafkaSuccess(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	legacyLeg legacyLegOutcome,
	legacyErr error,
	record model.BrokerRecord,
	published bool,
) {
	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if legacyLeg == legacyLegOwed {
		p.deferLegacyLeg(bookkeeping, row, attempt, legacyErr, record, published)

		return
	}

	if markErr := p.store.MarkEventDispatched(bookkeeping, row.ID, row.ClaimToken, record); markErr != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), markErr).Error(
			"event relay: the event was published but its outbox row could not be marked dispatched; " +
				"it will be republished when its lease expires and must be suppressed on event_id",
		)

		// NOT COUNTED. The broker has the event but nothing durable says so, and this row is
		// going to be published again when its lease expires — counting here and again after
		// that republish would report one event twice. See recordDurableEventDelivery.
		return
	}

	// THE DURABLE TRANSITION HAS COMMITTED, which is the moment the delivery is recorded
	// and therefore the only moment it may be counted.
	if published {
		recordDurableEventDelivery(bookkeeping, row.Topic, row.EventType)
	}

	// THE ONE PER-EVENT DELIVERY COUNT, recorded here and only here for this arm.
	recordEventDispatched(bookkeeping, row)
}

// recordEventDispatched increments the per-event delivery count for a row that has just
// reached the dispatched state.
func recordEventDispatched(ctx context.Context, row model.EventOutbox) {
	metrics.EventsDispatchedTotal.Add(ctx, 1, otelmetric.WithAttributes(
		attribute.String(publishAttrTopic, boundedTopicLabel(row.Topic)),
		attribute.String(publishAttrEventType, boundedEventTypeLabel(row.EventType)),
	))
}

// deferLegacyLeg records a Kafka leg that is done alongside a legacy webhook leg that
// is still owed, and reports the arm the datasource chose.
func (p *EventRelayProcessor) deferLegacyLeg(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
	record model.BrokerRecord,
	published bool,
) {
	reason := "the legacy webhook enqueue failed"
	if cause != nil {
		reason = relayFailureReason(cause)
	}

	// row.WebhookAttempts is the count BEFORE this failure, so the attempt now being
	// recorded is one past it — the same relationship attempt has to row.Attempts.
	retryAfter := p.retry.backoffFor(row.WebhookAttempts + 1)

	outcome, err := p.store.MarkEventWebhookPending(
		ctx, row.ID, row.ClaimToken, reason, retryAfter, record,
	)
	if err != nil {
		// The row keeps its lease and returns to the claimable set when it expires, so
		// nothing is lost: the Kafka publish is not repeated, because kafka_dispatched_at was
		// already stamped by whichever earlier pass succeeded, or will be stamped by the next
		// pass's own success.
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
			"event relay: the event was published to Kafka but its outstanding legacy webhook " +
				"leg could not be recorded; the row keeps its lease and is retried when it expires",
		)

		// NOT COUNTED: nothing durable records this delivery yet, and the row will be
		// re-settled on a later claim which is where the increment belongs.
		return
	}

	// THE DURABLE TRANSITION HAS COMMITTED. kafka_dispatched_at now names this delivery,
	// so the event is counted here — once — whether the webhook that follows it succeeds,
	// is retried or is ultimately abandoned.
	if published {
		recordDurableEventDelivery(ctx, row.Topic, row.EventType)
	}

	fields := p.rowFields(row, attempt)
	fields["webhook_attempts"] = outcome.WebhookAttempts
	fields["webhook_max_attempts"] = p.rowMaxAttempts(row)
	fields["reason"] = reason

	if outcome.Abandoned {
		// THE SECOND ARM THAT REACHES dispatched. The abandon arm moves the row to dispatched
		// with the webhook given up on, so this event's delivery is now durably recorded and
		// it must be counted — omitting it here would under-report deliveries by exactly the
		// number of events whose legacy leg was abandoned, and make the dead-letter rate look
		// higher than it is.
		recordEventDispatched(ctx, row)

		logrus.WithFields(fields).Error(
			"event relay: the legacy webhook leg for this event is ABANDONED after exhausting its " +
				"own enqueue budget. The event IS on its Kafka topic; the webhook will never be " +
				"delivered. Any subscriber still consuming webhooks has missed this event",
		)

		return
	}

	fields["retry_after"] = retryAfter.String()
	logrus.WithFields(fields).Warn(
		"event relay: the event is published to Kafka and its legacy webhook leg is still owed; " +
			"the row stays claimable for the webhook alone and will not be republished",
	)
}

// legacyLegDue reports whether the legacy webhook leg must run for an event being
// dispatched at instant, through the injectable seam so a test can pin the clock.
func (p *EventRelayProcessor) legacyLegDue(instant time.Time) bool {
	if p == nil || p.dualDeliveryActive == nil {
		return false
	}

	return p.dualDeliveryActive(instant)
}

// windowStateAt resolves where an instant falls relative to the dual-delivery window,
// through the injectable seam so a test can pin it.
func (p *EventRelayProcessor) windowStateAt(instant time.Time) WebhookWindowState {
	if p == nil || p.windowState == nil {
		return WebhookWindowUnavailable
	}

	return p.windowState(instant)
}

// rowMaxAttempts returns the retry budget in force for a row.
func (p *EventRelayProcessor) rowMaxAttempts(row model.EventOutbox) int {
	if row.MaxAttempts > 0 {
		return row.MaxAttempts
	}

	if p.retry.maxAttempts > 0 {
		return p.retry.maxAttempts
	}

	return 1
}
