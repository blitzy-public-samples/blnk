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
	// THE CLAIM IS BOUNDED, and the bound is the only thing that distinguishes a relay that
	// is working slowly from one that has stopped. A claim whose cost outgrows its indexes
	// held a single statement open for 504 seconds in testing while this loop published
	// nothing and reported nothing — the goroutine was inside the driver, so there was no
	// tick to observe and no error to log. With a deadline the condition becomes a timeout
	// the repository names, this counter records and the next tick retries.
	claiming, cancelClaim := context.WithTimeout(ctx, eventRelayClaimBudget)
	claimStartedAt := p.now()
	rows, err := p.store.ClaimPendingEventOutbox(claiming, p.batchSize, p.lockDuration, p.currentKeyCursor())
	claimElapsed := p.now().Sub(claimStartedAt)
	cancelClaim()

	// Recorded on EVERY path, including the failures, because the reading that matters most
	// is the one taken while claims are going wrong: a duration that has climbed into
	// seconds is the leading indicator of the stall, and it is visible here before any
	// throughput counter moves.
	recordEventClaim(ctx, claimElapsed, len(rows), err, claiming.Err() != nil)

	if err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"claim_duration_ms": claimElapsed.Milliseconds(),
			"batch_size":        p.batchSize,
		}), err).Error("failed to claim event outbox entries")

		return 0
	}

	p.advanceKeyCursor(rows)

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
			for position, row := range rows {
				// Cancellation is honoured BETWEEN rows as well as between groups. A claim returns
				// a CONTIGUOUS RUN per partition key, so a group routinely holds several rows, and
				// a group that kept publishing through an abort would be the one place
				// cancellation did not reach.
				if !publishingMayProceed(publishing) {
					return
				}

				if p.processRow(publishing, row, claimedAt) {
					continue
				}

				// THE REST OF THE RUN IS ABANDONED, and this is the second half of the ordering
				// guarantee — the half SQL cannot express.
				//
				// This row's Kafka leg did not reach a recorded, durable state: the publish failed,
				// or it succeeded and the transition that records it did not. Either way the row
				// will be published again on a later claim. Publishing the rows BEHIND it now would
				// put event N+1 on the topic before event N, for the same partition key, which is
				// precisely the per-aggregate ordering this pipeline promises.
				//
				// The abandoned rows keep the lease this claim took and re-enter the claimable set
				// when it expires. The claim cannot hand them to anyone else in the meantime,
				// because a key's run is only ever taken from its head forward and this row is now
				// that head — so the next claim gets this row first, and its successors only once it
				// has settled.
				if remaining := len(rows) - position - 1; remaining > 0 {
					logrus.WithFields(logrus.Fields{
						"event_id":       row.EventID,
						"partition_key":  row.EffectiveKey(),
						"abandoned_rows": remaining,
					}).Warn(
						"event relay: a row of a partition-key run did not settle, so the rest of the run " +
							"is left for a later claim; publishing behind it would break this aggregate's order",
					)
				}

				return
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
//
// Parameters:
//   - ctx context.Context: cancels the publish; bookkeeping writes are detached from it.
//   - row model.EventOutbox: the claimed row.
//   - claimedAt time.Time: when the batch was claimed, for the publish-duration histogram.
//
// Returns:
//   - bool: whether this row's KAFKA leg reached a recorded, durable state — either
//     published and transitioned now, or already published on an earlier pass. False means
//     the row will be published again later, and the caller MUST NOT publish anything
//     behind it that shares its partition key. The legacy webhook leg deliberately does
//     not affect this answer: it carries no ordering promise, and a row whose Kafka leg is
//     recorded is never republished on its account.
func (p *EventRelayProcessor) processRow(
	ctx context.Context,
	row model.EventOutbox,
	claimedAt time.Time,
) bool {
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
		return p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, model.BrokerRecord{}, false)
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

		return false
	}

	return p.settleAfterKafkaSuccess(ctx, row, attempt, legacyLeg, legacyErr, result.Record, true)
}

// settleAfterKafkaSuccess drives a row whose KAFKA leg is complete to the right state,
// which depends entirely on what the legacy leg did.
//
// Returns:
//   - bool: whether the Kafka leg is now RECORDED as well as complete. False when the
//     transition that records it failed, which leaves the row to be published again — and
//     therefore stops the caller publishing its partition-key successors.
func (p *EventRelayProcessor) settleAfterKafkaSuccess(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	legacyLeg legacyLegOutcome,
	legacyErr error,
	record model.BrokerRecord,
	published bool,
) bool {
	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if legacyLeg == legacyLegOwed {
		return p.deferLegacyLeg(bookkeeping, row, attempt, legacyErr, record, published)
	}

	// THE LEGACY MARKER RIDES ALONG, rather than costing a statement of its own. legacyLeg is
	// settled here by construction — the owed arm returned above — so this row owes no
	// webhook, whether because one was just enqueued, because one was already recorded,
	// because the window has closed, or because no legacy sink is configured at all. Folding
	// the marker into this write is what removes one row update and one commit per published
	// event; see MarkEventDispatched for the measurement and for the crash window it moves.
	if markErr := p.store.MarkEventDispatched(
		bookkeeping, row.ID, row.ClaimToken, record, true,
	); markErr != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), markErr).Error(
			"event relay: the event was published but its outbox row could not be marked dispatched; " +
				"it will be republished when its lease expires and must be suppressed on event_id",
		)

		// NOT COUNTED. The broker has the event but nothing durable says so, and this row is
		// going to be published again when its lease expires — counting here and again after
		// that republish would report one event twice. See recordDurableEventDelivery.
		return false
	}

	// THE DURABLE TRANSITION HAS COMMITTED, which is the moment the delivery is recorded
	// and therefore the only moment it may be counted.
	if published {
		recordDurableEventDelivery(bookkeeping, row.Topic, row.EventType)
	}

	// THE ONE PER-EVENT DELIVERY COUNT, recorded here and only here for this arm.
	recordEventDispatched(bookkeeping, row)

	return true
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
// Returns:
//   - bool: whether the Kafka leg is now recorded. The webhook leg's own fate — owed,
//     retried or abandoned — does not change the answer; only a failure to RECORD the
//     Kafka leg does, because that is the only outcome that republishes the event.
func (p *EventRelayProcessor) deferLegacyLeg(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
	record model.BrokerRecord,
	published bool,
) bool {
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
		return false
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

		return true
	}

	fields["retry_after"] = retryAfter.String()
	logrus.WithFields(fields).Warn(
		"event relay: the event is published to Kafka and its legacy webhook leg is still owed; " +
			"the row stays claimable for the webhook alone and will not be republished",
	)

	return true
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
