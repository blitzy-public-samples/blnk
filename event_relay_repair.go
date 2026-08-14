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
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// The two repair legs, named so the metrics attribute and the log field cannot disagree about
// which backlog a reading describes.
const (
	repairLegDeadLetter    = "dead_letter"
	repairLegLegacyWebhook = "legacy_webhook"
)

// repairChained runs one repair pass repeatedly until it drains, up to the per-tick
// bound, and publishes what the pass achieved.
func (p *EventRelayProcessor) repairChained(ctx context.Context, leg string, pass func(context.Context) int) {
	budget := p.repairBatchesPerTick
	if budget < 1 {
		budget = 1
	}

	batches := 0
	repaired := 0
	saturated := false

	for chained := 0; chained < budget; chained++ {
		// The same non-blocking check the publish loop makes before every batch: claiming new
		// work while stopping would only lease rows and leave them.
		if !p.shouldClaimAnotherBatch(ctx) {
			break
		}

		done := pass(ctx)
		batches++
		repaired += done

		if done < p.repairBatchSize {
			// Drained, or every row of the batch failed — either way there is nothing this tick
			// can usefully chain onto. A pass that failed every row has already logged per row.
			break
		}

		// A full batch on the LAST permitted iteration means the budget, not the backlog, ended
		// the chain.
		if chained == budget-1 {
			saturated = true
		}
	}

	p.recordRepairOutcome(ctx, leg, repaired, batches, saturated)
}

// recordRepairOutcome publishes what one tick's chained repair achieved for one leg.
func (p *EventRelayProcessor) recordRepairOutcome(ctx context.Context, leg string, repaired, batches int, saturated bool) {
	attributes := otelmetric.WithAttributes(attribute.String("leg", leg))

	if metrics.EventRepairsCompletedTotal != nil && repaired > 0 {
		metrics.EventRepairsCompletedTotal.Add(ctx, int64(repaired), attributes)
	}

	if metrics.EventRepairSaturated != nil {
		saturatedValue := int64(0)
		if saturated {
			saturatedValue = 1
		}

		metrics.EventRepairSaturated.Record(ctx, saturatedValue, attributes)
	}

	// Logged only when something happened, because this runs every poll interval and an
	// idle pass has nothing to say. Saturation IS something happening even at zero
	// repaired: it means the budget was spent on rows that all failed.
	if repaired == 0 && !saturated {
		return
	}

	fields := logrus.Fields{
		"leg":       leg,
		"repaired":  repaired,
		"batches":   batches,
		"saturated": saturated,
	}

	if saturated {
		logrus.WithFields(fields).Warn(
			"event relay: the repair budget for this leg was spent with work still outstanding; " +
				"raise RELAY_REPAIR_MAX_BATCHES_PER_TICK or RELAY_REPAIR_BATCH_SIZE if the backlog " +
				"is not falling fast enough",
		)

		return
	}

	logrus.WithFields(fields).Info("event relay: repaired outstanding rows for this leg")
}

// repairRowsConcurrently writes one claimed repair batch, bounded by repairConcurrency.
func (p *EventRelayProcessor) repairRowsConcurrently(
	ctx context.Context,
	rows []model.EventOutbox,
	repair func(context.Context, model.EventOutbox) bool,
) int {
	width := p.repairConcurrency
	if width < 1 {
		width = 1
	}

	groups := groupEventRowsByPartitionKey(rows)
	permits := semaphore.NewWeighted(int64(width))

	var (
		wait     sync.WaitGroup
		mu       sync.Mutex
		repaired int
	)

	for _, group := range groups {
		if !publishingMayProceed(ctx) {
			break
		}

		// Acquired before spawning, so the number of in-flight writes is bounded by the permit
		// count rather than by the number of groups.
		if acquireErr := permits.Acquire(ctx, 1); acquireErr != nil {
			break
		}

		wait.Add(1)

		go func(group []model.EventOutbox) {
			defer wait.Done()
			defer permits.Release(1)

			done := 0
			for _, row := range group {
				if !publishingMayProceed(ctx) {
					break
				}

				if repair(ctx, row) {
					done++
				}
			}

			if done == 0 {
				return
			}

			mu.Lock()
			repaired += done
			mu.Unlock()
		}(group)
	}

	// Waiting here is what makes Stop's promise true: the tick cannot return, and so p.wg cannot
	// drain, until the writes in flight have finished.
	wait.Wait()

	return repaired
}

// shouldClaimAnotherBatch reports whether the relay may CLAIM MORE WORK.
func (p *EventRelayProcessor) shouldClaimAnotherBatch(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-p.stopCh:
		return false
	default:
		return true
	}
}

// recoverUnpreservedDeadLetters retries the dead-letter write for rows whose retry
// budget is spent and whose preservation did not succeed, and reports how many it
// repaired.
func (p *EventRelayProcessor) recoverUnpreservedDeadLetters(ctx context.Context) int {
	if p.deadLetters == nil {
		return 0
	}

	rows, err := p.store.ClaimFailedEventOutboxForDeadLetter(ctx, p.repairBatchSize, p.lockDuration)
	if err != nil {
		withLoggableCause(nil, err).Error(
			"event relay: could not claim events awaiting dead-letter preservation; they stay in the " +
				"dead-letter inventory and are retried on a later poll",
		)

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	logrus.WithField("claimed", len(rows)).Info(
		"event relay: retrying the dead-letter preservation of events whose earlier write failed",
	)

	// Written repairConcurrency groups at a time rather than one row after another. Every
	// row here needs its own Kafka write, so a sequential loop bounded the whole recovery
	// at one broker acknowledgement at a time — about 100 rows a second — which is an
	// order of magnitude under what an outage's backlog needs. Ordering is preserved by
	// grouping on the same effective key the publisher hashes; see repairRowsConcurrently.
	return p.repairRowsConcurrently(ctx, rows, p.preserveOneDeadLetter)
}

// preserveOneDeadLetter retries the `<topic>.dlt` write for one claimed row.
func (p *EventRelayProcessor) preserveOneDeadLetter(ctx context.Context, row model.EventOutbox) bool {
	outcome, dltErr := p.deadLetters.DeadLetter(ctx, row, unpreservedDeadLetterCause(row))
	if dltErr != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts)), dltErr).Warn(
			"event relay: retrying the dead-letter preservation of this event failed again; it " +
				"stays in the dead-letter inventory and is retried once its lease expires",
		)

		return false
	}

	logrus.WithFields(p.rowFields(row, row.Attempts)).WithFields(logrus.Fields{
		"dlt_topic": outcome.DeadLetterTopic,
		"attempts":  outcome.Metadata.AttemptCount,
	}).Warn(
		"event relay: an event whose dead-letter write had failed is now preserved on its " +
			"dead-letter topic and is replayable",
	)

	return true
}

// recoverOwedLegacyWebhooks finishes the LEGACY leg of rows whose Kafka leg has
// finished and whose webhook enqueue never succeeded.
func (p *EventRelayProcessor) recoverOwedLegacyWebhooks(ctx context.Context) int {
	if p.legacy == nil || !p.legacyLegDue(p.now()) {
		return 0
	}

	rows, err := p.store.ClaimPendingWebhookDeliveries(ctx, p.repairBatchSize, p.lockDuration)
	if err != nil {
		withLoggableCause(nil, err).Error(
			"event relay: could not claim events whose legacy webhook leg is still owed; the legs " +
				"stay recorded on their rows and are retried on a later poll",
		)

		return 0
	}

	if len(rows) == 0 {
		return 0
	}

	logrus.WithField("claimed", len(rows)).Info(
		"event relay: finishing the legacy webhook leg of events whose earlier enqueue failed",
	)

	// Bounded-concurrent for the same reason the dead-letter leg is, and grouped on the
	// effective key so two enqueues of one aggregate keep their order — which matters more
	// here than on the dead-letter leg, because a subscriber still receiving webhooks sees
	// this order directly.
	return p.repairRowsConcurrently(ctx, rows, func(ctx context.Context, row model.EventOutbox) bool {
		return p.recoverOneOwedLegacyWebhook(ctx, row)
	})
}

// recoverOneOwedLegacyWebhook enqueues one outstanding legacy delivery and records the
// outcome.
func (p *EventRelayProcessor) recoverOneOwedLegacyWebhook(ctx context.Context, row model.EventOutbox) bool {
	// The attempt label is the WEBHOOK's count, not the Kafka one: this row's Kafka leg is
	// finished and its attempt number would mislead an operator reading the line.
	fields := p.rowFields(row, row.Attempts)
	fields["webhook_attempts"] = row.WebhookAttempts
	fields["webhook_max_attempts"] = p.rowMaxAttempts(row)
	fields["row_status"] = row.Status

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if err := p.legacy.EnqueueLegacyWebhookDelivery(row.EventID, row.Payload); err != nil {
		// The backoff is indexed by the WEBHOOK attempt count so the two legs walk the same
		// curve while spending separate budgets, exactly as deferLegacyLeg does.
		retryAfter := p.retry.backoffFor(row.WebhookAttempts + 1)

		outcome, markErr := p.store.MarkEventLegacyWebhookAttempted(
			bookkeeping, row.ID, row.ClaimToken, retryAfter,
		)
		if markErr != nil {
			// The row keeps its lease and returns to this pass's candidate set when it
			// expires. Nothing is lost, and nothing about the Kafka leg is affected.
			withLoggableCause(logrus.WithFields(fields), markErr).Error(
				"event relay: a failed legacy webhook recovery could not be recorded; the row keeps " +
					"its lease and the leg is retried when it expires",
			)

			return false
		}

		fields["webhook_attempts"] = outcome.WebhookAttempts
		fields["reason"] = relayLogReason(err)

		if outcome.Abandoned {
			logrus.WithFields(fields).Error(
				"event relay: the legacy webhook leg for this event is ABANDONED after exhausting its " +
					"own enqueue budget. The webhook will never be delivered; any subscriber still " +
					"consuming webhooks has missed this event",
			)

			return false
		}

		fields["retry_after"] = retryAfter.String()
		logrus.WithFields(fields).Warn(
			"event relay: recovering the legacy webhook leg of this event failed again; it is " +
				"retried once its backoff elapses",
		)

		return false
	}

	if err := p.store.MarkWebhookDispatched(bookkeeping, row.ID, row.ClaimToken); err != nil {
		// The task IS enqueued and will be delivered; only the marker is missing. A later
		// pass re-enqueues under the same task identity, which the queue refuses as a
		// duplicate, so this is an observability gap rather than a delivery defect.
		withLoggableCause(logrus.WithFields(fields), err).Warn(
			"event relay: a recovered legacy webhook was enqueued but the row could not be marked; " +
				"a re-enqueue is suppressed by the task identity",
		)
	}

	logrus.WithFields(fields).Info(
		"event relay: the legacy webhook leg of an event whose earlier enqueue had failed is now " +
			"enqueued; the event's Kafka state is untouched",
	)

	return true
}

// unpreservedDeadLetterCause reconstructs the failure to report for a row being
// repaired.
func unpreservedDeadLetterCause(row model.EventOutbox) error {
	if reason := strings.TrimSpace(row.LastError); reason != "" {
		return errors.New(reason)
	}

	return errors.New("the retry budget was exhausted and the dead-letter write is being retried")
}
