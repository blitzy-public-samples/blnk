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
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/model"
)

// recordFailedAttempt records one failed publish attempt, schedules the retry, and
// hands off to the dead-letter path when nothing further will be tried.
func (p *EventRelayProcessor) recordFailedAttempt(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	result PublishResult,
	cause error,
) {
	if result.PermanentFailure() {
		p.recordPermanentFailure(ctx, row, attempt, cause)

		return
	}

	retryAfter := p.retry.backoffFor(attempt)
	reason := relayFailureReason(cause)
	terminal := publishFailureIsTerminal(result, cause)

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	// THE RELAY'S OWN LOCK DURATION IS THE HAND-OFF LEASE. On the exhaustion arm the row
	// is held under it while this worker writes the event to its `<topic>.dlt` sibling, so
	// the repair pass cannot reclaim the row and write a second copy; on the retry arm it
	// is ignored and the lease is released. Using the same duration the claim uses keeps
	// one value governing the whole ownership window.
	outcome, err := p.store.MarkEventFailed(bookkeeping, row.ID, row.ClaimToken, reason, retryAfter, terminal, p.lockDuration)
	if err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
			"event relay: recording a failed publish attempt failed; the row keeps its lease and " +
				"becomes claimable again when it expires",
		)

		return
	}

	if !outcome.Exhausted {
		logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
			"attempts":       outcome.Attempts,
			"retry_after":    retryAfter.String(),
			"next_attempt":   p.now().UTC().Add(retryAfter).Format(time.RFC3339),
			"error":          relayLogReason(cause),
			"row_status":     outcome.Status,
			"max_attempts":   p.rowMaxAttempts(row),
			"backoff_capped": retryAfter >= p.retry.maxBackoff,
		}).Warn("event relay: publish failed and the event is scheduled for another attempt")

		return
	}

	// WHY the budget is spent, stated on the line rather than left to be inferred from the
	// numbers. "attempt 1 of 5, exhausted" reads as a bookkeeping defect unless the line
	// also says the failure was permanent, which is the one case where it is the correct
	// outcome.
	exhaustionFields := p.rowFields(row, attempt)
	exhaustionFields["terminal"] = outcome.Terminal
	exhaustionFields["retry_after"] = retryAfter.String()
	exhaustionFields["retry_after_waited"] = false
	exhaustionFields["attempts"] = outcome.Attempts
	exhaustionFields["max_attempts"] = p.rowMaxAttempts(row)
	exhaustionFields["error"] = relayLogReason(cause)

	// ONE line on BOTH arms, with the wording selected by the verdict. The permanent arm
	// already had one; the budget-spent arm had none at all, so the attempt that ended a
	// retried event's life was the one attempt the relay said nothing about — the
	// schedule's final delay went unreported and the transition itself was only visible
	// indirectly, through the dead-letter write that followed it.
	if outcome.Terminal {
		logrus.WithFields(exhaustionFields).Warn(
			"event relay: the publish failed PERMANENTLY, so the remaining retry budget is " +
				"abandoned and the event goes straight to its dead-letter topic; no retry could " +
				"change this outcome",
		)
	} else {
		logrus.WithFields(exhaustionFields).Warn(
			"event relay: publish failed on the last attempt this row's budget allowed, so the " +
				"schedule's final delay is recorded on the row but never waited and the event is " +
				"handed to its dead-letter topic instead of being published again",
		)
	}

	// The budget is spent. The row is already failed, and the claim token MarkEventFailed
	// RETAINED on that arm travels on the row so the dead-letter write and the transition
	// that records it stay the exclusive property of the worker that spent the last
	// attempt — which is what stops two workers writing the same event to the dead-letter
	// topic. The attempt count and status are carried across too, so the failure metadata
	// reports the number the database actually recorded.
	row.ClaimToken = outcome.ClaimToken
	row.Attempts = outcome.Attempts
	row.Status = outcome.Status

	p.deadLetter(ctx, row, attempt, cause, relayTerminalBudgetSpent)
}

// recordPermanentFailure records an attempt whose failure can never succeed and hands
// the row to the dead-letter writer on that attempt.
func (p *EventRelayProcessor) recordPermanentFailure(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
) {
	reason := relayFailureReason(cause)

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	// The hand-off lease, for the same reason it is passed on the exhaustion arm above.
	outcome, err := p.store.MarkEventPermanentlyFailed(bookkeeping, row.ID, row.ClaimToken, reason, p.lockDuration)
	if err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, attempt)), err).Error(
			"event relay: recording a permanently failed publish attempt failed; the row keeps its " +
				"lease and becomes claimable again when it expires",
		)

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"attempts":   outcome.Attempts,
		"error":      relayLogReason(cause),
		"row_status": outcome.Status,
	}).Warn(
		"event relay: publish failed permanently, so the remaining retry budget is not spent and " +
			"the event goes straight to its dead-letter topic",
	)

	// Exactly as the exhaustion arm does: the retained claim token, the recorded attempt
	// count and the new status travel on the row, so the dead-letter write and the
	// transition that records it stay the exclusive property of this worker and the
	// failure metadata reports the attempt count the database actually holds.
	row.ClaimToken = outcome.ClaimToken
	row.Attempts = outcome.Attempts
	row.Status = outcome.Status

	p.deadLetter(ctx, row, attempt, cause, relayTerminalPermanentFailure)
}

// relayTerminalReason says WHY a row reached the dead-letter hand-off, and it exists so
// the two log lines that describe the hand-off cannot claim the wrong one.
type relayTerminalReason int

const (
	// relayTerminalBudgetSpent means the row used every attempt its budget allowed.
	relayTerminalBudgetSpent relayTerminalReason = iota

	// relayTerminalPermanentFailure means the publisher reported a failure no further
	// attempt could change, so the budget was deliberately left unspent.
	relayTerminalPermanentFailure
)

// String renders the reason as the bounded log-field value, so the two terminal paths
// are separable with a field match rather than by parsing a message.
//
// Returns:
//   - string: "budget_spent" or "permanent_failure".
func (r relayTerminalReason) String() string {
	switch r {
	case relayTerminalBudgetSpent:
		return "budget_spent"
	case relayTerminalPermanentFailure:
		return "permanent_failure"
	default:
		return "unspecified"
	}
}

// description renders the clause the dead-letter log line ends with, so the sentence an
// operator reads matches what actually happened to the event.
func (r relayTerminalReason) description() string {
	switch r {
	case relayTerminalBudgetSpent:
		return "after exhausting its retry budget"
	case relayTerminalPermanentFailure:
		return "after a permanent publish failure, with its retry budget deliberately unspent"
	default:
		return "for an unspecified terminal reason"
	}
}

// deadLetter hands a terminal row to the dead-letter writer.
func (p *EventRelayProcessor) deadLetter(
	ctx context.Context,
	row model.EventOutbox,
	attempt int,
	cause error,
	reason relayTerminalReason,
) {
	if p.deadLetters == nil {
		// Unreachable: Start refuses without a dead-letter service. Logged rather than
		// dereferenced, because the alternative is a nil panic in a ledger process.
		logrus.WithFields(p.rowFields(row, attempt)).WithField("terminal_reason", reason.String()).Error(
			"event relay: the event is terminal but no dead-letter service is configured; " +
				"the row stays failed and remains in the dead-letter inventory",
		)

		return
	}

	outcome, err := p.deadLetters.DeadLetter(ctx, row, cause)
	if err != nil {
		withLoggableCause(
			logrus.WithFields(p.rowFields(row, attempt)).
				WithField("terminal_reason", reason.String()), err).Error(
			"event relay: the event is terminal but could not be written to its dead-letter topic; " +
				"the row stays failed, remains in the dead-letter inventory, and its hand-off is " +
				"retried by recoverUnpreservedDeadLetters once the lease the terminal transition " +
				"retained has lapsed",
		)

		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"dlt_topic":       outcome.DeadLetterTopic,
		"attempts":        outcome.Metadata.AttemptCount,
		"error":           relayLogReason(cause),
		"terminal_reason": reason.String(),
	}).Warn("event relay: event dead-lettered " + reason.description())
}

// ---------------------------------------------------------------------------
// DUAL-DELIVERY BRANCH — temporary by design

// deliverLegacyWebhook enqueues the legacy HTTP delivery of one claimed row.
func (p *EventRelayProcessor) deliverLegacyWebhook(
	ctx context.Context,
	row model.EventOutbox,
) (legacyLegOutcome, error) {
	if p.legacy == nil {
		return legacyLegSettled, nil
	}

	// Already enqueued on an earlier claim. The flag makes the common case cheap; the task
	// identity carried by the enqueue makes the crash case correct.
	if row.WebhookDispatched {
		return legacyLegSettled, nil
	}

	// The boundary, immediately before the enqueue and no earlier. Outside the window the
	// leg is not owed at all: before the start because the relay refuses to run there (see
	// startupObstacle), and from the sunset onwards because Kafka is then the only
	// transport. A row that reaches the sunset with its webhook still owed is settled
	// rather than stranded — the window has ended, and the promise with it.
	if !p.legacyLegDue(p.now()) {
		return legacyLegSettled, nil
	}

	if err := p.legacy.EnqueueLegacyWebhookDelivery(row.EventID, row.Payload); err != nil {
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts+1)), err).Warn(
			"event relay: enqueuing the legacy webhook delivery failed; the Kafka publish is " +
				"unaffected and the row stays claimable for the webhook leg alone",
		)

		return legacyLegOwed, err
	}

	bookkeeping, cancel := detachedBookkeepingContext(ctx)
	defer cancel()

	if err := p.store.MarkWebhookDispatched(bookkeeping, row.ID, row.ClaimToken); err != nil {
		// The task is enqueued and will be delivered; only the marker is missing. A later
		// claim re-enqueues under the same task identity, which the queue refuses as a
		// duplicate, so this is an observability gap rather than a delivery defect — and the
		// leg is reported as SETTLED, because it is: the webhook is on the queue.
		withLoggableCause(logrus.WithFields(p.rowFields(row, row.Attempts+1)), err).Warn(
			"event relay: the legacy webhook was enqueued but the row could not be marked; " +
				"a re-enqueue is suppressed by the task identity",
		)
	}

	return legacyLegSettled, nil
}

// legacyLegOutcome is what one pass of the legacy webhook leg achieved.
type legacyLegOutcome int

const (
	// legacyLegSettled means no webhook is owed: none was applicable, one was already
	// enqueued, the window has closed, or the enqueue just succeeded.
	legacyLegSettled legacyLegOutcome = iota

	// legacyLegOwed means the enqueue was attempted and FAILED, so this row still owes a
	// webhook delivery and must stay claimable for it.
	legacyLegOwed
)

// ---------------------------------------------------------------------------
// END OF THE DUAL-DELIVERY BRANCH
// ---------------------------------------------------------------------------

// logAttempt logs a SUCCESSFUL publish attempt at debug, and deliberately says nothing
// at all about a failed one.
func (p *EventRelayProcessor) logAttempt(
	row model.EventOutbox,
	attempt int,
	result PublishResult,
	err error,
) {
	if err != nil {
		// Silence, by design. The publisher has already emitted the canonical per-attempt
		// record for this failure; the durable consequences are logged by the transitions
		// that perform them.
		return
	}

	if !logrus.IsLevelEnabled(logrus.DebugLevel) {
		return
	}

	logrus.WithFields(p.rowFields(row, attempt)).WithFields(logrus.Fields{
		"status":      string(result.Status),
		"duration_ms": result.Duration.Milliseconds(),
	}).Debug("event relay: published a ledger event")
}

// rowFields renders the identity of a row and its attempt as logrus fields.
func (p *EventRelayProcessor) rowFields(row model.EventOutbox, attempt int) logrus.Fields {
	return logrus.Fields{
		"event_id":     row.EventID,
		"event_type":   row.EventType,
		"topic":        row.Topic,
		"attempt":      attempt,
		"max_attempts": p.rowMaxAttempts(row),
		"outbox_id":    row.ID,
	}
}

// publishFailureIsTerminal reads the publisher's verdict on whether another attempt
// could succeed, and reports the answer the durable transition needs.
func publishFailureIsTerminal(result PublishResult, cause error) bool {
	// The result's own verdict, which requires an affirmative Classified marker.
	if result.PermanentFailure() {
		return true
	}

	// The error's verdict, for a caller holding only an error. A *PublishError is the
	// only carrier of a classification, so its absence is the "nobody classified this"
	// case and must NOT be read as permanent.
	var publishErr *PublishError
	if errors.As(cause, &publishErr) {
		return !publishErr.Transient
	}

	return false
}

// relayFailureReason renders a publish failure as text safe to STORE in the row's
// last_error column and in the dead-letter failure metadata.
func relayFailureReason(cause error) string {
	if cause == nil {
		return "the publish failed without reporting a reason"
	}

	reason := sanitizeLogValue(cause.Error(), maxLoggedErrorLength)
	if reason == "" {
		return "the publish failed without reporting a reason"
	}

	return reason
}

// relayLogReason renders a publish failure as text safe to write to a LOG at a normal
// level: everything relayFailureReason does, plus network topology redacted.
func relayLogReason(cause error) string {
	if cause == nil {
		return "the publish failed without reporting a reason"
	}

	reason := loggableCause(cause)
	if reason == "" {
		return "the publish failed without reporting a reason"
	}

	return reason
}

// detachedBookkeepingContext derives a short-lived context for a transition the relay
// ALREADY OWES, so a shutdown cannot leave the database disagreeing with what has
// already happened on the wire.
func detachedBookkeepingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), eventRelayBookkeepingTimeout)
}
