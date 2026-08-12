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

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// MarkEventDispatched marks an entry dispatched once the broker has acknowledged the
// publish, and does so ONLY IF the caller still holds the claim.
func (d Datasource) MarkEventDispatched(
	ctx context.Context,
	id int64,
	claimToken string,
	record model.BrokerRecord,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDispatched")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusDispatched); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			dispatched_at = NOW(),
			kafka_dispatched_at = COALESCE(kafka_dispatched_at, NOW()),
			kafka_topic = COALESCE($6, kafka_topic),
			kafka_partition = COALESCE($7, kafka_partition),
			kafka_offset = COALESCE($8, kafka_offset),
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $2 AND claim_token = $3 AND status IN ($4, $5)
	`, model.EventOutboxStatusDispatched, id, claimToken,
		model.EventOutboxStatusProcessing, model.EventOutboxStatusReplaying,
		recordTopic, recordPartition, recordOffset)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dispatched", "mark_event_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDispatched); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// MarkEventFailed records one failed publish attempt against a claimed row and settles
// it: back to pending with a next-attempt instant, or into the terminal failed state
// with a dead-letter hand-off lease when the retry budget is spent.
func (d Datasource) MarkEventFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	terminal bool,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventFailed")
	defer span.End()
	deadLetterLease = normalizeDeadLetterHandoffLease(deadLetterLease, "mark_event_failed")
	if retryAfter < 0 {
		retryAfter = 0
	}
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
		attribute.Bool("event_outbox.terminal", terminal),
		attribute.String("event_outbox.dead_letter_lease", deadLetterLease.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, err
	}

	// $8 is the caller's terminal verdict, ORed into every arm of the decision so the
	// status, the due instant, the retained token AND the retained lease all agree about
	// which arm was taken. Repeating the condition rather than computing it once is what
	// keeps the whole decision inside the single statement.
	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN $1 ELSE $2 END,
			attempts = attempts + 1,
			last_error = $3,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN NOW() + $9::interval ELSE NULL END,
			next_attempt_at = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN next_attempt_at ELSE NOW() + $4::interval END,
			claim_token = CASE WHEN $8::boolean OR attempts + 1 >= max_attempts THEN claim_token ELSE NULL END
		WHERE id = $5 AND claim_token = $6 AND status = $7
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, model.EventOutboxStatusPending, errMsg, retryAfter.String(), id, claimToken,
		model.EventOutboxStatusProcessing, terminal,
		eventDeadLetterHandoffLease.String()).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row matched, so the claim was lost or the row is no longer
			// processing. RETURNING makes this reachable as ErrNoRows rather than as
			// a zero affected count.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			failDatabaseSpan(span, lost)
			return model.EventFailureOutcome{}, lost
		}
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as failed", "mark_event_failed", err)
	}

	outcome.Exhausted = outcome.Status == model.EventOutboxStatusFailed
	// Echoed back from what the caller passed rather than inferred from the attempt count,
	// so a row that exhausted on its budget alone reports false even when its last attempt
	// happened to be permanent. The two causes need to stay distinguishable in the log:
	outcome.Terminal = terminal

	if outcome.Exhausted {
		// Retained by the UPDATE above, and returned so the caller need not
		// remember which arm keeps it.
		outcome.ClaimToken = claimToken
	}

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.attempts", outcome.Attempts),
		attribute.Bool("event_outbox.exhausted", outcome.Exhausted),
		attribute.Bool("event_outbox.terminal", outcome.Terminal),
	)
	return outcome, nil
}

// MarkEventPermanentlyFailed records a publish attempt that failed PERMANENTLY: the row
// becomes failed on this attempt, whatever budget it had left, and the dead-letter
// write is owed immediately.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: the failure reason, stored in last_error.
//   - deadLetterLease time.Duration: how long this worker holds the row for the
//     dead-letter hand-off.
//
// Returns:
//   - model.EventFailureOutcome: Status failed, the new attempt count, Exhausted true —
//     because no further attempt will be made whatever the count says — and the token
//     for the dead-letter hand-off.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventPermanentlyFailed(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	deadLetterLease time.Duration,
) (model.EventFailureOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventPermanentlyFailed")
	defer span.End()
	deadLetterLease = normalizeDeadLetterHandoffLease(deadLetterLease, "mark_event_permanently_failed")
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dead_letter_lease", deadLetterLease.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusFailed); err != nil {
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, err
	}

	var outcome model.EventFailureOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			attempts = attempts + 1,
			last_error = $2,
			first_attempted_at = COALESCE(first_attempted_at, NOW()),
			last_attempted_at = NOW(),
			locked_until = NOW() + $6::interval
		WHERE id = $3 AND claim_token = $4 AND status = $5
		RETURNING status, attempts
	`, model.EventOutboxStatusFailed, errMsg, id, claimToken,
		model.EventOutboxStatusProcessing,
		eventDeadLetterHandoffLease.String()).Scan(&outcome.Status, &outcome.Attempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The claim was lost, or the row is no longer processing. Reachable as
			// ErrNoRows rather than as a zero affected count because of RETURNING.
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusFailed)
			failDatabaseSpan(span, lost)
			return model.EventFailureOutcome{}, lost
		}
		failDatabaseSpan(span, err)
		return model.EventFailureOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to mark event outbox entry as permanently failed", "mark_event_permanently_failed", err)
	}

	// Always true on this path: the caller established that no further attempt is
	// possible, which is what Exhausted means to the caller — "the dead-letter write is
	// now owed" — rather than "the counter reached max_attempts".
	outcome.Exhausted = true
	outcome.ClaimToken = claimToken

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.attempts", outcome.Attempts),
		attribute.Bool("event_outbox.exhausted", outcome.Exhausted),
	)
	return outcome, nil
}

// MarkEventDeadLettered records that an entry has been written to its dead-letter
// topic, moving it to the dead_lettered terminal state and storing the dead-letter
// record — and does so ONLY IF the caller still holds the claim.
func (d Datasource) MarkEventDeadLettered(
	ctx context.Context,
	id int64,
	claimToken, dltTopic string,
	failureMetadata json.RawMessage,
	record model.BrokerRecord,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventDeadLettered")
	defer span.End()
	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.dlt_topic", dltTopic),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusDeadLettered); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	var dltTopicArg interface{}
	if dltTopic != "" {
		dltTopicArg = dltTopic
	}

	var failureMetadataArg interface{}
	if len(failureMetadata) > 0 {
		failureMetadataArg = []byte(failureMetadata)
	}

	// The coordinate here names the record on the DEAD-LETTER topic, not on the original
	// one: the original publish is what failed, so there is no record of it to name. It is
	// assigned rather than COALESCEd, because a dead-letter write supersedes whatever a
	// failed original attempt may have left behind — a coordinate on the main topic would
	// send an operator looking for a record the retries never produced.
	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, dlt_topic = $2, failure_metadata = $3,
			kafka_topic = $8, kafka_partition = $9, kafka_offset = $10,
			locked_until = NULL, claim_token = NULL
		WHERE id = $4 AND claim_token = $5 AND status IN ($6, $7)
	`, model.EventOutboxStatusDeadLettered, dltTopicArg, failureMetadataArg, id, claimToken,
		model.EventOutboxStatusFailed, model.EventOutboxStatusProcessing,
		recordTopic, recordPartition, recordOffset)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark event outbox entry as dead-lettered", "mark_event_dead_lettered", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusDeadLettered); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// ClaimEventForReplay atomically claims a dead-lettered event for replay, moving it
// from dead_lettered to replaying and stamping a fresh claim token, and returns the
// claimed row.
func (d Datasource) ClaimEventForReplay(ctx context.Context, eventID string, lockDuration time.Duration) (*model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventForReplay")
	defer span.End()
	span.SetAttributes(attribute.String("event_outbox.event_id", eventID))

	if lockDuration <= 0 {
		lockDuration = defaultEventClaimLease
	}
	claimToken := uuid.NewString()

	// TWO CLAIMABLE STATES, and $1 appears in both the SET and the WHERE deliberately: it
	// is the replaying literal, so the second arm reads "a replay whose lease has run out".
	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1, claim_token = $2, locked_until = NOW() + $3::interval
		WHERE event_id = $4
		  AND (status = $5
		       OR status = $1 AND (locked_until IS NULL OR locked_until < NOW()))
		RETURNING `+eventOutboxColumns, model.EventOutboxStatusReplaying, claimToken,
		lockDuration.String(), eventID, model.EventOutboxStatusDeadLettered)

	entry, err := scanEventOutbox(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, d.describeUnclaimableReplay(ctx, eventID)
		}
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim event for replay", "claim_event_for_replay", err)
	}

	span.SetAttributes(attribute.String("event_outbox.claim_token", claimToken))
	return &entry, nil
}

// describeUnclaimableReplay explains why ClaimEventForReplay matched nothing, so the
// caller can answer "no such event" and "that event is not dead-lettered" differently
// instead of conflating them into one unhelpful failure.
func (d Datasource) describeUnclaimableReplay(ctx context.Context, eventID string) error {
	existing, lookupErr := d.GetEventByID(ctx, eventID)
	if lookupErr != nil || existing == nil {
		return apierror.NewAPIError(apierror.ErrNotFound, "Event not found", nil)
	}

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event is not available for replay: its status is %q", existing.Status), nil)
}

// ReleaseEventReplay returns a replaying row to dead_lettered, releasing the claim.
func (d Datasource) ReleaseEventReplay(ctx context.Context, id int64, claimToken, replayErr string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ReleaseEventReplay")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusReplaying); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET status = $1,
			claim_token = NULL,
			locked_until = NULL,
			last_error = COALESCE(NULLIF($2, ''), last_error)
		WHERE id = $3 AND claim_token = $4 AND status = $5
	`, model.EventOutboxStatusDeadLettered, replayErr, id, claimToken, model.EventOutboxStatusReplaying)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to release event replay claim", "release_event_replay", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, model.EventOutboxStatusReplaying); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// MarkWebhookDispatched records that the legacy HTTP webhook leg was dispatched for
// this entry, and does so ONLY IF the caller still holds the claim.
func (d Datasource) MarkWebhookDispatched(ctx context.Context, id int64, claimToken string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkWebhookDispatched")
	defer span.End()
	span.SetAttributes(attribute.Int64("event_outbox.id", id))

	if err := requireEventOutboxClaimToken(claimToken, "webhook_dispatched"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_dispatched = TRUE
		WHERE id = $1 AND claim_token = $2
	`, id, claimToken)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark webhook dispatched for event outbox entry", "mark_webhook_dispatched", err)
	}

	if affectedErr := requireEventOutboxRowAffected(result, id, "webhook_dispatched"); affectedErr != nil {
		failDatabaseSpan(span, affectedErr)
		return affectedErr
	}
	return nil
}

// MarkEventWebhookPending records that this row's KAFKA leg is complete while its
// LEGACY WEBHOOK leg is still owed, and decides in the same statement whether another
// webhook attempt is made or the legacy leg is abandoned.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token the claim issued; the update is refused without it.
//   - errMsg string: why the enqueue failed, stored in last_error.
//   - retryAfter time.Duration: how long before the row is due again.
//
// Returns:
//   - model.EventWebhookOutcome: the resulting status, the new webhook attempt count,
//     and whether the legacy leg has been abandoned.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventWebhookPending(
	ctx context.Context,
	id int64,
	claimToken, errMsg string,
	retryAfter time.Duration,
	record model.BrokerRecord,
) (model.EventWebhookOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventWebhookPending")
	defer span.End()

	if retryAfter < 0 {
		retryAfter = 0
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
		attribute.String("event_outbox.broker_record", record.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, model.EventOutboxStatusWebhookPending); err != nil {
		failDatabaseSpan(span, err)
		return model.EventWebhookOutcome{}, err
	}

	// COALESCEd for the reason MarkEventDispatched documents: this row's Kafka leg may
	// already have completed on an earlier claim, and a webhook-only retry carries no
	// coordinate. A straight assignment would erase the record the first successful
	// publish named.
	recordTopic, recordPartition, recordOffset := brokerRecordBindings(record)

	var outcome model.EventWebhookOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_attempts = webhook_attempts + 1,
			kafka_dispatched_at = COALESCE(kafka_dispatched_at, NOW()),
			kafka_topic = COALESCE($9, kafka_topic),
			kafka_partition = COALESCE($10, kafka_partition),
			kafka_offset = COALESCE($11, kafka_offset),
			last_error = $1,
			status = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN $2
				ELSE $3
			END,
			dispatched_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN NOW()
				ELSE dispatched_at
			END,
			next_attempt_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN next_attempt_at
				ELSE NOW() + $4::interval
			END,
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $5 AND claim_token = $6 AND status IN ($7, $8)
		RETURNING status, webhook_attempts
	`,
		errMsg,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusWebhookPending,
		retryAfter.String(),
		id,
		claimToken,
		model.EventOutboxStatusProcessing,
		model.EventOutboxStatusWebhookPending,
		recordTopic,
		recordPartition,
		recordOffset,
	).Scan(&outcome.Status, &outcome.WebhookAttempts)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			lost := eventOutboxClaimLost(id, model.EventOutboxStatusWebhookPending)
			failDatabaseSpan(span, lost)

			return model.EventWebhookOutcome{}, lost
		}

		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the pending legacy webhook leg for event outbox entry", "mark_event_webhook_pending", err)
	}

	// Derived from the status the database chose, never recomputed, so the two cannot
	// disagree about which arm was taken.
	outcome.Abandoned = outcome.Status == model.EventOutboxStatusDispatched

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.webhook_attempts", outcome.WebhookAttempts),
		attribute.Bool("event_outbox.legacy_leg_abandoned", outcome.Abandoned),
	)

	return outcome, nil
}

// MarkEventLegacyWebhookAttempted records one failed legacy webhook enqueue against a
// row whose KAFKA leg has already finished, and decides in the same statement whether
// another webhook attempt is made or the legacy leg is abandoned.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - id int64: the row's surrogate key.
//   - claimToken string: the token ClaimPendingWebhookDeliveries issued; refused
//     without it.
//   - retryAfter time.Duration: how long before the legacy leg is due again.
//
// Returns:
//   - model.EventWebhookOutcome: the row's UNCHANGED status, the new webhook attempt
//     count, and whether the legacy leg has been abandoned.
//   - error: the typed claim-lost error when no row matched, or a wrapped driver error.
func (d Datasource) MarkEventLegacyWebhookAttempted(
	ctx context.Context,
	id int64,
	claimToken string,
	retryAfter time.Duration,
) (model.EventWebhookOutcome, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkEventLegacyWebhookAttempted")
	defer span.End()

	if retryAfter < 0 {
		retryAfter = 0
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.id", id),
		attribute.String("event_outbox.retry_after", retryAfter.String()),
	)

	if err := requireEventOutboxClaimToken(claimToken, "legacy_webhook_attempt"); err != nil {
		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, err
	}

	var outcome model.EventWebhookOutcome
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_outbox
		SET webhook_attempts = webhook_attempts + 1,
			next_attempt_at = CASE
				WHEN webhook_attempts + 1 >= max_attempts THEN next_attempt_at
				ELSE NOW() + $1::interval
			END,
			locked_until = NULL,
			claim_token = NULL
		WHERE id = $2 AND claim_token = $3 AND status IN ($4, $5, $6)
		RETURNING status, webhook_attempts, webhook_attempts >= max_attempts
	`,
		retryAfter.String(),
		id,
		claimToken,
		model.EventOutboxStatusDispatched,
		model.EventOutboxStatusFailed,
		model.EventOutboxStatusDeadLettered,
	).Scan(&outcome.Status, &outcome.WebhookAttempts, &outcome.Abandoned)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			lost := eventOutboxClaimLost(id, "legacy_webhook_attempt")
			failDatabaseSpan(span, lost)

			return model.EventWebhookOutcome{}, lost
		}

		failDatabaseSpan(span, err)

		return model.EventWebhookOutcome{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the legacy webhook attempt for event outbox entry",
			"mark_event_legacy_webhook_attempted", err)
	}

	span.SetAttributes(
		attribute.String("event_outbox.status", outcome.Status),
		attribute.Int("event_outbox.webhook_attempts", outcome.WebhookAttempts),
		attribute.Bool("event_outbox.legacy_leg_abandoned", outcome.Abandoned),
	)

	return outcome, nil
}
