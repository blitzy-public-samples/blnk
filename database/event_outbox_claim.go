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
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// claimPendingEventOutboxQuery claims a batch of publishable rows, takes a lease on
// them and stamps a fresh claim token, all in one statement. It is a package-level
// variable rather than a local so its text is reachable from tests, which assert that
// FOR UPDATE SKIP LOCKED, the occurred_at ordering and the earlier-same-key exclusion
// are all still present — the three properties a well-meaning refactor is most likely
// to drop. A variable and not a constant only because the effective-key expression is
// composed from eventOutboxEffectiveKeySQL rather than written twice; nothing assigns
// to it after initialisation.
var claimPendingEventOutboxQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('pending', 'processing', 'webhook_pending')
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.attempts < candidate.max_attempts
			  AND candidate.next_attempt_at <= NOW()
			  AND NOT EXISTS (
				SELECT 1 FROM blnk.event_outbox earlier
				WHERE ` + eventOutboxEffectiveKeySQL("earlier") + ` = ` + eventOutboxEffectiveKeySQL("candidate") + `
				  AND earlier.status IN ('pending', 'processing')
				  AND (earlier.occurred_at, earlier.id) < (candidate.occurred_at, candidate.id)
			  )
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET status = $1,
				locked_until = NOW() + $2::interval,
				claim_token = $3,
				first_attempted_at = COALESCE(first_attempted_at, NOW()),
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingEventOutbox claims a batch of pending entries for publishing, takes a
// lease on them for lockDuration and stamps every claimed row with one fresh claim
// token. The returned entries are ordered oldest occurrence first, which is the order
// they must be published in, and each carries the token in its ClaimToken field — every
// transition the caller subsequently performs must present it.
func (d Datasource) ClaimPendingEventOutbox(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingEventOutbox")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive event outbox lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingEventOutboxQuery,
		model.EventOutboxStatusProcessing, lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending event outbox entries", "claim_pending_event_outbox", err)
	}
	defer func() {
		// A close failure is logged rather than returned: the rows have already
		// been read, and replacing a successful claim with an error here would
		// leave those rows leased but unpublished until the lease expired.
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_event_outbox", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_event_outbox", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimFailedEventOutboxForDeadLetterQuery claims rows whose retry budget is spent and
// whose dead-letter PRESERVATION has not happened, so it can be attempted again.
const claimFailedEventOutboxForDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status = 'failed'
			  AND candidate.dlt_topic IS NULL
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimFailedEventOutboxForDeadLetter claims up to batchSize rows whose retry budget is
// spent and whose dead-letter write has not yet succeeded, taking a lease on them and
// stamping one fresh claim token, so the preservation can be attempted again.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: the maximum number of rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest first. Empty when nothing needs
//     repair, which is the normal state.
//   - error: a bad-request error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimFailedEventOutboxForDeadLetter(
	ctx context.Context,
	batchSize int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimFailedEventOutboxForDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest,
			"Event outbox dead-letter recovery batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)

		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter recovery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
		attribute.String("event_outbox.claim_token", claimToken),
	)

	rows, err := d.Conn.QueryContext(ctx, claimFailedEventOutboxForDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to claim event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox entry awaiting dead-letter preservation",
				"claim_failed_event_outbox_for_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox entries awaiting dead-letter preservation",
			"claim_failed_event_outbox_for_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))

	return entries, nil
}

// brokerRecordBindings renders a broker coordinate as the three nullable SQL parameters
// the marking statements bind, so all three transitions agree on one definition.
func brokerRecordBindings(record model.BrokerRecord) (topic, partition, offset any) {
	if !record.Confirmed() {
		return nil, nil, nil
	}

	return strings.TrimSpace(record.Topic), record.Partition, record.Offset
}

// requireEventOutboxClaimToken rejects a transition attempted without a claim token.
func requireEventOutboxClaimToken(claimToken, transition string) error {
	if strings.TrimSpace(claimToken) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest,
			fmt.Sprintf("Event outbox transition %q requires the claim token held for the row", transition), nil)
	}
	return nil
}

// normalizeDeadLetterHandoffLease resolves the lease a terminal transition holds the
// row under while its dead-letter write is owed.
func normalizeDeadLetterHandoffLease(lease time.Duration, transition string) time.Duration {
	if lease > 0 {
		return lease
	}

	logrus.WithFields(logrus.Fields{
		"transition":      transition,
		"requested_lease": lease.String(),
		"applied_lease":   defaultEventClaimLease.String(),
	}).Warn(
		"Non-positive dead-letter hand-off lease; falling back to the default. A lease that has " +
			"already expired lets the dead-letter repair pass reclaim the row while this worker's " +
			"dead-letter write is still in flight, which is how one event reaches a .dlt topic twice",
	)

	return defaultEventClaimLease
}

// eventOutboxClaimLost is the typed error every conditional transition returns when its
// UPDATE matched no row, and it is a DELIBERATE behaviour change from the previous
// warn-and-continue treatment.
func eventOutboxClaimLost(id int64, transition string) error {
	logrus.WithFields(logrus.Fields{
		"event_outbox_id": id,
		"transition":      transition,
	}).Warn("Event outbox transition matched no row: the claim was lost or the row already moved on")

	return apierror.NewAPIError(apierror.ErrConflict,
		fmt.Sprintf("Event outbox row is no longer claimed for transition %q", transition), nil)
}

// requireEventOutboxRowAffected converts an UPDATE that matched no row into the typed
// lost-claim conflict above.
func requireEventOutboxRowAffected(result sql.Result, id int64, transition string) error {
	if result == nil {
		return nil
	}

	affected, err := result.RowsAffected()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"event_outbox_id": id,
			"transition":      transition,
			"error_class":     databaseErrorClass(err),
			"sqlstate":        postgresSQLState(err),
		}).Debug("Could not determine rows affected for event outbox transition")
		logDatabaseDiagnostic("require_event_outbox_row_affected", err)
		return nil
	}
	if affected == 0 {
		return eventOutboxClaimLost(id, transition)
	}

	return nil
}

// RenewEventOutboxLease extends the lease on every row still being worked under one
// claim token, and reports how many it extended.
//
// Parameters:
//   - ctx context.Context: cancels the update.
//   - claimToken string: the token the claim issued. Required; without it the statement
//     could only match rows nobody holds.
//   - lease time.Duration: how long from NOW the extended lease should run.
//
// Returns:
//   - int64: how many rows were extended. Zero is a legitimate answer.
//   - error: a bad-request error for a missing token, or a wrapped driver error.
func (d Datasource) RenewEventOutboxLease(ctx context.Context, claimToken string, lease time.Duration) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RenewEventOutboxLease")
	defer span.End()

	if err := requireEventOutboxClaimToken(claimToken, "renew_lease"); err != nil {
		failDatabaseSpan(span, err)
		return 0, err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive event outbox lease renewal; falling back to %s", defaultEventClaimLease)
		lease = defaultEventClaimLease
	}

	span.SetAttributes(
		attribute.String("event_outbox.claim_token", claimToken),
		attribute.String("event_outbox.lease", lease.String()),
	)

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_outbox
		SET locked_until = NOW() + $1::interval
		WHERE claim_token = $2 AND status = $3
	`, lease.String(), claimToken, model.EventOutboxStatusProcessing)
	if err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the event outbox lease", "renew_event_outbox_lease", err)
	}

	renewed, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The renewal itself succeeded, so this is
		// reported as zero rather than as a failure: the caller uses the count only to
		// decide whether to keep renewing, and one uncounted round is harmless.
		logrus.WithFields(logrus.Fields{
			"claim_token": claimToken,
			"error_class": databaseErrorClass(err),
			"sqlstate":    postgresSQLState(err),
		}).Debug("Could not determine how many event outbox leases were renewed")
		logDatabaseDiagnostic("renew_event_outbox_lease", err)

		return 0, nil
	}

	span.SetAttributes(attribute.Int64("event_outbox.renewed_count", renewed))

	return renewed, nil
}

// claimPendingWebhookDeliveriesQuery claims rows whose Kafka leg has FINISHED and whose
// LEGACY WEBHOOK leg is still owed, taking a lease and stamping a fresh claim token
// WITHOUT changing the row's status.
const claimPendingWebhookDeliveriesQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('dispatched', 'failed', 'dead_lettered')
			  AND candidate.webhook_dispatched = FALSE
			  AND candidate.webhook_attempts < candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimPendingWebhookDeliveries claims rows whose Kafka leg has finished and whose
// legacy HTTP webhook leg has not been recorded, so the dual-delivery window can finish
// a leg that failed alongside the Kafka publish — whether that publish then succeeded
// or was itself given up on.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first, each carrying the
//     claim token MarkWebhookDispatched requires.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimPendingWebhookDeliveries(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimPendingWebhookDeliveries")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Webhook delivery claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive webhook delivery lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimPendingWebhookDeliveriesQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim pending webhook deliveries", "claim_pending_webhook_deliveries", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_pending_webhook_deliveries", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_pending_webhook_deliveries", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}

// claimEventsOwedDeadLetterQuery claims rows whose retry budget is spent and whose
// dead-letter write has NOT been recorded, taking a lease and stamping a fresh claim
// token WITHOUT changing their status.
const claimEventsOwedDeadLetterQuery = `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.id FROM blnk.event_outbox candidate
			WHERE candidate.status IN ('failed', 'processing')
			  AND candidate.dlt_topic IS NULL
			  AND candidate.attempts >= candidate.max_attempts
			  AND (candidate.locked_until IS NULL OR candidate.locked_until < NOW())
			  AND candidate.next_attempt_at <= NOW()
			ORDER BY candidate.next_attempt_at ASC, candidate.occurred_at ASC, candidate.id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE blnk.event_outbox
			SET locked_until = NOW() + $1::interval,
				claim_token = $2,
				last_attempted_at = NOW()
			WHERE id IN (SELECT id FROM candidates)
			RETURNING ` + eventOutboxColumns + `
		)
		SELECT * FROM claimed ORDER BY occurred_at ASC, id ASC
	`

// ClaimEventsOwedDeadLetter claims events whose retry budget is spent and whose
// dead-letter write is still owed, so the hand-off can be retried instead of the event
// being stranded in the only table that holds it.
//
// Parameters:
//   - ctx context.Context: cancels the claim.
//   - batchSize int: how many rows to claim.
//   - lockDuration time.Duration: the lease.
//
// Returns:
//   - []model.EventOutbox: the claimed rows, oldest occurrence first.
//   - error: a validation error for a non-positive batch, or a wrapped driver error.
func (d Datasource) ClaimEventsOwedDeadLetter(ctx context.Context, batchSize int, lockDuration time.Duration) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimEventsOwedDeadLetter")
	defer span.End()

	if batchSize <= 0 {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Dead-letter hand-off claim batch size must be greater than zero", nil)
		failDatabaseSpan(span, err)
		return nil, err
	}

	if lockDuration <= 0 {
		logrus.WithField("requested_lock_duration", lockDuration.String()).
			Warnf("Non-positive dead-letter hand-off lock duration; falling back to %s", defaultEventClaimLease)
		lockDuration = defaultEventClaimLease
	}

	claimToken := uuid.NewString()

	span.SetAttributes(
		attribute.Int("event_outbox.batch_size", batchSize),
		attribute.String("event_outbox.lock_duration", lockDuration.String()),
	)

	rows, err := d.Conn.QueryContext(ctx, claimEventsOwedDeadLetterQuery,
		lockDuration.String(), claimToken, batchSize)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to claim events owed a dead-letter write", "claim_events_owed_dead_letter", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event outbox entry", "claim_events_owed_dead_letter", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event outbox entries", "claim_events_owed_dead_letter", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.claimed_count", len(entries)))
	return entries, nil
}
