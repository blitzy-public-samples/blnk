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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RecordSubscriberCredential persists the outcome of a credential issuance: a
// non-reversible reference to the credential and the instant it was issued, which
// together are the complete stored record of an issuance.
func (d Datasource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredential")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return err
	}
	if strings.TrimSpace(credentialReference) == "" {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Credential reference is required", nil)
		failDatabaseSpan(span, err)
		return err
	}
	if err := model.ValidateCredentialReference(credentialReference); err != nil {
		// The offending value is NOT quoted, in the message or in the details: if a caller has
		// passed a secret by mistake, echoing it into a log is the very disclosure this check
		// exists to prevent.
		err = apierror.NewAPIError(apierror.ErrInvalidInput,
			"The credential reference is not a reference derived by the issuance service", nil)
		failDatabaseSpan(span, err)

		return err
	}
	if issuedAt.IsZero() {
		// A zero timestamp would be stored as year 1 and read back as a real
		// issuance instant, so the current time is used instead. The two credential
		// columns are only ever meaningful together.
		issuedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_reference = $1,
			credential_issued_at = $2,
			updated_at = $3
		WHERE subscriber_id = $4
	`, credentialReference, issuedAt, time.Now(), subscriberID)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to record subscriber credential", "record_subscriber_credential", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to record subscriber credential"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	// The issuance instant is recorded because it is an audit fact rather than a
	// secret. The reference itself is not recorded, not even truncated.
	span.AddEvent("Subscriber credential recorded", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.credential_issued_at", issuedAt.UTC().Format(time.RFC3339)),
	))
	return nil
}

// RecordSubscriberWebhookURL records a subscriber's legacy HTTP endpoint AND clears
// migrated_at, in one statement.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record, stored verbatim.
//
// Returns:
//   - error: a typed validation error for a blank or unacceptable URL,
//     ErrSubscriberNotFound when no such subscriber exists, or a wrapped write error.
func (d Datasource) RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberWebhookURL")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	if strings.TrimSpace(webhookURL) == "" {
		err := apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A webhook URL is required",
			errors.New(
				"database: the webhook url is blank; clear the subscription instead of recording "+
					"an empty one",
			),
		)
		failDatabaseSpan(span, err)

		return err
	}

	// Validated and stored VERBATIM, never trimmed here. requireSafeWebhookURL refuses
	// surrounding whitespace outright for the reason documented on it: this column is
	// stored as given, so validating a trimmed copy and persisting the original would let
	// bytes reach the row that the check never saw.
	if err := requireSafeWebhookURL(&webhookURL); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	// ONE STATEMENT. Two would leave a window in which the row holds both values, and a
	// crash inside that window would make the window permanent.
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET webhook_url  = $1,
			migrated_at  = NULL,
			updated_at   = $2
		WHERE subscriber_id = $3
	`, webhookURL, time.Now(), subscriberID)
	if err != nil {
		failDatabaseSpan(span, err)
		return classifySubscriberWriteError(err, "Failed to record subscriber webhook subscription")
	}

	if err := assertSubscriberRowAffected(result, "Failed to record subscriber webhook subscription"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	// The URL itself is not recorded on the span: it is a third-party address, and a
	// trace is a wider audience than the registry.
	span.AddEvent("Subscriber webhook subscription recorded")

	return nil
}

// ClearSubscriberWebhookURL forgets one subscriber's recorded legacy endpoint WITHOUT
// stamping a migration.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - error: ErrSubscriberNotFound when no such subscriber exists, or a wrapped write
//     error.
func (d Datasource) ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClearSubscriberWebhookURL")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET webhook_url = NULL,
			updated_at  = $1
		WHERE subscriber_id = $2
	`, time.Now(), subscriberID)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(
			apierror.ErrInternalServer,
			"Failed to clear subscriber webhook subscription",
			"clear_subscriber_webhook_url",
			err,
		)
	}

	if err := assertSubscriberRowAffected(result, "Failed to clear subscriber webhook subscription"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	span.AddEvent("Subscriber webhook subscription cleared")

	return nil
}

// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber has
// completed its move from legacy HTTP webhook delivery to Kafka consumption.
func (d Datasource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberMigrated")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return err
	}
	if migratedAt.IsZero() {
		// A zero timestamp is still NOT NULL, so it would read as "migrated in year
		// 1" and quietly corrupt progress reporting rather than leaving the row
		// counted as unmigrated.
		migratedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	// One row is returned when the subscriber exists, and none when it does not — which is
	// what makes "not found" a fact about the locked read rather than a second guess. The
	// single column reports whether the conditional stamp inside the same statement landed.
	var stamped bool

	err := d.Conn.QueryRowContext(ctx, `
		WITH target AS (
			SELECT subscriber_id,
			       (webhook_url IS NULL) AS stampable
			FROM blnk.event_subscribers
			WHERE subscriber_id = $3
			FOR UPDATE
		),
		stamped AS (
			UPDATE blnk.event_subscribers AS s
			SET migrated_at = $1,
				updated_at = $2
			FROM target AS t
			WHERE s.subscriber_id = t.subscriber_id
			  AND t.stampable
			RETURNING s.subscriber_id
		)
		SELECT EXISTS (SELECT 1 FROM stamped) AS stamped
		FROM target AS t
	`, migratedAt, time.Now(), subscriberID).Scan(&stamped)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// The locked read matched nothing, so there is no subscriber to stamp.
		refusal := apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
		failDatabaseSpan(span, refusal)

		return refusal

	case err != nil:
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark subscriber migrated", "mark_subscriber_migrated", err)
	}

	if !stamped {
		// The row exists and the predicate declined it, which — subscriber_id being the only
		// other term — means webhook_url was non-NULL on the row this statement locked.
		refusal := apierror.NewAPIError(
			apierror.ErrGenConflict,
			"This subscriber still has a legacy webhook URL recorded, so it cannot be marked "+
				"migrated on its own. Complete the migration instead, which forgets the URL and "+
				"records the instant together.",
			errors.New(
				"database: migrated_at was not stamped because webhook_url is still set; a row "+
					"holding both asserts that the subscriber has and has not stopped receiving "+
					"legacy pushes. Use CompleteSubscriberWebhookMigration, which moves both "+
					"columns in one statement",
			),
		)
		failDatabaseSpan(span, refusal)

		return refusal
	}

	span.AddEvent("Subscriber marked migrated", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.migrated_at", migratedAt.UTC().Format(time.RFC3339)),
	))
	return nil
}

// A refused migration stamp is explained by the caller from the fence state it reads,
// not by a second read here: a helper that re-read the row could describe a state the
// row had already left.

// CompleteSubscriberWebhookMigration forgets a subscriber's legacy endpoint and records
// that it has finished moving to Kafka — in ONE statement.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - migratedAt time.Time: the instant to record.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, so a caller can report the
//     instant and confirm the URL is gone without re-reading.
//   - error: a typed not-found error when no subscriber matches, or a logged internal
//     error.
func (d Datasource) CompleteSubscriberWebhookMigration(
	ctx context.Context,
	subscriberID string,
	migratedAt time.Time,
) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CompleteSubscriberWebhookMigration")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return nil, err
	}

	if migratedAt.IsZero() {
		migratedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	// COALESCE, so the FIRST transition is the one that is kept.
	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_subscribers
		SET webhook_url = NULL,
			migrated_at = COALESCE(migrated_at, $1),
			updated_at = $2
		WHERE subscriber_id = $3
		RETURNING `+eventSubscriberColumns,
		migratedAt, time.Now(), strings.TrimSpace(subscriberID),
	)

	migrated, err := scanEventSubscriber(row)
	if err != nil {
		failDatabaseSpan(span, err)

		if errors.Is(err, sql.ErrNoRows) {
			return nil, apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
		}

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to complete the subscriber's webhook migration",
			"complete_subscriber_webhook_migration", err)
	}

	span.AddEvent("Subscriber webhook migration completed", trace.WithAttributes(
		attribute.String("subscriber.id", migrated.SubscriberID),
		attribute.String("subscriber.migrated_at", migratedAt.UTC().Format(time.RFC3339)),
		attribute.Bool("subscriber.legacy_webhook_retained", migrated.WebhookURL != nil),
	))

	return &migrated, nil
}

// ---------------------------------------------------------------------------------------
// DURABLE SETTLEMENT OBLIGATIONS
// ---------------------------------------------------------------------------------------

// RecordSubscriberGrantReconcilePending marks a subscriber as owing a broker-side grant
// reconciliation.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the row to mark. Required.
//   - pendingAt time.Time: the instant to record. A zero value becomes time.Now().
//   - fenceToken string: the caller's provisioning claim. Required.
//
// Returns:
//   - error: ErrSubscriberNotFound, a claim-lost conflict, a validation error, or a
//     wrapped write failure.
func (d Datasource) RecordSubscriberGrantReconcilePending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberGrantReconcilePending")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if pendingAt.IsZero() {
		pendingAt = time.Now()
	}

	// COALESCE keeps the FIRST instant. Overwriting it on every retry would make an obligation
	// that has been outstanding for a day look freshly raised, which is precisely the signal an
	// age-based alert reads.
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET grant_reconcile_pending_at = COALESCE(grant_reconcile_pending_at, $1),
			updated_at = $2
		WHERE subscriber_id = $3
		  AND provisioning_token = $4
	`, pendingAt, time.Now(), strings.TrimSpace(subscriberID), fenceToken)
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the subscriber's pending grant reconciliation",
			"record_subscriber_grant_reconcile_pending", err)
	}

	return d.requireFencedWriteLanded(ctx, result, subscriberID, fenceToken,
		"record_subscriber_grant_reconcile_pending")
}

// ClearSubscriberGrantReconcilePending discharges the grant-reconciliation obligation.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the row to clear. Required.
//   - fenceToken string: the caller's provisioning claim. Required.
//
// Returns:
//   - error: ErrSubscriberNotFound, a claim-lost conflict, a validation error, or a
//     wrapped write failure.
func (d Datasource) ClearSubscriberGrantReconcilePending(
	ctx context.Context,
	subscriberID string,
	fenceToken string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClearSubscriberGrantReconcilePending")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	// The counters are reset only when NOTHING remains owed. A row that still owes the
	// credential cleanup keeps its history, because that history belongs to the obligation still
	// outstanding rather than to the one just discharged.
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET grant_reconcile_pending_at = NULL,
			settlement_attempts = CASE
				WHEN credential_cleanup_pending_at IS NULL THEN 0
				ELSE settlement_attempts
			END,
			settlement_last_error = CASE
				WHEN credential_cleanup_pending_at IS NULL THEN NULL
				ELSE settlement_last_error
			END,
			updated_at = $1
		WHERE subscriber_id = $2
		  AND provisioning_token = $3
	`, time.Now(), strings.TrimSpace(subscriberID), fenceToken)
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to clear the subscriber's pending grant reconciliation",
			"clear_subscriber_grant_reconcile_pending", err)
	}

	return d.requireFencedWriteLanded(ctx, result, subscriberID, fenceToken,
		"clear_subscriber_grant_reconcile_pending")
}

// RecordSubscriberCredentialCleanupPending marks a subscriber as owing a credential
// cleanup: a SCRAM credential may exist that Blnk intended to destroy, or the row names
// one that no longer works.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the row to mark. Required.
//   - pendingAt time.Time: the instant to record. A zero value becomes time.Now().
//   - fenceToken string: the caller's provisioning claim. Required.
//
// Returns:
//   - error: ErrSubscriberNotFound, a claim-lost conflict, a validation error, or a
//     wrapped write failure.
func (d Datasource) RecordSubscriberCredentialCleanupPending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredentialCleanupPending")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if pendingAt.IsZero() {
		pendingAt = time.Now()
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_cleanup_pending_at = COALESCE(credential_cleanup_pending_at, $1),
			updated_at = $2
		WHERE subscriber_id = $3
		  AND provisioning_token = $4
	`, pendingAt, time.Now(), strings.TrimSpace(subscriberID), fenceToken)
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the subscriber's pending credential cleanup",
			"record_subscriber_credential_cleanup_pending", err)
	}

	return d.requireFencedWriteLanded(ctx, result, subscriberID, fenceToken,
		"record_subscriber_credential_cleanup_pending")
}

// ClearSubscriberCredential erases a subscriber's credential record, returning it to
// the "registered, not yet provisioned" state.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Returns:
//   - error: a typed conflict when the claim is no longer the caller's, a typed
//     not-found error when no subscriber matches, or a logged internal error.
func (d Datasource) ClearSubscriberCredential(ctx context.Context, subscriberID, fenceToken string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClearSubscriberCredential")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_reference = NULL,
			credential_issued_at = NULL,
			credential_cleanup_pending_at = NULL,
			-- AND THE REVOCATION MARKERS, in the same statement. F14: revocation_pending_at is
			-- stamped BEFORE the broker is touched, so it means "a principal may still
			-- authenticate". Every caller of this statement reaches it only once the broker has
			-- CONFIRMED the revocation, at which point that sentence is false — and a row that
			-- kept the stamp meant the opposite of what the column says.
			--
			-- It costs more than tidiness. CountSubscriberRevocationsPending counts tombstoned
			-- rows and blnk_subscribers_oldest_revocation_age_seconds raises a CRITICAL alert
			-- whose runbook tells an operator to delete a SCRAM credential by hand; counting a
			-- confirmed-clean row sent them after a principal that no longer exists, and made a
			-- row where a credential really was unaccounted for indistinguishable from inert
			-- residue. The failure marker goes with it because it describes the latest ATTEMPT,
			-- and the latest attempt succeeded.
			revocation_pending_at = NULL,
			revocation_failed_at = NULL,
			settlement_attempts = CASE
				WHEN grant_reconcile_pending_at IS NULL THEN 0
				ELSE settlement_attempts
			END,
			settlement_last_error = CASE
				WHEN grant_reconcile_pending_at IS NULL THEN NULL
				ELSE settlement_last_error
			END,
			updated_at = $1
		WHERE subscriber_id = $2
		  AND provisioning_token = $3
	`, time.Now(), strings.TrimSpace(subscriberID), strings.TrimSpace(fenceToken))
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to clear subscriber credential", "clear_subscriber_credential", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to clear subscriber credential", "clear_subscriber_credential", err)
	}

	if affected == 0 {
		miss := d.classifyFencedWriteMiss(ctx, subscriberID, fenceToken,
			"Failed to clear subscriber credential")
		failDatabaseSpan(span, miss)

		return miss
	}

	span.AddEvent("Subscriber credential cleared", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))

	return nil
}

// PurgeMigratedSubscriberWebhookURLs erases the legacy webhook URL of every subscriber
// that completed its migration before a cut-off.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - migratedBefore time.Time: purge subscribers whose migration completed strictly
//     before this instant.
//
// Returns:
//   - int64: how many rows were purged. Zero is a normal outcome.
//   - error: a typed invalid-input error for a zero cut-off, or a logged internal
//     error.
func (d Datasource) PurgeMigratedSubscriberWebhookURLs(
	ctx context.Context,
	migratedBefore time.Time,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PurgeMigratedSubscriberWebhookURLs")
	defer span.End()

	if migratedBefore.IsZero() {
		err := apierror.NewAPIError(apierror.ErrInvalidInput,
			"A retention cut-off is required to purge migrated subscriber webhook URLs", nil)
		failDatabaseSpan(span, err)

		return 0, err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET webhook_url = NULL,
			updated_at = $1
		WHERE webhook_url IS NOT NULL
		  AND migrated_at IS NOT NULL
		  AND migrated_at < $2
	`, time.Now(), migratedBefore.UTC())
	if err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to purge migrated subscriber webhook URLs",
			"purge_migrated_subscriber_webhook_urls", err)
	}

	purged, err := result.RowsAffected()
	if err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to purge migrated subscriber webhook URLs",
			"purge_migrated_subscriber_webhook_urls", err)
	}

	if purged > 0 {
		logrus.WithFields(logrus.Fields{
			"purged":          purged,
			"migrated_before": migratedBefore.UTC().Format(time.RFC3339),
		}).Info("purged the legacy webhook URL of migrated subscribers")
	}

	span.SetAttributes(attribute.Int64("subscriber.webhook_urls_purged", purged))

	return purged, nil
}

// MarkSubscriberCredentialOrphaned records that a credential exists at the broker which
// Blnk could neither record nor revoke.
//
// Parameters:
//   - ctx context.Context: bounds the write.
//   - subscriberID string: the row to mark. Required.
//   - orphanedAt time.Time: the instant to record on the FIRST marking.
//
// Returns:
//   - error: nil when the marker is in place or the row is gone; a logged internal
//     error when the write itself failed.
func (d Datasource) MarkSubscriberCredentialOrphaned(
	ctx context.Context,
	subscriberID string,
	orphanedAt time.Time,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberCredentialOrphaned")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	if _, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_orphaned_at = COALESCE(credential_orphaned_at, $1),
			updated_at = $2
		WHERE subscriber_id = $3
	`, orphanedAt, time.Now(), strings.TrimSpace(subscriberID)); err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record an orphaned subscriber credential",
			"mark_subscriber_credential_orphaned", err)
	}

	span.AddEvent("Subscriber credential marked orphaned")

	return nil
}

// MarkSubscriberRevocationFailed records that the most recent revocation attempt was
// refused by the broker.
//
// Parameters:
//   - ctx context.Context: bounds the write. Callers pass a compensation context.
//   - subscriberID string: the row to mark. Required.
//   - failedAt time.Time: when the attempt failed.
//
// Returns:
//   - error: nil when the marker is in place or the row is gone; a logged internal
//     error otherwise.
func (d Datasource) MarkSubscriberRevocationFailed(
	ctx context.Context,
	subscriberID string,
	failedAt time.Time,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberRevocationFailed")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	if _, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET revocation_failed_at = $1,
			updated_at = $2
		WHERE subscriber_id = $3
	`, failedAt, time.Now(), strings.TrimSpace(subscriberID)); err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record a failed subscriber revocation",
			"mark_subscriber_revocation_failed", err)
	}

	span.AddEvent("Subscriber revocation marked failed")

	return nil
}

// RecordSubscriberCredentialIfUnchanged persists an issuance ONLY IF the subscriber
// still holds the credential reference the caller last observed.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - expected *string: the credential reference the caller observed before
//     provisioning.
//   - credentialReference string: the new non-reversible reference.
//   - issuedAt time.Time: the issuance instant.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Returns:
//   - error: a typed conflict when the stored reference no longer matches expected or
//     the claim is no longer the caller's, a typed not-found when no subscriber
//     matches, an invalid-input error when the reference is not a derived reference, or
//     a logged internal error.
func (d Datasource) RecordSubscriberCredentialIfUnchanged(
	ctx context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
	claimToken string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredentialIfUnchanged")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}
	if err := requireFenceToken(claimToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	// The same guard RecordSubscriberCredential applies, and for the same reason: the
	// column must never hold anything a caller could authenticate with, and the offending
	// value is deliberately not echoed into the error, because a caller who passed a
	// secret here by mistake must not have it copied into a log line.
	if err := model.ValidateCredentialReference(credentialReference); err != nil {
		wrapped := apierror.NewAPIError(apierror.ErrInvalidInput,
			"Credential reference must be a reference derived by model.DeriveCredentialReference", nil)
		failDatabaseSpan(span, wrapped)

		return wrapped
	}

	if err := requireFenceToken(claimToken); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	span.SetAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.Bool("subscriber.first_issuance", expected == nil),
	)

	// Two arms rather than one predicate, because `credential_reference = NULL` is never
	// true in SQL: IS NULL and = <value> are different operators, and folding them into one
	// statement with a coalesce would make an empty-string reference collide with the
	// no-credential case.
	var (
		result sql.Result
		err    error
		now    = time.Now()
	)
	// BOTH conditions, and each guards a different race.
	if expected == nil {
		result, err = d.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET credential_reference = $1,
				credential_issued_at = $2,
				credential_cleanup_pending_at = NULL,
				-- AND THE ORPHAN MARKER, in the same statement. Kafka stores ONE SCRAM
				-- credential per principal, so the issuance being recorded here REPLACED
				-- whatever was orphaned: the orphaned secret stopped authenticating the moment
				-- this one was written. A marker left standing would keep a critical alert
				-- firing on an exposure that no longer exists, which is how an alert stops
				-- being believed — and it is why the documented remedy for an orphan is to
				-- issue once more rather than to clear a column by hand.
				credential_orphaned_at = NULL,
				settlement_attempts = CASE
					WHEN grant_reconcile_pending_at IS NULL THEN 0
					ELSE settlement_attempts
				END,
				settlement_last_error = CASE
					WHEN grant_reconcile_pending_at IS NULL THEN NULL
					ELSE settlement_last_error
				END,
				updated_at = $3
			WHERE subscriber_id = $4
			  AND credential_reference IS NULL
			  AND provisioning_token = $5
		`, credentialReference, issuedAt, now, strings.TrimSpace(subscriberID),
			strings.TrimSpace(claimToken))
	} else {
		result, err = d.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET credential_reference = $1,
				credential_issued_at = $2,
				credential_cleanup_pending_at = NULL,
				-- AND THE ORPHAN MARKER, in the same statement. Kafka stores ONE SCRAM
				-- credential per principal, so the issuance being recorded here REPLACED
				-- whatever was orphaned: the orphaned secret stopped authenticating the moment
				-- this one was written. A marker left standing would keep a critical alert
				-- firing on an exposure that no longer exists, which is how an alert stops
				-- being believed — and it is why the documented remedy for an orphan is to
				-- issue once more rather than to clear a column by hand.
				credential_orphaned_at = NULL,
				settlement_attempts = CASE
					WHEN grant_reconcile_pending_at IS NULL THEN 0
					ELSE settlement_attempts
				END,
				settlement_last_error = CASE
					WHEN grant_reconcile_pending_at IS NULL THEN NULL
					ELSE settlement_last_error
				END,
				updated_at = $3
			WHERE subscriber_id = $4
			  AND credential_reference = $5
			  AND provisioning_token = $6
		`, credentialReference, issuedAt, now, strings.TrimSpace(subscriberID), *expected,
			strings.TrimSpace(claimToken))
	}
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record subscriber credential",
			"record_subscriber_credential_if_unchanged", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record subscriber credential",
			"record_subscriber_credential_if_unchanged", err)
	}

	if affected == 0 {
		// No row matched, and the reasons need different answers: the subscriber may be gone,
		// the claim may no longer be the caller's, or the row may exist under this claim
		// holding a different reference. One read settles the first two; only when the claim
		// is found intact is this the superseding-issuance case.
		miss, readErr := d.describeFencedWriteMiss(ctx, subscriberID, claimToken,
			"Failed to record subscriber credential")
		if readErr != nil {
			failDatabaseSpan(span, readErr)

			return readErr
		}

		if miss != subscriberFenceMissOther {
			classified := fencedWriteMissError(subscriberID, miss)
			failDatabaseSpan(span, classified)

			return classified
		}

		conflict := apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber's credential changed while this issuance was in flight",
			fmt.Errorf("subscriber %s no longer holds the expected credential reference; "+
				"a concurrent issuance superseded this one, and the secret it returned is the "+
				"one that works", hashedEventIdentifier(strings.TrimSpace(subscriberID))))
		failDatabaseSpan(span, conflict)

		return conflict
	}

	span.AddEvent("Subscriber credential recorded", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.credential_fingerprint",
			model.CredentialFingerprint(credentialReference)),
	))

	return nil
}
