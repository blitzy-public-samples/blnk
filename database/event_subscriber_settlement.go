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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CountSubscriberSettlementObligations reports how many subscribers owe broker-side
// reconciliation, split by kind, and when the oldest of those obligations was recorded.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//
// Returns:
//   - model.SubscriberSettlementBacklog: the counts and the oldest instant.
//   - error: a logged internal error when the read failed.
func (d Datasource) CountSubscriberSettlementObligations(
	ctx context.Context,
) (model.SubscriberSettlementBacklog, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountSubscriberSettlementObligations")
	defer span.End()

	var (
		backlog model.SubscriberSettlementBacklog
		oldest  sql.NullTime
	)

	// LEAST over the two MINs rather than MIN over a coalesce: LEAST ignores NULL arguments, so a
	// deployment owing only one kind of obligation still reports that kind's oldest instant
	// instead of NULL.
	err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FILTER (
				   WHERE grant_reconcile_pending_at IS NOT NULL
					  OR credential_cleanup_pending_at IS NOT NULL
			   ),
			   COUNT(*) FILTER (WHERE grant_reconcile_pending_at IS NOT NULL),
			   COUNT(*) FILTER (WHERE credential_cleanup_pending_at IS NOT NULL),
			   LEAST(MIN(grant_reconcile_pending_at), MIN(credential_cleanup_pending_at))
		FROM blnk.event_subscribers
	`).Scan(
		&backlog.Outstanding,
		&backlog.GrantReconcilePending,
		&backlog.CredentialCleanupPending,
		&oldest,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.SubscriberSettlementBacklog{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count outstanding subscriber settlement obligations",
			"count_subscriber_settlement_obligations", err)
	}

	if oldest.Valid {
		backlog.OldestPendingAt = oldest.Time.UTC()
	}

	span.SetAttributes(attribute.Int64("subscriber.settlement_outstanding", backlog.Outstanding))

	return backlog, nil
}

// GetSubscriberSettlementObligation reads what ONE subscriber currently owes.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - subscriberID string: the row to read. Required.
//
// Returns:
//   - model.SubscriberSettlementObligation: what the subscriber owes.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a validation error,
//     or a wrapped read failure.
func (d Datasource) GetSubscriberSettlementObligation(
	ctx context.Context,
	subscriberID string,
) (model.SubscriberSettlementObligation, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetSubscriberSettlementObligation")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return model.SubscriberSettlementObligation{}, err
	}

	obligation := model.SubscriberSettlementObligation{}

	err := d.Conn.QueryRowContext(ctx, `
		SELECT subscriber_id,
			   grant_reconcile_pending_at IS NOT NULL,
			   credential_cleanup_pending_at IS NOT NULL,
			   settlement_attempts,
			   COALESCE(settlement_last_error, '')
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, strings.TrimSpace(subscriberID)).Scan(
		&obligation.SubscriberID,
		&obligation.GrantReconcilePending,
		&obligation.CredentialCleanupPending,
		&obligation.Attempts,
		&obligation.LastError,
	)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		notFound := apierror.NewAPIError(apierror.ErrSubscriberNotFound,
			"Subscriber not found", err)
		failDatabaseSpan(span, notFound)

		return model.SubscriberSettlementObligation{}, notFound
	case err != nil:
		failDatabaseSpan(span, err)

		return model.SubscriberSettlementObligation{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the subscriber's settlement obligation",
			"get_subscriber_settlement_obligation", err)
	}

	return obligation, nil
}

// ListSubscriberSettlementObligations returns the subscribers that owe broker-side
// work, oldest attempt first.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - limit int: the maximum number of obligations to return. Bounded to the registry
//     page maximum; a non-positive value takes the default.
//   - notBefore time.Time: skip rows attempted at or after this instant.
//
// Returns:
//   - []model.SubscriberSettlementObligation: the outstanding obligations, oldest
//     attempt first.
//   - error: a wrapped read failure.
func (d Datasource) ListSubscriberSettlementObligations(
	ctx context.Context,
	limit int,
	notBefore time.Time,
) ([]model.SubscriberSettlementObligation, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListSubscriberSettlementObligations")
	defer span.End()

	if limit <= 0 {
		limit = defaultSubscriberPageSize
	}
	if limit > maxSubscriberPageSize {
		limit = maxSubscriberPageSize
	}

	// The WHERE clause repeats the partial index's predicate EXACTLY. A partial index is
	// usable only when the query's condition implies the index's, and the planner proves
	// that by comparing the expressions it can see — so spelling this differently would
	// silently turn every poll into a sequential scan of the whole registry.
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT subscriber_id,
			   grant_reconcile_pending_at IS NOT NULL,
			   credential_cleanup_pending_at IS NOT NULL,
			   settlement_attempts,
			   COALESCE(settlement_last_error, '')
		FROM blnk.event_subscribers
		WHERE (grant_reconcile_pending_at IS NOT NULL
			   OR credential_cleanup_pending_at IS NOT NULL)
		  AND ($1::timestamptz IS NULL
			   OR settlement_last_attempt_at IS NULL
			   OR settlement_last_attempt_at < $1)
		ORDER BY settlement_last_attempt_at ASC NULLS FIRST, id ASC
		LIMIT $2
	`, nullableTime(notBefore), limit)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list the subscribers owing broker-side settlement",
			"list_subscriber_settlement_obligations", err)
	}
	// The house form in this package: the error is discarded explicitly rather than
	// silently. A Close failure after a successful scan tells the caller nothing
	// actionable — the rows are already read — but discarding it in writing is what
	// distinguishes a deliberate choice from an oversight, and it is what every other
	// paged read here does.
	defer func() { _ = rows.Close() }()

	obligations := make([]model.SubscriberSettlementObligation, 0, limit)

	for rows.Next() {
		var obligation model.SubscriberSettlementObligation
		if scanErr := rows.Scan(
			&obligation.SubscriberID,
			&obligation.GrantReconcilePending,
			&obligation.CredentialCleanupPending,
			&obligation.Attempts,
			&obligation.LastError,
		); scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to read a subscriber settlement obligation",
				"list_subscriber_settlement_obligations", scanErr)
		}

		obligations = append(obligations, obligation)
	}

	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the subscriber settlement obligations",
			"list_subscriber_settlement_obligations", err)
	}

	return obligations, nil
}

// MarkSubscriberSettlementAttempt records that a settlement pass tried this row and
// what happened.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the row attempted. Required.
//   - attemptedAt time.Time: when. A zero value becomes time.Now().
//   - failure string: the sanitized failure text, or "" when the pass succeeded.
//
// Returns:
//   - error: a validation error or a wrapped write failure.
func (d Datasource) MarkSubscriberSettlementAttempt(
	ctx context.Context,
	subscriberID string,
	attemptedAt time.Time,
	failure string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberSettlementAttempt")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if attemptedAt.IsZero() {
		attemptedAt = time.Now()
	}

	if _, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET settlement_attempts = settlement_attempts + 1,
			settlement_last_attempt_at = $1,
			settlement_last_error = NULLIF($2, ''),
			updated_at = $3
		WHERE subscriber_id = $4
	`, attemptedAt, failure, time.Now(), strings.TrimSpace(subscriberID)); err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record the subscriber's settlement attempt",
			"mark_subscriber_settlement_attempt", err)
	}

	return nil
}

// TakeEventSubscriber removes a subscriber and RETURNS the row it removed.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Returns:
//   - *model.EventSubscriber: the row as it was immediately before deletion.
//   - error: a typed conflict when the claim is no longer the caller's, a typed
//     not-found error when no subscriber matched, or a logged internal error.
func (d Datasource) TakeEventSubscriber(
	ctx context.Context,
	subscriberID string,
	fenceToken string,
) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TakeEventSubscriber")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return nil, err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)

		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
		  AND provisioning_token = $2
		RETURNING `+eventSubscriberColumns,
		strings.TrimSpace(subscriberID),
		strings.TrimSpace(fenceToken),
	)

	deleted, err := scanEventSubscriber(row)
	if err != nil {
		failDatabaseSpan(span, err)

		if errors.Is(err, sql.ErrNoRows) {
			return nil, d.classifyFencedWriteMiss(ctx, subscriberID, fenceToken,
				"Failed to delete event subscriber")
		}

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to delete event subscriber", "take_event_subscriber", err)
	}

	span.AddEvent("Event subscriber deleted", trace.WithAttributes(
		attribute.String("subscriber.id", deleted.SubscriberID),
		attribute.String("subscriber.principal", deleted.KafkaPrincipal),
		attribute.Int("subscriber.authorized_topic_count", len(deleted.AuthorizedTopics)),
	))

	return &deleted, nil
}

// CountSubscriberRevocationsPending reports how many subscribers still owe a
// broker-side credential revocation, and when the oldest of those obligations was
// recorded.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//
// Returns:
//   - model.SubscriberRevocationBacklog: the count and the oldest instant.
//   - error: a logged internal error when the read failed.
func (d Datasource) CountSubscriberRevocationsPending(
	ctx context.Context,
) (model.SubscriberRevocationBacklog, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountSubscriberRevocationsPending")
	defer span.End()

	var (
		backlog model.SubscriberRevocationBacklog
		oldest  sql.NullTime
	)

	err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(revocation_pending_at)
		FROM blnk.event_subscribers
		WHERE revocation_pending_at IS NOT NULL
	`).Scan(&backlog.Pending, &oldest)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.SubscriberRevocationBacklog{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count outstanding subscriber revocations",
			"count_subscriber_revocations_pending", err)
	}

	if oldest.Valid {
		backlog.OldestPendingAt = oldest.Time.UTC()
	}

	span.SetAttributes(attribute.Int64("subscriber.revocations_pending", backlog.Pending))

	return backlog, nil
}

// CountSubscriberAccessResidue reports how much broker-side access is UNACCOUNTED FOR:
//
// Parameters:
//   - ctx context.Context: cancels the read.
//
// Returns:
//   - model.SubscriberAccessResidue: the two counts and the two oldest instants.
//   - error: a logged internal error when the read failed.
func (d Datasource) CountSubscriberAccessResidue(
	ctx context.Context,
) (model.SubscriberAccessResidue, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountSubscriberAccessResidue")
	defer span.End()

	var (
		residue                 model.SubscriberAccessResidue
		oldestOrphan, oldestBad sql.NullTime
	)

	err := d.Conn.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE credential_orphaned_at IS NOT NULL),
			MIN(credential_orphaned_at),
			COUNT(*) FILTER (WHERE revocation_failed_at IS NOT NULL),
			MIN(revocation_failed_at)
		FROM blnk.event_subscribers
	`).Scan(
		&residue.OrphanedCredentials,
		&oldestOrphan,
		&residue.FailedRevocations,
		&oldestBad,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.SubscriberAccessResidue{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count unaccounted subscriber access",
			"count_subscriber_access_residue", err)
	}

	if oldestOrphan.Valid {
		residue.OldestOrphanedAt = oldestOrphan.Time.UTC()
	}
	if oldestBad.Valid {
		residue.OldestFailedRevocationAt = oldestBad.Time.UTC()
	}

	span.SetAttributes(
		attribute.Int64("subscriber.credential_orphans", residue.OrphanedCredentials),
		attribute.Int64("subscriber.revocation_failures", residue.FailedRevocations),
	)

	return residue, nil
}
