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
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ---------------------------------------------------------------------------------------
// The revocation lifecycle and the provisioning fence

// defaultSubscriberFenceLease is the fence lease used when a caller supplies none.
const defaultSubscriberFenceLease = 15 * time.Second

// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - pendingAt time.Time: the instant to record on the FIRST marking.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Returns:
//   - *model.EventSubscriber: the marked row, carrying the principal and topics to
//     revoke.
//   - error: a typed conflict when the claim is no longer the caller's, a typed
//     not-found when no subscriber matches, or a logged internal error.
func (d Datasource) MarkSubscriberRevocationPending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
	fenceToken string,
) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberRevocationPending")
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
		UPDATE blnk.event_subscribers
		SET revocation_pending_at = COALESCE(revocation_pending_at, $1),
			updated_at = $2,
			-- EVERY NEW ATTEMPT CLEARS THE LAST ONE'S FAILURE. revocation_failed_at means
			-- "the most recent attempt was refused by the broker", which is a different fact
			-- from revocation_pending_at's "a deregistration began" — and the two are only
			-- readable together if this one describes the LATEST attempt rather than
			-- accumulating history. Clearing it here, at the start of the attempt, is what
			-- makes "pending set, failed NULL" mean "in flight or awaiting deletion" and
			-- "pending set, failed set" mean "the broker refused; fix the broker side".
			--
			-- revocation_pending_at deliberately keeps its FIRST value through the COALESCE
			-- above, because the quantity an operator alerts on is the age of the exposure.
			revocation_failed_at = NULL
		WHERE subscriber_id = $3
		  AND provisioning_token = $4
		RETURNING `+eventSubscriberColumns,
		pendingAt, time.Now(), strings.TrimSpace(subscriberID), strings.TrimSpace(fenceToken),
	)

	marked, err := scanEventSubscriber(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			miss := d.classifyFencedWriteMiss(ctx, subscriberID, fenceToken,
				"Failed to mark the subscriber for revocation")
			failDatabaseSpan(span, miss)

			return nil, miss
		}

		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to mark the subscriber for revocation",
			"mark_subscriber_revocation_pending", err)
	}

	span.AddEvent("Subscriber marked for revocation", trace.WithAttributes(
		attribute.String("subscriber.id", marked.SubscriberID),
		attribute.String("subscriber.principal", marked.KafkaPrincipal),
	))

	return &marked, nil
}

// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation and
// returns the token that claim is held under.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - lease time.Duration: how long the claim is held.
//
// Returns:
//   - string: the claim token, which every completion or release must present.
//   - error: a typed conflict when another operation holds a live claim, a typed
//     not-found when no subscriber matches, or a logged internal error.
func (d Datasource) ClaimSubscriberForProvisioning(
	ctx context.Context,
	subscriberID string,
	lease time.Duration,
) (string, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimSubscriberForProvisioning")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return "", err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive subscriber provisioning fence lease; falling back to %s", defaultSubscriberFenceLease)
		lease = defaultSubscriberFenceLease
	}

	token := uuid.NewString()

	span.SetAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.fence_lease", lease.String()),
	)

	var claimed string
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_subscribers
		SET provisioning_token = $1,
			provisioning_until = NOW() + $2::interval,
			updated_at = $3
		WHERE subscriber_id = $4
		  AND (provisioning_until IS NULL OR provisioning_until < NOW())
		RETURNING provisioning_token
	`, token, lease.String(), time.Now(), strings.TrimSpace(subscriberID)).Scan(&claimed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row matched, and the two reasons need different answers: the subscriber may not
			// exist, or it may exist under a live claim. A separate read distinguishes them,
			// because reporting a conflict for a subscriber that was never registered would send
			// a caller looking for a race that did not happen.
			if _, readErr := d.GetEventSubscriberByID(ctx, subscriberID); readErr != nil {
				failDatabaseSpan(span, readErr)

				return "", readErr
			}

			conflict := apierror.NewAPIError(apierror.ErrConflict,
				"Another credential operation for this subscriber is already in progress",
				fmt.Errorf("subscriber %s is fenced by a live provisioning claim; retry once it "+
					"completes or once its lease expires",
					hashedEventIdentifier(strings.TrimSpace(subscriberID))))
			failDatabaseSpan(span, conflict)

			return "", conflict
		}

		failDatabaseSpan(span, err)

		return "", loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to claim the subscriber for provisioning",
			"claim_subscriber_for_provisioning", err)
	}

	span.AddEvent("Subscriber claimed for provisioning", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))

	return claimed, nil
}

// RenewSubscriberProvisioningFence extends a live claim, if the caller still holds it.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - token string: the token the claim was taken under. Required.
//   - lease time.Duration: how much longer the claim is held FROM NOW.
//
// Returns:
//   - error: a typed conflict when the claim is no longer the caller's, a typed
//     not-found when the row is gone, a typed validation error for a missing token, or
//     a logged internal error.
func (d Datasource) RenewSubscriberProvisioningFence(
	ctx context.Context,
	subscriberID string,
	token string,
	lease time.Duration,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RenewSubscriberProvisioningFence")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if err := requireFenceToken(token); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive subscriber provisioning fence renewal; falling back to %s",
				defaultSubscriberFenceLease)
		lease = defaultSubscriberFenceLease
	}

	span.SetAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.fence_lease", lease.String()),
	)

	// provisioning_until is recomputed from NOW() rather than added to its current value,
	// so a renewal grants exactly one lease of headroom however late it arrives. Extending
	// the stored value instead would let a caller that renewed often accumulate a fence
	// far longer than the lease, which is the long-outage-after-a-crash case the short
	// lease exists to avoid.
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET provisioning_until = NOW() + $1::interval,
			updated_at = $2
		WHERE subscriber_id = $3
		  AND provisioning_token = $4
	`, lease.String(), time.Now(), strings.TrimSpace(subscriberID), strings.TrimSpace(token))
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the subscriber provisioning fence",
			"renew_subscriber_provisioning_fence", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to renew the subscriber provisioning fence",
			"renew_subscriber_provisioning_fence", err)
	}

	if affected == 0 {
		// Unlike the release path, a failed renewal is NOT merely logged: the caller is about
		// to touch the broker and must not, so the reason is classified and returned.
		miss := d.classifyFencedWriteMiss(ctx, subscriberID, token,
			"Failed to renew the subscriber provisioning fence")
		failDatabaseSpan(span, miss)

		return miss
	}

	return nil
}

// ReleaseSubscriberProvisioningFence clears a claim, if the caller still holds it.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - token string: the token the claim was taken under. Required.
//
// Returns:
//   - error: a typed conflict when the claim is no longer the caller's, a typed
//     validation error for a missing token, or a logged internal error.
func (d Datasource) ReleaseSubscriberProvisioningFence(
	ctx context.Context,
	subscriberID string,
	token string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ReleaseSubscriberProvisioningFence")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)

		return err
	}

	if strings.TrimSpace(token) == "" {
		err := apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required to release the fence", nil)
		failDatabaseSpan(span, err)

		return err
	}

	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET provisioning_token = NULL,
			provisioning_until = NULL,
			updated_at = $1
		WHERE subscriber_id = $2
		  AND provisioning_token = $3
	`, time.Now(), strings.TrimSpace(subscriberID), strings.TrimSpace(token))
	if err != nil {
		failDatabaseSpan(span, err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to release the subscriber provisioning fence",
			"release_subscriber_provisioning_fence", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The release itself succeeded, and the caller
		// only logs this outcome, so reporting success is the honest answer rather than
		// manufacturing a conflict from a bookkeeping gap.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": hashedEventIdentifier(strings.TrimSpace(subscriberID)),
			"error_class":        databaseErrorClass(err),
			"sqlstate":           postgresSQLState(err),
		}).Debug("Could not determine whether the subscriber provisioning fence was released")
		logDatabaseDiagnostic("release_subscriber_provisioning_fence", err)

		return nil
	}

	if affected == 0 {
		conflict := apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller",
			fmt.Errorf("releasing the provisioning fence of subscriber %s matched no row because "+
				subscriberFenceLostMarker+"; the claim expired or was taken over while the "+
				"operation was in flight",
				hashedEventIdentifier(strings.TrimSpace(subscriberID))))
		failDatabaseSpan(span, conflict)

		return conflict
	}

	return nil
}
