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
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------------------
// Registry CRUD

// RegisterSubscriber records a new subscriber and returns the stored row.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: a typed validation error, a typed conflict when the id or principal is
//     taken, or the repository's error.
func (s *EventSubscriberService) RegisterSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	// An ABSENT identifier is generated rather than refused, which is the house convention
	// every other Blnk resource follows — ledgers, balances, identities and API keys all
	// mint their own business key when the caller supplies none. The generated form is
	// "sub_<uuid>", so it satisfies the canonical identifier rule the schema's CHECK
	// constraints and the principal derivation share, and it is recognisable on sight in a
	// broker ACL listing.
	subscriberID := registration.SubscriberID
	if subscriberID == "" {
		subscriberID = model.GenerateSubscriberID()
	} else {
		canonical, canonicalErr := model.CanonicalizeSubscriberIdentifier(subscriberID)
		if canonicalErr != nil {
			return nil, invalidSubscriberIdentifier(canonicalErr)
		}

		subscriberID = canonical
	}

	name, err := normalizeSubscriberName(registration.Name)
	if err != nil {
		return nil, err
	}

	if err := validateSubscriberGrant(registration.AuthorizedTopics); err != nil {
		return nil, err
	}

	keyPrefix, err := normalizeSubscriberKeyScope(registration.PartitionKeyPrefix)
	if err != nil {
		return nil, err
	}

	principal, err := SubscriberKafkaPrincipal(subscriberID)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := SubscriberConsumerGroupID(subscriberID)
	if err != nil {
		return nil, err
	}

	if err := requireRecordableWebhookURL(registration.WebhookURL); err != nil {
		return nil, err
	}

	subscriber := &model.EventSubscriber{
		SubscriberID:       subscriberID,
		Name:               name,
		KafkaPrincipal:     principal,
		ConsumerGroupID:    consumerGroup,
		AuthorizedTopics:   registration.AuthorizedTopics,
		PartitionKeyPrefix: keyPrefix,
		WebhookURL:         registration.WebhookURL,
	}

	stored, err := store.CreateEventSubscriber(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(stored.SubscriberID),
		"principal_hash":         subscriberLogLabel(stored.KafkaPrincipal),
		"consumer_group_hash":    consumerGroupLogLabel(stored.ConsumerGroupID),
		"authorized_topic_count": len(stored.AuthorizedTopics),
		"legacy_webhook":         stored.WebhookURL != nil,
	}).Info("event subscriber: registered; no credential has been issued yet")

	return stored, nil
}

// GetSubscriber reads one subscriber by its business key.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank id, or the repository's error.
func (s *EventSubscriberService) GetSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	return store.GetEventSubscriberByID(ctx, strings.TrimSpace(subscriberID))
}

// ListSubscribers pages the registry, newest first.
func (s *EventSubscriberService) ListSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	store, err := s.requireStore()
	if err != nil {
		return model.SubscriberPage{}, err
	}

	return store.ListEventSubscribers(ctx, query)
}

// ListAndCountSubscribers pages the registry and counts it from ONE DATABASE SNAPSHOT.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - query model.SubscriberPageQuery: the page.
//
// Returns:
//   - model.SubscriberPage: the page, as ListSubscribers.
//   - int64: the registry size in the same snapshot.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the
//     repository's error.
func (s *EventSubscriberService) ListAndCountSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return model.SubscriberPage{}, 0, err
	}

	return store.ListAndCountEventSubscribers(ctx, query)
}

// CountSubscribers reports how many subscribers the registry holds.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//
// Returns:
//   - int64: the number of registered subscribers.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the
//     repository's error.
func (s *EventSubscriberService) CountSubscribers(ctx context.Context) (int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return 0, err
	}

	return store.CountEventSubscribers(ctx)
}

// UpdateSubscriber applies the mutable subset of a subscriber and returns the stored
// row.
//
// Parameters:
//   - ctx context.Context: cancels the reconciliation, the read and the write.
//   - subscriberID string: the business key of the row to update.
//   - changes SubscriberUpdate: the fields to apply.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank name or an ungrantable topic, ErrSubscriberProvisioningFailed
//     when the broker-side reconciliation did not complete, or the repository's error.
func (s *EventSubscriberService) UpdateSubscriber(
	ctx context.Context,
	subscriberID string,
	changes SubscriberUpdate,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED before anything is read, so the authorization this call reconciles cannot be
	// changed underneath it by a concurrent issuance writing the same bindings.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer fence.release(ctx)

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return nil, err
	}

	// REFUSED IN MEMORY ON A TOMBSTONED ROW, before anything is spent on it.
	if err := requireActiveSubscriber(subscriber, subscriberRefusedUpdateMessage); err != nil {
		return nil, err
	}
	// The BROKER-REPRESENTABLE authorization as the registry holds it, snapshotted before
	// the changes are applied on top. It is what authorizationNeedsReconciliation compares
	// against and what abandonUpdateAfterLostFence reports as still recorded.
	storedAuthorization := newSubscriberAuthorization(subscriber)

	// AND THE TWO FACTS THE POST-WRITE DISCLOSURE NEEDS, read here for the same reason:
	priorKeyPrefix := subscriberKeyPrefix(subscriber)
	priorCredentialIssued := subscriber.IsProvisioned()

	if changes.Name != nil {
		name, nameErr := normalizeSubscriberName(*changes.Name)
		if nameErr != nil {
			return nil, nameErr
		}

		subscriber.Name = name
	}

	// Non-nil replaces the whole set, INCLUDING when it is non-nil and empty: revoking
	// every topic grant is a legitimate instruction, and it is the only way to express
	// "authorised for nothing" on a row that currently holds topics.
	if changes.AuthorizedTopics != nil {
		if grantErr := validateSubscriberGrant(changes.AuthorizedTopics); grantErr != nil {
			return nil, grantErr
		}

		subscriber.AuthorizedTopics = changes.AuthorizedTopics
	}

	// A present empty string CLEARS the recorded key scope, which is why the column is
	// nullable and why this cannot be a plain string: NULL means "no key constraint" while
	// the empty string would mean "constrained to the empty prefix", and those are
	// opposite intents. normalizeSubscriberKeyScope owns that three-way mapping so
	// registration and update cannot disagree about it.
	if changes.PartitionKeyPrefix != nil {
		keyPrefix, prefixErr := normalizeSubscriberKeyScope(changes.PartitionKeyPrefix)
		if prefixErr != nil {
			return nil, prefixErr
		}

		subscriber.PartitionKeyPrefix = keyPrefix
	}

	// Same three-way handling for the legacy URL. Clearing it is how a migrated
	// subscriber's dual-run artefact is removed one row at a time;
	// PurgeMigratedWebhookURLs does it in bulk once a retention period has elapsed.
	if changes.WebhookURL != nil {
		if err := requireRecordableWebhookURL(changes.WebhookURL); err != nil {
			return nil, err
		}

		if strings.TrimSpace(*changes.WebhookURL) == "" {
			subscriber.WebhookURL = nil
		} else {
			webhookURL := *changes.WebhookURL
			subscriber.WebhookURL = &webhookURL
		}
	}

	// A partition key prefix recorded over a row that ALREADY HOLDS A CREDENTIAL is
	// refused unless something in front of the brokers authorises record keys. The refusal
	// is one half of a pair — requireProvisionableKeyScope guards the other order, prefix
	// first and credential afterwards — and on its own either is walked around by
	// approaching the state from the far side.
	_, keyScopeEnforced := s.keyScopeEnforcement()
	if err := requireRecordableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// AND CLEARING A PREFIX IS REFUSED WHERE THE DEPLOYMENT DECLARES A KEY-SCOPED MODEL.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// DOES THE BROKER HAVE TO BE TOUCHED AT ALL?
	//
	// THE PRESENCE OF A KEY SCOPE *IS* PART OF THIS, and its VALUE is not.
	reconcileBroker := authorizationNeedsReconciliation(storedAuthorization, subscriber, changes)

	// REFUSED: WIDENING THE BOUNDARY OF A PRINCIPAL WHOSE CREDENTIAL IS UNACCOUNTED FOR.
	if reconcileBroker {
		if err := refuseWideningUnaccountedAccess(subscriber, storedAuthorization); err != nil {
			return nil, err
		}
	}

	pruned, granted := 0, 0

	if reconcileBroker {
		// STEP 0 — RECORD THE OBLIGATION, before a single broker round trip.
		if err := store.RecordSubscriberGrantReconcilePending(
			ctx, subscriberID, time.Now(), fence.token,
		); err != nil {
			return nil, err
		}

		// STEP 1 — PRUNE. Whatever the new authorization no longer implies is removed at the
		// broker BEFORE the row records the narrowing, so a failure of the write below leaves
		// the subscriber with less access than the registry claims rather than more.
		prunePhase, endPrunePhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endPrunePhase()

			return nil, phaseErr
		}

		pruned, err = s.pruneBrokerAccess(prunePhase, subscriber)
		endPrunePhase()

		if err != nil {
			return nil, err
		}
	}

	// STEP 2 — PERSIST, under the claim. A write that matches no row here means the claim
	// was lost or the row was tombstoned while the prune above was in flight, and the
	// conflict that reports it is the whole point: the prune has already narrowed the
	// broker, so continuing to STEP 3 with a stale view would re-grant an authorization
	// somebody else has replaced. THE ROW AS STORED, not the one assembled above.
	persisted, err := store.UpdateEventSubscriber(ctx, subscriber, fence.token)
	if err != nil {
		// A LOST CLAIM IS NOT AN ORDINARY CONFLICT WHEN THE PRUNE ALREADY LANDED. The broker
		// now grants less than the registry records, and that residue has to be described
		// rather than merely returned as a 409 — see abandonUpdateAfterLostFence for why it
		// is deliberately NOT undone.
		if subscriberFenceWasLost(err) {
			return nil, s.abandonUpdateAfterLostFence(subscriber, storedAuthorization, pruned, err)
		}

		return nil, err
	}

	// From here the stored row IS the subscriber: the grant step below derives its
	// bindings from it, so using the assembled copy would risk granting against a value
	// the database did not accept.
	subscriber = persisted

	if reconcileBroker {
		// STEP 3 — GRANT. Whatever the new authorization adds reaches the broker only now
		// that the registry records it, so a failure here is again fail-closed. The error is
		// returned, since a caller told the update succeeded would believe a grant exists
		// that does not.
		grantPhase, endGrantPhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endGrantPhase()

			return nil, phaseErr
		}

		granted, err = s.grantBrokerAccess(grantPhase, subscriber)
		endGrantPhase()

		if err != nil {
			return nil, err
		}

		// STEP 4 — DISCHARGE. Only now do the broker and the row demonstrably agree, so only
		// now is the obligation satisfied.
		if clearErr := store.ClearSubscriberGrantReconcilePending(
			ctx, subscriberID, fence.token,
		); clearErr != nil {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
				"error":              sanitizeLogValue(clearErr.Error(), maxLoggedErrorLength),
			}).Warn(
				"event subscriber: the authorization change completed at both the registry and the broker, " +
					"but its pending-reconciliation marker could not be cleared; settlement will re-reconcile " +
					"a broker that already matches and clear it then",
			)
		}
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(subscriber.SubscriberID),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
		"legacy_webhook":         subscriber.WebhookURL != nil,
		"acl_reconciled":         reconcileBroker,
		"acl_bindings_removed":   pruned,
		"acl_bindings_created":   granted,
	}).Info(
		"event subscriber: registry row updated; its broker-side grant was reconciled only if the " +
			"recorded authorization could have moved",
	)

	// THE ONE TRANSITION THE LINE ABOVE CANNOT REPORT. Its ACL churn is the same churn any
	// narrowing produces and says nothing about a credential already in a subscriber's
	// hands: that credential was delivered with a response declaring direct broker access,
	// the Read it described has just been withdrawn, and the statement cannot be recalled.
	warnOnKeyScopeRecordedAfterIssuance(subscriber, priorKeyPrefix, priorCredentialIssued)

	return subscriber, nil
}

// DeregisterSubscriber ends a subscriber's access at the broker and then removes it
// from the registry.
//
// Parameters:
//   - ctx context.Context: cancels the revocation and the removal.
//   - subscriberID string: the business key of the subscriber to remove.
//
// Returns:
//   - *model.EventSubscriber: the row that was removed on success, or the TOMBSTONED
//     row when revocation failed — which is the description of the state left behind
//     and the row a retry will find.
//   - error: ErrSubscriberNotFound when no such subscriber exists; a typed conflict
//     when another operation holds the subscriber's claim;
//     ErrSubscriberProvisioningFailed when the broker-side revocation did not complete
//     and the row was therefore NOT deleted; the repository's error otherwise.
func (s *EventSubscriberService) DeregisterSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// STEP 1 — FENCE. Nothing may issue a credential for a principal that is being taken out of
	// service, and no second deregistration may run alongside this one.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer fence.release(ctx)

	// STEP 2 — TOMBSTONE. The row survives, carrying the principal and topics revocation
	// needs and saying plainly that this subscriber is on its way out.
	pending, err := store.MarkSubscriberRevocationPending(ctx, subscriberID, s.clock(), fence.token)
	if err != nil {
		return nil, err
	}

	admin, err := s.provisioner()
	if err != nil {
		// The row is TOMBSTONED, not deleted, so nothing is lost: it names the principal and
		// a retry finds it. The error is returned so the caller does not read the removal as
		// complete.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        kafkaErrorClassField("deregister_new_kafka_admin", err),
		}).Error(
			"event subscriber: no Kafka administrative client could be built, so this subscriber's " +
				"access was NOT revoked and its registry row is kept, marked pending revocation; " +
				"retry the deregistration once the broker is reachable",
		)

		return pending, err
	}

	if !admin.IsConfigured() {
		// THE MOST CONSEQUENTIAL BRANCH IN THIS FILE. Deleting the row destroys
		// the only record of which principal still has to be revoked, so the residue is
		// unfindable rather than merely unrevoked. The row stays, tombstoned, which is what
		// makes the retry possible at all.
		if err := refuseUnconfirmableBrokerWork(
			pending, "revoking this subscriber's Kafka access",
		); err != nil {
			return pending, err
		}

		// REACHING HERE MEANS THE ROW CARRIES NO CREDENTIAL EVIDENCE AT ALL: no reference, no
		// orphan marker and no unsettled cleanup obligation. Nothing can authenticate as the
		// principal, so any ACL binding naming it is inert and the row can be removed
		// directly. This is what keeps the registry usable without Kafka.
		removed, takeErr := store.TakeEventSubscriber(ctx, subscriberID, fence.token)
		if takeErr != nil {
			return pending, takeErr
		}

		// The row carried the claim, so it is gone too.
		fence.consumed()

		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(removed.SubscriberID),
			"principal_hash":     subscriberLogLabel(removed.KafkaPrincipal),
		}).Info(
			"event subscriber: deregistered; no broker is configured, so there was no " +
				"broker-side access to revoke",
		)

		return removed, nil
	}

	// STEP 3 — REVOKE, using the row that still exists, under a renewed claim and a
	// bounded phase deadline. Revocation is a sequence of administrative round trips — the
	// ACL bindings and then the SCRAM credential — and this method arrived with no
	// deadline of its own, so without the bound the phase could outlive the claim and STEP
	// 4 would then delete a row another operation had taken over.
	revokePhase, endRevokePhase, phaseErr := fence.brokerPhaseContext(ctx)
	if phaseErr != nil {
		endRevokePhase()

		// The row stays tombstoned, so the obligation is durable and a retry under a fresh
		// claim finishes it. Returned rather than pressed on with: the claim is not this
		// caller's, so revoking now would race the owner that holds it.
		return pending, phaseErr
	}

	revokeErr := admin.RevokeSubscriber(revokePhase, pending)
	endRevokePhase()

	if err := revokeErr; err != nil {
		// RECORD THAT THE ATTEMPT WAS REFUSED, not merely that it began.
		failureFields := logrus.Fields{
			"subscriber_id_hash":     subscriberLogLabel(pending.SubscriberID),
			"principal_hash":         subscriberLogLabel(pending.KafkaPrincipal),
			"authorized_topic_count": len(pending.AuthorizedTopics),
			"revocation_pending_at":  subscriberPendingSince(pending),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error_class": kafkaErrorClassField("subscriber_revocation", err),
		}

		markCtx, cancelMark := subscriberDurabilityContext(ctx)
		failureFields["revocation_failure_recorded"] = s.markRevocationFailed(
			markCtx, pending, failureFields, s.clock(),
		)
		cancelMark()

		logrus.WithFields(failureFields).Error(
			"event subscriber: revoking this subscriber's Kafka access failed, so its registry row " +
				"was NOT deleted and is kept marked pending revocation; the principal named here may " +
				"still authenticate until the deregistration is retried",
		)

		return pending, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber's Kafka access could not be revoked, so it was not removed",
			// The bounded detail, matching RevokeSubscriberCredential's treatment of the same
			// failure: the principal and the broker's own words stay in the log line above, and
			// the caller receives the diagnosis plus the fact that a retry is safe. Revocation
			// is idempotent at the broker, so repeating it cannot make things worse — and the
			// tombstone on the row is what makes it findable.
			NewSubscriberErrorDetail(
				"Revoking the subscriber's Kafka access failed", pending.SubscriberID, true,
			),
		)
	}

	// STEP 3b — WITHDRAW THE KEY-SCOPE BINDING at the declared component, now that the
	// broker credential is gone.
	if err := s.revokeKeyScopeBinding(ctx, pending); err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        kafkaErrorClassField("key_scope_gateway_revoke", err),
		}).Warn(
			"event subscriber: this subscriber's Kafka credential was revoked, but withdrawing its " +
				"key-scope binding at the declared enforcement gateway failed; the principal can no " +
				"longer authenticate, so no access remains — remove the stale binding at the gateway " +
				"when it is reachable again",
		)
	}

	// STEP 4 — DELETE, now that the broker-side cleanup is confirmed, and under the claim.
	removed, err := store.TakeEventSubscriber(ctx, subscriberID, fence.token)
	if err != nil {
		// The access is already gone, so this residue is the harmless direction: a registry
		// row describing a subscriber that can no longer authenticate. Retrying the
		// deregistration removes it, and the tombstone is what makes the row identifiable as
		// needing that. F14: THE ROW MUST STOP DESCRIBING A CREDENTIAL AND AN OUTSTANDING
		// REVOCATION.
		fields := logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        database.ErrorClass(err),
			"sqlstate":           database.SQLState(err),
		}

		if cleared := s.clearCredentialRecord(ctx, pending, fence.token, fields); cleared {
			// The RETURNED row is brought into line with the stored one. A caller handed a row
			// that still reports IsRevocationPending would conclude a principal may still
			// authenticate, which the confirmed revocation has just made false — and the caller
			// is the one place that reads this value programmatically.
			pending.CredentialReference = nil
			pending.CredentialIssuedAt = nil
			pending.RevocationPendingAt = nil
			pending.RevocationFailedAt = nil

			fields["credential_record_cleared"] = true
		} else {
			fields["credential_record_cleared"] = false
		}

		logrus.WithFields(fields).Error(
			"event subscriber: Kafka access was revoked but the registry row could not be deleted; " +
				"the subscriber can no longer authenticate and the inert row remains until the " +
				"deregistration is retried",
		)

		return pending, err
	}

	// The row carried the claim, so it is gone too.
	fence.consumed()

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(removed.SubscriberID),
		"principal_hash":     subscriberLogLabel(removed.KafkaPrincipal),
	}).Info("event subscriber: Kafka access revoked and the subscriber deregistered")

	return removed, nil
}

// ---------------------------------------------------------------------------------------
// The dual-run migration surface

// RecordLegacyWebhookSubscription records the legacy HTTP endpoint a migrating
// subscriber received pushes on, and returns the row as it now stands.
//
// Parameters:
//   - ctx context.Context: cancels the write and the read-back.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, read back after the write.
//   - error: a typed validation error for a blank or unacceptable URL,
//     ErrSubscriberNotFound, or the repository's write error.
func (s *EventSubscriberService) RecordLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID, webhookURL string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// Trimmed HERE rather than at the repository, which stores and validates the value
	// verbatim: one column, one stored form, and the check sees the bytes that land in it.
	trimmed := strings.TrimSpace(webhookURL)
	if trimmed == "" {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A webhook URL is required",
			errors.New(
				"event subscriber: the webhook url is blank; clear the subscription instead of "+
					"recording an empty one",
			),
		)
	}

	// REFUSED PAST THE RETIREMENT INSTANT, through the same helper UpdateSubscriber uses.
	if err := requireRecordableWebhookURL(&trimmed); err != nil {
		return nil, err
	}

	if err := store.RecordSubscriberWebhookURL(ctx, subscriberID, trimmed); err != nil {
		return nil, err
	}

	logrus.WithField(
		"subscriber_id_hash", subscriberLogLabel(subscriberID),
	).Info(
		"event subscriber: legacy webhook subscription recorded; the subscriber is counted as " +
			"awaiting migration again",
	)

	// Read back rather than assembled, so the response describes the row as stored — the
	// write cleared a column this call never mentioned, and a caller must see that.
	return store.GetEventSubscriberByID(ctx, subscriberID)
}

// ClearLegacyWebhookSubscription removes a single subscriber's recorded legacy
// endpoint.
//
// Parameters:
//   - ctx context.Context: cancels the write and the read-back.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound, or the repository's write error.
func (s *EventSubscriberService) ClearLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	if err := store.ClearSubscriberWebhookURL(ctx, subscriberID); err != nil {
		return nil, err
	}

	logrus.WithField(
		"subscriber_id_hash", subscriberLogLabel(subscriberID),
	).Info("event subscriber: legacy webhook subscription cleared")

	return store.GetEventSubscriberByID(ctx, subscriberID)
}

// MarkSubscriberMigrated stamps the instant a subscriber completed its move from legacy
// HTTP delivery to Kafka consumption, WITHOUT touching webhook_url.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - time.Time: the instant recorded, in UTC, so a caller can report it without
//     re-reading the row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, or the repository's
//     error.
func (s *EventSubscriberService) MarkSubscriberMigrated(
	ctx context.Context,
	subscriberID string,
) (time.Time, error) {
	store, err := s.requireStore()
	if err != nil {
		return time.Time{}, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return time.Time{}, err
	}

	migratedAt := s.clock()
	if err := store.MarkSubscriberMigrated(ctx, strings.TrimSpace(subscriberID), migratedAt); err != nil {
		return time.Time{}, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
	}).Info("event subscriber: recorded as migrated to Kafka consumption")

	return migratedAt, nil
}

// CompleteWebhookMigration performs the whole cutover for one subscriber: it forgets
// the recorded legacy endpoint and records that the subscriber has finished moving to
// Kafka.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank identifier, or the repository's error.
func (s *EventSubscriberService) CompleteWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	migratedAt := s.clock()

	migrated, err := store.CompleteSubscriberWebhookMigration(
		ctx, strings.TrimSpace(subscriberID), migratedAt)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(migrated.SubscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
		// The URL itself is NOT logged: it is a third-party address, and this is the operation
		// that exists to stop retaining it. Whether one was present is the fact worth recording.
		"legacy_webhook_forgotten": true,
	}).Info(
		"event subscriber: legacy webhook subscription forgotten and the migration recorded in " +
			"one write",
	)

	return migrated, nil
}

// PurgeMigratedWebhookURLs forgets the legacy endpoint of every subscriber that
// migrated before a cut-off.
func (s *EventSubscriberService) PurgeMigratedWebhookURLs(
	ctx context.Context,
	migratedBefore time.Time,
) (int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return 0, err
	}

	purged, err := store.PurgeMigratedSubscriberWebhookURLs(ctx, migratedBefore)
	if err != nil {
		return 0, err
	}

	logrus.WithFields(logrus.Fields{
		"purged":          purged,
		"migrated_before": migratedBefore.UTC().Format(time.RFC3339Nano),
	}).Info("event subscriber: legacy webhook URLs purged for migrated subscribers")

	return purged, nil
}

// CompleteLegacyWebhookMigration forgets a subscriber's recorded legacy endpoint AND
// records that it finished migrating, atomically.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant
//     so a caller can report it without re-reading.
//   - error: a typed validation error for a blank identifier, ErrSubscriberNotFound, or
//     the repository's error.
func (s *EventSubscriberService) CompleteLegacyWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	migratedAt := s.clock()

	migrated, err := store.CompleteSubscriberWebhookMigration(
		ctx, strings.TrimSpace(subscriberID), migratedAt,
	)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(migrated.SubscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
	}).Info(
		"event subscriber: legacy webhook subscription forgotten and the migration recorded in one " +
			"write",
	)

	return migrated, nil
}

// maxSubscriberKeyScopeLength bounds the recorded key-scoped constraint.
const maxSubscriberKeyScopeLength = 256

// subscriberHashResolutionPageSize is how many registry rows one page of a pseudonym
// resolution reads.
const subscriberHashResolutionPageSize = 100

// subscriberHashResolutionMaxPages bounds a pseudonym resolution.
const subscriberHashResolutionMaxPages = 100

// ErrSubscriberHashResolutionExhausted is returned when a pseudonym resolution reached
// its page ceiling without a match.
var ErrSubscriberHashResolutionExhausted = errors.New(
	"blnk: the subscriber pseudonym could not be resolved within the bounded number of registry " +
		"pages, so whether it names a subscriber is unknown",
)

// ResolveSubscriberByPseudonym finds the subscriber a subscriber_id_hash token names.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//   - pseudonym string: the token, as it appears in a log line, a metric label or an
//     alert.
//
// Returns:
//   - *model.EventSubscriber: the matching row.
//   - error: a typed validation error for a blank token, ErrSubscriberNotFound when the
//     registry holds no subscriber with that pseudonym,
//     ErrSubscriberHashResolutionExhausted when the ceiling was reached first, or the
//     repository's error.
func (s *EventSubscriberService) ResolveSubscriberByPseudonym(
	ctx context.Context,
	pseudonym string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	wanted := strings.ToLower(strings.TrimSpace(pseudonym))
	if wanted == "" {
		return nil, apierror.NewAPIError(apierror.ErrInvalidInput,
			"A subscriber pseudonym is required to resolve a subscriber by its hash", nil)
	}

	var resolved *model.EventSubscriber

	complete, err := walkSubscriberRegistry(ctx, store, "pseudonym_resolution",
		func(subscriber model.EventSubscriber) bool {
			if model.HashIdentifier(subscriber.SubscriberID) != wanted {
				return true
			}

			found := subscriber
			resolved = &found

			return false
		})
	if err != nil {
		return nil, err
	}

	if resolved != nil {
		return resolved, nil
	}

	// The registry was walked to its END, so the token is genuinely unknown rather than
	// merely unreached. This is the ONLY path that may answer "not found".
	if complete {
		return nil, apierror.NewAPIError(apierror.ErrSubscriberNotFound,
			"No subscriber in the registry has that pseudonym", nil)
	}

	return nil, ErrSubscriberHashResolutionExhausted
}

// ListSubscribersAwaitingRevocation returns every subscriber whose broker-side access
// Blnk began taking away and could not finish. It is the read behind GET
// /subscribers?revocation_pending=true.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//
// Returns:
//   - []model.EventSubscriber: the affected rows, oldest obligation first, so the
//     responder works the longest exposure first — the same order the alert fires on.
//   - error: ErrSubscriberHashResolutionExhausted when the walk could not cover the
//     registry, or the repository's error.
func (s *EventSubscriberService) ListSubscribersAwaitingRevocation(
	ctx context.Context,
) ([]model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	pending := make([]model.EventSubscriber, 0)

	complete, err := walkSubscriberRegistry(ctx, store, "revocation_pending_scan",
		func(subscriber model.EventSubscriber) bool {
			if subscriber.RevocationPendingAt != nil {
				pending = append(pending, subscriber)
			}

			return true
		})
	if err != nil {
		return nil, err
	}

	if !complete {
		return nil, ErrSubscriberHashResolutionExhausted
	}

	// Oldest obligation first. The alert fires on the oldest age, so this order puts the
	// row the alert is actually about at the top rather than leaving the responder to
	// compare timestamps.
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].RevocationPendingAt.Before(*pending[j].RevocationPendingAt)
	})

	return pending, nil
}

// walkSubscriberRegistry pages the whole registry, calling visit for each row.
func walkSubscriberRegistry(
	ctx context.Context,
	store eventSubscriberStore,
	operation string,
	visit func(model.EventSubscriber) bool,
) (bool, error) {
	var cursor *model.SubscriberCursor

	for page := 0; page < subscriberHashResolutionMaxPages; page++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}

		batch, listErr := store.ListEventSubscribers(ctx, model.SubscriberPageQuery{
			Limit:  subscriberHashResolutionPageSize,
			Cursor: cursor,
		})
		if listErr != nil {
			return false, listErr
		}

		for i := range batch.Subscribers {
			if !visit(batch.Subscribers[i]) {
				return true, nil
			}
		}

		// The repository answers whether anything follows, so the end of the registry is READ
		// rather than inferred from a short page — and the cursor is what makes the next page
		// resume exactly where this one stopped even if rows were registered or deregistered
		// in between.
		if !batch.HasMore || batch.NextCursor == nil {
			return true, nil
		}

		cursor = batch.NextCursor
	}

	logrus.WithFields(logrus.Fields{
		"operation":  operation,
		"pages_read": subscriberHashResolutionMaxPages,
		"page_size":  subscriberHashResolutionPageSize,
	}).Warn(
		"event subscriber: a registry walk reached its page ceiling, so its answer is UNKNOWN " +
			"rather than negative; the registry is larger than the walk's bound",
	)

	return false, nil
}

// ListEventSubscribersAwaitingRevocation returns every subscriber with an outstanding
// broker-side revocation. It is the read behind GET
// /subscribers?revocation_pending=true, and the first step of the
// SubscriberRevocationOutstanding runbook.
func (b *Blnk) ListEventSubscribersAwaitingRevocation(
	ctx context.Context,
) ([]model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ListSubscribersAwaitingRevocation(ctx)
}

// ResolveEventSubscriberByPseudonym finds the subscriber a subscriber_id_hash token names. It
// is the read behind GET /subscribers?subscriber_id_hash=<token>.
func (b *Blnk) ResolveEventSubscriberByPseudonym(
	ctx context.Context,
	pseudonym string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ResolveSubscriberByPseudonym(ctx, pseudonym)
}
