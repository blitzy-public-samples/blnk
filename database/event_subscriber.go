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

// event_subscriber.go is the repository implementation of the eventSubscriber contract
// declared in repository.go: persistence for blnk.event_subscribers, the registry of
// Kafka subscribers and the access boundary provisioned for each one.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
)

// eventSubscriberColumns is the column list every read of blnk.event_subscribers
// projects, in the exact order scanEventSubscriber consumes it. It is declared once so
// the single-row and listing paths cannot drift apart in either projection or scan
// order — a drift that would not fail to compile and would instead surface as silently
// transposed fields. THE SETTLEMENT MARKERS ARE PART OF THE ROW, and they were not.
const eventSubscriberColumns = `id, subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, ` +
	`partition_key_prefix, credential_reference, credential_issued_at, webhook_url, migrated_at, ` +
	`revocation_pending_at, revocation_failed_at, credential_orphaned_at, ` +
	`grant_reconcile_pending_at, credential_cleanup_pending_at, created_at, updated_at`

// Page-size bounds for the registry listing. The default keeps an unqualified
// request cheap; the maximum stops a caller turning a management endpoint into a
// full table scan. Both match the bounds the dead-letter listing uses, so the two
// management surfaces page alike.
const (
	defaultSubscriberPageSize = 50
	maxSubscriberPageSize     = 500
)

// subscriberNotFoundMessage is the single not-found message this file reports, so
// every path that can fail to locate a subscriber says the same thing.
const subscriberNotFoundMessage = "Subscriber not found"

// The two unique indexes a write can collide with. They are named here rather
// than inlined so the conflict classifier reads as the schema does, and so a
// rename in the migration has exactly one place to be reflected.
const (
	subscriberIDUniqueIndex     = "event_subscribers_subscriber_id_uidx"
	kafkaPrincipalUniqueIndex   = "event_subscribers_kafka_principal_uidx"
	uniqueViolationPostgresCode = "unique_violation"
)

// The retired key-scope CHECK constraint (added by sql/1781248920.sql, dropped by
// sql/1781248930.sql) and the driver's name for a CHECK failure.
const (
	keyScopeCheckConstraint = "event_subscribers_key_scope_chk"

	// subscriberFenceLostMarker is the phrase EVERY lost-fence conflict raised by this
	// repository must carry in its wrapped detail.
	subscriberFenceLostMarker  = "the provisioning claim was no longer held"
	checkViolationPostgresCode = "check_violation"
)

// eventSubscriberScanner is the minimum surface scanEventSubscriber needs,
// satisfied by both *sql.Row and *sql.Rows. It lets the single-row read and the
// listing share one decoder instead of maintaining two scan orders.
type eventSubscriberScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventSubscriber decodes one blnk.event_subscribers row into a
// model.EventSubscriber.
func scanEventSubscriber(s eventSubscriberScanner) (model.EventSubscriber, error) {
	var sub model.EventSubscriber
	var authorizedTopics pq.StringArray
	var partitionKeyPrefix, credentialReference, webhookURL sql.NullString
	var credentialIssuedAt, migratedAt, revocationPendingAt sql.NullTime
	var revocationFailedAt, credentialOrphanedAt sql.NullTime
	var grantReconcilePendingAt, credentialCleanupPendingAt sql.NullTime

	if err := s.Scan(
		&sub.ID,
		&sub.SubscriberID,
		&sub.Name,
		&sub.KafkaPrincipal,
		&sub.ConsumerGroupID,
		&authorizedTopics,
		&partitionKeyPrefix,
		&credentialReference,
		&credentialIssuedAt,
		&webhookURL,
		&migratedAt,
		&revocationPendingAt,
		&revocationFailedAt,
		&credentialOrphanedAt,
		&grantReconcilePendingAt,
		&credentialCleanupPendingAt,
		&sub.CreatedAt,
		&sub.UpdatedAt,
	); err != nil {
		return model.EventSubscriber{}, err
	}

	sub.AuthorizedTopics = []string(authorizedTopics)
	if sub.AuthorizedTopics == nil {
		// Normalising nil to empty keeps HasTopicAccess and every JSON response
		// consistent: a subscriber authorised for nothing serialises as [] rather
		// than null, so an API consumer never has to treat the two differently.
		sub.AuthorizedTopics = []string{}
	}

	if partitionKeyPrefix.Valid {
		value := partitionKeyPrefix.String
		sub.PartitionKeyPrefix = &value
	}
	if credentialReference.Valid {
		value := credentialReference.String
		sub.CredentialReference = &value
	}
	if webhookURL.Valid {
		value := webhookURL.String
		sub.WebhookURL = &value
	}
	if credentialIssuedAt.Valid {
		issuedAt := credentialIssuedAt.Time
		sub.CredentialIssuedAt = &issuedAt
	}
	if migratedAt.Valid {
		migrated := migratedAt.Time
		sub.MigratedAt = &migrated
	}
	if revocationFailedAt.Valid {
		failed := revocationFailedAt.Time
		sub.RevocationFailedAt = &failed
	}
	if credentialOrphanedAt.Valid {
		orphaned := credentialOrphanedAt.Time
		sub.CredentialOrphanedAt = &orphaned
	}
	if revocationPendingAt.Valid {
		pending := revocationPendingAt.Time
		sub.RevocationPendingAt = &pending
	}
	if grantReconcilePendingAt.Valid {
		pending := grantReconcilePendingAt.Time
		sub.GrantReconcilePendingAt = &pending
	}
	if credentialCleanupPendingAt.Valid {
		pending := credentialCleanupPendingAt.Time
		sub.CredentialCleanupPendingAt = &pending
	}

	return sub, nil
}

// normalizeAuthorizedTopics prepares the topic grant for writing.
func normalizeAuthorizedTopics(topics []string) pq.StringArray {
	if topics == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(topics)
}

// requireSubscriberID rejects a blank business key before any query is issued.
func requireSubscriberID(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber ID is required", nil)
	}
	return nil
}

// requireSubscriberFields validates the four NOT NULL business columns AND
// canonicalizes the three that make up the subscriber's Kafka identity.
func requireSubscriberFields(subscriber *model.EventSubscriber) error {
	if subscriber == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber is required", nil)
	}
	if err := requireSubscriberID(subscriber.SubscriberID); err != nil {
		return err
	}
	if strings.TrimSpace(subscriber.Name) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber name is required", nil)
	}

	canonicalID, err := model.CanonicalizeSubscriberIdentifier(subscriber.SubscriberID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a Kafka identity", err)
	}

	principal, err := model.CanonicalKafkaPrincipal(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a Kafka principal", err)
	}

	group, err := model.CanonicalConsumerGroupID(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a consumer group", err)
	}

	namespace, err := model.CanonicalConsumerGroupNamespace(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a consumer group namespace", err)
	}

	// ABSENT means DERIVE; PRESENT means it must match exactly.
	if subscriber.KafkaPrincipal == "" {
		subscriber.KafkaPrincipal = principal
	}

	if subscriber.KafkaPrincipal != principal {
		// NO DETAILS, and the rule stated in full in the message instead.
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The Kafka principal must be exactly the one derived from the subscriber ID — the "+
				"\"blnk-sub-\" namespace followed by the identifier — or omitted so that it is "+
				"derived", nil)
	}

	// An absent group is likewise derived, to the default leaf. Deriving the DEFAULT rather
	// than the namespace itself is deliberate: the bare namespace is not a usable group, and a
	// prefixed ACL over it would grant the namespace instead of a group inside it.
	if subscriber.ConsumerGroupID == "" {
		subscriber.ConsumerGroupID = group
	}

	// The recorded group may be any leaf inside the subscriber's own namespace — that is what
	// the prefixed ACL grant is for — but never a value outside it, which a prefixed grant
	// would turn into a reach into another subscriber's groups.
	if !model.IsInSubscriberGroupNamespace(subscriber.ConsumerGroupID, namespace) {
		// No details, for the reason the principal check above documents: the namespace and
		// the default leaf are both derived from the subscriber id.
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The consumer group must lie inside the subscriber's own namespace — the "+
				"\"blnk-sub-\" namespace, the identifier, then \".\" and any leaf — or be "+
				"omitted so that the default leaf is derived", nil)
	}

	if err := requireGrantableTopics(subscriber.AuthorizedTopics); err != nil {
		return err
	}

	if err := requireSafeWebhookURL(subscriber.WebhookURL); err != nil {
		return err
	}

	subscriber.SubscriberID = canonicalID
	subscriber.Name = strings.TrimSpace(subscriber.Name)

	return nil
}

// grantableTopicPrefixes is the set of topic names a subscriber may be authorised for,
// as bare category topics under the configured prefix.
func grantableTopicPrefixes() map[string]struct{} {
	grantable := make(map[string]struct{})
	for _, topic := range model.SubscriberAuthorizableTopics(
		configuredEventTopicPrefix(), subscriberInternalTopicAccessDeclared(),
	) {
		grantable[topic] = struct{}{}
	}

	return grantable
}

// subscriberInternalTopicAccessDeclared reports whether this deployment has
// acknowledged that a subscriber may hold the internal category topic,
// `<prefix>.system`.
func subscriberInternalTopicAccessDeclared() bool {
	cnf, err := config.Fetch()
	if err != nil || cnf == nil {
		return false
	}

	return cnf.Kafka.SubscriberInternalTopicAccess
}

// requireGrantableTopics refuses an authorised-topic list containing anything a
// subscriber may not be granted.
func requireGrantableTopics(topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	grantable := grantableTopicPrefixes()

	allowed := make([]string, 0, len(grantable))
	for topic := range grantable {
		allowed = append(allowed, topic)
	}
	sort.Strings(allowed)

	for _, topic := range topics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}

		if _, ok := grantable[trimmed]; !ok {
			return apierror.NewAPIError(apierror.ErrInvalidInput,
				"Authorized topics must be Blnk-owned category topics",
				fmt.Errorf("topic %q is not grantable; the grantable topics are %s",
					trimmed, strings.Join(allowed, ", ")))
		}
	}

	return nil
}

// requireSafeWebhookURL applies the destination policy to the legacy dual-run webhook
// URL.
func requireSafeWebhookURL(webhookURL *string) error {
	if webhookURL == nil {
		return nil
	}

	// THE ONE POLICY, in model.ValidateWebhookURL. This function carries no copy of its
	// own — and no destination classifier — because a second copy beside the request DTO's
	// would give one column two rules: a service, CLI or migration caller reaching this
	// repository directly would be judged by a different standard from an HTTP caller, and
	// the same rejected host would produce two different reason phrases. What stays
	// here is the ERROR TYPE, because this layer answers with a typed apierror while the
	// DTO answers with a plain validation error.
	message, reason := model.ValidateWebhookURL(*webhookURL)
	if message == "" {
		return nil
	}

	return apierror.NewAPIError(apierror.ErrInvalidInput, message, errors.New(reason))
}

// classifySubscriberWriteError turns a driver error from an insert or update into the
// typed error the API layer should answer with.
func classifySubscriberWriteError(err error, internalMessage string) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code.Name() == uniqueViolationPostgresCode {
		switch pqErr.Constraint {
		case subscriberIDUniqueIndex:
			return loggedDatabaseError(apierror.ErrConflict, "A subscriber with this ID already exists", "classify_subscriber_write_error", err)
		case kafkaPrincipalUniqueIndex:
			return loggedDatabaseError(apierror.ErrConflict, "Another subscriber already uses this Kafka principal", "classify_subscriber_write_error", err)
		default:
			return loggedDatabaseError(apierror.ErrConflict, "Subscriber already exists", "classify_subscriber_write_error", err)
		}
	}

	// THE KEY-SCOPE CHECK MEANS THE SCHEMA IS BEHIND THE CODE, and that is the only thing
	// it can mean now. event_subscribers_key_scope_chk forbade "partition_key_prefix
	// recorded AND credential_reference recorded"; sql/1781248930.sql drops it, because a
	// subscriber's boundary includes its key prefix and refusing the combination denied
	// the subscriber a credential rather than narrowing what it could read. The service
	// permits the combination, so on a migrated database this branch is unreachable.
	if errors.As(err, &pqErr) &&
		pqErr.Code.Name() == checkViolationPostgresCode &&
		pqErr.Constraint == keyScopeCheckConstraint {
		return loggedDatabaseError(
			apierror.ErrInternalServer,
			"This database still carries the event_subscribers_key_scope_chk constraint, which "+
				"forbids recording a partition key prefix on a subscriber that holds a Kafka "+
				"credential. Apply the pending migrations (sql/1781248930.sql removes it) and "+
				"repeat the request",
			"classify_subscriber_write_error",
			err,
		)
	}

	return loggedDatabaseError(apierror.ErrInternalServer, internalMessage, "classify_subscriber_write_error", err)
}

// assertSubscriberRowAffected turns a zero-row write into a typed not-found error.
func assertSubscriberRowAffected(result sql.Result, internalMessage string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return loggedDatabaseError(apierror.ErrInternalServer, internalMessage, "assert_subscriber_row_affected", err)
	}
	if affected == 0 {
		return apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
	}
	return nil
}

// requireFenceToken rejects a blank provisioning-fence token before a statement runs.
func requireFenceToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required to modify this subscriber",
			errors.New("event subscriber: a fenced write was attempted with no provisioning claim token"))
	}

	return nil
}

// subscriberFenceMiss names WHY a fenced write matched no row.
type subscriberFenceMiss int

const (
	// subscriberFenceMissOther: the row exists, the claim IS the caller's, and no
	// revocation is pending — so one of the statement's own additional conditions stopped
	// holding. For a statement whose only predicates are the key and the claim this is
	// unreachable; for one that adds a compare-and-set it is the ordinary superseded case.
	subscriberFenceMissOther subscriberFenceMiss = iota

	// subscriberFenceMissRowGone: no row carries the subscriber ID any more.
	subscriberFenceMissRowGone

	// subscriberFenceMissClaimLost: the row exists but the provisioning claim is not the
	// caller's. This is the outcome the token predicate exists to produce.
	subscriberFenceMissClaimLost

	// subscriberFenceMissRevocationPending: the row carries the revocation tombstone, so its
	// authorization is deliberately frozen.
	subscriberFenceMissRevocationPending
)

// describeFencedWriteMiss reads the two facts that decide why a fenced write missed.
func (d Datasource) describeFencedWriteMiss(
	ctx context.Context,
	subscriberID, token, internalMessage string,
) (subscriberFenceMiss, error) {
	var (
		holdsClaim        bool
		revocationPending bool
	)

	err := d.Conn.QueryRowContext(ctx, `
		SELECT provisioning_token IS NOT DISTINCT FROM $2,
		       revocation_pending_at IS NOT NULL
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, strings.TrimSpace(subscriberID), strings.TrimSpace(token)).Scan(&holdsClaim, &revocationPending)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return subscriberFenceMissRowGone, nil
		}

		return subscriberFenceMissOther, loggedDatabaseError(apierror.ErrInternalServer,
			internalMessage, "describe_fenced_write_miss", err)
	}

	switch {
	case !holdsClaim:
		return subscriberFenceMissClaimLost, nil
	case revocationPending:
		return subscriberFenceMissRevocationPending, nil
	default:
		return subscriberFenceMissOther, nil
	}
}

// fencedWriteMissError turns a miss reason into the typed error its remedy calls for.
func fencedWriteMissError(subscriberID string, miss subscriberFenceMiss) error {
	id := strings.TrimSpace(subscriberID)

	switch miss {
	case subscriberFenceMissRowGone:
		return apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)

	case subscriberFenceMissClaimLost:
		// The detail carries subscriberFenceLostMarker, and it must: this is the general
		// fenced-write miss, so it is the path most credential writes take when their claim
		// lapses, and without the marker the service treats it as an ordinary conflict and
		// skips the broker-side compensation.
		return apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller, so the change was not applied",
			fmt.Errorf("event subscriber: a fenced write for subscriber %q matched no row because "+
				subscriberFenceLostMarker+"; the claim expired or was taken over while the "+
				"operation was in flight; nothing was written, and the operation must be retried "+
				"under a fresh claim", id))

	case subscriberFenceMissRevocationPending:
		// THE SAME TYPED CODE THE SERVICE-LEVEL GUARD RAISES. This is that guard's condition
		// arriving a moment later — the tombstone landed between the read and the write — and a
		// caller that branched on SUBSCRIBER_DEPROVISIONING would otherwise see a bare
		// GEN_CONFLICT for the identical state purely because of WHEN it was noticed. The
		// remedy is the same either way: complete or reverse the deregistration, then retry.
		return apierror.NewAPIError(apierror.ErrSubscriberDeprovisioning,
			"This subscriber is being deregistered, so its access model can no longer be changed",
			fmt.Errorf("event subscriber: subscriber %q carries a revocation tombstone, so its "+
				"authorization is frozen; complete or reverse its deregistration first", id))

	default:
		return apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber changed while this operation was in flight, so the change was not applied",
			fmt.Errorf("event subscriber: a fenced write for subscriber %q matched no row while the claim "+
				"was still held and no revocation was pending, so one of the statement's other "+
				"conditions stopped holding", id))
	}
}

// classifyFencedWriteMiss explains why a fenced write matched no row, as a typed error.
func (d Datasource) classifyFencedWriteMiss(
	ctx context.Context,
	subscriberID, token, internalMessage string,
) error {
	miss, err := d.describeFencedWriteMiss(ctx, subscriberID, token, internalMessage)
	if err != nil {
		return err
	}

	return fencedWriteMissError(subscriberID, miss)
}

// requireFencedWriteLanded turns "the statement matched no row" into the typed error
// that says WHY, for a fenced write whose only predicates are the business key and the
// provisioning claim.
func (d Datasource) requireFencedWriteLanded(
	ctx context.Context,
	result sql.Result,
	subscriberID, fenceToken, operation string,
) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to confirm the subscriber settlement write", operation, err)
	}

	if affected == 0 {
		return d.classifyFencedWriteMiss(ctx, subscriberID, fenceToken,
			"Failed to confirm the subscriber settlement write")
	}

	return nil
}

// nullableTime renders a time as a driver value that is SQL NULL when the time is
// unset.
func nullableTime(value time.Time) interface{} {
	if value.IsZero() {
		return nil
	}

	return value
}

//   * requireProvisioningToken was a SECOND NAME for requireFenceToken.
