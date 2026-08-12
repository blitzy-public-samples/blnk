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
//
// There is therefore no prior art to be consistent with INSIDE this domain, which is
// exactly why consistency with the conventions of THIS FOLDER matters more than usual
// here: value receivers on Datasource, apierror-wrapped failures, lib/pq array handling
// and OpenTelemetry spans on the transaction.database tracer are what make this file
// recognisable as Blnk code rather than as an import from somewhere else.
//
// This is the defining constraint on this file, and it is structural rather than
// aspirational. Read the signatures below: not one of them takes a password, and not
// one of them returns a value anything can be authenticated with.
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
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
//
// The constraint forbade a row recording a partition key prefix while it also held a
// credential reference. That has been reversed deliberately: a recorded prefix is a
// boundary the CONSUMER keeps, so forbidding the combination denied the subscriber a
// mandatory capability instead of narrowing what it could read.
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
//
// pq.StringArray is used rather than a hand-built '{a,b}' literal so element quoting
// and escaping are the driver's problem, which is what keeps a topic name containing a
// comma or a brace from corrupting the value.
func normalizeAuthorizedTopics(topics []string) pq.StringArray {
	if topics == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(topics)
}

// requireSubscriberID rejects a blank business key before any query is issued.
//
// Failing in memory rather than at the database matters twice over: it keeps the
// 5-second provisioning budget for a request that cannot succeed anyway, and it answers
// with "required" instead of the misleading "Subscriber not found" that an empty WHERE
// match would otherwise produce. Whitespace is trimmed for the test only — the caller's
// value is stored verbatim, because silently rewriting an identifier would be a worse
// surprise than rejecting it.
func requireSubscriberID(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber ID is required", nil)
	}
	return nil
}

// requireSubscriberFields validates the four NOT NULL business columns AND
// canonicalizes the three that make up the subscriber's Kafka identity.
//
// NOT NULL alone does not stop an empty string, and each of these being blank is a real
// hazard rather than a cosmetic one. A blank subscriber_id yields a row no endpoint can
// address.
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
//
// blnk.SubscriberInternalTopicAccessDeclared is the root package's reader of the same
// field for the same purpose; the duplication is forced by the import direction and is
// one field read, not a second policy.
//
// A configuration that cannot be read answers FALSE, the fail-closed direction.
//
// Returns:
//   - bool: true only when the deployment has declared the acknowledgement.
func subscriberInternalTopicAccessDeclared() bool {
	cnf, err := config.Fetch()
	if err != nil || cnf == nil {
		return false
	}

	return cnf.Kafka.SubscriberInternalTopicAccess
}

// requireGrantableTopics refuses an authorised-topic list containing anything a
// subscriber may not be granted.
//
// The list is what the ACL bindings are built from, so whatever is stored here is what
// the credential can read. Three classes must be impossible and the check is here as
// well as in the provisioning path because the row outlives any single request: a topic
// accepted now is granted at the next issuance, whichever code path performs it.
//
//   - "*" and other WILDCARDS, because Kafka treats the resource name "*" as matching
//     any resource, so one such entry turns a per-topic grant into a cluster-wide one.
//   - FOREIGN topics, because a grant over a topic Blnk does not own is a grant into
//     somebody else's data on a broker Blnk may share.
//   - DEAD-LETTER topics, because every DLT carries other subscribers' failed events
//     together with Blnk's own failure metadata, so it has no subscriber audience.
//   - THE INTERNAL CATEGORY TOPIC "<prefix>.system", UNLESS the deployment has declared
//     KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. system.error's frozen payload renders
//     Blnk's error text verbatim and the category is the catalogue's catch-all, so it
//     is an operator surface: the default set is the three tenant categories, and the
//     privileged name is admitted only where the deployment has acknowledged the
//     disclosure. model.SubscriberPrivilegedEventCategories owns that decision.
//
// An EMPTY list is accepted: a subscriber authorised for nothing is the fail-closed
// default of a fresh registration, and refusing it would make registration and
// authorisation one inseparable step.
//
// Parameters:
//   - topics []string: the authorised topics as supplied.
//
// Returns:
//   - error: a typed invalid-input error naming the first offending topic, or nil.
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
//
// The URL is recorded so a subscriber already receiving HTTP pushes has somewhere to be
// migrated FROM. Nothing sends to it yet — but a stored URL is a future sink, and the
// moment any code does send to it, whatever is in this column becomes a request Blnk
// makes from inside its own network.
//
// Two rules, and each closes a distinct route:
//
//   - HTTPS ONLY. A cleartext push carries ledger and identity data — names, email
//     addresses, phone numbers, addresses, dates of birth — over a network Blnk does
//     not control, and it is trivially redirectable.
//   - NO INTERNAL DESTINATION. Loopback, link-local (including the 169.254.169.254
//     cloud metadata address), private ranges and unqualified hostnames are refused.
//
// Parameters:
//   - webhookURL *string: the URL as supplied. Nil and empty are accepted — most
//     subscribers never had a webhook.
//
// Returns:
//   - error: a typed invalid-input error naming the rule broken, or nil.
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
	//
	// An empty or all-whitespace value passes: it means "clear the record", which is the
	// same three-way nil/empty/value mapping the service layer applies and the reason the
	// column is nullable.
	message, reason := model.ValidateWebhookURL(*webhookURL)
	if message == "" {
		return nil
	}

	return apierror.NewAPIError(apierror.ErrInvalidInput, message, errors.New(reason))
}

// classifySubscriberWriteError turns a driver error from an insert or update into the
// typed error the API layer should answer with.
//
// errors.As is used rather than a bare type assertion so the classification also holds
// if the driver error arrives wrapped, which a bare assertion would silently misfile as
// an internal server error.
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
//
// A RowsAffected failure is reported as an internal error rather than swallowed,
// following DeleteLineageMapping: if the count cannot be read, whether the row existed
// is genuinely unknown, and claiming success is the one answer that is certainly wrong.
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
//
// The fence is LEASED. An operation that stalls past its lease — a slow broker round
// trip, a paused process, a long garbage-collection pause — no longer owns the
// subscriber, and another operation may legitimately have claimed it.
//
// Parameters:
//   - token string: the token the caller's claim was issued under.
//
// Returns:
//   - error: a typed invalid-input error when the token is blank.
func requireFenceToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required to modify this subscriber",
			errors.New("event subscriber: a fenced write was attempted with no provisioning claim token"))
	}

	return nil
}

// subscriberFenceMiss names WHY a fenced write matched no row.
//
// It exists because the four reasons need four different answers and one of them is not
// an error at all from the calling statement's point of view. A write that carries its
// own extra predicate — the credential CAS is the one that does — has to be able to
// tell "I no longer own this subscriber" from "I still own it and my other condition
// stopped holding", because only the first means the operation must be abandoned.
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
//
// The two facts are read in ONE statement rather than through GetEventSubscriberByID,
// because the provisioning token is deliberately NOT a field of the read model —
// exposing it would put a concurrency-control secret on every API response — so it
// cannot be inspected from a row struct.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key.
//   - token string: the token the caller presented.
//   - internalMessage string: the message a driver failure is reported under.
//
// Returns:
//   - subscriberFenceMiss: the reason, meaningful only when the error is nil.
//   - error: non-nil only when the read itself failed, in which case it is already
//     logged.
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
//
// Parameters:
//   - subscriberID string: the business key, named in the internal detail only.
//   - miss subscriberFenceMiss: the reason.
//
// Returns:
//   - error: always non-nil; a typed not-found or a typed conflict naming the reason.
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
		return apierror.NewAPIError(apierror.ErrConflict,
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
//
// It is the whole answer for a statement whose only predicates are the business key,
// the provisioning claim and the revocation tombstone. A statement carrying an
// ADDITIONAL predicate calls describeFencedWriteMiss instead, so it can recognise
// subscriberFenceMissOther as its own condition failing rather than as a lost fence.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key.
//   - token string: the token the caller presented.
//   - internalMessage string: the message a driver failure is reported under.
//
// Returns:
//   - error: always non-nil.
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
//
// Parameters:
//   - ctx context.Context: cancels the follow-up classification read.
//   - result sql.Result: the executed statement's result.
//   - subscriberID string: the business key the statement addressed.
//   - fenceToken string: the token the caller presented.
//   - operation string: the operation name a driver failure is logged under.
//
// Returns:
//   - error: nil when at least one row was written; otherwise the classified miss, or
//     an internal error when the driver could not report the row count.
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
//
// Parameters:
//   - value time.Time: the instant, or the zero value for none.
//
// Returns:
//   - interface{}: the time, or nil for SQL NULL.
func nullableTime(value time.Time) interface{} {
	if value.IsZero() {
		return nil
	}

	return value
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
//
// The row is read back through RETURNING rather than reconstructed in Go, so the caller
// receives the database's own surrogate key and bookkeeping timestamps instead of a
// struct that merely resembles what was stored.
func (d Datasource) CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CreateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		failDatabaseSpan(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriber.SubscriberID))

	// Stamped in Go and bound as parameters, as the sibling repositories do, so the
	// values written are the ones the caller can see in the returned row.
	now := time.Now()

	row := d.Conn.QueryRowContext(ctx, `
		INSERT INTO blnk.event_subscribers
			(subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics,
			 partition_key_prefix, webhook_url, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING `+eventSubscriberColumns,
		subscriber.SubscriberID,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		normalizeAuthorizedTopics(subscriber.AuthorizedTopics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		now,
		now,
	)

	stored, err := scanEventSubscriber(row)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, classifySubscriberWriteError(err, "Failed to create event subscriber")
	}

	span.AddEvent("Event subscriber created", trace.WithAttributes(
		attribute.String("subscriber.id", stored.SubscriberID),
		attribute.String("subscriber.principal", stored.KafkaPrincipal),
		attribute.String("subscriber.consumer_group", stored.ConsumerGroupID),
		attribute.Int("subscriber.authorized_topic_count", len(stored.AuthorizedTopics)),
	))
	return &stored, nil
}

// GetEventSubscriberByID retrieves a subscriber by its business subscriber_id,
// resolving through the unique index the credential endpoint's 5-second budget depends
// on.
func (d Datasource) GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventSubscriberByID")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)

	subscriber, err := scanEventSubscriber(row)
	if err != nil {
		failDatabaseSpan(span, err)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, loggedDatabaseError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, "get_event_subscriber_by_id", err)
		}
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to retrieve event subscriber", "get_event_subscriber_by_id", err)
	}

	span.AddEvent("Event subscriber retrieved", trace.WithAttributes(
		attribute.String("subscriber.id", subscriber.SubscriberID),
		attribute.String("subscriber.principal", subscriber.KafkaPrincipal),
		attribute.Int("subscriber.authorized_topic_count", len(subscriber.AuthorizedTopics)),
	))
	return &subscriber, nil
}

// ListEventSubscribers returns one page of the registry, NEWEST FIRST, resuming from
// the caller's cursor.
//
// Two callers page this registry and both are better served by a position than by a
// depth. The management API gets a page whose cost does not depend on how deep it is
// and does not silently repeat or skip rows when a subscriber is registered
// mid-pagination.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.SubscriberPageQuery: the page size and the position to resume from.
//
// Returns:
//   - model.SubscriberPage: the page and the cursor for the next one, which is nil when
//     this page is the last.
const listEventSubscribersQuery = `
		SELECT ` + eventSubscriberColumns + `
		FROM blnk.event_subscribers
		WHERE $1::timestamptz IS NULL OR (created_at, id) < ($1::timestamptz, $2::bigint)
		ORDER BY created_at DESC, id DESC
		LIMIT $3
	`

// - error: a logged internal error.
func (d Datasource) ListEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListEventSubscribers")
	defer span.End()

	return listEventSubscribers(ctx, d.Conn, span, query)
}

// listEventSubscribers is the page read, parameterised by the connection it runs on, so
// that the standalone read and the snapshot-consistent read that pairs the page with
// its total run the identical statement over identical bindings. See
// ListAndCountEventSubscribers.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - conn subscriberQueryer: *sql.DB for a standalone read, *sql.Tx for the paired
//     read.
//   - span trace.Span: the caller's span, annotated here.
//   - query model.SubscriberPageQuery: the page.
//
// Returns:
//   - model.SubscriberPage: as ListEventSubscribers.
//   - error: as ListEventSubscribers.
func listEventSubscribers(
	ctx context.Context,
	conn subscriberQueryer,
	span trace.Span,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = defaultSubscriberPageSize
	}
	if limit > maxSubscriberPageSize {
		limit = maxSubscriberPageSize
	}

	var cursorInstant interface{}
	var cursorID int64
	if query.Cursor != nil {
		cursorInstant = query.Cursor.CreatedAt.UTC()
		cursorID = query.Cursor.ID
	}

	span.SetAttributes(
		attribute.Int("subscriber.page_limit", limit),
		attribute.Bool("subscriber.cursor_present", query.Cursor != nil),
	)

	// limit+1: the extra row establishes that another page exists and is discarded, which
	// answers "is there more" without counting the registry.
	rows, err := conn.QueryContext(ctx, listEventSubscribersQuery, cursorInstant, cursorID, limit+1)
	if err != nil {
		failDatabaseSpan(span, err)
		return model.SubscriberPage{}, loggedDatabaseError(apierror.ErrInternalServer, "Failed to list event subscribers", "list_event_subscribers", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Logged rather than returned. The rows have already been read by the
			// time this runs, so surfacing a close failure would discard a correct
			// result the caller needs in favour of a condition it cannot act on.
			withLoggableCause(nil, closeErr).Error("failed to close event subscriber rows")
		}
	}()

	// Non-nil so an empty page serialises as [] rather than null, and pre-sized to
	// the bounded page so a full page does not repeatedly regrow the slice.
	page := model.SubscriberPage{Subscribers: make([]model.EventSubscriber, 0, limit)}
	for rows.Next() {
		subscriber, scanErr := scanEventSubscriber(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return model.SubscriberPage{}, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event subscriber", "list_event_subscribers", scanErr)
		}

		if len(page.Subscribers) == limit {
			// The probe row: read to learn that a further page exists, never returned.
			page.HasMore = true

			break
		}

		page.Subscribers = append(page.Subscribers, subscriber)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return model.SubscriberPage{}, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event subscribers", "list_event_subscribers", err)
	}

	if page.HasMore && len(page.Subscribers) > 0 {
		last := page.Subscribers[len(page.Subscribers)-1]
		page.NextCursor = &model.SubscriberCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	span.AddEvent("Event subscribers listed", trace.WithAttributes(
		attribute.Int("subscriber.count", len(page.Subscribers)),
		attribute.Bool("subscriber.has_more", page.HasMore),
	))
	return page, nil
}

// CountEventSubscribers reports how many rows the registry holds.
//
// Returning the length of the page instead would be worse than refusing: a paging
// client that treats the total as the size of the set would read a full page of 20 as
// "20 subscribers exist" and stop, or loop forever comparing a page length against
// itself.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//
// Returns:
//   - int64: the number of registered subscribers, zero when none are.
//   - error: a wrapped internal error when the query fails.
func (d Datasource) CountEventSubscribers(ctx context.Context) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountEventSubscribers")
	defer span.End()

	return countEventSubscribers(ctx, d.Conn, span)
}

// countEventSubscribers is the count, parameterised by the connection it runs on, for
// the same reason listEventSubscribers is. See ListAndCountEventSubscribers.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - conn subscriberQueryer: *sql.DB for a standalone count, *sql.Tx for the paired
//     read.
//   - span trace.Span: the caller's span, annotated here.
//
// Returns:
//   - int64: as CountEventSubscribers.
//   - error: as CountEventSubscribers.
func countEventSubscribers(
	ctx context.Context,
	conn subscriberQueryer,
	span trace.Span,
) (int64, error) {
	var total int64
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_subscribers
	`).Scan(&total); err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(
			apierror.ErrInternalServer,
			"Failed to count event subscribers",
			"count_event_subscribers",
			err,
		)
	}

	span.SetAttributes(attribute.Int64("subscriber.total", total))
	return total, nil
}

// subscriberQueryer is the minimum read surface these two statements need, satisfied by
// both *sql.DB and *sql.Tx.
type subscriberQueryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// ListAndCountEventSubscribers returns one page of the registry AND how many rows it
// holds, both read from a single snapshot.
//
// A subscriber registered or deregistered between them is counted by one and absent
// from the other, so the total describes a registry the page is not a slice of — and
// the response asserted that they described "the same set by construction", which two
// snapshots cannot make true however identical their predicates are.
//
// Parameters:
//   - ctx context.Context: cancels the transaction.
//   - query model.SubscriberPageQuery: the page.
//
// Returns:
//   - model.SubscriberPage: the page, as ListEventSubscribers.
//   - int64: how many subscribers exist in the same snapshot.
//   - error: the repository's typed error.
func (d Datasource) ListAndCountEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListAndCountEventSubscribers")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		failDatabaseSpan(span, err)

		return model.SubscriberPage{}, 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list event subscribers", "list_and_count_event_subscribers", err)
	}

	// Rolled back unconditionally, never committed: nothing was written, and a rollback releases
	// the snapshot on every path. The result is already in hand, so a rollback failure is logged
	// rather than returned.
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			withLoggableCause(nil, rollbackErr).Error(
				"failed to roll back the subscriber registry snapshot")
		}
	}()

	page, err := listEventSubscribers(ctx, tx, span, query)
	if err != nil {
		return model.SubscriberPage{}, 0, err
	}

	total, err := countEventSubscribers(ctx, tx, span)
	if err != nil {
		return model.SubscriberPage{}, 0, err
	}

	return page, total, nil
}

// UpdateEventSubscriber updates a subscriber's access model and its legacy webhook URL,
// CONDITIONAL on the caller still holding the provisioning fence and on the row not
// being tombstoned for deregistration.
//
// Only the mutable columns are written. subscriber_id locates the row rather than being
// changed — it is the business key an issued credential and a live consumer are both
// addressed by, so reassigning it here would silently orphan them.
//
// THE CREDENTIAL COLUMNS ARE DELIBERATELY NOT TOUCHED. They belong exclusively to
// RecordSubscriberCredential, which means an ordinary registry edit can neither blank
// out nor forge an issuance record, and there is exactly one place to look when
// auditing when a credential was minted.
//
// Changing kafka_principal or authorized_topics here changes only the RECORD of the
// access boundary. The broker is a separate system of record and is not reconciled by
// this call: the service layer must re-provision the ACLs, or the registry and the
// broker will disagree about what the subscriber may read.
//
// Matching on subscriber_id alone would leave two fail-OPEN paths, and each additional
// predicate closes one:
//
//   - provisioning_token. The fence is leased, so an operation that stalled past its
//     lease no longer owns the subscriber.
//   - revocation_pending_at IS NULL. A row carrying the tombstone is being
//     deregistered: its principal is on its way out and its ACLs are being removed.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriber *model.EventSubscriber: the row as it should be written.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriber *model.EventSubscriber: the row as it should be written.
//   - fenceToken string: the provisioning claim the caller holds. Required.
//
// Returns:
//   - *model.EventSubscriber: the row as stored after the write, never nil when err is
//     nil.
//   - error: a typed conflict when the claim is lost or the row is tombstoned, a typed
//     not-found when the row is gone, a typed conflict for a principal collision, or a
//     logged internal error.
func (d Datasource) UpdateEventSubscriber(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "UpdateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		failDatabaseSpan(span, err)
		return nil, err
	}

	if err := requireFenceToken(fenceToken); err != nil {
		failDatabaseSpan(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriber.SubscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_subscribers
		SET name = $1,
			kafka_principal = $2,
			consumer_group_id = $3,
			authorized_topics = $4,
			partition_key_prefix = $5,
			webhook_url = $6,
			updated_at = $7
		WHERE subscriber_id = $8
		  AND provisioning_token = $9
		  AND revocation_pending_at IS NULL
		RETURNING `+eventSubscriberColumns,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		normalizeAuthorizedTopics(subscriber.AuthorizedTopics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		time.Now(),
		subscriber.SubscriberID,
		strings.TrimSpace(fenceToken),
	)

	updated, err := scanEventSubscriber(row)
	if err != nil {
		failDatabaseSpan(span, err)

		// NO ROWS IS THE FENCED-WRITE MISS, and it carries the same condition an affected-row
		// count would report. Either predicate failed — the claim expired, or the row was
		// tombstoned — or the row is gone, and those call for opposite responses, so the
		// classifier decides rather than this statement reporting a flat not-found.
		if errors.Is(err, sql.ErrNoRows) {
			miss := d.classifyFencedWriteMiss(ctx, subscriber.SubscriberID, fenceToken,
				"Failed to update event subscriber")
			failDatabaseSpan(span, miss)

			return nil, miss
		}

		// Reachable through the principal index: moving a principal onto one another
		// subscriber already holds is a conflict, not a server fault.
		return nil, classifySubscriberWriteError(err, "Failed to update event subscriber")
	}

	span.AddEvent("Event subscriber updated", trace.WithAttributes(
		attribute.String("subscriber.id", updated.SubscriberID),
		attribute.String("subscriber.principal", updated.KafkaPrincipal),
		attribute.String("subscriber.consumer_group", updated.ConsumerGroupID),
		attribute.Int("subscriber.authorized_topic_count", len(updated.AuthorizedTopics)),
	))

	return &updated, nil
}

// DeleteEventSubscriber removes a subscriber from the registry.
//
// This is the REGISTRY HALF ONLY. Deleting the row does not revoke the subscriber's
// broker-side SCRAM credential or its ACL bindings, because the broker is a separate
// system of record: a caller that deletes here without deprovisioning there leaves a
// principal that can still authenticate and still read.
func (d Datasource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "DeleteEventSubscriber")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		failDatabaseSpan(span, err)
		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)
	if err != nil {
		failDatabaseSpan(span, err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to delete event subscriber", "delete_event_subscriber", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to delete event subscriber"); err != nil {
		failDatabaseSpan(span, err)
		return err
	}

	span.AddEvent("Event subscriber deleted", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))
	return nil
}

// RecordSubscriberCredential persists the outcome of a credential issuance: a
// non-reversible reference to the credential and the instant it was issued, which
// together are the complete stored record of an issuance.
//
// This method neither hashes nor derives anything, and it must stay that way. The
// caller that owns issuance generates the SASL password, returns it to the API caller
// exactly once, derives the reference and hands only the reference here.
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
// So an operator correcting a migrated subscriber's endpoint — a legitimate action,
// since the record exists to be corrected — produced precisely that self-contradicting
// row, with no error and nothing to notice.
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
// It is deliberately NOT CompleteSubscriberWebhookMigration. Deleting an address is not
// evidence that a subscriber moved to Kafka, and conflating the two would let a purge
// report migrations that never happened.
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
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what migration-progress
// reporting counts during the 30-day dual-delivery window and what the index over the
// column exists to serve. Together with webhook_url this is DUAL-RUN-ONLY state: both
// columns and that index are dropped at sunset, and this method goes with them.
//
// Re-stamping simply moves the timestamp forward. It is idempotent in effect — a
// subscriber that is migrated stays migrated — so a retried migration step needs no
// guard at the call site.
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
// The ordering was chosen so that a failure in between could not OVER-claim progress,
// and that much was true. What it did instead was strand the row permanently:
//
//   - webhook_url is NULL, so the subscriber no longer appears in "still receiving HTTP
//     pushes" — there is no endpoint left to migrate it FROM.
//   - migrated_at is NULL, so it is not counted in "migrated" either.
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
	//
	// This statement is idempotent by design — it is how a subscriber's dual-run artefact
	// is removed, and DELETE /subscribers/{id}/webhook-subscription can legitimately be
	// called again on a row already migrated — but an unconditional assignment made each
	// repeat rewrite migrated_at to the current instant. The column is the record of WHEN
	// a subscriber left HTTP delivery: it is what migration-progress reporting reads and
	// what an operator uses to decide whether the sunset window has been served, so a
	// second call quietly moved the migration forward in time and made a long-migrated
	// subscriber look like it moved today. webhook_url is already NULL by then, so nothing
	// else in the row distinguishes the repeat.
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
// It is written BEFORE the broker work an authorization change performs, not after a
// failure, and that ordering is the entire point. A marker written after a failure
// cannot be written by the failure that matters most — the process disappearing
// mid-change — because there is nothing left to write it.
//
// The write is idempotent: re-marking a row that already owes a reconciliation leaves
// the original instant, so the age of the obligation reflects when the divergence began
// rather than when it was last noticed.
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
// It is called only once the broker and the row are known to agree, so it also resets
// the settlement counters: an obligation raised again later starts its own history
// rather than inheriting the attempts of one that was successfully discharged.
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
// Like the grant marker it keeps the FIRST instant, so the obligation's age is the age
// of the divergence.
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
// A settlement pass finds an obligation, then takes the subscriber's provisioning
// claim, and time passes in between. Within it a successful re-issuance can DISCHARGE
// the credential-cleanup obligation, because provisioning upserts the principal's SCRAM
// credential and so replaces the orphan the obligation was about.
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
// It is UNFENCED and deliberately so: the settlement worker owns no subscriber, and a
// scan that required a claim could never find the obligations left behind by an owner
// that died holding one. The worker takes its own claim before it acts on a row; this
// call only decides where to look.
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
// It is UNFENCED, for the same reason the scan is: the worker records the attempt
// whether or not it could take the row's claim, and an attempt it could not record
// would be an attempt that never paced the next one — turning the retry interval into a
// busy loop against a broker that is already failing.
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

// ClearSubscriberCredential erases a subscriber's credential record, returning it to
// the "registered, not yet provisioned" state.
//
// It is IDEMPOTENT with respect to the credential columns: clearing a subscriber that
// holds no credential is a successful no-change, because the desired end state has been
// reached. A MISSING SUBSCRIBER is still an error, for the same reason every other
// write here reports one — silently succeeding would let a caller believe it had
// cleaned up a row that does not exist.
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

// TakeEventSubscriber removes a subscriber and RETURNS the row it removed.
//
// DeleteEventSubscriber tells a caller only whether a row went away, which is not
// enough to deprovision: revoking at the broker needs the principal and the authorised
// topics, and those are only in the row that has just been deleted. A caller therefore
// had to read, then delete, then revoke — and between the read and the delete the row
// could change, so it could revoke a boundary that was no longer the one in force.
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

// PurgeMigratedSubscriberWebhookURLs erases the legacy webhook URL of every subscriber
// that completed its migration before a cut-off.
//
// The URL is set to NULL rather than the row being deleted, because the subscriber is
// still a live subscriber; only the migration artefact is expired. migrated_at is
// deliberately KEPT: it is an audit fact about when the cutover happened, it is not
// personal or third-party data, and the migration report reads it.
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

// CountSubscriberRevocationsPending reports how many subscribers still owe a
// broker-side credential revocation, and when the oldest of those obligations was
// recorded.
//
// No index is required and none is added. blnk.event_subscribers is documented as small
// and read-rarely; the aggregate touches a few hundred rows at most and returns two
// values, so a partial index would cost a migration to save nothing measurable.
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
// SCRAM credentials that outlived their registry record, and revocations the broker
// refused.
//
// The predicates match the two partial indexes sql/1781248950.sql creates, so in the
// healthy steady state both indexes are empty and the aggregate reads nothing.
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

// MarkSubscriberCredentialOrphaned records that a credential exists at the broker which
// Blnk could neither record nor revoke.
//
// Every other write on a provisioning path carries the claim token so a lapsed lease
// cannot publish stale state. This one must not, and the asymmetry is the point: it is
// reached only when an issuance has ALREADY failed and its compensation has ALSO
// failed, and one of the ways that happens is precisely that the claim lapsed.
//
// It is idempotent and keeps the FIRST instant, because the quantity an operator alerts
// on is how long the exposure has stood, not when it was last re-observed.
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
// A row that has since been deleted is not an error: the deregistration succeeded on
// another attempt, which is the outcome this marker exists to be superseded by.
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
// Credential issuance is not idempotent — each call generates a new secret and writes
// it to the broker, where the LAST write wins and every earlier secret stops working.
// Two operators issuing at once therefore end with one usable secret and two
// successful-looking responses, and the one holding the loser's secret has a credential
// that authenticates against nothing.
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

// ---------------------------------------------------------------------------------------
// The revocation lifecycle and the provisioning fence
//
// Two systems hold a subscriber's access — this table holds WHICH principal and WHICH
// topics, the broker holds the credential and the bindings — and no transaction spans
// them. Everything below exists because of that, and each piece answers one of the two
// ways the gap between them can be lost:
//
//   - A REVOCATION THAT FAILED must stay recoverable, so deregistration tombstones the
//     row before it touches the broker and deletes it only once the revocation is
//     confirmed.
//   - TWO CONCURRENT OPERATIONS on one subscriber must not interleave, so each claims
//     the subscriber under a leased token before it touches the broker.
// ---------------------------------------------------------------------------------------

// defaultSubscriberFenceLease is the fence lease used when a caller supplies none.
//
// It is three times the credential issuance budget, which is the shape the lease has to have:
// long enough that a legitimate issuance cannot lose its own claim mid-flight, short enough
// that a process killed while holding one does not fence the subscriber for materially longer
// than an operator would wait before retrying.
const defaultSubscriberFenceLease = 15 * time.Second

// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row.
//
// When the revocation failed, the principal kept authenticating and kept reading, and
// the only record of WHICH principal that was had just been deleted — so the residue
// was live access that nothing in Blnk could see, recoverable only from a log line if
// anyone read it.
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
// Kafka stores ONE SCRAM credential per principal. Two overlapping issuances therefore
// both write a credential and the second replaces the first, so the broker ends up
// holding one password while this table may hold the reference derived from the other —
// and the caller holding the recorded one cannot authenticate.
//
// So the broker is only ever touched under this claim. The second caller is refused as
// a conflict before it generates a secret, which is the whole point — a refused
// issuance costs a caller one retry, while an interleaved one costs it a credential
// that does not work and gives it no way to find out.
//
// Revocation and deregistration claim it too, so an issuance cannot interleave with the
// removal of the very credential it is writing.
//
// It is the shape blnk.event_outbox already claims rows with, and it is chosen for the
// same two reasons. It is cross-process without a lock server, so two Blnk instances
// fence each other.
//
// The token is rotated on every claim, so a caller whose lease expired cannot release
// or complete a claim that has since been taken over.
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
// A single fixed lease cannot satisfy both. Sized for the worst case it becomes a long
// outage after a crash; sized for recovery it expires mid-operation, and the operation
// then keeps going with a claim it no longer owns — which is the failure this method
// exists to remove.
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
// Releasing early is what keeps the fence from making a retry wait out the whole lease
// after a fast failure. It is conditional on the token for the same reason every other
// transition in this schema is: a caller whose lease expired no longer owns the claim,
// and clearing a claim somebody else has taken would let a third operation start
// alongside it.
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

//
//   * requireProvisioningToken was a SECOND NAME for requireFenceToken.
//   * subscriberFenceState read (exists, held) as two columns and subscriberHoldsClaim
//     reduced it to a boolean. describeFencedWriteMiss replaced both with ONE read of
//     the two facts that actually decide the answer — whether the claim is still held,
//     and whether a revocation is pending — and returns a miss REASON rather than a
//     pair a caller has to interpret.
//   * assertFencedSubscriberRowAffected turned a row count into an error by re-reading
//     the fence. requireFencedWriteLanded and classifyFencedWriteMiss do that, on the
//     same one read.
//   * subscriberFenceLost built the typed lost-fence error. Its two live counterparts
//     build the identical error, and both carry subscriberFenceLostMarker — the phrase
//     every producer of this conflict must spell identically, because the service
//     branches on it to decide whether to abandon an operation and compensate.
