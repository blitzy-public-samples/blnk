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

// event_subscriber.go is the repository implementation of the eventSubscriber
// contract declared in repository.go: persistence for blnk.event_subscribers, the
// registry of Kafka subscribers and the access boundary provisioned for each one.
//
// # The subscriber concept is new, so the folder's conventions carry the weight
//
// Nothing in this codebase resembled a subscriber before this change — there was
// no subscriber table, model, repository or endpoint to extend. There is therefore
// no prior art to be consistent with INSIDE this domain, which is exactly why
// consistency with the conventions of THIS FOLDER matters more than usual here:
// value receivers on Datasource, apierror-wrapped failures, lib/pq array handling
// and OpenTelemetry spans on the transaction.database tracer are what make this
// file recognisable as Blnk code rather than as an import from somewhere else.
//
// # NO PLAINTEXT SASL SECRET IS ACCEPTED, RETURNED, LOGGED OR PERSISTED HERE
//
// This is the defining constraint on this file, and it is structural rather than
// aspirational. Read the signatures below: not one of them takes a password, and
// not one of them returns a value anything can be authenticated with. There is no
// getter, no reveal path, no decrypt path — and deliberately no verify method
// either, because a verify method is the first step toward a reveal method.
//
// The SASL secret is generated during provisioning by the service layer, returned
// to the API caller EXACTLY ONCE, and persisted nowhere. What this file writes is
// a NON-REVERSIBLE credential_reference that the caller has ALREADY derived, plus
// the instant of issuance. A lost secret is therefore replaced by a fresh issuance
// rather than recovered. This mirrors api_key.go, whose key column holds a bcrypt
// hash and whose raw key is never stored anywhere.
//
// golang.org/x/crypto/bcrypt is intentionally NOT imported. Hashing and derivation
// belong to the caller that owns issuance; importing bcrypt here would signal that
// this file handles secrets, which must not be true. If a future change appears to
// need a secret column, the design has been misread — see requirement R-7.
//
// One non-obvious hazard is worth naming, because it is the likeliest way a secret
// could ever reach a log from this package: apierror.NewAPIError LOGS its details
// argument (logrus.WithField("details", details)). Every call below therefore
// passes only the driver error or nil as details, never a credential value and
// never a caller-supplied string that could hold one. The same rule governs span
// attributes: the subscriber id, the Kafka principal and the consumer group are
// recorded because they are identifiers rather than secrets; the credential
// reference is never recorded, not even truncated.
//
// # The credential columns have exactly one writer
//
// CreateEventSubscriber and UpdateEventSubscriber do not touch
// credential_reference or credential_issued_at at all. RecordSubscriberCredential
// is the only method that writes them. That single-writer rule is what keeps the
// audit story legible: an issuance record cannot be forged or blanked out by an
// ordinary registry edit, and there is one place to look when asking when a
// credential was minted.
//
// # webhook_url and migrated_at are dual-run-only
//
// Both columns exist solely for the 30-day window in which Kafka publishing and
// legacy HTTP webhook delivery run side by side. They are here because the
// requirement to migrate existing subscribers off the webhook subscription REST
// API meets a repository in which no such API exists: the whole subscription
// surface today is one global WebhookConfig{Url, Headers} value. Recording a legacy
// URL per subscriber is what gives an existing subscriber somewhere to be migrated
// FROM, and what makes the post-sunset 410 Gone behaviour observable at all. Their
// use ENDS AT SUNSET, when they and the index over migrated_at are dropped.
//
// # The 5-second provisioning budget shapes every query below
//
// POST /subscribers/{id}/kafka-credentials must complete within 5 seconds, and the
// Kafka admin round trips consume most of that. Every method here is therefore a
// SINGLE index-served statement: no N+1 reads, no transaction where an UPDATE
// suffices, and no cache. d.Cache is deliberately untouched — a stale subscriber
// ACL set is a security problem, not a performance win.
package database

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// eventSubscriberColumns is the column list every read of blnk.event_subscribers
// projects, in the exact order scanEventSubscriber consumes it. It is declared
// once so the single-row and listing paths cannot drift apart in either
// projection or scan order — a drift that would not fail to compile and would
// instead surface as silently transposed fields.
const eventSubscriberColumns = `id, subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, ` +
	`partition_key_prefix, credential_reference, credential_issued_at, webhook_url, migrated_at, created_at, updated_at`

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

// eventSubscriberScanner is the minimum surface scanEventSubscriber needs,
// satisfied by both *sql.Row and *sql.Rows. It lets the single-row read and the
// listing share one decoder instead of maintaining two scan orders.
type eventSubscriberScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventSubscriber decodes one blnk.event_subscribers row into a
// model.EventSubscriber.
//
// authorized_topics is a TEXT[] and is read through a pq.StringArray local that is
// then converted with []string(...), exactly as api_key.go reads scopes — the only
// other array column in this schema. A NOT NULL '{}' column decodes to an empty
// slice rather than to a one-element slice holding "", which is the classic lib/pq
// array trap; the nil normalisation below closes the remaining case so callers
// never have to distinguish nil from empty.
//
// The five nullable columns are preserved AS pointers rather than flattened to
// zero values, because for each of them NULL carries meaning a zero value would
// destroy: a nil PartitionKeyPrefix means "entitled to whole topics" and NOT
// "restrict to the empty prefix", which would invert the grant; a nil
// CredentialReference is the reliable test for "registered, not yet provisioned";
// a nil MigratedAt means not yet migrated, which is precisely what
// migration-progress reporting counts. Each value is copied into its own local
// before its address is taken, so no field aliases a scan destination.
func scanEventSubscriber(s eventSubscriberScanner) (model.EventSubscriber, error) {
	var sub model.EventSubscriber
	var authorizedTopics pq.StringArray
	var partitionKeyPrefix, credentialReference, webhookURL sql.NullString
	var credentialIssuedAt, migratedAt sql.NullTime

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

	return sub, nil
}

// normalizeAuthorizedTopics prepares the topic grant for writing.
//
// The column is NOT NULL DEFAULT '{}' precisely so that an unprovisioned
// subscriber is authorised for NOTHING instead of for NULL, which is a security
// property rather than a convenience: NULL would make "no grant recorded"
// indistinguishable from "grant not yet known" and force every authorisation
// check to guess which was meant. A nil slice from the caller is therefore written
// as the empty array, so the registry FAILS CLOSED.
//
// pq.StringArray is used rather than a hand-built '{a,b}' literal so element
// quoting and escaping are the driver's problem, which is what keeps a topic name
// containing a comma or a brace from corrupting the value.
func normalizeAuthorizedTopics(topics []string) pq.StringArray {
	if topics == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(topics)
}

// requireSubscriberID rejects a blank business key before any query is issued.
//
// Failing in memory rather than at the database matters twice over: it keeps the
// 5-second provisioning budget for a request that cannot succeed anyway, and it
// answers with "required" instead of the misleading "Subscriber not found" that an
// empty WHERE match would otherwise produce. Whitespace is trimmed for the test
// only — the caller's value is stored verbatim, because silently rewriting an
// identifier would be a worse surprise than rejecting it.
func requireSubscriberID(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber ID is required", nil)
	}
	return nil
}

// requireSubscriberFields validates the four NOT NULL business columns.
//
// NOT NULL alone does not stop an empty string, and each of these being blank is a
// real hazard rather than a cosmetic one. A blank subscriber_id yields a row no
// endpoint can address. A blank name defeats the reason the registry exists — the
// schema documents name as NOT NULL because an unnamed principal cannot be
// triaged. A blank kafka_principal is the worst of the four: it would occupy the
// unique principal index while naming a principal that authenticates as nothing,
// and because the index is unique it would also block the next subscriber that
// really did have no principal set. A blank consumer_group_id would be returned
// verbatim to a subscriber that then could not join a group.
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
	if strings.TrimSpace(subscriber.KafkaPrincipal) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Kafka principal is required", nil)
	}
	if strings.TrimSpace(subscriber.ConsumerGroupID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Consumer group ID is required", nil)
	}
	return nil
}

// classifySubscriberWriteError turns a driver error from an insert or update into
// the typed error the API layer should answer with.
//
// A unique violation is a CALLER mistake and must surface as a conflict — 409 —
// rather than as an internal error, and never as a raw driver error or a panic.
// The two unique indexes are told apart through pqErr.Constraint so the message
// names what actually collided: "this ID" and "this Kafka principal" are different
// operator problems, and a single generic message would send whoever reads it to
// check the wrong column first. An unrecognised constraint still resolves to a
// conflict, because the violation is real even when the index name is not one of
// the two this file knows about.
//
// errors.As is used rather than a bare type assertion so the classification also
// holds if the driver error arrives wrapped, which a bare assertion would silently
// misfile as an internal server error.
func classifySubscriberWriteError(err error, internalMessage string) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code.Name() == uniqueViolationPostgresCode {
		switch pqErr.Constraint {
		case subscriberIDUniqueIndex:
			return apierror.NewAPIError(apierror.ErrConflict, "A subscriber with this ID already exists", err)
		case kafkaPrincipalUniqueIndex:
			return apierror.NewAPIError(apierror.ErrConflict, "Another subscriber already uses this Kafka principal", err)
		default:
			return apierror.NewAPIError(apierror.ErrConflict, "Subscriber already exists", err)
		}
	}
	return apierror.NewAPIError(apierror.ErrInternalServer, internalMessage, err)
}

// assertSubscriberRowAffected turns a zero-row write into a typed not-found error.
//
// PostgreSQL reports a WHERE clause that matched nothing as a SUCCESSFUL statement
// affecting no rows. Without this check every write against a non-existent
// subscriber would return nil and the caller would believe it had succeeded — and
// for RecordSubscriberCredential that would mean reporting a provisioned
// credential for a subscriber that does not exist, which is the one outcome the
// endpoint must never produce. A missing row has to be LOUD.
//
// A RowsAffected failure is reported as an internal error rather than swallowed,
// following DeleteLineageMapping: if the count cannot be read, whether the row
// existed is genuinely unknown, and claiming success is the one answer that is
// certainly wrong.
//
// The subscriber-specific not-found code is used in preference to the generic one.
// Both resolve to HTTP 404 through apierror.StatusForCode, so the response status
// is identical either way, but the typed code lets the subscriber API propagate
// what it received instead of re-deriving a domain meaning from a generic error.
func assertSubscriberRowAffected(result sql.Result, internalMessage string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, internalMessage, err)
	}
	if affected == 0 {
		return apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
	}
	return nil
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
//
// The row is read back through RETURNING rather than reconstructed in Go, so the
// caller receives the database's own surrogate key and bookkeeping timestamps
// instead of a struct that merely resembles what was stored.
//
// THE CREDENTIAL AND MIGRATION COLUMNS ARE NOT WRITTEN HERE, and any values the
// supplied struct carries in CredentialReference, CredentialIssuedAt or MigratedAt
// are ignored rather than persisted. A newly registered subscriber is by definition
// "registered, not yet provisioned" and "not yet migrated", and those columns have
// exactly one writer each — RecordSubscriberCredential and MarkSubscriberMigrated.
// The returned row shows the true stored state, so a caller that passed such a
// value can see it was not kept.
//
// webhook_url IS accepted at creation, because recording an existing subscriber's
// legacy URL is the whole point of the dual-run columns: it is what gives a
// subscriber already receiving HTTP pushes somewhere to be migrated FROM.
func (d Datasource) CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CreateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		span.RecordError(err)
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
		span.RecordError(err)
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
// resolving through the unique index the credential endpoint's 5-second budget
// depends on.
//
// A missing row is reported as a typed not-found error and NEVER as a bare
// sql.ErrNoRows, so the API layer maps it to 404 without inspecting driver
// sentinels. Returning (nil, nil) instead — as the lineage mapping reader does —
// would be wrong here: the subscriber API has to tell "no such subscriber" apart
// from "found", and a nil-nil result forces every caller to rediscover that
// distinction for itself.
func (d Datasource) GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventSubscriberByID")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
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
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve event subscriber", err)
	}

	span.AddEvent("Event subscriber retrieved", trace.WithAttributes(
		attribute.String("subscriber.id", subscriber.SubscriberID),
		attribute.String("subscriber.principal", subscriber.KafkaPrincipal),
		attribute.Int("subscriber.authorized_topic_count", len(subscriber.AuthorizedTopics)),
	))
	return &subscriber, nil
}

// ListEventSubscribers pages the registry NEWEST FIRST, ordered by created_at
// descending and tie-broken by the surrogate key.
//
// The tie-break is what makes the order deterministic rather than merely sorted:
// created_at is stamped in Go and two subscribers registered in the same
// microsecond would otherwise page in arbitrary relative order, which can both
// repeat and skip a row across pages. Ordering by the monotonic id as the second
// key removes that.
//
// The limit is normalised and bounded, and a negative offset is clamped, so a
// malformed page request degrades to a cheap query instead of a full table scan.
func (d Datasource) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListEventSubscribers")
	defer span.End()

	if limit <= 0 {
		limit = defaultSubscriberPageSize
	}
	if limit > maxSubscriberPageSize {
		limit = maxSubscriberPageSize
	}
	if offset < 0 {
		offset = 0
	}
	span.SetAttributes(
		attribute.Int("subscriber.page_limit", limit),
		attribute.Int("subscriber.page_offset", offset),
	)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to list event subscribers", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Logged rather than returned. The rows have already been read by the
			// time this runs, so surfacing a close failure would discard a correct
			// result the caller needs in favour of a condition it cannot act on.
			logrus.WithError(closeErr).Error("failed to close event subscriber rows")
		}
	}()

	// Non-nil so an empty page serialises as [] rather than null, and pre-sized to
	// the bounded page so a full page does not repeatedly regrow the slice.
	subscribers := make([]model.EventSubscriber, 0, limit)
	for rows.Next() {
		subscriber, scanErr := scanEventSubscriber(rows)
		if scanErr != nil {
			span.RecordError(scanErr)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan event subscriber", scanErr)
		}
		subscribers = append(subscribers, subscriber)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over event subscribers", err)
	}

	span.AddEvent("Event subscribers listed", trace.WithAttributes(
		attribute.Int("subscriber.count", len(subscribers)),
	))
	return subscribers, nil
}

// UpdateEventSubscriber updates a subscriber's access model and its legacy webhook
// URL.
//
// Only the mutable columns are written. subscriber_id locates the row rather than
// being changed — it is the business key an issued credential and a live consumer
// are both addressed by, so reassigning it here would silently orphan them.
//
// THE CREDENTIAL COLUMNS ARE DELIBERATELY NOT TOUCHED. They belong exclusively to
// RecordSubscriberCredential, which means an ordinary registry edit can neither
// blank out nor forge an issuance record, and there is exactly one place to look
// when auditing when a credential was minted.
//
// Changing kafka_principal or authorized_topics here changes only the RECORD of the
// access boundary. The broker is a separate system of record and is not reconciled
// by this call: the service layer must re-provision the ACLs, or the registry and
// the broker will disagree about what the subscriber may read.
func (d Datasource) UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "UpdateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriber.SubscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET name = $1,
			kafka_principal = $2,
			consumer_group_id = $3,
			authorized_topics = $4,
			partition_key_prefix = $5,
			webhook_url = $6,
			updated_at = $7
		WHERE subscriber_id = $8
	`,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		normalizeAuthorizedTopics(subscriber.AuthorizedTopics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		time.Now(),
		subscriber.SubscriberID,
	)
	if err != nil {
		span.RecordError(err)
		// Reachable through the principal index: moving a principal onto one another
		// subscriber already holds is a conflict, not a server fault.
		return classifySubscriberWriteError(err, "Failed to update event subscriber")
	}

	if err := assertSubscriberRowAffected(result, "Failed to update event subscriber"); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Event subscriber updated", trace.WithAttributes(
		attribute.String("subscriber.id", subscriber.SubscriberID),
		attribute.String("subscriber.principal", subscriber.KafkaPrincipal),
		attribute.String("subscriber.consumer_group", subscriber.ConsumerGroupID),
		attribute.Int("subscriber.authorized_topic_count", len(subscriber.AuthorizedTopics)),
	))
	return nil
}

// DeleteEventSubscriber removes a subscriber from the registry.
//
// This is the REGISTRY HALF ONLY. Deleting the row does not revoke the
// subscriber's broker-side SCRAM credential or its ACL bindings, because the broker
// is a separate system of record: a caller that deletes here without deprovisioning
// there leaves a principal that can still authenticate and still read. The
// deletion is reported as not-found when no row matched, so a caller cannot mistake
// "already gone" for "just removed" and skip the broker-side work.
func (d Datasource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "DeleteEventSubscriber")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete event subscriber", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to delete event subscriber"); err != nil {
		span.RecordError(err)
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
// # credentialReference is ALREADY non-reversible when it arrives
//
// This method neither hashes nor derives anything, and it must stay that way. The
// caller that owns issuance generates the SASL password, returns it to the API
// caller exactly once, derives the reference and hands only the reference here. If
// a future change moves derivation into this method, the next step is a matching
// verify method, and a verify method is the first step toward a reveal method —
// which is precisely the path the schema was designed to close off. There is no
// column here capable of holding a password, so there is nothing for such a path to
// read from.
//
// Nothing in this method may carry a secret: the reference is written as a bound
// parameter and is never interpolated into a message, never passed as apierror
// details (which are logged) and never recorded as a span attribute.
//
// # A reissue overwrites, and that is correct
//
// Both columns are written together because together they are one issuance record,
// and issuing again replaces both. The previous credential is revoked out of band
// at the broker, so retaining a history of superseded references here would add an
// audit surface with no reader and no authority — do not add one.
//
// # A missing subscriber is an error, never a no-op
//
// Zero affected rows means the issuance was recorded against a subscriber that does
// not exist. Reporting success there would let the credential endpoint hand back a
// working credential for an unregistered subscriber, so it fails loudly instead.
//
// An empty reference is rejected rather than written: once stored it would be
// indistinguishable from NULL and would make the "registered, not yet provisioned"
// test lie about a subscriber that really had been provisioned.
func (d Datasource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredential")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	if strings.TrimSpace(credentialReference) == "" {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Credential reference is required", nil)
		span.RecordError(err)
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
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record subscriber credential", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to record subscriber credential"); err != nil {
		span.RecordError(err)
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

// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber has
// completed its move from legacy HTTP webhook delivery to Kafka consumption.
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what
// migration-progress reporting counts during the 30-day dual-delivery window and
// what the index over the column exists to serve. Together with webhook_url this
// is DUAL-RUN-ONLY state: both columns and that index are dropped at sunset, and
// this method goes with them.
//
// Re-stamping simply moves the timestamp forward. It is idempotent in effect — a
// subscriber that is migrated stays migrated — so a retried migration step needs no
// guard at the call site.
func (d Datasource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberMigrated")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	if migratedAt.IsZero() {
		// A zero timestamp is still NOT NULL, so it would read as "migrated in year
		// 1" and quietly corrupt progress reporting rather than leaving the row
		// counted as unmigrated.
		migratedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET migrated_at = $1,
			updated_at = $2
		WHERE subscriber_id = $3
	`, migratedAt, time.Now(), subscriberID)
	if err != nil {
		span.RecordError(err)
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark subscriber migrated", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to mark subscriber migrated"); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Subscriber marked migrated", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.migrated_at", migratedAt.UTC().Format(time.RFC3339)),
	))
	return nil
}
