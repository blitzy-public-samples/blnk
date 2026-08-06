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
// # No secret is written or read here, and none can be
//
// The SASL secret is generated during provisioning, returned to the caller exactly
// once by the service layer, and persisted nowhere. This file writes only a
// non-reversible credential_reference and the issuance instant, mirroring
// api_key.go where the key column holds a bcrypt hash and the raw key is never
// stored. blnk.event_subscribers has no column able to hold a plaintext or
// reversibly encrypted secret, so there is nothing here for such a value to be
// written to — the prohibition is enforced by the schema rather than by review.
//
// The TEXT[] authorized_topics column is read and written through lib/pq's
// pq.Array, exactly as api_key.go handles the only other array column in this
// schema.
package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/lib/pq"
)

// eventSubscriberColumns is the column list every read of
// blnk.event_subscribers projects, in the exact order scanEventSubscriber
// consumes it. Declared once so the single-row and listing paths cannot drift
// apart in projection or scan order.
const eventSubscriberColumns = `id, subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, ` +
	`partition_key_prefix, credential_reference, credential_issued_at, webhook_url, migrated_at, created_at, updated_at`

// eventSubscriberScanner is the minimum surface scanEventSubscriber needs,
// satisfied by both *sql.Row and *sql.Rows.
type eventSubscriberScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventSubscriber decodes one blnk.event_subscribers row into a
// model.EventSubscriber.
//
// The five nullable columns are preserved AS pointers rather than flattened to
// zero values, because for each of them NULL carries meaning a zero value would
// destroy: a nil PartitionKeyPrefix means "no key restriction" and not "restrict
// to the empty prefix"; a nil CredentialReference means no credential has ever
// been issued; a nil MigratedAt means not yet migrated. Collapsing any of them
// would invert the intent recorded in the schema.
func scanEventSubscriber(s eventSubscriberScanner) (model.EventSubscriber, error) {
	var sub model.EventSubscriber
	var partitionKeyPrefix, credentialReference, webhookURL sql.NullString
	var credentialIssuedAt, migratedAt sql.NullTime

	if err := s.Scan(
		&sub.ID,
		&sub.SubscriberID,
		&sub.Name,
		&sub.KafkaPrincipal,
		&sub.ConsumerGroupID,
		pq.Array(&sub.AuthorizedTopics),
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
		sub.CredentialIssuedAt = &credentialIssuedAt.Time
	}
	if migratedAt.Valid {
		sub.MigratedAt = &migratedAt.Time
	}

	// A NOT NULL '{}' column decodes to an empty, non-nil slice. Normalising nil
	// to empty here keeps HasTopicAccess and every JSON response consistent: an
	// unauthorised subscriber serialises as [] rather than null, so a consumer of
	// the API never has to distinguish the two.
	if sub.AuthorizedTopics == nil {
		sub.AuthorizedTopics = []string{}
	}

	return sub, nil
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
//
// The row is read back through RETURNING rather than reconstructed in Go so the
// caller receives the database's own values for the surrogate key and both
// bookkeeping timestamps, instead of a struct that merely looks like what was
// stored. A duplicate subscriber_id or kafka_principal — both uniquely indexed —
// is reported as a conflict rather than an internal error, because it is a
// caller mistake and the API layer should answer 409.
func (d Datasource) CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) {
	if subscriber == nil {
		return nil, apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber is required", nil)
	}

	topics := subscriber.AuthorizedTopics
	if topics == nil {
		topics = []string{}
	}

	row := d.Conn.QueryRowContext(ctx, `
		INSERT INTO blnk.event_subscribers
		(subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, partition_key_prefix, webhook_url, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		RETURNING `+eventSubscriberColumns,
		subscriber.SubscriberID,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		pq.Array(topics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
	)

	stored, err := scanEventSubscriber(row)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code.Name() == "unique_violation" {
			return nil, apierror.NewAPIError(apierror.ErrConflict, "A subscriber with this ID or Kafka principal already exists", err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create event subscriber", err)
	}

	return &stored, nil
}

// GetEventSubscriberByID retrieves a subscriber by its business subscriber_id.
//
// A missing row is returned as a typed not-found error rather than a bare
// sql.ErrNoRows, so the API layer can map it to a 404 without inspecting driver
// sentinels.
func (d Datasource) GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)

	subscriber, err := scanEventSubscriber(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, "Subscriber not found", err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve event subscriber", err)
	}

	return &subscriber, nil
}

// ListEventSubscribers pages the registry, newest first.
//
// The limit is bounded for the same reason the dead-letter listing bounds its
// own: an unbounded page size turns a management endpoint into a full table
// scan.
func (d Datasource) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	if limit <= 0 {
		limit = defaultSubscriberPageSize
	}
	if limit > maxSubscriberPageSize {
		limit = maxSubscriberPageSize
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to list event subscribers", err)
	}
	defer func() { _ = rows.Close() }()

	var subscribers []model.EventSubscriber
	for rows.Next() {
		subscriber, scanErr := scanEventSubscriber(rows)
		if scanErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan event subscriber", scanErr)
		}
		subscribers = append(subscribers, subscriber)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over event subscribers", err)
	}

	return subscribers, nil
}

// Page-size bounds for the subscriber listing.
const (
	defaultSubscriberPageSize = 50
	maxSubscriberPageSize     = 500
)

// UpdateEventSubscriber updates a subscriber's access model and its legacy
// webhook URL.
//
// Only the mutable columns are written. subscriber_id is the business key and is
// used to locate the row rather than changed, and the credential columns are
// deliberately NOT touched here — they are owned exclusively by
// RecordSubscriberCredential, so an ordinary update can never blank out or
// forge an issuance record. updated_at is maintained here in the repository
// layer, as every other table in this schema does it; blnk has no updated_at
// trigger anywhere and this table does not introduce the first one.
func (d Datasource) UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error {
	if subscriber == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber is required", nil)
	}

	topics := subscriber.AuthorizedTopics
	if topics == nil {
		topics = []string{}
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET name = $1,
			kafka_principal = $2,
			consumer_group_id = $3,
			authorized_topics = $4,
			partition_key_prefix = $5,
			webhook_url = $6,
			updated_at = NOW()
		WHERE subscriber_id = $7
	`,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		pq.Array(topics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		subscriber.SubscriberID,
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code.Name() == "unique_violation" {
			return apierror.NewAPIError(apierror.ErrConflict, "Another subscriber already uses this Kafka principal", err)
		}
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update event subscriber", err)
	}

	return assertEventSubscriberAffected(result, "Subscriber not found", "Failed to update event subscriber")
}

// DeleteEventSubscriber removes a subscriber from the registry.
//
// Deleting the registry row does NOT revoke the subscriber's broker-side
// credential or ACLs — the broker is a separate system of record, and
// deprovisioning it is the service layer's responsibility. This method is the
// registry half only, and a caller that deletes here without deprovisioning
// there leaves a principal that can still authenticate.
func (d Datasource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete event subscriber", err)
	}

	return assertEventSubscriberAffected(result, "Subscriber not found", "Failed to delete event subscriber")
}

// RecordSubscriberCredential persists the outcome of a credential issuance: a
// non-reversible reference to the credential and the instant it was issued.
//
// credentialReference has ALREADY been derived by the caller and is never a
// password, nor anything a password can be recovered from. The secret itself is
// returned to the API caller exactly once and stored nowhere, so a lost secret is
// replaced by a fresh issuance rather than recovered. The two columns are written
// together because together they are the complete record of an issuance; a
// reissue overwrites both.
//
// An empty reference is rejected rather than written, because a blank reference
// is indistinguishable from NULL once stored and would make the "registered, not
// yet provisioned" test lie about a subscriber that really was provisioned.
func (d Datasource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	if credentialReference == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Credential reference is required", nil)
	}
	if issuedAt.IsZero() {
		issuedAt = time.Now()
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_reference = $1,
			credential_issued_at = $2,
			updated_at = NOW()
		WHERE subscriber_id = $3
	`, credentialReference, issuedAt, subscriberID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record subscriber credential", err)
	}

	return assertEventSubscriberAffected(result, "Subscriber not found", "Failed to record subscriber credential")
}

// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber has
// completed its move from legacy HTTP webhook delivery to Kafka consumption.
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what
// migration-progress reporting counts during the dual-delivery window and what
// the index over the column exists to serve.
func (d Datasource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	if migratedAt.IsZero() {
		migratedAt = time.Now()
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET migrated_at = $1,
			updated_at = NOW()
		WHERE subscriber_id = $2
	`, migratedAt, subscriberID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to mark subscriber migrated", err)
	}

	return assertEventSubscriberAffected(result, "Subscriber not found", "Failed to mark subscriber migrated")
}

// assertEventSubscriberAffected turns a zero-row result into a typed not-found
// error.
//
// Postgres reports a WHERE clause that matched nothing as a successful statement
// affecting no rows, so without this check every write against a non-existent
// subscriber would return nil and the caller would believe it succeeded. A driver
// that cannot report the affected count is treated as success rather than
// failure, because the statement itself did not error and inventing a not-found
// from a missing capability would be worse than the check being skipped.
func assertEventSubscriberAffected(result sql.Result, notFoundMsg, internalMsg string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return nil
	}
	if affected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, notFoundMsg, nil)
	}
	return nil
}
