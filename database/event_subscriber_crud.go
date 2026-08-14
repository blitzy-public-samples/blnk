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

// CreateEventSubscriber registers a subscriber and returns the stored row.
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
