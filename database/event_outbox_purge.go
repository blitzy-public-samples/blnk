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
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// PurgeTerminalEventsBefore deletes terminal event rows whose occurrence is older than
// cutoff, and returns how many it removed. It is the retention primitive behind the
// outbox's data-minimisation contract.
func (d Datasource) PurgeTerminalEventsBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PurgeTerminalEventsBefore")
	defer span.End()

	if cutoff.IsZero() {
		// A zero cutoff would read as "delete everything older than the year 1", which
		// deletes nothing — but it is far more likely to be an unset field than an intention,
		// and silently doing nothing would hide a broken retention job that looks like it is
		// running.
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Event outbox retention cutoff is required", nil)
		failDatabaseSpan(span, err)
		return 0, err
	}
	if limit <= 0 {
		limit = defaultEventPurgeBatchSize
	}
	if limit > maxEventPurgeBatchSize {
		limit = maxEventPurgeBatchSize
	}

	span.SetAttributes(
		attribute.String("event_outbox.retention_cutoff", cutoff.UTC().Format(time.RFC3339)),
		attribute.Int("event_outbox.retention_limit", limit),
	)

	// This is the one IN-subquery LIMIT in this file that is NOT wrapped in a MATERIALIZED
	// CTE, and the omission is deliberate rather than an oversight.
	row := d.Conn.QueryRowContext(ctx, `
		WITH removed AS (
			DELETE FROM blnk.event_outbox
			WHERE id IN (
				SELECT id FROM blnk.event_outbox
				WHERE status = $1
				  AND occurred_at < $2
				ORDER BY occurred_at ASC
				LIMIT $3
			)
			RETURNING occurred_at, kafka_offset
		), logged AS (
			INSERT INTO blnk.event_outbox_purge_log
				(cutoff, rows_removed, confirmed_removed, oldest_occurred_at, newest_occurred_at)
			SELECT $2, COUNT(*), COUNT(kafka_offset), MIN(occurred_at), MAX(occurred_at)
			FROM removed
			HAVING COUNT(*) > 0
			RETURNING rows_removed
		)
		SELECT COUNT(*) FROM removed
	`, model.EventOutboxStatusDispatched, cutoff, limit)

	var purged int64
	if err := row.Scan(&purged); err != nil {
		failDatabaseSpan(span, err)
		return 0, loggedDatabaseError(apierror.ErrInternalServer, "Failed to purge terminal event outbox entries", "purge_terminal_events_before", err)
	}

	span.SetAttributes(attribute.Int64("event_outbox.purged_count", purged))
	return purged, nil
}

// SumPurgedTerminalEvents reports what retention has removed from blnk.event_outbox
// over the table's whole life.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - model.EventOutboxPurgeTotals: the totals.
//   - error: the repository's typed error.
func (d Datasource) SumPurgedTerminalEvents(ctx context.Context) (model.EventOutboxPurgeTotals, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "SumPurgedTerminalEvents")
	defer span.End()

	totals := model.EventOutboxPurgeTotals{Recorded: true}

	var lastPurgedAt sql.NullTime
	var newestPurgedOccurrence sql.NullTime

	err := d.Conn.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(rows_removed), 0)      AS rows_removed,
			COALESCE(SUM(confirmed_removed), 0) AS confirmed_removed,
			COUNT(*)                            AS batches,
			MAX(purged_at)                      AS last_purged_at,
			MAX(newest_occurred_at)             AS newest_purged_occurrence
		FROM blnk.event_outbox_purge_log
	`).Scan(
		&totals.RowsRemoved,
		&totals.ConfirmedRemoved,
		&totals.Batches,
		&lastPurgedAt,
		&newestPurgedOccurrence,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventOutboxPurgeTotals{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to total the event outbox purge log", "sum_purged_terminal_events", err)
	}

	if lastPurgedAt.Valid {
		instant := lastPurgedAt.Time.UTC()
		totals.LastPurgedAt = &instant
	}
	if newestPurgedOccurrence.Valid {
		instant := newestPurgedOccurrence.Time.UTC()
		totals.NewestPurgedOccurrence = &instant
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.purged_rows_total", totals.RowsRemoved),
		attribute.Int64("event_outbox.purged_confirmed_total", totals.ConfirmedRemoved),
		attribute.Int64("event_outbox.purge_batches", totals.Batches),
	)

	return totals, nil
}

// AuditEventRecordCoordinates reports, per (topic, partition), how many terminal rows
// claim a broker record there and what the extreme claimed offsets are.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - model.EventRecordCoordinateAudit: one entry per claimed partition, ordered by
//     topic then partition.
//   - error: the repository's typed error.
func (d Datasource) AuditEventRecordCoordinates(ctx context.Context) (model.EventRecordCoordinateAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditEventRecordCoordinates")
	defer span.End()

	audit := model.EventRecordCoordinateAudit{
		Coordinates: []model.EventRecordCoordinate{},
		MeasuredAt:  time.Now().UTC(),
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT kafka_topic, kafka_partition, COUNT(*), MIN(kafka_offset), MAX(kafka_offset)
		FROM blnk.event_outbox
		WHERE kafka_offset IS NOT NULL
		  AND kafka_topic IS NOT NULL
		  AND kafka_partition IS NOT NULL
		GROUP BY kafka_topic, kafka_partition
		ORDER BY kafka_topic ASC, kafka_partition ASC
	`)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker coordinates of published event outbox entries",
			"audit_event_record_coordinates", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	for rows.Next() {
		var coordinate model.EventRecordCoordinate
		if scanErr := rows.Scan(
			&coordinate.Topic,
			&coordinate.Partition,
			&coordinate.Rows,
			&coordinate.MinOffset,
			&coordinate.MaxOffset,
		); scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an event outbox broker coordinate",
				"audit_event_record_coordinates", scanErr)
		}
		audit.Coordinates = append(audit.Coordinates, coordinate)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordCoordinateAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox broker coordinates",
			"audit_event_record_coordinates", err)
	}

	span.SetAttributes(
		attribute.Int("event_outbox.claimed_partitions", len(audit.Coordinates)),
		attribute.Int64("event_outbox.claimed_rows", audit.TotalRows()),
	)

	return audit, nil
}
