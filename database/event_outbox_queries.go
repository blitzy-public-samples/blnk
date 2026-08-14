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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

// GetEventByID retrieves an entry by its business event_id UUID.
func (d Datasource) GetEventByID(ctx context.Context, eventID string) (*model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventByID")
	defer span.End()
	span.SetAttributes(attribute.String("event_outbox.event_id", eventID))

	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE event_id = $1
	`, eventID)

	entry, err := scanEventOutbox(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Not recorded on the span as an error: a lookup that finds nothing
			// is an ordinary outcome of a caller-supplied id, not a fault.
			return nil, loggedDatabaseError(apierror.ErrNotFound, "Event not found", "get_event_by_id", err)
		}
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to retrieve event outbox entry", "get_event_by_id", err)
	}

	span.SetAttributes(attribute.String("event_outbox.status", entry.Status))
	return &entry, nil
}

// ListDeadLetteredEvents pages the dead-letter inventory, applying EVERY narrowing the
// query expresses IN SQL.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterQuery: the narrowing and the page. The zero value is valid.
//
// Returns:
//   - []model.EventOutbox: the matching page, newest occurrence first.
//   - error: the repository's typed error.
func (d Datasource) ListDeadLetteredEvents(
	ctx context.Context,
	query model.DeadLetterQuery,
) ([]model.EventOutbox, error) {
	return d.ListDeadLetteredEventsFiltered(ctx, query, query.Limit, query.Offset)
}

// ListDeadLetterInventory returns one page of the dead-letter inventory an operator
// triages from, narrowed and paged entirely in SQL.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterInventoryQuery: the page and its narrowing.
//
// Returns:
//   - model.DeadLetterInventoryPage: the entries, plus the cursor for the next page
//     when one exists.
//   - error: a logged internal error.
func (d Datasource) ListDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListDeadLetterInventory")
	defer span.End()

	return listDeadLetterInventory(ctx, d.Conn, span, query)
}

// listDeadLetterInventory is the page read, parameterised by the connection it runs on.
func listDeadLetterInventory(
	ctx context.Context,
	conn sqlQueryer,
	span trace.Span,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = defaultDeadLetterPageSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}

	// The cursor's two halves are bound as a nullable pair, because a nil cursor and a
	// cursor at the zero instant must be the same request — "start at the newest entry" —
	// and binding a zero timestamp would instead exclude everything older than year one,
	// which is every row.
	var cursorInstant interface{}
	var cursorID int64
	if query.Cursor != nil {
		cursorInstant = query.Cursor.OccurredAt.UTC()
		cursorID = query.Cursor.ID
	}

	span.SetAttributes(
		attribute.Int("event_outbox.limit", limit),
		attribute.String("event_outbox.status_filter", query.Status),
		attribute.Bool("event_outbox.cursor_present", query.Cursor != nil),
	)

	// limit+1: the extra row is read to establish that another page exists and is then
	// discarded, which answers "is there more" without a COUNT over the inventory.
	var occurredFrom, occurredTo interface{}
	if !query.OccurredFrom.IsZero() {
		occurredFrom = query.OccurredFrom.UTC()
	}

	if !query.OccurredTo.IsZero() {
		occurredTo = query.OccurredTo.UTC()
	}

	rows, err := conn.QueryContext(ctx, listDeadLetterInventoryQuery,
		model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed,
		query.Status, query.EventType, query.Topic,
		cursorInstant, cursorID, occurredFrom, occurredTo, limit+1)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list the dead-letter inventory", "list_dead_letter_inventory", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	page := model.DeadLetterInventoryPage{
		Entries: make([]model.DeadLetterInventoryEntry, 0, limit),
	}

	for rows.Next() {
		entry, scanErr := scanDeadLetterInventoryEntry(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan a dead-letter inventory entry", "list_dead_letter_inventory", scanErr)
		}

		if len(page.Entries) == limit {
			// The probe row. Its existence is the answer, and the cursor is taken from the
			// LAST RETURNED entry rather than from this one, so the next page resumes
			// exactly where this one stopped.
			page.HasMore = true

			break
		}

		page.Entries = append(page.Entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over the dead-letter inventory", "list_dead_letter_inventory", err)
	}

	if page.HasMore && len(page.Entries) > 0 {
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = &model.DeadLetterCursor{OccurredAt: last.OccurredAt, ID: last.ID}
	}

	span.SetAttributes(
		attribute.Int("event_outbox.dead_lettered_count", len(page.Entries)),
		attribute.Bool("event_outbox.has_more", page.HasMore),
	)

	return page, nil
}

// deadLetterFilterClause renders a dead-letter filter as a SQL predicate and its
// arguments.
func deadLetterFilterClause(filter model.DeadLetterFilter, next int) (string, []interface{}, int) {
	clauses := make([]string, 0, 3)
	args := make([]interface{}, 0, 4)

	if status := strings.TrimSpace(filter.Status); status != "" {
		clauses = append(clauses, fmt.Sprintf("status = $%d", next))
		args = append(args, status)
		next++
	} else {
		clauses = append(clauses, fmt.Sprintf("status IN ($%d, $%d)", next, next+1))
		args = append(args,
			model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed)
		next += 2
	}

	if eventType := strings.TrimSpace(filter.EventType); eventType != "" {
		clauses = append(clauses, fmt.Sprintf("event_type = $%d", next))
		args = append(args, eventType)
		next++
	}

	if topic := strings.TrimSpace(filter.Topic); topic != "" {
		clauses = append(clauses, fmt.Sprintf("topic = $%d", next))
		args = append(args, topic)
		next++
	}

	// THE OCCURRENCE WINDOW, bound inclusively at both ends and omitted when either end is
	// zero: zero means "unbounded", not "year one", and binding it literally would exclude
	// every row at one end and none at the other. occurred_at is the right column for a
	// triage window rather than the row's creation or last-attempt instant — it is when
	// the ledger mutation happened, which is what an operator correlating a backlog
	// against an incident timeline holds, and it is the column the listing is ordered by,
	// so the window and the paging agree about what "newest first" selects.
	if !filter.OccurredFrom.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at >= $%d", next))
		args = append(args, filter.OccurredFrom.UTC())
		next++
	}

	if !filter.OccurredTo.IsZero() {
		clauses = append(clauses, fmt.Sprintf("occurred_at <= $%d", next))
		args = append(args, filter.OccurredTo.UTC())
		next++
	}

	return strings.Join(clauses, " AND "), args, next
}

// ListDeadLetteredEventsFiltered pages the dead-letter inventory with the caller's
// filters applied IN SQL.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - filter model.DeadLetterFilter: the narrowing.
//   - limit int: page size. Non-positive selects the default; oversized is capped.
//   - offset int: how many matching rows to skip. Negative is clamped to zero.
//
// Returns:
//   - []model.EventOutbox: the matching page, nil when nothing matches.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) ListDeadLetteredEventsFiltered(
	ctx context.Context,
	filter model.DeadLetterFilter,
	limit, offset int,
) ([]model.EventOutbox, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListDeadLetteredEventsFiltered")
	defer span.End()

	if limit <= 0 {
		limit = defaultDeadLetterPageSize
	}
	if limit > maxDeadLetterPageSize {
		limit = maxDeadLetterPageSize
	}
	if offset < 0 {
		offset = 0
	}

	predicate, args, next := deadLetterFilterClause(filter, 1)
	args = append(args, limit, offset)

	span.SetAttributes(
		attribute.Int("event_outbox.limit", limit),
		attribute.Int("event_outbox.offset", offset),
		attribute.Bool("event_outbox.filtered", filter.Narrows()),
	)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventOutboxColumns+`
		FROM blnk.event_outbox
		WHERE `+predicate+`
		ORDER BY occurred_at DESC, id DESC
		LIMIT $`+strconv.Itoa(next)+` OFFSET $`+strconv.Itoa(next+1), args...)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list dead-lettered events", "list_dead_lettered_events_filtered", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	var entries []model.EventOutbox
	for rows.Next() {
		entry, scanErr := scanEventOutbox(rows)
		if scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan dead-lettered event", "list_dead_lettered_events_filtered", scanErr)
		}
		entries = append(entries, entry)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over dead-lettered events", "list_dead_lettered_events_filtered", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.dead_lettered_count", len(entries)))

	return entries, nil
}

// CountDeadLetteredEvents counts the dead-letter inventory THE SAME FILTER selects.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - filter model.DeadLetterFilter: the narrowing. The zero value counts the whole
//     inventory.
//
// Returns:
//   - int64: how many rows match. Zero is a legitimate answer and not an error.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) CountDeadLetteredEvents(
	ctx context.Context,
	filter model.DeadLetterFilter,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountDeadLetteredEvents")
	defer span.End()

	predicate, args, _ := deadLetterFilterClause(filter, 1)

	var total int64
	if err := d.Conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_outbox
		WHERE `+predicate, args...).Scan(&total); err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count dead-lettered events", "count_dead_lettered_events", err)
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.dead_lettered_total", total),
		attribute.Bool("event_outbox.filtered", filter.Narrows()),
	)

	return total, nil
}

// CountUnresolvedEventOutbox returns a status-keyed count of every row that has NOT
// reached its terminal dispatched state, and touches no dispatched row at all.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//
// Returns:
//   - map[string]int64: counts by status for every non-dispatched status, never nil on
//     success.
//   - error: a logged internal error.
func (d Datasource) CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountUnresolvedEventOutbox")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, countUnresolvedEventOutboxQuery,
		model.EventOutboxStatusDispatched)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count unresolved event outbox entries by status",
			"count_unresolved_event_outbox", err)
	}

	return collectEventOutboxStatusCounts(span, rows, "count_unresolved_event_outbox")
}

// CountEventOutboxByStatus returns a status-keyed count of blnk.event_outbox rows,
// INCLUDING the dispatched history inside the caller's window.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - since time.Time: the earliest occurrence instant a dispatched row must have to be
//     counted.
//
// Returns:
//   - map[string]int64: counts by status, never nil on success.
//   - error: a logged internal error.
func (d Datasource) CountEventOutboxByStatus(ctx context.Context, since time.Time) (map[string]int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountEventOutboxByStatus")
	defer span.End()

	since = normalizeEventCountWindow(since)
	span.SetAttributes(attribute.String("event_outbox.window_start", since.Format(time.RFC3339)))

	rows, err := d.Conn.QueryContext(ctx, countEventOutboxByStatusQuery,
		model.EventOutboxStatusDispatched, since)
	if err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to count event outbox entries by status", "count_event_outbox_by_status", err)
	}

	return collectEventOutboxStatusCounts(span, rows, "count_event_outbox_by_status")
}

// collectEventOutboxStatusCounts drains a `(status, row_count)` result set into a map.
func collectEventOutboxStatusCounts(
	span trace.Span,
	rows *sql.Rows,
	operation string,
) (map[string]int64, error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	counts := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			failDatabaseSpan(span, scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan event outbox status count", operation, scanErr)
		}
		counts[status] = count
	}

	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over event outbox status counts", operation, err)
	}

	span.SetAttributes(attribute.Int("event_outbox.status_count", len(counts)))

	return counts, nil
}

// AuditEventRecordsInIntervals classifies every row that claims a Kafka record against
// the MEASURED offset windows of the partitions those records live in.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - intervals []model.PartitionOffsetInterval: the measured windows, from
//     TopicOffsetReport.PartitionIntervals().
//
// Returns:
//   - model.EventRecordIntervalAudit: the classification and the instants it covers.
//   - error: a logged internal error carrying no driver detail.
func (d Datasource) AuditEventRecordsInIntervals(
	ctx context.Context,
	intervals []model.PartitionOffsetInterval,
) (model.EventRecordIntervalAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditEventRecordsInIntervals")
	defer span.End()

	audit := model.EventRecordIntervalAudit{MeasuredAt: time.Now().UTC()}

	// The windows are passed as four parallel arrays and zipped by unnest, so one
	// statement serves any number of partitions with a fixed parameter count. Built with
	// make rather than left nil, because lib/pq renders a nil slice as SQL NULL and
	// unnest(NULL) yields no rows at all — which would classify correctly by accident
	// here, but would break the moment the join was used in the other direction.
	topics := make([]string, 0, len(intervals))
	partitions := make([]int64, 0, len(intervals))
	firstOffsets := make([]int64, 0, len(intervals))
	endOffsets := make([]int64, 0, len(intervals))
	for _, interval := range intervals {
		topic := strings.TrimSpace(interval.Topic)
		if topic == "" || interval.Partition < 0 {
			// A window with no topic or a negative partition cannot match any stored
			// coordinate, and passing it would only make the arrays longer. Dropping it here
			// keeps "unmeasured" meaning what it says.
			continue
		}

		first := interval.FirstOffset
		if first < 0 {
			// A broker that could not report the earliest retained offset is reported as zero
			// rather than as a negative sentinel, which is the widest window the measurement
			// supports and therefore the reading that cannot manufacture an aged-out row that is
			// not one.
			first = 0
		}

		topics = append(topics, topic)
		partitions = append(partitions, int64(interval.Partition))
		firstOffsets = append(firstOffsets, first)
		endOffsets = append(endOffsets, interval.EndOffset)
	}

	span.SetAttributes(attribute.Int("event_outbox.measured_partitions", len(topics)))

	var oldestTerminalAt, corroboratedFrom, corroboratedTo sql.NullTime

	err := d.Conn.QueryRowContext(ctx, `
		WITH measured AS (
			SELECT *
			FROM unnest($2::text[], $3::bigint[], $4::bigint[], $5::bigint[])
				AS m(topic_name, partition_id, first_offset, end_offset)
		),
		terminal AS (
			SELECT
				kafka_topic,
				kafka_partition,
				kafka_offset,
				COALESCE(kafka_dispatched_at, occurred_at) AS published_at
			FROM blnk.event_outbox
			WHERE kafka_dispatched_at IS NOT NULL OR status = $1
		),
		classified AS (
			SELECT
				t.kafka_topic,
				t.kafka_partition,
				t.kafka_offset,
				t.published_at,
				CASE
					WHEN t.kafka_offset IS NULL              THEN 'unconfirmed'
					WHEN m.topic_name IS NULL                THEN 'unmeasured'
					WHEN t.kafka_offset >= m.end_offset      THEN 'beyond_end'
					WHEN t.kafka_offset <  m.first_offset    THEN 'aged_out'
					ELSE 'corroborated'
				END AS classification
			FROM terminal t
			LEFT JOIN measured m
				ON m.topic_name = t.kafka_topic
				AND m.partition_id = t.kafka_partition
		)
		SELECT
			COUNT(*)                                                       AS published_rows,
			COUNT(*) FILTER (WHERE classification = 'corroborated')        AS corroborated_rows,
			COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
				FILTER (WHERE classification = 'corroborated'
					AND kafka_offset IS NOT NULL)                          AS distinct_corroborated,
			COUNT(*) FILTER (WHERE classification = 'unconfirmed')         AS unconfirmed_rows,
			COUNT(*) FILTER (WHERE classification = 'unmeasured')          AS unmeasured_rows,
			COUNT(*) FILTER (WHERE classification = 'aged_out')            AS aged_out_rows,
			COUNT(*) FILTER (WHERE classification = 'beyond_end')          AS beyond_end_rows,
			MIN(published_at)                                              AS oldest_terminal_at,
			MIN(published_at) FILTER (WHERE classification = 'corroborated') AS corroborated_from,
			MAX(published_at) FILTER (WHERE classification = 'corroborated') AS corroborated_to
		FROM classified
	`,
		model.EventOutboxStatusDeadLettered,
		pq.Array(topics), pq.Array(partitions), pq.Array(firstOffsets), pq.Array(endOffsets),
	).Scan(
		&audit.PublishedRows,
		&audit.CorroboratedRows,
		&audit.DistinctCorroboratedRecords,
		&audit.UnconfirmedRows,
		&audit.UnmeasuredRows,
		&audit.AgedOutRows,
		&audit.BeyondEndRows,
		&oldestTerminalAt,
		&corroboratedFrom,
		&corroboratedTo,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventRecordIntervalAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker records of published event outbox entries",
			"audit_event_records_in_intervals", err)
	}

	if oldestTerminalAt.Valid {
		audit.OldestTerminalAt = oldestTerminalAt.Time.UTC()
	}
	if corroboratedFrom.Valid {
		audit.CorroboratedFrom = corroboratedFrom.Time.UTC()
	}
	if corroboratedTo.Valid {
		audit.CorroboratedTo = corroboratedTo.Time.UTC()
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.published_rows", audit.PublishedRows),
		attribute.Int64("event_outbox.corroborated_rows", audit.CorroboratedRows),
		attribute.Int64("event_outbox.uncorroborated_rows", audit.UncorroboratedRows()),
		attribute.Int64("event_outbox.beyond_end_rows", audit.BeyondEndRows),
	)

	return audit, nil
}

// ListUndrainedEventTopics groups every row that still owes a publish by the
// destination topic recorded on it. See database.eventOutboxRepository for what the
// audit is for and why "undrained" is the two sets it is rather than simply the
// non-terminal statuses.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//
// Returns:
//   - []model.EventTopicBacklog: one entry per distinct destination topic that still
//     owes work, ordered by the oldest outstanding occurrence first, so the most
//     overdue generation is read first.
//   - error: a logged internal error.
func (d Datasource) ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListUndrainedEventTopics")
	defer span.End()

	// The status sets are spelled from the model's own literals through parameters, so a
	// status renamed there cannot leave this statement silently matching nothing.
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT
			topic,
			COUNT(*) FILTER (WHERE status NOT IN ($1, $2))          AS undelivered_rows,
			COUNT(*) FILTER (WHERE status = $2)                     AS replayable_rows,
			MIN(occurred_at)                                        AS oldest_occurred_at
		FROM blnk.event_outbox
		WHERE status NOT IN ($1, $3)
		GROUP BY topic
		ORDER BY oldest_occurred_at ASC, topic ASC
	`, model.EventOutboxStatusDispatched, model.EventOutboxStatusDeadLettered,
		model.EventOutboxStatusWebhookPending)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to list the undrained destination topics of the event outbox",
			"list_undrained_event_topics", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	backlogs := make([]model.EventTopicBacklog, 0, len(model.AllEventCategories())*2)
	for rows.Next() {
		var (
			backlog model.EventTopicBacklog
			oldest  sql.NullTime
		)
		if err := rows.Scan(
			&backlog.Topic, &backlog.UndeliveredRows, &backlog.ReplayableRows, &oldest,
		); err != nil {
			failDatabaseSpan(span, err)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to read an undrained destination topic of the event outbox",
				"list_undrained_event_topics", err)
		}
		if strings.TrimSpace(backlog.Topic) == "" {
			continue
		}
		if oldest.Valid {
			backlog.OldestOccurredAt = oldest.Time.UTC()
		}

		backlogs = append(backlogs, backlog)
	}
	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to iterate the undrained destination topics of the event outbox",
			"list_undrained_event_topics", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.undrained_topics", len(backlogs)))

	return backlogs, nil
}

// deadLetterInventoryPredicate builds the WHERE clause shared by the dead-letter
// listing and its count, and appends the values it binds.
func deadLetterInventoryPredicate(
	query model.DeadLetterQuery,
	args []interface{},
) (string, []interface{}) {
	var clause strings.Builder

	args = append(args, model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed)
	clause.WriteString(fmt.Sprintf("WHERE status IN ($%d, $%d)", len(args)-1, len(args)))

	if query.Status != "" {
		args = append(args, query.Status)
		clause.WriteString(fmt.Sprintf(" AND status = $%d", len(args)))
	}
	if query.EventType != "" {
		args = append(args, query.EventType)
		clause.WriteString(fmt.Sprintf(" AND event_type = $%d", len(args)))
	}
	if query.Topic != "" {
		// The ORIGINAL category topic, compared against the stored column. That comparison is
		// exact rather than a best effort: topic is NOT NULL and carries CHECK (btrim(topic)
		// <> ''), so no stored row can hold a blank the service would have to substitute an
		// event-type derivation for.
		args = append(args, query.Topic)
		clause.WriteString(fmt.Sprintf(" AND topic = $%d", len(args)))
	}

	// THE SAME OCCURRENCE WINDOW THE LISTING APPLIES. A count drawn from a narrowing the page
	// was not is worse than no count at all: a paging client comparing the two would never
	// terminate, so the window is applied here or in neither place.
	if !query.OccurredFrom.IsZero() {
		args = append(args, query.OccurredFrom.UTC())
		clause.WriteString(fmt.Sprintf(" AND occurred_at >= $%d", len(args)))
	}

	if !query.OccurredTo.IsZero() {
		args = append(args, query.OccurredTo.UTC())
		clause.WriteString(fmt.Sprintf(" AND occurred_at <= $%d", len(args)))
	}

	return clause.String(), args
}

// CountDeadLetterInventory counts the entries a listing query matches, ignoring its
// page. Both dead-letter listings clamp their own limit against
// defaultDeadLetterPageSize and maxDeadLetterPageSize inline, and the inventory listing
// pages by CURSOR rather than by offset.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - query model.DeadLetterQuery: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: how many entries match. Zero is a valid, successful answer.
//   - error: the repository's typed error.
func (d Datasource) CountDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterQuery,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CountDeadLetterInventory")
	defer span.End()

	return countDeadLetterInventory(ctx, d.Conn, span, query)
}

// countDeadLetterInventory is the count, parameterised by the connection it runs on,
// for the same reason listDeadLetterInventory is: the paired read must count with the
// predicate the page was drawn with, not with one that resembles it.
func countDeadLetterInventory(
	ctx context.Context,
	conn sqlQueryer,
	span trace.Span,
	query model.DeadLetterQuery,
) (int64, error) {
	span.SetAttributes(attribute.Bool("event_outbox.filtered", query.HasFilters()))

	where, args := deadLetterInventoryPredicate(query, make([]interface{}, 0, 5))

	var total int64
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM blnk.event_outbox
		`+where, args...).Scan(&total)
	if err != nil {
		failDatabaseSpan(span, err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to count the dead-letter inventory", "count_dead_letter_inventory", err)
	}

	span.SetAttributes(attribute.Int64("event_outbox.dead_letter_total", total))

	return total, nil
}

// ListAndCountDeadLetterInventory returns one page of the inventory AND how many
// entries the same narrowing matches, both read from a single snapshot.
//
// Parameters:
//   - ctx context.Context: cancels the transaction.
//   - query model.DeadLetterInventoryQuery: the narrowing and the page.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page, as ListDeadLetterInventory.
//   - int64: how many entries the narrowing matches in the same snapshot.
//   - error: the repository's typed error.
func (d Datasource) ListAndCountDeadLetterInventory(
	ctx context.Context,
	query model.DeadLetterInventoryQuery,
) (model.DeadLetterInventoryPage, int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListAndCountDeadLetterInventory")
	defer span.End()

	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		failDatabaseSpan(span, err)

		return model.DeadLetterInventoryPage{}, 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the dead-letter inventory", "list_and_count_dead_letter_inventory", err)
	}

	// ROLLED BACK UNCONDITIONALLY, never committed. Nothing was written, so there is
	// nothing to commit, and a rollback releases the snapshot on every path including the
	// error ones. The result is already in hand by then, so a rollback failure is logged
	// rather than returned: discarding a correct answer over a condition the caller cannot
	// act on would be the worse outcome.
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			withLoggableCause(nil, rollbackErr).Error(
				"failed to roll back the dead-letter inventory snapshot")
		}
	}()

	page, err := listDeadLetterInventory(ctx, tx, span, query)
	if err != nil {
		return model.DeadLetterInventoryPage{}, 0, err
	}

	// The page's own narrowing, with the cursor and the limit dropped: which matches to return is
	// not a question about how many there are.
	total, err := countDeadLetterInventory(ctx, tx, span, query.FilterQuery())
	if err != nil {
		return model.DeadLetterInventoryPage{}, 0, err
	}

	return page, total, nil
}

// AuditTerminalEventRecords counts, over a window, how many terminal rows claim a
// broker record and how many of those records are distinct. It is the repository half of
// the zero-loss reconciliation: the three counts are what a comparison against the
// broker's own offsets is drawn from.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//
// Returns:
//   - model.EventOutboxAudit: the counts and the instant they were read.
//   - error: a logged internal error.
func (d Datasource) AuditTerminalEventRecords(ctx context.Context, since time.Time) (model.EventOutboxAudit, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "AuditTerminalEventRecords")
	defer span.End()

	since = normalizeEventCountWindow(since)
	span.SetAttributes(attribute.String("event_outbox.window_start", since.Format(time.RFC3339)))

	audit := model.EventOutboxAudit{MeasuredAt: time.Now().UTC(), WindowStart: since}

	// THE FILTER ON THE DISTINCT COUNT IS REQUIRED, not defensive.
	err := d.Conn.QueryRowContext(ctx, auditTerminalEventRecordsQuery,
		model.EventOutboxStatusDeadLettered, since).Scan(
		&audit.PublishedRows, &audit.ConfirmedRows, &audit.DistinctRecords,
	)
	if err != nil {
		failDatabaseSpan(span, err)

		return model.EventOutboxAudit{}, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to audit the broker records of published event outbox entries",
			"audit_terminal_event_records", err)
	}

	span.SetAttributes(
		attribute.Int64("event_outbox.published_rows", audit.PublishedRows),
		attribute.Int64("event_outbox.confirmed_rows", audit.ConfirmedRows),
		attribute.Int64("event_outbox.unconfirmed_rows", audit.UnconfirmedRows()),
	)

	return audit, nil
}

// scanDeadLetterInventoryEntry decodes one narrow inventory row.
func scanDeadLetterInventoryEntry(s eventOutboxScanner) (model.DeadLetterInventoryEntry, error) {
	var entry model.DeadLetterInventoryEntry
	var ledgerID, lastError, dltTopic sql.NullString
	var firstAttemptedAt, lastAttemptedAt sql.NullTime
	var failureMetadata []byte
	var payloadBytes sql.NullInt64

	if err := s.Scan(
		&entry.ID,
		&entry.EventID,
		&entry.EventType,
		&entry.AggregateID,
		&entry.PartitionKey,
		&ledgerID,
		&entry.Topic,
		&entry.SchemaVersion,
		&entry.OccurredAt,
		&entry.Status,
		&entry.Attempts,
		&lastError,
		&firstAttemptedAt,
		&lastAttemptedAt,
		&dltTopic,
		&failureMetadata,
		&payloadBytes,
	); err != nil {
		return model.DeadLetterInventoryEntry{}, err
	}

	entry.LedgerID = ledgerID.String
	entry.LastError = lastError.String
	entry.DLTTopic = dltTopic.String
	entry.PayloadBytes = int(payloadBytes.Int64)

	if firstAttemptedAt.Valid {
		instant := firstAttemptedAt.Time
		entry.FirstAttemptedAt = &instant
	}
	if lastAttemptedAt.Valid {
		instant := lastAttemptedAt.Time
		entry.LastAttemptedAt = &instant
	}

	// Assigned only when present, so SQL NULL stays a nil RawMessage rather than becoming
	// the four bytes "null" — which a reader would then decode into a zero-valued record and
	// report as metadata that was captured and happened to be empty.
	if len(failureMetadata) > 0 {
		entry.FailureMetadata = append(json.RawMessage(nil), failureMetadata...)
	}

	return entry, nil
}

// oldestDeadLetterAgeByTopicQuery reports the oldest outstanding entry per dead-letter
// topic as ONE grouped aggregate.
//
// THE AGE ANCHOR DEPENDS ON WHETHER THE ROW HAS REACHED A TOPIC, and it has to, because
// the two states are cleared by different actors and only one of them can be trusted to
// leave a timestamp alone.
//
//   - A row that HAS been preserved (dlt_topic set) is measured from last_attempted_at:
//     the moment it was given up on, and therefore the moment it started waiting for a
//     human. Nothing touches that column again — the claim statements that do all
//     require dlt_topic IS NULL — so the clock runs.
//
//   - A row that is still OWED its dead-letter write (dlt_topic NULL) is measured from
//     first_attempted_at instead. The repair pass re-claims exactly these rows on every
//     poll tick and stamps last_attempted_at = NOW() as it does, so anchoring them there
//     RESET THE CLOCK ON EVERY ATTEMPT: the age of a row nothing could preserve stayed
//     pinned at a few tens of seconds, DeadLetterMessageStuck could never fire for it,
//     and the one class of event that exists in no Kafka topic at all was the one class
//     the alert was blind to. first_attempted_at is stamped once
//     (COALESCE(first_attempted_at, NOW())) and never rewritten, so the debt ages
//     monotonically and the process failing to discharge it cannot hide it.
//
// occurred_at remains the fallback in both arms, so a row with no attempt timestamp at
// all still ages — from something older, which errs toward reporting a problem.
const oldestDeadLetterAgeByTopicQuery = `
		SELECT
			COALESCE(NULLIF(dlt_topic, ''), topic || $3) AS age_topic,
			MIN(
				CASE
					WHEN NULLIF(dlt_topic, '') IS NULL THEN COALESCE(first_attempted_at, occurred_at)
					ELSE COALESCE(last_attempted_at, occurred_at)
				END
			)        AS oldest,
			COUNT(*) AS outstanding
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		GROUP BY age_topic
	`

// OldestDeadLetterAgeByTopic reports, per dead-letter topic, the age instant of the
// oldest entry still outstanding and how many are outstanding there.
//
// Parameters:
//   - ctx context.Context: cancels the aggregate.
//   - deadLetterSuffix string: the `.dlt` suffix used to name the sibling topic of a
//     row whose dead-letter write has not happened yet.
//
// Returns:
//   - []model.DeadLetterTopicAge: one entry per topic holding something, empty when
//     nothing is outstanding — which is the healthy state and must be reported as a
//     reading rather than as an absence.
//   - error: a logged internal error.
func (d Datasource) OldestDeadLetterAgeByTopic(
	ctx context.Context,
	deadLetterSuffix string,
) ([]model.DeadLetterTopicAge, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "OldestDeadLetterAgeByTopic")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, oldestDeadLetterAgeByTopicQuery,
		model.EventOutboxStatusDeadLettered, model.EventOutboxStatusFailed, deadLetterSuffix)
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read the oldest dead-letter age per topic", "oldest_dead_letter_age_by_topic", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	ages := make([]model.DeadLetterTopicAge, 0, len(model.AllEventCategories()))
	for rows.Next() {
		var age model.DeadLetterTopicAge
		var oldest sql.NullTime

		if scanErr := rows.Scan(&age.Topic, &oldest, &age.Outstanding); scanErr != nil {
			failDatabaseSpan(span, scanErr)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan a dead-letter age row", "oldest_dead_letter_age_by_topic", scanErr)
		}

		// MIN over a group that exists cannot be NULL here, because the COALESCE in the
		// query falls back to a NOT NULL column. Scanned through a nullable time all the
		// same, so a future column change cannot turn a reading into a scan error.
		if oldest.Valid {
			age.Oldest = oldest.Time.UTC()
		}

		ages = append(ages, age)
	}

	if err = rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Error iterating over dead-letter age rows", "oldest_dead_letter_age_by_topic", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.dead_letter_topics", len(ages)))

	return ages, nil
}

// countEventOutboxByStatusQuery counts every row that is NOT terminally delivered
// exactly, and the terminally delivered rows only inside the caller's window.
const countEventOutboxByStatusQuery = `
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status <> $1
		GROUP BY status
		UNION ALL
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status = $1 AND occurred_at >= $2
		GROUP BY status
	`

// countUnresolvedEventOutboxQuery is countEventOutboxByStatusQuery's FIRST ARM ALONE:
const countUnresolvedEventOutboxQuery = `
		SELECT status, COUNT(*) AS row_count
		FROM blnk.event_outbox
		WHERE status <> $1
		GROUP BY status
	`

// defaultEventCountWindow is the window applied when a caller asks for none.
const defaultEventCountWindow = 24 * time.Hour

// normalizeEventCountWindow turns a caller's window start into a usable one.
func normalizeEventCountWindow(since time.Time) time.Time {
	now := time.Now().UTC()
	if since.IsZero() || since.After(now) {
		return now.Add(-defaultEventCountWindow)
	}

	return since.UTC()
}

// auditTerminalEventRecordsQuery counts the rows inside the window that claim a broker
// record, and how many of them name it.
const auditTerminalEventRecordsQuery = `
		SELECT
			COUNT(*)            AS published_rows,
			COUNT(kafka_offset) AS confirmed_rows,
			COUNT(DISTINCT (kafka_topic, kafka_partition, kafka_offset))
				FILTER (WHERE kafka_offset IS NOT NULL) AS distinct_records
		FROM blnk.event_outbox
		WHERE kafka_dispatched_at >= $2
		   OR (status = $1 AND last_attempted_at >= $2)
	`

// listDeadLetterInventoryQuery pages the dead-letter inventory with every predicate
// applied in SQL and the page bounded by a KEYSET rather than an offset.
const deadLetterInventoryColumns = `id, event_id, event_type, aggregate_id, partition_key, ledger_id, topic, ` +
	`schema_version, occurred_at, status, attempts, last_error, first_attempted_at, last_attempted_at, ` +
	`dlt_topic, failure_metadata, octet_length(payload_raw) AS payload_bytes`

const listDeadLetterInventoryQuery = `
		SELECT ` + deadLetterInventoryColumns + `
		FROM blnk.event_outbox
		WHERE status IN ($1, $2)
		  AND ($3 = '' OR status = $3)
		  AND ($4 = '' OR event_type = $4)
		  AND ($5 = '' OR topic = $5)
		  AND ($6::timestamptz IS NULL OR (occurred_at, id) < ($6::timestamptz, $7::bigint))
		  AND ($8::timestamptz IS NULL OR occurred_at >= $8::timestamptz)
		  AND ($9::timestamptz IS NULL OR occurred_at <= $9::timestamptz)
		ORDER BY occurred_at DESC, id DESC
		LIMIT $10
	`

// ListDeadLetteredEvents pages the dead-letter inventory behind the dead-letter API.
//
// Parameters:
//   - ctx context.Context: request context.
//   - eventIDs []string: the ids to test. Empty or all-blank returns an empty map with
//     no query.
//
// Returns:
//   - map[string]struct{}: the ids that exist. Never nil on success.
//   - error: a typed internal error when the query or scan fails.
func (d Datasource) ExistingEventIDs(ctx context.Context, eventIDs []string) (map[string]struct{}, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ExistingEventIDs")
	defer span.End()

	wanted := make([]string, 0, len(eventIDs))
	for _, id := range eventIDs {
		if strings.TrimSpace(id) != "" {
			wanted = append(wanted, id)
		}
	}

	present := make(map[string]struct{}, len(wanted))
	if len(wanted) == 0 {
		return present, nil
	}

	span.SetAttributes(attribute.Int("event_outbox.requested", len(wanted)))

	// pq.Array keeps this ONE statement with ONE parameter however many ids arrive, so a
	// hundred-transaction batch neither builds a hundred placeholders nor issues a hundred
	// queries. The unique index on event_id serves the lookup.
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT event_id
		FROM blnk.event_outbox
		WHERE event_id = ANY($1)
	`, pq.Array(wanted))
	if err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to look up existing event outbox entries", "existing_event_ids", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logrus.Errorf("Error closing rows: %v", closeErr)
		}
	}()

	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			failDatabaseSpan(span, err)

			return nil, loggedDatabaseError(apierror.ErrInternalServer,
				"Failed to scan an existing event outbox entry", "existing_event_ids", err)
		}
		present[eventID] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		failDatabaseSpan(span, err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to read existing event outbox entries", "existing_event_ids", err)
	}

	span.SetAttributes(attribute.Int("event_outbox.present", len(present)))

	return present, nil
}
