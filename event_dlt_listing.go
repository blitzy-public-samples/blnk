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
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// ListDeadLetterEvents pages the dead-letter inventory an operator triages from.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the page and its optional narrowing.
//
// Returns:
//   - []model.EventOutbox: the matching entries, oldest last. Never nil on success.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) ListDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, error) {
	ctx, span := tracer.Start(ctx, "ListDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return model.DeadLetterInventoryPage{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Listing dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	// THE NARROW PROJECTION, PAGED BY CURSOR. The listing reads the triage coordinate of
	// each entry and the SIZE of its payload rather than the payload itself, so an
	// inventory of large events costs a page of metadata instead of a page of event bodies
	// — and the cursor keeps that cost the same at any depth. The full stored bytes are
	// read only by the replay path, which is the one caller that needs them.
	page, listErr := s.store.ListDeadLetterInventory(ctx, normalized.inventoryQuery())
	if listErr != nil {
		span.RecordError(listErr)

		return model.DeadLetterInventoryPage{}, listErr
	}

	// The repository allocates the slice, but a defensive normalisation keeps the contract
	// true for any future store implementation: a handler marshals this directly and [] is
	// the right empty JSON, not null.
	if page.Entries == nil {
		page.Entries = []model.DeadLetterInventoryEntry{}
	}
	span.SetAttributes(
		attribute.Int("dead_letter.returned", len(page.Entries)),
		attribute.Bool("dead_letter.has_more", page.HasMore),
	)

	return page, nil
}

// ListAndCountDeadLetterEvents returns one page of the inventory together with how many
// entries the same narrowing matches, both drawn from ONE DATABASE SNAPSHOT.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - opts DeadLetterListOptions: the page and its narrowing. The count applies the
//     same narrowing and ignores the page.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page, as ListDeadLetterEvents.
//   - int64: how many entries the narrowing matches in the same snapshot.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) ListAndCountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, int64, error) {
	ctx, span := tracer.Start(ctx, "ListAndCountDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return model.DeadLetterInventoryPage{}, 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Listing dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, 0, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	page, total, err := s.store.ListAndCountDeadLetterInventory(ctx, normalized.inventoryQuery())
	if err != nil {
		span.RecordError(err)

		return model.DeadLetterInventoryPage{}, 0, err
	}

	// Defensive, exactly as in ListDeadLetterEvents: a handler marshals this directly and [] is
	// the right empty JSON, not null.
	if page.Entries == nil {
		page.Entries = []model.DeadLetterInventoryEntry{}
	}

	span.SetAttributes(
		attribute.Int("dead_letter.returned", len(page.Entries)),
		attribute.Bool("dead_letter.has_more", page.HasMore),
		attribute.Int64("dead_letter.total", total),
	)

	return page, total, nil
}

// CountDeadLetterEvents reports how many inventory entries the SAME narrowing matches,
// ignoring the page.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: the number of matching entries; zero when none match.
//   - error: a validation error for an unusable status filter or a reversed occurrence
//     window, or the repository's own typed error.
func (s *EventDeadLetterService) CountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (int64, error) {
	ctx, span := tracer.Start(ctx, "CountDeadLetterEvents")
	defer span.End()

	if s == nil || s.store == nil {
		return 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Counting dead-lettered events requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	normalized, err := normalizeDeadLetterListOptions(opts)
	if err != nil {
		span.RecordError(err)

		return 0, err
	}

	span.SetAttributes(deadLetterListSpanAttributes(normalized)...)

	total, err := s.store.CountDeadLetteredEvents(ctx, normalized.deadLetterQuery())
	if err != nil {
		span.RecordError(err)

		return 0, err
	}

	span.SetAttributes(attribute.Int64("dead_letter.total", total))

	return total, nil
}

// deadLetterListSpanAttributes describes a normalised listing request on a span.
func deadLetterListSpanAttributes(opts DeadLetterListOptions) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.Int("dead_letter.limit", opts.Limit),
		attribute.Int("dead_letter.offset", opts.Offset),
		attribute.Bool("dead_letter.cursor_present", opts.Cursor != nil),
		attribute.Bool("dead_letter.filtered", opts.filtered()),
	}
	if opts.EventType != "" {
		attributes = append(attributes, attribute.String("dead_letter.event_type", opts.EventType))
	}
	if opts.Topic != "" {
		attributes = append(attributes, attribute.String("dead_letter.topic", opts.Topic))
	}
	if opts.Status != "" {
		attributes = append(attributes, attribute.String("dead_letter.status", opts.Status))
	}
	if !opts.OccurredFrom.IsZero() {
		attributes = append(attributes, attribute.String("dead_letter.occurred_from", opts.OccurredFrom.UTC().Format(time.RFC3339Nano)))
	}
	if !opts.OccurredTo.IsZero() {
		attributes = append(attributes, attribute.String("dead_letter.occurred_to", opts.OccurredTo.UTC().Format(time.RFC3339Nano)))
	}

	return attributes
}

// RefreshDeadLetterAgeGauge recomputes and publishes the dead-letter age gauge.
//
// Parameters:
//   - ctx context.Context: cancels the counts, the walk and the gauge recording.
//
// Returns:
//   - DeadLetterAgeReport: the ages the gauge was set from. Its map is never nil on
//     success.
//   - error: the repository's own typed error.
func (s *EventDeadLetterService) RefreshDeadLetterAgeGauge(ctx context.Context) (DeadLetterAgeReport, error) {
	ctx, span := tracer.Start(ctx, "RefreshDeadLetterAgeGauge")
	defer span.End()

	if s == nil || s.store == nil {
		return DeadLetterAgeReport{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Refreshing the dead-letter age gauge requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	now := s.now().UTC()
	report := DeadLetterAgeReport{
		GeneratedAt:   now,
		OldestByTopic: make(map[string]time.Duration),
	}

	// Every topic Blnk owns starts at zero, so a topic that has nothing outstanding is
	// actively reported as clear instead of keeping a stale age.
	for _, topic := range AllDeadLetterTopics() {
		report.OldestByTopic[topic] = 0
	}

	ages, err := s.store.OldestDeadLetterAgeByTopic(ctx, DeadLetterTopicSuffix)
	if err != nil {
		span.RecordError(err)

		return DeadLetterAgeReport{}, err
	}

	for _, age := range ages {
		// The topic label must be bounded before it reaches a metric, and it is bounded here
		// rather than at the recording call. Bounding it in one place
		// keeps the returned report and the published series identical, so an operator
		// reading the report and an alert reading the gauge cannot disagree about which topic
		// an age belongs to.
		topic := boundedTopicLabel(age.Topic)

		report.Outstanding += age.Outstanding

		elapsed := now.Sub(age.Oldest)
		if elapsed < 0 {
			// Clock skew, or an occurrence dated in the future. A negative age would read
			// as "newer than now" and would silently lower the maximum.
			elapsed = 0
		}

		if elapsed > report.OldestByTopic[topic] {
			report.OldestByTopic[topic] = elapsed
		}
	}

	// The pre-dead-letter population, counted from the same aggregate rather than from a
	// second query: a row whose retry budget is spent but whose `<topic>.dlt` write has
	// not landed is grouped under the sibling topic it is BOUND FOR, and its dlt_topic is
	// still NULL. Reporting it separately is what keeps a broker refusing dead-letter
	// writes distinguishable from a busy triage queue.
	failedAwaiting, err := s.countFailedAwaitingDeadLetter(ctx)
	if err != nil {
		span.RecordError(err)

		return DeadLetterAgeReport{}, err
	}
	report.FailedAwaitingDeadLetter = failedAwaiting

	for topic, age := range report.OldestByTopic {
		metrics.DLTOldestMessageAgeSeconds.Record(ctx, age.Seconds(), otelmetric.WithAttributes(
			attribute.String(publishAttrTopic, topic),
		))
	}

	span.SetAttributes(
		attribute.Int64("dead_letter.outstanding", report.Outstanding),
		attribute.Int64("dead_letter.failed_awaiting_dlt", report.FailedAwaitingDeadLetter),
		attribute.Int("dead_letter.topics", len(ages)),
		attribute.Float64("dead_letter.oldest_age_seconds", report.OldestAge().Seconds()),
	)

	return report, nil
}

// countFailedAwaitingDeadLetter reads how many rows have spent their retry budget
// without reaching a dead-letter topic.
func (s *EventDeadLetterService) countFailedAwaitingDeadLetter(ctx context.Context) (int64, error) {
	counts, err := s.store.CountUnresolvedEventOutbox(ctx)
	if err != nil {
		return 0, err
	}

	// A status with no rows is absent from the map rather than present with a zero, so the
	// two-value read is not optional — but the zero value is the correct reading of an
	// absent key here, which is what makes the single-value form safe.
	return counts[model.EventOutboxStatusFailed], nil
}
