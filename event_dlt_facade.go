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

	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------
// Blnk-instance entry points

// EventDeadLetters returns a dead-letter service bound to this instance's datasource.
//
// Returns:
//   - *EventDeadLetterService: a ready service the caller owns.
func (b *Blnk) EventDeadLetters() *EventDeadLetterService {
	if b == nil {
		return NewEventDeadLetterService(nil, nil)
	}

	return NewEventDeadLetterService(b.datasource, nil)
}

// ListDeadLetterEvents pages the dead-letter inventory. It is the read behind GET
// /events/dead-letter.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the page and its optional narrowing.
//
// Returns:
//   - []model.EventOutbox: the matching entries.
//   - error: as EventDeadLetterService.ListDeadLetterEvents.
func (b *Blnk) ListDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, error) {
	return b.EventDeadLetters().ListDeadLetterEvents(ctx, opts)
}

// ListAndCountDeadLetterEvents pages the inventory and counts it from one snapshot. It
// is the read behind GET /events/dead-letter?include_count=true.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - opts DeadLetterListOptions: the page and its narrowing.
//
// Returns:
//   - model.DeadLetterInventoryPage: the page.
//   - int64: the total the same narrowing matches, as at that page.
//   - error: as EventDeadLetterService.ListAndCountDeadLetterEvents.
func (b *Blnk) ListAndCountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (model.DeadLetterInventoryPage, int64, error) {
	return b.EventDeadLetters().ListAndCountDeadLetterEvents(ctx, opts)
}

// CountDeadLetterEvents counts the inventory a listing with the same options pages
// through. It is the total behind a standalone count.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - opts DeadLetterListOptions: the narrowing. Limit and Offset are ignored.
//
// Returns:
//   - int64: the number of matching entries.
//   - error: as EventDeadLetterService.CountDeadLetterEvents.
func (b *Blnk) CountDeadLetterEvents(
	ctx context.Context,
	opts DeadLetterListOptions,
) (int64, error) {
	return b.EventDeadLetters().CountDeadLetterEvents(ctx, opts)
}

// ReplayDeadLetteredEvent replays one dead-lettered event to its original topic. It is
// the write behind POST /events/dead-letter/:event_id/replay.
//
// Parameters:
//   - ctx context.Context: cancels the lookup, the publish and the recording.
//   - eventID string: the event's UUID.
//
// Returns:
//   - ReplayOutcome: the record of the replay.
//   - error: as EventDeadLetterService.ReplayDeadLetteredEvent.
func (b *Blnk) ReplayDeadLetteredEvent(ctx context.Context, eventID string) (ReplayOutcome, error) {
	service := b.EventDeadLetters()
	defer closeDeadLetterService(service)

	return service.ReplayDeadLetteredEvent(ctx, eventID)
}

// RefreshDeadLetterAgeGauge recomputes and publishes the dead-letter age gauge for this
// instance. It is what the statistics endpoint and any periodic caller invoke.
//
// Parameters:
//   - ctx context.Context: cancels the counts and the walk.
//
// Returns:
//   - DeadLetterAgeReport: the ages the gauge was set from.
//   - error: as EventDeadLetterService.RefreshDeadLetterAgeGauge.
func (b *Blnk) RefreshDeadLetterAgeGauge(ctx context.Context) (DeadLetterAgeReport, error) {
	return b.EventDeadLetters().RefreshDeadLetterAgeGauge(ctx)
}

// closeDeadLetterService closes a service built for one operation, logging rather than
// propagating a close failure.
func closeDeadLetterService(service *EventDeadLetterService) {
	if err := service.Close(); err != nil {
		withLoggableCause(nil, err).Warn("closing the short-lived dead-letter event publisher failed")
	}
}
