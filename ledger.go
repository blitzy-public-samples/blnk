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

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
)

// postLedgerActions performs some actions after a ledger has been created.
// It sends the newly created ledger to the search index queue, which indexes the ledger in Typesense.
//
// IT NO LONGER CAPTURES THE EVENT, and that is the point rather than an omission. The
// ledger.created row is now inserted INSIDE the transaction that inserts the ledger, by the
// repository, from the preparer ledgerCreatedEventPreparer supplies — so the event and the
// ledger commit together instead of the event being written from a goroutine after the fact.
// Capturing it here as well would publish the same event twice, and event_id is derived from
// the ledger's identity, so the second insert would be refused by the unique index and the
// only visible result would be a logged conflict on every ledger creation.
//
// Indexing stays here because it is genuinely post-commit work: TypeSense is a separate
// system with its own retry queue and nothing about it belongs in a ledger transaction.
//
// # THE ONE CASE IT STILL PUBLISHES
//
// A webhook-only deployment — a webhook URL and no KAFKA_BROKERS — gets NO preparer, because
// capturing rows no relay can drain is what eventCaptureEnabled exists to avoid. Nothing
// captures the event on that shape, so the legacy publish is retained here as a fallback for
// it alone, exactly as postTransactionActions retains one for a transaction its atomic writer
// did not record. Without it this deployment lost ledger.created from BOTH transports.
//
// publishEntityEventWhenUncaptured owns that decision and returns immediately whenever a
// preparer was supplied, so the atomic capture and this call can never both run.
//
// Parameters:
//   - ctx context.Context: the creating request's context. Detached from cancellation before
//     it is handed to the publish, which outlives the request that spawned it.
//   - ledger *model.Ledger: A pointer to the newly created Ledger model.
func (l *Blnk) postLedgerActions(ctx context.Context, ledger *model.Ledger) {
	// Derived outside the goroutine, while ctx is still live, and detached from cancellation
	// for the same reason postBalanceActions detaches: CreateLedger is reached from the API
	// with the request context, which net/http cancels as soon as the handler returns, and
	// the enqueue below would then fail whenever the response won the race.
	publishCtx := context.WithoutCancel(ctx)

	go func() {
		err := l.queue.queueIndexData(ledger.LedgerID, "ledgers", ledger)
		if err != nil {
			notification.NotifyError(err)
		}
		err = l.publishEntityEventWhenUncaptured(publishCtx, ledger.LedgerID, NewWebhook{
			Event:   "ledger.created",
			Payload: ledger,
		}, WithEventLedgerID(ledger.LedgerID))
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

// ledgerCreatedEventPreparer returns the preparer that builds the ledger.created outbox
// row, for the repository to insert INSIDE the transaction that inserts the ledger.
//
// # Why the event is captured through a callback rather than published here
//
// It used to be published from postLedgerActions, in a goroutine, after CreateLedger had
// already committed. That is the window requirement R-2 exists to close: the ledger was
// durable and its event was not, so a crash — or a failed insert — between the two left a
// ledger that no subscriber would ever hear about, with nothing left to replay from. The
// event now commits with the ledger or not at all.
//
// The callback shape is forced by WHERE the ledger id comes from. The repository mints
// ldg_<uuid> and stamps CreatedAt during the insert, and both the payload and the event's
// aggregate id are derived from the finished ledger — so there is no row to prepare before
// the call, and moving id generation into this layer would change domain behaviour this
// change may not touch. See database.EventPreparer.
//
// # The payload and the transport substitution are unchanged
//
// The event string is still "ledger.created" and the payload is still the created
// *model.Ledger, so the bytes recorded in the outbox are the bytes the legacy webhook body
// carried — which is what makes the dual-delivery equivalence verifiable by reading this
// diff. The relay publishes that one row to its topic and, while the window is open,
// enqueues the legacy webhook from the very same row.
//
// WithEventLedgerID states the ledger explicitly rather than leaving it to be derived from
// the payload. The derivation would reach the same value for this event type, and stating
// it is what makes the R-6 partitioning dimension a property of the CALL SITE — the place
// that actually knows which ledger a mutation belonged to — rather than of a type switch
// that has to be kept in step with the payload shapes.
//
// When publishing is not configured the preparer returns (nil, nil) and the repository
// commits the ledger alone, which preserves the no-op-when-unconfigured contract this
// pipeline inherited from SendWebhook.
//
// Parameters:
//   - ctx context.Context: the creating request's context, captured for tracing only. The
//     preparer performs no I/O and cannot be cancelled part-way.
//
// Returns:
//   - database.EventPreparer[model.Ledger]: the preparer to hand to the repository.
func (l *Blnk) ledgerCreatedEventPreparer(ctx context.Context) database.EventPreparer[model.Ledger] {
	// A NIL PREPARER when nothing is configured, so the repository stays on its
	// single-statement path instead of opening a transaction to insert no event. See
	// eventCaptureEnabled.
	if !l.eventCaptureEnabled() {
		return nil
	}

	return func(created model.Ledger) (*model.EventOutbox, error) {
		return l.PrepareEventOutbox(ctx, NewWebhook{
			Event:   "ledger.created",
			Payload: &created,
		}, WithEventLedgerID(created.LedgerID))
	}
}

// CreateLedger creates a new ledger together with its ledger.created event, atomically.
//
// The event preparer is handed to the repository, which inserts the ledger and the event row
// in one transaction (requirement R-2). A failure to prepare or insert the event therefore
// fails the creation, and the caller sees no ledger — which is the correct outcome: a ledger
// whose event was lost is a ledger no subscriber knows exists, and the alternative silently
// trades a visible failure for an invisible one.
//
// postLedgerActions then performs the remaining post-commit work, which is indexing only.
//
// Parameters:
// - ledger: A Ledger model representing the ledger to be created.
//
// Returns:
// - model.Ledger: The created Ledger model.
// - error: An error if the ledger could not be created, or if its event could not be captured.
func (l *Blnk) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	ctx := context.Background()

	ledger, err := l.datasource.CreateLedger(ledger, l.ledgerCreatedEventPreparer(ctx))
	if err != nil {
		return model.Ledger{}, err
	}
	l.postLedgerActions(ctx, &ledger)
	return ledger, nil
}

// GetAllLedgers retrieves all ledgers from the datasource.
// It returns a slice of Ledger models and an error if the operation fails.
//
// Returns:
// - []model.Ledger: A slice of Ledger models.
// - error: An error if the ledgers could not be retrieved.
func (l *Blnk) GetAllLedgers(limit, offset int) ([]model.Ledger, error) {
	return l.datasource.GetAllLedgers(limit, offset)
}

// GetAllLedgersWithFilter retrieves ledgers from the datasource using advanced filters.
// It returns a slice of Ledger models and an error if the operation fails.
//
// Parameters:
// - ctx: Context for the operation.
// - filters: A QueryFilterSet containing filter conditions.
// - limit: Maximum number of ledgers to return.
// - offset: Offset for pagination.
//
// Returns:
// - []model.Ledger: A slice of Ledger models matching the filter criteria.
// - error: An error if the ledgers could not be retrieved.
func (l *Blnk) GetAllLedgersWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Ledger, error) {
	return l.datasource.GetAllLedgersWithFilter(ctx, filters, limit, offset)
}

// GetAllLedgersWithFilterAndOptions retrieves ledgers with filters, sorting, and optional count.
//
// Parameters:
// - ctx: Context for the operation.
// - filters: A QueryFilterSet containing filter conditions.
// - opts: Query options including sorting and count settings.
// - limit: Maximum number of ledgers to return.
// - offset: Offset for pagination.
//
// Returns:
// - []model.Ledger: A slice of Ledger models matching the filter criteria.
// - *int64: Optional total count of matching records (if opts.IncludeCount is true).
// - error: An error if the ledgers could not be retrieved.
func (l *Blnk) GetAllLedgersWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Ledger, *int64, error) {
	return l.datasource.GetAllLedgersWithFilterAndOptions(ctx, filters, opts, limit, offset)
}

// GetLedgerByID retrieves a ledger by its ID from the datasource.
// It returns a pointer to the Ledger model and an error if the operation fails.
//
// Parameters:
// - id: A string representing the ID of the ledger to retrieve.
//
// Returns:
// - *model.Ledger: A pointer to the Ledger model if found.
// - error: An error if the ledger could not be retrieved.
func (l *Blnk) GetLedgerByID(id string) (*model.Ledger, error) {
	return l.datasource.GetLedgerByID(id)
}

// UpdateLedger updates an existing ledger's name.
// It calls postLedgerActions after a successful update to handle indexing and webhooks.
//
// Parameters:
// - id: A string representing the ID of the ledger to update.
// - name: A string representing the new name for the ledger.
//
// Returns:
// - *model.Ledger: A pointer to the updated Ledger model.
// - error: An error if the ledger could not be updated.
func (l *Blnk) UpdateLedger(id, name string) (*model.Ledger, error) {
	ledger, err := l.datasource.UpdateLedger(id, name)
	if err != nil {
		return nil, err
	}
	l.postLedgerActions(context.Background(), ledger)
	return ledger, nil
}
