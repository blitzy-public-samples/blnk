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
	"time"

	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------------------
// Blnk-instance entry points

// EventSubscribers returns a subscriber service bound to this instance's datasource.
func (b *Blnk) EventSubscribers() *EventSubscriberService {
	if b == nil {
		return NewEventSubscriberService(nil, nil)
	}

	// The two seams that make a per-request service behave like a process-scoped one.
	return NewEventSubscriberService(b.datasource, nil).
		WithKafkaAdminResolver(func() (subscriberPrincipalProvisioner, error) {
			admin, err := b.KafkaAdmin()
			if err != nil {
				return nil, err
			}

			// A typed nil pointer in an interface is a non-nil interface that panics on first
			// use, so the interface is only ever built from a client that exists.
			if admin == nil {
				return nil, ErrKafkaAdminNotConfigured
			}

			return admin, nil
		}).
		WithBackgroundScheduler(b.scheduleBackgroundWork)
}

// RegisterEventSubscriber records a new subscriber. It is the write behind POST /subscribers.
func (b *Blnk) RegisterEventSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.RegisterSubscriber(ctx, registration)
}

// GetEventSubscriber reads one subscriber. It is the read behind
// GET /subscribers/:subscriber_id.
func (b *Blnk) GetEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.GetSubscriber(ctx, subscriberID)
}

// ListEventSubscribers pages the registry. It is the read behind GET /subscribers.
func (b *Blnk) ListEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ListSubscribers(ctx, query)
}

// ListAndCountEventSubscribers pages the registry and counts it from one snapshot. It
// is the read behind GET /subscribers?include_count=true.
func (b *Blnk) ListAndCountEventSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ListAndCountSubscribers(ctx, query)
}

// CountEventSubscribers counts the registry. It is the read behind a standalone count.
func (b *Blnk) CountEventSubscribers(ctx context.Context) (int64, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.CountSubscribers(ctx)
}

// UpdateEventSubscriber applies the mutable subset of a subscriber. It is the write
// behind PUT /subscribers/:subscriber_id.
func (b *Blnk) UpdateEventSubscriber(
	ctx context.Context,
	subscriberID string,
	changes SubscriberUpdate,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.UpdateSubscriber(ctx, subscriberID, changes)
}

// DeregisterEventSubscriber removes a subscriber and revokes its Kafka access. It is the write
// behind DELETE /subscribers/:subscriber_id.
func (b *Blnk) DeregisterEventSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.DeregisterSubscriber(ctx, subscriberID)
}

// IssueSubscriberKafkaCredentials mints a subscriber's SASL/SCRAM credential. It is the
// write behind POST /subscribers/:subscriber_id/kafka-credentials.
func (b *Blnk) IssueSubscriberKafkaCredentials(
	ctx context.Context,
	subscriberID string,
) (SubscriberCredential, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.IssueSubscriberCredential(ctx, subscriberID)
}

// RevokeSubscriberKafkaCredentials ends a subscriber's Kafka access while leaving it
// registered.
func (b *Blnk) RevokeSubscriberKafkaCredentials(ctx context.Context, subscriberID string) error {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.RevokeSubscriberCredential(ctx, subscriberID)
}

// RecordSubscriberWebhookSubscription records a migrating subscriber's legacy endpoint.
func (b *Blnk) RecordSubscriberWebhookSubscription(
	ctx context.Context,
	subscriberID, webhookURL string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.RecordLegacyWebhookSubscription(ctx, subscriberID, webhookURL)
}

// ClearSubscriberWebhookSubscription removes a subscriber's recorded legacy endpoint. It is
// the write behind DELETE /subscribers/:subscriber_id/webhook-subscription.
func (b *Blnk) ClearSubscriberWebhookSubscription(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ClearLegacyWebhookSubscription(ctx, subscriberID)
}

// CompleteEventSubscriberWebhookMigration retires a subscriber's legacy endpoint and
// records the migration in ONE transition. It is the write behind DELETE
// /subscribers/:subscriber_id/webhook-subscription.
func (b *Blnk) CompleteEventSubscriberWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.CompleteWebhookMigration(ctx, subscriberID)
}

// MarkEventSubscriberMigrated records that a subscriber has completed its move to Kafka
// consumption WITHOUT touching its legacy endpoint.
func (b *Blnk) MarkEventSubscriberMigrated(ctx context.Context, subscriberID string) (time.Time, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.MarkSubscriberMigrated(ctx, subscriberID)
}

// CompleteSubscriberWebhookMigration forgets a subscriber's legacy endpoint and records
// its migration in ONE write. It is the write behind DELETE
// /subscribers/:subscriber_id/webhook-subscription.
func (b *Blnk) CompleteSubscriberWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.CompleteWebhookMigration(ctx, subscriberID)
}

// PurgeMigratedSubscriberWebhookURLs forgets the legacy endpoints of subscribers that migrated
// before a cut-off.
func (b *Blnk) PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.PurgeMigratedWebhookURLs(ctx, migratedBefore)
}

// closeEventSubscriberService closes a service built for one operation, logging rather
// than propagating a close failure.
func closeEventSubscriberService(service *EventSubscriberService) {
	if err := service.Close(); err != nil {
		kafkaErrorEntry("close_subscriber_management_admin", err).Warn(
			"closing the short-lived Kafka administrative client for subscriber management failed",
		)
	}
}
