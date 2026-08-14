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

package model

import (
	"time"
)

// CreateWebhookSubscription is the request body for POST
// /subscribers/:subscriber_id/webhook-subscription.
type CreateWebhookSubscription struct {
	// WebhookURL is the legacy HTTP endpoint to record for this subscriber. Required: a
	// record whose only field is absent records nothing. It is the subscriber's own
	// endpoint, written down for migration tracking — see the type comment for why nothing
	// is delivered to it.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the recorded URL.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (c CreateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(c.WebhookURL)
}

// UpdateWebhookSubscription is the request body for PUT
// /subscribers/:subscriber_id/webhook-subscription. It follows the Create/Update split
// this package uses, and carries the same single mutable field, so correcting a
// recorded URL does not have to go through a delete and re-create.
type UpdateWebhookSubscription struct {
	// WebhookURL is the replacement legacy HTTP endpoint. Required for the same reason it
	// is on create, and delivered to for the same reason: none.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// Validate applies the destination policy to the replacement URL.
//
// Returns:
//   - error: describing the violation, nil when the URL is acceptable.
func (u UpdateWebhookSubscription) Validate() error {
	return validateLegacyWebhookURL(u.WebhookURL)
}

// WebhookSubscriptionResponse is the read shape for the legacy webhook-subscription
// routes, returned by the create, read and update calls. The delete route answers 204
// with no body and so needs no shape.
type WebhookSubscriptionResponse struct {
	// SubscriberID is the subscriber the recorded subscription belongs to.
	SubscriberID string `json:"subscriber_id"`

	// WebhookURL is the recorded legacy HTTP endpoint. Omitted when the
	// subscriber has none, which is the normal state after migration.
	WebhookURL string `json:"webhook_url,omitempty"`

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means the subscriber is still counted
	// as awaiting migration.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`
}
