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
	"encoding/json"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// This file holds the API-boundary request and response shapes for the Kafka
// event-streaming endpoints (GET /events/dead-letter,
// POST /events/dead-letter/:event_id/replay and GET /events/stats), for the
// subscriber-management endpoints (POST|GET /subscribers,
// GET|PUT|DELETE /subscribers/:subscriber_id and
// POST /subscribers/:subscriber_id/kafka-credentials), and for the legacy
// webhook-subscription surface that exists only for the 30-day dual-delivery
// window.
//
// Every type below is a passive struct: no constructors, no methods, no
// validation, no normalisation, no conversion. That is deliberate and it
// matches the rest of this package. The behavioural layer lives in model.go;
// any normalisation a handler needs, the handler applies itself or delegates to
// the root package's subscriber service.
//
// Three things are deliberately NOT declared here:
//
//   - No error shape. Error responses are the exclusive business of
//     api/errors.go's respondCode/respondError, which emit
//     {"error", "error_detail"} from a typed code in
//     internal/apierror/codes.go. A DTO carrying an HTTP status or an error
//     code would create a second, competing error contract, and an ad-hoc
//     status would bypass the single source of truth for code-to-status
//     mapping.
//   - No list envelope and no pagination request. api.FilterResponse and
//     api.FilterRequest already exist in api/filter_helper.go, and the
//     handlers wrap the item types below in those. A second envelope here
//     would compete with them.
//   - No consumer-side shapes. Blnk publishes the <topic>.dlt naming
//     convention and builds nothing subscriber-side: no consumer error
//     handling and no subscriber-managed dead-lettering.
//
// The JSON tags are a contract rather than a preference. The handler tests
// assert these shapes over real HTTP, and the tags reuse the column names of
// blnk.event_outbox and blnk.event_subscribers so that an operator reading a
// response sees the same keys as the DDL and the same keys as the root
// model.EventOutbox / model.EventSubscriber entities.
//
// Request types define no custom UnmarshalJSON and rely on Gin's default,
// non-strict JSON binding, which ignores unknown fields. That is required
// rather than incidental: the API-key middleware rewrites every POST body from
// a non-master caller, unmarshalling it to a map and injecting
// meta_data.BLNK_GENERATED_BY before re-marshalling, so every request shape
// here has to tolerate an unexpected meta_data key. No field is declared for
// it, because neither blnk.event_subscribers nor blnk.event_outbox has a
// meta_data column to persist it to, and declaring one would advertise
// persistence that cannot happen.
//
// A note on timestamps, applied consistently below. A nullable instant is
// *time.Time with omitempty, exactly as model.LineageOutbox does for
// processed_at and locked_until; an instant the handler always stamps is a
// plain time.Time with a plain tag. omitempty is deliberately NOT used on any
// non-pointer time.Time, because encoding/json never treats a struct value as
// empty, so the tag would be inert and would advertise an omission that can
// never happen.

// DeadLetterEvent is the item shape returned by GET /events/dead-letter, and
// the read shape for a single dead-lettered event. It is a projection of the
// persisted model.EventOutbox row: the event envelope, the relay's terminal
// state, and the dead-letter record written when the retry budget was spent.
//
// Payload is json.RawMessage, and that is a correctness requirement rather
// than a style choice. The bytes are the marshaled legacy webhook object, the
// two-key {"event": ..., "data": ...} form, carried verbatim with BOTH keys so
// an existing subscriber's body parser keeps working and only the transport
// differs. Decoding them into map[string]interface{} would re-order the keys
// on the way back out, because a Go map marshals with its keys sorted
// alphabetically. That would break the two byte-fidelity guarantees this
// pipeline is built on: the dual-delivery window's promise that the Kafka
// message and the legacy webhook body are identical, and the dead-letter
// replay's promise that a replayed event matches the original. Both hold
// because every reader takes these bytes from the one outbox row and nothing
// transforms them in between; a DTO that decoded and re-encoded the payload
// would be the one thing that broke that.
//
// This type is deliberately not wrapped in a list envelope. The handler
// returns []DeadLetterEvent directly, or nests it in api.FilterResponse when
// the caller asks for a total count.
type DeadLetterEvent struct {
	// EventID is the UUID that uniquely identifies the event. It is the value
	// callers pass to the replay endpoint, and it doubles as the subscriber
	// idempotency key.
	EventID string `json:"event_id"`

	// EventType is the event name, for example "transaction.applied". It
	// duplicates the event name inside Payload, hoisted to the envelope so an
	// operator can triage without parsing the payload.
	EventType string `json:"event_type"`

	// AggregateID identifies the aggregate the event belongs to: the
	// transaction, balance, identity or ledger the mutation acted on.
	AggregateID string `json:"aggregate_id"`

	// LedgerID is the Kafka message key, and therefore what pinned this event
	// to its partition. Optional because not every event category carries a
	// ledger, so it is omitted rather than reported as an empty string.
	LedgerID string `json:"ledger_id,omitempty"`

	// OccurredAt is the instant the domain action happened, RFC3339 on the
	// wire. It is the ordering column throughout the outbox, never created_at.
	OccurredAt time.Time `json:"occurred_at"`

	// SchemaVersion is the envelope schema version the event was written
	// under, starting at 1.
	SchemaVersion int `json:"schema_version"`

	// Topic is the category topic the event was originally destined for, and
	// the topic a replay re-publishes it to.
	Topic string `json:"topic"`

	// DLTTopic is the dead-letter topic the event was actually written to: the
	// "<topic>.dlt" sibling of Topic. Empty, and so omitted, on a row that has
	// not been dead-lettered.
	DLTTopic string `json:"dlt_topic,omitempty"`

	// Status is the relay state machine's durable state, drawn from the
	// model.EventOutboxStatus* vocabulary. Only a dead_lettered row is
	// eligible for replay.
	Status string `json:"status"`

	// Attempts is the number of publish attempts made before the event was
	// dead-lettered.
	Attempts int `json:"attempts"`

	// LastError is the most recent publish failure reason, kept for operator
	// triage. Omitted when the row has never failed.
	LastError string `json:"last_error,omitempty"`

	// FailureMetadata is the dead-letter record attached to the event when its
	// retry budget was spent: the original topic, the error reason, the
	// attempt count, and the first- and last-attempted instants.
	//
	// It is typed as the root model.FailureMetadata rather than a local mirror
	// so that it stays in permanent lockstep with the declaration site and
	// cannot drift to a sixth field or silently lose one. The dead-letter
	// triage runbook reads all five. Being a struct, it also marshals in
	// declaration order, so the key order is deterministic.
	//
	// Nil, and so omitted, for a row that has not been dead-lettered.
	FailureMetadata *model.FailureMetadata `json:"failure_metadata,omitempty"`

	// Payload is the stored event body, passed through untransformed. See the
	// type comment for why this is json.RawMessage and must stay that way.
	Payload json.RawMessage `json:"payload,omitempty"`
}

// ReplayEventResponse is returned by POST /events/dead-letter/:event_id/replay.
//
// Topic is the topic the event was replayed TO, which is its original category
// topic rather than the dead-letter topic it was read from. The response
// deliberately does not echo the payload bytes back: replay fidelity is
// established by what the consumer receives on the original topic, not by the
// body of this acknowledgement, and echoing a payload would invite callers to
// diff the wrong pair of byte strings.
type ReplayEventResponse struct {
	// EventID is the event that was replayed, unchanged from the original.
	EventID string `json:"event_id"`

	// Topic is the original category topic the event was re-published to.
	Topic string `json:"topic"`

	// Status reports the outcome of the re-publish using the
	// model.PublishStatus vocabulary. Omitted when the handler has nothing
	// more specific to add than the 200 itself.
	Status string `json:"status,omitempty"`

	// ReplayedAt is the instant the re-publish was acknowledged.
	ReplayedAt time.Time `json:"replayed_at"`
}

// EventOutboxStatsResponse is returned by GET /events/stats and serves the
// daily zero-loss reconciliation procedure in the operations runbook: the sum
// of dispatched and dead-lettered rows is reconciled against the end offsets
// of the main and dead-letter topics, and the two must agree.
//
// The five per-status counts are explicit fields rather than a map keyed by
// status. That is on purpose: the runbook names each one, and a map can
// silently omit a status whose count happens to be zero, which reads as
// "no such state" rather than "none in that state". All five members of the
// model.EventOutboxStatus* vocabulary are present.
type EventOutboxStatsResponse struct {
	// Pending counts rows written and committed but not yet claimed.
	Pending int64 `json:"pending"`

	// Processing counts rows currently claimed by a relay instance.
	Processing int64 `json:"processing"`

	// Dispatched counts rows the broker has acknowledged. Terminal.
	Dispatched int64 `json:"dispatched"`

	// Failed counts rows whose retry budget is spent but which have not yet
	// been written to a dead-letter topic.
	Failed int64 `json:"failed"`

	// DeadLettered counts rows written to a dead-letter topic. Terminal, and
	// the only state from which an event may be replayed.
	DeadLettered int64 `json:"dead_lettered"`

	// TopicEndOffsets is the per-topic end offset read from the broker, the
	// right-hand side of the reconciliation.
	//
	// The omitempty is mandatory rather than cosmetic. A deployment with no
	// brokers configured is a legitimate steady state, not an error: the
	// publisher resolves to its no-op implementation and no offsets can be
	// read. This endpoint must still serialise cleanly there, reporting the
	// outbox counts it does know and omitting the key entirely rather than
	// emitting a null the reconciliation script would have to special-case.
	TopicEndOffsets map[string]int64 `json:"topic_end_offsets,omitempty"`

	// GeneratedAt is the instant the snapshot was taken. The counts and the
	// offsets are read at slightly different moments under live traffic, so a
	// reconciliation that compares them needs to know when the snapshot was
	// made.
	GeneratedAt time.Time `json:"generated_at"`
}

// CreateSubscriber is the request body for POST /subscribers.
//
// A subscriber is a Kafka principal: registering one records who may consume,
// which topics they are entitled to and under which consumer group, and it is
// the row a later credential issuance attaches its non-reversible reference to.
// Registration and provisioning are separate steps, so a freshly created
// subscriber legitimately holds no credential at all.
//
// Only Name is required. Every other field is either generated by the service
// when omitted (the subscriber ID, the Kafka principal and the consumer group
// are all derivable from the subscriber's identity) or genuinely optional. Name
// cannot be derived: it is the human label an operator recognises the principal
// by months later, it is NOT NULL in blnk.event_subscribers, and marking it
// required here matches CreateAPIKeyRequest, the closest analogue in this
// package.
//
// AuthorizedTopics is intentionally not required. The column defaults to the
// empty array, which means a subscriber registered without an explicit grant is
// authorised for nothing rather than for everything: the registry fails closed,
// and widening a grant is a deliberate follow-up call.
type CreateSubscriber struct {
	// SubscriberID is the business key, and the {id} in
	// POST /subscribers/{id}/kafka-credentials. Omit it to have the service
	// generate one in the repository's "<prefix>_<uuid>" form.
	SubscriberID string `json:"subscriber_id"`

	// Name is the human label the subscriber is triaged by. Required: it is
	// the one field the service cannot invent a meaningful value for.
	Name string `json:"name" binding:"required"`

	// KafkaPrincipal is the SASL/SCRAM username the ACLs are granted to. It is
	// the join key between this registry row and the broker's own
	// authorization state. Omit it to have the service derive it.
	KafkaPrincipal string `json:"kafka_principal"`

	// ConsumerGroupID is the consumer group the subscriber reads under, and the
	// group the provisioned ACL grants Read on with a prefixed pattern type.
	// Omit it to have the service derive it.
	ConsumerGroupID string `json:"consumer_group_id"`

	// AuthorizedTopics is the set of topics this subscriber may Read and
	// Describe, mapping to the authorized_topics TEXT[] column and to the exact
	// set of ACL bindings provisioned for the principal. Omitted or empty means
	// no grant, which is the safe default rather than a missing value.
	AuthorizedTopics []string `json:"authorized_topics"`

	// PartitionKeyPrefix narrows the grant to a key-prefixed slice of the
	// authorised topics. Omit it to grant whole topics, which is a legitimate
	// grant and not an absent one.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// WebhookURL records the legacy HTTP webhook URL a migrating subscriber
	// received pushes on before moving to Kafka. It exists only for the
	// dual-run window and is never required; a subscriber onboarded after the
	// cutover never had one.
	WebhookURL string `json:"webhook_url,omitempty"`
}

// UpdateSubscriber is the request body for PUT /subscribers/:subscriber_id and
// carries the mutable subset of a subscriber only.
//
// Every field is a pointer so that "omitted" is distinguishable from
// "explicitly set to empty", which this shape genuinely needs rather than
// merely benefits from. partition_key_prefix is the clear case: NULL means the
// subscriber is entitled to whole topics, while the empty string would mean
// restricted to the empty prefix, and those are opposite intents. A plain
// string cannot express the difference, so it would make silently inverting an
// operator's intent possible. The nullable columns are represented the same way
// on the root model.EventSubscriber entity, so pointers here are the
// established representation rather than a new convention.
//
// Three groups of fields are deliberately absent:
//
//   - KafkaPrincipal. It is the join key to every ACL binding already
//     provisioned for this subscriber, so changing it would orphan them all
//     and leave the registry claiming access the broker does not grant.
//     Re-pointing a subscriber at a new principal is a re-provisioning
//     operation, not an attribute edit.
//   - CredentialReference and CredentialIssuedAt. Those two together are the
//     record of an issuance, written only by the credential endpoint. A client
//     that could set them could claim an issuance that never happened.
//   - MigratedAt, and any secret of any kind. The service owns stamping the
//     migration instant, and no request shape anywhere accepts a credential.
type UpdateSubscriber struct {
	// Name replaces the human label when present.
	Name *string `json:"name,omitempty"`

	// ConsumerGroupID replaces the consumer group when present. Changing it
	// requires re-provisioning the group ACL before the subscriber can read
	// under the new group.
	ConsumerGroupID *string `json:"consumer_group_id,omitempty"`

	// AuthorizedTopics replaces the whole authorised set when present. It is
	// already nilable as a slice, so no pointer is needed to tell "omitted"
	// from "set to empty": nil is omitted, and a present empty array revokes
	// every topic grant.
	AuthorizedTopics []string `json:"authorized_topics,omitempty"`

	// PartitionKeyPrefix replaces the key-prefix restriction when present. A
	// present empty string clears the restriction back to whole topics; see the
	// type comment for why this cannot be a plain string.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// WebhookURL replaces the recorded legacy webhook URL when present, so an
	// operator can correct a subscriber's dual-run record.
	WebhookURL *string `json:"webhook_url,omitempty"`
}

// SubscriberResponse is the read shape for GET /subscribers,
// GET /subscribers/:subscriber_id, and the bodies returned by the create and
// update routes.
//
// This type deliberately exposes only the NON-REVERSIBLE credential reference
// and the instant of issuance, never the credential itself. That is not merely
// a convention here, it is structurally guaranteed: blnk.event_subscribers has
// no column capable of holding a plaintext or reversibly-encrypted secret, so a
// password field on this type could never be populated from persistence. It
// could only ever leak one. The posture mirrors blnk.api_keys, where the stored
// value is a bcrypt hash and the raw key is never kept.
//
// A nil CredentialIssuedAt is the reliable test for "registered, but no
// credential has ever been issued", which is a real state the registry has to
// represent.
type SubscriberResponse struct {
	// SubscriberID is the business key callers address the subscriber by.
	SubscriberID string `json:"subscriber_id"`

	// Name is the human label the subscriber is triaged by.
	Name string `json:"name"`

	// KafkaPrincipal is the SASL/SCRAM username the subscriber's ACLs are
	// granted to.
	KafkaPrincipal string `json:"kafka_principal"`

	// ConsumerGroupID is the consumer group the subscriber reads under.
	ConsumerGroupID string `json:"consumer_group_id"`

	// AuthorizedTopics is the set of topics the subscriber may Read and
	// Describe. Reported even when empty, because an empty grant is a
	// meaningful, fail-closed state an operator needs to see.
	AuthorizedTopics []string `json:"authorized_topics"`

	// PartitionKeyPrefix is the key-prefix restriction on the grant. Omitted
	// when the subscriber is entitled to whole topics.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// CredentialReference is a non-reversible reference to the issued
	// credential: enough to correlate an issuance with broker state, and not
	// enough to authenticate with. Omitted when no credential has been issued.
	CredentialReference string `json:"credential_reference,omitempty"`

	// CredentialIssuedAt is when the current credential was issued. Nil, and so
	// omitted, when none ever has been.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// WebhookURL is the legacy HTTP webhook URL recorded for the dual-run
	// window. Omitted for a subscriber onboarded after the cutover.
	WebhookURL string `json:"webhook_url,omitempty"`

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means not yet migrated, which is what
	// migration-progress reporting counts.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// CreatedAt is when the subscriber was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the registry row was last written.
	UpdatedAt time.Time `json:"updated_at"`
}

// KafkaCredentialsResponse is returned by
// POST /subscribers/:subscriber_id/kafka-credentials. It hands a subscriber
// everything needed to start consuming: where the brokers are, which topics it
// may read, which consumer group to read under, and the SASL/SCRAM credential
// to authenticate with.
//
// SECURITY: this is the ONLY type in the API surface that carries a password,
// and it must stay that way.
//
// Password is populated exactly once, in the response to the issuing call. It
// is never persisted: blnk.event_subscribers stores only a non-reversible
// reference and the issuance instant, and has no column capable of holding the
// secret. It is therefore not retrievable afterwards by any route, including
// this one re-called, which mints a NEW credential rather than returning the
// old one. A lost password can only be replaced, never recovered. It must never
// be logged, echoed into an error message, or written to a trace attribute.
// This mirrors Blnk's API-key posture, where the stored value is a bcrypt hash
// and the raw key is never kept.
//
// Being response-only, no field carries a binding tag.
type KafkaCredentialsResponse struct {
	// Brokers is the bootstrap broker list the subscriber connects to, as
	// configured for the deployment.
	Brokers []string `json:"brokers"`

	// BrokerEndpoint is a convenience rendering of the same bootstrap list as
	// a single connection string, for clients configured with one endpoint
	// string rather than a list. Omitted when no brokers are configured.
	BrokerEndpoint string `json:"broker_endpoint,omitempty"`

	// AuthorizedTopics is the exact set of topics the issued credential is
	// granted Read and Describe on. Reading anything outside it fails
	// authorization at the broker.
	AuthorizedTopics []string `json:"authorized_topics"`

	// ConsumerGroupID is the consumer group the credential is granted Read on.
	ConsumerGroupID string `json:"consumer_group_id"`

	// Username is the SASL/SCRAM username, i.e. the subscriber's Kafka
	// principal.
	Username string `json:"username"`

	// Password is the generated SASL/SCRAM secret. Returned once, never
	// persisted, never retrievable again, and never to be logged. See the type
	// comment.
	Password string `json:"password"`

	// Mechanism is the SASL mechanism the credential authenticates with,
	// SCRAM-SHA-512.
	Mechanism string `json:"mechanism"`

	// IssuedAt is the instant the credential was minted, matching the
	// credential_issued_at recorded on the subscriber.
	IssuedAt time.Time `json:"issued_at"`
}

// CreateWebhookSubscription is the request body for
// POST /subscribers/:subscriber_id/webhook-subscription.
//
// The route exists because the requirement to migrate existing subscribers off
// the webhook subscription REST API meets a repository in which no such API
// exists: the entire subscription surface today is one global webhook URL in the
// configuration, with no per-subscriber storage and no registration endpoint.
// Recording a legacy URL per subscriber gives an existing subscriber somewhere
// to be recorded and migrated FROM, and it is what makes the sunset behaviour
// observable at all, since without a subscription route there is no request on
// which a 410 could ever be seen.
//
// This is not the /hooks surface. Those are the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts, they carry a response contract that
// can influence transaction processing, they remain fully functional, and they
// have nothing to do with these types.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard and no webhook is delivered. Use
// the Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
type CreateWebhookSubscription struct {
	// WebhookURL is the legacy HTTP endpoint to record for this subscriber.
	// Required: a subscription without a URL has nothing to deliver to.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// UpdateWebhookSubscription is the request body for
// PUT /subscribers/:subscriber_id/webhook-subscription. It follows the
// Create/Update split this package uses, and carries the same single mutable
// field, so correcting a recorded URL does not have to go through a delete and
// re-create.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard and no webhook is delivered. Use
// the Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
type UpdateWebhookSubscription struct {
	// WebhookURL is the replacement legacy HTTP endpoint. Required for the same
	// reason it is on create.
	WebhookURL string `json:"webhook_url" binding:"required"`
}

// WebhookSubscriptionResponse is the read shape for the legacy
// webhook-subscription routes, returned by the create, read and update calls.
// The delete route answers 204 with no body and so needs no shape.
//
// It carries no credential and no headers: the legacy transport's signing secret
// and configured headers are deployment-wide configuration, never per-subscriber
// data, and surfacing them here would turn a migration-tracking response into a
// secret-bearing one.
//
// Deprecated: the legacy webhook-subscription surface exists only for the
// 30-day dual-delivery window during which Kafka publishing and HTTP webhook
// delivery run side by side from the same outbox events. Once
// WEBHOOK_DEPRECATION_SUNSET_DATE has passed, every request to these routes is
// answered with 410 Gone by the sunset guard and no webhook is delivered. Use
// the Kafka event stream and the subscriber credential endpoint instead; see
// docs/webhook-to-kafka-migration.md.
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
