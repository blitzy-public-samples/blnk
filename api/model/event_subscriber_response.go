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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/model"
)

// SubscriberResponse is the read shape for GET /subscribers, GET
// /subscribers/:subscriber_id, and the bodies returned by the create and update routes.
type SubscriberResponse struct {
	// SubscriberID is the business key callers address the subscriber by.
	SubscriberID string `json:"subscriber_id"`

	// SubscriberIDHash is the pseudonym this subscriber appears under in METRICS AND LOGS,
	// published here so that a token read off a dashboard, an alert notification or a log
	// line can be resolved back to the subscriber.
	SubscriberIDHash string `json:"subscriber_id_hash"`

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

	// PartitionKeyPrefix is the key-scoped authorization recorded for this subscriber: it
	// is entitled only to records whose message key carries this prefix. Omitted when none
	// is recorded.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// EnforcedAccess declares which parts of this subscriber's access model are actually
	// enforced, and where. It is always present, including when no credential has been
	// issued, because it describes the boundary issuance WILL establish and an integrator
	// needs it before wiring a consumer.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

	// CredentialIssuanceBlocked reports that POST /subscribers/{id}/kafka-credentials will
	// REFUSE for this row as it currently stands. Always present, including when false,
	// because a client that had to infer it from the absence of a field would infer it
	// wrong.
	CredentialIssuanceBlocked bool `json:"credential_issuance_blocked"`

	// CredentialIssuanceBlockedReason names what to change, in the imperative, and is
	// present only when CredentialIssuanceBlocked is true.
	CredentialIssuanceBlockedReason string `json:"credential_issuance_blocked_reason,omitempty"`

	// CredentialFingerprint is a short, non-sensitive digest fragment identifying which
	// credential issuance this row records. It answers "is this the same credential I saw
	// last time?" and nothing else — it cannot be authenticated with, and it is not the
	// stored reference. Empty, and so omitted, when no credential has been issued.
	CredentialFingerprint string `json:"credential_fingerprint,omitempty"`

	// CredentialIssuedAt is when the current credential was issued. Nil, and so
	// omitted, when none ever has been.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// NO webhook_url FIELD. The recorded URL is read through GET
	// /subscribers/:subscriber_id/webhook-subscription, which is fronted by the sunset
	// guard and stops disclosing it at the retirement instant. Echoing it here as well
	// would keep it readable through an unguarded route after that, so the two reads would
	// disagree about whether the legacy surface still exists.

	// MigratedAt is when the subscriber completed its move to Kafka
	// consumption. Nil, and so omitted, means not yet migrated, which is what
	// migration-progress reporting counts.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// ===================================================================
	// the three unsettled-state markers, projected
	//
	// A marker an operator cannot see through the registry is a marker they cannot triage,
	// whatever a runbook says.
	// ===================================================================

	// RevocationPendingAt is when a deregistration began taking this subscriber's
	// broker-side access away. Present means THE ROW IS NOT AN ACTIVE SUBSCRIBER —
	// credential issuance is refused for it — and a principal that may still authenticate
	// is awaiting revocation.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt was refused by the
	// broker, as distinct from a deregistration that merely began. It is cleared at the
	// start of every new attempt, so a present value means no attempt has been made since
	// the refusal.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// CredentialOrphanedAt is when an issuance left a credential at the broker that Blnk
	// could neither record nor revoke. Present means a principal can authenticate while
	// CredentialFingerprint above does not describe the credential that works.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// CreatedAt is when the subscriber was registered.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the registry row was last written.
	UpdatedAt time.Time `json:"updated_at"`

	// RevocationPending reports that broker-side revocation is still owed for this
	// subscriber: the row is tombstoned for deregistration and its principal may still be
	// able to authenticate until the revocation completes. Always present, including when
	// false, because a client that had to infer it from a missing field would infer it
	// wrong.
	RevocationPending bool `json:"revocation_pending"`

	// RevocationPendingReason names what is outstanding and what to do about it. Present
	// only when RevocationPending is true.
	RevocationPendingReason string `json:"revocation_pending_reason,omitempty"`
}

// Enforcement dimensions reported by SubscriberEnforcedAccess.EnforcedBy. They name the
// dimensions enforced on every request a subscriber makes — two of them at the broker's own
// authorizer, and one at the key-authorising component the deployment DECLARED in front of the
// brokers (KAFKA_KEY_SCOPE_ENFORCEMENT). Blnk ships no such component and serves no records
// itself, so the third dimension is listed only on a credential that exists, and such a
// credential is issued only where a component is declared.
const (
	// EnforcementDimensionTopic is Read and Describe bound to an EXACT topic name, so a
	// topic absent from the grant is refused at the broker.
	EnforcementDimensionTopic = "topic"

	// EnforcementDimensionConsumerGroup is Read bound to the subscriber's consumer-group
	// namespace as a PREFIXED pattern, which reserves that namespace to the subscriber and
	// refuses every group outside it.
	EnforcementDimensionConsumerGroup = "consumer_group"
)

// SubscriberEnforcedAccess declares, inside the response body, WHERE each part of a
// subscriber's recorded access model is enforced.
type SubscriberEnforcedAccess struct {
	// EnforcedBy names every dimension Blnk enforces for this subscriber, and is
	// exhaustive.
	EnforcedBy []string `json:"enforced_by"`

	// NotEnforcedBy names every access-shaped dimension this API accepts and NOTHING
	// enforces.
	NotEnforcedBy []string `json:"not_enforced_by"`

	// Topics is the exact set of topics the principal may Read and Describe. Anything
	// outside it is refused at the broker, not filtered by the client.
	Topics []string `json:"topics"`

	// ConsumerGroupNamespace is the group-id prefix reserved to this subscriber. Any group
	// beginning with it is usable; any group outside it is refused. Empty only when the
	// subscriber's consumer group is not derivable, which registration prevents.
	ConsumerGroupNamespace string `json:"consumer_group_namespace,omitempty"`

	// PartitionKeyScopeState is HOW FAR this subscriber's key scope has actually got, and
	// it is the field to read when the booleans below are not enough.
	PartitionKeyScopeState model.SubscriberKeyScopeState `json:"partition_key_scope_state"`

	// PartitionKeyPrefixEnforced answers the one question whose wrong answer is a
	// data-disclosure bug, outright rather than by inference from EnforcedBy.
	PartitionKeyPrefixEnforced bool `json:"partition_key_prefix_enforced"`

	// PartitionKeyPrefix echoes the routing hint recorded on the subscriber, or is empty
	// when none is recorded.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// GatewayDeliveryRequired is TRUE exactly when PartitionKeyPrefixEnforced is, and it
	// is the actionable instruction the rest of this object only implies: CONSUME THROUGH
	// THE KEY-AUTHORISING COMPONENT THE DEPLOYMENT DECLARED, whose address BrokerEndpoint
	// carries, not directly from the Kafka brokers.
	GatewayDeliveryRequired bool `json:"gateway_delivery_required"`

	// BrokerRecordAccess reports whether this subscriber's credential may fetch records
	// DIRECTLY from the broker. It is TRUE exactly when no key scope is recorded.
	BrokerRecordAccess bool `json:"broker_record_access"`

	// PartitionKeyPrefixEnforcedBy names WHICH COMPONENT enforces the key scope, and it is
	// the same fact the booleans above state, said as a place rather than as yes/no
	// answers: "broker_gateway" when a recorded prefix has an enforcement point in this
	// deployment, "none" when no prefix is recorded OR when nothing is declared to keep
	// the one that is. The value is the SAME WORD the deployment declares in
	// KAFKA_KEY_SCOPE_ENFORCEMENT, so a response and the configuration that made it
	// issuable cannot name two different components.
	PartitionKeyPrefixEnforcedBy model.KeyScopeEnforcementStatus `json:"partition_key_prefix_enforced_by"`

	// ExclusiveGrantVerified reports that Blnk READ the principal's complete ACL grant at
	// the broker and found no ALLOW binding outside the set described above.
	ExclusiveGrantVerified bool `json:"exclusive_grant_verified"`

	// Guidance is the REMEDY, carried in the same object as the boundary it describes, and
	// it is always SubscriberKeyScopeGuidance.
	Guidance string `json:"guidance"`
}

// EnforcementDimensionPartitionKey names the access-shaped dimension the BROKER does
// not evaluate: a subscriber's partition-key prefix, enforced by the key-authorising
// component the deployment declared in front of the brokers.
const EnforcementDimensionPartitionKey = "partition_key"

// SubscriberKeyScopeGuidance is the routing instruction every subscriber and credential
// response carries, in SubscriberEnforcedAccess.Guidance.
const SubscriberKeyScopeGuidance = "Kafka authorises whole topics and consumer groups and has " +
	"no message-key dimension, so a subscriber's partition-key prefix is enforced outside the " +
	"broker: a subscriber that records one is granted Describe but NOT Read on its topics, so " +
	"the broker refuses every direct fetch, and its records are delivered by the key-authorising " +
	"component this deployment declared — dial the broker_endpoint in this response, not the " +
	"Kafka brokers. Blnk does not ship that component and serves no records itself, so where " +
	"none is declared a key-scoped subscriber is refused a credential outright rather than " +
	"issued one that can fetch nothing. A subscriber with no prefix consumes directly from the " +
	"broker and is confined by its authorized_topics alone, which means a granted topic is " +
	"readable in full — including records written for other ledgers and other subscribers on " +
	"that topic."

// NewSubscriberEnforcedAccess builds the enforced-access declaration for a subscriber
// whose DEPLOYMENT STATE THE CALLER DOES NOT HOLD.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant.
//   - partitionKeyPrefix string: the recorded prefix, trimmed and echoed verbatim.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with no key-scope enforcement claimed.
func NewSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	return NewSubscriberEnforcedAccessUnder(
		subscriberID, topics, partitionKeyPrefix, model.KeyScopeEnforcementNone,
	)
}

// NewSubscriberEnforcedAccessUnder builds the declaration for a KNOWN enforcement
// point, and it is the form every truthful projection uses.
//
// Parameters:
//   - subscriberID string: used to derive the consumer-group namespace.
//   - topics []string: the exact topics the principal may Read and Describe.
//   - partitionKeyPrefix string: the recorded prefix, trimmed here.
//   - enforcement model.KeyScopeEnforcementStatus: where this DEPLOYMENT can enforce a
//     key scope. Only broker_gateway can make PartitionKeyPrefixEnforced true, and only
//     for a row that records a prefix.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, internally consistent by construction.
func NewSubscriberEnforcedAccessUnder(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
	enforcement model.KeyScopeEnforcementStatus,
) SubscriberEnforcedAccess {
	partitionKeyPrefix = strings.TrimSpace(partitionKeyPrefix)

	// THE ROW'S HALF of the derivation. Trimmed, because a whitespace-only column is not a scope
	// — and because model.EventSubscriber's own predicates trim, so a row that reached here with
	// one must not be reported as narrowed when nothing narrows it.
	keyScoped := partitionKeyPrefix != ""

	// THE DEPLOYMENT'S HALF, and the conjunction is what every enforcement field below is
	// derived from. Two facts, not one: an intent recorded on the row, and a component
	// declared in the environment able to keep it. Neither alone is an enforced boundary,
	// and treating the first as though it were the pair is exactly what made this
	// projection untruthful.
	keyScopeEnforced := keyScoped && enforcement == model.KeyScopeEnforcementGateway

	// THE STATE, resolved by the one derivation in model so a caller reading the scale and a
	// caller reading the booleans cannot be told different things. Issuance upgrades it to
	// attested afterwards; nothing here can, because nothing here made the round trip.
	state := model.SubscriberAccessDeployment{KeyScopeEnforcement: enforcement}.
		KeyScopeStateFor(partitionKeyPrefix)

	enforced := SubscriberEnforcedAccess{
		EnforcedBy: []string{
			EnforcementDimensionTopic,
			EnforcementDimensionConsumerGroup,
		},
		// An explicitly allocated empty slice rather than nil, so the body carries [] instead
		// of null: "nothing is unenforced" is a claim the response makes, and null would read
		// as "not stated". Populated below for the one state in which a dimension really is
		// kept by nobody.
		NotEnforcedBy:          []string{},
		Topics:                 topics,
		PartitionKeyScopeState: state,
		// TRUE only for a recorded prefix WITH a declared enforcement point. The component named
		// below applies it to every record's key before returning one; where none is declared
		// there is nothing to name and nothing to claim.
		PartitionKeyPrefixEnforced: keyScopeEnforced,
		// THE RECORDED VALUE, trimmed, and empty when there is none. It is reported whatever
		// the deployment state, because an operator inspecting a blocked row needs to see the
		// prefix that is blocking it. A sentinel was tried here and removed: this string is
		// the prefix record keys are matched against, so any stand-in for "no restriction"
		// describes a filter that matches nothing.
		PartitionKeyPrefix: partitionKeyPrefix,
		// The transport instruction, and it points somewhere only when there IS somewhere to
		// point. A key-scoped row with nothing declared has neither this nor broker record
		// access, which is the "no usable path yet" state CredentialIssuanceBlocked explains.
		GatewayDeliveryRequired: keyScopeEnforced,
		BrokerRecordAccess:      !keyScoped,
		// The place-shaped form of the same conjunction, so a caller reading the enforcement
		// point and a caller reading the booleans are told the same thing. Derived rather
		// than echoed from the parameter: the parameter is a deployment-wide fact, and a row
		// with no prefix has no key scope for that component to enforce.
		PartitionKeyPrefixEnforcedBy: model.KeyScopeEnforcementNone,
		// THE ROUTING INSTRUCTION, in the same object as the boundary, and the same sentence
		// for every caller of this constructor. Assigned unconditionally: it describes what
		// Kafka's authorizer can evaluate and where Blnk puts the boundary it cannot, neither
		// of which is a property of this row. Its text already covers the undeclared case,
		// which is why there is one sentence rather than one per state.
		Guidance: SubscriberKeyScopeGuidance,
		// FALSE by default, and the default is the honest answer for every caller of this
		// constructor except issuance. This builds the REQUESTED boundary from a registry
		// row, which involves no broker round trip, so nothing here observed what the broker
		// actually grants. Only NewVerifiedSubscriberEnforcedAccess sets it, and only because
		// issuance really did read the grant and refuse a broader one.
		ExclusiveGrantVerified: false,
	}

	// THE TWO LISTS, and which one the key dimension lands in is the response's answer to "is
	// this boundary kept?". Appended here rather than listed above so both are visibly derived
	// from one condition and the dimension cannot appear in both.
	if keyScopeEnforced {
		enforced.PartitionKeyPrefixEnforcedBy = model.KeyScopeEnforcementGateway
		enforced.EnforcedBy = append(enforced.EnforcedBy, EnforcementDimensionPartitionKey)
	} else if keyScoped {
		enforced.NotEnforcedBy = append(enforced.NotEnforcedBy, EnforcementDimensionPartitionKey)
	}

	if namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID); err == nil {
		enforced.ConsumerGroupNamespace = namespace
	}

	if enforced.Topics == nil {
		enforced.Topics = []string{}
	}

	return enforced
}

// NewVerifiedSubscriberEnforcedAccess builds the enforced-access declaration for a
// boundary Blnk has just READ AT THE BROKER and found exclusive.
//
// Parameters:
//   - subscriberID string: the business key the principal and group are derived from.
//   - topics []string: the subscriber's exact topic grant.
//   - partitionKeyPrefix string: the routing hint the credential was issued under.
//
// Returns:
//   - SubscriberEnforcedAccess: the declaration, with ExclusiveGrantVerified true.
func NewVerifiedSubscriberEnforcedAccess(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
) SubscriberEnforcedAccess {
	return NewVerifiedSubscriberEnforcedAccessUnder(
		subscriberID, topics, partitionKeyPrefix, model.KeyScopeEnforcementNone,
	)
}

// NewVerifiedSubscriberEnforcedAccessUnder is the issuance form: a verified grant AND a
// known enforcement point.
//
// Parameters:
//   - subscriberID string: used to derive the consumer-group namespace.
//   - topics []string: the exact topics the principal may Read and Describe.
//   - partitionKeyPrefix string: the recorded prefix.
//   - enforcement model.KeyScopeEnforcementStatus: where the scope is enforced.
//
// Returns:
//   - SubscriberEnforcedAccess: with ExclusiveGrantVerified set, and the key-scope
//     state attested whenever a prefix was issued under a declared enforcement point.
func NewVerifiedSubscriberEnforcedAccessUnder(
	subscriberID string,
	topics []string,
	partitionKeyPrefix string,
	enforcement model.KeyScopeEnforcementStatus,
) SubscriberEnforcedAccess {
	enforced := NewSubscriberEnforcedAccessUnder(subscriberID, topics, partitionKeyPrefix, enforcement)
	enforced.ExclusiveGrantVerified = true

	// THE UPGRADE, and it is conditioned on the state the shared constructor resolved
	// rather than on the parameters again. Available is precisely "a prefix is recorded
	// and this deployment declares a component that can keep it", which is the state
	// issuance had to be in to have asked for an attestation at all — so reading it back
	// is what keeps this from being a second, drifting derivation of the same conjunction.
	if enforced.PartitionKeyScopeState == model.SubscriberKeyScopeStateAvailable {
		enforced.PartitionKeyScopeState = model.SubscriberKeyScopeStateAttested
	}

	return enforced
}

// NewSubscriberResponse projects a stored subscriber into its response shape.
//
// Parameters:
//   - subscriber model.EventSubscriber: the stored registry row.
//   - deployment model.SubscriberAccessDeployment: the resolved configuration this
//     subscriber lives in — where a key scope can be enforced, whether
//     subscriber-facing brokers are advertised, and whether whole-topic access has been
//     declared.
//
// Returns:
//   - SubscriberResponse: the response body, carrying no credential material.
func NewSubscriberResponse(
	subscriber model.EventSubscriber,
	deployment model.SubscriberAccessDeployment,
) SubscriberResponse {
	response := SubscriberResponse{
		SubscriberID: subscriber.SubscriberID,
		// The pivot back from a metric label or a log line. Projected here rather than by the
		// handler for the same reason the credential fingerprint is: a field that must be
		// constructed cannot be omitted by a handler that forgot it, and a subscriber
		// response missing it is a token nothing can resolve.
		SubscriberIDHash:   model.HashIdentifier(subscriber.SubscriberID),
		Name:               subscriber.Name,
		KafkaPrincipal:     subscriber.KafkaPrincipal,
		ConsumerGroupID:    subscriber.ConsumerGroupID,
		AuthorizedTopics:   subscriber.AuthorizedTopics,
		CredentialIssuedAt: subscriber.CredentialIssuedAt,
		MigratedAt:         subscriber.MigratedAt,
		// the unsettled-state markers travel with the row. Copied
		// rather than derived, because each is a durable fact the registry
		// recorded and the response must not soften or summarise it.
		RevocationPendingAt:  subscriber.RevocationPendingAt,
		RevocationFailedAt:   subscriber.RevocationFailedAt,
		CredentialOrphanedAt: subscriber.CredentialOrphanedAt,
		CreatedAt:            subscriber.CreatedAt,
		UpdatedAt:            subscriber.UpdatedAt,
	}

	// AuthorizedTopics is reported even when empty — an empty grant is a meaningful,
	// fail-closed state an operator needs to see — so a nil slice becomes [] rather
	// than null, which a client would otherwise have to special-case.
	if response.AuthorizedTopics == nil {
		response.AuthorizedTopics = []string{}
	}

	// Assembled here rather than by the handler, so every subscriber response states the
	// enforced boundary and none can imply that the advisory key prefix is one.
	if subscriber.PartitionKeyPrefix != nil {
		response.PartitionKeyPrefix = *subscriber.PartitionKeyPrefix
	}

	// AFTER the top-level prefix is resolved, and built FROM it, so the two cannot describe
	// different values. The declaration restates the prefix inside the object that says the
	// broker does not enforce it, which is the only place a reader cannot take it for a boundary.
	response.EnforcedAccess = NewSubscriberEnforcedAccessUnder(
		subscriber.SubscriberID, response.AuthorizedTopics, response.PartitionKeyPrefix,
		deployment.KeyScopeEnforcement,
	)

	// CredentialFingerprint, never the reference. model.CredentialFingerprint yields
	// "" for anything that is not a validly derived reference, so a value mis-stored
	// by some other path is dropped entirely instead of being echoed.
	if subscriber.CredentialReference != nil {
		response.CredentialFingerprint = model.CredentialFingerprint(*subscriber.CredentialReference)
	}

	response.CredentialIssuanceBlocked, response.CredentialIssuanceBlockedReason =
		credentialIssuanceBlock(subscriber, deployment)

	// Assigned as a PAIR, from one reading of the tombstone, so the flag and the reason
	// cannot disagree: a reader that only needs "is this row on its way out" does not have
	// to know that an absent instant means no, and a reader acting on the reason is never
	// handed one for a row that is settled.
	response.RevocationPending, response.RevocationPendingReason =
		outstandingRevocation(subscriber)

	return response
}

// outstandingRevocation reports whether broker-side revocation is still owed for this
// subscriber, and names the remedy when it is.
func outstandingRevocation(subscriber model.EventSubscriber) (bool, string) {
	if !subscriber.IsRevocationPending() {
		return false, ""
	}

	return true, "A broker-side credential revocation is outstanding: this subscriber's SASL " +
		"credential may still authenticate, and its registry row is kept because it names the " +
		"principal that has to be revoked. Retry the deregistration (DELETE this subscriber), " +
		"which is idempotent at the broker, or revoke the principal at the broker by hand."
}

// credentialIssuanceBlock predicts whether POST /subscribers/{id}/kafka-credentials
// will refuse for this row in this deployment, and names the remedy when it will.
func credentialIssuanceBlock(
	subscriber model.EventSubscriber,
	deployment model.SubscriberAccessDeployment,
) (bool, string) {
	if subscriber.IsRevocationPending() {
		return true, "This subscriber is being deregistered and its broker-side revocation is " +
			"still owed, so issuing a credential would re-arm a principal that is on its way out. " +
			"Complete or abandon the deregistration first."
	}

	keyScoped := subscriber.DeclaresKeyScope()
	keyScopeAvailable := deployment.KeyScopeEnforcement == model.KeyScopeEnforcementGateway

	// (2) A RECORDED PREFIX WITH NOTHING TO KEEP IT. Kafka's authorizer has no message-key
	// dimension, so this row's boundary can only be applied by a component in front of the
	// brokers, and this deployment declares none — issuance refuses rather than minting a
	// credential whose declared scope nothing enforces. All three remedies are real, which
	// is why all three are named.
	if keyScoped && !keyScopeAvailable {
		return true, "This subscriber records a partition key prefix and this deployment declares " +
			"no component that can enforce one, so no credential will be issued for it: Kafka " +
			"authorises topics and consumer groups and has no message-key dimension. Declare a " +
			"key-authorising component with KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway together " +
			"with its gateway addresses and attestation endpoint, or clear the partition key " +
			"prefix to accept access to whole topics, or narrow the subscriber's authorized " +
			"topics, which the broker does enforce."
	}

	// (3) THE MIRROR REFUSAL. In a deployment whose subscribers are confined by record key, the
	// one subscriber with no prefix is the one credential that escapes the model — granted literal
	// topic Read while every other principal is confined — so it is refused too.
	if !keyScoped && keyScopeAvailable {
		return true, "This deployment enforces subscriber access by record key, and this " +
			"subscriber records no partition key prefix, so a credential for it would be granted " +
			"whole-topic reads while every other subscriber is confined to its own ledgers. Record " +
			"a partition_key_prefix on this subscriber."
	}

	// (4) THE WHOLE-TOPIC MODEL AS A DECISION RATHER THAN A DEFAULT. Reached only for a
	// prefix-less row outside a key-scoped deployment, which is exactly when the credential would
	// read a granted topic in full.
	if !keyScoped && !deployment.WholeTopicAccessPermitted {
		return true, "This deployment has not declared how subscriber access is scoped, and a " +
			"credential for this subscriber would read every record on each topic it is granted — " +
			"every ledger's, and every other subscriber's. Declare the model once: set " +
			"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true to acknowledge whole-topic subscriber " +
			"reads, or declare the key-scoped model and record a partition key prefix on each " +
			"subscriber."
	}

	if len(subscriber.AuthorizedTopics) == 0 {
		return true, "This subscriber is authorised for no topics, so any credential issued for " +
			"it would be a live Kafka principal that may read nothing. Set authorized_topics to " +
			"the category topics it is entitled to."
	}

	// (6) THE ADDRESS THE SUBSCRIBER WOULD DIAL. There is no fallback to KAFKA_BROKERS —
	// those are the addresses Blnk dials, internal in every real deployment — so issuance
	// answers a typed 503 rather than handing out a one-time secret together with an
	// endpoint nothing outside can reach.
	if !deployment.SubscriberBrokersAdvertised {
		return true, "This deployment advertises no subscriber-facing Kafka brokers, so a " +
			"credential issued now would name no endpoint the subscriber could dial and issuance " +
			"answers 503 instead. Set KAFKA_SUBSCRIBER_BROKERS to the externally advertised " +
			"listener addresses."
	}

	return false, ""
}

// KafkaCredentialsResponse is returned by POST
// /subscribers/:subscriber_id/kafka-credentials. It hands a subscriber everything
// needed to start consuming: where the brokers are, which topics it may read, which
// consumer group to read under, and the SASL/SCRAM credential to authenticate with.
type KafkaCredentialsResponse struct {
	// Brokers is the SUBSCRIBER-FACING bootstrap broker list: the externally advertised
	// addresses this subscriber connects to, from KAFKA_SUBSCRIBER_BROKERS.
	Brokers []string `json:"brokers"`

	// BrokerEndpoint is a convenience rendering of the same subscriber-facing
	// list as a single connection string, for clients configured with one
	// endpoint string rather than a list.
	BrokerEndpoint string `json:"broker_endpoint,omitempty"`

	// AuthorizedTopics is the exact set of topics the issued credential is
	// granted Read and Describe on. Reading anything outside it fails
	// authorization at the broker.
	AuthorizedTopics []string `json:"authorized_topics"`

	// ConsumerGroupID is the consumer group the credential is granted Read on.
	ConsumerGroupID string `json:"consumer_group_id"`

	// EnforcedAccess declares which parts of the subscriber's access model the broker
	// enforces for this credential. It is the field a consumer is configured from: the
	// topic set is exact and the group namespace is reserved, and no key-based filtering
	// is applied by the broker to either.
	EnforcedAccess SubscriberEnforcedAccess `json:"enforced_access"`

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

	// CredentialFingerprint is the short, non-sensitive digest fragment of the stored
	// credential reference — the SAME value GET /subscribers reports for this row once the
	// issuance is recorded.
	CredentialFingerprint string `json:"credential_fingerprint"`

	// Replaced reports that this principal ALREADY held a SCRAM credential and this
	// issuance replaced it.
	Replaced bool `json:"replaced"`
}
