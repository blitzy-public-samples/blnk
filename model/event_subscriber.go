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
	"errors"
	"fmt"
	"strings"
	"time"
)

// EventSubscriber is one row of blnk.event_subscribers: a registered Kafka subscriber,
// the access boundary provisioned for it, and — during the dual-run only — the legacy
// webhook URL it is being migrated away from.
type EventSubscriber struct {
	// ID is the BIGSERIAL surrogate primary key, assigned by the database. The
	// key callers use is SubscriberID.
	ID int64 `json:"id"`

	// --- Subscriber identity ---

	// SubscriberID is the business key and the {id} in POST
	// /subscribers/{id}/kafka-credentials. It is a '<prefix>_<uuid>' string produced by
	// GenerateUUIDWithSuffix, exactly as every other business key in this schema is.
	SubscriberID string `json:"subscriber_id"`
	// Name is the human label an operator recognises the subscriber by. It is required,
	// because an unnamed principal cannot be triaged — being able to answer "who is this
	// principal?" months later is most of the reason the registry exists.
	Name string `json:"name"`

	// --- The Kafka access model ---

	// KafkaPrincipal is the SASL/SCRAM username the ACLs are granted to.
	KafkaPrincipal string `json:"kafka_principal"`
	// ConsumerGroupID is the consumer group the subscriber reads under, returned verbatim
	// by the credential endpoint. The provisioned ACL grants Read on it with a prefixed
	// pattern type, reserving the subscriber's whole group namespace without enumerating
	// every group it might create.
	ConsumerGroupID string `json:"consumer_group_id"`
	// AuthorizedTopics is the exact set of topics the subscriber may Read and Describe,
	// and the set the ACLs are granted over. Empty means authorised for nothing — the
	// registry fails closed.
	AuthorizedTopics []string `json:"authorized_topics"`
	// PartitionKeyPrefix records a key-scoped authorization constraint that KAFKA CANNOT
	// ENFORCE, so a non-nil value makes the subscriber UNPROVISIONABLE: credential
	// issuance refuses rather than mint a credential whose real scope is every record on
	// every authorised topic.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// --- The credential record ---

	// CredentialReference is a non-reversible reference to the issued credential. It is
	// NOT the secret and nothing can be authenticated with it. Nil means no credential has
	// ever been issued.
	CredentialReference *string `json:"credential_reference,omitempty"`
	// CredentialIssuedAt is when the credential was issued, set together with
	// CredentialReference. A reissue overwrites both.
	CredentialIssuedAt *time.Time `json:"credential_issued_at,omitempty"`

	// --- Dual-run migration tracking (temporary by design) ---

	// WebhookURL is a MIGRATION RECORD, not a delivery destination. NOTHING SENDS TO IT.
	WebhookURL *string `json:"webhook_url,omitempty"`
	// MigratedAt is when the subscriber completed its move to Kafka consumption.
	MigratedAt *time.Time `json:"migrated_at,omitempty"`

	// --- Deregistration in progress ---

	// RevocationPendingAt is when deregistration began taking this subscriber's
	// broker-side access away. Nil for every ordinary subscriber.
	RevocationPendingAt *time.Time `json:"revocation_pending_at,omitempty"`

	// RevocationFailedAt is when the MOST RECENT revocation attempt failed at the broker.
	RevocationFailedAt *time.Time `json:"revocation_failed_at,omitempty"`

	// --- A credential that outlived its record ---

	// CredentialOrphanedAt is when an issuance left a credential at the broker that Blnk
	// could neither record nor revoke. Nil for every healthy subscriber.
	CredentialOrphanedAt *time.Time `json:"credential_orphaned_at,omitempty"`

	// --- Settlement obligations the background pass owes ---

	// GrantReconcilePendingAt and CredentialCleanupPendingAt are the two settlement
	// markers, stamped when a broker-side step could not be completed and cleared only
	// once it has been.
	GrantReconcilePendingAt    *time.Time `json:"grant_reconcile_pending_at,omitempty"`
	CredentialCleanupPendingAt *time.Time `json:"credential_cleanup_pending_at,omitempty"`

	// --- Row bookkeeping ---

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasTopicAccess reports whether the subscriber's RECORDED GRANT covers the given
// topic.
func (s *EventSubscriber) HasTopicAccess(topic string) bool {
	for _, t := range s.AuthorizedTopics {
		if t == topic {
			return true
		}
	}
	return false
}

// RequestedKeyScope is the key scope recorded for this subscriber, flattening the
// nullable column to a plain string.
func (s *EventSubscriber) RequestedKeyScope() string {
	if s == nil || s.PartitionKeyPrefix == nil {
		return ""
	}

	return strings.TrimSpace(*s.PartitionKeyPrefix)
}

// HasKeyAccess reports whether this subscriber is entitled to a record carrying the
// given message key.
func (s *EventSubscriber) HasKeyAccess(key string) bool {
	// DeclaresKeyScope, not `RequestedKeyScope() != ""`. The two differ on a
	// whitespace-only column, and here that difference decides whether EVERY record is
	// excluded: no ledger id begins with a tab, so a scope of "\t" would make this rule
	// refuse the subscriber's whole stream without an error anywhere. All three of this
	// predicate, EffectiveKeyScope and the credential contract read the same presence test
	// for that reason.
	if !s.DeclaresKeyScope() {
		return true
	}

	// The recorded value VERBATIM, not the trimmed one: keys are opaque identifiers, so
	// trimming here would test a different prefix than the registry recorded. Only the
	// question "is there a scope at all" tolerates trimming.
	return strings.HasPrefix(key, s.RequestedKeyScope())
}

// SubscriberKeyScopeAllKeys is the key scope of a credential issued to a subscriber
// that recorded no prefix, and the value the credential contract reports for it: EVERY
// key on the authorised topics.
const SubscriberKeyScopeAllKeys = "all-keys"

// EffectiveKeyScope reports the key scope a credential issued for this subscriber would
// ACTUALLY carry, together with whether that scope is enforced.
//
// Returns:
//   - scope: SubscriberKeyScopeAllKeys when no scope was requested, which is the honest
//     description of what a topic ACL grants on its own; otherwise the recorded prefix,
//     which is the boundary the consumer must apply.
//   - enforced: whether THE BROKER enforces the scope.
func (s *EventSubscriber) EffectiveKeyScope() (scope string, enforced bool) {
	// DeclaresKeyScope is the presence test, NOT `RequestedKeyScope() != ""`, and the
	// difference is a whitespace-only prefix. RequestedKeyScope returns the column
	// verbatim — correct, because message keys are opaque and normalising one would select
	// a different set of records — so a column holding a tab would otherwise be reported
	// here as a real scope. This pair is what the credential endpoint delivers to a
	// consumer, and a consumer applying a scope of "\t" discards its ENTIRE stream
	// silently.
	if s.DeclaresKeyScope() {
		return s.RequestedKeyScope(), false
	}

	return SubscriberKeyScopeAllKeys, true
}

// SubscriberSettlementObligation is one subscriber's outstanding broker-side
// obligation, as the settlement pass sees it.
type SubscriberSettlementObligation struct {
	// SubscriberID identifies the row that owes the work.
	SubscriberID string

	// GrantReconcilePending means the broker's ACL bindings may not match the row's recorded
	// authorization, so the grant must be reconciled to the row.
	GrantReconcilePending bool

	// CredentialCleanupPending means a SCRAM credential may exist that Blnk intended to
	// destroy, or the row names one that no longer works. Settlement revokes and then clears.
	CredentialCleanupPending bool

	// Attempts is how many settlement passes have already tried this row, and LastError is
	// what the most recent one said.
	Attempts  int
	LastError string
}

// Outstanding reports whether this obligation still requires work.
func (o SubscriberSettlementObligation) Outstanding() bool {
	return o.GrantReconcilePending || o.CredentialCleanupPending
}

// IsProvisioned reports whether a credential has ever been issued to this subscriber.
func (s *EventSubscriber) IsProvisioned() bool {
	return s != nil && s.CredentialReference != nil && *s.CredentialReference != ""
}

// MayHaveBrokerCredential reports whether a means of authenticating as this row's
// principal MIGHT exist at a broker.
//
// Returns:
//   - bool: true when any of the three states above holds.
func (s *EventSubscriber) MayHaveBrokerCredential() bool {
	if s == nil {
		return false
	}

	return s.IsProvisioned() ||
		s.CredentialOrphanedAt != nil ||
		s.CredentialCleanupPendingAt != nil
}

// BrokerCredentialEvidence names WHY MayHaveBrokerCredential answered true, for the log
// line and the error detail that accompany a refusal.
//
// Returns:
//   - string: a short phrase naming the strongest evidence, or "" when there is none.
func (s *EventSubscriber) BrokerCredentialEvidence() string {
	switch {
	case s == nil:
		return ""
	case s.IsProvisioned():
		return "the registry records an issued credential for this principal"
	case s.CredentialOrphanedAt != nil:
		return "an issuance left a credential at the broker that Blnk could neither record nor revoke"
	case s.CredentialCleanupPendingAt != nil:
		return "a credential this row intended to destroy has not been confirmed destroyed"
	default:
		return ""
	}
}

// IsMigrated reports whether the subscriber has completed its move from legacy HTTP
// webhook delivery to Kafka consumption. A nil MigratedAt means not yet migrated, which
// is what dual-window migration-progress reporting counts.
func (s *EventSubscriber) IsMigrated() bool {
	return s != nil && s.MigratedAt != nil
}

// IsRevocationPending reports whether deregistration has begun taking this subscriber's
// broker-side access away and has not yet confirmed it.
func (s *EventSubscriber) IsRevocationPending() bool {
	return s != nil && s.RevocationPendingAt != nil
}

// RequiresGatewayDelivery reports whether the subscriber records a partition key
// prefix, and therefore that its records may only be delivered through the
// key-authorising component an operator has declared in front of the brokers.
//
// Returns:
//   - bool: true when a non-blank partition key prefix is recorded.
func (s *EventSubscriber) RequiresGatewayDelivery() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// GrantsBrokerRecordAccess reports whether this subscriber's credential may fetch
// records DIRECTLY from the broker.
//
// Returns:
//   - bool: true when no key scope is recorded.
func (s *EventSubscriber) GrantsBrokerRecordAccess() bool {
	return !s.RequiresGatewayDelivery()
}

// Two registry subscribers then shared one credential and one ACL set, and reissuing
// for either silently rewrote the other's.

// SubscriberPrincipalNamespace prefixes every Kafka principal and consumer group Blnk
// derives, so a Blnk-issued identity is recognisable at the broker and cannot collide with
// an operator's own principals.
const SubscriberPrincipalNamespace = "blnk-sub-"

// SubscriberGroupTerminator ends a subscriber's consumer group namespace.
const SubscriberGroupTerminator = "."

// SubscriberDefaultGroupLeaf is the leaf of the consumer group a subscriber is given when it
// has not chosen one. It is a leaf inside the subscriber's own namespace, so using it is
// already inside the grant.
const SubscriberDefaultGroupLeaf = "default"

// maxSubscriberIdentifierLen bounds the identifier so the derived principal stays inside
// Kafka's own 255-character limit on a SCRAM user name with generous room for the namespace
// prefix and the group leaf.
const maxSubscriberIdentifierLen = 128

// minSubscriberIdentifierLen keeps an identifier long enough to be meaningful. A
// one-character identifier is almost certainly a mistake, and it maximises the chance of one
// identifier being a leading substring of another.
const minSubscriberIdentifierLen = 3

// ErrInvalidSubscriberIdentifier reports an identifier that cannot be used to derive a
// Kafka identity.
var ErrInvalidSubscriberIdentifier = errors.New(
	"model: subscriber identifier cannot be used to derive a Kafka identity",
)

// CanonicalizeSubscriberIdentifier returns the identifier in its one permitted
// spelling, or refuses it.
//
// Parameters:
//   - identifier string: the subscriber's business identifier, normally its
//     '<prefix>_<uuid>' subscriber_id.
//
// Returns:
//   - string: the identifier, unchanged, when it is already canonical.
//   - error: wrapping ErrInvalidSubscriberIdentifier, naming the specific rule broken.
func CanonicalizeSubscriberIdentifier(identifier string) (string, error) {
	if identifier == "" {
		return "", fmt.Errorf("%w: it is empty", ErrInvalidSubscriberIdentifier)
	}

	if identifier != strings.TrimSpace(identifier) {
		// The message names the exact rule broken, and an operator who sees "surrounding
		// whitespace" fixes it immediately.
		return "", fmt.Errorf(
			"%w: %q has surrounding whitespace, and a value that differs from another only by "+
				"whitespace would provision the same Kafka principal twice",
			ErrInvalidSubscriberIdentifier, identifier,
		)
	}

	if len(identifier) < minSubscriberIdentifierLen || len(identifier) > maxSubscriberIdentifierLen {
		return "", fmt.Errorf(
			"%w: %q is %d characters, outside the permitted %d to %d",
			ErrInvalidSubscriberIdentifier, identifier, len(identifier),
			minSubscriberIdentifierLen, maxSubscriberIdentifierLen,
		)
	}

	for i := 0; i < len(identifier); i++ {
		character := identifier[i]

		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case (character == '_' || character == '-') && i > 0:
		default:
			return "", fmt.Errorf(
				"%w: %q contains %q at position %d; only lowercase ASCII letters, digits, "+
					"underscore and hyphen are permitted, and the first character must be a letter or digit",
				ErrInvalidSubscriberIdentifier, identifier, string(character), i,
			)
		}
	}

	return identifier, nil
}

// CanonicalKafkaPrincipal derives the SASL/SCRAM username for a subscriber.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalKafkaPrincipal(identifier string) (string, error) {
	canonical, err := CanonicalizeSubscriberIdentifier(identifier)
	if err != nil {
		return "", err
	}

	return SubscriberPrincipalNamespace + canonical, nil
}

// CanonicalConsumerGroupNamespace derives the PREFIXED ACL resource name that reserves
// a subscriber's consumer group namespace.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>.".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalConsumerGroupNamespace(identifier string) (string, error) {
	canonical, err := CanonicalizeSubscriberIdentifier(identifier)
	if err != nil {
		return "", err
	}

	return SubscriberPrincipalNamespace + canonical + SubscriberGroupTerminator, nil
}

// CanonicalConsumerGroupID derives the consumer group a subscriber reads under by
// default.
//
// Parameters:
//   - identifier string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<identifier>.default".
//   - error: wrapping ErrInvalidSubscriberIdentifier when the identifier is unusable.
func CanonicalConsumerGroupID(identifier string) (string, error) {
	namespace, err := CanonicalConsumerGroupNamespace(identifier)
	if err != nil {
		return "", err
	}

	return namespace + SubscriberDefaultGroupLeaf, nil
}

// IsInSubscriberGroupNamespace reports whether a consumer group lies inside a
// namespace.
//
// Parameters:
//   - group string: the consumer group to test.
//   - namespace string: a namespace from CanonicalConsumerGroupNamespace.
//
// Returns:
//   - bool: true when group is a proper leaf of namespace.
func IsInSubscriberGroupNamespace(group, namespace string) bool {
	if namespace == "" || group == "" {
		return false
	}

	return len(group) > len(namespace) && strings.HasPrefix(group, namespace)
}

// isLegalKafkaTopicName reports whether every character is one Kafka permits in a topic
// name.
func isLegalKafkaTopicName(name string) bool {
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return false
		}
	}

	return true
}

// ValidateSubscriberTopics checks a topic grant against the bounds the registry stores
// it under, and is the SINGLE definition of those bounds.
//
// Parameters:
//   - topics []string: the grant to check. Nil and empty are both accepted.
//
// Returns:
//   - error: nil when the grant is within every bound, otherwise a plain error naming
//     the offending rule and, where it helps, the offending element.
func ValidateSubscriberTopics(topics []string) error {
	if len(topics) > MaxSubscriberTopics {
		return fmt.Errorf(
			"a subscriber may be authorised for at most %d topics, got %d",
			MaxSubscriberTopics, len(topics))
	}

	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return errors.New("an authorised topic must not be blank")
		}

		if len(topic) > MaxTopicNameLength {
			return fmt.Errorf(
				"an authorised topic must be at most %d characters, got %d",
				MaxTopicNameLength, len(topic))
		}

		if !isLegalKafkaTopicName(topic) {
			return fmt.Errorf(
				"authorised topic %q contains a character Kafka does not permit in a topic name; "+
					"only letters, digits, dots, underscores and hyphens are allowed", topic)
		}

		if _, duplicate := seen[topic]; duplicate {
			return fmt.Errorf("authorised topic %q is listed more than once", topic)
		}

		seen[topic] = struct{}{}
	}

	return nil
}

// DeclaresKeyScope reports whether this subscriber records a partition-key scope.
//
// Returns:
//   - bool: true when a non-blank partition-key prefix is recorded.
func (s *EventSubscriber) DeclaresKeyScope() bool {
	return s != nil && s.PartitionKeyPrefix != nil && strings.TrimSpace(*s.PartitionKeyPrefix) != ""
}

// There is no internalEventCategories set and no exported IsInternalEventCategory
// predicate, and the absence is deliberate now that the membership has ONE holder.

// KeyScopeEnforcementStatus names WHERE a subscriber's key scope is enforced.
type KeyScopeEnforcementStatus string

const (
	// KeyScopeEnforcementNone is reported when no key scope is recorded: the
	// broker-enforced topic and consumer-group ACLs are the subscriber's entire boundary
	// and there is nothing left for a consumer to filter.
	KeyScopeEnforcementNone KeyScopeEnforcementStatus = "none"

	// KeyScopeEnforcementGateway is reported when a key scope IS recorded AND the
	// deployment has declared a key-authorising component in front of the brokers: that
	// component applies the recorded prefix to every record's key before returning it.
	KeyScopeEnforcementGateway KeyScopeEnforcementStatus = "broker_gateway"
)

// KeyScopeEnforcement reports where this subscriber's key scope is enforced.
//
// Returns:
//   - KeyScopeEnforcementStatus: Gateway when a non-blank prefix is recorded, otherwise
//     None.
func (s *EventSubscriber) KeyScopeEnforcement() KeyScopeEnforcementStatus {
	if s.DeclaresKeyScope() {
		return KeyScopeEnforcementGateway
	}

	return KeyScopeEnforcementNone
}

// SubscriberKeyScopeState is how far a subscriber's key scope has actually got, from a
// prefix somebody typed into a registration body to a boundary a component confirmed it
// is keeping.
type SubscriberKeyScopeState string

const (
	// SubscriberKeyScopeStateNotRequested means no prefix is recorded, so there is no key
	// scope to enforce and none is claimed. The subscriber's boundary is its topic grant
	// and its consumer-group namespace, both of which the broker keeps in full.
	SubscriberKeyScopeStateNotRequested SubscriberKeyScopeState = "not_requested"

	// SubscriberKeyScopeStateRequested means a prefix IS recorded and the deployment
	// declares nothing that can keep it.
	SubscriberKeyScopeStateRequested SubscriberKeyScopeState = "requested"

	// SubscriberKeyScopeStateAvailable means a prefix is recorded AND the deployment
	// declares a key-authorising component with a reachable attestation endpoint, so a
	// credential for this row can be minted.
	SubscriberKeyScopeStateAvailable SubscriberKeyScopeState = "available"

	// SubscriberKeyScopeStateAttested means the declared component answered an
	// authenticated request confirming it is applying this exact prefix for this exact
	// principal.
	SubscriberKeyScopeStateAttested SubscriberKeyScopeState = "attested"
)

// SubscriberAccessDeployment is the DEPLOYMENT STATE a subscriber projection has to
// know before it can describe that subscriber's access truthfully.
type SubscriberAccessDeployment struct {
	// KeyScopeEnforcement is where this deployment can enforce a partition-key scope,
	// resolved from config.KafkaConfig.KeyScopeGateway — so it is Gateway only when the
	// mode is declared, the gateway address list is non-empty and distinct from
	// KAFKA_BROKERS, AND an attestation endpoint is configured.
	KeyScopeEnforcement KeyScopeEnforcementStatus

	// SubscriberBrokersAdvertised reports whether KAFKA_SUBSCRIBER_BROKERS names an
	// externally advertised bootstrap list, from
	// config.KafkaConfig.SubscriberFacingBrokers.
	SubscriberBrokersAdvertised bool

	// WholeTopicAccessPermitted reports whether this deployment may be issued a credential
	// that reads a granted topic in full: true outside secure mode, or in secure mode once
	// KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS declares the model.
	WholeTopicAccessPermitted bool
}

// KeyScopeStateFor resolves how far a recorded prefix has got in THIS deployment.
//
// Parameters:
//   - partitionKeyPrefix string: the prefix recorded on the row. Blank means none.
//
// Returns:
//   - SubscriberKeyScopeState: NotRequested for a blank prefix, Available when the
//     deployment declares a usable enforcement point, otherwise Requested.
func (d SubscriberAccessDeployment) KeyScopeStateFor(partitionKeyPrefix string) SubscriberKeyScopeState {
	if strings.TrimSpace(partitionKeyPrefix) == "" {
		return SubscriberKeyScopeStateNotRequested
	}

	if d.KeyScopeEnforcement == KeyScopeEnforcementGateway {
		return SubscriberKeyScopeStateAvailable
	}

	return SubscriberKeyScopeStateRequested
}
