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
	"unicode"
	"unicode/utf8"

	"github.com/blnkfinance/blnk/model"
)

// CreateSubscriber is the request body for POST /subscribers.
type CreateSubscriber struct {
	// SubscriberID is the business key, and the {id} in POST
	// /subscribers/{id}/kafka-credentials. Omit it to have the service generate one in the
	// repository's "<prefix>_<uuid>" form.
	SubscriberID string `json:"subscriber_id"`

	// Name is the human label the subscriber is triaged by. Required: it is the one field
	// the service cannot invent a meaningful value for, it is NOT NULL in
	// blnk.event_subscribers, and it is what an operator recognises a principal by months
	// later.
	Name string `json:"name"`

	// AuthorizedTopics is the set of topics this subscriber may Read and Describe, mapping
	// to the authorized_topics TEXT[] column and to the exact set of ACL bindings
	// provisioned for the principal. Omitted or empty means no grant, which is the safe
	// default rather than a missing value.
	AuthorizedTopics []string `json:"authorized_topics" binding:"max=16,dive,max=249"`

	// PartitionKeyPrefix names the THIRD SCOPE of the access model: the subscriber is
	// entitled only to records whose message key carries this prefix. Because every Blnk
	// event is keyed by ledger id, that is a ledger boundary.
	PartitionKeyPrefix string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD, and its absence is the sunset being enforceable.
}

// errSubscriberNameRequired is the single refusal for a missing subscriber name.
var errSubscriberNameRequired = errors.New("name is required")

// Derived returns the Kafka principal and consumer group for this request.
//
// Returns:
//   - principal string: "blnk-sub-<subscriber_id>".
//   - consumerGroup string: "blnk-sub-<subscriber_id>.default".
//   - err error: wrapping model.ErrInvalidSubscriberIdentifier when the identifier is
//     not canonical.
func (c CreateSubscriber) Derived() (principal, consumerGroup string, err error) {
	principal, err = model.CanonicalKafkaPrincipal(c.SubscriberID)
	if err != nil {
		return "", "", err
	}

	consumerGroup, err = model.CanonicalConsumerGroupID(c.SubscriberID)
	if err != nil {
		return "", "", err
	}

	return principal, consumerGroup, nil
}

// Validate checks every caller-supplied value on the request.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX, needed to resolve which
//     topic names this deployment owns. A blank value falls back to the strictest
//     namespace rather than a permissive one.
//   - options ...SubscriberGrantOption: the deployment facts a grant is judged against,
//     currently WithInternalTopicAccess. Passing no option means nothing is
//     acknowledged, so `<prefix>.system` is refused; pass WithInternalTopicAccess(true)
//     only where KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS is declared. Omission is
//     therefore fail-closed, which is why the variadic form is safe for callers that do
//     not know about the privileged category at all.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (c CreateSubscriber) Validate(topicPrefix string, options ...SubscriberGrantOption) error {
	// The prefix-independent rules run FIRST and are shared with nothing else, which is
	// what makes this the single validation entry point.
	if err := c.validateBodyRules(); err != nil {
		return err
	}

	// The identifier is checked only when supplied: an omitted one is generated
	// by the service, and generation produces a canonical value by construction.
	if c.SubscriberID != "" {
		if _, err := model.CanonicalizeSubscriberIdentifier(c.SubscriberID); err != nil {
			return err
		}
	}

	if err := validateGrantableTopics(c.AuthorizedTopics, topicPrefix, options...); err != nil {
		return err
	}

	return validateSubscriberKeyScope(c.PartitionKeyPrefix)
}

// UpdateSubscriber is the request body for PUT /subscribers/:subscriber_id and carries
// the mutable subset of a subscriber only.
type UpdateSubscriber struct {
	// Name replaces the human label when present. Bounded in the binder for the
	// same reason the create request's is; see validateSubscriberName.
	Name *string `json:"name,omitempty" binding:"omitempty,max=1024"`

	// AuthorizedTopics replaces the whole authorised set when present. It is already
	// nilable as a slice, so no pointer is needed to tell "omitted" from "set to empty":
	AuthorizedTopics []string `json:"authorized_topics,omitempty" binding:"omitempty,max=16,dive,max=249"`

	// PartitionKeyPrefix RECORDS the key-scoped authorization when present and non-empty,
	// and CLEARS it when present and empty. See the type comment for why this cannot be a
	// plain string: absent and "clear it" are different requests.
	PartitionKeyPrefix *string `json:"partition_key_prefix,omitempty"`

	// NO webhook_url FIELD. See the note on CreateSubscriber: this route is not
	// deprecated and not fronted by the sunset guard, so accepting legacy webhook state
	// here would let a caller keep writing it after the guarded routes had begun
	// answering 410 Gone. PUT /subscribers/:subscriber_id/webhook-subscription is the
	// one write path for it, and it is guarded.
}

// Validate checks every caller-supplied value that is present on the request.
//
// Parameters:
//   - topicPrefix string: the configured KAFKA_TOPIC_PREFIX.
//   - options ...SubscriberGrantOption: the deployment facts a grant is judged against,
//     on the same fail-closed terms as CreateSubscriber.Validate — omitting
//     WithInternalTopicAccess(true) refuses `<prefix>.system`.
//
// Returns:
//   - error: describing the first violation, nil when the request is usable.
func (u UpdateSubscriber) Validate(topicPrefix string, options ...SubscriberGrantOption) error {
	// The prefix-independent rules first, for the reason CreateSubscriber.Validate gives.
	if err := u.validateBodyRules(); err != nil {
		return err
	}

	if u.AuthorizedTopics != nil {
		if err := validateGrantableTopics(u.AuthorizedTopics, topicPrefix, options...); err != nil {
			return err
		}
	}

	// PartitionKeyPrefix is checked at the SAME standard as create. The prefix is issued
	// with the credential as a disclosed, consumer-side filtering contract, so it is
	// persisted, echoed in the credential response and written into log fields, and this
	// validator is the only thing standing between a caller and a control character or a
	// half-kilobyte value in all three. A rule applied only on creation is a rule with an
	// edit-shaped hole, and an edit is how a hostile value arrives: creation is scripted
	// from a template, editing is done by hand.
	if u.PartitionKeyPrefix != nil {
		if err := validateSubscriberKeyScope(*u.PartitionKeyPrefix); err != nil {
			return err
		}
	}

	return nil
}

// MaxSubscriberNameLength bounds the one free-text, caller-supplied field on a
// subscriber.
const MaxSubscriberNameLength = 256

// validateBodyRules applies the rules on a create body that need no topic prefix.
func (c CreateSubscriber) validateBodyRules() error {
	if err := validateSubscriberName(c.Name, true); err != nil {
		return err
	}

	// The RESOURCE bounds on the grant — cardinality, blank elements, name length, the
	// Kafka character set and duplicates. They are prefix-independent by nature, and they
	// are applied here rather than left to the binding tags because the tags can express
	// only two of the five.
	return model.ValidateSubscriberTopics(c.AuthorizedTopics)
}

// validateBodyRules applies the rules on an update body that need no topic prefix.
func (u UpdateSubscriber) validateBodyRules() error {
	if u.Name != nil {
		if err := validateSubscriberName(*u.Name, true); err != nil {
			return err
		}
	}

	if u.AuthorizedTopics == nil {
		return nil
	}

	return model.ValidateSubscriberTopics(u.AuthorizedTopics)
}

// validateSubscriberName applies the one bound and the one requirement on a subscriber
// name.
func validateSubscriberName(name string, required bool) error {
	trimmed := strings.TrimSpace(name)

	if trimmed == "" {
		if required {
			// THE SENTINEL, not a fresh fmt.Errorf. One shared validator already guarantees the
			// two Validate paths answer a missing name with the same words; returning the
			// package's own error value additionally makes the refusal MATCHABLE, so a caller
			// that has to distinguish "no name" from every other validation failure uses
			// errors.Is rather than comparing prose.
			return errSubscriberNameRequired
		}

		return nil
	}

	if utf8.RuneCountInString(trimmed) > MaxSubscriberNameLength {
		return fmt.Errorf(
			"name must be at most %d characters, got %d",
			MaxSubscriberNameLength, utf8.RuneCountInString(trimmed),
		)
	}

	for _, r := range trimmed {
		if unicode.IsControl(r) {
			return fmt.Errorf("name must not contain control characters")
		}
	}

	return nil
}

// validateGrantableTopics refuses an authorised-topic list containing anything a
// subscriber may not be granted.
func validateGrantableTopics(topics []string, topicPrefix string, options ...SubscriberGrantOption) error {
	if len(topics) == 0 {
		return nil
	}

	policy := resolveSubscriberGrantPolicy(options)
	grantable := model.SubscriberAuthorizableTopics(topicPrefix, policy.internalTopicAccess)

	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("authorized_topics contains an empty topic name")
		}

		if model.IsSubscriberAuthorizableTopicName(topic, topicPrefix, policy.internalTopicAccess) {
			continue
		}

		// A PRIVILEGED NAME REFUSED FOR WANT OF THE ACKNOWLEDGEMENT GETS ITS OWN MESSAGE. The
		// generic refusal below would send the operator to read the allowlist, which does not
		// contain the answer: the name IS a Blnk category topic and the missing piece is a
		// deployment declaration. Naming the variable is what makes the refusal actionable,
		// and it discloses nothing — the variable is documented in .env.example, both Compose
		// files and the Kubernetes configuration.
		if model.IsSubscriberPrivilegedTopicName(topic, topicPrefix) {
			return fmt.Errorf(
				"authorized_topics entry %q is the internal category topic: it carries Blnk's own "+
					"error text verbatim and every event type the catalogue does not yet recognise, "+
					"so it is grantable only where the deployment has acknowledged that by setting "+
					"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS. Set it to grant this topic, or grant "+
					"only the tenant category topics (%s)",
				topic, strings.Join(model.SubscriberGrantableTopics(topicPrefix), ", "),
			)
		}

		return fmt.Errorf(
			"authorized_topics entry %q is not grantable; a subscriber may be granted only "+
				"Blnk-owned category topics (%s). A dead-letter topic carries Blnk's own "+
				"failure metadata and every other subscriber's failed events, so it has no "+
				"subscriber audience",
			topic, strings.Join(grantable, ", "),
		)
	}

	return nil
}

// subscriberGrantPolicy carries the DEPLOYMENT facts that decide how wide the grant
// allowlist is for one validation.
type subscriberGrantPolicy struct {
	// internalTopicAccess is KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS as resolved by the
	// caller. It widens the allowlist by exactly one name and nothing else.
	internalTopicAccess bool
}

// SubscriberGrantOption declares one deployment fact for a single grant validation.
type SubscriberGrantOption func(*subscriberGrantPolicy)

// WithInternalTopicAccess declares whether this deployment has acknowledged that a
// subscriber may hold the internal category topic.
//
// Parameters:
//   - declared bool: the value of KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS.
//
// Returns:
//   - SubscriberGrantOption: applied by Validate and validateGrantableTopics.
func WithInternalTopicAccess(declared bool) SubscriberGrantOption {
	return func(policy *subscriberGrantPolicy) {
		policy.internalTopicAccess = declared
	}
}

// resolveSubscriberGrantPolicy folds the options into one policy value, so the zero value —
// nothing acknowledged — is what an absent option resolves to.
func resolveSubscriberGrantPolicy(options []SubscriberGrantOption) subscriberGrantPolicy {
	var policy subscriberGrantPolicy
	for _, option := range options {
		if option != nil {
			option(&policy)
		}
	}

	return policy
}

// validateSubscriberKeyScope constrains the recorded key-scoped authorization.
func validateSubscriberKeyScope(prefix string) error {
	if prefix == "" {
		return nil
	}

	if prefix != strings.TrimSpace(prefix) {
		return fmt.Errorf("partition_key_prefix must not have surrounding whitespace")
	}

	if len(prefix) > maxSubscriberKeyScopeLen {
		return fmt.Errorf("partition_key_prefix must be at most %d characters, got %d",
			maxSubscriberKeyScopeLen, len(prefix))
	}

	for _, character := range prefix {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("partition_key_prefix must not contain control characters")
		}
	}

	return nil
}

// maxSubscriberKeyScopeLen bounds the recorded key-scoped authorization. A key prefix is a fragment
// of a ledger ID — a '<prefix>_<uuid>' string — so 256 characters is far more than any
// legitimate value needs and still refuses an unbounded one.
const maxSubscriberKeyScopeLen = 256

// maxSubscriberNameLen bounds the human label, in runes.
const maxSubscriberNameLen = MaxSubscriberNameLength

// validateLegacyWebhookURL applies the destination policy to the dual-run webhook URL.
func validateLegacyWebhookURL(rawURL string) error {
	// THE ONE POLICY, in model.ValidateWebhookURL. This function carries no copy of the
	// https, host, whitespace and internal-destination rules and no destination classifier
	// of its own: a second copy beside the repository's would give one column two rules,
	// with the same rejected host producing two different reason phrases depending on which
	// door the request came through. What stays here is the ERROR SHAPE: a DTO
	// validation error rather than a typed apierror, and prefixed with the field name
	// because that is what a caller of this endpoint needs in order to know which body key
	// to correct.
	message, reason := model.ValidateWebhookURL(rawURL)
	if message == "" {
		return nil
	}

	// The MESSAGE and the REASON together, because the reason names the offending host or scheme —
	// the caller's own value, and the one thing that tells them what to change. Neither echoes the
	// whole URL or the parser's rendering of it.
	return fmt.Errorf("webhook_url: %s (%s)", strings.ToLower(message[:1])+message[1:], reason)
}
