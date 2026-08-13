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
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// requireRecordableWebhookURL refuses to record a legacy webhook endpoint once the
// retirement instant has passed.
func requireRecordableWebhookURL(webhookURL *string) error {
	if webhookURL == nil || strings.TrimSpace(*webhookURL) == "" {
		return nil
	}

	if !WebhookSunsetPassed(time.Now()) {
		return nil
	}

	return apierror.NewAPIError(
		apierror.ErrGenGone,
		"Legacy webhook delivery has been retired, so a webhook URL can no longer be recorded. "+
			"Subscribers consume events from Kafka; issue credentials with "+
			"POST /subscribers/{subscriber_id}/kafka-credentials. An endpoint already on the "+
			"record can still be cleared",
		errors.New(
			"event subscriber: refusing to record a legacy webhook_url after the configured "+
				"retirement instant; the relay enqueues no legacy deliveries past it, so a recorded "+
				"endpoint would describe a subscription nothing serves",
		),
	)
}

// subscriberCredentialIssuedAt renders a subscriber's issuance instant for a
// diagnostic, or "an unrecorded time" when the row carries none.
func subscriberCredentialIssuedAt(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.CredentialIssuedAt == nil {
		return "an unrecorded time"
	}

	return subscriber.CredentialIssuedAt.UTC().Format(time.RFC3339)
}

// requireProvisionableKeyScope refuses to mint a credential for a subscriber whose row
// records a partition key prefix WHILE THIS DEPLOYMENT DECLARES NOTHING ABLE TO ENFORCE
// ONE.
func requireProvisionableKeyScope(subscriber *model.EventSubscriber, enforced bool) error {
	if subscriber == nil || !subscriber.DeclaresKeyScope() || enforced {
		return nil
	}

	return apierror.NewAPIError(
		// The TYPED code, not the generic conflict. Both resolve to 409, but a client
		// discriminates on the code, and this refusal has a specific remedy that "CONFLICT"
		// cannot express — see apierror.ErrSubscriberKeyScopeUnenforced, whose documentation
		// describes exactly this refusal and both of the orders it is reachable from.
		apierror.ErrSubscriberKeyScopeUnenforced,
		"This subscriber records a partition key prefix and this deployment declares no component "+
			"that can enforce one, so no credential will be issued for it: Kafka's authorizer has "+
			"no message-key dimension, so the credential would read every record on every "+
			"authorized topic, including other ledgers' and other subscribers'. Declare a "+
			"key-authorising component by setting KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway with "+
			"its gateway addresses and attestation endpoint, or clear the partition key prefix to "+
			"accept access to whole topics, or narrow the subscriber's authorized topics, which "+
			"the broker does enforce",
		fmt.Errorf(
			"event subscriber: subscriber %q records a partition key prefix and no key-scope "+
				"enforcement component is declared; Kafka's authorizer has no message-key dimension, "+
				"so any credential issued would grant every record on every authorised topic and the "+
				"registry would describe a narrower boundary than exists",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		),
	)
}

// requireKeyScopeWhenEnforced is the MIRROR of requireProvisionableKeyScope: it refuses
// to mint a credential for a subscriber that records NO key scope, in a deployment that
// has declared its subscriber access to be key-scoped.
func requireKeyScopeWhenEnforced(subscriber *model.EventSubscriber, enforced bool) error {
	if subscriber == nil || !enforced || subscriber.DeclaresKeyScope() {
		return nil
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberKeyScopeRequired,
		"This deployment has declared that subscriber access is scoped by record key "+
			"(KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway), and this subscriber records no partition "+
			"key prefix. A credential issued for it would be granted whole-topic Read and would read "+
			"every ledger's records on every authorized topic, including other subscribers' — the one "+
			"principal the declared model does not cover. Record the partition key prefix this "+
			"subscriber is entitled to, provision a whole-topic consumer as an operator-managed "+
			"principal outside the subscriber registry, or stop declaring the key-scoped model and "+
			"acknowledge whole-topic access with KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS",
		fmt.Errorf(
			"event subscriber: subscriber %q records no partition key prefix while key-scope "+
				"enforcement is active; issuing would grant whole-topic Read in a deployment that "+
				"declares key-scoped subscriber access",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		),
	)
}

// requireAcknowledgedSharedTopicAccess refuses to mint a whole-topic credential in a
// PRODUCTION deployment that has declared nothing about its subscriber access model.
func requireAcknowledgedSharedTopicAccess(subscriber *model.EventSubscriber, enforced bool) error {
	if enforced {
		return nil
	}

	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		// A configuration that cannot be read is not a production posture anybody declared,
		// and it is not this check's business to invent one: every other reader of the
		// configuration on this path has already failed by now, with a message about the
		// configuration rather than about the access model.
		return nil
	}

	if !cnf.Server.Secure || cnf.Kafka.SubscriberSharedTopicAccess {
		return nil
	}

	subscriberID := ""
	if subscriber != nil {
		subscriberID = subscriber.SubscriberID
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberSharedTopicAccessUnacknowledged,
		"This deployment has not declared how subscriber access is scoped, and the credential "+
			"requested would read every record on each topic it is granted — every ledger's, and "+
			"every other subscriber's — because Kafka authorizes topics and consumer groups and has "+
			"no message-key dimension. Declare the model once: set "+
			"KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true to acknowledge whole-topic subscriber reads, "+
			"or set KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway with its gateway addresses and "+
			"attestation endpoint and record a partition key prefix on each subscriber",
		fmt.Errorf(
			"event subscriber: subscriber %q would be issued a whole-topic credential in secure mode "+
				"with no declared subscriber access model",
			sanitizeLogValue(subscriberID, maxLoggedFilterLength),
		),
	)
}

// requireRecordableKeyScope refuses to RECORD a key scope on a subscriber that already
// holds a credential. It is the other half of the refusal above, and without it that
// half is decorative.
func requireRecordableKeyScope(subscriber *model.EventSubscriber, enforced bool) error {
	if subscriber == nil || !subscriber.DeclaresKeyScope() || !subscriber.IsProvisioned() || enforced {
		return nil
	}

	return apierror.NewAPIError(
		// The same typed code issuance refuses with. One state, one code: a client that
		// handles SUBSCRIBER_KEY_SCOPE_UNENFORCED from the credential endpoint needs no
		// second case to handle it here, and the remedies are the same two plus revocation.
		apierror.ErrSubscriberKeyScopeUnenforced,
		"This subscriber already holds a Kafka credential, and Kafka cannot enforce a partition "+
			"key prefix, so recording one would describe a narrower boundary than the credential "+
			"actually has. Revoke the credential first if the prefix is what you want, clear the "+
			"prefix in this same request, or narrow the subscriber's authorized topics, which the "+
			"broker does enforce",
		fmt.Errorf(
			"event subscriber: subscriber %q holds a credential issued at %s; recording a partition "+
				"key prefix on it would leave a live principal with Read on whole topics under a row "+
				"claiming key-scoped access, and would leave that row unable to rotate its secret, "+
				"because re-issuance is refused for the same reason",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			subscriberCredentialIssuedAt(subscriber),
		),
	)
}

// requireGrantedTopics refuses to mint a credential for a subscriber authorised for
// nothing.
func requireGrantedTopics(subscriber *model.EventSubscriber) error {
	if subscriber == nil {
		return nil
	}

	for _, topic := range subscriber.AuthorizedTopics {
		if strings.TrimSpace(topic) != "" {
			return nil
		}
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberGrantEmpty,
		"This subscriber is authorized for no topics, so a credential for it could read nothing. "+
			"Grant it at least one authorized topic, then request the credential again",
		fmt.Errorf(
			"event subscriber: subscriber %q has an empty authorized topic list; issuing would mint a "+
				"live SASL principal holding no topic binding, and the response would be "+
				"indistinguishable from working access",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		),
	)
}

// Refusals a revocation tombstone produces, one per operation it blocks.
const (
	// subscriberRefusedIssuanceMessage is issuance's refusal. Minting a credential would re-arm
	// a principal mid-removal.
	subscriberRefusedIssuanceMessage = "This subscriber is being deregistered, so no credential " +
		"will be issued for it"

	// subscriberRefusedUpdateMessage is the authorization change's refusal. Recording a
	// wider grant would make the following grant step re-create bindings for a principal
	// whose revocation is already in flight.
	subscriberRefusedUpdateMessage = "This subscriber is being deregistered, so its access model " +
		"can no longer be changed"
)

// requireActiveSubscriber refuses an operation that would GRANT or WIDEN the access of
// a subscriber being deregistered.
func requireActiveSubscriber(subscriber *model.EventSubscriber, refusal string) error {
	if subscriber == nil || !subscriber.IsRevocationPending() {
		return nil
	}

	return apierror.NewAPIError(
		// SUBSCRIBER_DEPROVISIONING, not the generic conflict. Both carry 409 — the request is
		// well formed and it is the row's state that has to change — but the STATUS is not the
		// discriminator: registering a duplicate subscriber id answers 409 as well, and the
		// two call for opposite actions. A duplicate id means pick another one and never
		// retry; a revocation tombstone means this id is on its way out, so finish or reverse
		// the deregistration and then retry unchanged. Reported as one code, the two were
		// distinguishable only by reading the English prose, which is the one thing a client's
		// error handling cannot do.
		apierror.ErrSubscriberDeprovisioning,
		refusal,
		fmt.Errorf(
			"event subscriber: subscriber %q carries a revocation tombstone from %s; complete or "+
				"reverse its deregistration first",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			subscriber.RevocationPendingAt.UTC().Format(time.RFC3339),
		),
	)
}

// warnOnKeyScopeRecordedAfterIssuance reports the one key-scope transition whose
// consequence reaches BACKWARDS past the request that caused it.
func warnOnKeyScopeRecordedAfterIssuance(
	subscriber *model.EventSubscriber,
	priorKeyPrefix string,
	priorCredentialIssued bool,
) {
	currentKeyPrefix := subscriberKeyPrefix(subscriber)
	if !priorCredentialIssued || currentKeyPrefix == "" || currentKeyPrefix == priorKeyPrefix {
		return
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":         subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":             subscriberLogLabel(subscriber.KafkaPrincipal),
		"partition_key_prefix":       sanitizeLogValue(currentKeyPrefix, maxLoggedFilterLength),
		"prior_partition_key_prefix": sanitizeLogValue(priorKeyPrefix, maxLoggedFilterLength),
		// Derived from the persisted row, so this line reports the prefix and the component that
		// enforces it together. A prefix logged on its own says nothing about where it is applied.
		"key_scope_enforcement": string(describeSubscriberKeyScope(subscriber).Enforcement),
		"authorized_topics":     len(subscriber.AuthorizedTopics),
		"credential_issued_at":  subscriberCredentialIssuedAt(subscriber),
	}).Warn(
		"event subscriber: recorded a partition key prefix on a subscriber that already holds a " +
			"Kafka credential. Record-level Read has been WITHDRAWN from the existing principal, so " +
			"its next direct fetch is refused by the broker and its records must now be delivered by " +
			"the key-authorising component declared in KAFKA_KEY_SCOPE_ENFORCEMENT. The response that " +
			"credential was delivered with declared direct broker access, so reissue the credential " +
			"to deliver the corrected enforced_access declaration — or clear the prefix to restore " +
			"direct consumption",
	)
}

// refuseUnconfirmableBrokerWork is the guard: no broker IN THIS PROCESS is not the same
// fact as no broker-side access.
func refuseUnconfirmableBrokerWork(subscriber *model.EventSubscriber, action string) error {
	if !subscriber.MayHaveBrokerCredential() {
		return nil
	}

	// WHICH of the four states, because the remedies differ: an orphan is settled by
	// re-issuing, a tombstone by retrying the deregistration, a cleanup obligation by
	// letting settlement run. A refusal that named none of them left an operator to work
	// that out from the columns.
	evidence := subscriber.BrokerCredentialEvidence()

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"action":             action,
		"evidence":           evidence,
	}).Error(
		"event subscriber: this subscriber may hold live Kafka access (" + evidence + ") but no " +
			"broker is configured, so " + action + " CANNOT be confirmed and the registry was left " +
			"describing the access the broker still allows. Configure the broker and retry",
	)

	return apierror.NewAPIError(
		apierror.ErrKafkaUnavailable,
		"No Kafka broker is configured, so this subscriber's broker-side access cannot be "+
			"changed and the registry was not updated",
		// Retryable: nothing was written, and the request succeeds unchanged once a broker is
		// reachable. The row is the record that makes the retry findable.
		NewSubscriberErrorDetail(
			"Changing the broker-side access of a subscriber that may hold a credential requires "+
				"a reachable broker: "+evidence,
			subscriber.SubscriberID, true,
		),
	)
}

// subscriberAuthorization is the subset of a subscriber that determines its broker-side
// ACL bindings, in a form two snapshots can be compared by.
type subscriberAuthorization struct {
	principal     string
	consumerGroup string
	topics        []string

	// keyScoped is the PRESENCE of a partition-key prefix, not its value, and it belongs
	// in this snapshot because it decides the SHAPE of the grant: a key-scoped row is
	// provisioned with Describe and no Read on its topics so that the declared
	// key-authorising component is the only path its records can take.
	keyScoped bool
}

// newSubscriberAuthorization snapshots the authorization-bearing fields of a row.
func newSubscriberAuthorization(subscriber *model.EventSubscriber) subscriberAuthorization {
	if subscriber == nil {
		return subscriberAuthorization{}
	}

	topics := make([]string, 0, len(subscriber.AuthorizedTopics))
	seen := make(map[string]struct{}, len(subscriber.AuthorizedTopics))
	for _, topic := range subscriber.AuthorizedTopics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}
		if _, duplicate := seen[trimmed]; duplicate {
			continue
		}

		seen[trimmed] = struct{}{}
		topics = append(topics, trimmed)
	}
	sort.Strings(topics)

	return subscriberAuthorization{
		principal:     strings.TrimSpace(subscriber.KafkaPrincipal),
		consumerGroup: strings.TrimSpace(subscriber.ConsumerGroupID),
		topics:        topics,
		// The model's own predicate, so "declares a key scope" means the same thing here, at
		// provisioning, and in every response. It trims, so a whitespace-only column is not a
		// scope in any of the three.
		keyScoped: subscriber.RequiresGatewayDelivery(),
	}
}

// widens reports whether this snapshot implies any broker-side binding the prior one
// did not.
func (a subscriberAuthorization) widens(prior subscriberAuthorization) bool {
	if a.principal != prior.principal || a.consumerGroup != prior.consumerGroup {
		// A different principal or group is a binding on a name that carried none, whatever the
		// topic list does.
		return true
	}

	// CLEARING a key scope widens, and it is the one widening that leaves the topic list
	// untouched. A key-scoped subscriber holds Describe and no Read; clearing its prefix
	// grants Read on every topic it already had, so an unaccounted-for credential would
	// gain record access to all of them. Recording a prefix is the narrowing direction and
	// is not reported here.
	if prior.keyScoped && !a.keyScoped {
		return true
	}

	held := make(map[string]struct{}, len(prior.topics))
	for _, topic := range prior.topics {
		held[topic] = struct{}{}
	}

	for _, topic := range a.topics {
		if _, ok := held[topic]; !ok {
			return true
		}
	}

	return false
}

// refuseWideningUnaccountedAccess blocks a widening whose principal may hold a
// credential Blnk cannot account for.
func refuseWideningUnaccountedAccess(
	subscriber *model.EventSubscriber,
	prior subscriberAuthorization,
) error {
	// A RECORDED credential is not the concern: its reference is the join key that makes a
	// revocation possible, so a widening remains reversible. The unaccounted states are
	// the ones with no reference to revoke.
	if subscriber == nil || subscriber.IsProvisioned() {
		return nil
	}

	if subscriber.CredentialOrphanedAt == nil && subscriber.CredentialCleanupPendingAt == nil {
		return nil
	}

	if !newSubscriberAuthorization(subscriber).widens(prior) {
		return nil
	}

	evidence := subscriber.BrokerCredentialEvidence()

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"evidence":           evidence,
	}).Error(
		"event subscriber: refusing to widen the Kafka authorization of a principal whose " +
			"credential is unaccounted for (" + evidence + "); the new bindings would grant that " +
			"outstanding credential access it does not have, and there is no recorded reference to " +
			"revoke it by. Re-issue the credential to settle the state, or narrow the grant",
	)

	return apierror.NewAPIError(
		apierror.ErrConflict,
		"This subscriber's Kafka credential is unaccounted for, so its authorization cannot be "+
			"widened; re-issue its credentials to settle the state first, or narrow the grant",
		// NOT retryable: the same request will be refused until the state is settled, and the
		// settlement is a different call.
		NewSubscriberErrorDetail(evidence, subscriber.SubscriberID, false),
	)
}

// equals reports whether two snapshots imply the same broker-side bindings.
func (a subscriberAuthorization) equals(other subscriberAuthorization) bool {
	return a.principal == other.principal &&
		a.consumerGroup == other.consumerGroup &&
		a.keyScoped == other.keyScoped &&
		slices.Equal(a.topics, other.topics)
}

// authorizationNeedsReconciliation decides whether an update must touch the broker.
func authorizationNeedsReconciliation(
	stored subscriberAuthorization,
	updated *model.EventSubscriber,
	changes SubscriberUpdate,
) bool {
	if changes.AuthorizedTopics != nil {
		return true
	}

	return !stored.equals(newSubscriberAuthorization(updated))
}

// keyScopeDisclosure describes a subscriber's key scope and where it is enforced, so
// every surface that reports the prefix reports the enforcement point with it.
type keyScopeDisclosure struct {
	// Prefix is the recorded partition-key prefix, or "" when none is recorded.
	Prefix string

	// Enforcement is WHICH COMPONENT enforces the scope: the declared key-authorising
	// component (broker_gateway) when a prefix is recorded, none when the topic and group
	// ACLs are the whole boundary. It describes the ROW, so it reports where a prefix
	// WOULD be enforced even on a deployment that has declared nothing — which is exactly
	// the state a refused issuance has to be able to describe.
	Enforcement model.KeyScopeEnforcementStatus
}

// describeSubscriberKeyScope builds the disclosure for a registry row.
func describeSubscriberKeyScope(subscriber *model.EventSubscriber) keyScopeDisclosure {
	return keyScopeDisclosure{
		Prefix:      strings.TrimSpace(SubscriberPartitionKeyPrefix(subscriber)),
		Enforcement: subscriber.KeyScopeEnforcement(),
	}
}

// logKeyScopeDisclosure records the issuance line that accompanies a credential issued
// to a subscriber carrying a key scope.
func logKeyScopeDisclosure(subscriber *model.EventSubscriber, disclosure keyScopeDisclosure) {
	if disclosure.Prefix == "" {
		return
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":    subscriberLogLabel(subscriber.SubscriberID),
		"partition_key_prefix":  sanitizeLogValue(disclosure.Prefix, maxLoggedFilterLength),
		"key_scope_enforcement": string(disclosure.Enforcement),
		"authorized_topics":     len(subscriber.AuthorizedTopics),
		"consumer_group_hash":   consumerGroupLogLabel(subscriber.ConsumerGroupID),
	}).Info(
		"issued a Kafka credential to a subscriber that records a partition key prefix: Kafka ACLs " +
			"have no message-key dimension, so this credential was granted Describe but NOT Read on " +
			"its authorised topics — the broker refuses every direct fetch — and its records are " +
			"delivered key-filtered by the component declared in KAFKA_KEY_SCOPE_ENFORCEMENT",
	)
}
