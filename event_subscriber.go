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
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// THERE ARE NO PER-TENANT TOPICS. Blnk owns four category topics and a `.dlt` sibling
// for each (event_topics.go).

// SubscriberCredentialIssuanceBudget is the hard wall-clock ceiling on one credential
// issuance.
const SubscriberCredentialIssuanceBudget = 5 * time.Second

// subscriberCompensationReserve is how much of the issuance budget is HELD BACK so that
// a failure can still be compensated inside the same absolute deadline.
const subscriberCompensationReserve = 1250 * time.Millisecond

// generatedSubscriberPasswordLength is how many characters a generated SASL secret
// carries.
const generatedSubscriberPasswordLength = 48

// subscriberPasswordAlphabet is the alphabet a generated secret is drawn from:
const subscriberPasswordAlphabet = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789"

// subscriberPasswordGenerationAttempts bounds the retries that guarantee the generated
// secret clears the distinct-character floor validateSCRAMPassword applies.
const subscriberPasswordGenerationAttempts = 8

// errSubscriberStoreUnavailable reports a service built with no datasource.
var errSubscriberStoreUnavailable = errors.New(
	"event subscriber: no datasource is configured, so the subscriber registry is unavailable",
)

// SubscriberErrorDetail is the detail attached to a typed API error whose cause came
// from outside Blnk — the Kafka client, the database driver, or the TLS material on
// disk.
type SubscriberErrorDetail struct {
	// Reason states what failed, in fixed wording. It never interpolates the cause.
	Reason string `json:"reason"`

	// SubscriberID is the caller's own handle, and what an operator needs to find the
	// registry row and the log line. Bounded because it reaches a response body.
	SubscriberID string `json:"subscriber_id,omitempty"`

	// Retryable reports whether repeating the request may succeed, which is the actionable
	// half of the diagnosis. It is stated by the call site rather than inferred, because
	// only the call site knows whether the operation is idempotent.
	Retryable bool `json:"retryable"`

	// CredentialWritten and Compensated describe what reached the BROKER, and they are the
	// two facts a caller cannot otherwise learn. Together they say whether a secret may
	// exist that the caller does not hold: written-and-compensated means the broker is
	// clean, written-and-not-compensated means a principal exists that needs attention.
	CredentialWritten bool `json:"credential_written,omitempty"`
	Compensated       bool `json:"compensated,omitempty"`

	// CompensationPending reports that the credential's removal has been SCHEDULED and its
	// outcome was not known when this response was written.
	CompensationPending bool `json:"compensation_pending,omitempty"`
}

// NewSubscriberErrorDetail builds a bounded subscriber-failure detail.
//
// Parameters:
//   - reason string: fixed wording describing the failure.
//   - subscriberID string: the caller's identifier. Bounded here so a call site cannot
//     forget to.
//   - retryable bool: whether repeating the request may succeed.
//
// Returns:
//   - SubscriberErrorDetail: the safe detail.
func NewSubscriberErrorDetail(reason, subscriberID string, retryable bool) SubscriberErrorDetail {
	return SubscriberErrorDetail{
		Reason:       reason,
		SubscriberID: sanitizeLogValue(subscriberID, maxLoggedFilterLength),
		Retryable:    retryable,
	}
}

// eventSubscriberStore is the persistence surface this service needs, and nothing more.
type eventSubscriberStore interface {
	// CreateEventSubscriber registers a subscriber and returns the stored row, including
	// the database's own surrogate key and bookkeeping timestamps.
	CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error)

	// GetEventSubscriberByID reads one subscriber by its business key, returning a typed
	// not-found error rather than a bare sql.ErrNoRows.
	GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)

	// ListEventSubscribers pages the registry newest first.
	ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error)

	// ListAndCountEventSubscribers pages the registry and counts it from ONE SNAPSHOT, and
	// is what a listing that asked for a total reads. Two reads on two connections observe
	// two registries, so a subscriber registered between them makes the total describe a
	// set the page is not a slice of.
	ListAndCountEventSubscribers(
		ctx context.Context,
		query model.SubscriberPageQuery,
	) (model.SubscriberPage, int64, error)

	// CountEventSubscribers counts the whole registry, which is what the listing's
	// include_count option answers with. It is a separate query rather than the length of
	// a page, because a page length is not a total.
	CountEventSubscribers(ctx context.Context) (int64, error)

	// UpdateEventSubscriber replaces the mutable columns of an existing row, under the
	// caller's provisioning claim and only while the row is not tombstoned for
	// deregistration. A miss on either is a conflict naming which condition failed.
	UpdateEventSubscriber(
		ctx context.Context,
		subscriber *model.EventSubscriber,
		fenceToken string,
	) (*model.EventSubscriber, error)

	// TakeEventSubscriber removes a subscriber and RETURNS the row it removed, so the
	// caller still holds the principal and topics that broker-side revocation needs. It is
	// conditional on the caller's provisioning claim, because it runs after a broker round
	// trip and must not delete a row another operation has since taken over.
	TakeEventSubscriber(ctx context.Context, subscriberID string, fenceToken string) (*model.EventSubscriber, error)

	// RecordSubscriberCredentialIfUnchanged persists an issuance only while the row still
	// holds the reference observed before provisioning AND the provisioning claim is still
	// the caller's, reporting a conflict otherwise. The reference alone is not sufficient:
	RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time, claimToken string) error

	// ClearSubscriberCredential returns a row to the "registered, not yet provisioned"
	// state. It is the compensating half of an issuance record, and is conditional on the
	// caller's provisioning claim so that a stale owner cannot blank the record a newer
	// issuance just wrote.
	ClearSubscriberCredential(ctx context.Context, subscriberID, fenceToken string) error

	// MarkSubscriberMigrated stamps the instant a subscriber completed its move from
	// legacy HTTP delivery to Kafka consumption, and nothing else. Prefer
	// CompleteSubscriberWebhookMigration when a URL is being retired at the same time.
	MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error

	// CompleteSubscriberWebhookMigration forgets the legacy endpoint AND stamps the
	// migration instant in ONE statement, so a failure cannot leave the row absent from
	// both sides of the migration report.
	CompleteSubscriberWebhookMigration(ctx context.Context, subscriberID string, migratedAt time.Time) (*model.EventSubscriber, error)
	// RecordSubscriberWebhookURL records a legacy endpoint and clears migrated_at in one
	// statement, so a row can never be migrated and still carry a live URL.
	RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error

	// ClearSubscriberWebhookURL forgets one subscriber's endpoint without claiming it
	// migrated.
	ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error

	// MarkSubscriberCredentialOrphaned records that a credential exists at the broker for
	// a subscriber whose registry row does not account for it. Unfenced deliberately: it
	// is the compensating write on a path whose claim may already be lost, and the
	// exposure must be recorded either way.
	MarkSubscriberCredentialOrphaned(ctx context.Context, subscriberID string, orphanedAt time.Time) error

	// MarkSubscriberRevocationFailed records that the most recent revocation attempt was
	// refused by the broker, so the obligation is visible rather than living only in a log.
	MarkSubscriberRevocationFailed(ctx context.Context, subscriberID string, failedAt time.Time) error

	// PurgeMigratedSubscriberWebhookURLs erases the legacy URL of every subscriber whose
	// migration completed strictly before the cut-off, returning how many rows changed.
	PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error)

	// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row,
	// so deregistration can revoke at the broker while the row that names the principal
	// still exists.
	MarkSubscriberRevocationPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) (*model.EventSubscriber, error)

	// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation,
	// returning the token the claim is held under and a conflict when somebody else holds
	// it. Nothing may touch the broker for a subscriber without holding its claim.
	ClaimSubscriberForProvisioning(ctx context.Context, subscriberID string, lease time.Duration) (string, error)

	// ReleaseSubscriberProvisioningFence clears a claim the caller still holds, so a retry
	// after a fast failure need not wait out the lease.
	ReleaseSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string) error

	// RenewSubscriberProvisioningFence extends a claim the caller still holds. It is
	// called immediately BEFORE each broker phase: a successful renewal proves ownership
	// at that instant, so the phase cannot interleave with another operation's, and a
	// refused one means the operation must be abandoned rather than continued.
	RenewSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string, lease time.Duration) error

	// RecordSubscriberGrantReconcilePending marks a subscriber as owing a broker-side
	// grant reconciliation. It is written BEFORE the broker work, so the obligation
	// survives an outcome nobody is left to record.
	RecordSubscriberGrantReconcilePending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// ClearSubscriberGrantReconcilePending discharges that obligation, once the row and the
	// broker are known to agree.
	ClearSubscriberGrantReconcilePending(ctx context.Context, subscriberID, fenceToken string) error

	// RecordSubscriberCredentialCleanupPending marks a subscriber as owing a credential
	// cleanup: a SCRAM credential may exist that Blnk intended to destroy, or the row may
	// name one that no longer works.
	RecordSubscriberCredentialCleanupPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// GetSubscriberSettlementObligation reads what ONE subscriber currently owes. The
	// flags are re-read under the claim rather than carried from a scan, because a
	// successful re-issuance discharges the credential-cleanup obligation in between.
	GetSubscriberSettlementObligation(ctx context.Context, subscriberID string) (model.SubscriberSettlementObligation, error)

	// ListSubscriberSettlementObligations returns the subscribers owing broker-side work,
	// oldest attempt first, skipping any attempted more recently than the caller's bound.
	ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error)

	// MarkSubscriberSettlementAttempt records that a settlement pass tried a row and what
	// happened. It does not discharge anything.
	MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error
}

// subscriberPrincipalProvisioner is the administrative surface credential issuance
// needs.
type subscriberPrincipalProvisioner interface {
	// IsConfigured reports whether any broker is configured. False means issuance cannot
	// succeed and is answered with ErrKafkaUnavailable before anything is generated.
	IsConfigured() bool

	// Brokers returns the bootstrap list the credential response hands to the subscriber.
	Brokers() []string

	// ProvisionSubscriberPrincipal mints or replaces the SCRAM credential and binds the
	// ACLs that are the subscriber's access boundary.
	ProvisionSubscriberPrincipal(ctx context.Context, req SubscriberProvisioningRequest) (SubscriberProvisioningResult, error)

	// RevokeSubscriber removes a subscriber's ACL bindings and then its SCRAM credential,
	// ending its access at the broker.
	RevokeSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error

	// PruneSubscriberAccess removes the broker-side grants a subscriber's recorded
	// authorization no longer implies. An authorization change calls it BEFORE persisting,
	// so a narrowing is in force at the broker before the registry claims it is.
	PruneSubscriberAccess(ctx context.Context, subscriber *model.EventSubscriber) (SubscriberACLReconciliation, error)

	// GrantSubscriberAccess creates the grants the recorded authorization implies. An
	// authorization change calls it AFTER persisting, so a widening reaches the broker
	// only once the registry records it.
	GrantSubscriberAccess(ctx context.Context, subscriber *model.EventSubscriber) (SubscriberACLReconciliation, error)

	// Close releases the client's pooled connections.
	Close() error
	// CompensateProvisioning performs the compensation a DEFERRED provisioning failure
	// reported instead of running, and is a no-op for a result that owes none.
	CompensateProvisioning(ctx context.Context, result SubscriberProvisioningResult) error
}

// Compile-time proof that the production types satisfy both seams. A signature drift fails
// the build here, on the lines that state the contract, rather than at a call site.
var (
	_ eventSubscriberStore           = (database.IDataSource)(nil)
	_ subscriberPrincipalProvisioner = (KafkaAdmin)(nil)
)

// SubscriberRegistration is what registering a subscriber requires.
type SubscriberRegistration struct {
	// SubscriberID is the business key and the {id} in POST
	// /subscribers/{id}/kafka-credentials. Leave it blank to have one generated in the
	// repository's "<prefix>_<uuid>" form.
	SubscriberID string

	// Name is the human label an operator triages the subscriber by. Required, because an
	// unnamed principal cannot be triaged and being able to answer "who is this
	// principal?" months later is most of the reason the registry exists.
	Name string

	// AuthorizedTopics is the exact set of topics the subscriber may Read and Describe.
	AuthorizedTopics []string

	// PartitionKeyPrefix records a key-scoped constraint Kafka CANNOT enforce, so a
	// subscriber registered with one is UNPROVISIONABLE: issuance refuses until it is
	// cleared. Nil, the ordinary case, means no such constraint.
	PartitionKeyPrefix *string

	// WebhookURL records the legacy HTTP endpoint a migrating subscriber received pushes
	// on before moving to Kafka. Nil for a subscriber onboarded after the cutover, which
	// never had one. Dual-run only.
	WebhookURL *string
}

// SubscriberUpdate is the mutable subset of a registered subscriber.
type SubscriberUpdate struct {
	// Name replaces the human label when non-nil. A blank replacement is refused, because
	// NOT NULL does not stop an empty string and an unnamed principal cannot be triaged.
	Name *string

	// AuthorizedTopics replaces the whole authorised set when NON-NIL, including when it
	// is non-nil and empty, which revokes every topic grant on the row.
	AuthorizedTopics []string

	// PartitionKeyPrefix replaces the recorded key prefix when non-nil; a present empty
	// string CLEARS it. It grants and revokes NOTHING at the broker, and it does not gate
	// credential issuance either.
	PartitionKeyPrefix *string

	// WebhookURL replaces the recorded legacy endpoint when non-nil; a present empty
	// string clears it. Any non-empty value must be HTTPS and must not address an internal
	// destination — the persistence boundary enforces both.
	WebhookURL *string
}

// SubscriberCredential is the result of one credential issuance: everything a
// subscriber needs to start consuming, and the ONLY place in this package where a
// plaintext SASL secret exists.
type SubscriberCredential struct {
	// SubscriberID is the subscriber the credential belongs to.
	SubscriberID string

	// Brokers is the bootstrap list, read from the administrative client that dialled
	// rather than from configuration a second time, so the address the subscriber is given
	// is the address Blnk itself uses.
	Brokers []string

	// BrokerEndpoint is the same list rendered as one comma-separated connection string,
	// for clients configured with a single endpoint value rather than a list.
	BrokerEndpoint string

	// AuthorizedTopics is the exact set the credential was granted Read and Describe on.
	AuthorizedTopics []string

	// ConsumerGroupID is the group the subscriber reads under. The grant is PREFIXED over
	// the subscriber's namespace, so any group inside it also works.
	ConsumerGroupID string

	// Username is the SASL/SCRAM username: the subscriber's Kafka principal.
	Username string

	// Mechanism is the SASL mechanism, always SubscriberSASLMechanism.
	Mechanism string

	// IssuedAt is the instant the issuance was recorded, matching credential_issued_at on
	// the row.
	IssuedAt time.Time

	// PartitionKeyPrefix is the routing hint recorded on the subscriber, echoed here so
	// the response can state it beside the declaration that the broker does not enforce
	// it.
	PartitionKeyPrefix string

	// Fingerprint is the short, non-sensitive form of the stored credential reference. It
	// is what a client uses to tell one issuance from another and is not a secret; it
	// cannot be authenticated with and the reference cannot be recovered from it.
	Fingerprint string

	// Replaced reports that the principal already held a SCRAM credential and this
	// issuance replaced it. An operator reading it knows an existing consumer's credential
	// has just stopped working.
	Replaced bool

	// KeyScopeEnforcement says WHICH COMPONENT enforces the recorded scope, and it travels
	// with the prefix rather than being derivable from it. broker_gateway — the same word
	// the deployment declares in KAFKA_KEY_SCOPE_ENFORCEMENT — means the subscriber was
	// granted Describe but NOT Read on its topics, so the broker refuses every direct
	// fetch and its records are delivered, key-filtered, by that declared component; none
	// means no scope was recorded and the topic and group ACLs are the entire boundary,
	// which the broker keeps in full.
	KeyScopeEnforcement model.KeyScopeEnforcementStatus

	// password is the plaintext, held in the redacting type and reachable only through
	// Password. NEVER add an exported field carrying it, and never copy it into a log
	// field, an error message, a trace attribute or a metric label.
	password SubscriberSecret
}

// Password reveals the generated secret, and is the ONE place it can be read from.
func (c SubscriberCredential) Password() string {
	return c.password.reveal()
}

// PasswordLength reports how long the generated secret is, without revealing it.
//
// Returns:
//   - int: the secret's length in bytes; zero when none is held.
func (c SubscriberCredential) PasswordLength() int {
	return c.password.Len()
}

// KeyScope reports this credential's key boundary in the form a consumer is told it,
// together with whether THE BROKER keeps that boundary.
//
// Returns:
//   - scope string: the recorded prefix, or model.SubscriberKeyScopeAllKeys when none
//     is recorded.
//   - enforcedByBroker bool: false exactly when a narrowing prefix is recorded, which
//     is when the declared component enforces it instead.
func (c SubscriberCredential) KeyScope() (scope string, enforcedByBroker bool) {
	trimmed := strings.TrimSpace(c.PartitionKeyPrefix)
	if trimmed == "" {
		return model.SubscriberKeyScopeAllKeys, true
	}

	// FALSE, and the boolean means what it is named: the BROKER does not enforce this.
	return trimmed, false
}

// issuedKeyScopeEnforcement resolves the enforcement point to record on an ISSUED
// credential.
func issuedKeyScopeEnforcement(
	subscriber *model.EventSubscriber,
	enforced bool,
) model.KeyScopeEnforcementStatus {
	if enforced && subscriber.DeclaresKeyScope() {
		return model.KeyScopeEnforcementGateway
	}

	return model.KeyScopeEnforcementNone
}

// LogFields is the safe projection of an issuance for structured logging.
func (c SubscriberCredential) LogFields() logrus.Fields {
	return logrus.Fields{
		"subscriber_id_hash":      subscriberLogLabel(c.SubscriberID),
		"principal_hash":          subscriberLogLabel(c.Username),
		"consumer_group_hash":     consumerGroupLogLabel(c.ConsumerGroupID),
		"authorized_topic_count":  len(c.AuthorizedTopics),
		"mechanism":               c.Mechanism,
		"credential_fingerprint":  c.Fingerprint,
		"credential_replaced":     c.Replaced,
		"credential_secret_bytes": c.password.Len(),
		"issued_at":               c.IssuedAt.UTC().Format(time.RFC3339Nano),
		// Where the key scope is enforced, on EVERY issuance line rather than only the
		// key-scope line, so a search over issuance records can separate the subscribers
		// whose records are delivered through the declared component from the ones whose
		// whole boundary is broker-enforced. The prefix VALUE is not repeated here — it is
		// caller-supplied text, and the line logKeyScopeDisclosure emits alongside this one
		// carries it sanitized, once. On an ISSUED credential this reads "broker_gateway" for
		// a row recording a prefix and "none" otherwise; it never reads consumer_side,
		// because a credential is minted only where an enforcement point is declared.
		"key_scope_enforcement": string(c.KeyScopeEnforcement),
	}
}

// String renders the redacted summary, so a Stringer-aware caller cannot print the secret.
func (c SubscriberCredential) String() string {
	return fmt.Sprintf(
		"SubscriberCredential{subscriber:%s principal:%s group:%s topics:%d mechanism:%s "+
			"fingerprint:%s password:%s}",
		c.SubscriberID, c.Username, c.ConsumerGroupID, len(c.AuthorizedTopics),
		c.Mechanism, c.Fingerprint, RedactedSecretPlaceholder,
	)
}

// GoString renders the same redacted summary for %#v, which would otherwise print the
// struct literal including the unexported field's contents.
func (c SubscriberCredential) GoString() string {
	return c.String()
}

// Format renders the redacted summary for EVERY fmt verb.
//
// Parameters:
//   - state fmt.State: the destination fmt is writing to.
//   - _ rune: the verb, deliberately ignored — every verb gets the same safe rendering.
func (c SubscriberCredential) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(c.String()))
}

// EventSubscriberService owns the subscriber registry and credential issuance.
type EventSubscriberService struct {
	// store is the registry repository. It may be nil — NewBlnk(nil) is a supported
	// construction — and every operation then reports errSubscriberStoreUnavailable rather
	// than dereferencing nil.
	store eventSubscriberStore

	// mu guards admin and ownsAdmin, which are assigned together on first use when no
	// client was injected.
	mu sync.Mutex

	// admin is the administrative client. Nil until injected or resolved.
	admin subscriberPrincipalProvisioner

	// ownsAdmin records that this service built the client itself and is therefore the
	// only thing allowed to close it. An injected client outlives this service.
	ownsAdmin bool

	// issuanceBudget is the wall-clock ceiling on one issuance. Always positive after
	// construction; SubscriberCredentialIssuanceBudget unless a test shortened it.
	issuanceBudget time.Duration

	// now is the clock. Always set by the constructor; an in-package test may replace it
	// so that recorded issuance instants are exact rather than approximately now.
	now func() time.Time

	// generatePassword mints the SASL secret. Always set by the constructor; an in-package
	// test may replace it to drive a generator failure, which has no other trigger.
	generatePassword func() (string, error)

	// resolveAdmin resolves the PROCESS-WIDE administrative client, when one is available.
	resolveAdmin func() (subscriberPrincipalProvisioner, error)

	// deferWork schedules a compensating task OFF the response path.
	deferWork func(func())

	// keyScopeGateway is the control-plane client for the declared key-authorising
	// component.
	keyScopeGateway KeyScopeGatewayClient
}

// NewEventSubscriberService builds the subscriber registry service.
func NewEventSubscriberService(
	store eventSubscriberStore,
	admin subscriberPrincipalProvisioner,
) *EventSubscriberService {
	service := &EventSubscriberService{
		store:            store,
		issuanceBudget:   SubscriberCredentialIssuanceBudget,
		now:              time.Now,
		generatePassword: generateSubscriberPassword,
	}

	// A typed nil pointer placed in an interface field is a NON-NIL interface holding a
	// nil pointer, which passes every nil guard and then panics on first use. The guard
	// here is against the plain nil interface only; callers that hold a possibly-nil
	// *KafkaAdminClient must do what cmd/server.go does and assign the interface only on
	// success.
	if admin != nil {
		service.admin = admin
		service.ownsAdmin = false
	}

	return service
}

// WithIssuanceBudget overrides the wall-clock ceiling on one credential issuance.
func (s *EventSubscriberService) WithIssuanceBudget(budget time.Duration) *EventSubscriberService {
	if budget <= 0 {
		budget = SubscriberCredentialIssuanceBudget
	}

	s.issuanceBudget = budget

	return s
}

// WithKafkaAdmin injects an already-built administrative client.
//
// Parameters:
//   - admin subscriberPrincipalProvisioner: the client to use, or nil to resolve
//     lazily.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithKafkaAdmin(admin subscriberPrincipalProvisioner) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.admin = admin
	s.ownsAdmin = false

	return s
}

// WithKeyScopeGateway injects the control-plane client for the declared key-authorising
// component.
//
// Parameters:
//   - gateway KeyScopeGatewayClient: the client to use, or nil to resolve from
//     configuration.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithKeyScopeGateway(gateway KeyScopeGatewayClient) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.keyScopeGateway = gateway

	return s
}

// keyScopeGatewayClient resolves the control-plane client for the declared component.
func (s *EventSubscriberService) keyScopeGatewayClient() (KeyScopeGatewayClient, error) {
	s.mu.Lock()
	injected := s.keyScopeGateway
	s.mu.Unlock()

	if injected != nil {
		return injected, nil
	}

	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		// A configuration that cannot be read declares nothing, which is the same fail-closed
		// answer keyScopeEnforcement gives: no gateway, so no key-scoped credential.
		return nil, ErrKeyScopeGatewayNotConfigured
	}

	return NewKeyScopeGatewayClient(cnf)
}

// attestKeyScope requires the declared component to confirm a subscriber's recorded key
// scope before anything is minted.
func (s *EventSubscriberService) attestKeyScope(
	ctx context.Context, subscriber *model.EventSubscriber,
) error {
	binding := keyScopeBindingFor(subscriber)

	gateway, err := s.keyScopeGatewayClient()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(binding.SubscriberID),
			"error_class":        kafkaErrorClassField("key_scope_gateway_resolution", err),
		}).Error(
			"event subscriber: this subscriber records a partition key prefix and enforcement was " +
				"reported active, but no key-scope gateway control endpoint could be resolved, so no " +
				"credential was minted",
		)

		return apierror.NewAPIError(
			apierror.ErrSubscriberKeyScopeUnattested,
			"No key-scope enforcement gateway control endpoint is configured, so the partition key "+
				"prefix recorded on this subscriber cannot be confirmed with the component that would "+
				"apply it. No credential was issued. Set KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL and "+
				"KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN, or clear the partition key prefix and "+
				"narrow authorized_topics, which the broker enforces in full",
			// Not retryable: the identical request cannot succeed until the deployment's
			// configuration changes.
			NewSubscriberErrorDetail(
				"No key-scope enforcement gateway control endpoint is configured",
				binding.SubscriberID, false,
			),
		)
	}

	return gateway.AttestBinding(ctx, binding)
}

// revokeKeyScopeBinding withdraws a subscriber's binding at the declared component.
func (s *EventSubscriberService) revokeKeyScopeBinding(
	ctx context.Context, subscriber *model.EventSubscriber,
) error {
	if subscriber == nil || strings.TrimSpace(subscriber.KafkaPrincipal) == "" {
		return nil
	}

	gateway, err := s.keyScopeGatewayClient()
	if err != nil {
		// Nothing declared, nothing to withdraw. This is the shipped default and must not be
		// reported as a cleanup failure.
		return nil
	}

	return gateway.RevokeBinding(ctx, subscriber.KafkaPrincipal)
}

// provisioner returns the administrative client, building one on first use when none
// was injected.
func (s *EventSubscriberService) provisioner() (subscriberPrincipalProvisioner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.admin != nil {
		return s.admin, nil
	}

	// THE PROCESS-WIDE CLIENT FIRST, when a resolver was installed. It is remembered
	// locally so that an operation resolving twice — UpdateSubscriber prunes and then
	// grants — does not ask again, and it is recorded as NOT OWNED, because the process
	// owns it and closing it here would take the transport away from every later request.
	if s.resolveAdmin != nil {
		if shared, err := s.resolveAdmin(); err == nil && shared != nil {
			s.admin = shared
			s.ownsAdmin = false

			return s.admin, nil
		}
	}

	cnf, err := fetchConfiguration()
	if err != nil {
		// LOGGED HERE AND BOUNDED, RETURNED WITHOUT THE CAUSE. A configuration failure can
		// quote the offending file content or a path inside the container, and NewAPIError
		// would both re-log it unsanitized and serialise it into the response. See
		// SubscriberErrorDetail.
		kafkaErrorEntry("subscriber_admin_fetch_configuration", err).Error(
			"event subscriber: the configuration could not be read, so no Kafka administrative " +
				"client could be built and no credential can be provisioned",
		)

		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Configuration is unavailable, so Kafka credentials cannot be provisioned",
			NewSubscriberErrorDetail("The Blnk configuration could not be read", "", false),
		)
	}

	admin, err := NewKafkaAdmin(cnf)
	if err != nil {
		// The most disclosure-prone cause in this file. Building the administrative client
		// reads TLS material from disk and prepares a SCRAM mechanism, so the failure can
		// name a filesystem path inside the container or the administrative principal — and
		// the transport refuses plaintext by returning an error, so an ordinary
		// misconfiguration reaches this branch on a normal deployment.
		kafkaErrorEntry("subscriber_admin_new_kafka_admin", err).Error(
			"event subscriber: the Kafka administrative client could not be built, so no subscriber " +
				"credential can be provisioned; check KAFKA_BROKERS, the administrative SASL pair " +
				"and the TLS settings",
		)

		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Failed to build the Kafka administrative client for subscriber provisioning",
			NewSubscriberErrorDetail(
				"The Kafka administrative client could not be built from the current configuration",
				"", false,
			),
		)
	}

	s.admin = admin
	s.ownsAdmin = true

	return s.admin, nil
}

// Close releases an administrative client this service built for itself.
//
// Returns:
//   - error: the client's close error, or nil when there was nothing to close.
func (s *EventSubscriberService) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	admin := s.admin
	owned := s.ownsAdmin
	if owned {
		s.admin = nil
		s.ownsAdmin = false
	}
	s.mu.Unlock()

	if !owned || admin == nil {
		return nil
	}

	return admin.Close()
}

// requireStore is the guard every operation runs before touching the registry.
func (s *EventSubscriberService) requireStore() (eventSubscriberStore, error) {
	if s == nil || s.store == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The subscriber registry is unavailable",
			errSubscriberStoreUnavailable,
		)
	}

	return s.store, nil
}

// clock reads the service's clock, tolerating a zero-valued service.
func (s *EventSubscriberService) clock() time.Time {
	if s == nil || s.now == nil {
		return time.Now().UTC()
	}

	return s.now().UTC()
}

// budget reads the configured issuance ceiling, tolerating a zero-valued service.
func (s *EventSubscriberService) budget() time.Duration {
	if s == nil || s.issuanceBudget <= 0 {
		return SubscriberCredentialIssuanceBudget
	}

	return s.issuanceBudget
}

// compensationReserve is how much of the budget this service holds back for
// compensation.
func (s *EventSubscriberService) compensationReserve() time.Duration {
	reserve := s.budget() / 4
	if reserve > subscriberCompensationReserve {
		return subscriberCompensationReserve
	}

	return reserve
}

// ---------------------------------------------------------------------------------------
// The derivations, and why each is derived rather than chosen

// SubscriberKafkaPrincipal derives the SASL/SCRAM username for a subscriber id.
//
// Parameters:
//   - subscriberID string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<subscriber_id>".
//   - error: a typed validation error when the identifier cannot be canonicalized,
//     which also means it cannot be provisioned at all.
func SubscriberKafkaPrincipal(subscriberID string) (string, error) {
	principal, err := model.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		return "", invalidSubscriberIdentifier(err)
	}

	return principal, nil
}

// SubscriberConsumerGroupID derives the consumer group a subscriber reads under by
// default.
//
// Parameters:
//   - subscriberID string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<subscriber_id>.default".
//   - error: a typed validation error when the identifier cannot be canonicalized.
func SubscriberConsumerGroupID(subscriberID string) (string, error) {
	group, err := model.CanonicalConsumerGroupID(subscriberID)
	if err != nil {
		return "", invalidSubscriberIdentifier(err)
	}

	return group, nil
}

// SubscriberConsumerGroupNamespace derives the PREFIXED ACL resource name that reserves
// a subscriber's whole consumer-group namespace.
//
// Parameters:
//   - subscriberID string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<subscriber_id>." — the trailing terminator is significant.
//   - error: a typed validation error when the identifier cannot be canonicalized.
func SubscriberConsumerGroupNamespace(subscriberID string) (string, error) {
	namespace, err := model.CanonicalConsumerGroupNamespace(subscriberID)
	if err != nil {
		return "", invalidSubscriberIdentifier(err)
	}

	return namespace, nil
}

// SubscriberPartitionKeyPrefix reports the key-scoped constraint recorded for a
// subscriber, flattening the nullable column to a plain string.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the registry row. Nil yields "".
//
// Returns:
//   - string: the recorded constraint, or "" when none was recorded.
func SubscriberPartitionKeyPrefix(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.PartitionKeyPrefix == nil {
		return ""
	}

	return *subscriber.PartitionKeyPrefix
}

// invalidSubscriberIdentifier wraps an identifier failure in the typed validation error
// the API layer answers 400 with.
func invalidSubscriberIdentifier(cause error) error {
	return apierror.NewAPIError(
		apierror.ErrGenValidation,
		"The subscriber ID cannot be used to derive a Kafka identity",
		cause,
	)
}

// generateSubscriberPassword mints a SASL/SCRAM secret with crypto/rand.
func generateSubscriberPassword() (string, error) {
	alphabetSize := big.NewInt(int64(len(subscriberPasswordAlphabet)))

	for attempt := 1; attempt <= subscriberPasswordGenerationAttempts; attempt++ {
		password := make([]byte, generatedSubscriberPasswordLength)

		for i := range password {
			index, err := rand.Int(rand.Reader, alphabetSize)
			if err != nil {
				// The CSPRNG failing is a system-level fault, not a subscriber problem. It is
				// reported verbatim because there is nothing sensitive in it: the error describes
				// the source, never a drawn value.
				return "", fmt.Errorf(
					"event subscriber: reading %d bytes from the system CSPRNG to generate a "+
						"SASL credential: %w", generatedSubscriberPasswordLength, err,
				)
			}

			password[i] = subscriberPasswordAlphabet[index.Int64()]
		}

		candidate := string(password)
		if err := validateSCRAMPassword(candidate); err != nil {
			// Not returned: a draw that fails a strength floor is re-drawn, and the error is
			// deliberately NOT logged either, because the only way it can occur is a
			// pathologically repetitive draw and logging its diagnosis would invite somebody to
			// log the value beside it.
			continue
		}

		return candidate, nil
	}

	return "", fmt.Errorf(
		"event subscriber: %d generated SASL credentials in a row failed the strength floors "+
			"(minimum %d characters over at least %d distinct symbols); the alphabet or the length "+
			"constant has been changed to something that cannot satisfy them",
		subscriberPasswordGenerationAttempts, MinSCRAMPasswordLength, MinSCRAMPasswordDistinctChars,
	)
}

// validateSubscriberGrant checks an authorised-topic list against the ALLOWLIST.
func validateSubscriberGrant(topics []string) error {
	if err := model.ValidateSubscriberTopics(topics); err != nil {
		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The authorized topic list is not acceptable",
			err,
		)
	}

	for _, topic := range topics {
		if IsSubscriberGrantableTopic(topic) {
			continue
		}

		// The privileged name gets its own message, for the reason the DTO's validator gives:
		// "not grantable" sends an operator to read the allowlist, and the allowlist does not
		// contain the answer when the missing piece is a deployment declaration.
		if IsSubscriberPrivilegedTopic(topic) {
			return apierror.NewAPIError(
				apierror.ErrGenValidation,
				"The internal category topic is grantable only where the deployment has acknowledged it",
				fmt.Errorf(
					"topic %q carries Blnk's own error text verbatim and every event type the catalogue "+
						"does not yet recognise, so it is grantable only where "+
						"KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS is set; the topics grantable here are %s",
					sanitizeLogValue(topic, maxLoggedFilterLength),
					strings.Join(SubscriberGrantableTopics(), ", "),
				),
			)
		}

		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"Authorized topics must be Blnk-owned category topics",
			fmt.Errorf(
				"topic %q does not exist or is not grantable; the grantable topics are %s",
				sanitizeLogValue(topic, maxLoggedFilterLength),
				strings.Join(SubscriberGrantableTopics(), ", "),
			),
		)
	}

	return nil
}

// normalizeSubscriberName trims, bounds and sanity-checks the human label. CWE-20.
func normalizeSubscriberName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber name is required",
			errors.New("event subscriber: name is blank, and an unnamed principal cannot be triaged"),
		)
	}

	if count := utf8.RuneCountInString(trimmed); count > maxSubscriberNameLength {
		return "", apierror.NewAPIError(
			apierror.ErrGenValidation,
			fmt.Sprintf("A subscriber name must be at most %d characters", maxSubscriberNameLength),
			fmt.Errorf(
				"event subscriber: name is %d characters, over the %d-character bound; the column is "+
					"TEXT and bounds nothing, and the label is echoed in API responses and log lines",
				count, maxSubscriberNameLength,
			),
		)
	}

	for _, character := range trimmed {
		if character < 0x20 || character == 0x7f {
			return "", apierror.NewAPIError(
				apierror.ErrGenValidation,
				"A subscriber name must not contain control characters",
				errors.New(
					"event subscriber: name carries a control character; a newline in a label forges a "+
						"second line in any log that renders it, and an escape sequence can rewrite a "+
						"terminal",
				),
			)
		}
	}

	return trimmed, nil
}

// normalizeSubscriberKeyScope normalizes a requested partition-key scope into the value
// the registry will hold, or reports why it cannot be held.
func normalizeSubscriberKeyScope(prefix *string) (*string, error) {
	if prefix == nil {
		return nil, nil
	}

	value := *prefix
	if strings.TrimSpace(value) == "" {
		// BLANK IS NOT AN ERROR, it is the clearing case: a caller removing a scope sends the
		// field with an empty value, and a row recording "" would make RequestedKeyScope
		// non-empty while filtering nothing, which is the whitespace-only defect below in
		// another guise.
		return nil, nil
	}

	if value != strings.TrimSpace(value) {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The partition key prefix must not have surrounding whitespace",
			fmt.Errorf(
				"event subscriber: partition key prefix %q has surrounding whitespace; a key prefix "+
					"differing from another only by whitespace filters a different set of records, so "+
					"trimming it silently would change the boundary the subscriber was told it has",
				sanitizeLogValue(value, maxLoggedFilterLength),
			),
		)
	}

	if len(value) > maxSubscriberKeyScopeLength {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The partition key prefix is too long",
			fmt.Errorf(
				"event subscriber: partition key prefix is %d characters, above the %d permitted",
				len(value), maxSubscriberKeyScopeLength,
			),
		)
	}

	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return nil, apierror.NewAPIError(
				apierror.ErrGenValidation,
				"The partition key prefix must not contain control characters",
				errors.New(
					"event subscriber: the partition key prefix contains a control character, which "+
						"corrupts every log line and dashboard label it appears in",
				),
			)
		}
	}

	// Copied rather than aliased: the caller's pointer may be reused or mutated, and the row
	// this value lands on is retained by the repository.
	normalized := value

	return &normalized, nil
}

// A subscriber whose row records a partition-key prefix Kafka's authorizer has no
// dimension for is reported by logKeyScopeDisclosure, on the issuance path — ONE
// key-scope diagnostic per issuance, fired where the credential's shape is decided. It
// says what that shape is: Describe and no topic Read, with delivery through the
// key-authorising component the deployment declared, or a refusal to issue at all where
// none is declared.

// subscriberKeyPrefix reads a subscriber's recorded partition-key prefix, trimmed, or
// "" when none is recorded.
func subscriberKeyPrefix(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.PartitionKeyPrefix == nil {
		return ""
	}

	return strings.TrimSpace(*subscriber.PartitionKeyPrefix)
}
