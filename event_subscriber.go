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
	"slices"
	"sort"
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
//
// Enforcing it HERE rather than relying on the admin client's own request timeout is
// deliberate. That timeout is per round trip, so four of them serialised can exceed
// this budget while every individual call looks healthy.
const SubscriberCredentialIssuanceBudget = 5 * time.Second

// subscriberCompensationReserve is how much of the issuance budget is HELD BACK so that
// a failure can still be compensated inside the same absolute deadline.
const subscriberCompensationReserve = 1250 * time.Millisecond

// generatedSubscriberPasswordLength is how many characters a generated SASL secret
// carries.
const generatedSubscriberPasswordLength = 48

// subscriberPasswordAlphabet is the alphabet a generated secret is drawn from:
// alphanumerics only, deliberately.
const subscriberPasswordAlphabet = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789"

// subscriberPasswordGenerationAttempts bounds the retries that guarantee the generated
// secret clears the distinct-character floor validateSCRAMPassword applies.
const subscriberPasswordGenerationAttempts = 8

// errSubscriberStoreUnavailable reports a service built with no datasource.
//
// NewBlnk(nil) is a supported construction in this codebase — several tests use it — so
// a service can legitimately hold a nil store, and every operation reports this instead
// of dereferencing nil. It is a sentinel so a test can assert the condition without
// matching on message text.
var errSubscriberStoreUnavailable = errors.New(
	"event subscriber: no datasource is configured, so the subscriber registry is unavailable",
)

// SubscriberErrorDetail is the detail attached to a typed API error whose cause came
// from outside Blnk — the Kafka client, the database driver, or the TLS material on
// disk.
//
// What those causes actually contain is the reason this matters:
//
//   - A KAFKA CLIENT error renders as "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe",
//     naming internal addresses and broker topology. *net.OpError is a struct with exported
//     Op, Net and Addr fields, so marshalling one into a JSON response publishes them as
//     structured data rather than merely as text.
//   - A POSTGRES error renders with the schema, table, column, constraint, source file and
//     routine that produced it.
//   - A TLS MATERIAL error names a filesystem path inside the container.
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
// The cause is deliberately NOT a parameter. Excluding it from the signature is what
// makes the guarantee structural rather than a matter of care at each call site: there
// is no way to pass a cause in, so no rendering of the result — as JSON, with %v, or
// field by field — can contain one.
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
//
// It is a NARROW seam over database.IDataSource for two reasons. It makes every
// registry operation testable with an in-memory fake and no PostgreSQL, which is what
// lets the secret-handling assertions run in the unit suite.
//
// NO METHOD HERE ACCEPTS OR RETURNS A PLAINTEXT SECRET, and none may be added that
// does. RecordSubscriberCredentialIfUnchanged takes a REFERENCE the caller has already
// derived, which is what makes "the secret is not persisted" a property of the contract
// rather than a habit of its callers.
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
	// while a new owner is still provisioning the stored reference is unchanged, so a
	// caller whose lease expired would match and win.
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
//
// The seam also removes the broker from the unit tests entirely. Provisioning is where
// the interesting failure modes are (a timeout, an ACL grant that fails after the
// credential was written), and a fake is the only way to drive those deterministically.
type subscriberPrincipalProvisioner interface {
	// IsConfigured reports whether any broker is configured. False means issuance cannot
	// succeed and is answered with ErrKafkaUnavailable before anything is generated.
	IsConfigured() bool

	// Brokers returns the bootstrap list the credential response hands to the subscriber.
	// Reading it from the client that dials rather than from configuration a second time
	// is what guarantees the two agree.
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
//
// It is the service-layer input, distinct from api/model.CreateSubscriber, so that the
// registry is usable from the CLI, a migration or a test without constructing an HTTP
// request body. The two carry the same information; the DTO additionally carries the
// binding tags and the wire names.
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
	// Empty is VALID and means authorised for nothing, which is the fail-closed default of
	// a fresh registration rather than a missing value.
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
//
// The principal, the consumer group, the credential record and the migration instant
// are all absent for the reasons api/model.UpdateSubscriber sets out: the first two are
// derived boundaries rather than attributes, and the last two are written only by the
// operations that own them.
type SubscriberUpdate struct {
	// Name replaces the human label when non-nil. A blank replacement is refused, because
	// NOT NULL does not stop an empty string and an unnamed principal cannot be triaged.
	Name *string

	// AuthorizedTopics replaces the whole authorised set when NON-NIL, including when it
	// is non-nil and empty, which revokes every topic grant on the row.
	//
	// A slice is already nilable, so no pointer is needed to tell "omitted" from "set to
	// empty".
	//
	// CHANGING THIS LIST CHANGES WHAT THE SUBSCRIBER CAN READ, and it does so at the
	// broker as part of the update rather than at the next issuance.
	//
	// UpdateSubscriber reconciles the broker-side grant in three steps around the write:
	// the bindings the new list no longer implies are DELETED first, the row is persisted
	// second, and the bindings it adds are created third. That order is what keeps every
	// partial failure fail-closed — a narrowing is in force before the registry claims it,
	// and a widening reaches the broker only after the registry records it.
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
//
// LogFields is the safe way to log an issuance; it is composed of non-secret values
// only.
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
	// Reading anything outside it fails authorization at the broker.
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
//
// It exists because the credential endpoint has to put the secret in its response body;
// there is no other legitimate caller. It reads the value out of this in-memory result
// — it does NOT read anything back from storage, and it cannot, because nothing was
// stored.
func (c SubscriberCredential) Password() string {
	return c.password.reveal()
}

// PasswordLength reports how long the generated secret is, without revealing it.
//
// The length is the one property of a secret that is safe to publish, and it is what
// lets a test assert that a credential of the expected strength was generated without
// the value appearing in the test's output on failure.
//
// Returns:
//   - int: the secret's length in bytes; zero when none is held.
func (c SubscriberCredential) PasswordLength() int {
	return c.password.Len()
}

// KeyScope reports this credential's key boundary in the form a consumer is told it,
// together with whether THE BROKER keeps that boundary.
//
// A caller must not be able to read the prefix without reading who enforces it.
// Delivered on its own, a prefix says nothing about where it is applied.
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
	// Kafka's authorizer has no message-key dimension, so no ACL evaluates the prefix —
	// which is why such a credential holds no topic Read at all and its records are
	// delivered, key-filtered, by the declared component. The boundary IS enforced, and
	// KeyScopeEnforcement on this credential names the component that enforces it.
	return trimmed, false
}

// issuedKeyScopeEnforcement resolves the enforcement point to record on an ISSUED
// credential.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row being issued for.
//   - enforced bool: whether config.KafkaConfig.KeyScopeGateway reported active
//     enforcement, read once by the caller so the refusal and this value cannot
//     disagree.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row being issued for.
//   - enforced bool: whether config.KafkaConfig.KeyScopeGateway reported active
//     enforcement, read once by the caller so the refusal and this value cannot
//     disagree.
//
// Returns:
//   - model.KeyScopeEnforcementStatus: broker_gateway for a key-scoped subscriber under
//     active enforcement, otherwise none.
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
//
// Every value here is non-secret by construction: the fingerprint is a 48-bit prefix of
// a keyed digest, the topics and the group are already public to the subscriber, and
// the password appears only as its LENGTH. Logging through this rather than the struct
// is what keeps an issuance auditable without making it disclosable.
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
// Implementing fmt.Formatter rather than only fmt.Stringer is what closes `%+v`, `%#v`,
// `%q` and `%x`: fmt consults Formatter first and ignores Stringer entirely when one is
// present, so a single method covers the verbs a Stringer never reaches.
//
// Parameters:
//   - state fmt.State: the destination fmt is writing to.
//   - _ rune: the verb, deliberately ignored — every verb gets the same safe rendering.
func (c SubscriberCredential) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(c.String()))
}

// EventSubscriberService owns the subscriber registry and credential issuance.
//
// One instance is enough for a process. It is safe for concurrent USE once configured:
// the store, the clock, the generator and the budget are read-only after construction,
// and the lazily resolved administrative client is guarded by a mutex.
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
//
// The store may be nil — NewBlnk(nil) is a supported construction — and every operation then
// reports a typed unavailability error rather than dereferencing nil. The administrative client
// is resolved lazily unless one is injected; see the type's documentation for which to prefer.
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
//
// Production must not call this. It exists so a test can assert that the budget is
// enforced without waiting five seconds, and so an operator running against a
// deliberately slow staging broker can be given a longer, explicitly chosen ceiling.
func (s *EventSubscriberService) WithIssuanceBudget(budget time.Duration) *EventSubscriberService {
	if budget <= 0 {
		budget = SubscriberCredentialIssuanceBudget
	}

	s.issuanceBudget = budget

	return s
}

// WithKafkaAdmin injects an already-built administrative client.
//
// The injected client is NOT owned by this service and is never closed by it, which is
// what makes it safe to hand over the process's shared client. Passing nil clears an
// injected client and returns the service to resolving one lazily.
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
// Passing nil clears an injected client and returns the service to building one from
// live configuration, which is what production does.
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
//
// It is cheap. The client is a struct with an http.Client whose transport pools four
// idle connections; issuance is not a hot path, and building one per credential costs
// less than the TLS handshake the call itself performs.
//
// Returns:
//   - KeyScopeGatewayClient: the injected client when a test installed one, otherwise a
//     client built from live configuration.
//   - error: ErrKeyScopeGatewayNotConfigured when no usable endpoint is declared.
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
//
// Parameters:
//   - ctx context.Context: the forward-path context, already bounded by the issuance
//     budget.
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: nil when the component attested the exact recorded prefix for this exact
//     principal; otherwise a typed ErrSubscriberKeyScopeUnattested.
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
//
// So a failure is LOGGED AND RETURNED to the caller as information, never allowed to
// fail a deregistration whose broker half has already succeeded. Failing it would leave
// an operator unable to remove a subscriber while a component is down, in order to
// complete a cleanup for access that is already gone.
//
// Parameters:
//   - ctx context.Context: a bounded cleanup context.
//   - subscriber *model.EventSubscriber: the row being deregistered.
//
// Returns:
//   - error: the component's failure, for the caller to log. Nil when there was nothing
//     to do
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
//
// Configuration is read through fetchConfiguration — the package's configuration seam,
// declared in event_sunset.go — rather than config.Fetch directly, so a test that swaps
// it sees consistent behaviour across every event file.
//
// Returns:
//   - subscriberPrincipalProvisioner: the client, never nil when the error is nil.
//   - error: a typed ErrKafkaUnavailable when no client can be built.
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
// It is idempotent and nil-safe, and it NEVER closes an injected client: a client
// passed in belongs to whoever built it and usually outlives this service. That
// ownership rule is what makes Close safe to call from a handler's defer without
// knowing where the client came from.
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
//
// Returns:
//   - eventSubscriberStore: the store, never nil when the error is nil.
//   - error: a typed internal error wrapping errSubscriberStoreUnavailable.
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
//
// Returns:
//   - time.Time: the current instant in UTC. Issuance timestamps are stored and compared
//     across processes, so they are normalised here rather than at each call site.
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
//
// The reserve is capped at subscriberCompensationReserve so a deployment that RAISES
// the budget does not hand compensation more time than the two round trips and two
// writes it performs actually need.
//
// Returns:
//   - time.Duration: the slice of the budget kept back, always less than the budget
//     itself.
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
// It is RECORDED, not derived, and that is not an omission. Every other value in the
// access model — the principal, the consumer group — is derived from the subscriber's
// identity; this one cannot be, because it lives in a different namespace.
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
//
// crypto/rand and NOT math/rand: this is a credential, and a predictable one is no
// credential at all. An error from the reader is returned rather than falling back to a
// weaker source, because a failed issuance is recoverable and a guessable password is
// not.
//
// Returns:
//   - string: a password of generatedSubscriberPasswordLength characters.
//   - error: the reader's error, unwrapped.
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
//
// An EMPTY LIST IS VALID and means "authorised for nothing", which is the fail-closed
// default of a newly registered subscriber. A duplicate is rejected rather than folded,
// because a duplicate in a grant request is a caller error worth reporting.
//
// Returns:
//   - error: a typed validation error naming the offending topic, or nil.
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
//
// NOT NULL does not stop an empty string, and a blank name defeats the reason the
// registry exists: it is the value an operator recognises a principal by months later,
// and no service can invent a meaningful one.
//
// Parameters:
//   - name string: the supplied label.
//
// Returns:
//   - string: the trimmed label.
//   - error: a typed validation error when it is blank, over the bound, or carries a
//     control character.
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
//
// Because every Blnk event is keyed by ledger id, that is a ledger boundary.
//
// Parameters:
//   - prefix *string: the supplied constraint. Nil means none was requested, and a
//     blank or whitespace-only value means the same thing — the clearing case.
//
// Returns:
//   - *string: the value to record, or nil for "no key scope".
//   - error: a typed validation error for a value that cannot be recorded honestly.
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
//
// A nil row and a nil column both answer "", so callers on read paths — where a
// repository's not-found representation is a nil pointer — need no guard of their own.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row. May be nil.
//
// Returns:
//   - string: the trimmed prefix, or "" when absent or blank.
func subscriberKeyPrefix(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.PartitionKeyPrefix == nil {
		return ""
	}

	return strings.TrimSpace(*subscriber.PartitionKeyPrefix)
}

// requireRecordableWebhookURL refuses to record a legacy webhook endpoint once the
// retirement instant has passed.
//
// Parameters:
//   - webhookURL *string: the requested value. nil means the caller did not mention the
//     field.
//
// Returns:
//   - error: apierror.ErrGenGone — which the catalogue maps to 410 — when a non-empty
//     URL is offered after the retirement instant. nil otherwise.
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
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row. May be nil.
//
// Returns:
//   - string: an RFC3339 instant, or a fixed phrase.
func subscriberCredentialIssuedAt(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.CredentialIssuedAt == nil {
		return "an unrecorded time"
	}

	return subscriber.CredentialIssuedAt.UTC().Format(time.RFC3339)
}

// requireProvisionableKeyScope refuses to mint a credential for a subscriber whose row
// records a partition key prefix WHILE THIS DEPLOYMENT DECLARES NOTHING ABLE TO ENFORCE
// ONE.
//
// So issuance fails closed, and says exactly what to do about it. ALL THREE remedies
// are real and all three are named.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//   - enforced bool: whether config.KafkaConfig.KeyScopeGateway reported an active,
//     attestable enforcement point.
//
// Returns:
//   - error: a typed conflict when the row records a key scope this deployment cannot
//     enforce, otherwise nil.
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
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//   - enforced bool: whether key-scope enforcement is active AND attestable.
//
// Returns:
//   - error: a typed conflict when a key-scoped deployment would mint a whole-topic
//     credential, otherwise nil.
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
//
// It is not a claim that whole-topic access is a defect. It is the access model the
// requirement mandates: category topics, no per-tenant topics, and an authorizer with
// no message-key dimension.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned, named in the
//     refusal.
//   - enforced bool: whether key-scope enforcement is active.
//
// Returns:
//   - error: a typed conflict when a secure deployment has declared neither model,
//     otherwise nil.
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
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as it WOULD be written, with the
//     requested changes already applied.
//
// Returns:
//   - error: a typed conflict when the row would record a key scope while holding a
//     credential, otherwise nil.
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
//
// Issuing against that state is a different matter. The credential is minted, the SASL
// principal authenticates, and it holds no topic binding whatsoever: it can list
// nothing and read nothing.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: a typed conflict when the subscriber has no authorised topic, otherwise
//     nil.
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
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be changed.
//   - refusal string: the message describing which operation is being refused.
//
// Returns:
//   - error: a typed conflict when the subscriber is being deregistered, otherwise nil.
func requireActiveSubscriber(subscriber *model.EventSubscriber, refusal string) error {
	if subscriber == nil || !subscriber.IsRevocationPending() {
		return nil
	}

	return apierror.NewAPIError(
		apierror.ErrConflict,
		refusal,
		fmt.Errorf(
			"event subscriber: subscriber %q carries a revocation tombstone from %s; complete or "+
				"reverse its deregistration first",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			subscriber.RevocationPendingAt.UTC().Format(time.RFC3339),
		),
	)
}

// subscriberBrokerPhaseBudget bounds ONE broker phase of a fenced operation.
//
// That is what made the fence lease impossible to size. A lease long enough to cover
// the worst case would fence a subscriber for a minute after a crash, and a lease short
// enough to recover from a crash quickly expires mid-phase — after which the operation
// keeps going and writes under a claim it no longer owns.
const subscriberBrokerPhaseBudget = 3 * SubscriberCredentialIssuanceBudget

// SubscriberProvisioningFenceLease is how long an issuance or revocation holds its
// fence.
const SubscriberProvisioningFenceLease = subscriberBrokerPhaseBudget +
	subscriberCleanupBudget + SubscriberCredentialIssuanceBudget

// subscriberFence is a held provisioning claim, and the only way to reach the broker.
//
// So nothing touches the broker for a subscriber without holding its claim, and the
// second caller is refused with a conflict BEFORE it generates a secret. A refused
// issuance costs a caller one retry; an interleaved one costs it a credential that does
// not work.
type subscriberFence struct {
	store        eventSubscriberStore
	subscriberID string

	// token is the claim this operation holds. It is a CONCURRENCY-CONTROL SECRET: it must
	// never reach an API response or a log line, because a caller holding another
	// operation's token could complete writes against a subscriber it does not own. It is
	// deliberately absent from model.EventSubscriber for the same reason.
	token string

	// lease is how long one claim renewal is good for, carried so the heartbeat renews on the
	// same term the claim was taken on.
	lease time.Duration

	// stop ends the heartbeat; done is closed once it has exited, so release can wait for it
	// and no renewal can land after the claim has been cleared.
	stop chan struct{}
	done chan struct{}

	stopOnce sync.Once
	released sync.Once
}

// fenceSubscriber claims a subscriber for one operation and returns the claim.
//
// Parameters:
//   - ctx context.Context: the operation's context. Its VALUES are reused for the
//     release.
//   - store eventSubscriberStore: the registry.
//   - subscriberID string: the subscriber to fence.
//
// Returns:
//   - *subscriberFence: the held claim. NEVER NIL, so a caller can defer its release
//     unconditionally even on the error path.
//   - error: a typed conflict when another operation holds the claim, a typed not-found
//     when the subscriber does not exist, or the repository's error.
func fenceSubscriber(
	ctx context.Context,
	store eventSubscriberStore,
	subscriberID string,
) (*subscriberFence, error) {
	token, err := store.ClaimSubscriberForProvisioning(
		ctx, subscriberID, SubscriberProvisioningFenceLease)
	if err != nil {
		// NIL, NOT A FENCE WITH NO TOKEN. A half-built fence looks like a held claim to every
		// caller that does not inspect its fields: `defer fence.release(ctx)` on one would
		// issue a release for a claim this call never took, and `renew` has to carry an
		// explicit guard for a state that should not be constructible. Returning nil makes
		// the refusal unmistakable, and release is deliberately nil-safe — see
		// TestSubscriberFence_ReleaseIsIdempotentAndNilSafe — so a caller may still defer the
		// release before it knows whether the claim succeeded.
		return nil, err
	}

	fence := &subscriberFence{
		store:        store,
		subscriberID: strings.TrimSpace(subscriberID),
		token:        token,
	}

	// THE CLAIM IS RENEWED WHILE THE OPERATION RUNS. A provisioning claim is a LEASE, and
	// the operations that hold one make several broker round trips under it — so a slow
	// broker, or a compensation that outlives the request, could let the lease expire
	// underneath an operation still working. The heartbeat renews it on a fraction of its
	// term, which is what keeps the fence true for the whole operation rather than only
	// for its first few seconds.
	fence.lease = SubscriberProvisioningFenceLease
	fence.stop = make(chan struct{})
	fence.done = make(chan struct{})

	go fence.heartbeat()

	return fence, nil
}

// renew proves the claim is still this caller's and extends it by one lease.
//
// This is the half of the fix that keeps a long operation legitimate. The lease is
// short because it is also the recovery time after a crash, so an operation making
// several broker phases cannot fit inside one lease and must not try to — it says it is
// still working, and gets one more lease of headroom.
//
// Parameters:
//   - ctx context.Context: cancels the renewal.
//
// Returns:
//   - error: a typed conflict when the claim is no longer this caller's, a typed
//     not-found when the row is gone, or the repository's error.
func (f *subscriberFence) renew(ctx context.Context) error {
	if f == nil || f.token == "" {
		// Unreachable through fenceSubscriber, which returns the claim and the error
		// together. Reported rather than treated as "held" because a fence with no token
		// constrains nothing, and silently proceeding would restore the unfenced behaviour
		// exactly.
		return apierror.NewAPIError(apierror.ErrConflict,
			"No provisioning claim is held for this subscriber, so the operation was abandoned",
			errors.New("event subscriber: a broker phase was attempted without a provisioning claim"))
	}

	return f.store.RenewSubscriberProvisioningFence(
		ctx, f.subscriberID, f.token, SubscriberProvisioningFenceLease)
}

// consumed records that the claim went away with the row it was held on.
//
// The provisioning columns live ON the subscriber row, so a successful deregistration
// deletes the claim along with everything else. The deferred release then matched no
// row and reported a conflict — "the claim expired or was taken over" — on the one path
// where the claim's disappearance is the INTENDED outcome.
func (f *subscriberFence) consumed() {
	if f == nil {
		return
	}

	// The Once is CONSUMED rather than the token blanked, so an explicit release already made
	// before this point is still not repeated, and a deferred release after it does nothing.
	f.released.Do(func() {})
}

// release clears the claim so a retry need not wait out the lease.
//
// A claim taken by an issuance whose budget then expired must still be released, or the
// subscriber stays fenced until the lease runs out — turning one slow broker call into
// half a minute of refused retries. So the release is bounded by its own budget rather
// than by the caller's remaining time, exactly as every other cleanup here is.
//
// Parameters:
//   - ctx context.Context: the operation's context, used for its VALUES only.
func (f *subscriberFence) release(ctx context.Context) {
	if f == nil || f.token == "" {
		return
	}

	f.released.Do(func() {
		// STOPPED AND WAITED FOR FIRST, so no renewal can land after the claim has been cleared
		// and re-fence a subscriber this operation has finished with.
		f.stopHeartbeat()

		releaseCtx, cancel := subscriberCleanupContext(ctx)
		defer cancel()

		if err := f.store.ReleaseSubscriberProvisioningFence(
			releaseCtx, f.subscriberID, f.token); err != nil {
			withLoggableCause(logrus.WithField(
				"subscriber_id_hash", subscriberLogLabel(f.subscriberID),
			), err).Warn(
				"event subscriber: releasing the provisioning fence failed; the subscriber stays " +
					"fenced until its claim lease expires, after which the next attempt proceeds",
			)
		}
	})
}

// brokerPhaseContext bounds one broker phase and confirms the claim before it starts.
//
// It is the single place the two halves of the fix meet, so no fenced operation can
// acquire one without the other: a caller that wants to reach the broker asks for a
// phase context, and the renewal that proves ownership happens on the way to getting
// one.
//
// The returned context is DETACHED from the caller's cancellation in neither direction
// — it derives from ctx, so a client that disconnects still aborts the phase — but it
// adds a deadline the caller may not have supplied, which is what turns an unbounded
// sequence of round trips into a bounded one.
//
// Parameters:
//   - ctx context.Context: the operation's context.
//
// Returns:
//   - context.Context: bounded by subscriberBrokerPhaseBudget, or by the caller's own
//     deadline when that is sooner.
//   - context.CancelFunc: must be called, conventionally by defer.
//   - error: the renewal's error, in which case the phase MUST NOT run.
func (f *subscriberFence) brokerPhaseContext(
	ctx context.Context,
) (context.Context, context.CancelFunc, error) {
	if err := f.renew(ctx); err != nil {
		return ctx, func() {}, err
	}

	phase, cancel := context.WithTimeout(ctx, subscriberBrokerPhaseBudget)

	return phase, cancel, nil
}

// subscriberCleanupBudget bounds a compensating write when no absolute deadline is in
// force.
const subscriberCleanupBudget = SubscriberCredentialIssuanceBudget

// subscriberIssuanceDeadlineKey is the context key the absolute issuance deadline travels on.
//
// An unexported struct type, which is the standard way to make a context key that no other
// package can collide with or read by accident.
type subscriberIssuanceDeadlineKey struct{}

// withSubscriberIssuanceDeadline attaches the ONE absolute deadline an issuance is
// bounded by.
//
// So the deadline is carried SEPARATELY, as a value, which survives WithoutCancel. A
// cleanup then re-imposes it deliberately: detached from cancellation, still bounded by
// the same instant.
//
// Parameters:
//   - ctx context.Context: the request's context.
//   - deadline time.Time: the absolute instant the whole issuance must be finished by.
//
// Returns:
//   - context.Context: ctx carrying the deadline as a value.
func withSubscriberIssuanceDeadline(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, subscriberIssuanceDeadlineKey{}, deadline)
}

// subscriberIssuanceDeadline reads the absolute issuance deadline, if one is in force.
//
// Parameters:
//   - ctx context.Context: the context to read.
//
// Returns:
//   - time.Time: the deadline.
//   - bool: false when the caller is not under an issuance deadline, which is the
//     normal case for deregistration, revocation and the migration sweeps.
func subscriberIssuanceDeadline(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}

	deadline, ok := ctx.Value(subscriberIssuanceDeadlineKey{}).(time.Time)
	if !ok || deadline.IsZero() {
		return time.Time{}, false
	}

	return deadline, true
}

// subscriberCompensationFloor is the smallest window a compensating operation is ever
// given.
//
// In the ordinary case it never applies. The forward path runs on a deadline of
// absolute-minus-reserve and compensation runs to absolute, so the reserve IS the
// compensation window and this floor is never reached.
const subscriberCompensationFloor = subscriberCompensationReserve

// boundedCompensationDeadline resolves the instant a compensating operation must finish
// by.
//
// Parameters:
//   - ctx context.Context: the failing operation's context, read for the issuance
//     deadline.
//   - fallback time.Duration: the budget to use when no issuance deadline is in force.
//
// Returns:
//   - time.Time: the deadline to impose, always strictly in the future.
func boundedCompensationDeadline(ctx context.Context, fallback time.Duration) time.Time {
	now := time.Now()
	ceiling := now.Add(fallback)

	absolute, ok := subscriberIssuanceDeadline(ctx)
	if !ok {
		return ceiling
	}

	// The forward path overran the absolute instant, so there is nothing left of the
	// reserve to compensate inside. Grant the floor: bounded, named, and once per request
	// because the cleanup helpers rebase the value they read.
	if !absolute.After(now) {
		absolute = now.Add(subscriberCompensationFloor)
	}

	if absolute.Before(ceiling) {
		return absolute
	}

	return ceiling
}

// subscriberCleanupContext derives the context a compensating write runs on.
//
// A fresh context detached from the caller's cancellation fixes that, and it is BOUNDED
// rather than merely detached for the same reason the relay's bookkeeping context is:
// finishing what is owed must not become blocking indefinitely on a system that has
// gone away.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values and its issuance
//     deadline.
//
// Returns:
//   - context.Context: detached from cancellation, bounded by the earlier of the
//     issuance deadline and subscriberCleanupBudget, and carrying that instant forward
//     as the issuance deadline so nested cleanups share this window instead of opening
//     another.
//   - context.CancelFunc: must be called, conventionally by defer.
func subscriberCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// ONE PHASE BUILDER, so this and kafkaCleanupContext cannot disagree about when a
	// compensation ends. Both were written for the same bound and arrived reading two
	// different context keys, which meant the wall clock one of them attached was
	// invisible to the other and every broker cleanup silently took a fresh ten seconds.
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, subscriberCleanupBudget, true)
}

// requireSubscriberIdentifier rejects a blank subscriber id before anything is spent on
// it.
//
// Failing in memory rather than at the database matters twice over: it preserves the
// issuance budget for a request that cannot succeed anyway, and it answers "required"
// instead of the misleading "Subscriber not found" an empty WHERE match would produce.
//
// Parameters:
//   - subscriberID string: the identifier as supplied.
//
// Returns:
//   - error: a typed validation error when blank, otherwise nil.
func requireSubscriberIdentifier(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber ID is required",
			errors.New("event subscriber: the subscriber id is blank"),
		)
	}

	return nil
}

// ---------------------------------------------------------------------------------------
// Registry CRUD

// RegisterSubscriber records a new subscriber and returns the stored row.
//
// A PARTITION KEY PREFIX is not: normalizeSubscriberKeyScope refuses every non-blank
// value here, because Kafka's authorizer has no message-key dimension and a recorded
// prefix would state a boundary no credential for the row can have. Narrow the
// authorised topics instead — that is enforced at the broker.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: a typed validation error, a typed conflict when the id or principal is
//     taken, or the repository's error.
func (s *EventSubscriberService) RegisterSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	// An ABSENT identifier is generated rather than refused, which is the house convention
	// every other Blnk resource follows — ledgers, balances, identities and API keys all
	// mint their own business key when the caller supplies none. The generated form is
	// "sub_<uuid>", so it satisfies the canonical identifier rule the schema's CHECK
	// constraints and the principal derivation share, and it is recognisable on sight in a
	// broker ACL listing.
	subscriberID := registration.SubscriberID
	if subscriberID == "" {
		subscriberID = model.GenerateSubscriberID()
	} else {
		canonical, canonicalErr := model.CanonicalizeSubscriberIdentifier(subscriberID)
		if canonicalErr != nil {
			return nil, invalidSubscriberIdentifier(canonicalErr)
		}

		subscriberID = canonical
	}

	name, err := normalizeSubscriberName(registration.Name)
	if err != nil {
		return nil, err
	}

	if err := validateSubscriberGrant(registration.AuthorizedTopics); err != nil {
		return nil, err
	}

	keyPrefix, err := normalizeSubscriberKeyScope(registration.PartitionKeyPrefix)
	if err != nil {
		return nil, err
	}

	principal, err := SubscriberKafkaPrincipal(subscriberID)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := SubscriberConsumerGroupID(subscriberID)
	if err != nil {
		return nil, err
	}

	if err := requireRecordableWebhookURL(registration.WebhookURL); err != nil {
		return nil, err
	}

	subscriber := &model.EventSubscriber{
		SubscriberID:       subscriberID,
		Name:               name,
		KafkaPrincipal:     principal,
		ConsumerGroupID:    consumerGroup,
		AuthorizedTopics:   registration.AuthorizedTopics,
		PartitionKeyPrefix: keyPrefix,
		WebhookURL:         registration.WebhookURL,
	}

	stored, err := store.CreateEventSubscriber(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(stored.SubscriberID),
		"principal_hash":         subscriberLogLabel(stored.KafkaPrincipal),
		"consumer_group_hash":    consumerGroupLogLabel(stored.ConsumerGroupID),
		"authorized_topic_count": len(stored.AuthorizedTopics),
		"legacy_webhook":         stored.WebhookURL != nil,
	}).Info("event subscriber: registered; no credential has been issued yet")

	return stored, nil
}

// GetSubscriber reads one subscriber by its business key.
//
// A missing subscriber is reported as apierror.ErrSubscriberNotFound — 404 — and never
// as a bare sql.ErrNoRows: the repository maps it, and this method propagates the typed
// code unchanged so the API layer answers from the domain meaning rather than
// re-deriving one.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank id, or the repository's error.
func (s *EventSubscriberService) GetSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	return store.GetEventSubscriberByID(ctx, strings.TrimSpace(subscriberID))
}

// ListSubscribers pages the registry, newest first.
//
// The page bounds are the repository's: a non-positive limit selects its default and an
// oversized one is capped, so a malformed page request degrades to a cheap query rather
// than a full table scan. They are not re-implemented here, because two independent
// sets of bounds is one more than can be kept in step.
func (s *EventSubscriberService) ListSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	store, err := s.requireStore()
	if err != nil {
		return model.SubscriberPage{}, err
	}

	return store.ListEventSubscribers(ctx, query)
}

// ListAndCountSubscribers pages the registry and counts it from ONE DATABASE SNAPSHOT.
//
// The two single-purpose reads remain for callers that want only one of the two
// answers. This is the path for the caller that wants both and needs them to agree.
//
// It does NOT promise that paging to the total exhausts the registry — paging spans
// many requests over a live registry. The total is exact as at this page.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - query model.SubscriberPageQuery: the page.
//
// Returns:
//   - model.SubscriberPage: the page, as ListSubscribers.
//   - int64: the registry size in the same snapshot.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the
//     repository's error.
func (s *EventSubscriberService) ListAndCountSubscribers(
	ctx context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return model.SubscriberPage{}, 0, err
	}

	return store.ListAndCountEventSubscribers(ctx, query)
}

// CountSubscribers reports how many subscribers the registry holds.
//
// The alternative — reporting the length of the page — is worse than the refusal was: a
// paging client that reads the total as the size of the set would stop after one full
// page, or compare a page length against itself forever.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//
// Returns:
//   - int64: the number of registered subscribers.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the
//     repository's error.
func (s *EventSubscriberService) CountSubscribers(ctx context.Context) (int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return 0, err
	}

	return store.CountEventSubscribers(ctx)
}

// UpdateSubscriber applies the mutable subset of a subscriber and returns the stored
// row.
//
// An authorization change is applied at the broker as part of the update, in three
// steps around the write, and the ORDER is the whole design:
//
//  1. PRUNE at the broker. Every Blnk-owned binding the new authorization no longer implies is
//     deleted first, so a NARROWING is in force before the registry claims it is.
//  2. PERSIST the row.
//  3. GRANT at the broker. The bindings the new authorization adds are created last, so a
//     WIDENING reaches the broker only once the registry records it.
//
// Parameters:
//   - ctx context.Context: cancels the reconciliation, the read and the write.
//   - subscriberID string: the business key of the row to update.
//   - changes SubscriberUpdate: the fields to apply.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank name or an ungrantable topic, ErrSubscriberProvisioningFailed
//     when the broker-side reconciliation did not complete, or the repository's error.
func (s *EventSubscriberService) UpdateSubscriber(
	ctx context.Context,
	subscriberID string,
	changes SubscriberUpdate,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED before anything is read, so the authorization this call reconciles cannot be
	// changed underneath it by a concurrent issuance writing the same bindings.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer fence.release(ctx)

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return nil, err
	}

	// REFUSED IN MEMORY ON A TOMBSTONED ROW, before anything is spent on it.
	if err := requireActiveSubscriber(subscriber, subscriberRefusedUpdateMessage); err != nil {
		return nil, err
	}
	// The BROKER-REPRESENTABLE authorization as the registry holds it, snapshotted before
	// the changes are applied on top. It is what authorizationNeedsReconciliation compares
	// against and what abandonUpdateAfterLostFence reports as still recorded.
	storedAuthorization := newSubscriberAuthorization(subscriber)

	// AND THE TWO FACTS THE POST-WRITE DISCLOSURE NEEDS, read here for the same reason:
	// both describe the row BEFORE this update, and once the changes are applied neither
	// can be recovered. warnOnKeyScopeRecordedAfterIssuance reports the one transition
	// whose consequence reaches backwards — a prefix recorded onto a row whose credential
	// was already delivered with a response declaring direct broker access.
	priorKeyPrefix := subscriberKeyPrefix(subscriber)
	priorCredentialIssued := subscriber.IsProvisioned()

	if changes.Name != nil {
		name, nameErr := normalizeSubscriberName(*changes.Name)
		if nameErr != nil {
			return nil, nameErr
		}

		subscriber.Name = name
	}

	// Non-nil replaces the whole set, INCLUDING when it is non-nil and empty: revoking
	// every topic grant is a legitimate instruction, and it is the only way to express
	// "authorised for nothing" on a row that currently holds topics.
	if changes.AuthorizedTopics != nil {
		if grantErr := validateSubscriberGrant(changes.AuthorizedTopics); grantErr != nil {
			return nil, grantErr
		}

		subscriber.AuthorizedTopics = changes.AuthorizedTopics
	}

	// A present empty string CLEARS the recorded key scope, which is why the column is
	// nullable and why this cannot be a plain string: NULL means "no key constraint" while
	// the empty string would mean "constrained to the empty prefix", and those are
	// opposite intents. normalizeSubscriberKeyScope owns that three-way mapping so
	// registration and update cannot disagree about it.
	if changes.PartitionKeyPrefix != nil {
		keyPrefix, prefixErr := normalizeSubscriberKeyScope(changes.PartitionKeyPrefix)
		if prefixErr != nil {
			return nil, prefixErr
		}

		subscriber.PartitionKeyPrefix = keyPrefix
	}

	// Same three-way handling for the legacy URL. Clearing it is how a migrated
	// subscriber's dual-run artefact is removed one row at a time;
	// PurgeMigratedWebhookURLs does it in bulk once a retention period has elapsed.
	if changes.WebhookURL != nil {
		if err := requireRecordableWebhookURL(changes.WebhookURL); err != nil {
			return nil, err
		}

		if strings.TrimSpace(*changes.WebhookURL) == "" {
			subscriber.WebhookURL = nil
		} else {
			webhookURL := *changes.WebhookURL
			subscriber.WebhookURL = &webhookURL
		}
	}

	// A partition key prefix recorded over a row that ALREADY HOLDS A CREDENTIAL is
	// refused unless something in front of the brokers authorises record keys. The refusal
	// is one half of a pair — requireProvisionableKeyScope guards the other order, prefix
	// first and credential afterwards — and on its own either is walked around by
	// approaching the state from the far side.
	_, keyScopeEnforced := s.keyScopeEnforcement()
	if err := requireRecordableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// AND CLEARING A PREFIX IS REFUSED WHERE THE DEPLOYMENT DECLARES A KEY-SCOPED MODEL.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// DOES THE BROKER HAVE TO BE TOUCHED AT ALL?
	//
	// THE PRESENCE OF A KEY SCOPE *IS* PART OF THIS, and its VALUE is not.
	reconcileBroker := authorizationNeedsReconciliation(storedAuthorization, subscriber, changes)

	// REFUSED: WIDENING THE BOUNDARY OF A PRINCIPAL WHOSE CREDENTIAL IS UNACCOUNTED FOR.
	if reconcileBroker {
		if err := refuseWideningUnaccountedAccess(subscriber, storedAuthorization); err != nil {
			return nil, err
		}
	}

	pruned, granted := 0, 0

	if reconcileBroker {
		// STEP 0 — RECORD THE OBLIGATION, before a single broker round trip.
		if err := store.RecordSubscriberGrantReconcilePending(
			ctx, subscriberID, time.Now(), fence.token,
		); err != nil {
			return nil, err
		}

		// STEP 1 — PRUNE. Whatever the new authorization no longer implies is removed at the
		// broker BEFORE the row records the narrowing, so a failure of the write below leaves
		// the subscriber with less access than the registry claims rather than more.
		prunePhase, endPrunePhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endPrunePhase()

			return nil, phaseErr
		}

		pruned, err = s.pruneBrokerAccess(prunePhase, subscriber)
		endPrunePhase()

		if err != nil {
			return nil, err
		}
	}

	// STEP 2 — PERSIST, under the claim. A write that matches no row here means the claim
	// was lost or the row was tombstoned while the prune above was in flight, and the
	// conflict that reports it is the whole point: the prune has already narrowed the
	// broker, so continuing to STEP 3 with a stale view would re-grant an authorization
	// somebody else has replaced. THE ROW AS STORED, not the one assembled above.
	persisted, err := store.UpdateEventSubscriber(ctx, subscriber, fence.token)
	if err != nil {
		// A LOST CLAIM IS NOT AN ORDINARY CONFLICT WHEN THE PRUNE ALREADY LANDED. The broker
		// now grants less than the registry records, and that residue has to be described
		// rather than merely returned as a 409 — see abandonUpdateAfterLostFence for why it
		// is deliberately NOT undone.
		if subscriberFenceWasLost(err) {
			return nil, s.abandonUpdateAfterLostFence(subscriber, storedAuthorization, pruned, err)
		}

		return nil, err
	}

	// From here the stored row IS the subscriber: the grant step below derives its
	// bindings from it, so using the assembled copy would risk granting against a value
	// the database did not accept.
	subscriber = persisted

	if reconcileBroker {
		// STEP 3 — GRANT. Whatever the new authorization adds reaches the broker only now
		// that the registry records it, so a failure here is again fail-closed. The error is
		// returned, since a caller told the update succeeded would believe a grant exists
		// that does not.
		grantPhase, endGrantPhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endGrantPhase()

			return nil, phaseErr
		}

		granted, err = s.grantBrokerAccess(grantPhase, subscriber)
		endGrantPhase()

		if err != nil {
			return nil, err
		}

		// STEP 4 — DISCHARGE. Only now do the broker and the row demonstrably agree, so only
		// now is the obligation satisfied.
		if clearErr := store.ClearSubscriberGrantReconcilePending(
			ctx, subscriberID, fence.token,
		); clearErr != nil {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
				"error":              sanitizeLogValue(clearErr.Error(), maxLoggedErrorLength),
			}).Warn(
				"event subscriber: the authorization change completed at both the registry and the broker, " +
					"but its pending-reconciliation marker could not be cleared; settlement will re-reconcile " +
					"a broker that already matches and clear it then",
			)
		}
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(subscriber.SubscriberID),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
		"legacy_webhook":         subscriber.WebhookURL != nil,
		"acl_reconciled":         reconcileBroker,
		"acl_bindings_removed":   pruned,
		"acl_bindings_created":   granted,
	}).Info(
		"event subscriber: registry row updated; its broker-side grant was reconciled only if the " +
			"recorded authorization could have moved",
	)

	// THE ONE TRANSITION THE LINE ABOVE CANNOT REPORT. Its ACL churn is the same churn any
	// narrowing produces and says nothing about a credential already in a subscriber's
	// hands: that credential was delivered with a response declaring direct broker access,
	// the Read it described has just been withdrawn, and the statement cannot be recalled.
	// Emitted after the write, so it reports what was persisted rather than what was
	// attempted.
	warnOnKeyScopeRecordedAfterIssuance(subscriber, priorKeyPrefix, priorCredentialIssued)

	return subscriber, nil
}

// SettleSubscriber discharges whatever broker-side work a subscriber still owes.
//
// It is the remedy SubscriberSettlementProcessor drives, and the counterpart of the two
// markers written when an operation could not finish. See event_settlement.go for why
// those markers exist at all; this function is what makes them go away.
//
// Parameters:
//   - ctx context.Context: bounds the whole remedy. The caller applies its own budget.
//   - subscriberID string: the subscriber to settle.
//
// Returns:
//   - error: a typed conflict when another operation holds the subscriber's claim —
//     which is a SKIP rather than a fault, and is why the processor simply retries
//     later — a typed not-found when the subscriber has since been deregistered, or the
//     failure that stopped the remedy.
func (s *EventSubscriberService) SettleSubscriber(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED, like every other operation that changes broker state. A subscriber somebody
	// is actively issuing for is left alone: the conflict propagates, the processor
	// records the attempt, and the obligation is taken by a later pass. Settling
	// underneath a live issuance would revoke the credential it is in the middle of
	// writing.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer fence.release(ctx)

	// RE-READ UNDER THE CLAIM. The flags the processor scanned are advisory: between the
	// scan and this point a successful re-issuance can have discharged the
	// credential-cleanup obligation, and acting on the stale flag would revoke a
	// credential that had just been issued. This read is the first moment at which they
	// cannot change underneath the remedy.
	obligation, err := store.GetSubscriberSettlementObligation(ctx, subscriberID)
	if err != nil {
		return err
	}

	if !obligation.Outstanding() {
		logrus.WithField(
			"subscriber_id_hash", subscriberLogLabel(subscriberID),
		).Debug(
			"subscriber settlement: nothing is owed for this subscriber any more, so nothing was done",
		)

		return nil
	}

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return err
	}

	if err := s.settleCredentialCleanup(ctx, store, fence, subscriber, obligation); err != nil {
		return err
	}

	return s.settleGrantReconciliation(ctx, store, fence, subscriber, obligation)
}

// settleCredentialCleanup revokes a credential Blnk no longer accounts for and clears
// the row's record of it.
//
// Parameters:
//   - ctx context.Context: bounds the remedy.
//   - store eventSubscriberStore: the registry.
//   - fence *subscriberFence: the held claim. Renewed before the broker phase.
//   - subscriber *model.EventSubscriber: the row.
//   - obligation model.SubscriberSettlementObligation: what the row owes, read under
//     the claim.
//
// Returns:
//   - error: the failure that stopped the remedy, or nil when nothing was owed or all
//     of it was settled.
func (s *EventSubscriberService) settleCredentialCleanup(
	ctx context.Context,
	store eventSubscriberStore,
	fence *subscriberFence,
	subscriber *model.EventSubscriber,
	obligation model.SubscriberSettlementObligation,
) error {
	if !obligation.CredentialCleanupPending {
		return nil
	}

	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
	}

	if admin.IsConfigured() {
		phase, endPhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endPhase()

			return phaseErr
		}

		revokeErr := admin.RevokeSubscriber(phase, subscriber)
		endPhase()

		if revokeErr != nil {
			logrus.WithFields(fields).WithField(
				"revocation_error", sanitizeLogValue(revokeErr.Error(), maxLoggedErrorLength),
			).Error(
				"subscriber settlement: revoking this principal's Kafka access failed, so its " +
					"pending credential cleanup remains outstanding",
			)

			return revokeErr
		}
	}

	// The broker no longer holds anything for this principal, so the row must stop saying
	// it does. This is the write that ALSO clears the marker — see
	// ClearSubscriberCredential — which is why nothing else in this function touches it.
	if err := store.ClearSubscriberCredential(ctx, subscriber.SubscriberID, fence.token); err != nil {
		logrus.WithFields(fields).WithField(
			"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"subscriber settlement: the principal's Kafka access was revoked but clearing the " +
				"registry's credential record failed, so the registry still over-reports it",
		)

		return err
	}

	logrus.WithFields(fields).Info(
		"subscriber settlement: the principal's Kafka credential was revoked and the registry's " +
			"credential record cleared",
	)

	return nil
}

// settleGrantReconciliation brings the broker's ACL bindings back into line with the
// row.
//
// It reuses pruneBrokerAccess and grantBrokerAccess — the same two steps, in the same
// order, that UpdateSubscriber performs — rather than a second implementation. The row
// is the source of truth, so this converges regardless of how far the original attempt
// got, and it needs no record of which step failed.
//
// Parameters:
//   - ctx context.Context: bounds the remedy.
//   - store eventSubscriberStore: the registry.
//   - fence *subscriberFence: the held claim. Renewed before each broker phase.
//   - subscriber *model.EventSubscriber: the row whose authorization the broker must
//     match.
//   - obligation model.SubscriberSettlementObligation: what the row owes, read under
//     the claim.
//
// Returns:
//   - error: the failure that stopped the remedy, or nil when nothing was owed or all
//     of it was settled.
func (s *EventSubscriberService) settleGrantReconciliation(
	ctx context.Context,
	store eventSubscriberStore,
	fence *subscriberFence,
	subscriber *model.EventSubscriber,
	obligation model.SubscriberSettlementObligation,
) error {
	if !obligation.GrantReconcilePending {
		return nil
	}

	fields := logrus.Fields{
		"subscriber_id_hash":     subscriberLogLabel(subscriber.SubscriberID),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
	}

	// A tombstoned row is discharged without being acted on. Reconciling TO it would
	// re-create the grants its deregistration is removing, and the tombstone is already a
	// durable marker the deregistration retry finds — so this obligation would otherwise
	// be one no pass could ever satisfy.
	if subscriber.IsRevocationPending() {
		if err := store.ClearSubscriberGrantReconcilePending(
			ctx, subscriber.SubscriberID, fence.token,
		); err != nil {
			return err
		}

		logrus.WithFields(fields).Warn(
			"subscriber settlement: this subscriber is being deregistered, so its broker-side " +
				"grant was NOT reconciled to the row; the pending-reconciliation marker was " +
				"discharged and the revocation tombstone remains the outstanding work",
		)

		return nil
	}

	prunePhase, endPrunePhase, err := fence.brokerPhaseContext(ctx)
	if err != nil {
		endPrunePhase()

		return err
	}

	pruned, err := s.pruneBrokerAccess(prunePhase, subscriber)
	endPrunePhase()

	if err != nil {
		return err
	}

	grantPhase, endGrantPhase, err := fence.brokerPhaseContext(ctx)
	if err != nil {
		endGrantPhase()

		return err
	}

	granted, err := s.grantBrokerAccess(grantPhase, subscriber)
	endGrantPhase()

	if err != nil {
		return err
	}

	// Discharged only now that the broker and the row demonstrably agree.
	if err := store.ClearSubscriberGrantReconcilePending(
		ctx, subscriber.SubscriberID, fence.token,
	); err != nil {
		logrus.WithFields(fields).WithField(
			"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"subscriber settlement: the broker was reconciled to the registry row but the " +
				"pending-reconciliation marker could not be cleared, so the reconciliation will " +
				"be repeated harmlessly on a later pass",
		)

		return err
	}

	logrus.WithFields(fields).WithFields(logrus.Fields{
		"acl_bindings_removed": pruned,
		"acl_bindings_created": granted,
	}).Info(
		"subscriber settlement: the broker's grants for this subscriber were reconciled to the " +
			"registry row",
	)

	return nil
}

// warnOnKeyScopeRecordedAfterIssuance reports the one key-scope transition whose
// consequence reaches BACKWARDS past the request that caused it.
//
// That is the correct outcome, and it is not a silent one. An operator who narrows a
// subscriber's declared boundary should expect its direct consumption to stop; a
// consumer whose fetches begin failing needs the reason to be findable.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as it now stands, after the write.
//   - priorKeyPrefix string: the prefix recorded before this update, "" when none was.
//   - priorCredentialIssued bool: whether the row already held a credential before this
//     update.
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
//
// Each path's residue is different and all four are wrong in the same direction:
//
//   - DEREGISTRATION deleted the row, and credential_reference is the ONLY record of which
//     principal still has to be revoked — so a live SASL credential with live ACL bindings was
//     left at the broker with nothing anywhere naming it, and no retry could find it because
//     there was no longer a row to retry from. This is the unrecoverable one.
//   - REVOCATION cleared credential_reference, erasing the evidence while the credential kept
//     authenticating.
//   - NARROWING persisted a smaller authorization while the broker kept the wider grant, so the
//     registry UNDER-reports real access — the direction that misleads an operator asking "can
//     this subscriber still see that topic?".
//   - WIDENING recorded a grant that was never created, which the settlement obligation covers.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as the registry holds it.
//   - action string: what could not be confirmed, for the log line and the detail.
//
// Returns:
//   - error: a retryable ErrKafkaUnavailable when the row may hold broker access,
//     otherwise nil.
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

// pruneBrokerAccess removes the broker-side grants a subscriber's new authorization no
// longer implies, and reports how many it removed.
//
// It is the first of UpdateSubscriber's three steps. A deployment with no broker
// configured has no broker-side grant, so it is a no-op there; anything else is
// reported as a provisioning failure, because an update that could not narrow the
// boundary must not be reported as having narrowed it.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the NEW authorization.
//
// Returns:
//   - int: how many bindings were removed.
//   - error: ErrSubscriberProvisioningFailed when the broker refused, or the admin
//     client's construction error.
func (s *EventSubscriberService) pruneBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		// Narrowing the registry while the broker keeps the wider grant makes the
		// registry under-report real access, which is the dangerous direction.
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "removing this subscriber's obsolete Kafka grants",
		); err != nil {
			return 0, err
		}

		return 0, nil
	}

	report, err := admin.PruneSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
			// SANITIZED AND BOUNDED, not logrus.WithError. A refused administrative request
			// comes back from the Kafka client with the whole request appended to it — hundreds
			// of lines carrying broker addresses, listener names and every field of the call —
			// and an unbounded value with newlines in it can also forge log structure.
			// sanitizeLogValue keeps the broker's own words, which are what an operator needs,
			// and drops the dump.
			"error_class": kafkaErrorClassField("subscriber_credential_issuance", err),
		}).Error(
			"event subscriber: the obsolete Kafka grants of this subscriber could not be removed, so " +
				"the registry was NOT updated; the subscriber keeps the access it has and the change " +
				"can be retried",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"Failed to remove the subscriber's obsolete Kafka grants, so its authorization was not changed",
			// THE BOUNDED DETAIL, never the cause. NewAPIError does two things with what it is
			// given: it re-logs it through logrus unsanitized, and it serialises it into the
			// response body's `details` member. Passing the wrapped broker error here therefore
			// undid the sanitizing done immediately above AND published whatever the client
			// chose to put in that error.
			NewSubscriberErrorDetail(
				"Removing the subscriber's obsolete Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Removed, nil
}

// grantBrokerAccess creates the broker-side grants a subscriber's authorization
// implies, and reports how many it created.
//
// It is the last of UpdateSubscriber's three steps, and it runs AFTER the row is
// persisted so a widening cannot precede the record of it. A failure here leaves the
// registry recording more access than the broker grants, which is the safe direction
// and self-heals: re-running the update, or issuing credentials, creates the missing
// bindings.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the persisted row.
//
// Returns:
//   - int: how many bindings were created.
//   - error: ErrSubscriberProvisioningFailed when the broker refused, or the admin
//     client's construction error.
func (s *EventSubscriberService) grantBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		// The widening half: the row would record a grant nothing created, and a caller told
		// the update succeeded would believe a boundary exists that does not.
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "creating this subscriber's new Kafka grants",
		); err != nil {
			return 0, err
		}

		return 0, nil
	}

	report, err := admin.GrantSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error_class": kafkaErrorClassField("subscriber_credential_issuance", err),
		}).Error(
			"event subscriber: the registry was updated but the subscriber's new Kafka grants could " +
				"not be created, so it currently has LESS access than the registry records; re-run the " +
				"update or issue credentials to complete it",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber was updated but its new Kafka grants could not be created",
			// The bounded detail, never the cause — see pruneBrokerAccess. Retryable, and safely
			// so: the registry already records the intended authorization, and both re-running
			// the update and issuing credentials create the missing bindings.
			NewSubscriberErrorDetail(
				"Creating the subscriber's new Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Created, nil
}

// keyScopeEnforcement resolves whether this deployment enforces subscriber key scopes,
// and where.
//
// It goes through fetchConfiguration, the package's configuration seam declared in
// event_sunset.go, for the same reason subscriberFacingBrokers does: a test that swaps
// the seam sees consistent behaviour across every event file.
//
// Returns:
//   - gateway []string: the enforcing bootstrap list, nil unless enforcement is active.
//   - enforced bool: true only when config.KafkaConfig.KeyScopeGateway reports an
//     active, distinct gateway.
func (s *EventSubscriberService) keyScopeEnforcement() (gateway []string, enforced bool) {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return nil, false
	}

	return cnf.Kafka.KeyScopeGateway()
}

// SubscriberAccessDeployment resolves the three configuration facts that decide what a
// subscriber's access ACTUALLY is, for the projection that has to describe it.
//
// Registration announced a verified isolation boundary and issuance denied it, in two
// consecutive requests, and nothing in either body said which was right.
//
// Returns:
//   - model.SubscriberAccessDeployment: the resolved deployment state, safe to project
//     from.
func SubscriberAccessDeployment() model.SubscriberAccessDeployment {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return model.SubscriberAccessDeployment{
			KeyScopeEnforcement: model.KeyScopeEnforcementNone,
		}
	}

	deployment := model.SubscriberAccessDeployment{
		KeyScopeEnforcement: model.KeyScopeEnforcementNone,
	}

	if _, enforced := cnf.Kafka.KeyScopeGateway(); enforced {
		deployment.KeyScopeEnforcement = model.KeyScopeEnforcementGateway
	}

	_, deployment.SubscriberBrokersAdvertised = cnf.Kafka.SubscriberFacingBrokers()

	// THE COMPOSED PREDICATE, spelled exactly as requireAcknowledgedSharedTopicAccess
	// spells it: outside secure mode nothing is asked of the operator, and in secure mode
	// the declaration is what unblocks a whole-topic credential. Recomputing it from
	// Server.Secure and the variable separately in the projection is how the two would
	// come to disagree.
	deployment.WholeTopicAccessPermitted = !cnf.Server.Secure || cnf.Kafka.SubscriberSharedTopicAccess

	return deployment
}

// subscriberFacingBrokers resolves the bootstrap list to report to a subscriber, or
// refuses.
//
// admin.Brokers() is what BLNK dials. Inside a deployment those addresses are internal
// — "kafka:9092" on a compose network, a ClusterIP or headless Service in Kubernetes —
// and they do not resolve for a subscriber outside it.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the subscriber being provisioned, named in the
//     log and in the error so an operator knows which request it was.
//
// Returns:
//   - []string: the subscriber-facing bootstrap list, never empty on success.
//   - error: ErrSubscriberBrokersNotConfigured (503) when no subscriber-facing list is
//     configured.
func (s *EventSubscriberService) subscriberFacingBrokers(
	subscriber *model.EventSubscriber,
) ([]string, error) {
	identifier := ""
	if subscriber != nil {
		identifier = subscriber.SubscriberID
	}

	// Read through fetchConfiguration — the package's configuration seam, declared in
	// event_sunset.go — for the same reason provisioner does: a test that swaps it sees
	// consistent behaviour across every event file.
	cnf, err := fetchConfiguration()
	if err == nil && cnf != nil {
		if brokers, advertised := cnf.Kafka.SubscriberFacingBrokers(); advertised {
			return brokers, nil
		}
	}

	logrus.WithField("subscriber_id_hash", subscriberLogLabel(identifier)).Error(
		"event subscriber: KAFKA_SUBSCRIBER_BROKERS is not configured, so no credential was " +
			"issued; only an operator knows the externally advertised broker addresses a " +
			"subscriber can dial, and a one-time secret handed out with an address that does not " +
			"resolve for it costs a reissue to diagnose",
	)

	return nil, apierror.NewAPIError(
		apierror.ErrSubscriberBrokersNotConfigured,
		// THE VARIABLE IS NAMED IN THE MESSAGE, not only in the detail and the log. This is
		// the one string that reaches the operator running the request, and "no broker list
		// is configured" without the key is a message that cannot be acted on. A
		// configuration key name discloses nothing: it is documented in.env.example and in
		// the manifests.
		"no subscriber-facing Kafka broker list is configured, so credentials cannot be issued. "+
			"Set KAFKA_SUBSCRIBER_BROKERS to the externally advertised broker addresses "+
			"subscribers connect to — the same value as KAFKA_BROKERS when they run inside this "+
			"deployment. The addresses Blnk dials internally are not reported, because they do "+
			"not resolve for a subscriber outside it",
		NewSubscriberErrorDetail(
			"no subscriber-facing Kafka broker list is configured", identifier,
			// Retryable: nothing was written, and the request succeeds unchanged once the
			// variable is set.
			true,
		),
	)
}

// DeregisterSubscriber ends a subscriber's access at the broker and then removes it
// from the registry.
//
// The ORDER is the whole of this function. Delete the row first and a failed revocation
// leaves the principal authenticating and reading with the only record of WHICH
// principal that was already destroyed.
//
//  1. FENCE the subscriber, so an issuance cannot mint a credential for a principal that is
//     being taken out of service — and so the deregistration cannot race one that is already
//     in flight.
//  2. TOMBSTONE the row. MarkSubscriberRevocationPending stamps revocation_pending_at and
//     returns the row, so the principal and the topic list revocation needs are in hand while
//     the row itself SURVIVES. A tombstoned row is not an active subscriber: issuance refuses
//     for it.
//  3. REVOKE at the broker. The ACL bindings go first and the SCRAM credential second, so a
//     partial failure always leaves the principal with FEWER rights rather than more.
//  4. DELETE the row, and only now. TakeEventSubscriber removes it and returns it, so the
//     caller can report exactly what was revoked.
//
// Parameters:
//   - ctx context.Context: cancels the revocation and the removal.
//   - subscriberID string: the business key of the subscriber to remove.
//
// Returns:
//   - *model.EventSubscriber: the row that was removed on success, or the TOMBSTONED
//     row when revocation failed — which is the description of the state left behind
//     and the row a retry will find.
//   - error: ErrSubscriberNotFound when no such subscriber exists; a typed conflict
//     when another operation holds the subscriber's claim;
//     ErrSubscriberProvisioningFailed when the broker-side revocation did not complete
//     and the row was therefore NOT deleted; the repository's error otherwise.
func (s *EventSubscriberService) DeregisterSubscriber(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// STEP 1 — FENCE. Nothing may issue a credential for a principal that is being taken out of
	// service, and no second deregistration may run alongside this one.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer fence.release(ctx)

	// STEP 2 — TOMBSTONE. The row survives, carrying the principal and topics revocation
	// needs and saying plainly that this subscriber is on its way out.
	pending, err := store.MarkSubscriberRevocationPending(ctx, subscriberID, s.clock(), fence.token)
	if err != nil {
		return nil, err
	}

	admin, err := s.provisioner()
	if err != nil {
		// The row is TOMBSTONED, not deleted, so nothing is lost: it names the principal and
		// a retry finds it. The error is returned so the caller does not read the removal as
		// complete.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        kafkaErrorClassField("deregister_new_kafka_admin", err),
		}).Error(
			"event subscriber: no Kafka administrative client could be built, so this subscriber's " +
				"access was NOT revoked and its registry row is kept, marked pending revocation; " +
				"retry the deregistration once the broker is reachable",
		)

		return pending, err
	}

	if !admin.IsConfigured() {
		// THE MOST CONSEQUENTIAL BRANCH IN THIS FILE. Deleting the row destroys
		// the only record of which principal still has to be revoked, so the residue is
		// unfindable rather than merely unrevoked. The row stays, tombstoned, which is what
		// makes the retry possible at all.
		if err := refuseUnconfirmableBrokerWork(
			pending, "revoking this subscriber's Kafka access",
		); err != nil {
			return pending, err
		}

		// REACHING HERE MEANS THE ROW CARRIES NO CREDENTIAL EVIDENCE AT ALL: no reference, no
		// orphan marker and no unsettled cleanup obligation. Nothing can authenticate as the
		// principal, so any ACL binding naming it is inert and the row can be removed
		// directly. This is what keeps the registry usable without Kafka.
		//
		// The row's own revocation tombstone does not count against it, and deliberately so —
		// this path stamped that tombstone itself two steps ago, so a guard that read it as
		// evidence would make deregistration impossible in a deployment that never configured
		// Kafka. See model.EventSubscriber.MayHaveBrokerCredential.
		removed, takeErr := store.TakeEventSubscriber(ctx, subscriberID, fence.token)
		if takeErr != nil {
			return pending, takeErr
		}

		// The row carried the claim, so it is gone too.
		fence.consumed()

		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(removed.SubscriberID),
			"principal_hash":     subscriberLogLabel(removed.KafkaPrincipal),
		}).Info(
			"event subscriber: deregistered; no broker is configured, so there was no " +
				"broker-side access to revoke",
		)

		return removed, nil
	}

	// STEP 3 — REVOKE, using the row that still exists, under a renewed claim and a
	// bounded phase deadline. Revocation is a sequence of administrative round trips — the
	// ACL bindings and then the SCRAM credential — and this method arrived with no
	// deadline of its own, so without the bound the phase could outlive the claim and STEP
	// 4 would then delete a row another operation had taken over.
	revokePhase, endRevokePhase, phaseErr := fence.brokerPhaseContext(ctx)
	if phaseErr != nil {
		endRevokePhase()

		// The row stays tombstoned, so the obligation is durable and a retry under a fresh
		// claim finishes it. Returned rather than pressed on with: the claim is not this
		// caller's, so revoking now would race the owner that holds it.
		return pending, phaseErr
	}

	revokeErr := admin.RevokeSubscriber(revokePhase, pending)
	endRevokePhase()

	if err := revokeErr; err != nil {
		// RECORD THAT THE ATTEMPT WAS REFUSED, not merely that it began.
		failureFields := logrus.Fields{
			"subscriber_id_hash":     subscriberLogLabel(pending.SubscriberID),
			"principal_hash":         subscriberLogLabel(pending.KafkaPrincipal),
			"authorized_topic_count": len(pending.AuthorizedTopics),
			"revocation_pending_at":  subscriberPendingSince(pending),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error_class": kafkaErrorClassField("subscriber_revocation", err),
		}

		markCtx, cancelMark := subscriberDurabilityContext(ctx)
		failureFields["revocation_failure_recorded"] = s.markRevocationFailed(
			markCtx, pending, failureFields, s.clock(),
		)
		cancelMark()

		logrus.WithFields(failureFields).Error(
			"event subscriber: revoking this subscriber's Kafka access failed, so its registry row " +
				"was NOT deleted and is kept marked pending revocation; the principal named here may " +
				"still authenticate until the deregistration is retried",
		)

		return pending, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber's Kafka access could not be revoked, so it was not removed",
			// The bounded detail, matching RevokeSubscriberCredential's treatment of the same
			// failure: the principal and the broker's own words stay in the log line above, and
			// the caller receives the diagnosis plus the fact that a retry is safe. Revocation
			// is idempotent at the broker, so repeating it cannot make things worse — and the
			// tombstone on the row is what makes it findable.
			NewSubscriberErrorDetail(
				"Revoking the subscriber's Kafka access failed", pending.SubscriberID, true,
			),
		)
	}

	// STEP 3b — WITHDRAW THE KEY-SCOPE BINDING at the declared component, now that the
	// broker credential is gone.
	if err := s.revokeKeyScopeBinding(ctx, pending); err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        kafkaErrorClassField("key_scope_gateway_revoke", err),
		}).Warn(
			"event subscriber: this subscriber's Kafka credential was revoked, but withdrawing its " +
				"key-scope binding at the declared enforcement gateway failed; the principal can no " +
				"longer authenticate, so no access remains — remove the stale binding at the gateway " +
				"when it is reachable again",
		)
	}

	// STEP 4 — DELETE, now that the broker-side cleanup is confirmed, and under the claim.
	// A miss means the claim was lost while the revocation was in flight; the row then
	// stays tombstoned, which is the recoverable direction, and the conflict tells the
	// caller so.
	removed, err := store.TakeEventSubscriber(ctx, subscriberID, fence.token)
	if err != nil {
		// The access is already gone, so this residue is the harmless direction: a registry
		// row describing a subscriber that can no longer authenticate. Retrying the
		// deregistration removes it, and the tombstone is what makes the row identifiable as
		// needing that. F14: THE ROW MUST STOP DESCRIBING A CREDENTIAL AND AN OUTSTANDING
		// REVOCATION.
		fields := logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(pending.SubscriberID),
			"principal_hash":     subscriberLogLabel(pending.KafkaPrincipal),
			"error_class":        database.ErrorClass(err),
			"sqlstate":           database.SQLState(err),
		}

		if cleared := s.clearCredentialRecord(ctx, pending, fence.token, fields); cleared {
			// The RETURNED row is brought into line with the stored one. A caller handed a row
			// that still reports IsRevocationPending would conclude a principal may still
			// authenticate, which the confirmed revocation has just made false — and the caller
			// is the one place that reads this value programmatically.
			pending.CredentialReference = nil
			pending.CredentialIssuedAt = nil
			pending.RevocationPendingAt = nil
			pending.RevocationFailedAt = nil

			fields["credential_record_cleared"] = true
		} else {
			fields["credential_record_cleared"] = false
		}

		logrus.WithFields(fields).Error(
			"event subscriber: Kafka access was revoked but the registry row could not be deleted; " +
				"the subscriber can no longer authenticate and the inert row remains until the " +
				"deregistration is retried",
		)

		return pending, err
	}

	// The row carried the claim, so it is gone too.
	fence.consumed()

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(removed.SubscriberID),
		"principal_hash":     subscriberLogLabel(removed.KafkaPrincipal),
	}).Info("event subscriber: Kafka access revoked and the subscriber deregistered")

	return removed, nil
}

// subscriberPendingSince renders the revocation tombstone for a log field.
//
// It is what turns "this revocation failed" into "this revocation has been outstanding
// since 14:02", which is the difference between a line an operator can act on and one
// they cannot. A row with no tombstone reports the empty string rather than a zero
// instant, because a formatted year 1 would read as data.
func subscriberPendingSince(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.RevocationPendingAt == nil {
		return ""
	}

	return subscriber.RevocationPendingAt.UTC().Format(time.RFC3339)
}

// ---------------------------------------------------------------------------------------
// Credential issuance

// IssueSubscriberCredential mints a subscriber's SASL/SCRAM credential, binds its ACLs,
// and returns the secret to the caller.
//
// FORWARD PROVISIONING AND THE RESPONSE ARE BOUNDED BY
// SubscriberCredentialIssuanceBudget. Owed cleanup may continue afterwards under its
// own bounded context and is reported by the settlement markers rather than waited for.
//
// Parameters:
//   - ctx context.Context: the caller's context, wrapped in the issuance budget.
//   - subscriberID string: the business key of the subscriber to provision.
//
// Returns:
//   - SubscriberCredential: the connection details and the one-time secret.
//   - error: ErrSubscriberNotFound (404); ErrKafkaUnavailable (503) when no broker is
//     configured, the client cannot be built, or the budget expired;
//     ErrSubscriberProvisioningFailed (503) when the broker refused the credential or
//     the bindings; a typed conflict (409) when a concurrent issuance superseded this
//     one; a typed validation error (400) for an unusable subscriber id.
func (s *EventSubscriberService) IssueSubscriberCredential(
	ctx context.Context,
	subscriberID string,
) (SubscriberCredential, error) {
	store, err := s.requireStore()
	if err != nil {
		return SubscriberCredential{}, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return SubscriberCredential{}, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// THE ONE ABSOLUTE DEADLINE. Everything this request does is inside it: the lookup, up
	// to four broker round trips, the issuance record, AND any compensation that follows a
	// failure.
	absolute := time.Now().Add(s.budget())
	ctx = withSubscriberIssuanceDeadline(ctx, absolute)

	forward, cancel := context.WithDeadline(ctx, absolute.Add(-s.compensationReserve()))
	defer cancel()

	// Shadowed deliberately, so no step below can accidentally use the unbounded parent:
	// the forward path must be the one with the timeout, and the absolute instant reaches
	// the cleanup helpers through the value that travels on it either way.
	ctx = forward

	admin, err := s.provisioner()
	if err != nil {
		return SubscriberCredential{}, err
	}

	if !admin.IsConfigured() {
		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Kafka is not configured, so subscriber credentials cannot be issued",
			ErrKafkaAdminNotConfigured,
		)
	}

	// THE FENCE, taken BEFORE the broker is touched and before a secret is generated.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		// A budget spent on the claim is a TIMEOUT, not a server fault, and this is the
		// commonest place for it to be spent: the claim is the first write of the request. A
		// conflict — another operation holds the claim — passes through unchanged, because it
		// is true whether or not the deadline also expired.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "claiming the subscriber for provisioning", err,
		)
	}

	// THE COMPENSATION THIS ATTEMPT TURNS OUT TO OWE, AND THE CLAIM RELEASE, LEAVE THE
	// RESPONSE PATH TOGETHER — in ONE scheduled task, compensation FIRST.
	var owedCompensation func(base context.Context)

	defer func() {
		compensate := owedCompensation

		s.schedule(func() {
			// THE RELEASE'S WINDOW IS RESOLVED HERE, not when the issuance started.
			releaseCtx, endRelease := subscriberCleanupContext(ctx)
			defer endRelease()

			if compensate != nil {
				compensate(releaseCtx)
			}

			fence.release(releaseCtx)
		})
	}()

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "reading the subscriber's registry row", err,
		)
	}

	// Refuse a subscriber that is on its way out, so an issuance cannot re-arm a principal
	// whose deregistration is already in flight.
	if err := requireActiveSubscriber(subscriber, subscriberRefusedIssuanceMessage); err != nil {
		return SubscriberCredential{}, err
	}

	// THE ENFORCEMENT FACT, READ ONCE. Both halves of the key-scope decision consult it —
	// whether this subscriber may be issued a credential at all, and which endpoint that
	// credential names — so the two cannot disagree.
	keyScopeGateway, keyScopeEnforced := s.keyScopeEnforcement()

	// FAIL CLOSED ON AN UNENFORCEABLE KEY SCOPE. A row recording a partition key prefix
	// describes a boundary Kafka's authorizer has no dimension for, so a credential
	// carrying topic Read would read every record on every authorised topic — other
	// ledgers' and other subscribers' included. Disclosure was tried in place of a
	// boundary and is not one: the party asked to apply the filter is the party holding
	// the credential.
	if err := requireProvisionableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND FAIL CLOSED IN THE OTHER DIRECTION. The refusal above covers a key scope nothing
	// enforces; this one covers the credential that ESCAPES a declared key-scoped model —
	// a subscriber with no prefix, issued literal topic Read, reading every ledger on a
	// shared topic while every other subscriber in the deployment is confined. One
	// prefix-less registration was all it took, and the DTO makes the field optional, so
	// nothing marked the occasion.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND MAKE THE WHOLE-TOPIC MODEL A DECISION RATHER THAN A DEFAULT. Where no key-scoped
	// model is declared, a granted topic is read in full — which is the mandated access
	// model and is correct for a single-tenant deployment. In secure mode Blnk asks the
	// operator to have said so, once, instead of reaching the widest credential it can
	// issue by configuring nothing.
	if err := requireAcknowledgedSharedTopicAccess(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// And refuse a subscriber authorised for nothing, so a live principal that can read
	// nothing is never handed out looking like one that can. Checked in the same place and
	// for the same reason as the three above: before a secret exists and before the broker
	// is touched, so the refusal leaves no residue anywhere.
	if err := requireGrantedTopics(subscriber); err != nil {
		return SubscriberCredential{}, err
	}

	// THE ENDPOINT THE SUBSCRIBER WILL DIAL, resolved before anything is minted.
	subscriberBrokers, err := s.subscriberFacingBrokers(subscriber)
	if err != nil {
		return SubscriberCredential{}, err
	}

	// THE DECLARED ENFORCING ENDPOINT WINS over the advertised broker list for a
	// key-scoped subscriber, and it has to: where one is declared, that subscriber's
	// connection is terminated by it rather than by a broker. Reporting the brokers
	// instead would hand out a credential declaring key-scoped isolation together with an
	// endpoint that bypasses the component enforcing it.
	if subscriber.DeclaresKeyScope() && len(keyScopeGateway) > 0 {
		subscriberBrokers = keyScopeGateway
	}

	// THE DECLARED COMPONENT IS ASKED TO CONFIRM THE BOUNDARY, before a secret exists and
	// before the broker is touched.
	if subscriber.DeclaresKeyScope() && keyScopeEnforced {
		if err := s.attestKeyScope(ctx, subscriber); err != nil {
			return SubscriberCredential{}, err
		}
	}

	// The reference OBSERVED before provisioning. It is what makes the issuance record
	// conditional: if another issuance for this subscriber commits in the meantime, this
	// value no longer matches and the write reports a conflict instead of silently
	// overwriting a record whose secret is the one that works.
	observedReference := subscriber.CredentialReference

	password, err := s.newPassword()
	if err != nil {
		// The cause is not returned, and here that is more than a topology concern: this is
		// the credential-generation path, so its error is the one most likely to render
		// something derived from the secret itself. It is logged bounded and the caller
		// receives the diagnosis only.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"error_class":        kafkaErrorClassField("subscriber_provisioning", err),
		}).Error("event subscriber: generating a SASL credential failed; nothing was written")

		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to generate a SASL credential for the subscriber",
			// Retryable: generation is local and stateless, so nothing was written and
			// repeating the request is safe.
			NewSubscriberErrorDetail(
				"Generating the SASL credential failed", subscriber.SubscriberID, true,
			),
		)
	}

	// Derived from the principal and the secret, before the secret leaves this function's
	// control, so that what is persisted is a digest and never the value itself.
	reference, err := model.DeriveCredentialReference(subscriber.KafkaPrincipal, password)
	if err != nil {
		// The password is an argument to the call that failed, so its error is the single
		// most dangerous cause in this file to propagate. It is logged bounded — and the
		// bounded rendering is of the ERROR, never of the inputs — and the caller receives no
		// cause at all.
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
			"error_class":        kafkaErrorClassField("subscriber_provisioning", err),
		}).Error(
			"event subscriber: deriving the credential reference failed, so nothing was written and " +
				"the generated secret is dead",
		)

		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to derive the subscriber credential reference",
			NewSubscriberErrorDetail(
				"Deriving the credential reference failed", subscriber.SubscriberID, true,
			),
		)
	}

	// The claim is CONFIRMED on the way into the broker phase, so the credential about to
	// be written cannot be interleaved with another operation's. The issuance budget
	// already caps this phase — it caps the whole request — so the phase context adds only
	// the renewal, and the caller's shorter deadline is what continues to govern the round
	// trips.
	provisionPhase, endProvisionPhase, phaseErr := fence.brokerPhaseContext(ctx)
	if phaseErr != nil {
		endProvisionPhase()

		// Nothing has been written and the generated secret is dead. Classified so that a
		// budget spent waiting on the renewal reads as the timeout it was.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "confirming the provisioning claim before provisioning", phaseErr,
		)
	}

	// DeferCompensation is set only when a scheduler is installed. Without one the
	// compensation would be "deferred" to an inline call moments later, which is the same
	// two round trips in a less obvious place — so the request asks for the inline
	// behaviour it is actually going to get, and the result's flags describe what really
	// happened.
	request := NewSubscriberProvisioningRequest(subscriber, password)
	request.DeferCompensation = s.defersWork()

	result, err := admin.ProvisionSubscriberPrincipal(provisionPhase, request)
	endProvisionPhase()

	if err != nil {
		if result.CompensationOwed {
			// The credential is at the broker with no boundary, and undoing it is two round
			// trips this response must not wait for. Scheduled with the fence still held, so a
			// retry cannot have its own fresh credential revoked by this cleanup.
			owed := result
			owedCompensation = func(base context.Context) {
				cleanup, cancel := subscriberCleanupContext(base)
				defer cancel()

				// The outcome is not returned anywhere: this runs after the response. It is logged
				// by the compensation itself — at ERROR, naming the principal and the manual
				// remedy, when the credential could not be revoked.
				_ = admin.CompensateProvisioning(cleanup, owed)
			}
		}

		// ctx and the claim are threaded in because a broker that was left holding something
		// is a DURABLE obligation, not just a log line, and recording it is a write that
		// needs both. The claim is still this caller's here — the deferred release has not
		// run — so the write is fenced like every other mutation on the row.
		return SubscriberCredential{}, s.provisioningFailure(ctx, subscriber, fence.token, result, err)
	}

	// THE RENEWAL, between the last broker round trip and the write it protects.
	if renewErr := fence.renew(ctx); renewErr != nil {
		residue, failure := s.recordFailure(ctx, admin, subscriber, fence.token, renewErr)

		return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
			ctx, subscriber, "confirming the provisioning claim before recording the issuance",
			failure, residue,
		)
	}

	issuedAt := s.clock()

	// Under the claim as well as the reference CAS. The reference alone cannot detect a
	// caller whose lease expired: while the rightful new owner is still provisioning it
	// has recorded nothing, so the stored reference is still the one this caller observed,
	// and its write would land and then refuse the winner.
	if err := store.RecordSubscriberCredentialIfUnchanged(
		ctx, subscriberID, observedReference, reference, issuedAt, fence.token,
	); err != nil {
		// recordFailure owns the compensation and its own logging, and REPORTS WHAT THE
		// BROKER IS LEFT HOLDING once it has run. The classification then re-reports a spent
		// budget here as the timeout it was, and runs on the OUTSIDE so the compensation
		// happens first either way, leaving recordFailure's typed conflict — a superseded
		// issuance — untouched.
		if s.defersWork() {
			recordErr := err
			owedCompensation = func(base context.Context) {
				_, _ = s.recordFailure(base, admin, subscriber, fence.token, recordErr)
			}

			return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
				ctx, subscriber, "recording the issuance in the registry", err,
				SubscriberProvisioningResult{CredentialWritten: true},
			)
		}

		residue, failure := s.recordFailure(ctx, admin, subscriber, fence.token, err)

		return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
			ctx, subscriber, "recording the issuance in the registry", failure, residue,
		)
	}

	credential := SubscriberCredential{
		SubscriberID:     subscriber.SubscriberID,
		Brokers:          subscriberBrokers,
		BrokerEndpoint:   strings.Join(subscriberBrokers, ","),
		AuthorizedTopics: result.Topics,
		ConsumerGroupID:  subscriber.ConsumerGroupID,
		Username:         subscriber.KafkaPrincipal,
		Mechanism:        SubscriberSASLMechanism,
		IssuedAt:         issuedAt,
		// Read from the row this credential was minted against, so the response cannot pair a
		// credential with a prefix that was not in force at issuance.
		PartitionKeyPrefix: subscriberKeyPrefix(subscriber),
		// Derived from the SAME row the prefix above was read from, so the two cannot
		// disagree. Reporting a prefix without saying where it is enforced is what let the
		// field be read as a broker boundary, and reading it as one is what the withheld
		// credential was reaching for. Always populated — "none" when no scope is recorded —
		// so a client branches on this rather than on whether the prefix happens to be empty.
		KeyScopeEnforcement: issuedKeyScopeEnforcement(subscriber, keyScopeEnforced),
		Fingerprint:         model.CredentialFingerprint(reference),
		Replaced:            result.CredentialReplaced,
		password:            NewSubscriberSecret(password),
	}

	// LogFields, never the credential itself: the secret appears here only as its length.
	logrus.WithFields(credential.LogFields()).Info(
		"event subscriber: Kafka credential issued; the secret is returned once and is not " +
			"recoverable afterwards",
	)

	// AND THE KEY-SCOPE LINE, for a key-scoped row only — logKeyScopeDisclosure returns
	// without emitting when no prefix is recorded, so this is one call rather than a
	// condition here.
	logKeyScopeDisclosure(subscriber, describeSubscriberKeyScope(subscriber))

	return credential, nil
}

// newPassword draws a secret through the service's generator seam.
//
// The indirection exists so a test can drive the generator-failure branch, which has no other
// trigger — the system CSPRNG does not fail on demand. A nil generator falls back to the real
// one rather than panicking, so a hand-built zero-valued service still behaves.
func (s *EventSubscriberService) newPassword() (string, error) {
	if s == nil || s.generatePassword == nil {
		return generateSubscriberPassword()
	}

	return s.generatePassword()
}

// provisioningFailure turns a broker-side provisioning failure into the right typed
// error and records what state the broker was left in.
//
// Five outcomes, each needing a different answer:
//
//   - NOT CONFIGURED. The broker list emptied between the check and the call. 503
//     ErrKafkaUnavailable, and nothing was written.
//   - ACCESS BEYOND AUTHORIZATION. The principal carries foreign ACL bindings that grant more
//     than the registry describes, so provisioning refused rather than issuing a credential
//     whose real permissions exceed its recorded ones. 409
//     ErrSubscriberAccessExceedsAuthorization, and not retryable until a human clears the
//     bindings at the broker.
//   - BUDGET EXPIRED or CALLER CANCELLED. 503 ErrKafkaUnavailable. What the broker did or did
//     not write is genuinely unknown, which the log line says; a retry re-provisions the same
//     boundary idempotently.
//   - COMPENSATED. The credential was written, the ACL grant failed, and the revocation was
//     CONFIRMED — the broker is clean and the generated secret is dead. Logged as a warning.
//   - COMPENSATION FAILED. The credential was written and the revocation failed too, so a
//     principal exists that can authenticate with no boundary. Logged at ERROR with the
//     principal named, which is the only record of what must be revoked by hand.
//
// Parameters:
//   - ctx context.Context: the request's context.
//   - subscriber *model.EventSubscriber: the row being provisioned, for the log fields.
//   - fenceToken string: the caller's provisioning claim, which the settlement writes
//     are conditional on.
//   - result SubscriberProvisioningResult: the only source of truth for what reached
//     the broker.
//   - cause error: the provisioning error.
//
// Returns:
//   - error: a typed ErrKafkaUnavailable, ErrSubscriberAccessExceedsAuthorization or
//     ErrSubscriberProvisioningFailed.
func (s *EventSubscriberService) provisioningFailure(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	result SubscriberProvisioningResult,
	cause error,
) error {
	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"credential_written": result.CredentialWritten,
		"compensated":        result.Compensated,
		"error_class":        kafkaErrorClassField("subscriber_provisioning", cause),
	}

	// Settled BEFORE the error is chosen, so that every branch below settles — including
	// the timeout and cancellation branches, where the result flags are the only evidence
	// of what the broker was left holding and where the temptation to treat "unknown" as
	// "nothing" is strongest.
	s.settleProvisioningRemnant(ctx, subscriber, fenceToken, result, fields)

	switch {
	case errors.Is(cause, ErrKafkaAdminNotConfigured):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was attempted with no broker configured; nothing " +
				"was written",
		)

		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Kafka is not configured, so subscriber credentials cannot be issued",
			// Bounded, and the cause stays in the log line above. See SubscriberErrorDetail:
			// NewAPIError both re-logs its details unsanitized and serialises them into the
			// response, so passing the cause here undid the sanitizing this function had just
			// done — on both sides at once.
			NewSubscriberErrorDetail(
				"Kafka is not configured", subscriber.SubscriberID, false,
			),
		)

	case errors.Is(cause, ErrForeignACLGrantsAccess):
		// Named as its own branch because the three state-based branches below describe what
		// the broker was LEFT in, and none of them describes what went wrong here. Falling
		// through to them logged "the ACL grant failed", which is the opposite of the truth:
		// the grant succeeded, and the refusal is that a binding Blnk does not own grants
		// MORE than the grant does. An operator reading the wrong cause looks for a broker
		// fault instead of the hand-made binding that is the actual blocker.
		if result.CredentialWritten {
			logrus.WithFields(fields).Error(
				"event subscriber: credential issuance was refused because the principal holds " +
					"foreign ACL bindings granting access beyond its authorization, AND revoking the " +
					"credential failed, so the principal named here can authenticate and read outside " +
					"its authorized topics; revoke it by hand immediately",
			)
		} else {
			logrus.WithFields(fields).Warn(
				"event subscriber: credential issuance was refused because the principal holds " +
					"foreign ACL bindings granting access beyond its authorization; the credential " +
					"was revoked, the generated secret is dead and the issuance was not recorded",
			)
		}

		return apierror.NewAPIError(
			// 409, not the 503 of ErrSubscriberProvisioningFailed. The broker answered and
			// provisioning completed; the refusal is a judgement about the boundary that
			// resulted, and no retry can change it. The code discriminates from the key-scope
			// refusal because the state to fix is at the broker rather than on the row.
			apierror.ErrSubscriberAccessExceedsAuthorization,
			"The subscriber's Kafka principal holds access beyond its authorization, so no credential was issued",
			// NOT retryable: the offending bindings are an operator's, so an immediate retry
			// re-reads the same broker state and refuses again. It becomes retryable only once a
			// human removes the bindings or records the access on the subscriber, which is what
			// the log line above asks for.
			s.provisioningDetail(
				"The Kafka principal holds ACL bindings granting access beyond its recorded authorization",
				subscriber, result, false,
			),
		)

	case errors.Is(cause, ErrSubscriberKeyScopeUnenforced):
		// Named as its own branch for the same reason the foreign-ACL refusal above is: the
		// state-based branches below describe what the broker was LEFT in, and none of them
		// describes what went wrong here. Provisioning reached the broker and completed; the
		// refusal is that the grant it produced would NOT have kept the boundary the row
		// declares, so a password whose isolation nobody verified was never returned.
		if result.CredentialWritten {
			logrus.WithFields(fields).Error(
				"event subscriber: credential issuance was refused because this subscriber's " +
					"partition-key boundary could not be established at the broker, AND revoking the " +
					"credential failed, so the principal named here can authenticate; revoke it by " +
					"hand immediately",
			)
		} else {
			logrus.WithFields(fields).Warn(
				"event subscriber: credential issuance was refused because this subscriber records a " +
					"partition-key prefix and the grant that resulted would have admitted it to every " +
					"record on its authorised topics; the credential was revoked, the generated secret " +
					"is dead and the issuance was not recorded",
			)
		}

		return apierror.NewAPIError(
			// 409, and the same code the foreign-ACL refusal uses, because it is the same
			// judgement: the effective access the broker would grant exceeds what the
			// subscriber's row authorises. No retry changes it.
			apierror.ErrSubscriberAccessExceedsAuthorization,
			"This subscriber records a partition-key prefix, so its credential must be granted no "+
				"record-level Read and its records must be delivered by the key-authorising component "+
				"declared in KAFKA_KEY_SCOPE_ENFORCEMENT; that boundary could not be established, so "+
				"no credential was issued",
			// NOT retryable: either the broker's ACL enforcement is unconfirmed or a binding
			// outside Blnk's control restores the withheld access, and both need an operator.
			s.provisioningDetail(
				"The subscriber's partition-key boundary could not be enforced at the broker",
				subscriber, result, false,
			),
		)

	case errors.Is(cause, context.DeadlineExceeded):
		logrus.WithFields(fields).Error(
			"event subscriber: credential issuance exceeded its budget, so whether the broker wrote " +
				"the credential is unknown; retrying re-provisions the same boundary idempotently",
		)

		return apierror.NewAPIError(
			// SUBSCRIBER_PROVISIONING_FAILED, not EVENT_KAFKA_UNAVAILABLE, and the same code the
			// registry half of this issuance reports for the identical condition — one wall
			// clock running out — so a client does not have to know which internal dependency
			// was slow in order to recognise the outcome. Its 503 is the approved retryable
			// answer for a spent budget; the error taxonomy carries no separate timeout code,
			// deliberately, so what the broker was left holding stays in the DETAIL. That is the
			// only place it can be anyway: no status code can say whether a credential the
			// caller does not hold may already exist.
			apierror.ErrSubscriberProvisioningFailed,
			fmt.Sprintf(
				"Provisioning Kafka credentials did not complete within %s",
				s.budget(),
			),
			// RETRYABLE, and saying so is the point: a retry re-provisions the same boundary
			// idempotently, which is exactly what the log line above tells an operator. The
			// state flags are carried because whether the broker wrote the credential is
			// genuinely unknown here, and a caller deciding whether to retry needs to know that
			// a secret may already exist that they do not hold.
			s.provisioningDetail(
				"Provisioning the Kafka credential did not complete within the budget",
				subscriber, result, true,
			),
		)

	case errors.Is(cause, context.Canceled):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was cancelled before it completed; whether the " +
				"broker wrote the credential is unknown",
		)

		return apierror.NewAPIError(
			// The same code as the deadline branch above, for the same reason: one wall clock
			// ran out, and the caller should not have to know which dependency noticed first.
			apierror.ErrSubscriberProvisioningFailed,
			"Provisioning Kafka credentials was cancelled before it completed",
			s.provisioningDetail(
				"Provisioning the Kafka credential was cancelled before it completed",
				subscriber, result, true,
			),
		)

	case result.CredentialWritten:
		// Compensation itself failed: the credential exists and has no boundary.
		logrus.WithFields(fields).Error(
			"event subscriber: the SCRAM credential was written, its ACL grant failed AND revoking " +
				"the credential failed, so the principal named here can authenticate with no " +
				"authorization boundary; revoke it by hand immediately",
		)

	case result.Compensated:
		logrus.WithFields(fields).Warn(
			"event subscriber: the ACL grant failed after the SCRAM credential was written, so the " +
				"credential was revoked and the broker is clean; the generated secret is dead and " +
				"the issuance was not recorded",
		)

	default:
		logrus.WithFields(fields).Error(
			"event subscriber: provisioning the Kafka principal failed before any credential was " +
				"written; nothing was changed at the broker",
		)
	}

	// The three broker-side outcomes converge here, and the detail is what distinguishes
	// them for the caller: the state flags say whether a credential reached the broker and
	// whether it was compensated away, which is the difference between "retry and you are
	// fine" and "a principal exists that a human has to revoke". The specific cause, and
	// the principal's name, stay in the log lines above.
	message := "Failed to provision Kafka credentials for the subscriber"
	reason := "Provisioning the Kafka principal failed at the broker"

	if errors.Is(cause, ErrSubscriberPrincipalReserved) {
		logrus.WithFields(fields).Error(
			"event subscriber: credentials were REFUSED because this subscriber's derived principal " +
				"is one of this deployment's own Kafka identities; provisioning it would have rotated " +
				"that credential and returned it. Move KAFKA_SASL_USER and KAFKA_SASL_ADMIN_USER " +
				"outside the reserved 'blnk-sub-' namespace and restart",
		)

		// Deliberately says nothing about WHICH identity it collides with. That is a
		// deployment fault, the remedy is in the log line above and in the start-up
		// validation, and confirming to a caller that a particular principal name is
		// privileged would answer a question they should not be able to ask.
		message = "Kafka credentials cannot be issued for this subscriber because its derived " +
			"principal is reserved by this deployment; contact an administrator"
		reason = "The subscriber's derived Kafka principal is reserved"

		return apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			message,
			s.provisioningDetail(reason, subscriber, result, false),
		)
	}

	if errors.Is(cause, ErrSubscriberForeignACLGrant) {
		logrus.WithFields(fields).Error(
			"event subscriber: credentials were REFUSED because this principal carries ALLOW ACL " +
				"bindings Blnk did not provision, so its effective access is broader than the " +
				"registry records and no isolation boundary can be stated; remove them with " +
				"kafka-acls — the bindings are named in the error above — and retry",
		)

		message = "The subscriber's Kafka principal carries ACL grants Blnk did not provision, " +
			"so its effective access is broader than its authorization records and credentials " +
			"cannot be issued; remove the foreign ACL bindings from the principal and retry"
		reason = "The Kafka principal carries foreign ALLOW ACL bindings, so no access boundary can be stated"
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		message,
		s.provisioningDetail(reason, subscriber, result, false),
	)
}

// settleProvisioningRemnant reconciles the registry with whatever a failed provisioning
// left at the broker, and records a DURABLE obligation whenever it cannot.
//
// COMPENSATION FAILED — result.CredentialWritten is still true after provisioning tried
// to revoke. A SCRAM credential exists for a principal with no authorization boundary.
//
// COMPENSATION SUCCEEDED — the credential was revoked and confirmed gone, but the ROW
// may still name one. That row is not merely stale: because provisioning UPSERTS the
// SCRAM credential, a re-issue that later failed and compensated destroyed the
// credential the row named as well as the one it had just written.
//
// NOTHING REACHED THE BROKER — nothing is owed, and no marker is written. Raising an
// obligation for a failure that changed no state would put every rejected request into
// the settlement backlog and drown the two states above.
//
// The commonest reason for being here is a spent budget, and a cleanup inheriting that
// deadline would fail on arrival — leaving precisely the state it exists to record.
// subscriberCleanupContext detaches from the caller's cancellation and grants its own
// bounded budget, the same way recordFailure's cleanups already do.
//
// The caller's answer is the provisioning failure. A settlement problem is an
// operational fact layered on top of it, not a different answer to the request, and
// reporting it instead would replace an accurate diagnosis with a misleading one.
//
// Parameters:
//   - ctx context.Context: the request's context, used only as the parent to detach
//     from.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - fenceToken string: the caller's provisioning claim; every write here is
//     conditional on it.
//   - result SubscriberProvisioningResult: the only source of truth for what reached
//     the broker.
//   - fields logrus.Fields: the caller's log fields, so a settlement line carries the
//     same subscriber and principal as the failure line beside it.
func (s *EventSubscriberService) settleProvisioningRemnant(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	result SubscriberProvisioningResult,
	fields logrus.Fields,
) {
	if !result.CredentialWritten && !result.Compensated {
		return
	}

	cleanup, cancel := subscriberCleanupContext(ctx)
	defer cancel()

	// A credential that is still written is a live principal with no boundary. There is
	// nothing to clear on the row — the issuance was never recorded — so the only remedy
	// is broker-side, and that is what the obligation asks settlement to perform.
	if result.CredentialWritten {
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal with no authorization boundary")

		return
	}

	// Confirmed compensated. The broker is clean, so the row is what is wrong.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		return
	}

	// The clear failed, so the registry over-reports. Settlement's remedy is the same one
	// it would apply to a live credential — revoke, which is a harmless no-op against a
	// principal whose credential is already gone, then clear the row — so the same marker
	// covers both.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")
}

// recordCleanupObligation writes the credential-cleanup marker, and escalates when it
// cannot.
//
// Parameters:
//   - ctx context.Context: the bounded cleanup context.
//   - subscriber *model.EventSubscriber: the row to mark.
//   - fenceToken string: the caller's provisioning claim.
//   - fields logrus.Fields: the caller's log fields.
//   - condition string: what is being recorded, in operator-readable words.
func (s *EventSubscriberService) recordCleanupObligation(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	fields logrus.Fields,
	condition string,
) {
	store, storeErr := s.requireStore()
	if storeErr != nil {
		logrus.WithFields(fields).Error(
			"event subscriber: " + condition + ", but the registry is not configured, so no " +
				"settlement obligation could be recorded",
		)

		return
	}

	if err := store.RecordSubscriberCredentialCleanupPending(
		ctx, subscriber.SubscriberID, time.Now(), fenceToken,
	); err != nil {
		logrus.WithFields(fields).WithField(
			"settlement_error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"event subscriber: " + condition + ", AND recording the pending credential cleanup " +
				"failed, so THIS LOG LINE IS THE ONLY RECORD of it; the principal named here must be " +
				"reconciled by hand",
		)

		return
	}

	logrus.WithFields(fields).Warn(
		"event subscriber: " + condition + "; a pending credential cleanup was recorded and " +
			"settlement will reconcile it",
	)
}

// provisioningDetail builds a bounded provisioning-failure detail carrying what reached
// the broker.
//
// Parameters:
//   - reason string: fixed wording describing the failure.
//   - subscriber *model.EventSubscriber: the row being provisioned, read for its
//     identifier.
//   - result SubscriberProvisioningResult: the only source of truth for what reached
//     the broker.
//   - retryable bool: whether repeating the request may succeed.
//
// Returns:
//   - SubscriberErrorDetail: the safe detail, with the broker-state flags set.
func (s *EventSubscriberService) provisioningDetail(
	reason string,
	subscriber *model.EventSubscriber,
	result SubscriberProvisioningResult,
	retryable bool,
) SubscriberErrorDetail {
	subscriberID := ""
	if subscriber != nil {
		subscriberID = subscriber.SubscriberID
	}

	detail := NewSubscriberErrorDetail(reason, subscriberID, retryable)
	detail.CredentialWritten = result.CredentialWritten
	detail.Compensated = result.Compensated
	detail.CompensationPending = result.CompensationOwed

	return detail
}

// classifyIssuanceTimeout re-reports a registry failure that was really the issuance
// budget running out, or the caller going away, as the timeout it was.
//
// Parameters:
//   - ctx context.Context: the issuance context, consulted for expiry.
//   - subscriberID string: named in the diagnostic so a spent budget is attributable.
//   - stage string: fixed wording for the step that was in flight, so an operator can
//     tell a slow fence claim from a slow row read.
//   - cause error: the error as the repository reported it.
//
// Returns:
//   - error: a typed timeout when the context expired and the cause was generic,
//     otherwise cause unchanged.
func (s *EventSubscriberService) classifyIssuanceTimeout(
	ctx context.Context,
	subscriberID string,
	stage string,
	cause error,
) error {
	verdict := s.issuanceTimeoutFor(ctx, stage, cause)
	if !verdict.Rewrite {
		return cause
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"stage":              stage,
		"budget":             s.budget().String(),
		"cancelled":          verdict.Cancelled,
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}).Warn(
		"event subscriber: credential issuance ran out of time at the registry rather than failing; " +
			"the provisioning claim is released and a retry is safe, because issuance re-provisions " +
			"the same boundary idempotently",
	)

	// SUBSCRIBER_PROVISIONING_FAILED, which resolves to a retryable 503, and the same code
	// the broker half of this issuance reports for a spent budget. The taxonomy carries no
	// separate timeout code; the spent budget is named in verdict.Message and in the log
	// line above, and the detail's retryable flag is what a client branches on.
	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		verdict.Message,
		NewSubscriberErrorDetail(verdict.Reason, subscriberID, true),
	)
}

// issuanceTimeoutVerdict is the shared decision behind both timeout classifications:
// whether a failure should be re-reported as a spent budget, and the wording to report
// it with.
type issuanceTimeoutVerdict struct {
	// Rewrite reports whether the cause should be replaced by a typed timeout. False means the
	// caller returns the cause unchanged, which is the common case.
	Rewrite bool

	// Cancelled distinguishes a caller that went away from a budget that ran out. Both are
	// timeouts to this service and neither is a defect in it, but they are worded
	// differently because only one of them is worth an operator's attention.
	Cancelled bool

	// Message is the client-facing sentence.
	Message string

	// Reason is the operator-facing detail. The post-broker classifier appends to it.
	Reason string
}

// issuanceTimeoutFor decides whether a failure during credential issuance is really a
// spent budget, and produces the wording for it.
//
// Parameters:
//   - ctx context.Context: the issuance context, read for its expiry.
//   - stage string: what was being attempted, for the reason line.
//   - cause error: the failure to classify. Nil yields no rewrite.
//
// Returns:
//   - issuanceTimeoutVerdict: Rewrite false for anything that is not a timeout.
func (s *EventSubscriberService) issuanceTimeoutFor(
	ctx context.Context,
	stage string,
	cause error,
) issuanceTimeoutVerdict {
	if cause == nil {
		return issuanceTimeoutVerdict{}
	}

	expiry := ctx.Err()
	if expiry == nil || !isInternalServerError(cause) {
		return issuanceTimeoutVerdict{}
	}

	verdict := issuanceTimeoutVerdict{
		Rewrite:   true,
		Cancelled: errors.Is(expiry, context.Canceled),
		Message: fmt.Sprintf(
			"Provisioning Kafka credentials did not complete within %s", s.budget(),
		),
		Reason: "The registry did not answer within the issuance budget while " + stage,
	}

	if verdict.Cancelled {
		verdict.Message = "Provisioning Kafka credentials was cancelled before it completed"
		verdict.Reason = "The request was cancelled while " + stage
	}

	return verdict
}

// classifyPostProvisioningTimeout is classifyIssuanceTimeout for the failures that
// happen AFTER the broker has been written to.
//
// Parameters:
//   - ctx context.Context: the issuance context, read for its expiry.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - stage string: what was being attempted, for the reason line.
//   - cause error: the failure, already compensated for by recordFailure.
//   - residue SubscriberProvisioningResult: what the broker is left holding.
//
// Returns:
//   - error: a typed SUBSCRIBER_PROVISIONING_FAILED carrying the residue and naming the
//     spent budget in its message, or cause unchanged when this was not a timeout.
func (s *EventSubscriberService) classifyPostProvisioningTimeout(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	stage string,
	cause error,
	residue SubscriberProvisioningResult,
) error {
	verdict := s.issuanceTimeoutFor(ctx, stage, cause)
	if !verdict.Rewrite {
		return cause
	}

	subscriberID := ""
	principal := ""

	if subscriber != nil {
		subscriberID = subscriber.SubscriberID
		principal = subscriber.KafkaPrincipal
	}

	reason := verdict.Reason

	switch {
	case residue.CredentialWritten:
		reason += ". The credential written at the broker could not be revoked, so a principal " +
			"exists that no registry row records"
	case residue.Compensated:
		reason += ". The credential written at the broker was revoked, so the subscriber has no " +
			"Kafka access until credentials are re-issued"
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"principal_hash":     subscriberLogLabel(principal),
		"stage":              stage,
		"budget":             s.budget().String(),
		"cancelled":          verdict.Cancelled,
		"credential_written": residue.CredentialWritten,
		"compensated":        residue.Compensated,
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}).Warn(
		"event subscriber: credential issuance ran out of time at the registry AFTER the broker " +
			"was written to; the provisioning claim is released and a retry is safe, because " +
			"issuance re-provisions the same boundary idempotently. credential_written here means " +
			"a principal is unaccounted for and needs manual revocation if no retry follows",
	)

	// The same retryable SUBSCRIBER_PROVISIONING_FAILED as every other expiry in the issuance
	// path, with the broker residue carried in the detail rather than in the status.
	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		verdict.Message,
		s.provisioningDetail(reason, subscriber, residue, true),
	)
}

// isInternalServerError reports whether an error carries the generic internal-server
// code.
//
// Parameters:
//   - err error: the error to classify. May be nil.
//
// Returns:
//   - bool: true when the error is a generic internal-server failure.
func isInternalServerError(err error) bool {
	if err == nil {
		return false
	}

	isInternalCode := func(code apierror.ErrorCode) bool {
		return apierror.Normalize(code) == apierror.ErrGenInternal
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return isInternalCode(apiErr.Code)
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return isInternalCode(apiErrPtr.Code)
	}

	return false
}

// recordFailure handles the narrow window in which the broker holds a credential the
// registry could not record.
//
// Revocation is BEST-EFFORT by design: the write has already failed and the caller is
// getting an error either way, so a revocation problem is logged with the principal
// named rather than replacing the error the caller actually needs to see.
//
// Parameters:
//   - ctx context.Context: the issuance context.
//   - admin subscriberPrincipalProvisioner: the client that provisioned.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - fenceToken string: the provisioning claim the failed issuance holds.
//   - cause error: the repository's error, already typed.
//
// Returns:
//   - error: the typed error to answer the request with.
func (s *EventSubscriberService) recordFailure(
	ctx context.Context,
	admin subscriberPrincipalProvisioner,
	subscriber *model.EventSubscriber,
	fenceToken string,
	cause error,
) (SubscriberProvisioningResult, error) {
	fields := logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"error_class":        kafkaErrorClassField("subscriber_credential_recording", cause),
	}

	if isConflictError(cause) {
		// A CONFLICT IS NOT REVOKED, and the marker is recorded instead.
		orphanedAt := s.clock()

		durable, cancelDurable := subscriberDurabilityContext(ctx)
		orphanRecorded := s.markCredentialOrphaned(durable, subscriber, fields, orphanedAt)
		cancelDurable()

		logrus.WithFields(fields).WithFields(logrus.Fields{
			"orphan_recorded":   orphanRecorded,
			"orphaned_at":       orphanedAt.UTC().Format(time.RFC3339),
			"lost_fence":        subscriberFenceWasLost(cause),
			"settlement_remedy": "issue once more, serially",
		}).Warn(
			"event subscriber: a concurrent operation took this subscriber, so the credential this " +
				"call generated was NOT recorded and may or may not be the one now live at the " +
				"broker; the credential was deliberately not revoked, because one SCRAM credential " +
				"exists per principal and revoking would destroy the other issuance's secret too. " +
				"The row is marked credential_orphaned_at so the ambiguity is visible; issue once " +
				"more, serially, to make the registry and the broker agree, which settles the marker",
		)

		return SubscriberProvisioningResult{CredentialWritten: true}, cause
	}

	cleanup, cancelCleanup := subscriberCleanupContext(ctx)
	defer cancelCleanup()

	if err := admin.RevokeSubscriber(cleanup, subscriber); err != nil {
		// The bounded class only: a refused administrative request carries the broker's whole
		// request dump, and the raw text goes to the trace-level diagnostic sink instead.
		logrus.WithFields(fields).WithField(
			"revocation_error_class", kafkaErrorClassField("subscriber_compensating_revocation", err),
		).Error(
			"event subscriber: recording the issuance failed AND revoking the credential it had " +
				"already written failed, so the principal named here holds a credential that no " +
				"registry row records; revoke it by hand",
		)

		// A DURABLE obligation, not only the line above. This is the same state
		// settleProvisioningRemnant records for a failed compensation — a credential live at
		// the broker that nothing in Blnk accounts for — reached by a different route, and it
		// needs the same remedy. Recorded on the cleanup context, so the spent budget that is
		// often the reason for being here does not also lose the record of it.
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal that no registry row records")

		// AND THE ORPHAN MARKER, which is not the same fact. The obligation above is FENCED
		// and tells the settlement pass there is broker-side work to finish; this one is
		// UNFENCED and is what the orphaned-credential gauge and its alert count. Two
		// different readers, two different remedies — settlement discharges the first, and
		// re-issuing or deprovisioning settles the second — so recording only one leaves the
		// other blind.
		orphanedAt := s.clock()

		durable, cancelDurable := subscriberDurabilityContext(ctx)
		orphanRecorded := s.markCredentialOrphaned(durable, subscriber, fields, orphanedAt)
		cancelDurable()

		if !orphanRecorded {
			logrus.WithFields(fields).WithField(
				"orphaned_at", orphanedAt.UTC().Format(time.RFC3339),
			).Error(
				"event subscriber: the orphan marker could not be persisted either, so the " +
					"principal named here holds a credential that NOTHING in Blnk records; this " +
					"log line is the only trace — revoke it by hand",
			)
		}

		return SubscriberProvisioningResult{CredentialWritten: true}, cause
	}

	if isSubscriberNotFoundError(cause) {
		logrus.WithFields(fields).Warn(
			"event subscriber: the subscriber was removed while its credential was being issued, so " +
				"the credential was revoked and nothing was recorded",
		)

		return SubscriberProvisioningResult{Compensated: true}, cause
	}

	// The row survives and still names whatever credential it held BEFORE this issuance —
	// but that credential no longer exists: the upsert replaced it and the revocation
	// above removed the replacement. Clearing the reference is what stops the registry
	// claiming access that has gone, which every reader of it (an operator, the migration
	// report, a reconciliation) would otherwise believe. Best-effort, because the write
	// that just failed is the same connection this one uses.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		logrus.WithFields(fields).Warn(
			"event subscriber: recording the issuance failed, so the credential written at the " +
				"broker was revoked and the registry's credential record was cleared; the " +
				"subscriber has no Kafka access until credentials are re-issued",
		)

		return SubscriberProvisioningResult{Compensated: true}, cause
	}

	// The revocation succeeded but the row still names a credential that no longer exists,
	// so the registry over-reports this subscriber's access. clearCredentialRecord has
	// already said so at ERROR; the obligation is what makes it a to-do item settlement
	// will finish rather than a line waiting to be read.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")

	return SubscriberProvisioningResult{Compensated: true}, cause
}

// clearCredentialRecord erases a subscriber's credential record, best-effort.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriber *model.EventSubscriber: the row whose record is being cleared.
//   - fenceToken string: the provisioning claim the caller holds.
//   - fields logrus.Fields: the caller's log fields, reused so both lines correlate.
//
// Returns:
//   - bool: true when the record was cleared, so the caller can phrase its own log line
//     accordingly.
func (s *EventSubscriberService) clearCredentialRecord(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fenceToken string,
	fields logrus.Fields,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.ClearSubscriberCredential(ctx, subscriber.SubscriberID, fenceToken); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the broker-side credential was revoked but clearing the registry's " +
				"credential record failed, so the registry now over-reports this subscriber's " +
				"access; clear it once the write path recovers",
		)

		return false
	}

	return true
}

// isSubscriberNotFoundError reports whether an error means "no such subscriber".
//
// It extends the package's isNotFoundError with the subscriber-specific code. That
// extension is necessary rather than tidy: ErrSubscriberNotFound is its own canonical
// code, so apierror.Normalize leaves it unchanged and the generic classifier — which
// recognises the generic and event-specific codes — does not match it.
//
// Parameters:
//   - err error: the error to classify. May be nil.
//
// Returns:
//   - bool: true when the error means the subscriber does not exist.
func isSubscriberNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if isNotFoundError(err) {
		return true
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return apierror.Normalize(apiErr.Code) == apierror.ErrSubscriberNotFound
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return apierror.Normalize(apiErrPtr.Code) == apierror.ErrSubscriberNotFound
	}

	return false
}

// RevokeSubscriberCredential ends a subscriber's Kafka access while leaving it
// registered.
//
// The credential RECORD is cleared only after the broker confirms the revocation, so
// the registry never says "no credential" while one still authenticates.
//
// Returns:
//   - error: ErrSubscriberNotFound, a revocation failure, or the repository's error.
func (s *EventSubscriberService) RevokeSubscriberCredential(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED, for the same reason issuance is: revoking the credential an issuance is in
	// the middle of writing would leave the broker and the registry describing different
	// states, and which one won would depend on the order two network calls happened to
	// complete in.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer fence.release(ctx)

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return err
	}

	// DELIBERATELY NOT GUARDED BY requireActiveSubscriber, unlike issuance and update. A
	// tombstoned row is a subscriber whose access is being taken away, and this operation
	// takes access away — refusing it would block the one manual remedy available for a
	// deregistration whose broker step keeps failing. The guard exists to stop access
	// being GRANTED or WIDENED mid-removal, and revocation does neither.
	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	// Clearing credential_reference with no broker to revoke against erases the only
	// evidence of what has to be revoked, while the credential keeps authenticating.
	if !admin.IsConfigured() {
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "revoking this subscriber's Kafka credential",
		); err != nil {
			return err
		}
	}

	// With no broker AND no credential evidence — no reference, no orphan marker, no
	// unsettled cleanup obligation — there is nothing to revoke, so the registry record is
	// simply cleared. A deployment that has never configured Kafka can still tidy a row
	// that predates that decision.
	if admin.IsConfigured() {
		// The claim is confirmed on the way in and the phase is bounded, for the same reason
		// deregistration's is: revocation is several administrative round trips and this
		// method arrived with no deadline of its own, so without the bound the phase could
		// outlive the claim and the clear below would then blank a newer issuance's record.
		revokePhase, endRevokePhase, phaseErr := fence.brokerPhaseContext(ctx)
		if phaseErr != nil {
			endRevokePhase()

			return phaseErr
		}

		revokeErr := admin.RevokeSubscriber(revokePhase, subscriber)
		endRevokePhase()

		if err := revokeErr; err != nil {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
				"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
				// Bounded, for the reason given in pruneBrokerAccess.
				"error_class": kafkaErrorClassField("subscriber_migration", err),
			}).Error(
				"event subscriber: revoking the subscriber's Kafka access failed, so the registry " +
					"record was left untouched and the credential may still work; retry the revocation",
			)

			return apierror.NewAPIError(
				apierror.ErrSubscriberProvisioningFailed,
				"Failed to revoke the subscriber's Kafka access",
				// Same treatment as the deregistration path, for the same reason: the principal and
				// the broker's own words stay in the log, and the caller receives the diagnosis
				// plus the fact that a retry is safe. Revocation is idempotent at the broker, so
				// repeating it cannot make things worse.
				NewSubscriberErrorDetail(
					"Revoking the subscriber's Kafka access failed at the broker",
					subscriber.SubscriberID, true,
				),
			)
		}
	}

	// Under the claim: the broker revocation above may have taken long enough for the lease to
	// have been lost, and clearing then would blank whatever the new owner has since recorded.
	if err := store.ClearSubscriberCredential(ctx, subscriberID, fence.token); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"broker_present":     admin.IsConfigured(),
	}).Info(
		"event subscriber: credential revoked; the subscriber remains registered and can be " +
			"re-issued",
	)

	return nil
}

// ---------------------------------------------------------------------------------------
// The dual-run migration surface

// RecordLegacyWebhookSubscription records the legacy HTTP endpoint a migrating
// subscriber received pushes on, and returns the row as it now stands.
//
// So this is a REGISTRY WRITE and nothing more: no admin client is resolved, no fence
// is taken, and no binding is touched. The statuses it can answer are therefore only
// the ones a registry write can produce.
//
// Parameters:
//   - ctx context.Context: cancels the write and the read-back.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, read back after the write.
//   - error: a typed validation error for a blank or unacceptable URL,
//     ErrSubscriberNotFound, or the repository's write error.
func (s *EventSubscriberService) RecordLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID, webhookURL string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// Trimmed HERE rather than at the repository, which stores and validates the value
	// verbatim: one column, one stored form, and the check sees the bytes that land in it.
	trimmed := strings.TrimSpace(webhookURL)
	if trimmed == "" {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A webhook URL is required",
			errors.New(
				"event subscriber: the webhook url is blank; clear the subscription instead of "+
					"recording an empty one",
			),
		)
	}

	// REFUSED PAST THE RETIREMENT INSTANT, through the same helper UpdateSubscriber uses.
	if err := requireRecordableWebhookURL(&trimmed); err != nil {
		return nil, err
	}

	if err := store.RecordSubscriberWebhookURL(ctx, subscriberID, trimmed); err != nil {
		return nil, err
	}

	logrus.WithField(
		"subscriber_id_hash", subscriberLogLabel(subscriberID),
	).Info(
		"event subscriber: legacy webhook subscription recorded; the subscriber is counted as " +
			"awaiting migration again",
	)

	// Read back rather than assembled, so the response describes the row as stored — the
	// write cleared a column this call never mentioned, and a caller must see that.
	return store.GetEventSubscriberByID(ctx, subscriberID)
}

// ClearLegacyWebhookSubscription removes a single subscriber's recorded legacy
// endpoint.
//
// Parameters:
//   - ctx context.Context: cancels the write and the read-back.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound, or the repository's write error.
func (s *EventSubscriberService) ClearLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	if err := store.ClearSubscriberWebhookURL(ctx, subscriberID); err != nil {
		return nil, err
	}

	logrus.WithField(
		"subscriber_id_hash", subscriberLogLabel(subscriberID),
	).Info("event subscriber: legacy webhook subscription cleared")

	return store.GetEventSubscriberByID(ctx, subscriberID)
}

// MarkSubscriberMigrated stamps the instant a subscriber completed its move from legacy
// HTTP delivery to Kafka consumption, WITHOUT touching webhook_url.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - time.Time: the instant recorded, in UTC, so a caller can report it without
//     re-reading the row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, or the repository's
//     error.
func (s *EventSubscriberService) MarkSubscriberMigrated(
	ctx context.Context,
	subscriberID string,
) (time.Time, error) {
	store, err := s.requireStore()
	if err != nil {
		return time.Time{}, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return time.Time{}, err
	}

	migratedAt := s.clock()
	if err := store.MarkSubscriberMigrated(ctx, strings.TrimSpace(subscriberID), migratedAt); err != nil {
		return time.Time{}, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
	}).Info("event subscriber: recorded as migrated to Kafka consumption")

	return migratedAt, nil
}

// CompleteWebhookMigration performs the whole cutover for one subscriber: it forgets
// the recorded legacy endpoint and records that the subscriber has finished moving to
// Kafka.
//
// One repository statement removes the window instead of choosing which side of it to
// fail on.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation
//     error for a blank identifier, or the repository's error.
func (s *EventSubscriberService) CompleteWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	migratedAt := s.clock()

	migrated, err := store.CompleteSubscriberWebhookMigration(
		ctx, strings.TrimSpace(subscriberID), migratedAt)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(migrated.SubscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
		// The URL itself is NOT logged: it is a third-party address, and this is the operation
		// that exists to stop retaining it. Whether one was present is the fact worth recording.
		"legacy_webhook_forgotten": true,
	}).Info(
		"event subscriber: legacy webhook subscription forgotten and the migration recorded in " +
			"one write",
	)

	return migrated, nil
}

// PurgeMigratedWebhookURLs forgets the legacy endpoint of every subscriber that
// migrated before a cut-off.
//
// The cut-off is the CALLER'S, so the retention period stays a policy decision rather
// than a constant buried in a service. A zero cut-off is refused by the repository
// rather than read as "purge everything".
func (s *EventSubscriberService) PurgeMigratedWebhookURLs(
	ctx context.Context,
	migratedBefore time.Time,
) (int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return 0, err
	}

	purged, err := store.PurgeMigratedSubscriberWebhookURLs(ctx, migratedBefore)
	if err != nil {
		return 0, err
	}

	logrus.WithFields(logrus.Fields{
		"purged":          purged,
		"migrated_before": migratedBefore.UTC().Format(time.RFC3339Nano),
	}).Info("event subscriber: legacy webhook URLs purged for migrated subscribers")

	return purged, nil
}

// ---------------------------------------------------------------------------------------
// Blnk-instance entry points

// EventSubscribers returns a subscriber service bound to this instance's datasource.
//
// The returned service resolves an administrative client from live configuration the
// first time one is needed, so EVERY CALLER MUST CLOSE IT — `defer service.Close()` —
// or the connections that client opens are held until the process ends.
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
//
// No broker is touched: registration describes a boundary, it does not grant one.
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
//
// The short-lived administrative client this resolves is closed before returning, so the
// handler needs no lifecycle handling of its own.
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
//
// The returned credential carries the plaintext secret, and this is the only call that
// ever will: put it in the response and let it go. Do not log it, do not store it, and
// do not expect to be able to read it again.
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
// It is the write behind POST and PUT /subscribers/:subscriber_id/webhook-subscription.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once WEBHOOK_DEPRECATION_SUNSET_DATE has passed every request
// to those routes is answered with 410 Gone.
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
//
// Deprecated: see RecordSubscriberWebhookSubscription.
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
//
// Deprecated: see RecordSubscriberWebhookSubscription.
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
//
// Parameters:
//   - service *EventSubscriberService: the service to close. May be nil.
func closeEventSubscriberService(service *EventSubscriberService) {
	if err := service.Close(); err != nil {
		kafkaErrorEntry("close_subscriber_management_admin", err).Warn(
			"closing the short-lived Kafka administrative client for subscriber management failed",
		)
	}
}

// maxSubscriberNameLength bounds the human label.
const maxSubscriberNameLength = 256

// subscriberFenceWasLost reports whether an error is the repository's lost-fence
// conflict.
//
// A lost fence is the one conflict that obliges the caller to compensate. Every other
// conflict — a superseded credential, a principal collision, a row already taken —
// means the operation was simply beaten to it and can be abandoned as it stands.
//
// Parameters:
//   - err error: the error returned by a fence-protected write.
//
// Returns:
//   - bool: true when the write was refused because the provisioning claim was no
//     longer held.
func subscriberFenceWasLost(err error) bool {
	if err == nil {
		return false
	}

	var apiErr apierror.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Code != apierror.ErrConflict {
		return false
	}

	// Details is an interface{} carrying the wrapped cause, so it is rendered rather than
	// type-asserted: the marker must be found whether the producer wrapped an error, a
	// string or a struct that prints one.
	if detail, ok := apiErr.Details.(error); ok {
		return strings.Contains(detail.Error(), subscriberFenceLostMarker)
	}
	if apiErr.Details != nil {
		return strings.Contains(fmt.Sprint(apiErr.Details), subscriberFenceLostMarker)
	}

	return false
}

// subscriberFenceLostMarker is the phrase database.subscriberFenceLost puts in every
// lost-fence error, and the one subscriberFenceWasLost matches on.
const subscriberFenceLostMarker = "the provisioning claim was no longer held"

// THE ISSUANCE WALL CLOCK HAS ONE CONTEXT KEY, and it is subscriberIssuanceDeadlineKey.
//
// withSubscriberSLA and subscriberSLADeadline survive as the vocabulary the phase
// builder reads in, and they now delegate to the one key.

// subscriberDurabilityReserve is the final slice of the SLA, held back so that the
// record of an unaccounted credential can always be written.
const subscriberDurabilityReserve = 500 * time.Millisecond

// withSubscriberSLA attaches the wall-clock instant the whole operation must finish by.
//
// Parameters:
//   - ctx context.Context: the operation's context.
//   - deadline time.Time: the absolute instant every phase must finish inside.
//
// Returns:
//   - context.Context: ctx carrying the SLA instant.
func withSubscriberSLA(ctx context.Context, deadline time.Time) context.Context {
	return withSubscriberIssuanceDeadline(ctx, deadline)
}

// subscriberSLADeadline reads the wall-clock instant back, if one was attached.
//
// It survives context.WithoutCancel, which is precisely why the SLA is carried as a
// value: a compensation context is detached from the work context's CANCELLATION but
// must still be bound by the same wall clock, and a value is the only part of a context
// that survives that detachment.
//
// Parameters:
//   - ctx context.Context: the context to inspect.
//
// Returns:
//   - time.Time: the SLA instant, or the zero instant.
//   - bool: whether an SLA is in force.
func subscriberSLADeadline(ctx context.Context) (time.Time, bool) {
	return subscriberIssuanceDeadline(ctx)
}

// subscriberCompensationWindowKey is the context key the RESOLVED compensation window
// travels on, as distinct from the wall clock it was carved out of.
type subscriberCompensationWindowKey struct{}

// withSubscriberCompensationWindow publishes the instant a compensation phase resolved
// to.
//
// Parameters:
//   - ctx context.Context: the phase context.
//   - window time.Time: the instant this compensation ends at.
//
// Returns:
//   - context.Context: ctx carrying the window.
func withSubscriberCompensationWindow(ctx context.Context, window time.Time) context.Context {
	return context.WithValue(ctx, subscriberCompensationWindowKey{}, window)
}

// subscriberCompensationWindow reads back the open compensation window, if there is
// one.
//
// Parameters:
//   - ctx context.Context: the context to inspect.
//
// Returns:
//   - time.Time: the window's end.
//   - bool: false when no compensation is already in progress, which is the ordinary
//     case.
func subscriberCompensationWindow(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}

	window, ok := ctx.Value(subscriberCompensationWindowKey{}).(time.Time)
	if !ok || window.IsZero() {
		return time.Time{}, false
	}

	return window, true
}

// subscriberPhaseContext derives one phase of the wall clock.
//
// A slice that has already elapsed is given a floor rather than a deadline in the past.
// The floor is not generosity: a zero-length context fails every call instantly, so a
// compensation reached slightly late would do nothing at all and report that it had
// tried — which is exactly the failure, reintroduced by arithmetic.
//
// Parameters:
//   - ctx context.Context: the context whose VALUES are kept. Cancellation is kept only
//     when detached is false.
//   - headroom time.Duration: how long before the SLA instant this phase must end.
//   - fallback time.Duration: the budget to use when no SLA is in force.
//   - detached bool: true for a compensation phase, which must survive the work phase's
//     cancellation; false for the work phase itself, which must honour the caller's.
//
// Returns:
//   - context.Context: the phase context.
//   - context.CancelFunc: must be called, conventionally by defer.
func subscriberPhaseContext(
	ctx context.Context,
	headroom, fallback time.Duration,
	detached bool,
) (context.Context, context.CancelFunc) {
	// THE WORK PHASE honours the caller's cancellation and gets no floor. Its whole
	// purpose is to keep the caller's promise, so extending it past a deadline that has
	// already passed would do work the caller has stopped waiting for — and it would hide
	// the expiry from the classifier that turns a spent budget into a timeout rather than
	// a server fault.
	if !detached {
		sla, ok := subscriberSLADeadline(ctx)
		if !ok {
			return context.WithTimeout(ctx, fallback)
		}

		return context.WithDeadline(ctx, sla.Add(-headroom))
	}

	// A COMPENSATION ALREADY IN PROGRESS IS SHARED, to the same instant.
	deadline, open := subscriberCompensationWindow(ctx)
	if !open {
		// THE FIRST COMPENSATION OF THE REQUEST. Its slice is carved off the wall clock by
		// subtracting the headroom this phase must leave behind it, and
		// boundedCompensationDeadline then applies the two rules a compensation is subject to
		// whatever it is compensating: it may never be given more than its own budget, and a
		// slice that has already elapsed yields the floor rather than a dead context. Passing
		// the reduced instant rather than duplicating that arithmetic here is what keeps ONE
		// function deciding when a compensation ends.
		phase := ctx
		if sla, hasSLA := subscriberSLADeadline(ctx); hasSLA && headroom > 0 {
			phase = withSubscriberSLA(ctx, sla.Add(-headroom))
		}

		deadline = boundedCompensationDeadline(phase, fallback)

		// THE RESERVE IS A SLICE OF THE PROMISE, NEVER AN EXTENSION OF IT.
		if sla, hasSLA := subscriberSLADeadline(ctx); hasSLA &&
			sla.After(time.Now()) && deadline.After(sla) {
			deadline = sla
		}
	}

	// DETACHED FROM CANCELLATION, STILL BOUNDED BY THE REQUEST'S OWN DEADLINE. This is the
	// only context.WithoutCancel in the subscriber lifecycle, together with the one in
	// event_admin.go, and it is written as this exact expression because WithoutCancel
	// strips a real deadline along with the cancellation — the absolute instant travels as
	// a context value precisely so it can be put back here.
	// TestSubscriberLifecycle_NoDetachedContextEscapesTheCleanupHelpers pins the spelling,
	// because a new cleanup path reaching for WithoutCancel directly is the obvious thing
	// to write and restores a compensation with no bound at all.
	base := withSubscriberIssuanceDeadline(context.WithoutCancel(ctx), deadline)

	// The window is published as well as the deadline, so a phase derived inside this one shares
	// it exactly instead of carving a second slice out of what is left.
	return context.WithDeadline(withSubscriberCompensationWindow(base, deadline), deadline)
}

// It is granted ONCE per request rather than once per nested phase, which is what
// subscriberCompensationWindow enforces.

// subscriberDurabilityContext derives the context the durable exposure markers are
// written on.
//
// Detached from the caller's cancellation, like every compensation here.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values only.
//
// Returns:
//   - context.Context: the phase context.
//   - context.CancelFunc: must be called, conventionally by defer.
func subscriberDurabilityContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return subscriberPhaseContext(ctx, 0, subscriberDurabilityReserve, true)
}

// abandonUpdateAfterLostFence records what an update leaves behind when its claim
// lapses, and returns the error the caller should see.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as this call would have written it.
//   - stored subscriberAuthorization: the authorization the registry still records.
//   - pruned int: how many bindings the abandoned attempt had already removed.
//   - cause error: the lost-fence error.
//
// Returns:
//   - error: cause, unchanged, so the API layer answers the same 409 it would for any
//     conflict.
func (s *EventSubscriberService) abandonUpdateAfterLostFence(
	subscriber *model.EventSubscriber,
	stored subscriberAuthorization,
	pruned int,
	cause error,
) error {
	if pruned == 0 {
		// Nothing reached the broker, so there is no residue to describe and a conflict is the
		// whole story.
		return cause
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":   subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":       subscriberLogLabel(subscriber.KafkaPrincipal),
		"acl_bindings_removed": pruned,
		"recorded_topics":      len(stored.topics),
	}).Warn(
		"event subscriber: an authorization change lost its provisioning claim after pruning Kafka " +
			"grants, so the registry was NOT updated and the subscriber now has LESS access than the " +
			"registry records; this is fail-closed and the next successful authorization change " +
			"restores it, or retry this one",
	)

	return cause
}

// subscriberAuthorization is the subset of a subscriber that determines its broker-side
// ACL bindings, in a form two snapshots can be compared by.
//
// subscriberDesiredBindings derives every binding from exactly four things: the bound
// principal, the normalized topic list, the consumer-group prefix, and whether the row
// declares a key scope. Those are therefore the whole of this type.
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
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row to snapshot. Nil yields the zero
//     snapshot.
//
// Returns:
//   - subscriberAuthorization: the comparable snapshot.
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
//
// It is asymmetric on purpose. Removing a topic, or leaving the set alone, cannot give
// a principal access it lacked; ADDING one can, and so can moving the principal or the
// consumer group, because each of those is a new binding on a name that had none.
//
// Parameters:
//   - prior subscriberAuthorization: the authorization as the registry held it.
//
// Returns:
//   - bool: true when this snapshot names a binding prior did not.
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
//
// NARROWING, and re-issuance. Removing bindings reduces the exposure, which is what an
// operator responding to an orphan needs; freezing the row at its widest would make the
// guard worse than the gap.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row with the requested changes already
//     applied.
//   - prior subscriberAuthorization: the authorization as the registry held it before
//     them.
//
// Returns:
//   - error: ErrConflict when the change widens the boundary of an unaccounted-for
//     principal, otherwise nil.
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
//
// Parameters:
//   - other subscriberAuthorization: the snapshot to compare against.
//
// Returns:
//   - bool: true when the principal, consumer group and normalized topic set all match.
func (a subscriberAuthorization) equals(other subscriberAuthorization) bool {
	return a.principal == other.principal &&
		a.consumerGroup == other.consumerGroup &&
		a.keyScoped == other.keyScoped &&
		slices.Equal(a.topics, other.topics)
}

// authorizationNeedsReconciliation decides whether an update must touch the broker.
//
// The obvious one is that the authorization MOVED: the snapshots differ, so the desired
// binding set differs and the broker has to be brought to it.
//
// Parameters:
//   - stored subscriberAuthorization: the authorization as the registry held it.
//   - updated *model.EventSubscriber: the row as it will be written.
//   - changes SubscriberUpdate: the request, consulted for which fields were explicitly
//     given.
//
// Returns:
//   - bool: true when prune and grant must run.
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

// markCredentialOrphaned persists the issuance-orphan state, best-effort.
//
// The write takes no claim token by design; see the store seam's documentation for why
// conditioning it on the claim would lose the marker in exactly the case that produces
// it.
//
// Parameters:
//   - ctx context.Context: bounds the write.
//   - subscriber *model.EventSubscriber: the row whose principal holds the orphaned
//     credential.
//   - fields logrus.Fields: the caller's log fields, reused so the lines correlate.
//   - orphanedAt time.Time: the instant to record on the first marking.
//
// Returns:
//   - bool: true when the state is durably recorded.
func (s *EventSubscriberService) markCredentialOrphaned(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fields logrus.Fields,
	orphanedAt time.Time,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.MarkSubscriberCredentialOrphaned(ctx, subscriber.SubscriberID, orphanedAt); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the orphaned-credential marker could not be written, so this " +
				"exposure is recorded nowhere but the logs and no gauge or alert can see it",
		)

		return false
	}

	return true
}

// markRevocationFailed persists the failed-deregistration state, best-effort.
//
// Best-effort, and its failure is logged rather than returned: the caller is already
// answering with a provisioning failure, and the tombstone — the durable part that
// makes the work findable — is already in place.
//
// Parameters:
//   - ctx context.Context: bounds the write.
//   - subscriber *model.EventSubscriber: the tombstoned row.
//   - fields logrus.Fields: the caller's log fields.
//   - failedAt time.Time: when the attempt was refused.
//
// Returns:
//   - bool: true when the state is durably recorded.
func (s *EventSubscriberService) markRevocationFailed(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fields logrus.Fields,
	failedAt time.Time,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.MarkSubscriberRevocationFailed(ctx, subscriber.SubscriberID, failedAt); err != nil {
		withLoggableCause(logrus.WithFields(fields), err).Error(
			"event subscriber: the failed-revocation marker could not be written, so the row records " +
				"only that a deregistration began and not that the broker refused it",
		)

		return false
	}

	return true
}

// CompleteLegacyWebhookMigration forgets a subscriber's recorded legacy endpoint AND
// records that it finished migrating, atomically.
//
// It also removes a second cost. Nothing in these two columns is an authorization and
// nothing here reaches Kafka, so neither the fence nor the full-row rewrite was ever
// needed.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant
//     so a caller can report it without re-reading.
//   - error: a typed validation error for a blank identifier, ErrSubscriberNotFound, or
//     the repository's error.
func (s *EventSubscriberService) CompleteLegacyWebhookMigration(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return nil, err
	}

	migratedAt := s.clock()

	migrated, err := store.CompleteSubscriberWebhookMigration(
		ctx, strings.TrimSpace(subscriberID), migratedAt,
	)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(migrated.SubscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
	}).Info(
		"event subscriber: legacy webhook subscription forgotten and the migration recorded in one " +
			"write",
	)

	return migrated, nil
}

// maxSubscriberKeyScopeLength bounds the recorded key-scoped constraint.
const maxSubscriberKeyScopeLength = 256

// subscriberHashResolutionPageSize is how many registry rows one page of a pseudonym
// resolution reads.
//
// The repository caps a page, so this is chosen at that cap rather than above it: a
// larger value would be silently reduced and the walk would then make more round trips
// than its own arithmetic predicted.
const subscriberHashResolutionPageSize = 100

// subscriberHashResolutionMaxPages bounds a pseudonym resolution.
//
// A resolution walks the registry, so it needs a ceiling or one request can read an
// arbitrarily large table. One hundred pages of one hundred rows is ten thousand
// subscribers, which is far beyond the scale this registry is built for while still
// being a bounded amount of work for a master-key-gated operator query.
const subscriberHashResolutionMaxPages = 100

// ErrSubscriberHashResolutionExhausted is returned when a pseudonym resolution reached
// its page ceiling without a match.
//
// It is distinct from "not found" deliberately: a resolution that stopped early has not
// established that the token is unknown, and answering not-found would tell an operator
// the subscriber does not exist when what happened is that nobody looked.
var ErrSubscriberHashResolutionExhausted = errors.New(
	"blnk: the subscriber pseudonym could not be resolved within the bounded number of registry " +
		"pages, so whether it names a subscriber is unknown",
)

// ResolveSubscriberByPseudonym finds the subscriber a subscriber_id_hash token names.
//
// Every metric attribute and every log field naming a subscriber carries a PSEUDONYM
// rather than the identifier — see subscriberLagLabel for why. An operator therefore
// reads a token off a lag alert and needs the subscriber.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//   - pseudonym string: the token, as it appears in a log line, a metric label or an
//     alert.
//
// Returns:
//   - *model.EventSubscriber: the matching row.
//   - error: a typed validation error for a blank token, ErrSubscriberNotFound when the
//     registry holds no subscriber with that pseudonym,
//     ErrSubscriberHashResolutionExhausted when the ceiling was reached first, or the
//     repository's error.
func (s *EventSubscriberService) ResolveSubscriberByPseudonym(
	ctx context.Context,
	pseudonym string,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	wanted := strings.ToLower(strings.TrimSpace(pseudonym))
	if wanted == "" {
		return nil, apierror.NewAPIError(apierror.ErrInvalidInput,
			"A subscriber pseudonym is required to resolve a subscriber by its hash", nil)
	}

	var resolved *model.EventSubscriber

	complete, err := walkSubscriberRegistry(ctx, store, "pseudonym_resolution",
		func(subscriber model.EventSubscriber) bool {
			if model.HashIdentifier(subscriber.SubscriberID) != wanted {
				return true
			}

			found := subscriber
			resolved = &found

			return false
		})
	if err != nil {
		return nil, err
	}

	if resolved != nil {
		return resolved, nil
	}

	// The registry was walked to its END, so the token is genuinely unknown rather than
	// merely unreached. This is the ONLY path that may answer "not found".
	if complete {
		return nil, apierror.NewAPIError(apierror.ErrSubscriberNotFound,
			"No subscriber in the registry has that pseudonym", nil)
	}

	return nil, ErrSubscriberHashResolutionExhausted
}

// ListSubscribersAwaitingRevocation returns every subscriber whose broker-side access
// Blnk began taking away and could not finish. It is the read behind GET
// /subscribers?revocation_pending=true.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//
// Returns:
//   - []model.EventSubscriber: the affected rows, oldest obligation first, so the
//     responder works the longest exposure first — the same order the alert fires on.
//   - error: ErrSubscriberHashResolutionExhausted when the walk could not cover the
//     registry, or the repository's error.
func (s *EventSubscriberService) ListSubscribersAwaitingRevocation(
	ctx context.Context,
) ([]model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	pending := make([]model.EventSubscriber, 0)

	complete, err := walkSubscriberRegistry(ctx, store, "revocation_pending_scan",
		func(subscriber model.EventSubscriber) bool {
			if subscriber.RevocationPendingAt != nil {
				pending = append(pending, subscriber)
			}

			return true
		})
	if err != nil {
		return nil, err
	}

	if !complete {
		return nil, ErrSubscriberHashResolutionExhausted
	}

	// Oldest obligation first. The alert fires on the oldest age, so this order puts the
	// row the alert is actually about at the top rather than leaving the responder to
	// compare timestamps.
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].RevocationPendingAt.Before(*pending[j].RevocationPendingAt)
	})

	return pending, nil
}

// walkSubscriberRegistry pages the whole registry, calling visit for each row.
//
// It is shared by the pseudonym resolver and the revocation scan because both need the
// same three properties, and each is easy to get subtly wrong on its own: a bound on
// the work, a per-page cancellation check, and — the important one — a truthful answer
// about whether the walk actually REACHED THE END. A walk that stopped at its ceiling
// has established nothing about the rows it did not read, and a caller that treats that
// as an empty result states something it does not know: "no such subscriber", or "no
// credentials to revoke".
//
// Parameters:
//   - ctx context.Context: checked before every page as well as inside each read,
//     because a walk is the one operation here that can outlive its caller's interest
//     in the answer.
//   - store eventSubscriberStore: the registry.
//   - operation string: a fixed literal naming the caller, for the ceiling warning.
//   - visit func(model.EventSubscriber) bool: called per row; returning false stops the
//     walk early, which counts as complete because the answer was found.
//
// Returns:
//   - bool: true when the walk reached the end of the registry or visit stopped it.
//   - error: the repository's error, or the context's.
func walkSubscriberRegistry(
	ctx context.Context,
	store eventSubscriberStore,
	operation string,
	visit func(model.EventSubscriber) bool,
) (bool, error) {
	var cursor *model.SubscriberCursor

	for page := 0; page < subscriberHashResolutionMaxPages; page++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}

		batch, listErr := store.ListEventSubscribers(ctx, model.SubscriberPageQuery{
			Limit:  subscriberHashResolutionPageSize,
			Cursor: cursor,
		})
		if listErr != nil {
			return false, listErr
		}

		for i := range batch.Subscribers {
			if !visit(batch.Subscribers[i]) {
				return true, nil
			}
		}

		// The repository answers whether anything follows, so the end of the registry is READ
		// rather than inferred from a short page — and the cursor is what makes the next page
		// resume exactly where this one stopped even if rows were registered or deregistered
		// in between.
		if !batch.HasMore || batch.NextCursor == nil {
			return true, nil
		}

		cursor = batch.NextCursor
	}

	logrus.WithFields(logrus.Fields{
		"operation":  operation,
		"pages_read": subscriberHashResolutionMaxPages,
		"page_size":  subscriberHashResolutionPageSize,
	}).Warn(
		"event subscriber: a registry walk reached its page ceiling, so its answer is UNKNOWN " +
			"rather than negative; the registry is larger than the walk's bound",
	)

	return false, nil
}

// ListEventSubscribersAwaitingRevocation returns every subscriber with an outstanding
// broker-side revocation. It is the read behind GET
// /subscribers?revocation_pending=true, and the first step of the
// SubscriberRevocationOutstanding runbook.
func (b *Blnk) ListEventSubscribersAwaitingRevocation(
	ctx context.Context,
) ([]model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ListSubscribersAwaitingRevocation(ctx)
}

// ResolveEventSubscriberByPseudonym finds the subscriber a subscriber_id_hash token names. It
// is the read behind GET /subscribers?subscriber_id_hash=<token>.
//
// The short-lived service this resolves is closed before returning, so the handler needs no
// lifecycle handling of its own.
func (b *Blnk) ResolveEventSubscriberByPseudonym(
	ctx context.Context,
	pseudonym string,
) (*model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ResolveSubscriberByPseudonym(ctx, pseudonym)
}

// WithKafkaAdminResolver installs the resolver this service asks for a PROCESS-WIDE
// administrative client the first time it needs one.
//
// Parameters:
//   - resolve func() (subscriberPrincipalProvisioner, error): the resolver, or nil.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithKafkaAdminResolver(
	resolve func() (subscriberPrincipalProvisioner, error),
) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resolveAdmin = resolve

	return s
}

// WithBackgroundScheduler installs the scheduler compensating work is handed to.
//
// None of that work is response-critical. The caller's answer does not depend on it: a
// failed issuance is an error whatever the cleanup achieves, and the fence release only
// spares the next caller from waiting out a lease that expires on its own.
//
// Parameters:
//   - schedule func(func()): the scheduler, or nil for inline execution.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
func (s *EventSubscriberService) WithBackgroundScheduler(schedule func(func())) *EventSubscriberService {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deferWork = schedule

	return s
}

// schedule hands a compensating task to the installed scheduler, or runs it inline when
// there is none.
//
// Every caller must be a task whose outcome the RESPONSE DOES NOT DEPEND ON. A task
// that runs inline in a test and in the background in production cannot be one whose
// result is read.
//
// Parameters:
//   - task func(): the work to run. Nil is ignored.
func (s *EventSubscriberService) schedule(task func()) {
	if task == nil {
		return
	}

	if s == nil {
		task()

		return
	}

	s.mu.Lock()
	deferWork := s.deferWork
	s.mu.Unlock()

	if deferWork == nil {
		task()

		return
	}

	deferWork(task)
}

// defersWork reports whether a scheduler is installed, so a caller can ask the broker
// to defer work only when there is somewhere for it to be deferred TO.
//
// Returns:
//   - bool: true when scheduled work runs off the caller's goroutine.
func (s *EventSubscriberService) defersWork() bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deferWork != nil
}

// subscriberFenceRenewalDivisor sets how often a held fence is renewed, as a fraction
// of the lease.
const subscriberFenceRenewalDivisor = 3

// subscriberFenceMinRenewalInterval floors the renewal interval so a short test lease cannot
// turn the heartbeat into a busy loop against the database.
const subscriberFenceMinRenewalInterval = 250 * time.Millisecond

// heartbeat renews the claim until it is stopped, the claim is lost, or the process ends.
//
// It runs on its OWN bounded context per renewal rather than the operation's: the commonest
// reason an operation needs a renewal is that it is slow, and on a cancelled operation context
// every renewal would fail instantly — a heartbeat that stops beating exactly when it is needed.
func (f *subscriberFence) heartbeat() {
	defer close(f.done)

	interval := f.lease / subscriberFenceRenewalDivisor
	if interval < subscriberFenceMinRenewalInterval {
		interval = subscriberFenceMinRenewalInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			renewal, cancel := context.WithTimeout(context.Background(), subscriberCleanupBudget)
			err := f.store.RenewSubscriberProvisioningFence(renewal, f.subscriberID, f.token, f.lease)
			cancel()

			if err == nil {
				continue
			}

			logrus.WithField(
				"subscriber_id_hash", subscriberLogLabel(f.subscriberID),
			).WithField(
				"error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
			).Error(
				"event subscriber: this operation's provisioning claim could not be renewed, so it is " +
					"no longer fenced; a concurrent issuance or deregistration can now interleave with " +
					"it, and this operation's own conditional writes are what will report that as a " +
					"conflict",
			)

			return
		}
	}
}

// stopHeartbeat ends the renewal loop and waits for it to exit, so no renewal can be in flight
// when the claim is cleared. Idempotent.
func (f *subscriberFence) stopHeartbeat() {
	// A fence whose claim was never granted has no heartbeat to stop, and waiting on its
	// nil done channel would block for ever. fenceSubscriber returns exactly that fence
	// alongside its error so callers can defer the release unconditionally.
	if f == nil || f.stop == nil || f.done == nil {
		return
	}

	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
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
//
// A nil row yields the empty, none-enforced disclosure rather than panicking: it is
// read on rows loaded from a repository whose not-found representation is a nil
// pointer, and inside paths that are already reporting a problem.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the registry row. May be nil.
//
// Returns:
//   - keyScopeDisclosure: the prefix and its enforcement point.
func describeSubscriberKeyScope(subscriber *model.EventSubscriber) keyScopeDisclosure {
	return keyScopeDisclosure{
		Prefix:      strings.TrimSpace(SubscriberPartitionKeyPrefix(subscriber)),
		Enforcement: subscriber.KeyScopeEnforcement(),
	}
}

// logKeyScopeDisclosure records the issuance line that accompanies a credential issued
// to a subscriber carrying a key scope.
//
// What replaced the refusal was DISCLOSURE: issue the whole-topic credential, and state
// that the narrowing is the subscriber's own to apply. That was accurate and it was not
// a boundary.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row a credential has just been issued for.
//   - disclosure keyScopeDisclosure: the prefix and the component that enforces it.
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
