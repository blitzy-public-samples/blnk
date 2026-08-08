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

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// event_subscriber.go is the subscriber registry and the one-time credential issuance behind
// POST /subscribers/{id}/kafka-credentials (requirement R-7).
//
// # The access model, stated once
//
// THERE ARE NO PER-TENANT TOPICS. Blnk owns five category topics and a `.dlt` sibling for each
// (event_topics.go); four of those categories are SUBSCRIBER-FACING — transactions, balances,
// identities and ledgers — while system is INTERNAL and no subscriber may be granted it, nor any
// dead-letter topic. Every event that describes LEDGER STATE therefore has a grantable route;
// the one event type without one is system.error, whose frozen body carries verbatim internal
// error text. A subscriber is granted a SUBSET of the grantable set,
// whatever its registry row authorises, and isolation comes from four things:
//
//  1. The KAFKA PRINCIPAL. Each subscriber is a distinct SASL/SCRAM identity derived
//     from its immutable subscriber id, so one subscriber's credential can never
//     authenticate as another's.
//  2. The ACL GRANT. Read and Describe are bound to exactly the topics on the registry
//     row, with a LITERAL pattern per topic. There is no wildcard, no prefixed topic
//     pattern and no cluster-wide Describe, which is what the subscriber-isolation
//     criterion asserts.
//  3. The CONSUMER GROUP namespace, granted with a PREFIXED pattern so the subscriber
//     may run several groups of its own without an administrative round trip, while
//     every other subscriber's namespace stays out of reach.
//
// Those three are the whole boundary, and each of them is something the broker evaluates
// on every request. The grant is also RECONCILED rather than accumulated: every issuance
// and every authorization change reads the principal's live bindings and deletes the ones
// the current authorization no longer implies, so narrowing a subscriber actually narrows
// it at the broker instead of leaving the union of everything it was ever granted.
//
// THE PARTITION-KEY PREFIX IS NOT A FOURTH MECHANISM. Kafka's authorizer has no
// message-key dimension, so no ACL can confine a subscriber to a slice of a topic by key.
// A row that records one is therefore recording an authorization narrower than any
// credential this service can mint — so ISSUANCE REFUSES for that row rather than handing
// back whole-topic access under a registry that says otherwise. Calling the field
// "advisory" was the more dangerous framing: a qualification in a comment does not survive
// somebody reading the registry to decide who can see what, and a refused issuance does.
//
// Creating a topic per subscriber would be the obvious alternative and it is the wrong
// one: it multiplies the partition count and the ordering guarantee by the number of
// subscribers, and it makes adding a subscriber a topic-provisioning event rather than a
// credential one. It is, however, the answer if per-key isolation is genuinely required —
// see model.EventSubscriber's documentation, which names it and the filtering-gateway
// alternative.
//
// # SECRET HANDLING IS THE POINT OF THIS FILE
//
// The generated SASL password is returned ONLY BY ISSUANCE, in the SubscriberCredential value
// IssueSubscriberCredential produces, and it is PERSISTED NOWHERE. Only a non-reversible
// reference (model.DeriveCredentialReference) and the issuance instant are stored — the same
// posture as blnk.api_keys, where the stored value is a bcrypt hash and the raw key is never
// kept.
//
// Password() reads that in-memory value and may be called as often as the caller likes while
// the value lives; what does not exist is any route to obtain the secret AFTERWARDS. Nothing
// reads it back from storage, no list projection, log line, error message, trace attribute or
// metric label carries it, and blnk.event_subscribers has no column capable of holding one. A
// lost password can only be REPLACED by issuing a new one.
//
// # The legacy webhook columns, and what /hooks is not
//
// A subscriber row carries a legacy webhook_url and a migrated_at timestamp FOR THE DUAL-RUN
// WINDOW ONLY: they record where a subscriber used to receive pushes and when it finished
// moving, which is what gives an already-webhooked subscriber somewhere to be migrated from
// and what makes the 410 Gone sunset behaviour observable on a route at all.
//
// /hooks IS NOT THAT SURFACE AND IS UNTOUCHED. Those are the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts in internal/hooks — synchronous interception with a
// response contract that can influence transaction processing. They stay fully functional and
// have nothing to do with this file.
//
// # What is deliberately NOT here
//
//   - NO SQL. database/event_subscriber.go owns every statement; this file delegates.
//   - NO HTTP handling and NO master-key gating. api/subscribers.go owns those, following the
//     ensureHookManagementAuthorized pattern in api/hooks.go.
//   - NO direct Kafka admin calls. Everything broker-side goes through event_admin.go, so
//     there is one authenticated administrative client and one place where a credential or an
//     ACL is written.
//   - NO CONSUMER of any kind: no consumer library, no consumer error handling and no
//     subscriber-side dead-lettering. Blnk publishes the `<topic>.dlt` naming convention in
//     docs/event-streaming.md and stops there.

// SubscriberCredentialIssuanceBudget is the hard wall-clock ceiling on one credential
// issuance, and it is a REQUIREMENT rather than a tuning knob (R-7).
//
// Issuance makes up to four broker round trips — the authorizer probe, the
// credential-existence probe, the SCRAM upsert and the ACL batch — followed by one
// database write. Any of the four can hang on a broker that accepts a TCP connection and
// then stops answering, which is precisely the failure a per-request timeout exists for:
// without one, the handler's goroutine is held until the client gives up and the operator
// learns nothing about why.
//
// Enforcing it HERE rather than relying on the admin client's own request timeout is
// deliberate. That timeout is per round trip, so four of them serialised can exceed this
// budget while every individual call looks healthy. The budget is therefore applied once,
// around the whole operation, and every step inherits the remaining time.
const SubscriberCredentialIssuanceBudget = 5 * time.Second

// generatedSubscriberPasswordLength is how many characters a generated SASL secret
// carries.
//
// Forty-eight over the 62-character alphabet below is roughly 285 bits of entropy. That is
// far beyond what MinSCRAMPasswordLength requires, and the excess is free: the secret is
// generated and copied by machines, never typed, so length costs nobody anything while a
// short secret is open — a Kafka SASL handshake has no rate limit and no lockout.
const generatedSubscriberPasswordLength = 48

// subscriberPasswordAlphabet is the alphabet a generated secret is drawn from:
// alphanumerics only, deliberately.
//
// A SASL/SCRAM password travels through several parsers that treat punctuation specially —
// a JAAS configuration string, a Java properties file, a shell command line in a runbook, a
// Compose environment file — and a secret containing a quote, a backslash or a dollar sign
// breaks one of them in a way that reads as a wrong password rather than as a quoting bug.
// Length carries the entropy instead; see generatedSubscriberPasswordLength.
const subscriberPasswordAlphabet = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"0123456789"

// subscriberPasswordGenerationAttempts bounds the retries that guarantee the generated
// secret clears the distinct-character floor validateSCRAMPassword applies.
//
// A 48-character draw from a 62-character alphabet falls below sixteen distinct characters
// with probability far below one in 10^30, so this loop is expected to run exactly once —
// but "expected" is not "guaranteed", and a generator that can emit a value the broker
// boundary will reject is a generator with a failure mode nothing tests. Re-drawing makes
// the guarantee total at a cost of nothing.
const subscriberPasswordGenerationAttempts = 8

// errSubscriberStoreUnavailable reports a service built with no datasource.
//
// NewBlnk(nil) is a supported construction in this codebase — several tests use it — so a
// service can legitimately hold a nil store, and every operation reports this instead of
// dereferencing nil. It is a sentinel so a test can assert the condition without matching
// on message text.
var errSubscriberStoreUnavailable = errors.New(
	"event subscriber: no datasource is configured, so the subscriber registry is unavailable",
)

// SubscriberErrorDetail is the detail attached to a typed API error whose cause came from
// outside Blnk — the Kafka client, the database driver, or the TLS material on disk.
//
// # DATA-01: a sanitized log is not a boundary if the raw cause is returned anyway
//
// The service was already careful with its logs: every failure path builds fields through
// sanitizeLogValue before writing them. It then handed the RAW cause to
// apierror.NewAPIError, and that function does two things with what it is given —
// `logrus.WithField("details", details).Error("API error")` and
// `Details interface{} \`json:"details,omitempty"\“ on the returned struct. So the raw
// cause was logged a second time, unsanitized, and serialised into the HTTP RESPONSE. The
// sanitizer was bypassed on both sides at once, and the response side is the more serious of
// the two because it leaves the deployment.
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
//
// None of that is knowledge an API caller needs, and all of it is a map of the deployment's
// interior handed to whoever provisioned a subscriber.
//
// # What replaces it
//
// Fixed wording chosen at the call site, the caller's OWN subscriber identifier — which they
// supplied and which is therefore not a disclosure — and the small set of state flags that
// tell the caller what to do next. The cause is not discarded, only redirected: the call site
// logs it, bounded through sanitizeLogValue, so an operator keeps the broker's exact words
// while the caller receives the diagnosis and nothing else.
//
// This mirrors EventTransportErrorDetail in event_publisher.go, which is the established
// pattern for the same problem on the publish path.
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
}

// NewSubscriberErrorDetail builds a bounded subscriber-failure detail.
//
// The cause is deliberately NOT a parameter. Excluding it from the signature is what makes
// the guarantee structural rather than a matter of care at each call site: there is no way to
// pass a cause in, so no rendering of the result — as JSON, with %v, or field by field — can
// contain one.
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
// It is a NARROW seam over database.IDataSource for two reasons. It makes every registry
// operation testable with an in-memory fake and no PostgreSQL, which is what lets the
// secret-handling assertions run in the unit suite. And it states, in one readable block,
// exactly which of the datasource's methods subscriber management may reach — so this
// service cannot record a transaction, publish an event or touch the outbox even by
// mistake.
//
// NO METHOD HERE ACCEPTS OR RETURNS A PLAINTEXT SECRET, and none may be added that does.
// RecordSubscriberCredentialIfUnchanged takes a REFERENCE the caller has already derived,
// which is what makes "the secret is not persisted" a property of the contract rather than
// a habit of its callers.
type eventSubscriberStore interface {
	// CreateEventSubscriber registers a subscriber and returns the stored row, including
	// the database's own surrogate key and bookkeeping timestamps.
	CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error)

	// GetEventSubscriberByID reads one subscriber by its business key, returning a typed
	// not-found error rather than a bare sql.ErrNoRows.
	GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)

	// ListEventSubscribers pages the registry newest first.
	ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error)

	// UpdateEventSubscriber replaces the mutable columns of an existing row.
	UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error

	// TakeEventSubscriber removes a subscriber and RETURNS the row it removed, so the
	// caller still holds the principal and topics that broker-side revocation needs.
	TakeEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)

	// RecordSubscriberCredentialIfUnchanged persists an issuance only while the row still
	// holds the reference observed before provisioning, reporting a conflict otherwise.
	RecordSubscriberCredentialIfUnchanged(ctx context.Context, subscriberID string, expected *string, credentialReference string, issuedAt time.Time) error

	// ClearSubscriberCredential returns a row to the "registered, not yet provisioned"
	// state. It is the compensating half of an issuance record.
	ClearSubscriberCredential(ctx context.Context, subscriberID string) error

	// MarkSubscriberMigrated stamps the instant a subscriber completed its move from
	// legacy HTTP delivery to Kafka consumption.
	MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error

	// PurgeMigratedSubscriberWebhookURLs erases the legacy URL of every subscriber whose
	// migration completed strictly before the cut-off, returning how many rows changed.
	PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error)

	// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row,
	// so deregistration can revoke at the broker while the row that names the principal
	// still exists.
	MarkSubscriberRevocationPending(ctx context.Context, subscriberID string, pendingAt time.Time) (*model.EventSubscriber, error)

	// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation,
	// returning the token the claim is held under and a conflict when somebody else holds
	// it. Nothing may touch the broker for a subscriber without holding its claim.
	ClaimSubscriberForProvisioning(ctx context.Context, subscriberID string, lease time.Duration) (string, error)

	// ReleaseSubscriberProvisioningFence clears a claim the caller still holds, so a retry
	// after a fast failure need not wait out the lease.
	ReleaseSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string) error
}

// subscriberPrincipalProvisioner is the administrative surface credential issuance needs.
//
// It is the smallest useful subset of KafkaAdmin: report whether a broker is configured,
// report where it is, provision a principal, and revoke one. It notably CANNOT create or
// delete a topic, read offsets or measure lag — a registry service that could create
// topics would be one misplaced call away from changing the topology it hands out.
//
// The seam also removes the broker from the unit tests entirely. Provisioning is where the
// interesting failure modes are (a timeout, an ACL grant that fails after the credential
// was written), and a fake is the only way to drive those deterministically.
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
	// authorization change calls it AFTER persisting, so a widening reaches the broker only
	// once the registry records it.
	GrantSubscriberAccess(ctx context.Context, subscriber *model.EventSubscriber) (SubscriberACLReconciliation, error)

	// Close releases the client's pooled connections.
	Close() error
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
//
// The Kafka PRINCIPAL and the CONSUMER GROUP are deliberately ABSENT, and this is the same
// decision api/model.CreateSubscriber documents. Neither is a name a caller wants; each is
// an authorization boundary a caller would be selecting. The principal is what every ACL
// binding is granted to, so a caller able to choose it could have its own topics added to
// another subscriber's grant; the consumer group is granted with a PREFIXED pattern, so a
// caller able to choose it could name a prefix spanning other subscribers' namespaces and
// take their partition assignments. Both are therefore DERIVED from the subscriber
// identifier, and there is no field through which a value can be offered.
type SubscriberRegistration struct {
	// SubscriberID is the business key and the {id} in
	// POST /subscribers/{id}/kafka-credentials. Leave it blank to have one generated in
	// the repository's "<prefix>_<uuid>" form.
	//
	// When supplied it must already be canonical — lowercase ASCII letters, digits,
	// underscore and hyphen — because the principal and the consumer group are derived
	// from it. A non-canonical value is REFUSED rather than folded: Kafka principals are
	// compared byte for byte, so folding case would merge two distinct broker identities
	// onto one credential and one grant.
	SubscriberID string

	// Name is the human label an operator triages the subscriber by. Required, because an
	// unnamed principal cannot be triaged and being able to answer "who is this
	// principal?" months later is most of the reason the registry exists.
	Name string

	// AuthorizedTopics is the exact set of topics the subscriber may Read and Describe.
	// Empty is VALID and means authorised for nothing, which is the fail-closed default of
	// a fresh registration rather than a missing value.
	//
	// Every entry must be a subscriber-facing Blnk category topic under the configured
	// prefix. Dead-letter topics and the internal categories are not grantable — see
	// validateSubscriberGrant.
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
// Every field is nilable so that "omitted" is distinguishable from "explicitly cleared",
// which this shape needs rather than merely benefits from: an omitted PartitionKeyPrefix
// leaves whatever the row records, while a present empty string CLEARS the constraint — and
// clearing it is what makes an unprovisionable subscriber provisionable again. A plain string
// cannot express the difference, so it would make silently inverting an operator's intent
// possible.
//
// The principal, the consumer group, the credential record and the migration instant are
// all absent for the reasons api/model.UpdateSubscriber sets out: the first two are derived
// boundaries rather than attributes, and the last two are written only by the operations
// that own them.
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
	// CHANGING THIS LIST CHANGES WHAT THE SUBSCRIBER CAN READ, and it does so at the broker
	// as part of the update rather than at the next issuance.
	//
	// UpdateSubscriber reconciles the broker-side grant in three steps around the write:
	// the bindings the new list no longer implies are DELETED first, the row is persisted
	// second, and the bindings it adds are created third. That order is what keeps every
	// partial failure fail-closed — a narrowing is in force before the registry claims it,
	// and a widening reaches the broker only after the registry records it.
	//
	// It used to be additive only, so a topic removed from this list kept its Read and
	// Describe bindings until somebody revoked the whole subscriber: the registry said the
	// access was gone and the subscriber kept consuming, with nothing failing to say so.
	AuthorizedTopics []string

	// PartitionKeyPrefix replaces the recorded key constraint when non-nil; a present empty
	// string CLEARS it. It grants and revokes nothing at the broker — what it does is decide
	// whether a credential can be issued at all, because Kafka cannot enforce a key scope
	// and this service refuses to pretend otherwise.
	//
	// SETTING IT ON A SUBSCRIBER THAT ALREADY HOLDS A CREDENTIAL IS REFUSED, with
	// apierror.ErrSubscriberIsolationUnenforceable — see requireRecordableKeyScope. Accepting
	// it was the defect: the credential keeps Read on whole topics while the row starts
	// announcing that the subscriber sees only one prefix, so the registry states a boundary
	// that does not exist and every reader of it is misled. Clearing the prefix is always
	// allowed, whatever the row holds, because clearing narrows nothing.
	PartitionKeyPrefix *string

	// WebhookURL replaces the recorded legacy endpoint when non-nil; a present empty
	// string clears it. Any non-empty value must be HTTPS and must not address an internal
	// destination — the persistence boundary enforces both.
	WebhookURL *string
}

// SubscriberCredential is the result of one credential issuance: everything a subscriber
// needs to start consuming, and the ONLY place in this package where a plaintext SASL
// secret exists.
//
// # The secret is returned by issuance and lives only in this value
//
// The plaintext is held in an UNEXPORTED field of the redacting SubscriberSecret type and is
// reachable through one accessor, Password, which reads this in-memory value and may be called
// as often as the holder likes while the value lives. What does not exist is a route to obtain
// the secret afterwards: nothing persists it, no reader returns it, and blnk.event_subscribers
// has no column capable of holding it. A subscriber that loses its password can only be issued
// a new one.
//
// # Why every rendering is redacted
//
// Format, String and GoString all render a redacted summary, so `%v`, `%+v`, `%#v`, `%s` and
// `%q` — the shapes a log line, a test failure message or a panic trace actually take — cannot
// print the secret. fmt consults a Formatter before descending into fields, which is what makes
// that total rather than verb-by-verb. encoding/json is safe for the same structural reason: it
// skips unexported fields, so no MarshalJSON is needed here and none should be added.
//
// LogFields is the safe way to log an issuance; it is composed of non-secret values only.
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

	// Fingerprint is the short, non-sensitive form of the stored credential reference. It
	// is what a client uses to tell one issuance from another and is not a secret; it
	// cannot be authenticated with and the reference cannot be recovered from it.
	Fingerprint string

	// Replaced reports that the principal already held a SCRAM credential and this
	// issuance replaced it. An operator reading it knows an existing consumer's credential
	// has just stopped working.
	Replaced bool

	// password is the plaintext, held in the redacting type and reachable only through
	// Password. NEVER add an exported field carrying it, and never copy it into a log
	// field, an error message, a trace attribute or a metric label.
	password SubscriberSecret
}

// Password reveals the generated secret, and is the ONE place it can be read from.
//
// It exists because the credential endpoint has to put the secret in its response body; there
// is no other legitimate caller. It reads the value out of this in-memory result — it does NOT
// read anything back from storage, and it cannot, because nothing was stored. Repeated calls
// return the same value for as long as the result lives; the guarantee is non-persistence and
// non-retrievability, not a single read.
//
// Hand the value to the response and let it go. Do not log it, do not put it in an error, do
// not attach it to a span, and do not keep it.
func (c SubscriberCredential) Password() string {
	return c.password.reveal()
}

// PasswordLength reports how long the generated secret is, without revealing it.
//
// The length is the one property of a secret that is safe to publish, and it is what lets a
// test assert that a credential of the expected strength was generated without the value
// appearing in the test's output on failure.
//
// Returns:
//   - int: the secret's length in bytes; zero when none is held.
func (c SubscriberCredential) PasswordLength() int {
	return c.password.Len()
}

// LogFields is the safe projection of an issuance for structured logging.
//
// Every value here is non-secret by construction: the fingerprint is a 48-bit prefix of a
// keyed digest, the topics and the group are already public to the subscriber, and the
// password appears only as its LENGTH. Logging through this rather than the struct is what
// keeps an issuance auditable without making it disclosable.
func (c SubscriberCredential) LogFields() logrus.Fields {
	return logrus.Fields{
		"subscriber":              sanitizeLogValue(c.SubscriberID, maxLoggedFilterLength),
		"principal":               sanitizeLogValue(c.Username, maxLoggedFilterLength),
		"consumer_group":          sanitizeLogValue(c.ConsumerGroupID, maxLoggedFilterLength),
		"authorized_topic_count":  len(c.AuthorizedTopics),
		"mechanism":               c.Mechanism,
		"credential_fingerprint":  c.Fingerprint,
		"credential_replaced":     c.Replaced,
		"credential_secret_bytes": c.password.Len(),
		"issued_at":               c.IssuedAt.UTC().Format(time.RFC3339Nano),
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
// One instance is enough for a process. It is safe for concurrent USE once configured: the
// store, the clock, the generator and the budget are read-only after construction, and the
// lazily resolved administrative client is guarded by a mutex. The fluent configurators are
// NOT synchronised, so configuration must be complete before the service is shared — they must
// not race an operation in flight.
//
// The administrative client may be INJECTED or RESOLVED. Inject the process's own client —
// as the metrics collector's caller does in cmd/server.go — when one already exists, so
// issuance reuses its connection pool and SASL session. Leave it nil and the service builds
// one from live configuration on first use and closes it in Close, which is the right trade
// for a rare, operator-triggered request: one dial and one SASL handshake, in exchange for
// the handler not having to own a client's lifetime. It is the wrong trade inside a loop,
// and nothing here loops.
//
// This service is deliberately a THIN FACADE, in the shape of apikey.go: it derives the
// values that must be derived, validates what must be validated, and delegates every
// statement to the datasource and every broker operation to event_admin.go. It contains no
// SQL and no Kafka protocol code at all.
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
	//
	// It is a FUNCTION FIELD rather than a direct call so that the failure path is
	// reachable in a test. It must never be replaced with anything that returns a fixed
	// value outside a test: a predictable subscriber secret is an open topic.
	generatePassword func() (string, error)
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

	// A typed nil pointer placed in an interface field is a NON-NIL interface holding a nil
	// pointer, which passes every nil guard and then panics on first use. The guard here is
	// against the plain nil interface only; callers that hold a possibly-nil *KafkaAdminClient
	// must do what cmd/server.go does and assign the interface only on success.
	if admin != nil {
		service.admin = admin
		service.ownsAdmin = false
	}

	return service
}

// WithIssuanceBudget overrides the wall-clock ceiling on one credential issuance.
//
// It follows the fluent configurator convention the outbox processors already use. A
// non-positive value RESTORES the default rather than disabling the bound, because an
// unbounded issuance is never the intent: the whole point of the budget is that a broker
// which accepts a connection and then stops answering cannot hold a request open.
//
// Production must not call this. It exists so a test can assert that the budget is enforced
// without waiting five seconds, and so an operator running against a deliberately slow
// staging broker can be given a longer, explicitly chosen ceiling.
// A non-positive budget restores SubscriberCredentialIssuanceBudget.
func (s *EventSubscriberService) WithIssuanceBudget(budget time.Duration) *EventSubscriberService {
	if budget <= 0 {
		budget = SubscriberCredentialIssuanceBudget
	}

	s.issuanceBudget = budget

	return s
}

// WithKafkaAdmin injects an already-built administrative client.
//
// The injected client is NOT owned by this service and is never closed by it, which is what
// makes it safe to hand over the process's shared client. Passing nil clears an injected
// client and returns the service to resolving one lazily.
//
// Parameters:
//   - admin subscriberPrincipalProvisioner: the client to use, or nil to resolve lazily.
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

// provisioner returns the administrative client, building one on first use when none was
// injected.
//
// # Why a failure here is ErrKafkaUnavailable and not an internal error
//
// Two things can go wrong and both are environmental rather than defects in this service:
// configuration may not be loaded, or the configured SASL credentials or TLS material may
// be unusable. Neither can be retried into success by the caller resending the same
// request, but both are resolved by an operator, and 503 is the status that says so. An
// unconfigured broker list is reported the same way by the operations themselves, through
// ErrKafkaAdminNotConfigured.
//
// Configuration is read through fetchConfiguration — the package's configuration seam,
// declared in event_sunset.go — rather than config.Fetch directly, so a test that swaps it
// sees consistent behaviour across every event file.
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

	cnf, err := fetchConfiguration()
	if err != nil {
		// LOGGED HERE AND BOUNDED, RETURNED WITHOUT THE CAUSE. A configuration failure can
		// quote the offending file content or a path inside the container, and NewAPIError
		// would both re-log it unsanitized and serialise it into the response. See
		// SubscriberErrorDetail.
		logrus.WithField("error", sanitizeLogValue(err.Error(), maxLoggedErrorLength)).Error(
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
		logrus.WithField("error", sanitizeLogValue(err.Error(), maxLoggedErrorLength)).Error(
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
// It is idempotent and nil-safe, and it NEVER closes an injected client: a client passed in
// belongs to whoever built it and usually outlives this service. That ownership rule is
// what makes Close safe to call from a handler's defer without knowing where the client
// came from.
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

// ---------------------------------------------------------------------------------------
// The derivations, and why each is derived rather than chosen
//
// A subscriber's KAFKA PRINCIPAL and CONSUMER GROUP NAMESPACE are the two names its access
// boundary is expressed in — the SCRAM credential is minted for the principal, and every ACL
// binding names the principal and the namespace — so whoever chooses those strings chooses the
// boundary. They are therefore DERIVED from the subscriber's immutable identifier and never
// accepted from a request. Both derivations are pure functions of it, which is what keeps a
// subscriber's consumer group and ACL grant intact across a credential rotation and lets an
// operator reconstruct them from a subscriber id alone:
//
//	principal        = "blnk-sub-" + <subscriber_id>
//	group namespace  = "blnk-sub-" + <subscriber_id> + "."       (the PREFIXED ACL resource)
//	default group    = "blnk-sub-" + <subscriber_id> + ".default"
//
// The trailing terminator is the disjointness guarantee: it cannot occur inside a canonical
// identifier, so "blnk-sub-abc." and "blnk-sub-abcd." can never overlap. Without it, a
// subscriber whose id is a leading substring of another's would reserve the other's namespace
// and could join its consumer groups.
//
// The composition itself lives in model/event.go, because the persistence layer's CHECK
// constraints have to agree with it byte for byte and cannot import this package.
// ---------------------------------------------------------------------------------------

// SubscriberKafkaPrincipal derives the SASL/SCRAM username for a subscriber id.
//
// Parameters:
//   - subscriberID string: the subscriber's business identifier.
//
// Returns:
//   - string: "blnk-sub-<subscriber_id>".
//   - error: a typed validation error when the identifier cannot be canonicalized, which
//     also means it cannot be provisioned at all.
func SubscriberKafkaPrincipal(subscriberID string) (string, error) {
	principal, err := model.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		return "", invalidSubscriberIdentifier(err)
	}

	return principal, nil
}

// SubscriberConsumerGroupID derives the consumer group a subscriber reads under by default.
//
// The value is a LEAF inside the namespace the subscriber's prefixed ACL reserves, so a
// subscriber using it is already inside its grant, and a subscriber wanting a second group —
// a replay group beside its live one, say — picks another leaf with no administrative round
// trip.
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

// SubscriberConsumerGroupNamespace derives the PREFIXED ACL resource name that reserves a
// subscriber's whole consumer-group namespace.
//
// This is the string that appears in the ACL binding, and it is deliberately NOT the group
// id: granting the group id literally would pin the subscriber to exactly one group, while
// granting the namespace with a prefixed pattern reserves everything beneath it and nothing
// beside it.
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

// SubscriberPartitionKeyPrefix reports the key-scoped constraint recorded for a subscriber,
// flattening the nullable column to a plain string.
//
// It is RECORDED, not derived, and that is not an omission. Every other value in the access
// model is derived from the subscriber's identity; this one cannot be. A Kafka message key on
// Blnk's topics is the outbox row's STORED PARTITION KEY — a ledger id when the payload yields
// one, otherwise a balance, identity, monitor or batch id, or the event type — so a prefix
// derived from the subscriber's own identifier would match no record ever produced, and a
// subscriber filtering on it would silently discard its entire stream.
//
// Every other value in the access model is derived from the subscriber's identity. This one
// cannot be, and must not be. Kafka message keys on Blnk's topics are LEDGER partition keys
// — the publisher keys each event by the aggregate's ledger so that one aggregate's events
// land on one partition — so a prefix derived from the subscriber's own identifier would
// match no record ever produced. The prefix is a statement about which ledgers a subscriber
// is authorised for, which only the caller registering it can make.
//
// # A NON-EMPTY RESULT MEANS THE SUBSCRIBER CANNOT BE PROVISIONED
//
// Kafka's authorizer has no message-key dimension: there is no ACL that restricts a consumer
// to a slice of a topic by key. A subscriber granted a topic can read every record on it
// regardless of this value. So a non-empty prefix records an authorization NARROWER than any
// credential this service can mint, and IssueSubscriberCredential refuses for such a row —
// see requireProvisionableKeyScope. The alternative, issuing anyway, means believing two
// subscribers on one topic cannot see each other's events, which is false, and handing out a
// credential that makes that belief look justified.
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

// invalidSubscriberIdentifier wraps an identifier failure in the typed validation error the
// API layer answers 400 with.
//
// It exists so that model.ErrInvalidSubscriberIdentifier — which is a plain error, because
// model cannot import apierror — reaches the response boundary as a client error rather than
// falling through to 500. The cause is carried in the details so the specific rule broken
// stays visible to the caller; the message names the consequence rather than the rule,
// because "cannot be used to derive a Kafka identity" is what the caller has to act on.
func invalidSubscriberIdentifier(cause error) error {
	return apierror.NewAPIError(
		apierror.ErrGenValidation,
		"The subscriber ID cannot be used to derive a Kafka identity",
		cause,
	)
}

// generateSubscriberPassword mints a SASL/SCRAM secret with crypto/rand.
//
// crypto/rand and NOT math/rand: this is a credential, and a predictable one is no credential
// at all. An error from the reader is returned rather than falling back to a weaker source,
// because a failed issuance is recoverable and a guessable password is not.
//
// Indices are drawn with rejection sampling rather than by taking a byte modulo the alphabet
// length, which would bias the low characters — 256 is not a multiple of 62 — and the bias is
// measurable rather than theoretical.
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
				// The CSPRNG failing is a system-level fault, not a subscriber problem. It
				// is reported verbatim because there is nothing sensitive in it: the error
				// describes the source, never a drawn value.
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
			// pathologically repetitive draw and logging its diagnosis would invite somebody
			// to log the value beside it.
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
// Every entry must be an exact member of event_topics.SubscriberGrantableTopics() — the
// subscriber-facing category topics under the configured prefix. The test is membership,
// never a prefix match: a prefix match would accept "blnk.transactions.something-else" and,
// with a caller-supplied prefix, very nearly anything.
//
// An EMPTY LIST IS VALID and means "authorised for nothing", which is the fail-closed default
// of a newly registered subscriber. A duplicate is rejected rather than folded, because a
// duplicate in a grant request is a caller error worth reporting.
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

		return apierror.NewAPIError(
			apierror.ErrGenValidation,
			"Authorized topics must be Blnk-owned subscriber-facing category topics",
			fmt.Errorf(
				"topic %q does not exist or is not grantable; the grantable topics are %s",
				sanitizeLogValue(topic, maxLoggedFilterLength),
				strings.Join(SubscriberGrantableTopics(), ", "),
			),
		)
	}

	return nil
}

// normalizeSubscriberName trims and requires the human label.
//
// NOT NULL does not stop an empty string, and a blank name defeats the reason the registry
// exists: it is the value an operator recognises a principal by months later, and no service
// can invent a meaningful one.
//
// Parameters:
//   - name string: the supplied label.
//
// Returns:
//   - string: the trimmed label.
//   - error: a typed validation error when it is blank.
func normalizeSubscriberName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", apierror.NewAPIError(
			apierror.ErrGenValidation,
			"A subscriber name is required",
			errors.New("event subscriber: name is blank, and an unnamed principal cannot be triaged"),
		)
	}

	return trimmed, nil
}

// maxSubscriberKeyScopeLength bounds the recorded key-scoped constraint.
//
// A partition key on Blnk's topics is a ledger partition key — a "<prefix>_<uuid>" string —
// so 256 characters is far more than any legitimate prefix of one needs while still refusing
// an unbounded value. It matches the bound api/model applies to the same field; the number is
// restated rather than imported because the API package depends on this one and not the other
// way round.
const maxSubscriberKeyScopeLength = 256

// normalizeSubscriberKeyScope validates and normalises the recorded key-scoped constraint.
//
// # Three inputs, three meanings
//
//   - NIL means omitted, and stays nil: no constraint was recorded.
//   - A value that is blank or whitespace-only means CLEAR, and becomes nil, so the column
//     holds NULL — "no key constraint" — rather than the empty string, which would read as
//     "constrained to the empty prefix". Those are opposite intents, the nullable column exists
//     to keep them distinguishable, and the difference decides whether a credential can be
//     issued at all.
//   - Anything else is kept verbatim after being checked.
//
// # It is still validated even though a value here blocks issuance
//
// A recorded constraint is a durable statement about who this subscriber is authorised to see,
// read by every later report and by whoever eventually decides how to satisfy it. So a
// malformed one is refused at the boundary rather than stored and puzzled over later: the
// refusal a caller gets for a control character is more useful than a row nobody can act on.
//
// # Why it is checked here and not only in the request DTO
//
// The DTO applies the same rules, and that covers HTTP. It does not cover a CLI caller, a
// migration or a fixture, all of which reach this service directly — and the row outlives any
// single request. Surrounding whitespace is REFUSED rather than trimmed for the reason a topic
// name is: a value differing from another only by whitespace is almost always a copy-paste
// artefact, and quietly rewriting it would store a constraint the caller did not ask for.
//
// Parameters:
//   - prefix *string: the supplied constraint. Nil is accepted.
//
// Returns:
//   - *string: the normalised value, or nil for omitted and cleared.
//   - error: a typed validation error naming the rule broken.
func normalizeSubscriberKeyScope(prefix *string) (*string, error) {
	if prefix == nil {
		return nil, nil
	}

	value := *prefix
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}

	if value != strings.TrimSpace(value) {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The partition key prefix must not have surrounding whitespace",
			fmt.Errorf(
				"event subscriber: partition key prefix %q has surrounding whitespace; a key prefix "+
					"differing from another only by whitespace filters a different set of records",
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

// requireProvisionableKeyScope refuses to provision a subscriber whose row records a
// key-scoped authorization Kafka cannot enforce. SEC-05.
//
// It guards ONE of the two orders in which the forbidden state can be reached — record the
// prefix, then ask for a credential. requireRecordableKeyScope guards the other, and both are
// needed: on its own, either can be walked around by approaching the state from the far side.
//
// # Why this is a refusal and not a warning
//
// Kafka's authorizer has no message-key dimension, so there is no binding, pattern type or
// operation that confines a consumer to the records whose key carries a given prefix. A row
// carrying such a prefix therefore records an authorization NARROWER THAN ANY CREDENTIAL THIS
// SERVICE CAN MINT, and there are only two honest responses to that: issue the wider credential
// and tell nobody, or refuse.
//
// Issuing was the previous behaviour, and it fails in the direction that matters. The registry
// row says the subscriber may see one ledger's records; the credential it hands out reads every
// record on every authorised topic. Anybody deciding tenancy from the registry — an operator, a
// migration report, a support engineer answering "can this subscriber see that ledger?" — gets
// the wrong answer, and nothing anywhere fails to correct them. Documenting the field as
// advisory did not fix that, because a comment is not what a person reads when they read a
// database row.
//
// So provisioning fails closed and says exactly what to do about it. Both remedies are real:
// clearing the prefix accepts whole-topic access explicitly, which is a decision somebody has
// now made rather than one the system made silently; narrowing authorized_topics is the
// enforceable form of the same intent whenever the ledgers in question map onto topics.
//
// # Why 409 rather than 400 or 503
//
// The caller sent no body — POST /subscribers/{id}/kafka-credentials has none — so nothing
// about the REQUEST is invalid, which rules out 400. Nothing is unavailable and a retry cannot
// succeed, which rules out 503. What is wrong is the current STATE of the resource being
// provisioned, which is what 409 means, and the message names the state and the two exits.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: a typed conflict when the row records an unenforceable key scope, otherwise nil.
func requireProvisionableKeyScope(subscriber *model.EventSubscriber) error {
	if subscriber == nil || !subscriber.KeyScopeUnenforceable() {
		return nil
	}

	return apierror.NewAPIError(
		// The TYPED code, not the generic conflict. Both resolve to 409, but a client
		// discriminates on the code, and this refusal has a specific remedy that
		// "CONFLICT" cannot express — see ErrSubscriberIsolationUnenforceable, whose
		// documentation describes exactly this refusal.
		apierror.ErrSubscriberIsolationUnenforceable,
		"This subscriber records a partition key prefix, which Kafka cannot enforce, so no "+
			"credential will be issued for it. Clear the partition key prefix to accept access to "+
			"whole topics, or narrow the subscriber's authorized topics, which is enforceable",
		fmt.Errorf(
			"event subscriber: subscriber %q records a partition key prefix; Kafka's authorizer has "+
				"no message-key dimension, so any credential issued would grant every record on every "+
				"authorised topic and the registry would describe a narrower boundary than exists",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		),
	)
}

// requireRecordableKeyScope refuses to RECORD a key scope on a subscriber that already holds a
// credential. It is the other half of SEC-05, and without it the first half is decorative.
//
// # The hole this closes
//
// requireProvisionableKeyScope guards the order "record a key prefix, then ask for a
// credential". It says nothing about the reverse order, and the reverse order is one ordinary
// API call: register, issue — which succeeds, because no prefix is recorded — and then update
// the row with a prefix. That update used to be accepted silently. The result is precisely the
// state the refusal exists to prevent, reached by a path that never touches the refusal: a live
// SASL credential with Read on whole topics, under a registry row announcing that the
// subscriber may see only the records whose key carries one prefix. Nothing failed, nothing was
// logged, and the credential kept working — so anybody answering "can this subscriber see that
// ledger?" from the registry answered it wrongly, which is the entire defect SEC-05 was written
// about.
//
// So the two guards are now SYMMETRIC. Issuance refuses "prefix recorded, credential about to
// exist"; this refuses "credential exists, prefix about to be recorded". Between them the state
// is unreachable through the service, and blnk.event_subscribers'
// event_subscribers_key_scope_chk makes it unrepresentable in the database as well — three
// independent barriers, because the state is one a reader of the registry cannot detect.
//
// # Why the check is on the RESULTING row and not on the request
//
// A prefix on a row that holds NO credential is legitimate and must stay legitimate: it is the
// state a caller reaches by registering with a prefix, and requireProvisionableKeyScope handles
// it — at issuance, with a message naming both exits. Refusing every update that leaves a
// prefix in place would break unrelated edits to such a row, including the rename an operator
// makes while deciding what to do about it. What must be refused is the COMBINATION, so the
// predicate reads the row as it would be written.
//
// # Why refuse rather than revoke
//
// Revoking the credential as a side effect of accepting the prefix would also keep the registry
// honest, and it was considered. It destroys a working credential — the subscriber stops
// consuming — in response to a request that said nothing about revocation, and the secret
// cannot be recovered: somebody must reissue and redistribute it. A refusal costs the caller one
// decision and takes nothing away. Whoever does want the credential gone can revoke it and then
// record the prefix, which is the same outcome asked for explicitly.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as it WOULD be written, with the requested
//     changes already applied.
//
// Returns:
//   - error: a typed conflict when the row would record a key scope while holding a
//     credential, otherwise nil.
func requireRecordableKeyScope(subscriber *model.EventSubscriber) error {
	if subscriber == nil || !subscriber.KeyScopeUnenforceable() || !subscriber.IsProvisioned() {
		return nil
	}

	return apierror.NewAPIError(
		// The same typed code issuance refuses with. One state, one code: a client that
		// handles SUBSCRIBER_ISOLATION_UNENFORCEABLE from the credential endpoint needs no
		// second case to handle it here, and the remedies are the same two plus revocation.
		apierror.ErrSubscriberIsolationUnenforceable,
		"This subscriber already holds a Kafka credential, and Kafka cannot enforce a partition "+
			"key prefix, so recording one would describe a narrower boundary than the credential "+
			"actually has. Revoke the credential first if the prefix is what you want, or narrow "+
			"the subscriber's authorized topics, which is enforceable",
		fmt.Errorf(
			"event subscriber: subscriber %q holds a credential issued at %s; recording a partition "+
				"key prefix on it would leave a live principal with Read on whole topics under a row "+
				"claiming key-scoped access, which is the state SEC-05 refuses at issuance",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			subscriberCredentialIssuedAt(subscriber),
		),
	)
}

// subscriberCredentialIssuedAt renders a subscriber's issuance instant for a diagnostic, or
// "an unrecorded time" when the row carries none.
//
// The schema's event_subscribers_credential_pair_chk keeps the reference and the instant
// written or cleared together, so a provisioned row always has one — but this runs inside an
// error path, and dereferencing a pointer that "cannot" be nil is how an error path becomes a
// panic in a process that moves money.
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

// requireGrantedTopics refuses to mint a credential for a subscriber authorised for nothing.
//
// # Why an empty grant is refused at issuance but accepted everywhere else
//
// An empty authorized_topics list is a real registry state and stays one. It is the fail-closed
// default of a newly registered subscriber, and setting the list back to empty is the only way
// to say "authorised for nothing" about a row that currently holds topics — a narrowing
// UpdateSubscriber must be able to express, and does, by withdrawing every binding at the
// broker. Neither registration nor update is touched here.
//
// Issuing against that state is a different matter. The credential is minted, the SASL principal
// authenticates, and it holds no topic binding whatsoever: it can list nothing and read nothing.
// The RESPONSE, though, is shaped exactly like a working one — a secret returned once, a broker
// endpoint, a consumer group — so what the caller receives is indistinguishable at a glance from
// access. They hand it to a consumer, the consumer sees an empty topic list, and the silence is
// diagnosed as a delivery problem in the pipeline rather than as an authorization the registry
// never granted. Meanwhile the broker holds a live principal that no ACL describes and that
// somebody must remember to revoke.
//
// The admin layer already warns when it provisions an empty grant, and that warning stays: it
// covers the paths that legitimately reconcile a row with no topics. A warning in a log is the
// wrong instrument for a request that can simply be answered, which is what this does — naming
// the missing grant and the one step that fixes it.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: a typed conflict when the subscriber has no authorised topic, otherwise nil.
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

// requireActiveSubscriber refuses to provision a subscriber that is being deregistered.
//
// A row carrying the revocation tombstone is on its way out: its broker-side access is being
// taken away and may still be live. Minting a credential for it would re-arm a principal
// mid-removal, and the deregistration that is already in flight would then delete the registry
// row that records the credential just issued — leaving exactly the orphaned live principal the
// tombstone exists to prevent.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: a typed conflict when the subscriber is being deregistered, otherwise nil.
func requireActiveSubscriber(subscriber *model.EventSubscriber) error {
	if subscriber == nil || !subscriber.IsRevocationPending() {
		return nil
	}

	return apierror.NewAPIError(
		apierror.ErrConflict,
		"This subscriber is being deregistered, so no credential will be issued for it",
		fmt.Errorf(
			"event subscriber: subscriber %q carries a revocation tombstone from %s; complete or "+
				"reverse its deregistration before issuing credentials",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			subscriber.RevocationPendingAt.UTC().Format(time.RFC3339),
		),
	)
}

// SubscriberProvisioningFenceLease is how long an issuance or revocation holds its fence.
//
// It is three times the issuance budget, and the ratio is the requirement: long enough that a
// legitimate operation cannot lose its own claim while it is still working — the fence must
// outlive the budget it protects, plus the cleanup that follows a failure — and short enough
// that a process killed while holding one does not fence the subscriber for materially longer
// than an operator would wait before retrying.
const SubscriberProvisioningFenceLease = 3 * SubscriberCredentialIssuanceBudget

// fenceSubscriber claims a subscriber for one operation and returns the release function.
//
// # What the fence prevents, precisely
//
// Kafka stores ONE SCRAM credential per principal, so two overlapping issuances both write a
// credential and the second replaces the first. The broker then holds one password while the
// registry may hold the reference derived from the other, and the caller holding the recorded
// one cannot authenticate — with no way to discover it, because its request returned 200 with a
// password in it. RecordSubscriberCredentialIfUnchanged detects the DATABASE half of that race;
// it cannot decide which password the BROKER kept, because that is settled by whichever call
// reached the broker last, independently of who won the write.
//
// So nothing touches the broker for a subscriber without holding its claim, and the second
// caller is refused with a conflict BEFORE it generates a secret. A refused issuance costs a
// caller one retry; an interleaved one costs it a credential that does not work.
//
// Revocation and deregistration fence too, so an issuance cannot interleave with the removal of
// the very credential it is writing.
//
// # The release runs on a FRESH context
//
// A claim taken by an issuance whose budget then expired must still be released, or the
// subscriber stays fenced until the lease runs out — turning one slow broker call into a
// minute of refused retries. So the release is bounded by its own budget rather than by the
// caller's remaining time, exactly as every other cleanup here is. A release that finds the
// claim gone is logged, not returned: the operation it belonged to has already finished, and
// the useful record is that the fence was lost, not a second error for the caller to read.
//
// Parameters:
//   - ctx context.Context: the operation's context. Its VALUES are used for the release.
//   - store eventSubscriberStore: the registry.
//   - subscriberID string: the subscriber to fence.
//
// Returns:
//   - func(): releases the claim. Never nil, so the caller can defer it unconditionally, and
//     idempotent.
//   - error: a typed conflict when another operation holds the claim, a typed not-found when
//     the subscriber does not exist, or the repository's error.
func fenceSubscriber(
	ctx context.Context,
	store eventSubscriberStore,
	subscriberID string,
) (func(), error) {
	token, err := store.ClaimSubscriberForProvisioning(ctx, subscriberID, SubscriberProvisioningFenceLease)
	if err != nil {
		return func() {}, err
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			release, cancel := subscriberCleanupContext(ctx)
			defer cancel()

			if releaseErr := store.ReleaseSubscriberProvisioningFence(release, subscriberID, token); releaseErr != nil {
				logrus.WithError(releaseErr).WithField(
					"subscriber", sanitizeLogValue(subscriberID, maxLoggedFilterLength),
				).Warn(
					"event subscriber: releasing the provisioning fence failed; the subscriber stays " +
						"fenced until its claim lease expires, after which the next attempt proceeds",
				)
			}
		})
	}, nil
}

// subscriberCleanupBudget bounds a compensating write after an operation has already failed.
//
// It is the issuance budget over again, which is the right size for the one or two round trips
// a cleanup makes, and it is deliberately its OWN budget rather than the remainder of the
// caller's — see subscriberCleanupContext.
const subscriberCleanupBudget = SubscriberCredentialIssuanceBudget

// subscriberCleanupContext derives the context a compensating write runs on. CLEAN-01.
//
// # The failure this exists for
//
// Every cleanup path in this file — revoking a credential the registry could not record,
// clearing a credential reference that no longer describes anything, releasing the provisioning
// fence — used to run on the ISSUANCE context. That context carries the 5-second budget, and
// its expiry is one of the commonest reasons issuance fails at all. So the cleanups were
// attempted with an already-cancelled context, returned immediately, and left exactly the
// residue they exist to remove: a live credential nothing records, or a registry claiming
// access the broker no longer grants.
//
// A fresh context detached from the caller's cancellation fixes that, and it is BOUNDED rather
// than merely detached for the same reason the relay's bookkeeping context is: finishing what
// is owed must not become blocking indefinitely on a system that has gone away.
//
// The caller's VALUES are kept, so the cleanup appears under the span that caused it.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values only.
//
// Returns:
//   - context.Context: a fresh context bounded by subscriberCleanupBudget.
//   - context.CancelFunc: must be called, conventionally by defer.
func subscriberCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), subscriberCleanupBudget)
}

// requireSubscriberIdentifier rejects a blank subscriber id before anything is spent on it.
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
//
// Every statement is the datasource's. This layer generates the identifier, derives the
// boundary, validates the grant against the live topic catalogue, and delegates — which is
// the shape apikey.go established for a service over a repository, and the restraint is the
// point rather than an economy: a second place that writes the registry is a second place
// that can write a row the derivation would have refused.
// ---------------------------------------------------------------------------------------

// RegisterSubscriber records a new subscriber and returns the stored row.
//
// # What is derived and what is supplied
//
// The KAFKA PRINCIPAL and the CONSUMER GROUP are DERIVED from the subscriber id and are not
// caller-supplied: they are the two values isolation rests on, so a caller cannot choose a
// principal that collides with another subscriber's, and the derivation is a pure function of
// an immutable id. The name, the authorised topics, the advisory key prefix and the legacy
// webhook URL are the caller's.
//
// An id is generated only when the caller supplies none. A supplied id is validated as given
// — surrounding whitespace is a rejection rather than something to fold away — because the id
// derives the principal and the group namespace, so silently rewriting it would make the
// stored subscriber differ from the one the caller asked for.
//
// The row is fail-closed: with no authorised topics the subscriber can read nothing until it
// is granted something and credentials are issued.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: a typed validation error, a typed conflict when the id or principal is taken, or
//     the repository's error.
func (s *EventSubscriberService) RegisterSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	// An ABSENT identifier is generated rather than refused, which is the house convention
	// every other Blnk resource follows — ledgers, balances, identities and API keys all mint
	// their own business key when the caller supplies none. The generated form is
	// "sub_<uuid>", so it satisfies the canonical identifier rule the schema's CHECK
	// constraints and the principal derivation share, and it is recognisable on sight in a
	// broker ACL listing.
	//
	// It is worth stating because the alternative reads as safer and is not: a caller who
	// omits the field has expressed no preference, and refusing would only push identifier
	// invention out to every client, where nothing checks it against that rule. A BLANK-BUT-
	// PRESENT value is a different matter and is still refused everywhere it counts — see
	// requireSubscriberIdentifier, which guards every operation that acts on an existing row.
	subscriberID := strings.TrimSpace(registration.SubscriberID)
	if subscriberID == "" {
		subscriberID = model.GenerateSubscriberID()
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
		"subscriber":             sanitizeLogValue(stored.SubscriberID, maxLoggedFilterLength),
		"principal":              sanitizeLogValue(stored.KafkaPrincipal, maxLoggedFilterLength),
		"consumer_group":         sanitizeLogValue(stored.ConsumerGroupID, maxLoggedFilterLength),
		"authorized_topic_count": len(stored.AuthorizedTopics),
		"legacy_webhook":         stored.WebhookURL != nil,
	}).Info("event subscriber: registered; no credential has been issued yet")

	return stored, nil
}

// GetSubscriber reads one subscriber by its business key.
//
// A missing subscriber is reported as apierror.ErrSubscriberNotFound — 404 — and never as a
// bare sql.ErrNoRows: the repository maps it, and this method propagates the typed code
// unchanged so the API layer answers from the domain meaning rather than re-deriving one.
//
// The returned row carries the credential REFERENCE, which is not a secret and cannot be
// authenticated with. It carries no password, because none is stored.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation error
//     for a blank id, or the repository's error.
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
// oversized one is capped, so a malformed page request degrades to a cheap query rather than
// a full table scan. They are not re-implemented here, because two independent sets of
// bounds is one more than can be kept in step.
//
// No entry carries a secret. The credential reference on a row is a keyed digest; the API
// layer reduces it further to a fingerprint through api/model.NewSubscriberResponse.
// A non-positive limit selects the repository default and a negative offset is clamped to zero.
func (s *EventSubscriberService) ListSubscribers(
	ctx context.Context,
	limit, offset int,
) ([]model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

	return store.ListEventSubscribers(ctx, limit, offset)
}

// UpdateSubscriber applies the mutable subset of a subscriber and returns the stored row.
//
// # Read, apply, write — and why the read is not optional
//
// The repository's update writes every mutable column, so a caller assembling the row itself
// would have to know each column's current value or it would blank the ones it left out. The
// current row is therefore read first and the requested changes applied to it, which is what
// makes an omitted field mean "leave as stored" rather than "set to empty".
//
// # AUTH-02: it changes the BOUNDARY, not only the record of it
//
// An authorization change is applied at the broker as part of the update, in three steps
// around the write, and the ORDER is the whole design:
//
//  1. PRUNE at the broker. Every Blnk-owned binding the new authorization no longer implies is
//     deleted first, so a NARROWING is in force before the registry claims it is.
//  2. PERSIST the row.
//  3. GRANT at the broker. The bindings the new authorization adds are created last, so a
//     WIDENING reaches the broker only once the registry records it.
//
// There is no transaction spanning Blnk and Kafka, so some ordering will be observable on a
// partial failure; this one makes every partial failure fail-CLOSED. The subscriber ends with
// less access than the registry records, never more, and re-running the update completes it.
//
// The alternative — updating the row and leaving the broker alone — was the previous behaviour
// and it fails open. ACL creation is additive, so a topic removed from the list kept its Read
// and Describe bindings until somebody revoked the entire subscriber: the registry said the
// access was gone, the subscriber went on consuming, and nothing failed to say otherwise.
// Revocation could not clean it up either, because it derives the bindings to delete from the
// current row — the row that no longer names the topic.
//
// The update is FENCED per subscriber, so it cannot interleave with an issuance that is
// writing the very bindings it is reconciling.
//
// A deployment with no broker configured skips both broker steps and updates the registry
// alone, exactly as registration and listing do.
//
// The principal, the consumer group, the credential record and the migration instant are all
// unchangeable through this call, by construction — SubscriberUpdate has no field for any of
// them.
//
// Parameters:
//   - ctx context.Context: cancels the reconciliation, the read and the write.
//   - subscriberID string: the business key of the row to update.
//   - changes SubscriberUpdate: the fields to apply. A wholly empty update is legitimate and
//     rewrites the row with its own values, which is a harmless no-op rather than an error.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation error
//     for a blank name or an ungrantable topic, ErrSubscriberProvisioningFailed when the
//     broker-side reconciliation did not complete, or the repository's error.
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
	releaseFence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer releaseFence()

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return nil, err
	}

	if changes.Name != nil {
		name, nameErr := normalizeSubscriberName(*changes.Name)
		if nameErr != nil {
			return nil, nameErr
		}

		subscriber.Name = name
	}

	// Non-nil replaces the whole set, INCLUDING when it is non-nil and empty: revoking every
	// topic grant is a legitimate instruction, and it is the only way to express "authorised
	// for nothing" on a row that currently holds topics.
	if changes.AuthorizedTopics != nil {
		if grantErr := validateSubscriberGrant(changes.AuthorizedTopics); grantErr != nil {
			return nil, grantErr
		}

		subscriber.AuthorizedTopics = changes.AuthorizedTopics
	}

	// A present empty string CLEARS the recorded key scope, which is why the column is
	// nullable and why this cannot be a plain string: NULL means "no key constraint" while the
	// empty string would mean "constrained to the empty prefix", and those are opposite
	// intents. normalizeSubscriberKeyScope owns that three-way mapping so registration and
	// update cannot disagree about it — and clearing it here is what makes an unprovisionable
	// subscriber provisionable again.
	if changes.PartitionKeyPrefix != nil {
		keyPrefix, prefixErr := normalizeSubscriberKeyScope(changes.PartitionKeyPrefix)
		if prefixErr != nil {
			return nil, prefixErr
		}

		subscriber.PartitionKeyPrefix = keyPrefix
	}

	// Same three-way handling for the legacy URL. Clearing it is how a migrated subscriber's
	// dual-run artefact is removed one row at a time; PurgeMigratedWebhookURLs does it in
	// bulk once a retention period has elapsed.
	if changes.WebhookURL != nil {
		if strings.TrimSpace(*changes.WebhookURL) == "" {
			subscriber.WebhookURL = nil
		} else {
			webhookURL := *changes.WebhookURL
			subscriber.WebhookURL = &webhookURL
		}
	}

	// SEC-05, THE OTHER HALF: refuse to record a key scope on a row that already holds a
	// credential.
	//
	// Checked on the row AS IT WOULD BE WRITTEN and before anything reaches the broker or the
	// database, so a refusal changes nothing at all. Issuance refuses the same combination
	// approached from the other direction; between them, "prefix recorded AND credential live"
	// is unreachable through this service, and the schema's key-scope CHECK makes it
	// unrepresentable even to a caller that bypasses the service.
	if err := requireRecordableKeyScope(subscriber); err != nil {
		return nil, err
	}

	// STEP 1 — PRUNE. Whatever the new authorization no longer implies is removed at the broker
	// BEFORE the row records the narrowing, so a failure of the write below leaves the
	// subscriber with less access than the registry claims rather than more.
	pruned, err := s.pruneBrokerAccess(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	// STEP 2 — PERSIST.
	if err := store.UpdateEventSubscriber(ctx, subscriber); err != nil {
		return nil, err
	}

	// STEP 3 — GRANT. Whatever the new authorization adds reaches the broker only now that the
	// registry records it, so a failure here is again fail-closed. The error is returned, since
	// a caller told the update succeeded would believe a grant exists that does not.
	granted, err := s.grantBrokerAccess(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber":             sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
		"legacy_webhook":         subscriber.WebhookURL != nil,
		"acl_bindings_removed":   pruned,
		"acl_bindings_created":   granted,
	}).Info(
		"event subscriber: registry row updated and its broker-side grant reconciled to match",
	)

	return subscriber, nil
}

// pruneBrokerAccess removes the broker-side grants a subscriber's new authorization no longer
// implies, and reports how many it removed.
//
// It is the first of UpdateSubscriber's three steps. A deployment with no broker configured has
// no broker-side grant, so it is a no-op there; anything else is reported as a provisioning
// failure, because an update that could not narrow the boundary must not be reported as having
// narrowed it.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the row carrying the NEW authorization.
//
// Returns:
//   - int: how many bindings were removed.
//   - error: ErrSubscriberProvisioningFailed when the broker refused, or the admin client's
//     construction error.
func (s *EventSubscriberService) pruneBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		return 0, nil
	}

	report, err := admin.PruneSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
			// SANITIZED AND BOUNDED, not logrus.WithError. A refused administrative request
			// comes back from the Kafka client with the whole request appended to it —
			// hundreds of lines carrying broker addresses, listener names and every field of
			// the call — and an unbounded value with newlines in it can also forge log
			// structure. sanitizeLogValue keeps the broker's own words, which are what an
			// operator needs, and drops the dump.
			"error": sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		}).Error(
			"event subscriber: the obsolete Kafka grants of this subscriber could not be removed, so " +
				"the registry was NOT updated; the subscriber keeps the access it has and the change " +
				"can be retried",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"Failed to remove the subscriber's obsolete Kafka grants, so its authorization was not changed",
			// THE BOUNDED DETAIL, never the cause. NewAPIError does two things with what it
			// is given: it re-logs it through logrus unsanitized, and it serialises it into
			// the response body's `details` member. Passing the wrapped broker error here
			// therefore undid the sanitizing done immediately above AND published whatever
			// the client chose to put in that error. Retryable: the registry was not
			// touched, so repeating the update starts from the state this attempt found.
			NewSubscriberErrorDetail(
				"Removing the subscriber's obsolete Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Removed, nil
}

// grantBrokerAccess creates the broker-side grants a subscriber's authorization implies, and
// reports how many it created.
//
// It is the last of UpdateSubscriber's three steps, and it runs AFTER the row is persisted so a
// widening cannot precede the record of it. A failure here leaves the registry recording more
// access than the broker grants, which is the safe direction and self-heals: re-running the
// update, or issuing credentials, creates the missing bindings.
//
// Parameters:
//   - ctx context.Context: cancels the round trips.
//   - subscriber *model.EventSubscriber: the persisted row.
//
// Returns:
//   - int: how many bindings were created.
//   - error: ErrSubscriberProvisioningFailed when the broker refused, or the admin client's
//     construction error.
func (s *EventSubscriberService) grantBrokerAccess(
	ctx context.Context,
	subscriber *model.EventSubscriber,
) (int, error) {
	admin, err := s.provisioner()
	if err != nil {
		return 0, err
	}

	if !admin.IsConfigured() {
		return 0, nil
	}

	report, err := admin.GrantSubscriberAccess(ctx, subscriber)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error": sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		}).Error(
			"event subscriber: the registry was updated but the subscriber's new Kafka grants could " +
				"not be created, so it currently has LESS access than the registry records; re-run the " +
				"update or issue credentials to complete it",
		)

		return 0, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber was updated but its new Kafka grants could not be created",
			// The bounded detail, never the cause — see pruneBrokerAccess. Retryable, and
			// safely so: the registry already records the intended authorization, and both
			// re-running the update and issuing credentials create the missing bindings.
			NewSubscriberErrorDetail(
				"Creating the subscriber's new Kafka grants failed", subscriber.SubscriberID, true,
			),
		)
	}

	return report.Created, nil
}

// subscriberFacingBrokers resolves the bootstrap list to report to a subscriber, or refuses.
//
// # Why this is not admin.Brokers()
//
// admin.Brokers() is what BLNK dials. Inside a deployment those addresses are internal —
// "kafka:9092" on a compose network, a ClusterIP or headless Service in Kubernetes — and they
// do not resolve for a subscriber outside it. Kafka compounds the problem rather than
// tolerating it: a broker answers every client with the ADVERTISED address of the listener the
// connection arrived on, so even an externally reachable bootstrap address hands back internal
// ones for the actual partition leaders. Only an operator knows the externally advertised
// list, which is why it is configuration (KAFKA_SUBSCRIBER_BROKERS) and not a derivation.
//
// # Why an absent list is a refusal rather than a fallback
//
// Falling back to the internal list returns 200 with an endpoint the subscriber cannot use.
// The credential is real, the topics are real, and the one field that decides whether any of
// it works is wrong — so the failure appears as a connection timeout in the subscriber's logs,
// nowhere near this request, with a secret that is not recoverable and must be reissued to
// diagnose. Refusing costs an operator one variable and makes the requirement explicit; the
// fallback costs a subscriber a day. A deployment whose subscribers genuinely are in-cluster
// sets the variable to the same value as KAFKA_BROKERS.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the subscriber being provisioned, named in the error
//     so an operator knows which request was refused. May be nil.
//
// Returns:
//   - []string: the subscriber-facing bootstrap list, never empty on success.
//   - error: ErrSubscriberBrokersNotConfigured (503) when no list is configured.
func (s *EventSubscriberService) subscriberFacingBrokers(
	subscriber *model.EventSubscriber,
) ([]string, error) {
	// Read through fetchConfiguration — the package's configuration seam, declared in
	// event_sunset.go — for the same reason provisioner does: a test that swaps it sees
	// consistent behaviour across every event file. A configuration that cannot be read is
	// treated as "not configured", which refuses rather than falling back.
	if cnf, err := fetchConfiguration(); err == nil && cnf != nil {
		if brokers, configured := cnf.Kafka.SubscriberFacingBrokers(); configured {
			return brokers, nil
		}
	}

	identifier := ""
	if subscriber != nil {
		identifier = subscriber.SubscriberID
	}

	logrus.WithField("subscriber", sanitizeLogValue(identifier, maxLoggedFilterLength)).Error(
		"event subscriber: KAFKA_SUBSCRIBER_BROKERS is not configured, so no credential was " +
			"issued; set it to the externally advertised broker addresses subscribers connect to " +
			"(the same value as KAFKA_BROKERS when subscribers run inside the deployment)",
	)

	return nil, apierror.NewAPIError(
		apierror.ErrSubscriberBrokersNotConfigured,
		// The VARIABLE IS NAMED IN THE MESSAGE, not only in the detail and the log. This is
		// the one string that reaches the operator running the request, and "no broker list is
		// configured" without the key is a message that cannot be acted on. A configuration
		// key name discloses nothing: it is documented in .env.example and the manifests.
		"KAFKA_SUBSCRIBER_BROKERS is not configured, so no subscriber-facing Kafka broker list "+
			"is available and credentials cannot be issued. Set it to the externally advertised "+
			"broker addresses subscribers connect to",
		NewSubscriberErrorDetail(
			"KAFKA_SUBSCRIBER_BROKERS is not configured", identifier,
			// Retryable: nothing was written, and the request succeeds unchanged once the
			// variable is set.
			true,
		),
	)
}

// DeregisterSubscriber ends a subscriber's access at the broker and then removes it from the
// registry.
//
// # AUTH-03: REVOKE FIRST, DELETE ONLY ONCE THE REVOCATION IS CONFIRMED
//
// The order is the whole of this function, and the previous order was wrong. It used to DELETE
// the row and revoke afterwards. When the revocation then failed, the principal kept
// authenticating and kept reading — and the only record of WHICH principal that was had just
// been destroyed. The residue was live broker access that nothing in Blnk could see, and the
// error message plus a log line were all an operator had to work from.
//
// The sequence is now four steps, and each one exists because of the failure it prevents:
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
// A failure at step 3 leaves the tombstoned row in place and returns an error. That row is a
// DURABLE TO-DO ITEM rather than a lost one: it names the principal, it says how long the
// revocation has been outstanding, and RETRYING THE DEREGISTRATION FINISHES THE JOB — the
// tombstone is idempotent, revocation is idempotent, and the delete happens once the revocation
// finally succeeds.
//
// # A deployment with no Kafka deregisters cleanly
//
// When no broker is configured there is no broker-side state, so revocation is skipped and the
// row is deleted directly. That keeps the registry usable in a Kafka-less deployment, exactly as
// registration and listing are.
//
// Parameters:
//   - ctx context.Context: cancels the revocation and the removal.
//   - subscriberID string: the business key of the subscriber to remove.
//
// Returns:
//   - *model.EventSubscriber: the row that was removed on success, or the TOMBSTONED row when
//     revocation failed — which is the description of the state left behind and the row a retry
//     will find.
//   - error: ErrSubscriberNotFound when no such subscriber exists; a typed conflict when another
//     operation holds the subscriber's claim; ErrSubscriberProvisioningFailed when the
//     broker-side revocation did not complete and the row was therefore NOT deleted; the
//     repository's error otherwise.
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
	releaseFence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer releaseFence()

	// STEP 2 — TOMBSTONE. The row survives, carrying the principal and topics revocation needs
	// and saying plainly that this subscriber is on its way out.
	pending, err := store.MarkSubscriberRevocationPending(ctx, subscriberID, s.clock())
	if err != nil {
		return nil, err
	}

	admin, err := s.provisioner()
	if err != nil {
		// The row is TOMBSTONED, not deleted, so nothing is lost: it names the principal and a
		// retry finds it. The error is returned so the caller does not read the removal as
		// complete.
		logrus.WithError(err).WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(pending.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(pending.KafkaPrincipal, maxLoggedFilterLength),
		}).Error(
			"event subscriber: no Kafka administrative client could be built, so this subscriber's " +
				"access was NOT revoked and its registry row is kept, marked pending revocation; " +
				"retry the deregistration once the broker is reachable",
		)

		return pending, err
	}

	if !admin.IsConfigured() {
		// No broker, so there is no broker-side access and nothing to confirm. The row is
		// removed directly, which is what keeps the registry usable without Kafka.
		removed, takeErr := store.TakeEventSubscriber(ctx, subscriberID)
		if takeErr != nil {
			return pending, takeErr
		}

		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
		}).Info(
			"event subscriber: deregistered; no broker is configured, so there was no " +
				"broker-side access to revoke",
		)

		return removed, nil
	}

	// STEP 3 — REVOKE, using the row that still exists.
	if err := admin.RevokeSubscriber(ctx, pending); err != nil {
		logrus.WithFields(logrus.Fields{
			"subscriber":             sanitizeLogValue(pending.SubscriberID, maxLoggedFilterLength),
			"principal":              sanitizeLogValue(pending.KafkaPrincipal, maxLoggedFilterLength),
			"authorized_topic_count": len(pending.AuthorizedTopics),
			"revocation_pending_at":  subscriberPendingSince(pending),
			// Sanitized and bounded, for the reason given in pruneBrokerAccess.
			"error": sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		}).Error(
			"event subscriber: revoking this subscriber's Kafka access failed, so its registry row " +
				"was NOT deleted and is kept marked pending revocation; the principal named here may " +
				"still authenticate until the deregistration is retried",
		)

		return pending, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber's Kafka access could not be revoked, so it was not removed",
			// The bounded detail, matching RevokeSubscriberCredential's treatment of the same
			// failure: the principal and the broker's own words stay in the log line above,
			// and the caller receives the diagnosis plus the fact that a retry is safe.
			// Revocation is idempotent at the broker, so repeating it cannot make things
			// worse — and the tombstone on the row is what makes it findable.
			NewSubscriberErrorDetail(
				"Revoking the subscriber's Kafka access failed", pending.SubscriberID, true,
			),
		)
	}

	// STEP 4 — DELETE, now that the broker-side cleanup is confirmed.
	removed, err := store.TakeEventSubscriber(ctx, subscriberID)
	if err != nil {
		// The access is already gone, so this residue is the harmless direction: a registry row
		// describing a subscriber that can no longer authenticate. Retrying the deregistration
		// removes it, and the tombstone is what makes the row identifiable as needing that.
		logrus.WithError(err).WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(pending.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(pending.KafkaPrincipal, maxLoggedFilterLength),
		}).Error(
			"event subscriber: Kafka access was revoked but the registry row could not be deleted; " +
				"the subscriber can no longer authenticate and the row remains marked pending " +
				"revocation until the deregistration is retried",
		)

		return pending, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber": sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
		"principal":  sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
	}).Info("event subscriber: Kafka access revoked and the subscriber deregistered")

	return removed, nil
}

// subscriberPendingSince renders the revocation tombstone for a log field.
//
// It is what turns "this revocation failed" into "this revocation has been outstanding since
// 14:02", which is the difference between a line an operator can act on and one they cannot.
// A row with no tombstone reports the empty string rather than a zero instant, because a
// formatted year 1 would read as data.
func subscriberPendingSince(subscriber *model.EventSubscriber) string {
	if subscriber == nil || subscriber.RevocationPendingAt == nil {
		return ""
	}

	return subscriber.RevocationPendingAt.UTC().Format(time.RFC3339)
}

// ---------------------------------------------------------------------------------------
// Credential issuance
//
// This is the operation behind POST /subscribers/{id}/kafka-credentials, and the one place
// in Blnk where a SASL secret comes into existence.
// ---------------------------------------------------------------------------------------

// IssueSubscriberCredential mints a subscriber's SASL/SCRAM credential, binds its ACLs, and
// returns the secret to the caller.
//
// # The 5-second budget is enforced here, explicitly
//
// The whole operation runs under context.WithTimeout(ctx, SubscriberCredentialIssuanceBudget)
// and that derived context is passed into every step, so the ceiling covers the four possible
// broker round trips AND the database write together. The admin client's own per-request
// timeout would not do: four ten-second timeouts serialised is forty seconds while every
// individual call looks healthy. A caller whose own context expires sooner still wins, because
// WithTimeout only ever shortens.
//
// # Nothing is generated for a request that cannot succeed
//
// The broker configuration is checked BEFORE a secret is drawn: with no brokers the answer is
// ErrKafkaUnavailable (503), and no random value is generated, no reference derived and no row
// touched.
//
// # Re-issuance is supported, defined, and destructive to the previous secret
//
// A second call mints a NEW secret and reference and overwrites credential_reference and
// credential_issued_at. THE PREVIOUS SECRET STOPS WORKING IMMEDIATELY — Kafka stores one SCRAM
// credential per principal, so the upsert replaces it and a consumer still using the old one
// fails at its next handshake. That is reported through SubscriberCredential.Replaced and
// logged, never a silent no-op, and the old secret is never returned. The consumer group and
// the principal are unchanged, both being derived from the immutable subscriber id.
//
// # Partial failure, and what the result can and cannot promise
//
// event_admin.go compensates a failure that lands between the credential and its ACLs by
// revoking the credential before returning. THE OUTCOME OF THAT REVOCATION IS REPORTED, not
// assumed: Compensated is set only when the broker confirmed the deletion, and a revocation
// that itself failed leaves CredentialWritten true. This method never records an issuance on
// any error path, and it distinguishes compensated (broker confirmed clean, retry) from
// compensation-failed (a live principal is unaccounted for, logged at ERROR with the principal
// named). Removal of the attempted bindings is best-effort and deliberately not part of that
// flag, because inert bindings for a principal that no longer exists grant nothing. Both
// answer ErrSubscriberProvisioningFailed (503).
//
// # The secret goes to the caller and nowhere else
//
// Only a NON-REVERSIBLE reference (model.DeriveCredentialReference: HMAC-SHA-256 keyed by the
// principal) and the issuance instant are persisted. The plaintext is returned inside
// SubscriberCredential and is never written to the database, logged, placed in an error
// message, or attached to a span or a metric label; nothing can read it back afterwards.
//
// Parameters:
//   - ctx context.Context: the caller's context, wrapped in the issuance budget.
//   - subscriberID string: the business key of the subscriber to provision.
//
// Returns:
//   - SubscriberCredential: the connection details and the one-time secret. Zero-valued on
//     every error path.
//   - error: ErrSubscriberNotFound (404); ErrKafkaUnavailable (503) when no broker is
//     configured, the client cannot be built, or the budget expired;
//     ErrSubscriberProvisioningFailed (503) when the broker refused the credential or the
//     bindings; a typed conflict (409) when a concurrent issuance superseded this one; a
//     typed validation error (400) for an unusable subscriber id.
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

	// THE BUDGET. Everything below — the lookup, up to four broker round trips and the
	// issuance record — shares this one deadline.
	ctx, cancel := context.WithTimeout(ctx, s.budget())
	defer cancel()

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
	//
	// Two overlapping issuances cannot both be right: the broker keeps one password and the
	// registry may record the other. The claim makes the second caller a conflict instead of a
	// silent loser — see fenceSubscriber. It is taken before the row is read so that everything
	// this call decides from is read under the claim.
	releaseFence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		// A budget spent on the claim is a TIMEOUT, not a server fault, and this is the
		// commonest place for it to be spent: the claim is the first write of the request.
		// A conflict — another operation holds the claim — passes through unchanged, because
		// it is true whether or not the deadline also expired.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "claiming the subscriber for provisioning", err,
		)
	}
	defer releaseFence()

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "reading the subscriber's registry row", err,
		)
	}

	// SEC-05: fail closed on an authorization Kafka cannot enforce. Checked here — after the
	// row is read, before a secret exists — so a subscriber recording a partition key prefix
	// never receives a credential whose real scope is wider than the registry describes.
	if err := requireProvisionableKeyScope(subscriber); err != nil {
		return SubscriberCredential{}, err
	}

	// And refuse a subscriber that is on its way out, so an issuance cannot re-arm a principal
	// whose deregistration is already in flight.
	if err := requireActiveSubscriber(subscriber); err != nil {
		return SubscriberCredential{}, err
	}

	// And refuse a subscriber authorised for nothing, so a live principal that can read
	// nothing is never handed out looking like one that can. Checked in the same place and for
	// the same reason as the two above: before a secret exists and before the broker is
	// touched, so the refusal leaves no residue anywhere.
	if err := requireGrantedTopics(subscriber); err != nil {
		return SubscriberCredential{}, err
	}

	// THE ENDPOINT THE SUBSCRIBER WILL DIAL, resolved before anything is minted.
	//
	// Checked HERE — after the row is read, before a secret exists and before the broker is
	// touched — because the alternative orderings are both worse. Later, and a credential has
	// been created at the broker and recorded in the registry for a response that cannot be
	// returned. Never, and the response carries admin.Brokers(): the addresses BLNK dials,
	// which inside a deployment are internal and do not resolve for the subscriber. That is a
	// 200 whose failure surfaces as an unexplained connection timeout in somebody else's logs,
	// and it publishes the internal topology on the way out.
	subscriberBrokers, err := s.subscriberFacingBrokers(subscriber)
	if err != nil {
		return SubscriberCredential{}, err
	}

	// The reference OBSERVED before provisioning. It is what makes the issuance record
	// conditional: if another issuance for this subscriber commits in the meantime, this
	// value no longer matches and the write reports a conflict instead of silently
	// overwriting a record whose secret is the one that works.
	//
	// It is retained even under the fence, and that is not redundancy. The fence is LEASED, so
	// an issuance whose process stalled past its lease can find itself superseded; the
	// conditional write is what makes that outcome a reported conflict rather than a silent
	// overwrite.
	observedReference := subscriber.CredentialReference

	password, err := s.newPassword()
	if err != nil {
		// The cause is not returned, and here that is more than a topology concern: this is
		// the credential-generation path, so its error is the one most likely to render
		// something derived from the secret itself. It is logged bounded and the caller
		// receives the diagnosis only.
		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			"error":      sanitizeLogValue(err.Error(), maxLoggedErrorLength),
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
		// bounded rendering is of the ERROR, never of the inputs — and the caller receives
		// no cause at all.
		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
			"error":      sanitizeLogValue(err.Error(), maxLoggedErrorLength),
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

	result, err := admin.ProvisionSubscriberPrincipal(
		ctx,
		NewSubscriberProvisioningRequest(subscriber, password),
	)
	if err != nil {
		return SubscriberCredential{}, s.provisioningFailure(subscriber, result, err)
	}

	issuedAt := s.clock()

	if err := store.RecordSubscriberCredentialIfUnchanged(
		ctx, subscriberID, observedReference, reference, issuedAt,
	); err != nil {
		// recordFailure owns the compensation and its own logging; the classification then
		// re-reports a spent budget here as the timeout it was, exactly as on the two paths
		// above. It runs on the OUTSIDE so the compensation happens first either way, and it
		// leaves recordFailure's typed conflict — a superseded issuance — untouched.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "recording the issuance in the registry",
			s.recordFailure(ctx, admin, subscriber, err),
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
		Fingerprint:      model.CredentialFingerprint(reference),
		Replaced:         result.CredentialReplaced,
		password:         NewSubscriberSecret(password),
	}

	// LogFields, never the credential itself: the secret appears here only as its length.
	logrus.WithFields(credential.LogFields()).Info(
		"event subscriber: Kafka credential issued; the secret is returned once and is not " +
			"recoverable afterwards",
	)

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

// provisioningFailure turns a broker-side provisioning failure into the right typed error and
// records what state the broker was left in.
//
// Four outcomes, each needing a different answer:
//
//   - NOT CONFIGURED. The broker list emptied between the check and the call. 503
//     ErrKafkaUnavailable, and nothing was written.
//   - BUDGET EXPIRED or CALLER CANCELLED. 503 ErrKafkaUnavailable. What the broker did or did
//     not write is genuinely unknown, which the log line says; a retry re-provisions the same
//     boundary idempotently.
//   - COMPENSATED. The credential was written, the ACL grant failed, and the revocation was
//     CONFIRMED — the broker is clean and the generated secret is dead. Logged as a warning.
//   - COMPENSATION FAILED. The credential was written and the revocation failed too, so a
//     principal exists that can authenticate with no boundary. Logged at ERROR with the
//     principal named, which is the only record of what must be revoked by hand.
//
// No branch records an issuance and no branch returns a credential, so a caller that receives
// an error holds no secret. No message contains the password: it is not a field of the result
// and is never passed to this function.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row being provisioned, for the log fields.
//   - result SubscriberProvisioningResult: the only source of truth for what reached the
//     broker.
//   - cause error: the provisioning error.
//
// Returns:
//   - error: a typed ErrKafkaUnavailable or ErrSubscriberProvisioningFailed.
func (s *EventSubscriberService) provisioningFailure(
	subscriber *model.EventSubscriber,
	result SubscriberProvisioningResult,
	cause error,
) error {
	fields := logrus.Fields{
		"subscriber":         sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		"principal":          sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
		"credential_written": result.CredentialWritten,
		"compensated":        result.Compensated,
		"error":              sanitizeLogValue(cause.Error(), maxLoggedErrorLength),
	}

	switch {
	case errors.Is(cause, ErrKafkaAdminNotConfigured):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was attempted with no broker configured; nothing " +
				"was written",
		)

		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Kafka is not configured, so subscriber credentials cannot be issued",
			// Bounded, and the cause stays in the log line above. See
			// SubscriberErrorDetail: NewAPIError both re-logs its details unsanitized and
			// serialises them into the response, so passing the cause here undid the
			// sanitizing this function had just done — on both sides at once.
			NewSubscriberErrorDetail(
				"Kafka is not configured", subscriber.SubscriberID, false,
			),
		)

	case errors.Is(cause, context.DeadlineExceeded):
		logrus.WithFields(fields).Error(
			"event subscriber: credential issuance exceeded its budget, so whether the broker wrote " +
				"the credential is unknown; retrying re-provisions the same boundary idempotently",
		)

		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			fmt.Sprintf(
				"Provisioning Kafka credentials did not complete within %s",
				s.budget(),
			),
			// RETRYABLE, and saying so is the point: a retry re-provisions the same
			// boundary idempotently, which is exactly what the log line above tells an
			// operator. The state flags are carried because whether the broker wrote the
			// credential is genuinely unknown here, and a caller deciding whether to retry
			// needs to know that a secret may already exist that they do not hold.
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
			apierror.ErrKafkaUnavailable,
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
	// fine" and "a principal exists that a human has to revoke". The specific cause, and the
	// principal's name, stay in the log lines above.
	//
	// NOT retryable: each of these branches represents a broker that answered and refused,
	// or a compensation decision already taken, so an immediate retry repeats the same
	// failure. That is the opposite of the timeout and cancellation branches above, and
	// stating it is what stops a client retrying into a loop.
	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		"Failed to provision Kafka credentials for the subscriber",
		s.provisioningDetail(
			"Provisioning the Kafka principal failed at the broker",
			subscriber, result, false,
		),
	)
}

// provisioningDetail builds a bounded provisioning-failure detail carrying what reached the
// broker.
//
// It exists so the four branches of provisioningFailure cannot disagree about which state
// flags they report: the flags are the only way a caller learns whether a credential they do
// not hold may exist at the broker, and a branch that omitted them would leave that
// unanswerable.
//
// Parameters:
//   - reason string: fixed wording describing the failure.
//   - subscriber *model.EventSubscriber: the row being provisioned, read for its identifier.
//   - result SubscriberProvisioningResult: the only source of truth for what reached the
//     broker.
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

	return detail
}

// classifyIssuanceTimeout re-reports a registry failure that was really the issuance budget
// running out, or the caller going away, as the timeout it was.
//
// # The defect
//
// Issuance shares ONE deadline across the fence claim, the row read, up to four broker round
// trips and the issuance record. The broker half already distinguishes a timeout: see
// provisioningFailure, which answers a spent deadline or a cancelled caller with a retryable
// error rather than a server fault. The REGISTRY half did not. Both reads happen before the
// broker is touched, and both report through the repository's generic internal-server code, so a
// five-second budget spent waiting on a slow database arrived at the caller as HTTP 500 —
// indistinguishable from a defect in this service, and the correct reaction to the two is
// opposite: a defect must not be retried into a loop, while this should be retried and safely
// can be, because nothing has been written when it happens here.
//
// # Why the CONTEXT is consulted rather than the error
//
// loggedDatabaseError builds apierror.APIError{Code, Message} and deliberately does not carry
// the driver error, so the cause never reaches this function and errors.Is(err,
// context.DeadlineExceeded) cannot answer. What is reliable is the context: if it is done, the
// deadline this function's own caller installed has expired or the caller cancelled, and any
// failure of a call made under it is that expiry until something more specific says otherwise.
//
// # What it must NOT rewrite
//
// Only a generic internal-server failure is reclassified. A not-found, a conflict — the fence
// held by another operation, a superseded issuance — or a validation error each carry a meaning
// the caller needs, and each remains true whether or not the context also expired: the fence
// WAS held, the subscriber IS absent. Rewriting those to a timeout would send a client to retry
// something that will never succeed, which is the same class of mistake as the one being fixed,
// pointing the other way.
//
// Parameters:
//   - ctx context.Context: the issuance context, consulted for expiry.
//   - subscriberID string: named in the diagnostic so a spent budget is attributable.
//   - stage string: fixed wording for the step that was in flight, so an operator can tell a
//     slow fence claim from a slow row read.
//   - cause error: the error as the repository reported it.
//
// Returns:
//   - error: a typed timeout when the context expired and the cause was generic, otherwise
//     cause unchanged.
func (s *EventSubscriberService) classifyIssuanceTimeout(
	ctx context.Context,
	subscriberID string,
	stage string,
	cause error,
) error {
	if cause == nil {
		return nil
	}

	expiry := ctx.Err()
	if expiry == nil || !isInternalServerError(cause) {
		return cause
	}

	cancelled := errors.Is(expiry, context.Canceled)

	logrus.WithFields(logrus.Fields{
		"subscriber": sanitizeLogValue(subscriberID, maxLoggedFilterLength),
		"stage":      stage,
		"budget":     s.budget().String(),
		"cancelled":  cancelled,
		"error":      sanitizeLogValue(cause.Error(), maxLoggedErrorLength),
	}).Warn(
		"event subscriber: credential issuance ran out of time at the registry rather than failing; " +
			"the provisioning claim is released and a retry is safe, because issuance re-provisions " +
			"the same boundary idempotently",
	)

	message := fmt.Sprintf(
		"Provisioning Kafka credentials did not complete within %s", s.budget(),
	)
	reason := "The registry did not answer within the issuance budget while " + stage

	if cancelled {
		message = "Provisioning Kafka credentials was cancelled before it completed"
		reason = "The request was cancelled while " + stage
	}

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningTimeout,
		message,
		// RETRYABLE, and true rather than optimistic: these paths run before a secret is
		// generated and before the broker is touched, so a retry starts from the same state
		// the first attempt found. The broker-state flags provisioningDetail carries are
		// deliberately absent — no credential can have been written this early, and reporting
		// flags that are structurally false would invite a caller to check for residue that
		// cannot exist.
		NewSubscriberErrorDetail(reason, subscriberID, true),
	)
}

// isInternalServerError reports whether an error carries the generic internal-server code.
//
// It exists so classifyIssuanceTimeout can tell "the repository had no better answer" from a
// typed domain outcome, and it mirrors isNotFoundError and isConflictError: the code is read
// through apierror.Normalize, so the legacy INTERNAL_SERVER_ERROR the database layer still
// constructs and the canonical GEN_INTERNAL are the same answer. errors.As is used rather than a
// type assertion so a wrapped error classifies identically.
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

// recordFailure handles the narrow window in which the broker holds a credential the registry
// could not record.
//
// The broker write happened and the database write did not, so the subscriber's principal can
// authenticate while blnk.event_subscribers still describes the previous state. The secret is
// NOT returned in that case — handing back a credential whose issuance was not recorded would
// leave an untracked, working credential in a caller's hands — and the credential is revoked so
// the broker and the registry agree again.
//
//   - SUPERSEDED (conflict). Another issuance for this subscriber committed while this one was
//     at the broker, so the registry now records ITS reference. REVOCATION IS REFUSED HERE:
//     Kafka stores one credential per principal, so revoking would destroy the winner's
//     working secret as well as this one's. The conflict is returned as it came — 409 — so the
//     handler can say "your issuance was superseded" rather than hand back a password that may
//     authenticate against nothing. A warning records the remedy: issue once more, serially,
//     so that the registry's reference and the broker's credential are known to agree.
//   - SUBSCRIBER GONE (not found). The row was deleted mid-flight, so there is no registry
//     record left to protect and the broker holds a credential for a principal nothing
//     describes. It is revoked, best-effort.
//   - ANYTHING ELSE (a real write failure). The registry does not record this issuance and the
//     caller receives an error, so the secret is returned to nobody: leaving it live would
//     leave an orphan credential that nothing references. It is revoked, best-effort, which
//     over-reports the loss of access rather than hiding a live secret — the safe direction,
//     and the one Datasource.ClearSubscriberCredential's documentation prescribes.
//
// Revocation is BEST-EFFORT by design: the write has already failed and the caller is getting
// an error either way, so a revocation problem is logged with the principal named rather than
// replacing the error the caller actually needs to see.
//
// # CLEAN-01: both cleanups run on a FRESH deadline
//
// They used to run on the issuance context, which carries the 5-second budget — and that
// budget expiring is one of the commonest reasons the recording write fails. So on exactly the
// occasion this function exists for, the revocation and the record-clearing were attempted
// against an already-cancelled context, failed instantly, and left the residue they exist to
// remove: a live credential the registry does not record. A cleanup that is busiest when it
// cannot work is not a cleanup.
//
// Parameters:
//   - ctx context.Context: the issuance context. Used for its VALUES only; a fresh bounded
//     context is derived for each cleanup write, so an expired budget no longer prevents them.
//   - admin subscriberPrincipalProvisioner: the client that provisioned.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - cause error: the repository's error, already typed.
//
// Returns:
//   - error: the typed error to answer the request with.
func (s *EventSubscriberService) recordFailure(
	ctx context.Context,
	admin subscriberPrincipalProvisioner,
	subscriber *model.EventSubscriber,
	cause error,
) error {
	fields := logrus.Fields{
		"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		"principal":  sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
		"error":      sanitizeLogValue(cause.Error(), maxLoggedErrorLength),
	}

	if isConflictError(cause) {
		logrus.WithFields(fields).Warn(
			"event subscriber: a concurrent issuance superseded this one, so the credential this " +
				"call generated was NOT recorded and may or may not be the one now live at the " +
				"broker; the credential was deliberately not revoked, because one SCRAM credential " +
				"exists per principal and revoking would destroy the other issuance's secret too. " +
				"Issue once more, serially, to make the registry and the broker agree",
		)

		return cause
	}

	cleanup, cancelCleanup := subscriberCleanupContext(ctx)
	defer cancelCleanup()

	if err := admin.RevokeSubscriber(cleanup, subscriber); err != nil {
		logrus.WithFields(fields).WithField(
			// Bounded: a refused administrative request carries the broker's whole request dump.
			"revocation_error", sanitizeLogValue(err.Error(), maxLoggedErrorLength),
		).Error(
			"event subscriber: recording the issuance failed AND revoking the credential it had " +
				"already written failed, so the principal named here holds a credential that no " +
				"registry row records; revoke it by hand",
		)

		return cause
	}

	if isSubscriberNotFoundError(cause) {
		logrus.WithFields(fields).Warn(
			"event subscriber: the subscriber was removed while its credential was being issued, so " +
				"the credential was revoked and nothing was recorded",
		)

		return cause
	}

	// The row survives and still names whatever credential it held BEFORE this issuance — but
	// that credential no longer exists: the upsert replaced it and the revocation above removed
	// the replacement. Clearing the reference is what stops the registry claiming access that
	// has gone, which every reader of it (an operator, the migration report, a reconciliation)
	// would otherwise believe. Best-effort, because the write that just failed is the same
	// connection this one uses.
	if s.clearCredentialRecord(cleanup, subscriber, fields) {
		logrus.WithFields(fields).Warn(
			"event subscriber: recording the issuance failed, so the credential written at the " +
				"broker was revoked and the registry's credential record was cleared; the " +
				"subscriber has no Kafka access until credentials are re-issued",
		)
	}

	return cause
}

// clearCredentialRecord erases a subscriber's credential record, best-effort.
//
// It is the registry half of a revocation and is deliberately non-fatal: it runs on paths
// where the caller is already receiving an error, so failing here would replace the error the
// caller needs with one about bookkeeping. A failure is logged with the subscriber named so
// the divergence — registry claiming a credential the broker no longer holds — is visible and
// correctable.
//
// Parameters:
//   - ctx context.Context: cancels the write. Callers on a failure path pass a FRESH bounded
//     context rather than their own — see subscriberCleanupContext — because the deadline that
//     failed is often the reason they are here.
//   - subscriber *model.EventSubscriber: the row whose record is being cleared.
//   - fields logrus.Fields: the caller's log fields, reused so both lines correlate.
//
// Returns:
//   - bool: true when the record was cleared, so the caller can phrase its own log line
//     accordingly.
func (s *EventSubscriberService) clearCredentialRecord(
	ctx context.Context,
	subscriber *model.EventSubscriber,
	fields logrus.Fields,
) bool {
	store, err := s.requireStore()
	if err != nil {
		return false
	}

	if err := store.ClearSubscriberCredential(ctx, subscriber.SubscriberID); err != nil {
		logrus.WithError(err).WithFields(fields).Error(
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
// It extends the package's isNotFoundError with the subscriber-specific code. That extension
// is necessary rather than tidy: ErrSubscriberNotFound is its own canonical code, so
// apierror.Normalize leaves it unchanged and the generic classifier — which recognises the
// generic and event-specific codes — does not match it. Without this, a subscriber deleted
// mid-issuance would be classified as an ordinary write failure and reported with the wrong
// remedy.
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

// RevokeSubscriberCredential ends a subscriber's Kafka access while leaving it registered.
//
// It is the operation to reach for when a secret has leaked: the subscriber stays in the
// registry, its topics and group are unchanged, and a later issuance restores access with a new
// secret. Deleting the SCRAM credential is what ends access, because that is what the SASL
// handshake checks; the ACL bindings the row describes are removed with it.
//
// The credential RECORD is cleared only after the broker confirms the revocation, so the
// registry never says "no credential" while one still authenticates.
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

	// FENCED, for the same reason issuance is: revoking the credential an issuance is in the
	// middle of writing would leave the broker and the registry describing different states,
	// and which one won would depend on the order two network calls happened to complete in.
	releaseFence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer releaseFence()

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return err
	}

	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	// With no broker there is nothing to revoke, so the registry record is simply cleared. A
	// deployment that has never configured Kafka can still tidy a row that predates that
	// decision.
	if admin.IsConfigured() {
		if err := admin.RevokeSubscriber(ctx, subscriber); err != nil {
			logrus.WithFields(logrus.Fields{
				"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
				"principal":  sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
				// Bounded, for the reason given in pruneBrokerAccess.
				"error": sanitizeLogValue(err.Error(), maxLoggedErrorLength),
			}).Error(
				"event subscriber: revoking the subscriber's Kafka access failed, so the registry " +
					"record was left untouched and the credential may still work; retry the revocation",
			)

			return apierror.NewAPIError(
				apierror.ErrSubscriberProvisioningFailed,
				"Failed to revoke the subscriber's Kafka access",
				// Same treatment as the deregistration path, for the same reason: the
				// principal and the broker's own words stay in the log, and the caller
				// receives the diagnosis plus the fact that a retry is safe. Revocation is
				// idempotent at the broker, so repeating it cannot make things worse.
				NewSubscriberErrorDetail(
					"Revoking the subscriber's Kafka access failed at the broker",
					subscriber.SubscriberID, true,
				),
			)
		}
	}

	if err := store.ClearSubscriberCredential(ctx, subscriberID); err != nil {
		return err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber":     sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		"principal":      sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
		"broker_present": admin.IsConfigured(),
	}).Info(
		"event subscriber: credential revoked; the subscriber remains registered and can be " +
			"re-issued",
	)

	return nil
}

// ---------------------------------------------------------------------------------------
// The dual-run migration surface
//
// These four operations exist for the window during which Kafka publishing and legacy HTTP
// webhook delivery run side by side from the same outbox events. They record where a subscriber
// used to receive pushes, when it finished moving, and — once a retention period has passed —
// forget the endpoint. They are also what makes the sunset observable: without a per-subscriber
// record there is no route on which a 410 Gone could be seen after the sunset date.
//
// /hooks IS NOT THIS SURFACE. Those are the PRE_TRANSACTION and POST_TRANSACTION request-time
// callouts in internal/hooks; they carry a response contract that can influence transaction
// processing, they stay fully functional, and nothing here touches them.
// ---------------------------------------------------------------------------------------

// RecordLegacyWebhookSubscription records the legacy HTTP endpoint a migrating subscriber
// received pushes on.
//
// The URL is validated at the persistence boundary — HTTPS only, and no loopback, link-local,
// private-range or unqualified destination. That policy is applied there rather than here
// because the column is a FUTURE REQUEST SINK: nothing sends to it today, and the moment
// anything does, whatever is stored becomes a request Blnk makes from inside its own network.
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record. Blank is refused, because a subscription
//     with no URL records nothing; use ClearLegacyWebhookSubscription to remove one.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: a typed validation error for a blank URL, ErrSubscriberNotFound, or the
//     repository's validation or write error.
func (s *EventSubscriberService) RecordLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID, webhookURL string,
) (*model.EventSubscriber, error) {
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

	return s.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{WebhookURL: &trimmed})
}

// ClearLegacyWebhookSubscription removes a single subscriber's recorded legacy endpoint.
//
// It is the one-row form of the retention rule PurgeMigratedWebhookURLs applies in bulk, for
// an operator correcting a record or honouring a request before the retention period elapses.
// migrated_at is untouched: it is an audit fact rather than third-party data, and it is what
// migration-progress reporting counts.
func (s *EventSubscriberService) ClearLegacyWebhookSubscription(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	cleared := ""

	return s.UpdateSubscriber(ctx, subscriberID, SubscriberUpdate{WebhookURL: &cleared})
}

// MarkSubscriberMigrated stamps the instant a subscriber completed its move from legacy HTTP
// delivery to Kafka consumption.
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what migration-progress
// reporting counts during the dual-delivery window. Re-stamping an already-migrated subscriber
// overwrites the instant rather than failing, because the useful question is "has it moved?"
// and a correction is a legitimate answer to "when?".
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - time.Time: the instant recorded, in UTC, so a caller can report it without re-reading
//     the row.
//   - error: ErrSubscriberNotFound when no such subscriber exists, or the repository's error.
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
		"subscriber":  sanitizeLogValue(subscriberID, maxLoggedFilterLength),
		"migrated_at": migratedAt.Format(time.RFC3339Nano),
	}).Info("event subscriber: recorded as migrated to Kafka consumption")

	return migratedAt, nil
}

// PurgeMigratedWebhookURLs forgets the legacy endpoint of every subscriber that migrated
// before a cut-off.
//
// The endpoint exists solely to give an already-webhooked subscriber somewhere to be migrated
// FROM. Once migrated it is a third-party address with no remaining purpose — retention
// without a reason, and a destination that becomes a request the moment any sender is wired to
// it. migrated_at is deliberately EXEMPT and is never purged: it is an audit fact, not
// third-party data.
//
// The cut-off is the CALLER'S, so the retention period stays a policy decision rather than a
// constant buried in a service. A zero cut-off is refused by the repository rather than read
// as "purge everything".
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
//
// These are how the rest of the codebase reaches the subscriber surface: the API handlers
// hold a *Blnk and call through it, exactly as they do for every other domain operation.
//
// The pattern is the one event_dlt.go established, with one rule applied without exception:
// EVERY wrapper closes the service it builds. Not only the ones that reach the broker today.
//
// The narrower rule — close only where an administrative client is resolved — is what this
// replaced, and it was wrong twice over. It leaked outright at UpdateSubscriber, which resolves
// a provisioner twice, once to prune the broker-side grant and once to re-grant it, and whose
// wrapper closed nothing; every update therefore held that client's connections until the
// process ended. And it made every other wrapper's correctness depend on a fact about a
// DIFFERENT function, several hundred lines away, that no compiler checks: the moment a
// registry-only operation grew a broker touch, its wrapper would start leaking silently and
// the wrapper itself would look untouched in the diff.
//
// Closing unconditionally costs nothing to get right. Close is idempotent, nil-safe, a no-op
// when nothing was resolved, and it never closes an INJECTED client — so a wrapper cannot be
// wrong by calling it, and cannot be right by omitting it.
//
// Nothing caches a service on the Blnk instance, because that would put administrative-client
// lifetime inside a struct whose Close does not own it.
// ---------------------------------------------------------------------------------------

// EventSubscribers returns a subscriber service bound to this instance's datasource.
//
// The returned service resolves an administrative client from live configuration the first
// time one is needed, so EVERY CALLER MUST CLOSE IT — `defer service.Close()` — or the
// connections that client opens are held until the process ends.
//
// The obligation is stated unconditionally on purpose. Close is a no-op when nothing was
// resolved, so a caller doing registry work only loses nothing by honouring it, while a caller
// that reasons about which operations touch the broker is relying on a fact about another
// function that will eventually stop being true. Every Blnk wrapper below closes.
//
// It is nil-safe: a nil instance yields a service whose operations report an unavailable
// registry rather than panicking.
func (b *Blnk) EventSubscribers() *EventSubscriberService {
	if b == nil {
		return NewEventSubscriberService(nil, nil)
	}

	return NewEventSubscriberService(b.datasource, nil)
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
func (b *Blnk) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.ListSubscribers(ctx, limit, offset)
}

// UpdateEventSubscriber applies the mutable subset of a subscriber. It is the write behind
// PUT /subscribers/:subscriber_id.
//
// It REACHES THE BROKER, which is easy to miss from the name: reconciling authorized_topics
// means pruning the ACL bindings the new authorization no longer implies and granting the ones
// it adds, so UpdateSubscriber resolves an administrative client twice. The short-lived client
// is closed before returning, so the handler needs no lifecycle handling of its own — and
// before this close existed, every subscriber update held that client's connections for the
// remaining life of the process.
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

// IssueSubscriberKafkaCredentials mints a subscriber's SASL/SCRAM credential. It is the write
// behind POST /subscribers/:subscriber_id/kafka-credentials.
//
// The returned credential carries the plaintext secret, and this is the only call that ever
// will: put it in the response and let it go. Do not log it, do not store it, and do not
// expect to be able to read it again.
//
// The five-second issuance budget is applied inside the service, so a handler needs no
// timeout of its own — though a shorter one it already holds is still honoured.
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

// RecordSubscriberWebhookSubscription records a migrating subscriber's legacy endpoint. It is
// the write behind POST and PUT /subscribers/:subscriber_id/webhook-subscription.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once WEBHOOK_DEPRECATION_SUNSET_DATE has passed every request to
// those routes is answered with 410 Gone. Use the Kafka event stream and
// IssueSubscriberKafkaCredentials instead.
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

// MarkEventSubscriberMigrated records that a subscriber has completed its move to Kafka
// consumption.
func (b *Blnk) MarkEventSubscriberMigrated(ctx context.Context, subscriberID string) (time.Time, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.MarkSubscriberMigrated(ctx, subscriberID)
}

// PurgeMigratedSubscriberWebhookURLs forgets the legacy endpoints of subscribers that migrated
// before a cut-off.
func (b *Blnk) PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.PurgeMigratedWebhookURLs(ctx, migratedBefore)
}

// closeEventSubscriberService closes a service built for one operation, logging rather than
// propagating a close failure.
//
// It exists so the deferred close reads as one call and the error is handled in exactly one
// place: errcheck is satisfied, and a teardown problem cannot be mistaken for a failed
// issuance — which would send an operator to retry something that already succeeded and, for
// issuance specifically, would replace a working credential.
//
// Parameters:
//   - service *EventSubscriberService: the service to close. May be nil.
func closeEventSubscriberService(service *EventSubscriberService) {
	if err := service.Close(); err != nil {
		logrus.WithError(err).Warn(
			"closing the short-lived Kafka administrative client for subscriber management failed",
		)
	}
}
