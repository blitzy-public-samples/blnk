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

// event_subscriber.go is the subscriber registry and the one-time credential issuance
// behind POST /subscribers/{id}/kafka-credentials (requirement R-7).
//
// # The access model, stated once
//
// THERE ARE NO PER-TENANT TOPICS. Every subscriber reads the same four category topics
// that event_topics.go names, and isolation comes from four things instead:
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
//  4. The PARTITION-KEY PREFIX, which is ADVISORY ONLY. Kafka's authorizer has no
//     message-key dimension, so this narrows what a subscriber WANTS to read and never
//     what it CAN read. Nothing here or anywhere else may treat it as a boundary.
//
// Creating a topic per subscriber would be the obvious alternative and it is the wrong
// one: it multiplies the partition count and the ordering guarantee by the number of
// subscribers, and it makes adding a subscriber a topic-provisioning event rather than a
// credential one.
//
// # SECRET HANDLING IS THE POINT OF THIS FILE
//
// The generated SASL password is returned to the caller EXACTLY ONCE, in the
// SubscriberCredential value that IssueSubscriberCredential produces, and it is
// persisted NOWHERE. Only a non-reversible reference (model.DeriveCredentialReference)
// and the issuance instant are stored, which is the same posture as blnk.api_keys where
// the stored value is a bcrypt hash and the raw key is never kept.
//
// There is deliberately NO code path — no getter, no list projection, no log line, no
// error message, no trace attribute and no metric label — through which the password can
// be read back. A lost password can only be REPLACED by issuing a new one, never
// recovered. If a future change adds a "convenience" accessor that returns a stored
// secret, it is wrong twice over: the value it would read does not exist in the schema
// (blnk.event_subscribers has no column capable of holding one), and its existence would
// undo the guarantee this file is written to keep.
//
// # The legacy webhook columns, and what /hooks is not (AMBIGUITY-1)
//
// A subscriber row carries a legacy webhook_url and a migrated_at timestamp FOR THE
// DUAL-RUN WINDOW ONLY. They exist because the requirement to migrate existing
// subscribers off "the webhook subscription REST API" meets a repository in which no such
// API exists: the entire subscription surface today is one global
// WebhookConfig{Url, Headers} value in the configuration, consumed by processHTTP in
// webhooks.go, with no per-subscriber URL storage and no registration endpoint anywhere.
// Recording a legacy URL per subscriber is what gives an already-webhooked subscriber
// somewhere to be recorded and migrated FROM, and it is what makes the 410 Gone sunset
// behaviour observable at all — without a subscription surface there is no route on which
// a 410 could ever be seen.
//
// /hooks IS NOT THAT SURFACE AND IS UNTOUCHED. Those are the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts in internal/hooks: synchronous interception with
// a response contract that can influence transaction processing. They remain fully
// functional, they are out of scope for the sunset, and they have nothing to do with this
// file.
//
// # What is deliberately NOT here
//
//   - NO SQL. database/event_subscriber.go owns every statement; this file delegates.
//   - NO HTTP handling and NO master-key gating. api/subscribers.go owns those, following
//     the ensureHookManagementAuthorized pattern in api/hooks.go.
//   - NO direct Kafka admin calls. Everything broker-side goes through event_admin.go so
//     there is exactly one authenticated administrative client and one place where a
//     credential or an ACL is written.
//   - NO CONSUMER of any kind: no consumer library, no consumer error handling and no
//     subscriber-side dead-lettering. Blnk publishes the `<topic>.dlt` naming convention
//     in docs/event-streaming.md and stops there; what a subscriber does with a failed
//     record is the subscriber's own concern.

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

// subscriberPasswordAlphabet is the alphabet a generated secret is drawn from.
//
// It is unreserved printable ASCII — letters and digits only — and each exclusion is a
// correctness requirement rather than a style preference:
//
//   - Nothing outside 0x21-0x7E, because a SCRAM client applies SASLprep to the password
//     before proving knowledge of it while the broker stores a value derived from the
//     bytes it was handed. Outside printable ASCII the two can differ, and the result is a
//     credential that authenticates for nobody with an error indistinguishable from a
//     wrong password. validateSCRAMPassword in event_admin.go refuses such a value; this
//     generator cannot produce one.
//   - No SPACE, no QUOTES and no SHELL METACHARACTERS, because the secret's whole life is
//     spent being pasted into client configuration: a JAAS sasl.jaas.config string, a
//     librdkafka property file, a Kubernetes secret, an environment variable, a docker
//     compose file. Every one of those has a quoting story, and a secret that survives all
//     of them unquoted is a secret an operator cannot corrupt by accident.
//
// The alphabet is 62 characters, which is NOT a power of two, so the draw below must
// reject modulo bias rather than mask bits — see generateSubscriberPassword.
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

	// PartitionKeyPrefix is an ADVISORY consumer-side filter hint, not a restriction on
	// what the subscriber can read. Nil means none was requested.
	PartitionKeyPrefix *string

	// WebhookURL records the legacy HTTP endpoint a migrating subscriber received pushes
	// on before moving to Kafka. Nil for a subscriber onboarded after the cutover, which
	// never had one. Dual-run only.
	WebhookURL *string
}

// SubscriberUpdate is the mutable subset of a registered subscriber.
//
// Every field is nilable so that "omitted" is distinguishable from "explicitly cleared",
// which this shape needs rather than merely benefits from: a nil PartitionKeyPrefix means
// the subscriber is entitled to whole topics, while a present empty string means it has
// asked to filter on the empty prefix. A plain string cannot express the difference, so it
// would make silently inverting an operator's intent possible.
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
	// CHANGING THIS ROW DOES NOT CHANGE WHAT THE SUBSCRIBER CAN READ TODAY. The row is the
	// record of intent that the NEXT credential issuance provisions from. Widening takes
	// effect when credentials are re-issued. NARROWING needs more than that: ACL creation
	// is additive and idempotent, so a binding for a topic that has been removed from this
	// list stands until it is explicitly deleted — which revocation does. A reduction is
	// therefore completed by revoking the subscriber's access and re-issuing, never by
	// editing this list alone.
	AuthorizedTopics []string

	// PartitionKeyPrefix replaces the advisory filter hint when non-nil; a present empty
	// string clears it. It grants and revokes nothing.
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
// # The secret is returned once and lives only in this value
//
// The plaintext is held in an UNEXPORTED field of the redacting SubscriberSecret type and
// is reachable through exactly one accessor, Password. That is the one-time hand-over the
// requirement describes, and it is the only route: nothing persists the value, no reader
// returns it, and blnk.event_subscribers has no column capable of holding it. A subscriber
// that loses its password can only be issued a new one.
//
// # Why every rendering is redacted
//
// Format, String and GoString all render a redacted summary, so `%v`, `%+v`, `%#v`, `%s`
// and `%q` on this value — the shapes a log line, a test failure message or a panic trace
// actually take — cannot print the secret. fmt consults a Formatter before it considers
// descending into fields, which is what makes the guarantee total rather than
// verb-by-verb: an unexported field of a redacting type would otherwise be printed by
// `%+v` as its raw contents, because fmt cannot call methods on a field it cannot take the
// interface of. encoding/json is safe for the same structural reason — it skips unexported
// fields — so no MarshalJSON is needed here and none should be added, since a hand-rolled
// one would have to be kept in step with every future field.
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

// Password reveals the generated secret, and is the ONE place it can be read.
//
// It exists because the credential endpoint has to put the secret in its response body
// exactly once; there is no other legitimate caller. It reads the value out of this
// in-memory result — it does NOT read anything back from storage, and it cannot, because
// nothing was stored.
//
// Call it once, hand the value to the response, and let it go. Do not log it, do not put it
// in an error, do not attach it to a span, and do not keep it: the moment the response is
// written the plaintext should exist nowhere in the process.
//
// Returns:
//   - string: the plaintext SASL/SCRAM password, or "" on a zero value.
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
//
// Returns:
//   - logrus.Fields: the fields to log an issuance with.
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
//
// Returns:
//   - string: an identifying, secret-free description.
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
//
// Returns:
//   - string: an identifying, secret-free description.
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
// One instance is enough for a process and it is safe for concurrent use: the store, the
// clock, the generator and the budget are read-only after construction, and the lazily
// resolved administrative client is guarded by a mutex.
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
// A nil store is accepted rather than rejected, following NewEventDeadLetterService:
// construction is not where that becomes a problem, and every operation reports it with a
// clear error. That keeps this constructor free of an error return for a case no production
// caller reaches.
//
// A nil admin is likewise accepted and means "resolve one from configuration when
// issuance first needs it". Registry reads and writes never need one at all, so a
// deployment with no Kafka can register, list and update subscribers exactly as it can
// without this feature — only issuance itself reports ErrKafkaUnavailable.
//
// Parameters:
//   - store eventSubscriberStore: the repository. database.IDataSource satisfies it. May
//     be nil.
//   - admin subscriberPrincipalProvisioner: the administrative client, or nil to resolve
//     one lazily. KafkaAdmin satisfies it.
//
// Returns:
//   - *EventSubscriberService: a ready service.
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
//
// Parameters:
//   - budget time.Duration: the new ceiling. Non-positive restores
//     SubscriberCredentialIssuanceBudget.
//
// Returns:
//   - *EventSubscriberService: the service, for chaining.
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
		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Configuration is unavailable, so Kafka credentials cannot be provisioned",
			fmt.Errorf("blnk: loading configuration for subscriber provisioning: %w", err),
		)
	}

	admin, err := NewKafkaAdmin(cnf)
	if err != nil {
		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Failed to build the Kafka administrative client for subscriber provisioning",
			err,
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
//
// Returns:
//   - time.Duration: always positive.
func (s *EventSubscriberService) budget() time.Duration {
	if s == nil || s.issuanceBudget <= 0 {
		return SubscriberCredentialIssuanceBudget
	}

	return s.issuanceBudget
}

// ---------------------------------------------------------------------------------------
// The derivations, and why each is derived rather than chosen
//
// A subscriber's KAFKA PRINCIPAL and CONSUMER GROUP NAMESPACE are the two names its entire
// access boundary is expressed in: the SCRAM credential is minted for the principal, and
// every ACL binding names the principal and the namespace. Whoever chooses those two strings
// chooses the boundary — so they are derived, always, from the subscriber's immutable
// identifier, and never accepted from a request.
//
// Both derivations are PURE FUNCTIONS of the identifier, which is what makes them stable
// across re-issuance: issuing a new credential changes the secret and nothing else, so a
// subscriber's consumer group survives a rotation and its ACL grant does not have to be
// re-pointed. It is also what lets an operator reconstruct them by hand while triaging with
// nothing but a subscriber id:
//
//	principal        = "blnk-sub-" + <subscriber_id>
//	group namespace  = "blnk-sub-" + <subscriber_id> + "."       (the PREFIXED ACL resource)
//	default group    = "blnk-sub-" + <subscriber_id> + ".default"
//
// The trailing terminator on the namespace is the disjointness guarantee: it cannot occur
// inside a canonical identifier, so "blnk-sub-abc." and "blnk-sub-abcd." can never overlap
// however the identifiers relate. Without it, a subscriber whose id is a leading substring
// of another's would reserve the other's namespace and could join its consumer groups and
// take its partition assignments.
//
// The composition itself lives in model/event.go, not here, because the persistence layer
// and its CHECK constraints have to agree with it byte for byte and neither can import this
// package. These wrappers exist so the root package — and, through it, the API and the CLI
// — reach the derivations by name rather than by re-implementing a string concatenation.
//
// docs/kafka-operations.md carries the same three lines in its ACL-model section for
// operators who are reading a runbook rather than this file.
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

// SubscriberPartitionKeyPrefix reports the ADVISORY partition-key prefix recorded for a
// subscriber, flattening the nullable column to a plain string.
//
// # It is recorded, not derived, and that is not an omission
//
// Every other value in the access model is derived from the subscriber's identity. This one
// cannot be, and must not be. Kafka message keys on Blnk's topics are LEDGER partition keys
// — the publisher keys each event by the aggregate's ledger so that one aggregate's events
// land on one partition — so a prefix derived from the subscriber's own identifier would
// match no record ever produced, and a subscriber that filtered on it would silently discard
// its entire stream. The prefix is therefore a statement about which ledgers a subscriber
// cares about, which only the caller registering it can make.
//
// # It is ADVISORY and is not an authorization boundary
//
// Kafka's authorizer has no message-key dimension: there is no ACL that restricts a
// consumer to a slice of a topic by key. A subscriber granted a topic can read every record
// on it regardless of this value. Nothing may treat a non-empty prefix as narrowing what a
// subscriber CAN read — only as describing what it WANTS to read. Treating it as isolation
// would mean believing two subscribers on one topic cannot see each other's events, which is
// false, and making that belief the basis of a tenancy decision.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the registry row. Nil yields "".
//
// Returns:
//   - string: the recorded prefix, or "" when none was requested.
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
//
// Parameters:
//   - cause error: the derivation failure.
//
// Returns:
//   - error: a typed apierror carrying the cause.
func invalidSubscriberIdentifier(cause error) error {
	return apierror.NewAPIError(
		apierror.ErrGenValidation,
		"The subscriber ID cannot be used to derive a Kafka identity",
		cause,
	)
}

// generateSubscriberPassword mints a SASL/SCRAM secret with crypto/rand.
//
// # Never math/rand
//
// This value is the whole of a subscriber's authentication. math/rand is seeded
// deterministically and its output is reconstructible from a handful of observations, so a
// secret drawn from it is guessable by anyone who has seen another — which, for a service
// that issues one secret per subscriber, is every subscriber. crypto/rand reads the
// operating system's CSPRNG and is the only acceptable source here.
//
// # The draw is unbiased
//
// The alphabet is 62 characters, which is not a power of two, so taking a random byte modulo
// 62 would make the first four characters of the alphabet measurably more likely than the
// rest. crypto/rand.Int draws uniformly over [0, n) and handles the rejection internally, so
// the bias cannot be reintroduced by an arithmetic slip here.
//
// # The distinct-character floor is guaranteed, not hoped for
//
// event_admin.go refuses a password that is long but repetitive, because thirty-two
// identical characters clear a length floor while carrying almost no entropy. A 48-character
// draw from 62 symbols falls below that floor with probability far below one in 10^30, so
// the loop below is expected to run exactly once — and it exists anyway, because a generator
// that CAN emit a value the boundary will reject has a failure mode nothing tests. Re-drawing
// makes the guarantee total at no cost.
//
// The final validation is the same function the admin client applies, called here so a
// generator defect fails at the generator with the secret nowhere in the message, rather
// than as an obscure rejection one broker round trip later.
//
// Returns:
//   - string: a fresh secret of generatedSubscriberPasswordLength printable ASCII
//     characters.
//   - error: when the system CSPRNG is unavailable, or — unreachably — when the draw could
//     not satisfy the strength floors. Neither message contains any part of a secret.
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

// validateSubscriberGrant checks an authorised-topic list against the REAL topic catalogue.
//
// # Why a grant is validated here as well as at persistence and at provisioning
//
// A subscriber must not be registered against a topic that does not exist. The ACL grant
// would then name a resource nothing publishes to, so it would authorise nothing, and — far
// worse — the subscriber-isolation test would be asserting against a boundary that has no
// content: it would pass while proving nothing at all.
//
// Two checks, and neither subsumes the other:
//
//   - model.ValidateSubscriberTopics applies the RESOURCE BOUNDS: cardinality, blank
//     elements, name length, the character set Kafka permits, and duplicates. These hold
//     whatever the catalogue contains.
//   - IsSubscriberGrantableTopic applies the CATALOGUE, resolved from the configured topic
//     prefix by event_topics.go. It is an exact match against the subscriber-facing category
//     topics, so a dead-letter topic, an internal category, a foreign topic, a wildcard and a
//     name differing only in case or whitespace are all refused.
//
// An EMPTY grant is VALID and must stay valid: a subscriber authorised for nothing is the
// fail-closed default of a fresh registration, and refusing it would make registration and
// authorisation one inseparable step — a subscriber could not exist before its topics were
// decided.
//
// Parameters:
//   - topics []string: the grant as supplied. Nil and empty are both accepted.
//
// Returns:
//   - error: a typed validation error naming the first offending topic and the grantable
//     set, or nil.
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

// maxAdvisoryPartitionKeyPrefixLength bounds the advisory filter hint.
//
// A partition key on Blnk's topics is a ledger partition key — a "<prefix>_<uuid>" string —
// so 256 characters is far more than any legitimate prefix of one needs while still refusing
// an unbounded value. It matches the bound api/model applies to the same field; the number is
// restated rather than imported because the API package depends on this one and not the other
// way round.
const maxAdvisoryPartitionKeyPrefixLength = 256

// normalizeAdvisoryKeyPrefix validates and normalises the ADVISORY partition-key prefix.
//
// # Three inputs, three meanings
//
//   - NIL means omitted, and stays nil: no filter was requested.
//   - A value that is blank or whitespace-only means CLEAR, and becomes nil, so the column
//     holds NULL — "entitled to whole topics" — rather than the empty string, which would read
//     as "restricted to the empty prefix". Those are opposite intents and the nullable column
//     exists to keep them distinguishable.
//   - Anything else is kept verbatim after being checked.
//
// # Why it is checked here and not only in the request DTO
//
// The DTO applies the same rules, and that covers HTTP. It does not cover a CLI caller, a
// migration or a fixture, all of which reach this service directly — and the row outlives any
// single request, so a value accepted once is read by every later provisioning and every later
// report. Surrounding whitespace is REFUSED rather than trimmed for the reason a topic name is:
// a value differing from another only by whitespace is almost always a copy-paste artefact,
// and quietly rewriting it would store a filter the caller did not ask for.
//
// Parameters:
//   - prefix *string: the supplied prefix. Nil is accepted.
//
// Returns:
//   - *string: the normalised prefix, or nil for omitted and cleared.
//   - error: a typed validation error naming the rule broken.
func normalizeAdvisoryKeyPrefix(prefix *string) (*string, error) {
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

	if len(value) > maxAdvisoryPartitionKeyPrefixLength {
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"The partition key prefix is too long",
			fmt.Errorf(
				"event subscriber: partition key prefix is %d characters, above the %d permitted",
				len(value), maxAdvisoryPartitionKeyPrefixLength,
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
// # What it does, in order
//
//  1. Generates a subscriber id when none was supplied, in the repository's
//     "<prefix>_<uuid>" form via model.GenerateSubscriberID. The module suffix is "sub"
//     (model.SubscriberIDPrefix), matching the convention ledgers ("ldg"), balances ("bln")
//     and identities ("idt") already follow. A generated id is canonical by construction,
//     so it can always be derived from; a SUPPLIED id is checked and refused if it cannot.
//  2. Requires a name, because an unnamed principal cannot be triaged.
//  3. Validates the grant against the real topic catalogue, so a subscriber cannot be
//     registered against a topic that does not exist.
//  4. Derives the Kafka principal and the default consumer group from the id.
//  5. Delegates the insert, which validates all of the above again at the persistence
//     boundary and once more in the schema's CHECK constraints.
//
// # It provisions nothing
//
// Registration touches no broker and mints no credential. A freshly registered subscriber
// is "registered, not yet provisioned": it has a boundary described but not granted, and it
// can read nothing until IssueSubscriberCredential is called. That separation is deliberate
// — registration is a cheap, reversible bookkeeping act, while provisioning writes
// authorization state to another system.
//
// Parameters:
//   - ctx context.Context: cancels the insert.
//   - registration SubscriberRegistration: the subscriber to record.
//
// Returns:
//   - *model.EventSubscriber: the stored row, carrying the database's surrogate key and
//     bookkeeping timestamps.
//   - error: a typed validation error for an unusable id, a blank name or an ungrantable
//     topic; a typed conflict when the id or the derived principal is already taken; the
//     repository's error otherwise.
func (s *EventSubscriberService) RegisterSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	store, err := s.requireStore()
	if err != nil {
		return nil, err
	}

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

	keyPrefix, err := normalizeAdvisoryKeyPrefix(registration.PartitionKeyPrefix)
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
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - limit int: page size. Non-positive selects the repository default.
//   - offset int: page offset. Negative is clamped to zero.
//
// Returns:
//   - []model.EventSubscriber: the page, newest first.
//   - error: the repository's error.
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
// # It changes the RECORD of the boundary, not the boundary
//
// The broker is a separate system of record and is not reconciled here. A widened grant
// takes effect at the next credential issuance, which binds the new topic set. A NARROWED
// grant needs more: ACL creation is additive and idempotent, so a binding for a topic that
// has just been removed from this list stands until it is explicitly deleted. Completing a
// reduction therefore means revoking the subscriber's access and re-issuing — see
// DeregisterSubscriber for the revocation half and docs/kafka-operations.md for the runbook.
// Nothing here can do it silently, and nothing should: shrinking a live grant ends a
// consumer's access and is an operator-visible act.
//
// The principal, the consumer group, the credential record and the migration instant are all
// unchangeable through this call, by construction — SubscriberUpdate has no field for any of
// them.
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key of the row to update.
//   - changes SubscriberUpdate: the fields to apply. A wholly empty update is legitimate and
//     rewrites the row with its own values, which is a harmless no-op rather than an error.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation error
//     for a blank name or an ungrantable topic, or the repository's error.
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

	subscriber, err := store.GetEventSubscriberByID(ctx, strings.TrimSpace(subscriberID))
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

	// A present empty string CLEARS the advisory prefix, which is why the column is nullable
	// and why this cannot be a plain string: NULL means "entitled to whole topics" while the
	// empty string would mean "restricted to the empty prefix", and those are opposite
	// intents. normalizeAdvisoryKeyPrefix owns that three-way mapping so registration and
	// update cannot disagree about it.
	if changes.PartitionKeyPrefix != nil {
		keyPrefix, prefixErr := normalizeAdvisoryKeyPrefix(changes.PartitionKeyPrefix)
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

	if err := store.UpdateEventSubscriber(ctx, subscriber); err != nil {
		return nil, err
	}

	logrus.WithFields(logrus.Fields{
		"subscriber":             sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		"authorized_topic_count": len(subscriber.AuthorizedTopics),
		"legacy_webhook":         subscriber.WebhookURL != nil,
	}).Info(
		"event subscriber: registry row updated; the broker-side boundary is unchanged until " +
			"credentials are re-issued",
	)

	return subscriber, nil
}

// DeregisterSubscriber removes a subscriber from the registry and ends its access at the
// broker.
//
// # The order is the one event_admin.go prescribes, and it is not interchangeable
//
// Take the row, revoke using it, and — if that fails — name the principal:
//
//  1. TakeEventSubscriber DELETES the row and RETURNS it. A plain delete would destroy the
//     principal and topic list that revocation needs; a read followed by a delete would
//     leave a window in which the row can change, so what got revoked might not be the
//     boundary that was actually in force. Taking closes both.
//  2. RevokeSubscriber removes the ACL bindings first and the SCRAM credential second, so a
//     partial failure always leaves the principal with FEWER rights rather than more.
//  3. If revocation fails, the registry no longer describes a principal that may still
//     authenticate. That is the one residue this sequence can leave, so it is reported as an
//     error AND logged at error level WITH THE PRINCIPAL, which is the only remaining record
//     of what an operator has to revoke by hand.
//
// # A deployment with no Kafka deregisters cleanly
//
// When no broker is configured there is no broker-side state, so revocation is skipped and
// the removal succeeds. That keeps the registry usable in a Kafka-less deployment, exactly as
// registration and listing are.
//
// Parameters:
//   - ctx context.Context: cancels the removal and the revocation.
//   - subscriberID string: the business key of the subscriber to remove.
//
// Returns:
//   - *model.EventSubscriber: the row that was removed, so the caller can report what it
//     revoked. Returned even when revocation failed, because it is the only description of
//     the state left behind.
//   - error: ErrSubscriberNotFound when no such subscriber exists;
//     ErrSubscriberProvisioningFailed when the row was removed but the broker-side revocation
//     did not complete; the repository's error otherwise.
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

	removed, err := store.TakeEventSubscriber(ctx, strings.TrimSpace(subscriberID))
	if err != nil {
		return nil, err
	}

	admin, err := s.provisioner()
	if err != nil {
		// The registry row is already gone. Reporting the principal here is what keeps the
		// residue actionable, and the error is returned so the caller does not read the
		// removal as complete.
		logrus.WithError(err).WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
		}).Error(
			"event subscriber: the registry row was removed but no Kafka administrative client " +
				"could be built, so the principal may still authenticate; revoke it by hand",
		)

		return removed, err
	}

	if !admin.IsConfigured() {
		logrus.WithFields(logrus.Fields{
			"subscriber": sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
			"principal":  sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
		}).Info(
			"event subscriber: deregistered; no broker is configured, so there was no " +
				"broker-side access to revoke",
		)

		return removed, nil
	}

	if err := admin.RevokeSubscriber(ctx, removed); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"subscriber":             sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
			"principal":              sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
			"authorized_topic_count": len(removed.AuthorizedTopics),
		}).Error(
			"event subscriber: the registry row was removed but revoking its Kafka access failed, " +
				"so the principal named here may still authenticate and still read; revoke it by hand",
		)

		return removed, apierror.NewAPIError(
			apierror.ErrSubscriberProvisioningFailed,
			"The subscriber was removed but its Kafka access could not be revoked",
			fmt.Errorf(
				"blnk: revoking Kafka access for principal %q after removing subscriber %q: %w",
				removed.KafkaPrincipal, removed.SubscriberID, err,
			),
		)
	}

	logrus.WithFields(logrus.Fields{
		"subscriber": sanitizeLogValue(removed.SubscriberID, maxLoggedFilterLength),
		"principal":  sanitizeLogValue(removed.KafkaPrincipal, maxLoggedFilterLength),
	}).Info("event subscriber: deregistered and Kafka access revoked")

	return removed, nil
}

// ---------------------------------------------------------------------------------------
// Credential issuance
//
// This is the operation behind POST /subscribers/{id}/kafka-credentials, and the one place
// in Blnk where a SASL secret comes into existence.
// ---------------------------------------------------------------------------------------

// IssueSubscriberCredential mints a subscriber's SASL/SCRAM credential, binds its ACLs, and
// returns the secret EXACTLY ONCE.
//
// # The 5-second budget is enforced here, explicitly
//
// The whole operation runs under context.WithTimeout(ctx, SubscriberCredentialIssuanceBudget)
// and that derived context is passed into every step, so the ceiling covers the four possible
// broker round trips AND the database write together. Relying on the admin client's own
// per-request timeout would not do: four ten-second timeouts serialised is forty seconds
// while every individual call looks healthy. A broker that accepts a connection and then
// stops answering therefore fails this call at five seconds instead of holding the request
// open indefinitely.
//
// A caller whose own context expires sooner still wins, because WithTimeout only ever
// shortens.
//
// # Nothing is generated for a request that cannot succeed
//
// The broker configuration is checked BEFORE a secret is drawn. With no brokers configured
// the answer is ErrKafkaUnavailable — 503 — and no random value is generated, no reference is
// derived and no row is touched. Generating a secret that could never be provisioned would
// mean the process briefly held a credential nobody could ever use.
//
// # Re-issuance is supported, defined, and destructive to the previous secret
//
// A second call mints a NEW secret and a new reference, and overwrites credential_reference
// and credential_issued_at. The PREVIOUS SECRET STOPS WORKING IMMEDIATELY: Kafka stores one
// SCRAM credential per principal, so the upsert replaces it, and a consumer still
// authenticating with the old one begins failing at its next handshake. That is reported
// through SubscriberCredential.Replaced so a caller can say so, and logged so an operator can
// see it. It is never a silent no-op and the old secret is never returned — both would be
// worse than the disruption, because the caller would believe it held a working credential.
//
// The consumer group and the principal are UNCHANGED by a re-issue: both are derived from the
// immutable subscriber id, so a rotation replaces the secret and nothing else, and the
// subscriber's existing group offsets and ACL bindings continue to apply.
//
// # Partial failure has one documented outcome
//
// event_admin.go compensates a failure that lands between the credential and its ACLs: if
// the bindings cannot be created after the credential was written, it revokes the credential
// before returning, so the broker is left CLEAN rather than holding a principal that can
// authenticate with no boundary. This method therefore never records an issuance on any error
// path, and it distinguishes the two states in its logging — compensated (nothing to do) from
// compensation-failed (a live principal needs manual revocation, and the log line names it).
// Both are answered with ErrSubscriberProvisioningFailed, which is 503, because provisioning
// depends on an external system and the caller's correct response is to retry.
//
// # The secret goes to the caller and nowhere else
//
// Only a NON-REVERSIBLE reference (model.DeriveCredentialReference: HMAC-SHA-256 keyed by the
// principal) and the issuance instant are persisted. The plaintext is returned inside
// SubscriberCredential, is never written to the database, never logged, never placed in an
// error message and never attached to a span or a metric label. There is no route by which it
// can be read again; a lost password can only be replaced.
//
// Parameters:
//   - ctx context.Context: the caller's context. It is wrapped in the issuance budget, so a
//     caller supplying no deadline still gets one.
//   - subscriberID string: the business key of the subscriber to provision.
//
// Returns:
//   - SubscriberCredential: the connection details and the one-time secret. Zero-valued on
//     every error path, so a failed issuance cannot hand back a partial credential.
//   - error: ErrSubscriberNotFound (404) when no such subscriber exists; ErrKafkaUnavailable
//     (503) when no broker is configured, the client cannot be built, or the budget expired;
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

	subscriber, err := store.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		return SubscriberCredential{}, err
	}

	// The reference OBSERVED before provisioning. It is what makes the issuance record
	// conditional: if another issuance for this subscriber commits in the meantime, this
	// value no longer matches and the write reports a conflict instead of silently
	// overwriting a record whose secret is the one that works.
	observedReference := subscriber.CredentialReference

	password, err := s.newPassword()
	if err != nil {
		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to generate a SASL credential for the subscriber",
			err,
		)
	}

	// Derived from the principal and the secret, before the secret leaves this function's
	// control, so that what is persisted is a digest and never the value itself.
	reference, err := model.DeriveCredentialReference(subscriber.KafkaPrincipal, password)
	if err != nil {
		return SubscriberCredential{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to derive the subscriber credential reference",
			err,
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
		return SubscriberCredential{}, s.recordFailure(ctx, admin, subscriber, err)
	}

	credential := SubscriberCredential{
		SubscriberID:     subscriber.SubscriberID,
		Brokers:          admin.Brokers(),
		BrokerEndpoint:   strings.Join(admin.Brokers(), ","),
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
//
// Returns:
//   - string: the generated secret.
//   - error: the generator's error.
func (s *EventSubscriberService) newPassword() (string, error) {
	if s == nil || s.generatePassword == nil {
		return generateSubscriberPassword()
	}

	return s.generatePassword()
}

// provisioningFailure turns a broker-side provisioning failure into the right typed error and
// records what state the broker was left in.
//
// # Four outcomes, and each needs a different answer
//
//   - NOT CONFIGURED. The broker list emptied between the check and the call, or an injected
//     client reports it. 503 ErrKafkaUnavailable, and nothing was written.
//   - BUDGET EXPIRED or CALLER CANCELLED. 503 ErrKafkaUnavailable, because a broker that did
//     not answer within the budget is unreachable as far as this request is concerned. What
//     the broker did or did not write is genuinely unknown, which the log line says: the
//     remedy is to retry, and a retry re-provisions the same boundary idempotently.
//   - COMPENSATED. The credential was written, the ACL grant failed, and event_admin.go
//     revoked the credential — so the broker is clean and the secret generated here is dead.
//     Logged as a warning, not an error: the system is in a consistent state and a retry is
//     all that is needed.
//   - COMPENSATION FAILED. The credential was written, the bindings failed AND the revocation
//     failed, so a principal exists that can authenticate with no boundary. This is the one
//     state that needs a human, so it is logged at ERROR with the principal named, which is
//     the only record of what has to be revoked by hand.
//
// No branch records an issuance, and no branch returns a credential: a caller that receives
// an error holds no secret, and the registry continues to describe whatever it described
// before.
//
// No message contains the password. The password is not a field of the result and is never
// passed to this function.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row being provisioned, for the log fields.
//   - result SubscriberProvisioningResult: partially populated on failure, and the only
//     source of truth for what reached the broker.
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
			cause,
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
			cause,
		)

	case errors.Is(cause, context.Canceled):
		logrus.WithFields(fields).Warn(
			"event subscriber: credential issuance was cancelled before it completed; whether the " +
				"broker wrote the credential is unknown",
		)

		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"Provisioning Kafka credentials was cancelled before it completed",
			cause,
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

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningFailed,
		"Failed to provision Kafka credentials for the subscriber",
		cause,
	)
}

// recordFailure handles the narrow window in which the broker holds a credential the registry
// did not manage to record.
//
// # Three cases, and only two of them may revoke
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
// Parameters:
//   - ctx context.Context: the issuance context, already carrying the budget. A revocation
//     attempted after the budget expired will fail fast, which is logged and tolerated.
//   - admin subscriberPrincipalProvisioner: the client that provisioned.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - cause error: the repository's error, already typed.
//
// Returns:
//   - error: the repository's error, unchanged, so its code and message survive to the
//     response.
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

	if err := admin.RevokeSubscriber(ctx, subscriber); err != nil {
		logrus.WithError(err).WithFields(fields).Error(
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
	if s.clearCredentialRecord(ctx, subscriber, fields) {
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
//   - ctx context.Context: cancels the write.
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
// It is the counterpart to IssueSubscriberCredential and the operation to reach for when a
// secret is suspected of being compromised but the subscriber itself is staying: the
// credential and its bindings go, the registry row and its topic grant remain, and a single
// re-issue restores access.
//
// # The order over-reports rather than under-reports
//
// Broker first, registry second — which is what Datasource.ClearSubscriberCredential's
// documentation prescribes. Broker-side deletion is what actually ends access, so doing it
// first means a failure between the two leaves the registry claiming a credential that no
// longer exists: it over-reports access, which is visible and harmless, instead of hiding a
// credential that still works.
//
// Clearing a subscriber that holds no credential succeeds, because the desired end state is
// already true. A missing subscriber is still an error.
//
// Parameters:
//   - ctx context.Context: cancels the revocation and the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - error: ErrSubscriberNotFound when no such subscriber exists; ErrKafkaUnavailable when no
//     administrative client can be built; ErrSubscriberProvisioningFailed when the broker
//     refused the revocation; the repository's error when the record could not be cleared.
func (s *EventSubscriberService) RevokeSubscriberCredential(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

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
			logrus.WithError(err).WithFields(logrus.Fields{
				"subscriber": sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
				"principal":  sanitizeLogValue(subscriber.KafkaPrincipal, maxLoggedFilterLength),
			}).Error(
				"event subscriber: revoking the subscriber's Kafka access failed, so the registry " +
					"record was left untouched and the credential may still work; retry the revocation",
			)

			return apierror.NewAPIError(
				apierror.ErrSubscriberProvisioningFailed,
				"Failed to revoke the subscriber's Kafka access",
				fmt.Errorf(
					"blnk: revoking Kafka access for principal %q: %w", subscriber.KafkaPrincipal, err,
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
// The dual-run migration surface (AMBIGUITY-1)
//
// These four operations exist ONLY for the 30-day window during which Kafka publishing and
// legacy HTTP webhook delivery run side by side from the same outbox events. They record
// where a subscriber used to receive pushes, when it finished moving, and — once a retention
// period has passed — forget the endpoint.
//
// They are the reason the sunset is observable. The requirement to migrate subscribers off
// "the webhook subscription REST API" meets a repository whose entire subscription surface is
// one global configuration value, so without a per-subscriber record there is no route on
// which a 410 Gone could ever be seen after the sunset date.
//
// AGAIN, BECAUSE IT IS THE EASIEST THING TO GET WRONG: /hooks is NOT this surface. Those are
// the PRE_TRANSACTION and POST_TRANSACTION request-time callouts in internal/hooks, they carry
// a response contract that can influence transaction processing, they stay fully functional,
// and nothing here touches them.
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
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, with no recorded endpoint.
//   - error: ErrSubscriberNotFound, or the repository's write error.
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
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - migratedBefore time.Time: purge subscribers whose migration completed strictly before
//     this instant.
//
// Returns:
//   - int64: how many rows were purged.
//   - error: the repository's error, including its refusal of a zero cut-off.
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
// The pattern is the one event_dlt.go established. Operations that touch only the registry
// build a service, use it, and let it go — no administrative client is resolved, so there is
// nothing to release, and they work with the broker down. Operations that reach the broker
// build a service, use it, and CLOSE it, so the client such a service resolves for itself is
// released rather than held until the process ends.
//
// Nothing caches a service on the Blnk instance, because that would put administrative-client
// lifetime inside a struct whose Close does not own it.
// ---------------------------------------------------------------------------------------

// EventSubscribers returns a subscriber service bound to this instance's datasource.
//
// The returned service resolves an administrative client from live configuration the first
// time one is needed, so a CALLER THAT ISSUES OR REVOKES MUST CLOSE IT — `defer
// service.Close()` — or the connections that client opens are held until the process ends. A
// caller doing registry work only may ignore Close: nothing is resolved and nothing is held.
//
// It is nil-safe: a nil instance yields a service whose operations report an unavailable
// registry rather than panicking.
//
// Returns:
//   - *EventSubscriberService: a ready service the caller owns.
func (b *Blnk) EventSubscribers() *EventSubscriberService {
	if b == nil {
		return NewEventSubscriberService(nil, nil)
	}

	return NewEventSubscriberService(b.datasource, nil)
}

// RegisterEventSubscriber records a new subscriber. It is the write behind POST /subscribers.
//
// No broker is touched: registration describes a boundary, it does not grant one.
//
// Parameters:
//   - ctx context.Context: cancels the insert.
//   - registration SubscriberRegistration: the subscriber to record.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: as EventSubscriberService.RegisterSubscriber.
func (b *Blnk) RegisterEventSubscriber(
	ctx context.Context,
	registration SubscriberRegistration,
) (*model.EventSubscriber, error) {
	return b.EventSubscribers().RegisterSubscriber(ctx, registration)
}

// GetEventSubscriber reads one subscriber. It is the read behind
// GET /subscribers/:subscriber_id.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the stored row.
//   - error: as EventSubscriberService.GetSubscriber.
func (b *Blnk) GetEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	return b.EventSubscribers().GetSubscriber(ctx, subscriberID)
}

// ListEventSubscribers pages the registry. It is the read behind GET /subscribers.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//   - limit int: page size; non-positive selects the repository default.
//   - offset int: page offset; negative is clamped.
//
// Returns:
//   - []model.EventSubscriber: the page, newest first.
//   - error: as EventSubscriberService.ListSubscribers.
func (b *Blnk) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	return b.EventSubscribers().ListSubscribers(ctx, limit, offset)
}

// UpdateEventSubscriber applies the mutable subset of a subscriber. It is the write behind
// PUT /subscribers/:subscriber_id.
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key.
//   - changes SubscriberUpdate: the fields to apply.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: as EventSubscriberService.UpdateSubscriber.
func (b *Blnk) UpdateEventSubscriber(
	ctx context.Context,
	subscriberID string,
	changes SubscriberUpdate,
) (*model.EventSubscriber, error) {
	return b.EventSubscribers().UpdateSubscriber(ctx, subscriberID, changes)
}

// DeregisterEventSubscriber removes a subscriber and revokes its Kafka access. It is the write
// behind DELETE /subscribers/:subscriber_id.
//
// The short-lived administrative client this resolves is closed before returning, so the
// handler needs no lifecycle handling of its own.
//
// Parameters:
//   - ctx context.Context: cancels the removal and the revocation.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row that was removed.
//   - error: as EventSubscriberService.DeregisterSubscriber.
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
//
// Parameters:
//   - ctx context.Context: the request context.
//   - subscriberID string: the business key.
//
// Returns:
//   - SubscriberCredential: the connection details and the one-time secret.
//   - error: as EventSubscriberService.IssueSubscriberCredential.
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
//
// Parameters:
//   - ctx context.Context: cancels the revocation and the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - error: as EventSubscriberService.RevokeSubscriberCredential.
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
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: as EventSubscriberService.RecordLegacyWebhookSubscription.
func (b *Blnk) RecordSubscriberWebhookSubscription(
	ctx context.Context,
	subscriberID, webhookURL string,
) (*model.EventSubscriber, error) {
	return b.EventSubscribers().RecordLegacyWebhookSubscription(ctx, subscriberID, webhookURL)
}

// ClearSubscriberWebhookSubscription removes a subscriber's recorded legacy endpoint. It is
// the write behind DELETE /subscribers/:subscriber_id/webhook-subscription.
//
// Deprecated: see RecordSubscriberWebhookSubscription.
//
// Parameters:
//   - ctx context.Context: cancels the read and the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands.
//   - error: as EventSubscriberService.ClearLegacyWebhookSubscription.
func (b *Blnk) ClearSubscriberWebhookSubscription(
	ctx context.Context,
	subscriberID string,
) (*model.EventSubscriber, error) {
	return b.EventSubscribers().ClearLegacyWebhookSubscription(ctx, subscriberID)
}

// MarkEventSubscriberMigrated records that a subscriber has completed its move to Kafka
// consumption.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - time.Time: the instant recorded, in UTC.
//   - error: as EventSubscriberService.MarkSubscriberMigrated.
func (b *Blnk) MarkEventSubscriberMigrated(ctx context.Context, subscriberID string) (time.Time, error) {
	return b.EventSubscribers().MarkSubscriberMigrated(ctx, subscriberID)
}

// PurgeMigratedSubscriberWebhookURLs forgets the legacy endpoints of subscribers that migrated
// before a cut-off.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - migratedBefore time.Time: the retention cut-off, chosen by the caller.
//
// Returns:
//   - int64: how many rows were purged.
//   - error: as EventSubscriberService.PurgeMigratedWebhookURLs.
func (b *Blnk) PurgeMigratedSubscriberWebhookURLs(ctx context.Context, migratedBefore time.Time) (int64, error) {
	return b.EventSubscribers().PurgeMigratedWebhookURLs(ctx, migratedBefore)
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
