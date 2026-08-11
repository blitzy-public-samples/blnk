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

// event_subscriber.go is the subscriber registry and the one-time credential issuance behind
// POST /subscribers/{id}/kafka-credentials (requirement R-7).
//
// # The access model, stated once
//
// THERE ARE NO PER-TENANT TOPICS. Blnk owns four category topics and a `.dlt` sibling for each
// (event_topics.go). THREE of those categories — transactions, balances and identities — may be
// granted to a subscriber. `<prefix>.system` may NOT, and no dead-letter topic may ever be,
// whatever its category: the system topic carries system.error's frozen body, which renders
// verbatim internal error text, and it is the catalogue's catch-all, so it is an operator
// surface rather than a subscriber one. That decision lives in
// model.SubscriberGrantableEventCategories and is enforced identically by the request DTO, the
// persistence boundary and the ACL provisioner. A subscriber is granted a SUBSET of the
// grantable set, whatever its registry row authorises, and isolation comes from four things:
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
// see model.EventSubscriber's documentation, which names it and the declared-gateway
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
//
// WHAT IT BOUNDS IS THE ATTEMPT, NOT THE WHOLE REQUEST, and the distinction is worth
// stating because the requirement is quoted as a wall-clock promise. A successful issuance
// completes inside it. A FAILING one may spend this budget and then a COMPENSATION budget on
// top: compensating writes run synchronously on fresh detached contexts, precisely because an
// expired deadline is one of the commonest reasons they are needed (CLEAN-01), and their own
// ceilings are subscriberCleanupBudget here and kafkaCleanupBudget in event_admin.go. Trimming
// compensation to fit inside five seconds would make it fail more often than it succeeds — the
// broker dial timeout alone is a meaningful fraction of it — and the residue it exists to
// remove is a live SASL credential the registry does not record. A slow error response is the
// better failure. Callers set their own deadline accordingly, and the operator-facing bound is
// documented in docs/kafka-operations.md rather than left to be inferred from this constant.
const SubscriberCredentialIssuanceBudget = 5 * time.Second

// subscriberCompensationReserve is how much of the issuance budget is HELD BACK so that a
// failure can still be compensated inside the same absolute deadline.
//
// # The trade this makes, stated plainly
//
// The forward path gets the budget MINUS this, so a broker that needs more than 3.5 seconds to
// provision now fails where it previously succeeded at up to 5. That is a real cost and it is
// the right one, because the alternative costs more: without a reserve, the one failure that
// most needs compensating — the deadline expiring — is the one with no time left to compensate
// in, and AUTH-01 is exactly about not leaving a live credential behind that failure. A broker
// taking over 3.5 seconds for four round trips is not healthy, and the operator's answer to it
// is to fix the broker and retry, which the refusal tells them to do.
//
// 1.25 seconds covers what compensation actually does: two broker round trips (delete the ACL
// bindings, delete the SCRAM credential) and up to two local writes (clear the credential
// record, release the provisioning fence). Against a broker that is REFUSING rather than
// hanging — the ordinary shape of the failure being compensated — that is ample. Against one
// that is hanging, compensation is abandoned when the deadline passes, is logged at ERROR with
// the principal named, and is the manual-revocation case the runbook covers. That was already
// the outcome when the old ten-second cleanup budget expired; what has changed is that the
// caller is no longer made to wait for it.
const subscriberCompensationReserve = 1250 * time.Millisecond

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

	// CompensationPending reports that the credential's removal has been SCHEDULED and its
	// outcome was not known when this response was written. PERF-P09.
	//
	// It is the honest third state, and it exists because the alternatives are both wrong.
	// Reporting Compensated would claim a clean broker nobody had confirmed; reporting
	// neither would read as "the revocation failed", which is the one state that needs a
	// human. Pending means: retry — the retry is refused with a conflict until the cleanup
	// finishes, and the cleanup's own failure is logged at ERROR with the principal named.
	CompensationPending bool `json:"compensation_pending,omitempty"`
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
	ListEventSubscribers(ctx context.Context, query model.SubscriberPageQuery) (model.SubscriberPage, error)

	// ListAndCountEventSubscribers pages the registry and counts it from ONE SNAPSHOT, and is
	// what a listing that asked for a total reads. Two reads on two connections observe two
	// registries, so a subscriber registered between them makes the total describe a set the
	// page is not a slice of.
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
	//
	// It returns the row as STORED after the write, so a caller never answers with the
	// pre-update updated_at it read on the way in.
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
	// migration instant in ONE statement, so a failure cannot leave the row absent from both
	// sides of the migration report.
	CompleteSubscriberWebhookMigration(ctx context.Context, subscriberID string, migratedAt time.Time) (*model.EventSubscriber, error)
	// RecordSubscriberWebhookURL records a legacy endpoint and clears migrated_at in one
	// statement, so a row can never be migrated and still carry a live URL.
	RecordSubscriberWebhookURL(ctx context.Context, subscriberID, webhookURL string) error

	// ClearSubscriberWebhookURL forgets one subscriber's endpoint without claiming it
	// migrated.
	ClearSubscriberWebhookURL(ctx context.Context, subscriberID string) error

	// MarkSubscriberCredentialOrphaned records that a credential exists at the broker for a
	// subscriber whose registry row does not account for it. Unfenced deliberately: it is the
	// compensating write on a path whose claim may already be lost, and the exposure must be
	// recorded either way.
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

	// RenewSubscriberProvisioningFence extends a claim the caller still holds. It is called
	// immediately BEFORE each broker phase: a successful renewal proves ownership at that
	// instant, so the phase cannot interleave with another operation's, and a refused one
	// means the operation must be abandoned rather than continued.
	RenewSubscriberProvisioningFence(ctx context.Context, subscriberID string, token string, lease time.Duration) error

	// RecordSubscriberGrantReconcilePending marks a subscriber as owing a broker-side grant
	// reconciliation. It is written BEFORE the broker work, so the obligation survives an
	// outcome nobody is left to record.
	RecordSubscriberGrantReconcilePending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// ClearSubscriberGrantReconcilePending discharges that obligation, once the row and the
	// broker are known to agree.
	ClearSubscriberGrantReconcilePending(ctx context.Context, subscriberID, fenceToken string) error

	// RecordSubscriberCredentialCleanupPending marks a subscriber as owing a credential
	// cleanup: a SCRAM credential may exist that Blnk intended to destroy, or the row may
	// name one that no longer works.
	RecordSubscriberCredentialCleanupPending(ctx context.Context, subscriberID string, pendingAt time.Time, fenceToken string) error

	// GetSubscriberSettlementObligation reads what ONE subscriber currently owes. The flags
	// are re-read under the claim rather than carried from a scan, because a successful
	// re-issuance discharges the credential-cleanup obligation in between.
	GetSubscriberSettlementObligation(ctx context.Context, subscriberID string) (model.SubscriberSettlementObligation, error)

	// ListSubscriberSettlementObligations returns the subscribers owing broker-side work,
	// oldest attempt first, skipping any attempted more recently than the caller's bound.
	ListSubscriberSettlementObligations(ctx context.Context, limit int, notBefore time.Time) ([]model.SubscriberSettlementObligation, error)

	// MarkSubscriberSettlementAttempt records that a settlement pass tried a row and what
	// happened. It does not discharge anything.
	MarkSubscriberSettlementAttempt(ctx context.Context, subscriberID string, attemptedAt time.Time, failure string) error
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
	// CompensateProvisioning performs the compensation a DEFERRED provisioning failure
	// reported instead of running (PERF-P09), and is a no-op for a result that owes none.
	//
	// It is in this seam because the compensation now happens on the service's schedule
	// rather than inside the provisioning call: the two round trips it makes must not be
	// added to a response whose five-second budget has usually just expired.
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
	// Every entry must be one of the four Blnk category topics under the configured
	// prefix. No dead-letter topic is grantable — see validateSubscriberGrant.
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
// leaves whatever the row records, while a present empty string CLEARS the recorded scope —
// which is how an operator says "topic-level access is what I want" and how the credential
// response comes to report no key scope at all. A plain string cannot express the difference,
// so it would make silently inverting an operator's intent possible.
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

	// PartitionKeyPrefix replaces the recorded key prefix when non-nil; a present empty
	// string CLEARS it. It grants and revokes NOTHING at the broker, and it does not gate
	// credential issuance either.
	//
	// IT IS A ROUTING HINT AND NOT AN ACCESS BOUNDARY, and it is accepted on a subscriber that
	// already holds a credential. Kafka's authorizer has no message-key resource dimension, so
	// no credential Blnk can mint is confined to the records whose key carries a prefix — which
	// means recording one neither narrows the live credential nor can be made to. Refusing the
	// combination was the previous behaviour and it withdrew the mandatory credential capability
	// from any subscriber that recorded a prefix; the boundary is now STATED in every response
	// through enforced_access instead, where a reader cannot miss it.
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

	// PartitionKeyPrefix is the routing hint recorded on the subscriber, echoed here so the
	// response can state it beside the declaration that the broker does not enforce it.
	//
	// It is carried on the credential rather than re-read from the row by the handler for one
	// reason: the value that reaches the response must be the value that was in force when the
	// credential was minted. A second read could see a different one, and the response would
	// then pair a credential with a prefix it was not issued under.
	//
	// It is returned so the subscriber receives the filtering contract it is expected to
	// apply, in the one response that also carries the credential. Empty when none is
	// recorded.
	PartitionKeyPrefix string

	// Fingerprint is the short, non-sensitive form of the stored credential reference. It
	// is what a client uses to tell one issuance from another and is not a secret; it
	// cannot be authenticated with and the reference cannot be recovered from it.
	Fingerprint string

	// Replaced reports that the principal already held a SCRAM credential and this
	// issuance replaced it. An operator reading it knows an existing consumer's credential
	// has just stopped working.
	Replaced bool

	// KeyScopeEnforcement says WHICH COMPONENT enforces the recorded scope, and it travels with
	// the prefix rather than being derivable from it. broker_gateway — the same word the
	// deployment declares in KAFKA_KEY_SCOPE_ENFORCEMENT — means the subscriber was granted
	// Describe but NOT Read on its topics, so the broker refuses every direct fetch and its
	// records are delivered, key-filtered, by that declared component; none means no scope was
	// recorded and the topic and group ACLs are the entire boundary, which the broker keeps in
	// full.
	//
	// It is never reported for a key-scoped row while no component is declared, because no
	// credential exists to report it on: issuance answers SUBSCRIBER_KEY_SCOPE_UNENFORCED.
	//
	// Its value was "consumer_side" until the isolation correction, and the pair was what
	// replaced refusing to issue at all. Both of those were answers to the same question and
	// both were wrong: refusing withheld the only credential such a row can have, and
	// disclosing an unenforced prefix left every record on the shared topic readable by a
	// subscriber the registry described as narrowed. The credential is issued, and the boundary
	// is kept where it can be kept.
	KeyScopeEnforcement model.KeyScopeEnforcementStatus

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

// KeyScope reports this credential's key boundary in the form a consumer is told it, together
// with whether THE BROKER keeps that boundary.
//
// # Why the two facts are one call
//
// A caller must not be able to read the prefix without reading who enforces it. Delivered on its
// own, a prefix says nothing about where it is applied. Returning the pair means a response
// cannot state the boundary while omitting the component that keeps it.
//
// # The two shapes it reports
//
// A recorded prefix is returned verbatim with enforcedByBroker FALSE, and the false is precise
// rather than discouraging: Kafka's authorizer has no message-key dimension, so the broker
// cannot evaluate this scope — which is exactly why such a credential is granted no topic Read
// and its records are delivered, key-filtered, by the declared key-authorising component. The
// boundary IS enforced; the broker is not what enforces it, and KeyScopeEnforcement on this
// credential names the component that does.
//
// No recorded prefix is reported as model.SubscriberKeyScopeAllKeys with enforcedByBroker TRUE,
// because in that case the topic grant IS the whole boundary and the broker does keep all of it.
// That second reading is stated rather than left as an empty string for a caller to interpret.
//
// It is DERIVED from PartitionKeyPrefix rather than stored beside it, so the pair cannot
// disagree: a stored flag would eventually be set from one place and the prefix from another.
//
// Returns:
//   - scope string: the recorded prefix, or model.SubscriberKeyScopeAllKeys when none is recorded.
//   - enforcedByBroker bool: false exactly when a narrowing prefix is recorded, which is when the
//     declared component enforces it instead.
func (c SubscriberCredential) KeyScope() (scope string, enforcedByBroker bool) {
	trimmed := strings.TrimSpace(c.PartitionKeyPrefix)
	if trimmed == "" {
		return model.SubscriberKeyScopeAllKeys, true
	}

	// FALSE, and the boolean means what it is named: the BROKER does not enforce this. Kafka's
	// authorizer has no message-key dimension, so no ACL evaluates the prefix — which is why such
	// a credential holds no topic Read at all and its records are delivered, key-filtered, by the
	// declared component. The boundary IS enforced, and KeyScopeEnforcement on this credential
	// names the component that enforces it. Reporting true here because SOMETHING enforces the scope
	// would state a broker boundary Kafka does not keep, which is the disclosure this pair of
	// return values exists to prevent.
	return trimmed, false
}

// issuedKeyScopeEnforcement resolves the enforcement point to record on an ISSUED credential.
//
// It differs from EventSubscriber.KeyScopeEnforcement in exactly one way, and the difference is
// the reason it exists: that method reads a registry row, which knows nothing about whether the
// deployment has declared an enforcing component, so it answers where a recorded prefix WOULD be
// enforced. A credential, by contrast, only exists where issuance permitted it, and issuance
// refuses a recorded prefix while no component is declared — so on a credential a recorded prefix
// implies a running one.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row being issued for. May be nil, which reports
//     none.
//   - enforced bool: whether config.KafkaConfig.KeyScopeGateway reported active enforcement,
//     read once by the caller so the refusal and this value cannot disagree.
//
// Returns:
//   - model.KeyScopeEnforcementStatus: broker_gateway for a key-scoped subscriber under active
//     enforcement, otherwise whatever the row itself reports.
func issuedKeyScopeEnforcement(
	subscriber *model.EventSubscriber,
	enforced bool,
) model.KeyScopeEnforcementStatus {
	if enforced && subscriber.DeclaresKeyScope() {
		return model.KeyScopeEnforcementGateway
	}

	return subscriber.KeyScopeEnforcement()
}

// LogFields is the safe projection of an issuance for structured logging.
//
// Every value here is non-secret by construction: the fingerprint is a 48-bit prefix of a
// keyed digest, the topics and the group are already public to the subscriber, and the
// password appears only as its LENGTH. Logging through this rather than the struct is what
// keeps an issuance auditable without making it disclosable.
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
		// key-scope line, so a search over issuance records can separate the subscribers whose
		// records are delivered through the declared component from the ones whose whole
		// boundary is broker-enforced. The prefix VALUE is not repeated here — it is
		// caller-supplied text, and the line logKeyScopeDisclosure emits alongside this one
		// carries it sanitized, once. On an ISSUED credential this reads "broker_gateway" for a
		// row recording a prefix and "none" otherwise; it never reads consumer_side, because a
		// credential is minted only where an enforcement point is declared.
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

	// resolveAdmin resolves the PROCESS-WIDE administrative client, when one is available.
	//
	// It is a resolver rather than a client so that installing it costs nothing: a registry
	// read never calls it, so a deployment whose Kafka configuration is broken does not log
	// a client-construction failure on every GET. See provisioner.
	resolveAdmin func() (subscriberPrincipalProvisioner, error)

	// deferWork schedules a compensating task OFF the response path.
	//
	// Nil runs the task inline, which is both the historical behaviour and the right default
	// for a hand-built service: a CLI or a test wants the cleanup finished before the call
	// returns, and determinism matters more there than latency. Production installs a
	// scheduler, so the five-second issuance budget bounds the RESPONSE rather than the
	// response plus every cleanup that followed it.
	deferWork func(func())

	// keyScopeGateway is the control-plane client for the declared key-authorising component.
	//
	// Nil is the normal state, and it does NOT mean "no gateway": the client is built from
	// live configuration on demand by keyScopeGatewayClient, for the same reason
	// keyScopeEnforcement reads configuration rather than a captured copy — a value captured
	// when the service was constructed would describe the deployment as it stood then, and the
	// enforcement fact and the client that verifies it must come from one read.
	//
	// Assigning it is for TESTS, through WithKeyScopeGateway: it is what lets a conformance
	// double stand in for a component this repository does not ship.
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

// WithKeyScopeGateway injects the control-plane client for the declared key-authorising
// component.
//
// It exists for TESTS, and it is the seam that makes SEC-01's enforcement point exercisable end
// to end: Blnk does not ship a record-filtering gateway, so the only way to prove that issuance
// binds, attests, refuses a mismatch and revokes is to stand a conformance double in its place.
//
// Passing nil clears an injected client and returns the service to building one from live
// configuration, which is what production does.
//
// Parameters:
//   - gateway KeyScopeGatewayClient: the client to use, or nil to resolve from configuration.
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
// # Why it is resolved per call rather than held
//
// The same reason keyScopeEnforcement re-reads configuration: the enforcement FACT and the
// client that verifies it must come from one read of one configuration. A client captured at
// construction would keep attesting against an endpoint the deployment had since changed, and —
// worse — could exist while enforcement was reported inactive, which is the combination that
// mints a credential nothing was asked about.
//
// It is cheap. The client is a struct with an http.Client whose transport pools four idle
// connections; issuance is not a hot path, and building one per credential costs less than the
// TLS handshake the call itself performs.
//
// Returns:
//   - KeyScopeGatewayClient: the injected client when a test installed one, otherwise a client
//     built from live configuration.
//   - error: ErrKeyScopeGatewayNotConfigured when no usable endpoint is declared. The caller
//     turns that into the state refusal an operator can act on; it is deliberately NOT wrapped
//     here, so a caller can compare against the sentinel.
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

// attestKeyScope requires the declared component to confirm a subscriber's recorded key scope
// before anything is minted.
//
// # Where this sits, and why
//
// It runs after the row is read and the enforcement fact is resolved, and BEFORE a password
// exists or the broker is touched — the same position subscriberFacingBrokers occupies, and for
// the same reason: a refusal there leaves no residue anywhere. There is no credential to revoke,
// no ACL to prune, no registry write to undo, and no secret that has to be treated as
// compromised because it was generated and then abandoned.
//
// # Why a missing client is a refusal rather than a skip
//
// The caller only reaches this when enforcement is ACTIVE, and enforcement is active only when
// config.KafkaConfig.KeyScopeAttestation reports a usable endpoint — so the client cannot be
// missing here unless configuration changed between the two reads. Treating that as a skip would
// mint exactly the credential this whole path exists to prevent, so it is a refusal.
//
// Parameters:
//   - ctx context.Context: the forward-path context, already bounded by the issuance budget.
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
// # Best effort, and deliberately so
//
// The authoritative revocation is the BROKER's: deleting the SCRAM credential ends the
// principal's ability to authenticate anywhere the credential was accepted, gateway included,
// because the gateway terminates the same SASL exchange. This call exists so the component does
// not accumulate bindings for principals that no longer exist, which is hygiene and audit
// rather than access.
//
// So a failure is LOGGED AND RETURNED to the caller as information, never allowed to fail a
// deregistration whose broker half has already succeeded. Failing it would leave an operator
// unable to remove a subscriber while a component is down, in order to complete a cleanup for
// access that is already gone.
//
// A deployment with no gateway declared has nothing to revoke and this is a no-op.
//
// Parameters:
//   - ctx context.Context: a bounded cleanup context.
//   - subscriber *model.EventSubscriber: the row being deregistered. May be nil.
//
// Returns:
//   - error: the component's failure, for the caller to log. Nil when there was nothing to do
//     or the binding was withdrawn.
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

	// THE PROCESS-WIDE CLIENT FIRST, when a resolver was installed. It is remembered locally so
	// that an operation resolving twice — UpdateSubscriber prunes and then grants — does not ask
	// again, and it is recorded as NOT OWNED, because the process owns it and closing it here
	// would take the transport away from every later request.
	//
	// A resolver failure falls through to building a client of this service's own rather than
	// being returned: the resolver's failure modes are exactly this constructor's, so the
	// fallback produces the same typed error with the same log line, and a caller cannot end up
	// with a worse diagnosis because a shared client happened to be unavailable.
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

// compensationReserve is how much of the budget this service holds back for compensation.
//
// It scales with the budget rather than being fixed, because the budget is a test seam: a test
// that pins issuance to 50ms would otherwise reserve 1.25 seconds out of it and hand the
// forward path a deadline that has already passed, so every such test would fail on a timeout
// it never asked for. A QUARTER of the budget keeps the same proportion at any size, and at the
// production budget of five seconds it is the documented 1.25.
//
// The reserve is capped at subscriberCompensationReserve so a deployment that RAISES the budget
// does not hand compensation more time than the two round trips and two writes it performs
// actually need.
//
// Returns:
//   - time.Duration: the slice of the budget kept back, always less than the budget itself.
func (s *EventSubscriberService) compensationReserve() time.Duration {
	reserve := s.budget() / 4
	if reserve > subscriberCompensationReserve {
		return subscriberCompensationReserve
	}

	return reserve
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
// model — the principal, the consumer group — is derived from the subscriber's identity; this
// one cannot be, because it lives in a different namespace. A Kafka message key on Blnk's
// topics is the outbox row's STORED PARTITION KEY, which is the LEDGER ID for every
// ledger-scoped event (transactions included) and otherwise an identity id, a batch id or the
// event type. A prefix derived from the subscriber's own identifier would therefore match no
// record ever produced, and a subscriber filtering on it would silently discard its entire
// stream. The prefix is a statement about which LEDGERS a subscriber is authorised for, which
// only the caller registering it can make.
//
// # A NON-EMPTY RESULT NAMES A BOUNDARY THE BROKER CANNOT KEEP
//
// Kafka's authorizer has no message-key dimension: there is no ACL that restricts a consumer
// to a slice of a topic by key. A subscriber granted Read on a topic can read every record on
// it regardless of this value.
//
// So a recorded prefix is provisioned as Describe WITHOUT Read on the authorised topics — the
// broker refuses every record fetch — and the records are delivered, key-filtered, by the
// key-authorising component the deployment DECLARED in front of the brokers
// (KAFKA_KEY_SCOPE_ENFORCEMENT). Where no component is declared, which is the shipped default,
// issuance REFUSES with SUBSCRIBER_KEY_SCOPE_UNENFORCED rather than minting a credential that
// can fetch nothing or, worse, one granted whole-topic Read beside a prefix nothing applies.
// The credential response and every subscriber read carry an enforced-access object naming the
// component that keeps each dimension. See model.EventSubscriber.RequiresGatewayDelivery and
// api/model.SubscriberEnforcedAccess.
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
// Every entry must be an exact member of event_topics.SubscriberGrantableTopics() — the four
// category topics under the configured prefix. The test is membership,
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
// NOT NULL does not stop an empty string, and a blank name defeats the reason the registry
// exists: it is the value an operator recognises a principal by months later, and no service
// can invent a meaningful one.
//
// # Why the bound and the character check are HERE and not only in api/model
//
// api/model.validateSubscriberName refuses the same three things at the request boundary,
// which is where a malformed body is cheapest to reject and where the caller gets the clearest
// message. This is not a duplicate of it — it is the check that holds when the boundary is not
// in the path at all: the service is reachable from the CLI, from a test, and from any future
// caller that constructs a registration directly, and the column is TEXT, which bounds nothing.
// A label reaching the registry unbounded would be echoed in the subscriber list, in the
// credential response and in operator log lines; a label carrying a newline would forge a
// second line in every rendering of it.
//
// The RUNE count is the bound rather than the byte length, so the allowance does not shrink for
// a label written in a non-Latin script. The alphabet is otherwise unrestricted: a name is a
// human label, and refusing letters, punctuation or emoji would be a usability defect dressed
// as security.
//
// Parameters:
//   - name string: the supplied label.
//
// Returns:
//   - string: the trimmed label.
//   - error: a typed validation error when it is blank, over the bound, or carries a control
//     character.
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

// normalizeSubscriberKeyScope normalizes a requested partition-key scope into the value the
// registry will hold, or reports why it cannot be held.
//
// # What the key scope is, and what it is not
//
// It is the THIRD scope of the subscriber access model (AAP R-7): the subscriber is entitled
// only to records whose message key carries this prefix. Because every Blnk event is keyed by
// ledger id, that is a ledger boundary.
//
// It is ENFORCED AT THE CONSUMER, NOT AT THE BROKER, and the credential response says so in
// the same object that carries the prefix — see api/model.SubscriberEnforcedAccess and its
// partition_key_prefix_enforced field, which is always false. Kafka's authorizer is
// topic-level and group-level and has no message-key dimension, so a principal granted Read on
// a shared category topic reads every record on it whatever the key.
//
// # Why this no longer refuses
//
// It used to refuse every non-blank value with SUBSCRIBER_ISOLATION_UNENFORCEABLE, reasoning
// that a registry row must not record a boundary the broker cannot keep. That reasoning was
// sound about the broker and wrong about the remedy: refusing implemented no part of the third
// scope, it withheld the CREDENTIAL instead, so a key-scoped subscriber could not consume at
// all and R-7's third dimension existed nowhere in the system. The boundary is now recorded,
// returned to the consumer, and labelled unenforced — which is the only form of it that can
// be delivered without per-tenant topics, and R-7 forbids those.
//
// # What is still refused
//
// A value that cannot be held honestly. Surrounding whitespace is refused rather than trimmed
// because two prefixes differing only by whitespace filter different sets of records and
// silently normalising one into the other would change which records a subscriber believes it
// is entitled to. An over-long value is refused because the prefix is stored, echoed in every
// registry response and written into log fields on every issuance. A control character is
// refused because it corrupts every log line and dashboard label it reaches.
//
// Parameters:
//   - prefix *string: the supplied constraint. Nil means none was requested, and a blank or
//     whitespace-only value means the same thing — the clearing case.
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

// warnOnUnenforceableKeyScope HAS BEEN REMOVED. Its subject — a subscriber whose row records a
// partition-key prefix Kafka's authorizer has no dimension for — is now reported by
// logKeyScopeDisclosure, on the issuance path, so there is ONE key-scope diagnostic per issuance
// rather than two that can drift, and it fires where the credential's shape is decided. It says
// what that shape is: Describe and no topic Read, with delivery through the key-authorising
// component the deployment declared — or a refusal to issue at all where none is declared.

// subscriberKeyPrefix reads a subscriber's recorded partition-key prefix, trimmed, or "" when
// none is recorded.
//
// A nil row and a nil column both answer "", so callers on read paths — where a repository's
// not-found representation is a nil pointer — need no guard of their own.
//
// # Trimmed, and why that is not cosmetic
//
// The prefix is a byte-exact test a consumer applies to an opaque message key, so surrounding
// whitespace is never meaningful and a value that is entirely whitespace is not a scope at all.
// Trimming here means "recorded" and "non-blank" are the same question everywhere, which is what
// lets DeclaresKeyScope, the enforcement point and the disclosure lines all agree.
//
// # It reports the recorded scope, NOT an enforced one
//
// Nothing about this value is enforced at the broker: Kafka's authorizer has no message-key
// resource dimension, so a credential granted Read on a topic reads every record on it whatever
// the keys are. That is why a recorded prefix is a REFUSAL at the two points where it would
// otherwise become access — requireProvisionableKeyScope at issuance and requireRecordableKeyScope
// when recording one onto a provisioned row — and why registration and every read still carry it:
// the row is a legitimate statement of intent, and only minting a credential against it is not.
// The database CHECK constraint that once made the combination unrepresentable stays dropped
// (sql/1781248930.sql), so a directly seeded row can still hold it and this value must still
// describe it. Never report this value without the enforcement point beside it;
// describeSubscriberKeyScope pairs them so that a caller cannot obtain one without the other.
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

// requireRecordableWebhookURL refuses to record a legacy webhook endpoint once the retirement
// instant has passed.
//
// # Why the registry needs its own guard
//
// The deprecated /webhook-subscription routes are fronted by middleware.WebhookSunsetGuard and
// answer 410 Gone after the sunset. The GENERIC subscriber routes — POST /subscribers and
// PUT /subscribers/{subscriber_id} — are not, and must not be: registering a subscriber and
// granting it topics are Kafka-era operations that go on working forever. But both of those
// requests carry an OPTIONAL webhook_url, so without this check the one field the sunset
// retires stayed writable through a route the sunset does not guard, and R-12's "nothing
// accepts a webhook URL after the sunset" held only for three of the four ways in.
//
// Guarding it HERE rather than at the request boundary is deliberate. api/model cannot import
// the root package — the root package's own tests import api/model, so the edge would close a
// cycle in the test binary — and duplicating the sunset comparison in the HTTP layer would
// create a second place that decides the retirement, which is exactly what event_sunset.go
// exists to prevent. Every writer of the column reaches RegisterSubscriber or UpdateSubscriber,
// including RecordLegacyWebhookSubscription, so one check here covers all of them.
//
// # CLEARING IS ALWAYS ALLOWED, and that asymmetry is the point
//
// A nil pointer means "no change" and an empty value means "forget it". Neither records
// anything, and both must keep working after the sunset: the rows written during the window
// still hold third-party endpoints, and ClearLegacyWebhookSubscription and
// PurgeMigratedWebhookURLs are how they are erased. A guard that refused every mention of the
// field would retire the only means of cleaning up after it.
//
// The verdict comes from WebhookSunsetPassed, so it is the same instant the relay stops
// enqueueing legacy deliveries at and the same one the HTTP guard refuses from — including the
// fail-closed reading a publishing deployment with an unusable window resolves to.
//
// Parameters:
//   - webhookURL *string: the requested value. nil means the caller did not mention the field.
//
// Returns:
//   - error: apierror.ErrGenGone — which the catalogue maps to 410 — when a non-empty URL is
//     offered after the retirement instant. nil otherwise.
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

// requireProvisionableKeyScope refuses to mint a credential for a subscriber whose row records
// a partition key prefix, because Kafka cannot enforce one.
//
// It guards ONE of the two orders in which that state is reachable — record the prefix, then ask
// for a credential. requireRecordableKeyScope guards the other, and both are needed: on its own
// either is walked around by approaching the state from the far side.
//
// # Why this is a refusal and not a disclosure
//
// Kafka's authorizer has no message-key dimension. There is no binding, pattern type or
// operation that confines a consumer to the records whose key carries a given prefix, so a row
// carrying such a prefix records an authorization NARROWER THAN ANY CREDENTIAL THIS SERVICE CAN
// MINT. There are only three responses to that, and two of them fail.
//
// Issuing silently is the obvious failure: the row says the subscriber may see one ledger's
// records while the credential reads every record on every authorised topic, and anybody
// deciding tenancy from the registry — an operator, a migration report, a support engineer
// answering "can this subscriber see that ledger?" — is answered wrongly with nothing to correct
// them.
//
// Issuing WITH THE LIMITATION DISCLOSED was tried next, and it is the one that looks safe. The
// response stated partition_key_prefix_enforced=false, named the consumer as the component
// applying the narrowing, and carried the remedy in the same object. Every word of it was true,
// and it still handed out a credential that reads other ledgers' and other subscribers'
// records — because a client-side filter is a CONVENTION, and the party expected to honour it is
// the one holding the credential. A convention the holder can ignore is not an authorization
// boundary, so disclosure changed what Blnk said and nothing about what the principal could
// read.
//
// So issuance fails closed, and says exactly what to do about it. Both remedies are real:
// clearing the prefix accepts whole-topic access explicitly, which is a decision somebody has
// now made rather than one the system made silently; narrowing authorized_topics is the
// enforceable form of the same intent whenever the ledgers in question map onto topics, and the
// broker really does keep it.
//
// # What is NOT refused
//
// Registering or holding a key-scoped row. That state is legitimate and stays legitimate — it is
// how an operator records an intent before deciding how to realise it, and every read of the row
// declares that the broker does not enforce the prefix and names both remedies. What is refused
// is minting a credential against it, which is the only step that turns the mismatch into
// access.
//
// # Why 409 rather than 400 or 503
//
// The caller sent no body — POST /subscribers/{id}/kafka-credentials has none — so nothing about
// the REQUEST is invalid, which rules out 400. Nothing is unavailable and a retry cannot
// succeed, which rules out 503. What is wrong is the current STATE of the resource, which is
// what 409 means, and the message names the state and both exits.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//
// Returns:
//   - error: a typed conflict when the row records an unenforceable key scope, otherwise nil.
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
		"This subscriber records a partition key prefix, which Kafka cannot enforce, so no "+
			"credential will be issued for it: the credential would read every record on every "+
			"authorized topic, including other ledgers' and other subscribers'. Clear the "+
			"partition key prefix to accept access to whole topics, or narrow the subscriber's "+
			"authorized topics, which the broker does enforce",
		fmt.Errorf(
			"event subscriber: subscriber %q records a partition key prefix; Kafka's authorizer has "+
				"no message-key dimension, so any credential issued would grant every record on every "+
				"authorised topic and the registry would describe a narrower boundary than exists",
			sanitizeLogValue(subscriber.SubscriberID, maxLoggedFilterLength),
		),
	)
}

// requireKeyScopeWhenEnforced is the MIRROR of requireProvisionableKeyScope: it refuses to mint
// a credential for a subscriber that records NO key scope, in a deployment that has declared its
// subscriber access to be key-scoped.
//
// # The hole this closes (SEC-01)
//
// Withholding topic Read from key-scoped principals made those subscribers safe. It said nothing
// about the subscriber registered WITHOUT a prefix, and that one is granted literal topic Read on
// every topic it is authorised for — which means every ledger's records on a shared category
// topic, in a deployment whose whole point was that subscribers see only their own.
//
// So in a tenant-scoped deployment, one prefix-less registration is the single credential that
// escapes the model, and nothing refused it. "An administrator registers or retains a subscriber
// without a prefix and issues credentials" is not an exotic attack: it is the default shape of
// the DTO, where partition_key_prefix is optional, and it leaves no trace that anything unusual
// happened.
//
// # Why the deployment's declaration is the right trigger
//
// Blnk cannot tell a tenant apart from an internal analytics consumer by looking at a row. What
// it can read is what the OPERATOR declared: KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway, with a
// verified control endpoint, is the deployment stating that subscriber access is scoped by record
// key. Under that statement every credential must carry a key scope, or the statement is false
// for at least one principal.
//
// A deployment that needs one whole-topic consumer alongside key-scoped ones has two honest
// routes, both named in the refusal: provision that consumer as an operator-managed principal
// outside the subscriber registry — which is what scripts/kafka-provision.sh does for the
// producer and the sample subscriber — or stop declaring the key-scoped model and acknowledge
// whole-topic access explicitly. Neither is a per-subscriber opt-out, because a per-subscriber
// opt-out is the hole again with a field name.
//
// # Why 409
//
// The request has no body, so nothing about it is malformed, and no dependency is unavailable.
// What is wrong is the row's state relative to the deployment's declared model, and after either
// remedy the identical request succeeds — which is what 409 means.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned.
//   - enforced bool: whether key-scope enforcement is active AND attestable.
//
// Returns:
//   - error: a typed conflict when a key-scoped deployment would mint a whole-topic credential,
//     otherwise nil.
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

// requireAcknowledgedSharedTopicAccess refuses to mint a whole-topic credential in a PRODUCTION
// deployment that has declared nothing about its subscriber access model.
//
// # What it is not
//
// It is not a claim that whole-topic access is a defect. It is the access model the requirement
// mandates: category topics, no per-tenant topics, and an authorizer with no message-key
// dimension. A credential granted `<prefix>.transactions` reads every transaction event in the
// deployment, and for a single-tenant ledger or a trusted internal consumer that is exactly
// right.
//
// What was wrong is that it was the DEFAULT, reached by configuring nothing. A deployment that
// had never thought about tenancy got the widest credential Blnk can issue, and every artefact
// described that correctly — the row, the response, the runbook — while no human had decided it.
// "Documented" is not "decided".
//
// # The declaration, and why it is cheap
//
// One variable, set once: KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true. The alternative is to
// declare the key-scoped model instead. Either way an operator has said which deployment this is,
// and the answer is then visible in configuration to everybody who reads it afterwards.
//
// # Why only in secure mode
//
// Server.Secure is this repository's production signal — the same flag whose falsity already
// announces that API authentication is disabled, and the same one resolveSearchCredential reads
// before refusing a public default credential. Outside it, the local stack, both compose files
// and the whole test suite issue credentials exactly as before: the declaration exists to make a
// production decision explicit, not to break `make run`.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned, named in the refusal.
//   - enforced bool: whether key-scope enforcement is active. Under it this check does not
//     apply, because the key scope IS the declaration and requireKeyScopeWhenEnforced has
//     already established that one is recorded.
//
// Returns:
//   - error: a typed conflict when a secure deployment has declared neither model, otherwise
//     nil.
func requireAcknowledgedSharedTopicAccess(subscriber *model.EventSubscriber, enforced bool) error {
	if enforced {
		return nil
	}

	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		// A configuration that cannot be read is not a production posture anybody declared, and
		// it is not this check's business to invent one: every other reader of the configuration
		// on this path has already failed by now, with a message about the configuration rather
		// than about the access model.
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

// requireRecordableKeyScope refuses to RECORD a key scope on a subscriber that already holds a
// credential. It is the other half of the refusal above, and without it that half is decorative.
//
// # The hole this closes
//
// requireProvisionableKeyScope guards the order "record a key prefix, then ask for a
// credential". It says nothing about the reverse order, and the reverse order is one ordinary
// sequence of API calls: register, issue — which succeeds, because no prefix is recorded — and
// then update the row with a prefix. The result is exactly the state the refusal exists to
// prevent, reached by a path that never touches it: a live SASL credential with Read on whole
// topics under a registry row announcing that the subscriber may see only the records whose key
// carries one prefix.
//
// There is a second cost that is easy to miss. Kafka stores ONE SCRAM credential per principal,
// so re-issuance IS issuance — a row that reached this state would be refused by
// requireProvisionableKeyScope for ever after, which means it could never rotate its secret. A
// guard that leaves rows unable to rotate is worse than no guard, and refusing the transition is
// what keeps rotation available on every row that has a credential.
//
// # Why the check is on the RESULTING row and not on the request
//
// A prefix on a row that holds NO credential is legitimate and must stay legitimate: it is the
// state a caller reaches by registering with a prefix, and issuance handles it with a message
// naming both exits. Refusing every update that merely leaves such a prefix in place would break
// unrelated edits to those rows, including the rename an operator makes while deciding what to
// do. What must be refused is the COMBINATION, so the predicate reads the row as it would be
// written.
//
// A row that ALREADY holds both — written before this guard existed, or seeded directly into the
// database, which is possible because no CHECK constraint enforces this — is therefore editable
// only by an update that also resolves the combination: clearing the prefix in the same request,
// or revoking the credential first. The message names both.
//
// # Why refuse rather than revoke
//
// Revoking the credential as a side effect of accepting the prefix would also keep the registry
// honest, and it was considered. It destroys a working credential — the subscriber stops
// consuming — in response to a request that said nothing about revocation, and the secret cannot
// be recovered: somebody must reissue and redistribute it. A refusal costs the caller one
// decision and takes nothing away. Whoever does want the credential gone can revoke it and then
// record the prefix, which is the same outcome asked for explicitly.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as it WOULD be written, with the requested
//     changes already applied.
//
// Returns:
//   - error: a typed conflict when the row would record a key scope while holding a credential,
//     otherwise nil.
func requireRecordableKeyScope(subscriber *model.EventSubscriber, enforced bool) error {
	if subscriber == nil || !subscriber.DeclaresKeyScope() || !subscriber.IsProvisioned() || enforced {
		return nil
	}

	return apierror.NewAPIError(
		// The same typed code issuance refuses with. One state, one code: a client that handles
		// SUBSCRIBER_KEY_SCOPE_UNENFORCED from the credential endpoint needs no second case to
		// handle it here, and the remedies are the same two plus revocation.
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

// Refusals a revocation tombstone produces, one per operation it blocks.
//
// The message names the operation that was refused, because a 409 whose text describes a
// DIFFERENT operation sends an operator looking in the wrong place — and the two callers of
// requireActiveSubscriber are refusing genuinely different things. The remedy is the same for
// both, and it is stated rather than implied: finish or reverse the deregistration.
const (
	// subscriberRefusedIssuanceMessage is issuance's refusal. Minting a credential would re-arm
	// a principal mid-removal.
	subscriberRefusedIssuanceMessage = "This subscriber is being deregistered, so no credential " +
		"will be issued for it"

	// subscriberRefusedUpdateMessage is the authorization change's refusal. Recording a wider
	// grant would make the following grant step re-create bindings for a principal whose
	// revocation is already in flight.
	subscriberRefusedUpdateMessage = "This subscriber is being deregistered, so its access model " +
		"can no longer be changed"
)

// requireActiveSubscriber refuses an operation that would GRANT or WIDEN the access of a
// subscriber being deregistered.
//
// A row carrying the revocation tombstone is on its way out: its broker-side access is being
// taken away and may still be live. Minting a credential for it would re-arm a principal
// mid-removal, and the deregistration that is already in flight would then delete the registry
// row that records the credential just issued — leaving exactly the orphaned live principal the
// tombstone exists to prevent. Recording a wider authorization does the same thing by the other
// route: the caller's grant step creates the bindings the new topic list implies.
//
// It is called by the two operations that can hand access out. It is deliberately NOT called by
// deregistration, for which an existing tombstone is the expected input of a retry, nor by
// credential revocation, which TAKES access away and is the one manual remedy available for a
// deregistration whose broker step keeps failing.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be changed.
//   - refusal string: the message describing which operation is being refused. Use
//     subscriberRefusedIssuanceMessage or subscriberRefusedUpdateMessage.
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
// # The unbounded phase this replaces
//
// An authorization reconciliation is a SEQUENCE of administrative round trips whose length
// depends on the change: readiness, an authorizer check, a SCRAM upsert, a describe, a delete
// and a create are all reachable within one phase. Each is individually capped by
// kafkaAdminRequestTimeout, but only individually — a caller that arrived with no deadline of
// its own, which every registry mutation except issuance did, could therefore spend that cap
// several times over in one phase. The phase had no bound at all; only its steps did.
//
// That is what made the fence lease impossible to size. A lease long enough to cover the worst
// case would fence a subscriber for a minute after a crash, and a lease short enough to recover
// from a crash quickly expires mid-phase — after which the operation keeps going and writes
// under a claim it no longer owns.
//
// # Why THIS length
//
// It is three times the issuance budget, and the comparison is the justification: R-7 requires
// a complete provisioning — credential upsert plus the full ACL reconciliation, the most
// round-trip-heavy phase there is — to finish inside SubscriberCredentialIssuanceBudget, and it
// does. A phase given three times that has ample headroom on a healthy broker while still
// failing in bounded time on an unhealthy one.
//
// Bounding a phase can only IMPROVE the outcome of a slow one. Without the bound the phase runs
// past the lease and its durable write is then refused anyway, having already changed the
// broker under a lost claim; with it the phase fails first, cleanly, with the fence still held
// and nothing half-applied.
const subscriberBrokerPhaseBudget = 3 * SubscriberCredentialIssuanceBudget

// SubscriberProvisioningFenceLease is how long an issuance or revocation holds its fence.
//
// It is DERIVED from the work it has to cover rather than chosen, which is the property the
// previous value lacked. One renewal must span exactly one broker phase, the durable write that
// follows it, and the compensating write that follows a failure — so the lease is the sum of
// those three budgets and nothing more.
//
// Keeping it to that sum is what preserves the other half of the requirement: a process killed
// while holding a claim fences the subscriber only until the lease expires, and that wait must
// stay close to what an operator would spend before retrying by hand. Renewal, not a longer
// lease, is what covers an operation that legitimately needs more time.
const SubscriberProvisioningFenceLease = subscriberBrokerPhaseBudget +
	subscriberCleanupBudget + SubscriberCredentialIssuanceBudget

// subscriberFence is a held provisioning claim, and the only way to reach the broker.
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
// # THE TOKEN IS CARRIED, NOT SWALLOWED
//
// The claim used to be returned as a bare release closure, which captured the token and gave
// the caller no way to reach it. Holding a claim therefore had no effect on anything the
// operation subsequently wrote: every durable write matched on the subscriber id alone, so an
// operation whose lease had expired still landed its write — over the top of whatever the new
// owner had just reconciled with the broker — and reported success. The fence refused a
// concurrent STARTER and then failed to constrain the operation it had admitted.
//
// Carrying the token is what closes that. Every mutating write presents it, so the write is
// conditional on the claim still being the caller's, and a caller that lost the race is refused
// with a conflict instead of overwriting the winner.
//
// A subscriberFence is used from ONE goroutine, the one running the operation, so it holds no
// lock of its own; only release is idempotent, because it is deferred and may also be reached
// explicitly.
type subscriberFence struct {
	store        eventSubscriberStore
	subscriberID string

	// token is the claim this operation holds. It is a CONCURRENCY-CONTROL SECRET: it must
	// never reach an API response or a log line, because a caller holding another operation's
	// token could complete writes against a subscriber it does not own. It is deliberately
	// absent from model.EventSubscriber for the same reason.
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
//   - ctx context.Context: the operation's context. Its VALUES are reused for the release.
//   - store eventSubscriberStore: the registry.
//   - subscriberID string: the subscriber to fence.
//
// Returns:
//   - *subscriberFence: the held claim. NEVER NIL, so a caller can defer its release
//     unconditionally even on the error path.
//   - error: a typed conflict when another operation holds the claim, a typed not-found when
//     the subscriber does not exist, or the repository's error.
func fenceSubscriber(
	ctx context.Context,
	store eventSubscriberStore,
	subscriberID string,
) (*subscriberFence, error) {
	token, err := store.ClaimSubscriberForProvisioning(
		ctx, subscriberID, SubscriberProvisioningFenceLease)
	if err != nil {
		// NIL, NOT A FENCE WITH NO TOKEN. A half-built fence looks like a held claim to every
		// caller that does not inspect its fields: `defer fence.release(ctx)` on one would issue
		// a release for a claim this call never took, and `renew` has to carry an explicit guard
		// for a state that should not be constructible. Returning nil makes the refusal
		// unmistakable, and release is deliberately nil-safe — see
		// TestSubscriberFence_ReleaseIsIdempotentAndNilSafe — so a caller may still defer the
		// release before it knows whether the claim succeeded.
		return nil, err
	}

	fence := &subscriberFence{
		store:        store,
		subscriberID: strings.TrimSpace(subscriberID),
		token:        token,
	}

	// THE CLAIM IS RENEWED WHILE THE OPERATION RUNS. A provisioning claim is a LEASE, and the
	// operations that hold one make several broker round trips under it — so a slow broker, or a
	// compensation that outlives the request, could let the lease expire underneath an operation
	// still working. The heartbeat renews it on a fraction of its term, which is what keeps the
	// fence true for the whole operation rather than only for its first few seconds.
	fence.lease = SubscriberProvisioningFenceLease
	fence.stop = make(chan struct{})
	fence.done = make(chan struct{})

	go fence.heartbeat()

	return fence, nil
}

// renew proves the claim is still this caller's and extends it by one lease.
//
// # It is called BEFORE each broker phase, and its failure ABANDONS the operation
//
// This is the half of the fix that keeps a long operation legitimate. The lease is short
// because it is also the recovery time after a crash, so an operation making several broker
// phases cannot fit inside one lease and must not try to — it says it is still working, and
// gets one more lease of headroom.
//
// Calling it immediately before a phase rather than after is deliberate: a successful renewal
// is PROOF of ownership at that instant, so the phase that follows cannot be interleaved with
// another operation's. Renewing afterwards would prove only that nobody took over while the
// broker was being changed, which is the thing already too late to learn.
//
// A refused renewal is returned, never logged and continued past. The caller has learned that
// it does not own the subscriber, and continuing is precisely how a stale owner comes to
// overwrite the state a new owner established — the failure the whole fence exists to remove.
//
// Parameters:
//   - ctx context.Context: cancels the renewal.
//
// Returns:
//   - error: a typed conflict when the claim is no longer this caller's, a typed not-found when
//     the row is gone, or the repository's error. Nil when the claim is confirmed held.
func (f *subscriberFence) renew(ctx context.Context) error {
	if f == nil || f.token == "" {
		// Unreachable through fenceSubscriber, which returns the claim and the error together.
		// Reported rather than treated as "held" because a fence with no token constrains
		// nothing, and silently proceeding would restore the unfenced behaviour exactly.
		return apierror.NewAPIError(apierror.ErrConflict,
			"No provisioning claim is held for this subscriber, so the operation was abandoned",
			errors.New("event subscriber: a broker phase was attempted without a provisioning claim"))
	}

	return f.store.RenewSubscriberProvisioningFence(
		ctx, f.subscriberID, f.token, SubscriberProvisioningFenceLease)
}

// consumed records that the claim went away with the row it was held on.
//
// # The false alarm this removes
//
// The provisioning columns live ON the subscriber row, so a successful deregistration deletes
// the claim along with everything else. The deferred release then matched no row and reported a
// conflict — "the claim expired or was taken over" — on the one path where the claim's
// disappearance is the INTENDED outcome. Every successful deregistration therefore emitted a
// lost-fence warning.
//
// That is worse than untidy. The lost-fence signal is the observable half of the fence: it is
// how an operator learns that an operation ran past its lease and had a write refused. A signal
// that also fires on every success is a signal nobody reads, so the false alarm was quietly
// spending the value of the real one.
//
// Calling this after the row is deleted makes the subsequent release a no-op, which is the
// truth: there is no claim left to release.
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
// # The release runs on a FRESH context
//
// A claim taken by an issuance whose budget then expired must still be released, or the
// subscriber stays fenced until the lease runs out — turning one slow broker call into half a
// minute of refused retries. So the release is bounded by its own budget rather than by the
// caller's remaining time, exactly as every other cleanup here is. A release that finds the
// claim gone is logged, not returned: the operation it belonged to has already finished, and
// the useful record is that the fence was lost, not a second error for the caller to read.
//
// It is idempotent and safe on a fence whose claim was never taken, so callers defer it
// unconditionally.
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
// It is the single place the two halves of the fix meet, so no fenced operation can acquire one
// without the other: a caller that wants to reach the broker asks for a phase context, and the
// renewal that proves ownership happens on the way to getting one.
//
// The returned context is DETACHED from the caller's cancellation in neither direction — it
// derives from ctx, so a client that disconnects still aborts the phase — but it adds a
// deadline the caller may not have supplied, which is what turns an unbounded sequence of
// round trips into a bounded one.
//
// Parameters:
//   - ctx context.Context: the operation's context.
//
// Returns:
//   - context.Context: bounded by subscriberBrokerPhaseBudget, or by the caller's own deadline
//     when that is sooner.
//   - context.CancelFunc: must be called, conventionally by defer. Non-nil even on error.
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

// subscriberCleanupBudget bounds a compensating write when no absolute deadline is in force.
//
// It is the issuance budget over again, which is the right size for the one or two round trips
// a cleanup makes. It applies to callers that are NOT under an issuance deadline —
// deregistration, revocation, the migration sweeps — where there is no endpoint bound to
// respect and a fresh budget is the correct behaviour. Under an issuance deadline the cleanup is
// capped by that deadline instead; see subscriberCleanupContext.
const subscriberCleanupBudget = SubscriberCredentialIssuanceBudget

// subscriberIssuanceDeadlineKey is the context key the absolute issuance deadline travels on.
//
// An unexported struct type, which is the standard way to make a context key that no other
// package can collide with or read by accident.
type subscriberIssuanceDeadlineKey struct{}

// withSubscriberIssuanceDeadline attaches the ONE absolute deadline an issuance is bounded by.
//
// # Why a context VALUE and not just the context's own deadline
//
// A compensating operation must survive the caller's CANCELLATION — the deadline expiring is
// the commonest reason it is needed at all, so a cleanup that inherited the cancelled context
// could never run on the occasion it exists for. context.WithoutCancel is what detaches it, and
// WithoutCancel necessarily strips the deadline along with the cancellation: they are the same
// mechanism.
//
// So the deadline is carried SEPARATELY, as a value, which survives WithoutCancel. A cleanup
// then re-imposes it deliberately: detached from cancellation, still bounded by the same
// instant. That is what makes "one absolute deadline" true of the whole request rather than only
// of its forward path, and it is why the value has to exist at all.
//
// It is read by subscriberCleanupContext here and by kafkaCleanupContext in event_admin.go, so
// the broker-side compensation is bounded by the same instant as the registry-side one.
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
//   - bool: false when the caller is not under an issuance deadline, which is the normal case
//     for deregistration, revocation and the migration sweeps.
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

// subscriberCompensationFloor is the smallest window a compensating operation is ever given.
//
// # When it applies, and why it has to exist
//
// In the ordinary case it never applies. The forward path runs on a deadline of
// absolute-minus-reserve and compensation runs to absolute, so the reserve IS the compensation
// window and this floor is never reached. The floor is for the one case the arithmetic cannot
// cover: the forward path overrunning the absolute instant itself, which happens only when a
// step DID NOT HONOUR ITS CONTEXT — a driver call that ignores cancellation, a blocking syscall,
// a stop-the-world pause. No deadline arithmetic can bound a call that never looks at its
// deadline.
//
// In that regime the choice is not between "within the bound" and "past the bound", because the
// bound is already gone. It is between leaving a live SASL credential at the broker that no
// registry row records — unfindable, unrevokable, granted ACLs, exactly the residue AUTH-01
// exists to prevent — and spending one bounded, named window removing it. Removing it is right.
//
// It is the reserve over again, because it buys the same work: two broker round trips (delete
// the ACL bindings, delete the SCRAM credential) and up to two local writes (clear the
// credential record, release the provisioning fence). And it is granted ONCE per request, not
// once per nested cleanup: subscriberCleanupContext and kafkaCleanupContext both REBASE the
// deadline value to the instant they resolved, so a broker cleanup nested inside a registry
// cleanup shares the window rather than starting a second one. That is what keeps the worst case
// at one floor past the bound instead of the fifteen seconds of stacked fresh budgets AAP-02
// reported.
const subscriberCompensationFloor = subscriberCompensationReserve

// boundedCompensationDeadline resolves the instant a compensating operation must finish by.
//
// It is the EARLIER of the fallback budget and the absolute issuance deadline, so a cleanup can
// never extend a request past its bound and can never be given more time than its own budget
// allows either — with one exception, stated explicitly because it is the whole subtlety of the
// function: an absolute instant that has ALREADY PASSED yields the floor rather than a dead
// context, because a compensation born expired is a compensation that never runs, and that is
// the failure CLEAN-01 exists to prevent. See subscriberCompensationFloor for why that trade is
// the right way round.
//
// Parameters:
//   - ctx context.Context: the failing operation's context, read for the issuance deadline.
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

	// The forward path overran the absolute instant, so there is nothing left of the reserve to
	// compensate inside. Grant the floor: bounded, named, and once per request because the
	// cleanup helpers rebase the value they read.
	if !absolute.After(now) {
		absolute = now.Add(subscriberCompensationFloor)
	}

	if absolute.Before(ceiling) {
		return absolute
	}

	return ceiling
}

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
// # AAP-02: detached from CANCELLATION, still bounded by the request's own DEADLINE
//
// Detaching from cancellation used to mean detaching from the deadline too, because
// WithoutCancel strips both. The cleanup then ran on a budget of its own, which is how a
// five-second endpoint came to answer in twenty: five seconds of forward work, then a fresh five
// for the registry writes and a fresh ten for the broker round trips underneath them.
//
// Now the absolute deadline travels separately as a context value and is re-imposed here, so a
// cleanup keeps the one property it needs — it runs even though the caller's context is
// cancelled — without being able to extend the request past its bound. When no issuance deadline
// is in force, which is every caller outside credential issuance, the fallback budget applies
// exactly as before.
//
// The caller's VALUES are kept, so the cleanup appears under the span that caused it.
//
// Parameters:
//   - ctx context.Context: the caller's context, used for its values and its issuance deadline.
//
// Returns:
//   - context.Context: detached from cancellation, bounded by the earlier of the issuance
//     deadline and subscriberCleanupBudget, and carrying that instant forward as the issuance
//     deadline so nested cleanups share this window instead of opening another.
//   - context.CancelFunc: must be called, conventionally by defer.
func subscriberCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// ONE PHASE BUILDER, so this and kafkaCleanupContext cannot disagree about when a
	// compensation ends. Both were written for the same bound and arrived reading two different
	// context keys, which meant the wall clock one of them attached was invisible to the other
	// and every broker cleanup silently took a fresh ten seconds.
	//
	// The durability reserve is the headroom: the record that says what the compensation could
	// not fix must not be starved by the compensation itself. subscriberCleanupBudget is the
	// ceiling that applies when there is no wall clock at all — deregistration, revocation and
	// the migration sweeps make no timed promise.
	return subscriberPhaseContext(ctx, subscriberDurabilityReserve, subscriberCleanupBudget, true)
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
// an immutable id. The name, the authorised topics and the legacy webhook URL are the caller's.
//
// A PARTITION KEY PREFIX is not: normalizeSubscriberKeyScope refuses every non-blank value here,
// because Kafka's authorizer has no message-key dimension and a recorded prefix would state a
// boundary no credential for the row can have. Narrow the authorised topics instead — that is
// enforced at the broker.
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
	// invention out to every client, where nothing checks it against that rule.
	//
	// A SUPPLIED VALUE IS JUDGED AS GIVEN, and that distinction is the whole of it. Trimming
	// first and then testing for empty conflates three different requests: the field omitted
	// (no preference — mint one), the field present and whitespace (a value that names nothing —
	// refuse), and the field present with surrounding whitespace (a value that names something
	// ELSE — refuse). Trimming answered the second by minting an identifier the caller never
	// asked for, and the third by silently substituting a different identity: " sub_acme" and
	// "sub_acme" would then be one subscriber, and SEC-04's whole point is that two values
	// differing only in whitespace must not provision the same Kafka principal twice.
	//
	// model.CanonicalizeSubscriberIdentifier owns that judgement, so registration and every
	// operation that acts on an existing row apply one rule.
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
// The page is a KEYSET page rather than an offset one: a non-positive limit selects the
// repository default and an oversized one is capped, and a nil cursor starts at the most
// recently registered subscriber. Keyset paging is what keeps a page's cost independent of how
// far into the registry it sits, and what stops a concurrent registration from shifting rows
// across page boundaries while a client is walking them.
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
// It exists because "the listing narrows by nothing, so this count and that page describe the
// same set by construction" was not true across two reads: a subscriber registered or
// deregistered between them is counted by one and absent from the other, however identical the
// two predicates are. Both statements run inside one read-only REPEATABLE READ transaction here,
// which is what makes the pair coherent.
//
// The two single-purpose reads remain for callers that want only one of the two answers. This is
// the path for the caller that wants both and needs them to agree.
//
// It does NOT promise that paging to the total exhausts the registry — paging spans many
// requests over a live registry. The total is exact as at this page.
//
// Parameters:
//   - ctx context.Context: cancels the read.
//   - query model.SubscriberPageQuery: the page.
//
// Returns:
//   - model.SubscriberPage: the page, as ListSubscribers.
//   - int64: the registry size in the same snapshot.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the repository's error.
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
// It exists so GET /subscribers can honour include_count, which used to be refused outright
// because nothing at any layer could answer it. The alternative — reporting the length of the
// page — is worse than the refusal was: a paging client that reads the total as the size of the
// set would stop after one full page, or compare a page length against itself forever.
//
// The listing narrows by nothing, so this count and that page describe the same set by
// construction. A filter added to one must be added to the other in the same change.
//
// Parameters:
//   - ctx context.Context: cancels the query.
//
// Returns:
//   - int64: the number of registered subscribers.
//   - error: ErrEventKafkaUnavailable when no registry is configured, or the repository's error.
func (s *EventSubscriberService) CountSubscribers(ctx context.Context) (int64, error) {
	store, err := s.requireStore()
	if err != nil {
		return 0, err
	}

	return store.CountEventSubscribers(ctx)
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
	//
	// A row carrying the revocation tombstone is being deregistered: its principal is on its
	// way out and its ACL bindings are being removed. This method was the one mutation that
	// never asked. So an update on such a row was accepted, and because a non-nil
	// AuthorizedTopics REPLACES the whole set, it could WIDEN the authorization — after which
	// STEP 3 below dutifully created broker bindings for a principal whose revocation was
	// already in flight. The subscriber regained access while being removed, and the registry
	// recorded it as an ordinary edit.
	//
	// The repository refuses the same combination through its own predicate, and that is
	// deliberate rather than redundant: this check answers with the reason and costs nothing,
	// while the predicate is what holds if a row is tombstoned between here and the write.
	// Neither substitutes for the other — this one prevents the BROKER work in STEP 1 from
	// running at all, which no database predicate can.
	if err := requireActiveSubscriber(subscriber, subscriberRefusedUpdateMessage); err != nil {
		return nil, err
	}
	// The BROKER-REPRESENTABLE authorization as the registry holds it, snapshotted before the
	// changes are applied on top. It is what authorizationNeedsReconciliation compares against
	// and what abandonUpdateAfterLostFence reports as still recorded.
	storedAuthorization := newSubscriberAuthorization(subscriber)

	// AND THE TWO FACTS THE POST-WRITE DISCLOSURE NEEDS, read here for the same reason: both
	// describe the row BEFORE this update, and once the changes are applied neither can be
	// recovered. warnOnKeyScopeRecordedAfterIssuance reports the one transition whose consequence
	// reaches backwards — a prefix recorded onto a row whose credential was already delivered
	// with a response declaring direct broker access.
	priorKeyPrefix := subscriberKeyPrefix(subscriber)
	priorCredentialIssued := subscriber.IsProvisioned()

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

	// A present empty string CLEARS the recorded key scope, which is why the column is nullable
	// and why this cannot be a plain string: NULL means "no key constraint" while the empty
	// string would mean "constrained to the empty prefix", and those are opposite intents.
	// normalizeSubscriberKeyScope owns that three-way mapping so registration and update cannot
	// disagree about it.
	//
	// RECORDING A PREFIX ON A PROVISIONED SUBSCRIBER IS REFUSED, by requireRecordableKeyScope
	// below, once the change has been applied to the row. The refusal is deliberately NOT here:
	// the state that has to be judged is the row as it would be WRITTEN, so that an update which
	// clears the prefix — the remedy — is never refused by the guard meant to protect it.
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
	//
	// RECORDING one past the sunset is refused, while CLEARING one stays available forever —
	// see requireRecordableWebhookURL. Retirement must not strand the rows it created.
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

	// A partition key prefix recorded over a row that ALREADY HOLDS A CREDENTIAL is refused
	// unless something in front of the brokers authorises record keys. The refusal is one half
	// of a pair — requireProvisionableKeyScope guards the other order, prefix first and
	// credential afterwards — and on its own either is walked around by approaching the state
	// from the far side.
	//
	// Judged on the row as it would be WRITTEN, which is why it sits after every change above
	// has been applied and before any broker work below: an update that clears the prefix, or
	// that leaves a prefix on a row with no credential, passes through untouched. What is
	// refused is the combination — a live principal with Read on whole topics beneath a row
	// announcing key-scoped access — because Kafka's authorizer has no message-key dimension to
	// make the announcement true, and because a row in that state could never rotate its
	// secret, since re-issuance is refused for the same reason.
	//
	// THE ENFORCEMENT FACT IS CONFIGURATION, NOT A PROPERTY OF THIS BINARY. Blnk ships no
	// key-authorising component and serves no records itself; KAFKA_KEY_SCOPE_ENFORCEMENT is an
	// operator declaring that one is deployed in front of the brokers. See keyScopeEnforcement.
	_, keyScopeEnforced := s.keyScopeEnforcement()
	if err := requireRecordableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// AND CLEARING A PREFIX IS REFUSED WHERE THE DEPLOYMENT DECLARES A KEY-SCOPED MODEL (SEC-01).
	//
	// This is the third order the unscoped-credential state is reachable from, and the only one
	// the issuance guard cannot see. Record a prefix, get a credential, then clear the prefix:
	// requireKeyScopeWhenEnforced never runs again, and clearBrokerAccess below dutifully WIDENS
	// the live principal to whole-topic Read — a subscriber in a tenant-scoped deployment reading
	// every ledger, reached by an ordinary sequence of API calls with nothing refusing it.
	//
	// Judged on the row as it would be written, like every guard around it, so an update that
	// merely renames a key-scoped subscriber or replaces one prefix with another passes through.
	// What is refused is arriving at "no key scope" while the deployment says every subscriber
	// has one, and the refusal names the same remedies issuance names.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return nil, err
	}

	// DOES THE BROKER HAVE TO BE TOUCHED AT ALL? ADMIN-02 and PERF-P15 are the same finding
	// reached from two directions, and this one decision answers both.
	//
	// Prune and grant exist to move the broker-side boundary, and reconciliation is two
	// DescribeACLs round trips before it can even conclude there is nothing to change. Running
	// them on EVERY update meant a rename, or recording the legacy webhook URL of a subscriber
	// migrating off HTTP, paid the full administrative cost of an authorization change nobody
	// was making — and FAILED when the broker did not answer. That coupled deprecated migration
	// bookkeeping to Kafka's availability: an operator recording where a webhook used to point
	// could not do so while the broker was down, for no reason the data justified.
	//
	// The gate is on what the request NAMES rather than on a comparison of values, because a
	// comparison is wrong in the case that matters most: when a grant failed halfway the row is
	// already persisted, so a caller retrying the same list would find the boundary "unchanged"
	// and skip the very repair it is retrying. Naming the topic list is therefore always a
	// re-apply instruction. The value comparison is kept as well, for the fields a caller can
	// move without naming the topics — the principal and the consumer group.
	//
	// THE PRESENCE OF A KEY SCOPE *IS* PART OF THIS, and its VALUE is not. That split is what
	// keeps PERF-P15's saving while making the isolation boundary real.
	//
	// A prefix has no broker-side representation — Kafka's authorizer has no message-key
	// dimension — so changing one prefix for another moves no binding and must not cost two
	// DescribeACLs round trips. But RECORDING a prefix on a row that had none, or CLEARING the
	// one it had, changes the grant itself: a key-scoped subscriber is provisioned with Describe
	// and NO Read on its topics, so that the declared key-authorising component is the only path
	// its records can take. An edit across that line therefore has to reach the broker, in the correct
	// direction: recording a prefix PRUNES the Read bindings, and clearing one grants them back.
	//
	// Leaving it out — which is what the previous revision did, on the reasoning that a prefix
	// "cannot move a single binding" — meant an operator could narrow a subscriber's declared
	// boundary while its live credential kept reading every record on the shared topic, with the
	// response reporting the narrowing as enforced. subscriberAuthorization snapshots the
	// presence for exactly that reason.
	reconcileBroker := authorizationNeedsReconciliation(storedAuthorization, subscriber, changes)

	// REFUSED: WIDENING THE BOUNDARY OF A PRINCIPAL WHOSE CREDENTIAL IS UNACCOUNTED FOR.
	//
	// An orphaned row names a principal for which a SCRAM credential exists at the broker that
	// Blnk could neither record nor revoke — so a means of authenticating as it is outstanding and
	// unknown. A cleanup obligation is the same doubt from the other direction. Creating NEW ACL
	// bindings for such a principal hands that outstanding credential access it did not have,
	// which is the one direction that cannot be undone by discovering the mistake later.
	//
	// NARROWING STAYS ALLOWED, and that distinction is the whole of this guard. Removing a
	// binding reduces the exposure, which is exactly what an operator responding to an orphan
	// wants to be able to do; refusing every edit would leave the row frozen at its widest.
	// Re-issuance also stays available and is the documented remedy: Kafka stores one credential
	// per principal, so a new issuance REPLACES the orphan by construction and settles the state.
	if reconcileBroker {
		if err := refuseWideningUnaccountedAccess(subscriber, storedAuthorization); err != nil {
			return nil, err
		}
	}

	pruned, granted := 0, 0

	if reconcileBroker {
		// STEP 0 — RECORD THE OBLIGATION, before a single broker round trip.
		//
		// Prune, persist and grant are three writes to two systems and they are not atomic.
		// Every intermediate state is reachable — the prune lands and the persist fails, the
		// persist lands and the grant fails, or the process disappears between any two of
		// them — and each used to be recorded as nothing more than a returned error and a log
		// line. A log line is not queryable per subscriber, cannot be retried and cannot be
		// alerted on, so a subscriber left mid-change stayed mid-change until somebody read the
		// right line.
		//
		// The marker is written FIRST because the failure that matters most cannot write
		// anything: a marker recorded after a failure is absent from exactly the case where the
		// process died mid-change, which is the case an operator cannot detect any other way.
		//
		// ONE marker covers a failure at either boundary because the ROW IS THE SOURCE OF
		// TRUTH: whichever step failed, the remedy is identical — reconcile the broker to
		// whatever the row says — so settlement needs no knowledge of how far this attempt got.
		//
		// A marker that cannot be written FAILS THE OPERATION before the broker is touched,
		// because proceeding without it is the state this whole mechanism exists to prevent.
		//
		// It is recorded only when the broker WILL be touched. An update that cannot move a
		// binding owes no reconciliation, and recording one anyway would hand settlement work
		// to do for a change that never diverged.
		if err := store.RecordSubscriberGrantReconcilePending(
			ctx, subscriberID, time.Now(), fence.token,
		); err != nil {
			return nil, err
		}

		// STEP 1 — PRUNE. Whatever the new authorization no longer implies is removed at the
		// broker BEFORE the row records the narrowing, so a failure of the write below leaves
		// the subscriber with less access than the registry claims rather than more.
		//
		// Each broker phase runs under its OWN renewed claim and its own deadline. Prune and
		// grant are two independent sequences of administrative round trips, so a single lease
		// taken at the top could not cover both — and this method arrived with no deadline of
		// its own, so each round trip fell back to the admin client's per-request cap and the
		// pair together were unbounded. The renewal proves the claim is still this caller's on
		// the way in; the deadline makes the phase's worst case knowable.
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

	// STEP 2 — PERSIST, under the claim. A write that matches no row here means the claim was
	// lost or the row was tombstoned while the prune above was in flight, and the conflict that
	// reports it is the whole point: the prune has already narrowed the broker, so continuing
	// to STEP 3 with a stale view would re-grant an authorization somebody else has replaced.
	// THE ROW AS STORED, not the one assembled above. The write stamps updated_at, so the
	// assembled value carries the instant this call READ — strictly older than the one now stored —
	// and answering with it gives a client using updated_at to detect concurrent modification a
	// value that predates its own write.
	persisted, err := store.UpdateEventSubscriber(ctx, subscriber, fence.token)
	if err != nil {
		// A LOST CLAIM IS NOT AN ORDINARY CONFLICT WHEN THE PRUNE ALREADY LANDED. The broker
		// now grants less than the registry records, and that residue has to be described
		// rather than merely returned as a 409 — see abandonUpdateAfterLostFence for why it is
		// deliberately NOT undone.
		if subscriberFenceWasLost(err) {
			return nil, s.abandonUpdateAfterLostFence(subscriber, storedAuthorization, pruned, err)
		}

		return nil, err
	}

	// From here the stored row IS the subscriber: the grant step below derives its bindings from
	// it, so using the assembled copy would risk granting against a value the database did not
	// accept.
	subscriber = persisted

	if reconcileBroker {
		// STEP 3 — GRANT. Whatever the new authorization adds reaches the broker only now that
		// the registry records it, so a failure here is again fail-closed. The error is
		// returned, since a caller told the update succeeded would believe a grant exists that
		// does not.
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
		//
		// A failure to clear it is logged and NOT returned, because the operation genuinely
		// succeeded and reporting it as failed would invite a caller to repeat a change that is
		// already fully applied. What remains is a stale obligation, and the settlement pass
		// that picks it up reconciles a broker that already matches — which is a no-op — and
		// clears it. Over-reporting an obligation costs one idempotent reconciliation;
		// under-reporting one costs an undetected divergence, so the asymmetry is deliberate.
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
	// narrowing produces and says nothing about a credential already in a subscriber's hands: that
	// credential was delivered with a response declaring direct broker access, the Read it
	// described has just been withdrawn, and the statement cannot be recalled. Emitted after the
	// write, so it reports what was persisted rather than what was attempted.
	warnOnKeyScopeRecordedAfterIssuance(subscriber, priorKeyPrefix, priorCredentialIssued)

	return subscriber, nil
}

// SettleSubscriber discharges whatever broker-side work a subscriber still owes.
//
// It is the remedy SubscriberSettlementProcessor drives, and the counterpart of the two markers
// written when an operation could not finish. See event_settlement.go for why those markers exist
// at all; this function is what makes them go away.
//
// # It is IDEMPOTENT, which is what makes retrying it safe
//
// Every step is either a round trip that converges on the same broker state or a write
// conditional on the caller's claim. Running it against a subscriber that owes nothing costs two
// reads and changes nothing. Running it twice produces the same result as running it once. That
// property is load-bearing: an obligation is discharged only AFTER its remedy returned
// successfully, so a pass that crashed in between leaves the marker outstanding and this runs
// again.
//
// # The order between the two remedies is fixed
//
// CREDENTIAL CLEANUP FIRST. Its revocation removes every binding the principal holds — by
// principal rather than by the current grant, because the broker's bindings are the union of
// every grant a principal has ever held — so reconciling the grant first and revoking second
// would undo the reconciliation. In this order the pass converges on bindings that match the row,
// no credential, and a row reporting "registered, not yet provisioned".
//
// # A subscriber being deregistered is NOT reconciled
//
// A row carrying the revocation tombstone is on its way out: its principal is being destroyed and
// its bindings removed. Reconciling the broker TO such a row would re-create the very grants the
// deregistration is removing — the exact widening the tombstone predicate exists to prevent — so
// the grant obligation is DISCHARGED rather than acted on. Nothing is lost by that: the tombstone
// is itself a durable, indexed marker that the deregistration retry finds, so the outstanding work
// remains tracked by the mechanism that owns it. Leaving the grant marker set instead would create
// an obligation no pass could ever satisfy.
//
// The credential cleanup IS still performed on a tombstoned row, because revoking a credential is
// exactly what a deregistration wants and doing it early cannot be wrong.
//
// Parameters:
//   - ctx context.Context: bounds the whole remedy. The caller applies its own budget.
//   - subscriberID string: the subscriber to settle.
//
// Returns:
//   - error: a typed conflict when another operation holds the subscriber's claim — which is a
//     SKIP rather than a fault, and is why the processor simply retries later — a typed not-found
//     when the subscriber has since been deregistered, or the failure that stopped the remedy.
func (s *EventSubscriberService) SettleSubscriber(ctx context.Context, subscriberID string) error {
	store, err := s.requireStore()
	if err != nil {
		return err
	}

	if err := requireSubscriberIdentifier(subscriberID); err != nil {
		return err
	}

	subscriberID = strings.TrimSpace(subscriberID)

	// FENCED, like every other operation that changes broker state. A subscriber somebody is
	// actively issuing for is left alone: the conflict propagates, the processor records the
	// attempt, and the obligation is taken by a later pass. Settling underneath a live issuance
	// would revoke the credential it is in the middle of writing.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return err
	}
	defer fence.release(ctx)

	// RE-READ UNDER THE CLAIM. The flags the processor scanned are advisory: between the scan
	// and this point a successful re-issuance can have discharged the credential-cleanup
	// obligation, and acting on the stale flag would revoke a credential that had just been
	// issued. This read is the first moment at which they cannot change underneath the remedy.
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

// settleCredentialCleanup revokes a credential Blnk no longer accounts for and clears the row's
// record of it.
//
// Both halves run unconditionally once the marker is set, because the marker covers two shapes
// that are indistinguishable from the row — a credential live at the broker with no boundary, and
// a row naming a credential that is already gone — and the same two steps settle either. Revoking
// an absent credential is a no-op; clearing an already-clear record is a no-op.
//
// The marker is cleared only by the credential record's own clear-up, which is deliberate: the
// row's credential columns and the obligation are written by the same statement, so the registry
// cannot end up reporting "no credential" while still owing a cleanup, or the reverse.
//
// Parameters:
//   - ctx context.Context: bounds the remedy.
//   - store eventSubscriberStore: the registry.
//   - fence *subscriberFence: the held claim. Renewed before the broker phase.
//   - subscriber *model.EventSubscriber: the row.
//   - obligation model.SubscriberSettlementObligation: what the row owes, read under the claim.
//
// Returns:
//   - error: the failure that stopped the remedy, or nil when nothing was owed or all of it was
//     settled.
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

	// The broker no longer holds anything for this principal, so the row must stop saying it
	// does. This is the write that ALSO clears the marker — see ClearSubscriberCredential — which
	// is why nothing else in this function touches it.
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

// settleGrantReconciliation brings the broker's ACL bindings back into line with the row.
//
// It reuses pruneBrokerAccess and grantBrokerAccess — the same two steps, in the same order, that
// UpdateSubscriber performs — rather than a second implementation. The row is the source of
// truth, so this converges regardless of how far the original attempt got, and it needs no record
// of which step failed.
//
// Parameters:
//   - ctx context.Context: bounds the remedy.
//   - store eventSubscriberStore: the registry.
//   - fence *subscriberFence: the held claim. Renewed before each broker phase.
//   - subscriber *model.EventSubscriber: the row whose authorization the broker must match.
//   - obligation model.SubscriberSettlementObligation: what the row owes, read under the claim.
//
// Returns:
//   - error: the failure that stopped the remedy, or nil when nothing was owed or all of it was
//     settled.
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

	// A tombstoned row is discharged without being acted on. Reconciling TO it would re-create
	// the grants its deregistration is removing, and the tombstone is already a durable marker
	// the deregistration retry finds — so this obligation would otherwise be one no pass could
	// ever satisfy.
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

// warnOnKeyScopeRecordedAfterIssuance reports the one key-scope transition whose consequence
// reaches BACKWARDS past the request that caused it.
//
// # The transition, and why it is worth its own diagnostic
//
// logKeyScopeDisclosure already records every issuance made to a row carrying a prefix, so such a
// subscriber is told at issuance that its records come from the declared key-authorising
// component. This one covers the reverse order: the credential was issued FIRST, against a row
// with no prefix, so its
// response said broker_record_access = true and gateway_delivery_required = false — truthfully,
// at the time. A later update then records a prefix, and that earlier statement, already
// delivered and unretractable, is now wrong.
//
// # And the consequence is immediate, which is why this is a warning
//
// The broker-side grant DOES change across this transition. Recording a prefix narrows the
// authorization: UpdateSubscriber prunes the Read bindings so that the declared key-authorising
// component is the only path the subscriber's records can take, exactly as issuance would have
// provisioned it. The
// holder's existing credential still authenticates and can still describe its topics — and its
// next fetch is refused by the broker with TOPIC_AUTHORIZATION_FAILED.
//
// That is the correct outcome, and it is not a silent one. An operator who narrows a subscriber's
// declared boundary should expect its direct consumption to stop; a consumer whose fetches begin
// failing needs the reason to be findable. So the line states the transport change and the
// remedy: reissue, which delivers the corrected enforced-access declaration naming the declared
// component — and which REFUSES outright where none is declared, so the narrowing cannot be
// reported as enforced by nothing — or clear the prefix to restore direct broker consumption.
//
// The previous revision of this comment said the grant could not change, "because Kafka has no
// message-key authorization dimension to change". Kafka still has none; what changed is that Blnk
// no longer grants whole-topic Read to a subscriber whose row asks for less than a whole topic.
//
// Only a transition INTO a recorded prefix is reported. Clearing one restores what the holder was
// originally told, and an unchanged prefix was already declared by the issuance that carried it.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as it now stands, after the write.
//   - priorKeyPrefix string: the prefix recorded before this update, "" when none was.
//   - priorCredentialIssued bool: whether the row already held a credential before this update.
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

// refuseUnconfirmableBrokerWork is the AUTH-02 guard: no broker IN THIS PROCESS is not the same
// fact as no broker-side access.
//
// # The inference it removes
//
// Four lifecycle paths branched on admin.IsConfigured() and, finding no broker, concluded there
// was nothing at a broker to act on. That reads the configuration of THIS PROCESS as a fact about
// the world, and the two differ in exactly the case that matters: a subscriber provisioned while
// Kafka was configured and then deregistered after a config change, a deploy that dropped
// KAFKA_BROKERS, or a replica reading a different environment.
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
// # What decides instead
//
// The ROW'S OWN EVIDENCE, read through model.EventSubscriber.MayHaveBrokerCredential — which is
// the union of every state that can coexist with a live credential, not just a recorded reference.
//
// This guard used to test IsProvisioned, i.e. credential_reference alone, and that test is
// exactly backwards for the worst of the four states. An ORPHANED credential is one an issuance
// wrote at the broker and could then neither record nor revoke: the reference is NIL precisely
// BECAUSE the recording failed, so the absence of the record is the evidence rather than its
// refutation. IsProvisioned answered false for such a row, the caller concluded there was nothing
// at a broker to act on, and deregistration deleted the only row naming a principal that can
// still authenticate — with no row left to retry from, which is the unrecoverable outcome this
// whole guard exists to prevent. A row carrying an unsettled cleanup obligation, or a
// deregistration tombstone, fell through the same hole.
//
// The predicate is conservative on purpose: a false positive costs a retryable refusal that a
// configured broker resolves, while a false negative costs live broker access nothing accounts
// for. See MayHaveBrokerCredential for why grant_reconcile_pending_at is deliberately excluded.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as the registry holds it. Nil may hold nothing.
//   - action string: what could not be confirmed, for the log line and the detail.
//
// Returns:
//   - error: a retryable ErrKafkaUnavailable when the row may hold broker access, otherwise nil.
func refuseUnconfirmableBrokerWork(subscriber *model.EventSubscriber, action string) error {
	if !subscriber.MayHaveBrokerCredential() {
		return nil
	}

	// WHICH of the four states, because the remedies differ: an orphan is settled by re-issuing, a
	// tombstone by retrying the deregistration, a cleanup obligation by letting settlement run.
	// A refusal that named none of them left an operator to work that out from the columns.
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
		// AUTH-02. Narrowing the registry while the broker keeps the wider grant makes the
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
			// comes back from the Kafka client with the whole request appended to it —
			// hundreds of lines carrying broker addresses, listener names and every field of
			// the call — and an unbounded value with newlines in it can also forge log
			// structure. sanitizeLogValue keeps the broker's own words, which are what an
			// operator needs, and drops the dump.
			"error_class": kafkaErrorClassField("subscriber_credential_issuance", err),
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
		// AUTH-02, the widening half. The row records a grant nothing created, and a caller told
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

// keyScopeEnforcement resolves whether this deployment enforces subscriber key scopes, and
// where.
//
// One read, consulted by both halves of the issuance decision — whether a key-scoped
// subscriber may be issued a credential at all, and which endpoint that credential names — so
// the two cannot disagree. Reading the configuration twice would permit a credential minted
// under enforcement that reports the brokers, or the reverse.
//
// It goes through fetchConfiguration, the package's configuration seam declared in
// event_sunset.go, for the same reason subscriberFacingBrokers does: a test that swaps the seam
// sees consistent behaviour across every event file.
//
// A configuration that cannot be read answers "not enforcing". That is the fail-closed
// direction: it withholds credentials from key-scoped subscribers rather than issuing ones
// whose declared boundary nothing keeps.
//
// Returns:
//   - gateway []string: the enforcing bootstrap list, nil unless enforcement is active.
//   - enforced bool: true only when config.KafkaConfig.KeyScopeGateway reports an active,
//     distinct gateway.
func (s *EventSubscriberService) keyScopeEnforcement() (gateway []string, enforced bool) {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return nil, false
	}

	return cnf.Kafka.KeyScopeGateway()
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
// # IT FAILS CLOSED, AND THE POLICY LIVES IN ONE PLACE
//
// The decision is config.KafkaConfig.SubscriberFacingBrokers and nothing else: this function
// asks it and refuses when it reports no advertised list. There is no fallback to
// KAFKA_BROKERS.
//
// A fallback existed, with a warning on every issuance, and the warning was not a control. It
// landed in Blnk's log while the consequence landed on the subscriber: an internal address
// reported to an outside consumer surfaces as an unexplained connection timeout in THEIR logs
// days later, and because the SASL secret is shown exactly once, diagnosing it costs a
// reissue — a one-time secret spent on an address nothing can dial. It also published the
// deployment's internal topology in a response body. Refusing is the cheaper failure: it is
// immediate, it names the variable to set, it discloses no internal address, and it happens
// BEFORE a password exists or the broker is touched, so it leaves no residue anywhere.
//
// The refusal is not a new configuration requirement smuggled past requirement R-10's eight
// variables so much as the one it always documented: .env.example, both Compose files and
// infrastructure/k8s-manifests/blnk-config.yaml have all described this refusal, and the local
// stack sets KAFKA_SUBSCRIBER_BROKERS to the broker's external listener. A deployment whose
// subscribers really do run inside it states that in one line by setting this to the same value
// as KAFKA_BROKERS, which is a claim an operator has made rather than one the service invented.
//
// Every other endpoint runs without it. Only issuance needs it, and only because only an
// operator can know the address a subscriber outside the deployment reaches.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the subscriber being provisioned, named in the log and
//     in the error so an operator knows which request it was. May be nil.
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
		// THE VARIABLE IS NAMED IN THE MESSAGE, not only in the detail and the log. This is the
		// one string that reaches the operator running the request, and "no broker list is
		// configured" without the key is a message that cannot be acted on. A configuration key
		// name discloses nothing: it is documented in .env.example and in the manifests.
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
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		return nil, err
	}
	defer fence.release(ctx)

	// STEP 2 — TOMBSTONE. The row survives, carrying the principal and topics revocation needs
	// and saying plainly that this subscriber is on its way out.
	//
	// Under the claim, even though it is the first write: the tombstone freezes the
	// subscriber's authorization for every other operation, so a caller that had lost its claim
	// must not be able to apply it to a subscriber somebody else is mid-issuance for.
	//
	// It is deliberately NOT preceded by requireActiveSubscriber. A row that is already
	// tombstoned is a deregistration that failed part way through, and the correct response to
	// retrying it is to FINISH it — this is the one mutation for which an existing tombstone is
	// the expected input rather than a refusal, which is why the stamp keeps the first instant.
	pending, err := store.MarkSubscriberRevocationPending(ctx, subscriberID, s.clock(), fence.token)
	if err != nil {
		return nil, err
	}

	admin, err := s.provisioner()
	if err != nil {
		// The row is TOMBSTONED, not deleted, so nothing is lost: it names the principal and a
		// retry finds it. The error is returned so the caller does not read the removal as
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
		// AUTH-02, AND THE MOST CONSEQUENTIAL BRANCH IN THIS FILE. Deleting the row destroys the
		// only record of which principal still has to be revoked, so the residue is unfindable
		// rather than merely unrevoked. The row stays, tombstoned, which is what makes the retry
		// possible at all.
		if err := refuseUnconfirmableBrokerWork(
			pending, "revoking this subscriber's Kafka access",
		); err != nil {
			return pending, err
		}

		// REACHING HERE MEANS THE ROW CARRIES NO CREDENTIAL EVIDENCE AT ALL: no reference, no
		// orphan marker and no unsettled cleanup obligation. Nothing can authenticate as the
		// principal, so any ACL binding naming it is inert and the row can be removed directly.
		// This is what keeps the registry usable without Kafka.
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

	// STEP 3 — REVOKE, using the row that still exists, under a renewed claim and a bounded
	// phase deadline. Revocation is a sequence of administrative round trips — the ACL bindings
	// and then the SCRAM credential — and this method arrived with no deadline of its own, so
	// without the bound the phase could outlive the claim and STEP 4 would then delete a row
	// another operation had taken over.
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
		//
		// The tombstone is stamped before the broker is touched, so on its own it cannot
		// distinguish "the broker said no" — which needs the administrative principal's grants
		// or the broker's reachability fixed before any retry can work — from "the process never
		// got this far", which needs only the retry. One count cannot tell an operator which
		// they are looking at, so they would retry into a refusal.
		//
		// It runs on the DURABILITY slice rather than on ctx, because the commonest cause of the
		// failure being reported is a deadline that has also spent the caller's own.
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
			// failure: the principal and the broker's own words stay in the log line above,
			// and the caller receives the diagnosis plus the fact that a retry is safe.
			// Revocation is idempotent at the broker, so repeating it cannot make things
			// worse — and the tombstone on the row is what makes it findable.
			NewSubscriberErrorDetail(
				"Revoking the subscriber's Kafka access failed", pending.SubscriberID, true,
			),
		)
	}

	// STEP 3b — WITHDRAW THE KEY-SCOPE BINDING at the declared component (SEC-01), now that the
	// broker credential is gone.
	//
	// ORDER MATTERS AND THIS IS THE SAFE END OF IT. The broker's revocation is the authoritative
	// one: the principal can no longer authenticate anywhere the credential was accepted,
	// gateway included, because the gateway terminates the same SASL exchange. So by the time
	// this runs there is no access left for a failure here to leave behind — only a stale entry
	// in the component's binding table.
	//
	// It is therefore NOT allowed to fail the deregistration. Doing so would leave an operator
	// unable to remove a subscriber while a component is unreachable, in order to finish a
	// cleanup for access that is already gone. The failure is logged at WARNING with the
	// principal's pseudonym and the remedy, which is the honest description of a hygiene
	// obligation the operator can discharge at the component.
	//
	// A deployment that declares no component does nothing here.
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

	// STEP 4 — DELETE, now that the broker-side cleanup is confirmed, and under the claim. A
	// miss means the claim was lost while the revocation was in flight; the row then stays
	// tombstoned, which is the recoverable direction, and the conflict tells the caller so.
	removed, err := store.TakeEventSubscriber(ctx, subscriberID, fence.token)
	if err != nil {
		// The access is already gone, so this residue is the harmless direction: a registry row
		// describing a subscriber that can no longer authenticate. Retrying the deregistration
		// removes it, and the tombstone is what makes the row identifiable as needing that.
		// F14: THE ROW MUST STOP DESCRIBING A CREDENTIAL AND AN OUTSTANDING REVOCATION.
		//
		// The broker has confirmed the revocation, so the tombstone — which means "a principal
		// may still authenticate" — is now false, and credential_reference names a credential
		// that no longer exists. Left as they were, this row is counted by
		// CountSubscriberRevocationsPending and raises the CRITICAL oldest-revocation alert
		// whose runbook sends an operator to delete a SCRAM credential by hand that is already
		// gone — and it makes a row where a credential really IS unaccounted for
		// indistinguishable from this inert residue.
		//
		// ClearSubscriberCredential nulls both halves and both markers in one statement, so
		// there is no window in which the row describes half of each. Best-effort: the delete
		// that just failed uses the same connection, and the caller is already receiving an
		// error.
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

	// THE ONE ABSOLUTE DEADLINE. Everything this request does is inside it: the lookup, up to
	// four broker round trips, the issuance record, AND any compensation that follows a
	// failure. AAP-02.
	//
	// Two contexts come out of one instant, and the difference between them is the whole fix:
	//
	//   - `ctx` carries the deadline as a VALUE and no timeout of its own. Compensation reads
	//     it after detaching from cancellation — WithoutCancel strips a real deadline, so a
	//     value is the only way to carry one across that boundary — and re-imposes it. That is
	//     what stops a cleanup from starting a fresh budget after the request's bound.
	//   - `forward` carries the deadline as a real timeout, minus the compensation reserve, and
	//     is what every step of the forward path runs on. It expires EARLIER than the absolute
	//     instant, which is precisely why there is time left to compensate in on the one
	//     failure that most needs it.
	//
	// A caller whose own deadline is earlier keeps it: WithDeadline never extends.
	absolute := time.Now().Add(s.budget())
	ctx = withSubscriberIssuanceDeadline(ctx, absolute)

	forward, cancel := context.WithDeadline(ctx, absolute.Add(-s.compensationReserve()))
	defer cancel()

	// Shadowed deliberately, so no step below can accidentally use the unbounded parent: the
	// forward path must be the one with the timeout, and the absolute instant reaches the
	// cleanup helpers through the value that travels on it either way.
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
	//
	// Two overlapping issuances cannot both be right: the broker keeps one password and the
	// registry may record the other. The claim makes the second caller a conflict instead of a
	// silent loser — see fenceSubscriber. It is taken before the row is read so that everything
	// this call decides from is read under the claim.
	fence, err := fenceSubscriber(ctx, store, subscriberID)
	if err != nil {
		// A budget spent on the claim is a TIMEOUT, not a server fault, and this is the
		// commonest place for it to be spent: the claim is the first write of the request.
		// A conflict — another operation holds the claim — passes through unchanged, because
		// it is true whether or not the deadline also expired.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "claiming the subscriber for provisioning", err,
		)
	}
	// THE RELEASE LEAVES THE RESPONSE PATH. It is a write the caller's answer does not depend
	// on, so with a scheduler installed the issuance budget bounds the RESPONSE rather than the
	// response plus its cleanup; with none — a CLI, a test — schedule runs it inline and the
	// ordering is exactly what it was.
	//
	// Any compensation this attempt owes has already run by the time this does, because it runs
	// inline on the failure paths below. That order is load-bearing rather than tidy: Kafka
	// stores one SCRAM credential per principal, so a claim released before the compensation
	// finished would let an immediate retry mint a working credential and have this attempt's
	// cleanup delete it moments later. Holding the claim until then makes that retry a conflict
	// instead, which is a refusal the caller can act on. The heartbeat keeps the claim alive
	// throughout.
	// The compensation this attempt turns out to owe, and the claim release, in ONE scheduled
	// task — compensation FIRST. The order is load-bearing rather than tidy: Kafka stores one
	// SCRAM credential per principal, so a claim released before the compensation finished would
	// let an immediate retry mint a working credential and have this attempt's cleanup delete it
	// moments later — a 200 whose password stops working, with nothing recording why. Holding the
	// claim until the compensation is done makes that retry a conflict instead, which is a
	// refusal the caller can act on. The fence's heartbeat keeps the claim alive throughout.
	// Takes the compensation window as a parameter rather than closing over one resolved at the
	// top of the request, so the compensation and the claim release SHARE the window the
	// scheduled task opens. One request, one window: nesting subscriberCleanupContext inside it
	// clamps to the same instant rather than carving a second slice.
	var owedCompensation func(base context.Context)

	defer func() {
		compensate := owedCompensation

		s.schedule(func() {
			// THE RELEASE'S WINDOW IS RESOLVED HERE, not when the issuance started.
			//
			// Resolved at the top it is measured from the wrong clock: a forward path that used
			// most of its budget hands control back after the window has already closed, and the
			// release is then lost to the very deadline it exists to survive. The claim would
			// stand until its lease expired, refusing every retry in the meantime — one slow
			// broker call turned into a lease-long outage for that subscriber.
			//
			// subscriberCleanupContext is what makes "detached from cancellation, still bounded"
			// true of it: past the request's bound it yields the floor rather than a dead
			// context, which is the whole of CLEAN-01.
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

	// THE ENFORCEMENT FACT, READ ONCE. Both halves of the key-scope decision consult it — whether
	// this subscriber may be issued a credential at all, and which endpoint that credential
	// names — so the two cannot disagree.
	//
	// IT COMES ENTIRELY FROM CONFIGURATION. Blnk ships no component that authorises record keys
	// and serves no records itself, so on the default KAFKA_KEY_SCOPE_ENFORCEMENT=none nothing
	// evaluates a partition-key prefix and issuance for a prefix-recording row FAILS CLOSED
	// below. It is true only where an operator has declared an enforcing component in front of
	// the brokers, and keyScopeGateway is then the address of that component — which is also
	// what the credential must name in place of the broker list.
	keyScopeGateway, keyScopeEnforced := s.keyScopeEnforcement()

	// FAIL CLOSED ON AN UNENFORCEABLE KEY SCOPE. A row recording a partition key prefix
	// describes a boundary Kafka's authorizer has no dimension for, so a credential carrying
	// topic Read would read every record on every authorised topic — other ledgers' and other
	// subscribers' included. Disclosure was tried in place of a boundary and is not one: the
	// party asked to apply the filter is the party holding the credential.
	//
	// What makes issuance PROCEED here is that the narrowing is real AND something is declared to
	// keep it: aclEntries withholds topic Read from a key-scoped principal, verifyKeyScopeBoundary
	// proves that withholding against the bindings actually sent before the password becomes
	// returnable, and the declared key-authorising component is the path its records take instead.
	// Where no component is declared — the shipped default — this REFUSES, and
	// requireProvisionableKeyScope names both remedies.
	if err := requireProvisionableKeyScope(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND FAIL CLOSED IN THE OTHER DIRECTION (SEC-01). The refusal above covers a key scope
	// nothing enforces; this one covers the credential that ESCAPES a declared key-scoped model —
	// a subscriber with no prefix, issued literal topic Read, reading every ledger on a shared
	// topic while every other subscriber in the deployment is confined. One prefix-less
	// registration was all it took, and the DTO makes the field optional, so nothing marked the
	// occasion.
	if err := requireKeyScopeWhenEnforced(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// AND MAKE THE WHOLE-TOPIC MODEL A DECISION RATHER THAN A DEFAULT. Where no key-scoped model
	// is declared, a granted topic is read in full — which is the mandated access model and is
	// correct for a single-tenant deployment. In secure mode Blnk asks the operator to have said
	// so, once, instead of reaching the widest credential it can issue by configuring nothing.
	if err := requireAcknowledgedSharedTopicAccess(subscriber, keyScopeEnforced); err != nil {
		return SubscriberCredential{}, err
	}

	// And refuse a subscriber authorised for nothing, so a live principal that can read
	// nothing is never handed out looking like one that can. Checked in the same place and for
	// the same reason as the three above: before a secret exists and before the broker is
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

	// THE DECLARED ENFORCING ENDPOINT WINS over the advertised broker list for a key-scoped
	// subscriber, and it has to: where one is declared, that subscriber's connection is
	// terminated by it rather than by a broker. Reporting the brokers instead would hand out a
	// credential declaring key-scoped isolation together with an endpoint that bypasses the
	// component enforcing it.
	//
	// THE CONDITION IS THE ADDRESS RATHER THAN THE ENFORCEMENT FACT, so that the two stay
	// separable: the fact decides whether issuance happens at all (requireProvisionableKeyScope
	// above), and a non-empty address decides which endpoint the credential names. They agree by
	// construction here, because keyScopeEnforcement derives both from one configuration read
	// and reports enforcement only when the address list is non-empty and distinct from
	// KAFKA_BROKERS — but a substitution written against the boolean would silently hand out a
	// credential naming NO endpoint if that ever stopped being true.
	//
	// A subscriber with no prefix keeps the advertised broker list even where an enforcing
	// component is declared: it has no key scope to enforce, so routing it through that
	// component would add a hop which authorises nothing.
	if subscriber.DeclaresKeyScope() && len(keyScopeGateway) > 0 {
		subscriberBrokers = keyScopeGateway
	}

	// THE DECLARED COMPONENT IS ASKED TO CONFIRM THE BOUNDARY, before a secret exists and before
	// the broker is touched (SEC-01).
	//
	// Everything above this line has established that the row records a key scope and that the
	// deployment declares a component to keep it. Neither of those is evidence that the component
	// exists: they are two configuration values and a column. This call is the evidence — an
	// authenticated request the component must answer with "yes, for this principal, with this
	// exact prefix" — and it is what stops Blnk minting a credential whose response declares an
	// enforced key boundary nobody was ever asked about.
	//
	// POSITIONED HERE for the reason every other refusal on this path is: no password has been
	// generated, no ACL created, no registry row written, so a refusal leaves nothing behind.
	if subscriber.DeclaresKeyScope() && keyScopeEnforced {
		if err := s.attestKeyScope(ctx, subscriber); err != nil {
			return SubscriberCredential{}, err
		}
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
		// bounded rendering is of the ERROR, never of the inputs — and the caller receives
		// no cause at all.
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

	// The claim is CONFIRMED on the way into the broker phase, so the credential about to be
	// written cannot be interleaved with another operation's. The issuance budget already caps
	// this phase — it caps the whole request — so the phase context adds only the renewal, and
	// the caller's shorter deadline is what continues to govern the round trips.
	provisionPhase, endProvisionPhase, phaseErr := fence.brokerPhaseContext(ctx)
	if phaseErr != nil {
		endProvisionPhase()

		// Nothing has been written and the generated secret is dead. Classified so that a
		// budget spent waiting on the renewal reads as the timeout it was.
		return SubscriberCredential{}, s.classifyIssuanceTimeout(
			ctx, subscriberID, "confirming the provisioning claim before provisioning", phaseErr,
		)
	}

	// DeferCompensation is set only when a scheduler is installed. Without one the compensation
	// would be "deferred" to an inline call moments later, which is the same two round trips in
	// a less obvious place — so the request asks for the inline behaviour it is actually going to
	// get, and the result's flags describe what really happened.
	request := NewSubscriberProvisioningRequest(subscriber, password)
	request.DeferCompensation = s.defersWork()

	result, err := admin.ProvisionSubscriberPrincipal(provisionPhase, request)
	endProvisionPhase()

	if err != nil {
		if result.CompensationOwed {
			// The credential is at the broker with no boundary, and undoing it is two round trips
			// this response must not wait for. Scheduled with the fence still held, so a retry
			// cannot have its own fresh credential revoked by this cleanup.
			owed := result
			owedCompensation = func(base context.Context) {
				cleanup, cancel := subscriberCleanupContext(base)
				defer cancel()

				// The outcome is not returned anywhere: this runs after the response. It is
				// logged by the compensation itself — at ERROR, naming the principal and the
				// manual remedy, when the credential could not be revoked.
				_ = admin.CompensateProvisioning(cleanup, owed)
			}
		}

		// ctx and the claim are threaded in because a broker that was left holding something
		// is a DURABLE obligation, not just a log line, and recording it is a write that needs
		// both. The claim is still this caller's here — the deferred release has not run — so
		// the write is fenced like every other mutation on the row.
		return SubscriberCredential{}, s.provisioningFailure(ctx, subscriber, fence.token, result, err)
	}

	// THE RENEWAL, between the last broker round trip and the write it protects.
	//
	// Provisioning is up to four administrative round trips with their own timeouts, so an
	// issuance can legitimately still be working when its lease expires — at which point another
	// operation claims the subscriber and both proceed. A renewal placed anywhere EARLIER
	// re-confirms a claim that was never in doubt; this is the only position from which it can
	// refuse a write that is about to race.
	//
	// A refusal is a CONFLICT, and it is routed through recordFailure's conflict arm rather than
	// answered here, because the two need the identical response: the credential is already at
	// the broker, revoking it would destroy whatever the operation that took the claim has
	// written over it, and the honest state is "the registry's reference may not describe what
	// authenticates" — which is the orphan marker.
	if renewErr := fence.renew(ctx); renewErr != nil {
		residue, failure := s.recordFailure(ctx, admin, subscriber, fence.token, renewErr)

		return SubscriberCredential{}, s.classifyPostProvisioningTimeout(
			ctx, subscriber, "confirming the provisioning claim before recording the issuance",
			failure, residue,
		)
	}

	issuedAt := s.clock()

	// Under the claim as well as the reference CAS. The reference alone cannot detect a caller
	// whose lease expired: while the rightful new owner is still provisioning it has recorded
	// nothing, so the stored reference is still the one this caller observed, and its write
	// would land and then refuse the winner.
	if err := store.RecordSubscriberCredentialIfUnchanged(
		ctx, subscriberID, observedReference, reference, issuedAt, fence.token,
	); err != nil {
		// recordFailure owns the compensation and its own logging, and REPORTS WHAT THE BROKER
		// IS LEFT HOLDING once it has run. The classification then re-reports a spent budget
		// here as the timeout it was, and runs on the OUTSIDE so the compensation happens first
		// either way, leaving recordFailure's typed conflict — a superseded issuance —
		// untouched.
		//
		// CLASSIFIED BY THE POST-BROKER CLASSIFIER, not the pre-broker one. The broker has been
		// written to by the time this line is reachable, so the pre-broker classifier's premise
		// — that nothing can have been written, and therefore that no state flags are needed —
		// is false here. Using it left a failed revocation reported as credential_written=false,
		// compensated=false: a caller told to retry, and told nothing at all about the live
		// principal the retry was supposed to reconcile.
		// OFF THE RESPONSE PATH when a scheduler is installed, exactly as the provisioning
		// failure above is. Neither of this compensation's writes changes the caller's answer —
		// revoking the credential at the broker and clearing the registry's reference to it —
		// and both used to run inline on fresh budgets after the issuance budget was already
		// spent, which is how a five-second contract came to answer in twenty.
		//
		// The residue reported to the classifier is then what is TRUE at the moment of
		// answering: the credential is at the broker and its cleanup has not run yet. Reporting
		// the outcome of a cleanup that has not happened would be a guess, and the flags exist
		// precisely so a caller can tell whether a secret it does not hold may exist.
		//
		// Scheduled into the SAME task as the claim release, so the order is unchanged from the
		// inline version: broker first, registry second, fence last. Releasing the claim before
		// the revocation finished would let an immediate retry mint a working credential and
		// have this attempt's cleanup delete it moments later.
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
		// Derived from the SAME row the prefix above was read from, so the two cannot disagree.
		// Reporting a prefix without saying where it is enforced is what let the field be read as
		// a broker boundary, and reading it as one is what the withheld credential was reaching
		// for. Always populated — "none" when no scope is recorded — so a client branches on this
		// rather than on whether the prefix happens to be empty.
		// THE ENFORCEMENT POINT AS ISSUED, not as the row would read on its own.
		// EventSubscriber.KeyScopeEnforcement reads only the row, which knows nothing about
		// whether the deployment declared an enforcing component; an ISSUED credential exists
		// only where one is declared, so a key-scoped credential always reports broker_gateway
		// and can never report an enforcement point that is not running.
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

	// AND THE KEY-SCOPE LINE, for a key-scoped row only — logKeyScopeDisclosure returns without
	// emitting when no prefix is recorded, so this is one call rather than a condition here.
	//
	// It is emitted from the issuance path because that is the claim it makes: that THIS
	// credential was granted Describe and not Read, and that the subscriber's records travel
	// through the declared component instead. warnOnKeyScopeRecordedAfterIssuance is written against
	// its existence — it covers only the reverse order, on the stated grounds that issuance
	// already reports this one — so leaving the call site out would silently make that reasoning
	// false and leave the transition undiagnosed in the order operators take most often.
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

// provisioningFailure turns a broker-side provisioning failure into the right typed error and
// records what state the broker was left in.
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
// No branch records an issuance and no branch returns a credential, so a caller that receives
// an error holds no secret. No message contains the password: it is not a field of the result
// and is never passed to this function.
//
// # EVERY BRANCH ALSO SETTLES WHAT THE BROKER WAS LEFT HOLDING
//
// Before choosing the typed error, this function reconciles the registry with the broker state
// the result describes — see settleProvisioningRemnant. That is not incidental tidying. Two of
// the outcomes above used to be recorded ONLY as a log line: a compensation that failed left a
// live credential with no boundary, and a compensation that succeeded left the row still naming
// a credential that no longer exists. Neither was queryable, retryable or alertable, so both
// depended on somebody reading the right line.
//
// Parameters:
//   - ctx context.Context: the request's context. Used only to DERIVE a fresh bounded context
//     for the settlement writes, because the deadline that failed is frequently the reason this
//     function was called at all.
//   - subscriber *model.EventSubscriber: the row being provisioned, for the log fields.
//   - fenceToken string: the caller's provisioning claim, which the settlement writes are
//     conditional on.
//   - result SubscriberProvisioningResult: the only source of truth for what reached the
//     broker.
//   - cause error: the provisioning error.
//
// Returns:
//   - error: a typed ErrKafkaUnavailable, ErrSubscriberAccessExceedsAuthorization or
//     ErrSubscriberProvisioningFailed. The settlement outcome deliberately does NOT change it:
//     the caller's failure is the provisioning failure, and an obligation raised on top of it is
//     an operational fact rather than a different answer to the request.
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

	// Settled BEFORE the error is chosen, so that every branch below settles — including the
	// timeout and cancellation branches, where the result flags are the only evidence of what
	// the broker was left holding and where the temptation to treat "unknown" as "nothing" is
	// strongest.
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
			// Bounded, and the cause stays in the log line above. See
			// SubscriberErrorDetail: NewAPIError both re-logs its details unsanitized and
			// serialises them into the response, so passing the cause here undid the
			// sanitizing this function had just done — on both sides at once.
			NewSubscriberErrorDetail(
				"Kafka is not configured", subscriber.SubscriberID, false,
			),
		)

	case errors.Is(cause, ErrForeignACLGrantsAccess):
		// Named as its own branch because the three state-based branches below describe what
		// the broker was LEFT in, and none of them describes what went wrong here. Falling
		// through to them logged "the ACL grant failed", which is the opposite of the truth:
		// the grant succeeded, and the refusal is that a binding Blnk does not own grants MORE
		// than the grant does. An operator reading the wrong cause looks for a broker fault
		// instead of the hand-made binding that is the actual blocker.
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
			// re-reads the same broker state and refuses again. It becomes retryable only once
			// a human removes the bindings or records the access on the subscriber, which is
			// what the log line above asks for.
			s.provisioningDetail(
				"The Kafka principal holds ACL bindings granting access beyond its recorded authorization",
				subscriber, result, false,
			),
		)

	case errors.Is(cause, ErrSubscriberKeyScopeUnenforced):
		// SEC-06. Named as its own branch for the same reason the foreign-ACL refusal above is:
		// the state-based branches below describe what the broker was LEFT in, and none of them
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
			// SLA-01: THE TIMEOUT CODE, not EVENT_KAFKA_UNAVAILABLE. This branch answered 503
			// while the registry half of the SAME issuance answered
			// SUBSCRIBER_PROVISIONING_TIMEOUT (504) for the identical condition — one wall clock
			// running out — so a client had to know which internal dependency was slow in order
			// to recognise a timeout. A 503 additionally asserts that a dependency is DOWN,
			// which is a different fact and invites a different retry policy. What the broker
			// was left holding stays in the DETAIL, which is the only place it can be: a status
			// code cannot say whether a credential the caller does not hold may already exist.
			apierror.ErrSubscriberProvisioningTimeout,
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
			// The same code as the deadline branch above, for the same reason: one wall clock
			// ran out, and the caller should not have to know which dependency noticed first.
			apierror.ErrSubscriberProvisioningTimeout,
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
	//
	// AUTH-03: the ONE refusal whose remedy is operational rather than a retry gets its own
	// wording. Every branch above logs what happened at the broker; this says what the caller
	// has to DO about it. A generic "provisioning failed at the broker" would send an operator
	// to look at broker health for a principal whose only problem is a hand-made ALLOW binding
	// that Blnk deliberately will not delete — and the response is the only place they will
	// see it, because a 4xx/5xx body is what reaches an integrator and the log line does not.
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

// settleProvisioningRemnant reconciles the registry with whatever a failed provisioning left at
// the broker, and records a DURABLE obligation whenever it cannot.
//
// # THE TWO STATES THIS EXISTS FOR
//
// COMPENSATION FAILED — result.CredentialWritten is still true after provisioning tried to
// revoke. A SCRAM credential exists for a principal with no authorization boundary. Before this,
// the entire record of that was one log line saying "revoke it by hand immediately", which
// nothing queried and nothing retried. A durable marker turns it into a to-do item the
// settlement pass finds and finishes.
//
// COMPENSATION SUCCEEDED — the credential was revoked and confirmed gone, but the ROW may still
// name one. That row is not merely stale: because provisioning UPSERTS the SCRAM credential, a
// re-issue that later failed and compensated destroyed the credential the row named as well as
// the one it had just written. The registry then reported a provisioned subscriber whose
// credential authenticates nothing at all — and every consumer of the registry, including the
// migration report and the credential-issuance CAS, read that as truth. So the record is cleared
// here, and if the clear itself fails the divergence becomes an obligation rather than a warning.
//
// NOTHING REACHED THE BROKER — nothing is owed, and no marker is written. Raising an obligation
// for a failure that changed no state would put every rejected request into the settlement
// backlog and drown the two states above.
//
// # WHY A FRESH CONTEXT
//
// The commonest reason for being here is a spent budget, and a cleanup inheriting that deadline
// would fail on arrival — leaving precisely the state it exists to record. subscriberCleanupContext
// detaches from the caller's cancellation and grants its own bounded budget, the same way
// recordFailure's cleanups already do.
//
// # WHY NO ERROR IS RETURNED
//
// The caller's answer is the provisioning failure. A settlement problem is an operational fact
// layered on top of it, not a different answer to the request, and reporting it instead would
// replace an accurate diagnosis with a misleading one. Everything it cannot do it writes down —
// as an obligation where one is possible, and at ERROR where even that fails.
//
// Parameters:
//   - ctx context.Context: the request's context, used only as the parent to detach from.
//   - subscriber *model.EventSubscriber: the row being provisioned.
//   - fenceToken string: the caller's provisioning claim; every write here is conditional on it.
//   - result SubscriberProvisioningResult: the only source of truth for what reached the broker.
//   - fields logrus.Fields: the caller's log fields, so a settlement line carries the same
//     subscriber and principal as the failure line beside it.
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

	// A credential that is still written is a live principal with no boundary. There is nothing
	// to clear on the row — the issuance was never recorded — so the only remedy is broker-side,
	// and that is what the obligation asks settlement to perform.
	if result.CredentialWritten {
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal with no authorization boundary")

		return
	}

	// Confirmed compensated. The broker is clean, so the row is what is wrong.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		return
	}

	// The clear failed, so the registry over-reports. Settlement's remedy is the same one it
	// would apply to a live credential — revoke, which is a harmless no-op against a principal
	// whose credential is already gone, then clear the row — so the same marker covers both.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")
}

// recordCleanupObligation writes the credential-cleanup marker, and escalates when it cannot.
//
// It is the last line of defence, so its failure branch is deliberately loud: if the marker
// cannot be written there is no durable record of the divergence anywhere, and the log line is
// once again the only thing that knows. Saying exactly that — rather than a generic write
// failure — is what tells an operator the difference matters.
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
	detail.CompensationPending = result.CompensationOwed

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

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningTimeout,
		verdict.Message,
		NewSubscriberErrorDetail(verdict.Reason, subscriberID, true),
	)
}

// issuanceTimeoutVerdict is the shared decision behind both timeout classifications: whether a
// failure should be re-reported as a spent budget, and the wording to report it with.
//
// It exists so the pre-broker and post-broker classifiers cannot drift. They differ only in what
// they can SAY about broker state; the question "was this a timeout, and what do we call it" has
// one answer, and it is decided here once.
type issuanceTimeoutVerdict struct {
	// Rewrite reports whether the cause should be replaced by a typed timeout. False means the
	// caller returns the cause unchanged, which is the common case.
	Rewrite bool

	// Cancelled distinguishes a caller that went away from a budget that ran out. Both are
	// timeouts to this service and neither is a defect in it, but they are worded differently
	// because only one of them is worth an operator's attention.
	Cancelled bool

	// Message is the client-facing sentence.
	Message string

	// Reason is the operator-facing detail. The post-broker classifier appends to it.
	Reason string
}

// issuanceTimeoutFor decides whether a failure during credential issuance is really a spent
// budget, and produces the wording for it.
//
// A failure counts as a timeout only when BOTH facts hold: the context has expired, and the error
// is an internal-server-class one. The second condition is what keeps a genuine typed refusal — a
// missing subscriber, a lost fence, a superseded issuance — from being relabelled a timeout merely
// because the budget also happened to run out. Those refusals are the same refusals whether they
// arrive early or late, and a caller must be able to act on them.
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

// classifyPostProvisioningTimeout is classifyIssuanceTimeout for the failures that happen AFTER
// the broker has been written to.
//
// # Why a second classifier rather than one
//
// The pre-broker classifier's premise is that nothing can have been written yet, so the detail it
// builds carries no broker-state flags: credential_written and compensated are both left false
// because both are known to be false. Reached after provisioning, that premise is wrong in the
// most misleading possible direction. A compensating revocation that itself failed leaves a live
// SCRAM principal no registry row accounts for, and reporting credential_written=false told the
// caller to retry while saying nothing about the principal the retry was meant to reconcile.
//
// # What it adds to the reason
//
// The residue, in words rather than as a flag the reader has to interpret: a credential that could
// not be revoked needs manual revocation, and a credential that was revoked means the subscriber
// has no access until issuance is repeated. Those are different jobs for whoever reads the error,
// so they are stated rather than implied.
//
// Parameters:
//   - ctx context.Context: the issuance context, read for its expiry.
//   - subscriber *model.EventSubscriber: the row being provisioned. A nil row is tolerated.
//   - stage string: what was being attempted, for the reason line.
//   - cause error: the failure, already compensated for by recordFailure.
//   - residue SubscriberProvisioningResult: what the broker is left holding.
//
// Returns:
//   - error: a typed SUBSCRIBER_PROVISIONING_TIMEOUT carrying the residue, or cause unchanged when
//     this was not a timeout.
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

	return apierror.NewAPIError(
		apierror.ErrSubscriberProvisioningTimeout,
		verdict.Message,
		s.provisioningDetail(reason, subscriber, residue, true),
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
//   - fenceToken string: the provisioning claim the failed issuance holds. Required for the
//     record-clearing write, which is conditional on it — a stale owner must not be able to
//     blank the credential record a newer issuance has since written.
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
		//
		// Two conflicts reach here — a superseded credential and a LOST FENCE — and the same
		// reasoning covers both: Kafka stores ONE SCRAM credential per principal, so by the time
		// either is detected the broker may already hold the OTHER operation's password.
		// Revoking would destroy a credential that works, for a subscriber that has been handed
		// it and is about to connect with it.
		//
		// But "may or may not be the one now live at the broker" IS an unaccounted credential:
		// the registry's reference may not describe what authenticates. That ambiguity used to
		// be a log line and nothing else. The orphan marker makes it visible to the
		// orphaned-credential gauge and its alert, and it is UNFENCED for the reason this arm
		// exists — the claim is frequently the very thing that was lost, so a write conditioned
		// on it would be absent from exactly the case that produces the state.
		//
		// It is settled automatically by the remedy the log line names: issuing once more,
		// serially, replaces whatever is live and clears the marker in the same statement.
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
		// settleProvisioningRemnant records for a failed compensation — a credential live at the
		// broker that nothing in Blnk accounts for — reached by a different route, and it needs
		// the same remedy. Recorded on the cleanup context, so the spent budget that is often
		// the reason for being here does not also lose the record of it.
		s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
			"a SCRAM credential exists for this principal that no registry row records")

		// AND THE ORPHAN MARKER, which is not the same fact. The obligation above is FENCED and
		// tells the settlement pass there is broker-side work to finish; this one is UNFENCED and
		// is what the orphaned-credential gauge and its alert count. Two different readers, two
		// different remedies — settlement discharges the first, and re-issuing or deprovisioning
		// settles the second — so recording only one leaves the other blind.
		//
		// THE DURABILITY SLICE, not the compensation one. The revocation above has just spent
		// whatever the compensation phase had, so writing the marker on that context would find
		// it exhausted exactly when the exposure is real.
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

	// The row survives and still names whatever credential it held BEFORE this issuance — but
	// that credential no longer exists: the upsert replaced it and the revocation above removed
	// the replacement. Clearing the reference is what stops the registry claiming access that
	// has gone, which every reader of it (an operator, the migration report, a reconciliation)
	// would otherwise believe. Best-effort, because the write that just failed is the same
	// connection this one uses.
	if s.clearCredentialRecord(cleanup, subscriber, fenceToken, fields) {
		logrus.WithFields(fields).Warn(
			"event subscriber: recording the issuance failed, so the credential written at the " +
				"broker was revoked and the registry's credential record was cleared; the " +
				"subscriber has no Kafka access until credentials are re-issued",
		)

		return SubscriberProvisioningResult{Compensated: true}, cause
	}

	// The revocation succeeded but the row still names a credential that no longer exists, so
	// the registry over-reports this subscriber's access. clearCredentialRecord has already said
	// so at ERROR; the obligation is what makes it a to-do item settlement will finish rather
	// than a line waiting to be read.
	s.recordCleanupObligation(cleanup, subscriber, fenceToken, fields,
		"the broker-side credential was revoked but the registry still records one")

	return SubscriberProvisioningResult{Compensated: true}, cause
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
//   - fenceToken string: the provisioning claim the caller holds. The write is conditional on
//     it, so a caller whose lease expired cannot blank a newer issuance's record — which would
//     leave the registry reporting "not provisioned" for a subscriber holding a live credential.
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
	// tombstoned row is a subscriber whose access is being taken away, and this operation takes
	// access away — refusing it would block the one manual remedy available for a
	// deregistration whose broker step keeps failing. The guard exists to stop access being
	// GRANTED or WIDENED mid-removal, and revocation does neither.
	admin, err := s.provisioner()
	if err != nil {
		return err
	}

	// AUTH-02. Clearing credential_reference with no broker to revoke against erases the only
	// evidence of what has to be revoked, while the credential keeps authenticating.
	if !admin.IsConfigured() {
		if err := refuseUnconfirmableBrokerWork(
			subscriber, "revoking this subscriber's Kafka credential",
		); err != nil {
			return err
		}
	}

	// With no broker AND no credential evidence — no reference, no orphan marker, no unsettled
	// cleanup obligation — there is nothing to revoke, so the registry record is simply cleared. A
	// deployment that has never configured Kafka can still tidy a row that predates that decision.
	if admin.IsConfigured() {
		// The claim is confirmed on the way in and the phase is bounded, for the same reason
		// deregistration's is: revocation is several administrative round trips and this method
		// arrived with no deadline of its own, so without the bound the phase could outlive the
		// claim and the clear below would then blank a newer issuance's record.
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
// received pushes on, and returns the row as it now stands.
//
// # It does NOT reconcile Kafka access, and that is the point
//
// This used to delegate to UpdateSubscriber, which reconciles the subscriber's ACL bindings
// at the broker around its write — pruning what the authorization no longer implies and
// granting what it adds. Recording a webhook URL changes no authorization, so every one of
// those broker round trips was work with no possible effect, and each was a way for this
// call to fail for a reason that has nothing to do with it: a broker outage answered 503
// SUBSCRIBER_PROVISIONING_FAILED, and a concurrent issuance holding the provisioning fence
// answered 409, both for a request that only ever wanted to write one column of migration
// metadata. An operator recording a URL during an unrelated Kafka incident was simply
// unable to.
//
// So this is a REGISTRY WRITE and nothing more: no admin client is resolved, no fence is
// taken, and no binding is touched. The statuses it can answer are therefore only the ones
// a registry write can produce.
//
// # It clears migrated_at, atomically
//
// webhook_url and migrated_at are one fact between them — "receives legacy pushes at this
// address" and "no longer receives legacy pushes" — so a row holding both asserts the
// opposite of itself. UpdateSubscriber wrote the URL and left the timestamp, so recording an
// endpoint on an already-migrated subscriber produced exactly that row, silently. The
// repository now writes both columns in one statement, and the schema refuses the
// combination outright.
//
// Clearing the timestamp is the correct direction: a subscriber with an endpoint recorded
// again is, by that act, back in the population awaiting migration. Refusing the write
// instead would leave an operator unable to correct a record whose correction is the entire
// purpose of the column.
//
// The URL is validated at the persistence boundary — HTTPS only, and no loopback,
// link-local, private-range or unqualified destination — because the column is a FUTURE
// REQUEST SINK: nothing sends to it today, and the moment anything does, whatever is stored
// becomes a request Blnk makes from inside its own network.
//
// Parameters:
//   - ctx context.Context: cancels the write and the read-back.
//   - subscriberID string: the business key.
//   - webhookURL string: the endpoint to record. Blank is refused, because a subscription
//     with no URL records nothing; use ClearLegacyWebhookSubscription to remove one.
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
	//
	// This is the DEDICATED route for recording a legacy endpoint, so a guard that covered only
	// the generic update left the retirement enforceable on one path and bypassable on the
	// other — and this is the path a migration script would reach for. Clearing an endpoint
	// already on the record stays available for ever; requireRecordableWebhookURL only refuses
	// a non-blank value.
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

// ClearLegacyWebhookSubscription removes a single subscriber's recorded legacy endpoint.
//
// It is the one-row form of the retention rule PurgeMigratedWebhookURLs applies in bulk, for
// an operator correcting a record or honouring an erasure request before the retention period
// elapses. migrated_at is untouched: whether the subscriber migrated is an audit fact that
// forgetting an address does not change, and it is what migration-progress reporting counts.
//
// It does not reconcile Kafka access either, for the reason given on
// RecordLegacyWebhookSubscription — and it deliberately does not stamp a migration.
// Deleting an address is not evidence that a subscriber moved to Kafka; conflating the two
// would let an erasure request report a migration that never happened.
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

// MarkSubscriberMigrated stamps the instant a subscriber completed its move from legacy HTTP
// delivery to Kafka consumption, WITHOUT touching webhook_url.
//
// Prefer CompleteWebhookMigration. This form exists for a subscriber that never had a legacy
// endpoint recorded — an onboarding completed entirely on Kafka, which is the ordinary case
// after the cutover — where there is nothing to retire and stamping alone is the whole
// transition.
//
// It must NOT be used on a row that still holds a URL: the result asserts that a subscriber
// both has and has not stopped receiving legacy pushes. Such a call FAILS rather than
// corrupting the row — the repository's statement carries `AND webhook_url IS NULL` and
// answers ErrGenConflict — so the rule is enforced rather than merely documented. It is
// enforced there, at the write, and not by a schema CHECK: see MarkSubscriberMigrated in
// database/event_subscriber.go for why the self-contradicting pair has to stay
// representable for the retention purge to have anything to clean up.
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what migration-progress
// reporting counts during the dual-delivery window. Re-stamping an already-migrated
// subscriber overwrites the instant rather than failing, because the useful question is "has
// it moved?" and a correction is a legitimate answer to "when?".
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
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"migrated_at":        migratedAt.Format(time.RFC3339Nano),
	}).Info("event subscriber: recorded as migrated to Kafka consumption")

	return migratedAt, nil
}

// CompleteWebhookMigration performs the whole cutover for one subscriber: it forgets the
// recorded legacy endpoint and records that the subscriber has finished moving to Kafka.
//
// # Why this exists rather than a clear followed by a stamp
//
// The cutover endpoint used to call ClearLegacyWebhookSubscription and then
// MarkSubscriberMigrated. Nothing spanned the two, so a failure between them left the row with
// no endpoint and no migration instant — absent from BOTH halves of the migration report, since
// "still on webhooks" is keyed on the URL and "migrated" on the instant. Progress then
// under-reported for the rest of the dual-run window, and the caller had no way to learn that
// repeating the request was the remedy.
//
// One repository statement removes the window instead of choosing which side of it to fail on.
//
// # It is the CUTOVER, and it is not the same operation as clearing a URL
//
// ClearLegacyWebhookSubscription stays, and the two are deliberately distinct.
// Clearing alone is for an operator correcting a mis-recorded endpoint, and it must NOT assert
// that a migration happened — migrated_at is an audit fact, and stamping it for a subscriber
// that has not moved is a false one. This operation asserts both facts together because that is
// what the cutover IS.
//
// It also no longer routes through the fenced authorization update, which means it no longer
// takes a provisioning claim or makes two Kafka administrative round trips to erase a URL.
// Neither column it writes has a broker counterpart, so there was nothing for those round trips
// to reconcile.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant.
//   - error: ErrSubscriberNotFound when no such subscriber exists, a typed validation error for
//     a blank identifier, or the repository's error.
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

	// The two seams that make a per-request service behave like a process-scoped one.
	//
	// The RESOLVER hands over the process's administrative client instead of letting this
	// service build its own — a transport, a connection per broker and a SASL/SCRAM handshake
	// that would otherwise be paid on every subscriber request and thrown away. It is a
	// resolver rather than the client itself so that a registry read never forces the client
	// into existence, and the borrowed client is never closed by the service.
	//
	// The SCHEDULER is where cleanup goes so that the five-second issuance budget bounds the
	// RESPONSE. It is tracked on this instance, so Close drains it.
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

// ListAndCountEventSubscribers pages the registry and counts it from one snapshot. It is the
// read behind GET /subscribers?include_count=true.
//
// The page and the total used to be two calls, and a registration between them made the total
// describe a set the page was not a slice of.
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

// CompleteEventSubscriberWebhookMigration retires a subscriber's legacy endpoint and records
// the migration in ONE transition. It is the write behind DELETE
// /subscribers/:subscriber_id/webhook-subscription.
//
// Prefer it to MarkEventSubscriberMigrated wherever a recorded URL is being retired: doing the
// two as separate calls leaves a row that is migrated with a live URL, or unmigrated with none,
// if anything fails between them.
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
//
// It is for a subscriber that never had one recorded. On a row that still holds a URL the
// schema refuses the write, because the result would assert that the subscriber both has and
// has not stopped receiving legacy pushes — use CompleteEventSubscriberWebhookMigration.
func (b *Blnk) MarkEventSubscriberMigrated(ctx context.Context, subscriberID string) (time.Time, error) {
	service := b.EventSubscribers()
	defer closeEventSubscriberService(service)

	return service.MarkSubscriberMigrated(ctx, subscriberID)
}

// CompleteSubscriberWebhookMigration forgets a subscriber's legacy endpoint and records its
// migration in ONE write. It is the write behind DELETE
// /subscribers/:subscriber_id/webhook-subscription.
//
// It replaces a clear-then-stamp pair whose partial failure left the row absent from both halves
// of the migration report — no endpoint to migrate from, and no instant to be counted by.
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
		kafkaErrorEntry("close_subscriber_management_admin", err).Warn(
			"closing the short-lived Kafka administrative client for subscriber management failed",
		)
	}
}

// maxSubscriberNameLength bounds the human label.
//
// # Why the bound exists
//
// name is the only free-text, caller-supplied column on blnk.event_subscribers, and nothing
// bounded it — so the effective limit was the global 5 MiB request-body cap. The value is
// STORED, returned in every registry response, and written into log fields on every issuance,
// revocation and provisioning failure, so an unbounded one is amplified by every read of the
// registry and by every log line that names the subscriber. One registration could inflate an
// unrelated response and a day of logs.
//
// 256 characters is generous for a human label in any script — it is a label, not a
// description — and matches the bound api/model applies to the same field and the
// event_subscribers_name_length_chk constraint in sql/1781248950.sql. The number is restated
// rather than imported for the reason maxSubscriberKeyScopeLength gives: the API package
// depends on this one and not the other way round.
const maxSubscriberNameLength = 256

// subscriberFenceWasLost reports whether an error is the repository's lost-fence conflict.
//
// # Why the caller has to be able to ask
//
// A lost fence is the one conflict that obliges the caller to compensate. Every other conflict
// — a superseded credential, a principal collision, a row already taken — means the operation
// was simply beaten to it and can be abandoned as it stands. A lost fence means something else
// may be operating on this subscriber right now and CANNOT know about the broker-side principal
// or ACL binding this operation already created, so the caller must undo it before returning.
//
// The test is on the error's DETAIL rather than on its code, because the code is the generic
// conflict the API layer already maps to 409 — introducing a second conflict code would change
// the wire contract for every existing caller to communicate something only this file acts on.
// The detail is produced in exactly one place, database.subscriberFenceLost, so the marker
// cannot drift between producer and consumer.
//
// Parameters:
//   - err error: the error returned by a fence-protected write.
//
// Returns:
//   - bool: true when the write was refused because the provisioning claim was no longer held.
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
	// type-asserted: the marker must be found whether the producer wrapped an error, a string or
	// a struct that prints one.
	if detail, ok := apiErr.Details.(error); ok {
		return strings.Contains(detail.Error(), subscriberFenceLostMarker)
	}
	if apiErr.Details != nil {
		return strings.Contains(fmt.Sprint(apiErr.Details), subscriberFenceLostMarker)
	}

	return false
}

// subscriberFenceLostMarker is the phrase database.subscriberFenceLost puts in every lost-fence
// error, and the one subscriberFenceWasLost matches on.
//
// It is a const in this file and a literal in that one deliberately: the two packages must agree
// on the phrase, and a shared symbol would put a database-layer detail in the model or force
// this file to import something only for a string. A test asserts the two still agree, so a
// reworded message fails the build's tests rather than silently disabling every compensation
// path in this file.
const subscriberFenceLostMarker = "the provisioning claim was no longer held"

// THE ISSUANCE WALL CLOCK HAS ONE CONTEXT KEY, and it is subscriberIssuanceDeadlineKey.
//
// A second key of its own — subscriberSLAKey — used to be declared here, for the same fact. Two
// keys for one fact is indistinguishable from no key at all: the issuance path attached one of
// them and the phase builder read the other, so every phase concluded that no wall clock was in
// force and fell back to a budget of its own. That is precisely the stacked-budget defect the
// wall clock exists to prevent, reintroduced by a name.
//
// A context VALUE rather than a parameter threaded through every function, because the
// compensation paths are reached from four call depths and through two seams
// (subscriberPrincipalProvisioner, eventSubscriberStore) whose signatures are deliberately
// narrow — adding a deadline parameter to them would put a scheduling concern into contracts
// that exist to bound capability. Both keys are unexported types, so nothing outside this
// package can set or read either one: the wall clock stays a property of issuance rather than
// something a caller can widen.
//
// withSubscriberSLA and subscriberSLADeadline survive as the vocabulary the phase builder reads
// in, and they now delegate to the one key.

// subscriberDurabilityReserve is the final slice of the SLA, held back so that the record of an
// unaccounted credential can always be written.
//
// It is last and smallest because it is one UPDATE against a single indexed row, and it is
// RESERVED rather than left to whatever remains because it is the slice that must not be
// starved: without it, a compensation that ran out of time leaves an exposure whose only
// representation is a log line, which is the failure ORPHAN-01 exists to remove.
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
// It survives context.WithoutCancel, which is precisely why the SLA is carried as a value: a
// compensation context is detached from the work context's CANCELLATION but must still be bound
// by the same wall clock, and a value is the only part of a context that survives that
// detachment.
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

// subscriberCompensationWindowKey is the context key the RESOLVED compensation window travels
// on, as distinct from the wall clock it was carved out of.
//
// The two are different facts and conflating them is what let nested cleanups stack. The wall
// clock is the instant the whole request must be finished by; the window is the instant THIS
// request's compensation was resolved to end at, reserves and floor already applied. A phase
// derived inside an open window must share it EXACTLY rather than carve a second one out of it,
// or a broker cleanup nested inside a registry cleanup subtracts the durability reserve twice
// and resolves a floor of its own against a later clock.
type subscriberCompensationWindowKey struct{}

// withSubscriberCompensationWindow publishes the instant a compensation phase resolved to.
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

// subscriberCompensationWindow reads back the open compensation window, if there is one.
//
// Parameters:
//   - ctx context.Context: the context to inspect.
//
// Returns:
//   - time.Time: the window's end.
//   - bool: false when no compensation is already in progress, which is the ordinary case.
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
// # How the slice is computed
//
// When an SLA is in force the phase ends at `sla - headroom`, so the slices are ABSOLUTE and a
// phase that overruns cannot borrow from the phase after it. When none is — every operation
// except credential issuance — the phase gets `fallback` from now, because those operations make
// no wall-clock promise and imposing one would abort work that is legitimately slower.
//
// A slice that has already elapsed is given a floor rather than a deadline in the past. The
// floor is not generosity: a zero-length context fails every call instantly, so a compensation
// reached slightly late would do nothing at all and report that it had tried — which is exactly
// the CLEAN-01 defect, reintroduced by arithmetic. The floor is small enough that it cannot
// meaningfully extend the SLA and large enough for one local round trip to a healthy dependency.
//
// Parameters:
//   - ctx context.Context: the context whose VALUES are kept. Cancellation is kept only when
//     detached is false.
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
	// THE WORK PHASE honours the caller's cancellation and gets no floor. Its whole purpose is
	// to keep the caller's promise, so extending it past a deadline that has already passed
	// would do work the caller has stopped waiting for — and it would hide the expiry from the
	// classifier that turns a spent budget into a timeout rather than a server fault.
	if !detached {
		sla, ok := subscriberSLADeadline(ctx)
		if !ok {
			return context.WithTimeout(ctx, fallback)
		}

		return context.WithDeadline(ctx, sla.Add(-headroom))
	}

	// A COMPENSATION ALREADY IN PROGRESS IS SHARED, to the same instant.
	//
	// One request, one compensation window, however many levels of cleanup are nested inside it.
	// Without this arm a broker cleanup reached through a registry cleanup would subtract the
	// durability reserve a second time and resolve its own floor against a later clock, so the
	// windows would stack — the shape of the defect AAP-02 reported, only smaller each time.
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
		//
		// boundedCompensationDeadline knows only the instant it was handed, which is the wall
		// clock minus this phase's headroom. When the wall clock is closer than the headroom,
		// that reduced instant is already in the past and the function's floor takes over — and
		// the floor is sized for two broker round trips, so it can land AFTER the request's real
		// bound. That is the stacked-budget failure in miniature: a compensation granted time
		// the request never had.
		//
		// So while the real bound is still ahead of us, it wins. Once it has passed there is
		// nothing left to preserve and the floor is the right answer, which is the CLEAN-01
		// trade boundedCompensationDeadline documents.
		if sla, hasSLA := subscriberSLADeadline(ctx); hasSLA &&
			sla.After(time.Now()) && deadline.After(sla) {
			deadline = sla
		}
	}

	// DETACHED FROM CANCELLATION, STILL BOUNDED BY THE REQUEST'S OWN DEADLINE. This is the only
	// context.WithoutCancel in the subscriber lifecycle, together with the one in event_admin.go,
	// and it is written as this exact expression because WithoutCancel strips a real deadline
	// along with the cancellation — the absolute instant travels as a context value precisely so
	// it can be put back here. TestSubscriberLifecycle_NoDetachedContextEscapesTheCleanupHelpers
	// pins the spelling, because a new cleanup path reaching for WithoutCancel directly is the
	// obvious thing to write and restores a compensation with no bound at all.
	base := withSubscriberIssuanceDeadline(context.WithoutCancel(ctx), deadline)

	// The window is published as well as the deadline, so a phase derived inside this one shares
	// it exactly instead of carving a second slice out of what is left.
	return context.WithDeadline(withSubscriberCompensationWindow(base, deadline), deadline)
}

// subscriberWorkHeadroom WAS RETIRED HERE, and both of its properties are kept.
//
// It computed how much of the wall clock the forward path had to leave behind — the full
// compensation plus durability reserve, or a third of what remained when the clock was tighter
// than three times that. The forward path now takes its bound from
// EventSubscriberService.compensationReserve, which is min(budget/4, subscriberCompensationReserve):
//
//   * THE PROPORTIONAL SHRINK is budget/4 rather than remaining/3, so a small budget still leaves
//     a proportional slice instead of being consumed whole by a fixed reserve. Same property,
//     expressed against the budget the service was configured with rather than against however
//     much of it a particular request has already spent.
//   * THE DURABILITY SLICE is carved out INSIDE the reserve rather than beside it:
//     subscriberCleanupContext reserves subscriberDurabilityReserve out of the compensation
//     window, so the durable markers cannot be starved by a slow compensating broker call. That
//     is strictly better than reserving both up front, because a request that never compensates
//     does not pay for a durability slice it will not use.
// THE FLOOR A SPENT PHASE FALLS BACK TO IS subscriberCompensationFloor, and there is one of it.
//
// A second constant — subscriberPhaseFloor, 250 milliseconds — used to be declared here for the
// same purpose, and the two were applied by two different pieces of arithmetic for the same
// decision. The surviving one is the larger, because it is sized to the work a compensation
// actually has to do: two broker round trips to remove the ACL bindings and the SCRAM
// credential, and up to two local writes to clear the credential record and release the fence.
// A floor that covers one statement but not that sequence is a floor that lets a compensation
// report having tried while leaving a live credential behind.
//
// It is granted ONCE per request rather than once per nested phase, which is what
// subscriberCompensationWindow enforces.

// subscriberDurabilityContext derives the context the durable exposure markers are written on.
//
// It is the LAST slice of the wall clock and consumes it entirely, because there is nothing after
// it. It is reserved rather than left to whatever remains for the reason
// subscriberDurabilityReserve gives: this is the write that turns an invisible exposure into a
// counted one, so starving it would restore the defect the marker exists to remove.
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

// abandonUpdateAfterLostFence records what an update leaves behind when its claim lapses, and
// returns the error the caller should see.
//
// # Why the residue is NOT undone
//
// The prune has already removed bindings the stored authorization still implies, so the broker
// now grants LESS than the registry records. Re-granting the stored boundary would look like the
// tidy compensation, and it is the wrong move: the operation that now holds the claim may
// already have narrowed the row further, and re-granting would widen the broker past what the
// registry records. Every partial failure in this file is designed to fail CLOSED, and a
// compensation that can fail open is worse than the residue it removes.
//
// What the residue needs instead is to be VISIBLE and self-healing, and it is both. The next
// successful authorization change reconciles it — prune and grant are both computed from the
// row rather than from a diff — and this log line names the subscriber and the count so an
// operator does not have to infer it from a 409.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row as this call would have written it.
//   - stored subscriberAuthorization: the authorization the registry still records.
//   - pruned int: how many bindings the abandoned attempt had already removed.
//   - cause error: the lost-fence error.
//
// Returns:
//   - error: cause, unchanged, so the API layer answers the same 409 it would for any conflict.
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

// subscriberAuthorization is the subset of a subscriber that determines its broker-side ACL
// bindings, in a form two snapshots can be compared by.
//
// # What is in it, and what is deliberately not
//
// subscriberDesiredBindings derives every binding from exactly four things: the bound principal,
// the normalized topic list, the consumer-group prefix, and whether the row declares a key scope.
// Those are therefore the whole of this type. Two rows that agree on them imply byte-identical
// ACL requests, so a change that leaves them equal cannot move the boundary and does not need
// the broker.
//
// THE PREFIX'S VALUE IS ABSENT ON PURPOSE, and its PRESENCE is not. Kafka's authorizer has no
// message-key dimension, so swapping one prefix for another moves no binding and including the
// value here would make every prefix edit pay for two administrative round trips that cannot
// change anything. Crossing between "has a prefix" and "has none" is different in kind: it moves
// each authorised topic between Describe-only and Read+Describe, which is a binding difference,
// so the presence is carried and compared.
//
// The topic list is stored normalized and sorted so that a reordering, a duplicate or a
// whitespace difference does not read as a change; those are exactly the differences the ACL
// derivation itself ignores.
type subscriberAuthorization struct {
	principal     string
	consumerGroup string
	topics        []string

	// keyScoped is the PRESENCE of a partition-key prefix, not its value, and it belongs in this
	// snapshot because it decides the SHAPE of the grant: a key-scoped row is provisioned with
	// Describe and no Read on its topics so that the declared key-authorising component is the
	// only path its records can take.
	//
	// The value is deliberately absent. Swapping one prefix for another moves no binding —
	// Kafka's authorizer has no message-key dimension — so recording it here would make every
	// prefix edit pay for a broker reconciliation that changes nothing. Crossing between "has a
	// prefix" and "has none" is the transition that moves bindings, and it is the only one this
	// field can report.
	keyScoped bool
}

// newSubscriberAuthorization snapshots the authorization-bearing fields of a row.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row to snapshot. Nil yields the zero snapshot.
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

// widens reports whether this snapshot implies any broker-side binding the prior one did not.
//
// It is asymmetric on purpose. Removing a topic, or leaving the set alone, cannot give a principal
// access it lacked; ADDING one can, and so can moving the principal or the consumer group, because
// each of those is a new binding on a name that had none.
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
	// untouched. A key-scoped subscriber holds Describe and no Read; clearing its prefix grants
	// Read on every topic it already had, so an unaccounted-for credential would gain record
	// access to all of them. Recording a prefix is the narrowing direction and is not reported
	// here.
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

// refuseWideningUnaccountedAccess blocks a widening whose principal may hold a credential Blnk
// cannot account for.
//
// # What it prevents
//
// An orphaned row names a principal for which a SCRAM credential exists at the broker that Blnk
// could neither record nor revoke, so a means of authenticating as it is outstanding and its
// password is not known here. A cleanup obligation is the same doubt reached from the other side.
// Creating new ACL bindings for such a principal grants that outstanding credential access it did
// not previously have — and unlike a recorded credential, there is no reference to revoke, so the
// grant cannot be walked back by revoking the thing that uses it.
//
// # What it deliberately allows
//
// NARROWING, and re-issuance. Removing bindings reduces the exposure, which is what an operator
// responding to an orphan needs; freezing the row at its widest would make the guard worse than
// the gap. Re-issuance is the documented settlement — Kafka stores one credential per principal,
// so a new issuance replaces the orphan by construction — and it does not go through this path.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row with the requested changes already applied.
//   - prior subscriberAuthorization: the authorization as the registry held it before them.
//
// Returns:
//   - error: ErrConflict when the change widens the boundary of an unaccounted-for principal,
//     otherwise nil.
func refuseWideningUnaccountedAccess(
	subscriber *model.EventSubscriber,
	prior subscriberAuthorization,
) error {
	// A RECORDED credential is not the concern: its reference is the join key that makes a
	// revocation possible, so a widening remains reversible. The unaccounted states are the ones
	// with no reference to revoke.
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
// # Two reasons to reconcile, and why the second one is not redundant
//
// The obvious one is that the authorization MOVED: the snapshots differ, so the desired binding
// set differs and the broker has to be brought to it.
//
// The second is that the caller EXPLICITLY SUPPLIED an authorization field even though the value
// did not change. That is not a no-op instruction, it is a re-apply instruction, and the
// documented recovery path depends on honouring it. When an earlier update persisted the row and
// then failed at the grant step, the stored authorization already equals the desired one while
// the broker is still missing the widening; a comparison alone would call that "unchanged" and
// skip the very grant that completes it, stranding the subscriber permanently one step short.
// Treating an explicit field as a reconcile request is what keeps "re-running the update
// completes it" true.
//
// Everything else — a rename, a legacy webhook URL recorded or cleared, a prefix REPLACED by a
// different prefix — is skipped, because none of them can change a binding.
//
// A key scope APPEARING or DISAPPEARING is not in that list: it moves the grant between
// Describe-only and Read+Describe on every authorised topic, so the snapshot carries its
// presence and this comparison reconciles for it. Only the prefix's value is inert.
//
// Parameters:
//   - stored subscriberAuthorization: the authorization as the registry held it.
//   - updated *model.EventSubscriber: the row as it will be written.
//   - changes SubscriberUpdate: the request, consulted for which fields were explicitly given.
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
// # Why it is best-effort and yet worth attempting
//
// It runs on a path where the caller is already returning an error, so failing here must not
// replace that error — the caller needs to hear about the issuance, not about bookkeeping. But
// the marker is the difference between an exposure a gauge can count and an exposure that exists
// only in a log line, so the attempt is made and its OUTCOME is reported to the caller, which
// phrases its log line differently depending on whether the record survived.
//
// The write takes no claim token by design; see the store seam's documentation for why
// conditioning it on the claim would lose the marker in exactly the case that produces it.
//
// Parameters:
//   - ctx context.Context: bounds the write. Callers pass a compensation context, because the
//     issuance deadline expiring is one of the reasons they are here.
//   - subscriber *model.EventSubscriber: the row whose principal holds the orphaned credential.
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
// # Why this is distinct from the tombstone the caller already wrote
//
// revocation_pending_at is stamped BEFORE the broker is touched, so on its own it cannot say
// whether the broker REFUSED the revocation or the process simply never got that far. The two
// need different work: a refusal means the administrative principal's authorization or the
// broker's reachability has to be fixed before any retry can succeed, while an interrupted
// attempt needs nothing but the retry. This marker separates them, and it is cleared at the
// start of every new attempt so it always describes the latest one.
//
// Best-effort, and its failure is logged rather than returned: the caller is already answering
// with a provisioning failure, and the tombstone — the durable part that makes the work findable
// — is already in place.
//
// Parameters:
//   - ctx context.Context: bounds the write. Callers pass a compensation context, because the
//     broker failure they are reporting is often a timeout that also spent their own deadline.
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

// CompleteLegacyWebhookMigration forgets a subscriber's recorded legacy endpoint AND records
// that it finished migrating, atomically.
//
// # ADMIN-02: one operation because it is one fact
//
// "This subscriber has completed its move to Kafka" is a single change of state, and it was
// expressed as two calls with no transaction spanning them: clear the URL through the registry
// update, then stamp migrated_at. Between them the row described something untrue whichever
// order was used — either an unmigrated subscriber with nothing left to migrate from, or a
// migrated one still advertising a legacy endpoint. The repository now performs both columns in
// one statement, so the intermediate state cannot be observed and a failure is simply a call to
// repeat.
//
// It also removes a second cost. The clear used to route through UpdateSubscriber, which takes
// the provisioning fence and rewrites every mutable column — so a piece of dual-run bookkeeping
// could be refused because a credential issuance happened to hold the claim. Nothing in these
// two columns is an authorization and nothing here reaches Kafka, so neither the fence nor the
// full-row rewrite was ever needed.
//
// THIS IS NOT ClearLegacyWebhookSubscription. That one clears the URL and leaves migrated_at
// ALONE, which is what an operator correcting a mis-recorded endpoint needs — clearing a URL
// must not be able to assert that a migration happened. Use this one only when the migration
// genuinely completed.
//
// Parameters:
//   - ctx context.Context: cancels the write.
//   - subscriberID string: the business key.
//
// Returns:
//   - *model.EventSubscriber: the row as it now stands, carrying the recorded instant so a
//     caller can report it without re-reading.
//   - error: a typed validation error for a blank identifier, ErrSubscriberNotFound, or the
//     repository's error.
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
//
// A partition key on Blnk's topics is a ledger partition key — a "<prefix>_<uuid>" string —
// so 256 characters is far more than any legitimate prefix of one needs while still refusing
// an unbounded value. It matches the bound api/model applies to the same field; the number is
// restated rather than imported because the API package depends on this one and not the other
// way round.
const maxSubscriberKeyScopeLength = 256

// subscriberHashResolutionPageSize is how many registry rows one page of a pseudonym
// resolution reads.
//
// The repository caps a page, so this is chosen at that cap rather than above it: a larger
// value would be silently reduced and the walk would then make more round trips than its own
// arithmetic predicted.
const subscriberHashResolutionPageSize = 100

// subscriberHashResolutionMaxPages bounds a pseudonym resolution.
//
// A resolution walks the registry, so it needs a ceiling or one request can read an
// arbitrarily large table. One hundred pages of one hundred rows is ten thousand subscribers,
// which is far beyond the scale this registry is built for while still being a bounded amount
// of work for a master-key-gated operator query.
//
// Reaching the ceiling is reported as an ERROR rather than as "not found", which is the whole
// point of having a ceiling rather than a silent truncation: a resolution that stopped early
// has not established that the token is unknown, and answering 404 would tell an operator the
// subscriber does not exist when what happened is that nobody looked.
const subscriberHashResolutionMaxPages = 100

// ErrSubscriberHashResolutionExhausted is returned when a pseudonym resolution reached its page
// ceiling without a match.
//
// It is distinct from "not found" deliberately: a resolution that stopped early has not
// established that the token is unknown, and answering not-found would tell an operator the
// subscriber does not exist when what happened is that nobody looked.
var ErrSubscriberHashResolutionExhausted = errors.New(
	"blnk: the subscriber pseudonym could not be resolved within the bounded number of registry " +
		"pages, so whether it names a subscriber is unknown",
)

// ResolveSubscriberByPseudonym finds the subscriber a subscriber_id_hash token names.
//
// # Why this exists, and why paging the registry by hand is not a substitute
//
// Every metric attribute and every log field naming a subscriber carries a PSEUDONYM rather
// than the identifier — see subscriberLagLabel for why. An operator therefore reads a token off
// a lag alert and needs the subscriber. The obvious procedure, documented before this existed,
// was to fetch a page of subscribers and hash each id locally; that answers correctly only
// while the whole registry fits in one page, and fails SILENTLY once it does not — the operator
// hashes a hundred rows, finds no match, and concludes the alert names a subscriber that no
// longer exists.
//
// This walks every page until it matches or the registry is exhausted, and reports a resolution
// that hit its ceiling as an error rather than as an absence.
//
// The comparison is on the token, not on the identifier: the caller holds only the hash, and
// model.HashIdentifier is not invertible. Each candidate is hashed with the SAME function the
// metric label and the log field use, which is what makes the match exact rather than
// approximate.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//   - pseudonym string: the token, as it appears in a log line, a metric label or an alert.
//     Trimmed and compared case-insensitively, because an operator copying a token out of a
//     dashboard should not be defeated by its casing.
//
// Returns:
//   - *model.EventSubscriber: the matching row.
//   - error: a typed validation error for a blank token, ErrSubscriberNotFound when the
//     registry holds no subscriber with that pseudonym, ErrSubscriberHashResolutionExhausted
//     when the ceiling was reached first, or the repository's error.
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

// ListSubscribersAwaitingRevocation returns every subscriber whose broker-side access Blnk
// began taking away and could not finish. It is the read behind
// GET /subscribers?revocation_pending=true.
//
// # It exists because a CRITICAL alert's first step is "find the rows"
//
// SubscriberRevocationOutstanding fires on the AGE of the oldest outstanding revocation and
// carries no subscriber attribute, because a label would export a tenant identifier into every
// notification. The responder therefore has to ask the registry which subscribers are
// affected, and the marker is rare — usually zero rows out of the whole registry — so paging
// the registry by hand and eyeballing a field is exactly the procedure that does not get
// followed at three in the morning. One request answers it.
//
// The walk is bounded like every other, and an INCOMPLETE walk is reported as an error rather
// than as a shorter list. That distinction is the whole value of the endpoint here: a partial
// list of live, unaccounted-for credentials reads as "these are the ones to revoke", and
// acting on it would leave the rest in place while the responder believed the incident closed.
//
// Parameters:
//   - ctx context.Context: cancellation stops the walk between pages and inside a read.
//
// Returns:
//   - []model.EventSubscriber: the affected rows, oldest obligation first, so the responder
//     works the longest exposure first — the same order the alert fires on. Empty and non-nil
//     when the registry holds none, which is the ordinary state.
//   - error: ErrSubscriberHashResolutionExhausted when the walk could not cover the registry,
//     or the repository's error.
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

	// Oldest obligation first. The alert fires on the oldest age, so this order puts the row
	// the alert is actually about at the top rather than leaving the responder to compare
	// timestamps.
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].RevocationPendingAt.Before(*pending[j].RevocationPendingAt)
	})

	return pending, nil
}

// walkSubscriberRegistry pages the whole registry, calling visit for each row.
//
// It is shared by the pseudonym resolver and the revocation scan because both need the same
// three properties, and each is easy to get subtly wrong on its own: a bound on the work, a
// per-page cancellation check, and — the important one — a truthful answer about whether the
// walk actually REACHED THE END. A walk that stopped at its ceiling has established nothing
// about the rows it did not read, and a caller that treats that as an empty result states
// something it does not know: "no such subscriber", or "no credentials to revoke".
//
// Parameters:
//   - ctx context.Context: checked before every page as well as inside each read, because a
//     walk is the one operation here that can outlive its caller's interest in the answer.
//   - store eventSubscriberStore: the registry.
//   - operation string: a fixed literal naming the caller, for the ceiling warning.
//   - visit func(model.EventSubscriber) bool: called per row; returning false stops the walk
//     early, which counts as complete because the answer was found.
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
// broker-side revocation. It is the read behind GET /subscribers?revocation_pending=true, and
// the first step of the SubscriberRevocationOutstanding runbook.
//
// The short-lived service this resolves is closed before returning, so the handler needs no
// lifecycle handling of its own.
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
// administrative client the first time it needs one. PERF-P10.
//
// # The defect it removes
//
// Every Blnk wrapper builds a service per request and closes it afterwards, so before this
// existed each broker-touching request built its OWN administrative client and threw it away:
// a fresh transport, a fresh TCP connection per broker, a fresh SASL/SCRAM handshake — two
// round trips of PBKDF2-derived proof — and a fresh, empty metadata cache, all before the
// first administrative request could be sent. The broker also re-evaluated the connection's
// authorization from cold each time. Credential issuance has a five-second budget and spends
// a measurable fraction of it on setup that a shared client has already paid once.
//
// A resolver rather than a client, and the distinction is what keeps a registry read cheap and
// quiet: the resolver is called only when an operation genuinely reaches the broker, so listing
// subscribers on a deployment whose Kafka credentials are misconfigured neither builds a client
// nor logs a construction failure.
//
// The resolved client is NOT OWNED by this service and is never closed by it — it belongs to
// the process — which is what makes the per-request `defer service.Close()` correct rather than
// destructive. Passing nil clears the resolver and returns the service to building its own.
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

// WithBackgroundScheduler installs the scheduler compensating work is handed to. PERF-P09.
//
// # What runs on it, and why it must not run on the response path
//
// Requirement R-7 gives credential provisioning five seconds. The budget was applied to the
// operation and then exceeded by everything that followed it: a failed provisioning revoked the
// credential it had written on a fresh ten-second budget, cleared the registry's record on
// another five, and released the provisioning fence on another five — all synchronously, all
// after the budget was already spent. A successful issuance still paid the fence release, so the
// documented five-second endpoint answered in ten; a failed one could answer in twenty-five.
//
// None of that work is response-critical. The caller's answer does not depend on it: a failed
// issuance is an error whatever the cleanup achieves, and the fence release only spares the next
// caller from waiting out a lease that expires on its own. So it is scheduled instead, and the
// response returns inside the budget.
//
// # It stays durable
//
// Scheduling is not forgetting. The task still runs, in this process, on its own bounded
// context, and it still logs its outcome exactly as it did when it blocked the response —
// including the ERROR line that names a principal nobody revoked. The process's scheduler tracks
// outstanding tasks so a graceful shutdown waits for them, and the provisioning fence is held
// until the task finishes, so a caller retrying immediately is refused with a conflict rather
// than having its fresh credential revoked by the previous attempt's cleanup.
//
// Nil restores inline execution.
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

// schedule hands a compensating task to the installed scheduler, or runs it inline when there
// is none.
//
// Every caller must be a task whose outcome the RESPONSE DOES NOT DEPEND ON. A task that runs
// inline in a test and in the background in production cannot be one whose result is read.
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

// defersWork reports whether a scheduler is installed, so a caller can ask the broker to defer
// work only when there is somewhere for it to be deferred TO.
//
// Without this, provisioning would be told to hand its compensation back and the service would
// then run it inline anyway — the same two round trips on the response path, in a less obvious
// place, with the result reporting them as pending when they were not.
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

// subscriberFenceRenewalDivisor sets how often a held fence is renewed, as a fraction of the
// lease.
//
// A third, for the same arithmetic the event relay's lease heartbeat uses: two consecutive
// renewals may be missed — one to a slow database round trip, one to scheduling — and the claim
// still has a third of its life left when the third succeeds. Renewing at half the lease
// survives one miss; renewing at the lease itself survives none.
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
	// A fence whose claim was never granted has no heartbeat to stop, and waiting on its nil
	// done channel would block for ever. fenceSubscriber returns exactly that fence alongside
	// its error so callers can defer the release unconditionally.
	if f == nil || f.stop == nil || f.done == nil {
		return
	}

	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
}

// keyScopeDisclosure describes a subscriber's key scope and where it is enforced, so every
// surface that reports the prefix reports the enforcement point with it.
//
// # Why a value rather than two separate reads
//
// The prefix and its enforcement point are only safe TOGETHER. A response, a log line or a
// support answer carrying the prefix alone states a boundary the broker does not keep — the
// precise misreading that led to credential issuance being refused outright, which removed the
// subscriber's access instead of narrowing it. Pairing them in one value means a caller cannot
// obtain the prefix from this package without also obtaining the sentence that qualifies it.
//
// It ACCOMPANIES requireProvisionableKeyScope and requireRecordableKeyScope rather than
// replacing them. Those two refuse the states in which the mismatch would become access —
// minting a credential for a key-scoped row, and recording a prefix on a row that already holds
// one — while this value is what every surface that merely REPORTS a key-scoped row uses: the
// registry reads, which legitimately describe a row whose prefix nothing enforces, and the log
// line that names a refused issuance. The database CHECK constraint that once made the
// combination unrepresentable stays dropped (sql/1781248930.sql), so a row seeded directly can
// still hold it and these reports must still be able to describe it truthfully.
type keyScopeDisclosure struct {
	// Prefix is the recorded partition-key prefix, or "" when none is recorded.
	Prefix string

	// Enforcement is WHICH COMPONENT enforces the scope: the declared key-authorising component
	// (broker_gateway) when a prefix is recorded, none when the topic and group ACLs are the
	// whole boundary. It describes the ROW, so it reports where a prefix WOULD be enforced even
	// on a deployment that has declared nothing — which is exactly the state a refused issuance
	// has to be able to describe.
	Enforcement model.KeyScopeEnforcementStatus
}

// describeSubscriberKeyScope builds the disclosure for a registry row.
//
// A nil row yields the empty, none-enforced disclosure rather than panicking: it is read on
// rows loaded from a repository whose not-found representation is a nil pointer, and inside
// paths that are already reporting a problem.
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

// logKeyScopeDisclosure records the issuance line that accompanies a credential issued to a
// subscriber carrying a key scope.
//
// # Why a line at all, and why it is no longer a warning
//
// This is the moment a principal comes into existence for a row that describes a boundary
// narrower than a topic, so it is the moment an operator wants to be able to find: the line
// names the prefix, the component that enforces it and the topics the grant covers, so it answers
// "what can this principal see, and how?" without a second lookup.
//
// It was a WARNING while the answer was "more than the row suggests". A key-scoped credential
// then carried Read on whole shared topics and the prefix was the consumer's own filter, so every
// issuance genuinely was something an operator needed to know about. That is no longer what
// happens — such a credential is granted Describe and no Read, and issuance only succeeds where a
// key-authorising component is declared to apply the prefix — so the line records a normal,
// fully-enforced issuance and its severity says so. Leaving it at warning would have taught
// operators to ignore the one severity that is supposed to mean something.
//
// # WHAT THIS REPLACED, twice over
//
// Two guards used to REFUSE here. One turned credential issuance into a permanent conflict for
// any subscriber recording a prefix; the other refused to record a prefix on a subscriber that
// already held a credential. Between them the state was unreachable, and a database CHECK
// constraint made it unrepresentable as well. The intent was honesty — a topic-level credential
// really is wider than a key-scoped row appears to describe — but the effect was to withdraw a
// REQUIRED capability for a state the registry is explicitly designed to hold.
//
// What replaced the refusal was DISCLOSURE: issue the whole-topic credential, and state that the
// narrowing is the subscriber's own to apply. That was accurate and it was not a boundary. A
// subscriber that ignored the obligation — or used any other consumer — read every other ledger's
// records on the shared category topic, and the platform could neither prevent nor detect it.
//
// What replaced disclosure is a NARROWER GRANT plus an enforcing delivery path, which is what
// this line now describes. The prefix is sanitized before it is logged: it arrives from a
// caller-supplied field, so an unsanitized value could inject a newline into a line-oriented
// aggregator or run to any length.
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

// logKeyScopeRecordedOnProvisionedSubscriber HAS BEEN REMOVED, and its content was inherited by
// warnOnKeyScopeRecordedAfterIssuance rather than lost.
//
// Both were written for one transition — a partition-key prefix recorded onto a row that already
// holds a live credential — and both DISCLOSED it, on the reasoning that such a prefix bound
// nobody but a cooperating consumer. That reasoning no longer holds: the update NARROWS the live
// grant, pruning record-level Read from every authorised topic, and the declared key-authorising
// component becomes the only path that subscriber's records can take.
//
// So the transition is still reachable and still worth a line; what changed is the line's content,
// not its existence. It reports that Read was WITHDRAWN from a principal whose credential is
// already in a subscriber's hands, together with the prefix, the component that enforces it, the
// size of the grant, and the instant that credential was issued — the last being what separates "I
// provisioned this a moment ago" from "a principal minted an hour ago has just lost the access its
// response promised". Two functions saying almost the same thing from two call sites is how one of
// them came to be emitted and the other not, so there is one.
