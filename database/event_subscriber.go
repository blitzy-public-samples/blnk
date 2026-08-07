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

// event_subscriber.go is the repository implementation of the eventSubscriber
// contract declared in repository.go: persistence for blnk.event_subscribers, the
// registry of Kafka subscribers and the access boundary provisioned for each one.
//
// # The subscriber concept is new, so the folder's conventions carry the weight
//
// Nothing in this codebase resembled a subscriber before this change — there was
// no subscriber table, model, repository or endpoint to extend. There is therefore
// no prior art to be consistent with INSIDE this domain, which is exactly why
// consistency with the conventions of THIS FOLDER matters more than usual here:
// value receivers on Datasource, apierror-wrapped failures, lib/pq array handling
// and OpenTelemetry spans on the transaction.database tracer are what make this
// file recognisable as Blnk code rather than as an import from somewhere else.
//
// # NO PLAINTEXT SASL SECRET IS ACCEPTED, RETURNED, LOGGED OR PERSISTED HERE
//
// This is the defining constraint on this file, and it is structural rather than
// aspirational. Read the signatures below: not one of them takes a password, and
// not one of them returns a value anything can be authenticated with. There is no
// getter, no reveal path, no decrypt path — and deliberately no verify method
// either, because a verify method is the first step toward a reveal method.
//
// The SASL secret is generated during provisioning by the service layer, returned
// to the API caller EXACTLY ONCE, and persisted nowhere. What this file writes is
// a NON-REVERSIBLE credential_reference that the caller has ALREADY derived, plus
// the instant of issuance. A lost secret is therefore replaced by a fresh issuance
// rather than recovered. This mirrors api_key.go, whose key column holds a bcrypt
// hash and whose raw key is never stored anywhere.
//
// golang.org/x/crypto/bcrypt is intentionally NOT imported. Hashing and derivation
// belong to the caller that owns issuance; importing bcrypt here would signal that
// this file handles secrets, which must not be true. If a future change appears to
// need a secret column, the design has been misread — see requirement R-7.
//
// One non-obvious hazard is worth naming, because it is the likeliest way a secret
// could ever reach a log from this package: apierror.NewAPIError LOGS its details
// argument (logrus.WithField("details", details)). Every call below therefore
// passes only the driver error or nil as details, never a credential value and
// never a caller-supplied string that could hold one. The same rule governs span
// attributes: the subscriber id, the Kafka principal and the consumer group are
// recorded because they are identifiers rather than secrets; the credential
// reference is never recorded, not even truncated.
//
// # The credential columns have exactly one writer
//
// CreateEventSubscriber and UpdateEventSubscriber do not touch
// credential_reference or credential_issued_at at all. RecordSubscriberCredential
// is the only method that writes them. That single-writer rule is what keeps the
// audit story legible: an issuance record cannot be forged or blanked out by an
// ordinary registry edit, and there is one place to look when asking when a
// credential was minted.
//
// # webhook_url and migrated_at are dual-run-only
//
// Both columns exist solely for the 30-day window in which Kafka publishing and
// legacy HTTP webhook delivery run side by side. They are here because the
// requirement to migrate existing subscribers off the webhook subscription REST
// API meets a repository in which no such API exists: the whole subscription
// surface today is one global WebhookConfig{Url, Headers} value. Recording a legacy
// URL per subscriber is what gives an existing subscriber somewhere to be migrated
// FROM, and what makes the post-sunset 410 Gone behaviour observable at all. Their
// use ENDS AT SUNSET, when they and the index over migrated_at are dropped.
//
// # The 5-second provisioning budget shapes every query below
//
// POST /subscribers/{id}/kafka-credentials must complete within 5 seconds, and the
// Kafka admin round trips consume most of that. Every method here is therefore a
// SINGLE index-served statement: no N+1 reads, no transaction where an UPDATE
// suffices, and no cache. d.Cache is deliberately untouched — a stale subscriber
// ACL set is a security problem, not a performance win.
package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// eventSubscriberColumns is the column list every read of blnk.event_subscribers
// projects, in the exact order scanEventSubscriber consumes it. It is declared
// once so the single-row and listing paths cannot drift apart in either
// projection or scan order — a drift that would not fail to compile and would
// instead surface as silently transposed fields.
const eventSubscriberColumns = `id, subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics, ` +
	`partition_key_prefix, credential_reference, credential_issued_at, webhook_url, migrated_at, ` +
	`revocation_pending_at, created_at, updated_at`

// Page-size bounds for the registry listing. The default keeps an unqualified
// request cheap; the maximum stops a caller turning a management endpoint into a
// full table scan. Both match the bounds the dead-letter listing uses, so the two
// management surfaces page alike.
const (
	defaultSubscriberPageSize = 50
	maxSubscriberPageSize     = 500
)

// subscriberNotFoundMessage is the single not-found message this file reports, so
// every path that can fail to locate a subscriber says the same thing.
const subscriberNotFoundMessage = "Subscriber not found"

// The two unique indexes a write can collide with. They are named here rather
// than inlined so the conflict classifier reads as the schema does, and so a
// rename in the migration has exactly one place to be reflected.
const (
	subscriberIDUniqueIndex     = "event_subscribers_subscriber_id_uidx"
	kafkaPrincipalUniqueIndex   = "event_subscribers_kafka_principal_uidx"
	uniqueViolationPostgresCode = "unique_violation"
)

// eventSubscriberScanner is the minimum surface scanEventSubscriber needs,
// satisfied by both *sql.Row and *sql.Rows. It lets the single-row read and the
// listing share one decoder instead of maintaining two scan orders.
type eventSubscriberScanner interface {
	Scan(dest ...interface{}) error
}

// scanEventSubscriber decodes one blnk.event_subscribers row into a
// model.EventSubscriber.
//
// authorized_topics is a TEXT[] and is read through a pq.StringArray local that is
// then converted with []string(...), exactly as api_key.go reads scopes — the only
// other array column in this schema. A NOT NULL '{}' column decodes to an empty
// slice rather than to a one-element slice holding "", which is the classic lib/pq
// array trap; the nil normalisation below closes the remaining case so callers
// never have to distinguish nil from empty.
//
// The six nullable columns are preserved AS pointers rather than flattened to
// zero values, because for each of them NULL carries meaning a zero value would
// destroy: a nil PartitionKeyPrefix means no key constraint is recorded and NOT
// "constrained to the empty prefix", which would invert the intent and refuse a
// subscriber that asked for nothing; a nil CredentialReference is the reliable test
// for "registered, not yet provisioned"; a nil MigratedAt means not yet migrated,
// which is precisely what migration-progress reporting counts; and a nil
// RevocationPendingAt means the subscriber is not in the middle of being
// deregistered. Each value is copied into its own local before its address is
// taken, so no field aliases a scan destination.
//
// The two FENCE columns — provisioning_token and provisioning_until — are
// deliberately NOT projected. They are the claim an in-flight issuance holds, they
// live for seconds, and every read of them is a conditional UPDATE inside this file.
// model.EventSubscriber is serialised into API responses, so carrying a transient
// internal claim on it would publish operational state that no consumer can act on
// and that is stale by the time it is read.
func scanEventSubscriber(s eventSubscriberScanner) (model.EventSubscriber, error) {
	var sub model.EventSubscriber
	var authorizedTopics pq.StringArray
	var partitionKeyPrefix, credentialReference, webhookURL sql.NullString
	var credentialIssuedAt, migratedAt, revocationPendingAt sql.NullTime

	if err := s.Scan(
		&sub.ID,
		&sub.SubscriberID,
		&sub.Name,
		&sub.KafkaPrincipal,
		&sub.ConsumerGroupID,
		&authorizedTopics,
		&partitionKeyPrefix,
		&credentialReference,
		&credentialIssuedAt,
		&webhookURL,
		&migratedAt,
		&revocationPendingAt,
		&sub.CreatedAt,
		&sub.UpdatedAt,
	); err != nil {
		return model.EventSubscriber{}, err
	}

	sub.AuthorizedTopics = []string(authorizedTopics)
	if sub.AuthorizedTopics == nil {
		// Normalising nil to empty keeps HasTopicAccess and every JSON response
		// consistent: a subscriber authorised for nothing serialises as [] rather
		// than null, so an API consumer never has to treat the two differently.
		sub.AuthorizedTopics = []string{}
	}

	if partitionKeyPrefix.Valid {
		value := partitionKeyPrefix.String
		sub.PartitionKeyPrefix = &value
	}
	if credentialReference.Valid {
		value := credentialReference.String
		sub.CredentialReference = &value
	}
	if webhookURL.Valid {
		value := webhookURL.String
		sub.WebhookURL = &value
	}
	if credentialIssuedAt.Valid {
		issuedAt := credentialIssuedAt.Time
		sub.CredentialIssuedAt = &issuedAt
	}
	if migratedAt.Valid {
		migrated := migratedAt.Time
		sub.MigratedAt = &migrated
	}
	if revocationPendingAt.Valid {
		pending := revocationPendingAt.Time
		sub.RevocationPendingAt = &pending
	}

	return sub, nil
}

// normalizeAuthorizedTopics prepares the topic grant for writing.
//
// The column is NOT NULL DEFAULT '{}' precisely so that an unprovisioned
// subscriber is authorised for NOTHING instead of for NULL, which is a security
// property rather than a convenience: NULL would make "no grant recorded"
// indistinguishable from "grant not yet known" and force every authorisation
// check to guess which was meant. A nil slice from the caller is therefore written
// as the empty array, so the registry FAILS CLOSED.
//
// pq.StringArray is used rather than a hand-built '{a,b}' literal so element
// quoting and escaping are the driver's problem, which is what keeps a topic name
// containing a comma or a brace from corrupting the value.
func normalizeAuthorizedTopics(topics []string) pq.StringArray {
	if topics == nil {
		return pq.StringArray{}
	}
	return pq.StringArray(topics)
}

// requireSubscriberID rejects a blank business key before any query is issued.
//
// Failing in memory rather than at the database matters twice over: it keeps the
// 5-second provisioning budget for a request that cannot succeed anyway, and it
// answers with "required" instead of the misleading "Subscriber not found" that an
// empty WHERE match would otherwise produce. Whitespace is trimmed for the test
// only — the caller's value is stored verbatim, because silently rewriting an
// identifier would be a worse surprise than rejecting it.
func requireSubscriberID(subscriberID string) error {
	if strings.TrimSpace(subscriberID) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber ID is required", nil)
	}
	return nil
}

// requireSubscriberFields validates the four NOT NULL business columns AND canonicalizes the
// three that make up the subscriber's Kafka identity.
//
// NOT NULL alone does not stop an empty string, and each of these being blank is a real hazard
// rather than a cosmetic one. A blank subscriber_id yields a row no endpoint can address. A
// blank name defeats the reason the registry exists — the schema documents name as NOT NULL
// because an unnamed principal cannot be triaged. A blank kafka_principal is the worst of the
// four: it would occupy the unique principal index while naming a principal that authenticates
// as nothing, and because the index is unique it would also block the next subscriber that
// really did have no principal set. A blank consumer_group_id would be returned verbatim to a
// subscriber that then could not join a group.
//
// # SEC-04: canonical BEFORE persistence, not at provisioning
//
// Canonicalization used to happen only where the credential was provisioned, and the row was
// stored as given. So the rows "alice" and " alice " both satisfied the unique index — two
// distinct registry subscribers — while provisioning trimmed both to the SAME Kafka principal.
// The two subscribers then shared one credential and one ACL set, and reissuing for either
// silently invalidated the other's consumer. Nothing in the registry could show that, because
// as far as the registry was concerned they were different subscribers.
//
// The identifier is canonicalized here, at the write, so the DATABASE can only ever hold one
// spelling of it — which is what makes the unique index mean what it says. The migration adds
// matching CHECK constraints, so the guarantee survives a write that bypasses this function.
//
// # The principal and the group are DERIVED, and a mismatch is refused
//
// Both are the subscriber's access boundary rather than labels: the SCRAM credential is minted
// for the principal and every ACL binding names it and the group namespace. Accepting either
// from a caller is accepting a caller's choice of boundary, so both are derived from the
// canonical identifier and a supplied value that differs is REFUSED rather than corrected — a
// caller that sent a different boundary asked for something it may not have, and silently
// substituting the right answer would hide that. The refusal names the derived value, so the
// caller learns what to send.
//
// The subscriber struct is MUTATED to its canonical form on success, so the row written and the
// row a caller subsequently reads are the same, and a caller that trimmed nothing still stores
// something canonical.
func requireSubscriberFields(subscriber *model.EventSubscriber) error {
	if subscriber == nil {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber is required", nil)
	}
	if err := requireSubscriberID(subscriber.SubscriberID); err != nil {
		return err
	}
	if strings.TrimSpace(subscriber.Name) == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Subscriber name is required", nil)
	}

	canonicalID, err := model.CanonicalizeSubscriberIdentifier(subscriber.SubscriberID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a Kafka identity", err)
	}

	principal, err := model.CanonicalKafkaPrincipal(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a Kafka principal", err)
	}

	group, err := model.CanonicalConsumerGroupID(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a consumer group", err)
	}

	namespace, err := model.CanonicalConsumerGroupNamespace(canonicalID)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"Subscriber ID cannot be used to derive a consumer group namespace", err)
	}

	// ABSENT means DERIVE; PRESENT means it must match exactly.
	//
	// The two cases are different requests and deserve different answers. An empty principal
	// is a caller that has not expressed an opinion, and there is exactly one correct value to
	// fill in — filling it is safe by construction, because what gets stored is the canonical
	// derivation itself. Refusing instead would force every caller to compute the value only
	// to have it compared against the same computation, which adds a step that can be got
	// wrong without adding a check that can catch anything.
	//
	// A NON-EMPTY principal that differs is refused, and that is the SEC-04 guarantee: a
	// supplied principal is not a request for a name, it is a request for a BOUNDARY, because
	// the principal is what every ACL binding is granted to.
	if subscriber.KafkaPrincipal == "" {
		subscriber.KafkaPrincipal = principal
	}

	if subscriber.KafkaPrincipal != principal {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The Kafka principal must be the one derived from the subscriber ID",
			fmt.Errorf("subscriber %q may only hold principal %q, not %q",
				canonicalID, principal, subscriber.KafkaPrincipal))
	}

	// An absent group is likewise derived, to the default leaf. Deriving the DEFAULT rather
	// than the namespace itself is deliberate: the bare namespace is not a usable group, and a
	// prefixed ACL over it would grant the namespace instead of a group inside it.
	if subscriber.ConsumerGroupID == "" {
		subscriber.ConsumerGroupID = group
	}

	// The recorded group may be any leaf inside the subscriber's own namespace — that is what
	// the prefixed ACL grant is for — but never a value outside it, which a prefixed grant
	// would turn into a reach into another subscriber's groups.
	if !model.IsInSubscriberGroupNamespace(subscriber.ConsumerGroupID, namespace) {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The consumer group must lie inside the subscriber's own namespace",
			fmt.Errorf("subscriber %q may use any group under %q (for example %q), not %q",
				canonicalID, namespace, group, subscriber.ConsumerGroupID))
	}

	if err := requireGrantableTopics(subscriber.AuthorizedTopics); err != nil {
		return err
	}

	if err := requireSafeWebhookURL(subscriber.WebhookURL); err != nil {
		return err
	}

	subscriber.SubscriberID = canonicalID
	subscriber.Name = strings.TrimSpace(subscriber.Name)

	return nil
}

// grantableTopicPrefixes is the set of topic names a subscriber may be authorised for, as
// bare category topics under the configured prefix.
//
// It is derived from the model's own category vocabulary rather than listed here, so a new
// category is covered without an edit and an INTERNAL category is excluded automatically.
// The repository cannot call into the root package — the root imports database — so the
// prefix is read from configuration the same way the outbox's topic validation reads it.
func grantableTopicPrefixes() map[string]struct{} {
	grantable := make(map[string]struct{})
	for _, topic := range model.SubscriberGrantableTopics(expectedEventTopicPrefix()) {
		grantable[topic] = struct{}{}
	}

	return grantable
}

// requireGrantableTopics refuses an authorised-topic list containing anything a subscriber may
// not be granted.
//
// # SEC-03 at the persistence boundary
//
// The list is what the ACL bindings are built from, so whatever is stored here is what the
// credential can read. Three classes must be impossible and the check is here as well as in
// the provisioning path because the row outlives any single request: a topic accepted now is
// granted at the next issuance, whichever code path performs it.
//
//   - "*" and other WILDCARDS, because Kafka treats the resource name "*" as matching any
//     resource, so one such entry turns a per-topic grant into a cluster-wide one.
//   - FOREIGN topics, because a grant over a topic Blnk does not own is a grant into somebody
//     else's data on a broker Blnk may share.
//   - DEAD-LETTER and INTERNAL topics, because the DLTs carry failed events with Blnk's own
//     failure metadata and the system category carries Blnk's internal
//     diagnostics and uncatalogued payloads. Neither has a subscriber audience.
//
// An EMPTY list is accepted: a subscriber authorised for nothing is the fail-closed default of
// a fresh registration, and refusing it would make registration and authorisation one
// inseparable step.
//
// Parameters:
//   - topics []string: the authorised topics as supplied.
//
// Returns:
//   - error: a typed invalid-input error naming the first offending topic, or nil.
func requireGrantableTopics(topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	grantable := grantableTopicPrefixes()

	allowed := make([]string, 0, len(grantable))
	for topic := range grantable {
		allowed = append(allowed, topic)
	}
	sort.Strings(allowed)

	for _, topic := range topics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}

		if _, ok := grantable[trimmed]; !ok {
			return apierror.NewAPIError(apierror.ErrInvalidInput,
				"Authorized topics must be Blnk-owned subscriber-facing category topics",
				fmt.Errorf("topic %q is not grantable; the grantable topics are %s",
					trimmed, strings.Join(allowed, ", ")))
		}
	}

	return nil
}

// requireSafeWebhookURL applies the destination policy to the legacy dual-run webhook URL.
//
// # SSRF-01: the column has no processed sink TODAY, which is exactly when to constrain it
//
// The URL is recorded so a subscriber already receiving HTTP pushes has somewhere to be
// migrated FROM. Nothing sends to it yet — but a stored URL is a future sink, and the moment
// any code does send to it, whatever is in this column becomes a request Blnk makes from
// inside its own network. Constraining it now costs nothing; constraining it after a sender
// exists means auditing every row already written.
//
// Two rules, and each closes a distinct route:
//
//   - HTTPS ONLY. A cleartext push carries ledger and identity data — names, email addresses,
//     phone numbers, addresses, dates of birth — over a network Blnk does not control, and it
//     is trivially redirectable. "http" is refused rather than upgraded, because upgrading a
//     URL an operator supplied would send data somewhere they did not name.
//   - NO INTERNAL DESTINATION. Loopback, link-local (including the 169.254.169.254 cloud
//     metadata address), private ranges and unqualified hostnames are refused. Those are the
//     targets a server-side request forgery aims at: the metadata endpoint hands out cloud
//     credentials, and a private address reaches services that trust the network rather than
//     the caller.
//
// A hostname that RESOLVES to an internal address cannot be caught here — that is a DNS
// rebinding problem and it belongs to the sender, at connect time, not to a validator running
// hours earlier. This function refuses what is visibly internal; docs/kafka-operations.md
// records the rest as an obligation on whoever wires delivery.
//
// Parameters:
//   - webhookURL *string: the URL as supplied. Nil and empty are accepted — most subscribers
//     never had a webhook.
//
// Returns:
//   - error: a typed invalid-input error naming the rule broken, or nil.
func requireSafeWebhookURL(webhookURL *string) error {
	if webhookURL == nil || strings.TrimSpace(*webhookURL) == "" {
		return nil
	}

	raw := *webhookURL

	// SURROUNDING WHITESPACE IS REFUSED, not trimmed, and this rule belongs here rather than
	// only in the request DTO.
	//
	// The write paths store this column VERBATIM, so trimming for validation and then storing
	// the original meant a value could pass a check the stored bytes did not satisfy: " https://
	// hooks.example.com/blnk " was validated as the trimmed URL and persisted with the spaces,
	// where it is a different URL to every reader and to whatever eventually sends to it. The
	// API DTO already refuses it, so trimming here also made a service, CLI or migration caller
	// subject to a laxer policy than an HTTP caller — one column, two rules.
	//
	// An all-whitespace value is NOT refused: the guard above treats it as "clear the record",
	// which is the same three-way nil/empty/value mapping the service layer applies, and the
	// nullable column exists to keep those distinguishable.
	if raw != strings.TrimSpace(raw) {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The webhook URL must not have surrounding whitespace",
			errors.New(
				"a URL differing from another only by whitespace is a copy-paste artefact, and this "+
					"column is stored verbatim, so trimming it would persist a destination the caller "+
					"did not supply",
			))
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The webhook URL is not a valid URL", err)
	}

	if parsed.Scheme != "https" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The webhook URL must use https",
			fmt.Errorf("scheme %q is not permitted; ledger and identity payloads must not be pushed in cleartext",
				parsed.Scheme))
	}

	host := parsed.Hostname()
	if host == "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The webhook URL must name a host", nil)
	}

	if reason := internalDestinationReason(host); reason != "" {
		return apierror.NewAPIError(apierror.ErrInvalidInput,
			"The webhook URL must not address an internal destination",
			fmt.Errorf("host %q is refused: %s", host, reason))
	}

	return nil
}

// internalDestinationReason reports why a host is an internal destination, or "" when it is
// not visibly internal.
//
// Literal addresses are classified with net.IP so every form of them is covered — IPv4, IPv6,
// and IPv4-mapped IPv6, which is the spelling a denylist of strings always misses. A name
// without a dot is refused because it can only resolve through a search domain or a hosts
// entry, both of which are inside the deployment.
//
// Parameters:
//   - host string: the hostname or literal address from the URL.
//
// Returns:
//   - string: a short reason, or "" when the host is acceptable.
func internalDestinationReason(host string) string {
	if address := net.ParseIP(host); address != nil {
		switch {
		case address.IsLoopback():
			return "it is a loopback address, which would make Blnk call itself"
		case address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast():
			return "it is a link-local address, the range the cloud metadata service lives on"
		case address.IsPrivate():
			return "it is a private address, which reaches services that trust the network rather than the caller"
		case address.IsUnspecified():
			return "it is the unspecified address"
		case address.IsInterfaceLocalMulticast(), address.IsMulticast():
			return "it is a multicast address"
		default:
			return ""
		}
	}

	lowered := strings.ToLower(host)
	switch {
	case lowered == "localhost", strings.HasSuffix(lowered, ".localhost"):
		return "it resolves to loopback"
	case strings.HasSuffix(lowered, ".local"), strings.HasSuffix(lowered, ".internal"):
		return "it is an internal-only name"
	case !strings.Contains(lowered, "."):
		return "it is unqualified, so it can only resolve inside this deployment"
	default:
		return ""
	}
}

// classifySubscriberWriteError turns a driver error from an insert or update into
// the typed error the API layer should answer with.
//
// A unique violation is a CALLER mistake and must surface as a conflict — 409 —
// rather than as an internal error, and never as a raw driver error or a panic.
// The two unique indexes are told apart through pqErr.Constraint so the message
// names what actually collided: "this ID" and "this Kafka principal" are different
// operator problems, and a single generic message would send whoever reads it to
// check the wrong column first. An unrecognised constraint still resolves to a
// conflict, because the violation is real even when the index name is not one of
// the two this file knows about.
//
// errors.As is used rather than a bare type assertion so the classification also
// holds if the driver error arrives wrapped, which a bare assertion would silently
// misfile as an internal server error.
func classifySubscriberWriteError(err error, internalMessage string) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code.Name() == uniqueViolationPostgresCode {
		switch pqErr.Constraint {
		case subscriberIDUniqueIndex:
			return loggedDatabaseError(apierror.ErrConflict, "A subscriber with this ID already exists", "classify_subscriber_write_error", err)
		case kafkaPrincipalUniqueIndex:
			return loggedDatabaseError(apierror.ErrConflict, "Another subscriber already uses this Kafka principal", "classify_subscriber_write_error", err)
		default:
			return loggedDatabaseError(apierror.ErrConflict, "Subscriber already exists", "classify_subscriber_write_error", err)
		}
	}
	return loggedDatabaseError(apierror.ErrInternalServer, internalMessage, "classify_subscriber_write_error", err)
}

// assertSubscriberRowAffected turns a zero-row write into a typed not-found error.
//
// PostgreSQL reports a WHERE clause that matched nothing as a SUCCESSFUL statement
// affecting no rows. Without this check every write against a non-existent
// subscriber would return nil and the caller would believe it had succeeded — and
// for RecordSubscriberCredential that would mean reporting a provisioned
// credential for a subscriber that does not exist, which is the one outcome the
// endpoint must never produce. A missing row has to be LOUD.
//
// A RowsAffected failure is reported as an internal error rather than swallowed,
// following DeleteLineageMapping: if the count cannot be read, whether the row
// existed is genuinely unknown, and claiming success is the one answer that is
// certainly wrong.
//
// The subscriber-specific not-found code is used in preference to the generic one.
// Both resolve to HTTP 404 through apierror.StatusForCode, so the response status
// is identical either way, but the typed code lets the subscriber API propagate
// what it received instead of re-deriving a domain meaning from a generic error.
func assertSubscriberRowAffected(result sql.Result, internalMessage string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return loggedDatabaseError(apierror.ErrInternalServer, internalMessage, "assert_subscriber_row_affected", err)
	}
	if affected == 0 {
		return apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
	}
	return nil
}

// CreateEventSubscriber registers a subscriber and returns the stored row.
//
// The row is read back through RETURNING rather than reconstructed in Go, so the
// caller receives the database's own surrogate key and bookkeeping timestamps
// instead of a struct that merely resembles what was stored.
//
// THE CREDENTIAL AND MIGRATION COLUMNS ARE NOT WRITTEN HERE, and any values the
// supplied struct carries in CredentialReference, CredentialIssuedAt or MigratedAt
// are ignored rather than persisted. A newly registered subscriber is by definition
// "registered, not yet provisioned" and "not yet migrated", and those columns have
// exactly one writer each — RecordSubscriberCredential and MarkSubscriberMigrated.
// The returned row shows the true stored state, so a caller that passed such a
// value can see it was not kept.
//
// webhook_url IS accepted at creation, because recording an existing subscriber's
// legacy URL is the whole point of the dual-run columns: it is what gives a
// subscriber already receiving HTTP pushes somewhere to be migrated FROM.
func (d Datasource) CreateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "CreateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriber.SubscriberID))

	// Stamped in Go and bound as parameters, as the sibling repositories do, so the
	// values written are the ones the caller can see in the returned row.
	now := time.Now()

	row := d.Conn.QueryRowContext(ctx, `
		INSERT INTO blnk.event_subscribers
			(subscriber_id, name, kafka_principal, consumer_group_id, authorized_topics,
			 partition_key_prefix, webhook_url, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING `+eventSubscriberColumns,
		subscriber.SubscriberID,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		normalizeAuthorizedTopics(subscriber.AuthorizedTopics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		now,
		now,
	)

	stored, err := scanEventSubscriber(row)
	if err != nil {
		span.RecordError(err)
		return nil, classifySubscriberWriteError(err, "Failed to create event subscriber")
	}

	span.AddEvent("Event subscriber created", trace.WithAttributes(
		attribute.String("subscriber.id", stored.SubscriberID),
		attribute.String("subscriber.principal", stored.KafkaPrincipal),
		attribute.String("subscriber.consumer_group", stored.ConsumerGroupID),
		attribute.Int("subscriber.authorized_topic_count", len(stored.AuthorizedTopics)),
	))
	return &stored, nil
}

// GetEventSubscriberByID retrieves a subscriber by its business subscriber_id,
// resolving through the unique index the credential endpoint's 5-second budget
// depends on.
//
// A missing row is reported as a typed not-found error and NEVER as a bare
// sql.ErrNoRows, so the API layer maps it to 404 without inspecting driver
// sentinels. Returning (nil, nil) instead — as the lineage mapping reader does —
// would be wrong here: the subscriber API has to tell "no such subscriber" apart
// from "found", and a nil-nil result forces every caller to rediscover that
// distinction for itself.
func (d Datasource) GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "GetEventSubscriberByID")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)

	subscriber, err := scanEventSubscriber(row)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, loggedDatabaseError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, "get_event_subscriber_by_id", err)
		}
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to retrieve event subscriber", "get_event_subscriber_by_id", err)
	}

	span.AddEvent("Event subscriber retrieved", trace.WithAttributes(
		attribute.String("subscriber.id", subscriber.SubscriberID),
		attribute.String("subscriber.principal", subscriber.KafkaPrincipal),
		attribute.Int("subscriber.authorized_topic_count", len(subscriber.AuthorizedTopics)),
	))
	return &subscriber, nil
}

// ListEventSubscribers pages the registry NEWEST FIRST, ordered by created_at
// descending and tie-broken by the surrogate key.
//
// The tie-break is what makes the order deterministic rather than merely sorted:
// created_at is stamped in Go and two subscribers registered in the same
// microsecond would otherwise page in arbitrary relative order, which can both
// repeat and skip a row across pages. Ordering by the monotonic id as the second
// key removes that.
//
// The limit is normalised and bounded, and a negative offset is clamped, so a
// malformed page request degrades to a cheap query instead of a full table scan.
func (d Datasource) ListEventSubscribers(ctx context.Context, limit, offset int) ([]model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ListEventSubscribers")
	defer span.End()

	if limit <= 0 {
		limit = defaultSubscriberPageSize
	}
	if limit > maxSubscriberPageSize {
		limit = maxSubscriberPageSize
	}
	if offset < 0 {
		offset = 0
	}
	span.SetAttributes(
		attribute.Int("subscriber.page_limit", limit),
		attribute.Int("subscriber.page_offset", offset),
	)

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT `+eventSubscriberColumns+`
		FROM blnk.event_subscribers
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to list event subscribers", "list_event_subscribers", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Logged rather than returned. The rows have already been read by the
			// time this runs, so surfacing a close failure would discard a correct
			// result the caller needs in favour of a condition it cannot act on.
			logrus.WithError(closeErr).Error("failed to close event subscriber rows")
		}
	}()

	// Non-nil so an empty page serialises as [] rather than null, and pre-sized to
	// the bounded page so a full page does not repeatedly regrow the slice.
	subscribers := make([]model.EventSubscriber, 0, limit)
	for rows.Next() {
		subscriber, scanErr := scanEventSubscriber(rows)
		if scanErr != nil {
			span.RecordError(scanErr)
			return nil, loggedDatabaseError(apierror.ErrInternalServer, "Failed to scan event subscriber", "list_event_subscribers", scanErr)
		}
		subscribers = append(subscribers, subscriber)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, loggedDatabaseError(apierror.ErrInternalServer, "Error iterating over event subscribers", "list_event_subscribers", err)
	}

	span.AddEvent("Event subscribers listed", trace.WithAttributes(
		attribute.Int("subscriber.count", len(subscribers)),
	))
	return subscribers, nil
}

// UpdateEventSubscriber updates a subscriber's access model and its legacy webhook
// URL.
//
// Only the mutable columns are written. subscriber_id locates the row rather than
// being changed — it is the business key an issued credential and a live consumer
// are both addressed by, so reassigning it here would silently orphan them.
//
// THE CREDENTIAL COLUMNS ARE DELIBERATELY NOT TOUCHED. They belong exclusively to
// RecordSubscriberCredential, which means an ordinary registry edit can neither
// blank out nor forge an issuance record, and there is exactly one place to look
// when auditing when a credential was minted.
//
// Changing kafka_principal or authorized_topics here changes only the RECORD of the
// access boundary. The broker is a separate system of record and is not reconciled
// by this call: the service layer must re-provision the ACLs, or the registry and
// the broker will disagree about what the subscriber may read.
func (d Datasource) UpdateEventSubscriber(ctx context.Context, subscriber *model.EventSubscriber) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "UpdateEventSubscriber")
	defer span.End()

	if err := requireSubscriberFields(subscriber); err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriber.SubscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET name = $1,
			kafka_principal = $2,
			consumer_group_id = $3,
			authorized_topics = $4,
			partition_key_prefix = $5,
			webhook_url = $6,
			updated_at = $7
		WHERE subscriber_id = $8
	`,
		subscriber.Name,
		subscriber.KafkaPrincipal,
		subscriber.ConsumerGroupID,
		normalizeAuthorizedTopics(subscriber.AuthorizedTopics),
		subscriber.PartitionKeyPrefix,
		subscriber.WebhookURL,
		time.Now(),
		subscriber.SubscriberID,
	)
	if err != nil {
		span.RecordError(err)
		// Reachable through the principal index: moving a principal onto one another
		// subscriber already holds is a conflict, not a server fault.
		return classifySubscriberWriteError(err, "Failed to update event subscriber")
	}

	if err := assertSubscriberRowAffected(result, "Failed to update event subscriber"); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Event subscriber updated", trace.WithAttributes(
		attribute.String("subscriber.id", subscriber.SubscriberID),
		attribute.String("subscriber.principal", subscriber.KafkaPrincipal),
		attribute.String("subscriber.consumer_group", subscriber.ConsumerGroupID),
		attribute.Int("subscriber.authorized_topic_count", len(subscriber.AuthorizedTopics)),
	))
	return nil
}

// DeleteEventSubscriber removes a subscriber from the registry.
//
// This is the REGISTRY HALF ONLY. Deleting the row does not revoke the
// subscriber's broker-side SCRAM credential or its ACL bindings, because the broker
// is a separate system of record: a caller that deletes here without deprovisioning
// there leaves a principal that can still authenticate and still read. The
// deletion is reported as not-found when no row matched, so a caller cannot mistake
// "already gone" for "just removed" and skip the broker-side work.
//
// PREFER TakeEventSubscriber for any deletion that has to be followed by broker-side
// revocation. It performs the same removal but returns the row it deleted, which carries the
// principal and the authorised topics that revocation needs — information this function
// destroys without reporting. Use this one only when the broker side is already deprovisioned,
// or when there is demonstrably no broker-side state to revoke.
func (d Datasource) DeleteEventSubscriber(ctx context.Context, subscriberID string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "DeleteEventSubscriber")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID)
	if err != nil {
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to delete event subscriber", "delete_event_subscriber", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to delete event subscriber"); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Event subscriber deleted", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))
	return nil
}

// RecordSubscriberCredential persists the outcome of a credential issuance: a
// non-reversible reference to the credential and the instant it was issued, which
// together are the complete stored record of an issuance.
//
// # credentialReference is ALREADY non-reversible when it arrives
//
// This method neither hashes nor derives anything, and it must stay that way. The
// caller that owns issuance generates the SASL password, returns it to the API
// caller exactly once, derives the reference and hands only the reference here. If
// a future change moves derivation into this method, the next step is a matching
// verify method, and a verify method is the first step toward a reveal method —
// which is precisely the path the schema was designed to close off. There is no
// column here capable of holding a password, so there is nothing for such a path to
// read from.
//
// Nothing in this method may carry a secret: the reference is written as a bound
// parameter and is never interpolated into a message, never passed as apierror
// details (which are logged) and never recorded as a span attribute.
//
// # A reissue overwrites, and that is correct
//
// Both columns are written together because together they are one issuance record,
// and issuing again replaces both. The previous credential is revoked out of band
// at the broker, so retaining a history of superseded references here would add an
// audit surface with no reader and no authority — do not add one.
//
// # A missing subscriber is an error, never a no-op
//
// Zero affected rows means the issuance was recorded against a subscriber that does
// not exist. Reporting success there would let the credential endpoint hand back a
// working credential for an unregistered subscriber, so it fails loudly instead.
//
// An empty reference is rejected rather than written: once stored it would be
// indistinguishable from NULL and would make the "registered, not yet provisioned"
// test lie about a subscriber that really had been provisioned.
//
// # SECRET-01: the FORMAT is validated, so a secret cannot be mislabelled as a reference
//
// The column is TEXT and would accept anything — INCLUDING A PLAINTEXT PASSWORD handed over by
// a caller who misunderstood the field. Once a secret has been written to a column documented
// as never holding one, removing it is an incident rather than a migration: it is in the
// backups, in the replicas, and in whatever read it since.
//
// So the value is checked against the format model.DeriveCredentialReference produces before it
// is written. A generated SASL secret cannot satisfy that shape — it has no scheme prefix and no
// 64-character hex digest — so the mistake is refused at the boundary instead of being detected
// later by reading the column, which is the one way of detecting it that requires reading
// secrets. The check makes "no secret is stored here" a property of the schema rather than a
// convention in a comment.
func (d Datasource) RecordSubscriberCredential(ctx context.Context, subscriberID, credentialReference string, issuedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredential")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	if strings.TrimSpace(credentialReference) == "" {
		err := apierror.NewAPIError(apierror.ErrBadRequest, "Credential reference is required", nil)
		span.RecordError(err)
		return err
	}
	if err := model.ValidateCredentialReference(credentialReference); err != nil {
		// The offending value is NOT quoted, in the message or in the details: if a caller has
		// passed a secret by mistake, echoing it into a log is the very disclosure this check
		// exists to prevent.
		err = apierror.NewAPIError(apierror.ErrInvalidInput,
			"The credential reference is not a reference derived by the issuance service", nil)
		span.RecordError(err)

		return err
	}
	if issuedAt.IsZero() {
		// A zero timestamp would be stored as year 1 and read back as a real
		// issuance instant, so the current time is used instead. The two credential
		// columns are only ever meaningful together.
		issuedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_reference = $1,
			credential_issued_at = $2,
			updated_at = $3
		WHERE subscriber_id = $4
	`, credentialReference, issuedAt, time.Now(), subscriberID)
	if err != nil {
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to record subscriber credential", "record_subscriber_credential", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to record subscriber credential"); err != nil {
		span.RecordError(err)
		return err
	}

	// The issuance instant is recorded because it is an audit fact rather than a
	// secret. The reference itself is not recorded, not even truncated.
	span.AddEvent("Subscriber credential recorded", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.credential_issued_at", issuedAt.UTC().Format(time.RFC3339)),
	))
	return nil
}

// MarkSubscriberMigrated stamps migrated_at, recording that the subscriber has
// completed its move from legacy HTTP webhook delivery to Kafka consumption.
//
// A NULL migrated_at means NOT YET MIGRATED, which is exactly what
// migration-progress reporting counts during the 30-day dual-delivery window and
// what the index over the column exists to serve. Together with webhook_url this
// is DUAL-RUN-ONLY state: both columns and that index are dropped at sunset, and
// this method goes with them.
//
// Re-stamping simply moves the timestamp forward. It is idempotent in effect — a
// subscriber that is migrated stays migrated — so a retried migration step needs no
// guard at the call site.
func (d Datasource) MarkSubscriberMigrated(ctx context.Context, subscriberID string, migratedAt time.Time) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberMigrated")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)
		return err
	}
	if migratedAt.IsZero() {
		// A zero timestamp is still NOT NULL, so it would read as "migrated in year
		// 1" and quietly corrupt progress reporting rather than leaving the row
		// counted as unmigrated.
		migratedAt = time.Now()
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET migrated_at = $1,
			updated_at = $2
		WHERE subscriber_id = $3
	`, migratedAt, time.Now(), subscriberID)
	if err != nil {
		span.RecordError(err)
		return loggedDatabaseError(apierror.ErrInternalServer, "Failed to mark subscriber migrated", "mark_subscriber_migrated", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to mark subscriber migrated"); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent("Subscriber marked migrated", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.migrated_at", migratedAt.UTC().Format(time.RFC3339)),
	))
	return nil
}

// ClearSubscriberCredential erases a subscriber's credential record, returning it to the
// "registered, not yet provisioned" state.
//
// # AUTH-01: the registry has to be able to say that a credential is gone
//
// Revocation happens at the broker — deleting the SCRAM credential is what actually ends
// access, because it is what the SASL handshake checks. But if the registry still shows a
// credential reference and an issuance timestamp afterwards, every reader of the registry
// believes the subscriber is provisioned: the operator triaging it, the migration report
// counting provisioned subscribers, and any future reconciliation comparing registry state
// against broker state. The two records have to be able to agree.
//
// It is the COMPENSATING half of RecordSubscriberCredential and is written the same way: both
// columns together, because together they are one issuance record. Nothing here touches the
// broker; the caller revokes there FIRST and clears here second, so a failure between the two
// leaves the registry claiming a credential that no longer exists — which is the safe
// direction, because it over-reports access rather than under-reporting it.
//
// It is IDEMPOTENT with respect to the credential columns: clearing a subscriber that holds no
// credential is a successful no-change, because the desired end state has been reached. A
// MISSING SUBSCRIBER is still an error, for the same reason every other write here reports one
// — silently succeeding would let a caller believe it had cleaned up a row that does not exist.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//
// Returns:
//   - error: a typed not-found error when no subscriber matches, or a logged internal error.
func (d Datasource) ClearSubscriberCredential(ctx context.Context, subscriberID string) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClearSubscriberCredential")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET credential_reference = NULL,
			credential_issued_at = NULL,
			updated_at = $1
		WHERE subscriber_id = $2
	`, time.Now(), strings.TrimSpace(subscriberID))
	if err != nil {
		span.RecordError(err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to clear subscriber credential", "clear_subscriber_credential", err)
	}

	if err := assertSubscriberRowAffected(result, "Failed to clear subscriber credential"); err != nil {
		span.RecordError(err)

		return err
	}

	span.AddEvent("Subscriber credential cleared", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))

	return nil
}

// TakeEventSubscriber removes a subscriber and RETURNS the row it removed.
//
// # AUTH-01: deletion must hand back what has to be revoked
//
// DeleteEventSubscriber tells a caller only whether a row went away, which is not enough to
// deprovision: revoking at the broker needs the principal and the authorised topics, and those
// are only in the row that has just been deleted. A caller therefore had to read, then delete,
// then revoke — and between the read and the delete the row could change, so it could revoke a
// boundary that was no longer the one in force.
//
// Returning the deleted row closes that window: what is revoked is exactly what was removed,
// in one statement. The intended sequence is deleted-row-then-revoke, and the ORDER is
// deliberate: revoking first and failing to delete leaves a registry row claiming access that
// no longer exists (over-reporting, discoverable), while deleting first and failing to revoke
// leaves live broker access with nothing to describe it (under-reporting, invisible). The
// returned row is what makes the second recoverable — a caller that cannot revoke can log the
// principal it must revoke by hand.
//
// A missing subscriber is a typed not-found error rather than a nil row, so "already gone" and
// "just removed" cannot be confused, and a caller cannot skip the broker-side work by mistake.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//
// Returns:
//   - *model.EventSubscriber: the row as it was immediately before deletion.
//   - error: a typed not-found error when no subscriber matched, or a logged internal error.
func (d Datasource) TakeEventSubscriber(ctx context.Context, subscriberID string) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "TakeEventSubscriber")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return nil, err
	}
	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		DELETE FROM blnk.event_subscribers
		WHERE subscriber_id = $1
		RETURNING `+eventSubscriberColumns,
		strings.TrimSpace(subscriberID),
	)

	deleted, err := scanEventSubscriber(row)
	if err != nil {
		span.RecordError(err)

		if errors.Is(err, sql.ErrNoRows) {
			return nil, apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, nil)
		}

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to delete event subscriber", "take_event_subscriber", err)
	}

	span.AddEvent("Event subscriber deleted", trace.WithAttributes(
		attribute.String("subscriber.id", deleted.SubscriberID),
		attribute.String("subscriber.principal", deleted.KafkaPrincipal),
		attribute.Int("subscriber.authorized_topic_count", len(deleted.AuthorizedTopics)),
	))

	return &deleted, nil
}

// PurgeMigratedSubscriberWebhookURLs erases the legacy webhook URL of every subscriber that
// completed its migration before a cut-off.
//
// # RETAIN-01: the dual-run columns are temporary by design and must actually go
//
// webhook_url exists for one purpose: to give a subscriber already receiving HTTP pushes
// somewhere to be migrated FROM. Once it has migrated, the column holds a third-party endpoint
// — an operational secret of somebody else's system, and a destination that becomes a request
// Blnk makes the moment any sender is wired to it — with no remaining use. Keeping it is
// retention without a purpose, which is the definition of the finding.
//
// The URL is set to NULL rather than the row being deleted, because the subscriber is still a
// live subscriber; only the migration artefact is expired. migrated_at is deliberately KEPT: it
// is an audit fact about when the cutover happened, it is not personal or third-party data, and
// the migration report reads it.
//
// The cut-off is the caller's, so the retention period is a policy decision made where policy
// belongs and not a constant buried here. A zero time is refused rather than treated as "purge
// everything", because a zero-valued argument is far more often a bug than an intention.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - migratedBefore time.Time: purge subscribers whose migration completed strictly before
//     this instant. Required.
//
// Returns:
//   - int64: how many rows were purged. Zero is a normal outcome.
//   - error: a typed invalid-input error for a zero cut-off, or a logged internal error.
func (d Datasource) PurgeMigratedSubscriberWebhookURLs(
	ctx context.Context,
	migratedBefore time.Time,
) (int64, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "PurgeMigratedSubscriberWebhookURLs")
	defer span.End()

	if migratedBefore.IsZero() {
		err := apierror.NewAPIError(apierror.ErrInvalidInput,
			"A retention cut-off is required to purge migrated subscriber webhook URLs", nil)
		span.RecordError(err)

		return 0, err
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET webhook_url = NULL,
			updated_at = $1
		WHERE webhook_url IS NOT NULL
		  AND migrated_at IS NOT NULL
		  AND migrated_at < $2
	`, time.Now(), migratedBefore.UTC())
	if err != nil {
		span.RecordError(err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to purge migrated subscriber webhook URLs",
			"purge_migrated_subscriber_webhook_urls", err)
	}

	purged, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)

		return 0, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to purge migrated subscriber webhook URLs",
			"purge_migrated_subscriber_webhook_urls", err)
	}

	if purged > 0 {
		logrus.WithFields(logrus.Fields{
			"purged":          purged,
			"migrated_before": migratedBefore.UTC().Format(time.RFC3339),
		}).Info("purged the legacy webhook URL of migrated subscribers")
	}

	span.SetAttributes(attribute.Int64("subscriber.webhook_urls_purged", purged))

	return purged, nil
}

// RecordSubscriberCredentialIfUnchanged persists an issuance ONLY IF the subscriber still
// holds the credential reference the caller last observed.
//
// # AUTH-01: two concurrent issuances must not both report success
//
// Credential issuance is not idempotent — each call generates a new secret and writes it to
// the broker, where the LAST write wins and every earlier secret stops working. Two
// operators issuing at once therefore end with one usable secret and two successful-looking
// responses, and the one holding the loser's secret has a credential that authenticates
// against nothing. They have no way to know: their request returned 200 with a password in
// it.
//
// This is the write that lets the loser find out. The caller reads the subscriber, provisions
// at the broker, then records with the reference it read as `expected`; the UPDATE matches
// only while that is still the stored value. The second writer to arrive matches no row and
// receives a CONFLICT, which its handler turns into "your issuance was superseded, read the
// subscriber and issue again" instead of a secret that does not work.
//
// It does not make issuance atomic across Blnk and the broker — nothing at this layer can,
// because the broker is a separate system of record with its own last-write-wins semantics.
// What it does is make the DIVERGENCE DETECTABLE at the only point where both outcomes are
// still visible, which is the difference between a confusing failure and a silent one.
//
// # Why a CAS rather than a lock
//
// A row lock held across provisioning would serialize correctly, but it would hold a
// PostgreSQL transaction open across a network call to Kafka for the length of the 5-second
// issuance budget — a lock whose duration is set by a third party's responsiveness. The CAS
// costs one predicate and holds nothing.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - expected *string: the credential reference the caller observed before provisioning.
//     Pass nil to require that NO credential has been issued, which is the first-issuance
//     case and is what makes a race between two first issuances detectable too.
//   - credentialReference string: the new non-reversible reference. Validated, and never a
//     secret.
//   - issuedAt time.Time: the issuance instant.
//
// Returns:
//   - error: a typed conflict when the stored reference no longer matches expected, a typed
//     not-found when no subscriber matches, an invalid-input error when the reference is not
//     a derived reference, or a logged internal error.
func (d Datasource) RecordSubscriberCredentialIfUnchanged(
	ctx context.Context,
	subscriberID string,
	expected *string,
	credentialReference string,
	issuedAt time.Time,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "RecordSubscriberCredentialIfUnchanged")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return err
	}

	// The same guard RecordSubscriberCredential applies, and for the same reason: the
	// column must never hold anything a caller could authenticate with, and the offending
	// value is deliberately not echoed into the error, because a caller who passed a
	// secret here by mistake must not have it copied into a log line.
	if err := model.ValidateCredentialReference(credentialReference); err != nil {
		wrapped := apierror.NewAPIError(apierror.ErrInvalidInput,
			"Credential reference must be a reference derived by model.DeriveCredentialReference", nil)
		span.RecordError(wrapped)

		return wrapped
	}

	span.SetAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.Bool("subscriber.first_issuance", expected == nil),
	)

	// Two arms rather than one predicate, because `credential_reference = NULL` is never
	// true in SQL: IS NULL and = <value> are different operators, and folding them into one
	// statement with a coalesce would make an empty-string reference collide with the
	// no-credential case.
	var (
		result sql.Result
		err    error
		now    = time.Now()
	)
	if expected == nil {
		result, err = d.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET credential_reference = $1,
				credential_issued_at = $2,
				updated_at = $3
			WHERE subscriber_id = $4
			  AND credential_reference IS NULL
		`, credentialReference, issuedAt, now, strings.TrimSpace(subscriberID))
	} else {
		result, err = d.Conn.ExecContext(ctx, `
			UPDATE blnk.event_subscribers
			SET credential_reference = $1,
				credential_issued_at = $2,
				updated_at = $3
			WHERE subscriber_id = $4
			  AND credential_reference = $5
		`, credentialReference, issuedAt, now, strings.TrimSpace(subscriberID), *expected)
	}
	if err != nil {
		span.RecordError(err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record subscriber credential",
			"record_subscriber_credential_if_unchanged", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		span.RecordError(err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to record subscriber credential",
			"record_subscriber_credential_if_unchanged", err)
	}

	if affected == 0 {
		// No row matched, and the two reasons need different answers: the subscriber may not
		// exist, or it may exist holding a different reference. A separate read distinguishes
		// them, because reporting a conflict for a subscriber that was never registered
		// would send an operator looking for a race that did not happen.
		if _, readErr := d.GetEventSubscriberByID(ctx, subscriberID); readErr != nil {
			span.RecordError(readErr)

			return readErr
		}

		conflict := apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber's credential changed while this issuance was in flight",
			fmt.Errorf("subscriber %q no longer holds the expected credential reference; "+
				"a concurrent issuance superseded this one, and the secret it returned is the "+
				"one that works", subscriberID))
		span.RecordError(conflict)

		return conflict
	}

	span.AddEvent("Subscriber credential recorded", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.credential_fingerprint",
			model.CredentialFingerprint(credentialReference)),
	))

	return nil
}

// ---------------------------------------------------------------------------------------
// The revocation lifecycle and the provisioning fence
//
// Two systems hold a subscriber's access — this table holds WHICH principal and WHICH
// topics, the broker holds the credential and the bindings — and no transaction spans
// them. Everything below exists because of that, and each piece answers one of the two
// ways the gap between them can be lost:
//
//   - A REVOCATION THAT FAILED must stay recoverable, so deregistration tombstones the row
//     before it touches the broker and deletes it only once the revocation is confirmed.
//   - TWO CONCURRENT OPERATIONS on one subscriber must not interleave, so each claims the
//     subscriber under a leased token before it touches the broker.
// ---------------------------------------------------------------------------------------

// defaultSubscriberFenceLease is the fence lease used when a caller supplies none.
//
// It is three times the credential issuance budget, which is the shape the lease has to have:
// long enough that a legitimate issuance cannot lose its own claim mid-flight, short enough
// that a process killed while holding one does not fence the subscriber for materially longer
// than an operator would wait before retrying.
const defaultSubscriberFenceLease = 15 * time.Second

// MarkSubscriberRevocationPending stamps the revocation tombstone and returns the row.
//
// # Why the tombstone comes before the broker call
//
// Deregistration used to delete the row and then revoke. When the revocation failed, the
// principal kept authenticating and kept reading, and the only record of WHICH principal that
// was had just been deleted — so the residue was live access that nothing in Blnk could see,
// recoverable only from a log line if anyone read it.
//
// Marking first inverts that. The row survives, it names the principal and the topics
// revocation needs, and it says plainly that the subscriber is on its way out. A failed
// revocation therefore leaves a durable to-do item rather than an invisible one, and retrying
// the deregistration finishes the job.
//
// # It is idempotent, and it keeps the FIRST instant
//
// A retry re-marks a row that is already marked, and COALESCE keeps the original timestamp
// rather than refreshing it. That is deliberate: the value an operator needs is how long this
// revocation has been outstanding, and a timestamp that moved on every retry would report the
// age of the last attempt instead — always small, however long the row had been stuck.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - pendingAt time.Time: the instant to record on the FIRST marking.
//
// Returns:
//   - *model.EventSubscriber: the marked row, carrying the principal and topics to revoke.
//   - error: a typed not-found when no subscriber matches, or a logged internal error.
func (d Datasource) MarkSubscriberRevocationPending(
	ctx context.Context,
	subscriberID string,
	pendingAt time.Time,
) (*model.EventSubscriber, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "MarkSubscriberRevocationPending")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return nil, err
	}

	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	row := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_subscribers
		SET revocation_pending_at = COALESCE(revocation_pending_at, $1),
			updated_at = $2
		WHERE subscriber_id = $3
		RETURNING `+eventSubscriberColumns,
		pendingAt, time.Now(), strings.TrimSpace(subscriberID),
	)

	marked, err := scanEventSubscriber(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			notFound := apierror.NewAPIError(apierror.ErrSubscriberNotFound, subscriberNotFoundMessage, err)
			span.RecordError(notFound)

			return nil, notFound
		}

		span.RecordError(err)

		return nil, loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to mark the subscriber for revocation",
			"mark_subscriber_revocation_pending", err)
	}

	span.AddEvent("Subscriber marked for revocation", trace.WithAttributes(
		attribute.String("subscriber.id", marked.SubscriberID),
		attribute.String("subscriber.principal", marked.KafkaPrincipal),
	))

	return &marked, nil
}

// ClaimSubscriberForProvisioning fences a subscriber for one issuance or revocation and
// returns the token that claim is held under.
//
// # The interleaving it makes impossible
//
// Kafka stores ONE SCRAM credential per principal. Two overlapping issuances therefore both
// write a credential and the second replaces the first, so the broker ends up holding one
// password while this table may hold the reference derived from the other — and the caller
// holding the recorded one cannot authenticate. RecordSubscriberCredentialIfUnchanged detects
// the case where both callers observed the same prior reference, but it cannot decide which
// password the BROKER kept: that is settled by whichever call reached the broker last,
// independently of who won the database.
//
// So the broker is only ever touched under this claim. The second caller is refused as a
// conflict before it generates a secret, which is the whole point — a refused issuance costs
// a caller one retry, while an interleaved one costs it a credential that does not work and
// gives it no way to find out.
//
// Revocation and deregistration claim it too, so an issuance cannot interleave with the
// removal of the very credential it is writing.
//
// # Why a leased claim token rather than a lock
//
// It is the shape blnk.event_outbox already claims rows with, and it is chosen for the same
// two reasons. It is cross-process without a lock server, so two Blnk instances fence each
// other. And it EXPIRES: a process killed mid-issuance does not fence the subscriber for
// ever, which a session-scoped advisory lock would also achieve but only by holding a
// database connection open across a call to Kafka — a lock whose duration a third party's
// responsiveness decides.
//
// The token is rotated on every claim, so a caller whose lease expired cannot release or
// complete a claim that has since been taken over.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - lease time.Duration: how long the claim is held. Non-positive is normalised to
//     defaultSubscriberFenceLease rather than rejected, because a claim that expires on
//     arrival fences nothing while failing the call would stop provisioning outright.
//
// Returns:
//   - string: the claim token, which every completion or release must present.
//   - error: a typed conflict when another operation holds a live claim, a typed not-found
//     when no subscriber matches, or a logged internal error.
func (d Datasource) ClaimSubscriberForProvisioning(
	ctx context.Context,
	subscriberID string,
	lease time.Duration,
) (string, error) {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ClaimSubscriberForProvisioning")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return "", err
	}

	if lease <= 0 {
		logrus.WithField("requested_lease", lease.String()).
			Warnf("Non-positive subscriber provisioning fence lease; falling back to %s", defaultSubscriberFenceLease)
		lease = defaultSubscriberFenceLease
	}

	token := uuid.NewString()

	span.SetAttributes(
		attribute.String("subscriber.id", subscriberID),
		attribute.String("subscriber.fence_lease", lease.String()),
	)

	var claimed string
	err := d.Conn.QueryRowContext(ctx, `
		UPDATE blnk.event_subscribers
		SET provisioning_token = $1,
			provisioning_until = NOW() + $2::interval,
			updated_at = $3
		WHERE subscriber_id = $4
		  AND (provisioning_until IS NULL OR provisioning_until < NOW())
		RETURNING provisioning_token
	`, token, lease.String(), time.Now(), strings.TrimSpace(subscriberID)).Scan(&claimed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No row matched, and the two reasons need different answers: the subscriber may
			// not exist, or it may exist under a live claim. A separate read distinguishes
			// them, because reporting a conflict for a subscriber that was never registered
			// would send a caller looking for a race that did not happen.
			if _, readErr := d.GetEventSubscriberByID(ctx, subscriberID); readErr != nil {
				span.RecordError(readErr)

				return "", readErr
			}

			conflict := apierror.NewAPIError(apierror.ErrConflict,
				"Another credential operation for this subscriber is already in progress",
				fmt.Errorf("subscriber %q is fenced by a live provisioning claim; retry once it "+
					"completes or once its lease expires", subscriberID))
			span.RecordError(conflict)

			return "", conflict
		}

		span.RecordError(err)

		return "", loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to claim the subscriber for provisioning",
			"claim_subscriber_for_provisioning", err)
	}

	span.AddEvent("Subscriber claimed for provisioning", trace.WithAttributes(
		attribute.String("subscriber.id", subscriberID),
	))

	return claimed, nil
}

// ReleaseSubscriberProvisioningFence clears a claim, if the caller still holds it.
//
// Releasing early is what keeps the fence from making a retry wait out the whole lease after
// a fast failure. It is conditional on the token for the same reason every other transition in
// this schema is: a caller whose lease expired no longer owns the claim, and clearing a claim
// somebody else has taken would let a third operation start alongside it.
//
// A claim that no longer matches is REPORTED, not swallowed. This function's callers run it in
// a deferred cleanup where the useful response is a log line rather than a failed request, so
// the policy belongs at the call site; reporting nothing here would hide a fence that had been
// lost mid-operation, which is exactly the condition worth knowing about.
//
// Parameters:
//   - ctx context.Context: cancels the statement.
//   - subscriberID string: the business key. Required.
//   - token string: the token the claim was taken under. Required.
//
// Returns:
//   - error: a typed conflict when the claim is no longer the caller's, a typed validation
//     error for a missing token, or a logged internal error.
func (d Datasource) ReleaseSubscriberProvisioningFence(
	ctx context.Context,
	subscriberID string,
	token string,
) error {
	ctx, span := otel.Tracer("transaction.database").Start(ctx, "ReleaseSubscriberProvisioningFence")
	defer span.End()

	if err := requireSubscriberID(subscriberID); err != nil {
		span.RecordError(err)

		return err
	}

	if strings.TrimSpace(token) == "" {
		err := apierror.NewAPIError(apierror.ErrInvalidInput,
			"A provisioning claim token is required to release the fence", nil)
		span.RecordError(err)

		return err
	}

	span.SetAttributes(attribute.String("subscriber.id", subscriberID))

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.event_subscribers
		SET provisioning_token = NULL,
			provisioning_until = NULL,
			updated_at = $1
		WHERE subscriber_id = $2
		  AND provisioning_token = $3
	`, time.Now(), strings.TrimSpace(subscriberID), strings.TrimSpace(token))
	if err != nil {
		span.RecordError(err)

		return loggedDatabaseError(apierror.ErrInternalServer,
			"Failed to release the subscriber provisioning fence",
			"release_subscriber_provisioning_fence", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// The driver could not report a count. The release itself succeeded, and the caller
		// only logs this outcome, so reporting success is the honest answer rather than
		// manufacturing a conflict from a bookkeeping gap.
		logrus.WithError(err).WithField("subscriber", subscriberID).
			Debug("Could not determine whether the subscriber provisioning fence was released")

		return nil
	}

	if affected == 0 {
		conflict := apierror.NewAPIError(apierror.ErrConflict,
			"The subscriber provisioning claim is no longer held by this caller",
			fmt.Errorf("releasing the provisioning fence of subscriber %q matched no row; the claim "+
				"expired or was taken over while the operation was in flight", subscriberID))
		span.RecordError(conflict)

		return conflict
	}

	return nil
}
