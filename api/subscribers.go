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

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/sirupsen/logrus"
)

// This file is the HTTP surface of the Kafka subscriber registry: ten
// master-key-gated endpoints in three groups — registry CRUD on /subscribers,
// credential issuance on /subscribers/:subscriber_id/kafka-credentials, and the
// deprecated legacy /subscribers/:subscriber_id/webhook-subscription surface, whose
// own section below explains why it exists.
//
// # WHERE THE BOUNDARY SITS
//
// The Kafka identity and every privileged operation on it stay out of this file:
// no SQL, no Kafka client, no SCRAM computation, no password generation and no ACL
// construction. What the handlers own is the HTTP contract — route parameters,
// request binding, response projection and error codes — and, where a route's
// contract needs more than one call, the ordering of those calls. Deleting a
// webhook subscription is the clearest case: it clears the recorded URL and then
// stamps the migration instant, and the reason that order matters is documented on
// the handler. Three divisions are load-bearing rather than stylistic:
//
//   - The subscriber identifier is the root of the subscriber's whole Kafka
//     identity: the SASL principal and the consumer-group namespace are DERIVED
//     from it by the root package, and every ACL binding is granted against
//     those derived values. A handler that assembled either would be a second
//     answer to "who is this principal", and a caller able to influence it could
//     name another subscriber's namespace.
//   - Credential issuance mints a secret, upserts a SCRAM credential, binds ACLs
//     and records the issuance, and it compensates a partial failure by revoking
//     what it wrote. Splitting any of that across the HTTP layer would put a
//     compensation path outside the operation it compensates.
//   - Error responses go through respondCode/respondError from api/errors.go with
//     a typed code from internal/apierror. An ad-hoc c.JSON status here would
//     bypass statusByCode, which is the single source of truth for the status of
//     every error code, and an unmapped code silently becomes 500.
//
// # PRIVILEGE
//
// All ten are OPERATOR endpoints and every one of them gates on the master key as
// its very first act, before a parameter is read and before a body is bound,
// mirroring ensureHookManagementAuthorized. Issuance is the reason this is not
// negotiable: a scoped API key that could reach it would be able to mint a Kafka
// credential for any subscriber in the deployment and read that subscriber's
// event stream. Gating first also keeps the routes from becoming an oracle — a
// non-master caller learns that the master key is required and never whether a
// given subscriber id exists.
//
// # RESPONSES COMMON TO EVERY HANDLER
//
// Each handler below documents only the responses distinctive to it. These five are
// produced by every one of the ten and are not repeated:
//
//	400 GEN_MISSING_PARAMETER    a blank identifier in the route
//	403 AUTH_MASTER_KEY_REQUIRED a non-master caller
//	404 SUBSCRIBER_NOT_FOUND     no such subscriber
//	500 GEN_INTERNAL             a repository failure
//
// and, on the four deprecated webhook-subscription routes only:
//
//	410 GEN_GONE                 past the retirement instant, written by the
//	                             per-route sunset guard before the handler is reached
//
// # THE SECRET
//
// The plaintext SASL password exists in exactly one expression in this file: the
// KafkaCredentialsResponse literal in IssueKafkaCredentials. It is never logged,
// never interpolated into a message, never placed in an apierror details payload,
// and never returned by any read. This file imports no logger at all, which is
// the structural form of that promise. Only a non-reversible reference and the
// issuance instant are persisted, and blnk.event_subscribers has no column able
// to hold the secret, so a lost password can be replaced but never recovered.
//
// Because that one response is the only chance to disclose the password, it is
// also the only place in this package where the CHANNEL is part of the contract:
// ensureCredentialTransportConfidential refuses issuance unless the deployment has
// established the channel as confidential — TLS in-process, a declared proxy
// boundary reporting https, or a loopback peer. An authenticated request over
// plaintext is a correctly authorised leak, and no amount of care about where the
// password is not written protects against carrying it in clear.
//
// # NO SUNSET LOGIC LIVES HERE
//
// The four deprecated handlers read no date, call no sunset predicate and compare
// no instants. api/api.go installs middleware.WebhookSunsetGuard globally, ahead of
// authentication, and it retires the whole path — these four methods, every method
// nobody registered, and every unauthenticated attempt — by asking the root
// package's single predicate. Two independent comparisons could let the service
// stop dual-writing while still accepting webhook management calls, or the reverse,
// and neither failure raises anything to notice.

const (
	// subscriberPageDefaultLimit and subscriberPageMaxLimit are the page bounds
	// applied at the HTTP boundary.
	//
	// They match ParseFiltersFromBody in api/filter_helper.go exactly —
	// including its habit of resetting an oversized limit to the default rather
	// than clamping it to the ceiling — so that every list endpoint in this
	// package answers a page request the same way. The repository applies its own
	// bounds as well, which is what protects an in-process caller; these are the
	// public surface's uniform ones.
	subscriberPageDefaultLimit = 20
	subscriberPageMaxLimit     = 100

	// subscriberRouteParam is the route parameter every per-subscriber route
	// carries, declared once so the readers and the error messages cannot drift
	// from the paths api/api.go registers.
	subscriberRouteParam = "subscriber_id"

	// headerForwardedProto and forwardedProtoHTTPS are the proxy's report of the
	// scheme the CLIENT used, which credential issuance consults only when the
	// deployment has declared the proxy that sets it — see
	// ensureCredentialTransportConfidential. Named constants rather than inline
	// literals because the header is compared case-insensitively in one place and
	// a second spelling of either would be a silently weaker check.
	headerForwardedProto = "X-Forwarded-Proto"
	forwardedProtoHTTPS  = "https"
)

// Query-parameter names, declared once so the readers and any message naming
// them agree by construction.
const (
	subscriberQueryParamLimit        = "limit"
	subscriberQueryParamCursor       = "cursor"
	subscriberQueryParamIncludeCount = "include_count"
	subscriberQueryParamSortBy       = "sort_by"
	subscriberQueryParamSortOrder    = "sort_order"

	// The event-stream parameters. topic and partition address the log, offset is the
	// caller's cursor, limit bounds the page (shared with the listing above, since it means
	// the same thing) and max_wait_ms is how long the broker may hold the read.
	subscriberQueryParamTopic     = "topic"
	subscriberQueryParamPartition = "partition"
	subscriberQueryParamOffset    = "offset"
	subscriberQueryParamMaxWait   = "max_wait_ms"

	// headerSubscriberSecret and headerSubscriberPrincipal carry the subscriber's OWN SASL
	// credential to the event-stream endpoint.
	//
	// A header rather than a query parameter, and the distinction is not cosmetic: query
	// strings are written to access logs, proxy logs and browser history by default, and this
	// value is a live password. The principal is optional — the registry already knows which
	// one belongs to the subscriber — and is checked when sent, because a caller naming a
	// different principal is asking about a different identity.
	headerSubscriberSecret    = "X-Blnk-Subscriber-Secret"
	headerSubscriberPrincipal = "X-Blnk-Subscriber-Principal"
)

// subscriberStreamQueryParameters is every query parameter GET /subscribers/:id/events
// accepts, in the order its refusal lists them.
//
// Closed for the same reason the listing's set is: a misspelled `?offsett=100` accepted in
// silence would serve the default offset, and a client reading the answer as its own cursor
// would either re-read records it had already processed or skip past ones it had not.
var subscriberStreamQueryParameters = []string{
	subscriberQueryParamTopic,
	subscriberQueryParamPartition,
	subscriberQueryParamOffset,
	subscriberQueryParamLimit,
	subscriberQueryParamMaxWait,
}

// subscriberListQueryParameters is every query parameter GET /subscribers accepts,
// in the order the refusal lists them.
//
// # Why an unrecognised parameter is refused rather than ignored
//
// The set used to be open, so `?limitt=5`, `?status=active` or `?topic=blnk.balances`
// were accepted in silence and the caller received the whole first page. An operator
// running a migration report cannot tell that from a correct answer, and would read
// "every subscriber" as "every subscriber matching my filter" — the more dangerous of
// the two readings, because it looks complete.
//
// sort_by and sort_order are accepted and INERT, exactly as they are on the
// dead-letter inventory: the repository fixes the ordering because paging stability
// depends on it, and a client-chosen sort column could show a row twice or skip it
// between pages. They are accepted rather than refused so a caller reusing a generic
// list-endpoint client is not rejected for sending them.
var subscriberListQueryParameters = []string{
	subscriberQueryParamLimit,
	subscriberQueryParamCursor,
	subscriberQueryParamIncludeCount,
	subscriberQueryParamSortBy,
	subscriberQueryParamSortOrder,
	// THE TWO NARROWINGS THIS ROUTE ALSO ANSWERS. They have to be members of the accepted set
	// or the closed-set check refuses them as unknown before the dispatch below can read them,
	// which is how they came to be unreachable.
	subscriberQueryParamPseudonym,
	subscriberQueryParamRevocationPending,
}

// Client-facing messages, held as constants so that every response carrying one
// is byte-identical. Interpolating the request path or the subscriber id would
// make every body slightly different, which costs log aggregation its grouping
// and invites a client to parse prose when the value to branch on is the
// machine-readable code in error_detail.
const (
	// subscriberIDRequiredMessage mirrors Api.Search's shape for a missing route
	// parameter: what is missing, and where it belongs.
	subscriberIDRequiredMessage = "subscriber_id is required. pass it in the route /subscribers/:subscriber_id"

	// subscriberInvalidLimitMessage answers a page size that is not an integer at all.
	// A value that is merely out of range is normalised, but a non-numeric one is a
	// client mistake and is refused rather than silently defaulted — defaulting it
	// would answer a different question from the one asked, with nothing to say so.
	subscriberInvalidLimitMessage = "Invalid " + subscriberQueryParamLimit + " value"

	// subscriberInvalidCursorMessage answers a cursor value this endpoint cannot decode
	// into a page position.
	//
	// It is REFUSED rather than treated as "start from the beginning" (PERF-P08). A
	// paging client that receives page one in answer to a cursor it thought pointed
	// into the middle of the registry pages for ever.
	//
	// It says DECODE and not "a cursor this endpoint issued", which is what it used to
	// claim. A keyset cursor is a coordinate rather than a capability — unsigned, with no
	// issuer recorded — so any value that decodes to a well-formed position is accepted as
	// one, including a token another listing produced. Harmless, since the cursor carries
	// no authorization, but the stronger wording invited a client to hunt for an issuance
	// record that does not exist. The dead-letter listing answers in the same terms; the
	// two must not describe one mechanism two ways.
	subscriberInvalidCursorMessage = "\"" + subscriberQueryParamCursor +
		"\" could not be decoded as a page position; omit it for the first page, or pass back " +
		"the \"next_cursor\" value from a previous response verbatim"

	// subscriberMissingRowMessage covers a service that reported success without
	// returning the row. It cannot happen through any current path, and it is
	// answered rather than dereferenced so that the impossible case is a 500 with
	// an explanation instead of a panic recovered by the middleware.
	subscriberMissingRowMessage = "The subscriber registry reported success without returning the subscriber"

	// credentialIssuanceTimedOutMessage and credentialIssuanceCancelledMessage
	// are the last-resort answers for an issuance whose context ended while the
	// failure carried no typed code of its own. The budget is named in neither,
	// because the value is configuration and a stale number in a message is
	// worse than no number.
	credentialIssuanceTimedOutMessage = "Provisioning Kafka credentials did not complete within the issuance budget"

	credentialIssuanceCancelledMessage = "Provisioning Kafka credentials was cancelled before it completed"

	// credentialInsecureTransportMessage is the refusal to put a one-time SASL
	// password on a channel this deployment has not declared confidential.
	//
	// It names all three ways to satisfy the contract, because every one of them
	// is a deployment change and the operator reading this message is the person
	// who makes it. It names no header value and no address, so the body is
	// identical for every refused request and discloses nothing about the topology
	// back to the caller.
	credentialInsecureTransportMessage = "Kafka credentials are not issued over a transport this " +
		"deployment has not established as confidential, because the response carries a one-time " +
		"password. Terminate TLS in Blnk (BLNK_SERVER_SSL), or declare the proxy that terminates it " +
		"and sets X-Forwarded-Proto (BLNK_SERVER_TRUST_FORWARDED_PROTO), or call this endpoint over " +
		"loopback"

	// credentialIssuanceAbandonedMessage answers a request the handler stopped waiting for
	// because the wall-clock ceiling elapsed while the service was still working.
	//
	// It says explicitly that a credential may have been minted, because that is the one thing
	// the caller cannot otherwise know and the one thing that changes what it should do:
	// re-issuing is safe and is the remedy, since Kafka holds one SCRAM credential per
	// principal and the next issuance replaces whatever this one left behind.
	credentialIssuanceAbandonedMessage = "Provisioning Kafka credentials did not complete within the " +
		"issuance budget and was abandoned; a credential may have been created and was not returned. " +
		"Re-issue to obtain one"
)

// errSubscribersRequireMasterKey is the message a non-master caller receives from
// any of the ten endpoints in this file.
var errSubscribersRequireMasterKey = errors.New("subscriber management requires master key")

// ensureSubscriberManagementAuthorized enforces the master key on the subscriber
// management surface, writing the refusal itself when the caller does not hold it.
//
// It mirrors ensureHookManagementAuthorized in shape and reuses isMasterKeyRequest
// rather than restating how the auth middleware records the principal, so there is
// one reading of "is this the master key" in this package.
//
// The refusal carries apierror.ErrAuthMasterKeyRequired. That code and
// ErrAuthUnknownResource both resolve to HTTP 403, so a test proving this gate
// fired — as opposed to the /subscribers prefix being unmapped in
// middleware.pathToResource, which aborts every request to it — must assert on
// error_detail.code and never on the status alone.
func ensureSubscriberManagementAuthorized(c *gin.Context) bool {
	if isMasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errSubscribersRequireMasterKey.Error(), nil)

	return false
}

// ensureCredentialTransportConfidential refuses credential issuance unless the request
// arrived over a channel this deployment has ESTABLISHED as confidential, writing the
// refusal itself when it did not.
//
// # Why an authenticated, authorised request can still be refused
//
// POST /subscribers/{id}/kafka-credentials is the only endpoint in Blnk whose response
// body contains a secret, and it contains it exactly once: the password is not stored and
// cannot be read back, so the response carrying it is the single opportunity to disclose
// it to anything on the wire. Authentication answers WHO is asking. It says nothing about
// whether the answer can be read by somebody else on the way back, and those are
// independent — a correct master key over plaintext HTTP is a correctly authorised
// credential leak.
//
// The deployment shape makes this concrete rather than theoretical. Blnk's own listener is
// plaintext unless BLNK_SERVER_SSL is set, and the Kubernetes manifests terminate TLS at an
// ingress with a plaintext hop to the pod, so a process that assumed "the caller used
// https" would be assuming the part it cannot see.
//
// # The three channels, and why each is something the process can establish
//
//  1. TLS IN-PROCESS. c.Request.TLS is non-nil only when this process completed the
//     handshake. Proven, not asserted.
//  2. A DECLARED PROXY BOUNDARY. X-Forwarded-Proto is a request header, so any client can
//     send it and no process can tell a proxy's value from a caller's. It is therefore
//     believed only when BLNK_SERVER_TRUST_FORWARDED_PROTO declares that a proxy in front
//     of Blnk sets it and overwrites what the client sent — the operator asserting the
//     topology once, explicitly, instead of every request asserting it for itself. A
//     declaration plus a header reading anything other than https is a REFUSAL: the proxy
//     is reporting a plaintext client hop.
//  3. A LOOPBACK PEER. Bytes to 127.0.0.0/8 or ::1 never reach a network, so a local
//     operator or a shell inside the container can issue a credential with no TLS at all.
//     The peer is read from Request.RemoteAddr — THE SOCKET — and never from
//     c.ClientIP(), which honours X-Forwarded-For and would let a remote caller claim to
//     be local. That distinction is the whole value of this branch.
//
// Anything else is refused, which is deny-by-default: a deployment that has stated
// nothing gets no secret on the wire.
//
// SecurityHeaders in api/middleware takes the header at face value for the same question,
// and that difference is deliberate rather than an inconsistency: believing it there
// merely adds an HSTS header a plaintext client ignores, while believing it here would
// disclose a password.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
//
// Returns:
//   - bool: true when the channel is confidential and issuance may proceed; false when
//     the refusal has been written.
func (a *Api) ensureCredentialTransportConfidential(c *gin.Context) bool {
	if a.credentialTransportConfidential(c) {
		return true
	}

	respondCode(c, apierror.ErrSubscriberInsecureTransport, credentialInsecureTransportMessage, nil)

	return false
}

// credentialTransportConfidential reports whether the request's channel is one of the
// three confidential ones ensureCredentialTransportConfidential documents.
//
// Split out so the decision is a pure predicate over the request and the live
// configuration, testable without a response recorder and without a router.
//
// Parameters:
//   - c *gin.Context: the request.
//
// Returns:
//   - bool: true when the channel is confidential.
func (a *Api) credentialTransportConfidential(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}

	// 1. This process terminated TLS.
	if c.Request.TLS != nil {
		return true
	}

	// 2. The deployment declared the proxy that did, and that proxy says the client hop
	// was https. Read live from the configuration store for the same reason
	// subscriberTopicPrefix is: a value captured when the router was built would describe
	// the deployment as it stood then.
	if configuration := a.blnk.Config(); configuration != nil && configuration.Server.TrustForwardedProto {
		if strings.EqualFold(strings.TrimSpace(c.GetHeader(headerForwardedProto)), forwardedProtoHTTPS) {
			return true
		}
	}

	// 3. The peer is on this host, so the bytes never reach a network.
	return requestPeerIsLoopback(c.Request)
}

// requestPeerIsLoopback reports whether the request's PEER — the far end of the accepted
// socket — is a loopback address.
//
// It reads Request.RemoteAddr and nothing else. c.ClientIP() consults X-Forwarded-For and
// X-Real-Ip, which are request headers, so a remote caller could present itself as local
// and satisfy the very check that exists to establish that it is not remote. A malformed
// or absent RemoteAddr is NOT loopback: unparseable means unestablished, and this
// predicate only ever grants confidence.
//
// Parameters:
//   - r *http.Request: the request. May be nil.
//
// Returns:
//   - bool: true when the peer address is a loopback IP.
func requestPeerIsLoopback(r *http.Request) bool {
	if r == nil || strings.TrimSpace(r.RemoteAddr) == "" {
		return false
	}

	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		// Some servers and test harnesses record a bare address with no port.
		host = strings.TrimSpace(r.RemoteAddr)
	}

	// A zone-qualified IPv6 literal ("::1%lo0") is stripped to its address part; ParseIP
	// rejects the qualified form outright.
	if index := strings.Index(host, "%"); index >= 0 {
		host = host[:index]
	}

	address := net.ParseIP(strings.Trim(host, "[]"))

	return address != nil && address.IsLoopback()
}

// subscriberIDFromRoute reads the subscriber identifier out of the route, writing
// the refusal itself when it is absent or blank.
//
// The value is TRIMMED before it is validated and is returned trimmed, because the
// service trims too and returning the untrimmed form would let this layer and the
// service disagree about which row was addressed. Canonicalization would otherwise
// refuse surrounding whitespace outright, which would change what a padded path
// segment answers for every route in this file rather than only for the malformed
// ones this closes.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - string: the trimmed, canonical identifier.
//   - bool: false when the refusal has been written.
func subscriberIDFromRoute(c *gin.Context) (string, bool) {
	subscriberID, passed := c.Params.Get(subscriberRouteParam)

	trimmed := strings.TrimSpace(subscriberID)
	if !passed || trimmed == "" {
		respondCode(c, apierror.ErrGenMissingParameter, subscriberIDRequiredMessage, nil)

		return "", false
	}

	canonical, err := coremodel.CanonicalizeSubscriberIdentifier(trimmed)
	if err != nil {
		// The model's message names the specific rule broken and quotes the offending
		// value, which is what makes the refusal actionable. It is echoed rather than
		// replaced with a generic sentence for that reason; the value came from the
		// request line, so it discloses nothing the caller did not send.
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return "", false
	}

	return canonical, true
}

// subscriberPageLimitFromQuery reads limit and normalises it exactly as
// ParseFiltersFromBody does: absent, non-positive or above the ceiling all become
// the default. A value that is not an integer is refused instead.
func subscriberPageLimitFromQuery(c *gin.Context) (int, bool) {
	raw := strings.TrimSpace(c.Query(subscriberQueryParamLimit))
	if raw == "" {
		return subscriberPageDefaultLimit, true
	}

	limit, err := strconv.Atoi(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, subscriberInvalidLimitMessage, nil)

		return 0, false
	}

	if limit <= 0 || limit > subscriberPageMaxLimit {
		limit = subscriberPageDefaultLimit
	}

	return limit, true
}

// subscriberCursorFromQuery reads the opaque page cursor.
//
// # PERF-P08: this replaced an unbounded OFFSET
//
// The endpoint accepted any non-negative offset and passed it into an OFFSET clause, whose
// cost PostgreSQL pays by reading and discarding every row before it. The depth was the
// caller's to choose and nothing capped it, so the page-size limit bounded the response and
// bounded no work at all. A cursor names the ordering key of the last row the previous page
// returned, which makes every page the same cost and makes the sequence stable under
// concurrent registration.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - *coremodel.SubscriberCursor: the decoded position, nil for the first page.
//   - bool: false when the refusal has been written.
func subscriberCursorFromQuery(c *gin.Context) (*coremodel.SubscriberCursor, bool) {
	cursor, err := coremodel.ParseSubscriberCursor(c.Query(subscriberQueryParamCursor))
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, subscriberInvalidCursorMessage, nil)

		return nil, false
	}

	return cursor, true
}

// optionalSubscriberField converts a request DTO's plain string into the nilable
// form the service and the nullable columns use.
//
// A blank value becomes nil rather than a pointer to "", because for both columns
// this is applied to — partition_key_prefix and webhook_url — NULL means "not
// recorded" while the empty string would mean "recorded as empty", and those are
// different claims. The create DTO cannot express the second, so a blank field
// there is always the first.
func optionalSubscriberField(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	return &value
}

// respondSubscriber writes a stored subscriber as a success body.
//
// It routes every subscriber response through model.NewSubscriberResponse, which
// is the single place the credential reference is reduced to a non-sensitive
// fingerprint and the enforced-access declaration is assembled. No handler here
// builds a subscriber body field by field, so none can leak the reference or
// imply that the advisory partition-key prefix is an enforced boundary.
func respondSubscriber(c *gin.Context, status int, subscriber *coremodel.EventSubscriber) {
	if subscriber == nil {
		respondCode(c, apierror.ErrGenInternal, subscriberMissingRowMessage, nil)

		return
	}

	c.JSON(status, subscriberResponse(*subscriber))
}

// subscriberResponse projects a stored row and then applies the ONE post-sunset
// adjustment the projection itself cannot make.
//
// model.NewSubscriberResponse is the single place a subscriber body is assembled, and
// it stays that way — this adds nothing to the body and removes exactly one field from
// it. api/model cannot ask whether the retirement instant has passed, because the root
// package that owns that decision imports api/model in its own tests and the reverse
// edge would close a cycle; so the question is asked here, where the root package is
// already a dependency, and answered by the same predicate the relay and the 410 guard
// use.
//
// WHAT IT SUPPRESSES, AND WHY ONLY THIS. Past the retirement instant nothing delivers
// to a legacy endpoint and nothing may record one, so continuing to publish
// webhook_url would hand out a third-party URL that describes a subscription which no
// longer exists — the read half of the same retirement the write path refuses. The
// column itself is NOT erased here: erasing rows is
// ClearLegacyWebhookSubscription's and PurgeMigratedWebhookURLs' job, and an operator
// mid-cleanup still needs the value in the database. migrated_at survives in the body,
// because migration progress is an audit fact about Blnk rather than third-party data.
//
// Parameters:
//   - subscriber coremodel.EventSubscriber: the stored registry row.
//
// Returns:
//   - model.SubscriberResponse: the body to write.
func subscriberResponse(subscriber coremodel.EventSubscriber) model.SubscriberResponse {
	// NOTHING TO SUPPRESS HERE, and that is the stronger position. The registry-read shape does
	// not carry the legacy webhook URL AT ALL — see model.NewSubscriberResponse — so a
	// third-party endpoint cannot leak through this route either before or after the retirement
	// instant. Blanking it post-sunset, which is what this function used to do, would have left
	// it disclosed for the whole dual-run window. The URL is readable only from the dedicated
	// webhook-subscription route, which the sunset guard retires outright.
	return model.NewSubscriberResponse(subscriber)
}

// respondWebhookSubscription writes the legacy webhook-subscription read shape.
//
// It carries the recorded URL and the migration instant and nothing else. The
// legacy transport's signing secret and configured headers are deployment-wide
// configuration rather than per-subscriber data, so surfacing them here would
// turn a migration-tracking response into a secret-bearing one.
//
// Deprecated: this projection belongs to the legacy webhook-subscription surface
// and goes when that surface is deleted — a later release, not something the
// retirement instant performs; past the instant it is simply unreachable, because
// the guard answers before any handler that would call it. The marker is not
// decoration: it keeps the deprecated response DTO's own deprecation from being
// reported as a defect at every call site inside the surface being retired, and it
// makes this helper impossible to reuse from a route meant to outlive it.
func respondWebhookSubscription(c *gin.Context, status int, subscriber *coremodel.EventSubscriber) {
	if subscriber == nil {
		respondCode(c, apierror.ErrGenInternal, subscriberMissingRowMessage, nil)

		return
	}

	body := model.WebhookSubscriptionResponse{
		SubscriberID: subscriber.SubscriberID,
		MigratedAt:   subscriber.MigratedAt,
	}

	if subscriber.WebhookURL != nil {
		body.WebhookURL = *subscriber.WebhookURL
	}

	c.JSON(status, body)
}

// respondSubscriberRegistryError writes the failure of a registry operation.
//
// The service and the repository return typed apierror values on every path that
// has a domain meaning, so respondError resolves the code from the error itself.
// Two options are applied on top:
//
//   - withUpgrade(GEN_NOT_FOUND, SUBSCRIBER_NOT_FOUND) because api/errors.go's
//     classifyMessage ends with a broad {"not found"} catch-all. Without the
//     upgrade an unknown subscriber id reached through a path that returned an
//     untyped message would answer GEN_NOT_FOUND, and a client branching on the
//     code could not tell a missing subscriber from a missing anything else. The
//     upgrade applies to the typed branch too, so the answer is the same however
//     the error was constructed.
//   - withDefault(GEN_INTERNAL) so that an unclassified repository failure is
//     reported as an internal error with a sanitised message rather than echoing
//     driver text to the caller.
func respondSubscriberRegistryError(c *gin.Context, err error) {
	respondError(c, err,
		withUpgrade(apierror.ErrGenNotFound, apierror.ErrSubscriberNotFound),
		withDefault(apierror.ErrGenInternal),
	)
}

// isTypedAPIError reports whether err carries a catalog code of its own.
//
// It exists so the issuance failure path can tell "the service already decided
// what this is" from "the context ended and nothing decided anything", without
// re-deriving the code. errors.As is used rather than a type assertion so a
// wrapped error classifies identically.
func isTypedAPIError(err error) bool {
	var typed apierror.APIError

	return errors.As(err, &typed)
}

// respondCredentialIssuanceError writes the failure of a credential issuance.
//
// # Why the order of these branches matters
//
// A TYPED error wins outright. The service classifies its own outcomes and the
// codes it produces are more precise than anything this layer could re-derive:
// SUBSCRIBER_NOT_FOUND for an unknown id, EVENT_KAFKA_UNAVAILABLE when no broker
// is configured or reachable, SUBSCRIBER_BROKERS_NOT_CONFIGURED when no
// subscriber-facing endpoint is published, SUBSCRIBER_PROVISIONING_FAILED when
// the broker refused the credential or its bindings, SUBSCRIBER_GRANT_EMPTY for a
// row authorised for nothing and therefore not provisionable as it stands,
// SUBSCRIBER_ISOLATION_UNENFORCEABLE for a row recording a partition-key prefix,
// which no ACL can express,
// SUBSCRIBER_PROVISIONING_TIMEOUT when the registry itself ran out of
// budget, and a typed conflict when a concurrent issuance superseded this one.
// Downgrading any of those to a single generic code would take information away
// from the operator who has to act on it.
//
// Only when the failure carries NO code of its own is the context consulted, and
// then an expired or cancelled issuance is answered with
// SUBSCRIBER_PROVISIONING_FAILED — 503, retryable — rather than being allowed to
// fall through to a bare 500. That is what the endpoint's budget buys: an expiry is
// ANSWERED rather than waited out, and the answer says whether retrying is sensible.
// It does not promise that every response is written within five seconds — see the
// handler's own budget section for the failure paths that legitimately take longer.
//
// SLA-01: THE TIMEOUT CODE, NOT THE FAILURE CODE. This branch used to answer
// SUBSCRIBER_PROVISIONING_FAILED (503) while the service answered
// SUBSCRIBER_PROVISIONING_TIMEOUT (504) for the same condition, so whether a
// client saw a timeout depended on which layer happened to notice the expiry
// first. A 503 additionally asserts that a dependency is unavailable, which is a
// different fact from "we ran out of time" and invites a different retry policy.
// Every deadline expiry in the issuance path now reports the same code, and the
// partial broker state — whether a credential may exist that the caller does not
// hold — is reported in the error DETAIL rather than through the status code,
// which could never have carried it.
//
// Anything else unclassified defaults to SUBSCRIBER_PROVISIONING_FAILED for the
// same reason: at this point the request has already passed the gate, the
// parameter and the lookup, so a failure without a code is a dependency failure
// rather than a client mistake.
//
// NO CREDENTIAL MATERIAL REACHES ANY BRANCH BELOW. The service returns a
// zero-valued credential on every error path, and no message or details payload
// assembled here reads from one.
func respondCredentialIssuanceError(c *gin.Context, issuance context.Context, err error) {
	if isTypedAPIError(err) {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrSubscriberNotFound))

		return
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(issuance.Err(), context.DeadlineExceeded) {
		respondCode(c, apierror.ErrSubscriberProvisioningTimeout, credentialIssuanceTimedOutMessage, nil)

		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(issuance.Err(), context.Canceled) {
		respondCode(c, apierror.ErrSubscriberProvisioningTimeout, credentialIssuanceCancelledMessage, nil)

		return
	}

	respondError(c, err,
		withUpgrade(apierror.ErrGenNotFound, apierror.ErrSubscriberNotFound),
		withDefault(apierror.ErrSubscriberProvisioningFailed),
	)
}

// issueWithinBudget runs a credential issuance and STOPS WAITING when the budget
// elapses, so the HTTP request cannot outlive its own ceiling.
//
// # The five-second guarantee, and what used to break it
//
// AAP R-7 requires credential provisioning to complete within five seconds, and the
// handler bounds the service's context to exactly that. That bound was not enough on
// its own, and the reason is worth stating precisely because it is the opposite of a
// missing timeout.
//
// The service's compensating writes — revoking a credential the registry could not
// record, clearing a reference that describes nothing, releasing the provisioning
// fence — deliberately run on a FRESH bounded context detached from the caller's,
// because the expired issuance deadline is one of the commonest reasons a cleanup is
// needed at all and a cleanup attempted on an already-cancelled context does
// nothing. That is right, and it must stay. But those fresh budgets are the same
// five seconds, and a synchronous call chain of issuance, then compensation, then a
// deferred fence release can therefore take fifteen seconds of wall clock before the
// handler returns. Every one of the three is bounded and the request still is not.
//
// # Why abandoning is safe, and why waiting is not the alternative
//
// The two requirements are not in tension once the request and the work are
// separated. The work keeps its own budgets and finishes what it owes; the REQUEST
// answers on time.
//
// Abandoning costs nothing that waiting would have saved. The abandoned attempt
// observes the very context that expired, so it takes its own failure path and
// compensates on its detached budget exactly as it would have done with the caller
// still waiting — a credential it had already written at the broker is revoked
// rather than left live, and the fence is released. Nothing is left behind that
// waiting would have prevented; the caller simply learns sooner.
//
// A password is never lost in a way that matters either. Kafka holds ONE SCRAM
// credential per principal, so re-issuing replaces whatever the abandoned attempt
// left, and the caller is told to re-issue. The alternative — waiting up to fifteen
// seconds to return a secret — fails the requirement outright.
//
// # The response is written by the caller, never here
//
// The goroutine touches no gin state and the channel is buffered to one, so it can
// always send and exit even after this function has returned; writing to a
// *gin.Context after its handler has returned is a data race against a response
// that may already be flushed. This function returns a plain result and lets the
// handler own every byte of the response.
//
// Parameters:
//   - ctx context.Context: the budgeted issuance context. Its expiry is the ceiling.
//   - subscriberID string: the subscriber to issue for.
//   - issue func: the service call, taken as a parameter so the ceiling is testable
//     without a broker.
//
// Returns:
//   - blnk.SubscriberCredential: the credential, zero-valued unless completed is
//     true and err is nil.
//   - error: the issuance failure, if any.
//   - bool: false when the budget elapsed first and the result was abandoned. The
//     credential and the error are both meaningless then.
func issueWithinBudget(
	ctx context.Context,
	subscriberID string,
	issue func(context.Context, string) (blnk.SubscriberCredential, error),
) (blnk.SubscriberCredential, error, bool) {
	type outcome struct {
		credential blnk.SubscriberCredential
		err        error
	}

	// Buffered, so the goroutine never blocks on a send this function has stopped
	// receiving. An unbuffered channel here would leak the goroutine — and with it
	// the secret it holds — for as long as the send blocked.
	results := make(chan outcome, 1)

	go func() {
		credential, err := issue(ctx, subscriberID)
		results <- outcome{credential: credential, err: err}
	}()

	select {
	case result := <-results:
		return result.credential, result.err, true
	case <-ctx.Done():
		// Already elapsed or cancelled. The service is still running and will observe
		// this same context; see above for why that is the safe outcome.
		return blnk.SubscriberCredential{}, nil, false
	}
}

// subscriberTopicPrefix reports the configured Kafka topic namespace, which the
// request DTOs need in order to tell a Blnk-owned topic from any other name.
//
// It is read live from the configuration store on every request rather than
// captured once, because the store's contents are replaced wholesale and a value
// captured at construction time would describe the deployment as it stood when
// the router was assembled. A blank result is a legitimate answer and makes the
// DTOs validate against model.DefaultEventTopicPrefix, which is the strictest
// available namespace rather than a permissive one.
func (a *Api) subscriberTopicPrefix() string {
	if configuration := a.blnk.Config(); configuration != nil {
		return configuration.Kafka.TopicPrefix
	}

	return ""
}

// ---------------------------------------------------------------------------------------
// Registry CRUD
// ---------------------------------------------------------------------------------------

// CreateSubscriber serves POST /subscribers: it registers a Kafka subscriber.
//
// Registration DESCRIBES an access boundary; it does not grant one. No broker is
// touched, no credential is minted and no ACL is bound, so a freshly registered
// subscriber can read nothing at all until POST
// /subscribers/:subscriber_id/kafka-credentials is called for it. That is
// fail-closed by construction rather than by a check.
//
// The Kafka principal and the consumer group are DERIVED from the subscriber
// identifier by the root package and cannot be supplied: the request DTO has no
// field for either. Both are the values isolation rests on — the principal is what
// every ACL binding is granted to, and the consumer group is granted with a
// prefixed pattern that reserves a namespace — so a caller able to name them could
// have its own topics added to another subscriber's grant or take another
// subscriber's partition assignments.
//
// The identifier itself is optional. Omitting it has the service mint one, which
// is the convention every other Blnk resource follows and which guarantees the
// canonical form the derivation requires. A value that IS supplied must already be
// canonical and is refused rather than folded, because Kafka principals are
// compared byte for byte and folding case would merge two broker identities onto
// one credential.
//
// # Responses
//
//	201 the registered subscriber, carrying no credential material
//	400 GEN_MALFORMED_REQUEST for an unbindable body — malformed JSON, a field of
//	    the wrong JSON type, an absent body, more than one JSON value, or a binding
//	    tag the body violates
//	400 GEN_VALIDATION_ERROR for a field this shape does not declare, and for a body
//	    the DTO refuses — a missing or blank name, a non-canonical identifier, a topic
//	    outside the deployment's grantable set, an unusable key scope or an
//	    unacceptable legacy URL. An UNKNOWN FIELD is refused rather than dropped: a
//	    misspelled authorized_topics would otherwise register a subscriber authorised
//	    for nothing, with a 201 saying it worked. See bindStrictManagementJSON
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	409 GEN_CONFLICT when the identifier or the derived principal is already taken
func (a *Api) CreateSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	var req model.CreateSubscriber
	if !bindStrictManagementJSON(c, &req) {
		return
	}

	// The DTO owns every rule the binding tags cannot express, and it is given the
	// live prefix so that "is this topic ours?" is answered against this
	// deployment's namespace rather than a default. respondCode is used rather than
	// respondError because a DTO refusal is a KNOWN condition: routing it through
	// the message-pattern table would risk a validation message being classified
	// as some unrelated domain's error.
	if err := req.Validate(a.subscriberTopicPrefix()); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	subscriber, err := a.blnk.RegisterEventSubscriber(c.Request.Context(), blnk.SubscriberRegistration{
		SubscriberID:     req.SubscriberID,
		Name:             req.Name,
		AuthorizedTopics: req.AuthorizedTopics,
		// The nullable column takes the nilable form, so "not recorded" stays
		// distinguishable from "recorded as empty" all the way to the row.
		//
		// WebhookURL is deliberately NOT set from the request: this route accepts no
		// webhook_url, because it is not fronted by the sunset guard and would otherwise
		// be a way to keep writing legacy webhook state after the guarded routes had
		// begun answering 410 Gone. A newly registered subscriber therefore carries no
		// legacy URL until one is recorded through
		// POST /subscribers/:subscriber_id/webhook-subscription.
		PartitionKeyPrefix: optionalSubscriberField(req.PartitionKeyPrefix),
	})
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondSubscriber(c, http.StatusCreated, subscriber)
}

// ListSubscribers serves GET /subscribers: the paged registry an operator answers
// "who is this principal?" and "who has not migrated yet?" from.
//
// # ONE RESPONSE SHAPE, FOR ALL THREE READINGS
//
// This route has three readings — the ordinary keyset page, the `subscriber_id_hash` resolution
// and the `revocation_pending=true` scan — and every one of them answers with the
// SubscriberPageResponse envelope: `{data, next_cursor, has_more, total_count?}`. A client reads
// `.data[]` whichever it asked for.
//
// Two of them used to answer with a bare JSON array. That is a breaking difference rather than a
// cosmetic one: a client written against the page reading fails on the resolver with a type error
// rather than a message, and the two bare-array readings are precisely the ones an operator
// reaches from a runbook DURING AN INCIDENT, where a deserialisation failure is the most expensive
// thing that can happen.
//
// The two non-paging readings return COMPLETE results — the resolver at most one row, the scan the
// whole registry — so they report `has_more: false`, emit no cursor, and always carry
// `total_count`, which for a complete result is simply its length. They also REFUSE `limit` and
// `cursor` rather than ignoring them: see refuseSubscriberPagingOptions.
//
// # Paging, ordering and counting
//
// limit follows this package's uniform convention: absent, non-positive or oversized values
// become 20, and a value that is not an integer at all is refused rather than silently
// defaulted. PAGING IS BY CURSOR (PERF-P08): the response carries next_cursor and a caller
// hands it back as ?cursor= for the following page. An offset used to be accepted at any depth
// and passed straight into an OFFSET clause, whose cost grows with that depth — so the page
// limit bounded the response while bounding no work — and an offset also repeats and skips rows
// when a subscriber is registered while a caller pages.
//
// Ordering is FIXED by the repository at newest registration first, ties broken by descending
// id, which is what makes the cursor stable; sort_by and sort_order are accepted for client
// compatibility and do not change it.
//
// include_count is HONOURED, and it ADDS total_count to the response rather than changing
// its shape: the body is the {data, next_cursor, has_more} envelope either way, because
// cursor paging has to return the cursor somewhere. A client reads `.data[]`
// unconditionally. This comment used to say that the body was a bare array without
// include_count, which described a response the code has not produced since paging moved
// off the offset.
//
// The total used to be refused outright, because no layer could count the registry. The
// repository now does, with a query rather than the length of the page — reporting a page
// length as a total would be a falsehood a paging client loops on forever — and it is read
// from the SAME snapshot as the page, so the two describe one registry rather than merely
// sharing a (nonexistent) filter.
//
// The value is parsed strictly: "1", "t", "true", "TRUE", "True" and their false
// counterparts are accepted, an absent or empty parameter means false, and anything
// else is a validation error rather than a silent false. A misspelling that changes
// the response shape without saying so is worse than one that is refused.
//
// The parameter set is CLOSED. An unrecognised name is refused, because a narrowing
// a caller asked for and did not get hands them a result that answers a different
// question with nothing to say so.
//
// An empty registry is 200 with `"data": []` — never 404, and `data` is never `null`, so a
// migration-progress script can range over `.data[]` unconditionally. Every entry
// is projected by model.NewSubscriberResponse, so none carries a credential
// reference, only its fingerprint.
//
// # Responses
//
//	200 {"data":[...], "next_cursor":"...", "has_more":bool}, plus "total_count":N
//	    when include_count is true. data may be empty; it is never null.
//	400 GEN_VALIDATION_ERROR for a non-integer page parameter, an unparseable
//	    include_count, or an unsupported query parameter
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) ListSubscribers(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	// The accepted set is CLOSED. A misspelled or invented parameter is refused rather
	// than ignored, because a narrowing silently dropped hands the caller a result that
	// answers a different question with nothing to say so.
	if !rejectUnsupportedQueryParameters(c, subscriberListQueryParameters) {
		return
	}

	// THE PSEUDONYM RESOLVER, offered on the LIST route rather than as a route of its own.
	//
	// A subscriber_id_hash is what an operator holds after reading a consumer-lag alert or a log
	// line — a raw subscriber id must never become a metric label or a log field, so the hash is
	// what those surfaces carry — and resolving it is a lookup that returns at most one
	// subscriber. It answers here as a narrowed collection rather than as a singular resource
	// because the token is NOT an addressable identity: it is derived, it may resolve to nothing,
	// and a route shaped /subscribers/hash/:token would imply otherwise. A caller therefore
	// receives an empty page for an unknown token and a one-element page for a known one, which
	// is what a filter means everywhere else in this package.
	//
	// It short-circuits the page readers below deliberately: a limit and a cursor describe a walk
	// through the registry, and a resolution is not a walk the caller is performing.
	if pseudonym, resolving := subscriberPseudonymFromQuery(c); resolving {
		if !refuseSubscriberPagingOptions(c, subscriberQueryParamPseudonym) {
			return
		}

		a.resolveSubscriberPseudonym(c, pseudonym)

		return
	}

	// THE REVOCATION SCAN, on the same route and for the same reason: it narrows the collection
	// rather than addressing a resource. It is the first step of the runbook for an outstanding
	// revocation, which the alert itself cannot attribute to a subscriber because a label would
	// export a tenant identifier into every notification — so without this the alert names a
	// count and nothing an operator can act on.
	if pending, present := subscriberRevocationFilterFromQuery(c); present && pending {
		if !refuseSubscriberPagingOptions(c, subscriberQueryParamRevocationPending) {
			return
		}

		a.listSubscribersAwaitingRevocation(c)

		return
	}

	limit, ok := subscriberPageLimitFromQuery(c)
	if !ok {
		return
	}

	cursor, ok := subscriberCursorFromQuery(c)
	if !ok {
		return
	}

	// Parsed explicitly rather than through ParseQueryOptions, which reads only the
	// lowercase "true" and would silently drop "TRUE" or "1" — changing the response
	// SHAPE without saying so. See includeCountFromQuery.
	includeCount, ok := includeCountFromQuery(c)
	if !ok {
		return
	}

	// ONE READ WHEN A TOTAL WAS ASKED FOR, TWO OTHERWISE.
	//
	// The page and the total used to be two independent calls, and the response asserted that
	// they described "the same set by construction" — which identical predicates cannot make
	// true across two snapshots. A subscriber registered or deregistered between them is counted
	// by one read and absent from the other. The combined read draws both from one read-only
	// REPEATABLE READ snapshot; a caller that did not ask for a total still takes the cheaper
	// single-statement path, because there is then no second answer to be coherent with.
	pageQuery := coremodel.SubscriberPageQuery{Limit: limit, Cursor: cursor}

	var (
		page  coremodel.SubscriberPage
		total int64
		err   error
	)

	if includeCount {
		page, total, err = a.blnk.ListAndCountEventSubscribers(c.Request.Context(), pageQuery)
	} else {
		page, err = a.blnk.ListEventSubscribers(c.Request.Context(), pageQuery)
	}

	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	// Allocated with make so an empty page marshals as [] rather than null.
	items := make([]model.SubscriberResponse, 0, len(page.Subscribers))
	for _, subscriber := range page.Subscribers {
		items = append(items, subscriberResponse(subscriber))
	}

	// The page and the position to resume from. A caller pages by handing next_cursor back
	// verbatim, and its absence is the termination condition — one fewer request than paging
	// until an empty page, and unlike an offset it cannot repeat or skip a row when a
	// subscriber is registered mid-pagination.
	response := SubscriberPageResponse{Data: items, HasMore: page.HasMore}
	if page.NextCursor != nil {
		response.NextCursor = page.NextCursor.Encode()
	}

	if includeCount {
		// An AGGREGATE over the registry rather than the length of this page: a paging client that
		// reads the page length as the total stops after one full page, or compares a number
		// against itself for ever. It was read in the same snapshot as the page above, so the two
		// describe one registry — see SubscriberPageResponse.TotalCount for what that does and
		// does not promise.
		response.TotalCount = &total
	}

	c.JSON(http.StatusOK, response)
}

// SubscriberPageResponse is the body GET /subscribers returns.
//
// It replaced a bare JSON array when paging moved from an offset to a cursor (PERF-P08): the
// cursor has to be returned somewhere, and an envelope is the only place a page can carry the
// position to resume from without inventing a header.
type SubscriberPageResponse struct {
	// Data is the page, newest registration first. Never null.
	Data []model.SubscriberResponse `json:"data"`

	// NextCursor is the opaque token that requests the following page. Absent on the last
	// page.
	NextCursor string `json:"next_cursor,omitempty"`

	// HasMore mirrors the presence of NextCursor, so a client can branch on a boolean
	// without inspecting the token.
	HasMore bool `json:"has_more"`

	// TotalCount is how many subscribers the registry holds, present only when the caller
	// asked for it with include_count. A pointer so that "not requested" and "zero" are
	// distinguishable rather than both rendering as 0.
	//
	// It is read from the SAME snapshot as data, so the total and THIS page describe one
	// registry. That pairing used to be asserted rather than delivered — the two were separate
	// reads on separate connections, and a registration between them left the total describing a
	// set the page was not a slice of.
	//
	// It is not a promise about a paging SESSION: paging is many requests over a live registry, so
	// a later page may hold subscribers this total did not count. Read it as "the registry size,
	// as at this page".
	TotalCount *int64 `json:"total_count,omitempty"`
}

// GetSubscriber serves GET /subscribers/:subscriber_id: one registry row.
//
// The response carries the credential FINGERPRINT and the issuance instant, never
// the stored reference and never a secret — a nil issuance instant is the reliable
// test for "registered, but no credential has ever been issued", which is a real
// state the registry has to represent. There is no route, here or anywhere, that
// returns a password: only a non-reversible reference is persisted, so a lost
// password can be replaced but never read back.
//
// # Responses
//
//	200 the subscriber
func (a *Api) GetSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	subscriber, err := a.blnk.GetEventSubscriber(c.Request.Context(), subscriberID)
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondSubscriber(c, http.StatusOK, subscriber)
}

// UpdateSubscriber serves PUT /subscribers/:subscriber_id: it applies the mutable
// subset of a subscriber.
//
// # This CHANGES ACCESS, not merely the record of it
//
// Replacing authorized_topics is reconciled at the broker as part of the update:
// the service removes the bindings the new authorization no longer implies BEFORE
// persisting the row, and creates the ones it adds AFTER. Every partial failure is
// therefore fail-closed — the subscriber ends with less access than the registry
// records, never more — and re-running the same request completes it. A widening
// that could not be applied is reported as a provisioning failure rather than
// being reported as an update that succeeded.
//
// Every field is optional, and each is a pointer or a nilable slice so that
// "omitted" stays distinguishable from "explicitly cleared". Omitted means leave
// as stored; a present empty string clears the key scope or the legacy URL; a
// present empty array revokes every topic grant. The principal, the consumer
// group, the credential record and the migration instant have no field at all —
// the first two are derived boundaries rather than attributes, and the last two
// are written only by the operations that own them, so a client cannot claim an
// issuance that never happened.
//
// # Responses
//
//	200 the subscriber as it now stands
//	400 GEN_MALFORMED_REQUEST for an unbindable body
//	400 GEN_VALIDATION_ERROR for a field this shape does not declare, or for a body
//	    the DTO refuses. An unknown field is refused rather than dropped: a body whose
//	    fields were all misspelled decodes to an empty update, which is a legitimate
//	    instruction, so the response was 200 with the row unchanged
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_ISOLATION_UNENFORCEABLE when the update would record a
//	    partition-key prefix on a subscriber that already holds a credential. Kafka
//	    has no message-key dimension, so the live credential reads whole topics and
//	    the row would describe something narrower — and re-issuance would be refused
//	    for the same reason, leaving the row unable to rotate its secret. Revoke the
//	    credential first, or narrow authorized_topics
//	409 GEN_CONFLICT when a concurrent issuance holds the provisioning fence
//	503 SUBSCRIBER_PROVISIONING_FAILED when the broker-side reconciliation did not
//	    complete
func (a *Api) UpdateSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	var req model.UpdateSubscriber
	if !bindStrictManagementJSON(c, &req) {
		return
	}

	if err := req.Validate(a.subscriberTopicPrefix()); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	// Passed through in their nilable form without normalisation, because the
	// nilability IS the instruction: collapsing a present empty string to nil here
	// would silently turn "clear this" into "leave it alone".
	// WebhookURL is left nil, which SubscriberUpdate reads as "leave as stored": this
	// route accepts no webhook_url, for the reason given on CreateSubscriber. The
	// guarded PUT /subscribers/:subscriber_id/webhook-subscription is the write path.
	subscriber, err := a.blnk.UpdateEventSubscriber(c.Request.Context(), subscriberID, blnk.SubscriberUpdate{
		Name:               req.Name,
		AuthorizedTopics:   req.AuthorizedTopics,
		PartitionKeyPrefix: req.PartitionKeyPrefix,
	})
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondSubscriber(c, http.StatusOK, subscriber)
}

// DeleteSubscriber serves DELETE /subscribers/:subscriber_id: it deregisters a
// subscriber and revokes its Kafka access.
//
// It removes the registry row AND the broker-side identity the row describes — the
// SCRAM credential and every ACL binding derived from it — because a deleted row
// whose principal still authenticates is worse than no deletion at all: the access
// survives with nothing left to describe or audit it. A revocation the broker
// could not confirm is reported rather than swallowed, so an operator is never
// told a subscriber is gone while its credential still works.
//
// This deletes the SUBSCRIBER. Removing only a subscriber's recorded legacy
// endpoint is DELETE /subscribers/:subscriber_id/webhook-subscription, which
// leaves the subscriber registered and consuming.
//
// 204 with no body follows the RevokeAPIKey precedent in this package: the caller
// asked for the resource to be gone and there is nothing meaningful left to
// return.
//
// # Responses
//
//	204 the subscriber is deregistered and its Kafka access revoked
//	409 SUBSCRIBER_DEPROVISIONING or GEN_CONFLICT when a concurrent operation holds
//	    the provisioning fence
//	503 SUBSCRIBER_PROVISIONING_FAILED when the broker-side revocation did not
//	    complete
func (a *Api) DeleteSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	// The removed row is returned by the service and deliberately dropped: 204
	// carries no body, and echoing a resource the caller just deleted would invite
	// a client to treat it as still current.
	if _, err := a.blnk.DeregisterEventSubscriber(c.Request.Context(), subscriberID); err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	c.Status(http.StatusNoContent)
}

// ---------------------------------------------------------------------------------------
// Credential issuance
// ---------------------------------------------------------------------------------------

// IssueKafkaCredentials serves POST /subscribers/:subscriber_id/kafka-credentials:
// it mints the subscriber's SASL/SCRAM credential and hands back everything needed
// to start consuming — the subscriber-facing broker endpoint, the authorised topic
// list, the consumer group and the credential itself.
//
// # THE PASSWORD IS RETURNED EXACTLY ONCE
//
// The plaintext appears in one expression in this file: the response literal
// below. It is never logged, never interpolated into a message, never placed in an
// apierror details payload, and no other endpoint anywhere returns it. Only a
// non-reversible reference and the issuance instant are persisted, and
// blnk.event_subscribers has no column able to hold the secret, so the value
// cannot be read back by any route — including this one re-called, which mints a
// NEW credential rather than returning the old one. A lost password can be
// replaced, never recovered.
//
// Re-issuing is supported and DESTRUCTIVE to the previous secret: Kafka stores one
// SCRAM credential per principal, so the upsert replaces it and a consumer still
// using the old password fails at its next handshake. The subscriber id, the
// principal and the consumer group are unchanged, all being derived from the
// immutable identifier.
//
// # The deployment contract this endpoint requires before it will answer
//
// Three things must hold, and only the first two are about the caller:
//
//   - SECURE MODE (BLNK_SERVER_SECURE) with a MASTER KEY (BLNK_SERVER_SECRET_KEY,
//     from a secret rather than a manifest literal). With secure mode off the auth
//     middleware skips authentication entirely and never marks a caller as master,
//     so this endpoint answers 403 to everybody — unreachable rather than open,
//     but unreachable for a reason worth knowing when a deployment is being
//     assembled.
//   - THE MASTER KEY on the request, which is what ensureSubscriberManagementAuthorized
//     checks.
//   - A CONFIDENTIAL CHANNEL, which is what ensureCredentialTransportConfidential
//     checks: TLS terminated in-process, or a declared proxy boundary reporting
//     https, or a loopback peer. Authentication says who is asking; it says nothing
//     about who else can read the answer, and this response is the one place in
//     Blnk where the answer is a secret.
//
// The last is enforced in code precisely because the first two can be satisfied by
// a deployment that still carries the password over a plaintext hop — which is the
// normal ingress shape unless somebody thought about it.
//
// # The five-second budget is enforced here, in code
//
// The service applies the same budget internally, and this handler applies it again
// at the boundary so the guarantee holds for the HTTP request whatever a future
// service refactor does with its own timeout. context.WithTimeout only ever
// shortens, so the two cannot conflict and a caller whose own context expires
// sooner still wins. blnk.SubscriberCredentialIssuanceBudget is used rather than a
// literal duration so the endpoint's ceiling cannot drift from the operation's.
//
// SLA-01: THE BUDGET COVERS THE COMPENSATION TOO. It once covered only the work,
// and the compensation that follows a failure then ran on budgets of its own — a
// ten-second broker cleanup, a five-second registry cleanup and a five-second fence
// release — so a five-second promise could answer in roughly twenty. The service
// now divides ONE absolute instant into three phases (work, compensation, durable
// record), so a failed issuance still answers inside the budget AND still finishes
// what it owes; see blnk.subscriberPhaseContext. The context this handler installs
// is what the service adopts as that instant, since it is the sooner of the two.
//
// An expiry is answered, not waited out: see respondCredentialIssuanceError. The
// request therefore always ends within the budget and always ends with a code that
// says whether retrying is sensible — never a hang and never a bare 500.
//
// # The budget bounds the RESPONSE, including on the failure paths (PERF-P09)
//
// That guarantee used to hold for the operation and not for the request. Every
// cleanup the operation owed — releasing the provisioning fence, revoking a
// credential the registry could not record, clearing a record that no longer
// described anything — ran synchronously afterwards on a fresh budget of its own,
// so a five-second endpoint answered in ten on success and could take considerably
// longer when it failed. Those cleanups are now scheduled off the response path by
// the service, which is why this ceiling is now a statement about what a caller
// waits for rather than about what the service starts.
//
// A failure whose broker-side compensation has been scheduled but not yet confirmed
// reports `compensation_pending` in its detail. It means retry: the subscriber stays
// fenced until the cleanup finishes, so an immediate retry is refused with a
// conflict rather than racing it.
//
// # The boundary the response describes, and it has TWO shapes
//
// For a subscriber with NO partition-key prefix the credential grants topic-level
// Read and Describe on the authorised topics plus Read on its prefixed
// consumer-group namespace, and nothing further. Within an authorised topic there
// is no restriction: Kafka authorises at topic and group granularity and has no
// message-key dimension, so such a subscriber reads every record on a topic it is
// granted — including records written for other ledgers on that topic.
//
// For a subscriber that RECORDS a prefix the credential grants Describe but NOT
// Read on those topics, so the broker refuses every record fetch it attempts, and
// its records are delivered by GET /subscribers/{subscriber_id}/events, which
// applies the prefix to each record's key before returning it. The
// enforced-access declaration — assembled by model.NewSubscriberEnforcedAccess,
// never here — reports that as gateway_delivery_required, broker_record_access
// false and partition_key_prefix_enforced_by "blnk_stream_gateway", so a client
// learns where to consume from in the same body that carries the secret.
//
// A SUBSCRIBER RECORDING A PREFIX IS STILL PROVISIONED. This endpoint used to
// refuse it with 409, which handed back no credential at all — not a narrower
// boundary but an absent one, and a subscriber that consumes nothing is not
// isolated. It then issued whole-topic Read and declared the prefix the client's
// own filter, which is the exposure the isolation correction closed: cooperation
// is not an authorization boundary. Dead-letter topics are never grantable, so no
// "<topic>.dlt" can appear in the list.
//
// Provisioning VERIFIES the narrower shape before the secret becomes returnable —
// no topic Read among the reconciled bindings, ACL enforcement confirmed, no
// foreign ALLOW binding — and answers 409
// SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION, having revoked the credential it wrote,
// when it cannot.
//
// # Responses
//
//	200 the credential and the connection details
//	400 GEN_VALIDATION_ERROR for an identifier no Kafka identity can be derived from
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller,
//	    SUBSCRIBER_INSECURE_TRANSPORT when the channel is not established as
//	    confidential
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_GRANT_EMPTY for a subscriber authorised for no topics,
//	    SUBSCRIBER_ISOLATION_UNENFORCEABLE for a subscriber recording a
//	    partition-key prefix, which Kafka cannot enforce,
//	    GEN_CONFLICT when a concurrent issuance superseded this one
//	503 EVENT_KAFKA_UNAVAILABLE when no broker is configured or reachable,
//	    SUBSCRIBER_BROKERS_NOT_CONFIGURED when no subscriber-facing endpoint is
//	    published, SUBSCRIBER_PROVISIONING_FAILED when the broker REFUSED the
//	    credential or its bindings
//	504 SUBSCRIBER_PROVISIONING_TIMEOUT for EVERY deadline expiry or cancellation,
//	    whichever dependency consumed the budget — the broker, the registry, or the
//	    caller going away. Whether a credential may already exist at the broker that
//	    the caller does not hold is reported in the error detail, not the code.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) IssueKafkaCredentials(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	// THE IDENTIFIER FIRST, and it is refused in memory before anything else is judged. A
	// value no subscriber id can ever equal — "sub_ABC!", "*", "a" — gets the 400 that says
	// "that is not an identifier" on THIS route and on GET /subscribers/:id alike, which is the
	// point: both read the parameter through the same helper, so a caller cannot be told two
	// different things about one malformed value.
	//
	// It used to be checked after the transport gate below, which meant a developer working over
	// plain HTTP was told about the transport, fixed that, and only then learned the id was
	// malformed. Nothing about the gate's purpose depends on the order — a 400 written here
	// carries no credential, so there is nothing to leak — and the cheaper, more specific answer
	// belongs first.
	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	// THE TRANSPORT, checked after the caller and the identifier and before anything is minted. A
	// correct master key over a channel nobody has established as confidential is an authorised
	// credential leak, and the refusal has to happen before the broker is touched: a password
	// refused after issuance would already exist at the broker, unusable by the caller that never
	// received it and still requiring revocation.
	if !a.ensureCredentialTransportConfidential(c) {
		return
	}

	// THE BUDGET. Everything the service does under it — the lookup, the
	// provisioning fence, up to four broker round trips and the issuance record —
	// shares this one deadline.
	ctx, cancel := context.WithTimeout(c.Request.Context(), blnk.SubscriberCredentialIssuanceBudget)
	defer cancel()

	credential, err, completed := issueWithinBudget(ctx, subscriberID, a.blnk.IssueSubscriberKafkaCredentials)
	if !completed {
		// THE CEILING FIRED. The service is still working and this request stops
		// waiting for it — see issueWithinBudget for why waiting is not an option and
		// why abandoning is safe.
		logrus.WithFields(logrus.Fields{
			// HASHED, LIKE EVERY OTHER SUBSCRIBER IDENTIFIER IN A LOG LINE. The value is safe in
			// the injection sense — subscriberIDFromRoute has already refused anything
			// model.CanonicalizeSubscriberIdentifier would not accept, so it holds no whitespace
			// and no control characters — but that was never the reason the rest of this codebase
			// hashes it. A subscriber id is a TENANT-CHOSEN name that reaches logs, alert
			// annotations and incident tickets, so every other layer emits the keyed digest under
			// a _hash key and this one line emitted the name itself: enough to make the pseudonyms
			// everywhere else resolvable by anyone reading the same stream, and it was the field
			// that failed most visibly, on a timeout an operator goes looking for.
			//
			// logsafe.Identifier is the shared rule, so the token printed here is the same token
			// the service layer, the repository and the consumer-lag sweep print for this
			// subscriber — which is what makes the lines correlatable at all.
			"subscriber_id_hash": logsafe.Identifier(subscriberID),
			"budget":             blnk.SubscriberCredentialIssuanceBudget.String(),
		}).Warn(
			"subscribers api: credential issuance exceeded the issuance budget and the request was " +
				"answered without waiting for it. The abandoned attempt observes the same expired " +
				"context and compensates on its own detached budget, so a credential it had already " +
				"written is revoked rather than left live; re-issuing is safe either way",
		)

		respondCode(c, apierror.ErrSubscriberProvisioningTimeout, credentialIssuanceAbandonedMessage, nil)

		return
	}

	if err != nil {
		respondCredentialIssuanceError(c, ctx, err)

		return
	}

	// Normalised once and used for both the top-level grant and the enforced-access
	// declaration, so neither can report null for a set the subscriber has to read.
	// Issuance refuses an empty grant outright, so a successful response always
	// names at least one topic.
	authorizedTopics := credential.AuthorizedTopics
	if authorizedTopics == nil {
		authorizedTopics = []string{}
	}

	c.JSON(http.StatusOK, model.KafkaCredentialsResponse{
		Brokers:          credential.Brokers,
		BrokerEndpoint:   credential.BrokerEndpoint,
		AuthorizedTopics: authorizedTopics,
		ConsumerGroupID:  credential.ConsumerGroupID,
		// Derived in api/model from the subscriber id, so the namespace reported is
		// necessarily the one the broker actually reserved.
		//
		// The VERIFIED form, and only issuance may use it: provisioning read this
		// principal's complete ACL grant at the broker and refused to return a
		// password while any ALLOW binding sat outside the declared set, so
		// exclusive_grant_verified here is an observation rather than a hope. A
		// subscriber read uses the unverified form, because it makes no broker
		// round trip.
		// THE ENFORCEMENT POINT COMES FROM THE ISSUANCE, not from the prefix. The service read
		// the deployment's key-scope enforcement mode to decide whether to mint this credential
		// at all — a recorded prefix with nothing enforcing it is refused — so the value it
		// recorded is the only one that can describe what was actually issued. Deriving it here
		// from the prefix would report consumer_side for a credential minted under a gateway.
		EnforcedAccess: model.NewVerifiedSubscriberEnforcedAccessUnder(
			credential.SubscriberID, authorizedTopics, credential.PartitionKeyPrefix,
			credential.KeyScopeEnforcement,
		),
		Username: credential.Username,
		// THE ONE AND ONLY READ OF THE PLAINTEXT SECRET IN THIS PACKAGE. It goes
		// straight into the response body and is not held, copied or logged.
		Password:  credential.Password(),
		Mechanism: credential.Mechanism,
		IssuedAt:  credential.IssuedAt,
		// THE TWO FIELDS THE SERVICE ESTABLISHED AND THE RESPONSE USED TO DROP.
		//
		// The fingerprint is the only handle a client has on an issuance afterwards, since the
		// password is returned once and nothing persists it — it is the same value a subscriber
		// read reports, so the two become comparable. Replaced is the destructive-action
		// confirmation: Kafka stores one credential per principal, so a true here means a live
		// consumer's password has just stopped working, and that is not something to learn from a
		// support ticket.
		CredentialFingerprint: credential.Fingerprint,
		Replaced:              credential.Replaced,
	})
}

// StreamSubscriberEvents serves one page of a subscriber's own event stream, applying its
// partition-key scope.
//
// GET /subscribers/:subscriber_id/events
//
// # Why this endpoint exists, and why it is the only non-management route in this file
//
// It is the ENFORCEMENT POINT for requirement R-7's partition-key dimension. Kafka's
// authorizer has no message-key resource, so a subscriber confined to a key prefix cannot be
// given a broker grant that expresses it; Blnk grants such a subscriber Describe and NO Read —
// closing the broker path outright — and serves its records here, filtered per record.
//
// Every other route in this file is an OPERATOR action gated on the master key. This one is a
// SUBSCRIBER action, so it is authenticated differently: the caller presents the subscriber's
// own SASL secret in X-Blnk-Subscriber-Secret and the gateway compares it, in constant time,
// against the credential reference the registry recorded. The deployment's own API
// authentication still applies on top through the standard middleware — this is an additional
// factor, not a replacement for one — which is why an operator cannot read a subscriber's
// stream without holding that subscriber's secret.
//
// # Responses
//
//	200 with the page, `records: []` when the subscriber is caught up
//	400 GEN_VALIDATION for a malformed identifier, partition, offset or wait,
//	    GEN_MISSING_PARAMETER when no topic is named
//	401 SUBSCRIBER_CREDENTIAL_INVALID for every authentication failure, indistinguishably:
//	    no such subscriber, no credential issued, the wrong principal, the wrong secret, or a
//	    credential pending revocation. Distinguishing them would let an unauthenticated caller
//	    enumerate the registry
//	403 SUBSCRIBER_TOPIC_NOT_GRANTED when the topic is outside the subscriber's grant or is
//	    one no subscriber may be granted, SUBSCRIBER_INSECURE_TRANSPORT when the channel is
//	    not one this deployment has established as confidential
//	503 EVENT_KAFKA_UNAVAILABLE when no broker is configured or the read failed
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) StreamSubscriberEvents(c *gin.Context) {
	// NO MASTER-KEY GATE, and its absence is deliberate rather than an omission: this is the
	// data plane, and the credential that authorises it is the SUBSCRIBER's. The middleware
	// chain has already required the deployment's own API authentication.
	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	if !rejectUnsupportedQueryParameters(c, subscriberStreamQueryParameters) {
		return
	}

	// THE TRANSPORT, before the secret is read. The caller is sending a live password in a
	// request header, so a channel nobody has established as confidential discloses it on the
	// way IN — the mirror of the issuance endpoint's concern, and the same gate answers it.
	if !a.ensureStreamTransportConfidential(c) {
		return
	}

	request, ok := subscriberStreamRequestFrom(c, subscriberID)
	if !ok {
		return
	}

	// THE BUDGET. It bounds the registry read, the SASL handshake on a cold transport and the
	// fetch — including the full max_wait a caller may have asked the broker to hold for.
	ctx, cancel := context.WithTimeout(c.Request.Context(), blnk.SubscriberStreamBudget)
	defer cancel()

	page, err := a.blnk.ReadSubscriberEventStream(ctx, request)
	if err != nil {
		respondError(c, err)

		return
	}

	// NO-STORE, ALWAYS. The body carries ledger event payloads belonging to one subscriber,
	// and a shared cache or a browser history holding them would disclose them to whoever
	// reads that cache next. It is set on the success path only because the error paths carry
	// no records — and respondError's own headers are its business.
	c.Header("Cache-Control", "no-store, no-cache, must-revalidate, private")
	c.Header("Pragma", "no-cache")

	c.JSON(http.StatusOK, subscriberEventStreamResponse(page))
}

// subscriberEventStreamResponse projects a gateway page onto the wire shape.
//
// The record slice is allocated rather than left nil, so a caught-up subscriber receives `[]`
// and not `null`: an empty page is the ordinary steady state of a healthy consumer, and every
// client would otherwise have to special-case it.
//
// Parameters:
//   - page blnk.SubscriberStreamPage: the gateway's answer.
//
// Returns:
//   - model.SubscriberEventStreamResponse: the response body.
func subscriberEventStreamResponse(page blnk.SubscriberStreamPage) model.SubscriberEventStreamResponse {
	records := make([]model.SubscriberEventStreamRecord, 0, len(page.Records))
	for _, record := range page.Records {
		records = append(records, model.SubscriberEventStreamRecord{
			Offset:    record.Offset,
			Partition: record.Partition,
			Key:       record.Key,
			Timestamp: record.Timestamp,
			// THE BYTES AS THEY SIT ON THE TOPIC. json.RawMessage, so the envelope is
			// embedded rather than re-encoded — a re-marshal would reorder keys and rewrite
			// numbers, and a subscriber comparing this against a record read directly from
			// the topic would find two different payloads for one event.
			Event: record.Value,
		})
	}

	return model.SubscriberEventStreamResponse{
		SubscriberID:     page.SubscriberID,
		Topic:            page.Topic,
		Partition:        page.Partition,
		Records:          records,
		NextOffset:       page.NextOffset,
		HighWatermark:    page.HighWatermark,
		LogStartOffset:   page.LogStartOffset,
		RecordsScanned:   page.RecordsScanned,
		RecordsWithheld:  page.RecordsWithheld,
		Truncated:        page.Truncated,
		KeyScope:         page.KeyScope,
		KeyScopeEnforced: page.KeyScopeEnforced,
	}
}

// subscriberStreamRequestFrom reads the stream request out of the route, query and headers,
// writing the refusal itself when any of it is malformed.
//
// # Why the numeric parameters are refused rather than defaulted
//
// A page size or a wait that is out of range is CLAMPED by the gateway, because a caller
// asking for more than the ceiling is expressing a throughput preference and the response
// reports what was actually read. A value that is not a number at all is different: it is a
// client defect, and serving the default for `offset=abc` would silently re-read from
// wherever the default happens to point — which for a cursor is either duplicate processing
// or skipped events.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
//   - subscriberID string: the canonical identifier from the route.
//
// Returns:
//   - blnk.SubscriberStreamRequest: the request to hand the gateway.
//   - bool: false when the refusal has been written.
func subscriberStreamRequestFrom(
	c *gin.Context,
	subscriberID string,
) (blnk.SubscriberStreamRequest, bool) {
	request := blnk.SubscriberStreamRequest{
		SubscriberID: subscriberID,
		Topic:        strings.TrimSpace(c.Query(subscriberQueryParamTopic)),
		// THE SECRET, read from the header and passed straight through. It is not logged, not
		// echoed and not stored anywhere in this package.
		Secret:    strings.TrimSpace(c.GetHeader(headerSubscriberSecret)),
		Principal: strings.TrimSpace(c.GetHeader(headerSubscriberPrincipal)),
	}

	partition, ok := subscriberStreamIntFromQuery(c, subscriberQueryParamPartition, 0)
	if !ok {
		return request, false
	}
	request.Partition = int(partition)

	// ZERO BY DEFAULT — the earliest ABSOLUTE offset, not the latest. A client that omits its
	// cursor is served from the beginning of the retained log, which at worst re-delivers
	// events it can suppress by event_id; defaulting to the end would silently skip everything
	// currently on the partition, and a skipped event is not recoverable by idempotency.
	offset, ok := subscriberStreamIntFromQuery(c, subscriberQueryParamOffset, 0)
	if !ok {
		return request, false
	}
	request.Offset = offset

	limit, ok := subscriberStreamIntFromQuery(c, subscriberQueryParamLimit, 0)
	if !ok {
		return request, false
	}
	request.Limit = int(limit)

	maxWait, ok := subscriberStreamIntFromQuery(c, subscriberQueryParamMaxWait, 0)
	if !ok {
		return request, false
	}
	if maxWait > 0 {
		request.MaxWait = time.Duration(maxWait) * time.Millisecond
	}

	return request, true
}

// subscriberStreamIntFromQuery reads one integer query parameter, or its default when absent.
//
// It parses as int64 so that a partition or a page size beyond int range is refused as a
// malformed number rather than wrapping into a plausible small value on a 32-bit build.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written.
//   - name string: the parameter to read.
//   - fallback int64: the value an absent parameter takes.
//
// Returns:
//   - int64: the parsed value, or the fallback.
//   - bool: false when the refusal has been written.
func subscriberStreamIntFromQuery(c *gin.Context, name string, fallback int64) (int64, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return fallback, true
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q must be an integer, got %q", name, raw,
		), nil)

		return 0, false
	}

	return value, true
}

// ensureStreamTransportConfidential refuses a stream read whose channel this deployment has
// not established as confidential.
//
// It shares credentialTransportConfidential with the issuance endpoint — one predicate over
// the request and the live configuration, so the two routes cannot disagree about which
// channels are confidential — and differs only in the message, because the DIRECTION of the
// exposure differs. Issuance is refused because the RESPONSE carries a one-time password; this
// is refused because the REQUEST does, in a header, on every poll for as long as the
// subscriber runs. A stream read over plaintext therefore discloses a live credential
// repeatedly rather than once.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
//
// Returns:
//   - bool: true when the channel is confidential and the read may proceed.
func (a *Api) ensureStreamTransportConfidential(c *gin.Context) bool {
	if a.credentialTransportConfidential(c) {
		return true
	}

	respondCode(c, apierror.ErrSubscriberInsecureTransport, streamInsecureTransportMessage, nil)

	return false
}

// ---------------------------------------------------------------------------------------
// The deprecated legacy webhook-subscription surface
//
// # Why these four routes exist, so they are not removed as dead code
//
// They are MIGRATION BOOKKEEPING. Recording a subscriber's legacy webhook URL
// gives an already-webhooked subscriber somewhere to be recorded and migrated
// FROM, which makes migration progress a queryable fact rather than a spreadsheet.
// They are also what makes the retirement observable: without a subscription route
// there is no request on which a 410 could be seen, and the sunset criterion would
// have nothing to assert against.
//
// # What this surface does NOT do
//
// Recording a URL here causes nothing to be delivered to it. Blnk has one webhook
// destination and it is deployment-wide; the relay's legacy leg posts every event
// of the dual-delivery window to that one endpoint. The column is a MIGRATION
// RECORD. Per-subscriber HTTP fan-out is deliberately not built, because building
// new delivery into the transport being retired is the opposite of retiring it.
//
// # /hooks IS NOT THIS SURFACE
//
// api/hooks.go and internal/hooks implement the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts, whose responses can influence how a
// transaction is processed — synchronous interception, not asynchronous event
// notification. They stay fully functional after the retirement instant and no
// guard is attached to them. They also share infrastructure with the transport
// being retired: the hook manager enqueues its work onto the webhook asynq queue by
// name, which is why that queue outlives the transport. Conflating the two would
// break a live, supported feature that merely shares a word with this one.
//
// # NO SUNSET LOGIC IS PRESENT BELOW
//
// None of the four handlers reads the configured retirement instant, calls a
// sunset predicate or compares a date. api/api.go installs
// middleware.WebhookSunsetGuard globally and ahead of authentication, and it asks
// the single predicate in the root package. Before the retirement instant the guard
// is transparent and these handlers answer exactly as written; after it, they are
// unreachable and every request to this path is answered 410 GEN_GONE — including
// methods that were never registered and callers that never authenticated, neither
// of which could reach a per-route guard. One decision, two consumers — a second
// comparison here could let the service stop dual-writing while still accepting
// these calls, or the reverse, and neither failure raises anything to notice.
// ---------------------------------------------------------------------------------------

// RegisterWebhookSubscription serves POST
// /subscribers/:subscriber_id/webhook-subscription: it records the legacy HTTP
// endpoint a migrating subscriber used to receive pushes on.
//
// The URL is validated twice over — by the DTO here and again at the persistence
// boundary — because it is an externally supplied destination held inside Blnk's
// own network boundary, and a stored value that nothing validates is one
// misconfiguration away from being fetched. HTTPS is required and internal
// destinations are refused, so the column cannot be used to point Blnk at a
// metadata service or a private address. Nothing sends to it, and per-subscriber
// HTTP fan-out is deliberately not built.
//
// # Every status this route can answer, and the ones it no longer can
//
// The list below is exhaustive. It used to be shorter than the truth: this route
// went through the general subscriber update, which reconciles the subscriber's ACL
// bindings at the broker around its write, so a broker outage answered 503
// SUBSCRIBER_PROVISIONING_FAILED and a concurrent credential issuance holding the
// provisioning fence answered 409 — both for a request that only ever wanted to
// write one column of migration metadata, and neither documented.
//
// Recording a webhook URL changes no authorization, so there was never any grant to
// reconcile. This route now performs a REGISTRY WRITE and nothing else: no admin
// client is resolved, no fence is taken, no binding is touched, and neither of those
// statuses is reachable any more. An operator can record a URL during a Kafka
// incident, which is exactly when a migration record is most likely to be edited.
//
// # Responses
//
//	201 the recorded subscription. migrated_at is CLEARED by the same statement, so
//	    the subscriber is counted as awaiting migration again
//	400 GEN_MALFORMED_REQUEST for an unbindable body or a missing webhook_url
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a field this shape does not declare, for a
//	    non-canonical identifier, or for a URL the destination policy refuses
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
//
// NOT reachable: 409 and 503. No broker is contacted and no provisioning fence is
// taken, so neither a Kafka outage nor a concurrent issuance can fail this request.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window during which Kafka publishing and HTTP webhook delivery run
// side by side from the same outbox events. Once the configured retirement instant
// has passed, every request to these routes is answered 410 Gone by the sunset
// guard. Use the Kafka event stream and IssueKafkaCredentials instead; see
// docs/webhook-to-kafka-migration.md.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) RegisterWebhookSubscription(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	var req model.CreateWebhookSubscription
	if !bindStrictManagementJSON(c, &req) {
		return
	}

	if err := req.Validate(); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	subscriber, err := a.blnk.RecordSubscriberWebhookSubscription(
		c.Request.Context(), subscriberID, req.WebhookURL,
	)
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondWebhookSubscription(c, http.StatusCreated, subscriber)
}

// GetWebhookSubscription serves GET
// /subscribers/:subscriber_id/webhook-subscription: it reports the legacy endpoint
// recorded for a subscriber and, when it is set, the instant the subscriber
// finished moving to Kafka.
//
// A subscriber with no recorded endpoint is 200 with the URL omitted rather than
// 404: the subscriber exists and its subscription record is legitimately empty,
// which is the normal state both for one onboarded after the cutover and for one
// whose record has already been cleared. Only an unknown subscriber is 404.
//
// A nil migration instant means "still counted as awaiting migration", which is
// exactly what migration-progress reporting counts during the window.
//
// # Responses
//
//	200 the recorded subscription, possibly with no URL
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a non-canonical identifier
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
//
// NOT reachable: 409 and 503. This route only reads the registry row.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka
// event stream and IssueKafkaCredentials instead; see
// docs/webhook-to-kafka-migration.md.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) GetWebhookSubscription(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	subscriber, err := a.blnk.GetEventSubscriber(c.Request.Context(), subscriberID)
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondWebhookSubscription(c, http.StatusOK, subscriber)
}

// UpdateWebhookSubscription serves PUT
// /subscribers/:subscriber_id/webhook-subscription: it replaces the recorded
// legacy endpoint so a mis-recorded URL can be corrected without a delete and a
// re-create.
//
// The replacement is held to exactly the same destination policy as the original,
// because a URL that arrives by an edit is stored in the same column and read by
// the same readers. A policy applied only on creation is a policy with an
// edit-shaped hole.
//
// # Every status this route can answer, and the ones it no longer can
//
// The list below is exhaustive. It used to be shorter than the truth: this route
// went through the general subscriber update, which reconciles the subscriber's ACL
// bindings at the broker around its write, so a broker outage answered 503
// SUBSCRIBER_PROVISIONING_FAILED and a concurrent credential issuance holding the
// provisioning fence answered 409 — both for a request that only ever wanted to
// write one column of migration metadata, and neither documented.
//
// Recording a webhook URL changes no authorization, so there was never any grant to
// reconcile. This route now performs a REGISTRY WRITE and nothing else: no admin
// client is resolved, no fence is taken, no binding is touched, and neither of those
// statuses is reachable any more. An operator can record a URL during a Kafka
// incident, which is exactly when a migration record is most likely to be edited.
//
// # Responses
//
//	200 the recorded subscription as it now stands. migrated_at is CLEARED by the
//	    same statement, so the subscriber is counted as awaiting migration again
//	400 GEN_MALFORMED_REQUEST for an unbindable body or a missing webhook_url
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a field this shape does not declare, for a
//	    non-canonical identifier, or for a URL the destination policy refuses
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
//
// NOT reachable: 409 and 503. No broker is contacted and no provisioning fence is
// taken, so neither a Kafka outage nor a concurrent issuance can fail this request.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka
// event stream and IssueKafkaCredentials instead; see
// docs/webhook-to-kafka-migration.md.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) UpdateWebhookSubscription(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	var req model.UpdateWebhookSubscription
	if !bindStrictManagementJSON(c, &req) {
		return
	}

	if err := req.Validate(); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	// The same service operation the create route uses: recording a replacement and
	// recording a first value are the same write, and giving them two code paths
	// would be two places for the destination policy to be applied differently.
	subscriber, err := a.blnk.RecordSubscriberWebhookSubscription(
		c.Request.Context(), subscriberID, req.WebhookURL,
	)
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	respondWebhookSubscription(c, http.StatusOK, subscriber)
}

// DeleteWebhookSubscription serves DELETE
// /subscribers/:subscriber_id/webhook-subscription: it forgets the subscriber's
// recorded legacy endpoint and stamps the instant the subscriber completed its
// move to Kafka consumption.
//
// It deletes a WEBHOOK SUBSCRIPTION, not the subscriber. The subscriber stays
// registered, keeps its credential and goes on consuming; DELETE
// /subscribers/:subscriber_id is the separate operation that deregisters it.
//
// # ONE call, because it is one transition
//
// This handler used to make TWO: clear the URL, then stamp the instant. There is no
// transaction spanning two service calls, so between them the row was either
// migrated with a live URL or unmigrated with none — and a failure landing in that
// gap made the wrong state PERMANENT, with nothing to indicate that a row needed
// repairing. Choosing the ordering only decided WHICH wrong state was left behind.
//
// Worse, the clear went through the general subscriber update, which reconciles ACL
// bindings at the broker. A broker outage or a concurrent issuance holding the
// provisioning fence could therefore fail step one — for reasons with nothing to do
// with migration metadata — leaving exactly that residue.
//
// The two writes are now one statement inside one repository operation, so the
// transition either happens or does not. It is also idempotent: re-completing an
// already-migrated subscriber moves the timestamp forward rather than failing, so
// repeating this request after any failure is always safe.
//
// The invariant is enforced at every repository write, not only here: recording a
// URL clears migrated_at in the same statement, and MarkSubscriberMigrated refuses
// to stamp a row that still holds one. So no path THROUGH THE API can produce a row
// that is migrated and still carries a live endpoint.
//
// It is NOT a schema CHECK, and an operator should not assume one. A psql session or
// a restored backup CAN hold that pair, and deliberately so: the RETAIN-01 retention
// purge selects exactly it, so a constraint forbidding it would leave that control
// with nothing it could ever match. Audits that need the guarantee should read it as
// "true for API-managed rows".
//
// # Every status this route can answer, and the ones it no longer can
//
// The list below is exhaustive. It used to be shorter than the truth: this route
// went through the general subscriber update, which reconciles the subscriber's ACL
// bindings at the broker around its write, so a broker outage answered 503
// SUBSCRIBER_PROVISIONING_FAILED and a concurrent credential issuance holding the
// provisioning fence answered 409 — both for a request that only ever wanted to
// write one column of migration metadata, and neither documented.
//
// Recording a webhook URL changes no authorization, so there was never any grant to
// reconcile. This route now performs a REGISTRY WRITE and nothing else: no admin
// client is resolved, no fence is taken, no binding is touched, and neither of those
// statuses is reachable any more. An operator can record a URL during a Kafka
// incident, which is exactly when a migration record is most likely to be edited.
//
// # Responses
//
//	204 the recorded subscription is forgotten and the migration instant stamped, in
//	    one statement
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a non-canonical identifier
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
//
// NOT reachable: 409 and 503. No broker is contacted and no provisioning fence is
// taken, so neither a Kafka outage nor a concurrent issuance can fail this request.
//
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka
// event stream and IssueKafkaCredentials instead; see
// docs/webhook-to-kafka-migration.md.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) DeleteWebhookSubscription(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	// ONE call. The instant it returns is dropped because 204 carries no body; the
	// error is not, because a transition that did not land must never be reported as
	// a completed migration.
	if _, err := a.blnk.CompleteEventSubscriberWebhookMigration(
		c.Request.Context(), subscriberID,
	); err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	c.Status(http.StatusNoContent)
}

// subscriberPseudonymFromQuery reads the subscriber_id_hash resolver parameter.
//
// A PRESENT-BUT-BLANK value counts as resolving rather than as absent, so
// ?subscriber_id_hash= is refused by the resolver instead of quietly returning the
// first page of the registry. A caller who wrote the parameter meant to resolve
// something, and answering with an unrelated page is the wrong kind of helpfulness.
//
// Parameters:
//   - c *gin.Context: the request.
//
// Returns:
//   - string: the token, trimmed.
//   - bool: true when the parameter was present at all.
func subscriberPseudonymFromQuery(c *gin.Context) (string, bool) {
	raw, present := c.GetQuery(subscriberQueryParamPseudonym)

	return strings.TrimSpace(raw), present
}

// resolveSubscriberPseudonym answers a subscriber_id_hash resolution.
//
// # Three outcomes, and why the third is neither a 404 nor an empty page
//
//   - RESOLVED: a one-element array, shaped exactly like a page so a client needs no
//     second parser and no second code path.
//   - NOT IN THE REGISTRY: an empty array and 200. A filter that matches nothing is an
//     empty result rather than a missing resource — the collection exists and was
//     searched to its end.
//   - NOT ESTABLISHED: the resolver reached its bounded page ceiling on a registry
//     larger than that bound, so whether the token names a subscriber is UNKNOWN. An
//     empty page here would tell the operator that the alert names a subscriber which no
//     longer exists, which is exactly the false conclusion this resolver exists to
//     prevent, so it is reported as an internal failure instead.
//
// Parameters:
//   - c *gin.Context: the request and response.
//   - pseudonym string: the trimmed token. Empty is refused.
func (a *Api) resolveSubscriberPseudonym(c *gin.Context, pseudonym string) {
	if pseudonym == "" {
		respondCode(c, apierror.ErrGenValidation, subscriberBlankPseudonymMessage, nil)

		return
	}

	subscriber, err := a.blnk.ResolveEventSubscriberByPseudonym(c.Request.Context(), pseudonym)
	if err != nil {
		if errors.Is(err, blnk.ErrSubscriberHashResolutionExhausted) {
			respondCode(c, apierror.ErrGenInternal, subscriberPseudonymUnresolvedMessage, nil)

			return
		}

		var typed apierror.APIError
		if errors.As(err, &typed) && typed.Code == apierror.ErrSubscriberNotFound {
			// AN EMPTY PAGE, IN THE SAME ENVELOPE, and a searched-to-the-end total of zero. This
			// used to be a bare `[]`, which made "no such subscriber" a differently SHAPED answer
			// from every other reading of this route.
			c.JSON(http.StatusOK, completeSubscriberPage(nil))

			return
		}

		respondSubscriberRegistryError(c, err)

		return
	}

	c.JSON(http.StatusOK, completeSubscriberPage(
		[]model.SubscriberResponse{model.NewSubscriberResponse(*subscriber)},
	))
}

// bindStrictManagementJSON decodes a management request body, REFUSING any field the shape does
// not declare, and then runs the binding tags exactly as gin would.
//
// # What silently ignoring an unknown field costs on this surface
//
// gin's ShouldBindJSON discards unknown keys. encoding/json's default behaviour is the same, so a
// misspelling was accepted and dropped, and the consequence differs per route in a way that makes
// each one worse than a plain no-op:
//
//   - POST /subscribers with "authorised_topics" (the British spelling, or any typo) registered a
//     subscriber authorised for NOTHING. The field is optional at the binder, so the request
//     succeeded with 201 and the operator held a subscriber that can obtain a credential and read
//     no topic — a failure they discover from the consumer, not from the API.
//   - PUT /subscribers/{id} with every field misspelled decoded to a wholly empty update, which is
//     a legitimate instruction meaning "rewrite the row with its own values". So the response was
//     200 with the unchanged row, and a caller comparing the response against what they sent had
//     no way to tell an ignored field from a value the server chose to keep.
//   - The webhook-subscription bodies carry one field each, so a misspelling there is refused by
//     binding:"required" — but only by accident of the shape having nothing else in it.
//
// A rejected filter and a rejected body field are the same class of problem, and this API already
// refuses an unknown QUERY parameter for exactly this reason (see rejectUnsupportedQueryParameters).
// The body was the remaining half.
//
// # Why not gin's global toggle
//
// gin.EnableJsonDecoderDisallowUnknownFields() is process-wide and would change every endpoint in
// this API, including the many no finding covers. The same reasoning keeps ParseQueryOptions
// untouched. So the strictness is applied per handler, on the four management bodies.
//
// # Why the binding tags still run
//
// Decoding through encoding/json directly bypasses gin's validator, and these shapes depend on it:
// binding:"required" on the webhook URL, and "max=16,dive,max=249" on the topic list, which is what
// keeps a caller from submitting a thousand topic names or one longer than Kafka accepts. So the
// validator is invoked explicitly afterwards through the same binding.Validator gin itself uses —
// one decode, and the identical rules.
//
// # The two error codes, and why an unknown field is not "malformed"
//
// A body carrying an unknown field PARSES; nothing about the transport is wrong. It is a caller
// naming something this endpoint cannot honour, which is a validation failure and is the code an
// unknown query parameter already answers with. GEN_MALFORMED_REQUEST stays for what it always
// meant: JSON that does not parse, or a field of the wrong JSON type.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this returns.
//   - target any: a pointer to the request DTO.
//
// Returns:
//   - bool: false when the refusal has been written.
func bindStrictManagementJSON(c *gin.Context, target any) bool {
	if c.Request == nil || c.Request.Body == nil {
		respondCode(c, apierror.ErrGenMalformedRequest, "A JSON request body is required", nil)

		return false
	}

	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		// encoding/json reports an unknown field as `json: unknown field "x"` and offers no typed
		// error for it, so the prefix is the only handle. It is a stable, documented message; a
		// change to it would fail the test that pins this refusal rather than passing silently.
		if field, unknown := unknownJSONField(err); unknown {
			respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
				"%q is not a field this endpoint accepts; a field named but not honoured would "+
					"leave the request half-applied with nothing in the response to say so",
				field,
			), nil)

			return false
		}

		if errors.Is(err, io.EOF) {
			respondCode(c, apierror.ErrGenMalformedRequest, "A JSON request body is required", nil)

			return false
		}

		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

		return false
	}

	// A SECOND JSON VALUE after the first is refused rather than ignored, for the same reason an
	// unknown field is: `{"name":"a"}{"name":"b"}` is a caller sending two instructions and
	// receiving the first, with the response describing neither ambiguity.
	if decoder.More() {
		respondCode(c, apierror.ErrGenMalformedRequest,
			"The request body must contain exactly one JSON object", nil)

		return false
	}

	// THE BINDING TAGS, run through gin's own validator so decoding directly does not quietly
	// drop them. binding.Validator is the same instance ShouldBindJSON would have used.
	if err := binding.Validator.ValidateStruct(target); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

		return false
	}

	return true
}

// unknownJSONField extracts the field name from encoding/json's unknown-field error.
//
// Parameters:
//   - err error: the decode error.
//
// Returns:
//   - string: the offending field name, unquoted.
//   - bool: true when err is an unknown-field error.
func unknownJSONField(err error) (string, bool) {
	const prefix = "json: unknown field "

	message := err.Error()
	if !strings.HasPrefix(message, prefix) {
		return "", false
	}

	return strings.Trim(strings.TrimPrefix(message, prefix), `"`), true
}

// refuseSubscriberPagingOptions refuses a paging option a non-paging reading of GET /subscribers
// cannot honour.
//
// # Why refusing beats ignoring, and beats honouring
//
// The pseudonym resolution and the awaiting-revocation scan are not walks. The resolver returns
// at most one row; the scan deliberately covers the WHOLE registry, because a partial list of
// live unaccounted-for credentials reads exactly like a complete one and acting on it would leave
// the rest authenticating while the incident looked closed — which is why the service refuses
// rather than truncating when the registry exceeds its bound.
//
// So `?revocation_pending=true&limit=10` asks for something that does not exist. It was silently
// ignored: the caller received every matching row, believing they had asked for ten, with nothing
// in the response saying which they got. On this endpoint that is the dangerous direction — an
// operator who thinks they are holding a bounded page of a longer list stops looking. Honouring
// it is worse still, since a truncated scan is the exact failure mode the service refuses.
//
// `include_count` is deliberately NOT refused here: both readings are complete, so their length
// IS the total, and completeSubscriberPage reports it unconditionally. Asking for a total that is
// already there is not a contradiction.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this returns.
//   - reading string: the parameter that selected this reading, named in the refusal so the
//     operator can see which two of their parameters disagree.
//
// Returns:
//   - bool: false when the refusal has been written.
func refuseSubscriberPagingOptions(c *gin.Context, reading string) bool {
	for _, parameter := range []string{
		subscriberQueryParamLimit, subscriberQueryParamCursor,
	} {
		if _, present := c.GetQuery(parameter); !present {
			continue
		}

		respondCode(c, apierror.ErrGenValidation, fmt.Sprintf(
			"%q cannot be combined with %q: that reading returns a COMPLETE result rather than a "+
				"page, so there is nothing to bound or to resume from. Remove %q, or omit %q to "+
				"page the registry",
			parameter, reading, parameter, reading,
		), nil)

		return false
	}

	return true
}

// completeSubscriberPage wraps a COMPLETE result — one this route did not page — in the same
// envelope every other reading of GET /subscribers answers in.
//
// # Why the shape is not allowed to vary by branch
//
// This route has three readings: the ordinary keyset page, the pseudonym resolution and the
// awaiting-revocation scan. Two of them used to answer with a bare JSON array while the third
// answered with `{data, next_cursor, has_more, total_count?}`. A client written against one
// reading breaks on another with a type error rather than a message, and the two bare-array
// readings are the ones an operator reaches DURING AN INCIDENT, from a runbook, when a
// deserialisation failure is the most expensive thing that can happen.
//
// # Why has_more is false and total_count is always present here
//
// Neither of these readings is a page. The resolver returns at most one row and the revocation
// scan covers the WHOLE registry, so there is nothing to resume from — has_more is false and no
// cursor is emitted, which is exactly what a paging client needs to see to stop after one
// request. And because the result is complete, its length IS the total: reporting it is honest
// here in a way it would not be for a page, and it saves the caller inferring a total from a
// response shape that elsewhere means something else.
//
// Parameters:
//   - items []model.SubscriberResponse: the complete result. Nil is rendered as [].
//
// Returns:
//   - SubscriberPageResponse: the envelope, with total_count set to len(items).
func completeSubscriberPage(items []model.SubscriberResponse) SubscriberPageResponse {
	if items == nil {
		// [] rather than null, so a script can range over .data[] unconditionally.
		items = []model.SubscriberResponse{}
	}

	total := int64(len(items))

	return SubscriberPageResponse{Data: items, HasMore: false, TotalCount: &total}
}

// subscriberRevocationFilterFromQuery reads the revocation_pending filter.
//
// Only "true" selects the scan. An explicit "false" is accepted and means "the ordinary page",
// which is what the parameter's absence already means — carried rather than refused so a client
// building the query from a boolean variable does not have to special-case one of its values.
// Anything else is refused, for the reason the page readers refuse a non-numeric limit:
// answering a different question from the one asked, with nothing to say so, is worse than a
// 400.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this returns.
//
// Returns:
//   - bool: true when the scan was requested.
//   - bool: false when the refusal has been written.
func subscriberRevocationFilterFromQuery(c *gin.Context) (bool, bool) {
	raw, present := c.GetQuery(subscriberQueryParamRevocationPending)
	if !present {
		return false, true
	}

	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true":
		return true, true
	case "false", "":
		return false, true
	default:
		respondCode(c, apierror.ErrGenValidation, subscriberInvalidRevocationFilterMessage, nil)

		return false, false
	}
}

// listSubscribersAwaitingRevocation answers the revocation scan.
//
// # An incomplete scan is a FAILURE, never a shorter list
//
// The rows are live SASL/SCRAM credentials at the broker that no registry row records an
// issuance for. A truncated list reads as "these are the ones to revoke", and a responder
// acting on it would revoke those, close the incident, and leave the rest authenticating. So a
// walk that could not cover the registry is reported as an internal failure that names the
// reason, and the responder falls back to the SQL in the runbook.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) listSubscribersAwaitingRevocation(c *gin.Context) {
	subscribers, err := a.blnk.ListEventSubscribersAwaitingRevocation(c.Request.Context())
	if err != nil {
		if errors.Is(err, blnk.ErrSubscriberHashResolutionExhausted) {
			respondCode(c, apierror.ErrGenInternal, subscriberRevocationScanIncompleteMessage, nil)

			return
		}

		respondSubscriberRegistryError(c, err)

		return
	}

	items := make([]model.SubscriberResponse, 0, len(subscribers))
	for _, subscriber := range subscribers {
		items = append(items, model.NewSubscriberResponse(subscriber))
	}

	// THE SAME ENVELOPE as every other reading of this route. It used to be a bare array, so the
	// first step of the outstanding-revocation runbook returned a differently shaped body from the
	// ordinary listing — during an incident, from a command an operator copies.
	//
	// The scan covers the WHOLE registry rather than a page, so the total is exact and there is
	// nothing to resume from. See completeSubscriberPage.
	c.JSON(http.StatusOK, completeSubscriberPage(items))
}

// Query-parameter names and refusal messages for the operator lookups that page the
// registry: the pseudonym resolution and the awaiting-revocation filter.
const (
	// subscriberQueryParamPseudonym turns GET /subscribers into the RESOLVER for a
	// subscriber_id_hash token. Named for the response field it resolves, so an
	// operator reading subscriber_id_hash in a log line or on a lag alert can guess
	// the query without consulting the reference.
	subscriberQueryParamPseudonym = "subscriber_id_hash"

	// subscriberQueryParamRevocationPending narrows the list to subscribers with an
	// outstanding broker-side revocation. Named for the response field it filters on,
	// so the alert's remediation text and the query agree.
	subscriberQueryParamRevocationPending = "revocation_pending"

	// subscriberBlankPseudonymMessage answers ?subscriber_id_hash= with no value. See
	// subscriberPseudonymFromQuery for why a present-but-blank token is refused rather
	// than treated as an ordinary page request.
	subscriberBlankPseudonymMessage = "\"" + subscriberQueryParamPseudonym +
		"\" was supplied with no value; pass the token as it appears in the log line, the metric " +
		"label or the alert, or omit the parameter to page the registry"

	// subscriberPseudonymUnresolvedMessage answers a resolution that could not be
	// established either way. It says so explicitly, because the alternative reading —
	// "no such subscriber" — is the wrong conclusion and the expensive one.
	subscriberPseudonymUnresolvedMessage = "The subscriber registry is larger than the pseudonym " +
		"resolver's bound, so whether that token names a subscriber could not be established; " +
		"resolve it against the registry directly"

	// subscriberInvalidRevocationFilterMessage answers a revocation_pending value that is
	// neither "true" nor "false".
	subscriberInvalidRevocationFilterMessage = "\"" + subscriberQueryParamRevocationPending +
		"\" accepts only \"true\" or \"false\"; omit it to page the whole registry"

	// subscriberRevocationScanIncompleteMessage answers a scan that could not cover the
	// registry. It refuses rather than returning what it found, because a partial list of
	// live unaccounted-for credentials reads as a complete one.
	subscriberRevocationScanIncompleteMessage = "The subscriber registry is larger than the " +
		"revocation scan's bound, so the set of subscribers awaiting revocation could not be " +
		"established; read blnk.event_subscribers directly, filtering on revocation_pending_at"

	// streamInsecureTransportMessage answers a stream read whose channel is not confidential.
	//
	// It names the SASL secret in the request rather than a password in the response, because
	// that is what is exposed here and the difference changes how urgent it is: the header is
	// sent on every poll, so a plaintext consumer leaks a live credential continuously rather
	// than once.
	streamInsecureTransportMessage = "The subscriber event stream is not served over a transport " +
		"this deployment has not established as confidential, because the request carries the " +
		"subscriber's SASL secret in a header on every poll. Terminate TLS in Blnk " +
		"(BLNK_SERVER_SSL), or declare the proxy that terminates it and sets X-Forwarded-Proto " +
		"(BLNK_SERVER_TRUST_FORWARDED_PROTO), or call this endpoint over loopback"
)
