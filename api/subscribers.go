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

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/sirupsen/logrus"
)

// This file is the HTTP surface of the Kafka subscriber registry: ten master-key-gated
// endpoints in three groups — registry CRUD on /subscribers, credential issuance on
// /subscribers/:subscriber_id/kafka-credentials, and the deprecated legacy
// /subscribers/:subscriber_id/webhook-subscription surface, whose own section below
// explains why it exists.
//
// The Kafka identity and every privileged operation on it stay out of this file: no
// SQL, no Kafka client, no SCRAM computation, no password generation and no ACL
// construction. What the handlers own is the HTTP contract — route parameters, request
// binding, response projection and error codes — and, where a route's contract needs
// more than one call, the ordering of those calls.
//
//   - The subscriber identifier is the root of the subscriber's whole Kafka identity:
//     the SASL principal and the consumer-group namespace are DERIVED from it by the
//     root package, and every ACL binding is granted against those derived values.
//   - Credential issuance mints a secret, upserts a SCRAM credential, binds ACLs and
//     records the issuance, and it compensates a partial failure by revoking what it
//     wrote.
//   - Error responses go through respondCode/respondError from api/errors.go with a
//     typed code from internal/apierror.
//
// All ten are OPERATOR endpoints and every one of them gates on the master key as its
// very first act, before a parameter is read and before a body is bound, mirroring
// ensureHookManagementAuthorized. Issuance is the reason this is not negotiable: a
// scoped API key that could reach it would be able to mint a Kafka credential for any
// subscriber in the deployment and read that subscriber's event stream.
//
// The plaintext SASL password exists in exactly one expression in this file: the
// KafkaCredentialsResponse literal in IssueKafkaCredentials. It is never logged, never
// interpolated into a message, never placed in an apierror details payload, and never
// returned by any read. This file imports no logger at all, which is the structural
// form of that promise.
//
// Because that one response is the only chance to disclose the password, it is also the
// only place in this package where the CHANNEL is part of the contract:
// ensureCredentialTransportConfidential refuses issuance unless the deployment has
// established the channel as confidential — TLS in-process, a declared proxy boundary
// reporting https, or a loopback peer on a host the deployment has declared to be
// local-development. An authenticated request over plaintext is a correctly authorised
// leak, and no amount of care about where the password is not written protects against
// carrying it in clear.

const (
	// subscriberPageDefaultLimit and subscriberPageMaxLimit are the page bounds applied at
	// the HTTP boundary.
	subscriberPageDefaultLimit = 20
	subscriberPageMaxLimit     = 100

	// subscriberRouteParam is the route parameter every per-subscriber route
	// carries, declared once so the readers and the error messages cannot drift
	// from the paths api/api.go registers.
	subscriberRouteParam = "subscriber_id"

	// headerForwardedProto and forwardedProtoHTTPS are the proxy's report of the scheme
	// the CLIENT used, which credential issuance consults only when the deployment has
	// declared the proxy that sets it — see ensureCredentialTransportConfidential. Named
	// constants rather than inline literals because the header is compared
	// case-insensitively in one place and a second spelling of either would be a silently
	// weaker check.
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
)

// subscriberListQueryParameters is every query parameter GET /subscribers accepts, in
// the order the refusal lists them.
//
// An operator running a migration report cannot tell that from a correct answer, and
// would read "every subscriber" as "every subscriber matching my filter" — the more
// dangerous of the two readings, because it looks complete.
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

	// subscriberInvalidLimitMessage answers a page size that is not an integer at all. A
	// value that is merely out of range is normalised, but a non-numeric one is a client
	// mistake and is refused rather than silently defaulted — defaulting it would answer a
	// different question from the one asked, with nothing to say so.
	subscriberInvalidLimitMessage = "Invalid " + subscriberQueryParamLimit + " value"

	// subscriberInvalidCursorMessage answers a cursor value this endpoint cannot decode
	// into a page position.
	//
	// It is REFUSED rather than treated as "start from the beginning". A paging client
	// that receives page one in answer to a cursor it thought pointed into the middle of
	// the registry pages for ever.
	subscriberInvalidCursorMessage = "\"" + subscriberQueryParamCursor +
		"\" could not be decoded as a page position; omit it for the first page, or pass back " +
		"the \"next_cursor\" value from a previous response verbatim"

	// subscriberMissingRowMessage covers a service that reported success without returning
	// the row. It cannot happen through any current path, and it is answered rather than
	// dereferenced so that the impossible case is a 500 with an explanation instead of a
	// panic recovered by the middleware.
	subscriberMissingRowMessage = "The subscriber registry reported success without returning the subscriber"

	// credentialIssuanceTimedOutMessage and credentialIssuanceCancelledMessage are the
	// last-resort answers for an issuance whose context ended while the failure carried no
	// typed code of its own. The budget is named in neither, because the value is
	// configuration and a stale number in a message is worse than no number.
	credentialIssuanceTimedOutMessage = "Provisioning Kafka credentials did not complete within the issuance budget"

	credentialIssuanceCancelledMessage = "Provisioning Kafka credentials was cancelled before it completed"

	// credentialInsecureTransportMessage is the refusal to put a one-time SASL password on
	// a channel this deployment has not declared confidential.
	credentialInsecureTransportMessage = "Kafka credentials are not issued over a transport this " +
		"deployment has not established as confidential, because the response carries a one-time " +
		"password. Terminate TLS in Blnk (BLNK_SERVER_SSL), or declare the proxy that terminates it " +
		"and sets X-Forwarded-Proto (BLNK_SERVER_TRUST_FORWARDED_PROTO) TOGETHER WITH the proxy " +
		"addresses that declaration applies to (BLNK_SERVER_TRUSTED_PROXIES) — the header is " +
		"believed only on a request whose socket peer is one of them, because any other caller can " +
		"send the same header. For local development only, a loopback caller can be permitted with " +
		"BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE, which is unsafe on any host running a " +
		"reverse proxy"

	// credentialIssuanceAbandonedMessage answers a request the handler stopped waiting for
	// because the wall-clock ceiling elapsed while the service was still working.
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
// rather than restating how the auth middleware records the principal, so there is one
// reading of "is this the master key" in this package.
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
//  1. TLS IN-PROCESS. c.Request.TLS is non-nil only when this process completed the
//     handshake.
//
//  2. A DECLARED PROXY BOUNDARY, FROM A DECLARED PROXY. X-Forwarded-Proto is a request
//     header, so any client can send it and no process can tell a proxy's value from a
//     caller's.
//
//     declaration is a statement about the intended path, and a request that arrives by
//     any other route is not on it: a pod IP inside the cluster, a port-forward, a second
//     Service, an ingress that passes the client's header through. Each of those sends its
//     own X-Forwarded-Proto, and believing it hands that caller the one-time password.
//     Requiring the peer costs nothing a real proxy deployment does not already have,
//     because BLNK_SERVER_TRUSTED_PROXIES is the list that deployment already sets for the
//     client address to be trustworthy — the same question about the same proxy, so a
//     second variable would only let the two answers disagree. A universal range
//     (0.0.0.0/0, ::/0) is not an allowlist and is rejected; config.validateForwardedProtoTrust
//     refuses that combination outright in secure mode.
//
//  3. A DECLARED LOCAL-DEVELOPMENT HOST WITH A LOOPBACK PEER. Bytes to 127.0.0.0/8 or
//     ::1 never reach a network on their LAST hop, which is not the same as never
//     reaching one at all: a reverse proxy on this host can accept a plaintext request
//     from the internet and forward it over 127.0.0.1, so the peer is loopback while the
//     client's hop was public and readable.
//
// Anything else is refused, which is deny-by-default: a deployment that has stated
// nothing gets no secret on the wire. A loopback caller on an undeclared host is
// refused too, which is the whole point of channel 3 being a declaration.
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

	// Both remaining channels are DECLARATIONS by the deployment, so both read the live
	// configuration store — for the same reason subscriberTopicPrefix does: a value
	// captured when the router was built would describe the deployment as it stood then. A
	// store that cannot be read declares nothing, which refuses.
	configuration := a.blnk.Config()
	if configuration == nil {
		return false
	}

	// 2. The deployment declared the proxy that terminated TLS, THIS REQUEST CAME FROM ONE
	//    OF THE PROXIES IT NAMED, and that proxy says the client hop was https.
	if configuration.TrustsForwardedProtoFrom(c.Request.RemoteAddr) {
		if strings.EqualFold(strings.TrimSpace(c.GetHeader(headerForwardedProto)), forwardedProtoHTTPS) {
			return true
		}
	}

	// 3. The deployment declared itself a local-development host AND the peer is on it. The
	// declaration is required because a loopback peer is only ever evidence about the LAST
	// hop; see the channel list above for the same-host proxy shape it cannot distinguish.
	if configuration.Server.AllowLoopbackCredentialIssuance {
		return requestPeerIsLoopback(c.Request)
	}

	return false
}

// requestPeerIsLoopback reports whether the request's PEER — the far end of the
// accepted socket — is a loopback address.
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

// subscriberIDFromRoute reads the subscriber identifier out of the route, writing the
// refusal itself when it is absent or blank.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
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
		// The model's message names the specific rule broken and quotes the offending value,
		// which is what makes the refusal actionable. It is echoed rather than replaced with
		// a generic sentence for that reason; the value came from the request line, so it
		// discloses nothing the caller did not send.
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
// The endpoint accepted any non-negative offset and passed it into an OFFSET clause,
// whose cost PostgreSQL pays by reading and discarding every row before it. The depth
// was the caller's to choose and nothing capped it, so the page-size limit bounded the
// response and bounded no work at all.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
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

// optionalSubscriberField converts a request DTO's plain string into the nilable form
// the service and the nullable columns use.
func optionalSubscriberField(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	return &value
}

// respondSubscriber writes a stored subscriber as a success body.
//
// It routes every subscriber response through model.NewSubscriberResponse, which is the
// single place the credential reference is reduced to a non-sensitive fingerprint and
// the enforced-access declaration is assembled. No handler here builds a subscriber
// body field by field, so none can leak the reference or imply that a partition-key
// prefix nothing keeps is an enforced boundary.
func (a *Api) respondSubscriber(c *gin.Context, status int, subscriber *coremodel.EventSubscriber) {
	if subscriber == nil {
		respondCode(c, apierror.ErrGenInternal, subscriberMissingRowMessage, nil)

		return
	}

	c.JSON(status, a.subscriberResponse(*subscriber))
}

// subscriberAccessDeployment resolves the configuration facts a subscriber body has to
// be truthful about, once per response.
//
// The three facts are whether this deployment can enforce a partition-key scope,
// whether it advertises subscriber-facing brokers, and whether whole-topic subscriber
// access has been declared. All three are configuration, and api/model cannot read
// configuration: the root package that owns these predicates imports api/model in its
// own tests, so the reverse edge would close a cycle.
//
// Returns:
//   - coremodel.SubscriberAccessDeployment: the resolved state. Fail-closed when
//     configuration cannot be read, so a body understates the deployment's capability
//     rather than overstating it.
func (a *Api) subscriberAccessDeployment() coremodel.SubscriberAccessDeployment {
	return blnk.SubscriberAccessDeployment()
}

// subscriberResponse projects a stored row and then applies the ONE post-sunset
// adjustment the projection itself cannot make.
//
// WHAT IT SUPPRESSES, AND WHY ONLY THIS. Past the retirement instant nothing delivers
// to a legacy endpoint and nothing may record one, so continuing to publish webhook_url
// would hand out a third-party URL that describes a subscription which no longer exists
// — the read half of the same retirement the write path refuses. The column itself is
// NOT erased here: erasing rows is ClearLegacyWebhookSubscription's and
// PurgeMigratedWebhookURLs' job, and an operator mid-cleanup still needs the value in
// the database. migrated_at survives in the body, because migration progress is an
// audit fact about Blnk rather than third-party data.
//
// Parameters:
//   - subscriber coremodel.EventSubscriber: the stored registry row.
//
// Returns:
//   - model.SubscriberResponse: the body to write.
func (a *Api) subscriberResponse(subscriber coremodel.EventSubscriber) model.SubscriberResponse {
	// NOTHING TO SUPPRESS HERE, and that is the stronger position. The registry-read shape
	// does not carry the legacy webhook URL AT ALL — see model.NewSubscriberResponse — so
	// a third-party endpoint cannot leak through this route either before or after the
	// retirement instant. The URL is readable only from the dedicated webhook-subscription
	// route, which the sunset guard retires outright.
	return model.NewSubscriberResponse(subscriber, a.subscriberAccessDeployment())
}

// respondWebhookSubscription writes the legacy webhook-subscription read shape.
//
// It carries the recorded URL and the migration instant and nothing else. The legacy
// transport's signing secret and configured headers are deployment-wide configuration
// rather than per-subscriber data, so surfacing them here would turn a
// migration-tracking response into a secret-bearing one.
//
// Deprecated: it serves only the legacy webhook-subscription routes, which are answered
// with 410 Gone once WEBHOOK_DEPRECATION_SUNSET_DATE has passed, and it is removed with
// them. See docs/webhook-to-kafka-migration.md.
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
// The service and the repository return typed apierror values on every path that has a
// domain meaning, so respondError resolves the code from the error itself. Two options
// are applied on top:
//
//   - withUpgrade(GEN_NOT_FOUND, SUBSCRIBER_NOT_FOUND) because api/errors.go's
//     classifyMessage ends with a broad {"not found"} catch-all.
//   - withDefault(GEN_INTERNAL) so that an unclassified repository failure is reported
//     as an internal error with a sanitised message rather than echoing driver text to
//     the caller.
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
// Only when the failure carries NO code of its own is the context consulted, and then
// an expired or cancelled issuance is answered with SUBSCRIBER_PROVISIONING_FAILED —
// 503, retryable — rather than being allowed to fall through to a bare 500. That is
// what the endpoint's budget buys: an expiry is ANSWERED rather than waited out, and
// the answer says whether retrying is sensible.
func respondCredentialIssuanceError(c *gin.Context, issuance context.Context, err error) {
	if isTypedAPIError(err) {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrSubscriberNotFound))

		return
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(issuance.Err(), context.DeadlineExceeded) {
		respondCode(c, apierror.ErrSubscriberProvisioningFailed, credentialIssuanceTimedOutMessage, nil)

		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(issuance.Err(), context.Canceled) {
		respondCode(c, apierror.ErrSubscriberProvisioningFailed, credentialIssuanceCancelledMessage, nil)

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
// The contract requires credential provisioning to complete within five seconds, and
// the handler bounds the service's context to exactly that. That bound was not enough
// on its own, and the reason is worth stating precisely because it is the opposite of a
// missing timeout.
//
// Parameters:
//   - ctx context.Context: the budgeted issuance context. Its expiry is the ceiling.
//   - subscriberID string: the subscriber to issue for.
//   - issue func: the service call, taken as a parameter so the ceiling is testable
//     without a broker.
//
// Returns:
//   - blnk.SubscriberCredential: the credential, zero-valued unless completed is true
//     and err is nil.
//   - error: the issuance failure, if any.
//   - bool: false when the budget elapsed first and the result was abandoned.
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

// subscriberTopicPrefix reports the configured Kafka topic namespace, which the request
// DTOs need in order to tell a Blnk-owned topic from any other name.
func (a *Api) subscriberTopicPrefix() string {
	if configuration := a.blnk.Config(); configuration != nil {
		return configuration.Kafka.TopicPrefix
	}

	return ""
}

// subscriberInternalTopicAccess reports whether this deployment has acknowledged that a
// subscriber may be granted the internal category topic, `<prefix>.system`.
//
// It is read live from the same configuration store as the prefix, for the same reason:
// the store's contents are replaced wholesale, so a value captured when the router was
// assembled would describe the deployment as it stood then. An unloaded configuration
// answers FALSE, which is the fail-closed direction — a missing configuration must
// never be the thing that makes Blnk's own error stream grantable.
//
// The value reaches the request DTOs as model.WithInternalTopicAccess, so the DTO
// validates against the deployment it is running in without reading configuration
// itself.
//
// Returns:
//   - bool: KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS, or false when configuration is
//     unavailable.
func (a *Api) subscriberInternalTopicAccess() bool {
	if configuration := a.blnk.Config(); configuration != nil {
		return configuration.Kafka.SubscriberInternalTopicAccess
	}

	return false
}

// ---------------------------------------------------------------------------------------
// Registry CRUD
// ---------------------------------------------------------------------------------------

// CreateSubscriber serves POST /subscribers: it registers a Kafka subscriber.
//
// Registration DESCRIBES an access boundary; it does not grant one. No broker is
// touched, no credential is minted and no ACL is bound, so a freshly registered
// subscriber can read nothing at all until POST
// /subscribers/:subscriber_id/kafka-credentials is called for it.
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

	// The DTO owns every rule the binding tags cannot express, and it is given the live
	// prefix so that "is this topic ours?" is answered against this deployment's namespace
	// rather than a default. respondCode is used rather than respondError because a DTO
	// refusal is a KNOWN condition: routing it through the message-pattern table would
	// risk a validation message being classified as some unrelated domain's error.
	if err := req.Validate(
		a.subscriberTopicPrefix(),
		model.WithInternalTopicAccess(a.subscriberInternalTopicAccess()),
	); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	subscriber, err := a.blnk.RegisterEventSubscriber(c.Request.Context(), blnk.SubscriberRegistration{
		SubscriberID:     req.SubscriberID,
		Name:             req.Name,
		AuthorizedTopics: req.AuthorizedTopics,
		// The nullable column takes the nilable form, so "not recorded" stays distinguishable
		// from "recorded as empty" all the way to the row.
		//
		// WebhookURL is deliberately NOT set from the request: this route accepts no
		// webhook_url, because it is not fronted by the sunset guard and would otherwise be a
		// way to keep writing legacy webhook state after the guarded routes had begun
		// answering 410 Gone. A newly registered subscriber therefore carries no legacy URL
		// until one is recorded through POST
		// /subscribers/:subscriber_id/webhook-subscription.
		PartitionKeyPrefix: optionalSubscriberField(req.PartitionKeyPrefix),
	})
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	a.respondSubscriber(c, http.StatusCreated, subscriber)
}

// ListSubscribers serves GET /subscribers: the paged registry an operator answers "who
// is this principal?" and "who has not migrated yet?" from.
//
// This route has three readings — the ordinary keyset page, the `subscriber_id_hash`
// resolution and the `revocation_pending=true` scan — and every one of them answers
// with the SubscriberPageResponse envelope: `{data, next_cursor, has_more,
// total_count?}`. A client reads `.data[]` whichever it asked for.
//
// That is a breaking difference rather than a cosmetic one: a client written against
// the page reading fails on the resolver with a type error rather than a message, and
// the two bare-array readings are precisely the ones an operator reaches from a runbook
// DURING AN INCIDENT, where a deserialisation failure is the most expensive thing that
// can happen.
//
// The two non-paging readings return COMPLETE results — the resolver at most one row,
// the scan the whole registry — so they report `has_more: false`, emit no cursor, and
// always carry `total_count`, which for a complete result is simply its length. They
// also REFUSE `limit` and `cursor` rather than ignoring them: see
// refuseSubscriberPagingOptions.
//
// limit follows this package's uniform convention: absent, non-positive or oversized
// values become 20, and a value that is not an integer at all is refused rather than
// silently defaulted. PAGING IS BY CURSOR: the response carries next_cursor and a
// caller hands it back as ?cursor= for the following page.
//
// The value is parsed strictly: "1", "t", "true", "TRUE", "True" and their false
// counterparts are accepted, an absent or empty parameter means false, and anything
// else is a validation error rather than a silent false. A misspelling that changes the
// response shape without saying so is worse than one that is refused.
//
// The parameter set is CLOSED. An unrecognised name is refused, because a narrowing a
// caller asked for and did not get hands them a result that answers a different
// question with nothing to say so.
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
	if pseudonym, resolving := subscriberPseudonymFromQuery(c); resolving {
		if !refuseSubscriberPagingOptions(c, subscriberQueryParamPseudonym) {
			return
		}

		a.resolveSubscriberPseudonym(c, pseudonym)

		return
	}

	// THE REVOCATION SCAN, on the same route and for the same reason: it narrows the
	// collection rather than addressing a resource. It is the first step of the runbook
	// for an outstanding revocation, which the alert itself cannot attribute to a
	// subscriber because a label would export a tenant identifier into every notification
	// — so without this the alert names a count and nothing an operator can act on.
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
	// A subscriber registered or deregistered between them is counted by one read and
	// absent from the other. The combined read draws both from one read-only REPEATABLE
	// READ snapshot; a caller that did not ask for a total still takes the cheaper
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
		items = append(items, a.subscriberResponse(subscriber))
	}

	// The page and the position to resume from. A caller pages by handing next_cursor back
	// verbatim, and its absence is the termination condition — one fewer request than
	// paging until an empty page, and unlike an offset it cannot repeat or skip a row when
	// a subscriber is registered mid-pagination.
	response := SubscriberPageResponse{Data: items, HasMore: page.HasMore}
	if page.NextCursor != nil {
		response.NextCursor = page.NextCursor.Encode()
	}

	if includeCount {
		// An AGGREGATE over the registry rather than the length of this page: a paging client
		// that reads the page length as the total stops after one full page, or compares a
		// number against itself for ever. It was read in the same snapshot as the page above,
		// so the two describe one registry — see SubscriberPageResponse.TotalCount for what
		// that does and does not promise.
		response.TotalCount = &total
	}

	c.JSON(http.StatusOK, response)
}

// SubscriberPageResponse is the body GET /subscribers returns.
//
// It replaced a bare JSON array when paging moved from an offset to a cursor: the
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
	TotalCount *int64 `json:"total_count,omitempty"`
}

// GetSubscriber serves GET /subscribers/:subscriber_id: one registry row.
//
// 200 the subscriber
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

	a.respondSubscriber(c, http.StatusOK, subscriber)
}

// UpdateSubscriber serves PUT /subscribers/:subscriber_id: it applies the mutable
// subset of a subscriber.
//
// Replacing authorized_topics is reconciled at the broker as part of the update: the
// service removes the bindings the new authorization no longer implies BEFORE
// persisting the row, and creates the ones it adds AFTER. Every partial failure is
// therefore fail-closed — the subscriber ends with less access than the registry
// records, never more — and re-running the same request completes it. A widening that
// could not be applied is reported as a provisioning failure rather than being reported
// as an update that succeeded.
//
//	200 the subscriber as it now stands
//	400 GEN_MALFORMED_REQUEST for an unbindable body
//	400 GEN_VALIDATION_ERROR for a field this shape does not declare, or for a body
//	    the DTO refuses. An unknown field is refused rather than dropped: a body whose
//	    fields were all misspelled decodes to an empty update, which is a legitimate
//	    instruction, so the response was 200 with the row unchanged
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_KEY_SCOPE_UNENFORCED when the update would record a partition-key
//	    prefix and this deployment declares no component that authorises record keys.
//	    Kafka's authorizer has no message-key dimension, so the prefix would describe
//	    a boundary nothing keeps. Declare the component (KAFKA_KEY_SCOPE_ENFORCEMENT
//	    with its gateway addresses and attestation endpoint), or narrow
//	    authorized_topics to what the broker does enforce
//	409 SUBSCRIBER_KEY_SCOPE_REQUIRED when the update would CLEAR a partition-key
//	    prefix while this deployment declares the key-scoped model. Clearing is the
//	    one path that widens a live principal back to whole-topic Read without
//	    issuance running again, so it is refused for as long as the declaration
//	    stands: record the prefix this subscriber is entitled to instead, or stop
//	    declaring the model and acknowledge whole-topic access with
//	    KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS
//	409 GEN_CONFLICT when a concurrent issuance holds the provisioning fence, or when
//	    the row carries a revocation tombstone — complete or reverse its
//	    deregistration first
//	503 SUBSCRIBER_PROVISIONING_FAILED when the broker-side reconciliation did not
//	    complete. The registry row may already have been written: the row and the
//	    broker are reconciled in that order, and a failure is reported rather than
//	    swallowed. Retry the identical request — naming authorized_topics is always
//	    a re-apply instruction, so a retry repairs a half-applied grant
//
// It does NOT answer 410 GEN_GONE. The service refuses to record a legacy webhook_url
// after the retirement instant, but this route passes none — a body naming webhook_url
// is a 400 GEN_VALIDATION_ERROR from the strict decoder before the service is reached,
// and PUT /subscribers/:subscriber_id/webhook-subscription is the write path that owns
// that refusal.
//
// The diagnosis was right about the grant and wrong about the remedy: refusing left the
// wide access in place and removed the operator's only way to record that it should go.
// The update is what NARROWS it — every authorised topic moves from Read+Describe to
// Describe alone, so the existing credential's next direct fetch is refused by the
// broker — and a WARNING is logged naming the withdrawal, because the response that
// credential was delivered with declared direct broker access and cannot be recalled.
// That code no longer exists.
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

	if err := req.Validate(
		a.subscriberTopicPrefix(),
		model.WithInternalTopicAccess(a.subscriberInternalTopicAccess()),
	); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	// Passed through in their nilable form without normalisation, because the nilability
	// IS the instruction: collapsing a present empty string to nil here would silently
	// turn "clear this" into "leave it alone". WebhookURL is left nil, which
	// SubscriberUpdate reads as "leave as stored": this route accepts no webhook_url, for
	// the reason given on CreateSubscriber. The guarded PUT
	// /subscribers/:subscriber_id/webhook-subscription is the write path.
	subscriber, err := a.blnk.UpdateEventSubscriber(c.Request.Context(), subscriberID, blnk.SubscriberUpdate{
		Name:               req.Name,
		AuthorizedTopics:   req.AuthorizedTopics,
		PartitionKeyPrefix: req.PartitionKeyPrefix,
	})
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	a.respondSubscriber(c, http.StatusOK, subscriber)
}

// DeleteSubscriber serves DELETE /subscribers/:subscriber_id: it deregisters a
// subscriber and revokes its Kafka access.
//
// This deletes the SUBSCRIBER. Removing only a subscriber's recorded legacy endpoint is
// DELETE /subscribers/:subscriber_id/webhook-subscription, which leaves the subscriber
// registered and consuming.
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

// IssueKafkaCredentials serves POST /subscribers/:subscriber_id/kafka-credentials: it
// mints the subscriber's SASL/SCRAM credential and hands back everything needed to
// start consuming — the subscriber-facing broker endpoint, the authorised topic list,
// the consumer group and the credential itself.
//
// The plaintext appears in one expression in this file: the response literal below. It
// is never logged, never interpolated into a message, never placed in an apierror
// details payload, and no other endpoint anywhere returns it. Only a non-reversible
// reference and the issuance instant are persisted, and blnk.event_subscribers has no
// column able to hold the secret, so the value cannot be read back by any route —
// including this one re-called, which mints a NEW credential rather than returning the
// old one.
//
// Three things must hold, and only the first two are about the caller:
//
//   - SECURE MODE (BLNK_SERVER_SECURE) with a MASTER KEY (BLNK_SERVER_SECRET_KEY, from
//     a secret rather than a manifest literal).
//   - THE MASTER KEY on the request, which is what ensureSubscriberManagementAuthorized
//     checks.
//   - A CONFIDENTIAL CHANNEL, which is what ensureCredentialTransportConfidential
//     checks: TLS terminated in-process, or a declared proxy boundary reporting https,
//     or — on a host declared local-development with
//     BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE — a loopback peer.
//
// What the caller is guaranteed:
//
//   - The request ends within the budget on EVERY path. The service fixes one absolute
//     instant and this handler installs the same ceiling at the boundary, so the
//     guarantee holds whatever a future service refactor does with its own timeout.
//     context.WithTimeout only ever shortens, so the two cannot conflict and a caller
//     whose own context expires sooner still wins.
//     blnk.SubscriberCredentialIssuanceBudget is used rather than a literal duration so
//     the endpoint's ceiling cannot drift from the operation's.
//   - An expiry is ANSWERED, not waited out: see respondCredentialIssuanceError.
//
// What the caller is NOT guaranteed, and how it finds out:
//
//   - That everything a failed attempt owed is finished by the time it answers.
//   - A failure whose broker-side compensation has been scheduled but not yet confirmed
//     reports `compensation_pending` in its detail. It means retry: the subscriber
//     stays fenced until the cleanup finishes, so an immediate retry is refused with a
//     conflict rather than racing it.
//   - A cleanup that could NOT finish leaves a durable settlement marker on the row —
//     credential_orphaned_at, credential_cleanup_pending_at, grant_reconcile_pending_at
//     — which GET /subscribers publishes and the docs/kafka-operations.md runbook acts
//     on.
//
// THAT SHAPE NOW REQUIRES A DECLARATION IN PRODUCTION, and it is refused without one.
// Whole-topic reads are the mandated access model rather than a defect, and they are
// right for a single-tenant ledger; what was wrong is that they were the DEFAULT,
// reached by configuring nothing, so the widest credential Blnk can issue was the one a
// deployment got before anybody had decided. In secure mode this route now answers 409
// SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED until the deployment sets
// KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true or declares the key-scoped model instead.
//
// AND IN A DEPLOYMENT THAT DECLARES THE KEY-SCOPED MODEL, this shape is refused
// outright with 409 SUBSCRIBER_KEY_SCOPE_REQUIRED: a prefix-less subscriber there is
// the single principal the declared boundary does not cover, and it would be granted
// literal topic Read while every other subscriber is confined to its own ledgers.
//
// For a subscriber that RECORDS a prefix the credential grants Describe but NOT Read on
// those topics, so the broker refuses every record fetch it attempts, and the records
// have to reach it through the key-authorising component the deployment DECLARED in
// front of the brokers — whose address the response carries in broker_endpoint in place
// of KAFKA_SUBSCRIBER_BROKERS. The enforced-access declaration — assembled by
// model.NewSubscriberEnforcedAccessUnder, never here — reports that as
// gateway_delivery_required, broker_record_access false and
// partition_key_prefix_enforced_by "broker_gateway", so a client learns where to
// consume from in the same body that carries the secret.
//
// AND WHERE NO COMPONENT IS DECLARED — the shipped default — THIS ROUTE REFUSES SUCH A
// SUBSCRIBER with 409 SUBSCRIBER_KEY_SCOPE_UNENFORCED. Blnk ships no key-authorising
// component and exposes no data-plane route of its own: there is no GET under
// /subscribers that returns records.
//
// A DECLARATION IS NOT ENOUGH EITHER. Before a secret exists and before the broker is
// touched, Blnk calls the declared component's
// control endpoint over an authenticated channel and requires it to confirm that it
// enforces key scopes, for THIS principal, with the recorded prefix byte-for-byte. An
// unreachable, unauthenticated, refusing or disagreeing component answers 409
// SUBSCRIBER_KEY_SCOPE_UNATTESTED and no credential is minted.
//
// The order matters and it is the whole of the correction. Issuing whole-topic Read
// beside a prefix nothing applies is the exposure that was closed: cooperation is not
// an authorization boundary. Refusing outright, rather than minting a principal that
// can fetch nothing, is what keeps the response from ever claiming a boundary no
// component keeps.
//
//	200 the credential and the connection details
//	400 GEN_VALIDATION_ERROR for an identifier no Kafka identity can be derived from
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller,
//	    SUBSCRIBER_INSECURE_TRANSPORT when the channel is not established as
//	    confidential
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_GRANT_EMPTY for a subscriber authorised for no topics,
//	    SUBSCRIBER_KEY_SCOPE_UNENFORCED for a subscriber recording a partition-key
//	    prefix while no key-authorising component is declared, which Kafka's
//	    authorizer has no dimension for,
//	    SUBSCRIBER_KEY_SCOPE_REQUIRED for a subscriber recording NO prefix while the
//	    deployment DOES declare a key-scoped model — the one credential that would
//	    escape it, since it would be granted whole-topic Read,
//	    SUBSCRIBER_KEY_SCOPE_UNATTESTED when the declared enforcement component did
//	    not confirm this principal's exact recorded prefix over its authenticated
//	    control endpoint, whether because it was unreachable, refused, or answered
//	    with a different prefix. The error detail's `retryable` flag distinguishes a
//	    transport failure from a mismatch,
//	    SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED in secure mode when the
//	    deployment has declared neither model and the credential would therefore read
//	    every record on each granted topic,
//	    SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION when the broker would grant the
//	    principal more than the registry records, the credential having been revoked
//	    again,
//	    GEN_CONFLICT when a concurrent issuance superseded this one
//	503 EVENT_KAFKA_UNAVAILABLE when no broker is configured or reachable,
//	    SUBSCRIBER_BROKERS_NOT_CONFIGURED when no subscriber-facing endpoint is
//	    published, SUBSCRIBER_PROVISIONING_FAILED when the broker REFUSED the
//	    credential or its bindings AND for EVERY deadline expiry or cancellation,
//	    whichever dependency consumed the budget — the broker, the registry, or the
//	    caller going away. One code covers both because both are retryable and the
//	    published taxonomy carries no separate timeout code; the message says which
//	    happened, and whether a credential may already exist at the broker that the
//	    caller does not hold is reported in the error detail.
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) IssueKafkaCredentials(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	// THE IDENTIFIER FIRST, and it is refused in memory before anything else is judged. A
	// value no subscriber id can ever equal — "sub_ABC!", "*", "a" — gets the 400 that
	// says "that is not an identifier" on THIS route and on GET /subscribers/:id alike,
	// which is the point: both read the parameter through the same helper, so a caller
	// cannot be told two different things about one malformed value.
	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	// THE TRANSPORT, checked after the caller and the identifier and before anything is
	// minted. A correct master key over a channel nobody has established as confidential
	// is an authorised credential leak, and the refusal has to happen before the broker is
	// touched: a password refused after issuance would already exist at the broker,
	// unusable by the caller that never received it and still requiring revocation.
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
			// and no control characters — but that was never the reason the rest of this
			// codebase hashes it. A subscriber id is a TENANT-CHOSEN name that reaches logs,
			// alert annotations and incident tickets, so every other layer emits the keyed
			// digest under a _hash key and this one line emitted the name itself: enough to make
			// the pseudonyms everywhere else resolvable by anyone reading the same stream, and
			// it was the field that failed most visibly, on a timeout an operator goes looking
			// for.
			"subscriber_id_hash": logsafe.Identifier(subscriberID),
			"budget":             blnk.SubscriberCredentialIssuanceBudget.String(),
		}).Warn(
			"subscribers api: credential issuance exceeded the issuance budget and the request was " +
				"answered without waiting for it. The abandoned attempt observes the same expired " +
				"context and compensates on its own detached budget, so a credential it had already " +
				"written is revoked rather than left live; re-issuing is safe either way",
		)

		respondCode(c, apierror.ErrSubscriberProvisioningFailed, credentialIssuanceAbandonedMessage, nil)

		return
	}

	if err != nil {
		respondCredentialIssuanceError(c, ctx, err)

		return
	}

	// Normalised once and used for both the top-level grant and the enforced-access
	// declaration, so neither can report null for a set the subscriber has to read.
	// Issuance refuses an empty grant outright, so a successful response always names at
	// least one topic.
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
		// TWO FIELDS THE CLIENT CANNOT RECONSTRUCT.
		//
		// The fingerprint is the only handle a client has on an issuance afterwards, since
		// the password is returned once and nothing persists it — it is the same value a
		// subscriber read reports, so the two become comparable. Replaced is the
		// destructive-action confirmation: Kafka stores one credential per principal, so a
		// true here means a live consumer's password has just stopped working, and that is
		// not something to learn from a support ticket.
		CredentialFingerprint: credential.Fingerprint,
		Replaced:              credential.Replaced,
	})
}

// ---------------------------------------------------------------------------------------
// The deprecated legacy webhook-subscription surface
//
// They are MIGRATION BOOKKEEPING. Recording a subscriber's legacy webhook URL gives an
// already-webhooked subscriber somewhere to be recorded and migrated FROM, which makes
// migration progress a queryable fact rather than a spreadsheet.
// ---------------------------------------------------------------------------------------

// RegisterWebhookSubscription serves POST
// /subscribers/:subscriber_id/webhook-subscription: it records the legacy HTTP endpoint
// a migrating subscriber used to receive pushes on.
//
// The URL is validated twice over — by the DTO here and again at the persistence
// boundary — because it is an externally supplied destination held inside Blnk's own
// network boundary, and a stored value that nothing validates is one misconfiguration
// away from being fetched. HTTPS is required and internal destinations are refused, so
// the column cannot be used to point Blnk at a metadata service or a private address.
// Nothing sends to it, and per-subscriber HTTP fan-out is deliberately not built.
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
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window during which Kafka publishing and HTTP webhook delivery run side
// by side from the same outbox events. Once the configured retirement instant has
// passed, every request to these routes is answered 410 Gone by the sunset guard. Use
// the Kafka event stream and IssueKafkaCredentials instead; see
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

// GetWebhookSubscription serves GET /subscribers/:subscriber_id/webhook-subscription:
// it reports the legacy endpoint recorded for a subscriber and, when it is set, the
// instant the subscriber finished moving to Kafka.
//
// A nil migration instant means "still counted as awaiting migration", which is exactly
// what migration-progress reporting counts during the window.
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
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka event
// stream and IssueKafkaCredentials instead; see docs/webhook-to-kafka-migration.md.
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
// /subscribers/:subscriber_id/webhook-subscription: it replaces the recorded legacy
// endpoint so a mis-recorded URL can be corrected without a delete and a re-create.
//
// The replacement is held to exactly the same destination policy as the original,
// because a URL that arrives by an edit is stored in the same column and read by the
// same readers. A policy applied only on creation is a policy with an edit-shaped hole.
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
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka event
// stream and IssueKafkaCredentials instead; see docs/webhook-to-kafka-migration.md.
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
// recorded legacy endpoint and stamps the instant the subscriber completed its move to
// Kafka consumption.
//
// The two writes are now one statement inside one repository operation, so the
// transition either happens or does not. It is also idempotent: re-completing an
// already-migrated subscriber moves the timestamp forward rather than failing, so
// repeating this request after any failure is always safe.
//
// The invariant is enforced at every repository write, not only here: recording a URL
// clears migrated_at in the same statement, and MarkSubscriberMigrated refuses to stamp
// a row that still holds one. So no path THROUGH THE API can produce a row that is
// migrated and still carries a live endpoint.
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
// Deprecated: the legacy webhook-subscription surface exists only for the 30-day
// dual-delivery window. Once the configured retirement instant has passed, every
// request to these routes is answered 410 Gone by the sunset guard. Use the Kafka event
// stream and IssueKafkaCredentials instead; see docs/webhook-to-kafka-migration.md.
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
//   - RESOLVED: a one-element array, shaped exactly like a page so a client needs no
//     second parser and no second code path.
//   - NOT IN THE REGISTRY: an empty array and 200. A filter that matches nothing is an
//     empty result rather than a missing resource — the collection exists and was
//     searched to its end.
//   - NOT ESTABLISHED: the resolver reached its bounded page ceiling on a registry
//     larger than that bound, so whether the token names a subscriber is UNKNOWN.
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
			// AN EMPTY PAGE, IN THE SAME ENVELOPE, and a searched-to-the-end total of zero.
			c.JSON(http.StatusOK, completeSubscriberPage(nil))

			return
		}

		respondSubscriberRegistryError(c, err)

		return
	}

	c.JSON(http.StatusOK, completeSubscriberPage(
		[]model.SubscriberResponse{a.subscriberResponse(*subscriber)},
	))
}

// bindStrictManagementJSON decodes a management request body, REFUSING any field the
// shape does not declare, and then runs the binding tags exactly as gin would.
//
// gin's ShouldBindJSON discards unknown keys. encoding/json's default behaviour is the
// same, so a misspelling was accepted and dropped, and the consequence differs per
// route in a way that makes each one worse than a plain no-op:
//
//   - POST /subscribers with "authorised_topics" (the British spelling, or any typo)
//     registered a subscriber authorised for NOTHING.
//   - PUT /subscribers/{id} with every field misspelled decoded to a wholly empty
//     update, which is a legitimate instruction meaning "rewrite the row with its own
//     values".
//   - The webhook-subscription bodies carry one field each, so a misspelling there is
//     refused by binding:"required" — but only by accident of the shape having nothing
//     else in it.
//
// A rejected filter and a rejected body field are the same class of problem, and this
// API already refuses an unknown QUERY parameter for exactly this reason (see
// rejectUnsupportedQueryParameters). The body was the remaining half.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
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

// refuseSubscriberPagingOptions refuses a paging option a non-paging reading of GET
// /subscribers cannot honour.
//
// The pseudonym resolution and the awaiting-revocation scan are not walks. The resolver
// returns at most one row; the scan deliberately covers the WHOLE registry, because a
// partial list of live unaccounted-for credentials reads exactly like a complete one
// and acting on it would leave the rest authenticating while the incident looked closed
// — which is why the service refuses rather than truncating when the registry exceeds
// its bound.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
//   - reading string: the parameter that selected this reading, named in the refusal so
//     the operator can see which two of their parameters disagree.
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

// completeSubscriberPage wraps a COMPLETE result — one this route did not page — in the
// same envelope every other reading of GET /subscribers answers in.
//
// Neither of these readings is a page. The resolver returns at most one row and the
// revocation scan covers the WHOLE registry, so there is nothing to resume from —
// has_more is false and no cursor is emitted, which is exactly what a paging client
// needs to see to stop after one request.
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
// Only "true" selects the scan. An explicit "false" is accepted and means "the ordinary
// page", which is what the parameter's absence already means — carried rather than
// refused so a client building the query from a boolean variable does not have to
// special-case one of its values.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when this
//     returns.
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
// The rows are live SASL/SCRAM credentials at the broker that no registry row records
// an issuance for. A truncated list reads as "these are the ones to revoke", and a
// responder acting on it would revoke those, close the incident, and leave the rest
// authenticating.
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
		items = append(items, a.subscriberResponse(subscriber))
	}

	// THE SAME ENVELOPE as every other reading of this route.
	//
	// The scan covers the WHOLE registry rather than a page, so the total is exact and
	// there is nothing to resume from. See completeSubscriberPage.
	c.JSON(http.StatusOK, completeSubscriberPage(items))
}

// Query-parameter names and refusal messages for the operator lookups that page the
// registry: the pseudonym resolution and the awaiting-revocation filter.
const (
	// subscriberQueryParamPseudonym turns GET /subscribers into the RESOLVER for a
	// subscriber_id_hash token. Named for the response field it resolves, so an operator
	// reading subscriber_id_hash in a log line or on a lag alert can guess the query
	// without consulting the reference.
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
)
