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
	authz "github.com/blnkfinance/blnk/api/middleware"
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
// It resolves the credential through authz.MasterKeyRequest rather than reading the
// context flag directly, for the reason ensureEventManagementAuthorized sets out: with
// secure mode off nothing sets that flag, so the whole surface refused the very credential
// its documentation names. The refusal is unchanged for a caller that does not present it.
func ensureSubscriberManagementAuthorized(c *gin.Context) bool {
	if authz.MasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errSubscribersRequireMasterKey.Error(), nil)

	return false
}

// ensureCredentialTransportConfidential refuses credential issuance unless the request
// arrived over a channel this deployment has ESTABLISHED as confidential, writing the
// refusal itself when it did not.
func (a *Api) ensureCredentialTransportConfidential(c *gin.Context) bool {
	if a.credentialTransportConfidential(c) {
		return true
	}

	respondCode(c, apierror.ErrSubscriberInsecureTransport, credentialInsecureTransportMessage, nil)

	return false
}

// credentialTransportConfidential reports whether the request's channel is one of the
// three confidential ones ensureCredentialTransportConfidential documents.
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
func (a *Api) respondSubscriber(c *gin.Context, status int, subscriber *coremodel.EventSubscriber) {
	if subscriber == nil {
		respondCode(c, apierror.ErrGenInternal, subscriberMissingRowMessage, nil)

		return
	}

	c.JSON(status, a.subscriberResponse(*subscriber))
}

// subscriberAccessDeployment resolves the configuration facts a subscriber body has to
// be truthful about, once per response.
func (a *Api) subscriberAccessDeployment() coremodel.SubscriberAccessDeployment {
	return blnk.SubscriberAccessDeployment()
}

// subscriberResponse projects a stored row and then applies the ONE post-sunset
// adjustment the projection itself cannot make.
func (a *Api) subscriberResponse(subscriber coremodel.EventSubscriber) model.SubscriberResponse {
	// NOTHING TO SUPPRESS HERE, and that is the stronger position. The registry-read shape
	// does not carry the legacy webhook URL AT ALL — see model.NewSubscriberResponse — so
	// a third-party endpoint cannot leak through this route either before or after the
	// retirement instant. The URL is readable only from the dedicated webhook-subscription
	// route, which the sunset guard retires outright.
	return model.NewSubscriberResponse(subscriber, a.subscriberAccessDeployment())
}

// respondWebhookSubscription writes the legacy webhook-subscription read shape.
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
func respondSubscriberRegistryError(c *gin.Context, err error) {
	respondError(c, err,
		withUpgrade(apierror.ErrGenNotFound, apierror.ErrSubscriberNotFound),
		withDefault(apierror.ErrGenInternal),
	)
}

// isTypedAPIError reports whether err carries a catalog code of its own.
func isTypedAPIError(err error) bool {
	var typed apierror.APIError

	return errors.As(err, &typed)
}

// respondCredentialIssuanceError writes the failure of a credential issuance.
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
		CredentialFingerprint: credential.Fingerprint,
		Replaced:              credential.Replaced,
	})
}

// ---------------------------------------------------------------------------------------
// The deprecated legacy webhook-subscription surface

// RegisterWebhookSubscription serves POST
// /subscribers/:subscriber_id/webhook-subscription: it records the legacy HTTP endpoint
// a migrating subscriber used to receive pushes on.
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
func subscriberPseudonymFromQuery(c *gin.Context) (string, bool) {
	raw, present := c.GetQuery(subscriberQueryParamPseudonym)

	return strings.TrimSpace(raw), present
}

// resolveSubscriberPseudonym answers a subscriber_id_hash resolution.
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
func completeSubscriberPage(items []model.SubscriberResponse) SubscriberPageResponse {
	if items == nil {
		// [] rather than null, so a script can range over .data[] unconditionally.
		items = []model.SubscriberResponse{}
	}

	total := int64(len(items))

	return SubscriberPageResponse{Data: items, HasMore: false, TotalCount: &total}
}

// subscriberRevocationFilterFromQuery reads the revocation_pending filter.
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
