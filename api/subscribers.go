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
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
)

// This file is the HTTP surface of the Kafka subscriber registry. It holds ten
// master-key-gated endpoints in three groups:
//
//	Registry CRUD
//	  POST   /subscribers                                       CreateSubscriber
//	  GET    /subscribers                                       ListSubscribers
//	  GET    /subscribers/:subscriber_id                        GetSubscriber
//	  PUT    /subscribers/:subscriber_id                        UpdateSubscriber
//	  DELETE /subscribers/:subscriber_id                        DeleteSubscriber
//
//	Credential issuance
//	  POST   /subscribers/:subscriber_id/kafka-credentials      IssueKafkaCredentials
//
//	The deprecated legacy webhook-subscription surface
//	  POST   /subscribers/:subscriber_id/webhook-subscription   RegisterWebhookSubscription
//	  GET    /subscribers/:subscriber_id/webhook-subscription   GetWebhookSubscription
//	  PUT    /subscribers/:subscriber_id/webhook-subscription   UpdateWebhookSubscription
//	  DELETE /subscribers/:subscriber_id/webhook-subscription   DeleteWebhookSubscription
//
// # IT TRANSLATES HTTP AND NOTHING ELSE
//
// Every handler below reads a route parameter, binds a request DTO, calls exactly
// one service operation on *blnk.Blnk, and serialises the result through a
// projection in api/model. There is no SQL here, no Kafka client, no SCRAM
// computation, no password generation and no ACL construction. That division is
// load-bearing rather than stylistic:
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
// # NO SUNSET LOGIC LIVES HERE
//
// The four deprecated handlers read no date, call no sunset predicate and compare
// no instants. api/api.go fronts each of them with middleware.WebhookSunsetGuard,
// which asks the root package's single predicate. Two independent comparisons
// could let the service stop dual-writing while still accepting webhook
// management calls, or the reverse, and neither failure raises anything to
// notice.

const (
	// subscriberFilterTable is the table name handed to ParseQueryOptions.
	//
	// filter.GetValidFieldsForTable has no entry for it, so any sort_by
	// normalises to the empty string and the repository's own ordering applies —
	// newest registration first, ties broken by descending id. That is
	// deliberate: paging stability depends on the ordering, and a client-chosen
	// sort column would let a row be shown twice or skipped between pages.
	subscriberFilterTable = "event_subscribers"

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
)

// Query-parameter names, declared once so the readers and any message naming
// them agree by construction.
const (
	subscriberQueryParamLimit        = "limit"
	subscriberQueryParamOffset       = "offset"
	subscriberQueryParamIncludeCount = "include_count"
)

// Client-facing messages, held as constants so that every response carrying one
// is byte-identical. Interpolating the request path or the subscriber id would
// make every body slightly different, which costs log aggregation its grouping
// and invites a client to parse prose when the value to branch on is the
// machine-readable code in error_detail.
const (
	// subscriberIDRequiredMessage mirrors Api.Search's shape for a missing route
	// parameter: what is missing, and where it belongs.
	subscriberIDRequiredMessage = "subscriber_id is required. pass it in the route /subscribers/:subscriber_id"

	// subscriberInvalidLimitMessage and subscriberInvalidOffsetMessage answer a
	// page parameter that is not an integer at all. A value that is merely out of
	// range is normalised, but a non-numeric one is a client mistake and is
	// refused rather than silently defaulted — defaulting it would answer a
	// different question from the one asked, with nothing to say so.
	subscriberInvalidLimitMessage  = "Invalid " + subscriberQueryParamLimit + " value"
	subscriberInvalidOffsetMessage = "Invalid " + subscriberQueryParamOffset + " value"

	// subscriberIncludeCountUnsupportedMessage explains the one list option this
	// endpoint cannot honour. No count of blnk.event_subscribers exists at any
	// layer — the repository exposes paging and no aggregate — and reporting the
	// size of the returned page as total_count would be a falsehood a paging
	// client would loop on forever. So it is refused rather than approximated,
	// which is the position GET /events/dead-letter takes on the same option.
	subscriberIncludeCountUnsupportedMessage = "\"" + subscriberQueryParamIncludeCount +
		"\" is not supported by this endpoint, because no total count of the subscriber registry " +
		"exists; page the registry without one"

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
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - bool: true when the caller holds the master key and the handler may
//     continue; false when the refusal has been written.
func ensureSubscriberManagementAuthorized(c *gin.Context) bool {
	if isMasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errSubscribersRequireMasterKey.Error(), nil)

	return false
}

// subscriberIDFromRoute reads the subscriber identifier out of the route, writing
// the refusal itself when it is absent or blank.
//
// A blank-but-present identifier is refused here rather than passed on, so the
// answer is "required" instead of the misleading "subscriber not found" an empty
// WHERE match would produce. The value is returned trimmed because the service
// trims it too, and returning the untrimmed form would let this layer and the
// service disagree about which row was addressed.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - string: the trimmed identifier.
//   - bool: false when the refusal has been written.
func subscriberIDFromRoute(c *gin.Context) (string, bool) {
	subscriberID, passed := c.Params.Get(subscriberRouteParam)
	if trimmed := strings.TrimSpace(subscriberID); passed && trimmed != "" {
		return trimmed, true
	}

	respondCode(c, apierror.ErrGenMissingParameter, subscriberIDRequiredMessage, nil)

	return "", false
}

// subscriberPageLimitFromQuery reads limit and normalises it exactly as
// ParseFiltersFromBody does: absent, non-positive or above the ceiling all become
// the default. A value that is not an integer is refused instead.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - int: the normalised limit.
//   - bool: false when the refusal has been written.
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

// subscriberPageOffsetFromQuery reads offset and clamps a negative value to zero,
// matching ParseFiltersFromBody. A non-integer value is refused for the same
// reason as in subscriberPageLimitFromQuery.
//
// Parameters:
//   - c *gin.Context: the request. On refusal the response is already written when
//     this returns.
//
// Returns:
//   - int: the normalised offset.
//   - bool: false when the refusal has been written.
func subscriberPageOffsetFromQuery(c *gin.Context) (int, bool) {
	raw := strings.TrimSpace(c.Query(subscriberQueryParamOffset))
	if raw == "" {
		return 0, true
	}

	offset, err := strconv.Atoi(raw)
	if err != nil {
		respondCode(c, apierror.ErrGenValidation, subscriberInvalidOffsetMessage, nil)

		return 0, false
	}

	if offset < 0 {
		offset = 0
	}

	return offset, true
}

// optionalSubscriberField converts a request DTO's plain string into the nilable
// form the service and the nullable columns use.
//
// A blank value becomes nil rather than a pointer to "", because for both columns
// this is applied to — partition_key_prefix and webhook_url — NULL means "not
// recorded" while the empty string would mean "recorded as empty", and those are
// different claims. The create DTO cannot express the second, so a blank field
// there is always the first.
//
// Parameters:
//   - value string: the field as bound.
//
// Returns:
//   - *string: nil when the value is blank after trimming, otherwise a pointer to
//     the value as supplied.
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
//
// Parameters:
//   - c *gin.Context: the response.
//   - status int: the success status to write.
//   - subscriber *coremodel.EventSubscriber: the stored row. A nil row is
//     answered as an internal error rather than dereferenced.
func respondSubscriber(c *gin.Context, status int, subscriber *coremodel.EventSubscriber) {
	if subscriber == nil {
		respondCode(c, apierror.ErrGenInternal, subscriberMissingRowMessage, nil)

		return
	}

	c.JSON(status, model.NewSubscriberResponse(*subscriber))
}

// respondWebhookSubscription writes the legacy webhook-subscription read shape.
//
// It carries the recorded URL and the migration instant and nothing else. The
// legacy transport's signing secret and configured headers are deployment-wide
// configuration rather than per-subscriber data, so surfacing them here would
// turn a migration-tracking response into a secret-bearing one.
//
// Deprecated: this projection belongs to the legacy webhook-subscription surface
// and is removed with it once the webhook retirement instant has passed. The
// marker is not decoration — it is what keeps the deprecated response DTO's own
// deprecation from being reported as a defect at every call site inside the
// surface being retired, and it makes this helper impossible to reuse from a
// route that is meant to outlive the retirement.
//
// Parameters:
//   - c *gin.Context: the response.
//   - status int: the success status to write.
//   - subscriber *coremodel.EventSubscriber: the stored row. A nil row is
//     answered as an internal error rather than dereferenced.
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
//
// Parameters:
//   - c *gin.Context: the response.
//   - err error: the failure as returned by the service.
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
//
// Parameters:
//   - err error: the error to inspect. May be nil.
//
// Returns:
//   - bool: true when err is, or wraps, an apierror.APIError.
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
// the broker refused the credential or its bindings, SUBSCRIBER_GRANT_EMPTY and
// SUBSCRIBER_ISOLATION_UNENFORCEABLE for a row that cannot be provisioned as it
// stands, SUBSCRIBER_PROVISIONING_TIMEOUT when the registry itself ran out of
// budget, and a typed conflict when a concurrent issuance superseded this one.
// Downgrading any of those to a single generic code would take information away
// from the operator who has to act on it.
//
// Only when the failure carries NO code of its own is the context consulted, and
// then an expired or cancelled issuance is answered with
// SUBSCRIBER_PROVISIONING_FAILED — 503, retryable — rather than being allowed to
// fall through to a bare 500. That is the guarantee the endpoint's budget is for:
// the request always ends within the budget, and it ends with an answer that says
// whether retrying is sensible.
//
// Anything else unclassified defaults to SUBSCRIBER_PROVISIONING_FAILED for the
// same reason: at this point the request has already passed the gate, the
// parameter and the lookup, so a failure without a code is a dependency failure
// rather than a client mistake.
//
// NO CREDENTIAL MATERIAL REACHES ANY BRANCH BELOW. The service returns a
// zero-valued credential on every error path, and no message or details payload
// assembled here reads from one.
//
// Parameters:
//   - c *gin.Context: the response.
//   - issuance context.Context: the budgeted context the service ran under,
//     consulted only to distinguish an expiry from a cancellation.
//   - err error: the failure as returned by the service.
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

// subscriberTopicPrefix reports the configured Kafka topic namespace, which the
// request DTOs need in order to tell a Blnk-owned topic from any other name.
//
// It is read live from the configuration store on every request rather than
// captured once, because the store's contents are replaced wholesale and a value
// captured at construction time would describe the deployment as it stood when
// the router was assembled. A blank result is a legitimate answer and makes the
// DTOs validate against model.DefaultEventTopicPrefix, which is the strictest
// available namespace rather than a permissive one.
//
// Returns:
//   - string: the configured KAFKA_TOPIC_PREFIX, or "" when none is configured.
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
//	400 GEN_MALFORMED_REQUEST for an unbindable body
//	400 GEN_VALIDATION_ERROR for a body the DTO refuses — a missing name, a topic
//	    outside the deployment's grantable set, an unusable key scope or an
//	    unacceptable legacy URL
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	409 GEN_CONFLICT when the identifier or the derived principal is already taken
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) CreateSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	var req model.CreateSubscriber
	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

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
		// Both nullable columns take the nilable form, so "not recorded" stays
		// distinguishable from "recorded as empty" all the way to the row.
		PartitionKeyPrefix: optionalSubscriberField(req.PartitionKeyPrefix),
		WebhookURL:         optionalSubscriberField(req.WebhookURL),
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
// # Paging and ordering
//
// limit and offset follow this package's uniform convention: absent, non-positive
// or oversized limits become 20, a negative offset becomes 0, and a value that is
// not an integer at all is refused rather than silently defaulted. Ordering is
// FIXED by the repository at newest registration first, ties broken by descending
// id, which is what makes paging stable; sort_by and sort_order are accepted for
// client compatibility and do not change it.
//
// include_count is REFUSED rather than ignored. No count of the registry exists at
// any layer, and returning the size of the page as a total would be a falsehood a
// paging client would loop on forever. Quietly dropping the option would hand the
// caller a response that answers a different question with nothing to say so.
//
// An empty registry is 200 and `[]` — never 404 and never `null`. "No subscribers
// are registered" is a successful answer, and a migration-progress script must be
// able to range over the result unconditionally.
//
// Every entry is projected by model.NewSubscriberResponse, so no entry carries a
// credential reference, only its fingerprint.
//
// # Responses
//
//	200 a JSON array of subscribers, possibly empty
//	400 GEN_VALIDATION_ERROR for a non-integer page parameter or include_count
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) ListSubscribers(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	limit, ok := subscriberPageLimitFromQuery(c)
	if !ok {
		return
	}

	offset, ok := subscriberPageOffsetFromQuery(c)
	if !ok {
		return
	}

	// ParseQueryOptions is this package's reader for the GET-side sort and count
	// options, and it is used here rather than reimplemented so that a sort field
	// this table does not publish is coerced the same way it is everywhere else.
	// Only IncludeCount is actionable; see the ordering note above.
	if ParseQueryOptions(c, subscriberFilterTable).IncludeCount {
		respondCode(c, apierror.ErrGenValidation, subscriberIncludeCountUnsupportedMessage, nil)

		return
	}

	subscribers, err := a.blnk.ListEventSubscribers(c.Request.Context(), limit, offset)
	if err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	// Allocated with make so an empty page marshals as [] rather than null.
	items := make([]model.SubscriberResponse, 0, len(subscribers))
	for _, subscriber := range subscribers {
		items = append(items, model.NewSubscriberResponse(subscriber))
	}

	c.JSON(http.StatusOK, items)
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
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
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
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a body the DTO refuses
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_ISOLATION_UNENFORCEABLE when a key scope would be recorded on a
//	    subscriber that already holds a credential, or GEN_CONFLICT when a
//	    concurrent issuance holds the provisioning fence
//	503 SUBSCRIBER_PROVISIONING_FAILED when the broker-side reconciliation did not
//	    complete
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) UpdateSubscriber(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	var req model.UpdateSubscriber
	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

		return
	}

	if err := req.Validate(a.subscriberTopicPrefix()); err != nil {
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)

		return
	}

	// Passed through in their nilable form without normalisation, because the
	// nilability IS the instruction: collapsing a present empty string to nil here
	// would silently turn "clear this" into "leave it alone".
	subscriber, err := a.blnk.UpdateEventSubscriber(c.Request.Context(), subscriberID, blnk.SubscriberUpdate{
		Name:               req.Name,
		AuthorizedTopics:   req.AuthorizedTopics,
		PartitionKeyPrefix: req.PartitionKeyPrefix,
		WebhookURL:         req.WebhookURL,
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
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_DEPROVISIONING or GEN_CONFLICT when a concurrent operation holds
//	    the provisioning fence
//	503 SUBSCRIBER_PROVISIONING_FAILED when the broker-side revocation did not
//	    complete
//	500 GEN_INTERNAL for a repository failure
//
// Parameters:
//   - c *gin.Context: the request and response.
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
// # The five-second budget is enforced here, in code
//
// The service applies the same ceiling internally, and this handler applies it
// again at the boundary so that the guarantee holds for the HTTP request whatever
// a future service refactor does with its own timeout. context.WithTimeout only
// ever shortens, so the two cannot conflict and a caller whose own context expires
// sooner still wins. blnk.SubscriberCredentialIssuanceBudget is used rather than a
// literal duration so the endpoint's ceiling cannot drift from the operation's.
//
// An expiry is answered, not waited out: see respondCredentialIssuanceError. The
// request therefore always ends within the budget and always ends with a code that
// says whether retrying is sensible — never a hang and never a bare 500.
//
// # The boundary the response describes
//
// The credential grants topic-level Read and Describe on the authorised topics
// plus Read on the subscriber's prefixed consumer-group namespace, and nothing
// further. WITHIN an authorised topic there is no restriction at all: Kafka
// authorises at topic and group granularity and has no message-key dimension, so
// the response carries no partition-key prefix and its enforced-access
// declaration — assembled by model.NewSubscriberEnforcedAccess, never here —
// states outright that key filtering is not enforced. Dead-letter topics are never
// grantable, so no "<topic>.dlt" can appear in the list.
//
// # Responses
//
//	200 the credential and the connection details
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for an identifier no Kafka identity can be derived from
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	409 SUBSCRIBER_GRANT_EMPTY for a subscriber authorised for no topics,
//	    SUBSCRIBER_ISOLATION_UNENFORCEABLE for one carrying a key scope Kafka
//	    cannot enforce, GEN_CONFLICT when a concurrent issuance superseded this one
//	503 EVENT_KAFKA_UNAVAILABLE when no broker is configured or reachable,
//	    SUBSCRIBER_BROKERS_NOT_CONFIGURED when no subscriber-facing endpoint is
//	    published, SUBSCRIBER_PROVISIONING_FAILED when the broker refused the
//	    credential or its bindings
//	504 SUBSCRIBER_PROVISIONING_TIMEOUT when the registry ran out of budget
//
// Parameters:
//   - c *gin.Context: the request and response.
func (a *Api) IssueKafkaCredentials(c *gin.Context) {
	if !ensureSubscriberManagementAuthorized(c) {
		return
	}

	subscriberID, ok := subscriberIDFromRoute(c)
	if !ok {
		return
	}

	// THE BUDGET. Everything the service does under it — the lookup, the
	// provisioning fence, up to four broker round trips and the issuance record —
	// shares this one deadline.
	ctx, cancel := context.WithTimeout(c.Request.Context(), blnk.SubscriberCredentialIssuanceBudget)
	defer cancel()

	credential, err := a.blnk.IssueSubscriberKafkaCredentials(ctx, subscriberID)
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
		EnforcedAccess: model.NewSubscriberEnforcedAccess(credential.SubscriberID, authorizedTopics),
		Username:       credential.Username,
		// THE ONE AND ONLY READ OF THE PLAINTEXT SECRET IN THIS PACKAGE. It goes
		// straight into the response body and is not held, copied or logged.
		Password:  credential.Password(),
		Mechanism: credential.Mechanism,
		IssuedAt:  credential.IssuedAt,
	})
}

// ---------------------------------------------------------------------------------------
// The deprecated legacy webhook-subscription surface
//
// # Why these four routes exist, so they are not removed as dead code
//
// The requirement to migrate existing subscribers off "the webhook subscription
// REST API" meets a repository in which no such API exists: the entire
// subscription surface today is one deployment-wide configuration value,
// Notification.Webhook, consumed by the legacy delivery path. There is no
// registration endpoint and no per-subscriber URL storage anywhere.
//
// Recording a legacy URL per subscriber gives an already-webhooked subscriber
// somewhere to be recorded and migrated FROM, which makes migration progress a
// queryable fact rather than a spreadsheet. It is also what makes the sunset
// OBSERVABLE: without a subscription route there is no request on which a 410
// could ever be seen, and the retirement criterion would have nothing to assert
// against.
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
// notification. They stay fully functional, they keep answering normally after the
// retirement instant, and no guard is attached to them. They also share
// infrastructure with the transport being retired: the hook manager enqueues its
// work onto the webhook asynq queue by name, which is why that queue outlives the
// transport. Conflating the two would break a live, supported feature that merely
// shares a word with the one being removed.
//
// # NO SUNSET LOGIC IS PRESENT BELOW
//
// None of the four handlers reads the configured retirement instant, calls a
// sunset predicate or compares a date. api/api.go fronts each route with
// middleware.WebhookSunsetGuard, which asks the single predicate in the root
// package. Before the retirement instant the guard is transparent and these
// handlers answer exactly as written; after it, they are unreachable and every
// request is answered 410 GEN_GONE. One decision, two consumers — a second
// comparison here could let the service stop dual-writing while still accepting
// these calls, or the reverse, and neither failure raises anything to notice.
// ---------------------------------------------------------------------------------------

// RegisterWebhookSubscription serves POST
// /subscribers/:subscriber_id/webhook-subscription: it records the legacy HTTP
// endpoint a migrating subscriber used to receive pushes on.
//
// The URL is validated twice over — by the DTO here and again at the persistence
// boundary — because the column is a FUTURE REQUEST SINK: nothing sends to it
// today, and the moment anything did, whatever is stored would become a request
// Blnk makes from inside its own network. HTTPS is required and internal
// destinations are refused.
//
// # Responses
//
//	201 the recorded subscription
//	400 GEN_MALFORMED_REQUEST for an unbindable body or a missing webhook_url
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a URL the destination policy refuses
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
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
	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

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
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
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
// # Responses
//
//	200 the recorded subscription as it now stands
//	400 GEN_MALFORMED_REQUEST for an unbindable body or a missing webhook_url
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	400 GEN_VALIDATION_ERROR for a URL the destination policy refuses
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
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
	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)

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
// # Why the endpoint clears first and stamps second
//
// There is no transaction spanning the two writes, so one ordering has to be
// chosen and the choice decides what a partial failure looks like:
//
//   - CLEAR then STAMP — this one. A failure after the clear leaves the URL gone
//     and migrated_at unset, so the row still reports the subscriber as awaiting
//     migration and repeating the same request completes the operation. The record
//     never over-claims progress.
//   - STAMP then clear would leave a subscriber reported as migrated while its
//     legacy endpoint was still recorded, which is precisely the state
//     migration-progress reporting is meant to rule out, and nothing would fail to
//     say so.
//
// migrated_at is stamped through the service's migration-progress operation rather
// than being written as part of the clear, because the clear deliberately leaves it
// alone: it is an audit fact rather than third-party data, it is never purged, and
// an operator correcting a mis-recorded URL must be able to clear one without
// asserting that a migration happened.
//
// # Responses
//
//	204 the recorded subscription is forgotten and the migration instant stamped
//	400 GEN_MISSING_PARAMETER for a blank identifier
//	403 AUTH_MASTER_KEY_REQUIRED for a non-master caller
//	404 SUBSCRIBER_NOT_FOUND when no such subscriber exists
//	410 GEN_GONE once the webhook retirement instant has passed, written by the
//	    per-route sunset guard before this handler is reached
//	500 GEN_INTERNAL for a repository failure
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

	requestContext := c.Request.Context()

	// STEP 1 — CLEAR. The updated row is dropped because 204 carries no body.
	if _, err := a.blnk.ClearSubscriberWebhookSubscription(requestContext, subscriberID); err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	// STEP 2 — STAMP. The instant is returned so a caller can report it without
	// re-reading the row; this endpoint has no body to report it in, so it is
	// dropped. The error is not: a stamp that did not land must not be reported as
	// a completed migration.
	if _, err := a.blnk.MarkEventSubscriberMigrated(requestContext, subscriberID); err != nil {
		respondSubscriberRegistryError(c, err)

		return
	}

	c.Status(http.StatusNoContent)
}
