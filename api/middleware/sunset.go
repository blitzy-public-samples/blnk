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

package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
)

// This file is the HTTP half of the legacy webhook retirement.
//
// Retiring the transport has two observable behaviours — the relay stops enqueueing
// legacy delivery work, and the deprecated webhook-subscription management routes stop
// answering — and they read ONE resolved retirement window, owned by the root package's
// event_sunset.go. They ask different questions of it because they sit at different
// boundaries: the relay asks WebhookDualDeliveryActive, true only inside
// [start, sunset), while this guard asks WebhookSunsetPassed, which turns on the sunset
// instant alone. Delivery has two ends to respect; a 410 has one.
//
// Neither compares a date itself, and that is what makes them unable to disagree. A
// duplicated comparison could leave the service accepting webhook management calls
// after it had stopped delivering webhooks, or refusing them while deliveries were
// still going out — neither of which raises anything to notice.

// The advisory response headers the guard advertises whenever a retirement instant is
// configured, on both sides of the boundary.
//
// THEY COME FROM TWO DIFFERENT RFCs AND USE TWO DIFFERENT DATE FORMATS, which is the
// detail most easily got wrong here:
//
//   - Sunset is RFC 8594. It carries the instant at which the routes stop responding,
//     as an HTTP-date — the IMF-fixdate that http.TimeFormat renders.
//   - Deprecation is RFC 9745. Its value MUST be a Structured Fields Date (RFC 9651
//     §3.3.7): an "@" followed by integer seconds since the Unix epoch, for example
//     "@1688169599". The boolean "true" this once carried is NOT valid syntax under
//     that specification, so a conforming client parsing it sees a malformed field
//     and is entitled to discard the whole header — losing the notice entirely.
//
// The instant each carries is different too, and deliberately so. Deprecation carries
// the moment the surface BECAME deprecated, which is when the dual-delivery window
// opened; Sunset carries the moment it stops answering. RFC 9745 §4 requires the
// Sunset timestamp not to precede the Deprecation timestamp, and that holds by
// construction: the window start is exactly config.WebhookDualDeliveryWindowDays
// before the sunset, and both come from one resolution in event_sunset.go.
//
// Both are purely informational. The refusal below is never a function of either.
const (
	webhookSunsetHeader      = "Sunset"
	webhookDeprecationHeader = "Deprecation"

	// The RFC 9651 §3.3.7 sf-date sigil. A Date is serialised as this byte followed by
	// the integer seconds, and nothing else — no quoting, no sub-second part.
	structuredFieldDatePrefix = "@"
)

// DeprecatedWebhookSubscriptionRoute is the ONE registered gin route template the webhook
// retirement covers, and DeprecatedWebhookSubscriptionMethods are the only methods
// registered on it.
//
// They are declared here, beside the guard, rather than restated in api/api.go, so the
// pre-authentication guard and the per-route guard cannot come to cover different sets. A
// route added to api/api.go and not added here is simply not retired; a template written
// here that api/api.go never registers matches nothing, and
// TestWebhookSunset_PreAuthGuardMatchesExactlyTheDeprecatedRoutes proves both directions
// against the live route table rather than against this comment.
const DeprecatedWebhookSubscriptionRoute = "/subscribers/:subscriber_id/webhook-subscription"

// DeprecatedWebhookSubscriptionMethods is the method set. It is a set rather than a slice so
// the membership test is exact and order-free.
var DeprecatedWebhookSubscriptionMethods = map[string]struct{}{
	http.MethodPost:   {},
	http.MethodGet:    {},
	http.MethodPut:    {},
	http.MethodDelete: {},
}

// IsDeprecatedWebhookSubscriptionRequest reports whether a request addresses one of the four
// deprecated legacy webhook-subscription management routes.
//
// It takes the MATCHED ROUTE TEMPLATE — gin's c.FullPath() — and not the raw request path.
// Matching the raw path would mean re-implementing gin's parameter matching here, and a
// prefix or substring test on it would be a retirement that spreads: "/subscribers/x/webhook-subscription/anything"
// is a different (unregistered) route, and a path-prefix test would retire it too. The
// template is what routing already decided, so it is exact by construction. An unmatched
// request has an empty template and is therefore never deprecated, which is correct: gin
// answers it 404 or 405 and there is no deprecated handler behind it to shield.
//
// Parameters:
//   - fullPath string: the matched route template, from gin.Context.FullPath().
//   - method string: the request method.
//
// Returns:
//   - bool: true only for the four deprecated method-and-template combinations.
func IsDeprecatedWebhookSubscriptionRequest(fullPath, method string) bool {
	if fullPath != DeprecatedWebhookSubscriptionRoute {
		return false
	}

	_, deprecated := DeprecatedWebhookSubscriptionMethods[method]

	return deprecated
}

// requestAddressesRetiredWebhookSurface reports whether THIS request addresses the retired
// webhook-subscription surface, by whichever of the two available facts applies.
//
// # Why two facts and not one
//
// The matched route template is exact by construction and is the right answer whenever there is
// one: it is what gin's own parameter matching decided, so no path parsing here can disagree
// with routing. But a request that matches NO ROUTE has an empty template — an unsupported verb
// on the retired path (PATCH, HEAD, OPTIONS, TRACE, anything), and a path with no handler at all
// — and those are exactly the requests requirement R-12 means by "every request". Answered from
// the template alone they fall through to gin's own 404 or 405: "no such thing", or "wrong verb,
// try another", about a surface that has been retired, which sends a straggler
// looking for a verb that will never work.
//
// So: the template when there is one, and the concrete request path when there is not. The
// fallback is a strict THREE-SEGMENT shape test, not a prefix test — see
// isDeprecatedWebhookSubscriptionPath — so /subscribers/x/webhook-subscription/anything, a
// different and unregistered route, is not retired by association.
//
// The method is only consulted on the template branch. On the fallback branch the verb is
// precisely what did not match, so requiring it to be one of the four would defeat the purpose.
//
// Parameters:
//   - c *gin.Context: the request in flight.
//
// Returns:
//   - bool: true when the request addresses the retired surface, by either reading.
func requestAddressesRetiredWebhookSurface(c *gin.Context) bool {
	if template := c.FullPath(); template != "" {
		return IsDeprecatedWebhookSubscriptionRequest(template, c.Request.Method)
	}

	if c.Request == nil || c.Request.URL == nil {
		return false
	}

	return isDeprecatedWebhookSubscriptionPath(c.Request.URL.Path)
}

// WebhookSunsetPreAuthGuard answers the four deprecated webhook-subscription routes with 410
// Gone BEFORE authentication runs, and lets every other request through untouched.
//
// # The defect it closes (AAP-06)
//
// Requirement R-12 is that after the retirement instant "the webhook REST API returns 410 Gone
// on EVERY request". The per-route WebhookSunsetGuard cannot deliver that on its own, because
// api/api.go installs Authenticate() with router.Use and gin runs global middleware ahead of
// per-route middleware. A request carrying a missing, malformed, revoked or wrongly-scoped key
// was therefore refused 401 or 403 and never reached the guard — so the retirement was
// observable only by callers who were still correctly authenticated, which is precisely the
// population least likely to be the stragglers a retirement notice is for. A client whose
// credential had also lapsed was told to fix its key, indefinitely, for a surface that no
// longer exists.
//
// # Why answering before authentication is safe here
//
// 410 on these routes discloses nothing an unauthenticated caller could not already read in
// the published migration guide: that one fixed, documented management surface has been
// removed. The response is IDENTICAL for every caller and every subscriber id — the guard
// never looks at the parameter, never touches the registry, and cannot therefore reveal
// whether a subscriber exists. Nothing is enumerable through it that is not enumerable through
// the documentation.
//
// It is also the narrowest possible pre-authentication surface: one route template and four
// methods, tested against the router's own table. Every other route in the API — /hooks above
// all, which is a live, supported feature that merely shares a word with the transport being
// retired — reaches Authenticate() exactly as before, in the same order, with the same result.
//
// # It does not replace the per-route guard
//
// Both are installed. This one exists to reach unauthenticated requests; the per-route guard
// remains the retirement's local, visible statement at each route registration and would still
// refuse if this middleware were ever removed from the chain. Two independent barriers for a
// behaviour whose failure mode is "the routes quietly keep working" is deliberate, and they
// cannot disagree: both ask blnk.WebhookSunsetPassed, the single decision point.
//
// Returns:
//   - gin.HandlerFunc: global middleware that aborts with apierror.ErrGenGone for the four
//     deprecated requests once the retirement instant has passed, and is otherwise a no-op.
func WebhookSunsetPreAuthGuard() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !requestAddressesRetiredWebhookSurface(c) {
			// NOT a deprecated request. Nothing is added — not even the advisory headers —
			// because this middleware runs on every request in the API and a Sunset header
			// on /transactions would announce the retirement of something that is not being
			// retired.
			c.Next()

			return
		}

		if deprecated, sunset, configured := blnk.WebhookDeprecationWindow(); configured {
			// Both headers are rendered from ONE resolution of the window, so the pair
			// cannot describe windows that disagree and the ordering RFC 9745 §4 requires
			// holds by construction — exactly as the per-route guard behind this one does
			// it, because a caller must not be told two different things about the same
			// retirement depending on which guard answered.
			c.Header(webhookSunsetHeader, sunset.UTC().Format(http.TimeFormat))
			c.Header(webhookDeprecationHeader, formatDeprecationDate(deprecated))
		}

		if blnk.WebhookSunsetPassed(time.Now()) {
			abortWithCode(c, apierror.ErrGenGone, webhookSunsetGoneMessage)

			return
		}

		// Inside the window the request continues into Authenticate() and then into the
		// per-route guard, which re-asks the same predicate and adds the same headers. The
		// duplicate header write is idempotent — c.Header replaces rather than appends — and
		// the duplicate predicate call is one map read of a live configuration snapshot.
		c.Next()
	}
}

// formatDeprecationDate renders an instant as an RFC 9651 Structured Fields Date, the
// form RFC 9745 requires of the Deprecation field.
//
// Seconds are TRUNCATED toward the epoch by time.Unix's own definition rather than
// rounded, so the rendered value never describes an instant later than the one supplied.
// That matters for exactly one reason: rounding up could push the deprecation instant
// past a sunset configured with sub-second precision, producing the ordering RFC 9745 §4
// forbids from a window that is in fact correct.
//
// Parameters:
//   - instant time.Time: the moment to render. Any location; the epoch value is absolute.
//
// Returns:
//   - string: the field value, for example "@1688169599".
//
// DeprecationHeaderValue is the exported rendering of the Deprecation field value, for tests
// and for any caller that must predict exactly what the guards emit.
//
// It exists so a test asserts the CONTRACT rather than restating the format: a test that spelled
// "@" + strconv.Itoa(...) itself would keep passing if the renderer stopped complying, which is
// the one thing worth guarding here. RFC 9745 requires an RFC 9651 §3.3.7 sf-date and the literal
// "true" this field once carried satisfies neither its syntax nor its meaning.
//
// Parameters:
//   - instant time.Time: the window's opening instant.
//
// Returns:
//   - string: the field value, for example "@1688169599".
func DeprecationHeaderValue(instant time.Time) string {
	return formatDeprecationDate(instant)
}

func formatDeprecationDate(instant time.Time) string {
	return structuredFieldDatePrefix + strconv.FormatInt(instant.Unix(), 10)
}

// webhookSunsetGoneMessage is the single, stable explanation returned once the legacy
// webhook surface has been retired.
//
// It is a constant rather than a formatted string deliberately. Interpolating the
// request path, the subscriber id or the configured instant would make every body
// slightly different — which costs log aggregation its grouping, and invites a client
// to parse prose when the value a client should branch on is the machine-readable code
// carried in error_detail. The wording matches what the deprecated request and response
// shapes in api/model/event.go already publish, so a subscriber reading the type
// documentation and a subscriber reading a refusal are told the same thing.
//
// It says "retired", not "removed". The routes and their handlers are still registered
// and still compiled after the sunset — the date withdraws the behaviour, and deleting
// the surface is a separate, later release — so a body claiming the endpoint no longer
// exists would describe a state the deployment is not in.
const webhookSunsetGoneMessage = "Webhook subscription management has been retired and is no " +
	"longer available. Use the Kafka event stream and the subscriber credential endpoint " +
	"instead; see the webhook-to-Kafka migration guide under docs/."

// deprecatedWebhookSubscriptionSegments is the path shape of the retired surface:
// /subscribers/{subscriber_id}/webhook-subscription.
//
// The first and last segments are matched literally and the middle one is the
// subscriber id, whatever it happens to be. Matching a shape rather than a list of
// concrete paths is what lets one guard cover every subscriber without the router and
// this file having to agree on a set of ids.
const (
	deprecatedWebhookSubscriptionPathSegments = 3
	deprecatedWebhookSubscriptionRoot         = "subscribers"
	deprecatedWebhookSubscriptionLeaf         = "webhook-subscription"
)

// IsDeprecatedWebhookSubscriptionPath is the exported form of the matcher, for callers
// outside this package that need to recognise the retired surface — a test asserting the
// boundary, and any future router wiring that must answer for it.
//
// Parameters:
//   - path string: the request path.
//
// Returns:
//   - bool: true when the path addresses the retired webhook-subscription surface.
func IsDeprecatedWebhookSubscriptionPath(path string) bool {
	return isDeprecatedWebhookSubscriptionPath(path)
}

// isDeprecatedWebhookSubscriptionPath reports whether a request path addresses the
// retired webhook-subscription surface.
//
// It is deliberately an exact structural match — exactly three segments, the first and
// third literal — rather than a prefix or substring test. A prefix test on
// "/subscribers/" would retire the live subscriber registry and the credential
// endpoint along with the webhook surface, and a substring test on
// "webhook-subscription" would retire anything that merely embedded the word. Both
// would turn this guard from a retirement into an outage, which is the single most
// damaging way it could fail.
//
// Two decisions here are worth stating because both look like oversights:
//
//   - The comparison is case-sensitive, matching Gin's own routing. "/Subscribers/..."
//     never named a route in this API, so it answered 404 before the retirement; it
//     should answer 404 after it too. Matching case-insensitively would invent a Gone
//     resource that never existed.
//   - Exactly one trailing slash is tolerated. Gin redirects "/a/b/" to "/a/b" before
//     any handler runs when the latter is registered, so the tolerance is not what
//     handles the common case; it is here so that after the routes are deleted — when
//     there is no registered path left to redirect to and the redirect therefore stops
//     happening — the slashed form is still recognised as the retired surface rather
//     than decaying into a 404.
//
// Parameters:
//   - path: the request path, already percent-decoded, as Gin itself routes on.
//
// Returns:
//   - bool: true when the path addresses the retired webhook-subscription surface.
func isDeprecatedWebhookSubscriptionPath(path string) bool {
	if len(path) > 1 {
		path = strings.TrimSuffix(path, "/")
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) != deprecatedWebhookSubscriptionPathSegments {
		return false
	}

	// The subscriber id is not validated, only required to be present. A malformed id
	// on a retired surface is still a request to a retired surface, and answering it
	// with anything other than Gone would leak the fact that the route is gone behind
	// a validation error.
	return segments[0] == deprecatedWebhookSubscriptionRoot &&
		segments[1] != "" &&
		segments[2] == deprecatedWebhookSubscriptionLeaf
}

// WebhookSunsetGuard refuses requests to the deprecated legacy webhook-subscription
// management routes once the webhook retirement instant has passed. Before it, the guard
// is transparent: it adds the advisory headers and hands the request on, so the routes
// answer exactly as they did before it was attached.
//
// # Scope, and why /hooks is excluded
//
// The retired surface is /subscribers/{subscriber_id}/webhook-subscription, whose
// POST, GET, PUT and DELETE forms api/api.go registers for the dual-delivery window.
//
// # Why this is installed globally, and must run before authentication
//
// An earlier form of this guard was attached per route and inspected neither the path
// nor the method, on the reasoning that routing had already decided which requests
// reach it. That reasoning does not hold, and it left the retirement incomplete in two
// ways that no error surfaces:
//
//   - Authentication is installed with router.Use and therefore precedes every
//     per-route handler. An unauthenticated or wrongly-scoped caller was answered by
//     the auth middleware and never reached the guard, so it learned the route was
//     forbidden rather than that it was gone — and a caller holding no credentials at
//     all could never discover the retirement it most needed to know about.
//   - Only four method-and-path pairs were registered. Any other method on the retired
//     path — PATCH, HEAD, OPTIONS — matched no route, so it was answered by Gin's
//     fallback handling as though the surface had never existed, which is a different
//     claim from the one the retirement makes.
//
// Installing it with router.Use ahead of the authentication middleware fixes both at
// once. Gin rebuilds its no-route handler chain from the global handlers whenever Use
// is called, so a request to the retired path is answered here whether or not its
// method was ever registered, and whether or not the caller is authenticated.
//
// The consequence is that this guard must now do what the earlier one could assume:
// tell a request to the retired surface from every other request in the API. That is
// what isDeprecatedWebhookSubscriptionPath is for, and why it matches an exact shape.
//
// # Registration order is part of the contract
//
// This must be registered before router.Use(auth.Authenticate()). Registered after it,
// the retirement is once again invisible to unauthenticated callers — the precise
// defect above, reintroduced silently, because every test using a valid master key
// still passes. api/api.go registers it immediately before authentication for that
// reason, and the webhook_sunset tests assert the unauthenticated case specifically so
// the ordering cannot regress unnoticed.
//
// # /hooks is NOT one of them, deliberately
//
// The /hooks routes are a different feature and must keep answering normally after the
// retirement. They are the PRE_TRANSACTION and POST_TRANSACTION request-time callouts
// in internal/hooks, whose responses can influence how a transaction is processed —
// synchronous interception, not asynchronous event notification. Only the asynchronous
// notification transport is being retired, so retiring /hooks would break a live,
// supported feature that merely shares a word with the one being removed. The two share
// infrastructure as well: the hook manager enqueues its work onto the webhook asynq
// queue by name, which is why that queue outlives the transport.
//
// Now that the guard runs for every request, the same care extends much further: the
// live subscriber registry and the credential endpoint sit directly beneath the same
// /subscribers prefix as the retired surface, so the path match has to separate them.
// It does, by requiring the webhook-subscription leaf and an exact segment count.
//
// # /hooks is NOT one of them, deliberately
//
// The /hooks routes are a different feature and must keep answering normally after the
// retirement. They are the PRE_TRANSACTION and POST_TRANSACTION request-time callouts
// in internal/hooks, whose responses can influence how a transaction is processed —
// synchronous interception, not asynchronous event notification. Only the asynchronous
// notification transport is being retired, so attaching this guard to /hooks would
// break a live, supported feature that merely shares a word with the one being removed.
// The two share infrastructure as well: the hook manager enqueues its work onto the
// webhook asynq queue by name, which is why that queue outlives the transport.
//
// # Where the answer comes from
//
// blnk.WebhookSunsetSnapshotAt is the only thing consulted, and it is consulted EXACTLY
// ONCE per request from inside the returned handler. Three properties of that are worth
// stating, because each is easy to break by accident:
//
//   - No date is compared here. event_sunset.go resolves one retirement window, which
//     this guard and the relay's dual-delivery predicate both read, so they cannot form
//     different opinions about when it closes. An unconfigured instant is a legitimate
//     steady state for a deployment with NO Kafka brokers, and the routes keep answering
//     indefinitely; brokers configured without an instant is rejected at start-up and
//     fails closed if it reaches the runtime anyway. Both answers are the predicate's,
//     not a fallback applied here.
//   - The verdict is not resolved when the middleware is built. Configuration is read
//     live from a store whose contents are replaced wholesale, so a verdict captured at
//     construction time would answer the question as it stood when the router was
//     assembled rather than when the request arrived. WebhookSunsetGuard therefore does
//     nothing but return the closure; every branch is inside it.
//   - ONE SNAPSHOT SERVES BOTH the header and the verdict. This used to pair
//     WebhookSunsetDate with WebhookSunsetPassed, and each of those re-reads the live
//     store — so a reload landing between them produced one response advertising date A
//     while refusing under date B. A client reading the header was told it had until A
//     by the very response that had already applied B, with nothing in the body to
//     disclose it. The window for that is narrow, which is exactly why it is closed
//     structurally: it cannot be reproduced on demand and would never be caught by
//     watching for it.
//
// # What it responds with
//
// Past the retirement instant it aborts through abortWithCode carrying
// apierror.ErrGenGone, which the error catalog maps to the HTTP Gone status. Going
// through the catalog instead of writing a status directly is what makes the code and
// the status impossible to get out of step, and it yields the same dual body every
// other error response in this API carries: the flat "error" string alongside the
// structured "error_detail" object. Before the instant the guard is transparent — it
// adds the advisory headers and hands the request straight on, so the four routes
// answer exactly as they did before it was attached.
//
// Returns:
//   - gin.HandlerFunc: a global middleware function that aborts with
//     apierror.ErrGenGone once the webhook retirement instant has passed, for any
//     method addressing the retired path, and otherwise passes the request through
//     untouched.
func WebhookSunsetGuard() gin.HandlerFunc {
	return func(c *gin.Context) {
		// THE PATH TEST COMES FIRST, which is what makes this guard SAFE UNDER EITHER
		// INSTALLATION.
		//
		// api.Router attaches it PER ROUTE, to the four retired verbs, where every request that
		// reaches it addresses the retired surface by construction and the test is therefore
		// always satisfied. That is not the only way it is installed: this package's own tests
		// install it with router.Use — which is the only way to exercise the neighbours it must
		// leave alone — and a future simplification moving the four attachments to one
		// router.Use would look entirely reasonable.
		//
		// Under a global installation it runs for every request: /hooks, the live subscriber
		// registry, the credential endpoint a subscriber needs in order to migrate, every ledger
		// and transaction route. Reaching the verdict below without first establishing that THIS
		// request addresses the retired surface would answer 410 to all of them the moment the
		// instant passed, and every test that authenticates against the retired path would still
		// pass while it happened. So the test is here rather than left to the attachment: a
		// guard whose blast radius depends on where somebody put it is one line away from
		// retiring the whole API.
		//
		// requestAddressesRetiredWebhookSurface is the shared reading: the matched route
		// template when routing produced one, and an exact three-segment path shape when it did
		// not, so an unregistered verb on the retired path is still answered here while
		// /subscribers/x/webhook-subscription/anything is not retired by association.
		if !requestAddressesRetiredWebhookSurface(c) {
			c.Next()

			return
		}

		// RESOLVED ONCE, and everything below reads this one value. The clock is read
		// here too, so the instant the verdict was taken against is the instant that
		// produced the header beside it.
		snapshot := blnk.WebhookSunsetSnapshotAt(time.Now())

		// Advertise the retirement date whenever there is one to advertise: before the
		// verdict, and regardless of it, so a client still inside the window is warned
		// by the very responses it is succeeding with. WebhookDeprecationWindow reports
		// both ends of the SAME resolution the predicate uses, so the headers cannot
		// describe a different moment from the one the refusal rests on. Its third
		// result answers only "is there a window to render", never "has the instant
		// passed" — that question belongs to the predicate alone, and nothing here
		// branches on it.
		if deprecated, _, configured := blnk.WebhookDeprecationWindow(); configured {
			// The layout hard-codes GMT, so the value has to be rendered from UTC for
			// the zone it claims to be true. Taken from the SNAPSHOT rather than from the
			// window's second result, because the snapshot is the resolution the verdict
			// below rests on: reading the date from a second call would let the header
			// describe a different moment from the refusal beside it.
			stamp := snapshot.Date.UTC().Format(http.TimeFormat)
			c.Header(webhookSunsetHeader, stamp)
			// The window's opening instant, in the sf-date form RFC 9745 requires. It
			// comes from the SAME resolution as the sunset above, so the two headers
			// cannot describe windows that disagree, and the ordering RFC 9745 §4
			// requires holds by construction.
			c.Header(webhookDeprecationHeader, formatDeprecationDate(deprecated))
		}

		if snapshot.Passed {
			// abortWithCode resolves the status from the code, writes the body and
			// aborts the chain, so the route handler is already unreachable from here
			// and nothing further may be written to the response.
			abortWithCode(c, apierror.ErrGenGone, webhookSunsetGoneMessage)

			return
		}

		c.Next()
	}
}
