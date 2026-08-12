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

// The advisory response headers the guard advertises whenever a retirement instant is
// configured, on both sides of the boundary.
const (
	webhookSunsetHeader      = "Sunset"
	webhookDeprecationHeader = "Deprecation"

	// The RFC 9651 §3.3.7 sf-date sigil. A Date is serialised as this byte followed by
	// the integer seconds, and nothing else — no quoting, no sub-second part.
	structuredFieldDatePrefix = "@"
)

// DeprecatedWebhookSubscriptionRoute is the ONE registered gin route template the
// webhook retirement covers, and DeprecatedWebhookSubscriptionMethods are the only
// methods registered on it.
const DeprecatedWebhookSubscriptionRoute = "/subscribers/:subscriber_id/webhook-subscription"

// DeprecatedWebhookSubscriptionMethods is the method set. It is a set rather than a slice so
// the membership test is exact and order-free.
var DeprecatedWebhookSubscriptionMethods = map[string]struct{}{
	http.MethodPost:   {},
	http.MethodGet:    {},
	http.MethodPut:    {},
	http.MethodDelete: {},
}

// IsDeprecatedWebhookSubscriptionRequest reports whether a request addresses one of the
// four deprecated legacy webhook-subscription management routes.
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

// requestAddressesRetiredWebhookSurface reports whether THIS request addresses the
// retired webhook-subscription surface, by whichever of the two available facts
// applies.
func requestAddressesRetiredWebhookSurface(c *gin.Context) bool {
	if template := c.FullPath(); template != "" {
		return IsDeprecatedWebhookSubscriptionRequest(template, c.Request.Method)
	}

	if c.Request == nil || c.Request.URL == nil {
		return false
	}

	return isDeprecatedWebhookSubscriptionPath(c.Request.URL.Path)
}

// WebhookSunsetPreAuthGuard answers the four deprecated webhook-subscription routes
// with 410 Gone BEFORE authentication runs, and lets every other request through
// untouched.
//
// Returns:
//   - gin.HandlerFunc: global middleware that aborts with apierror.ErrGenGone for the
//     four deprecated requests once the retirement instant has passed, and is otherwise
//     a no-op.
func WebhookSunsetPreAuthGuard() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !requestAddressesRetiredWebhookSurface(c) {
			// NOT a deprecated request. Nothing is added — not even the advisory headers —
			// because this middleware runs on every request in the API and a Sunset header on
			// /transactions would announce the retirement of something that is not being
			// retired.
			c.Next()

			return
		}

		// ONE SNAPSHOT, and the clock is read into it, so the two headers and the verdict
		// below all rest on the same resolution of the same configuration generation.
		snapshot := blnk.WebhookSunsetSnapshotAt(time.Now())

		if snapshot.DateConfigured {
			// Both headers come from the SAME snapshot, so the pair cannot describe windows
			// that disagree and the ordering RFC 9745 §4 requires holds by construction.
			c.Header(webhookSunsetHeader, snapshot.Date.UTC().Format(http.TimeFormat))
			c.Header(webhookDeprecationHeader, formatDeprecationDate(snapshot.WindowStart))
		}

		if snapshot.Passed {
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
// Parameters:
//   - instant time.Time: the moment to render. Any location; the epoch value is
//     absolute.
//
// Returns:
//   - string: the field value, for example "@1688169599".
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
const webhookSunsetGoneMessage = "Webhook subscription management has been retired and is no " +
	"longer available. Use the Kafka event stream and the subscriber credential endpoint " +
	"instead; see the webhook-to-Kafka migration guide under docs/."

// deprecatedWebhookSubscriptionSegments is the path shape of the retired surface:
const (
	deprecatedWebhookSubscriptionPathSegments = 3
	deprecatedWebhookSubscriptionRoot         = "subscribers"
	deprecatedWebhookSubscriptionLeaf         = "webhook-subscription"
)

// IsDeprecatedWebhookSubscriptionPath is the exported form of the matcher, for callers
// outside this package that need to recognise the retired surface — a test asserting
// the boundary, and any future router wiring that must answer for it.
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
func isDeprecatedWebhookSubscriptionPath(path string) bool {
	if len(path) > 1 {
		path = strings.TrimSuffix(path, "/")
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) != deprecatedWebhookSubscriptionPathSegments {
		return false
	}

	// The subscriber id is not validated, only required to be present. A malformed id on a
	// retired surface is still a request to a retired surface, and answering it with
	// anything other than Gone would leak the fact that the route is gone behind a
	// validation error.
	return segments[0] == deprecatedWebhookSubscriptionRoot &&
		segments[1] != "" &&
		segments[2] == deprecatedWebhookSubscriptionLeaf
}

// WebhookSunsetGuard refuses requests to the deprecated legacy webhook-subscription
// management routes once the webhook retirement instant has passed. Before it, the
// guard is transparent: it adds the advisory headers and hands the request on, so the
// routes answer exactly as they did before it was attached.
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
		if !requestAddressesRetiredWebhookSurface(c) {
			c.Next()

			return
		}

		// RESOLVED ONCE, and everything below reads this one value. The clock is read
		// here too, so the instant the verdict was taken against is the instant that
		// produced the header beside it.
		snapshot := blnk.WebhookSunsetSnapshotAt(time.Now())

		// Advertise the retirement date whenever there is one to advertise: before the
		// verdict, and regardless of it, so a client still inside the window is warned by the
		// very responses it is succeeding with.
		if snapshot.DateConfigured {
			// The layout hard-codes GMT, so the value has to be rendered from UTC for
			// the zone it claims to be true.
			stamp := snapshot.Date.UTC().Format(http.TimeFormat)
			c.Header(webhookSunsetHeader, stamp)
			// The window's opening instant, in the sf-date form RFC 9745 requires. It comes from
			// the SAME resolution as the sunset above, so the two headers cannot describe
			// windows that disagree, and the ordering RFC 9745 §4 requires holds by
			// construction.
			c.Header(webhookDeprecationHeader, formatDeprecationDate(snapshot.WindowStart))
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
