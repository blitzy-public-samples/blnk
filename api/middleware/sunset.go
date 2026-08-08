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
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
)

// This file is the HTTP half of the legacy webhook retirement.
//
// Retiring the webhook transport has two observable behaviours — the relay stops
// enqueueing legacy delivery work, and the deprecated webhook-subscription management
// routes stop answering — and they are two consumers of ONE decision, which lives in
// the root package's event_sunset.go. Nothing about the retirement is decided here.
// This guard asks the predicate and turns its answer into a response.
//
// That separation is the point rather than a tidiness preference. Were the comparison
// duplicated in the request path, the service could go on accepting webhook
// subscription management calls after it had already stopped delivering webhooks, or
// refuse those calls while deliveries were still going out. Neither failure raises an
// error to notice; each is simply the wrong behaviour, indefinitely.

// The advisory response headers the guard advertises whenever a retirement instant is
// configured, on both sides of the boundary.
//
// Sunset is the RFC 8594 field. It carries the instant at which the routes stop
// responding, which is the one thing a client integrating against them needs in order
// to schedule its own migration. Deprecation is its companion and carries the token
// form rather than a date, because the two fields answer different questions: one is
// when the surface goes away, the other is whether it is already deprecated. Dating
// the second with the first instant would assert that deprecation begins at the moment
// the routes disappear, which is the opposite of what a deprecation notice is for.
//
// Both are purely informational. The refusal below is never a function of either.
const (
	webhookSunsetHeader      = "Sunset"
	webhookDeprecationHeader = "Deprecation"
	webhookDeprecationValue  = "true"
)

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
const webhookSunsetGoneMessage = "Webhook subscription management has been permanently removed. " +
	"Use the Kafka event stream and the subscriber credential endpoint instead; " +
	"see the webhook-to-Kafka migration guide under docs/."

// WebhookSunsetGuard refuses requests to the deprecated legacy webhook-subscription
// management routes once the webhook retirement instant has passed.
//
// # The routes it guards
//
// api/api.go attaches it per route, to exactly four:
//
//   - POST   /subscribers/:subscriber_id/webhook-subscription
//   - GET    /subscribers/:subscriber_id/webhook-subscription
//   - PUT    /subscribers/:subscriber_id/webhook-subscription
//   - DELETE /subscribers/:subscriber_id/webhook-subscription
//
// It is never installed globally with router.Use and never joins the middleware chain
// assembled in NewAPI, so it has no need to tell a guarded request from an unguarded
// one: routing already made that decision, and this handler only ever runs where it was
// attached. That is why it inspects neither the request path nor the method. Deciding
// again here would give the repository two places to keep in step, and they would
// eventually disagree.
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
// blnk.WebhookSunsetPassed is the only thing consulted, and it is consulted once per
// request from inside the returned handler. Two properties of that are worth stating,
// because both are easy to break by accident:
//
//   - No date is read or compared here. The root package's event_sunset.go owns the one
//     comparison in the codebase, so this guard and the relay's dual-delivery branch
//     cannot form different opinions about when the window closes. A deployment with no
//     retirement instant configured is a legitimate steady state and keeps working
//     indefinitely — that is the predicate's answer, not a fallback applied here — and
//     an unusable one fails closed, which is likewise the predicate's judgement to make.
//   - The verdict is not resolved when the middleware is built. Configuration is read
//     live from a store whose contents are replaced wholesale, so a verdict captured at
//     construction time would answer the question as it stood when the router was
//     assembled rather than when the request arrived. WebhookSunsetGuard therefore does
//     nothing but return the closure; every branch is inside it.
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
//   - gin.HandlerFunc: a per-route middleware function that aborts with
//     apierror.ErrGenGone once the webhook retirement instant has passed, and otherwise
//     passes the request through untouched.
func WebhookSunsetGuard() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Advertise the retirement date whenever there is one to advertise: before the
		// verdict, and regardless of it, so a client still inside the window is warned
		// by the very responses it is succeeding with. WebhookSunsetDate reports the
		// same resolved instant the predicate uses, so the headers cannot describe a
		// different moment from the one the refusal rests on. Its second result answers
		// only "is there an instant to render", never "has the instant passed" — that
		// question belongs to the predicate alone, and nothing here branches on it.
		if sunset, configured := blnk.WebhookSunsetDate(); configured {
			// The layout hard-codes GMT, so the value has to be rendered from UTC for
			// the zone it claims to be true.
			stamp := sunset.UTC().Format(http.TimeFormat)
			c.Header(webhookSunsetHeader, stamp)
			c.Header(webhookDeprecationHeader, webhookDeprecationValue)
		}

		if blnk.WebhookSunsetPassed(time.Now()) {
			// abortWithCode resolves the status from the code, writes the body and
			// aborts the chain, so the route handler is already unreachable from here
			// and nothing further may be written to the response.
			abortWithCode(c, apierror.ErrGenGone, webhookSunsetGoneMessage)

			return
		}

		c.Next()
	}
}
