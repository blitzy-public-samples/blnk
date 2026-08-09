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
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers the webhook retirement guard, which is the HTTP half of requirement R-12:
// after the configured instant the webhook REST API answers 410 Gone on EVERY request.
//
// The behaviour lives here, in the guard's own package, rather than in an end-to-end API test,
// because the case that was broken cannot be reached through a route at all. Gin decides which
// handlers run by matching method AND path, so a guard attached per route only ever sees the
// methods somebody registered — and the methods nobody registered were exactly the ones
// answering 404 and 405 about a surface that had been retired. Driving the guard
// behind a router with no route for PATCH, OPTIONS or HEAD is what proves those now answer 410,
// and it needs no database to do it.

// deprecatedWebhookPath is a request path on the retired surface.
const deprecatedWebhookPath = "/subscribers/sub_01HXYZ/webhook-subscription"

// everyHTTPMethod is the full set a client can send. All of them must answer 410 after the
// retirement instant — including the four that had handlers and the ones that never did, which
// is the distinction the per-route attachment could not express.
var everyHTTPMethod = []string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodOptions,
	http.MethodConnect,
	http.MethodTrace,
	// A method nobody has ever registered anywhere, to make the point that the guard answers on
	// the PATH rather than on a list of verbs.
	"PROPFIND",
}

// storeSunsetWindow publishes a deprecation window, restoring whatever was there afterwards.
//
// The whole configuration is replaced because that is how the store works — an atomic value
// holding one document — and the previous contents are restored on cleanup so a test cannot
// leak its window into the next one.
func storeSunsetWindow(t *testing.T, start, sunset time.Time) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(&config.Configuration{
		WebhookDeprecationStartDate:  start.UTC().Format(time.RFC3339),
		WebhookDeprecationSunsetDate: sunset.UTC().Format(time.RFC3339),
	})
}

// sunsetTestRouter builds a router carrying the guard exactly as api.Router installs it:
// GLOBALLY, and before anything else.
//
// The four deprecated methods get handlers, as they do in production during the window, and
// every other method deliberately gets none — so an unguarded request to one of those reaches
// Gin's own 404/405 answer, which is the behaviour this guard exists to replace. A marker
// handler records that the request got past the guard, because "did the handler run" is the
// question for the pre-retirement case and 200 alone does not answer it.
func sunsetTestRouter() (*gin.Engine, *bool) {
	gin.SetMode(gin.TestMode)

	reached := false
	router := gin.New()
	router.Use(WebhookSunsetGuard())

	handler := func(c *gin.Context) {
		reached = true
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}

	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
	} {
		router.Handle(method, "/subscribers/:subscriber_id/webhook-subscription", handler)
	}

	// The neighbours that must keep working. /hooks is a different feature — the
	// PRE_TRANSACTION and POST_TRANSACTION request-time callouts — and the rest of
	// /subscribers is the REPLACEMENT for the retired surface, so retiring either would be
	// worse than the bug being fixed.
	router.POST("/hooks", handler)
	router.GET("/hooks", handler)
	router.GET("/hooks/:id", handler)
	router.POST("/subscribers", handler)
	router.GET("/subscribers/:subscriber_id", handler)
	router.POST("/subscribers/:subscriber_id/kafka-credentials", handler)

	return router, &reached
}

// TestWebhookSunsetGuard_AnswersGoneOnEveryMethodAfterTheSunset is the criterion itself.
//
// # What was wrong
//
// The guard was attached per route, to the four deprecated methods that had handlers. Gin
// matches method AND path to choose handlers, so PATCH, OPTIONS, HEAD and anything else to the
// same path matched no route and were answered by the ROUTER: 404, or 405 with an Allow header
// listing the four verbs that "work". Both are false about a surface that has been permanently
// removed, and the 405 actively tells a client to try again with a different verb.
//
// # Why the fix is a global guard rather than more routes
//
// Registering the remaining verbs would be an endless list — the last entry below is a method
// nobody has ever heard of — and it would put handlers on a surface being removed. Gin includes
// the global chain in its no-route and no-method handler lists, so one global guard that matches
// the PATH answers for every verb that exists and every verb that does not.
func TestWebhookSunsetGuard_AnswersGoneOnEveryMethodAfterTheSunset(t *testing.T) {
	now := time.Now()
	// A window that closed yesterday: the retirement has passed.
	storeSunsetWindow(t, now.AddDate(0, 0, -31), now.AddDate(0, 0, -1))

	router, reached := sunsetTestRouter()

	for _, method := range everyHTTPMethod {
		t.Run(method, func(t *testing.T) {
			*reached = false

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(method, deprecatedWebhookPath, nil))

			require.Equal(t, http.StatusGone, recorder.Code,
				"%s to a retired route must answer 410 Gone. Anything else is a claim about the "+
					"surface that is not true: 404 says it never existed, and 405 tells the client "+
					"to try another verb on something that is never coming back", method)
			assert.False(t, *reached,
				"the handler must be unreachable after the retirement; the guard aborts the chain")

			// HEAD carries no body by protocol, so only the status is assertable there.
			if method == http.MethodHead {
				return
			}

			var body struct {
				Error       string `json:"error"`
				ErrorDetail struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error_detail"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body),
				"the refusal must carry this API's standard error body, not a bare status")
			assert.Equal(t, "GEN_GONE", body.ErrorDetail.Code,
				"the machine-readable code is what a client branches on, and it must be the typed "+
					"code whose statusByCode entry produced the 410 — writing the status directly "+
					"would let the two drift")
			// "retired", not "removed": the routes and their handlers are still registered and
			// still compiled after the instant, and moving the date back restores them, so a
			// body claiming the endpoint no longer exists would describe a state the deployment
			// is not in. The 410 status already carries "do not come back"; the prose only has
			// to be true.
			assert.Contains(t, body.ErrorDetail.Message, "retired and is no longer available")
		})
	}
}

// TestWebhookSunsetGuard_PerRouteAttachmentCannotAnswerForUnregisteredMethods pins down WHY the
// installation position is the fix, by demonstrating the shape that was wrong.
//
// The guard's own logic was never the problem — the same handler is used here, unmodified, and
// it answers 410 correctly for the four methods somebody registered. What it cannot do from this
// position is answer for a method that has no route, because Gin never runs a route's handler
// chain for a request that matched no route. So this test builds the OLD wiring and asserts the
// leak: 404 or 405 for PATCH, OPTIONS and HEAD, about a surface that is permanently gone.
//
// Keeping it makes the suite above impossible to satisfy by accident. Every assertion in
// TestWebhookSunsetGuard_AnswersGoneOnEveryMethodAfterTheSunset for an unregistered method fails
// under this wiring and passes under the global one, so the two tests together demonstrate that
// the global installation is what does the work rather than something incidental about the
// handler.
func TestWebhookSunsetGuard_PerRouteAttachmentCannotAnswerForUnregisteredMethods(t *testing.T) {
	now := time.Now()
	storeSunsetWindow(t, now.AddDate(0, 0, -31), now.AddDate(0, 0, -1))

	gin.SetMode(gin.TestMode)
	oldWiring := gin.New()
	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
	} {
		oldWiring.Handle(method, "/subscribers/:subscriber_id/webhook-subscription",
			WebhookSunsetGuard(),
			func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	}

	// The registered four are answered correctly even by the old wiring, which is exactly why
	// the gap went unnoticed: any test using a verb that has a handler passes either way.
	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
	} {
		recorder := httptest.NewRecorder()
		oldWiring.ServeHTTP(recorder, httptest.NewRequest(method, deprecatedWebhookPath, nil))
		assert.Equalf(t, http.StatusGone, recorder.Code,
			"%s had a route, so even the per-route attachment refused it", method)
	}

	// The unregistered ones are the finding. Gin answers them itself, and its answer is a
	// statement about routing rather than about retirement.
	for _, method := range []string{http.MethodPatch, http.MethodOptions, http.MethodHead} {
		recorder := httptest.NewRecorder()
		oldWiring.ServeHTTP(recorder, httptest.NewRequest(method, deprecatedWebhookPath, nil))

		require.NotEqualf(t, http.StatusGone, recorder.Code,
			"this test documents the defect: a per-route guard cannot answer for %s, because Gin "+
				"runs a route's handlers only for a request that matched that route. If this now "+
				"returns 410, Gin's dispatch has changed and the reasoning in the guard's "+
				"documentation needs revisiting", method)
		assert.Containsf(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, recorder.Code,
			"%s leaked as a routing answer (%d): 404 claims the surface never existed and 405 "+
				"tells the client to retry with one of the four verbs, both of which are false "+
				"about a retired surface", method, recorder.Code)
	}
}

// TestWebhookSunsetGuard_IsTransparentBeforeTheSunset asserts the other side of the boundary.
//
// A guard that answered 410 early would end the dual-delivery window ahead of the date
// published to subscribers, which is a worse failure than the one being fixed: subscribers who
// have not migrated lose their transport with the configured date still claiming otherwise. The
// advisory headers are present throughout, so a client still succeeding is warned by the very
// responses it is succeeding with.
func TestWebhookSunsetGuard_IsTransparentBeforeTheSunset(t *testing.T) {
	now := time.Now()
	sunset := now.AddDate(0, 0, 15)
	storeSunsetWindow(t, now.AddDate(0, 0, -15), sunset)

	router, reached := sunsetTestRouter()

	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
	} {
		t.Run(method+" reaches its handler", func(t *testing.T) {
			*reached = false

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(method, deprecatedWebhookPath, nil))

			assert.Equal(t, http.StatusOK, recorder.Code)
			assert.True(t, *reached,
				"inside the window the deprecated routes must answer exactly as they always did")
			assert.Equal(t, sunset.UTC().Format(http.TimeFormat), recorder.Header().Get("Sunset"),
				"the RFC 8594 Sunset header must advertise the retirement instant while the routes "+
					"still work; it is the only thing a client can schedule its migration against")
			deprecated, _, configured := blnk.WebhookDeprecationWindow()
			require.True(t, configured, "a configured sunset must yield a window to advertise")
			assert.Equal(t, DeprecationHeaderValue(deprecated), recorder.Header().Get("Deprecation"),
				"the Deprecation field is RFC 9745: an RFC 9651 structured-field date for the "+
					"instant the deprecation began, not the token \"true\"")
		})
	}

	t.Run("an unregistered method is still just unrouted before the sunset", func(t *testing.T) {
		// The guard must not invent a route. Before the retirement a PATCH to this path is
		// exactly what it always was — unrouted — and answering 410 for it early would retire
		// the surface ahead of the date.
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPatch, deprecatedWebhookPath, nil))

		assert.NotEqual(t, http.StatusGone, recorder.Code,
			"before the retirement instant nothing may answer 410")
	})
}

// TestWebhookSunsetGuard_LeavesEveryOtherRouteAlone is the blast-radius assertion, and it is the
// half a global middleware makes worth checking.
//
// Attaching the guard globally is what lets it answer for unregistered methods; the risk it
// introduces is retiring something healthy. Two families must be untouched, and both would be
// caught by a prefix match rather than an exact one:
//
//   - /hooks, a different and fully supported feature: synchronous PRE_TRANSACTION and
//     POST_TRANSACTION callouts whose responses influence transaction processing. Only the
//     asynchronous notification transport is being retired.
//   - the rest of /subscribers, which is the REPLACEMENT for the retired surface. A guard that
//     matched the prefix would retire the migration path along with the thing being migrated
//     away from — including the credential endpoint a subscriber needs in order to leave.
func TestWebhookSunsetGuard_LeavesEveryOtherRouteAlone(t *testing.T) {
	now := time.Now()
	storeSunsetWindow(t, now.AddDate(0, 0, -31), now.AddDate(0, 0, -1))

	router, reached := sunsetTestRouter()

	survivors := []struct {
		method string
		path   string
		why    string
	}{
		{http.MethodPost, "/hooks", "transaction hooks are a different feature and stay live"},
		{http.MethodGet, "/hooks", "listing hooks must keep working after the retirement"},
		{http.MethodGet, "/hooks/hook_1", "and so must reading one"},
		{http.MethodPost, "/subscribers", "the registry is the replacement, not the thing retired"},
		{http.MethodGet, "/subscribers/sub_01HXYZ", "reading a subscriber is part of that registry"},
		{
			http.MethodPost, "/subscribers/sub_01HXYZ/kafka-credentials",
			"credential issuance is how a subscriber migrates AWAY from the retired surface; " +
				"retiring it would strand exactly the callers being asked to move",
		},
	}

	for _, survivor := range survivors {
		t.Run(survivor.method+" "+survivor.path, func(t *testing.T) {
			*reached = false

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(survivor.method, survivor.path, nil))

			require.Equal(t, http.StatusOK, recorder.Code, survivor.why)
			assert.True(t, *reached, survivor.why)
			assert.Empty(t, recorder.Header().Get("Sunset"),
				"a route that is not being retired must not advertise a retirement date: the guard "+
					"is installed globally, so a header added unconditionally would appear on the "+
					"whole API and tell every client it was going away")
		})
	}
}

// TestIsDeprecatedWebhookSubscriptionPath_MatchesOneExactShape is the matcher's boundary.
//
// The matcher IS the blast radius of a global guard, so its width is the property that matters:
// too narrow and a retired path keeps answering, too wide and a live feature is retired by a
// middleware nobody attached to it.
func TestIsDeprecatedWebhookSubscriptionPath_MatchesOneExactShape(t *testing.T) {
	matches := []string{
		"/subscribers/sub_1/webhook-subscription",
		// One trailing slash is tolerated: Gin would otherwise redirect it to the same route,
		// and a redirect is not the answer for a surface that is gone.
		"/subscribers/sub_1/webhook-subscription/",
		// The id is opaque, so anything non-empty is a subscriber as far as this is concerned.
		"/subscribers/00000000-0000-0000-0000-000000000000/webhook-subscription",
	}
	for _, path := range matches {
		assert.Truef(t, IsDeprecatedWebhookSubscriptionPath(path), "%q is the retired surface", path)
	}

	rejects := map[string]string{
		"/subscribers":                                  "the registry itself",
		"/subscribers/sub_1":                            "reading one subscriber",
		"/subscribers/sub_1/kafka-credentials":          "the migration path",
		"/subscribers//webhook-subscription":            "an empty id addressed no route",
		"/subscribers/sub_1/webhook-subscription/extra": "there are no sub-resources",
		"/subscribers/sub_1/webhook-subscription//":     "two trailing slashes routed nowhere",
		"/webhook-subscription":                         "the segment alone was never a route",
		"/hooks":                                        "a different feature entirely",
		"/hooks/sub_1/webhook-subscription":             "the shape under the wrong root",
		"/Subscribers/sub_1/webhook-subscription":       "the router compares case-sensitively",
		"/subscribers/sub_1/Webhook-Subscription":       "likewise for the last segment",
		"/subscribers/sub_1/webhook-subscriptions":      "a near-miss segment is not the surface",
		"/api/subscribers/sub_1/webhook-subscription":   "nothing is mounted under a prefix",
		"":  "an empty path",
		"/": "the root",
	}
	for path, why := range rejects {
		assert.Falsef(t, IsDeprecatedWebhookSubscriptionPath(path),
			"%q must not be treated as the retired surface: %s", path, why)
	}
}

// TestWebhookSunsetGuard_UnconfiguredRetirementKeepsAnswering asserts the shipped default.
//
// No retirement instant configured is a legitimate steady state — it is how every deployment
// runs before it opts into the migration — so the guard must be invisible, headers included.
// The verdict comes from the root package's single decision point, which is what stops this
// guard and the relay's dual-delivery branch forming different opinions about the window.
func TestWebhookSunsetGuard_UnconfiguredRetirementKeepsAnswering(t *testing.T) {
	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})
	config.ConfigStore.Store(&config.Configuration{})

	router, reached := sunsetTestRouter()

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

	assert.Equal(t, http.StatusOK, recorder.Code,
		"with no retirement instant configured the deprecated routes keep working indefinitely")
	assert.True(t, *reached)
	assert.Empty(t, recorder.Header().Get("Sunset"),
		"there is no date to advertise, and inventing one would announce a retirement nobody "+
			"configured")
}

// TestAPIRouter_InstallsTheSunsetGuardGloballyAndBeforeAuthentication is the wiring half.
//
// # Why it is asserted against the source
//
// The two properties that make the fix work are POSITIONAL, and position is invisible from a
// response: a guard attached per route answers correctly for the four registered methods and
// wrongly for every other one, and a guard installed after authentication answers 401 instead
// of 410 to an unauthenticated caller. Both look fine from any test that authenticates and uses
// a registered verb. The behavioural matrix above proves what the guard does; this proves where
// api.Router puts it, which is what decides whether the guard is ever consulted.
//
// # It also pins the barrier's position relative to the middleware that can end a request
//
// Being installed globally and before authentication is not enough. The chain is assembled by TWO
// functions — NewAPI, then Router on the engine NewAPI returned — so every Use in Router runs
// after every Use in NewAPI. The barrier lived in Router while RateLimitMiddleware lived in
// NewAPI, which meant a throttled request to a retired route was answered 429 and never reached
// either guard. That position is invisible from any response that is not itself throttled, so it
// is asserted here against the source.
//
// # Two layers, and which one may be global
//
// There are two guards and they are not interchangeable. WebhookSunsetPreAuthGuard matches the
// path and the method itself BEFORE authentication runs, so it is the one that may be — and
// must be — installed globally: only there can it answer a verb that routes nowhere and a caller
// whose credential has also lapsed. WebhookSunsetGuard tests the path too, so a global
// installation of it would be harmless rather than catastrophic; what it cannot do is answer
// ahead of authentication or for a request that matches no route, because a per-route handler
// runs only after gin has matched one. Installing IT globally therefore does not retire the API
// — it merely does the pre-auth guard's job a second time, in the one position where that job
// cannot be done. Both facts are asserted below.
// # Why it reads the syntax tree rather than the text
//
// The first version of this test matched strings, and it passed against an api.go whose
// installation had been COMMENTED OUT — the comment still contained the text being searched for.
// A test that a disabled line satisfies is worse than no test, because it reports the property
// as covered. Parsing gives the real thing: a commented-out call is not a call, so it is simply
// absent from the tree, and the ordering assertion compares actual statement positions rather
// than positions in a file that may not all be code.
func TestAPIRouter_InstallsTheSunsetGuardGloballyAndBeforeAuthentication(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "../api.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parsing api/api.go")

	render := func(node ast.Node) string {
		var out strings.Builder
		require.NoError(t, printer.Fprint(&out, fileSet, node))

		return out.String()
	}

	// Every live call that is handed the guard, and every live call that installs the
	// authenticator, with the callee rendered so the SHAPE of the installation is assertable and
	// not merely its presence.
	type installation struct {
		callee string
		// enclosing is the function the installation sits in. It matters because the chain is
		// assembled by TWO functions — NewAPI first, then Router on the engine NewAPI returned —
		// so a file position only orders two installations that share a function. Across
		// functions, "which function" IS the order.
		enclosing string
		at        token.Pos
	}

	// The function enclosing a position, resolved by containment rather than by name matching, so
	// a helper added between the two does not silently make every installation "unknown".
	enclosingFunc := func(pos token.Pos) string {
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}

			if pos >= function.Body.Pos() && pos <= function.Body.End() {
				return function.Name.Name
			}
		}

		return ""
	}

	var preAuthInstallations []installation
	var routeAttachments []installation
	var authInstallations []installation
	// The middlewares that can END a request before a later one is reached. The barrier must
	// precede every one of them, or the refusal THEY issue becomes the answer a retired route
	// gives. They end a request by different means, and both count:
	//
	//   - RateLimitMiddleware aborts outright with 429. This is the one M-5 was raised about.
	//   - RequestSizeLimit does not abort; it swaps the body for a MaxBytesReader, so the 413
	//     arrives later when something reads it. Ordering the barrier ahead of it is not strictly
	//     required today for that reason, and it is asserted anyway: the guard reads no body, so
	//     nothing is lost, and if this middleware is ever changed to refuse up front the barrier
	//     is already in front of it.
	abortingInstallations := map[string]*installation{
		"middleware.RequestSizeLimit":    nil,
		"middleware.RateLimitMiddleware": nil,
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		for _, argument := range call.Args {
			rendered := render(argument)

			// The aborting middlewares are constructed WITH ARGUMENTS, so their rendered form
			// carries the argument text and cannot be matched exactly. The callee prefix is the
			// stable part.
			for name := range abortingInstallations {
				if strings.HasPrefix(rendered, name+"(") {
					abortingInstallations[name] = &installation{
						callee: render(call.Fun), enclosing: enclosingFunc(call.Pos()), at: call.Pos(),
					}
				}
			}

			switch rendered {
			case "middleware.WebhookSunsetPreAuthGuard()":
				preAuthInstallations = append(preAuthInstallations,
					installation{callee: render(call.Fun), enclosing: enclosingFunc(call.Pos()), at: call.Pos()})
			case "middleware.WebhookSunsetGuard()":
				routeAttachments = append(routeAttachments,
					installation{callee: render(call.Fun), enclosing: enclosingFunc(call.Pos()), at: call.Pos()})
			case "a.auth.Authenticate()":
				authInstallations = append(authInstallations,
					installation{callee: render(call.Fun), enclosing: enclosingFunc(call.Pos()), at: call.Pos()})
			}
		}

		return true
	})

	require.Len(t, preAuthInstallations, 1,
		"the PRE-AUTH guard must be installed exactly once, so there is one place that decides "+
			"which requests never reach a route at all. Found: %v", preAuthInstallations)
	require.Len(t, authInstallations, 1, "the authenticator must still be installed exactly once")

	assert.True(t, strings.HasSuffix(preAuthInstallations[0].callee, ".Use"),
		"the pre-auth guard must be installed GLOBALLY, with Use on the engine. Attached to a "+
			"route instead it cannot answer for a method nobody registered, and those were exactly "+
			"the methods answering 404 and 405 about a retired surface. Attached to a "+
			"/subscribers group it would retire the registry and the credential endpoint, which "+
			"are the migration path away from the retired surface. Got %q",
		preAuthInstallations[0].callee)

	// M-5: THE BARRIER MUST PRECEDE EVERY MIDDLEWARE THAT CAN ABORT, and it is installed in
	// NewAPI precisely so that it does.
	//
	// It used to be installed in Router. Every Use in Router runs AFTER every Use in NewAPI, and
	// NewAPI installs RequestSizeLimit and RateLimitMiddleware — both of which abort — so a
	// throttled or oversized request to a retired route was answered 429 or 413 and never reached
	// either sunset guard. A caller told to slow down and retry would retry a surface that is
	// gone, forever. R-12 says every request answers 410, and "every request except the throttled
	// ones" is not that.
	require.Equal(t, "NewAPI", preAuthInstallations[0].enclosing,
		"the barrier must be installed in NewAPI, which is the only function that runs before the "+
			"aborting middleware it has to precede. Got %q", preAuthInstallations[0].enclosing)

	for name, aborting := range abortingInstallations {
		require.NotNil(t, aborting, "%s is expected in the chain; if it moved, this test must "+
			"be told where, because the barrier's position is defined relative to it", name)
		require.Equal(t, "NewAPI", aborting.enclosing,
			"%s is expected in NewAPI alongside the barrier, so their file positions order them. "+
				"Got %q", name, aborting.enclosing)

		assert.Less(t, preAuthInstallations[0].at, aborting.at,
			"the retirement barrier must be installed BEFORE %s. Installed after it, a request to "+
				"a retired route is answered by %s instead of 410 — which is what happened when the "+
				"barrier lived in Router", name, name)
	}

	// BEFORE AUTHENTICATION, still. Across functions the enclosing function IS the order: NewAPI
	// builds the engine and returns it, Router then installs the authenticator on that same
	// engine, so anything NewAPI installed necessarily runs first. Comparing file positions here
	// would assert the wrong thing — Router is declared above NewAPI in this file.
	require.Equal(t, "Router", authInstallations[0].enclosing,
		"the authenticator is expected in Router; if it moves into NewAPI this test must compare "+
			"positions instead of functions. Got %q", authInstallations[0].enclosing)
	assert.NotEqual(t, authInstallations[0].enclosing, preAuthInstallations[0].enclosing,
		"the pre-auth guard must be installed BEFORE the authenticator. After it, an "+
			"unauthenticated request to a retired route is answered 401 and a wrongly-scoped one "+
			"403 - neither is 410, and a caller told 401 will go on fixing credentials for a route "+
			"that is never coming back. Nothing is disclosed by answering first: the path is "+
			"published in the migration guide and the body is a fixed sentence")

	// THE SECOND LAYER, and the assertion is that it is exactly the second layer. The per-route
	// guard is the retirement's local statement at each registration and would still refuse if
	// the global middleware were removed from the chain; both read the same predicate, so they
	// cannot disagree about when the window closes.
	//
	// WHAT MUST NEVER HAPPEN is router.Use of THIS guard AS THE ONLY BARRIER. It runs after gin
	// has matched a route, so globally installed it still cannot answer an unregistered verb or
	// an unauthenticated caller — the two cases the retirement has to cover and the two that a
	// response-level test cannot distinguish from success. It would not retire the API: the guard
	// tests the path, and this package's own tests install it globally to prove it leaves /hooks
	// and the rest of /subscribers alone. The assertion below is about POSITION, which is the
	// property no behavioural test can see.
	require.Len(t, routeAttachments, 4,
		"the per-route guard belongs on exactly the four retired verbs. Found: %v", routeAttachments)

	for _, attachment := range routeAttachments {
		assert.Equal(t, "Router", attachment.enclosing,
			"the per-route attachments belong with the route registrations in Router: got %q",
			attachment.enclosing)
		assert.NotEqual(t, "router.Use", attachment.callee,
			"WebhookSunsetGuard runs only after a route has matched, so installing it with router.Use "+
				"leaves the retirement unable to answer an unregistered verb or an unauthenticated "+
				"caller — WebhookSunsetPreAuthGuard is the one that belongs there. This one is attached "+
				"per route: got %q", attachment.callee)
		assert.Contains(t, []string{"router.POST", "router.GET", "router.PUT", "router.DELETE"},
			attachment.callee,
			"and only to the four retired verbs: got %q", attachment.callee)
		assert.Greater(t, attachment.at, authInstallations[0].at,
			"a per-route handler necessarily runs after the global chain, so its registration "+
				"sitting before the authenticator's would mean the routes were registered on a "+
				"router that had not been given its middleware yet")
	}

}

// sunsetTestConfig installs a configuration whose only relevant field is the retirement
// instant, and restores nothing: config.MockConfig replaces the whole snapshot, which is what
// every other test in this package relies on.
//
// Parameters:
//   - t *testing.T: the test, for require.
//   - sunset string: the RFC3339 retirement instant, or "" for a deployment that has
//     configured none.
func sunsetTestConfig(t *testing.T, sunset string) {
	t.Helper()

	config.MockConfig(&config.Configuration{
		ProjectName:                  "blnk-sunset-preauth",
		WebhookDeprecationSunsetDate: sunset,
		Redis:                        config.RedisConfig{Dns: "localhost:6379"},
		DataSource:                   config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})

	_, err := config.Fetch()
	require.NoError(t, err, "the sunset fixture configuration must load")
}

// sunsetPreAuthRouter builds a router in the SAME ORDER api/api.go does: the pre-auth guard
// first, then a stand-in for Authenticate() that refuses everything, then the routes.
//
// The stand-in refusing everything is the point. It reproduces the state the finding was about
// — a caller whose key is missing, malformed, revoked or wrongly scoped — so a route that
// answers 410 here can only have been answered BEFORE authentication, and a route that answers
// 401 proves the guard did not reach it.
//
// Parameters:
//   - t *testing.T: the test.
//   - reached *bool: flipped by any handler the chain actually reaches.
//
// Returns:
//   - *gin.Engine: the router.
func sunsetPreAuthRouter(t *testing.T, reached *bool) *gin.Engine {
	t.Helper()

	gin.SetMode(gin.TestMode)
	router := gin.New()

	router.Use(WebhookSunsetPreAuthGuard())
	router.Use(func(c *gin.Context) {
		abortWithCode(c, apierror.ErrAuthMissingAPIKey, "no credential was supplied")
	})

	handler := func(c *gin.Context) {
		*reached = true
		c.String(http.StatusOK, "handler-ok")
	}

	// The four deprecated registrations, with the per-route guard attached exactly as
	// api/api.go attaches it, so the two guards are exercised together rather than in
	// isolation.
	router.POST(DeprecatedWebhookSubscriptionRoute, WebhookSunsetGuard(), handler)
	router.GET(DeprecatedWebhookSubscriptionRoute, WebhookSunsetGuard(), handler)
	router.PUT(DeprecatedWebhookSubscriptionRoute, WebhookSunsetGuard(), handler)
	router.DELETE(DeprecatedWebhookSubscriptionRoute, WebhookSunsetGuard(), handler)

	// Neighbours that must be entirely unaffected. /hooks is the important one: it is a live,
	// supported feature that merely shares a word with the transport being retired.
	router.POST("/hooks", handler)
	router.GET("/hooks", handler)
	router.POST("/subscribers", handler)
	router.GET("/subscribers/:subscriber_id", handler)
	router.POST("/subscribers/:subscriber_id/kafka-credentials", handler)
	router.POST("/transactions", handler)

	return router
}

// sunsetRequest issues one request and returns the recorder.
func sunsetRequest(router *gin.Engine, method, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))

	return recorder
}

// TestWebhookSunset_PreAuthGuardAnswersGoneWithoutACredential is the R-12 assertion: after the
// retirement instant EVERY request to the deprecated surface is answered 410, including the
// unauthenticated ones.
//
// Before this guard existed those requests were refused 401 by the global authentication
// middleware, which gin runs ahead of any per-route middleware, so the retirement was invisible
// to exactly the callers most likely to have stopped maintaining their integration.
func TestWebhookSunset_PreAuthGuardAnswersGoneWithoutACredential(t *testing.T) {
	sunsetTestConfig(t, time.Now().Add(-48*time.Hour).UTC().Format(time.RFC3339))

	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			reached := false
			router := sunsetPreAuthRouter(t, &reached)

			recorder := sunsetRequest(router, method, "/subscribers/sub_1/webhook-subscription")

			assert.Equal(t, http.StatusGone, recorder.Code,
				"a retired route must answer 410 whether or not the caller authenticated")
			assert.False(t, reached, "the deprecated handler must be unreachable after the retirement")

			var body map[string]interface{}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			detail, ok := body["error_detail"].(map[string]interface{})
			require.True(t, ok, "the refusal must carry the structured error_detail every error carries")
			assert.Equal(t, "GEN_GONE", detail["code"],
				"the typed code is what a client branches on; the status alone is not a contract")

			assert.NotEmpty(t, recorder.Header().Get(webhookSunsetHeader),
				"the RFC 8594 Sunset header must state the instant the surface was retired")
			// The RFC 9745 Deprecation field, in the RFC 9651 sf-date form. Asserted as that
			// exact rendering rather than merely non-empty, because two guards answering the
			// same retirement with different spellings is the divergence this pairing exists
			// to rule out.
			deprecated, _, configured := blnk.WebhookDeprecationWindow()
			require.True(t, configured, "the fixture configures the window, so it must resolve")
			assert.Equal(t, formatDeprecationDate(deprecated),
				recorder.Header().Get(webhookDeprecationHeader),
				"both guards must render the deprecation instant identically")
		})
	}
}

// TestWebhookSunset_PreAuthGuardLeavesEveryOtherRouteToAuthentication is the containment
// assertion, and it is the one that makes the guard safe to install globally.
//
// A pre-authentication guard is a hole in the authentication boundary by construction, so its
// reach has to be proven rather than asserted in a comment. Every neighbour here must still be
// refused by the authentication stand-in — 401, not 410 — even with the retirement long past.
func TestWebhookSunset_PreAuthGuardLeavesEveryOtherRouteToAuthentication(t *testing.T) {
	sunsetTestConfig(t, time.Now().Add(-48*time.Hour).UTC().Format(time.RFC3339))

	neighbours := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/hooks"},
		{http.MethodGet, "/hooks"},
		{http.MethodPost, "/subscribers"},
		{http.MethodGet, "/subscribers/sub_1"},
		{http.MethodPost, "/subscribers/sub_1/kafka-credentials"},
		{http.MethodPost, "/transactions"},
	}

	for _, neighbour := range neighbours {
		t.Run(neighbour.method+" "+neighbour.path, func(t *testing.T) {
			reached := false
			router := sunsetPreAuthRouter(t, &reached)

			recorder := sunsetRequest(router, neighbour.method, neighbour.path)

			assert.Equal(t, http.StatusUnauthorized, recorder.Code,
				"the retirement must not reach this route: it is answered by authentication, as before")
			assert.False(t, reached)
			assert.Empty(t, recorder.Header().Get(webhookSunsetHeader),
				"a Sunset header here would announce the retirement of something that is not retired")
		})
	}
}

// TestWebhookSunset_PreAuthGuardIsTransparentInsideTheWindow asserts the other side of the
// boundary: while the window is open the guard must change nothing at all, so the deprecated
// routes are still authenticated exactly like every other route.
//
// Getting this wrong in the permissive direction would let an unauthenticated caller read or
// rewrite a subscriber's recorded webhook URL for the whole 30-day window.
func TestWebhookSunset_PreAuthGuardIsTransparentInsideTheWindow(t *testing.T) {
	sunsetTestConfig(t, time.Now().Add(21*24*time.Hour).UTC().Format(time.RFC3339))

	reached := false
	router := sunsetPreAuthRouter(t, &reached)

	recorder := sunsetRequest(router, http.MethodPut, "/subscribers/sub_1/webhook-subscription")

	assert.Equal(t, http.StatusUnauthorized, recorder.Code,
		"inside the window the deprecated routes are ordinary authenticated routes")
	assert.False(t, reached, "and an unauthenticated caller reaches no handler")
	assert.NotEmpty(t, recorder.Header().Get(webhookSunsetHeader),
		"the advisory headers are still advertised inside the window; that is what warns a client")
}

// TestWebhookSunset_PreAuthGuardIsInertWithNoConfiguredInstant covers the shipped steady state:
// a deployment that has configured no retirement instant keeps working indefinitely, and the
// guard neither refuses nor advertises anything.
func TestWebhookSunset_PreAuthGuardIsInertWithNoConfiguredInstant(t *testing.T) {
	sunsetTestConfig(t, "")

	reached := false
	router := sunsetPreAuthRouter(t, &reached)

	recorder := sunsetRequest(router, http.MethodGet, "/subscribers/sub_1/webhook-subscription")

	assert.Equal(t, http.StatusUnauthorized, recorder.Code,
		"with no instant configured the route is live and authentication answers it")
	assert.Empty(t, recorder.Header().Get(webhookSunsetHeader),
		"there is no instant to advertise, so no header may claim one")
}

// TestWebhookSunset_PreAuthGuardMatchesExactlyTheDeprecatedRoutes proves the predicate against
// route templates rather than against the guard's documentation.
//
// The false cases are the ones that matter. A prefix or substring test on the path would retire
// the credential-issuance endpoint and any unregistered child of the deprecated route, and an
// empty template — every unmatched request, which gin answers 404 or 405 — must never match,
// because there is no deprecated handler behind it to shield.
func TestWebhookSunset_PreAuthGuardMatchesExactlyTheDeprecatedRoutes(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete} {
		assert.True(t, IsDeprecatedWebhookSubscriptionRequest(DeprecatedWebhookSubscriptionRoute, method),
			"%s on the deprecated template is deprecated", method)
	}

	cases := []struct {
		name     string
		fullPath string
		method   string
	}{
		{"an unmatched request has no template", "", http.MethodGet},
		{"a method not registered on the template", DeprecatedWebhookSubscriptionRoute, http.MethodPatch},
		{"the credential endpoint", "/subscribers/:subscriber_id/kafka-credentials", http.MethodPost},
		{"the subscriber itself", "/subscribers/:subscriber_id", http.MethodGet},
		{"the subscriber collection", "/subscribers", http.MethodPost},
		{"a child of the deprecated route", DeprecatedWebhookSubscriptionRoute + "/extra", http.MethodGet},
		{"hooks", "/hooks", http.MethodPost},
		{"a raw path rather than a template", "/subscribers/sub_1/webhook-subscription", http.MethodGet},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.False(t, IsDeprecatedWebhookSubscriptionRequest(testCase.fullPath, testCase.method),
				"the retirement must not reach %q", testCase.fullPath)
		})
	}
}

// TestSunsetGuards_EachResolveTheWindowExactlyOnce is the guard-level half of M-4.
//
// # Why this is asserted against the source
//
// The property is "one resolution per request", and a response cannot show how many times the
// configuration store was read to produce it. Both guards answered correctly under every existing
// test in this package while each made TWO resolutions:
//
//   - WebhookSunsetPreAuthGuard called WebhookDeprecationWindow for its headers and then
//     WebhookSunsetPassed(time.Now()) for its verdict.
//   - WebhookSunsetGuard took a snapshot for its verdict but still called
//     WebhookDeprecationWindow for the Deprecation header and for the decision of whether to
//     render headers at all.
//
// Each of those calls re-reads a store whose contents are replaced wholesale on reload, so a
// reload landing between them produced a response advertising one window while refusing under
// another — and, because the window's two ends are two independently configured fields, a
// Deprecation instant that can fall AFTER the Sunset instant beside it, which RFC 9745 §4 forbids.
// The race is narrow, cannot be reproduced on demand, and would never be caught by watching for
// it, which is exactly why the structural property is pinned instead.
//
// The two forbidden helpers are not deprecated in general — WebhookDeprecationWindow is the right
// call for a startup log line, which resolves once and is not composing a response. They are
// forbidden HERE, where something else has already been resolved in the same request.
func TestSunsetGuards_EachResolveTheWindowExactlyOnce(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "sunset.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err, "parsing api/middleware/sunset.go")

	render := func(node ast.Node) string {
		var out strings.Builder
		require.NoError(t, printer.Fprint(&out, fileSet, node))

		return out.String()
	}

	// Every call into the root package made from inside one function body, by callee name.
	callsInto := func(function *ast.FuncDecl) map[string]int {
		found := map[string]int{}
		ast.Inspect(function, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}

			if callee := render(call.Fun); strings.HasPrefix(callee, "blnk.") {
				found[callee]++
			}

			return true
		})

		return found
	}

	guards := map[string]*ast.FuncDecl{}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}

		if function.Name.Name == "WebhookSunsetGuard" || function.Name.Name == "WebhookSunsetPreAuthGuard" {
			guards[function.Name.Name] = function
		}
	}

	require.Len(t, guards, 2, "both guards must exist in this file; found %v", guards)

	for name, guard := range guards {
		t.Run(name, func(t *testing.T) {
			calls := callsInto(guard)

			assert.Equal(t, 1, calls["blnk.WebhookSunsetSnapshotAt"],
				"%s must take EXACTLY ONE snapshot: it is the only value that carries the headers, "+
					"the render decision and the verdict from a single configuration read. Calls "+
					"observed: %v", name, calls)

			// The two helpers that each perform their OWN resolution. Reaching for either from
			// inside a guard reintroduces the split, and it reads as harmless at the call site.
			for _, forbidden := range []string{
				"blnk.WebhookDeprecationWindow",
				"blnk.WebhookSunsetPassed",
				"blnk.WebhookSunsetPassedNow",
				"blnk.WebhookSunsetDate",
			} {
				assert.Zero(t, calls[forbidden],
					"%s must not call %s: it resolves the window again, so the value it returns can "+
						"come from a different configuration generation than the snapshot already "+
						"taken. Everything this guard needs is on the snapshot — Date, WindowStart, "+
						"DateConfigured and Passed", name, forbidden)
			}
		})
	}
}
