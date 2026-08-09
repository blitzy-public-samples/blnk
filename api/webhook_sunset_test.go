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

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// This file is the HTTP half of acceptance criterion V-10: after the retirement instant
// the webhook REST API answers 410 Gone on EVERY request.
//
// "Every" is the load-bearing word, and it is the reason these tests exist as a set
// rather than as one happy-path check. A guard attached per route and behind
// authentication satisfies the obvious reading of V-10 — the four registered methods,
// called with a valid master key, all return 410 — while leaving two populations of
// request answering something else entirely:
//
//   - Callers that do not authenticate. They are answered by the authentication
//     middleware first and told the surface is unauthorized, which is a claim about
//     their credentials rather than about the surface. A client with no credentials
//     cannot discover the retirement at all.
//   - Methods nobody registered. PATCH, HEAD and OPTIONS match no route, so Gin's
//     fallback answers as though the path had never existed.
//
// Both populations are asserted below, and both are the kind of gap that no amount of
// testing the four registered methods with a valid key would ever reveal.

// sunsetTestSecretKey is the master key the retirement tests authenticate with when
// they want to isolate the retirement from authentication rather than test their
// interaction.
const sunsetTestSecretKey = "sunset-test-master-key"

// setupSunsetRouter builds a fully wired router with a chosen retirement instant and
// secure-mode setting.
//
// Two details matter for correctness rather than convenience:
//
//   - Router() calls router.Use, so it must be invoked exactly once per engine. A
//     second call would install the whole global chain — the retirement guard and
//     authentication included — a second time, and every request would then be judged
//     twice. Each test therefore gets its own instance.
//   - MockConfig must be applied BEFORE NewBlnk, because the Blnk instance captures
//     the configuration it is constructed with and only falls back to the live store.
//
// Parameters:
//   - t: the test.
//   - sunsetDate: the RFC3339 retirement instant, or "" for a deployment that has
//     configured none.
//   - secure: whether secure mode is enabled, which is what makes the authentication
//     middleware actually enforce.
//
// Returns:
//   - *gin.Engine: the assembled router.
func setupSunsetRouter(t *testing.T, sunsetDate string, secure bool) *gin.Engine {
	t.Helper()

	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_sunset",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:    secure,
			SecretKey: sunsetTestSecretKey,
		},
		WebhookDeprecationSunsetDate: sunsetDate,
	})

	cnf, err := config.Fetch()
	require.NoError(t, err, "fetching the mocked configuration")
	require.Equal(t, sunsetDate, cnf.WebhookDeprecationSunsetDate,
		"the retirement instant did not reach the configuration store; MockConfig "+
			"silently keeps the previous configuration when validation fails, which "+
			"would make every assertion below test the wrong deployment")

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err, "creating the datasource")

	instance, err := blnk.NewBlnk(db)
	require.NoError(t, err, "creating the Blnk instance")

	api := NewAPI(instance)
	require.NotNil(t, api, "NewAPI returned nil, which means configuration fetch failed")

	return api.Router()
}

// retiredWebhookPath is a concrete request path on the retired surface.
const retiredWebhookPath = "/subscribers/sub_sunset_test/webhook-subscription"

// pastSunset is an instant comfortably behind any clock the suite could run under.
func pastSunset() string {
	return time.Now().UTC().AddDate(0, 0, -1).Format(time.RFC3339)
}

// futureSunset is an instant far enough ahead that the dual-delivery window is open.
func futureSunset() string {
	return time.Now().UTC().AddDate(0, 0, 10).Format(time.RFC3339)
}

// doSunsetRequest issues a request through the router and returns the recorder.
func doSunsetRequest(t *testing.T, router *gin.Engine, method, path string, authenticate bool) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	if authenticate {
		req.Header.Set("X-Blnk-Key", sunsetTestSecretKey)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	return rec
}

// Past the retirement instant, every method addressed to the retired path is Gone —
// including the ones no route was ever registered for.
//
// The unregistered methods are the substantive half. PATCH, HEAD and OPTIONS reach no
// route, so they are answered by the handler chain Gin rebuilds for unmatched requests.
// That chain contains the global middleware and therefore the retirement guard, which
// is exactly why the guard has to be installed globally: attached per route it cannot
// run for a method that has no route.
func TestWebhookSunset_EveryMethodOnTheRetiredPathIsGone(t *testing.T) {
	router := setupSunsetRouter(t, pastSunset(), true)

	registered := []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
	}
	unregistered := []string{
		http.MethodPatch, http.MethodHead, http.MethodOptions,
	}

	for _, method := range append(registered, unregistered...) {
		t.Run(method, func(t *testing.T) {
			rec := doSunsetRequest(t, router, method, retiredWebhookPath, true)

			require.Equal(t, http.StatusGone, rec.Code,
				"%s %s must be Gone after the retirement instant; got %d with body %q",
				method, retiredWebhookPath, rec.Code, rec.Body.String())

			// Note on HEAD: the body is asserted here as it is for every other method,
			// and that is correct even though a HEAD response carries no body on the
			// wire. httptest.ResponseRecorder records exactly what the handler wrote,
			// whereas net/http is what discards the body for HEAD when serving a real
			// connection. So the recorded body proves the handler produced the right
			// refusal, and the suppression is the server's job rather than the guard's.
			assert.Equal(t, string(apierror.ErrGenGone), errorCodeOf(t, rec.Body.Bytes()),
				"the refusal must carry the typed Gone code so clients branch on the "+
					"code rather than parsing prose")
		})
	}
}

// A caller that does not authenticate is told the surface is Gone, not that it is
// unauthorized.
//
// This is the single most important assertion in the file, because it is the one that
// fails the moment the guard is registered after authentication instead of before it.
// Secure mode is deliberately ON: with it off the authentication middleware passes
// everything through and the test would pass regardless of ordering, proving nothing.
// With it on, an unauthenticated request would be answered 401 by authentication if it
// ran first — so 410 here is positive evidence that the retirement precedes it.
func TestWebhookSunset_AnUnauthenticatedCallerIsToldGoneNotUnauthorized(t *testing.T) {
	router := setupSunsetRouter(t, pastSunset(), true)

	t.Run("secure mode really would reject an unauthenticated caller", func(t *testing.T) {
		// The control. A live, supported route under the same prefix must still
		// challenge an unauthenticated caller, which establishes that authentication is
		// genuinely enforcing in this configuration and that the 410 below is not simply
		// authentication being inert.
		rec := doSunsetRequest(t, router, http.MethodGet, "/subscribers", false)

		require.Equal(t, http.StatusUnauthorized, rec.Code,
			"a live route must challenge an unauthenticated caller when secure mode is "+
				"on; if it does not, this test cannot distinguish guard ordering from "+
				"authentication being disabled")
	})

	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch,
	} {
		t.Run(method+" unauthenticated is Gone", func(t *testing.T) {
			rec := doSunsetRequest(t, router, method, retiredWebhookPath, false)

			require.Equal(t, http.StatusGone, rec.Code,
				"%s %s must be Gone for an unauthenticated caller; %d suggests the "+
					"retirement guard is registered AFTER authentication, which hides "+
					"the retirement from exactly the clients that need to learn about it",
				method, retiredWebhookPath, rec.Code)

			assert.Equal(t, string(apierror.ErrGenGone), errorCodeOf(t, rec.Body.Bytes()),
				"an unauthenticated caller must receive the Gone code, not an auth code")
		})
	}
}

// The retirement is scoped to the retired surface and nothing else.
//
// A globally installed guard is offered every request in the service, so the blast
// radius of a sloppy path match is the whole API. These routes are the neighbours most
// at risk: two share the /subscribers prefix with the retired surface, /hooks shares
// its vocabulary, and the health route is what an orchestrator uses to decide whether
// the process is alive at all.
func TestWebhookSunset_RetiresNothingBeyondTheRetiredSurface(t *testing.T) {
	router := setupSunsetRouter(t, pastSunset(), true)

	for _, tc := range []struct {
		name   string
		method string
		path   string
		why    string
	}{
		{
			name:   "the live subscriber registry",
			method: http.MethodGet,
			path:   "/subscribers",
			why:    "the registry is the replacement for the retired surface",
		},
		{
			name:   "the credential-issuance endpoint",
			method: http.MethodPost,
			path:   "/subscribers/sub_sunset_test/kafka-credentials",
			why:    "retiring this would remove the migration path off webhooks entirely",
		},
		{
			name:   "the hooks surface of a different feature",
			method: http.MethodGet,
			path:   "/hooks",
			why:    "PRE_TRANSACTION and POST_TRANSACTION callouts are fully supported",
		},
		{
			name:   "the dead-letter inventory",
			method: http.MethodGet,
			path:   "/events/stats",
			why:    "the event surface is the feature the retirement exists to move traffic to",
		},
		{
			name:   "the health route",
			method: http.MethodGet,
			path:   "/",
			why:    "retiring this would take the deployment down, not a transport",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := doSunsetRequest(t, router, tc.method, tc.path, true)

			assert.NotEqual(t, http.StatusGone, rec.Code,
				"%s %s must not be retired: %s", tc.method, tc.path, tc.why)
		})
	}
}

// Inside the dual-delivery window the guard is transparent, and it advertises the
// instant at which it will stop being so.
//
// The headers are asserted on a SUCCEEDING request deliberately. RFC 8594's Sunset
// field exists to warn a client that is still working, and a deprecation notice
// delivered only with the refusal arrives precisely too late to act on.
func TestWebhookSunset_BeforeTheInstantTheGuardIsTransparentAndWarns(t *testing.T) {
	router := setupSunsetRouter(t, futureSunset(), true)

	rec := doSunsetRequest(t, router, http.MethodGet, retiredWebhookPath, true)

	require.NotEqual(t, http.StatusGone, rec.Code,
		"inside the dual-delivery window the deprecated routes must still answer; "+
			"body %q", rec.Body.String())

	assert.NotEmpty(t, rec.Header().Get("Sunset"),
		"a request succeeding inside the window must still carry the Sunset header, "+
			"which is the only warning a working client receives")
	deprecated, _, configured := blnk.WebhookDeprecationWindow()
	require.True(t, configured, "a configured sunset must yield a window to advertise")
	assert.Equal(t, middleware.DeprecationHeaderValue(deprecated), rec.Header().Get("Deprecation"),
		"the Deprecation field is RFC 9745 — a structured-field date for the instant the "+
			"deprecation BEGAN — and it must accompany Sunset on a request that still succeeds")
}

// A deployment that has configured no retirement instant keeps working indefinitely.
//
// This is a legitimate steady state rather than a misconfiguration, and it is the state
// every deployment that has not yet scheduled its migration is in. A guard that treated
// an absent instant as a passed one would retire the surface on upgrade, for everybody,
// without anyone having chosen a date.
func TestWebhookSunset_NoConfiguredInstantRetiresNothing(t *testing.T) {
	router := setupSunsetRouter(t, "", true)

	rec := doSunsetRequest(t, router, http.MethodGet, retiredWebhookPath, true)

	require.NotEqual(t, http.StatusGone, rec.Code,
		"with no retirement instant configured the deprecated routes must keep "+
			"answering; body %q", rec.Body.String())

	assert.Empty(t, rec.Header().Get("Sunset"),
		"there is no instant to advertise, so no Sunset header may be invented")
}

// errorCodeOf reads the machine-readable code out of an error response body.
//
// The status alone cannot distinguish a deliberate 410 from an unmapped code that defaulted
// to one, so every sunset assertion checks the code as well.
//
// Parameters:
//   - t *testing.T: for the fatal on an unparseable body.
//   - body []byte: the raw response body.
//
// Returns:
//   - string: the "code" field, or the empty string when the body carries none.
func errorCodeOf(t *testing.T, body []byte) string {
	t.Helper()

	// THE CODE IS NESTED, and reading it from the top level is what made this helper report
	// an empty string for every refusal. respondCode writes the API's standard envelope —
	// {"error": <message>, "error_detail": {"code": ..., "message": ...}} — so a top-level
	// `code` key does not exist on any error response this package produces, and a helper
	// looking for one turns "the guard answered with the wrong code" and "the guard answered
	// correctly" into the same empty answer.
	var envelope struct {
		ErrorDetail struct {
			Code string `json:"code"`
		} `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "body: %s", string(body))

	return envelope.ErrorDetail.Code
}

// A sunset instant safely in the past and one safely in the future. Both are fixed rather
// than computed from time.Now with a small offset, so a slow test machine cannot drift
// across the boundary mid-run and make a failure look like a logic error.
const (
	sunsetPassedInstant = "2020-01-01T00:00:00Z"
	sunsetFutureInstant = "2999-01-01T00:00:00Z"
)

// A concrete path on the deprecated surface, and the supported credential route that must
// keep working. The deprecated path is built from a literal subscriber id rather than from
// the route pattern, because a request carries a concrete path and the point of the
// interceptor is that it reads that.
const (
	deprecatedWebhookPath = "/subscribers/sub_sunset_probe/webhook-subscription"
	supportedHooksPath    = "/hooks"
)

// sunsetRouter builds a real router with the retirement instant set, and — when secure is
// true — with authentication genuinely enabled so the 401 and 403 paths are the real ones
// rather than a simulation.
//
// setupAuthedRouter is deliberately NOT used: it injects an authenticated context with a
// middleware of its own, which is exactly the ordering under test. Here the request must
// arrive with nothing, or with a real key, and meet the real chain.
func sunsetRouter(t *testing.T, sunset string, secure bool) *gin.Engine {
	t.Helper()

	router, _, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = sunset
		if secure {
			cfg.Server.Secure = true
			cfg.Server.SecretKey = sunsetMasterKey
		}
	})

	return router
}

// sunsetMasterKey is the master secret the secure-mode cases authenticate with.
const sunsetMasterKey = "sunset-master-key-for-the-real-router-tests"

// TestWebhookSunset_AnsweredForEveryVerbOnEveryDeprecatedRoute is the V-10 criterion stated
// as the request matrix it actually is.
//
// Each of the four supported verbs is sent to the deprecated path through the real router,
// and each must be answered 410 with the GEN_GONE code — not merely a 410 status, because
// an unmapped code would resolve to 500 and a hand-written status would drift from the
// catalog.
func TestWebhookSunset_AnsweredForEveryVerbOnEveryDeprecatedRoute(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			router := sunsetRouter(t, sunsetPassedInstant, false)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(method, deprecatedWebhookPath, strings.NewReader("{}")))

			assertErrorCode(t, w, http.StatusGone, apierror.ErrGenGone)
		})
	}
}

// TestWebhookSunset_AnswersGoneBeforeAuthentication is the ordering assertion, and it is the
// finding itself.
//
// Every credential state must reach 410. A missing key must not be answered 401 and an
// insufficiently scoped key must not be answered 403: both were, and both sent a client to
// investigate its credentials for a surface that has been removed from every scope there is.
// Authentication is genuinely enabled here, so these are the real refusals being displaced.
func TestWebhookSunset_AnswersGoneBeforeAuthentication(t *testing.T) {
	for name, apiKey := range map[string]string{
		"no credentials at all": "",
		"an unknown key":        "a-key-that-was-never-issued",
		"the master key":        sunsetMasterKey,
	} {
		t.Run(name, func(t *testing.T) {
			router := sunsetRouter(t, sunsetPassedInstant, true)

			request := httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil)
			if apiKey != "" {
				request.Header.Set("X-Blnk-Key", apiKey)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, request)

			require.NotEqual(t, http.StatusUnauthorized, w.Code,
				"401 BEFORE 410 IS THE DEFECT: the surface is gone, so a credential is not what "+
					"is missing, and telling a client otherwise sends it to debug its keys")
			require.NotEqual(t, http.StatusForbidden, w.Code,
				"and 403 is the same mistake: no scope grants access to a route that no longer exists")

			assertErrorCode(t, w, http.StatusGone, apierror.ErrGenGone)
		})
	}
}

// TestWebhookSunset_AnswersGoneForUnsupportedMethods covers the class of request that could
// not previously reach any guard at all.
//
// A verb the router never registered matches no route, so no route-attached handler exists
// to run: Gin answered it itself. "Every request" includes these, and a global interceptor
// running before authentication is the only thing that can see them — Gin runs global
// middleware for its NoRoute and NoMethod paths.
func TestWebhookSunset_AnswersGoneForUnsupportedMethods(t *testing.T) {
	for _, method := range []string{
		http.MethodPatch,
		http.MethodHead,
		http.MethodOptions,
		http.MethodTrace,
		"PROPFIND", // an entirely unregistered verb, not merely an unrouted standard one
	} {
		t.Run(method, func(t *testing.T) {
			router := sunsetRouter(t, sunsetPassedInstant, true)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(method, deprecatedWebhookPath, nil))

			require.Equal(t, http.StatusGone, w.Code,
				"an unsupported verb on a retired path reached no route, so nothing route-attached "+
					"could answer it; the interceptor is what makes 'every request' true")

			// HEAD carries no body by definition, so only the status is assertable there.
			if method != http.MethodHead {
				assertErrorCode(t, w, http.StatusGone, apierror.ErrGenGone)
			}
		})
	}
}

// TestWebhookSunset_LeavesTheSurfaceAloneBeforeTheInstant is the other half of the boundary,
// and without it the tests above are satisfied by a router that answers 410 always.
//
// Inside the dual-delivery window the deprecated routes must behave exactly as they did:
// authentication runs, the handler runs, and an unsupported verb still gets Gin's own
// refusal rather than a premature retirement.
func TestWebhookSunset_LeavesTheSurfaceAloneBeforeTheInstant(t *testing.T) {
	t.Run("a supported verb is not retired early", func(t *testing.T) {
		router := sunsetRouter(t, sunsetFutureInstant, false)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

		assert.NotEqual(t, http.StatusGone, w.Code,
			"the window is still open, so the route must answer as itself — 404 for an unknown "+
				"subscriber is a correct answer here, 410 is not")
	})

	t.Run("authentication still applies inside the window", func(t *testing.T) {
		// The interceptor passes the request on before the instant, so the chain behind it
		// must be intact. A missing credential is answered by authentication, as always.
		router := sunsetRouter(t, sunsetFutureInstant, true)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

		assert.Equal(t, http.StatusUnauthorized, w.Code,
			"inside the window the deprecated routes are ordinary authenticated routes; an "+
				"interceptor that swallowed the request would have disabled authentication on them")
	})

	t.Run("an unsupported verb is not retired early either", func(t *testing.T) {
		router := sunsetRouter(t, sunsetFutureInstant, false)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPatch, deprecatedWebhookPath, nil))

		assert.NotEqual(t, http.StatusGone, w.Code,
			"PATCH is unsupported, which is not the same as retired; answering 410 before the "+
				"instant would retire the surface early for one class of caller")
	})
}

// TestWebhookSunset_AdvertisesTheInstantOnBothSidesOfTheBoundary pins the advisory headers.
//
// RFC 8594 Sunset is what lets a client schedule its own migration, so it is most useful
// BEFORE the instant — on the very responses the client is still succeeding with. It is
// asserted on both sides because a header that appeared only after the surface was gone
// would be a notice nobody could act on.
func TestWebhookSunset_AdvertisesTheInstantOnBothSidesOfTheBoundary(t *testing.T) {
	expected, err := time.Parse(time.RFC3339, sunsetPassedInstant)
	require.NoError(t, err)

	t.Run("after the instant", func(t *testing.T) {
		router := sunsetRouter(t, sunsetPassedInstant, false)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

		require.Equal(t, http.StatusGone, w.Code)
		assert.Equal(t, expected.UTC().Format(http.TimeFormat), w.Header().Get("Sunset"),
			"the Sunset header must carry the configured instant, rendered in GMT as the layout claims")
		// RFC 9745 requires an Item Structured Field Date per RFC 9651 §3.3.7 — "@" followed
		// by an integer — for the window's OPENING instant, which is the sunset less the
		// dual-delivery window. The literal "true" this once asserted satisfies neither the
		// syntax nor the meaning, and a client parsing the field per the RFC would reject it.
		deprecated, _, configured := blnk.WebhookDeprecationWindow()
		require.True(t, configured, "a configured sunset must yield a window to advertise")
		assert.Equal(t, middleware.DeprecationHeaderValue(deprecated), w.Header().Get("Deprecation"),
			"the Deprecation header must be the RFC 9745 structured-field date of the window's "+
				"opening instant, and it must be rendered from the SAME resolution as the Sunset "+
				"header beside it")
	})

	t.Run("before the instant", func(t *testing.T) {
		router := sunsetRouter(t, sunsetFutureInstant, false)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

		future, parseErr := time.Parse(time.RFC3339, sunsetFutureInstant)
		require.NoError(t, parseErr)
		assert.Equal(t, future.UTC().Format(http.TimeFormat), w.Header().Get("Sunset"),
			"the notice is most useful while the surface still works, so it must be present here")
	})

	t.Run("with no instant configured", func(t *testing.T) {
		router := sunsetRouter(t, "", false)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, deprecatedWebhookPath, nil))

		assert.Empty(t, w.Header().Get("Sunset"),
			"a deployment with no retirement instant has nothing to advertise, and an empty or "+
				"invented header would be worse than none")
		assert.NotEqual(t, http.StatusGone, w.Code,
			"and no instant means no retirement: this is a legitimate steady state")
	})
}

// TestWebhookSunset_DoesNotTouchAnyOtherRoute is the blast-radius assertion, and /hooks is
// the route it exists for.
//
// /hooks is the PRE_TRANSACTION and POST_TRANSACTION request-time callout feature. It shares
// a word with the transport being retired, shares the asynq queue with it, and must keep
// working. The supported subscriber routes matter for the same reason: the credential
// endpoint is the REPLACEMENT for the retired surface, so retiring it by accident would
// leave a caller with neither.
func TestWebhookSunset_DoesNotTouchAnyOtherRoute(t *testing.T) {
	for name, target := range map[string]struct {
		method string
		path   string
	}{
		"hooks list":                   {http.MethodGet, supportedHooksPath},
		"hooks create":                 {http.MethodPost, supportedHooksPath},
		"subscriber read":              {http.MethodGet, "/subscribers/sub_sunset_probe"},
		"subscriber list":              {http.MethodGet, "/subscribers"},
		"kafka credential issuance":    {http.MethodPost, "/subscribers/sub_sunset_probe/kafka-credentials"},
		"a ledger route":               {http.MethodGet, "/ledgers"},
		"a path that shares a word":    {http.MethodGet, "/subscribers/webhook-subscription"},
		"a deeper deprecated-ish path": {http.MethodGet, "/subscribers/sub_x/webhook-subscription/extra"},
	} {
		t.Run(name, func(t *testing.T) {
			// The instant is PAST, so anything the interceptor wrongly matches answers 410
			// and this assertion catches it.
			router := sunsetRouter(t, sunsetPassedInstant, false)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(target.method, target.path, strings.NewReader("{}")))

			assert.NotEqual(t, http.StatusGone, w.Code,
				"%s %s was retired by the webhook sunset. Only the deprecated "+
					"webhook-subscription surface may be; retiring anything else silently removes "+
					"a supported feature", target.method, target.path)
		})
	}
}

// TestWebhookSunset_TheTwoLayersCoverTheSameRoutes is the drift guard between the global
// interceptor and the per-route guard, and it walks the ROUTER rather than the source.
//
// The two layers share the sunset predicate, so they cannot disagree about when the window
// closes. What they could disagree about is WHICH paths are retired: the interceptor matches
// a concrete path, the guard is attached during registration. This asserts the sets agree in
// both directions — every registered route carrying the deprecated suffix is one the matcher
// recognises, and the matcher recognises nothing that is not registered as deprecated.
func TestWebhookSunset_TheTwoLayersCoverTheSameRoutes(t *testing.T) {
	router := sunsetRouter(t, sunsetPassedInstant, false)

	deprecatedRoutes := 0

	for _, route := range router.Routes() {
		concrete := strings.ReplaceAll(route.Path, ":subscriber_id", "sub_sunset_probe")
		matched := middleware.IsDeprecatedWebhookSubscriptionPath(concrete)

		if strings.HasSuffix(route.Path, "/webhook-subscription") {
			deprecatedRoutes++

			assert.True(t, matched,
				"route %s %s is registered as deprecated but the interceptor's matcher does not "+
					"recognise it, so requests to it would be answered by authentication first",
				route.Method, route.Path)

			continue
		}

		assert.False(t, matched,
			"route %s %s is a SUPPORTED route that the interceptor's matcher claims, so it would "+
				"be retired along with the webhook surface", route.Method, route.Path)
	}

	assert.Equal(t, 4, deprecatedRoutes,
		"the deprecated surface is four verbs on one path; a different count means a route was "+
			"added or removed without this matrix being updated")
}

// webhookSunsetRoute is one deprecated route, addressed the way a client addresses it.
type webhookSunsetRoute struct {
	method string
	path   string
	body   string
}

// webhookSunsetRoutes is every deprecated webhook-subscription route and method.
//
// The list is exhaustive on purpose: the acceptance criterion is "410 on every request",
// so a route that kept answering would be a hole in the retirement, and a method that did
// would be the same hole reached differently. Bodies are supplied where the handler binds
// one, so a refusal cannot be mistaken for a bind failure that happens to share a status.
var webhookSunsetRoutes = []webhookSunsetRoute{
	{
		method: http.MethodPost,
		path:   "/subscribers/sub_sunset_probe/webhook-subscription",
		body:   `{"url":"https://example.test/hooks"}`,
	},
	{method: http.MethodGet, path: "/subscribers/sub_sunset_probe/webhook-subscription"},
	{
		method: http.MethodPut,
		path:   "/subscribers/sub_sunset_probe/webhook-subscription",
		body:   `{"url":"https://example.test/hooks"}`,
	},
	{method: http.MethodDelete, path: "/subscribers/sub_sunset_probe/webhook-subscription"},
}

// name renders a route for a subtest name.
func (r webhookSunsetRoute) name() string {
	return r.method + " " + r.path
}

// webhookSunsetConfig builds the configuration the router under test reads.
//
// The base is this package's usual test configuration; mutate adjusts the retirement
// instant and, where the case needs it, the broker list — those two together are the whole
// input to the sunset decision.
//
// Parameters:
//   - mutate func(*config.Configuration): applied before the configuration is published.
//
// Returns:
//   - *config.Configuration: the configuration to publish.
func webhookSunsetConfig(mutate func(*config.Configuration)) *config.Configuration {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_sunset",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	}
	if mutate != nil {
		mutate(cfg)
	}

	return cfg
}

// webhookSunsetRouter publishes the given configuration and returns the REAL router, with
// the master-key principal in context.
//
// The master key is granted so that a refusal can only come from the retirement guard. A
// non-master caller is covered separately, because the ORDER of the two gates is itself a
// property worth pinning.
//
// The configuration is restored to one with no retirement instant on cleanup, so a past
// instant published here cannot leak into another test in this package and retire routes
// it expects to work.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting and cleanup registration.
//   - mutate func(*config.Configuration): adjusts the published configuration.
//
// Returns:
//   - *gin.Engine: the router NewAPI builds, with every route registered as production
//     registers it.
func webhookSunsetRouter(t *testing.T, mutate func(*config.Configuration)) *gin.Engine {
	t.Helper()

	config.MockConfig(webhookSunsetConfig(mutate))
	t.Cleanup(func() { config.MockConfig(webhookSunsetConfig(nil)) })

	cnf, err := config.Fetch()
	require.NoError(t, err)

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err)

	instance, err := blnk.NewBlnk(db)
	require.NoError(t, err)

	apiInstance := NewAPI(instance)
	apiInstance.router.Use(func(c *gin.Context) {
		c.Set("isMasterKey", true)
		c.Next()
	})

	return apiInstance.Router()
}

// webhookSunsetRequest issues one request against the router and returns the recorder.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - router *gin.Engine: the router under test.
//   - route webhookSunsetRoute: the request to issue.
//
// Returns:
//   - *httptest.ResponseRecorder: the response.
func webhookSunsetRequest(t *testing.T, router *gin.Engine, route webhookSunsetRoute) *httptest.ResponseRecorder {
	t.Helper()

	var body *strings.Reader
	if route.body == "" {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader(route.body)
	}

	request, err := http.NewRequestWithContext(context.Background(), route.method, route.path, body)
	require.NoError(t, err)
	if route.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

// webhookSunsetErrorCode reads the catalog code out of an error response, or "" when the
// response is not an error payload.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - recorder *httptest.ResponseRecorder: the response to read.
//
// Returns:
//   - apierror.ErrorCode: the code carried in error_detail, or "".
func webhookSunsetErrorCode(t *testing.T, recorder *httptest.ResponseRecorder) apierror.ErrorCode {
	t.Helper()

	var body struct {
		ErrorDetail apierror.APIError `json:"error_detail"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		return ""
	}

	return body.ErrorDetail.Code
}

// TestWebhookSubscriptionRoutes_AnswerGoneOnEveryRouteAndMethodAfterTheSunset is acceptance
// criterion V-10.
//
// The status is asserted together with the CODE, and both matter. The status alone would
// pass if a handler wrote 410 directly, which is the implementation the error catalog exists
// to prevent: an unmapped code resolves to 500, so a typed code with an explicit status
// mapping is the only thing that makes 410 reachable at all, and asserting the code is what
// proves the response came through the catalog.
func TestWebhookSubscriptionRoutes_AnswerGoneOnEveryRouteAndMethodAfterTheSunset(t *testing.T) {
	// The precondition, asserted first so a catalog regression fails here with a clear
	// message rather than as four confusing 500s below.
	require.Equal(t, http.StatusGone, apierror.StatusForCode(apierror.ErrGenGone),
		"the retirement answers 410 only because the catalog maps this code to it; an unmapped "+
			"code resolves to 500 and the acceptance criterion would fail silently")

	sunset := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	router := webhookSunsetRouter(t, func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)
	})

	for _, route := range webhookSunsetRoutes {
		t.Run(route.name(), func(t *testing.T) {
			recorder := webhookSunsetRequest(t, router, route)

			assertErrorCode(t, recorder, http.StatusGone, apierror.ErrGenGone)
			assert.Contains(t, recorder.Body.String(), "retired and is no longer available",
				"the body must say what happened, and say the same thing on every route. It says "+
					"RETIRED rather than removed because the handlers are still registered and still "+
					"compiled: the instant withdraws the behaviour, deleting the surface is a later "+
					"release, and moving the date back restores it")
			assert.Contains(t, recorder.Body.String(), "Kafka event stream",
				"and must name the replacement, or a client has nowhere to go")

			// RFC 8594 advisory headers, on the refusal as well as on a success.
			assert.Equal(t, sunset.Format(http.TimeFormat), recorder.Header().Get("Sunset"),
				"the Sunset header must carry the configured instant, rendered in GMT as the field requires")
			deprecated, _, configured := blnk.WebhookDeprecationWindow()
			require.True(t, configured)
			assert.Equal(t, middleware.DeprecationHeaderValue(deprecated),
				recorder.Header().Get("Deprecation"),
				"the same structured-field date accompanies the refusal, so a client sees one "+
					"consistent field on both sides of the boundary")
		})
	}

	t.Run("a body is not required to be refused", func(t *testing.T) {
		// A retired route must not read the request before refusing it: binding first would
		// answer a malformed-body error to a caller whose real problem is that the surface is
		// gone, and would make the refusal depend on what was sent.
		recorder := webhookSunsetRequest(t, router, webhookSunsetRoute{
			method: http.MethodPost,
			path:   "/subscribers/sub_sunset_probe/webhook-subscription",
			body:   "}{ not json at all",
		})

		assertErrorCode(t, recorder, http.StatusGone, apierror.ErrGenGone)
	})

	t.Run("the surface is gone for a non-master caller too", func(t *testing.T) {
		// The guard is attached ahead of the handler, so it answers before the master-key
		// gate does. That ordering is the correct one: the resource no longer exists, and
		// which principal is asking cannot change that.
		nonMaster := webhookSunsetRouter(t, func(cfg *config.Configuration) {
			cfg.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)
		})

		for _, route := range webhookSunsetRoutes {
			recorder := webhookSunsetRequest(t, nonMaster, route)
			assert.Equal(t, http.StatusGone, recorder.Code, "%s", route.name())
			assert.Equal(t, apierror.ErrGenGone, webhookSunsetErrorCode(t, recorder), "%s", route.name())
		}
	})
}

// TestWebhookSubscriptionRoutes_KeepAnsweringBeforeTheSunset is the other half of the
// criterion, and the half that keeps the dual-delivery window usable.
//
// A guard that refused early would end the window before its 30 days were up, silently
// cutting off subscribers who have not migrated yet — a failure that raises no error
// anywhere and looks exactly like a successful retirement.
func TestWebhookSubscriptionRoutes_KeepAnsweringBeforeTheSunset(t *testing.T) {
	sunset := time.Now().UTC().Add(15 * 24 * time.Hour).Truncate(time.Second)
	router := webhookSunsetRouter(t, func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)
	})

	for _, route := range webhookSunsetRoutes {
		t.Run(route.name(), func(t *testing.T) {
			recorder := webhookSunsetRequest(t, router, route)

			assert.NotEqual(t, http.StatusGone, recorder.Code,
				"inside the window the route must answer as it always did; body was: %s", recorder.Body.String())
			assert.NotEqual(t, apierror.ErrGenGone, webhookSunsetErrorCode(t, recorder),
				"and must never carry the retirement code")

			// Warned by the very responses it is succeeding with, which is the point of
			// advertising the headers on both sides of the boundary.
			assert.Equal(t, sunset.Format(http.TimeFormat), recorder.Header().Get("Sunset"))
			deprecated, _, configured := blnk.WebhookDeprecationWindow()
			require.True(t, configured)
			assert.Equal(t, middleware.DeprecationHeaderValue(deprecated),
				recorder.Header().Get("Deprecation"))
		})
	}

	t.Run("the boundary is inclusive: AT the instant the surface is gone", func(t *testing.T) {
		// Asked of the predicate rather than over HTTP, because "now == the configured
		// instant" cannot be arranged in a request. The guard consults this predicate and
		// nothing else, so pinning it here pins the boundary the routes observe.
		assert.True(t, blnk.WebhookSunsetPassed(sunset),
			"the retirement takes effect ON the configured instant, not the moment after it")
		assert.False(t, blnk.WebhookSunsetPassed(sunset.Add(-time.Nanosecond)),
			"and not one instant before")
	})

	t.Run("no retirement instant is a legitimate steady state", func(t *testing.T) {
		unconfigured := webhookSunsetRouter(t, nil)

		for _, route := range webhookSunsetRoutes {
			recorder := webhookSunsetRequest(t, unconfigured, route)

			assert.NotEqual(t, http.StatusGone, recorder.Code,
				"%s: a deployment that has scheduled no retirement keeps working indefinitely", route.name())
			assert.Empty(t, recorder.Header().Get("Sunset"),
				"%s: there is no instant to advertise, so the header must be absent rather than empty-valued",
				route.name())
			assert.Empty(t, recorder.Header().Get("Deprecation"), "%s", route.name())
		}
	})
}

// TestWebhookSunsetWindow_IsSettledAtLoadRatherThanPerRequest is the configuration half of
// the retirement, asserted where the 410 lives.
//
// The routes never have to cope with a window nobody can read, because a configuration
// carrying one is REFUSED before it is published: an unparseable instant is fatal, and an
// absent instant is fatal too once brokers are configured. Both refusals matter to an
// operator, and they are the reason the environment template must describe the retirement
// instant as required-and-fatal rather than as advisory — a template promising a warning
// leads an operator to deploy a configuration the service will not start under.
//
// The observation is on the published configuration rather than on a returned error because
// that is what a request reads. A refused configuration leaves the previous one in place, so
// nothing serves traffic under the rejected value.
func TestWebhookSunsetWindow_IsSettledAtLoadRatherThanPerRequest(t *testing.T) {
	// A valid baseline: no transport, no window. This is the shape every existing
	// deployment runs, and it must stay loadable.
	config.MockConfig(webhookSunsetConfig(nil))
	t.Cleanup(func() { config.MockConfig(webhookSunsetConfig(nil)) })

	baseline, err := config.Fetch()
	require.NoError(t, err)
	require.Empty(t, baseline.WebhookDeprecationSunsetDate)
	require.Empty(t, baseline.Kafka.Brokers)

	refused := []struct {
		name   string
		mutate func(*config.Configuration)
		why    string
	}{
		{
			name: "an unparseable instant",
			mutate: func(cfg *config.Configuration) {
				cfg.Kafka.Brokers = []string{"kafka:9092"}
				cfg.Kafka.InsecureLocalDev = true
				cfg.WebhookDeprecationSunsetDate = "30 days from now"
			},
			why: "a mis-typed instant would otherwise resolve to \"keep the legacy behaviour\", " +
				"keeping the deprecated transport alive indefinitely with nothing failing",
		},
		{
			name: "no instant at all while publishing",
			mutate: func(cfg *config.Configuration) {
				cfg.Kafka.Brokers = []string{"kafka:9092"}
				cfg.Kafka.InsecureLocalDev = true
			},
			why: "a publishing deployment with no window has no retirement to honour, and the " +
				"runtime predicate fails closed — so accepting it here would let configuration " +
				"and behaviour disagree about the one decision the retirement is made of",
		},
	}

	for _, testCase := range refused {
		t.Run("refused: "+testCase.name, func(t *testing.T) {
			config.MockConfig(webhookSunsetConfig(testCase.mutate))

			published, fetchErr := config.Fetch()
			require.NoError(t, fetchErr,
				"a refused configuration must leave the previous one in place rather than emptying the store")
			assert.Empty(t, published.WebhookDeprecationSunsetDate, testCase.why)
			assert.Empty(t, published.Kafka.Brokers,
				"the whole configuration is refused, not the offending field: %s", testCase.why)
		})
	}

	t.Run("accepted: a publishing deployment with a stated instant", func(t *testing.T) {
		sunset := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
		config.MockConfig(webhookSunsetConfig(func(cfg *config.Configuration) {
			cfg.Kafka.Brokers = []string{"kafka:9092"}
			cfg.Kafka.InsecureLocalDev = true
			cfg.WebhookDeprecationSunsetDate = sunset.Format(time.RFC3339)
		}))

		published, fetchErr := config.Fetch()
		require.NoError(t, fetchErr)
		require.NotEmpty(t, published.Kafka.Brokers,
			"the valid combination must actually be published, or the refusals above prove nothing")

		parsed, parseErr := time.Parse(time.RFC3339, published.WebhookDeprecationSunsetDate)
		require.NoError(t, parseErr)
		assert.True(t, sunset.Equal(parsed))
		assert.False(t, blnk.WebhookSunsetPassed(time.Now()),
			"a 30-day window opens now, so the routes must still answer")
	})

	t.Run("accepted: no transport and no window", func(t *testing.T) {
		config.MockConfig(webhookSunsetConfig(nil))

		published, fetchErr := config.Fetch()
		require.NoError(t, fetchErr)
		assert.Empty(t, published.WebhookDeprecationSunsetDate)
		assert.False(t, blnk.WebhookSunsetPassed(time.Now()),
			"with nothing to migrate to there is no retirement, so the routes answer indefinitely")
	})
}

// TestWebhookSunsetGuard_DecidesPerRequestRatherThanAtRouterBuildTime is what makes the
// retirement happen on its own, without a deployment.
//
// Configuration is read live from a store that is replaced wholesale, so a verdict captured
// when the router was assembled would answer the question as it stood at start-up. A
// long-running process would then serve the deprecated routes for ever, and the retirement
// would appear to work only because every test restarts the process.
func TestWebhookSunsetGuard_DecidesPerRequestRatherThanAtRouterBuildTime(t *testing.T) {
	future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	router := webhookSunsetRouter(t, func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = future.Format(time.RFC3339)
	})

	probe := webhookSunsetRoutes[1] // GET, which binds no body.

	before := webhookSunsetRequest(t, router, probe)
	require.NotEqual(t, http.StatusGone, before.Code,
		"the fixture must start inside the window, or this proves nothing")

	// The window closes underneath the very same router.
	past := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	config.MockConfig(webhookSunsetConfig(func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = past.Format(time.RFC3339)
	}))

	after := webhookSunsetRequest(t, router, probe)
	assertErrorCode(t, after, http.StatusGone, apierror.ErrGenGone)
	assert.Equal(t, past.Format(http.TimeFormat), after.Header().Get("Sunset"),
		"and the advertised instant must follow the configuration too, not the one the router was built with")
}

// TestHooksRoutes_SurviveTheWebhookSunset guards the most expensive mistake available here.
//
// /hooks is a different feature that merely shares a word: the PRE_TRANSACTION and
// POST_TRANSACTION request-time callouts, whose responses influence transaction processing.
// Attaching the retirement guard with router.Use, or to a group, would take them down with
// the transport — and it would look like a correct retirement while doing it.
func TestHooksRoutes_SurviveTheWebhookSunset(t *testing.T) {
	router := webhookSunsetRouter(t, func(cfg *config.Configuration) {
		cfg.WebhookDeprecationSunsetDate = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	})

	for _, route := range []webhookSunsetRoute{
		{method: http.MethodGet, path: "/hooks"},
		{method: http.MethodGet, path: "/hooks/hook_probe"},
		{method: http.MethodPost, path: "/hooks", body: `{"name":"probe"}`},
		{method: http.MethodPut, path: "/hooks/hook_probe", body: `{"name":"probe"}`},
		{method: http.MethodDelete, path: "/hooks/hook_probe"},
		// The live subscriber surface the retirement migrates callers TO must also survive.
		{method: http.MethodGet, path: "/subscribers"},
		{method: http.MethodGet, path: "/subscribers/sub_sunset_probe"},
	} {
		t.Run(route.name(), func(t *testing.T) {
			recorder := webhookSunsetRequest(t, router, route)

			assert.NotEqual(t, http.StatusGone, recorder.Code,
				"only the deprecated webhook-subscription routes are retired; body was: %s",
				recorder.Body.String())
			assert.NotEqual(t, apierror.ErrGenGone, webhookSunsetErrorCode(t, recorder))
			assert.Empty(t, recorder.Header().Get("Sunset"),
				"an unguarded route must not advertise a retirement it is not subject to")
		})
	}
}

// setupThrottledSunsetRouter builds the same router setupSunsetRouter does, but with a rate limit
// small enough that the SECOND request from one client is refused.
//
// The tiny limit is the whole apparatus. The production defaults are 2000 rps with a burst of
// 4000, so exhausting them would take four thousand requests and would prove the same thing far
// more slowly.
//
// Parameters:
//   - t *testing.T: the test, for require.
//   - sunsetDate string: the RFC3339 retirement instant.
//
// Returns:
//   - *gin.Engine: the assembled router, whose limiter admits one request per client.
func setupThrottledSunsetRouter(t *testing.T, sunsetDate string) *gin.Engine {
	t.Helper()

	rps := 1.0
	burst := 1

	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_sunset_throttled",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:    false,
			SecretKey: sunsetTestSecretKey,
		},
		RateLimit:                    config.RateLimitConfig{RequestsPerSecond: &rps, Burst: &burst},
		WebhookDeprecationSunsetDate: sunsetDate,
	})

	cnf, err := config.Fetch()
	require.NoError(t, err, "fetching the mocked configuration")
	require.NotNil(t, cnf.RateLimit.RequestsPerSecond,
		"the rate limit did not reach the configuration store, so nothing below would be throttled")
	require.Equal(t, sunsetDate, cnf.WebhookDeprecationSunsetDate,
		"the retirement instant did not reach the configuration store")

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err, "creating the datasource")

	instance, err := blnk.NewBlnk(db)
	require.NoError(t, err, "creating the Blnk instance")

	api := NewAPI(instance)
	require.NotNil(t, api, "NewAPI returned nil, which means configuration fetch failed")

	return api.Router()
}

// TestWebhookSunset_IsNotPreemptedByRateLimiting is the M-5 guard, asserted through a response
// rather than through the source.
//
// # The defect
//
// The retirement barrier was installed in api.Router, and every router.Use there runs AFTER every
// r.Use in NewAPI — where RateLimitMiddleware is installed. So a throttled request to a retired
// webhook-subscription route was answered 429 by the limiter and never reached either sunset
// guard. The caller was told to slow down and try again, about a surface that is gone, and would
// have retried it indefinitely. Requirement R-12 says the webhook REST API answers 410 Gone on
// every request; "every request except the throttled ones" does not satisfy it.
//
// # Why the limiter is exhausted rather than mocked
//
// The bug is one of ORDER, and order is a property of the assembled chain. A test that installed
// its own middleware would assemble a different chain and could pass while the real one stayed
// broken, which is precisely what every existing test in this file did — they authenticate, use a
// registered verb, and send one request each, so none of them is ever throttled.
func TestWebhookSunset_IsNotPreemptedByRateLimiting(t *testing.T) {
	router := setupThrottledSunsetRouter(t, pastSunset())

	// FIRST, prove the limiter is actually armed: a live route must start refusing. Without this
	// the test below would pass on a router that throttles nothing, which is the failure mode a
	// too-generous limit would produce and the reason the limit is asserted rather than assumed.
	var throttled bool
	for range 8 {
		rec := doSunsetRequest(t, router, http.MethodGet, "/ledgers", true)
		if rec.Code == http.StatusTooManyRequests {
			throttled = true

			break
		}
	}
	require.True(t, throttled,
		"the rate limiter never refused a live route, so nothing below would have been preempted "+
			"and this test would prove nothing")

	// NOW the retired path, with the limiter already exhausted for this client. Every one of these
	// would have been 429 before the barrier moved.
	for _, method := range []string{
		http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete,
		// The unregistered verbs too: they reach no route, so they are answered by the chain gin
		// rebuilds for unmatched requests — which contains the limiter as well.
		http.MethodPatch, http.MethodHead, http.MethodOptions,
	} {
		t.Run(method, func(t *testing.T) {
			rec := doSunsetRequest(t, router, method, retiredWebhookPath, true)

			require.Equal(t, http.StatusGone, rec.Code,
				"a throttled request to a retired route must still be told the route is GONE, not "+
					"that it should retry. Got %d: %s", rec.Code, rec.Body.String())

			// HEAD carries no body by definition, so the code assertion applies to the others.
			if method != http.MethodHead {
				assert.Equal(t, string(apierror.ErrGenGone), errorCodeOf(t, rec.Body.Bytes()),
					"the refusal must carry GEN_GONE, so a client can tell a retirement from a "+
						"throttle without parsing prose")
			}
		})
	}

	// AND THE NEIGHBOURS ARE STILL THROTTLED. Moving the barrier ahead of the limiter must not
	// have moved the limiter, and a barrier that somehow admitted everything would pass every
	// assertion above while disabling rate limiting for the whole API.
	rec := doSunsetRequest(t, router, http.MethodGet, "/ledgers", true)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code,
		"a live route must still be throttled; the barrier is scoped to the retired surface and "+
			"must not have displaced the limiter for anything else. Got %d", rec.Code)
}
