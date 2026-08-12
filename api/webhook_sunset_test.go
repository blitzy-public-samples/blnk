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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// the webhook subscription REST API answers 410 Gone on EVERY request, and nothing else
// in the API is retired with it.
//
// A guard attached only per route, only behind authentication, satisfies the obvious
// reading of the sunset rule — the four registered methods, called with a valid master key, all
// answer 410 — while leaving three whole populations of request answering something
// else:
//
//   - Callers that do not authenticate. Authentication would answer them first and tell
//     them their credentials are the problem, which is a claim about the caller rather
//     than about the surface.
//   - Methods nobody registered. PATCH, HEAD, OPTIONS and friends match no route, so
//     there is no route-attached handler for them to reach.
//   - Callers the rate limiter has already refused. A 429 tells a client to slow down
//     and try again, about a surface that is never coming back.
//
// All three are asserted below, and none of them would be revealed by any amount of
// testing the four registered verbs with a valid key.
//
// api/api.go installs the retirement twice, deliberately, and both installations are
// covered here:
//
//   - middleware.WebhookSunsetPreAuthGuard(), installed globally in NewAPI ahead of the
//     request-size limit, the rate limiter and Authenticate().
//   - middleware.WebhookSunsetGuard(), attached to each of the four registered routes.

// sunsetMasterKey is the master secret the secure-mode cases authenticate with.
//
// It is an obviously fake, clearly-labelled test value: it must never resemble a real
// credential, because the whole point of the secure-mode cases is that a request
// carrying it is fully authorised and STILL told the surface is gone.
const sunsetMasterKey = "not-a-real-secret-sunset-test-master-key"

// The two retirement instants every test is driven from.
//
// They are FIXED literals rather than time.Now offsets on purpose.
const (
	sunsetPassedInstant = "2020-01-01T00:00:00Z"
	sunsetFutureInstant = "2999-01-01T00:00:00Z"
)

// retiredProbeSubscriberID is the subscriber identifier the retired-surface probes
// address.
//
// It deliberately names no subscriber that exists.
const retiredProbeSubscriberID = "sub_sunset_probe"

// retiredWebhookPath is a concrete request path on the retired surface.
var retiredWebhookPath = webhookSubscriptionPath(retiredProbeSubscriberID)

// sunsetRoute is one request on the retired surface, together with the status that
// request answers while the dual-delivery window is still open.
//
// Pinning the normal status IN THE TABLE is what stops the "before the instant" half of
// the criterion from degenerating into "not 410".
type sunsetRoute struct {
	// method is the HTTP method, which is the only thing that varies across the
	// retired surface: it is four verbs on ONE path.
	method string
	// body is the request payload, supplied wherever the handler binds one so that a
	// refusal can never be confused with a bind failure that happens to share a status.
	body string
	// normalStatus is what the route answers inside the window, taken from
	// api/subscribers.go rather than guessed: POST 201 Created, GET 200 OK, PUT 200 OK,
	// DELETE 204 No Content.
	normalStatus int
}

// name renders a route for a subtest name.
//
// Returns:
//   - string: the HTTP method, which uniquely identifies a route on this surface.
func (r sunsetRoute) name() string {
	return r.method
}

// sunsetRoutes is every registered method on the retired surface.
//
// The list is exhaustive by requirement rather than by convenience: the rule is 410 on
// every request, so a verb missing from here would be a hole in the retirement reached
// by a client and by nothing in this file.
var sunsetRoutes = []sunsetRoute{
	{
		method:       http.MethodPost,
		body:         `{"webhook_url":"https://sunset-first.example.com/blnk-events"}`,
		normalStatus: http.StatusCreated,
	},
	{
		method:       http.MethodGet,
		normalStatus: http.StatusOK,
	},
	{
		method:       http.MethodPut,
		body:         `{"webhook_url":"https://sunset-second.example.net/kafka-cutover"}`,
		normalStatus: http.StatusOK,
	},
	{
		method:       http.MethodDelete,
		normalStatus: http.StatusNoContent,
	},
}

// sunsetUnregisteredMethods are methods the router registers for no route at all.
var sunsetUnregisteredMethods = []string{
	http.MethodPatch,
	http.MethodHead,
	http.MethodOptions,
	http.MethodTrace,
	"PROPFIND",
}

// sunsetFixture is the assembled state of one router under test.
//
// It is mutated only by sunsetRouterOption values before the router is built.
type sunsetFixture struct {
	// secure turns real authentication on. With it off, Authenticate() short-circuits
	// and never runs, so the 401 and 403 paths cannot be observed at all.
	secure bool
	// masterKeyPrincipal injects an authenticated master-key context ahead of the whole
	// chain, so that a refusal can only have come from the retirement.
	masterKeyPrincipal bool
	// rateLimitRPS and rateLimitBurst arm the limiter tightly enough that a second
	// request from one client is refused.
	rateLimitRPS   *float64
	rateLimitBurst *int
	// brokers and insecureBrokerTransport describe a publishing deployment, which is
	// what makes the configuration layer insist on a usable window.
	brokers                 []string
	insecureBrokerTransport bool
}

// sunsetRouterOption adjusts a fixture before its router is assembled.
type sunsetRouterOption func(*sunsetFixture)

// withSunsetSecureMode enables real authentication with sunsetMasterKey as the master
// secret.
//
// It is required by any case that asserts something about the ORDER of the retirement
// and authentication.
//
// Returns:
//   - sunsetRouterOption: the option.
func withSunsetSecureMode() sunsetRouterOption {
	return func(f *sunsetFixture) { f.secure = true }
}

// withSunsetMasterKeyPrincipal injects an authenticated master-key context ahead of the
// chain, the way this package's other router fixtures do.
//
// Returns:
//   - sunsetRouterOption: the option.
func withSunsetMasterKeyPrincipal() sunsetRouterOption {
	return func(f *sunsetFixture) { f.masterKeyPrincipal = true }
}

// withSunsetRateLimit arms the rate limiter.
//
// The production defaults are 2000 requests per second with a burst of 4000, so
// exhausting them would take four thousand requests to prove the same thing far more
// slowly.
//
// Parameters:
//   - requestsPerSecond float64: the sustained rate.
//   - burst int: the burst allowance.
//
// Returns:
//   - sunsetRouterOption: the option.
func withSunsetRateLimit(requestsPerSecond float64, burst int) sunsetRouterOption {
	return func(f *sunsetFixture) {
		rps := requestsPerSecond
		allowance := burst
		f.rateLimitRPS = &rps
		f.rateLimitBurst = &allowance
	}
}

// withSunsetKafkaBrokers describes a deployment that publishes to Kafka.
//
// The transport is declared insecure alongside it because that is what a single-broker
// local stack is, and because leaving it undeclared makes the configuration layer
// refuse the fixture for a reason that has nothing to do with the retirement.
//
// Parameters:
//   - brokers ...string: the broker list.
//
// Returns:
//   - sunsetRouterOption: the option.
func withSunsetKafkaBrokers(brokers ...string) sunsetRouterOption {
	return func(f *sunsetFixture) {
		f.brokers = brokers
		f.insecureBrokerTransport = true
	}
}

// sunsetConfiguration builds the configuration a fixture publishes.
//
// Parameters:
//   - sunset string: the RFC3339 retirement instant, or "" for a deployment that has
//     configured none.
//   - fixture sunsetFixture: the assembled fixture state.
//
// Returns:
//   - *config.Configuration: the configuration to publish.
func sunsetConfiguration(sunset string, fixture sunsetFixture) *config.Configuration {
	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_sunset",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:    fixture.secure,
			SecretKey: sunsetMasterKey,
		},
		WebhookDeprecationSunsetDate: sunset,
	}

	if fixture.rateLimitRPS != nil {
		cfg.RateLimit = config.RateLimitConfig{
			RequestsPerSecond: fixture.rateLimitRPS,
			Burst:             fixture.rateLimitBurst,
		}
	}

	if len(fixture.brokers) > 0 {
		cfg.Kafka.Brokers = fixture.brokers
		cfg.Kafka.InsecureLocalDev = fixture.insecureBrokerTransport
	}

	return cfg
}

// publishSunsetConfiguration publishes a configuration and proves it actually landed.
//
// THE PROOF IS NOT OPTIONAL. config.MockConfig validates before it stores and, on
// failure, logs and RETURNS — leaving the previous configuration in place.
//
// Parameters:
//   - t *testing.T: the test.
//   - sunset string: the retirement instant the fixture asked for.
//   - fixture sunsetFixture: the assembled fixture state.
//
// Returns:
//   - *config.Configuration: the published configuration.
func publishSunsetConfiguration(t *testing.T, sunset string, fixture sunsetFixture) *config.Configuration {
	t.Helper()

	config.MockConfig(sunsetConfiguration(sunset, fixture))

	cnf, err := config.Fetch()
	require.NoError(t, err, "fetching the published configuration")
	require.Equal(t, sunset, cnf.WebhookDeprecationSunsetDate,
		"the retirement instant did not reach the configuration store. config.MockConfig "+
			"keeps the PREVIOUS configuration when validation fails, so every assertion "+
			"below would have been made against the wrong deployment")

	return cnf
}

// buildSunsetRouter is the ONE construction path behind both public fixtures.
//
// Two details are correctness rather than convenience:
//
//   - Router() calls router.Use, so it must be invoked exactly once per engine.
//   - The configuration is published BEFORE NewBlnk and NewAPI, because both capture
//     what they were constructed with: NewAPI resolves the rate limiter and the
//     middleware chain at construction time.
//
// Parameters:
//   - t *testing.T: the test.
//   - sunset string: the RFC3339 retirement instant, or "" for none.
//   - fixture sunsetFixture: the assembled fixture state.
//   - registryFor func(*config.Configuration) database.IDataSource: builds the
//     datasource from the configuration that was actually published.
//
// Returns:
//   - *gin.Engine: the router, with every route registered as production registers it.
func buildSunsetRouter(
	t *testing.T,
	sunset string,
	fixture sunsetFixture,
	registryFor func(cnf *config.Configuration) database.IDataSource,
) *gin.Engine {
	t.Helper()

	cnf := publishSunsetConfiguration(t, sunset, fixture)
	t.Cleanup(func() { config.MockConfig(sunsetConfiguration("", sunsetFixture{})) })

	instance, err := blnk.NewBlnk(registryFor(cnf))
	require.NoError(t, err, "creating the Blnk instance")

	apiInstance := NewAPI(instance)
	require.NotNil(t, apiInstance,
		"NewAPI returned nil, which means the configuration it fetched was unusable")

	if fixture.masterKeyPrincipal {
		// Installed before Router(), so it precedes the chain Router() adds — including
		// Authenticate(), which is what lets a non-secure fixture present a master-key
		// principal without a header.
		apiInstance.router.Use(func(c *gin.Context) {
			c.Set("isMasterKey", true)
			c.Next()
		})
	}

	return apiInstance.Router()
}

// setupSunsetRouter builds a router backed by the real registry, at a chosen retirement
// instant.
//
// This is the fixture almost every case here uses.
//
// Parameters:
//   - t *testing.T: the test.
//   - sunset string: the RFC3339 retirement instant, or "" for a deployment that has
//     configured none.
//   - opts ...sunsetRouterOption: fixture adjustments.
//
// Returns:
//   - *gin.Engine: the assembled router.
func setupSunsetRouter(t *testing.T, sunset string, opts ...sunsetRouterOption) *gin.Engine {
	t.Helper()

	var fixture sunsetFixture
	for _, opt := range opts {
		opt(&fixture)
	}

	return buildSunsetRouter(t, sunset, fixture,
		func(cnf *config.Configuration) database.IDataSource {
			registry, err := database.NewDataSource(cnf)
			require.NoError(t, err, "creating the datasource")

			return registry
		})
}

// setupSunsetRouterWithRegistrySpy builds the same router over a SPY datasource and
// hands the spy back, so a test can assert what the registry was — and was not — asked
// to do.
//
// It exists for one assertion the real datasource cannot make.
//
// Parameters:
//   - t *testing.T: the test.
//   - sunset string: the RFC3339 retirement instant, or "" for none.
//   - opts ...sunsetRouterOption: fixture adjustments.
//
// Returns:
//   - *gin.Engine: the assembled router.
//   - *mocks.MockDataSource: the registry spy backing it.
func setupSunsetRouterWithRegistrySpy(
	t *testing.T,
	sunset string,
	opts ...sunsetRouterOption,
) (*gin.Engine, *mocks.MockDataSource) {
	t.Helper()

	var fixture sunsetFixture
	for _, opt := range opts {
		opt(&fixture)
	}

	spy := new(mocks.MockDataSource)

	router := buildSunsetRouter(t, sunset, fixture,
		func(*config.Configuration) database.IDataSource { return spy })

	return router, spy
}

// sunsetRequestAs issues one request through the assembled router, optionally carrying
// an API key.
//
// The peer is forced to loopback. httptest.NewRequest records 192.0.2.1 — a
// documentation address, deliberately not loopback — and the credential-issuance
// endpoint refuses to put a one-time secret on a channel it cannot establish as
// confidential. Without this, the blast-radius case covering POST
// /subscribers/{id}/kafka-credentials would be asserting against a transport refusal
// rather than against the route.
//
// Parameters:
//   - t *testing.T: the test.
//   - router *gin.Engine: the router under test.
//   - method string: the HTTP method.
//   - path string: the request path.
//   - body string: the request payload, or "" for none.
//   - apiKey string: the X-Blnk-Key value, or "" to send no credential at all.
//
// Returns:
//   - *httptest.ResponseRecorder: the recorded response.
func sunsetRequestAs(
	t *testing.T,
	router *gin.Engine,
	method, path, body, apiKey string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		request.Header.Set("X-Blnk-Key", apiKey)
	}
	request.RemoteAddr = "127.0.0.1:54321"

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

// sunsetRequest issues one unauthenticated-by-header request through the router.
//
// "Unauthenticated by header" is not the same as unauthorised: a fixture built with
// withSunsetMasterKeyPrincipal presents a master-key principal from context, and a
// fixture built without secure mode is not challenged at all.
//
// Parameters:
//   - t *testing.T: the test.
//   - router *gin.Engine: the router under test.
//   - method string: the HTTP method.
//   - path string: the request path.
//   - body string: the request payload, or "" for none.
//
// Returns:
//   - *httptest.ResponseRecorder: the recorded response.
func sunsetRequest(
	t *testing.T,
	router *gin.Engine,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	return sunsetRequestAs(t, router, method, path, body, "")
}

// sunsetErrorCode reads the machine-readable code out of a response, or "" when the
// response carries none.
//
// THE CODE IS NESTED.
//
// Parameters:
//   - t *testing.T: the test.
//   - recorder *httptest.ResponseRecorder: the response to read.
//
// Returns:
//   - apierror.ErrorCode: the code carried in error_detail, or "".
func sunsetErrorCode(t *testing.T, recorder *httptest.ResponseRecorder) apierror.ErrorCode {
	t.Helper()

	var envelope struct {
		ErrorDetail apierror.APIError `json:"error_detail"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		return ""
	}

	return envelope.ErrorDetail.Code
}

// expectedSunsetHeaders returns the advisory header values the currently published
// window must produce.
//
// Parameters:
//   - t *testing.T: the test.
//
// Returns:
//   - string: the expected RFC 8594 Sunset header value, in GMT as the field requires.
//   - string: the expected RFC 9745 Deprecation header value for the window's opening
//     instant.
func expectedSunsetHeaders(t *testing.T) (string, string) {
	t.Helper()

	windowStart, sunsetInstant, configured := blnk.WebhookDeprecationWindow()
	require.True(t, configured,
		"a configured retirement instant must resolve to a window; with none there is "+
			"nothing to advertise and this helper must not be called")

	return sunsetInstant.UTC().Format(http.TimeFormat), middleware.DeprecationHeaderValue(windowStart)
}

// assertSunsetHeadersAdvertised asserts that a response carries both advisory headers
// for the published window.
//
// It is applied to SUCCESSFUL responses as well as refusals, and that is the
// substantive half.
//
// Parameters:
//   - t *testing.T: the test.
//   - recorder *httptest.ResponseRecorder: the response to inspect.
func assertSunsetHeadersAdvertised(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()

	sunsetHeader, deprecationHeader := expectedSunsetHeaders(t)

	assert.Equal(t, sunsetHeader, recorder.Header().Get("Sunset"),
		"the Sunset header must carry the configured instant, rendered in GMT as the "+
			"field's layout claims")
	assert.Equal(t, deprecationHeader, recorder.Header().Get("Deprecation"),
		"the Deprecation header must be the RFC 9745 structured-field date of the "+
			"window's OPENING instant, rendered from the SAME resolution as the Sunset "+
			"header beside it, so the two cannot describe windows that disagree")
}

// assertNotRetired asserts that a response is not the retirement.
//
// Both halves are needed.
//
// Parameters:
//   - t *testing.T: the test.
//   - recorder *httptest.ResponseRecorder: the response to inspect.
//   - method string: the method, for the failure message.
//   - path string: the path, for the failure message.
//   - why string: why this request must not be retired.
func assertNotRetired(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	method, path, why string,
) {
	t.Helper()

	// Sanitised because one of the routes this walks is the credential issuance route: a
	// diagnostic that rendered a successful response from it would carry a SASL password.
	assert.NotEqual(t, http.StatusGone, recorder.Code,
		"%s %s was retired by the webhook sunset: %s. %s",
		method, path, why, safeResponseBody(recorder))
	assert.NotEqual(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder),
		"%s %s carries the retirement code: %s", method, path, why)
}

// TestWebhookSunset_GoneStatusComesFromCodeCatalogue is the precondition every other
// test in this file rests on, asserted on its own so that a catalogue regression fails
// HERE with an unambiguous message instead of surfacing as a handful of confusing 500s.
//
// apierror.StatusForCode defaults UNKNOWN codes to 500.
func TestWebhookSunset_GoneStatusComesFromCodeCatalogue(t *testing.T) {
	require.Equal(t, http.StatusGone, apierror.StatusForCode(apierror.ErrGenGone),
		"internal/apierror must map the retirement code to 410. Without the statusByCode "+
			"entry StatusForCode falls back to 500, and the sunset guard would answer "+
			"Internal Server Error to every request on the retired surface")

	assert.Equal(t, apierror.ErrorCode("GEN_GONE"), apierror.ErrGenGone,
		"GEN_GONE is the CLIENT-VISIBLE contract: it is the string a subscriber branches "+
			"on to tell a retirement from any other refusal. Renaming the constant is a "+
			"breaking change, so the literal is pinned here rather than only referenced")

	assert.Equal(t, apierror.ErrGenGone, apierror.Normalize(apierror.ErrGenGone),
		"the retirement code must be canonical, i.e. absent from legacyToCanonical. Were "+
			"it mapped there, responses would surface some other code and a client "+
			"branching on GEN_GONE would never see it")
}

// registered or not, answered 410 Gone with the GEN_GONE code.
//
// The status is asserted TOGETHER WITH the code on every case, and both matter.
func TestWebhookSunset_ReturnsGoneAfterSunsetDate(t *testing.T) {
	// Fully authorised, so nobody can claim the 410 was really an authorization failure.
	router := setupSunsetRouter(t, sunsetPassedInstant, withSunsetMasterKeyPrincipal())

	for _, route := range sunsetRoutes {
		t.Run("registered "+route.name(), func(t *testing.T) {
			recorder := sunsetRequest(t, router, route.method, retiredWebhookPath, route.body)

			// assertErrorCode is the package's shared reader: it asserts the status, the nested
			// error_detail.code, a non-empty message AND the legacy flat "error" field. That
			// last one is the dual response contract the middleware's abortWithCode produces,
			// and it is what keeps a pre-catalogue client working.
			assertErrorCode(t, recorder, http.StatusGone, apierror.ErrGenGone)

			// The literal, so the client-visible code is pinned on the wire and cannot be
			// renamed by a change that keeps the Go constant compiling.
			assert.Contains(t, recorder.Body.String(), `"GEN_GONE"`,
				"the response body must carry the literal code a client branches on")

			assert.Contains(t, recorder.Body.String(), "retired and is no longer available",
				"the body must say what happened, and say the same thing on every route. "+
					"It says RETIRED rather than removed because the handlers are still "+
					"registered and still compiled: the instant withdraws the behaviour, "+
					"deleting the surface is a later release, and moving the date back "+
					"restores it")
			assert.Contains(t, recorder.Body.String(), "Kafka event stream",
				"and it must name the replacement, or a client has nowhere to go")

			assertSunsetHeadersAdvertised(t, recorder)
		})
	}

	// The unregistered verbs are the substantive half of "every request". They match no
	// route, so nothing route-attached can answer them: the global pre-auth barrier is
	// what makes the criterion true for them, and without it gin would answer as though
	// the path had never existed.
	for _, method := range sunsetUnregisteredMethods {
		t.Run("unregistered "+method, func(t *testing.T) {
			recorder := sunsetRequest(t, router, method, retiredWebhookPath, "")

			require.Equal(t, http.StatusGone, recorder.Code,
				"%s reached no route, so no route-attached guard could answer it; the "+
					"global barrier is what makes 'every request' true. Got %d: %s",
				method, recorder.Code, recorder.Body.String())

			// A HEAD response carries no body on the wire, but httptest.ResponseRecorder records
			// what the handler WROTE — net/http is what discards it when serving a real
			// connection. So the recorded body still proves the refusal was formed correctly,
			// and the suppression is the server's job rather than the guard's.
			assert.Equal(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder),
				"an unregistered verb must be refused with the retirement code too, so a "+
					"client can tell a retirement from a routing mistake")
		})
	}

	t.Run("a body is not required to be refused", func(t *testing.T) {
		// A retired route must not read the request before refusing it. Binding first would
		// answer a malformed-body error to a caller whose real problem is that the surface is
		// gone, and would make the refusal depend on what was sent.
		recorder := sunsetRequest(t, router, http.MethodPost, retiredWebhookPath,
			"}{ not json at all")

		assertErrorCode(t, recorder, http.StatusGone, apierror.ErrGenGone)
	})

	t.Run("a non-master caller is told Gone, not Forbidden", func(t *testing.T) {
		// No master-key principal at all. The retirement precedes the master-key gate, and
		// that ordering is the correct one: the resource no longer exists, and which
		// principal is asking cannot change that.
		nonMaster := setupSunsetRouter(t, sunsetPassedInstant)

		for _, route := range sunsetRoutes {
			recorder := sunsetRequest(t, nonMaster, route.method, retiredWebhookPath, route.body)

			assert.Equal(t, http.StatusGone, recorder.Code, "%s", route.name())
			assert.Equal(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder), "%s", route.name())
			assert.NotEqual(t, apierror.ErrAuthMasterKeyRequired, sunsetErrorCode(t, recorder),
				"%s: the master-key gate must not answer ahead of the retirement", route.name())
		}
	})
}

// TestWebhookSunset_ReturnsGoneBeforeAuthentication is the ordering assertion, and it
// is the one that fails the moment the retirement is installed behind authentication
// instead of ahead of it.
//
// Every credential state must reach 410.
func TestWebhookSunset_ReturnsGoneBeforeAuthentication(t *testing.T) {
	router := setupSunsetRouter(t, sunsetPassedInstant, withSunsetSecureMode())

	t.Run("control: a live route really does challenge an unauthenticated caller", func(t *testing.T) {
		// A LIVE route under the very same prefix. If this did not challenge, the 410s
		// below could not be distinguished from authentication being inert.
		recorder := sunsetRequest(t, router, http.MethodGet, "/subscribers", "")

		require.Equal(t, http.StatusUnauthorized, recorder.Code,
			"a live route must challenge an unauthenticated caller when secure mode is "+
				"on; without that this test cannot tell guard ordering from disabled "+
				"authentication. body: %s", recorder.Body.String())
	})

	for name, apiKey := range map[string]string{
		"no credentials at all":            "",
		"a key that was never issued":      "a-key-that-was-never-issued",
		"the master key, fully authorised": sunsetMasterKey,
	} {
		t.Run(name, func(t *testing.T) {
			// Every registered verb, plus one unregistered verb, because the two reach
			// the barrier by different routes through gin.
			for _, method := range append(
				[]string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete},
				http.MethodPatch,
			) {
				recorder := sunsetRequestAs(t, router, method, retiredWebhookPath, "", apiKey)

				require.NotEqual(t, http.StatusUnauthorized, recorder.Code,
					"%s: 401 BEFORE 410 IS THE DEFECT. The surface is gone, so a "+
						"credential is not what is missing", method)
				require.NotEqual(t, http.StatusForbidden, recorder.Code,
					"%s: and 403 is the same mistake — no scope grants access to a "+
						"route that no longer exists", method)
				require.Equal(t, http.StatusGone, recorder.Code,
					"%s must be Gone regardless of credentials. Got %d: %s",
					method, recorder.Code, recorder.Body.String())
				assert.Equal(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder),
					"%s must carry the retirement code, not an auth code", method)
			}
		})
	}
}

// TestWebhookSunset_ReturnsGoneWithoutReachingTheRegistry proves the retirement REFUSES
// the request rather than performing it and then denying it.
//
// The mirror matters as much as the assertion.
func TestWebhookSunset_ReturnsGoneWithoutReachingTheRegistry(t *testing.T) {
	// The three repository methods the four retired handlers reach, and the only ways the
	// registry can be touched from this surface:
	//   GET    -> GetEventSubscriberByID          (blnk.GetEventSubscriber)
	//   POST   -> RecordSubscriberWebhookURL      (blnk.RecordSubscriberWebhookSubscription)
	//   PUT    -> RecordSubscriberWebhookURL      (same write)
	//   DELETE -> CompleteSubscriberWebhookMigration
	//             (blnk.CompleteEventSubscriberWebhookMigration)
	registryMethods := []string{
		"GetEventSubscriberByID",
		"RecordSubscriberWebhookURL",
		"CompleteSubscriberWebhookMigration",
	}

	t.Run("after the instant the registry is never consulted", func(t *testing.T) {
		router, spy := setupSunsetRouterWithRegistrySpy(
			t, sunsetPassedInstant, withSunsetMasterKeyPrincipal(),
		)

		for _, route := range sunsetRoutes {
			recorder := sunsetRequest(t, router, route.method, retiredWebhookPath, route.body)
			require.Equal(t, http.StatusGone, recorder.Code,
				"%s must be refused before the handler runs. Got %d: %s",
				route.name(), recorder.Code, recorder.Body.String())
			require.Equal(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder), "%s", route.name())
		}

		for _, method := range registryMethods {
			spy.AssertNotCalled(t, method, mock.Anything, mock.Anything)
		}

		// No expectation was ever set on the spy, so this asserts the stronger property: the
		// datasource was not touched AT ALL, by any method, including any the four handlers
		// might reach in a future refactor.
		spy.AssertExpectations(t)
		assert.Empty(t, spy.Calls,
			"the retired surface must not touch the datasource at all past the instant; "+
				"calls recorded: %v", spy.Calls)
	})

	t.Run("inside the window the very same request does reach the registry", func(t *testing.T) {
		router, spy := setupSunsetRouterWithRegistrySpy(
			t, sunsetFutureInstant, withSunsetMasterKeyPrincipal(),
		)

		// A canonical identifier, because a non-canonical one is refused by the service
		// before the store is reached and the mirror would then prove nothing.
		subscriberID := uniqueSubscriberID()

		// Answered as an unknown subscriber. The VALUE of the read is irrelevant here;
		// that the read happened at all is the whole assertion.
		spy.On("GetEventSubscriberByID", mock.Anything, subscriberID).
			Return(nil, apierror.NewAPIError(
				apierror.ErrGenNotFound, "subscriber not found", nil,
			))

		recorder := sunsetRequest(
			t, router, http.MethodGet, webhookSubscriptionPath(subscriberID), "",
		)

		require.Equal(t, http.StatusNotFound, recorder.Code,
			"inside the window the handler runs, so an unknown subscriber is 404 — not "+
				"410, and not a 200 conjured by a guard that swallowed the request. "+
				"body: %s", recorder.Body.String())
		assert.Equal(t, apierror.ErrSubscriberNotFound, sunsetErrorCode(t, recorder),
			"and the answer comes from the registry layer, which is what makes this the "+
				"mirror of the assertion above")

		spy.AssertCalled(t, "GetEventSubscriberByID", mock.Anything, subscriberID)
		spy.AssertExpectations(t)
	})
}

// TestWebhookSunset_RoutesAnswerNormallyBeforeSunsetDate is the other half of the
// criterion, and without it every assertion above is satisfied by a router that answers
// 410 always.
//
// The exact statuses are PINNED rather than merely asserted to be "not 410".
func TestWebhookSunset_RoutesAnswerNormallyBeforeSunsetDate(t *testing.T) {
	t.Run("a future instant: the routes answer exactly as they always did", func(t *testing.T) {
		router := setupSunsetRouter(t, sunsetFutureInstant, withSunsetMasterKeyPrincipal())

		// A REAL registered subscriber, so each handler can complete its own work and the
		// documented success status is reachable. Registration and cleanup are the shared
		// helpers from api/subscribers_api_test.go, so this file does not maintain a second
		// notion of what a valid subscriber is.
		subscriberID := registerSubscriberForWebhookTests(t, router)
		path := webhookSubscriptionPath(subscriberID)

		// Ordered deliberately: record, read, replace, then complete the migration. The
		// DELETE is last because it forgets the URL and stamps the migration, which is the
		// terminal state of this surface.
		for _, route := range sunsetRoutes {
			t.Run(route.name(), func(t *testing.T) {
				recorder := sunsetRequest(t, router, route.method, path, route.body)

				require.Equal(t, route.normalStatus, recorder.Code,
					"inside the window %s must answer %d, the status api/subscribers.go "+
						"documents. Got %d: %s",
					route.method, route.normalStatus, recorder.Code, recorder.Body.String())
				assert.NotEqual(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder),
					"a succeeding response must never carry the retirement code")

				// Warned by the very responses it is succeeding with, which is the point
				// of advertising the window on both sides of the boundary.
				assertSunsetHeadersAdvertised(t, recorder)
			})
		}
	})

	t.Run("an unregistered verb is not retired early either", func(t *testing.T) {
		router := setupSunsetRouter(t, sunsetFutureInstant, withSunsetMasterKeyPrincipal())

		recorder := sunsetRequest(t, router, http.MethodPatch, retiredWebhookPath, "")

		assertNotRetired(t, recorder, http.MethodPatch, retiredWebhookPath,
			"PATCH is unsupported, which is not the same as retired; answering 410 before "+
				"the instant would retire the surface early for one class of caller")
	})

	t.Run("authentication still applies inside the window", func(t *testing.T) {
		// The barrier passes the request on before the instant, so the chain behind it must
		// be intact. A missing credential is answered by authentication, as always.
		router := setupSunsetRouter(t, sunsetFutureInstant, withSunsetSecureMode())

		recorder := sunsetRequest(t, router, http.MethodGet, retiredWebhookPath, "")

		assert.Equal(t, http.StatusUnauthorized, recorder.Code,
			"inside the window the deprecated routes are ordinary authenticated routes. "+
				"body: %s", recorder.Body.String())
	})

	t.Run("no configured instant retires nothing, and advertises nothing", func(t *testing.T) {
		// A legitimate steady state, not a misconfiguration: it is the state every deployment
		// that has not yet scheduled its migration is in, and the state every deployment
		// predating this feature is in. A guard that read an absent instant as a passed one
		// would retire the surface on upgrade, for everybody, without anyone having chosen a
		// date.
		router := setupSunsetRouter(t, "", withSunsetMasterKeyPrincipal())

		for _, route := range sunsetRoutes {
			recorder := sunsetRequest(t, router, route.method, retiredWebhookPath, route.body)

			assertNotRetired(t, recorder, route.method, retiredWebhookPath,
				"a deployment that has scheduled no retirement keeps working indefinitely")
			assert.Empty(t, recorder.Header().Get("Sunset"),
				"%s: there is no instant to advertise, so the header must be ABSENT "+
					"rather than empty-valued or invented", route.name())
			assert.Empty(t, recorder.Header().Get("Deprecation"),
				"%s: and no deprecation may be announced either", route.name())
		}
	})
}

// sunsetHookLifecycle is one observed run of the whole /hooks lifecycle: the status every step
// answered, the identifier the registry minted, and the hook the registry read back.
type sunsetHookLifecycle struct {
	// statuses are the HTTP statuses of the steps, in the order sunsetDriveHookLifecycle
	// performs them, keyed by step name so a mismatch names the step rather than an index.
	statuses map[string]int
	// hookID is the identifier the registry assigned to the created hook.
	hookID string
	// readBack is the hook as the registry returned it from GET /hooks/{id}.
	readBack hooks.Hook
	// listedIDs are the identifiers GET /hooks reported.
	listedIDs []string
	// headers records, per step, whether the response advertised a retirement.
	advertised map[string]string
}

// sunsetDriveHookLifecycle registers a hook, reads it, lists it, updates it, deletes
// it, and reads it again — through the router given, recording what happened at every
// step.
//
// Parameters:
//   - t *testing.T: the test.
//   - router *gin.Engine: the router to drive.
//   - label string: distinguishes this run's hook from the other run's.
//
// Returns:
//   - sunsetHookLifecycle: everything observed, for comparison and for direct
//     assertion.
func sunsetDriveHookLifecycle(t *testing.T, router *gin.Engine, label string) sunsetHookLifecycle {
	t.Helper()

	observed := sunsetHookLifecycle{
		statuses:   map[string]int{},
		advertised: map[string]string{},
	}

	record := func(step string, recorder *httptest.ResponseRecorder) *httptest.ResponseRecorder {
		observed.statuses[step] = recorder.Code
		observed.advertised[step] = recorder.Header().Get("Sunset") +
			recorder.Header().Get("Deprecation")

		return recorder
	}

	fixture := hooks.Hook{
		Name:    "sunset lifecycle probe " + label,
		URL:     "https://hooks.example.com/blnk-" + label,
		Type:    hooks.PreTransaction,
		Active:  true,
		Timeout: 30,
	}
	payload, err := json.Marshal(fixture)
	require.NoError(t, err, "marshalling the hook fixture")

	created := record("register",
		sunsetRequest(t, router, http.MethodPost, "/hooks", string(payload)))
	if created.Code != http.StatusCreated {
		// Returned early rather than dereferenced: the caller asserts on the status, and
		// decoding a failure body as a hook would report a JSON error in place of the status
		// mismatch that is the real finding.
		return observed
	}

	var registered hooks.Hook
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &registered),
		"a 201 must carry the registered hook: %s", created.Body.String())
	require.NotEmpty(t, registered.ID,
		"the registry must assign an identifier, or nothing that follows can address the hook")
	observed.hookID = registered.ID

	read := record("read",
		sunsetRequest(t, router, http.MethodGet, "/hooks/"+registered.ID, ""))
	if read.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(read.Body.Bytes(), &observed.readBack),
			"a 200 must carry the hook: %s", read.Body.String())
	}

	// TWO LISTINGS, and the distinction is the manager's rather than this test's: hooks
	// are indexed in Redis under a per-TYPE set, so an unfiltered request addresses the
	// set for the empty type and legitimately reports nothing. Both are driven — the
	// unfiltered one for its status, the typed one because it is the only one that can
	// show the hook is really indexed.
	record("list", sunsetRequest(t, router, http.MethodGet, "/hooks", ""))

	typed := record("list by type", sunsetRequest(t, router,
		http.MethodGet, "/hooks?type="+string(hooks.PreTransaction), ""))
	if typed.Code == http.StatusOK {
		var all []hooks.Hook
		require.NoError(t, json.Unmarshal(typed.Body.Bytes(), &all),
			"a 200 must carry the hook list: %s", typed.Body.String())
		for _, one := range all {
			observed.listedIDs = append(observed.listedIDs, one.ID)
		}
	}

	renamed := registered
	renamed.Name = "sunset lifecycle probe " + label + " (renamed)"
	updatePayload, err := json.Marshal(renamed)
	require.NoError(t, err)

	record("update", sunsetRequest(t, router,
		http.MethodPut, "/hooks/"+registered.ID, string(updatePayload)))

	record("delete", sunsetRequest(t, router,
		http.MethodDelete, "/hooks/"+registered.ID, ""))

	record("read after delete",
		sunsetRequest(t, router, http.MethodGet, "/hooks/"+registered.ID, ""))

	return observed
}

// sunsetHookLifecycleExpectations is the status every step must answer on a WORKING
// /hooks surface, stated independently of what either router happens to return.
//
// Comparing the two runs to each other is necessary and not sufficient: two routers
// that both answered 404, or both 500, would agree perfectly.
var sunsetHookLifecycleExpectations = map[string]int{
	"register":          http.StatusCreated,
	"read":              http.StatusOK,
	"list":              http.StatusOK,
	"list by type":      http.StatusOK,
	"update":            http.StatusOK,
	"delete":            http.StatusOK,
	"read after delete": http.StatusNotFound,
}

// TestWebhookSunset_HooksRoutesUnaffectedAfterSunset guards the most expensive mistake
// available in this package.
//
// So the whole LIFECYCLE is driven — register, read, list, update, delete, read again —
// twice: on a router whose retirement instant is in the future, and on one whose
// instant has passed. Three properties are then required, and each excludes a different
// failure:
//
//  1. THE EXPECTED STATUSES, written out in sunsetHookLifecycleExpectations.
//  2. IDENTICAL ANSWERS EITHER SIDE OF THE INSTANT. This is the retirement-specific
//     property: the passed instant must change nothing, step for step.
//  3. OBSERVABLE REGISTRY STATE after the instant has passed. The hook read back is the
//     hook that was registered, the listing contains it, and it is gone after the
//     delete — so a surface answering 200 without storing anything fails here.
func TestWebhookSunset_HooksRoutesUnaffectedAfterSunset(t *testing.T) {
	inWindow := sunsetDriveHookLifecycle(t,
		setupSunsetRouter(t, sunsetFutureInstant, withSunsetMasterKeyPrincipal()),
		"in-window")

	retired := sunsetDriveHookLifecycle(t,
		setupSunsetRouter(t, sunsetPassedInstant, withSunsetMasterKeyPrincipal()),
		"retired")

	for step, want := range sunsetHookLifecycleExpectations {
		assert.Equalf(t, want, retired.statuses[step],
			"AFTER THE RETIREMENT INSTANT, %q on /hooks answered %d instead of %d. /hooks is the "+
				"PRE_TRANSACTION and POST_TRANSACTION callout feature, which is fully supported and "+
				"must be untouched by the webhook transport's retirement — and a regression to any "+
				"status, not only to 410, takes it down",
			step, retired.statuses[step], want)

		assert.Equalf(t, inWindow.statuses[step], retired.statuses[step],
			"the retirement instant changed the answer to %q from %d to %d; an identical request "+
				"either side of the instant must produce an identical status",
			step, inWindow.statuses[step], retired.statuses[step])

		assert.Emptyf(t, retired.advertised[step],
			"%q advertised a Sunset or Deprecation it is not subject to: %q",
			step, retired.advertised[step])
		assert.Emptyf(t, inWindow.advertised[step],
			"%q advertised a retirement inside the window too: %q",
			step, inWindow.advertised[step])
	}

	// OBSERVABLE STATE, on the RETIRED router: the surface did the work rather than merely
	// answering.
	require.NotEmpty(t, retired.hookID,
		"the retired router's registry must have minted an identifier")
	assert.Equal(t, retired.hookID, retired.readBack.ID,
		"the hook read back must be the hook that was registered")
	assert.Equal(t, "sunset lifecycle probe retired", retired.readBack.Name,
		"and it must carry the values it was registered with, or the surface answered 200 without "+
			"storing anything")
	assert.Equal(t, hooks.PreTransaction, retired.readBack.Type,
		"including the hook TYPE, which is what decides whether it runs before or after a "+
			"transaction")
	assert.Contains(t, retired.listedIDs, retired.hookID,
		"the typed listing must report the hook the registry holds, or the surface answered 201 "+
			"without indexing anything")
	assert.NotEqual(t, inWindow.hookID, retired.hookID,
		"the two runs must be distinct registrations, or one run's observable state could be "+
			"mistaken for the other's")
}

// TestWebhookSunset_RetiresNothingBeyondTheRetiredSurface is the blast-radius assertion
// for everything that is not /hooks.
//
// A globally installed barrier is offered every request in the service, so the blast
// radius of a sloppy path match is the whole API. The routes below are the neighbours
// most at risk and the ones whose loss would hurt most:
//
//   - The live subscriber registry and the credential endpoint are the REPLACEMENT for
//     the retired surface.
//   - The /events routes are the feature the retirement exists to move traffic to.
//   - The near-miss paths are the ones a prefix or substring match would wrongly claim.
//   - The health route is what an orchestrator uses to decide whether the process is
//     alive: retiring it would take the deployment down rather than a transport.
func TestWebhookSunset_RetiresNothingBeyondTheRetiredSurface(t *testing.T) {
	router := setupSunsetRouter(t, sunsetPassedInstant, withSunsetMasterKeyPrincipal())

	for name, target := range map[string]struct {
		method string
		path   string
		body   string
		why    string
	}{
		"the live subscriber registry": {
			method: http.MethodGet,
			path:   "/subscribers",
			why:    "the registry is the replacement for the retired surface",
		},
		"a single subscriber": {
			method: http.MethodGet,
			path:   "/subscribers/" + retiredProbeSubscriberID,
			why:    "reading a subscriber is not reading its webhook subscription",
		},
		"subscriber update": {
			method: http.MethodPut,
			path:   "/subscribers/" + retiredProbeSubscriberID,
			body:   `{"name":"renamed"}`,
			why:    "the generic update is a supported route on the same prefix",
		},
		"subscriber deletion": {
			method: http.MethodDelete,
			path:   "/subscribers/" + retiredProbeSubscriberID,
			why:    "deregistering a subscriber must stay possible after the cutover",
		},
		"kafka credential issuance": {
			method: http.MethodPost,
			path:   "/subscribers/" + retiredProbeSubscriberID + "/kafka-credentials",
			why:    "retiring this would remove the migration path off webhooks entirely",
		},
		"the dead-letter inventory": {
			method: http.MethodGet,
			path:   "/events/dead-letter",
			why:    "the event surface is the feature the retirement moves traffic to",
		},
		"a dead-letter replay": {
			method: http.MethodPost,
			path:   "/events/dead-letter/evt_sunset_probe/replay",
			why:    "replay is how a dead-lettered event is recovered",
		},
		"the outbox statistics": {
			method: http.MethodGet,
			path:   "/events/stats",
			why:    "the daily zero-loss reconciliation reads this route",
		},
		"an unrelated ledger route": {
			method: http.MethodGet,
			path:   "/ledgers",
			why:    "the retirement is scoped to one transport, not to the API",
		},
		"the health route": {
			method: http.MethodGet,
			path:   "/",
			why:    "retiring this would take the deployment down, not a transport",
		},
		"a path one segment short of the retired shape": {
			method: http.MethodGet,
			path:   "/subscribers/webhook-subscription",
			why:    "it merely shares the words; a substring match would claim it",
		},
		"a path one segment past the retired shape": {
			method: http.MethodGet,
			path:   "/subscribers/" + retiredProbeSubscriberID + "/webhook-subscription/extra",
			why:    "a prefix match would retire it by association",
		},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := sunsetRequest(t, router, target.method, target.path, target.body)

			assertNotRetired(t, recorder, target.method, target.path, target.why)
		})
	}
}

// TestWebhookSunset_TheTwoBarriersCoverTheSameRoutes is the drift guard between the
// global pre-auth barrier and the per-route guard, and it walks the ASSEMBLED ROUTER
// rather than the source.
func TestWebhookSunset_TheTwoBarriersCoverTheSameRoutes(t *testing.T) {
	router := setupSunsetRouter(t, sunsetPassedInstant, withSunsetMasterKeyPrincipal())

	retiredRoutes := 0

	for _, route := range router.Routes() {
		// The matcher reads a CONCRETE request path, so the route template's parameter is
		// filled in the way a client would fill it.
		concrete := strings.ReplaceAll(route.Path, ":subscriber_id", retiredProbeSubscriberID)
		claimed := middleware.IsDeprecatedWebhookSubscriptionPath(concrete)

		// The suffix is an INDEPENDENT reading of "is this route deprecated", written here
		// rather than derived from middleware.DeprecatedWebhookSubscriptionRoute on purpose.
		// Deriving the expectation from the same constant the matcher consults would make
		// this comparison circular: the two would agree by construction and the test would be
		// incapable of failing.
		if strings.HasSuffix(route.Path, "/webhook-subscription") {
			retiredRoutes++

			assert.True(t, claimed,
				"route %s %s is registered as deprecated but the barrier's matcher does "+
					"not recognise it, so requests to it would be answered by "+
					"authentication or by its handler instead of by the retirement",
				route.Method, route.Path)

			continue
		}

		assert.False(t, claimed,
			"route %s %s is a SUPPORTED route that the barrier's matcher claims, so it "+
				"would be retired along with the webhook surface",
			route.Method, route.Path)
	}

	assert.Equal(t, len(sunsetRoutes), retiredRoutes,
		"the retired surface is %d verbs on one path, and this file's route table must "+
			"describe exactly the routes the router registers. A different count means a "+
			"route was added or removed without sunsetRoutes being updated, and the "+
			"criterion would then be asserted against an incomplete matrix",
		len(sunsetRoutes))
}

// TestWebhookSunset_DecidesPerRequestRatherThanAtRouterBuildTime is what makes the
// retirement happen on its own, without a deployment.
//
// The window is closed UNDERNEATH the very same engine, which is the only way to tell a
// per-request decision from a per-build one.
func TestWebhookSunset_DecidesPerRequestRatherThanAtRouterBuildTime(t *testing.T) {
	router := setupSunsetRouter(t, sunsetFutureInstant, withSunsetMasterKeyPrincipal())

	// GET, because it binds no body and so cannot be confused with a bind failure.
	before := sunsetRequest(t, router, http.MethodGet, retiredWebhookPath, "")
	require.NotEqual(t, http.StatusGone, before.Code,
		"the fixture must start INSIDE the window, or this test proves nothing. body: %s",
		before.Body.String())

	// Only the configuration changes. The engine, its middleware chain and its route table
	// are all the ones built above.
	publishSunsetConfiguration(t, sunsetPassedInstant, sunsetFixture{masterKeyPrincipal: true})

	after := sunsetRequest(t, router, http.MethodGet, retiredWebhookPath, "")
	assertErrorCode(t, after, http.StatusGone, apierror.ErrGenGone)
	assertSunsetHeadersAdvertised(t, after)
}

// TestWebhookSunset_IsNotPreemptedByRateLimiting asserts the retirement through a
// response that the limiter would otherwise have owned.
//
// The barrier once lived in Api.Router, and every router.Use there runs AFTER every
// r.Use in NewAPI — where the request-size limit and the rate limiter are installed.
func TestWebhookSunset_IsNotPreemptedByRateLimiting(t *testing.T) {
	// One request per second, burst one: the SECOND request from this client is refused.
	router := setupSunsetRouter(t, sunsetPassedInstant,
		withSunsetMasterKeyPrincipal(), withSunsetRateLimit(1, 1))

	// FIRST, prove the limiter is actually armed. Without this the assertions below would
	// pass on a router that throttles nothing — the failure mode an over-generous limit
	// produces — and would prove nothing at all.
	var throttled bool
	for range 8 {
		recorder := sunsetRequest(t, router, http.MethodGet, "/ledgers", "")
		if recorder.Code == http.StatusTooManyRequests {
			throttled = true

			break
		}
	}
	require.True(t, throttled,
		"the rate limiter never refused a live route, so nothing below could have been "+
			"preempted and this test would be vacuous")

	// NOW the retired path, with this client's allowance already spent. Every one of these
	// was a 429 before the barrier moved ahead of the limiter.
	for _, method := range append(
		[]string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete},
		sunsetUnregisteredMethods...,
	) {
		t.Run(method, func(t *testing.T) {
			recorder := sunsetRequest(t, router, method, retiredWebhookPath, "")

			require.Equal(t, http.StatusGone, recorder.Code,
				"a throttled request to a retired route must still be told the route is "+
					"GONE, not that it should retry. Got %d: %s",
				recorder.Code, recorder.Body.String())
			assert.Equal(t, apierror.ErrGenGone, sunsetErrorCode(t, recorder),
				"and it must carry GEN_GONE, so a client can tell a retirement from a "+
					"throttle without parsing prose")
		})
	}

	// AND THE NEIGHBOURS ARE STILL THROTTLED. Moving the barrier ahead of the limiter must
	// not have moved the limiter: a barrier that somehow admitted everything would satisfy
	// every assertion above while disabling rate limiting for the whole API.
	recorder := sunsetRequest(t, router, http.MethodGet, "/ledgers", "")
	assert.Equal(t, http.StatusTooManyRequests, recorder.Code,
		"a live route must still be throttled; the barrier is scoped to the retired "+
			"surface and must not have displaced the limiter for anything else. Got %d",
		recorder.Code)
}

// TestWebhookSunset_AnUnusableWindowNeverReachesTheRoutes is the cross-layer half of
// the retirement, asserted where the 410 is observable.
//
// The runtime predicate FAILS CLOSED: a deployment that publishes to Kafka with no
// usable dual-delivery window is treated as ALREADY past the retirement instant. That
// is the right default for the relay — it must not keep pushing HTTP webhooks on a
// guess — but it means an operator typo could retire the management surface with
// nothing having failed to warn them.
func TestWebhookSunset_AnUnusableWindowNeverReachesTheRoutes(t *testing.T) {
	// A valid baseline: no transport, no window. This is the shape every deployment that
	// predates the feature runs, and it must stay loadable and unretired.
	router := setupSunsetRouter(t, "", withSunsetMasterKeyPrincipal())

	baseline := sunsetRequest(t, router, http.MethodGet, retiredWebhookPath, "")
	require.NotEqual(t, http.StatusGone, baseline.Code,
		"the baseline deployment must not be retired, or nothing below is measurable. "+
			"body: %s", baseline.Body.String())

	for _, refused := range []struct {
		name    string
		sunset  string
		brokers []string
		why     string
	}{
		{
			name:    "an unparseable instant while publishing",
			sunset:  "30 days from now",
			brokers: []string{"kafka:9092"},
			why: "a mis-typed instant must not resolve to a retirement; it would withdraw " +
				"the management surface with nothing having failed",
		},
		{
			name:    "no instant at all while publishing",
			brokers: []string{"kafka:9092"},
			why: "a publishing deployment with no window has no retirement to honour, and " +
				"the runtime predicate fails closed — so accepting this would let " +
				"configuration and behaviour disagree about the one decision the " +
				"retirement is made of",
		},
	} {
		t.Run(refused.name, func(t *testing.T) {
			config.MockConfig(sunsetConfiguration(refused.sunset, sunsetFixture{
				masterKeyPrincipal:      true,
				brokers:                 refused.brokers,
				insecureBrokerTransport: true,
			}))

			published, err := config.Fetch()
			require.NoError(t, err,
				"a refused configuration must leave the previous one in place rather than "+
					"emptying the store")
			assert.Empty(t, published.WebhookDeprecationSunsetDate, refused.why)
			assert.Empty(t, published.Kafka.Brokers,
				"the WHOLE configuration is refused, not the offending field: %s", refused.why)

			// And the routes, which is the point: they answer exactly as they did under the
			// baseline, because the rejected configuration never reached them.
			recorder := sunsetRequest(t, router, http.MethodGet, retiredWebhookPath, "")

			assertNotRetired(t, recorder, http.MethodGet, retiredWebhookPath, refused.why)
			assert.Empty(t, recorder.Header().Get("Sunset"),
				"and no window may be advertised from a configuration that was never "+
					"published")
		})
	}

	t.Run("accepted: publishing to Kafka with a stated instant still serves the routes", func(t *testing.T) {
		// The positive control, without which the refusals above would also be satisfied by a
		// configuration layer that rejected EVERY Kafka deployment. Configuring the new
		// transport must not by itself retire the old one — that is what the stated window is
		// for, and serving these routes throughout it is the whole point of dual delivery.
		publishing := setupSunsetRouter(t, sunsetFutureInstant,
			withSunsetMasterKeyPrincipal(), withSunsetKafkaBrokers("kafka:9092"))

		published, err := config.Fetch()
		require.NoError(t, err)
		require.NotEmpty(t, published.Kafka.Brokers,
			"the valid combination must actually be published, or the refusals above prove "+
				"nothing about validation and everything about Kafka being unconfigurable")

		recorder := sunsetRequest(t, publishing, http.MethodGet, retiredWebhookPath, "")

		assertNotRetired(t, recorder, http.MethodGet, retiredWebhookPath,
			"the window is open, so the legacy surface is still the one a subscriber that "+
				"has not migrated depends on")
		assertSunsetHeadersAdvertised(t, recorder)
	})
}
