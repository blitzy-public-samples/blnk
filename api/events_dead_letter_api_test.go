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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
)

// This file covers the HTTP boundary of the dead-letter operational surface: which routes
// exist, which query parameters they accept, and which they refuse.
//
// It deliberately does NOT reach a database. Every property asserted here is a property of
// the boundary — the route table, the master-key gate, the parameter vocabulary — and each is
// observable before any repository call is made. A test that needed a live outbox to prove
// "this route is registered" would be a slower test proving less.

// deadLetterTestRouter builds a router with the real route table, marking every request as
// carrying — or not carrying — the master key.
//
// It follows setupHookRouter, which is the established shape in this package for testing a
// master-key-gated surface: the middleware that resolves a principal is not installed under
// test, so the flag the gate reads is set directly. Every property asserted in this file is
// reached BEFORE any repository call — the route table, the gate, the parameter vocabulary and
// the request body — so no live outbox is needed to observe any of them.
//
// Parameters:
//   - t *testing.T: the test, for fatal setup failures.
//   - isMaster bool: whether requests are treated as holding the master key.
//
// Returns:
//   - *gin.Engine: the router, with the real route table registered.
func deadLetterTestRouter(t *testing.T, isMaster bool) *gin.Engine {
	t.Helper()

	gin.SetMode(gin.TestMode)

	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_dead_letter_api",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})

	cnf, err := config.Fetch()
	require.NoError(t, err, "the dead-letter API fixture configuration must load")

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err)

	newBlnk, err := blnk.NewBlnk(db)
	require.NoError(t, err)

	apiInstance := NewAPI(newBlnk)
	apiInstance.router.Use(func(c *gin.Context) {
		c.Set("isMasterKey", isMaster)
		c.Next()
	})

	return apiInstance.Router()
}

// TestDeadLetterRoutes_AreRegisteredAndMasterKeyGated pins the route table and the gate.
//
// # Why the route table needs a test of its own
//
// A handler with no route is unreachable, compiles cleanly and fails no other test. The
// resolve endpoint is new, and it is the endpoint the whole retention gate depends on: without
// it a dead-lettered row can never become eligible for the purge, so an operator's only
// options would be to keep every failure forever or to disable retention entirely.
//
// # Why the assertion is on the CODE and not the status
//
// apierror.ErrAuthMasterKeyRequired and apierror.ErrAuthUnknownResource both resolve to 403.
// A test asserting only the status would pass just as well against a route prefix that the
// authorization middleware does not recognise at all — which is the failure that denies every
// caller including the master key, and the one most worth telling apart.
func TestDeadLetterRoutes_AreRegisteredAndMasterKeyGated(t *testing.T) {
	router := deadLetterTestRouter(t, false)

	routes := make(map[string]bool, len(router.Routes()))
	for _, route := range router.Routes() {
		routes[route.Method+" "+route.Path] = true
	}

	for _, route := range []string{
		"GET /events/dead-letter",
		"POST /events/dead-letter/:event_id/replay",
		"POST /events/dead-letter/:event_id/resolve",
		"GET /events/stats",
	} {
		assert.True(t, routes[route], "%s must be registered; an unrouted handler is unreachable "+
			"and breaks no test", route)
	}

	t.Run("a non-master caller is refused with the master-key code", func(t *testing.T) {
		cases := []struct{ method, path string }{
			{http.MethodGet, "/events/dead-letter"},
			{http.MethodPost, "/events/dead-letter/evt_1/replay"},
			{http.MethodPost, "/events/dead-letter/evt_1/resolve"},
			{http.MethodGet, "/events/stats"},
		}

		for _, tc := range cases {
			t.Run(tc.method+" "+tc.path, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
				request.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(recorder, request)

				require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
				assert.Equal(t, string(apierror.ErrAuthMasterKeyRequired),
					deadLetterErrorCode(t, recorder.Body.Bytes()),
					"the gate that fired must be the MASTER KEY one; ErrAuthUnknownResource is also "+
						"403, and confusing the two hides an unmapped route prefix")
			})
		}
	})
}

// TestListDeadLetterEvents_AcceptsTheResolvedFilterAndRefusesAnUnusableOne covers the query
// vocabulary the SEC-09 fix added.
//
// The `resolved` filter is REFUSED rather than coerced when unrecognised, and that is the
// substantive property. Go's strconv.ParseBool and gin's own boolean readers both turn an
// unparseable value into false — and false here means "unresolved only", so `?resolved=maybe`
// would hand back the outstanding subset while the caller believed they had asked for
// something else. On an inventory endpoint a filter that quietly means something other than
// what was asked is worth an error: a short page reads as "nothing is stuck".
func TestListDeadLetterEvents_AcceptsTheResolvedFilterAndRefusesAnUnusableOne(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	t.Run("an unusable value is refused with a validation error", func(t *testing.T) {
		for _, value := range []string{"maybe", "2", "unresolved", "TRUE!", "null"} {
			t.Run(value, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet,
					"/events/dead-letter?resolved="+value, nil)
				router.ServeHTTP(recorder, request)

				require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
				assert.Equal(t, string(apierror.ErrGenValidation),
					deadLetterErrorCode(t, recorder.Body.Bytes()))
				assert.Contains(t, recorder.Body.String(), "resolved",
					"the message must name the parameter, or an operator cannot tell which of "+
						"several filters was rejected")
			})
		}
	})

	t.Run("an unknown parameter is still refused", func(t *testing.T) {
		// The endpoint refuses parameters it cannot honour rather than ignoring them,
		// because a silently ignored filter returns MORE rows than the caller asked for
		// while looking like it worked. `resolved` is now in the accepted set, so this also
		// proves the addition did not turn the check off.
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/events/dead-letter?resolvd=true", nil)
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Equal(t, string(apierror.ErrGenValidation), deadLetterErrorCode(t, recorder.Body.Bytes()))
		assert.Contains(t, recorder.Body.String(), "resolved",
			"the refusal lists the accepted parameters, and `resolved` must now be among them")
	})
}

// TestResolveDeadLetterEvent_RefusesAnUnusableBodyAtTheBoundary covers the request body.
//
// The body is OPTIONAL — resolving without an explanation is a legitimate action and requiring
// a document would make the simple case a ceremony — but a MALFORMED body is refused rather
// than discarded. Silently dropping a note the operator wrote would lose the audit trail they
// were trying to leave, which is the one thing the note exists for.
func TestResolveDeadLetterEvent_RefusesAnUnusableBodyAtTheBoundary(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	t.Run("malformed JSON is refused", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/events/dead-letter/evt_1/resolve",
			strings.NewReader("{not json"))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Equal(t, string(apierror.ErrGenValidation), deadLetterErrorCode(t, recorder.Body.Bytes()))
	})

	t.Run("an unusable note is refused before the repository is reached", func(t *testing.T) {
		// A control character in the note. Refused at the DTO, which is why this is
		// observable without a datasource — and refusing it here rather than at the service
		// means the caller learns the real bound rather than having the note truncated at
		// storage.
		body, err := json.Marshal(map[string]string{"note": "replayed\x00"})
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/events/dead-letter/evt_1/resolve",
			strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Equal(t, string(apierror.ErrGenValidation), deadLetterErrorCode(t, recorder.Body.Bytes()))
		assert.Contains(t, recorder.Body.String(), "control characters")
	})

	t.Run("an oversized note is refused by the binder before it is decoded", func(t *testing.T) {
		// The byte bound is the OUTER guard: without it an arbitrarily large body would be
		// decoded in full before any validation ran.
		body, err := json.Marshal(map[string]string{"note": strings.Repeat("x", 8192)})
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/events/dead-letter/evt_1/resolve",
			strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Equal(t, string(apierror.ErrGenValidation), deadLetterErrorCode(t, recorder.Body.Bytes()))
	})
}

// deadLetterErrorCode reads error_detail.code from a refusal.
//
// The CODE and not the status is what these tests assert on, for the reason spelled out on
// TestDeadLetterRoutes_AreRegisteredAndMasterKeyGated: several distinct refusals share one
// status, and telling them apart is the whole point.
func deadLetterErrorCode(t *testing.T, body []byte) string {
	t.Helper()

	var envelope struct {
		ErrorDetail struct {
			Code string `json:"code"`
		} `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "refusal body must be the error envelope: %s", body)

	return envelope.ErrorDetail.Code
}
