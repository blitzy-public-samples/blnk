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
// A handler with no route is unreachable, compiles cleanly and fails no other test — and the
// converse matters just as much here: an EXTRA route is a management surface nobody approved.
// The three routes below are the whole of this file's API. A fourth, POST
// /events/dead-letter/:event_id/resolve, was registered and has been removed: it took the
// management surface to fourteen routes where thirteen are approved, and it could not compose
// with replay, because a resolved row whose re-publish the broker acknowledged could not then
// be marked dispatched. Retention is modelled through replay alone.
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
		"GET /events/stats",
	} {
		assert.True(t, routes[route], "%s must be registered; an unrouted handler is unreachable "+
			"and breaks no test", route)
	}

	// AND NOTHING ELSE UNDER /events. The retired resolve route must stay retired: it is
	// unapproved surface, and its write could leave a dead-lettered row in a state from which a
	// broker-acknowledged replay could not be recorded.
	assert.False(t, routes["POST /events/dead-letter/:event_id/resolve"],
		"the resolve route is retired; retention is modelled through replay, which turns a "+
			"dead-lettered row into a dispatched receipt")

	t.Run("a non-master caller is refused with the master-key code", func(t *testing.T) {
		cases := []struct{ method, path string }{
			{http.MethodGet, "/events/dead-letter"},
			{http.MethodPost, "/events/dead-letter/evt_1/replay"},
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

// TestListDeadLetterEvents_RefusesAnUnknownQueryParameter covers the closed query vocabulary.
//
// The endpoint refuses parameters it cannot honour rather than ignoring them, because a
// silently ignored filter returns MORE rows than the caller asked for while looking like it
// worked, and on an inventory endpoint a page that is wider than requested reads as less loss
// than there is.
//
// `resolved` is one of the names now refused. It selected the resolved or unresolved subset of
// the inventory, and both the filter and the resolution it narrowed on are gone: retention
// spares every dead-lettered row, and a replay the broker acknowledges is what takes one out of
// the inventory.
func TestListDeadLetterEvents_RefusesAnUnknownQueryParameter(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	for _, name := range []string{"resolvd", "resolved", "unresolved_only"} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/events/dead-letter?"+name+"=true", nil)
			router.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			assert.Equal(t, string(apierror.ErrGenValidation),
				deadLetterErrorCode(t, recorder.Body.Bytes()))
			assert.Contains(t, recorder.Body.String(), name,
				"the refusal must name the parameter it could not honour, or an operator cannot "+
					"tell which of several filters was rejected")
		})
	}
}

// TestListDeadLetterEvents_RefusesTwoTopicFiltersThatDisagree closes the last silently
// discarded filter on this endpoint.
//
// Both spellings of the topic filter are accepted because both are natural: an operator
// reading the inventory sees dlt_topic on every item, while one asking "which transaction
// events are stuck" thinks in category topics. They resolve to one predicate, so naming the
// same topic in both is one instruction written twice and is honoured.
//
// Naming two DIFFERENT topics is not. It used to be resolved by preferring `topic` and
// dropping `dlt_topic` without a word, which returned a page describing a question the caller
// did not ask — and on an inventory endpoint a page drawn from the wrong topic reads as a
// different amount of loss than there is.
func TestListDeadLetterEvents_RefusesTwoTopicFiltersThatDisagree(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	t.Run("two spellings naming different topics are refused", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet,
			"/events/dead-letter?topic=blnk.transactions&dlt_topic=blnk.balances.dlt", nil)
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assert.Equal(t, string(apierror.ErrGenValidation),
			deadLetterErrorCode(t, recorder.Body.Bytes()))

		// BOTH VALUES HAVE TO APPEAR. A refusal that says only "conflicting filters" leaves the
		// operator to guess which pair of a five-parameter query it meant.
		body := recorder.Body.String()
		assert.Contains(t, body, "blnk.transactions")
		assert.Contains(t, body, "blnk.balances")
	})

	t.Run("the same topic in both spellings is one instruction, not a conflict", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet,
			"/events/dead-letter?topic=blnk.transactions&dlt_topic=blnk.transactions.dlt", nil)
		router.ServeHTTP(recorder, request)

		// The service is unreachable in this harness, so the assertion is that the request got
		// PAST validation rather than that it succeeded: any code other than 400 proves the
		// pair was accepted.
		assert.NotEqual(t, http.StatusBadRequest, recorder.Code,
			"the two spellings resolve to the same topic, so this is not a contradiction: %s",
			recorder.Body.String())
	})

	t.Run("either spelling alone is honoured", func(t *testing.T) {
		for _, query := range []string{
			"topic=blnk.transactions",
			"dlt_topic=blnk.transactions.dlt",
		} {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/events/dead-letter?"+query, nil)
			router.ServeHTTP(recorder, request)

			assert.NotEqual(t, http.StatusBadRequest, recorder.Code,
				"%s is a supported filter: %s", query, recorder.Body.String())
		}
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
