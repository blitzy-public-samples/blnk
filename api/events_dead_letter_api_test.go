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
	"os"
	"path/filepath"
	"reflect"
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

// This file covers the HTTP boundary of the dead-letter operational surface: which
// routes exist, which query parameters they accept, and which they refuse.

// deadLetterTestRouter builds a router with the real route table, marking every request
// as carrying — or not carrying — the master key.
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

// TestDeadLetterRoutes_AreRegisteredAndMasterKeyGated pins the route table and the
// gate.
//
// apierror.ErrAuthMasterKeyRequired and apierror.ErrAuthUnknownResource both resolve to
// 403.
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
	// unapproved surface, and its write could leave a dead-lettered row in a state from
	// which a broker-acknowledged replay could not be recorded.
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

// TestListDeadLetterEvents_RefusesAnUnknownQueryParameter covers the closed query
// vocabulary.
//
// The endpoint refuses parameters it cannot honour rather than ignoring them, because a
// silently ignored filter returns MORE rows than the caller asked for while looking
// like it worked, and on an inventory endpoint a page that is wider than requested
// reads as less loss than there is.
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
// Naming two DIFFERENT topics is not.
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

		// The service is unreachable in this harness, so the assertion is that the request
		// got PAST validation rather than that it succeeded: any code other than 400 proves
		// the pair was accepted.
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

// TestListDeadLetterEvents_RefusesAFilterCarryingANulByte closes the last filter value
// this endpoint answered an INTERNAL error for.
//
// A PostgreSQL text value cannot hold a NUL byte in any encoding, so a filter carrying
// one aborts the query with SQLSTATE 22021 and the repository reports a driver failure —
// which surfaced as 500 GEN_INTERNAL. The value came from the caller, so that is the
// wrong class of answer twice over: it tells an operator triaging a dead-letter backlog
// that Blnk is broken, and it is the one refusal on this endpoint that did not match the
// four beside it (`limit`, `cursor`, `status` and the occurrence bounds all answer 400).
func TestListDeadLetterEvents_RefusesAFilterCarryingANulByte(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	// %00 is the encoded NUL. Both spellings of the topic filter are covered because they
	// collapse into ONE service-level filter, and the refusal must still name the
	// parameter the caller actually sent.
	for _, parameter := range []string{"event_type", "topic", "dlt_topic"} {
		t.Run(parameter, func(t *testing.T) {
			for _, suffix := range []string{"", "&include_count=true"} {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet,
					"/events/dead-letter?"+parameter+"=a%00b"+suffix, nil)
				router.ServeHTTP(recorder, request)

				require.Equal(t, http.StatusBadRequest, recorder.Code,
					"a malformed filter value is a client error; 500 sends an operator looking for "+
						"a fault in Blnk. body: %s", recorder.Body.String())
				assert.Equal(t, string(apierror.ErrGenValidation),
					deadLetterErrorCode(t, recorder.Body.Bytes()),
					"and the typed validation code, which is what a caller branches on")
				assert.Contains(t, recorder.Body.String(), parameter,
					"the refusal must name the parameter it could not honour, or an operator cannot "+
						"tell which of eight filters was rejected")

				// THE VALUE IS NOT ECHOED. It is unvalidated caller input carrying a control byte,
				// and a response that repeats it hands a log aggregator or a terminal whatever else
				// was wrapped around the NUL.
				assert.NotContains(t, recorder.Body.String(), "\x00",
					"the refusal must not echo the NUL byte back")
			}
		})
	}

	t.Run("a legitimate value on the same filter is still honoured", func(t *testing.T) {
		for _, query := range []string{
			"event_type=transaction.applied",
			"topic=blnk.transactions",
			"dlt_topic=blnk.transactions.dlt",
		} {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/events/dead-letter?"+query, nil)
			router.ServeHTTP(recorder, request)

			assert.NotEqual(t, http.StatusBadRequest, recorder.Code,
				"the guard must reject the NUL byte and nothing else: %s", recorder.Body.String())
		}
	})
}

// deadLetterErrorCode reads error_detail.code from a refusal.
//
// The CODE and not the status is what these tests assert on, for the reason spelled out
// on TestDeadLetterRoutes_AreRegisteredAndMasterKeyGated: several distinct refusals
// share one status, and telling them apart is the whole point.
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

// runbookParameterTableHeader is the header row of the one `| Parameter | Effect |`
// table in docs/kafka-operations.md, which is the dead-letter listing's published
// parameter reference.
const runbookParameterTableHeader = "| Parameter | Effect |"

// operationsRunbookParameters reads the documented parameter names out of that table.
//
// Only the leading `| \x60name\x60 |` cell is read. The Effect column is prose and is not the
// contract; the name is.
func operationsRunbookParameters(t *testing.T) []string {
	t.Helper()

	runbook, err := os.ReadFile(filepath.Join("..", "docs", "kafka-operations.md"))
	require.NoError(t, err, "docs/kafka-operations.md must be readable: it is the published "+
		"reference for this endpoint's parameters")

	lines := strings.Split(string(runbook), "\n")

	header := -1
	for index, line := range lines {
		if strings.TrimSpace(line) == runbookParameterTableHeader {
			require.Equal(t, -1, header,
				"docs/kafka-operations.md carries more than one %q table, so this guard can no "+
					"longer tell which one documents the dead-letter listing. Give the second "+
					"table a different header, or teach this helper to pick by section.",
				runbookParameterTableHeader)
			header = index
		}
	}
	require.NotEqual(t, -1, header,
		"docs/kafka-operations.md must carry the %q table for the dead-letter listing; without "+
			"it an operator has no published parameter reference and this guard has nothing to "+
			"check", runbookParameterTableHeader)

	documented := make([]string, 0, len(deadLetterQueryParameters))

	// Skip the header and the |---| separator beneath it, then read until the table ends.
	for _, line := range lines[header+2:] {
		row := strings.TrimSpace(line)
		if !strings.HasPrefix(row, "|") {
			break
		}

		cell := strings.TrimSpace(strings.Split(strings.TrimPrefix(row, "|"), "|")[0])
		name := strings.Trim(cell, "`")
		if name == "" {
			continue
		}

		documented = append(documented, name)
	}

	return documented
}

// TestDeadLetterQueryParameters_MatchTheOperationsRunbookExactly is the regression
// guard for a documentation defect that cost an operator a step during an incident.
//
// The runbook's parameter table must name exactly the parameters the endpoint accepts:
// an advertised `offset` the handler refuses costs a step, and an accepted filter nobody
// documented is one nobody uses.
//
// Prose can drift from a var block, so the two are compared here rather than trusted to
// stay aligned. The guard runs in BOTH directions on purpose:
//
//   - A DOCUMENTED name that the endpoint refuses is the original defect: a step that
//     fails when followed.
//   - An ACCEPTED name that is undocumented is the same defect from the other side: a
//     filter nobody knows exists, which during a loss investigation is a filter nobody
//     uses.
func TestDeadLetterQueryParameters_MatchTheOperationsRunbookExactly(t *testing.T) {
	documented := operationsRunbookParameters(t)
	require.NotEmpty(t, documented, "the parameter table must not be empty")

	accepted := make(map[string]bool, len(deadLetterQueryParameters))
	for _, name := range deadLetterQueryParameters {
		accepted[name] = true
	}

	documentedSet := make(map[string]bool, len(documented))
	for _, name := range documented {
		documentedSet[name] = true
	}

	for _, name := range documented {
		assert.Truef(t, accepted[name],
			"docs/kafka-operations.md documents the query parameter %q, but "+
				"deadLetterQueryParameters does not accept it, so rejectUnsupportedQueryParameters "+
				"answers 400 GEN_VALIDATION_ERROR to an operator following the runbook. Either "+
				"remove the row or add the parameter.", name)
	}

	for _, name := range deadLetterQueryParameters {
		assert.Truef(t, documentedSet[name],
			"this endpoint accepts the query parameter %q, but docs/kafka-operations.md's "+
				"parameter table does not document it. An undocumented filter is one no operator "+
				"reaches for while they are trying to account for missing events.", name)
	}
}

// TestDeadLetterEnvelope_TotalCountIsDocumentedAsConditional is the second half of the
// same defect class: an envelope key the document promised unconditionally.
//
// It is not present either way — the field is `omitempty`, so the key is ABSENT unless
// the option is supplied.
func TestDeadLetterEnvelope_TotalCountIsDocumentedAsConditional(t *testing.T) {
	runbook, err := os.ReadFile(filepath.Join("..", "docs", "kafka-operations.md"))
	require.NoError(t, err)
	text := string(runbook)

	// The runbook must name the parameter that makes the key appear. That is a query
	// parameter name rather than a sentence, so the surrounding text can be rewritten
	// without failing here, while a runbook that stopped mentioning the option — and so
	// described the key as unconditional — still does.
	assert.Contains(t, text, "include_count",
		"docs/kafka-operations.md must name include_count where it documents total_count: the field "+
			"is omitempty on the response type, so the key is absent unless the option is supplied")

	// And the response type it describes must still be omitempty, so the documented
	// condition cannot become wrong in the other direction.
	field, ok := reflect.TypeOf(DeadLetterPageResponse{}).FieldByName("TotalCount")
	require.True(t, ok, "DeadLetterPageResponse must carry TotalCount")
	assert.Contains(t, field.Tag.Get("json"), "omitempty",
		"TotalCount is documented as absent unless asked for, which is what omitempty provides; "+
			"dropping it would make the runbook wrong again from the other direction")
}

// TestCursorRefusals_DescribeADecodeAndNotAnIssuance keeps both paging refusals honest
// about what they actually check.
//
// They do not check that.
//
// The two endpoints are asserted TOGETHER because they describe one mechanism.
func TestCursorRefusals_DescribeADecodeAndNotAnIssuance(t *testing.T) {
	router := deadLetterTestRouter(t, true)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/events/dead-letter?cursor=not-a-cursor", nil)
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	assert.Equal(t, string(apierror.ErrGenValidation), deadLetterErrorCode(t, recorder.Body.Bytes()))

	deadLetterRefusal := recorder.Body.String()

	for name, message := range map[string]string{
		"dead-letter listing": deadLetterRefusal,
		"subscriber registry": subscriberInvalidCursorMessage,
	} {
		assert.Containsf(t, message, "could not be decoded as a page position",
			"%s must describe the check it performs — decoding a position — rather than one it "+
				"does not", name)
		assert.NotContainsf(t, message, "is not a cursor this endpoint issued",
			"%s must not claim to verify that it issued the cursor: nothing records an issuance, "+
				"and a structurally valid coordinate from elsewhere is accepted as a position", name)
		assert.Containsf(t, message, "next_cursor",
			"%s must still say what to send instead, or the refusal is a dead end", name)
	}
}
