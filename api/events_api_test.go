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

// events_api_test.go covers the request-side contract of the event management surface:
// GET /events/dead-letter, POST /events/dead-letter/:event_id/replay and GET
// /events/stats.
//
// They cover the decisions the HANDLER owns — the master-key gate, the closed
// query-parameter set, parameter parsing and validation, and the response envelope —
// because those are the decisions no other layer can make on its behalf.
//
// They deliberately do NOT re-prove the narrowing itself.
//
// The listing reads the outbox table and never a Kafka topic, so every test below runs
// with no broker configured — which is also the deployment posture that must keep
// working.
//
// The tests split by what they have to observe, not by taste.
//
//   - The LIVE-DATASOURCE harness — eventsRouter and eventsRequest, over the real
//     PostgreSQL the api package already runs against — is used wherever the subject is
//     the query string or the response envelope.
//   - The MOCK-DATASOURCE harness — setupEventsRouter, over mocks.MockDataSource — is
//     used wherever the subject is a value crossing the handler/service boundary or a
//     FAILURE the database will not produce on demand.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// eventsRequest issues a request against a master-key router and returns the recorder.
//
// The master key is the authorised principal for every endpoint in this file, so a test
// that is about parameter handling should not have to restate the authorisation setup.
// The one test that is about authorisation builds its own non-master router.
func eventsRequest(t *testing.T, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	router, _ := setupAuthedRouter(t, true, nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, nil)
	router.ServeHTTP(recorder, request)

	return recorder
}

// assertEventsErrorCode asserts the status AND the error code.
func assertEventsErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	require.Equal(t, status, recorder.Code, "body: %s", recorder.Body.String())

	var body struct {
		ErrorDetail struct {
			Code string `json:"code"`
		} `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body), "body: %s", recorder.Body.String())
	assert.Equal(t, code, body.ErrorDetail.Code, "body: %s", recorder.Body.String())
}

// TestListDeadLetterEvents_RequiresTheMasterKey pins the gate that must fire before
// anything else happens.
//
// The dead-letter inventory carries the failure reason and the broker coordinate of
// every event a deployment could not deliver.
func TestListDeadLetterEvents_RequiresTheMasterKey(t *testing.T) {
	router, _ := setupAuthedRouter(t, false, nil)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/events/dead-letter", nil))

	assertEventsErrorCode(t, recorder, http.StatusForbidden, "AUTH_MASTER_KEY_REQUIRED")
}

// TestListDeadLetterEvents_AcceptsTheOccurrenceWindow covers the occurrence window this
// layer accepts. The window is a SQL predicate, so both bounds reach the repository
// rather than being dropped here.
func TestListDeadLetterEvents_AcceptsTheOccurrenceWindow(t *testing.T) {
	for _, target := range []string{
		"/events/dead-letter?occurred_from=2026-01-02T03:04:05Z",
		"/events/dead-letter?occurred_to=2026-01-02T03:04:05Z",
		"/events/dead-letter?occurred_from=2026-01-02T03:04:05Z&occurred_to=2026-01-02T04:04:05Z",
		// An offset rather than Z: the same instant expressed in another zone must be
		// accepted, or a caller pasting a timestamp out of a response into a filter is
		// refused.
		"/events/dead-letter?occurred_from=2026-01-02T04:04:05%2B01:00",
	} {
		t.Run(target, func(t *testing.T) {
			recorder := eventsRequest(t, http.MethodGet, target)

			require.Equal(t, http.StatusOK, recorder.Code,
				"an occurrence window is a supported filter: body %s", recorder.Body.String())

			// The shape is the PAGE ENVELOPE, not a bare array. Paging is keyset-based so the
			// position to resume from has to be returned somewhere and an envelope is the only
			// place a page can carry it without inventing a header.
			var envelope struct {
				Data       []model.DeadLetterEvent `json:"data"`
				HasMore    bool                    `json:"has_more"`
				NextCursor string                  `json:"next_cursor"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope),
				"body: %s", recorder.Body.String())
			assert.NotNil(t, envelope.Data)
		})
	}
}

// TestListDeadLetterEvents_RefusesAnUnusableOccurrenceWindow covers the two ways a
// window can be wrong.
//
// Both are refused rather than ignored, and that is the whole point: this is a triage
// endpoint, and an operator who asked for a twenty-minute window and silently received
// the whole inventory would read stale entries as current with nothing in the response
// to say the filter was dropped.
//
//   - An UNPARSEABLE bound is a 400 from the handler. A bare date or a local timestamp
//     with no zone is included deliberately: guessing a zone would shift the window by
//     hours.
//   - A REVERSED window is a 400 from the service. It is unsatisfiable by construction,
//     so no row can be inside it and an empty page would read as "nothing is stuck".
func TestListDeadLetterEvents_RefusesAnUnusableOccurrenceWindow(t *testing.T) {
	for name, target := range map[string]string{
		"not a timestamp at all":     "/events/dead-letter?occurred_from=yesterday",
		"a bare date has no zone":    "/events/dead-letter?occurred_from=2026-01-02",
		"a local timestamp has none": "/events/dead-letter?occurred_to=2026-01-02T03:04:05",
		"an epoch is not RFC3339":    "/events/dead-letter?occurred_to=1767322445",
		"reversed window": "/events/dead-letter?occurred_from=2026-01-02T04:04:05Z" +
			"&occurred_to=2026-01-02T03:04:05Z",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := eventsRequest(t, http.MethodGet, target)
			assertEventsErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")
		})
	}
}

// TestListDeadLetterEvents_CountsEveryNarrowing covers include_count alongside every
// narrowing the listing accepts.
func TestListDeadLetterEvents_CountsEveryNarrowing(t *testing.T) {
	for _, target := range []string{
		"/events/dead-letter?include_count=true",
		"/events/dead-letter?include_count=true&event_type=transaction.applied",
		"/events/dead-letter?include_count=true&topic=blnk.transactions",
		"/events/dead-letter?include_count=true&dlt_topic=blnk.transactions.dlt",
		"/events/dead-letter?include_count=true&status=dead_lettered",
		"/events/dead-letter?include_count=true&event_type=transaction.applied&topic=blnk.transactions",
		"/events/dead-letter?include_count=true&occurred_from=2020-01-02T03:04:05Z",
	} {
		t.Run(target, func(t *testing.T) {
			recorder := eventsRequest(t, http.MethodGet, target)
			require.Equal(t, http.StatusOK, recorder.Code,
				"include_count must be honoured for every supported filter: body %s",
				recorder.Body.String())

			var envelope struct {
				Data       []model.DeadLetterEvent `json:"data"`
				TotalCount *int64                  `json:"total_count"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope),
				"body: %s", recorder.Body.String())

			require.NotNil(t, envelope.TotalCount,
				"include_count must produce the {data, total_count} envelope")
			assert.NotNil(t, envelope.Data, "the page must marshal as [] rather than null")
			assert.GreaterOrEqual(t, *envelope.TotalCount, int64(len(envelope.Data)),
				"the total must never be short of the page it accompanies, or a paging client "+
					"stops before it has seen everything")
		})
	}
}

// TestListDeadLetterEvents_RefusesAnUnsupportedQueryParameter keeps the accepted set
// closed.
//
// Quietly ignoring a narrowing hands a caller a result that answers a different
// question than the one they posed, and they have no way to tell.
func TestListDeadLetterEvents_RefusesAnUnsupportedQueryParameter(t *testing.T) {
	recorder := eventsRequest(t, http.MethodGet,
		"/events/dead-letter?occurred_from=2026-01-02T03:04:05Z&aggregate_id=abc")

	assertEventsErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")

	body := recorder.Body.String()
	assert.Contains(t, body, "aggregate_id")
	assert.Contains(t, body, "occurred_from")
	assert.Contains(t, body, "occurred_from",
		"the refusal must name the parameters that ARE accepted, including the window")
	assert.Contains(t, body, "occurred_to")
}

// TestReplayDeadLetterEvent_ValidatesTheEventIDBeforeLookingItUp separates a malformed
// request from a missing event.
//
// Every id this pipeline mints is a lowercase canonical UUID, and the lookup is an
// exact string comparison.
func TestReplayDeadLetterEvent_ValidatesTheEventIDBeforeLookingItUp(t *testing.T) {
	for name, eventID := range map[string]string{
		"not a uuid":          "not-a-uuid",
		"uppercase":           "3F2504E0-4F89-11D3-9A0C-0305E82C3301",
		"braced":              "%7B3f2504e0-4f89-11d3-9a0c-0305e82c3301%7D",
		"unhyphenated":        "3f2504e04f8911d39a0c0305e82c3301",
		"urn prefixed":        "urn:uuid:3f2504e0-4f89-11d3-9a0c-0305e82c3301",
		"a numeric surrogate": "12345",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := eventsRequest(t, http.MethodPost, "/events/dead-letter/"+eventID+"/replay")
			assertEventsErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")
		})
	}

	t.Run("surrounding whitespace is trimmed rather than treated as malformed", func(t *testing.T) {
		// Consistent with every other parameter in this package, and it is the difference
		// between a copy-and-paste that picked up a space being usable and being refused. The
		// trimmed value is canonical, so it reaches the lookup and is answered as a missing
		// event rather than as a bad request.
		recorder := eventsRequest(t, http.MethodPost,
			"/events/dead-letter/%203f2504e0-4f89-11d3-9a0c-0305e82c3301%20/replay")
		assertEventsErrorCode(t, recorder, http.StatusNotFound, "EVENT_NOT_FOUND")
	})

	t.Run("a canonical id that does not exist is a NOT FOUND, not a validation error", func(t *testing.T) {
		// The distinction is the whole point of the gate: a well-formed id reaches
		// the lookup and is reported as a missing event.
		recorder := eventsRequest(t, http.MethodPost,
			"/events/dead-letter/3f2504e0-4f89-11d3-9a0c-0305e82c3301/replay")
		assertEventsErrorCode(t, recorder, http.StatusNotFound, "EVENT_NOT_FOUND")
	})

	t.Run("the replay endpoint requires the master key", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, false, nil)

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost,
			"/events/dead-letter/3f2504e0-4f89-11d3-9a0c-0305e82c3301/replay", nil))

		assertEventsErrorCode(t, recorder, http.StatusForbidden, "AUTH_MASTER_KEY_REQUIRED")
	})
}

// deadLetterFixtureTopic is a real Blnk-owned category topic, used because the
// persistence layer refuses a row naming a topic outside that namespace — so the topic
// column cannot carry a test marker the way the event type can.
const deadLetterFixtureTopic = "blnk.transactions"

// newDeadLetterEventType mints an event type nothing else in the table can be using.
//
// It is what makes the count assertions in this file exact rather than "at least":
// blnk.event_outbox is shared with the relay and with sibling test processes, so a
// total asserted over any unscoped dimension is a race.
func newDeadLetterEventType(t *testing.T) string {
	t.Helper()

	return "apitest.deadletter." + uuid.NewString()
}

// seedDeadLetteredEvent inserts one row already in a terminal failure state.
//
// The insert is written here rather than driven through the relay's transitions on
// purpose.
//
// Parameters:
//   - t *testing.T: the test, failed on any seeding error.
//   - ds *database.Datasource: the live repository.
//   - eventType string: the marker-scoped event type.
//   - status string: EventOutboxStatusDeadLettered or EventOutboxStatusFailed.
//   - occurredAt time.Time: the occurrence instant, which fixes the listing order.
//
// Returns:
//   - string: the event id of the seeded row.
func seedDeadLetteredEvent(
	t *testing.T,
	ds *database.Datasource,
	eventType, status string,
	occurredAt time.Time,
) string {
	t.Helper()

	eventID := uuid.NewString()
	payload := fmt.Sprintf(
		`{"event":%q,"data":{"transaction_id":%q,"status":"REJECTED"}}`, eventType, eventID)
	envelope := fmt.Sprintf(
		`{"event_id":%q,"event_type":%q,"aggregate_id":%q,"occurred_at":%q,"payload":%s,"schema_version":%d}`,
		eventID, eventType, eventID, occurredAt.UTC().Format(time.RFC3339Nano), payload,
		coremodel.SchemaVersionV1,
	)

	var dltTopic interface{}
	var failureMetadata interface{}
	if status == coremodel.EventOutboxStatusDeadLettered {
		dltTopic = deadLetterFixtureTopic + ".dlt"
		failureMetadata = fmt.Sprintf(
			`{"original_topic":%q,"error_reason":"broker refused the publish","attempt_count":5}`,
			deadLetterFixtureTopic)
	}

	// payload and payload_raw carry the SAME bytes and are bound from separate
	// placeholders with explicit casts, because one parameter cannot be deduced as both
	// jsonb and bytea.
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.event_outbox
			(event_id, event_type, aggregate_id, partition_key, ledger_id, topic,
			 schema_version, payload, payload_raw, event_raw, occurred_at, status,
			 attempts, max_attempts, last_error, first_attempted_at, last_attempted_at,
			 dlt_topic, failure_metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::bytea, $10::bytea, $11, $12, 5, 5,
			 'broker refused the publish', $11, $11, $13, $14::jsonb)
	`,
		eventID, eventType, eventID, eventID, nil, deadLetterFixtureTopic,
		coremodel.SchemaVersionV1, payload, []byte(payload), []byte(envelope),
		occurredAt.UTC(), status, dltTopic, failureMetadata,
	)
	require.NoError(t, err, "failed to seed a dead-letter fixture row")

	t.Cleanup(func() {
		if _, cleanupErr := ds.Conn.Exec(
			`DELETE FROM blnk.event_outbox WHERE event_id = $1`, eventID); cleanupErr != nil {
			t.Logf("failed to remove dead-letter fixture %s: %v", eventID, cleanupErr)
		}
	})

	return eventID
}

// eventsTestDataSource returns the concrete repository behind a Blnk instance.
//
// The handler reaches its data through the interface; the fixtures need the connection,
// which only the concrete type exposes.
func eventsTestDataSource(t *testing.T, instance *blnk.Blnk) *database.Datasource {
	t.Helper()

	ds, ok := instance.GetDataSource().(*database.Datasource)
	require.True(t, ok, "the api test harness must expose the concrete datasource")

	return ds
}

// TestListDeadLetterEvents_HonoursIncludeCountAlongsideEveryFilter is the endpoint half
// of the filtered-inventory contract.
func TestListDeadLetterEvents_HonoursIncludeCountAlongsideEveryFilter(t *testing.T) {
	router, instance := setupAuthedRouter(t, true, nil)
	ds := eventsTestDataSource(t, instance)

	eventType := newDeadLetterEventType(t)
	base := time.Now().Add(-time.Hour)

	const (
		deadLettered = 4
		failed       = 2
		matches      = deadLettered + failed
	)

	for i := 0; i < deadLettered; i++ {
		seedDeadLetteredEvent(t, ds, eventType,
			coremodel.EventOutboxStatusDeadLettered, base.Add(time.Duration(i)*time.Second))
	}
	for i := 0; i < failed; i++ {
		seedDeadLetteredEvent(t, ds, eventType,
			coremodel.EventOutboxStatusFailed, base.Add(time.Duration(deadLettered+i)*time.Second))
	}

	// A second event type, seeded so that a wrong count has something to be wrong WITH: a
	// whole-table or whole-status aggregate would include these rows.
	otherType := newDeadLetterEventType(t)
	seedDeadLetteredEvent(t, ds, otherType,
		coremodel.EventOutboxStatusDeadLettered, base.Add(time.Minute))

	// get issues an authenticated request and returns the decoded {data, total_count}
	// envelope, failing the test on any status other than 200.
	get := func(t *testing.T, query string) (int, []interface{}) {
		t.Helper()

		recorder := httptest.NewRecorder()
		request, err := http.NewRequest(http.MethodGet, "/events/dead-letter?"+query, nil)
		require.NoError(t, err)
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var envelope struct {
			Data       []interface{} `json:"data"`
			TotalCount *int          `json:"total_count"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
		require.NotNil(t, envelope.TotalCount,
			"include_count must produce a total rather than a bare array")

		return *envelope.TotalCount, envelope.Data
	}

	t.Run("event_type filter with a count", func(t *testing.T) {
		total, data := get(t, "event_type="+eventType+"&include_count=true&limit=2")

		assert.Equal(t, matches, total,
			"the total must count the matching rows, not the table")
		assert.Len(t, data, 2,
			"the page must still honour the limit; the total is of the set, not of the page")
	})

	t.Run("event_type and topic together with a count", func(t *testing.T) {
		total, data := get(t,
			"event_type="+eventType+"&topic="+deadLetterFixtureTopic+"&include_count=true")

		assert.Equal(t, matches, total)
		assert.Len(t, data, matches,
			"a page large enough for every match must contain every match")
	})

	t.Run("the dlt_topic spelling of the topic filter with a count", func(t *testing.T) {
		total, _ := get(t,
			"event_type="+eventType+"&dlt_topic="+deadLetterFixtureTopic+".dlt&include_count=true")

		assert.Equal(t, matches, total,
			"both spellings of the topic filter resolve to one predicate, so both must count the same set")
	})

	t.Run("a status filter narrows the total as well as the page", func(t *testing.T) {
		deadLetteredTotal, deadLetteredData := get(t,
			"event_type="+eventType+"&status="+coremodel.EventOutboxStatusDeadLettered+"&include_count=true")
		assert.Equal(t, deadLettered, deadLetteredTotal)
		assert.Len(t, deadLetteredData, deadLettered)

		failedTotal, failedData := get(t,
			"event_type="+eventType+"&status="+coremodel.EventOutboxStatusFailed+"&include_count=true")
		assert.Equal(t, failed, failedTotal,
			"a row whose retry budget is spent but whose dead-letter write failed must be countable on its own: it is the population most in need of attention")
		assert.Len(t, failedData, failed)
	})

	t.Run("a topic the rows are not on returns an empty page and a zero total", func(t *testing.T) {
		total, data := get(t,
			"event_type="+eventType+"&topic=blnk.balances&include_count=true")

		assert.Zero(t, total, "the topic predicate must be applied to the count as well as to the page")
		assert.Empty(t, data)
	})

	t.Run("a filter matching nothing is a successful empty answer", func(t *testing.T) {
		total, data := get(t, "event_type="+newDeadLetterEventType(t)+"&include_count=true")

		assert.Zero(t, total)
		assert.Empty(t, data,
			"an empty filtered inventory is 200 and [], never 404: no events being stuck is a successful answer")
	})
}

// TestListDeadLetterEvents_RefusesAnUnsupportedStatusBeforeCounting keeps validation
// ahead of the count.
//
// The status vocabulary is closed on purpose: a filter that quietly matched nothing
// would answer "nothing is stuck" to an operator who asked a different question.
func TestListDeadLetterEvents_RefusesAnUnsupportedStatusBeforeCounting(t *testing.T) {
	router, _ := setupAuthedRouter(t, true, nil)

	for _, status := range []string{
		coremodel.EventOutboxStatusPending,
		coremodel.EventOutboxStatusProcessing,
		coremodel.EventOutboxStatusDispatched,
		"nonsense",
	} {
		t.Run(status, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request, err := http.NewRequest(http.MethodGet,
				"/events/dead-letter?status="+status+"&include_count=true", nil)
			require.NoError(t, err)
			router.ServeHTTP(recorder, request)

			assert.Equal(t, http.StatusBadRequest, recorder.Code, "body: %s", recorder.Body.String())
		})
	}
}

// eventsRouterRequest issues a request against a router the caller supplies, with an
// optional JSON body.
//
// Parameters:
//   - t *testing.T: the test, for Helper.
//   - router *gin.Engine: the router under test.
//   - method, path string: the request line.
//   - body string: a JSON body, or "" for none.
//
// Returns:
//   - *httptest.ResponseRecorder: the recorded response.
func eventsRouterRequest(t *testing.T, router *gin.Engine, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()

	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}

	request := httptest.NewRequest(method, path, reader)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)

	return w
}

// eventsRouter builds a real router for the /events surface, with the master-key
// context injected into the API's OWN engine before the routes are registered.
//
// It mirrors setupAuthedRouter — reaching for apiInstance.router, which this test
// package can — rather than wrapping the finished router in a second engine.
func eventsRouter(t *testing.T, isMaster bool) *gin.Engine {
	t.Helper()

	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_md_async",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})

	cnf, err := config.Fetch()
	require.NoError(t, err)

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err)

	newBlnk, err := blnk.NewBlnk(db)
	require.NoError(t, err)

	apiInstance := NewAPI(newBlnk)
	require.NotNil(t, apiInstance, "NewAPI returned nil, which means the configuration was unusable")

	apiInstance.router.Use(func(c *gin.Context) {
		c.Set("isMasterKey", isMaster)
		c.Next()
	})

	return apiInstance.Router()
}

// TestEventsAPI_RoutesAreReachableAndMasterKeyGated is the authorization-ordering
// matrix, and it covers the trap that has no other symptom.
//
// The gate itself is what this table asserts: a non-master caller must be refused with
// AUTH_MASTER_KEY_REQUIRED, and refused BEFORE any work — these are triage endpoints
// over the whole deployment's event inventory, not a tenant's own data — while the
// master key must pass it and reach the handler.
func TestEventsAPI_RoutesAreReachableAndMasterKeyGated(t *testing.T) {
	for name, target := range map[string]struct {
		method string
		path   string
	}{
		"dead-letter inventory": {http.MethodGet, "/events/dead-letter"},
		"outbox statistics":     {http.MethodGet, "/events/stats"},
		"replay":                {http.MethodPost, "/events/dead-letter/evt_probe/replay"},
	} {
		t.Run(name+" refuses a non-master caller", func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, false), target.method, target.path, "")

			assertErrorCode(t, w, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
		})

		t.Run(name+" is reachable by the master key", func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, true), target.method, target.path, "")

			require.NotEqual(t, http.StatusForbidden, w.Code,
				"the master key must pass the endpoint's own gate")

			// A backstop, not the proof: secure mode is off here, so the resource map was
			// never consulted. The real assertion lives in the secure-mode test below.
			body := w.Body.String()
			assert.NotContains(t, body, string(apierror.ErrAuthUnknownResource),
				"%s %s was aborted as an unknown authorization resource, which means the path "+
					"prefix is missing from pathToResource and NO caller can reach it",
				target.method, target.path)
		})
	}
}

// TestEventsAPI_DeadLetterInventoryRespondsWithAJSONArray pins the response SHAPE,
// which a reconciliation script depends on more than it depends on any single field.
func TestEventsAPI_DeadLetterInventoryRespondsWithAJSONArray(t *testing.T) {
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet, "/events/dead-letter", "")

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	// THE ENVELOPE, not a bare array: this endpoint pages by keyset, so the response has to
	// carry has_more beside the rows or a caller cannot know whether to ask again. What the
	// test is really about survives unchanged — `data` must be an ARRAY and never null, or a
	// script that ranges over it breaks precisely when nothing is wrong.
	var envelope struct {
		Data    []map[string]interface{} `json:"data"`
		HasMore bool                     `json:"has_more"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &envelope), "body: %s", w.Body.String())
	require.NotNil(t, envelope.Data,
		"an empty inventory must serialise as [] rather than null; got %s",
		strings.TrimSpace(w.Body.String()))
}

// TestEventsAPI_DeadLetterPagingIsBoundedAndValidated covers the query-string contract,
// and it asserts the contract this package actually keeps rather than the one it might
// have.
//
// The split is deliberate and it is the package's convention, shared with
// ParseFiltersFromBody and GetAllLedgers: a value that does not PARSE is refused,
// because defaulting it would serve a page the caller never asked for and the mistake
// is the caller's to see; a value that parses but is out of range is NORMALISED,
// because "give me everything" and "give me nothing" are asks with a sensible reading
// and refusing them would make this endpoint behave unlike every other list endpoint
// here.
func TestEventsAPI_DeadLetterPagingIsBoundedAndValidated(t *testing.T) {
	t.Run("a valid page is accepted", func(t *testing.T) {
		// limit and cursor, because this endpoint pages by KEYSET. An offset into a table
		// rows are continuously appended to and removed from cannot describe a stable page,
		// so `offset` is not among the accepted parameters and is refused with the other
		// unknown narrowings — see TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter.
		w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
			"/events/dead-letter?limit=5", "")

		assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	})

	for name, query := range map[string]string{
		"a non-numeric limit": "limit=many",
		"a fractional limit":  "limit=2.5",
	} {
		t.Run(name+" is refused", func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
				"/events/dead-letter?"+query, "")

			assert.Equal(t, http.StatusBadRequest, w.Code,
				"a value that does not parse must be refused rather than defaulted: the caller "+
					"cannot tell that the page it received is not the page it asked for. body: %s",
				w.Body.String())
		})
	}

	for name, query := range map[string]string{
		"a zero limit":              "limit=0",
		"a negative limit":          "limit=-1",
		"a limit above the ceiling": "limit=10000",
	} {
		t.Run(name+" is normalised rather than refused", func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
				"/events/dead-letter?"+query, "")

			assert.Equal(t, http.StatusOK, w.Code,
				"an out-of-range but parseable page is normalised, matching every other list "+
					"endpoint in this package. body: %s", w.Body.String())
		})
	}

	// THE CEILING ITSELF. Asserted at the reader, because over an empty inventory a
	// normalised page and an unbounded one produce the same response body.
	t.Run("the normalisation keeps the page bounded", func(t *testing.T) {
		for name, tt := range map[string]struct {
			query string
			limit int
		}{
			"absent":              {"", deadLetterPageDefaultLimit},
			"within the ceiling":  {"limit=50", 50},
			"exactly the ceiling": {fmt.Sprintf("limit=%d", deadLetterPageMaxLimit), deadLetterPageMaxLimit},
			"above the ceiling":   {fmt.Sprintf("limit=%d", deadLetterPageMaxLimit+1), deadLetterPageDefaultLimit},
			"far above": {fmt.Sprintf("limit=%d", 1_000_000),
				deadLetterPageDefaultLimit},
			"zero":     {"limit=0", deadLetterPageDefaultLimit},
			"negative": {"limit=-5", deadLetterPageDefaultLimit},
		} {
			t.Run(name, func(t *testing.T) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodGet, "/events/dead-letter?"+tt.query, nil)

				limit, ok := deadLetterPageLimitFromQuery(c)
				require.True(t, ok, "a parseable value must not be refused")
				assert.Equal(t, tt.limit, limit,
					"NO PAGE MAY EXCEED THE CEILING: an unbounded limit would let one request read "+
						"the whole dead-letter inventory")
				assert.LessOrEqual(t, limit, deadLetterPageMaxLimit)
				assert.Positive(t, limit, "and a page of zero or fewer rows answers nothing")
			})
		}
	})

	// NO OFFSET SUB-TEST, deliberately. This endpoint pages by CURSOR, not by offset: the
	// accepted parameter is `cursor` and `offset` is refused with the rest of the unknown
	// narrowings by TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter below. An
	// offset into a table rows are continuously appended to and removed from cannot
	// describe a stable page, which is why the keyset cursor exists; a test asserting an
	// offset were clamped would pin a parameter the surface does not accept.
}

// TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter is the fail-loud contract on
// filters, and it is a deliberate departure from ignoring what you cannot honour.
//
// These are triage endpoints.
func TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter(t *testing.T) {
	// aggregate_id, and NOT an occurrence bound. This once sent occurred_after on the
	// strength of the window being unsupported; the endpoint now applies a window, so that
	// request is a legitimate 200 and the test proved nothing about refusals.
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
		"/events/dead-letter?aggregate_id=txn_01HXYZ", "")

	assert.Equal(t, http.StatusBadRequest, w.Code,
		"a narrowing this endpoint cannot apply must be refused, not ignored: ignoring it answers "+
			"a different question than the operator asked. body: %s", w.Body.String())
}

// TestEventsAPI_ReplayRefusesAnUnknownEvent covers the replay error matrix at the
// router.
func TestEventsAPI_ReplayRefusesAnUnknownEvent(t *testing.T) {
	// A WELL-FORMED identifier that matches nothing. event_id is a canonical UUID by
	// contract, so a malformed one is a 400 about the REQUEST and is covered separately;
	// what this test is about is the identifier that could have existed and does not.
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodPost,
		"/events/dead-letter/"+uuid.NewString()+"/replay", "")

	require.NotEqual(t, http.StatusInternalServerError, w.Code,
		"an unknown event identifier is a caller-side mistake, not a server fault. body: %s",
		w.Body.String())
	assert.Contains(t, []int{http.StatusNotFound, http.StatusConflict, http.StatusServiceUnavailable},
		w.Code,
		"the refusal must be typed: 404 when no such event, 409 when it is not dead-lettered, "+
			"503 when the transport cannot take it. body: %s", w.Body.String())
}

// TestEventsAPI_ReplayRefusesABlankIdentifier pins the parameter guard.
//
// Gin does not route an empty path parameter, so this arrives as a different path entirely;
// what must not happen is a 500 or a replay of something arbitrary.
func TestEventsAPI_ReplayRefusesABlankIdentifier(t *testing.T) {
	for name, path := range map[string]string{
		"an empty identifier":      "/events/dead-letter//replay",
		"a whitespace identifier":  "/events/dead-letter/%20/replay",
		"a missing replay segment": "/events/dead-letter/evt_probe",
	} {
		t.Run(name, func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodPost, path, "")

			assert.NotEqual(t, http.StatusInternalServerError, w.Code,
				"a malformed replay path must not reach the service as a server fault. body: %s",
				w.Body.String())
			assert.NotEqual(t, http.StatusOK, w.Code,
				"and it must certainly not succeed")
		})
	}
}

// TestEventsAPI_StatsRespondsWithTheOutboxCounts covers the endpoint the daily
// zero-loss reconciliation is built on.
func TestEventsAPI_StatsRespondsWithTheOutboxCounts(t *testing.T) {
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet, "/events/stats", "")

	require.Equal(t, http.StatusOK, w.Code,
		"the statistics endpoint must answer from PostgreSQL even when no broker is configured, "+
			"or the daily reconciliation cannot be run. body: %s", w.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body),
		"the response must be a JSON object: %s", w.Body.String())
	assert.NotEmpty(t, body, "an empty object tells a reconciliation nothing")
}

// TestEventsAPI_StatsRejectsAnUnknownQueryParameter applies the same fail-loud rule to the
// statistics endpoint, where the consequence is a reconciliation that silently compares the
// wrong two numbers.
func TestEventsAPI_StatsRejectsAnUnknownQueryParameter(t *testing.T) {
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
		"/events/stats?status=dead_lettered", "")

	assert.Equal(t, http.StatusBadRequest, w.Code,
		"a parameter this endpoint does not honour must be refused. body: %s", w.Body.String())
}

// TestEventsAPI_StatsRefusesAWindowItCannotReport is the guard.
//
// The response reports the interval it measured as `window_seconds`, an integer.
//
// An operator comparing a count against the window they asked for would be comparing it
// against a different one, with nothing in the response revealing the difference.
func TestEventsAPI_StatsRefusesAWindowItCannotReport(t *testing.T) {
	router := eventsRouter(t, true)

	t.Run("a fractional window is refused rather than truncated", func(t *testing.T) {
		for _, window := range []string{"1500ms", "500ms", "90s500ms", "1us", "2m30s100ms"} {
			t.Run(window, func(t *testing.T) {
				w := eventsRouterRequest(t, router, http.MethodGet,
					"/events/stats?window="+window, "")

				require.Equal(t, http.StatusBadRequest, w.Code,
					"a window that cannot be reported in whole seconds must be refused. body: %s",
					w.Body.String())
				assertErrorCode(t, w, http.StatusBadRequest, apierror.ErrGenValidation)

				// The refusal has to say WHAT is wrong and what is acceptable, or the operator's
				// next attempt is another guess.
				body := w.Body.String()
				assert.Contains(t, body, "whole number of seconds")
				assert.Contains(t, body, eventQueryParamWindow)
			})
		}
	})

	t.Run("a whole-second window is honoured", func(t *testing.T) {
		// 1.5h is 5400 seconds exactly, so a fractional UNIT is not the test: what matters is
		// whether the duration lands on a whole second.
		for _, window := range []string{"1s", "90s", "15m", "24h", "2m30s", "1.5h"} {
			t.Run(window, func(t *testing.T) {
				w := eventsRouterRequest(t, router, http.MethodGet,
					"/events/stats?window="+window, "")

				assert.Equal(t, http.StatusOK, w.Code,
					"%s is a whole number of seconds and must be accepted. body: %s",
					window, w.Body.String())
			})
		}
	})
}

// eventsRouteExists reports whether the router serves pattern under ANY method.
func eventsRouteExists(router *gin.Engine, pattern string) bool {
	for _, route := range router.Routes() {
		if route.Path == pattern {
			return true
		}
	}

	return false
}

// TestEventsAPI_UnsupportedMethodsDoNotReachTheHandlers is the routing assertion.
//
// The three routes are registered for one verb each.
//
// The expectation is exact rather than negative, and the two acceptable answers are
// different facts about the router:
//
//   - 405 METHOD NOT ALLOWED means gin matched the PATH and rejected the verb, which is
//     what HandleMethodNotAllowed produces when it is enabled.
//   - 404 NOT FOUND means no route matched at all, which is gin's default for an
//     unregistered method/path pair.
func TestEventsAPI_UnsupportedMethodsDoNotReachTheHandlers(t *testing.T) {
	// A canonical id the replay handler would accept. With a malformed one, a wrongly
	// registered verb answers 400 from the handler's own validation and this test cannot
	// tell that apart from the router refusing it.
	replayableID := uuid.NewString()

	// pattern is the shape gin REGISTERS, which is what router.Routes() reports. It is
	// carried separately from the path actually requested because the replay route is
	// parameterised: a concrete "/events/dead-letter/<uuid>/replay" can never equal the
	// registered "/events/dead-letter/:event_id/replay", so a structural check written
	// against the request path would hold no matter what the router contained — vacuously,
	// for exactly the three cases that most need it.
	for name, target := range map[string]struct {
		method  string
		path    string
		pattern string
	}{
		"POST to the inventory": {http.MethodPost, "/events/dead-letter", "/events/dead-letter"},
		"DELETE the inventory":  {http.MethodDelete, "/events/dead-letter", "/events/dead-letter"},
		"PUT the statistics":    {http.MethodPut, "/events/stats", "/events/stats"},
		"DELETE the statistics": {http.MethodDelete, "/events/stats", "/events/stats"},
		"GET the replay": {
			http.MethodGet,
			"/events/dead-letter/" + replayableID + "/replay",
			"/events/dead-letter/:event_id/replay",
		},
		"DELETE the replay": {
			http.MethodDelete,
			"/events/dead-letter/" + replayableID + "/replay",
			"/events/dead-letter/:event_id/replay",
		},
		"PUT the replay": {
			http.MethodPut,
			"/events/dead-letter/" + replayableID + "/replay",
			"/events/dead-letter/:event_id/replay",
		},
	} {
		t.Run(name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)

			// THE MASTER KEY IS PRESENTED. This harness runs in secure mode, so an
			// unauthenticated request is refused with 401 before gin routes it at all — and a
			// 401 says nothing about whether the verb is registered. Authenticating removes
			// authentication from the answer, leaving the router as the only thing that can
			// refuse.
			w := eventsKeyedRequest(t, router, target.method, target.path, eventsMockMasterKey)

			assert.Containsf(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, w.Code,
				"%s %s answered %d. It is registered for no handler, so the ROUTER must refuse it "+
					"with 404 or 405; any other status means the verb reached a handler registered "+
					"for a different one. body: %s",
				target.method, target.path, w.Code, w.Body.String())

			// STRUCTURAL, not only observational: the pair is absent from the route table. A gin
			// router can answer 404 for a registered route whose handler is unreachable for some
			// other reason, and 405 is emitted by a setting rather than by the table, so the
			// table is inspected directly as well.
			for _, route := range router.Routes() {
				if route.Method != target.method || route.Path != target.pattern {
					continue
				}

				assert.Failf(t, "an unsupported verb is registered",
					"%s %s IS REGISTERED, to %s. The event surface registers one verb per route; a "+
						"second one is either a duplicate registration or a router.Any",
					target.method, target.pattern, route.Handler)
			}
			// The pattern itself must exist under SOME method, or this case is aimed at a path
			// the surface does not serve at all and would pass for the wrong reason.
			assert.Truef(t, eventsRouteExists(router, target.pattern),
				"no route is registered at %s under any method, so this case proves nothing about "+
					"%s being unsupported — it proves the path is misspelled",
				target.pattern, target.method)

			// AND NOTHING WAS QUERIED. A verb that reached a handler and then failed its own
			// validation would answer 4xx while having already read the outbox.
			assert.Emptyf(t, eventsRepositoryCallsExcludingAuth(ds),
				"%s %s reached the repository: %v",
				target.method, target.path, eventsRepositoryCallsExcludingAuth(ds))
		})
	}
}

// eventsResourceMapMasterKey is the master secret the secure-mode resource-map test configures.
const eventsResourceMapMasterKey = "resource-map-master-key-for-the-real-router-tests"

// TestEventsAPI_PathPrefixesResolveToAuthorizationResources is the trap the other tests
// in this file cannot see, and it needs SECURE MODE plus a NON-MASTER key to be visible
// at all.
func TestEventsAPI_PathPrefixesResolveToAuthorizationResources(t *testing.T) {
	router, newBlnk, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
		cfg.Server.Secure = true
		cfg.Server.SecretKey = eventsResourceMapMasterKey
	})

	// A real, valid, NON-MASTER key. Its scopes are deliberately unrelated to these routes:
	// what is under test is that the path RESOLVES to a resource, not that this key may use it.
	scoped, err := newBlnk.CreateAPIKey(
		context.Background(),
		"resource-map-probe-"+coremodel.GenerateUUIDWithSuffix("key"),
		"resource-map-owner",
		[]string{"ledgers:read"},
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err, "creating the scoped probe key")

	for name, target := range map[string]struct {
		method string
		path   string
		scope  string
	}{
		"events dead-letter": {http.MethodGet, "/events/dead-letter", "events:read"},
		"events stats":       {http.MethodGet, "/events/stats", "events:read"},
		"events replay":      {http.MethodPost, "/events/dead-letter/evt_probe/replay", "events:write"},
		"subscribers list":   {http.MethodGet, "/subscribers", "subscribers:read"},
		"subscribers read":   {http.MethodGet, "/subscribers/sub_probe", "subscribers:read"},
		"kafka credentials": {
			http.MethodPost, "/subscribers/sub_probe/kafka-credentials", "subscribers:write",
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(target.method, target.path, strings.NewReader(""))
			request.Header.Set("X-Blnk-Key", scoped.Key)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, request)

			var body struct {
				ErrorDetail apierror.APIError `json:"error_detail"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body),
				"every refusal on this surface carries the structured detail: %s", w.Body.String())

			require.NotEqual(t, apierror.ErrAuthUnknownResource, body.ErrorDetail.Code,
				"%s %s RESOLVES TO NO AUTHORIZATION RESOURCE, so the middleware aborts every "+
					"request from every non-master principal. The path prefix is missing from "+
					"pathToResource in api/middleware/auth.go; adding the scope constant alone is "+
					"not enough", target.method, target.path)

			// The resolved scope, named in the refusal. This is the assertion that pins the
			// MAPPING rather than merely its existence: a prefix pointed at the wrong resource
			// would authorise these routes under another feature's scope and nothing else in the
			// suite would see it.
			require.Equal(t, apierror.ErrAuthInsufficientPermissions, body.ErrorDetail.Code,
				"a key scoped to something else must be refused on permissions, which is what "+
					"proves the route was resolved and evaluated. body: %s", w.Body.String())
			assert.Contains(t, body.ErrorDetail.Message, target.scope,
				"the refusal must name %q: that is the resource this path maps to and the action "+
					"this method maps to, and both have to be right", target.scope)
		})
	}
}

// eventsQueryContext builds a gin context carrying a GET request with the given raw
// query string, plus the recorder any refusal is written to.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - rawQuery string: the query string WITHOUT the leading "?".
//
// Returns:
//   - *gin.Context: the context the readers under test receive.
//   - *httptest.ResponseRecorder: the response a refusal is written to.
func eventsQueryContext(t *testing.T, rawQuery string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)

	target := "/events/dead-letter"
	if rawQuery != "" {
		target += "?" + rawQuery
	}

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	require.NoError(t, err)
	c.Request = request

	return c, recorder
}

// Four tests that arrived with the occurrence window are GONE, and what they asserted
// is covered above rather than lost.
//
// They were written against a different shape of the same feature: the window carried
// as *time.Time so an absent bound is nil, a per-status aggregate reached directly on
// the datasource, and a countableDeadLetterPage predicate deciding which filters may be
// counted at all. This tree resolves each of those differently, and the difference is a
// capability rather than a preference:
//
//   - The window is time.Time VALUES on DeadLetterListOptions, zero meaning unbounded,
//     bound as SQL index conditions by deadLetterFilterClause.
//   - The total comes from the dead-letter SERVICE, through a paired read that draws it
//     and the page from ONE database snapshot, so the total describes the very
//     population the page is a slice of.
//   - deadLetterInventoryTotal therefore has no per-status map to assert arguments
//     against.

// TestDeadLetterQueryParameters_AcceptTheOccurrenceWindow pins the accepted set.
//
// The accepted set is CLOSED: an unknown parameter is refused rather than ignored,
// because an operator who filtered and received the whole inventory reads stale entries
// as current. That makes membership of the set the whole contract — a window the
// endpoint can apply but does not list would be rejected as unsupported, which is the
// same wrong answer with a different message.
func TestDeadLetterQueryParameters_AcceptTheOccurrenceWindow(t *testing.T) {
	assert.Contains(t, deadLetterQueryParameters, eventQueryParamOccurredFrom)
	assert.Contains(t, deadLetterQueryParameters, eventQueryParamOccurredTo)
	assert.Equal(t, "occurred_from", eventQueryParamOccurredFrom,
		"the parameter name is published to operators and to the triage runbook")
	assert.Equal(t, "occurred_to", eventQueryParamOccurredTo)

	// RETARGETED ONTO rejectUnsupportedQueryParameters, WHICH IS THE FUNCTION THE ROUTER
	// REACHES.
	//
	// A byte-for-byte second copy of that guard, invoked by nothing on the request path,
	// would attach the coverage for "an unknown query parameter is refused" to code that
	// never runs and leave the guard actually protecting GET /events/dead-letter with none.
	// These subtests therefore exercise the live one.
	t.Run("a windowed request is not refused as unsupported", func(t *testing.T) {
		c, recorder := eventsQueryContext(t,
			"occurred_from=2026-08-01T00:00:00Z&occurred_to=2026-08-02T00:00:00Z")

		assert.True(t, rejectUnsupportedQueryParameters(c, deadLetterQueryParameters))
		assert.Equal(t, http.StatusOK, recorder.Code, "no refusal may be written")
		assert.Empty(t, recorder.Body.String())
	})

	t.Run("an unknown parameter is still refused, and the window is offered back", func(t *testing.T) {
		c, recorder := eventsQueryContext(t, "occurred_at=2026-08-01T00:00:00Z")

		assert.False(t, rejectUnsupportedQueryParameters(c, deadLetterQueryParameters))
		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
		assert.Contains(t, recorder.Body.String(), "occurred_from",
			"the refusal must name the parameters the endpoint does accept, or a near miss is a dead end")
		assert.Contains(t, recorder.Body.String(), "occurred_to")
	})
}

// TestDeadLetterQueryParameters_AreDocumentedInTheTriageRunbook binds the accepted set
// to the runbook an operator actually reads.
//
// The rendering matters as much as the presence.
func TestDeadLetterQueryParameters_AreDocumentedInTheTriageRunbook(t *testing.T) {
	runbook := eventsReadRepositoryFile(t, "docs", "kafka-operations.md")

	for _, parameter := range deadLetterQueryParameters {
		assert.Contains(t, runbook, "| `"+parameter+"` |",
			"every accepted query parameter must appear in the dead-letter filter table in "+
				"docs/kafka-operations.md, or the published runbook and the endpoint disagree")
	}

	// And the runbook must not carry the claim the window replaced, in any of the forms it was
	// written in. A stale denial is worse than an omission: it tells an operator not to try.
	for _, staleClaim := range []string{
		"no occurrence-window filter",
		"There is **no occurrence-window filter at any layer**",
	} {
		assert.NotContains(t, runbook, staleClaim,
			"the runbook must not deny a filter the endpoint accepts")
	}
}

// TestDeadLetterListing_RunbookCommandsConsumeTheEnvelope binds the response SHAPE to
// the commands an operator copies out of the runbook.
//
// The handler has always answered with a page envelope, and every runbook command
// parsed the body as a bare array.
func TestDeadLetterListing_RunbookCommandsConsumeTheEnvelope(t *testing.T) {
	runbook := eventsReadRepositoryFile(t, "docs", "kafka-operations.md")

	// The three jq forms that read a bare array. Each is the exact text that appeared in a
	// dead-letter command, so a reintroduction is caught rather than merely discouraged.
	for _, bareArray := range []string{
		`jq '.[] | {event_id`,
		`jq -r '.[].event_id'`,
		`jq 'group_by(.failure_reason)`,
	} {
		assert.NotContainsf(t, runbook, bareArray,
			"a dead-letter command must read .data[]; %q parses the envelope as an array and "+
				"silently yields the wrong thing", bareArray)
	}

	// And the envelope has to be STATED, not merely used, because an operator writing their own
	// script reads the prose rather than reverse-engineering the examples.
	for _, required := range []string{
		"`{data, next_cursor, has_more, total_count?}` envelope",
		"read `.data[]`",
		`jq -r '.data[].event_id'`,
	} {
		assert.Containsf(t, runbook, required,
			"the runbook must publish the envelope contract: %q is missing", required)
	}

	// The empty answer, in the form it is actually returned. "200 and []" described a body
	// the endpoint does not produce, and a script branching on it would treat every empty
	// inventory as a shape mismatch.
	assert.Contains(t, runbook, "`200` with `\"data\": []`",
		"an empty inventory is an empty data array inside the envelope, never a bare []")
}

// TestDeadLetterPageResponse_IsTheOnlyShapeTheListingReturns pins the envelope at the
// type level.
//
// A client written from that description reads `.[]` and gets nothing usable.
func TestDeadLetterPageResponse_IsTheOnlyShapeTheListingReturns(t *testing.T) {
	response := DeadLetterPageResponse{Data: []model.DeadLetterEvent{}}

	body, err := json.Marshal(response)
	require.NoError(t, err)

	// data is present and is [] rather than null even with no entries, which is what lets a
	// script range over it unconditionally.
	assert.JSONEq(t, `{"data":[],"has_more":false}`, string(body),
		"the envelope is the shape with or without a total; total_count is added, not substituted")

	withTotal := response
	total := int64(0)
	withTotal.TotalCount = &total

	body, err = json.Marshal(withTotal)
	require.NoError(t, err)
	assert.JSONEq(t, `{"data":[],"has_more":false,"total_count":0}`, string(body),
		"a measured zero must render, which is why the field is a pointer")
}

// eventsReadRepositoryFile returns the text of a repository file, so a published
// contract can be asserted rather than trusted.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - elements ...string: the path elements, relative to the module root.
//
// Returns:
//   - string: the file's contents.
func eventsReadRepositoryFile(t *testing.T, elements ...string) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err, "the working directory must be readable to locate the module root")

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			break
		}

		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent,
			"walked to the filesystem root from %q without finding go.mod", dir)
		dir = parent
	}

	path := filepath.Join(append([]string{dir}, elements...)...)
	contents, err := os.ReadFile(path) //nolint:gosec // a fixed, repository-relative path
	require.NoError(t, err, "%s must be readable to assert its contents", path)

	return string(contents)
}

// ---------------------------------------------------------------------------
// The mock-datasource harness
// ---------------------------------------------------------------------------

// eventsMockMasterKey is the master secret the mock-datasource harness configures.
//
// It is distinct from every other master key in this package so that a router built
// here can never be authorised by a secret another test left in the configuration
// store, and vice versa.
const eventsMockMasterKey = "events-api-mock-datasource-master-key"

// eventsMockAPIKeySecret is the raw credential the non-master probe presents.
//
// It is a plausible opaque token rather than a recognisable literal because it travels
// in an X-Blnk-Key header through the real authentication middleware, and the
// middleware compares it against the master key with a constant-time comparison: a
// value that shares a prefix with the master key would prove nothing extra but would
// make a failure harder to read.
const eventsMockAPIKeySecret = "events-api-non-master-probe-credential"

// eventsBackgroundTouchTimeout bounds the wait for the middleware's background
// last-used update.
//
// It is a safety valve rather than a delay: in a correct run the update lands in
// microseconds and the wait returns immediately.
const eventsBackgroundTouchTimeout = 5 * time.Second

// eventsBackgroundTouchGrace is how long a REFUSED request is watched for a last-used
// update it must never make.
//
// The admitted path waits for an update that is expected, so its bound can be generous
// — it is never reached.
const eventsBackgroundTouchGrace = 200 * time.Millisecond

// requireEventsLastUsedTouch joins an authenticated request with the background
// last-used update the authentication middleware starts for it, and FAILS if that
// update never happens.
//
// Parameters:
//   - t *testing.T: the test, FAILED when the update does not arrive.
//   - touched <-chan struct{}: the channel expectEventsAPIKeyLookup returns.
func requireEventsLastUsedTouch(t *testing.T, touched <-chan struct{}) {
	t.Helper()

	select {
	case <-touched:
	case <-time.After(eventsBackgroundTouchTimeout):
		t.Fatalf(
			"the authentication middleware did not update the API key's last-used timestamp "+
				"within %s. It fires that update on a goroutine for every non-master credential it "+
				"admits, so either the request was not authenticated as expected or the update was "+
				"removed — and a test that proceeded here would be asserting on a call log another "+
				"goroutine may still be writing to", eventsBackgroundTouchTimeout,
		)
	}
}

// setupEventsRouter builds a REAL router over a MOCK datasource.
//
// Secure mode is ON and a master key is configured, because two of the properties under
// test are properties of the authentication middleware: that it resolves "/events" to
// an authorization resource at all, and that it admits a correctly scoped non-master
// key rather than aborting it. Both are invisible with secure mode off — the middleware
// returns before consulting the resource map — so a harness that disabled it could not
// fail for the reason it exists to catch.
//
// Parameters:
//   - t *testing.T: the test, failed on any construction error.
//   - mutate func(*config.Configuration): an optional hook to adjust the configuration
//     before it is published.
//
// Returns:
//   - *gin.Engine: the router, with the full production middleware chain.
//   - *mocks.MockDataSource: the datasource behind it, ready to be programmed.
func setupEventsRouter(t *testing.T, mutate func(*config.Configuration)) (*gin.Engine, *mocks.MockDataSource) {
	t.Helper()

	apiInstance, ds := newEventsAPIOverMockDatasource(t, mutate)

	return apiInstance.Router(), ds
}

// newEventsAPIOverMockDatasource is setupEventsRouter without the route registration.
//
// It exists because two of the handler contracts are only reachable by CALLING THE
// HANDLER: a route parameter that is absent altogether cannot be expressed as a URL the
// registered route matches, since gin will not match a path with a missing segment.
//
// Parameters:
//   - t *testing.T: the test, failed on any construction error.
//   - mutate func(*config.Configuration): an optional configuration hook.
//
// Returns:
//   - *Api: the constructed API, with no routes registered.
//   - *mocks.MockDataSource: the datasource behind it.
func newEventsAPIOverMockDatasource(
	t *testing.T,
	mutate func(*config.Configuration),
) (*Api, *mocks.MockDataSource) {
	t.Helper()

	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_events",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:    true,
			SecretKey: eventsMockMasterKey,
		},
	}
	if mutate != nil {
		mutate(cfg)
	}

	// RESTORED WHEN THE TEST ENDS. config.ConfigStore is a process-global atomic.Value, so
	// everything installed here — the master key, the secure-mode flag, the topic prefix
	// and, for the replay test, a live broker list — is read by every LATER test in the
	// package that calls config.Fetch. Without this, a test asserting the unconfigured or
	// insecure posture fails inside a helper that never mentions Kafka, and the failure
	// names neither this harness nor the test that ran before it.
	previousConfiguration := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previousConfiguration != nil {
			config.ConfigStore.Store(previousConfiguration)

			return
		}

		// An atomic.Value cannot be emptied, so a process that held nothing before this test is
		// returned to a configuration that carries nothing rather than left holding this one.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.MockConfig(cfg)

	cnf, err := config.Fetch()
	require.NoError(t, err, "the mocked configuration must load")
	// THE CALLER'S OWN POSTURE MUST SURVIVE validateAndAddDefaults, whichever it asked for.
	// Asserted rather than assumed in both directions: with secure mode on, a value silently
	// defaulted back to false would make the authentication middleware return before it
	// consults the resource map and every authorization assertion below would pass vacuously;
	// with it deliberately off — the posture docker-compose.yaml ships, and the one
	// TestEventsAPI_IsOperableWithSecureModeOff covers — a value defaulted back to true would
	// mean that test never exercised the path it exists for.
	require.Equal(t, cfg.Server.Secure, cnf.Server.Secure,
		"the mocked secure-mode posture must survive validateAndAddDefaults; the harness and the "+
			"middleware would otherwise disagree about which authentication path is under test")
	// THE DEFAULT POSTURE IS NO BROKER, and it is asserted rather than assumed: it is what
	// makes a replay answer EVENT_KAFKA_UNAVAILABLE deterministically and it is the
	// graceful-degradation deployment the listing and statistics routes must work in.
	if len(cfg.Kafka.Brokers) == 0 {
		require.Empty(t, cnf.Kafka.Brokers,
			"this harness must run with NO broker configured unless the caller asked for one: "+
				"that is the posture the endpoints are required to degrade gracefully in")
	} else {
		require.Len(t, cnf.Kafka.Brokers, len(cfg.Kafka.Brokers),
			"config.MockConfig REJECTED the configuration and left the previous one in the "+
				"store, so this test would run against another test's deployment. Its reason is "+
				"on the error log just above")
	}

	ds := new(mocks.MockDataSource)

	// The mock goes to the service container UNWRAPPED. That race is fixed at its source,
	// so the harness is an ordinary one again and the join is an ordinary wait — see
	// requireEventsLastUsedTouch.
	instance, err := blnk.NewBlnk(ds)
	require.NoError(t, err, "the service container must build over the mock datasource")

	apiInstance := NewAPI(instance)
	require.NotNil(t, apiInstance, "NewAPI returned nil, which means the configuration was unusable")

	return apiInstance, ds
}

// eventsTestAPIKey is a valid, NON-MASTER principal for the authorization tests.
//
// Validity is the whole point and it is not incidental: model.APIKey.IsValid is
// `!IsRevoked && time.Now().Before(ExpiresAt)`, and the middleware aborts an invalid
// key with AUTH_EXPIRED_API_KEY long before it reaches the resource map.
//
// Parameters:
//   - scopes ...string: the granted scopes, in "resource:action" form.
//
// Returns:
//   - *coremodel.APIKey: the principal, carrying eventsMockAPIKeySecret as its key.
func eventsTestAPIKey(scopes ...string) *coremodel.APIKey {
	return &coremodel.APIKey{
		APIKeyID:  "api_key_events_probe",
		Key:       eventsMockAPIKeySecret,
		Name:      "events surface probe",
		OwnerID:   "owner_events_probe",
		Scopes:    scopes,
		ExpiresAt: time.Now().Add(time.Hour),
		CreatedAt: time.Now().Add(-time.Hour),
		IsRevoked: false,
	}
}

// expectEventsAPIKeyLookup programs the two datasource calls the authentication
// middleware makes for a non-master credential.
//
// GetAPIKey is the datasource method behind blnk.GetAPIKeyByKey, so that is the
// expectation name — not the service method's.
//
// Parameters:
//   - ds *mocks.MockDataSource: the datasource to program.
//   - key *coremodel.APIKey: the principal the lookup resolves to.
//
// Returns:
//   - <-chan struct{}: signalled once the background last-used update has been
//     recorded.
func expectEventsAPIKeyLookup(ds *mocks.MockDataSource, key *coremodel.APIKey) <-chan struct{} {
	touched := make(chan struct{}, 1)

	ds.On("GetAPIKey", mock.Anything, eventsMockAPIKeySecret).Return(key, nil)
	ds.On("UpdateLastUsed", mock.Anything, key.APIKeyID).
		Run(func(mock.Arguments) {
			select {
			case touched <- struct{}{}:
			default:
			}
		}).
		Return(nil).
		Once()

	return touched
}

// expectEventsAPIKeyLookupWithoutTouch programs the datasource for a credential the
// authentication middleware is going to REFUSE.
//
// UpdateLastUsed is PROGRAMMED but `.Maybe()`.
//
// Parameters:
//   - ds *mocks.MockDataSource: the datasource to program.
//   - key *coremodel.APIKey: the principal the lookup resolves to, whose scopes get it
//     refused.
//
// Returns:
//   - <-chan struct{}: signalled if a last-used update happens, which is the failure.
func expectEventsAPIKeyLookupWithoutTouch(
	ds *mocks.MockDataSource,
	key *coremodel.APIKey,
) <-chan struct{} {
	touched := make(chan struct{}, 1)

	ds.On("GetAPIKey", mock.Anything, eventsMockAPIKeySecret).Return(key, nil)
	ds.On("UpdateLastUsed", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			select {
			case touched <- struct{}{}:
			default:
			}
		}).
		Return(nil).
		Maybe()

	return touched
}

// requireNoEventsLastUsedTouch FAILS if the middleware recorded a last-used update for
// a credential it refused.
//
// Parameters:
//   - t *testing.T: the test, FAILED when an update arrives.
//   - touched <-chan struct{}: the channel expectEventsAPIKeyLookupWithoutTouch
//     returns.
func requireNoEventsLastUsedTouch(t *testing.T, touched <-chan struct{}) {
	t.Helper()

	select {
	case <-touched:
		t.Fatal(
			"the authentication middleware updated the API key's last-used timestamp for a request " +
				"it REFUSED on scope. last_used_at is read as evidence that a credential is in use, " +
				"so a refused attempt must not advance it; the update belongs below the permission " +
				"check in api/middleware/auth.go, not above it",
		)
	case <-time.After(eventsBackgroundTouchGrace):
	}
}

// eventsAPIRoute is one registered route on the event management surface, named so a
// failure message says which endpoint broke rather than which table row did.
type eventsAPIRoute struct {
	name   string
	method string
	path   string
	// scope is the "resource:action" the authorization middleware must resolve this route
	// to. It is asserted verbatim in the refusal message, which is what pins the mapping
	// rather than merely its existence.
	scope string
	// repositoryMethod is the first datasource method this route's handler would reach if
	// the master-key gate let it through. It is the subject of the AssertNotCalled checks:
	// a gate that fires AFTER the work has started is not a gate.
	repositoryMethod string
}

// eventsAPIRoutes is every route in api/events.go, with the authorization scope and the
// repository seam each one owns.
//
// Declared once and shared by every table-driven test below, so a route added to the
// surface without being added here is visible as an omission in one place.
var eventsAPIRoutes = []eventsAPIRoute{
	{
		name:             "dead-letter inventory",
		method:           http.MethodGet,
		path:             "/events/dead-letter",
		scope:            "events:read",
		repositoryMethod: "ListDeadLetterInventory",
	},
	{
		name:             "outbox statistics",
		method:           http.MethodGet,
		path:             "/events/stats",
		scope:            "events:read",
		repositoryMethod: "CountEventOutboxByStatus",
	},
	{
		name:             "dead-letter replay",
		method:           http.MethodPost,
		path:             "/events/dead-letter/3f2504e0-4f89-11d3-9a0c-0305e82c3301/replay",
		scope:            "events:write",
		repositoryMethod: "ClaimEventForReplay",
	},
}

// eventsKeyedRequest issues a request carrying an X-Blnk-Key credential through the
// real middleware chain.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting.
//   - router *gin.Engine: the router under test.
//   - method, path string: the request line.
//   - key string: the credential for the X-Blnk-Key header. Empty sends no header.
//
// Returns:
//   - *httptest.ResponseRecorder: the recorded response.
func eventsKeyedRequest(
	t *testing.T,
	router *gin.Engine,
	method, path, key string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(method, path, strings.NewReader(""))
	if key != "" {
		request.Header.Set("X-Blnk-Key", key)
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

// eventsErrorDetail decodes the structured error object every refusal on this surface
// carries.
//
// Parameters:
//   - t *testing.T: the test, failed when the body is not the dual error payload.
//   - recorder *httptest.ResponseRecorder: the recorded response.
//
// Returns:
//   - apierror.APIError: the decoded error_detail object.
func eventsErrorDetail(t *testing.T, recorder *httptest.ResponseRecorder) apierror.APIError {
	t.Helper()

	var body struct {
		ErrorDetail apierror.APIError `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body),
		"every refusal on this surface carries error_detail: %s", recorder.Body.String())

	return body.ErrorDetail
}

// eventsRepositoryCallsExcludingAuth names every datasource method a request reached,
// with the two the authentication middleware makes removed.
//
// It is how "no service call is made" is asserted as an absolute rather than as a list
// of specific methods that were not called.
//
// Parameters:
//   - ds *mocks.MockDataSource: the datasource the request ran against.
//
// Returns:
//   - []string: the method names, in call order, excluding GetAPIKey and
//     UpdateLastUsed.
func eventsRepositoryCallsExcludingAuth(ds *mocks.MockDataSource) []string {
	reached := make([]string, 0, len(ds.Calls))
	for _, call := range ds.Calls {
		switch call.Method {
		case "GetAPIKey", "UpdateLastUsed":
			continue
		default:
			reached = append(reached, call.Method)
		}
	}

	return reached
}

// TestEventsAPI_AuthorizationResourceIsRegistered is THE decisive authorization test,
// and it is the only one in this file that can fail for the reason it exists to catch.
//
// api/middleware/auth.go returns early twice before the resource map is read.
//
// The resource map is reached by exactly one kind of request: secure mode, a valid
// credential, and NOT the master key. That is the combination assembled here.
//
// getResourceFromPath returns "" and the middleware aborts with AUTH_UNKNOWN_RESOURCE
// for every AUTHENTICATED NON-MASTER caller of that route — not for the master key,
// which is matched and returned on before the resource is resolved, as described above.
// That is why "events" had to be added to BOTH api/middleware/scope.go and
// api/middleware/auth.go, and why registering the scope constant alone is not enough:
// the omission is invisible to the master key and total for everyone else.
//
// The key here is scoped TO these routes, so all three assertions below are available
// at once and each excludes a different failure:
//
//   - NOT AUTH_UNKNOWN_RESOURCE: the path prefix resolved to a resource.
//   - NOT AUTH_INSUFFICIENT_PERMISSIONS: it resolved to the RIGHT resource and the
//     method mapped to the right action, so a key granted exactly this scope was
//     admitted rather than refused.
//   - IS AUTH_MASTER_KEY_REQUIRED: the request traversed the whole middleware chain and
//     was refused by the ENDPOINT's own gate, which is the last thing that can refuse
//     it.
func TestEventsAPI_AuthorizationResourceIsRegistered(t *testing.T) {
	for _, route := range eventsAPIRoutes {
		t.Run(route.name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			touched := expectEventsAPIKeyLookup(ds, eventsTestAPIKey(route.scope))

			recorder := eventsKeyedRequest(t, router, route.method, route.path, eventsMockAPIKeySecret)

			// The middleware's background last-used update is joined BEFORE anything reads the
			// mock's call log below, so no goroutine is in flight while it is being inspected.
			requireEventsLastUsedTouch(t, touched)

			detail := eventsErrorDetail(t, recorder)

			require.NotEqual(t, apierror.ErrAuthUnknownResource, detail.Code,
				"%s %s RESOLVES TO NO AUTHORIZATION RESOURCE, so the middleware aborts every "+
					"request to it from every non-master principal. The path prefix is missing from "+
					"pathToResource in api/middleware/auth.go; adding the scope constant to "+
					"api/middleware/scope.go alone is not enough. THIS IS THE ASSERTION THIS "+
					"TEST EXISTS FOR: it is the only failure mode of the two-file registration "+
					"that has no other symptom", route.method, route.path)

			require.NotEqual(t, apierror.ErrAuthInsufficientPermissions, detail.Code,
				"a key granted exactly %q was refused on permissions, which means %s %s resolves "+
					"to a DIFFERENT resource or the method mapped to a different action; the "+
					"routes would then be authorised under some other feature's scope",
				route.scope, route.method, route.path)

			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)

			// And the endpoint's gate refused BEFORE any work: a fully authorised non-master
			// caller must learn only that the master key is required, never whether an event id
			// exists — which would turn the replay route into an existence oracle for anyone
			// holding an events-scoped key.
			assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
				"the gate must refuse before touching the repository; it reached %v",
				eventsRepositoryCallsExcludingAuth(ds))

			ds.AssertExpectations(t)
		})
	}
}

// TestEventsAPI_AuthorizationRefusesAnUnrelatedScopeInItsOwnName is the companion that
// pins the mapping to the RIGHT resource from the other direction.
//
// The test above proves an events-scoped key is admitted.
func TestEventsAPI_AuthorizationRefusesAnUnrelatedScopeInItsOwnName(t *testing.T) {
	for _, route := range eventsAPIRoutes {
		t.Run(route.name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			// Deliberately unrelated to this surface, and deliberately a real resource: an
			// unparseable scope would be refused by scope parsing rather than by the permission
			// check, which is a different code path.
			touched := expectEventsAPIKeyLookupWithoutTouch(ds, eventsTestAPIKey("ledgers:read"))

			recorder := eventsKeyedRequest(t, router, route.method, route.path, eventsMockAPIKeySecret)

			// A credential refused on scope must not be recorded as used, and this is where that
			// is decided. It doubles as the join for the call-log assertions below: after it
			// returns, nothing the middleware started for this request is still in flight.
			requireNoEventsLastUsedTouch(t, touched)

			detail := eventsErrorDetail(t, recorder)

			require.NotEqual(t, apierror.ErrAuthUnknownResource, detail.Code,
				"%s %s must resolve to an authorization resource", route.method, route.path)
			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthInsufficientPermissions)
			assert.Contains(t, detail.Message, route.scope,
				"the refusal must name %q: that is the resource this path maps to and the action "+
					"this method maps to, and both have to be right", route.scope)

			assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
				"a request refused by the middleware must never reach the repository")

			ds.AssertExpectations(t)
		})
	}
}

// TestEnsureEventManagementAuthorized_AdmitsTheMasterKeyAndRefusesEveryoneElse tests
// the gate in isolation, exactly as TestEnsureHookManagementAuthorized does for the
// hook surface it is modelled on.
//
// Testing it directly is worth doing on top of the routed tests because the gate's
// CONTRACT has two halves that a routed test only observes one of: it returns a boolean
// the handler branches on, AND it writes the refusal itself.
func TestEnsureEventManagementAuthorized_AdmitsTheMasterKeyAndRefusesEveryoneElse(t *testing.T) {
	t.Run("the master key is admitted and nothing is written", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Set("isMasterKey", true)

		assert.True(t, ensureEventManagementAuthorized(c),
			"the master key is the authorised principal for the event management surface")
		assert.Equal(t, http.StatusOK, recorder.Code,
			"an admitted request must have nothing written for it, or the handler's own "+
				"response is a second body")
		assert.Empty(t, recorder.Body.String(), "and no body either")
	})

	t.Run("an absent flag is refused and the refusal is written", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)

		assert.False(t, ensureEventManagementAuthorized(c))
		assert.Equal(t, http.StatusForbidden, recorder.Code)
		assert.Contains(t, recorder.Body.String(), errEventsRequireMasterKey.Error(),
			"the caller must be told what is required, and told it in the surface's own words")
		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
	})

	// The flag is read from a context value, so the three ways it can be present and still
	// not mean "master" are worth pinning: a false boolean, and a value of some other type
	// that a naive type-assertion would panic on or coerce to true.
	for name, value := range map[string]interface{}{
		"an explicit false":        false,
		"a string that says true":  "true",
		"a number":                 1,
		"a nil interface value":    nil,
		"a pointer to a true bool": new(bool),
	} {
		t.Run(name+" is not the master key", func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Set("isMasterKey", value)

			assert.False(t, ensureEventManagementAuthorized(c),
				"only a boolean true means the master key; anything else must refuse rather "+
					"than be coerced")
			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
		})
	}
}

// TestEventsAPI_IsOperableWithSecureModeOff is the reachability guard on the
// configuration the project SHIPS.
//
// docker-compose.yaml sets no BLNK_SERVER_SECURE, so a local stack runs with secure mode
// off — and on that configuration the auth middleware returns before comparing any
// credential, because authentication is disabled. Nothing then sets the "isMasterKey"
// context value, so the gate, which read only that value, refused every caller INCLUDING
// the master key: all three of these endpoints answered 403 AUTH_MASTER_KEY_REQUIRED with
// the correct key presented.
//
// That made two published procedures unexecutable rather than merely awkward. The
// dead-letter triage runbook and the daily outbox-versus-offset reconciliation in
// docs/kafka-operations.md are operated ENTIRELY through this surface, and an operator
// following either on a shipped local stack could not complete a single step of it.
//
// The refusal must survive for a caller that presents nothing, which is the other half of
// the same assertion: insecure mode opens the ordinary routes, and it must not open the
// privileged ones.
func TestEventsAPI_IsOperableWithSecureModeOff(t *testing.T) {
	insecure := func(cfg *config.Configuration) {
		cfg.Server.Secure = false
	}

	t.Run("the master key reaches the handler", func(t *testing.T) {
		router, ds := setupEventsRouter(t, insecure)
		// The DEFAULT reading's aggregate, which is the one an operator following either runbook
		// takes: no include_offsets, so the unresolved inventory is counted exactly and the
		// dispatched history is not scanned at all.
		ds.On("CountUnresolvedEventOutbox", mock.Anything).
			Return(eventsStatsUnresolvedCounts(), nil).Once()
		expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/stats", eventsMockMasterKey)

		require.Equal(t, http.StatusOK, recorder.Code,
			"THE FINDING: with secure mode off this answered 403 AUTH_MASTER_KEY_REQUIRED for the "+
				"very credential docs/kafka-operations.md tells an operator to use. body: %s",
			recorder.Body.String())

		// REACHED THE HANDLER, not merely passed the gate: the repository call is what proves the
		// request was served rather than short-circuited into an empty 200.
		ds.AssertCalled(t, "CountUnresolvedEventOutbox", mock.Anything)
		ds.AssertExpectations(t)
	})

	t.Run("and a caller with no credential is still refused", func(t *testing.T) {
		router, ds := setupEventsRouter(t, insecure)

		for _, route := range eventsAPIRoutes {
			t.Run(route.name, func(t *testing.T) {
				recorder := eventsKeyedRequest(t, router, route.method, route.path, "")

				assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
			})
		}

		assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
			"a refused request must never reach the repository, whatever secure mode is set to; "+
				"it reached %v", eventsRepositoryCallsExcludingAuth(ds))
	})

	t.Run("and a caller presenting the wrong key is still refused", func(t *testing.T) {
		router, _ := setupEventsRouter(t, insecure)

		// A key that is not the master key and is not an issued API key either. With secure mode
		// off the middleware does not look it up at all, so the gate is the only thing that can
		// refuse it — which is exactly the path the fallback comparison runs on.
		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/stats", eventsMockMasterKey+"-not-really")

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
	})
}

// eventsDeadLetterEntry builds one inventory entry with every field populated.
//
// Every field is set on purpose.
//
// Parameters:
//   - eventID string: the entry's event id.
//   - eventType string: the event name.
//   - occurredAt time.Time: the occurrence instant, which is the listing's ordering
//     key.
//
// Returns:
//   - coremodel.DeadLetterInventoryEntry: the populated entry.
func eventsDeadLetterEntry(
	eventID, eventType string,
	occurredAt time.Time,
) coremodel.DeadLetterInventoryEntry {
	first := occurredAt.Add(time.Second)
	last := occurredAt.Add(31 * time.Second)

	return coremodel.DeadLetterInventoryEntry{
		ID:               4242,
		EventID:          eventID,
		EventType:        eventType,
		AggregateID:      "txn_" + eventID,
		PartitionKey:     "partition_key_from_the_column",
		LedgerID:         "ldg_events_api_probe",
		Topic:            deadLetterFixtureTopic,
		SchemaVersion:    coremodel.SchemaVersionV1,
		OccurredAt:       occurredAt,
		Status:           coremodel.EventOutboxStatusDeadLettered,
		Attempts:         5,
		LastError:        "dial tcp 10.0.0.7:9092: connect: connection refused",
		FirstAttemptedAt: &first,
		LastAttemptedAt:  &last,
		DLTTopic:         deadLetterFixtureTopic + ".dlt",
		PayloadBytes:     512,
	}
}

// eventsReplayableRow builds a dead-lettered outbox row a replay claim can return.
//
// Parameters:
//   - eventID string: the row's event id.
//
// Returns:
//   - *coremodel.EventOutbox: the row, held in the claimed state the service expects.
func eventsReplayableRow(eventID string) *coremodel.EventOutbox {
	return &coremodel.EventOutbox{
		ID:            777,
		EventID:       eventID,
		EventType:     "transaction.applied",
		AggregateID:   "txn_" + eventID,
		PartitionKey:  "ldg_events_api_probe",
		LedgerID:      "ldg_events_api_probe",
		Topic:         deadLetterFixtureTopic,
		SchemaVersion: coremodel.SchemaVersionV1,
		Payload:       json.RawMessage(`{"event":"transaction.applied","data":{"status":"APPLIED"}}`),
		EventRaw:      []byte(`{"event_id":"` + eventID + `","event_type":"transaction.applied"}`),
		OccurredAt:    time.Now().Add(-time.Hour).UTC(),
		Status:        coremodel.EventOutboxStatusReplaying,
		Attempts:      5,
		MaxAttempts:   5,
		LastError:     "dial tcp 10.0.0.7:9092: connect: connection refused",
		ClaimToken:    "claim-token-events-api-probe",
		DLTTopic:      deadLetterFixtureTopic + ".dlt",
	}
}

// TestListDeadLetterEvents_RefusesANonMasterCallerBeforeAnyRepositoryWork proves the
// gate fires BEFORE the work rather than merely instead of the response.
//
// The construction matters and it is not the obvious one: the repository is programmed
// to SUCCEED.
func TestListDeadLetterEvents_RefusesANonMasterCallerBeforeAnyRepositoryWork(t *testing.T) {
	router, ds := setupEventsRouter(t, nil)
	touched := expectEventsAPIKeyLookup(ds, eventsTestAPIKey("events:read"))

	// Programmed to succeed, so a leaking gate produces a 200 and this test fails
	// for the right reason.
	ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
		Return(coremodel.DeadLetterInventoryPage{
			Entries: []coremodel.DeadLetterInventoryEntry{
				eventsDeadLetterEntry(uuid.NewString(), "transaction.applied", time.Now().UTC()),
			},
		}, nil).Maybe()

	recorder := eventsKeyedRequest(t, router,
		http.MethodGet, "/events/dead-letter", eventsMockAPIKeySecret)

	requireEventsLastUsedTouch(t, touched)

	assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
	ds.AssertNotCalled(t, "ListDeadLetterInventory", mock.Anything, mock.Anything)
	ds.AssertNotCalled(t, "ListAndCountDeadLetterInventory", mock.Anything, mock.Anything)
	assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
		"a gate that fires after the work has started is not a gate")
	assert.NotContains(t, recorder.Body.String(), deadLetterFixtureTopic,
		"no fragment of the inventory may appear in a refusal")
}

// TestReplayDeadLetterEvent_RefusesANonMasterCallerBeforeAnyRepositoryWork is the same
// property on the one WRITE this surface exposes, where the consequence is larger.
//
// Replay re-publishes a ledger event.
func TestReplayDeadLetterEvent_RefusesANonMasterCallerBeforeAnyRepositoryWork(t *testing.T) {
	eventID := uuid.NewString()

	router, ds := setupEventsRouter(t, nil)
	touched := expectEventsAPIKeyLookup(ds, eventsTestAPIKey("events:write"))

	ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
		Return(eventsReplayableRow(eventID), nil).Maybe()
	ds.On("ReleaseEventReplay", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	recorder := eventsKeyedRequest(t, router,
		http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockAPIKeySecret)

	requireEventsLastUsedTouch(t, touched)

	assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
	ds.AssertNotCalled(t, "ClaimEventForReplay", mock.Anything, mock.Anything, mock.Anything)
	ds.AssertNotCalled(t, "GetEventByID", mock.Anything, mock.Anything)
	assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
		"the row must not be claimed, and its existence must not be revealed, for a caller "+
			"that is not permitted to replay it")
}

// TestGetEventOutboxStats_RefusesANonMasterCallerBeforeAnyRepositoryWork completes the
// set.
//
// The statistics expose the whole outbox and, when a broker is configured, its offsets:
// the size and health of every event stream in the deployment.
func TestGetEventOutboxStats_RefusesANonMasterCallerBeforeAnyRepositoryWork(t *testing.T) {
	router, ds := setupEventsRouter(t, nil)
	touched := expectEventsAPIKeyLookup(ds, eventsTestAPIKey("events:read"))

	ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
		Return(map[string]int64{coremodel.EventOutboxStatusDispatched: 9_999_999}, nil).Maybe()
	ds.On("CountBalanceMonitorHandoffByStatus", mock.Anything).
		Return(map[string]int64{}, nil).Maybe()
	ds.On("CountUnfinalizedBulkTransactionBatches", mock.Anything, mock.Anything).
		Return(0, nil, nil).Maybe()

	recorder := eventsKeyedRequest(t, router,
		http.MethodGet, "/events/stats", eventsMockAPIKeySecret)

	requireEventsLastUsedTouch(t, touched)

	assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
	ds.AssertNotCalled(t, "CountEventOutboxByStatus", mock.Anything, mock.Anything)
	assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
		"the outbox must not be counted for a caller that may not read it")
	assert.NotContains(t, recorder.Body.String(), "9999999",
		"no count may leak into a refusal")
}

// TestEventsAPI_TypedCodesResolveToIntendedStatuses pins the error catalogue this
// surface's whole contract rests on.
//
// internal/apierror's statusByCode is the single source of truth for the default status
// of every code, and StatusForCode DEFAULTS AN UNKNOWN CODE TO 500.
func TestEventsAPI_TypedCodesResolveToIntendedStatuses(t *testing.T) {
	for name, expectation := range map[string]struct {
		code   apierror.ErrorCode
		status int
	}{
		"an unknown event is a not found":            {apierror.ErrEventNotFound, http.StatusNotFound},
		"an unreplayable event is a conflict":        {apierror.ErrEventNotDeadLettered, http.StatusConflict},
		"a failed replay is a server fault":          {apierror.ErrEventReplayFailed, http.StatusInternalServerError},
		"an unreachable broker is unavailable":       {apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable},
		"a missing master key is forbidden":          {apierror.ErrAuthMasterKeyRequired, http.StatusForbidden},
		"an unmapped route prefix is also forbidden": {apierror.ErrAuthUnknownResource, http.StatusForbidden},
		"a missing route parameter is a bad request": {apierror.ErrGenMissingParameter, http.StatusBadRequest},
		"an unusable parameter is a bad request":     {apierror.ErrGenValidation, http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, expectation.status, apierror.StatusForCode(expectation.code),
				"%s must be mapped in statusByCode; an unmapped code silently answers 500 and "+
					"the documented status never reaches a client", expectation.code)
		})
	}

	// AND THE TWO 403s MUST BE DISTINGUISHABLE ONLY BY CODE. This is the reason every
	// assertion in this file is written against error_detail.code: the status cannot tell
	// "the endpoint refused me" from "the route prefix is unmapped", and those are a
	// permissions answer and a deployment bug respectively.
	assert.Equal(t,
		apierror.StatusForCode(apierror.ErrAuthMasterKeyRequired),
		apierror.StatusForCode(apierror.ErrAuthUnknownResource),
		"these two share a status, which is why a status-only authorization assertion is "+
			"unfalsifiable on this surface")
	assert.NotEqual(t, apierror.ErrAuthMasterKeyRequired, apierror.ErrAuthUnknownResource,
		"and they must remain distinct codes, or the distinction is unobservable anywhere")
}

// eventsCapturedInventoryQuery returns the single inventory query the repository was
// handed, failing the test when it was handed none or more than one.
//
// Parameters:
//   - t *testing.T: the test, failed when the expectation is not met.
//   - ds *mocks.MockDataSource: the datasource the request ran against.
//   - method string: "ListDeadLetterInventory" or "ListAndCountDeadLetterInventory".
//
// Returns:
//   - coremodel.DeadLetterInventoryQuery: the query as the repository received it.
func eventsCapturedInventoryQuery(
	t *testing.T,
	ds *mocks.MockDataSource,
	method string,
) coremodel.DeadLetterInventoryQuery {
	t.Helper()

	var captured []coremodel.DeadLetterInventoryQuery
	for _, call := range ds.Calls {
		if call.Method != method {
			continue
		}

		query, ok := call.Arguments.Get(1).(coremodel.DeadLetterInventoryQuery)
		require.True(t, ok, "%s must be called with a model.DeadLetterInventoryQuery", method)
		captured = append(captured, query)
	}

	require.Len(t, captured, 1,
		"%s must be reached exactly once per request; it was reached %d times", method, len(captured))

	return captured[0]
}

// eventsEmptyInventoryPage is the repository answer for a healthy, empty inventory.
var eventsEmptyInventoryPage = coremodel.DeadLetterInventoryPage{
	Entries: []coremodel.DeadLetterInventoryEntry{},
}

// TestListDeadLetterEvents_HandsTheNormalisedPageAndEveryFilterToTheRepository is the
// assertion the live-datasource tests structurally cannot make.
//
// Over an inventory that matches nothing — which is the healthy state, and the state a
// shared test database is in for any synthetic filter — a bounded page and an unbounded
// one produce byte-identical responses.
//
// The two properties this pins are the ones with a real failure mode.
func TestListDeadLetterEvents_HandsTheNormalisedPageAndEveryFilterToTheRepository(t *testing.T) {
	windowFrom := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	windowTo := time.Date(2026, time.January, 2, 4, 4, 5, 0, time.UTC)

	for name, tt := range map[string]struct {
		query    string
		expected coremodel.DeadLetterInventoryQuery
	}{
		"no query at all takes the default page": {
			query:    "",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"a limit within the ceiling is honoured verbatim": {
			query:    "limit=50",
			expected: coremodel.DeadLetterInventoryQuery{Limit: 50},
		},
		"a limit exactly at the ceiling is honoured": {
			query:    fmt.Sprintf("limit=%d", deadLetterPageMaxLimit),
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageMaxLimit},
		},
		"one past the ceiling falls back to the default rather than to the ceiling": {
			query:    fmt.Sprintf("limit=%d", deadLetterPageMaxLimit+1),
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"an absurd limit cannot become a full-table read": {
			query:    "limit=1000000",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"a zero limit is normalised, not passed through as no limit": {
			query:    "limit=0",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"a negative limit is normalised": {
			query:    "limit=-5",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"an event type reaches SQL unchanged": {
			query: "event_type=transaction.applied",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:     deadLetterPageDefaultLimit,
				EventType: "transaction.applied",
			},
		},
		"surrounding whitespace on a filter is trimmed rather than matched": {
			query: "event_type=%20transaction.applied%20",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:     deadLetterPageDefaultLimit,
				EventType: "transaction.applied",
			},
		},
		"a category topic reaches SQL unchanged": {
			query: "topic=" + deadLetterFixtureTopic,
			expected: coremodel.DeadLetterInventoryQuery{
				Limit: deadLetterPageDefaultLimit,
				Topic: deadLetterFixtureTopic,
			},
		},
		"the dlt spelling is resolved to its category sibling before it reaches SQL": {
			// The repository filters on the ORIGINAL topic, so the ".dlt" suffix has to be
			// removed here or the filter matches nothing — an operator filtering by the
			// dlt_topic printed on every inventory item would receive an empty page and read it
			// as "nothing is stuck".
			query: "dlt_topic=" + deadLetterFixtureTopic + ".dlt",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit: deadLetterPageDefaultLimit,
				Topic: deadLetterFixtureTopic,
			},
		},
		"both spellings agreeing resolve to one filter": {
			query: "topic=" + deadLetterFixtureTopic + "&dlt_topic=" + deadLetterFixtureTopic + ".dlt",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit: deadLetterPageDefaultLimit,
				Topic: deadLetterFixtureTopic,
			},
		},
		"a status filter reaches SQL unchanged": {
			query: "status=" + coremodel.EventOutboxStatusDeadLettered,
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:  deadLetterPageDefaultLimit,
				Status: coremodel.EventOutboxStatusDeadLettered,
			},
		},
		"the more urgent failed status is filterable too": {
			query: "status=" + coremodel.EventOutboxStatusFailed,
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:  deadLetterPageDefaultLimit,
				Status: coremodel.EventOutboxStatusFailed,
			},
		},
		"an occurrence window reaches SQL as instants": {
			query: "occurred_from=2026-01-02T03:04:05Z&occurred_to=2026-01-02T04:04:05Z",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:        deadLetterPageDefaultLimit,
				OccurredFrom: windowFrom,
				OccurredTo:   windowTo,
			},
		},
		"a zone offset names the same instant as its Z spelling": {
			query: "occurred_from=2026-01-02T04:04:05%2B01:00",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:        deadLetterPageDefaultLimit,
				OccurredFrom: windowFrom,
			},
		},
		"a one-sided window leaves the other end unbounded": {
			query: "occurred_to=2026-01-02T04:04:05Z",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:      deadLetterPageDefaultLimit,
				OccurredTo: windowTo,
			},
		},
		"every narrowing at once arrives intact": {
			query: "limit=7&event_type=transaction.applied&dlt_topic=" + deadLetterFixtureTopic +
				".dlt&status=" + coremodel.EventOutboxStatusDeadLettered +
				"&occurred_from=2026-01-02T03:04:05Z&occurred_to=2026-01-02T04:04:05Z",
			expected: coremodel.DeadLetterInventoryQuery{
				Limit:        7,
				EventType:    "transaction.applied",
				Topic:        deadLetterFixtureTopic,
				Status:       coremodel.EventOutboxStatusDeadLettered,
				OccurredFrom: windowFrom,
				OccurredTo:   windowTo,
			},
		},
		"the inert sort options do not alter the query": {
			// sort_by and sort_order are accepted for client compatibility and the ordering is
			// fixed by the repository. Accepted-and-inert is the contract; what must not happen
			// is a sort option silently becoming a filter.
			query:    "sort_by=occurred_at&sort_order=asc",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
		"an unrecognised sort order is still inert rather than refused": {
			query:    "sort_order=sideways",
			expected: coremodel.DeadLetterInventoryQuery{Limit: deadLetterPageDefaultLimit},
		},
	} {
		t.Run(name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
				Return(eventsEmptyInventoryPage, nil).Once()

			target := "/events/dead-letter"
			if tt.query != "" {
				target += "?" + tt.query
			}

			recorder := eventsKeyedRequest(t, router, http.MethodGet, target, eventsMockMasterKey)
			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

			query := eventsCapturedInventoryQuery(t, ds, "ListDeadLetterInventory")

			assert.Equal(t, tt.expected.Limit, query.Limit,
				"NO PAGE MAY EXCEED THE CEILING and none may be unbounded: an uncapped limit "+
					"lets one request read the whole dead-letter inventory")
			assert.LessOrEqual(t, query.Limit, deadLetterPageMaxLimit)
			assert.Positive(t, query.Limit, "a page of zero rows answers nothing")

			assert.Equal(t, tt.expected.EventType, query.EventType)
			assert.Equal(t, tt.expected.Topic, query.Topic)
			assert.Equal(t, tt.expected.Status, query.Status)
			assert.True(t, tt.expected.OccurredFrom.Equal(query.OccurredFrom),
				"occurred_from must reach SQL as %s, not %s",
				tt.expected.OccurredFrom, query.OccurredFrom)
			assert.True(t, tt.expected.OccurredTo.Equal(query.OccurredTo),
				"occurred_to must reach SQL as %s, not %s",
				tt.expected.OccurredTo, query.OccurredTo)
			assert.Nil(t, query.Cursor, "no cursor was supplied, so the page starts at the newest entry")

			ds.AssertExpectations(t)
		})
	}
}

// TestListDeadLetterEvents_ResumesFromTheCursorItIssued closes the paging loop.
//
// A cursor is only useful if the token the response hands back is the token the next
// request is understood by.
func TestListDeadLetterEvents_ResumesFromTheCursorItIssued(t *testing.T) {
	position := coremodel.DeadLetterCursor{
		OccurredAt: time.Date(2026, time.March, 4, 5, 6, 7, 891011, time.UTC),
		ID:         4242,
	}

	var issued string

	t.Run("a page with more behind it hands back a cursor", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(coremodel.DeadLetterInventoryPage{
				Entries: []coremodel.DeadLetterInventoryEntry{
					eventsDeadLetterEntry(uuid.NewString(), "transaction.applied", position.OccurredAt),
				},
				NextCursor: &position,
				HasMore:    true,
			}, nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter", eventsMockMasterKey)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var envelope DeadLetterPageResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))

		require.NotEmpty(t, envelope.NextCursor,
			"a page with more behind it must say where to resume, or paging cannot continue")
		assert.True(t, envelope.HasMore,
			"has_more must mirror the cursor so a client can branch on a boolean")
		assert.Equal(t, position.Encode(), envelope.NextCursor,
			"the token must be the repository's position, encoded")

		issued = envelope.NextCursor
		ds.AssertExpectations(t)
	})

	t.Run("handing that cursor back resumes at the same coordinate", func(t *testing.T) {
		require.NotEmpty(t, issued, "the previous sub-test must have produced a cursor")

		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(eventsEmptyInventoryPage, nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter?cursor="+issued, eventsMockMasterKey)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		query := eventsCapturedInventoryQuery(t, ds, "ListDeadLetterInventory")
		require.NotNil(t, query.Cursor,
			"the cursor must reach the repository, or every page restarts at the newest entry "+
				"and a paging client never terminates")
		assert.True(t, position.OccurredAt.Equal(query.Cursor.OccurredAt),
			"the decoded instant must be the encoded one: %s", query.Cursor.OccurredAt)
		assert.Equal(t, position.ID, query.Cursor.ID,
			"and the tie-break id with it, or rows sharing an instant repeat or vanish")

		ds.AssertExpectations(t)
	})

	t.Run("the last page omits the cursor entirely", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(eventsEmptyInventoryPage, nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter", eventsMockMasterKey)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))
		assert.NotContains(t, raw, "next_cursor",
			"the termination condition is an ABSENT cursor; an empty-string token sends a "+
				"client that branches on presence around again forever")
		ds.AssertExpectations(t)
	})
}

// TestListDeadLetterEvents_TakesThePageAndTheTotalFromOneReadOnlyWhenATotalIsAskedFor
// pins the two-path read contract.
//
// A client comparing the page against the total does not terminate on that.
func TestListDeadLetterEvents_TakesThePageAndTheTotalFromOneReadOnlyWhenATotalIsAskedFor(t *testing.T) {
	t.Run("without a total there is one plain read and no total_count field", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(eventsEmptyInventoryPage, nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter", eventsMockMasterKey)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		ds.AssertNotCalled(t, "ListAndCountDeadLetterInventory", mock.Anything, mock.Anything)

		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))
		assert.Contains(t, raw, "data", "the envelope is the shape with or without a total")
		assert.Contains(t, raw, "has_more")
		assert.NotContains(t, raw, "total_count",
			"a total that was not asked for must be ABSENT rather than zero: a client cannot "+
				"tell a measured zero from an unmeasured one otherwise")

		ds.AssertExpectations(t)
	})

	t.Run("with a total there is one combined read carrying the page's own narrowing", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("ListAndCountDeadLetterInventory", mock.Anything, mock.Anything).
			Return(coremodel.DeadLetterInventoryPage{
				Entries: []coremodel.DeadLetterInventoryEntry{
					eventsDeadLetterEntry(uuid.NewString(), "transaction.applied", time.Now().UTC()),
				},
			}, int64(17), nil).Once()

		recorder := eventsKeyedRequest(t, router, http.MethodGet,
			"/events/dead-letter?include_count=true&event_type=transaction.applied&topic="+
				deadLetterFixtureTopic, eventsMockMasterKey)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		ds.AssertNotCalled(t, "ListDeadLetterInventory", mock.Anything, mock.Anything)

		// The COUNT must be taken with the page's own filters. A total derived from a
		// whole-table aggregate would describe a different set from the page beside it, which
		// is what the combined read prevents.
		query := eventsCapturedInventoryQuery(t, ds, "ListAndCountDeadLetterInventory")
		assert.Equal(t, "transaction.applied", query.EventType,
			"the count and the page must share one narrowing")
		assert.Equal(t, deadLetterFixtureTopic, query.Topic)

		var envelope DeadLetterPageResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
		require.NotNil(t, envelope.TotalCount, "include_count must produce a total")
		assert.Equal(t, int64(17), *envelope.TotalCount,
			"the total reported must be the total measured, not the size of the page")
		assert.Len(t, envelope.Data, 1)

		ds.AssertExpectations(t)
	})

	// include_count is parsed with strconv.ParseBool rather than by comparing against
	// "true", because a misspelling that silently changes the response SHAPE is worse than
	// one that is refused: a caller that asked for a total and received a bare envelope
	// cannot tell its spelling was ignored from a deployment that cannot count.
	for _, spelling := range []string{"true", "TRUE", "True", "1", "t"} {
		t.Run("include_count="+spelling+" is honoured", func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			ds.On("ListAndCountDeadLetterInventory", mock.Anything, mock.Anything).
				Return(eventsEmptyInventoryPage, int64(0), nil).Once()

			recorder := eventsKeyedRequest(t, router, http.MethodGet,
				"/events/dead-letter?include_count="+spelling, eventsMockMasterKey)
			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

			var envelope DeadLetterPageResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
			require.NotNil(t, envelope.TotalCount,
				"%q is one of strconv.ParseBool's true spellings and must not be dropped", spelling)
			assert.Equal(t, int64(0), *envelope.TotalCount,
				"a measured zero must render, which is why the field is a pointer")

			ds.AssertExpectations(t)
		})
	}

	for _, spelling := range []string{"yes", "on", "maybe", "2"} {
		t.Run("include_count="+spelling+" is refused rather than read as false", func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)

			recorder := eventsKeyedRequest(t, router, http.MethodGet,
				"/events/dead-letter?include_count="+spelling, eventsMockMasterKey)

			assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
			assert.Contains(t, recorder.Body.String(), spelling,
				"the refusal must name the value, or the caller is guessing again")
			assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
				"an unusable parameter must be refused before the read")
		})
	}
}

// TestListDeadLetterEvents_ProjectsTheWholeFailureRecordOntoTheResponse pins the triage
// payload.
//
// The raw driver text is asserted ABSENT, and that is not incidental tidiness.
func TestListDeadLetterEvents_ProjectsTheWholeFailureRecordOntoTheResponse(t *testing.T) {
	occurredAt := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	eventID := uuid.NewString()
	entry := eventsDeadLetterEntry(eventID, "transaction.applied", occurredAt)

	router, ds := setupEventsRouter(t, nil)
	ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
		Return(coremodel.DeadLetterInventoryPage{
			Entries: []coremodel.DeadLetterInventoryEntry{entry},
		}, nil).Once()

	recorder := eventsKeyedRequest(t, router,
		http.MethodGet, "/events/dead-letter", eventsMockMasterKey)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var envelope DeadLetterPageResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Len(t, envelope.Data, 1)

	item := envelope.Data[0]
	assert.Equal(t, eventID, item.EventID, "the id a replay is addressed by")
	assert.Equal(t, "transaction.applied", item.EventType)
	assert.Equal(t, entry.AggregateID, item.AggregateID)
	assert.Equal(t, entry.LedgerID, item.LedgerID)
	assert.Equal(t, coremodel.SchemaVersionV1, item.SchemaVersion)
	assert.Equal(t, deadLetterFixtureTopic, item.Topic,
		"the ORIGINAL category topic, which is where a replay sends the event back to")
	assert.Equal(t, deadLetterFixtureTopic+".dlt", item.DLTTopic,
		"and the dlt sibling it was actually preserved on")
	assert.Equal(t, coremodel.EventOutboxStatusDeadLettered, item.Status)
	assert.Equal(t, 5, item.Attempts)
	assert.Equal(t, entry.PayloadBytes, item.PayloadBytes,
		"the SIZE of the stored body, so an inventory of large events costs a page of "+
			"metadata rather than a page of event bodies")
	assert.True(t, occurredAt.Equal(item.OccurredAt))

	// THE EFFECTIVE MESSAGE KEY, not the stored column. The publish path prefers the
	// ledger id whenever there is one, so on a row whose partition_key was derived before
	// the ledger was known the two differ — and that is precisely the row an operator
	// investigating an ordering question is looking at.
	assert.Equal(t, entry.LedgerID, item.PartitionKey,
		"an entry carrying a ledger was keyed on the ledger; reporting the stored column "+
			"would name a key the event was not routed by")

	require.NotNil(t, item.FirstAttemptedAt)
	require.NotNil(t, item.LastAttemptedAt)
	assert.True(t, entry.FirstAttemptedAt.Equal(*item.FirstAttemptedAt))
	assert.True(t, entry.LastAttemptedAt.Equal(*item.LastAttemptedAt))
	assert.True(t, item.LastAttemptedAt.After(*item.FirstAttemptedAt),
		"the pair bounds the window the failure persisted over, which is what separates a "+
			"momentary broker blip from a sustained outage")

	assert.Equal(t, model.FailureReasonBrokerUnavailable, item.FailureReason,
		"a connection refusal is a broker problem and must be classified as one")

	body := recorder.Body.String()
	assert.NotContains(t, body, "10.0.0.7",
		"the broker address must never reach a response body")
	assert.NotContains(t, body, entry.LastError,
		"the raw driver text stays in the outbox row and the runbook reads it through the "+
			"database; the wire carries the classified reason")

	ds.AssertExpectations(t)
}

// TestListDeadLetterEvents_BackfillsTheItemFromTheStoredFailureMetadata covers the
// second source of the same five facts.
//
// Without this the fields would render as zero and nil on exactly the rows that have
// been stuck longest.
func TestListDeadLetterEvents_BackfillsTheItemFromTheStoredFailureMetadata(t *testing.T) {
	occurredAt := time.Date(2026, time.April, 5, 6, 7, 8, 0, time.UTC)
	metadata := coremodel.FailureMetadata{
		OriginalTopic:    deadLetterFixtureTopic,
		ErrorReason:      "SASL authentication failed for the producer principal",
		AttemptCount:     4,
		FirstAttemptedAt: occurredAt.Add(2 * time.Second),
		LastAttemptedAt:  occurredAt.Add(45 * time.Second),
	}
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)

	eventID := uuid.NewString()
	// Deliberately BARE: no LastError, no attempt count, no attempt instants. Everything
	// asserted below therefore has to have come from the metadata record.
	entry := coremodel.DeadLetterInventoryEntry{
		ID:              99,
		EventID:         eventID,
		EventType:       "identity.created",
		AggregateID:     "idt_" + eventID,
		PartitionKey:    "idt_" + eventID,
		Topic:           deadLetterFixtureTopic,
		SchemaVersion:   coremodel.SchemaVersionV1,
		OccurredAt:      occurredAt,
		Status:          coremodel.EventOutboxStatusDeadLettered,
		DLTTopic:        deadLetterFixtureTopic + ".dlt",
		FailureMetadata: encoded,
	}

	router, ds := setupEventsRouter(t, nil)
	ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
		Return(coremodel.DeadLetterInventoryPage{
			Entries: []coremodel.DeadLetterInventoryEntry{entry},
		}, nil).Once()

	recorder := eventsKeyedRequest(t, router,
		http.MethodGet, "/events/dead-letter", eventsMockMasterKey)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var envelope DeadLetterPageResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Len(t, envelope.Data, 1)

	item := envelope.Data[0]
	assert.Equal(t, metadata.AttemptCount, item.Attempts,
		"attempt_count must be read from the failure record when the column is unset")
	assert.Equal(t, model.FailureReasonAuthorizationDenied, item.FailureReason,
		"error_reason must be classified from the failure record, and a SASL refusal is a "+
			"grant problem rather than a broker outage")
	require.NotNil(t, item.FirstAttemptedAt)
	require.NotNil(t, item.LastAttemptedAt)
	assert.True(t, metadata.FirstAttemptedAt.Equal(*item.FirstAttemptedAt))
	assert.True(t, metadata.LastAttemptedAt.Equal(*item.LastAttemptedAt))

	// An entry with NO ledger falls back to the stored partition key, which is the other
	// half of the effective-key rule.
	assert.Equal(t, entry.PartitionKey, item.PartitionKey)
	assert.Empty(t, item.LedgerID,
		"a ledger-less event omits ledger_id rather than reporting an empty string")

	assert.NotContains(t, recorder.Body.String(), metadata.ErrorReason,
		"the raw reason is classified before it reaches the wire, here as elsewhere")

	ds.AssertExpectations(t)
}

// TestListDeadLetterEvents_AnswersAnEmptyPageRatherThanANotFound pins the healthy case.
//
// "No events are stuck" is a successful answer, and it is the answer a reconciliation
// script sees every day it runs against a working deployment.
func TestListDeadLetterEvents_AnswersAnEmptyPageRatherThanANotFound(t *testing.T) {
	for name, page := range map[string]coremodel.DeadLetterInventoryPage{
		"an empty slice from the repository": eventsEmptyInventoryPage,
		// A nil slice is the other way a repository can say "nothing": the service normalises
		// it defensively and the handler allocates with make, so both must render as [].
		"a nil slice from the repository": {Entries: nil},
	} {
		t.Run(name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).Return(page, nil).Once()

			recorder := eventsKeyedRequest(t, router,
				http.MethodGet, "/events/dead-letter", eventsMockMasterKey)

			require.Equal(t, http.StatusOK, recorder.Code,
				"an empty inventory is a 200, never a 404: it is the daily healthy answer. "+
					"body: %s", recorder.Body.String())
			assert.JSONEq(t, `{"data":[],"has_more":false}`, recorder.Body.String(),
				"data must be [] and never null, or a script that ranges over it breaks in "+
					"exactly the case where nothing is wrong")

			ds.AssertExpectations(t)
		})
	}
}

// TestListDeadLetterEvents_AnswersATypedCodeWithoutLeakingTheRepositoryError covers the
// failure the live-datasource harness cannot stage.
//
// Two behaviours are asserted, and they are the two halves of the error convention.
//
//   - An UNCLASSIFIED repository failure answers GEN_INTERNAL with the SANITIZED
//     message, never the driver's own text.
//   - A TYPED error is answered with ITS OWN code rather than with the handler's
//     default, which is what makes the service's vocabulary reach the client at all.
func TestListDeadLetterEvents_AnswersATypedCodeWithoutLeakingTheRepositoryError(t *testing.T) {
	t.Run("an unclassified repository failure is sanitized", func(t *testing.T) {
		driverText := `pq: relation "blnk.event_outbox" does not exist (SQLSTATE 42P01) in ` +
			`parse_relation.c:1381 routine=parserOpenTable`

		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(coremodel.DeadLetterInventoryPage{}, errors.New(driverText)).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusInternalServerError, apierror.ErrGenInternal)

		detail := eventsErrorDetail(t, recorder)
		assert.Equal(t, sanitizedInternalMessage, detail.Message,
			"an unclassified 5xx carries the fixed sanitized message")

		body := recorder.Body.String()
		for _, leak := range []string{"pq:", "SQLSTATE", "parse_relation.c", "parserOpenTable", "blnk.event_outbox"} {
			assert.NotContains(t, body, leak,
				"the driver's own text must never reach a client: %q leaked", leak)
		}

		ds.AssertExpectations(t)
	})

	t.Run("a typed repository failure keeps its own code", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("ListDeadLetterInventory", mock.Anything, mock.Anything).
			Return(coremodel.DeadLetterInventoryPage{}, apierror.NewAPIError(
				apierror.ErrGenResourceLocked,
				"The dead-letter inventory is being maintained",
				errors.New("blnk: maintenance lock held"),
			)).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/dead-letter", eventsMockMasterKey)

		assertErrorCode(t, recorder,
			apierror.StatusForCode(apierror.ErrGenResourceLocked), apierror.ErrGenResourceLocked)
		assert.NotEqual(t, http.StatusInternalServerError, recorder.Code,
			"a typed error must not be flattened into the handler's default")

		ds.AssertExpectations(t)
	})
}

// TestReplayDeadLetterEvent_AnswersTheTypedCodeForEveryFailureMode is the replay error
// matrix, and every row of it is unreachable without the datasource seam.
//
// Replay is the one write on this surface and its documented responses are five
// distinct codes across four statuses.
func TestReplayDeadLetterEvent_AnswersTheTypedCodeForEveryFailureMode(t *testing.T) {
	t.Run("a repository miss is a not found", func(t *testing.T) {
		eventID := uuid.NewString()
		router, ds := setupEventsRouter(t, nil)
		ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
			Return(nil, sql.ErrNoRows).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusNotFound, apierror.ErrEventNotFound)
		ds.AssertNotCalled(t, "ReleaseEventReplay",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		ds.AssertExpectations(t)
	})

	t.Run("a typed not-found from the repository keeps the event code", func(t *testing.T) {
		eventID := uuid.NewString()
		router, ds := setupEventsRouter(t, nil)
		ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
			Return(nil, apierror.NewAPIError(
				apierror.ErrEventNotFound,
				"No event with that id exists",
				errors.New("blnk: no such row"),
			)).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusNotFound, apierror.ErrEventNotFound)
		ds.AssertExpectations(t)
	})

	// THE THREE CONFLICTS. All three answer 409 EVENT_NOT_DEAD_LETTERED, and the code
	// alone would be satisfied by one message for all of them — but "you already replayed
	// this", "somebody else is replaying it right now" and "this event was never
	// replayable" call for three different actions, so the MESSAGE is asserted too.
	for name, tt := range map[string]struct {
		row     *coremodel.EventOutbox
		expects string
	}{
		"an event that has already been replayed": {
			row: &coremodel.EventOutbox{
				ID:       31,
				EventID:  "placeholder",
				Status:   coremodel.EventOutboxStatusDispatched,
				DLTTopic: deadLetterFixtureTopic + ".dlt",
			},
			expects: "already been replayed",
		},
		"an event a concurrent replay is holding": {
			row: &coremodel.EventOutbox{
				ID:      32,
				EventID: "placeholder",
				Status:  coremodel.EventOutboxStatusReplaying,
			},
			expects: "already being replayed",
		},
		"an event still working through ordinary delivery": {
			row: &coremodel.EventOutbox{
				ID:      33,
				EventID: "placeholder",
				Status:  coremodel.EventOutboxStatusPending,
			},
			expects: coremodel.EventOutboxStatusPending,
		},
	} {
		t.Run(name+" is a conflict that says why", func(t *testing.T) {
			eventID := uuid.NewString()
			row := *tt.row
			row.EventID = eventID

			router, ds := setupEventsRouter(t, nil)
			ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
				Return(nil, apierror.NewAPIError(
					apierror.ErrGenConflict,
					"The event is not in a replayable state",
					errors.New("blnk: claim precondition failed"),
				)).Once()
			// The explanatory read runs on the failure path only, and it is what turns
			// "the precondition failed" into the operator's actual situation.
			ds.On("GetEventByID", mock.Anything, eventID).Return(&row, nil).Once()

			recorder := eventsKeyedRequest(t, router,
				http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

			assertErrorCode(t, recorder, http.StatusConflict, apierror.ErrEventNotDeadLettered)
			assert.Contains(t, eventsErrorDetail(t, recorder).Message, tt.expects,
				"the refusal must name the situation; %q and the other two conflicts call for "+
					"different actions", tt.expects)
			ds.AssertExpectations(t)
		})
	}

	t.Run("an unclassified failure is a failed replay rather than a generic fault", func(t *testing.T) {
		eventID := uuid.NewString()
		router, ds := setupEventsRouter(t, nil)
		ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
			Return(nil, errors.New("blnk: the replay claim could not be recorded")).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

		// EVENT_REPLAY_FAILED, not GEN_INTERNAL: the handler's default is the endpoint's
		// own code, so an operator reading the response knows which operation failed.
		assertErrorCode(t, recorder, http.StatusInternalServerError, apierror.ErrEventReplayFailed)
		ds.AssertExpectations(t)
	})

	t.Run("no configured broker is unavailable, and the claim is released", func(t *testing.T) {
		// This is the row the whole harness is arranged for. The claim SUCCEEDS, so the
		// service holds the row in `replaying`, and the publisher is the no-op implementation
		// because no brokers are configured — which is a legitimate steady state rather than
		// a misconfiguration.
		eventID := uuid.NewString()
		row := eventsReplayableRow(eventID)

		router, ds := setupEventsRouter(t, nil)
		ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).Return(row, nil).Once()
		ds.On("ReleaseEventReplay", mock.Anything, row.ID, row.ClaimToken, mock.Anything).
			Return(nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusServiceUnavailable, apierror.ErrKafkaUnavailable)
		assert.Equal(t, string(apierror.ErrKafkaUnavailable), "EVENT_KAFKA_UNAVAILABLE",
			"the code carries the EVENT_ prefix deliberately, so the whole event surface reads "+
				"as one family in a client's error handling")

		// RELEASED WITH ITS FENCING TOKEN. The token is what makes the release safe against a
		// concurrent claim, so releasing without it — or with the wrong one — is not a
		// release at all.
		ds.AssertCalled(t, "ReleaseEventReplay", mock.Anything, row.ID, row.ClaimToken, mock.Anything)
		ds.AssertExpectations(t)
	})
}

// TestReplayDeadLetterEvent_DoesNotFallThroughToTheGenericNotFound is the guard against
// api/errors.go's broad catch-all, and it is the test that forces the handler to
// declare its upgrades.
func TestReplayDeadLetterEvent_DoesNotFallThroughToTheGenericNotFound(t *testing.T) {
	for name, cause := range map[string]error{
		"a bare not-found message":       errors.New("event outbox row not found"),
		"a message naming the id":        errors.New(`blnk: event "evt_probe" not found in the outbox`),
		"a wrapped driver miss":          fmt.Errorf("querying the outbox: %w", sql.ErrNoRows),
		"a repository phrasing with sql": errors.New("no rows in result set: outbox row not found"),
	} {
		t.Run(name+" answers EVENT_NOT_FOUND", func(t *testing.T) {
			eventID := uuid.NewString()
			router, ds := setupEventsRouter(t, nil)
			ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
				Return(nil, cause).Once()

			recorder := eventsKeyedRequest(t, router,
				http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

			detail := eventsErrorDetail(t, recorder)
			require.NotEqual(t, apierror.ErrGenNotFound, detail.Code,
				"GEN_NOT_FOUND is the catch-all at the END of classifyMessage's table; this "+
					"endpoint must answer in its own vocabulary. The handler needs "+
					"withUpgrade(ErrGenNotFound, ErrEventNotFound) — a bare respondError is not "+
					"enough. body: %s", recorder.Body.String())
			assertErrorCode(t, recorder, http.StatusNotFound, apierror.ErrEventNotFound)

			ds.AssertExpectations(t)
		})
	}

	t.Run("and a conflict phrased in prose is upgraded the same way", func(t *testing.T) {
		// The companion upgrade. Nothing in classifyMessage's table maps this text, so
		// without withUpgrade(ErrGenConflict, ErrEventNotDeadLettered) the typed conflict the
		// service raises would be the only source of a 409 — and a repository that reported
		// the precondition failure with the legacy generic code would answer 500.
		eventID := uuid.NewString()
		row := coremodel.EventOutbox{
			ID:      44,
			EventID: eventID,
			Status:  coremodel.EventOutboxStatusReplaying,
		}

		router, ds := setupEventsRouter(t, nil)
		ds.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).
			Return(nil, apierror.NewAPIError(
				apierror.ErrGenConflict, "precondition failed", errors.New("blnk: not claimable"),
			)).Once()
		ds.On("GetEventByID", mock.Anything, eventID).Return(&row, nil).Once()

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)

		detail := eventsErrorDetail(t, recorder)
		assert.NotEqual(t, apierror.ErrGenConflict, detail.Code,
			"the generic conflict must be upgraded to the endpoint's own code")
		assertErrorCode(t, recorder, http.StatusConflict, apierror.ErrEventNotDeadLettered)

		ds.AssertExpectations(t)
	})
}

// TestReplayDeadLetterEvent_AddressesTheServiceByEventIDAlone is the byte-fidelity
// obligation as it is observable at THIS layer.
//
// A replayed event must be byte-for-byte identical to the original aside from its
// failure metadata, and the only way to keep that promise is for the re-publish to send
// the STORED BYTES. A handler that unmarshalled the event, filled a struct and
// re-marshalled it would reorder JSON keys and break the guarantee while every
// field-by-field assertion still passed.
func TestReplayDeadLetterEvent_AddressesTheServiceByEventIDAlone(t *testing.T) {
	eventID := uuid.NewString()
	row := eventsReplayableRow(eventID)

	router, ds := setupEventsRouter(t, nil)
	ds.On("ClaimEventForReplay", mock.Anything, mock.Anything, mock.Anything).Return(row, nil).Once()
	ds.On("ReleaseEventReplay", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// A body is deliberately sent. If the handler read one — a topic override, a payload
	// patch, a "replay as" envelope — the replay would no longer be byte-faithful, and the
	// route would have a second, undocumented input.
	request := httptest.NewRequest(http.MethodPost,
		"/events/dead-letter/"+eventID+"/replay",
		jsonReader(`{"topic":"blnk.attacker","payload":{"event":"transaction.applied","data":{}}}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Blnk-Key", eventsMockMasterKey)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	// The claim is addressed by the event id and nothing else: the arguments are the
	// context, the id and the lease. No payload, no topic, no envelope.
	var claims []mock.Arguments
	for _, call := range ds.Calls {
		if call.Method == "ClaimEventForReplay" {
			claims = append(claims, call.Arguments)
		}
	}
	require.Len(t, claims, 1, "the replay must look the event up exactly once")
	require.Len(t, claims[0], 3,
		"ClaimEventForReplay takes a context, an event id and a lease — nothing else may be "+
			"threaded through it from the request")
	assert.Equal(t, eventID, claims[0].Get(1),
		"the identifier the service receives must be the one in the route, untransformed")
	assert.IsType(t, time.Duration(0), claims[0].Get(2),
		"and the third argument is the claim lease, which the handler does not choose")

	// The destination came from the STORED ROW, never from the request body.
	assert.NotContains(t, recorder.Body.String(), "blnk.attacker",
		"a topic supplied in the body must be ignored: the replay's destination is the "+
			"original topic recorded on the row")

	ds.AssertExpectations(t)
}

// TestReplayDeadLetterEvent_RequiresAnEventIDInTheRoute pins the parameter guard, and
// it pins the CODE rather than merely a non-200.
//
// GEN_MISSING_PARAMETER is a 400 that tells an operator the request was malformed.
func TestReplayDeadLetterEvent_RequiresAnEventIDInTheRoute(t *testing.T) {
	t.Run("a whitespace-only identifier is a missing parameter", func(t *testing.T) {
		// The route DOES match this — " " is a legal path segment — so it reaches the handler
		// and is refused there. Trimming to empty is the same rule every other parameter in
		// this package follows.
		router, ds := setupEventsRouter(t, nil)

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/%20/replay", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMissingParameter)
		assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
			"a malformed route must be refused before the lookup")
	})

	t.Run("no parameter at all is a missing parameter", func(t *testing.T) {
		// Reached by calling the handler directly, because gin cannot route a path with the
		// segment absent — and the guard has to hold for a caller that reaches the handler by
		// any means, including a future route registration that forgets the parameter.
		apiInstance, ds := newEventsAPIOverMockDatasource(t, nil)

		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Set("isMasterKey", true)
		c.Request = httptest.NewRequest(http.MethodPost, "/events/dead-letter//replay", nil)

		apiInstance.ReplayDeadLetterEvent(c)

		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMissingParameter)
		assert.Contains(t, recorder.Body.String(), "event_id",
			"the refusal must name the parameter that is missing")
		assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds))
	})

	t.Run("a malformed identifier is a validation error and not a missing one", func(t *testing.T) {
		// The two are different mistakes and the distinction is useful: "you did not send
		// an id" and "what you sent cannot be an id" send an operator to different places.
		router, ds := setupEventsRouter(t, nil)

		recorder := eventsKeyedRequest(t, router,
			http.MethodPost, "/events/dead-letter/NOT-A-UUID/replay", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
		assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
			"an identifier that cannot be an event id must not reach the lookup: an "+
				"uppercased spelling of a REAL event would otherwise miss the row and be "+
				"reported as a nonexistent event")
	})
}

// TestReplayEventResponse_IsTheShapeASuccessfulReplayReturns pins the success DTO at
// the type level.
func TestReplayEventResponse_IsTheShapeASuccessfulReplayReturns(t *testing.T) {
	replayedAt := time.Date(2026, time.May, 6, 7, 8, 9, 0, time.UTC)
	eventID := uuid.NewString()

	body, err := json.Marshal(model.ReplayEventResponse{
		EventID:    eventID,
		Topic:      deadLetterFixtureTopic,
		Status:     string(coremodel.PublishStatusDispatched),
		ReplayedAt: replayedAt,
	})
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(body, &decoded))

	assert.Equal(t, eventID, decoded["event_id"],
		"the id is UNCHANGED by a replay, which is what lets a subscriber deduplicating on "+
			"event_id absorb the copy")
	assert.Equal(t, deadLetterFixtureTopic, decoded["topic"],
		"the ORIGINAL category topic the event went back to, never the dead-letter topic it "+
			"was listed from")
	assert.Equal(t, string(coremodel.PublishStatusDispatched), decoded["status"])
	assert.Equal(t, replayedAt.Format(time.RFC3339), decoded["replayed_at"],
		"instants on this surface are RFC3339, matching the envelope's occurred_at")

	// The dead-letter topic must not be what a client is told to reason about, and the
	// failure metadata has no place on a success.
	assert.NotContains(t, string(body), ".dlt")
	assert.NotContains(t, string(body), "failure")

	// Status is the only optional field: a handler with nothing more specific to add than
	// the 200 itself omits it rather than reporting an empty string.
	withoutStatus, err := json.Marshal(model.ReplayEventResponse{
		EventID:    eventID,
		Topic:      deadLetterFixtureTopic,
		ReplayedAt: replayedAt,
	})
	require.NoError(t, err)
	assert.NotContains(t, string(withoutStatus), "status")
}

// eventsStatsCounts is a per-status census with a DISTINCT value for every status.
//
// Distinct on purpose: with two statuses sharing a number, a projection that reads the
// wrong key still produces the right answer and the test passes on a bug.
var eventsStatsCounts = map[string]int64{
	coremodel.EventOutboxStatusPending:        11,
	coremodel.EventOutboxStatusProcessing:     22,
	coremodel.EventOutboxStatusWebhookPending: 33,
	coremodel.EventOutboxStatusDispatched:     44,
	coremodel.EventOutboxStatusFailed:         55,
	coremodel.EventOutboxStatusDeadLettered:   66,
	coremodel.EventOutboxStatusReplaying:      77,
}

// eventsStatsUnresolvedCounts is eventsStatsCounts with the DISPATCHED key removed,
// which is what the unresolved aggregate actually returns.
func eventsStatsUnresolvedCounts() map[string]int64 {
	unresolved := make(map[string]int64, len(eventsStatsCounts))
	for status, count := range eventsStatsCounts {
		if status == coremodel.EventOutboxStatusDispatched {
			continue
		}
		unresolved[status] = count
	}

	return unresolved
}

// expectEventsStatsCensus programs the two producer-atomicity reads the statistics
// take.
//
// Parameters:
//   - ds *mocks.MockDataSource: the datasource to program.
//   - handoffs map[string]int64: the balance-monitor handoff census.
//   - batches int: how many bulk batches have not reported an outcome.
//   - oldest *time.Time: when the oldest of those began, or nil.
func expectEventsStatsCensus(
	ds *mocks.MockDataSource,
	handoffs map[string]int64,
	batches int,
	oldest *time.Time,
) {
	ds.On("CountBalanceMonitorHandoffByStatus", mock.Anything).Return(handoffs, nil).Once()
	ds.On("CountUnfinalizedBulkTransactionBatches", mock.Anything, mock.Anything).
		Return(batches, oldest, nil).Once()
}

// eventsStatsResponse issues a statistics request and decodes the DTO.
//
// Parameters:
//   - t *testing.T: the test, failed on a non-200 or an undecodable body.
//   - router *gin.Engine: the router under test.
//   - query string: the query string WITHOUT the leading "?", or "" for none.
//
// Returns:
//   - model.EventOutboxStatsResponse: the decoded statistics.
//   - map[string]json.RawMessage: the raw body, for asserting a field is ABSENT rather
//     than zero — which the typed DTO cannot distinguish.
func eventsStatsResponse(
	t *testing.T,
	router *gin.Engine,
	query string,
) (model.EventOutboxStatsResponse, map[string]json.RawMessage) {
	t.Helper()

	target := "/events/stats"
	if query != "" {
		target += "?" + query
	}

	recorder := eventsKeyedRequest(t, router, http.MethodGet, target, eventsMockMasterKey)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var statistics model.EventOutboxStatsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &statistics),
		"body: %s", recorder.Body.String())

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &raw))

	return statistics, raw
}

// TestGetEventOutboxStats_ReportsThePerStatusCountsTheReconciliationIsBuiltOn is the
// outbox half of the acceptance criterion.
//
// The daily zero-loss reconciliation the operations runbook describes compares these
// counts against the broker's own records.
func TestGetEventOutboxStats_ReportsThePerStatusCountsTheReconciliationIsBuiltOn(t *testing.T) {
	oldestBatch := time.Date(2026, time.June, 7, 8, 9, 10, 0, time.UTC)

	router, ds := setupEventsRouter(t, nil)
	ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
		Return(eventsStatsCounts, nil).Once()
	expectEventsStatsCensus(ds, map[string]int64{
		coremodel.OutboxStatusPending:    3,
		coremodel.OutboxStatusProcessing: 4,
		coremodel.OutboxStatusCompleted:  5,
		coremodel.OutboxStatusFailed:     6,
	}, 7, &oldestBatch)

	// best_effort, because this test is about the DISPATCHED count as well as the others,
	// and counting the dispatched population is the deliberate reading now. It is the
	// posture rather than `true` so that the absent broker degrades to 200 with the counts
	// instead of the 503 a required posture owes.
	statistics, raw := eventsStatsResponse(t, router, "include_offsets=best_effort")

	assert.Equal(t, int64(11), statistics.Pending)
	assert.Equal(t, int64(22), statistics.Processing)
	assert.Equal(t, int64(33), statistics.WebhookPending,
		"the legacy leg's outstanding rows are reported separately; folding them into "+
			"dispatched would hide the dual-delivery backlog")
	require.NotNil(t, statistics.Dispatched,
		"a posture that reads the broker side must count the dispatched population, and must "+
			"emit it rather than omitting the key")
	assert.Equal(t, int64(44), *statistics.Dispatched)
	assert.True(t, statistics.DispatchedHistoryCounted,
		"and must say that it counted it, so an absent key elsewhere is unambiguous")
	assert.Equal(t, int64(55), statistics.Failed,
		"failed is the retry budget being spent, and the dead-letter write may still be owed")
	assert.Equal(t, int64(66), statistics.DeadLettered,
		"dead_lettered is the event having additionally reached its .dlt sibling")
	assert.Equal(t, int64(77), statistics.Replaying)

	// THE PRODUCER-ATOMICITY CENSUS. An outstanding intent is an event that is OWED, and
	// it is invisible to the per-status counts because its row does not exist yet — so
	// omitting it would make the reconciliation look complete while events were still
	// unwritten.
	require.NotNil(t, statistics.ProducerAtomicity,
		"the census must be reported when it can be read")
	assert.Equal(t, int64(3), statistics.ProducerAtomicity.MonitorHandoffPending)
	assert.Equal(t, int64(4), statistics.ProducerAtomicity.MonitorHandoffProcessing)
	assert.Equal(t, int64(5), statistics.ProducerAtomicity.MonitorHandoffCompleted)
	assert.Equal(t, int64(6), statistics.ProducerAtomicity.MonitorHandoffFailed)
	assert.Equal(t, int64(7), statistics.ProducerAtomicity.UnfinalizedBatches)
	require.NotNil(t, statistics.ProducerAtomicity.OldestUnfinalizedBatchAt)
	assert.True(t, oldestBatch.Equal(*statistics.ProducerAtomicity.OldestUnfinalizedBatchAt))

	assert.False(t, statistics.GeneratedAt.IsZero(),
		"the snapshot instant is what an operator correlates the counts against")

	// GRACEFUL DEGRADATION, ASSERTED EXPLICITLY. No broker is configured — the legitimate
	// steady state every deployment ran in before this pipeline existed — so the counts
	// are returned with 200, offsets_complete is false, and the offset keys are OMITTED
	// rather than emitted as nulls a reconciliation script would have to special-case.
	assert.False(t, statistics.OffsetsComplete,
		"with no broker there is no broker side, so the reconciliation is not complete")
	assert.NotContains(t, raw, "topic_end_offsets",
		"an unmeasured offset map must be absent, not null: a script cannot tell a measured "+
			"empty map from an unmeasured one otherwise")
	assert.NotContains(t, raw, "reconciliation",
		"and no verdict may be reported when only one side of it was measured")
	assert.NotContains(t, raw, "offsets_measured_at")
	assert.Nil(t, statistics.OffsetsMeasuredAt)
	assert.Empty(t, statistics.MissingTopics)

	ds.AssertExpectations(t)
}

// TestGetEventOutboxStats_MeasuresTheWindowTheCallerAsked pins the parameter that was
// once accepted and then silently discarded.
func TestGetEventOutboxStats_MeasuresTheWindowTheCallerAsked(t *testing.T) {
	for name, tt := range map[string]struct {
		query   string
		window  time.Duration
		seconds int64
	}{
		"no window takes the daily default the runbook uses": {"", 24 * time.Hour, 86400},
		"an incident-sized window is honoured":               {"window=15m", 15 * time.Minute, 900},
		"a whole-second window is honoured":                  {"window=90s", 90 * time.Second, 90},
		// The ceiling IS the default now, so the parameter may only narrow. A week's exact
		// dispatched count is 302.4 million index entries at the target rate, which is not
		// servable inside any request timeout — permitting it bought an operator a timeout
		// and the database the scan anyway.
		"the daily ceiling is honoured": {"window=24h", 24 * time.Hour, 86400},
	} {
		t.Run(name, func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
				Return(eventsStatsCounts, nil).Once()
			expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

			// best_effort, because the window bounds the DISPATCHED arm and nothing else, so a
			// posture that does not count the dispatched history has no window to carry tt.query
			// is appended to it so the cases keep naming only the window.
			query := "include_offsets=best_effort"
			if tt.query != "" {
				query += "&" + tt.query
			}

			before := time.Now().UTC()
			statistics, _ := eventsStatsResponse(t, router, query)
			after := time.Now().UTC()

			var since []time.Time
			for _, call := range ds.Calls {
				if call.Method != "CountEventOutboxByStatus" {
					continue
				}
				instant, ok := call.Arguments.Get(1).(time.Time)
				require.True(t, ok, "the count is bounded by an instant")
				since = append(since, instant)
			}
			require.Len(t, since, 1, "the outbox is counted exactly once per request")

			// The window has to reach the QUERY, not merely the response. Bracketed against the
			// wall clock either side of the request rather than compared to a fixed instant, so
			// this cannot flake on a slow machine while still failing outright if the default
			// were substituted for the requested window.
			assert.False(t, since[0].Before(before.Add(-tt.window).Add(-5*time.Second)),
				"the count must be bounded by the window the caller asked for; %s is earlier "+
					"than %s allows", since[0], tt.window)
			assert.False(t, since[0].After(after.Add(-tt.window).Add(5*time.Second)),
				"the count must not be bounded by a SHORTER window than asked for either; %s "+
					"is later than %s allows", since[0], tt.window)

			assert.Equal(t, tt.seconds, statistics.WindowSeconds,
				"the response must report the interval it measured, so an operator can check "+
					"the counts against the window they meant")
			require.NotNil(t, statistics.WindowStart)
			assert.True(t, since[0].Equal(*statistics.WindowStart),
				"and the reported window start must be the one the query used, or the two "+
					"halves of the answer describe different intervals")

			ds.AssertExpectations(t)
		})
	}

	for name, query := range map[string]string{
		"a window that is not a duration":      "window=lastweek",
		"a zero window":                        "window=0s",
		"a negative window":                    "window=-1h",
		"beyond the daily ceiling":             "window=25h",
		"a week, which used to be the ceiling": "window=168h",
		"a sub-second window":                  "window=500ms",
	} {
		t.Run(name+" is refused before the count", func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)

			recorder := eventsKeyedRequest(t, router,
				http.MethodGet, "/events/stats?"+query, eventsMockMasterKey)

			assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
			assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
				"an unusable window must be refused before the outbox is read")
		})
	}
}

// TestGetEventOutboxStats_OmitsTheProducerAtomicityCensusRatherThanReportingZeros is
// the degradation rule for the one reading that is allowed to fail.
//
// Reporting zeros would say the opposite of the truth.
func TestGetEventOutboxStats_OmitsTheProducerAtomicityCensusRatherThanReportingZeros(t *testing.T) {
	t.Run("when the handoff census cannot be read", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("CountUnresolvedEventOutbox", mock.Anything).
			Return(eventsStatsUnresolvedCounts(), nil).Once()
		ds.On("CountBalanceMonitorHandoffByStatus", mock.Anything).
			Return(nil, errors.New("blnk: the handoff table could not be read")).Once()

		statistics, raw := eventsStatsResponse(t, router, "")

		assert.Nil(t, statistics.ProducerAtomicity)
		assert.NotContains(t, raw, "producer_atomicity",
			"an unread census must be ABSENT; a zeroed one asserts that nothing is outstanding")
		assert.Equal(t, int64(11), statistics.Pending,
			"and the counts that WERE read must still be reported")

		// The second census read must not have been attempted after the first failed:
		// a partial answer is not published.
		ds.AssertNotCalled(t, "CountUnfinalizedBulkTransactionBatches", mock.Anything, mock.Anything)
		ds.AssertExpectations(t)
	})

	t.Run("when the bulk-batch census cannot be read", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("CountUnresolvedEventOutbox", mock.Anything).
			Return(eventsStatsUnresolvedCounts(), nil).Once()
		ds.On("CountBalanceMonitorHandoffByStatus", mock.Anything).
			Return(map[string]int64{coremodel.OutboxStatusPending: 2}, nil).Once()
		ds.On("CountUnfinalizedBulkTransactionBatches", mock.Anything, mock.Anything).
			Return(0, nil, errors.New("blnk: the batch table could not be read")).Once()

		statistics, raw := eventsStatsResponse(t, router, "")

		assert.Nil(t, statistics.ProducerAtomicity,
			"half a census is not a census: the handoff figures are dropped with the batches "+
				"rather than reported beside a missing half")
		assert.NotContains(t, raw, "producer_atomicity")
		assert.Equal(t, int64(66), statistics.DeadLettered)

		ds.AssertExpectations(t)
	})
}

// TestGetEventOutboxStats_AnswersATypedErrorWhenTheOutboxCannotBeRead is the one
// reading whose failure is NOT tolerable.
//
// The per-status counts are the outbox side of the reconciliation.
func TestGetEventOutboxStats_AnswersATypedErrorWhenTheOutboxCannotBeRead(t *testing.T) {
	driverText := `pq: permission denied for table event_outbox (SQLSTATE 42501) in aclchk.c:2843`

	router, ds := setupEventsRouter(t, nil)
	// The DEFAULT posture's aggregate, because the property is that the endpoint refuses
	// when the outbox side cannot be read, and the default reading is the one every
	// routine caller takes. Its failure is no more tolerable than the history reading's.
	ds.On("CountUnresolvedEventOutbox", mock.Anything).
		Return(nil, errors.New(driverText)).Once()

	recorder := eventsKeyedRequest(t, router, http.MethodGet, "/events/stats", eventsMockMasterKey)

	assertErrorCode(t, recorder, http.StatusInternalServerError, apierror.ErrGenInternal)
	assert.Equal(t, sanitizedInternalMessage, eventsErrorDetail(t, recorder).Message)

	body := recorder.Body.String()
	for _, leak := range []string{"pq:", "SQLSTATE", "aclchk.c", "permission denied"} {
		assert.NotContains(t, body, leak, "the driver's own text must not reach a client: %q", leak)
	}

	ds.AssertExpectations(t)
}

// TestGetEventOutboxStats_HonoursTheThreeOffsetPostures pins include_offsets.
//
// The three postures exist because "the broker could not be read" means different
// things to different callers, and guessing which is wrong in both directions. The
// posture additionally decides whether the DISPATCHED HISTORY is counted at all,
// because that figure and the broker offsets are wanted by one caller and no
// other(/M02):
//
// An unrecognised value is refused rather than read as false: a caller would otherwise
// receive offsets_complete=false for a reason they cannot see and read a missing
// verdict as an unreachable broker.
func TestGetEventOutboxStats_HonoursTheThreeOffsetPostures(t *testing.T) {
	t.Run("required offsets with no broker is a 503 rather than a halved answer", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
			Return(eventsStatsCounts, nil).Once()
		expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

		recorder := eventsKeyedRequest(t, router,
			http.MethodGet, "/events/stats?include_offsets=true", eventsMockMasterKey)

		assertErrorCode(t, recorder, http.StatusServiceUnavailable, apierror.ErrKafkaUnavailable)
		ds.AssertExpectations(t)
	})

	// The two skipped forms are asserted TOGETHER because the whole substance of the guard
	// is that they behave identically: the cheap reading must be what a caller gets by
	// saying nothing, not only what they get by saying "false".
	for _, query := range []string{"", "include_offsets=false"} {
		name := "an absent include_offsets"
		if query != "" {
			name = "an explicit include_offsets=false"
		}

		t.Run(name+" answers from PostgreSQL alone and counts no history", func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)
			// THE LIGHTWEIGHT AGGREGATE, and it is the expectation itself that proves it:
			// CountEventOutboxByStatus is not programmed at all, so a handler that reached for
			// the history reading would fail on an unexpected call rather than quietly costing
			// 43.2 million index entries in production.
			ds.On("CountUnresolvedEventOutbox", mock.Anything).
				Return(eventsStatsUnresolvedCounts(), nil).Once()
			expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

			statistics, raw := eventsStatsResponse(t, router, query)

			assert.Nil(t, statistics.Dispatched,
				"the dispatched population was not counted, so the key must be ABSENT: a zero "+
					"would tell a reconciliation that nothing was published")
			assert.False(t, statistics.DispatchedHistoryCounted,
				"and the flag must say so, so the omission is readable")
			assert.NotContains(t, raw, "dispatched")

			assert.Equal(t, int64(11), statistics.Pending,
				"every unresolved count is still exact and complete: that is the reading this "+
					"posture takes, not a reduced one")
			assert.Equal(t, int64(66), statistics.DeadLettered)
			assert.False(t, statistics.OffsetsComplete)
			assert.NotContains(t, raw, "topic_end_offsets")
			ds.AssertExpectations(t)
		})
	}

	t.Run("best effort with no broker is a 200 with the counts and the history", func(t *testing.T) {
		// The posture every broker-less deployment runs in permanently, and the one an
		// operator uses when they want the dispatched figure without a 503 hanging on a
		// broker they do not have.
		router, ds := setupEventsRouter(t, nil)
		ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
			Return(eventsStatsCounts, nil).Once()
		expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

		statistics, _ := eventsStatsResponse(t, router, "include_offsets=best_effort")

		assert.Equal(t, int64(66), statistics.DeadLettered,
			"an unreachable broker must not cost the operator the outbox counts as well")
		require.NotNil(t, statistics.Dispatched,
			"best effort counts the history; only the BROKER half is best effort")
		assert.Equal(t, int64(44), *statistics.Dispatched)
		assert.True(t, statistics.DispatchedHistoryCounted)
		ds.AssertExpectations(t)
	})

	t.Run("best-effort is accepted with a hyphen as well", func(t *testing.T) {
		router, ds := setupEventsRouter(t, nil)
		ds.On("CountEventOutboxByStatus", mock.Anything, mock.Anything).
			Return(eventsStatsCounts, nil).Once()
		expectEventsStatsCensus(ds, map[string]int64{}, 0, nil)

		statistics, _ := eventsStatsResponse(t, router, "include_offsets=BEST-EFFORT")

		assert.True(t, statistics.DispatchedHistoryCounted,
			"the two spellings and the case must not be the difference between two postures")
		ds.AssertExpectations(t)
	})

	// The accepted vocabulary here is narrower than include_count's on purpose: this
	// parameter selects a POSTURE rather than a boolean, and "1" or "t" would have to be
	// guessed into one of three. So only "true", "false" and "best_effort" are honoured —
	// case, hyphenation and surrounding whitespace aside — and everything else is refused
	// rather than read as a posture the caller did not name.
	for _, value := range []string{"maybe", "1", "0", "yes", "t", "best", "besteffort"} {
		t.Run("include_offsets="+value+" is refused", func(t *testing.T) {
			router, ds := setupEventsRouter(t, nil)

			recorder := eventsKeyedRequest(t, router,
				http.MethodGet, "/events/stats?include_offsets="+value, eventsMockMasterKey)

			assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
			assert.Empty(t, eventsRepositoryCallsExcludingAuth(ds),
				"an unusable posture must be refused before the counts are taken")
		})
	}
}
