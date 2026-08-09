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

// events_api_test.go covers the request-side contract of the event management
// surface: GET /events/dead-letter, POST /events/dead-letter/:event_id/replay and
// GET /events/stats.
//
// # What these tests are for, and what they deliberately are not
//
// They cover the decisions the HANDLER owns — the master-key gate, the closed
// query-parameter set, parameter parsing and validation, and the response
// envelope — because those are the decisions no other layer can make on its
// behalf. A parameter the handler mis-parses is wrong however correct the
// repository query beneath it is.
//
// They deliberately do NOT re-prove the narrowing itself. Every filter is applied
// in SQL, and that is established where it can be established against real
// PostgreSQL: TestDeadLetterInventoryNarrowing_FiltersAndCountsInSQL_RealDB in
// the database package asserts each predicate selects, that the offset is an
// offset into the filtered set, and that the count and the page describe one set.
// Restating those assertions here against a shared table would only add flakes.
//
// The listing reads the outbox table and never a Kafka topic, so every test below
// runs with no broker configured — which is also the deployment posture that must
// keep working.
package api

import (
	"context"
	"encoding/json"
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
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventsRequest issues a request against a master-key router and returns the
// recorder.
//
// The master key is the authorised principal for every endpoint in this file, so
// a test that is about parameter handling should not have to restate the
// authorisation setup. The one test that is about authorisation builds its own
// non-master router.
func eventsRequest(t *testing.T, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	router, _ := setupAuthedRouter(t, true, nil)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, nil)
	router.ServeHTTP(recorder, request)

	return recorder
}

// assertEventsErrorCode asserts the status AND the error code.
//
// Both are required. apierror.ErrAuthMasterKeyRequired and
// ErrAuthUnknownResource both resolve to 403, so a status-only assertion cannot
// distinguish "this gate refused me" from "the route prefix is not registered in
// the authorization middleware" — which is a real and easy failure mode for a new
// route prefix, and one that would make every one of these tests pass for the
// wrong reason.
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

// TestListDeadLetterEvents_RequiresTheMasterKey pins the gate that must fire
// before anything else happens.
//
// The dead-letter inventory carries the failure reason and the broker coordinate
// of every event a deployment could not deliver. It is operator-only, and the
// gate is asserted on the CODE rather than the status for the reason given on
// assertEventsErrorCode.
func TestListDeadLetterEvents_RequiresTheMasterKey(t *testing.T) {
	router, _ := setupAuthedRouter(t, false, nil)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/events/dead-letter", nil))

	assertEventsErrorCode(t, recorder, http.StatusForbidden, "AUTH_MASTER_KEY_REQUIRED")
}

// TestListDeadLetterEvents_AcceptsTheOccurrenceWindow covers the filter that was
// previously REFUSED at this layer.
//
// The endpoint used to reject occurred_from and occurred_to as unsupported
// parameters, because no layer carried an occurrence window: the repository's
// listing took only a limit and an offset, and the service filtered what came
// back in memory. An operator scoping triage to an incident — "what is stuck from
// the twenty minutes the broker was down" — had no way to express it and had to
// page the inventory reading timestamps by eye.
//
// Now the window is a SQL predicate, so the parameters are accepted. This test
// asserts the acceptance and the parsing; the selection itself is proved against
// real PostgreSQL in the database package.
func TestListDeadLetterEvents_AcceptsTheOccurrenceWindow(t *testing.T) {
	for _, target := range []string{
		"/events/dead-letter?occurred_from=2026-01-02T03:04:05Z",
		"/events/dead-letter?occurred_to=2026-01-02T03:04:05Z",
		"/events/dead-letter?occurred_from=2026-01-02T03:04:05Z&occurred_to=2026-01-02T04:04:05Z",
		// An offset rather than Z: the same instant expressed in another zone
		// must be accepted, or a caller pasting a timestamp out of a response
		// into a filter is refused.
		"/events/dead-letter?occurred_from=2026-01-02T04:04:05%2B01:00",
	} {
		t.Run(target, func(t *testing.T) {
			recorder := eventsRequest(t, http.MethodGet, target)

			require.Equal(t, http.StatusOK, recorder.Code,
				"an occurrence window is a supported filter: body %s", recorder.Body.String())

			// The shape is the PAGE ENVELOPE, not a bare array. Paging is keyset-based
			// (PERF-P08), so the position to resume from has to be returned somewhere and
			// an envelope is the only place a page can carry it without inventing a header.
			// `data` must still be an array rather than null, so a reconciliation script
			// can range over it unconditionally on an empty inventory.
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

// TestListDeadLetterEvents_RefusesAnUnusableOccurrenceWindow covers the two ways
// a window can be wrong.
//
// Both are refused rather than ignored, and that is the whole point: this is a
// triage endpoint, and an operator who asked for a twenty-minute window and
// silently received the whole inventory would read stale entries as current with
// nothing in the response to say the filter was dropped.
//
//   - An UNPARSEABLE bound is a 400 from the handler. A bare date or a local
//     timestamp with no zone is included deliberately: guessing a zone would
//     shift the window by hours.
//   - A REVERSED window is a 400 from the service. It is unsatisfiable by
//     construction, so no row can be inside it and an empty page would read as
//     "nothing is stuck".
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

// TestListDeadLetterEvents_CountsEveryNarrowing covers include_count for filters
// that were previously REFUSED it.
//
// The total used to come from a whole-table per-status aggregate that knew nothing
// about the event-type or topic filter, so the endpoint answered 400 for
// include_count together with either of them: a total describing a different set
// than the page is worse than no total, because a paging client loops on it. The
// total is now counted from the same predicate as the page, so it is available for
// every narrowing.
//
// The assertion is on the ENVELOPE and on the relationship between the two
// numbers, not on an absolute count: the outbox table is shared with every other
// test tier and clone, so an absolute number would be a flake. What must hold is
// that total_count is present and is never short of the page it accompanies.
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

// TestListDeadLetterEvents_RefusesAnUnsupportedQueryParameter keeps the accepted
// set closed.
//
// Quietly ignoring a narrowing hands a caller a result that answers a different
// question than the one they posed, and they have no way to tell. Every offending
// name is reported at once so a caller fixes one request rather than discovering
// its parameters one round trip at a time.
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

// TestReplayDeadLetterEvent_ValidatesTheEventIDBeforeLookingItUp separates a
// malformed request from a missing event.
//
// Every id this pipeline mints is a lowercase canonical UUID, and the lookup is an
// exact string comparison. Without this gate an uppercased or braced spelling of a
// REAL event's id would miss the row and be reported as 404 EVENT_NOT_FOUND —
// which sends an operator to look for an event that is sitting right there. A 400
// tells them the id they pasted is wrong, which is the actual problem.
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
		// Consistent with every other parameter in this package, and it is the
		// difference between a copy-and-paste that picked up a space being usable and
		// being refused. The trimmed value is canonical, so it reaches the lookup and is
		// answered as a missing event rather than as a bad request.
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
// total asserted over any unscoped dimension is a race. There is deliberately no CHECK
// constraint enumerating event types — bulk events carry a status suffix — so a
// synthetic value is a legal row.
func newDeadLetterEventType(t *testing.T) string {
	t.Helper()

	return "apitest.deadletter." + uuid.NewString()
}

// seedDeadLetteredEvent inserts one row already in a terminal failure state.
//
// The insert is written here rather than driven through the relay's transitions on
// purpose. The subject of this file is the HANDLER — its query contract and the total it
// reports — and reaching a terminal state through claim-and-mark would make each fixture
// contend with any live relay for the row, turning a deterministic handler assertion
// into a timing one. Fidelity of the write path itself is covered where it belongs, in
// database/event_outbox_test.go, against the production insert.
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

// TestListDeadLetterEvents_HonoursIncludeCountAlongsideEveryFilter is the endpoint half of
// the filtered-inventory contract.
//
// The endpoint used to REFUSE include_count with a 400 whenever event_type or topic was
// set, because the only count available was a per-status aggregate over the whole table —
// a number that describes a different set from the page it would have been reported
// beside. The consequence was operational: an operator narrowing the inventory during an
// incident lost the one figure that says how much work is in front of them, and a paging
// client had no way to know whether a short page was the end of the matches.
//
// The assertions are therefore that the pair is now (a) accepted and (b) consistent — the
// total equals the number of matching rows, and it is NOT the size of the returned page
// and NOT the size of the table.
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

// TestListDeadLetterEvents_RefusesAnUnsupportedStatusBeforeCounting keeps validation ahead
// of the count.
//
// The status vocabulary is closed on purpose: a filter that quietly matched nothing would
// answer "nothing is stuck" to an operator who asked a different question. Honouring
// include_count must not have moved that refusal — a request that names an unusable status
// has to be refused whether or not it also asks for a total.
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

// eventsRouterRequest issues a request against a router the caller supplies, with an optional
// JSON body.
//
// It is separate from eventsRequest because the two suites in this file need different
// things: eventsRequest builds its own master-key router per call, which is what the
// gate and envelope tests want, while these tests build one router and exercise several
// requests against it, including bodies. Sharing one helper would have forced one of the
// two to test through a shape it does not mean.
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

// eventsRouter builds a real router for the /events surface, with the master-key context
// injected into the API's OWN engine before the routes are registered.
//
// It mirrors setupAuthedRouter — reaching for apiInstance.router, which this test package can
// — rather than wrapping the finished router in a second engine. Wrapping was tried and is
// wrong twice over: a nested engine's context is not the one the handlers read, so the
// injected flag is invisible to them, and re-entering through HandleContext writes the body
// twice.
//
// Secure mode is OFF here on purpose. The property under test is the ENDPOINT's own
// master-key gate — the privileged-endpoint convention — and not the authentication
// middleware, which api/middleware/auth_test.go and webhook_sunset_test.go exercise for real.
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

// TestEventsAPI_RoutesAreReachableAndMasterKeyGated is the authorization-ordering matrix, and
// it covers the trap that has no other symptom.
//
// The gate itself is what this table asserts: a non-master caller must be refused with
// AUTH_MASTER_KEY_REQUIRED, and refused BEFORE any work — these are triage endpoints over the
// whole deployment's event inventory, not a tenant's own data — while the master key must pass
// it and reach the handler.
//
// THE RESOURCE MAP IS NOT PROVEN HERE, and the absence-of-AUTH_UNKNOWN_RESOURCE check below is
// only a cheap backstop. This router runs with secure mode off, and the auth middleware returns
// early in that case without consulting pathToResource at all — so an unmapped prefix would
// pass every assertion in this function. TestEventsAPI_PathPrefixesResolveToAuthorizationResources
// is where that is actually established, and it needs secure mode and a non-master key to do it.
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

// TestEventsAPI_DeadLetterInventoryRespondsWithAJSONArray pins the response SHAPE, which a
// reconciliation script depends on more than it depends on any single field.
//
// An empty inventory must marshal as [] and not null: a caller that ranges over the response
// unconditionally breaks on null, and that break happens in exactly the healthy case where
// nothing has been dead-lettered.
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

// TestEventsAPI_DeadLetterPagingIsBoundedAndValidated covers the query-string contract, and
// it asserts the contract this package actually keeps rather than the one it might have.
//
// The split is deliberate and it is the package's convention, shared with
// ParseFiltersFromBody and GetAllLedgers: a value that does not PARSE is refused, because
// defaulting it would serve a page the caller never asked for and the mistake is the
// caller's to see; a value that parses but is out of range is NORMALISED, because "give me
// everything" and "give me nothing" are asks with a sensible reading and refusing them would
// make this endpoint behave unlike every other list endpoint here.
//
// The normalisation is asserted at the reader as well as through the router, because the
// property that matters — an oversized limit cannot turn one request into a full-table read —
// is about the VALUE, and a 200 over an empty inventory would look identical either way.
func TestEventsAPI_DeadLetterPagingIsBoundedAndValidated(t *testing.T) {
	t.Run("a valid page is accepted", func(t *testing.T) {
		// limit and cursor, because this endpoint pages by KEYSET. An offset into a table rows
		// are continuously appended to and removed from cannot describe a stable page, so
		// `offset` is not among the accepted parameters and is refused with the other unknown
		// narrowings — see TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter.
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
	// narrowings by TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter below. An offset
	// into a table rows are continuously appended to and removed from cannot describe a
	// stable page, which is why the keyset cursor exists; a test asserting an offset were
	// clamped would pin a parameter the surface does not accept.
}

// TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter is the fail-loud contract on
// filters, and it is a deliberate departure from ignoring what you cannot honour.
//
// These are triage endpoints. An operator who filtered by an occurrence window and silently
// received the whole inventory would read entries that do not answer their question and have
// no way to notice. So an unsupported narrowing is an error.
func TestEventsAPI_DeadLetterRefusesAnUnknownQueryParameter(t *testing.T) {
	// aggregate_id, and NOT an occurrence bound. This once sent occurred_after on the strength of
	// the window being unsupported; the endpoint now applies a window, so that request is a
	// legitimate 200 and the test proved nothing about refusals. The parameter has to be one the
	// accepted set genuinely does not contain, or this asserts the absence of a feature rather
	// than the presence of the guard.
	w := eventsRouterRequest(t, eventsRouter(t, true), http.MethodGet,
		"/events/dead-letter?aggregate_id=txn_01HXYZ", "")

	assert.Equal(t, http.StatusBadRequest, w.Code,
		"a narrowing this endpoint cannot apply must be refused, not ignored: ignoring it answers "+
			"a different question than the operator asked. body: %s", w.Body.String())
}

// TestEventsAPI_ReplayRefusesAnUnknownEvent covers the replay error matrix at the router.
//
// Replay is the one write on this surface, and an identifier that matches nothing must be a
// clean, typed refusal rather than a 500 — an operator triaging a dead-letter topic works
// from identifiers copied by hand, so a wrong one is an ordinary occurrence.
func TestEventsAPI_ReplayRefusesAnUnknownEvent(t *testing.T) {
	// A WELL-FORMED identifier that matches nothing. event_id is a canonical UUID by contract,
	// so a malformed one is a 400 about the REQUEST and is covered separately; what this test is
	// about is the identifier that could have existed and does not.
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

// TestEventsAPI_StatsRespondsWithTheOutboxCounts covers the endpoint the daily zero-loss
// reconciliation is built on.
//
// The counts come from PostgreSQL and the offsets from the broker, and the endpoint is
// documented to answer with the counts alone when the broker cannot be reached — so the
// no-broker case must be a 200 with counts, not a failure. That degradation is the property
// asserted here, because it is what makes the reconciliation runnable at all in a deployment
// whose broker is momentarily unreachable.
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

// TestEventsAPI_StatsRefusesAWindowItCannotReport is the MD-2 guard.
//
// The response reports the interval it measured as `window_seconds`, an integer. A sub-second or
// fractional window was accepted, honoured at full precision against the database and then
// reported truncated: `?window=1500ms` measured 1.5 seconds and answered `window_seconds: 1`, and
// `?window=500ms` answered 0 — which reads as "no window" and is the one value that means the
// opposite of what happened.
//
// An operator comparing a count against the window they asked for would be comparing it against a
// different one, with nothing in the response revealing the difference. Refusing costs one
// corrected request; truncating costs the arithmetic that follows it.
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
		for _, window := range []string{"1s", "90s", "15m", "36h", "2m30s", "1.5h"} {
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

// TestEventsAPI_UnsupportedMethodsDoNotReachTheHandlers is the routing assertion.
//
// The three routes are registered for one verb each. A different verb must not fall through
// to a handler for another route — which is what would happen if any of them were registered
// with router.Any — and must not be answered 200.
func TestEventsAPI_UnsupportedMethodsDoNotReachTheHandlers(t *testing.T) {
	for name, target := range map[string]struct {
		method string
		path   string
	}{
		"POST to the inventory": {http.MethodPost, "/events/dead-letter"},
		"DELETE the inventory":  {http.MethodDelete, "/events/dead-letter"},
		"PUT the statistics":    {http.MethodPut, "/events/stats"},
		"DELETE the statistics": {http.MethodDelete, "/events/stats"},
		"GET the replay":        {http.MethodGet, "/events/dead-letter/evt_probe/replay"},
		"DELETE the replay":     {http.MethodDelete, "/events/dead-letter/evt_probe/replay"},
	} {
		t.Run(name, func(t *testing.T) {
			w := eventsRouterRequest(t, eventsRouter(t, true), target.method, target.path, "")

			assert.NotEqual(t, http.StatusOK, w.Code,
				"%s %s must not be handled; a 200 here means the verb reached a handler that was "+
					"registered for a different one", target.method, target.path)
		})
	}
}

// eventsResourceMapMasterKey is the master secret the secure-mode resource-map test configures.
const eventsResourceMapMasterKey = "resource-map-master-key-for-the-real-router-tests"

// TestEventsAPI_PathPrefixesResolveToAuthorizationResources is the trap the other tests in this
// file cannot see, and it needs SECURE MODE plus a NON-MASTER key to be visible at all.
//
// # Why the obvious test is vacuous
//
// The auth middleware returns early when Server.Secure is false, so pathToResource is never
// consulted — every assertion made against an insecure router passes whether the prefix is
// mapped or not. It then returns early again for the MASTER key, which has all permissions and
// needs no resource. So the resource map is reached only by a request that is authenticated,
// not master, and secure: exactly one combination, and it is the combination nothing tested.
//
// # What an unmapped prefix does
//
// getResourceFromPath returns "" and the middleware aborts with AUTH_UNKNOWN_RESOURCE — for
// every caller of that route, forever. That is why "events" and "subscribers" had to be added
// to two files rather than one, and why registering the scope constant alone is not enough.
//
// # What this asserts
//
// The refusal a scoped key receives must NAME THE RESOLVED SCOPE — "events:read",
// "subscribers:write" and so on — and must never be AUTH_UNKNOWN_RESOURCE. Naming the scope is
// stronger evidence than merely getting past the map: it proves the prefix resolved to the
// RIGHT resource and that the method mapped to the right action, which are the two things the
// map is for. A prefix mapped to the wrong resource would authorise these routes under some
// other feature's scope, and nothing else in the suite would notice.
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
					"request to it — master key included. The path prefix is missing from "+
					"pathToResource in api/middleware/auth.go; adding the scope constant alone is "+
					"not enough", target.method, target.path)

			// The resolved scope, named in the refusal. This is the assertion that pins the
			// MAPPING rather than merely its existence: a prefix pointed at the wrong resource
			// would authorise these routes under another feature's scope and nothing else in
			// the suite would see it.
			require.Equal(t, apierror.ErrAuthInsufficientPermissions, body.ErrorDetail.Code,
				"a key scoped to something else must be refused on permissions, which is what "+
					"proves the route was resolved and evaluated. body: %s", w.Body.String())
			assert.Contains(t, body.ErrorDetail.Message, target.scope,
				"the refusal must name %q: that is the resource this path maps to and the action "+
					"this method maps to, and both have to be right", target.scope)
		})
	}
}

// eventsQueryContext builds a gin context carrying a GET request with the given
// raw query string, plus the recorder any refusal is written to.
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

// Four tests that arrived with the occurrence window are GONE, and what they asserted is
// covered above rather than lost.
//
// They were written against a different shape of the same feature: the window carried as
// *time.Time so an absent bound is nil, a per-status aggregate reached directly on the
// datasource, and a countableDeadLetterPage predicate deciding which filters may be counted at
// all. This tree resolves each of those differently, and the difference is a capability rather
// than a preference:
//
//   - The window is time.Time VALUES on DeadLetterListOptions, zero meaning unbounded, bound as
//     SQL index conditions by deadLetterFilterClause. TestGetDeadLetterEvents_AcceptsTheWindow
//     and its malformed-bound sibling above exercise every accepted and refused form over HTTP,
//     which is where an operator meets it.
//   - The total comes from the dead-letter SERVICE, through a paired read that draws it and the
//     page from ONE database snapshot, so the total describes the very population the page is a
//     slice of. That is why there is no countable-page predicate to test: include_count is
//     accepted alongside EVERY filter, and the restriction those tests pinned was the limitation
//     this tree removed. What the total does NOT promise is that paging to it exhausts the
//     matches — the inventory is live across a paging session — and DeadLetterPageResponse.
//     TotalCount states that scope rather than leaving a client to assume the stronger one.
//   - deadLetterInventoryTotal therefore has no per-status map to assert arguments against. Its
//     contract — the total describes THIS page's set — is asserted through the endpoint.
//
// TestDeadLetterQueryParameters_AcceptTheOccurrenceWindow and
// TestDeadLetterQueryParameters_AreDocumentedInTheTriageRunbook are kept, retargeted onto the
// implemented parameter names: they pin the closed accepted set and the runbook that publishes
// it, and neither depends on how the bound is represented in Go.

// TestDeadLetterQueryParameters_AcceptTheOccurrenceWindow pins the accepted set.
//
// The accepted set is CLOSED: an unknown parameter is refused rather than ignored, because an
// operator who filtered and received the whole inventory reads stale entries as current. That
// makes membership of the set the whole contract — a window the endpoint can apply but does not
// list would be rejected as unsupported, which is the same wrong answer with a different message.
func TestDeadLetterQueryParameters_AcceptTheOccurrenceWindow(t *testing.T) {
	assert.Contains(t, deadLetterQueryParameters, eventQueryParamOccurredFrom)
	assert.Contains(t, deadLetterQueryParameters, eventQueryParamOccurredTo)
	assert.Equal(t, "occurred_from", eventQueryParamOccurredFrom,
		"the parameter name is published to operators and to the triage runbook")
	assert.Equal(t, "occurred_to", eventQueryParamOccurredTo)

	// RETARGETED ONTO rejectUnsupportedQueryParameters, WHICH IS THE FUNCTION THE ROUTER REACHES.
	//
	// These two subtests used to call rejectUnsupportedEventQueryParameters — a byte-for-byte
	// second copy of that guard which nothing on the request path ever invoked. So the coverage
	// for "an unknown query parameter is refused" was attached to the copy that never ran, and the
	// guard actually protecting GET /events/dead-letter had none. The duplicate has been deleted;
	// these now exercise the live one, which is the same assertion against the code that runs.
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

// TestDeadLetterQueryParameters_AreDocumentedInTheTriageRunbook binds the accepted set to the
// runbook an operator actually reads.
//
// This is the drift the assertion exists for: the accepted set and the runbook's filter table are
// two descriptions of one contract, kept in two files, and a filter added to one without the other
// leaves the published documentation stating a capability the endpoint does not have — or, worse,
// denying one it does, which is how an operator ends up paging an entire inventory by hand.
//
// The rendering matters as much as the presence. Each name is asserted inside a backticked table
// cell, which is where the filter table lists them, so a name that merely appears in prose
// somewhere in the document does not satisfy the check.
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

// TestDeadLetterListing_RunbookCommandsConsumeTheEnvelope binds the response SHAPE to the
// commands an operator copies out of the runbook.
//
// The handler has always answered with a page envelope, and every runbook command parsed the body
// as a bare array. `jq '.[]'` over an object does not fail loudly with the value an operator
// wants; on this object it emits the four field VALUES, so a replay loop reading `.[].event_id`
// produced nothing at all and a triage listing printed field values with no ids — during an
// incident, from a command the documentation told them to run.
//
// The shape is the envelope, because cursor paging has to return the cursor somewhere (PERF-P08).
// So the commands are what changed, and this test is what keeps them changed: it asserts that no
// command in the dead-letter runbook reads the listing as an array.
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

	// The empty answer, in the form it is actually returned. "200 and []" described a body the
	// endpoint does not produce, and a script branching on it would treat every empty inventory
	// as a shape mismatch.
	assert.Contains(t, runbook, "`200` with `\"data\": []`",
		"an empty inventory is an empty data array inside the envelope, never a bare []")
}

// TestDeadLetterPageResponse_IsTheOnlyShapeTheListingReturns pins the envelope at the type level.
//
// The doc comments on both the handler and the DTO used to say the body was a bare array unless a
// total was asked for — a shape the code has not produced since paging moved to a cursor. A
// client written from that description reads `.[]` and gets nothing usable.
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

// eventsReadRepositoryFile returns the text of a repository file, so a published contract can be
// asserted rather than trusted.
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
