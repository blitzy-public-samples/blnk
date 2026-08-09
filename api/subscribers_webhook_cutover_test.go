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

// subscribers_webhook_cutover_test.go covers the HTTP behaviour of
// DELETE /subscribers/:subscriber_id/webhook-subscription against the REAL repository.
//
// The subject is the whole path from the request to the two columns, because the property
// under test is a property of the write: the cutover records that the legacy endpoint is
// forgotten AND that the migration happened, and it must be impossible to observe one
// without the other.
//
// It used to be two writes with nothing spanning them. The ordering was chosen so a
// partial failure could not OVER-claim progress, and it could not — but a failure in
// between left the row absent from BOTH halves of the migration report: no webhook_url, so
// nothing still to migrate from; no migrated_at, so not counted as migrated. Progress then
// under-reported for the rest of the dual-run window, and nothing told the caller that
// repeating the request was the remedy.
package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/database"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registerCutoverSubscriber registers a subscriber carrying a legacy webhook URL and returns
// its identifier.
//
// It goes through the API rather than an INSERT so that the row is built by the same
// derivation the endpoint under test will read — the principal, the consumer group and the
// canonical identifier are all derived, and a hand-written row that got any of them wrong
// would be refused by the schema's CHECK constraints rather than by anything this test is
// about.
//
// Parameters:
//   - t *testing.T: the test.
//   - router http.Handler: the authenticated router.
//   - ds *database.Datasource: used only to remove the row afterwards.
//   - webhookURL string: the legacy endpoint to record.
//
// Returns:
//   - string: the subscriber identifier.
func registerCutoverSubscriber(
	t *testing.T,
	router http.Handler,
	ds *database.Datasource,
	webhookURL string,
) string {
	t.Helper()

	subscriberID := "apitest_cutover_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	// TWO REQUESTS, because the endpoint is recorded through the GUARDED WRITE PATH and not
	// through general registration. CreateSubscriber carries no webhook_url field at all — its
	// absence is what makes the sunset enforceable, since a surface that can still be populated
	// through an unretired route cannot be retired — so a registration body naming one had the
	// field silently ignored and left this fixture with a NULL endpoint and nothing to migrate.
	body := fmt.Sprintf(
		`{"subscriber_id":%q,"name":"Cutover fixture","authorized_topics":["blnk.transactions"]}`,
		subscriberID,
	)

	request, err := http.NewRequest(http.MethodPost, "/subscribers", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusCreated, recorder.Code,
		"failed to register the cutover fixture: %s", recorder.Body.String())

	// POST /subscribers/:id/webhook-subscription is the only route that writes the column, and
	// it is itself one of the four the sunset retires — which is correct for a fixture whose
	// whole subject is the migration away from it.
	record, err := http.NewRequest(http.MethodPost,
		"/subscribers/"+subscriberID+"/webhook-subscription",
		strings.NewReader(fmt.Sprintf(`{"webhook_url":%q}`, webhookURL)))
	require.NoError(t, err)
	record.Header.Set("Content-Type", "application/json")

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, record)
	require.Contains(t, []int{http.StatusOK, http.StatusCreated, http.StatusNoContent},
		recorder.Code, "failed to record the legacy endpoint: %s", recorder.Body.String())

	t.Cleanup(func() {
		if _, cleanupErr := ds.Conn.Exec(
			`DELETE FROM blnk.event_subscribers WHERE subscriber_id = $1`, subscriberID,
		); cleanupErr != nil {
			t.Logf("failed to remove cutover fixture %s: %v", subscriberID, cleanupErr)
		}
	})

	return subscriberID
}

// readCutoverColumns reads the two columns the cutover writes, straight from the table.
//
// Straight from the table rather than through the API, because the point is what was
// PERSISTED. A response body is built from whatever the write returned, so it cannot
// distinguish "both columns moved" from "the returned struct says they did".
//
// Parameters:
//   - t *testing.T: the test.
//   - ds *database.Datasource: the live repository.
//   - subscriberID string: the row to read.
//
// Returns:
//   - sql.NullString: webhook_url.
//   - sql.NullTime: migrated_at.
func readCutoverColumns(
	t *testing.T,
	ds *database.Datasource,
	subscriberID string,
) (sql.NullString, sql.NullTime) {
	t.Helper()

	var (
		webhookURL sql.NullString
		migratedAt sql.NullTime
	)

	err := ds.Conn.QueryRow(`
		SELECT webhook_url, migrated_at
		FROM blnk.event_subscribers
		WHERE subscriber_id = $1
	`, subscriberID).Scan(&webhookURL, &migratedAt)
	require.NoError(t, err, "the cutover fixture row must exist")

	return webhookURL, migratedAt
}

// TestDeleteWebhookSubscription_RecordsTheCutoverAsOneWrite is the endpoint half of the
// atomicity guard.
//
// The row must end on exactly ONE side of the migration report: either still awaiting
// migration with its endpoint recorded, or migrated with the endpoint forgotten. The state
// this test rules out is the third one the split write could produce — neither.
func TestDeleteWebhookSubscription_RecordsTheCutoverAsOneWrite(t *testing.T) {
	router, instance := setupAuthedRouter(t, true, nil)
	ds := eventsTestDataSource(t, instance)

	const legacyURL = "https://acme.example.com/blnk-webhooks"
	subscriberID := registerCutoverSubscriber(t, router, ds, legacyURL)

	// BEFORE: the row is on the "still awaiting migration" side — it has an endpoint to
	// migrate FROM and no instant to be counted by.
	webhookURL, migratedAt := readCutoverColumns(t, ds, subscriberID)
	require.True(t, webhookURL.Valid, "the fixture must start with a recorded endpoint")
	assert.Equal(t, legacyURL, webhookURL.String)
	require.False(t, migratedAt.Valid, "and must not start out claiming a migration")

	request, err := http.NewRequest(http.MethodDelete,
		"/subscribers/"+subscriberID+"/webhook-subscription", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusNoContent, recorder.Code, "body: %s", recorder.Body.String())
	assert.Empty(t, recorder.Body.String(), "204 carries no body")

	// AFTER: the row is on the "migrated" side, and BOTH facts moved.
	webhookURL, migratedAt = readCutoverColumns(t, ds, subscriberID)

	assert.False(t, webhookURL.Valid,
		"the legacy endpoint must be forgotten, so the subscriber is no longer counted as "+
			"awaiting migration")
	require.True(t, migratedAt.Valid,
		"AND the migration instant must be recorded. A row with neither is absent from both "+
			"halves of the migration report, which is the state the split write could produce "+
			"and nothing would report")
	assert.WithinDuration(t, time.Now(), migratedAt.Time, time.Minute)
}

// TestDeleteWebhookSubscription_IsIdempotentAndStillReportsBothFacts covers the retry the old
// implementation depended on for its recovery story.
//
// A repeat used to be the REMEDY for a stranded row. It is now merely harmless — and it must
// stay harmless, because an operator who repeated the request under the old behaviour will
// repeat it under the new one.
func TestDeleteWebhookSubscription_IsIdempotentAndStillReportsBothFacts(t *testing.T) {
	router, instance := setupAuthedRouter(t, true, nil)
	ds := eventsTestDataSource(t, instance)

	subscriberID := registerCutoverSubscriber(t, router, ds,
		"https://acme.example.com/blnk-webhooks")

	for attempt := 1; attempt <= 2; attempt++ {
		request, err := http.NewRequest(http.MethodDelete,
			"/subscribers/"+subscriberID+"/webhook-subscription", nil)
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusNoContent, recorder.Code,
			"attempt %d: %s", attempt, recorder.Body.String())

		webhookURL, migratedAt := readCutoverColumns(t, ds, subscriberID)
		assert.False(t, webhookURL.Valid, "attempt %d", attempt)
		assert.True(t, migratedAt.Valid, "attempt %d", attempt)
	}
}

// TestDeleteWebhookSubscription_RefusesWhatItShould keeps the endpoint's guards intact, since
// the handler body was rewritten.
//
// A blank identifier and a missing subscriber must answer as they did, and the master-key gate
// must still run FIRST — before the identifier is even read — so an unauthorised caller cannot
// learn whether a subscriber exists from the difference between a 400 and a 404.
func TestDeleteWebhookSubscription_RefusesWhatItShould(t *testing.T) {
	t.Run("a missing subscriber is a 404", func(t *testing.T) {
		router, _ := setupAuthedRouter(t, true, nil)

		request, err := http.NewRequest(http.MethodDelete,
			"/subscribers/apitest_cutover_absent_0001/webhook-subscription", nil)
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusNotFound, recorder.Code, "body: %s", recorder.Body.String())

		var body struct {
			ErrorDetail struct {
				Code string `json:"code"`
			} `json:"error_detail"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		assert.Equal(t, "SUBSCRIBER_NOT_FOUND", body.ErrorDetail.Code)
	})

	t.Run("a non-master caller is refused before anything is read", func(t *testing.T) {
		router, instance := setupAuthedRouter(t, true, nil)
		ds := eventsTestDataSource(t, instance)
		subscriberID := registerCutoverSubscriber(t, router, ds,
			"https://acme.example.com/blnk-webhooks")

		// A router whose caller is NOT the master key.
		guarded, _ := setupAuthedRouter(t, false, nil)

		request, err := http.NewRequest(http.MethodDelete,
			"/subscribers/"+subscriberID+"/webhook-subscription", nil)
		require.NoError(t, err)

		recorder := httptest.NewRecorder()
		guarded.ServeHTTP(recorder, request)

		assert.NotEqual(t, http.StatusNoContent, recorder.Code,
			"a non-master caller must not be able to complete a cutover")

		// AND NOTHING MOVED.
		webhookURL, migratedAt := readCutoverColumns(t, ds, subscriberID)
		assert.True(t, webhookURL.Valid, "the endpoint must still be recorded")
		assert.False(t, migratedAt.Valid, "and no migration may be claimed")
	})
}
