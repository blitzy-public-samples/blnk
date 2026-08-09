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

// subscribers_api_test.go covers the request-side contract of the subscriber
// registry surface, and specifically the four decisions this layer owns outright
// because no layer beneath it can make them:
//
//	The five-second CEILING on credential issuance, which is a property of the
//	request rather than of the service call inside it.
//	Path-parameter VALIDATION, which decides whether a malformed identifier is a
//	client mistake or a missing resource.
//	The closed QUERY-PARAMETER SET on the listing, which decides whether a
//	misspelled filter narrows nothing in silence.
//	The include_count ENVELOPE, which is the shape every other listing in the API
//	already answers in.
//
// The service's own behaviour — provisioning, ACL reconciliation, the registry
// writes — is asserted in the root package against doubles and against a real
// broker, and is deliberately not restated here.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/internal/apierror"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subscribersRequest issues a request against a master-key router and returns the
// recorder. Every endpoint in this surface is master-key gated, so a test about
// parameter handling should not have to restate the authorisation setup.
func subscribersRequest(t *testing.T, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	router, _ := setupAuthedRouter(t, true, nil)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(method, target, nil))

	return recorder
}

// assertSubscribersErrorCode asserts the status AND the error code.
//
// Both are needed for the reason given on assertEventsErrorCode: several distinct
// refusals share a status, and a status-only assertion cannot tell "this gate
// refused me" from "the route prefix is missing from the authorization
// middleware" — a real failure mode for a new prefix, and one that would make
// every test here pass for the wrong reason.
func assertSubscribersErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
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

// TestSubscriberRoutes_RequireTheMasterKey pins the gate on every route in the
// surface.
//
// The credential route mints a SASL secret and the read routes disclose the
// broker-side access model of every subscriber, so a single unguarded route is
// the whole surface. It is asserted per route rather than once, because the gate
// is a call at the top of each handler and a new handler can be added without it.
func TestSubscriberRoutes_RequireTheMasterKey(t *testing.T) {
	router, _ := setupAuthedRouter(t, false, nil)

	for _, route := range []struct{ method, target string }{
		{http.MethodGet, "/subscribers"},
		{http.MethodGet, "/subscribers/sub_9f1c8a72"},
		{http.MethodPost, "/subscribers/sub_9f1c8a72/kafka-credentials"},
	} {
		t.Run(route.method+" "+route.target, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(route.method, route.target, nil))

			assertSubscribersErrorCode(t, recorder, http.StatusForbidden, "AUTH_MASTER_KEY_REQUIRED")
		})
	}
}

// TestSubscriberRoute_ValidatesTheIdentifierBeforeLookingItUp is C-07 at this
// surface.
//
// # The defect
//
// The route parameter was trimmed and passed straight to the lookup. A value no
// subscriber id can ever equal — "sub_ABC!", "*", "a" — therefore matched no row
// and came back as 404 SUBSCRIBER_NOT_FOUND, which reads as "that subscriber was
// deleted" when it means "that is not an identifier". The two demand opposite
// responses from a caller: re-register, or fix the request.
//
// It also spent a database round trip on a value refusable in memory, and on the
// credential route a slice of the five-second budget.
//
// The validator is model.CanonicalizeSubscriberIdentifier — the same function the
// registry, the schema check and the Kafka principal derivation use — so a value
// this layer accepts is provably one a row could have held. The cases below are
// its rules, each of which would previously have produced a 404.
func TestSubscriberRoute_ValidatesTheIdentifierBeforeLookingItUp(t *testing.T) {
	for name, identifier := range map[string]string{
		"a wildcard, which names every Kafka resource": "%2A",
		"uppercase, which would fold two principals":   "sub_ABC123",
		"a punctuation character":                      "sub_9f1c!",
		"a colon, the principal syntax separator":      "sub_9f1c%3Aadmin",
		"shorter than the minimum":                     "ab",
		"a leading underscore":                         "_sub_9f1c",
	} {
		t.Run(name, func(t *testing.T) {
			// Asserted on BOTH routes, because each reads the parameter through the same
			// helper and a regression in one is a regression in the other.
			for _, target := range []struct {
				method, path string
			}{
				{http.MethodGet, "/subscribers/" + identifier},
				{http.MethodPost, "/subscribers/" + identifier + "/kafka-credentials"},
			} {
				recorder := subscribersRequest(t, target.method, target.path)

				assertSubscribersErrorCode(t, recorder,
					http.StatusBadRequest, "GEN_VALIDATION_ERROR")
			}
		})
	}
}

// TestSubscriberRoute_StillAnswersMissingForABlankIdentifier keeps the two
// refusals distinct.
//
// A blank-but-present segment is a MISSING parameter, not a malformed one, and it
// must stay that way: collapsing the two would tell a caller that omitted the id
// that its id is invalid.
func TestSubscriberRoute_StillAnswersMissingForABlankIdentifier(t *testing.T) {
	recorder := subscribersRequest(t, http.MethodGet, "/subscribers/%20%20")

	assertSubscribersErrorCode(t, recorder, http.StatusBadRequest, "GEN_MISSING_PARAMETER")
}

// TestSubscriberRoute_AcceptsAGeneratedIdentifier is the negative control, and it
// is what stops the validation above being too strict.
//
// The identifier under test comes from model.GenerateSubscriberID — the function
// that mints every id in the system — so if canonicalization refused this shape,
// every real subscriber would be unreachable through its own routes. The assertion
// is only that the request got PAST validation: with no such row it reaches the
// lookup and answers 404, which is the correct answer and is precisely the answer
// a malformed id must NOT produce.
func TestSubscriberRoute_AcceptsAGeneratedIdentifier(t *testing.T) {
	recorder := subscribersRequest(t, http.MethodGet, "/subscribers/"+coremodel.GenerateSubscriberID())

	assertSubscribersErrorCode(t, recorder, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND")
}

// TestIssueWithinBudget_AnswersOnTimeWhenTheIssuanceOverrunsIt is C-23, and it is
// the only place the ceiling can be asserted as a property of the request.
//
// # The defect
//
// AAP R-7 requires issuance to complete within five seconds, and the handler
// bounds the service context to exactly that — which is not the same thing. The
// service's compensating writes and its fence release each run on a FRESH bounded
// context, deliberately, because an expired issuance deadline is one of the
// commonest reasons a cleanup is needed and a cleanup on an already-cancelled
// context does nothing. Those fresh budgets are the same five seconds, so a
// synchronous chain of issuance, compensation and release can take fifteen seconds
// of wall clock while every step in it is individually bounded.
//
// # What is asserted
//
// That the CALL returns when the budget elapses rather than when the work does.
// The fake issuance below never returns, so a helper that waited would hang this
// test — which is the point: the assertion is a real deadline, not a mocked clock.
//
// The budget is shortened to keep the test fast. The ceiling under test is a
// relationship between the context and the return, not the specific duration, and
// the production value is pinned separately below.
func TestIssueWithinBudget_AnswersOnTimeWhenTheIssuanceOverrunsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	began := time.Now()

	credential, err, completed := issueWithinBudget(ctx, "sub_9f1c8a72",
		func(context.Context, string) (blnk.SubscriberCredential, error) {
			close(started)
			// Blocks past the budget, standing in for an issuance that is still
			// compensating on its own detached context.
			<-release

			return blnk.SubscriberCredential{}, nil
		})

	<-started

	assert.False(t, completed,
		"the call must report that it stopped waiting: the work is still running, so a true here "+
			"would have the handler serialise a credential that was never issued")
	assert.NoError(t, err, "an abandoned attempt has produced no failure yet, so there is none to report")
	assert.Empty(t, credential.Password(),
		"NO SECRET MAY BE RETURNED on the abandoned path; the zero credential is what the handler "+
			"must receive")

	assert.Less(t, time.Since(began), time.Second,
		"the call must return at the CEILING rather than when the work finishes; taking longer is "+
			"the fifteen-second wall clock this closes")
}

// TestIssueWithinBudget_ReturnsTheIssuanceWhenItFinishesInTime is the other half:
// the ceiling must not become a truncation.
//
// A helper that abandoned unconditionally, or that raced its own goroutine, would
// make the endpoint useless while passing the test above. Both the success and the
// failure paths are asserted, because they return through different arms and a
// mutant that dropped the error would hand back a zero credential as a success.
func TestIssueWithinBudget_ReturnsTheIssuanceWhenItFinishesInTime(t *testing.T) {
	t.Run("a completed issuance is returned", func(t *testing.T) {
		issued := time.Now().UTC()

		credential, err, completed := issueWithinBudget(context.Background(), "sub_9f1c8a72",
			func(context.Context, string) (blnk.SubscriberCredential, error) {
				return blnk.SubscriberCredential{
					SubscriberID: "sub_9f1c8a72",
					Username:     "blnk-sub-sub_9f1c8a72",
					IssuedAt:     issued,
				}, nil
			})

		require.True(t, completed, "an issuance that finished inside the budget must be reported")
		require.NoError(t, err)
		assert.Equal(t, "blnk-sub-sub_9f1c8a72", credential.Username,
			"and returned intact, or the ceiling would have truncated a successful response")
		assert.Equal(t, issued, credential.IssuedAt)
	})

	t.Run("a failed issuance returns its error", func(t *testing.T) {
		failure := assert.AnError

		credential, err, completed := issueWithinBudget(context.Background(), "sub_9f1c8a72",
			func(context.Context, string) (blnk.SubscriberCredential, error) {
				return blnk.SubscriberCredential{}, failure
			})

		require.True(t, completed,
			"a failure IS a completion: the handler must classify it rather than report a timeout")
		assert.ErrorIs(t, err, failure,
			"the service's own error must survive, because the codes it classifies are more precise "+
				"than anything this layer could re-derive")
		assert.Empty(t, credential.Password())
	})

	t.Run("a caller that has already gone away is not waited for", func(t *testing.T) {
		// A cancelled request context lands in the same arm as an elapsed budget, which
		// is correct: there is nobody to answer, so the work is abandoned to its own
		// cleanup rather than held open.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, completed := issueWithinBudget(ctx, "sub_9f1c8a72",
			func(context.Context, string) (blnk.SubscriberCredential, error) {
				<-time.After(time.Minute)

				return blnk.SubscriberCredential{}, nil
			})

		assert.False(t, completed)
	})
}

// TestSubscriberCredentialIssuanceBudget_IsTheFiveSecondsTheRequirementNames pins
// the production value.
//
// The tests above shorten the budget so they run fast, which means none of them
// would notice the constant being changed. AAP R-7 names five seconds, the handler
// and the service both read this one constant so they cannot drift apart, and this
// is what fails if it moves.
func TestSubscriberCredentialIssuanceBudget_IsTheFiveSecondsTheRequirementNames(t *testing.T) {
	assert.Equal(t, 5*time.Second, blnk.SubscriberCredentialIssuanceBudget,
		"the credential endpoint's ceiling is a stated requirement, not a tuning parameter")
}

// TestListSubscribers_RefusesAnUnsupportedQueryParameter is C-13's closed set.
//
// # The defect
//
// The parameter set was open, so "?limitt=5", "?status=active" and
// "?topic=blnk.balances" were all accepted in silence and answered with the whole
// first page. An operator running a migration report cannot distinguish that from a
// correct answer, and would read "every subscriber" as "every subscriber matching my
// filter" — the more dangerous reading of the two, because it looks complete.
//
// The message names every offending parameter at once and lists the accepted set, so
// a caller fixes one request rather than discovering its parameters one round trip at
// a time.
func TestListSubscribers_RefusesAnUnsupportedQueryParameter(t *testing.T) {
	for name, target := range map[string]string{
		"a misspelled page bound":                         "/subscribers?limitt=5",
		"a filter that does not exist":                    "/subscribers?status=active",
		"a filter borrowed from another endpoint":         "/subscribers?topic=blnk.balances",
		"a supported parameter beside an unsupported one": "/subscribers?limit=5&nope=1",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := subscribersRequest(t, http.MethodGet, target)

			assertSubscribersErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")
		})
	}
}

// TestListSubscribers_AcceptsTheSupportedParameters is the negative control on the
// closed set.
//
// A guard that refused a legitimate parameter would break paging while passing the
// test above, and sort_by and sort_order in particular must be accepted-and-inert:
// the repository fixes the ordering because paging stability depends on it, and a
// caller reusing a generic list-endpoint client sends them regardless.
//
// `offset` IS NOT ONE OF THEM, and its absence is asserted below rather than left
// implicit. Paging moved to a keyset cursor (PERF-P08) because the registry is
// written while it is read: an offset can show a row twice or skip it entirely when a
// subscriber is registered between two pages, and its cost grows with its depth.
// Accepting it silently would be the worse failure — a caller would page with it and
// receive a stable-looking sequence that quietly repeats or loses rows — so the closed
// set refuses it and NAMES what it does accept.
func TestListSubscribers_AcceptsTheSupportedParameters(t *testing.T) {
	for _, target := range []string{
		"/subscribers",
		"/subscribers?limit=5",
		"/subscribers?cursor=",
		"/subscribers?sort_by=created_at&sort_order=asc",
		"/subscribers?include_count=false",
	} {
		t.Run(target, func(t *testing.T) {
			recorder := subscribersRequest(t, http.MethodGet, target)

			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
		})
	}

	t.Run("offset is refused, and the refusal names the supported set", func(t *testing.T) {
		for _, target := range []string{
			"/subscribers?offset=10",
			"/subscribers?limit=5&offset=10",
		} {
			recorder := subscribersRequest(t, http.MethodGet, target)

			assertSubscribersErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")
			assert.Contains(t, recorder.Body.String(), "cursor",
				"a caller sent an offset because it did not know about the cursor, so the "+
					"refusal has to name it")
		}
	})
}

// TestListSubscribers_HonoursIncludeCountWithTheEstablishedEnvelope is C-06.
//
// # The defect
//
// include_count was REFUSED with a validation error, because no layer could count the
// registry. Every other counted listing in this API answers {"data":[...],
// "total_count":N}, so a generic client asking for a total against this one endpoint
// received a 400 instead.
//
// The count is a real query rather than the length of the page. That distinction is
// the reason the refusal existed and is worth keeping in mind here: a client reading
// a full page of 20 as "20 exist" stops early, and one comparing a page length
// against itself never stops.
//
// Both shapes are asserted, and the option adds a FIELD rather than changing the
// shape. The body was once a bare JSON array, and include_count wrapped it; the
// envelope became unconditional when paging moved to a keyset cursor (PERF-P08),
// because the position to resume from has to be returned somewhere and there is
// nowhere else in a bare array to put it. So `data` is always present, `next_cursor`
// and `has_more` are always meaningful, and include_count adds `total_count`.
func TestListSubscribers_HonoursIncludeCountWithTheEstablishedEnvelope(t *testing.T) {
	t.Run("without include_count the envelope carries no total", func(t *testing.T) {
		recorder := subscribersRequest(t, http.MethodGet, "/subscribers?limit=1")
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var envelope struct {
			Data       []json.RawMessage `json:"data"`
			HasMore    bool              `json:"has_more"`
			TotalCount *int64            `json:"total_count"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope),
			"body: %s", recorder.Body.String())

		assert.NotNil(t, envelope.Data,
			"the page must be present as data, and as [] rather than null when empty")
		assert.Nil(t, envelope.TotalCount,
			"nobody asked for a total, and a pointer is what keeps not-requested "+
				"distinguishable from zero")
	})

	t.Run("with include_count the body is the data/total_count envelope", func(t *testing.T) {
		recorder := subscribersRequest(t, http.MethodGet, "/subscribers?limit=1&include_count=true")
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var envelope struct {
			Data       []json.RawMessage `json:"data"`
			TotalCount *int64            `json:"total_count"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope),
			"body: %s", recorder.Body.String())

		assert.NotNil(t, envelope.Data,
			"the page must be present as data, and as [] rather than null when empty")
		require.NotNil(t, envelope.TotalCount,
			"the total must be present: a null here is the refusal this replaced wearing a 200")
		assert.GreaterOrEqual(t, *envelope.TotalCount, int64(len(envelope.Data)),
			"the total counts the whole registry, so it can never be smaller than the page drawn "+
				"from it; a total equal to the page length whenever the page is full is the "+
				"len(items) falsehood a paging client loops on")
	})
}

// TestListSubscribers_ParsesIncludeCountStrictly is the other half of C-13.
//
// ParseQueryOptions reads include_count as `value == "true"`, so "TRUE", "True" and
// "1" all meant false and the caller received a bare array with nothing to indicate
// its spelling had been ignored. A misspelling that silently changes the response
// SHAPE is worse than one that is refused, so the accepted vocabulary is
// strconv.ParseBool's and everything else is a validation error.
//
// api/filter_helper.go is not modified: tightening ParseQueryOptions would change
// every listing in the API, including endpoints no finding covers.
func TestListSubscribers_ParsesIncludeCountStrictly(t *testing.T) {
	t.Run("the counted spellings all produce a total", func(t *testing.T) {
		for _, value := range []string{"true", "TRUE", "True", "t", "1"} {
			recorder := subscribersRequest(t, http.MethodGet,
				"/subscribers?limit=1&include_count="+value)
			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

			var envelope struct {
				TotalCount *int64 `json:"total_count"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
			assert.NotNil(t, envelope.TotalCount,
				"%q asked for a count and must receive one; reading it as false is the silent "+
					"shape change this closes", value)
		}
	})

	t.Run("the uncounted spellings all omit the total", func(t *testing.T) {
		// The empty and whitespace-only values are here rather than among the refusals:
		// not asking for a count is the ordinary request, and "?include_count=" is what
		// an HTTP client serialising an unset option produces.
		for _, value := range []string{"false", "FALSE", "False", "f", "0", "", "%20"} {
			recorder := subscribersRequest(t, http.MethodGet,
				"/subscribers?limit=1&include_count="+value)
			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

			var envelope struct {
				Data       []json.RawMessage `json:"data"`
				TotalCount *int64            `json:"total_count"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope),
				"body: %s", recorder.Body.String())
			assert.NotNil(t, envelope.Data, "%q must still return a page", value)
			assert.Nil(t, envelope.TotalCount,
				"%q did not ask for a count, so total_count must be absent", value)
		}
	})

	t.Run("an unparseable value is refused rather than read as false", func(t *testing.T) {
		for _, value := range []string{"yes", "no", "2", "maybe", "truthy"} {
			recorder := subscribersRequest(t, http.MethodGet,
				"/subscribers?limit=1&include_count="+value)

			assertSubscribersErrorCode(t, recorder, http.StatusBadRequest, "GEN_VALIDATION_ERROR")
		}
	})
}

// TestCreateSubscriber_AnswersAValidationErrorForAMissingName is C-24.
//
// # The defect
//
// name carried binding:"required", so the BINDER refused a body that omitted it and
// the handler answered GEN_MALFORMED_REQUEST — "this body could not be read". The
// endpoint's contract, and every other rule its DTO applies, answers
// GEN_VALIDATION_ERROR — "this body was read and one field is wrong". A caller
// distinguishing a transport problem from a field problem was told the wrong one.
//
// It also split one rule across two mechanisms: an OMITTED name was refused by the
// binder and a WHITESPACE-ONLY one by the DTO, so the same broken rule produced two
// codes depending on how the client spelled it. Both now take the DTO path.
//
// A genuinely unbindable body must still answer GEN_MALFORMED_REQUEST, or the fix
// would have removed the distinction rather than corrected it — hence the last case.
func TestCreateSubscriber_AnswersAValidationErrorForAMissingName(t *testing.T) {
	post := func(t *testing.T, body string) *httptest.ResponseRecorder {
		t.Helper()

		router, _ := setupAuthedRouter(t, true, nil)

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/subscribers", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)

		return recorder
	}

	t.Run("an omitted name", func(t *testing.T) {
		assertSubscribersErrorCode(t, post(t, `{"authorized_topics":[]}`),
			http.StatusBadRequest, "GEN_VALIDATION_ERROR")
	})

	t.Run("an explicitly empty name", func(t *testing.T) {
		assertSubscribersErrorCode(t, post(t, `{"name":""}`),
			http.StatusBadRequest, "GEN_VALIDATION_ERROR")
	})

	t.Run("a whitespace-only name, which the binder never refused", func(t *testing.T) {
		// This case always reached the DTO, and it is the one that shows the two paths
		// now agree: it answered GEN_VALIDATION_ERROR before and must still.
		assertSubscribersErrorCode(t, post(t, `{"name":"   "}`),
			http.StatusBadRequest, "GEN_VALIDATION_ERROR")
	})

	t.Run("an unbindable body is still a malformed request", func(t *testing.T) {
		assertSubscribersErrorCode(t, post(t, `{"name":`),
			http.StatusBadRequest, "GEN_MALFORMED_REQUEST")
	})

	t.Run("a field of the wrong type is still a malformed request", func(t *testing.T) {
		// The binder still owns TYPE errors, which is the distinction worth keeping: a
		// number where a string belongs is a body that could not be read as this shape.
		assertSubscribersErrorCode(t, post(t, `{"name":42}`),
			http.StatusBadRequest, "GEN_MALFORMED_REQUEST")
	})
}

// TestSubscriberCRUD_CarriesNoLegacyWebhookState is C-01 at the HTTP boundary.
//
// # The defect
//
// The four `/subscribers/:id/webhook-subscription` routes are each fronted by
// middleware.WebhookSunsetGuard and answer 410 Gone after the retirement instant.
// The GENERAL subscriber routes are not deprecated and are not guarded — and they
// accepted `webhook_url` on create and update, and echoed it on every read.
//
// So the sunset was bypassable by choosing a different route: a caller could keep
// writing and reading legacy webhook state indefinitely after the surface that owns
// it had been retired. A sunset with a way around it is not a sunset.
//
// # What is asserted, and why it is the shape rather than a status
//
// A request carrying an unknown JSON key is not refused — encoding/json ignores it —
// so the guarantee cannot be "sending webhook_url is a 400". It is that the field has
// no EFFECT and is never DISCLOSED: the create body has nowhere to put it, and no
// read projection reports it. Both halves are checked, because either alone leaves
// the bypass open in one direction.
func TestSubscriberCRUD_CarriesNoLegacyWebhookState(t *testing.T) {
	t.Run("the registration body has no webhook_url to accept", func(t *testing.T) {
		// The struct's JSON shape is the contract a client codes against. Asserted here
		// as well as in the DTO's own contract test because this is the layer where the
		// bypass was reachable.
		body, err := json.Marshal(model.CreateSubscriber{Name: "ledger-ops"})
		require.NoError(t, err)
		assert.NotContains(t, string(body), "webhook_url",
			"POST /subscribers is not guarded by the sunset, so it must not be a way to write "+
				"legacy webhook state")
	})

	t.Run("the update body has no webhook_url to accept", func(t *testing.T) {
		body, err := json.Marshal(model.UpdateSubscriber{})
		require.NoError(t, err)
		assert.NotContains(t, string(body), "webhook_url")
	})

	t.Run("no subscriber read projection discloses one", func(t *testing.T) {
		// Built from a row that DOES carry a URL, so this tests removal rather than the
		// absence of a fixture value.
		endpoint := "https://hooks.example.com/blnk"
		migrated := time.Now().UTC()

		body, err := json.Marshal(model.NewSubscriberResponse(coremodel.EventSubscriber{
			SubscriberID:     "sub_9f1c8a72",
			Name:             "ledger-ops",
			AuthorizedTopics: []string{"blnk.transactions"},
			WebhookURL:       &endpoint,
			MigratedAt:       &migrated,
		}))
		require.NoError(t, err)

		assert.NotContains(t, string(body), "webhook_url",
			"the general read is unguarded, so disclosing the endpoint through it would keep it "+
				"readable after the guarded route had begun answering 410")
		assert.NotContains(t, string(body), endpoint,
			"and not the value under any other key either")

		// migrated_at IS reported, deliberately: it is migration progress about this
		// deployment rather than legacy state, and a progress report needs it on both
		// sides of the sunset.
		assert.Contains(t, string(body), "migrated_at")
	})

	t.Run("a request that sends webhook_url anyway has no effect", func(t *testing.T) {
		// encoding/json ignores unknown keys, so this cannot be a refusal. What it must
		// not be is a write — which is guaranteed by there being no field to decode into,
		// and is asserted here so a future field addition breaks this test rather than
		// silently reopening the bypass.
		var request model.CreateSubscriber
		require.NoError(t, json.Unmarshal(
			[]byte(`{"name":"ledger-ops","webhook_url":"https://hooks.example.com/blnk"}`),
			&request,
		))
		assert.Equal(t, "ledger-ops", request.Name, "the recognised fields still decode")

		reencoded, err := json.Marshal(request)
		require.NoError(t, err)
		assert.NotContains(t, string(reencoded), "hooks.example.com",
			"the value must not survive decoding into the request shape")
	})
}

// subscribersRouter builds a real router for the /subscribers surface, reusing the /events
// helper so both files share one construction path.
func subscribersRouter(t *testing.T, isMaster bool) *gin.Engine {
	t.Helper()

	return eventsRouter(t, isMaster)
}

// subscriberRequest performs one request against the subscriber surface.
func subscriberRequest(
	t *testing.T, router *gin.Engine, method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	// A LOOPBACK PEER, because the credential endpoint refuses to put a one-time secret on a
	// channel it cannot establish as confidential. httptest.NewRequest records 192.0.2.1 — a
	// documentation address, deliberately not loopback — so without this every issuance test
	// would assert against SUBSCRIBER_INSECURE_TRANSPORT rather than against the handler.
	// Loopback is the honest one of the three confidential channels to claim here: no TLS was
	// terminated and no proxy is declared, but the bytes genuinely never reach a network.
	request.RemoteAddr = "127.0.0.1:54321"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)

	return w
}

// uniqueSubscriberID returns a canonical identifier no other test run uses.
//
// The registry is a shared real table and the identifier is unique-indexed, so a fixed value
// would make this file fail on its second run rather than on a defect. model's own generator
// is used so the value is canonical by construction — the principal and the consumer group
// are derived from it, and a non-canonical id is refused at registration.
func uniqueSubscriberID() string {
	return coremodel.GenerateSubscriberID()
}

// deleteSubscriber removes a subscriber through the API, for cleanup.
//
// Cleanup failures are REPORTED rather than ignored: a leaked row is a real registry row that
// the next run's list assertions have to tolerate, and a silent leak is how a suite becomes
// order-dependent. A 404 is accepted because the test may already have deleted it.
func deleteSubscriber(t *testing.T, router *gin.Engine, subscriberID string) {
	t.Helper()

	w := subscriberRequest(t, router, http.MethodDelete, "/subscribers/"+subscriberID, "")
	if w.Code != http.StatusOK && w.Code != http.StatusNoContent && w.Code != http.StatusNotFound {
		t.Errorf("cleanup: deleting subscriber %s answered %d: %s",
			subscriberID, w.Code, w.Body.String())
	}
}

// grantableTopic returns one topic a subscriber may legitimately be granted.
//
// It is read from the model rather than written as a literal, so a change to the topic
// catalogue cannot leave this file granting a topic that is no longer grantable — which would
// fail as a validation error and read as a routing problem.
func grantableTopic(t *testing.T) string {
	t.Helper()

	topics := coremodel.SubscriberGrantableTopics("blnk")
	require.NotEmpty(t, topics, "the catalogue must expose at least one grantable topic")

	return topics[0]
}

// TestSubscribersAPI_RoutesAreReachableAndMasterKeyGated is the authorization matrix for the
// endpoints' own master-key gate: every one of the six routes must refuse a non-master caller
// and admit the master key.
//
// THE RESOURCE MAP IS NOT PROVEN HERE. Secure mode is off, so the auth middleware returns early
// without consulting pathToResource, and an unmapped "subscribers" prefix would pass everything
// below. TestEventsAPI_PathPrefixesResolveToAuthorizationResources covers both surfaces' prefixes
// in secure mode with a non-master key, which is the only combination that reaches the map.
func TestSubscribersAPI_RoutesAreReachableAndMasterKeyGated(t *testing.T) {
	for name, target := range map[string]struct {
		method string
		path   string
		body   string
	}{
		"create":              {http.MethodPost, "/subscribers", `{"name":"probe"}`},
		"list":                {http.MethodGet, "/subscribers", ""},
		"read":                {http.MethodGet, "/subscribers/sub_probe", ""},
		"update":              {http.MethodPut, "/subscribers/sub_probe", `{"name":"renamed"}`},
		"delete":              {http.MethodDelete, "/subscribers/sub_probe", ""},
		"credential issuance": {http.MethodPost, "/subscribers/sub_probe/kafka-credentials", ""},
	} {
		t.Run(name+" refuses a non-master caller", func(t *testing.T) {
			w := subscriberRequest(t, subscribersRouter(t, false), target.method, target.path, target.body)

			assertErrorCode(t, w, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
		})

		t.Run(name+" is reachable by the master key", func(t *testing.T) {
			w := subscriberRequest(t, subscribersRouter(t, true), target.method, target.path, target.body)

			require.NotEqual(t, http.StatusForbidden, w.Code,
				"the master key must pass the endpoint's own gate. body: %s", w.Body.String())
			// A backstop only, for the reason in this function's comment.
			assert.NotContains(t, w.Body.String(), string(apierror.ErrAuthUnknownResource),
				"%s %s was aborted as an unknown authorization resource", target.method, target.path)
		})
	}
}

// TestSubscribersAPI_CRUDThroughTheRealRouter walks the whole lifecycle over HTTP.
//
// Each step reads the state back through the API rather than through the service, because what
// was unverified is the HTTP layer: the binding, the derived identity in the response, the
// status codes, and the JSON field names a client is written against.
func TestSubscribersAPI_CRUDThroughTheRealRouter(t *testing.T) {
	router := subscribersRouter(t, true)
	subscriberID := uniqueSubscriberID()
	topic := grantableTopic(t)

	t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })

	t.Run("create", func(t *testing.T) {
		body := fmt.Sprintf(
			`{"subscriber_id":%q,"name":"settlement consumer","authorized_topics":[%q]}`,
			subscriberID, topic,
		)

		w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
		require.Equal(t, http.StatusCreated, w.Code,
			"a created resource answers 201. body: %s", w.Body.String())

		var created map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

		assert.Equal(t, subscriberID, created["subscriber_id"])
		assert.Equal(t, "settlement consumer", created["name"])

		// The identity is DERIVED, never echoed from the request, so the response is where a
		// caller learns the principal and group its ACLs are bound to.
		principal, err := coremodel.CanonicalKafkaPrincipal(subscriberID)
		require.NoError(t, err)
		assert.Equal(t, principal, created["kafka_principal"],
			"the principal must be derived from the identifier, not chosen")

		group, err := coremodel.CanonicalConsumerGroupID(subscriberID)
		require.NoError(t, err)
		assert.Equal(t, group, created["consumer_group_id"])

		// NO SECRET may appear on a registration response: none has been minted.
		assert.NotContains(t, strings.ToLower(w.Body.String()), `"password"`,
			"registration mints no credential, so no password field may exist")
	})

	t.Run("read", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers/"+subscriberID, "")
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		var read map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &read))
		assert.Equal(t, subscriberID, read["subscriber_id"])

		// The enforced-access declaration must be present on every subscriber response, and it
		// is the object that keeps the key scope legible.
		enforced, ok := read["enforced_access"].(map[string]interface{})
		require.True(t, ok, "every subscriber response carries enforced_access: %s", w.Body.String())
		assert.Equal(t, false, enforced["partition_key_prefix_enforced"],
			"this field may never be true: Kafka has no message-key authorization dimension")
		assert.Empty(t, enforced["partition_key_prefix"],
			"a subscriber with no recorded prefix has nothing here: this field is what a consumer "+
				"FILTERS on, so a sentinel standing for \"no restriction\" would be a filter matching "+
				"nothing and a client applying it would discard its entire stream. The English form "+
				"of no-restriction belongs to the credential's KeyScope, which returns the scope and "+
				"its enforcer as a pair and cannot be mistaken for a filter")
		assert.Equal(t, string(coremodel.KeyScopeEnforcementNone), enforced["partition_key_prefix_enforced_by"],
			"and the enforcement point is what states the absence positively")
	})

	t.Run("update", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodPut, "/subscribers/"+subscriberID,
			`{"name":"renamed consumer"}`)
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		var updated map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &updated))
		assert.Equal(t, "renamed consumer", updated["name"])
	})

	t.Run("list includes it", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers?limit=100", "")
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		assert.Contains(t, w.Body.String(), subscriberID,
			"a registered subscriber must appear in the listing")
	})

	t.Run("delete", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodDelete, "/subscribers/"+subscriberID, "")
		require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, w.Code,
			"body: %s", w.Body.String())

		after := subscriberRequest(t, router, http.MethodGet, "/subscribers/"+subscriberID, "")
		assert.Equal(t, http.StatusNotFound, after.Code,
			"a deleted subscriber must be gone, or the delete only appeared to work")
	})
}

// TestSubscribersAPI_RecordsAndReturnsTheKeyScope is the F-05 contract at the HTTP boundary.
//
// A subscriber registered WITH a key scope must be accepted, and every response about it must
// carry the scope beside the statement that the broker does not enforce it. The adjacency is
// the property: the scope alone would read as a limit on the credential's reach.
func TestSubscribersAPI_RecordsAndReturnsTheKeyScope(t *testing.T) {
	router := subscribersRouter(t, true)
	subscriberID := uniqueSubscriberID()
	topic := grantableTopic(t)
	keyScope := "ldg_" + subscriberID

	t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })

	body := fmt.Sprintf(
		`{"subscriber_id":%q,"name":"key scoped consumer","authorized_topics":[%q],"partition_key_prefix":%q}`,
		subscriberID, topic, keyScope,
	)

	w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
	require.Equal(t, http.StatusCreated, w.Code,
		"a key scope must be ACCEPTED at registration. body: %s", w.Body.String())

	var created map[string]interface{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

	assert.Equal(t, keyScope, created["partition_key_prefix"],
		"the recorded scope must be reported, or an operator cannot see what was stored")

	enforced, ok := created["enforced_access"].(map[string]interface{})
	require.True(t, ok, "body: %s", w.Body.String())
	assert.Equal(t, keyScope, enforced["partition_key_prefix"],
		"and the scope must appear in enforced_access, which is where a consumer reads it")
	assert.Equal(t, false, enforced["partition_key_prefix_enforced"],
		"BESIDE the statement that the broker does not keep it — the two cannot be read apart")

	// The enforced dimensions must remain exactly the two the broker evaluates. A key entry
	// here would assert an ACL that cannot exist.
	dimensions, ok := enforced["enforced_by"].([]interface{})
	require.True(t, ok)
	assert.NotContains(t, dimensions, "partition_key")
	assert.NotContains(t, dimensions, "partition_key_prefix")
}

// TestSubscribersAPI_RefusesAnUngrantableTopic covers the grant validation at the boundary.
//
// Dead-letter topics and the internal system category are Blnk's own; granting one to a
// subscriber would hand it Blnk's failure stream. The refusal must be a 400-class validation
// error rather than a 500, because it is entirely a property of the request.
func TestSubscribersAPI_RefusesAnUngrantableTopic(t *testing.T) {
	router := subscribersRouter(t, true)

	for name, topic := range map[string]string{
		"a dead-letter topic": "blnk.transactions.dlt",

		"a topic outside the prefix": "someone.else.transactions",
		"an invented topic":          "blnk.not-a-category",
	} {
		t.Run(name, func(t *testing.T) {
			subscriberID := uniqueSubscriberID()
			body := fmt.Sprintf(
				`{"subscriber_id":%q,"name":"probe","authorized_topics":[%q]}`, subscriberID, topic,
			)

			w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)

			require.NotEqual(t, http.StatusCreated, w.Code,
				"%q must not be grantable: it is Blnk's own, not a subscriber's. body: %s",
				topic, w.Body.String())
			assert.Equal(t, http.StatusBadRequest, w.Code,
				"an ungrantable topic is a request-side error. body: %s", w.Body.String())

			if w.Code == http.StatusCreated {
				deleteSubscriber(t, router, subscriberID)
			}
		})
	}
}

// TestSubscribersAPI_RefusesAMalformedRegistration covers the binder and the DTO validation.
//
// Name is the one field the service cannot invent, and the topic-count and length caps live in
// the BINDING TAGS so they bound allocation before any handler code runs. Each of these must be
// a 400 rather than a 500 or a silent success.
func TestSubscribersAPI_RefusesAMalformedRegistration(t *testing.T) {
	router := subscribersRouter(t, true)

	oversizedGrant := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		oversizedGrant = append(oversizedGrant, fmt.Sprintf("%q", "blnk.transactions"))
	}

	for name, body := range map[string]string{
		"an unparseable body":      `{`,
		"no name at all":           `{"subscriber_id":"sub_probe"}`,
		"an empty name":            `{"subscriber_id":"sub_probe","name":""}`,
		"a non-canonical id":       `{"subscriber_id":"Not Canonical!","name":"probe"}`,
		"more topics than the cap": `{"name":"probe","authorized_topics":[` + strings.Join(oversizedGrant, ",") + `]}`,
	} {
		t.Run(name, func(t *testing.T) {
			w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)

			require.NotEqual(t, http.StatusCreated, w.Code,
				"a malformed registration must not create a subscriber. body: %s", w.Body.String())
			assert.Equal(t, http.StatusBadRequest, w.Code,
				"and it must be refused as a request error, never a server fault. body: %s",
				w.Body.String())
		})
	}
}

// TestSubscribersAPI_ReadAndUpdateRefuseAnUnknownSubscriber pins the not-found path.
//
// An identifier that matches nothing must be a clean 404. It is worth asserting at the router
// because the alternative — a 500 from a nil dereference in a projection — is the shape this
// class of bug usually takes.
func TestSubscribersAPI_ReadAndUpdateRefuseAnUnknownSubscriber(t *testing.T) {
	router := subscribersRouter(t, true)
	missing := uniqueSubscriberID()

	for name, target := range map[string]struct {
		method string
		body   string
	}{
		"read":   {http.MethodGet, ""},
		"update": {http.MethodPut, `{"name":"renamed"}`},
	} {
		t.Run(name, func(t *testing.T) {
			w := subscriberRequest(t, router, target.method, "/subscribers/"+missing, target.body)

			assertErrorCode(t, w, http.StatusNotFound, apierror.ErrSubscriberNotFound)
		})
	}
}

// TestSubscribersAPI_CredentialIssuanceRefusesWithoutABroker covers the credential endpoint's
// HTTP contract in the configuration the ordinary suite runs in.
//
// No broker is configured here, so the endpoint must refuse with the typed
// EVENT_KAFKA_UNAVAILABLE — a 503, retryable, and emphatically not a 500. That distinction is
// the whole reason the code exists: an operator seeing 500 looks for a defect in Blnk, while
// 503 names the dependency. The broker-backed success path is owned by
// event_isolation_integration_test.go, which skips without a broker; asserting it here would
// make this file skip in the ordinary suite.
func TestSubscribersAPI_CredentialIssuanceRefusesWithoutABroker(t *testing.T) {
	router := subscribersRouter(t, true)
	subscriberID := uniqueSubscriberID()
	topic := grantableTopic(t)

	t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })

	body := fmt.Sprintf(
		`{"subscriber_id":%q,"name":"credential probe","authorized_topics":[%q]}`, subscriberID, topic,
	)
	created := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	started := time.Now()
	w := subscriberRequest(t, router, http.MethodPost,
		"/subscribers/"+subscriberID+"/kafka-credentials", "")
	elapsed := time.Since(started)

	require.NotEqual(t, http.StatusInternalServerError, w.Code,
		"an unconfigured dependency is not a server defect, and answering 500 sends an operator "+
			"to look for one. body: %s", w.Body.String())
	assert.Equal(t, http.StatusServiceUnavailable, w.Code,
		"503 names the dependency and says a retry may succeed. body: %s", w.Body.String())

	// THE FIVE-SECOND BUDGET, asserted as HTTP behaviour: the request ENDS within it. The
	// requirement is that provisioning completes inside five seconds, and the endpoint's
	// contract is that an expiry is answered rather than waited out — so no request may hang.
	assert.Less(t, elapsed, blnk.SubscriberCredentialIssuanceBudget+2*time.Second,
		"the endpoint must answer within its own budget rather than holding the request open")

	// NO SECRET on a failed issuance, which is the invariant that survives every failure path.
	assert.NotContains(t, w.Body.String(), `"password"`,
		"a refused issuance returns no credential field at all")
}

// TestSubscribersAPI_CredentialIssuanceRefusesABlankIdentifier pins the parameter guard on the
// one route that mints a secret.
func TestSubscribersAPI_CredentialIssuanceRefusesABlankIdentifier(t *testing.T) {
	router := subscribersRouter(t, true)

	for name, path := range map[string]string{
		"an empty identifier":        "/subscribers//kafka-credentials",
		"a whitespace identifier":    "/subscribers/%20/kafka-credentials",
		"a non-canonical identifier": "/subscribers/Not%20Canonical!/kafka-credentials",
	} {
		t.Run(name, func(t *testing.T) {
			w := subscriberRequest(t, router, http.MethodPost, path, "")

			assert.NotEqual(t, http.StatusOK, w.Code,
				"a credential must never be minted for an identifier no identity derives from. "+
					"body: %s", w.Body.String())
			assert.NotEqual(t, http.StatusInternalServerError, w.Code,
				"and the refusal must be typed, not a server fault. body: %s", w.Body.String())
		})
	}
}

// TestSubscribersAPI_ListPagingIsBounded keeps one request from reading the whole registry.
func TestSubscribersAPI_ListPagingIsBounded(t *testing.T) {
	router := subscribersRouter(t, true)

	t.Run("a valid page is accepted", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers?limit=5", "")
		assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	})

	t.Run("an unparseable limit is refused", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers?limit=many", "")
		assert.Equal(t, http.StatusBadRequest, w.Code,
			"a value that does not parse must be refused rather than defaulted. body: %s",
			w.Body.String())
	})

	t.Run("the listing is a JSON array", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers?limit=1", "")
		require.Equal(t, http.StatusOK, w.Code)

		trimmed := strings.TrimSpace(w.Body.String())
		assert.True(t, strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{"),
			"the listing must be an array, or an object carrying one: %s", trimmed)
	})
}

// TestSubscribersAPI_NeverReturnsTheCredentialReference is the secret-handling assertion at the
// HTTP boundary, and it is the one that would matter most if it failed.
//
// The registry stores a non-reversible reference. It is not a secret, but it is not a caller's
// business either, and the response contract reduces it to a short fingerprint in exactly one
// place so no handler can return the raw value by writing the obvious assignment.
func TestSubscribersAPI_NeverReturnsTheCredentialReference(t *testing.T) {
	router := subscribersRouter(t, true)
	subscriberID := uniqueSubscriberID()

	t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })

	body := fmt.Sprintf(`{"subscriber_id":%q,"name":"reference probe"}`, subscriberID)
	created := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	for name, w := range map[string]*httptest.ResponseRecorder{
		"create": created,
		"read":   subscriberRequest(t, router, http.MethodGet, "/subscribers/"+subscriberID, ""),
		"list":   subscriberRequest(t, router, http.MethodGet, "/subscribers?limit=100", ""),
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotContains(t, w.Body.String(), `"credential_reference"`,
				"the raw reference must never reach a response body; the fingerprint is what a "+
					"caller gets")
			assert.NotContains(t, w.Body.String(), `"password"`,
				"and no response on this surface carries a secret")
		})
	}
}

// webhookSubscriptionPath returns the deprecated route for one subscriber.
//
// It is built from the exported route pattern rather than written as a literal so this file
// cannot drift from api/api.go's registration: the pattern is the single source of truth the
// router, the sunset interceptor and these tests all read.
func webhookSubscriptionPath(subscriberID string) string {
	return strings.Replace(
		middleware.DeprecatedWebhookSubscriptionRoute, ":subscriber_id", subscriberID, 1,
	)
}

// registerSubscriberForWebhookTests creates a subscriber and schedules its removal.
func registerSubscriberForWebhookTests(t *testing.T, router *gin.Engine) string {
	t.Helper()

	subscriberID := uniqueSubscriberID()
	t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })

	body := fmt.Sprintf(
		`{"subscriber_id":%q,"name":"migrating consumer","authorized_topics":[%q]}`,
		subscriberID, grantableTopic(t),
	)

	w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
	require.Equal(t, http.StatusCreated, w.Code,
		"the lifecycle tests need a registered subscriber. body: %s", w.Body.String())

	return subscriberID
}

// TestSubscribersAPI_WebhookSubscriptionLifecycleThroughTheRealRouter walks the deprecated
// surface end to end while the dual-delivery window is open.
//
// The router is built with NO retirement instant, so the sunset interceptor passes the request
// through and the handlers actually run. That is the only configuration in which these four
// handler bodies are reachable at all, which is why the coverage they carry cannot come from
// the sunset file.
//
// The delete step is the one worth reading closely. The handler clears the URL and then stamps
// the migration instant as two separate writes, in that order, so that a failure between them
// leaves a subscriber reported as awaiting migration rather than as migrated. The read after
// the delete asserts BOTH halves landed, because a 204 alone would be satisfied by a clear that
// never stamped.
func TestSubscribersAPI_WebhookSubscriptionLifecycleThroughTheRealRouter(t *testing.T) {
	router := subscribersRouter(t, true)
	subscriberID := registerSubscriberForWebhookTests(t, router)
	path := webhookSubscriptionPath(subscriberID)

	// The two URLs share no prefix. A replacement that merely extended the original would
	// make the "previous endpoint is gone" assertion below pass on a substring match even if
	// the old value were still stored.
	const recorded = "https://first.example.com/blnk-events"
	const replacement = "https://second.example.net/kafka-cutover"

	t.Run("read before anything is recorded is 200 with no url", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, w.Code,
			"an existing subscriber with an empty subscription record is 200, not 404. body: %s",
			w.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, subscriberID, body["subscriber_id"])
		assert.NotContains(t, body, "webhook_url",
			"an absent URL is omitted rather than returned blank")
		assert.NotContains(t, body, "migrated_at",
			"a subscriber that has not migrated carries no instant")
	})

	t.Run("record", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodPost, path,
			fmt.Sprintf(`{"webhook_url":%q}`, recorded))
		require.Equal(t, http.StatusCreated, w.Code, "body: %s", w.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, subscriberID, body["subscriber_id"])
		assert.Equal(t, recorded, body["webhook_url"])
		assert.NotContains(t, body, "migrated_at",
			"recording an endpoint is not migrating away from it")
	})

	t.Run("read returns what was recorded", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, recorded, body["webhook_url"],
			"the read must reflect the write, or the write only appeared to land")
	})

	t.Run("replace", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodPut, path,
			fmt.Sprintf(`{"webhook_url":%q}`, replacement))
		require.Equal(t, http.StatusOK, w.Code,
			"a replacement is 200, not 201. body: %s", w.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.Equal(t, replacement, body["webhook_url"])

		read := subscriberRequest(t, router, http.MethodGet, path, "")
		assert.Contains(t, read.Body.String(), replacement)
		assert.NotContains(t, read.Body.String(), recorded,
			"the previous endpoint must be gone, not merely shadowed")
	})

	t.Run("forget clears the url and stamps the migration", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodDelete, path, "")
		require.Equal(t, http.StatusNoContent, w.Code, "body: %s", w.Body.String())
		assert.Empty(t, strings.TrimSpace(w.Body.String()),
			"204 carries no body")

		read := subscriberRequest(t, router, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, read.Code,
			"the SUBSCRIBER survives; only its subscription was forgotten. body: %s",
			read.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(read.Body.Bytes(), &body))
		assert.NotContains(t, body, "webhook_url",
			"the recorded endpoint must be gone")
		assert.Contains(t, body, "migrated_at",
			"BOTH writes must land: a 204 from a clear that never stamped would leave the "+
				"subscriber reported as still awaiting migration forever")
		assert.NotEmpty(t, body["migrated_at"])
	})

	t.Run("the subscriber itself is still registered", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, "/subscribers/"+subscriberID, "")
		assert.Equal(t, http.StatusOK, w.Code,
			"deleting a webhook subscription must not deregister the subscriber. body: %s",
			w.Body.String())
	})
}

// TestSubscribersAPI_WebhookSubscriptionRefusesAnUnsafeDestination is the SSRF policy at the
// HTTP boundary, applied at the same standard on create and on replace.
//
// This route's whole purpose is to accept a URL, which makes it the most likely way an internal
// address reaches a column that is a future request sink. A policy enforced on create but not on
// replace is a policy with an edit-shaped hole, so every case is asserted against both verbs.
func TestSubscribersAPI_WebhookSubscriptionRefusesAnUnsafeDestination(t *testing.T) {
	router := subscribersRouter(t, true)
	path := webhookSubscriptionPath(registerSubscriberForWebhookTests(t, router))

	for name, testCase := range map[string]struct {
		body string
		code apierror.ErrorCode
	}{
		"plain http":           {`{"webhook_url":"http://subscriber.example.com/x"}`, apierror.ErrGenValidation},
		"loopback host":        {`{"webhook_url":"https://127.0.0.1/x"}`, apierror.ErrGenValidation},
		"private address":      {`{"webhook_url":"https://10.0.0.5/x"}`, apierror.ErrGenValidation},
		"link local metadata":  {`{"webhook_url":"https://169.254.169.254/latest"}`, apierror.ErrGenValidation},
		"surrounding space":    {`{"webhook_url":" https://subscriber.example.com/x"}`, apierror.ErrGenValidation},
		"no host":              {`{"webhook_url":"https:///x"}`, apierror.ErrGenValidation},
		"missing field":        {`{}`, apierror.ErrGenMalformedRequest},
		"blank field":          {`{"webhook_url":""}`, apierror.ErrGenMalformedRequest},
		"unparseable body":     {`{"webhook_url":`, apierror.ErrGenMalformedRequest},
		"wrong type for a url": {`{"webhook_url":42}`, apierror.ErrGenMalformedRequest},
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			t.Run(name+" via "+method, func(t *testing.T) {
				w := subscriberRequest(t, router, method, path, testCase.body)
				assertErrorCode(t, w, http.StatusBadRequest, testCase.code)
			})
		}
	}

	t.Run("nothing was stored by any refusal", func(t *testing.T) {
		w := subscriberRequest(t, router, http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

		var body map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		assert.NotContains(t, body, "webhook_url",
			"a refused URL must not be persisted; a 400 that still wrote is the worse defect")
	})
}

// TestSubscribersAPI_WebhookSubscriptionRefusesAnUnknownSubscriber pins the not-found path on
// all four verbs.
//
// The identifier is canonical, so the refusal can only come from the registry rather than from
// identifier validation — which is what makes this a not-found assertion and not a format one.
func TestSubscribersAPI_WebhookSubscriptionRefusesAnUnknownSubscriber(t *testing.T) {
	router := subscribersRouter(t, true)
	path := webhookSubscriptionPath(uniqueSubscriberID())

	for _, target := range []struct {
		method string
		body   string
	}{
		{http.MethodPost, `{"webhook_url":"https://subscriber.example.com/x"}`},
		{http.MethodGet, ""},
		{http.MethodPut, `{"webhook_url":"https://subscriber.example.com/x"}`},
		{http.MethodDelete, ""},
	} {
		t.Run(target.method, func(t *testing.T) {
			w := subscriberRequest(t, router, target.method, path, target.body)
			assertErrorCode(t, w, http.StatusNotFound, apierror.ErrSubscriberNotFound)
		})
	}
}

// TestSubscribersAPI_WebhookSubscriptionIsMasterKeyGated covers the privileged-endpoint gate on
// the deprecated surface.
//
// The gate must hold on all four verbs while the window is open. It is asserted separately from
// the sunset file's authorization matrix because that file proves 410 comes FIRST once the
// instant has passed; this one proves that before the instant the gate is still the thing that
// stops a non-master caller, rather than the surface being open because it is deprecated.
func TestSubscribersAPI_WebhookSubscriptionIsMasterKeyGated(t *testing.T) {
	router := subscribersRouter(t, false)
	path := webhookSubscriptionPath(uniqueSubscriberID())

	for _, target := range []struct {
		method string
		body   string
	}{
		{http.MethodPost, `{"webhook_url":"https://subscriber.example.com/x"}`},
		{http.MethodGet, ""},
		{http.MethodPut, `{"webhook_url":"https://subscriber.example.com/x"}`},
		{http.MethodDelete, ""},
	} {
		t.Run(target.method, func(t *testing.T) {
			w := subscriberRequest(t, router, target.method, path, target.body)
			assertErrorCode(t, w, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)
		})
	}
}

// TestSubscribersAPI_WebhookSubscriptionNeverReturnsTheSigningSecret keeps the deprecated read
// shape from becoming a secret-bearing response.
//
// The legacy transport's signing secret and configured headers are deployment-wide
// configuration, not per-subscriber data. Returning them here would turn migration tracking
// into credential distribution on the one surface that is being retired and therefore attracts
// the least scrutiny.
func TestSubscribersAPI_WebhookSubscriptionNeverReturnsTheSigningSecret(t *testing.T) {
	router := subscribersRouter(t, true)
	path := webhookSubscriptionPath(registerSubscriberForWebhookTests(t, router))

	created := subscriberRequest(t, router, http.MethodPost, path,
		`{"webhook_url":"https://subscriber.example.com/blnk-events"}`)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	read := subscriberRequest(t, router, http.MethodGet, path, "")
	require.Equal(t, http.StatusOK, read.Code, "body: %s", read.Body.String())

	for name, w := range map[string]*httptest.ResponseRecorder{
		"record": created,
		"read":   read,
	} {
		t.Run(name, func(t *testing.T) {
			lowered := strings.ToLower(w.Body.String())
			for _, forbidden := range []string{"secret", "signature", "headers", "password", "token"} {
				assert.NotContains(t, lowered, forbidden,
					"the deprecated read shape carries the URL and the migration instant only")
			}
		})
	}
}
