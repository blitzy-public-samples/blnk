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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
//
// The diagnostics are SANITISED because this helper is used on the credential route as well as on
// the read routes: an assertion expecting a refusal renders its message when the request
// SUCCEEDED, and a successful credential response carries a plaintext SASL password. Decoding
// still runs against the untouched body, so nothing about what is asserted changes.
func assertSubscribersErrorCode(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	require.Equal(t, status, recorder.Code, safeResponseBody(recorder))

	var body struct {
		ErrorDetail struct {
			Code string `json:"code"`
		} `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body), safeResponseBody(recorder))
	assert.Equal(t, code, body.ErrorDetail.Code, safeResponseBody(recorder))
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

// subscribersSlowWorkDrainBudget bounds how long a subtest waits for an ABANDONED
// issuance goroutine to exit after it has been released.
//
// The wait itself is the point — see parkedIssuance — and this is only its ceiling.
// It is generous because the released goroutine has nothing left to do but return,
// so anything approaching this value means it never observed the release at all.
const subscribersSlowWorkDrainBudget = 5 * time.Second

// parkedIssuance builds an issuance fake that BLOCKS until this subtest releases it,
// and guarantees the released goroutine is gone before the subtest ends.
//
// # Why the fakes here must park rather than sleep
//
// issueWithinBudget abandons work that overruns its budget: it returns while the
// goroutine is still running. A fake that stood in for that overrun by sleeping for a
// fixed duration — a bare `<-time.After(time.Minute)` was what these tests used —
// leaves a goroutine parked for the remaining lifetime of the TEST BINARY, holding
// everything it captured. A leaked goroutine is not a harmless one: `go test -race`
// accounts for every live goroutine, a panic dump lists it against whichever test
// happens to be running when the dump is taken, and one leaked per subtest is how a
// package comes to exhaust its own scheduler.
//
// # Why it parks on this channel and NOT on the context it is handed
//
// issueWithinBudget passes the work the SAME context it selects on. Work that
// returned as soon as that context was done would make both of those select arms
// ready at once, and `completed` would then come out true or false depending on the
// scheduler — turning the property under test into a coin flip. The release channel
// is an input this subtest controls, so the abandonment is deterministic.
//
// # What the cleanup adds
//
// Releasing and then WAITING makes the abandonment observable rather than merely
// tolerated: the work is proven to have been left running at the moment
// issueWithinBudget returned, and proven to be gone before the subtest ends. The wait
// is bounded because an unbounded one would hang the whole binary — taking every
// unrelated package with it and reporting nothing, since a killed binary loses its
// buffered output.
//
// The returned started channel closes when the work is entered. Asserting on it is
// what separates "the work was abandoned" from "the work was never invoked", which
// are different behaviours with the same observable result at the call site.
func parkedIssuance(t *testing.T) (
	issue func(context.Context, string) (blnk.SubscriberCredential, error),
	started <-chan struct{},
	hasFinished func() bool,
) {
	t.Helper()

	release := make(chan struct{})
	finished := make(chan struct{})
	begun := make(chan struct{})

	t.Cleanup(func() {
		close(release)

		select {
		case <-finished:
		case <-time.After(subscribersSlowWorkDrainBudget):
			t.Errorf("the abandoned issuance work did not exit within %s of being released, so it "+
				"is still holding whatever it captured", subscribersSlowWorkDrainBudget)
		}
	})

	issue = func(context.Context, string) (blnk.SubscriberCredential, error) {
		defer close(finished)
		close(begun)

		<-release

		return blnk.SubscriberCredential{}, nil
	}

	hasFinished = func() bool {
		select {
		case <-finished:
			return true
		default:
			return false
		}
	}

	return issue, begun, hasFinished
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

	// Blocks past the budget, standing in for an issuance that is still compensating on
	// its own detached context.
	issue, started, _ := parkedIssuance(t)

	began := time.Now()

	credential, err, completed := issueWithinBudget(ctx, "sub_9f1c8a72", issue)

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

		// The slow work parks until this subtest releases it, and its exit is waited for.
		issue, started, hasFinished := parkedIssuance(t)

		_, _, completed := issueWithinBudget(ctx, "sub_9f1c8a72", issue)

		assert.False(t, completed,
			"a caller that has already gone away must not be waited for")

		// The premise: the work really did start. Without this the subtest would pass
		// against an implementation that never invoked the work at all, which is a
		// different behaviour with the same observable result at the call site.
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("the issuance work was never started, so nothing was abandoned and this " +
				"subtest is asserting the wrong property")
		}

		assert.False(t, hasFinished(),
			"the work had already finished when issueWithinBudget returned, so it was WAITED "+
				"for rather than abandoned")
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

// TestManagementBodies_RefuseAFieldTheShapeDoesNotDeclare is the M-9 guard.
//
// gin's ShouldBindJSON discards unknown keys, so a misspelling was accepted and dropped. The
// consequence differs per route, and on two of them it is worse than a no-op:
//
//   - POST /subscribers with "authorised_topics" — the British spelling, or any typo — registered a
//     subscriber authorised for NOTHING and answered 201. The operator holds a subscriber that can
//     obtain a credential and read no topic, and learns it from the consumer rather than the API.
//   - PUT /subscribers/{id} with every field misspelled decoded to a wholly EMPTY update, which is
//     a legitimate instruction meaning "rewrite the row with its own values" — so the answer was
//     200 with the unchanged row, and a caller diffing the response against what they sent could
//     not tell an ignored field from a value the server kept.
//
// A rejected filter and a rejected body field are the same class of problem, and this API already
// refuses an unknown QUERY parameter. The body was the remaining half.
func TestManagementBodies_RefuseAFieldTheShapeDoesNotDeclare(t *testing.T) {
	router := subscribersRouter(t, true)

	cases := map[string]struct {
		method string
		path   string
		body   string
		field  string
	}{
		"registration with a misspelled grant": {
			method: http.MethodPost,
			path:   "/subscribers",
			body:   `{"name":"Acme","authorised_topics":["blnk.transactions"]}`,
			field:  "authorised_topics",
		},
		"registration with an invented field": {
			method: http.MethodPost,
			path:   "/subscribers",
			body:   `{"name":"Acme","webhook_url":"https://hooks.example.com/blnk"}`,
			field:  "webhook_url",
		},
		"update with a misspelled grant": {
			method: http.MethodPut,
			path:   "/subscribers/" + uniqueSubscriberID(),
			body:   `{"authorised_topics":["blnk.transactions"]}`,
			field:  "authorised_topics",
		},
		"update with a misspelled key scope": {
			method: http.MethodPut,
			path:   "/subscribers/" + uniqueSubscriberID(),
			body:   `{"partition_key":"ldg_1"}`,
			field:  "partition_key",
		},
		"webhook registration with an invented field": {
			method: http.MethodPost,
			path:   "/subscribers/" + uniqueSubscriberID() + "/webhook-subscription",
			body:   `{"webhook_url":"https://hooks.example.com/blnk","headers":{"X":"y"}}`,
			field:  "headers",
		},
		"webhook update with an invented field": {
			method: http.MethodPut,
			path:   "/subscribers/" + uniqueSubscriberID() + "/webhook-subscription",
			body:   `{"webhook_url":"https://hooks.example.com/blnk","secret":"s"}`,
			field:  "secret",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := subscriberRequest(t, router, testCase.method, testCase.path, testCase.body)

			require.Equal(t, http.StatusBadRequest, recorder.Code,
				"a field this endpoint cannot honour must be refused, not dropped; body: %s",
				recorder.Body.String())

			// GEN_VALIDATION_ERROR, not GEN_MALFORMED_REQUEST: the body PARSES, and nothing about
			// the transport is wrong. It is the same code an unknown query parameter answers with.
			assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)

			assert.Contains(t, recorder.Body.String(), testCase.field,
				"the refusal must NAME the field, or a caller with six keys cannot tell which one "+
					"was rejected")
		})
	}
}

// TestManagementBodies_StillAcceptWhatTheyDeclare is the other half of M-9: strictness must not
// have narrowed the accepted vocabulary.
//
// A guard that refused a legitimate body would be a worse regression than the gap it closes, and
// the binding tags have to keep running — decoding through encoding/json directly bypasses gin's
// validator, and these shapes depend on it for binding:"required" and for the topic-list bounds.
func TestManagementBodies_StillAcceptWhatTheyDeclare(t *testing.T) {
	router := subscribersRouter(t, true)

	t.Run("every declared field is accepted", func(t *testing.T) {
		subscriberID := uniqueSubscriberID()
		body := fmt.Sprintf(
			`{"subscriber_id":%q,"name":"Acme","authorized_topics":["blnk.transactions"],`+
				`"partition_key_prefix":"ldg_1"}`, subscriberID)

		recorder := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
		require.Equal(t, http.StatusCreated, recorder.Code,
			"a body naming only declared fields must be accepted; body: %s", recorder.Body.String())

		t.Cleanup(func() { deleteSubscriber(t, router, subscriberID) })
	})

	t.Run("a required field is still enforced by its binding tag", func(t *testing.T) {
		// The validator is invoked explicitly after the strict decode, so binding:"required"
		// still refuses. Losing it would have made the webhook routes accept an empty body.
		recorder := subscriberRequest(t, router, http.MethodPost,
			"/subscribers/"+uniqueSubscriberID()+"/webhook-subscription", `{}`)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMalformedRequest)
	})

	t.Run("a topic list beyond the binding bound is still refused", func(t *testing.T) {
		// binding:"max=16,dive,max=249" is what stops a caller submitting a thousand topic names
		// or one longer than Kafka accepts, and it only runs because the validator is invoked
		// explicitly.
		topics := make([]string, 0, 17)
		for i := range 17 {
			topics = append(topics, fmt.Sprintf("blnk.t%d", i))
		}
		encoded, err := json.Marshal(topics)
		require.NoError(t, err)

		recorder := subscriberRequest(t, router, http.MethodPost, "/subscribers",
			`{"name":"Acme","authorized_topics":`+string(encoded)+`}`)

		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMalformedRequest)
	})

	t.Run("malformed JSON is still a transport problem, not a validation one", func(t *testing.T) {
		for name, body := range map[string]string{
			"unparseable": `{"name":`,
			"wrong type":  `{"name":123}`,
			"absent body": ``,
			"two values":  `{"name":"a"}{"name":"b"}`,
		} {
			t.Run(name, func(t *testing.T) {
				recorder := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)

				require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
				assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMalformedRequest)
			})
		}
	})
}

// TestListSubscribers_AnswersOneShapeForEveryReading is the M-7 guard.
//
// GET /subscribers has three readings — the ordinary keyset page, the `subscriber_id_hash`
// resolution and the `revocation_pending=true` scan — and two of them used to answer with a bare
// JSON array while the third answered with the page envelope. That is a breaking difference, not a
// cosmetic one: a client written against the page reading fails on the resolver with a type error
// rather than a message.
//
// And it fails where it costs most. The two bare-array readings are the ones an operator reaches
// from a runbook DURING AN INCIDENT — the first step of the outstanding-revocation procedure, and
// the resolution of a pseudonym off a consumer-lag alert — so the shape that broke was the one
// exercised while something was already wrong.
//
// Each reading is asserted to decode into the SAME envelope struct, which is the property a client
// depends on and the one a bare array cannot satisfy.
func TestListSubscribers_AnswersOneShapeForEveryReading(t *testing.T) {
	type envelope struct {
		Data       []json.RawMessage `json:"data"`
		NextCursor string            `json:"next_cursor"`
		HasMore    bool              `json:"has_more"`
		TotalCount *int64            `json:"total_count"`
	}

	decode := func(t *testing.T, target string) envelope {
		t.Helper()

		recorder := subscribersRequest(t, http.MethodGet, target)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		// THE ASSERTION THAT A BARE ARRAY CANNOT PASS. Unmarshalling `[...]` into a struct is a
		// type error, which is exactly the failure a client written against one reading hit on
		// another.
		var body envelope
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body),
			"every reading must decode into the page envelope; body: %s", recorder.Body.String())
		require.NotNil(t, body.Data,
			"data must be [] rather than null or absent, so a script can range over it "+
				"unconditionally")

		return body
	}

	t.Run("the ordinary page", func(t *testing.T) {
		decode(t, "/subscribers?limit=1")
	})

	t.Run("the pseudonym resolution of an unknown token", func(t *testing.T) {
		// A token no subscriber can hash to. An empty result is a SUCCESSFUL answer here — the
		// registry was searched to its end — so it must be an empty data array, not a 404 and not
		// a bare [].
		body := decode(t, "/subscribers?subscriber_id_hash="+strings.Repeat("f", 16))

		assert.Empty(t, body.Data)
		assert.False(t, body.HasMore, "a resolution is not a walk, so there is nothing to resume")
		assert.Empty(t, body.NextCursor)
		require.NotNil(t, body.TotalCount,
			"the resolution is COMPLETE, so its length is the exact total and reporting it is honest")
		assert.Zero(t, *body.TotalCount)
	})

	t.Run("the awaiting-revocation scan", func(t *testing.T) {
		body := decode(t, "/subscribers?revocation_pending=true")

		assert.False(t, body.HasMore,
			"the scan covers the WHOLE registry, so a caller must not be invited to page it")
		assert.Empty(t, body.NextCursor)
		require.NotNil(t, body.TotalCount)
		assert.EqualValues(t, len(body.Data), *body.TotalCount,
			"a complete result's total is its length; anything else would misdescribe it")
	})

	t.Run("revocation_pending=false is the ordinary page", func(t *testing.T) {
		// Carried rather than refused so a client building the query from a boolean variable does
		// not have to special-case one of its values — and it must therefore behave exactly as the
		// parameter's absence does.
		body := decode(t, "/subscribers?revocation_pending=false&limit=1")
		assert.Nil(t, body.TotalCount, "nobody asked for a total on the ordinary page")
	})
}

// TestListSubscribers_RefusesPagingOptionsOnACompleteReading is the other half of M-7.
//
// The pseudonym resolution and the revocation scan are not walks: the resolver returns at most one
// row, and the scan deliberately covers the WHOLE registry because a partial list of live
// unaccounted-for credentials reads exactly like a complete one — acting on it would leave the rest
// authenticating while the incident looked closed, which is why the service refuses rather than
// truncating when the registry exceeds its bound.
//
// So `?revocation_pending=true&limit=10` asks for something that does not exist. It was silently
// ignored, and the caller received every matching row believing they had asked for ten. On this
// endpoint that is the dangerous direction: an operator who thinks they hold a bounded page of a
// longer list stops looking.
func TestListSubscribers_RefusesPagingOptionsOnACompleteReading(t *testing.T) {
	readings := map[string]string{
		"the pseudonym resolution": "subscriber_id_hash=" + strings.Repeat("a", 16),
		"the revocation scan":      "revocation_pending=true",
	}

	for readingName, reading := range readings {
		for _, option := range []string{"limit=10", "cursor=abc"} {
			t.Run(readingName+" refuses "+option, func(t *testing.T) {
				recorder := subscribersRequest(t, http.MethodGet,
					"/subscribers?"+reading+"&"+option)

				require.Equal(t, http.StatusBadRequest, recorder.Code,
					"a paging option this reading cannot honour must be refused rather than "+
						"ignored; body: %s", recorder.Body.String())

				// The refusal must name BOTH parameters, or the operator is left guessing which
				// two of their query values disagree.
				body := recorder.Body.String()
				assert.Contains(t, body, strings.SplitN(option, "=", 2)[0])
				assert.Contains(t, body, strings.SplitN(reading, "=", 2)[0])
			})
		}

		t.Run(readingName+" still accepts include_count", func(t *testing.T) {
			// Both readings are complete, so their length IS the total and it is reported
			// unconditionally. Asking for a total that is already there is not a contradiction.
			recorder := subscribersRequest(t, http.MethodGet,
				"/subscribers?"+reading+"&include_count=true")

			assert.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
		})
	}
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

	// IN-PROCESS TLS, because the credential endpoint refuses to put a one-time secret on a
	// channel it cannot establish as confidential, and this is the channel a production
	// deployment has. A loopback peer was presented here once; it establishes only that the LAST
	// hop stayed on the host, so relying on it made this harness model a posture that a
	// same-host reverse proxy silently invalidates.
	request.TLS = &tls.ConnectionState{}

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
			"false for a subscriber that recorded no prefix: there is no key boundary to keep, "+
				"which is a different state from a boundary nobody keeps")
		assert.Equal(t, true, enforced["broker_record_access"],
			"and such a subscriber holds topic Read, so its records come from the broker directly")
		assert.Equal(t, false, enforced["gateway_delivery_required"])
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
// carry the scope beside the component that enforces it. The adjacency is the property: the scope
// alone reads as a limit the credential itself carries, and it is not — the credential is refused
// record access at the broker, and the records must arrive through the key-authorising component
// the deployment declared, which is what applies the prefix. Registration accepts the intent
// whether or not such a component is declared; ISSUANCE is where the absence of one is refused.
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
	assert.Equal(t, true, enforced["partition_key_prefix_enforced"],
		"BESIDE the statement that it IS kept — the two cannot be read apart, and a false here "+
			"was the declaration that a recorded boundary was the subscriber's own problem")
	assert.Equal(t, string(coremodel.KeyScopeEnforcementGateway),
		enforced["partition_key_prefix_enforced_by"],
		"and the component is named, because 'enforced' without one is unverifiable")

	// THE TRANSPORT INSTRUCTION, which is the field a broken integration turns on: a client that
	// read false here would point a consumer at a broker that refuses its fetches.
	assert.Equal(t, true, enforced["gateway_delivery_required"])
	assert.Equal(t, false, enforced["broker_record_access"],
		"and its complement states why: no topic Read binding exists for a key-scoped principal")

	// The key dimension joins the ENFORCED list for a subscriber that recorded a prefix, and
	// nothing is left in the unenforced one.
	dimensions, ok := enforced["enforced_by"].([]interface{})
	require.True(t, ok)
	assert.Contains(t, dimensions, "partition_key")
	assert.Empty(t, enforced["not_enforced_by"],
		"no dimension of this subscriber's access is enforced by nobody")
}

// TestIssueKafkaCredentials_RefusesAKeyScopedSubscriberWithNoDeclaredGateway is the F-1 contract
// at the HTTP boundary as this build actually ships it: on the DEFAULT configuration a key-scoped
// row is refused a credential, with the typed conflict and both remedies.
//
// # Why the refusal is the contract
//
// Kafka's authorizer has no message-key dimension, so a credential carrying topic Read for a
// key-scoped row would read every record on every granted topic — other ledgers' and other
// subscribers' included. Three answers were tried and only the third holds.
//
// Disclosing the limitation in a 200 body is not a boundary: the party asked to apply the filter is
// the party holding the credential, and any other Kafka client ignores the request entirely.
// Serving the records from Blnk instead — a read path under /subscribers, reading with Blnk's own
// wide credential and filtering per record — was worse in three specific ways: it was a second data
// plane, it authenticated with a bespoke header the platform's authorization middleware knows
// nothing about, and revoking the subscriber's SCRAM credential at the broker left it open.
//
// What ships is a NARROWED GRANT plus a REFUSAL. A key-scoped principal is provisioned with
// Describe and no topic Read, and the records must be delivered by whatever key-authorising
// component the DEPLOYMENT declares in KAFKA_KEY_SCOPE_ENFORCEMENT. Blnk ships none — so with none
// declared there is no credential worth minting, and the endpoint says so.
//
// # What this asserts, and why at this layer
//
// The service layer owns the semantics and event_subscriber_test.go covers them. What belongs HERE
// is the boundary contract: the status, the typed code a client branches on, the remedies in the
// message, and that a refusal leaves nothing written and discloses nothing.
//
// The ORDER is part of it. The refusal happens after the row is read — so the fence claim and the
// registry read are required expectations below — and before any secret exists or the broker is
// touched, which is what makes it free of residue.
//
// The registry read of the same state — the prefix reported together with the component that would
// enforce it — is TestSubscribersAPI_RecordsAndReturnsTheKeyScope. The declared-gateway case is
// TestIssueKafkaCredentials_ProceedsForAKeyScopedSubscriberUnderADeclaredGateway, immediately below.
func TestIssueKafkaCredentials_RefusesAKeyScopedSubscriberWithNoDeclaredGateway(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
		// DELIBERATELY NO keyScopeGateway: this is the shipped default.
	})

	subscriberID := uniqueSubscriberID()

	// A row in the state under test: a recorded prefix, no credential yet. The credential
	// reference is cleared so the outcome cannot be confused with a re-issuance conflict.
	stored := subscribersFixtureRow(subscriberID)
	keyScope := "ldg_9f2c"
	stored.PartitionKeyPrefix = &keyScope
	stored.CredentialReference = nil
	stored.CredentialIssuedAt = nil

	const claimToken = "claim-token-for-the-key-scoped-issuance"

	// REQUIRED, both of them: the refusal is a judgement about the ROW, so the request must reach
	// the provisioning fence and then the registry before it can be made. An implementation that
	// refused every request carrying no gateway declaration — without reading the row — would
	// also refuse prefix-less subscribers, and AssertExpectations below is what separates the two.
	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return(claimToken, nil).Once()
	datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).Return(stored, nil).Once()
	// The fence's heartbeat and its release run on their own schedules, so both are permitted
	// rather than required.
	datasource.On("RenewSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("ReleaseSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	// PERMITTED, NOT EXPECTED, and never reached — registered only so that a build which somehow
	// completed the issuance would fail the assertion below rather than panicking inside the mock.
	datasource.On("RecordSubscriberCredentialIfUnchanged",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})
	body := recorder.Body.String()

	require.Equal(t, http.StatusConflict, recorder.Code,
		"a key-scoped row must be REFUSED where nothing is declared to apply its prefix: the only "+
			"credentials available are one wider than the row describes and one that can fetch "+
			"nothing. body: %s", body)
	assert.Contains(t, body, string(apierror.ErrSubscriberKeyScopeUnenforced),
		"and the TYPED code, because a client branches on it — a bare 409 is indistinguishable from "+
			"a concurrent re-issuance")
	assert.Contains(t, body, "Clear the partition key prefix",
		"the first remedy travels in the body: a refusal naming none is a dead end")
	assert.Contains(t, body, "narrow the subscriber's authorized topics",
		"and the enforceable alternative for an operator who wanted isolation")

	assert.NotContains(t, body, `"password"`,
		"A REFUSED ISSUANCE CARRIES NO SECRET: none is generated on this path")
	assert.NotContains(t, body, keyScope,
		"and it does not echo the prefix into the body, which is caller-supplied text")

	// NOTHING WAS WRITTEN. The refusal precedes the issuance record, so the registry cannot end up
	// describing a credential no subscriber holds.
	datasource.AssertNotCalled(t, "RecordSubscriberCredentialIfUnchanged",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	datasource.AssertExpectations(t)
}

// TestIssueKafkaCredentials_ProceedsForAKeyScopedSubscriberUnderADeclaredGateway is the other side
// of the same decision, and it is what keeps the refusal above from being read as "key scopes are
// unusable".
//
// With KAFKA_KEY_SCOPE_ENFORCEMENT declaring a component at a distinct endpoint, the identical
// request is NOT refused: it proceeds past the key-scope guard to the broker. This harness holds no
// admin SASL credential, so the broker step fails with a 503 — which is the assertion. A 503 here
// means the row was accepted and provisioning was attempted; a 409 would mean the guard refused.
//
// The pair is what makes either test conclusive. A build that always refused would satisfy the test
// above and fail this one; a build that never refused would satisfy this one and fail that.
func TestIssueKafkaCredentials_ProceedsForAKeyScopedSubscriberUnderADeclaredGateway(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
		// DISTINCT from brokers, or KeyScopeGateway reads it as no declaration and this test
		// would assert the refusal it exists to rule out.
		keyScopeGateway: []string{"keyscope-gateway.invalid:9095"},
	})

	subscriberID := uniqueSubscriberID()

	stored := subscribersFixtureRow(subscriberID)
	keyScope := "ldg_9f2c"
	stored.PartitionKeyPrefix = &keyScope
	stored.CredentialReference = nil
	stored.CredentialIssuedAt = nil

	const claimToken = "claim-token-for-the-declared-gateway-issuance"

	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return(claimToken, nil).Once()
	datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).Return(stored, nil).Once()
	datasource.On("RenewSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("ReleaseSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("RecordSubscriberCredentialIfUnchanged",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})
	body := recorder.Body.String()

	require.NotEqual(t, http.StatusConflict, recorder.Code,
		"with a component declared there is nothing for the key-scope guard to refuse. body: %s", body)
	assert.NotContains(t, body, string(apierror.ErrSubscriberKeyScopeUnenforced),
		"SUBSCRIBER_KEY_SCOPE_UNENFORCED says nothing applies the prefix; this deployment declared "+
			"something that does")

	// AND PROVISIONING WAS ATTEMPTED AT THE BROKER, which is what proves the row was accepted
	// rather than skipped. It fails because the harness holds no admin SASL credential, and a 503
	// is the honest answer: nothing was created, and a retry against a reachable, authenticated
	// cluster is what would succeed.
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code,
		"the row was accepted, so the next step is the broker, and this harness cannot "+
			"authenticate to one. body: %s", body)
	assert.Contains(t, body, string(apierror.ErrSubscriberProvisioningFailed),
		"and the typed code names provisioning rather than the request or the row, because "+
			"neither is what failed")

	assert.NotContains(t, body, `"password"`,
		"A FAILED ISSUANCE CARRIES NO SECRET: the credential is never marshalled on this path")

	datasource.AssertNotCalled(t, "RecordSubscriberCredentialIfUnchanged",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	datasource.AssertExpectations(t)
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

// =======================================================================================
// The mock-backed half of this file
//
// Everything above this line runs against a REAL datasource and, for the credential
// route, against no broker at all. That combination proves a great deal — the bindings,
// the derived identity, the status codes, the deprecated surface's lifecycle — but there
// are four properties it cannot reach, and each of them is the kind that fails silently.
//
//	THE AUTHORIZATION RESOURCE MAP. A master-key request never consults it: the auth
//	middleware matches the master key and returns before getResourceFromPath is called, so
//	a master-key-only reachability test passes even with "subscribers" missing from
//	middleware.pathToResource — the state in which every request to the surface from a
//	NON-MASTER principal is aborted with ErrAuthUnknownResource, which in a secure
//	deployment is every integration. Proving the map needs secure mode AND a valid
//	non-master key, which needs a datasource that can answer GetAPIKey without a
//	database row.
//
//	THAT THE GATE SHORT-CIRCUITS. "Refused with AUTH_MASTER_KEY_REQUIRED" and "refused
//	before any registry work" are different claims. The second one is only observable by
//	watching the store, and a real datasource cannot be watched.
//
//	THE FIVE-SECOND CEILING AS AN HTTP OUTCOME. It takes a dependency that stalls PAST the
//	budget, which no real dependency does on demand.
//
//	THE PLAINTEXT SASL PASSWORD. A SubscriberCredential's password field is unexported and
//	has no exported constructor, deliberately, so this package cannot fabricate one: the
//	only way a plaintext reaches a response is a real issuance against a real broker. The
//	test that does it is skipped, with a reason, when no broker is configured.
//
// Rules status, stated because UR4 requires it: review_rules reports NO USER RULES for
// this project. The baseline held to instead is the one AAP §0.8 anchors to conventions
// this repository already keeps — tests beside their source in package api, testify
// assertions, configuration injected only through config.MockConfig, assertions on
// error_detail.code rather than on a status that several codes share, the secret-handling
// posture proved rather than asserted in prose, and the Apache-2.0 header this file
// already carries.
// =======================================================================================

// subscribersAPITestMasterKey is the master secret the mock-backed router runs with.
//
// It is a test fixture and deliberately self-describing: it matches no provider's
// credential format, so a secret scanner reading this file finds a string that says what
// it is rather than something it has to guess about.
const subscribersAPITestMasterKey = "subscribers-api-test-master-key-not-a-real-credential"

// subscribersHarness describes the deployment a mock-backed router is built for.
//
// The three knobs exist because the properties below need genuinely different
// deployments, and a single boolean could not express them:
//
//   - secure selects the authentication middleware. TRUE is required to prove anything
//     about the resource map or about scopes; FALSE is what allows the master-key flag to
//     be injected directly, which is the only way to observe a request that reaches a
//     handler while making NO datasource call at all.
//   - masterKey is the injected principal, honoured only when secure is false. In secure
//     mode the key travels on the request instead, so that the whole chain is exercised.
//   - brokers configures Kafka. Empty is the graceful-degradation deployment the ordinary
//     suite runs in; non-empty is what lets a request reach the registry through the
//     credential route, which is where the ceiling lives.
type subscribersHarness struct {
	secure    bool
	masterKey bool
	brokers   []string

	// keyScopeGateway declares a key-authorising component in front of the brokers, at these
	// addresses. Empty is the SHIPPED DEFAULT, under which issuance refuses a subscriber
	// recording a partition_key_prefix — so a test that needs a key-scoped issuance to proceed
	// sets this, and a test asserting the refusal must not.
	//
	// The addresses must be DISTINCT from brokers: config.KafkaConfig.KeyScopeGateway reads a
	// gateway list equal to the broker list as no declaration at all, on the grounds that a
	// component which IS the brokers cannot be evaluating keys.
	//
	// SETTING THIS ALSO STARTS AN ATTESTING STUB and declares its control endpoint, because a
	// mode and an address are no longer a complete declaration: issuance binds the recorded
	// prefix at the component and requires it to attest that binding back (SEC-01). A harness
	// that declared addresses alone would refuse every key-scoped issuance with
	// SUBSCRIBER_KEY_SCOPE_UNATTESTED — a refusal, just not the one under test.
	keyScopeGateway []string

	// allowLoopbackIssuance declares the deployment a LOCAL-DEVELOPMENT host, which is the only
	// state in which a loopback peer establishes a confidential channel for credential issuance.
	//
	// Default false, matching production: a loopback peer proves only that the last hop stayed
	// on the host, and a same-host reverse proxy makes a public plaintext hop look loopback from
	// inside the process. Tests here therefore present IN-PROCESS TLS instead, which is the
	// channel a production deployment actually has; this field exists for the two tests whose
	// subject IS the local-development exception.
	allowLoopbackIssuance bool

	// trustForwardedProto declares the proxy in front of Blnk that terminated TLS and sets
	// X-Forwarded-Proto, which is the production Kubernetes shape: TLS ends at the ingress and
	// the hop to the pod is plaintext, so the process never sees a handshake. Without the
	// declaration the header is a claim any caller can make and is not believed.
	trustForwardedProto bool

	// trustedProxies names the peers that declaration applies to
	// (BLNK_SERVER_TRUSTED_PROXIES), and it is the SECOND HALF of the forwarded-HTTPS channel
	// rather than a refinement of it (SEC-03).
	//
	// The declaration alone is a statement about the intended path, and a request arriving by
	// any other route — a pod IP, a port-forward, a second Service — is not on it while still
	// being able to send the same header. So trustForwardedProto with this left empty is a
	// deployment in which the forwarded channel establishes NOTHING, and that combination is
	// exercised deliberately below rather than avoided.
	trustedProxies string
}

// setupSubscribersRouter builds a router over a mock datasource and returns both.
//
// The mock is returned because it IS the assertion surface for half the properties in
// this section: which repository methods a request reached, with which arguments, and —
// most often — that it reached none.
//
// blnk.NewBlnk accepts the mock because it takes a database.IDataSource and touches none
// of its methods during construction. Composing &blnk.Blnk{datasource: …} directly is
// impossible from package api because the field is unexported; the root package's own
// tests do that only because they are in package blnk.
//
// The DataSource and Redis DSNs are set even though no test here reaches PostgreSQL:
// config.MockConfig runs validateAndAddDefaults, which requires both. WebhookDeprecation
// SunsetDate is set only in the Kafka case, and to a FUTURE instant — configuration
// validation refuses brokers without a sunset date, because a Kafka deployment with no
// usable dual-delivery window is treated as already past the retirement. A future instant
// keeps the sunset guard transparent, which is the state the deprecated routes are
// exercised in here; the retirement itself belongs to api/webhook_sunset_test.go and is
// not touched from this file.
//
// Parameters:
//   - t *testing.T: the test, for fatal reporting and cleanup.
//   - harness subscribersHarness: the deployment to build.
//
// Returns:
//   - *gin.Engine: the router, with the whole production middleware chain.
//   - *mocks.MockDataSource: the store every handler in the surface reads through.
func setupSubscribersRouter(
	t *testing.T, harness subscribersHarness,
) (*gin.Engine, *mocks.MockDataSource) {
	t.Helper()

	configuration := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_subs",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:                          harness.secure,
			SecretKey:                       subscribersAPITestMasterKey,
			AllowLoopbackCredentialIssuance: harness.allowLoopbackIssuance,
			TrustForwardedProto:             harness.trustForwardedProto,
			TrustedProxies:                  harness.trustedProxies,
		},
	}

	if len(harness.brokers) > 0 {
		configuration.Kafka = config.KafkaConfig{
			Brokers:           harness.brokers,
			SubscriberBrokers: harness.brokers,
			TopicPrefix:       coremodel.DefaultEventTopicPrefix,
			MinPartitions:     blnk.MinTopicPartitions,
			ReplicationFactor: 1,
			// The local stack listens on SASL_PLAINTEXT and nothing else, and the
			// transport refuses to dial in the clear without this acknowledgement.
			InsecureLocalDev: true,
		}

		if len(harness.keyScopeGateway) > 0 {
			configuration.Kafka.KeyScopeEnforcement = config.KeyScopeEnforcementBrokerGateway
			configuration.Kafka.KeyScopeGatewayBrokers = append(
				[]string(nil), harness.keyScopeGateway...,
			)

			// AND THE CONTROL ENDPOINT, without which the declaration is incomplete and every
			// key-scoped issuance is refused before it reaches the broker.
			endpoint, token := startKeyScopeGatewayStub(t)
			configuration.Kafka.KeyScopeGatewayAttestationURL = endpoint
			configuration.Kafka.KeyScopeGatewayAttestationToken = token
		}

		configuration.WebhookDeprecationSunsetDate = time.Now().
			Add(20 * 24 * time.Hour).UTC().Format(time.RFC3339)
	}

	// RESTORED WHEN THE TEST ENDS. config.ConfigStore is a process-global atomic.Value, so a
	// broker list, a master key or a sunset date installed here is read by every later test in
	// the package that calls config.Fetch — including the ones asserting the unconfigured or
	// insecure posture, which then fail somewhere that never mentioned Kafka. Saving and
	// restoring is the established idiom; see the same block in newEventsAPIOverMockDatasource.
	previousConfiguration := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previousConfiguration != nil {
			config.ConfigStore.Store(previousConfiguration)

			return
		}

		// An atomic.Value cannot be emptied, so a process that held nothing before this test
		// is returned to a configuration that carries nothing rather than left holding this one.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.MockConfig(configuration)

	fetched, err := config.Fetch()
	require.NoError(t, err, "the mock configuration must be loadable")
	require.Equal(t, len(harness.brokers), len(fetched.Kafka.Brokers),
		"config.MockConfig REJECTED the configuration and left the previous one in the store, "+
			"so this test would have run against another test's deployment. Its reason is on the "+
			"error log above")

	datasource := new(mocks.MockDataSource)

	// THE MOCK IS HANDED OVER UNWRAPPED, and the wrapper it replaced is gone rather than
	// retired. This harness used to interpose a synchronised datasource plus a barrier
	// middleware so a non-master request could be ordered ahead of the background last-used
	// update the authentication middleware starts for it — scaffolding that existed only
	// because the middleware read c.Request from INSIDE that goroutine, racing otelgin's
	// deferred restore.
	//
	// That race was fixed at its cause: the middleware now captures the request context and the
	// key id on the request goroutine and runs the update on a detached bounded context
	// (context.WithoutCancel), so nothing here has to schedule around it. A test that needs to
	// OBSERVE the update joins it through expectEventsAPIKeyLookup / requireEventsLastUsedTouch
	// in events_api_test.go — same package, one implementation of that join.
	service, err := blnk.NewBlnk(datasource)
	require.NoError(t, err, "the service container must be constructible over the mock store")

	instance := NewAPI(service)
	require.NotNil(t, instance, "NewAPI returned nil, which means the configuration was unusable")

	if !harness.secure {
		// Injected into the API's OWN engine before the routes are registered, exactly as
		// setupHookRouter and eventsRouter do. Wrapping the finished router in a second
		// engine does not work: the nested context is not the one the handlers read.
		//
		// This is only reachable with secure mode off. With it on, the authentication
		// middleware answers 401 before any injected value is consulted, which is why the
		// secure harness sends the key on the request instead.
		instance.router.Use(func(c *gin.Context) {
			c.Set("isMasterKey", harness.masterKey)
			c.Next()
		})
	}

	return instance.Router(), datasource
}

// subscribersTestAPIKey builds a VALID non-master API key carrying the given scopes.
//
// Valid is the operative word, and it is two conditions rather than one:
// model.APIKey.IsValid() is !IsRevoked && now.Before(ExpiresAt), and an invalid key is
// refused with AUTH_EXPIRED_API_KEY before the resource map is ever consulted — which
// would make the authorization test below pass while proving nothing.
//
// Parameters:
//   - scopes ...string: the scopes to grant, in resource:action form.
//
// Returns:
//   - *coremodel.APIKey: the principal the middleware resolves the request to.
func subscribersTestAPIKey(scopes ...string) *coremodel.APIKey {
	return &coremodel.APIKey{
		APIKeyID:  "key_subscribers_api_test",
		Key:       "subscribers-api-test-scoped-key-not-a-real-credential",
		Name:      "subscribers api test key",
		OwnerID:   "subscribers-api-test-owner",
		Scopes:    scopes,
		ExpiresAt: time.Now().Add(time.Hour),
		CreatedAt: time.Now().Add(-time.Hour),
	}
}

// subscribersCall is one request against the surface.
//
// # The transport this presents by default, and why it is TLS rather than loopback
//
// The credential route refuses to put a one-time password on a channel the deployment has not
// established as confidential, so every call has to present one of the three. This harness
// presents IN-PROCESS TLS — the channel a production deployment actually has — because the
// alternative it used to present, a loopback peer, is not evidence of confidentiality on its
// own: a reverse proxy on the same host forwards a public plaintext request over 127.0.0.1, so
// the peer reads as loopback while the client's hop was readable. Defaulting to it made every
// test in this file assert against a posture no production deployment should have.
//
// peer is left to httptest's 192.0.2.1 unless a test sets it, and plaintext drops the TLS state,
// so the two tests whose subject IS the transport gate can compose any of the four shapes:
// TLS, declared proxy, declared local-development host with a loopback peer, and none of those.
type subscribersCall struct {
	method string
	path   string
	body   string
	key    string
	peer   string

	// plaintext omits the in-process TLS state, leaving the request on a channel the deployment
	// has established nothing about unless it declared a proxy or a local-development host.
	plaintext bool

	// forwardedProto sets X-Forwarded-Proto, which is believed only where the deployment
	// declared the proxy that sets it.
	forwardedProto string
}

// subscribersServe performs one call and returns the recorder.
func subscribersServe(
	t *testing.T, router *gin.Engine, call subscribersCall,
) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(call.method, call.path, jsonReader(call.body))
	if call.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	if call.key != "" {
		request.Header.Set(middleware.KeyHeader, call.key)
	}

	if call.peer != "" {
		request.RemoteAddr = call.peer
	}

	if call.forwardedProto != "" {
		request.Header.Set("X-Forwarded-Proto", call.forwardedProto)
	}

	// IN-PROCESS TLS, unless the call is deliberately plaintext. A non-nil Request.TLS is what a
	// server that completed the handshake itself records, and it is the one confidential channel
	// that needs no declaration by the deployment — which makes it the honest default for a
	// harness whose subject is the handlers rather than the transport gate.
	if !call.plaintext {
		request.TLS = &tls.ConnectionState{}
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

// subscribersEveryRoute is the whole MASTER-KEY-GATED surface: the six registry routes and
// the four deprecated webhook-subscription ones.
//
// It is a function rather than a package-level slice so that each caller gets its own
// copy and cannot mutate a shared table. The bodies are the smallest ones each route
// binds, because these tables drive tests about AUTHORIZATION: a request must fail on the
// gate rather than on its body, or the assertion moves to a different property.
//
// EVERY ROUTE UNDER /subscribers IS HERE, and that is now a complete statement rather than a
// qualified one: there is no data-plane route to leave out. A record-serving route existed and was
// removed — it was authenticated by the subscriber's own SASL secret rather than by an operator's
// master key, which is precisely why it could not appear in a table asserting
// ErrAuthMasterKeyRequired for every entry. Its absence is asserted directly by
// TestSubscribersAPI_HasNoRecordServingRoute, and the absence of the header it read by
// TestSubscribersAPI_DoesNotReadASubscriberSecretHeader.
func subscribersEveryRoute(subscriberID string) []subscribersCall {
	webhookPath := webhookSubscriptionPath(subscriberID)

	return []subscribersCall{
		{method: http.MethodPost, path: "/subscribers", body: `{"name":"probe"}`},
		{method: http.MethodGet, path: "/subscribers"},
		{method: http.MethodGet, path: "/subscribers/" + subscriberID},
		{method: http.MethodPut, path: "/subscribers/" + subscriberID, body: `{"name":"renamed"}`},
		{method: http.MethodDelete, path: "/subscribers/" + subscriberID},
		{method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials"},
		{
			method: http.MethodPost, path: webhookPath,
			body: `{"webhook_url":"https://subscriber.example.com/blnk-events"}`,
		},
		{method: http.MethodGet, path: webhookPath},
		{
			method: http.MethodPut, path: webhookPath,
			body: `{"webhook_url":"https://subscriber.example.com/blnk-events"}`,
		},
		{method: http.MethodDelete, path: webhookPath},
	}
}

// subscribersFixtureRow is a stored registry row, with the two columns a response must
// never disclose deliberately populated.
//
// CredentialReference and WebhookURL are set on every fixture. A read shape that dropped
// them would pass a test built from a row that never carried them, which is the shape of
// test that lets a leak through: the assertion has to be able to fail.
func subscribersFixtureRow(subscriberID string) *coremodel.EventSubscriber {
	principal, err := coremodel.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		// A fixture built from a non-canonical identifier would exercise the validator
		// rather than the handler, so this is a defect in the test rather than a case to
		// handle at run time. Panicking names it at the point it was introduced.
		panic("subscribers api test: fixture identifier is not canonical: " + err.Error())
	}

	group, err := coremodel.CanonicalConsumerGroupID(subscriberID)
	if err != nil {
		panic("subscribers api test: fixture identifier yields no consumer group: " + err.Error())
	}

	reference := "scram-sha-512-ref-v1$" + strings.Repeat("a", 64)
	webhookURL := "https://legacy.example.com/blnk-events"
	issued := time.Now().Add(-time.Hour).UTC()

	return &coremodel.EventSubscriber{
		ID:                  42,
		SubscriberID:        subscriberID,
		Name:                "settlement consumer",
		KafkaPrincipal:      principal,
		ConsumerGroupID:     group,
		AuthorizedTopics:    coremodel.SubscriberGrantableTopics(coremodel.DefaultEventTopicPrefix)[:1],
		CredentialReference: &reference,
		CredentialIssuedAt:  &issued,
		WebhookURL:          &webhookURL,
		CreatedAt:           time.Now().Add(-2 * time.Hour).UTC(),
		UpdatedAt:           issued,
	}
}

// TestSubscribersAPI_ACorrectlyScopedKeyIsAdmittedAndRefusedByTheHandlersGate is the
// AUTHORIZED-CALLER half of the two-file registration proof, and it is the half that was
// missing.
//
// # Why the unrelated-scope test is not enough on its own
//
// TestSubscribersAPI_AuthorizationResourceIsRegistered above drives every route with a key
// scoped to ANOTHER feature. That refusal proves the prefix resolves and names the scope it
// resolves to, and it stops there — by construction, because the request is refused by the
// MIDDLEWARE. Everything past that point is untested by it:
//
//   - Whether a caller granted exactly `subscribers:<action>` is ADMITTED. A permission check
//     that refused the correct scope — an action table off by one entry, a scope comparison that
//     never matches — would leave every route unusable by every non-master principal, and the
//     unrelated-scope test would still pass, because it EXPECTS a refusal.
//   - Whether the ENDPOINT'S OWN master-key gate then refuses. AUTH_MASTER_KEY_REQUIRED is the
//     only code that proves a request traversed the entire chain and was stopped last by the
//     handler. A route that forgot its gate would answer 200, or reach the registry, and again
//     the unrelated-scope test would notice nothing: its request never got that far.
//   - Whether the gate refuses BEFORE doing work. A gate that fires after the service call has
//     started is not a gate, and on the credential route it would mint a credential for a caller
//     it was about to refuse.
//
// # Why this could not be written before
//
// A correctly scoped key is exactly the path that spawns the authentication middleware's
// fire-and-forget last-used update, and that goroutine races otelgin's deferred restore of
// c.Request — a PRE-EXISTING production race, reproducible on /ledgers and every resource older
// than this feature. The harness now joins each request with its own background work through
// expectEventsAPIKeyLookup and requireEventsLastUsedTouch, which is a happens-before edge
// rather than a delay, so the path is testable without the race being reported against this file
// and without pretending it does not exist.
//
// Every assertion names a CODE, never a status: AUTH_UNKNOWN_RESOURCE,
// AUTH_INSUFFICIENT_PERMISSIONS and AUTH_MASTER_KEY_REQUIRED all resolve to 403, so a
// status-only assertion passes in all three of the failure modes above.
func TestSubscribersAPI_ACorrectlyScopedKeyIsAdmittedAndRefusedByTheHandlersGate(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	// The action each method maps to in middleware.methodToAction. The scope granted below is
	// built from it, so a key is granted EXACTLY what the route requires and nothing more: a
	// wildcard would pass even if the method mapped to the wrong action.
	actionForMethod := map[string]middleware.Action{
		http.MethodGet:    middleware.ActionRead,
		http.MethodPost:   middleware.ActionWrite,
		http.MethodPut:    middleware.ActionWrite,
		http.MethodDelete: middleware.ActionDelete,
	}

	for _, call := range subscribersEveryRoute(subscriberID) {
		target := call

		t.Run(target.method+" "+target.path, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

			action, mapped := actionForMethod[target.method]
			require.Truef(t, mapped, "no action is mapped for %s, so no scope can be granted",
				target.method)

			scope := middleware.BuildScope(middleware.ResourceSubscribers, action)
			key := subscribersTestAPIKey(scope)

			// The two calls the authentication middleware makes for a non-master credential,
			// programmed together with the join that orders the second one against the call-log
			// read further down. The barrier that used to provide that ordering was removed with
			// the c.Request race it existed for — see the note on setupSubscribersRouter — so the
			// join is what keeps this test's read off testify's internals while a goroutine is
			// still appending to them.
			touched := expectSubscribersAPIKeyLookup(datasource, key)

			target.key = key.Key
			recorder := subscribersServe(t, router, target)

			var body struct {
				ErrorDetail apierror.APIError `json:"error_detail"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body),
				"the refusal must be a typed error body; got %q", recorder.Body.String())

			require.NotEqual(t, apierror.ErrAuthUnknownResource, body.ErrorDetail.Code,
				"%s %s RESOLVES TO NO AUTHORIZATION RESOURCE, so the middleware aborts every "+
					"request to it from every non-master principal. Add \"subscribers\" to pathToResource "+
					"in api/middleware/auth.go", target.method, target.path)

			require.NotEqualf(t, apierror.ErrAuthInsufficientPermissions, body.ErrorDetail.Code,
				"A KEY GRANTED EXACTLY %q WAS REFUSED ON PERMISSIONS. Either %s %s resolves to a "+
					"different resource, or the method maps to a different action than %q — and "+
					"either way no non-master principal can ever use this route, however it is "+
					"scoped. message: %s",
				scope, target.method, target.path, action, body.ErrorDetail.Message)

			// THE ASSERTION THIS TEST EXISTS FOR. Only a request that passed authentication AND
			// the permission check reaches the handler, and only the handler's own gate answers
			// this code. A route that forgot the gate would answer 200 or 4xx-with-another-code
			// here.
			assertErrorCode(t, recorder,
				http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)

			// THE CREDENTIAL WAS ADMITTED, which the audit write is the observable proof of: the
			// middleware performs it only for a principal it authenticated and permitted, so its
			// arrival says the refusal below came from the HANDLER rather than from authentication.
			// It is also the join that makes the call-log read that follows safe.
			requireSubscribersLastUsedTouch(t, touched)

			// AND IT REFUSED BEFORE DOING ANY WORK. A fully authorised non-master caller must
			// learn only that the master key is required — never whether a subscriber exists,
			// and on the credential route never at the cost of a credential actually being
			// minted at the broker for a caller about to be refused.
			reached := subscribersRepositoryCallsExcludingAuth(datasource)
			assert.Emptyf(t, reached,
				"the gate must refuse before touching the registry; %s %s reached %v",
				target.method, target.path, reached)

			datasource.AssertExpectations(t)
		})
	}
}

// subscribersAssertSecretAbsent requires that a corpus does not contain the one-time password,
// without handing testify either the secret or the text containing it.
//
// # Why this is not assert.NotContains
//
// testify renders BOTH operands when an assertion fails, so
// assert.NotContains(t, body, password) prints the plaintext SCRAM password into the build log
// on exactly the run where the leak is real. CI logs are retained, searchable, and readable by
// more people and for far longer than the HTTP response ever was — so the assertion written to
// prove the secret is never disclosed would become the widest disclosure of it, and a credential
// cannot be un-leaked afterwards. The corpus is no safer to print: by hypothesis it contains the
// secret.
//
// So the comparison happens in Go and only a BOOLEAN reaches testify. The failure message says
// WHICH corpus and at what offset, which is what a reader needs to find the leak in the code, and
// carries neither the secret nor the text around it.
//
// Parameters:
//   - t *testing.T: the test.
//   - corpus string: the text that must not contain the secret. Never printed.
//   - secret string: the live one-time password. Never printed.
//   - what string: a description of the corpus, printed on failure.
func subscribersAssertSecretAbsent(t *testing.T, corpus, secret, what string) {
	t.Helper()

	// An empty secret would make every Index call return 0 and every assertion below fail for a
	// reason that has nothing to do with disclosure, so the fixture's own precondition is
	// checked rather than assumed.
	require.NotEmpty(t, secret,
		"the secret under test is empty, so %s cannot be checked for it", what)

	offset := strings.Index(corpus, secret)
	assert.Falsef(t, offset >= 0,
		"THE ONE-TIME PASSWORD APPEARS IN %s, at byte offset %d of %d. Neither the secret nor the "+
			"text containing it is printed here, deliberately: this assertion exists to prove the "+
			"plaintext is never disclosed, and rendering it into a retained build log to report the "+
			"failure would disclose it more widely than the leak under test. Reproduce locally and "+
			"inspect that value directly.",
		what, offset, len(corpus))
}

// subscribersBackgroundTouchTimeout bounds the wait for the middleware's background last-used
// update. A safety valve rather than a delay: in a correct run the update lands in microseconds.
// It matches eventsBackgroundTouchTimeout deliberately — one grace period for one mechanism.
const subscribersBackgroundTouchTimeout = 5 * time.Second

// expectSubscribersAPIKeyLookup programs the two datasource calls the authentication middleware
// makes for a non-master credential, and returns a channel that reports when the second has been
// RECORDED.
//
// # Why the channel exists
//
// The middleware answers the request and updates last_used_at on a goroutine that outlives the
// response, and that goroutine appends to testify's call log. Any assertion that reads the log —
// subscribersRepositoryCallsExcludingAuth below — is therefore reading a slice another goroutine
// may still be writing, which `go test -race` reports as a data race and which is one, not an
// artefact of the detector.
//
// A gin middleware used to serialise this by waiting after c.Next(). It was removed with the
// production race it was scheduling around (see the note on setupSubscribersRouter), and removing
// it left this file's readers with nothing ordering them. The channel is what replaces it: the
// signal is sent from testify's own Run hook, which fires AFTER the call has been appended, so a
// receive on it establishes a happens-before edge covering the append. No scheduling is
// influenced and nothing is guessed at.
//
// UpdateLastUsed is expected STRICTLY, exactly once and on the key that was admitted, for the
// same reason it is in api/events_api_test.go: `.Maybe()` hides both the update being dropped
// altogether and the update being made for a different key id.
//
// Parameters:
//   - datasource *mocks.MockDataSource: the datasource to program.
//   - key *coremodel.APIKey: the principal the lookup resolves to.
//
// Returns:
//   - <-chan struct{}: signalled once the background last-used update has been recorded.
//     Buffered, so the production goroutine never blocks on a test channel.
func expectSubscribersAPIKeyLookup(
	datasource *mocks.MockDataSource, key *coremodel.APIKey,
) <-chan struct{} {
	touched := make(chan struct{}, 1)

	datasource.On("GetAPIKey", mock.Anything, key.Key).Return(key, nil)
	datasource.On("UpdateLastUsed", mock.Anything, key.APIKeyID).
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

// requireSubscribersLastUsedTouch joins an authenticated request with the background last-used
// update the middleware starts for it, and FAILS if that update never happens.
//
// Parameters:
//   - t *testing.T: the test, FAILED when the update does not arrive.
//   - touched <-chan struct{}: the channel expectSubscribersAPIKeyLookup returns.
func requireSubscribersLastUsedTouch(t *testing.T, touched <-chan struct{}) {
	t.Helper()

	select {
	case <-touched:
	case <-time.After(subscribersBackgroundTouchTimeout):
		t.Fatalf(
			"the authentication middleware did not update the API key's last-used timestamp "+
				"within %s. It fires that update on a goroutine for every non-master credential it "+
				"admits, so either the request was not authenticated as expected or the update was "+
				"removed — and a test that proceeded here would be asserting on a call log another "+
				"goroutine may still be writing to", subscribersBackgroundTouchTimeout,
		)
	}
}

// subscribersRepositoryCallsExcludingAuth names every datasource method a request reached apart
// from the two the authentication middleware itself makes.
//
// GetAPIKey and UpdateLastUsed are authentication's own work and are present on every
// non-master request by definition, so counting them would make "reached nothing" impossible to
// express. Everything else is the handler's, and on a refused request there must be none of it.
//
// THE CALLER MUST HAVE JOINED THE BACKGROUND UPDATE FIRST — requireSubscribersLastUsedTouch —
// because the log this reads is appended to from that goroutine. Reading it while the goroutine
// is live is a data race on testify's internals, and the join, not a delay, is what orders them.
//
// Parameters:
//   - datasource *mocks.MockDataSource: the store behind the router.
//
// Returns:
//   - []string: the method names reached, in call order, excluding authentication's own.
func subscribersRepositoryCallsExcludingAuth(datasource *mocks.MockDataSource) []string {
	reached := make([]string, 0, len(datasource.Calls))
	for _, recorded := range datasource.Calls {
		if recorded.Method == "GetAPIKey" || recorded.Method == "UpdateLastUsed" {
			continue
		}

		reached = append(reached, recorded.Method)
	}

	return reached
}

// TestSubscribersAPI_AuthorizationResourceIsRegistered is the decisive test for the
// "subscribers" authorization resource, and it is the only one in this package that can be.
//
// # The failure mode it closes
//
// Registering a new route prefix takes edits in TWO files. api/middleware/scope.go declares
// ResourceSubscribers, and api/middleware/auth.go maps the first path segment to it in
// pathToResource. Make only the first edit and getResourceFromPath returns the empty
// resource, at which point the middleware ABORTS every request to the prefix with
// ErrAuthUnknownResource — every request, that is, from an authenticated NON-MASTER caller.
// The master key is deliberately excepted by the middleware itself, which matches it and
// returns on at api/middleware/auth.go:232-238 before any resource is resolved. The surface is
// therefore unreachable by every integration principal while remaining reachable by the one
// credential an operator is likeliest to try, and nothing in the ordinary suite notices.
//
// # Why a master-key test cannot see it
//
// The middleware matches the master key and returns BEFORE the resource is resolved. A
// master-key request therefore reaches the handler whether the prefix is mapped or not, so a
// reachability test built on the master key passes VACUOUSLY with pathToResource unedited.
// Reaching the map at all takes secure mode and a valid NON-MASTER key, which is what the
// mock store makes possible without seeding a database row.
//
// # Why the refusal asserted is INSUFFICIENT_PERMISSIONS
//
// The key below is scoped to another feature entirely. A request whose prefix resolves is
// therefore evaluated against "subscribers:<action>" and refused on permissions — and the
// refusal NAMES that scope, which is a stronger statement than mere resolution: a prefix
// wired to some other feature's resource would also resolve, would also pass a wildcard key,
// and would authorise this surface under that feature's scope with nothing failing. The
// scope named in the message is what pins the mapping's TARGET.
//
// This path is also chosen for what it does NOT touch. A CORRECTLY scoped non-master key
// passes HasPermission, and the middleware then spawns a background goroutine that reads
// c.Request after the handler may already have returned (api/middleware/auth.go:285-286) —
// which races with otelgin's deferred restore of the same field, because gin recycles its
// contexts through a pool. That race is PRE-EXISTING production behaviour, reproducible on
// /ledgers and every other resource that predates this feature, and it has nothing to do with
// this prefix. Refused-on-permissions reaches the map without ever creating that goroutine,
// so this test proves the mapping's TARGET while depending on none of it.
//
// The correctly-scoped path IS driven, separately, by
// TestSubscribersAPI_ACorrectlyScopedKeyIsAdmittedAndRefusedByTheHandlersGate, which joins each
// request to its own background update through eventsSynchronizedDatasource and
// requireEventsLastUsedTouch — a happens-before edge rather than a delay. The two tests answer
// different questions: this one asks whether the prefix resolves to the right resource, that one
// asks whether a key granted exactly that resource is admitted and then stopped by the handler.
//
// Every assertion names a CODE and never a status, because ErrAuthUnknownResource,
// ErrAuthInsufficientPermissions and ErrAuthMasterKeyRequired all resolve to 403.
func TestSubscribersAPI_AuthorizationResourceIsRegistered(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	// The action each method maps to in middleware.methodToAction, which is the other half of
	// the scope the refusal must name.
	actionForMethod := map[string]middleware.Action{
		http.MethodGet:    middleware.ActionRead,
		http.MethodPost:   middleware.ActionWrite,
		http.MethodPut:    middleware.ActionWrite,
		http.MethodDelete: middleware.ActionDelete,
	}

	t.Run("every route on the surface resolves to the subscribers resource", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

		// Scoped to another feature on purpose: what is under test is that the path RESOLVES
		// and to WHAT, not that this key may use it.
		key := subscribersTestAPIKey(
			middleware.BuildScope(middleware.ResourceLedgers, middleware.ActionAll))

		datasource.On("GetAPIKey", mock.Anything, key.Key).Return(key, nil)

		for _, call := range subscribersEveryRoute(subscriberID) {
			target := call
			target.key = key.Key

			expectedScope := middleware.BuildScope(
				middleware.ResourceSubscribers, actionForMethod[target.method])

			t.Run(target.method+" "+target.path, func(t *testing.T) {
				recorder := subscribersServe(t, router, target)

				assertErrorCode(t, recorder,
					http.StatusForbidden, apierror.ErrAuthInsufficientPermissions)

				var body struct {
					ErrorDetail apierror.APIError `json:"error_detail"`
				}
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

				// THE PROOF that pathToResource carries "subscribers". Asserted explicitly
				// and separately, because this is the failure the whole test exists for and
				// the two codes share a status.
				require.NotEqual(t, apierror.ErrAuthUnknownResource, body.ErrorDetail.Code,
					"%s %s RESOLVES TO NO AUTHORIZATION RESOURCE, so the middleware aborts "+
						"every request to it from every non-master principal. Add \"subscribers\" to "+
						"pathToResource in api/middleware/auth.go; declaring "+
						"ResourceSubscribers in scope.go alone is not enough",
					target.method, target.path)

				// THE PROOF that it resolves to the RIGHT resource and the right action.
				assert.Contains(t, body.ErrorDetail.Message, expectedScope,
					"the refusal must name %q: the resource this prefix maps to and the "+
						"action this method maps to, and both have to be right", expectedScope)

				// Nothing beyond the key lookup may have happened: an unauthorised caller
				// must not reach the registry.
				for _, recorded := range datasource.Calls {
					assert.Contains(t, []string{"GetAPIKey", "UpdateLastUsed"}, recorded.Method,
						"authentication resolves the principal and nothing else; %s was called "+
							"for a request that was refused", recorded.Method)
				}
			})
		}

		datasource.AssertExpectations(t)
	})

	t.Run("the master key travels on the header and reaches the handler", func(t *testing.T) {
		// The whole chain in its production configuration: secure mode on, no injected
		// context value, the key carried in X-Blnk-Key and matched by constant-time
		// comparison. Without this leg every master-key assertion in this file would rest on
		// an injected flag, and a regression in key extraction or comparison would be
		// invisible here.
		//
		// One route is enough, and this one is chosen because it reaches the store with a
		// single call: the property is the CHAIN, and reachability of the other nine is
		// covered by TestSubscribersAPI_RoutesAreReachableAndMasterKeyGated.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

		datasource.On("ListEventSubscribers", mock.Anything, mock.Anything).
			Return(coremodel.SubscriberPage{}, nil).Once()

		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodGet, path: "/subscribers", key: subscribersAPITestMasterKey,
		})

		require.Equal(t, http.StatusOK, recorder.Code,
			"the master key on the real header must authenticate and pass the endpoint's own "+
				"gate. body: %s", recorder.Body.String())
		assert.NotContains(t, recorder.Body.String(), string(apierror.ErrAuthUnknownResource))
		assert.NotContains(t, recorder.Body.String(), string(apierror.ErrAuthMasterKeyRequired))

		datasource.AssertExpectations(t)
	})

	t.Run("a wrong master key is refused rather than being treated as an api key", func(t *testing.T) {
		// The negative control for the leg above. A near-miss secret must not fall through to
		// the API-key branch and be reported as an invalid key, because the two say different
		// things to an operator: one means "your secret is wrong", the other means "that key
		// does not exist".
		router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

		// A TYPED nil. mocks.MockDataSource.GetAPIKey asserts its first return to
		// *model.APIKey unconditionally, so an untyped nil panics inside the mock and the
		// recovery middleware turns the result into a 500 — a green-looking test asserting
		// the wrong thing.
		datasource.On("GetAPIKey", mock.Anything, mock.Anything).
			Return((*coremodel.APIKey)(nil), fmt.Errorf("api key not found")).Once()

		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodGet, path: "/subscribers",
			key: subscribersAPITestMasterKey + "-wrong",
		})

		assertErrorCode(t, recorder, http.StatusUnauthorized, apierror.ErrAuthInvalidAPIKey)
		datasource.AssertNotCalled(t, "ListEventSubscribers", mock.Anything, mock.Anything)
	})

	t.Run("an expired key never reaches the resource map at all", func(t *testing.T) {
		// Why this belongs here rather than being a separate concern: if the fixture key were
		// invalid, every request in the first sub-test would be refused with
		// AUTH_EXPIRED_API_KEY before getResourceFromPath ran, and those assertions would be
		// asserting nothing. This pins the ordering that makes the fixture's validity
		// load-bearing.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

		expired := subscribersTestAPIKey(
			middleware.BuildScope(middleware.ResourceAll, middleware.ActionAll))
		expired.ExpiresAt = time.Now().Add(-time.Minute)

		datasource.On("GetAPIKey", mock.Anything, expired.Key).Return(expired, nil).Once()

		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodGet, path: "/subscribers", key: expired.Key,
		})

		// 401 rather than 403: the caller was never authenticated, so there was no principal
		// to evaluate a scope against and no resource lookup to reach.
		assertErrorCode(t, recorder, http.StatusUnauthorized, apierror.ErrAuthExpiredAPIKey)
		datasource.AssertNotCalled(t, "ListEventSubscribers", mock.Anything, mock.Anything)
	})
}

// TestSubscribersAPI_GateHelperAnswersTheRefusalItself unit-tests the gate in isolation,
// as api/hooks_test.go does for its counterpart.
//
// ensureSubscriberManagementAuthorized both DECIDES and RESPONDS, and the two halves fail
// independently: a helper that returned false without writing anything would produce an
// empty 200, and one that wrote the refusal but returned true would carry on into the
// handler after answering. Driving it through a bare test context is the only way to
// observe both halves at once.
func TestSubscribersAPI_GateHelperAnswersTheRefusalItself(t *testing.T) {
	t.Run("the master key is admitted and nothing is written", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Set("isMasterKey", true)

		assert.True(t, ensureSubscriberManagementAuthorized(c))
		assert.Equal(t, http.StatusOK, recorder.Code,
			"an admitted caller must leave the response to the handler")
		assert.Empty(t, recorder.Body.String(),
			"and no body may be written on the way through")
	})

	t.Run("an api key is refused and the refusal is written here", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)

		assert.False(t, ensureSubscriberManagementAuthorized(c))
		assert.Equal(t, http.StatusForbidden, recorder.Code)
		assert.Contains(t, recorder.Body.String(), errSubscribersRequireMasterKey.Error(),
			"the caller is told what it lacks, in the one wording every endpoint in the "+
				"surface shares")
		assert.Contains(t, recorder.Body.String(), string(apierror.ErrAuthMasterKeyRequired),
			"and the machine-readable code is what a client branches on")
	})

	t.Run("a principal that is not the master key is refused", func(t *testing.T) {
		// isMasterKeyRequest reads the value as a bool. A non-bool under that key — which a
		// future middleware change could introduce — must fail CLOSED.
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Set("isMasterKey", "true")

		assert.False(t, ensureSubscriberManagementAuthorized(c),
			"only a true boolean is the master key; a truthy-looking string is not")
		assert.Equal(t, http.StatusForbidden, recorder.Code)
	})
}

// TestSubscribersAPI_MasterKeyGateShortCircuitsBeforeTheRegistry proves the gate refuses
// BEFORE any work, on all ten endpoints.
//
// "Refused" and "refused before doing anything" are different claims, and only the second
// one is a gate. A handler that read the row, reconciled a grant or minted a credential and
// only then noticed the caller has already done the work: the response says no while the
// side effects say yes, and on the credential route the side effect is a live SCRAM
// credential at the broker.
//
// The store is the witness. Secure mode is OFF here deliberately — that is what makes the
// expected number of datasource calls exactly ZERO, since in secure mode the middleware
// itself would legitimately call GetAPIKey and the assertion would have to allow for it.
func TestSubscribersAPI_MasterKeyGateShortCircuitsBeforeTheRegistry(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	for _, call := range subscribersEveryRoute(subscriberID) {
		t.Run(call.method+" "+call.path, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: false})

			recorder := subscribersServe(t, router, call)

			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)

			// THE DECISIVE ASSERTION. Not "these particular methods were not called" but
			// "the store was not touched at all", which no future handler can slip past by
			// reaching for a method this list does not name.
			assert.Empty(t, datasource.Calls,
				"the gate must refuse before ANY registry work; %s %s reached the store",
				call.method, call.path)

			// The named form as well, because a failure here reports WHICH method a
			// regression reached and is the first thing a reader wants to know.
			datasource.AssertNotCalled(t, "CreateEventSubscriber", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "ListEventSubscribers", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "ListAndCountEventSubscribers", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "GetEventSubscriberByID", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "UpdateEventSubscriber",
				mock.Anything, mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "DeleteEventSubscriber", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "TakeEventSubscriber",
				mock.Anything, mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "ClaimSubscriberForProvisioning",
				mock.Anything, mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "RecordSubscriberCredentialIfUnchanged",
				mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "RecordSubscriberWebhookURL",
				mock.Anything, mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "ClearSubscriberWebhookURL", mock.Anything, mock.Anything)
			datasource.AssertNotCalled(t, "CompleteSubscriberWebhookMigration",
				mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// TestCreateSubscriber_AnswersTheRegisteredSubscriber pins the registration response and
// the row the handler asks the registry to store.
//
// Both halves matter. The response is what a client is written against, and the ARGUMENT is
// where the derived identity appears: the Kafka principal and the consumer group are
// computed from the identifier and have no field in the request DTO, precisely so a caller
// cannot name the values every ACL binding is granted to. Reading them off the captured
// argument is what proves they were derived rather than defaulted to empty.
func TestCreateSubscriber_AnswersTheRegisteredSubscriber(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)
	topic := stored.AuthorizedTopics[0]

	datasource.On("CreateEventSubscriber", mock.Anything, mock.Anything).Return(stored, nil).Once()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost,
		path:   "/subscribers",
		body: fmt.Sprintf(
			`{"subscriber_id":%q,"name":"settlement consumer","authorized_topics":[%q]}`,
			subscriberID, topic,
		),
	})

	require.Equal(t, http.StatusCreated, recorder.Code,
		"a created resource answers 201. body: %s", recorder.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	assert.Equal(t, subscriberID, body["subscriber_id"])
	assert.Equal(t, stored.KafkaPrincipal, body["kafka_principal"],
		"the derived principal is reported, because it is the value ACLs are granted to")
	assert.Equal(t, stored.ConsumerGroupID, body["consumer_group_id"])

	// authorized_topics ROUND-TRIPS AS A JSON ARRAY. It is a TEXT[] column read through
	// lib/pq, so the two places it could stop being an array are the driver's array
	// handling and this projection. A single-element grant serialising as a bare string
	// would break every client that iterates it, and would look correct in a body dump.
	topics, ok := body["authorized_topics"].([]interface{})
	require.True(t, ok, "authorized_topics must be a JSON array: %s", recorder.Body.String())
	require.Len(t, topics, 1)
	assert.Equal(t, topic, topics[0])

	assert.NotContains(t, recorder.Body.String(), `"credential_reference"`,
		"registration mints nothing and discloses no reference, even though the fixture row "+
			"carries one")

	require.Len(t, datasource.Calls, 1,
		"registration is ONE registry write: no broker is touched and nothing is read first")

	row, ok := datasource.Calls[0].Arguments[1].(*coremodel.EventSubscriber)
	require.True(t, ok, "the repository receives the assembled row")
	assert.Equal(t, subscriberID, row.SubscriberID)
	assert.Equal(t, stored.KafkaPrincipal, row.KafkaPrincipal,
		"the principal must be DERIVED before the write; an empty one would make the "+
			"subscriber unprovisionable")
	assert.Equal(t, stored.ConsumerGroupID, row.ConsumerGroupID)
	assert.Equal(t, []string{topic}, row.AuthorizedTopics)
	assert.Nil(t, row.CredentialReference,
		"registration records no credential: a freshly registered subscriber can read nothing "+
			"until issuance is called for it")
	assert.Nil(t, row.WebhookURL,
		"and it cannot record a legacy URL either: that is the guarded route's job")

	datasource.AssertExpectations(t)
}

// TestCreateSubscriber_RefusesAnUnbindableBodyBeforeTheRegistry covers the binding failures.
//
// Each case is refused with a TYPED code rather than a panic recovered by the middleware,
// and — the part worth asserting — none of them reaches the store. A body that cannot be
// bound cannot describe a subscriber, so a write attempted from it would be a write of
// whatever the zero value happened to be.
func TestCreateSubscriber_RefusesAnUnbindableBodyBeforeTheRegistry(t *testing.T) {
	for name, expected := range map[string]struct {
		body string
		code apierror.ErrorCode
	}{
		"malformed json":            {`{`, apierror.ErrGenMalformedRequest},
		"an absent body":            {"", apierror.ErrGenMalformedRequest},
		"a field of the wrong type": {`{"name":42}`, apierror.ErrGenMalformedRequest},
		"more than one json value":  {`{"name":"a"}{"name":"b"}`, apierror.ErrGenMalformedRequest},
		"a bare array":              {`[{"name":"a"}]`, apierror.ErrGenMalformedRequest},
		"a field the shape does not declare": {
			`{"name":"probe","authorised_topics":["blnk.transactions"]}`,
			apierror.ErrGenValidation,
		},
		"a missing name": {`{}`, apierror.ErrGenValidation},
		"a blank name":   {`{"name":"   "}`, apierror.ErrGenValidation},
		"a non-canonical identifier": {
			`{"subscriber_id":"Sub_ABC!","name":"probe"}`, apierror.ErrGenValidation,
		},
		"a topic outside the grantable set": {
			`{"name":"probe","authorized_topics":["someone.elses.topic"]}`,
			apierror.ErrGenValidation,
		},
	} {
		target := expected

		t.Run(name, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

			recorder := subscribersServe(t, router, subscribersCall{
				method: http.MethodPost, path: "/subscribers", body: target.body,
			})

			assertErrorCode(t, recorder, http.StatusBadRequest, target.code)
			assert.Empty(t, datasource.Calls,
				"a body that cannot describe a subscriber must not reach the registry")
		})
	}
}

// TestListSubscribers_NormalisesEveryPageBoundTheSameWay proves the normalisation by
// reading the query the repository actually received.
//
// Asserting on the response cannot distinguish "the bound was normalised" from "the store
// happened to return few rows", which is why the captured argument is the assertion. The
// rule matches ParseFiltersFromBody in api/filter_helper.go exactly, INCLUDING its habit of
// resetting an oversized limit to the default rather than clamping it to the ceiling, so
// that every listing in this package answers a page request the same way.
func TestListSubscribers_NormalisesEveryPageBoundTheSameWay(t *testing.T) {
	for name, expected := range map[string]struct {
		query string
		limit int
	}{
		"an absent limit takes the default":       {"", subscriberPageDefaultLimit},
		"zero takes the default":                  {"?limit=0", subscriberPageDefaultLimit},
		"a negative limit takes the default":      {"?limit=-5", subscriberPageDefaultLimit},
		"above the ceiling takes the default":     {"?limit=1000", subscriberPageDefaultLimit},
		"exactly the ceiling is honoured":         {"?limit=100", subscriberPageMaxLimit},
		"a value inside the range is honoured":    {"?limit=7", 7},
		"the ceiling plus one takes the default":  {"?limit=101", subscriberPageDefaultLimit},
		"a sort order is accepted and is inert":   {"?sort_order=asc&limit=7", 7},
		"a sort column is accepted and is inert":  {"?sort_by=created_at&limit=7", 7},
		"an unknown sort order is still accepted": {"?sort_order=sideways&limit=7", 7},
	} {
		target := expected

		t.Run(name, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

			datasource.On("ListEventSubscribers", mock.Anything, mock.Anything).
				Return(coremodel.SubscriberPage{}, nil).Once()

			recorder := subscribersServe(t, router, subscribersCall{
				method: http.MethodGet, path: "/subscribers" + target.query,
			})

			require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())
			require.Len(t, datasource.Calls, 1)

			query, ok := datasource.Calls[0].Arguments[1].(coremodel.SubscriberPageQuery)
			require.True(t, ok, "the repository receives a typed page query")
			assert.Equal(t, target.limit, query.Limit,
				"%q must reach the repository as limit %d", target.query, target.limit)
			assert.Nil(t, query.Cursor,
				"no cursor was sent, so none may be invented: a fabricated position would "+
					"silently skip the first page")

			datasource.AssertExpectations(t)
		})
	}

	t.Run("a limit that is not an integer is refused rather than defaulted", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodGet, path: "/subscribers?limit=many",
		})

		assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenValidation)
		assert.Empty(t, datasource.Calls,
			"defaulting it would answer a different question from the one asked, with nothing "+
				"in the response to say so")
	})

	t.Run("include_count switches to the counting reading", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

		datasource.On("ListAndCountEventSubscribers", mock.Anything, mock.Anything).
			Return(coremodel.SubscriberPage{}, int64(3), nil).Once()

		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodGet, path: "/subscribers?include_count=true",
		})

		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var page SubscriberPageResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
		require.NotNil(t, page.TotalCount,
			"a caller that asked for the count must receive it, even when the page is empty")
		assert.Equal(t, int64(3), *page.TotalCount)

		datasource.AssertNotCalled(t, "ListEventSubscribers", mock.Anything, mock.Anything)
		datasource.AssertExpectations(t)
	})
}

// TestListSubscribers_AnswersAnEmptyRegistryWithAnEmptyPage keeps an empty listing a
// success.
//
// An empty collection is not a missing one. A 404 here would make a paging client treat
// "no subscribers yet" as "this endpoint does not exist", and a null data field would make
// a client that iterates it fail on a legitimate answer.
func TestListSubscribers_AnswersAnEmptyRegistryWithAnEmptyPage(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	datasource.On("ListEventSubscribers", mock.Anything, mock.Anything).
		Return(coremodel.SubscriberPage{}, nil).Once()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodGet, path: "/subscribers",
	})

	require.Equal(t, http.StatusOK, recorder.Code,
		"an empty registry is a successful reading, never a 404. body: %s", recorder.Body.String())

	// Asserted on the RAW bytes as well as the decoded shape, because `null` and `[]` both
	// decode into a nil slice and only one of them is a usable answer.
	assert.Contains(t, recorder.Body.String(), `"data":[]`,
		"the page must carry an empty ARRAY: null breaks every client that iterates it")
	assert.NotContains(t, recorder.Body.String(), `"data":null`)

	var page SubscriberPageResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
	assert.Empty(t, page.Data)
	assert.False(t, page.HasMore, "there is nothing further to fetch")
	assert.Empty(t, page.NextCursor,
		"and no cursor, because a cursor would invite a second request for the same nothing")
	assert.Nil(t, page.TotalCount, "the count was not asked for, so it is absent rather than zero")

	datasource.AssertExpectations(t)
}

// TestGetSubscriber_AnswersTheStoredSubscriberWithoutItsSecrets is the read contract.
//
// The fixture row deliberately carries BOTH columns a response must never disclose — the
// credential reference and the legacy webhook URL — so the assertions below can fail. A row
// that never held them would make this test pass against a projection that copies every
// field it is given.
func TestGetSubscriber_AnswersTheStoredSubscriberWithoutItsSecrets(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)

	datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).Return(stored, nil).Once()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodGet, path: "/subscribers/" + subscriberID,
	})

	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	raw := recorder.Body.String()
	assert.NotContains(t, raw, *stored.CredentialReference,
		"THE RAW REFERENCE MUST NOT REACH A RESPONSE. The fingerprint is what a caller gets, "+
			"and it is what makes two issuances comparable without disclosing the stored value")
	assert.NotContains(t, raw, `"credential_reference"`,
		"not under that key either")
	assert.NotContains(t, raw, *stored.WebhookURL,
		"and the registry read shape does not carry the legacy destination AT ALL, so a "+
			"third-party URL cannot leak through the surface that replaces it")
	assert.NotContains(t, raw, `"password"`)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	assert.Equal(t, subscriberID, body["subscriber_id"])
	assert.NotEmpty(t, body["subscriber_id_hash"],
		"the pseudonym every log line and alert annotation uses must be resolvable from a read")
	assert.NotEmpty(t, body["credential_fingerprint"],
		"a subscriber that HAS a credential reports its fingerprint: that is the "+
			"non-reversible handle the reference is reduced to")
	assert.NotEmpty(t, body["credential_issued_at"],
		"and the issuance instant, which is the other half of what is persisted about a "+
			"credential")

	datasource.AssertExpectations(t)
}

// TestSubscribersAPI_UntypedNotFoundBecomesTheSubscriberCode is the catch-all guard, and it
// is the one assertion in this file that pins a decision made in api/errors.go.
//
// classifyMessage ends with a broad {"not found"} entry that resolves to GEN_NOT_FOUND. A
// repository or service path that returns an untyped error whose text happens to contain
// those words would therefore be answered GEN_NOT_FOUND, and a client branching on the code
// could not tell a missing SUBSCRIBER from a missing anything else — while the status,
// which is 404 either way, would look perfectly correct.
//
// respondSubscriberRegistryError applies withUpgrade(GEN_NOT_FOUND, SUBSCRIBER_NOT_FOUND)
// for exactly this reason, and this is what fails if the upgrade is dropped. It is driven on
// every route that can reach a lookup, because the upgrade is applied per call site.
func TestSubscribersAPI_UntypedNotFoundBecomesTheSubscriberCode(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	// An UNTYPED error, which is the whole point: a typed apierror would already carry the
	// right code and the upgrade would never be exercised.
	untyped := fmt.Errorf("event subscriber %s not found", subscriberID)

	for name, target := range map[string]struct {
		call    subscribersCall
		program func(*mocks.MockDataSource)
	}{
		"read": {
			call: subscribersCall{method: http.MethodGet, path: "/subscribers/" + subscriberID},
			program: func(datasource *mocks.MockDataSource) {
				datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).
					Return(nil, untyped).Once()
			},
		},
		"update": {
			call: subscribersCall{
				method: http.MethodPut, path: "/subscribers/" + subscriberID,
				body: `{"name":"renamed"}`,
			},
			program: func(datasource *mocks.MockDataSource) {
				// The update takes the provisioning claim first, so this is where an unknown
				// subscriber is discovered on that route.
				datasource.On("ClaimSubscriberForProvisioning",
					mock.Anything, subscriberID, mock.Anything).Return("", untyped).Once()
			},
		},
		"delete": {
			call: subscribersCall{method: http.MethodDelete, path: "/subscribers/" + subscriberID},
			program: func(datasource *mocks.MockDataSource) {
				datasource.On("ClaimSubscriberForProvisioning",
					mock.Anything, subscriberID, mock.Anything).Return("", untyped).Once()
			},
		},
		"record a legacy webhook subscription": {
			call: subscribersCall{
				method: http.MethodPost, path: webhookSubscriptionPath(subscriberID),
				body: `{"webhook_url":"https://subscriber.example.com/blnk-events"}`,
			},
			program: func(datasource *mocks.MockDataSource) {
				datasource.On("RecordSubscriberWebhookURL",
					mock.Anything, subscriberID, mock.Anything).Return(untyped).Once()
			},
		},
		"read a legacy webhook subscription": {
			call: subscribersCall{
				method: http.MethodGet, path: webhookSubscriptionPath(subscriberID),
			},
			program: func(datasource *mocks.MockDataSource) {
				datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).
					Return(nil, untyped).Once()
			},
		},
	} {
		expected := target

		t.Run(name, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})
			expected.program(datasource)

			recorder := subscribersServe(t, router, expected.call)

			assertErrorCode(t, recorder, http.StatusNotFound, apierror.ErrSubscriberNotFound)

			var body struct {
				ErrorDetail apierror.APIError `json:"error_detail"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.NotEqual(t, apierror.ErrGenNotFound, body.ErrorDetail.Code,
				"the broad {\"not found\"} entry in api/errors.go's classifyMessage table won "+
					"and the subscriber-specific code was lost. The handler must route registry "+
					"failures through respondSubscriberRegistryError, which upgrades it")

			datasource.AssertExpectations(t)
		})
	}
}

// TestUpdateSubscriber_AnswersTheUpdatedSubscriber walks the mutable subset over HTTP.
//
// The sequence is asserted, not just the status. The update takes the provisioning claim
// BEFORE it reads the row and persists under that claim's token, which is what stops a
// concurrent credential issuance and an update interleaving into a row that describes
// neither. A handler that wrote without the token would still answer 200.
func TestUpdateSubscriber_AnswersTheUpdatedSubscriber(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)

	renamed := *stored
	renamed.Name = "renamed consumer"

	const claimToken = "claim-token-for-the-update"

	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return(claimToken, nil).Once()
	datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).Return(stored, nil).Once()
	datasource.On("UpdateEventSubscriber", mock.Anything, mock.Anything, claimToken).
		Return(&renamed, nil).Once()
	// The fence's heartbeat and its release run on their own schedules, so both are
	// permitted rather than required: requiring them would make this test assert the
	// timing of a background goroutine.
	datasource.On("RenewSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("ReleaseSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPut, path: "/subscribers/" + subscriberID,
		body: `{"name":"renamed consumer"}`,
	})

	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	assert.Equal(t, "renamed consumer", body["name"])
	assert.NotContains(t, recorder.Body.String(), *stored.CredentialReference,
		"an update discloses no more than a read does")

	written, ok := findSubscribersCallArgument(datasource, "UpdateEventSubscriber")
	require.True(t, ok, "the repository must receive the row to persist")
	assert.Equal(t, "renamed consumer", written.Name)
	assert.Equal(t, stored.AuthorizedTopics, written.AuthorizedTopics,
		"an omitted field means LEAVE AS STORED; collapsing it to the zero value would "+
			"revoke every topic grant on a rename")

	datasource.AssertExpectations(t)
}

// findSubscribersCallArgument returns the *EventSubscriber a recorded call was given.
//
// Reaching into Mock.Calls rather than using a capture closure keeps the expectation
// declarations readable, and it works for a call that may not have happened — which is what
// lets the caller assert its absence rather than dereference a nil.
func findSubscribersCallArgument(
	datasource *mocks.MockDataSource, method string,
) (*coremodel.EventSubscriber, bool) {
	for _, call := range datasource.Calls {
		if call.Method != method {
			continue
		}

		for _, argument := range call.Arguments {
			if row, ok := argument.(*coremodel.EventSubscriber); ok {
				return row, true
			}
		}
	}

	return nil, false
}

// TestDeleteSubscriber_AnswersNoContentWithAnEmptyBody pins the deregistration response
// exactly.
//
// 204 with an empty body is the shape RevokeAPIKey already answers in, and the exactness is
// the point: a 200 carrying the deleted row would tell a client that a resource it must
// treat as gone still has a representation, and a body on a 204 is a protocol violation
// some proxies drop and others forward.
func TestDeleteSubscriber_AnswersNoContentWithAnEmptyBody(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)
	// A subscriber that HOLDS a credential cannot be deregistered without a reachable
	// broker — see the sub-test below, which is the fail-closed half of this contract. This
	// one is about the success shape, so the row records no issuance.
	stored.CredentialReference = nil
	stored.CredentialIssuedAt = nil

	const claimToken = "claim-token-for-the-deregistration"

	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return(claimToken, nil).Once()
	datasource.On("MarkSubscriberRevocationPending",
		mock.Anything, subscriberID, mock.Anything, claimToken).Return(stored, nil).Once()
	datasource.On("TakeEventSubscriber", mock.Anything, subscriberID, claimToken).
		Return(stored, nil).Once()
	datasource.On("RenewSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("ReleaseSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodDelete, path: "/subscribers/" + subscriberID,
	})

	require.Equal(t, http.StatusNoContent, recorder.Code,
		"deregistration answers 204, the shape RevokeAPIKey established. body: %s",
		recorder.Body.String())
	assert.Empty(t, recorder.Body.String(),
		"and carries no body: a deleted subscriber has no representation left to return")

	// THE REVOCATION IS MARKED BEFORE THE ROW IS TAKEN, which is what makes a failure
	// between the two recoverable: the subscriber is left recorded as awaiting revocation
	// rather than vanishing while its principal still authenticates at the broker.
	marked, taken := -1, -1
	for index, call := range datasource.Calls {
		switch call.Method {
		case "MarkSubscriberRevocationPending":
			marked = index
		case "TakeEventSubscriber":
			taken = index
		}
	}
	require.NotEqual(t, -1, marked, "the revocation must be recorded")
	require.NotEqual(t, -1, taken, "and the row removed")
	assert.Less(t, marked, taken,
		"a row removed before its revocation is recorded leaves broker access with nothing "+
			"left to describe or audit it")

	datasource.AssertExpectations(t)

	t.Run("a subscriber holding a credential is not deleted without a broker", func(t *testing.T) {
		// FAIL-CLOSED, and it is the more important half of deregistration. Deleting the row
		// of a principal that still authenticates would leave live Kafka access with nothing
		// left to describe or audit it — strictly worse than not deleting at all — so the
		// refusal is 503 and retryable rather than a silent registry-only delete.
		guarded, store := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

		credentialed := uniqueSubscriberID()
		row := subscribersFixtureRow(credentialed)
		require.NotNil(t, row.CredentialReference,
			"the fixture must record an issuance, or this asserts nothing")

		store.On("ClaimSubscriberForProvisioning", mock.Anything, credentialed, mock.Anything).
			Return("guarded-claim-token", nil).Once()
		store.On("MarkSubscriberRevocationPending",
			mock.Anything, credentialed, mock.Anything, mock.Anything).Return(row, nil).Maybe()
		store.On("RenewSubscriberProvisioningFence",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		store.On("ReleaseSubscriberProvisioningFence",
			mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		refused := subscribersServe(t, guarded, subscribersCall{
			method: http.MethodDelete, path: "/subscribers/" + credentialed,
		})

		assertErrorCode(t, refused, http.StatusServiceUnavailable, apierror.ErrKafkaUnavailable)
		store.AssertNotCalled(t, "TakeEventSubscriber",
			mock.Anything, mock.Anything, mock.Anything)
	})
}

// TestSubscribersAPI_BlankIdentifierIsAMissingParameterOnEveryRoute keeps the two parameter
// refusals distinct across the whole surface.
//
// A blank-but-present segment is a MISSING parameter; a present-but-unusable one is a
// VALIDATION error. Collapsing them would tell a caller that omitted the identifier that its
// identifier is invalid, and the two demand opposite fixes. Every per-subscriber route reads
// the parameter through the same helper, so this is asserted on all of them: a regression in
// one is a regression in all, and only a table shows that.
func TestSubscribersAPI_BlankIdentifierIsAMissingParameterOnEveryRoute(t *testing.T) {
	// A single encoded space: present in the path, blank once trimmed.
	const blank = "%20"

	for _, call := range []subscribersCall{
		{method: http.MethodGet, path: "/subscribers/" + blank},
		{method: http.MethodPut, path: "/subscribers/" + blank, body: `{"name":"renamed"}`},
		{method: http.MethodDelete, path: "/subscribers/" + blank},
		{method: http.MethodPost, path: "/subscribers/" + blank + "/kafka-credentials"},
		{
			method: http.MethodPost, path: webhookSubscriptionPath(blank),
			body: `{"webhook_url":"https://subscriber.example.com/blnk-events"}`,
		},
		{method: http.MethodGet, path: webhookSubscriptionPath(blank)},
		{
			method: http.MethodPut, path: webhookSubscriptionPath(blank),
			body: `{"webhook_url":"https://subscriber.example.com/blnk-events"}`,
		},
		{method: http.MethodDelete, path: webhookSubscriptionPath(blank)},
	} {
		target := call

		t.Run(target.method+" "+target.path, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

			recorder := subscribersServe(t, router, target)

			assertErrorCode(t, recorder, http.StatusBadRequest, apierror.ErrGenMissingParameter)
			assert.Empty(t, datasource.Calls,
				"a value refusable in memory must not cost a database round trip")
		})
	}
}

// TestIssueKafkaCredentials_AnswersAtTheCeilingWhenProvisioningStalls is requirement R-7 as
// an HTTP outcome rather than as a documented intention.
//
// # What is under test
//
// R-7 requires credential provisioning to complete within five seconds. The handler bounds
// the service's context to exactly that, and that bound alone is NOT the guarantee: the
// service's compensating writes and its fence release deliberately run on FRESH bounded
// contexts detached from the caller's, because an expired issuance deadline is one of the
// commonest reasons a cleanup is needed and a cleanup on an already-cancelled context does
// nothing. Those fresh budgets are the same five seconds, so a synchronous chain of
// issuance, compensation and release is individually bounded and collectively fifteen
// seconds long.
//
// issueWithinBudget separates the REQUEST from the WORK, and this test is the only place
// that separation is observable end to end: the store below blocks until the test releases
// it, so a handler that waited for the work would hold the request open indefinitely.
//
// # The assertions, and why each bound is there
//
//	Not less than the budget — an earlier answer would mean the ceiling is not what
//	produced it, and the test would be passing for another reason.
//	Not much more than the budget — this is the fifteen-second wall clock the ceiling closes.
//	Not never — the request must not hang, which is the failure a timeout-less handler has.
//
// # The code it answers with
//
// SUBSCRIBER_PROVISIONING_TIMEOUT (504), which is the implemented contract and is asserted
// as such deliberately. Every deadline expiry in the issuance path reports this one code, so
// a client's retry policy does not depend on which layer noticed the expiry first; answering
// SUBSCRIBER_PROVISIONING_FAILED (503) here would additionally assert that a dependency is
// unavailable, which is a different fact from "we ran out of time".
//
// The block is bounded and released with defer, so this test cannot hang the suite even if
// every assertion in it fails.
func TestIssueKafkaCredentials_AnswersAtTheCeilingWhenProvisioningStalls(t *testing.T) {
	// Brokers must be configured or issuance refuses before the store is reached at all —
	// the address is never dialled, because the stall happens first.
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
	})

	subscriberID := uniqueSubscriberID()

	release := make(chan time.Time)
	// RELEASED EXACTLY ONCE, whether this test reaches its join or fails before it. Closing a
	// channel makes every pending receive return, so the stalled call always finishes and its
	// goroutine always exits; the Once is what lets the join at the end of this test release the
	// stall explicitly while a deferred safety net still covers an early failure.
	var releaseOnce sync.Once
	releaseStall := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseStall()

	// The provisioning claim is the FIRST store call issuance makes, which is what keeps the
	// stall to exactly one method: the service returns before registering its compensation
	// when the claim fails, so nothing else is reached on the way out.
	//
	// The error it eventually returns is the one the REAL repository returns for a claim it could
	// not take — an internal-server-class APIError, exactly as database.ClaimSubscriberForProvisioning
	// builds — rather than a bare error. It changes nothing about the response asserted below,
	// which comes from the ceiling and not from this error, and it makes the abandoned attempt
	// take the path a real one takes: the service reclassifies a spent budget at the registry as a
	// timeout and says so, which is what requireAbandonedIssuanceSettled waits for at the end.
	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		WaitUntil(release).
		Return("", apierror.NewAPIError(apierror.ErrInternalServer,
			"Failed to claim the subscriber for provisioning",
			fmt.Errorf("the provisioning claim was abandoned")))

	logs := logtest.NewGlobal()
	defer logs.Reset()

	started := time.Now()
	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})
	elapsed := time.Since(started)

	assertErrorCode(t, recorder,
		http.StatusGatewayTimeout, apierror.ErrSubscriberProvisioningTimeout)

	assert.GreaterOrEqual(t, elapsed, blnk.SubscriberCredentialIssuanceBudget,
		"answering EARLIER than the budget means something other than the ceiling produced "+
			"this response, and the test would be proving nothing")
	assert.Less(t, elapsed, blnk.SubscriberCredentialIssuanceBudget+2*time.Second,
		"the request must end at its own ceiling. Waiting for the work instead is the "+
			"fifteen-second wall clock of issuance, then compensation, then the fence release")

	// NO SECRET ON THE ABANDONED PATH. The handler receives a zero-valued credential when
	// the ceiling fires, so there is nothing to serialise — and the message says a credential
	// may nevertheless exist at the broker, because that is the one thing the caller cannot
	// otherwise know and the thing that decides what it should do next.
	assert.NotContains(t, recorder.Body.String(), `"password"`,
		"an abandoned issuance returns no credential field at all")
	assert.Contains(t, recorder.Body.String(), "Re-issue to obtain one",
		"the caller is told the remedy: Kafka holds one credential per principal, so "+
			"re-issuing replaces whatever the abandoned attempt left behind")

	// THE LOG LINE PSEUDONYMISES THE SUBSCRIBER. A subscriber id is a tenant-chosen name
	// that reaches logs, alert annotations and incident tickets, and every other layer emits
	// the keyed digest under a _hash key. Emitting the raw name on the one line an operator
	// goes looking for after a timeout would make the pseudonyms everywhere else resolvable
	// by anyone reading the same stream.
	timeoutLogged := false
	for _, entry := range logs.AllEntries() {
		if !strings.Contains(entry.Message, "exceeded the issuance budget") {
			continue
		}

		timeoutLogged = true
		assert.NotContains(t, entry.Message, subscriberID,
			"the raw subscriber id must not appear in the message")
		hash, ok := entry.Data["subscriber_id_hash"]
		require.True(t, ok, "the pseudonym is what correlates this line with the service's own")
		assert.NotEmpty(t, hash)
		assert.NotEqual(t, subscriberID, hash, "and it must be a digest, not the name itself")
	}
	assert.True(t, timeoutLogged,
		"an abandoned issuance must say so in the log: it is the only record that work may "+
			"still be running after the response was written")

	// JOIN THE ABANDONED ATTEMPT BEFORE THIS TEST ENDS.
	//
	// Everything above is about the REQUEST, and the request is over. What is still running is
	// the issuance goroutine, parked in the stalled store call. Releasing it and walking away —
	// which is what a bare `defer close(release)` amounted to — lets it wake up inside whatever
	// test runs next: it writes a Warn line through the GLOBAL logrus logger, and the global test
	// hook installed here is never uninstalled, so that line lands in this hook after Reset has
	// cleared it and in every hook a later test installs. A later test that scans the global log
	// then sees an entry no request of its own produced, and fails somewhere else entirely.
	//
	// So the stall is released HERE, explicitly, and the attempt is waited for.
	releaseStall()
	requireAbandonedIssuanceSettled(t, logs)
}

// subscribersAbandonedIssuanceMarker is the last thing an abandoned issuance does.
//
// The service classifies a budget spent on the provisioning claim as a timeout and says so at
// Warn before returning; nothing it does afterwards touches anything a test owns. That makes this
// line the completion signal, and asserting on it is worth doing in its own right: it is the only
// record that an attempt which had already been answered for went on to release its claim, and it
// is what an operator correlates with the request's own line.
const subscribersAbandonedIssuanceMarker = "ran out of time at the registry"

// subscribersAbandonedIssuanceTimeout bounds the wait for that line.
//
// It is generous because it is never reached in a correct run — the attempt is already released
// and has one classification left to perform — and because reaching it means the attempt did not
// finish, which is a real leak rather than a slow machine.
const subscribersAbandonedIssuanceTimeout = 10 * time.Second

// requireAbandonedIssuanceSettled waits for an abandoned credential issuance to finish, and FAILS
// if it never does.
//
// The wait is a poll of the global log hook rather than a channel, because the goroutine being
// waited for is started inside the handler by issueWithinBudget and is deliberately unobservable
// from outside: the handler hands the service call a context and a callback and keeps no handle,
// which is exactly what lets the request answer at its ceiling. The hook is mutex-protected, so
// polling it while the attempt is still writing to it is safe.
//
// Parameters:
//   - t *testing.T: the test, FAILED when the abandoned attempt does not settle.
//   - logs *logtest.Hook: the global hook the attempt writes its closing line to.
func requireAbandonedIssuanceSettled(t *testing.T, logs *logtest.Hook) {
	t.Helper()

	deadline := time.Now().Add(subscribersAbandonedIssuanceTimeout)
	for {
		for _, entry := range logs.AllEntries() {
			if strings.Contains(entry.Message, subscribersAbandonedIssuanceMarker) {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"the abandoned issuance did not report its own outcome within %s. It is released "+
					"by now, so either it is still running — and will write to the global logger "+
					"and to this hook after this test has ended, inside whichever test runs next "+
					"— or it stopped classifying a spent budget at the registry as a timeout",
				subscribersAbandonedIssuanceTimeout,
			)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// TestIssueKafkaCredentials_RefusesBeforeTheRegistryWhenNoBrokerIsConfigured pins the
// graceful-degradation contract on the one route that cannot degrade.
//
// Every other part of Blnk runs unchanged without Kafka — the publisher resolves to a no-op,
// exactly as SendWebhook no-ops without a configured URL. Credential issuance is the
// exception, because there is no useful credential for a broker that does not exist, and the
// refusal has to be the RIGHT refusal: 503 EVENT_KAFKA_UNAVAILABLE names the dependency and
// says a retry may succeed, while a 500 sends an operator looking for a defect in Blnk.
//
// It must also refuse before touching the registry. A provisioning claim taken for an
// issuance that cannot proceed would block a concurrent update for the lease's duration
// against a subscriber nothing was ever going to be issued for.
func TestIssueKafkaCredentials_RefusesBeforeTheRegistryWhenNoBrokerIsConfigured(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()

	started := time.Now()
	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})
	elapsed := time.Since(started)

	assertErrorCode(t, recorder, http.StatusServiceUnavailable, apierror.ErrKafkaUnavailable)

	require.NotEqual(t, http.StatusInternalServerError, recorder.Code,
		"an unconfigured dependency is not a server defect")
	assert.Empty(t, datasource.Calls,
		"no broker means no issuance, so the provisioning claim must not be taken: it would "+
			"fence a subscriber against concurrent updates for nothing")
	assert.Less(t, elapsed, time.Second,
		"a configuration answer is immediate; spending the budget on it would make a "+
			"misconfigured deployment look like a slow one")
	assert.NotContains(t, recorder.Body.String(), `"password"`)
}

// TestIssueKafkaCredentials_RefusesAnUnconfidentialTransportBeforeMintingAnything covers the
// gate that exists only on this route.
//
// This is the one endpoint in Blnk whose response body carries a secret, so an authorised
// request over a channel nobody has established as confidential is an authorised credential
// leak. The refusal must come BEFORE the broker is touched: a password refused after issuance
// would already exist at the broker, unusable by the caller that never received it and still
// requiring revocation.
//
// The request below is PLAINTEXT with a remote peer: no in-process TLS, no declared proxy, and
// no loopback. It is the negative control for the whole harness, which presents in-process TLS
// so that every other test asserts against the handler rather than against this gate.
func TestIssueKafkaCredentials_RefusesAnUnconfidentialTransportBeforeMintingAnything(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
	})

	subscriberID := uniqueSubscriberID()

	recorder := subscribersServe(t, router, subscribersCall{
		method:    http.MethodPost,
		path:      "/subscribers/" + subscriberID + "/kafka-credentials",
		peer:      "192.0.2.1:1234",
		plaintext: true,
	})

	assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
	assert.Empty(t, datasource.Calls,
		"nothing may be claimed, read or minted for a request that cannot be answered safely")

	// The refusal names all three ways to satisfy the contract, because each is a deployment
	// change and the operator reading the message is the person who makes it.
	body := recorder.Body.String()
	for _, remedy := range []string{
		"BLNK_SERVER_SSL",
		"BLNK_SERVER_TRUST_FORWARDED_PROTO",
		"BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE",
	} {
		assert.Contains(t, body, remedy,
			"the refusal must name every way to establish a confidential channel")
	}
	assert.Contains(t, body, "local development only",
		"and it must say which of the three is not a production answer, or an operator reaches for "+
			"the easiest one on a host where it is unsafe")
	assert.NotContains(t, body, "192.0.2.1",
		"and it must disclose nothing about the topology back to the caller: every refused "+
			"request gets the identical body")
}

// TestIssueKafkaCredentials_RefusesALoopbackPeerOnAnUndeclaredHost is the production half of the
// transport gate, and the one that closes a real disclosure.
//
// # Why a loopback peer proves less than it appears to
//
// Request.RemoteAddr is the far end of the accepted socket, so a loopback value establishes that
// the LAST hop stayed on this host. It establishes nothing about the hop before it. The shape
// that matters is ordinary: a reverse proxy on the same host — nginx, Caddy, an Envoy sidecar —
// accepts a request from the internet, possibly over plain http, and forwards it to Blnk over
// 127.0.0.1. From inside the process that request is indistinguishable from an operator running
// curl in the container, and answering it puts a one-time SASL password on the public hop.
//
// Blnk cannot tell those apart, so it does not try: the loopback channel is a DECLARATION the
// deployment makes, exactly like the trusted-proxy one, and the default is to refuse. This test
// is the default.
func TestIssueKafkaCredentials_RefusesALoopbackPeerOnAnUndeclaredHost(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
	})

	subscriberID := uniqueSubscriberID()

	for name, peer := range map[string]string{
		"IPv4 loopback":        "127.0.0.1:54321",
		"the 127/8 block":      "127.9.9.9:54321",
		"IPv6 loopback":        "[::1]:54321",
		"a zone-qualified ::1": "[::1%lo0]:54321",
	} {
		t.Run(name, func(t *testing.T) {
			recorder := subscribersServe(t, router, subscribersCall{
				method:    http.MethodPost,
				path:      "/subscribers/" + subscriberID + "/kafka-credentials",
				peer:      peer,
				plaintext: true,
			})

			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
			assert.NotContains(t, recorder.Body.String(), `"password"`,
				"no secret may be disclosed to a peer whose earlier hops this process cannot see")
		})
	}

	assert.Empty(t, datasource.Calls,
		"and nothing may be claimed or minted: the refusal precedes the registry, so a request "+
			"that cannot be answered safely leaves no provisioning fence behind")
}

// TestIssueKafkaCredentials_AllowsALoopbackPeerOnADeclaredLocalDevelopmentHost is the exception,
// tested separately from the production posture on purpose.
//
// The local stack is a plaintext listener reached over loopback and nothing else, so without this
// declaration `make run` plus curl could never issue a credential. What the test pins is that the
// exception is reached ONLY through the declaration: the same request refused above is admitted
// here, and the only difference is BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE.
//
// The assertion is that the transport gate PASSED, not that a credential was issued. Issuance
// then fails against the unreachable broker in this harness, which is a different code — and
// asserting the transport code's ABSENCE is what distinguishes "the gate admitted it" from "the
// gate refused it" without needing a live broker.
func TestIssueKafkaCredentials_AllowsALoopbackPeerOnADeclaredLocalDevelopmentHost(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey:             true,
		brokers:               []string{"127.0.0.1:9092"},
		allowLoopbackIssuance: true,
	})

	subscriberID := uniqueSubscriberID()

	// The registry read the gate now lets the request reach. It answers "no such subscriber", so
	// the handler's own refusal is the 404 — which is only reachable past the transport gate.
	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return("", apierror.NewAPIError(apierror.ErrSubscriberNotFound, "no such subscriber", nil))

	recorder := subscribersServe(t, router, subscribersCall{
		method:    http.MethodPost,
		path:      "/subscribers/" + subscriberID + "/kafka-credentials",
		peer:      "127.0.0.1:54321",
		plaintext: true,
	})

	body := recorder.Body.String()
	assert.NotContains(t, body, string(apierror.ErrSubscriberInsecureTransport),
		"the declared local-development host must satisfy the transport gate: %s", body)
	assert.NotEqual(t, http.StatusForbidden, recorder.Code,
		"and the request must get past it rather than being refused for its channel")
	datasource.AssertCalled(t, "ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything)
}

// TestIssueKafkaCredentials_AllowsADeclaredProxyReportingHTTPS is the third channel, and the one a
// production Kubernetes deployment actually uses.
//
// TLS terminates at the ingress and the hop to the pod is plaintext, so the process never sees a
// handshake. X-Forwarded-Proto is a request header any client can set, so it is believed only
// where BLNK_SERVER_TRUST_FORWARDED_PROTO declares that a proxy sets it and overwrites what the
// client sent — AND only on a request that arrived from a proxy BLNK_SERVER_TRUSTED_PROXIES names.
//
// # Why the peer is a condition rather than a refinement (SEC-03)
//
// The declaration is a statement about the intended path: "requests reach this process through our
// ingress, which owns this header". It is true of that path and says nothing about a request that
// arrived by another one — a pod IP inside the cluster, a `kubectl port-forward`, a second Service,
// an ingress rule that passes the client's header straight through. Each of those can set
// `X-Forwarded-Proto: https` itself, and the response to this endpoint is a one-time SASL password,
// so being believed is the entire exploit. Requiring the socket peer to be a named proxy is what
// makes the declaration true of the request in hand rather than of the topology diagram.
//
// Every combination is exercised below because each one fails differently, and three of them used
// to be admitted: the header alone, the declaration with no allowlist, the declaration with a
// universal range, and a peer outside the allowlist.
func TestIssueKafkaCredentials_AllowsADeclaredProxyReportingHTTPS(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	// The proxy range the declaration applies to, and a peer inside it. Documentation ranges
	// (RFC 5737) so nothing here could resolve to a real host.
	const (
		proxyRange  = "192.0.2.0/24"
		proxyPeer   = "192.0.2.1:1234"
		outsidePeer = "198.51.100.7:1234"
	)

	t.Run("the header alone establishes nothing", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey: true,
			brokers:   []string{"127.0.0.1:9092"},
		})

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
		assert.Empty(t, datasource.Calls,
			"a caller must not be able to assert its own confidentiality with a header")
	})

	t.Run("the declaration with no named proxy establishes nothing either", func(t *testing.T) {
		// THE SEC-03 CASE. This combination was admitted, and it is the one a deployment reaches
		// by setting the flag and nothing else — so the refusal has to hold here or the peer
		// condition is decorative.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			// trustedProxies deliberately empty.
		})

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
		assert.Empty(t, datasource.Calls,
			"with no proxy named, the header is believed from every peer — which is the state the "+
				"declaration was supposed to replace")
		assert.Contains(t, recorder.Body.String(), "BLNK_SERVER_TRUSTED_PROXIES",
			"and the refusal must name the missing half, or an operator who set the flag has no "+
				"way to learn why it did not take effect")
	})

	t.Run("a universal range is not an allowlist", func(t *testing.T) {
		// 0.0.0.0/0 matches every peer, so honouring it would restore the forgeable behaviour
		// while reading as though a decision had been made. In secure mode configuration
		// validation refuses this outright; here — secure off, as the whole harness runs — the
		// channel simply establishes nothing.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			trustedProxies:      "0.0.0.0/0, ::/0",
		})

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
		assert.Empty(t, datasource.Calls)
	})

	t.Run("a peer outside the named proxies is refused", func(t *testing.T) {
		// The direct caller: the declaration is complete and correct, and this request simply did
		// not come through the proxy it describes.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			trustedProxies:      proxyRange,
		})

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           outsidePeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
		assert.Empty(t, datasource.Calls,
			"a caller that did not arrive through the declared proxy must not be handed the "+
				"one-time password on the strength of that proxy's declaration")
	})

	t.Run("the declared proxy is believed", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			trustedProxies:      proxyRange,
		})

		datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
			Return("", apierror.NewAPIError(apierror.ErrSubscriberNotFound, "no such subscriber", nil))

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assert.NotContains(t, recorder.Body.String(), string(apierror.ErrSubscriberInsecureTransport),
			"the declared proxy, reporting https, from a peer inside the declared range IS a "+
				"confidential channel — the production deployment must keep working")
		datasource.AssertCalled(t, "ClaimSubscriberForProvisioning",
			mock.Anything, subscriberID, mock.Anything)
	})

	t.Run("a bare IP names one proxy", func(t *testing.T) {
		// The single-instance shape. A bare literal is one of the two forms
		// BLNK_SERVER_TRUSTED_PROXIES documents, so it must work here without the operator
		// having to write /32.
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			trustedProxies:      "192.0.2.1",
		})

		datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
			Return("", apierror.NewAPIError(apierror.ErrSubscriberNotFound, "no such subscriber", nil))

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "https",
		})

		assert.NotContains(t, recorder.Body.String(), string(apierror.ErrSubscriberInsecureTransport),
			"a bare IP literal must be honoured as a single-address range: %s", recorder.Body.String())
		datasource.AssertCalled(t, "ClaimSubscriberForProvisioning",
			mock.Anything, subscriberID, mock.Anything)
	})

	t.Run("a declared proxy reporting http is a refusal", func(t *testing.T) {
		router, datasource := setupSubscribersRouter(t, subscribersHarness{
			masterKey:           true,
			brokers:             []string{"127.0.0.1:9092"},
			trustForwardedProto: true,
			trustedProxies:      proxyRange,
		})

		recorder := subscribersServe(t, router, subscribersCall{
			method:         http.MethodPost,
			path:           "/subscribers/" + subscriberID + "/kafka-credentials",
			peer:           proxyPeer,
			plaintext:      true,
			forwardedProto: "http",
		})

		assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrSubscriberInsecureTransport)
		assert.Empty(t, datasource.Calls,
			"the proxy is REPORTING a plaintext client hop, which is a refusal rather than an "+
				"absence of information")
	})
}

// TestIssueKafkaCredentials_RefusesAnUnknownSubscriber keeps the missing-row answer on the
// route that mints secrets.
//
// 404 rather than 503: an identifier that matches nothing is a client mistake, and answering
// it as a dependency failure would have a caller retry for ever against a subscriber that
// was never registered.
func TestIssueKafkaCredentials_RefusesAnUnknownSubscriber(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{
		masterKey: true,
		brokers:   []string{"127.0.0.1:9092"},
	})

	subscriberID := uniqueSubscriberID()

	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return("", fmt.Errorf("event subscriber %s not found", subscriberID)).Once()

	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})

	assertErrorCode(t, recorder, http.StatusNotFound, apierror.ErrSubscriberNotFound)
	assert.NotContains(t, recorder.Body.String(), `"password"`)

	datasource.AssertExpectations(t)
}

// subscribersKafkaEnvironment resolves the broker environment for the end-to-end issuance
// test, or explains what is missing.
//
// It returns a REASON rather than skipping itself, so the caller decides. The variable names
// are the un-prefixed ones the deployment contract mandates, which is what .env, both
// compose files and event_isolation_integration_test.go all use.
//
// KAFKA_SUBSCRIBER_BROKERS is required and deliberately NOT defaulted to KAFKA_BROKERS. A
// credential names the addresses the SUBSCRIBER will dial, which in a real deployment is the
// broker's external listener rather than the internal bootstrap list Blnk itself uses.
// Issuance refuses when it is unset rather than falling back, so that a deployment cannot
// hand out its internal addresses by omission — and defaulting it here would make this test
// pass while that refusal went unexercised.
//
// Nothing is written to the environment: configuration reaches the service only through
// config.MockConfig, and these values are READ so the test can tell whether a broker exists.
func subscribersKafkaEnvironment(t *testing.T) (config.KafkaConfig, string, bool) {
	t.Helper()

	split := func(raw string) []string {
		parts := strings.Split(raw, ",")
		brokers := make([]string, 0, len(parts))
		for _, part := range parts {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				brokers = append(brokers, trimmed)
			}
		}

		return brokers
	}

	brokers := split(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return config.KafkaConfig{},
			"KAFKA_BROKERS is not set, so there is no broker to mint a real SASL credential at",
			false
	}

	subscriberBrokers := split(os.Getenv("KAFKA_SUBSCRIBER_BROKERS"))
	if len(subscriberBrokers) == 0 {
		return config.KafkaConfig{},
			"KAFKA_SUBSCRIBER_BROKERS is not set, so no subscriber-facing broker list exists and " +
				"issuance refuses by design. For the local stack it is the broker's external " +
				"listener, the value .env.example and docker-compose.yaml both carry",
			false
	}

	adminUser := strings.TrimSpace(os.Getenv("KAFKA_SASL_ADMIN_USER"))
	adminSecret := os.Getenv("KAFKA_SASL_ADMIN_SECRET")
	if adminUser == "" || adminSecret == "" {
		return config.KafkaConfig{},
			"KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET must both be set: minting a SCRAM " +
				"credential and binding its ACLs are administrative operations",
			false
	}

	producer := strings.TrimSpace(os.Getenv("KAFKA_SASL_USER"))
	producerSecret := os.Getenv("KAFKA_SASL_SECRET")
	if producer == "" || producerSecret == "" {
		return config.KafkaConfig{},
			"KAFKA_SASL_USER and KAFKA_SASL_SECRET must both be set: the service container " +
				"refuses to publish as the administrator, so a server-role Blnk cannot be built " +
				"without a dedicated producer principal",
			false
	}

	tlsEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("KAFKA_TLS_ENABLED")), "true")

	return config.KafkaConfig{
		Brokers:           brokers,
		SubscriberBrokers: subscriberBrokers,
		TopicPrefix:       strings.TrimSpace(os.Getenv("KAFKA_TOPIC_PREFIX")),
		SASLUser:          producer,
		SASLSecret:        producerSecret,
		SASLAdminUser:     adminUser,
		SASLAdminSecret:   adminSecret,
		MinPartitions:     blnk.MinTopicPartitions,
		ReplicationFactor: 1,
		TLS: config.KafkaTLSConfig{
			Enabled:    tlsEnabled,
			CAFile:     strings.TrimSpace(os.Getenv("KAFKA_TLS_CA_FILE")),
			ServerName: strings.TrimSpace(os.Getenv("KAFKA_TLS_SERVER_NAME")),
		},
		// The transport refuses to dial in the clear without this acknowledgement, which is
		// the right default for production and exactly what the local single-broker KRaft
		// stack needs waived. Scoped to the TLS-off case so an operator running against a
		// TLS broker never sees it applied.
		InsecureLocalDev: !tlsEnabled,
	}, "", true
}

// TestIssueKafkaCredentials_ReturnsTheConnectionDetailsAndTheSecretExactlyOnce is the
// highest-value test in this file, and the only one in this package that can prove the
// secret-handling posture rather than describe it.
//
// # Why it needs a real broker
//
// blnk.SubscriberCredential holds its password in an unexported field with no exported
// constructor, deliberately, so no test outside the root package can fabricate one. A
// plaintext therefore reaches a response only through a real issuance, which means a real
// SCRAM credential and real ACL bindings at a real broker. It skips with a reason when none
// is configured; the ordinary suite runs in that state, and every other assertion about this
// route above is reachable without one.
//
// # The four properties it establishes
//
//	THE RESPONSE CONTRACT (R-7): the broker endpoint, the authorised topic list, the consumer
//	group and the SASL credentials — username, password, mechanism — are all present and
//	non-empty. A subscriber that receives any one of them empty cannot connect.
//
//	THE BUDGET, from the other side: a real issuance completes far inside five seconds. The
//	ceiling test above proves the request ends when the budget elapses; this proves the
//	budget is not itself the thing making requests slow.
//
//	THE SECRET IS RETURNED ONCE. Asserted against the RAW BYTES of every subsequent read,
//	not against a decoded struct, so that a stray field under any key would fail. The row is
//	read back through the datasource as well, which is what proves nothing capable of holding
//	the plaintext was written.
//
//	THE SECRET IS NEVER LOGGED. Every logrus entry emitted during the issuance is captured
//	and searched, message and fields alike.
//
// The store is a mock so that the arguments the credential record was written with can be
// inspected directly. Cleanup deregisters through the API, which revokes the principal and
// its bindings at the broker rather than leaving them behind.
func TestIssueKafkaCredentials_ReturnsTheConnectionDetailsAndTheSecretExactlyOnce(t *testing.T) {
	kafkaConfig, reason, ok := subscribersKafkaEnvironment(t)
	if !ok {
		t.Skip(reason)
	}

	configuration := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_subs",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Server: config.ServerConfig{
			Secure:    false,
			SecretKey: subscribersAPITestMasterKey,
		},
		Kafka: kafkaConfig,
		// A FUTURE instant, for the reason given on setupSubscribersRouter: configuration
		// validation refuses brokers without one. The retirement itself is not exercised here.
		WebhookDeprecationSunsetDate: time.Now().Add(20 * 24 * time.Hour).UTC().Format(time.RFC3339),
	}
	config.MockConfig(configuration)

	fetched, err := config.Fetch()
	require.NoError(t, err)
	require.NotEmpty(t, fetched.Kafka.Brokers,
		"config.MockConfig rejected the broker configuration; its reason is on the error log above")

	datasource := new(mocks.MockDataSource)

	service, err := blnk.NewBlnk(datasource)
	require.NoError(t, err)

	instance := NewAPI(service)
	require.NotNil(t, instance)
	instance.router.Use(func(c *gin.Context) {
		c.Set("isMasterKey", true)
		c.Next()
	})
	router := instance.Router()

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)
	// A subscriber that has never been issued for, so `replaced` is meaningful and the
	// credential record is written rather than compared against a stale reference.
	stored.CredentialReference = nil
	stored.CredentialIssuedAt = nil
	stored.AuthorizedTopics = coremodel.SubscriberGrantableTopics(fetched.Kafka.TopicPrefix)[:1]

	const claimToken = "claim-token-for-the-issuance"

	datasource.On("ClaimSubscriberForProvisioning", mock.Anything, subscriberID, mock.Anything).
		Return(claimToken, nil)
	datasource.On("GetEventSubscriberByID", mock.Anything, subscriberID).Return(stored, nil)
	datasource.On("RecordSubscriberCredentialIfUnchanged",
		mock.Anything, subscriberID, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	datasource.On("RenewSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	datasource.On("ReleaseSubscriberProvisioningFence",
		mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	// Deregistration, for cleanup: it is what revokes the SCRAM credential and every ACL
	// binding this test creates at the broker.
	datasource.On("MarkSubscriberRevocationPending",
		mock.Anything, subscriberID, mock.Anything, mock.Anything).Return(stored, nil).Maybe()
	datasource.On("TakeEventSubscriber", mock.Anything, subscriberID, mock.Anything).
		Return(stored, nil).Maybe()

	t.Cleanup(func() {
		recorder := subscribersServe(t, router, subscribersCall{
			method: http.MethodDelete, path: "/subscribers/" + subscriberID,
		})
		if recorder.Code != http.StatusNoContent && recorder.Code != http.StatusOK {
			t.Errorf("cleanup: deregistering %s answered %d, so a live SCRAM principal and its "+
				"ACL bindings may have been left at the broker: %s",
				subscriberID, recorder.Code, safeResponseBody(recorder))
		}
	})

	logs := logtest.NewGlobal()
	defer logs.Reset()

	started := time.Now()
	recorder := subscribersServe(t, router, subscribersCall{
		method: http.MethodPost, path: "/subscribers/" + subscriberID + "/kafka-credentials",
	})
	elapsed := time.Since(started)

	// SANITISED, and this is the assertion that made it necessary: the message renders only when
	// the status is NOT 200, which is precisely the regression — a handler answering 500 while
	// still marshalling the credential — that would write a real SASL password into CI output.
	// The assertions below that forbid a secret in the body cannot help, because a failed
	// require aborts before they run.
	require.Equal(t, http.StatusOK, recorder.Code,
		"a successful issuance answers 200. %s", safeResponseBody(recorder))

	var credential model.KafkaCredentialsResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &credential))

	// 1. THE RESPONSE CONTRACT.
	assert.NotEmpty(t, credential.BrokerEndpoint,
		"the subscriber is told where to connect, as one endpoint string")
	assert.NotEmpty(t, credential.Brokers, "and as the bootstrap list")
	assert.Equal(t, stored.AuthorizedTopics, credential.AuthorizedTopics,
		"the grant it was actually given, which is the boundary the broker enforces")
	assert.Equal(t, stored.ConsumerGroupID, credential.ConsumerGroupID,
		"the group it reads under, granted as a prefixed namespace")
	assert.Equal(t, stored.KafkaPrincipal, credential.Username,
		"the SASL username is the derived principal every binding was granted to")
	assert.NotEmpty(t, credential.Mechanism, "and the mechanism to authenticate with")
	assert.False(t, credential.IssuedAt.IsZero(), "and the instant it was issued")
	assert.NotEmpty(t, credential.CredentialFingerprint,
		"the only handle a client keeps on this issuance once the password is gone")
	assert.True(t, credential.EnforcedAccess.ExclusiveGrantVerified,
		"issuance READ the principal's complete ACL grant at the broker and refused to return "+
			"a password while any binding sat outside the declared set, so this is an "+
			"observation rather than a hope")

	password := credential.Password
	require.NotEmpty(t, password,
		"THE ONE-TIME SECRET. Without it the response is useless and no subscriber can connect")
	require.GreaterOrEqual(t, len(password), 24,
		"a SCRAM password short enough to guess is worse than none")

	// 2. THE BUDGET, from the fast side.
	require.Less(t, elapsed, blnk.SubscriberCredentialIssuanceBudget,
		"issuance must complete well inside its five-second budget (requirement R-7); it took %s",
		elapsed)

	// 3. THE SECRET IS RETURNED ONCE, asserted on raw bytes so no key can hide it.
	datasource.On("ListEventSubscribers", mock.Anything, mock.Anything).
		Return(coremodel.SubscriberPage{Subscribers: []coremodel.EventSubscriber{*stored}}, nil).
		Maybe()

	for name, subsequent := range map[string]subscribersCall{
		"read": {method: http.MethodGet, path: "/subscribers/" + subscriberID},
		"list": {method: http.MethodGet, path: "/subscribers?limit=100"},
	} {
		call := subsequent

		t.Run("the password is absent from a subsequent "+name, func(t *testing.T) {
			later := subscribersServe(t, router, call)
			require.Equal(t, http.StatusOK, later.Code, safeResponseBody(later))

			// The corpus is checked WITHOUT handing testify the secret; see
			// subscribersAssertSecretAbsent for why that distinction is not pedantry.
			subscribersAssertSecretAbsent(t, later.Body.String(), password,
				"the body of a subsequent "+name+" (the password is returned ONCE and nothing "+
					"persisted it, so a hit here means a field was added that can read it back)")
			assert.NotContains(t, later.Body.String(), `"password"`,
				"and no read shape carries the key at all")
		})
	}

	// 4. THE SECRET IS NEVER LOGGED, and neither is the persisted reference.
	for index, entry := range logs.AllEntries() {
		subscribersAssertSecretAbsent(t, entry.Message, password,
			fmt.Sprintf("the message of log entry %d", index))

		for field, value := range entry.Data {
			subscribersAssertSecretAbsent(t, fmt.Sprintf("%v", value), password,
				fmt.Sprintf("log entry %d, field %q", index, field))
		}
	}

	// 5. ONLY A NON-REVERSIBLE REFERENCE AND AN ISSUANCE TIMESTAMP ARE PERSISTED. Read off
	// the arguments the credential record was actually written with, which is the closest a
	// test can stand to the column itself.
	recorded := false
	for _, call := range datasource.Calls {
		if call.Method != "RecordSubscriberCredentialIfUnchanged" {
			continue
		}

		recorded = true

		reference, isString := call.Arguments[3].(string)
		require.True(t, isString, "the reference is persisted as text")
		assert.NotEmpty(t, reference, "something must identify the issuance")
		assert.Falsef(t, reference == password,
			"THE REFERENCE IS THE PASSWORD. Storing the plaintext under another name is the exact "+
				"failure this posture exists to prevent. The value is not printed: it is the live "+
				"secret. It is %d bytes, as is the password.", len(reference))
		subscribersAssertSecretAbsent(t, reference, password,
			"the persisted credential reference")

		issuedAt, isTime := call.Arguments[4].(time.Time)
		require.True(t, isTime, "the issuance instant is persisted")
		assert.False(t, issuedAt.IsZero(),
			"an issuance with no recorded instant cannot be reconciled against the broker")

		for index, argument := range call.Arguments {
			subscribersAssertSecretAbsent(t, fmt.Sprintf("%v", argument), password,
				fmt.Sprintf("argument %d of the persisted credential record", index))
		}
	}
	require.True(t, recorded,
		"the issuance must record its credential reference, or the fingerprint the response "+
			"reports describes nothing")

	// 6. THE ERROR PATH DISCLOSES NOTHING EITHER. A second issuance whose record cannot be
	// written fails AFTER the password was generated, which is the one failure path a
	// plaintext could ride out on.
	t.Run("a failure after generation returns no secret", func(t *testing.T) {
		failing := new(mocks.MockDataSource)

		failingService, err := blnk.NewBlnk(failing)
		require.NoError(t, err)

		failingInstance := NewAPI(failingService)
		require.NotNil(t, failingInstance)
		failingInstance.router.Use(func(c *gin.Context) {
			c.Set("isMasterKey", true)
			c.Next()
		})

		secondID := uniqueSubscriberID()
		secondRow := subscribersFixtureRow(secondID)
		secondRow.CredentialReference = nil
		secondRow.CredentialIssuedAt = nil
		secondRow.AuthorizedTopics = coremodel.SubscriberGrantableTopics(fetched.Kafka.TopicPrefix)[:1]

		failing.On("ClaimSubscriberForProvisioning", mock.Anything, secondID, mock.Anything).
			Return("second-claim-token", nil)
		failing.On("GetEventSubscriberByID", mock.Anything, secondID).Return(secondRow, nil)
		failing.On("RecordSubscriberCredentialIfUnchanged",
			mock.Anything, secondID, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(fmt.Errorf("the registry could not record the issuance"))
		failing.On("ClearSubscriberCredential", mock.Anything, secondID, mock.Anything).
			Return(nil).Maybe()
		failing.On("RecordSubscriberCredentialCleanupPending",
			mock.Anything, secondID, mock.Anything, mock.Anything).Return(nil).Maybe()
		failing.On("RenewSubscriberProvisioningFence",
			mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		failing.On("ReleaseSubscriberProvisioningFence",
			mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		failing.On("MarkSubscriberRevocationPending",
			mock.Anything, secondID, mock.Anything, mock.Anything).Return(secondRow, nil).Maybe()
		failing.On("TakeEventSubscriber", mock.Anything, secondID, mock.Anything).
			Return(secondRow, nil).Maybe()

		failingRouter := failingInstance.Router()

		t.Cleanup(func() {
			// The failed issuance compensates by revoking what it wrote at the broker, but
			// deregistering as well is what guarantees nothing is left behind if the
			// compensation itself failed.
			subscribersServe(t, failingRouter, subscribersCall{
				method: http.MethodDelete, path: "/subscribers/" + secondID,
			})
		})

		failed := subscribersServe(t, failingRouter, subscribersCall{
			method: http.MethodPost, path: "/subscribers/" + secondID + "/kafka-credentials",
		})

		// SANITISED for the mirror-image reason: this message renders only when the status IS
		// 200, so the body is a successful credential response and the plaintext password is
		// certainly in it. The two NotContains assertions below are the real checks, and they
		// run only if this require passes.
		require.NotEqual(t, http.StatusOK, failed.Code,
			"an issuance the registry could not record is not a success: the caller would hold "+
				"a password Blnk has no record of. %s", safeResponseBody(failed))
		assert.NotContains(t, failed.Body.String(), `"password"`,
			"and NO SECRET may ride out on the error body")
		assert.NotContains(t, failed.Body.String(), blnk.RedactedSecretPlaceholder,
			"not even redacted: a failure body has no business carrying a credential field")
	})
}

// TestSubscribersAPI_NoPersistedShapeCanCarryAPlaintextSecret is the schema-level half of the
// posture, and it runs whether or not a broker is configured.
//
// The end-to-end test above proves that no plaintext WAS persisted on the path it exercised.
// This proves something stronger and cheaper: the stored shape has nowhere to put one. That
// is what makes "absent from every subsequent read" a structural guarantee rather than a
// property of the projection that happens to be written today — a new read shape assembled
// from a registry row cannot leak a secret that the row cannot hold.
//
// KafkaCredentialsResponse is the deliberate exception and is asserted as such: it is the one
// shape in the system that carries a plaintext, it is a RESPONSE rather than a stored row,
// and it exists for exactly one write of exactly one field.
func TestSubscribersAPI_NoPersistedShapeCanCarryAPlaintextSecret(t *testing.T) {
	secretish := []string{"password", "secret", "plaintext", "credential_value"}

	fieldNames := func(sample interface{}) []string {
		shape := reflect.TypeOf(sample)
		names := make([]string, 0, shape.NumField())
		for index := 0; index < shape.NumField(); index++ {
			field := shape.Field(index)
			name := strings.ToLower(field.Name)
			if tag := field.Tag.Get("json"); tag != "" {
				name += " " + strings.ToLower(strings.Split(tag, ",")[0])
			}
			names = append(names, name)
		}

		return names
	}

	t.Run("the stored registry row", func(t *testing.T) {
		for _, name := range fieldNames(coremodel.EventSubscriber{}) {
			for _, forbidden := range secretish {
				assert.NotContains(t, name, forbidden,
					"model.EventSubscriber is what the registry table holds, and %q could carry a "+
						"plaintext SASL password. Only a non-reversible reference and an issuance "+
						"instant may be stored", name)
			}
		}

		// The positive half: the two fields that MUST exist, or there is nothing to
		// reconcile an issuance against.
		shape := reflect.TypeOf(coremodel.EventSubscriber{})
		_, hasReference := shape.FieldByName("CredentialReference")
		_, hasIssuedAt := shape.FieldByName("CredentialIssuedAt")
		assert.True(t, hasReference, "the non-reversible reference is what is stored instead")
		assert.True(t, hasIssuedAt, "beside the instant it was issued")
	})

	t.Run("the subscriber read shape", func(t *testing.T) {
		for _, name := range fieldNames(model.SubscriberResponse{}) {
			for _, forbidden := range secretish {
				assert.NotContains(t, name, forbidden,
					"the read shape answers every registry read, and %q would put a secret on "+
						"a response that is fetched repeatedly and cached by clients", name)
			}
			assert.NotContains(t, name, "credential_reference",
				"the raw reference is reduced to a fingerprint in exactly one place")
			assert.NotContains(t, name, "webhook_url",
				"and the legacy third-party destination is readable only from the dedicated "+
					"route the retirement guard retires")
		}
	})

	t.Run("the issuance response is the one exception", func(t *testing.T) {
		shape := reflect.TypeOf(model.KafkaCredentialsResponse{})

		field, ok := shape.FieldByName("Password")
		require.True(t, ok,
			"the credential endpoint's response is the ONE place a plaintext appears; without "+
				"it the endpoint cannot do its job")
		assert.Equal(t, "password", strings.Split(field.Tag.Get("json"), ",")[0],
			"under the key a client reads it from")
		assert.Equal(t, reflect.String, field.Type.Kind(),
			"as a plain string handed straight to the response: a redacting wrapper here would "+
				"serialise the placeholder and the subscriber could never connect")
	})
}

// TestSubscribersAPI_NoFailureDiagnosticCanCarryTheIssuedSecret is the other half of that
// posture: the response shape may carry a plaintext exactly once, and nothing this suite WRITES
// may.
//
// # What was wrong
//
// Assertions about the credential endpoint passed the whole response body into their failure
// message. A failure message renders only when the assertion fails, which is the case where the
// body is not what the test assumed — and for the two mirror-image shapes on this endpoint that is
// precisely when the body holds a real password. `require.Equal(t, 200, code, "…body: %s")` renders
// when the status is NOT 200, so a handler that answered 500 while still marshalling the credential
// disclosed it; `require.NotEqual(t, 200, code, "…body: %s")` renders when the status IS 200, so the
// body is a successful credential response and disclosure is certain. Both had an assertion on the
// next line forbidding a secret in the body, and neither ever reached it: a failed require aborts.
//
// CI output is durable and broadly readable, so a secret written there is disclosed however
// carefully the endpoint behaves afterwards — which is the one-time-issuance posture undone by its
// own test suite.
//
// # Why the helper is asserted rather than the call sites
//
// Every diagnostic on this endpoint now renders through safeResponseBody, so the property worth
// pinning is the helper's: given a real credential response, the password does not appear in what
// it returns, and the fields that make a diagnostic worth having do. Asserting that once is
// stronger than grepping the call sites, because a new assertion added later inherits it.
func TestSubscribersAPI_NoFailureDiagnosticCanCarryTheIssuedSecret(t *testing.T) {
	const plaintext = "a-real-looking-sasl-password-8f3b1c"

	credentialResponse := func(t *testing.T) *httptest.ResponseRecorder {
		t.Helper()

		recorder := httptest.NewRecorder()
		recorder.Code = http.StatusOK
		payload, err := json.Marshal(model.KafkaCredentialsResponse{
			Brokers:          []string{"localhost:9092"},
			BrokerEndpoint:   "localhost:9092",
			AuthorizedTopics: []string{"blnk.transactions"},
			ConsumerGroupID:  "blnk-sub-group",
			Username:         "blnk-sub-principal",
			Password:         plaintext,
			Mechanism:        "SCRAM-SHA-512",
		})
		require.NoError(t, err)
		recorder.Body.Write(payload)

		return recorder
	}

	t.Run("the issued password never reaches a diagnostic", func(t *testing.T) {
		recorder := credentialResponse(t)

		rendered := safeResponseBody(recorder)

		assert.NotContains(t, rendered, plaintext,
			"the plaintext SASL password must not appear in a failure message: CI output is "+
				"durable, and a disclosed credential is disclosed whatever the endpoint does next")
		assert.Contains(t, rendered, `"password"`,
			"the KEY stays, so a reader can see that the body was credential-shaped rather than "+
				"wondering whether the field was missing")
		assert.Contains(t, rendered, "withheld",
			"and the value says it was withheld rather than looking empty, which would read as a "+
				"handler defect")
	})

	t.Run("everything a diagnostic is for survives", func(t *testing.T) {
		rendered := safeResponseBody(credentialResponse(t))

		assert.Contains(t, rendered, "status=200",
			"the status is the first thing a reader needs and it is not in the body at all")
		for _, kept := range []string{
			"blnk.transactions",  // the grant, which is what most assertions here are about
			"blnk-sub-group",     // the consumer group
			"blnk-sub-principal", // the derived principal
			"SCRAM-SHA-512",      // the mechanism
			"localhost:9092",     // where the subscriber was told to connect
		} {
			assert.Containsf(t, rendered, kept,
				"%q is not a secret and is exactly what makes the diagnostic useful; withholding "+
					"the whole body instead would trade one problem for another", kept)
		}
	})

	t.Run("a secret nested anywhere is withheld too", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.Code = http.StatusInternalServerError
		recorder.Body.WriteString(
			`{"items":[{"sasl_password":"` + plaintext + `","username":"u"}],` +
				`"meta":{"admin_secret":"` + plaintext + `","API_TOKEN":"` + plaintext + `"},` +
				`"credential_reference":"sha256:abc123"}`)

		rendered := safeResponseBody(recorder)

		assert.NotContains(t, rendered, plaintext,
			"a secret inside an array element or a nested object is the same disclosure; the scrub "+
				"walks the whole decoded value, and matches the key case-insensitively")
		assert.Contains(t, rendered, "sha256:abc123",
			"credential_reference is the NON-REVERSIBLE reference the registry stores precisely so "+
				"it can be shown, and withholding it would remove the field that identifies which "+
				"issuance a diagnostic is about")
	})

	t.Run("a body that is not JSON is withheld entirely", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.Code = http.StatusBadGateway
		recorder.Body.WriteString("upstream failure: password=" + plaintext)

		rendered := safeResponseBody(recorder)

		assert.NotContains(t, rendered, plaintext,
			"there is no structure to sanitise in a plain-text body, so it is withheld rather than "+
				"printed and hoped over")
		assert.Contains(t, rendered, "not JSON",
			"and the reason is stated, with a byte count, so the diagnostic is still actionable")
		assert.Contains(t, rendered, "status=502")
	})

	t.Run("an empty body is reported as empty", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.Code = http.StatusNoContent

		assert.Equal(t, "status=204 body=<empty>", safeResponseBody(recorder),
			"an empty body is a fact worth reporting plainly: a 204 with a body would be the "+
				"defect, and this is how a reader tells the two apart")
	})

	// AND THE CALL SITES ACTUALLY USE IT. A sanitiser nothing calls is decorative, and the
	// idiom it replaces — `"...body: %s", recorder.Body.String()` — is the one every other
	// assertion in this suite is written with, so it will be reached for again by anyone adding
	// a case here. The region asserted on is the end-to-end issuance test, which is the only
	// test in the file that holds a REAL password, plus the surface's shared error helper,
	// which is used on the credential route as well as on the read routes.
	t.Run("no diagnostic in the issuance test renders a raw body", func(t *testing.T) {
		source, err := os.ReadFile("subscribers_api_test.go")
		require.NoError(t, err, "this test reads its own file")

		const anchor = "func TestIssueKafkaCredentials_ReturnsTheConnectionDetailsAndTheSecretExactlyOnce"
		start := strings.Index(string(source), anchor)
		require.Positive(t, start, "the end-to-end issuance test must exist under its own name")
		end := strings.Index(string(source)[start:], "\n// TestSubscribersAPI_NoPersistedShape")
		require.Positive(t, end, "the region must be bounded by the test that follows it")
		region := string(source)[start : start+end]

		// A raw body used as an assertion SUBJECT is fine and necessary — several assertions
		// here check that a later read carries no password, which requires the untouched
		// bytes. What must not appear is a raw body as a MESSAGE ARGUMENT, because that is
		// what gets written out on failure. The region is whitespace-collapsed first so a
		// wrapped call is matched the same as a single-line one.
		collapsed := strings.Join(strings.Fields(region), " ")
		diagnostic := regexp.MustCompile(`%s"[^)]*\.Body\.String\(\)`)

		assert.Empty(t, diagnostic.FindAllString(collapsed, -1),
			"every diagnostic in the end-to-end issuance test must render through "+
				"safeResponseBody: this is the one test that holds a real SASL password, and a "+
				"raw body in a failure message writes it to durable CI output")
		assert.Contains(t, region, "safeResponseBody(",
			"and it must still print something, or a failure becomes unfixable")

		helper := string(source)[strings.Index(string(source), "func assertSubscribersErrorCode("):]
		if closing := strings.Index(helper, "\n}\n"); closing > 0 {
			helper = helper[:closing]
		}
		assert.Empty(t, diagnostic.FindAllString(strings.Join(strings.Fields(helper), " "), -1),
			"assertSubscribersErrorCode is used on the credential route too, where its message "+
				"renders because the request SUCCEEDED — so the body it would print is a "+
				"credential response")
	})
}

// TestSubscribersAPI_TypedCodesResolveToIntendedStatuses pins the status catalogue this
// surface answers from.
//
// statusByCode in internal/apierror/codes.go is the single source of truth for every code's
// default status, and StatusForCode defaults an UNKNOWN code to 500. A code declared but
// never mapped therefore turns every refusal that carries it into a server error: a missing
// subscriber would read as a Blnk defect, and a retryable dependency failure would read as
// one too. Nothing else in the suite fails when that happens, because the code in the body
// still looks correct.
//
// The last case is why every other assertion in this file is written against
// error_detail.code rather than against a status.
func TestSubscribersAPI_TypedCodesResolveToIntendedStatuses(t *testing.T) {
	for code, status := range map[apierror.ErrorCode]int{
		apierror.ErrSubscriberNotFound:           http.StatusNotFound,
		apierror.ErrSubscriberProvisioningFailed: http.StatusServiceUnavailable,
		apierror.ErrKafkaUnavailable:             http.StatusServiceUnavailable,
		apierror.ErrAuthMasterKeyRequired:        http.StatusForbidden,
		// The rest of the codes this surface can answer, mapped for the same reason.
		apierror.ErrSubscriberProvisioningTimeout:  http.StatusGatewayTimeout,
		apierror.ErrSubscriberInsecureTransport:    http.StatusForbidden,
		apierror.ErrSubscriberBrokersNotConfigured: http.StatusServiceUnavailable,
		apierror.ErrSubscriberGrantEmpty:           http.StatusConflict,
		apierror.ErrGenGone:                        http.StatusGone,
		apierror.ErrGenMissingParameter:            http.StatusBadRequest,
		apierror.ErrGenValidation:                  http.StatusBadRequest,
		apierror.ErrGenMalformedRequest:            http.StatusBadRequest,
	} {
		expected := status

		t.Run(string(code), func(t *testing.T) {
			resolved := apierror.StatusForCode(code)

			assert.Equal(t, expected, resolved,
				"%s must be mapped in statusByCode; an unmapped code silently becomes 500", code)
			assert.NotEqual(t, http.StatusInternalServerError, resolved,
				"a 500 here means the code was never mapped, and every refusal carrying it is "+
					"reported as a Blnk defect")
		})
	}

	t.Run("two codes on this surface share a status", func(t *testing.T) {
		// This is not trivia: it is the reason every assertion in this file names a code.
		assert.Equal(t,
			apierror.StatusForCode(apierror.ErrAuthUnknownResource),
			apierror.StatusForCode(apierror.ErrAuthMasterKeyRequired),
			"AUTH_UNKNOWN_RESOURCE and AUTH_MASTER_KEY_REQUIRED both answer 403, so a "+
				"status-only assertion cannot tell \"the gate refused me\" from \"the route "+
				"prefix is missing from pathToResource and the surface does not exist\"")
	})
}

// ---------------------------------------------------------------------------------------
// THERE IS NO DATA-PLANE ROUTE UNDER /subscribers, and this section is what enforces that
//
// # What was here, and why it is gone rather than fixed
//
// GET /subscribers/:subscriber_id/events served a key-scoped subscriber its own records: Blnk read
// the shared category topic with its OWN administrative credential, applied the subscriber's
// partition-key prefix per record, and returned a page. Three properties of that design are why it
// was removed outright rather than hardened.
//
//  1. It was a SECOND DATA PLANE holding a credential far wider than any subscriber's. Every record
//     on the topic passed through the filter, so a defect in the filter — or a diagnostic field
//     added to the page in good faith — disclosed other ledgers' records.
//  2. It authenticated with a BESPOKE HEADER, X-Blnk-Subscriber-Secret, which the platform's
//     authorization middleware knows nothing about. Two authentication systems on one router is how
//     one of them comes to be forgotten.
//  3. REVOCATION DID NOT REVOKE. Removing a subscriber's SCRAM credential at the broker closed the
//     direct path and left the served one open, because the served one authenticated against a
//     registry column rather than against the broker.
//
// AAP requirement R-7 authorises exactly one subscriber endpoint — POST
// /subscribers/{id}/kafka-credentials — and §0.5.2 enumerates the four new routes this feature adds.
// A record-serving route is not among them.
//
// # What replaces it
//
// A key-scoped subscriber's records are delivered by the key-authorising component the DEPLOYMENT
// declares in front of the brokers (KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway, addressed by
// KAFKA_KEY_SCOPE_GATEWAY_BROKERS). Blnk does not ship it. Where none is declared, issuance refuses
// the row — see TestIssueKafkaCredentials_RefusesAKeyScopedSubscriberWithNoDeclaredGateway — so no
// credential ever exists that claims an enforcement point nothing is running.
// ---------------------------------------------------------------------------------------

// TestSubscribersAPI_HasNoRecordServingRoute is the guard that keeps the removal removed.
//
// # Why an absence needs a test
//
// A deleted route leaves no compile error behind. Re-adding one is a single line in api.go, and the
// reasoning against it lives in prose that a future change need not read. So the absence is asserted
// the only way an absence can be: by making the requests such a route would answer and requiring
// the ROUTER to refuse them before any handler exists to serve them.
//
// # Why it asserts 404 specifically, and what would break it
//
// gin answers an unregistered path with 404 from the router itself, ahead of the whole middleware
// chain. That is the fingerprint of "no such route", and it is distinguishable from every answer a
// registered route could give: 403 would mean the path resolved and authorization judged it, 401
// that something authenticated it, 405 that the path exists under another method, 500 that a handler
// ran. Any of those means a record-serving route is back.
//
// The master key is presented on purpose. A 404 while holding the most privileged credential the
// deployment has is the strongest available statement that nothing is registered there — a refusal
// that depended on the caller's scope would leave the route reachable by someone.
func TestSubscribersAPI_HasNoRecordServingRoute(t *testing.T) {
	router, _ := setupSubscribersRouter(t, subscribersHarness{secure: true, masterKey: true})

	subscriberID := uniqueSubscriberID()
	topic := coremodel.SubscriberGrantableTopics(coremodel.DefaultEventTopicPrefix)[0]

	// Every shape the removed route accepted, plus the two neighbouring paths a re-introduction
	// would most plausibly choose.
	for name, call := range map[string]subscribersCall{
		"the removed stream route": {
			method: http.MethodGet,
			path: fmt.Sprintf(
				"/subscribers/%s/events?topic=%s&partition=0&offset=0&limit=100", subscriberID, topic,
			),
			key: subscribersAPITestMasterKey,
		},
		"the removed stream route with no query": {
			method: http.MethodGet,
			path:   "/subscribers/" + subscriberID + "/events",
			key:    subscribersAPITestMasterKey,
		},
		"a records path": {
			method: http.MethodGet,
			path:   "/subscribers/" + subscriberID + "/records",
			key:    subscribersAPITestMasterKey,
		},
		"a stream path": {
			method: http.MethodGet,
			path:   "/subscribers/" + subscriberID + "/stream",
			key:    subscribersAPITestMasterKey,
		},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := subscribersServe(t, router, call)

			assert.Equal(t, http.StatusNotFound, recorder.Code,
				"%s %s must not be registered. Blnk serves no subscriber records: a route here is a "+
					"second data plane holding Blnk's own wide credential, authenticated outside the "+
					"authorization middleware, and unaffected by revoking the subscriber's SCRAM "+
					"credential at the broker. A key scope is delivered by the component the deployment "+
					"declares in KAFKA_KEY_SCOPE_ENFORCEMENT. body: %s",
				call.method, call.path, recorder.Body.String())
		})
	}
}

// TestSubscribersAPI_DoesNotReadASubscriberSecretHeader pins the second half of the removal: not
// merely that the route is gone, but that no surviving route accepts the header it used.
//
// X-Blnk-Subscriber-Secret was a live SASL password presented on every poll, authenticated against
// a registry column by code that sat outside the authorization middleware. If any subscriber route
// still honoured it, the removal would have moved the second authentication system rather than
// deleted it — so the credential endpoint is exercised with the header and WITHOUT a master key, and
// must refuse on the master key exactly as it does when no header is present at all.
func TestSubscribersAPI_DoesNotReadASubscriberSecretHeader(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

	subscriberID := uniqueSubscriberID()

	// A key scoped to the subscriber resource, so the request is authenticated and authorised as
	// far as the master-key gate. Whatever refuses it below refuses it there and not earlier.
	//
	// Programmed through the shared helper because authentication ALSO starts a background
	// last-used update for every non-master credential it admits; the helper is what makes that
	// call expected and joinable rather than an unexpected-call panic on another goroutine.
	key := subscribersTestAPIKey(
		middleware.BuildScope(middleware.ResourceSubscribers, middleware.ActionAll))
	touched := expectSubscribersAPIKeyLookup(datasource, key)

	request := httptest.NewRequest(
		http.MethodPost, "/subscribers/"+subscriberID+"/kafka-credentials", nil,
	)
	request.Header.Set(middleware.KeyHeader, key.Key)
	request.Header.Set("X-Blnk-Subscriber-Secret", "the-secret-this-subscriber-was-issued")
	request.Header.Set("X-Blnk-Subscriber-Principal", "blnk-sub-"+subscriberID)
	// In-process TLS, so the request cannot be refused for its transport before reaching the
	// master-key gate this test is about.
	request.TLS = &tls.ConnectionState{}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusForbidden, recorder.Code,
		"a subscriber secret must buy nothing: this route is master-key only. body: %s",
		recorder.Body.String())

	var body struct {
		ErrorDetail apierror.APIError `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))

	assert.Equal(t, apierror.ErrAuthMasterKeyRequired, body.ErrorDetail.Code,
		"the refusal must be the MASTER KEY gate, which is what says the subscriber headers were "+
			"never consulted. A different code here would mean something read them")

	// Joined before the call log is read, so the background update cannot still be appending to it.
	requireSubscribersLastUsedTouch(t, touched)

	// AND NOTHING REACHED THE REGISTRY, so no header-driven lookup happened on the way to the
	// refusal.
	datasource.AssertNotCalled(t, "GetEventSubscriberByID", mock.Anything, mock.Anything)
	datasource.AssertNotCalled(t, "ClaimSubscriberForProvisioning",
		mock.Anything, mock.Anything, mock.Anything)
}

// startKeyScopeGatewayStub starts a stub for the key-authorising component a deployment declares
// in front of its brokers, and returns its control endpoint and bearer token.
//
// # Why the HTTP layer needs one at all
//
// Issuance for a key-scoped subscriber now BINDS the recorded prefix at that component and
// requires it to attest the binding back before a secret exists (SEC-01). A declaration Blnk
// cannot verify is treated as no declaration, so without a reachable endpoint every key-scoped
// request through this router answers SUBSCRIBER_KEY_SCOPE_UNATTESTED — and the tests whose
// subject is the request contract, the five-second ceiling or the broker step would all be
// asserting that one refusal instead.
//
// It is deliberately MINIMAL. The conformance cases — a component that trims the prefix, answers
// for the wrong principal, enforces nothing, or rejects Blnk's token — belong to the client and
// the service, and are covered in the root package's event_keyscope_gateway_test.go against a
// double that implements the contract in full. What this stub has to be is CORRECT, so that a
// request under test is not refused by its stand-in.
//
// Parameters:
//   - t *testing.T: for the helper marker and to close the server when the test ends.
//
// Returns:
//   - string: the control endpoint to declare in KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL.
//   - string: the bearer token to declare alongside it.
func startKeyScopeGatewayStub(t *testing.T) (endpoint string, token string) {
	t.Helper()

	const stubToken = "api-harness-gateway-token"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// AUTHENTICATED, like the real contract: a stub that answered unauthenticated requests
		// would let a build that forgot to present the token pass every test here.
		if r.Header.Get("Authorization") != "Bearer "+stubToken {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		answer := map[string]any{"key_scope_enforced": true, "detail": "api harness stub"}

		if r.Method == http.MethodPost {
			// Answered FROM THE REQUEST, which is the one liberty this stub takes over the root
			// package's double: it holds no state, so it echoes the binding it was given. That is
			// enough to attest, and the mismatch cases that require stored state are asserted
			// where the state is.
			var binding struct {
				Principal          string `json:"principal"`
				PartitionKeyPrefix string `json:"partition_key_prefix"`
			}
			if err := json.NewDecoder(r.Body).Decode(&binding); err != nil {
				w.WriteHeader(http.StatusBadRequest)

				return
			}

			answer["principal"] = binding.Principal
			answer["partition_key_prefix"] = binding.PartitionKeyPrefix
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(server.Close)

	return server.URL, stubToken
}
