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

// subscribers_api_test.go covers the request-side contract of the subscriber registry
// surface, and specifically the four decisions this layer owns outright because no
// layer beneath it can make them:
//
// The five-second CEILING on credential issuance, which is a property of the request
// rather than of the service call inside it.
//
// The service's own behaviour — provisioning, ACL reconciliation, the registry writes —
// is asserted in the root package against doubles and against a real broker, and is
// deliberately not restated here.
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

// TestSubscriberRoutes_RequireTheMasterKey pins the gate on every route in the surface.
//
// The credential route mints a SASL secret and the read routes disclose the broker-side
// access model of every subscriber, so a single unguarded route is the whole surface.
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

// TestSubscriberRoute_ValidatesTheIdentifierBeforeLookingItUp is the identifier-first refusal at this surface.
//
// The route parameter was trimmed and passed straight to the lookup.
//
// It also spent a database round trip on a value refusable in memory, and on the
// credential route a slice of the five-second budget.
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

// TestSubscriberRoute_StillAnswersMissingForABlankIdentifier keeps the two refusals
// distinct.
func TestSubscriberRoute_StillAnswersMissingForABlankIdentifier(t *testing.T) {
	recorder := subscribersRequest(t, http.MethodGet, "/subscribers/%20%20")

	assertSubscribersErrorCode(t, recorder, http.StatusBadRequest, "GEN_MISSING_PARAMETER")
}

// TestSubscriberRoute_AcceptsAGeneratedIdentifier is the negative control, and it is
// what stops the validation above being too strict.
func TestSubscriberRoute_AcceptsAGeneratedIdentifier(t *testing.T) {
	recorder := subscribersRequest(t, http.MethodGet, "/subscribers/"+coremodel.GenerateSubscriberID())

	assertSubscribersErrorCode(t, recorder, http.StatusNotFound, "SUBSCRIBER_NOT_FOUND")
}

// subscribersSlowWorkDrainBudget bounds how long a subtest waits for an ABANDONED
// issuance goroutine to exit after it has been released.
//
// The wait itself is the point — see parkedIssuance — and this is only its ceiling.
const subscribersSlowWorkDrainBudget = 5 * time.Second

// parkedIssuance builds an issuance fake that BLOCKS until this subtest releases it,
// and guarantees the released goroutine is gone before the subtest ends.
//
// issueWithinBudget abandons work that overruns its budget: it returns while the
// goroutine is still running.
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

// TestIssueWithinBudget_AnswersOnTimeWhenTheIssuanceOverrunsIt is the budget answer, and it is the
// only place the ceiling can be asserted as a property of the request.
//
// The contract requires issuance to complete within five seconds, and the handler
// bounds the service context to exactly that — which is not the same thing.
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

// TestIssueWithinBudget_ReturnsTheIssuanceWhenItFinishesInTime is the other half: the
// ceiling must not become a truncation.
//
// A helper that abandoned unconditionally, or that raced its own goroutine, would make
// the endpoint useless while passing the test above.
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
		// A cancelled request context lands in the same arm as an elapsed budget, which is
		// correct: there is nobody to answer, so the work is abandoned to its own cleanup
		// rather than held open.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// The slow work parks until this subtest releases it, and its exit is waited for.
		issue, started, hasFinished := parkedIssuance(t)

		_, _, completed := issueWithinBudget(ctx, "sub_9f1c8a72", issue)

		assert.False(t, completed,
			"a caller that has already gone away must not be waited for")

		// The premise: the work really did start. Without this the subtest would pass against
		// an implementation that never invoked the work at all, which is a different
		// behaviour with the same observable result at the call site.
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

// TestSubscriberCredentialIssuanceBudget_IsTheFiveSecondsTheRequirementNames pins the
// production value.
func TestSubscriberCredentialIssuanceBudget_IsTheFiveSecondsTheRequirementNames(t *testing.T) {
	assert.Equal(t, 5*time.Second, blnk.SubscriberCredentialIssuanceBudget,
		"the credential endpoint's ceiling is a stated requirement, not a tuning parameter")
}

// TestListSubscribers_RefusesAnUnsupportedQueryParameter is the closed parameter set.
//
// The parameter set was open, so "?limitt=5", "?status=active" and
// "?topic=blnk.balances" were all accepted in silence and answered with the whole first
// page.
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
// `offset` IS NOT ONE OF THEM, and its absence is asserted below rather than left
// implicit.
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

// TestListSubscribers_HonoursIncludeCountWithTheEstablishedEnvelope is the established list envelope.
//
// include_count was REFUSED with a validation error, because no layer could count the
// registry.
//
// The count is a real query rather than the length of the page.
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

// TestManagementBodies_RefuseAFieldTheShapeDoesNotDeclare is the guard.
//
// gin's ShouldBindJSON discards unknown keys, so a misspelling was accepted and
// dropped. The consequence differs per route, and on two of them it is worse than a
// no-op:
//
//   - POST /subscribers with "authorised_topics" — the British spelling, or any typo —
//     registered a subscriber authorised for NOTHING and answered 201.
//   - PUT /subscribers/{id} with every field misspelled decoded to a wholly EMPTY
//     update, which is a legitimate instruction meaning "rewrite the row with its own
//     values" — so the answer was 200 with the unchanged row, and a caller diffing the
//     response against what they sent could not tell an ignored field from a value the
//     server kept.
//
// A rejected filter and a rejected body field are the same class of problem, and this
// API already refuses an unknown QUERY parameter. The body was the remaining half.
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

// TestManagementBodies_StillAcceptWhatTheyDeclare is the other half of the strictness rule:
// must not have narrowed the accepted vocabulary.
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
		// binding:"max=16,dive,max=249" is what stops a caller submitting a thousand topic
		// names or one longer than Kafka accepts, and it only runs because the validator is
		// invoked explicitly.
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

// TestListSubscribers_AnswersOneShapeForEveryReading is the guard.
//
// That is a breaking difference, not a cosmetic one: a client written against the page
// reading fails on the resolver with a type error rather than a message.
//
// And it fails where it costs most.
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
		// A token no subscriber can hash to. An empty result is a SUCCESSFUL answer here —
		// the registry was searched to its end — so it must be an empty data array, not a 404
		// and not a bare [].
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
		// Carried rather than refused so a client building the query from a boolean variable
		// does not have to special-case one of its values — and it must therefore behave
		// exactly as the parameter's absence does.
		body := decode(t, "/subscribers?revocation_pending=false&limit=1")
		assert.Nil(t, body.TotalCount, "nobody asked for a total on the ordinary page")
	})
}

// TestListSubscribers_RefusesPagingOptionsOnACompleteReading is the other half of the complete-reading rule.
//
// The pseudonym resolution and the revocation scan are not walks: the resolver returns
// at most one row, and the scan deliberately covers the WHOLE registry because a
// partial list of live unaccounted-for credentials reads exactly like a complete one —
// acting on it would leave the rest authenticating while the incident looked closed,
// which is why the service refuses rather than truncating when the registry exceeds its
// bound.
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

// TestListSubscribers_ParsesIncludeCountStrictly is the other half of the closed parameter set.
//
// ParseQueryOptions reads include_count as `value == "true"`, so "TRUE", "True" and "1"
// all meant false and the caller received a bare array with nothing to indicate its
// spelling had been ignored.
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
		// The empty and whitespace-only values are here rather than among the refusals: not
		// asking for a count is the ordinary request, and "?include_count=" is what an HTTP
		// client serialising an unset option produces.
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

// TestCreateSubscriber_AnswersAValidationErrorForAMissingName is the missing-name validation error.
//
// name carried binding:"required", so the BINDER refused a body that omitted it and the
// handler answered GEN_MALFORMED_REQUEST — "this body could not be read".
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

// TestSubscriberCRUD_CarriesNoLegacyWebhookState is the no-legacy-state rule at the HTTP boundary.
//
// The four `/subscribers/:id/webhook-subscription` routes are each fronted by
// middleware.WebhookSunsetGuard and answer 410 Gone after the retirement instant. The
// GENERAL subscriber routes are not deprecated and are not guarded — and they accepted
// `webhook_url` on create and update, and echoed it on every read.
func TestSubscriberCRUD_CarriesNoLegacyWebhookState(t *testing.T) {
	t.Run("the registration body has no webhook_url to accept", func(t *testing.T) {
		// The struct's JSON shape is the contract a client codes against. Asserted here as
		// well as in the DTO's own contract test because this is the layer where the bypass
		// was reachable.
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
			// The deployment state does not affect what this asserts — the legacy endpoint is a
			// row fact — so the fail-closed zero value is passed.
		}, coremodel.SubscriberAccessDeployment{}))
		require.NoError(t, err)

		assert.NotContains(t, string(body), "webhook_url",
			"the general read is unguarded, so disclosing the endpoint through it would keep it "+
				"readable after the guarded route had begun answering 410")
		assert.NotContains(t, string(body), endpoint,
			"and not the value under any other key either")

		// migrated_at IS reported, deliberately: it is migration progress about this
		// deployment rather than legacy state, and a progress report needs it on both sides
		// of the sunset.
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

// subscribersRouterUnderDeclaredKeyScope is subscribersRouter over a deployment that
// DECLARES a key-authorising component, with the same real datasource.
func subscribersRouterUnderDeclaredKeyScope(t *testing.T) *gin.Engine {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	router := eventsRouter(t, true)

	cnf, err := config.Fetch()
	require.NoError(t, err)

	endpoint, token := startKeyScopeGatewayStub(t)

	updated := *cnf
	// REQUIRED ONCE BROKERS ARE SET. config.MockConfig refuses a Kafka deployment with no usable
	// dual-delivery window, because the runtime reads an absent sunset date as ALREADY past.
	updated.WebhookDeprecationSunsetDate = time.Now().Add(20 * 24 * time.Hour).UTC().Format(time.RFC3339)
	updated.Kafka = config.KafkaConfig{
		Brokers:                         []string{"127.0.0.1:9092"},
		SubscriberBrokers:               []string{"127.0.0.1:9092"},
		TopicPrefix:                     coremodel.DefaultEventTopicPrefix,
		MinPartitions:                   blnk.MinTopicPartitions,
		ReplicationFactor:               1,
		InsecureLocalDev:                true,
		KeyScopeEnforcement:             config.KeyScopeEnforcementBrokerGateway,
		KeyScopeGatewayBrokers:          []string{"keyscope-gateway.invalid:9095"},
		KeyScopeGatewayAttestationURL:   endpoint,
		KeyScopeGatewayAttestationToken: token,
	}
	config.MockConfig(&updated)

	fetched, err := config.Fetch()
	require.NoError(t, err)
	_, active := fetched.Kafka.KeyScopeGateway()
	require.True(t, active,
		"the harness must declare an ACTIVE enforcement point, or it is asserting the shipped "+
			"default a second time")

	return router
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

	// IN-PROCESS TLS, because the credential endpoint refuses to put a one-time secret on
	// a channel it cannot establish as confidential, and this is the channel a production
	// deployment has. A loopback peer was presented here once; it establishes only that
	// the LAST hop stayed on the host, so relying on it made this harness model a posture
	// that a same-host reverse proxy silently invalidates.
	request.TLS = &tls.ConnectionState{}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, request)

	return w
}

// uniqueSubscriberID returns a canonical identifier no other test run uses.
//
// The registry is a shared real table and the identifier is unique-indexed, so a fixed
// value would make this file fail on its second run rather than on a defect. model's
// own generator is used so the value is canonical by construction — the principal and
// the consumer group are derived from it, and a non-canonical id is refused at
// registration.
func uniqueSubscriberID() string {
	return coremodel.GenerateSubscriberID()
}

// deleteSubscriber removes a subscriber through the API, for cleanup.
//
// Cleanup failures are REPORTED rather than ignored: a leaked row is a real registry
// row that the next run's list assertions have to tolerate, and a silent leak is how a
// suite becomes order-dependent.
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
// catalogue cannot leave this file granting a topic that is no longer grantable — which
// would fail as a validation error and read as a routing problem.
func grantableTopic(t *testing.T) string {
	t.Helper()

	topics := coremodel.SubscriberGrantableTopics("blnk")
	require.NotEmpty(t, topics, "the catalogue must expose at least one grantable topic")

	return topics[0]
}

// TestSubscribersAPI_RoutesAreReachableAndMasterKeyGated is the authorization matrix
// for the endpoints' own master-key gate: every one of the six routes must refuse a
// non-master caller and admit the master key.
//
// THE RESOURCE MAP IS NOT PROVEN HERE.
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
// Each step reads the state back through the API rather than through the service,
// because what was unverified is the HTTP layer: the binding, the derived identity in
// the response, the status codes, and the JSON field names a client is written against.
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

// TestSubscribersAPI_RecordsAndReturnsTheKeyScope is the contract at the HTTP boundary,
// It is TWO contracts because the same row means two different things
// in two deployments.
func TestSubscribersAPI_RecordsAndReturnsTheKeyScope(t *testing.T) {
	// THE SHIPPED DEFAULT: brokers configured, no key-authorising component declared. The
	// registration must be accepted and every enforcement claim must be withheld.
	t.Run("with nothing declared the scope is recorded, not enforced", func(t *testing.T) {
		// DELIBERATELY NO key-scope gateway: eventsRouter installs no Kafka block, which is the
		// shipped default resolved the way SubscriberAccessDeployment resolves it.
		router := subscribersRouter(t, true)

		subscriberID := uniqueSubscriberID()
		topic := grantableTopic(t)
		keyScope := "ldg_" + subscriberID

		body := fmt.Sprintf(
			`{"subscriber_id":%q,"name":"key scoped consumer","authorized_topics":[%q],"partition_key_prefix":%q}`,
			subscriberID, topic, keyScope,
		)

		w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
		require.Equal(t, http.StatusCreated, w.Code,
			"a key scope must be ACCEPTED at registration whatever the deployment declares: "+
				"recording an intent is how an operator states one before realising it. body: %s",
			w.Body.String())

		var created map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

		assert.Equal(t, keyScope, created["partition_key_prefix"],
			"the recorded scope must be reported, or an operator cannot see what was stored")

		enforced, ok := created["enforced_access"].(map[string]interface{})
		require.True(t, ok, "body: %s", w.Body.String())
		assert.Equal(t, keyScope, enforced["partition_key_prefix"],
			"and the scope must appear in enforced_access, which is where a consumer reads it")

		assert.Equal(t, string(coremodel.SubscriberKeyScopeStateRequested),
			enforced["partition_key_scope_state"],
			"REQUESTED: an intent recorded on a deployment with nothing able to keep it. This is "+
				"the state the whole correction exists to name")
		assert.Equal(t, false, enforced["partition_key_prefix_enforced"],
			"NOT enforced, because nothing enforces it. A true here was a false isolation "+
				"guarantee published on every read of the resource")
		assert.Equal(t, string(coremodel.KeyScopeEnforcementNone),
			enforced["partition_key_prefix_enforced_by"],
			"and no component may be named: a place name for something never deployed sends an "+
				"operator looking for a host instead of at their configuration")
		assert.Equal(t, false, enforced["gateway_delivery_required"],
			"nor may a client be instructed to dial an endpoint this deployment does not have")
		assert.Equal(t, false, enforced["broker_record_access"],
			"and the prefix still withholds direct broker reads, so both transport fields are "+
				"false — which is the honest description of a subscriber with no usable path yet")

		dimensions, ok := enforced["not_enforced_by"].([]interface{})
		require.True(t, ok)
		assert.Contains(t, dimensions, "partition_key",
			"the key dimension belongs in the UNENFORCED list here; asserting that list is always "+
				"empty is how the projection came to claim a boundary it did not have")
		assert.NotContains(t, enforced["enforced_by"], "partition_key")

		// AND THE PREDICTION MATCHES THE REFUSAL. The very next call would be refused with
		// SUBSCRIBER_KEY_SCOPE_UNENFORCED — see
		// TestIssueKafkaCredentials_RefusesAKeyScopedSubscriberWithNoDeclaredGateway — so the
		// row has to say so, with the remedy, rather than reporting itself provisionable.
		require.Equal(t, true, created["credential_issuance_blocked"],
			"the registry must predict the refusal instead of contradicting it. body: %s",
			w.Body.String())
		reason, ok := created["credential_issuance_blocked_reason"].(string)
		require.True(t, ok, "body: %s", w.Body.String())
		assert.Contains(t, reason, "KAFKA_KEY_SCOPE_ENFORCEMENT",
			"and it must name the variable that unblocks it")

		// AND ON EVERY LATER READ, not only on the registration that produced it. The
		// credential is delivered once, so an operator auditing tenancy months later reads
		// the row rather than the registration response — and a projection that told the
		// truth at create time and reverted to the claim on GET would be the same defect with
		// a longer fuse.
		read := subscriberRequest(t, router, http.MethodGet, "/subscribers/"+subscriberID, "")
		require.Equal(t, http.StatusOK, read.Code, "body: %s", read.Body.String())

		var fetched map[string]interface{}
		require.NoError(t, json.Unmarshal(read.Body.Bytes(), &fetched))

		readEnforced, ok := fetched["enforced_access"].(map[string]interface{})
		require.True(t, ok, "body: %s", read.Body.String())
		assert.Equal(t, string(coremodel.SubscriberKeyScopeStateRequested),
			readEnforced["partition_key_scope_state"])
		assert.Equal(t, false, readEnforced["partition_key_prefix_enforced"])
		assert.Equal(t, string(coremodel.KeyScopeEnforcementNone),
			readEnforced["partition_key_prefix_enforced_by"])
		assert.Equal(t, true, fetched["credential_issuance_blocked"])

		deleteSubscriber(t, router, subscriberID)
	})

	// AND WITH A COMPONENT DECLARED: the same registration, and now every claim above reverses.
	// This is the deployment the enforced shape belongs to.
	t.Run("with a component declared the scope is enforced and provisionable", func(t *testing.T) {
		router := subscribersRouterUnderDeclaredKeyScope(t)

		subscriberID := uniqueSubscriberID()
		topic := grantableTopic(t)
		keyScope := "ldg_" + subscriberID

		body := fmt.Sprintf(
			`{"subscriber_id":%q,"name":"key scoped consumer","authorized_topics":[%q],"partition_key_prefix":%q}`,
			subscriberID, topic, keyScope,
		)

		w := subscriberRequest(t, router, http.MethodPost, "/subscribers", body)
		require.Equal(t, http.StatusCreated, w.Code, "body: %s", w.Body.String())

		var created map[string]interface{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &created))

		enforced, ok := created["enforced_access"].(map[string]interface{})
		require.True(t, ok, "body: %s", w.Body.String())
		assert.Equal(t, keyScope, enforced["partition_key_prefix"])

		assert.Equal(t, string(coremodel.SubscriberKeyScopeStateAvailable),
			enforced["partition_key_scope_state"],
			"AVAILABLE and not attested: a registry read makes no round trip to the component, so "+
				"nothing has confirmed it is keeping THIS binding. Only a credential response can "+
				"report attested")
		assert.Equal(t, true, enforced["partition_key_prefix_enforced"],
			"BESIDE the statement that it IS kept — the two cannot be read apart, and this may be "+
				"true here because something is declared to keep it")
		assert.Equal(t, string(coremodel.KeyScopeEnforcementGateway),
			enforced["partition_key_prefix_enforced_by"],
			"and the component is named, because 'enforced' without one is unverifiable")

		// THE TRANSPORT INSTRUCTION, which is the field a broken integration turns on: a client
		// that read false here would point a consumer at a broker that refuses its fetches.
		assert.Equal(t, true, enforced["gateway_delivery_required"])
		assert.Equal(t, false, enforced["broker_record_access"],
			"and its complement states why: no topic Read binding exists for a key-scoped principal")

		dimensions, ok := enforced["enforced_by"].([]interface{})
		require.True(t, ok)
		assert.Contains(t, dimensions, "partition_key")
		assert.Empty(t, enforced["not_enforced_by"],
			"no dimension of this subscriber's access is enforced by nobody in THIS deployment")

		assert.Equal(t, false, created["credential_issuance_blocked"],
			"and issuance is genuinely unblocked here, which is why the claim is safe to make")

		deleteSubscriber(t, router, subscriberID)
	})
}

// TestIssueKafkaCredentials_RefusesAKeyScopedSubscriberWithNoDeclaredGateway is the
// contract at the HTTP boundary as this build actually ships it: on the DEFAULT
// configuration a key-scoped row is refused a credential, with the typed conflict and
// both remedies.
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

	// REQUIRED, both of them: the refusal is a judgement about the ROW, so the request
	// must reach the provisioning fence and then the registry before it can be made. An
	// implementation that refused every request carrying no gateway declaration — without
	// reading the row — would also refuse prefix-less subscribers, and AssertExpectations
	// below is what separates the two.
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
	assert.Contains(t, body, "KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway",
		"the remedy that KEEPS the recorded boundary travels in the body too: it was absent, so the "+
			"only supported route to the capability was invisible at the moment somebody wanted it")
	assert.Contains(t, body, "clear the partition key prefix",
		"the remedy that abandons it: a refusal naming none is a dead end")
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

// TestIssueKafkaCredentials_ProceedsForAKeyScopedSubscriberUnderADeclaredGateway is the
// other side of the same decision, and it is what keeps the refusal above from being
// read as "key scopes are unusable".
//
// The pair is what makes either test conclusive.
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

	// AND PROVISIONING WAS ATTEMPTED AT THE BROKER, which is what proves the row was
	// accepted rather than skipped. It fails because the harness holds no admin SASL
	// credential, and a 503 is the honest answer: nothing was created, and a retry against
	// a reachable, authenticated cluster is what would succeed.
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

// TestSubscribersAPI_RefusesAnUngrantableTopic covers the grant validation at the
// boundary.
//
// Dead-letter topics and the internal system category are Blnk's own; granting one to a
// subscriber would hand it Blnk's failure stream.
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

// TestSubscribersAPI_RefusesAMalformedRegistration covers the binder and the DTO
// validation.
//
// Name is the one field the service cannot invent, and the topic-count and length caps
// live in the BINDING TAGS so they bound allocation before any handler code runs.
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
// An identifier that matches nothing must be a clean 404.
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

// TestSubscribersAPI_CredentialIssuanceRefusesWithoutABroker covers the credential
// endpoint's HTTP contract in the configuration the ordinary suite runs in.
//
// No broker is configured here, so the endpoint must refuse with the typed
// EVENT_KAFKA_UNAVAILABLE — a 503, retryable, and emphatically not a 500.
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
	// contract is that an expiry is answered rather than waited out — so no request may
	// hang.
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

// TestSubscribersAPI_NeverReturnsTheCredentialReference is the secret-handling
// assertion at the HTTP boundary, and it is the one that would matter most if it
// failed.
//
// The registry stores a non-reversible reference.
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
// It is built from the exported route pattern rather than written as a literal so this
// file cannot drift from api/api.go's registration: the pattern is the single source of
// truth the router, the sunset interceptor and these tests all read.
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

// TestSubscribersAPI_WebhookSubscriptionLifecycleThroughTheRealRouter walks the
// deprecated surface end to end while the dual-delivery window is open.
//
// The router is built with NO retirement instant, so the sunset interceptor passes the
// request through and the handlers actually run.
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

// TestSubscribersAPI_WebhookSubscriptionRefusesAnUnsafeDestination is the SSRF policy
// at the HTTP boundary, applied at the same standard on create and on replace.
//
// This route's whole purpose is to accept a URL, which makes it the most likely way an
// internal address reaches a column that is a future request sink.
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

// TestSubscribersAPI_WebhookSubscriptionRefusesAnUnknownSubscriber pins the not-found
// path on all four verbs.
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

// TestSubscribersAPI_WebhookSubscriptionIsMasterKeyGated covers the privileged-endpoint
// gate on the deprecated surface.
//
// The gate must hold on all four verbs while the window is open.
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

// TestSubscribersAPI_WebhookSubscriptionNeverReturnsTheSigningSecret keeps the
// deprecated read shape from becoming a secret-bearing response.
//
// The legacy transport's signing secret and configured headers are deployment-wide
// configuration, not per-subscriber data.
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
// route, against no broker at all.
//
// THE AUTHORIZATION RESOURCE MAP.
//
// THAT THE GATE SHORT-CIRCUITS.
//
// THE FIVE-SECOND CEILING AS AN HTTP OUTCOME.
//
// THE PLAINTEXT SASL PASSWORD.
//
// Rules status, stated because UR4 requires it: review_rules reports NO USER RULES for
// this project.
// =======================================================================================

// subscribersAPITestMasterKey is the master secret the mock-backed router runs with.
//
// It is a test fixture and deliberately self-describing: it matches no provider's
// credential format, so a secret scanner reading this file finds a string that says
// what it is rather than something it has to guess about.
const subscribersAPITestMasterKey = "subscribers-api-test-master-key-not-a-real-credential"

// subscribersHarness describes the deployment a mock-backed router is built for.
//
// The three knobs exist because the properties below need genuinely different
// deployments, and a single boolean could not express them:
//
//   - secure selects the authentication middleware. TRUE is required to prove anything
//     about the resource map or about scopes; FALSE is what allows the master-key flag
//     to be injected directly, which is the only way to observe a request that reaches
//     a handler while making NO datasource call at all.
//   - masterKey is the injected principal, honoured only when secure is false.
//   - brokers configures Kafka. Empty is the graceful-degradation deployment the
//     ordinary suite runs in; non-empty is what lets a request reach the registry
//     through the credential route, which is where the ceiling lives.
type subscribersHarness struct {
	secure    bool
	masterKey bool
	brokers   []string

	// keyScopeGateway declares a key-authorising component in front of the brokers, at
	// these addresses. Empty is the SHIPPED DEFAULT, under which issuance refuses a
	// subscriber recording a partition_key_prefix — so a test that needs a key-scoped
	// issuance to proceed sets this, and a test asserting the refusal must not.
	keyScopeGateway []string

	// allowLoopbackIssuance declares the deployment a LOCAL-DEVELOPMENT host, which is the
	// only state in which a loopback peer establishes a confidential channel for
	// credential issuance.
	allowLoopbackIssuance bool

	// trustForwardedProto declares the proxy in front of Blnk that terminated TLS and sets
	// X-Forwarded-Proto, which is the production Kubernetes shape: TLS ends at the ingress
	// and the hop to the pod is plaintext, so the process never sees a handshake. Without
	// the declaration the header is a claim any caller can make and is not believed.
	trustForwardedProto bool

	// trustedProxies names the peers that declaration applies to
	// (BLNK_SERVER_TRUSTED_PROXIES), and it is the SECOND HALF of the forwarded-HTTPS
	// channel rather than a refinement of it.
	trustedProxies string
}

// setupSubscribersRouter builds a router over a mock datasource and returns both.
//
// The mock is returned because it IS the assertion surface for half the properties in
// this section: which repository methods a request reached, with which arguments, and —
// most often — that it reached none.
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

	// RESTORED WHEN THE TEST ENDS. config.ConfigStore is a process-global atomic.Value, so
	// a broker list, a master key or a sunset date installed here is read by every later
	// test in the package that calls config.Fetch — including the ones asserting the
	// unconfigured or insecure posture, which then fail somewhere that never mentioned
	// Kafka. Saving and restoring is the established idiom; see the same block in
	// newEventsAPIOverMockDatasource.
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

	// THE MOCK IS HANDED OVER UNWRAPPED, because the authentication middleware does not
	// read c.Request from inside the background last-used update it starts. Interposing a
	// synchronised datasource plus a barrier middleware, to order a non-master request ahead
	// of that update, would be scaffolding for a race this code does not have.
	service, err := blnk.NewBlnk(datasource)
	require.NoError(t, err, "the service container must be constructible over the mock store")

	instance := NewAPI(service)
	require.NotNil(t, instance, "NewAPI returned nil, which means the configuration was unusable")

	if !harness.secure {
		// Injected into the API's OWN engine before the routes are registered, exactly as
		// setupHookRouter and eventsRouter do. Wrapping the finished router in a second
		// engine does not work: the nested context is not the one the handlers read.
		instance.router.Use(func(c *gin.Context) {
			c.Set("isMasterKey", harness.masterKey)
			c.Next()
		})
	}

	return instance.Router(), datasource
}

// subscribersTestAPIKey builds a VALID non-master API key carrying the given scopes.
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
// The credential route refuses to put a one-time password on a channel the deployment
// has not established as confidential, so every call has to present one of the three.
// Defaulting to it made every test in this file assert against a posture no production
// deployment should have.
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

	// IN-PROCESS TLS, unless the call is deliberately plaintext. A non-nil Request.TLS is
	// what a server that completed the handshake itself records, and it is the one
	// confidential channel that needs no declaration by the deployment — which makes it
	// the honest default for a harness whose subject is the handlers rather than the
	// transport gate.
	if !call.plaintext {
		request.TLS = &tls.ConnectionState{}
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

// subscribersEveryRoute is the whole MASTER-KEY-GATED surface: the six registry routes
// and the four deprecated webhook-subscription ones.
//
// It is a function rather than a package-level slice so that each caller gets its own
// copy and cannot mutate a shared table.
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
// CredentialReference and WebhookURL are set on every fixture.
func subscribersFixtureRow(subscriberID string) *coremodel.EventSubscriber {
	principal, err := coremodel.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		// A fixture built from a non-canonical identifier would exercise the validator rather
		// than the handler, so this is a defect in the test rather than a case to handle at
		// run time. Panicking names it at the point it was introduced.
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
// AUTHORIZED-CALLER half of the two-file registration proof, and it is the half that
// was missing.
//
// TestSubscribersAPI_AuthorizationResourceIsRegistered above drives every route with a
// key scoped to ANOTHER feature. That refusal proves the prefix resolves and names the
// scope it resolves to, and it stops there — by construction, because the request is
// refused by the MIDDLEWARE. Everything past that point is untested by it:
//
//   - Whether a caller granted exactly `subscribers:<action>` is ADMITTED.
//   - Whether the ENDPOINT'S OWN master-key gate then refuses. AUTH_MASTER_KEY_REQUIRED
//     is the only code that proves a request traversed the entire chain and was stopped
//     last by the handler.
//   - Whether the gate refuses BEFORE doing work. A gate that fires after the service
//     call has started is not a gate, and on the credential route it would mint a
//     credential for a caller it was about to refuse.
func TestSubscribersAPI_ACorrectlyScopedKeyIsAdmittedAndRefusedByTheHandlersGate(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	// The action each method maps to in middleware.methodToAction. The scope granted below
	// is built from it, so a key is granted EXACTLY what the route requires and nothing
	// more: a wildcard would pass even if the method mapped to the wrong action.
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
			// read further down.
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
			// this code.
			assertErrorCode(t, recorder,
				http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)

			// THE CREDENTIAL WAS ADMITTED, which the audit write is the observable proof of: the
			// middleware performs it only for a principal it authenticated and permitted, so its
			// arrival says the refusal below came from the HANDLER rather than from
			// authentication. It is also the join that makes the call-log read that follows
			// safe.
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

// subscribersAssertSecretAbsent requires that a corpus does not contain the one-time
// password, without handing testify either the secret or the text containing it.
//
// So the comparison happens in Go and only a BOOLEAN reaches testify.
//
// Parameters:
//   - t *testing.T: the test.
//   - corpus string: the text that must not contain the secret. Never printed.
//   - secret string: the live one-time password. Never printed.
//   - what string: a description of the corpus, printed on failure.
func subscribersAssertSecretAbsent(t *testing.T, corpus, secret, what string) {
	t.Helper()

	// An empty secret would make every Index call return 0 and every assertion below fail
	// for a reason that has nothing to do with disclosure, so the fixture's own
	// precondition is checked rather than assumed.
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

// expectSubscribersAPIKeyLookup programs the two datasource calls the authentication
// middleware makes for a non-master credential, and returns a channel that reports when
// the second has been RECORDED.
//
// The channel is what serialises the assertion, rather than a gin middleware waiting
// after c.Next().
//
// Parameters:
//   - datasource *mocks.MockDataSource: the datasource to program.
//   - key *coremodel.APIKey: the principal the lookup resolves to.
//
// Returns:
//   - <-chan struct{}: signalled once the background last-used update has been
//     recorded.
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

// requireSubscribersLastUsedTouch joins an authenticated request with the background
// last-used update the middleware starts for it, and FAILS if that update never
// happens.
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

// subscribersRepositoryCallsExcludingAuth names every datasource method a request
// reached apart from the two the authentication middleware itself makes.
//
// Parameters:
//   - datasource *mocks.MockDataSource: the store behind the router.
//
// Returns:
//   - []string: the method names reached, in call order, excluding authentication's
//     own.
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
// "subscribers" authorization resource, and it is the only one in this package that can
// be.
//
// The middleware matches the master key and returns BEFORE the resource is resolved. A
// master-key request therefore reaches the handler whether the prefix is mapped or not,
// so a reachability test built on the master key passes VACUOUSLY with pathToResource
// unedited. Reaching the map at all takes secure mode and a valid NON-MASTER key, which
// is what the mock store makes possible without seeding a database row.
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

				// THE PROOF that pathToResource carries "subscribers". Asserted explicitly and
				// separately, because this is the failure the whole test exists for and the two
				// codes share a status.
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

// TestSubscribersAPI_GateHelperAnswersTheRefusalItself unit-tests the gate in
// isolation, as api/hooks_test.go does for its counterpart.
//
// ensureSubscriberManagementAuthorized both DECIDES and RESPONDS, and the two halves
// fail independently: a helper that returned false without writing anything would
// produce an empty 200, and one that wrote the refusal but returned true would carry on
// into the handler after answering.
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

// TestSubscribersAPI_MasterKeyGateShortCircuitsBeforeTheRegistry proves the gate
// refuses BEFORE any work, on all ten endpoints.
//
// "Refused" and "refused before doing anything" are different claims, and only the
// second one is a gate.
func TestSubscribersAPI_MasterKeyGateShortCircuitsBeforeTheRegistry(t *testing.T) {
	subscriberID := uniqueSubscriberID()

	for _, call := range subscribersEveryRoute(subscriberID) {
		t.Run(call.method+" "+call.path, func(t *testing.T) {
			router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: false})

			recorder := subscribersServe(t, router, call)

			assertErrorCode(t, recorder, http.StatusForbidden, apierror.ErrAuthMasterKeyRequired)

			// THE DECISIVE ASSERTION. Not "these particular methods were not called" but "the
			// store was not touched at all", which no future handler can slip past by reaching
			// for a method this list does not name.
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

// TestCreateSubscriber_AnswersTheRegisteredSubscriber pins the registration response
// and the row the handler asks the registry to store.
//
// Both halves matter.
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
	// handling and this projection.
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

// TestCreateSubscriber_RefusesAnUnbindableBodyBeforeTheRegistry covers the binding
// failures.
//
// Each case is refused with a TYPED code rather than a panic recovered by the
// middleware, and — the part worth asserting — none of them reaches the store. A body
// that cannot be bound cannot describe a subscriber, so a write attempted from it would
// be a write of whatever the zero value happened to be.
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
// An empty collection is not a missing one.
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
// The fixture row deliberately carries BOTH columns a response must never disclose —
// the credential reference and the legacy webhook URL — so the assertions below can
// fail.
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

// TestSubscribersAPI_UntypedNotFoundBecomesTheSubscriberCode is the catch-all guard,
// and it is the one assertion in this file that pins a decision made in api/errors.go.
//
// classifyMessage ends with a broad {"not found"} entry that resolves to GEN_NOT_FOUND.
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
// The sequence is asserted, not just the status.
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
// declarations readable, and it works for a call that may not have happened — which is
// what lets the caller assert its absence rather than dereference a nil.
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
func TestDeleteSubscriber_AnswersNoContentWithAnEmptyBody(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{masterKey: true})

	subscriberID := uniqueSubscriberID()
	stored := subscribersFixtureRow(subscriberID)
	// A subscriber that HOLDS a credential cannot be deregistered without a reachable
	// broker — see the sub-test below, which is the fail-closed half of this contract.
	// This one is about the success shape, so the row records no issuance.
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

// TestSubscribersAPI_BlankIdentifierIsAMissingParameterOnEveryRoute keeps the two
// parameter refusals distinct across the whole surface.
//
// A blank-but-present segment is a MISSING parameter; a present-but-unusable one is a
// VALIDATION error.
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

// TestIssueKafkaCredentials_AnswersAtTheCeilingWhenProvisioningStalls is the
// requirement as an HTTP outcome rather than as a documented intention.
//
// Credential provisioning must complete within five seconds.
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

	// The provisioning claim is the FIRST store call issuance makes, which is what keeps
	// the stall to exactly one method: the service returns before registering its
	// compensation when the claim fails, so nothing else is reached on the way out.
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
		http.StatusServiceUnavailable, apierror.ErrSubscriberProvisioningFailed)

	assert.GreaterOrEqual(t, elapsed, blnk.SubscriberCredentialIssuanceBudget,
		"answering EARLIER than the budget means something other than the ceiling produced "+
			"this response, and the test would be proving nothing")
	assert.Less(t, elapsed, blnk.SubscriberCredentialIssuanceBudget+2*time.Second,
		"the request must end at its own ceiling. Waiting for the work instead is the "+
			"fifteen-second wall clock of issuance, then compensation, then the fence release")

	// NO SECRET ON THE ABANDONED PATH. The handler receives a zero-valued credential when
	// the ceiling fires, so there is nothing to serialise — and the message says a
	// credential may nevertheless exist at the broker, because that is the one thing the
	// caller cannot otherwise know and the thing that decides what it should do next.
	assert.NotContains(t, recorder.Body.String(), `"password"`,
		"an abandoned issuance returns no credential field at all")
	assert.Contains(t, recorder.Body.String(), "Re-issue to obtain one",
		"the caller is told the remedy: Kafka holds one credential per principal, so "+
			"re-issuing replaces whatever the abandoned attempt left behind")

	// THE LOG LINE PSEUDONYMISES THE SUBSCRIBER. A subscriber id is a tenant-chosen name
	// that reaches logs, alert annotations and incident tickets, and every other layer
	// emits the keyed digest under a _hash key.
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
	// Everything above is about the REQUEST, and the request is over.
	releaseStall()
	requireAbandonedIssuanceSettled(t, logs)
}

// subscribersAbandonedIssuanceMarker is the last thing an abandoned issuance does.
//
// The service classifies a budget spent on the provisioning claim as a timeout and says
// so at Warn before returning; nothing it does afterwards touches anything a test owns.
const subscribersAbandonedIssuanceMarker = "ran out of time at the registry"

// subscribersAbandonedIssuanceTimeout bounds the wait for that line.
//
// It is generous because it is never reached in a correct run — the attempt is already
// released and has one classification left to perform — and because reaching it means
// the attempt did not finish, which is a real leak rather than a slow machine.
const subscribersAbandonedIssuanceTimeout = 10 * time.Second

// requireAbandonedIssuanceSettled waits for an abandoned credential issuance to finish,
// and FAILS if it never does.
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
// Every other part of Blnk runs unchanged without Kafka — the publisher resolves to a
// no-op, exactly as SendWebhook no-ops without a configured URL.
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

// TestIssueKafkaCredentials_RefusesAnUnconfidentialTransportBeforeMintingAnything
// covers the gate that exists only on this route.
//
// The request below is PLAINTEXT with a remote peer: no in-process TLS, no declared
// proxy, and no loopback.
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

// TestIssueKafkaCredentials_RefusesALoopbackPeerOnAnUndeclaredHost is the production
// half of the transport gate, and the one that closes a real disclosure.
//
// Request.RemoteAddr is the far end of the accepted socket, so a loopback value
// establishes that the LAST hop stayed on this host.
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

// TestIssueKafkaCredentials_AllowsALoopbackPeerOnADeclaredLocalDevelopmentHost is the
// exception, tested separately from the production posture on purpose.
//
// The local stack is a plaintext listener reached over loopback and nothing else, so
// without this declaration `make run` plus curl could never issue a credential.
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

// TestIssueKafkaCredentials_AllowsADeclaredProxyReportingHTTPS is the third channel,
// and the one a production Kubernetes deployment actually uses.
//
// TLS terminates at the ingress and the hop to the pod is plaintext, so the process
// never sees a handshake.
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
		// THE GRANT-SCOPE CASE. This combination is the one a deployment
		// reaches by setting the flag and nothing else — so the refusal has to hold here or
		// the peer condition is decorative.
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
		// validation refuses this outright; here — secure off, as the whole harness runs —
		// the channel simply establishes nothing.
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

// TestIssueKafkaCredentials_RefusesAnUnknownSubscriber keeps the missing-row answer on
// the route that mints secrets.
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

// subscribersKafkaEnvironment resolves the broker environment for the end-to-end
// issuance test, or explains what is missing.
//
// It returns a REASON rather than skipping itself, so the caller decides.
//
// KAFKA_SUBSCRIBER_BROKERS is required and deliberately NOT defaulted to KAFKA_BROKERS.
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
		// stack needs waived. Scoped to the TLS-off case so an operator running against a TLS
		// broker never sees it applied.
		InsecureLocalDev: !tlsEnabled,
	}, "", true
}

// TestIssueKafkaCredentials_ReturnsTheConnectionDetailsAndTheSecretExactlyOnce is the
// highest-value test in this file, and the only one in this package that can prove the
// secret-handling posture rather than describe it.
//
// blnk.SubscriberCredential holds its password in an unexported field with no exported
// constructor, deliberately, so no test outside the root package can fabricate one.
//
// THE RESPONSE CONTRACT: the broker endpoint, the authorised topic list, the consumer
// group and the SASL credentials — username, password, mechanism — are all present and
// non-empty.
//
// THE BUDGET, from the other side: a real issuance completes far inside five seconds.
//
// THE SECRET IS RETURNED ONCE.
//
// THE SECRET IS NEVER LOGGED. Every logrus entry emitted during the issuance is
// captured and searched, message and fields alike.
//
// The store is a mock so that the arguments the credential record was written with can
// be inspected directly.
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

	// SANITISED, and this is the assertion that made it necessary: the message renders
	// only when the status is NOT 200, which is precisely the regression — a handler
	// answering 500 while still marshalling the credential — that would write a real SASL
	// password into CI output. The assertions below that forbid a secret in the body
	// cannot help, because a failed require aborts before they run.
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
		"issuance must complete well inside its five-second budget; it took %s",
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

	// 5. ONLY A NON-REVERSIBLE REFERENCE AND AN ISSUANCE TIMESTAMP ARE PERSISTED.
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
	//    written fails AFTER the password was generated, which is the one failure path a
	//    plaintext could ride out on.
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

// TestSubscribersAPI_NoPersistedShapeCanCarryAPlaintextSecret is the schema-level half
// of the posture, and it runs whether or not a broker is configured.
//
// The end-to-end test above proves that no plaintext WAS persisted on the path it
// exercised.
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

// TestSubscribersAPI_NoFailureDiagnosticCanCarryTheIssuedSecret is the other half of
// that posture: the response shape may carry a plaintext exactly once, and nothing this
// suite WRITES may.
//
// CI output is durable and broadly readable, so a secret written there is disclosed
// however carefully the endpoint behaves afterwards — which is the one-time-issuance
// posture undone by its own test suite.
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
	// assertion in this suite is written with, so it will be reached for again by anyone
	// adding a case here.
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
		// what gets written out on failure.
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
// statusByCode in internal/apierror/codes.go is the single source of truth for every
// code's default status, and StatusForCode defaults an UNKNOWN code to 500.
func TestSubscribersAPI_TypedCodesResolveToIntendedStatuses(t *testing.T) {
	for code, status := range map[apierror.ErrorCode]int{
		apierror.ErrSubscriberNotFound:           http.StatusNotFound,
		apierror.ErrSubscriberProvisioningFailed: http.StatusServiceUnavailable,
		apierror.ErrKafkaUnavailable:             http.StatusServiceUnavailable,
		apierror.ErrAuthMasterKeyRequired:        http.StatusForbidden,
		// The rest of the codes this surface can answer, mapped for the same reason.
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
// THERE IS NO DATA-PLANE ROUTE UNDER /subscribers, and this section is what enforces
// that
//
//  1. It was a SECOND DATA PLANE holding a credential far wider than any subscriber's.
//  2. It authenticated with a BESPOKE HEADER, X-Blnk-Subscriber-Secret, which the
//     platform's authorization middleware knows nothing about.
//  3. REVOCATION DID NOT REVOKE. Removing a subscriber's SCRAM credential at the broker
//     closed the direct path and left the served one open, because the served one
//     authenticated against a registry column rather than against the broker.
// ---------------------------------------------------------------------------------------

// TestSubscribersAPI_HasNoRecordServingRoute is the guard that keeps the removal
// removed.
//
// A deleted route leaves no compile error behind.
//
// The master key is presented on purpose. A 404 while holding the most privileged
// credential the deployment has is the strongest available statement that nothing is
// registered there — a refusal that depended on the caller's scope would leave the
// route reachable by someone.
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

// TestSubscribersAPI_DoesNotReadASubscriberSecretHeader pins the second half of the
// removal: not merely that the route is gone, but that no surviving route accepts the
// header it used.
func TestSubscribersAPI_DoesNotReadASubscriberSecretHeader(t *testing.T) {
	router, datasource := setupSubscribersRouter(t, subscribersHarness{secure: true})

	subscriberID := uniqueSubscriberID()

	// A key scoped to the subscriber resource, so the request is authenticated and
	// authorised as far as the master-key gate. Whatever refuses it below refuses it there
	// and not earlier.
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

// startKeyScopeGatewayStub starts a stub for the key-authorising component a deployment
// declares in front of its brokers, and returns its control endpoint and bearer token.
//
// Issuance for a key-scoped subscriber now BINDS the recorded prefix at that component
// and requires it to attest the binding back before a secret exists.
//
// Parameters:
//   - t *testing.T: for the helper marker and to close the server when the test ends.
//
// Returns:
//   - string: the control endpoint to declare in
//     KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL.
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
