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

// Tests for what the subscriber service RETURNS when something outside Blnk fails.
//
// The service was already careful with its logs: every failure path builds its fields
// through sanitizeLogValue before writing them.
//
// The causes are not innocuous.
//
// Every test here checks the JSON the caller would actually receive, not just the field
// values.
package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// subscriberHostileCause is a cause of the exact shape the Kafka client produces,
// carrying every kind of value that must not reach a response.
func subscriberHostileCause() error {
	return &net.OpError{
		Op:  "write",
		Net: "tcp",
		Addr: &net.TCPAddr{
			IP:   net.IPv4(10, 0, 0, 7),
			Port: 9092,
		},
		Err: errors.New("broken pipe on broker-alpha.internal"),
	}
}

// subscriberHostileFragments are the strings that must not appear anywhere in a response.
//
// Each is present in subscriberHostileCause and each is a distinct disclosure: an internal
// address, a broker hostname, a port, and the transport verb that identifies the operation.
func subscriberHostileFragments() []string {
	return []string{"10.0.0.7", "broker-alpha.internal", "9092", "broken pipe"}
}

// requireNoHostileFragments asserts that none of the hostile strings survives into the
// marshalled form of an error, and returns the marshalled bytes for further assertions.
//
// Marshalling is the operation under test: an APIError's Details member is serialised
// into the response body, so what json.Marshal produces IS what a caller receives.
func requireNoHostileFragments(t *testing.T, err error) string {
	t.Helper()

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr, "the failure must surface as a typed API error")

	encoded, marshalErr := json.Marshal(apierror.ErrorResponse{Error: apiErr})
	require.NoError(t, marshalErr, "the error response must be serialisable")

	body := string(encoded)
	for _, fragment := range subscriberHostileFragments() {
		assert.NotContains(t, body, fragment,
			"the response body leaks %q; a cause reaching Details is serialised to the caller", fragment)
	}

	return body
}

// newSubscriberErrorFixture returns a service and a registry row for the failure paths.
func newSubscriberErrorFixture(t *testing.T) (*EventSubscriberService, *model.EventSubscriber) {
	t.Helper()

	service := NewEventSubscriberService(nil, nil)
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Logf("closing the subscriber service: %v", err)
		}
	})

	subscriberID := model.GenerateSubscriberID()
	principal, err := model.CanonicalKafkaPrincipal(subscriberID)
	require.NoError(t, err)

	return service, &model.EventSubscriber{
		SubscriberID:   subscriberID,
		Name:           "error fixture",
		KafkaPrincipal: principal,
	}
}

// TestProvisioningFailure_ReturnsABoundedDetailForEveryBranch is the bounded-detail
// guard.
//
// All four branches of provisioningFailure must bound the cause rather than pass it raw.
func TestProvisioningFailure_ReturnsABoundedDetailForEveryBranch(t *testing.T) {
	cases := []struct {
		name               string
		cause              error
		result             SubscriberProvisioningResult
		wantCode           apierror.ErrorCode
		wantRetryable      bool
		wantCredential     bool
		wantCompensated    bool
		wantReasonFragment string
	}{
		{
			name:               "not configured",
			cause:              fmt.Errorf("wrapping: %w", ErrKafkaAdminNotConfigured),
			wantCode:           apierror.ErrKafkaUnavailable,
			wantRetryable:      false,
			wantReasonFragment: "not configured",
		},
		{
			name: "budget expired",
			// RETRYABLE, and the flag is the whole answer: the log says a retry re-provisions
			// the same boundary idempotently, so the detail has to say the same thing or the
			// caller cannot act on it.
			cause:              fmt.Errorf("provisioning: %w", context.DeadlineExceeded),
			result:             SubscriberProvisioningResult{CredentialWritten: true},
			wantCode:           apierror.ErrSubscriberProvisioningFailed,
			wantRetryable:      true,
			wantCredential:     true,
			wantReasonFragment: "did not complete within the budget",
		},
		{
			name: "caller cancelled",
			// Also the provisioning-failure code, and retryable for the same reason. The partial
			// broker state stays in the DETAIL, which is where it always was and the only place
			// it could be — a status code cannot say whether a credential the caller does not
			// hold may already exist.
			cause:              fmt.Errorf("provisioning: %w", context.Canceled),
			wantCode:           apierror.ErrSubscriberProvisioningFailed,
			wantRetryable:      true,
			wantReasonFragment: "cancelled",
		},
		{
			name: "compensation failed, so a principal exists with no boundary",
			// The one state that needs a human. The flags are what tell the caller a
			// credential they do not hold may still authenticate.
			cause:              subscriberHostileCause(),
			result:             SubscriberProvisioningResult{CredentialWritten: true},
			wantCode:           apierror.ErrSubscriberProvisioningFailed,
			wantRetryable:      false,
			wantCredential:     true,
			wantReasonFragment: "failed at the broker",
		},
		{
			name:               "compensated, so the broker is clean",
			cause:              subscriberHostileCause(),
			result:             SubscriberProvisioningResult{CredentialWritten: true, Compensated: true},
			wantCode:           apierror.ErrSubscriberProvisioningFailed,
			wantRetryable:      false,
			wantCredential:     true,
			wantCompensated:    true,
			wantReasonFragment: "failed at the broker",
		},
		{
			name:               "nothing was written",
			cause:              subscriberHostileCause(),
			wantCode:           apierror.ErrSubscriberProvisioningFailed,
			wantRetryable:      false,
			wantReasonFragment: "failed at the broker",
		},
		{
			// THE PRINCIPAL ALREADY CARRIED ALLOW BINDINGS BLNK DID NOT PROVISION, and this
			// branch used to fall through to the provisioning-failure return above, answering
			// 503 — the class reserved for a transient dependency failure. The broker answered
			// perfectly well; the refusal is a judgement about the access its bindings amount
			// to, and only a human removing them can change it. So it carries the same code the
			// post-provisioning discovery of the identical condition carries, and its 409, or
			// the same finding would report two status classes depending on when it was noticed
			// — with the 503 half inviting a retry that can never succeed.
			name:               "the principal carries foreign ALLOW bindings",
			cause:              fmt.Errorf("provisioning: %w", ErrSubscriberForeignACLGrant),
			result:             SubscriberProvisioningResult{CredentialWritten: true, Compensated: true},
			wantCode:           apierror.ErrSubscriberAccessExceedsAuthorization,
			wantRetryable:      false,
			wantCredential:     true,
			wantCompensated:    true,
			wantReasonFragment: "foreign ALLOW ACL bindings",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			service, subscriber := newSubscriberErrorFixture(t)

			// ctx and a claim token are threaded in because provisioningFailure now SETTLES what
			// the broker was left holding before it classifies the failure. The fixture's store
			// has no claim on this row, so the settlement write is refused — which is deliberate
			// here: this table is about the typed error each outcome produces, and the
			// settlement behaviour has its own tests. A refused settlement must not change the
			// answer the caller receives, and that is exactly what this asserts.
			err := service.provisioningFailure(
				context.Background(), subscriber, "settlement-fence-token", tc.result, tc.cause,
			)
			require.Error(t, err)

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.Code)

			detail, ok := apiErr.Details.(SubscriberErrorDetail)
			require.True(t, ok,
				"the detail must be the bounded type, not the cause; got %T", apiErr.Details)

			assert.Contains(t, detail.Reason, tc.wantReasonFragment,
				"the reason must describe the failure in fixed wording")
			assert.Equal(t, subscriber.SubscriberID, detail.SubscriberID,
				"the caller's own identifier is what lets them and an operator correlate; it is not a disclosure")
			assert.Equal(t, tc.wantRetryable, detail.Retryable,
				"whether repeating the request may succeed is the actionable half of the diagnosis")
			assert.Equal(t, tc.wantCredential, detail.CredentialWritten,
				"what reached the broker is the fact a caller cannot otherwise learn")
			assert.Equal(t, tc.wantCompensated, detail.Compensated)

			// THE PRINCIPAL IS NOT IN THE RESPONSE. It is in the log, where the failure paths
			// name it deliberately so a human knows what to revoke, but a response body naming a
			// principal whose provisioning just failed tells a reader which identity to attack.
			body := requireNoHostileFragments(t, err)
			assert.NotContains(t, body, subscriber.KafkaPrincipal,
				"the response must not name the Kafka principal")
		})
	}
}

// TestNewSubscriberErrorDetail_CannotCarryACause pins the property that makes the
// guarantee structural rather than a matter of care at each call site.
//
// The constructor takes no cause, so there is no argument through which one could
// arrive.
func TestNewSubscriberErrorDetail_CannotCarryACause(t *testing.T) {
	detail := NewSubscriberErrorDetail("something failed", "sub_0f6e2c8a", true)

	assert.Equal(t, "something failed", detail.Reason)
	assert.Equal(t, "sub_0f6e2c8a", detail.SubscriberID)
	assert.True(t, detail.Retryable)

	// Every exported member must be a scalar. An error, an interface or a nested struct is
	// how a cause gets back in.
	encoded, err := json.Marshal(detail)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	for key, value := range decoded {
		switch value.(type) {
		case string, bool, float64:
		default:
			t.Fatalf("member %q serialises as %T; only scalars may cross the response boundary, "+
				"because anything structured is how a cause returns", key, value)
		}
	}

	t.Run("the subscriber identifier is bounded and single-line", func(t *testing.T) {
		// The identifier reaches a response body and a log field, so it gets the same
		// treatment every other identifier in this pipeline gets. It is validated upstream,
		// but this is the last gate and it must not depend on that.
		hostile := "sub_" + strings.Repeat("z", maxLoggedFilterLength*2) + "\nFORGED"
		bounded := NewSubscriberErrorDetail("reason", hostile, false)

		assert.NotContains(t, bounded.SubscriberID, "\n",
			"a newline in a log field forges an entry in a line-oriented aggregator")
		assert.LessOrEqual(t, len([]rune(bounded.SubscriberID)),
			maxLoggedFilterLength+len([]rune(logTruncationSuffix)),
			"the identifier must be length-capped")
	})

	t.Run("an absent identifier is omitted rather than empty", func(t *testing.T) {
		// The administrative-client paths have no subscriber in hand, and an empty
		// subscriber_id key would read as "the subscriber is blank" instead of "this failure
		// is not about one subscriber".
		encoded, err := json.Marshal(NewSubscriberErrorDetail("no subscriber here", "", false))
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), "subscriber_id")
	})
}

// TestSubscriberBrokerReconciliation_ReturnsABoundedDetailForEveryPath covers the three
// AUTHORIZATION-RECONCILIATION paths, which kept the defect after provisioningFailure
// was fixed.
//
// The cause here is not a tidy sentence.
//
// Every one of the three is reported RETRYABLE, and that is a claim about state rather
// than optimism: the prune runs before the registry is written, the grant runs after it
// and is additive, and revocation is idempotent at the broker with a tombstone left on
// the row.
func TestSubscriberBrokerReconciliation_ReturnsABoundedDetailForEveryPath(t *testing.T) {
	renamed := "reconciliation probe"

	cases := []struct {
		name           string
		failingMethod  string
		invoke         func(*subscriberLifecycle) error
		reasonFragment string
	}{
		{
			name:          "pruning the obsolete grants",
			failingMethod: "PruneSubscriberAccess",
			invoke: func(run *subscriberLifecycle) error {
				_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
					SubscriberUpdate{AuthorizedTopics: []string{"blnk.transactions"}})

				return err
			},
			reasonFragment: "obsolete Kafka grants",
		},
		{
			name:          "creating the new grants",
			failingMethod: "GrantSubscriberAccess",
			invoke: func(run *subscriberLifecycle) error {
				// AN AUTHORIZATION CHANGE, not a rename. The broker is touched only
				// when the recorded authorization could have moved, so a rename no longer reaches
				// the grant step at all — see
				// TestUpdateSubscriber_TouchesTheBrokerOnlyWhenTheAuthorizationCouldHaveMoved.
				_, err := run.service.UpdateSubscriber(context.Background(), subscriberFixtureID,
					SubscriberUpdate{
						Name:             &renamed,
						AuthorizedTopics: []string{"blnk.transactions", "blnk.balances"},
					})

				return err
			},
			reasonFragment: "new Kafka grants",
		},
		{
			name:          "revoking during deregistration",
			failingMethod: "RevokeSubscriber",
			invoke: func(run *subscriberLifecycle) error {
				_, err := run.service.DeregisterSubscriber(context.Background(), subscriberFixtureID)

				return err
			},
			reasonFragment: "Revoking the subscriber's Kafka access",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := newSubscriberLifecycle(t)
			run.admin.failing(tc.failingMethod, subscriberHostileCause())

			err := tc.invoke(run)
			require.Error(t, err, "a broker refusal must not be reported as success")

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, apierror.ErrSubscriberProvisioningFailed, apiErr.Code)

			detail, ok := apiErr.Details.(SubscriberErrorDetail)
			require.True(t, ok,
				"the detail must be the bounded type, not the cause; got %T", apiErr.Details)
			assert.Contains(t, detail.Reason, tc.reasonFragment,
				"the reason must say which reconciliation step failed, in fixed wording")
			assert.Equal(t, subscriberFixtureID, detail.SubscriberID)
			assert.True(t, detail.Retryable,
				"each of these three leaves a state a repeat of the same request completes")

			body := requireNoHostileFragments(t, err)
			assert.NotContains(t, body, "blnk-sub-"+subscriberFixtureID,
				"the response must not name the Kafka principal whose reconciliation just failed")
		})
	}
}

// TestSubscriberService_AdminConstructionFailureWithholdsTheCause covers the
// administrative-client path, which is the most disclosure-prone in the file.
//
// Building the client reads TLS material from disk and prepares a SCRAM mechanism, so
// the failure can name a filesystem path inside the container or the administrative
// principal — and because the transport REFUSES plaintext by returning an error, an
// ordinary misconfiguration reaches this branch on a normal deployment rather than only
// in a disaster.
func TestSubscriberService_AdminConstructionFailureWithholdsTheCause(t *testing.T) {
	// Brokers configured but neither TLS nor the local-dev acknowledgement, which is the
	// refusal NewKafkaTransport documents. No broker is contacted.
	storeSubscriberErrorConfiguration(t)

	service := NewEventSubscriberService(nil, nil)
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Logf("closing the subscriber service: %v", err)
		}
	})

	_, err := service.provisioner()
	require.Error(t, err, "an unbuildable administrative client must be an error, not a nil client")

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.ErrKafkaUnavailable, apiErr.Code,
		"an unbuildable client is a Kafka availability problem from the caller's point of view")

	detail, ok := apiErr.Details.(SubscriberErrorDetail)
	require.True(t, ok, "the detail must be the bounded type; got %T", apiErr.Details)
	assert.NotEmpty(t, detail.Reason)
	assert.False(t, detail.Retryable,
		"a configuration refusal does not clear by retrying; it clears by being configured")

	encoded, marshalErr := json.Marshal(apierror.ErrorResponse{Error: apiErr})
	require.NoError(t, marshalErr)

	body := string(encoded)
	// The transport's refusal names the environment variables and explains the exposure.
	// That is exactly the right message for an operator reading a log and exactly the
	// wrong one for a response body: it confirms to a caller what the deployment's
	// transport posture is.
	assert.NotContains(t, body, "KAFKA_INSECURE_LOCAL_DEV",
		"the transport's own refusal text must not reach the caller")
	assert.NotContains(t, body, "KAFKA_TLS_ENABLED")
	assert.NotContains(t, body, "10.0.0.7")
}

// storeSubscriberErrorConfiguration makes the package's configuration seam return a
// Kafka block that names brokers but permits no transport, so building the
// administrative client fails without any I/O.
func storeSubscriberErrorConfiguration(t *testing.T) {
	t.Helper()

	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	original := fetchConfiguration
	t.Cleanup(func() { fetchConfiguration = original })

	fetchConfiguration = func() (*config.Configuration, error) {
		return &config.Configuration{
			Kafka: config.KafkaConfig{
				Brokers:     []string{"10.0.0.7:9092"},
				TopicPrefix: DefaultTopicPrefix,
				// Neither TLS nor the local-dev acknowledgement: NewKafkaTransport refuses,
				// which is the ordinary misconfiguration this branch exists for.
			},
		}, nil
	}
}
