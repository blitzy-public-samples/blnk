// Copyright 2024 Blnk Finance Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blnk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// keyScopeGatewayDouble is a CONFORMANCE DOUBLE for the key-authorising component a deployment
// declares in front of its brokers.
//
// Blnk does not ship that component — it is a custom Kafka authorizer plugin or a protocol-aware
// proxy, with its own lifecycle — so a double is the only way to exercise the integration end to
// end. What it doubles is the CONTROL plane, which is the whole of the contract Blnk owns: the
// authenticated bind-and-attest, the revoke and the health probe. It filters no records, because
// neither does Blnk.
//
// It implements the published contract literally rather than helpfully, and that is deliberate:
// a double that normalised the prefix, or that answered from the request rather than from what it
// had stored, would hide exactly the class of defect these tests exist to catch.
type keyScopeGatewayDouble struct {
	mu sync.Mutex

	// token is the bearer credential the double requires. A request without it is answered 401,
	// which is what proves Blnk authenticates rather than merely posting.
	token string

	// bindings is what the double has been told to enforce, keyed by principal. Attestation
	// answers FROM THIS MAP, so a component that was never told about a prefix cannot confirm
	// one.
	bindings map[string]string

	// enforcing is the component's own answer to "do you enforce key scopes at all?". Setting it
	// false is how the "declared but not enforcing" case is exercised.
	enforcing bool

	// prefixOverride, when non-empty, is echoed INSTEAD of the prefix that was sent. It is the
	// misbehaving-component case that matters most: a proxy that trims, lower-cases or truncates
	// a prefix is enforcing a wider boundary than the registry records.
	prefixOverride string

	// principalOverride, when non-empty, is echoed instead of the principal that was sent — the
	// permissive proxy that answers 200 to everything without reading the body.
	principalOverride string

	// status, when non-zero, is returned in place of 200.
	status int

	// delay stalls every request, for the timeout case.
	delay time.Duration

	// requests counts every method the double received, so a test can assert that revocation
	// actually reached it rather than inferring it from a lack of error.
	requests map[string]int

	// lastAuthorization is the header the double last saw, so a test can assert the scheme and
	// that the token was presented at all.
	lastAuthorization string
}

// newKeyScopeGatewayDouble starts a double and returns it with its URL.
func newKeyScopeGatewayDouble(t *testing.T) (*keyScopeGatewayDouble, string) {
	t.Helper()

	double := &keyScopeGatewayDouble{
		token:     "gateway-token",
		bindings:  map[string]string{},
		enforcing: true,
		requests:  map[string]int{},
	}

	server := httptest.NewServer(double)
	t.Cleanup(server.Close)

	return double, server.URL
}

func (d *keyScopeGatewayDouble) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	delay := d.delay
	token := d.token
	d.requests[r.Method]++
	d.lastAuthorization = r.Header.Get("Authorization")
	d.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if r.Header.Get("Authorization") != "Bearer "+token {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}

	switch r.Method {
	case http.MethodPost:
		d.bind(w, r)
	case http.MethodDelete:
		d.revoke(w, r)
	case http.MethodGet:
		d.health(w)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (d *keyScopeGatewayDouble) bind(w http.ResponseWriter, r *http.Request) {
	var binding KeyScopeBinding
	if err := json.NewDecoder(r.Body).Decode(&binding); err != nil {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	d.mu.Lock()
	d.bindings[binding.Principal] = binding.PartitionKeyPrefix
	answer := keyScopeAttestation{
		KeyScopeEnforced:   d.enforcing,
		Principal:          binding.Principal,
		PartitionKeyPrefix: d.bindings[binding.Principal],
		Detail:             "conformance double",
	}
	if d.prefixOverride != "" {
		answer.PartitionKeyPrefix = d.prefixOverride
	}
	if d.principalOverride != "" {
		answer.Principal = d.principalOverride
	}
	status := d.status
	d.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(answer)
}

func (d *keyScopeGatewayDouble) revoke(w http.ResponseWriter, r *http.Request) {
	principal := r.URL.Query().Get("principal")

	d.mu.Lock()
	_, held := d.bindings[principal]
	delete(d.bindings, principal)
	status := d.status
	d.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)

		return
	}

	if !held {
		// A binding it never held is the state the caller is asking for. The client treats 404
		// as success, and this arm is what proves it.
		w.WriteHeader(http.StatusNotFound)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (d *keyScopeGatewayDouble) health(w http.ResponseWriter) {
	d.mu.Lock()
	answer := keyScopeAttestation{KeyScopeEnforced: d.enforcing}
	status := d.status
	d.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(answer)
}

// heldPrefix reports what the double currently enforces for a principal.
func (d *keyScopeGatewayDouble) heldPrefix(principal string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	prefix, held := d.bindings[principal]

	return prefix, held
}

// requestCount reports how many requests of one method the double received.
func (d *keyScopeGatewayDouble) requestCount(method string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.requests[method]
}

// authorizationHeader reports the last Authorization header the double saw.
func (d *keyScopeGatewayDouble) authorizationHeader() string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.lastAuthorization
}

// keyScopeGatewayConfiguration publishes a configuration declaring the double as the enforcement
// point, and returns it.
//
// The gateway BOOTSTRAP list is deliberately distinct from the broker list, because
// config.KafkaConfig.KeyScopeGateway reads an identical list as no declaration at all — a
// component that IS the brokers cannot be evaluating keys.
func keyScopeGatewayConfiguration(t *testing.T, endpoint, token string) *config.Configuration {
	t.Helper()

	return &config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:                         []string{"kafka-internal:9092"},
			SubscriberBrokers:               []string{"kafka-external:9092"},
			TopicPrefix:                     model.DefaultEventTopicPrefix,
			KeyScopeEnforcement:             config.KeyScopeEnforcementBrokerGateway,
			KeyScopeGatewayBrokers:          []string{"gateway:9093"},
			KeyScopeGatewayAttestationURL:   endpoint,
			KeyScopeGatewayAttestationToken: token,
		},
	}
}

// TestKeyScopeAttestation_IsRequiredForEnforcementToBeActive is the configuration half of SEC-01.
//
// # What was wrong
//
// Enforcement was "active" on the strength of two values: a mode and a bootstrap list distinct
// from the brokers. Both are assertions a deployment makes about itself, and any address
// satisfied them — so a deployment could declare a component that did not exist and Blnk would
// mint a credential declaring an enforced key boundary.
//
// Enforcement now additionally requires a usable CONTROL endpoint, because that is the only part
// of the declaration Blnk can verify. The direction of the change is what matters: an incomplete
// declaration is read as NO declaration, so key-scoped subscribers are refused rather than issued
// something unverified.
func TestKeyScopeAttestation_IsRequiredForEnforcementToBeActive(t *testing.T) {
	base := func() config.KafkaConfig {
		return config.KafkaConfig{
			Brokers:                         []string{"kafka:9092"},
			KeyScopeEnforcement:             config.KeyScopeEnforcementBrokerGateway,
			KeyScopeGatewayBrokers:          []string{"gateway:9093"},
			KeyScopeGatewayAttestationURL:   "https://gateway.internal/key-scopes",
			KeyScopeGatewayAttestationToken: "token",
		}
	}

	t.Run("a complete declaration is active and attestable", func(t *testing.T) {
		kafka := base()

		endpoint, token, attestable := kafka.KeyScopeAttestation()
		require.True(t, attestable, "a complete declaration must be attestable")
		assert.Equal(t, "https://gateway.internal/key-scopes", endpoint)
		assert.Equal(t, "token", token)

		gateway, active := kafka.KeyScopeGateway()
		assert.True(t, active, "and enforcement must then be active")
		assert.Equal(t, []string{"gateway:9093"}, gateway)
	})

	t.Run("no attestation endpoint means no enforcement", func(t *testing.T) {
		kafka := base()
		kafka.KeyScopeGatewayAttestationURL = ""

		_, _, attestable := kafka.KeyScopeAttestation()
		assert.False(t, attestable)

		_, active := kafka.KeyScopeGateway()
		assert.False(t, active,
			"a declared mode and gateway list without a control endpoint must read as NOT enforcing: "+
				"the endpoint is the only part of the declaration Blnk can verify, so without it "+
				"issuance must refuse rather than mint an unverified credential")
	})

	t.Run("no token means no enforcement", func(t *testing.T) {
		kafka := base()
		kafka.KeyScopeGatewayAttestationToken = "   "

		_, _, attestable := kafka.KeyScopeAttestation()
		assert.False(t, attestable,
			"an unauthenticated control endpoint is one anybody who can reach it may rewrite "+
				"bindings on, so it is not an enforcement point")

		_, active := kafka.KeyScopeGateway()
		assert.False(t, active)
	})

	t.Run("plaintext to a remote host is refused, loopback is permitted", func(t *testing.T) {
		refused := map[string]string{
			"plaintext to a remote host": "http://gateway.internal/key-scopes",
			"a scheme Blnk cannot dial":  "ftp://gateway.internal/key-scopes",
			"a schemeless value":         "gateway.internal/key-scopes",
			"a relative path":            "/key-scopes",
			"an empty host":              "https:///key-scopes",
		}
		for name, endpoint := range refused {
			kafka := base()
			kafka.KeyScopeGatewayAttestationURL = endpoint

			_, _, attestable := kafka.KeyScopeAttestation()
			assert.Falsef(t, attestable,
				"%s (%q) must not be attestable: the request carries a bearer credential",
				name, endpoint)
		}

		permitted := []string{
			"http://127.0.0.1:8080/key-scopes",
			"http://localhost:8080/key-scopes",
			"http://[::1]:8080/key-scopes",
		}
		for _, endpoint := range permitted {
			kafka := base()
			kafka.KeyScopeGatewayAttestationURL = endpoint

			_, _, attestable := kafka.KeyScopeAttestation()
			assert.Truef(t, attestable,
				"%q is a loopback host, which is how a sidecar and a test double are reached", endpoint)
		}

		kafka := base()
		kafka.KeyScopeGatewayAttestationURL = "http://localhost.evil.example/key-scopes"
		_, _, attestable := kafka.KeyScopeAttestation()
		assert.False(t, attestable,
			"a host that merely CONTAINS localhost is remote, and plaintext to it would put the "+
				"bearer token on the wire")
	})

	t.Run("the mode gates everything", func(t *testing.T) {
		kafka := base()
		kafka.KeyScopeEnforcement = config.KeyScopeEnforcementNone

		_, _, attestable := kafka.KeyScopeAttestation()
		assert.False(t, attestable,
			"without the mode there is no declared component, whatever endpoint is configured")
	})

	t.Run("the timeout is bounded on both sides", func(t *testing.T) {
		kafka := base()

		assert.Equal(t,
			config.DEFAULT_KEY_SCOPE_ATTESTATION_TIMEOUT_MS*time.Millisecond,
			kafka.KeyScopeAttestationTimeout(),
			"an unset timeout takes the default")

		kafka.KeyScopeGatewayAttestationTimeoutMS = -1
		assert.Equal(t,
			config.DEFAULT_KEY_SCOPE_ATTESTATION_TIMEOUT_MS*time.Millisecond,
			kafka.KeyScopeAttestationTimeout(),
			"a non-positive timeout is not 'unbounded' inside a five-second issuance contract")

		kafka.KeyScopeGatewayAttestationTimeoutMS = 60_000
		assert.Equal(t, config.MaxKeyScopeAttestationTimeout, kafka.KeyScopeAttestationTimeout(),
			"a timeout longer than the issuance budget can never fire, so it is capped")

		kafka.KeyScopeGatewayAttestationTimeoutMS = 750
		assert.Equal(t, 750*time.Millisecond, kafka.KeyScopeAttestationTimeout())
	})
}

// TestKeyScopeGatewayClient_AttestsOnlyTheExactRecordedBinding is the verification half of
// SEC-01, and the prefix comparison is its centre.
//
// A component that answers "yes" to everything, or that returns a normalised form of the prefix,
// is enforcing a DIFFERENT and wider boundary than the registry records — and the credential
// response would describe the registry's. Every arm below is a way that could happen.
func TestKeyScopeGatewayClient_AttestsOnlyTheExactRecordedBinding(t *testing.T) {
	binding := KeyScopeBinding{
		Principal:           "blnk-sub-acme",
		SubscriberID:        "acme",
		PartitionKeyPrefix:  "ldg_9f1c8a72",
		AuthorizedTopics:    []string{"blnk.transactions"},
		ConsumerGroupPrefix: "blnk-sub-acme.",
	}

	t.Run("an exact attestation is accepted and the binding is registered", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		require.NoError(t, client.AttestBinding(context.Background(), binding))

		held, ok := double.heldPrefix(binding.Principal)
		assert.True(t, ok,
			"the POST must REGISTER the binding as well as attest it: a component cannot honestly "+
				"attest a prefix it was never told about")
		assert.Equal(t, binding.PartitionKeyPrefix, held)
		assert.Equal(t, "Bearer "+double.token, double.authorizationHeader(),
			"the control request must authenticate Blnk to the component")
		assert.Equal(t, endpoint, client.Endpoint())
	})

	t.Run("a prefix that differs by one byte is refused", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.prefixOverride = binding.PartitionKeyPrefix + "0"

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, false)
	})

	t.Run("a normalised prefix is refused", func(t *testing.T) {
		// The subtle one, and the reason the comparison is `==` rather than a trimmed or
		// case-insensitive test. A component that upper-cases, pads or truncates is not "close
		// enough": Kafka message keys are bytes, so it would be filtering on a different set of
		// records from the one the registry row and the credential response describe — and in
		// every case below, a WIDER one or a disjoint one.
		for _, override := range []string{
			strings.ToUpper(binding.PartitionKeyPrefix),
			" " + binding.PartitionKeyPrefix,
			binding.PartitionKeyPrefix[:4],
		} {
			double, endpoint := newKeyScopeGatewayDouble(t)
			double.prefixOverride = override

			client, err := NewKeyScopeGatewayClient(
				keyScopeGatewayConfiguration(t, endpoint, double.token),
			)
			require.NoError(t, err)

			err = client.AttestBinding(context.Background(), binding)
			requireKeyScopeUnattested(t, err, false)
		}
	})

	t.Run("a different principal is refused", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.principalOverride = "blnk-sub-somebody-else"

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, false)
	})

	t.Run("key_scope_enforced false is refused", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.enforcing = false

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, false)
	})

	t.Run("a wrong token is refused and is not retryable", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, "wrong-token"))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		// 401 is a 4xx: a decision, not a transient condition, so repeating it changes nothing.
		requireKeyScopeUnattested(t, err, false)
		assert.Empty(t, double.bindings,
			"a component that refused Blnk's credential must not have recorded the binding either")
	})

	t.Run("a 5xx is refused and IS retryable", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.status = http.StatusBadGateway

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, true)
	})

	t.Run("an unreachable component is refused and IS retryable", func(t *testing.T) {
		// A port nothing listens on: the double is started and immediately closed, so the URL is
		// well formed and the dial fails.
		server := httptest.NewServer(http.NotFoundHandler())
		endpoint := server.URL
		server.Close()

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, "token"))
		require.NoError(t, err)

		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, true)
	})

	t.Run("a component that stalls past the timeout is refused", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.delay = 300 * time.Millisecond

		configuration := keyScopeGatewayConfiguration(t, endpoint, double.token)
		configuration.Kafka.KeyScopeGatewayAttestationTimeoutMS = 50

		client, err := NewKeyScopeGatewayClient(configuration)
		require.NoError(t, err)

		start := time.Now()
		err = client.AttestBinding(context.Background(), binding)
		requireKeyScopeUnattested(t, err, true)
		assert.Less(t, time.Since(start), 2*time.Second,
			"the call must be bounded by the attestation timeout, because it runs inside the "+
				"five-second issuance budget")
	})

	t.Run("a binding with no prefix is refused before any request is made", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		unscoped := binding
		unscoped.PartitionKeyPrefix = ""

		require.Error(t, client.AttestBinding(context.Background(), unscoped),
			"a subscriber with no key scope has no binding to attest, and a component echoing two "+
				"empty strings would otherwise satisfy the comparison")
		assert.Zero(t, double.requestCount(http.MethodPost),
			"and nothing must reach the component")
	})
}

// TestKeyScopeGatewayClient_RevokesAndProbes covers the lifecycle halves of the integration:
// withdrawal on deregistration, and the health probe an operator and start-up read.
func TestKeyScopeGatewayClient_RevokesAndProbes(t *testing.T) {
	binding := KeyScopeBinding{
		Principal:          "blnk-sub-acme",
		SubscriberID:       "acme",
		PartitionKeyPrefix: "ldg_9f1c8a72",
	}

	t.Run("revocation withdraws the binding", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		require.NoError(t, client.AttestBinding(context.Background(), binding))
		_, held := double.heldPrefix(binding.Principal)
		require.True(t, held)

		require.NoError(t, client.RevokeBinding(context.Background(), binding.Principal))

		_, held = double.heldPrefix(binding.Principal)
		assert.False(t, held,
			"deregistration must withdraw the binding, or the component keeps a key scope for a "+
				"principal that no longer exists")
		assert.Equal(t, 1, double.requestCount(http.MethodDelete))
	})

	t.Run("a binding the component never held is success", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		assert.NoError(t, client.RevokeBinding(context.Background(), "blnk-sub-never-bound"),
			"404 is the state the caller is asking for, so a retried deregistration must not be "+
				"noisier than the first attempt")
	})

	t.Run("a refused revocation is reported", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.status = http.StatusInternalServerError

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		assert.Error(t, client.RevokeBinding(context.Background(), binding.Principal))
	})

	t.Run("revoking without a principal is refused locally", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		assert.Error(t, client.RevokeBinding(context.Background(), "   "))
		assert.Zero(t, double.requestCount(http.MethodDelete))
	})

	t.Run("the health probe passes on an enforcing component", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		assert.NoError(t, client.Health(context.Background()))
		assert.Equal(t, 1, double.requestCount(http.MethodGet))
	})

	t.Run("the health probe fails when the component is not enforcing", func(t *testing.T) {
		double, endpoint := newKeyScopeGatewayDouble(t)
		double.enforcing = false

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, endpoint, double.token))
		require.NoError(t, err)

		assert.Error(t, client.Health(context.Background()),
			"a component reporting key_scope_enforced=false is not an enforcement point, and an "+
				"operator must learn that at start-up rather than from a subscriber")
	})

	t.Run("the health probe fails on a body that is not the documented shape", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<html>not json</html>"))
		}))
		t.Cleanup(server.Close)

		client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, server.URL, "token"))
		require.NoError(t, err)

		assert.Error(t, client.Health(context.Background()),
			"an endpoint that answers 200 with anything at all is how a URL pointing at the wrong "+
				"service passes for a control plane")
	})
}

// TestKeyScopeGatewayClient_RefusesToFollowARedirect is a small, specific control with a specific
// consequence: the control request carries a long-lived bearer credential, and a client that
// followed a 302 would present it to whatever host the redirect named.
func TestKeyScopeGatewayClient_RefusesToFollowARedirect(t *testing.T) {
	var receivedElsewhere int

	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		receivedElsewhere++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(elsewhere.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	client, err := NewKeyScopeGatewayClient(keyScopeGatewayConfiguration(t, redirector.URL, "token"))
	require.NoError(t, err)

	err = client.AttestBinding(context.Background(), KeyScopeBinding{
		Principal:          "blnk-sub-acme",
		SubscriberID:       "acme",
		PartitionKeyPrefix: "ldg_1",
	})
	requireKeyScopeUnattested(t, err, true)
	assert.Zero(t, receivedElsewhere,
		"the bearer credential must never be presented to a host a redirect named")
}

// TestNewKeyScopeGatewayClient_ReportsAnUndeclaredComponent pins the sentinel callers branch on.
//
// It matters because the two answers lead to different behaviour: "no component declared" is a
// state refusal an operator can act on, while a transport failure against a declared component is
// a different message with a different remedy.
func TestNewKeyScopeGatewayClient_ReportsAnUndeclaredComponent(t *testing.T) {
	_, err := NewKeyScopeGatewayClient(nil)
	assert.ErrorIs(t, err, ErrKeyScopeGatewayNotConfigured,
		"a nil configuration declares nothing")

	_, err = NewKeyScopeGatewayClient(&config.Configuration{})
	assert.ErrorIs(t, err, ErrKeyScopeGatewayNotConfigured,
		"the shipped default declares nothing")

	_, err = NewKeyScopeGatewayClient(&config.Configuration{
		Kafka: config.KafkaConfig{
			KeyScopeEnforcement:    config.KeyScopeEnforcementBrokerGateway,
			KeyScopeGatewayBrokers: []string{"gateway:9093"},
		},
	})
	assert.ErrorIs(t, err, ErrKeyScopeGatewayNotConfigured,
		"a mode and a gateway list without a control endpoint declare a component Blnk cannot ask "+
			"anything, which must not produce a client that appears able to attest")
}

// requireKeyScopeUnattested asserts the typed refusal and its retryability.
//
// The retryable flag is asserted rather than ignored because it is the difference between an
// operator repeating a request and an operator changing something: a component that was briefly
// unreachable will answer next time, and one that holds a different prefix never will.
func requireKeyScopeUnattested(t *testing.T, err error, retryable bool) {
	t.Helper()

	require.Error(t, err)

	var apiErr apierror.APIError
	require.ErrorAs(t, err, &apiErr,
		"every attestation failure must carry the typed code, or the handler answers 500")
	assert.Equal(t, apierror.ErrSubscriberKeyScopeUnattested, apiErr.Code)
	assert.Equal(t, http.StatusConflict, apierror.StatusForCode(apiErr.Code),
		"the code must map to 409: the deployment's enforcement point is state, not a Blnk "+
			"dependency being unavailable")

	detail, ok := apiErr.Details.(SubscriberErrorDetail)
	require.True(t, ok, "the refusal must carry a subscriber error detail, got %T", apiErr.Details)
	assert.Equal(t, retryable, detail.Retryable,
		"the retryable flag is what tells a caller whether repeating the request can help")
}
