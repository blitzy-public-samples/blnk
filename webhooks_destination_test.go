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
package blnk

// This file covers the two controls that keep the legacy HTTP transport from being turned
// against Blnk's own network, and the control that stops it delivering after its own
// retirement:
//
//   - SSRF-01: the redirect refusal, the resolved-address guard, the scheme requirement and
//     the protocol-owned-header protection.
//   - Q4-09: the execution-time sunset gate in ProcessWebhook.
//
// The distinction that matters throughout is between what is CONFIGURED and what is
// REACHED. Several tests below configure a destination that passes every text check and then
// prove the delivery is still refused, because the address it resolves or redirects to is one
// Blnk must not reach. A test that only checked configured URLs would pass against a
// transport with no guard at all.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// destinationTestPayload is a well-formed legacy webhook body.
//
// Well-formed matters: ProcessWebhook unmarshals to validate before delivering, so a
// malformed body would fail for the wrong reason and a destination test would prove nothing
// about destinations.
func destinationTestPayload(t *testing.T) []byte {
	t.Helper()

	body, err := json.Marshal(NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"transaction_id": "txn_destination_guard"},
	})
	require.NoError(t, err)

	return body
}

// storeDestinationConfig publishes a configuration with the given URL and assertion, and
// restores whatever was published before.
//
// Restoration is through t.Cleanup rather than a defer because several tests below use
// subtests, and a defer in the parent would run after the children have already observed the
// mutated store.
func storeDestinationConfig(t *testing.T, url string, allowPrivate bool) *config.Configuration {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	cnf := &config.Configuration{
		Redis:  config.RedisConfig{Dns: "localhost:6379"},
		Server: config.ServerConfig{SecretKey: "destination-guard-signing-secret"},
		Queue: config.QueueConfig{
			WebhookQueue:   fmt.Sprintf("destination_q_%d", time.Now().UnixNano()),
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url:                     url,
				AllowPrivateDestination: allowPrivate,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	return cnf
}

// ---------------------------------------------------------------------------
// The redirect refusal
// ---------------------------------------------------------------------------

// TestLegacyWebhookClient_RefusesToFollowARedirect is the primary CWE-918 proof, and it is
// built the only way that proves anything: with a real second listener that records whether
// it was reached.
//
// Two servers. The first is the configured subscriber and answers 307 — the status that
// PRESERVES the method and body, so a client that followed it would repost the signed ledger
// payload. The second stands in for the internal service an attacker is aiming at. The
// assertion is not merely that an error came back; it is that the second server's counter is
// still zero. An error with a delivered payload would be a leak with a tidy log line.
func TestLegacyWebhookClient_RefusesToFollowARedirect(t *testing.T) {
	var internalHits int
	var mu sync.Mutex

	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		internalHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer internal.Close()

	// Every status a client might follow, including the two that preserve the body.
	for _, status := range []int{
		http.StatusMovedPermanently,  // 301
		http.StatusFound,             // 302
		http.StatusSeeOther,          // 303
		http.StatusTemporaryRedirect, // 307 — preserves method and body
		http.StatusPermanentRedirect, // 308 — preserves method and body
	} {
		t.Run(fmt.Sprintf("%d", status), func(t *testing.T) {
			mu.Lock()
			internalHits = 0
			mu.Unlock()

			subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", internal.URL+"/latest/meta-data/")
				w.WriteHeader(status)
			}))
			defer subscriber.Close()

			storeDestinationConfig(t, subscriber.URL, true)

			err := processHTTPRaw(context.Background(), "", destinationTestPayload(t), initializeHTTPClient())

			require.Error(t, err, "a redirected delivery must fail rather than be followed")
			assert.ErrorIs(t, err, errLegacyWebhookRedirect,
				"the failure must be attributable to the redirect refusal, so a caller can tell "+
					"it apart from a transport error it would handle differently")

			mu.Lock()
			hits := internalHits
			mu.Unlock()
			assert.Zero(t, hits,
				"THE POINT OF THE WHOLE CONTROL: the redirect target must never have been "+
					"contacted. An error returned after the payload was already delivered "+
					"would be a leak with a tidy log line")
		})
	}
}

// TestLegacyWebhookClient_RedirectRefusalIsNotConfigurable pins that no assertion re-enables
// following redirects.
//
// AllowPrivateDestination opens loopback and http. If it also opened redirects — by being
// consulted in CheckRedirect, or by a future author reading it as a general "trust this
// destination" flag — the control would be off in exactly the deployments most likely to set
// it. Asserting both settings is what keeps the two concerns separate.
func TestLegacyWebhookClient_RedirectRefusalIsNotConfigurable(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer subscriber.Close()

	for _, allowPrivate := range []bool{true, false} {
		t.Run(fmt.Sprintf("allow_private_destination=%t", allowPrivate), func(t *testing.T) {
			storeDestinationConfig(t, subscriber.URL, allowPrivate)

			// With the assertion unset the delivery is refused before a request is even built,
			// so the redirect is only reachable with it set. Both arms must fail; the arm that
			// reaches the redirect must fail BECAUSE of it.
			err := processHTTPRaw(context.Background(), "", destinationTestPayload(t), initializeHTTPClient())
			require.Error(t, err)

			if allowPrivate {
				assert.ErrorIs(t, err, errLegacyWebhookRedirect,
					"the assertion opens loopback and http; it must not open redirects")
			} else {
				assert.ErrorIs(t, err, errLegacyWebhookDestination,
					"without the assertion the http loopback destination is refused outright")
			}
		})
	}
}

// TestRefuseLegacyWebhookRedirect_ReportsTheTargetWithoutThePath pins what the hook puts in
// the error, because that string reaches a log line and the URL is third-party text.
func TestRefuseLegacyWebhookRedirect_ReportsTheTargetWithoutThePath(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost,
		"https://evil.example.com/subscriber-42/secret-path", nil)
	require.NoError(t, err)

	hookErr := refuseLegacyWebhookRedirect(request, []*http.Request{{}, {}})

	require.Error(t, hookErr)
	assert.ErrorIs(t, hookErr, errLegacyWebhookRedirect)
	assert.Contains(t, hookErr.Error(), "evil.example.com", "the host is what an operator needs")
	assert.NotContains(t, hookErr.Error(), "secret-path",
		"the path can carry a subscriber's own identifiers and has no business in Blnk's logs")
	assert.Contains(t, hookErr.Error(), "2 hop(s)", "the hop count comes from via")

	t.Run("a nil request does not panic", func(t *testing.T) {
		// CheckRedirect is called by net/http, so a nil request should be impossible — but the
		// hook runs on the delivery path and a panic there would take down a worker.
		assert.ErrorIs(t, refuseLegacyWebhookRedirect(nil, nil), errLegacyWebhookRedirect)
	})
}

// ---------------------------------------------------------------------------
// The resolved-address guard
// ---------------------------------------------------------------------------

// TestGuardLegacyWebhookDial_JudgesTheAddressAboutToBeConnectedTo covers the hook directly.
//
// Calling it directly rather than through a dial is deliberate: the addresses that matter
// most — the metadata endpoint, multicast, the unspecified address — cannot be dialled in a
// test without either reaching something real or hanging. The hook is the whole decision, so
// exercising it directly loses nothing, and TestLegacyWebhookClient_RefusesARebindingHost
// below proves it is actually installed on the client.
func TestGuardLegacyWebhookDial_JudgesTheAddressAboutToBeConnectedTo(t *testing.T) {
	t.Run("refused with no operator assertion", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", false)

		for _, address := range []string{
			"127.0.0.1:443",
			"[::1]:443",
			"169.254.169.254:80",
			"10.0.0.5:5432",
			"192.168.1.9:6379",
			"100.64.0.1:80",
			"0.0.0.0:80",
			"224.0.0.1:80",
			"198.18.0.1:443",
			"[64:ff9b::7f00:1]:443",
		} {
			err := guardLegacyWebhookDial("tcp4", address, nil)
			require.Error(t, err, "%s must not be dialled", address)
			assert.ErrorIs(t, err, errLegacyWebhookDestination)
		}
	})

	t.Run("the operator assertion opens only what an operator can own", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", true)

		for _, address := range []string{"127.0.0.1:8443", "[::1]:8443", "10.0.0.5:443", "172.16.9.9:443"} {
			assert.NoError(t, guardLegacyWebhookDial("tcp4", address, nil),
				"%s is on a network the operator asserted is theirs", address)
		}

		for _, address := range []string{
			"169.254.169.254:80",
			"[fe80::1]:80",
			"0.0.0.0:80",
			"224.0.0.1:80",
			"100.64.0.1:80",
			"198.18.0.1:443",
			"192.0.0.1:443",
			"[64:ff9b::7f00:1]:443",
			"[64:ff9b::a00:5]:443",
		} {
			err := guardLegacyWebhookDial("tcp4", address, nil)
			require.Error(t, err,
				"%s must stay refused WITH the assertion set; the flag is not an off switch", address)
			assert.ErrorIs(t, err, errLegacyWebhookDestination)
		}
	})

	t.Run("public addresses are dialled", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", false)

		for _, address := range []string{"93.184.216.34:443", "[2606:4700:4700::1111]:443"} {
			assert.NoError(t, guardLegacyWebhookDial("tcp", address, nil),
				"%s is publicly routable and must be deliverable", address)
		}
	})

	t.Run("anything it cannot judge is refused", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", true)

		assert.ErrorIs(t, guardLegacyWebhookDial("unix", "/var/run/redis.sock", nil),
			errLegacyWebhookDestination, "a non-TCP network is not a webhook destination")
		assert.ErrorIs(t, guardLegacyWebhookDial("tcp", "not-a-host-port", nil),
			errLegacyWebhookDestination, "an unparseable dial address must fail closed")
		assert.ErrorIs(t, guardLegacyWebhookDial("tcp", "definitely-not-an-ip:443", nil),
			errLegacyWebhookDestination,
			"Control only ever receives resolved literals, so a name here means something is "+
				"wrong and refusing is the only safe answer")
	})

	t.Run("it fails closed when there is no configuration to read", func(t *testing.T) {
		previous := config.ConfigStore.Load()
		t.Cleanup(func() {
			if previous != nil {
				config.ConfigStore.Store(previous)
			}
		})

		// A typed nil is how an absent configuration is expressed here: ConfigStore is an
		// atomic.Value, so storing an untyped nil or a differently-typed value panics rather
		// than reproducing the condition. config.Fetch hands the nil straight back, which is
		// exactly what the guard must not mistake for permission.
		config.ConfigStore.Store((*config.Configuration)(nil))

		require.False(t, legacyWebhookAllowsPrivateDestination(),
			"an absent configuration must answer 'not asserted', not 'asserted'")
		assert.ErrorIs(t, guardLegacyWebhookDial("tcp", "127.0.0.1:8443", nil),
			errLegacyWebhookDestination,
			"so a loopback address is refused rather than permitted by accident")
	})
}

// TestLegacyWebhookClient_RefusesARebindingHost is the DNS-rebinding proof, and the reason
// the guard lives in the dialer rather than in a URL check.
//
// The configured URL is a perfectly ordinary public-looking name that passes every text
// check. Its resolution is what is hostile. Resolving the name to 127.0.0.1 through the
// dialer's own Resolver reproduces exactly that: the text never changes and the answer is
// internal.
//
// It also proves the hook is INSTALLED. Every other guard test calls
// guardLegacyWebhookDial directly, which would keep passing if initializeHTTPClient stopped
// wiring it up.
func TestLegacyWebhookClient_RefusesARebindingHost(t *testing.T) {
	var reached int
	var mu sync.Mutex

	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		reached++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer internal.Close()

	_, port, err := net.SplitHostPort(internal.Listener.Addr().String())
	require.NoError(t, err)

	// The assertion is deliberately NOT set: this is the production posture, in which a public
	// name that answers with loopback must be refused.
	storeDestinationConfig(t, fmt.Sprintf("https://rebound.example.com:%s/blnk", port), false)

	client := initializeHTTPClient()

	// The client's own transport is kept — only name resolution is redirected, which is what a
	// rebinding attack does. Reaching into the transport rather than replacing it is what keeps
	// guardLegacyWebhookDial on the path.
	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "the transport must stay a *http.Transport")

	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		Control: guardLegacyWebhookDial,
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(_ context.Context, _, _ string) (net.Conn, error) {
				return nil, errors.New("resolution is substituted in this test")
			},
		},
	}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		// The hostile answer: whatever was asked for, reply with loopback.
		_, dialPort, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			return nil, splitErr
		}

		return dialer.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", dialPort))
	}

	deliveryErr := processHTTPRaw(context.Background(), "", destinationTestPayload(t), client)

	require.Error(t, deliveryErr, "a name that resolves to loopback must not be delivered to")
	assert.ErrorIs(t, deliveryErr, errLegacyWebhookDestination,
		"the refusal must come from the destination guard, not from a coincidental transport error")

	mu.Lock()
	hits := reached
	mu.Unlock()
	assert.Zero(t, hits,
		"the internal listener must never have been contacted; this is the assertion a URL-text "+
			"check cannot make, because the text was never the problem")
}

// ---------------------------------------------------------------------------
// The URL-text half
// ---------------------------------------------------------------------------

// TestValidateLegacyWebhookDestination_RefusesSchemesAndInternalHosts pins the cheap half of
// the policy — the one that turns a misconfiguration into one clear error instead of a dial
// failure an operator has to interpret.
func TestValidateLegacyWebhookDestination_RefusesSchemesAndInternalHosts(t *testing.T) {
	t.Run("without the operator assertion", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", false)

		assert.NoError(t, validateLegacyWebhookDestination("https://hooks.example.com/blnk"))
		assert.NoError(t, validateLegacyWebhookDestination("https://hooks.example.com:8443/blnk?v=1"))

		for name, raw := range map[string]string{
			"http downgrade":     "http://hooks.example.com/blnk",
			"file scheme":        "file:///etc/passwd",
			"gopher scheme":      "gopher://hooks.example.com/",
			"ftp scheme":         "ftp://hooks.example.com/",
			"no scheme at all":   "hooks.example.com/blnk",
			"no host":            "https:///blnk",
			"loopback literal":   "https://127.0.0.1/blnk",
			"loopback name":      "https://localhost/blnk",
			"metadata literal":   "https://169.254.169.254/latest/meta-data/",
			"private literal":    "https://10.0.0.5/blnk",
			"internal-only name": "https://metadata.google.internal/blnk",
			"unqualified name":   "https://postgres/blnk",
		} {
			err := validateLegacyWebhookDestination(raw)
			require.Error(t, err, "%s must be refused", name)
			assert.ErrorIs(t, err, errLegacyWebhookDestination, name)
		}
	})

	t.Run("with the operator assertion", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", true)

		assert.NoError(t, validateLegacyWebhookDestination("http://127.0.0.1:8080/blnk"),
			"an http loopback sink is the case the assertion exists for")
		assert.NoError(t, validateLegacyWebhookDestination("https://10.0.0.5:8443/blnk"),
			"an on-premise receiver on RFC1918 is a legitimate deployment")
		assert.NoError(t, validateLegacyWebhookDestination("https://webhooks.internal/blnk"),
			"a NAME is admitted so the dial hook can judge what it resolves to; that hook "+
				"refuses link-local no matter what this flag says")

		metadataErr := validateLegacyWebhookDestination("https://169.254.169.254/latest/meta-data/")
		require.Error(t, metadataErr,
			"the metadata endpoint is not a network anybody owns, so no assertion reaches it")
		assert.ErrorIs(t, metadataErr, errLegacyWebhookDestination)

		nat64Err := validateLegacyWebhookDestination("https://[64:ff9b::7f00:1]/blnk")
		require.Error(t, nat64Err, "loopback smuggled through NAT64 stays refused")
		assert.ErrorIs(t, nat64Err, errLegacyWebhookDestination)

		fileErr := validateLegacyWebhookDestination("file:///etc/passwd")
		require.Error(t, fileErr, "no assertion opens a non-HTTP scheme")
		assert.ErrorIs(t, fileErr, errLegacyWebhookDestination)
	})

	t.Run("the error names the host but never the path", func(t *testing.T) {
		storeDestinationConfig(t, "https://hooks.example.com/blnk", false)

		err := validateLegacyWebhookDestination("https://10.0.0.5/tenant-42/secret-token")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "10.0.0.5")
		assert.NotContains(t, err.Error(), "secret-token",
			"a configured URL's path is third-party material and must not be echoed")
	})
}

// TestProcessHTTPRaw_RejudgesTheDestinationOnEveryDelivery pins that the check is not a
// one-time admission check.
//
// A task is enqueued, then the destination becomes unacceptable, then the task runs. That is
// an ordinary sequence — configuration is reloadable and the queue has depth — and the
// delivery must be refused at the moment it happens rather than permitted because it was
// acceptable when it was queued.
func TestProcessHTTPRaw_RejudgesTheDestinationOnEveryDelivery(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	cnf := storeDestinationConfig(t, server.URL, true)
	payload := destinationTestPayload(t)
	client := initializeHTTPClient()

	require.NoError(t, processHTTPRaw(context.Background(), "", payload, client),
		"the acceptable destination must deliver, or the negative below proves nothing")
	require.Len(t, received(), 1)

	// The same process, the same client, a destination that is no longer acceptable.
	cnf.Notification.Webhook.Url = "https://169.254.169.254/latest/meta-data/"
	config.ConfigStore.Store(cnf)

	err := processHTTPRaw(context.Background(), "", payload, client)
	require.Error(t, err, "the destination is re-judged on every delivery, not once at enqueue")
	assert.ErrorIs(t, err, errLegacyWebhookDestination)
	assert.Len(t, received(), 1, "no second delivery reached the original receiver either")
}

// ---------------------------------------------------------------------------
// Protocol-owned headers
// ---------------------------------------------------------------------------

// TestProcessHTTPRaw_ConfiguredHeadersCannotShadowTheSignature is an integrity assertion, not
// a tidiness one.
//
// Configured headers are applied after the signature. Before they were filtered, a header
// named X-Blnk-Signature replaced the real HMAC with a constant — which every
// signature-verifying subscriber would reject, and every subscriber that merely checks the
// header is present would accept forever, including for a forged body.
func TestProcessHTTPRaw_ConfiguredHeadersCannotShadowTheSignature(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	const forged = "0000000000000000000000000000000000000000000000000000000000000000"

	cnf := storeDestinationConfig(t, server.URL, true)
	cnf.Notification.Webhook.Headers = map[string]string{
		"X-Blnk-Signature": forged,
		"x-blnk-timestamp": "1",
		"content-type":     "text/plain",
		"X-Custom-Tenant":  "tenant-42",
	}
	config.ConfigStore.Store(cnf)

	require.NoError(t, processHTTPRaw(context.Background(), "", destinationTestPayload(t), initializeHTTPClient()))

	requests := received()
	require.Len(t, requests, 1)
	headers := requests[0].headers

	assert.NotEqual(t, forged, headers.Get("X-Blnk-Signature"),
		"the configured value must not have replaced the computed HMAC")
	assert.Len(t, headers.Get("X-Blnk-Signature"), 64,
		"a real hex-encoded SHA-256 HMAC is still present")
	assert.NotEqual(t, "1", headers.Get("X-Blnk-Timestamp"),
		"the lower-cased spelling must be caught too; Header.Set canonicalises, so a raw-key "+
			"comparison would have missed this one")
	assert.Equal(t, "application/json", headers.Get("Content-Type"),
		"the body is always JSON and a configured value must not make a correct receiver mis-parse it")
	assert.Equal(t, "tenant-42", headers.Get("X-Custom-Tenant"),
		"headers the transport does not own are still applied; this filter is narrow by design")
}

// ---------------------------------------------------------------------------
// Q4-09: the execution-time sunset gate
// ---------------------------------------------------------------------------

// sunsetGateHarness is a real asynq worker running the real handler.
//
// Nothing here is a stand-in for the worker role. The task is enqueued through the real
// client, routed by a mux registered exactly as initializeWebhookTaskHandlers registers it,
// and executed by a real asynq.Server on its own miniredis. That is what makes the
// post-sunset assertion meaningful: the claim is not "the handler returns nil" but "a task
// already sitting in the queue when the sunset arrives never reaches the subscriber, and
// drains instead of accumulating in the retry set".
type sunsetGateHarness struct {
	blnk      *Blnk
	inspector *asynq.Inspector
	queueName string
	received  func() []receivedWebhook
}

// newSunsetGateHarness builds the worker, pinning the handler's clock to now.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration.
//   - sunset time.Time: the configured sunset date.
//   - now time.Time: the instant the handler evaluates the sunset against.
//
// Returns:
//   - *sunsetGateHarness: the running worker and its observation points.
func newSunsetGateHarness(t *testing.T, sunset, now time.Time) *sunsetGateHarness {
	t.Helper()

	server, received := newWebhookReceiver(http.StatusOK)
	t.Cleanup(server.Close)

	redisServer := miniredis.RunT(t)

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	queueName := fmt.Sprintf("sunset_gate_q_%d", time.Now().UnixNano())
	cnf := &config.Configuration{
		Redis:  config.RedisConfig{Dns: redisServer.Addr()},
		Server: config.ServerConfig{SecretKey: "sunset-gate-signing-secret"},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url:                     server.URL,
				AllowPrivateDestination: true,
			},
		},
		WebhookDeprecationSunsetDate: sunset.Format(time.RFC3339),
	}
	config.ConfigStore.Store(cnf)

	// The parse-warning guard is process-global and would suppress a warning a later test
	// wants to see.
	sunsetParseWarnings.reset()

	asynqClient := asynq.NewClient(asynq.RedisClientOpt{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = asynqClient.Close() })

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = inspector.Close() })

	instance := &Blnk{
		config:      cnf,
		asynqClient: asynqClient,
		httpClient:  initializeHTTPClient(),
		// The handler's clock. Only the clock is injectable — WebhookSunsetPassed remains the
		// one decision point, so this cannot make the handler and the relay disagree about the
		// RULE, only about the hour.
		legacyWebhookNow: func() time.Time { return now },
	}

	// The mux, registered as cmd/workers.go registers it: the task TYPE is the queue name.
	mux := asynq.NewServeMux()
	mux.HandleFunc(queueName, instance.ProcessWebhook)

	worker := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisServer.Addr()},
		asynq.Config{
			Concurrency: 1,
			Queues:      map[string]int{queueName: 1},
			// A retried task must be observable quickly, because "it went to the retry set
			// rather than draining" is one of the outcomes under test.
			RetryDelayFunc: asynq.RetryDelayFunc(func(_ int, _ error, _ *asynq.Task) time.Duration {
				return time.Hour
			}),
		},
	)
	require.NoError(t, worker.Start(mux))
	t.Cleanup(worker.Shutdown)

	return &sunsetGateHarness{
		blnk:      instance,
		inspector: inspector,
		queueName: queueName,
		received:  received,
	}
}

// enqueue puts one legacy delivery on the queue through the production entry point.
func (h *sunsetGateHarness) enqueue(t *testing.T) string {
	t.Helper()

	eventID := gofakeit.UUID()
	require.NoError(t, h.blnk.EnqueueLegacyWebhookDelivery(eventID, destinationTestPayload(t)))

	return eventID
}

// awaitDrained waits until the queue holds no pending, active, retrying or scheduled task.
//
// Polling is unavoidable — the worker is a real server on its own goroutines — but the
// condition is a precise one rather than a sleep: every task has left every actionable state.
func (h *sunsetGateHarness) awaitDrained(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		info, err := h.inspector.GetQueueInfo(h.queueName)
		if err == nil && info.Pending == 0 && info.Active == 0 && info.Retry == 0 && info.Scheduled == 0 {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	info, err := h.inspector.GetQueueInfo(h.queueName)
	require.NoError(t, err)
	t.Fatalf("the queue never drained: pending=%d active=%d retry=%d scheduled=%d archived=%d",
		info.Pending, info.Active, info.Retry, info.Scheduled, info.Archived)
}

// TestProcessWebhook_DeliversWhileTheWindowIsOpen is the control case.
//
// Without it, every post-sunset assertion below could be passing because the harness never
// delivers anything at all.
func TestProcessWebhook_DeliversWhileTheWindowIsOpen(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	harness := newSunsetGateHarness(t, now.Add(15*24*time.Hour), now)

	harness.enqueue(t)
	harness.awaitDrained(t)

	assert.Len(t, harness.received(), 1,
		"inside the window the queued task must be delivered; this is the baseline the "+
			"post-sunset assertions are measured against")

	info, err := harness.inspector.GetQueueInfo(harness.queueName)
	require.NoError(t, err)
	assert.Zero(t, info.Archived, "a successful delivery is not archived")
}

// TestProcessWebhook_DropsAQueuedTaskOnceTheSunsetHasPassed is the Q4-09 proof.
//
// The task is enqueued and the worker executes it with the sunset already behind it — the
// exact shape of the defect: a task that was legitimately queued before the boundary and
// executed after it. Three things are asserted, and all three are necessary:
//
//  1. NO DELIVERY. The subscriber received nothing. Without this the gate does not exist.
//  2. NO RETRY, NO ARCHIVE. The queue drained. A gate that returned an error would send the
//     task through its whole backoff schedule and then archive it, reporting a policy
//     decision as an incident and leaving a post-sunset deployment with a growing archive.
//  3. THE TASK IS GONE. Retirement means the obligation is closed, not deferred.
func TestProcessWebhook_DropsAQueuedTaskOnceTheSunsetHasPassed(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	// One nanosecond past. The sunset instant is the first moment of the post-sunset era, so
	// the boundary is where a mistake would hide; a generous offset would pass even if the
	// comparison were inclusive by accident.
	harness := newSunsetGateHarness(t, now.Add(-time.Nanosecond), now)

	harness.enqueue(t)
	harness.awaitDrained(t)

	assert.Empty(t, harness.received(),
		"THE FINDING: a task already in the queue at the sunset must never be delivered")

	info, err := harness.inspector.GetQueueInfo(harness.queueName)
	require.NoError(t, err)
	assert.Zero(t, info.Retry,
		"a retired task must not be retried; no retry could ever succeed, because the sunset "+
			"only moves forward")
	assert.Zero(t, info.Archived,
		"nor archived: archiving would report a deliberate policy decision as an incident")
	assert.Zero(t, info.Pending, "the queue drains, which is what retirement means")
}

// TestProcessWebhook_SunsetVerdictIsTheSharedDecisionPoint pins that the handler reads the
// same predicate as everything else, rather than a comparison of its own.
//
// Two independent readings of the sunset can disagree — one stops dual-writing while the
// other keeps delivering — and that disagreement is invisible until a subscriber reports
// receiving a webhook from a deployment that has already retired webhooks. Asserting the
// handler's behaviour against WebhookSunsetPassed for the same configuration and clock is
// what forecloses it.
func TestProcessWebhook_SunsetVerdictIsTheSharedDecisionPoint(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

	for name, sunset := range map[string]time.Time{
		"inside the window":       now.Add(24 * time.Hour),
		"exactly at the sunset":   now,
		"one nanosecond past it":  now.Add(-time.Nanosecond),
		"comfortably past it":     now.Add(-30 * 24 * time.Hour),
		"the window not yet open": now.Add(90 * 24 * time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			harness := newSunsetGateHarness(t, sunset, now)

			// The predicate's own answer for this configuration and clock, read directly.
			retired := WebhookSunsetPassed(now)

			harness.enqueue(t)
			harness.awaitDrained(t)

			if retired {
				assert.Empty(t, harness.received(),
					"the predicate says retired, so the handler must deliver nothing")
			} else {
				assert.Len(t, harness.received(), 1,
					"the predicate says the window is open, so the handler must deliver")
			}
		})
	}
}

// TestProcessWebhook_EnqueueTimeCheckIsNotEnoughOnItsOwn states the gap the handler gate
// closes, as an executable claim rather than a comment.
//
// The task is enqueued while the window is open — the relay's own check passes, and the task
// is accepted onto the queue. The sunset then arrives before the worker gets to it. Only an
// execution-time gate can refuse this delivery, because at enqueue time there was nothing to
// refuse.
func TestProcessWebhook_EnqueueTimeCheckIsNotEnoughOnItsOwn(t *testing.T) {
	now := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

	// A window that is open at enqueue time.
	harness := newSunsetGateHarness(t, now.Add(24*time.Hour), now)
	require.False(t, WebhookSunsetPassed(now),
		"the enqueue must happen inside the window, or this test is not about the gap")

	// The queue is PAUSED for the enqueue. That is what makes "the task sat in the queue
	// across the boundary" a controlled fact rather than a race with a live worker: without
	// it the worker can consume the task before the assertion below runs, and the test
	// becomes flaky in exactly the direction that hides the defect.
	require.NoError(t, harness.inspector.PauseQueue(harness.queueName))

	eventID := harness.enqueue(t)

	tasks, err := harness.inspector.ListPendingTasks(harness.queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the task was legitimately accepted onto the queue")
	require.Equal(t, legacyWebhookTaskID(eventID), tasks[0].ID)

	// Time passes, and the sunset arrives while the task is still queued. Advancing the
	// HANDLER's clock is exactly that: the queue is unchanged, and only the moment of
	// execution has moved.
	harness.blnk.legacyWebhookNow = func() time.Time { return now.Add(48 * time.Hour) }

	// The worker is let loose only now, so the task it picks up is one that was accepted
	// before the sunset and executed after it.
	require.NoError(t, harness.inspector.UnpauseQueue(harness.queueName))

	harness.awaitDrained(t)

	assert.Empty(t, harness.received(),
		"a task accepted inside the window and executed outside it must not be delivered; "+
			"the enqueue-time check had nothing to refuse when it ran")
}

// TestRetiredLegacyWebhookLogFields_ReportsWhatItKnowsAndOmitsWhatItDoesNot pins that the
// drop can always be logged.
//
// A drop that panicked or was skipped because the task's identity was unavailable would make
// a post-sunset deployment unable to account for what it retired.
func TestRetiredLegacyWebhookLogFields_ReportsWhatItKnowsAndOmitsWhatItDoesNot(t *testing.T) {
	fields := retiredLegacyWebhookLogFields(context.Background())

	assert.Equal(t, legacyWebhookRetiredAtSunsetReason, fields["reason"],
		"the reason is always present, and it is the same string the relay records in last_error")
	assert.NotContains(t, fields, "task_id",
		"a plain context carries no asynq identity, and an absent field is honest where a zero value would not be")
	assert.NotContains(t, fields, "queue")
	assert.NotContains(t, fields, "retry_count")

	t.Run("a nil context does not panic", func(t *testing.T) {
		//nolint:staticcheck // A nil context is a programming error, but the drop path must
		// still log rather than take down a worker.
		assert.NotEmpty(t, retiredLegacyWebhookLogFields(nil)["reason"])
	})
}

// TestGuardsHoldForEveryDeliveryEntryPoint pins that both entry points are guarded.
//
// processHTTP is the struct-shaped wrapper and processHTTPRaw is the primitive. A guard added
// to one and not the other would leave a live path to the socket, and the wrapper is the one
// existing callers use.
func TestGuardsHoldForEveryDeliveryEntryPoint(t *testing.T) {
	storeDestinationConfig(t, "https://169.254.169.254/latest/meta-data/", true)

	client := initializeHTTPClient()

	rawErr := processHTTPRaw(context.Background(), "", destinationTestPayload(t), client)
	require.Error(t, rawErr)
	assert.ErrorIs(t, rawErr, errLegacyWebhookDestination)

	wrapperErr := processHTTP(context.Background(), NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"transaction_id": "txn_destination_guard"},
	}, client)
	require.Error(t, wrapperErr, "the struct-shaped wrapper must be guarded too")
	assert.ErrorIs(t, wrapperErr, errLegacyWebhookDestination,
		"processHTTP delegates to processHTTPRaw, so the guard cannot be bypassed by choosing "+
			"the other entry point")
}

// ---------------------------------------------------------------------------
// The event identity the receiver deduplicates on
// ---------------------------------------------------------------------------

// TestProcessHTTPRaw_CarriesTheEventIdentityToTheReceiver is the delivery half of the
// at-least-once contract: the transport is at-least-once with a bounded suppression window, so
// the receiver has to be able to deduplicate, and until this header existed it had nothing to
// deduplicate on.
//
// The body cannot carry it. It is the FROZEN legacy envelope — `{"event":…,"data":…}` — whose
// bytes are asserted equal to the bytes published to Kafka, and two deliveries of one event are
// byte-identical, so hashing the body cannot tell a duplicate from a legitimately repeated
// event. A header is the only place the identity fits without breaking the payload-preservation
// guarantee in the migration's final week.
func TestProcessHTTPRaw_CarriesTheEventIdentityToTheReceiver(t *testing.T) {
	const eventID = "0f6e2c8a-1b4d-4e9f-8a7c-2d5b6e3f1a9c"

	t.Run("the identity reaches the receiver and the body is untouched", func(t *testing.T) {
		server, received := newWebhookReceiver(http.StatusOK)
		defer server.Close()

		storeDestinationConfig(t, server.URL, true)
		payload := destinationTestPayload(t)

		require.NoError(t, processHTTPRaw(context.Background(), eventID, payload, initializeHTTPClient()))

		requests := received()
		require.Len(t, requests, 1)

		assert.Equal(t, eventID, requests[0].headers.Get(LegacyWebhookEventIDHeader),
			"the receiver must be given the event id, which is the SAME key the Kafka subscriber "+
				"deduplicates on, so a migrating subscriber keeps one idempotency key rather than two")

		// THE BODY IS BYTE-IDENTICAL. This is the assertion that makes the header safe: the
		// payload-preservation guarantee is that a subscriber's existing parser works unchanged
		// and that these bytes equal the bytes on the Kafka topic, and a member added to the
		// envelope would break both.
		assert.Equal(t, payload, requests[0].body,
			"adding the identity must not disturb a single byte of the body")
	})

	t.Run("the identity is outside the signature, which still verifies over the body", func(t *testing.T) {
		server, received := newWebhookReceiver(http.StatusOK)
		defer server.Close()

		cnf := storeDestinationConfig(t, server.URL, true)
		cnf.Server.SecretKey = "destination-test-signing-secret"
		config.ConfigStore.Store(cnf)

		payload := destinationTestPayload(t)
		require.NoError(t, processHTTPRaw(context.Background(), eventID, payload, initializeHTTPClient()))

		requests := received()
		require.Len(t, requests, 1)
		headers := requests[0].headers

		timestamp := headers.Get("X-Blnk-Timestamp")
		require.NotEmpty(t, timestamp)

		// Recomputed exactly as a receiver does — over timestamp + "." + body, with no header
		// material. The identity is therefore NOT signed, which is a property to state rather
		// than to leave implicit: it is a correlation and deduplication key, and the signature
		// over the body remains the only evidence the delivery came from this deployment.
		mac := hmac.New(sha256.New, []byte("destination-test-signing-secret"))
		_, err := mac.Write([]byte(timestamp + "." + string(requests[0].body)))
		require.NoError(t, err)
		assert.Equal(t, hex.EncodeToString(mac.Sum(nil)), headers.Get("X-Blnk-Signature"),
			"the signature must still verify over the body alone, unaffected by the added header")

		assert.Equal(t, eventID, headers.Get(LegacyWebhookEventIDHeader))
	})

	t.Run("no identity means no header, never an empty one", func(t *testing.T) {
		server, received := newWebhookReceiver(http.StatusOK)
		defer server.Close()

		storeDestinationConfig(t, server.URL, true)

		// SendWebhook's path: a struct, so there is no outbox row and no event id behind it.
		for name, identity := range map[string]string{
			"an absent identity":  "",
			"a blank identity":    "   ",
			"a tab and a newline": "\t\n",
		} {
			require.NoError(t, processHTTPRaw(context.Background(), identity, destinationTestPayload(t), initializeHTTPClient()),
				"%s must still deliver", name)
		}

		requests := received()
		require.Len(t, requests, 3)

		for index, request := range requests {
			values, present := request.headers[textproto.CanonicalMIMEHeaderKey(LegacyWebhookEventIDHeader)]

			// ABSENT, not empty. A receiver keying on an empty value would treat every such
			// delivery as the same event and discard all but the first — silently, and for as
			// long as its idempotency store retained the key.
			assert.Falsef(t, present,
				"delivery %d must omit the header entirely rather than send it blank; got %v",
				index, values)
		}
	})

	t.Run("a configured header cannot forge or erase the identity", func(t *testing.T) {
		server, received := newWebhookReceiver(http.StatusOK)
		defer server.Close()

		cnf := storeDestinationConfig(t, server.URL, true)
		cnf.Notification.Webhook.Headers = map[string]string{
			// Both spellings, because Header.Set canonicalises and a lower-cased key would
			// otherwise reach the same entry through a different comparison.
			"X-Blnk-Event-Id": "forged-identity",
			"x-blnk-event-id": "",
		}
		config.ConfigStore.Store(cnf)

		require.NoError(t, processHTTPRaw(context.Background(), eventID, destinationTestPayload(t), initializeHTTPClient()))

		requests := received()
		require.Len(t, requests, 1)

		// The transport owns this header for the same reason it owns the signature: a receiver
		// deduplicating on it must be able to trust it. A configured constant would collapse
		// every event onto one identity, and the receiver would discard all but the first.
		assert.Equal(t, eventID, requests[0].headers.Get(LegacyWebhookEventIDHeader),
			"a configured value must not replace the identity the transport computed")
		assert.NotEqual(t, "forged-identity", requests[0].headers.Get(LegacyWebhookEventIDHeader))
	})
}

// TestLegacyWebhookEventIDFromContext_YieldsNothingWithoutARelayTaskIdentity covers the read
// side of the arrangement that gets the identity to the delivery.
//
// The identity travels as the asynq TASK ID, because the body is frozen and carries none. Only
// the negative half is reachable from a test: asynq builds the handler context inside an
// internal package with no exported constructor, so a context carrying a task ID cannot be
// constructed outside the library. The positive half is therefore asserted where it can be —
// the namespace round trip in TestLegacyWebhookEventID_IsTheExactInverseOfTheTaskIdentity, the
// header behaviour in the test above, and the WIRING in
// TestProcessWebhook_PassesTheRecoveredIdentityToTheDelivery.
func TestLegacyWebhookEventIDFromContext_YieldsNothingWithoutARelayTaskIdentity(t *testing.T) {
	//nolint:staticcheck // a nil context is exactly the case being asserted
	assert.Empty(t, legacyWebhookEventIDFromContext(nil),
		"a nil context must yield nothing rather than panic: asynq's accessors dereference "+
			"without a nil check, and recovering an identity must never be the reason a worker dies")

	assert.Empty(t, legacyWebhookEventIDFromContext(context.Background()),
		"a context with no task metadata must yield nothing, so a delivery outside the worker "+
			"omits the header rather than sending a fabricated one")
}

// TestProcessWebhook_PassesTheRecoveredIdentityToTheDelivery asserts the WIRING, because no
// test can assert it by running it.
//
// asynq's handler context is built by an internal package with no exported constructor, so a
// test cannot produce a context carrying a task ID and cannot observe the header on the path
// production actually takes. The pieces are each covered — the namespace round trip, the
// context accessor, and the header behaviour given an identity — and this is what proves they
// are connected. Without it, ProcessWebhook could pass the empty string forever and every other
// test in this file would still pass.
func TestProcessWebhook_PassesTheRecoveredIdentityToTheDelivery(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(moduleRootDir(t), "webhooks.go"))
	require.NoError(t, err, "webhooks.go must be readable to assert its wiring")

	assert.Contains(t, string(source),
		"processHTTPRaw(ctx, legacyWebhookEventIDFromContext(ctx), task.Payload(), b.httpClient)",
		"ProcessWebhook must recover the event id from the task identity and hand it to the "+
			"delivery, or the receiver never sees the key it deduplicates on")

	// And the struct path must pass nothing rather than invent something.
	assert.Contains(t, string(source), `processHTTPRaw(ctx, "", payloadBytes, client)`,
		"processHTTP has only a struct and no outbox row behind it, so it must pass no identity")
}
