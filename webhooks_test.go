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

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers the LEGACY HTTP webhook transport, which stays live for the whole
// dual-delivery window; webhooks.go records when it is retired. Two narrow subjects:
//
//  1. THE LEGACY TRANSPORT'S OWN BEHAVIOUR — the enqueue and the pooled HTTP client, both
//     unchanged by the move to Kafka. What changed is only WHO CALLS the transport: the
//     domain post-actions no longer do, the relay's dual-delivery branch does, from a
//     claimed blnk.event_outbox row. That change of caller is asserted by driving the
//     relay's processBatch directly against substituted store and Kafka seams.
//
//  2. PUBLISHER CONSTRUCTION, both with and without Kafka. The construction tests pass a nil
//     datasource to NewBlnk, so what they prove is construction and nothing about delivery:
//     an unconfigured or blank broker list selects the no-op publisher, returns a nil error,
//     dials nothing and blocks on nothing; a CONFIGURED broker list does not select the no-op,
//     and in both cases the pooled HTTP client, its timeouts and the legacy enqueue are
//     unchanged. A failure or a hang here means initializeEventPublisher in blnk.go is wrong,
//     not that an assertion needs relaxing.
//
// Scope deliberately held elsewhere: payload equivalence between the two transports is
// event_dual_delivery_test.go's subject (acceptance criterion V-8), the sunset date
// arithmetic is event_sunset_test.go's, and no Kafka CLIENT is imported here — the
// configured-broker case names a black-holed address precisely so that nothing dials.

func TestSendWebhook(t *testing.T) {
	mr := miniredis.RunT(t)

	// Scoped rather than stored bare. config.ConfigStore is a process-global atomic.Value,
	// so a configuration left installed here surfaces as a failure in an unrelated test —
	// with a webhook URL and queue name that test never mentioned.
	storeLegacyWebhookConfiguration(t, &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: "http://localhost:8080",
			},
		},
	})

	testData := NewWebhook{
		Event:   "transaction.queued",
		Payload: getTransactionMock(10000, false),
	}
	// Closed on cleanup by the helper, so the asynq client's Redis connections do not
	// outlive the test that opened them.
	blnk, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)

	require.NoError(t, blnk.SendWebhook(testData))

	// Verify that the task was enqueued
	assert.NotEmpty(t, mr.Keys(), "the enqueue must have written asynq's keys to Redis")
}

// legacyWebhookConnectionCounter is an httptest server that counts the TCP connections the
// client actually opened to it.
//
// # Why the connection count is measured on the SERVER
//
// The property under test is that the pooled client REUSES a connection, and the honest
// evidence for it is how many connections the receiver saw. The alternative — httptrace on
// the client — cannot be used here without changing production code: processHTTP and
// processHTTPRaw build their own requests and take no context, so there is nowhere to attach
// a ClientTrace without widening a signature the sunset is about to delete.
//
// http.Server's ConnState hook fires once per connection with StateNew, which makes "how many
// connections were opened" an exact integer rather than an inference from remote addresses.
// The previous version of this test counted distinct r.RemoteAddr values, which is why it
// could only ever assert `unique <= total` — a statement that is true of every possible
// outcome, including the zero-reuse one it was written to catch.
type legacyWebhookConnectionCounter struct {
	server      *httptest.Server
	connections atomic.Int64
	requests    atomic.Int64

	mu       sync.Mutex
	bodies   [][]byte
	requestS []*http.Request
}

// newLegacyWebhookConnectionCounter starts a receiver that records every request and counts
// every new connection.
//
// The server is UNSTARTED first, because ConnState has to be installed on the underlying
// http.Server before it begins accepting: setting it after Start races the first connection
// and can miss it, which would understate the count in exactly the direction that makes a
// reuse assertion pass when it should fail.
func newLegacyWebhookConnectionCounter(t *testing.T) *legacyWebhookConnectionCounter {
	t.Helper()

	counter := &legacyWebhookConnectionCounter{}

	counter.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err, "the receiver must be able to read the delivered body")

		counter.mu.Lock()
		counter.bodies = append(counter.bodies, body)
		counter.requestS = append(counter.requestS, r.Clone(context.Background()))
		counter.mu.Unlock()

		counter.requests.Add(1)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))

	counter.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			counter.connections.Add(1)
		}
	}

	counter.server.Start()
	t.Cleanup(counter.server.Close)

	return counter
}

// snapshot returns the bodies and requests received so far.
func (c *legacyWebhookConnectionCounter) snapshot() ([][]byte, []*http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([][]byte(nil), c.bodies...), append([]*http.Request(nil), c.requestS...)
}

// TestConnectionReuse proves the pooled client reuses one connection across sequential
// deliveries.
//
// # What this actually establishes, and why it needed rewriting
//
// The assertion is that N SEQUENTIAL deliveries open exactly ONE connection. That is not a
// statement about performance; it is the only observable consequence of two things
// production depends on:
//
//   - the shared transport's idle pool is used rather than a fresh transport per delivery,
//     which is what initializeHTTPClient's MaxIdleConnsPerHost exists for;
//   - every response body is DRAINED AND CLOSED. Go returns a connection to the idle pool
//     only when the body has been read to EOF and closed. A handler that returned early on a
//     non-2xx status without draining would leak the connection and force a new one — and
//     processHTTPRaw's non-2xx branch is exactly such a candidate.
//
// SEQUENTIAL, not concurrent, and that is the whole reason this can assert anything.
// Concurrent requests may legitimately each open a connection — the pool has nothing idle to
// hand out while every request is in flight — so the previous concurrent version had no
// deterministic expectation available to it and settled for `unique <= total`, which holds
// even when reuse is completely broken. It reported a real failure as a log line beginning
// with a warning emoji, and passed.
//
// The second burst is not a repetition. It asserts the connection survived being IDLE
// between bursts, which is the state a real deployment's client is in almost all the time,
// and it is what an IdleConnTimeout regression would break while a single burst still passed.
func TestConnectionReuse(t *testing.T) {
	receiver := newLegacyWebhookConnectionCounter(t)
	mr := miniredis.RunT(t)

	storeLegacyWebhookConfiguration(t, &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: receiver.server.URL,
				// The receiver is an httptest server, so it is plain http on a loopback
				// address — which validateLegacyWebhookDestination refuses by default,
				// deliberately: ledger data and its HMAC must not cross a network in clear
				// text, and a loopback or link-local destination is the SSRF shape. This is
				// the documented escape hatch for exactly this case, "a destination on a
				// network you own", and it is what every other delivery test sets. Nothing
				// here is about the destination policy; the subject is connection reuse.
				AllowPrivateDestination: true,
			},
		},
	})

	blnk, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)

	const perBurst = 5
	webhook := NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"test": "data"},
	}

	for delivery := 0; delivery < perBurst; delivery++ {
		require.NoError(t, processHTTP(context.Background(), webhook, blnk.httpClient),
			"delivery %d must succeed", delivery+1)
	}

	require.EqualValues(t, perBurst, receiver.requests.Load(),
		"every delivery must have reached the receiver")
	require.EqualValues(t, 1, receiver.connections.Load(),
		"%d sequential deliveries through the pooled client must open exactly ONE connection; "+
			"more means the connection is not being returned to the idle pool — the usual cause "+
			"is a response body that is not drained to EOF and closed", perBurst)

	// A second burst after the first has fully finished: the pool now holds an IDLE
	// connection, and it must be the one that is used.
	for delivery := 0; delivery < perBurst; delivery++ {
		require.NoError(t, processHTTP(context.Background(), webhook, blnk.httpClient))
	}

	assert.EqualValues(t, 2*perBurst, receiver.requests.Load())
	assert.EqualValues(t, 1, receiver.connections.Load(),
		"the idle connection must be reused across bursts as well as within one; a second "+
			"connection here means idle connections are being discarded between deliveries")
}

func TestHTTPClientConfiguration(t *testing.T) {
	mr := miniredis.RunT(t)

	storeLegacyWebhookConfiguration(t, &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
	})

	blnk, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)

	// Verify HTTP client is configured properly
	require.NotNil(t, blnk.httpClient, "HTTP client should be initialized")
	assert.Equal(t, 30*time.Second, blnk.httpClient.Timeout, "Timeout should be 30 seconds")

	// Verify transport configuration
	transport, ok := blnk.httpClient.Transport.(*http.Transport)
	require.True(t, ok, "Transport should be *http.Transport")
	assert.Equal(t, 100, transport.MaxIdleConns, "MaxIdleConns should be 100")
	assert.Equal(t, 10, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should be 10")
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should be 90 seconds")
}

// legacyWebhookCountingTransport wraps a RoundTripper and counts the requests that pass
// through it.
//
// # Why a wrapper is needed to prove which client the handler used
//
// Counting connections at the receiver proves REUSE but cannot identify the CLIENT, and the
// difference matters here. A handler that built `&http.Client{}` per task would still open
// only one connection, because a client with a nil Transport uses the package-global
// http.DefaultTransport — which has an idle pool of its own. So the connection count alone
// cannot distinguish "used the pooled b.httpClient" from "used the default transport", and a
// test that relied on it would pass for the regression it was written to catch.
//
// Wrapping b.httpClient's own transport makes the question directly observable: every request
// that goes through the field increments the counter, and every request that does not is
// invisible to it. Delegating to the real transport rather than answering the request itself
// is what keeps the receiver-side connection assertions meaningful at the same time.
type legacyWebhookCountingTransport struct {
	next     http.RoundTripper
	requests atomic.Int64
}

func (c *legacyWebhookCountingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	c.requests.Add(1)

	return c.next.RoundTrip(request)
}

// TestProcessWebhookWithReusedClient proves the asynq HANDLER delivers through the shared
// pooled client.
//
// # What the previous version could not establish
//
// It called processHTTP directly, passing blnk.httpClient in as an argument, and then
// asserted that blnk.httpClient was still the same pointer it had read a moment earlier. Both
// halves are unfalsifiable: the client under test was supplied by the test rather than chosen
// by the code, and a field nobody writes to cannot change. ProcessWebhook — the function
// named in the test, and the only client-selecting code on this path — was never invoked at
// all, so a regression that gave it http.DefaultClient, or a fresh client per task, would
// have left this test green.
//
// # What it establishes now
//
// Two REAL handler invocations, each with the asynq task shape the relay enqueues, and the
// evidence is taken from the receiver:
//
//   - ONE connection for two deliveries. A per-task client, or http.DefaultClient alongside
//     the pooled one, cannot produce that: each would bring its own idle pool and the second
//     delivery would open a second connection. This is the assertion that actually pins
//     "shares the pooled b.httpClient", which is what ProcessWebhook's documentation claims.
//   - The delivered bodies are the task payloads BYTE FOR BYTE, and each carries the
//     signature headers. That is what says the handler went through processHTTPRaw with the
//     configured secret rather than re-marshalling or posting something else.
func TestProcessWebhookWithReusedClient(t *testing.T) {
	receiver := newLegacyWebhookConnectionCounter(t)
	mr := miniredis.RunT(t)

	storeLegacyWebhookConfiguration(t, &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Server: config.ServerConfig{
			SecretKey: "process-webhook-shared-client-secret",
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: receiver.server.URL,
				// The receiver is an httptest server, so it is plain http on a loopback
				// address — which validateLegacyWebhookDestination refuses by default,
				// deliberately: ledger data and its HMAC must not cross a network in clear
				// text, and a loopback or link-local destination is the SSRF shape. This is
				// the documented escape hatch for exactly this case, "a destination on a
				// network you own", and it is what every other delivery test sets. Nothing
				// here is about the destination policy; the subject is connection reuse.
				AllowPrivateDestination: true,
			},
		},
	})

	blnk, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)

	clientBefore := blnk.httpClient

	// Instrument the POOLED transport in place, delegating to the real one. Anything the
	// handler sends through b.httpClient is counted here; anything it sends through a client
	// of its own is not.
	pooled, ok := clientBefore.Transport.(*http.Transport)
	require.True(t, ok, "the pooled client must carry the configured *http.Transport")
	counting := &legacyWebhookCountingTransport{next: pooled}
	clientBefore.Transport = counting
	t.Cleanup(func() { clientBefore.Transport = pooled })

	// The task shape the relay's dual-delivery branch enqueues: the task type is the
	// configured queue name and the payload is the legacy body's bytes.
	firstBody, err := json.Marshal(NewWebhook{Event: "test.event1", Payload: map[string]string{"id": "1"}})
	require.NoError(t, err)
	secondBody, err := json.Marshal(NewWebhook{Event: "test.event2", Payload: map[string]string{"id": "2"}})
	require.NoError(t, err)

	require.NoError(t, blnk.ProcessWebhook(context.Background(), asynq.NewTask("webhook_queue", firstBody)))
	require.NoError(t, blnk.ProcessWebhook(context.Background(), asynq.NewTask("webhook_queue", secondBody)))

	require.EqualValues(t, 2, receiver.requests.Load(), "both handler calls must have delivered")
	require.EqualValues(t, 2, counting.requests.Load(),
		"both deliveries must have gone through b.httpClient: a handler that constructed its "+
			"own client per task, or reached for http.DefaultClient, would bypass this transport "+
			"entirely and leave the count at zero while still delivering")
	assert.EqualValues(t, 1, receiver.connections.Load(),
		"two ProcessWebhook calls must share ONE connection: a handler that built its own "+
			"client per task, or used http.DefaultClient, would bring a second idle pool and "+
			"open a second connection")

	bodies, requests := receiver.snapshot()
	require.Len(t, bodies, 2)
	assert.Equal(t, string(firstBody), string(bodies[0]),
		"the handler must deliver the task payload verbatim, not a re-marshalling of it")
	assert.Equal(t, string(secondBody), string(bodies[1]))

	for index, request := range requests {
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"),
			"delivery %d must be sent as JSON", index+1)
		assert.NotEmpty(t, request.Header.Get("X-Blnk-Signature"),
			"delivery %d must be signed with the configured secret, which is what proves it "+
				"went through processHTTPRaw rather than some other request path", index+1)
		assert.NotEmpty(t, request.Header.Get("X-Blnk-Timestamp"),
			"delivery %d must carry the timestamp the signature covers", index+1)
	}

	assert.Same(t, clientBefore, blnk.httpClient,
		"the pooled client must not be replaced by handling a task; it is constructed once "+
			"and shared for the process's lifetime")
}

// ===========================================================================
// ADDITIONS FOR THE DUAL-DELIVERY WINDOW
// ===========================================================================

// legacyWebhookConstructionBudget bounds how long NewBlnk may take in this file. The bound IS
// the assertion: "publisher construction performs no I/O" is only observable as "construction
// did not wait", and a constructor that dialled a broker or fetched metadata could not finish
// inside two seconds against an unroutable address.
const legacyWebhookConstructionBudget = 2 * time.Second

// legacyWebhookBlackholeBroker is an RFC 1918 private address routed nowhere in a default
// environment, so a construction that dialled it would stall past
// legacyWebhookConstructionBudget. Passing the budget with it configured is the evidence of
// the ABSENCE of a dial.
//
// Private space rather than the RFC 5737 TEST-NET-2 address this used to be: TEST-NET is
// PUBLIC, and acknowledged plaintext no longer reaches a broker outside Blnk's own network
// (requireLocalBrokersForPlaintext now verifies KAFKA_INSECURE_LOCAL_DEV's claim instead of
// only warning). The unroutability the probe depends on is unchanged.
const legacyWebhookBlackholeBroker = "10.255.255.1:9092"

// legacyWebhookNeverCalledURL is configured but never contacted: the enqueue paths only check
// that the URL is non-empty, and no worker runs here. Port 1 is privileged and unbound, so an
// accidental delivery attempt fails loudly instead of reaching something real.
const legacyWebhookNeverCalledURL = "http://127.0.0.1:1/never-called"

// legacyWebhookQueueName is deliberately neither "webhook_queue" nor any default: the invariant
// is that the task's type and queue are both the CONFIGURED Queue.WebhookQueue, and a
// recognisable name would let a hardcoded default satisfy the assertion anyway.
const legacyWebhookQueueName = "legacy_webhook_dual_delivery_q"

// legacyWebhookRelayFixedNow pins the relay's clock so the claim instant is deterministic.
const legacyWebhookRelayFixedNow = "2026-03-01T12:00:00Z"

// legacyWebhookRelayClaimToken is the token the fake store stamps on a claim. Every relay
// transition is conditional on it, so asserting the token a transition presented proves the
// transition belonged to the claim that produced the row rather than to a stale worker.
const legacyWebhookRelayClaimToken = "legacy-webhook-claim-token"

// storeLegacyWebhookConfiguration publishes cnf for the duration of one test and restores
// whatever was there before.
//
// config.ConfigStore is a process-global atomic.Value, so a configuration left behind by one
// test produces failures in unrelated ones that look nothing like their cause. EVERY test in
// this file goes through this helper, including the four legacy ones above, which used to
// store bare and leave a webhook URL and a queue name installed for whatever ran next.
//
// It writes DIRECTLY rather than through config.MockConfig because MockConfig runs
// validateAndAddDefaults, which silently refuses a configuration with no data-source DSN and
// supplies the very Kafka defaults some cases here need to observe as absent.
func storeLegacyWebhookConfiguration(t *testing.T, cnf *config.Configuration) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(cnf)
}

// legacyWebhookConfiguration returns this file's shape: a Redis DSN, the webhook queue, a
// configured webhook URL, and NO Kafka block. The absent Kafka block is the point — it is the
// shape of a deployment that does not run Kafka. An empty webhookURL disables the transport.
func legacyWebhookConfiguration(redisDSN, webhookURL string) *config.Configuration {
	return &config.Configuration{
		Redis: config.RedisConfig{
			Dns: redisDSN,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   legacyWebhookQueueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: webhookURL,
			},
		},
	}
}

// newBlnkWithinLegacyWebhookBudget constructs a Blnk instance with a nil datasource and fails
// the test if the constructor has not returned within legacyWebhookConstructionBudget. Running
// it on its own goroutine turns "NewBlnk blocked" from a hang consuming the package's whole
// test timeout into one failing test naming the cause. The instance is closed on cleanup.
func newBlnkWithinLegacyWebhookBudget(t *testing.T) (*Blnk, error) {
	t.Helper()

	type construction struct {
		instance *Blnk
		err      error
	}

	// Buffered, so the constructor goroutine can always deliver its result and exit even
	// when nobody is left waiting for it.
	done := make(chan construction, 1)

	go func() {
		instance, err := NewBlnk(nil)
		done <- construction{instance: instance, err: err}
	}()

	select {
	case result := <-done:
		if result.instance != nil {
			t.Cleanup(func() {
				assert.NoError(t, result.instance.Close(), "closing the Blnk instance must not fail")
			})
		}

		return result.instance, result.err
	case <-time.After(legacyWebhookConstructionBudget):
		// The budget was missed, so this test has failed — but the constructor goroutine is
		// still running, and if it later succeeds it hands back an instance holding an asynq
		// client and a Redis connection that nobody will ever close. Draining it on cleanup
		// releases those, bounded so that a genuinely BLOCKED constructor cannot hold the
		// package's teardown open: leaving the goroutine parked on a full-lifetime block is
		// unavoidable in that case, and it is reported rather than waited on for ever.
		t.Cleanup(func() {
			select {
			case late := <-done:
				if late.instance != nil {
					_ = late.instance.Close()
				}
			case <-time.After(legacyWebhookConstructionBudget):
				t.Logf(
					"the NewBlnk goroutine had still not returned %s after the budget expired, so "+
						"whatever it holds could not be released; it is blocked rather than merely slow",
					legacyWebhookConstructionBudget,
				)
			}
		})

		t.Fatalf(
			"NewBlnk did not return within %s; publisher construction must dial nothing and "+
				"block on nothing, or every process start and every test here depends on a "+
				"reachable broker",
			legacyWebhookConstructionBudget,
		)

		return nil, nil
	}
}

// TestNewBlnk_NoKafkaConfiguredUsesNoOpPublisher covers CONSTRUCTION, not delivery: with no
// brokers, publisher construction selects the no-op implementation, returns a nil error, and
// reaches neither a resolver nor a socket, while the legacy HTTP client is left intact. It says
// nothing about whether an event reaches a subscriber — no relay and no worker run here.
//
// The legacy tests above run with no Kafka configuration, and the ones that build a service
// container do it through NewBlnk(nil), so they depend silently on this behaviour; were
// construction to fail or dial, they would break for a reason unrelated to webhooks and the fix
// would be in initializeEventPublisher (blnk.go). The cases below are therefore named after the
// tests whose CONFIGURATION they reproduce rather than after the call they make; blnk_test.go
// states the same property generally over its own fixtures.
//
// The blank-broker case matters: treated as an address rather than as absence, "   " would
// produce a Kafka publisher that can never connect while nothing in the configuration looked
// wrong.
func TestNewBlnk_NoKafkaConfiguredUsesNoOpPublisher(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		build   func(redisDSN string) *config.Configuration
		brokers []string
	}{
		{
			name: "only a Redis DSN, as TestHTTPClientConfiguration constructs it",
			build: func(redisDSN string) *config.Configuration {
				return &config.Configuration{Redis: config.RedisConfig{Dns: redisDSN}}
			},
		},
		{
			name: "the full legacy webhook configuration, as TestSendWebhook constructs it",
			build: func(redisDSN string) *config.Configuration {
				return legacyWebhookConfiguration(redisDSN, legacyWebhookNeverCalledURL)
			},
		},
		{
			name: "a webhook configuration whose broker list holds nothing but blank separators",
			build: func(redisDSN string) *config.Configuration {
				return legacyWebhookConfiguration(redisDSN, legacyWebhookNeverCalledURL)
			},
			brokers: []string{"", "   ", "\t"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			redisServer := miniredis.RunT(t)

			cnf := testCase.build(redisServer.Addr())
			cnf.Kafka.Brokers = testCase.brokers
			storeLegacyWebhookConfiguration(t, cnf)

			instance, err := newBlnkWithinLegacyWebhookBudget(t)
			require.NoError(t, err,
				"an unconfigured broker list must never fail construction; the legacy webhook "+
					"tests in this file and every deployment without Kafka depend on it")
			require.NotNil(t, instance)

			require.NotNil(t, instance.events,
				"the publisher handle must always be populated so Close and the relay never nil-check it")
			assert.True(t, IsNoopEventPublisher(instance.events),
				"no brokers must select the no-op publisher")
			assert.IsType(t, &NoopEventPublisher{}, instance.events,
				"the no-op is the only implementation that provably performs no I/O, which is what "+
					"makes constructing a Blnk instance in this file safe")

			assert.NoError(t, instance.events.Publish(context.Background(), model.LedgerEvent{}),
				"the no-op accepts an event and reports success, exactly as SendWebhook returns nil "+
					"when no webhook URL is configured")

			// The legacy transport must be untouched by the event wiring. Asserting it here is
			// what makes this a regression gate for the four tests above rather than a
			// restatement of the publisher's own unit tests.
			require.NotNil(t, instance.httpClient,
				"the legacy HTTP transport must survive the dual-delivery window")
			assert.Equal(t, 30*time.Second, instance.httpClient.Timeout,
				"the legacy client's timeout is TestHTTPClientConfiguration's subject and must not move")
		})
	}
}

// TestNewBlnk_KafkaConfiguredKeepsTheLegacyTransport is the other half of the construction
// gate: without it, a publisher wired to return the no-op UNCONDITIONALLY would pass every
// assertion in this file, start every process without complaint and publish nothing. So this
// case configures brokers and requires that the no-op is not selected.
//
// What it asserts beyond that is still construction-scoped: with Kafka configured, the pooled
// HTTP client and its timeouts are unchanged and the legacy enqueue still reaches Redis. It does
// not assert that either transport delivers.
//
// No SASL credentials are set and InsecureLocalDev acknowledges the resulting plaintext
// transport: nothing is dialled here, so there is no credential to protect.
func TestNewBlnk_KafkaConfiguredKeepsTheLegacyTransport(t *testing.T) {
	redisServer := miniredis.RunT(t)

	cnf := legacyWebhookConfiguration(redisServer.Addr(), legacyWebhookNeverCalledURL)
	cnf.Kafka = config.KafkaConfig{
		Brokers:          []string{legacyWebhookBlackholeBroker},
		TopicPrefix:      DefaultTopicPrefix,
		InsecureLocalDev: true,
	}
	storeLegacyWebhookConfiguration(t, cnf)

	instance, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err,
		"a configured broker list must build a publisher without contacting the broker")
	require.NotNil(t, instance)

	require.NotNil(t, instance.events)
	assert.False(t, IsNoopEventPublisher(instance.events),
		"a configured broker list must select the Kafka publisher; if this were the no-op the "+
			"assertions in the sibling test would be vacuous and a deployment that believed it "+
			"was publishing events would emit none")

	// The legacy transport, unchanged, while Kafka is configured.
	require.NotNil(t, instance.httpClient)
	assert.Equal(t, 30*time.Second, instance.httpClient.Timeout)

	transport, isHTTPTransport := instance.httpClient.Transport.(*http.Transport)
	require.True(t, isHTTPTransport, "the legacy client must keep its configured transport")
	assert.Equal(t, 100, transport.MaxIdleConns)
	assert.Equal(t, 10, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout)

	require.NoError(t, instance.SendWebhook(NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"transaction_id": "txn_kafka_configured"},
	}), "the legacy enqueue must keep working while Kafka is configured — both transports "+
		"are live together for the whole dual-delivery window")

	assert.NotEmpty(t, redisServer.Keys(),
		"the legacy task must still reach Redis; configuring Kafka must not silently disable "+
			"the transport that is still delivering")
}

// legacyWebhookRelayMark records one claim-token-conditional state transition.
type legacyWebhookRelayMark struct {
	id         int64
	claimToken string

	// record is the broker coordinate the transition was given. Recorded so a test can
	// assert that dual delivery persists the SAME coordinate the Kafka leg reported, which
	// is what keeps the zero-loss reconciliation able to account for a dual-delivered row.
	record model.BrokerRecord
}

// legacyWebhookRelayStore is the eventRelayStore seam, narrowed to what driving one batch needs.
//
// The relay runs against this rather than PostgreSQL because the subject here is THE LEGACY
// TRANSPORT AND ITS CALLER, not the outbox repository, whose claim semantics, FIFO ordering and
// skip-locked concurrency are database/event_outbox_test.go's subject. What is real is the legacy
// leg: processor.legacy is the actual *Blnk, so the enqueue under assertion is the production one
// reaching a real asynq client and a real Redis.
//
// Every field is mutex-guarded and read through a snapshot accessor because processBatch
// publishes partition-key groups on concurrent goroutines.
type legacyWebhookRelayStore struct {
	mu sync.Mutex

	// pending is the claimable set. It is drained by the first claim, so a second poll in
	// the same test finds nothing and cannot re-enqueue a webhook for a row already done.
	pending []model.EventOutbox

	claims       int
	dispatched   []legacyWebhookRelayMark
	webhookMarks []legacyWebhookRelayMark
	failures     []string
	// webhookPendings records the reasons MarkEventWebhookPending was called with — the
	// transition that keeps an outstanding legacy leg claimable instead of losing it behind
	// a terminal state. Nothing in this file should reach it, because the enqueue here is
	// the production one against a real Redis, so recording it is what lets a test assert
	// its ABSENCE rather than infer success.
	webhookPendings []string
}

// Compile-time proof that the fake is a faithful subset of the seam the relay drives.
var _ eventRelayStore = (*legacyWebhookRelayStore)(nil)

// ClaimPendingEventOutbox hands out the seeded rows once, stamping each with the processing
// status, the batch's claim token and a lease, exactly as the repository does.
func (s *legacyWebhookRelayStore) ClaimPendingEventOutbox(
	_ context.Context,
	_ int,
	lockDuration time.Duration,
) ([]model.EventOutbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.claims++

	if len(s.pending) == 0 {
		return nil, nil
	}

	lease := legacyWebhookRelayNow().Add(lockDuration)

	claimed := make([]model.EventOutbox, 0, len(s.pending))

	for _, row := range s.pending {
		row.Status = model.EventOutboxStatusProcessing
		row.ClaimToken = legacyWebhookRelayClaimToken
		row.LockedUntil = &lease
		claimed = append(claimed, row)
	}

	s.pending = nil

	return claimed, nil
}

// MarkEventDispatched records the Kafka leg's success terminal transition.
func (s *legacyWebhookRelayStore) MarkEventDispatched(
	_ context.Context,
	id int64,
	claimToken string,
	record model.BrokerRecord,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, legacyWebhookRelayMark{
		id: id, claimToken: claimToken, record: record,
	})

	return nil
}

// MarkEventFailed records a failed publish attempt and reports that budget remains. Nothing here
// should reach it, so the recording lets a test assert the ABSENCE of failures rather than infer
// success from a dispatch.
//
// The trailing lease is the dead-letter hand-off window the exhaustion arm holds the row under.
// It is ignored here because this double never takes that arm — every recorded call is a
// retryable failure that keeps budget — and a test asserting the absence of failures does not
// need the lease to prove it.
func (s *legacyWebhookRelayStore) MarkEventFailed(
	_ context.Context,
	_ int64,
	_ string,
	errMsg string,
	_ time.Duration,
	_ bool,
	_ time.Duration,
) (model.EventFailureOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures = append(s.failures, errMsg)

	return model.EventFailureOutcome{
		Status:   model.EventOutboxStatusPending,
		Attempts: 1,
	}, nil
}

// MarkEventPermanentlyFailed completes the relay's store contract.
//
// As with MarkEventFailed, nothing in this file should reach it — the publisher adopted here
// accepts every event — so the reason is recorded alongside the budget-driven failures, which
// lets a test assert the ABSENCE of any failure transition rather than infer success.
func (s *legacyWebhookRelayStore) MarkEventPermanentlyFailed(
	_ context.Context,
	_ int64,
	_ string,
	errMsg string,
	_ time.Duration,
) (model.EventFailureOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures = append(s.failures, errMsg)

	return model.EventFailureOutcome{
		Status:    model.EventOutboxStatusFailed,
		Attempts:  1,
		Exhausted: true,
	}, nil
}

// MarkWebhookDispatched records the legacy leg of the dual-delivery window. This method,
// and every assertion on it below, disappears at the sunset with the branch that calls it.
func (s *legacyWebhookRelayStore) MarkWebhookDispatched(_ context.Context, id int64, claimToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.webhookMarks = append(s.webhookMarks, legacyWebhookRelayMark{id: id, claimToken: claimToken})

	return nil
}

// MarkEventWebhookPending records an outstanding legacy leg and reports that another webhook
// attempt is owed. SUNSET: goes with the leg it serves.
func (s *legacyWebhookRelayStore) MarkEventWebhookPending(
	_ context.Context,
	_ int64,
	_ string,
	errMsg string,
	_ time.Duration,
	_ model.BrokerRecord,
) (model.EventWebhookOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.webhookPendings = append(s.webhookPendings, errMsg)

	return model.EventWebhookOutcome{
		Status:          model.EventOutboxStatusWebhookPending,
		WebhookAttempts: 1,
	}, nil
}

// snapshotWebhookPendings copies the recorded outstanding-leg transitions.
func (s *legacyWebhookRelayStore) snapshotWebhookPendings() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.webhookPendings...)
}

// RenewEventOutboxLease is a no-op that reports one row still in flight.
//
// The relay renews the lease of a batch it is holding, and these tests seed exactly one row
// per batch. Reporting one keeps the relay's heartbeat alive for the life of the batch, which
// is what production does; reporting zero would retire the heartbeat early and prove nothing
// either way.
func (s *legacyWebhookRelayStore) RenewEventOutboxLease(
	_ context.Context,
	_ string,
	_ time.Duration,
) (int64, error) {
	return 1, nil
}

// ClaimFailedEventOutboxForDeadLetter claims nothing.
//
// The repair pass runs on every tick, and this file seeds no rows awaiting preservation, so an
// empty result is the honest answer. Returning rows here would drag the dead-letter writer into
// tests whose subject is the legacy transport.
func (s *legacyWebhookRelayStore) ClaimFailedEventOutboxForDeadLetter(
	_ context.Context,
	_ int,
	_ time.Duration,
) ([]model.EventOutbox, error) {
	return nil, nil
}

// ClaimPendingWebhookDeliveries claims nothing, for the same reason: this file seeds no rows
// whose legacy leg was left owed by an earlier pass. The relay's recovery pass runs every tick,
// so an empty result is the honest answer and keeps these tests' subject the INLINE enqueue.
//
// SUNSET: goes with the leg it serves.
func (s *legacyWebhookRelayStore) ClaimPendingWebhookDeliveries(
	_ context.Context,
	_ int,
	_ time.Duration,
) ([]model.EventOutbox, error) {
	return nil, nil
}

// MarkEventLegacyWebhookAttempted records a recovery-path enqueue failure. Nothing here should
// reach it — the recovery claim above returns nothing — so the recording lets a test assert its
// ABSENCE rather than infer it.
//
// SUNSET: goes with the leg it serves.
func (s *legacyWebhookRelayStore) MarkEventLegacyWebhookAttempted(
	_ context.Context,
	_ int64,
	_ string,
	_ time.Duration,
) (model.EventWebhookOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.webhookPendings = append(s.webhookPendings, "legacy webhook recovery attempt recorded")

	return model.EventWebhookOutcome{
		Status:          model.EventOutboxStatusDispatched,
		WebhookAttempts: 1,
	}, nil
}

// claimCount reports how many claims were served.
func (s *legacyWebhookRelayStore) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.claims
}

// snapshotDispatched copies the recorded Kafka-leg transitions.
func (s *legacyWebhookRelayStore) snapshotDispatched() []legacyWebhookRelayMark {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]legacyWebhookRelayMark(nil), s.dispatched...)
}

// snapshotWebhookMarks copies the recorded legacy-leg transitions.
func (s *legacyWebhookRelayStore) snapshotWebhookMarks() []legacyWebhookRelayMark {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]legacyWebhookRelayMark(nil), s.webhookMarks...)
}

// snapshotFailures copies the recorded publish failures.
func (s *legacyWebhookRelayStore) snapshotFailures() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.failures...)
}

// legacyWebhookRelayNow is the pinned relay clock, parsed from legacyWebhookRelayFixedNow. It is
// a function rather than a variable so the constant stays the single written form of the instant.
func legacyWebhookRelayNow() time.Time {
	parsed, err := time.Parse(time.RFC3339, legacyWebhookRelayFixedNow)
	if err != nil {
		panic("legacyWebhookRelayFixedNow must be a valid RFC3339 instant: " + err.Error())
	}

	return parsed
}

// legacyWebhookOutboxRow builds one pending blnk.event_outbox row whose payload is a REAL legacy
// webhook body: the marshaled NewWebhook envelope, both keys included, because that is what the
// capture path stores and what the relay hands to the legacy transport verbatim. Building it any
// other way would make the provenance assertion compare bytes production never produces.
func legacyWebhookOutboxRow(t *testing.T, eventID, eventType string) model.EventOutbox {
	t.Helper()

	body, err := json.Marshal(NewWebhook{
		Event: eventType,
		Payload: map[string]interface{}{
			"transaction_id": eventID,
			"status":         "APPLIED",
			"amount":         1250,
		},
	})
	require.NoError(t, err, "the legacy webhook envelope must marshal")

	occurred := legacyWebhookRelayNow().Add(-time.Minute)

	return model.EventOutbox{
		ID:            1,
		EventID:       eventID,
		EventType:     eventType,
		AggregateID:   "ldg_legacy_webhook_dual_delivery",
		PartitionKey:  "ldg_legacy_webhook_dual_delivery",
		Topic:         TopicForEvent(eventType),
		SchemaVersion: model.SchemaVersionV1,
		Payload:       body,
		OccurredAt:    occurred,
		Status:        model.EventOutboxStatusPending,
		MaxAttempts:   5,
		NextAttemptAt: occurred,
	}
}

// legacyWebhookRelay builds a relay whose LEGACY LEG IS THE REAL instance and whose store is the
// fake above, with the clock and the sunset answer pinned. sunsetPassed false is inside the window.
//
// The three assertions inside it are the ones that make everything after it meaningful, so
// they are made here once rather than repeated in each test:
//
//   - processor.legacy is the very *Blnk that was passed in. NewEventRelayProcessor wires it,
//     which is what proves the branch under test calls production code — the same
//     EnqueueLegacyWebhookDelivery a deployed relay calls — rather than a test double.
//   - processor.publisher was adopted and is the no-op. With no brokers configured the Kafka
//     leg accepts every event and performs no I/O, which is what lets this file assert the
//     relay's behaviour without a broker and without importing a Kafka client. It is also
//     why MarkEventDispatched being recorded is a sound proxy for "the Kafka leg completed".
//   - the dual-delivery decision is a pinned function. WebhookDualDeliveryActive reads both
//     ends of the configured window, and event_sunset.go owns that arithmetic; pinning the
//     ANSWER is what makes these tests about the branch rather than about date parsing, and it
//     keeps them independent of whatever deprecation window the ambient environment carries.
//
// Parameters:
//   - t *testing.T: the test.
//   - instance *Blnk: the live instance, used as the legacy transport.
//   - sunsetPassed bool: the pinned sunset answer; false is inside the window.
//   - rows ...model.EventOutbox: the rows to seed as claimable.
//
// Returns:
//   - *EventRelayProcessor: the processor, ready for processBatch.
//   - *legacyWebhookRelayStore: the store, for the transition assertions.
//   - *relayFakePublisher: the Kafka leg, for asserting it ran independently of the legacy one.
func legacyWebhookRelay(
	t *testing.T,
	instance *Blnk,
	sunsetPassed bool,
	rows ...model.EventOutbox,
) (*EventRelayProcessor, *legacyWebhookRelayStore, *relayFakePublisher) {
	t.Helper()

	store := &legacyWebhookRelayStore{pending: append([]model.EventOutbox(nil), rows...)}

	processor := NewEventRelayProcessor(instance)
	require.NotNil(t, processor, "the relay constructor must never return nil")

	legacyTransport, isInstance := processor.legacy.(*Blnk)
	require.True(t, isInstance,
		"the relay's legacy leg must be the Blnk instance itself; that is what makes this test "+
			"exercise the production enqueue rather than a double")
	require.Same(t, instance, legacyTransport,
		"the relay must hold the very instance it was constructed from, so the asynq client the "+
			"enqueue reaches is the one backed by this test's Redis")

	require.NotNil(t, processor.publisher,
		"the relay must adopt the process publisher; without it processBatch could not complete a row")
	require.True(t, IsNoopEventPublisher(processor.publisher),
		"with no brokers configured the constructor must adopt the no-op — and a relay holding it "+
			"drains the outbox over the legacy leg alone, which is NOT the branch under test here")

	publisher := &relayFakePublisher{}

	processor.store = store
	processor.publisher = publisher
	processor.now = legacyWebhookRelayNow
	// Inside the window is the NEGATION of "the sunset has passed": the caller states the
	// boundary in the sunset's terms because that is what these tests are about, and the
	// relay's seam is the window predicate.
	processor.dualDeliveryActive = func(time.Time) bool { return !sunsetPassed }
	processor.windowState = func(time.Time) WebhookWindowState {
		if sunsetPassed {
			return WebhookWindowClosed
		}

		return WebhookWindowActive
	}

	return processor, store, publisher
}

// legacyWebhookPendingTasks lists the pending tasks on a queue, treating ONLY ErrQueueNotFound as
// an empty queue: asynq creates a queue lazily, so "nothing was enqueued" surfaces as that error
// rather than as an empty list. Every other listing error fails the test.
func legacyWebhookPendingTasks(t *testing.T, inspector *asynq.Inspector, queue string) []*asynq.TaskInfo {
	t.Helper()

	tasks, err := inspector.ListPendingTasks(queue)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return nil
	}

	require.NoError(t, err, "the webhook queue must be inspectable")

	return tasks
}

// legacyWebhookInspector returns an inspector bound to a Redis address, closed on cleanup.
func legacyWebhookInspector(t *testing.T, redisDSN string) *asynq.Inspector {
	t.Helper()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisDSN})
	t.Cleanup(func() {
		assert.NoError(t, inspector.Close(), "closing the inspector must not fail")
	})

	return inspector
}

// TestSendWebhook_InvokedFromRelayDualDeliveryBranch asserts WHO calls the legacy transport: not
// a domain post-action, but the relay's dual-delivery branch, from a CLAIMED blnk.event_outbox
// row. Both transports therefore read one row rather than serialising the payload twice.
//
// "A task appeared on the queue" would be satisfied by either caller, so provenance is asserted
// two ways. The task IDENTITY is derived from the row's event_id — only
// EnqueueLegacyWebhookDelivery sets an asynq task ID, so a task carrying the event id came from
// the row, and the identity is also what suppresses a duplicate after a re-claim. The task BODY is
// the row's stored bytes rather than an equal-looking re-serialisation. Comparing those bytes
// against what Kafka received is criterion V-8's job in event_dual_delivery_test.go, not this
// test's.
//
// The queue and the task type must both equal the configured Queue.WebhookQueue AND equal each
// other: the worker's mux dispatches on the TYPE while asynq.Queue routes to the QUEUE, so a task
// whose type stopped matching its queue would expire unhandled with nothing failing to say so.
//
// The store and the Kafka leg are substituted here (the fake above and the no-op publisher), so
// what is proven is the enqueue and its provenance — not that any subscriber is reached.
func TestSendWebhook_InvokedFromRelayDualDeliveryBranch(t *testing.T) {
	redisServer := miniredis.RunT(t)

	cnf := legacyWebhookConfiguration(redisServer.Addr(), legacyWebhookNeverCalledURL)
	storeLegacyWebhookConfiguration(t, cnf)

	instance, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)
	require.NotNil(t, instance)

	row := legacyWebhookOutboxRow(t, "evt_relay_dual_delivery_1", "transaction.applied")
	processor, store, publisher := legacyWebhookRelay(t, instance, false, row)

	claimed := processor.processBatch(context.Background())
	require.Equal(t, 1, claimed, "the relay must claim and process the seeded outbox row")
	require.Equal(t, 1, store.claimCount(), "the batch must come from a claim, not from a domain call")

	inspector := legacyWebhookInspector(t, redisServer.Addr())

	tasks := legacyWebhookPendingTasks(t, inspector, cnf.Queue.WebhookQueue)
	require.Len(t, tasks, 1,
		"the dual-delivery branch must enqueue exactly one legacy delivery for the claimed row")

	task := tasks[0]

	assert.Equal(t, cnf.Queue.WebhookQueue, task.Type,
		"the asynq task type must be the CONFIGURED webhook queue name, because the worker's mux "+
			"dispatches on the task type")
	assert.Equal(t, cnf.Queue.WebhookQueue, task.Queue,
		"the task must be routed to the CONFIGURED webhook queue")
	assert.Equal(t, task.Queue, task.Type,
		"the task type and the queue name are deliberately the same string; if they diverge the "+
			"task lands on a queue whose mux has no handler for its type and expires unhandled")

	assert.Equal(t, legacyWebhookTaskID(row.EventID), task.ID,
		"the enqueue must carry the row-derived task identity, which is what suppresses a "+
			"duplicate webhook when a crash leaves the row re-claimable")
	assert.Contains(t, task.ID, row.EventID,
		"the identity must be derived from the row's event_id: SendWebhook sets no task ID at all, "+
			"so asynq would mint a random UUID that could not contain it. This is what proves the "+
			"enqueue came from the claimed outbox row rather than from a domain post-action")

	assert.Equal(t, []byte(row.Payload), task.Payload,
		"the legacy leg must carry the row's STORED BYTES, not a re-serialisation of them; that is "+
			"what makes the row the single source both transports read")

	webhookMarks := store.snapshotWebhookMarks()
	require.Len(t, webhookMarks, 1, "the legacy leg must be recorded so a re-claim does not repeat it")
	assert.Equal(t, row.ID, webhookMarks[0].id)
	assert.Equal(t, legacyWebhookRelayClaimToken, webhookMarks[0].claimToken,
		"the marker is conditional on the claim token, so it must be presented before "+
			"MarkEventDispatched clears it")

	published := publisher.snapshotRequests()
	require.Len(t, published, 1,
		"the Kafka leg must publish the same row: dual delivery means BOTH transports run off one "+
			"claim, not one or the other")
	assert.Equal(t, row.EventID, published[0].Event.EventID)
	assert.Equal(t, []byte(row.Payload), []byte(published[0].Event.Payload),
		"and it must carry the row's stored bytes too, which is why the two legs cannot drift")

	dispatched := store.snapshotDispatched()
	require.Len(t, dispatched, 1,
		"and the row must be marked dispatched exactly once for that one claim")
	assert.Equal(t, row.ID, dispatched[0].id)
	assert.Equal(t, legacyWebhookRelayClaimToken, dispatched[0].claimToken)

	assert.Empty(t, store.snapshotFailures(),
		"no publish attempt may be recorded as failed; a legacy enqueue must never consume the "+
			"Kafka retry budget")

	assert.Empty(t, store.snapshotWebhookPendings(),
		"and nothing may be recorded as OUTSTANDING: the enqueue succeeded against a real Redis, so "+
			"the row owes the legacy transport nothing and belongs in its terminal state")
}

// TestSendWebhook_NotInvokedFromRelayAfterTheSunset is what makes the test above discriminating:
// a relay that enqueued the legacy delivery unconditionally would satisfy every assertion in this
// file and the 30-day window would silently become permanent. The same harness runs with the
// sunset PASSED and no enqueue may happen.
//
// The Kafka leg must still complete, because the sunset retires one transport and must not disturb
// the other, and the row must still be claimed and processed so that "nothing was enqueued" cannot
// pass for the uninteresting reason that nothing ran.
func TestSendWebhook_NotInvokedFromRelayAfterTheSunset(t *testing.T) {
	redisServer := miniredis.RunT(t)

	cnf := legacyWebhookConfiguration(redisServer.Addr(), legacyWebhookNeverCalledURL)
	storeLegacyWebhookConfiguration(t, cnf)

	instance, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)
	require.NotNil(t, instance)

	row := legacyWebhookOutboxRow(t, "evt_relay_after_sunset_1", "transaction.applied")
	processor, store, publisher := legacyWebhookRelay(t, instance, true, row)

	claimed := processor.processBatch(context.Background())
	require.Equal(t, 1, claimed,
		"the row must still be claimed and processed after the sunset; only the legacy leg goes")

	inspector := legacyWebhookInspector(t, redisServer.Addr())

	assert.Empty(t, legacyWebhookPendingTasks(t, inspector, cnf.Queue.WebhookQueue),
		"after the sunset no legacy webhook may be enqueued, even for a webhook URL that is still "+
			"configured; the configuration outliving the date must not resurrect the transport")
	assert.Empty(t, store.snapshotWebhookMarks(),
		"and nothing may be recorded as legacy-dispatched, because nothing was dispatched")

	published := publisher.snapshotRequests()
	require.Len(t, published, 1,
		"the Kafka leg is unaffected by the sunset: after the date it is simply the only transport")
	assert.Equal(t, row.EventID, published[0].Event.EventID)

	require.Len(t, store.snapshotDispatched(), 1,
		"and the row is still retired on the strength of that publish")
	assert.Empty(t, store.snapshotFailures())
	assert.Empty(t, store.snapshotWebhookPendings(),
		"a leg that is no longer owed must not be recorded as outstanding: after the sunset the "+
			"promise has ended, so the row is terminal rather than left cycling through claims")
}
