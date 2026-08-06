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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// This file covers the LEGACY HTTP webhook transport, and it is DELETED TOGETHER WITH
// webhooks.go at the webhook sunset — see the "===== SUNSET =====" procedure at the foot
// of webhooks.go, whose STEP 2 names this file and webhooks_process_test.go explicitly.
// Nothing here is deleted before then: the deletion is the terminal step of the Kafka
// event-streaming feature and is due only once WebhookSunsetPassed (event_sunset.go)
// answers true for the deployed WEBHOOK_DEPRECATION_SUNSET_DATE, that is only after the
// full 30-day dual-delivery window has elapsed. Until that date the legacy transport is
// live in production, so it stays covered here rather than being retired early.
//
// # What this file is responsible for during the dual-delivery window
//
// Two things, and they are deliberately narrow.
//
//  1. THE LEGACY TRANSPORT'S OWN BEHAVIOUR. The enqueue, the pooled HTTP client and its
//     connection reuse are the subject; they are unchanged by the move to Kafka, and a
//     regression in them during the window is a regression in a transport that is still
//     delivering. What changed is only WHO CALLS the transport: the domain post-actions
//     no longer do, the relay's dual-delivery branch does, from a claimed
//     blnk.event_outbox row. That change of caller is asserted here too, because the
//     legacy path being reachable from the relay is what makes the window work at all.
//
//  2. THE GRACEFUL-DEGRADATION GATE. Every test here constructs NewBlnk(nil) with a nil
//     datasource and NO Kafka configuration whatsoever. That is exactly the shape of
//     every deployment that does not run Kafka, so these tests are the practical proof
//     that an unconfigured broker list is a legitimate steady state: publisher
//     construction must select the no-op implementation, return a nil error, dial
//     nothing and block on nothing. IF ANY TEST IN THIS FILE STARTS FAILING OR HANGING,
//     THE PUBLISHER CONSTRUCTION IS WRONG — fix initializeEventPublisher in blnk.go, and
//     never the assertions here.
//
// What this file deliberately does NOT do, so that ownership stays single:
//
//   - It does not compare the Kafka message against the webhook body. Payload
//     equivalence between the two transports is acceptance criterion V-8 and belongs to
//     event_dual_delivery_test.go; duplicating it here would put one guarantee in two
//     places that could then disagree.
//   - It does not parse or reason about the sunset date. The relay tests below pin the
//     sunset decision to a fixed answer so they exercise the BRANCH; the date
//     arithmetic itself is event_sunset_test.go's subject.
//   - It imports no Kafka client. The Kafka-touching tests are event_publisher_test.go,
//     event_admin_test.go, event_dlt_test.go and the integration tests.

// MockConfigFetcher is a mock for the config fetching
type MockConfigFetcher struct {
	mock.Mock
}

func (m *MockConfigFetcher) Fetch() (*config.Configuration, error) {
	args := m.Called()
	return args.Get(0).(*config.Configuration), args.Error(1)
}

func TestSendWebhook(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
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
	}
	config.ConfigStore.Store(cnf)

	testData := NewWebhook{
		Event:   "transaction.queued",
		Payload: getTransactionMock(10000, false),
	}
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)

	err = blnk.SendWebhook(testData)
	assert.NoError(t, err)

	// Verify that the task was enqueued
	assert.NoError(t, err)
	tasks := mr.Keys()
	assert.NoError(t, err)
	assert.NotEmpty(t, tasks)
}

func TestConnectionReuse(t *testing.T) {
	// Track unique connections
	var connectionsMutex sync.Mutex
	connections := make(map[string]bool)
	requestCount := 0

	// Create test server that tracks connections
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionsMutex.Lock()
		defer connectionsMutex.Unlock()

		// Track unique remote addresses (connections)
		remoteAddr := r.RemoteAddr
		connections[remoteAddr] = true
		requestCount++

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	// Setup miniredis for queue
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	// Configure with test server URL
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: server.URL,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	// Create Blnk instance
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	// Discarding the close error matches the idiom used throughout the webhook tests
	// (see webhooks_process_test.go): the assertion subject is the client's configuration
	// and its connection reuse, and a shutdown error here would mask that subject rather
	// than report on it. Behaviour is identical to the bare defer this replaces.
	defer func() { _ = blnk.Close() }()

	// Send multiple webhook requests directly (bypass queue for immediate testing)
	numRequests := 10
	testWebhook := NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"test": "data"},
	}

	// Send multiple requests concurrently to test connection reuse
	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := processHTTP(testWebhook, blnk.httpClient)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Allow some time for all requests to complete
	time.Sleep(100 * time.Millisecond)

	connectionsMutex.Lock()
	uniqueConnections := len(connections)
	totalRequests := requestCount
	connectionsMutex.Unlock()

	// Verify connection reuse
	t.Logf("Total requests: %d, Unique connections: %d", totalRequests, uniqueConnections)

	// With connection reuse, we should have fewer unique connections than requests
	// Allow for some flexibility as the exact number can vary based on timing
	assert.Equal(t, numRequests, totalRequests, "All requests should have been received")
	assert.LessOrEqual(t, uniqueConnections, totalRequests, "Should have fewer or equal connections than requests")

	// In most cases with proper connection reuse, we should see significantly fewer connections
	// This is a more lenient check to account for test environment variations
	if uniqueConnections < totalRequests {
		t.Logf("✅ Connection reuse working: %d connections for %d requests", uniqueConnections, totalRequests)
	} else {
		t.Logf("⚠️  Connection reuse may not be optimal: %d connections for %d requests", uniqueConnections, totalRequests)
	}
}

func TestHTTPClientConfiguration(t *testing.T) {
	// Setup miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
	}
	config.ConfigStore.Store(cnf)

	// Create Blnk instance
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	// Discarding the close error matches the idiom used throughout the webhook tests
	// (see webhooks_process_test.go): the assertion subject is the client's configuration
	// and its connection reuse, and a shutdown error here would mask that subject rather
	// than report on it. Behaviour is identical to the bare defer this replaces.
	defer func() { _ = blnk.Close() }()

	// Verify HTTP client is configured properly
	assert.NotNil(t, blnk.httpClient, "HTTP client should be initialized")
	assert.Equal(t, 30*time.Second, blnk.httpClient.Timeout, "Timeout should be 30 seconds")

	// Verify transport configuration
	transport, ok := blnk.httpClient.Transport.(*http.Transport)
	assert.True(t, ok, "Transport should be *http.Transport")
	assert.Equal(t, 100, transport.MaxIdleConns, "MaxIdleConns should be 100")
	assert.Equal(t, 10, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should be 10")
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should be 90 seconds")
}

func TestProcessWebhookWithReusedClient(t *testing.T) {
	// Track that the same client instance is used
	var clientUsed *http.Client
	var clientMutex sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	// Setup miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: server.URL,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	// Discarding the close error matches the idiom used throughout the webhook tests
	// (see webhooks_process_test.go): the assertion subject is the client's configuration
	// and its connection reuse, and a shutdown error here would mask that subject rather
	// than report on it. Behaviour is identical to the bare defer this replaces.
	defer func() { _ = blnk.Close() }()

	// Store reference to the HTTP client
	clientMutex.Lock()
	clientUsed = blnk.httpClient
	clientMutex.Unlock()

	// Create webhook payloads
	webhook1 := NewWebhook{Event: "test.event1", Payload: map[string]string{"id": "1"}}
	webhook2 := NewWebhook{Event: "test.event2", Payload: map[string]string{"id": "2"}}

	// Process webhooks using the same client
	err1 := processHTTP(webhook1, blnk.httpClient)
	err2 := processHTTP(webhook2, blnk.httpClient)

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Verify the same client instance was used
	clientMutex.Lock()
	assert.Same(t, clientUsed, blnk.httpClient, "Should use the same HTTP client instance")
	clientMutex.Unlock()
}

// ===========================================================================
// ADDITIONS FOR THE DUAL-DELIVERY WINDOW — DELETED WITH THIS FILE AT SUNSET
//
// Everything below this banner is retired at exactly the same moment as the four tests
// above, because it has exactly the same subject: a transport that no longer exists has
// no behaviour to assert and no caller to be reached from. Nothing here needs to be
// relocated first — unlike NewWebhook and getEventFromStatus in webhooks.go, none of it
// is contract that outlives the transport.
// ===========================================================================

// legacyWebhookConstructionBudget bounds how long NewBlnk may take in this file.
//
// The bound IS the assertion, not a convenience. "Publisher construction performs no I/O"
// is only observable as "construction did not wait": a constructor that dialled a broker,
// resolved a name or fetched partition metadata could not finish inside two seconds
// against an unroutable address, and one that blocked outright would hang the whole
// package until the test binary's timeout killed it rather than failing with a legible
// message. Two seconds is generous for pure computation and far below any dial timeout.
const legacyWebhookConstructionBudget = 2 * time.Second

// legacyWebhookBlackholeBroker is an address that exists in no routing table.
//
// 198.51.100.1 is TEST-NET-2, reserved by RFC 5737 for documentation and guaranteed to be
// routed nowhere, so a construction that dialled it would stall until the transport's dial
// timeout — well past legacyWebhookConstructionBudget. Passing the budget with this
// address configured is therefore evidence of the ABSENCE of a dial, which is the only way
// to test for an absence.
const legacyWebhookBlackholeBroker = "198.51.100.1:9092"

// legacyWebhookNeverCalledURL is a webhook URL that is configured but never contacted.
//
// EnqueueLegacyWebhookDelivery and SendWebhook only ever check that the URL is non-empty —
// delivery is the worker's job, and no worker runs in these tests — so a port that refuses
// every connection is the honest way to say "configured, not exercised". Port 1 is
// privileged and unbound, so an accidental delivery attempt would fail loudly instead of
// reaching something real.
const legacyWebhookNeverCalledURL = "http://127.0.0.1:1/never-called"

// legacyWebhookQueueName is deliberately NOT the "webhook_queue" the tests above use, and
// not any default either.
//
// The invariant under test is that the asynq task's type and queue are both the CONFIGURED
// Queue.WebhookQueue string. A recognisable name would let a regression that hardcoded a
// default, or that read a different configuration field, still satisfy the assertion. An
// unmistakable one cannot appear on the queue by accident.
const legacyWebhookQueueName = "legacy_webhook_dual_delivery_q"

// legacyWebhookRelayFixedNow pins the relay's clock so the claim instant is deterministic.
const legacyWebhookRelayFixedNow = "2026-03-01T12:00:00Z"

// legacyWebhookRelayClaimToken is the token the fake store stamps on a claim.
//
// Every state transition the relay performs is conditional on the claim token, so asserting
// the token a transition presented is how these tests prove the transition belonged to the
// claim that produced the row rather than to a stale worker.
const legacyWebhookRelayClaimToken = "legacy-webhook-claim-token"

// storeLegacyWebhookConfiguration publishes cnf for the duration of one test and restores
// whatever was there before.
//
// config.ConfigStore is a process-global atomic.Value, so a configuration left behind by one
// test produces failures in unrelated ones that look nothing like their cause. The four
// tests above predate this helper and are left exactly as they are; every addition below
// uses it, so the additions cannot be the source of such a leak.
//
// It writes DIRECTLY rather than through config.MockConfig because MockConfig runs
// validateAndAddDefaults, which both refuses a configuration with no data-source DSN — the
// value would be silently dropped — and supplies the very Kafka defaults some cases here
// need to observe as absent.
//
// Parameters:
//   - t *testing.T: the test whose lifetime the configuration is scoped to.
//   - cnf *config.Configuration: the configuration to publish.
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

// legacyWebhookConfiguration returns the configuration shape this file's legacy tests use:
// a Redis DSN, the webhook queue, a configured webhook URL, and NO Kafka block at all.
//
// The absent Kafka block is the point rather than an omission. It is the shape of every
// deployment that does not run Kafka, and it is what the graceful-degradation assertions
// below are about.
//
// Parameters:
//   - redisDSN string: the miniredis address backing the asynq client.
//   - webhookURL string: the configured webhook URL; empty disables the legacy transport.
//
// Returns:
//   - *config.Configuration: a configuration ready to store.
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

// newBlnkWithinLegacyWebhookBudget constructs a Blnk instance and fails the test if the
// constructor has not returned within legacyWebhookConstructionBudget.
//
// Running the constructor on its own goroutine is what turns "NewBlnk blocked" from a hang
// that consumes the package's whole test timeout into a single failing test with a message
// naming the cause. The instance is closed on cleanup so neither the asynq client's
// connections nor the publisher's resources outlive the test.
//
// It always passes a NIL DATASOURCE, exactly as every test in this file does, because that
// is the construction whose continued support is being asserted.
//
// Parameters:
//   - t *testing.T: the test to bound and to attach the cleanup to.
//
// Returns:
//   - *Blnk: the constructed instance, or nil when construction failed.
//   - error: whatever NewBlnk reported.
func newBlnkWithinLegacyWebhookBudget(t *testing.T) (*Blnk, error) {
	t.Helper()

	type construction struct {
		instance *Blnk
		err      error
	}

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
		t.Fatalf(
			"NewBlnk did not return within %s; publisher construction must dial nothing and "+
				"block on nothing, or every process start and every test here depends on a "+
				"reachable broker",
			legacyWebhookConstructionBudget,
		)

		return nil, nil
	}
}

// TestNewBlnk_NoKafkaConfiguredUsesNoOpPublisher is the graceful-degradation gate for the
// four legacy tests above.
//
// Every one of them constructs NewBlnk(nil) with no Kafka configuration, so all four depend
// silently on an unconfigured broker list being a legitimate steady state. This test states
// that dependency out loud: with no brokers, publisher construction selects the no-op
// implementation, returns a NIL ERROR, and reaches neither a resolver nor a socket. Were it
// to return an error instead, or to dial, the four tests above would fail or hang for a
// reason that had nothing to do with webhooks — and the right fix would be in
// initializeEventPublisher (blnk.go), never in their assertions.
//
// It is scoped deliberately to the two configuration literals THIS FILE actually builds.
// blnk_test.go asserts the same selection over its own fixtures and is the general
// statement of the property; this is the specific guard for these four tests, which is why
// the cases below are named after the tests whose input they reproduce rather than after
// abstract configuration shapes.
//
// The blank-broker case is the interesting one. Treated as an address rather than as
// absence, "   " would produce a Kafka publisher that can never connect, so every publish
// would fail against something that is not an address while nothing in the configuration
// looked wrong.
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

// TestNewBlnk_KafkaConfiguredKeepsTheLegacyTransport is the other half of the gate, and
// without it the assertions above would be satisfied by a constructor that ALWAYS returned
// the no-op.
//
// That failure mode is not hypothetical: a publisher wired to return the no-op
// unconditionally would pass every assertion in this file, start every process without
// complaint, and publish nothing at all. So this case configures brokers and requires that
// the no-op is NOT selected.
//
// It states the dual-delivery wiring claim at the same time: configuring Kafka must leave
// the legacy transport exactly as it was. During the 30-day window both transports are live
// together, so the arrival of the new one must not disturb the pooled HTTP client, its
// timeouts, or the enqueue that feeds the webhook queue — all three are asserted below
// while Kafka is configured.
//
// No SASL credentials are set and InsecureLocalDev acknowledges the resulting plaintext
// transport. That is the honest way to reach a constructed Kafka publisher here: nothing is
// dialled, so there is no credential to protect, and inventing one would put a
// secret-shaped literal in a test that has no use for it.
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
}

// legacyWebhookRelayStore is the eventRelayStore seam, narrowed to what driving one batch
// needs and nothing more.
//
// The relay is exercised against this rather than against PostgreSQL because the subject of
// these tests is THE LEGACY TRANSPORT AND ITS CALLER, not the outbox repository — whose own
// claim semantics, FIFO ordering and skip-locked concurrency are database/event_outbox_test.go's
// subject. What must be real here is the legacy leg: processor.legacy is the actual *Blnk,
// so the enqueue under assertion is the production one, reaching a real asynq client and a
// real Redis.
//
// Every field is mutex-guarded and read back through a snapshot accessor because
// processBatch publishes partition-key groups on concurrent goroutines, so an unguarded
// counter would be a data race that -race would rightly fail.
type legacyWebhookRelayStore struct {
	mu sync.Mutex

	// pending is the claimable set. It is drained by the first claim, so a second poll in
	// the same test finds nothing and cannot re-enqueue a webhook for a row already done.
	pending []model.EventOutbox

	claims       int
	dispatched   []legacyWebhookRelayMark
	webhookMarks []legacyWebhookRelayMark
	failures     []string
}

// Compile-time proof that the fake is a faithful subset of the seam the relay drives. It
// fails the build here, on the line that states the contract, rather than at a call site.
var _ eventRelayStore = (*legacyWebhookRelayStore)(nil)

// ClaimPendingEventOutbox hands out the seeded rows once, stamping each with the processing
// status, the batch's claim token and a lease, exactly as the repository does.
//
// Parameters:
//   - _ context.Context: unused; the fake never blocks.
//   - _ int: the batch size, unused because the seeded set is always small enough.
//   - lockDuration time.Duration: the lease the relay asked for, used to stamp LockedUntil.
//
// Returns:
//   - []model.EventOutbox: the claimed rows on the first call, nil thereafter.
//   - error: always nil.
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
func (s *legacyWebhookRelayStore) MarkEventDispatched(_ context.Context, id int64, claimToken string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dispatched = append(s.dispatched, legacyWebhookRelayMark{id: id, claimToken: claimToken})

	return nil
}

// MarkEventFailed records a failed publish attempt and reports that budget remains.
//
// Nothing in this file should reach it — the publisher adopted here accepts every event —
// so the recording exists so that a test can assert the ABSENCE of failures rather than
// infer success from the presence of a dispatch.
func (s *legacyWebhookRelayStore) MarkEventFailed(
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
		Status:   model.EventOutboxStatusPending,
		Attempts: 1,
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

// legacyWebhookRelayNow is the pinned relay clock, parsed from legacyWebhookRelayFixedNow.
//
// It is a function rather than a package variable so the constant above stays the single
// written form of the instant, and it cannot fail: the literal is a compile-time-visible
// RFC3339 string, and a malformed one would fail every test in this section immediately and
// unmistakably rather than subtly.
func legacyWebhookRelayNow() time.Time {
	parsed, err := time.Parse(time.RFC3339, legacyWebhookRelayFixedNow)
	if err != nil {
		panic("legacyWebhookRelayFixedNow must be a valid RFC3339 instant: " + err.Error())
	}

	return parsed
}

// legacyWebhookOutboxRow builds one pending blnk.event_outbox row whose payload is a REAL
// legacy webhook body.
//
// The payload is the marshaled NewWebhook envelope, both keys included, because that is
// what the production capture path stores and therefore what the relay hands to the legacy
// transport verbatim. Building it any other way would make the provenance assertion below
// compare bytes that production never produces.
//
// Parameters:
//   - t *testing.T: the test, for the marshal assertion.
//   - eventID string: the row's event_id, which becomes the asynq task identity.
//   - eventType string: the event name, carried in the envelope and used to resolve the topic.
//
// Returns:
//   - model.EventOutbox: a pending row ready to be claimed.
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

// legacyWebhookRelay builds a relay whose LEGACY LEG IS THE REAL instance and whose store is
// the fake above, with the clock and the sunset decision pinned.
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
//   - the sunset decision is a pinned function. WebhookSunsetPassed reads the configured
//     date, and event_sunset.go owns that arithmetic; pinning the ANSWER is what makes these
//     tests about the branch rather than about date parsing, and it keeps them independent of
//     whatever WEBHOOK_DEPRECATION_SUNSET_DATE the ambient environment happens to carry.
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
func legacyWebhookRelay(
	t *testing.T,
	instance *Blnk,
	sunsetPassed bool,
	rows ...model.EventOutbox,
) (*EventRelayProcessor, *legacyWebhookRelayStore) {
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
		"with no brokers configured the Kafka leg must be the no-op, which is what lets this file "+
			"drive the relay without a broker and without a Kafka client import")

	processor.store = store
	processor.now = legacyWebhookRelayNow
	processor.sunsetPassed = func(time.Time) bool { return sunsetPassed }

	return processor, store
}

// legacyWebhookPendingTasks lists the pending tasks on a queue, treating a queue that does
// not exist as an empty queue.
//
// asynq creates a queue lazily on the first enqueue, so "nothing was enqueued" surfaces as
// ErrQueueNotFound rather than as an empty list. Both mean the same thing to the assertions
// below, and collapsing them here keeps the negative test from having to branch on it.
//
// Parameters:
//   - t *testing.T: the test.
//   - inspector *asynq.Inspector: an inspector bound to the test's Redis.
//   - queue string: the queue name.
//
// Returns:
//   - []*asynq.TaskInfo: the pending tasks, or nil when the queue was never created.
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
//
// Parameters:
//   - t *testing.T: the test to attach the cleanup to.
//   - redisDSN string: the address of the test's Redis.
//
// Returns:
//   - *asynq.Inspector: a ready inspector.
func legacyWebhookInspector(t *testing.T, redisDSN string) *asynq.Inspector {
	t.Helper()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisDSN})
	t.Cleanup(func() {
		assert.NoError(t, inspector.Close(), "closing the inspector must not fail")
	})

	return inspector
}

// TestSendWebhook_InvokedFromRelayDualDeliveryBranch is the retargeting assertion: the
// legacy transport is still delivering, but it is no longer the DOMAIN that calls it.
//
// Before this change, a domain post-action called SendWebhook directly. Now the domain writes
// an event to blnk.event_outbox inside the ledger transaction, and the relay's dual-delivery
// branch enqueues the legacy delivery from the CLAIMED ROW. That change of caller is the whole
// mechanism of the 30-day window, and it is what makes the two transports carry the same bytes
// structurally: both read one row, so there is no second serialisation for them to drift apart
// through.
//
// # How the assertions establish provenance rather than merely delivery
//
// "A task appeared on the queue" would be satisfied by either caller. Two properties
// distinguish them, and both are asserted:
//
//   - THE TASK IDENTITY IS DERIVED FROM THE ROW'S event_id. Only
//     EnqueueLegacyWebhookDelivery sets an asynq task ID, and it derives it from the event id
//     so that a re-claim after a crash is refused as a duplicate. SendWebhook sets none, so
//     asynq mints a random UUID that cannot contain the event id. A task whose identity
//     carries the event id therefore came from the outbox row.
//   - THE TASK BODY IS THE ROW'S STORED BYTES. Not an equal-looking re-serialisation: the same
//     bytes. This asserts the row was the SOURCE, which is the provenance claim. It is
//     deliberately NOT a comparison against what Kafka received — payload equivalence between
//     the two transports is acceptance criterion V-8 and belongs to event_dual_delivery_test.go,
//     and duplicating it here would put one guarantee in two places that could disagree.
//
// The queue and the task type are asserted to be the configured Queue.WebhookQueue string AND
// to be the same string as each other, because they are deliberately identical: the worker's
// mux dispatches on the task TYPE while asynq.Queue routes to the QUEUE, so a task whose type
// stopped matching its queue would land somewhere with no handler for it and expire unhandled
// with nothing failing to say so.
func TestSendWebhook_InvokedFromRelayDualDeliveryBranch(t *testing.T) {
	redisServer := miniredis.RunT(t)

	cnf := legacyWebhookConfiguration(redisServer.Addr(), legacyWebhookNeverCalledURL)
	storeLegacyWebhookConfiguration(t, cnf)

	instance, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)
	require.NotNil(t, instance)

	row := legacyWebhookOutboxRow(t, "evt_relay_dual_delivery_1", "transaction.applied")
	processor, store := legacyWebhookRelay(t, instance, false, row)

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

	dispatched := store.snapshotDispatched()
	require.Len(t, dispatched, 1,
		"the Kafka leg must complete for the same row: dual delivery means BOTH transports run off "+
			"one claim, not one or the other")
	assert.Equal(t, row.ID, dispatched[0].id)
	assert.Equal(t, legacyWebhookRelayClaimToken, dispatched[0].claimToken)

	assert.Empty(t, store.snapshotFailures(),
		"no publish attempt may be recorded as failed; a legacy enqueue must never consume the "+
			"Kafka retry budget")
}

// TestSendWebhook_NotInvokedFromRelayAfterTheSunset is the boundary that makes the test above
// discriminating rather than merely descriptive.
//
// Without it, a relay that enqueued the legacy delivery unconditionally — ignoring the sunset
// entirely — would satisfy every assertion in this file, and the 30-day window would silently
// become permanent. So the same harness is run with the sunset PASSED and the enqueue must not
// happen at all.
//
// The Kafka leg is asserted to complete regardless, because the sunset retires one transport
// and must not disturb the other: after the date, Kafka is simply the only transport. The
// relay is also asserted to have claimed and processed the row, so "nothing was enqueued"
// cannot be passing for the uninteresting reason that nothing ran.
//
// This test outlives neither the file nor the branch. At the sunset, when the dual-delivery
// branch is deleted, its subject ceases to exist along with everything else here.
func TestSendWebhook_NotInvokedFromRelayAfterTheSunset(t *testing.T) {
	redisServer := miniredis.RunT(t)

	cnf := legacyWebhookConfiguration(redisServer.Addr(), legacyWebhookNeverCalledURL)
	storeLegacyWebhookConfiguration(t, cnf)

	instance, err := newBlnkWithinLegacyWebhookBudget(t)
	require.NoError(t, err)
	require.NotNil(t, instance)

	row := legacyWebhookOutboxRow(t, "evt_relay_after_sunset_1", "transaction.applied")
	processor, store := legacyWebhookRelay(t, instance, true, row)

	claimed := processor.processBatch(context.Background())
	require.Equal(t, 1, claimed,
		"the row must still be claimed and processed after the sunset; only the legacy leg goes")

	inspector := legacyWebhookInspector(t, redisServer.Addr())

	assert.Empty(t, legacyWebhookPendingTasks(t, inspector, cnf.Queue.WebhookQueue),
		"after the sunset no legacy webhook may be enqueued, even for a webhook URL that is still "+
			"configured; the configuration outliving the date must not resurrect the transport")
	assert.Empty(t, store.snapshotWebhookMarks(),
		"and nothing may be recorded as legacy-dispatched, because nothing was dispatched")

	require.Len(t, store.snapshotDispatched(), 1,
		"the Kafka leg is unaffected by the sunset: after the date it is simply the only transport")
	assert.Empty(t, store.snapshotFailures())
}
