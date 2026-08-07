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

// THIS FILE IS LIVE COVERAGE, NOT LEGACY DEBRIS — AND IT IS DELETED WITH webhooks.go.
//
// It is the specification of the legacy HTTP webhook transport: the signed wire contract,
// the no-op-when-unconfigured semantic, the retry-on-non-2xx semantic, the task identity on
// the shared webhook queue, and the byte fidelity that makes the dual-delivery
// payload-equivalence guarantee measurable. Every one of those behaviours is a behaviour the
// Kafka pipeline either reproduces or is compared against, so the coverage below must stay
// green for the whole 30-day dual-delivery window rather than being retired early with the
// call sites that used to drive it.
//
// WHEN IT GOES. This file is deleted TOGETHER WITH webhooks.go, at step 2 of the SUNSET block
// at the foot of that file, and only once WebhookSunsetPassed (event_sunset.go) answers true
// for the deployed WEBHOOK_DEPRECATION_SUNSET_DATE. It has no subject after that: every
// function it exercises — processHTTP, processHTTPRaw, SendWebhook,
// EnqueueLegacyWebhookDelivery, legacyWebhookTaskID and ProcessWebhook — is deleted in the
// same change. Deleting it EARLIER removes the second transport the equivalence guarantee is
// measured against; deleting it LATER leaves a test file with nothing to compile against.
//
// WHAT MUST NOT GO WITH IT. Two behaviours asserted here outlive this file because the
// symbols that own them do: the NewWebhook envelope, which relocates to event_outbox.go and
// IS the payload every LedgerEvent carries, and the getEventFromStatus vocabulary, which
// relocates to event_topics.go. Their coverage lives on where they land — event_outbox_test.go
// and event_topics_test.go — so the sunset deletion loses no assertion about either. The
// getEventFromStatus tests at the foot of this file are deliberate duplicates of that
// vocabulary from the payload contract's side, kept here for as long as this file is the home
// of the contract.
//
// Nothing in this file may assert /hooks behaviour. The webhook asynq QUEUE is shared with
// transaction hooks and TypeSense indexing and survives the sunset untouched; only the
// ProcessWebhook handler mapping is removed. See SUNSET step 4 in webhooks.go.

package blnk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/config"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test infrastructure: the Redis these tests use, the queues they own, and how a
// queue listing is allowed to fail
// ---------------------------------------------------------------------------

// legacyWebhookRedisAddrEnv overrides the Redis address these tests use.
//
// The tests below are integration tests by nature: asynq's enqueue path and its Inspector are
// the subject, not a stand-in for them, so they need a real Redis. The address was hard-coded
// at every one of a dozen call sites, which made "run this suite against a different Redis"
// impossible and gave a wrong address a dozen separate ways to fail.
const legacyWebhookRedisAddrEnv = "BLNK_TEST_REDIS_ADDR"

// legacyWebhookDefaultRedisAddr is the address the local stack publishes.
const legacyWebhookDefaultRedisAddr = "localhost:6379"

// legacyWebhookRedisDialBudget bounds the reachability probe below.
const legacyWebhookRedisDialBudget = 2 * time.Second

// legacyWebhookRedisAddr resolves the Redis address and proves something is listening on it.
//
// # Why it SKIPS rather than fails
//
// An unreachable Redis is a statement about the environment, not about the legacy webhook
// transport, and it is indistinguishable from it in the failure output: asynq reports a dial
// error from inside Enqueue, so a dozen tests fail at once with a message that names neither
// Redis nor the fact that one is required. The probe converts that into one legible skip per
// test, with the address it tried and the command that provides one.
//
// # Why it is a skip and not miniredis
//
// asynq's Inspector reads its own Redis data structures through Lua, and several of these
// tests assert on task IDENTITY and RETENTION — behaviour that lives in those scripts. A
// stand-in that implements Redis approximately would make those assertions statements about
// the stand-in. The real broker is the subject here, and the tests that do NOT need one
// (the whole of webhooks_test.go, and every relay test) already use miniredis or fakes.
//
// Parameters:
//   - t *testing.T: the test to skip when nothing is listening.
//
// Returns:
//   - string: the resolved "host:port".
func legacyWebhookRedisAddr(t *testing.T) string {
	t.Helper()

	addr := strings.TrimSpace(os.Getenv(legacyWebhookRedisAddrEnv))
	if addr == "" {
		addr = legacyWebhookDefaultRedisAddr
	}

	connection, err := net.DialTimeout("tcp", addr, legacyWebhookRedisDialBudget)
	if err != nil {
		t.Skipf(
			"no Redis is listening on %s (%v), and these tests assert on asynq's real enqueue "+
				"and Inspector behaviour rather than a stand-in for it. Start one with "+
				"'docker compose up -d redis', or point %s at another instance",
			addr, err, legacyWebhookRedisAddrEnv,
		)
	}
	require.NoError(t, connection.Close())

	return addr
}

// legacyWebhookUniqueQueueName returns a queue name no other test, and no other clone, can produce.
//
// The names were built from time.Now().UnixNano(), which is unique only by luck: two processes
// that start in the same nanosecond collide, and — more to the point — CLONE_INDEX-separated
// clones sharing one Redis would silently observe each other's tasks, so a test asserting "the
// queue is empty" could fail because a sibling clone had just filled it. A UUID plus the clone
// index removes both, and the prefix keeps the names greppable in Redis.
//
// Parameters:
//   - t *testing.T: the test the queue belongs to; its name is carried for legibility.
//   - prefix string: a short prefix naming the group of tests the queue serves.
//
// Returns:
//   - string: a unique asynq queue name.
func legacyWebhookUniqueQueueName(t *testing.T, prefix string) string {
	t.Helper()

	clone := strings.TrimSpace(os.Getenv("CLONE_INDEX"))
	if clone == "" {
		clone = "local"
	}

	return fmt.Sprintf("%s_%s_%s", prefix, clone, strings.ReplaceAll(gofakeit.UUID(), "-", ""))
}

// newLegacyWebhookInspector builds an Inspector for queueName and closes it on cleanup.
//
// An Inspector holds its own Redis client. Every one of these tests used to create one and
// leave it open, so a package run leaked one connection per test — enough, on a Redis with a
// modest maxclients, to make a LATER test fail on connection exhaustion for a reason nothing
// in its output would explain. The cleanup also empties and removes the queue, so a failed
// assertion does not leave tasks behind for the next run to trip over.
//
// Parameters:
//   - t *testing.T: the test owning the inspector.
//   - addr string: the Redis address, from legacyWebhookRedisAddr.
//   - queueName string: the queue to clean up.
//
// Returns:
//   - *asynq.Inspector: the inspector, closed when the test ends.
func newLegacyWebhookInspector(t *testing.T, addr, queueName string) *asynq.Inspector {
	t.Helper()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: addr})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_, _ = inspector.DeleteAllCompletedTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
		assert.NoError(t, inspector.Close(), "the inspector's Redis connection must be released")
	})

	return inspector
}

// listLegacyPendingTasks lists a queue's pending tasks, accepting ONLY "the queue does not
// exist" as a reason for there to be none.
//
// # The assertion this replaces could not fail
//
// Every "nothing was enqueued" test used to read `tasks, err := ListPendingTasks(q)` and then
// assert emptiness only `if err == nil`. Under that shape a Redis that was down, a wrong
// address, a permission error or an asynq API change all satisfied the test silently — the
// assertion was simply never reached. The tests that mattered most were exactly the ones that
// could not fail: "no URL configured enqueues nothing" and "past the sunset the relay enqueues
// nothing" are both proofs of an ABSENCE, and an absence is only evidence when the observation
// that looked for it is known to have worked.
//
// asynq wraps ErrQueueNotFound for a queue that has never held a task, which is a legitimate
// and expected shape of "empty" here since asynq creates a queue lazily on first enqueue. That
// one error is translated to an empty slice; every other error fails the test.
//
// Parameters:
//   - t *testing.T: the test, for Helper and failure reporting.
//   - inspector *asynq.Inspector: the inspector to list through.
//   - queueName string: the queue to list.
//
// Returns:
//   - []*asynq.TaskInfo: the pending tasks; empty when the queue does not exist.
func listLegacyPendingTasks(t *testing.T, inspector *asynq.Inspector, queueName string) []*asynq.TaskInfo {
	t.Helper()

	tasks, err := inspector.ListPendingTasks(queueName)
	if err != nil {
		require.ErrorIs(t, err, asynq.ErrQueueNotFound,
			"listing queue %q failed for a reason other than the queue not existing: an absence "+
				"of tasks is only evidence when the observation that looked for them worked",
			queueName)

		return nil
	}

	return tasks
}

// requireNoLegacyPendingTasks asserts a queue holds no pending task, failing on any listing
// error that is not "the queue does not exist".
func requireNoLegacyPendingTasks(t *testing.T, inspector *asynq.Inspector, queueName string, reason string) {
	t.Helper()

	assert.Empty(t, listLegacyPendingTasks(t, inspector, queueName), reason)
}

// receivedWebhook captures everything the webhook receiver saw for one request.
type receivedWebhook struct {
	body    []byte
	headers http.Header
	method  string
}

// newWebhookReceiver starts an httptest server responding with status and
// returns an accessor for the captured requests.
func newWebhookReceiver(status int) (*httptest.Server, func() []receivedWebhook) {
	var mu sync.Mutex
	var received []receivedWebhook

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, receivedWebhook{
			body:    body,
			headers: r.Header.Clone(),
			method:  r.Method,
		})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))

	get := func() []receivedWebhook {
		mu.Lock()
		defer mu.Unlock()
		out := make([]receivedWebhook, len(received))
		copy(out, received)
		return out
	}
	return server, get
}

// storeWebhookTestConfig stores a configuration pointing the webhook notification at url, with
// a unique webhook queue per test so that no two tests — and no two clones — can observe each
// other's tasks.
//
// It RESTORES whatever was published before, when the test finishes. config.ConfigStore is a
// process-global atomic.Value: a webhook URL, a signing secret or a queue name left installed
// here is read by every later test in the package that calls config.Fetch, and the failure
// then appears in a test that never mentioned webhooks at all.
//
// Parameters:
//   - t *testing.T: the test whose lifetime the configuration is scoped to.
//   - url string: the webhook receiver URL, or "" for the unconfigured no-op contract.
//   - secret string: the signing secret, or "" for unsigned delivery.
//   - headers map[string]string: extra headers to apply to every delivery.
//
// Returns:
//   - *config.Configuration: the published configuration.
//   - string: the unique webhook queue name.
func storeWebhookTestConfig(t *testing.T, url, secret string, headers map[string]string) (*config.Configuration, string) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

	queueName := legacyWebhookUniqueQueueName(t, "webhook_q")
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: legacyWebhookRedisAddr(t)},
		Server: config.ServerConfig{
			SecretKey: secret,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url:                     url,
				Headers:                 headers,
				AllowPrivateDestination: true,
			},
		},
	}
	config.ConfigStore.Store(cnf)
	return cnf, queueName
}

func TestProcessWebhook_DeliversSignedPayload(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	const secret = "webhook-signing-secret"
	storeWebhookTestConfig(t, server.URL, secret, map[string]string{
		"X-Custom-Tenant": "tenant-42",
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	event := NewWebhook{
		Event: "transaction.applied",
		Payload: map[string]interface{}{
			"transaction_id": "txn_process_test",
			"amount":         150.25,
		},
	}
	taskPayload, err := json.Marshal(event)
	require.NoError(t, err)

	before := time.Now().Unix()
	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", taskPayload))
	after := time.Now().Unix()
	require.NoError(t, err)

	reqs := received()
	require.Len(t, reqs, 1, "receiver must get exactly one delivery")
	req := reqs[0]

	// Method and content type.
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "application/json", req.headers.Get("Content-Type"))

	// Custom configured header is forwarded.
	assert.Equal(t, "tenant-42", req.headers.Get("X-Custom-Tenant"))

	// Event envelope: {"event": ..., "data": ...}.
	var envelope map[string]interface{}
	require.NoError(t, json.Unmarshal(req.body, &envelope))
	assert.Equal(t, "transaction.applied", envelope["event"])
	data, ok := envelope["data"].(map[string]interface{})
	require.True(t, ok, "data field must be the event payload object")
	assert.Equal(t, "txn_process_test", data["transaction_id"])
	assert.Equal(t, 150.25, data["amount"])

	// Timestamp header must be a unix timestamp from the delivery window.
	tsHeader := req.headers.Get("X-Blnk-Timestamp")
	require.NotEmpty(t, tsHeader, "X-Blnk-Timestamp must be set")
	ts, err := strconv.ParseInt(tsHeader, 10, 64)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, ts, before)
	assert.LessOrEqual(t, ts, after)

	// Signature must be HMAC-SHA256(secret, timestamp + "." + body) hex-encoded,
	// recomputable by the receiver from what it was given.
	sigHeader := req.headers.Get("X-Blnk-Signature")
	require.NotEmpty(t, sigHeader, "X-Blnk-Signature must be set")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsHeader + "." + string(req.body)))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expectedSig, sigHeader, "signature must verify against timestamp.body with the shared secret")
}

func TestProcessWebhook_MalformedTaskPayloadReturnsError(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", []byte(`{"event": "broken`)))
	assert.Error(t, err, "corrupt task payload must surface an error so asynq can retry/dead-letter")
	assert.Empty(t, received(), "no HTTP call should be made for an unparseable payload")
}

func TestProcessWebhook_NoURLConfiguredIsNoOp(t *testing.T) {
	storeWebhookTestConfig(t, "", "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.NoError(t, err, "missing webhook URL should be a silent no-op, not a task failure")
}

func TestProcessWebhook_ConnectionRefusedReturnsError(t *testing.T) {
	server, _ := newWebhookReceiver(http.StatusOK)
	deadURL := server.URL
	server.Close() // receiver is down

	storeWebhookTestConfig(t, deadURL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.Error(t, err, "transport-level failure must propagate so asynq retries the delivery")
}

func TestProcessWebhook_Non2xxTriggersRetry(t *testing.T) {
	// Regression: processHTTP used to swallow non-2xx responses (nil error),
	// so asynq marked the task succeeded and the webhook was permanently
	// lost on receiver-side failures. It must now return an error so asynq's
	// retry machinery redelivers.
	server, received := newWebhookReceiver(http.StatusServiceUnavailable)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.Error(t, err, "non-2xx from receiver must fail the task so asynq retries delivery")
	assert.Len(t, received(), 1, "exactly one attempt per ProcessWebhook call; retries are asynq's job")
}

func TestSendWebhook_EnqueuesTaskWithPayloadOnConfiguredQueue(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "http://localhost:1/never-called", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	event := NewWebhook{
		Event: "transaction.inflight",
		Payload: map[string]interface{}{
			"transaction_id": gofakeit.UUID(),
			"status":         "INFLIGHT",
		},
	}
	require.NoError(t, b.SendWebhook(event))

	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "exactly one task should land on the webhook queue")

	task := tasks[0]
	assert.Equal(t, cnf.Queue.WebhookQueue, task.Type, "task type must be the webhook queue name for worker routing")
	assert.Equal(t, queueName, task.Queue)

	var roundTripped NewWebhook
	require.NoError(t, json.Unmarshal(task.Payload, &roundTripped))
	assert.Equal(t, "transaction.inflight", roundTripped.Event)
	data, ok := roundTripped.Payload.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "INFLIGHT", data["status"])
}

func TestSendWebhook_NoURLConfiguredSkipsEnqueue(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	require.NoError(t, b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}}))

	requireNoLegacyPendingTasks(t, inspector, queueName,
		"nothing should be enqueued when no webhook URL is configured")
}

func TestSendWebhook_UnmarshalablePayloadReturnsError(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "http://localhost:1/never-called", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// channels cannot be JSON-marshaled
	err = b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: make(chan int)})
	assert.Error(t, err)

	requireNoLegacyPendingTasks(t, inspector, queueName,
		"nothing should be enqueued when payload serialization fails")
}

func TestProcessWebhook_NoSignatureHeadersWithEmptySecret(t *testing.T) {
	// Regression: an empty Server.SecretKey used to produce an HMAC over the
	// empty key — computable (and forgeable) by anyone. The delivery is now
	// sent unsigned so receivers cannot mistake a forgeable header for
	// authentication.
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.void", Payload: map[string]string{"id": "1"}})
	require.NoError(t, err)
	require.NoError(t, b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload)))

	reqs := received()
	require.Len(t, reqs, 1)
	assert.Empty(t, reqs[0].headers.Get("X-Blnk-Signature"), "no signature header may be sent without a configured secret")
	assert.Empty(t, reqs[0].headers.Get("X-Blnk-Timestamp"), "no timestamp header may be sent without a configured secret")
}

// ===== DUAL-DELIVERY BYTE FIDELITY AND TASK IDENTITY =====
//
// The tests above cover the legacy transport as it always behaved. The ones below cover the
// two properties the Kafka dual-delivery window adds to it, both of which are about the
// relay handing this transport bytes it already holds rather than a struct to re-serialise.

// nonCanonicalLegacyBody returns a legacy webhook body whose bytes CANNOT survive a round
// trip through NewWebhook.Payload interface{}.
//
// Every element of it is chosen to break under re-marshalling, because a body that survives
// proves nothing:
//
//   - The data object's keys are in deliberately non-alphabetical order. Decoding makes them
//     a map[string]interface{}, and Go marshals map keys SORTED, so re-marshalling
//     alphabetises them.
//   - The identifier is a large integer. Decoding makes it a float64, and re-marshalling a
//     float64 of that magnitude produces scientific notation.
//   - The amount carries a trailing zero and high precision. float64 re-rendering drops the
//     trailing zero and can perturb the last digits.
//
// A ledger event's payload is a marshaled domain struct, whose fields serialise in
// declaration order rather than alphabetically, so this is the realistic shape rather than a
// contrived one.
func nonCanonicalLegacyBody() []byte {
	return []byte(`{"event":"transaction.applied","data":{"transaction_id":"txn_fidelity",` +
		`"amount":150.250,"reference":"ref_1","source":"bal_src","precise_amount":9007199254740993,` +
		`"currency":"USD"}}`)
}

// TestEnqueueLegacyWebhookDelivery_CarriesTheStoredBytesVerbatim is the executable half of
// the dual-delivery payload-equivalence guarantee (V-8).
//
// The guarantee is that the legacy webhook and the Kafka message carry the SAME payload for
// the same event, asserted byte for byte. The relay holds the authoritative bytes — the exact
// blnk.event_outbox payload it published to Kafka — so the only way this transport can honour
// the guarantee is to carry them unchanged.
//
// SendWebhook cannot: it takes a struct, so a caller holding bytes must decode into
// NewWebhook.Payload interface{} and marshal again, and that round trip re-orders object keys
// and re-renders numbers. The failure is invisible to anything but a byte comparison, because
// the two bodies stay semantically equal — which is exactly why this test asserts bytes and
// then separately demonstrates that the round trip really would have changed them.
func TestEnqueueLegacyWebhookDelivery_CarriesTheStoredBytesVerbatim(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "http://localhost:1/never-called", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	stored := nonCanonicalLegacyBody()
	eventID := gofakeit.UUID()

	require.NoError(t, b.EnqueueLegacyWebhookDelivery(eventID, stored))

	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "exactly one task should land on the webhook queue")

	task := tasks[0]
	assert.Equal(t, cnf.Queue.WebhookQueue, task.Type,
		"the task type must remain the webhook queue name, or the worker mux has no handler for it")
	assert.Equal(t, queueName, task.Queue)

	assert.Equal(t, string(stored), string(task.Payload),
		"the enqueued payload must be the stored bytes verbatim; anything else forfeits byte equivalence "+
			"with the Kafka message published from the same row")

	// Proof that the assertion above has teeth: the struct round trip SendWebhook performs
	// really does change these bytes, so passing is a property of this path and not of the
	// fixture being insensitive.
	var decoded NewWebhook
	require.NoError(t, json.Unmarshal(stored, &decoded))
	reMarshalled, err := json.Marshal(decoded)
	require.NoError(t, err)
	assert.NotEqual(t, string(stored), string(reMarshalled),
		"the fixture must be one that a decode/re-marshal round trip demonstrably alters, or this test "+
			"would pass even if the raw path did not exist")
}

// TestProcessWebhook_DeliversTheEnqueuedBytesWithoutReserialising closes the second half of
// the path.
//
// Preserving bytes into the queue is worthless if the handler re-serialises them on the way
// out. This asserts the body the receiver is handed is byte-identical to the body that was
// enqueued, and — because a receiver verifies HMAC over the bytes it received — that the
// signature covers those same bytes.
func TestProcessWebhook_DeliversTheEnqueuedBytesWithoutReserialising(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	const secret = "webhook-signing-secret"
	storeWebhookTestConfig(t, server.URL, secret, nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	stored := nonCanonicalLegacyBody()

	require.NoError(t, b.ProcessWebhook(context.Background(),
		asynq.NewTask("webhook_delivery", stored)))

	reqs := received()
	require.Len(t, reqs, 1)

	assert.Equal(t, string(stored), string(reqs[0].body),
		"the delivered body must be the stored bytes verbatim, with no key reordering and no number "+
			"re-rendering")

	// The signature must cover the bytes AS SENT. A body that changed between signing and
	// sending would fail verification at every subscriber.
	timestamp := reqs[0].headers.Get("X-Blnk-Timestamp")
	require.NotEmpty(t, timestamp)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + string(reqs[0].body)))
	assert.Equal(t, hex.EncodeToString(mac.Sum(nil)), reqs[0].headers.Get("X-Blnk-Signature"),
		"the signature must verify over the delivered bytes")
}

// TestEnqueueLegacyWebhookDelivery_SuppressesADuplicateForTheSameEvent covers the task
// identity (F27).
//
// Enqueuing the delivery and recording that the webhook leg was dispatched are two
// operations against two systems with no transaction spanning them. A crash in between
// leaves the row unmarked, so the next claim enqueues the delivery again — a duplicate
// webhook for one event, which for a payment notification is a real consequence and not a
// tidiness concern.
//
// The event ID is the task's identity, so the second enqueue is refused by asynq rather than
// accepted. The refusal is reported as SUCCESS, because the post-condition the caller needs
// — this event's webhook leg is queued exactly once — already holds, and failing would stall
// the row behind a condition that is already satisfied.
func TestEnqueueLegacyWebhookDelivery_SuppressesADuplicateForTheSameEvent(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "http://localhost:1/never-called", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	stored := nonCanonicalLegacyBody()
	eventID := gofakeit.UUID()

	require.NoError(t, b.EnqueueLegacyWebhookDelivery(eventID, stored))
	require.NoError(t, b.EnqueueLegacyWebhookDelivery(eventID, stored),
		"a re-enqueue after a crash must be reported as success, because the task is already queued")

	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	assert.Len(t, tasks, 1, "one event must produce exactly one queued delivery, however many times it is claimed")

	// A DIFFERENT event is a different identity and must still be enqueued, or the dedup key
	// would be suppressing real work.
	require.NoError(t, b.EnqueueLegacyWebhookDelivery(gofakeit.UUID(), stored))

	tasks, err = inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	assert.Len(t, tasks, 2, "a distinct event must not be suppressed by another event's identity")
}

// TestEnqueueLegacyWebhookDelivery_RefusesAnUnusableRequest covers the three inputs that
// cannot produce a correct delivery.
//
// The missing event ID matters most and is the least obvious: asynq SILENTLY IGNORES an empty
// task ID rather than rejecting it, so a caller that omitted it would get at-least-once
// delivery with nothing anywhere indicating that the duplicate suppression it was relying on
// was not in effect.
func TestEnqueueLegacyWebhookDelivery_RefusesAnUnusableRequest(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "http://localhost:1/never-called", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	t.Run("a missing event id is refused", func(t *testing.T) {
		err := b.EnqueueLegacyWebhookDelivery("   ", nonCanonicalLegacyBody())
		require.Error(t, err, "without an event id there is no dedup key and asynq would not complain")
		assert.Contains(t, err.Error(), "event id")
	})

	t.Run("a body that is not a webhook envelope is refused", func(t *testing.T) {
		err := b.EnqueueLegacyWebhookDelivery(gofakeit.UUID(), []byte("{not json"))
		require.Error(t, err, "a malformed body would be delivered verbatim and fail at every subscriber")
	})

	t.Run("nothing was enqueued by either refusal", func(t *testing.T) {
		requireNoLegacyPendingTasks(t, inspector, queueName,
			"a refused request must leave the queue untouched")
	})
}

// TestEnqueueLegacyWebhookDelivery_NoURLConfiguredSkipsEnqueue pins the same
// no-op-when-unconfigured contract SendWebhook has.
//
// It is load-bearing rather than defensive: a deployment with no webhook URL runs the Kafka
// leg alone, and an error here would fail the relay's dual-delivery branch on every event for
// a transport the operator deliberately did not configure.
func TestEnqueueLegacyWebhookDelivery_NoURLConfiguredSkipsEnqueue(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig(t, "", "secret", nil)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// The event id and body are deliberately valid, so the no-op is the reason nothing is
	// enqueued rather than a validation failure standing in for it.
	require.NoError(t, b.EnqueueLegacyWebhookDelivery(gofakeit.UUID(), nonCanonicalLegacyBody()))

	requireNoLegacyPendingTasks(t, inspector, queueName,
		"nothing may be enqueued when no webhook URL is configured")
}

// TestLegacyWebhookTaskID_NamespacesTheEventID guards against a cross-feature collision.
//
// The webhook queue is SHARED with transaction hooks and TypeSense indexing, and asynq task
// IDs are unique per QUEUE rather than per task type. A bare event ID as the identity would be
// one accidental identifier collision away from silently dropping another feature's task as a
// duplicate — or having one of ours dropped.
func TestLegacyWebhookTaskID_NamespacesTheEventID(t *testing.T) {
	id := legacyWebhookTaskID("evt_123")

	assert.Equal(t, "legacy-webhook:evt_123", id)
	assert.NotEqual(t, "evt_123", id,
		"a bare event id would share an identity namespace with the hook and index tasks on this queue")
	assert.NotEqual(t, legacyWebhookTaskID("evt_123"), legacyWebhookTaskID("evt_124"),
		"distinct events must have distinct identities")
}

// ===== THE DUAL-DELIVERY BRANCH IS THE ONLY THING THAT DRIVES THIS TRANSPORT =====
//
// Everything above exercises the legacy transport through its own entry points, which is what
// pins the wire contract. What it cannot show is WHO CALLS IT, and that is the part the Kafka
// migration changed.
//
// Before: eight domain post-actions called SendWebhook directly. After: none of them do. A
// ledger mutation records an event into blnk.event_outbox inside the very transaction that
// performed it, and the relay in event_relay.go claims that row, publishes it to Kafka and —
// for as long as the dual-delivery window is open — enqueues the legacy delivery from THAT SAME
// CLAIMED ROW. The relay's dual-delivery branch is the one and only caller.
//
// The tests below are therefore the only ones in this file that start from an outbox row rather
// than from a webhook envelope, and they are deliberately the REAL transport end to end: the
// relay's legacy seam is the real *Blnk, the enqueue is a real asynq enqueue onto the real
// Redis queue, the handler is the real ProcessWebhook, and the receiver is a real HTTP server.
// event_relay_test.go covers the same branch against a fake legacy transport, which is where
// the relay's decisions belong; what only this file can show is that the decision actually
// reaches a socket.
//
// The sunset decision is likewise NOT stubbed here. processor.sunsetPassed is left as
// NewEventRelayProcessor assigned it — event_sunset.go's WebhookSunsetPassed — so the branch is
// driven by the configured WEBHOOK_DEPRECATION_SUNSET_DATE through the single decision point,
// which is the only arrangement that can show the real predicate wired to the real queue.

// storeDualDeliveryWebhookConfig publishes a configuration describing a deployment inside — or
// past the end of — the 30-day dual-delivery window.
//
// It differs from storeWebhookTestConfig in exactly two ways, and both are deliberate:
//
//   - It carries WebhookDeprecationSunsetDate, which is the sole input to the sunset decision
//     the relay's dual-delivery branch consults. Kafka.Brokers is left empty on purpose: with a
//     window that parses, the verdict is a straight comparison against it, so configuring a
//     broker would add nothing here except a dependency on one being reachable.
//   - It RESTORES whatever configuration was published before, when the test finishes. A sunset
//     date left behind in the global store would be read by every later test in this package
//     that asks WebhookSunsetPassed — including the ones asserting the unconfigured default —
//     and the failure would appear in a test that never mentioned a sunset.
//
// The field set is otherwise storeWebhookTestConfig's. This ADDS a field to a configuration
// literal local to this file; it changes no field's name, type or meaning, so the configuration
// shape the rest of the suite constructs is untouched.
//
// Parameters:
//   - t *testing.T: the test, for Helper and Cleanup registration.
//   - url string: the webhook receiver URL, or "" for the unconfigured no-op contract.
//   - secret string: the signing secret, or "" for unsigned delivery.
//   - sunset time.Time: the sunset instant. Whole seconds only — time.RFC3339 carries no
//     fractional part, so a sub-second offset would be silently lost in formatting.
//
// Returns:
//   - *config.Configuration: the published configuration.
//   - string: the unique webhook queue name, so concurrent runs cannot collide on the shared
//     Redis instance.
func storeDualDeliveryWebhookConfig(
	t *testing.T,
	url, secret string,
	sunset time.Time,
) (*config.Configuration, string) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. An empty configuration is strictly closer to
		// that than one carrying a sunset date.
		config.ConfigStore.Store(&config.Configuration{})
	})

	queueName := legacyWebhookUniqueQueueName(t, "dual_delivery_q")
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: legacyWebhookRedisAddr(t)},
		Server: config.ServerConfig{
			SecretKey: secret,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: url,
				// The dual-delivery fixtures deliver to an httptest server on loopback over
				// http, which is exactly the assertion this flag makes: the destination is on
				// a network the operator owns. It opens loopback and http and nothing else —
				// the redirect refusal stays unconditional and link-local, metadata,
				// multicast, unspecified and NAT64-wrapped addresses stay refused with it set.
				AllowPrivateDestination: true,
			},
		},
		WebhookDeprecationSunsetDate: sunset.Format(time.RFC3339),
	}
	config.ConfigStore.Store(cnf)

	return cnf, queueName
}

// dualDeliveryRelay is a relay whose Kafka and database seams are substituted while its LEGACY
// seam is the real thing.
//
// The row's own fields are carried alongside the fakes so an assertion can name the claimed
// row's event id, topic, payload and id without re-deriving any of them — the point of every
// test below is that both transports came from ONE row, and re-deriving a value would let an
// assertion pass against a row the relay never saw.
type dualDeliveryRelay struct {
	processor *EventRelayProcessor
	store     *relayFakeStore
	publisher *relayFakePublisher

	// The claimed row, unpacked. rowID is the outbox primary key the two markers are recorded
	// against; payload is the stored legacy body; topic is the destination the row itself
	// recorded.
	eventID string
	topic   string
	payload []byte
	rowID   int64
}

// newDualDeliveryRelay builds a relay holding exactly one claimable outbox row, wired to
// publish through a fake Kafka publisher and to deliver through the real legacy transport.
//
// WHAT IS SUBSTITUTED, AND WHAT IS EMPHATICALLY NOT. The store and the publisher are fakes
// because the alternative is a database and a broker for a test about an HTTP transport, and
// event_relay_test.go already owns the relay's behaviour against them. Two things are left
// exactly as NewEventRelayProcessor assigned them, and they are the reason these tests exist:
//
//   - processor.legacy is the real *Blnk, so EnqueueLegacyWebhookDelivery, the asynq client,
//     the task identity, the retention and the queue routing are all production code.
//   - processor.sunsetPassed is event_sunset.go's WebhookSunsetPassed, so the dual-delivery
//     branch is decided by the configured date through the single decision point rather than by
//     a stub that could disagree with it.
//
// The clock is pinned to relayFixedNow — the package's fixed test instant, which the fake store
// also uses — so the sunset verdict is a deterministic comparison against the configured date
// rather than a race with the wall clock.
//
// Parameters:
//   - t *testing.T: the test, for Helper and assertions about the wiring.
//   - b *Blnk: the real instance, built AFTER the configuration was published so that its
//     cached configuration carries the test's webhook URL and queue.
//   - eventID string: the outbox event id, which becomes the legacy task's identity.
//
// Returns:
//   - *dualDeliveryRelay: the processor and the fakes, with the claimed row unpacked.
func newDualDeliveryRelay(t *testing.T, b *Blnk, eventID string) *dualDeliveryRelay {
	t.Helper()

	// A transaction event on one ledger: the common case, and the one whose ordering guarantee
	// the partition key carries. relayRow's payload is a marshaled NewWebhook, so the row holds
	// the bytes a real ledger mutation would have recorded.
	row := relayRow(1, eventID, "transaction.applied", "ldg_dual_delivery", relayFixedNow)

	// The stored payload is deliberately one that CANNOT survive a decode/re-marshal round trip
	// — non-alphabetical keys, a large integer identifier, a trailing-zero decimal. See
	// nonCanonicalLegacyBody. relayRow's own payload is canonical, so a chain that re-serialised
	// the body somewhere between the outbox row and the socket would carry it unchanged and this
	// harness would prove nothing about byte fidelity. With this body, any re-serialisation
	// anywhere along claim → enqueue → handler → socket shows up as a byte difference.
	row.Payload = nonCanonicalLegacyBody()

	store := newRelayFakeStore(row)
	publisher := &relayFakePublisher{}

	processor := NewEventRelayProcessor(b)
	processor.store = store
	processor.publisher = publisher
	processor.deadLetters = &relayFakeDeadLetterer{}
	processor.now = func() time.Time { return relayFixedNow }

	// The wiring these tests depend on is asserted rather than assumed. If a future change to
	// NewEventRelayProcessor stopped adopting the instance as the legacy transport, every test
	// below would still pass — against nothing — because a nil legacy seam is a silent no-op.
	legacy, ok := processor.legacy.(*Blnk)
	require.True(t, ok,
		"the relay's legacy transport must be the *Blnk instance; these tests assert the real "+
			"asynq enqueue rather than a fake, and a substituted seam would make them vacuous")
	require.Same(t, b, legacy,
		"the relay must deliver through the very instance whose configuration and asynq client this test set up")

	return &dualDeliveryRelay{
		processor: processor,
		store:     store,
		publisher: publisher,
		eventID:   row.EventID,
		topic:     row.Topic,
		payload:   row.Payload,
		rowID:     row.ID,
	}
}

// pendingLegacyDeliveries lists the tasks waiting on queueName.
//
// It is the dual-delivery tests' name for listLegacyPendingTasks, kept because those tests read
// better with it. It USED to swallow any listing error as "the queue is empty", on the argument
// that asynq creates a queue lazily so a missing queue and an empty one are the same
// observation. That argument is right about a missing queue and wrong about everything else: a
// Redis that was down, a wrong address or a permission error read as "nothing was enqueued"
// too, which is the exact conclusion the post-sunset case draws. Only ErrQueueNotFound is
// accepted now, and every other failure is reported.
//
// Parameters:
//   - t *testing.T: the test, for Helper and diagnostics.
//   - inspector *asynq.Inspector: the inspector to list through.
//   - queueName string: the queue to list.
//
// Returns:
//   - []*asynq.TaskInfo: the pending tasks; empty when the queue does not exist.
func pendingLegacyDeliveries(t *testing.T, inspector *asynq.Inspector, queueName string) []*asynq.TaskInfo {
	t.Helper()

	return listLegacyPendingTasks(t, inspector, queueName)
}

// TestProcessWebhook_DeliversFromDualDeliveryBranch is the end-to-end proof that the legacy
// transport is now driven by the relay off a claimed outbox row.
//
// It starts where production starts — one row in blnk.event_outbox — and finishes where
// production finishes: a signed HTTP request at a subscriber. Nothing in between is fabricated
// by the test. Nothing calls SendWebhook, nothing calls EnqueueLegacyWebhookDelivery directly,
// and no domain post-action is involved, because none of those is how a webhook is produced any
// more.
//
// The chain it closes, link by link:
//
//	claimed row → relay's dual-delivery branch → EnqueueLegacyWebhookDelivery → real asynq
//	queue → the task the relay actually produced → ProcessWebhook → signed POST at the receiver
//
// The handler is driven with the task READ BACK FROM THE QUEUE rather than one the test builds,
// which is what makes this an end-to-end assertion rather than two half-tested halves: a body
// mangled by the enqueue, a task type the worker mux could not route, or an identity the queue
// rejected would all surface here.
func TestProcessWebhook_DeliversFromDualDeliveryBranch(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	const secret = "dual-delivery-signing-secret"

	// Fifteen days into a thirty-day window: unambiguously inside it, and not on the boundary
	// that TestDualDeliveryBranch_EnqueuesNothingOnceTheSunsetHasPassed owns.
	cnf, queueName := storeDualDeliveryWebhookConfig(t, server.URL, secret, relayFixedNow.Add(15*24*time.Hour))

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// The HANDLER's clock, pinned to the relay's. ProcessWebhook enforces the sunset at
	// execution time (Q4-09), and this fixture expresses its window relative to relayFixedNow
	// — so leaving the handler on the wall clock would have it judge the window closed while
	// the relay judged it open, and the delivery this test exists to observe would never be
	// attempted. Both legs reading one instant is also what makes "the handler and the relay
	// agree" a property of the test rather than an accident of when the suite runs.
	b.legacyWebhookNow = func() time.Time { return relayFixedNow }

	relay := newDualDeliveryRelay(t, b, gofakeit.UUID())

	// The fixture must describe the window it claims to. Asserting the real predicate here means
	// a mis-typed date cannot turn this into an accidental post-sunset test that passes because
	// nothing was delivered at all.
	require.False(t, WebhookSunsetPassed(relayFixedNow),
		"the pinned clock must fall inside the dual-delivery window for this test to have a legacy leg to assert")

	// ONE claim of ONE row is the entire input.
	require.Equal(t, 1, relay.processor.processBatch(context.Background()),
		"the relay must claim and process the single pending row")

	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1,
		"the dual-delivery branch must enqueue exactly one legacy delivery for the claimed row")

	task := tasks[0]
	assert.Equal(t, cnf.Queue.WebhookQueue, task.Type,
		"the task type must be the webhook queue name; the worker mux dispatches on the type, so any other value expires unhandled")
	assert.Equal(t, queueName, task.Queue)
	assert.Equal(t, legacyWebhookTaskID(relay.eventID), task.ID,
		"the task identity must be the claimed row's event id, namespaced for the shared queue")

	// The handler now runs against the task the relay produced.
	require.NoError(t, b.ProcessWebhook(context.Background(), asynq.NewTask(task.Type, task.Payload)))

	reqs := received()
	require.Len(t, reqs, 1, "one claimed row must produce exactly one HTTP delivery")
	req := reqs[0]

	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "application/json", req.headers.Get("Content-Type"))

	// The body a subscriber receives is the payload stored in the outbox row, unchanged. This is
	// an assertion about ONE transport carrying the row's bytes to the socket; the comparison
	// BETWEEN the two transports is criterion V-8 and belongs to event_dual_delivery_test.go.
	assert.Equal(t, string(relay.payload), string(req.body),
		"the delivered body must be the stored outbox payload verbatim, with no key reordering and no number re-rendering")

	// The envelope is still the frozen two-key contract, read from the wire rather than from the
	// struct that produced it.
	var envelope map[string]interface{}
	require.NoError(t, json.Unmarshal(req.body, &envelope))
	assert.Equal(t, "transaction.applied", envelope["event"],
		"the event name must survive the outbox and the queue unchanged")
	assert.Contains(t, envelope, "data",
		"the data key is the frozen payload contract every subscriber parser reads")

	// The signature is recomputed here from the timestamp and body the receiver was handed,
	// exactly as a subscriber would, rather than being read back from the implementation.
	timestamp := req.headers.Get("X-Blnk-Timestamp")
	require.NotEmpty(t, timestamp, "X-Blnk-Timestamp must be set when a secret is configured")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "." + string(req.body)))
	assert.Equal(t, hex.EncodeToString(mac.Sum(nil)), req.headers.Get("X-Blnk-Signature"),
		"a relay-driven delivery must be signed over timestamp.body exactly as a directly enqueued one is")
}

// TestDualDeliveryBranch_OneClaimedRowFeedsBothTransports asserts the SHARED PROVENANCE that
// makes payload equivalence structural rather than procedural.
//
// The dual-delivery guarantee is not upheld by two code paths being careful to agree. It is
// upheld because there is only one source: a single claimed row, read once, feeding both legs.
// Two transports reading one row cannot describe two different events, and no amount of drift in
// either path can make them.
//
// So what is asserted here is the JOIN, not the bytes: one claim, one Kafka publish carrying that
// row's event id and the topic the row itself recorded, one legacy task whose identity is that
// same event id, and both legs recorded against the same outbox row under the same claim token.
// Deep byte-for-byte equality between the two transports is criterion V-8 and is owned by
// event_dual_delivery_test.go; duplicating it here would assert the same fact twice and leave
// the provenance — the reason it holds — asserted nowhere.
func TestDualDeliveryBranch_OneClaimedRowFeedsBothTransports(t *testing.T) {
	// No receiver is needed: this test is about what the relay produces, not about delivery. The
	// URL only has to be non-empty, because an empty one is the no-op contract.
	cnf, queueName := storeDualDeliveryWebhookConfig(
		t, "http://localhost:1/never-called", "secret", relayFixedNow.Add(15*24*time.Hour),
	)

	inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	relay := newDualDeliveryRelay(t, b, gofakeit.UUID())

	require.Equal(t, 1, relay.processor.processBatch(context.Background()))

	// ONE claim served the batch. Both legs below therefore descend from one read of one row,
	// which is the property everything else here depends on.
	require.Len(t, relay.store.snapshotClaims(), 1,
		"one batch must be claimed once; a second claim would mean the two legs could descend from different reads")

	// The Kafka leg.
	publishes := relay.publisher.snapshotRequests()
	require.Len(t, publishes, 1, "the claimed row must be published exactly once")
	assert.Equal(t, relay.eventID, publishes[0].Event.EventID,
		"the published event id must be the claimed row's")
	assert.Equal(t, relay.topic, publishes[0].Topic,
		"the destination must be the topic the row recorded, not one re-derived at publish time")

	// The legacy leg, tied to the Kafka leg through the event id rather than through anything the
	// test supplied.
	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the same row must produce exactly one legacy delivery")
	assert.Equal(t, legacyWebhookTaskID(publishes[0].Event.EventID), tasks[0].ID,
		"the legacy task's identity must be derived from the very event id that was published to Kafka")

	// Both legs are recorded against the SAME ROW under the SAME CLAIM TOKEN. The token is what
	// makes this a property of the schema: a leg recorded under a stale token is refused by the
	// repository, so two legs sharing one token cannot have come from two different claims.
	marks := relay.store.snapshotWebhookMarks()
	require.Len(t, marks, 1, "the legacy leg must be recorded exactly once")
	assert.Equal(t, relay.rowID, marks[0].id, "the legacy marker must name the claimed row")
	assert.NotEmpty(t, marks[0].claimToken, "the legacy marker must carry the claim token; an empty one would be unconditional")

	dispatched := relay.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "the Kafka leg must be recorded exactly once")
	assert.Equal(t, relay.rowID, dispatched[0].id, "the dispatch marker must name the same row")
	assert.Equal(t, marks[0].claimToken, dispatched[0].claimToken,
		"both legs must be recorded under one claim token, which is what proves they came from one claimed row")
}

// TestDualDeliveryBranch_EnqueuesNothingOnceTheSunsetHasPassed asserts the enqueue side of the
// sunset: after the window closes, no legacy task is produced at all.
//
// The three cases differ in ONE INPUT — the configured RFC3339 sunset date — and in nothing else.
// Same row, same relay wiring, same clock, same queue mechanics. That is what makes this a test
// of the sunset rather than a test of a relay that happened to stop working: the Kafka leg is
// asserted to run in every case, so the only thing the date changes is whether the deprecated
// transport is fed.
//
// The verdict is never recomputed here. Each case states what event_sunset.go's WebhookSunsetPassed
// must answer for the pinned clock and asserts it, so a fixture that has drifted from the case it
// is named for fails as a fixture rather than silently asserting the wrong behaviour. The boundary
// case — a sunset instant equal to the clock — pins the documented half-open window: the instant
// itself is the first moment of the post-sunset era, so dual delivery has already stopped there.
//
// Scope: this file owns the ENQUEUE side of the sunset. The HTTP 410 Gone answer on the deprecated
// webhook management routes is the other consumer of the same decision and is owned by
// api/webhook_sunset_test.go.
func TestDualDeliveryBranch_EnqueuesNothingOnceTheSunsetHasPassed(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		sunset       time.Time
		sunsetPassed bool
		wantEnqueued int
	}{
		{
			name:         "inside the window the legacy leg is still fed",
			sunset:       relayFixedNow.Add(24 * time.Hour),
			sunsetPassed: false,
			wantEnqueued: 1,
		},
		{
			// The sunset instant IS the first moment after the window, so a clock exactly on it
			// has already passed it. One second either side of this line is the whole difference
			// between an inclusive and an exclusive boundary.
			name:         "at the sunset instant it is not",
			sunset:       relayFixedNow,
			sunsetPassed: true,
			wantEnqueued: 0,
		},
		{
			name:         "long after the sunset it is not",
			sunset:       relayFixedNow.Add(-30 * 24 * time.Hour),
			sunsetPassed: true,
			wantEnqueued: 0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cnf, queueName := storeDualDeliveryWebhookConfig(
				t, "http://localhost:1/never-called", "secret", testCase.sunset,
			)

			inspector := newLegacyWebhookInspector(t, cnf.Redis.Dns, queueName)

			b, err := NewBlnk(nil)
			require.NoError(t, err)
			defer func() { _ = b.Close() }()

			relay := newDualDeliveryRelay(t, b, gofakeit.UUID())

			require.Equal(t, testCase.sunsetPassed, WebhookSunsetPassed(relayFixedNow),
				"the configured date %q must make the single sunset decision answer %v for the pinned clock",
				testCase.sunset.Format(time.RFC3339), testCase.sunsetPassed)

			require.Equal(t, 1, relay.processor.processBatch(context.Background()),
				"the row is claimed and processed in every case; only the legacy leg is conditional")

			assert.Len(t, pendingLegacyDeliveries(t, inspector, queueName), testCase.wantEnqueued,
				"the legacy webhook leg must be enqueued only while the sunset is in the future")

			// The row must not be marked as webhook-dispatched for a delivery that never happened:
			// a marker without a task would make the migration look further along than it is.
			assert.Len(t, relay.store.snapshotWebhookMarks(), testCase.wantEnqueued,
				"the legacy marker must be recorded exactly when a legacy task was enqueued, and never otherwise")

			// The KAFKA leg runs in every case. Without this, a relay that had simply stopped
			// publishing would satisfy the assertions above.
			assert.Len(t, relay.publisher.snapshotRequests(), 1,
				"the sunset retires the legacy transport only; Kafka publishing is unaffected by it")
			dispatched := relay.store.snapshotDispatched()
			require.Len(t, dispatched, 1, "the row must reach its dispatched terminal state in every case")
			assert.Equal(t, relay.rowID, dispatched[0].id)
		})
	}
}

// ===== THE TRANSACTION EVENT VOCABULARY (AND ONE DEFECT PRESERVED ON PURPOSE) =====
//
// getEventFromStatus is declared in webhooks.go, and seven of the thirteen event names Blnk emits
// originate there. Those names are not internal labels: each one is the value of the "event" key
// in the body every subscriber parses, each is duplicated into the LedgerEvent envelope's
// event_type, and each decides which Kafka topic the event is published to. Change one and a
// subscriber's routing changes with it.
//
// The names are asserted here because this file is the home of the payload contract until the
// sunset relocates it. event_topics_test.go asserts the same mapping from the ROUTING side — that
// every name reaches the transactions topic — and model/event.go owns the table itself. The
// overlap is deliberate: this is the vocabulary the whole feature is built on, and the value it
// most needs is that no single edit can change it without something failing.

// TestGetEventFromStatus_PinsTheTransactionEventVocabulary pins every transaction event name
// value by value, and pins them through the wire envelope that carries them.
//
// Exact values rather than a shape. "Something starting with transaction." is not the contract: a
// separator change, a tense change or a dropped prefix all produce a plausible-looking name, all
// pass a loose assertion, and all silently stop matching what a subscriber filters on.
//
// The statuses come from the constants rather than being re-spelled, so a change to a status value
// is carried into this test instead of being hidden behind a stale literal.
func TestGetEventFromStatus_PinsTheTransactionEventVocabulary(t *testing.T) {
	for _, testCase := range []struct {
		status    string
		eventType string
	}{
		{status: StatusQueued, eventType: "transaction.queued"},
		{status: StatusApplied, eventType: "transaction.applied"},
		{status: StatusScheduled, eventType: "transaction.scheduled"},
		{status: StatusInflight, eventType: "transaction.inflight"},
		{status: StatusVoid, eventType: "transaction.void"},
		{status: StatusRejected, eventType: "transaction.rejected"},
	} {
		t.Run(testCase.status, func(t *testing.T) {
			assert.Equal(t, testCase.eventType, getEventFromStatus(testCase.status),
				"the %q status must produce the %q event name", testCase.status, testCase.eventType)

			// Asserted through the frozen envelope, because the name's only purpose is to be the
			// "event" value a subscriber reads. Pinning it in isolation would leave the envelope
			// free to rename the key that carries it.
			body, err := json.Marshal(NewWebhook{
				Event:   getEventFromStatus(testCase.status),
				Payload: map[string]string{"transaction_id": "txn_vocabulary"},
			})
			require.NoError(t, err)
			assert.JSONEq(t,
				`{"event":"`+testCase.eventType+`","data":{"transaction_id":"txn_vocabulary"}}`,
				string(body),
				"the marshaled envelope must carry the event name under the frozen \"event\" key alongside the \"data\" payload")
		})
	}

	// The mapping is case-insensitive, so the lower-cased spelling of a status must produce the
	// same name. Tying the two calls together rather than asserting a literal means this keeps
	// holding if a status value changes.
	assert.Equal(t, getEventFromStatus(StatusApplied), getEventFromStatus("applied"),
		"the status comparison is case-insensitive, so both spellings must produce one event name")

	// An unrecognised status still yields a real, routable transaction event rather than an empty
	// string. An empty event name would be published to a topic with a body no subscriber filter
	// matches, silently.
	assert.Equal(t, "transaction.unknown", getEventFromStatus("NO_SUCH_STATUS"),
		"an unrecognised status must fall through to transaction.unknown")
	assert.Equal(t, "transaction.unknown", getEventFromStatus(""),
		"an empty status must fall through to transaction.unknown rather than producing an empty event name")
}

// TestGetEventFromStatus_CommitFallsThroughToUnknown pins a DEFECT THAT IS PRESERVED ON PURPOSE.
// Do not "correct" the mapping to make this test read differently.
//
// StatusCommit ("COMMIT", declared in transaction_inflight.go and reached when an inflight
// transaction is committed) has no case in the mapping, so it falls through to the default and its
// event is named transaction.unknown.
//
// THIS IS PRE-EXISTING BEHAVIOUR, NOT A REGRESSION. It predates the Kafka event pipeline by a long
// way, and it is kept exactly as it is for one reason: the dual-delivery guarantee is measured by
// comparing the Kafka message and the legacy webhook BYTE FOR BYTE (criterion V-8), and adding a
// transaction.commit case would change one side of that comparison and fail it for a reason that
// has nothing whatever to do with the transport.
//
// It is documented in docs/event-streaming.md so that it can be corrected LATER as a separate,
// deliberately reviewed change — one that renames an event subscribers route on, and so belongs to
// nobody's incidental refactor.
//
// Without this test the trap is wide open: adding the missing case looks exactly like a bug fix,
// passes every other test in the repository, and silently breaks the dual-delivery comparison.
func TestGetEventFromStatus_CommitFallsThroughToUnknown(t *testing.T) {
	assert.Equal(t, "transaction.unknown", getEventFromStatus(StatusCommit),
		"COMMIT has no case in the mapping and must keep falling through to transaction.unknown; "+
			"see docs/event-streaming.md before changing this")
	assert.NotEqual(t, "transaction.commit", getEventFromStatus(StatusCommit),
		"adding a transaction.commit case would change one side of the byte-for-byte dual-delivery comparison")

	// The status value itself is pinned, so this cannot become a test about a status nothing emits.
	assert.Equal(t, "COMMIT", StatusCommit,
		"the preserved fall-through is about the COMMIT status specifically")
	assert.Equal(t, getEventFromStatus(StatusCommit), getEventFromStatus("COMMIT"),
		"the constant and its literal spelling must resolve identically")

	// transaction.unknown is therefore a REAL event name reached by a REAL status, not a defensive
	// default nothing produces — which is exactly why it must be treated as part of the vocabulary
	// rather than as an error case.
	assert.Equal(t, getEventFromStatus("NO_SUCH_STATUS"), getEventFromStatus(StatusCommit),
		"a committed transaction and an unrecognised status are indistinguishable on the wire today")

	// And it travels in the frozen envelope like any other name, which is the form in which the
	// dual-delivery comparison sees it.
	body, err := json.Marshal(NewWebhook{
		Event:   getEventFromStatus(StatusCommit),
		Payload: map[string]string{"transaction_id": "txn_commit"},
	})
	require.NoError(t, err)
	assert.JSONEq(t,
		`{"event":"transaction.unknown","data":{"transaction_id":"txn_commit"}}`,
		string(body),
		"a COMMIT transaction's webhook body carries transaction.unknown today, and both transports must agree on that")
}
