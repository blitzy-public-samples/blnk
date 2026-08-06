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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/config"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// storeWebhookTestConfig stores a configuration pointing the webhook
// notification at url, with a unique webhook queue per test to avoid
// cross-test interference on the shared Redis instance.
func storeWebhookTestConfig(url, secret string, headers map[string]string) (*config.Configuration, string) {
	queueName := fmt.Sprintf("webhook_q_%d", time.Now().UnixNano())
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: "localhost:6379"},
		Server: config.ServerConfig{
			SecretKey: secret,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url:     url,
				Headers: headers,
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
	storeWebhookTestConfig(server.URL, secret, map[string]string{
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
	storeWebhookTestConfig(server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", []byte(`{"event": "broken`)))
	assert.Error(t, err, "corrupt task payload must surface an error so asynq can retry/dead-letter")
	assert.Empty(t, received(), "no HTTP call should be made for an unparseable payload")
}

func TestProcessWebhook_NoURLConfiguredIsNoOp(t *testing.T) {
	storeWebhookTestConfig("", "secret", nil)

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

	storeWebhookTestConfig(deadURL, "secret", nil)

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
	storeWebhookTestConfig(server.URL, "secret", nil)

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
	cnf, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

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
	_, queueName := storeWebhookTestConfig("", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	require.NoError(t, b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}}))

	tasks, err := inspector.ListPendingTasks(queueName)
	if err == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when no webhook URL is configured")
	}
	// err != nil means the queue does not exist at all, which is equally correct.
}

func TestSendWebhook_UnmarshalablePayloadReturnsError(t *testing.T) {
	_, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// channels cannot be JSON-marshaled
	err = b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: make(chan int)})
	assert.Error(t, err)

	tasks, listErr := inspector.ListPendingTasks(queueName)
	if listErr == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when payload serialization fails")
	}
}

func TestProcessWebhook_NoSignatureHeadersWithEmptySecret(t *testing.T) {
	// Regression: an empty Server.SecretKey used to produce an HMAC over the
	// empty key — computable (and forgeable) by anyone. The delivery is now
	// sent unsigned so receivers cannot mistake a forgeable header for
	// authentication.
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(server.URL, "", nil)

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
	cnf, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

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
	storeWebhookTestConfig(server.URL, secret, nil)

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
	_, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

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
	_, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

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
		tasks, err := inspector.ListPendingTasks(queueName)
		if err == nil {
			assert.Empty(t, tasks)
		}
	})
}

// TestEnqueueLegacyWebhookDelivery_NoURLConfiguredSkipsEnqueue pins the same
// no-op-when-unconfigured contract SendWebhook has.
//
// It is load-bearing rather than defensive: a deployment with no webhook URL runs the Kafka
// leg alone, and an error here would fail the relay's dual-delivery branch on every event for
// a transport the operator deliberately did not configure.
func TestEnqueueLegacyWebhookDelivery_NoURLConfiguredSkipsEnqueue(t *testing.T) {
	_, queueName := storeWebhookTestConfig("", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// The event id and body are deliberately valid, so the no-op is the reason nothing is
	// enqueued rather than a validation failure standing in for it.
	require.NoError(t, b.EnqueueLegacyWebhookDelivery(gofakeit.UUID(), nonCanonicalLegacyBody()))

	tasks, err := inspector.ListPendingTasks(queueName)
	if err == nil {
		assert.Empty(t, tasks, "nothing may be enqueued when no webhook URL is configured")
	}
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
