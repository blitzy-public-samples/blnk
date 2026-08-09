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

// Dual-delivery payload equivalence — acceptance criterion V-8.
//
// The criterion is that, during the dual-delivery window, the Kafka message and the legacy HTTP
// webhook carry THE SAME PAYLOAD for the same event, asserted byte for byte. It holds
// STRUCTURALLY: the relay claims one blnk.event_outbox row and drives both transports from it,
// so the Kafka message value and the legacy body are the same slice and there is no second
// serialisation for them to drift apart through.
//
// The regression it exists to catch is a future change that REBUILDS THE LEGACY PAYLOAD FROM THE
// DOMAIN OBJECT instead of from the row: SendWebhook still takes a NewWebhook struct, so "just
// call SendWebhook from the relay" reads like a simplification. It is not, and the two bodies
// would stay SEMANTICALLY EQUAL after it — which is why every equality assertion here is on
// []byte or its string form and never on an unmarshalled map.
//
// # Which seams are real, and which are substituted
//
// Real, and each touches the bytes: PublishEvent builds the row, the real EventRelayProcessor
// claims it, the real EnqueueLegacyWebhookDelivery puts it on a real asynq queue (miniredis), the
// real ProcessWebhook takes it off again, and the real processHTTPRaw signs and posts it. Because
// the legacy body is serialised into Redis and read back out, its equality with the Kafka value
// cannot be an artefact of two slices sharing a backing array.
//
// Substituted:
//
//   - THE BROKER, by a publisher recording the request it was handed, passed through the
//     production marshalLedgerEvent so assertions are made against the bytes a live writer would
//     put on the wire. The wire itself is not covered here; a live publish is
//     event_ordering_integration_test.go's and event_isolation_integration_test.go's subject.
//   - THE OUTBOX TABLE, by the relay's four-method store seam, which leases out the row
//     PublishEvent actually produced. So THIS FILE DOES NOT PROVE THAT POSTGRESQL PERSISTS AND
//     RETURNS THE BYTES UNCHANGED — that link is database/event_outbox_test.go's, in
//     TestInsertEventOutboxInTx_PayloadBytesPassThroughUnmodified and
//     TestInsertEventOutbox_PreservesAHostilePayloadSpellingByteForByte, which round-trip a
//     hostile payload through the payload_raw BYTEA column.
//
// Scope is the CROSS-TRANSPORT comparison alone: the retry schedule and relay lifecycle are
// event_relay_test.go's, the 410 Gone routes api/webhook_sunset_test.go's, replay fidelity
// event_replay_fidelity_test.go's, and the legacy path's single-transport behaviour
// webhooks_test.go's and webhooks_process_test.go's.
package blnk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/ast"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/hibiken/asynq"
	"github.com/jarcoal/httpmock"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// dualDeliveryFixedNow is the instant the relay's clock is pinned to.
//
// It is pinned so the sunset boundary is exact rather than approximately now: every
// configured window below is expressed as an offset from this instant, and the verdict is
// then taken by event_sunset.go against it. Pinning the CLOCK rather than stubbing the
// DECISION is what keeps the real predicate under test.
var dualDeliveryFixedNow = time.Date(2026, time.June, 1, 9, 0, 0, 0, time.UTC)

// dualDeliveryWebhookURL is the legacy subscriber endpoint the harness configures.
//
// The .invalid TLD is reserved by RFC 2606 and can never resolve, so a request that
// escaped the httpmock transport would fail loudly instead of reaching something real.
const dualDeliveryWebhookURL = "https://subscriber.dual-delivery.invalid/webhooks/blnk"

// dualDeliverySigningSecret is the server secret the legacy transport signs with.
//
// It is deliberately a long, self-describing, obviously-fake string: it names itself as a
// test value so it cannot be mistaken for a leaked credential, and it matches no provider's
// key format.
const dualDeliverySigningSecret = "blnk-dual-delivery-test-signing-key-not-a-real-secret"

// The configured transport header, which the legacy contract applies to every delivery.
const (
	dualDeliveryHeaderName  = "X-Blnk-Subscriber"
	dualDeliveryHeaderValue = "subscriber-dual-delivery"
)

// dualDeliveryKafkaBroker is a broker address that is never dialled.
//
// Its only job is to make eventPublishingConfigured answer true, which is what enables
// event capture at all. Nothing in this file connects to it: the publisher seam is
// substituted, and the publisher construction path performs no I/O in any case.
//
// An unqualified single-label name, not the qualified .invalid name it used to be. Both
// fail to resolve, which is all this constant needs, but a qualified name outside Blnk's
// own network is no longer reachable over acknowledged plaintext — see
// requireLocalBrokersForPlaintext. A single-label name is what a Compose service name is,
// so the locality check treats it as internal while it still names nothing.
const dualDeliveryKafkaBroker = "kafka-dual-delivery-does-not-exist:9092"

// dualDeliveryOutboxRowID is the surrogate key the harness stamps on a captured row.
//
// PrepareEventOutbox leaves ID zero because blnk.event_outbox.id is a BIGSERIAL assigned by
// the database. The relay identifies a row by that id in every transition it drives, so the
// harness assigns one to stand in for the INSERT ... RETURNING the repository performs.
const dualDeliveryOutboxRowID int64 = 1

// dualDeliveryFirstClaimToken is the token the fake store mints for the first claim.
//
// It is spelled out so the marker assertions can state that the legacy leg was recorded
// UNDER THE CLAIM STILL HELD, rather than merely that some token was passed.
const dualDeliveryFirstClaimToken = "token-1"

// dualDeliverySunsetIn renders a sunset date offset from the pinned clock, in the RFC3339
// form WEBHOOK_DEPRECATION_SUNSET_DATE is documented to take.
//
// A positive offset puts the sunset in the future, which is the dual-delivery window; a
// non-positive one puts it at or behind the clock, which is the post-sunset era. THIS
// FUNCTION MAKES NO DECISION — it only writes a date. Whether that date has passed is
// event_sunset.go's judgement, taken through the relay, and this file never forms an
// opinion of its own about it.
func dualDeliverySunsetIn(offset time.Duration) string {
	return dualDeliveryFixedNow.Add(offset).Format(time.RFC3339)
}

// dualDeliveryWindowOpen is a sunset comfortably inside the 30-day window.
//
// It is derived from config.WebhookDualDeliveryWindowDays rather than written as a literal
// number of hours, so it stays inside the window the configuration loader enforces even if
// that constant is ever revisited.
func dualDeliveryWindowOpen() string {
	return dualDeliverySunsetIn(config.WebhookDualDeliveryWindowDays * 24 * time.Hour / 2)
}

// dualDeliveryWindowClosed is a sunset one nanosecond behind the pinned clock.
//
// One nanosecond, not one day, because the boundary is the interesting part: the sunset
// instant is documented as the FIRST MOMENT of the post-sunset era, so the window is
// half-open and excludes it. A generous offset would pass even if that boundary were
// inclusive by accident.
func dualDeliveryWindowClosed() string {
	return dualDeliverySunsetIn(-time.Nanosecond)
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

// dualDeliveryHTTPCapture is everything the legacy subscriber saw for one delivery.
//
// The body is kept as raw bytes and never decoded on the way in, because decoding is the one
// thing that would destroy the property under test.
type dualDeliveryHTTPCapture struct {
	// body is the request body exactly as it arrived.
	body []byte
	// headers is a clone of the request headers, so the signing block and the configured
	// transport headers can both be asserted.
	headers http.Header
	// method is the HTTP method, which the legacy contract fixes as POST.
	method string
	// url is the endpoint the delivery was addressed to.
	url string
}

// dualDeliveryObservation is one event's journey down BOTH transports, with every byte
// sequence involved kept side by side so the comparison is a direct one.
type dualDeliveryObservation struct {
	// row is the outbox row PublishEvent produced — the single source both transports read.
	row *model.EventOutbox

	// kafkaRequest is the request the relay handed the publisher.
	kafkaRequest PublishRequest
	// kafkaPayload is the payload field of that request: the bytes that become the
	// envelope's "payload" member.
	kafkaPayload []byte
	// kafkaMessage is the serialised envelope a live kafka.Writer would produce for that
	// request, produced by the production marshalLedgerEvent so the assertion is against
	// the real wire form rather than a test's idea of it.
	kafkaMessage []byte

	// queuedPayload is the asynq task payload read back out of Redis: the legacy body after
	// a genuine round trip through the queue.
	queuedPayload []byte
	// queuedTaskType and queuedTaskQueue are the task's type and queue, which the legacy
	// contract requires to be the same string.
	queuedTaskType  string
	queuedTaskQueue string

	// http is the delivery the subscriber received.
	http dualDeliveryHTTPCapture
}

// dualDeliveryHarness wires the real capture path, the real relay, the real legacy transport
// and a real asynq queue together, substituting only the broker and the outbox table.
//
// One harness per test. It owns process-global state — the configuration store, the httpmock
// transport registry and a miniredis instance — so nothing here may run in parallel, and
// every piece of that state is restored through t.Cleanup.
type dualDeliveryHarness struct {
	t *testing.T

	// configuration is the published configuration, kept so a test can assert against the
	// values the transports actually read.
	configuration *config.Configuration
	// queueName is this harness's webhook queue, unique per harness so two tests cannot see
	// each other's tasks.
	queueName string

	// blnk is the service container: real config, real asynq client, real HTTP client, and
	// the spy datasource the capture path writes its row to.
	blnk *Blnk
	// spy records the outbox insert, which is how the harness gets hold of the row.
	spy *outboxSpyDatasource
	// redis is the in-process Redis backing the asynq queue.
	redis *miniredis.Miniredis
	// inspector reads tasks back out of that queue.
	inspector *asynq.Inspector

	// relay is the real EventRelayProcessor, wired by NewEventRelayProcessor so its sunset
	// decision, its retry schedule and its legacy transport are the production ones.
	relay *EventRelayProcessor
	// store is the substituted outbox table.
	store *relayFakeStore
	// kafka is the substituted broker.
	kafka *relayFakePublisher

	// mu guards the two fields below. The responder runs on whichever goroutine issued the
	// request, and the relay publishes from a worker pool, so unguarded access is a race the
	// detector fails the suite on.
	mu sync.Mutex
	// deliveries is every captured HTTP delivery, in arrival order.
	deliveries []dualDeliveryHTTPCapture
	// responseStatus is what the subscriber answers. Tests set it to a failure code to prove
	// a legacy failure cannot reach the Kafka leg.
	responseStatus int
}

// dualDeliveryWebhookQueue is the webhook queue every harness configures.
//
// It is a fixed name rather than a generated one because isolation comes from somewhere
// better: each harness runs its own miniredis, so one harness's queue is in a different
// Redis from every other's. A fixed name also lets the wire-contract assertions read as the
// contract itself — the asynq task type and the queue name are deliberately the same string,
// and that is easier to see when the string is named once.
const dualDeliveryWebhookQueue = "dual_delivery_webhook_queue"

// newDualDeliveryHarness builds the full dual-delivery path with the given sunset date.
//
// The date is a PARAMETER rather than a fixed value because both sides of the boundary are
// under test, and it is handed to configuration rather than to a stub: the relay takes its
// verdict from event_sunset.go, reading this configuration, exactly as it does in
// production. Pass dualDeliveryWindowOpen() for the window, dualDeliveryWindowClosed() for
// the post-sunset era, or the empty string for the unusable window that fails closed.
//
// Everything the harness allocates is released through t.Cleanup — the configuration store
// is restored, the httpmock transport is deactivated and its counters reset, miniredis is
// stopped and both asynq connections are closed — so a failing test leaks nothing into the
// next one.
func newDualDeliveryHarness(t *testing.T, sunsetDate string) *dualDeliveryHarness {
	t.Helper()

	redisServer, err := miniredis.Run()
	require.NoError(t, err, "the in-process Redis backing the legacy webhook queue must start")
	t.Cleanup(redisServer.Close)

	// Both transports are configured, which is what makes this a DUAL-delivery harness:
	// Kafka.Brokers is what enables event capture at all, and Notification.Webhook.Url is
	// what stops EnqueueLegacyWebhookDelivery taking its no-op-when-unconfigured branch.
	configuration := &config.Configuration{
		Redis:  config.RedisConfig{Dns: redisServer.Addr()},
		Server: config.ServerConfig{SecretKey: dualDeliverySigningSecret},
		Queue: config.QueueConfig{
			WebhookQueue:   dualDeliveryWebhookQueue,
			NumberOfQueues: 1,
		},
		Kafka: config.KafkaConfig{Brokers: []string{dualDeliveryKafkaBroker}},
		Relay: config.RelayConfig{
			MaxRetryAttempts:   config.MaxRelayRetryAttempts,
			RetryBaseBackoffMS: 1000,
			RetryMaxBackoffMS:  30000,
		},
		Notification: config.Notification{Webhook: config.WebhookConfig{
			Url:     dualDeliveryWebhookURL,
			Headers: map[string]string{dualDeliveryHeaderName: dualDeliveryHeaderValue},
		}},
		WebhookDeprecationSunsetDate: sunsetDate,
	}

	// Published to the process-global store and restored afterwards. The store is what
	// event_sunset.go reads, what ProcessWebhook re-reads at delivery time, and what topic
	// naming resolves through, so the harness cannot get away with the instance copy alone.
	outboxStoreConfiguration(t, configuration)

	// The malformed/absent-window warning is suppressed after its first occurrence, process
	// wide. Resetting the guard makes the fail-closed test's log assertion independent of
	// whether an earlier test already tripped it.
	sunsetParseWarnings.reset()

	asynqClient := asynq.NewClient(asynq.RedisClientOpt{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = asynqClient.Close() })

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = inspector.Close() })

	// The REAL pooled client the legacy transport uses, with its transport intercepted.
	// Intercepting rather than starting a listener keeps the request object intact — every
	// header the signing block set is still on it — while giving an exact call counter, which
	// is what the post-sunset assertion needs: "no HTTP request was made" has to be a count
	// of zero, not an empty body.
	httpClient := initializeHTTPClient()
	httpmock.ActivateNonDefault(httpClient)
	t.Cleanup(httpmock.DeactivateAndReset)
	httpmock.ZeroCallCounters()

	harness := &dualDeliveryHarness{
		t:              t,
		configuration:  configuration,
		queueName:      configuration.Queue.WebhookQueue,
		spy:            newOutboxSpyDatasource(),
		redis:          redisServer,
		inspector:      inspector,
		kafka:          &relayFakePublisher{},
		responseStatus: http.StatusOK,
	}

	// The service container is assembled directly rather than through NewBlnk so that no
	// TypeSense client, hook manager, queue or cache is constructed: none of them is on the
	// path under test, and none should be able to make a byte comparison depend on
	// infrastructure. Every field that IS on the path is the real thing.
	//
	// events is set BEFORE the relay is built, because NewEventRelayProcessor adopts it as
	// the relay's publisher and builds the dead-letter service from it. Setting it here
	// rather than assigning processor.publisher afterwards means the relay is wired exactly
	// as the server role wires it, and the only difference from production is which
	// EventPublisher implementation is behind the interface.
	harness.blnk = &Blnk{
		config:      configuration,
		datasource:  harness.spy,
		asynqClient: asynqClient,
		httpClient:  httpClient,
		events:      harness.kafka,
		// THE HANDLER'S CLOCK, PINNED TO THE SAME INSTANT AS THE RELAY'S. Both legs must judge
		// the same window, and ProcessWebhook re-reads the sunset at DELIVERY time — correctly,
		// so that a deployment which has passed its sunset since a task was queued drops it
		// rather than delivering to a retired transport. Left at the wall clock here, the relay
		// would enqueue inside the window the configuration describes and the handler would then
		// drop the task as retired, because the configured date is relative to
		// dualDeliveryFixedNow and not to today.
		//
		// Only the CLOCK is injected. WebhookSunsetPassed stays the single decision point both
		// the relay and the handler read, so this makes the two agree about the hour without
		// letting either disagree about the rule.
		legacyWebhookNow: func() time.Time { return dualDeliveryFixedNow },
	}

	harness.relay = NewEventRelayProcessor(harness.blnk)

	// The outbox table, substituted. The relay's constructor pointed store at the datasource;
	// the spy carries no expectation for the claim, so leaving it would fail loudly rather
	// than silently — but the tests need a table that leases rows out, so it is replaced.
	harness.store = newRelayFakeStore()
	harness.store.now = func() time.Time { return dualDeliveryFixedNow }
	harness.relay.store = harness.store

	// The clock, pinned. sunsetPassed is deliberately NOT touched: it is
	// event_sunset.go's predicate, adopted by the constructor, and the whole point of
	// configuring a date above is that the real predicate decides.
	harness.relay.now = func() time.Time { return dualDeliveryFixedNow }

	httpmock.RegisterResponder(http.MethodPost, dualDeliveryWebhookURL, harness.respond)

	return harness
}

// newDualDeliveryHarnessInWindow is the common case: both transports live, the sunset
// comfortably in the future.
func newDualDeliveryHarnessInWindow(t *testing.T) *dualDeliveryHarness {
	t.Helper()

	return newDualDeliveryHarness(t, dualDeliveryWindowOpen())
}

// respond is the httpmock responder standing in for the legacy subscriber.
//
// It records the request verbatim and answers with the harness's configured status, which is
// how a receiver-side failure is simulated without changing anything about the request.
func (h *dualDeliveryHarness) respond(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}

	h.mu.Lock()
	status := h.responseStatus
	h.deliveries = append(h.deliveries, dualDeliveryHTTPCapture{
		body:    body,
		headers: request.Header.Clone(),
		method:  request.Method,
		url:     request.URL.String(),
	})
	h.mu.Unlock()

	return httpmock.NewStringResponse(status, `{"received":true}`), nil
}

// captureEvent runs the REAL capture path for one event and returns the row it produced.
//
// This is the producer's half of the pipeline, unmodified: PublishEvent is the one-for-one
// replacement for SendWebhook at all eight call sites, and it marshals the whole two-key
// NewWebhook envelope into the row's payload column. Nothing in this file constructs a row
// by hand, because a hand-built row could carry bytes the capture path would never have
// produced and the comparison would then be against a fiction.
//
// The surrogate key stands in for the BIGSERIAL the INSERT would have returned; the relay
// names a row by that id in every transition it drives.
func (h *dualDeliveryHarness) captureEvent(event NewWebhook, options ...EventOption) *model.EventOutbox {
	h.t.Helper()

	require.NoError(h.t, h.blnk.PublishEvent(context.Background(), event, options...),
		"capturing %q in the outbox must succeed", event.Event)

	rows := h.spy.standalone()
	require.Len(h.t, rows, 1,
		"exactly one outbox row must be written for one event; both transports read that one row")

	row := rows[0]
	row.ID = dualDeliveryOutboxRowID

	return row
}

// makeClaimable puts rows into the substituted outbox table's claimable set.
//
// It is separate from captureEvent because the restart test has to make the SAME row
// claimable a second time, which is what the database does when a lease expires.
func (h *dualDeliveryHarness) makeClaimable(rows ...model.EventOutbox) {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	h.store.pending = append(h.store.pending, rows...)
}

// relayOneBatch runs one claim-and-publish cycle and returns how many rows were claimed.
//
// processBatch is called directly rather than through Start so the cycle is synchronous and
// complete when it returns: it waits for its own worker pool, so every publish, every legacy
// enqueue and every transition has finished by the time the count is returned. A ticker-driven
// Start would make every assertion below a race against a background goroutine.
func (h *dualDeliveryHarness) relayOneBatch() int {
	h.t.Helper()

	return h.relay.processBatch(context.Background())
}

// soleKafkaRequest returns the one request the relay handed the publisher, failing if there
// is not exactly one.
//
// "Exactly one" is part of the assertion rather than a convenience: two requests for one
// event would mean the event was published twice, which is a duplicate the subscriber would
// have to suppress, and none would mean the Kafka leg silently did not run.
func (h *dualDeliveryHarness) soleKafkaRequest() PublishRequest {
	h.t.Helper()

	requests := h.kafka.snapshotRequests()
	require.Len(h.t, requests, 1, "exactly one Kafka publish must have been attempted")

	return requests[0]
}

// queuedTask reads the legacy task back OUT OF REDIS by its event-derived identity, which keeps
// the assertions independent of ordering and states the identity itself: the task is addressable
// as legacy-webhook:<event_id>, which is what lets asynq refuse a duplicate enqueue after a crash
// between enqueuing and marking the row.
//
// ONLY a missing task or a missing queue counts as absence — until the first enqueue the queue
// does not exist in Redis, and asynq reports the queue rather than the task. Any other error is a
// broken observation, not an absent task, and fails the test rather than being read as "not
// enqueued".
func (h *dualDeliveryHarness) queuedTask(eventID string) (*asynq.TaskInfo, bool) {
	h.t.Helper()

	info, err := h.inspector.GetTaskInfo(h.queueName, legacyWebhookTaskID(eventID))
	if errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound) {
		return nil, false
	}

	require.NoError(h.t, err, "the webhook queue must be inspectable; an unreadable task proves nothing")

	return info, true
}

// queuedTaskCount returns how many legacy deliveries are waiting on the webhook queue, treating
// ONLY a queue that does not exist as empty. Every other listing error fails the test.
func (h *dualDeliveryHarness) queuedTaskCount() int {
	h.t.Helper()

	tasks, err := h.inspector.ListPendingTasks(h.queueName)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		return 0
	}

	require.NoError(h.t, err, "the webhook queue must be listable; an unlistable queue proves nothing")

	return len(tasks)
}

// runWebhookWorker drives the legacy handler over a task read back out of Redis.
//
// This stands in for the asynq worker server, which is a separate process role and would
// bring a whole worker lifecycle into a byte-equality test for nothing. What matters is
// preserved exactly: ProcessWebhook is handed the bytes REDIS RETURNED, not the bytes the
// relay held in memory, so the legacy body has genuinely round-tripped through the queue and
// its equality with the Kafka payload cannot be an artefact of two slices sharing a backing
// array.
// NOTE ON THE EVENT-ID HEADER, so its absence here is not mistaken for a defect.
//
// A real delivery carries LegacyWebhookEventIDHeader, recovered from the asynq TASK ID. asynq
// builds the handler context inside an internal package with no exported constructor, so the
// context below carries no task metadata and the header is therefore OMITTED on this path. That
// is the harness's limitation, not the transport's: the header is covered against a real HTTP
// receiver in TestProcessHTTPRaw_CarriesTheEventIdentityToTheReceiver, and the wiring that
// supplies the identity in production is covered in
// TestProcessWebhook_PassesTheRecoveredIdentityToTheDelivery.
//
// Nothing this file asserts is affected, because the header is outside both the body and the
// signature: the byte-equality guarantee is about the BODY, and the HMAC is computed over
// timestamp + "." + body.
func (h *dualDeliveryHarness) runWebhookWorker(task *asynq.TaskInfo) error {
	h.t.Helper()

	return h.blnk.ProcessWebhook(context.Background(), asynq.NewTask(task.Type, task.Payload))
}

// httpDeliveries returns a copy of every delivery the subscriber received.
func (h *dualDeliveryHarness) httpDeliveries() []dualDeliveryHTTPCapture {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]dualDeliveryHTTPCapture(nil), h.deliveries...)
}

// soleHTTPDelivery returns the one delivery the subscriber received, failing if there is not
// exactly one.
func (h *dualDeliveryHarness) soleHTTPDelivery() dualDeliveryHTTPCapture {
	h.t.Helper()

	deliveries := h.httpDeliveries()
	require.Len(h.t, deliveries, 1, "exactly one legacy webhook must have been delivered")

	return deliveries[0]
}

// failNextDeliveries makes the subscriber answer status for every subsequent delivery.
func (h *dualDeliveryHarness) failNextDeliveries(status int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.responseStatus = status
}

// deliver drives ONE event down BOTH transports and returns every byte sequence involved.
//
// The whole pipeline, in the order production runs it:
//
//	PublishEvent          → one pending outbox row, payload = marshaled NewWebhook
//	processBatch          → claims the row, enqueues the legacy delivery from it, publishes
//	                        it to Kafka, records both outcomes
//	Redis                 → the legacy body, serialised and read back by identity
//	ProcessWebhook        → validates without transforming, signs, and posts those bytes
//
// It requires both legs to have run, because a test that quietly observed only one of them
// would report success for a window in which half the guarantee had stopped holding.
func (h *dualDeliveryHarness) deliver(event NewWebhook, options ...EventOption) dualDeliveryObservation {
	h.t.Helper()

	row := h.captureEvent(event, options...)
	require.Equal(h.t, model.EventOutboxStatusPending, row.Status,
		"a freshly captured row must be pending, which is what makes it claimable")

	h.makeClaimable(*row)
	require.Equal(h.t, 1, h.relayOneBatch(), "the relay must claim the row it was given")

	request := h.soleKafkaRequest()

	// The envelope a live writer would put on the wire, produced by the production
	// serialiser. Asserting against this rather than against a locally-composed envelope is
	// what makes the splice assertion a statement about Kafka's bytes.
	message, err := marshalLedgerEvent(request.Event)
	require.NoError(h.t, err, "the recorded request must serialise as a ledger event envelope")

	task, queued := h.queuedTask(row.EventID)
	require.True(h.t, queued,
		"the legacy delivery must be enqueued during the dual-delivery window, under the identity "+
			"legacy-webhook:%s", row.EventID)

	require.NoError(h.t, h.runWebhookWorker(task), "the legacy delivery must succeed")

	return dualDeliveryObservation{
		row:             row,
		kafkaRequest:    request,
		kafkaPayload:    []byte(request.Event.Payload),
		kafkaMessage:    message,
		queuedPayload:   task.Payload,
		queuedTaskType:  task.Type,
		queuedTaskQueue: task.Queue,
		http:            h.soleHTTPDelivery(),
	}
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

// dualDeliveryPayloadMarker is the exact byte sequence that precedes the payload inside a
// serialised LedgerEvent envelope.
const dualDeliveryPayloadMarker = `,"payload":`

// dualDeliverySchemaMarker is the exact byte sequence that follows it.
const dualDeliverySchemaMarker = `,"schema_version":`

// assertTransportsCarryIdenticalBytes is THE assertion this file exists for. It compares four
// byte sequences that must all be equal, each comparison closing a different way for the
// guarantee to break: the reference json.Marshal of the originating NewWebhook (the payload
// contract itself), the stored row's payload column (the capture path), the Kafka message's
// payload (the relay rebuilding the envelope), and the body the subscriber received after a
// queue round trip (the legacy leg re-serialising).
//
// The splice check is the strongest form of the statement: the delivered body appears inside the
// serialised Kafka envelope as a CONTIGUOUS byte run between the payload and schema_version
// members, so the HTTP body is literally a slice of the Kafka message.
func assertTransportsCarryIdenticalBytes(
	t *testing.T,
	observation dualDeliveryObservation,
	event NewWebhook,
) {
	t.Helper()

	reference := outboxLegacyWebhookBody(t, event)

	assert.Equal(t, string(reference), string(observation.row.Payload),
		"the stored payload must be the marshaled two-key webhook envelope, byte for byte")

	assert.Equal(t, string(observation.row.Payload), string(observation.kafkaPayload),
		"the Kafka message's payload must be the row's stored bytes, not a re-marshal of them")

	assert.Equal(t, string(observation.row.Payload), string(observation.queuedPayload),
		"the queued legacy body must be the row's stored bytes; a struct round trip on the way "+
			"into the queue would reorder keys and re-render numbers")

	assert.Equal(t, string(observation.kafkaPayload), string(observation.http.body),
		"THE DUAL-DELIVERY GUARANTEE: the Kafka payload and the delivered webhook body must be "+
			"byte-identical, because both are the same claimed outbox row")

	assert.Equal(t, string(reference), string(observation.http.body),
		"and both must equal the body SendWebhook would have sent, so a subscriber's existing "+
			"parser keeps working when only the transport changed")

	assert.Contains(t, string(observation.kafkaMessage),
		dualDeliveryPayloadMarker+string(observation.http.body)+dualDeliverySchemaMarker,
		"the delivered body must appear inside the serialised Kafka envelope as one contiguous "+
			"byte run: the envelope splices the payload in rather than re-encoding it")
}

// assertKafkaEnvelopeDescribesTheRow asserts the published envelope's identity fields are the
// row's, which is the other half of "one row, two transports".
//
// Byte equality alone would be satisfied by a message published under a different event id or
// to a different topic, and a subscriber routes on exactly those fields — so an envelope that
// disagreed with the row would break routing and idempotency while carrying the right bytes.
func assertKafkaEnvelopeDescribesTheRow(t *testing.T, observation dualDeliveryObservation) {
	t.Helper()

	envelope := observation.kafkaRequest.Event

	assert.Equal(t, observation.row.EventID, envelope.EventID,
		"event_id is the subscriber idempotency key and must be the row's")
	assert.Equal(t, observation.row.EventType, envelope.EventType,
		"event_type is what a subscriber routes on and must be the row's")
	assert.Equal(t, observation.row.AggregateID, envelope.AggregateID,
		"aggregate_id is what a subscriber groups by and must be the row's")
	assert.Equal(t, observation.row.SchemaVersion, envelope.SchemaVersion,
		"schema_version must be the row's")
	assert.Equal(t, model.SchemaVersionV1, envelope.SchemaVersion,
		"and the envelope version is 1")
	assert.Equal(t, observation.row.Topic, observation.kafkaRequest.Topic,
		"the destination must be the topic the row recorded at insert time, not one re-derived now")
	assert.Equal(t, observation.row.PartitionKey, observation.kafkaRequest.Key,
		"the message key must be the row's partition key, which is the value dispatch was "+
			"serialised on")
}

// dualDeliverySampleEvent is the representative event the single-event tests use: the most
// common one Blnk emits, with the payload transaction execution actually passes.
func dualDeliverySampleEvent() NewWebhook {
	return NewWebhook{
		Event:   model.EventTypeTransactionApplied,
		Payload: outboxSampleTransaction(StatusApplied),
	}
}

// ---------------------------------------------------------------------------
// The byte-equality guarantee across the whole event catalogue
// ---------------------------------------------------------------------------

// TestDualDelivery_EveryEventTypeCarriesIdenticalBytesOnBothTransports is acceptance criterion
// V-8, driven for EVERY event type: capture, claim, Kafka publish, legacy enqueue, queue round
// trip, HTTP delivery.
//
// Coverage is the point rather than a bonus. Requirement R-1 is 100% of the event types that
// reached the legacy webhook sender, so a comparison proved for one and assumed for the rest
// would leave it unverified. The catalogue comes from outboxEventFixtures, the repository's
// single statement of those types and of the payload SHAPE each producer passes — pointer,
// value, and all three shapes of the runtime-composed bulk family.
func TestDualDelivery_EveryEventTypeCarriesIdenticalBytesOnBothTransports(t *testing.T) {
	fixtures := outboxEventFixtures()
	require.NotEmpty(t, fixtures, "the event catalogue must not be empty")

	// Recorded from the fixture list rather than from inside the subtests, so a failing
	// subtest cannot make the completeness check pass by omission.
	covered := make(map[string]struct{}, len(fixtures))
	for _, fixture := range fixtures {
		covered[outboxVocabularyKey(fixture.eventType)] = struct{}{}
	}

	for _, fixture := range fixtures {
		fixture := fixture

		t.Run(fixture.name, func(t *testing.T) {
			harness := newDualDeliveryHarnessInWindow(t)

			// The SAME payload value is used for the publish and for the reference marshal.
			// outboxEventFixtures builds fresh payloads on every call, so re-deriving the
			// reference from a second call would compare two different values that merely
			// happen to be equal.
			event := NewWebhook{Event: fixture.eventType, Payload: fixture.payload}
			observation := harness.deliver(event)

			assertTransportsCarryIdenticalBytes(t, observation, event)
			assertKafkaEnvelopeDescribesTheRow(t, observation)

			assert.Equal(t, fixture.topic, observation.row.Topic,
				"%q must route to %s", fixture.eventType, fixture.topic)
			assert.Equal(t, fixture.aggregateID, observation.row.AggregateID)
			assert.Equal(t, fixture.partitionKey, observation.row.PartitionKey)

			// The two-key envelope, asserted as a byte PREFIX rather than through a decode:
			// the outer object is {"event": ..., "data": ...} in that order, which is what an
			// existing subscriber's parser reads.
			assert.True(t,
				strings.HasPrefix(string(observation.http.body), outboxEnvelopePrefix(t, fixture.eventType)),
				"the delivered body must open with the two-key webhook envelope for %q; got %s",
				fixture.eventType, string(observation.http.body))

			assert.Equal(t, 1, httpmock.GetTotalCallCount(),
				"exactly one legacy delivery per event during the window")
		})
	}

	assert.Len(t, covered, outboxEventCatalogueSize,
		"the sweep must cover all %d event types Blnk emits; requirement R-1 admits no exceptions",
		outboxEventCatalogueSize)

	for _, eventType := range outboxEventVocabulary {
		assert.Contains(t, covered, eventType,
			"%q reached the legacy webhook sender, so it must be proved byte-equivalent here too",
			eventType)
	}
}

// TestDualDelivery_CommitStatusStaysTransactionUnknownOnBothTransports pins the one event
// name in the catalogue that is a PRESERVED DEFECT rather than a design.
//
// The COMMIT status has no case in the status-to-event table, so a committed inflight
// transaction is published as transaction.unknown. That is pre-existing behaviour, and it is
// kept exactly as-is on purpose: this comparison asserts the Kafka message and the legacy
// webhook carry identical bytes for the same event, and adding a transaction.commit case would
// change the event name inside the payload on both transports at once — failing this test for a
// reason that has nothing to do with the transport, in the middle of a change about transports.
//
// So the literal is pinned here deliberately. Correcting the mapping is a separate,
// deliberately reviewed change with a subscriber-facing event-name change attached, and this
// test failing is the signal that somebody has begun making it inside the wrong change.
func TestDualDelivery_CommitStatusStaysTransactionUnknownOnBothTransports(t *testing.T) {
	eventType := getEventFromStatus(StatusCommit)

	require.Equal(t, model.EventTypeTransactionUnknown, eventType,
		"the COMMIT status falls through to the unknown event name; see the note above")
	require.Equal(t, "transaction.unknown", eventType,
		"the literal is pinned: adding a transaction.commit case is a separate change with a "+
			"subscriber-facing rename attached, and must not ride along with a transport change")

	harness := newDualDeliveryHarnessInWindow(t)
	event := NewWebhook{Event: eventType, Payload: outboxSampleTransaction(StatusCommit)}
	observation := harness.deliver(event)

	assertTransportsCarryIdenticalBytes(t, observation, event)
	assertKafkaEnvelopeDescribesTheRow(t, observation)

	assert.Equal(t, "transaction.unknown", observation.row.EventType)
	assert.Equal(t, "blnk.transactions", observation.row.Topic,
		"an unknown transaction event still belongs to the transactions topic")

	body := string(observation.http.body)
	assert.True(t, strings.HasPrefix(body, outboxEnvelopePrefix(t, "transaction.unknown")),
		"the delivered envelope must name the event transaction.unknown; got %s", body)

	// The payload still says COMMIT while the event name says unknown. That mismatch IS the
	// preserved defect, and asserting it on both transports is what proves the transport
	// change neither introduced nor concealed it.
	assert.Contains(t, body, `"status":"COMMIT"`,
		"the payload still reports the real status; only the event NAME falls through")
	assert.Contains(t, string(observation.kafkaPayload), `"status":"COMMIT"`,
		"and the Kafka side reports exactly the same, because it is the same bytes")
}

// ---------------------------------------------------------------------------
// The re-serialisation regression this file exists to catch
// ---------------------------------------------------------------------------

// dualDeliveryNonCanonicalPayload returns an event payload whose BYTES CANNOT SURVIVE a round trip
// through NewWebhook.Payload interface{}, since a fixture that survives a re-marshal proves
// nothing: the keys are non-alphabetical (a decode makes the object a map, and Go marshals map keys
// sorted), precise_amount is 2^53+1 — the smallest positive integer a float64 cannot represent, so
// a re-marshal renders it off by one in a field naming money — and amount carries a trailing zero
// that float64 rendering drops.
//
// It is a json.RawMessage, which json.Marshal writes through verbatim, so the harness controls the
// exact byte form. That is realistic: a real payload is a marshaled struct whose fields serialise
// in declaration order, and model.Transaction.PreciseAmount is a big.Int marshalling as an
// unbounded JSON number.
func dualDeliveryNonCanonicalPayload() json.RawMessage {
	return json.RawMessage(`{"transaction_id":"txn_dual_delivery_fidelity","amount":150.250,` +
		`"precise_amount":9007199254740993,"reference":"ref_dual_delivery","currency":"USD",` +
		`"created_at":"2026-03-14T15:09:26.535897932Z"}`)
}

// TestDualDelivery_NeitherTransportReserialisesTheStoredPayload is the test that fails when
// somebody rebuilds the legacy body from the domain object.
//
// It drives a payload that a decode/re-marshal round trip demonstrably alters, and then asserts
// the delivered body is unchanged — key order, trailing zero and 53-bit-exceeding integer
// intact — and identical to the Kafka payload. The final assertion proves the fixture has
// teeth: the round trip really does change these bytes, so passing is a property of the path
// and not of a fixture that happens to be insensitive.
func TestDualDelivery_NeitherTransportReserialisesTheStoredPayload(t *testing.T) {
	harness := newDualDeliveryHarnessInWindow(t)

	payload := dualDeliveryNonCanonicalPayload()
	event := NewWebhook{Event: model.EventTypeTransactionApplied, Payload: payload}
	observation := harness.deliver(event)

	assertTransportsCarryIdenticalBytes(t, observation, event)

	body := string(observation.http.body)

	assert.Contains(t, body, `{"event":"transaction.applied","data":{"transaction_id":`,
		"declaration order must survive: a re-marshal would sort the data object's keys and put "+
			"amount first")
	assert.Contains(t, body, `"amount":150.250`,
		"the trailing zero must survive: float64 re-rendering writes 150.25")
	assert.Contains(t, body, `"precise_amount":9007199254740993`,
		"the amount must survive EXACTLY: float64 cannot represent 2^53+1 and a re-marshal "+
			"writes 9007199254740992, understating the money by one unit")

	// The teeth. If a round trip did not alter these bytes, every assertion above would hold
	// even on a path that re-derived the body, and this whole file would be decorative.
	var decoded NewWebhook
	require.NoError(t, json.Unmarshal(observation.http.body, &decoded),
		"the delivered body must still be a valid webhook envelope")

	reMarshalled, err := json.Marshal(decoded)
	require.NoError(t, err)

	assert.NotEqual(t, string(observation.http.body), string(reMarshalled),
		"the fixture must be one a decode/re-marshal round trip demonstrably alters, or this test "+
			"could not distinguish a byte-carrying path from a re-deriving one")

	t.Logf("delivered verbatim : %s", body)
	t.Logf("after a round trip : %s", string(reMarshalled))
}

// ---------------------------------------------------------------------------
// Same-row provenance
// ---------------------------------------------------------------------------

// TestDualDelivery_BothLegsAreDrivenFromTheSameClaimedRow states the MECHANISM behind the
// guarantee rather than the guarantee itself.
//
// Byte equality is a consequence of provenance: the relay claims one row and both legs read
// that row's payload column. This asserts the provenance directly — the queued legacy body, the
// Kafka payload and the stored column are one byte sequence, the envelope's identity fields are
// the row's, and the dual-delivery marker was written on that row WHILE THE CLAIM WAS STILL
// HELD, before MarkEventDispatched cleared the token.
//
// The claim-token assertions are not bookkeeping trivia. The substituted store refuses a
// transition naming a token it is not currently holding, exactly as the repository's
// token-matched UPDATE does, so a recorded marker is proof the leg ran inside the claim rather
// than after a lease had lapsed and another worker had taken the row on.
func TestDualDelivery_BothLegsAreDrivenFromTheSameClaimedRow(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	harness := newDualDeliveryHarnessInWindow(t)
	event := dualDeliverySampleEvent()
	observation := harness.deliver(event)

	assertTransportsCarryIdenticalBytes(t, observation, event)
	assertKafkaEnvelopeDescribesTheRow(t, observation)

	// The legacy task is addressed by the ROW's event id, which is what makes a re-enqueue
	// after a crash a duplicate asynq can refuse.
	assert.Equal(t, legacyWebhookTaskID(observation.row.EventID),
		legacyWebhookTaskID(observation.kafkaRequest.Event.EventID),
		"both legs must be identified by the same event id, because they are the same event")
	assert.Equal(t, 1, harness.queuedTaskCount(),
		"one row must produce exactly one queued legacy delivery")

	marks := harness.store.snapshotWebhookMarks()
	require.Len(t, marks, 1, "the legacy leg must be recorded on the row, or a re-claim re-enqueues it")
	assert.Equal(t, dualDeliveryOutboxRowID, marks[0].id, "recorded on the row that was claimed")
	assert.Equal(t, dualDeliveryFirstClaimToken, marks[0].claimToken,
		"recorded under the claim still held; MarkEventDispatched clears the token, so the marker "+
			"has to be written first")

	dispatched := harness.store.snapshotDispatched()
	require.Len(t, dispatched, 1, "the Kafka leg must reach its terminal state")
	assert.Equal(t, marks[0].id, dispatched[0].id, "both transitions name the same row")
	assert.Equal(t, marks[0].claimToken, dispatched[0].claimToken,
		"and both were performed under the one claim")

	state, terminal := harness.store.terminalState(dualDeliveryOutboxRowID)
	require.True(t, terminal, "the row must have reached a terminal state")
	assert.Equal(t, model.EventOutboxStatusDispatched, state)

	assert.Empty(t, relayEntriesWithMessage(hook, "could not be marked"),
		"the marker must have been accepted; a rejected token would mean the leg ran outside the claim")

	// The side-by-side view. Reading these two lines is the human form of the assertion above,
	// and it is what makes a failure diff legible when the bytes ever do diverge.
	t.Logf("kafka payload : %s", string(observation.kafkaPayload))
	t.Logf("webhook body  : %s", string(observation.http.body))
	t.Logf("kafka envelope: %s", string(observation.kafkaMessage))
}

// TestDualDelivery_ARelayRestartDeliversTheWebhookExactlyOnce covers the crash window the row
// marker alone cannot close: enqueuing the legacy delivery and recording that it was enqueued are
// two operations against two systems with no transaction spanning them, so a crash in between
// leaves the row unmarked and the next claim enqueues AGAIN.
//
// The event id is the asynq task identity, so that second enqueue is refused and the refusal is
// reported as success — the post-condition the relay needs already holds. The sequence is driven
// exactly: the marker write fails, the row is claimed again as the database would offer it, and
// the subscriber still receives one delivery. The Kafka leg legitimately publishes twice (delivery
// there is at-least-once, deduplicated at the subscriber on event_id), and both publishes must
// carry identical bytes.
func TestDualDelivery_ARelayRestartDeliversTheWebhookExactlyOnce(t *testing.T) {
	harness := newDualDeliveryHarnessInWindow(t)
	event := dualDeliverySampleEvent()
	row := harness.captureEvent(event)

	// The crash window, reproduced: the task is enqueued and the marker never lands.
	harness.store.webhookErr = errors.New("dual delivery test: the marker write did not land")

	harness.makeClaimable(*row)
	require.Equal(t, 1, harness.relayOneBatch())
	require.Equal(t, 1, harness.queuedTaskCount(), "the first claim must enqueue the delivery")
	require.Empty(t, harness.httpDeliveries(), "the worker has not run yet")

	// The relay restarts. The row is offered again exactly as the database holds it — unmarked,
	// because the marker write failed — so the dual-delivery branch runs a second time.
	harness.store.webhookErr = nil
	harness.makeClaimable(*row)
	require.Equal(t, 1, harness.relayOneBatch())

	assert.Equal(t, 1, harness.queuedTaskCount(),
		"the re-enqueue must be refused by the event-id task identity; one event, one queued delivery")

	task, queued := harness.queuedTask(row.EventID)
	require.True(t, queued, "the single task must still be addressable by the event's identity")
	require.NoError(t, harness.runWebhookWorker(task))

	assert.Len(t, harness.httpDeliveries(), 1,
		"the subscriber must receive exactly one delivery however many times the row was claimed")
	assert.Equal(t, 1, httpmock.GetTotalCallCount(),
		"one HTTP request in total, counted at the transport")

	requests := harness.kafka.snapshotRequests()
	require.Len(t, requests, 2,
		"the Kafka leg republishes after a re-claim, which is the documented at-least-once behaviour")
	assert.Equal(t, string(requests[0].Event.Payload), string(requests[1].Event.Payload),
		"a republish must carry the same bytes; it is the same row")
	assert.Equal(t, requests[0].Event.EventID, requests[1].Event.EventID,
		"and the same event id, which is what lets the subscriber suppress the duplicate")

	delivery := harness.soleHTTPDelivery()
	assert.Equal(t, string(requests[1].Event.Payload), string(delivery.body),
		"the one delivered body must still be byte-identical to what Kafka carried")
	assert.Equal(t, string(row.Payload), string(task.Payload),
		"and the queued bytes must still be the row's, unchanged by the second claim")
}

// ---------------------------------------------------------------------------
// The legacy transport's own wire contract, over the shared bytes
// ---------------------------------------------------------------------------

// TestDualDelivery_PreservesTheLegacyWireContractOverTheSharedBytes asserts that carrying the
// outbox row's bytes did not cost the legacy transport any part of its contract.
//
// SIGNING IS THE REASON THIS BELONGS HERE rather than in webhooks_process_test.go. A receiver
// recomputes the HMAC over the body it received, so the signature has to cover the bytes AS
// SENT — and during the window those bytes are the row's, which are the Kafka payload. Signing
// over anything else would mean either that the delivered body is not what was signed, or that
// what was signed is not what Kafka carried. The test therefore recomputes the signature
// independently over BOTH byte sequences and requires the same digest from each.
//
// The queue routing is asserted for a blunter reason: the asynq task type and the queue name are
// deliberately the same string, because the worker mux dispatches on the type while asynq.Queue
// routes on the name. If they diverge the task lands on a queue whose mux has no handler for it
// and expires unhandled — delivering nothing, with nothing failing to say so.
func TestDualDelivery_PreservesTheLegacyWireContractOverTheSharedBytes(t *testing.T) {
	harness := newDualDeliveryHarnessInWindow(t)
	event := dualDeliverySampleEvent()

	// The signing timestamp comes from the wall clock inside the transport, so the window is
	// bracketed with the wall clock too. Both bounds are unix SECONDS compared as integers.
	before := time.Now().Unix()
	observation := harness.deliver(event)
	after := time.Now().Unix()

	assertTransportsCarryIdenticalBytes(t, observation, event)

	assert.Equal(t, http.MethodPost, observation.http.method, "the legacy contract is a POST")
	assert.Equal(t, dualDeliveryWebhookURL, observation.http.url,
		"the delivery must go to the configured Notification.Webhook.Url")
	assert.Equal(t, "application/json", observation.http.headers.Get("Content-Type"))
	assert.Equal(t, dualDeliveryHeaderValue, observation.http.headers.Get(dualDeliveryHeaderName),
		"configured Notification.Webhook.Headers must still be applied")

	timestamp := observation.http.headers.Get("X-Blnk-Timestamp")
	require.NotEmpty(t, timestamp, "a configured secret means the delivery is signed and timestamped")

	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	require.NoError(t, err, "X-Blnk-Timestamp must be a unix second")
	assert.GreaterOrEqual(t, seconds, before, "the timestamp must come from the delivery window")
	assert.LessOrEqual(t, seconds, after)

	signature := observation.http.headers.Get("X-Blnk-Signature")
	require.NotEmpty(t, signature, "a configured secret must produce a signature")

	// Recomputed the way a receiver does: HMAC-SHA256 over timestamp + "." + body, hex encoded.
	deliveredMAC := hmac.New(sha256.New, []byte(dualDeliverySigningSecret))
	_, err = deliveredMAC.Write([]byte(timestamp + "." + string(observation.http.body)))
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(deliveredMAC.Sum(nil)), signature,
		"the signature must verify over the bytes the subscriber actually received")

	// And over the Kafka payload, which must be the same material. This is the assertion that
	// ties the signing to the equivalence: if these two digests differ, the two transports are
	// no longer carrying one body.
	kafkaMAC := hmac.New(sha256.New, []byte(dualDeliverySigningSecret))
	_, err = kafkaMAC.Write([]byte(timestamp + "." + string(observation.kafkaPayload)))
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(kafkaMAC.Sum(nil)), signature,
		"the signature over the Kafka payload must be the same digest, because it is the same body")

	assert.Equal(t, dualDeliveryWebhookQueue, harness.configuration.Queue.WebhookQueue,
		"the harness configures the queue the assertions below name")
	assert.Equal(t, harness.configuration.Queue.WebhookQueue, observation.queuedTaskQueue,
		"the delivery must land on the configured webhook queue")
	assert.Equal(t, harness.configuration.Queue.WebhookQueue, observation.queuedTaskType,
		"and the task TYPE must be that same string, or the worker mux has no handler for it")
	assert.Equal(t, observation.queuedTaskType, observation.queuedTaskQueue,
		"type and queue are deliberately identical; they must not be allowed to drift apart")
}

// ---------------------------------------------------------------------------
// Both sides of the sunset boundary (requirement R-12)
// ---------------------------------------------------------------------------

// TestDualDelivery_SunsetVerdictComesFromConfiguration walks the boundary the 30-day window
// ends at, from both sides.
//
// The verdict is never stubbed here. Each case configures WEBHOOK_DEPRECATION_SUNSET_DATE and
// lets event_sunset.go decide against the relay's pinned clock, which is the only arrangement
// that tests the wiring as well as the behaviour: a stubbed predicate would keep passing even if
// the relay stopped consulting the real one.
//
// The post-sunset assertion is a COUNT OF ZERO at the transport, not an empty body. "No HTTP
// request was made" and "an empty request was made" are different facts, and only the first
// satisfies the criterion.
func TestDualDelivery_SunsetVerdictComesFromConfiguration(t *testing.T) {
	t.Run("inside the window both transports deliver", func(t *testing.T) {
		harness := newDualDeliveryHarness(t, dualDeliveryWindowOpen())
		event := dualDeliverySampleEvent()

		observation := harness.deliver(event)

		assertTransportsCarryIdenticalBytes(t, observation, event)
		assert.Equal(t, 1, httpmock.GetTotalCallCount(),
			"the legacy transport is still live while the sunset is in the future")
		assert.Len(t, harness.store.snapshotWebhookMarks(), 1)
	})

	// The four cases below are all POST-SUNSET, reached four different ways, and all must behave
	// identically: Kafka alone, nothing enqueued, nothing delivered. The last two are the
	// FAIL-CLOSED arm — an unusable window with a live Kafka transport — which matters because
	// the alternative would keep the deprecated transport alive indefinitely on the strength of
	// one mis-typed environment variable, invisibly.
	postSunset := []struct {
		name   string
		sunset string
		// logFragment, when set, is a message the fail-closed path must emit. It is asserted only
		// where the emission is deterministic: the warn-once guard keys on the RAW VALUE, and a
		// freshly reset guard already holds the empty string, so the unset case's line is
		// legitimately suppressed. The log contract itself belongs to event_sunset_test.go; what
		// matters here is that failing closed is not silent.
		logFragment string
		why         string
	}{
		{
			name:   "one nanosecond past the sunset instant",
			sunset: dualDeliveryWindowClosed(),
			why:    "the window is closed",
		},
		{
			name:   "exactly at the sunset instant",
			sunset: dualDeliverySunsetIn(0),
			why: "the window is half-open and EXCLUDES its end: the configured instant is the " +
				"first moment of the post-sunset era, so dual delivery stops at it and not after it",
		},
		{
			name:   "an unset window with Kafka configured",
			sunset: "",
			why: "a missing window with a live Kafka transport FAILS CLOSED: carrying on with the " +
				"deprecated transport indefinitely on the strength of an unset variable is the " +
				"failure the fail-closed answer exists to prevent",
		},
		{
			name: "a mis-typed window with Kafka configured",
			// A space where the T belongs and no zone: the single most likely way an operator
			// mis-writes an RFC3339 instant.
			sunset:      "2026-06-01 09:00:00",
			logFragment: "not a valid RFC3339 instant",
			why: "an unparseable window fails closed for the same reason an absent one does, and " +
				"it is the case the AAP calls out: one mis-typed variable must not silently " +
				"preserve the deprecated transport",
		},
	}

	for _, testCase := range postSunset {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			harness := newDualDeliveryHarness(t, testCase.sunset)
			event := dualDeliverySampleEvent()
			row := harness.captureEvent(event)

			harness.makeClaimable(*row)
			require.Equal(t, 1, harness.relayOneBatch())

			// Kafka is unaffected by the sunset — it is the transport that outlives it.
			request := harness.soleKafkaRequest()
			assert.Equal(t, string(row.Payload), string(request.Event.Payload),
				"the Kafka leg still carries the row's bytes: %s", testCase.why)
			assert.Len(t, harness.store.snapshotDispatched(), 1,
				"and the row still reaches its terminal state")

			_, queued := harness.queuedTask(row.EventID)
			assert.False(t, queued, "no legacy delivery may be enqueued: %s", testCase.why)
			assert.Equal(t, 0, harness.queuedTaskCount(), "the webhook queue must be empty")
			assert.Empty(t, harness.store.snapshotWebhookMarks(),
				"and nothing may be recorded for a leg that did not run")

			assert.Zero(t, httpmock.GetTotalCallCount(),
				"NO HTTP REQUEST may be made after the sunset — asserted as a call count, because "+
					"an empty body would satisfy a weaker check")
			assert.Empty(t, harness.httpDeliveries())

			if testCase.logFragment != "" {
				entries := relayEntriesWithMessage(hook, testCase.logFragment)
				require.NotEmpty(t, entries,
					"failing closed must be LOUD: an operator has to learn that the deprecated "+
						"transport was retired because the window is unusable, not because the "+
						"migration finished")
				assert.Equal(t, logrus.ErrorLevel, entries[0].Level,
					"with Kafka configured, an unusable window is an error rather than a warning: it "+
						"has retired a live transport")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Isolation between the two transports
// ---------------------------------------------------------------------------

// TestDualDelivery_ALegacyFailureNeverReachesTheKafkaLeg asserts the one-way isolation the window
// depends on: letting the DEPRECATED transport influence the Kafka leg would give a down webhook
// receiver the power to spend Kafka retry attempts and dead-letter events on its replacement.
//
// Both places the legacy leg can fail are exercised — the subscriber rejecting the delivery, and
// the queue being unreachable so nothing is enqueued. In each, the Kafka publish happens, the row
// reaches its dispatched terminal state, and no retry attempt is recorded.
func TestDualDelivery_ALegacyFailureNeverReachesTheKafkaLeg(t *testing.T) {
	t.Run("the subscriber rejects the delivery", func(t *testing.T) {
		harness := newDualDeliveryHarnessInWindow(t)
		harness.failNextDeliveries(http.StatusServiceUnavailable)

		event := dualDeliverySampleEvent()
		row := harness.captureEvent(event)

		harness.makeClaimable(*row)
		require.Equal(t, 1, harness.relayOneBatch())

		request := harness.soleKafkaRequest()
		assert.Equal(t, string(row.Payload), string(request.Event.Payload),
			"the Kafka publish happens regardless of what the subscriber will answer")

		require.Len(t, harness.store.snapshotDispatched(), 1)
		state, terminal := harness.store.terminalState(dualDeliveryOutboxRowID)
		require.True(t, terminal)
		assert.Equal(t, model.EventOutboxStatusDispatched, state,
			"the row's dispatched state describes the Kafka leg and nothing else")
		assert.Empty(t, harness.store.snapshotFailures(),
			"a subscriber-side failure must never spend a Kafka retry attempt")

		task, queued := harness.queuedTask(row.EventID)
		require.True(t, queued)

		err := harness.runWebhookWorker(task)
		require.Error(t, err,
			"a non-2xx must fail the task so asynq redelivers; retry there is asynq's job and always was")

		delivery := harness.soleHTTPDelivery()
		assert.Equal(t, string(request.Event.Payload), string(delivery.body),
			"the rejected delivery still carried the shared bytes; the failure is the subscriber's, "+
				"not the payload's")
	})

	t.Run("the legacy queue is unreachable", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		harness := newDualDeliveryHarnessInWindow(t)
		event := dualDeliverySampleEvent()
		row := harness.captureEvent(event)

		// The queue goes away between capture and relay. Closing it is idempotent, so the
		// harness's own cleanup stays safe.
		harness.redis.Close()

		harness.makeClaimable(*row)
		require.Equal(t, 1, harness.relayOneBatch())

		request := harness.soleKafkaRequest()
		assert.Equal(t, string(row.Payload), string(request.Event.Payload),
			"an unreachable legacy queue must not stop the Kafka publish")
		assert.Empty(t, harness.store.snapshotFailures(),
			"nor spend a Kafka retry attempt")
		assert.Empty(t, harness.store.snapshotWebhookMarks(),
			"nothing may be recorded for a leg that never ran")

		// The row is NOT dispatched, because a webhook is still owed and dispatched is
		// outside the claim predicate. It is recorded as webhook_pending instead: the Kafka
		// leg is final, the legacy leg is retried on a later claim, and — because the Kafka
		// leg is recorded separately — that later claim publishes nothing to the topic.
		assert.Empty(t, harness.store.snapshotDispatched(),
			"a row whose legacy leg is still owed must not be moved to its terminal state; that "+
				"is what used to lose the webhook permanently")
		pendings := harness.store.snapshotWebhookPendings()
		require.Len(t, pendings, 1, "the outstanding legacy leg must be recorded")
		assert.Equal(t, row.ID, pendings[0].id)
		assert.NotEmpty(t, pendings[0].reason,
			"last_error must say why the enqueue failed, or an operator has nothing to triage")
		assert.Zero(t, httpmock.GetTotalCallCount(), "and no delivery can have been made")

		entries := relayEntriesWithMessage(hook, "enqueuing the legacy webhook delivery failed")
		require.NotEmpty(t, entries, "the legacy failure must still be visible to an operator")
		assert.Equal(t, logrus.WarnLevel, entries[0].Level,
			"a deprecated transport failing is a warning, not an error on the transport replacing it")
	})
}

// ---------------------------------------------------------------------------
// This file's own discipline
// ---------------------------------------------------------------------------

// dualDeliveryParseOwnSource parses THIS FILE so a test can assert the ABSENCE of something in
// it.
//
// An absence is the one property a behavioural test cannot reach: a second sunset comparison, or
// a stub over the relay's predicate, is invisible from the outside for exactly as long as it
// happens to agree with event_sunset.go — and single ownership is worth asserting precisely
// because it has to keep agreeing after somebody changes one of them.
//
// # Why the AST, and not the text
//
// This used to read the file as a string, and a self-referential text scan cannot work. The
// needles appeared in the file's own source — inside the very assertion looking for them — so
// each one had to be ASSEMBLED FROM CONCATENATED FRAGMENTS to avoid matching itself. At that
// point the check had become a check on its own obfuscation: it could be defeated by an extra
// space, and it failed on any comment that happened to name the construct it forbids.
//
// Parsing removes the problem at the root. Comments and string literals are not AST nodes, so
// the file can freely DOCUMENT the rule it obeys, and the needles are written plainly here
// because a plain identifier in this comment is not an identifier in the tree.
//
// Parameters:
//   - t *testing.T: the test.
//
// Returns:
//   - *ast.File: this file, parsed without comments.
func dualDeliveryParseOwnSource(t *testing.T) *ast.File {
	t.Helper()

	return parseRepositoryGoFile(t, "event_dual_delivery_test.go")
}

// TestDualDelivery_MakesNoSunsetDecisionOfItsOwn keeps this file honest about the boundary it
// tests.
//
// The sunset is ONE decision with two consumers — the relay's dual-delivery branch and the API's
// 410 Gone guard — and both must ask event_sunset.go. A test that formed its own opinion, either
// by comparing instants itself or by replacing the relay's predicate with a stub, would report
// the boundary as correct while the production wiring had stopped consulting it.
//
// The needles below are written PLAINLY. They no longer have to be hidden from the assertion
// that looks for them, because the assertion reads the parsed tree: an identifier inside a
// comment or a string literal is not an identifier in the AST, so this file can name the very
// constructs it forbids while still being proved free of them.
func TestDualDelivery_MakesNoSunsetDecisionOfItsOwn(t *testing.T) {
	source := dualDeliveryParseOwnSource(t)

	require.Positive(t, identifierUses(source, "newDualDeliveryHarness"),
		"the parse must have read THIS file, or every assertion below passes vacuously")

	// No stub over the relay's own decision. Both of the relay's predicate fields are named,
	// because replacing either one would test the branch without testing that the branch still
	// asks event_sunset.go — and assignmentTargets sees a plain assignment and a composite-literal
	// field alike, which is what the two separate text needles used to cover between them.
	assigned := assignmentTargets(source)
	for _, field := range []string{"sunsetPassed", "dualDeliveryActive", "windowState"} {
		assert.Zero(t, assigned[field],
			"this file must not assign %s: substituting the relay's predicate would exercise the "+
				"dual-delivery branch while proving nothing about whether that branch still "+
				"consults event_sunset.go", field)
	}

	// No instant comparison of its own. This is the second, independently drifting copy the rule
	// exists to prevent: a test that decided for itself whether the window was open would report
	// the boundary as correct while production had stopped asking.
	calls := selectorCallNames(source)
	assert.Zero(t, calls["Before"],
		"comparing instants here would be a second copy of the sunset decision, free to drift")
	assert.Zero(t, calls["After"], "the same, in the other direction")
	assert.Zero(t, qualifiedCallCount(source, "time", "Parse"),
		"nor may it parse a date: event_sunset.go owns what the configured string means")

	// The positive half: the boundary IS driven, and it is driven through configuration, which is
	// the only input event_sunset.go reads.
	assert.Positive(t, identifierUses(source, "WebhookDeprecationSunsetDate"),
		"the sunset cases must configure the deployment's window and let event_sunset.go judge it")
	assert.Positive(t, identifierUses(source, "dualDeliveryWindowClosed"),
		"and both sides of the boundary must be exercised")
	assert.Positive(t, identifierUses(source, "dualDeliveryWindowOpen"),
		"including the open side, or the closed cases alone would be satisfied by a relay that "+
			"never dual-delivered at all")
}
