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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redis_db "github.com/blnkfinance/blnk/internal/redis-db"
)

// This file covers the OBSERVABILITY AND CANCELLATION contract of the legacy HTTP webhook
// transport: which context a delivery is bound to, and what a failed delivery says about
// itself. It is DELETED WITH webhooks.go at the sunset, and STEP 2 of the sunset procedure
// at the foot of that file names it.
//
// It exists separately from webhooks_process_test.go because that file's subject is the WIRE
// CONTRACT — the body, the signature, the headers, the retry semantics — and every test in
// it asserts what a subscriber receives. The subject here is what an OPERATOR receives, and
// the two failure modes it guards are the kind that leave the wire contract intact:
//
//   - A delivery that cannot be cancelled looks perfect at the receiver and holds a draining
//     worker open for as long as the socket does.
//   - A failure record that names no task and no event looks like a log entry and answers no
//     question, and at the pipeline's 500-events-per-second target there are enough of them
//     to bury everything that would.
//
// Both are asserted here rather than in webhooks_process_test.go so that neither file's
// subject has to be read through the other's.

// legacyWebhookEnvelope marshals a webhook envelope for use as a task payload.
//
// Parameters:
//   - t *testing.T: the test, for the marshal failure.
//   - event string: the event name.
//
// Returns:
//   - []byte: the marshaled two-key legacy envelope.
func legacyWebhookEnvelope(t *testing.T, event string) []byte {
	t.Helper()

	body, err := json.Marshal(NewWebhook{
		Event:   event,
		Payload: map[string]interface{}{"id": "txn_logging_test"},
	})
	require.NoError(t, err)

	return body
}

// captureLogsUntil is captureLogs for logging that happens on ANOTHER GOROUTINE.
//
// captureLogs returns the moment fn returns, which is correct for logging fn performs
// itself and useless for logging a worker performs later: the buffer would be read before
// the record was written, and the test would report an absence it created. So this waits for
// the record to arrive, and then keeps waiting.
//
// THE SETTLING WINDOW IS THE POINT. Returning as soon as `want` entries have arrived would
// make "exactly one record" unprovable — a second, duplicate record still in flight would
// simply not be in the buffer yet, and the assertion would pass on a race. Waiting past the
// arrival of the last expected entry is what turns "one record" into evidence.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration and decode failures.
//   - level logrus.Level: the level to capture at.
//   - want int: how many entries to wait for before starting the settling window.
//   - fn func(): the code that triggers the logging. May return long before it happens.
//
// Returns:
//   - capturedLog: the decoded entries and the raw rendering.
func captureLogsUntil(t *testing.T, level logrus.Level, want int, fn func()) capturedLog {
	t.Helper()

	const (
		arrivalTimeout = 20 * time.Second
		settling       = 250 * time.Millisecond
		poll           = 20 * time.Millisecond
	)

	return captureLogs(t, level, func() {
		fn()

		deadline := time.Now().Add(arrivalTimeout)
		for time.Now().Before(deadline) {
			if countLogEntries(t) >= want {
				break
			}

			time.Sleep(poll)
		}

		time.Sleep(settling)
	})
}

// countLogEntries counts the JSON records the standard logger's current output holds.
//
// It reaches into the redirected buffer through the logger rather than being handed it,
// which keeps captureLogsUntil a thin wrapper over captureLogs instead of a second copy of
// its redirect-and-restore logic.
//
// It is called on the test goroutine while a delivery may still be logging from an asynq
// worker, so the read is only safe because captureLogs installs a lock-guarded buffer; the
// fmt.Stringer assertion below is what proves the redirect is in place before reading it.
//
// Returns:
//   - int: the number of complete records written so far.
func countLogEntries(t *testing.T) int {
	t.Helper()

	buffer, ok := logrus.StandardLogger().Out.(fmt.Stringer)
	require.True(t, ok, "captureLogs must have redirected the logger to a buffer")

	written := buffer.String()
	if written == "" {
		return 0
	}

	return len(strings.Split(strings.TrimRight(written, "\n"), "\n"))
}

// TestProcessWebhook_HonoursACancelledHandlerContext is the cancellation half of the context
// fix.
//
// ProcessWebhook took its context as `_` and built the request with http.NewRequest, so the
// only bound on a delivery was b.httpClient's thirty-second timeout. asynq cancels a
// handler's context when a worker shuts down, and that cancellation reached nothing: a
// worker draining for deployment had to wait out every delivery already on the wire, and a
// receiver holding connections open could stretch that to thirty seconds per in-flight task.
//
// A cancelled context is asserted to produce an error AND NO REQUEST, which is what
// distinguishes a request that was never sent from one that was sent and then abandoned. The
// second is not good enough: an abandoned request may still have been delivered, so a
// subscriber could receive an event the task reports as failed and asynq then retries.
//
// Losing nothing is the other half. The task fails, so asynq retries it, and the outbox row
// behind it is untouched — cancelling a delivery costs a redelivery, not an event.
func TestProcessWebhook_HonoursACancelledHandlerContext(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.ProcessWebhook(ctx, asynq.NewTask("webhook_delivery", legacyWebhookEnvelope(t, "transaction.applied")))

	require.Error(t, err, "a cancelled handler context must fail the delivery rather than complete it")
	assert.ErrorIs(t, err, context.Canceled,
		"the failure must be the cancellation itself, so an operator reading the record sees a shutdown rather than a receiver fault")
	assert.Empty(t, received(),
		"no request may reach the receiver: an abandoned-but-sent request would deliver an event the task reports as failed")
}

// TestProcessWebhook_MalformedPayloadRecordNamesTheTaskAndWithholdsWhatItCannotKnow covers
// the failure mode where there is no event to name.
//
// A task payload that will not parse is a permanent failure, and the only facts available
// about it are the task it arrived as and how many bytes it carried. Both are asserted
// present, because a record saying only "could not unmarshal" cannot be acted on: an
// operator cannot find, inspect or archive a task they cannot identify.
//
// event_type is asserted ABSENT, and that is not a detail. Nothing could be decoded, so any
// event name in this record would have been fabricated — and a fabricated event name is
// worse than a missing one, because it sends the investigation to an event that had nothing
// to do with the failure.
func TestProcessWebhook_MalformedPayloadRecordNamesTheTaskAndWithholdsWhatItCannotKnow(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	const truncated = `{"event": "broken`

	var handlerErr error
	captured := captureLogs(t, logrus.DebugLevel, func() {
		handlerErr = b.ProcessWebhook(
			context.Background(),
			asynq.NewTask("webhook_delivery", []byte(truncated)),
		)
	})

	require.Error(t, handlerErr)
	assert.Empty(t, received(), "no HTTP call may be made for a payload that is not an envelope")

	const message = "not a webhook envelope"
	require.Equal(t, 1, captured.count(message),
		"a malformed task must produce exactly ONE record; the second one this replaced said the same thing in different words")

	assert.Equal(t, "legacy_webhook", captured.field(message, "transport"))
	assert.Equal(t, "webhook_delivery", captured.field(message, "task_type"))
	assert.Equal(t, float64(len(truncated)), captured.field(message, "payload_bytes"),
		"the body size is the only fact available about a body that would not parse, and it separates an empty task from a truncated one")
	assert.NotEmpty(t, captured.field(message, "error"),
		"the parse failure itself must be reported; without it the record says only that something was wrong")

	assert.Nil(t, captured.field(message, "event_type"),
		"nothing was decoded, so naming an event here would be a fabrication that misdirects the investigation")
}

// TestProcessWebhook_DeliveryFailureProducesOneRecordCarryingTheStatus covers the failure
// mode where the event IS known.
//
// Two records used to describe one failed delivery: a Warn in processHTTPRaw naming the
// status and nothing else, and — for the parse path only — an Error here. Neither named the
// event, so at the acceptance target of five hundred events a second the log filled with
// statuses that could not be attributed to a task, a subscriber or a batch.
//
// So the count is asserted first, and the status is asserted to have SURVIVED the
// consolidation. Removing the duplicate would be a regression if the fact it carried went
// with it: the status is what separates a receiver rejecting the request from a receiver that
// is not there.
func TestProcessWebhook_DeliveryFailureProducesOneRecordCarryingTheStatus(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusServiceUnavailable)
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	var handlerErr error
	captured := captureLogs(t, logrus.DebugLevel, func() {
		handlerErr = b.ProcessWebhook(
			context.Background(),
			asynq.NewTask("webhook_delivery", legacyWebhookEnvelope(t, "transaction.applied")),
		)
	})

	require.Error(t, handlerErr, "a non-2xx must fail the task so asynq retries it")
	require.Len(t, received(), 1, "exactly one attempt per call")

	const message = "legacy webhook delivery failed"
	require.Equal(t, 1, captured.count(message),
		"one failed delivery must produce one record")
	require.Len(t, captured.entries, 1,
		"and NOTHING else may be logged for it: the status line processHTTPRaw used to write is the duplicate this consolidates")

	assert.Equal(t, "legacy_webhook", captured.field(message, "transport"))
	assert.Equal(t, "transaction.applied", captured.field(message, "event_type"),
		"the event family is what attributes the failure to something a subscriber cares about")

	reported, ok := captured.field(message, "error").(string)
	require.True(t, ok, "the failure must be reported as a field rather than interpolated into the message")
	assert.Contains(t, reported, "503",
		"the status must survive the consolidation; it is what separates a rejecting receiver from an absent one")
}

// TestProcessWebhook_FailureRecordCapsACauseTheReceiverControlsTheLengthOf pins the bound on
// a value whose SIZE is chosen outside Blnk.
//
// The cause of a delivery failure is not Blnk's text. A transport error is a *url.Error whose
// message contains the URL it was trying to reach, and a receiver decides that URL by
// redirecting: Go follows the redirect, fails, and reports the target verbatim. So a receiver
// can make one failed delivery's error string arbitrarily long, and at five hundred events a
// second an unbounded cause lets one misbehaving receiver decide how much of the log
// everything else has to share.
//
// The redirect is used rather than a fabricated error because it is a real path from outside
// Blnk to a logged string. Note what it does NOT demonstrate: Go percent-encodes control
// characters when it re-renders a URL, so this vector cannot carry a newline.
// TestLegacyWebhookFailureRecord_StripsWhatWouldForgeALogLine owns that property, asserted
// where a newline can actually be handed in.
func TestProcessWebhook_FailureRecordCapsACauseTheReceiverControlsTheLengthOf(t *testing.T) {
	server := newRedirectingWebhookReceiver("http://127.0.0.1:1/" + strings.Repeat("p", 4000))
	defer server.Close()
	storeWebhookTestConfig(t, server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	var handlerErr error
	captured := captureLogs(t, logrus.DebugLevel, func() {
		handlerErr = b.ProcessWebhook(
			context.Background(),
			asynq.NewTask("webhook_delivery", legacyWebhookEnvelope(t, "transaction.applied")),
		)
	})

	require.Error(t, handlerErr)

	const message = "legacy webhook delivery failed"
	reported, ok := captured.field(message, "error").(string)
	require.True(t, ok, "the cause must be reported as a field rather than interpolated into the message")

	require.NotEmpty(t, reported, "the cause must still be reported; capping it must not amount to dropping it")
	assert.LessOrEqual(t, len([]rune(reported)), maxLoggedErrorLength+len([]rune(logTruncationSuffix)),
		"the cause must be capped so no single receiver can decide how much of the log a failure occupies")
	assert.Contains(t, reported, logTruncationSuffix,
		"a shortened cause must SAY it was shortened, or it reads as the whole of the failure")
}

// TestLegacyWebhookFailureRecord_StripsWhatWouldForgeALogLine asserts the control-character
// half of the sanitisation, against the helper that owns it.
//
// It is asserted here rather than through a delivery because the delivery vectors cannot
// supply a newline: Go encodes them out of a URL. What CAN is the general case this record is
// built for — an error from any dependency in the chain, and a task type read from
// configuration. logrus's JSON formatter would escape a newline in a field, but the
// deployment's formatter is not this function's to choose, and under the text formatter a
// value carrying a newline splits the line and everything after it reads as a separate entry
// of whatever level the forged prefix claims.
//
// The three inputs are asserted separately because they arrive by three different routes, and
// sanitising two of them is a fix that looks complete.
func TestLegacyWebhookFailureRecord_StripsWhatWouldForgeALogLine(t *testing.T) {
	forged := "boom\n{\"level\":\"info\",\"msg\":\"delivery succeeded\"}"

	captured := captureLogs(t, logrus.DebugLevel, func() {
		legacyWebhookFailureRecord(
			context.Background(),
			asynq.NewTask(forged, []byte("{}")),
			NewWebhook{Event: forged},
			errors.New(forged),
		).Error("probe")
	})

	require.Len(t, captured.entries, 1)

	for _, field := range []string{"error", "event_type", "task_type"} {
		value, ok := captured.field("probe", field).(string)
		require.True(t, ok, "%s must be present as a string, or this asserts nothing about it", field)

		assert.NotContains(t, value, "\n",
			"%s must not carry a newline: under a line-oriented formatter it splits the record and the remainder reads as an entry of its own", field)
		assert.Contains(t, value, "boom",
			"%s must keep its diagnostic content; stripping the structure must not strip the meaning", field)
	}
}

// TestLegacyWebhookFailureRecord_OmitsAnIdentityItWasNotGiven is the honesty property of the
// record.
//
// Every asynq accessor reports whether the value was present, and a context asynq did not
// create has none of them. Defaulting the missing ones would be worse than omitting them:
// retry_count=0 reads as a first attempt, and an empty task_id reads as a task that can be
// looked up. A record that says nothing about what it cannot know is one an operator can
// trust the rest of.
func TestLegacyWebhookFailureRecord_OmitsAnIdentityItWasNotGiven(t *testing.T) {
	captured := captureLogs(t, logrus.DebugLevel, func() {
		legacyWebhookFailureRecord(
			context.Background(),
			asynq.NewTask("webhook_delivery", []byte("{}")),
			NewWebhook{Event: "transaction.applied"},
			errors.New("boom"),
		).Error("probe")
	})

	require.Len(t, captured.entries, 1)

	for _, field := range []string{"task_id", "event_id", "queue", "retry_count", "max_retry"} {
		assert.Nil(t, captured.field("probe", field),
			"%s comes from asynq alone, so a context asynq did not create must leave it absent rather than defaulted", field)
	}

	// What IS knowable is still reported, or the omissions above would be indistinguishable
	// from the record having failed to assemble.
	assert.Equal(t, "legacy_webhook", captured.field("probe", "transport"))
	assert.Equal(t, "webhook_delivery", captured.field("probe", "task_type"))
	assert.Equal(t, "transaction.applied", captured.field("probe", "event_type"))
}

// newRedirectingWebhookReceiver starts a receiver that answers every request with a redirect
// to location.
//
// The redirect is the mechanism: Go's http.Client follows it, fails to reach the target, and
// returns an error whose text contains location verbatim. That is how a value chosen outside
// Blnk gets into an error Blnk logs, and it is the exposure the sanitisation exists for.
//
// Parameters:
//   - location string: the redirect target.
//
// Returns:
//   - *httptest.Server: the running receiver. The caller closes it.
func newRedirectingWebhookReceiver(location string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
}

// TestProcessWebhook_FailureRecordCarriesTheAsynqTaskAndEventIdentity is the reason the
// handler context is threaded through rather than merely accepted.
//
// Every other test in this file drives ProcessWebhook with context.Background(), which is
// honest — the identity fields are then absent — but proves nothing about the production
// path, where the context comes from asynq and CARRIES the identity. asynq's accessors read
// values only asynq can set, so the only way to observe them is to let a real asynq server
// invoke the handler. That is what this does: a real Redis queue, a real server, the real
// ProcessWebhook registered on the mux exactly as cmd/workers.go registers it.
//
// The task is enqueued the way the relay's dual-delivery branch enqueues one — the queue name
// as the task type, and legacyWebhookTaskID(eventID) as the identity — so what is asserted is
// the identity production actually produces:
//
//   - task_id, so an operator can find and archive the task.
//   - event_id, DECODED from that task id, which is the join to the blnk.event_outbox row and
//     to the Kafka message published from the very same row. This is the field that makes a
//     webhook failure diagnosable against the event rather than in isolation, and it is the
//     one that cannot exist without the context.
//   - queue and retry_count, which say where it came from and whether asynq will try again.
//
// The retry delay is pushed out to an hour so exactly one attempt lands inside the test.
func TestProcessWebhook_FailureRecordCarriesTheAsynqTaskAndEventIdentity(t *testing.T) {
	server, _ := newWebhookReceiver(http.StatusServiceUnavailable)
	defer server.Close()

	cnf, queueName := storeWebhookTestConfig(t, server.URL, "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_, _ = inspector.DeleteAllRetryTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	eventID := gofakeit.UUID()
	require.NoError(t, b.EnqueueLegacyWebhookDelivery(eventID, legacyWebhookEnvelope(t, "transaction.applied")),
		"the fixture must enqueue through the production entry point so the task identity is the production one")

	redisOption, err := redis_db.ParseRedisURL(cnf.Redis.Dns, false)
	require.NoError(t, err)

	worker := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:      redisOption.Addr,
			Password:  redisOption.Password,
			DB:        redisOption.DB,
			TLSConfig: redisOption.TLSConfig,
		},
		asynq.Config{
			Concurrency: 1,
			Queues:      map[string]int{queueName: 1},
			Logger:      newTestLogger(t),
			// One attempt inside the test window. Without this the default schedule would
			// redeliver during the settling wait and the "exactly one record" assertion would
			// be asserting the retry policy instead of the record.
			RetryDelayFunc: asynq.RetryDelayFunc(func(int, error, *asynq.Task) time.Duration {
				return time.Hour
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(queueName, b.ProcessWebhook)

	const message = "legacy webhook delivery failed"
	captured := captureLogsUntil(t, logrus.DebugLevel, 1, func() {
		require.NoError(t, worker.Start(mux))
		t.Cleanup(worker.Shutdown)
	})

	require.Equal(t, 1, captured.count(message),
		"the worker must produce exactly one record for the one delivery it attempted")

	assert.Equal(t, legacyWebhookTaskID(eventID), captured.field(message, "task_id"),
		"the task id must come from the handler context; it is how an operator finds the task")
	assert.Equal(t, eventID, captured.field(message, "event_id"),
		"the event id must be decoded from the task id: it is the join to the outbox row and to the Kafka message from that same row")
	assert.Equal(t, queueName, captured.field(message, "queue"))
	assert.Equal(t, float64(0), captured.field(message, "retry_count"),
		"a first attempt must say so; without it a failure line cannot be told from a final one")
	assert.NotNil(t, captured.field(message, "max_retry"),
		"how many attempts remain is what says whether asynq will retry or archive")
}

// TestLegacyWebhookEventID_IsTheExactInverseOfTheTaskIdentity guards the pair the join
// depends on.
//
// legacyWebhookTaskID and legacyWebhookEventID are inverses, and they share one namespace
// constant so they cannot drift. The symptom of drift would be silent — task ids would still
// be correctly namespaced, and event_id would simply stop appearing in failure records — so
// the round trip is asserted rather than left to the shared constant.
//
// A foreign task id yielding "" is the other half. The webhook queue is shared with
// transaction hooks and TypeSense indexing, and reporting another producer's task id as an
// event id would attribute a hook failure to an outbox row that does not exist.
func TestLegacyWebhookEventID_IsTheExactInverseOfTheTaskIdentity(t *testing.T) {
	for _, eventID := range []string{
		"evt_123",
		gofakeit.UUID(),
		"evt:with:colons",
	} {
		assert.Equal(t, eventID, legacyWebhookEventID(legacyWebhookTaskID(eventID)),
			"the decoder must recover exactly what the encoder was given")
	}

	for name, taskID := range map[string]string{
		"a hook task":                   "hook_execution:abc",
		"an index task":                 "new:index:batch",
		"a bare event id":               "evt_123",
		"an empty identity":             "",
		"the namespace with no payload": "legacy-webhook:",
	} {
		assert.Empty(t, legacyWebhookEventID(taskID),
			"%s must not be reported as an event id: the queue is shared, and a foreign id would be attributed to an outbox row that does not exist", name)
	}
}

// TestWarnWebhookSentUnsigned_IsRateLimitedRatherThanPerDelivery is the warning-storm fix.
//
// The condition is one bit — server.secret_key is configured or it is not — and it was warned
// about once per delivery. At the pipeline's acceptance target of five hundred events a
// second that is five hundred identical lines a second for a fact that has not changed, which
// does not make the warning louder: it buries every other line in the log and trains whoever
// reads it to skim past this one.
//
// What is asserted is that the signal SURVIVES the rate limit, not merely that the volume
// fell. Three properties together are what make it a rate limit rather than a mute:
//
//   - The first occurrence warns immediately. A condition that waits ten minutes to be
//     reported is a condition nobody sees during a deployment.
//   - Occurrences inside the interval are counted and reported with the warning that ends the
//     silence, so the scale of the exposure is not lost. One unsigned delivery and forty
//     thousand must not produce the same line.
//   - The interval elapsing re-warns, for as long as the condition lasts. This is the
//     property a sync.Once latch would not have: configuration is re-read per delivery, so a
//     key removed from a running deployment must be reported even though the process warned
//     about something else hours earlier.
func TestWarnWebhookSentUnsigned_IsRateLimitedRatherThanPerDelivery(t *testing.T) {
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	previous := unsignedWebhookWarning
	t.Cleanup(func() { unsignedWebhookWarning = previous })

	unsignedWebhookWarning = &rateLimitedWarning{
		interval: unsignedWebhookWarningInterval,
		now:      func() time.Time { return at },
	}

	const message = "webhook sent unsigned"

	first := captureLogs(t, logrus.DebugLevel, func() { warnWebhookSentUnsigned() })
	require.Equal(t, 1, first.count(message),
		"the first unsigned delivery must warn immediately; a warning nobody sees during a deployment is not a warning")
	assert.Nil(t, first.field(message, "deliveries_suppressed_since_last_warning"),
		"nothing was suppressed before the first warning, so the count must be absent rather than reported as zero")

	storm := captureLogs(t, logrus.DebugLevel, func() {
		for i := 0; i < 5_000; i++ {
			warnWebhookSentUnsigned()
		}
	})
	assert.Equal(t, 0, storm.count(message),
		"five thousand deliveries inside the interval must produce no further warning; this is the storm the fix removes")

	at = at.Add(unsignedWebhookWarningInterval)

	next := captureLogs(t, logrus.DebugLevel, func() { warnWebhookSentUnsigned() })
	require.Equal(t, 1, next.count(message),
		"the interval elapsing must re-warn: the condition is not permanent, so a latch would have gone silent through a real exposure")
	assert.Equal(t, float64(5_000), next.field(message, "deliveries_suppressed_since_last_warning"),
		"the suppressed count is what keeps the rate limit honest; without it one unsigned delivery and five thousand read identically")
}

// TestProcessWebhook_UnsignedDeliveryWarnsOnceAcrossManyDeliveries closes the loop through
// the real delivery path.
//
// The test above exercises the limiter directly, which pins its arithmetic. This one proves
// the limiter is actually WHERE the deliveries are: a rate limiter that the delivery path
// does not call is a rate limiter that changes nothing, and the assertion that would have
// caught the pre-fix code is this one.
func TestProcessWebhook_UnsignedDeliveryWarnsOnceAcrossManyDeliveries(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	// No secret: this is the unsigned posture the warning describes.
	storeWebhookTestConfig(t, server.URL, "", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	previous := unsignedWebhookWarning
	t.Cleanup(func() { unsignedWebhookWarning = previous })

	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	unsignedWebhookWarning = &rateLimitedWarning{
		interval: unsignedWebhookWarningInterval,
		now:      func() time.Time { return at },
	}

	const deliveries = 25

	captured := captureLogs(t, logrus.DebugLevel, func() {
		for i := 0; i < deliveries; i++ {
			require.NoError(t, b.ProcessWebhook(
				context.Background(),
				asynq.NewTask("webhook_delivery", legacyWebhookEnvelope(t, "transaction.applied")),
			))
		}
	})

	require.Len(t, received(), deliveries, "every delivery must still be sent; the fix bounds the LOG, not the transport")
	assert.Equal(t, 1, captured.count("webhook sent unsigned"),
		"one static configuration must produce one warning, however many deliveries observe it")

	for _, req := range received() {
		assert.Empty(t, req.headers.Get("X-Blnk-Signature"),
			"the deliveries must genuinely be unsigned, or this test is asserting the absence of a warning nothing triggered")
	}
}

// capturedLog is what one redirected logging window produced: the entries, decoded, and the
// raw bytes.
//
// Both halves are needed and neither substitutes for the other. The ENTRIES answer "is the
// field there, and what is in it"; the RAW BYTES answer "is this string absent from the whole
// record", which is the only honest way to assert a non-disclosure — a secret that moved from
// a field into the message text would still satisfy every field assertion.
type capturedLog struct {
	entries []map[string]interface{}
	raw     string
}

// field returns the value of key on the first entry whose message contains needle.
//
// Returns:
//   - interface{}: the field value, or nil when no entry matches or the field is absent.
func (c capturedLog) field(needle, key string) interface{} {
	for _, entry := range c.entries {
		message, _ := entry["msg"].(string)
		if strings.Contains(message, needle) {
			return entry[key]
		}
	}

	return nil
}

// count reports how many captured entries carry needle in their message.
//
// This is the primitive behind every "emitted ONCE" assertion in this file. Counting is the
// assertion: a warning that arrives twice for one configuration is not a smaller version of
// the same signal, it is the thing that teaches an operator to skim past it.
//
// Returns:
//   - int: the number of matching entries.
func (c capturedLog) count(needle string) int {
	matches := 0
	for _, entry := range c.entries {
		message, _ := entry["msg"].(string)
		if strings.Contains(message, needle) {
			matches++
		}
	}

	return matches
}

// syncLogBuffer is the buffer captureLogs redirects the standard logger into.
//
// IT IS MUTEX-GUARDED BECAUSE THE LOGGING THIS FILE OBSERVES HAPPENS ON ANOTHER GOROUTINE.
// logrus serialises its own writes behind the logger's mutex, so a plain bytes.Buffer is
// safe against two concurrent log calls — but nothing in logrus guards a READER, and this
// file has two of them: countLogEntries polls the buffer every 20ms while an asynq worker is
// still delivering, and captureLogs renders it the moment fn returns, which for
// captureLogsUntil is while that worker may still be logging.
//
// A bytes.Buffer read concurrently with a logrus write is a data race on the buffer's length
// and on its backing array. It is reported by `go test -race`, which .github/workflows/go.yml
// runs across the whole module on every push, so an unguarded buffer here fails the build for
// the package rather than only for this file. It is not reporting-only noise either:
// Buffer.grow reallocates mid-Write, so a String taken at the wrong moment can observe a
// half-copied record and turn "exactly one record" into an undecodable capture — a failure
// that would read as a defect in the code under test.
//
// Every method takes the same mutex, which is what orders the reads against the writes
// instead of merely making a collision rare.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p under the lock. This is the io.Writer logrus is handed.
//
// Parameters:
//   - p []byte: the rendered record.
//
// Returns:
//   - int: the number of bytes written.
//   - error: always nil — bytes.Buffer writes do not fail.
func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// String renders everything written so far.
//
// countLogEntries reaches the buffer through fmt.Stringer rather than through this type, so
// this method is also what keeps that indirection working now the buffer is no longer a
// bytes.Buffer.
//
// Returns:
//   - string: the accumulated rendering.
func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// Bytes returns a COPY of everything written so far.
//
// The copy is the point. bytes.Buffer.Bytes aliases the live backing array, so decoding
// straight out of it would read that array after the lock is dropped, while the worker keeps
// appending — the same race, moved one call away from where it is visible.
//
// Returns:
//   - []byte: an independent snapshot, safe to decode without holding the lock.
func (b *syncLogBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]byte(nil), b.buf.Bytes()...)
}

// captureLogs redirects the standard logger for the duration of fn and returns what it wrote.
//
// The standard logger is process-global, so the previous output, formatter and level are
// restored through t.Cleanup rather than at the end of the function — a require failure
// inside fn aborts the goroutine, and a deferred restore that never runs would leave every
// subsequent test in the package logging into a dead buffer. No test in this file calls
// t.Parallel, which is what makes a global redirect safe here.
//
// The output is a syncLogBuffer rather than a bytes.Buffer because fn's logging may land on
// another goroutine; see that type for why an unguarded buffer is a data race and not merely
// a tidiness question.
//
// Parameters:
//   - t *testing.T: the test, for cleanup registration and decode failures.
//   - level logrus.Level: the level to capture at.
//   - fn func(): the code whose logging is being observed.
//
// Returns:
//   - capturedLog: the decoded entries and the raw rendering.
func captureLogs(t *testing.T, level logrus.Level, fn func()) capturedLog {
	t.Helper()

	logger := logrus.StandardLogger()
	previousOut := logger.Out
	previousFormatter := logger.Formatter
	previousLevel := logger.GetLevel()
	t.Cleanup(func() {
		logger.SetOutput(previousOut)
		logger.SetFormatter(previousFormatter)
		logger.SetLevel(previousLevel)
	})

	buf := &syncLogBuffer{}
	logger.SetOutput(buf)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(level)

	fn()

	// ONE snapshot, read once and then decoded, rather than String() followed by Bytes(): two
	// reads of a sink a goroutine may still be writing to could disagree, and the raw rendering
	// quoted in a failure message would then describe different bytes from the entries asserted
	// on.
	written := buf.Bytes()

	captured := capturedLog{raw: string(written)}
	decoder := json.NewDecoder(bytes.NewReader(written))
	for decoder.More() {
		entry := map[string]interface{}{}
		require.NoError(t, decoder.Decode(&entry), "the captured log must be decodable JSON")
		captured.entries = append(captured.entries, entry)
	}

	return captured
}
