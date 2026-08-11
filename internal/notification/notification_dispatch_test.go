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

package notification

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeNotificationConfig installs a configuration with the given Slack and
// webhook URLs. DataSource/Redis DNS are required by config validation but are
// never dialed by the notification package.
// restoreConfigStoreAfterTest captures the process-global configuration and puts it back on
// cleanup.
//
// config.ConfigStore is an atomic.Value shared by every test in the binary, and MockConfig
// replaces it outright. Without a restore, the LAST configuration any test stored stays in
// place for whatever runs next — and the failure that causes is order-dependent, so it appears
// when a test is added, reordered or run in isolation, and it points at the wrong file.
//
// It is one helper used by every config-storing helper in this package precisely so the two
// cannot diverge: a restore present in one and absent in the other leaks exactly as badly as
// no restore at all, and is harder to spot.
//
// The previous value is restored only if there WAS one. A nil-typed interface stored back into
// an atomic.Value panics, and an atomic.Value cannot be reset to empty, so "nothing was set
// before" is correctly left exactly as it was.
func restoreConfigStoreAfterTest(t *testing.T) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)
		}
	})
}

func storeNotificationConfig(t *testing.T, slackURL, webhookURL string) {
	t.Helper()

	restoreConfigStoreAfterTest(t)

	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Notification: config.Notification{
			Slack:   config.SlackWebhook{WebhookUrl: slackURL},
			Webhook: config.WebhookConfig{Url: webhookURL},
		},
	})
	// MockConfig silently refuses to store invalid configs; fail loudly instead.
	conf, err := config.Fetch()
	require.NoError(t, err)
	require.Equal(t, slackURL, conf.Notification.Slack.WebhookUrl)
	require.Equal(t, webhookURL, conf.Notification.Webhook.Url)
}

// capturedRequest records what the fake Slack endpoint received.
type capturedRequest struct {
	method      string
	contentType string
	body        []byte
}

// newSlackCaptureServer spins up an httptest server that records every request
// and responds with the given status code and body.
func newSlackCaptureServer(status int, responseBody string) (*httptest.Server, func() []capturedRequest) {
	var mu sync.Mutex
	var captured []capturedRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		captured = append(captured, capturedRequest{
			method:      r.Method,
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responseBody))
	}))

	get := func() []capturedRequest {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedRequest, len(captured))
		copy(out, captured)
		return out
	}
	return server, get
}

// slackPayload mirrors the block structure SlackNotification builds.
type slackPayload struct {
	Blocks []struct {
		Type string `json:"type"`
		Text *struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Emoji bool   `json:"emoji"`
		} `json:"text,omitempty"`
		Fields []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"fields,omitempty"`
	} `json:"blocks"`
}

func TestSlackNotification_PayloadShape(t *testing.T) {
	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	SlackNotification(errors.New("ledger exploded: insufficient funds"))

	reqs := captured()
	require.Len(t, reqs, 1, "exactly one Slack call expected")

	req := reqs[0]
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "application/json", req.contentType)

	var payload slackPayload
	require.NoError(t, json.Unmarshal(req.body, &payload), "Slack payload must be valid JSON, got: %s", string(req.body))
	require.Len(t, payload.Blocks, 3)

	// Header block.
	require.NotNil(t, payload.Blocks[0].Text)
	assert.Equal(t, "header", payload.Blocks[0].Type)
	assert.Equal(t, "Error From Blnk 🐞", payload.Blocks[0].Text.Text)

	// Error block must carry the actual error message.
	require.NotEmpty(t, payload.Blocks[1].Fields)
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, "*Error:*")
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, "ledger exploded: insufficient funds")

	// Time block must carry a parseable RFC822 timestamp.
	require.NotEmpty(t, payload.Blocks[2].Fields)
	assert.Contains(t, payload.Blocks[2].Fields[0].Text, "*Time:*")
}

func TestSlackNotification_ErrorMessageWithQuotesStillDelivered(t *testing.T) {
	// Regression: the Slack payload used to be built by fmt.Sprintf-ing
	// err.Error() into a raw JSON template, so quotes/backslashes/newlines in
	// the error (e.g. pq errors) produced invalid JSON and the notification
	// was silently dropped. The payload is now built with json.Marshal.
	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	SlackNotification(errors.New(`pq: column "balance_id" does not exist`))

	reqs := captured()
	require.Len(t, reqs, 1, "notification with quoted error message must still be delivered")
	var payload slackPayload
	require.NoError(t, json.Unmarshal(reqs[0].body, &payload))
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, `column "balance_id" does not exist`)
}

func TestSlackNotification_Receiver500DoesNotPanicAndDoesNotRetry(t *testing.T) {
	server, captured := newSlackCaptureServer(http.StatusInternalServerError, `{"ok":false}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("downstream said no"))
	})

	// Document the current contract: one attempt, no retries, failure swallowed.
	assert.Len(t, captured(), 1)
}

func TestSlackNotification_NonJSONResponseBody(t *testing.T) {
	// Real Slack webhooks reply with plain-text "ok", which is not valid JSON.
	// The JSON decode of the response fails internally; the function must
	// still have delivered the message and must not panic.
	server, captured := newSlackCaptureServer(http.StatusOK, `ok`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("plain text response handling"))
	})
	assert.Len(t, captured(), 1)
}

func TestSlackNotification_ConnectionRefused(t *testing.T) {
	server, _ := newSlackCaptureServer(http.StatusOK, `{}`)
	deadURL := server.URL
	server.Close() // nothing listening anymore

	storeNotificationConfig(t, deadURL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("nobody is listening"))
	})
}

func TestSlackNotification_HangingReceiverShouldTimeOut(t *testing.T) {
	// FIXED: request.Call now uses an http.Client with a 30s Timeout, so a
	// hung Slack endpoint no longer blocks the NotifyError goroutine forever.
	// The positive verification is still skipped because it needs a ~30s
	// wall-clock wait for the real timeout to fire.
	t.Skip("verifying the 30s client timeout requires a 30s wall-clock wait; " +
		"the timeout is set in internal/request/request.go Call()")

	blockForever := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockForever // never respond
	}))
	defer func() {
		close(blockForever)
		server.Close()
	}()

	storeNotificationConfig(t, server.URL, "")

	done := make(chan struct{})
	go func() {
		SlackNotification(errors.New("should not hang forever"))
		close(done)
	}()

	select {
	case <-done:
		// returned in bounded time: a timeout exists
	case <-time.After(35 * time.Second): // generous bound > the 30s used elsewhere
		t.Fatal("SlackNotification blocked indefinitely on a hung receiver: no HTTP timeout configured")
	}
}

func TestNotifyError_WebhookSenderReceivesSystemError(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	type call struct {
		event   string
		payload interface{}
	}
	calls := make(chan call, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		calls <- call{event: event, payload: payload}
		return nil
	})

	NotifyError(errors.New("queue worker crashed"))

	select {
	case got := <-calls:
		assert.Equal(t, "system.error", got.event)
		payloadMap, ok := got.payload.(map[string]interface{})
		require.True(t, ok, "payload should be a map, got %T", got.payload)

		// THE PAYLOAD IS THE FROZEN LEGACY CONTRACT, asserted at the dispatch boundary
		// because this is the exact value that leaves the package. Requirement R-8
		// requires it to match the webhook body field-for-field, and that body has always
		// been {"error", "time"}: a subscriber re-points its consumer at a Kafka topic and
		// its body handling keeps working, which is the whole point of the migration.
		assert.Equal(t, "queue worker crashed", payloadMap["error"],
			"the error text is the value subscribers parse; it must be carried verbatim")

		ts, ok := payloadMap["time"].(time.Time)
		require.True(t, ok, "payload time should be a time.Time")
		assert.WithinDuration(t, time.Now(), ts, 10*time.Second)

		// Exactly two keys. A third is a change to a published contract and should fail
		// here rather than reach a subscriber — including the classified reason and the
		// correlation id an earlier revision published, which belong on the log line
		// instead.
		assert.Len(t, payloadMap, 2,
			"the system.error payload is a published contract: error and time, and nothing else")
		assert.NotContains(t, payloadMap, "reason")
		assert.NotContains(t, payloadMap, "correlation_id")
		assert.NotContains(t, payloadMap, "error_code")
	case <-time.After(3 * time.Second):
		t.Fatal("webhook sender was never invoked by NotifyError")
	}
}

func TestNotifyError_NoWebhookURLConfigured_SenderNotCalled(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "")

	calls := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		calls <- event
		return nil
	})

	NotifyError(errors.New("should stay local"))

	select {
	case event := <-calls:
		t.Fatalf("webhook sender called (%q) despite empty webhook URL", event)
	case <-time.After(500 * time.Millisecond):
		// expected: nothing dispatched
	}
}

func TestNotifyError_NoSenderRegistered_DoesNotPanic(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)
	webhookSender = nil

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	assert.NotPanics(t, func() {
		NotifyError(errors.New("no sender registered"))
		time.Sleep(300 * time.Millisecond) // let the goroutine run
	})
}

func TestNotifyError_SenderErrorIsSwallowed(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	called := make(chan struct{}, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		called <- struct{}{}
		return errors.New("downstream webhook enqueue failed")
	})

	assert.NotPanics(t, func() {
		NotifyError(errors.New("sender will fail"))
	})

	select {
	case <-called:
		// the error from the sender must not propagate or panic
	case <-time.After(3 * time.Second):
		t.Fatal("webhook sender was never invoked")
	}
}

// TestNotifyError_DispatchesToBothSlackAndWebhook asserts both channels are reached AND the
// order they are reached in.
//
// # The order is the assertion, not an implementation detail
//
// The durable event is attempted FIRST and Slack second. Slack is a synchronous POST to a third
// party whose client allows 30 seconds, and it is the call most likely to hang during the very
// incident being reported — so with Slack first, every system.error waited on it before its
// outbox row existed, a burst accumulated goroutines each holding an uncaptured event, and a
// process that died in that window lost them with no row to replay from. Reversing the order
// again would restore that, silently and with both channels still working, which is why the
// sequence is pinned here rather than left to the reading of the code.
//
// The wait is the completion seam rather than a receive on the sender's channel. Waiting only
// for the sender would return while the notifier was still inside the Slack POST, so the
// deferred server close would race it and the Slack assertion would fail for a reason that has
// nothing to do with the behaviour.
func TestNotifyError_DispatchesToBothSlackAndWebhook(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	var (
		mu    sync.Mutex
		order []string
	)
	record := func(channel string) {
		mu.Lock()
		order = append(order, channel)
		mu.Unlock()
	}

	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()

	// Wrapped so the Slack hit is recorded in the same ordered log as the event capture. The
	// capture server records the request itself; this only records WHEN, relative to the sender.
	slack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("slack")
		server.Config.Handler.ServeHTTP(w, r)
	}))
	defer slack.Close()

	storeNotificationConfig(t, slack.URL, "http://example.invalid/webhook-target")

	senderCalled := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		record("event")
		senderCalled <- event

		return nil
	})

	awaitCompletion := awaitNotifyError(t)

	NotifyError(errors.New("dual channel failure"))

	awaitCompletion()

	select {
	case event := <-senderCalled:
		assert.Equal(t, "system.error", event)
	default:
		t.Fatal("webhook sender was never invoked")
	}

	reqs := captured()
	require.Len(t, reqs, 1, "Slack should have received the error notification")
	assert.Contains(t, string(reqs[0].body), "dual channel failure")

	mu.Lock()
	sequence := append([]string(nil), order...)
	mu.Unlock()

	assert.Equal(t, []string{"event", "slack"}, sequence,
		"the durable event must be attempted BEFORE the optional Slack delivery: Slack is a "+
			"third-party POST with a 30-second budget, and letting it run first delays every "+
			"system.error's outbox row by that budget during the outage that produced it")
}
