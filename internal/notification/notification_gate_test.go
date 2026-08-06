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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is the regression gate for ONE line of notification.go: the condition that
// decides whether NotifyError hands system.error to the registered webhook sender.
//
// # What changed, and why it needs its own gate
//
// The condition used to be `sender != nil && conf.Notification.Webhook.Url != ""`. It is
// now `sender != nil && (len(conf.Kafka.Brokers) > 0 || conf.Notification.Webhook.Url != "")`.
//
// That sender is system.error's ONLY route into the event pipeline. Every other event
// type Blnk publishes has a producer call site that names it — ledger.created in
// ledger.go, balance.created in balance.go, and so on — but system.error has none: it
// exists solely because NotifyError invokes whatever sender the root package registered,
// and the root package registers a closure that writes to the transactional outbox.
// Gating that invocation on the LEGACY webhook URL alone therefore dropped system.error
// entirely on a Kafka-only deployment, which is the intended end state once the 30-day
// dual-delivery window closes and the HTTP transport is retired. It is the one event type
// that a webhook-shaped gate can silently lose.
//
// Nothing else in the repository can catch a regression here. The root package's event
// tests cover the closure and the outbox row; the sibling files in this package cover the
// Slack transport, the sanitizer, and the legacy webhook-URL path. A refactor that
// restored the webhook-URL-only gate would leave all of them green, because every one of
// them configures a webhook URL. Only a test that configures Kafka and NO webhook URL
// fails, and that test is TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked
// below.
//
// # Why the file exists separately
//
// The three other test files in this package are left untouched. notification_test.go
// covers sender registration, notification_dispatch_test.go covers the Slack and webhook
// transports, and notification_sanitize_test.go covers what a system.error payload is
// permitted to contain. The dispatch condition is a fourth subject, and it gets a fourth
// file rather than an edit to any of theirs.
//
// # Discipline these tests follow
//
// NotifyError runs its entire body in a goroutine, so nothing here asserts immediately
// after calling it. A positive expectation is a receive on a buffered channel bounded by a
// generous timeout; a negative expectation is a timeout that wins a race against a
// receive. Every test saves and restores the package-global sender, and none of them calls
// t.Parallel(), because the sender and the configuration store are both global.

// gateSunsetDate is a valid RFC3339 instant used only to keep the configuration
// acceptable to config validation. See storeGateConfig for why it cannot be omitted.
//
// Nothing in this package reads it: NotifyError consults the Slack URL, the Kafka broker
// list, and the webhook URL, and nothing else. It is a fixed literal rather than an
// offset from time.Now() so that no test here depends on the clock.
const gateSunsetDate = "2099-01-01T00:00:00Z"

// gateBroker is a broker address that is never dialed.
//
// The gate reads len(conf.Kafka.Brokers) and never connects to anything, so the address
// only has to survive config normalisation — which trims each entry and drops the blank
// ones — and be recognisable in a failure message.
const gateBroker = "localhost:9092"

// storeGateConfig installs a configuration carrying the given Kafka brokers, Slack URL
// and webhook URL, and then proves that it was actually stored.
//
// It is deliberately a SECOND helper rather than a widened storeNotificationConfig.
// That helper is shared by eleven tests in notification_dispatch_test.go, and adding a
// brokers parameter to it would mean editing a file this change leaves alone.
//
// # Two of the fields below are load-bearing even though no assertion reads them
//
// config.MockConfig validates before it stores and REFUSES INVALID CONFIGURATIONS
// SILENTLY: it logs the error and returns, leaving config.Fetch to hand back whichever
// configuration a previously-run test left in the global store. A test built on a refused
// store is not testing what it appears to test — it is measuring the leftovers, and it can
// pass or fail for reasons that have nothing to do with its assertions. Two separate rules
// can trigger that refusal here:
//
//   - DataSource.Dns and Redis.Dns are required unconditionally. Neither is ever dialed by
//     this package; the values are the same ones storeNotificationConfig uses.
//   - WebhookDeprecationSunsetDate is required as soon as a broker is configured, because
//     a deployment with Kafka but no sunset instant would run dual delivery forever.
//     Supplying only the sunset end of the window is accepted verbatim and the start is
//     back-filled from it, so there is no 30-day arithmetic to get right here.
//
// The sunset date is set on every call, including the calls that pass no brokers, where it
// is equally valid and equally inert. Setting it unconditionally means a scenario that
// later gains a broker cannot quietly slide back into the refused-store trap.
//
// The require calls after the store are what convert that silent refusal into a visible
// failure, and they check the brokers as well as the two URLs because the broker list is
// the input the gate actually turns on.
//
// Parameters:
//   - t *testing.T: the test installing the configuration.
//   - brokers []string: the Kafka broker list. A nil slice means "no Kafka configured",
//     and is passed as nil rather than as an empty slice because nil is the value a
//     deployment that never mentions Kafka actually produces, which makes the gate's
//     len() check exercised against the real production shape.
//   - slackURL string: the Slack webhook URL. Every test here passes "" so that no HTTP
//     request is attempted; the Slack transport is covered by its own tests.
//   - webhookURL string: the legacy webhook URL.
func storeGateConfig(t *testing.T, brokers []string, slackURL, webhookURL string) {
	t.Helper()

	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Kafka:      config.KafkaConfig{Brokers: brokers},
		Notification: config.Notification{
			Slack:   config.SlackWebhook{WebhookUrl: slackURL},
			Webhook: config.WebhookConfig{Url: webhookURL},
		},
		WebhookDeprecationSunsetDate: gateSunsetDate,
	})

	// MockConfig silently refuses to store invalid configs; fail loudly instead.
	conf, err := config.Fetch()
	require.NoError(t, err)
	require.Equal(t, brokers, conf.Kafka.Brokers,
		"the broker list was not stored: MockConfig refused the configuration, so this test "+
			"would have run against whatever a previous test left in the global store")
	require.Equal(t, slackURL, conf.Notification.Slack.WebhookUrl)
	require.Equal(t, webhookURL, conf.Notification.Webhook.Url)
}

// TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked is the reason this file
// exists.
//
// Restoring the old condition was verified to fail exactly this test and the payload test
// at the end of this file, and to leave every other test in this package passing.
//
// Kafka is configured and the legacy webhook URL is EMPTY: the shape of a deployment that
// has finished migrating off HTTP webhooks, and the shape every other test in this package
// avoids. Under the old webhook-URL-only condition the sender is never called and
// system.error — the thirteenth and last event type in the catalogue — is silently absent
// from the event stream, with no test anywhere turning red. Under the current condition the
// sender is called and the event reaches the outbox like every other event type.
//
// The event name is asserted as a literal rather than against the systemErrorEventType
// constant on purpose: comparing the constant with itself would still pass if the constant
// were changed, and the string is a wire value that subscribers route on.
func TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, []string{gateBroker}, "", "")

	events := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})

	NotifyError(errors.New("ledger relay worker stopped unexpectedly"))

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	case <-time.After(3 * time.Second):
		t.Fatal("the webhook sender was never invoked with Kafka brokers configured: " +
			"system.error is being dropped on a Kafka-only deployment, which is the " +
			"regression this test exists to catch")
	}
}

// TestNotifyError_NeitherTransportConfigured_SenderNotCalled pins the other side of the
// relaxed condition, and it is the invariant the change was most likely to break.
//
// An empty broker list is a legitimate steady state rather than a misconfiguration: it
// selects the no-op event publisher, which is what lets a deployment run with no Kafka at
// all and what lets the existing test suite run unchanged. Relaxing a gate is exactly the
// kind of edit that turns "dispatch when something is configured" into "dispatch always",
// so the no-transport case is asserted explicitly rather than assumed.
//
// The broker list is passed as nil, not as an empty slice, because nil is the value a
// deployment that never mentions Kafka actually produces, and it is what makes this a test
// of the len() check's nil-safety as well as of the gate.
func TestNotifyError_NeitherTransportConfigured_SenderNotCalled(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, nil, "", "")

	events := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})

	// NotifyError returns as soon as it has spawned its goroutine, so this guards the
	// spawn only. A panic inside the goroutine would take the whole test binary down
	// instead of being recovered here, which the wait below leaves time for.
	assert.NotPanics(t, func() {
		NotifyError(errors.New("no transport for this one"))
	})

	select {
	case event := <-events:
		t.Fatalf("the webhook sender was called (%q) with neither Kafka brokers nor a "+
			"webhook URL configured: the unconfigured deployment must stay silent", event)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing dispatched. The window has to be long enough for the
		// goroutine to have reached the gate, or this passes without proving anything.
	}
}

// TestNotifyError_WebhookURLWithoutKafka_SenderInvoked proves the legacy path still works.
//
// Relaxing a condition should widen it, not move it. During the 30-day dual-delivery window
// a deployment can legitimately have a webhook URL and no brokers at all, and that
// deployment must keep receiving system.error exactly as it did before. This is the corner
// that a refactor replacing the disjunction with a Kafka-only check would break.
func TestNotifyError_WebhookURLWithoutKafka_SenderInvoked(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, nil, "", "http://example.invalid/webhook-target")

	events := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})

	NotifyError(errors.New("legacy webhook path still carries this"))

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	case <-time.After(3 * time.Second):
		t.Fatal("the webhook sender was never invoked with a webhook URL configured: the " +
			"legacy transport has regressed, and it must keep working for the whole " +
			"dual-delivery window")
	}
}

// TestNotifyError_BothTransportsConfigured_SenderInvokedExactlyOnce guards against the
// mirror-image mistake: dispatching twice.
//
// The gate is a single disjunction over two configured transports, and the obvious
// "readable" refactor of it is two separate if blocks — one per transport — which sends
// system.error twice whenever both are configured. That is the state every deployment is in
// for the whole 30-day dual-delivery window, and it is invisible to every other test here:
// each of them would receive its one expected invocation and pass, because a duplicate on a
// buffered channel that nobody reads twice is simply never observed.
//
// Duplication matters beyond tidiness. The sender writes an outbox row, so a second
// invocation is a second row with its own event_id, and event_id is the idempotency key
// subscribers deduplicate on — two distinct ids are two distinct events to every consumer,
// and no amount of consumer-side care collapses them.
//
// The count is read from an atomic rather than inferred from the channel alone so that a
// third or later invocation, which would block on a full channel, still shows up in the
// assertion.
func TestNotifyError_BothTransportsConfigured_SenderInvokedExactlyOnce(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, []string{gateBroker}, "", "http://example.invalid/webhook-target")

	var invocations atomic.Int32
	// Capacity 2 so that a second dispatch lands instead of blocking, which is what makes
	// it observable below rather than merely deadlocked.
	events := make(chan string, 2)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		invocations.Add(1)
		events <- event
		return nil
	})

	NotifyError(errors.New("dispatched once, never twice"))

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	case <-time.After(3 * time.Second):
		t.Fatal("the webhook sender was never invoked with both transports configured")
	}

	select {
	case event := <-events:
		t.Fatalf("system.error was dispatched twice (second invocation %q): the gate must "+
			"remain ONE disjunction over the two transports, not one branch per "+
			"transport, or every dual-delivery deployment emits two outbox rows with two "+
			"different event ids for a single error", event)
	case <-time.After(500 * time.Millisecond):
		// Expected: exactly one dispatch, however many transports are configured.
	}

	assert.Equal(t, int32(1), invocations.Load(),
		"system.error must be handed to the sender exactly once per error regardless of "+
			"how many transports are configured")
}

// TestNotifyError_KafkaConfiguredWithNoSenderRegistered_DoesNotPanic proves the nil check
// still short-circuits.
//
// `sender != nil` is the first conjunct of the gate, and it has to stay first: the process
// registers its sender during construction, so any error notified before that point — or in
// any process role that never registers one — finds a nil sender while Kafka is fully
// configured. Reordering the conjuncts, or folding the nil check into the disjunction,
// turns an error notification into a nil-function call, and a panic in NotifyError's
// goroutine cannot be recovered by the caller: it takes the process down. In a service whose
// job is to notify about errors, that converts any error into an outage.
//
// The nil is installed through RegisterWebhookSender rather than by assigning the global
// directly. Both produce the same state, but the setter takes the mutex that guards it,
// which keeps this test clean under the race detector even if a goroutine from an earlier
// test is still reading the sender.
func TestNotifyError_KafkaConfiguredWithNoSenderRegistered_DoesNotPanic(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)
	RegisterWebhookSender(nil)

	storeGateConfig(t, []string{gateBroker}, "", "")

	assert.NotPanics(t, func() {
		NotifyError(errors.New("no sender to hand this to"))
		time.Sleep(300 * time.Millisecond) // let the goroutine reach the gate
	})
}

// TestNotifyError_KafkaConfigured_PayloadShapeIsUnchanged pins the payload the sender
// receives, so that widening the gate cannot quietly change WHAT is dispatched while
// changing WHEN it is dispatched.
//
// # The shape asserted here is the sanitized one, deliberately
//
// This payload was once {"error": systemError.Error(), "time": now}. It no longer carries
// the error text at all, and this test asserts the current three-key shape rather than the
// historical two-key one, because the raw text was the problem: Blnk's internal errors
// render with the schema, table, constraint and routine that produced them, with internal
// host addresses and broker ports, and sometimes with quoted account references or a DSN
// password — and system.error is published to a replicated, retained Kafka topic, stored in
// the outbox table, and copied to a dead-letter topic if it fails. What is published instead
// is a diagnosis and a handle: a classified reason drawn from a fixed vocabulary, and a
// correlation id that appears on the log line carrying the full error. Reinstating an
// "error" key here would reinstate the leak, so its ABSENCE is asserted too.
//
// Three keys is the count for an untyped error. A typed apierror also carries error_code,
// which is omitted rather than blanked when there is none; that rule has its own test in
// notification_sanitize_test.go and is not restated here. The length assertion is the point
// of this test: a fourth key added to the payload is a change to a published contract, and
// it should fail here rather than reach a subscriber.
func TestNotifyError_KafkaConfigured_PayloadShapeIsUnchanged(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, []string{gateBroker}, "", "")

	type call struct {
		event   string
		payload interface{}
	}
	calls := make(chan call, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		calls <- call{event: event, payload: payload}
		return nil
	})

	systemError := errors.New("ledger relay worker stopped unexpectedly")
	NotifyError(systemError)

	select {
	case got := <-calls:
		assert.Equal(t, "system.error", got.event)

		payloadMap, ok := got.payload.(map[string]interface{})
		require.True(t, ok, "payload should be a map, got %T", got.payload)

		// The diagnosis: a value from the fixed reason vocabulary. A plain errors.New
		// whose text matches none of the known signatures classifies as unclassified.
		assert.Equal(t, SystemErrorReasonUnclassified, payloadMap["reason"],
			"the payload must carry a classified reason from the fixed vocabulary")

		// The handle: the id that ties this event to the log line holding the full error.
		// Without it the sanitized payload would be a dead end for an operator.
		correlationID, ok := payloadMap["correlation_id"].(string)
		require.True(t, ok, "payload correlation_id should be a string")
		assert.NotEmpty(t, correlationID,
			"an empty correlation id would leave the event unlinkable to any log line")

		ts, ok := payloadMap["time"].(time.Time)
		require.True(t, ok, "payload time should be a time.Time")
		assert.WithinDuration(t, time.Now(), ts, 10*time.Second)

		assert.Len(t, payloadMap, 3,
			"the system.error payload is a published contract: reason, correlation_id and "+
				"time for an untyped error, and nothing else")

		// The sanitization invariant, restated at the dispatch boundary because this is
		// the exact value that leaves the package.
		assert.NotContains(t, payloadMap, "error",
			"the payload must not carry an error key at all; the full error belongs in the log")
		for key, value := range payloadMap {
			if text, isString := value.(string); isString {
				assert.NotContains(t, text, systemError.Error(),
					"%s must not carry the raw error text", key)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the webhook sender was never invoked, so the payload could not be inspected")
	}
}
