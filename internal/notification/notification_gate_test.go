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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is the regression gate for the condition in notification.go that decides whether
// NotifyError hands system.error to the registered webhook sender:
// `sender != nil && (len(conf.Kafka.Brokers) > 0 || conf.Notification.Webhook.Url != "")`.
// That sender is system.error's only route into the event pipeline, so a gate testing the legacy
// webhook URL alone would drop the event on a Kafka-only deployment with no sibling test failing.
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
// Slack transport, the payload contract and the classifier, and the legacy webhook-URL
// path. A refactor that
// restored the webhook-URL-only gate would leave all of them green, because every one of
// them configures a webhook URL. Only a test that configures Kafka and NO webhook URL
// fails, and that test is TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked
// below.
//
// # Why the file exists separately
//
// The three other test files in this package are left untouched. notification_test.go
// covers sender registration, notification_dispatch_test.go covers the Slack and webhook
// transports, and notification_sanitize_test.go covers the system.error payload contract
// and the classifier that accompanies it in the log. The dispatch condition is a fourth subject, and it gets a fourth
// file rather than an edit to any of theirs.
//
// # Discipline these tests follow
//
// NotifyError runs its entire body in a goroutine, so nothing here asserts immediately
// after calling it. A positive expectation is a receive on a buffered channel bounded by a
// generous timeout; a negative expectation is a timeout that wins a race against a
// receive. Every test saves and restores the package-global sender, and none of them calls
// t.Parallel(), because the sender and the configuration store are both global.

// gateSunsetDate keeps the stored configuration valid; nothing in this package reads it. A fixed
// literal rather than an offset from time.Now() so no test here depends on the clock.
const gateSunsetDate = "2099-01-01T00:00:00Z"

// gateBroker is never dialed: the gate reads len(conf.Kafka.Brokers) and connects to nothing.
const gateBroker = "localhost:9092"

// storeGateConfig installs a configuration carrying the given Kafka brokers, Slack URL and webhook
// URL, and then proves it was actually stored. A nil broker slice means "no Kafka", and is passed
// as nil rather than empty because nil is the shape a deployment that never mentions Kafka produces.
//
// The require calls exist because config.MockConfig validates before it stores and refuses an
// invalid configuration SILENTLY, leaving config.Fetch to return whatever a previous test left in
// the global store. Redis.Dns, DataSource.Dns and — once a broker is present — the sunset date are
// all required by that validation; none is dialed here, and the sunset date is set unconditionally
// so a scenario that later gains a broker cannot slide back into the refused-store trap.
func storeGateConfig(t *testing.T, brokers []string, slackURL, webhookURL string) {
	t.Helper()

	// RESTORED when the test ends. config.ConfigStore is a process-global atomic.Value, and a
	// broker list or a sunset date left installed here is read by every later test in this
	// package that calls config.Fetch — including the ones asserting the unconfigured default,
	// which then fail in a test that never mentioned Kafka.
	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}

		config.ConfigStore.Store(&config.Configuration{})
	})

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

// gateCompletionBudget bounds how long a test waits for NotifyError's goroutine to finish.
//
// It is a DEADLOCK bound, not a timing assumption. Every wait below is on a signal the notifier
// itself raises, so the budget is only reached when the goroutine never finished at all — and
// the message says so rather than reporting a slow machine as a defect.
const gateCompletionBudget = 5 * time.Second

// awaitNotifyError returns a function that blocks until NotifyError's goroutine has completed.
//
// # What this replaces, and why it is not a smaller sleep
//
// The tests here used to assert a NEGATIVE by racing a 500ms timer against a channel receive:
// if nothing arrived in half a second, nothing was dispatched. That is not an observation of
// the gate, it is an observation of the scheduler — a machine under load reaches the gate late,
// the timer wins, and the test reports success for a dispatch that happened a millisecond
// afterwards. It fails in the direction that matters: a regression which dispatches when it
// must not would pass on a busy CI runner and fail on a fast laptop.
//
// The seam is a completion callback the notifier's goroutine runs on every exit path, so
// "the decision has been made" becomes an event rather than an elapsed duration. Nothing about
// the notifier's behaviour changes; the test simply stops guessing.
//
// It must be installed BEFORE NotifyError is called. The returned function may be called once.
//
// Parameters:
//   - t *testing.T: the test, which is failed if the goroutine never completes.
//
// Returns:
//   - func(): blocks until the notifier has finished, or fails the test.
func awaitNotifyError(t *testing.T) func() {
	t.Helper()

	// Buffered so the notifier never blocks on a test that has already given up waiting.
	completed := make(chan struct{}, 1)
	setNotifyErrorCompleted(func() {
		select {
		case completed <- struct{}{}:
		default:
		}
	})
	t.Cleanup(func() { setNotifyErrorCompleted(nil) })

	return func() {
		t.Helper()

		select {
		case <-completed:
		case <-time.After(gateCompletionBudget):
			t.Fatalf(
				"NotifyError's goroutine did not complete within %s: it is blocked rather than "+
					"slow, and no assertion about what it dispatched can be trusted",
				gateCompletionBudget,
			)
		}
	}
}

// TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked is the reason this file
// exists.
//
// WHAT IS ASSERTED IS THE GATE, and nothing beyond it — that the registered sender is invoked
// with the system.error event name. Whether an outbox row is then written and delivered is the
// root package's subject; this package cannot see the outbox. The event name is a literal rather
// than the systemErrorEventType constant so that changing the constant fails here.
func TestNotifyError_KafkaConfiguredWithoutWebhookURL_SenderInvoked(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, []string{gateBroker}, "", "")

	events := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})

	awaitCompletion := awaitNotifyError(t)

	NotifyError(errors.New("ledger relay worker stopped unexpectedly"))

	awaitCompletion()

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	default:
		t.Fatal("the webhook sender was never invoked with Kafka brokers configured: " +
			"system.error is being dropped on a Kafka-only deployment, which is the " +
			"regression this test exists to catch")
	}
}

// TestNotifyError_NeitherTransportConfigured_SenderNotCalled pins the other side of the gate.
// An empty broker list is a legitimate steady state — it selects the no-op event publisher —
// and relaxing a gate is the kind of edit that turns "dispatch when something is configured"
// into "dispatch always", so the silent case is asserted rather than assumed. nil rather than an
// empty slice also exercises the len() check's nil-safety.
func TestNotifyError_NeitherTransportConfigured_SenderNotCalled(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, nil, "", "")

	events := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		events <- event
		return nil
	})

	// Installed before the notification, so the wait below cannot miss the completion.
	awaitCompletion := awaitNotifyError(t)

	// NotifyError returns as soon as it has spawned its goroutine, so this guards the
	// spawn only. A panic inside the goroutine would take the whole test binary down
	// instead of being recovered here, which the wait below leaves time for.
	assert.NotPanics(t, func() {
		NotifyError(errors.New("no transport for this one"))
	})

	// The notifier has now finished, so "nothing was dispatched" is a completed observation
	// rather than a race against a timer that a loaded machine wins by being slow.
	awaitCompletion()

	select {
	case event := <-events:
		t.Fatalf("the webhook sender was called (%q) with neither Kafka brokers nor a "+
			"webhook URL configured: the unconfigured deployment must stay silent", event)
	default:
		// Expected: nothing dispatched, and the notifier is known to have reached its gate.
	}
}

// TestNotifyError_WebhookURLWithoutKafka_SenderInvoked proves the legacy path still works, and
// carries the legacy payload while doing it.
//
// Relaxing a condition should widen it, not move it. During the 30-day dual-delivery window
// a deployment can legitimately have a webhook URL and no brokers at all, and that
// deployment must keep receiving system.error exactly as it did before. This is the corner
// that a refactor replacing the disjunction with a Kafka-only check would break.
//
// # What this test can establish, and where the rest of the proof lives
//
// It used to stop at "the sender was invoked", which leaves the interesting half unstated: a
// webhook-only deployment is exactly the one where the ROUTE past the sender was broken. The
// registered sender writes to blnk.event_outbox, and with no brokers configured the relay used
// to refuse to run at all — so every system.error was recorded and then stranded in the table,
// invisible, with this test green.
//
// That route cannot be exercised from HERE: the sender is registered by the root blnk package,
// which imports this one, so this package cannot reach the outbox, the relay or the HTTP
// transport without an import cycle. What this test therefore asserts is everything on THIS
// side of the boundary — the gate fires, the event name is right, and the payload handed over
// is the frozen two-key legacy body — and the delivery half is asserted in the root package by
// TestSystemError_WebhookOnlyDeploymentDeliversThroughTheRelay, which drives this exact
// function through the real registered sender, the real outbox and the relay's legacy-only
// mode to an HTTP receiver. Neither test is sufficient alone and the pair is, which is why
// each names the other.
func TestNotifyError_WebhookURLWithoutKafka_SenderInvoked(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, nil, "", "http://example.invalid/webhook-target")

	type dispatch struct {
		event   string
		payload interface{}
	}
	dispatched := make(chan dispatch, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		dispatched <- dispatch{event: event, payload: payload}

		return nil
	})

	awaitCompletion := awaitNotifyError(t)

	systemError := errors.New("legacy webhook path still carries this")
	NotifyError(systemError)

	awaitCompletion()

	select {
	case got := <-dispatched:
		assert.Equal(t, "system.error", got.event)

		// The payload matters as much as the invocation on this path: a webhook-only
		// subscriber's body parser is written against these two keys, and the whole point of
		// the migration is that the transport changes and the body does not.
		payload, ok := got.payload.(map[string]interface{})
		require.True(t, ok, "the payload must be the legacy map, got %T", got.payload)
		assert.Equal(t, systemError.Error(), payload["error"],
			"the rendered error is the value a webhook subscriber reads")
		assert.Contains(t, payload, "time")
		assert.Len(t, payload, 2,
			"the legacy body is error and time, and a webhook-only deployment is precisely the "+
				"one whose parser would break on a third key")
	default:
		t.Fatal("the webhook sender was never invoked with a webhook URL configured: the " +
			"legacy transport has regressed, and it must keep working for the whole " +
			"dual-delivery window")
	}
}

// TestNotifyError_SenderFailureIsReportedRatherThanSwallowed pins what happens when the route
// past the sender fails.
//
// The sender writes an outbox row, so its error means the event was NOT recorded — and
// system.error is the one event type with no other producer, so a swallowed failure here is an
// error that vanished entirely. NotifyError cannot propagate it (it returns nothing, by
// design: notifying an error must not fail the caller that notified it), which leaves the log
// as the only channel, and makes "it is logged" the behaviour worth pinning.
//
// The correlation id is what makes that log line usable: the full error text is logged once, at
// the top of the notifier, and this line carries the id that ties the dispatch failure to it.
func TestNotifyError_SenderFailureIsReportedRatherThanSwallowed(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeGateConfig(t, nil, "", "http://example.invalid/webhook-target")

	hook := logtest.NewGlobal()
	t.Cleanup(hook.Reset)

	RegisterWebhookSender(func(string, interface{}) error {
		return errors.New("the outbox insert failed")
	})

	awaitCompletion := awaitNotifyError(t)

	NotifyError(errors.New("an error whose notification cannot be delivered"))

	awaitCompletion()

	// The reason is carried in a BOUNDED FIELD rather than interpolated into the message, and
	// the entry is found by its message instead. That is not a detail of where to look: the
	// sender's error comes from the event pipeline or an HTTP client, so it can carry broker
	// addresses, a topic name or a whole response body, at any length and with any control
	// characters in it. Interpolating it with %v applied no length bound and let a newline
	// forge a second entry in a line-oriented aggregator.
	var reported *logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel &&
			strings.Contains(entry.Message, "could not be published") {
			reported = entry

			break
		}
	}

	require.NotNil(t, reported,
		"a sender failure must be logged at error level: it means the system.error event was "+
			"never recorded, and there is no other producer for it")
	assert.Contains(t, reported.Data["sender_error"], "the outbox insert failed",
		"and it must carry the underlying reason, or the failure is unactionable")
	assert.Contains(t, reported.Data, "correlation_id",
		"the failure line must carry the correlation id that ties it to the line holding the full "+
			"error text, or an operator has a delivery failure with nothing to correlate it to")
}

// TestNotifyError_BothTransportsConfigured_SenderInvokedExactlyOnce guards against the
// mirror-image mistake: dispatching twice.
//
// The count is read from an atomic rather than the channel alone so that a third invocation,
// which would block on a full channel, still reaches the assertion.
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

	awaitCompletion := awaitNotifyError(t)

	NotifyError(errors.New("dispatched once, never twice"))

	awaitCompletion()

	select {
	case event := <-events:
		assert.Equal(t, "system.error", event)
	default:
		t.Fatal("the webhook sender was never invoked with both transports configured")
	}

	select {
	case event := <-events:
		t.Fatalf("system.error was dispatched twice (second invocation %q): the gate must "+
			"remain ONE disjunction over the two transports, not one branch per "+
			"transport, or every dual-delivery deployment emits two outbox rows with two "+
			"different event ids for a single error", event)
	default:
		// Expected: exactly one dispatch, however many transports are configured. Both
		// observations are made after the notifier finished, so a second dispatch could not
		// still be in flight.
	}

	assert.Equal(t, int32(1), invocations.Load(),
		"system.error must be handed to the sender exactly once per error regardless of "+
			"how many transports are configured")
}

// TestNotifyError_KafkaConfiguredWithNoSenderRegistered_DoesNotPanic proves the nil check still
// short-circuits. The process registers its sender during construction, so an error notified
// before that point — or in a role that never registers one — finds a nil sender with Kafka fully
// configured, and a nil call panics inside NotifyError's goroutine where no caller can recover it.
//
// THE NIL CHECK MUST REMAIN A CONJUNCT: `sender != nil` and `configured && sender != nil` are both
// safe, since every conjunct must hold before the call; folding the nil check INTO the disjunction
// is not, because a configured transport alone would then satisfy it.
//
// The nil is installed through RegisterWebhookSender rather than by assigning the global, so the
// mutex guarding it is taken and the test stays clean under the race detector.
func TestNotifyError_KafkaConfiguredWithNoSenderRegistered_DoesNotPanic(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)
	RegisterWebhookSender(nil)

	storeGateConfig(t, []string{gateBroker}, "", "")

	awaitCompletion := awaitNotifyError(t)

	assert.NotPanics(t, func() {
		NotifyError(errors.New("no sender to hand this to"))
		// Waits for the goroutine to REACH AND PASS the gate rather than sleeping for a
		// duration that might not cover it. A nil-sender panic happens inside that goroutine
		// and cannot be recovered by this NotPanics — it takes the test binary down — so the
		// value of the wait is that the panic, if any, has already happened by the time this
		// returns.
		awaitCompletion()
	})
}

// TestNotifyError_KafkaConfigured_PayloadShapeIsUnchanged pins the payload the sender receives, so
// that widening the gate cannot quietly change WHAT is dispatched while changing WHEN.
//
// # The shape asserted here is the FROZEN LEGACY one, and the name means what it says
//
// Requirement R-8 requires a LedgerEvent's payload to match today's webhook body
// FIELD-FOR-FIELD, and the webhook body for system.error has always been two keys: the
// rendered error and the time. Every subscriber's parser is written against them, so the
// migration must move the transport and leave the body alone — a subscriber re-points its
// consumer and its body handling keeps working.
//
// An earlier revision of this test asserted a THREE-KEY sanitized shape instead: a classified
// reason, a correlation id and a time, with the error key asserted absent. The concern behind
// that change is real — Blnk's internal errors render with schema, constraint and routine
// names, with internal addresses, and sometimes with a quoted account reference or a DSN
// password, and system.error is published to a replicated, retained topic — but substituting
// one payload for another under the same event name is a BREAKING CHANGE to a published
// contract, delivered as a side effect of a transport migration. It is addressed instead by
// two things that cost no contract anything: system.error routes to blnk.system, which
// model.SubscriberGrantableEventCategories withholds from every subscriber, so no credential
// Blnk issues can read this body at all — and the bounded classification is logged rather than
// published. The audience for this body is therefore the OPERATOR and nobody else, which is
// what model.EventCategorySystem records.
//
// The LENGTH ASSERTION is the point of this test: a third key is a change to a published
// contract, and it should fail here rather than reach a subscriber. The three keys the
// sanitized revision published are asserted ABSENT for exactly that reason.
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

	awaitCompletion := awaitNotifyError(t)
	NotifyError(systemError)
	awaitCompletion()

	select {
	case got := <-calls:
		assert.Equal(t, "system.error", got.event)

		payloadMap, ok := got.payload.(map[string]interface{})
		require.True(t, ok, "payload should be a map, got %T", got.payload)

		assert.Equal(t, systemError.Error(), payloadMap["error"],
			"the error text is the value subscribers parse; it must be carried verbatim")

		ts, ok := payloadMap["time"].(time.Time)
		require.True(t, ok, "payload time should be a time.Time")
		assert.WithinDuration(t, time.Now(), ts, 10*time.Second)

		assert.Len(t, payloadMap, 2,
			"the system.error payload is a published contract: error and time, and nothing else")
		assert.NotContains(t, payloadMap, "reason",
			"a classified diagnosis belongs on the log line, not in a frozen payload")
		assert.NotContains(t, payloadMap, "correlation_id")
		assert.NotContains(t, payloadMap, "error_code")
	default:
		t.Fatal("the webhook sender was never invoked, so the payload could not be inspected")
	}
}

// TestStoreGateConfig_RestoresTheProcessGlobalConfiguration is the guard for the leak itself.
//
// Every other test in this package asserts something about NotifyError; this one asserts
// something about the HELPERS, because a leaked global is invisible to all of them. The
// symptom of the leak is not a failure here — it is a failure in some other package's test,
// or in a test added to this one later, caused by a configuration this file stored and never
// took back. That is why the property is pinned directly rather than trusted to be preserved.
func TestStoreGateConfig_RestoresTheProcessGlobalConfiguration(t *testing.T) {
	// A recognisable configuration standing in for "whatever was here before".
	const sentinelWebhookURL = "https://sentinel.example.com/pre-existing"

	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{Url: sentinelWebhookURL},
		},
	})

	before, err := config.Fetch()
	require.NoError(t, err)
	require.Equal(t, sentinelWebhookURL, before.Notification.Webhook.Url,
		"the sentinel must be in place, or this test proves nothing")

	// A nested test is the only way to observe a t.Cleanup running: the helper registers its
	// restore against the subtest's lifetime, so the assertion after Run sees the restored
	// state.
	t.Run("inner", func(t *testing.T) {
		storeGateConfig(t, []string{gateBroker}, "", "")

		during, fetchErr := config.Fetch()
		require.NoError(t, fetchErr)
		assert.Equal(t, []string{gateBroker}, during.Kafka.Brokers,
			"the helper must actually take effect inside the test that called it")
		assert.Empty(t, during.Notification.Webhook.Url)
	})

	after, err := config.Fetch()
	require.NoError(t, err)
	assert.Equal(t, sentinelWebhookURL, after.Notification.Webhook.Url,
		"THE LEAK: the configuration the helper replaced must be back. Leaving the Kafka "+
			"broker list and the empty webhook URL in the global store makes every later test "+
			"in the binary run against a configuration it did not ask for, and the resulting "+
			"failure is order-dependent and points at the wrong file")
	assert.Empty(t, after.Kafka.Brokers,
		"and the broker list the helper set must be gone")
}

// TestStoreNotificationConfig_RestoresTheProcessGlobalConfiguration covers the sibling helper.
//
// Both helpers exist and both replace the same global, so a restore in one and not the other
// leaks exactly as badly as no restore at all — and is harder to spot, because half the file
// looks correct. They share one implementation for that reason, and this test is what keeps
// the sharing honest.
func TestStoreNotificationConfig_RestoresTheProcessGlobalConfiguration(t *testing.T) {
	const sentinelSlackURL = "https://sentinel.example.com/slack"

	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Notification: config.Notification{
			Slack: config.SlackWebhook{WebhookUrl: sentinelSlackURL},
		},
	})

	t.Run("inner", func(t *testing.T) {
		storeNotificationConfig(t, "", "https://subscriber.example.com/hook")

		during, fetchErr := config.Fetch()
		require.NoError(t, fetchErr)
		assert.Equal(t, "https://subscriber.example.com/hook", during.Notification.Webhook.Url)
	})

	after, err := config.Fetch()
	require.NoError(t, err)
	assert.Equal(t, sentinelSlackURL, after.Notification.Slack.WebhookUrl,
		"the sibling helper must restore the global too")
	assert.Empty(t, after.Notification.Webhook.Url)
}
