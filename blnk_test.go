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

// Tests for the service container's event-publisher wiring in blnk.go.
//
// Three properties are pinned here, and each of them is a thing that breaks quietly rather
// than loudly if it regresses:
//
//  1. GRACEFUL DEGRADATION. With no Kafka brokers configured, NewBlnk must succeed and must
//     select the no-op publisher. This is the property that keeps every deployment which
//     does not run Kafka — and the large part of this test suite that constructs
//     NewBlnk(nil) with nothing but a Redis DSN — working unchanged. A constructor that
//     returned an error, dialled a broker or blocked here would take all of them down at
//     once.
//  2. NO I/O AT CONSTRUCTION, on the configured path too. Brokers pointed at an
//     unroutable address must still produce a publisher promptly, because writers connect
//     lazily. If construction ever dialled, process startup would begin to depend on broker
//     reachability.
//  3. system.error IS CAPTURED. blnk.go registers the notification package's WebhookSender,
//     and that closure is the eighth and only indirect producer call site. If it is not
//     routed at the outbox, one of the thirteen event types silently disappears from the
//     pipeline while every other one keeps flowing.
//
// Nothing here performs network I/O. Redis is miniredis, no datasource is contacted, and the
// only broker addresses used are RFC 5737 documentation addresses that are never dialled.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
)

// wiringBlackholeBroker is an RFC 5737 TEST-NET-2 documentation address. It is reserved for
// documentation and is not routable, so if any code path under test ever did dial a broker
// the failure would be an unambiguous timeout rather than a connection to something real.
const wiringBlackholeBroker = "198.51.100.1:9092"

// wiringConstructionBudget bounds how long NewBlnk may take.
//
// It is deliberately well under eventTransportDialTimeout (5s): a single dial attempt
// against wiringBlackholeBroker could not finish inside this budget, so exceeding it is
// evidence that construction performed I/O rather than merely that the machine was busy.
const wiringConstructionBudget = 2 * time.Second

// wiringSpyDatasource records the event-outbox rows PublishEvent inserts.
//
// The embedded *mocks.MockDataSource supplies the remainder of database.IDataSource and
// carries NO expectations, which is itself an assertion: any other datasource method this
// path touched would panic rather than pass unnoticed.
//
// Rows are delivered over a buffered channel rather than accumulated behind a mutex because
// the only writer is the goroutine notification.NotifyError spawns, and the reader needs to
// block until it arrives. The channel is the synchronisation.
type wiringSpyDatasource struct {
	*mocks.MockDataSource

	rows chan *model.EventOutbox
}

// newWiringSpyDatasource returns a spy wired for use as the Blnk datasource.
func newWiringSpyDatasource() *wiringSpyDatasource {
	return &wiringSpyDatasource{
		MockDataSource: new(mocks.MockDataSource),
		// Buffered so a row is never lost if the test is not yet waiting, and so the
		// producing goroutine can never block on a test that has already failed.
		rows: make(chan *model.EventOutbox, 4),
	}
}

// InsertEventOutbox records a standalone outbox insert.
func (s *wiringSpyDatasource) InsertEventOutbox(_ context.Context, e *model.EventOutbox) error {
	select {
	case s.rows <- e:
	default:
	}

	return nil
}

// await returns the next recorded row, failing the test if none arrives.
func (s *wiringSpyDatasource) await(t *testing.T) *model.EventOutbox {
	t.Helper()

	select {
	case row := <-s.rows:
		return row
	case <-time.After(5 * time.Second):
		t.Fatal("no event-outbox row was recorded; the registered WebhookSender did not reach PublishEvent")

		return nil
	}
}

// wiringRecordingPublisher is a publisher that counts Close calls.
//
// It embeds *NoopEventPublisher so it satisfies the whole TopicEventPublisher contract
// without restating it, and overrides only Close, which is the single behaviour under test.
type wiringRecordingPublisher struct {
	*NoopEventPublisher

	guard  sync.Mutex
	closes int
	err    error
}

// Close records the call and returns the configured error.
func (p *wiringRecordingPublisher) Close() error {
	p.guard.Lock()
	defer p.guard.Unlock()

	p.closes++

	return p.err
}

// closeCount reports how many times Close was called.
func (p *wiringRecordingPublisher) closeCount() int {
	p.guard.Lock()
	defer p.guard.Unlock()

	return p.closes
}

// wiringStoreConfiguration publishes cnf for the duration of one test and restores whatever
// was there before.
//
// config.ConfigStore is a process-global atomic.Value, so a configuration leaked out of one
// test produces confusing failures in unrelated ones. It writes DIRECTLY rather than through
// config.MockConfig because MockConfig runs validateAndAddDefaults, which refuses a
// configuration lacking a data-source DSN — the value would be silently dropped — and which
// would also supply the very Kafka defaults some cases here need to observe as absent.
func wiringStoreConfiguration(t *testing.T, cnf *config.Configuration) {
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

// wiringRedisConfiguration returns a configuration whose only mandatory field is set.
//
// A Redis DSN is all NewBlnk genuinely requires, and miniredis provides one without a
// server. This mirrors exactly what the legacy webhook tests construct, which is the input
// the event-publisher wiring must not break.
func wiringRedisConfiguration(t *testing.T) *config.Configuration {
	t.Helper()

	server := miniredis.RunT(t)

	return &config.Configuration{
		Redis: config.RedisConfig{Dns: server.Addr()},
	}
}

// wiringNewBlnkWithin constructs a Blnk instance and fails the test if it takes longer than
// wiringConstructionBudget.
//
// The bound is the assertion, not a convenience: "construction performs no I/O" is only
// observable as "construction did not wait". Running the constructor on its own goroutine
// means a genuinely blocking call fails the test with a clear message instead of hanging
// until the package-level test timeout kills the whole run.
func wiringNewBlnkWithin(t *testing.T, db *wiringSpyDatasource) (*Blnk, error) {
	t.Helper()

	type outcome struct {
		instance *Blnk
		err      error
	}

	done := make(chan outcome, 1)
	go func() {
		// A nil *wiringSpyDatasource must be passed as a nil INTERFACE, not as a typed
		// nil pointer: a typed nil would satisfy database.IDataSource and defeat the
		// nil-datasource handling the legacy webhook tests rely on.
		if db == nil {
			instance, err := NewBlnk(nil)
			done <- outcome{instance: instance, err: err}

			return
		}

		instance, err := NewBlnk(db)
		done <- outcome{instance: instance, err: err}
	}()

	select {
	case result := <-done:
		if result.instance != nil {
			t.Cleanup(func() {
				assert.NoError(t, result.instance.Close(), "closing the Blnk instance must not fail")
			})
		}

		return result.instance, result.err
	case <-time.After(wiringConstructionBudget):
		t.Fatalf("NewBlnk did not return within %s; construction must perform no I/O", wiringConstructionBudget)

		return nil, nil
	}
}

// TestNewBlnk_SelectsTheNoopEventPublisherWhenNoBrokersAreConfigured is the
// graceful-degradation acceptance criterion.
//
// An empty broker list is a legitimate steady state, not a misconfiguration: it reproduces
// SendWebhook's own no-op-when-unconfigured contract, and it is the reason a Blnk deployment
// has always been able to run with no notification sink at all. The three cases below are
// the three ways "no brokers" actually reaches the constructor — the field never set, an
// explicitly empty list, and a list holding nothing but blank separators left in an
// environment file — and all three must resolve identically.
//
// A whitespace entry is the interesting one. Treated as a broker it would produce a
// kafkaPublisher that can never connect, so every publish would fail against an address that
// is not an address, and nothing in the configuration would look wrong.
func TestNewBlnk_SelectsTheNoopEventPublisherWhenNoBrokersAreConfigured(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		brokers []string
	}{
		{name: "the Kafka block is never set", brokers: nil},
		{name: "an explicitly empty broker list", brokers: []string{}},
		{name: "a broker list of blank separators", brokers: []string{"", "   ", "\t"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cnf := wiringRedisConfiguration(t)
			cnf.Kafka.Brokers = testCase.brokers
			wiringStoreConfiguration(t, cnf)

			instance, err := wiringNewBlnkWithin(t, nil)
			require.NoError(t, err,
				"an unconfigured broker list must never fail construction; every deployment without Kafka depends on it")
			require.NotNil(t, instance)

			require.NotNil(t, instance.events,
				"the events field must always be populated so Close and the relay never have to nil-check it")
			assert.True(t, IsNoopEventPublisher(instance.events),
				"no brokers must select the no-op publisher")
			assert.IsType(t, &NoopEventPublisher{}, instance.events,
				"the no-op is the only implementation that provably performs no I/O, which is what makes this safe")

			// The legacy transport is untouched by the event wiring. Asserting it here is
			// what proves the change was additive rather than a substitution.
			require.NotNil(t, instance.httpClient, "the legacy HTTP transport must survive the dual-delivery window")
			assert.Equal(t, 30*time.Second, instance.httpClient.Timeout)
		})
	}
}

// TestNewBlnk_ConstructsTheKafkaPublisherWithoutDiallingABroker pins the constructor's
// load-bearing property on the CONFIGURED path.
//
// The broker address is unroutable on purpose. If construction dialled, negotiated SASL or
// fetched partition metadata, this test could not pass inside its budget — so passing is
// evidence of the absence, which is the only way to test for it. That absence is what lets
// a process start before its broker does, and what keeps startup independent of broker
// reachability.
//
// InsecureLocalDev is set because the transport refuses to dial a plaintext broker without
// it; see the sibling case below, which asserts that refusal.
func TestNewBlnk_ConstructsTheKafkaPublisherWithoutDiallingABroker(t *testing.T) {
	cnf := wiringRedisConfiguration(t)
	cnf.Kafka = config.KafkaConfig{
		Brokers:          []string{wiringBlackholeBroker},
		TopicPrefix:      DefaultTopicPrefix,
		SASLUser:         "blnk-producer",
		SASLSecret:       "producer-secret",
		InsecureLocalDev: true,
	}
	wiringStoreConfiguration(t, cnf)

	instance, err := wiringNewBlnkWithin(t, nil)
	require.NoError(t, err)
	require.NotNil(t, instance)

	require.NotNil(t, instance.events)
	assert.False(t, IsNoopEventPublisher(instance.events),
		"a configured broker list must select the Kafka publisher, not the no-op")
	assert.IsType(t, &kafkaPublisher{}, instance.events)
}

// TestNewBlnk_PropagatesAnEventPublisherConfigurationFailure covers the one case in which
// the publisher legitimately fails construction.
//
// SASL/SCRAM authenticates the client to the broker; it does not encrypt the connection. So
// a configured broker with TLS disabled and no explicit local-development acknowledgement
// would put ledger amounts, identity records and the SCRAM handshake itself on the wire in
// the clear. That is refused, and the refusal must reach the CALLER: silently degrading to
// the no-op would leave a deployment that believes it is publishing events emitting nothing
// at all, which is precisely the failure the loud error exists to prevent.
func TestNewBlnk_PropagatesAnEventPublisherConfigurationFailure(t *testing.T) {
	cnf := wiringRedisConfiguration(t)
	cnf.Kafka = config.KafkaConfig{
		Brokers:     []string{wiringBlackholeBroker},
		TopicPrefix: DefaultTopicPrefix,
		// TLS.Enabled is false and InsecureLocalDev is not set: neither encrypted nor
		// acknowledged.
	}
	wiringStoreConfiguration(t, cnf)

	instance, err := wiringNewBlnkWithin(t, nil)
	require.Error(t, err,
		"an insecure Kafka configuration must fail construction rather than be downgraded to the no-op")
	assert.Nil(t, instance, "a failed construction must not hand back a half-built instance")
	assert.Contains(t, err.Error(), "KAFKA_INSECURE_LOCAL_DEV",
		"the error must name the acknowledgement an operator has to make, or it is not actionable")
}

// TestNewBlnk_RegistersTheSystemErrorSenderIntoTheEventOutbox proves the eighth producer
// call site is covered.
//
// system.error is the only event type emitted indirectly: internal/notification cannot
// import this package, so it holds a registered WebhookSender and NotifyError calls through
// it. NewBlnk registering that closure is what captures the event, and routing the closure
// at PublishEvent is what puts it in the outbox alongside the other twelve — earning it the
// same durable retry, dead-lettering and replay.
//
// BOTH transports are exercised because NotifyError guards the call on either being
// configured. The Kafka-only case is the one that used to be unreachable: a guard testing
// the webhook URL alone would drop system.error on every Kafka-only deployment, and nothing
// would fail to say so.
func TestNewBlnk_RegistersTheSystemErrorSenderIntoTheEventOutbox(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		apply func(*config.Configuration)
	}{
		{
			name: "kafka only, as a post-migration deployment is configured",
			apply: func(cnf *config.Configuration) {
				cnf.Kafka = config.KafkaConfig{
					Brokers:          []string{wiringBlackholeBroker},
					TopicPrefix:      DefaultTopicPrefix,
					InsecureLocalDev: true,
				}
			},
		},
		{
			name: "legacy webhook only, as a deployment inside the dual-delivery window is",
			apply: func(cnf *config.Configuration) {
				cnf.Notification.Webhook.Url = "http://webhook.invalid/blnk"
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cnf := wiringRedisConfiguration(t)
			testCase.apply(cnf)
			wiringStoreConfiguration(t, cnf)

			// The sender is process-global. Clearing it afterwards stops this test's
			// instance from serving a later one; every test that needs a sender either
			// registers its own or constructs a Blnk.
			t.Cleanup(func() { notification.RegisterWebhookSender(nil) })

			datasource := newWiringSpyDatasource()
			instance, err := wiringNewBlnkWithin(t, datasource)
			require.NoError(t, err)
			require.NotNil(t, instance)

			systemError := errors.New("wiring test: a system error worth notifying about")
			notification.NotifyError(systemError)

			row := datasource.await(t)
			require.NotNil(t, row)

			assert.Equal(t, "system.error", row.EventType,
				"the closure must forward the event name it was handed, unaltered")
			assert.Equal(t, TopicForEvent("system.error"), row.Topic)
			assert.Equal(t, model.EventOutboxStatusPending, row.Status,
				"the row must be left for the relay to claim")

			// THE PAYLOAD GUARANTEE, asserted rather than described: the stored bytes are
			// the marshaled two-key legacy webhook body — {"event": ..., "data": ...} —
			// carrying the payload object NotifyError built, field for field. The closure
			// in blnk.go forwards what it is handed and adds nothing.
			var body struct {
				Event string `json:"event"`
				Data  struct {
					Reason        string    `json:"reason"`
					CorrelationID string    `json:"correlation_id"`
					Time          time.Time `json:"time"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(row.Payload, &body),
				"the payload must be the marshaled NewWebhook object")
			assert.Equal(t, "system.error", body.Event, "the outer envelope keeps its event key")
			assert.Equal(t, notification.SystemErrorReasonUnclassified, body.Data.Reason,
				"the inner data object keeps the classification NotifyError assigned")
			assert.NotEmpty(t, body.Data.CorrelationID,
				"the correlation id is the only handle back to the full error text, so it must survive the round trip")
			assert.False(t, body.Data.Time.IsZero(), "NotifyError's timestamp must survive the round trip")

			// The notification package deliberately keeps the raw error text OUT of the
			// payload and logs it against the correlation id instead. Asserting the
			// absence here is what proves the wiring is a pass-through: a closure that
			// re-derived or enriched the payload on its way to the outbox would put the
			// text back and silently undo that sanitization.
			assert.NotContains(t, string(row.Payload), systemError.Error(),
				"the sanitized payload must not carry the raw error text")
		})
	}
}

// TestBlnkClose_ReleasesTheEventPublisherAndStaysNilSafe covers shutdown.
//
// A publisher's writers hold pooled broker connections and a SASL session per broker, and
// closing a writer flushes what it has batched. Leaving them open leaks the connections and
// loses events that were accepted but not yet produced, and neither is visible at the time.
//
// The nil case matters just as much. A Blnk built as a struct literal — which many tests do
// — has no publisher and no asynq client, and Close must stay silent rather than panic on
// the nil interface.
func TestBlnkClose_ReleasesTheEventPublisherAndStaysNilSafe(t *testing.T) {
	t.Run("the publisher is closed exactly once", func(t *testing.T) {
		publisher := &wiringRecordingPublisher{NoopEventPublisher: NewNoopEventPublisher()}
		instance := &Blnk{events: publisher}

		require.NoError(t, instance.Close())
		assert.Equal(t, 1, publisher.closeCount(),
			"Close must release the publisher, or its broker connections leak for the life of the process")
	})

	t.Run("a publisher close failure is reported", func(t *testing.T) {
		failure := errors.New("wiring test: the writer refused to close")
		publisher := &wiringRecordingPublisher{NoopEventPublisher: NewNoopEventPublisher(), err: failure}
		instance := &Blnk{events: publisher}

		err := instance.Close()
		require.Error(t, err, "a failed publisher close must not be swallowed")
		assert.ErrorIs(t, err, failure)
	})

	t.Run("an unset publisher is skipped", func(t *testing.T) {
		instance := &Blnk{}

		assert.NoError(t, instance.Close(),
			"Close must remain nil-safe: a struct-literal Blnk has neither a publisher nor an asynq client")
	})

	t.Run("the no-op publisher closes cleanly", func(t *testing.T) {
		instance := &Blnk{events: NewNoopEventPublisher()}

		assert.NoError(t, instance.Close(),
			"the no-op releases nothing because it acquired nothing")
	})
}

// TestInitializeEventPublisher_NeverReturnsANilPublisher pins the field invariant the
// wiring site establishes.
//
// Close and, once it exists, the relay both read b.events without a nil check, and they can
// only do that because this constructor guarantees a value. A nil configuration is included
// because it is genuinely reachable: a process whose configuration has not been loaded yet
// resolves one.
func TestInitializeEventPublisher_NeverReturnsANilPublisher(t *testing.T) {
	for _, testCase := range []struct {
		name string
		cnf  *config.Configuration
	}{
		{name: "a nil configuration", cnf: nil},
		{name: "an empty configuration", cnf: &config.Configuration{}},
		{name: "an empty broker list", cnf: &config.Configuration{Kafka: config.KafkaConfig{Brokers: []string{}}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			publisher, err := initializeEventPublisher(testCase.cnf)
			require.NoError(t, err, fmt.Sprintf("%s must not fail construction", testCase.name))
			require.NotNil(t, publisher)
			assert.True(t, IsNoopEventPublisher(publisher))
		})
	}
}
