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

// Tests for the event publisher: the contract it exposes, the settings it publishes with, and
// the process-wide lifecycle of the instance that does the publishing.
//
// The first half covers the PROCESS-WIDE LIFECYCLE — which instance a caller gets, when it is
// rebuilt, and who may close it. The second half, from "The mandated contract, the writer
// settings, and the wire form of one publish", covers ONE PUBLISH. Both rest on the same
// load-bearing property: construction performs no I/O, so a publisher can be built, inspected
// and published through in a unit test with no broker anywhere.
//
// A publisher is one Kafka writer per owned topic over ONE SHARED TRANSPORT, so the writers
// share its connection pool and its authentication configuration; connections are established
// lazily per broker and reused. Constructing it performs no I/O at all — kafka.TCP only
// canonicalises address strings and a SCRAM mechanism is pure computation — but the FIRST WRITE
// through the transport dials a broker and authenticates. That asymmetry is the subject of the
// lifecycle half: a publisher is cheap to build and expensive to use for the first time, so
// building one per request means continuous connection and authentication churn.
//
// The lifecycle properties are therefore about IDENTITY and OWNERSHIP: one instance is reused
// across callers; a configuration change rebuilds it so a rotated SASL credential takes effect;
// the cache key never carries a credential in the clear; and an INJECTED publisher is never
// closed or replaced by this package, because the relay's publisher outlives anything that
// borrows it.
//
// No lifecycle test contacts a broker. The publish tests DO write, but every write is
// intercepted at kafka.Writer.Transport by an in-memory fake round tripper, so the whole file
// runs under `go test -short` with no Kafka, no credentials and — as one of its own assertions
// proves — no name resolution.

package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// sharedPublisherReset returns the process-wide publisher slot to its initial state, both
// before and after a test that touches it.
//
// Both ends matter. A shared slot is global state: a publisher left behind by an earlier
// test would be handed to this one, and one left behind by this test would be handed to
// whatever runs next. Resetting on entry and on cleanup keeps every test in this group
// independent of ordering.
func sharedPublisherReset(t *testing.T) {
	t.Helper()

	require.NoError(t, CloseSharedEventPublisher())
	t.Cleanup(func() {
		if err := CloseSharedEventPublisher(); err != nil {
			t.Logf("failed to close the shared event publisher: %v", err)
		}
	})
}

// storeKafkaConfig publishes a full Kafka configuration for the duration of one test and
// restores whatever was there before.
//
// A BROKER LIST IS ESSENTIAL to these tests, not incidental. With no brokers configured
// NewEventPublisher correctly resolves *NoopEventPublisher, which is an EMPTY STRUCT — and
// Go gives pointers to distinct zero-size values an implementation-defined identity, so two
// separately built no-op publishers can compare equal. Any identity assertion made against
// them proves nothing, in either direction. Configuring brokers makes the resolved publisher
// a *kafkaPublisher, a real object at a real address, so "the same instance" and "a
// different instance" become statements that can actually be tested.
//
// Publishing this configuration dials nothing: construction performs no I/O by design, and
// no test here ever writes through the publisher it resolves.
func storeKafkaConfig(t *testing.T, kafka config.KafkaConfig) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(&config.Configuration{Kafka: kafka})
}

// TestSharedEventPublisher_ReusesOneInstancePerProcess is the connection-reuse property.
//
// Building a publisher costs no I/O, but the first write through its shared transport dials a
// broker and authenticates. A caller that builds one per request pays that setup per request
// and then tears it down, which under concurrent replays is continuous churn. Handing back the
// same instance removes it, so IDENTITY is the property asserted rather than "a publisher is
// returned".
func TestSharedEventPublisher_ReusesOneInstancePerProcess(t *testing.T) {
	sharedPublisherReset(t)
	storeKafkaConfig(t, config.KafkaConfig{
		Brokers:     []string{"broker-1:9092"},
		TopicPrefix: DefaultTopicPrefix,
		// The shared transport refuses a plaintext broker without this acknowledgement,
		// because SASL over plaintext puts the credential on the wire in the clear.
		InsecureLocalDev: true,
	})

	first, err := SharedEventPublisher()
	require.NoError(t, err)
	require.NotNil(t, first)
	require.IsType(t, &kafkaPublisher{}, first,
		"the identity assertion below is only meaningful against a real publisher; see storeKafkaConfig")

	second, err := SharedEventPublisher()
	require.NoError(t, err)

	assert.Same(t, first, second,
		"repeated calls must hand back ONE instance; a new publisher per call is the per-request transport churn this exists to remove")
}

// TestSharedEventPublisher_RebuildsWhenTheConfigurationChanges covers the case that makes a
// cached publisher dangerous rather than merely stale.
//
// A rotated SASL credential must take effect. A publisher still holding the previous one
// authenticates as nobody, and every publish through it fails — for a reason that looks
// nothing like "the credential changed". Keying the cache on a fingerprint of the broker
// list, the topic prefix and the SASL identity is what makes a rotation land, and a change to
// anything else leave the instance alone.
func TestSharedEventPublisher_RebuildsWhenTheConfigurationChanges(t *testing.T) {
	sharedPublisherReset(t)
	storeKafkaConfig(t, config.KafkaConfig{
		Brokers:     []string{"broker-1:9092"},
		TopicPrefix: DefaultTopicPrefix,
		// The shared transport refuses a plaintext broker without this acknowledgement,
		// because SASL over plaintext puts the credential on the wire in the clear.
		InsecureLocalDev: true,
	})

	first, err := SharedEventPublisher()
	require.NoError(t, err)
	require.IsType(t, &kafkaPublisher{}, first)

	// A different topic prefix is a different set of topics, and therefore a different
	// publisher: the writers it holds are keyed by topic name.
	storeKafkaConfig(t, config.KafkaConfig{
		Brokers:     []string{"broker-1:9092"},
		TopicPrefix: "acme",
		// The shared transport refuses a plaintext broker without this acknowledgement,
		// because SASL over plaintext puts the credential on the wire in the clear.
		InsecureLocalDev: true,
	})

	second, err := SharedEventPublisher()
	require.NoError(t, err)

	assert.NotSame(t, first, second,
		"a configuration change must produce a new publisher, or a rotated credential would never take effect")

	third, err := SharedEventPublisher()
	require.NoError(t, err)
	assert.Same(t, second, third,
		"and the new one must then be reused; rebuilding on every call would reintroduce the churn")
}

// TestSharedEventPublisher_FingerprintCoversEveryFieldThatChangesThePublisher pins what the
// cache key is derived from.
//
// # Under-covering is the dangerous direction, and it was under-covered
//
// The fingerprint IS the cache key, so a transport-affecting field omitted from it is a
// configuration change that never takes effect — silently, for the lifetime of the process,
// while the configuration file says otherwise. Three groups were missing and each had a
// concrete cost:
//
//   - The PRODUCER pair, which is the credential kafkaTransportCredentials prefers. Rotating
//     KAFKA_SASL_SECRET was inert, and it was MOST inert on the recommended least-privilege
//     deployment: one with a producer principal and no administrative credentials in the
//     publishing process.
//   - The whole TLS block. A deployment that switched from SASL_PLAINTEXT to SASL_SSL kept
//     publishing in the clear.
//   - InsecureSkipVerify and InsecureLocalDev. Turning verification back on did nothing.
//
// Over-covering merely rebuilds more often than necessary, so the equality case is asserted
// too — and the geometry fields, which belong to topic assurance rather than to the writer,
// are asserted NOT to rebuild a working transport.
//
// The base configuration below sets every field to a non-zero value, which is what makes each
// mutation discriminating: mutating a field that was already zero to another zero-ish value
// would produce an equal digest for the right reason and hide the wrong one.
func TestSharedEventPublisher_FingerprintCoversEveryFieldThatChangesThePublisher(t *testing.T) {
	base := &config.Configuration{Kafka: config.KafkaConfig{
		Brokers:         []string{"broker-1:9092"},
		TopicPrefix:     "blnk",
		SASLUser:        "producer",
		SASLSecret:      "producer-secret",
		SASLAdminUser:   "admin",
		SASLAdminSecret: "secret",
		TLS: config.KafkaTLSConfig{
			Enabled:            true,
			CAFile:             "/etc/blnk/kafka/ca.pem",
			CertFile:           "/etc/blnk/kafka/client.pem",
			KeyFile:            "/etc/blnk/kafka/client-key.pem",
			ServerName:         "kafka.internal.example.com",
			InsecureSkipVerify: false,
		},
		InsecureLocalDev:     false,
		MinPartitions:        6,
		ReplicationFactor:    3,
		AllowPartitionGrowth: false,
	}}
	baseline := eventPublisherFingerprint(base)

	t.Run("differs on", func(t *testing.T) {
		for name, mutate := range map[string]func(*config.KafkaConfig){
			"the broker list":     func(k *config.KafkaConfig) { k.Brokers = []string{"broker-2:9092"} },
			"an added broker":     func(k *config.KafkaConfig) { k.Brokers = []string{"broker-1:9092", "broker-2:9092"} },
			"the topic prefix":    func(k *config.KafkaConfig) { k.TopicPrefix = "acme" },
			"the admin user":      func(k *config.KafkaConfig) { k.SASLAdminUser = "other" },
			"the admin secret":    func(k *config.KafkaConfig) { k.SASLAdminSecret = "rotated" },
			"the producer user":   func(k *config.KafkaConfig) { k.SASLUser = "other-producer" },
			"the producer secret": func(k *config.KafkaConfig) { k.SASLSecret = "rotated" },
			"a producer pair removed": func(k *config.KafkaConfig) {
				// Falling back to the administrative pair is a DIFFERENT transport identity,
				// and it is the transition an operator makes in the wrong direction by
				// clearing a variable.
				k.SASLUser = ""
				k.SASLSecret = ""
			},
			"TLS being disabled":       func(k *config.KafkaConfig) { k.TLS.Enabled = false },
			"the CA bundle":            func(k *config.KafkaConfig) { k.TLS.CAFile = "/etc/blnk/kafka/rotated-ca.pem" },
			"the client certificate":   func(k *config.KafkaConfig) { k.TLS.CertFile = "/etc/blnk/kafka/renewed.pem" },
			"the client key":           func(k *config.KafkaConfig) { k.TLS.KeyFile = "/etc/blnk/kafka/renewed-key.pem" },
			"the expected server name": func(k *config.KafkaConfig) { k.TLS.ServerName = "kafka.other.example.com" },
			"skipping certificate verification": func(k *config.KafkaConfig) {
				k.TLS.InsecureSkipVerify = true
			},
			"acknowledged plaintext development": func(k *config.KafkaConfig) {
				k.InsecureLocalDev = true
			},
		} {
			t.Run(name, func(t *testing.T) {
				changed := &config.Configuration{Kafka: base.Kafka}
				mutate(&changed.Kafka)

				assert.NotEqual(t, baseline, eventPublisherFingerprint(changed),
					"a change to %s must produce a different publisher: an unchanged digest means "+
						"the cached transport — its connections, its SASL session and its TLS "+
						"configuration — keeps being handed out, so the change never takes effect", name)
			})
		}
	})

	t.Run("is stable for an equivalent configuration", func(t *testing.T) {
		same := &config.Configuration{Kafka: base.Kafka}
		assert.Equal(t, baseline, eventPublisherFingerprint(same),
			"an equivalent configuration must reuse the publisher rather than rebuild it")
	})

	t.Run("does not rebuild for topic geometry", func(t *testing.T) {
		// Geometry is the ADMIN client's business — EnsureTopics reads it — and the writer
		// never does. Rebuilding the publisher for it would discard a working connection pool
		// and a live SASL session to change a number nothing on this path consults.
		for name, mutate := range map[string]func(*config.KafkaConfig){
			"the partition floor":    func(k *config.KafkaConfig) { k.MinPartitions = 12 },
			"the replication factor": func(k *config.KafkaConfig) { k.ReplicationFactor = 1 },
			"partition growth":       func(k *config.KafkaConfig) { k.AllowPartitionGrowth = true },
		} {
			t.Run(name, func(t *testing.T) {
				changed := &config.Configuration{Kafka: base.Kafka}
				mutate(&changed.Kafka)

				assert.Equal(t, baseline, eventPublisherFingerprint(changed),
					"%s does not affect the transport, so it must not throw one away", name)
			})
		}
	})

	t.Run("does not collide across a field boundary", func(t *testing.T) {
		// Without a separator between fields, "admin"+"secret" and "admins"+"ecret"
		// would hash identically and a credential rotation of exactly that shape would
		// go undetected.
		shifted := &config.Configuration{Kafka: base.Kafka}
		shifted.Kafka.SASLAdminUser = "admins"
		shifted.Kafka.SASLAdminSecret = "ecret"

		assert.NotEqual(t, baseline, eventPublisherFingerprint(shifted),
			"fields must be separated in the digest, or characters shifted across a boundary collide")
	})

	t.Run("never carries a secret in the clear", func(t *testing.T) {
		// Both pairs, because both are now digested and either would be a credential in a
		// diagnostic if the digest were ever replaced by concatenation.
		for _, forbidden := range []string{"secret", "producer-secret", "admin", "producer"} {
			assert.NotContains(t, baseline, forbidden,
				"the fingerprint is held in memory and may be printed in a diagnostic; it must be "+
					"a non-reversible digest, never %q", forbidden)
		}
		assert.NotContains(t, baseline, "kafka.internal.example.com",
			"nor may it carry the deployment's topology in the clear")
	})

	t.Run("a nil configuration has its own fingerprint", func(t *testing.T) {
		assert.NotEqual(t, baseline, eventPublisherFingerprint(nil))
		assert.Equal(t, eventPublisherFingerprint(nil), eventPublisherFingerprint(nil))
	})
}

// TestSetSharedEventPublisher_InjectedPublisherIsNotOwned pins the ownership rule, which is
// the same one EventDeadLetterService follows: close only what you built.
//
// An injected publisher belongs to its injector — the relay's publisher outlives any service
// that borrows it, and closing it here would take the relay's transport down with it while
// every publish afterwards failed on a closed writer. It must also not be replaced by a
// configuration change, for the same reason.
func TestSetSharedEventPublisher_InjectedPublisherIsNotOwned(t *testing.T) {
	sharedPublisherReset(t)
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	injected := NewNoopEventPublisher()
	SetSharedEventPublisher(injected)

	resolved, err := SharedEventPublisher()
	require.NoError(t, err)
	assert.Same(t, injected, resolved, "an injected publisher must be handed back as-is")

	// A configuration change must NOT replace it: this package has no standing to
	// substitute a publisher it does not own.
	storeKafkaTopicPrefix(t, "acme")
	afterChange, err := SharedEventPublisher()
	require.NoError(t, err)
	assert.Same(t, injected, afterChange,
		"an injected publisher belongs to its injector and must survive a configuration change")

	// And closing the slot must not close it.
	require.NoError(t, CloseSharedEventPublisher())
	assert.NoError(t, injected.Publish(context.Background(), model.LedgerEvent{}),
		"an injected publisher must still be usable after the shared slot is released; closing it would tear down a transport its owner is still using")
}

// TestCloseSharedEventPublisher_IsIdempotentAndSafeWhenNothingWasBuilt covers shutdown being
// called more than once, or before anything was ever resolved — both of which a real process
// does, since shutdown paths run on failure as well as success.
func TestCloseSharedEventPublisher_IsIdempotentAndSafeWhenNothingWasBuilt(t *testing.T) {
	sharedPublisherReset(t)

	assert.NoError(t, CloseSharedEventPublisher(), "closing when nothing was built must be a no-op")
	assert.NoError(t, CloseSharedEventPublisher(), "and must stay one")

	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	_, err := SharedEventPublisher()
	require.NoError(t, err)

	assert.NoError(t, CloseSharedEventPublisher())
	assert.NoError(t, CloseSharedEventPublisher(), "a second close after a real one must not fail")

	// The slot is clear, so the next resolution builds afresh rather than handing back a
	// closed publisher.
	rebuilt, err := SharedEventPublisher()
	require.NoError(t, err)
	assert.NotNil(t, rebuilt)
	assert.NoError(t, rebuilt.Publish(context.Background(), model.LedgerEvent{}),
		"a publisher resolved after a close must be usable, not the closed one")
}

// newTestKafkaPublisher builds a real Kafka publisher without contacting anything.
//
// Construction performs no I/O by design — kafka.TCP only canonicalises an address string
// and a writer connects lazily on its first write — so a publisher with a plausible broker
// list is safe to build in a unit test as long as nothing writes through it. Nothing here
// does: every test below exercises writer RESOLUTION, which is pure map and lock work.
func newTestKafkaPublisher(t *testing.T) *kafkaPublisher {
	t.Helper()

	publisher, err := newKafkaPublisher([]string{"broker-1:9092"}, config.KafkaConfig{
		TopicPrefix: DefaultTopicPrefix,
		// The shared transport REFUSES a plaintext broker unless this is set, because SASL
		// over plaintext puts the credential on the wire in the clear. These tests exercise
		// writer resolution and never dial, so the acknowledgement is the honest way to get
		// a constructed publisher rather than a reason to weaken the refusal.
		InsecureLocalDev: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeErr := publisher.Close(); closeErr != nil {
			t.Logf("failed to close the test publisher: %v", closeErr)
		}
	})

	return publisher
}

// TestWriterFor_ResolvesTheConstructedInventoryWithoutGrowing is the baseline the bound is
// measured against: the topics the publisher was built with must already be present,
// so ordinary publishing never grows the cache at all.
func TestWriterFor_ResolvesTheConstructedInventoryWithoutGrowing(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	inventory := AllTopicsWithDeadLetters()
	// Derived from the category vocabulary rather than hardcoded: the inventory is one
	// topic plus one dead-letter sibling per category, and a hardcoded count would fail
	// the moment a category is added — which has already happened once, when the
	// internal system category is the catch-all for blank and uncatalogued event types.
	require.Len(t, inventory, len(model.AllEventCategories())*2)

	for _, topic := range inventory {
		writer, err := publisher.writerFor(topic)
		require.NoError(t, err, "topic %q is part of the constructed inventory", topic)
		require.NotNil(t, writer)
		assert.Equal(t, topic, writer.Topic)
	}

	publisher.mu.RLock()
	lazy := len(publisher.lazyTopics)
	publisher.mu.RUnlock()

	assert.Zero(t, lazy,
		"resolving the constructed inventory must not count as lazy growth; if it did, ordinary publishing would churn the retirement bookkeeping")
}

// TestWriterFor_RefusesATopicThatIsNotAnOwnedForm is the resource-growth half of the fix.
//
// Lazy growth exists for one reason — a topic-prefix change, and stored rows from before one —
// so every name it legitimately has to accept has the owned shape. Caching a writer for a name
// that does not would hold connections open for a destination that cannot be right, so the
// assertion that matters is the second in each case: the writer must NOT have been cached.
//
// TestWriterFor_KeepsServingTheConstructedInventoryAfterAPrefixChange is the
// event-is-never-stranded property. An outbox row records its destination topic at INSERT time,
// so a row written before KAFKA_TOPIC_PREFIX changed still names the previous generation's
// topic and must stay publishable; the PRE-CREATED inventory serves it from the map's fast path,
// and lazy growth covers the new prefix's topics. A generation that predates this process is
// deliberately REFUSED, because admitting any '<something>.<category>' name would also admit
// another system's topic at the point where a topic name becomes an outbound connection.
func TestWriterFor_KeepsServingTheConstructedInventoryAfterAPrefixChange(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	constructed := AllTopicsWithDeadLetters()
	require.NotEmpty(t, constructed)

	storeKafkaTopicPrefix(t, "renamed")

	for _, topic := range constructed {
		writer, err := publisher.writerFor(topic)
		require.NoError(t, err,
			"topic %q was pre-created and must still publish, or a committed event is stranded", topic)
		require.NotNil(t, writer)
		assert.Equal(t, topic, writer.Topic)
	}

	publisher.mu.RLock()
	lazyAfterHistorical := len(publisher.lazyTopics)
	publisher.mu.RUnlock()
	assert.Zero(t, lazyAfterHistorical,
		"serving a pre-created topic is the fast path and must not count as lazy growth")

	grown, err := publisher.writerFor("renamed.transactions")
	require.NoError(t, err, "the new prefix is Blnk's namespace and must be admitted")
	require.NotNil(t, grown)

	publisher.mu.RLock()
	lazyAfterGrowth := len(publisher.lazyTopics)
	publisher.mu.RUnlock()
	assert.Equal(t, 1, lazyAfterGrowth, "a lazily created writer is tracked for retirement")

	for _, refused := range []string{"legacy.transactions", "attacker.transactions"} {
		_, err = publisher.writerFor(refused)
		assert.ErrorIs(t, err, ErrTopicNotOwned,
			"%q is outside the configured namespace and is not admitted on form alone", refused)
	}
}

// newTestKafkaPublisherForPrefixes builds a publisher as a RESTARTED PROCESS would build it:
// from a configuration that is already the post-rename one.
//
// The distinction from newTestKafkaPublisher matters and is the whole point of the tests
// below. That helper always constructs under DefaultTopicPrefix, which models a process that
// was running BEFORE a rename and therefore pre-created the old generation's writers. Nothing
// it can assert says anything about the process that starts AFTER the rename, whose inventory
// is built from the new configuration alone — and that was precisely the gap.
//
// The configuration is published before construction, because the constructor composes its
// inventory from the live store.
func newTestKafkaPublisherForPrefixes(t *testing.T, current string, historical ...string) *kafkaPublisher {
	t.Helper()

	kafkaConfig := config.KafkaConfig{
		TopicPrefix:             current,
		HistoricalTopicPrefixes: historical,
		InsecureLocalDev:        true,
	}
	storeKafkaConfig(t, kafkaConfig)

	publisher, err := newKafkaPublisher([]string{"broker-1:9092"}, kafkaConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closeErr := publisher.Close(); closeErr != nil {
			t.Logf("failed to close the test publisher: %v", closeErr)
		}
	})

	return publisher
}

// TestWriterFor_ARestartAfterAPrefixChangeStrandsRowsUntilThePrefixIsDeclared is the
// RESTART half of the event-is-never-stranded property, and it is the case the pre-existing
// prefix-change test could not reach.
//
// # What was wrong
//
// An outbox row records its fully-resolved destination topic at insert time, so a deployment
// that changes KAFKA_TOPIC_PREFIX keeps committed rows naming the previous generation's
// topics. A RUNNING process published them fine — it had pre-created a writer for every one
// before the change. A RESTARTED process pre-created the new generation only, and the
// ownership test was pinned to the configured prefix alone, so every stored old-prefix row
// was refused at writer resolution. The rows were not lost, and each attempt logged a
// refusal, but nothing drained them and their dead-letter writes and replays were refused
// with them. A rename plus a rolling restart is ordinary maintenance, and it silently stopped
// delivery of everything captured before it.
//
// # What this asserts, in the order an operator meets it
//
// First the refusal, from a publisher built exactly as the restarted process builds one, so
// the failure this fix exists for is pinned rather than described. Then the remedy: with the
// previous prefix declared in KAFKA_HISTORICAL_TOPIC_PREFIXES, every one of those topics —
// category AND dead-letter, since a failing old row must still be able to dead-letter — is
// served, and served from the PRE-CREATED inventory rather than by lazy growth, which is what
// keeps the drain off the write lock and out of the retirement bound.
func TestWriterFor_ARestartAfterAPrefixChangeStrandsRowsUntilThePrefixIsDeclared(t *testing.T) {
	stored := AllTopicsWithDeadLettersForPrefix(DefaultTopicPrefix)
	require.NotEmpty(t, stored)

	t.Run("undeclared: every stored topic of the previous generation is refused", func(t *testing.T) {
		publisher := newTestKafkaPublisherForPrefixes(t, "renamed")

		for _, topic := range stored {
			_, err := publisher.writerFor(topic)
			assert.ErrorIsf(t, err, ErrTopicNotOwned,
				"%q must be refused while no historical prefix is declared: admitting it on form "+
					"alone would also admit another system's topic at the one point a stored name "+
					"becomes an outbound connection carrying Blnk's producer credentials", topic)
		}
	})

	t.Run("declared: every stored topic is served from the pre-created inventory", func(t *testing.T) {
		publisher := newTestKafkaPublisherForPrefixes(t, "renamed", DefaultTopicPrefix)

		for _, topic := range stored {
			writer, err := publisher.writerFor(topic)
			require.NoErrorf(t, err,
				"%q names a declared historical namespace and must publish, or every event "+
					"captured before the rename is stranded", topic)
			require.NotNil(t, writer)
			assert.Equal(t, topic, writer.Topic)
		}

		// The new generation is present too: declaring a historical prefix must not displace
		// the configured one, which is where every NEW event goes.
		for _, topic := range AllTopicsWithDeadLettersForPrefix("renamed") {
			writer, err := publisher.writerFor(topic)
			require.NoErrorf(t, err, "%q is the live generation and must publish", topic)
			require.NotNil(t, writer)
		}

		publisher.mu.RLock()
		lazy := len(publisher.lazyTopics)
		publisher.mu.RUnlock()
		assert.Zero(t, lazy,
			"a declared prefix's topics are PRE-CREATED, so draining the previous generation "+
				"must not take the write lock per topic or consume the lazy-growth bound that "+
				"exists for prefixes declared after this process started")

		// And the boundary still holds: declaring one namespace does not admit any other.
		for _, refused := range []string{"attacker.transactions", "legacy.transactions"} {
			_, err := publisher.writerFor(refused)
			assert.ErrorIsf(t, err, ErrTopicNotOwned,
				"%q was never declared and must stay refused", refused)
		}
	})
}

// TestWriterFor_ADeclaredHistoricalPrefixIsExactRatherThanAPattern is the boundary of the
// remedy above.
//
// The allowlist is what makes a stored old-prefix topic publishable, so its width is a
// security property: an entry must admit that one namespace and nothing that merely resembles
// it. A prefix comparison rather than an exact one would turn "blnk" into a licence for
// "blnkfinance.transactions", and a name that shares a category token is not the same topic.
func TestWriterFor_ADeclaredHistoricalPrefixIsExactRatherThanAPattern(t *testing.T) {
	publisher := newTestKafkaPublisherForPrefixes(t, "renamed", "blnk")

	admitted, err := publisher.writerFor("blnk.transactions")
	require.NoError(t, err, "the exact declared namespace must be admitted")
	require.NotNil(t, admitted)

	for _, refused := range []string{
		// A longer name that starts with the declared prefix.
		"blnkfinance.transactions",
		// A name the declared prefix starts with.
		"bln.transactions",
		// The declared prefix as a SEGMENT rather than the whole namespace.
		"acme.blnk.transactions",
		// The declared namespace with an unknown category.
		"blnk.ledger",
		// A dead-letter topic's dead-letter topic.
		"blnk.transactions.dlt.dlt",
	} {
		_, err := publisher.writerFor(refused)
		assert.ErrorIsf(t, err, ErrTopicNotOwned,
			"%q is not the declared namespace's category or dead-letter topic and must be refused",
			refused)
	}
}

// TestWriterFor_BoundsAndRetiresLazilyCreatedWriters is the bound itself.
//
// Without it the cache only ever grows: every distinct historical topic name keeps a writer,
// and its connections, for the life of the process. The bound is two prefix generations'
// worth, and past that the OLDEST lazily-created writer is retired.
//
// Retirement must never touch the constructed inventory — those eight are the deployment's
// current topics and its steady-state hot path — and must never be a refusal: a retired
// topic asked for again simply gets a new writer, which is what makes bounding the cache a
// cost decision rather than a correctness one. All three properties are asserted.
func TestWriterFor_BoundsAndRetiresLazilyCreatedWriters(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	constructedTopics := AllTopicsWithDeadLetters()
	constructed := len(constructedTopics)

	// Well past the bound, each a distinct prefix generation. Growth is driven by REAL
	// prefix changes, because that is the only way a topic legitimately reaches the lazy
	// path: the ownership check admits the configured namespace and nothing else, so
	// inventing '<foreign>.transactions' names would exercise a path production cannot take.
	lazyTopics := make([]string, 0, maxLazyTopicWriters+6)
	for i := 0; i < maxLazyTopicWriters+6; i++ {
		generation := fmt.Sprintf("gen%d", i)
		storeKafkaTopicPrefix(t, generation)

		topic := generation + ".transactions"
		lazyTopics = append(lazyTopics, topic)

		writer, err := publisher.writerFor(topic)
		require.NoError(t, err)
		require.NotNil(t, writer)
	}

	// Restored before the assertions, so the inventory named below is the one the publisher
	// was actually constructed with rather than the last generation's.
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher.mu.RLock()
	lazyCount := len(publisher.lazyTopics)
	totalWriters := len(publisher.writers)
	tracked := make([]string, len(publisher.lazyTopics))
	copy(tracked, publisher.lazyTopics)
	cachedInventory := 0
	for _, topic := range constructedTopics {
		if _, ok := publisher.writers[topic]; ok {
			cachedInventory++
		}
	}
	publisher.mu.RUnlock()

	assert.LessOrEqual(t, lazyCount, maxLazyTopicWriters,
		"the lazy writer cache must stay within its bound; unbounded growth is the finding")
	assert.Equal(t, constructed+lazyCount, totalWriters,
		"the writer map and the retirement bookkeeping must agree, or retirement would evict names for writers that no longer exist")

	assert.Equal(t, constructed, cachedInventory,
		"retirement must never evict a constructed-inventory writer; those are the deployment's current topics and its hot path")

	// The oldest generations went, the newest stayed.
	assert.NotContains(t, tracked, lazyTopics[0],
		"the OLDEST lazily-created writer must be the one retired")
	assert.Contains(t, tracked, lazyTopics[len(lazyTopics)-1],
		"the most recently created writer must be retained")

	// And a retired topic is still publishable: retirement is an eviction, not a refusal.
	// Re-resolving a retired topic needs its prefix configured again, exactly as the first
	// resolution did: retirement is a cache eviction, never a change of ownership.
	storeKafkaTopicPrefix(t, "gen0")
	revived, err := publisher.writerFor(lazyTopics[0])
	require.NoError(t, err,
		"a retired topic must be re-creatable; if retirement refused it, bounding the cache would have stranded events")
	assert.Equal(t, lazyTopics[0], revived.Topic)
}

// TestWriterFor_IsSafeUnderConcurrentGrowth exercises the locking on the growth path.
//
// The growth path takes the write lock, may evict an entry, releases the lock and only then
// closes the evicted writer — because a flush under the lock would stall unrelated
// publishes. That restructuring is exactly where a lock is easy to release twice or not at
// all, and neither mistake is a compile error: a double unlock panics and a missing unlock
// deadlocks, both only under concurrency.
//
// Racing many goroutines over a mix of known and unknown topics is what makes either
// failure show up here rather than in production. Run with -race this also covers unguarded
// access to the map and the bookkeeping slice.
func TestWriterFor_IsSafeUnderConcurrentGrowth(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	constructedTopics := AllTopicsWithDeadLetters()

	// The four historical topics are WARMED through real prefix changes, for the same reason
	// as the bound test: a prefix outside the configured namespace is refused outright, so a
	// warmed previous-generation writer is what concurrent resolution actually contends over.
	historical := []string{"gen1.transactions", "gen2.balances", "gen3.identities", "gen4.system.dlt"}
	for i, topic := range historical {
		storeKafkaTopicPrefix(t, fmt.Sprintf("gen%d", i+1))
		_, err := publisher.writerFor(topic)
		require.NoError(t, err)
	}
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	const goroutines = 24
	topics := append(append([]string{}, constructedTopics...), historical...)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()

			for j := range topics {
				topic := topics[(offset+j)%len(topics)]
				if _, err := publisher.writerFor(topic); err != nil {
					t.Errorf("writerFor(%q) failed: %v", topic, err)

					return
				}
			}
			// A refused topic on the same goroutines, so the error exit's unlock is
			// exercised concurrently too.
			if _, err := publisher.writerFor("__consumer_offsets"); err == nil {
				t.Error("a non-owned topic must be refused")
			}
		}(i)
	}
	wg.Wait()

	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	assert.LessOrEqual(t, len(publisher.lazyTopics), maxLazyTopicWriters)
	assert.Equal(t, len(constructedTopics)+len(publisher.lazyTopics), len(publisher.writers),
		"concurrent growth must leave the map and the bookkeeping consistent")
}

// TestPublishSuccessLog_IsGuardedByTheDebugLevel is the hot-path allocation fix.
//
// logrus evaluates a WithFields argument before it decides whether the level is enabled, so
// an unguarded debug call on the success path builds a seven-entry map AND an Entry for
// every event published, then discards both — at Debug off, which is the normal production
// setting. At the 500 events/second the throughput target names that is roughly 43 million
// pointless allocations a day, on the very path whose p99 latency is an acceptance
// criterion.
//
// # What is asserted, and with which instrument
//
// The OBSERVABLE half is asserted by running real publishes through the fake transport and
// reading what reached the logger, which is what makes it robust: it holds whatever the guard
// is spelled as, and it fails if the guard is right but the log it protects has been lost.
//
//	Debug ON,  publish succeeds -> exactly one Debug entry, carrying every field.
//	Debug OFF, publish fails    -> exactly one Error entry, carrying attempt and reason.
//
// That second case is the important one. Requirement R-4 mandates the attempt number and
// error reason on EVERY attempt, so the failure log must not be behind a level guard of any
// kind — and driving a failure with the level pinned above Debug proves it directly, for any
// guard, rather than for one hard-coded spelling of one.
//
// The STRUCTURAL half — that the success call is enclosed by an IsLevelEnabled check — is the
// part behaviour genuinely cannot reach. logrus filters by level inside Entry.Debug, so an
// unguarded call and a guarded one are indistinguishable from the outside; only the
// allocation the guard avoids differs, and that is not something to assert by counting
// allocations through a whole publish path. So it is read from the AST: an if-statement whose
// condition calls logrus.IsLevelEnabled, CONTAINING the Debug call. That is containment
// asserted as containment, replacing an earlier byte-distance comparison that changed its
// verdict whenever a comment was added between the two lines.
func TestPublishSuccessLog_IsGuardedByTheDebugLevel(t *testing.T) {
	t.Run("with debug on, a successful publish logs the whole result", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()
		relayPinLogLevel(t, logrus.DebugLevel)

		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())
		event := publisherEvent(model.EventTypeTransactionApplied, "txn_log_guard_ok", publisherPayload)

		_, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event: event,
			Topic: TopicForEvent(event.EventType),
			Key:   publisherLedgerID,
		})
		require.NoError(t, err)

		entries := relayEntriesWithMessage(hook, "ledger event published to kafka")
		require.Len(t, entries, 1,
			"the guard must let the line through when Debug IS enabled: the fix was to guard the "+
				"call, not to remove the observability, and a guard on the wrong level would "+
				"silence it permanently")
		assert.Equal(t, logrus.DebugLevel, entries[0].Level)
		assert.Len(t, entries[0].Data, len(PublishResult{}.LogFields()),
			"the guarded call must carry the full field set, which is the allocation it defers")
	})

	t.Run("with debug off, a failed publish still logs every attempt", func(t *testing.T) {
		// The level is pinned ABOVE Debug on purpose. If the failure log were guarded — by the
		// Debug level, or by any other level above Error — this is where it disappears.
		hook := logtest.NewGlobal()
		defer hook.Reset()
		relayPinLogLevel(t, logrus.ErrorLevel)

		transport := newPublisherFakeTransport().failProduceWith(kafka.NotLeaderForPartition)
		publisher := publisherWithFakeTransport(t, transport)
		event := publisherEvent(model.EventTypeTransactionApplied, "txn_log_guard_fail", publisherPayload)

		_, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:   event,
			Topic:   TopicForEvent(event.EventType),
			Key:     publisherLedgerID,
			Attempt: 3,
		})
		require.Error(t, err, "the fake broker refused the produce request")

		entries := relayEntriesWithMessage(hook, "publishing a ledger event to kafka failed")
		require.Len(t, entries, 1,
			"requirement R-4 mandates the attempt number and error reason on EVERY attempt, so "+
				"the failure log must never sit behind a level guard")
		assert.Equal(t, logrus.ErrorLevel, entries[0].Level)
		assert.Equal(t, 3, entries[0].Data["attempt"],
			"the attempt number is half of what R-4 requires on every attempt")
		assert.NotEmpty(t, entries[0].Data["error"], "and the error reason is the other half")

		// Silence at Debug is asserted too, so the case above cannot be satisfied by a logger
		// that simply emits everything at every level.
		assert.Empty(t, relayEntriesWithMessage(hook, "ledger event published to kafka"),
			"nothing succeeded, and nothing may be logged as though it had")
	})

	t.Run("the success call is structurally enclosed by the level guard", func(t *testing.T) {
		file := parseRepositoryGoFile(t, "event_publisher.go")

		// The call exists at all. Without this the containment assertion below is satisfied by
		// a file that stopped logging the success case entirely.
		require.Positive(t, selectorCallNames(file)["Debug"],
			"the success debug log must still exist; the fix is to guard it, not to delete it")

		guarded := callsGuardedBy(file, "logrus", "IsLevelEnabled")
		assert.Positive(t, guarded["Debug"],
			"a Debug call must sit INSIDE an if whose condition calls logrus.IsLevelEnabled. "+
				"logrus evaluates the WithFields argument before it checks the level, so an "+
				"unguarded call builds the whole field map per published event and discards it — "+
				"about 43 million wasted allocations a day at the 500 events/second the "+
				"throughput target names, on the path whose p99 latency is an acceptance criterion")

		assert.Zero(t, guarded["Error"],
			"and no Error call may be enclosed by a level guard: R-4 mandates the attempt number "+
				"and error reason on every attempt, so a guarded failure log would violate the "+
				"requirement silently whenever the level was raised")
	})
}

// TestPublishResultLogFields_CarriesTheFieldsTheGuardDefers confirms the guard defers real
// work rather than nothing.
//
// If LogFields were cheap the guard would be pointless, so the cost being avoided is worth
// stating: a fresh map with an entry per field, built per event.
func TestPublishResultLogFields_CarriesTheFieldsTheGuardDefers(t *testing.T) {
	result := PublishResult{
		EventID:      "evt_fields",
		EventType:    "transaction.applied",
		Topic:        "blnk.transactions",
		PartitionKey: "ldg_1",
		Attempt:      1,
		Status:       model.PublishStatusDispatched,
		Duration:     3 * time.Millisecond,
	}

	fields := result.LogFields()
	assert.Len(t, fields, 7,
		"the guard defers building a seven-entry map per published event, which is the allocation the fix removes")

	// INDEPENDENCE IS PROVEN BY MUTATION, not by comparing addresses.
	//
	// This assertion used to be assert.NotSame(&fields, &first), which compares the addresses
	// of two LOCAL VARIABLES. Two distinct locals never share an address, so it held for every
	// possible implementation — including one that returned a single package-level map to every
	// caller, which is the implementation it was written to rule out. That implementation would
	// make the level guard pointless (there would be no per-event allocation to defer) and,
	// worse, would let one log line's fields be mutated by the next event.
	//
	// Writing into one map and reading the other is the observation that actually distinguishes
	// them: a shared map shows the write, a fresh one cannot.
	second := result.LogFields()
	second["injected_by_the_test"] = true

	assert.NotContains(t, fields, "injected_by_the_test",
		"each call must allocate a FRESH map: writing into one returned map must not be visible "+
			"through another, or the deferred allocation the level guard exists to avoid does not "+
			"exist and concurrent log lines share mutable state")
	assert.Len(t, fields, 7, "and the first map must be unchanged by the write to the second")
	assert.Len(t, second, 8, "while the map that was written to carries the extra entry")
}

// TestMaxLazyTopicWriters_IsTwoPrefixGenerations checks the arithmetic behind the writer
// cache bound instead of trusting the literal.
//
// The bound exists to cover a topic-prefix change with stored rows still arriving for the
// previous name, plus one further change on top of it. That makes its intended value two
// prefix generations' worth of writers, and a generation is one topic and one dead-letter
// sibling per category.
//
// A const cannot call a function, so the literal cannot derive itself — which makes it
// exactly the kind of value that goes stale silently, and it has gone stale in both
// directions. Left too LOW, lazy writers are retired while a previous generation is still in
// use, costing a reconnection per message rather than per topic; left too HIGH, the bound no
// longer describes what it claims to. Neither fails at build or run time, which is why the
// arithmetic is asserted here instead.
func TestMaxLazyTopicWriters_IsTwoPrefixGenerations(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	generation := len(AllTopicsWithDeadLetters())
	require.Equal(t, len(model.AllEventCategories())*2, generation,
		"a generation is one topic and one dead-letter sibling per category")

	assert.Equal(t, generation*2, maxLazyTopicWriters,
		"the bound must be TWO generations' worth: one for the previous prefix whose rows are "+
			"still arriving, and one for a further change on top of it")
}

// ---------------------------------------------------------------------------
// The outbox-to-message key contract
// ---------------------------------------------------------------------------

// TestPublishRequestFromOutbox_KeysByTheStoredPartitionKey covers the events that have NO
// LEDGER, where the stored partition key is what carries per-aggregate ordering.
//
// # What the two columns are for
//
// Requirement R-6 partitions by ledger ID, and PublishRequestFromOutbox keys on ledger_id
// first for exactly that reason. The two columns AGREE wherever a ledger exists, because
// WithEventLedgerID writes the supplied ledger into both and every payload that yields a
// ledger of its own does the same — so the value ClaimPendingEventOutbox serialises dispatch
// on is the value Kafka partitions on, and the database's ordering guarantee reaches the
// consumer intact.
//
// The fixtures here are the shapes whose payloads yield no ledger: transactions built without
// a supplied ledger, bulk batches, balance monitors, identities and system.error. Their
// ledger_id is legitimately empty, and the stored partition key is the rung that keeps each
// aggregate's events on one partition. That is what this test pins.
//
// # Why this test is shaped the way it is
//
// It asserts on rows built by the REAL producer path, PrepareEventOutbox, across the whole
// event catalogue, because the risk lives exactly in the seam between the producer's two
// columns and the publisher's single key. A test that hand-built a row could set the two
// columns to the same value and pass either way.
//
// The shapes where partition_key differs from BOTH ledger_id and aggregate_id are counted
// and required to be non-empty, so this test cannot quietly degrade into a tautology if
// the fixtures are ever simplified.
func TestPublishRequestFromOutbox_KeysByTheStoredPartitionKey(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	discriminating := 0

	for _, fixture := range outboxEventFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			row := mustPrepareEventOutbox(t, blnk, NewWebhook{
				Event:   fixture.eventType,
				Payload: fixture.payload,
			})

			require.Equal(t, fixture.partitionKey, row.PartitionKey,
				"the fixture's expected partition key must be what the producer stored")
			require.NotEmpty(t, row.PartitionKey,
				"a blank key would be scattered across partitions with nothing in the data to show it")

			request := PublishRequestFromOutbox(*row, 1)

			// The ledger takes precedence when the row records one, which for these fixtures
			// happens only where the payload itself yields a ledger — and there the two
			// columns hold the same value, so one expectation covers both cases.
			expected := row.LedgerID
			if expected == "" {
				expected = row.PartitionKey
			}

			assert.Equal(t, expected, request.Key,
				"the request must be keyed by the ledger, falling back to the column "+
					"ClaimPendingEventOutbox serialises on")
			assert.Equal(t, expected, resolvePartitionKey(request),
				"and resolution must not fall through to the aggregate when a key is present")
			assert.Equal(t, []byte(expected), partitionKeyBytes(resolvePartitionKey(request)),
				"the bytes handed to kafka.Message.Key must be the resolved key verbatim")
		})

		if fixture.partitionKey != fixture.aggregateID {
			discriminating++
		}
	}

	assert.GreaterOrEqual(t, discriminating, 3,
		"at least the transaction, bulk and monitor shapes must key by something OTHER than their "+
			"aggregate, or this test would pass even if the stored key were ignored entirely")
}

// TestPublishRequestFromOutbox_AppliesTheDocumentedFallbackChain pins the three rungs, in
// the order requirement R-6 dictates: ledger_id, then partition_key, then aggregate_id.
//
// THE LEDGER COMES FIRST, and that is the requirement rather than a preference — "partitioned
// by ledger ID". The two columns normally agree, because every production capture path
// supplies the ledger through WithEventLedgerID and that option writes both; the deliberately
// DISAGREEING row below is what proves the precedence rather than assuming it, and it is the
// shape a caller that set only the ledger would produce.
//
// partition_key is NOT NULL with a not-blank CHECK, so a row read back from PostgreSQL always
// supplies the second rung for an event that genuinely has no ledger. The third exists for a
// row assembled in Go with neither, and is asserted because an unkeyed message is the one
// outcome that silently discards the ordering guarantee.
func TestPublishRequestFromOutbox_AppliesTheDocumentedFallbackChain(t *testing.T) {
	base := model.EventOutbox{
		EventID:       "evt_fallback_chain",
		EventType:     "transaction.applied",
		AggregateID:   "txn_fallback",
		PartitionKey:  "bln_fallback_source",
		LedgerID:      "ldg_fallback",
		Topic:         "blnk.transactions",
		SchemaVersion: model.SchemaVersionV1,
	}

	t.Run("the recorded ledger wins", func(t *testing.T) {
		assert.Equal(t, "ldg_fallback", resolvePartitionKey(PublishRequestFromOutbox(base, 1)),
			"requirement R-6 partitions by ledger id, so a row that records one must be keyed on it "+
				"even when its partition_key column holds a different value")
	})

	t.Run("a blank ledger falls back to the stored partition key", func(t *testing.T) {
		row := base
		row.LedgerID = "   "

		assert.Equal(t, "bln_fallback_source", resolvePartitionKey(PublishRequestFromOutbox(row, 1)),
			"whitespace must read as absence rather than becoming a key made of spaces")
	})

	t.Run("neither leaves the aggregate", func(t *testing.T) {
		row := base
		row.PartitionKey = ""
		row.LedgerID = ""

		assert.Equal(t, "txn_fallback", resolvePartitionKey(PublishRequestFromOutbox(row, 1)),
			"the last rung still pins one aggregate's events to one partition")
	})

	t.Run("an event belonging to nothing is written unkeyed", func(t *testing.T) {
		row := base
		row.PartitionKey = ""
		row.LedgerID = ""
		row.AggregateID = ""

		key := resolvePartitionKey(PublishRequestFromOutbox(row, 1))
		assert.Empty(t, key)
		assert.Nil(t, partitionKeyBytes(key),
			"an absent key must be nil so the balancer spreads the message rather than pinning it")
	})
}

// TestPublishRequestFromOutbox_PinsThePublishedKeyContractPerEventFamily is the machine-checked
// form of the ordering-key table in docs/event-streaming.md.
//
// # Why the table needs a test rather than a careful author
//
// The documented contract had drifted from the code and the drift was invisible: the guide told
// subscribers the key for a transaction was its SOURCE BALANCE and specifically warned them not
// to assume it was the ledger, while PublishRequestFromOutbox resolves ledger_id first and every
// ledger-scoped producer supplies it. Both statements were locally defensible — the balance
// answer describes the outbox row's stored partition key, which is real — and nothing failed,
// because no test connected the published claim to the resolved key.
//
// A consumer designing around the wrong answer does not get an error either. It gets a
// partitioning model that disagrees with the broker's, which surfaces later as "ordering is
// broken" with nothing in the data to explain it.
//
// So each row below is one row of the published table, asserting the KEY THE MESSAGE IS WRITTEN
// WITH — not the column it came from. A future producer that stops supplying its ledger, or a
// resolution order that stops preferring it, fails here and the guide is corrected with it.
func TestPublishRequestFromOutbox_PinsThePublishedKeyContractPerEventFamily(t *testing.T) {
	const (
		ledger      = "ldg_key_contract"
		balance     = "bln_key_contract_source"
		transaction = "txn_key_contract"
		identity    = "idt_key_contract"
		batch       = "bulk_key_contract"
	)

	for name, testCase := range map[string]struct {
		row  model.EventOutbox
		want string
		why  string
	}{
		// --- LEDGER-SCOPED: the key is the ledger, which is the whole of requirement R-6 ---
		"a transaction captured with its ledger": {
			row: model.EventOutbox{
				EventType: "transaction.applied", AggregateID: transaction,
				LedgerID: ledger, PartitionKey: ledger, Topic: "blnk.transactions",
			},
			want: ledger,
			why:  "transaction execution resolves the ledger from the balances it has already loaded and states it at capture time",
		},
		"a balance event": {
			row: model.EventOutbox{
				EventType: "balance.created", AggregateID: balance,
				LedgerID: ledger, PartitionKey: ledger, Topic: "blnk.balances",
			},
			want: ledger,
			why:  "a balance belongs to exactly one ledger, so every balance event in that ledger is co-located",
		},
		"a balance monitor alert": {
			row: model.EventOutbox{
				EventType: "balance.monitor", AggregateID: "mon_key_contract",
				LedgerID: ledger, PartitionKey: balance, Topic: "blnk.balances",
			},
			want: ledger,
			why:  "the monitored balance's ledger is supplied explicitly, and the ledger rung wins over the stored key",
		},
		"a ledger creation": {
			row: model.EventOutbox{
				EventType: "ledger.created", AggregateID: ledger,
				LedgerID: ledger, PartitionKey: ledger, Topic: "blnk.system",
			},
			want: ledger,
			why:  "the event announces the ledger it is keyed on",
		},

		// --- NO LEDGER: the approved fallback, one row per documented family ---
		"a rejected transaction, which never loaded its balances": {
			row: model.EventOutbox{
				EventType: "transaction.rejected", AggregateID: transaction,
				PartitionKey: balance, Topic: "blnk.transactions",
			},
			want: balance,
			why: "the rejection path knows no ledger — frequently that is why the transaction was " +
				"rejected — so the stored source-balance key carries per-aggregate ordering instead",
		},
		"an identity, which is not ledger-scoped": {
			row: model.EventOutbox{
				EventType: "identity.created", AggregateID: identity,
				PartitionKey: identity, Topic: "blnk.identities",
			},
			want: identity,
			why:  "one identity can be referenced by balances in several ledgers, so it has no single ledger",
		},
		"a bulk batch summary, which may span several ledgers": {
			row: model.EventOutbox{
				EventType: "bulk_transaction.applied", AggregateID: batch,
				PartitionKey: batch, Topic: "blnk.transactions",
			},
			want: batch,
			why:  "the batch id is what makes one batch's progress events mutually ordered",
		},
		"a system error, which has no aggregate at all": {
			row: model.EventOutbox{
				EventType: "system.error", AggregateID: "system.error",
				PartitionKey: "system.error", Topic: "blnk.system",
			},
			want: "system.error",
			why:  "keying on the event type gives the error stream one partition and therefore a total order",
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := PublishRequestFromOutbox(testCase.row, 1)

			assert.Equal(t, testCase.want, resolvePartitionKey(request), testCase.why)
			assert.NotEmpty(t, partitionKeyBytes(resolvePartitionKey(request)),
				"no published family may be written unkeyed: an absent key is scattered round-robin "+
					"and destroys ordering with nothing in the data to show it")
		})
	}
}

// TestPublishRequestFromOutbox_OrdersTwoAggregatesSharingOneKeyOntoOneKey is the property
// requirement V-6 is decided by, expressed at the seam this file owns.
//
// Two transactions that move value between the same balances are DIFFERENT aggregates and
// therefore carry different aggregate ids, but they share a partition key. The outbox
// serialises them relative to each other; keying both by that shared value is what makes
// the broker place them on one partition and preserve the order the outbox established.
// Keying by the aggregate would place them independently and lose it.
func TestPublishRequestFromOutbox_OrdersTwoAggregatesSharingOneKeyOntoOneKey(t *testing.T) {
	blnk := newOutboxBlnk(t, outboxPublishingConfiguration(), nil)

	first := mustPrepareEventOutbox(t, blnk, NewWebhook{
		Event:   "transaction.queued",
		Payload: outboxSampleTransaction("QUEUED"),
	})

	second := outboxSampleTransaction("APPLIED")
	second.TransactionID = "txn_outbox_second"
	applied := mustPrepareEventOutbox(t, blnk, NewWebhook{
		Event:   "transaction.applied",
		Payload: second,
	})

	require.NotEqual(t, first.AggregateID, applied.AggregateID,
		"the two events must be about different transactions, or the property is untested")
	require.Equal(t, first.PartitionKey, applied.PartitionKey,
		"both transactions move value from the same source balance, so they share a partition key")

	firstKey := resolvePartitionKey(PublishRequestFromOutbox(*first, 1))
	appliedKey := resolvePartitionKey(PublishRequestFromOutbox(*applied, 1))

	assert.Equal(t, firstKey, appliedKey,
		"two events serialised against each other in the outbox must be keyed identically, or the "+
			"broker places them independently and the ordering the claim query paid for is lost")
	assert.Equal(t, outboxSourceBalanceID, firstKey,
		"and the shared key is the source balance, which is what a transaction payload yields on "+
			"its own; production supplies the ledger and keys on that")
}

// ---------------------------------------------------------------------------
// The mandated contract, the writer settings, and the wire form of one publish
// ---------------------------------------------------------------------------

// Everything below asserts the properties of ONE PUBLISH — the interface it is made through,
// the three writer settings that decide where a message lands and how durably, the key that
// decides its partition, the bytes that reach the topic, and what the publisher reports — with
// NO KAFKA ANYWHERE.
//
// kafka-go exposes the seam: kafka.Writer.Transport is an exported kafka.RoundTripper, and a
// writer falls back to the shared default transport only when it is nil, so a fake round
// tripper intercepts the only two requests a synchronous write issues — the metadata lookup
// and the produce request. That is what turns intentions into facts: the acknowledgement mode
// is asserted as the produce request's Acks, the balancer's decision as the partition it names,
// and the key and value as the record it carries. Reading the writer's struct fields instead
// would agree with the implementation by construction and would still pass if the writer never
// used the field.
//
// Retry and backoff belong to event_relay_test.go, dead-letter routing and replay to
// event_dlt_test.go, topic assurance and provisioning to event_admin_test.go, and row
// construction to event_outbox_test.go.

// publisherFakePartitions is the partition count the fake broker reports for every topic.
//
// Six is not arbitrary: it is the minimum partition count requirement R-6 states and the
// default KAFKA_MIN_PARTITIONS carries, so the modular arithmetic the balancer performs
// here is the arithmetic it performs against a provisioned topic. It is also more than
// one, which two of the assertions below depend on — with a single partition every key
// would "route correctly" no matter what the balancer or the key did.
const publisherFakePartitions = 6

// publisherConstructionBudget bounds how long building a publisher may take.
//
// It is deliberately far above the cost of the pure computation construction actually
// performs (microseconds) and far below the transport's five-second dial timeout, so it
// distinguishes the two outcomes it exists to distinguish rather than measuring machine
// speed: a construction that dialled or resolved a name would blow through it, and a
// construction that did neither cannot approach it.
const publisherConstructionBudget = 2 * time.Second

// publisherBlackholeBroker is an address in TEST-NET-2 (RFC 5737), reserved for
// documentation and routed nowhere. A dial to it hangs until it times out rather than
// failing fast, which is precisely what makes it a useful probe: if construction dialled,
// the construction budget above would be exceeded.
//
// RFC 1918 PRIVATE SPACE RATHER THAN RFC 5737 TEST-NET, and the change is not cosmetic.
// This was 198.51.100.1, which is a PUBLIC address — and plaintext to a broker outside
// Blnk's own network is now refused outright by requireLocalBrokersForPlaintext, because
// KAFKA_INSECURE_LOCAL_DEV asserts the broker is local and nothing used to check that.
// A test that reached a constructed publisher over an acknowledged-plaintext transport to
// a public address was therefore exercising a configuration the transport must reject.
//
// 10.255.255.1 keeps every property this constant was chosen for — it is routed nowhere
// in a default environment, so a dial hangs rather than failing fast — while being inside
// private space, which is what the locality check recognises. The probe is unchanged; only
// the address family is.
const publisherBlackholeBroker = "10.255.255.1:9092"

// publisherUnresolvableBroker is a single-label name that no search domain resolves. If
// construction resolved names, this address would either fail construction outright or
// make it wait on a resolver timeout.
//
// It was broker.publisher-test.invalid, using the RFC 2606 .invalid TLD. That guaranteed
// non-resolution but is a QUALIFIED name outside Blnk's network, which acknowledged
// plaintext no longer reaches — see publisherBlackholeBroker above. An unqualified
// single-label name is what a Compose service name and an in-cluster Kubernetes Service
// name both are, so the locality check treats it as internal, and one that deliberately
// names nothing still fails to resolve. Both properties the constant needs, retained.
const publisherUnresolvableBroker = "broker-publisher-test-does-not-exist:9092"

// publisherLedgerID and publisherOtherLedgerID are two ledger identifiers, which are the
// values the message key carries. Requirement R-6 partitions by ledger, and
// PrepareEventOutbox stores a known ledger in the partition-key column, so a ledger id is
// what a real outbox-backed publish keys on.
const (
	publisherLedgerID      = "ldg_publisher_wire_001"
	publisherOtherLedgerID = "ldg_publisher_wire_002"
)

// publisherPayload is an ordinary legacy webhook body: the marshalled NewWebhook object,
// both of its keys included, exactly as the dual-delivery guarantee requires the payload
// column to carry it.
const publisherPayload = `{"event":"transaction.applied","data":{"transaction_id":"txn_publisher_wire","status":"APPLIED","amount":100}}`

// publisherAdversarialPayload is the payload that separates SPLICING the stored bytes from
// re-marshalling a struct, and every element of it is chosen for that purpose:
//
//   - `<`, `>` and `&` are rewritten as \u003c, \u003e and \u0026 by encoding/json, whose
//     HTML escaping is on by default.
//   - The runs of insignificant whitespace are removed by the encoder's compactor.
//   - 9007199254740993 is 2^53+1, which cannot be represented exactly as a float64, so a
//     decode-then-encode round trip through interface{} would silently change the number.
//
// A message value that still contains these bytes verbatim can only have been spliced.
const publisherAdversarialPayload = `{"event":"identity.created","data":{"note":"<b>a & b</b>",  "identity_id":"idt_publisher_wire",   "credit_score":9007199254740993}}`

// publisherIdentityEventType is the identity event name, spelled as a literal because model
// declares event-name constants only for the transaction family and the two repeatable
// names. It is the same string the event catalogue in event_topics_test.go states.
const publisherIdentityEventType = "identity.created"

// publisherFixedOccurredAt is a fixed occurrence instant, so that the message timestamp
// asserted below is a stated value rather than whatever the clock said.
var publisherFixedOccurredAt = time.Date(2026, time.May, 18, 11, 42, 3, 250000000, time.UTC)

// publisherProducedRecord is one record as the broker would have received it: the key that
// decides its partition, the value that reaches subscribers, and the timestamp.
type publisherProducedRecord struct {
	Key   []byte
	Value []byte
	Time  time.Time

	// Headers are captured because the TRACE CONTEXT travels as record headers rather than in
	// the value — deliberately, so the byte-equality guarantees over the value are unaffected.
	// A double that dropped them would let the header injection be removed with every existing
	// assertion still passing.
	Headers []publisherProducedHeader
}

// publisherProducedBatch is one produce request as the broker would have received it.
//
// Acks and Partition are the two fields that make the writer's configuration observable
// rather than merely declared: Acks is int16(Writer.RequiredAcks) as the request carries
// it, and Partition is the partition the writer's balancer chose for the key.
type publisherProducedBatch struct {
	Topic     string
	Partition int
	Acks      int16
	Records   []publisherProducedRecord
}

// publisherFakeTransport is a kafka.RoundTripper that answers metadata and produce
// requests in memory, capturing everything a publish sends.
//
// It is deliberately narrow — no mocking library, no expectations to arrange, and only the
// two request types a synchronous write issues. Anything else is recorded as unexpected and
// refused, so a change that made the publish path talk to the broker in some further way
// would surface as a test failure rather than as a silently-ignored request.
//
// It is safe for concurrent use because it must be: kafka-go performs the produce on the
// partition writer's own goroutine, and the concurrency test below drives several publishes
// at once through one shared writer.
type publisherFakeTransport struct {
	mu sync.Mutex

	// partitions is how many partitions every topic reports in metadata.
	partitions int

	// produceErrorCode is returned as the produce response's per-partition error code.
	// Zero means success. A non-zero value is how a BROKER-SIDE failure is simulated,
	// which is the realistic failure shape: the request was delivered and the cluster
	// refused it.
	produceErrorCode int16

	// transportErr, when set, fails the produce round trip itself rather than the
	// response — the shape a connection failure takes.
	transportErr error

	// metadataTopics and batches are what was asked of the broker, in order.
	metadataTopics []string
	batches        []publisherProducedBatch

	// unexpected records any request type other than metadata or produce.
	unexpected []string
}

// Compile-time proof that the double satisfies the interface the writer's field takes. If
// kafka-go ever changed that contract, this fails the build here rather than leaving a
// test that quietly stopped intercepting anything.
var _ kafka.RoundTripper = (*publisherFakeTransport)(nil)

// newPublisherFakeTransport returns a transport that reports publisherFakePartitions
// partitions for every topic and acknowledges every write.
func newPublisherFakeTransport() *publisherFakeTransport {
	return &publisherFakeTransport{partitions: publisherFakePartitions}
}

// failProduceWith makes the fake broker refuse every produce request with a Kafka error
// code, returning the transport so it can be configured inline.
func (f *publisherFakeTransport) failProduceWith(code kafka.Error) *publisherFakeTransport {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.produceErrorCode = int16(code)

	return f
}

// RoundTrip answers one request.
//
// The context check comes first and is not defensive: a real transport fails on an expired
// or cancelled context, and one of the assertions below depends on this double behaving the
// same way rather than succeeding where a broker could not have been reached.
//
// Parameters:
//   - ctx context.Context: the request context.
//   - _ net.Addr: the broker address, unused because nothing is dialled.
//   - request kafka.Request: the request to answer.
//
// Returns:
//   - kafka.Response: the response for a recognised request.
//   - error: the context error, a configured transport failure, or a refusal for an
//     unrecognised request type.
func (f *publisherFakeTransport) RoundTrip(
	ctx context.Context,
	_ net.Addr,
	request kafka.Request,
) (kafka.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch typed := request.(type) {
	case *metadataAPI.Request:
		return f.metadata(typed), nil
	case *produceAPI.Request:
		return f.produce(typed)
	default:
		f.mu.Lock()
		f.unexpected = append(f.unexpected, fmt.Sprintf("%T", request))
		f.mu.Unlock()

		return nil, fmt.Errorf(
			"publisherFakeTransport: unexpected kafka request %T; a synchronous publish must "+
				"issue only metadata and produce requests", request,
		)
	}
}

// metadata answers the partition lookup a write performs before balancing.
//
// Every requested topic is reported with publisherFakePartitions partitions, all led by the
// single fake broker. The writer reads only the partition COUNT from this, which is what it
// hands to the balancer.
func (f *publisherFakeTransport) metadata(request *metadataAPI.Request) *metadataAPI.Response {
	f.mu.Lock()
	f.metadataTopics = append(f.metadataTopics, request.TopicNames...)
	partitions := f.partitions
	f.mu.Unlock()

	response := &metadataAPI.Response{
		Brokers:      []metadataAPI.ResponseBroker{{NodeID: 1, Host: "fake-broker", Port: 9092}},
		ControllerID: 1,
		ClusterID:    "publisher-fake-cluster",
	}

	for _, name := range request.TopicNames {
		topic := metadataAPI.ResponseTopic{Name: name}
		for partition := 0; partition < partitions; partition++ {
			topic.Partitions = append(topic.Partitions, metadataAPI.ResponsePartition{
				PartitionIndex: int32(partition),
				LeaderID:       1,
				ReplicaNodes:   []int32{1},
				IsrNodes:       []int32{1},
			})
		}
		response.Topics = append(response.Topics, topic)
	}

	return response
}

// produce captures the request and answers it.
//
// The records are drained BEFORE any configured failure is applied, so a failing publish is
// still observable: the bytes and the acknowledgement mode of a refused write are exactly
// what a test about failure classification needs to be able to see.
func (f *publisherFakeTransport) produce(request *produceAPI.Request) (kafka.Response, error) {
	f.mu.Lock()
	errorCode := f.produceErrorCode
	transportErr := f.transportErr
	f.mu.Unlock()

	captured := publisherProducedBatch{Acks: request.Acks}
	response := &produceAPI.Response{}

	for _, topic := range request.Topics {
		captured.Topic = topic.Topic

		for _, partition := range topic.Partitions {
			captured.Partition = int(partition.Partition)

			records, err := publisherDrainRecords(partition.RecordSet.Records)
			if err != nil {
				return nil, err
			}
			captured.Records = append(captured.Records, records...)

			response.Topics = append(response.Topics, produceAPI.ResponseTopic{
				Topic: topic.Topic,
				Partitions: []produceAPI.ResponsePartition{{
					Partition:  partition.Partition,
					ErrorCode:  errorCode,
					BaseOffset: 1,
				}},
			})
		}
	}

	f.mu.Lock()
	f.batches = append(f.batches, captured)
	f.mu.Unlock()

	if transportErr != nil {
		return nil, transportErr
	}

	return response, nil
}

// publisherDrainRecords reads every record out of a produce request's record set.
//
// The reader is single-pass and the fake is its terminal consumer, so draining it here is
// both safe and the only way to see the key and value bytes: they travel as
// protocol.Bytes readers rather than as slices.
//
// Parameters:
//   - reader protocol.RecordReader: the record set's reader. May be nil.
//
// Returns:
//   - []publisherProducedRecord: the records, in order.
//   - error: a read failure, which would be a defect in this double rather than in the
//     publisher.
func publisherDrainRecords(reader protocol.RecordReader) ([]publisherProducedRecord, error) {
	if reader == nil {
		return nil, nil
	}

	records := make([]publisherProducedRecord, 0, 1)

	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}

		key, err := protocol.ReadAll(record.Key)
		if err != nil {
			return nil, err
		}

		value, err := protocol.ReadAll(record.Value)
		if err != nil {
			return nil, err
		}

		headers := make([]publisherProducedHeader, 0, len(record.Headers))
		for _, header := range record.Headers {
			headers = append(headers, publisherProducedHeader{
				Key:   header.Key,
				Value: string(header.Value),
			})
		}

		records = append(records, publisherProducedRecord{
			Key:     key,
			Value:   value,
			Time:    record.Time,
			Headers: headers,
		})
	}
}

// publisherProducedHeader is one record header as the transport saw it, with the value rendered
// as a string because every header this pipeline sets is W3C ASCII text.
type publisherProducedHeader struct {
	Key   string
	Value string
}

// headerValue returns the value of a captured record header, and whether it was present.
//
// Presence is returned separately from the value because ABSENT and EMPTY are different
// outcomes here: an untraced publish must set no header at all, and an empty header value
// would give a consumer an invalid span context to extract rather than nothing to extract.
func (r publisherProducedRecord) headerValue(key string) (string, bool) {
	for _, header := range r.Headers {
		if header.Key == key {
			return header.Value, true
		}
	}

	return "", false
}

// producedBatches returns a copy of everything produced so far.
func (f *publisherFakeTransport) producedBatches() []publisherProducedBatch {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]publisherProducedBatch, len(f.batches))
	copy(out, f.batches)

	return out
}

// onlyRecord returns the single record of the single batch produced so far, failing the
// test if the publish path batched, split or duplicated anything.
//
// The requirement is exact rather than lenient because "one publish is one produce request
// carrying one record" is itself a property worth holding: batching across rows would
// couple the latency of unrelated events, and a duplicate record on one request would be a
// silent double delivery.
//
// Returns:
//   - publisherProducedBatch: the batch.
//   - publisherProducedRecord: its only record.
func (f *publisherFakeTransport) onlyRecord(t *testing.T) (publisherProducedBatch, publisherProducedRecord) {
	t.Helper()

	batches := f.producedBatches()
	require.Len(t, batches, 1, "exactly one produce request must have been issued")
	require.Len(t, batches[0].Records, 1, "one publish must carry exactly one record")

	return batches[0], batches[0].Records[0]
}

// requestedMetadataTopics returns the topics whose partition counts were looked up.
func (f *publisherFakeTransport) requestedMetadataTopics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.metadataTopics))
	copy(out, f.metadataTopics)

	return out
}

// unexpectedRequests returns the request types the publish path issued that this double did
// not expect.
func (f *publisherFakeTransport) unexpectedRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.unexpected))
	copy(out, f.unexpected)

	return out
}

// publisherWithFakeTransport builds a real Kafka publisher whose every writer is wired to
// transport, so publishing exercises the production code path end to end without a broker.
//
// The topic prefix is published FIRST because the publisher pre-creates one writer per owned
// topic at construction, and the inventory it enumerates is derived from the configured
// prefix. Substituting the transport afterwards is safe precisely because construction
// performs no I/O: no writer has connected, so none is holding the transport it was built
// with.
//
// Parameters:
//   - transport *publisherFakeTransport: the double every writer will use.
//
// Returns:
//   - *kafkaPublisher: the publisher, closed automatically when the test ends.
func publisherWithFakeTransport(t *testing.T, transport *publisherFakeTransport) *kafkaPublisher {
	t.Helper()

	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := newTestKafkaPublisher(t)

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	for _, writer := range publisher.writers {
		writer.Transport = transport
	}

	return publisher
}

// publisherWriterSnapshot copies the writer inventory out from under the lock, so
// assertions run without holding it.
func publisherWriterSnapshot(publisher *kafkaPublisher) map[string]*kafka.Writer {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	writers := make(map[string]*kafka.Writer, len(publisher.writers))
	for topic, writer := range publisher.writers {
		writers[topic] = writer
	}

	return writers
}

// publisherPartitionIDs is the partition list the writer hands its balancer: every
// partition of the topic, in ascending order.
func publisherPartitionIDs() []int {
	partitions := make([]int, 0, publisherFakePartitions)
	for partition := 0; partition < publisherFakePartitions; partition++ {
		partitions = append(partitions, partition)
	}

	return partitions
}

// publisherExpectedPartition computes where Murmur2 places a key, independently of the
// publisher.
//
// This is the behavioural half of the balancer assertion: the struct field says which
// balancer was configured, and this says where that balancer actually puts the message. A
// switch to any other stable hash changes this number for at least some keys, so the two
// assertions together cannot both be satisfied by a different balancer.
func publisherExpectedPartition(key string) int {
	balancer := kafka.Murmur2Balancer{}

	return balancer.Balance(kafka.Message{Key: []byte(key)}, publisherPartitionIDs()...)
}

// publisherKeyOnAnotherPartition returns a ledger identifier that Murmur2 places on a
// different partition from key.
//
// It exists so the "a different ledger can land elsewhere" assertion is deterministic
// rather than hopeful: with six partitions, two arbitrary keys collide one time in six, and
// a test that assumed otherwise would fail intermittently for a reason having nothing to do
// with the publisher.
//
// Parameters:
//   - key string: the key whose partition must be avoided.
//
// Returns:
//   - string: a key on a different partition.
func publisherKeyOnAnotherPartition(t *testing.T, key string) string {
	t.Helper()

	target := publisherExpectedPartition(key)

	for candidate := 0; candidate < 512; candidate++ {
		probe := fmt.Sprintf("ldg_publisher_probe_%03d", candidate)
		if publisherExpectedPartition(probe) != target {
			return probe
		}
	}

	t.Fatalf("no probe key out of 512 landed on a partition other than %d, which cannot happen "+
		"with %d partitions unless the balancer ignores the key entirely", target, publisherFakePartitions)

	return ""
}

// publisherEvent builds a LedgerEvent with a fresh identifier and a fixed occurrence
// instant.
func publisherEvent(eventType, aggregateID, payload string) model.LedgerEvent {
	return model.LedgerEvent{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		OccurredAt:    publisherFixedOccurredAt,
		Payload:       json.RawMessage(payload),
		SchemaVersion: model.SchemaVersionV1,
	}
}

// publisherMetricRecord is one captured measurement: its value and its attributes,
// flattened to strings so an assertion can compare a whole attribute set at once. Comparing
// the SET rather than probing for individual keys is what catches an extra, unbounded
// attribute as well as a missing one.
type publisherMetricRecord struct {
	value      float64
	attributes map[string]string
}

// publisherRecordedCounter captures Int64Counter measurements.
//
// The instruments live as package-level variables in internal/metrics, so a test swaps the
// variable rather than installing a global meter provider. That keeps the capture local:
// otel delegates its global meter to the first provider set, once and permanently, so a
// provider installed here would silently blind every other test in the binary that recorded
// afterwards.
type publisherRecordedCounter struct {
	embedded.Int64Counter

	mu      sync.Mutex
	records []publisherMetricRecord
}

var _ otelmetric.Int64Counter = (*publisherRecordedCounter)(nil)

// Add captures one increment and its attributes.
func (c *publisherRecordedCounter) Add(_ context.Context, value int64, options ...otelmetric.AddOption) {
	attributes := publisherAttributeMap(otelmetric.NewAddConfig(options).Attributes())

	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, publisherMetricRecord{value: float64(value), attributes: attributes})
}

// Enabled reports that measurements are always processed, so the code under test takes the
// same path it takes in a process with an exporter configured.
func (c *publisherRecordedCounter) Enabled(context.Context) bool { return true }

// snapshot returns the captured measurements in order.
func (c *publisherRecordedCounter) snapshot() []publisherMetricRecord {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]publisherMetricRecord, len(c.records))
	copy(out, c.records)

	return out
}

// total sums the captured increments, which is how a counter is actually read.
func (c *publisherRecordedCounter) total() float64 {
	total := 0.0
	for _, record := range c.snapshot() {
		total += record.value
	}

	return total
}

// publisherRecordedHistogram captures Float64Histogram measurements.
type publisherRecordedHistogram struct {
	embedded.Float64Histogram

	mu      sync.Mutex
	records []publisherMetricRecord
}

var _ otelmetric.Float64Histogram = (*publisherRecordedHistogram)(nil)

// Record captures one observation and its attributes.
func (h *publisherRecordedHistogram) Record(_ context.Context, value float64, options ...otelmetric.RecordOption) {
	attributes := publisherAttributeMap(otelmetric.NewRecordConfig(options).Attributes())

	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, publisherMetricRecord{value: value, attributes: attributes})
}

// Enabled reports that measurements are always processed.
func (h *publisherRecordedHistogram) Enabled(context.Context) bool { return true }

// snapshot returns the captured observations in order.
func (h *publisherRecordedHistogram) snapshot() []publisherMetricRecord {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]publisherMetricRecord, len(h.records))
	copy(out, h.records)

	return out
}

// publisherAttributeMap flattens an attribute set to a comparable map.
//
// Parameters:
//   - set attribute.Set: the recorded attributes.
//
// Returns:
//   - map[string]string: key to rendered value; never nil, so an empty set compares
//     cleanly.
func publisherAttributeMap(set attribute.Set) map[string]string {
	attributes := make(map[string]string, set.Len())
	for _, keyValue := range set.ToSlice() {
		// Value.String rather than the deprecated Value.Emit; the two agree exactly for the
		// string, int64 and bool attributes the publisher records.
		attributes[string(keyValue.Key)] = keyValue.Value.String()
	}

	return attributes
}

// publisherInstruments is the five instruments a publish can reach.
//
// captureToDispatch is the one acceptance criterion V-1 is read from, so a harness that omitted
// it would leave the criterion's data source untested — which is exactly how it came to be
// declared, bucketed and documented while nothing recorded it.
//
// published is captured even though the PUBLISHER never records it, and that is the point: it is
// how "the acknowledgement is not the delivery" is asserted rather than assumed. The publisher
// used to increment it on the broker's acknowledgement, one step ahead of its own contract; the
// relay owns it now, after the transition that makes the delivery durable. Capturing it here
// means a regression that moves the increment back would fail a test instead of quietly
// double-counting republished events.
type publisherInstruments struct {
	published         *publisherRecordedCounter
	acknowledgements  *publisherRecordedCounter
	attempts          *publisherRecordedCounter
	duration          *publisherRecordedHistogram
	captureToDispatch *publisherRecordedHistogram
}

// publisherCaptureInstruments swaps the five publish instruments for recorders and
// restores the originals when the test ends.
func publisherCaptureInstruments(t *testing.T) *publisherInstruments {
	t.Helper()

	captured := &publisherInstruments{
		published:         &publisherRecordedCounter{},
		acknowledgements:  &publisherRecordedCounter{},
		attempts:          &publisherRecordedCounter{},
		duration:          &publisherRecordedHistogram{},
		captureToDispatch: &publisherRecordedHistogram{},
	}

	publishedOriginal := metrics.EventsPublishedTotal
	acknowledgementsOriginal := metrics.EventBrokerAcknowledgementsTotal
	attemptsOriginal := metrics.EventPublishAttemptsTotal
	durationOriginal := metrics.EventPublishDuration
	captureOriginal := metrics.EventCaptureToDispatchDuration

	t.Cleanup(func() {
		metrics.EventsPublishedTotal = publishedOriginal
		metrics.EventBrokerAcknowledgementsTotal = acknowledgementsOriginal
		metrics.EventPublishAttemptsTotal = attemptsOriginal
		metrics.EventPublishDuration = durationOriginal
		metrics.EventCaptureToDispatchDuration = captureOriginal
	})

	metrics.EventsPublishedTotal = captured.published
	metrics.EventBrokerAcknowledgementsTotal = captured.acknowledgements
	metrics.EventPublishAttemptsTotal = captured.attempts
	metrics.EventPublishDuration = captured.duration
	metrics.EventCaptureToDispatchDuration = captured.captureToDispatch

	return captured
}

// TestRecordPublishAttempt_RecordsTheEndToEndAgeOfAnAcknowledgedEvent is the guard on the
// instrument acceptance criterion V-1 is read from.
//
// V-1 is stated over outbox-to-Kafka latency, and blnk.events.publish.duration cannot answer it:
// its clock starts at the claim, so it excludes the row waiting for the next poll tick, the poll
// interval, and the claim query's latency. A relay stalled for a minute would report a
// five-millisecond publish. blnk.events.capture_to_dispatch.duration is the honest measure — and
// it was declared, bucketed, described and asserted in the metrics package while NOTHING in the
// pipeline recorded it, so the criterion had no data source at all and every dashboard built on
// it would have been empty rather than wrong.
func TestRecordPublishAttempt_RecordsTheEndToEndAgeOfAnAcknowledgedEvent(t *testing.T) {
	captured := publisherCaptureInstruments(t)

	capturedAt := time.Now().Add(-3 * time.Second)
	recordPublishAttempt(context.Background(), PublishResult{
		Status:     model.PublishStatusDispatched,
		Topic:      "blnk.transactions",
		Attempt:    1,
		Purpose:    PublishPurposeOriginal,
		Duration:   40 * time.Millisecond,
		CapturedAt: capturedAt,
	})

	observations := captured.captureToDispatch.snapshot()
	require.Len(t, observations, 1, "an acknowledged publish must be observed exactly once")

	assert.GreaterOrEqual(t, observations[0].value, 3.0,
		"the interval must start at the CAPTURE instant, not at the claim: a figure measured from "+
			"the claim is what lets a stalled relay report a healthy p99")
	assert.Less(t, observations[0].value, 60.0,
		"and it must be the age itself, not a clock difference against the zero time")

	assert.Equal(t, map[string]string{"topic": "blnk.transactions", "attempt": "1"},
		observations[0].attributes,
		"topic and attempt only. The first-attempt population is what V-1 is stated over, so the "+
			"attempt label is what makes the criterion selectable; outcome would be a constant "+
			"here, since only acknowledged publishes are recorded")

	assert.Len(t, captured.duration.snapshot(), 1,
		"the per-attempt duration is still recorded as well: for THIS event the difference "+
			"between the two is its own queue wait, which is what distinguishes a slow broker "+
			"from an under-provisioned relay. Per event only — the same subtraction between two "+
			"p99 figures is a diagnostic, not a queue-wait percentile")
}

// TestRecordPublishAttempt_RecordsNoEndToEndAgeForAnUnacknowledgedEvent covers the three cases
// that must produce NO observation, each of which would corrupt the quantile in a different way.
func TestRecordPublishAttempt_RecordsNoEndToEndAgeForAnUnacknowledgedEvent(t *testing.T) {
	capturedAt := time.Now().Add(-3 * time.Second)

	cases := map[string]struct {
		result PublishResult
		reason string
	}{
		"a retrying attempt": {
			result: PublishResult{
				Status: model.PublishStatusRetrying, Topic: "blnk.transactions", Attempt: 2,
				Purpose: PublishPurposeOriginal, CapturedAt: capturedAt,
			},
			reason: "the event has not arrived, so it has no end-to-end latency; recording one " +
				"credits the histogram with a short duration for an event that is still waiting",
		},
		"a dead-lettered attempt": {
			result: PublishResult{
				Status: model.PublishStatusDeadLettered, Topic: "blnk.transactions", Attempt: 5,
				Purpose: PublishPurposeOriginal, CapturedAt: capturedAt,
			},
			reason: "a dead-letter write is not a delivery",
		},
		"no capture instant": {
			result: PublishResult{
				Status: model.PublishStatusDispatched, Topic: "blnk.transactions", Attempt: 1,
				Purpose: PublishPurposeOriginal,
			},
			reason: "the envelope-only path carries no occurred_at, and the zero time would " +
				"render as an age of several decades",
		},
		"a capture stamped in the future": {
			result: PublishResult{
				Status: model.PublishStatusDispatched, Topic: "blnk.transactions", Attempt: 1,
				Purpose: PublishPurposeOriginal, CapturedAt: time.Now().Add(time.Hour),
			},
			reason: "clock skew between the capturing process and this one must produce a GAP, " +
				"not a zero: a zero is indistinguishable from an instant publish and would " +
				"quietly improve the quantile the criterion is read from",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			captured := publisherCaptureInstruments(t)

			recordPublishAttempt(context.Background(), testCase.result)

			assert.Empty(t, captured.captureToDispatch.snapshot(), testCase.reason)
			assert.Len(t, captured.duration.snapshot(), 1,
				"the per-attempt duration is recorded for every attempt, successful or not")
		})
	}
}

// ---------------------------------------------------------------------------
// The mandated interface
// ---------------------------------------------------------------------------

// Compile-time proof that both implementations satisfy the contract, and that the method
// they satisfy it with has EXACTLY the mandated shape.
//
// The signature `Publish(ctx context.Context, event model.LedgerEvent) error` is a literal
// artifact supplied by the user, so it is pinned in two independent places: here, where an
// added parameter or a tuple return fails the BUILD, and in the reflection test below, which
// states the same contract in a form a reader can see the requirement in. Duplicating the
// implementation file's own assertions is deliberate — a test file that assumed the
// implementation was still asserting them would pass after they were deleted.
var (
	_ EventPublisher = (*kafkaPublisher)(nil)
	_ EventPublisher = (*NoopEventPublisher)(nil)

	// Both implementations also satisfy the fuller internal contract, which is what lets a
	// caller hold either without feature-detecting.
	_ TopicEventPublisher = (*kafkaPublisher)(nil)
	_ TopicEventPublisher = (*NoopEventPublisher)(nil)

	// The typed function-variable assignment only compiles for the exact signature. The
	// values are discarded; the assignment is the assertion.
	_ func(context.Context, model.LedgerEvent) error = (&kafkaPublisher{}).Publish
	_ func(context.Context, model.LedgerEvent) error = (&NoopEventPublisher{}).Publish
)

// TestEventPublisher_MandatedSignatureIsPinnedByReflection states the user-supplied contract
// as a runtime assertion over the interface's own type.
//
// The compile-time assignments above already stop the build on drift, so why also assert it
// here? Because they can be deleted as easily as the signature can be changed, and because
// they cannot say WHY the shape is what it is. This test names the requirement: one method,
// called Publish, taking a context and a model.LedgerEvent, returning a bare error and
// nothing else. Anything richer that a caller needs — the destination, the attempt, the
// result — is exposed through TopicEventPublisher instead, which is how the contract stays
// exactly as it was specified while the pipeline around it grows.
func TestEventPublisher_MandatedSignatureIsPinnedByReflection(t *testing.T) {
	contract := reflect.TypeOf((*EventPublisher)(nil)).Elem()
	require.Equal(t, reflect.Interface, contract.Kind())

	require.Equal(t, 1, contract.NumMethod(),
		"EventPublisher must declare exactly one method; anything else belongs on TopicEventPublisher")

	method := contract.Method(0)
	assert.Equal(t, "Publish", method.Name, "the method name is part of the mandated form")

	signature := method.Type
	require.Equal(t, reflect.Func, signature.Kind())
	assert.False(t, signature.IsVariadic(), "the mandated form takes no variadic options")

	// Two parameters, in this order. A reflected interface method carries no receiver, so
	// these are exactly the declared parameters.
	require.Equal(t, 2, signature.NumIn(),
		"Publish must take a context and an event and nothing more; a third parameter is drift")
	assert.Equal(t, reflect.TypeOf((*context.Context)(nil)).Elem(), signature.In(0),
		"the first parameter must be context.Context, so a publish is cancellable")
	assert.Equal(t, reflect.TypeOf(model.LedgerEvent{}), signature.In(1),
		"the second parameter must be model.LedgerEvent BY VALUE, which is the canonical "+
			"envelope every producer builds")

	// One return, and it is the bare error the specification names.
	require.Equal(t, 1, signature.NumOut(),
		"Publish must return only an error; the richer per-attempt record is PublishToTopic's job")
	assert.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), signature.Out(0))
}

// ---------------------------------------------------------------------------
// The writer inventory and the three load-bearing settings
// ---------------------------------------------------------------------------

// TestEventPublisher_HoldsOneWriterPerOwnedTopic asserts the shape of the publisher's writer
// inventory: one writer per topic Blnk owns, no writer for anything else, and one shared
// transport and address underneath all of them.
//
// A MISSING writer means a publish to that topic takes the lazy-growth path on the hot path,
// taking the write lock for every event. An EXTRA writer holds a connection pool for a
// destination nothing publishes to. The sharing is what keeps the connection count proportional
// to brokers rather than to brokers times topics.
//
// The inventory is taken from AllTopicsWithDeadLetters rather than hardcoded, so this test and
// event_topics.go work from ONE list and a new category cannot leave a topic without a writer.
// The ten names — five category topics and their five `.dlt` siblings — are then required
// individually, because deriving the whole expectation from the implementation would let a
// silently-dropped category pass.
func TestEventPublisher_HoldsOneWriterPerOwnedTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	inventory := AllTopicsWithDeadLetters()
	require.NotEmpty(t, inventory, "the owned-topic inventory cannot be empty")

	writers := publisherWriterSnapshot(publisher)
	require.Len(t, writers, len(inventory),
		"exactly one writer per owned topic: a missing one forces lazy creation onto the publish "+
			"path, an extra one holds connections for a destination nothing publishes to")

	// The three category topics requirement R-6 names, the internal system category
	// ledger.created made necessary, the one internal category system-error and
	// unrecognised events made necessary, and the five `.dlt` siblings requirement R-5
	// names. Written out as literals so a renamed topic or a dropped dead-letter sibling
	// fails here.
	for _, topic := range []string{
		"blnk.transactions", "blnk.balances", "blnk.identities", "blnk.ledgers", "blnk.system",
		"blnk.transactions.dlt", "blnk.balances.dlt", "blnk.identities.dlt", "blnk.ledgers.dlt", "blnk.system.dlt",
	} {
		writer, present := writers[topic]
		require.True(t, present, "topic %q must have its own writer", topic)
		require.NotNil(t, writer)
		assert.Equal(t, topic, writer.Topic,
			"the writer keyed by %q must publish to %q; a mismatch delivers events to the wrong topic", topic, topic)
	}

	// Distinct writers, one shared transport, one shared address.
	seen := make(map[*kafka.Writer]string, len(writers))
	for topic, writer := range writers {
		if previous, duplicate := seen[writer]; duplicate {
			t.Fatalf("topics %q and %q share one writer, so one of them publishes to the other's topic",
				previous, topic)
		}
		seen[writer] = topic

		transport, isSharedTransport := writer.Transport.(*kafka.Transport)
		require.True(t, isSharedTransport, "writer for %q must use the publisher's transport", topic)
		assert.Same(t, publisher.transport, transport,
			"every writer must draw on ONE transport, and so on one connection pool and one "+
				"authentication configuration")
		assert.Equal(t, publisher.addr, writer.Addr,
			"every writer must address the same bootstrap list the publisher was built with")
	}
}

// TestEventPublisher_WriterSettingsAreStatedExplicitly pins every setting on every writer,
// and it exists because three of kafka-go's defaults are actively wrong for a ledger event
// pipeline while the rest would let a library upgrade change delivery semantics with no code
// change at all.
//
// The three the plan calls load-bearing are asserted first and hardest:
//
//   - THE BALANCER is Murmur2, asserted by concrete type AND by value. Any stable hash
//     would pin a key to a partition, but Murmur2 reproduces the Java client's default
//     partitioner exactly, so a message Blnk produces for a key lands where a Java or
//     librdkafka producer would have put it — which matters the moment anything else writes
//     to these topics or a subscriber reasons about partition assignment from the key. The
//     value assertion additionally pins Consistent to false, which is the Java behaviour for
//     a keyless message: spread it, rather than pinning every keyless event to one partition.
//   - REQUIRED ACKS is RequireAll. This MUST be set explicitly: kafka-go substitutes
//     RequireAll for a zero value only inside its deprecated WriterConfig constructor, and a
//     Writer built as a struct literal — which is what this code does — keeps the zero value,
//     which is RequireNone. Under RequireNone a produce call returns as soon as the request
//     is written to the socket, so WriteMessages would report success for events the cluster
//     never stored and the relay would mark their rows dispatched. The assertion below is
//     therefore written to fail if the field is left out, and the wire-level test that follows
//     proves the value actually reaches the broker.
//   - THE INTERNAL RETRY LOOP is off (MaxAttempts 1). kafka-go retries ten times by default
//     with its own backoff, which would multiply the relay's attempt count by ten and make
//     the logged attempt number a fiction. The relay owns retry; its schedule is asserted in
//     event_relay_test.go, not here.
func TestEventPublisher_WriterSettingsAreStatedExplicitly(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newTestKafkaPublisher(t)

	writers := publisherWriterSnapshot(publisher)
	require.NotEmpty(t, writers)

	// The zero value of the field is RequireNone, which is what makes "set explicitly"
	// a testable property rather than a comment: the assertion below cannot be satisfied
	// by omitting the field.
	require.Equal(t, kafka.RequireNone, kafka.RequiredAcks(0),
		"kafka-go's zero RequiredAcks is RequireNone; if that ever changes, the durability "+
			"assertion below needs rewriting rather than merely re-running")
	require.Equal(t, kafka.RequireAll, eventWriterRequiredAcks,
		"the publisher's acknowledgement constant must be the all-replicas setting")

	for topic, writer := range writers {
		t.Run(topic, func(t *testing.T) {
			// SETTING 1's mechanism: a per-topic writer, so the message carries no topic of
			// its own and the key is free to carry the partition key.
			assert.Equal(t, topic, writer.Topic)

			// SETTING 2 — a stable hash balancer, and specifically Murmur2.
			require.NotNil(t, writer.Balancer,
				"a nil balancer falls back to round robin, which scatters one aggregate's events "+
					"across every partition and silently destroys ordering")
			assert.IsType(t, kafka.Murmur2Balancer{}, writer.Balancer,
				"the balancer must be Murmur2, which matches the Java client's default partitioner")
			assert.Equal(t, kafka.Murmur2Balancer{}, writer.Balancer,
				"Consistent must stay false, so a keyless event is spread rather than pinned to one partition")

			// The alternatives kafka-go offers, refused by type. Naming them is what makes a
			// swap to another hash — which would still "work" for ordering — fail here.
			for _, rejected := range []kafka.Balancer{
				&kafka.Hash{}, &kafka.ReferenceHash{}, kafka.CRC32Balancer{},
				&kafka.RoundRobin{}, &kafka.LeastBytes{},
			} {
				assert.NotEqual(t, reflect.TypeOf(rejected), reflect.TypeOf(writer.Balancer),
					"%T is not the configured balancer", rejected)
			}

			// SETTING 3 — the acknowledgement mode, present on the constructed writer.
			assert.Equal(t, kafka.RequireAll, writer.RequiredAcks,
				"only an all-in-sync-replicas acknowledgement makes a dispatched row a durable statement")
			assert.EqualValues(t, -1, writer.RequiredAcks,
				"RequireAll is -1 on the wire; this is the value the produce request must carry")
			assert.NotEqual(t, kafka.RequiredAcks(0), writer.RequiredAcks,
				"the field must be SET: left at its zero value it is RequireNone, and every publish "+
					"would report success without any acknowledgement at all")

			// The remaining settings, each stated rather than inherited.
			assert.Equal(t, 1, writer.MaxAttempts,
				"the writer's own retry loop must be off; retry is the relay's, bounded by its budget")
			assert.Equal(t, 1, writer.BatchSize,
				"a synchronous write must flush immediately; batching to 100 would put kafka-go's "+
					"one-second batch timeout on the latency of every non-bursty publish")
			assert.False(t, writer.Async,
				"an async writer returns nil the moment a message is queued, so a broker outage "+
					"would be invisible and every outbox row would be marked dispatched")
			assert.False(t, writer.AllowAutoTopicCreation,
				"auto-creation would silently take the cluster's default partition count and "+
					"replication factor, discarding both guarantees the pipeline is built on")
			assert.EqualValues(t, eventWriterBatchBytes, writer.BatchBytes,
				"the byte ceiling must leave headroom above the application limit for a dead-letter "+
					"copy, which is the original envelope plus its failure metadata")
			assert.Equal(t, eventWriterBatchTimeout, writer.BatchTimeout)
			assert.Equal(t, eventWriterWriteTimeout, writer.WriteTimeout)
			assert.Equal(t, eventWriterReadTimeout, writer.ReadTimeout)
			assert.NotNil(t, writer.Addr, "a writer with a nil address refuses every write")
			assert.NotNil(t, writer.Logger, "the client's own diagnostics must reach the structured log")
			assert.NotNil(t, writer.ErrorLogger)
		})
	}
}

// TestEventPublisher_ProducesTheEventOnTheWireAsConfigured is the assertion the struct-field
// tests above cannot make: that the settings are what the BROKER would have seen.
//
// One publish is intercepted at the transport and every part of the resulting produce
// request is checked — the destination topic, the acknowledgement mode, the partition the
// balancer chose, the key, the value and the timestamp — together with the result the
// publisher reported for it. This is the single test that would fail if the writer read its
// key from somewhere other than the resolved partition key, if the balancer were swapped, or
// if RequiredAcks were removed and left at its RequireNone zero value.
func TestEventPublisher_ProducesTheEventOnTheWireAsConfigured(t *testing.T) {
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	event := publisherEvent(model.EventTypeTransactionApplied, "txn_publisher_wire", publisherPayload)

	result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
		Event: event,
		Topic: TopicForEvent(event.EventType),
		Key:   publisherLedgerID,
	})
	require.NoError(t, err, "an acknowledged write must not report an error")

	batch, record := transport.onlyRecord(t)

	assert.Equal(t, "blnk.transactions", batch.Topic,
		"a transaction event must be produced to the transactions category topic")

	// The acknowledgement mode, as the broker would have received it.
	assert.EqualValues(t, kafka.RequireAll, batch.Acks,
		"the produce request must ask for an all-in-sync-replicas acknowledgement")
	assert.EqualValues(t, -1, batch.Acks,
		"RequireAll is -1; a 0 here would be RequireNone — fire-and-forget with reported success")

	// The key, which is the whole ordering mechanism.
	assert.Equal(t, []byte(publisherLedgerID), record.Key,
		"the message key must be the request's stored partition key verbatim; a ledger id is one "+
			"example of such a key, not the definition")

	// The partition, which is the balancer's decision about that key.
	assert.Equal(t, publisherExpectedPartition(publisherLedgerID), batch.Partition,
		"the partition must be the one Murmur2 selects for this key out of %d; another stable "+
			"hash would choose differently", publisherFakePartitions)

	// The value, which is the serialised envelope.
	expectedValue, marshalErr := marshalLedgerEvent(event)
	require.NoError(t, marshalErr)
	assert.Equal(t, expectedValue, record.Value,
		"the message value must be the envelope the publisher serialises, byte for byte")
	assert.True(t, bytes.Contains(record.Value, []byte(publisherPayload)),
		"and the stored payload must appear inside it untransformed")
	assert.True(t, event.OccurredAt.Equal(record.Time),
		"the record timestamp must be the event's occurrence instant, not the publish instant")

	// The result the publisher reported for the attempt.
	assert.Equal(t, model.PublishStatusDispatched, result.Status)
	assert.True(t, result.Dispatched())
	assert.NoError(t, result.Err)
	assert.Equal(t, event.EventID, result.EventID)
	assert.Equal(t, model.EventTypeTransactionApplied, result.EventType)
	assert.Equal(t, "blnk.transactions", result.Topic)
	assert.Equal(t, publisherLedgerID, result.PartitionKey)
	assert.Equal(t, 1, result.Attempt, "an unstated attempt is the first attempt")
	assert.Equal(t, PublishPurposeOriginal, result.Purpose,
		"an unstated purpose is a first delivery, which is what the envelope-only path submits")

	// And nothing else was asked of the broker.
	assert.Equal(t, []string{"blnk.transactions"}, transport.requestedMetadataTopics(),
		"the only metadata lookup must be for the destination topic")
	assert.Empty(t, transport.unexpectedRequests(),
		"a publish must issue only metadata and produce requests")
}

// TestEventPublisher_MessageValueSplicesThePayloadBytesVerbatim is the byte-fidelity
// guarantee, asserted on the wire.
//
// LedgerEvent.Payload is typed json.RawMessage precisely so the stored bytes pass through
// untransformed, and two acceptance criteria rest on that: V-8 requires the Kafka message and
// the legacy webhook body to be identical during the dual-delivery window, and V-9 requires a
// replayed dead-lettered event to match the original byte for byte. Both are only achievable
// if the payload is SPLICED into the envelope rather than re-marshalled.
//
// The proof is constructive rather than assumed: the same envelope is also produced by
// marshalling the struct, and the two are asserted to DIFFER. If splicing were replaced by a
// struct marshal, the difference would vanish and this test would fail on that assertion —
// which is what stops it from passing for both implementations.
func TestEventPublisher_MessageValueSplicesThePayloadBytesVerbatim(t *testing.T) {
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	// The event name is a literal rather than a constant because model declares constants
	// only for the transaction family and the two repeatable names; "identity.created" is
	// spelled the same way the event catalogue in event_topics_test.go spells it.
	event := publisherEvent(publisherIdentityEventType, "idt_publisher_wire", publisherAdversarialPayload)

	require.NoError(t, publisher.Publish(context.Background(), event))

	_, record := transport.onlyRecord(t)

	assert.True(t, bytes.Contains(record.Value, []byte(publisherAdversarialPayload)),
		"the payload bytes must appear in the message verbatim: HTML characters unescaped, "+
			"insignificant whitespace intact and the 2^53+1 integer unrounded")

	// The payload is reachable as the envelope's payload member, byte-identical.
	var envelope struct {
		EventID       string          `json:"event_id"`
		EventType     string          `json:"event_type"`
		AggregateID   string          `json:"aggregate_id"`
		Payload       json.RawMessage `json:"payload"`
		SchemaVersion int             `json:"schema_version"`
	}
	require.NoError(t, json.Unmarshal(record.Value, &envelope),
		"the spliced envelope must still be valid JSON")
	assert.Equal(t, publisherAdversarialPayload, string(envelope.Payload),
		"the payload member must be the stored bytes and nothing else")
	assert.Equal(t, event.EventID, envelope.EventID,
		"event_id is the subscriber idempotency key and must survive serialisation")
	assert.Equal(t, publisherIdentityEventType, envelope.EventType)
	assert.Equal(t, "idt_publisher_wire", envelope.AggregateID)
	assert.Equal(t, model.SchemaVersionV1, envelope.SchemaVersion)

	// The counter-example: marshalling the struct produces different bytes, so a message
	// equal to the spliced form cannot have been produced that way.
	remarshalled, err := json.Marshal(event)
	require.NoError(t, err)
	assert.NotEqual(t, remarshalled, record.Value,
		"a struct marshal escapes < > & and compacts whitespace, so it cannot equal the spliced "+
			"envelope; if these are equal the payload is no longer being spliced")
	assert.NotContains(t, string(remarshalled), publisherAdversarialPayload,
		"which is exactly why re-marshalling would break the dual-delivery and replay guarantees")
}

// ---------------------------------------------------------------------------
// Key derivation: the mechanism behind per-partition-key ordering
// ---------------------------------------------------------------------------

// publisherMustPublish publishes one request and requires it to succeed, returning the
// result.
func publisherMustPublish(t *testing.T, publisher *kafkaPublisher, request PublishRequest) PublishResult {
	t.Helper()

	result, err := publisher.PublishToTopic(context.Background(), request)
	require.NoError(t, err)
	require.True(t, result.Dispatched())

	return result
}

// TestEventPublisher_OneLedgerIsOneKeyAndOnePartition is the mechanism acceptance criterion
// V-6 rests on, asserted where it is actually decided: the message on the wire. A ledger id is
// the STORED PARTITION KEY used here as an example; the property is about the key, whatever
// value the outbox row carries.
//
// Kafka orders within a PARTITION and nowhere else, so ordering exists only if every event
// sharing a key shares a partition — which is what a stable hash over a shared key produces.
// Two requests with the same key must therefore carry the same key bytes AND land on the same
// partition, whatever else differs: different event types, different aggregate ids, different
// payloads.
//
// The converse is asserted too, and not for symmetry's sake: if different keys could never land
// on different partitions the first assertion would hold vacuously — a balancer that ignored
// the key, or a single-partition topic, would satisfy it — so a key known to hash elsewhere is
// published and required to land elsewhere. That makes this a test of the key's INFLUENCE
// rather than of its constancy.
//
// It mirrors the precedent the repository already set: the transaction queue shards by hashing
// the source balance id so work for one pair stays on one lane.
func TestEventPublisher_OneLedgerIsOneKeyAndOnePartition(t *testing.T) {
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	// Two DIFFERENT aggregates, in the same ledger, with different event types.
	first := publisherEvent(model.EventTypeTransactionQueued, "txn_publisher_first", publisherPayload)
	second := publisherEvent(model.EventTypeTransactionApplied, "txn_publisher_second", publisherPayload)

	firstResult := publisherMustPublish(t, publisher, PublishRequest{Event: first, Key: publisherLedgerID})
	secondResult := publisherMustPublish(t, publisher, PublishRequest{Event: second, Key: publisherLedgerID})

	assert.Equal(t, publisherLedgerID, firstResult.PartitionKey)
	assert.Equal(t, firstResult.PartitionKey, secondResult.PartitionKey)

	batches := transport.producedBatches()
	require.Len(t, batches, 2)
	assert.Equal(t, batches[0].Records[0].Key, batches[1].Records[0].Key,
		"two events in one ledger must be keyed identically")
	assert.Equal(t, batches[0].Partition, batches[1].Partition,
		"and must therefore land on ONE partition, because Kafka orders within a partition only")

	// A different ledger: a different key, and — for a key chosen to hash elsewhere — a
	// different partition.
	elsewhere := publisherKeyOnAnotherPartition(t, publisherLedgerID)
	third := publisherEvent(model.EventTypeTransactionApplied, "txn_publisher_third", publisherPayload)
	thirdResult := publisherMustPublish(t, publisher, PublishRequest{Event: third, Key: elsewhere})

	assert.NotEqual(t, publisherLedgerID, thirdResult.PartitionKey)

	batches = transport.producedBatches()
	require.Len(t, batches, 3)
	assert.NotEqual(t, batches[0].Records[0].Key, batches[2].Records[0].Key,
		"a different ledger must produce a different key")
	assert.NotEqual(t, batches[0].Partition, batches[2].Partition,
		"and the key must actually drive placement: if every key landed on one partition, the "+
			"ordering assertion above would hold for a balancer that ignored the key")

	// A second ledger id spelled out, for the plain statement of the rule.
	fourth := publisherEvent(model.EventTypeTransactionApplied, "txn_publisher_fourth", publisherPayload)
	fourthResult := publisherMustPublish(t, publisher, PublishRequest{Event: fourth, Key: publisherOtherLedgerID})
	assert.Equal(t, publisherOtherLedgerID, fourthResult.PartitionKey)
	assert.NotEqual(t, firstResult.PartitionKey, fourthResult.PartitionKey)
}

// TestEventPublisher_KeyIsNeverEmptyForAnyCatalogueEventType walks the WHOLE event catalogue
// and requires every one of the thirteen event strings to reach the broker with a key.
//
// An empty key is the one failure mode here that nothing reports: kafka-go treats a nil key
// as absent and the balancer spreads the message across partitions, so ordering is lost
// quietly, with no error, no log line and no metric — and it is lost for the aggregate whose
// events were spread, which is a correctness property of the ledger rather than a delivery
// inconvenience.
//
// Both rungs of the documented fallback chain are exercised for every event type: the
// supplied partition key, which is what PublishRequestFromOutbox provides from the stored
// row, and the aggregate id, which is what the envelope-only Publish path falls back to. The
// runtime-composed bulk family and system.error are included by construction, because the
// catalogue is the source of the list rather than a hand-written subset of it — and the
// composed bulk name is additionally published, since that family's names never appear as
// literals anywhere.
func TestEventPublisher_KeyIsNeverEmptyForAnyCatalogueEventType(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	eventTypes := catalogueEventTypes()
	require.Len(t, eventTypes, eventCatalogueSize,
		"every event string Blnk emits must be covered; a fourteenth arriving without a catalogue "+
			"row would otherwise be published unkeyed with nothing to say so")

	// A name composed at runtime the way transaction_bulk.go composes it — the literal
	// prefix plus the batch status, of which "failed" is one that reaches it — so the family
	// is covered as a family and not only through its one catalogued member. Its names never
	// appear as literals in the producer, so nothing else can cover it.
	eventTypes = append(eventTypes, "bulk_transaction.failed")

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			aggregateID := "agg_" + strings.ReplaceAll(eventType, ".", "_")

			t.Run("keyed by the stored partition key", func(t *testing.T) {
				transport := newPublisherFakeTransport()
				publisher := publisherWithFakeTransport(t, transport)

				event := publisherEvent(eventType, aggregateID, publisherPayload)
				result := publisherMustPublish(t, publisher, PublishRequest{
					Event: event,
					Topic: TopicForEvent(eventType),
					Key:   publisherLedgerID,
				})

				_, record := transport.onlyRecord(t)
				assert.Equal(t, publisherLedgerID, result.PartitionKey)
				assert.Equal(t, []byte(publisherLedgerID), record.Key,
					"the stored key must reach the wire for %q", eventType)
				assert.NotEmpty(t, record.Key)
			})

			t.Run("falling back to the aggregate when no key is supplied", func(t *testing.T) {
				transport := newPublisherFakeTransport()
				publisher := publisherWithFakeTransport(t, transport)

				event := publisherEvent(eventType, aggregateID, publisherPayload)

				// The envelope-only path: no topic, no key, no attempt — exactly what the
				// mandated Publish method submits.
				require.NoError(t, publisher.Publish(context.Background(), event))

				_, record := transport.onlyRecord(t)
				assert.Equal(t, []byte(aggregateID), record.Key,
					"with no stored key, %q must still be pinned to one partition by its aggregate", eventType)
				assert.NotEmpty(t, record.Key,
					"a keyless message is spread across partitions, which discards ordering silently")
			})
		})
	}
}

// TestEventPublisher_AnEventBelongingToNothingIsWrittenUnkeyed is the deliberate exception,
// asserted so that it stays deliberate.
//
// An event with neither a partition key nor an aggregate — an internal error notification, for
// instance — has nothing to be ordered against, and pinning every such event to one partition
// would only create a hot spot. The key must then be NIL rather than an empty slice, because
// Murmur2 (like the Java partitioner it reproduces) treats a nil key as absent and spreads the
// message, while an empty non-nil slice is a defined value that hashes to one fixed partition.
//
// This case is unreachable for an outbox-backed event: the partition key column is NOT NULL
// with a non-blank CHECK, and PrepareEventOutbox has its own fallback chain.
func TestEventPublisher_AnEventBelongingToNothingIsWrittenUnkeyed(t *testing.T) {
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	event := publisherEvent(model.EventTypeSystemError, "", publisherPayload)

	result := publisherMustPublish(t, publisher, PublishRequest{Event: event})

	assert.Empty(t, result.PartitionKey,
		"an event belonging to neither a ledger nor an aggregate has no key to report")

	_, record := transport.onlyRecord(t)
	assert.Nil(t, record.Key,
		"the key must be nil, not empty: an empty slice is a defined value that would pin every "+
			"such event to one partition")
}

// ---------------------------------------------------------------------------
// Graceful degradation: the no-broker steady state
// ---------------------------------------------------------------------------

// TestNoOpPublisher_IsSelectedWhenNoBrokersAreConfigured is the single most load-bearing test
// in this file, because it protects code that has nothing to do with Kafka.
//
// # Why an unconfigured deployment must not be an error
//
// Blnk has always been able to run with no notification sink: SendWebhook returns nil without
// enqueuing anything when no webhook URL is configured (webhooks.go), so a deployment with no
// subscriber is a supported configuration rather than a broken one. The Kafka publisher
// reproduces that contract exactly — no brokers means the no-op and a NIL ERROR — and the
// consequences of getting it wrong reach far outside this feature:
//
//   - webhooks_test.go calls NewBlnk(nil) with only Redis.Dns configured, and
//     TestHTTPClientConfiguration does so with nothing else configured at all. NewBlnk builds
//     the event publisher on that path, so a constructor that errored, dialled or blocked on an
//     empty broker list would break a large part of the root test suite for reasons unrelated
//     to what those tests assert.
//   - Every existing production deployment that does not run Kafka would fail to start.
//
// The no-broker state is a legitimate steady state, not a degraded one. Every way of arriving
// at it is asserted, because they are all reachable: a process whose configuration has not
// been loaded, a deployment that never sets KAFKA_BROKERS, and an environment file whose stray
// separator or trailing comma leaves whitespace-only entries behind.
func TestNoOpPublisher_IsSelectedWhenNoBrokersAreConfigured(t *testing.T) {
	testCases := []struct {
		name          string
		configuration *config.Configuration
	}{
		{
			name:          "a nil configuration, from a process whose configuration was never loaded",
			configuration: nil,
		},
		{
			name:          "no Kafka block at all, which is every deployment that does not run Kafka",
			configuration: &config.Configuration{},
		},
		{
			name:          "an explicitly empty broker list",
			configuration: &config.Configuration{Kafka: config.KafkaConfig{Brokers: []string{}}},
		},
		{
			name:          "a whitespace-only broker, from a stray separator in an environment file",
			configuration: &config.Configuration{Kafka: config.KafkaConfig{Brokers: []string{"   "}}},
		},
		{
			name: "empty entries left by a trailing comma and a stray newline",
			configuration: &config.Configuration{Kafka: config.KafkaConfig{
				Brokers: []string{"", "\t", "\n"},
			}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			started := time.Now()
			publisher, err := NewEventPublisher(testCase.configuration)
			elapsed := time.Since(started)

			require.NoError(t, err,
				"the absence of Kafka must never be reported as an error, or every deployment "+
					"without a broker fails to start")
			require.NotNil(t, publisher,
				"the publisher must never be nil, so no caller needs a nil branch")
			assert.IsType(t, &NoopEventPublisher{}, publisher,
				"an empty broker list must select the no-op implementation")
			assert.True(t, IsNoopEventPublisher(publisher),
				"and that choice must be observable, because startup validation asserts it")

			assert.Less(t, elapsed, publisherConstructionBudget,
				"selecting the no-op must be immediate: nothing may be dialled or resolved")

			assert.NoError(t, publisher.Publish(context.Background(), model.LedgerEvent{}),
				"the no-op accepts an event and reports success, exactly as SendWebhook did with "+
					"no webhook URL configured")
		})
	}
}

// TestEventPublisher_ConstructionPerformsNoDialAndNoNameResolution proves the no-I/O property
// for the CONFIGURED case too, which is the half that could regress unnoticed.
//
// The no-op path obviously touches no network. The Kafka path is the interesting one: it
// builds a transport, prepares SASL credentials and creates a writer per owned topic, and
// every one of those is pure computation — kafka.TCP only canonicalises address strings, a
// SCRAM mechanism is arithmetic over the credential, and a kafka.Writer connects lazily on its
// first write. If any of it became eager, NewBlnk would start depending on a reachable broker
// on every process start and in every test that constructs a Blnk instance.
//
// The two broker addresses are chosen so that eagerness cannot hide. broker.publisher-test.invalid
// uses the .invalid TLD, which RFC 2606 guarantees never resolves, so a construction that
// resolved names would fail or wait on a resolver timeout. publisherBlackholeBroker is RFC 1918
// private space, routed nowhere in a default environment, so a construction that dialled would
// hang until the transport's five-second dial timeout — well past the budget asserted here. The writer statistics are then
// read as a direct statement of the same fact: zero dials, zero writes, zero errors.
func TestEventPublisher_ConstructionPerformsNoDialAndNoNameResolution(t *testing.T) {
	kafkaConfig := config.KafkaConfig{
		Brokers:     []string{publisherUnresolvableBroker, publisherBlackholeBroker},
		TopicPrefix: DefaultTopicPrefix,
		// The transport refuses a plaintext broker without this acknowledgement, because SASL
		// over plaintext puts the credential on the wire in the clear. Nothing here dials, so
		// the acknowledgement is the honest way to reach a constructed publisher rather than a
		// reason to weaken the refusal.
		InsecureLocalDev: true,
	}
	storeKafkaConfig(t, kafkaConfig)

	started := time.Now()
	publisher, err := NewEventPublisher(&config.Configuration{Kafka: kafkaConfig})
	elapsed := time.Since(started)

	require.NoError(t, err, "a configured broker list must build a publisher without contacting it")
	require.IsType(t, &kafkaPublisher{}, publisher,
		"a configured broker list must select the Kafka implementation, not the no-op")
	assert.False(t, IsNoopEventPublisher(publisher))

	kafkaBacked, isKafkaBacked := publisher.(*kafkaPublisher)
	require.True(t, isKafkaBacked)
	t.Cleanup(func() {
		if closeErr := kafkaBacked.Close(); closeErr != nil {
			t.Logf("failed to close the publisher: %v", closeErr)
		}
	})

	assert.Less(t, elapsed, publisherConstructionBudget,
		"construction must be pure computation; a dial to TEST-NET-2 would hang for the "+
			"transport's five-second dial timeout and a name lookup for a .invalid host would "+
			"wait on the resolver")

	writers := publisherWriterSnapshot(kafkaBacked)
	require.Len(t, writers, len(AllTopicsWithDeadLetters()),
		"every owned topic must have a writer, all of them built without I/O")

	for topic, writer := range writers {
		stats := writer.Stats()
		assert.Zero(t, stats.Dials, "writer for %q must not have dialled during construction", topic)
		assert.Zero(t, stats.Writes, "writer for %q must not have written during construction", topic)
		assert.Zero(t, stats.Errors, "writer for %q must not have failed during construction", topic)
	}
}

// TestNoOpPublisher_PublishesNothingAndFailsNothing pins the no-op's whole observable
// behaviour.
//
// The I/O claim is proved with a CANCELLED context, which is the strongest statement available
// without a broker to not-contact: any implementation that performed a network call would fail
// on it, so returning success is evidence that nothing was attempted. The same reasoning covers
// the invalid payload — a publisher that serialised the event would reject bytes that are not
// JSON, and this one has nothing to serialise.
//
// Its lifecycle is asserted for the same reason blnk.go's shutdown path can stay simple: Close
// is idempotent and nil-safe, and a nil publisher counts as a no-op, because a publisher that
// publishes nothing and one that does not exist have identical observable behaviour.
func TestNoOpPublisher_PublishesNothingAndFailsNothing(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := NewNoopEventPublisher()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, publisher.Publish(cancelled, model.LedgerEvent{}),
		"the no-op must succeed even on a cancelled context: a real network call could not")
	assert.NoError(t, publisher.Publish(
		cancelled,
		publisherEvent(model.EventTypeTransactionApplied, "txn_noop", publisherPayload),
	))
	assert.NoError(t, publisher.Publish(context.Background(), model.LedgerEvent{
		EventID:   uuid.NewString(),
		EventType: model.EventTypeTransactionApplied,
		Payload:   json.RawMessage(`{"not":`),
	}), "nothing is serialised, so even a payload no broker would accept is discarded cleanly")

	// The fuller contract is satisfied too, so a relay or dead-letter writer holding a no-op
	// needs no special case.
	result, err := publisher.PublishToTopic(cancelled, PublishRequest{
		Event: publisherEvent(model.EventTypeTransactionApplied, "txn_noop", publisherPayload),
		Key:   publisherLedgerID,
	})
	require.NoError(t, err)
	assert.Equal(t, model.PublishStatusDispatched, result.Status,
		"with no broker configured there is nothing to fail against, so the result is dispatched")
	assert.True(t, result.Dispatched())
	assert.Equal(t, "blnk.transactions", result.Topic,
		"the request's topic is resolved through the same fallback the Kafka path uses")
	assert.Equal(t, publisherLedgerID, result.PartitionKey)
	assert.Equal(t, 1, result.Attempt)
	assert.Equal(t, PublishPurposeOriginal, result.Purpose)
	assert.Zero(t, result.Duration, "nothing was dispatched, so nothing took any time")

	assert.NoError(t, publisher.Close())
	assert.NoError(t, publisher.Close(), "Close must be idempotent")

	var unassigned *NoopEventPublisher
	assert.NoError(t, unassigned.Close(),
		"Close must be nil-safe, which is what lets the Blnk shutdown path stay nil-guarded")
	assert.True(t, IsNoopEventPublisher(nil),
		"a nil publisher publishes nothing, which is the same observable behaviour as the no-op")
}

// ---------------------------------------------------------------------------
// PublishResult: what one attempt reports
// ---------------------------------------------------------------------------

// TestEventPublisher_PublishResultReportsTheOutcomeOfTheAttempt covers the status vocabulary a
// single attempt can report, and why the distinctions in it exist.
//
// PublishResult is the observability record requirement R-3 asks for, and it is consumed by two
// audiences that need different things: the relay reads the classification to decide whether
// another attempt is worth making, and the metrics layer reads the status verbatim as the
// outcome attribute of the publish-attempts counter. Both break in the same way if a failure is
// described imprecisely.
//
// The four cases below are the four an attempt can actually be in, and the two failure axes are
// deliberately independent:
//
//   - TRANSIENT says what the failure LOOKED like — a leader election in flight, a broker that
//     is down, a timeout.
//   - RETRYABLE says whether anything further will actually be tried, which is transient AND
//     budget remaining.
//
// They differ exactly on the last permitted attempt, and that is the case an operator most needs
// to see: reporting it as "retrying" describes retry pressure that no longer exists and makes a
// permanently-stuck event indistinguishable from a busy one on the attempts counter.
//
// The retry SCHEDULE — base delay, multiplier, cap, attempt count — is the relay's and is
// asserted in event_relay_test.go. Nothing here sleeps or loops.
func TestEventPublisher_PublishResultReportsTheOutcomeOfTheAttempt(t *testing.T) {
	t.Run("a broker acknowledgement is dispatched", func(t *testing.T) {
		transport := newPublisherFakeTransport()
		publisher := publisherWithFakeTransport(t, transport)

		event := publisherEvent(model.EventTypeTransactionApplied, "txn_dispatched", publisherPayload)
		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Key:         publisherLedgerID,
			Attempt:     1,
			MaxAttempts: 5,
		})

		require.NoError(t, err)
		assert.Equal(t, model.PublishStatusDispatched, result.Status)
		assert.True(t, result.Dispatched(), "Dispatched must read the status, so success has one definition")
		assert.NoError(t, result.Err)
		assert.False(t, result.Transient, "a success has no failure to classify")
		assert.False(t, result.Retryable, "and nothing to retry")
		assert.Equal(t, 5, result.MaxAttempts,
			"the stated budget must be carried through, so a log line can say 'attempt 1 of 5'")
		assert.GreaterOrEqual(t, result.Duration, time.Duration(0))
	})

	t.Run("a transient failure inside the budget is retrying", func(t *testing.T) {
		transport := newPublisherFakeTransport().failProduceWith(kafka.LeaderNotAvailable)
		publisher := publisherWithFakeTransport(t, transport)

		event := publisherEvent(model.EventTypeTransactionApplied, "txn_retrying", publisherPayload)
		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Key:         publisherLedgerID,
			Attempt:     2,
			MaxAttempts: 5,
		})

		require.Error(t, err, "an unacknowledged write must be reported, never swallowed")
		assert.Equal(t, model.PublishStatusRetrying, result.Status,
			"a leader election in flight is the textbook recoverable failure")
		assert.True(t, result.Transient)
		assert.True(t, result.Retryable, "attempt 2 of 5 leaves budget, so another attempt is possible")
		assert.False(t, result.Dispatched())
		assert.Equal(t, err, result.Err,
			"the returned error and the result's error must be the same value, so a caller may branch on either")

		// The classification must also be readable from the error alone, because the mandated
		// Publish signature narrows the result away.
		assert.True(t, IsTransientPublishError(err))

		var publishErr *PublishError
		require.ErrorAs(t, err, &publishErr, "the error must be the typed publish error")
		assert.Equal(t, 2, publishErr.Attempt)
		assert.Equal(t, event.EventID, publishErr.EventID)
		assert.True(t, publishErr.Transient)

		// And the underlying broker failure must remain reachable through the wrapper, which is
		// what lets a caller inspect the real cause.
		var writeErrors kafka.WriteErrors
		require.ErrorAs(t, err, &writeErrors,
			"Unwrap must expose the library's own error rather than replacing it")
		assert.Equal(t, 1, writeErrors.Count())

		// The write did reach the broker — it was refused, not skipped.
		batch, record := transport.onlyRecord(t)
		assert.EqualValues(t, kafka.RequireAll, batch.Acks)
		assert.Equal(t, []byte(publisherLedgerID), record.Key)
	})

	t.Run("a transient failure on the last permitted attempt is terminal", func(t *testing.T) {
		transport := newPublisherFakeTransport().failProduceWith(kafka.LeaderNotAvailable)
		publisher := publisherWithFakeTransport(t, transport)

		event := publisherEvent(model.EventTypeTransactionApplied, "txn_exhausted", publisherPayload)
		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Key:         publisherLedgerID,
			Attempt:     5,
			MaxAttempts: 5,
		})

		require.Error(t, err)
		assert.Equal(t, model.PublishStatusRetrying, result.Status,
			"every failed attempt reports retrying: requirement R-3 fixes the outcome vocabulary "+
				"at three values and it is a published metric label domain. It is NOT dead_lettered "+
				"— that value means an acknowledged write to a `.dlt` sibling, which this attempt "+
				"did not perform and may never lead to")
		assert.True(t, result.Transient,
			"the failure still LOOKED recoverable, which is a different question from whether "+
				"anything more will be attempted")
		assert.False(t, result.Retryable)
		assert.False(t, result.PermanentFailure(),
			"and it is not PERMANENT: the budget ran out, which the database decides inside "+
				"MarkEventFailed so that two instances cannot both conclude they were last. The "+
				"relay must reach that decision through the ordinary path, not short-circuit it")
		assert.Equal(t, 5, result.MaxAttempts)
	})

	t.Run("a permanent failure is terminal whatever the budget says", func(t *testing.T) {
		transport := newPublisherFakeTransport().failProduceWith(kafka.TopicAuthorizationFailed)
		publisher := publisherWithFakeTransport(t, transport)

		event := publisherEvent(model.EventTypeTransactionApplied, "txn_permanent", publisherPayload)
		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Key:         publisherLedgerID,
			Attempt:     1,
			MaxAttempts: 5,
		})

		require.Error(t, err)
		assert.Equal(t, model.PublishStatusRetrying, result.Status,
			"the status stays inside the three-value vocabulary even for a permanent failure; the "+
				"permanence is carried on Classified and Transient, asserted below. dead_lettered "+
				"is reserved for an acknowledged write to a `.dlt` sibling, so reporting it here "+
				"made the attempts counter assert one dead letter per attempt for a single real "+
				"one — and five for events that produced none at all")
		assert.True(t, result.Classified,
			"the publisher reached a verdict, and that marker is what authorises PermanentFailure "+
				"to end this event's life early")
		assert.False(t, result.Transient,
			"an unauthorised principal is not a condition another attempt can change")
		assert.False(t, result.Retryable,
			"so the whole budget must not be spent establishing what is already known")
		assert.True(t, result.PermanentFailure(),
			"and the relay reads exactly this to act on it: the verdict used to be computed, "+
				"logged and ignored, so the log said retryable=false and then announced another "+
				"attempt on the next line")
		assert.False(t, IsTransientPublishError(err))
	})
}

// ---------------------------------------------------------------------------
// The three instruments one publish records
// ---------------------------------------------------------------------------

// TestEventPublisher_RecordsThePublishInstruments asserts the observability the plan specifies,
// with the attribute sets the instrument declarations in internal/metrics document.
//
// Each of the three carries a specific operational weight, which is why the attributes are
// asserted as whole SETS rather than probed key by key — an extra attribute is as much of a
// defect as a missing one, since a histogram multiplies its label cardinality by its bucket
// count:
//
//   - EventBrokerAcknowledgementsTotal is ACKNOWLEDGED BROKER WRITE THROUGHPUT. It is
//     incremented here, on the acknowledgement, which is before the outbox row is moved — so a
//     redelivery after a crash increments it a second time for one event. That is the right
//     reading of write throughput and the wrong reading of delivery, which is why the per-event
//     counts live at the relay's durable transitions. It also carries PURPOSE, because a replay
//     and a dead-letter write are real acknowledgements and only this instrument counts them.
//   - EventPublishAttemptsTotal makes retry pressure visible independently of delivery volume,
//     and its outcome vocabulary is the model.PublishStatus values, reused rather than
//     redeclared so the code and the label set cannot drift.
//   - EventPublishDuration is where criterion V-1's sub-two-second p99 is read from, and the
//     instrument's declaration states that query as {attempt="1",outcome="dispatched"}. Both
//     attributes must therefore be present, or the population the target is stated over cannot
//     be selected at all.
//
// EventsPublishedTotal and EventsDispatchedTotal are deliberately NOT among them, and their
// absence is asserted rather than merely unmentioned. That counter's contract — stated on its declaration in internal/metrics and
// published in docs/metrics.md — is one increment per ORIGINAL event whose delivery is DURABLY
// RECORDED, and a broker acknowledgement is not that: the outbox row can still fail to be marked,
// in which case the same original event is published again. Counting here made the series a count
// of broker WRITES, which inflated the V-1 throughput figure and understated the V-3 dead-letter
// rate on exactly the runs where both matter. The increment therefore belongs to the relay's
// dispatched transition, which is conditional on the claim token and so succeeds once per event;
// TestEventRelay_CountsOneSettledPublicationPerOriginalEvent covers it there.
//
// The envelope-only Publish method is used deliberately: it submits a request with no topic, no
// key, no attempt and no purpose, so the recorded attributes are the ones the fallbacks produce
// — which is the path every domain call site takes.
func TestEventPublisher_RecordsThePublishInstruments(t *testing.T) {
	instruments := publisherCaptureInstruments(t)
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)

	event := publisherEvent(model.EventTypeTransactionApplied, "txn_publisher_metrics", publisherPayload)
	require.NoError(t, publisher.Publish(context.Background(), event))

	acknowledgements := instruments.acknowledgements.snapshot()
	require.Len(t, acknowledgements, 1,
		"an acknowledged write must increment the write counter exactly once; without it there is "+
			"no measure of broker throughput and no numerator for the redelivery factor")
	assert.EqualValues(t, 1, acknowledgements[0].value)
	assert.Equal(t, map[string]string{
		"topic":      "blnk.transactions",
		"event_type": "transaction.applied",
		"purpose":    string(PublishPurposeOriginal),
	}, acknowledgements[0].attributes,
		"writes are attributed by topic, event type and purpose — topic and event type being the "+
			"SAME pair EventsPublishedTotal carries, which is what makes the two joinable into the "+
			"redelivery factor, and purpose being what keeps a replay or a dead-letter write out of "+
			"that division")

	assert.Zero(t, instruments.published.total(),
		"and the PER-EVENT counter is not touched here. Its contract is one increment per original "+
			"event whose Kafka leg is durably recorded, which is a fact only the relay's marking "+
			"transition establishes — counting it on the acknowledgement is what made it a count of "+
			"writes")

	attempts := instruments.attempts.snapshot()
	require.Len(t, attempts, 1, "exactly one attempt was made, so exactly one is counted")
	assert.EqualValues(t, 1, attempts[0].value)
	assert.Equal(t, map[string]string{
		"outcome":  string(model.PublishStatusDispatched),
		"terminal": publishTerminalFalse,
	}, attempts[0].attributes,
		"the outcome vocabulary is model.PublishStatus, reused rather than redeclared, and the "+
			"terminal dimension accompanies it on every attempt — a success is never terminal, and "+
			"recording the pair unconditionally is what keeps {outcome=\"retrying\",terminal=\"true\"} "+
			"a complete answer to \"which events are stuck\"")

	duration := instruments.duration.snapshot()
	require.Len(t, duration, 1)
	assert.Equal(t, map[string]string{
		"topic":   "blnk.transactions",
		"attempt": "1",
		"outcome": string(model.PublishStatusDispatched),
	}, duration[0].attributes,
		"the latency target is read from {attempt=\"1\",outcome=\"dispatched\"}, so both attributes "+
			"must be recorded or that query matches nothing")
	assert.GreaterOrEqual(t, duration[0].value, 0.0, "the duration is recorded in seconds")
	assert.Less(t, duration[0].value, publisherConstructionBudget.Seconds(),
		"an in-memory publish cannot plausibly take longer than this")
}

// TestEventPublisher_KeepsRetriedAndReplayedPublishesOutOfTheFirstAttemptPopulation is the other
// half of the latency contract: the population the p99 is read from must contain first
// deliveries and nothing else.
//
// Three cases would contaminate it if the attempt attribute were derived from the attempt number
// alone, and each is asserted here:
//
//   - A RETRY is a genuine attempt in a retry sequence and gets its number, so it is visible but
//     separable.
//   - A REPLAY is an operator-triggered re-publication of a dead-lettered event. It is not part
//     of any retry sequence — the sequence it belonged to ended — so it carries the fixed
//     "replay" token instead of a number. It must never reach the published-events counter
//     either, which it cannot: no publish increments that counter, and the relay's settlement
//     is the only writer.
//   - A FAILED attempt is recorded on the duration histogram too, under its own outcome. A broker
//     that times out is precisely when latency data matters, and the outcome attribute is what
//     keeps those observations out of the success population.
func TestEventPublisher_KeepsRetriedAndReplayedPublishesOutOfTheFirstAttemptPopulation(t *testing.T) {
	t.Run("a retry carries its attempt number", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())

		publisherMustPublish(t, publisher, PublishRequest{
			Event:       publisherEvent(model.EventTypeTransactionApplied, "txn_retry_label", publisherPayload),
			Key:         publisherLedgerID,
			Attempt:     3,
			MaxAttempts: 5,
		})

		duration := instruments.duration.snapshot()
		require.Len(t, duration, 1)
		assert.Equal(t, "3", duration[0].attributes["attempt"],
			"a third attempt must be reported as such, so first-attempt latency stays readable alone")
		assert.Zero(t, instruments.published.total(),
			"a retried delivery is still a first delivery of the event and is counted exactly once — "+
				"but on the relay's durable transition, not here. An acknowledgement whose "+
				"bookkeeping then fails is followed by another publish, and counting both is how "+
				"one event became two")
	})

	t.Run("a replay is neither counted as original throughput nor labelled with an attempt number", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())

		publisherMustPublish(t, publisher, PublishRequest{
			Event:   publisherEvent(model.EventTypeTransactionApplied, "txn_replay_label", publisherPayload),
			Key:     publisherLedgerID,
			Attempt: 1,
			Purpose: PublishPurposeReplay,
		})

		assert.Zero(t, instruments.published.total(),
			"a replay is triage, not throughput: counting it here would make measured write "+
				"throughput depend on how much dead-letter triage happened that day")

		duration := instruments.duration.snapshot()
		require.Len(t, duration, 1)
		assert.Equal(t, "replay", duration[0].attributes["attempt"],
			"a replay belongs to no retry sequence, so it gets a fixed token rather than a number")

		attempts := instruments.attempts.snapshot()
		require.Len(t, attempts, 1,
			"a replay is still an attempt: it is visible on the attempts counter, which is where "+
				"re-delivery belongs")
	})

	t.Run("a dead-letter write is kept out of the throughput count as well", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())

		publisherMustPublish(t, publisher, PublishRequest{
			Event:   publisherEvent(model.EventTypeTransactionApplied, "txn_dlt_label", publisherPayload),
			Topic:   DLTFor(TopicForEvent(model.EventTypeTransactionApplied)),
			Key:     publisherLedgerID,
			Attempt: 5,
			Purpose: PublishPurposeDeadLetter,
		})

		assert.Zero(t, instruments.published.total(),
			"a dead-letter write is the same event's second trip to a broker, so counting it as "+
				"original throughput would count one event's arrival twice")

		acknowledgements := instruments.acknowledgements.snapshot()
		require.Len(t, acknowledgements, 1,
			"the `.dlt` write was acknowledged, and that is traffic the broker accepted")
		assert.Equal(t, string(PublishPurposeDeadLetter), acknowledgements[0].attributes["purpose"])
		assert.Equal(t, "blnk.transactions.dlt", acknowledgements[0].attributes["topic"],
			"attributed to the sibling it actually landed on, not the topic it came from")

		duration := instruments.duration.snapshot()
		require.Len(t, duration, 1)
		assert.Equal(t, "blnk.transactions.dlt", duration[0].attributes["topic"])
		assert.Equal(t, "dead_letter", duration[0].attributes["attempt"])
	})

	t.Run("a failed attempt is still measured, under its own outcome", func(t *testing.T) {
		instruments := publisherCaptureInstruments(t)
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport().failProduceWith(kafka.LeaderNotAvailable))

		_, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       publisherEvent(model.EventTypeTransactionApplied, "txn_failed_label", publisherPayload),
			Key:         publisherLedgerID,
			Attempt:     1,
			MaxAttempts: 5,
		})
		require.Error(t, err)

		assert.Zero(t, instruments.published.total(),
			"no write was acknowledged, so no write is counted")

		attempts := instruments.attempts.snapshot()
		require.Len(t, attempts, 1)
		assert.Equal(t, string(model.PublishStatusRetrying), attempts[0].attributes["outcome"])

		duration := instruments.duration.snapshot()
		require.Len(t, duration, 1,
			"a broker that refuses a write is exactly when latency data matters, so the observation "+
				"is recorded rather than dropped")
		assert.Equal(t, "1", duration[0].attributes["attempt"])
		assert.Equal(t, string(model.PublishStatusRetrying), duration[0].attributes["outcome"],
			"and its outcome keeps it out of the success population the p99 is read from")
	})
}

// ---------------------------------------------------------------------------
// Concurrency: one writer, many publishers
// ---------------------------------------------------------------------------

// TestEventPublisher_ConcurrentPublishesShareOneWriterPerTopic is the property that makes
// sharing a publisher safe, and it is the reason this file must be run under -race.
//
// The relay publishes a claimed batch concurrently over the publisher it was handed, so several
// goroutines write through the SAME *kafka.Writer at once — which kafka-go explicitly supports
// and which is how throughput is reached without one writer per event. What has to hold under
// that load is not only that no publish is lost, but that the publisher's own bookkeeping is not
// racing: the writer map is read on every publish and written only on lazy growth, the
// retirement list is ordered, and the metric recorders are shared.
//
// So three things are asserted: every publish succeeds, every publish reaches the broker exactly
// once, and the writer inventory is UNCHANGED afterwards — no lazy growth, no duplicate writer
// for a topic two goroutines happened to publish to simultaneously. Several event types are used
// so more than one writer is exercised at a time.
func TestEventPublisher_ConcurrentPublishesShareOneWriterPerTopic(t *testing.T) {
	transport := newPublisherFakeTransport()
	publisher := publisherWithFakeTransport(t, transport)
	instruments := publisherCaptureInstruments(t)

	eventTypes := []string{
		model.EventTypeTransactionApplied,
		model.EventTypeTransactionQueued,
		publisherIdentityEventType,
		model.EventTypeBalanceMonitor,
		model.EventTypeSystemError,
	}

	const goroutines, perGoroutine = 8, 5

	var group sync.WaitGroup
	failures := make(chan error, goroutines*perGoroutine)

	for worker := 0; worker < goroutines; worker++ {
		group.Add(1)

		go func(worker int) {
			defer group.Done()

			for publish := 0; publish < perGoroutine; publish++ {
				eventType := eventTypes[(worker+publish)%len(eventTypes)]
				event := publisherEvent(
					eventType,
					fmt.Sprintf("agg_publisher_concurrent_%d_%d", worker, publish),
					publisherPayload,
				)

				if err := publisher.Publish(context.Background(), event); err != nil {
					failures <- err
				}
			}
		}(worker)
	}

	group.Wait()
	close(failures)

	for err := range failures {
		assert.NoError(t, err, "every concurrent publish must succeed over the shared writers")
	}

	assert.Len(t, transport.producedBatches(), goroutines*perGoroutine,
		"every publish must reach the broker exactly once: a lost one is a lost event and a "+
			"duplicated one is a double delivery")
	assert.EqualValues(t, goroutines*perGoroutine, instruments.attempts.total(),
		"and each must be counted once on the ATTEMPTS counter, which also exercises the shared "+
			"instruments under load. The published-events counter is deliberately not read here: "+
			"the publisher does not touch it, because a delivery is counted on the relay's durable "+
			"transition rather than at the acknowledgement")

	publisher.mu.RLock()
	writers := len(publisher.writers)
	lazy := len(publisher.lazyTopics)
	publisher.mu.RUnlock()

	assert.Equal(t, len(AllTopicsWithDeadLetters()), writers,
		"concurrent publishing must not grow the writer cache; every destination was pre-created")
	assert.Zero(t, lazy,
		"and none of it may be recorded as lazy growth, which would churn the retirement bookkeeping "+
			"on the hot path")
}

// ---------------------------------------------------------------------------------------
// Broker coordinate capture — OBS-02
// ---------------------------------------------------------------------------------------

// TestPublishAcknowledgement_CarriesTheBrokersCoordinateBackToTheCaller pins the correlation
// seam the zero-loss reconciliation depends on.
//
// # Why a per-message carrier and not a per-writer field
//
// kafka-go reports offsets through Writer.Completion, which is a property of the WRITER — and
// writers are pooled per topic and shared by every concurrent publish to that topic. A callback
// writing into publisher state could not tell which publish a message belonged to, and one batch
// legitimately carries messages from several. kafka.Message.WriterData is the library's own
// correlation seam: it rides with the message, comes back on the completion, and never reaches
// the wire.
//
// This test drives completeWrite exactly as kafka-go does — one call, a batch of messages from
// several different publishes — and requires each carrier to receive its own coordinate.
func TestPublishAcknowledgement_CarriesTheBrokersCoordinateBackToTheCaller(t *testing.T) {
	publisher := &kafkaPublisher{}

	first := &publishAcknowledgement{}
	second := &publishAcknowledgement{}
	third := &publishAcknowledgement{}

	// One batch, three publishes, all on the same partition — which is what a batch IS, and
	// therefore the case that must not cross its coordinates over.
	publisher.completeWrite([]kafka.Message{
		{Topic: "blnk.transactions", Partition: 4, Offset: 1_000, WriterData: first},
		{Topic: "blnk.transactions", Partition: 4, Offset: 1_001, WriterData: second},
		{Topic: "blnk.transactions", Partition: 4, Offset: 1_002, WriterData: third},
	}, nil)

	for index, acknowledgement := range []*publishAcknowledgement{first, second, third} {
		record, confirmed := acknowledgement.coordinate()
		require.True(t, confirmed, "carrier %d received no coordinate", index)
		assert.Equal(t, int64(1_000+index), record.Offset,
			"each publish must receive ITS OWN offset; a shared one would let two rows claim the "+
				"same record")
		assert.Equal(t, "blnk.transactions", record.Topic)
		assert.Equal(t, 4, record.Partition)
	}
}

// TestPublishAcknowledgement_CannotBeMadeToPanic is a hard requirement rather than defensive
// habit.
//
// kafka-go documents that a panic in a completion function TERMINATES THE PROGRAM, because the
// panic bubbles up a writer goroutine that nothing recovers. So the callback has to survive every
// shape of input the library or a caller could hand it — a nil carrier, a foreign WriterData, no
// WriterData at all, a nil slice — and a ledger process must not be brought down by a message
// somebody forgot to attach a carrier to.
func TestPublishAcknowledgement_CannotBeMadeToPanic(t *testing.T) {
	publisher := &kafkaPublisher{}

	assert.NotPanics(t, func() {
		publisher.completeWrite(nil, nil)
		publisher.completeWrite([]kafka.Message{}, nil)
		publisher.completeWrite([]kafka.Message{{Topic: "blnk.transactions"}}, nil)
		publisher.completeWrite([]kafka.Message{{WriterData: nil}}, nil)
		publisher.completeWrite([]kafka.Message{{WriterData: "not a carrier"}}, nil)
		publisher.completeWrite([]kafka.Message{{WriterData: (*publishAcknowledgement)(nil)}}, nil)
		publisher.completeWrite([]kafka.Message{{WriterData: &publishAcknowledgement{}}},
			errors.New("the batch failed"))
	})
}

// TestPublishAcknowledgement_RefusesACoordinateForAWriteThatDidNotLand keeps the mapping honest.
//
// A message the broker did not answer for carries no usable coordinate: kafka-go leaves the
// fields at their zero values and reports the error separately. Accepting that would manufacture
// "partition 0, offset 0" — a REAL location — for a write that never landed, and the audit would
// count the row as confirmed while an operator looking there found somebody else's event. Nothing
// is worse for a mechanism whose whole purpose is auditability.
func TestPublishAcknowledgement_RefusesACoordinateForAWriteThatDidNotLand(t *testing.T) {
	publisher := &kafkaPublisher{}

	for name, message := range map[string]kafka.Message{
		"no topic":            {Partition: 3, Offset: 91},
		"blank topic":         {Topic: "   ", Offset: 4},
		"negative offset":     {Topic: "blnk.transactions", Offset: -1},
		"nothing set at all":  {},
		"topic but no offset": {Topic: "blnk.transactions", Offset: -1, Partition: 2},
	} {
		t.Run(name, func(t *testing.T) {
			acknowledgement := &publishAcknowledgement{}
			publisher.completeWrite([]kafka.Message{
				{Topic: message.Topic, Partition: message.Partition, Offset: message.Offset,
					WriterData: acknowledgement},
			}, nil)

			_, confirmed := acknowledgement.coordinate()
			assert.False(t, confirmed,
				"a coordinate that cannot be looked up must not be recorded as one")
		})
	}

	t.Run("the first record on partition zero IS accepted", func(t *testing.T) {
		// The distinction the whole guard rests on: offset 0 on partition 0 is the first record
		// on a fresh partition, and refusing it would make the row that produced it count as an
		// unconfirmed publication forever.
		acknowledgement := &publishAcknowledgement{}
		publisher.completeWrite([]kafka.Message{
			{Topic: "blnk.system", Partition: 0, Offset: 0, WriterData: acknowledgement},
		}, nil)

		record, confirmed := acknowledgement.coordinate()
		require.True(t, confirmed)
		assert.Equal(t, "blnk.system/0@0", record.String())
	})
}

// TestPublishAcknowledgement_KeepsTheFirstCoordinateItIsGiven pins the idempotence.
//
// kafka-go calls Completion once per batch and a message belongs to exactly one batch, so a
// second call for the same carrier would mean an internal retry re-reporting the message. The
// first coordinate is the one the offset series was assigned from, so it is the one kept —
// overwriting would leave the row naming a record the caller was never told about.
func TestPublishAcknowledgement_KeepsTheFirstCoordinateItIsGiven(t *testing.T) {
	publisher := &kafkaPublisher{}
	acknowledgement := &publishAcknowledgement{}

	publisher.completeWrite([]kafka.Message{
		{Topic: "blnk.transactions", Partition: 1, Offset: 500, WriterData: acknowledgement},
	}, nil)
	publisher.completeWrite([]kafka.Message{
		{Topic: "blnk.transactions", Partition: 2, Offset: 999, WriterData: acknowledgement},
	}, nil)

	record, confirmed := acknowledgement.coordinate()
	require.True(t, confirmed)
	assert.Equal(t, "blnk.transactions/1@500", record.String())
}

// TestPublishAcknowledgement_IsSafeUnderConcurrentCompletionAndRead covers the memory model.
//
// Completion runs on the writer's own goroutines, so the write and the read genuinely cross
// goroutine boundaries. With Async false, WriteMessages blocks on Completion — which ORDERS them
// but does not by itself make the access race-free — so the carrier carries its own mutex, and
// this test is the one that fails under `-race` if it is ever removed.
func TestPublishAcknowledgement_IsSafeUnderConcurrentCompletionAndRead(t *testing.T) {
	publisher := &kafkaPublisher{}

	const carriers = 64

	acknowledgements := make([]*publishAcknowledgement, carriers)
	for i := range acknowledgements {
		acknowledgements[i] = &publishAcknowledgement{}
	}

	var wait sync.WaitGroup
	for i, acknowledgement := range acknowledgements {
		wait.Add(2)

		go func(index int, carrier *publishAcknowledgement) {
			defer wait.Done()
			publisher.completeWrite([]kafka.Message{
				{Topic: "blnk.transactions", Partition: index % 6, Offset: int64(index),
					WriterData: carrier},
			}, nil)
		}(i, acknowledgement)

		go func(carrier *publishAcknowledgement) {
			defer wait.Done()
			carrier.coordinate()
		}(acknowledgement)
	}

	wait.Wait()

	for index, acknowledgement := range acknowledgements {
		record, confirmed := acknowledgement.coordinate()
		require.True(t, confirmed, "carrier %d lost its coordinate", index)
		assert.Equal(t, int64(index), record.Offset)
	}
}

// TestNewWriter_InstallsTheCompletionCallback is the wiring assertion.
//
// Everything above tests the carrier in isolation, which proves nothing if the writers the
// publisher actually builds never call it: the coordinate would be absent for every event, every
// row would count as an unconfirmed publication, and the zero-loss reconciliation would report
// itself permanently inconclusive with no test failing.
func TestNewWriter_InstallsTheCompletionCallback(t *testing.T) {
	publisher := &kafkaPublisher{}
	writer := publisher.newWriter("blnk.transactions")

	require.NotNil(t, writer.Completion,
		"without the callback no event can ever name its record")
	assert.False(t, writer.Async,
		"Async must stay false, or WriteMessages would return before the completion runs and the "+
			"coordinate would not be available to the result")

	// Driven through the field the writer actually holds, so the assertion covers the wiring
	// rather than the method it happens to point at.
	acknowledgement := &publishAcknowledgement{}
	writer.Completion([]kafka.Message{
		{Topic: "blnk.transactions", Partition: 5, Offset: 77, WriterData: acknowledgement},
	}, nil)

	record, confirmed := acknowledgement.coordinate()
	require.True(t, confirmed)
	assert.Equal(t, "blnk.transactions/5@77", record.String())
}

// TestPublishToTopic_ClassifiesTheTwoWriterFailuresDifferently pins a distinction that only
// became load-bearing once the relay started ACTING on the classification.
//
// Both failures come from the same line — writerFor could not hand back a writer — and both are
// permanent for the ATTEMPT. They are opposite for the EVENT:
//
//   - A CLOSED PUBLISHER is the process shutting down. The next attempt, in this process after a
//     restart or in another replica right now, has a live transport and publishes the event
//     normally. Classifying it permanent was harmless while the relay retried everything within
//     budget; now that the relay dead-letters a permanent failure immediately, it would mean a
//     shutdown landing mid-batch dead-lettered perfectly deliverable events — the opposite of
//     what a graceful shutdown is for.
//   - A REFUSED TOPIC is permanent for the event itself. The destination is not one Blnk may
//     write to, so no retry and no broker state makes the write legitimate, and the event
//     belongs in the dead-letter inventory now rather than in five attempts' time.
func TestPublishToTopic_ClassifiesTheTwoWriterFailuresDifferently(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	event := publisherEvent(model.EventTypeTransactionApplied, "txn_writer_failure", publisherPayload)

	t.Run("a closed publisher is recoverable, so the event is not abandoned", func(t *testing.T) {
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())
		require.NoError(t, publisher.Close())

		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Topic:       TopicForEvent(event.EventType),
			Key:         publisherLedgerID,
			Attempt:     1,
			MaxAttempts: 5,
		})

		require.ErrorIs(t, err, ErrEventPublisherClosed)
		assert.True(t, result.Transient,
			"a shutdown says nothing about the event: another attempt in a live process publishes it")
		assert.Equal(t, model.PublishStatusRetrying, result.Status)
		assert.True(t, result.Retryable, "and the budget is untouched, so a retry is available")
		assert.False(t, result.PermanentFailure(),
			"the relay must NOT take this row to its terminal state: a shutdown mid-batch would "+
				"otherwise dead-letter every event the batch was still holding")
		assert.True(t, IsBrokerUnavailableError(err),
			"and the API boundary already answered 503 for this error, so the two now agree")
	})

	t.Run("a refused topic is permanent, so the event stops here", func(t *testing.T) {
		publisher := publisherWithFakeTransport(t, newPublisherFakeTransport())

		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Topic:       "attacker.transactions",
			Key:         publisherLedgerID,
			Attempt:     1,
			MaxAttempts: 5,
		})

		require.ErrorIs(t, err, ErrTopicNotOwned)
		assert.False(t, result.Transient,
			"a destination outside Blnk's namespace is not a condition another attempt can change")
		assert.Equal(t, model.PublishStatusRetrying, result.Status,
			"the status stays inside the three-value vocabulary; the permanence is on Classified")
		assert.True(t, result.Classified)
		assert.False(t, result.Retryable)
		assert.True(t, result.PermanentFailure(),
			"so the relay takes the row straight to its terminal state on this attempt")
	})
}

// TestPublishResult_PermanentFailureIsAffirmative covers the predicate the relay's retry
// decision reads, and the case it exists to be safe about.
//
// `!Retryable` and `!Transient` are both true on a result NOBODY POPULATED. The relay borrows
// whichever publisher the process built, so a bare error with an empty result has to read as
// "not permanent" — otherwise the first failure of any such implementation would dead-letter
// an event that a retry would have delivered. The predicate therefore requires all three
// facts, and the zero value is the row of the table that matters most.
//
// The third fact is PublishResult.Classified. It used to be `Status ==
// model.PublishStatusFailed`, which tied this safety check to a published metric label domain
// that requirement R-3 fixes at three values; the check is unchanged and only the field it
// reads moved. The "a failed attempt with no classification" row below is the one that pins
// the difference: it carries the same status a classified permanent failure now carries, and
// must still read as non-permanent.
func TestPublishResult_PermanentFailureIsAffirmative(t *testing.T) {
	cause := errors.New("publisher test: refused")

	for name, testCase := range map[string]struct {
		result PublishResult
		want   bool
	}{
		"a classified permanent failure": {
			result: PublishResult{Status: model.PublishStatusRetrying, Classified: true, Transient: false, Err: cause},
			want:   true,
		},
		"a transient failure that spent the budget": {
			result: PublishResult{Status: model.PublishStatusRetrying, Classified: true, Transient: true, Err: cause},
			want:   false,
		},
		"a failed attempt with no classification, which is the unsafe reading this guards": {
			// The SAME status a classified permanent failure carries, and the same
			// zero-valued Transient — so only the missing Classified marker separates the
			// two. Without that marker, an implementation returning a bare error and an
			// otherwise-empty result would have every failure dead-lettered on attempt one.
			result: PublishResult{Status: model.PublishStatusRetrying, Err: cause},
			want:   false,
		},
		"a retryable failure": {
			result: PublishResult{Status: model.PublishStatusRetrying, Classified: true, Transient: true, Err: cause},
			want:   false,
		},
		"a success": {
			result: PublishResult{Status: model.PublishStatusDispatched},
			want:   false,
		},
		"the zero value, which is what an unclassifying publisher produces": {
			result: PublishResult{},
			want:   false,
		},
		"a bare error with no status": {
			result: PublishResult{Err: cause},
			want:   false,
		},
		"a dead-letter write's own result": {
			result: PublishResult{Status: model.PublishStatusDeadLettered, Err: cause},
			want:   false,
		},
		"a classification with no error, which states nothing about a failure": {
			result: PublishResult{Status: model.PublishStatusRetrying, Classified: true, Transient: false},
			want:   false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, testCase.want, testCase.result.PermanentFailure())
		})
	}
}

// TestIsPermanentPublishError_IsAffirmativeAndNotTheNegationOfTransient pins the property that
// makes the relay's terminal decision safe: an error nothing classified is NEITHER transient
// nor permanent.
//
// The two predicates are deliberately not exhaustive, and the third state is the one every
// unrecognised failure occupies. Asserting the negation relationship is exactly what this test
// must NOT do — `!IsTransientPublishError(err)` is true for a bare error, and reading that as
// permanence is what dead-lettered events on their first attempt with four attempts unspent.
// So the bare-error rows below assert false on BOTH predicates, which is the invariant a future
// refactor would have to break in order to reintroduce the defect.
func TestIsPermanentPublishError_IsAffirmativeAndNotTheNegationOfTransient(t *testing.T) {
	cause := errors.New("publisher test: the underlying failure")

	for name, testCase := range map[string]struct {
		err           error
		wantPermanent bool
		wantTransient bool
	}{
		"a PublishError classified as permanent": {
			err:           &PublishError{Topic: "blnk.transactions", Err: cause, Transient: false},
			wantPermanent: true,
			wantTransient: false,
		},
		"a PublishError classified as transient": {
			err:           &PublishError{Topic: "blnk.transactions", Err: cause, Transient: true},
			wantPermanent: false,
			wantTransient: true,
		},
		"a permanent PublishError reached through a wrapper": {
			err: fmt.Errorf("relaying: %w",
				&PublishError{Topic: "blnk.balances", Err: cause, Transient: false}),
			wantPermanent: true,
			wantTransient: false,
		},
		"a bare error this pipeline never classified": {
			err:           cause,
			wantPermanent: false,
			wantTransient: false,
		},
		"a wrapped bare error": {
			err:           fmt.Errorf("relaying: %w", cause),
			wantPermanent: false,
			wantTransient: false,
		},
		"a context deadline that did not come through the publish path": {
			err:           context.DeadlineExceeded,
			wantPermanent: false,
			wantTransient: false,
		},
		"no error at all": {
			err:           nil,
			wantPermanent: false,
			wantTransient: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, testCase.wantPermanent, IsPermanentPublishError(testCase.err),
				"permanence must be affirmative: only a PublishError classified as "+
					"non-recoverable reports true")
			assert.Equal(t, testCase.wantTransient, IsTransientPublishError(testCase.err))

			if !testCase.wantPermanent && !testCase.wantTransient && testCase.err != nil {
				assert.False(t,
					IsPermanentPublishError(testCase.err) == !IsTransientPublishError(testCase.err),
					"an unclassified error must be neither, so the two predicates must NOT be "+
						"each other's negation; treating them as such is what dead-letters a "+
						"retryable event on its first attempt")
			}
		})
	}
}

// KAFKA_INSECURE_LOCAL_DEV asserts that the broker is a local development broker.
// Nothing used to check that, so the flag disabled encryption to ANY broker — and the
// local stack defaulted it on, which meant repointing KAFKA_BROKERS at a remote host
// was enough to put ledger amounts, identity records and the SASL/SCRAM handshake on
// the public internet in cleartext, with a log warning as the only trace.
//
// The local cases matter as much as the remote ones here. A guard that refused the
// values the local stack actually uses would be reverted within a day, so
// "kafka:29092" — a Compose service name, and the same single-label shape an in-cluster
// Kubernetes Service has — is pinned alongside the loopback forms.
func TestRequireLocalBrokersForPlaintext_ScopesTheAcknowledgementToLocalBrokers(t *testing.T) {
	t.Parallel()

	t.Run("local brokers are permitted", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name    string
			brokers []string
		}{
			{"the host-side loopback name the .env uses", []string{"localhost:9092"}},
			{"an IPv4 loopback literal", []string{"127.0.0.1:9092"}},
			{"an IPv6 loopback literal", []string{"[::1]:9092"}},
			{"an IPv4-mapped loopback literal", []string{"[::ffff:127.0.0.1]:9092"}},
			{"the Compose service name", []string{"kafka:29092"}},
			{"a bare host with no port", []string{"kafka"}},
			{"an .internal zone name", []string{"kafka.internal:9092"}},
			{"an mDNS .local name", []string{"broker.local:9092"}},
			{"several local brokers", []string{"localhost:9092", "kafka:29092"}},
			{"no brokers at all", nil},
			{"a blank entry", []string{"  "}},
		} {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				assert.NoError(t, requireLocalBrokersForPlaintext(tc.brokers),
					"%v is local, so the acknowledgement covers it; refusing it would break "+
						"the local stack and get this guard reverted", tc.brokers)
			})
		}
	})

	t.Run("remote brokers are refused", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name    string
			brokers []string
		}{
			{"a public FQDN", []string{"kafka.example.com:9092"}},
			{"a public IPv4 literal", []string{"203.0.113.10:9092"}},
			{"a managed-Kafka endpoint", []string{"b-1.msk.us-east-1.amazonaws.com:9096"}},
			{"one remote among locals", []string{"localhost:9092", "kafka.example.com:9092"}},
		} {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				err := requireLocalBrokersForPlaintext(tc.brokers)
				require.Error(t, err,
					"%v is not local, so plaintext must be refused rather than warned about",
					tc.brokers)

				// The message has to name the offending broker: an operator reading
				// "plaintext refused" with a multi-broker list has no way to tell which
				// entry is the problem.
				assert.Contains(t, err.Error(), "KAFKA_INSECURE_LOCAL_DEV",
					"the refusal must name the setting whose claim was not met")
				assert.Contains(t, err.Error(), "KAFKA_TLS_ENABLED",
					"the refusal must name the remedy, not only the problem")
			})
		}
	})
}

// ---------------------------------------------------------------------------
// RETRY-01: the retry verdict and the broker-unavailability verdict are ONE verdict
// ---------------------------------------------------------------------------

// publisherBrokerOutages are the failure shapes that mean THE BROKER could not take the write.
//
// Each one is the real value a client produces, not a message that reads like it: a resolution
// failure is a *net.DNSError, a refused or unreachable dial is a *net.OpError, and a broker
// declaring itself unable to serve a partition is a kafka.Error code. Asserting over the values
// is what makes these tests a contract about classification rather than about error text.
func publisherBrokerOutages() map[string]error {
	return map[string]error{
		// THE FINDING. A broker whose name stops resolving is the ordinary shape of a container
		// or pod replacement, a Service recreation, or a resolver restart. It carries no errno,
		// is not Temporary and is not a Timeout, so every rule that predates this map reported it
		// permanent — and the relay dead-lettered the event on the spot with four attempts unspent.
		"the broker name does not resolve": &net.DNSError{
			Err: "no such host", Name: "kafka.internal", IsNotFound: true,
		},
		"the resolver itself is unreachable": &net.DNSError{
			Err: "server misbehaving", Name: "kafka.internal", IsTemporary: false,
		},
		"the dial was refused": &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
			Err:  syscall.ECONNREFUSED,
		},
		"the host is unreachable": &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.ParseIP("10.9.8.7"), Port: 9092},
			Err:  syscall.EHOSTUNREACH,
		},
		"the broker declares itself unavailable": kafka.BrokerNotAvailable,
		"a replica is not available":             kafka.ReplicaNotAvailable,
		"a batch member could not be resolved": kafka.WriteErrors{
			&net.DNSError{Err: "no such host", Name: "kafka.internal", IsNotFound: true},
		},
		"a resolution failure reached through a wrapper": fmt.Errorf(
			"kafka.(*Client).Produce: %w",
			&net.DNSError{Err: "no such host", Name: "kafka.internal", IsNotFound: true},
		),
	}
}

// TestClassifyTransientPublishError_KeepsTheBudgetForEveryBrokerOutage is RETRY-01.
//
// # The defect this pins closed
//
// Two functions answered two questions that have one answer, and they disagreed.
// classifyTransientPublishError decided whether another attempt was worth making;
// brokerUnavailable decided whether the broker was the reason. The second recognised
// *net.DNSError and *net.OpError and the first did not, so a broker whose name stopped
// resolving produced ONE error that was reported terminal — skipping the whole retry budget and
// dead-lettering a live ledger event immediately — while the very next log line classified it
// failure_class=broker_unavailable. An ordinary rolling restart therefore converted every event
// in flight into a manual replay, breaching requirement R-4's bounded five-attempt budget and
// acceptance criterion V-3's 0.1% dead-letter budget during planned maintenance.
//
// The verdicts are one predicate now, and the assertion below is the invariant that states it:
// a failure the pipeline calls broker_unavailable is ALWAYS retryable while budget remains.
func TestClassifyTransientPublishError_KeepsTheBudgetForEveryBrokerOutage(t *testing.T) {
	for name, cause := range publisherBrokerOutages() {
		t.Run(name, func(t *testing.T) {
			assert.True(t, classifyTransientPublishError(cause),
				"a broker that cannot take the write right now is the archetypal recoverable "+
					"failure; reporting it permanent spends none of the budget and dead-letters "+
					"a deliverable event")

			assert.True(t, brokerUnavailable(cause),
				"and the broker-unavailability verdict must agree, because it is the same verdict")

			assert.True(t, IsBrokerUnavailableError(cause),
				"the exported form the API boundary reads must agree too, or one response says "+
					"503-retry-this while the pipeline has already given up")
		})
	}
}

// TestClassifyTransientPublishError_StillRefusesToRetryWhatCannotSucceed is the other half of
// RETRY-01, and it is the half that keeps the widening honest.
//
// Widening a retry classification is only safe if it widened by the failures a later attempt
// can succeed at and nothing else. An unrecognised error, an authorisation refusal and an
// oversized record are all conditions no amount of waiting changes, and each must still be
// reported permanent so the event reaches the dead-letter inventory an operator can see rather
// than spending five attempts first.
func TestClassifyTransientPublishError_StillRefusesToRetryWhatCannotSucceed(t *testing.T) {
	for name, cause := range map[string]error{
		"a bare error this pipeline never recognised": errors.New("publisher test: unknown failure"),
		"the principal may not write to the topic":    kafka.TopicAuthorizationFailed,
		"the record is larger than the broker allows": kafka.MessageSizeTooLarge,
		"the event exceeded Blnk's own size ceiling":  ErrEventMessageTooLarge,
		"the destination is not a Blnk topic":         ErrTopicNotOwned,
		"an empty batch of write errors": kafka.WriteErrors{
			errors.New("publisher test: unknown batch failure"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, classifyTransientPublishError(cause),
				"a condition no later attempt can change must not consume the retry budget")
			assert.False(t, brokerUnavailable(cause),
				"and it is not the broker's fault either, so the two verdicts still agree")
		})
	}
}

// TestPublishToTopic_SpendsTheBudgetWhenTheBrokerNameStopsResolving carries RETRY-01 through the
// PUBLISH PATH rather than asserting the classifier in isolation.
//
// The classifier's verdict only matters because of what it becomes: PublishError.Transient,
// PublishResult.Retryable, PublishResult.Terminal and PermanentFailure — the field the relay
// actually branches on. A fix that corrected the predicate and left any one of those reading the
// old way would leave the event dead-lettered exactly as before, so each is asserted here.
func TestPublishToTopic_SpendsTheBudgetWhenTheBrokerNameStopsResolving(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	unresolvable := &net.DNSError{Err: "no such host", Name: "kafka.internal", IsNotFound: true}
	event := publisherEvent(model.EventTypeTransactionApplied, "txn_dns_failure", publisherPayload)

	t.Run("with budget remaining the event is retried, not abandoned", func(t *testing.T) {
		transport := newPublisherFakeTransport()
		transport.transportErr = unresolvable
		publisher := publisherWithFakeTransport(t, transport)

		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Topic:       TopicForEvent(event.EventType),
			Key:         publisherLedgerID,
			Attempt:     2,
			MaxAttempts: 5,
		})

		require.Error(t, err)
		assert.True(t, result.Transient,
			"a name that does not resolve says nothing about the event: the next attempt publishes it")
		assert.True(t, result.Retryable, "attempt 2 of 5 leaves budget, so another attempt is available")
		assert.False(t, result.Terminal,
			"and the attempt is not terminal, which is what the `terminal` metric attribute and the "+
				"log line report")
		assert.False(t, result.PermanentFailure(),
			"the relay reads this field, and true here is the dead-lettering that must no longer happen")

		assert.False(t, IsPermanentPublishError(err),
			"nor may the error alone report permanence, because the relay's terminal branch reads it")
		assert.True(t, IsTransientPublishError(err))
		assert.True(t, IsBrokerUnavailableError(err),
			"the API boundary answers 503 for this error, so the two verdicts must not contradict")

		var publishErr *PublishError
		require.ErrorAs(t, err, &publishErr)
		assert.True(t, publishErr.Transient)

		// AND THE BATCH IS WHY THE CLASSIFIER HAS TO RECURSE BY HAND. kafka-go returns the
		// failure as a kafka.WriteErrors slice, which exposes no Unwrap — so errors.As can reach
		// the slice and cannot see the *net.DNSError inside it. A classifier written with
		// errors.As alone therefore never sees the resolution failure at all, which is half of how
		// the two verdicts came to disagree.
		var writeErrors kafka.WriteErrors
		require.ErrorAs(t, err, &writeErrors,
			"the library's own error must remain reachable through the wrapper")
		require.Equal(t, 1, writeErrors.Count())

		var dnsErr *net.DNSError
		assert.False(t, errors.As(err, &dnsErr),
			"errors.As cannot see through kafka.WriteErrors, which is exactly why the "+
				"classification inspects its members directly")
		assert.True(t, errors.As(writeErrors[0], &dnsErr),
			"and the member IS the resolution failure, so the recursion is reading a real value")
	})

	t.Run("on the last permitted attempt it is terminal because the BUDGET is spent", func(t *testing.T) {
		// The distinction the finding turned on. Terminal is legitimate here — five of five —
		// and it must be reached by spending the budget rather than by skipping it.
		transport := newPublisherFakeTransport()
		transport.transportErr = unresolvable
		publisher := publisherWithFakeTransport(t, transport)

		result, err := publisher.PublishToTopic(context.Background(), PublishRequest{
			Event:       event,
			Topic:       TopicForEvent(event.EventType),
			Key:         publisherLedgerID,
			Attempt:     5,
			MaxAttempts: 5,
		})

		require.Error(t, err)
		assert.True(t, result.Transient, "the classification is unchanged by which attempt this was")
		assert.False(t, result.Retryable, "there is no sixth attempt")
		assert.True(t, result.Terminal,
			"so the attempt is reported terminal, which is what the `terminal` metric attribute and "+
				"the dead-letter triage query read")

		// AND STILL NOT A PERMANENT FAILURE, which is the distinction the fix turns on.
		// PermanentFailure covers only the reasons the database cannot see; budget exhaustion is
		// decided inside MarkEventFailed's UPDATE so that two instances racing on one row cannot
		// both conclude they were last. A DNS failure reaching the dead-letter topic therefore
		// gets there by SPENDING the budget, never by skipping it.
		assert.False(t, result.PermanentFailure(),
			"a transient failure on the last attempt is terminal because the BUDGET ran out, and "+
				"that decision belongs to the database, not to this predicate")
	})
}

// TestKafkaLogger_RedactsBrokerTopologyAtANormalLevel is DATA-02 on the one path whose text is
// not this codebase's own.
//
// kafka-go writes its failures for a developer at a terminal, and this hook is installed at
// ERROR level, so forwarding the string unchanged published the broker's address and port — and,
// for a resolution failure, the internal resolver's address — into every deployment's log at the
// default level. docs/kafka-operations.md publishes the opposite policy in a table: redacted in
// a line at info, warn or error; verbatim only at debug, and in the row's own columns.
//
// Both halves are asserted, because either alone is a different defect: redacting without a
// debug escape hatch lengthens an outage by withholding the address a broker investigation needs,
// and the escape hatch without redaction is the leak itself.
func TestKafkaLogger_RedactsBrokerTopologyAtANormalLevel(t *testing.T) {
	// The exact shape kafka-go produces, both failures the fault-injection runs observed.
	const dialFailure = "kafka.(*Client).Produce: dial tcp 10.0.3.14:9092: connect: connection refused"
	const resolveFailure = "kafka.(*Client).Produce: dial tcp: lookup kafka on 127.0.0.11:53: no such host"

	t.Run("at error level the addresses are gone and the diagnosis survives", func(t *testing.T) {
		relayPinLogLevel(t, logrus.InfoLevel)
		hook := logtest.NewGlobal()

		kafkaLogger{level: logrus.ErrorLevel, topic: "blnk.transactions"}.Printf("%s", dialFailure)
		kafkaLogger{level: logrus.ErrorLevel, topic: "blnk.identities"}.Printf("%s", resolveFailure)

		entries := hook.AllEntries()
		require.Len(t, entries, 2, "both failures must still be reported; redaction is not suppression")

		for _, entry := range entries {
			assert.NotContains(t, entry.Message, "10.0.3.14",
				"the broker's address must not reach a log an aggregator ships onwards")
			assert.NotContains(t, entry.Message, "9092",
				"nor its port, which is the other half of the endpoint")
			assert.NotContains(t, entry.Message, "127.0.0.11",
				"nor the internal resolver's address, which a resolution failure names")
			assert.NotContains(t, entry.Message, "lookup kafka on",
				"and the resolver clause must not survive with its address merely reformatted")
			assert.NotContains(t, entry.Data, "message_verbatim",
				"the verbatim text is a debug-only companion, so it must be absent at info, "+
					"which is the level this line is emitted at")

			assert.Equal(t, "kafka-writer", entry.Data["component"],
				"the line must stay attributable to the writer that produced it")
			assert.NotEmpty(t, entry.Data["topic"],
				"and to the topic, which is the field that replaces the address as the locator")
		}

		assert.Contains(t, entries[0].Message, "connection refused",
			"the diagnosis is what an operator acts on and must survive redaction")
		assert.Contains(t, entries[1].Message, "no such host",
			"the same for a resolution failure: the class of failure is still legible")
	})

	t.Run("at debug the verbatim text is reachable, which is what makes redaction acceptable", func(t *testing.T) {
		relayPinLogLevel(t, logrus.DebugLevel)
		hook := logtest.NewGlobal()

		kafkaLogger{level: logrus.ErrorLevel, topic: "blnk.transactions"}.Printf("%s", dialFailure)

		entries := hook.AllEntries()
		require.Len(t, entries, 1)

		verbatim, ok := entries[0].Data["message_verbatim"].(string)
		require.True(t, ok,
			"an operator who set BLNK_LOG_LEVEL=debug has asked for exactly this detail")
		assert.Contains(t, verbatim, "10.0.3.14:9092",
			"and the address is the most useful part of a broker investigation")
		assert.NotContains(t, entries[0].Message, "10.0.3.14",
			"while the message itself stays redacted, so one grep still finds no topology")
	})

	t.Run("the debug hook is forwarded verbatim, because debug is itself the consent", func(t *testing.T) {
		relayPinLogLevel(t, logrus.DebugLevel)
		hook := logtest.NewGlobal()

		kafkaLogger{level: logrus.DebugLevel, topic: "blnk.balances"}.Printf(
			"writing %d messages to %s", 3, "10.0.3.14:9092")

		entries := hook.AllEntries()
		require.Len(t, entries, 1)
		assert.Contains(t, entries[0].Message, "10.0.3.14:9092",
			"the chatty hook is only ever emitted when a deployment asked for debug")
	})
}
