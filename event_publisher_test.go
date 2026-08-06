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

// Tests for the event publisher's PROCESS-WIDE LIFECYCLE.
//
// # What this file defends
//
// A publisher is eight Kafka writers over one shared transport with one SASL session per
// broker. Constructing it performs no I/O at all — kafka.TCP only canonicalises address
// strings and a SCRAM mechanism is pure computation — but the FIRST WRITE on each writer
// dials a broker and negotiates SASL. That asymmetry is the whole subject here: a
// publisher is cheap to build and expensive to use for the first time, so building one per
// request is not a minor inefficiency but continuous connection and SASL churn against the
// broker, with a fresh principal session per request.
//
// The properties asserted are therefore about IDENTITY and OWNERSHIP rather than about
// publishing:
//
//   - One instance is reused across callers, because reuse is the entire point.
//   - A configuration change rebuilds it, because a rotated SASL credential must take
//     effect; a publisher still holding the old one authenticates as nobody, and every
//     publish through it fails for a reason that looks nothing like the real cause.
//   - The cache key never carries a credential in the clear, matching the repository's
//     posture of keeping non-reversible references rather than secrets.
//   - An INJECTED publisher is never closed or replaced by this package. The relay's
//     publisher outlives anything that borrows it, and closing it would take a live
//     transport down while every later publish failed on a closed writer.
//
// None of these tests contact a broker, and none can: every publisher resolved here is
// either the no-op implementation or a Kafka publisher whose writers are never written to.

package blnk

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
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
// Building a publisher costs no I/O, but the first write on each of its eight writers dials
// a broker and negotiates SASL. A caller that builds one per request therefore pays
// connection setup and a SASL handshake per request and then tears it all down — which under
// concurrent replays is continuous churn, with the broker opening a fresh principal session
// every time. Handing back the same instance is what removes that, so identity is the
// property asserted, not merely "a publisher is returned".
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
// Under-covering is the dangerous direction: a field omitted from the fingerprint is a change
// that silently keeps the old publisher, and the SASL secret is the case where that is a
// production incident rather than an inconvenience. Over-covering merely rebuilds more often
// than necessary, so the equality cases are asserted too.
func TestSharedEventPublisher_FingerprintCoversEveryFieldThatChangesThePublisher(t *testing.T) {
	base := &config.Configuration{Kafka: config.KafkaConfig{
		Brokers:         []string{"broker-1:9092"},
		TopicPrefix:     "blnk",
		SASLAdminUser:   "admin",
		SASLAdminSecret: "secret",
	}}
	baseline := eventPublisherFingerprint(base)

	t.Run("differs on", func(t *testing.T) {
		for name, mutate := range map[string]func(*config.KafkaConfig){
			"the broker list":  func(k *config.KafkaConfig) { k.Brokers = []string{"broker-2:9092"} },
			"an added broker":  func(k *config.KafkaConfig) { k.Brokers = []string{"broker-1:9092", "broker-2:9092"} },
			"the topic prefix": func(k *config.KafkaConfig) { k.TopicPrefix = "acme" },
			"the SASL user":    func(k *config.KafkaConfig) { k.SASLAdminUser = "other" },
			"the SASL secret":  func(k *config.KafkaConfig) { k.SASLAdminSecret = "rotated" },
		} {
			t.Run(name, func(t *testing.T) {
				changed := &config.Configuration{Kafka: base.Kafka}
				mutate(&changed.Kafka)

				assert.NotEqual(t, baseline, eventPublisherFingerprint(changed),
					"a change to %s must produce a different publisher", name)
			})
		}
	})

	t.Run("is stable for an equivalent configuration", func(t *testing.T) {
		same := &config.Configuration{Kafka: base.Kafka}
		assert.Equal(t, baseline, eventPublisherFingerprint(same),
			"an equivalent configuration must reuse the publisher rather than rebuild it")
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

	t.Run("never carries the secret in the clear", func(t *testing.T) {
		assert.NotContains(t, baseline, "secret",
			"the fingerprint is held in memory and may be printed in a diagnostic; it must be a non-reversible digest, never the credential")
		assert.NotContains(t, baseline, "admin")
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
	// quarantine category was introduced for blank and uncatalogued event types.
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
// Lazy growth exists for one reason — a topic-prefix change, and stored rows from before
// one — so every name it legitimately has to accept has the owned shape. A name that does
// not is not a topic Blnk has ever published to, and caching a writer for it would hold
// connections open for a destination that cannot be right while letting the cache grow with
// whatever names happened to arrive.
//
// The assertion that matters is the second one in each case: the writer must NOT have been
// cached. Returning an error while still caching would leave the growth unbounded and the
// TestWriterFor_KeepsServingTheConstructedInventoryAfterAPrefixChange is the
// event-is-never-stranded property, expressed through the mechanism that actually delivers
// it.
//
// An outbox row records its destination topic at INSERT time, so a row written before
// KAFKA_TOPIC_PREFIX changed still names the previous generation's topic. That row must stay
// publishable — a committed event the relay can never publish would sit in the outbox for
// ever, and nothing would fail to say so.
//
// What makes it publishable is the PRE-CREATED inventory: the publisher builds a writer for
// every topic of the prefix it was constructed with, so the historical name is served by the
// map's fast path and never reaches the ownership check. Lazy growth then covers the other
// direction — the NEW prefix's topics, which the process has never built a writer for.
//
// A generation that predates this process is a different case and is deliberately REFUSED:
// admitting any '<something>.<category>' name would also admit another system's topic at the
// one point where a topic name becomes an outbound connection. The remedy is an operator
// action — restore the prefix, or drain the old topics — not a silent admission.
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
// A level guard cannot be observed from behaviour, so this asserts on the SOURCE: the call
// must sit inside an IsLevelEnabled check. The complementary assertion matters just as much
// — the FAILURE log must NOT be guarded, because requirement R-4 mandates the attempt
// number and error reason on every attempt.
func TestPublishSuccessLog_IsGuardedByTheDebugLevel(t *testing.T) {
	source, err := os.ReadFile("event_publisher.go")
	require.NoError(t, err)

	body := string(source)

	successCall := `logrus.WithFields(result.LogFields()).Debug("ledger event published to kafka")`
	require.Contains(t, body, successCall,
		"the success log line must still exist; the fix is to guard it, not to remove the observability")

	guard := "if logrus.IsLevelEnabled(logrus.DebugLevel) {"
	guardAt := strings.Index(body, guard)
	require.Positive(t, guardAt, "the success debug log must be guarded by a level check")

	callAt := strings.Index(body, successCall)
	require.Greater(t, callAt, guardAt,
		"the guard must PRECEDE the success log call, or it guards something else entirely")
	assert.Less(t, callAt-guardAt, 120,
		"the guard must immediately enclose the success log call rather than sit somewhere earlier in the file")

	// And the per-attempt failure log is deliberately NOT guarded.
	failureCall := `logrus.WithFields(result.LogFields()).Error("publishing a ledger event to kafka failed")`
	require.Contains(t, body, failureCall)
	assert.NotContains(t, body, "if logrus.IsLevelEnabled(logrus.ErrorLevel)",
		"requirement R-4 mandates the attempt number and error reason on EVERY attempt, so the failure log must never be level-guarded")
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

	first := result.LogFields()
	assert.NotSame(t, &fields, &first, "each call allocates a fresh map, which is precisely why it must not be called when Debug is off")
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
// exactly the kind of value that goes stale silently. It was 16 when Blnk owned four
// categories, and adding a fifth made 16 less than two generations with nothing anywhere
// failing: lazy writers would then be retired while a previous generation was still in use,
// costing a reconnection per message rather than per topic.
func TestMaxLazyTopicWriters_IsTwoPrefixGenerations(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	generation := len(AllTopicsWithDeadLetters())
	require.Equal(t, len(model.AllEventCategories())*2, generation,
		"a generation is one topic and one dead-letter sibling per category")

	assert.Equal(t, generation*2, maxLazyTopicWriters,
		"the bound must be TWO generations' worth: one for the previous prefix whose rows are "+
			"still arriving, and one for a further change on top of it")
}
