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
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every assertion in this file compares against an EXACT expected string, never merely
// against "something non-empty". Topic naming has no runtime failure mode: a wrong name
// is a perfectly valid Kafka topic that simply nobody reads, so a test that only checks
// for a non-empty result would pass while every event went nowhere. Exact expectations
// are also what makes the file resistant to mutation testing, which plants one-line
// changes — a flipped separator, a dropped suffix, a swapped category — that a
// non-empty check cannot detect.
//
// The event catalogue below is written out LONGHAND on purpose. Deriving the
// expectations from the table under test, or from model.EventCategory, would make the
// test agree with any table, including one that had quietly lost an entry. Spelling all
// thirteen event strings and all four topic names by hand is what makes this file a
// second, independent statement of the routing contract.

// storeKafkaTopicPrefix publishes a configuration carrying prefix as
// config.Kafka.TopicPrefix, restoring whatever configuration was in place when the test
// finishes.
//
// It writes to config.ConfigStore directly rather than through config.MockConfig, which
// is the same approach the sunset and legacy webhook tests take, and for the same
// reasons: MockConfig runs validateAndAddDefaults, which refuses to store a
// configuration without a data-source and Redis DSN (the test's value would be silently
// dropped) and which would also apply the topic-prefix default itself — defeating the
// point of a test that has to observe an unset prefix.
//
// That is not a workaround, it is the condition under test. Storing directly reproduces
// exactly the situation the production code has to survive: a configuration that never
// passed through the defaulting path, and therefore carries an empty prefix.
func storeKafkaTopicPrefix(t *testing.T, prefix string) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. Leaving the test's prefix in place
		// could influence later tests, so publish an empty configuration — strictly
		// closer to the original state than a configured prefix.
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(&config.Configuration{
		Kafka: config.KafkaConfig{TopicPrefix: prefix},
	})
}

// withUnloadedConfiguration makes the configuration read fail for the duration of one
// test, exercising the "configuration has not been loaded" branch of prefix resolution.
//
// The branch is otherwise unreachable in a test binary: config.ConfigStore is an
// atomic.Value, and once any earlier test has published a configuration it can never be
// emptied again. Swapping the package's configuration seam is the only way to reach it,
// and the seam exists for precisely that reason.
func withUnloadedConfiguration(t *testing.T) {
	t.Helper()

	original := fetchConfiguration
	t.Cleanup(func() { fetchConfiguration = original })

	fetchConfiguration = func() (*config.Configuration, error) {
		return nil, errors.New("config not loaded")
	}
}

// parseEventTopicsSource parses event_topics.go into an AST so that structural
// properties of the implementation can be asserted rather than merely documented.
//
// Comments are parsed too, because one of the properties asserted below is that a
// specific comment block still exists.
func parseEventTopicsSource(t *testing.T) *ast.File {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), "event_topics.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments)
	require.NoError(t, err, "event_topics.go must be parseable to assert its structure")

	return parsed
}

// TestTopicForEvent_RoutesEveryEmittedEventString pins the routing of all thirteen event
// strings Blnk emits to their exact topic names.
//
// This is the coverage requirement made executable: every event type that used to reach
// the legacy webhook sender must reach Kafka, with zero exceptions. The thirteen strings
// and the four topics are spelled out here independently of the implementation, so a
// table that lost an entry, or a category that was renamed, fails this test rather than
// silently agreeing with itself.
func TestTopicForEvent_RoutesEveryEmittedEventString(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	// The seven transaction lifecycle events. All seven originate in
	// getEventFromStatus, which is the transaction event-string vocabulary and which
	// relocates into event_topics.go at sunset.
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.queued"))
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.applied"))
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.scheduled"))
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.inflight"))
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.void"))
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.rejected"))
	// transaction.unknown is reachable, not hypothetical: the COMMIT status has no case
	// in getEventFromStatus and falls through to it. That behaviour is preserved
	// deliberately so the dual-delivery payload comparison stays exact, so the event
	// name must route like any other.
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.unknown"))

	// The eighth transaction event is composed at runtime from the batch status, and is
	// covered exhaustively by the prefix test below.
	assert.Equal(t, "blnk.transactions", TopicForEvent("bulk_transaction.applied"))

	// The two balance events.
	assert.Equal(t, "blnk.balances", TopicForEvent("balance.created"))
	assert.Equal(t, "blnk.balances", TopicForEvent("balance.monitor"))

	// The single identity event.
	assert.Equal(t, "blnk.identities", TopicForEvent("identity.created"))

	// The two events that belong to none of the three requirement-named categories, and
	// which are the entire reason the fourth category exists. If either of these ever
	// resolves to blnk.transactions, blnk.balances or blnk.identities, the fourth
	// category has been "simplified away" and a subscriber filtering that topic is now
	// receiving events it never subscribed to.
	assert.Equal(t, "blnk.system", TopicForEvent("ledger.created"))
	assert.Equal(t, "blnk.system", TopicForEvent("system.error"))
}

// TestTopicForEvent_DelegatesTheMappingToModel enforces the single-source-of-truth
// split: model.EventCategory owns the event-type mapping and this file owns the naming.
//
// The composition is asserted from the outside — prefix, separator, category — for every
// emitted event string. If a second mapping table were ever introduced into
// event_topics.go, it would only have to disagree with model.EventCategory on one event
// for this test to fail, which is the earliest possible detection of a divergence that
// otherwise shows up as missing events on a subscriber's topic.
func TestTopicForEvent_DelegatesTheMappingToModel(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, eventType := range []string{
		"transaction.queued",
		"transaction.applied",
		"transaction.scheduled",
		"transaction.inflight",
		"transaction.void",
		"transaction.rejected",
		"transaction.unknown",
		"bulk_transaction.failed",
		"balance.created",
		"balance.monitor",
		"identity.created",
		"ledger.created",
		"system.error",
	} {
		expected := "blnk" + "." + model.EventCategory(eventType)
		assert.Equal(t, expected, TopicForEvent(eventType),
			"the topic for %q must be the configured prefix joined to the category model.EventCategory resolves; a second mapping table in event_topics.go would break this", eventType)
	}
}

// TestTopicForEvent_BulkTransactionMatchesByPrefix proves bulk transaction events are
// matched by PREFIX and not by exact equality.
//
// Their names are composed at runtime as "bulk_transaction." + batch status, so the
// suffix set is open-ended. An exact-match-only table would route every one of them to
// the catch-all system topic — no error, no log line, just transaction events arriving
// on the wrong topic. Statuses that exist today and statuses that do not are both
// asserted, because the whole point of prefix matching is that tomorrow's status needs
// no code change.
func TestTopicForEvent_BulkTransactionMatchesByPrefix(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, status := range []string{
		// Emitted today.
		"failed",
		"applied",
		"inflight",
		// Not emitted today. These are the proof of prefix matching: an exact-match
		// table cannot route a status it has never been told about.
		"teleported",
		"partially_applied",
		"QUEUED",
		"9",
	} {
		eventType := "bulk_transaction." + status
		assert.Equal(t, "blnk.transactions", TopicForEvent(eventType),
			"every bulk_transaction.<status> must route to the transactions topic, including status %q which no producer emits today", status)
		assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent(eventType),
			"and its dead-letter sibling must follow the same route for status %q", status)
	}

	// The bare prefix with an empty status still satisfies the prefix rule. Requiring a
	// non-empty suffix would be a stricter rule than the producer guarantees.
	assert.Equal(t, "blnk.transactions", TopicForEvent("bulk_transaction."))

	// Without the trailing separator it is not an emitted event string, so it must NOT
	// satisfy the prefix rule. This is the assertion that catches a prefix constant with
	// the dot dropped — which would otherwise route "bulk_transactions_report" and any
	// other similarly-named string onto the transactions topic.
	assert.Equal(t, "blnk.system", TopicForEvent("bulk_transaction"))
}

// TestTopicForEvent_UnrecognisedEventRoutesToTheSystemTopic pins the chosen behaviour
// for an event type that is not in the catalogue.
//
// The choice is deliberate and it is the safe one. The relay publishes to whatever topic
// it is handed, so returning an empty string would strand the event: no topic, no
// publish, no dead-letter entry, nothing to replay. Routing to the system topic keeps
// the event published, observable and replayable. A producer added later without
// extending the mapping therefore degrades to "landed on the catch-all topic" instead of
// "silently lost".
func TestTopicForEvent_UnrecognisedEventRoutesToTheSystemTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, eventType := range []string{
		"",
		" ",
		"totally.unknown",
		"account.created",
		"reconciliation.completed",
		// Case matters: the mapping is exact and case-sensitive by design, so an
		// upper-cased event name is simply not an emitted string.
		"TRANSACTION.APPLIED",
		"Balance.Created",
		// A longer string that merely starts with an emitted name is not that event.
		"transaction.applied.v2",
	} {
		assert.Equal(t, "blnk.system", TopicForEvent(eventType),
			"unrecognised event type %q must route to the system catch-all topic", eventType)
		assert.NotEmpty(t, TopicForEvent(eventType),
			"TopicForEvent must never return an empty string: the relay would strand the event")
	}
}

// TestDLTFor_DerivesTheFourDeadLetterTopics asserts the exact dead-letter names for all
// four category topics.
//
// Three of these four strings are the literal names the requirements specify, so they
// must match byte for byte. They are also the names the local provisioning path creates,
// the names the alert rules label and the names the operator runbooks tell a human to
// look at, which is why they are written out here rather than derived.
func TestDLTFor_DerivesTheFourDeadLetterTopics(t *testing.T) {
	assert.Equal(t, "blnk.transactions.dlt", DLTFor("blnk.transactions"))
	assert.Equal(t, "blnk.balances.dlt", DLTFor("blnk.balances"))
	assert.Equal(t, "blnk.identities.dlt", DLTFor("blnk.identities"))
	assert.Equal(t, "blnk.system.dlt", DLTFor("blnk.system"))

	// The suffix constant is the published convention. Its value is part of the
	// subscriber contract, so it is pinned independently of the names derived from it.
	assert.Equal(t, ".dlt", DeadLetterTopicSuffix)
}

// TestDLTFor_IsIdempotent proves a second application is absorbed rather than compounded.
//
// Double application is a real hazard, not a theoretical one: the dead-letter writer
// resolves a name, persists it on the outbox row, and the replay path reads it back, so
// there are several places a second call could creep in. The result would be
// blnk.transactions.dlt.dlt — a topic that Kafka will happily create, that no consumer
// subscribes to and that no alert covers. Absorbing the second application makes the
// whole class of bug impossible.
func TestDLTFor_IsIdempotent(t *testing.T) {
	for _, topic := range []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	} {
		once := DLTFor(topic)
		assert.Equal(t, once, DLTFor(once),
			"applying DLTFor twice to %q must not compound the suffix", topic)
		assert.Equal(t, once, DLTFor(DLTFor(DLTFor(topic))),
			"and a third application must not either")
		assert.False(t, strings.HasSuffix(once, DeadLetterTopicSuffix+DeadLetterTopicSuffix),
			"the doubled suffix must never appear")
	}
}

// TestDLTFor_BlankTopicYieldsTheEmptyString pins the empty-input behaviour.
//
// There is nothing to derive a sibling from, and returning a bare ".dlt" would name a
// real topic consisting only of a suffix. The empty result makes the caller's own publish
// fail visibly instead, since Kafka rejects an empty topic — the correct outcome for what
// is a programming error at the call site.
func TestDLTFor_BlankTopicYieldsTheEmptyString(t *testing.T) {
	for _, blank := range []string{"", " ", "\t", "\n", "  \t\n "} {
		assert.Equal(t, "", DLTFor(blank),
			"a blank topic %q must yield the empty string, never a bare %q", blank, DeadLetterTopicSuffix)
	}
}

// TestDLTFor_IgnoresSurroundingWhitespace documents the tolerance and, more importantly,
// pins that the whitespace is not carried into the derived name.
//
// A topic name with a leading or trailing space is a name Kafka rejects, so silently
// preserving it would turn a recoverable configuration slip into an unpublishable event.
func TestDLTFor_IgnoresSurroundingWhitespace(t *testing.T) {
	assert.Equal(t, "blnk.transactions.dlt", DLTFor("  blnk.transactions  "))
	assert.Equal(t, "blnk.system.dlt", DLTFor("\tblnk.system\n"))
	assert.Equal(t, "blnk.balances.dlt", DLTFor(" blnk.balances.dlt "),
		"whitespace around an already-derived name must be trimmed without a second suffix")
}

// TestDeadLetterTopicForEvent_RoutesEveryEmittedEventString pins the dead-letter name for
// each of the thirteen event strings.
//
// A dead-lettered event has to be replayable to the topic it failed to reach, so the
// dead-letter topic must always be the sibling of the event's own category topic. Getting
// this wrong would scatter failures across the wrong dead-letter topics and make the
// triage runbook point at the wrong place.
func TestDeadLetterTopicForEvent_RoutesEveryEmittedEventString(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.queued"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.applied"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.scheduled"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.inflight"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.void"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.rejected"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("transaction.unknown"))
	assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent("bulk_transaction.failed"))
	assert.Equal(t, "blnk.balances.dlt", DeadLetterTopicForEvent("balance.created"))
	assert.Equal(t, "blnk.balances.dlt", DeadLetterTopicForEvent("balance.monitor"))
	assert.Equal(t, "blnk.identities.dlt", DeadLetterTopicForEvent("identity.created"))
	assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent("ledger.created"))
	assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent("system.error"))

	// An unrecognised event dead-letters to the catch-all's sibling, so even an event
	// nobody mapped remains recoverable.
	assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent("totally.unknown"))
	assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent(""))
}

// TestDeadLetterTopicForEvent_IsDLTForOfTopicForEvent pins the composition order.
//
// DLTFor(TopicForEvent(x)) is the only correct order, and the helper exists so callers
// cannot get it wrong. Asserting the equality here is what keeps the helper honest if
// either half changes.
func TestDeadLetterTopicForEvent_IsDLTForOfTopicForEvent(t *testing.T) {
	storeKafkaTopicPrefix(t, "acme")

	for _, eventType := range []string{
		"transaction.applied",
		"bulk_transaction.failed",
		"balance.monitor",
		"identity.created",
		"ledger.created",
		"system.error",
		"totally.unknown",
	} {
		assert.Equal(t, DLTFor(TopicForEvent(eventType)), DeadLetterTopicForEvent(eventType),
			"DeadLetterTopicForEvent(%q) must equal DLTFor(TopicForEvent(%q))", eventType, eventType)
	}
}

// TestIsDeadLetterTopic_DistinguishesTheTwoKindsOfTopic covers the suffix predicate.
//
// Callers use it to avoid republishing a dead-letter entry onto a dead-letter topic and
// to label metrics and operator output, so it has to answer for any configured namespace
// — hence the non-default prefix cases — and it must not answer true for a blank name.
func TestIsDeadLetterTopic_DistinguishesTheTwoKindsOfTopic(t *testing.T) {
	assert.True(t, IsDeadLetterTopic("blnk.transactions.dlt"))
	assert.True(t, IsDeadLetterTopic("blnk.balances.dlt"))
	assert.True(t, IsDeadLetterTopic("blnk.identities.dlt"))
	assert.True(t, IsDeadLetterTopic("blnk.system.dlt"))
	assert.True(t, IsDeadLetterTopic("  blnk.system.dlt  "),
		"surrounding whitespace must not change the verdict")
	assert.True(t, IsDeadLetterTopic("acme.events.transactions.dlt"),
		"the test is on the suffix alone, so it must hold for any configured namespace")

	assert.False(t, IsDeadLetterTopic("blnk.transactions"))
	assert.False(t, IsDeadLetterTopic("blnk.balances"))
	assert.False(t, IsDeadLetterTopic("blnk.identities"))
	assert.False(t, IsDeadLetterTopic("blnk.system"))
	assert.False(t, IsDeadLetterTopic(""))
	assert.False(t, IsDeadLetterTopic("   "))
	assert.False(t, IsDeadLetterTopic("blnk.transactions.dlt.replayed"),
		"the suffix must be terminal: a name with trailing segments is not a dead-letter topic")
}

// TestTopicPrefix_DefaultsToBlnkWhenUnset pins the documented default.
//
// The default matters more than it looks: a configuration published without passing
// through the defaulting path carries an empty prefix, and composing from an empty prefix
// would produce ".transactions" — a valid Kafka topic that no provisioning script creates
// and no subscriber reads.
func TestTopicPrefix_DefaultsToBlnkWhenUnset(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, "blnk", TopicPrefix())
	assert.Equal(t, "blnk", DefaultTopicPrefix)

	// Every derived name must show the default, not just the prefix accessor.
	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, AllTopics())
}

// TestTopicPrefix_HonoursAConfiguredOverride proves the whole namespace is
// configuration-driven, which is the reason no topic name may be spelled as a literal
// anywhere else in the codebase.
//
// Asserting the accessor alone would be far too weak: it would pass even if a name
// somewhere were hardcoded to "blnk.". Every derived name is therefore checked against
// the override.
func TestTopicPrefix_HonoursAConfiguredOverride(t *testing.T) {
	storeKafkaTopicPrefix(t, "acme.events")

	assert.Equal(t, "acme.events", TopicPrefix())

	assert.Equal(t, "acme.events.transactions", TopicForEvent("transaction.applied"))
	assert.Equal(t, "acme.events.transactions", TopicForEvent("bulk_transaction.failed"))
	assert.Equal(t, "acme.events.balances", TopicForEvent("balance.created"))
	assert.Equal(t, "acme.events.identities", TopicForEvent("identity.created"))
	assert.Equal(t, "acme.events.system", TopicForEvent("ledger.created"))
	assert.Equal(t, "acme.events.system", TopicForEvent("system.error"))
	assert.Equal(t, "acme.events.system", TopicForEvent("totally.unknown"))

	assert.Equal(t, "acme.events.transactions.dlt", DeadLetterTopicForEvent("transaction.applied"))
	assert.Equal(t, "acme.events.system.dlt", DeadLetterTopicForEvent("system.error"))

	assert.Equal(t, []string{
		"acme.events.transactions",
		"acme.events.balances",
		"acme.events.identities",
		"acme.events.system",
	}, AllTopics())

	assert.Equal(t, []string{
		"acme.events.transactions.dlt",
		"acme.events.balances.dlt",
		"acme.events.identities.dlt",
		"acme.events.system.dlt",
	}, AllDeadLetterTopics())

	assert.Equal(t, []string{
		"acme.events.transactions",
		"acme.events.balances",
		"acme.events.identities",
		"acme.events.system",
		"acme.events.transactions.dlt",
		"acme.events.balances.dlt",
		"acme.events.identities.dlt",
		"acme.events.system.dlt",
	}, AllTopicsWithDeadLetters())

	// No derived name may retain the default namespace once an override is configured.
	for _, topic := range AllTopicsWithDeadLetters() {
		assert.False(t, strings.HasPrefix(topic, DefaultTopicPrefix+"."),
			"topic %q still carries the default prefix despite an override; some name is hardcoded", topic)
	}
}

// TestTopicPrefix_ThenUnsetReturnsToTheDefault proves the prefix is re-read on every call
// rather than cached.
//
// A cached prefix would keep answering from configuration that no longer exists, because
// the configuration store is re-published whenever configuration is (re)loaded. The
// override-then-unset sequence is the smallest observation that distinguishes a live read
// from a cached one.
func TestTopicPrefix_ThenUnsetReturnsToTheDefault(t *testing.T) {
	storeKafkaTopicPrefix(t, "acme")
	require.Equal(t, "acme", TopicPrefix())
	require.Equal(t, "acme.transactions", TopicForEvent("transaction.applied"))

	storeKafkaTopicPrefix(t, "")
	assert.Equal(t, "blnk", TopicPrefix(),
		"unsetting the prefix must return the default; a cached value would still answer \"acme\"")
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.applied"))
}

// TestTopicPrefix_TrimsWhitespaceAndStraySeparators covers the normalisation.
//
// Both halves of the cutset earn their place. Whitespace, because the configuration
// loader trims only a fixed set of fields and the topic prefix is not one of them, so a
// trailing newline from an environment file would be carried into every topic name.
// Separators, because KAFKA_TOPIC_PREFIX=acme. is the natural mistake and would otherwise
// compose "acme..transactions" — which Kafka accepts as a topic distinct from the intended
// one, so nothing fails and no subscriber ever receives anything.
func TestTopicPrefix_TrimsWhitespaceAndStraySeparators(t *testing.T) {
	for _, raw := range []string{
		" acme ",
		"\tacme\n",
		"acme.",
		".acme",
		".acme.",
		"..acme..",
		" .acme. ",
		"\n.acme.\t",
	} {
		storeKafkaTopicPrefix(t, raw)

		assert.Equal(t, "acme", TopicPrefix(),
			"prefix %q must normalise to \"acme\"", raw)
		assert.Equal(t, "acme.transactions", TopicForEvent("transaction.applied"),
			"prefix %q must compose a single separator", raw)
		assert.Equal(t, "acme.transactions.dlt", DeadLetterTopicForEvent("transaction.applied"),
			"prefix %q must compose a single separator on the dead-letter name too", raw)
	}

	// A prefix made only of whitespace and separators has nothing left after trimming, so
	// it resolves to the default rather than to an empty namespace.
	for _, raw := range []string{" ", "\t\n", ".", "..", " . ", "\n.\t"} {
		storeKafkaTopicPrefix(t, raw)

		assert.Equal(t, "blnk", TopicPrefix(),
			"prefix %q trims to nothing and must resolve to the default", raw)
	}

	// Interior characters are deliberately left alone. A prefix the broker will reject
	// must reach the broker and be rejected loudly, not be silently rewritten here into a
	// topic namespace the operator never asked for.
	storeKafkaTopicPrefix(t, "acme events")
	assert.Equal(t, "acme events", TopicPrefix(),
		"interior characters must be preserved so an invalid prefix fails visibly at the broker")
}

// TestTopicPrefix_DefaultsWhenConfigurationIsNotLoaded pins the unloaded-configuration
// branch.
//
// Answering with the default keeps topic naming total — every event still resolves to a
// real topic name — which matters because naming is exercised by tests, by tooling and by
// early start-up paths that have no reason to have loaded a full configuration. Returning
// an empty prefix or panicking here would break all three.
func TestTopicPrefix_DefaultsWhenConfigurationIsNotLoaded(t *testing.T) {
	withUnloadedConfiguration(t)

	assert.Equal(t, "blnk", TopicPrefix())
	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.applied"))
	assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent("system.error"))
	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, AllTopics())
}

// TestTopicPrefixFrom_ResolvesTheDefaultForEveryAbsentForm exercises the pure resolver
// directly, including the nil-configuration branch that the exported accessor can only
// reach through its error path.
func TestTopicPrefixFrom_ResolvesTheDefaultForEveryAbsentForm(t *testing.T) {
	assert.Equal(t, "blnk", topicPrefixFrom(nil),
		"a nil configuration must resolve to the default rather than dereference")

	assert.Equal(t, "blnk", topicPrefixFrom(&config.Configuration{}),
		"a configuration with no Kafka block must resolve to the default")

	assert.Equal(t, "blnk", topicPrefixFrom(&config.Configuration{
		Kafka: config.KafkaConfig{TopicPrefix: "   "},
	}))

	assert.Equal(t, "acme", topicPrefixFrom(&config.Configuration{
		Kafka: config.KafkaConfig{TopicPrefix: " acme. "},
	}))
}

// TestDefaultTopicPrefix_MatchesTheConfigurationDefault pins the two defaults equal.
//
// The value is declared in two places out of necessity: the configuration package applies
// it on the validate-and-default path, and this file applies it for configurations that
// never took that path. Two independent declarations of one value can drift, and a drift
// would mean the topics the code writes to depend on how configuration happened to be
// loaded. Comparing them against each other is the only way to catch that.
func TestDefaultTopicPrefix_MatchesTheConfigurationDefault(t *testing.T) {
	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	// MockConfig runs the real validate-and-default path, so the prefix below is filled
	// by the configuration package's own default rather than by anything in this file.
	//
	// The two DSNs are the only fields that path requires, and it merely checks they are
	// non-empty — nothing is dialled here. They are therefore written without credentials
	// of any kind, deliberately: a test fixture must never carry a credential-shaped
	// string, not even a fake one, so that a secret scan over this repository has nothing
	// to triage.
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://localhost:5432/blnk?sslmode=disable"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
	})

	loaded, err := config.Fetch()
	require.NoError(t, err, "MockConfig must publish a configuration")
	require.Equal(t, DefaultTopicPrefix, loaded.Kafka.TopicPrefix,
		"the configuration package's Kafka topic-prefix default and DefaultTopicPrefix must be the same value")

	assert.Equal(t, "blnk.transactions", TopicForEvent("transaction.applied"),
		"and a fully defaulted configuration must yield the documented topic names")
}

// TestAllTopics_IsTheFourCategoryTopicsInCanonicalOrder pins the publish-side inventory,
// exactly and in order.
//
// Order is part of the contract, not a detail: the provisioning path and the operator
// documentation list these names in this order so the two can be diffed line for line.
func TestAllTopics_IsTheFourCategoryTopicsInCanonicalOrder(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, AllTopics())

	// None of the publish-side topics may be a dead-letter topic; that would mean events
	// were being published directly onto a dead-letter topic.
	for _, topic := range AllTopics() {
		assert.False(t, IsDeadLetterTopic(topic),
			"%q is a category topic and must not carry the dead-letter suffix", topic)
	}
}

// TestAllDeadLetterTopics_IsTheFourSiblingsInCanonicalOrder pins the dead-letter
// inventory and its index alignment with AllTopics.
//
// The alignment lets a caller zip the two slices to pair a topic with its sibling, which
// the admin path and the triage runbook both rely on.
func TestAllDeadLetterTopics_IsTheFourSiblingsInCanonicalOrder(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, []string{
		"blnk.transactions.dlt",
		"blnk.balances.dlt",
		"blnk.identities.dlt",
		"blnk.system.dlt",
	}, AllDeadLetterTopics())

	category := AllTopics()
	deadLetter := AllDeadLetterTopics()
	require.Len(t, deadLetter, len(category),
		"the two inventories must be the same length so they can be zipped")
	for i := range category {
		assert.Equal(t, DLTFor(category[i]), deadLetter[i],
			"index %d must pair %q with its own sibling", i, category[i])
		assert.True(t, IsDeadLetterTopic(deadLetter[i]))
	}
}

// TestAllTopicsWithDeadLetters_IsTheEightProvisionedTopics pins the complete inventory —
// the single source of truth shared by the Go admin path and the provisioning script.
//
// These eight names, in this order, are what the local stack provisions. If this list and
// the provisioning script ever disagree, the failure is silent in the worst way: the
// script creates topics the code never writes to, and the code writes to topics the script
// never created, which either fails on an unprovisioned topic or auto-creates one with a
// single partition and the wrong replication factor — quietly discarding the ordering and
// durability guarantees.
func TestAllTopicsWithDeadLetters_IsTheEightProvisionedTopics(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
		"blnk.transactions.dlt",
		"blnk.balances.dlt",
		"blnk.identities.dlt",
		"blnk.system.dlt",
	}, AllTopicsWithDeadLetters())

	// It must be exactly the concatenation of the two halves, in that order.
	assert.Equal(t, append(AllTopics(), AllDeadLetterTopics()...), AllTopicsWithDeadLetters())

	// Every name must be distinct. A duplicate would make topic assurance attempt the
	// same topic twice and would misreport the inventory size to an operator.
	seen := make(map[string]struct{}, len(AllTopicsWithDeadLetters()))
	for _, topic := range AllTopicsWithDeadLetters() {
		_, duplicate := seen[topic]
		assert.False(t, duplicate, "topic %q appears twice in the inventory", topic)
		seen[topic] = struct{}{}
	}
	assert.Len(t, seen, 8, "the inventory is four category topics plus four dead-letter siblings")
}

// TestTopicInventory_ReturnsFreshSlicesCallersMayMutate proves the accessors hand back
// copies rather than shared package state.
//
// The admin path sorts and filters these lists. If any accessor returned a slice backed by
// a package-level array, one caller's sort would silently reorder the inventory for every
// other caller — including the order this file's own tests depend on.
func TestTopicInventory_ReturnsFreshSlicesCallersMayMutate(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	mutated := AllTopics()
	mutated[0] = "clobbered"
	assert.Equal(t, "blnk.transactions", AllTopics()[0],
		"AllTopics must not be backed by shared state")

	mutatedDLT := AllDeadLetterTopics()
	mutatedDLT[0] = "clobbered"
	assert.Equal(t, "blnk.transactions.dlt", AllDeadLetterTopics()[0],
		"AllDeadLetterTopics must not be backed by shared state")

	mutatedAll := AllTopicsWithDeadLetters()
	mutatedAll[0] = "clobbered"
	assert.Equal(t, "blnk.transactions", AllTopicsWithDeadLetters()[0],
		"AllTopicsWithDeadLetters must not be backed by shared state")

	mutatedCategories := EventCategories()
	mutatedCategories[0] = "clobbered"
	assert.Equal(t, model.EventCategoryTransactions, EventCategories()[0],
		"EventCategories must not be backed by shared state")
}

// TestEventCategories_IsTheFourTokensInCanonicalOrder pins the category enumeration.
//
// The tokens are compared against the model constants they are built from, so the
// enumeration cannot drift from the vocabulary; the ORDER, which model does not declare,
// is pinned by the literal list.
func TestEventCategories_IsTheFourTokensInCanonicalOrder(t *testing.T) {
	assert.Equal(t, []string{
		"transactions",
		"balances",
		"identities",
		"system",
	}, EventCategories())

	assert.Equal(t, []string{
		model.EventCategoryTransactions,
		model.EventCategoryBalances,
		model.EventCategoryIdentities,
		model.EventCategorySystem,
	}, EventCategories(),
		"the enumeration must be built from the model constants, not from re-spelled literals")
}

// TestEventCategories_ContainsNoCategoryNamedDLT enforces the invariant that DLFor's
// idempotency implies.
//
// A category named "dlt" would compose blnk.dlt, which is indistinguishable from an
// already-derived dead-letter name, so it could never be given a sibling of its own — its
// failures would have nowhere to go. Equally, a category token containing the separator
// would add an unintended topic segment.
func TestEventCategories_ContainsNoCategoryNamedDLT(t *testing.T) {
	for _, category := range EventCategories() {
		assert.NotEqual(t, strings.TrimPrefix(DeadLetterTopicSuffix, "."), category,
			"no category may be named after the dead-letter suffix: %q could never get a dead-letter sibling", category)
		assert.NotContains(t, category, ".",
			"category %q must not contain the separator: the naming layer owns it", category)
		assert.NotEmpty(t, category, "a category token must not be empty")
	}

	storeKafkaTopicPrefix(t, "")
	for _, topic := range AllTopics() {
		assert.False(t, IsDeadLetterTopic(topic),
			"category topic %q must be distinguishable from a dead-letter topic", topic)
	}
}

// TestTopicForCategory_BlankCategoryFallsBackToSystem pins the guard against composing a
// name with an empty final segment.
//
// "<prefix>." is a topic Kafka would accept and nothing would read. Falling back to the
// catch-all keeps every result a usable topic, which is the same never-drop-an-event
// policy the event mapping's own catch-all implements.
func TestTopicForCategory_BlankCategoryFallsBackToSystem(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, blank := range []string{"", " ", "\t", ".", " . ", "\n"} {
		assert.Equal(t, "blnk.system", TopicForCategory(blank),
			"a blank category %q must fall back to the system topic, never compose %q", blank, "blnk.")
		assert.NotEqual(t, "blnk.", TopicForCategory(blank))
	}
}

// TestTopicForCategory_ComposesAnUnknownCategoryAsGiven documents that this function is a
// name composer and not a validator.
//
// A category added to the event mapping must work here with no edit, which is what keeps
// the two files from having to change in lockstep. An unprovisioned topic then surfaces
// loudly on the admin path, which is the right place for it.
func TestTopicForCategory_ComposesAnUnknownCategoryAsGiven(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, "blnk.orders", TopicForCategory("orders"))
	assert.Equal(t, "blnk.orders", TopicForCategory("  orders  "),
		"surrounding whitespace and separators are trimmed from the category too")
	assert.Equal(t, "blnk.transactions", TopicForCategory(model.EventCategoryTransactions))
	assert.Equal(t, "blnk.balances", TopicForCategory(model.EventCategoryBalances))
	assert.Equal(t, "blnk.identities", TopicForCategory(model.EventCategoryIdentities))
	assert.Equal(t, "blnk.system", TopicForCategory(model.EventCategorySystem))
}

// TestEventTopicsSource_HardcodesNoComposedTopicName is a structural guarantee that every
// topic name really is derived from configuration.
//
// A behavioural test cannot prove this on its own: a hardcoded "blnk.transactions"
// somewhere would pass every default-prefix assertion in this file and only fail under an
// override, and only if that particular call site happened to be exercised. Scanning the
// AST's string literals proves it for the whole file at once.
//
// Only string literals are inspected, so the documentation is free to spell the topic
// names out — which it must, to be useful — while executable code may not.
func TestEventTopicsSource_HardcodesNoComposedTopicName(t *testing.T) {
	parsed := parseEventTopicsSource(t)

	prefixLiterals := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}

		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}

		assert.False(t, strings.HasPrefix(value, DefaultTopicPrefix+"."),
			"string literal %q composes a topic name; every name must be derived from TopicPrefix()", value)

		if value == DefaultTopicPrefix {
			prefixLiterals++
		}

		return true
	})

	assert.Equal(t, 1, prefixLiterals,
		"the default namespace must appear exactly once, as DefaultTopicPrefix; any other occurrence is a hardcoded name")
}

// TestEventTopicsSource_ImportsNoKafkaClient keeps the naming layer broker-free.
//
// Topic naming is the one decision the whole pipeline depends on, so it must stay
// exercisable to the last branch by a unit test with no broker, no network and no
// fixtures. A Kafka import here would be the first step towards naming logic that can
// only be tested with a live cluster.
func TestEventTopicsSource_ImportsNoKafkaClient(t *testing.T) {
	parsed := parseEventTopicsSource(t)

	for _, imported := range parsed.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		require.NoError(t, err, "every import path must be a valid string literal")

		assert.NotContains(t, strings.ToLower(path), "kafka",
			"event_topics.go must not import a Kafka client; broker interaction belongs to the publisher, admin and dead-letter files")
	}
}

// TestEventTopicsSource_RecordsTheSunsetRelocationOfGetEventFromStatus protects the
// receiving half of the sunset procedure.
//
// getEventFromStatus must be relocated into event_topics.go before webhooks.go is
// deleted, because it is seven of the thirteen event strings — the vocabulary this file
// routes. That instruction lives in a comment, so nothing but a test can stop it being
// deleted, and losing it means a future sunset deletes the vocabulary along with its
// host file.
//
// The test also asserts the function is NOT yet declared here. A premature relocation
// would be a duplicate declaration in package blnk and would fail the build outright, so
// this assertion exists to name the reason rather than to catch it late.
func TestEventTopicsSource_RecordsTheSunsetRelocationOfGetEventFromStatus(t *testing.T) {
	parsed := parseEventTopicsSource(t)

	var comments strings.Builder
	for _, group := range parsed.Comments {
		comments.WriteString(group.Text())
	}
	documentation := comments.String()

	assert.Contains(t, documentation, "SUNSET RELOCATION TARGET",
		"the delimited sunset relocation block must remain in event_topics.go")
	assert.Contains(t, documentation, "getEventFromStatus",
		"the relocation block must name the symbol that relocates here")

	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}

		assert.NotEqual(t, "getEventFromStatus", function.Name.Name,
			"getEventFromStatus must stay declared only in webhooks.go until the dual-delivery window closes; a second declaration in package blnk breaks the build")
	}

	// It must still be reachable from its current home, which is what the transaction
	// producer call site depends on.
	assert.Equal(t, "transaction.applied", getEventFromStatus(StatusApplied),
		"the event vocabulary must remain callable from its declaration site in webhooks.go")
	assert.Equal(t, "transaction.unknown", getEventFromStatus(StatusCommit),
		"and the deliberately preserved COMMIT fall-through must be untouched")
}
