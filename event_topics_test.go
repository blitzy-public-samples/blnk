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
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

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

// eventCatalogueSize is the number of event strings Blnk emits: thirteen.
//
// It is spelled as a number, separately from the catalogue itself, so that the catalogue
// cannot silently shrink. `assert.Len(eventCatalogue, len(eventCatalogue))` would be a
// tautology; comparing against an independently written constant is not.
//
// Thirteen is a CONTRACTUAL figure, not an incidental one. The coverage requirement is
// that every event type which reached the legacy webhook sender reaches Kafka, with zero
// exceptions, and thirteen is what an exhaustive sweep of the producer call sites found:
// seven transaction lifecycle events from getEventFromStatus, the runtime-composed bulk
// transaction family, two balance events, one identity event, one ledger event, and the
// system error raised through the registered webhook-sender indirection.
//
// Changing this number is therefore a deliberate act. A fourteenth event type is welcome,
// but it must arrive with a catalogue row, a topic assertion and — if it needs one — a new
// category, which is exactly what failing here forces whoever adds it to do.
const eventCatalogueSize = 13

// eventCatalogueEntry is one row of the event catalogue: an event string Blnk emits,
// paired with every exact name it must resolve to.
type eventCatalogueEntry struct {
	// eventType is the event string as a producer spells it. For the bulk transaction
	// family this is one concrete member of the family; the open-ended suffix set is
	// covered separately and exhaustively by the prefix test.
	eventType string

	// vocabularyKey is how this event appears in model.EventCategory's SOURCE.
	//
	// For twelve of the thirteen entries it is identical to eventType, because the
	// mapping matches those by exact equality and each one is a literal in a case
	// clause. For the bulk transaction family it is the bare "bulk_transaction." prefix,
	// because those names are composed at runtime from the batch status: there is no
	// exhaustive literal to name, and the implementation matches them by prefix.
	//
	// The field exists so the count assertion can compare this hand-written catalogue
	// against the vocabulary actually present in the implementation. Without it, the
	// comparison would have to guess which rows are exact matches and which are
	// families.
	vocabularyKey string

	// topic is the fully-qualified category topic, with the default prefix applied.
	topic string

	// deadLetterTopic is topic's dead-letter sibling, written out in full rather than
	// derived, so that a change to either the topic or the suffix fails here.
	deadLetterTopic string

	// category is the bare token model.EventCategory must resolve eventType to. It
	// catches a category rename, which would otherwise only show up as a changed topic
	// name and could be mistaken for an intended renaming of the namespace.
	category string
}

// eventCatalogue is the complete, independently written statement of the routing
// contract: all thirteen event strings Blnk emits, the four category topics they route to and their
// four dead-letter siblings.
//
// EVERY VALUE HERE IS A LITERAL, and that is the single most important property of this
// file. Nothing is computed from model.EventCategory, from TopicForEvent, or from the
// prefix and separator constants. A table built by asking the implementation what it
// thinks would agree with the implementation no matter what the implementation said —
// including after an entry had been deleted, a category renamed or the separator changed.
// Writing the expectations out by hand is what makes this a second opinion rather than an
// echo, and it is what makes the file resistant to mutation testing.
//
// The order is the order the events are documented in: the transaction family, then
// balances, then identities, then the two events that motivate the one extra category.
var eventCatalogue = []eventCatalogueEntry{
	// The seven transaction lifecycle events. All seven originate in
	// getEventFromStatus, which is the transaction event-string vocabulary and which
	// relocates into event_topics.go when the legacy transport is retired.
	{
		eventType:       "transaction.queued",
		vocabularyKey:   "transaction.queued",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	{
		eventType:       "transaction.applied",
		vocabularyKey:   "transaction.applied",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	{
		eventType:       "transaction.scheduled",
		vocabularyKey:   "transaction.scheduled",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	{
		eventType:       "transaction.inflight",
		vocabularyKey:   "transaction.inflight",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	{
		eventType:       "transaction.void",
		vocabularyKey:   "transaction.void",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	{
		eventType:       "transaction.rejected",
		vocabularyKey:   "transaction.rejected",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	// transaction.unknown is reachable, not hypothetical: the COMMIT status has no case
	// in getEventFromStatus and falls through to it. That behaviour is preserved
	// deliberately so the dual-delivery payload comparison stays exact, so the event
	// name must route like any other.
	{
		eventType:       "transaction.unknown",
		vocabularyKey:   "transaction.unknown",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	// The eighth transaction event is a FAMILY, composed at runtime as
	// "bulk_transaction." + batch status. One concrete member stands for it here; the
	// open suffix set is covered exhaustively by the prefix test.
	{
		eventType:       "bulk_transaction.applied",
		vocabularyKey:   "bulk_transaction.",
		topic:           "blnk.transactions",
		deadLetterTopic: "blnk.transactions.dlt",
		category:        "transactions",
	},
	// The two balance events.
	{
		eventType:       "balance.created",
		vocabularyKey:   "balance.created",
		topic:           "blnk.balances",
		deadLetterTopic: "blnk.balances.dlt",
		category:        "balances",
	},
	{
		eventType:       "balance.monitor",
		vocabularyKey:   "balance.monitor",
		topic:           "blnk.balances",
		deadLetterTopic: "blnk.balances.dlt",
		category:        "balances",
	},
	// The single identity event.
	{
		eventType:       "identity.created",
		vocabularyKey:   "identity.created",
		topic:           "blnk.identities",
		deadLetterTopic: "blnk.identities.dlt",
		category:        "identities",
	},
	// The two events that belong to none of the three requirement-named categories, and
	// which are the entire reason the one extra category exists. If either ever resolves to
	// blnk.transactions, blnk.balances or blnk.identities, a category has been "simplified
	// away" and a subscriber filtering that topic is now receiving events it never
	// subscribed to.
	//
	// THEY SHARE ONE CATEGORY, which is AMBIGUITY-2's recorded answer: two uncatalogued
	// event types are covered by a single fourth category rather than one each. The category
	// is the narrowest category because system.error carries Blnk's own error text verbatim in a frozen
	// payload, so ledger.created is published, observable and replayable while not being
	// consumable by a subscriber credential. That consequence is stated for subscribers in
	// docs/event-streaming.md. Splitting ledger.created into a fifth grantable category
	// would remove the limitation and is a deliberate contract change, not a tidy-up: it
	// adds two names to the inventory the provisioning script, the Kubernetes configuration
	// and the local stack all enumerate.
	{
		eventType:       "ledger.created",
		vocabularyKey:   "ledger.created",
		topic:           "blnk.system",
		deadLetterTopic: "blnk.system.dlt",
		category:        "system",
	},
	{
		eventType:       "system.error",
		vocabularyKey:   "system.error",
		topic:           "blnk.system",
		deadLetterTopic: "blnk.system.dlt",
		category:        "system",
	},
}

// catalogueEventTypes returns just the event strings from the catalogue.
//
// It exists so that a test asserting membership does not have to rebuild the projection
// inline, and it deliberately returns the eventType column rather than vocabularyKey: the
// question being asked is "is this a name a producer emits", not "is this how the mapping
// spells it".
//
// Returns:
//   - []string: a fresh slice the caller may append to or mutate; the catalogue itself is
//     never handed out, so no test can reorder or truncate it for another.
func catalogueEventTypes() []string {
	eventTypes := make([]string, 0, len(eventCatalogue))
	for _, entry := range eventCatalogue {
		eventTypes = append(eventTypes, entry.eventType)
	}

	return eventTypes
}

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

// findFunctionDeclaration returns the top-level function of the given name, or nil.
func findFunctionDeclaration(parsed *ast.File, name string) *ast.FuncDecl {
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name {
			return function
		}
	}

	return nil
}

// constStringValue returns the value of the named string constant declared in parsed.
//
// The second result reports whether a constant of that name with an unquotable string
// value was found, so a caller can produce a failure message that names the constant
// rather than reporting a bare empty string.
func constStringValue(parsed *ast.File, name string) (string, bool) {
	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}

		for _, specification := range general.Specs {
			value, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for i, identifier := range value.Names {
				if identifier.Name != name || i >= len(value.Values) {
					continue
				}

				literal, ok := value.Values[i].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}

				unquoted, err := strconv.Unquote(literal.Value)
				if err != nil {
					continue
				}

				return unquoted, true
			}
		}
	}

	return "", false
}

// discoverEventVocabulary reads the event vocabulary out of model/event.go's source and
// returns it as a set of vocabulary keys, directly comparable with eventCatalogue's
// vocabularyKey column.
//
// # Why the source is read rather than the function called
//
// This is what makes the thirteen-event count assertion mean something. model.EventCategory
// answers for ANY input — its catch-all routes an unmapped event to the system category
// without complaint — so no amount of calling it can reveal that an event string was
// deleted from its table, nor that a fourteenth was added. Both are invisible
// behaviourally and both are plainly visible in the source.
//
// The alternative, exporting the table from model so a test could count it, would put the
// count under the control of the code being counted: an entry removed from the table would
// be removed from the count too and the test would agree with the loss. Reading the source
// keeps the test's thirteen literals as the only authority.
//
// # What counts as one entry in the vocabulary
//
// Two forms, matching the two ways the implementation matches:
//
//   - Every string literal in a case clause of EventCategory's switch. These are the
//     exact-match names, twelve of the thirteen.
//   - The prefix constant used by the single strings.HasPrefix guard, which stands for the
//     whole runtime-composed bulk transaction family. The constant is resolved through the
//     identifier the guard actually names, so renaming it breaks nothing here; removing the
//     guard, or adding a second one, does — a second prefix family is a routing rule this
//     test has never been told about.
func discoverEventVocabulary(t *testing.T) []string {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), "model", "event.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	require.NoError(t, err, "model/event.go must be parseable to discover the event vocabulary")

	mapping := findFunctionDeclaration(parsed, "EventCategory")
	require.NotNil(t, mapping,
		"model.EventCategory must exist: it is the single event-type-to-category mapping the topic layer delegates to")

	// The named event types come from the CATALOGUE TABLE the mapping consults —
	// model.eventTypeCategories — rather than from case clauses inside the function. The
	// table is what EventCategory and model.IsCataloguedEventType both read, so its keys
	// are the vocabulary by definition, and reading them here means this test discovers a
	// type that was added to the table without a routing expectation being written down.
	vocabulary := mapLiteralKeys(t, parsed, "eventTypeCategories")

	prefixIdentifiers := make([]string, 0, 1)

	ast.Inspect(mapping, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "HasPrefix" || len(call.Args) != 2 {
			return true
		}

		// The prefix is passed as a named constant rather than a literal, so the
		// identifier is resolved to its declaration below.
		if identifier, isIdentifier := call.Args[1].(*ast.Ident); isIdentifier {
			prefixIdentifiers = append(prefixIdentifiers, identifier.Name)
		}

		return true
	})

	require.Len(t, prefixIdentifiers, 1,
		"model.EventCategory must contain exactly one prefix-matched event family (the bulk transaction one); found %v", prefixIdentifiers)

	prefix, found := constStringValue(parsed, prefixIdentifiers[0])
	require.True(t, found,
		"the prefix constant %q named by model.EventCategory's HasPrefix guard must be a string constant declared in model/event.go", prefixIdentifiers[0])

	return append(vocabulary, prefix)
}

// mapLiteralKeys returns the string keys of a package-level map composite literal,
// discovered from the parsed source.
//
// It exists so that the event vocabulary is read from the ONE table the implementation
// consults instead of being re-spelled in a test. A key added to that table without a
// routing expectation, or an expectation left behind after a key was removed, is then a
// failure here rather than a silent divergence — the catch-all absorbs an orphaned event
// type without complaining, so nothing observable would otherwise show it.
//
// Parameters:
//   - file *ast.File: the parsed source file.
//   - name string: the variable name whose map literal is read.
//
// Returns:
//   - []string: the unquoted string keys, in declaration order.
func mapLiteralKeys(t *testing.T, file *ast.File, name string) []string {
	t.Helper()

	var keys []string
	found := false

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}

		for _, spec := range general.Specs {
			value, isValue := spec.(*ast.ValueSpec)
			if !isValue {
				continue
			}

			for i, identifier := range value.Names {
				if identifier.Name != name || i >= len(value.Values) {
					continue
				}

				composite, isComposite := value.Values[i].(*ast.CompositeLit)
				if !isComposite {
					continue
				}

				found = true
				for _, element := range composite.Elts {
					pair, isPair := element.(*ast.KeyValueExpr)
					if !isPair {
						continue
					}

					literal, isLiteral := pair.Key.(*ast.BasicLit)
					if !isLiteral || literal.Kind != token.STRING {
						continue
					}

					if unquoted, err := strconv.Unquote(literal.Value); err == nil {
						keys = append(keys, unquoted)
					}
				}
			}
		}
	}

	require.True(t, found,
		"a package-level map literal named %q must exist; if it is renamed or restructured, update this helper rather than deleting the parity it enforces", name)

	return keys
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

	require.Len(t, eventCatalogue, eventCatalogueSize,
		"the catalogue must hold every event string Blnk emits; see eventCatalogueSize")

	for _, entry := range eventCatalogue {
		t.Run(entry.eventType, func(t *testing.T) {
			assert.Equal(t, entry.topic, TopicForEvent(entry.eventType),
				"%q must be published to %q", entry.eventType, entry.topic)

			assert.Equal(t, entry.deadLetterTopic, DeadLetterTopicForEvent(entry.eventType),
				"%q must dead-letter to %q", entry.eventType, entry.deadLetterTopic)

			// The category is asserted separately from the topic so that a renamed
			// category is distinguishable from a renamed namespace. Both change the
			// topic name; only one of them is ever intended.
			assert.Equal(t, entry.category, model.EventCategory(entry.eventType),
				"%q must resolve to the %q category", entry.eventType, entry.category)

			// A category topic must never itself look like a dead-letter topic: that
			// would mean events were being published directly onto a dead-letter topic,
			// which no subscriber reads and every alert treats as a failure.
			assert.False(t, IsDeadLetterTopic(entry.topic),
				"%q is a category topic and must not carry the dead-letter suffix", entry.topic)
			assert.True(t, IsDeadLetterTopic(entry.deadLetterTopic),
				"%q must be recognisable as a dead-letter topic", entry.deadLetterTopic)
		})
	}

	// Coverage of the category topics is asserted from the catalogue's own rows, so an event
	// silently rerouted away from a topic — leaving that topic with no producers at all —
	// fails here as well as in its own subtest.
	routed := make(map[string][]string, len(eventCatalogue))
	for _, entry := range eventCatalogue {
		routed[entry.topic] = append(routed[entry.topic], entry.eventType)
	}

	assert.Len(t, routed["blnk.transactions"], 8,
		"the transactions topic carries the seven lifecycle events plus the bulk transaction family")
	assert.Len(t, routed["blnk.balances"], 2, "the balances topic carries balance.created and balance.monitor")
	assert.Len(t, routed["blnk.identities"], 1, "the identities topic carries identity.created")
	assert.Len(t, routed["blnk.system"], 2, "the system topic carries ledger.created and system.error")
	assert.Len(t, routed, 4,
		"every EMITTED event must land on one of exactly four category topics — the frozen topic "+
			"contract — and the system topic is additionally where an event type the "+
			"mapping table does not recognise is routed")
}

// TestTopicForEvent_CoversEveryEventTypeTheMappingKnows is the count assertion, and it is
// the assertion that makes the coverage requirement enforceable rather than merely stated.
//
// It compares the thirteen event strings written out by hand in eventCatalogue against the
// vocabulary actually present in model.EventCategory's source, in BOTH directions:
//
//   - An event string in the implementation but not in the catalogue means a fourteenth
//     event type was added without a routing expectation. That is the case the requirement
//     is worried about: the new event would route somewhere, nobody would have said where,
//     and no test would have disagreed.
//   - An event string in the catalogue but not in the implementation means a mapping was
//     deleted. Behaviourally that is invisible — the catch-all quietly absorbs the orphaned
//     event onto the system topic — so nothing but a source-level comparison catches it.
//
// Neither direction is detectable by calling the mapping, because the mapping answers for
// every input. See discoverEventVocabulary for why the vocabulary is read from source.
func TestTopicForEvent_CoversEveryEventTypeTheMappingKnows(t *testing.T) {
	expected := make([]string, 0, len(eventCatalogue))
	for _, entry := range eventCatalogue {
		expected = append(expected, entry.vocabularyKey)
	}

	require.Len(t, expected, eventCatalogueSize,
		"the catalogue must contribute exactly %d vocabulary keys", eventCatalogueSize)

	discovered := discoverEventVocabulary(t)

	assert.Len(t, discovered, eventCatalogueSize,
		"model.EventCategory recognises %d event types but the catalogue names %d; a routed event type has been added or removed without updating this test",
		len(discovered), eventCatalogueSize)

	assert.ElementsMatch(t, expected, discovered,
		"the catalogue and model.EventCategory must name exactly the same event types; anything only on one side is an event with no asserted topic, or an asserted topic with no event")

	// Every vocabulary key must be distinct. A duplicated case literal would inflate the
	// discovered count and could mask a deletion elsewhere in the same table.
	seen := make(map[string]struct{}, len(discovered))
	for _, key := range discovered {
		_, duplicate := seen[key]
		assert.False(t, duplicate, "event type %q is mapped twice", key)
		assert.NotEmpty(t, key, "the vocabulary must not contain an empty event type")
		seen[key] = struct{}{}
	}
	assert.Len(t, seen, eventCatalogueSize)
}

// TestGetEventFromStatus_EveryTransactionStatusRoutesToTheTransactionsTopic joins the two
// halves of transaction event routing: the status-to-event-name mapping that produces the
// names, and the name-to-topic mapping that routes them.
//
// Testing them separately is not enough. getEventFromStatus is the ONLY producer of the
// seven transaction event strings, and it is declared in webhooks.go — a file scheduled for
// deletion. If it ever emitted a name the routing table does not know, the event would land
// on the system catch-all: transaction events arriving on a topic no transaction subscriber
// reads, with no error, no log line and no alert. Driving the real status constants through
// the real function and asserting the real topic is the only way to close that gap.
//
// The statuses are taken from the constants rather than re-spelled, so a change to a status
// value is carried into this test automatically instead of being hidden by a stale literal.
func TestGetEventFromStatus_EveryTransactionStatusRoutesToTheTransactionsTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, testCase := range []struct {
		status    string
		eventType string
	}{
		{status: StatusQueued, eventType: "transaction.queued"},
		{status: StatusApplied, eventType: "transaction.applied"},
		{status: StatusScheduled, eventType: "transaction.scheduled"},
		{status: StatusInflight, eventType: "transaction.inflight"},
		{status: StatusVoid, eventType: "transaction.void"},
		{status: StatusRejected, eventType: "transaction.rejected"},
		// COMMIT has no case in getEventFromStatus and falls through to the default.
		// That is a PRE-EXISTING behaviour, preserved on purpose: correcting it here
		// would make the dual-delivery payload comparison differ for a reason that has
		// nothing to do with the transport. It is documented for correction as a
		// separate, intentional change. Until then transaction.unknown is a real event
		// name reached by a real status, so it must route like any other.
		{status: StatusCommit, eventType: "transaction.unknown"},
	} {
		t.Run(testCase.status, func(t *testing.T) {
			assert.Equal(t, testCase.eventType, getEventFromStatus(testCase.status),
				"the %q status must produce the %q event", testCase.status, testCase.eventType)

			// The event name produced from the status must be one the catalogue knows.
			// This is the join: a name that routes correctly but is not in the catalogue
			// is a name nobody asserted, and a name in neither is an event that silently
			// reaches the catch-all.
			assert.Contains(t, catalogueEventTypes(), testCase.eventType,
				"the event name produced from the %q status must appear in the catalogue", testCase.status)

			assert.Equal(t, "blnk.transactions", TopicForEvent(getEventFromStatus(testCase.status)),
				"every transaction status must route to the transactions topic, including %q", testCase.status)
			assert.Equal(t, "blnk.transactions.dlt", DeadLetterTopicForEvent(getEventFromStatus(testCase.status)),
				"and to the transactions dead-letter sibling on exhaustion, including %q", testCase.status)
		})
	}

	// The status mapping is case-insensitive, so a lower-cased status must produce the
	// same event and route identically. Asserting it here prevents a future normalisation
	// change from quietly sending correctly-cased-but-unexpected statuses to the
	// catch-all.
	assert.Equal(t, "transaction.applied", getEventFromStatus(strings.ToLower(StatusApplied)))
	assert.Equal(t, "blnk.transactions", TopicForEvent(getEventFromStatus(strings.ToLower(StatusApplied))))

	// A status nobody defined still yields a routable transaction event rather than
	// nothing, which is what keeps an unrecognised status from stranding its event.
	assert.Equal(t, "transaction.unknown", getEventFromStatus("NO_SUCH_STATUS"))
	assert.Equal(t, "blnk.transactions", TopicForEvent(getEventFromStatus("NO_SUCH_STATUS")))
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

	// The catalogue supplies the inputs deliberately, and it is safe for it to do so:
	// the EXPECTATION here is computed from model.EventCategory, not from the catalogue,
	// so this test asserts the composition rule rather than the mapping values. Driving
	// it from the catalogue also means a fourteenth event type inherits this check for
	// free once its row is added.
	for _, eventType := range append(catalogueEventTypes(), "bulk_transaction.failed") {
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
	assert.False(t, model.IsCataloguedEventType("bulk_transaction"),
		"and it must be reported as uncatalogued, which is what makes the mis-spelling visible instead of looking like a routed event")
}

// TestTopicForEvent_UnrecognisedEventRoutesToTheSystemTopic pins the chosen behaviour for
// an event type that is not in the catalogue, and pins WHICH topic it is.
//
// Two requirements meet here, and both must hold of the one destination:
//
//   - The routing must be TOTAL. The relay publishes to whatever topic it is handed, so
//     returning an empty string would strand a committed event — no topic, no publish,
//     no dead-letter entry, nothing to replay — and dropping it would lose it outright.
//   - The destination must be the least surprising one. An event type nobody mapped must
//     not appear in the middle of a domain stream a subscriber filters on and reasons
//     about; it belongs with Blnk's own material, on the topic an operator grants
//     deliberately.
//
// blnk.system satisfies both, and the fallback stays OBSERVABLE rather than becoming a
// hiding place: model.IsCataloguedEventType reports the same input as uncatalogued and the
// publisher logs at warning level when it routes one, so the fix is always to extend the
// catalogue.
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
		// Unusual characters. None of these is a realistic event name, and that is the
		// point: routing has to be TOTAL. Anything a caller can hand this function must
		// come back as a usable topic, because the relay publishes to whatever name it is
		// given and has no branch for "no topic". A panic, an empty string or a
		// half-composed name here would each strand the event, so the awkward inputs are
		// asserted rather than assumed.
		"événement.créé",             // non-ASCII letters
		"事件.已创建",                     // multi-byte, no ASCII at all
		"transaction.applied.💥",      // an emoji outside the Basic Multilingual Plane
		"!@#$%^&*()",                 // punctuation only
		"...",                        // separators only, which must not compose an empty segment
		".",                          // a lone separator
		"transaction\n.applied",      // an embedded newline, as a mangled environment value might carry
		"transaction\t.applied",      // an embedded tab
		"transaction\x00.applied",    // an embedded NUL, which Kafka rejects but naming must survive
		"  transaction.applied  ",    // surrounded by whitespace, so NOT the emitted string
		"transaction.applied\u200b",  // a zero-width space, invisible in a diff
		strings.Repeat("very.", 200), // longer than Kafka's 249-character topic limit
	} {
		assert.Equal(t, "blnk.system", TopicForEvent(eventType),
			"unrecognised event type %q must route to the system topic, the catalogue's catch-all", eventType)
		assert.False(t, model.IsCataloguedEventType(eventType),
			"unrecognised event type %q must be reported as uncatalogued, which is what raises the capture-time warning naming the producer to fix", eventType)
		assert.NotEmpty(t, TopicForEvent(eventType),
			"TopicForEvent must never return an empty string: the relay would strand the event")
		assert.True(t, IsBlnkOwnedTopic(TopicForEvent(eventType)),
			"the catch-all topic must be one Blnk owns and provisions, or an unrecognised event has nowhere to land")

		// The event's own name must never leak into the topic name. Composing the event
		// into the topic would create a topic per event type on demand — unprovisioned,
		// single-partition, wrongly replicated, and read by nobody.
		assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent(eventType),
			"and its dead-letter sibling must be the system topic's, not one derived from %q", eventType)
	}
}

// TestDLTFor_DerivesEveryDeadLetterTopic asserts the exact dead-letter names for
// every category topic.
//
// Three of these four strings are the literal names the requirements specify, so they
// must match byte for byte. They are also the names the local provisioning path creates,
// the names the alert rules label and the names the operator runbooks tell a human to
// look at, which is why they are written out here rather than derived.
func TestDLTFor_DerivesEveryDeadLetterTopic(t *testing.T) {
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

// TestAllTopics_IsEveryCategoryTopicInCanonicalOrder pins the publish-side inventory,
// exactly and in order.
//
// Order is part of the contract, not a detail: the provisioning path and the operator
// documentation list these names in this order so the two can be diffed line for line.
func TestAllTopics_IsEveryCategoryTopicInCanonicalOrder(t *testing.T) {
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

// TestAllDeadLetterTopics_IsEverySiblingInCanonicalOrder pins the dead-letter
// inventory and its index alignment with AllTopics.
//
// The alignment lets a caller zip the two slices to pair a topic with its sibling, which
// the admin path and the triage runbook both rely on.
func TestAllDeadLetterTopics_IsEverySiblingInCanonicalOrder(t *testing.T) {
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

// TestAllTopicsWithDeadLetters_IsTheProvisionedInventory pins the complete inventory —
// the single source of truth shared by the Go admin path and the provisioning script.
//
// These names, in this order, are what the provisioning script creates; the parity itself is
// enforced by TestKafkaProvisionScript_ProvisionsEveryCategoryTheCodeOwns. If this list and
// the provisioning script ever disagree, the failure is silent in the worst way: the
// script creates topics the code never writes to, and the code writes to topics the script
// never created, which either fails on an unprovisioned topic or auto-creates one with a
// single partition and the wrong replication factor — quietly discarding the ordering and
// durability guarantees.
func TestAllTopicsWithDeadLetters_IsTheProvisionedInventory(t *testing.T) {
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
	assert.Len(t, seen, 2*len(EventCategories()),
		"the inventory is one category topic plus one dead-letter sibling per category")

	// Exactly half the inventory is dead-letter topics, so a category that lost its
	// sibling — or gained a second one — is caught even if the total still came out right.
	deadLetters := 0
	for _, topic := range AllTopicsWithDeadLetters() {
		if IsDeadLetterTopic(topic) {
			deadLetters++
		}
	}
	assert.Equal(t, len(EventCategories()), deadLetters,
		"every category topic must contribute exactly one dead-letter sibling")
}

// TestTopicInventory_ContainsNoEmptyOrMalformedName is the guard against a name that is
// present in the inventory but unusable.
//
// The list equality assertions above prove the names are right for the default prefix, and
// for the one override they exercise. This test asserts the structural properties for every
// accessor under both, because the inventory is consumed by topic creation and by
// provisioning: an empty entry would be an attempt to create a topic with no name, and an
// entry with a stray separator or surrounding whitespace would create a topic adjacent to
// the intended one that nothing reads.
//
// It is deliberately about SHAPE, not about specific names — the specific names are pinned
// by the exact list comparisons — so it holds for any configured namespace.
func TestTopicInventory_ContainsNoEmptyOrMalformedName(t *testing.T) {
	for _, prefix := range []string{"", "acme.events"} {
		storeKafkaTopicPrefix(t, prefix)

		inventories := map[string][]string{
			"AllTopics":                AllTopics(),
			"AllDeadLetterTopics":      AllDeadLetterTopics(),
			"AllTopicsWithDeadLetters": AllTopicsWithDeadLetters(),
			"EventCategories":          EventCategories(),
		}

		for name, entries := range inventories {
			assert.NotEmpty(t, entries, "%s must not be empty", name)

			for i, entry := range entries {
				assert.NotEmpty(t, entry,
					"%s[%d] is empty; naming a topic nothing is not a name", name, i)
				assert.NotEmpty(t, strings.TrimSpace(entry),
					"%s[%d] = %q is only whitespace", name, i, entry)
				assert.Equal(t, strings.TrimSpace(entry), entry,
					"%s[%d] = %q carries surrounding whitespace, which Kafka rejects in a topic name", name, i, entry)
				assert.False(t, strings.HasPrefix(entry, "."),
					"%s[%d] = %q starts with the separator, so its first segment is empty", name, i, entry)
				assert.False(t, strings.HasSuffix(entry, "."),
					"%s[%d] = %q ends with the separator, so its last segment is empty", name, i, entry)
				assert.NotContains(t, entry, "..",
					"%s[%d] = %q contains a doubled separator, which names a topic adjacent to the intended one", name, i, entry)
			}
		}

		// The two halves must partition the whole: no category topic may appear among the
		// dead-letter topics and vice versa.
		for _, topic := range AllTopics() {
			assert.NotContains(t, AllDeadLetterTopics(), topic,
				"category topic %q must not also appear as a dead-letter topic", topic)
		}
	}
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

// TestEventCategories_IsTheCanonicalTokenListInOrder pins the category enumeration.
//
// The tokens are compared against the model constants they are built from, so the
// enumeration cannot drift from the vocabulary, and the literal list pins the ORDER —
// which the provisioning script and the operator documentation are diffed against.
//
// There is exactly ONE declaration of this list, in model.AllEventCategories. It used to
// be declared twice, here and in model, and a second copy of an enumeration is a second
// thing to forget: adding a category to the routing table without adding it to the topic
// inventory produces a topic that events route to and that nothing provisions.
func TestEventCategories_IsTheCanonicalTokenListInOrder(t *testing.T) {
	assert.Equal(t, []string{
		"transactions",
		"balances",
		"identities",
		"system",
	}, EventCategories(),
		"the topic contract is these four categories and the eight topics composed from them")

	assert.Equal(t, []string{
		model.EventCategoryTransactions,
		model.EventCategoryBalances,
		model.EventCategoryIdentities,
		model.EventCategorySystem,
	}, EventCategories(),
		"the enumeration must be built from the model constants, not from re-spelled literals")

	assert.Equal(t, model.AllEventCategories(), EventCategories(),
		"the topic layer must delegate to the single category declaration in model rather than keep a copy")

	// The accessor must hand back a copy: one caller sorting the result must not
	// reorder the inventory for the provisioning path.
	mutated := EventCategories()
	mutated[0] = "mutated"
	assert.NotContains(t, EventCategories(), "mutated")
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

// TestTopicForCategory_BlankCategoryFallsBackToTheSystemTopic pins the guard against
// composing a name with an empty final segment, and pins where the fallback goes.
//
// "<prefix>." is a topic Kafka would accept and nothing would read, so a blank category
// must still compose a usable name — the same never-drop-an-event policy the event
// mapping's own catch-all implements. It falls back to the SYSTEM topic, which is the
// catch-all model.EventCategory itself uses, so an event the caller could not classify
// lands on a topic that is provisioned, monitored and has a dead-letter sibling.
func TestTopicForCategory_BlankCategoryFallsBackToTheSystemTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, blank := range []string{"", " ", "\t", ".", " . ", "\n"} {
		assert.Equal(t, "blnk.system", TopicForCategory(blank),
			"a blank category %q must fall back to the system topic, never compose %q", blank, "blnk.")
		assert.True(t, IsBlnkOwnedTopic(TopicForCategory(blank)),
			"an unclassifiable event must land on a topic this deployment owns and provisions")
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

// TestSubscriberGrantableTopics_ExcludesDeadLetters is the test for the authorization
// allowlist, and it is the one that stops an over-broad grant.
//
// Before the allowlist existed, both the request-validation layer and the ACL
// provisioning simply trimmed whatever topic list the caller supplied and granted what
// remained. An authorized request could therefore name the literal wildcard, a
// dead-letter topic, or a topic belonging to another system entirely, and receive a real
// ACL binding over it. Every rejected value below is one of those.
func TestSubscriberGrantableTopics_ExcludesDeadLetters(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	// FOUR topics: the whole category inventory, and no dead-letter sibling.
	//
	// blnk.system is HERE, and it is the entry that matters. It carries ledger.created and
	// system.error, both of which the legacy webhook delivered, so an allowlist without it
	// would leave two event types published and unreachable — a consuming-side regression
	// rather than a tightening. What a grant of it discloses is system.error's frozen body,
	// so which subscribers hold it is an operator decision recorded per subscriber in
	// authorized_topics; this list is only what MAY be granted.
	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, SubscriberGrantableTopics(),
		"every category topic may be granted to a subscriber, and no dead-letter sibling may")

	for _, topic := range SubscriberGrantableTopics() {
		assert.True(t, IsSubscriberGrantableTopic(topic),
			"%q is listed as grantable, so the predicate must accept it", topic)
		assert.False(t, IsDeadLetterTopic(topic),
			"%q must not be a dead-letter topic: a .dlt record carries Blnk's failure metadata, which is operator detail rather than subscriber data", topic)
	}

	refused := map[string]string{
		"the transactions dead-letter topic":                "blnk.transactions.dlt",
		"the balances dead-letter topic":                    "blnk.balances.dlt",
		"the identities dead-letter topic":                  "blnk.identities.dlt",
		"the system dead-letter topic":                      "blnk.system.dlt",
		"a ledgers topic this contract has no category for": "blnk.ledgers",
		"the literal wildcard":                              "*",
		"a wildcard suffix":                                 "blnk.*",
		"a single-character wildcard":                       "blnk.transaction?",
		"an empty topic":                                    "",
		"a whitespace-only topic":                           "   ",
		"a foreign topic":                                   "someone-elses.topic",
		"a Kafka internal topic":                            "__consumer_offsets",
		"the KRaft metadata topic":                          "__cluster_metadata",
		"a near-miss with a suffix":                         "blnk.transactions.something-else",
		"a near-miss with a prefix":                         "not-blnk.transactions",
		"an upper-cased grantable topic":                    "BLNK.TRANSACTIONS",
		"a mixed-case grantable topic":                      "Blnk.Transactions",
		"a topic with an embedded newline":                  "blnk.transactions\nblnk.quarantine",
		"a topic with an embedded NUL":                      "blnk.transactions\x00",
	}
	for name, topic := range refused {
		assert.False(t, IsSubscriberGrantableTopic(topic),
			"%s (%q) must not be grantable to a subscriber", name, topic)
	}

	// Trailing and leading whitespace on an otherwise valid name IS tolerated, because
	// an operator pasting a topic name is a routine mistake and the trimmed value is
	// unambiguous. Nothing else is normalised.
	assert.True(t, IsSubscriberGrantableTopic("  blnk.transactions  "),
		"surrounding whitespace on an exact match is trimmed, which is the only normalisation applied")

	// The allowlist must follow the configured namespace, or a non-default deployment
	// could grant nothing at all.
	storeKafkaTopicPrefix(t, "acme.events")
	assert.Equal(t, []string{
		"acme.events.transactions",
		"acme.events.balances",
		"acme.events.identities",
		"acme.events.system",
	}, SubscriberGrantableTopics())
	assert.True(t, IsSubscriberGrantableTopic("acme.events.transactions"))
	assert.False(t, IsSubscriberGrantableTopic("blnk.transactions"),
		"a topic from a different namespace must not be grantable just because it looks Blnk-owned")
}

// TestIsBlnkOwnedTopic_CoversTheWholeInventoryAndNothingElse pins the broader check the
// publisher and the repository apply.
//
// The two predicates answer different questions and must not be confused:
// IsBlnkOwnedTopic answers "may WE write here?", which includes the dead-letter names
// because Blnk legitimately writes to all of them; IsSubscriberGrantableTopic answers
// "may a SUBSCRIBER read here?", which does not.
func TestIsBlnkOwnedTopic_CoversTheWholeInventoryAndNothingElse(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, topic := range AllTopicsWithDeadLetters() {
		assert.True(t, IsBlnkOwnedTopic(topic),
			"%q is in the inventory, so the publisher must be allowed to write to it", topic)
	}

	// Every grantable topic is Blnk-owned; the converse is deliberately false.
	for _, topic := range SubscriberGrantableTopics() {
		assert.True(t, IsBlnkOwnedTopic(topic))
	}
	// The system topic is owned and grantable both: Blnk writes ledger.created,
	// system.error and every unrecognised event type to it, and a subscriber may be granted
	// it because the first two are event types the legacy transport delivered.
	assert.True(t, IsBlnkOwnedTopic("blnk.system"),
		"Blnk writes to the system topic, so it must be inside the owned inventory")
	assert.True(t, IsSubscriberGrantableTopic("blnk.system"),
		"and a subscriber may be granted it, or ledger.created and system.error are published and unreachable")

	// And a DEAD-LETTER topic is Blnk-owned and never grantable, whichever category it
	// belongs to: a .dlt record carries other subscribers' failed events plus failure
	// metadata naming broker addresses and internal error text.
	assert.True(t, IsBlnkOwnedTopic("blnk.system.dlt"))
	assert.False(t, IsSubscriberGrantableTopic("blnk.system.dlt"))
	for _, topic := range AllDeadLetterTopics() {
		assert.True(t, IsBlnkOwnedTopic(topic), "%q is Blnk's to write", topic)
		assert.False(t, IsSubscriberGrantableTopic(topic), "%q must never be grantable", topic)
	}

	// THE INVENTORY IS CLOSED AT THE FOUR CATALOGUED CATEGORIES, so a plausible-looking
	// fifth name is refused rather than tolerated. Two are worth watching: "blnk.quarantine",
	// because an earlier design routed uncatalogued events to a topic of that name, and
	// "blnk.ledgers", because a fifth ledgers category was implemented and reverted. Admitting
	// a name Blnk never creates would let the ACL pruner treat another team's bindings on it as
	// its own to delete.
	for _, topic := range []string{
		"", "   ", "*", "blnk", "blnk.", "blnk.orders", "__consumer_offsets",
		"blnk.transactions.dlt.dlt", "blnk.transactions.replayed", "BLNK.TRANSACTIONS",
		"blnk.quarantine", "blnk.quarantine.dlt", "blnk.ledger",
		"blnk.ledgers", "blnk.ledgers.dlt",
	} {
		assert.False(t, IsBlnkOwnedTopic(topic),
			"%q is not in the inventory and must not be writable: a stored or replayed row must not be able to steer the publisher at an arbitrary topic", topic)
	}
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
// getEventFromStatus must be relocated into event_topics.go before webhooks.go is deleted,
// because it produces seven of the thirteen event strings — the vocabulary this file routes.
// That fact lives in a comment, so nothing but a test can stop it being deleted, and losing
// it means a future sunset deletes the vocabulary along with its host file.
//
// The test also asserts the function is NOT yet declared here: a premature relocation would
// be a duplicate declaration in package blnk and would fail the build outright, so this
// assertion exists to name the reason rather than to catch it late.
func TestEventTopicsSource_RecordsTheSunsetRelocationOfGetEventFromStatus(t *testing.T) {
	parsed := parseEventTopicsSource(t)

	var comments strings.Builder
	for _, group := range parsed.Comments {
		comments.WriteString(group.Text())
	}
	documentation := comments.String()

	// Each fragment is one half of the record: the symbol that must move, the fact that it
	// must outlive its host file, and the pointer to where the ordered procedure is kept.
	// Losing any one of them leaves a reader with no way back to the other two.
	//
	// Each is checked with strings.Contains behind assert.True rather than assert.Contains so
	// that a failure reports the missing fragment instead of dumping the whole file's
	// documentation — roughly twenty thousand characters — into the test output.
	for _, fragment := range []string{
		"getEventFromStatus",
		"must outlive that file",
		"sunset block at the foot of webhooks.go",
	} {
		assert.True(t, strings.Contains(documentation, fragment),
			"event_topics.go's documentation must still contain %q; it is the only record that getEventFromStatus must be moved here before webhooks.go is deleted", fragment)
	}

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

// ---------------------------------------------------------------------------------------
// Consequences of topic ownership — VALID-01 and DATA-01
//
// The rest of this file establishes WHICH topics Blnk owns. These tests cover what follows
// from that answer at the two places it is load-bearing outside naming: the publisher's
// refusal to write to a topic outside the namespace, and the bounding of stored strings
// before they become metric labels.
//
// They live here rather than beside the publisher because event_publisher_test.go belongs to
// a later checkpoint and is not in this scope, and because both behaviours are questions
// about ownership — this file is the authority on that. None of them performs I/O: a
// kafka.Writer is constructed lazily and no case below reaches a write.
// ---------------------------------------------------------------------------------------

// newOwnershipPublisher builds a real Kafka-backed publisher with no broker contact.
//
// newKafkaPublisher assembles a transport and one writer per owned topic, all of which are
// pure construction — kafka-go dials on first use, and no test here uses one.
func newOwnershipPublisher(t *testing.T) *kafkaPublisher {
	t.Helper()

	publisher, err := newKafkaPublisher([]string{"localhost:9092"}, config.KafkaConfig{
		Brokers:          []string{"localhost:9092"},
		TopicPrefix:      DefaultTopicPrefix,
		InsecureLocalDev: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Close() })

	return publisher
}

// TestWriterFor_RefusesATopicBlnkDoesNotOwn is the VALID-01 guard on the publish path.
//
// The destination of a publish comes from a STORED OUTBOX ROW. Writer creation used to grow
// lazily with no membership test, so ANY topic name that reached the table was created as a
// writer and published to — using Blnk's own producer credentials, on Blnk's own broker, at
// the direction of stored data. A row carrying "attacker.transactions", or a name with an
// injected segment, was delivered exactly as asked.
//
// Persistence now rejects such a row at insert, and this is the second half of that defence:
// a row would have to bypass validation AND survive this check to reach a foreign topic. The
// refusal is asserted to happen WITHOUT caching the writer, because a rejected name that
// entered the map would be admitted by the fast path on the next attempt.
func TestWriterFor_RefusesATopicBlnkDoesNotOwn(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := newOwnershipPublisher(t)
	owned := len(publisher.writers)

	for _, topic := range []string{
		"attacker.transactions",
		"blnkfinance.transactions",
		"transactions",
		"blnk",
		"BLNK.transactions",
		" blnk.transactions",
		"blnk.transactions\n",
		"__consumer_offsets",
		"legacy.transactions",
		"legacy.transactions.dlt",
	} {
		writer, err := publisher.writerFor(topic)
		require.Error(t, err, "writerFor(%q) must be refused", topic)
		assert.Nil(t, writer)
		assert.ErrorIs(t, err, ErrTopicNotOwned,
			"the refusal must be recognisable without matching message text")
		assert.Len(t, publisher.writers, owned,
			"a refused topic %q must not be cached, or the fast path would admit it next time", topic)
	}
}

// TestWriterFor_ServesEveryPreCreatedTopic covers the fast path.
//
// The pre-created set comes from AllTopicsWithDeadLetters — Blnk's own configuration — and under
// a fixed prefix it is already the whole owned namespace, since ownership is exactly
// prefix.<known-category> and its `.dlt` sibling. Every one must be served, and the writer must
// carry the topic it was asked for: a per-topic writer bound to the wrong topic would publish
// silently to the wrong audience, which no runtime error would reveal.
func TestWriterFor_ServesEveryPreCreatedTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	publisher := newOwnershipPublisher(t)
	require.NotEmpty(t, publisher.writers, "the publisher must pre-create its inventory")

	for _, topic := range AllTopicsWithDeadLetters() {
		writer, err := publisher.writerFor(topic)
		require.NoError(t, err, "an owned topic %q must be served", topic)
		require.NotNil(t, writer)
		assert.Equal(t, topic, writer.Topic)
	}
}

// TestWriterFor_GrowsForANewPrefixAndKeepsServingTheOld covers the ONE situation in which lazy
// growth is reachable, and the reason it does not strand a committed event.
//
// The prefix is re-read from live configuration on every naming call, so a reload starts
// resolving events to names this publisher was never constructed with. Those are owned under the
// new prefix, so they are admitted and cached — and asserting the cache grew by exactly one, and
// that a second call returns the SAME writer, is what proves growth is shared rather than
// per-publish.
//
// Meanwhile a row stored before the change still names a pre-created topic, so it is still
// served. That is the case the membership test must not break: the row recorded its destination
// precisely so the event would reach the topic it was bound for, and the process holds a writer
// for it built from its own configuration.
func TestWriterFor_GrowsForANewPrefixAndKeepsServingTheOld(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)
	publisher := newOwnershipPublisher(t)
	before := len(publisher.writers)

	storeKafkaTopicPrefix(t, "renamed")

	grown, err := publisher.writerFor("renamed.transactions")
	require.NoError(t, err, "the new prefix is Blnk's namespace and must be admitted")
	require.NotNil(t, grown)
	assert.Equal(t, "renamed.transactions", grown.Topic)
	assert.Len(t, publisher.writers, before+1, "and cached, so the next publish takes the fast path")

	again, err := publisher.writerFor("renamed.transactions")
	require.NoError(t, err)
	assert.Same(t, grown, again, "two publishes to one topic must share one writer")

	stored, err := publisher.writerFor("blnk.transactions")
	require.NoError(t, err,
		"a row stored before the prefix change names a pre-created topic and must still publish")
	assert.Equal(t, "blnk.transactions", stored.Topic)

	// A prefix change does not widen the namespace: a foreign name is refused under either.
	_, err = publisher.writerFor("attacker.transactions")
	assert.ErrorIs(t, err, ErrTopicNotOwned)
}

// TestBoundedTopicLabel_CollapsesEveryNameBlnkDoesNotOwn is the DATA-01 cardinality guard.
//
// Every value that reaches this label arrives from an outbox row, and the FAILURE path records
// an attempt for a topic that was rejected — precisely the case where the name is not Blnk's.
// An unbounded label is two defects at once: each distinct value creates a time series, so a
// stream of odd names is a storage attack on the metrics pipeline with no request rate to limit;
// and the label publishes whatever string was stored to anyone who can read /metrics.
func TestBoundedTopicLabel_CollapsesEveryNameBlnkDoesNotOwn(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	for _, topic := range AllTopicsWithDeadLetters() {
		assert.Equal(t, topic, boundedTopicLabel(topic),
			"an owned topic is reported verbatim, which is what the per-topic queries need")
	}

	for _, topic := range []string{
		"attacker.transactions",
		"",
		"blnk",
		strings.Repeat("x", 4096),
		"blnk.transactions; DROP TABLE",
		"legacy.transactions",
	} {
		assert.Equal(t, unownedTopicLabel, boundedTopicLabel(topic),
			"an unowned topic %q must collapse to one fixed label", topic)
	}
}

// TestBoundedEventTypeLabel_CollapsesEveryUncataloguedType is the other half of the same guard.
//
// The catalogue is the bound: a recognised event type is one of a fixed set this repository
// emits. An unrecognised one is a name that could be arbitrary — a stored row from a producer
// that was never catalogued.
//
// Collapsing does not hide the condition: a non-zero count on the collapsed label is the signal
// that something is publishing an uncatalogued event, and the capture-time warning names it.
func TestBoundedEventTypeLabel_CollapsesEveryUncataloguedType(t *testing.T) {
	for _, eventType := range []string{
		"transaction.queued", "transaction.applied", "transaction.scheduled",
		"transaction.inflight", "transaction.void", "transaction.rejected",
		"transaction.unknown", "bulk_transaction.applied", "bulk_transaction.failed",
		"balance.created", "balance.monitor", "identity.created",
		"ledger.created", "system.error",
	} {
		assert.Equal(t, eventType, boundedEventTypeLabel(eventType),
			"a catalogued event type is reported verbatim")
	}

	for _, eventType := range []string{
		"", "not.an.event", "Transaction.Applied", " transaction.applied",
		strings.Repeat("y", 4096), "transaction.applied\n",
	} {
		assert.Equal(t, unrecognisedEventTypeLabel, boundedEventTypeLabel(eventType),
			"an uncatalogued event type %q must collapse to one fixed label", eventType)
	}
}

// TestKafkaProvisionScript_ProvisionsEveryCategoryTheCodeOwns pins the parity between the
// Go topic catalogue and the shell script that provisions it.
//
// # Why this test exists rather than a comment
//
// event_admin.go states that AllTopicsWithDeadLetters is "the single source of truth shared
// with scripts/kafka-provision.sh", and the script states that its own list "is
// eventCategoryOrder". Both claims were true when written and neither was enforced, so when
// a category was added to the Go side the script kept provisioning the older, shorter set —
// and nothing anywhere failed. The drift is invisible in every unit test, invisible in every
// build, and visible only against a real broker, as a publish to a topic that does not
// exist.
//
// # Why a missing topic is not a cosmetic problem
//
// Against a broker with auto.create.topics.enable=false — the correct production setting,
// because auto-creation would silently manufacture a topic with the wrong partition count
// and the wrong replication factor — a publish to an unprovisioned topic FAILS. The row
// retries until its budget is spent and the dead-letter write then fails too, because the
// missing category's `.dlt` sibling is missing for the same reason. The event is stranded.
//
// That lands hardest on the system category, which is the one whose provisioning is easiest
// to think optional: besides ledger.created and system.error it is where an event type the
// mapping table does not recognise is routed, which is the whole of the zero-exceptions
// coverage guarantee. An unprovisioned system topic turns that guarantee into its opposite.
func TestKafkaProvisionScript_ProvisionsEveryCategoryTheCodeOwns(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(moduleRootDir(t), "scripts", "kafka-provision.sh"))
	require.NoError(t, err, "scripts/kafka-provision.sh must be readable to compare its catalogue")

	declaration := regexp.MustCompile(`(?m)^readonly EVENT_CATEGORIES=\(([^)]*)\)`)
	match := declaration.FindSubmatch(script)
	require.NotNil(t, match,
		"the script must declare its catalogue as `readonly EVENT_CATEGORIES=(...)`; if that "+
			"declaration is renamed or restructured, update this test rather than deleting it — "+
			"the parity it enforces is what keeps a publish from reaching a topic nobody created")

	scriptCategories := strings.Fields(string(match[1]))
	assert.Equal(t, model.AllEventCategories(), scriptCategories,
		"the script's categories must match model.AllEventCategories EXACTLY, including ORDER: "+
			"the script's own comment names eventCategoryOrder as canonical, and the ordering "+
			"fixes the order of AllTopics, AllDeadLetterTopics and AllTopicsWithDeadLetters that "+
			"provisioning is compared against")

	t.Run("every category is provisioned", func(t *testing.T) {
		// Provisioned and grantable are different questions, and the script answers the first
		// for every category without consulting the second. Conflating the two is how a topic
		// that events are routed to goes uncreated.
		for _, category := range model.AllEventCategories() {
			assert.Contains(t, scriptCategories, category,
				"%q must be provisioned, because events route to it whatever any subscriber may be granted", category)
		}
	})

	t.Run("the script's grantable set matches the code's", func(t *testing.T) {
		// The script grants its sample principal a DEFAULT topic list, and a second, hand-kept
		// copy of the allowlist is a second thing to forget. This is what keeps that list equal
		// to the one the API and the ACL provisioner use, in the same canonical order.
		declaration := regexp.MustCompile(`(?m)^readonly SUBSCRIBER_GRANTABLE_CATEGORIES=\(([^)]*)\)`)
		match := declaration.FindSubmatch(script)
		require.NotNil(t, match,
			"the script must declare `readonly SUBSCRIBER_GRANTABLE_CATEGORIES=(...)`, because its "+
				"sample-subscriber grant is derived from it")

		assert.Equal(t, model.SubscriberGrantableEventCategories(), strings.Fields(string(match[1])),
			"the sample principal's default grant must be exactly the categories a real subscriber "+
				"may be granted, in the same order: a category grantable in one and not the other means "+
				"the script mints a grant the API would refuse, or refuses one the API would allow")
	})

	t.Run("the dead-letter suffix agrees too", func(t *testing.T) {
		// The script derives the `.dlt` names rather than listing them, so the suffix is the
		// other half of the catalogue and drifts just as silently.
		suffix := regexp.MustCompile(`(?m)^readonly DEAD_LETTER_SUFFIX="([^"]*)"`).FindSubmatch(script)
		require.NotNil(t, suffix, "the script must declare DEAD_LETTER_SUFFIX")
		assert.Equal(t, DeadLetterTopicSuffix, string(suffix[1]),
			"the script's dead-letter suffix must equal DeadLetterTopicSuffix, which is the published "+
				"<topic>.dlt convention subscribers are told to stay clear of")
	})

	t.Run("the whole inventory is derivable from the script's two declarations", func(t *testing.T) {
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		derived := make([]string, 0, len(scriptCategories)*2)
		for _, category := range scriptCategories {
			derived = append(derived, DefaultTopicPrefix+"."+category)
		}
		for _, category := range scriptCategories {
			derived = append(derived, DefaultTopicPrefix+"."+category+DeadLetterTopicSuffix)
		}

		assert.Equal(t, AllTopicsWithDeadLetters(), derived,
			"the topics the script creates must be exactly the topics the publisher writes to and "+
				"the admin client assures")
	})
}

// TestKafkaProvisionScript_GrantsOnlyTheCategoriesTheCodeAllows pins the parity between the
// Go GRANT allowlist and the shell script that mints the sample subscriber's ACLs.
//
// # The defect this replaces
//
// An operator-supplied override used to be accepted verbatim, with no allowlist at all, so
// KAFKA_SAMPLE_SUBSCRIBER_TOPICS could name a dead-letter topic or a topic belonging to
// another system entirely and receive a real ACL binding. model.SubscriberGrantableTopics
// refuses both, and the subscriber DTO validation and the Kafka ACL request check against it,
// so the script was minting grants the API would have refused: the same principal requested
// through POST /subscribers/{id}/kafka-credentials could not have obtained them.
//
// # Why an over-granted sample principal is not merely untidy
//
// The dead-letter siblings hold the full payload of every event Blnk failed to publish, for
// every subscriber, together with failure metadata naming broker addresses and internal error
// reasons — and they are what the isolation acceptance criterion asserts a principal CANNOT
// read. A principal granted one makes that assertion vacuous.
//
// # Why this test runs the script instead of reading it
//
// The catalogue half of this parity is checkable by reading a declaration, because a
// declaration is what it is. The GRANT half is a decision made by code, and a test that
// grepped for the code would agree with whatever the code said — including with a broken
// version of it. So the refusals below EXECUTE scripts/kafka-provision.sh and assert on what
// it does. Every case stops in resolve_subscriber_topics, which runs before any network call,
// so no broker, no credential and no Kafka CLI is involved.
func TestKafkaProvisionScript_GrantsOnlyTheCategoriesTheCodeAllows(t *testing.T) {
	root := moduleRootDir(t)
	scriptPath := filepath.Join(root, "scripts", "kafka-provision.sh")

	require.FileExists(t, scriptPath, "scripts/kafka-provision.sh must exist to be executed")

	// Each case is a topic the script must refuse to grant, paired with why refusing it
	// matters. The prefix is deliberately NOT the default, so the refusal also proves the
	// allowlist is resolved under the configured prefix rather than against hard-coded names.
	const prefix = "acme"

	refusals := map[string]string{
		"a dead-letter sibling":              prefix + ".transactions.dlt",
		"the system dead-letter sibling":     prefix + ".system.dlt",
		"a topic under a different prefix":   "other.transactions",
		"a retired category name":            prefix + ".ledgers",
		"a topic that does not exist at all": prefix + ".invented",
		// A category this contract does not have. It is named explicitly because a ledgers
		// topic is the plausible mistake: ledger events are real and are published, but they
		// are published to the system topic, so "acme.ledgers" is a name nothing creates.
		"a category this contract does not have": prefix + ".ledgers",
	}

	for name, topic := range refusals {
		t.Run("refuses "+name, func(t *testing.T) {
			stdout, exitCode := runKafkaProvisionDecisionPhase(t, scriptPath, map[string]string{
				"KAFKA_TOPIC_PREFIX":             prefix,
				"KAFKA_SAMPLE_SUBSCRIBER_TOPICS": topic,
			})

			assert.NotEqual(t, 0, exitCode,
				"the script must FAIL rather than silently narrow or silently widen the grant")
			assert.Contains(t, stdout, "no subscriber may be granted",
				"the refusal must say what is wrong; output was:\n%s", stdout)
			assert.Contains(t, stdout, topic,
				"the refusal must name the offending topic, or an operator cannot tell which entry "+
					"of a comma-separated list was rejected")

			// The message lists the allowlist, and that list is the same array the DEFAULT
			// grant is built from — so asserting it here also pins the default.
			for _, allowed := range model.SubscriberGrantableTopics(prefix) {
				assert.Contains(t, stdout, allowed,
					"the refusal must name every grantable topic so the remedy is readable without "+
						"consulting the source")
			}
		})
	}

	t.Run("accepts a subset of the grantable topics", func(t *testing.T) {
		// The complement of the refusals, and the reason they are not vacuous: a script that
		// refused EVERY override would pass all of the cases above while granting nothing.
		grantable := model.SubscriberGrantableTopics(prefix)
		require.GreaterOrEqual(t, len(grantable), 2, "the subset case needs at least two grantable topics")

		stdout, exitCode := runKafkaProvisionDecisionPhase(t, scriptPath, map[string]string{
			"KAFKA_TOPIC_PREFIX":             prefix,
			"KAFKA_SAMPLE_SUBSCRIBER_TOPICS": strings.Join(grantable[:2], ","),
		})

		// It still exits non-zero, because the run continues past the grant decision and then
		// finds no credential and no broker — which is the point: it got PAST the allowlist.
		assert.NotEqual(t, 0, exitCode, "the run is expected to stop later, for want of a broker")
		assert.NotContains(t, stdout, "no subscriber may be granted",
			"a subset of the allowlist must be accepted; output was:\n%s", stdout)
	})
}

// runKafkaProvisionDecisionPhase executes scripts/kafka-provision.sh far enough to reach its
// pure-decision phase, and returns its combined output and exit code.
//
// The environment is built from EMPTY rather than inherited. A developer with KAFKA_BROKERS,
// a real KAFKA_SASL_ADMIN_SECRET or a KAFKA_CLIENT_CONFIG exported would otherwise run a
// different script than CI does — and in the worst case would reach the network and start
// provisioning a real broker from a unit test. Only PATH and the case's own variables are
// passed, and KAFKA_CONTAINER names a container that cannot exist so the delegation branch
// fails immediately instead of finding a live broker.
//
// KAFKA_BOOTSTRAP_SERVER is set because the script's very first action is to skip entirely
// when Kafka is unconfigured; without it every case would exit 0 having decided nothing.
func runKafkaProvisionDecisionPhase(t *testing.T, scriptPath string, env map[string]string) (string, int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, "bash", scriptPath)
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		// Unreachable on purpose: 127.0.0.1 port 1 refuses immediately rather than hanging,
		// and nothing in these cases is meant to get that far anyway.
		"KAFKA_BOOTSTRAP_SERVER=127.0.0.1:1",
		"KAFKA_CONTAINER=blnk-provision-test-no-such-container",
		"KAFKA_COMPOSE_SERVICE=blnk-provision-test-no-such-service",
		"KAFKA_PROVISION_TIMEOUT_SECONDS=1",
		"KAFKA_PROVISION_POLL_INTERVAL_SECONDS=1",
		// THE ADMINISTRATIVE PAIR IS REQUIRED TO REACH THE DECISION PHASE, and supplying it
		// is not a weakening of these cases. The script validates the administrative
		// credential before the sample-subscriber grant, deliberately: the grant check also
		// refuses a sample principal that COLLIDES with the administrative one, and it can
		// only compare against a value that has already been trimmed and published. Run the
		// other way round, an administrative username carrying a trailing newline from a
		// secret store would slip past the collision test.
		//
		// Nothing here reaches a broker regardless: the grant decision is in the script's
		// pure-decision phase, before the CLI is detected, and KAFKA_BOOTSTRAP_SERVER points
		// at a port that refuses immediately.
		"KAFKA_SASL_ADMIN_USER=provision-test-admin",
		// At least 32 characters and drawn from the allowed alphabet, because the script
		// enforces both — a rule worth complying with rather than relaxing, since it is
		// what stopped a one-character password passing the old alphabet-only check.
		"KAFKA_SASL_ADMIN_SECRET=provisionTestNotARealSecret0123456789",
	}
	for key, value := range env {
		command.Env = append(command.Env, key+"="+value)
	}

	output, runErr := command.CombinedOutput()

	require.NoError(t, ctx.Err(),
		"the script did not finish inside its budget; it must reach its decision phase without "+
			"waiting on a broker. Output so far:\n%s", output)

	exitCode := 0
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		exitCode = exitErr.ExitCode()
	} else {
		require.NoError(t, runErr, "the script could not be executed at all")
	}

	return string(output), exitCode
}

// TestKafkaProvisionScript_NeverGeneratesACredentialItCannotDeliverSafely is the credential
// -disclosure guard.
//
// # What was wrong
//
// A generated sample password was printed to stdout in a banner captioned "shown once, not
// stored". The caption was true of the banner and false of everything downstream: this script
// runs as the compose kafka-init service, so its stdout is a container log that retains the
// credential for the container's lifetime, hands it to anyone who can run
// `docker compose logs`, forwards it to whatever collects the host's logs, and cannot be
// redacted after the fact. A password printed once into a permanent log is not a password
// shown once.
//
// # What replaced it, and what this test pins
//
// Generation now needs a nominated mode-0600 destination; with none, an explicitly supplied
// secret is used or the principal is skipped. The one case that must FAIL rather than skip is
// an explicit rotation with nowhere to deliver the result — the operator asked for a new
// credential in so many words, and quietly doing nothing would leave them believing the old
// one had been replaced. For the PRODUCER it is worse: a rotation that half-succeeded leaves
// the running server and worker presenting a password nobody holds.
//
// The refusal is asserted to happen in the pure-decision phase, BEFORE any broker contact, so
// this test needs no broker — and so an operator reads the diagnosis immediately instead of
// after a successful topic run and a thirty-second readiness wait.
func TestKafkaProvisionScript_NeverGeneratesACredentialItCannotDeliverSafely(t *testing.T) {
	scriptPath := filepath.Join(moduleRootDir(t), "scripts", "kafka-provision.sh")

	rotations := []struct {
		name        string
		environment map[string]string
		variable    string
	}{
		{
			name: "the producer, whose rotation would strip the server and worker of their credential",
			environment: map[string]string{
				"KAFKA_ROTATE_PRODUCER_SECRET": "1",
			},
			variable: "KAFKA_ROTATE_PRODUCER_SECRET",
		},
		{
			name: "the sample subscriber",
			environment: map[string]string{
				"KAFKA_SKIP_PRODUCER":                   "1",
				"KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET": "1",
			},
			variable: "KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET",
		},
	}

	for _, rotation := range rotations {
		t.Run("refuses a rotation with no destination: "+rotation.name, func(t *testing.T) {
			stdout, exitCode := runKafkaProvisionDecisionPhase(t, scriptPath, rotation.environment)

			assert.NotEqual(t, 0, exitCode,
				"an explicit rotation that cannot deliver its result must FAIL, not silently skip: "+
					"skipping would report success while the credential was unchanged")
			assert.Contains(t, stdout, "nowhere to deliver the new password",
				"the refusal must say what is missing; output was:\n%s", stdout)
			assert.Contains(t, stdout, rotation.variable,
				"the refusal must name the variable that asked for the rotation")
			assert.Contains(t, stdout, "mode 0600",
				"the remedy must name the permissioned destination, or an operator's only obvious "+
					"way out is to go back to printing the credential")

			// Pre-network: the readiness wait logs a line of its own, and reaching it would mean
			// the operator waits for a broker before being told about a configuration mistake.
			assert.NotContains(t, stdout, "waiting for",
				"the refusal must happen in the decision phase, before the broker is contacted")
		})
	}

	t.Run("a rotation with a destination is not refused", func(t *testing.T) {
		// The complement, and what stops the two cases above from passing over a script that
		// refused every rotation unconditionally.
		stdout, exitCode := runKafkaProvisionDecisionPhase(t, scriptPath, map[string]string{
			"KAFKA_ROTATE_PRODUCER_SECRET": "1",
			"KAFKA_PRODUCER_SECRET_FILE":   filepath.Join(t.TempDir(), "producer.secret"),
			"KAFKA_SKIP_SAMPLE_SUBSCRIBER": "1",
		})

		assert.NotEqual(t, 0, exitCode, "the run is expected to stop later, for want of a broker")
		assert.NotContains(t, stdout, "nowhere to deliver the new password",
			"a rotation with a nominated destination must be allowed through; output was:\n%s", stdout)
	})

	t.Run("a supplied secret is enough on its own", func(t *testing.T) {
		// The other legitimate route: an explicit value needs no destination, because the
		// operator already holds it and the script never echoes it.
		stdout, exitCode := runKafkaProvisionDecisionPhase(t, scriptPath, map[string]string{
			"KAFKA_ROTATE_PRODUCER_SECRET": "1",
			"KAFKA_PRODUCER_SECRET":        "a-supplied-producer-secret",
			"KAFKA_SKIP_SAMPLE_SUBSCRIBER": "1",
		})

		assert.NotEqual(t, 0, exitCode, "the run is expected to stop later, for want of a broker")
		assert.NotContains(t, stdout, "nowhere to deliver the new password",
			"a supplied secret must satisfy the rotation; output was:\n%s", stdout)
		assert.NotContains(t, stdout, "a-supplied-producer-secret",
			"a supplied secret must never be echoed, in any diagnostic, at any point")
	})

	t.Run("no banner prints a credential anywhere in the script", func(t *testing.T) {
		// A source assertion, and the one place it is the right instrument: the property is
		// "no line of this file interpolates the password variable into output", which is a
		// statement about the text. Every behavioural case above can only cover the paths it
		// reaches; this covers the ones it does not.
		script, err := os.ReadFile(scriptPath)
		require.NoError(t, err)

		emitsPassword := regexp.MustCompile(`(?m)^\s*(printf|echo)[^\n]*\$\{?password`)
		assert.Nil(t, emitsPassword.Find(script),
			"no printf or echo in scripts/kafka-provision.sh may interpolate the generated "+
				"password: this script's stdout is the kafka-init container log. Deliver it "+
				"through stage_generated_secret and commit_staged_secret, which write a mode-0600 "+
				"file and report only the path")
	})
}

// TestStackInit_WritesSecretsOnlyIntoAPrivateFile is the secret-storage guard for stack.sh.
//
// # What was wrong
//
// `--init` copied .env.example — a committed template, correctly mode 0644 because it holds no
// secrets — with plain `cp`, which creates the destination under the process umask. On a
// typical developer machine that is 022, so .env was created WORLD-READABLE and every
// credential generated into it was written into a world-readable file: a PostgreSQL password
// and a Kafka SUPERUSER password, on a stack whose broker is published to the host.
//
// # Why the test forces a permissive umask
//
// Because the defect is invisible under a strict one. A developer running with umask 077 would
// have seen 0600 and concluded the code was fine; the bug only appears under the umask most
// machines actually have. So the umask is set to 022 deliberately, and the assertion is on the
// resulting mode rather than on the presence of a chmod call — a chmod placed after the writes
// would satisfy a source scan while still leaving a window in which the file existed readable.
func TestStackInit_WritesSecretsOnlyIntoAPrivateFile(t *testing.T) {
	root := moduleRootDir(t)
	workDir := t.TempDir()

	// stack.sh resolves .env and .env.example relative to the working directory, so both are
	// copied in and the script runs there. Nothing touches the repository's own tree.
	for _, name := range []string{"stack.sh", ".env.example"} {
		contents, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err, "%s must be readable", name)
		require.NoError(t, os.WriteFile(filepath.Join(workDir, name), contents, 0o644))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// umask 022 is applied by the shell that runs the script, reproducing a default developer
	// machine. A subshell is used so the value cannot leak into the test process.
	command := exec.CommandContext(ctx, "bash", "-c", "umask 022 && bash stack.sh --init")
	command.Dir = workDir
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + workDir, "TERM=dumb"}

	output, err := command.CombinedOutput()
	require.NoError(t, err, "stack.sh --init must succeed; output:\n%s", output)

	envPath := filepath.Join(workDir, ".env")
	info, err := os.Stat(envPath)
	require.NoError(t, err, ".env must have been created")

	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
		".env must be mode 0600. It holds a PostgreSQL password and a Kafka superuser password, "+
			"and this stack publishes the broker to the host, so group- or world-readable is a "+
			"credential disclosure and not an untidiness")

	contents, err := os.ReadFile(envPath)
	require.NoError(t, err)
	rendered := string(contents)

	// Every credential --init is responsible for must actually have been generated. A .env with
	// the right mode and an empty secret is a stack that cannot start, and asserting the mode
	// alone would pass over it.
	for _, key := range []string{
		"POSTGRES_PASSWORD",
		"KAFKA_SASL_ADMIN_USER",
		"KAFKA_SASL_ADMIN_SECRET",
		"KAFKA_PRODUCER_USER",
		"KAFKA_PRODUCER_SECRET",
		"KAFKA_SAMPLE_SUBSCRIBER_SECRET",
	} {
		value := envValueOf(t, rendered, key)
		assert.NotEmpty(t, value, "%s must be populated by --init", key)
		assert.NotContains(t, value, "{",
			"%s must not still carry a brace placeholder: nothing downstream substitutes one, so "+
				"the service reads the brace text itself as its password", key)
	}

	// The two generated Kafka secrets must be DIFFERENT values. The administrative principal is
	// a cluster superuser and the producer is not; reusing one value for both would make the
	// least-privilege split cosmetic, because compromising the publisher would hand over the
	// administrative credential too.
	assert.NotEqual(t,
		envValueOf(t, rendered, "KAFKA_SASL_ADMIN_SECRET"),
		envValueOf(t, rendered, "KAFKA_PRODUCER_SECRET"),
		"the administrative and producer secrets must be independently generated: sharing one "+
			"value would mean a compromised publisher is a compromised superuser, which is the "+
			"whole thing the separate principal exists to prevent")

	// base64 pads with "=", which is outside the credential alphabet kafka-bootstrap.sh and
	// kafka-provision.sh share — Kafka's --add-scram grammar has no escape for it — so a padded
	// password is refused and the broker never bootstraps.
	for _, key := range []string{"KAFKA_SASL_ADMIN_SECRET", "KAFKA_PRODUCER_SECRET", "KAFKA_SAMPLE_SUBSCRIBER_SECRET"} {
		assert.NotContains(t, envValueOf(t, rendered, key), "=",
			"%s must not contain base64 padding: the value grammar Kafka's --add-scram accepts has "+
				"no escape for '=', so a padded password is refused rather than truncated. Generate "+
				"from a byte count that is a multiple of three", key)
	}
}

// envValueOf reads one key's value out of a rendered .env, ignoring commented-out lines.
func envValueOf(t *testing.T, rendered, key string) string {
	t.Helper()

	for _, line := range strings.Split(rendered, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if name, value, found := strings.Cut(trimmed, "="); found && strings.TrimSpace(name) == key {
			return strings.TrimSpace(value)
		}
	}

	require.FailNowf(t, "key absent", "%s is not assigned in the rendered .env", key)

	return ""
}

// storeKafkaTopicPrefixes publishes a configuration with a configured prefix AND a historical
// allowlist, so the owned-prefix readers can be exercised over both generations.
func storeKafkaTopicPrefixes(t *testing.T, current string, historical ...string) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	config.ConfigStore.Store(&config.Configuration{
		Kafka: config.KafkaConfig{
			TopicPrefix:             current,
			HistoricalTopicPrefixes: historical,
		},
	})
}

// TestOwnedTopicPrefixes_LeadsWithTheConfiguredPrefixAndAppendsTheDeclaredHistory is the
// resolution order every owned-topic reader depends on.
//
// The configured prefix leads because that is where NEW events go, so a caller composing an
// inventory builds the live generation first — matching the provisioning script and the
// documentation — and a caller resolving one name finds the common case on the first
// comparison. The list is distinct and blank-free so that an inventory composed from it has
// no duplicates and no holes.
func TestOwnedTopicPrefixes_LeadsWithTheConfiguredPrefixAndAppendsTheDeclaredHistory(t *testing.T) {
	t.Run("no history declared is the ordinary case", func(t *testing.T) {
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		assert.Equal(t, []string{DefaultTopicPrefix}, OwnedTopicPrefixes())
		assert.Empty(t, HistoricalTopicPrefixes())
		assert.Equal(t, AllTopicsWithDeadLetters(), AllOwnedTopicsAcrossPrefixes(),
			"with no history declared the cross-prefix inventory must be exactly the current one, "+
				"or every caller pays for a mechanism no deployment is using")
	})

	t.Run("declared history follows the configured prefix in order", func(t *testing.T) {
		storeKafkaTopicPrefixes(t, "acme", " blnk\n", ".legacy.", "", "acme")

		assert.Equal(t, []string{"acme", "blnk", "legacy"}, OwnedTopicPrefixes(),
			"entries must be trimmed, deduplicated and stripped of the configured prefix even "+
				"when the configuration never went through validateAndAddDefaults")
		assert.Equal(t, []string{"blnk", "legacy"}, HistoricalTopicPrefixes())
	})

	t.Run("an unloaded configuration answers with the default prefix alone", func(t *testing.T) {
		// The strictest available answer. Widening the owned namespace because configuration
		// has not been published would be the one direction that is unsafe.
		storeKafkaTopicPrefixes(t, "")

		assert.Equal(t, []string{DefaultTopicPrefix}, OwnedTopicPrefixes())
	})
}

// TestAllOwnedTopicsAcrossPrefixes_CoversEveryGenerationInInventoryOrder is the inventory the
// publisher pre-creates and topic assurance keeps present.
//
// Both callers need every generation. The publisher, because a row captured before a rename
// names the previous generation's topic and must be served from the fast path after a restart;
// assurance, because a historical topic that was deleted — or is absent on a broker restored
// from elsewhere — would otherwise fail every publish of the rows naming it, or silently
// auto-create a topic with one partition and the wrong replication factor.
func TestAllOwnedTopicsAcrossPrefixes_CoversEveryGenerationInInventoryOrder(t *testing.T) {
	storeKafkaTopicPrefixes(t, "acme", "blnk")

	inventory := AllOwnedTopicsAcrossPrefixes()

	want := append(
		AllTopicsWithDeadLettersForPrefix("acme"),
		AllTopicsWithDeadLettersForPrefix("blnk")...,
	)
	assert.Equal(t, want, inventory,
		"the live generation's inventory must come first, then each declared generation's, so "+
			"this list can be diffed against the provisioning script line for line")

	seen := map[string]struct{}{}
	for _, topic := range inventory {
		_, duplicate := seen[topic]
		assert.Falsef(t, duplicate, "%q appears twice: a duplicate would pre-create two writers "+
			"for one topic and double-count it in every inventory report", topic)
		seen[topic] = struct{}{}
	}

	for _, topic := range want {
		assert.True(t, IsOwnedTopicUnderAnyConfiguredPrefix(topic),
			"every name in the inventory must pass the ownership test, or the publisher refuses "+
				"a topic it pre-created a writer for")
	}
}

// TestIsOwnedTopicUnderAnyConfiguredPrefix_IsExactPerDeclaredNamespace is the security boundary
// of the historical allowlist.
//
// Widening the ownership test is what keeps a stored old-prefix row publishable, and writer
// resolution is the one place a stored string becomes an outbound connection carrying Blnk's
// own producer credentials. So the widening must be EXACTLY the declared namespaces: a prefix
// comparison rather than an equality would turn "blnk" into a licence for
// "blnkfinance.transactions", and a form test would admit any third party's topic.
func TestIsOwnedTopicUnderAnyConfiguredPrefix_IsExactPerDeclaredNamespace(t *testing.T) {
	storeKafkaTopicPrefixes(t, "acme", "blnk")

	for _, owned := range append(
		AllTopicsWithDeadLettersForPrefix("acme"),
		AllTopicsWithDeadLettersForPrefix("blnk")...,
	) {
		assert.Truef(t, IsOwnedTopicUnderAnyConfiguredPrefix(owned), "%q is declared", owned)
	}

	for _, refused := range []string{
		"blnkfinance.transactions", // starts with a declared prefix
		"bln.transactions",         // a declared prefix starts with it
		"acme.blnk.transactions",   // declared prefix as an interior segment
		"legacy.transactions",      // owned FORM, never declared
		"attacker.transactions",    // the case the form test would admit
		"blnk.ledgers",             // declared namespace, unknown category
		"blnk.transactions.dlt.dlt",
		"transactions",
		"__consumer_offsets",
		"",
		" blnk.transactions",
	} {
		assert.Falsef(t, IsOwnedTopicUnderAnyConfiguredPrefix(refused),
			"%q must not be treated as owned", refused)
	}
}

// strandedAuditStore is a fake outbox for the stranded-prefix audit.
type strandedAuditStore struct {
	backlogs []model.EventTopicBacklog
	err      error
	calls    int
}

func (s *strandedAuditStore) ListUndrainedEventTopics(
	_ context.Context,
) ([]model.EventTopicBacklog, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}

	return s.backlogs, nil
}

// TestAuditStrandedTopicPrefixes_NamesTheUndeclaredGenerationAndNothingElse is the audit that
// makes a stranded prefix visible at all.
//
// # What it is for
//
// A row records its destination topic at insert time, so changing KAFKA_TOPIC_PREFIX leaves
// committed rows naming the previous generation's topics. They stay publishable only while
// that prefix is declared in KAFKA_HISTORICAL_TOPIC_PREFIXES. Declare nothing and the rows are
// stranded: the publisher refuses each topic, the rows stay claimable for ever, their
// dead-letter writes and replays are refused with them — and every status count still reads as
// ordinary outstanding work. The only evidence is a topic name on rows nobody is reading, so
// this audit exists to turn that into a prefix an operator can act on.
//
// # Why the negative cases matter as much as the finding
//
// The audit's output becomes advice to widen the owned namespace, so it must not name a
// namespace that is already owned (nothing to fix) or one parsed out of a malformed row
// (advice to admit a topic on the strength of one bad value).
func TestAuditStrandedTopicPrefixes_NamesTheUndeclaredGenerationAndNothingElse(t *testing.T) {
	older := time.Now().UTC().Add(-72 * time.Hour)
	newer := older.Add(time.Hour)

	t.Run("an undeclared generation is reported with its rows and its age", func(t *testing.T) {
		storeKafkaTopicPrefix(t, "acme")

		store := &strandedAuditStore{backlogs: []model.EventTopicBacklog{
			{Topic: "blnk.transactions", UndeliveredRows: 7, OldestOccurredAt: older},
			{Topic: "blnk.balances.dlt", ReplayableRows: 2, OldestOccurredAt: newer},
			// Owned: the live generation owes work too, and that is not a finding.
			{Topic: "acme.transactions", UndeliveredRows: 40, OldestOccurredAt: newer},
		}}

		stranded, err := AuditStrandedTopicPrefixes(context.Background(), store)
		require.NoError(t, err)
		require.Len(t, stranded, 1,
			"only the undeclared namespace is a finding; the configured one owes work in the "+
				"ordinary course and reporting it would train an operator to ignore this line")

		finding := stranded[0]
		assert.Equal(t, "blnk", finding.Prefix,
			"the reported prefix must be the exact value to add to KAFKA_HISTORICAL_TOPIC_PREFIXES")
		assert.Equal(t, []string{"blnk.transactions", "blnk.balances.dlt"}, finding.Topics,
			"both the category and the dead-letter topic are stranded, and a failing row must be "+
				"able to dead-letter as much as a pending one must be able to publish")
		assert.EqualValues(t, 7, finding.UndeliveredRows)
		assert.EqualValues(t, 2, finding.ReplayableRows)
		assert.EqualValues(t, 9, finding.TotalRows())
		assert.True(t, finding.OldestOccurredAt.Equal(older),
			"the oldest outstanding occurrence across the generation is what says how overdue it is")
	})

	t.Run("declaring the prefix clears the finding", func(t *testing.T) {
		storeKafkaTopicPrefixes(t, "acme", "blnk")

		store := &strandedAuditStore{backlogs: []model.EventTopicBacklog{
			{Topic: "blnk.transactions", UndeliveredRows: 7, OldestOccurredAt: older},
			{Topic: "blnk.balances.dlt", ReplayableRows: 2, OldestOccurredAt: newer},
		}}

		stranded, err := AuditStrandedTopicPrefixes(context.Background(), store)
		require.NoError(t, err)
		assert.Empty(t, stranded,
			"the remedy the audit recommends must actually clear it, or the log keeps reporting a "+
				"condition the operator has already fixed")
	})

	t.Run("a malformed destination is not reported as a namespace to declare", func(t *testing.T) {
		storeKafkaTopicPrefix(t, "acme")

		store := &strandedAuditStore{backlogs: []model.EventTopicBacklog{
			{Topic: "__consumer_offsets", UndeliveredRows: 1, OldestOccurredAt: older},
			{Topic: "transactions", UndeliveredRows: 1, OldestOccurredAt: older},
			{Topic: "blnk.ledgers", UndeliveredRows: 1, OldestOccurredAt: older},
			{Topic: "", UndeliveredRows: 1, OldestOccurredAt: older},
		}}

		stranded, err := AuditStrandedTopicPrefixes(context.Background(), store)
		require.NoError(t, err)
		assert.Empty(t, stranded,
			"none of these is a generation of Blnk topics, and recommending that an operator "+
				"declare a namespace parsed out of one would widen the owned set on the strength "+
				"of a row that should never have been storable")
	})

	t.Run("a measurement failure is returned rather than reported as no finding", func(t *testing.T) {
		storeKafkaTopicPrefix(t, "acme")

		store := &strandedAuditStore{err: errors.New("outbox unavailable")}

		stranded, err := AuditStrandedTopicPrefixes(context.Background(), store)
		require.Error(t, err, "a failed query says nothing about whether a prefix is stranded")
		assert.Nil(t, stranded)
		assert.Equal(t, 1, store.calls)
	})

	t.Run("a nil store is refused rather than dereferenced", func(t *testing.T) {
		_, err := AuditStrandedTopicPrefixes(context.Background(), nil)
		require.Error(t, err)
	})
}

// TestSubscriberGrantableTopics_ExcludesDeadLettersAndInternalTopics is the test for the
// authorization allowlist, and it is the one that stops an over-broad grant.
//
// Before the allowlist existed, both the request-validation layer and the ACL
// provisioning simply trimmed whatever topic list the caller supplied and granted what
// remained. An authorized request could therefore name the literal wildcard, a
// dead-letter topic, an internal topic, or a topic belonging to another system entirely,
// and receive a real ACL binding over it. Every rejected value below is one of those.
func TestSubscriberGrantableTopics_ExcludesDeadLettersAndInternalTopics(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	// FOUR topics — every category, and no dead-letter sibling. The exclusion is structural
	// rather than listed: the allowlist is composed as "<prefix>.<category>", so a ".dlt" name
	// can never be a member of it.
	//
	// blnk.system IS included, and it is the member that has to argue for itself, because it
	// carries system.error whose payload is the frozen legacy body and therefore contains raw
	// error text verbatim. It is included because withholding it withholds ledger.created with
	// it — the two share the category under the frozen four-category contract — leaving an
	// event type published to a topic no credential may name. That is strictly less delivery
	// than the webhook transport being replaced, which requirement R-1 forbids, and it is
	// exactly parity with today: internal/notification already delivers system.error, with that
	// same verbatim text, to the single configured webhook URL.
	//
	// WHAT THIS LIST IS NOT is a grant. It says which topics a subscriber MAY be authorised for;
	// which ones it IS authorised for is its authorized_topics, decided per subscriber.
	// docs/event-streaming.md states what granting blnk.system discloses so that decision can be
	// taken deliberately rather than by default.
	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, SubscriberGrantableTopics(),
		"every category topic may be granted to a subscriber and no dead-letter topic may")

	for _, topic := range SubscriberGrantableTopics() {
		assert.True(t, IsSubscriberGrantableTopic(topic),
			"%q is listed as grantable, so the predicate must accept it", topic)
		assert.False(t, IsDeadLetterTopic(topic),
			"%q must not be a dead-letter topic: a .dlt record carries Blnk's failure metadata, which is operator detail rather than subscriber data", topic)
	}

	refused := map[string]string{
		"the transactions dead-letter topic": "blnk.transactions.dlt",
		"the balances dead-letter topic":     "blnk.balances.dlt",
		"the identities dead-letter topic":   "blnk.identities.dlt",
		"the system dead-letter topic":       "blnk.system.dlt",
		"the literal wildcard":               "*",
		"a wildcard suffix":                  "blnk.*",
		"a single-character wildcard":        "blnk.transaction?",
		"an empty topic":                     "",
		"a whitespace-only topic":            "   ",
		"a foreign topic":                    "someone-elses.topic",
		"a Kafka internal topic":             "__consumer_offsets",
		"the KRaft metadata topic":           "__cluster_metadata",
		"a near-miss with a suffix":          "blnk.transactions.something-else",
		"a near-miss with a prefix":          "not-blnk.transactions",
		"an upper-cased grantable topic":     "BLNK.TRANSACTIONS",
		"a mixed-case grantable topic":       "Blnk.Transactions",
		"a topic with an embedded newline":   "blnk.transactions\nblnk.quarantine",
		"a topic with an embedded NUL":       "blnk.transactions\x00",
	}
	for name, topic := range refused {
		assert.False(t, IsSubscriberGrantableTopic(topic),
			"%s (%q) must not be grantable to a subscriber", name, topic)
	}

	// Trailing and leading whitespace on an otherwise valid name IS tolerated, because
	// an operator pasting a topic name is a routine mistake and the trimmed value is
	// unambiguous. Nothing else is normalised.
	assert.True(t, IsSubscriberGrantableTopic("  blnk.transactions  "),
		"surrounding whitespace on an exact match is trimmed, which is the only normalisation applied")

	// The allowlist must follow the configured namespace, or a non-default deployment
	// could grant nothing at all.
	storeKafkaTopicPrefix(t, "acme.events")
	assert.Equal(t, []string{
		"acme.events.transactions",
		"acme.events.balances",
		"acme.events.identities",
		"acme.events.system",
	}, SubscriberGrantableTopics())
	assert.True(t, IsSubscriberGrantableTopic("acme.events.transactions"))
	assert.False(t, IsSubscriberGrantableTopic("blnk.transactions"),
		"a topic from a different namespace must not be grantable just because it looks Blnk-owned")
}

// TestSubscriberGrantableTopics_IsEveryCategoryAndNoDeadLetter is the test for the
// authorization allowlist, and it is the one that stops an over-broad grant.
//
// Before the allowlist existed, both the request-validation layer and the ACL
// provisioning simply trimmed whatever topic list the caller supplied and granted what
// remained. An authorized request could therefore name the literal wildcard, a
// dead-letter topic, or a topic belonging to another system entirely, and receive a real
// ACL binding over it. Every rejected value below is one of those.
func TestSubscriberGrantableTopics_IsEveryCategoryAndNoDeadLetter(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	// THE FOUR CATEGORY TOPICS, and no dead-letter sibling. This is what MAY be granted;
	// which of them a particular subscriber holds is decided per subscriber by its
	// authorized_topics.
	//
	// blnk.system is HERE, and it is the entry that matters. It carries ledger.created and
	// system.error, both of which the legacy webhook transport delivers to the configured
	// endpoint today, so an allowlist without it would leave two of the thirteen migrated
	// event types published and unreachable at the same time — a consuming-side regression
	// rather than a tightening. It also carries verbatim internal error text, which is why an
	// operator grants it deliberately rather than by default; the sample principal the
	// provisioning script mints does not take it.
	assert.Equal(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
	}, SubscriberGrantableTopics(),
		"every category topic may be granted to a subscriber, and no dead-letter sibling ever "+
			"may be")

	for _, topic := range SubscriberGrantableTopics() {
		assert.True(t, IsSubscriberGrantableTopic(topic),
			"%q is listed as grantable, so the predicate must accept it", topic)
		assert.False(t, IsDeadLetterTopic(topic),
			"%q must not be a dead-letter topic: a .dlt record carries Blnk's failure metadata, which is operator detail rather than subscriber data", topic)
	}

	refused := map[string]string{
		"the transactions dead-letter topic": "blnk.transactions.dlt",
		"the balances dead-letter topic":     "blnk.balances.dlt",
		"the identities dead-letter topic":   "blnk.identities.dlt",
		"the system dead-letter topic":       "blnk.system.dlt",
		"a retired category name":            "blnk.ledgers",
		"the literal wildcard":               "*",
		"a wildcard suffix":                  "blnk.*",
		"a single-character wildcard":        "blnk.transaction?",
		"an empty topic":                     "",
		"a whitespace-only topic":            "   ",
		"a foreign topic":                    "someone-elses.topic",
		"a Kafka internal topic":             "__consumer_offsets",
		"the KRaft metadata topic":           "__cluster_metadata",
		"a near-miss with a suffix":          "blnk.transactions.something-else",
		"a near-miss with a prefix":          "not-blnk.transactions",
		"an upper-cased grantable topic":     "BLNK.TRANSACTIONS",
		"a mixed-case grantable topic":       "Blnk.Transactions",
		"a topic with an embedded newline":   "blnk.transactions\nblnk.quarantine",
		"a topic with an embedded NUL":       "blnk.transactions\x00",
	}
	for name, topic := range refused {
		assert.False(t, IsSubscriberGrantableTopic(topic),
			"%s (%q) must not be grantable to a subscriber", name, topic)
	}

	// Trailing and leading whitespace on an otherwise valid name IS tolerated, because
	// an operator pasting a topic name is a routine mistake and the trimmed value is
	// unambiguous. Nothing else is normalised.
	assert.True(t, IsSubscriberGrantableTopic("  blnk.transactions  "),
		"surrounding whitespace on an exact match is trimmed, which is the only normalisation applied")

	// The allowlist must follow the configured namespace, or a non-default deployment
	// could grant nothing at all.
	storeKafkaTopicPrefix(t, "acme.events")
	assert.Equal(t, []string{
		"acme.events.transactions",
		"acme.events.balances",
		"acme.events.identities",
		"acme.events.system",
	}, SubscriberGrantableTopics())
	assert.True(t, IsSubscriberGrantableTopic("acme.events.transactions"))
	assert.False(t, IsSubscriberGrantableTopic("blnk.transactions"),
		"a topic from a different namespace must not be grantable just because it looks Blnk-owned")
}

// TestTopicForEvent_UnrecognisedEventRoutesToTheInternalSystemTopic pins the chosen
// behaviour for an event type that is not in the catalogue, and pins WHICH topic it is.
//
// Two requirements meet here, and both must hold of the one destination:
//
//   - The routing must be TOTAL. The relay publishes to whatever topic it is handed, so
//     returning an empty string would strand a committed event — no topic, no publish,
//     no dead-letter entry, nothing to replay — and dropping it would lose it outright.
//   - The destination must be the NARROWEST topic in the catalogue, because a producer added
//     without extending the mapping lands its payload there — plausibly a balance or an
//     identity record — and whoever is reading that topic was not expecting it.
//
// blnk.system satisfies both: the event is published, observable and replayable, and the
// system category is the one an operator grants deliberately and rarely, only to a subscriber
// that needs ledger events (see model.EventCategorySystem for what such a grant discloses).
// Containment here is therefore a matter of WHO WAS GRANTED the topic rather than of the
// topic being ungrantable, and the capture-time warning below is what makes the omission
// visible so the mapping gets fixed rather than relied on.
func TestTopicForEvent_UnrecognisedEventRoutesToTheInternalSystemTopic(t *testing.T) {
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
		// Unusual characters. None of these is a realistic event name, and that is the
		// point: routing has to be TOTAL. Anything a caller can hand this function must
		// come back as a usable topic, because the relay publishes to whatever name it is
		// given and has no branch for "no topic". A panic, an empty string or a
		// half-composed name here would each strand the event, so the awkward inputs are
		// asserted rather than assumed.
		"événement.créé",             // non-ASCII letters
		"事件.已创建",                     // multi-byte, no ASCII at all
		"transaction.applied.💥",      // an emoji outside the Basic Multilingual Plane
		"!@#$%^&*()",                 // punctuation only
		"...",                        // separators only, which must not compose an empty segment
		".",                          // a lone separator
		"transaction\n.applied",      // an embedded newline, as a mangled environment value might carry
		"transaction\t.applied",      // an embedded tab
		"transaction\x00.applied",    // an embedded NUL, which Kafka rejects but naming must survive
		"  transaction.applied  ",    // surrounded by whitespace, so NOT the emitted string
		"transaction.applied\u200b",  // a zero-width space, invisible in a diff
		strings.Repeat("very.", 200), // longer than Kafka's 249-character topic limit
	} {
		assert.Equal(t, "blnk.system", TopicForEvent(eventType),
			"unrecognised event type %q must route to the internal system topic, the catalogue's catch-all", eventType)
		assert.False(t, model.IsCataloguedEventType(eventType),
			"unrecognised event type %q must be reported as uncatalogued, which is what raises the capture-time warning naming the producer to fix", eventType)
		assert.NotEmpty(t, TopicForEvent(eventType),
			"TopicForEvent must never return an empty string: the relay would strand the event")
		assert.Equal(t, "blnk.system", TopicForEvent(eventType),
			"the catch-all must stay the system topic: routing an unmapped event to a category "+
				"topic subscribers hold broadly would hand its payload to every one of them")

		// The event's own name must never leak into the topic name. Composing the event
		// into the topic would create a topic per event type on demand — unprovisioned,
		// single-partition, wrongly replicated, and read by nobody.
		assert.Equal(t, "blnk.system.dlt", DeadLetterTopicForEvent(eventType),
			"and its dead-letter sibling must be the system topic's, not one derived from %q", eventType)
	}
}

// TestTopicForCategory_BlankCategoryFallsBackToTheInternalSystemTopic pins the guard
// against composing a name with an empty final segment, and pins where the fallback goes.
//
// "<prefix>." is a topic Kafka would accept and nothing would read, so a blank category
// must still compose a usable name — the same never-drop-an-event policy the event
// mapping's own catch-all implements. It falls back to the SYSTEM topic, which is the
// catch-all model.EventCategory itself uses: a blank category means the caller could not
// classify the event, so its audience is unknown, and the system topic is the one an operator
// grants deliberately rather than by default.
func TestTopicForCategory_BlankCategoryFallsBackToTheInternalSystemTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	for _, blank := range []string{"", " ", "\t", ".", " . ", "\n"} {
		assert.Equal(t, "blnk.system", TopicForCategory(blank),
			"a blank category %q must fall back to the system topic, never compose %q", blank, "blnk.")
		assert.NotEqual(t, DefaultTopicPrefix+".", TopicForCategory(blank),
			"the fallback must be a usable name: %q is a topic Kafka accepts and nothing reads", "blnk.")
		assert.NotEqual(t, "blnk.", TopicForCategory(blank))
	}
}
