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
package model

import (
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the Kafka event contract declared in event.go — the canonical JSON
// event schema, the event-type-to-category mapping, and the status and category
// vocabularies that the topic-naming, persistence, relay, dead-letter and metrics
// layers all agree on.

// jsonTagName returns the name portion of a struct field's json tag, discarding any
// option suffix such as ",omitempty".
func jsonTagName(tag string) string {
	for i := 0; i < len(tag); i++ {
		if tag[i] == ',' {
			return tag[:i]
		}
	}
	return tag
}

// jsonTagNames returns the json tag names of every field of a struct type, in
// declaration order, with option suffixes stripped. Declaration order is
// preserved because encoding/json emits struct keys in that order, so the
// sequence itself is part of the observable wire shape.
func jsonTagNames(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	require.Equal(t, reflect.Struct, typ.Kind(), "jsonTagNames is only meaningful for a struct type")

	names := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("json")
		require.NotEmpty(t, tag, "field %s must carry an explicit json tag: an untagged field marshals under its Go name, which would not match its database column or its documented wire key", field.Name)
		names = append(names, jsonTagName(tag))
	}
	return names
}

// mustMarshal marshals v and fails the test immediately if it cannot, so that a
// marshalling failure is reported as itself rather than as a confusing downstream
// assertion failure on empty bytes.
func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err, "marshalling the event contract must never fail")
	return b
}

// decodeObject decodes a JSON object into a map of raw values. Decoding into
// json.RawMessage rather than interface{} is essential to every byte-fidelity assertion
// in this file: interface{} would turn the payload into a map[string]interface{},
// reordering its keys and renormalising its numbers, and the very thing under test
// would be destroyed by the test's own decoding.
func decodeObject(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	out := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(b, &out), "the marshaled form must be a JSON object")
	return out
}

// TestEventCategory_ResolvesEveryEmittedEventString walks the complete catalogue of
// event strings Blnk actually emits and asserts each one resolves to the category token
// that routes it to the right topic.
//
// The catalogue resolves to FOUR categories, not the three the topic requirements name:
// ledger.created and system.error belong to none of those three and route to the system
// category instead of being dropped.
func TestEventCategory_ResolvesEveryEmittedEventString(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		want      string
		reason    string
	}{
		// --- transactions: the seven names the status-to-event mapping returns ---
		{
			name:      "transaction queued",
			eventType: "transaction.queued",
			want:      EventCategoryTransactions,
			reason:    "transaction lifecycle events must route to the transactions category",
		},
		{
			name:      "transaction applied",
			eventType: "transaction.applied",
			want:      EventCategoryTransactions,
			reason:    "transaction lifecycle events must route to the transactions category",
		},
		{
			name:      "transaction scheduled",
			eventType: "transaction.scheduled",
			want:      EventCategoryTransactions,
			reason:    "transaction lifecycle events must route to the transactions category",
		},
		{
			name:      "transaction inflight",
			eventType: "transaction.inflight",
			want:      EventCategoryTransactions,
			reason:    "transaction lifecycle events must route to the transactions category",
		},
		{
			name:      "transaction void",
			eventType: "transaction.void",
			want:      EventCategoryTransactions,
			reason:    "transaction lifecycle events must route to the transactions category",
		},
		{
			name:      "transaction rejected",
			eventType: "transaction.rejected",
			want:      EventCategoryTransactions,
			reason:    "transaction.rejected is emitted from two places — the execution path and the worker rejection handler — so losing this arm drops events from both",
		},
		{
			name:      "transaction unknown",
			eventType: "transaction.unknown",
			want:      EventCategoryTransactions,
			reason:    "transaction.unknown is the status-to-event mapping's defensive default for a status no case names. No code path produces it today — a committed inflight transaction is normalised to APPLIED before its name is derived — but it must still route to the transactions topic so a status added in future cannot be dropped",
		},

		// --- transactions: bulk names, composed at runtime as prefix + status ---
		{
			name:      "bulk transaction applied",
			eventType: "bulk_transaction.applied",
			want:      EventCategoryTransactions,
			reason:    "bulk_transaction.applied is emitted on both the synchronous and asynchronous batch success paths",
		},
		{
			name:      "bulk transaction failed",
			eventType: "bulk_transaction.failed",
			want:      EventCategoryTransactions,
			reason:    "bulk_transaction.failed is emitted by the asynchronous batch failure handler",
		},
		{
			name:      "bulk transaction with a status not emitted today",
			eventType: "bulk_transaction.partially_applied",
			want:      EventCategoryTransactions,
			reason:    "this status is NOT emitted today and that is the point: bulk event names are composed at runtime as \"bulk_transaction.\" + status, so the suffix set is open. Matching by prefix is what routes any future status correctly; exact-match routing would silently misroute it to the catch-all category instead",
		},

		// --- balances ---
		{
			name:      "balance created",
			eventType: "balance.created",
			want:      EventCategoryBalances,
			reason:    "balance creation events must route to the balances category",
		},
		{
			name:      "balance monitor",
			eventType: "balance.monitor",
			want:      EventCategoryBalances,
			reason:    "balance monitor alerts are balance events, not system events, and must route to the balances category",
		},

		// --- identities ---
		{
			name:      "identity created",
			eventType: "identity.created",
			want:      EventCategoryIdentities,
			reason:    "identity events must route to the identities category",
		},

		// --- the two event types outside the three named categories, both on the one extra
		// category the catalogue adds for them ---
		{
			name:      "ledger created",
			eventType: "ledger.created",
			want:      EventCategorySystem,
			reason:    "ledger.created belongs to none of the three tenant categories, so the published four-category catalogue routes it to blnk.system alongside system.error. A subscriber therefore reaches it only through the privileged grant of that topic, which discloses Blnk's verbatim internal error text as well - a documented cost of the contract",
		},
		{
			name:      "system error",
			eventType: "system.error",
			want:      EventCategorySystem,
			reason:    "system.error carries Blnk's internal diagnostic detail verbatim under a payload R-8 freezes, so it routes to the system category - the one category outside the default grant set, which is what keeps that text an operator surface unless the deployment has declared KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EventCategory(tt.eventType), tt.reason)
		})
	}

	// Guard the table itself. Thirteen distinct event types are emitted today: the seven
	// transaction names, the runtime-composed bulk name, ledger, identity, the two balance
	// names, and the system error.
	assert.Len(t, tests, 15, "the table must cover all thirteen emitted event types, with the runtime-composed bulk name exercised by three suffixes")
}

// The catch-all arm's own test is not here. The surviving
// TestEventCategory_UnknownEventFallsBackToTheSystemCategory below states the contract
// correctly and asserts a superset: the same two fallback answers plus
// IsCataloguedEventType, which is what makes a routing omission observable rather than
// silent.

// TestSubscriberGrantableEventCategories_IsEveryTenantCategoryAndNotTheInternalOne pins
// the allowlist the subscriber authorization path and the Kafka ACL provisioning both
// consult.
//
// It asserts membership by NAME in both directions, because a count-only assertion
// passes just as happily when one category is swapped for another as when the list is
// correct.
//
// The catalogue holds four category topics. Three of them — transactions, balances and
// identities — are tenant data and are in the default grant set.
//
//   - system.error's payload is the frozen legacy body, so it renders Blnk's error text
//     as it comes — a PostgreSQL error names schema, table, column and routine; a
//     broker error names internal addresses.
//   - The category is the catalogue's CATCH-ALL, so a grant of it also stands over
//     every event type nobody has catalogued yet: access widened by a future routing
//     omission rather than by an authorization decision.
//
// IT IS STILL REACHABLE, through SubscriberPrivilegedEventCategories and only where the
// deployment has declared KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS — which is what makes
// the disclosure a decision recorded at deployment level rather than one a single PUT
// can reach. The route must exist: system.error reached webhook
// subscribers, so retiring that transport without a credential-reachable equivalent
// would drop the audience.
func TestSubscriberGrantableEventCategories_IsEveryTenantCategoryAndNotTheInternalOne(t *testing.T) {
	grantable := SubscriberGrantableEventCategories()

	assert.Contains(t, grantable, EventCategoryTransactions, "transactions is subscriber-facing ledger data")
	assert.Contains(t, grantable, EventCategoryBalances, "balances is subscriber-facing ledger data")
	assert.Contains(t, grantable, EventCategoryIdentities, "identities is subscriber-facing data")

	// THE ASSERTION THAT MATTERS MOST HERE, and the one a reader of the previous contract is
	// most likely to expect the opposite of.
	assert.NotContains(t, grantable, EventCategorySystem,
		"the system category carries system.error's frozen verbatim-error body and is the "+
			"catalogue's catch-all, so it is not in the DEFAULT grant set: it is an operator topic, "+
			"in the same class as every <topic>.dlt, and reaching it takes the deployment-level "+
			"acknowledgement SubscriberPrivilegedEventCategories governs")

	assert.Equal(t,
		[]string{
			EventCategoryTransactions, EventCategoryBalances, EventCategoryIdentities,
		},
		grantable,
		"the default grantable list is the three tenant categories in catalogue order: a missing one "+
			"makes a tenant category unreachable, and a fourth means the internal category reached the "+
			"default allowlist")

	// The privileged list is the other half of the same decision, and asserting both is
	// what keeps `system` a DEFERRED grant rather than a category that quietly
	// disappeared: an event type still routes to it, the topic is still created and
	// published to, and a deployment can still authorise it.
	assert.Equal(t, []string{EventCategorySystem}, SubscriberPrivilegedEventCategories(),
		"the privileged list is exactly the internal category: empty would leave system.error and "+
			"ledger.created with no subscriber route at all, and wider would make a tenant category "+
			"conditional on an acknowledgement it does not need")

	assert.Len(t, AllEventCategories(), 4,
		"the topic catalogue holds four categories; changing that obliges the provisioning script, "+
			"both compose stacks, the Kubernetes configuration and the subscriber documentation to "+
			"change with it — and every subscriber that has built against the published contract")
	assert.Contains(t, AllEventCategories(), EventCategorySystem,
		"the system category must still EXIST — it is the catch-all every uncatalogued event routes "+
			"to and the home of system.error; it is privileged, not absent")
	assert.Len(t, grantable, len(AllEventCategories())-len(SubscriberPrivilegedEventCategories()),
		"the default allowlist is the catalogue minus the privileged categories, so every category is "+
			"in exactly one of the two lists and none is unreachable by construction")

	// No dead-letter token can appear, whatever the category list becomes.
	for _, category := range grantable {
		assert.NotContains(t, category, ".dlt",
			"category %q must be a bare token: a dead-letter name reaching the grantable list would hand a subscriber every other subscriber's failed events", category)
	}

	// The accessors must hand back copies, or one caller's sort reorders every other
	// caller's view.
	grantable[0] = "mutated"
	assert.NotContains(t, SubscriberGrantableEventCategories(), "mutated",
		"SubscriberGrantableEventCategories must return a fresh slice")

	all := AllEventCategories()
	all[0] = "mutated"
	assert.NotContains(t, AllEventCategories(), "mutated",
		"AllEventCategories must return a fresh slice")

	privileged := SubscriberPrivilegedEventCategories()
	privileged[0] = "mutated"
	assert.NotContains(t, SubscriberPrivilegedEventCategories(), "mutated",
		"SubscriberPrivilegedEventCategories must return a fresh slice")
}

// TestSubscriberAuthorizableTopics_AddsTheInternalTopicOnlyWhenAcknowledged pins the
// ONE behavioural difference the deployment acknowledgement makes.
func TestSubscriberAuthorizableTopics_AddsTheInternalTopicOnlyWhenAcknowledged(t *testing.T) {
	const prefix = "blnk"

	withoutAck := SubscriberAuthorizableTopics(prefix, false)
	assert.Equal(t,
		[]string{"blnk.transactions", "blnk.balances", "blnk.identities"},
		withoutAck,
		"with no acknowledgement the allowlist is exactly the three tenant category topics")
	assert.NotContains(t, withoutAck, "blnk.system",
		"the internal topic must be absent by default: that default is the whole reason the "+
			"acknowledgement exists")
	assert.Equal(t, SubscriberGrantableTopics(prefix), withoutAck,
		"with nothing acknowledged the resolved allowlist and the default allowlist must be the "+
			"same list, or a caller that passed the flag and one that did not would disagree")

	withAck := SubscriberAuthorizableTopics(prefix, true)
	assert.Equal(t,
		[]string{"blnk.transactions", "blnk.balances", "blnk.identities", "blnk.system"},
		withAck,
		"the acknowledgement APPENDS the privileged topic and reorders nothing, so an inventory "+
			"rendered from either list reads the same up to the point the privileged names begin")

	// The membership tests must agree with the lists, in both deployment shapes: a layer that
	// composes the list and a layer that tests membership are different layers.
	assert.False(t, IsSubscriberAuthorizableTopicName("blnk.system", prefix, false),
		"the internal topic must be refused where nothing is acknowledged")
	assert.True(t, IsSubscriberAuthorizableTopicName("blnk.system", prefix, true),
		"and accepted where it is")
	assert.False(t, IsSubscriberGrantableTopicName("blnk.system", prefix),
		"the default-set membership test must never accept the privileged name, whatever the "+
			"deployment declares: it is the test that answers \"is this an ordinary grant?\"")
	assert.True(t, IsSubscriberPrivilegedTopicName("blnk.system", prefix),
		"the privileged-name test is what lets a refusal name the acknowledgement instead of the "+
			"allowlist")
	assert.False(t, IsSubscriberPrivilegedTopicName("blnk.identities", prefix),
		"a tenant topic must never be reported as privileged, or an operator would be told to set a "+
			"variable that has nothing to do with the refusal")

	// A dead-letter sibling stays out of both lists, whatever is acknowledged. The
	// acknowledgement widens the grant by exactly one name.
	for _, acknowledged := range []bool{false, true} {
		for _, dlt := range []string{"blnk.system.dlt", "blnk.identities.dlt", "blnk.transactions.dlt"} {
			assert.Falsef(t, IsSubscriberAuthorizableTopicName(dlt, prefix, acknowledged),
				"%q must never be grantable (acknowledged=%v): a dead-letter topic carries every other "+
					"subscriber's failed events together with Blnk's failure metadata", dlt, acknowledged)
		}
	}
}

// TestEventCategory_PrefixBoundaryIsExact pins the boundary between the prefix-matched
// bulk arm and the exact-matched arms.
func TestEventCategory_PrefixBoundaryIsExact(t *testing.T) {
	assert.Equal(t, EventCategoryTransactions, EventCategory("bulk_transaction."),
		"the bare prefix with an empty status must still satisfy the prefix test; requiring a non-empty suffix would be a stricter rule than the producer guarantees")

	assert.Equal(t, EventCategorySystem, EventCategory("bulk_transaction"),
		"\"bulk_transaction\" without the trailing dot is not an emitted event string and must NOT satisfy the prefix test — the separator is part of the prefix, so dropping it from the constant is caught here")
	assert.False(t, IsCataloguedEventType("bulk_transaction"),
		"and it must be reported as uncatalogued, which is how the mis-spelling becomes visible rather than looking like a routed event")

	assert.Equal(t, EventCategorySystem, EventCategory("transaction.applied.v2"),
		"the transaction entries are exact matches, not prefix matches: a longer string that merely starts with an emitted name must fall through to the catch-all rather than being routed as if it were that event")
	assert.False(t, IsCataloguedEventType("transaction.applied.v2"),
		"a near-miss of a catalogued name is uncatalogued")
}

// TestEventCategory_IsCaseSensitive documents that no case normalisation happens.
//
// Producers pass the exact literals, so folding case would add a branch with no
// behavioural benefit that then has to be mutation-tested.
func TestEventCategory_IsCaseSensitive(t *testing.T) {
	assert.Equal(t, EventCategorySystem, EventCategory("TRANSACTION.APPLIED"),
		"comparison is exact and case-sensitive: an upper-cased event type is not an emitted event string and must fall through to the catch-all, not be folded into the transactions category")

	assert.Equal(t, EventCategorySystem, EventCategory("BULK_TRANSACTION.applied"),
		"the prefix test is case-sensitive too, for the same reason: producers compose the lower-case prefix literally")

	assert.False(t, IsCataloguedEventType("TRANSACTION.APPLIED"),
		"case folding is absent from the catalogue membership test as well, or the two readers of eventTypeCategories would disagree about the same input")
}

// TestBulkTransactionEventPrefix_IsTheProducerLiteral pins the prefix constant to the
// exact literal the bulk producer composes its event names from.
//
// The producer builds the name as "bulk_transaction." + status.
func TestBulkTransactionEventPrefix_IsTheProducerLiteral(t *testing.T) {
	assert.Equal(t, "bulk_transaction.", bulkTransactionEventPrefix,
		"the prefix must match the literal the bulk transaction producer prepends to the batch status, including the trailing dot separator")
}

// TestSchemaVersionV1_IsOne pins the initial envelope version.
func TestSchemaVersionV1_IsOne(t *testing.T) {
	assert.Equal(t, 1, SchemaVersionV1,
		"the canonical event schema starts at version 1: subscribers branch on schema_version to tell an additive change they can ignore from a breaking reshaping, so the first published version must be 1")

	// Pinning the type as well as the value matters because the wire form differs: an
	// integer marshals as 1 while a string would marshal as "1", and the schema specifies
	// an integer.
	assert.Equal(t, reflect.Int, reflect.TypeOf(SchemaVersionV1).Kind(),
		"schema_version is specified as an integer, so the constant must default to int and marshal as 1 rather than \"1\"")
}

// TestEventCategoryConstants_HaveWireValues pins every category token.
//
// These are bare tokens, not topic names: the topic-naming layer composes
// "<prefix>.<category>" and "<prefix>.<category>.dlt" from them.
func TestEventCategoryConstants_HaveWireValues(t *testing.T) {
	assert.Equal(t, "transactions", EventCategoryTransactions,
		"the transactions token composes blnk.transactions and blnk.transactions.dlt")
	assert.Equal(t, "balances", EventCategoryBalances,
		"the balances token composes blnk.balances and blnk.balances.dlt")
	assert.Equal(t, "identities", EventCategoryIdentities,
		"the identities token composes blnk.identities and blnk.identities.dlt")
	assert.Equal(t, "system", EventCategorySystem,
		"the system token composes blnk.system and blnk.system.dlt — the internal category that carries ledger.created and system.error, and the catch-all that keeps an unrecognised event type published rather than dropped")

	assert.Len(t, AllEventCategories(), 4,
		"the topic contract is four categories and eight topics: changing it obliges the provisioning script, the Kubernetes configuration, the local stack and every subscriber's topic list to change with it")

	// Every token must be mutually distinct, or two categories would collapse onto one
	// topic and a subscriber filtering by topic would receive events it never subscribed
	// to.
	distinct := map[string]struct{}{
		EventCategoryTransactions: {},
		EventCategoryBalances:     {},
		EventCategoryIdentities:   {},
		EventCategorySystem:       {},
	}
	assert.Len(t, distinct, len(AllEventCategories()),
		"the category tokens must be mutually distinct: two sharing a value would silently merge two topics into one")
	// Derived from the canonical order rather than counted by hand, so a category added
	// there without a literal pinned above fails here instead of shipping unpinned.
	for _, category := range AllEventCategories() {
		assert.Contains(t, distinct, category,
			"category %q has no pinned wire value; add one above", category)
	}

	// A category token must never contain the separator the topic-naming layer uses, or
	// "<prefix>.<category>" would produce an extra segment and the resulting topic would
	// not be the documented one.
	for _, category := range []string{EventCategoryTransactions, EventCategoryBalances, EventCategoryIdentities, EventCategorySystem} {
		for i := 0; i < len(category); i++ {
			assert.NotEqual(t, byte('.'), category[i],
				"category token %q must not contain a dot: the topic-naming layer owns the separator, and an embedded one would add an unintended topic segment", category)
		}
	}
}

// TestEventCatalogue_CategorySetIsClosed is the guard against the topic catalogue
// growing a category that the rest of the repository does not know about.
//
// The catalogue is a contract subscribers build against, so widening it needs its own
// approval rather than being a decision an implementation may take.
func TestEventCatalogue_CategorySetIsClosed(t *testing.T) {
	require.ElementsMatch(t,
		[]string{"transactions", "balances", "identities", "system"},
		AllEventCategories(),
		"the topic catalogue is a published four-category contract. A further category here is a "+
			"topic that events route to and that scripts/kafka-provision.sh, both compose "+
			"stacks, blnk-config.yaml, .env.example, the makefile and the operator docs do "+
			"not create. Change all of them, then change this expectation",
	)

	// The canonical order is asserted separately from membership, because the ORDER fixes
	// the order of AllTopics, AllDeadLetterTopics and AllTopicsWithDeadLetters, and
	// operators diff those listings against the provisioning script's output.
	assert.Equal(t,
		[]string{"transactions", "balances", "identities", "system"},
		AllEventCategories(),
		"the canonical category order is what makes topic inventories, reports and their "+
			"diffs against provisioning comparable; reordering it silently changes every "+
			"listing the operator runbooks tell an operator to compare. The tenant categories "+
			"come first and contiguously, so the default grant set is a PREFIX of this order "+
			"and the privileged category is what the resolved allowlist appends",
	)

	// EVERY CATALOGUED EVENT TYPE MUST LAND IN THAT SET. This is the half that makes the
	// closure meaningful in the other direction: a category could be removed from the
	// order above while an event type still mapped to it, which would leave the event
	// routing to a topic nothing provisions — the same failure, reached from the far side.
	catalogued := map[string]struct{}{}
	for _, category := range AllEventCategories() {
		catalogued[category] = struct{}{}
	}

	for eventType, category := range eventTypeCategories {
		assert.Containsf(t, catalogued, category,
			"event type %q maps to category %q, which is not in the canonical catalogue; the "+
				"event would publish to a topic nothing provisions", eventType, category)
	}

	// And the runtime-composed bulk family, which is matched by prefix and therefore
	// absent from the table above.
	assert.Contains(t, catalogued, EventCategory("bulk_transaction.applied"),
		"the bulk transaction family must route into the canonical catalogue")

	// The catch-all included: an unrecognised event type must land on a catalogued topic,
	// or the never-drop-an-event guarantee would publish to a name nothing created.
	assert.Contains(t, catalogued, EventCategory("something.nobody.mapped"),
		"the catch-all must resolve to a catalogued category")
}

// TestPublishStatus_HasExactlyTheThreeReportedOutcomes pins the per-attempt publish
// outcome vocabulary — ALL of it.
//
// The metrics layer uses these values verbatim as metric attribute values — the
// publish-attempts counter is attributed by outcome — so a drift here does not break a
// build, it breaks the alerting queries that select on those attribute values.
// Declaring the vocabulary once and pinning it once is what stops the code and the
// label set from diverging unnoticed.
func TestPublishStatus_HasExactlyTheThreeReportedOutcomes(t *testing.T) {
	// PublishStatus is a named string type, so the comparison is made on the converted
	// value: a typed constant and an untyped string literal are not deeply equal, and
	// asserting them directly would fail for the wrong reason.
	assert.Equal(t, "dispatched", string(PublishStatusDispatched),
		"a broker acknowledgement is reported as \"dispatched\" — the value the publish-attempts counter carries as its outcome attribute")
	assert.Equal(t, "retrying", string(PublishStatusRetrying),
		"every failed attempt is reported as \"retrying\"; whether another attempt follows is the relay's decision against the row's budget, not a value in this domain")
	assert.Equal(t, "dead_lettered", string(PublishStatusDeadLettered),
		"an ACKNOWLEDGED write to a <topic>.dlt sibling is reported as \"dead_lettered\", the value the dead-letter alerting rule selects on")

	assert.Equal(t, reflect.String, reflect.TypeOf(PublishStatusDispatched).Kind(),
		"PublishStatus must remain a string-kinded named type so it can be used directly as a metric attribute value without conversion tables")

	// THE SET IS CLOSED, asserted as an exact set rather than a count so that swapping one
	// value for another fails too — a swap renames a metric label an operator's dashboard
	// selects on, which is a silent break rather than a loud one.
	require.ElementsMatch(t,
		[]PublishStatus{PublishStatusDispatched, PublishStatusRetrying, PublishStatusDeadLettered},
		AllPublishStatuses(),
		"requirement R-3 fixes the per-attempt outcome vocabulary at three values, and it is a "+
			"published metric label domain: a fourth here silently changes what every existing "+
			"dashboard selection and alert rule matches. Carry a new distinction on "+
			"PublishResult instead")

	distinct := map[PublishStatus]struct{}{}
	for _, status := range AllPublishStatuses() {
		distinct[status] = struct{}{}
	}
	assert.Len(t, distinct, 3,
		"exactly three outcomes are reported, and they must be mutually distinct: collapsing two would make a retry indistinguishable from a success in the metrics")

	// The outcome vocabulary and the durable row-state vocabulary overlap in spelling and
	// must not be confused: no attempt outcome is persisted as a row status, and a row's
	// terminal failure state is "dead_lettered".
	assert.NotEqual(t, EventOutboxStatusFailed, string(PublishStatusRetrying),
		"the attempt outcome and the row state vocabularies are separate; a retrying attempt is not a failed row")
	for _, status := range AllPublishStatuses() {
		assert.NotEqual(t, EventOutboxStatusFailed, string(status),
			"no per-attempt outcome may share the spelling of the failed row state, which would "+
				"let a metric label be mistaken for a durable status")
	}
}

// TestEventOutboxStatus_ValuesAndLineageRelationship pins the durable outbox state
// vocabulary and its documented value-level relationship to the lineage outbox.
func TestEventOutboxStatus_ValuesAndLineageRelationship(t *testing.T) {
	t.Run("the replaying state has its stored value and is distinct", func(t *testing.T) {
		assert.Equal(t, "replaying", EventOutboxStatusReplaying,
			"replaying is the transient state a replay CLAIMS a dead-lettered row into, which is what stops two concurrent replays from both publishing the same event")

		// It must not collide with any other state, or a replay claim would be
		// indistinguishable from an ordinary state and the conditional transition would match
		// rows it must not touch.
		for name, value := range map[string]string{
			"pending":         EventOutboxStatusPending,
			"processing":      EventOutboxStatusProcessing,
			"webhook_pending": EventOutboxStatusWebhookPending,
			"dispatched":      EventOutboxStatusDispatched,
			"failed":          EventOutboxStatusFailed,
			"dead_lettered":   EventOutboxStatusDeadLettered,
		} {
			assert.NotEqual(t, value, EventOutboxStatusReplaying,
				"replaying must be distinct from %s, or the replay claim's conditional UPDATE would match rows in that state too", name)
		}
	})

	t.Run("the six non-replay durable states have their stored values", func(t *testing.T) {
		assert.Equal(t, "pending", EventOutboxStatusPending,
			"pending is the initial state set by the column default when the row is inserted alongside the ledger mutation, and the value the pending partial index filters on")
		assert.Equal(t, "processing", EventOutboxStatusProcessing,
			"processing means a relay holds a lease on the row, and is one of the two values the composite claim index is restricted to")
		assert.Equal(t, "webhook_pending", EventOutboxStatusWebhookPending,
			"webhook_pending is the dual-delivery state: the Kafka leg is published and only the legacy webhook is still owed. Its literal value is what the relay's claim predicate filters on, so a rename here silently strands outstanding webhooks rather than failing to compile")
		assert.Equal(t, "dispatched", EventOutboxStatusDispatched,
			"dispatched is the success terminal state, named to match the dispatched_at column")
		assert.Equal(t, "failed", EventOutboxStatusFailed,
			"failed means the retry budget was exhausted, and is the value the failed partial index filters on")
		assert.Equal(t, "dead_lettered", EventOutboxStatusDeadLettered,
			"dead_lettered is the failure terminal state and the only state eligible for replay, so the dead-letter listing query selects exactly this value")

		distinct := map[string]struct{}{
			EventOutboxStatusPending:        {},
			EventOutboxStatusProcessing:     {},
			EventOutboxStatusWebhookPending: {},
			EventOutboxStatusDispatched:     {},
			EventOutboxStatusFailed:         {},
			EventOutboxStatusDeadLettered:   {},
		}
		assert.Len(t, distinct, 6,
			"the six states must be mutually distinct: two sharing a value would make the state machine ambiguous and the claim query non-deterministic")
	})

	t.Run("three values are shared with the lineage outbox vocabulary", func(t *testing.T) {
		// The overlap is asserted BY VALUE, not by reusing the lineage constants in event.go.
		// That is the documented arrangement: the shared values are what let the
		// partial-index convention proven on the lineage outbox table carry over to the event
		// outbox table unchanged, while the two vocabularies stay separate declarations so
		// the two state machines — separate tables served by separate relays — can evolve
		// independently. lineage.go is not modified by this feature; this test pins the
		// relationship without touching it.
		assert.Equal(t, OutboxStatusPending, EventOutboxStatusPending,
			"the pending value is shared with the lineage outbox so the pending partial index convention carries over unchanged")
		assert.Equal(t, OutboxStatusProcessing, EventOutboxStatusProcessing,
			"the processing value is shared with the lineage outbox so the composite claim index convention carries over unchanged")
		assert.Equal(t, OutboxStatusFailed, EventOutboxStatusFailed,
			"the failed value is shared with the lineage outbox so the failed partial index convention carries over unchanged")
	})

	t.Run("the event vocabulary diverges where the pipelines differ", func(t *testing.T) {
		// completed has no counterpart: dispatched names the same idea in this pipeline's
		// language and matches its column. dead_lettered has no lineage equivalent at all,
		// because the lineage machine has no terminal state meaning "we gave up and preserved
		// the event for replay".
		assert.NotEqual(t, OutboxStatusCompleted, EventOutboxStatusDispatched,
			"the event pipeline's success state is \"dispatched\", not the lineage outbox's \"completed\": the two must not be conflated, because the event table's column is dispatched_at")
		assert.NotEqual(t, OutboxStatusCompleted, EventOutboxStatusDeadLettered,
			"dead_lettered has no lineage equivalent — it is the state that distinguishes a preserved, replayable event from a plain failure")
		assert.NotEqual(t, OutboxStatusFailed, EventOutboxStatusDeadLettered,
			"failed and dead_lettered are distinct states: a row is failed once its budget is spent and only becomes dead_lettered once the event has actually been written to its dead-letter topic, and only the latter is replayable")
	})
}

// legacyWebhookBody is the payload fixture used by the byte-fidelity tests.
//
// Three properties of this literal are deliberate:
//
//   - It is COMPACT — no spaces, no newlines. encoding/json compacts a json.RawMessage
//     when marshalling it, so byte equality is only observable when the input is
//     already compact.
//   - Its keys are in NON-ALPHABETICAL order ("zeta" before "alpha").
//   - It carries a HIGH-PRECISION numeric string that no float64 can represent exactly.
const legacyWebhookBody = `{"event":"transaction.applied","data":{"zeta":1,"alpha":2,"precise_amount":"100000000000000000001"}}`

// TestLedgerEvent_PayloadIsRawMessage pins the payload field type on both the wire
// envelope and the persisted row.
//
// The type is the guarantee.
func TestLedgerEvent_PayloadIsRawMessage(t *testing.T) {
	rawMessageType := reflect.TypeOf(json.RawMessage(nil))

	t.Run("LedgerEvent.Payload", func(t *testing.T) {
		field, ok := reflect.TypeOf(LedgerEvent{}).FieldByName("Payload")
		require.True(t, ok, "LedgerEvent must declare a Payload field: it is the field that carries the legacy webhook body verbatim")
		assert.Equal(t, rawMessageType, field.Type,
			"LedgerEvent.Payload must be json.RawMessage so the published bytes are the bytes the producer marshaled; a map or a typed struct would reorder keys and renormalise numbers")
	})

	t.Run("EventOutbox.Payload", func(t *testing.T) {
		field, ok := reflect.TypeOf(EventOutbox{}).FieldByName("Payload")
		require.True(t, ok, "EventOutbox must declare a Payload field: it is the stored copy both transports read during the dual-delivery window")
		assert.Equal(t, rawMessageType, field.Type,
			"EventOutbox.Payload must be json.RawMessage so the bytes held in the payload_raw BYTEA column are neither reordered nor renormalised between the insert and the publish — that identity is what makes the two transports' payloads equal structurally rather than by careful coding")
	})

	t.Run("EventOutbox.FailureMetadata", func(t *testing.T) {
		field, ok := reflect.TypeOf(EventOutbox{}).FieldByName("FailureMetadata")
		require.True(t, ok, "EventOutbox must declare a FailureMetadata field: it is the stored dead-letter diagnostic record")
		assert.Equal(t, rawMessageType, field.Type,
			"EventOutbox.FailureMetadata must be json.RawMessage so the stored bytes are handed back to the dead-letter API exactly as they were written, rather than being re-rendered by a decode-and-re-encode round trip")
	})
}

// TestLedgerEvent_PayloadBytesSurviveMarshalUnchanged is the byte-identity round trip.
//
// Dual-delivery consistency holds because the relay publishes to Kafka and enqueues the
// legacy webhook task from the SAME claimed outbox row, so both transports carry the
// same payload bytes and cannot drift apart.
func TestLedgerEvent_PayloadBytesSurviveMarshalUnchanged(t *testing.T) {
	raw := json.RawMessage(legacyWebhookBody)

	event := LedgerEvent{
		EventID:       "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
		EventType:     "transaction.applied",
		AggregateID:   "txn_9f2b1c",
		OccurredAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Payload:       raw,
		SchemaVersion: SchemaVersionV1,
	}

	t.Run("payload bytes are identical after a marshal round trip", func(t *testing.T) {
		decoded := decodeObject(t, mustMarshal(t, event))

		got, ok := decoded["payload"]
		require.True(t, ok, "the marshaled envelope must carry a payload key")
		assert.Equal(t, legacyWebhookBody, string(got),
			"the payload bytes must survive marshalling unchanged, byte for byte. This identity is what makes the Kafka message and the legacy webhook body equal during the dual-delivery window, and what lets a dead-letter replay reproduce the original event exactly by re-publishing stored bytes instead of re-marshalling a struct")

		// State the two failure modes explicitly, so a regression reports which
		// one occurred rather than only that the bytes differ.
		assert.Equal(t, `{"event":"transaction.applied","data":{"zeta":1,"alpha":2,"precise_amount":"100000000000000000001"}}`, string(got),
			"neither key order nor number rendering may change: \"zeta\" must still precede \"alpha\" (no map round trip sorted them) and the 21-digit precise amount must still be the exact literal (no float64 renormalised it)")
	})

	t.Run("the payload is not re-nested or wrapped", func(t *testing.T) {
		decoded := decodeObject(t, mustMarshal(t, event))

		// Decode one level in and assert the two legacy keys are still the payload's own
		// top-level keys. If the envelope ever wrapped the payload in another object, an
		// existing subscriber's body parser would break even though the bytes round-tripped.
		inner := decodeObject(t, decoded["payload"])
		assert.Len(t, inner, 2,
			"the payload must remain the legacy webhook body's own two-key object — the entire {\"event\": ..., \"data\": ...} object is carried, both keys included, so a subscriber's existing parser works unchanged")
		assert.Equal(t, `"transaction.applied"`, string(inner["event"]),
			"the payload's inner event key is preserved verbatim, which is why the envelope's event_type is a redundant convenience for routing rather than a replacement for it")
		assert.Equal(t, `{"zeta":1,"alpha":2,"precise_amount":"100000000000000000001"}`, string(inner["data"]),
			"the payload's data object is preserved verbatim, including its declared key order")
	})

	t.Run("a pretty-printed payload is compacted, never reordered", func(t *testing.T) {
		// Documented separately so it cannot weaken the byte-identity assertion above.
		// encoding/json strips insignificant whitespace from a json.RawMessage when
		// marshalling it, but it never reorders keys and never renormalises number literals.
		// The practical consequence for the producers: store the payload compact, and byte
		// identity holds all the way through.
		pretty := LedgerEvent{
			EventID:       event.EventID,
			EventType:     event.EventType,
			AggregateID:   event.AggregateID,
			OccurredAt:    event.OccurredAt,
			SchemaVersion: event.SchemaVersion,
			Payload: json.RawMessage("{\n  \"event\": \"transaction.applied\",\n  " +
				"\"data\": {\n    \"zeta\": 1,\n    \"alpha\": 2,\n    " +
				"\"precise_amount\": \"100000000000000000001\"\n  }\n}"),
		}

		decoded := decodeObject(t, mustMarshal(t, pretty))
		assert.Equal(t, legacyWebhookBody, string(decoded["payload"]),
			"insignificant whitespace is stripped, but the key order and the exact numeric literal are untouched — so a payload stored compact is republished byte-identically, and one stored pretty-printed is compacted deterministically rather than reshaped")
	})
}

// TestLedgerEvent_WireContract pins the exact serialised shape of the envelope: the
// complete key set, the timestamp format, and the schema version.
//
// The key set is asserted by LENGTH as well as by membership, so that adding a field to
// the struct fails here.
func TestLedgerEvent_WireContract(t *testing.T) {
	event := LedgerEvent{
		EventID:       "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
		EventType:     "balance.monitor",
		AggregateID:   "bln_4c7d2e",
		OccurredAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Payload:       json.RawMessage(legacyWebhookBody),
		SchemaVersion: SchemaVersionV1,
	}
	decoded := decodeObject(t, mustMarshal(t, event))

	t.Run("the key set is exactly the six documented keys", func(t *testing.T) {
		for _, key := range []string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version"} {
			assert.Contains(t, decoded, key,
				"the envelope must always carry %q: no field is omitempty, so a subscriber can rely on all six keys being present on every message even when a value is at its zero value", key)
		}
		assert.Len(t, decoded, 6,
			"the envelope must carry exactly six keys — an added or renamed key is a change to the subscriber-facing contract and must be a deliberate decision, not a side effect of editing the struct")
	})

	t.Run("the declared json tag names match the wire keys", func(t *testing.T) {
		assert.Equal(t,
			[]string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version"},
			jsonTagNames(t, reflect.TypeOf(LedgerEvent{})),
			"the tags must be the documented snake_case keys in declaration order: encoding/json emits struct keys in that order, so the sequence is itself part of the observable wire shape")
	})

	t.Run("occurred_at is RFC3339", func(t *testing.T) {
		assert.Equal(t, `"2026-01-02T03:04:05Z"`, string(decoded["occurred_at"]),
			"occurred_at must serialise as RFC3339. time.Time's standard encoding already produces it, so no custom marshaller is defined or wanted — adding one would be a way to break the documented format silently")

		var occurredAt string
		require.NoError(t, json.Unmarshal(decoded["occurred_at"], &occurredAt), "occurred_at must be a JSON string")
		assert.Equal(t, "2026-01-02T03:04:05Z", occurredAt,
			"a UTC instant with zero nanoseconds encodes unambiguously with the Z designator and no fractional part")

		parsed, err := time.Parse(time.RFC3339, occurredAt)
		require.NoError(t, err, "the emitted timestamp must parse back with time.RFC3339: a subscriber in any language relies on that")
		assert.True(t, parsed.Equal(event.OccurredAt),
			"the round-tripped instant must equal the original — no truncation and no zone shift")
	})

	t.Run("schema_version marshals as the integer 1", func(t *testing.T) {
		assert.Equal(t, "1", string(decoded["schema_version"]),
			"schema_version must marshal as the bare integer 1, not the string \"1\": subscribers branch on it numerically")

		var schemaVersion int
		require.NoError(t, json.Unmarshal(decoded["schema_version"], &schemaVersion), "schema_version must be a JSON number")
		assert.Equal(t, SchemaVersionV1, schemaVersion,
			"the serialised version must be the value of SchemaVersionV1, so a future bump of that constant flows straight to the wire with no second literal to remember")
	})

	t.Run("the scalar envelope fields are carried verbatim", func(t *testing.T) {
		assert.Equal(t, `"8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f"`, string(decoded["event_id"]),
			"event_id is carried verbatim: it is the subscriber idempotency key, so any transformation of it would break duplicate suppression")
		assert.Equal(t, `"balance.monitor"`, string(decoded["event_type"]),
			"event_type is carried verbatim so subscribers can route and filter on it without parsing the payload")
		assert.Equal(t, `"bln_4c7d2e"`, string(decoded["aggregate_id"]),
			"aggregate_id is carried verbatim: it is what a consumer groups by once the messages arrive")
	})
}

// TestFailureMetadata_HasExactlyFiveFields pins the dead-letter diagnostic record.
//
// The five fields are exactly the five the requirement enumerates — original topic,
// error reason, attempt count, first attempted at, last attempted at — and they answer
// the three questions an operator triaging a dead-lettered event always has: where was
// this meant to go, why did it not get there, and over what window did we try.
func TestFailureMetadata_HasExactlyFiveFields(t *testing.T) {
	typ := reflect.TypeOf(FailureMetadata{})

	assert.Equal(t, 5, typ.NumField(),
		"the failure metadata must carry exactly the five specified fields: fewer leaves an operator unable to triage, and more is an undocumented addition to a published dead-letter schema")

	assert.Equal(t,
		[]string{"original_topic", "error_reason", "attempt_count", "first_attempted_at", "last_attempted_at"},
		jsonTagNames(t, typ),
		"the json tags are the published dead-letter schema — a rename would break every operator query and dashboard reading them, with no compile error to warn of it")

	t.Run("field types match their meaning", func(t *testing.T) {
		attemptCount, ok := typ.FieldByName("AttemptCount")
		require.True(t, ok, "FailureMetadata must declare AttemptCount")
		assert.Equal(t, reflect.Int, attemptCount.Type.Kind(),
			"attempt_count must be an integer so it marshals as a bare number an alerting rule can compare numerically")

		for _, name := range []string{"FirstAttemptedAt", "LastAttemptedAt"} {
			field, ok := typ.FieldByName(name)
			require.True(t, ok, "FailureMetadata must declare %s", name)
			assert.Equal(t, reflect.TypeOf(time.Time{}), field.Type,
				"%s must be a plain time.Time, not a pointer: by the time an event is dead-lettered it has definitively been attempted, so both timestamps are always known and neither is nullable", name)
		}
	})

	t.Run("the record serialises with all five keys", func(t *testing.T) {
		first := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		last := time.Date(2026, 1, 2, 3, 4, 35, 0, time.UTC)

		decoded := decodeObject(t, mustMarshal(t, FailureMetadata{
			OriginalTopic:    "blnk.transactions",
			ErrorReason:      "write tcp 10.0.0.4:9092: broken pipe",
			AttemptCount:     5,
			FirstAttemptedAt: first,
			LastAttemptedAt:  last,
		}))

		assert.Len(t, decoded, 5, "all five keys must be present — none is omitempty, so an operator never has to distinguish a missing key from a zero value")
		assert.Equal(t, `"blnk.transactions"`, string(decoded["original_topic"]),
			"original_topic records the topic a replay must send the event back to, so it is the field the replay path depends on")
		assert.Equal(t, `"write tcp 10.0.0.4:9092: broken pipe"`, string(decoded["error_reason"]),
			"error_reason carries the final attempt's failure verbatim, taken from the row's last recorded error")
		assert.Equal(t, "5", string(decoded["attempt_count"]),
			"attempt_count marshals as a bare integer: it is how many attempts were made before giving up, matching the configured retry budget of five")
		assert.Equal(t, `"2026-01-02T03:04:05Z"`, string(decoded["first_attempted_at"]),
			"first_attempted_at is RFC3339, consistent with the envelope's occurred_at")
		assert.Equal(t, `"2026-01-02T03:04:35Z"`, string(decoded["last_attempted_at"]),
			"last_attempted_at is RFC3339; together with first_attempted_at it bounds the window over which the failure persisted, which is what distinguishes a momentary broker blip from a sustained outage")
	})
}

// TestFailureMetadata_MarshalsAsAdditiveSibling proves the attachment is strictly
// additive, which is the precondition for byte-faithful replay.
//
// The dead-letter message is the envelope's own keys plus one sibling top-level key.
func TestFailureMetadata_MarshalsAsAdditiveSibling(t *testing.T) {
	envelopeKeys := []string{"event_id", "event_type", "aggregate_id", "occurred_at", "payload", "schema_version"}

	event := LedgerEvent{
		EventID:       "8f14e45f-ea8f-4b3a-9c2d-0a7b6c5d4e3f",
		EventType:     "transaction.applied",
		AggregateID:   "txn_9f2b1c",
		OccurredAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Payload:       json.RawMessage(legacyWebhookBody),
		SchemaVersion: SchemaVersionV1,
	}
	failure := FailureMetadata{
		OriginalTopic:    "blnk.transactions",
		ErrorReason:      "write tcp 10.0.0.4:9092: broken pipe",
		AttemptCount:     5,
		FirstAttemptedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		LastAttemptedAt:  time.Date(2026, 1, 2, 3, 4, 35, 0, time.UTC),
	}

	// Compose the dead-letter message the way the dead-letter publisher does: decode the
	// envelope into raw values, add exactly one key, re-encode. Decoding into
	// json.RawMessage is what keeps the payload bytes untouched while the sibling key is
	// added around them.
	deadLetter := decodeObject(t, mustMarshal(t, event))
	deadLetter["failure_metadata"] = mustMarshal(t, failure)
	deadLetterBytes := mustMarshal(t, deadLetter)

	t.Run("failure_metadata is a top-level sibling of the envelope keys", func(t *testing.T) {
		decoded := decodeObject(t, deadLetterBytes)

		assert.Len(t, decoded, 7,
			"the dead-letter message must be the six envelope keys plus exactly one added key — nothing removed, nothing replaced")
		assert.Contains(t, decoded, "failure_metadata",
			"failure_metadata must sit at the TOP LEVEL beside the envelope keys, not inside the payload: nesting it would rewrite the payload bytes and destroy replay fidelity")
		for _, key := range envelopeKeys {
			assert.Contains(t, decoded, key,
				"the envelope key %q must survive the attachment untouched", key)
		}

		// The added key must decode back to the metadata that was attached,
		// proving the attachment is a real record and not an opaque marker.
		var roundTripped FailureMetadata
		require.NoError(t, json.Unmarshal(decoded["failure_metadata"], &roundTripped),
			"the attached metadata must decode back into FailureMetadata")
		assert.Equal(t, failure.OriginalTopic, roundTripped.OriginalTopic,
			"the original topic must survive the attachment: it is what the replay path sends the event back to")
		assert.Equal(t, failure.ErrorReason, roundTripped.ErrorReason, "the error reason must survive the attachment")
		assert.Equal(t, failure.AttemptCount, roundTripped.AttemptCount, "the attempt count must survive the attachment")
		assert.True(t, roundTripped.FirstAttemptedAt.Equal(failure.FirstAttemptedAt), "the first-attempted instant must survive the attachment")
		assert.True(t, roundTripped.LastAttemptedAt.Equal(failure.LastAttemptedAt), "the last-attempted instant must survive the attachment")
	})

	t.Run("the payload is unchanged byte for byte by the attachment", func(t *testing.T) {
		decoded := decodeObject(t, deadLetterBytes)
		assert.Equal(t, legacyWebhookBody, string(decoded["payload"]),
			"attaching failure metadata must not touch one byte of the payload — the dead-lettered event carries the same bytes the original publish attempt carried, which is what makes the replay comparison byte-for-byte rather than approximate")
	})

	t.Run("removing failure_metadata reproduces the original envelope", func(t *testing.T) {
		stripped := decodeObject(t, deadLetterBytes)
		delete(stripped, "failure_metadata")
		strippedBytes := mustMarshal(t, stripped)
		decoded := decodeObject(t, strippedBytes)

		assert.Len(t, decoded, 6,
			"stripping the one added key must leave exactly the six envelope keys: that is the operational definition of an additive attachment, and it is why a replay can recover the original event from the dead-letter message")
		for _, key := range envelopeKeys {
			assert.Contains(t, decoded, key, "envelope key %q must be recovered after stripping the metadata", key)
		}
		assert.NotContains(t, decoded, "failure_metadata", "the metadata must be fully removable, leaving no residue behind")

		assert.Equal(t, legacyWebhookBody, string(decoded["payload"]),
			"the recovered payload must be byte-identical to the original — this is precisely the assertion behind the requirement that a replayed event matches the original byte-for-byte aside from the failure metadata")

		// Every scalar field must be recovered exactly too, so the recovered
		// envelope is the original event and not merely something shaped like it.
		var recovered LedgerEvent
		require.NoError(t, json.Unmarshal(strippedBytes, &recovered),
			"the stripped message must decode back into a LedgerEvent")
		assert.Equal(t, event.EventID, recovered.EventID,
			"the event_id must be recovered unchanged: it is the idempotency key a subscriber deduplicates a replayed event on")
		assert.Equal(t, event.EventType, recovered.EventType, "the event_type must be recovered unchanged so the replay routes to the same topic")
		assert.Equal(t, event.AggregateID, recovered.AggregateID, "the aggregate_id must be recovered unchanged")
		assert.Equal(t, event.SchemaVersion, recovered.SchemaVersion, "the schema_version must be recovered unchanged")
		assert.True(t, recovered.OccurredAt.Equal(event.OccurredAt),
			"the occurred_at instant must be recovered unchanged: a replay reports when the domain action happened, not when it was replayed")
		assert.Equal(t, legacyWebhookBody, string(recovered.Payload),
			"the decoded payload must still be the original bytes, because it is held as json.RawMessage all the way through")
	})
}

// TestEventOutbox_ColumnContract pins the persisted row's field set against the event
// outbox table's columns.
func TestEventOutbox_ColumnContract(t *testing.T) {
	typ := reflect.TypeOf(EventOutbox{})

	t.Run("the complete tag set matches the table columns", func(t *testing.T) {
		// The six documented groups, in declaration order: the event envelope, the relay
		// state machine, the dual-delivery marker, the broker coordinate, the dead-letter
		// record, and the captured trace context. Option suffixes such as ",omitempty" are
		// stripped before comparing, because the column name is the tag's name portion only.
		expected := []string{
			// event envelope
			"id", "event_id", "event_type", "aggregate_id", "partition_key", "ledger_id",
			// created_at accompanies occurred_at and is NOT a duplicate of it. occurred_at is
			// the DOMAIN instant and owns every ordering guarantee. The relay reads it so the
			// publish-latency histogram includes the queue wait — measuring from the claim
			// instead lets a relay an hour behind report the same p99 as an idle one. event_raw
			// is the CANONICAL ENVELOPE, composed once at capture and read by every later
			// transport.
			"topic", "schema_version", "payload", "event_raw", "occurred_at", "created_at",
			// relay state machine. next_attempt_at is the DURABLE form of the configured
			// backoff: the claim predicate is next_attempt_at <= NOW(), which is what keeps a
			// retrying row out of the claimable set for the delay the caller computed instead of
			// the relay having to sleep it.
			"status", "attempts", "max_attempts", "next_attempt_at", "last_error",
			"first_attempted_at", "last_attempted_at", "dispatched_at", "locked_until",
			"claim_token",
			// dual-delivery marker. All three go at the sunset. kafka_dispatched_at records the
			// KAFKA leg independently of dispatched_at, which is what lets a row whose webhook
			// enqueue failed stay claimable for the webhook alone instead of being marked
			// finished with that delivery lost; webhook_attempts is the legacy leg's own budget,
			// separate so a webhook receiver being down cannot spend a Kafka retry attempt.
			"webhook_dispatched", "kafka_dispatched_at", "webhook_attempts",
			// broker coordinate. WHERE this row's record actually landed, which is what turns
			// "this row claims a publication" into "this row IS that record" and so what makes
			// the zero-loss reconciliation able to detect loss at all: counting alone cannot
			// tell a surplus of redeliveries from a surplus that is masking an equal number of
			// losses.
			"kafka_topic", "kafka_partition", "kafka_offset",
			// dead-letter record
			"dlt_topic", "failure_metadata",
			// TRACE CONTEXT — the sixth group, and the only one that carries no ledger fact. It
			// is the W3C trace of the request that CAPTURED the event, written here because the
			// capture and the publish are decoupled: the request commits and returns, and the
			// relay claims the row later, so there is no in-memory context to hand over and a
			// trace not written on the row cannot be recovered from anything afterwards.
			"traceparent", "tracestate",
			// THERE IS NO OPERATOR-RESOLUTION GROUP, deliberately: a stored "an operator says
			// this dead-lettered entry needs no further action" fact cannot compose with a
			// replay in either order, and a dead-lettered row is ended by a replay the broker
			// acknowledges rather than by a resolution flag.
		}

		assert.Equal(t, expected, jsonTagNames(t, typ),
			"every json tag must match its event outbox column name, in the six documented groups: the repository scans rows into this struct, so a rename here is a runtime scan failure on a live ledger write rather than a compile error")
		assert.Equal(t, len(expected), typ.NumField(),
			"the row must declare exactly these %d fields — an extra field with no column, or a column with no field, breaks the insert and claim statements at runtime", len(expected))
	})

	t.Run("nullable timestamp columns are pointers", func(t *testing.T) {
		// Each of these columns is genuinely NULL for part of the row's life, so the Go field
		// must be able to represent absence. A plain time.Time would scan NULL as the zero
		// instant, making "never attempted" indistinguishable from "attempted at the zero
		// time" and, worse, making an unclaimed row look as though its lease had expired in
		// the year 1.
		timePointer := reflect.PointerTo(reflect.TypeOf(time.Time{}))

		nullable := map[string]string{
			"FirstAttemptedAt": "nil until the row is first claimed, so it distinguishes a never-attempted row from one attempted at the zero instant",
			"LastAttemptedAt":  "nil until the row is first claimed, and is what the retry bookkeeping advances on each attempt",
			"DispatchedAt":     "nil until the broker acknowledges the publish, and is set together with the dispatched status",
			"LockedUntil":      "nil while the row is unclaimed; a non-nil expired lease is what makes a crashed relay's in-flight work claimable again rather than stranded",
			// SUNSET: goes with the legacy leg. nil is load-bearing here — it is what the relay
			// reads as "the Kafka leg is not done yet", and a zero instant would instead read as
			// "published in the year 1", so the relay would skip the publish and the event would
			// never reach its topic.
			"KafkaDispatchedAt": "nil until the Kafka leg is acknowledged; a non-nil value is what tells the relay to deliver only the outstanding webhook and publish nothing",
		}
		for name, why := range nullable {
			field, ok := typ.FieldByName(name)
			require.True(t, ok, "EventOutbox must declare %s", name)
			assert.Equal(t, timePointer, field.Type,
				"%s must be *time.Time because its column is nullable: %s", name, why)
		}
	})

	t.Run("occurred_at is not nullable", func(t *testing.T) {
		field, ok := typ.FieldByName("OccurredAt")
		require.True(t, ok, "EventOutbox must declare OccurredAt")
		assert.Equal(t, reflect.TypeOf(time.Time{}), field.Type,
			"OccurredAt must be a plain time.Time, not a pointer: it is always known at insert time, and the relay claims rows in ascending occurred_at order, so a NULL would have no defined position in the FIFO ordering")
	})

	t.Run("created_at is not nullable and is distinct from occurred_at", func(t *testing.T) {
		created, ok := typ.FieldByName("CreatedAt")
		require.True(t, ok,
			"EventOutbox must declare CreatedAt: it is the durable capture instant acceptance criterion V-1's outbox-to-Kafka latency is measured from, and without it the relay can only time from the claim, which excludes the backlog")
		assert.Equal(t, reflect.TypeOf(time.Time{}), created.Type,
			"CreatedAt must be a plain time.Time: its column is NOT NULL with a NOW() default, so it always has a value")

		occurred, ok := typ.FieldByName("OccurredAt")
		require.True(t, ok)
		assert.NotEqual(t, created.Index, occurred.Index,
			"the two must be separate fields: a backfilled or replayed mutation carries an earlier occurred_at than its created_at, and collapsing them would either publish it out of domain order or misreport its latency")
	})

	t.Run("the state and counter fields have their storage types", func(t *testing.T) {
		id, ok := typ.FieldByName("ID")
		require.True(t, ok, "EventOutbox must declare ID")
		assert.Equal(t, reflect.Int64, id.Type.Kind(),
			"ID must be int64 to hold a BIGSERIAL surrogate key without overflow")

		for _, name := range []string{"Attempts", "MaxAttempts", "SchemaVersion", "WebhookAttempts"} {
			field, ok := typ.FieldByName(name)
			require.True(t, ok, "EventOutbox must declare %s", name)
			assert.Equal(t, reflect.Int, field.Type.Kind(),
				"%s must be an int: it maps to an integer column and is compared numerically by the claim query", name)
		}

		webhookDispatched, ok := typ.FieldByName("WebhookDispatched")
		require.True(t, ok, "EventOutbox must declare WebhookDispatched")
		assert.Equal(t, reflect.Bool, webhookDispatched.Type.Kind(),
			"WebhookDispatched must be a bool: it makes the legacy delivery leg individually idempotent, so a row republished to Kafka after a crash does not also re-enqueue a duplicate webhook")

		for _, name := range []string{
			"EventID", "EventType", "AggregateID", "PartitionKey", "LedgerID", "Topic",
			"Status", "LastError", "ClaimToken", "DLTTopic",
		} {
			field, ok := typ.FieldByName(name)
			require.True(t, ok, "EventOutbox must declare %s", name)
			assert.Equal(t, reflect.String, field.Type.Kind(),
				"%s must be a string: it maps to a text column", name)
		}
	})

	t.Run("optional fields are omitempty and required fields are not", func(t *testing.T) {
		// The distinction is not cosmetic. The row is serialised into dead-letter API
		// responses, where a required key must always be present so a caller never has to
		// distinguish a missing key from a zero value, while a field that is genuinely absent
		// for most of the row's life should not clutter every response with a null.
		omitempty := map[string]bool{
			"id": false, "event_id": false, "event_type": false, "aggregate_id": false,
			// partition_key is required: it is the Kafka message key and is never blank on a
			// persisted row, because a blank key would let the broker scatter the event
			// round-robin and silently destroy per-aggregate ordering. ledger_id, by contrast,
			// is legitimately absent for the event types that genuinely have no ledger.
			"partition_key": false, "ledger_id": true,
			"topic": false, "schema_version": false, "payload": false,
			// event_raw is the ONE always-populated column that is nevertheless omitted, and the
			// reason is the Go type rather than the column. It is []byte, so it renders as
			// BASE64 — an unreadable duplicate of the payload that is already rendered above as
			// JSON.
			"event_raw": true,
			// created_at is NOT NULL with a NOW() default, so it always has a value and is
			// always reported. It is what a reader of a dead-letter response needs to tell an
			// event that was captured minutes ago from one captured last week.
			"occurred_at": false, "created_at": false,
			"status": false, "attempts": false, "max_attempts": false,
			// next_attempt_at is NOT NULL on the table with a NOW() default, so it always has a
			// value and is always reported. Omitting it would make "due now" indistinguishable
			// from "the field was not populated", which is the one question an operator asks of
			// a row that is not being picked up.
			"next_attempt_at": false,
			"last_error":      true, "first_attempted_at": true, "last_attempted_at": true,
			"dispatched_at": true, "locked_until": true,
			// claim_token is empty on an unclaimed row and cleared at a terminal
			// state, so its absence is meaningful and should not render as a null.
			"claim_token": true,
			// webhook_attempts is NOT NULL with a zero default, exactly like attempts, so it is
			// always reported: a caller triaging a stuck legacy leg needs to see the zero.
			// kafka_dispatched_at IS omitted while it is nil, because "the Kafka leg is not
			// done" is genuinely the absence of a value rather than a zero one.
			"webhook_dispatched": false, "kafka_dispatched_at": true, "webhook_attempts": false,
			// The broker coordinate is omitted while absent, and its absence is MEANINGFUL: it
			// is what the zero-loss audit counts as an unconfirmed publication. Rendering
			// "kafka_partition": 0 for a row that has none would be worse than omitting it,
			// because partition 0 is a real partition and a reader would follow it.
			"kafka_topic": true, "kafka_partition": true, "kafka_offset": true,
			"dlt_topic": true, "failure_metadata": true,
			// Both trace values are omitted while absent, and absence is the COMMON case rather
			// than an edge one: every event captured without an active trace — a CLI mutation, a
			// worker-initiated rejection, a deployment with observability off — has neither.
			// Rendering "traceparent": "" would state that a trace was recorded and was empty,
			// which is not a state that exists, and a reader would try to resolve it.
			"traceparent": true, "tracestate": true,
			// resolved_at and resolution_note are ABSENT rather than expected-omitempty, because
			// the columns are gone. See the retirement note on the tag set above.
		}
		require.Len(t, omitempty, typ.NumField(), "every field must have a documented omitempty expectation")

		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := field.Tag.Get("json")
			name := jsonTagName(tag)
			want, known := omitempty[name]
			require.True(t, known, "unexpected json tag %q on field %s: the tag set is pinned above, so a new field must be added there deliberately", name, field.Name)

			hasOmitempty := tag != name
			assert.Equal(t, want, hasOmitempty,
				"field %s (column %q) must%s be omitempty: required columns are always present in a dead-letter API response so a caller never distinguishes a missing key from a zero value, while a column that is NULL for most of the row's life is omitted rather than rendered as null on every response",
				field.Name, name, map[bool]string{true: "", false: " not"}[want])
		}
	})
}

// TestMaxEventMessageBytes_SitsBelowTheWriterFloorWithHeadroom pins the one size limit
// the whole pipeline validates against.
//
// Both bounds are asserted.
func TestMaxEventMessageBytes_SitsBelowTheWriterFloorWithHeadroom(t *testing.T) {
	// kafka-go's Writer.BatchBytes default, and near enough a broker's own
	// max.message.bytes default. Written as a literal rather than imported, because
	// model must stay free of first-party and third-party imports.
	const writerFloorBytes = 1048576

	assert.Less(t, MaxEventMessageBytes, writerFloorBytes,
		"the limit must be strictly below the 1 MiB writer floor, or an accepted event cannot be published at all")

	// A dead-letter copy of a maximum-size event adds the failure metadata object,
	// including a broker error string of unbounded length. Requiring a quarter of the
	// floor as headroom is what makes "an event that can be published can also be
	// dead-lettered" true rather than usually true.
	headroom := writerFloorBytes - MaxEventMessageBytes
	assert.GreaterOrEqual(t, headroom, writerFloorBytes/5,
		"at least 20%% of the floor must be left free for the envelope, the failure metadata a dead-letter copy appends, and Kafka's key, header and framing overhead")

	// And it must be large enough to be useful: a limit below the largest legitimate
	// payload would reject real events.
	assert.Greater(t, MaxEventMessageBytes, 256*1024,
		"the limit must comfortably exceed a realistic ledger or identity payload, or valid events would be refused")
}

// TestDeriveCredentialReference_IsStableNonReversibleAndWellFormed covers the derived
// credential reference.
func TestDeriveCredentialReference_IsStableNonReversibleAndWellFormed(t *testing.T) {
	const (
		principal = "blnk-subscriber-acme"
		secret    = "aVeryLongGeneratedSecretValue0123456789"
	)

	reference, err := DeriveCredentialReference(principal, secret)
	require.NoError(t, err)

	t.Run("the reference is well-formed and self-describing", func(t *testing.T) {
		assert.NoError(t, ValidateCredentialReference(reference),
			"a freshly derived reference must validate; if it does not, nothing that persists one can trust the validator")
		assert.True(t, strings.HasPrefix(reference, "scram-sha-512-ref-v1$"),
			"the scheme must be part of the stored value so a reference is recognisable on sight and a later second scheme can coexist")
		assert.Len(t, reference, len("scram-sha-512-ref-v1$")+64,
			"the digest must be a full hex-encoded SHA-256")
	})

	t.Run("the secret is not recoverable from the reference", func(t *testing.T) {
		assert.NotContains(t, reference, secret,
			"the reference must not contain the secret in any form — that is the entire property that makes it safe to persist")
		assert.NotContains(t, reference, principal,
			"the principal is the HMAC key and must not be echoed either")
	})

	t.Run("the derivation is stable and pair-specific", func(t *testing.T) {
		again, err := DeriveCredentialReference(principal, secret)
		require.NoError(t, err)
		assert.Equal(t, reference, again,
			"the same pair must derive the same reference, or a reissue could never be distinguished from the credential it replaced")

		otherSecret, err := DeriveCredentialReference(principal, secret+"x")
		require.NoError(t, err)
		assert.NotEqual(t, reference, otherSecret, "a different secret must derive a different reference")

		// The principal is mixed in as the HMAC key precisely so that two subscribers issued
		// the same generated secret still derive different references. Without it, an
		// equality comparison on the reference would conflate them.
		otherPrincipal, err := DeriveCredentialReference(principal+"-2", secret)
		require.NoError(t, err)
		assert.NotEqual(t, reference, otherPrincipal,
			"the same secret under a different principal must derive a different reference")
	})

	t.Run("an incomplete pair is refused", func(t *testing.T) {
		_, err := DeriveCredentialReference("", secret)
		assert.Error(t, err, "a blank principal would let several rows share one reference")

		_, err = DeriveCredentialReference("   ", secret)
		assert.Error(t, err, "a whitespace-only principal is a blank principal")

		_, err = DeriveCredentialReference(principal, "")
		assert.Error(t, err, "a blank secret would derive a reference for a credential that was never issued")
	})

	t.Run("errors never carry the secret", func(t *testing.T) {
		_, err := DeriveCredentialReference("", secret)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secret,
			"no branch of the derivation may echo the secret, not even to complain about the other argument")
	})
}

// TestValidateCredentialReference_RefusesAnythingNotDerived is the test that makes the
// "no secret is stored here" property structural.
func TestValidateCredentialReference_RefusesAnythingNotDerived(t *testing.T) {
	valid, err := DeriveCredentialReference("principal", "secret-value")
	require.NoError(t, err)

	_, digest, found := strings.Cut(valid, "$")
	require.True(t, found)

	rejected := map[string]string{
		"a plaintext password":         "correct-horse-battery-staple",
		"an empty string":              "",
		"a bare digest with no scheme": digest,
		"the scheme with no digest":    "scram-sha-512-ref-v1$",
		"a truncated digest":           "scram-sha-512-ref-v1$" + digest[:32],
		"an over-long digest":          "scram-sha-512-ref-v1$" + digest + "00",
		"a non-hex digest":             "scram-sha-512-ref-v1$" + strings.Repeat("z", 64),
		"an upper-cased digest":        "scram-sha-512-ref-v1$" + strings.ToUpper(digest),
		"an unknown scheme":            "bcrypt$" + digest,
		"a bcrypt hash":                "$2a$10$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUV",
		"a json object":                `{"reference":"` + valid + `"}`,
	}

	for name, value := range rejected {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, ValidateCredentialReference(value), ErrInvalidCredentialReference,
				"%s must be refused: the reference column exists precisely so that a value which is not a derived reference — above all a plaintext secret — cannot be persisted as one", name)
			assert.Empty(t, CredentialFingerprint(value),
				"a value that fails validation must not be rendered even partially, or a mis-stored secret would leak through the fingerprint")
		})
	}
}

// TestCredentialFingerprint_IsShortAndDerivedFromTheDigest covers the only form of the
// reference that is safe to put in a response or a log line.
func TestCredentialFingerprint_IsShortAndDerivedFromTheDigest(t *testing.T) {
	reference, err := DeriveCredentialReference("principal", "secret-value")
	require.NoError(t, err)

	fingerprint := CredentialFingerprint(reference)

	assert.Len(t, fingerprint, CredentialFingerprintLen,
		"the fingerprint must be exactly CredentialFingerprintLen characters so responses are predictable")
	assert.True(t, strings.HasPrefix(reference, "scram-sha-512-ref-v1$"+fingerprint),
		"the fingerprint must be the leading digest characters, so two issuances can be told apart by comparing it")
	assert.NotContains(t, fingerprint, "$",
		"the fingerprint carries no scheme: it is an opaque short token, not a truncated reference a caller might try to validate")

	// Different issuances must be distinguishable, which is the fingerprint's only
	// job.
	other, err := DeriveCredentialReference("principal", "another-secret-value")
	require.NoError(t, err)
	assert.NotEqual(t, fingerprint, CredentialFingerprint(other),
		"two distinct credentials must produce distinct fingerprints, or the fingerprint answers nothing")
}

// TestEventSubscriber_PartitionKeyPrefixIsNotAnAccessCheck pins where the key scope is
// and is not decided.
//
// event_subscriber_test.go covers the other half: that a co-granted key-scoped topic is
// refused at registration, at update and at issuance.
func TestEventSubscriber_PartitionKeyPrefixIsNotAnAccessCheck(t *testing.T) {
	prefix := "acme_"
	withPrefix := &EventSubscriber{
		AuthorizedTopics:   []string{"blnk.transactions"},
		PartitionKeyPrefix: &prefix,
	}
	withoutPrefix := &EventSubscriber{
		AuthorizedTopics: []string{"blnk.transactions"},
	}

	assert.True(t, withPrefix.HasTopicAccess("blnk.transactions"),
		"a recorded topic grant is what HasTopicAccess reports; the key prefix does not narrow it here, because Kafka cannot enforce a key restriction and exclusivity is decided over the whole registry")
	assert.True(t, withoutPrefix.HasTopicAccess("blnk.transactions"),
		"the absence of a prefix does not widen the grant either — the two subscribers have identical broker-side access")
	assert.False(t, withPrefix.HasTopicAccess("blnk.balances"),
		"a topic outside the recorded grant is refused, which is the boundary that IS enforced")

	empty := &EventSubscriber{}
	assert.False(t, empty.HasTopicAccess("blnk.transactions"),
		"a subscriber with no grant matches nothing: the registry fails closed")
}

// ---------------------------------------------------------------------------------------
// Canonical subscriber identity
// ---------------------------------------------------------------------------------------

// TestCanonicalizeSubscriberIdentifier_RefusesEverythingThatCouldWidenABoundary is the
// primitive both boundary rules rest on.
//
// Each rejected class is here for its own concrete failure:
//
//   - WILDCARDS, because Kafka treats the resource name "*" as matching any resource,
//     so a binding on it is a cluster-wide grant.
//   - WHITESPACE, because a value differing from another only by whitespace is the
//     The derived-identity duplicate: two registry rows, one Kafka principal, one credential silently
//     shared.
//   - The GROUP TERMINATOR ".", because the disjointness of two prefixed group
//     namespaces depends on it being impossible inside an identifier.
//   - The SASL and ACL METACHARACTERS, because they carry meaning in "User:name", in
//     JAAS configuration and in shell arguments.
//   - UPPERCASE, because Kafka principals are case-sensitive: folding would merge two
//     distinct broker identities, and refusing leaves exactly one spelling.
func TestCanonicalizeSubscriberIdentifier_RefusesEverythingThatCouldWidenABoundary(t *testing.T) {
	valid := []string{
		"sub_0f6e2c8a",
		"sub-0f6e2c8a",
		"abc",
		"0f6e2c8a",
		strings.Repeat("a", 128),
	}
	for _, identifier := range valid {
		canonical, err := CanonicalizeSubscriberIdentifier(identifier)
		require.NoError(t, err, "%q is a canonical identifier", identifier)
		assert.Equal(t, identifier, canonical,
			"a canonical identifier is returned unchanged, never rewritten")
	}

	invalid := map[string]string{
		"empty":                   "",
		"too short":               "ab",
		"too long":                strings.Repeat("a", 129),
		"the wildcard":            "*",
		"a wildcard suffix":       "sub_0f6e2c8a*",
		"a question mark":         "sub_0f6e?c8a",
		"leading whitespace":      " sub_0f6e2c8a",
		"trailing whitespace":     "sub_0f6e2c8a ",
		"internal whitespace":     "sub 0f6e2c8a",
		"a tab":                   "sub\t0f6e2c8a",
		"a newline":               "sub_0f6e2c8a\n",
		"a null byte":             "sub_0f6e2c8a\x00",
		"uppercase":               "SUB_0F6E2C8A",
		"mixed case":              "Sub_0f6e2c8a",
		"the group terminator":    "sub.0f6e2c8a",
		"the principal delimiter": "sub:0f6e2c8a",
		"a comma":                 "sub,0f6e2c8a",
		"a semicolon":             "sub;0f6e2c8a",
		"an equals sign":          "sub=0f6e2c8a",
		"a slash":                 "sub/0f6e2c8a",
		"non-ascii":               "sub_0f6é2c8a",
		"a leading hyphen":        "-sub_0f6e2c8a",
		"a leading underscore":    "_sub_0f6e2c8a",
	}
	for name, identifier := range invalid {
		t.Run(name, func(t *testing.T) {
			_, err := CanonicalizeSubscriberIdentifier(identifier)
			require.Error(t, err, "%s must be refused", name)
			assert.ErrorIs(t, err, ErrInvalidSubscriberIdentifier,
				"the refusal must be recognisable without matching message text")
		})
	}
}

// TestCanonicalKafkaPrincipal_IsDerivedAndNamespaced pins the derivation.
//
// The namespace prefix keeps a Blnk-issued principal from colliding with an operator's
// own, and makes it recognisable at the broker.
func TestCanonicalKafkaPrincipal_IsDerivedAndNamespaced(t *testing.T) {
	principal, err := CanonicalKafkaPrincipal("sub_0f6e2c8a")
	require.NoError(t, err)
	assert.Equal(t, "blnk-sub-sub_0f6e2c8a", principal)
	assert.True(t, strings.HasPrefix(principal, SubscriberPrincipalNamespace))

	// An unusable identifier must not produce a partial name that somebody then binds.
	for _, identifier := range []string{"", "*", " sub_0f6e2c8a "} {
		derived, err := CanonicalKafkaPrincipal(identifier)
		require.Error(t, err, "%q must not derive a principal", identifier)
		assert.Empty(t, derived, "a failed derivation must return nothing, not a prefix")
	}
}

// TestCanonicalConsumerGroupNamespace_CannotOverlapAnotherSubscribersNamespace is the
// Allowlist disjointness proof, and it is the reason the terminator exists.
//
// The group binding is PREFIXED, which reserves "<namespace>*".
func TestCanonicalConsumerGroupNamespace_CannotOverlapAnotherSubscribersNamespace(t *testing.T) {
	shortNamespace, err := CanonicalConsumerGroupNamespace("abc")
	require.NoError(t, err)
	longNamespace, err := CanonicalConsumerGroupNamespace("abcdef")
	require.NoError(t, err)

	assert.Equal(t, "blnk-sub-abc.", shortNamespace)
	assert.True(t, strings.HasSuffix(shortNamespace, SubscriberGroupTerminator),
		"the terminator is the disjointness guarantee and must be present")

	assert.False(t, strings.HasPrefix(longNamespace, shortNamespace),
		"one subscriber's namespace must never be a prefix of another's, even when its identifier is")
	assert.False(t, strings.HasPrefix(shortNamespace, longNamespace))

	// And the leaves under each are correspondingly disjoint.
	assert.False(t, strings.HasPrefix(longNamespace+"live", shortNamespace),
		"a leaf of the longer namespace must not fall inside the shorter one")
}

// TestCanonicalConsumerGroupID_IsALeafInsideItsOwnNamespace ties the two derivations
// together.
func TestCanonicalConsumerGroupID_IsALeafInsideItsOwnNamespace(t *testing.T) {
	namespace, err := CanonicalConsumerGroupNamespace("sub_0f6e2c8a")
	require.NoError(t, err)
	group, err := CanonicalConsumerGroupID("sub_0f6e2c8a")
	require.NoError(t, err)

	assert.Equal(t, "blnk-sub-sub_0f6e2c8a.default", group)
	assert.True(t, IsInSubscriberGroupNamespace(group, namespace),
		"the default group must be inside the namespace its own ACL reserves")
}

// TestIsInSubscriberGroupNamespace_RequiresAProperLeaf covers the membership test the
// provisioning path validates a recorded group with.
func TestIsInSubscriberGroupNamespace_RequiresAProperLeaf(t *testing.T) {
	namespace, err := CanonicalConsumerGroupNamespace("sub_0f6e2c8a")
	require.NoError(t, err)

	for _, group := range []string{namespace + "default", namespace + "replay", namespace + "a"} {
		assert.True(t, IsInSubscriberGroupNamespace(group, namespace),
			"%q is a leaf of its own namespace", group)
	}

	for name, group := range map[string]string{
		"the bare namespace":        namespace,
		"a truncated namespace":     strings.TrimSuffix(namespace, SubscriberGroupTerminator),
		"another subscriber's leaf": "blnk-sub-sub_ffffffff.live",
		"the shared prefix":         SubscriberPrincipalNamespace,
		"the wildcard":              "*",
		"empty":                     "",
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, IsInSubscriberGroupNamespace(group, namespace),
				"%s must not count as inside the namespace", name)
		})
	}

	assert.False(t, IsInSubscriberGroupNamespace(namespace+"default", ""),
		"an empty namespace must match nothing rather than everything")
}

// ---------------------------------------------------------------------------
// Webhook destination policy
// ---------------------------------------------------------------------------

// TestInternalIPReason_RefusesEveryRangeThatCannotBeReachedPublicly is the deny-list,
// stated as a table.
//
// It is a table because the deny-list's whole failure mode is a range quietly falling
// off it.
func TestInternalIPReason_RefusesEveryRangeThatCannotBeReachedPublicly(t *testing.T) {
	cases := []struct {
		address string
		expect  string
		why     string
	}{
		{"127.0.0.1", "loopback", "the canonical loopback address"},
		{"127.9.9.9", "loopback", "the whole of 127/8 is loopback, not just .0.1"},
		{"::1", "loopback", "the IPv6 loopback"},
		{"::ffff:127.0.0.1", "loopback", "the IPv4-mapped spelling of loopback must not read as unfamiliar text"},
		{"169.254.169.254", "metadata", "the AWS/GCP/Azure instance metadata endpoint"},
		{"169.254.0.1", "metadata", "all of link-local, not only the metadata address itself"},
		{"fe80::1", "metadata", "IPv6 link-local"},
		{"10.0.0.5", "private", "RFC1918 10/8, where Blnk's own Postgres lives"},
		{"172.16.0.1", "private", "RFC1918 172.16/12"},
		{"192.168.1.1", "private", "RFC1918 192.168/16"},
		{"fd00::1", "private", "IPv6 unique-local"},
		{"0.0.0.0", "unspecified", "the unspecified address routes to a local listener"},
		{"::", "unspecified", "the IPv6 unspecified address"},
		{"224.0.0.1", "multicast", "IPv4 multicast, which is ALSO link-local — the reason must name multicast, not the metadata endpoint"},
		{"ff02::1", "multicast", "IPv6 link-local multicast, same overlap"},
		{"239.1.2.3", "multicast", "IPv4 administratively-scoped multicast, which is not link-local"},
		{"100.64.0.1", "carrier-grade NAT", "RFC 6598, where the GKE metadata proxy lives — IsPrivate does not classify it"},
		{"100.127.255.254", "carrier-grade NAT", "the far end of the /10, to prove the mask is right"},
		{"192.0.0.1", "protocol-assignment", "RFC 6890 IETF protocol assignments are not publicly routable"},
		{"198.18.0.1", "benchmarking", "RFC 2544 benchmarking range"},
		{"198.19.255.255", "benchmarking", "the far end of the /15"},
		{"64:ff9b::7f00:1", "NAT64", "127.0.0.1 smuggled through the NAT64 well-known prefix"},
		{"64:ff9b::a00:5", "NAT64", "10.0.0.5 smuggled through the same prefix"},
	}

	for _, tc := range cases {
		t.Run(tc.address, func(t *testing.T) {
			parsed := net.ParseIP(tc.address)
			require.NotNil(t, parsed, "the fixture itself must be a parseable address")

			reason := InternalIPReason(parsed)
			require.NotEmpty(t, reason, "%s must be refused: %s", tc.address, tc.why)
			assert.Contains(t, reason, tc.expect,
				"the reason must name the rule that refused it, so an operator reading the log "+
					"learns what they pointed Blnk at rather than that a policy exists")
		})
	}
}

// TestInternalIPReason_PermitsOrdinaryPublicAddresses is the other half: a deny-list that
// refuses everything is not a deny-list, it is an outage.
func TestInternalIPReason_PermitsOrdinaryPublicAddresses(t *testing.T) {
	for _, address := range []string{
		"93.184.216.34",        // example.com
		"1.1.1.1",              // a well-known public resolver
		"8.8.8.8",              // another
		"2606:4700:4700::1111", // public IPv6
		"64:ff9b::5d5c:d822",   // NAT64-wrapped 93.92.216.34 — public, so the wrapper is not itself a reason
	} {
		assert.Empty(t, InternalIPReason(net.ParseIP(address)),
			"%s is publicly routable and must be deliverable", address)
	}
}

// TestInternalIPReason_RefusesAnAddressItCouldNotUnderstand pins the fail-closed
// direction.
//
// A nil address reaches this function when a caller could not parse what it was about
// to dial.
func TestInternalIPReason_RefusesAnAddressItCouldNotUnderstand(t *testing.T) {
	assert.NotEmpty(t, InternalIPReason(nil),
		"an unparseable address must be refused, not permitted by omission")
}

// TestOperatorOwnableInternalIP_DrawsTheLineAtWhatAnOperatorCanOwn pins the TIERING,
// which is the part of this policy a reader is most likely to get wrong.
//
// The assertion an operator makes is "this network is mine".
func TestOperatorOwnableInternalIP_DrawsTheLineAtWhatAnOperatorCanOwn(t *testing.T) {
	t.Run("ranges an operator can plausibly own", func(t *testing.T) {
		for _, address := range []string{"127.0.0.1", "::1", "10.0.0.5", "172.20.1.1", "192.168.0.9", "fd00::1"} {
			assert.True(t, OperatorOwnableInternalIP(net.ParseIP(address)),
				"%s is a network an operator can legitimately run a webhook receiver on", address)
		}
	})

	t.Run("ranges no assertion may open", func(t *testing.T) {
		for _, address := range []string{
			"169.254.169.254", // the metadata endpoint
			"fe80::1",         // IPv6 link-local
			"0.0.0.0",         // unspecified
			"224.0.0.1",       // multicast
			"100.64.0.1",      // carrier-grade NAT
			"198.18.0.1",      // benchmarking
			"192.0.0.1",       // protocol assignments
			"64:ff9b::7f00:1", // loopback smuggled through NAT64
			"64:ff9b::a00:5",  // RFC1918 smuggled through NAT64
		} {
			assert.False(t, OperatorOwnableInternalIP(net.ParseIP(address)),
				"%s must stay refused even when the operator asserts ownership of their network", address)
		}
	})

	t.Run("nil", func(t *testing.T) {
		assert.False(t, OperatorOwnableInternalIP(nil),
			"an address that could not be parsed is not one anybody can claim to own")
	})
}

// TestInternalDestinationReason_JudgesNamesOnShapeAndLiteralsOnAddress pins the two modes.
func TestInternalDestinationReason_JudgesNamesOnShapeAndLiteralsOnAddress(t *testing.T) {
	t.Run("names that can only resolve inside the network", func(t *testing.T) {
		assert.Contains(t, InternalDestinationReason("localhost"), "loopback")
		assert.Contains(t, InternalDestinationReason("app.localhost"), "loopback")
		assert.Contains(t, InternalDestinationReason("printer.local"), "internal-only")
		assert.Contains(t, InternalDestinationReason("metadata.google.internal"), "internal-only")
		assert.Contains(t, InternalDestinationReason("postgres"), "unqualified")
		assert.Contains(t, InternalDestinationReason("LOCALHOST"), "loopback",
			"the name test is case-insensitive, because DNS is")
	})

	t.Run("literals are decided by the address policy", func(t *testing.T) {
		assert.Contains(t, InternalDestinationReason("169.254.169.254"), "metadata")
		assert.Contains(t, InternalDestinationReason("64:ff9b::7f00:1"), "NAT64",
			"a literal is judged by InternalIPReason, so every range it covers is covered here too")
	})

	t.Run("ordinary public names and addresses", func(t *testing.T) {
		assert.Empty(t, InternalDestinationReason("hooks.example.com"))
		assert.Empty(t, InternalDestinationReason("93.184.216.34"))
	})
}

// TestEventCategory_UnknownEventFallsBackToTheSystemCategory pins the catch-all arm,
// and pins WHERE it points.
//
// Two properties are asserted:
//
//  1. Coverage. An event type nobody mapped must still resolve to a real category, so a
//     producer added later that forgets to extend the table gets its events published
//     and observable rather than rejected or dropped.
//  2. A named destination. The fallback must land on the system category specifically,
//     not on a domain topic whose subscribers filter it expecting ledger data, and not
//     on an invented fifth topic the agreed inventory does not contain.
func TestEventCategory_UnknownEventFallsBackToTheSystemCategory(t *testing.T) {
	assert.Equal(t, EventCategorySystem, EventCategory("totally.unknown.event"),
		"an unrecognised event type must fall back to the system category — dropping it or returning an empty token would breach the zero-exceptions coverage guarantee for event types added later")

	assert.Equal(t, EventCategorySystem, EventCategory(""),
		"the empty event type must also resolve to a real category: the resolver is total, so no input can ever produce an empty category token that would then compose a malformed topic name")

	assert.False(t, IsCataloguedEventType("totally.unknown.event"),
		"an unrecognised event type must be reported as uncatalogued, which is what makes the fallback observable rather than silent")
	assert.False(t, IsCataloguedEventType(""),
		"a blank event type is uncatalogued too")
	assert.True(t, IsCataloguedEventType("transaction.applied"),
		"a named member of the catalogue must be reported as catalogued")
	assert.True(t, IsCataloguedEventType("bulk_transaction.applied"),
		"a runtime-composed bulk transaction name must be reported as catalogued, because it is matched by prefix rather than enumerated")
}
