// Copyright 2024 Blnk Finance Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventPredicatesInstant is a FIXED timestamp for the presence-of-a-timestamp predicates.
//
// Fixed rather than time.Now, because every one of them decides on whether the pointer is set and
// not on what it points at — a moving value would suggest otherwise to the next reader.
func eventPredicatesInstant() time.Time {
	return time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
}

// This file unit-tests the PURE DECISION FUNCTIONS of the event contract, in the package that
// owns them.
//
// # Why it exists
//
// Every predicate here was exercised only from the root blnk package — through a service, a
// relay or a live broker. That is the right place to prove the behaviour those layers deliver,
// and it is the wrong place to be the ONLY proof of a predicate, for two reasons.
//
// The first is diagnostic. When IsSubscriberGrantableTopicName stops refusing "*", the failure
// surfaces as an integration test somewhere else reporting that a credential was issued, and the
// distance between that symptom and its cause is the whole subscriber service.
//
// The second is that the repository gates this package with mutation testing, and gremlins scores
// only mutants a test in the package REACHES. A predicate no test here calls contributes nothing
// to the score either way — its mutants are reported "not covered" and excluded from the efficacy
// figure — so the gate that is supposed to protect the model was silently not looking at these
// functions at all.
//
// Several of these are security decisions: which topics a subscriber may be granted, whether a
// recorded isolation boundary is enforceable, whether a name is a topic this deployment owns.
// Each is tested with its boundary cases beside the ordinary ones, because a predicate is only
// as good as its edges.

// TestEventOutboxStatusVocabulary_IsClosedAndAgreesWithItself covers the four status helpers
// together, because their value is in AGREEING: a status the CHECK constraint permits that
// IsKnownEventOutboxStatus rejects, or a terminal status missing from the terminal list, splits
// the relay's state machine from the schema's.
func TestEventOutboxStatusVocabulary_IsClosedAndAgreesWithItself(t *testing.T) {
	all := EventOutboxStatuses()

	require.ElementsMatch(t, []string{
		EventOutboxStatusPending,
		EventOutboxStatusProcessing,
		EventOutboxStatusWebhookPending,
		EventOutboxStatusDispatched,
		EventOutboxStatusFailed,
		EventOutboxStatusDeadLettered,
		EventOutboxStatusReplaying,
	}, all,
		"EventOutboxStatuses must be the whole vocabulary the migration's CHECK constraint permits; a "+
			"status missing here is a status the statistics endpoint cannot report")

	// AND IT MUST BE THE WHOLE CLOSED SET, checked by size against the predicate rather than
	// by another hand-written list. The enumerated slice and the closed set behind
	// IsKnownEventOutboxStatus are two spellings of one vocabulary, and they drifted: the
	// slice omitted webhook_pending while the predicate accepted it, so a state the relay
	// really writes was absent from everything driven off the slice — including the
	// statistics response — and the loop below could not notice, because it only walks the
	// slice. Comparing the counts is what makes the omission fail.
	known := 0
	for _, status := range []string{
		EventOutboxStatusPending, EventOutboxStatusProcessing, EventOutboxStatusWebhookPending,
		EventOutboxStatusDispatched, EventOutboxStatusFailed, EventOutboxStatusDeadLettered,
		EventOutboxStatusReplaying, "dlt_pending", "queued", "sent", "archived",
	} {
		if IsKnownEventOutboxStatus(status) {
			known++
		}
	}
	assert.Equal(t, len(all), known,
		"every literal IsKnownEventOutboxStatus admits must be enumerated by EventOutboxStatuses; a "+
			"status the predicate accepts and the slice omits is a state the relay can write and "+
			"nothing driven off the vocabulary can report")

	for _, status := range all {
		assert.Truef(t, IsKnownEventOutboxStatus(status),
			"%q is in the vocabulary but IsKnownEventOutboxStatus rejects it", status)
	}

	for _, unknown := range []string{"", " ", "PENDING", "pending ", "done", "dead-lettered", "replay"} {
		assert.Falsef(t, IsKnownEventOutboxStatus(unknown),
			"%q is not a status; admitting it would let an unwritable value reach the CHECK constraint", unknown)
	}

	terminal := TerminalEventOutboxStatuses()
	require.ElementsMatch(t, []string{EventOutboxStatusDispatched, EventOutboxStatusDeadLettered}, terminal,
		"only a dispatched or a dead-lettered row has finished; anything else still owes work")

	for _, status := range all {
		wantTerminal := status == EventOutboxStatusDispatched || status == EventOutboxStatusDeadLettered
		assert.Equalf(t, wantTerminal, IsTerminalEventOutboxStatus(status),
			"IsTerminalEventOutboxStatus(%q) must agree with the terminal list", status)
	}

	// The two that matter most, spelled out: 'failed' is NOT terminal, because a failed row still
	// owes a dead-letter write and the relay's sweep must keep finding it; 'replaying' is not
	// terminal for the same shape of reason.
	assert.False(t, IsTerminalEventOutboxStatus(EventOutboxStatusFailed),
		"a failed row still owes its dead-letter write; calling it terminal is what strands it")
	assert.False(t, IsTerminalEventOutboxStatus(EventOutboxStatusReplaying),
		"a replaying row is mid-flight, not finished")
}

// TestIsBlnkEventTopic_AdmitsTheNamespaceAndNothingAdjacentToIt is the delimited-prefix property.
//
// An undelimited prefix test is the difference between owning "blnk.*" and owning every name in
// the cluster that happens to start with "blnk" — including another team's "blnkfinance.audit".
func TestIsBlnkEventTopic_AdmitsTheNamespaceAndNothingAdjacentToIt(t *testing.T) {
	const prefix = "blnk"

	admitted := []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
		"blnk.system",
		"blnk.transactions.dlt",
		"blnk.balances.dlt",
	}
	for _, topic := range admitted {
		assert.Truef(t, IsBlnkEventTopic(topic, prefix), "%q is inside the %q namespace", topic, prefix)
	}

	refused := map[string]string{
		"":                    "an empty name is not a topic",
		"blnk":                "the bare prefix is not a topic in the namespace, it IS the namespace",
		"blnkfinance.audit":   "a name that merely begins with the prefix is somebody else's topic",
		"blnk-transactions":   "the delimiter is a dot; a hyphen makes this a different namespace",
		" blnk.transactions":  "Kafka would treat the untrimmed form as a different topic",
		"blnk.transactions ":  "and so would it here",
		"other.transactions":  "a foreign namespace",
		"BLNK.transactions":   "topic names are case-sensitive, so this is a different topic",
		"prefixblnk.balances": "the prefix must start the name",

		// Inside the namespace but naming no category Blnk has. These are the cases that
		// distinguish "the name is in our namespace" from "the name is one of ours": a topic
		// somebody created under our prefix is still not a topic we own, and treating it as ours
		// would let a stray name reach a writer and a dead-letter composition.
		"blnk.unknown":            "the namespace is right but there is no such category",
		"blnk.quarantine":         "the catalogue is four categories; quarantine is not one of them, and admitting a name Blnk does not create would let the ACL pruner treat another team's bindings as its own to delete",
		"blnk.transaction":        "the singular is not the category name",
		"blnk.orders":             "a category this deployment does not have",
		"blnk.transactions.other": "a deeper name is not a category topic",
		"blnk.":                   "the delimiter with no category names nothing",
		"blnk.unknown.dlt":        "the dead-letter sibling of a category we do not have is not ours either",
	}
	for topic, because := range refused {
		assert.Falsef(t, IsBlnkEventTopic(topic, prefix), "%q must be refused: %s", topic, because)
	}

	// An empty prefix falls back to the default rather than admitting everything, which is the
	// only safe reading: a membership test that admits all names is not a membership test.
	assert.True(t, IsBlnkEventTopic(DefaultEventTopicPrefix+".transactions", ""),
		"an empty prefix must fall back to the default namespace")
	assert.False(t, IsBlnkEventTopic("anything.at.all", ""),
		"an empty prefix must not admit a foreign name")
}

// TestSubscriberGrantableTopics_IsTheAllowlistAndExcludesEveryInternalTopic is the allowlist's
// contract from both directions: exactly what it admits, and everything of Blnk's that it does
// not.
//
// The set is the three TENANT categories the requirement names. blnk.system is excluded even
// though it carries ledger.created and system.error — two of the thirteen event types the legacy
// transport delivered — and that exclusion is deliberate rather than an oversight, for the two
// reasons set out on TestSubscriberGrantableEventCategories_ExcludesEveryInternalCategory: the
// legacy audience for system.error was the operator's single configured URL rather than
// subscribers, and this category is the catalogue's catch-all, so making it grantable would give
// an unmapped event type an audience that never asked for it.
func TestSubscriberGrantableTopics_IsTheAllowlistAndExcludesEveryInternalTopic(t *testing.T) {
	const prefix = "blnk"

	grantable := SubscriberGrantableTopics(prefix)

	require.ElementsMatch(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
	}, grantable,
		"a subscriber may be granted exactly the three tenant-facing category topics; every "+
			"over-grant finding in this area reduces to this one enumerated boundary")

	for _, topic := range grantable {
		assert.Truef(t, IsSubscriberGrantableTopicName(topic, prefix),
			"%q is in the grantable list but the membership test rejects it", topic)
	}

	ungrantable := map[string]string{
		"blnk.system":             "the catalogue's catch-all, and system.error's frozen payload carries raw error text describing the deployment",
		"blnk.quarantine":         "not a catalogued category at all; the four-category inventory is closed, so it can be neither owned nor granted",
		"blnk.transactions.dlt":   "a dead-letter topic is Blnk's own; a subscriber builds its own <topic>.dlt",
		"blnk.balances.dlt":       "the same, for every category",
		"blnk.identities.dlt":     "the same",
		"blnk.system.dlt":         "the same",
		"blnk.quarantine.dlt":     "and neither is its dead-letter form",
		"*":                       "Kafka reads \"*\" as every resource, turning a per-topic grant cluster-wide",
		"":                        "an empty name grants nothing and must not be admitted as a member",
		"blnk.transactions ":      "an untrimmed name is a different topic to Kafka",
		" blnk.transactions":      "and so is this one",
		"other.transactions":      "a foreign namespace",
		"blnk":                    "the namespace itself is not a topic",
		"blnk.transactions.other": "a deeper name inside the namespace is still not a category topic",
	}
	for topic, because := range ungrantable {
		assert.Falsef(t, IsSubscriberGrantableTopicName(topic, prefix),
			"%q must NOT be grantable: %s", topic, because)
	}

	// The grantable set is a strict subset of the namespace: everything in it is Blnk's, and not
	// everything of Blnk's is in it. Both halves are asserted so a future widening cannot pass
	// unnoticed.
	for _, topic := range grantable {
		assert.Truef(t, IsBlnkEventTopic(topic, prefix), "%q must be inside the owned namespace", topic)
	}

	assert.Less(t, len(grantable), len(AllEventCategories()),
		"the grantable list must be a strict subset of the categories; equal length means an internal "+
			"category became grantable")

	// An empty prefix composes against the default rather than producing bare category names.
	for _, topic := range SubscriberGrantableTopics("") {
		assert.True(t, strings.HasPrefix(topic, DefaultEventTopicPrefix+"."),
			"an empty prefix must compose against the default namespace, got %q", topic)
	}
}

// TestValidateSubscriberTopics_RefusesEveryShapeThatCouldReachAnACL covers the grant validator
// and, through it, the topic-name legality check.
//
// The list it validates is composed into a Postgres array literal and into Kafka ACL resource
// names, so a character it lets through is a character in both. The count bound matters for the
// same reason from the other side: a grant of unbounded length is an unbounded number of ACL
// bindings created inside one issuance's five-second budget.
func TestValidateSubscriberTopics_RefusesEveryShapeThatCouldReachAnACL(t *testing.T) {
	t.Run("an ordinary grant is accepted", func(t *testing.T) {
		require.NoError(t, ValidateSubscriberTopics([]string{"blnk.transactions", "blnk.balances"}))
	})

	t.Run("an empty grant is accepted here and refused where it means something", func(t *testing.T) {
		// Emptiness is not this function's decision: a subscriber may legitimately be registered
		// before its grant is known, and the issuance path is what refuses to mint a credential
		// for an empty grant.
		require.NoError(t, ValidateSubscriberTopics(nil))
		require.NoError(t, ValidateSubscriberTopics([]string{}))
	})

	t.Run("the count bound is enforced at its edge", func(t *testing.T) {
		atLimit := make([]string, 0, MaxSubscriberTopics)
		for index := 0; index < MaxSubscriberTopics; index++ {
			atLimit = append(atLimit, "blnk.topic"+string(rune('a'+index)))
		}
		require.NoError(t, ValidateSubscriberTopics(atLimit), "exactly the limit must be accepted")

		overLimit := append(append([]string(nil), atLimit...), "blnk.one.too.many")
		err := ValidateSubscriberTopics(overLimit)
		require.Error(t, err, "one past the limit must be refused")
		assert.Contains(t, err.Error(), "at most")
	})

	t.Run("the name-length bound is enforced at its edge", func(t *testing.T) {
		require.NoError(t, ValidateSubscriberTopics([]string{strings.Repeat("a", MaxTopicNameLength)}),
			"Kafka's own maximum topic-name length must be accepted")

		err := ValidateSubscriberTopics([]string{strings.Repeat("a", MaxTopicNameLength+1)})
		require.Error(t, err, "one character past Kafka's maximum must be refused")
		assert.Contains(t, err.Error(), "at most")
	})

	t.Run("blank, duplicate and illegal names are refused", func(t *testing.T) {
		cases := map[string][]string{
			"blank":            {"blnk.transactions", "   "},
			"empty":            {""},
			"duplicate":        {"blnk.transactions", "blnk.transactions"},
			"comma":            {"blnk.transactions,blnk.balances"},
			"quote":            {`blnk."transactions`},
			"brace":            {"blnk.{transactions}"},
			"space inside":     {"blnk. transactions"},
			"semicolon":        {"blnk.transactions;drop"},
			"wildcard":         {"*"},
			"backslash":        {`blnk\transactions`},
			"newline":          {"blnk.transactions\nblnk.balances"},
			"null byte":        {"blnk.transactions\x00"},
			"unicode homoglot": {"blnk.trансactions"},
		}

		for name, topics := range cases {
			t.Run(name, func(t *testing.T) {
				require.Error(t, ValidateSubscriberTopics(topics),
					"a %s must be refused before it is composed into an array literal and an ACL rule", name)
			})
		}
	})
}

// TestEventSubscriberPredicates_ReadTheRowRatherThanAssuming covers the four state predicates on
// the registry row, including C-08's and C-10's.
func TestEventSubscriberPredicates_ReadTheRowRatherThanAssuming(t *testing.T) {
	t.Run("a nil subscriber answers false to everything", func(t *testing.T) {
		var absent *EventSubscriber

		// Every one of these is consulted on a row that may not have been found, so a nil
		// receiver must be a definite "no" rather than a panic in a ledger process.
		assert.False(t, absent.DeclaresUnenforceableIsolation())
		assert.False(t, absent.IsProvisioned())
		assert.False(t, absent.IsMigrated())
	})

	t.Run("DeclaresUnenforceableIsolation is C-08's refusal", func(t *testing.T) {
		// The predicate decides whether a credential may be minted at all, so its edges are the
		// difference between refusing a boundary Kafka cannot apply and handing one out.
		none := &EventSubscriber{}
		assert.False(t, none.DeclaresUnenforceableIsolation(),
			"no recorded prefix is nothing to be unable to enforce")

		blank := ""
		assert.False(t, (&EventSubscriber{PartitionKeyPrefix: &blank}).DeclaresUnenforceableIsolation(),
			"an empty recorded prefix declares no boundary")

		whitespace := "   \t "
		assert.False(t, (&EventSubscriber{PartitionKeyPrefix: &whitespace}).DeclaresUnenforceableIsolation(),
			"whitespace declares no boundary either; treating it as one would refuse issuance over nothing")

		declared := "ldg_customer-"
		assert.True(t, (&EventSubscriber{PartitionKeyPrefix: &declared}).DeclaresUnenforceableIsolation(),
			"a real recorded prefix IS a declared record-level boundary, and no Kafka ACL admits some "+
				"records of a partition and refuses others by key")

		padded := "  ldg_customer-  "
		assert.True(t, (&EventSubscriber{PartitionKeyPrefix: &padded}).DeclaresUnenforceableIsolation(),
			"a padded prefix still declares one; trimming to nothing is the only way it does not")
	})

	t.Run("IsProvisioned follows the credential record, not the principal", func(t *testing.T) {
		assert.False(t, (&EventSubscriber{KafkaPrincipal: "blnk-sub-x"}).IsProvisioned(),
			"a derived principal is not a credential; the row is provisioned only once a credential "+
				"reference has been recorded against it")

		reference := "scram-sha-512:abc"
		issued := eventPredicatesInstant()
		assert.True(t, (&EventSubscriber{CredentialReference: &reference, CredentialIssuedAt: &issued}).IsProvisioned(),
			"a recorded credential reference is what makes the row provisioned")
	})

	t.Run("IsMigrated follows the migration timestamp", func(t *testing.T) {
		assert.False(t, (&EventSubscriber{}).IsMigrated(),
			"a row with no migration timestamp is still counted as awaiting migration")

		migrated := eventPredicatesInstant()
		assert.True(t, (&EventSubscriber{MigratedAt: &migrated}).IsMigrated())
	})
}

// TestEventTypeForTransactionStatus_CoversEverySevenNamesIncludingTheCommitFallThrough is
// requirement R-1's transaction half.
//
// The COMMIT case is asserted as transaction.unknown ON PURPOSE. That is the pre-existing
// behaviour of the legacy mapping, the dual-delivery comparison asserts both transports carry
// identical bytes, and "correcting" it inside this change would make that comparison fail for a
// reason unrelated to the transport. It is recorded in docs/event-streaming.md so it can be
// changed deliberately, and this assertion is what makes such a change a visible decision rather
// than an accident.
func TestEventTypeForTransactionStatus_CoversEverySevenNamesIncludingTheCommitFallThrough(t *testing.T) {
	cases := map[string]string{
		"QUEUED":    EventTypeTransactionQueued,
		"APPLIED":   EventTypeTransactionApplied,
		"SCHEDULED": EventTypeTransactionScheduled,
		"INFLIGHT":  EventTypeTransactionInflight,
		"VOID":      EventTypeTransactionVoid,
		"REJECTED":  EventTypeTransactionRejected,
		"COMMIT":    EventTypeTransactionUnknown,
		"":          EventTypeTransactionUnknown,
		"anything":  EventTypeTransactionUnknown,
	}

	for status, want := range cases {
		assert.Equalf(t, want, EventTypeForTransactionStatus(status),
			"status %q must map to %q", status, want)
	}

	assert.Equal(t, EventTypeTransactionUnknown, EventTypeForTransactionStatus("COMMIT"),
		"COMMIT has no case of its own and falls through to transaction.unknown. This is the LEGACY "+
			"behaviour, preserved deliberately so the dual-delivery byte comparison holds; changing it is "+
			"a separate, intentional change")
}

// TestEventIdentityAndDerivation_MakeACaptureIdempotent covers the identity helpers together,
// because they are one mechanism: the identity of the mutation, folded into a deterministic event
// id, is what stops a retried capture from admitting a second row for one business event.
func TestEventIdentityAndDerivation_MakeACaptureIdempotent(t *testing.T) {
	t.Run("a derived id is deterministic and depends on all three inputs", func(t *testing.T) {
		base := DeriveEventID("txn_1", EventTypeTransactionApplied, SchemaVersionV1)
		require.NotEmpty(t, base)
		assert.Equal(t, base, DeriveEventID("txn_1", EventTypeTransactionApplied, SchemaVersionV1),
			"the same mutation, event type and schema version must derive the same id, or a retried "+
				"capture inserts a second row for one event")

		assert.NotEqual(t, base, DeriveEventID("txn_2", EventTypeTransactionApplied, SchemaVersionV1),
			"a different mutation must derive a different id")
		assert.NotEqual(t, base, DeriveEventID("txn_1", EventTypeTransactionVoid, SchemaVersionV1),
			"a different event type must derive a different id; one transaction emits several events")
		assert.NotEqual(t, base, DeriveEventID("txn_1", EventTypeTransactionApplied, SchemaVersionV1+1),
			"a different schema version must derive a different id, or a re-emission under a new "+
				"envelope collides with the old one")

		assert.True(t, IsCanonicalUUID(base),
			"a derived id must be a canonical UUID: the column is compared against ids from NewEventID "+
				"and read by subscribers as their idempotency key")
	})

	t.Run("a fresh id is canonical and unique", func(t *testing.T) {
		first, second := NewEventID(), NewEventID()

		assert.True(t, IsCanonicalUUID(first))
		assert.True(t, IsCanonicalUUID(second))
		assert.NotEqual(t, first, second, "two fresh ids must differ")
	})

	t.Run("the identity is read from the payload the producer passed", func(t *testing.T) {
		transaction := &Transaction{TransactionID: "txn_identity"}
		identity, ok := EventIdentityFor(EventTypeTransactionApplied, transaction)
		require.True(t, ok, "a transaction payload carries an identity")
		assert.Equal(t, "txn_identity", identity)

		byValue, ok := EventIdentityFor(EventTypeTransactionApplied, *transaction)
		require.True(t, ok, "a payload passed BY VALUE must resolve the same way; one producer does")
		assert.Equal(t, "txn_identity", byValue)

		ledger := &Ledger{LedgerID: "ldg_identity"}
		identity, ok = EventIdentityFor("ledger.created", ledger)
		require.True(t, ok, "a ledger payload carries an identity")
		assert.Equal(t, "ldg_identity", identity)
	})

	t.Run("no identity is invented when the payload has none", func(t *testing.T) {
		_, ok := EventIdentityFor(EventTypeSystemError, map[string]interface{}{"error": "boom"})
		assert.False(t, ok,
			"an untyped payload has no mutation identity, and inventing one would derive an id that "+
				"collides with a different event")

		var absent *Transaction
		_, ok = EventIdentityFor(EventTypeTransactionApplied, absent)
		assert.False(t, ok, "a nil typed payload must answer 'no identity' rather than panicking")

		_, ok = EventIdentityFor(EventTypeTransactionApplied, &Transaction{})
		assert.False(t, ok, "a payload whose identity field is empty carries no identity")
	})

	t.Run("only the genuinely repeatable event types are repeatable", func(t *testing.T) {
		// A repeatable type emits more than once for one aggregate — a monitor firing again, an
		// error recurring — so its id cannot be derived from the aggregate alone.
		assert.True(t, EventTypeIsRepeatable(EventTypeBalanceMonitor),
			"a balance monitor fires every time its condition is met")
		assert.True(t, EventTypeIsRepeatable(EventTypeSystemError),
			"the same error recurs, and each occurrence is its own event")

		for _, once := range []string{
			EventTypeTransactionApplied,
			EventTypeTransactionQueued,
			"ledger.created",
			"identity.created",
			"balance.created",
			"",
			"not.an.event",
		} {
			assert.Falsef(t, EventTypeIsRepeatable(once),
				"%q must NOT be repeatable: a derived id is what makes its capture idempotent", once)
		}

		assert.True(t, EventTypeIsRepeatable("  "+EventTypeBalanceMonitor+"  "),
			"the decision is made on the trimmed name, so a padded value cannot change it")
	})
}

// TestGeneratedIdentifiers_AreCanonicalByConstruction covers the two generators.
//
// Both feed values that are copied verbatim into a Kafka principal and a consumer group id, so an
// identifier that cannot be canonicalised cannot be provisioned at all — and the generator is the
// one place that guarantee can be made rather than checked.
func TestGeneratedIdentifiers_AreCanonicalByConstruction(t *testing.T) {
	t.Run("a generated subscriber id is canonical and prefixed", func(t *testing.T) {
		id := GenerateSubscriberID()

		require.True(t, strings.HasPrefix(id, SubscriberIDPrefix+"_"),
			"a generated subscriber id must carry the module prefix, got %q", id)

		canonical, err := CanonicalizeSubscriberIdentifier(id)
		require.NoErrorf(t, err,
			"a generated id must satisfy the canonical form, or the principal and consumer group derived "+
				"from it cannot be provisioned: %q", id)
		assert.Equal(t, id, canonical, "a generated id must already BE canonical, not merely canonicalisable")

		assert.NotEqual(t, id, GenerateSubscriberID(), "two generated ids must differ")
		assert.Equal(t, strings.ToLower(id), id,
			"the id is copied into a Kafka principal, so it must be lowercase already")
	})

	t.Run("a generated consumer group sits inside a terminated namespace", func(t *testing.T) {
		group := GenerateConsumerGroupID()

		require.NotEmpty(t, group)
		assert.True(t, strings.HasPrefix(group, ConsumerGroupIDPrefix),
			"a generated group must sit under the consumer-group namespace, got %q", group)
		assert.Contains(t, group, SubscriberGroupTerminator,
			"the namespace must be TERMINATED: an unterminated prefixed grant also reaches every group "+
				"whose name merely extends the subscriber's, got %q", group)
		assert.True(t, strings.HasSuffix(group, SubscriberGroupTerminator+SubscriberDefaultGroupLeaf),
			"a generated group is the subscriber's default leaf inside its own namespace, got %q", group)

		assert.NotEqual(t, group, GenerateConsumerGroupID(), "two generated groups must differ")
	})
}

// TestIsCanonicalUUID_AcceptsOnlyTheCanonicalForm guards the form every event id is compared in.
//
// Two ids for one event that differ only in case or in braces are two rows, and the unique index
// on event_id would not stop either.
func TestIsCanonicalUUID_AcceptsOnlyTheCanonicalForm(t *testing.T) {
	assert.True(t, IsCanonicalUUID("3f2504e0-4f89-41d3-9a0c-0305e82c3301"))
	assert.True(t, IsCanonicalUUID(NewEventID()), "a freshly minted id must be canonical")

	refused := map[string]string{
		"":                                              "empty",
		"3f2504e0-4f89-41d3-9a0c-0305e82c330":           "one character short",
		"3f2504e0-4f89-41d3-9a0c-0305e82c33011":         "one character long",
		"3F2504E0-4F89-41D3-9A0C-0305E82C3301":          "uppercase is a second spelling of one id",
		"{3f2504e0-4f89-41d3-9a0c-0305e82c3301}":        "braced",
		"3f2504e04f8941d39a0c0305e82c3301":              "unhyphenated",
		"urn:uuid:3f2504e0-4f89-41d3-9a0c-0305e82c3301": "urn form",
		"3f2504e0-4f89-41d3-9a0c-0305e82c330g":          "a non-hex character",
		" 3f2504e0-4f89-41d3-9a0c-0305e82c3301":         "leading whitespace",
		"3f2504e0-4f89-41d3-9a0c-0305e82c3301 ":         "trailing whitespace",
	}
	for value, because := range refused {
		assert.Falsef(t, IsCanonicalUUID(value), "%s (%q) is not the canonical form", because, value)
	}
}
