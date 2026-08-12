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

// This file unit-tests the PURE DECISION FUNCTIONS of the event contract, in the
// package that owns them.
//
// Every predicate here was exercised only from the root blnk package — through a
// service, a relay or a live broker.
//
// The first is diagnostic.
//
// The second is that the repository gates this package with mutation testing, and
// gremlins scores only mutants a test in the package REACHES.
//
// Several of these are security decisions: which topics a subscriber may be granted,
// whether a recorded isolation boundary is enforceable, whether a name is a topic this
// deployment owns.

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

	// AND IT MUST BE THE WHOLE CLOSED SET, checked by size against the predicate rather
	// than by another hand-written list. The enumerated slice and the closed set behind
	// IsKnownEventOutboxStatus are two spellings of one vocabulary, and they drifted: the
	// slice omitted webhook_pending while the predicate accepted it, so a state the relay
	// really writes was absent from everything driven off the slice — including the
	// statistics response — and the loop below could not notice, because it only walks the
	// slice.
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

	// "dlt_pending" is in this list deliberately. It was a status once, and it was
	// impossible to reach: nothing in the relay ever wrote it, while the migration's CHECK
	// constraint, two partial indexes and the dead-letter listing all carried the
	// vocabulary for it.
	for _, unknown := range []string{
		"", " ", "PENDING", "pending ", "done", "dead-lettered", "replay", "dlt_pending",
	} {
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

	// The two that matter most, spelled out: 'failed' is NOT terminal, because a failed
	// row still owes a dead-letter write and the relay's sweep must keep finding it;
	// 'replaying' is not terminal for the same shape of reason.
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
		// somebody created under our prefix is still not a topic we own, and treating it as
		// ours would let a stray name reach a writer and a dead-letter composition.
		"blnk.unknown":            "the namespace is right but there is no such category",
		"blnk.quarantine":         "the catalogue is four categories; quarantine is not one of them, and admitting a name Blnk does not create would let the ACL pruner treat another team's bindings as its own to delete",
		"blnk.ledgers":            "not a category in the catalogue, so not a topic Blnk owns, creates or writes to",
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

// TestValidateSubscriberTopics_RefusesEveryShapeThatCouldReachAnACL covers the grant
// validator and, through it, the topic-name legality check.
//
// The list it validates is composed into a Postgres array literal and into Kafka ACL
// resource names, so a character it lets through is a character in both.
func TestValidateSubscriberTopics_RefusesEveryShapeThatCouldReachAnACL(t *testing.T) {
	t.Run("an ordinary grant is accepted", func(t *testing.T) {
		require.NoError(t, ValidateSubscriberTopics([]string{"blnk.transactions", "blnk.balances"}))
	})

	t.Run("an empty grant is accepted here and refused where it means something", func(t *testing.T) {
		// Emptiness is not this function's decision: a subscriber may legitimately be
		// registered before its grant is known, and the issuance path is what refuses to mint
		// a credential for an empty grant.
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
// the registry row, including the migration-stamp and key-scope predicates.
func TestEventSubscriberPredicates_ReadTheRowRatherThanAssuming(t *testing.T) {
	t.Run("a nil subscriber answers false to everything", func(t *testing.T) {
		var absent *EventSubscriber

		// Every one of these is consulted on a row that may not have been found, so a nil
		// receiver must be a definite "no" rather than a panic in a ledger process.
		assert.False(t, absent.RequiresGatewayDelivery())
		assert.False(t, absent.IsProvisioned())
		assert.False(t, absent.IsMigrated())
	})

	t.Run("RequiresGatewayDelivery states which path this subscriber's records take", func(t *testing.T) {
		// The predicate decides the SHAPE OF THE GRANT and what the credential contract says.
		// Kafka has no key-level ACL resource, so a recorded prefix is a boundary kept
		// OUTSIDE the broker: such a subscriber is provisioned without topic Read, and its
		// records are delivered key-filtered by the component the deployment declared — or,
		// where none is declared, it is refused a credential entirely rather than issued a
		// wider one. Its edges are therefore the difference between withholding record access
		// from a subscriber that asked for a narrowing and granting a whole shared topic to
		// one that did.
		none := &EventSubscriber{}
		assert.False(t, none.RequiresGatewayDelivery(),
			"no recorded prefix means the topic grant is the boundary, so the broker delivers")
		assert.True(t, none.GrantsBrokerRecordAccess(),
			"and record access is granted, which is the complement this pair must never contradict")

		blank := ""
		assert.False(t, (&EventSubscriber{PartitionKeyPrefix: &blank}).RequiresGatewayDelivery(),
			"an empty recorded prefix declares no boundary")

		whitespace := "   \t "
		assert.False(t, (&EventSubscriber{PartitionKeyPrefix: &whitespace}).RequiresGatewayDelivery(),
			"whitespace declares no boundary either; treating it as one would announce an obligation "+
				"over nothing")

		declared := "ldg_customer-"
		scoped := &EventSubscriber{PartitionKeyPrefix: &declared}
		assert.True(t, scoped.RequiresGatewayDelivery(),
			"a real recorded prefix IS a record-level boundary, and no Kafka ACL admits some records "+
				"of a partition and refuses others by key, so Blnk applies it at the gateway")
		assert.False(t, scoped.GrantsBrokerRecordAccess(),
			"which is only a boundary because direct record access is withheld from such a row")

		padded := "  ldg_customer-  "
		assert.True(t, (&EventSubscriber{PartitionKeyPrefix: &padded}).RequiresGatewayDelivery(),
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

// TestEventTypeForTransactionStatus_CoversEverySevenNamesIncludingTheCommitFallThrough
// is the requirement transaction half.
//
// The COMMIT case is asserted as transaction.unknown ON PURPOSE.
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

		// The remaining two POINTER shapes, which are the shapes the producers actually pass:
		// identity.created carries *Identity and balance.created carries *Balance. Both arms
		// went unexercised here, so the mutation gate reported them NOT COVERED — and an id
		// derived from the wrong field, or not derived at all, is a duplicate-suppression key
		// that stops working, which is the one failure the derived id exists to prevent.
		subject := &Identity{IdentityID: "idt_identity"}
		identity, ok = EventIdentityFor("identity.created", subject)
		require.True(t, ok, "an identity payload carries an identity")
		assert.Equal(t, "idt_identity", identity)

		balance := &Balance{BalanceID: "bln_identity"}
		identity, ok = EventIdentityFor("balance.created", balance)
		require.True(t, ok, "a balance payload carries an identity")
		assert.Equal(t, "bln_identity", identity)

		// And by value, because each arm has a value twin that must agree with it —
		// two spellings of one rule are how the two arms drift apart.
		identity, ok = EventIdentityFor("identity.created", *subject)
		require.True(t, ok)
		assert.Equal(t, "idt_identity", identity)

		identity, ok = EventIdentityFor("balance.created", *balance)
		require.True(t, ok)
		assert.Equal(t, "bln_identity", identity)
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
// Both feed values that are copied verbatim into a Kafka principal and a consumer group
// id, so an identifier that cannot be canonicalised cannot be provisioned at all — and
// the generator is the one place that guarantee can be made rather than checked.
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

// TestSubscriberGrantableTopics_IsTheAllowlistAndExcludesEveryInternalTopic is the
// allowlist's contract from both directions: exactly what it admits, and everything of
// Blnk's that it does not.
//
// The set is exactly the THREE TENANT categories the requirement names.
//
// TWO CLASSES OF NAME ARE WITHHELD, and both are operator surfaces read under the
// master key:
//
//   - Every `<topic>.dlt`. A dead-letter record carries the original payload PLUS
//     Blnk's failure metadata — broker error text, attempt windows, internal topic
//     names — and is triaged through the master-key-gated dead-letter API.
//   - `blnk.system`. It carries `system.error`, whose frozen payload renders Blnk's
//     error text verbatim, and it is the catalogue's catch-all, so a grant of it would
//     also stand over every event type nobody has catalogued yet.
//
// `ledger.created` shares that topic, so it depends on that acknowledgement too.
//
// Grantable is not the same as granted: this list is what MAY be granted, and any given
// subscriber holds only the subset recorded on it.
func TestSubscriberGrantableTopics_IsTheAllowlistAndExcludesEveryInternalTopic(t *testing.T) {
	const prefix = "blnk"

	grantable := SubscriberGrantableTopics(prefix)

	require.ElementsMatch(t, []string{
		"blnk.transactions",
		"blnk.balances",
		"blnk.identities",
	}, grantable,
		"a subscriber may be granted exactly the three TENANT category topics by default; every "+
			"over-grant finding in this area reduces to this one enumerated boundary, and blnk.system "+
			"is absent from it because it carries system.error's verbatim body and every uncatalogued "+
			"event")

	for _, topic := range grantable {
		assert.Truef(t, IsSubscriberGrantableTopicName(topic, prefix),
			"%q is in the grantable list but the membership test rejects it", topic)
	}

	ungrantable := map[string]string{
		"blnk.system":             "the internal category is outside the DEFAULT grant set: system.error's frozen body renders Blnk's own error text, and the category is the catalogue's catch-all, so it takes a deployment-level acknowledgement",
		"blnk.quarantine":         "not a catalogued category at all; the five-category inventory is closed, so it can be neither owned nor granted",
		"blnk.transactions.dlt":   "a dead-letter topic is Blnk's own; a subscriber builds its own <topic>.dlt",
		"blnk.balances.dlt":       "the same, for every category",
		"blnk.identities.dlt":     "the same",
		"blnk.system.dlt":         "the same, and the internal category's dead-letter sibling is doubly withheld",
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

	// The grantable set is a strict subset of the namespace: everything in it is Blnk's,
	// and not everything of Blnk's is in it. Both halves are asserted so a future widening
	// cannot pass unnoticed.
	for _, topic := range grantable {
		assert.Truef(t, IsBlnkEventTopic(topic, prefix), "%q must be inside the owned namespace", topic)
	}

	assert.Len(t, grantable, len(AllEventCategories())-1,
		"the grantable list is the catalogue MINUS the one internal category: fewer means a tenant "+
			"category is published and unreachable, more means the internal category — or a name that "+
			"is not a category topic at all — reached the allowlist")

	// An empty prefix composes against the default rather than producing bare category names.
	for _, topic := range SubscriberGrantableTopics("") {
		assert.True(t, strings.HasPrefix(topic, DefaultEventTopicPrefix+"."),
			"an empty prefix must compose against the default namespace, got %q", topic)
	}
}

// TestValidateWebhookURL_IsTheSinglePolicyEveryLayerApplies pins the policy that used
// to exist twice.
//
// The https, host, whitespace and internal-destination rules were implemented
// independently in the repository and in the request DTO, each with its own destination
// classifier and its own wording.
//
// This test is where the rule now lives, so both callers inherit it and neither can
// drift.
func TestValidateWebhookURL_IsTheSinglePolicyEveryLayerApplies(t *testing.T) {
	t.Run("an absent endpoint is acceptable and means clear the record", func(t *testing.T) {
		// The column is nullable precisely so "no endpoint" and "this endpoint" stay
		// distinguishable, and every caller maps nil/empty/value the same three ways.
		for _, raw := range []string{"", "   ", "\t\n"} {
			message, reason := ValidateWebhookURL(raw)
			assert.Emptyf(t, message, "%q means clear the record, not a policy violation", raw)
			assert.Empty(t, reason)
		}
	})

	t.Run("an https URL to an external host is accepted", func(t *testing.T) {
		for _, raw := range []string{
			"https://hooks.example.com/blnk",
			"https://hooks.example.com:8443/blnk?v=1",
			"https://sub.domain.example.co.uk/path/to/hook",
		} {
			message, _ := ValidateWebhookURL(raw)
			assert.Emptyf(t, message, "%q is a legitimate destination", raw)
		}
	})

	t.Run("surrounding whitespace is refused, never trimmed", func(t *testing.T) {
		// The column is stored VERBATIM, so trimming for validation and storing the original
		// would persist a destination that never passed the check.
		for _, raw := range []string{
			" https://hooks.example.com/blnk",
			"https://hooks.example.com/blnk ",
			"\thttps://hooks.example.com/blnk\n",
		} {
			message, reason := ValidateWebhookURL(raw)
			require.NotEmptyf(t, message, "%q must be refused rather than trimmed", raw)
			assert.Contains(t, message, "whitespace")
			assert.Contains(t, reason, "stored verbatim",
				"the reason must say WHY trimming is not an option")
		}
	})

	t.Run("cleartext is refused whatever the destination", func(t *testing.T) {
		for _, raw := range []string{
			"http://hooks.example.com/blnk",
			"ftp://hooks.example.com/",
			"file:///etc/passwd",
			"gopher://hooks.example.com/",
		} {
			message, reason := ValidateWebhookURL(raw)
			require.NotEmptyf(t, message, "%q must be refused", raw)
			assert.Contains(t, message, "https", "the refusal must name the requirement")
			assert.Contains(t, reason, "cleartext")
		}
	})

	t.Run("every spelling of an internal destination is refused", func(t *testing.T) {
		// A webhook URL is third-party input that Blnk itself dials, which makes it a
		// server-side request forgery vector straight at the cloud metadata endpoint and at
		// every service that trusts the network rather than the caller.
		internal := map[string]string{
			"loopback literal":          "https://127.0.0.1/blnk",
			"loopback by name":          "https://localhost/blnk",
			"loopback subdomain":        "https://api.localhost/blnk",
			"IPv6 loopback":             "https://[::1]/blnk",
			"IPv4-mapped IPv6 loopback": "https://[::ffff:127.0.0.1]/blnk",
			"cloud metadata endpoint":   "https://169.254.169.254/latest/meta-data/",
			"private 10/8":              "https://10.0.0.7:9092/blnk",
			"private 172.16/12":         "https://172.16.4.9/blnk",
			"private 192.168/16":        "https://192.168.1.1/blnk",
			"unspecified address":       "https://0.0.0.0/blnk",
			"multicast":                 "https://239.1.2.3/blnk",
			"mDNS hostname":             "https://broker.local/blnk",
			"internal zone hostname":    "https://metadata.google.internal/blnk",
			"unqualified hostname":      "https://postgres/blnk",
		}

		for name, raw := range internal {
			t.Run(name, func(t *testing.T) {
				message, reason := ValidateWebhookURL(raw)
				require.NotEmptyf(t, message, "%q must be refused", raw)
				assert.Contains(t, message, "internal destination")

				// The REASON names the host and says which rule was hit. "Not allowed" sends an
				// operator looking for a policy document; naming the range tells them what they
				// just pointed Blnk at.
				assert.NotEmpty(t, reason)
				assert.Contains(t, reason, "refused")
			})
		}
	})

	t.Run("a URL with no host is refused", func(t *testing.T) {
		message, _ := ValidateWebhookURL("https:///blnk")
		require.NotEmpty(t, message)
		assert.Contains(t, message, "host")
	})

	t.Run("the length is bounded, and the bound is a byte count", func(t *testing.T) {
		// Every other caller-supplied text on blnk.event_subscribers is bounded — the name at
		// 256 by a CHECK, each topic at 249 by Kafka — and this one was not: a
		// 100,020-character https URL was accepted and stored. No security consequence, since
		// the destination is validated and the surface is master-key gated behind a body cap,
		// but an unbounded column only stays harmless while nothing reads it, and this one is
		// a future request sink.
		prefix := "https://hooks.example.com/"
		atLimit := prefix + strings.Repeat("a", MaxWebhookURLLength-len(prefix))
		require.Len(t, atLimit, MaxWebhookURLLength)

		message, reason := ValidateWebhookURL(atLimit)
		assert.Empty(t, message, "the bound is inclusive: exactly the maximum is acceptable")
		assert.Empty(t, reason)

		overLimit := atLimit + "a"
		message, reason = ValidateWebhookURL(overLimit)
		require.NotEmpty(t, message, "one byte over the maximum must be refused")
		assert.Contains(t, message, "too long")

		// BOTH NUMBERS, so a caller can see by how much, and NEITHER the URL nor any part of
		// its path — the length is the caller's own value, the endpoint is a third party's.
		assert.Contains(t, reason, "2049")
		assert.Contains(t, reason, "2048")
		assert.NotContains(t, reason, "hooks.example.com")
	})

	t.Run("an over-long URL is refused without being parsed", func(t *testing.T) {
		// The refusal has to precede url.Parse: parsing a 100 KB string is work spent on a
		// value that was never going to be accepted. It is observable through the ANSWER — a
		// value that is both over-length and unparseable is refused for its length.
		message, reason := ValidateWebhookURL("https://exa mple.com/" + strings.Repeat("z", 100000))
		require.NotEmpty(t, message)
		assert.Contains(t, message, "too long",
			"length is checked first, so this is not reported as a parser failure")
		assert.NotContains(t, reason, "exa mple")
	})

	t.Run("neither the message nor the reason echoes the whole URL", func(t *testing.T) {
		// The URL is a third party's endpoint, and its path and query can carry a token. The HOST
		// is named deliberately — it is the one thing the caller needs to see — but nothing else.
		const secret = "s3cr3t-token-in-the-path"

		message, reason := ValidateWebhookURL("https://127.0.0.1/blnk/" + secret + "?key=" + secret)
		require.NotEmpty(t, message)

		assert.NotContains(t, message, secret)
		assert.NotContains(t, reason, secret)
		assert.Contains(t, reason, "127.0.0.1", "the host is what a caller has to act on")
	})

	t.Run("an unparseable URL does not echo the parser's rendering of it", func(t *testing.T) {
		// url.Parse quotes the input back, and the input has no business in Blnk's error
		// responses or logs.
		message, reason := ValidateWebhookURL("https://exa mple.com/\x7f")
		require.NotEmpty(t, message)
		assert.NotContains(t, reason, "exa mple")
	})

	t.Run("an external host is not classified as internal", func(t *testing.T) {
		for _, host := range []string{
			"hooks.example.com", "8.8.8.8", "203.0.113.10", "[2001:db8::1]",
		} {
			assert.Emptyf(t, InternalWebhookDestinationReason(strings.Trim(host, "[]")),
				"%s is a public destination", host)
		}
	})
}
