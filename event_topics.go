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
	"strings"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// This file is the topic-naming layer of the Kafka event pipeline. It answers
// exactly two questions and owns nothing else:
//
//  1. Which topic does this event type go to?
//  2. What is the dead-letter sibling of that topic?
//
// Every Kafka topic name Blnk writes to, provisions, alerts on or documents is
// produced here. Nowhere else in the codebase may a topic name be spelled as a
// literal: a deployment can move the whole namespace with KAFKA_TOPIC_PREFIX, and a
// literal somewhere else would keep pointing at "blnk." after the prefix changed —
// silently publishing to a topic no subscriber is reading.
//
// # Deliberately dependency-light
//
// There is NO Kafka import in this file, and there must not be one. Broker
// interaction lives in the publisher, the admin client and the dead-letter writer;
// this file is pure string composition over configuration, so it is exercisable to
// the last branch by a unit test with no broker, no network and no fixtures. That
// testability is the point: topic naming is the single decision the entire pipeline
// depends on, and a naming bug is invisible at runtime — messages simply land
// somewhere nobody is listening.
//
// # The mapping table is NOT here
//
// The event-type-to-category mapping is declared exactly once, in
// model.EventCategory, and this file DELEGATES to it. That split is the whole design:
//
//	model.EventCategory  → the bare category token ("transactions", "balances",
//	                       "identities", "ledgers", "system"). Knows the event
//	                       vocabulary.
//	this file            → "<prefix>.<category>" and "<prefix>.<category>.dlt".
//	                       Knows the prefix and the separator. Knows no event names.
//
// Do NOT add a second event-type table here, not even "just for the topics". Two
// tables that drift produce a routing bug with no compile error, no runtime error and
// no log line, visible only when a subscriber eventually notices missing events. If a
// new event type appears, extend model.EventCategory; this file then routes it with no
// change at all.
//
// # Five categories, not three
//
// The three category topics named in the requirements — transactions, balances,
// identities — do not cover every event Blnk actually emits. "ledger.created" (raised
// by the post-ledger-creation actions in ledger.go) and "system.error" (raised through
// the notification package's registered webhook-sender indirection) belong to none of
// them, while the coverage requirement is absolute: every event type that reached the
// legacy webhook sender must be published, with zero exceptions.
//
// Two further categories resolve that tension while following the identical naming
// convention, so nothing about the scheme is special-cased and each dead-letter sibling
// is derived by the same rule as every other. This is the recorded resolution of the
// coverage-versus-topic-list ambiguity, and it is a decision, not an oversight.
//
// They are TWO rather than one because coverage and REACHABILITY are different
// questions, and one topic could not answer both:
//
//	"<prefix>.ledgers"  ledger.created. SUBSCRIBER-FACING, because it is ordinary
//	                    ledger data that a subscriber receives over the legacy webhook
//	                    today and must be able to keep receiving after the sunset.
//	"<prefix>.system"   system.error, and anything this catalogue does not recognise.
//	                    INTERNAL, because the frozen system.error body carries verbatim
//	                    error text — PostgreSQL schema, table and routine names; broker
//	                    addresses — and because an internal catch-all is what stops a
//	                    routing omission delivering a domain payload to an audience that
//	                    never asked for it.
//
// Sharing one topic between them was the earlier arrangement and it was wrong in both
// directions: keeping it internal made ledger.created unreachable by every subscriber,
// and making it grantable would have handed internal error detail to anyone granted
// ledger events.
//
// Do NOT "tidy" either away. Folding these events into, say, the transactions topic
// corrupts that topic's semantics for every subscriber filtering on it, and dropping
// them violates the coverage requirement outright. The local stack, the production
// provisioning path and the operator documentation all provision the whole inventory on
// this basis; removing a category here would leave an event type publishing to a topic
// that no longer exists.
//
// # What this file deliberately does NOT do
//
//   - Message keying. Ordering comes from keying each Kafka message by the outbox row's
//     STORED PARTITION KEY with a stable hash balancer, plus the relay claiming rows in
//     occurrence order. Topic layout supports that guarantee; it does not implement it.
//     The key belongs to the publisher, and putting it here would split one ordering
//     decision across two files.
//   - Partition count and replication factor. Both are configuration
//     (config.Kafka.MinPartitions, default 6, which is the required minimum; and
//     config.Kafka.ReplicationFactor, 3 in production and necessarily 1 on the
//     single-broker local stack, because a one-broker KRaft cluster cannot satisfy 3
//     and topic creation fails outright). They are applied by the admin client when it
//     creates or grows a topic. A literal here would make one of those two
//     environments impossible.
//   - Topic creation, ACLs, or any other broker-side effect. This file returns strings.
//
// getEventFromStatus, which produces the seven transaction.* names this file routes, is
// declared in webhooks.go for the whole dual-delivery window and must outlive that file:
// it is the transaction event-string vocabulary, not part of the HTTP transport. The
// ordered removal procedure — including where the two surviving symbols go — is recorded
// once, in the sunset block at the foot of webhooks.go, and is operator work rather than
// anything the runtime date performs.

// DefaultTopicPrefix is the topic namespace used when KAFKA_TOPIC_PREFIX is not
// configured. It yields the documented default topic names: blnk.transactions,
// blnk.balances, blnk.identities, blnk.ledgers, blnk.system, and their .dlt siblings.
//
// It duplicates config's own Kafka default by value, and that is necessary rather than
// sloppy. The configuration default is applied by setKafkaDefaults, which runs only on
// the validate-and-default path; a configuration published straight into the store —
// as tests do, and as any caller bypassing that path would — carries an empty prefix.
// Resolving the default here as well is what makes topic naming correct regardless of
// how configuration arrived, and the two values are pinned equal by test.
const DefaultTopicPrefix = "blnk"

// DeadLetterTopicSuffix is the suffix appended to a topic name to form its
// dead-letter sibling: blnk.transactions becomes blnk.transactions.dlt.
//
// This suffix is a PUBLISHED, SUBSCRIBER-FACING CONVENTION, not an internal detail.
// It is documented so that subscribers building their own consumer-side
// dead-lettering do not collide with Blnk-owned names. Blnk owns every `<topic>.dlt`
// name derived from its own topics and does not implement, manage or consume
// subscriber-side dead-letter topics. Changing this value is a breaking change to
// every subscriber, every provisioning script, every alert rule and every runbook
// that names these topics.
const DeadLetterTopicSuffix = ".dlt"

// topicSeparator joins the prefix to a category token. Kafka permits dots in topic
// names, and the dotted namespace convention is what the published topic names use.
//
// Note the interaction with Kafka's own metric naming: Kafka replaces dots with
// underscores when it converts a topic name into a metric name, so two topics
// differing only in dot-versus-underscore placement would collide in broker metrics.
// The category tokens contain no dots and no underscores, so no such collision is
// possible here — a property worth preserving when adding a category.
const topicSeparator = "."

// topicPrefixTrimCutset is the set of characters stripped from both ends of a
// configured topic prefix: ASCII whitespace plus the separator.
//
// Both halves are load-bearing. Whitespace, because the configuration loader trims
// only a fixed set of fields and the topic prefix is not one of them, so a trailing
// newline from an environment file would otherwise be carried into every topic name.
// The separator, because a prefix written with a trailing dot — KAFKA_TOPIC_PREFIX=acme.
// is the natural mistake — would otherwise compose "acme..transactions", which Kafka
// accepts happily as a topic distinct from the intended one, so nothing fails and no
// subscriber ever receives anything.
const topicPrefixTrimCutset = " \t\n\v\f\r" + topicSeparator

// eventCategoryOrder is the canonical order of the event categories.
//
// The values are referenced from model rather than re-spelled, so the category
// vocabulary stays single-sourced; only the ORDER is declared here. Order matters
// because it fixes the order of AllTopics, AllDeadLetterTopics and
// AllTopicsWithDeadLetters, and callers compare those lists against provisioning
// scripts and operator documentation. A stable order makes those comparisons and their
// diffs meaningful.
//
// This is an enumeration for provisioning, NOT a mapping: it says which categories
// exist, never which event belongs to which. The mapping remains model.EventCategory's
// alone.
//
// The list itself now lives in model.AllEventCategories rather than being re-declared
// here. It used to be a second copy, and a second copy of an enumeration is a second
// thing to forget: adding a category to model without adding it here would have
// produced a topic that events route to and that nothing provisions.
//
// eventCategoryOrder is the local, immutable snapshot every accessor copies from, so
// no caller can reorder or truncate the list for everybody else.
func eventCategoryOrder() []string {
	return model.AllEventCategories()
}

// EventCategories returns the event category tokens in canonical order:
// "transactions", "balances", "identities", "ledgers", "system".
//
// These are bare tokens, not topic names. Compose a topic from one with
// TopicForCategory; treating a returned value as a topic is a bug.
//
// Returns:
//   - []string: a fresh slice the caller may sort, filter or otherwise mutate freely.
func EventCategories() []string {
	return eventCategoryOrder()
}

// SubscriberGrantableTopics returns the fully-qualified topics a subscriber may be
// authorised to consume: every category topic, with the configured prefix applied, and
// no dead-letter sibling.
//
// # This function is an authorization allowlist, not a convenience
//
// It is the single answer to "may this subscriber be granted this topic?", and both the
// request-validation layer and the Kafka ACL provisioning check against it. Before it
// existed each of them trimmed the caller's topic list and granted whatever remained,
// so an authorized request could name the literal wildcard, a dead-letter topic, an
// internal topic, or a topic belonging to another system entirely, and receive a real
// ACL binding over it.
//
// Three exclusions, each deliberate:
//
//   - DEAD-LETTER TOPICS. A `<topic>.dlt` record carries the original payload PLUS
//     Blnk's failure metadata — the broker error text, the attempt window, internal
//     topic names. That is operational detail for whoever runs Blnk, not data for the
//     subscriber whose event failed. Dead letters are triaged through the
//     master-key-gated dead-letter API instead.
//   - INTERNAL CATEGORIES. The system topic carries Blnk's own error records and, as
//     the catalogue's catch-all, events of unknown provenance; see
//     model.IsInternalEventCategory.
//   - ANYTHING NOT ON THIS LIST. Including a topic that merely looks Blnk-owned. The
//     test is membership in this exact set, never a prefix match, because a prefix
//     match would accept "blnk.transactions.something-else" and, with a
//     caller-supplied prefix, very nearly anything.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names, in canonical category
//     order.
func SubscriberGrantableTopics() []string {
	// Composed by model.SubscriberGrantableTopics rather than assembled here, so that this
	// package, the persistence boundary and the request DTO all read ONE list. Three
	// independent reconstructions of the same allowlist is three chances for one to drift,
	// and drift means a topic one layer refuses and another grants. All this function adds
	// is the configured prefix, which model cannot see.
	return model.SubscriberGrantableTopics(TopicPrefix())
}

// IsSubscriberGrantableTopic reports whether a topic may be granted to a subscriber.
//
// The comparison is exact against SubscriberGrantableTopics. It is deliberately NOT a
// prefix test and NOT a normalising test: the caller-supplied value is compared as
// given, after trimming surrounding whitespace only, so "BLNK.TRANSACTIONS",
// "blnk.transactions ", "blnk.transactions.dlt", "*" and "blnk.system" are all
// refused. Being strict here is the whole value of the function — every leniency is a
// way for an unintended grant to slip through.
//
// Parameters:
//   - topic string: the candidate topic. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true only for an exact match against a non-internal category topic.
func IsSubscriberGrantableTopic(topic string) bool {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return false
	}

	for _, grantable := range SubscriberGrantableTopics() {
		if topic == grantable {
			return true
		}
	}

	return false
}

// IsBlnkOwnedTopic reports whether a topic is one of the names Blnk itself publishes
// to — a category topic or one of their dead-letter siblings.
//
// It is the check the publisher applies before creating a writer and the repository
// applies before storing a destination, so that neither can be steered at an arbitrary
// topic by a stored or replayed value. It is BROADER than
// IsSubscriberGrantableTopic — it includes the internal and dead-letter names, because
// Blnk legitimately writes to all of them — and the two must not be confused: this one
// answers "may WE write here?", the other answers "may a SUBSCRIBER read here?".
//
// Parameters:
//   - topic string: the candidate topic. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true only for an exact match against the current topic inventory.
func IsBlnkOwnedTopic(topic string) bool {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return false
	}

	for _, owned := range AllTopicsWithDeadLetters() {
		if topic == owned {
			return true
		}
	}

	return false
}

// topicPrefixFrom resolves the effective topic prefix from a configuration value.
//
// It is the pure half of prefix resolution — no configuration store, no globals — so
// every branch below is directly reachable from a unit test. TopicPrefix is the
// impure half that reads live configuration and delegates here.
//
// Three inputs all resolve to DefaultTopicPrefix, and each is a real condition rather
// than defensive padding:
//
//   - A nil configuration. Reached when configuration has not been loaded yet.
//   - An empty prefix. Reached whenever a configuration was published without running
//     the validate-and-default path that would have filled it.
//   - A prefix consisting only of whitespace and separators. Reached from a stray
//     newline or a lone dot in an environment file.
//
// Anything else is used as configured, with whitespace and separators stripped from
// both ends. Interior characters are deliberately left alone: a prefix with an
// embedded space or an illegal character is a misconfiguration that must surface as a
// loud topic-creation failure at the broker, not be silently rewritten here into a
// topic name the operator never asked for.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read the prefix from. May be nil.
//
// Returns:
//   - string: a non-empty prefix, never containing a leading or trailing separator.
func topicPrefixFrom(cnf *config.Configuration) string {
	if cnf == nil {
		return DefaultTopicPrefix
	}

	prefix := strings.Trim(cnf.Kafka.TopicPrefix, topicPrefixTrimCutset)
	if prefix == "" {
		return DefaultTopicPrefix
	}

	return prefix
}

// TopicPrefix returns the effective Kafka topic namespace prefix.
//
// The value comes from KAFKA_TOPIC_PREFIX (config.Kafka.TopicPrefix) and falls back to
// DefaultTopicPrefix when it is unset, blank, or configuration has not been loaded. It
// is the one place the prefix is read; every other name in this file is composed from
// its result, so a deployment that renames the namespace renames every topic
// consistently.
//
// A configuration store that has not been populated is not an error here. Answering
// with the default keeps topic naming total — every event still resolves to a real
// topic name — which matters because naming is exercised by tests and tooling that
// have no reason to load a full configuration.
//
// The value is re-read on every call rather than cached, for the same reason the
// sunset date is: the configuration store is an atomic value that is re-published
// whenever configuration is (re)loaded, so a cached prefix would keep answering from
// configuration that no longer exists. The cost is a trim of one short string. Nothing
// on a hot path pays it repeatedly either — the outbox row records its fully-resolved
// topic at insert time, so the relay never re-derives a name per publish attempt.
//
// Returns:
//   - string: a non-empty prefix with no leading or trailing separator.
func TopicPrefix() string {
	cnf, err := fetchConfiguration()
	if err != nil {
		return DefaultTopicPrefix
	}

	return topicPrefixFrom(cnf)
}

// TopicForCategory composes the fully-qualified topic name for a category token.
//
// The composition is "<prefix>.<category>", so with the default prefix the five
// category tokens yield blnk.transactions, blnk.balances, blnk.identities,
// blnk.ledgers and blnk.system.
//
// An empty or blank category resolves to the system category rather than composing
// "<prefix>." — a name with a trailing separator and an empty final segment, which
// Kafka would accept as a real topic that nothing reads. Falling back keeps every
// caller's result a usable topic and matches the never-drop-an-event policy that
// model.EventCategory's own catch-all implements.
//
// An unrecognised but non-blank category is composed as given rather than coerced.
// This function is a name composer, not a validator: a category added to
// model.EventCategory works here with no change, which is the extension path that
// keeps the two files from having to be edited in lockstep. Provisioning a topic for a
// new category is the admin path's concern, and an unprovisioned topic surfaces there
// loudly rather than being masked here.
//
// Parameters:
//   - category string: a category token, normally one of the values returned by
//     EventCategories. May be empty.
//
// Returns:
//   - string: a fully-qualified, non-empty topic name.
func TopicForCategory(category string) string {
	category = strings.Trim(category, topicPrefixTrimCutset)
	if category == "" {
		// The system category, which is model.EventCategory's own catch-all. A
		// blank category means the caller could not classify the event, and the
		// system category is where an unclassifiable event belongs for one
		// concrete reason: it is INTERNAL, so no subscriber can be granted its
		// topic. The event still lands on a real topic and stays replayable,
		// while a routing omission cannot deliver a domain payload to an audience
		// that never asked for it. See model.EventCategorySystem.
		category = model.EventCategorySystem
	}

	return TopicPrefix() + topicSeparator + category
}

// TopicForEvent returns the fully-qualified category topic an event type is published
// to.
//
// It is the routing entry point for the whole pipeline: the outbox records the topic
// this function returns, the publisher writes to it, and a replay sends a
// dead-lettered event back to it. Resolution is two steps, each owned by exactly one
// place — model.EventCategory maps the event type to a category token, and
// TopicForCategory applies the configured prefix.
//
// Coverage is total. Every event type Blnk emits resolves to a category topic:
//
//	blnk.transactions  transaction.queued, transaction.applied, transaction.scheduled,
//	                   transaction.inflight, transaction.void, transaction.rejected,
//	                   transaction.unknown, and any bulk_transaction.<status>
//	blnk.balances      balance.created, balance.monitor
//	blnk.identities    identity.created
//	blnk.ledgers       ledger.created
//	blnk.system        system.error, and anything this catalogue does not
//	                   recognise                                   (internal)
//
// Two properties of that resolution are easy to get wrong and are worth stating
// explicitly, because both live in the delegated mapping rather than here:
//
//   - Bulk transaction events are matched BY PREFIX. Their names are composed at
//     runtime as "bulk_transaction." + batch status, so the suffix set is open and an
//     exact-match table would silently route every one of them to the catch-all. Any
//     status value, including one introduced later, routes to blnk.transactions.
//   - An UNRECOGNISED event type routes to blnk.system, the catalogue's catch-all. It
//     is never rejected, and this function never returns an empty string: the relay
//     publishes whatever topic it is given, so an empty result would strand a
//     committed event and dropping it would lose one. The destination is safe because
//     blnk.system is INTERNAL and cannot be granted to any subscriber, so a producer
//     added without extending the catalogue cannot deliver its payload — possibly a
//     balance or an identity record — to an audience that never asked for it, while
//     the event stays durable, observable and replayable. Anything landing there
//     under an unrecognised name is a defect to fix by extending
//     model.EventCategory, and the publisher warns when it happens.
//
// Parameters:
//   - eventType string: the event name, for example "transaction.applied". May be
//     empty or unrecognised.
//
// Returns:
//   - string: a fully-qualified, non-empty topic name.
func TopicForEvent(eventType string) string {
	return TopicForCategory(model.EventCategory(eventType))
}

// DLTFor returns the dead-letter sibling of a topic by appending DeadLetterTopicSuffix:
// blnk.transactions becomes blnk.transactions.dlt.
//
// THIS FUNCTION IS THE PUBLISHED `<topic>.dlt` NAMING CONVENTION. It is the single
// implementation of a rule that subscribers, provisioning scripts, alert rules and
// operator runbooks all depend on, and it is documented externally so subscribers
// building their own consumer-side dead-lettering do not collide with Blnk-owned
// names. Blnk owns the `.dlt` sibling of every topic it owns, and it does not
// implement, manage or consume subscriber-side dead-letter topics.
//
// It is IDEMPOTENT: a topic that already ends in the suffix is returned unchanged.
// Double application is a real hazard — the dead-letter writer resolves a name, records
// it on the outbox row, and a replay path reads it back, so an accidental second
// application anywhere in that chain would produce blnk.transactions.dlt.dlt, a topic
// that exists (Kafka would create it on demand under permissive settings), that no
// consumer subscribes to, and that no alert covers. Absorbing the second application
// makes that whole class of bug impossible instead of merely discouraged.
//
// The idempotency rule implies one invariant: NO CATEGORY MAY BE NAMED "dlt". A
// blnk.dlt category topic would be indistinguishable from an already-derived name and
// could never get a dead-letter sibling of its own. None of the five categories is, and
// a test pins it.
//
// An empty or blank topic returns the empty string rather than a bare ".dlt". There is
// nothing to derive a sibling from, and inventing a name would create a real topic
// consisting only of a suffix. The caller's own publish then fails visibly, since Kafka
// rejects an empty topic — which is the correct outcome for what is a programming
// error at the call site.
//
// Parameters:
//   - topic string: a fully-qualified topic name. Surrounding whitespace is ignored.
//
// Returns:
//   - string: the dead-letter topic name, or the empty string when topic is blank.
func DLTFor(topic string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return ""
	}

	if strings.HasSuffix(topic, DeadLetterTopicSuffix) {
		return topic
	}

	return topic + DeadLetterTopicSuffix
}

// DeadLetterTopicForEvent returns the dead-letter topic an event type is routed to once
// its retry budget is exhausted.
//
// It is DLTFor(TopicForEvent(eventType)) and nothing more, declared so the dead-letter
// writer does not have to compose the two calls itself and cannot compose them wrongly
// — the ordering matters, and DLTFor(TopicForEvent(x)) is the only correct order.
// Because dead-lettering is per category, an event's dead-letter topic is always the
// sibling of the topic it failed to reach, which is what keeps a dead-letter entry
// replayable to its original destination.
//
// Parameters:
//   - eventType string: the event name. May be empty or unrecognised, in which case the
//     system category's dead-letter topic is returned.
//
// Returns:
//   - string: a fully-qualified, non-empty dead-letter topic name.
func DeadLetterTopicForEvent(eventType string) string {
	return DLTFor(TopicForEvent(eventType))
}

// IsDeadLetterTopic reports whether a topic name is a dead-letter topic.
//
// It exists so that callers which must distinguish the two kinds of topic — a
// dead-letter listing, an alert label, an operator-facing summary, a guard against
// republishing a dead-letter entry onto a dead-letter topic — test the same rule DLTFor
// applies, instead of each spelling the suffix check out for itself.
//
// The test is on the suffix alone, so it is prefix-agnostic and answers correctly for
// any configured namespace. A blank topic is not a dead-letter topic.
//
// Parameters:
//   - topic string: a topic name. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true when topic ends in DeadLetterTopicSuffix.
func IsDeadLetterTopic(topic string) bool {
	return strings.HasSuffix(strings.TrimSpace(topic), DeadLetterTopicSuffix)
}

// AllTopics returns every category topic in canonical order, with the configured
// prefix applied: blnk.transactions, blnk.balances, blnk.identities, blnk.ledgers and
// blnk.system.
//
// These are the topics events are published to, INCLUDING the internal one — Blnk
// writes to all of them, and all of them must be provisioned. Which of them a
// subscriber may be granted is a different question, answered by
// SubscriberGrantableTopics. It excludes the dead-letter siblings; use
// AllTopicsWithDeadLetters for everything Blnk owns.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names, one per category.
func AllTopics() []string {
	prefix := TopicPrefix()

	categories := eventCategoryOrder()

	topics := make([]string, 0, len(categories))
	for _, category := range categories {
		topics = append(topics, prefix+topicSeparator+category)
	}

	return topics
}

// AllDeadLetterTopics returns every dead-letter topic in canonical order, with the
// configured prefix applied: blnk.transactions.dlt, blnk.balances.dlt,
// blnk.identities.dlt, blnk.ledgers.dlt and blnk.system.dlt.
//
// Each is the DLTFor sibling of the AllTopics entry at the same index, so the two
// slices can be zipped safely.
//
// Returns:
//   - []string: a fresh slice of fully-qualified dead-letter topic names, one per
//     category.
func AllDeadLetterTopics() []string {
	topics := AllTopics()
	for i, topic := range topics {
		topics[i] = DLTFor(topic)
	}

	return topics
}

// AllTopicsWithDeadLetters returns every topic Blnk owns: every category topic
// followed by every dead-letter sibling — ten names with the five categories the
// topic contract declares.
//
// THIS IS THE SINGLE SOURCE OF TRUTH FOR THE TOPIC INVENTORY. The admin client's topic
// assurance creates and grows exactly this set, and the local provisioning script
// creates exactly this set. Deriving both from one list is what stops them drifting: a
// script that creates a topic the code never writes to is dead weight, and code that
// writes to a topic the script never created either fails on an unprovisioned topic or,
// worse, silently auto-creates one with a single partition and the wrong replication
// factor, quietly discarding the ordering and durability guarantees.
//
// The order — all category topics, then all dead-letter topics — is deliberate and
// matches the provisioning script's order, so the two can be diffed line for line.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names, two per category.
func AllTopicsWithDeadLetters() []string {
	category := AllTopics()

	topics := make([]string, 0, len(category)*2)
	topics = append(topics, category...)
	for _, topic := range category {
		topics = append(topics, DLTFor(topic))
	}

	return topics
}

// IsOwnedTopicForm reports whether a name has the SHAPE of a topic Blnk owns, without
// pinning it to the configured prefix: '<any prefix>.<category>' optionally followed by
// the dead-letter suffix.
//
// It exists for exactly one case, and it is a case a strict configured-prefix test gets
// wrong. An outbox row records its destination topic at INSERT time, so a row written
// before KAFKA_TOPIC_PREFIX changed still names the previous generation's topic. Refusing
// that name would strand a COMMITTED event: the relay could never publish it and it would
// sit in the outbox until an operator noticed. Accepting the owned form keeps it
// publishable while still refusing anything that is not one of Blnk's own categories —
// a broker-internal topic like '__consumer_offsets', another system's topic, an unknown
// category, a bare category with no prefix, or a dead-letter topic's dead-letter topic.
//
// It is deliberately WIDER than model.IsBlnkEventTopic, which is the configured-prefix
// test and remains the right check wherever a topic is being created or granted. The
// residual width — '<someone else>.transactions' has the owned form — is bounded by the
// only caller that needs it: a writer is resolved for a topic that came from a stored
// outbox row, and the outbox insert validates the topic against the owned inventory
// before the row exists at all.
//
// Parameters:
//   - topic string: the candidate topic name.
//
// Returns:
//   - bool: true when the name has the owned form under some prefix.
func IsOwnedTopicForm(topic string) bool {
	name := strings.TrimSpace(topic)
	if name == "" || len(name) > model.MaxTopicNameLength {
		return false
	}

	// The registry's own bounds double as a character-set check here: a name carrying a
	// comma, a quote or a brace cannot be a Kafka topic at all.
	if err := model.ValidateSubscriberTopics([]string{name}); err != nil {
		return false
	}

	// ONE suffix strip only. A second would accept '<prefix>.<category>.dlt.dlt', which
	// names a dead-letter topic's dead-letter topic — a thing Blnk never creates and never
	// publishes to.
	name = strings.TrimSuffix(name, DeadLetterTopicSuffix)

	separator := strings.LastIndex(name, ".")
	if separator <= 0 || separator == len(name)-1 {
		return false
	}

	prefix, category := name[:separator], name[separator+1:]
	if strings.TrimSpace(prefix) == "" {
		return false
	}

	for _, known := range model.AllEventCategories() {
		if category == known {
			return true
		}
	}

	return false
}
