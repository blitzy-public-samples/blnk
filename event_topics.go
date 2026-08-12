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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// eventTopicBacklogStore is the one repository method the stranded-prefix audit needs.
//
// Declared as its own interface rather than taking database.IDataSource so the audit is
// reachable from a test with a few lines of fake, and so this file's dependency on
// persistence stays exactly one method wide.
type eventTopicBacklogStore interface {
	ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error)
}

// This file is the topic-naming layer of the Kafka event pipeline. It answers exactly
// two questions and owns nothing else:
//
//  1. Which topic does this event type go to?
//  2. What is the dead-letter sibling of that topic?
//
// Every Kafka topic name Blnk writes to, provisions, alerts on or documents is produced
// here. Nowhere else in the codebase may a topic name be spelled as a literal: a
// deployment can move the whole namespace with KAFKA_TOPIC_PREFIX, and a literal
// elsewhere would keep pointing at "blnk." after the prefix changed — silently
// publishing to a topic no subscriber is reading.
//
// There is no Kafka import here, and there must not be one. Broker interaction lives in
// the publisher, the admin client and the dead-letter writer; this file is pure string
// composition over configuration, so a unit test covers it to the last branch with no
// broker and no fixtures.
//
// The event-type-to-category mapping is declared once, in model.EventCategory, and this
// file delegates to it:
//
//	model.EventCategory  → the bare category token ("transactions", "balances",
//	                       "identities", "system"). Knows the event vocabulary.
//	this file            → "<prefix>.<category>" and "<prefix>.<category>.dlt".
//	                       Knows the prefix and the separator, and no event names.
//
// FOUR CATEGORIES, so eight Blnk-owned topics. Three of them — transactions, balances,
// identities — are tenant categories any subscriber may be granted. The fourth,
// `<prefix>.system`, carries ledger.created, system.error and anything the catalogue does
// not recognise; it is INTERNAL, because the frozen system.error body renders verbatim
// error text, and it is grantable only where the deployment has declared
// KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS — which is therefore also how a subscriber
// reaches ledger.created, a cost documented in docs/event-streaming.md rather than avoided
// by widening the catalogue.

// getEventFromStatus maps a transaction status to a corresponding event string.
//
// This is the transaction event-string vocabulary: seven of the thirteen event names
// Blnk emits originate here, and they are the names that route a Kafka message to a
// topic. It belongs beside the topic resolution it feeds.
//
// The table itself lives in model.EventTypeForTransactionStatus and this is a one-line
// delegation to it, because the repository layer derives event rows inside the atomic
// writers and needs the same mapping while `model` cannot import the root package. One
// table in one package is what stops a status resolving to two different event names
// depending on which layer looked.
//
// StatusCommit ("COMMIT") has no case in the table and falls through to
// transaction.unknown. That is long-standing behaviour and it is kept: the dual-delivery
// comparison asserts the Kafka message and the legacy webhook carry identical bytes for
// the same event, so adding a transaction.commit case would fail that comparison for a
// reason unrelated to the transport. docs/event-streaming.md carries it.
//
// Parameters:
// - status string: The status of the transaction.
//
// Returns:
// - string: The corresponding event string for the transaction status.
func getEventFromStatus(status string) string {
	return model.EventTypeForTransactionStatus(status)
}

// DefaultTopicPrefix is the topic namespace used when KAFKA_TOPIC_PREFIX is not
// configured. It yields the documented default topic names: blnk.transactions,
// blnk.balances, blnk.identities, blnk.system, and their.dlt siblings.
const DefaultTopicPrefix = "blnk"

// DeadLetterTopicSuffix is the suffix appended to a topic name to form its dead-letter
// sibling: blnk.transactions becomes blnk.transactions.dlt.
//
// This suffix is a PUBLISHED, SUBSCRIBER-FACING CONVENTION, not an internal detail. It
// is documented so that subscribers building their own consumer-side dead-lettering do
// not collide with Blnk-owned names.
const DeadLetterTopicSuffix = ".dlt"

// topicSeparator joins the prefix to a category token. Kafka permits dots in topic
// names, and the dotted namespace convention is what the published topic names use.
const topicSeparator = "."

// topicPrefixTrimCutset is the set of characters stripped from both ends of a
// configured topic prefix: ASCII whitespace plus the separator.
//
// Both halves are load-bearing. Whitespace, because the configuration loader trims only
// a fixed set of fields and the topic prefix is not one of them, so a trailing newline
// from an environment file would otherwise be carried into every topic name.
const topicPrefixTrimCutset = " \t\n\v\f\r" + topicSeparator

// eventCategoryOrder is the canonical order of the event categories.
//
// This is an enumeration for provisioning, NOT a mapping: it says which categories
// exist, never which event belongs to which. The mapping remains model.EventCategory's
// alone.
func eventCategoryOrder() []string {
	return model.AllEventCategories()
}

// EventCategories returns the event category tokens in canonical order: "transactions",
// "balances", "identities", "system".
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
// Two exclusions, each deliberate:
//
//   - DEAD-LETTER TOPICS. A `<topic>.dlt` record carries the original payload PLUS
//     Blnk's failure metadata — the broker error text, the attempt window, internal
//     topic names. That is operational detail for whoever runs Blnk, not data for the
//     subscriber whose event failed. Dead letters are triaged through the
//     master-key-gated dead-letter API instead.
//   - ANYTHING NOT ON THIS LIST. Including a topic that merely looks Blnk-owned. The
//     test is membership in this exact set, never a prefix match, because a prefix
//     match would accept "blnk.transactions.something-else" and, with a
//     caller-supplied prefix, very nearly anything.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names, in canonical category
//     order.
func SubscriberGrantableTopics() []string {
	// Composed by model.SubscriberAuthorizableTopics rather than assembled here, so that
	// this package, the persistence boundary and the request DTO all read ONE list. Three
	// independent reconstructions of the same allowlist is three chances for one to drift,
	// and drift means a topic one layer refuses and another grants. All this function adds
	// is what model cannot see: the configured prefix, and whether this deployment has
	// acknowledged the privileged category.
	return model.SubscriberAuthorizableTopics(TopicPrefix(), SubscriberInternalTopicAccessDeclared())
}

// SubscriberInternalTopicAccessDeclared reports whether this deployment has
// acknowledged that a subscriber may be granted the internal category topic.
//
// A configuration that cannot be read answers FALSE — the fail-closed direction: an
// unloaded configuration must not be the thing that makes Blnk's internal error stream
// grantable.
//
// Returns:
//   - bool: true only when the deployment has declared the acknowledgement.
func SubscriberInternalTopicAccessDeclared() bool {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return false
	}

	return cnf.Kafka.SubscriberInternalTopicAccess
}

// IsSubscriberGrantableTopic reports whether a topic may be granted to a subscriber in
// this deployment.
//
// Parameters:
//   - topic string: the candidate topic. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true only for an exact match against a resolved grantable category topic.
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

// IsSubscriberPrivilegedTopic reports whether a topic is one of the names that require
// the deployment-level acknowledgement, independently of whether this deployment has
// made it.
//
// Parameters:
//   - topic string: the candidate topic. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true only for an exact match against a privileged category topic under the
//     configured prefix.
func IsSubscriberPrivilegedTopic(topic string) bool {
	return model.IsSubscriberPrivilegedTopicName(strings.TrimSpace(topic), TopicPrefix())
}

// IsBlnkOwnedTopic reports whether a topic is one of the names Blnk itself publishes to
// — a category topic or one of their dead-letter siblings.
//
// IT IS NOT THE TEST FOR A STORED TOPIC NAME. A row records its destination at insert
// time, so a row captured before a KAFKA_TOPIC_PREFIX rename names a topic that is
// owned and is deliberately absent from the current inventory.
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
// every branch below is directly reachable from a unit test. TopicPrefix is the impure
// half that reads live configuration and delegates here.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read the prefix from.
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
// A configuration store that has not been populated is not an error here. Answering
// with the default keeps topic naming total — every event still resolves to a real
// topic name — which matters because naming is exercised by tests and tooling that have
// no reason to load a full configuration.
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
// The composition is "<prefix>.<category>", so with the default prefix the four
// category tokens yield blnk.transactions, blnk.balances, blnk.identities and
// blnk.system.
//
// Parameters:
//   - category string: a category token, normally one of the values returned by
//     EventCategories.
//
// Returns:
//   - string: a fully-qualified, non-empty topic name.
func TopicForCategory(category string) string {
	category = strings.Trim(category, topicPrefixTrimCutset)
	if category == "" {
		// The system category, which is model.EventCategory's own catch-all. A blank category
		// means the caller could not classify the event, and the system category is where an
		// unclassifiable event belongs: it is a real, provisioned topic with a dead-letter
		// sibling and gauge coverage, so the event stays durable and replayable, and it is
		// the topic an operator grants deliberately rather than as a matter of course, so a
		// routing omission does not reach a domain topic's audience. See
		// model.EventCategorySystem.
		category = model.EventCategorySystem
	}

	return TopicPrefix() + topicSeparator + category
}

// TopicForEvent returns the fully-qualified category topic an event type is published
// to.
//
// It is the routing entry point for the whole pipeline: the outbox records the topic
// this function returns, the publisher writes to it, and a replay sends a dead-lettered
// event back to it. Resolution is two steps, each owned by exactly one place —
// model.EventCategory maps the event type to a category token, and TopicForCategory
// applies the configured prefix.
//
// Coverage is total. Every event type Blnk emits resolves to a category topic:
//
//	blnk.transactions  transaction.queued, transaction.applied, transaction.scheduled,
//	                   transaction.inflight, transaction.void, transaction.rejected,
//	                   transaction.unknown, and any bulk_transaction.<status>
//	blnk.balances      balance.created, balance.monitor
//	blnk.identities    identity.created
//	blnk.system        ledger.created, system.error, and anything this
//	                   catalogue does not recognise                 (internal)
//
// Parameters:
//   - eventType string: the event name, for example "transaction.applied".
//
// Returns:
//   - string: a fully-qualified, non-empty topic name.
func TopicForEvent(eventType string) string {
	return TopicForCategory(model.EventCategory(eventType))
}

// DLTFor returns the dead-letter sibling of a topic by appending DeadLetterTopicSuffix:
// blnk.transactions becomes blnk.transactions.dlt.
//
// The idempotency rule implies one invariant: NO CATEGORY MAY BE NAMED "dlt". A
// blnk.dlt category topic would be indistinguishable from an already-derived name and
// could never get a dead-letter sibling of its own.
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
// Parameters:
//   - topic string: a topic name. Surrounding whitespace is ignored.
//
// Returns:
//   - bool: true when topic ends in DeadLetterTopicSuffix.
func IsDeadLetterTopic(topic string) bool {
	return strings.HasSuffix(strings.TrimSpace(topic), DeadLetterTopicSuffix)
}

// AllTopics returns every category topic in canonical order, with the configured prefix
// applied — four names with the default prefix: blnk.transactions, blnk.balances,
// blnk.identities and blnk.system.
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

// HistoricalTopicPrefixes returns the topic namespaces this deployment USED TO own and
// must still be able to publish to, from KAFKA_HISTORICAL_TOPIC_PREFIXES.
//
// Empty for every deployment that has never renamed its namespace, which is almost all
// of them. See config.KafkaConfig.HistoricalTopicPrefixes for what listing a prefix
// does and why the list is meant to be drained rather than accumulated.
//
// Returns:
//   - []string: a fresh slice of distinct, non-blank prefixes, none equal to
//     TopicPrefix().
func HistoricalTopicPrefixes() []string {
	owned := OwnedTopicPrefixes()
	if len(owned) <= 1 {
		return nil
	}

	return owned[1:]
}

// OwnedTopicPrefixes returns every topic namespace this deployment owns: TopicPrefix()
// first, then each historical prefix.
//
// Returns:
//   - []string: one or more distinct, non-blank prefixes, the configured one first.
func OwnedTopicPrefixes() []string {
	cnf, err := fetchConfiguration()
	if err != nil || cnf == nil {
		return []string{DefaultTopicPrefix}
	}

	// The configured prefix is resolved through topicPrefixFrom rather than through the
	// config accessor's own trim, so that a blank or separator-only value resolves to
	// DefaultTopicPrefix exactly as every other name in this file does.
	current := topicPrefixFrom(cnf)

	prefixes := []string{current}
	seen := map[string]struct{}{current: {}}

	for _, raw := range cnf.Kafka.HistoricalTopicPrefixes {
		prefix := strings.Trim(raw, topicPrefixTrimCutset)
		if prefix == "" {
			continue
		}
		if _, already := seen[prefix]; already {
			continue
		}
		seen[prefix] = struct{}{}
		prefixes = append(prefixes, prefix)
	}

	return prefixes
}

// AllTopicsWithDeadLettersForPrefix returns every topic Blnk owns under ONE given
// prefix: every category topic followed by every dead-letter sibling.
//
// Parameters:
//   - prefix string: the namespace to compose under. Trimmed; a blank value resolves to
//     DefaultTopicPrefix, matching every other prefix reader here.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names, two per category.
func AllTopicsWithDeadLettersForPrefix(prefix string) []string {
	prefix = strings.Trim(prefix, topicPrefixTrimCutset)
	if prefix == "" {
		prefix = DefaultTopicPrefix
	}

	categories := eventCategoryOrder()

	topics := make([]string, 0, len(categories)*2)
	for _, category := range categories {
		topics = append(topics, prefix+topicSeparator+category)
	}
	for _, category := range categories {
		topics = append(topics, prefix+topicSeparator+category+DeadLetterTopicSuffix)
	}

	return topics
}

// AllOwnedTopicsAcrossPrefixes returns every topic Blnk owns across every owned prefix:
// the configured generation's inventory first, then each historical generation's.
//
// Two callers need the whole set rather than the current generation:
//
//   - The publisher pre-creates a writer for each, so a row captured before a prefix rename
//     is served from the fast path after a restart instead of being refused. A writer
//     performs no I/O until its first write, so an inventory for a namespace nothing writes
//     to costs a struct.
//   - Topic assurance keeps every one of them present, so a historical topic that was
//     deleted — or never existed on a broker restored from elsewhere — is recreated rather
//     than failing every publish of the rows that name it.
//
// Returns:
//   - []string: a fresh slice of fully-qualified topic names.
func AllOwnedTopicsAcrossPrefixes() []string {
	prefixes := OwnedTopicPrefixes()

	topics := make([]string, 0, len(prefixes)*len(eventCategoryOrder())*2)
	for _, prefix := range prefixes {
		topics = append(topics, AllTopicsWithDeadLettersForPrefix(prefix)...)
	}

	return topics
}

// IsOwnedTopicUnderAnyConfiguredPrefix reports whether a topic lies inside a namespace
// this deployment owns — the configured prefix or any historical one.
//
// Parameters:
//   - topic string: the fully-resolved candidate name.
//
// Returns:
//   - bool: true when the name is a category or dead-letter topic under an owned
//     prefix.
func IsOwnedTopicUnderAnyConfiguredPrefix(topic string) bool {
	return model.IsBlnkEventTopicUnderAnyPrefix(topic, OwnedTopicPrefixes())
}

// AllDeadLetterTopics returns every dead-letter topic in canonical order, with the
// configured prefix applied — four names with the default prefix:
// blnk.transactions.dlt, blnk.balances.dlt, blnk.identities.dlt and blnk.system.dlt.
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

// AllTopicsWithDeadLetters returns every topic Blnk owns: every category topic followed
// by every dead-letter sibling — eight names with the four categories the topic
// contract declares.
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

// StrandedTopicPrefix is one topic namespace that outbox rows still name but this
// deployment no longer owns.
//
// It is the finding of AuditStrandedTopicPrefixes, and it carries everything the remedy
// needs: the prefix to add to KAFKA_HISTORICAL_TOPIC_PREFIXES, how many rows are
// waiting behind it, and how old the oldest one is.
type StrandedTopicPrefix struct {
	// Prefix is the namespace, as parsed from the stored topic names. This is the exact
	// value to add to KAFKA_HISTORICAL_TOPIC_PREFIXES.
	Prefix string

	// Topics are the stored destination names under Prefix, in the order the audit read
	// them — oldest outstanding occurrence first.
	Topics []string

	// UndeliveredRows is how many rows under Prefix still owe a first successful publish.
	UndeliveredRows int64

	// ReplayableRows is how many dead-lettered rows under Prefix could be replayed.
	ReplayableRows int64

	// OldestOccurredAt is the occurrence instant of the oldest row counted here.
	OldestOccurredAt time.Time
}

// TotalRows is how many rows are stranded behind this prefix.
//
// Returns:
//   - int64: the sum of the two counts.
func (s StrandedTopicPrefix) TotalRows() int64 {
	return s.UndeliveredRows + s.ReplayableRows
}

// AuditStrandedTopicPrefixes reports the topic namespaces that outbox rows still name
// and this deployment no longer owns.
//
// This turns that into a prefix an operator can act on. Whoever calls it decides how
// loudly to say so; cmd/server.go reports each finding at start-up, naming the variable
// to set.
//
// Parameters:
//   - ctx context.Context: cancels the underlying query.
//   - store eventTopicBacklogStore: the outbox repository.
//
// Returns:
//   - []StrandedTopicPrefix: one entry per unowned prefix, oldest outstanding
//     occurrence first.
//   - error: the repository error, unwrapped, so a caller can log it as the measurement
//     failure it is rather than reporting a stranded prefix it never saw.
func AuditStrandedTopicPrefixes(
	ctx context.Context, store eventTopicBacklogStore,
) ([]StrandedTopicPrefix, error) {
	if store == nil {
		return nil, errors.New("blnk: an outbox repository is required to audit stranded topic prefixes")
	}

	backlogs, err := store.ListUndrainedEventTopics(ctx)
	if err != nil {
		return nil, err
	}

	// Insertion-ordered, so the "oldest outstanding occurrence first" ordering the query
	// established survives the grouping. A map alone would randomise it, and the ordering
	// is what puts the most overdue generation at the top of the log.
	order := make([]string, 0, len(backlogs))
	found := make(map[string]*StrandedTopicPrefix, len(backlogs))

	for _, backlog := range backlogs {
		if IsOwnedTopicUnderAnyConfiguredPrefix(backlog.Topic) {
			continue
		}

		// A name that is not even the owned FORM — no recognised category, or no prefix at
		// all — is not a stranded generation. It is a row that should never have been
		// storable, and reporting it as "add this to the allowlist" would be advice that
		// widens the namespace on the strength of one malformed row. The relay's own
		// per-attempt refusal already names it.
		prefix, ok := topicPrefixOf(backlog.Topic)
		if !ok {
			continue
		}

		entry, seen := found[prefix]
		if !seen {
			order = append(order, prefix)
			found[prefix] = &StrandedTopicPrefix{
				Prefix:           prefix,
				Topics:           []string{backlog.Topic},
				UndeliveredRows:  backlog.UndeliveredRows,
				ReplayableRows:   backlog.ReplayableRows,
				OldestOccurredAt: backlog.OldestOccurredAt,
			}

			continue
		}

		entry.Topics = append(entry.Topics, backlog.Topic)
		entry.UndeliveredRows += backlog.UndeliveredRows
		entry.ReplayableRows += backlog.ReplayableRows
		if !backlog.OldestOccurredAt.IsZero() &&
			(entry.OldestOccurredAt.IsZero() || backlog.OldestOccurredAt.Before(entry.OldestOccurredAt)) {
			entry.OldestOccurredAt = backlog.OldestOccurredAt
		}
	}

	stranded := make([]StrandedTopicPrefix, 0, len(order))
	for _, prefix := range order {
		stranded = append(stranded, *found[prefix])
	}

	return stranded, nil
}

// topicPrefixOf extracts the namespace from a topic name that has the Blnk-owned FORM.
//
// It reports failure rather than guessing for a name that is not of that form, because
// the only caller uses the result to tell an operator which prefix to declare as owned,
// and a prefix parsed out of a name that is not one of Blnk's own topics would be
// advice to widen the owned namespace on the strength of a malformed row.
//
// Parameters:
//   - topic string: the candidate topic name.
//
// Returns:
//   - string: the prefix, with the category and any dead-letter suffix removed.
//   - bool: false when topic does not have the owned form, in which case the string is
//     empty.
func topicPrefixOf(topic string) (string, bool) {
	if !IsOwnedTopicForm(topic) {
		return "", false
	}

	name := strings.TrimSuffix(strings.TrimSpace(topic), DeadLetterTopicSuffix)

	separator := strings.LastIndex(name, ".")
	if separator <= 0 {
		return "", false
	}

	return name[:separator], true
}

// AuditStrandedTopicPrefixes reports the topic namespaces this instance's outbox rows
// still name and this deployment no longer owns.
//
// Parameters:
//   - ctx context.Context: cancels the underlying query.
//
// Returns:
//   - []StrandedTopicPrefix: one entry per unowned prefix, oldest outstanding
//     occurrence first.
//   - error: when the instance has no datasource, or the query failed.
func (b *Blnk) AuditStrandedTopicPrefixes(ctx context.Context) ([]StrandedTopicPrefix, error) {
	if b == nil || b.datasource == nil {
		return nil, errors.New(
			"blnk: no datasource is configured, so the event outbox cannot be audited for stranded " +
				"topic prefixes",
		)
	}

	return AuditStrandedTopicPrefixes(ctx, b.datasource)
}
