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
type eventTopicBacklogStore interface {
	ListUndrainedEventTopics(ctx context.Context) ([]model.EventTopicBacklog, error)
}

// This file is the topic-naming layer of the Kafka event pipeline. It answers exactly
// two questions and owns nothing else:

// getEventFromStatus maps a transaction status to a corresponding event string.
func getEventFromStatus(status string) string {
	return model.EventTypeForTransactionStatus(status)
}

// DefaultTopicPrefix is the topic namespace used when KAFKA_TOPIC_PREFIX is not
// configured. It yields the documented default topic names: blnk.transactions,
// blnk.balances, blnk.identities, blnk.system, and their.dlt siblings.
const DefaultTopicPrefix = "blnk"

// DeadLetterTopicSuffix is the suffix appended to a topic name to form its dead-letter
// sibling: blnk.transactions becomes blnk.transactions.dlt.
const DeadLetterTopicSuffix = ".dlt"

// topicSeparator joins the prefix to a category token. Kafka permits dots in topic
// names, and the dotted namespace convention is what the published topic names use.
const topicSeparator = "."

// topicPrefixTrimCutset is the set of characters stripped from both ends of a
// configured topic prefix: ASCII whitespace plus the separator.
const topicPrefixTrimCutset = " \t\n\v\f\r" + topicSeparator

// eventCategoryOrder is the canonical order of the event categories.
func eventCategoryOrder() []string {
	return model.AllEventCategories()
}

// EventCategories returns the event category tokens in canonical order: "transactions",
// "balances", "identities", "system".
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
// Parameters:
//   - eventType string: the event name, for example "transaction.applied".
//
// Returns:
//   - string: a fully-qualified, non-empty topic name.
func TopicForEvent(eventType string) string {
	return TopicForCategory(model.EventCategory(eventType))
}

// DLTFor returns the dead-letter sibling of a topic by appending DeadLetterTopicSuffix:
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
