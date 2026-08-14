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
	"strings"

	"github.com/google/uuid"
)

// eventTopicDLTSuffix is the dead-letter suffix, duplicated from the root package's
// topic-naming layer for the structural check below ONLY.
const eventTopicDLTSuffix = ".dlt"

// DefaultEventTopicPrefix is the topic namespace assumed when none is configured.
const DefaultEventTopicPrefix = "blnk"

// IsBlnkEventTopic reports whether topic is a topic BLNK OWNS under the given prefix:
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true when the name is a Blnk-owned category or dead-letter topic.
func IsBlnkEventTopic(topic, prefix string) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultEventTopicPrefix
	}

	// The dead-letter sibling of an owned category topic is itself owned, and
	// stripping the suffix first means the category check below is written once.
	base := strings.TrimSuffix(topic, eventTopicDLTSuffix)

	remainder, ok := strings.CutPrefix(base, prefix+".")
	if !ok || remainder == "" {
		return false
	}

	for _, known := range eventCategoryOrder {
		if remainder == known {
			return true
		}
	}

	return false
}

// IsBlnkEventTopicUnderAnyPrefix reports whether topic is a topic Blnk owns under ANY
// of the given prefixes.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefixes []string: the namespaces this deployment owns. Blank entries are
//     skipped.
//
// Returns:
//   - bool: true when the name is a Blnk-owned category or dead-letter topic under at
//     least one of the prefixes.
func IsBlnkEventTopicUnderAnyPrefix(topic string, prefixes []string) bool {
	matched := false
	for _, prefix := range prefixes {
		if strings.TrimSpace(prefix) == "" {
			continue
		}
		matched = true
		if IsBlnkEventTopic(topic, prefix) {
			return true
		}
	}

	if matched {
		return false
	}

	// No usable prefix was supplied. Delegating with a blank prefix resolves to
	// DefaultEventTopicPrefix inside IsBlnkEventTopic, so the two functions cannot
	// disagree about what an unconfigured caller owns.
	return IsBlnkEventTopic(topic, "")
}

// uuidCanonicalLength is the length of a canonical hyphenated UUID,
// "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
const uuidCanonicalLength = 36

// IsCanonicalUUID reports whether s is a UUID in canonical hyphenated form.
//
// Parameters:
//   - s string: the candidate identifier.
//
// Returns:
//   - bool: true only for the 36-character lowercase hyphenated form.
func IsCanonicalUUID(s string) bool {
	if len(s) != uuidCanonicalLength {
		return false
	}
	if s != strings.ToLower(s) {
		return false
	}
	if _, err := uuid.Parse(s); err != nil {
		return false
	}
	return true
}

// EventCategory* are BARE CATEGORY TOKENS, not topic names.
const (
	// EventCategoryTransactions covers all transaction lifecycle events,
	// including the runtime-composed bulk transaction events.
	EventCategoryTransactions = "transactions"
	// EventCategoryBalances covers balance creation and balance monitor alerts.
	EventCategoryBalances = "balances"
	// EventCategoryIdentities covers identity events.
	EventCategoryIdentities = "identities"

	// EventCategorySystem covers `ledger.created` and internal-error events, and it is
	// also the CATCH-ALL for an event type EventCategory does not recognise.
	EventCategorySystem = "system"
)

// SubscriberGrantableEventCategories returns the categories a subscriber may be
// authorised to consume, in a stable order.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, in canonical order.
func SubscriberGrantableEventCategories() []string {
	grantable := make([]string, 0, len(eventCategoryOrder))
	for _, category := range eventCategoryOrder {
		// The ONE exclusion, expressed as a filter over the canonical order rather than as a
		// second hand-written list. A second list would drift the day a category is added:
		if isSubscriberPrivilegedEventCategory(category) {
			continue
		}

		grantable = append(grantable, category)
	}

	return grantable
}

// SubscriberPrivilegedEventCategories returns the categories a subscriber may be
// authorised to consume ONLY under an explicit deployment-level acknowledgement, in
// canonical order.
//
// Returns:
//   - []string: a fresh slice of bare category tokens, in canonical order.
func SubscriberPrivilegedEventCategories() []string {
	privileged := make([]string, 0, len(eventCategoryOrder))
	for _, category := range eventCategoryOrder {
		if isSubscriberPrivilegedEventCategory(category) {
			privileged = append(privileged, category)
		}
	}

	return privileged
}

// isSubscriberPrivilegedEventCategory is the single membership test behind both lists,
// so a category cannot be absent from the default list and absent from the privileged
// one as well — which is how a category would become ungrantable by accident.
func isSubscriberPrivilegedEventCategory(category string) bool {
	return category == EventCategorySystem
}

// SubscriberGrantableTopics returns the fully-resolved topic names a subscriber may be
// authorised for under prefix, in the canonical category order.
//
// Parameters:
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names, safe for the caller to
//     retain or sort.
func SubscriberGrantableTopics(prefix string) []string {
	return composeCategoryTopics(prefix, SubscriberGrantableEventCategories())
}

// SubscriberPrivilegedTopics returns the topic names a subscriber may be authorised for
// ONLY in a deployment that has acknowledged the disclosure, in canonical order.
//
// Parameters:
//   - prefix string: the namespace this deployment owns, trimmed, defaulted as above.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names. Never nil.
func SubscriberPrivilegedTopics(prefix string) []string {
	return composeCategoryTopics(prefix, SubscriberPrivilegedEventCategories())
}

// SubscriberAuthorizableTopics returns every topic a subscriber may be authorised for
// in THIS deployment: the default grantable names, plus the privileged ones when the
// deployment has acknowledged them.
//
// Parameters:
//   - prefix string: the namespace this deployment owns, trimmed, defaulted as above.
//   - includePrivileged bool: whether the deployment has acknowledged the privileged
//     categories.
//
// Returns:
//   - []string: a fresh slice of `<prefix>.<category>` names, safe to retain or sort.
func SubscriberAuthorizableTopics(prefix string, includePrivileged bool) []string {
	topics := SubscriberGrantableTopics(prefix)
	if !includePrivileged {
		return topics
	}

	return append(topics, SubscriberPrivilegedTopics(prefix)...)
}

// composeCategoryTopics applies prefix to categories, and is the ONE place a category token
// becomes a topic name in this file, so the prefix defaulting cannot differ between the
// default list and the privileged one.
func composeCategoryTopics(prefix string, categories []string) []string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultEventTopicPrefix
	}

	topics := make([]string, 0, len(categories))
	for _, category := range categories {
		topics = append(topics, prefix+"."+category)
	}

	return topics
}

// IsSubscriberGrantableTopicName reports whether topic is one a subscriber may be
// granted Read and Describe on under prefix WITHOUT a deployment-level acknowledgement.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true only for an exact member of the default grantable list.
func IsSubscriberGrantableTopicName(topic, prefix string) bool {
	return IsSubscriberAuthorizableTopicName(topic, prefix, false)
}

// IsSubscriberPrivilegedTopicName reports whether topic is one of the names that
// require the deployment-level acknowledgement.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//
// Returns:
//   - bool: true only for an exact member of the privileged list.
func IsSubscriberPrivilegedTopicName(topic, prefix string) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	for _, privileged := range SubscriberPrivilegedTopics(prefix) {
		if topic == privileged {
			return true
		}
	}

	return false
}

// IsSubscriberAuthorizableTopicName is the membership test over
// SubscriberAuthorizableTopics: whether topic may be granted in a deployment whose
// acknowledgement state is includePrivileged.
//
// Parameters:
//   - topic string: the fully-resolved topic name to test.
//   - prefix string: the namespace this deployment owns.
//   - includePrivileged bool: the deployment's acknowledgement state.
//
// Returns:
//   - bool: true only for an exact member of the resolved list.
func IsSubscriberAuthorizableTopicName(topic, prefix string, includePrivileged bool) bool {
	if topic == "" || topic != strings.TrimSpace(topic) {
		return false
	}

	for _, authorizable := range SubscriberAuthorizableTopics(prefix, includePrivileged) {
		if topic == authorizable {
			return true
		}
	}

	return false
}

// eventCategoryOrder is the canonical order every category listing uses, so that topic
// inventories, reports and log lines are deterministic.
var eventCategoryOrder = [...]string{
	EventCategoryTransactions,
	EventCategoryBalances,
	EventCategoryIdentities,
	EventCategorySystem,
}

// AllEventCategories returns every category token, in canonical order.
//
// Returns:
//   - []string: a fresh slice; the caller may sort or filter it freely.
func AllEventCategories() []string {
	categories := make([]string, 0, len(eventCategoryOrder))
	categories = append(categories, eventCategoryOrder[:]...)

	return categories
}

// bulkTransactionEventPrefix is the fixed prefix of every bulk transaction event
// name. The full name is composed at runtime as this prefix plus the batch
// status, so the prefix — and never the whole string — is what can be matched.
const bulkTransactionEventPrefix = "bulk_transaction."

// eventTypeCategories is THE CATALOGUE: every event type Blnk emits under a fixed name,
// mapped to the category whose topic it is published to.
var eventTypeCategories = map[string]string{
	"transaction.queued":    EventCategoryTransactions,
	"transaction.applied":   EventCategoryTransactions,
	"transaction.scheduled": EventCategoryTransactions,
	"transaction.inflight":  EventCategoryTransactions,
	"transaction.void":      EventCategoryTransactions,
	"transaction.rejected":  EventCategoryTransactions,
	"transaction.unknown":   EventCategoryTransactions,
	"balance.created":       EventCategoryBalances,
	"balance.monitor":       EventCategoryBalances,
	"identity.created":      EventCategoryIdentities,
	// ledger.created and system.error share the fourth category, which is what the agreed
	// plan's topic table specifies. A subscriber therefore reaches ledger.created only
	// through the PRIVILEGED grant of `<prefix>.system` — the same grant that discloses
	// system.error's verbatim internal error text — and that cost is published in
	// docs/event-streaming.md rather than worked around by adding a category to the
	// contract. See EventCategorySystem and SubscriberPrivilegedEventCategories.
	"ledger.created": EventCategorySystem,
	// system.error renders Blnk's own error text verbatim, and this category is the
	// catalogue's catch-all, which together are why the category is privileged rather than
	// ordinary.
	"system.error": EventCategorySystem,
}

// EventKeyDimension names WHICH identifier an event type's Kafka message key is taken
// from, and it is a DECLARATION rather than a description of whatever the payload
// happened to yield.
type EventKeyDimension string

const (
	// EventKeyDimensionLedger is declared for every event type that describes ledger
	// state.
	EventKeyDimensionLedger EventKeyDimension = "ledger"

	// EventKeyDimensionAggregate is declared for event types that genuinely have no ledger
	// and whose own aggregate is the correct ordering unit. An identity is not scoped to a
	// ledger in this model and may be referenced by balances in several, so per-identity
	// ordering is the strongest guarantee that is true; a bulk batch is a runtime grouping
	// whose members may span ledgers, so the batch is its own unit and each member still
	// carries its own ledger-keyed transaction event.
	EventKeyDimensionAggregate EventKeyDimension = "aggregate"

	// EventKeyDimensionEvent is declared for event types with no aggregate of any kind, and
	// it means the key is THE EVENT'S OWN ID: every such event gets a distinct key and
	// therefore spreads across the topic's partitions.
	//
	// IT REPLACED A TYPE-WIDE KEY, and the replacement is the whole point. Keying on the
	// event TYPE put every event of that type on one partition, which reads as a virtue —
	// a total order over the stream — and is in practice a per-category throughput
	// ceiling that no amount of horizontal scaling can raise: one key is claimed by one
	// relay instance and published by one goroutine, one message at a time. A production
	// run measured that ceiling at 1.00 event per second while errors arrived far faster,
	// and the resulting backlog reached 307,787 rows — 59% of the whole outbox — where it
	// also became the oldest population every other key had to be claimed around.
	//
	// The total order it bought was not worth that, because it was not a MEANINGFUL order.
	// These events share no aggregate: two unrelated internal errors have no causal
	// relationship, so nothing about their relative position on a partition tells a
	// consumer anything it could act on. A consumer that wants them in time order sorts by
	// occurred_at, which is on the envelope and is the only ordering that was ever real.
	//
	// What a subscriber loses, stated plainly: events of these types arrive INTERLEAVED
	// across partitions and in no mutual order. What every other event type promises is
	// unchanged — a per-event key is used only where there is no aggregate to key on.
	EventKeyDimensionEvent EventKeyDimension = "event"
)

// eventKeyDimensions declares the key dimension of every catalogued event type.
var eventKeyDimensions = map[string]EventKeyDimension{
	"transaction.queued":    EventKeyDimensionLedger,
	"transaction.applied":   EventKeyDimensionLedger,
	"transaction.scheduled": EventKeyDimensionLedger,
	"transaction.inflight":  EventKeyDimensionLedger,
	"transaction.void":      EventKeyDimensionLedger,
	"transaction.rejected":  EventKeyDimensionLedger,
	"transaction.unknown":   EventKeyDimensionLedger,
	"balance.created":       EventKeyDimensionLedger,
	"balance.monitor":       EventKeyDimensionLedger,
	"ledger.created":        EventKeyDimensionLedger,
	"identity.created":      EventKeyDimensionAggregate,
	"system.error":          EventKeyDimensionEvent,
}

// KeyDimensionForEventType returns the declared key dimension of an event type.
//
// Parameters:
//   - eventType string: the event name, exactly as the producer passes it.
//
// Returns:
//   - EventKeyDimension: the declared dimension; never empty.
func KeyDimensionForEventType(eventType string) EventKeyDimension {
	// Prefix first, so it cannot be shadowed by the exact-match arm below.
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return EventKeyDimensionAggregate
	}

	if dimension, declared := eventKeyDimensions[eventType]; declared {
		return dimension
	}

	return EventKeyDimensionAggregate
}

// CataloguedEventTypes returns every event type this repository emits under a fixed
// name.
//
// Returns:
//   - []string: a fresh slice in no guaranteed order; the caller may sort it freely.
func CataloguedEventTypes() []string {
	catalogued := make([]string, 0, len(eventTypeCategories))
	for eventType := range eventTypeCategories {
		catalogued = append(catalogued, eventType)
	}

	return catalogued
}

// EventKeyDimensionsByType returns a copy of the declared key-dimension table.
//
// Returns:
//   - map[string]EventKeyDimension: a fresh map; the caller may mutate it freely.
func EventKeyDimensionsByType() map[string]EventKeyDimension {
	declared := make(map[string]EventKeyDimension, len(eventKeyDimensions))
	for eventType, dimension := range eventKeyDimensions {
		declared[eventType] = dimension
	}

	return declared
}

// IsCataloguedEventType reports whether an event type is one this repository is known
// to emit.
//
// Parameters:
//   - eventType string: the event name, for example "transaction.applied".
//
// Returns:
//   - bool: true for a named member of the catalogue and for any
//     bulk_transaction.<status>.
func IsCataloguedEventType(eventType string) bool {
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return true
	}

	_, catalogued := eventTypeCategories[eventType]

	return catalogued
}

// EventCategory resolves an event type to its category token. It is THE single
// event-type-to-category mapping table in the repository: the topic-naming layer
// delegates to it rather than keeping a second table, because two tables that drift
// produce a routing bug that is invisible until a subscriber notices missing events.
func EventCategory(eventType string) string {
	// Bulk transaction event names are composed at runtime as bulkTransactionEventPrefix +
	// status, so they must be matched by PREFIX and never by exact equality — the suffix
	// set is open. Three statuses are emitted today ("failed", "inflight" and "applied");
	// any status added later routes correctly with no change here.
	if strings.HasPrefix(eventType, bulkTransactionEventPrefix) {
		return EventCategoryTransactions
	}

	if category, catalogued := eventTypeCategories[eventType]; catalogued {
		return category
	}

	// THE CATCH-ALL. An event type this repository does not yet know about is published to
	// the system category rather than dropped.
	return EventCategorySystem
}

// Transaction lifecycle event names. These are the strings subscribers filter on, so
// each is a published contract and none may be renamed without a subscriber-visible
// migration.
const (
	EventTypeTransactionQueued    = "transaction.queued"
	EventTypeTransactionApplied   = "transaction.applied"
	EventTypeTransactionScheduled = "transaction.scheduled"
	EventTypeTransactionInflight  = "transaction.inflight"
	EventTypeTransactionVoid      = "transaction.void"
	EventTypeTransactionRejected  = "transaction.rejected"
	// EventTypeTransactionUnknown is the DEFENSIVE DEFAULT for a status this mapping has no
	// case for, and that is the whole of what it is. It is not the name of any transaction
	// outcome, and no code path in this repository produces it.
	//
	// It used to be described here as the name a COMMIT is announced under, which was
	// wrong: updateTransactionDetails normalises COMMIT to APPLIED before the event is
	// derived, so a committed inflight transaction is announced transaction.applied — over
	// Kafka today and over the HTTP webhook before it. The fall-through is real in this
	// function and pinned by a test so an unmapped status can never be dropped silently,
	// but reaching it requires a status no writer assigns. A consumer should tolerate the
	// name and must not wait for it. See "The COMMIT Status" in docs/event-streaming.md.
	EventTypeTransactionUnknown = "transaction.unknown"
)

// The two REPEATABLE event names, declared so the one place that has to distinguish them
// from the rest — EventTypeIsRepeatable, which decides whether an id may be derived —
// names them rather than spelling them again. They are the same published strings
// EventCategory routes on.
const (
	EventTypeBalanceMonitor = "balance.monitor"
	EventTypeSystemError    = "system.error"
)

// Transaction status literals, as assigned by the root package's Status* constants.
const (
	transactionStatusQueued    = "QUEUED"
	transactionStatusApplied   = "APPLIED"
	transactionStatusScheduled = "SCHEDULED"
	transactionStatusInflight  = "INFLIGHT"
	transactionStatusVoid      = "VOID"
	transactionStatusRejected  = "REJECTED"
)

// EventTypeForTransactionStatus maps a transaction status to its event name.
//
// Parameters:
//   - status string: the transaction status, in any casing.
//
// Returns:
//   - string: the event name; EventTypeTransactionUnknown for an unmapped status.
func EventTypeForTransactionStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case transactionStatusQueued:
		return EventTypeTransactionQueued
	case transactionStatusApplied:
		return EventTypeTransactionApplied
	case transactionStatusScheduled:
		return EventTypeTransactionScheduled
	case transactionStatusInflight:
		return EventTypeTransactionInflight
	case transactionStatusVoid:
		return EventTypeTransactionVoid
	case transactionStatusRejected:
		return EventTypeTransactionRejected
	default:
		// A status this mapping has no case for. Nothing in this repository reaches it —
		// COMMIT does not, because it is normalised to APPLIED before an event is derived
		// from it — so this arm exists so that a status added in future cannot be dropped
		// silently. See the note on EventTypeTransactionUnknown.
		return EventTypeTransactionUnknown
	}
}

// Subscriber identifier generation and topic-grant bounds.
const (
	// SubscriberIDPrefix is the module prefix of a generated subscriber business key,
	// following the repository's GenerateUUIDWithSuffix convention so a subscriber id is
	// recognisable at a glance in a log line or a broker ACL listing.
	SubscriberIDPrefix = "sub"

	// ConsumerGroupIDPrefix is the prefix every consumer group Blnk issues carries. It is
	// SubscriberPrincipalNamespace, because a subscriber's consumer group and its Kafka
	// principal are derived from the same identifier and share one namespace — the ACL
	// grant that reserves the group namespace is what makes them one boundary rather than
	// two.
	ConsumerGroupIDPrefix = SubscriberPrincipalNamespace

	// MaxSubscriberTopics bounds an authorised-topic grant.
	MaxSubscriberTopics = 16

	// MaxTopicNameLength is Kafka's own limit on the length of a topic name.
	MaxTopicNameLength = 249
)
