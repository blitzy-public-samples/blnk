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

	"github.com/blnkfinance/blnk/model"
)

// This file holds the label and log-sanitisation primitives the event pipeline's
// observability shares. They sit here rather than beside either consumer because both the
// RECORDING path (recordConsumerLag in event_admin.go) and the BOOKKEEPING path
// (EventMetricsCollector in event_metrics.go) have to resolve a series' attributes the same
// way: a series is cleared by writing zero to the identical label tuple, so if the two
// resolved labels differently the collector would zero a series nobody ever published and
// leave the real one standing at its last reading for ever.

const (
	// maxLoggedErrorLength caps an error string from a dependency. Generous enough to keep
	// the diagnostic part of a real broker error — which leads with the useful text — and
	// short enough that no single line can dominate a log.
	maxLoggedErrorLength = 512

	// maxLoggedFilterLength caps a caller-supplied value echoed back into a log line.
	// Shorter than an error cap because these are identifiers and topic names, where
	// anything long is malformed input rather than detail.
	maxLoggedFilterLength = 128

	// logTruncationSuffix marks a value the cap shortened, so a truncated line is never
	// mistaken for a complete one.
	logTruncationSuffix = "…[truncated]"
)

const (
	// lagLabelUnattributed replaces a blank subscriber or group. A blank is a defect in the
	// registry row rather than a subscriber, and it must not share a series with a real one.
	lagLabelUnattributed = "unattributed"

	// lagLabelUnregistered replaces an identifier Blnk did not issue. Collapsing it is what
	// bounds the gauge's cardinality: an arbitrary string reaching an attribute would let
	// one malformed row mint a new time series on every tick.
	lagLabelUnregistered = "unregistered"

	// lagLabelOtherTopic replaces a topic outside the inventory Blnk owns. Measuring such a
	// topic is a legitimate reading, but its NAME comes from outside and must not become a
	// series of its own.
	lagLabelOtherTopic = "other"
)

// sanitizeLogValue makes an untrusted string safe to log: it strips the characters that let
// a value forge log structure, and it caps the length.
//
// Both halves matter. Newlines and carriage returns become spaces because a value carrying
// them SPLITS a line, and in a line-oriented log a forged newline followed by a plausible
// prefix is a fabricated entry — this pipeline logs values that arrive from a broker and
// from HTTP request parameters. Other control characters are removed because they corrupt
// terminals and confuse structured-log parsers. The length cap then bounds what remains.
//
// Truncation is marked rather than silent, so nobody reads a shortened broker error as the
// whole of it, and it happens on a RUNE boundary so a multi-byte character is never cut in
// half into invalid UTF-8.
//
// Parameters:
//   - value string: the untrusted text.
//   - max int: the maximum number of runes to keep. Values below 1 yield an empty string.
//
// Returns:
//   - string: the sanitized, bounded text.
func sanitizeLogValue(value string, max int) string {
	if value == "" || max < 1 {
		return ""
	}

	cleaned := strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		default:
			return r
		}
	}, value)

	cleaned = strings.TrimSpace(cleaned)

	runes := []rune(cleaned)
	if len(runes) <= max {
		return cleaned
	}

	return string(runes[:max]) + logTruncationSuffix
}

// isRegistrySubscriberIdentifier reports whether a subscriber business identifier is one the
// registry's canonical form admits.
//
// It delegates to model.CanonicalizeSubscriberIdentifier rather than restating the rule,
// because that function is the single definition the schema's CHECK constraints, the
// repository and the Kafka principal derivation all agree on. A second opinion here is
// exactly how a value could be measurable but unstorable, or vice versa.
//
// Parameters:
//   - identifier string: the raw identifier, already trimmed by the caller.
//
// Returns:
//   - bool: true when the identifier canonicalizes unchanged.
func isRegistrySubscriberIdentifier(identifier string) bool {
	canonical, err := model.CanonicalizeSubscriberIdentifier(identifier)

	return err == nil && canonical == identifier
}

// consumerGroupRoot reduces a runtime consumer group id to the subscriber-scoped root Blnk
// issued, and reports whether the group lies inside a namespace Blnk reserves at all.
//
// A subscriber's runtime group id legitimately EXTENDS its namespace: the provisioned ACL
// grants Read on the group with a prefixed pattern type, so a subscriber running three
// consumer instances may commit under 'blnk-sub-<id>.worker-1' and siblings. Those are one
// subscriber and must be ONE series — the alert says "this subscriber is behind", and
// splitting it per worker suffix would both multiply the series and let each split sit below
// the threshold while the subscriber as a whole was far past it. Reducing to the root also
// drops the suffix, which is the only part of the string the subscriber chose and therefore
// the only part that could carry a name.
//
// Parameters:
//   - group string: the raw, possibly suffixed consumer group id.
//
// Returns:
//   - string: 'blnk-sub-<identifier>', the subscriber-scoped root.
//   - bool: false when the group is not a proper leaf of a canonical namespace.
func consumerGroupRoot(group string) (string, bool) {
	if !strings.HasPrefix(group, model.SubscriberPrincipalNamespace) {
		return "", false
	}

	rest := group[len(model.SubscriberPrincipalNamespace):]

	terminator := strings.Index(rest, model.SubscriberGroupTerminator)
	if terminator <= 0 || terminator == len(rest)-len(model.SubscriberGroupTerminator) {
		return "", false
	}

	identifier := rest[:terminator]
	if !isRegistrySubscriberIdentifier(identifier) {
		return "", false
	}

	return model.SubscriberPrincipalNamespace + identifier, true
}

// isRegistryConsumerGroupID reports whether a consumer group id lies inside the namespace
// Blnk reserves for a canonical subscriber identifier.
//
// Parameters:
//   - group string: the raw consumer group id, already trimmed by the caller.
//
// Returns:
//   - bool: true when the group is a proper leaf of a canonical subscriber namespace.
func isRegistryConsumerGroupID(group string) bool {
	_, ok := consumerGroupRoot(group)

	return ok
}

// subscriberLagLabel resolves the 'subscriber' gauge attribute to a bounded, PSEUDONYMOUS
// value.
//
// # Why a tenant's own identifier must not be the label
//
// A metric label is the most widely readable thing this process emits. It is scraped into a
// time-series database, rendered on dashboards, quoted in alert notifications and forwarded to
// wherever those notifications go — a chat channel, an on-call phone, a paging vendor. A
// subscriber identifier is a TENANT NAME: "acme-payments-eu" on a lag alert discloses to
// everyone with dashboard access that Acme is a customer, roughly how much volume it consumes
// and when its integration is unhealthy. None of those readers was granted access to the
// ledger, and none of them needs the tenant's name to act.
//
// What monitoring actually needs from the label is that ONE SUBSCRIBER IS ONE SERIES: that
// its lag can be tracked over time, alerted on, and told apart from every other
// subscriber's. A stable hash gives exactly that. Correlating a series back to a tenant stays
// possible for whoever holds the registry — hash the identifier and match — which is the right
// place for that capability to live.
//
// # The two collapse tokens are NOT hashed
//
// "unattributed" and "unregistered" are classifications rather than identifiers: they say the
// measurement had no subject, or a subject the registry does not admit. Hashing them would
// turn two meaningful, greppable states into two opaque tokens and disclose nothing in
// exchange, since neither is anyone's name.
//
// # This function is the SINGLE resolver, and that is a correctness requirement
//
// The collector clears a stale series by writing zero to the identical label tuple. If the
// publish path and the clear path resolved a label differently, the clear would zero a tuple
// nobody published and leave the real series standing at its last reading for ever — an alert
// firing about a subscriber that no longer exists, which nothing could clear. Both paths call
// this.
//
// Parameters:
//   - subscriber string: the raw subscriber id.
//
// Returns:
//   - string: a stable pseudonymous token for a registry-admissible id, otherwise one of the
//     two collapse tokens.
func subscriberLagLabel(subscriber string) string {
	trimmed := strings.TrimSpace(subscriber)
	if trimmed == "" {
		return lagLabelUnattributed
	}

	if !isRegistrySubscriberIdentifier(trimmed) {
		return lagLabelUnregistered
	}

	return hashLogIdentifier(trimmed)
}

// consumerGroupLagLabel resolves the 'group' gauge attribute to a bounded value.
//
// Parameters:
//   - group string: the raw, possibly suffixed group id.
//
// Returns:
//   - string: the subscriber-scoped root, otherwise a collapse token.
func consumerGroupLagLabel(group string) string {
	trimmed := strings.TrimSpace(group)
	if trimmed == "" {
		return lagLabelUnattributed
	}

	if root, ok := consumerGroupRoot(trimmed); ok {
		// PSEUDONYMISED for the reason the subscriber label is: a consumer group id is derived
		// from the subscriber id, so publishing the group root publishes the tenant's name by
		// another route and would defeat hashing the subscriber label beside it. The root is
		// hashed rather than the raw group, so every group a subscriber runs still collapses to
		// ONE series — which is what bounds the cardinality — and that series is stable.
		return hashLogIdentifier(root)
	}

	return lagLabelUnregistered
}

// topicLagLabel resolves the 'topic' gauge attribute to a bounded value.
//
// The permitted set is the topics Blnk itself owns under the configured prefix — the four
// category topics and their four dead-letter siblings — the same closed set every other
// topic-attributed instrument in this pipeline uses.
//
// Parameters:
//   - topic string: the raw topic name.
//
// Returns:
//   - string: the topic when Blnk owns it, otherwise the collapse token.
func topicLagLabel(topic string) string {
	trimmed := strings.TrimSpace(topic)
	if trimmed == "" {
		return lagLabelOtherTopic
	}

	for _, owned := range AllTopicsWithDeadLetters() {
		if trimmed == owned {
			return trimmed
		}
	}

	return lagLabelOtherTopic
}
