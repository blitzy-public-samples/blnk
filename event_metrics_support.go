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

	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/blnkfinance/blnk/model"
)

// This file holds the label and log-sanitisation primitives the event pipeline's
// observability shares. They sit here rather than beside either consumer because both
// the RECORDING path (recordConsumerLag in event_admin.go) and the BOOKKEEPING path
// (EventMetricsCollector in event_metrics.go) have to resolve a series' attributes the
// same way: a series is cleared by writing zero to the identical label tuple, so if the
// two resolved labels differently the collector would zero a series nobody ever
// published and leave the real one standing at its last reading for ever.

// These three are the package-local names for the caps and the truncation marker. They
// are ALIASES of the canonical values in internal/logsafe rather than second copies,
// because package api needs the identical bounds for the request log and a second
// declaration is how two log sinks come to disagree about how much of an error they
// keep. The names stay because more than a hundred call sites in this package read
// better with them.
const (
	// maxLoggedErrorLength caps an error string from a dependency. Generous enough to keep
	// the diagnostic part of a real broker error — which leads with the useful text — and
	// short enough that no single line can dominate a log.
	maxLoggedErrorLength = logsafe.MaxErrorLength

	// maxLoggedFilterLength caps a caller-supplied value echoed back into a log line.
	maxLoggedFilterLength = logsafe.MaxValueLength

	// logTruncationSuffix marks a value the cap shortened, so a truncated line is never
	// mistaken for a complete one.
	logTruncationSuffix = logsafe.TruncationSuffix
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

// sanitizeLogValue makes an untrusted string safe to log: it strips the characters that
// let a value forge log structure, and it caps the length.
func sanitizeLogValue(value string, max int) string {
	return logsafe.Value(value, max)
}

// redactLogValue is sanitizeLogValue with NETWORK TOPOLOGY REDACTED: the rendering for
// a dependency's own words when they arrive as a STRING rather than as an error.
func redactLogValue(value string, max int) string {
	return logsafe.RedactedValue(value, max)
}

// loggableCause renders an error for an operational log line at a normal level: control
// characters stripped, NETWORK TOPOLOGY REDACTED, and length bounded.
func loggableCause(err error) string {
	return logsafe.Cause(err)
}

// withLoggableCause attaches a dependency error to a log entry in the two renderings an
// operator needs, and it is the ONLY way this package should put an error into a line.
func withLoggableCause(entry *logrus.Entry, err error) *logrus.Entry {
	if entry == nil {
		entry = logrus.NewEntry(logrus.StandardLogger())
	}

	if err == nil {
		return entry
	}

	entry = entry.WithField("cause", logsafe.Cause(err))

	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		entry = entry.WithField("cause_verbatim", logsafe.CauseVerbatim(err))
	}

	return entry
}

// isRegistrySubscriberIdentifier reports whether a subscriber business identifier is
// one the registry's canonical form admits.
func isRegistrySubscriberIdentifier(identifier string) bool {
	canonical, err := model.CanonicalizeSubscriberIdentifier(identifier)

	return err == nil && canonical == identifier
}

// consumerGroupRoot reduces a runtime consumer group id to the subscriber-scoped root
// Blnk issued, and reports whether the group lies inside a namespace Blnk reserves at
// all.
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

// isRegistryConsumerGroupID reports whether a consumer group id lies inside the
// namespace Blnk reserves for a canonical subscriber identifier.
func isRegistryConsumerGroupID(group string) bool {
	_, ok := consumerGroupRoot(group)

	return ok
}

// subscriberLagLabel resolves the 'subscriber' gauge attribute to a bounded,
// PSEUDONYMOUS value.
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
func consumerGroupLagLabel(group string) string {
	trimmed := strings.TrimSpace(group)
	if trimmed == "" {
		return lagLabelUnattributed
	}

	if root, ok := consumerGroupRoot(trimmed); ok {
		// PSEUDONYMISED for the reason the subscriber label is: a consumer group id is
		// derived from the subscriber id, so publishing the group root publishes the tenant's
		// name by another route and would defeat hashing the subscriber label beside it. The
		// root is hashed rather than the raw group, so every group a subscriber runs still
		// collapses to ONE series — which is what bounds the cardinality — and that series is
		// stable.
		return hashLogIdentifier(root)
	}

	return lagLabelUnregistered
}

// ---------------------------------------------------------------------------------------
// a log line must carry the SAME pseudonym the metric carries

// subscriberLogLabel resolves a subscriber identifier to the pseudonym a LOG FIELD
// carries.
func subscriberLogLabel(subscriber string) string {
	trimmed := strings.TrimSpace(subscriber)
	if trimmed == "" {
		return lagLabelUnattributed
	}

	return hashLogIdentifier(trimmed)
}

// consumerGroupLogLabel resolves a consumer group identifier to the pseudonym a LOG
// FIELD carries.
func consumerGroupLogLabel(group string) string {
	trimmed := strings.TrimSpace(group)
	if trimmed == "" {
		return lagLabelUnattributed
	}

	if root, ok := consumerGroupRoot(trimmed); ok {
		return hashLogIdentifier(root)
	}

	return hashLogIdentifier(trimmed)
}

// topicLagLabel resolves the 'topic' gauge attribute to a bounded value.
func topicLagLabel(topic string) string {
	trimmed := strings.TrimSpace(topic)
	if trimmed == "" {
		return lagLabelOtherTopic
	}

	// Across every owned prefix. A subscriber authorised before a topic-prefix rename is
	// still consuming the previous generation's topic while its rows drain, and collapsing
	// that name would hide exactly the lag an operator managing the migration needs to
	// see. The set stays bounded because the historical allowlist is bounded by
	// config.MaxHistoricalTopicPrefixes.
	for _, owned := range AllOwnedTopicsAcrossPrefixes() {
		if trimmed == owned {
			return trimmed
		}
	}

	return lagLabelOtherTopic
}
