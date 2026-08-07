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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"gopkg.in/yaml.v3"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// This file covers the OBSERVABILITY CONTRACT of the event pipeline: the values that
// leave the process as metric attribute values, and the values that leave it as log
// fields. Both are published interfaces even though neither appears in a Go signature,
// and both fail in ways a build cannot catch.
//
// A metric attribute value that is not in its declared domain does not error — it mints a
// new series. On the publish-duration histogram, where cardinality is multiplied by the
// bucket count, that is how one instrument comes to dominate an exporter, and it happens
// silently. A log field carrying an unbounded external string, a forgeable newline, or a
// financial identifier does not error either; it just leaves the process.
//
// So what is asserted here is exactly what those two contracts promise: the attempt label
// domain is closed at eight values whatever the input, the outcome of an attempt is a true
// statement about whether anything further will be tried, and log fields are bounded,
// structurally safe and free of plaintext ledger identifiers.

// ---------------------------------------------------------------------------
// The attempt label: a closed domain
// ---------------------------------------------------------------------------

// TestAttemptLabel_DomainIsClosedAtEightValues drives every reachable input through
// attemptLabel and asserts the output is always one of the eight declared values.
//
// The inputs deliberately include ones that "cannot happen" through configuration, because
// the label domain must hold for inputs that do not come from configuration at all: a row
// whose max_attempts was raised directly in the database reaches the publisher with an
// attempt number configuration never sanctioned, and it must collapse rather than extend
// the domain.
func TestAttemptLabel_DomainIsClosedAtEightValues(t *testing.T) {
	domain := map[string]struct{}{
		"1": {}, "2": {}, "3": {}, "4": {}, "5": {},
		publishAttemptLabelOverflow:   {},
		publishAttemptLabelReplay:     {},
		publishAttemptLabelDeadLetter: {},
	}
	require.Len(t, domain, 8, "the declared attempt domain is eight values")

	purposes := []PublishPurpose{
		PublishPurposeOriginal,
		PublishPurposeReplay,
		PublishPurposeDeadLetter,
	}
	attempts := []int{-7, 0, 1, 2, 3, 4, 5, 6, 99, 5000}

	for _, purpose := range purposes {
		for _, attempt := range attempts {
			label := attemptLabel(purpose, attempt)
			_, ok := domain[label]
			assert.True(t, ok,
				"attemptLabel(%q, %d) produced %q, which is outside the declared domain; "+
					"a value outside it mints a new histogram series per bucket",
				purpose, attempt, label)
		}
	}
}

// TestAttemptLabel_NumbersTheBudgetAndCollapsesEverythingAbove pins the exact mapping for
// an original publish.
//
// The numeric part is what the latency target selects on — attempt="1" is
// first-attempt-only — so each attempt inside the budget must render as its own number.
// Above the budget there is deliberately no number at all: a row whose max_attempts was
// raised in the database would otherwise widen the domain one value at a time.
func TestAttemptLabel_NumbersTheBudgetAndCollapsesEverythingAbove(t *testing.T) {
	for attempt := 1; attempt <= config.MaxRelayRetryAttempts; attempt++ {
		assert.Equal(t, strconv.Itoa(attempt), attemptLabel(PublishPurposeOriginal, attempt),
			"attempt %d is inside the budget and must render as its own number", attempt)
	}

	for _, attempt := range []int{config.MaxRelayRetryAttempts + 1, config.MaxRelayRetryAttempts + 40, 5000} {
		assert.Equal(t, publishAttemptLabelOverflow, attemptLabel(PublishPurposeOriginal, attempt),
			"attempt %d is beyond the supported budget and must collapse into one bucket", attempt)
	}

	// Below 1 is a caller mistake, not a distinct label: admitting "0" and negatives would
	// add label values that describe nothing.
	assert.Equal(t, "1", attemptLabel(PublishPurposeOriginal, 0))
	assert.Equal(t, "1", attemptLabel(PublishPurposeOriginal, -3))
}

// TestAttemptLabel_ReplayAndDeadLetterIgnoreTheAttemptNumberEntirely is the assertion that
// keeps the two non-retry publishes out of the latency population.
//
// Neither is part of a retry sequence, so neither may carry a number: the fixed token has
// to win over whatever attempt value the caller passed, or a replay submitted with
// attempt=1 would land in the attempt="1" series the p99 target is read from — the exact
// contamination the purpose exists to prevent.
func TestAttemptLabel_ReplayAndDeadLetterIgnoreTheAttemptNumberEntirely(t *testing.T) {
	for _, attempt := range []int{-1, 0, 1, 3, 5, 6, 900} {
		assert.Equal(t, publishAttemptLabelReplay, attemptLabel(PublishPurposeReplay, attempt),
			"a replay is always labelled %q whatever attempt number accompanies it", publishAttemptLabelReplay)
		assert.Equal(t, publishAttemptLabelDeadLetter, attemptLabel(PublishPurposeDeadLetter, attempt),
			"a dead-letter write is always labelled %q", publishAttemptLabelDeadLetter)
	}
}

// TestResolvePurpose_DefaultsAndRefusesToBeExtended pins the purpose normalisation.
//
// The purpose selects a bounded metric attribute value, so an unrecognised one must NOT be
// passed through: doing so would reopen the domain that PublishPurpose exists to close.
// Treating it as an original publish is the conservative choice — it is what the mandated
// envelope-only Publish method submits.
//
// The two unrecognised cases are NOT the same, and the log has to tell them apart. An
// UNSTATED purpose is the documented default on the most ordinary path there is, so it must
// pass silently; a warning there would fire on every envelope-only publish, and a warning
// that fires constantly on correct behaviour teaches an operator to filter the level out,
// taking the real warnings with it. A NON-EMPTY unknown value is a caller that got the
// vocabulary wrong and is worth exactly one line.
func TestResolvePurpose_DefaultsAndRefusesToBeExtended(t *testing.T) {
	assert.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{}),
		"an unstated purpose is a first delivery, which is what the zero-valued request means")
	assert.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{Purpose: PublishPurposeOriginal}))
	assert.Equal(t, PublishPurposeReplay, resolvePurpose(PublishRequest{Purpose: PublishPurposeReplay}))
	assert.Equal(t, PublishPurposeDeadLetter, resolvePurpose(PublishRequest{Purpose: PublishPurposeDeadLetter}))

	assert.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{Purpose: PublishPurpose("tenant-acme")}),
		"an unrecognised purpose must collapse to a declared value rather than becoming a new label")

	t.Run("the unstated default is silent", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		require.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{}))
		require.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{Purpose: PublishPurposeOriginal}))

		assert.Empty(t, hook.AllEntries(),
			"the documented default must not log; every envelope-only publish takes this path")
	})

	t.Run("a non-empty unknown purpose is reported once, with the value bounded", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		forged := "tenant-acme\nlevel=error msg=\"forged\"" + strings.Repeat("q", 400)
		require.Equal(t, PublishPurposeOriginal, resolvePurpose(PublishRequest{Purpose: PublishPurpose(forged)}))

		entries := hook.AllEntries()
		require.Len(t, entries, 1, "one mistake is worth one line, not none and not several")
		assert.Equal(t, logrus.WarnLevel, entries[0].Level)

		logged, ok := entries[0].Data["purpose"].(string)
		require.True(t, ok, "the line must name the value that was rejected, or it is unactionable")
		assert.NotContains(t, logged, "\n", "the rejected value must not be able to forge a log entry")
		assert.LessOrEqual(t, len([]rune(logged)), maxLoggedFilterLength+len([]rune(logTruncationSuffix)))
	})
}

// TestResolveMaxAttempts_TreatsAnUnstatedBudgetAsUnknown pins the difference between "no
// budget was stated" and "a budget of zero attempts".
//
// The distinction decides whether a transient failure reports retrying or failed, so
// conflating them would make every envelope-only publish report its first transient
// failure as terminal.
func TestResolveMaxAttempts_TreatsAnUnstatedBudgetAsUnknown(t *testing.T) {
	assert.Zero(t, resolveMaxAttempts(PublishRequest{}), "an unset budget is unknown, not zero attempts")
	assert.Zero(t, resolveMaxAttempts(PublishRequest{MaxAttempts: 0}))
	assert.Zero(t, resolveMaxAttempts(PublishRequest{MaxAttempts: -4}), "a negative budget is a mistake, read as unstated")
	assert.Equal(t, 1, resolveMaxAttempts(PublishRequest{MaxAttempts: 1}))
	assert.Equal(t, 5, resolveMaxAttempts(PublishRequest{MaxAttempts: 5}))
}

// TestAttemptBudgetSpent_OnlyDecidesWhenABudgetWasStated is the rule that separates a
// retryable failure from a terminal one.
func TestAttemptBudgetSpent_OnlyDecidesWhenABudgetWasStated(t *testing.T) {
	assert.False(t, attemptBudgetSpent(1, 0), "with no stated budget nothing about the attempt count is terminal")
	assert.False(t, attemptBudgetSpent(99, 0), "an unstated budget stays unstated however many attempts have been made")
	assert.False(t, attemptBudgetSpent(1, 5))
	assert.False(t, attemptBudgetSpent(4, 5))
	assert.True(t, attemptBudgetSpent(5, 5), "the attempt that reaches the budget is the last one")
	assert.True(t, attemptBudgetSpent(6, 5), "past the budget is spent too, not wrapped around")
	assert.True(t, attemptBudgetSpent(1, 1), "a budget of one is spent by the first attempt")
}

// ---------------------------------------------------------------------------
// Log fields: bounded, structurally safe, and free of plaintext identifiers
// ---------------------------------------------------------------------------

// TestPublishResultLogFields_CarriesTheAttemptAndItsBudgetOnEveryAttempt is requirement
// R-4 expressed as a test.
//
// R-4 requires the attempt count and the error reason on EVERY attempt rather than only on
// the final failure. An attempt count without its budget is not actionable — "attempt 3"
// against a budget of 3 and against a budget of 5 are opposite situations — so both are
// asserted, together with the outcome that says whether anything further will be tried.
func TestPublishResultLogFields_CarriesTheAttemptAndItsBudgetOnEveryAttempt(t *testing.T) {
	result := PublishResult{
		Status:      model.PublishStatusRetrying,
		EventID:     "3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		EventType:   "transaction.applied",
		Topic:       "blnk.transactions",
		Attempt:     3,
		MaxAttempts: 5,
		Transient:   true,
		Retryable:   true,
		Err:         errors.New("dial tcp 10.0.0.7:9092: connect: connection refused"),
	}

	fields := result.LogFields()

	assert.Equal(t, 3, fields["attempt"], "the attempt number must be on every attempt's line")
	assert.Equal(t, 5, fields["max_attempts"], "the budget must accompany the attempt number")
	assert.Equal(t, "retrying", fields["status"])
	assert.Equal(t, true, fields["retryable"])
	assert.Equal(t, true, fields["transient"])
	assert.Equal(t, result.EventID, fields["event_id"])
	assert.Equal(t, "blnk.transactions", fields["topic"])
	require.Contains(t, fields, "error", "the error reason must be present on a failed attempt")
	assert.Contains(t, fields["error"], "connection refused")
}

// TestPublishResultLogFields_OmitsAnUnstatedBudgetRatherThanLoggingZero keeps the log
// honest about what it does not know.
//
// A field reading max_attempts=0 says "the budget is zero attempts", which is a different
// and wrong statement. Omitting it is what leaves "no budget was stated" readable.
func TestPublishResultLogFields_OmitsAnUnstatedBudgetRatherThanLoggingZero(t *testing.T) {
	fields := PublishResult{
		Status:    model.PublishStatusDispatched,
		EventID:   "9c4d2a1e-1111-2222-3333-444455556666",
		EventType: "ledger.created",
		Topic:     "blnk.system",
		Attempt:   1,
	}.LogFields()

	assert.NotContains(t, fields, "max_attempts", "an unstated budget must be absent, not zero")
	assert.NotContains(t, fields, "error", "a successful attempt must not log an empty error reason")
	assert.NotContains(t, fields, "purpose", "an original publish is the default and needs no purpose field")
}

// TestPublishResultLogFields_HashesThePartitionKeyRatherThanPrintingIt is the
// data-protection assertion.
//
// The partition key is the LEDGER ID: a financial identifier naming the account whose
// events these are. Logs are routinely shipped to an aggregator with a weaker access
// boundary than the ledger, and this line is emitted once per attempt, so the plaintext
// would be exported at event rate.
//
// The hash keeps the one property a log needs — stability, so two lines can be recognised
// as the same ledger and therefore the same ordering guarantee — and gives up the
// identifier. An EMPTY key must stay empty rather than becoming the digest of the empty
// string: a keyless message was spread across partitions instead of pinned to one, which
// is exactly what someone investigating an ordering complaint needs to see.
func TestPublishResultLogFields_HashesThePartitionKeyRatherThanPrintingIt(t *testing.T) {
	const ledgerID = "ldg_8a1f0c1d-2e3b-4f21-9c8a-6d3c6b0e3f1d"

	fields := PublishResult{
		Status:       model.PublishStatusDispatched,
		EventID:      "3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		EventType:    "transaction.applied",
		Topic:        "blnk.transactions",
		PartitionKey: ledgerID,
		Attempt:      1,
	}.LogFields()

	assert.NotContains(t, fields, "partition_key", "the plaintext field must be gone, not merely renamed alongside it")
	hashed, ok := fields["partition_key_hash"].(string)
	require.True(t, ok, "the hashed partition key must be present as a string")
	assert.NotEqual(t, ledgerID, hashed)
	assert.NotContains(t, hashed, "ldg_", "no part of the identifier may survive")
	assert.Len(t, hashed, logIdentifierHashLength)

	// Stability is the property that makes the token useful at all.
	assert.Equal(t, hashed, hashLogIdentifier(ledgerID), "the same key must always hash to the same token")
	assert.NotEqual(t, hashed, hashLogIdentifier(ledgerID+"x"), "different keys must not collide")

	empty := PublishResult{Status: model.PublishStatusDispatched, Attempt: 1}.LogFields()
	assert.Equal(t, "", empty["partition_key_hash"],
		"a keyless message must stay visibly keyless: it was balanced across partitions, not pinned to one")
}

// TestSanitizeLogValue_BoundsLengthAndStripsLineForgery covers both halves of the log
// sanitizer.
//
// The length cap exists because the text is not ours: a broker error can carry a
// per-partition enumeration running to kilobytes, emitted once per attempt per event,
// which is how a log pipeline gets throttled for being over quota. The control-character
// strip exists because a value containing a newline SPLITS a line — in a line-oriented log
// a forged newline followed by a plausible prefix is a fabricated entry, and these values
// arrive from a broker and from HTTP request parameters.
func TestSanitizeLogValue_BoundsLengthAndStripsLineForgery(t *testing.T) {
	t.Run("a long value is truncated and marked", func(t *testing.T) {
		long := strings.Repeat("b", maxLoggedErrorLength*3)
		got := sanitizeLogValue(long, maxLoggedErrorLength)

		assert.True(t, strings.HasSuffix(got, logTruncationSuffix),
			"truncation must be marked so a shortened broker error is not read as the whole of it")
		assert.Equal(t, maxLoggedErrorLength+len([]rune(logTruncationSuffix)), len([]rune(got)))
	})

	t.Run("a value at the cap is untouched and unmarked", func(t *testing.T) {
		exact := strings.Repeat("c", maxLoggedErrorLength)
		assert.Equal(t, exact, sanitizeLogValue(exact, maxLoggedErrorLength))
	})

	t.Run("newlines and carriage returns cannot forge a log line", func(t *testing.T) {
		got := sanitizeLogValue("broker said no\nlevel=info msg=\"all clear\"\r\n", maxLoggedErrorLength)

		assert.NotContains(t, got, "\n", "a newline would split the line and fabricate a second entry")
		assert.NotContains(t, got, "\r")
		assert.Contains(t, got, "broker said no", "the real text must survive the sanitising")
	})

	t.Run("other control characters are removed entirely", func(t *testing.T) {
		got := sanitizeLogValue("topic\x00name\x1b[31m\x7f", maxLoggedErrorLength)

		assert.Equal(t, "topicname[31m", got,
			"NUL, ESC and DEL corrupt terminals and structured-log parsers and must not be forwarded")
	})

	t.Run("an empty value and a non-positive cap yield an empty string", func(t *testing.T) {
		assert.Equal(t, "", sanitizeLogValue("", maxLoggedErrorLength))
		assert.Equal(t, "", sanitizeLogValue("anything", 0))
		assert.Equal(t, "", sanitizeLogValue("anything", -1))
	})

	t.Run("truncation happens on a rune boundary", func(t *testing.T) {
		// Multi-byte runes: a byte-wise cut would leave half a character behind and
		// produce invalid UTF-8 in the log stream.
		got := sanitizeLogValue(strings.Repeat("é", 40), 10)

		assert.True(t, strings.HasPrefix(got, strings.Repeat("é", 10)))
		assert.Equal(t, 10+len([]rune(logTruncationSuffix)), len([]rune(got)))
	})
}

// TestLagLabelResolvers_BoundTheGaugeCardinalityAndStayClearable covers the three
// resolvers every consumer-lag attribute is routed through.
//
// They exist for two reasons that both have to hold at once, and a test that only checked
// the happy path would notice neither.
//
// The first is CARDINALITY. subscriber, group and topic all arrive from registry rows,
// which are caller-authored. An unresolved value would mint a new Prometheus time series
// per distinct string, on every collection cycle, from data an API client controls.
//
// The second is CLEARABILITY. The collector zeroes a stale series by writing to the
// identical label tuple, so the resolvers are the shared vocabulary between the recording
// path and the clearing path. A collapse token is therefore not merely a tidy default: it
// is a real, writable series that a stale non-conforming subscriber can be zeroed on.
func TestLagLabelResolvers_BoundTheGaugeCardinalityAndStayClearable(t *testing.T) {
	t.Run("a generated subscriber identifier becomes a stable pseudonym", func(t *testing.T) {
		identifier := model.GenerateSubscriberID()
		label := subscriberLagLabel(identifier)

		// PSEUDONYMOUS, not the identifier. A metric label is scraped into a time-series
		// database, rendered on dashboards and quoted into alert notifications, none of which
		// carries access control, so the registry identifier is withheld and a stable hash of
		// it published instead. One subscriber is still exactly one series, which is all
		// monitoring needs; correlating a series back to a tenant stays possible for whoever
		// holds the registry.
		assert.NotEqual(t, identifier, label,
			"the registry identifier must not be published as a label value")
		assert.NotContains(t, label, identifier)
		assert.NotEmpty(t, label, "a resolvable subscriber must still produce a series")
		assert.NotEqual(t, lagLabelUnattributed, label,
			"a registry-issued identifier is attributable, so it must not collapse")
		assert.NotEqual(t, lagLabelUnregistered, label,
			"and it must not read as one the registry could not have issued")

		// STABLE, which is the property alerting depends on: the same subscriber must land on
		// the same series across collection cycles, or its lag history is a new series each
		// time and no `for:` duration can ever elapse.
		assert.Equal(t, label, subscriberLagLabel(identifier),
			"the same identifier must always resolve to the same label")
		assert.Equal(t, label, subscriberLagLabel("  "+identifier+"  "),
			"surrounding whitespace is not a different subscriber")

		// DISTINCT, so two subscribers are two series rather than one aggregate.
		assert.NotEqual(t, label, subscriberLagLabel(model.GenerateSubscriberID()),
			"two subscribers must not share a series")
	})

	t.Run("an absent subscriber is attributed to no one, not to the empty string", func(t *testing.T) {
		assert.Equal(t, lagLabelUnattributed, subscriberLagLabel(""))
		assert.Equal(t, lagLabelUnattributed, subscriberLagLabel("   "))
	})

	t.Run("a subscriber identifier the registry could not have issued collapses", func(t *testing.T) {
		for _, hostile := range []string{
			"ACME-Corp",                       // not canonical: uppercase
			"sub_" + strings.Repeat("x", 400), // over the length bound
			"sub with spaces",
			"sub/../../etc/passwd",
			`sub",group="x`, // an attempt to break out of the label tuple itself
		} {
			assert.Equal(t, lagLabelUnregistered, subscriberLagLabel(hostile),
				"%q must not reach the gauge as a label value", hostile)
		}
	})

	t.Run("a derived group collapses to its subscriber-scoped root", func(t *testing.T) {
		identifier := model.GenerateSubscriberID()
		group, err := model.CanonicalConsumerGroupID(identifier)
		require.NoError(t, err)

		root := model.SubscriberPrincipalNamespace + identifier
		label := consumerGroupLagLabel(group)

		// The root is collapsed to FIRST and then hashed. Publishing the root itself would
		// publish the subscriber identifier by another route, since the group id is derived
		// from it, and would defeat pseudonymising the subscriber label beside it.
		assert.NotContains(t, label, identifier,
			"the group label must not carry the subscriber identifier it is derived from")
		assert.NotEqual(t, root, label)
		assert.NotEmpty(t, label)

		// Every group inside one subscriber's namespace resolves to the SAME series. This
		// is the property that keeps a subscriber running many consumer groups from
		// multiplying the alert into one firing instance per group.
		for _, leaf := range []string{"default", "recon", "replay-2026-03", "a"} {
			assert.Equal(t, label,
				consumerGroupLagLabel(root+model.SubscriberGroupTerminator+leaf),
				"leaf %q must not open a new series", leaf)
		}

		// And two subscribers' namespaces must not collide onto one series.
		other := model.SubscriberPrincipalNamespace + model.GenerateSubscriberID()
		assert.NotEqual(t, label,
			consumerGroupLagLabel(other+model.SubscriberGroupTerminator+"default"),
			"two subscribers' group namespaces must be two series")
	})

	t.Run("a group outside the derived shape collapses", func(t *testing.T) {
		assert.Equal(t, lagLabelUnattributed, consumerGroupLagLabel(""))
		assert.Equal(t, lagLabelUnattributed, consumerGroupLagLabel("  "))

		for _, hostile := range []string{
			"acme-recon-group",  // a plausible group an operator might pass by hand
			"blnk-sub-",         // the namespace with nothing in it
			"blnk-sub-.default", // an empty identifier
			"blnk-sub-sub_x.",   // a terminator with no leaf
			"blnk-sub-NOTCANONICAL.default",
			"prefix-blnk-sub-sub_x.default", // the namespace must be a PREFIX, not a substring
		} {
			assert.Equal(t, lagLabelUnregistered, consumerGroupLagLabel(hostile),
				"%q must not reach the gauge as a label value", hostile)
		}
	})

	t.Run("only a topic Blnk owns passes through", func(t *testing.T) {
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		owned := AllTopicsWithDeadLetters()
		require.NotEmpty(t, owned)
		for _, topic := range owned {
			assert.Equal(t, topic, topicLagLabel(topic),
				"a topic in the owned inventory is the label")
		}

		for _, foreign := range []string{
			"", "   ",
			"blnk.transactions.retry", // a subscriber-side convention, not ours
			"customer.events",
			"gen1.transactions", // a previous prefix generation
			"BLNK.TRANSACTIONS",
		} {
			assert.Equal(t, lagLabelOtherTopic, topicLagLabel(foreign),
				"%q is not in the owned inventory", foreign)
		}
	})

	t.Run("the resolved label space is bounded no matter what is fed in", func(t *testing.T) {
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		// The bound is what makes this safe to expose to caller-authored registry rows:
		// 2,000 distinct hostile inputs must not produce 2,000 series.
		subscribers := map[string]struct{}{}
		groups := map[string]struct{}{}
		topics := map[string]struct{}{}
		for i := 0; i < 1_000; i++ {
			subscribers[subscriberLagLabel(fmt.Sprintf("Tenant-%d", i))] = struct{}{}
			groups[consumerGroupLagLabel(fmt.Sprintf("group-%d", i))] = struct{}{}
			topics[topicLagLabel(fmt.Sprintf("topic.%d", i))] = struct{}{}
		}

		assert.Len(t, subscribers, 1, "every non-conforming subscriber shares one series")
		assert.Len(t, groups, 1, "every non-conforming group shares one series")
		assert.Len(t, topics, 1, "every unowned topic shares one series")
	})
}

// TestPublishResultLogFields_TruncatesADependencyErrorString ties the sanitizer to the
// place it actually protects.
func TestPublishResultLogFields_TruncatesADependencyErrorString(t *testing.T) {
	fields := PublishResult{
		Status:      model.PublishStatusDeadLettered,
		EventID:     "3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		EventType:   "transaction.applied",
		Topic:       "blnk.transactions",
		Attempt:     5,
		MaxAttempts: 5,
		Err:         errors.New(strings.Repeat("partition unavailable; ", 400)),
	}.LogFields()

	message, ok := fields["error"].(string)
	require.True(t, ok, "the error field must be a string")
	assert.LessOrEqual(t, len([]rune(message)), maxLoggedErrorLength+len([]rune(logTruncationSuffix)),
		"an unbounded dependency error must not reach the log at full length")
	assert.True(t, strings.HasSuffix(message, logTruncationSuffix))
}

// TestPublishResultLogFields_NamesANonDefaultPurpose keeps a replay's log line
// self-describing.
func TestPublishResultLogFields_NamesANonDefaultPurpose(t *testing.T) {
	fields := PublishResult{
		Status:    model.PublishStatusDispatched,
		EventID:   "3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		EventType: "transaction.applied",
		Topic:     "blnk.transactions",
		Attempt:   1,
		Purpose:   PublishPurposeReplay,
	}.LogFields()

	assert.Equal(t, "replay", fields["purpose"],
		"a replay's line must say so, or it reads as an ordinary first delivery")
}

// TestPublisherAuthMode_NamesTheMechanismAndNothingElse is the second half of the
// start-up log-disclosure fix.
//
// The mechanism is worth logging: a cluster with the KRaft standard authorizer enabled
// rejects an unauthenticated producer, so "none" against such a cluster explains every
// subsequent failure. The administrative USERNAME is not included even though it is not a
// secret — paired with the mechanism it is half a credential and names a valid principal,
// which is a starting point for a brute-force attempt that a mode name is not.
func TestPublisherAuthMode_NamesTheMechanismAndNothingElse(t *testing.T) {
	const adminUser = "blnk-test-admin"

	assert.Equal(t, "none", publisherAuthMode(config.KafkaConfig{}))
	assert.Equal(t, "none", publisherAuthMode(config.KafkaConfig{SASLAdminUser: "   "}),
		"a whitespace-only username is not a configured credential")

	mode := publisherAuthMode(config.KafkaConfig{
		SASLAdminUser:   adminUser,
		SASLAdminSecret: "placeholder-not-a-real-secret",
	})
	assert.Equal(t, "scram-sha-512", mode)
	assert.NotContains(t, mode, adminUser, "the principal name must not travel in the mode")
	assert.NotContains(t, mode, "placeholder", "no part of the secret may travel in the mode")
}

// ---------------------------------------------------------------------------
// The periodic collector: the one production maintainer of the three gauges
// ---------------------------------------------------------------------------

// collectorFakeOutbox answers the backlog query.
type collectorFakeOutbox struct {
	mu sync.Mutex

	counts map[string]int64
	err    error
	calls  int
}

func newCollectorFakeOutbox() *collectorFakeOutbox {
	return &collectorFakeOutbox{counts: map[string]int64{}}
}

// CountEventOutboxByStatus reproduces the repository's GROUP BY semantics exactly: a status
// with no rows is ABSENT from the map rather than present with a zero. That is what makes
// the collector's two-value reads mandatory, so a fake that returned zeros would let a
// buggy single-value read pass.
func (o *collectorFakeOutbox) CountEventOutboxByStatus(_ context.Context) (map[string]int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.calls++
	if o.err != nil {
		return nil, o.err
	}

	out := make(map[string]int64, len(o.counts))
	for status, count := range o.counts {
		if count > 0 {
			out[status] = count
		}
	}

	return out, nil
}

func (o *collectorFakeOutbox) set(status string, count int64) *collectorFakeOutbox {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.counts[status] = count

	return o
}

func (o *collectorFakeOutbox) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.calls
}

// collectorFakeRegistry pages a fixed subscriber list.
type collectorFakeRegistry struct {
	mu sync.Mutex

	rows  []model.EventSubscriber
	err   error
	pages []collectorPage
}

// collectorPage is one recorded enumeration request, so the paging and the budget can be
// asserted on the requests themselves rather than inferred from the results.
type collectorPage struct {
	limit  int
	offset int
}

func (r *collectorFakeRegistry) ListEventSubscribers(
	_ context.Context,
	limit, offset int,
) ([]model.EventSubscriber, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pages = append(r.pages, collectorPage{limit: limit, offset: offset})

	if r.err != nil {
		return nil, r.err
	}
	if offset >= len(r.rows) {
		return nil, nil
	}

	end := offset + limit
	if end > len(r.rows) {
		end = len(r.rows)
	}

	page := make([]model.EventSubscriber, end-offset)
	copy(page, r.rows[offset:end])

	return page, nil
}

func (r *collectorFakeRegistry) snapshotPages() []collectorPage {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]collectorPage, len(r.pages))
	copy(out, r.pages)

	return out
}

// collectorFakeAdmin measures lag without a broker, recording every request and returning a
// report the collector then projects through the REAL ConsumerLagReport.LagSamples — so the
// label resolution and the withholding rule under test are the production ones.
type collectorFakeAdmin struct {
	mu sync.Mutex

	configured bool
	lagByTopic map[string]int64

	// unavailableByTopic makes a topic's measurement INCOMPLETE: that many of its
	// partitions could not be read, so its lag is a lower bound and must be withheld from
	// the gauge the alert reads. See metrics.ConsumerLagUnmeasuredPartitions.
	unavailableByTopic map[string]int

	err      error
	requests []ConsumerLagRequest
}

func newCollectorFakeAdmin() *collectorFakeAdmin {
	return &collectorFakeAdmin{
		configured:         true,
		lagByTopic:         map[string]int64{},
		unavailableByTopic: map[string]int{},
	}
}

func (a *collectorFakeAdmin) IsConfigured() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.configured
}

func (a *collectorFakeAdmin) ConsumerLag(
	ctx context.Context,
	req ConsumerLagRequest,
) (ConsumerLagReport, error) {
	a.mu.Lock()
	a.requests = append(a.requests, req)
	err := a.err
	lags := make(map[string]int64, len(a.lagByTopic))
	for topic, lag := range a.lagByTopic {
		lags[topic] = lag
	}
	unavailable := make(map[string]int, len(a.unavailableByTopic))
	for topic, count := range a.unavailableByTopic {
		unavailable[topic] = count
	}
	a.mu.Unlock()

	if err != nil {
		return ConsumerLagReport{SubscriberID: req.SubscriberID, GroupID: req.GroupID}, err
	}

	report := ConsumerLagReport{
		SubscriberID: req.SubscriberID,
		GroupID:      req.GroupID,
		MeasuredAt:   time.Now().UTC(),
	}
	for _, topic := range req.Topics {
		lag := lags[topic]
		report.Topics = append(report.Topics, TopicLag{
			Topic:                 topic,
			TotalLag:              lag,
			PartitionsUnavailable: unavailable[topic],
		})
		report.TotalLag += lag
	}

	// NOTHING IS RECORDED HERE, and that is the production shape. The lag gauges are
	// asynchronous, so a measurement is not written when it is taken: the collector publishes
	// the whole inventory once per tick from every subscriber's samples. A fake that published
	// per measurement would retire every other subscriber's series on each call.
	return report, nil
}

func (a *collectorFakeAdmin) snapshotRequests() []ConsumerLagRequest {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]ConsumerLagRequest, len(a.requests))
	copy(out, a.requests)

	return out
}

// collectorFakeAgeRefresher stands in for the dead-letter service's age refresh.
type collectorFakeAgeRefresher struct {
	mu sync.Mutex

	report DeadLetterAgeReport
	err    error
	calls  int
}

func (d *collectorFakeAgeRefresher) RefreshDeadLetterAgeGauge(_ context.Context) (DeadLetterAgeReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls++
	if d.err != nil {
		return DeadLetterAgeReport{}, d.err
	}

	return d.report, nil
}

func (d *collectorFakeAgeRefresher) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.calls
}

// collectorRecordedInt64Gauge captures Int64Gauge measurements with their attributes.
type collectorRecordedInt64Gauge struct {
	embedded.Int64Gauge

	mu      sync.Mutex
	records []collectorGaugeRecord
}

// collectorGaugeRecord is one captured measurement.
type collectorGaugeRecord struct {
	value      int64
	attributes map[string]string
}

var _ otelmetric.Int64Gauge = (*collectorRecordedInt64Gauge)(nil)

func (g *collectorRecordedInt64Gauge) Record(_ context.Context, value int64, options ...otelmetric.RecordOption) {
	recorded := otelmetric.NewRecordConfig(options).Attributes()

	attributes := map[string]string{}
	for _, keyValue := range recorded.ToSlice() {
		attributes[string(keyValue.Key)] = keyValue.Value.Emit()
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.records = append(g.records, collectorGaugeRecord{value: value, attributes: attributes})
}

func (g *collectorRecordedInt64Gauge) Enabled(context.Context) bool { return true }

func (g *collectorRecordedInt64Gauge) snapshot() []collectorGaugeRecord {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make([]collectorGaugeRecord, len(g.records))
	copy(out, g.records)

	return out
}

// values returns the recorded values in order, for a gauge with no attributes.
//
// This recorder now serves ONLY the outbox-backlog gauge, which is the last synchronous
// Int64Gauge in the event pipeline and is deliberately unattributed — one process has one
// backlog, so a label would add cardinality without adding information. A companion that
// indexed records by label tuple existed for the consumer-lag gauge and was removed with it:
// the lag gauges are asynchronous now, so their telemetry is read from the published inventory
// (see captureLagInventory) rather than intercepted as writes.
func (g *collectorRecordedInt64Gauge) values() []int64 {
	out := []int64{}
	for _, record := range g.snapshot() {
		out = append(out, record.value)
	}

	return out
}

// captureBacklogGauge swaps the shared backlog gauge for a recorder.
func captureBacklogGauge(t *testing.T) *collectorRecordedInt64Gauge {
	t.Helper()

	recorder := &collectorRecordedInt64Gauge{}
	original := metrics.OutboxPendingBacklog
	t.Cleanup(func() { metrics.OutboxPendingBacklog = original })
	metrics.OutboxPendingBacklog = recorder

	return recorder
}

// lagInventoryView is the exported consumer-lag telemetry, keyed by series.
//
// It has three fields because the two gauges answer different questions and their difference
// is the whole point of the separation: lag carries only COMPLETE measurements, which is what
// the >10000 rule reads, while unmeasured is reported for every measured topic including as
// zero. series lists everything present so that an ABSENCE can be asserted — under the
// asynchronous gauges a retired series is absent rather than zero, so a test that looked only
// at values could not tell "retired" from "reported as zero".
type lagInventoryView struct {
	lag        map[string]int64
	unmeasured map[string]int
	series     []string
}

// has reports whether a series is present in the inventory at all.
func (v lagInventoryView) has(series string) bool {
	_, present := v.unmeasured[series]

	return present
}

// exportsLag reports whether a series exports a lag reading, as opposed to being present with
// its lag withheld.
func (v lagInventoryView) exportsLag(series string) bool {
	_, present := v.lag[series]

	return present
}

// captureLagInventory isolates the process-wide consumer-lag inventory for one test and
// returns a reader for it.
//
// The inventory is package-level state in internal/metrics — it has to be, because the SDK
// reads it from a collection callback that no test controls — so it is saved and restored
// rather than swapped for a fake. That replaces a fake INSTRUMENT, which an asynchronous gauge
// cannot have: an observable gauge exposes no Record to intercept, because a measurement is
// read at collection time rather than written when taken.
//
// Reading the inventory is strictly closer to what is exported than intercepting writes was.
// A fake instrument could only show what was written to it, and the defect this replaced
// existed precisely because writing was not the same as exporting: a zero written to a
// departed subscriber's tuple stopped it alerting while leaving the series in existence for
// ever.
func captureLagInventory(t *testing.T) func() lagInventoryView {
	t.Helper()

	original := metrics.ConsumerLagInventory()
	t.Cleanup(func() { metrics.PublishConsumerLagInventory(original) })
	metrics.PublishConsumerLagInventory(nil)

	return func() lagInventoryView {
		samples := metrics.ConsumerLagInventory()

		view := lagInventoryView{
			lag:        map[string]int64{},
			unmeasured: map[string]int{},
			series:     make([]string, 0, len(samples)),
		}

		for _, sample := range samples {
			key := sample.Subscriber + "|" + sample.Group + "|" + sample.Topic
			view.series = append(view.series, key)
			view.unmeasured[key] = sample.UnmeasuredPartitions

			// Recorded only when the measurement is complete, mirroring
			// observeConsumerLagInventory: a withheld sample must not be readable as a
			// value, or a test could assert on a number the alert can never see.
			if sample.LagComplete {
				view.lag[key] = sample.Lag
			}
		}

		return view
	}
}

// collectorSubscriber returns a registry row with canonical identifiers and the given
// topics.
func collectorSubscriber(topics ...string) model.EventSubscriber {
	subscriberID := model.GenerateSubscriberID()

	// The principal and the group are DERIVED from the subscriber id, because that is what
	// the registry stores: the schema's principal_derived and group_derived CHECK
	// constraints make any other combination unstorable, so a fixture that invented them
	// independently would describe a row that cannot exist.
	principal, err := model.CanonicalKafkaPrincipal(subscriberID)
	if err != nil {
		panic(err)
	}
	group, err := model.CanonicalConsumerGroupID(subscriberID)
	if err != nil {
		panic(err)
	}

	return model.EventSubscriber{
		SubscriberID:     subscriberID,
		Name:             "collector fixture",
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: topics,
	}
}

// lagSeriesKey renders the label tuple a lag series is actually published under.
//
// It goes through the SAME resolvers the recording path uses rather than pasting the raw
// column values together, and that is the point: the group label is reduced to the
// subscriber's namespace root so that a subscriber running several consumer instances is
// one series rather than one per worker suffix. A test that asserted on the raw values
// would be asserting on a series nothing publishes.
func lagSeriesKey(subscriber model.EventSubscriber, topic string) string {
	return subscriberLagLabel(subscriber.SubscriberID) + "|" +
		consumerGroupLagLabel(subscriber.ConsumerGroupID) + "|" +
		topicLagLabel(topic)
}

// TestEventMetricsCollector_PublishesTheBacklogIncludingZero is the fix for the backlog
// gauge having no production caller at all.
//
// Two properties, and the second is the one that is easy to omit. The backlog counts
// PENDING PLUS PROCESSING, because a processing row has been claimed under a lease but not
// acknowledged by the broker and is still un-published work — counting only pending rows
// would report a drained backlog at exactly the moment a stalled relay holds every
// claimable row. And a drained outbox records an explicit ZERO, because a gauge keeps its
// last value: without the zero, the backlog reported during an incident stays on the
// dashboard after the incident is over.
func TestEventMetricsCollector_PublishesTheBacklogIncludingZero(t *testing.T) {
	outbox := newCollectorFakeOutbox().
		set(model.EventOutboxStatusPending, 7).
		set(model.EventOutboxStatusProcessing, 3).
		set(model.EventOutboxStatusDispatched, 9_000).
		set(model.EventOutboxStatusDeadLettered, 4)

	gauge := captureBacklogGauge(t)
	collector := NewEventMetricsCollector(outbox, nil, nil, nil)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)

	assert.Equal(t, int64(10), report.PendingBacklog,
		"the backlog is pending plus processing: a claimed-but-unacknowledged row is still un-published work")
	assert.Equal(t, []int64{10}, gauge.values())

	t.Run("dispatched and dead-lettered rows are not backlog", func(t *testing.T) {
		// Both are terminal. Including either would make the backlog grow monotonically
		// with throughput and never drain, so the gauge would be meaningless.
		assert.NotEqual(t, int64(9_014), report.PendingBacklog)
		assert.Less(t, report.PendingBacklog, int64(9_000))
	})

	t.Run("a drained outbox publishes zero rather than leaving the last value standing", func(t *testing.T) {
		drained := newCollectorFakeOutbox().set(model.EventOutboxStatusDispatched, 9_000)
		drainedGauge := captureBacklogGauge(t)

		drainedReport, drainedErr := NewEventMetricsCollector(drained, nil, nil, nil).
			Collect(context.Background())
		require.NoError(t, drainedErr)

		assert.Zero(t, drainedReport.PendingBacklog)
		require.Len(t, drainedGauge.values(), 1,
			"an empty backlog must still be recorded; an unwritten gauge keeps its last value")
		assert.Equal(t, int64(0), drainedGauge.values()[0])
	})

	t.Run("a repository failure is reported and does not panic", func(t *testing.T) {
		failing := newCollectorFakeOutbox()
		failing.err = errors.New("dial tcp: connection refused")
		failingGauge := captureBacklogGauge(t)

		_, failErr := NewEventMetricsCollector(failing, nil, nil, nil).Collect(context.Background())
		require.Error(t, failErr)
		assert.Contains(t, failErr.Error(), "counting the event outbox by status")
		assert.Empty(t, failingGauge.values(),
			"a failed count must publish nothing rather than a fabricated zero")
	})

	t.Run("a collector with no datasource reports rather than panicking", func(t *testing.T) {
		_, err := NewEventMetricsCollector(nil, nil, nil, nil).Collect(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no outbox datasource")
	})
}

// TestEventMetricsCollector_DelegatesTheDeadLetterAgeRefresh is the fix for
// RefreshDeadLetterAgeGauge having no production caller.
//
// The collector DELEGATES rather than computing the age itself. The rule it would have to
// reproduce has three parts — measure from the persisted dead_lettered_at, over
// dead-lettered rows only, publishing every topic Blnk owns including the zeros — and a
// second implementation of it would be a second thing to keep correct, diverging exactly
// when one of the two was fixed.
func TestEventMetricsCollector_DelegatesTheDeadLetterAgeRefresh(t *testing.T) {
	refresher := &collectorFakeAgeRefresher{
		report: DeadLetterAgeReport{
			GeneratedAt:              time.Now().UTC(),
			Outstanding:              3,
			FailedAwaitingDeadLetter: 1,
			OldestByTopic: map[string]time.Duration{
				"blnk.transactions.dlt": 47 * time.Minute,
				"blnk.balances.dlt":     0,
			},
		},
	}

	collector := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, refresher, nil)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, refresher.callCount(), "each collection must refresh the age exactly once")
	assert.Equal(t, int64(3), report.DeadLetterAge.Outstanding)
	assert.Equal(t, int64(1), report.DeadLetterAge.FailedAwaitingDeadLetter,
		"the failed-before-dead-letter population must survive into the collector's report")
	assert.Equal(t, 47*time.Minute, report.DeadLetterAge.OldestAge())
	assert.Greater(t, report.DeadLetterAge.OldestAge().Seconds(), 900.0,
		"the value the 15-minute alert reads must be reachable through the collector")

	t.Run("a refresh failure is reported and the other gauges still publish", func(t *testing.T) {
		failing := &collectorFakeAgeRefresher{err: errors.New("dial tcp: refused")}
		backlog := captureBacklogGauge(t)

		failReport, failErr := NewEventMetricsCollector(
			newCollectorFakeOutbox().set(model.EventOutboxStatusPending, 5), nil, failing, nil,
		).Collect(context.Background())

		require.Error(t, failErr)
		assert.Contains(t, failErr.Error(), "refreshing the dead-letter age gauge")
		assert.Equal(t, int64(5), failReport.PendingBacklog,
			"one failed collection must not suppress the others: a degraded system is when the "+
				"remaining gauges matter most")
		assert.Equal(t, []int64{5}, backlog.values())
	})

	t.Run("no dead-letter service is not a failure", func(t *testing.T) {
		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, nil, nil).
			Collect(context.Background())
		assert.NoError(t, err, "a deployment without a dead-letter service still wants its backlog gauge")
	})
}

// TestEventMetricsCollector_EnumeratesTheRegistryAndMeasuresEverySubscriber is the fix for
// nothing ever asking for consumer lag on a schedule.
//
// ConsumerLag measures one group when asked, and before this collector nothing asked. The
// consequence was not a missing number but a MISSING ALERT: the lag gauge held only whatever
// an operator's ad-hoc query had recorded, so the 10,000-message rule could not fire for a
// subscriber nobody had thought to query. Enumeration is what turns a diagnostic into a
// monitored quantity.
func TestEventMetricsCollector_EnumeratesTheRegistryAndMeasuresEverySubscriber(t *testing.T) {
	first := collectorSubscriber("blnk.transactions", "blnk.balances")
	second := collectorSubscriber("blnk.identities")

	registry := &collectorFakeRegistry{rows: []model.EventSubscriber{first, second}}
	admin := newCollectorFakeAdmin()
	admin.lagByTopic["blnk.transactions"] = 12_500
	admin.lagByTopic["blnk.balances"] = 3
	admin.lagByTopic["blnk.identities"] = 0

	inventory := captureLagInventory(t)
	collector := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, admin)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 2, report.SubscribersMeasured, "every registered subscriber must be measured")
	assert.Zero(t, report.SubscribersSkipped)
	assert.Equal(t, 3, report.LagSeriesPublished, "one series per subscriber-topic pair")
	assert.False(t, report.BudgetReached)

	requests := admin.snapshotRequests()
	require.Len(t, requests, 2)
	assert.Equal(t, first.ConsumerGroupID, requests[0].GroupID,
		"the group measured must be the one recorded on the same registry row")
	assert.Equal(t, first.AuthorizedTopics, requests[0].Topics,
		"a subscriber's own authorised topics are the ones measured, not every topic Blnk owns")
	assert.Equal(t, second.ConsumerGroupID, requests[1].GroupID)

	published := inventory()
	assert.Len(t, published.series, 3, "the exported inventory is exactly this tick's measured set")
	assert.Equal(t, int64(12_500),
		published.lag[lagSeriesKey(first, "blnk.transactions")])
	assert.Equal(t, int64(3),
		published.lag[lagSeriesKey(first, "blnk.balances")])
	assert.Equal(t, int64(0),
		published.lag[lagSeriesKey(second, "blnk.identities")],
		"a caught-up subscriber must publish zero rather than no series at all")

	// Every fixture topic was fully readable, so all three are complete and all three report
	// zero unmeasured partitions. Zero is exported rather than omitted: it is the reading that
	// says the lag beside it can be trusted.
	for _, series := range published.series {
		assert.True(t, published.exportsLag(series), "a completely measured topic must export its lag")
		assert.Zero(t, published.unmeasured[series])
	}

	t.Run("the value the alert reads crosses its threshold", func(t *testing.T) {
		assert.Greater(t, published.lag[lagSeriesKey(first, "blnk.transactions")],
			int64(10_000), "the rule is blnk_kafka_consumer_lag > 10000")
	})

	t.Run("an unprovisioned subscriber is skipped rather than published as healthy", func(t *testing.T) {
		// Authorised for nothing is the fail-closed default of a freshly registered row.
		// Publishing a zero for it would render as a consumer that is keeping up.
		empty := collectorSubscriber()
		emptyInventory := captureLagInventory(t)

		emptyReport, emptyErr := NewEventMetricsCollector(
			newCollectorFakeOutbox(),
			&collectorFakeRegistry{rows: []model.EventSubscriber{empty}},
			nil, newCollectorFakeAdmin(),
		).Collect(context.Background())

		require.NoError(t, emptyErr)
		assert.Zero(t, emptyReport.SubscribersMeasured)
		assert.Equal(t, 1, emptyReport.SubscribersSkipped)
		assert.Empty(t, emptyInventory().series,
			"a subscriber authorised for nothing must publish no series at all")
	})

	t.Run("a row without generated identifiers is skipped loudly", func(t *testing.T) {
		// Measuring it would merge it with every other non-conforming row into the one
		// collapsed series, so the row itself is what needs fixing.
		named := collectorSubscriber("blnk.transactions")
		named.ConsumerGroupID = "acme-recon-group"

		hook := logtest.NewGlobal()
		defer hook.Reset()

		namedReport, namedErr := NewEventMetricsCollector(
			newCollectorFakeOutbox(),
			&collectorFakeRegistry{rows: []model.EventSubscriber{named}},
			nil, newCollectorFakeAdmin(),
		).Collect(context.Background())

		require.NoError(t, namedErr)
		assert.Zero(t, namedReport.SubscribersMeasured)
		assert.Equal(t, 1, namedReport.SubscribersSkipped)

		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "generated identifiers") {
				warned = true
			}
		}
		assert.True(t, warned, "an unmeasurable registry row must be reported, not silently passed over")
	})

	t.Run("one subscriber's measurement failure does not stop the others", func(t *testing.T) {
		failing := newCollectorFakeAdmin()
		failing.err = errors.New("dial tcp: refused")

		failReport, failErr := NewEventMetricsCollector(
			newCollectorFakeOutbox(), &collectorFakeRegistry{rows: []model.EventSubscriber{first, second}},
			nil, failing,
		).Collect(context.Background())

		require.Error(t, failErr)
		assert.Len(t, failing.snapshotRequests(), 2,
			"the second subscriber must still be attempted after the first fails")
		assert.Zero(t, failReport.SubscribersMeasured)
	})

	t.Run("a registry listing failure is reported", func(t *testing.T) {
		broken := &collectorFakeRegistry{err: errors.New("relation does not exist")}

		_, err := NewEventMetricsCollector(
			newCollectorFakeOutbox(), broken, nil, newCollectorFakeAdmin(),
		).Collect(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "listing event subscribers")
	})

	t.Run("no broker means no lag measurement and no failure", func(t *testing.T) {
		unconfigured := newCollectorFakeAdmin()
		unconfigured.configured = false

		report, err := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, unconfigured,
		).Collect(context.Background())

		require.NoError(t, err, "a deployment with no broker is a steady state, not an error")
		assert.Zero(t, report.SubscribersMeasured)
		assert.Empty(t, unconfigured.snapshotRequests(), "no offsets can be differenced without a broker")
	})
}

// TestEventMetricsCollector_RetiresTheSeriesOfASubscriberThatIsGone is the half of gauge
// maintenance that is easy to omit and impossible to notice afterwards.
//
// # What the failure was, and why zeroing was not enough
//
// A gauge series is retained until it is refreshed. Delete a subscriber, revoke its grant, or
// narrow its topic list, and the series for what it used to have is never refreshed — so it
// keeps its last value. If that value was above 10,000 the alert fires INDEFINITELY for a
// subscriber that no longer exists.
//
// The previous implementation answered that by WRITING A ZERO to the departed tuple, and the
// zero did stop the alert. What it could not do was stop the series EXISTING: a synchronous
// gauge has no delete, and its aggregator retains every attribute set it has ever been written
// with, so each departed subscriber left a permanent zero-valued series behind. Subscriber
// churn therefore grew the series count and the SDK's memory without bound, with no cardinality
// limit anywhere to stop it.
//
// So the assertions here changed shape with the instrument. A retired series is now ABSENT
// from the exported inventory rather than present at zero, which is why every check below is
// an absence check — and absence is the stronger property, since it is the one that bounds the
// series count to the current inventory.
func TestEventMetricsCollector_RetiresTheSeriesOfASubscriberThatIsGone(t *testing.T) {
	departing := collectorSubscriber("blnk.transactions")
	staying := collectorSubscriber("blnk.balances")

	registry := &collectorFakeRegistry{rows: []model.EventSubscriber{departing, staying}}
	admin := newCollectorFakeAdmin()
	admin.lagByTopic["blnk.transactions"] = 55_000 // well past the alert threshold
	admin.lagByTopic["blnk.balances"] = 12

	inventory := captureLagInventory(t)
	collector := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, admin)

	firstTick, err := collector.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, firstTick.LagSeriesPublished)
	require.Zero(t, firstTick.LagSeriesCleared, "nothing was published before, so nothing is stale")

	departingSeries := lagSeriesKey(departing, "blnk.transactions")
	require.Equal(t, int64(55_000), inventory().lag[departingSeries],
		"the departing subscriber must be alerting before it is removed, or the test proves nothing")

	// The subscriber is deleted from the registry.
	registry.mu.Lock()
	registry.rows = []model.EventSubscriber{staying}
	registry.mu.Unlock()

	secondTick, err := collector.Collect(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, secondTick.LagSeriesPublished)
	assert.Equal(t, 1, secondTick.LagSeriesCleared,
		"the departed subscriber's series must be reported as retired")

	afterRemoval := inventory()
	assert.False(t, afterRemoval.has(departingSeries),
		"the departed series must be ABSENT from the inventory, not present at zero: a zero stops the alert "+
			"but leaves the series in existence, which is what grew the series count without bound")
	assert.Equal(t, int64(12), afterRemoval.lag[lagSeriesKey(staying, "blnk.balances")],
		"the remaining subscriber must keep its real reading")
	assert.Len(t, afterRemoval.series, 1, "the inventory is the current set and nothing else")

	t.Run("a retired series is reported once and not re-reported forever", func(t *testing.T) {
		// The churn figure describes what LEFT this tick. A departed series that is already
		// gone did not leave again, so re-counting it would misreport a stable registry as
		// churning indefinitely.
		thirdTick, err := collector.Collect(context.Background())
		require.NoError(t, err)
		assert.Zero(t, thirdTick.LagSeriesCleared)
		assert.False(t, inventory().has(departingSeries), "and it must stay absent")
	})

	t.Run("narrowing a grant retires the topic that was dropped", func(t *testing.T) {
		narrowed := staying
		narrowed.AuthorizedTopics = []string{"blnk.identities"}

		registry.mu.Lock()
		registry.rows = []model.EventSubscriber{narrowed}
		registry.mu.Unlock()

		narrowedTick, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, 1, narrowedTick.LagSeriesCleared,
			"the topic removed from the grant must stop reporting a lag it can no longer have")
		assert.False(t, inventory().has(lagSeriesKey(staying, "blnk.balances")))
	})

	t.Run("losing the broker retires every series rather than freezing them", func(t *testing.T) {
		// A broker outage must not leave the last pre-outage lag standing and alerting: the
		// truthful statement is that lag is no longer being measured.
		admin.mu.Lock()
		admin.configured = false
		admin.mu.Unlock()

		outageTick, err := collector.Collect(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, outageTick.LagSeriesCleared)
		assert.Zero(t, outageTick.LagSeriesPublished)
		assert.Empty(t, inventory().series,
			"an empty publication retires everything, which is the honest telemetry for a deployment "+
				"that can no longer measure lag at all")
	})
}

// TestEventMetricsCollector_BoundsTheWorkOneTickCanDo pins the cardinality and cost budget.
//
// It is a budget on two scarce things at once. Each subscriber costs broker round trips per
// authorised topic, so an unbounded registry would make one tick unboundedly long and could
// overrun the collection interval; and each subscriber-topic pair is a retained gauge series,
// so the registry's size is also the series count. Reaching the budget is REPORTED rather
// than silently applied, because a silently truncated enumeration means some subscribers are
// simply not monitored.
func TestEventMetricsCollector_BoundsTheWorkOneTickCanDo(t *testing.T) {
	rows := make([]model.EventSubscriber, 0, 12)
	for i := 0; i < 12; i++ {
		rows = append(rows, collectorSubscriber("blnk.transactions"))
	}

	registry := &collectorFakeRegistry{rows: rows}
	collector := NewEventMetricsCollector(
		newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
	).WithSubscriberBudget(5)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 5, report.SubscribersMeasured, "the budget must stop the enumeration")
	assert.True(t, report.BudgetReached, "reaching the budget must be reported, never silently applied")
	assert.Equal(t, 5, report.LagSeriesPublished)

	pages := registry.snapshotPages()
	require.NotEmpty(t, pages)
	total := 0
	for _, page := range pages {
		total += page.limit
		assert.LessOrEqual(t, page.limit, 5,
			"a page may never ask for more rows than the budget still allows")
	}
	assert.LessOrEqual(t, total, 5, "the budget bounds rows requested, not merely rows used")

	t.Run("the defaults are the documented ones", func(t *testing.T) {
		fresh := NewEventMetricsCollector(nil, nil, nil, nil)
		assert.Equal(t, 15*time.Second, fresh.interval,
			"the interval matches the Prometheus scrape interval in prometheus.yml")
		assert.Equal(t, 200, fresh.subscriberBudget)
		assert.Equal(t, 15*time.Second, DefaultMetricsCollectionInterval)
		assert.Equal(t, 200, DefaultSubscriberMetricsBudget)
	})

	t.Run("a nonsensical configuration falls back rather than disabling the gauges", func(t *testing.T) {
		// A misconfigured cadence must not be able to stop the gauges being published: the
		// failure mode of that is an alert that never fires, which looks exactly like health.
		fallback := NewEventMetricsCollector(nil, nil, nil, nil).
			WithInterval(0).
			WithSubscriberBudget(-3)

		assert.Equal(t, DefaultMetricsCollectionInterval, fallback.interval)
		assert.Equal(t, DefaultSubscriberMetricsBudget, fallback.subscriberBudget)

		negative := NewEventMetricsCollector(nil, nil, nil, nil).WithInterval(-time.Minute)
		assert.Equal(t, DefaultMetricsCollectionInterval, negative.interval)
	})

	t.Run("an explicit configuration is honoured", func(t *testing.T) {
		configured := NewEventMetricsCollector(nil, nil, nil, nil).
			WithInterval(3 * time.Second).
			WithSubscriberBudget(7)

		assert.Equal(t, 3*time.Second, configured.interval)
		assert.Equal(t, 7, configured.subscriberBudget)
	})
}

// TestEventMetricsCollector_LifecycleMatchesTheHouseProcessor pins the lifecycle against the
// precedent every other background loop in this repository follows.
//
// The properties matter individually. A collector that waited a full interval before its
// first collection would leave the gauges ABSENT from /metrics for that interval after every
// deployment — and an absent gauge is not a zero one: a dashboard shows a gap and an alert
// expression over it evaluates to nothing. A second Start would run two loops writing the
// same gauges and interleaving their stale-series bookkeeping. And Stop must WAIT, because a
// tick interrupted mid-way leaves the exporter holding gauges from two different instants.
func TestEventMetricsCollector_LifecycleMatchesTheHouseProcessor(t *testing.T) {
	outbox := newCollectorFakeOutbox().set(model.EventOutboxStatusPending, 2)
	refresher := &collectorFakeAgeRefresher{report: DeadLetterAgeReport{}}
	captureBacklogGauge(t)

	collector := NewEventMetricsCollector(outbox, nil, refresher, nil).
		WithInterval(20 * time.Millisecond)

	require.False(t, collector.IsRunning(), "a fresh collector is not running")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collector.Start(ctx)
	assert.True(t, collector.IsRunning())

	// The first collection happens before the first tick, so a single interval is enough
	// to have seen at least two.
	require.Eventually(t, func() bool { return outbox.callCount() >= 2 }, 2*time.Second, 5*time.Millisecond,
		"the collector must collect immediately and then on every tick")
	assert.GreaterOrEqual(t, refresher.callCount(), 1, "every collection covers every gauge")

	t.Run("a second Start is a no-op", func(t *testing.T) {
		collector.Start(ctx)
		assert.True(t, collector.IsRunning())

		before := outbox.callCount()
		require.Eventually(t, func() bool { return outbox.callCount() > before }, 2*time.Second, 5*time.Millisecond)
		// One loop, not two: the count advances but the collector is still a single
		// runner, which Stop below proves by returning promptly and leaving nothing behind.
		assert.True(t, collector.IsRunning())
	})

	collector.Stop()
	assert.False(t, collector.IsRunning(), "Stop must leave the collector stopped")

	settled := outbox.callCount()
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, settled, outbox.callCount(),
		"no collection may happen after Stop returns; two loops would show up here")

	t.Run("Stop is idempotent", func(t *testing.T) {
		collector.Stop()
		assert.False(t, collector.IsRunning())
	})

	t.Run("a cancelled context stops the loop", func(t *testing.T) {
		cancellable, stop := context.WithCancel(context.Background())
		cancelled := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, nil, nil).
			WithInterval(10 * time.Millisecond)

		cancelled.Start(cancellable)
		require.True(t, cancelled.IsRunning())

		stop()

		// The loop returns on ctx.Done; Stop then joins it and must not block.
		require.Eventually(t, func() bool {
			cancelled.Stop()

			return !cancelled.IsRunning()
		}, 2*time.Second, 10*time.Millisecond, "a cancelled context must end the loop")
	})

	t.Run("a failing collection does not stop the loop", func(t *testing.T) {
		// The moment a dependency fails is when the remaining gauges matter most, so a
		// tick that reports an error must not take the collector down with it.
		failing := newCollectorFakeOutbox()
		failing.err = errors.New("dial tcp: refused")

		resilient := NewEventMetricsCollector(failing, nil, nil, nil).
			WithInterval(10 * time.Millisecond)

		resilient.Start(context.Background())
		defer resilient.Stop()

		require.Eventually(t, func() bool { return failing.callCount() >= 3 }, 2*time.Second, 5*time.Millisecond,
			"the loop must keep collecting after a failure")
		assert.True(t, resilient.IsRunning())
	})
}

// TestNewBlnkEventMetricsCollector_WiresWhatIsPresentAndNothingElse covers the constructor
// the server role will call.
//
// It exists so the eventual start-up call is one line that cannot pair one instance's
// datasource with another's admin client. Its one subtlety is TYPED NILS: assigning a nil
// *EventDeadLetterService into an interface field yields a non-nil interface holding a nil
// pointer, which passes every `!= nil` guard and then panics on first use. Each dependency
// is therefore assigned only when it is genuinely present.
func TestNewBlnkEventMetricsCollector_WiresWhatIsPresentAndNothingElse(t *testing.T) {
	t.Run("a nil container yields a collector that reports rather than panicking", func(t *testing.T) {
		collector := NewBlnkEventMetricsCollector(nil, nil, nil)
		require.NotNil(t, collector)

		var report EventMetricsReport
		var err error
		require.NotPanics(t, func() { report, err = collector.Collect(context.Background()) })
		require.Error(t, err, "a collector with no datasource must say so")
		assert.Zero(t, report.PendingBacklog)
	})

	t.Run("a nil dead-letter service is not stored as a typed nil", func(t *testing.T) {
		collector := NewBlnkEventMetricsCollector(nil, nil, nil)

		assert.Nil(t, collector.deadLetters,
			"a typed nil here would pass the nil guard and panic on first use")
		assert.Nil(t, collector.admin)
		assert.Nil(t, collector.outbox)
	})

	t.Run("present dependencies are wired", func(t *testing.T) {
		service := NewEventDeadLetterService(newDltFakeStore(), NewNoopEventPublisher())

		collector := NewBlnkEventMetricsCollector(nil, service, nil)
		assert.NotNil(t, collector.deadLetters)
		assert.Equal(t, DefaultMetricsCollectionInterval, collector.interval)
	})
}

// ---------------------------------------------------------------------------
// Alert wiring: the rule files have to be REACHABLE, and the two copies of the
// monitoring configuration have to stay in step
// ---------------------------------------------------------------------------

// TestPrometheusRuleFiles_AreMountedWhereTheGlobResolves is the fix for both alert rules
// being inert in the local stack.
//
// A rule file is inert unless prometheus.yml's rule_files stanza resolves to it, and that
// stanza names a path INSIDE the Prometheus container — /etc/prometheus/alerts/*.yml. The
// Compose projection mounted only ./prometheus.yml, so the glob matched nothing and BOTH
// rules were silently absent: a dead-letter entry stuck for 15 minutes and a subscriber
// trailing by 10,000 messages could never fire, however faithfully the gauges were
// published.
//
// The failure mode is what makes this worth a test rather than a comment. Prometheus does
// NOT error on a rule_files glob that matches nothing — it starts cleanly, serves, scrapes,
// and simply has no rules. Nothing in a log or a health check says so; the only symptom is
// an alert that never arrives, which is indistinguishable from a healthy system.
//
// This is asserted over the compose files as data rather than over a running container, so
// it holds in CI with no Docker. The runtime half — that a real Prometheus given exactly
// this mount reports both rules on /api/v1/rules — was verified separately.
func TestPrometheusRuleFiles_AreMountedWhereTheGlobResolves(t *testing.T) {
	root := moduleRootDir(t)

	config := readYAMLFile(t, filepath.Join(root, "prometheus.yml"))

	ruleGlobs, ok := config["rule_files"].([]interface{})
	require.True(t, ok, "prometheus.yml must declare a rule_files stanza, or no rule file is ever evaluated")
	require.NotEmpty(t, ruleGlobs)

	// Every declared glob's directory is what a mount has to provide.
	requiredDirs := map[string]struct{}{}
	for _, glob := range ruleGlobs {
		pattern, isString := glob.(string)
		require.True(t, isString, "a rule_files entry must be a path pattern")
		requiredDirs[path.Dir(pattern)] = struct{}{}
	}
	require.Contains(t, requiredDirs, "/etc/prometheus/alerts",
		"the documented rule directory must be the one the glob resolves against")

	for _, composeFile := range []string{"docker-compose.yaml", "docker-compose.dev.yaml"} {
		t.Run(composeFile, func(t *testing.T) {
			compose := readYAMLFile(t, filepath.Join(root, composeFile))

			services, ok := compose["services"].(map[string]interface{})
			require.True(t, ok, "%s must declare services", composeFile)

			prometheus, ok := services["prometheus"].(map[string]interface{})
			require.True(t, ok, "%s must declare a prometheus service", composeFile)

			volumes, ok := prometheus["volumes"].([]interface{})
			require.True(t, ok, "the prometheus service must mount its configuration")

			mountedTargets := map[string]string{}
			for _, volume := range volumes {
				spec, isString := volume.(string)
				require.True(t, isString, "only short-form volume specs are used in these files")

				parts := strings.Split(spec, ":")
				require.GreaterOrEqual(t, len(parts), 2, "a volume spec must name a source and a target")
				mountedTargets[parts[1]] = parts[0]
			}

			require.Contains(t, mountedTargets, "/etc/prometheus/prometheus.yml",
				"the scrape configuration must be mounted")

			for dir := range requiredDirs {
				source, mounted := mountedTargets[dir]
				require.True(t, mounted,
					"%s must mount a source at %s, or the rule_files glob matches nothing and both "+
						"alerts are silently inert", composeFile, dir)
				assert.Equal(t, "./alerts", source,
					"the mounted source must be this repository's alerts directory")
			}

			// A directory mount rather than a single file, so a rule file added to ./alerts
			// later is picked up without another edit to the compose file.
			assert.DirExists(t, filepath.Join(root, "alerts"),
				"the mounted source directory must exist in the repository")

			ruleFiles, err := filepath.Glob(filepath.Join(root, "alerts", "*.yml"))
			require.NoError(t, err)
			assert.NotEmpty(t, ruleFiles, "the mounted directory must actually contain a rule file")
		})
	}
}

// TestPrometheusConfigParity_KeepsTheKubernetesCopyInStepWithTheRoot is the automated guard
// the duplicated monitoring configuration was missing.
//
// prometheus.yml and alerts/blnk-kafka-alerts.yml exist twice: once at the repository root
// for the Compose stack, and once embedded as ConfigMap keys for Kubernetes, because
// Kubernetes has no bind mounts and a rule file has to travel as data. Two hand-maintained
// copies of an alerting configuration diverge — that is not a prediction, it is what
// happened to the annotation this very change had to correct in both places at once — and
// the divergence is SILENT: each file is valid on its own, each renders correctly in
// review, and the only symptom is one environment alerting while the other does not.
//
// The comparison is SEMANTIC and not textual. The embedded copies carry extra indentation
// from the block scalar and their comments are re-wrapped to the narrower width, so a byte
// comparison would fail on formatting that changes no behaviour and would then be silenced
// rather than fixed. Comparing the parsed documents asserts the only thing that matters:
// that Prometheus would behave identically in both deployments.
func TestPrometheusConfigParity_KeepsTheKubernetesCopyInStepWithTheRoot(t *testing.T) {
	root := moduleRootDir(t)

	configMap := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests", "prometheus-configmap.yaml"))

	data, ok := configMap["data"].(map[string]interface{})
	require.True(t, ok, "the ConfigMap must carry a data section")

	for _, pair := range []struct {
		key      string
		rootPath string
	}{
		{key: "prometheus.yml", rootPath: filepath.Join(root, "prometheus.yml")},
		{key: "blnk-kafka-alerts.yml", rootPath: filepath.Join(root, "alerts", "blnk-kafka-alerts.yml")},
	} {
		t.Run(pair.key, func(t *testing.T) {
			embeddedText, ok := data[pair.key].(string)
			require.True(t, ok, "the ConfigMap must carry a %s key", pair.key)

			var embedded map[string]interface{}
			require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &embedded),
				"the embedded %s must itself be valid YAML", pair.key)

			expected := readYAMLFile(t, pair.rootPath)

			// SCRAPE AUTHENTICATION IS THE ONE SANCTIONED DIVERGENCE, and it is stripped
			// from both sides before the documents are compared.
			//
			// Kubernetes always has a metrics bearer token, so its copy authenticates
			// unconditionally with credentials_file. The root copy cannot: Prometheus
			// validates credentials_file at CONFIG LOAD and refuses to start when the file
			// is absent, so enabling it there would break every local stack that runs
			// without a token — a legitimate and common shape, since server.secure defaults
			// to false locally. The block is therefore present-but-commented at the root and
			// live in the ConfigMap, deliberately.
			//
			// Everything else — the scrape targets, the intervals and rule_files — must
			// still match exactly, which is what this test exists for, so only this one key
			// is normalised away and the Kubernetes side's authentication is then asserted
			// positively below.
			stripScrapeAuthorization(expected)
			stripScrapeAuthorization(embedded)

			assert.Equal(t, expected, embedded,
				"the ConfigMap's %s has diverged from the repository-root copy. Update BOTH: "+
					"one environment alerting while the other does not is a silent failure, and "+
					"each file looks correct on its own", pair.key)

			if pair.key == "prometheus.yml" {
				assertKubernetesScrapesAuthenticate(t, embeddedText)
			}
		})
	}

	t.Run("the alert thresholds are the ones the acceptance criteria name", func(t *testing.T) {
		// Asserted on the embedded copy specifically, so a Kubernetes deployment cannot be
		// left evaluating a different threshold from the one the criteria state.
		embeddedText, ok := data["blnk-kafka-alerts.yml"].(string)
		require.True(t, ok)

		assert.Contains(t, embeddedText, "blnk_dlt_oldest_message_age_seconds > 900",
			"the dead-letter rule must fire above 15 minutes")
		assert.Contains(t, embeddedText, "blnk_kafka_consumer_lag > 10000",
			"the consumer-lag rule must fire above 10,000 messages")
	})

	t.Run("both rules reference instruments this codebase actually publishes", func(t *testing.T) {
		// A rule over a metric name nothing exports evaluates forever without firing, which
		// is the same silent failure as a rule that never loaded. The Prometheus form of an
		// OTel instrument name replaces dots with underscores.
		rules := readYAMLFile(t, filepath.Join(root, "alerts", "blnk-kafka-alerts.yml"))

		groups, ok := rules["groups"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, groups)

		exported := map[string]struct{}{}
		for _, instrument := range []string{
			"blnk.dlt.oldest_message_age_seconds",
			"blnk.kafka.consumer_lag",
			"blnk.kafka.consumer_lag_unmeasured_partitions",
			"blnk.outbox.pending",
			"blnk.subscribers.revocation_pending",
			"blnk.subscribers.oldest_revocation_age_seconds",
		} {
			exported[strings.ReplaceAll(instrument, ".", "_")] = struct{}{}
		}

		found := 0
		for _, group := range groups {
			entries, ok := group.(map[string]interface{})
			require.True(t, ok)

			ruleList, ok := entries["rules"].([]interface{})
			require.True(t, ok)

			for _, rule := range ruleList {
				declared, ok := rule.(map[string]interface{})
				require.True(t, ok)

				expression, ok := declared["expr"].(string)
				require.True(t, ok, "every alert must carry an expression")

				matched := false
				for instrument := range exported {
					if strings.Contains(expression, instrument) {
						matched = true

						break
					}
				}
				assert.True(t, matched,
					"alert %v evaluates %q, which names no instrument this codebase exports",
					declared["alert"], expression)
				found++
			}
		}
		// FOUR, and two of them are counted here precisely because their absence is invisible.
		//
		// The MEASUREMENT-HEALTH rule: without it, an incompletely measured topic publishes no
		// lag — correctly, since a partial sum is a lower bound — and nothing at all would then
		// alert on the subscriber, which reads exactly like a healthy one.
		//
		// The REVOCATION rule: a subscriber deleted from the registry whose broker-side SCRAM
		// credential and ACLs were not removed still authenticates and still reads. Nothing in
		// the API surface shows it, so the outstanding revocation is only ever visible as this
		// gauge and this alert.
		assert.Equal(t, 4, found, "every event-streaming alert must be present")
	})
}

// TestDeadLetterAlert_RemediationIsStatusAware is the alerting-usability requirement.
//
// # What was wrong
//
// The gauge this rule reads covers outbox rows in BOTH the dead_lettered and failed states,
// deliberately — an event whose dead-letter write itself failed is the most stranded an event can
// be, out of the relay's claimable set with nothing on any topic behind it, and excluding it would
// have let it age indefinitely while the alert reported health. But the remediation told the
// operator to replay, unconditionally, and replay accepts ONLY a dead_lettered row: on a failed
// row it refuses with EVENT_NOT_DEAD_LETTERED. So the instruction sent an operator to an endpoint
// that would reject them, on the more urgent of the two states it fires for, at the moment they
// were following a page.
//
// # What is asserted
//
// The remediation has to name both states and give each its own action, and it has to tell the
// operator to establish the status BEFORE acting. Each element is asserted separately so a
// partial rewrite — naming the states without the second action, say — still fails.
//
// It is asserted against the KUBERNETES copy specifically, for the reason the parity test exists:
// the two files each look correct alone, and a deployment reading the un-updated one would page
// operators with the misleading instruction.
func TestDeadLetterAlert_RemediationIsStatusAware(t *testing.T) {
	data := prometheusConfigMapData(t)

	embeddedText, ok := data["blnk-kafka-alerts.yml"].(string)
	require.True(t, ok, "the ConfigMap must carry the alert rules")

	var rules map[string]interface{}
	require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &rules))

	description := alertAnnotation(t, rules, "DeadLetterMessageStuck", "description")

	// BOTH populations named, because an operator cannot choose an action without knowing which
	// one they are looking at.
	assert.Contains(t, description, "dead_lettered",
		"the replayable state must be named, so the operator knows which entries the replay endpoint accepts")
	assert.Contains(t, description, "failed",
		"the non-replayable state must be named too; the alert fires for it and it is the more urgent of the two")

	// The DIAGNOSIS STEP, before either action.
	assert.Contains(t, description, "GET /events/dead-letter",
		"the remediation must direct the operator to read the entry's status first")

	// TWO ACTIONS, and specifically the refusal the wrong one produces.
	assert.Contains(t, description, "POST /events/dead-letter/:event_id/replay",
		"the replay action must still be given for the state that accepts it")
	assert.Contains(t, description, "EVENT_NOT_DEAD_LETTERED",
		"naming the typed refusal is what stops an operator diagnosing the rejection as a second fault")
	assert.Contains(t, description, "NOT replayable",
		"the failed state must be stated as not replayable rather than left for the operator to discover")

	t.Run("the age gauge's declaration documents the same two populations", func(t *testing.T) {
		// The rule's remediation is only correct because of what the gauge measures, so the two
		// have to agree. The declaration used to describe a persisted dead_lettered_at over
		// dead-letter rows only — a column that does not exist and a population narrower than
		// the real one — which is precisely why the remediation was written for one state.
		declaration, err := os.ReadFile(
			filepath.Join(moduleRootDir(t), "internal", "metrics", "metrics.go"),
		)
		require.NoError(t, err)

		body := string(declaration)
		assert.NotContains(t, body, "persisted dead_lettered_at",
			"there is no dead_lettered_at column; documenting one sends a reader looking for it")
		assert.Contains(t, body, "last_attempted_at",
			"the declaration must name the timestamp the age is actually measured from")
		assert.Contains(t, body, "occurred_at",
			"and the fallback, which is what makes a row with no attempt timestamp still age")
	})
}

// prometheusConfigMapData returns the data map of the Prometheus ConfigMap manifest.
//
// Returns:
//   - map[string]interface{}: the manifest's data section.
func prometheusConfigMapData(t *testing.T) map[string]interface{} {
	t.Helper()

	manifest := readYAMLFile(t, filepath.Join(
		moduleRootDir(t), "infrastructure", "k8s-manifests", "prometheus-configmap.yaml",
	))

	data, ok := manifest["data"].(map[string]interface{})
	require.True(t, ok, "the Prometheus manifest must be a ConfigMap with a data section")

	return data
}

// alertAnnotation returns one annotation of one named alert from a parsed rules document.
//
// It fails the test when the alert or the annotation is absent, so a renamed alert cannot make an
// assertion about its annotation pass vacuously.
//
// Returns:
//   - string: the annotation's value.
func alertAnnotation(t *testing.T, rules map[string]interface{}, alert, annotation string) string {
	t.Helper()

	groups, ok := rules["groups"].([]interface{})
	require.True(t, ok, "the rules document must carry groups")

	for _, group := range groups {
		entries, ok := group.(map[string]interface{})
		require.True(t, ok)

		ruleList, ok := entries["rules"].([]interface{})
		require.True(t, ok)

		for _, rule := range ruleList {
			declared, ok := rule.(map[string]interface{})
			require.True(t, ok)

			if declared["alert"] != alert {
				continue
			}

			annotations, ok := declared["annotations"].(map[string]interface{})
			require.True(t, ok, "alert %s must carry annotations", alert)

			value, ok := annotations[annotation].(string)
			require.True(t, ok, "alert %s must carry a %s annotation", alert, annotation)

			return value
		}
	}

	t.Fatalf("no alert named %s is declared", alert)

	return ""
}

// stripScrapeAuthorization removes the authorization block from every scrape job in a
// parsed Prometheus configuration, in place.
//
// It exists so the parity comparison can assert on everything that must match while
// tolerating the one setting that must not — see the note at the comparison site. A
// document with no scrape_configs, or jobs with no authorization, is left untouched.
//
// Parameters:
//   - document map[string]interface{}: the parsed configuration, mutated in place. A
//     document that is not a Prometheus scrape configuration at all is a no-op, which is
//     what lets the same helper be applied to the alert-rules file.
func stripScrapeAuthorization(document map[string]interface{}) {
	jobs, ok := document["scrape_configs"].([]interface{})
	if !ok {
		return
	}

	for _, entry := range jobs {
		job, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		delete(job, "authorization")
	}
}

// assertKubernetesScrapesAuthenticate pins the property the stripped comparison can no
// longer see: the Kubernetes copy authenticates every scrape, by file and never inline.
//
// An unauthenticated scrape against a deployment with server.secure true collects nothing,
// so every series the two Kafka alert rules match on is absent and neither can ever fire —
// a failure indistinguishable from health. And an inline credential would put the token
// into a ConfigMap, which is stored unencrypted and printed in full by kubectl describe.
//
// Parameters:
//   - t *testing.T: the test.
//   - embeddedText string: the ConfigMap's prometheus.yml value, parsed here rather than
//     taken pre-stripped.
func assertKubernetesScrapesAuthenticate(t *testing.T, embeddedText string) {
	t.Helper()

	var document map[string]interface{}
	require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &document))

	jobs, ok := document["scrape_configs"].([]interface{})
	require.True(t, ok, "the embedded configuration must declare scrape jobs")
	require.NotEmpty(t, jobs)

	for _, entry := range jobs {
		job, ok := entry.(map[string]interface{})
		require.True(t, ok)

		name, _ := job["job_name"].(string)
		authorization, ok := job["authorization"].(map[string]interface{})
		require.True(t, ok,
			"job %q must authenticate: an unauthenticated scrape of a secure deployment "+
				"collects nothing and leaves both Kafka alerts unable to fire", name)

		assert.Equal(t, "Bearer", authorization["type"], "job %q must present a bearer token", name)
		assert.Equal(t, "/etc/prometheus/secrets/metrics-bearer-token", authorization["credentials_file"],
			"job %q must read the token from the path prometheus-deployment.yaml projects "+
				"the blnk-metrics-token Secret into", name)
		assert.NotContains(t, job, "credentials",
			"job %q must not carry an inline credential: a ConfigMap is stored unencrypted "+
				"and printed in full by kubectl describe", name)
	}
}

// readYAMLFile parses a YAML file into a generic map.
//
// Generic rather than typed on purpose: what is being compared is the DOCUMENT, and a typed
// struct would silently drop every field it did not declare — which is exactly how a parity
// check comes to pass while the two copies differ in a field nobody thought to model.
func readYAMLFile(t *testing.T, path string) map[string]interface{} {
	t.Helper()

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "%s must be readable", path)

	var parsed map[string]interface{}
	require.NoError(t, yaml.Unmarshal(contents, &parsed), "%s must be valid YAML", path)

	return parsed
}
