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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
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

// ---------------------------------------------------------------------------
// The attempt label: a closed domain
// ---------------------------------------------------------------------------

// TestAttemptLabel_DomainIsClosedAtEightValues drives every reachable input through
// attemptLabel and asserts the output is always one of the eight declared values.
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

// TestAttemptLabel_NumbersTheBudgetAndCollapsesEverythingAbove pins the exact mapping
// for an original publish.
//
// The numeric part is what the latency target selects on — attempt="1" is
// first-attempt-only — so each attempt inside the budget must render as its own number.
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

// TestAttemptLabel_ReplayAndDeadLetterIgnoreTheAttemptNumberEntirely is the assertion
// that keeps the two non-retry publishes out of the latency population.
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
// The purpose selects a bounded metric attribute value, so an unrecognised one must NOT
// be passed through: doing so would reopen the domain that PublishPurpose exists to
// close.
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

// TestResolveMaxAttempts_TreatsAnUnstatedBudgetAsUnknown pins the difference between
// "no budget was stated" and "a budget of zero attempts".
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
// The per-attempt logging contract expressed as a test.
//
// The attempt count and the error reason are logged on EVERY attempt rather than only
// on the final failure.
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
// A field reading max_attempts=0 says "the budget is zero attempts", which is a
// different and wrong statement.
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
// events these are.
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
// They exist for two reasons that both have to hold at once, and a test that only
// checked the happy path would notice neither.
//
// The first is CARDINALITY. subscriber, group and topic all arrive from registry rows,
// which are caller-authored.
//
// The second is CLEARABILITY.
func TestLagLabelResolvers_BoundTheGaugeCardinalityAndStayClearable(t *testing.T) {
	t.Run("a generated subscriber identifier becomes a stable pseudonym", func(t *testing.T) {
		identifier := model.GenerateSubscriberID()
		label := subscriberLagLabel(identifier)

		// PSEUDONYMOUS, not the identifier. A metric label is scraped into a time-series
		// database, rendered on dashboards and quoted into alert notifications, none of which
		// carries access control, so the registry identifier is withheld and a stable hash of
		// it published instead.
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

		// Every group inside one subscriber's namespace resolves to the SAME series. This is
		// the property that keeps a subscriber running many consumer groups from multiplying
		// the alert into one firing instance per group.
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

	// unwindowedCalls counts calls to the UNWINDOWED aggregate, which is the only one the
	// seam offers now.
	unwindowedCalls int
}

func newCollectorFakeOutbox() *collectorFakeOutbox {
	return &collectorFakeOutbox{counts: map[string]int64{}}
}

// CountUnresolvedEventOutbox reproduces the repository's GROUP BY semantics exactly: a
// status with no rows is ABSENT from the map rather than present with a zero. That is
// what makes the collector's two-value reads mandatory, so a fake that returned zeros
// would let a buggy single-value read pass.
func (o *collectorFakeOutbox) CountUnresolvedEventOutbox(_ context.Context) (map[string]int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.calls++
	o.unwindowedCalls++

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

// unwindowedCallCount returns how many times the UNWINDOWED aggregate was asked for.
//
// It is the assertion surface the skip-the-broker posture needs: the property under test is no longer "the
// collector named a bounded window" but "the collector reached for the aggregate that
// has no history arm in it", and the count is what proves the call landed there.
func (o *collectorFakeOutbox) unwindowedCallCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.unwindowedCalls
}

// collectorFakeRegistry pages a fixed subscriber list.
type collectorFakeRegistry struct {
	// residue, residueCall and residueErr drive the subscriber access-residue read: the
	// orphaned credentials and refused revocations nothing else measures.
	residue     model.SubscriberAccessResidue
	residueCall int
	residueErr  error

	mu sync.Mutex

	rows  []model.EventSubscriber
	err   error
	pages []collectorPage

	// revocations is the outstanding-revocation backlog the aggregate read returns, and
	// revocationErr makes that read fail. They are SEPARATE from rows and err because the
	// two reads are separate: the revocation backlog is an aggregate that ignores the
	// paging budget and the lag sweep's skip rules, so a test must be able to fail one
	// without the other.
	revocations    model.SubscriberRevocationBacklog
	revocationErr  error
	revocationCall int

	// settlement is the broker-side settlement backlog the aggregate read returns, and
	// settlementErr makes that read fail. Separate from the revocation pair for the same
	// reason that pair is separate from rows and err: it is a third independent read, and
	// a test must be able to fail one without disturbing the others.
	settlement     model.SubscriberSettlementBacklog
	settlementErr  error
	settlementCall int

	// total, totalErr and totalCall drive the registry SIZE read, which is the denominator
	// the unmeasured count is judged against.
	total     *int64
	totalErr  error
	totalCall int
}

// collectorPage is one recorded enumeration request, so the paging and the budget can
// be asserted on the requests themselves rather than inferred from the results.
//
// The position is a CURSOR rather than an offset: a nil cursor is the start of a pass,
// and a non-nil one names the row the previous tick stopped at.
type collectorPage struct {
	limit  int
	cursor *model.SubscriberCursor
}

// CountSubscriberRevocationsPending returns the seeded backlog and counts its calls, so a test
// can assert that the collector reads it ONCE per tick — an aggregate consulted per subscriber
// would defeat the reason it is an aggregate.
func (r *collectorFakeRegistry) CountSubscriberRevocationsPending(
	_ context.Context,
) (model.SubscriberRevocationBacklog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.revocationCall++

	if r.revocationErr != nil {
		return model.SubscriberRevocationBacklog{}, r.revocationErr
	}

	return r.revocations, nil
}

// CountSubscriberSettlementObligations returns the seeded backlog and counts its calls, so a
// test can assert the collector reads it ONCE per tick — an aggregate consulted per subscriber
// would defeat the reason it is an aggregate.
func (r *collectorFakeRegistry) CountSubscriberSettlementObligations(
	_ context.Context,
) (model.SubscriberSettlementBacklog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.settlementCall++

	if r.settlementErr != nil {
		return model.SubscriberSettlementBacklog{}, r.settlementErr
	}

	return r.settlement, nil
}

func (r *collectorFakeRegistry) ListEventSubscribers(
	_ context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pages = append(r.pages, collectorPage{limit: query.Limit, cursor: query.Cursor})

	if r.err != nil {
		return model.SubscriberPage{}, r.err
	}

	limit := query.Limit
	if limit <= 0 {
		limit = 1
	}

	start := 0
	if query.Cursor != nil {
		// A cursor naming a row that is no longer here leaves start at len(rows), which is
		// what a keyset read does when the row it resumed from was deleted and everything
		// after it went with it: an empty page, and therefore a completed pass.
		start = len(r.rows)
		for i := range r.rows {
			if r.rows[i].CreatedAt.Equal(query.Cursor.CreatedAt) && r.rows[i].ID == query.Cursor.ID {
				start = i + 1

				break
			}
		}
	}

	if start >= len(r.rows) {
		return model.SubscriberPage{}, nil
	}

	end := start + limit
	if end > len(r.rows) {
		end = len(r.rows)
	}

	page := model.SubscriberPage{
		Subscribers: make([]model.EventSubscriber, end-start),
		HasMore:     end < len(r.rows),
	}
	copy(page.Subscribers, r.rows[start:end])

	if page.HasMore {
		last := page.Subscribers[len(page.Subscribers)-1]
		page.NextCursor = &model.SubscriberCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

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
	// missingTopics names the authorised topics the broker does not hold, so a test can
	// drive the incomplete-measurement path without a broker.
	missingTopics map[string]bool

	mu sync.Mutex

	configured bool
	lagByTopic map[string]int64

	// unavailableByTopic makes a topic's measurement INCOMPLETE: that many of its
	// partitions could not be read, so its lag is a lower bound and must be withheld from
	// the gauge the alert reads. See metrics.ConsumerLagUnmeasuredPartitions.
	unavailableByTopic map[string]int

	err      error
	requests []ConsumerLagRequest

	// beforeMeasure runs at the START of each ConsumerLag call, OUTSIDE the fake's own
	// mutex, so a test can observe how many measurements are in flight at once. It has to
	// be outside the mutex or every call would serialise on it and a concurrent sweep
	// would be indistinguishable from a sequential one — the fake's own lock would be
	// doing the serialising the test is trying to measure.
	beforeMeasure func()
}

func newCollectorFakeAdmin() *collectorFakeAdmin {
	return &collectorFakeAdmin{
		configured:         true,
		lagByTopic:         map[string]int64{},
		unavailableByTopic: map[string]int{},
		missingTopics:      map[string]bool{},
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
	before := a.beforeMeasure
	a.mu.Unlock()

	if before != nil {
		before()
	}

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
	missing := make(map[string]bool, len(a.missingTopics))
	for topic, absent := range a.missingTopics {
		missing[topic] = absent
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
		// A TOPIC THE BROKER DOES NOT HOLD PRODUCES NO TopicLag, which is the production
		// shape: it is named in MissingTopics and contributes no sample at all, so the
		// subscriber ends the tick with a series for its other topics and silence for this
		// one. Reporting it with a zero lag would be the silence dressed as health — the
		// exact reading the topic-missing accounting exists to break.
		if missing[topic] {
			report.MissingTopics = append(report.MissingTopics, topic)

			continue
		}

		lag := lags[topic]
		report.Topics = append(report.Topics, TopicLag{
			Topic:                 topic,
			TotalLag:              lag,
			PartitionsUnavailable: unavailable[topic],
		})
		report.TotalLag += lag
	}

	// NOTHING IS RECORDED HERE, and that is the production shape. The lag gauges are
	// asynchronous, so a measurement is not written when it is taken: the collector
	// publishes the whole inventory once per tick from every subscriber's samples.
	return report, nil
}

func (a *collectorFakeAdmin) snapshotRequests() []ConsumerLagRequest {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]ConsumerLagRequest, len(a.requests))
	copy(out, a.requests)

	return out
}

// reset clears the recorded requests, so a multi-tick test can attribute measurements to the
// tick that made them rather than to the whole run.
func (a *collectorFakeAdmin) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.requests = nil
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
		// Value.Emit renders the string, int64 and bool attributes these instruments
		// record exactly as they were recorded.
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
// This recorder serves the three synchronous, unattributed Int64Gauges of the event
// pipeline: the outbox backlog, and the measurement-coverage pair.
func (g *collectorRecordedInt64Gauge) values() []int64 {
	out := []int64{}
	for _, record := range g.snapshot() {
		out = append(out, record.value)
	}

	return out
}

// collectorUnmeasuredReasons is how many reason labels one coverage tick publishes.
//
// It matches the closed reason set in internal/metrics — budget, unprovisioned,
// measure_failed, registry_failed and topic_missing — and every tick writes all five,
// including the zeros.
const collectorUnmeasuredReasons = 5

// perTickTotals folds an ATTRIBUTED gauge's writes back into one number per tick.
//
// The unmeasured gauge is exported attributed BY REASON — the reason decides the
// remediation, and only 'budget' is answered by configuration — so one tick writes one
// record per reason rather than a single number.
//
// Parameters:
//   - t *testing.T: fails when the writes do not divide evenly into whole ticks, which
//     means a reason was skipped and the aggregate below would silently misattribute
//     one tick's total to another.
//   - reasons int: how many reason labels one tick writes.
//
// Returns:
//   - []int64: one total per tick, in order.
func (g *collectorRecordedInt64Gauge) perTickTotals(t *testing.T, reasons int) []int64 {
	t.Helper()

	records := g.snapshot()
	require.Zerof(t, len(records)%reasons,
		"every tick must publish all %d reasons, zeros included, so a healthy tick is "+
			"distinguishable from a collector that has stopped; got %d writes",
		reasons, len(records))

	totals := []int64{}
	for start := 0; start < len(records); start += reasons {
		var total int64
		for _, record := range records[start : start+reasons] {
			require.Containsf(t, record.attributes, "reason",
				"every unmeasured write must carry its reason; %v does not", record.attributes)
			total += record.value
		}
		totals = append(totals, total)
	}

	return totals
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

// coverageGauges holds the recorders standing in for the two measurement-coverage gauges.
//
// They are returned as a PAIR because neither is interpretable alone: a shortfall of
// three is a rounding error against a registry of three thousand and almost total
// blindness against a registry of five, so every assertion below reads both.
type coverageGauges struct {
	unmeasured *collectorRecordedInt64Gauge
	registered *collectorRecordedInt64Gauge
}

// captureCoverageGauges swaps the two subscriber-coverage gauges for recorders.
func captureCoverageGauges(t *testing.T) coverageGauges {
	t.Helper()

	gauges := coverageGauges{
		unmeasured: &collectorRecordedInt64Gauge{},
		registered: &collectorRecordedInt64Gauge{},
	}

	originalUnmeasured := metrics.SubscribersUnmeasured
	originalRegistered := metrics.SubscribersRegistered
	t.Cleanup(func() {
		metrics.SubscribersUnmeasured = originalUnmeasured
		metrics.SubscribersRegistered = originalRegistered
	})

	metrics.SubscribersUnmeasured = gauges.unmeasured
	metrics.SubscribersRegistered = gauges.registered

	return gauges
}

// captureMeasurementBudgetGauge swaps the exported sweep budget for a recorder.
//
// Separate from captureCoverageGauges rather than folded into it, because the two
// answer different questions and the existing cases assert on the pair above by
// position.
func captureMeasurementBudgetGauge(t *testing.T) *collectorRecordedInt64Gauge {
	t.Helper()

	recorder := &collectorRecordedInt64Gauge{}
	original := metrics.SubscriberMeasurementBudget
	t.Cleanup(func() { metrics.SubscriberMeasurementBudget = original })
	metrics.SubscriberMeasurementBudget = recorder

	return recorder
}

// lagInventoryView is the exported consumer-lag telemetry, keyed by series.
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
// Reading the inventory is strictly closer to what is exported than intercepting writes
// was.
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
			// observeConsumerLagInventory: a withheld sample must not be readable as a value, or
			// a test could assert on a number the alert can never see.
			if sample.LagComplete {
				view.lag[key] = sample.Lag
			}
		}

		return view
	}
}

// collectorSubscriberSeq numbers fixture rows so each one gets a DISTINCT keyset
// position.
var collectorSubscriberSeq atomic.Int64

// collectorSubscriber returns a registry row with canonical identifiers and the given
// topics.
func collectorSubscriber(topics ...string) model.EventSubscriber {
	subscriberID := model.GenerateSubscriberID()
	seq := collectorSubscriberSeq.Add(1)

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
		ID:               math.MaxInt32 - seq,
		SubscriberID:     subscriberID,
		Name:             "collector fixture",
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: topics,
		CreatedAt:        collectorFixtureEpoch.Add(-time.Duration(seq) * time.Second),
	}
}

// collectorFixtureEpoch is the instant fixture creation times descend from. Fixed rather than
// time.Now() so a keyset position is reproducible across a test run.
var collectorFixtureEpoch = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

// lagSeriesKey renders the label tuple a lag series is actually published under.
func lagSeriesKey(subscriber model.EventSubscriber, topic string) string {
	return subscriberLagLabel(subscriber.SubscriberID) + "|" +
		consumerGroupLagLabel(subscriber.ConsumerGroupID) + "|" +
		topicLagLabel(topic)
}

// TestEventMetricsCollector_PublishesTheBacklogIncludingZero is the fix for the backlog
// gauge having no production caller at all.
//
// Two properties, and the second is the one that is easy to omit. The backlog counts
// PENDING PLUS PROCESSING, because a processing row is claimed under a lease but not yet
// acknowledged and is still un-published work. And a drained outbox records an explicit
// ZERO, because a gauge keeps its last value until it is written again.
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
		assert.Contains(t, failErr.Error(), "counting the unresolved event outbox")
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
// The collector DELEGATES rather than computing the age itself.
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

// TestEventMetricsCollector_EnumeratesTheRegistryAndMeasuresEverySubscriber is the fix
// for nothing ever asking for consumer lag on a schedule.
//
// ConsumerLag measures one group when asked, and before this collector nothing asked.
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

	// The requests are matched BY GROUP rather than by position. A page is measured
	// concurrently, so which broker call is issued first is decided by goroutine
	// scheduling and is not a contract — what IS a contract is that each row is measured
	// with the group and the topics recorded on that same row.
	requests := admin.snapshotRequests()
	require.Len(t, requests, 2, "one measurement per registered subscriber, no more and no fewer")

	byGroup := make(map[string]ConsumerLagRequest, len(requests))
	for _, request := range requests {
		byGroup[request.GroupID] = request
	}

	firstRequest, measuredFirst := byGroup[first.ConsumerGroupID]
	require.True(t, measuredFirst,
		"the group measured must be the one recorded on the same registry row")
	assert.Equal(t, first.AuthorizedTopics, firstRequest.Topics,
		"a subscriber's own authorised topics are the ones measured, not every topic Blnk owns")

	secondRequest, measuredSecond := byGroup[second.ConsumerGroupID]
	require.True(t, measuredSecond, "every registered subscriber must be reached")
	assert.Equal(t, second.AuthorizedTopics, secondRequest.Topics,
		"the second row's topics must come from the second row, not be blended with the first")

	published := inventory()
	assert.Len(t, published.series, 3, "the exported inventory is exactly this tick's measured set")
	assert.Equal(t, int64(12_500),
		published.lag[lagSeriesKey(first, "blnk.transactions")])
	assert.Equal(t, int64(3),
		published.lag[lagSeriesKey(first, "blnk.balances")])
	assert.Equal(t, int64(0),
		published.lag[lagSeriesKey(second, "blnk.identities")],
		"a caught-up subscriber must publish zero rather than no series at all")

	// Every fixture topic was fully readable, so all three are complete and all three
	// report zero unmeasured partitions. Zero is exported rather than omitted: it is the
	// reading that says the lag beside it can be trusted.
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

// TestEventMetricsCollector_RetiresTheSeriesOfASubscriberThatIsGone is the half of
// gauge maintenance that is easy to omit and impossible to notice afterwards.
//
// A gauge series is retained until it is refreshed.
//
// So the assertions here changed shape with the instrument.
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

// TestEventMetricsCollector_BoundsTheWorkOneTickCanDo pins the cardinality and cost
// budget.
//
// It is a budget on two scarce things at once.
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

// TestEventMetricsCollector_RotatesUntilEverySubscriberIsMeasured is the guard, and it
// is the property the previous collector did not have at all.
//
// The lag sweep is BUDGETED, so a registry larger than one budget cannot be measured in
// a single tick.
func TestEventMetricsCollector_RotatesUntilEverySubscriberIsMeasured(t *testing.T) {
	const (
		registrySize = 7
		budget       = 3
	)

	rows := make([]model.EventSubscriber, 0, registrySize)
	for range registrySize {
		rows = append(rows, collectorSubscriber("blnk.transactions"))
	}

	registry := &collectorFakeRegistry{rows: rows}
	admin := newCollectorFakeAdmin()
	admin.lagByTopic["blnk.transactions"] = 41

	inventory := captureLagInventory(t)
	collector := NewEventMetricsCollector(
		newCollectorFakeOutbox(), registry, nil, admin,
	).WithSubscriberBudget(budget)

	measured := map[string]int{}
	passStarts := make([]time.Time, 0, 3)

	for tick := 1; tick <= 3; tick++ {
		report, err := collector.Collect(context.Background())
		require.NoError(t, err, "tick %d", tick)

		passStarts = append(passStarts, report.LagPassStartedAt)

		for _, request := range admin.snapshotRequests() {
			measured[request.SubscriberID]++
		}
		admin.reset()

		switch tick {
		case 1, 2:
			assert.Equal(t, budget, report.SubscribersMeasured, "tick %d measures a budget's worth", tick)
			assert.True(t, report.BudgetReached, "tick %d stops at the budget", tick)
			assert.False(t, report.LagPassComplete, "tick %d has not reached the end of the registry", tick)
		case 3:
			assert.Equal(t, registrySize-2*budget, report.SubscribersMeasured,
				"the last tick of a pass measures the remainder")
			assert.True(t, report.LagPassComplete, "and reports that the pass is now complete")
		}
	}

	assert.Len(t, measured, registrySize,
		"EVERY subscriber must be measured within one pass; the sweep this replaced measured the "+
			"newest %d for ever and never reached the rest at all", budget)
	for id, times := range measured {
		assert.Equal(t, 1, times, "subscriber %s must be measured exactly once per pass", id)
	}

	// Every reading is still exported at the end of the pass, including the ones taken two
	// ticks ago. That is the retained-reading union, and without it a rotating sweep would
	// export each slice in turn and retire the rest.
	final := inventory()
	assert.Len(t, final.series, registrySize,
		"the exported inventory is the whole registry, not the slice the last tick happened to reach")
	for _, row := range rows {
		assert.Equal(t, int64(41), final.lag[lagSeriesKey(row, "blnk.transactions")],
			"subscriber %s must still be exported from the tick that measured it", row.SubscriberID)
	}

	// The pass clock is the same instant for every tick of one pass and is restarted once
	// the pass completes, which is what makes "how long since every subscriber was last
	// measured" answerable from the report.
	assert.False(t, passStarts[0].IsZero(), "a pass must record when it began")
	assert.Equal(t, passStarts[0], passStarts[1], "the pass clock spans the ticks of one pass")
	assert.Equal(t, passStarts[0], passStarts[2])

	t.Run("the next tick begins a fresh pass from the newest subscriber", func(t *testing.T) {
		registry.mu.Lock()
		registry.pages = nil
		registry.mu.Unlock()

		fourth, err := collector.Collect(context.Background())
		require.NoError(t, err)

		pages := registry.snapshotPages()
		require.NotEmpty(t, pages)
		assert.Nil(t, pages[0].cursor,
			"a completed pass rewinds the cursor, so the next pass starts at the newest subscriber; "+
				"leaving it set would strand it past the end of a shrinking registry")
		assert.NotEqual(t, passStarts[0], fourth.LagPassStartedAt,
			"and restarts the pass clock")
	})

	t.Run("a completed pass retires what it never saw, without waiting for the TTL", func(t *testing.T) {
		// The TTL is the fallback, not the mechanism. A single tick cannot tell a deleted
		// subscriber from one it has not reached; a COMPLETED pass can, because it visited
		// the whole registry — so a retained series it never measured is retired at once.
		small := &collectorFakeRegistry{rows: []model.EventSubscriber{rows[0], rows[1]}}
		smallInventory := captureLagInventory(t)
		smallCollector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), small, nil, admin,
		).WithSubscriberBudget(1)

		// Two ticks to complete one pass over two rows.
		for range 2 {
			_, err := smallCollector.Collect(context.Background())
			require.NoError(t, err)
		}
		require.Len(t, smallInventory().series, 2)

		small.mu.Lock()
		small.rows = []model.EventSubscriber{rows[0]}
		small.mu.Unlock()

		// One tick now covers the whole registry, so the pass completes immediately.
		completed, err := smallCollector.Collect(context.Background())
		require.NoError(t, err)
		require.True(t, completed.LagPassComplete)

		assert.Equal(t, 1, completed.LagSeriesCleared,
			"the pass covered everything and never saw the deleted subscriber, so its series is gone now")
		assert.False(t, smallInventory().has(lagSeriesKey(rows[1], "blnk.transactions")))
		assert.Len(t, smallInventory().series, 1)
	})
}

// TestEventMetricsCollector_PublishesCoverageAlongsideTheLagItMeasured is the remaining
// half of The rotation is only trustworthy if an operator can see whether it is keeping
// up.
func TestEventMetricsCollector_PublishesCoverageAlongsideTheLagItMeasured(t *testing.T) {
	const (
		registrySize = 5
		budget       = 2
	)

	rows := make([]model.EventSubscriber, 0, registrySize)
	for range registrySize {
		rows = append(rows, collectorSubscriber("blnk.transactions", "blnk.balances"))
	}

	registry := &collectorFakeRegistry{rows: rows}
	admin := newCollectorFakeAdmin()
	admin.lagByTopic["blnk.transactions"] = 5
	admin.lagByTopic["blnk.balances"] = 5

	collector := NewEventMetricsCollector(
		newCollectorFakeOutbox(), registry, nil, admin,
	).WithSubscriberBudget(budget)

	t.Run("an outstanding pass reports an age that accumulates across its ticks", func(t *testing.T) {
		// Two in-progress ticks, and the assertion is that the age GROWS between them rather
		// than merely being non-negative. Growth is the property with content: the age is
		// measured from the instant the pass began, which is fixed for the whole rotation, so
		// a value that did not accumulate would mean the clock was being restarted every tick
		// — and then max_over_time could never reach the rotation latency it is supposed to
		// report, so a rotation slower than the reading TTL would look instantaneous.
		first, err := collector.Collect(context.Background())
		require.NoError(t, err)
		require.False(t, first.LagPassComplete, "a budget of 2 over 5 rows cannot complete in one tick")

		second, err := collector.Collect(context.Background())
		require.NoError(t, err)
		require.False(t, second.LagPassComplete, "nor in two")

		assert.Positive(t, first.LagPassAgeSeconds,
			"an outstanding rotation must report its age, which is what is compared against the "+
				"reading TTL to decide whether every subscriber's lag can still alert")
		assert.Greater(t, second.LagPassAgeSeconds, first.LagPassAgeSeconds,
			"the age must accumulate over the ticks of one pass, because the pass clock is what "+
				"it is measured from")
	})

	t.Run("a completed pass takes the age back to zero", func(t *testing.T) {
		// The third tick finishes the pass. Zero is published rather than the age the pass
		// reached, because the quantity is "how long has the CURRENT rotation been
		// outstanding" and a finished rotation has nothing outstanding — which is also what
		// makes max_over_time over the gauge equal to the rotation latency.
		last, err := collector.Collect(context.Background())
		require.NoError(t, err)

		require.True(t, last.LagPassComplete, "three ticks of 2 must cover 5 rows")
		assert.Zero(t, last.LagPassAgeSeconds,
			"a completed pass has no outstanding rotation, so its age is zero")
	})

	t.Run("covered subscribers counts subscribers, not series", func(t *testing.T) {
		// Each fixture row is authorised on TWO topics and therefore contributes two series.
		// Counting series would report five subscribers as ten, and the comparison an
		// operator actually makes — exported coverage against registry size — would be
		// meaningless.
		assert.Equal(t, registrySize, collector.coveredSubscriberCount(),
			"the whole registry is covered after a completed pass, counted once per subscriber")
	})

	t.Run("losing the broker reports zero coverage rather than the last good value", func(t *testing.T) {
		// The empty reading is the informative one. Leaving the previous count standing would
		// report a healthy rotation for a deployment that can no longer measure anything at
		// all, which is the moment the signal matters most.
		admin.mu.Lock()
		admin.configured = false
		admin.mu.Unlock()

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Zero(t, report.LagSeriesPublished, "no broker means nothing is measurable")
		assert.Zero(t, collector.coveredSubscriberCount(),
			"coverage must fall to zero with the inventory it describes")
	})
}

// TestEventMetricsCollector_MeasuresAPageWithBoundedConcurrency is the throughput half
// of
//
// The broker may already be the thing failing.
func TestEventMetricsCollector_MeasuresAPageWithBoundedConcurrency(t *testing.T) {
	const registrySize = 24

	rows := make([]model.EventSubscriber, 0, registrySize)
	for range registrySize {
		rows = append(rows, collectorSubscriber("blnk.transactions"))
	}

	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)

	admin := newCollectorFakeAdmin()
	admin.lagByTopic["blnk.transactions"] = 1

	// gate holds each measurement open until enough have arrived to prove they overlap, so the
	// test does not depend on any of them being slow.
	admin.beforeMeasure = func() {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()

		// Long enough for the semaphore to be saturated by its peers, short enough that a
		// serialised implementation still finishes the test quickly.
		time.Sleep(5 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	collector := NewEventMetricsCollector(
		newCollectorFakeOutbox(),
		&collectorFakeRegistry{rows: rows},
		nil, admin,
	)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, registrySize, report.SubscribersMeasured,
		"every row must still be measured; concurrency changes the timing, not the coverage")

	mu.Lock()
	observedPeak := peak
	mu.Unlock()

	assert.Greater(t, observedPeak, 1,
		"measurements must overlap; a sequential sweep is what makes the rotation too slow to "+
			"cover the registry inside the reading TTL")
	assert.LessOrEqual(t, observedPeak, lagMeasurementConcurrency,
		"the fan-out must never exceed the bound: an unbounded sweep would open a connection per "+
			"subscriber against a broker that may already be failing")
}

// TestEventMetricsCollector_ReadsTheUnresolvedAggregateAndNoHistory is the guard on the
// collector's side of the fix, and it replaces the guard that preceded it.
//
// The property is now the stronger one: the collector must reach for the aggregate that
// HAS no history arm.
func TestEventMetricsCollector_ReadsTheUnresolvedAggregateAndNoHistory(t *testing.T) {
	outbox := newCollectorFakeOutbox().
		set(model.EventOutboxStatusPending, 9).
		set(model.EventOutboxStatusProcessing, 4).
		set(model.EventOutboxStatusFailed, 7).
		set(model.EventOutboxStatusWebhookPending, 3)

	collector := NewEventMetricsCollector(outbox, nil, nil, nil)

	report, err := collector.Collect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(13), report.PendingBacklog,
		"the backlog is pending plus processing, both counted in full")

	// The two repair backlogs come out of the SAME aggregate, which is why removing the
	// history arm costs the collector no query and no coverage.
	assert.Equal(t, int64(7), report.DeadLetterRepairBacklog,
		"the dead-letter repair backlog is the `failed` count, from this same reading")
	assert.Equal(t, int64(3), report.LegacyWebhookRepairBacklog,
		"the legacy-webhook repair backlog is the `webhook_pending` count, from this same reading")

	assert.Equal(t, 1, outbox.unwindowedCallCount(),
		"one tick must ask the unresolved aggregate exactly once: a second call would double the "+
			"cost of the cheapest reading the pipeline has, and zero calls would mean the gauges "+
			"were published from something other than the outbox")
	assert.Equal(t, 1, outbox.callCount(),
		"and it must be the only outbox read a collection makes")
}

// TestEventMetricsCollector_LifecycleMatchesTheHouseProcessor pins the lifecycle
// against the precedent every other background loop in this repository follows.
//
// The properties matter individually.
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

// TestNewBlnkEventMetricsCollector_WiresWhatIsPresentAndNothingElse covers the
// constructor the server role will call.
//
// It exists so the eventual start-up call is one line that cannot pair one instance's
// datasource with another's admin client.
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
// Alert wiring: the rule files have to be REACHABLE, and the two copies of the monitoring
// configuration have to stay in step
// ---------------------------------------------------------------------------

// TestPrometheusRuleFiles_AreMountedWhereTheGlobResolves is the fix for both alert
// rules being inert in the local stack.
//
// The failure mode is what makes this worth a test rather than a comment.
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

	t.Run("everything the glob matches is a rule file", func(t *testing.T) {
		// THE COST OF A CONVENIENT GLOB. Picking up rule files added to ./alerts without a
		// further edit here also means picking up ANY .yml added there — and a file that is
		// not a rule file does not degrade gracefully.
		matched, err := filepath.Glob(filepath.Join(root, "alerts", "*.yml"))
		require.NoError(t, err)

		for _, file := range matched {
			parsed := readYAMLFile(t, file)

			assert.Containsf(t, parsed, "groups",
				"%s is matched by the rule_files glob, so it must be a rule file — a file with a "+
					"top-level key other than `groups` is refused at configuration load and takes "+
					"every rule with it. Unit-test files belong in alerts/tests, which the "+
					"non-recursive glob does not reach", filepath.Base(file))

			for _, key := range []string{"tests", "rule_files", "evaluation_interval"} {
				assert.NotContainsf(t, parsed, key,
					"%s carries the promtool unit-test key %q and is matched by the rule_files "+
						"glob; Prometheus would refuse the configuration and load no rules at all. "+
						"Move it to alerts/tests", filepath.Base(file), key)
			}
		}
	})

	// KUBERNETES HAS NO DIRECTORY TO MOUNT, which is why it needs its own assertion.
	t.Run("kubernetes projects every rule-file key into the glob directory", func(t *testing.T) {
		configMap := readYAMLFile(t, filepath.Join(root, "infrastructure", "k8s-manifests",
			"prometheus-configmap.yaml"))

		data, ok := configMap["data"].(map[string]interface{})
		require.True(t, ok, "the ConfigMap must carry a data section")

		// Every key that is a rule file: a *.yml key other than the scrape configuration.
		ruleKeys := map[string]struct{}{}
		for key := range data {
			if key == "prometheus.yml" {
				continue
			}
			if strings.HasSuffix(key, ".yml") || strings.HasSuffix(key, ".yaml") {
				ruleKeys[key] = struct{}{}
			}
		}
		require.NotEmpty(t, ruleKeys, "the ConfigMap must carry at least one rule-file key")

		deployment := readYAMLGeneric(t, filepath.Join(root, "infrastructure", "k8s-manifests",
			"prometheus-deployment.yaml"))

		mounts := prometheusRuleMounts(t, deployment)

		for key := range ruleKeys {
			mountPath, projected := mounts[key]
			require.True(t, projected,
				"ConfigMap key %q is a rule file but prometheus-deployment.yaml projects no "+
					"subPath mount for it, so Prometheus never loads it — and an unloaded rule "+
					"file raises no error, it is simply absent from /api/v1/rules", key)

			for dir := range requiredDirs {
				assert.Equal(t, path.Join(dir, key), mountPath,
					"key %q must be projected into %s, which is where the rule_files glob "+
						"resolves; anywhere else and it is mounted but never scanned", key, dir)
			}
		}
	})
}

// readYAMLGeneric parses a YAML file the same way readYAMLFile does, and exists so a
// caller that needs to walk a deeply nested manifest is not obliged to re-read the file
// itself.
//
// Parameters:
//   - t *testing.T: the test.
//   - path string: the file to read.
//
// Returns:
//   - map[string]interface{}: the parsed document.
func readYAMLGeneric(t *testing.T, path string) map[string]interface{} {
	t.Helper()

	return readYAMLFile(t, path)
}

// prometheusRuleMounts returns the Prometheus container's subPath volume mounts,
// indexed by subPath, so a caller can ask "is this ConfigMap key projected, and where".
//
// Parameters:
//   - t *testing.T: the test.
//   - deployment map[string]interface{}: the parsed prometheus Deployment.
//
// Returns:
//   - map[string]string: subPath to mountPath.
func prometheusRuleMounts(t *testing.T, deployment map[string]interface{}) map[string]string {
	t.Helper()

	spec, ok := deployment["spec"].(map[string]interface{})
	require.True(t, ok, "the Deployment must carry a spec")

	template, ok := spec["template"].(map[string]interface{})
	require.True(t, ok, "the Deployment must carry a pod template")

	podSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok, "the pod template must carry a spec")

	containers, ok := podSpec["containers"].([]interface{})
	require.True(t, ok, "the pod must declare containers")
	require.NotEmpty(t, containers)

	container, ok := containers[0].(map[string]interface{})
	require.True(t, ok)

	mounts, ok := container["volumeMounts"].([]interface{})
	require.True(t, ok, "the prometheus container must mount its configuration")

	projected := map[string]string{}
	for _, entry := range mounts {
		mount, isMap := entry.(map[string]interface{})
		require.True(t, isMap)

		subPath, hasSubPath := mount["subPath"].(string)
		mountPath, hasMountPath := mount["mountPath"].(string)
		if !hasSubPath || !hasMountPath {
			continue
		}
		projected[subPath] = mountPath
	}

	return projected
}

// TestPrometheusDiscovery_ShipsBothHalvesTogether binds the Kubernetes scrape
// configuration to the identity it needs in order to work.
//
// The pod was 1/1 Running, /-/ready was green, /api/v1/rules listed every rule, and
// /api/v1/targets was empty.
func TestPrometheusDiscovery_ShipsBothHalvesTogether(t *testing.T) {
	root := moduleRootDir(t)
	manifests := filepath.Join(root, "infrastructure", "k8s-manifests")

	configMap := readYAMLFile(t, filepath.Join(manifests, "prometheus-configmap.yaml"))
	data, ok := configMap["data"].(map[string]interface{})
	require.True(t, ok, "the ConfigMap must carry a data section")

	embeddedText, ok := data["prometheus.yml"].(string)
	require.True(t, ok, "the ConfigMap must carry a prometheus.yml key")

	var embedded map[string]interface{}
	require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &embedded),
		"the embedded prometheus.yml must be valid YAML")

	jobs, ok := embedded["scrape_configs"].([]interface{})
	require.True(t, ok, "the embedded configuration must declare scrape jobs")

	discoveringJobs := []string{}
	for _, entry := range jobs {
		job, isMap := entry.(map[string]interface{})
		require.True(t, isMap)

		if _, discovers := job["kubernetes_sd_configs"]; discovers {
			name, _ := job["job_name"].(string)
			discoveringJobs = append(discoveringJobs, name)
		}
	}

	podSpec := k8sPodSpec(t, root, "prometheus-deployment.yaml")

	if len(discoveringJobs) == 0 {
		// Static targets need no API access, so the least-privilege default applies and
		// the rest of this test has nothing to hold.
		assert.Equal(t, false, podSpec["automountServiceAccountToken"],
			"no scrape job discovers through the Kubernetes API, so the Prometheus pod "+
				"must not mount a ServiceAccount token")

		return
	}

	serviceAccount, named := podSpec["serviceAccountName"].(string)
	require.Truef(t, named,
		"jobs %v discover their targets through the Kubernetes API, so "+
			"prometheus-deployment.yaml must name a serviceAccountName. Without one the pod "+
			"runs as the namespace's `default` account, which prometheus-rbac.yaml's "+
			"RoleBinding does not bind — the API server answers 403, discovery finds nothing, "+
			"and Prometheus starts cleanly with an empty /api/v1/targets", discoveringJobs)
	require.NotEmpty(t, serviceAccount, "serviceAccountName must not be empty")

	assert.Equalf(t, true, podSpec["automountServiceAccountToken"],
		"jobs %v discover through the Kubernetes API, so the token must be mounted. Naming "+
			"the ServiceAccount is not sufficient on its own: with automount false there is "+
			"no token file for the client to read and discovery fails at startup with "+
			"`Cannot create service discovery`, after which the pod serves normally and "+
			"scrapes nothing", discoveringJobs)

	// The identity the Deployment names has to be the identity the RBAC file grants. These
	// are three separate objects in a file nothing else references, so each link is
	// asserted rather than assumed.
	rbac := readYAMLDocuments(t, filepath.Join(manifests, "prometheus-rbac.yaml"))

	var (
		accountFound bool
		roles        = map[string][]interface{}{}
		bindings     []map[string]interface{}
	)

	for _, document := range rbac {
		metadata, isMap := document["metadata"].(map[string]interface{})
		require.True(t, isMap, "every RBAC document must carry metadata")

		name, _ := metadata["name"].(string)
		assert.Equal(t, "blnk", metadata["namespace"],
			"RBAC object %q must live in the blnk namespace alongside the pod it grants", name)

		switch document["kind"] {
		case "ServiceAccount":
			if name == serviceAccount {
				accountFound = true
			}
		case "Role":
			rules, _ := document["rules"].([]interface{})
			roles[name] = rules
		case "RoleBinding":
			bindings = append(bindings, document)
		}
	}

	require.Truef(t, accountFound,
		"prometheus-deployment.yaml names ServiceAccount %q, but prometheus-rbac.yaml "+
			"declares no such account. Kubernetes admits the pod anyway and the API server "+
			"then refuses its requests, so the symptom is an empty /api/v1/targets rather "+
			"than a failed apply", serviceAccount)

	var boundRole string
	for _, binding := range bindings {
		subjects, isList := binding["subjects"].([]interface{})
		require.True(t, isList, "a RoleBinding must declare subjects")

		for _, entry := range subjects {
			subject, isMap := entry.(map[string]interface{})
			require.True(t, isMap)

			if subject["kind"] == "ServiceAccount" && subject["name"] == serviceAccount {
				assert.Equal(t, "blnk", subject["namespace"],
					"the RoleBinding subject must name the blnk namespace explicitly")

				roleRef, isMap := binding["roleRef"].(map[string]interface{})
				require.True(t, isMap, "a RoleBinding must declare a roleRef")
				boundRole, _ = roleRef["name"].(string)
			}
		}
	}

	require.NotEmptyf(t, boundRole,
		"no RoleBinding in prometheus-rbac.yaml binds ServiceAccount %q. The account exists "+
			"and the Role exists, and without the binding between them the API server still "+
			"answers 403 — the one failure mode that leaves the deployment looking healthy",
		serviceAccount)

	rules, granted := roles[boundRole]
	require.Truef(t, granted,
		"the RoleBinding references Role %q, which prometheus-rbac.yaml does not declare",
		boundRole)

	// role: pod discovery performs an initial list and then maintains a watch, so a Role
	// granting only list works once and then never notices a replacement pod — which under
	// an HPA that replaces pods continuously is the same defect arriving slowly.
	var podRule map[string]interface{}
	for _, entry := range rules {
		rule, isMap := entry.(map[string]interface{})
		require.True(t, isMap)

		resources, _ := rule["resources"].([]interface{})
		for _, resource := range resources {
			if resource == "pods" {
				podRule = rule
			}
		}
	}

	require.NotNilf(t, podRule,
		"Role %q must grant the `pods` resource: role: pod discovery reads pods and nothing "+
			"else, and without the grant it finds no targets and reports no error", boundRole)

	verbs, isList := podRule["verbs"].([]interface{})
	require.True(t, isList, "the pod rule must declare verbs")
	for _, verb := range []string{"get", "list", "watch"} {
		assert.Containsf(t, verbs, verb,
			"Role %q must grant %q on pods. All three are required: discovery lists once and "+
				"then watches, so a Role without `watch` discovers the pods present at "+
				"startup and never sees another one", boundRole, verb)
	}
}

// TestPrometheusConfigParity_KeepsTheKubernetesCopyInStepWithTheRoot is the automated
// guard the duplicated monitoring configuration was missing.
//
// The comparison is SEMANTIC and not textual.
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

			// TWO SANCTIONED DIVERGENCES, both stripped from both sides before the documents are
			// compared and both then asserted POSITIVELY on the Kubernetes side, so normalising
			// them away removes nothing from this test's reach.
			//
			// SCRAPE AUTHENTICATION.
			//
			// Kubernetes always has a metrics bearer token, so its copy authenticates
			// unconditionally with credentials_file.
			//
			// TARGET DISCOVERY.
			//
			// This assertion USED to compare the targets exactly, and it was right to while both
			// sides were static.
			//
			// Everything else — the job names, the intervals, rule_files and the whole rule file
			// — must still match exactly, which is what this test exists for.
			stripScrapeAuthorization(expected)
			stripScrapeAuthorization(embedded)
			stripScrapeDiscovery(expected)
			stripScrapeDiscovery(embedded)

			assert.Equal(t, expected, embedded,
				"the ConfigMap's %s has diverged from the repository-root copy. Update BOTH: "+
					"one environment alerting while the other does not is a silent failure, and "+
					"each file looks correct on its own", pair.key)

			if pair.key == "prometheus.yml" {
				assertKubernetesScrapesAuthenticate(t, embeddedText)
				assertKubernetesScrapesDiscoverPods(t, embeddedText)
				assertRootScrapesStaticTargets(t, pair.rootPath)
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
			"blnk.subscribers.settlement_outstanding",
			"blnk.subscribers.oldest_settlement_age_seconds",
			"blnk.subscribers.obligations_settled.total",
			// The unsettled-state markers and the collector's own health, all of which alerts
			// here evaluate. Enumerated rather than pattern-matched, because the whole point is
			// that a rule naming a series nothing publishes evaluates nothing and can never fire
			// — which is indistinguishable from health.
			"blnk.subscribers.oldest_credential_orphan_age_seconds",
			"blnk.subscribers.oldest_revocation_failure_age_seconds",
			"blnk.kafka.consumer_lag.pass_age_seconds",
			"blnk.kafka.consumer_lag_inventory_complete",
			"blnk.event_metrics.last_collection_age_seconds",
			"blnk.event_metrics.last_success_age_seconds",
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
		// FIVE, and three of them are counted here precisely because their absence is
		// invisible.
		//
		// The COVERAGE rule: lag is measured for a rotating slice of the registry, so a
		// rotation slower than the reading retention lets a subscriber's series EXPIRE — and
		// an absent series breaches no threshold, which means the lag rule reports nothing
		// wrong for exactly the subscribers it can no longer see. The lag signal cannot
		// report that about itself; only this rule can.
		//
		//   1. DeadLetterMessageStuck — events stranded in a dead-letter inventory
		//   2. SubscriberConsumerLagHigh — a consumer falling behind
		//   3. SubscriberRevocationOutstanding — a credential still owed a revocation
		//   4. SubscriberCredentialOrphaned — a credential the registry does not record
		//   5. SubscriberRevocationRefused — a revocation the broker refused
		//   6. ConsumerLagMeasurementDegraded — a topic measured only in part
		//   7. SubscriberLagCoverageStale — a rotation slower than its reading TTL
		//   8. SubscriberLagCoverageIncomplete — a subscriber with no lag series at all
		//   9. SubscriberSettlementNotProgressing — obligations outstanding, none settling
		//  10. SubscriberSettlementOutstanding — one obligation outstanding too long
		//  11. EventMetricsCollectionStale — the collector has stopped ticking
		//  12. EventMetricsCollectionFailing — it ticks and achieves nothing
		//  13. EventMetricsCollectionAbsent — there is no collector at all
		assert.Equal(t, 13, found, "every event-streaming alert must be present")
	})
}

// TestDeadLetterAlert_RemediationIsStatusAware is the alerting-usability requirement.
//
// The gauge this rule reads covers outbox rows in BOTH the dead_lettered and failed
// states, deliberately — an event whose dead-letter write itself failed is the most
// stranded an event can be, out of the relay's claimable set with nothing on any topic
// behind it, and excluding it would have let it age indefinitely while the alert
// reported health.
//
// The remediation has to name both states and give each its own action, and it has to
// tell the operator to establish the status BEFORE acting.
//
// It is asserted against the KUBERNETES copy specifically, for the reason the parity
// test exists: the two files each look correct alone, and a deployment reading the
// un-updated one would page operators with the misleading instruction.
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
		// The rule's remediation is only correct because of what the gauge measures, so the
		// two have to agree.
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

// alertAnnotation returns one annotation of one named alert from a parsed rules
// document.
//
// It fails the test when the alert or the annotation is absent, so a renamed alert
// cannot make an assertion about its annotation pass vacuously.
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
// tolerating the one setting that must not — see the note at the comparison site.
//
// Parameters:
//   - document map[string]interface{}: the parsed configuration, mutated in place.
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

// stripScrapeDiscovery removes the target-discovery mechanism from every scrape job.
//
// The second sanctioned divergence: the Kubernetes copy uses pod discovery and
// relabelling, the root copy names static targets, and neither is expressible in the
// other's topology.
//
// Parameters:
//   - document map[string]interface{}: a parsed prometheus configuration, mutated in
//     place.
func stripScrapeDiscovery(document map[string]interface{}) {
	jobs, ok := document["scrape_configs"].([]interface{})
	if !ok {
		return
	}

	for _, entry := range jobs {
		job, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		delete(job, "static_configs")
		delete(job, "kubernetes_sd_configs")
		delete(job, "relabel_configs")
	}
}

// assertKubernetesScrapesDiscoverPods pins what the stripped comparison can no longer
// see on the Kubernetes side: every job discovers individual pods, filters to its own
// workload, pins the metrics port and labels each target with its pod name.
//
// Each of those four is load-bearing and each fails quietly if dropped.
//
// Parameters:
//   - t *testing.T: the test.
//   - embeddedText string: the ConfigMap's prometheus.yml value.
func assertKubernetesScrapesDiscoverPods(t *testing.T, embeddedText string) {
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

		assert.NotContains(t, job, "static_configs",
			"job %q must not name a static Service target: the Service load-balances, so each "+
				"scrape reaches one replica of up to ten and its counters appear to reset", name)

		discovery, ok := job["kubernetes_sd_configs"].([]interface{})
		require.True(t, ok, "job %q must discover its targets from the Kubernetes API", name)
		require.NotEmpty(t, discovery, "job %q must declare at least one discovery config", name)

		first, ok := discovery[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "pod", first["role"],
			"job %q must use role: pod — endpoints discovery drops a NotReady replica at exactly "+
				"the moment its metrics matter", name)

		// Namespaced discovery is what allows the namespace-scoped Role in
		// prometheus-rbac.yaml. Widening it without widening that Role brings back a
		// discovery that finds nothing and reports no error.
		namespaces, ok := first["namespaces"].(map[string]interface{})
		require.True(t, ok, "job %q must restrict discovery to a namespace", name)
		assert.Equal(t, []interface{}{"blnk"}, namespaces["names"],
			"job %q must discover only in the blnk namespace, which is what a Role rather than "+
				"a ClusterRole can grant", name)

		relabels, ok := job["relabel_configs"].([]interface{})
		require.True(t, ok, "job %q must relabel its discovered targets", name)

		var keeps, addresses, instances int
		for _, rule := range relabels {
			relabel, ok := rule.(map[string]interface{})
			require.True(t, ok)

			if relabel["action"] == "keep" {
				keeps++
			}
			switch relabel["target_label"] {
			case "__address__":
				addresses++
			case "instance":
				instances++
			}
		}

		assert.Equal(t, 1, keeps,
			"job %q must keep exactly its own workload's pods", name)
		assert.Equal(t, 1, addresses,
			"job %q must pin the metrics port; pod discovery otherwise yields one target per "+
				"declared containerPort", name)
		assert.Equal(t, 1, instances,
			"job %q must label each target with its pod name, or every replica shares one "+
				"series and they overwrite each other", name)
	}
}

// assertRootScrapesStaticTargets pins the other half of the divergence, so the
// normalisation cannot quietly excuse a root copy that has lost its targets altogether.
//
// The Compose stack runs one container per service and has no Kubernetes API, so static
// targets are the correct answer there — but "correct" has to mean present.
//
// Parameters:
//   - t *testing.T: the test.
//   - rootPath string: path to the repository-root prometheus.yml.
func assertRootScrapesStaticTargets(t *testing.T, rootPath string) {
	t.Helper()

	document := readYAMLFile(t, rootPath)

	jobs, ok := document["scrape_configs"].([]interface{})
	require.True(t, ok, "%s must declare scrape jobs", rootPath)
	require.NotEmpty(t, jobs)

	for _, entry := range jobs {
		job, ok := entry.(map[string]interface{})
		require.True(t, ok)

		name, _ := job["job_name"].(string)

		assert.NotContains(t, job, "kubernetes_sd_configs",
			"job %q must not use Kubernetes discovery at the root: the Compose stack has no "+
				"API server to discover from, and Prometheus would fail every discovery cycle", name)

		statics, ok := job["static_configs"].([]interface{})
		require.True(t, ok, "job %q must name its target statically", name)
		require.NotEmpty(t, statics, "job %q must name at least one target", name)

		first, ok := statics[0].(map[string]interface{})
		require.True(t, ok)
		targets, ok := first["targets"].([]interface{})
		require.True(t, ok, "job %q must carry a targets list", name)
		assert.NotEmpty(t, targets, "job %q must name at least one address", name)
	}
}

// assertKubernetesScrapesAuthenticate pins the property the stripped comparison can no
// longer see: the Kubernetes copy authenticates every scrape, by file and never inline.
//
// Parameters:
//   - t *testing.T: the test.
//   - embeddedText string: the ConfigMap's prometheus.yml value, parsed here rather
//     than taken pre-stripped.
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

// readYAMLFile parses a YAML file and returns its FIRST document as a generic map.
//
// Generic rather than typed on purpose: what is being compared is the DOCUMENT, and a
// typed struct would silently drop every field it did not declare — which is exactly
// how a parity check comes to pass while the two copies differ in a field nobody
// thought to model.
//
// MULTI-DOCUMENT files are read rather than refused, because a Kubernetes manifest that
// ships an object with its companion policy — a StatefulSet with the
// PodDisruptionBudget that keeps its quorum, say — is one deployable unit and belongs
// in one file.
func readYAMLFile(t *testing.T, path string) map[string]interface{} {
	t.Helper()

	documents := readYAMLDocuments(t, path)
	require.NotEmpty(t, documents, "%s must contain at least one YAML document", path)

	return documents[0]
}

// readYAMLDocuments parses every document in a YAML file, in order.
//
// Returns:
//   - []map[string]interface{}: one entry per non-empty document.
func readYAMLDocuments(t *testing.T, path string) []map[string]interface{} {
	t.Helper()

	contents, err := os.ReadFile(path)
	require.NoError(t, err, "%s must be readable", path)

	decoder := yaml.NewDecoder(bytes.NewReader(contents))

	var documents []map[string]interface{}
	for {
		var parsed map[string]interface{}
		decodeErr := decoder.Decode(&parsed)
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		require.NoError(t, decodeErr, "%s must be valid YAML", path)

		if len(parsed) > 0 {
			documents = append(documents, parsed)
		}
	}

	return documents
}

// collectorRecordedFloat64Gauge records Float64Gauge measurements, so the revocation-age gauge
// can be asserted by value — including the explicit zero a settled backlog must publish.
type collectorRecordedFloat64Gauge struct {
	embedded.Float64Gauge

	mu      sync.Mutex
	records []float64
}

var _ otelmetric.Float64Gauge = (*collectorRecordedFloat64Gauge)(nil)

func (g *collectorRecordedFloat64Gauge) Record(_ context.Context, value float64, _ ...otelmetric.RecordOption) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.records = append(g.records, value)
}

func (g *collectorRecordedFloat64Gauge) Enabled(context.Context) bool { return true }

func (g *collectorRecordedFloat64Gauge) values() []float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	return append([]float64(nil), g.records...)
}

// collectorRecordedInt64Counter captures Int64Counter increments with their attributes.
//
// The ATTRIBUTES are the point rather than the total.
type collectorRecordedInt64Counter struct {
	embedded.Int64Counter

	mu      sync.Mutex
	records []collectorGaugeRecord
}

var _ otelmetric.Int64Counter = (*collectorRecordedInt64Counter)(nil)

func (c *collectorRecordedInt64Counter) Add(_ context.Context, value int64, options ...otelmetric.AddOption) {
	recorded := otelmetric.NewAddConfig(options).Attributes()

	attributes := map[string]string{}
	for _, keyValue := range recorded.ToSlice() {
		// Emit is the attribute package's rendering accessor at the version this module pins.
		// Every attribute this fake ever receives is an attribute.String, and for the STRING
		// kind it returns the raw value unchanged.
		attributes[string(keyValue.Key)] = keyValue.Value.Emit()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.records = append(c.records, collectorGaugeRecord{value: value, attributes: attributes})
}

func (c *collectorRecordedInt64Counter) Enabled(context.Context) bool { return true }

// collections returns the `collection` attribute of every increment, in order.
func (c *collectorRecordedInt64Counter) collections() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := []string{}
	for _, record := range c.records {
		out = append(out, record.attributes["collection"])
	}

	return out
}

// captureCollectionFailures swaps the collection-failure counter for a recorder.
func captureCollectionFailures(t *testing.T) *collectorRecordedInt64Counter {
	t.Helper()

	recorder := &collectorRecordedInt64Counter{}
	original := metrics.EventMetricsCollectionFailuresTotal
	t.Cleanup(func() { metrics.EventMetricsCollectionFailuresTotal = original })
	metrics.EventMetricsCollectionFailuresTotal = recorder

	return recorder
}

// captureRevocationGauges swaps both outstanding-revocation gauges for recorders.
//
// Both, together, because they are published as a pair and a fix that recorded only the count
// would leave the alert — which reads the AGE — as un-fireable as it was before.
func captureRevocationGauges(t *testing.T) (*collectorRecordedInt64Gauge, *collectorRecordedFloat64Gauge) {
	t.Helper()

	count := &collectorRecordedInt64Gauge{}
	age := &collectorRecordedFloat64Gauge{}

	originalCount := metrics.SubscriberRevocationsPending
	originalAge := metrics.OldestSubscriberRevocationAgeSeconds
	t.Cleanup(func() {
		metrics.SubscriberRevocationsPending = originalCount
		metrics.OldestSubscriberRevocationAgeSeconds = originalAge
	})

	metrics.SubscriberRevocationsPending = count
	metrics.OldestSubscriberRevocationAgeSeconds = age

	return count, age
}

// TestEventMetricsCollector_PublishesTheOutstandingRevocationBacklog is the fix for two
// gauges that were declared, initialised and NEVER RECORDED.
func TestEventMetricsCollector_PublishesTheOutstandingRevocationBacklog(t *testing.T) {
	t.Run("an outstanding backlog is published as a count and an age", func(t *testing.T) {
		count, age := captureRevocationGauges(t)

		registry := &collectorFakeRegistry{}
		registry.revocations = model.SubscriberRevocationBacklog{
			Pending:         3,
			OldestPendingAt: time.Now().UTC().Add(-90 * time.Minute),
		}

		collector := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{3}, count.values(),
			"the count must be published so an operator can tell one stuck subscriber from a "+
				"broker that has been unreachable for an hour")

		require.Len(t, age.values(), 1, "the AGE is the alertable quantity and must be published")
		assert.InDelta(t, (90 * time.Minute).Seconds(), age.values()[0], 5,
			"the age must be in SECONDS, matching the instrument's declared unit and the 3600 "+
				"threshold in alerts/blnk-kafka-alerts.yml")
		assert.Greater(t, age.values()[0], 3600.0,
			"and a 90-minute-old marker must therefore be able to cross that threshold")

		assert.Equal(t, int64(3), report.RevocationsPending)
		assert.InDelta(t, (90 * time.Minute).Seconds(), report.OldestRevocationAge.Seconds(), 5)
		assert.Equal(t, 1, registry.revocationCall,
			"one aggregate read per tick: consulting it per subscriber would defeat the reason it "+
				"is an aggregate rather than a walk of the registry")
	})

	t.Run("a settled backlog publishes an explicit zero", func(t *testing.T) {
		count, age := captureRevocationGauges(t)

		// The zero instant is what the repository returns when MIN() is NULL. Subtracting it
		// blindly would publish an age of fifty-odd years and pin the alert permanently.
		registry := &collectorFakeRegistry{}
		registry.revocations = model.SubscriberRevocationBacklog{}

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{0}, count.values(),
			"zero must be published explicitly, so 'nothing owed' is distinguishable from 'the "+
				"collector stopped'")
		assert.Equal(t, []float64{0}, age.values(),
			"and the age must be zero rather than the age of the epoch")
	})

	t.Run("a failed read publishes nothing and is reported", func(t *testing.T) {
		count, age := captureRevocationGauges(t)

		registry := &collectorFakeRegistry{}
		registry.revocationErr = errors.New("dial tcp: connection refused")

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "counting outstanding subscriber revocations")

		assert.Empty(t, count.values(),
			"a zero on a failed read would assert that everything is settled on the strength of a "+
				"reading that does not exist")
		assert.Empty(t, age.values())
	})

	t.Run("it is measured with no broker configured", func(t *testing.T) {
		// The commonest reason a marker exists at all is that the broker was unreachable when
		// the deregistration tried to revoke. Skipping the measurement without a broker — as
		// the consumer-lag sweep correctly does — would hide the backlog exactly while it
		// grew.
		count, age := captureRevocationGauges(t)

		registry := &collectorFakeRegistry{}
		registry.revocations = model.SubscriberRevocationBacklog{
			Pending:         1,
			OldestPendingAt: time.Now().UTC().Add(-2 * time.Minute),
		}

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{1}, count.values())
		require.Len(t, age.values(), 1)
		assert.Positive(t, age.values()[0])
	})

	t.Run("a collector with no registry publishes nothing rather than a zero", func(t *testing.T) {
		count, age := captureRevocationGauges(t)

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Empty(t, count.values(), "with no registry surface a zero would be an invention")
		assert.Empty(t, age.values())
	})
}

// TestSubscriberRevocationBacklog_OldestAgeIsNeverNegativeOrEpochal covers the two
// readings that would each break the alert in a different direction.
//
// The zero instant means "nothing outstanding" and must produce a zero age, not the age
// of the epoch — which would exceed every threshold for ever.
func TestSubscriberRevocationBacklog_OldestAgeIsNeverNegativeOrEpochal(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	assert.Zero(t, model.SubscriberRevocationBacklog{}.OldestAge(now),
		"nothing outstanding is an age of zero, not fifty-six years")

	assert.Zero(t, model.SubscriberRevocationBacklog{
		Pending:         1,
		OldestPendingAt: now.Add(time.Minute),
	}.OldestAge(now), "clock skew must clamp to zero rather than report a negative age")

	assert.Equal(t, 45*time.Minute, model.SubscriberRevocationBacklog{
		Pending:         1,
		OldestPendingAt: now.Add(-45 * time.Minute),
	}.OldestAge(now))
}

// captureSettlementGauges swaps all four settlement gauges for recorders.
//
// All four, together, because they are published as one reading.
func captureSettlementGauges(t *testing.T) (
	outstanding, grant, credential *collectorRecordedInt64Gauge,
	age *collectorRecordedFloat64Gauge,
) {
	t.Helper()

	outstanding = &collectorRecordedInt64Gauge{}
	grant = &collectorRecordedInt64Gauge{}
	credential = &collectorRecordedInt64Gauge{}
	age = &collectorRecordedFloat64Gauge{}

	originalOutstanding := metrics.SubscriberSettlementOutstanding
	originalGrant := metrics.SubscriberGrantReconcilePending
	originalCredential := metrics.SubscriberCredentialCleanupPending
	originalAge := metrics.OldestSubscriberSettlementAgeSeconds
	t.Cleanup(func() {
		metrics.SubscriberSettlementOutstanding = originalOutstanding
		metrics.SubscriberGrantReconcilePending = originalGrant
		metrics.SubscriberCredentialCleanupPending = originalCredential
		metrics.OldestSubscriberSettlementAgeSeconds = originalAge
	})

	metrics.SubscriberSettlementOutstanding = outstanding
	metrics.SubscriberGrantReconcilePending = grant
	metrics.SubscriberCredentialCleanupPending = credential
	metrics.OldestSubscriberSettlementAgeSeconds = age

	return outstanding, grant, credential, age
}

// TestEventMetricsCollector_PublishesTheSettlementBacklog is what makes the durable
// settlement obligations observable.
//
// A subscriber's state lives in two systems that cannot be written atomically, so a
// request that fails part-way leaves them disagreeing.
func TestEventMetricsCollector_PublishesTheSettlementBacklog(t *testing.T) {
	t.Run("an outstanding backlog is published split by kind, with its age", func(t *testing.T) {
		outstanding, grant, credential, age := captureSettlementGauges(t)

		registry := &collectorFakeRegistry{}
		registry.settlement = model.SubscriberSettlementBacklog{
			Outstanding:              3,
			GrantReconcilePending:    2,
			CredentialCleanupPending: 2,
			OldestPendingAt:          time.Now().UTC().Add(-90 * time.Minute),
		}

		report, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{3}, outstanding.values(),
			"the total is the figure to graph, and it is an OR rather than a sum because one "+
				"subscriber can owe both obligations — which is why 2 and 2 give 3 here")
		assert.Equal(t, []int64{2}, grant.values())
		assert.Equal(t, []int64{2}, credential.values(),
			"the split is what changes what an operator DOES: a rising credential-cleanup count is "+
				"a security matter, a rising grant count is an availability one")

		require.Len(t, age.values(), 1, "the AGE is the alertable quantity")
		assert.InDelta(t, (90 * time.Minute).Seconds(), age.values()[0], 5,
			"in SECONDS, matching the instrument's declared unit and the alert's threshold")

		assert.Equal(t, int64(3), report.SettlementBacklog.Outstanding)
		assert.InDelta(t, (90 * time.Minute).Seconds(), report.OldestSettlementAge.Seconds(), 5)
		assert.Equal(t, 1, registry.settlementCall,
			"one aggregate read per tick: consulting it per subscriber would defeat the reason it "+
				"is an aggregate")
	})

	t.Run("a settled backlog publishes an explicit zero", func(t *testing.T) {
		outstanding, grant, credential, age := captureSettlementGauges(t)

		registry := &collectorFakeRegistry{}
		registry.settlement = model.SubscriberSettlementBacklog{}

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{0}, outstanding.values(),
			"zero must be published explicitly, so 'nothing owed' is distinguishable from 'the "+
				"collector stopped'")
		assert.Equal(t, []int64{0}, grant.values())
		assert.Equal(t, []int64{0}, credential.values())
		assert.Equal(t, []float64{0}, age.values(),
			"and the age must be zero rather than the age of the epoch, which would exceed every "+
				"threshold for ever")
	})

	t.Run("a failed read publishes nothing and is reported", func(t *testing.T) {
		outstanding, grant, credential, age := captureSettlementGauges(t)

		registry := &collectorFakeRegistry{}
		registry.settlementErr = errors.New("dial tcp: connection refused")

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "counting outstanding subscriber settlement obligations")

		assert.Empty(t, outstanding.values(),
			"a zero on a failed read would assert that everything is settled on the strength of a "+
				"reading that does not exist")
		assert.Empty(t, grant.values())
		assert.Empty(t, credential.values())
		assert.Empty(t, age.values())
	})

	t.Run("it is measured with no broker configured", func(t *testing.T) {
		// Obligations are raised BY broker failures, and the commonest of those is the broker
		// being unreachable — so declining to measure the backlog without a broker would hide
		// it exactly when it is growing fastest.
		outstanding, _, _, age := captureSettlementGauges(t)

		registry := &collectorFakeRegistry{}
		registry.settlement = model.SubscriberSettlementBacklog{
			Outstanding:           1,
			GrantReconcilePending: 1,
			OldestPendingAt:       time.Now().UTC().Add(-2 * time.Minute),
		}

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{1}, outstanding.values())
		require.Len(t, age.values(), 1)
		assert.Positive(t, age.values()[0])
	})

	t.Run("a collector with no registry publishes nothing rather than a zero", func(t *testing.T) {
		outstanding, grant, credential, age := captureSettlementGauges(t)

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Empty(t, outstanding.values(),
			"there is nothing to read, so a zero would be an invention")
		assert.Empty(t, grant.values())
		assert.Empty(t, credential.values())
		assert.Empty(t, age.values())
	})
}

// CountSubscriberAccessResidue returns the seeded residue and counts its calls, so a test can
// assert the collector reads it ONCE per tick.
func (r *collectorFakeRegistry) CountSubscriberAccessResidue(
	_ context.Context,
) (model.SubscriberAccessResidue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.residueCall++

	if r.residueErr != nil {
		return model.SubscriberAccessResidue{}, r.residueErr
	}

	return r.residue, nil
}

// TestEventMetricsCollector_RotatesTheSweepSoNoSubscriberIsPermanentlyUnmeasured is the
// direct guard on the backlog gauge.
//
// Successive ticks must cover the whole registry.
func TestEventMetricsCollector_RotatesTheSweepSoNoSubscriberIsPermanentlyUnmeasured(t *testing.T) {
	rows := make([]model.EventSubscriber, 0, 12)
	for i := 0; i < 12; i++ {
		rows = append(rows, collectorSubscriber("blnk.transactions"))
	}

	registry := &collectorFakeRegistry{rows: rows}
	admin := newCollectorFakeAdmin()
	collector := NewEventMetricsCollector(
		newCollectorFakeOutbox(), registry, nil, admin,
	).WithSubscriberBudget(5)

	// 12 rows against a 5-row budget: 5, then 5, then the REMAINING 2. The last tick of a
	// pass measures the remainder rather than wrapping round to fill its budget, which is
	// why the per-tick expectation is stated as a schedule rather than as a constant — a
	// tick that always measured exactly the budget would have to re-measure rows it had
	// just done, paying for readings nothing needs.
	perTick := []int{5, 5, 2}

	measured := map[string]int{}
	for tick, want := range perTick {
		report, err := collector.Collect(context.Background())
		require.NoError(t, err)
		require.Equalf(t, want, report.SubscribersMeasured,
			"tick %d must measure %d: a tick spends its whole budget unless it reaches the end of "+
				"the registry, and then it measures what is left and the pass completes",
			tick+1, want)

		for _, sample := range metrics.ConsumerLagInventory() {
			measured[sample.Subscriber]++
		}
	}

	// 12 rows, 5 + 5 + 2 examinations: every row is reached exactly once and the pass
	// closes.
	assert.Len(t, measured, 12,
		"three ticks of a five-row budget must cover a twelve-row registry: a sweep that restarted "+
			"at the newest row would have covered five, for ever")
}

// TestEventMetricsCollector_ReportsWhetherTheSweepCoveredTheWholeRegistry pins the
// coverage signal, which is the only thing that makes a permanently unmeasured
// subscriber alertable.
//
// The lag inventory is whole-set, so an unmeasured subscriber has NO SERIES.
func TestEventMetricsCollector_ReportsWhetherTheSweepCoveredTheWholeRegistry(t *testing.T) {
	t.Run("a sweep that reaches the end of the registry is complete", func(t *testing.T) {
		rows := []model.EventSubscriber{
			collectorSubscriber("blnk.transactions"),
			collectorSubscriber("blnk.balances"),
		}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), &collectorFakeRegistry{rows: rows}, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(10)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.True(t, report.SweepComplete, "every registered subscriber was measured")
		assert.Equal(t, 2, report.RegistrySize)
		assert.False(t, report.BudgetReached)
	})

	t.Run("a sweep stopped by the budget is incomplete", func(t *testing.T) {
		rows := make([]model.EventSubscriber, 0, 6)
		for i := 0; i < 6; i++ {
			rows = append(rows, collectorSubscriber("blnk.transactions"))
		}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), &collectorFakeRegistry{rows: rows}, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(2)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.False(t, report.SweepComplete,
			"four registered subscribers have no lag series this tick, and that must be published "+
				"rather than left as an absence nothing can alert on")
		assert.True(t, report.BudgetReached)
	})

	t.Run("a sweep whose registry enumeration failed never claims completeness", func(t *testing.T) {
		registry := &collectorFakeRegistry{
			rows: []model.EventSubscriber{collectorSubscriber("blnk.transactions")},
			err:  errors.New("collector test: registry unavailable"),
		}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		)

		report, err := collector.Collect(context.Background())
		require.Error(t, err, "the listing failure must be reported")

		assert.True(t, report.ListingFailed)
		assert.False(t, report.SweepComplete,
			"the number of rows never reached is UNKNOWN, and claiming completeness on an unknown is "+
				"exactly the failure this signal exists to prevent")
	})

	t.Run("a sweep whose measurement failed is incomplete even though the row was examined", func(t *testing.T) {
		admin := newCollectorFakeAdmin()
		admin.err = errors.New("collector test: broker refused the measurement")

		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(),
			&collectorFakeRegistry{rows: []model.EventSubscriber{collectorSubscriber("blnk.transactions")}},
			nil, admin,
		)

		report, err := collector.Collect(context.Background())
		require.Error(t, err)

		assert.Equal(t, 1, report.SubscribersFailed,
			"a refused measurement is a broker fault and is counted apart from an unprovisioned row, "+
				"which is a registry fault and needs a different action")
		assert.Zero(t, report.SubscribersSkipped)
		assert.False(t, report.SweepComplete,
			"the row was examined and still has no series, so the inventory is incomplete")
	})

	// The quietest gap in the whole inventory, and the one that motivated a reason of its
	// own.
	t.Run("a measured subscriber whose authorised topic does not exist is incomplete", func(t *testing.T) {
		admin := newCollectorFakeAdmin()
		admin.lagByTopic["blnk.transactions"] = 42
		admin.missingTopics["blnk.balances"] = true

		row := collectorSubscriber("blnk.transactions", "blnk.balances")
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(),
			&collectorFakeRegistry{rows: []model.EventSubscriber{row}},
			nil, admin,
		)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err,
			"a missing topic is not a failed collection: the broker answered and the other topic "+
				"measured normally, so nothing here should surface as a collection error")

		assert.Equal(t, 1, report.SubscribersMeasured,
			"the measurement SUCCEEDED, which is exactly why this needs its own count")
		assert.Zero(t, report.SubscribersFailed,
			"a missing topic is a provisioning or registry fault, not a broker refusal, and "+
				"conflating them would send an operator to check broker health")
		assert.Equal(t, 1, report.SubscribersTopicMissing)
		assert.False(t, report.SweepComplete,
			"one of the subscriber's authorised topics has no lag series, so the inventory is not "+
				"complete however healthy the topics that did measure look")
		assert.Equal(t, 1, report.LagSeriesPublished,
			"only the topic that exists yields a series; the missing one contributes none, which is "+
				"the silence this count exists to break")
	})
}

// TestEventMetricsCollector_BoundsEveryDependencyCall is the guard on bounded dependency calls.
//
// The stall itself is not the damage: every gauge holds its last value, so the whole
// event pipeline goes on being SCRAPED AS CURRENT while nothing is being measured, and
// no value in any of those gauges could reveal it.
func TestEventMetricsCollector_BoundsEveryDependencyCall(t *testing.T) {
	outbox := &collectorBlockingOutbox{}
	collector := NewEventMetricsCollector(outbox, nil, nil, nil).
		WithTickBudget(500 * time.Millisecond).
		WithCallBudget(100 * time.Millisecond)

	started := time.Now()
	report, err := collector.Collect(context.Background())
	elapsed := time.Since(started)

	require.Error(t, err, "an abandoned collection must be reported as a failure, not as a zero reading")
	assert.NotEmpty(t, report.Failures)
	assert.Less(t, elapsed, 2*time.Second,
		"the collection must be abandoned on its own deadline; without one a hung dependency freezes "+
			"the refresh of every gauge while they all keep being scraped as current")
	assert.True(t, outbox.sawDeadline(),
		"the deadline must be imposed on the DEPENDENCY CALL, so the call itself unblocks rather "+
			"than the collector merely giving up on a goroutine that never returns")
}

// collectorBlockingOutbox blocks until its context is done, then reports whether that
// context carried a deadline.
//
// It models the dependency failure a timeout cannot be tested without: a connection
// that is accepted and never answered.
type collectorBlockingOutbox struct {
	mu       sync.Mutex
	deadline bool
}

func (o *collectorBlockingOutbox) CountUnresolvedEventOutbox(ctx context.Context) (map[string]int64, error) {
	_, hasDeadline := ctx.Deadline()

	o.mu.Lock()
	o.deadline = hasDeadline
	o.mu.Unlock()

	<-ctx.Done()

	return nil, ctx.Err()
}

func (o *collectorBlockingOutbox) sawDeadline() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.deadline
}

// TestCollectionHealthAlerts_CoverEveryWayTheInventoryCanGoUnmeasured is the guard on
// the measurability half of this file's alerting.
//
// The condition rules fire on numbers.
//
// AN UNSCOPED absent().
func TestCollectionHealthAlerts_CoverEveryWayTheInventoryCanGoUnmeasured(t *testing.T) {
	data := prometheusConfigMapData(t)

	embeddedText, ok := data["blnk-kafka-alerts.yml"].(string)
	require.True(t, ok, "the ConfigMap must carry the alert rules")

	var rules map[string]interface{}
	require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &rules))

	t.Run("the coverage rule names every unmeasured reason and its own action", func(t *testing.T) {
		description := alertAnnotation(t, rules, "SubscriberLagCoverageIncomplete", "description")

		for _, reason := range metrics.SubscriberUnmeasuredReasons() {
			assert.Contains(t, description, reason,
				"reason %q is published by the collector but not explained by the remediation, so an "+
					"operator reading this page would find a value in the metric that the runbook "+
					"does not account for", reason)
		}

		// The metric itself has to be named, or the reason breakdown the branches refer to is
		// unreachable from the notification.
		assert.Contains(t, description, "blnk_kafka_subscribers_unmeasured",
			"the remediation branches on a reason attribute, so it must say which series carries it")

		// The budget branch is the only one whose action is a configuration change, and the
		// variable name is the actionable part.
		assert.Contains(t, description, "RELAY_SUBSCRIBER_METRICS_BUDGET",
			"the budget reason is resolved by raising the budget, so the CANONICAL variable must be "+
				"named — an operator who is handed the alias has to discover the canonical spelling "+
				"before they can find it in .env.example or the ConfigMap")
	})

	t.Run("the absence rule is scoped to the one role that runs a collector", func(t *testing.T) {
		expression := alertExpression(t, rules, "EventMetricsCollectionAbsent")

		assert.Contains(t, expression, `job="blnk-server"`,
			"the collector is server-only, so an unscoped absent() fires for ever on every worker "+
				"target and the rule that distinguishes 'no problems' from 'no collector' gets silenced")
		assert.Contains(t, expression, "absent(",
			"this is the one condition in the file that IS absence, and it must be stated rather "+
				"than inferred from a missing condition series")
	})

	t.Run("the two collection-health rules ask different questions", func(t *testing.T) {
		// A collector whose database is refusing connections ticks perfectly on schedule: the
		// collection age stays near zero throughout and only the success age rises. Pointing
		// both rules at the same gauge would report a healthy monitoring pipeline while every
		// gauge it could not compute went stale, which is the exact failure the pair exists
		// to separate.
		stale := alertExpression(t, rules, "EventMetricsCollectionStale")
		failing := alertExpression(t, rules, "EventMetricsCollectionFailing")

		assert.Contains(t, stale, "blnk_event_metrics_last_collection_age_seconds",
			"'is the loop running' is answered by the collection age")
		assert.Contains(t, failing, "blnk_event_metrics_last_success_age_seconds",
			"'is the loop achieving anything' is answered by the success age, and only by it")
		assert.NotEqual(t, stale, failing,
			"two rules over one gauge would leave a ticking-but-failing collector undetected")
	})

	t.Run("the coverage rule dwells long enough to survive a rotating sweep", func(t *testing.T) {
		// Rotation means a registry larger than the per-tick budget legitimately reports
		// incomplete coverage on most ticks. Anything under half an hour turns that into a
		// standing page on precisely the deployments the rule matters for.
		assert.Equal(t, "30m", alertField(t, rules, "SubscriberLagCoverageIncomplete", "for"),
			"a short dwell fires permanently on any registry the budget cannot cover in one tick")
	})
}

// alertExpression returns the expr of one named alert from a parsed rules document.
//
// Separate from alertAnnotation because the expression is not an annotation, and
// because a rule whose expression this cannot find must fail loudly rather than be
// asserted against "".
//
// Returns:
//   - string: the alert's PromQL expression.
func alertExpression(t *testing.T, rules map[string]interface{}, alert string) string {
	t.Helper()

	return alertField(t, rules, alert, "expr")
}

// alertField returns one top-level string field of one named alert.
//
// Returns:
//   - string: the field's value. The test fails when the alert or the field is absent,
//     so a renamed alert cannot make an assertion pass vacuously.
func alertField(t *testing.T, rules map[string]interface{}, alert, field string) string {
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

			value, ok := declared[field].(string)
			require.True(t, ok, "alert %s must carry a string %s", alert, field)

			return value
		}
	}

	t.Fatalf("no alert named %s is declared", alert)

	return ""
}

// TestRunbookURLs_ResolveToTheRulesOwnProcedure covers F13.
//
// Every runbook_url was `docs/kafka-operations.md`: no scheme, no host, no fragment.
//
// Three properties are asserted, and each covers a different way the link can be
// useless:
//
//   - RESOLVABLE. An absolute https URL, so a notification's reader can open it.
//   - RULE-SPECIFIC. A fragment naming the rule, so it lands on that rule's own
//     section.
//   - ANCHORED IN REALITY. The fragment must correspond to a heading that actually
//     exists in the document, which is the assertion that keeps the link working as the
//     runbook is edited.
func TestRunbookURLs_ResolveToTheRulesOwnProcedure(t *testing.T) {
	root := moduleRootDir(t)
	headings := runbookHeadingAnchors(t, filepath.Join(root, "docs", "kafka-operations.md"))

	rootRules := readYAMLFile(t, filepath.Join(root, "alerts", "blnk-kafka-alerts.yml"))

	embeddedText, ok := prometheusConfigMapData(t)["blnk-kafka-alerts.yml"].(string)
	require.True(t, ok, "the ConfigMap must carry the alert rules")

	var embeddedRules map[string]interface{}
	require.NoError(t, yaml.Unmarshal([]byte(embeddedText), &embeddedRules))

	copies := map[string]map[string]interface{}{
		"alerts/blnk-kafka-alerts.yml":                           rootRules,
		"infrastructure/k8s-manifests/prometheus-configmap.yaml": embeddedRules,
	}

	for name, rules := range copies {
		t.Run(name, func(t *testing.T) {
			alerts := alertNames(t, rules)
			require.NotEmpty(t, alerts)

			for _, alert := range alerts {
				raw := alertAnnotation(t, rules, alert, "runbook_url")

				// The DEFAULT rendering: no runbook_base_url external label configured, which
				// is what an out-of-the-box deployment has.
				rendered := renderPrometheusAnnotation(t, raw, nil)

				assert.True(t, strings.HasPrefix(rendered, "https://"),
					"%s: runbook_url must render to an absolute URL a notification can open, got %q",
					alert, rendered)

				fragment := strings.ToLower(alert)
				assert.True(t, strings.HasSuffix(rendered, "#"+fragment),
					"%s: runbook_url must end in its own fragment #%s so the link lands on that "+
						"rule's procedure rather than at the top of the document, got %q",
					alert, fragment, rendered)

				assert.Contains(t, headings, fragment,
					"%s: the fragment #%s corresponds to no heading in docs/kafka-operations.md, so "+
						"the link degrades to the top of the document — which is the state this "+
						"finding was about", alert, fragment)

				// The OVERRIDE rendering: a deployment that publishes its own runbook copy.
				overridden := renderPrometheusAnnotation(t, raw, map[string]string{
					"runbook_base_url": "https://runbooks.example/blnk",
				})
				assert.Equal(t, "https://runbooks.example/blnk#"+fragment, overridden,
					"%s: a deployment that sets the runbook_base_url external label must get its "+
						"own documentation base, with the rule fragment preserved", alert)
			}
		})
	}
}

// renderPrometheusAnnotation expands an annotation the way Prometheus does.
//
// Prometheus prepends a PRELUDE declaring $labels, $externalLabels, $externalURL and
// $value and then executes the annotation as a text/template.
//
// Parameters:
//   - t *testing.T: the test; a template error is a hard failure.
//   - raw string: the annotation as written in the rule file.
//   - external map[string]string: the external labels. Nil means none configured.
//
// Returns:
//   - string: the expanded annotation.
func renderPrometheusAnnotation(t *testing.T, raw string, external map[string]string) string {
	t.Helper()

	if external == nil {
		external = map[string]string{}
	}

	const prelude = `{{$labels := .Labels}}{{$externalLabels := .ExternalLabels}}` +
		`{{$externalURL := .ExternalURL}}{{$value := .Value}}`

	parsed, err := template.New("annotation").Parse(prelude + raw)
	require.NoError(t, err, "the annotation must be a template Prometheus can parse: %q", raw)

	var built strings.Builder
	require.NoError(t, parsed.Execute(&built, struct {
		Labels         map[string]string
		ExternalLabels map[string]string
		ExternalURL    string
		Value          float64
	}{
		Labels:         map[string]string{},
		ExternalLabels: external,
		ExternalURL:    "http://prometheus:9090",
	}), "the annotation must execute with the variables Prometheus binds: %q", raw)

	return built.String()
}

// runbookHeadingAnchors returns the GitHub-style anchor of every heading in a markdown
// file.
//
// The derivation mirrors GitHub's: lowercase, drop everything that is not alphanumeric,
// space, hyphen or underscore, then replace spaces with hyphens.
//
// Parameters:
//   - t *testing.T: the test; an unreadable document is a hard failure.
//   - path string: the markdown file.
//
// Returns:
//   - map[string]bool: the anchors present.
func runbookHeadingAnchors(t *testing.T, path string) map[string]bool {
	t.Helper()

	body, err := os.ReadFile(path)
	require.NoError(t, err, "%s must be readable to verify the alert fragments resolve", path)

	heading := regexp.MustCompile(`^#{2,4}\s+(.+?)\s*$`)
	strip := regexp.MustCompile(`[^a-z0-9 \-_]`)

	anchors := make(map[string]bool)
	for _, line := range strings.Split(string(body), "\n") {
		match := heading.FindStringSubmatch(line)
		if match == nil {
			continue
		}

		anchor := strings.ReplaceAll(
			strings.TrimSpace(strip.ReplaceAllString(strings.ToLower(match[1]), "")), " ", "-",
		)
		anchors[anchor] = true
	}

	return anchors
}

// alertNames returns every alert name declared in a parsed rules document, in file
// order.
//
// Returns:
//   - []string: the alert names.
func alertNames(t *testing.T, rules map[string]interface{}) []string {
	t.Helper()

	groups, ok := rules["groups"].([]interface{})
	require.True(t, ok, "the rules document must carry groups")

	var names []string
	for _, group := range groups {
		entries, ok := group.(map[string]interface{})
		require.True(t, ok)

		ruleList, ok := entries["rules"].([]interface{})
		require.True(t, ok)

		for _, rule := range ruleList {
			declared, ok := rule.(map[string]interface{})
			require.True(t, ok)

			name, ok := declared["alert"].(string)
			require.True(t, ok, "every rule must declare an alert name")
			names = append(names, name)
		}
	}

	return names
}

// CountEventSubscribers reports the registry size and counts its calls, so a test can
// assert the collector reads it ONCE per tick — the aggregate exists so that sizing the
// registry does not cost more as it grows, and a per-subscriber read would defeat that.
func (r *collectorFakeRegistry) CountEventSubscribers(_ context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalCall++

	if r.totalErr != nil {
		return 0, r.totalErr
	}

	if r.total != nil {
		return *r.total, nil
	}

	return int64(len(r.rows)), nil
}

// TestEventMetricsCollector_PublishesHowMuchOfTheLagSignalIsMissing is the
// measurement-coverage contract.
//
// The budget above stops the enumeration, and that was reported as a BOOLEAN in the
// report and a WARN in the log.
//
// The shortfall is derived from rows actually EXAMINED rather than from the budget
// flag, which makes it cover four distinct situations with one number. Each is a
// different bug if it reports zero, and only the first is a budget exhaustion:
//
//   - the budget stopped the sweep;
//   - the sweep broke early on a repository error;
//   - no broker is configured, so nothing was measurable at all;
//   - no registry is wired, so there is nothing to be unmeasured.
func TestEventMetricsCollector_PublishesHowMuchOfTheLagSignalIsMissing(t *testing.T) {
	t.Run("the budget's shortfall is published as a number, not a boolean", func(t *testing.T) {
		gauges := captureCoverageGauges(t)

		// A registry of 900 served through pages the sweep will stop paging, so the
		// unmeasured population is one the fixtures never have to enumerate. This is the
		// production shape: the rows past the budget are never read.
		rows := make([]model.EventSubscriber, 0, 30)
		for i := 0; i < 30; i++ {
			rows = append(rows, collectorSubscriber("blnk.transactions"))
		}
		total := int64(900)

		registry := &collectorFakeRegistry{rows: rows, total: &total}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(10)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		require.True(t, report.BudgetReached, "the fixture must actually reach the budget")
		assert.Equal(t, int64(900), report.SubscribersRegistered)
		assert.Equal(t, int64(890), report.SubscribersUnmeasured,
			"890 subscribers have no lag series: 900 registered less the 10 the budget allowed")

		assert.Equal(t, []int64{890}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons),
			"the shortfall must reach the gauge SubscriberLagCoverageIncomplete evaluates")
		assert.Equal(t, []int64{900}, gauges.registered.values(),
			"the registry size must accompany it, or 890 cannot be read as a proportion")

		// The LOG still carries it too, with the variable an operator has to change. The
		// metric replaces the log as the ALERTING channel, not as the diagnostic one.
		assert.Contains(t, report.LogFields(), "subscribers_unmeasured")
		assert.Contains(t, report.LogFields(), "subscribers_registered")
	})

	t.Run("complete coverage publishes an explicit zero on every tick", func(t *testing.T) {
		// The zero is the measurement that says the signal is complete, and it has to be
		// written every tick: a gauge only written when something is wrong cannot distinguish
		// a healthy system from a collector that has stopped.
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{rows: []model.EventSubscriber{
			collectorSubscriber("blnk.transactions"),
			collectorSubscriber("blnk.balances"),
		}}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		for tick := 0; tick < 3; tick++ {
			report, err := collector.Collect(context.Background())
			require.NoError(t, err)

			assert.False(t, report.BudgetReached)
			assert.Zero(t, report.SubscribersUnmeasured)
			assert.Equal(t, int64(2), report.SubscribersRegistered)
		}

		assert.Equal(t, []int64{0, 0, 0}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons),
			"zero is published on every tick, not omitted when healthy")
		assert.Equal(t, []int64{2, 2, 2}, gauges.registered.values())
	})

	t.Run("with no broker every registered subscriber is unmeasured", func(t *testing.T) {
		// Reporting zero here would be the same false reassurance in a different disguise: a
		// deployment that has lost its broker has lost its lag signal completely, and that is
		// precisely the condition worth surfacing.
		gauges := captureCoverageGauges(t)

		total := int64(4)
		registry := &collectorFakeRegistry{
			rows:  []model.EventSubscriber{collectorSubscriber("blnk.transactions")},
			total: &total,
		}
		admin := newCollectorFakeAdmin()
		admin.configured = false

		collector := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, admin)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, int64(4), report.SubscribersRegistered)
		assert.Equal(t, int64(4), report.SubscribersUnmeasured,
			"no broker means no offsets to difference, so every registered subscriber is unmeasured")
		assert.Equal(t, []int64{4}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons))
		assert.Equal(t, []int64{4}, gauges.registered.values())

		assert.Empty(t, registry.snapshotPages(),
			"the registry must not be paged when nothing can be measured; the count answers it")
	})

	t.Run("with no registry the pair is an honest zero rather than an absence", func(t *testing.T) {
		gauges := captureCoverageGauges(t)

		collector := NewEventMetricsCollector(newCollectorFakeOutbox(), nil, nil, newCollectorFakeAdmin())

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Zero(t, report.SubscribersRegistered)
		assert.Zero(t, report.SubscribersUnmeasured)
		assert.Equal(t, []int64{0}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons),
			"nothing is registered, so nothing is unmeasured — and saying so is not the same as saying nothing")
		assert.Equal(t, []int64{0}, gauges.registered.values())
	})

	t.Run("a sweep that broke early reports the rows it never reached", func(t *testing.T) {
		// The listing failure and the budget are DIFFERENT CAUSES with the SAME consequence,
		// which is why the number is derived from rows examined rather than from the budget
		// flag. BudgetReached distinguishes the causes; the shortfall states the fact either
		// way.
		gauges := captureCoverageGauges(t)

		total := int64(12)
		registry := &collectorFakeRegistry{err: errors.New("relation does not exist"), total: &total}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		report, err := collector.Collect(context.Background())
		require.Error(t, err, "a listing failure is still reported as a failure")

		assert.False(t, report.BudgetReached, "the budget was not the cause here")
		assert.Equal(t, int64(12), report.SubscribersRegistered)
		assert.Equal(t, int64(12), report.SubscribersUnmeasured,
			"the sweep examined nothing, so every registered subscriber is unmeasured")
		assert.Equal(t, []int64{12}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons),
			"a failed enumeration must publish its coverage gap, not skip publishing")
	})

	t.Run("a failure to size the registry is recorded and stepped past", func(t *testing.T) {
		// The count is an observation ABOUT an observation. Refusing to measure lag because
		// the registry could not be sized would convert a reporting gap into a total one, so
		// the sweep proceeds and the failure is reported.
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{
			rows:     []model.EventSubscriber{collectorSubscriber("blnk.transactions")},
			totalErr: errors.New("statement timeout"),
		}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		report, err := collector.Collect(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "counting event subscribers",
			"the failure must name the read that failed, since the remediation differs from a listing failure")

		assert.Equal(t, 1, report.SubscribersMeasured,
			"lag is still measured for the subscribers the sweep could reach")
		assert.Zero(t, report.SubscribersRegistered, "an unknown registry size is not invented")
		assert.Equal(t, []int64{0}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons),
			"with no size to compare against, no shortfall can be claimed")
	})

	t.Run("a registry that grew mid-sweep never reports a negative shortfall", func(t *testing.T) {
		// The count and the sweep are separate reads, so the registry can shrink between them
		// — or the sweep can examine rows a stale count did not include. A negative shortfall
		// would render as an enormous unsigned value on a gauge and page somebody at the
		// least useful moment.
		gauges := captureCoverageGauges(t)

		rows := make([]model.EventSubscriber, 0, 6)
		for i := 0; i < 6; i++ {
			rows = append(rows, collectorSubscriber("blnk.transactions"))
		}
		stale := int64(2)

		registry := &collectorFakeRegistry{rows: rows, total: &stale}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, 6, report.SubscribersMeasured)
		assert.Zero(t, report.SubscribersUnmeasured, "floored at zero, never negative")
		assert.Equal(t, []int64{0}, gauges.unmeasured.perTickTotals(t, collectorUnmeasuredReasons))
	})

	t.Run("the registry is sized once per tick, not once per subscriber", func(t *testing.T) {
		// The reason it is an aggregate: observing how big the registry is must not cost more
		// as it grows. A per-subscriber read would defeat that and would make the coverage
		// measurement itself the thing that overruns the interval.
		rows := make([]model.EventSubscriber, 0, 8)
		for i := 0; i < 8; i++ {
			rows = append(rows, collectorSubscriber("blnk.transactions"))
		}

		registry := &collectorFakeRegistry{rows: rows}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		for tick := 0; tick < 2; tick++ {
			_, err := collector.Collect(context.Background())
			require.NoError(t, err)
		}

		registry.mu.Lock()
		calls := registry.totalCall
		registry.mu.Unlock()

		assert.Equal(t, 2, calls, "exactly one count per tick, whatever the registry holds")
	})
}

// TestRelayDefaults_AgreeWithTheCollector pins two constants that must hold the same
// value and cannot be one constant.
//
// Their divergence would be silent and would matter in one direction specifically.
func TestRelayDefaults_AgreeWithTheCollector(t *testing.T) {
	// config.ConfigStore is process-global and MockConfig writes it, so it is snapshotted
	// and restored: a leaked configuration would make every later test in this package
	// read this one's.
	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	// The two DSNs are the only required fields; everything else is defaulted, which is what
	// makes this fixture a reading of the DEFAULTS rather than of anything stated here.
	configured := &config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "mock-dns"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
	}
	config.MockConfig(configured)

	// MockConfig returns without storing when validation fails, and it defaults IN PLACE,
	// so a zero budget here would mean the defaults never ran and the comparison below
	// would be vacuous rather than failing.
	require.NotZero(t, configured.Relay.SubscriberMetricsBudget,
		"the configuration defaults must have been applied for this comparison to mean anything")

	require.Equal(t, DefaultSubscriberMetricsBudget, configured.Relay.SubscriberMetricsBudget,
		"config's relay default and the collector's fallback must be the same number: they are two "+
			"declarations of one budget, and only this test keeps them equal")
	assert.Equal(t, 200, DefaultSubscriberMetricsBudget,
		"200 is the documented default in .env.example, blnk-config.yaml, docs/metrics.md and the "+
			"SubscriberLagCoverageIncomplete remediation; changing it means changing all of them")

	t.Run("the configured value is what the collector is wired with", func(t *testing.T) {
		// cmd/server.go passes cfg.Relay.SubscriberMetricsBudget into WithSubscriberBudget.
		// Without the budget being configured, every deployment would run on
		// the built-in 200 with no way to raise it short of a code change — which is why this
		// asserts the plumbing and not only the constants.
		collector := NewEventMetricsCollector(nil, nil, nil, nil).
			WithSubscriberBudget(configured.Relay.SubscriberMetricsBudget)

		assert.Equal(t, 200, collector.subscriberBudget)

		raised := NewEventMetricsCollector(nil, nil, nil, nil).WithSubscriberBudget(5000)
		assert.Equal(t, 5000, raised.subscriberBudget,
			"a raised budget must reach the collector, or the variable is decorative")
	})
}

// TestKafkaAlertInventory_IsStatedOnceAndAgreesEverywhere is the guard.
//
// alerts/blnk-kafka-alerts.yml defines the rules.
//
// The counts were correct when written.
func TestKafkaAlertInventory_IsStatedOnceAndAgreesEverywhere(t *testing.T) {
	root := moduleRootDir(t)

	raw, err := os.ReadFile(filepath.Join(root, "alerts", "blnk-kafka-alerts.yml"))
	require.NoError(t, err, "the alerts file is the authority every other file describes")

	var parsed struct {
		Groups []struct {
			Name  string `yaml:"name"`
			Rules []struct {
				Alert string `yaml:"alert"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &parsed), "parsing the alerts file")

	var rules []string
	for _, group := range parsed.Groups {
		for _, rule := range group.Rules {
			if rule.Alert != "" {
				rules = append(rules, rule.Alert)
			}
		}
	}
	require.NotEmpty(t, rules, "an empty rule set would make every assertion below vacuous")

	count := strconv.Itoa(len(rules))

	readFile := func(elem ...string) string {
		contents, err := os.ReadFile(filepath.Join(append([]string{root}, elem...)...))
		require.NoError(t, err, "reading %v", elem)

		return string(contents)
	}

	t.Run("the runbook indexes every rule, and only real rules", func(t *testing.T) {
		runbook := readFile("docs", "kafka-operations.md")

		for _, rule := range rules {
			// The row an operator arriving from a notification reads.
			assert.Contains(t, runbook, "| `"+rule+"` |",
				"docs/kafka-operations.md must carry an index row for %q; a rule whose notification "+
					"leads to no row leaves the responder with no procedure", rule)

			// And the section that row links to. A row pointing at a missing anchor is a dead end
			// that renders as a working link.
			assert.Contains(t, runbook, "### "+rule+"\n",
				"docs/kafka-operations.md must carry a `### %s` response section, which is what the "+
					"index row anchors to", rule)
		}

		// THE REVERSE DIRECTION, which is how the mis-named entry survived: the index named
		// ConsumerLagCoverageIncomplete, no rule did, and nothing compared the two.
		indexStart := strings.Index(runbook, "| Alert | Response section |")
		require.GreaterOrEqual(t, indexStart, 0,
			"the alert index table must exist; it is the entry point every notification links into")
		indexTable := runbook[indexStart:]
		if end := strings.Index(indexTable, "\n\n"); end >= 0 {
			indexTable = indexTable[:end]
		}

		indexed := regexp.MustCompile(`(?m)^\| \x60([A-Za-z]+)\x60 \|`).
			FindAllStringSubmatch(indexTable, -1)
		require.Len(t, indexed, len(rules),
			"the alert index must have exactly one row per rule: %d rules, %d rows",
			len(rules), len(indexed))

		for _, row := range indexed {
			assert.Contains(t, rules, row[1],
				"docs/kafka-operations.md indexes %q, which is not a rule in alerts/blnk-kafka-alerts.yml. "+
					"Either the rule was renamed and the index was not, or the index invented a name — "+
					"and an operator following it reaches an anchor for an alert that cannot fire",
				row[1])
		}
	})

	t.Run("the environment template states the count and every name", func(t *testing.T) {
		// This file tells an operator what to CONFIRM at /rules after enabling metrics
		// authentication, so a wrong count here is a verification step that passes on a
		// broken deployment and fails on a correct one.
		env := readFile(".env.example")

		assert.Contains(t, env, count+" rules in alerts/blnk-kafka-alerts.yml",
			".env.example must state the real rule count (%s)", count)
		assert.Contains(t, env, count+" rules in the blnk-kafka-alerts group",
			".env.example's confirmation checklist must state the real rule count (%s)", count)

		for _, rule := range rules {
			assert.Contains(t, env, rule,
				".env.example lists the rules to confirm at /rules, so it must name %q; an operator "+
					"checking a partial list against a complete /rules page cannot tell a missing rule "+
					"from an extra one", rule)
		}
	})

	t.Run("every series an alert reads is documented in the metrics reference", func(t *testing.T) {
		// THE OTHER HALF OF names, not counts.
		//
		// Four instruments driving two rules — SubscriberCredentialOrphaned and
		// SubscriberRevocationRefused — were absent from docs/metrics.md entirely.
		//
		// The scope is deliberately "series an ALERT reads" rather than every instrument
		// declared in internal/metrics.
		reference := readFile("docs", "metrics.md")

		// Prometheus renders an OpenTelemetry instrument's dots as underscores, so the alert
		// expressions and the reference both use the underscored form.
		series := regexp.MustCompile(`blnk_[a-z0-9_]+`).FindAllString(string(raw), -1)
		require.NotEmpty(t, series, "the alert expressions must read some blnk_ series")

		seen := map[string]bool{}
		for _, name := range series {
			if seen[name] {
				continue
			}
			seen[name] = true

			assert.Contains(t, reference, name,
				"alerts/blnk-kafka-alerts.yml reads %q, so docs/metrics.md must document it. An "+
					"operator cannot tell a rule that is quiet because nothing is wrong from one that "+
					"is quiet because its series was never recorded", name)
		}
	})

	t.Run("every deployment surface states the same count", func(t *testing.T) {
		// Three files explain what an unmounted rule file or a disarmed metrics registry costs, and
		// each quantifies it. They are read by whoever is deciding whether to skip that step.
		for _, target := range [][]string{
			{"docker-compose.yaml"},
			{"docker-compose.dev.yaml"},
			{"infrastructure", "k8s-manifests", "worker-deployment.yaml"},
		} {
			t.Run(filepath.Join(target...), func(t *testing.T) {
				assert.Contains(t, readFile(target...), count,
					"this file quantifies the alert rules that go inert, so it must state %s", count)
			})
		}
	})
}

// TestMeasurementBudget_IsExportedSoHeadroomIsPortable closes the last hardcoded
// constant in the documented queries.
//
// Coverage headroom is budget minus registry size, and it is the figure to watch
// because it goes negative BEFORE any subscriber goes unmeasured.
func TestMeasurementBudget_IsExportedSoHeadroomIsPortable(t *testing.T) {
	t.Run("the configured budget is published on every tick", func(t *testing.T) {
		budget := captureMeasurementBudgetGauge(t)
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{}
		collector := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			WithSubscriberBudget(1000)

		for range 2 {
			_, err := collector.Collect(context.Background())
			require.NoError(t, err)
		}

		assert.Equal(t, []int64{1000, 1000}, budget.values(),
			"the RAISED budget must be exported, once per tick. A literal in the query instead of "+
				"this series is what showed a deployment running 1000 as exhausted at 200")
		require.Len(t, gauges.registered.values(), 2,
			"the denominator is published on the same ticks, or headroom is a subtraction over two "+
				"different moments")
	})

	t.Run("the default is published when nothing overrode it", func(t *testing.T) {
		budget := captureMeasurementBudgetGauge(t)
		captureCoverageGauges(t)

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), &collectorFakeRegistry{}, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Equal(t, []int64{int64(DefaultSubscriberMetricsBudget)}, budget.values(),
			"an unconfigured collector runs on the default and must say so; publishing a zero would "+
				"report every deployment as permanently out of budget while its sweeps completed")
	})

	t.Run("the documented headroom query subtracts two series", func(t *testing.T) {
		// The QUERIES, with the comment lines around them removed.
		reference := readRepoFile(t, filepath.Join("docs", "metrics.md"))
		section := reference[strings.Index(reference, "## Example Prometheus Queries"):]

		queries := []string{}
		for _, line := range strings.Split(section, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "```") {
				continue
			}
			queries = append(queries, trimmed)
		}
		joined := strings.Join(queries, "\n")
		require.NotEmpty(t, joined, "the examples section must contain queries")

		assert.Contains(t, joined,
			"blnk_subscribers_measurement_budget - blnk_subscribers_registered",
			"docs/metrics.md must document headroom as a difference of the two exported series")

		for _, query := range queries {
			assert.NotContainsf(t, query, "200 - blnk_subscribers_registered",
				"a documented query must not hardcode the default budget: it is configurable, and "+
					"a literal reports false exhaustion on every deployment that raised it — %s",
				query)
		}
	})
}

// TestCollectionFailures_AreAttributedForEveryCollection is the behavioural half of the
// `collection` vocabulary contract.
//
// The settlement and access-residue collectors appended their failure to the report and
// returned, without counting it.
func TestCollectionFailures_AreAttributedForEveryCollection(t *testing.T) {
	t.Run("a failed settlement read is counted under subscriber_settlement", func(t *testing.T) {
		failures := captureCollectionFailures(t)
		captureSettlementGauges(t)

		registry := &collectorFakeRegistry{settlementErr: errors.New("obligations query refused")}

		report, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.Error(t, err,
			"a tick with a failed collection reports it: Collect joins report.Failures into its "+
				"error so a caller cannot act on a partial reading as though it were complete")
		require.NotEmpty(t, report.Failures,
			"and the report carries it too — a caller holding it should not have to scrape metrics "+
				"to learn what its own call observed")

		assert.Contains(t, failures.collections(), collectionSettlement,
			"a settlement read that failed must be counted under %q. The collector publishes no "+
				"gauge on failure, so without this the backlog looks flat rather than unmeasured",
			collectionSettlement)
	})

	t.Run("a failed access-residue read is counted under subscriber_access_residue", func(t *testing.T) {
		failures := captureCollectionFailures(t)
		captureSettlementGauges(t)

		registry := &collectorFakeRegistry{residueErr: errors.New("residue query refused")}

		report, err := NewEventMetricsCollector(newCollectorFakeOutbox(), registry, nil, nil).
			Collect(context.Background())
		require.Error(t, err)
		require.NotEmpty(t, report.Failures)
		assert.False(t, report.ResidueMeasured,
			"the report's own flag must say the residue was not measured, which is what makes the "+
				"four zeroes it did not publish unambiguous")

		assert.Contains(t, failures.collections(), collectionAccessResidue,
			"a failed residue read must be counted under %q: the figures it withholds are "+
				"unaccounted broker access, so an unmeasured reading must not render as a clean one",
			collectionAccessResidue)
	})

	t.Run("a tick with nothing wrong counts nothing", func(t *testing.T) {
		failures := captureCollectionFailures(t)
		captureSettlementGauges(t)

		_, err := NewEventMetricsCollector(newCollectorFakeOutbox(), &collectorFakeRegistry{}, nil, nil).
			Collect(context.Background())
		require.NoError(t, err)

		assert.Empty(t, failures.collections(),
			"a healthy tick must leave this counter alone; an increment on success is a page for a "+
				"dependency that is working")
	})
}

// TestCollectionFailureVocabulary_IsEmittedAndDocumented reconciles the `collection`
// attribute's closed set with the code that emits it and the reference that publishes
// it.
//
// docs/metrics.md named six values.
func TestCollectionFailureVocabulary_IsEmittedAndDocumented(t *testing.T) {
	root := moduleRootDir(t)

	source, err := os.ReadFile(filepath.Join(root, "event_metrics.go"))
	require.NoError(t, err, "the collector is the authority on what it emits")

	// The closed set, read out of the declaration rather than listed, so the two directions
	// below compare the code against itself and against the document.
	declared := regexp.MustCompile(`(?m)^\t(collection[A-Za-z]+)\s+= "([a-z_]+)"$`).
		FindAllStringSubmatch(string(source), -1)
	require.NotEmpty(t, declared,
		"event_metrics.go must declare the collection vocabulary as a const block of fixed "+
			"literals; an empty match would make every assertion below vacuous")

	// The constants themselves, so a renamed literal fails here rather than drifting.
	values := map[string]string{}
	for _, entry := range declared {
		values[entry[1]] = entry[2]
	}
	require.Equal(t, map[string]string{
		"collectionOutboxBacklog":     collectionOutboxBacklog,
		"collectionDeadLetterAge":     collectionDeadLetterAge,
		"collectionRevocations":       collectionRevocations,
		"collectionSubscriberLag":     collectionSubscriberLag,
		"collectionSubscriberListing": collectionSubscriberListing,
		"collectionSettlement":        collectionSettlement,
		"collectionAccessResidue":     collectionAccessResidue,
	}, values,
		"the parsed const block must be the vocabulary this test names; a value added to the block "+
			"has to be emitted and documented, and one removed has to leave the reference")

	t.Run("every declared value is actually emitted", func(t *testing.T) {
		// The half that the documentation could not catch. A constant nothing passes to the
		// recorder is a series that never exists, and an empty query result reads as health.
		for name, value := range values {
			assert.Containsf(t, string(source), "recordCollectionFailure(ctx, "+name+")",
				"event_metrics.go declares %s = %q but never records it. A documented attribute "+
					"value no code emits makes an absent series indistinguishable from a healthy "+
					"one, which is the reading the counter exists to prevent", name, value)
		}
	})

	t.Run("the reference publishes exactly those values", func(t *testing.T) {
		reference, err := os.ReadFile(filepath.Join(root, "docs", "metrics.md"))
		require.NoError(t, err, "the metrics reference is what an operator reads")

		row := ""
		for _, line := range strings.Split(string(reference), "\n") {
			if strings.HasPrefix(line, "| `blnk_event_metrics_collection_failures_total` |") {
				require.Emptyf(t, row,
					"docs/metrics.md must describe blnk_event_metrics_collection_failures_total "+
						"exactly once, or two rows can disagree")
				row = line
			}
		}
		require.NotEmptyf(t, row,
			"docs/metrics.md must carry a row for blnk_event_metrics_collection_failures_total")

		for name, value := range values {
			assert.Containsf(t, row, "`"+value+"`",
				"docs/metrics.md must publish %q (%s) as a value of the `collection` attribute; a "+
					"dependency the counter can attribute but the reference does not name is one "+
					"nobody queries", value, name)
		}

		// THE REVERSE DIRECTION, which is how the two never-emitted values survived: the row
		// named them, no constant did, and nothing compared the two. Every backticked
		// snake_case token in the row that is neither a series name nor the attribute's own
		// name has to be a value of the vocabulary.
		emitted := map[string]bool{}
		for _, value := range values {
			emitted[value] = true
		}

		for _, token := range regexp.MustCompile("\x60([a-z][a-z0-9_]*)\x60").
			FindAllStringSubmatch(row, -1) {
			if strings.HasPrefix(token[1], "blnk_") || token[1] == "collection" {
				continue
			}

			assert.Truef(t, emitted[token[1]],
				"docs/metrics.md names %q as a `collection` value, and no constant in "+
					"event_metrics.go emits it. An operator querying it gets an empty result and "+
					"reads it as the dependency being healthy", token[1])
		}
	})
}

// perTickByReason folds an ATTRIBUTED gauge's writes into one reason-to-value map per
// tick.
//
// perTickTotals answers "how much of the lag signal is missing"; this answers "and
// why", which is the question the coverage alert's remediation branches on.
//
// Parameters:
//   - t *testing.T: fails when the writes do not divide evenly into whole ticks, which
//     would mean a reason was skipped and one tick's values would be read as another's.
//   - reasons int: how many reason labels one tick writes.
//
// Returns:
//   - []map[string]int64: one map per tick, in order, carrying every reason including
//     zeros.
func (g *collectorRecordedInt64Gauge) perTickByReason(t *testing.T, reasons int) []map[string]int64 {
	t.Helper()

	records := g.snapshot()
	require.Zerof(t, len(records)%reasons,
		"every tick must publish all %d reasons, zeros included; got %d writes", reasons, len(records))

	ticks := []map[string]int64{}
	for start := 0; start < len(records); start += reasons {
		tick := map[string]int64{}
		for _, record := range records[start : start+reasons] {
			reason, ok := record.attributes["reason"]
			require.Truef(t, ok, "every unmeasured write must carry its reason; %v does not",
				record.attributes)
			tick[reason] = record.value
		}
		ticks = append(ticks, tick)
	}

	return ticks
}

// TestEventMetricsCollector_AttributesEachCoverageGapToExactlyOneReason is the one-reason-per-gap invariant.
//
// The shortfall was "registered minus the subscribers currently exported", which is the
// count of registry rows with NO SERIES AT ALL — every one of them, whatever the cause.
//
// Two things broke as a result.
func TestEventMetricsCollector_AttributesEachCoverageGapToExactlyOneReason(t *testing.T) {
	t.Run("an unprovisioned subscriber is counted once, under its own reason", func(t *testing.T) {
		// The QA-observed shape, and the natural state immediately after POST /subscribers:
		// two measurable rows and one with no authorised topics yet.
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{rows: []model.EventSubscriber{
			collectorSubscriber("blnk.transactions"),
			collectorSubscriber("blnk.balances"),
			collectorSubscriber(),
		}}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(50)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		require.False(t, report.BudgetReached, "the budget must be nowhere near reached for this to mean anything")
		require.Equal(t, int64(3), report.SubscribersRegistered)
		require.Equal(t, 1, report.SubscribersSkipped, "the row with no topics is the skipped one")

		tick := gauges.unmeasured.perTickByReason(t, collectorUnmeasuredReasons)
		require.Len(t, tick, 1)

		assert.EqualValues(t, 1, tick[0][metrics.SubscribersUnmeasuredReasonUnprovisioned],
			"the gap belongs to the row that has no topics to measure")
		assert.Zero(t, tick[0][metrics.SubscribersUnmeasuredReasonBudget],
			"and NOT to the budget, which had 49 subscribers of headroom: 'budget' is the label "+
				"whose remediation is a configuration change, and it must not be attached to a gap "+
				"configuration cannot close")
		assert.EqualValues(t, 1, report.SubscribersUnmeasured,
			"one unmeasured subscriber is one, not two")
	})

	t.Run("a broker outage attributes the whole gap to measure_failed", func(t *testing.T) {
		// The second QA-observed shape. Both rows are measurable and the broker refuses both,
		// so the registry is entirely unmeasured — for a reason no budget change can fix.
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{rows: []model.EventSubscriber{
			collectorSubscriber("blnk.transactions"),
			collectorSubscriber("blnk.balances"),
		}}
		admin := newCollectorFakeAdmin()
		admin.err = errors.New("broker refused the offset fetch")

		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, admin,
		).WithSubscriberBudget(50)

		// The refusals are REPORTED as well as counted, so Collect returns them joined. The
		// coverage gauge is published either way, which is the property that matters here: a
		// collection that failed must still say how much of the signal is missing.
		report, err := collector.Collect(context.Background())
		require.Error(t, err, "a refused measurement is reported to the caller, not swallowed")

		require.Equal(t, int64(2), report.SubscribersRegistered)
		require.Equal(t, 2, report.SubscribersFailed)

		tick := gauges.unmeasured.perTickByReason(t, collectorUnmeasuredReasons)
		require.Len(t, tick, 1)

		assert.EqualValues(t, 2, tick[0][metrics.SubscribersUnmeasuredReasonMeasureFailed])
		assert.Zero(t, tick[0][metrics.SubscribersUnmeasuredReasonBudget],
			"a broker or ACL fault must never be reported as a budget shortfall: the remedies are "+
				"different and only one of them works")
		assert.EqualValues(t, 2, report.SubscribersUnmeasured,
			"two subscribers are unmeasured, not four")
	})

	t.Run("the budget keeps the residue it is genuinely responsible for", func(t *testing.T) {
		// The widening must not empty the label of meaning. A registry larger than one tick
		// can measure DOES have a rotation gap, and it belongs to 'budget' — alongside, not
		// instead of, the rows this tick explained for itself.
		gauges := captureCoverageGauges(t)

		rows := []model.EventSubscriber{collectorSubscriber(), collectorSubscriber()}
		for i := 0; i < 28; i++ {
			rows = append(rows, collectorSubscriber("blnk.transactions"))
		}
		total := int64(900)

		registry := &collectorFakeRegistry{rows: rows, total: &total}
		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, newCollectorFakeAdmin(),
		).WithSubscriberBudget(10)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		require.True(t, report.BudgetReached, "the fixture must actually reach the budget")
		require.Equal(t, int64(900), report.SubscribersRegistered)
		require.Equal(t, 2, report.SubscribersSkipped)

		tick := gauges.unmeasured.perTickByReason(t, collectorUnmeasuredReasons)
		require.Len(t, tick, 1)

		assert.EqualValues(t, 2, tick[0][metrics.SubscribersUnmeasuredReasonUnprovisioned])
		assert.EqualValues(t, 890, tick[0][metrics.SubscribersUnmeasuredReasonBudget],
			"900 registered less the 8 measured and the 2 explained: the rotation has not reached "+
				"the rest, which is exactly what raising the budget fixes")
		assert.EqualValues(t, 892, report.SubscribersUnmeasured,
			"and the total is the sum of the two real populations, still inside the registry")
	})

	t.Run("a missing topic is attributed without inventing a budget gap", func(t *testing.T) {
		// The shape that makes the floor load-bearing. A subscriber whose OTHER topics are
		// measurable is both COVERED and explained as topic_missing, so the subtraction goes
		// negative and a naive expression would report a negative or wrapped budget figure.
		gauges := captureCoverageGauges(t)

		registry := &collectorFakeRegistry{rows: []model.EventSubscriber{
			collectorSubscriber("blnk.transactions", "blnk.balances"),
			collectorSubscriber("blnk.identities"),
		}}
		admin := newCollectorFakeAdmin()
		admin.missingTopics["blnk.balances"] = true

		collector := NewEventMetricsCollector(
			newCollectorFakeOutbox(), registry, nil, admin,
		).WithSubscriberBudget(50)

		report, err := collector.Collect(context.Background())
		require.NoError(t, err)

		require.Equal(t, int64(2), report.SubscribersRegistered)
		require.Equal(t, 1, report.SubscribersTopicMissing)
		require.Equal(t, 2, report.SubscribersMeasured, "both rows were measured; one names an absent topic")

		tick := gauges.unmeasured.perTickByReason(t, collectorUnmeasuredReasons)
		require.Len(t, tick, 1)

		assert.EqualValues(t, 1, tick[0][metrics.SubscribersUnmeasuredReasonTopicMissing])
		assert.Zero(t, tick[0][metrics.SubscribersUnmeasuredReasonBudget],
			"the floor holds: a gap that is both covered and explained must not become a budget claim")
		assert.EqualValues(t, 1, report.SubscribersUnmeasured)
	})
}

// TestEventMetricsCollector_NeverReportsMoreUnmeasuredSubscribersThanExist states
// the one-reason-per-gap rule as the invariant rather than as a set of cases, because that is the property
// the gauge's own definition asserts and the one a future change would have to break to
// reintroduce the defect.
func TestEventMetricsCollector_NeverReportsMoreUnmeasuredSubscribersThanExist(t *testing.T) {
	for name, build := range map[string]func() (*collectorFakeRegistry, *collectorFakeAdmin, int){
		"every row unprovisioned": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			return &collectorFakeRegistry{rows: []model.EventSubscriber{
				collectorSubscriber(), collectorSubscriber(), collectorSubscriber(),
			}}, newCollectorFakeAdmin(), 50
		},
		"a mix of unprovisioned and measurable rows": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			return &collectorFakeRegistry{rows: []model.EventSubscriber{
				collectorSubscriber("blnk.transactions"),
				collectorSubscriber(),
				collectorSubscriber("blnk.balances"),
				collectorSubscriber(),
			}}, newCollectorFakeAdmin(), 50
		},
		"every measurement refused": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			admin := newCollectorFakeAdmin()
			admin.err = errors.New("broker refused the offset fetch")

			return &collectorFakeRegistry{rows: []model.EventSubscriber{
				collectorSubscriber("blnk.transactions"), collectorSubscriber("blnk.balances"),
			}}, admin, 50
		},
		"refusals beside unprovisioned rows": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			admin := newCollectorFakeAdmin()
			admin.err = errors.New("broker refused the offset fetch")

			return &collectorFakeRegistry{rows: []model.EventSubscriber{
				collectorSubscriber("blnk.transactions"), collectorSubscriber(),
			}}, admin, 50
		},
		"missing topics beside unprovisioned rows": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			admin := newCollectorFakeAdmin()
			admin.missingTopics["blnk.balances"] = true

			return &collectorFakeRegistry{rows: []model.EventSubscriber{
				collectorSubscriber("blnk.transactions", "blnk.balances"),
				collectorSubscriber("blnk.balances"),
				collectorSubscriber(),
			}}, admin, 50
		},
		"a budget smaller than the registry, with explained gaps inside it": func() (*collectorFakeRegistry, *collectorFakeAdmin, int) {
			rows := []model.EventSubscriber{collectorSubscriber(), collectorSubscriber()}
			for i := 0; i < 10; i++ {
				rows = append(rows, collectorSubscriber("blnk.transactions"))
			}
			total := int64(60)

			return &collectorFakeRegistry{rows: rows, total: &total}, newCollectorFakeAdmin(), 6
		},
	} {
		t.Run(name, func(t *testing.T) {
			gauges := captureCoverageGauges(t)
			registry, admin, budget := build()

			collector := NewEventMetricsCollector(
				newCollectorFakeOutbox(), registry, nil, admin,
			).WithSubscriberBudget(budget)

			// The error is deliberately not asserted either way: half of these shapes fail
			// measurements and half do not, and the invariant is a property of the PUBLISHED
			// gauge on every one of them — including, especially, the ticks that failed.
			report, _ := collector.Collect(context.Background())

			tick := gauges.unmeasured.perTickByReason(t, collectorUnmeasuredReasons)
			require.Len(t, tick, 1)

			var exported int64
			for _, value := range tick[0] {
				assert.GreaterOrEqual(t, value, int64(0), "no reason may carry a negative count")
				exported += value
			}

			assert.LessOrEqual(t, exported, report.SubscribersRegistered,
				"Σ(unmeasured) must never exceed the registry it is a subset of; %d against %d "+
					"means at least one gap was counted twice", exported, report.SubscribersRegistered)
			assert.Equal(t, report.SubscribersUnmeasured, exported,
				"and the report's total must be the exported sum, so the log line and the series "+
					"cannot disagree about how much is missing")
		})
	}
}
