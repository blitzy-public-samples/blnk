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
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeSunsetDate publishes a configuration carrying raw as the webhook sunset date
// and restores whatever configuration was in place when the test finishes.
func storeSunsetDate(t *testing.T, raw string) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		// Nothing was published before this test. Leaving the test's sunset date in place
		// could influence later tests, so publish an empty configuration — strictly closer to
		// the original state than a configured sunset date.
		config.ConfigStore.Store(&config.Configuration{})
	})

	sunsetParseWarnings.reset()
	startParseWarnings.reset()
	config.ConfigStore.Store(&config.Configuration{WebhookDeprecationSunsetDate: raw})
}

// storeDeprecationWindow publishes BOTH ends of the dual-delivery window for one test
// and restores whatever was there before.
//
// The pair is written verbatim rather than validated here. config refuses a window that
// is not exactly 30 days when it LOADS one; this writes to the store directly, exactly
// as storeSunsetDate does, so a test can also pin a deliberately inconsistent pair.
//
// Parameters:
//   - t *testing.T: the test, for the restore.
//   - start time.Time: the instant the window opens.
//   - sunset time.Time: the instant it closes.
func storeDeprecationWindow(t *testing.T, start, sunset time.Time) {
	t.Helper()

	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})

	sunsetParseWarnings.reset()
	startParseWarnings.reset()
	config.ConfigStore.Store(&config.Configuration{
		WebhookDeprecationStartDate:  start.UTC().Format(time.RFC3339),
		WebhookDeprecationSunsetDate: sunset.UTC().Format(time.RFC3339),
	})
}

// restoreFetchConfiguration snapshots the package's configuration seam and puts it back
// when the test finishes.
//
// Tests that need a configuration shape the global store cannot hold — brokers
// configured alongside a deliberately unusable sunset window, which
// validateAndAddDefaults refuses outright — swap the seam instead of the store. The
// snapshot is mandatory: a leaked stub would make every later test in the package read
// its configuration.
func restoreFetchConfiguration(t *testing.T) {
	t.Helper()

	original := fetchConfiguration
	t.Cleanup(func() { fetchConfiguration = original })
}

// mustParseSunset parses an RFC3339 instant that the test itself controls, so a
// failure to parse is a defect in the test rather than in the code under test.
func mustParseSunset(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err, "test fixture %q must be a valid RFC3339 instant", value)

	return parsed
}

func TestWebhookSunsetPassed_UnsetDateHasNotPassed(t *testing.T) {
	storeSunsetDate(t, "")

	// The safe default: with no sunset configured, dual delivery keeps running and the
	// deprecated webhook routes keep answering normally. Probing instants far apart proves
	// the answer does not depend on the clock at all.
	assert.False(t, WebhookSunsetPassed(time.Now()))
	assert.False(t, WebhookSunsetPassed(time.Unix(0, 0).UTC()))
	assert.False(t, WebhookSunsetPassed(time.Date(2999, time.December, 31, 23, 59, 59, 0, time.UTC)))
}

func TestWebhookSunsetPassed_WhitespaceOnlyDateHasNotPassed(t *testing.T) {
	for _, raw := range []string{" ", "\t", "\n", "  \t\n "} {
		storeSunsetDate(t, raw)

		assert.False(t, WebhookSunsetPassed(time.Now()),
			"whitespace-only value %q must be treated as unset", raw)

		date, configured := WebhookSunsetDate()
		assert.False(t, configured, "whitespace-only value %q must not count as configured", raw)
		assert.True(t, date.IsZero())
	}
}

func TestWebhookSunsetPassed_PastDateHasPassed(t *testing.T) {
	storeSunsetDate(t, "2020-01-01T00:00:00Z")

	assert.True(t, WebhookSunsetPassed(time.Now()))
	assert.True(t, WebhookSunsetPassed(mustParseSunset(t, "2020-01-01T00:00:01Z")))
}

func TestWebhookSunsetPassed_FutureDateHasNotPassed(t *testing.T) {
	storeSunsetDate(t, "2999-01-01T00:00:00Z")

	assert.False(t, WebhookSunsetPassed(time.Now()))
	assert.False(t, WebhookSunsetPassed(mustParseSunset(t, "2998-12-31T23:59:59Z")))
}

// TestWebhookSunsetPassed_BoundaryIsInclusiveToTheNanosecond pins the exact semantics
// of the sunset instant. The instant itself belongs to the post-sunset era, so the
// dual-delivery window is half-open: it ends at, and excludes, the configured instant.
func TestWebhookSunsetPassed_BoundaryIsInclusiveToTheNanosecond(t *testing.T) {
	const raw = "2026-06-15T12:30:45Z"
	storeSunsetDate(t, raw)

	sunset := mustParseSunset(t, raw)

	assert.False(t, WebhookSunsetPassed(sunset.Add(-time.Nanosecond)),
		"one nanosecond before the sunset instant the sunset has NOT passed")
	assert.True(t, WebhookSunsetPassed(sunset),
		"at the sunset instant the sunset HAS passed")
	assert.True(t, WebhookSunsetPassed(sunset.Add(time.Nanosecond)),
		"one nanosecond after the sunset instant the sunset HAS passed")
}

// TestWebhookSunsetPassed_ThirtyDayDualDeliveryWindowBoundary checks the predicate for
// a date configured 30 days out: the legacy transport is in use from the moment the
// window opens until the instant before the sunset, and retired from the sunset on.
// Nothing in the code records the opening instant or enforces the interval —
// configuring the date 30 days ahead is the operator's responsibility — so what is
// verified here is the boundary behaviour of a correctly configured date, not duration
// enforcement.
func TestWebhookSunsetPassed_ThirtyDayDualDeliveryWindowBoundary(t *testing.T) {
	windowOpens := mustParseSunset(t, "2026-01-01T00:00:00Z")
	sunset := windowOpens.Add(30 * 24 * time.Hour)

	storeSunsetDate(t, sunset.Format(time.RFC3339))

	assert.False(t, WebhookSunsetPassed(windowOpens),
		"dual delivery is active when the window opens")
	assert.False(t, WebhookSunsetPassed(sunset.Add(-time.Nanosecond)),
		"dual delivery is still active for the final nanosecond of the 30 days")
	assert.True(t, WebhookSunsetPassed(sunset),
		"the sunset lands exactly 30 days after the window opened")
	assert.True(t, WebhookSunsetPassed(sunset.Add(24*time.Hour)),
		"the sunset stays passed afterwards")
}

// TestWebhookSunsetPassed_ZoneSpellingDoesNotChangeTheVerdict asserts that the same
// instant written with a UTC offset and written as Z are indistinguishable, and that
// the zone of the caller's clock is equally irrelevant.
func TestWebhookSunsetPassed_ZoneSpellingDoesNotChangeTheVerdict(t *testing.T) {
	const zulu = "2026-03-01T00:00:00Z"
	const offset = "2026-03-01T01:00:00+01:00" // the very same instant
	const negativeOffset = "2026-02-28T19:00:00-05:00"

	sunset := mustParseSunset(t, zulu)
	require.True(t, sunset.Equal(mustParseSunset(t, offset)),
		"test fixtures must denote one instant")
	require.True(t, sunset.Equal(mustParseSunset(t, negativeOffset)),
		"test fixtures must denote one instant")

	probes := []time.Time{
		sunset.Add(-time.Nanosecond),
		sunset,
		sunset.Add(time.Nanosecond),
	}

	// Every spelling of the sunset date must produce the same verdict at every probe.
	var reference []bool
	for _, raw := range []string{zulu, offset, negativeOffset} {
		storeSunsetDate(t, raw)

		verdicts := make([]bool, 0, len(probes))
		for _, probe := range probes {
			verdicts = append(verdicts, WebhookSunsetPassed(probe))
		}

		if reference == nil {
			reference = verdicts
			assert.Equal(t, []bool{false, true, true}, verdicts,
				"the boundary must be inclusive for spelling %q", raw)

			continue
		}
		assert.Equal(t, reference, verdicts, "spelling %q must behave identically", raw)
	}

	// The zone the caller's own clock is expressed in must not matter either.
	storeSunsetDate(t, zulu)
	berlin := time.FixedZone("CET", int(time.Hour/time.Second))
	assert.False(t, WebhookSunsetPassed(sunset.Add(-time.Nanosecond).In(berlin)))
	assert.True(t, WebhookSunsetPassed(sunset.In(berlin)))
}

func TestWebhookSunsetPassed_TrimsSurroundingWhitespace(t *testing.T) {
	const canonical = "2026-06-15T12:30:45Z"
	sunset := mustParseSunset(t, canonical)

	// A trailing newline or stray space is a routine environment-file mistake. The
	// configuration loader does not trim this field, so it must be tolerated here or a
	// correct date would be read as malformed.
	for _, raw := range []string{" " + canonical, canonical + "\n", "\t" + canonical + " \n"} {
		storeSunsetDate(t, raw)

		assert.False(t, WebhookSunsetPassed(sunset.Add(-time.Nanosecond)), "value %q", raw)
		assert.True(t, WebhookSunsetPassed(sunset), "value %q", raw)

		date, configured := WebhookSunsetDate()
		require.True(t, configured, "value %q must be recognised after trimming", raw)
		assert.True(t, sunset.Equal(date))
	}
}

// TestWebhookSunsetPassed_MalformedDateHasNotPassedWithoutPanic covers the whole
// family of values that are present but unusable. Every one of them must resolve to
// "not passed" and none of them may panic or exit.
func TestWebhookSunsetPassed_MalformedDateHasNotPassedWithoutPanic(t *testing.T) {
	malformed := []string{
		"2026-13-45T99:99:99Z", // structurally plausible, every component out of range
		"2026-02-30T00:00:00Z", // a day that does not exist
		"2026-06-15",           // a date with no time or offset
		"12:30:45Z",            // a time with no date
		"2026-06-15T12:30:45",  // RFC3339 requires an offset
		"2026-06-15 12:30:45Z", // space instead of the T separator
		"1781248800",           // a Unix timestamp
		"tomorrow",
		"null",
		"-",
	}

	for _, raw := range malformed {
		storeSunsetDate(t, raw)

		assert.NotPanics(t, func() {
			assert.False(t, WebhookSunsetPassed(time.Now()), "value %q", raw)
			assert.False(t, WebhookSunsetPassed(time.Date(2999, time.January, 1, 0, 0, 0, 0, time.UTC)),
				"value %q must not pass even for a far-future clock", raw)
			assert.False(t, WebhookSunsetPassedNow(), "value %q", raw)

			date, configured := WebhookSunsetDate()
			assert.False(t, configured, "value %q must not count as configured", raw)
			assert.True(t, date.IsZero(), "value %q must yield the zero instant", raw)
		}, "value %q must never panic", raw)
	}
}

// TestWebhookSunsetPassed_MalformedDateWarnsThroughThePublicPath asserts the log
// contract on the path production actually takes.
func TestWebhookSunsetPassed_MalformedDateWarnsThroughThePublicPath(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	// Structurally plausible, every component out of range — the shape of a real typo.
	const raw = "2026-13-45T99:99:99Z"
	storeSunsetDate(t, raw) // also resets the warn suppressor

	require.False(t, WebhookSunsetPassed(time.Now()),
		"a malformed date must leave the sunset un-passed")

	var warning *logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "not a valid RFC3339 instant") {
			warning = entry

			break
		}
	}
	require.NotNil(t, warning,
		"a malformed sunset date must be reported through the public predicate, not swallowed")

	assert.Equal(t, raw, warning.Data["value"],
		"the warning must name the offending value")
	assert.Equal(t, webhookSunsetLayout, warning.Data["expected"],
		"the warning must name the layout the value failed to match")
	// The parse error travels in "cause" rather than logrus's own "error" key, because
	// every dependency error in this package is logged through withLoggableCause: it
	// sanitizes and bounds the text and redacts network topology, and it attaches the
	// verbatim rendering only when the logger is at debug. WithError does none of that.
	assert.NotEmpty(t, warning.Data["cause"],
		"the warning must carry the parse error itself, through the sanitized cause field")
	assert.Nil(t, warning.Data[logrus.ErrorKey],
		"logrus's raw error field is what rendered a dependency error verbatim; it must not return")
	// The consequence, which is what an operator acts on. With no Kafka broker configured
	// there is no dual-delivery window to end, so the stated consequence is that the
	// deprecated routes keep answering.
	assert.Contains(t, warning.Message, "no Kafka broker is configured",
		"the warning must say why the malformed value is being ignored rather than failing closed")
	assert.Contains(t, warning.Message, "keep answering normally",
		"the warning must state the consequence, which is what an operator acts on")
}

// TestWebhookSunsetInstant_MalformedDateWarnsOncePerDistinctValue verifies both
// halves of the log contract: a malformed value is reported, and it is reported once
// rather than on every call, because the predicate sits on two hot paths.
func TestWebhookSunsetInstant_MalformedDateWarnsOncePerDistinctValue(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	sunsetParseWarnings.reset()

	countWarnings := func(fragment string) int {
		matches := 0
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, fragment) {
				matches++
			}
		}

		return matches
	}

	const fragment = "not a valid RFC3339 instant"

	// No brokers configured, so a malformed value is a WARNING and resolves to "no
	// transport to migrate to". The fail-closed path is asserted separately, by
	// TestWebhookSunsetInstant_FailsClosedWhenPublishingWithoutAUsableWindow.
	first := &config.Configuration{WebhookDeprecationSunsetDate: "not-a-date"}
	for i := 0; i < 5; i++ {
		date, resolution := webhookSunsetInstant(first)
		assert.Equal(t, sunsetAbsentNoTransport, resolution)
		assert.True(t, date.IsZero())
	}
	assert.Equal(t, 1, countWarnings(fragment),
		"five calls with one malformed value must warn exactly once")

	// A different malformed value is new information and must warn again.
	second := &config.Configuration{WebhookDeprecationSunsetDate: "also-not-a-date"}
	_, resolution := webhookSunsetInstant(second)
	assert.Equal(t, sunsetAbsentNoTransport, resolution)
	assert.Equal(t, 2, countWarnings(fragment),
		"a changed malformed value must warn again")

	// Well-formed and absent values must never warn.
	hook.Reset()
	_, resolution = webhookSunsetInstant(&config.Configuration{WebhookDeprecationSunsetDate: "2026-06-15T12:30:45Z"})
	assert.Equal(t, sunsetResolved, resolution)
	_, resolution = webhookSunsetInstant(&config.Configuration{})
	assert.Equal(t, sunsetAbsentNoTransport, resolution)
	assert.Zero(t, countWarnings(fragment), "valid and absent values must not warn")
}

func TestWebhookSunsetInstant_NilConfigurationIsNotConfigured(t *testing.T) {
	date, resolution := webhookSunsetInstant(nil)

	assert.Equal(t, sunsetAbsentNoTransport, resolution,
		"no configuration means no brokers either, so there is nothing to fail closed about")
	assert.True(t, date.IsZero())
}

// TestWebhookSunsetInstant_FailsClosedWhenPublishingWithoutAUsableWindow pins the rule
// that the sunset fails CLOSED.
//
// The distinguishing fact is whether a Kafka transport exists.
func TestWebhookSunsetInstant_FailsClosedWhenPublishingWithoutAUsableWindow(t *testing.T) {
	unusable := []struct {
		name   string
		sunset string
	}{
		{name: "an unset window", sunset: ""},
		{name: "a whitespace-only window", sunset: "   \n"},
		{name: "a malformed window", sunset: "not-a-date"},
		{name: "a date-only window", sunset: "2026-09-04"},
	}

	for _, tc := range unusable {
		t.Run(tc.name, func(t *testing.T) {
			sunsetParseWarnings.reset()

			t.Run("fails closed when brokers are configured", func(t *testing.T) {
				cnf := &config.Configuration{
					Kafka:                        config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
					WebhookDeprecationSunsetDate: tc.sunset,
				}

				_, resolution := webhookSunsetInstant(cnf)
				assert.Equal(t, sunsetUnusableWithTransport, resolution,
					"an unusable window with a Kafka transport configured must fail closed")
			})

			t.Run("stays open when no broker is configured", func(t *testing.T) {
				cnf := &config.Configuration{WebhookDeprecationSunsetDate: tc.sunset}

				_, resolution := webhookSunsetInstant(cnf)
				assert.Equal(t, sunsetAbsentNoTransport, resolution,
					"with no Kafka transport there is nothing to have migrated to, so the sunset has not passed")
			})
		})
	}
}

// TestWebhookSunsetPassed_FailsClosedThroughThePublicPredicate carries the fail-closed
// behaviour all the way through the exported predicate both consumers call, so that the
// relay's dual-delivery branch and the 410 guard are proven to see it — not just the
// internal resolver.
func TestWebhookSunsetPassed_FailsClosedThroughThePublicPredicate(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()

	fetchConfiguration = func() (*config.Configuration, error) {
		return &config.Configuration{
			Kafka:                        config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
			WebhookDeprecationSunsetDate: "not-a-date",
		}, nil
	}

	assert.True(t, WebhookSunsetPassed(time.Now()),
		"the sunset must read as passed so dual delivery stops and the deprecated routes answer 410")
	assert.True(t, WebhookSunsetPassed(time.Unix(0, 0)),
		"the fail-closed verdict cannot depend on the clock")

	// There is no instant to describe, which callers rendering a Sunset header must be
	// able to tell apart from the verdict itself.
	date, configured := WebhookSunsetDate()
	assert.False(t, configured, "there is no parseable instant to report")
	assert.True(t, date.IsZero())
}

// TestWebhookSunsetPassed_UnloadedConfigurationHasNotPassed exercises the branch
// taken when the configuration store has never been populated. The store is an
// atomic.Value and cannot be emptied once written, so the read is swapped out for
// one that fails the way config.Fetch does in that state.
func TestWebhookSunsetPassed_UnloadedConfigurationHasNotPassed(t *testing.T) {
	original := fetchConfiguration
	t.Cleanup(func() { fetchConfiguration = original })

	fetchConfiguration = func() (*config.Configuration, error) {
		return nil, errors.New("config not loaded from file")
	}

	assert.False(t, WebhookSunsetPassed(time.Now()))
	assert.False(t, WebhookSunsetPassed(time.Date(2999, time.January, 1, 0, 0, 0, 0, time.UTC)))
	assert.False(t, WebhookSunsetPassedNow())

	date, configured := WebhookSunsetDate()
	assert.False(t, configured)
	assert.True(t, date.IsZero())
}

// TestWebhookSunsetPassed_HonoursRepublishedConfiguration is the anti-caching test.
// The verdict must be recomputed from live configuration on every call, because
// config.ConfigStore is re-published whenever configuration is loaded.
func TestWebhookSunsetPassed_HonoursRepublishedConfiguration(t *testing.T) {
	probe := mustParseSunset(t, "2026-06-15T12:30:45Z")

	storeSunsetDate(t, "2999-01-01T00:00:00Z")
	require.False(t, WebhookSunsetPassed(probe), "a future sunset has not passed")

	// Re-publish with a sunset already behind the probe instant. A cached verdict, or
	// a cached parse, would keep answering false here.
	storeSunsetDate(t, "2020-01-01T00:00:00Z")
	assert.True(t, WebhookSunsetPassed(probe), "the republished sunset must be honoured")

	// And back again, so the test cannot pass by latching in one direction.
	storeSunsetDate(t, "2999-01-01T00:00:00Z")
	assert.False(t, WebhookSunsetPassed(probe))
}

func TestWebhookSunsetDate_ReturnsTheParsedInstantInUTC(t *testing.T) {
	const raw = "2026-03-01T01:00:00+01:00"
	storeSunsetDate(t, raw)

	date, configured := WebhookSunsetDate()

	require.True(t, configured)
	assert.True(t, mustParseSunset(t, raw).Equal(date), "the same instant must come back")
	assert.Equal(t, time.UTC, date.Location(), "the instant must be normalised to UTC")
	assert.Equal(t, "2026-03-01T00:00:00Z", date.Format(time.RFC3339),
		"an offset spelling must be normalised on the way out")
}

func TestWebhookSunsetDate_UnsetDateIsNotConfigured(t *testing.T) {
	storeSunsetDate(t, "")

	date, configured := WebhookSunsetDate()

	assert.False(t, configured)
	assert.True(t, date.IsZero())
}

// TestWebhookSunsetSnapshotAt_AnswersBothQuestionsFromOneResolution is the one-resolution rule.
//
// The HTTP guard needs two things — a Sunset header advertising the date, and a verdict
// deciding whether to answer 410 — and it obtained them from two independent calls,
// WebhookSunsetDate then WebhookSunsetPassed.
func TestWebhookSunsetSnapshotAt_AnswersBothQuestionsFromOneResolution(t *testing.T) {
	const raw = "2026-06-15T12:30:45Z"
	sunset := mustParseSunset(t, raw)
	storeSunsetDate(t, raw)

	t.Run("the date and the verdict describe the same instant", func(t *testing.T) {
		before := WebhookSunsetSnapshotAt(sunset.Add(-time.Nanosecond))
		require.True(t, before.DateConfigured)
		assert.True(t, sunset.Equal(before.Date), "the resolved instant is reported as itself")
		assert.False(t, before.Passed, "one nanosecond before, the window is still open")

		at := WebhookSunsetSnapshotAt(sunset)
		assert.True(t, sunset.Equal(at.Date), "the same date")
		assert.True(t, at.Passed,
			"and the verdict flips AT the instant, inclusively — the same boundary "+
				"WebhookSunsetPassed applies, because it is the same comparison")
	})

	t.Run("it agrees with the two predicates it replaces", func(t *testing.T) {
		// The point is not that a third answer exists, but that it is the SAME answer
		// obtained once. Any divergence here would mean the guard had begun deciding on a
		// rule of its own.
		for _, probe := range []time.Time{
			sunset.Add(-time.Hour), sunset.Add(-time.Nanosecond), sunset, sunset.Add(time.Hour),
		} {
			snapshot := WebhookSunsetSnapshotAt(probe)

			assert.Equal(t, WebhookSunsetPassed(probe), snapshot.Passed,
				"the verdict must be the authoritative predicate's, at %s", probe)

			date, configured := WebhookSunsetDate()
			assert.Equal(t, configured, snapshot.DateConfigured)
			assert.True(t, date.Equal(snapshot.Date))
		}
	})

	t.Run("an unset window is renderable-as-nothing and has not passed", func(t *testing.T) {
		// No transport and no window: a legitimate steady state, so the routes keep
		// answering and there is no date to advertise.
		storeSunsetDate(t, "")

		snapshot := WebhookSunsetSnapshotAt(time.Now())
		assert.False(t, snapshot.DateConfigured, "there is no instant to render")
		assert.True(t, snapshot.Date.IsZero())
		assert.False(t, snapshot.Passed)
	})

	t.Run("DateConfigured is not the verdict, and the fail-closed state proves it", func(t *testing.T) {
		// THE PAIR THAT MUST NOT BE CONFLATED. A deployment publishing to Kafka with an
		// unusable window has NO instant to advertise and IS past the sunset, because that
		// state fails closed. A guard that took its verdict from DateConfigured would keep
		// the retired surface answering on exactly the deployment that had already moved.
		restoreFetchConfiguration(t)
		sunsetParseWarnings.reset()

		fetchConfiguration = func() (*config.Configuration, error) {
			return &config.Configuration{
				Kafka:                        config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
				WebhookDeprecationSunsetDate: "not-a-date",
			}, nil
		}

		snapshot := WebhookSunsetSnapshotAt(time.Now())
		assert.False(t, snapshot.DateConfigured, "nothing renderable")
		assert.True(t, snapshot.Passed, "and yet the sunset has passed")
	})
}

// TestWebhookSunsetPassedNow_DelegatesToTheParameterisedPredicate confirms the
// convenience wrapper is exactly the parameterised predicate evaluated against the
// current clock, and holds no independent comparison of its own.
func TestWebhookSunsetPassedNow_DelegatesToTheParameterisedPredicate(t *testing.T) {
	storeSunsetDate(t, "2020-01-01T00:00:00Z")
	assert.True(t, WebhookSunsetPassedNow())
	assert.Equal(t, WebhookSunsetPassed(time.Now()), WebhookSunsetPassedNow())

	storeSunsetDate(t, "2999-01-01T00:00:00Z")
	assert.False(t, WebhookSunsetPassedNow())
	assert.Equal(t, WebhookSunsetPassed(time.Now()), WebhookSunsetPassedNow())
}

// TestWebhookSunsetPassed_BothConsumersFlipAtTheSameInstant pins the invariant that
// gives this decision point its reason to exist.
func TestWebhookSunsetPassed_BothConsumersFlipAtTheSameInstant(t *testing.T) {
	const raw = "2026-06-15T12:30:45Z"
	storeSunsetDate(t, raw)

	sunset := mustParseSunset(t, raw)

	// The relay's dual-delivery branch, through the predicate the relay ACTUALLY calls
	// rather than a negation composed here. Only the sunset is configured, so the window
	// start is derived as sunset minus 30 days — which is why the earliest probe below
	// sits exactly on the start rather than before it.
	relayDualDelivers := WebhookDualDeliveryActive
	// The API guard's decision: the deprecated webhook routes answer 410 Gone from the
	// sunset instant onwards.
	webhookRoutesAreGone := func(now time.Time) bool { return WebhookSunsetPassed(now) }

	probes := []struct {
		name string
		now  time.Time
	}{
		{name: "long before the sunset", now: sunset.Add(-30 * 24 * time.Hour)},
		{name: "one nanosecond before", now: sunset.Add(-time.Nanosecond)},
		{name: "at the sunset instant", now: sunset},
		{name: "one nanosecond after", now: sunset.Add(time.Nanosecond)},
		{name: "long after the sunset", now: sunset.Add(30 * 24 * time.Hour)},
	}

	// The index of the probe at which each consumer changes its mind. Both must land on
	// the same index, and that index must be the sunset instant itself.
	const instantProbe = 2
	relayFlippedAt, guardFlippedAt := -1, -1

	for i, probe := range probes {
		dualDelivering := relayDualDelivers(probe.now)
		gone := webhookRoutesAreGone(probe.now)

		assert.NotEqual(t, dualDelivering, gone,
			"%s: dual delivery and 410 Gone must never both be on nor both be off", probe.name)

		if i == 0 {
			continue
		}
		if dualDelivering != relayDualDelivers(probes[i-1].now) {
			relayFlippedAt = i
		}
		if gone != webhookRoutesAreGone(probes[i-1].now) {
			guardFlippedAt = i
		}
	}

	require.Equal(t, instantProbe, relayFlippedAt,
		"dual delivery must stop exactly at the sunset instant, which is probe %d", instantProbe)
	assert.Equal(t, relayFlippedAt, guardFlippedAt,
		"both consumers must change their mind at the very same instant")

	// Restated without the indirection, so the expected behaviour of each consumer is
	// legible from the assertions alone.
	assert.True(t, relayDualDelivers(sunset.Add(-time.Nanosecond)),
		"the relay is still dual delivering one nanosecond before the sunset")
	assert.False(t, webhookRoutesAreGone(sunset.Add(-time.Nanosecond)),
		"the webhook routes still answer normally one nanosecond before the sunset")
	assert.False(t, relayDualDelivers(sunset),
		"the relay has stopped dual delivering at the sunset instant")
	assert.True(t, webhookRoutesAreGone(sunset),
		"the webhook routes are gone at the sunset instant")
}

// TestWebhookSunsetPassed_IsSafeForConcurrentCallers guards the warn suppressor's
// shared state. Both consumers of the predicate are concurrent — the relay's poll
// loop and the HTTP request path — so a data race here would be a production defect.
// Run under -race this fails if the guard is left unsynchronised.
func TestWebhookSunsetPassed_IsSafeForConcurrentCallers(t *testing.T) {
	// No brokers, so the malformed value resolves to sunsetAbsentNoTransport and the
	// verdict is false. What is under test here is the warn suppressor's locking, not the
	// verdict.
	storeSunsetDate(t, "definitely-not-a-date")

	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			assert.False(t, WebhookSunsetPassed(time.Now()))
			_, configured := WebhookSunsetDate()
			assert.False(t, configured)
		}()
	}

	wg.Wait()
}

// TestWebhookSunsetPassed_FailClosedIsSafeForConcurrentCallers repeats the race check
// on the fail-closed path, which takes a different branch of the warn suppressor: the
// unset-window arm keys the guard on the empty string rather than on the raw value.
func TestWebhookSunsetPassed_FailClosedIsSafeForConcurrentCallers(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()

	fetchConfiguration = func() (*config.Configuration, error) {
		return &config.Configuration{
			Kafka: config.KafkaConfig{Brokers: []string{"broker-1:9092"}},
		}, nil
	}

	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			assert.True(t, WebhookSunsetPassed(time.Now()))
		}()
	}

	wg.Wait()
}

// TestSunsetWarnGuard_WarnsOnChangeAndAfterReset pins the guard's own contract
// directly, so a regression in it is attributed to the guard rather than surfacing
// as a puzzling log assertion elsewhere.
func TestSunsetWarnGuard_WarnsOnChangeAndAfterReset(t *testing.T) {
	guard := &sunsetWarnGuard{}

	assert.True(t, guard.shouldWarn("first"), "the first value is always news")
	assert.False(t, guard.shouldWarn("first"), "the same value is not news twice")
	assert.True(t, guard.shouldWarn("second"), "a changed value is news again")
	assert.False(t, guard.shouldWarn("second"))

	guard.reset()
	assert.True(t, guard.shouldWarn("second"), "a reset guard treats the value as new")
}

// The single-decision-point invariant.

// sunsetRawDateReaders returns the only files permitted to read the raw sunset value,
// as paths relative to the module root.
//
// Built from the SOURCE GROUPS of the two units that own the decision rather than from
// two fixed names. The invariant is about which CODE may read the value, and splitting a
// file for size moves code without changing what is permitted — so the allow-list has to
// follow the split or it starts failing on a file that was always allowed.
//
// Parameters:
//   - t *testing.T: the test, for the group lookup's assertions.
//
// Returns:
//   - map[string]struct{}: the permitted set, keyed by repository-relative path.
func sunsetRawDateReaders(t *testing.T) map[string]struct{} {
	t.Helper()

	permitted := map[string]struct{}{}
	for _, member := range eventSourceGroups(t, "event_sunset.go", "config/config.go") {
		permitted[member] = struct{}{}
	}

	return permitted
}

// sunsetRawDateFieldIdents are the configuration fields holding the raw window values.
//
// BOTH ends are covered, not only the sunset.
var sunsetRawDateFieldIdents = []string{
	"WebhookDeprecationSunsetDate",
	"WebhookDeprecationStartDate",
}

// sunsetRawDateLiterals are the string spellings through which the raw values can be
// reached without naming the fields: the environment variables, and the JSON keys a
// hand-rolled decode of blnk.json would use.
var sunsetRawDateLiterals = []string{
	"WEBHOOK_DEPRECATION_SUNSET_DATE",
	"webhook_deprecation_sunset_date",
	"WEBHOOK_DEPRECATION_START_DATE",
	"webhook_deprecation_start_date",
}

// sunsetScanSkipDirs are directories holding no first-party source.
var sunsetScanSkipDirs = map[string]struct{}{
	".git":         {},
	"vendor":       {},
	"node_modules": {},
	"testdata":     {},
}

// moduleRootDir walks up from the test's working directory until it finds the
// directory holding go.mod.
func moduleRootDir(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err, "the working directory must be readable to locate the module root")

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent,
			"walked to the filesystem root from %q without finding go.mod", dir)
		dir = parent
	}
}

// eventSourceGroup returns the repository-relative paths of every non-test Go file that
// together constitutes the logical unit named by base: base itself plus the files it was
// split into, which by convention carry base's stem followed by an underscore.
//
// Source-level assertions name a UNIT, not a file. When a file is split for size the
// assertion must widen to the whole group rather than silently narrow to the remnant,
// because a declaration that moved into a sibling would otherwise stop being checked at
// the moment it moved — a guard that quietly stops guarding is worse than no guard.
//
// Parameters:
//   - t *testing.T: the test, used for its helper marking and its assertions.
//   - base string: a repository-relative path such as "event_admin.go" or
//     "database/event_outbox.go".
//
// Returns:
//   - []string: repository-relative, slash-separated, sorted, always containing base.
func eventSourceGroup(t *testing.T, base string) []string {
	t.Helper()

	root := moduleRootDir(t)
	dir := path.Dir(base)
	stem := strings.TrimSuffix(path.Base(base), ".go")

	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), stem+"*.go"))
	require.NoErrorf(t, err, "the source group for %s must be enumerable", base)

	group := make([]string, 0, len(matches))

	for _, match := range matches {
		name := filepath.Base(match)
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		// stem+"*" also matches an unrelated file that merely shares the prefix without
		// the separator, so membership requires either the base itself or the "<stem>_"
		// form the splitter produces.
		if name != stem+".go" && !strings.HasPrefix(name, stem+"_") {
			continue
		}

		group = append(group, path.Join(dir, name))
	}

	sort.Strings(group)
	require.Containsf(t, group, base, "%s must exist and must belong to its own source group", base)

	return group
}

// eventSourceGroups expands every base in bases through eventSourceGroup, preserving the
// given order and dropping the duplicates that overlapping bases would produce.
//
// Parameters:
//   - t *testing.T: the test.
//   - bases ...string: repository-relative paths.
//
// Returns:
//   - []string: the union of the groups, in the order the bases were given.
func eventSourceGroups(t *testing.T, bases ...string) []string {
	t.Helper()

	seen := make(map[string]struct{}, len(bases))
	expanded := make([]string, 0, len(bases))

	for _, base := range bases {
		for _, member := range eventSourceGroup(t, base) {
			if _, already := seen[member]; already {
				continue
			}

			seen[member] = struct{}{}
			expanded = append(expanded, member)
		}
	}

	return expanded
}

// TestWebhookSunsetDecision_IsTheOnlyPlaceTheRawDateIsRead enforces that exactly one
// place in this repository reads the sunset date and compares a clock against it.
func TestWebhookSunsetDecision_IsTheOnlyPlaceTheRawDateIsRead(t *testing.T) {
	root := moduleRootDir(t)
	fset := token.NewFileSet()
	permittedRawDateReaders := sunsetRawDateReaders(t)

	// findings maps an offending file to the human-readable places it read the value.
	findings := make(map[string][]string)
	scanned := 0

	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			if _, skip := sunsetScanSkipDirs[entry.Name()]; skip {
				return filepath.SkipDir
			}

			return nil
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}

		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		relative = filepath.ToSlash(relative)

		if _, allowed := permittedRawDateReaders[relative]; allowed {
			return nil
		}

		parsed, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			// A file that does not parse already fails `go build ./...`. Reporting it
			// here as a sunset violation would only misdirect whoever reads the failure.
			t.Logf("skipping unparseable file %s: %v", relative, parseErr)

			return nil
		}
		scanned++

		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.Ident:
				for _, field := range sunsetRawDateFieldIdents {
					if typed.Name == field {
						findings[relative] = append(findings[relative], fmt.Sprintf(
							"reads the %s field at line %d",
							field, fset.Position(typed.Pos()).Line,
						))
					}
				}
			case *ast.BasicLit:
				if typed.Kind != token.STRING {
					return true
				}
				for _, literal := range sunsetRawDateLiterals {
					if strings.Contains(typed.Value, literal) {
						findings[relative] = append(findings[relative], fmt.Sprintf(
							"names %s at line %d", literal, fset.Position(typed.Pos()).Line,
						))
					}
				}
			}

			return true
		})

		return nil
	})
	require.NoError(t, walkErr, "the module tree must be walkable for this invariant to mean anything")

	// Without this the test would pass vacuously if the walk ever stopped finding
	// files — the most dangerous way for a structural assertion to fail.
	require.Greater(t, scanned, 1,
		"the scan inspected %d files, so it cannot have covered the module", scanned)

	for file, places := range findings {
		t.Errorf(
			"%s reads a raw webhook deprecation date (%s). The window is a single decision "+
				"point: call WebhookSunsetPassed, WebhookSunsetPassedNow, WebhookSunsetDate, "+
				"WebhookDualDeliveryActive or WebhookDualDeliveryWindowState instead, so the "+
				"relay's dual-delivery branch and the API's 410 Gone guard can never disagree "+
				"about when the window opens or closes.",
			file, strings.Join(places, "; "),
		)
	}
}

// ---------------------------------------------------------------------------
// The dual-delivery window — both ends of it
// ---------------------------------------------------------------------------

// TestWebhookDualDeliveryWindowState_PlacesAnInstantInTheWindow pins all four states
// and both boundaries.
//
// The window is the half-open interval [start, sunset): the start instant is INSIDE and
// the sunset instant is OUTSIDE.
func TestWebhookDualDeliveryWindowState_PlacesAnInstantInTheWindow(t *testing.T) {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	sunset := start.Add(config.WebhookDualDeliveryWindow())
	storeDeprecationWindow(t, start, sunset)

	for _, probe := range []struct {
		name  string
		now   time.Time
		want  WebhookWindowState
		about string
	}{
		{
			name:  "long before the start",
			now:   start.Add(-30 * 24 * time.Hour),
			want:  WebhookWindowPending,
			about: "a window that has not opened is a misconfiguration, not a phase",
		},
		{
			name:  "one nanosecond before the start",
			now:   start.Add(-time.Nanosecond),
			want:  WebhookWindowPending,
			about: "the start boundary must be exact",
		},
		{
			name:  "at the start instant",
			now:   start,
			want:  WebhookWindowActive,
			about: "the start instant is INSIDE the window: the interval is half-open at the far end only",
		},
		{
			name:  "midway through",
			now:   start.Add(15 * 24 * time.Hour),
			want:  WebhookWindowActive,
			about: "the ordinary state during the migration",
		},
		{
			name:  "one nanosecond before the sunset",
			now:   sunset.Add(-time.Nanosecond),
			want:  WebhookWindowActive,
			about: "dual delivery runs up to, but not including, the sunset instant",
		},
		{
			name:  "at the sunset instant",
			now:   sunset,
			want:  WebhookWindowClosed,
			about: "the sunset instant is the first moment of the post-sunset era",
		},
		{
			name:  "long after the sunset",
			now:   sunset.Add(30 * 24 * time.Hour),
			want:  WebhookWindowClosed,
			about: "and it stays closed",
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			assert.Equal(t, probe.want, WebhookDualDeliveryWindowState(probe.now), probe.about)
			assert.Equal(t, probe.want == WebhookWindowActive, WebhookDualDeliveryActive(probe.now),
				"the boolean predicate must agree with the four-valued one; two answers could diverge")
		})
	}
}

// TestWebhookDualDeliveryActive_ConsumesTheConfiguredStartDate is the regression guard
// on the reason this predicate exists.
//
// WEBHOOK_DEPRECATION_START_DATE is resolved and validated by configuration, and a
// decision that looked at the sunset alone would never read it.
func TestWebhookDualDeliveryActive_ConsumesTheConfiguredStartDate(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	sunset := now.Add(15 * 24 * time.Hour)

	storeDeprecationWindow(t, now.Add(-15*24*time.Hour), sunset)
	require.True(t, WebhookDualDeliveryActive(now),
		"a start already past, with the sunset ahead, is the middle of the window")

	// SAME sunset, a start that has not arrived. If the start were ignored this would still
	// answer true, which is exactly the behaviour being guarded against.
	storeDeprecationWindow(t, now.Add(24*time.Hour), sunset)
	assert.False(t, WebhookDualDeliveryActive(now),
		"the configured start must be consulted, or the window is not the 30 days it claims to be")
	assert.Equal(t, WebhookWindowPending, WebhookDualDeliveryWindowState(now))

	// And the sunset predicate is unaffected: a route's availability is a function of the
	// sunset alone, so a window that has not opened must NOT make the routes answer 410 —
	// they are still serving webhook management calls for a transport still in use.
	assert.False(t, WebhookSunsetPassed(now),
		"the 410 guard must key on the sunset alone; refusing calls before the window opened "+
			"would break a transport that is still live")
}

// TestWebhookDualDeliveryWindowState_DerivesTheStartWhenOnlyTheSunsetIsConfigured
// asserts the window is computable from either end alone.
func TestWebhookDualDeliveryWindowState_DerivesTheStartWhenOnlyTheSunsetIsConfigured(t *testing.T) {
	sunset := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	storeSunsetDate(t, sunset.Format(time.RFC3339))

	derivedStart := sunset.Add(-config.WebhookDualDeliveryWindow())

	assert.Equal(t, WebhookWindowActive, WebhookDualDeliveryWindowState(derivedStart),
		"the derived start must be inside the window, exactly as a configured one is")
	assert.Equal(t, WebhookWindowPending, WebhookDualDeliveryWindowState(derivedStart.Add(-time.Nanosecond)),
		"and one nanosecond earlier must be outside it")
	assert.Equal(t, WebhookWindowClosed, WebhookDualDeliveryWindowState(sunset))

	start, resolvedSunset, ok := WebhookDeprecationWindow()
	require.True(t, ok, "a configured sunset must resolve a describable window")
	assert.Equal(t, derivedStart, start, "the described start must be the derived one")
	assert.Equal(t, sunset, resolvedSunset)
}

// TestWebhookDualDeliveryWindowState_MalformedOrAbsentWindowIsUnavailable covers the
// state that is neither inside nor outside a window, because there is no usable window
// at all.
//
// Both spellings resolve the same way and both fail closed for the legacy leg: an
// unparseable window cannot be trusted to say the sunset has not passed, and enqueuing
// onto a transport that may already be retired is worse than not enqueuing.
func TestWebhookDualDeliveryWindowState_MalformedOrAbsentWindowIsUnavailable(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	for name, raw := range map[string]string{
		"no window at all":      "",
		"an unparseable sunset": "definitely-not-a-date",
	} {
		t.Run(name, func(t *testing.T) {
			storeSunsetDate(t, raw)

			assert.Equal(t, WebhookWindowUnavailable, WebhookDualDeliveryWindowState(now))
			assert.False(t, WebhookDualDeliveryActive(now),
				"with no usable window the legacy leg must not run")

			_, _, ok := WebhookDeprecationWindow()
			assert.False(t, ok, "and there is nothing to describe")
		})
	}
}

// TestWebhookDualDeliveryWindowState_MalformedStartFallsBackToTheDerivedStart asserts a
// mis-typed start date does not take the window with it.
func TestWebhookDualDeliveryWindowState_MalformedStartFallsBackToTheDerivedStart(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	sunset := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	previous := config.ConfigStore.Load()
	t.Cleanup(func() {
		if previous != nil {
			config.ConfigStore.Store(previous)

			return
		}
		config.ConfigStore.Store(&config.Configuration{})
	})
	sunsetParseWarnings.reset()
	startParseWarnings.reset()
	config.ConfigStore.Store(&config.Configuration{
		WebhookDeprecationStartDate:  "the-first-of-march",
		WebhookDeprecationSunsetDate: sunset.Format(time.RFC3339),
	})

	derivedStart := sunset.Add(-config.WebhookDualDeliveryWindow())

	assert.Equal(t, WebhookWindowActive, WebhookDualDeliveryWindowState(derivedStart.Add(time.Hour)),
		"a malformed start must fall back to the derived one rather than voiding the window")

	var warned bool
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "webhook_deprecation_start_date is not a valid RFC3339 instant") {
			warned = true

			break
		}
	}
	assert.True(t, warned, "the malformed start must be warned about, or the fallback is silent")
}

// TestWebhookWindowPendingObstacle_ExplainsOnlyThePendingState asserts the refusal
// message a process starts up with, and that it is produced for that state only.
func TestWebhookWindowPendingObstacle_ExplainsOnlyThePendingState(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	sunset := start.Add(config.WebhookDualDeliveryWindow())
	storeDeprecationWindow(t, start, sunset)

	err := WebhookWindowPendingObstacle(WebhookWindowPending)
	require.Error(t, err)
	assert.Contains(t, err.Error(), start.Format(time.RFC3339),
		"the message must name when the window opens")
	assert.Contains(t, err.Error(), sunset.Format(time.RFC3339),
		"and when it closes, so an operator can see the span they configured")
	assert.Contains(t, err.Error(), "WEBHOOK_DEPRECATION_SUNSET_DATE",
		"and the ONE variable that moves the window, or the message is not actionable")
	assert.NotContains(t, err.Error(), "WEBHOOK_DEPRECATION_START_DATE",
		"the opening instant is derived and has no environment variable, so a message naming "+
			"one sends an operator to set a key that does not exist")

	for _, state := range []WebhookWindowState{
		WebhookWindowActive, WebhookWindowClosed, WebhookWindowUnavailable,
	} {
		assert.NoError(t, WebhookWindowPendingObstacle(state),
			"%s is not an obstacle: only a window that has not opened is", state)
	}
}

// TestWebhookWindowState_StringNamesEveryState keeps the log-field spellings stable.
//
// The value is written into the relay's batch log line and into assertions, so a rename
// would silently change what operators grep for.
func TestWebhookWindowState_StringNamesEveryState(t *testing.T) {
	assert.Equal(t, "active", WebhookWindowActive.String())
	assert.Equal(t, "pending", WebhookWindowPending.String())
	assert.Equal(t, "closed", WebhookWindowClosed.String())
	assert.Equal(t, "unavailable", WebhookWindowUnavailable.String())
	assert.Equal(t, "unknown", WebhookWindowState(99).String(),
		"an unforeseen value must render legibly rather than as a bare integer")
}

// TestWebhookSunsetSnapshotAt_ComposesTheWindowFromOneConfigurationRead is the guard,
// and it is deterministic rather than a race probe.
//
// That is not a cosmetic inconsistency.
func TestWebhookSunsetSnapshotAt_ComposesTheWindowFromOneConfigurationRead(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()
	startParseWarnings.reset()

	const (
		firstSunset  = "2030-01-01T00:00:00Z"
		firstStart   = "2029-12-02T00:00:00Z"
		secondSunset = "2040-01-01T00:00:00Z"
		secondStart  = "2039-12-02T00:00:00Z"
	)

	var reads int
	fetchConfiguration = func() (*config.Configuration, error) {
		reads++

		// EVERY call after the first returns the later generation. A reload is being simulated as
		// having landed between any two reads, whichever two they are.
		if reads > 1 {
			return &config.Configuration{
				WebhookDeprecationSunsetDate: secondSunset,
				WebhookDeprecationStartDate:  secondStart,
			}, nil
		}

		return &config.Configuration{
			WebhookDeprecationSunsetDate: firstSunset,
			WebhookDeprecationStartDate:  firstStart,
		}, nil
	}

	snapshot := WebhookSunsetSnapshotAt(mustParseSunset(t, "2029-12-15T00:00:00Z"))

	assert.Equal(t, 1, reads,
		"the snapshot must read the configuration store EXACTLY ONCE. Every additional read is a "+
			"chance to observe a different generation, and the two ends of the window are two "+
			"independently configured fields, so two reads can compose a window that never existed")

	// BOTH ENDS FROM THE FIRST GENERATION. This is the assertion that fails on a two-read
	// composition, and it fails loudly: the start would be 2039 while the sunset stayed 2030.
	assert.Equal(t, mustParseSunset(t, firstSunset), snapshot.Date,
		"the sunset must come from the generation that was read")
	assert.Equal(t, mustParseSunset(t, firstStart), snapshot.WindowStart,
		"the window start must come from the SAME generation as the sunset, not from whatever the "+
			"store held by the time a second read happened")

	// THE PROPERTY THE HEADERS DEPEND ON, asserted as itself rather than left implied by the two
	// equalities above. RFC 9745 §4 requires this ordering of the pair the guards render.
	assert.True(t, snapshot.WindowStart.Before(snapshot.Date),
		"the window must open before it closes: RFC 9745 §4 requires the Deprecation instant to "+
			"precede the Sunset instant, and the guards render both from this one snapshot")

	assert.True(t, snapshot.DateConfigured, "the first generation configures a renderable instant")
	assert.False(t, snapshot.Passed, "the probe instant is inside the first generation's window")
}

// TestWebhookDualDeliveryWindowState_PlacesTheInstantFromOneConfigurationRead is the
// same property for the OTHER consumer of the window, and it is the one that decides
// whether a subscriber still receives an HTTP delivery.
//
// WebhookSunsetSnapshotAt drives the 410 guard and reads once.
func TestWebhookDualDeliveryWindowState_PlacesTheInstantFromOneConfigurationRead(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()
	startParseWarnings.reset()

	const (
		firstSunset  = "2030-01-01T00:00:00Z"
		firstStart   = "2029-12-02T00:00:00Z"
		secondSunset = "2040-01-01T00:00:00Z"
		secondStart  = "2039-12-02T00:00:00Z"
	)

	// installFlippingSeam gives ONE entry point a store that answers with the first
	// generation once and the later generation from then on, and reports how many times it
	// was read.
	installFlippingSeam := func() *int {
		reads := 0
		fetchConfiguration = func() (*config.Configuration, error) {
			reads++

			if reads > 1 {
				return &config.Configuration{
					WebhookDeprecationSunsetDate: secondSunset,
					WebhookDeprecationStartDate:  secondStart,
				}, nil
			}

			return &config.Configuration{
				WebhookDeprecationSunsetDate: firstSunset,
				WebhookDeprecationStartDate:  firstStart,
			}, nil
		}

		return &reads
	}

	probe := mustParseSunset(t, "2029-12-15T00:00:00Z")

	t.Run("the state places the instant inside the generation it read", func(t *testing.T) {
		reads := installFlippingSeam()

		state := WebhookDualDeliveryWindowState(probe)

		assert.Equal(t, 1, *reads,
			"the window state must read the configuration store EXACTLY ONCE per call. A second "+
				"read is a second generation, and the start and the sunset are two independently "+
				"configured fields, so two reads can place an instant in a window that never existed")
		assert.Equal(t, WebhookWindowActive, state,
			"the probe is inside the generation that was read, so both transports must run. PENDING "+
				"here means the start came from a later generation than the sunset")
	})

	t.Run("the relay predicate inherits that single read", func(t *testing.T) {
		reads := installFlippingSeam()

		assert.True(t, WebhookDualDeliveryActive(probe),
			"the relay's dual-delivery branch is this state read through one delegation, so it must "+
				"reach the same verdict; false here means the legacy leg stops for a window that has "+
				"not opened in any configuration the process ever held")
		assert.Equal(t, 1, *reads,
			"and the delegation must not add a read of its own")
	})

	t.Run("the 410 guard agrees from its own single read", func(t *testing.T) {
		// THE PAIR THE SUNSET DEFINES TOGETHER, asserted as itself: whether the legacy leg runs and
		// whether the webhook routes answer 410 are one decision taken twice, so each entry
		// point reading one whole generation is what keeps them from disagreeing across a
		// reload.
		reads := installFlippingSeam()

		snapshot := WebhookSunsetSnapshotAt(probe)

		assert.Equal(t, 1, *reads, "the guard's snapshot must also read exactly once")
		assert.False(t, snapshot.Passed,
			"the guard must not consider the sunset passed at an instant the relay calls active; "+
				"that disagreement is the divergence a mixed-generation read produces")
		assert.Equal(t, mustParseSunset(t, firstStart), snapshot.WindowStart,
			"and both ends it renders must come from the generation it read")
	})
}

// TestWebhookDeprecationWindow_AgreesWithTheSnapshot pins the two public readings of
// one window to each other.
//
// They now share one resolver, and this is what holds them to it.
func TestWebhookDeprecationWindow_AgreesWithTheSnapshot(t *testing.T) {
	restoreFetchConfiguration(t)
	sunsetParseWarnings.reset()
	startParseWarnings.reset()

	t.Run("a configured window is reported identically by both", func(t *testing.T) {
		storeSunsetDate(t, "2030-06-01T00:00:00Z")

		start, sunset, configured := WebhookDeprecationWindow()
		require.True(t, configured)

		snapshot := WebhookSunsetSnapshotAt(time.Now())
		assert.Equal(t, sunset, snapshot.Date,
			"the sunset an operator is shown must be the sunset a client is refused under")
		assert.Equal(t, start, snapshot.WindowStart,
			"and the window start likewise, or the Deprecation header and the startup log describe "+
				"different migrations")
		assert.True(t, snapshot.DateConfigured,
			"both readings must agree there IS a window, since the guards use DateConfigured to "+
				"decide whether to render the headers at all")
	})

	t.Run("an unconfigured window is reported as absent by both", func(t *testing.T) {
		// The graceful-degradation steady state of a deployment with no Kafka. Neither reading may
		// invent a date, because rendering one would announce a retirement nobody configured.
		storeSunsetDate(t, "")

		_, _, configured := WebhookDeprecationWindow()
		assert.False(t, configured)

		snapshot := WebhookSunsetSnapshotAt(time.Now())
		assert.False(t, snapshot.DateConfigured)
		assert.True(t, snapshot.WindowStart.IsZero(),
			"an unresolved window has no start to render, and a zero value is what the guards test")
	})
}

// ---------------------------------------------------------------------------
// THE TERMINAL RELEASE — the deletion checklist, and what keeps it honest
// ---------------------------------------------------------------------------

// terminalReleaseHeading is the checklist's heading in the published migration guide.
//
// The heading is matched literally because the test derives everything else from the
// section under it: renaming the heading without updating this constant would silently
// reduce the whole guard to nothing, so the require below fails loudly instead.
const terminalReleaseHeading = "### The terminal release: the deletion checklist"

// terminalReleasePreserveMarker separates the checklist's two tables.
//
// Everything before it is what the release DELETES; everything after it is what the
// release must leave alone.
const terminalReleasePreserveMarker = "**Preserve."

// terminalReleaseRow is one parsed row: the artifacts it names, and the files it says they live
// in.
type terminalReleaseRow struct {
	// line is the raw row, quoted into failure messages so the reader sees the row to edit
	// rather than being told a symbol is missing somewhere.
	line string
	// artifacts are the backticked tokens in the first column: symbols, statements, module
	// paths — whatever the release acts on.
	artifacts []string
	// locations are the backticked tokens in the second column, every one of which must be a
	// path in this repository.
	locations []string
}

// backtickedTokens returns the tokens a markdown cell wraps in backticks.
func backtickedTokens(cell string) []string {
	parts := strings.Split(cell, "`")
	tokens := make([]string, 0, len(parts)/2)
	for index := 1; index < len(parts); index += 2 {
		token := strings.TrimSpace(parts[index])
		if token != "" {
			tokens = append(tokens, token)
		}
	}

	return tokens
}

// terminalReleaseChecklist parses the published checklist into its two halves.
//
// Parameters:
//   - t *testing.T: the test, for the helper marking and the requires.
//
// Returns:
//   - []terminalReleaseRow: the rows the terminal release deletes or unregisters.
//   - []terminalReleaseRow: the rows it must preserve.
func terminalReleaseChecklist(t *testing.T) ([]terminalReleaseRow, []terminalReleaseRow) {
	t.Helper()

	doc := readRepoFile(t, filepath.Join("docs", "webhook-to-kafka-migration.md"))

	start := strings.Index(doc, terminalReleaseHeading)
	require.GreaterOrEqual(t, start, 0,
		"docs/webhook-to-kafka-migration.md must carry the terminal-release checklist under %q. It "+
			"is what makes the deferred deletion answerable to something rather than acknowledged in "+
			"a comment", terminalReleaseHeading)

	section := doc[start+len(terminalReleaseHeading):]
	if next := strings.Index(section, "\n### "); next >= 0 {
		section = section[:next]
	}

	var (
		deletes    []terminalReleaseRow
		preserves  []terminalReleaseRow
		preserving bool
	)

	for _, line := range strings.Split(section, "\n") {
		trimmed := strings.TrimSpace(line)

		if strings.Contains(trimmed, terminalReleasePreserveMarker) {
			preserving = true

			continue
		}

		// Rows only. The header row and the alignment row carry no backticks in their first
		// column, so they are skipped by the emptiness check below rather than by position.
		if !strings.HasPrefix(trimmed, "|") {
			continue
		}

		cells := strings.Split(strings.Trim(trimmed, "|"), "|")
		if len(cells) < 3 {
			continue
		}

		row := terminalReleaseRow{
			line:      trimmed,
			artifacts: backtickedTokens(cells[0]),
			locations: backtickedTokens(cells[1]),
		}
		if len(row.artifacts) == 0 && len(row.locations) == 0 {
			continue
		}

		if preserving {
			preserves = append(preserves, row)

			continue
		}
		deletes = append(deletes, row)
	}

	return deletes, preserves
}

// terminalReleaseLocations expands one checklist location into the files it stands for.
//
// A production Go file stands for its whole SOURCE GROUP, because that is the unit the
// release edits and a split moves artifacts between siblings. Anything else — a test file,
// a manifest, a migration, go.mod — stands only for itself: the group convention is about
// production source, and applying it elsewhere would either widen the check or, for a test
// file, resolve to nothing at all.
//
// Parameters:
//   - t *testing.T: the test.
//   - location string: a repository-relative path taken from the checklist's location column.
//
// Returns:
//   - []string: the files to read for that location, never empty.
func terminalReleaseLocations(t *testing.T, location string) []string {
	t.Helper()

	if !strings.HasSuffix(location, ".go") || strings.HasSuffix(location, "_test.go") {
		return []string{location}
	}

	return eventSourceGroup(t, location)
}

// TestWebhookTerminalRelease_ChecklistMatchesTheSurface is what turns the deferred
// deletion from an acknowledgement into an obligation.
//
// The sunset has two halves that run in sequence: both transports deliver from
// the same outbox rows for the fixed window, and only after it closes is the delivery
// source removed.
//
// A deferral recorded only in prose decays in both directions.
//
// This test closes both directions against the PUBLISHED checklist:
//
//   - every file the checklist names must exist, and every artifact must be findable in
//     the file the checklist says holds it — so performing the release forces the table
//     to be edited in the same change, and a rename cannot leave the table pointing at
//     nothing;
//   - every EXPORTED symbol the legacy transport declares must be named in the
//     checklist — so a new one cannot escape the release;
//   - the preserve half must still be intact — so a release that over-applies its own
//     delete half fails here rather than silently disabling transaction hooks and
//     search indexing.
func TestWebhookTerminalRelease_ChecklistMatchesTheSurface(t *testing.T) {
	root := moduleRootDir(t)
	deletes, preserves := terminalReleaseChecklist(t)

	// Lower bounds rather than exact counts: the point is that the parse found the tables, not
	// that the tables have a fixed size. An exact count would fail on any legitimate addition.
	require.GreaterOrEqualf(t, len(deletes), 6,
		"the delete half of the checklist parsed as %d rows, which means the table shape changed "+
			"and this guard stopped reading it", len(deletes))
	require.GreaterOrEqualf(t, len(preserves), 6,
		"the preserve half parsed as %d rows; it is the half that protects the shared queue, so an "+
			"unparsed table is worse than a missing one", len(preserves))

	assertRow := func(t *testing.T, row terminalReleaseRow, half string) {
		t.Helper()

		require.NotEmptyf(t, row.locations,
			"every %s row must name where its artifacts live, so the release does not have to go "+
				"looking. Row: %s", half, row.line)

		// Each location is read as a source UNIT: the named file plus the files it was split
		// into. The checklist names where an artifact LIVES, and a file split for size moves
		// artifacts between siblings without changing which unit the release edits — so
		// resolving the group is what keeps the checklist a statement about the code rather
		// than about one file's current contents.
		contents := make([]string, 0, len(row.locations))

		for _, location := range row.locations {
			if _, statErr := os.Stat(filepath.Join(root, location)); statErr != nil {
				require.NoErrorf(t, statErr,
					"the %s checklist names %s, which must exist while the row does. If this release has "+
						"been performed, DELETE THE ROW in docs/webhook-to-kafka-migration.md — the "+
						"checklist and the tree are two halves of one statement. Row: %s",
					half, location, row.line)
			}

			for _, member := range terminalReleaseLocations(t, location) {
				body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(member)))
				require.NoErrorf(t, err, "%s must be readable to check the %s checklist", member, half)
				contents = append(contents, string(body))
			}
		}

		for _, artifact := range row.artifacts {
			found := false
			for _, body := range contents {
				if strings.Contains(body, artifact) {
					found = true

					break
				}
			}

			assert.Truef(t, found,
				"the %s checklist names %q as living in %v, and it is not there. Either the symbol "+
					"was renamed and the row was not, or the row's location column is wrong — a "+
					"release performed from a stale row deletes the wrong thing or misses the right "+
					"one. Row: %s", half, artifact, row.locations, row.line)
		}
	}

	t.Run("every artifact the release deletes exists where the checklist says", func(t *testing.T) {
		for _, row := range deletes {
			assertRow(t, row, "delete")
		}
	})

	t.Run("every artifact the release must preserve is still intact", func(t *testing.T) {
		for _, row := range preserves {
			assertRow(t, row, "preserve")
		}
	})

	t.Run("no exported symbol of the legacy transport escapes the checklist", func(t *testing.T) {
		const transport = "webhooks.go"

		if _, err := os.Stat(filepath.Join(root, transport)); err != nil {
			t.Skipf("%s is gone, so the terminal release has been performed and there is no "+
				"transport surface left to enumerate", transport)
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(root, transport), nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "%s must parse to be enumerated", transport)

		doc := readRepoFile(t, filepath.Join("docs", "webhook-to-kafka-migration.md"))
		section := doc[strings.Index(doc, terminalReleaseHeading):]
		if next := strings.Index(section, "\n### "); next >= 0 {
			section = section[:next]
		}

		exported := exportedTopLevelNames(file)
		require.NotEmptyf(t, exported,
			"%s must declare at least one exported symbol, or this enumeration proves nothing", transport)

		for _, name := range exported {
			assert.Containsf(t, section, name,
				"%s declares the exported symbol %q and the terminal-release checklist does not name "+
					"it. Anything exported from the legacy transport is reachable from outside this "+
					"package, so a release that does not know about it either leaves dead public API "+
					"behind or breaks a caller it never looked for. Add it to the checklist in "+
					"docs/webhook-to-kafka-migration.md", transport, name)
		}
	})

	t.Run("the checklist names the trigger rather than a date", func(t *testing.T) {
		doc := readRepoFile(t, filepath.Join("docs", "webhook-to-kafka-migration.md"))
		section := doc[strings.Index(doc, terminalReleaseHeading):]

		for _, predicate := range []string{"WebhookSunsetPassed", "WebhookDualDeliveryActive"} {
			assert.Containsf(t, section, predicate,
				"the checklist must state its precondition as %s, the predicate the running system "+
					"answers with. A hard-coded date would be a second sunset decision, and the two "+
					"could disagree", predicate)
		}
	})
}

// exportedTopLevelNames collects the exported top-level declarations of a parsed file:
// functions and methods, and every name in a type, const or var declaration.
//
// Methods are included by their own name rather than qualified by receiver.
//
// Parameters:
//   - file *ast.File: the parsed source.
//
// Returns:
//   - []string: the exported names, in declaration order.
func exportedTopLevelNames(file *ast.File) []string {
	var names []string

	for _, decl := range file.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			if typed.Name != nil && typed.Name.IsExported() {
				names = append(names, typed.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				switch specified := spec.(type) {
				case *ast.TypeSpec:
					if specified.Name.IsExported() {
						names = append(names, specified.Name.Name)
					}
				case *ast.ValueSpec:
					for _, ident := range specified.Names {
						if ident.IsExported() {
							names = append(names, ident.Name)
						}
					}
				}
			}
		}
	}

	return names
}

// TestWebhookSunset_TheRetiredSentinelIsTheClosedWindow pins the state a deployment ends
// up in once the migration is over and there is no instant left to state.
//
// Two things have to be true at once, and only one of them is obvious. The sentinel must
// behave like a sunset that has PASSED — the legacy leg stops, the deprecated routes
// answer 410 — and it must be a state the relay is willing to START in. A value that
// merely failed closed would achieve the first and not the second: WebhookWindowObstacle
// refuses an unavailable window, so `blnk start` would exit, and the deployment would be
// forced to keep an obsolete date on file for ever to avoid it.
func TestWebhookSunset_TheRetiredSentinelIsTheClosedWindow(t *testing.T) {
	now := time.Date(2027, 6, 1, 9, 30, 0, 0, time.UTC)

	for _, spelling := range []string{"retired", "RETIRED", " Retired "} {
		t.Run("spelled "+spelling, func(t *testing.T) {
			storeSunsetDate(t, spelling)

			assert.True(t, WebhookSunsetPassed(now),
				"a declared retirement is a sunset that has passed; the legacy transport must not run")
			assert.Equal(t, WebhookWindowClosed, WebhookDualDeliveryWindowState(now),
				"the CLOSED state is the intended end state, and it is what makes this a state the "+
					"relay may run in rather than one it refuses")
			assert.False(t, WebhookDualDeliveryActive(now),
				"dual delivery is over, so the legacy leg must not be enqueued for any event")

			require.NoError(t, WebhookWindowObstacle(WebhookDualDeliveryWindowState(now)),
				"a retired deployment must be able to START. Refusing here is what would make an "+
					"obsolete date a permanent deployment requirement")

			// NOTHING TO DESCRIBE, and that is the point of the sentinel rather than a
			// shortcoming of it: there is no instant, so the 410 response carries no Sunset
			// header instead of carrying a fabricated one.
			snapshot := WebhookSunsetSnapshotAt(now)
			assert.True(t, snapshot.Passed, "the guard reads the same verdict from the snapshot")
			assert.False(t, snapshot.DateConfigured,
				"there is no date to render, so no Sunset or Deprecation header may be emitted")
			assert.True(t, snapshot.Date.IsZero(), "and no instant may be invented for one")

			_, _, ok := WebhookDeprecationWindow()
			assert.False(t, ok, "a closed window has no ends left to publish")
		})
	}

	t.Run("an unusable window is still refused", func(t *testing.T) {
		// The distinction the sentinel exists to draw. Both states stop the legacy leg, and
		// only one of them is a DECLARATION; the other is a value nobody supplied, and a
		// publishing process must not start on it.
		storeSunsetDate(t, "")

		assert.Equal(t, WebhookWindowUnavailable, WebhookDualDeliveryWindowState(now))
		require.Error(t, WebhookWindowObstacle(WebhookDualDeliveryWindowState(now)),
			"an absent window must keep refusing to start, or the sentinel has bought silence "+
				"rather than clarity")
	})
}
