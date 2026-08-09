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
	"path/filepath"
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
		// Nothing was published before this test. Leaving the test's sunset date in
		// place could influence later tests, so publish an empty configuration —
		// strictly closer to the original state than a configured sunset date.
		config.ConfigStore.Store(&config.Configuration{})
	})

	sunsetParseWarnings.reset()
	startParseWarnings.reset()
	config.ConfigStore.Store(&config.Configuration{WebhookDeprecationSunsetDate: raw})
}

// storeDeprecationWindow publishes BOTH ends of the dual-delivery window for one test and
// restores whatever was there before.
//
// It exists because the window predicate reads both ends, so a test that set only the sunset
// would be asserting against a start DERIVED as sunset minus 30 days — which is correct
// behaviour and a confusing fixture: a sunset far in the future then places "now" BEFORE the
// window rather than inside it, and a test meaning "the window is open" would silently be
// testing "the window has not opened".
//
// The pair is written verbatim rather than validated here. config refuses a window that is
// not exactly 30 days when it LOADS one; this writes to the store directly, exactly as
// storeSunsetDate does, so a test can also pin a deliberately inconsistent pair.
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

// restoreFetchConfiguration snapshots the package's configuration seam and puts it
// back when the test finishes.
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

	// The safe default: with no sunset configured, dual delivery keeps running and
	// the deprecated webhook routes keep answering normally. Probing instants far
	// apart proves the answer does not depend on the clock at all.
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

// TestWebhookSunsetPassed_BoundaryIsInclusiveToTheNanosecond pins the exact
// semantics of the sunset instant. The instant itself belongs to the post-sunset
// era, so the dual-delivery window is half-open: it ends at, and excludes, the
// configured instant. Probing one nanosecond either side is what distinguishes an
// at-or-after comparison from a strictly-after one.
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

// TestWebhookSunsetPassed_ThirtyDayDualDeliveryWindowBoundary checks the predicate for a
// date configured 30 days out: the legacy transport is in use from the moment the window
// opens until the instant before the sunset, and retired from the sunset on. Nothing in
// the code records the opening instant or enforces the interval — configuring the date 30
// days ahead is the operator's responsibility — so what is verified here is the boundary
// behaviour of a correctly configured date, not duration enforcement.
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
	// configuration loader does not trim this field, so it must be tolerated here or
	// a correct date would be read as malformed.
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
	// The consequence, which is what an operator acts on. With no Kafka broker
	// configured there is no dual-delivery window to end, so the stated consequence is
	// that the deprecated routes keep answering. The Kafka-configured case states the
	// opposite consequence and is asserted by
	// TestWebhookSunsetInstant_FailsClosedWhenPublishingWithoutAUsableWindow.
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

	// No brokers configured, so a malformed value is a WARNING and resolves to
	// "no transport to migrate to". The fail-closed path is asserted separately, by
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

// TestWebhookSunsetInstant_FailsClosedWhenPublishingWithoutAUsableWindow is the test
// for the finding that the sunset used to fail OPEN.
//
// An unset or mis-typed WEBHOOK_DEPRECATION_SUNSET_DATE resolved to "the sunset has
// not passed", which preserved legacy HTTP webhook delivery and the deprecated webhook
// management surface indefinitely — on a deployment that had already moved to Kafka,
// silently, with nothing to alert on. One typo cancelled the retirement of the
// transport this whole feature replaces.
//
// The distinguishing fact is whether a Kafka transport exists. With brokers configured
// there IS somewhere to have migrated to, so an unusable window fails closed. With no
// brokers there is not, so "not passed" is simply the truth.
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

// TestWebhookSunsetSnapshotAt_AnswersBothQuestionsFromOneResolution is C-14.
//
// # The defect
//
// The HTTP guard needs two things — a Sunset header advertising the date, and a verdict
// deciding whether to answer 410 — and it obtained them from two independent calls,
// WebhookSunsetDate then WebhookSunsetPassed. Each re-reads the live configuration store,
// which is replaced wholesale on reload, so a reload landing between them produced a single
// response advertising date A while refusing under date B. A client reading the header was
// told it had until A by the very response that had already applied B.
//
// The window is narrow, which is exactly why it must be closed structurally: it cannot be
// reproduced on demand and would never be observed by watching for it.
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
	// start is derived as sunset minus 30 days — which is why the earliest probe below sits
	// exactly on the start rather than before it.
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
	// verdict is false. What is under test here is the warn suppressor's locking, not
	// the verdict.
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

// sunsetRawDateReaders are the only files permitted to read the raw sunset value,
// given as paths relative to the module root.
var sunsetRawDateReaders = map[string]struct{}{
	"event_sunset.go":  {},
	"config/config.go": {},
}

// sunsetRawDateFieldIdents are the configuration fields holding the raw window values.
//
// BOTH ends are covered, not only the sunset. The window is a span, and requirement R-12
// is about its length: a file that read the start date and compared it itself would be a
// second decision about when dual delivery runs, which is the same divergence single
// ownership of the sunset exists to prevent.
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

// TestWebhookSunsetDecision_IsTheOnlyPlaceTheRawDateIsRead enforces that exactly one
// place in this repository reads the sunset date and compares a clock against it.
func TestWebhookSunsetDecision_IsTheOnlyPlaceTheRawDateIsRead(t *testing.T) {
	root := moduleRootDir(t)
	fset := token.NewFileSet()

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

		if _, allowed := sunsetRawDateReaders[relative]; allowed {
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

// TestWebhookDualDeliveryWindowState_PlacesAnInstantInTheWindow pins all four states and
// both boundaries.
//
// The window is the half-open interval [start, sunset): the start instant is INSIDE and the
// sunset instant is OUTSIDE. That asymmetry is what makes the span exactly the configured
// number of days rather than a day either side of it, and both edges are asserted to the
// nanosecond because an off-by-one here is a day of the wrong behaviour on the one day
// anybody is watching.
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

// TestWebhookDualDeliveryActive_ConsumesTheConfiguredStartDate is the regression guard on the
// defect this predicate exists to fix.
//
// WEBHOOK_DEPRECATION_START_DATE was resolved and validated by configuration and then read by
// NOTHING: every decision looked at the sunset alone. A deployment whose relay began
// publishing weeks before its declared start therefore ran both transports for weeks longer
// than the 30 days its own configuration described, and no code disagreed with it.
//
// The two windows below share a sunset and differ only in their start, so the verdict can
// only differ if the start is genuinely consulted.
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

// TestWebhookDualDeliveryWindowState_DerivesTheStartWhenOnlyTheSunsetIsConfigured asserts the
// window is computable from either end alone.
//
// config.Configuration back-fills the start from the sunset when only the sunset is supplied,
// and this reproduces that rule for a configuration published some other way — a test, or a
// future reload path. Without the derivation such a configuration would have a window with no
// beginning, and the state of any instant in it would be undefined.
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

// TestWebhookDualDeliveryWindowState_MalformedOrAbsentWindowIsUnavailable covers the state
// that is neither inside nor outside a window, because there is no usable window at all.
//
// Both spellings resolve the same way and both fail closed for the legacy leg: an
// unparseable window cannot be trusted to say the sunset has not passed, and enqueuing onto a
// transport that may already be retired is worse than not enqueuing.
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
//
// A malformed START is recoverable in a way a malformed SUNSET is not: the sunset determines
// the window's length, so the start can be re-derived from it exactly as configuration does.
// Falling back is therefore strictly better than failing closed here — it keeps delivering to
// unmigrated subscribers — and the warning is what stops it being silent.
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

// TestWebhookWindowPendingObstacle_ExplainsOnlyThePendingState asserts the refusal message a
// process starts up with, and that it is produced for that state only.
//
// The message lives in this file because this file owns the window's vocabulary; a caller
// composing it would be a second place that knew what the configured dates are called, which
// the raw-read invariant rightly rejects.
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
