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
//
// It writes to config.ConfigStore directly rather than through config.MockConfig for
// two reasons: MockConfig runs validateAndAddDefaults, which refuses to store a
// configuration without a data-source and Redis DSN (so the test's value would be
// silently dropped), and it also warns about a malformed sunset date, which would
// contaminate the log assertions below. Storing directly is the same approach the
// legacy webhook tests take.
//
// The warn guard is reset on every call so that log assertions never depend on
// whether an earlier test already warned about the same value.
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
	config.ConfigStore.Store(&config.Configuration{WebhookDeprecationSunsetDate: raw})
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

// TestWebhookSunsetPassed_ExactlyThirtyDayDualDeliveryWindow expresses the
// requirement directly: a sunset instant set 30 days after the window opens yields
// exactly 30 days of dual delivery, no more and no less.
func TestWebhookSunsetPassed_ExactlyThirtyDayDualDeliveryWindow(t *testing.T) {
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

	first := &config.Configuration{WebhookDeprecationSunsetDate: "not-a-date"}
	for i := 0; i < 5; i++ {
		date, configured := webhookSunsetInstant(first)
		assert.False(t, configured)
		assert.True(t, date.IsZero())
	}
	assert.Equal(t, 1, countWarnings(fragment),
		"five calls with one malformed value must warn exactly once")

	// A different malformed value is new information and must warn again.
	second := &config.Configuration{WebhookDeprecationSunsetDate: "also-not-a-date"}
	_, configured := webhookSunsetInstant(second)
	assert.False(t, configured)
	assert.Equal(t, 2, countWarnings(fragment),
		"a changed malformed value must warn again")

	// Well-formed and absent values must never warn.
	hook.Reset()
	_, configured = webhookSunsetInstant(&config.Configuration{WebhookDeprecationSunsetDate: "2026-06-15T12:30:45Z"})
	assert.True(t, configured)
	_, configured = webhookSunsetInstant(&config.Configuration{})
	assert.False(t, configured)
	assert.Zero(t, countWarnings(fragment), "valid and absent values must not warn")
}

func TestWebhookSunsetInstant_NilConfigurationIsNotConfigured(t *testing.T) {
	date, configured := webhookSunsetInstant(nil)

	assert.False(t, configured)
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

// TestWebhookSunsetPassed_IsSafeForConcurrentCallers guards the warn suppressor's
// shared state. Both consumers of the predicate are concurrent — the relay's poll
// loop and the HTTP request path — so a data race here would be a production defect.
// Run under -race this fails if the guard is left unsynchronised.
func TestWebhookSunsetPassed_IsSafeForConcurrentCallers(t *testing.T) {
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
