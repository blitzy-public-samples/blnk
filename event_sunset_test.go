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

// TestWebhookSunsetPassed_MalformedDateWarnsThroughThePublicPath asserts the log
// contract on the path production actually takes.
//
// The test below this one covers the internal helper, but neither consumer of the
// sunset calls that helper: the relay and the HTTP guard both go through
// WebhookSunsetPassed. A warning that only fired on the internal path would leave a
// mis-typed environment variable completely silent in production, so the whole chain
// — configuration store, resolve, parse, warn — is exercised here end to end.
//
// The structured fields are asserted individually because they are what makes the
// warning actionable: an operator needs the offending value, the layout it failed to
// match, and the parser's own complaint. The message fragment is deliberately the one
// unique to this file ("instant"). The configuration loader emits its own,
// separately tested warning about the very same field using the word "timestamp", and
// an assertion that either could satisfy would prove nothing about this code.
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
	assert.NotNil(t, warning.Data[logrus.ErrorKey],
		"the warning must carry the parse error itself")
	assert.Contains(t, warning.Message, "dual delivery continues",
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

// TestWebhookSunsetPassed_BothConsumersFlipAtTheSameInstant pins the invariant that
// gives this decision point its reason to exist.
//
// The sunset has two observable consequences, and they live in two different
// packages. The relay stops dual delivering — it enqueues a legacy webhook task only
// while the sunset has NOT passed. The HTTP guard starts refusing the deprecated
// webhook routes with 410 Gone — only once it HAS passed. Those two are exact
// complements at every instant, and they must flip together. If they did not, the
// service could stop dual writing while still accepting webhook management calls, or
// keep dual writing after the routes had already gone; both are silent failures that
// would only surface as a subscriber complaining about missing events.
//
// The invariant holds here BY CONSTRUCTION: both closures below consult the one
// predicate, which is precisely why every consumer is routed through it. What keeps it
// that way over time is not this test but
// TestWebhookSunsetDecision_IsTheOnlyPlaceTheRawDateIsRead, which fails the moment any
// consumer grows a date comparison of its own. This test states the property the two
// consumers must satisfy and pins the exact instant at which both change their mind.
func TestWebhookSunsetPassed_BothConsumersFlipAtTheSameInstant(t *testing.T) {
	const raw = "2026-06-15T12:30:45Z"
	storeSunsetDate(t, raw)

	sunset := mustParseSunset(t, raw)

	// The relay's dual-delivery branch: legacy delivery continues for exactly as long
	// as the sunset has not passed.
	relayDualDelivers := func(now time.Time) bool { return !WebhookSunsetPassed(now) }
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

// -----------------------------------------------------------------------------
// The single-decision-point invariant.
//
// Everything above pins WHAT the sunset decision is. This section pins WHERE that
// decision is allowed to live, which is the structural property that stops the two
// consumers from ever drifting apart. A behavioural test cannot catch a second
// comparison appearing in another package; only a scan of the source can.
// -----------------------------------------------------------------------------

// sunsetRawDateReaders are the only files permitted to read the raw sunset value,
// given as paths relative to the module root.
//
//   - event_sunset.go is the decision point itself. Reading the raw value is its job.
//   - config/config.go declares the field and parses it once at load time purely to
//     warn about a malformed value. It performs no comparison and reaches no verdict,
//     and it cannot delegate to the helper because package blnk imports package
//     config, not the reverse.
//
// A file showing up in the scan below is a defect to fix, not a reason to extend this
// list. Consumers get the answer from WebhookSunsetPassed, WebhookSunsetPassedNow or
// WebhookSunsetDate.
var sunsetRawDateReaders = map[string]struct{}{
	"event_sunset.go":  {},
	"config/config.go": {},
}

// sunsetRawDateFieldIdent is the configuration field holding the raw value.
const sunsetRawDateFieldIdent = "WebhookDeprecationSunsetDate"

// sunsetRawDateLiterals are the string spellings through which the raw value can be
// reached without naming the field: the environment variable, and the JSON key a
// hand-rolled decode of blnk.json would use.
var sunsetRawDateLiterals = []string{
	"WEBHOOK_DEPRECATION_SUNSET_DATE",
	"webhook_deprecation_sunset_date",
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
//
// `go test` runs in the package directory, which for this package is already the
// module root, but resolving it explicitly keeps the scan correct if these tests are
// ever moved into a subpackage, and makes the failure legible if they are not run
// through `go test` at all.
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
//
// A consumer cannot compare against the sunset without first obtaining it, and there
// are only three ways to obtain it: the configuration field, the environment variable,
// or the JSON key. Scanning for those three is therefore equivalent to scanning for a
// second comparison, and it is far more precise than hunting for time.Parse calls —
// the codebase parses RFC3339 in many legitimate places that have nothing to do with
// the sunset.
//
// The scan reads the AST rather than the raw bytes, with comments deliberately left
// unattached. Explaining the sunset in a doc comment is encouraged; only executable
// code counts as a second reader. Test files are exempt because fixtures legitimately
// set the field, as this file and config/config_test.go both do.
//
// This test is forward looking. It is the tripwire that fires if the relay's
// dual-delivery branch or the API's 410 Gone guard is later written with a date
// comparison of its own instead of calling the helper — which is the exact regression
// TestWebhookSunsetPassed_BothConsumersFlipAtTheSameInstant could never detect.
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
				if typed.Name == sunsetRawDateFieldIdent {
					findings[relative] = append(findings[relative], fmt.Sprintf(
						"reads the %s field at line %d",
						sunsetRawDateFieldIdent, fset.Position(typed.Pos()).Line,
					))
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
			"%s reads the raw webhook sunset date (%s). The sunset is a single decision "+
				"point: call WebhookSunsetPassed, WebhookSunsetPassedNow or WebhookSunsetDate "+
				"instead, so the relay's dual-delivery branch and the API's 410 Gone guard "+
				"can never disagree about when the sunset happens.",
			file, strings.Join(places, "; "),
		)
	}
}
