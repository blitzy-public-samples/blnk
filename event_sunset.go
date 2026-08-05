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
	"sync"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/sirupsen/logrus"
)

// This file is the single decision point for the legacy webhook sunset.
//
// Retiring the HTTP webhook transport has two observable behaviours, and they are
// two consumers of ONE decision:
//
//  1. Dual delivery. Until the sunset instant, every claimed event-outbox row is
//     both published to Kafka and enqueued as a legacy webhook task from that same
//     row — which is why the two transports cannot carry divergent payloads. From
//     the sunset instant onwards only the Kafka publish happens.
//  2. HTTP 410 Gone. From the sunset instant onwards every request to a deprecated
//     webhook management route is refused with the GEN_GONE error code, which is
//     mapped explicitly to http.StatusGone in internal/apierror/codes.go.
//
// If that decision were duplicated — one date comparison in the relay, another in
// the API middleware — the two could drift apart, and the service could stop
// dual-writing while still accepting webhook management calls, or keep dual-writing
// after the routes had already gone. Both are silent, hard-to-diagnose failures.
//
// Therefore: WebhookSunsetPassed below is the ONLY place in this codebase that
// compares a clock against the configured sunset date. Callers ask it; they never
// re-derive the answer. Adding a second comparison anywhere else re-opens exactly
// the divergence this file exists to prevent.
//
// The one other place that touches the raw value is
// config.Configuration.validateWebhookDeprecationSunsetDate, which parses it purely
// to warn about a malformed value at load time. It performs no comparison and makes
// no decision, and it cannot delegate to this file because package blnk imports
// package config, not the reverse.

// webhookSunsetLayout is the layout the sunset date is written in. RFC3339 is the
// deployment contract for WEBHOOK_DEPRECATION_SUNSET_DATE, and it is what the
// configuration loader validates the value against.
const webhookSunsetLayout = time.RFC3339

// sunsetWarnGuard suppresses repeat warnings about the same malformed sunset date.
//
// The sunset predicate is on two hot paths: the relay consults it for every claimed
// outbox row on a one-second poll loop, and the API middleware consults it on every
// request to a deprecated route. Warning unconditionally would therefore turn one
// mis-typed environment variable into an unbounded log flood, which buries the very
// message an operator needs to see.
//
// The guard remembers only the most recently warned value, so its memory cost is a
// single string regardless of uptime. It deliberately holds no parsed date and no
// verdict: the decision itself is always recomputed from live configuration, so this
// state can never make the sunset behave differently from what is configured.
type sunsetWarnGuard struct {
	mu        sync.Mutex
	lastValue string
}

// shouldWarn reports whether raw differs from the last value warned about, and
// records it when it does. A changed value warns again, which is what an operator
// correcting one typo into another needs to see.
func (g *sunsetWarnGuard) shouldWarn(raw string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lastValue == raw {
		return false
	}
	g.lastValue = raw

	return true
}

// reset forgets the last warned value so the next malformed value warns again. It
// exists for tests, which must be able to assert the warning is emitted without
// depending on whether an earlier test already tripped the guard.
func (g *sunsetWarnGuard) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.lastValue = ""
}

// sunsetParseWarnings guards the malformed-date warning emitted by
// webhookSunsetInstant.
var sunsetParseWarnings = &sunsetWarnGuard{}

// fetchConfiguration is the seam through which the sunset date is read from the
// configuration store. It is config.Fetch in every build; it is a variable purely so
// that tests can exercise the "configuration has not been loaded" branch, which is
// otherwise unreachable once any earlier test has populated the global store — an
// atomic.Value cannot be emptied once written.
var fetchConfiguration = config.Fetch

// webhookSunsetInstant resolves the sunset instant from a configuration value.
//
// It is the only parse of the sunset date that feeds a decision, and it applies
// three deliberate leniencies, each of which resolves to "no sunset is configured":
//
//   - A nil configuration. Reached when configuration has not been loaded.
//   - An empty or whitespace-only value. Whitespace matters: the configuration
//     loader trims a fixed set of fields and this is not one of them, so a trailing
//     newline from an environment file would otherwise be read as a malformed date.
//   - A value that is not valid RFC3339. Reported as a warning, never as an error
//     and never as a panic. A mis-typed date must not take a ledger down, and it
//     must not silently retire a transport either.
//
// A successfully parsed instant is normalised to UTC. time.Time comparisons are
// absolute, so the location cannot change any verdict; normalising means the value
// this function hands back — to a log line, to a Sunset response header, to a test
// assertion — is always in one zone and reads unambiguously.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read the sunset date from. May
//     be nil.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - bool: true only when a sunset date is configured AND parses.
func webhookSunsetInstant(cnf *config.Configuration) (time.Time, bool) {
	if cnf == nil {
		return time.Time{}, false
	}

	raw := strings.TrimSpace(cnf.WebhookDeprecationSunsetDate)
	if raw == "" {
		return time.Time{}, false
	}

	parsed, err := time.Parse(webhookSunsetLayout, raw)
	if err != nil {
		if sunsetParseWarnings.shouldWarn(raw) {
			logrus.WithError(err).WithFields(logrus.Fields{
				"value":    raw,
				"expected": webhookSunsetLayout,
			}).Warn(
				"webhook_deprecation_sunset_date is not a valid RFC3339 instant and is being ignored; " +
					"the webhook sunset is treated as not yet passed, so dual delivery continues and the " +
					"deprecated webhook routes keep answering normally",
			)
		}

		return time.Time{}, false
	}

	return parsed.UTC(), true
}

// resolveWebhookSunset reads the sunset instant from the live configuration store.
//
// The value is re-read and re-parsed on every call rather than cached. That is
// intentional on two counts: parsing one short timestamp is cheap next to the work
// on either hot path, and config.ConfigStore is an atomic.Value that is re-published
// whenever configuration is (re)loaded — a cached verdict would keep answering from
// configuration that no longer exists.
//
// A configuration store that has not been populated is not an error here. It means
// no sunset date is configured, which resolves to "not passed" like every other
// absent value, and is logged at debug level so the hot paths stay quiet. This
// mirrors Blnk.Config, which likewise degrades to an empty configuration rather than
// failing when the store is empty.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - bool: true only when a sunset date is configured AND parses.
func resolveWebhookSunset() (time.Time, bool) {
	cnf, err := fetchConfiguration()
	if err != nil {
		logrus.WithError(err).Debug(
			"configuration is not loaded; treating the webhook sunset as not yet passed",
		)

		return time.Time{}, false
	}

	return webhookSunsetInstant(cnf)
}

// WebhookSunsetDate returns the configured webhook sunset instant.
//
// It exists so that callers which need to describe the sunset — an RFC 8594 Sunset
// response header, a Deprecation header, an operator-facing error message — can do
// so from the same parse that drives the decision, instead of re-parsing the raw
// configuration string and risking a different reading of it.
//
// The returned instant is in UTC. Callers rendering an HTTP Sunset header should
// format it with http.TimeFormat, which is the IMF-fixdate representation that
// RFC 8594 requires.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - bool: true only when a sunset date is configured AND parses. When false, the
//     returned time is meaningless and must not be rendered.
func WebhookSunsetDate() (time.Time, bool) {
	return resolveWebhookSunset()
}

// WebhookSunsetPassed reports whether the legacy webhook sunset has passed as of now.
//
// This is the authoritative sunset predicate. Every consumer of the sunset — the
// relay's dual-delivery branch and the HTTP 410 Gone guard alike — must call it, and
// no consumer may compare against the configured date itself.
//
// Boundary semantics, stated exactly because a subtle bug would live here: the
// sunset HAS passed when now is AT OR AFTER the configured instant. The instant
// itself is the first moment of the post-sunset era, so the dual-delivery window is
// the half-open interval that ends at — and excludes — the sunset instant. One
// nanosecond before it, dual delivery is still running and the deprecated webhook
// routes still answer normally; at it and after it, Kafka is the only transport and
// those routes answer 410 Gone. Configuring the instant 30 days after the window
// opens therefore yields exactly 30 days of dual delivery.
//
// Both sides of the comparison are normalised to UTC, so a date written with an
// offset (2026-03-01T01:00:00+01:00) and the same instant written as Z
// (2026-03-01T00:00:00Z) are indistinguishable, as they must be.
//
// When no sunset date is configured, or the configured value cannot be parsed, or
// configuration has not been loaded, the answer is false — the sunset has NOT
// passed. That default is chosen, not incidental: it keeps dual delivery running and
// keeps the webhook management routes answering normally. Answering true for an
// absent value would silently retire the HTTP transport on every deployment that
// never set the variable.
//
// Parameters:
//   - now time.Time: the instant to evaluate the sunset against. It is a parameter
//     rather than an internal time.Now() call so that callers — and the tests that
//     pin the boundary to the nanosecond — control the clock.
//
// Returns:
//   - bool: true when a sunset date is configured, parses, and now is at or after it.
func WebhookSunsetPassed(now time.Time) bool {
	sunset, configured := resolveWebhookSunset()
	if !configured {
		return false
	}

	return !now.UTC().Before(sunset)
}

// WebhookSunsetPassedNow reports whether the webhook sunset has passed as of the
// current wall clock.
//
// It is a convenience for call sites that have no clock of their own to inject, and
// it is deliberately nothing more than WebhookSunsetPassed(time.Now()) — the
// comparison lives in one place only. The Now suffix keeps the hidden clock visible
// at the call site; anything that needs to control the clock, including every test
// of sunset behaviour, must call WebhookSunsetPassed directly.
//
// Returns:
//   - bool: true when a sunset date is configured, parses, and the current instant
//     is at or after it.
func WebhookSunsetPassedNow() bool {
	return WebhookSunsetPassed(time.Now())
}
