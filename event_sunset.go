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
// config.Configuration.resolveWebhookDeprecationWindow, which parses it at load time
// to REFUSE a configuration whose window is malformed, inconsistent with the 30-day
// contract, or absent while Kafka publishing is enabled. It performs no clock
// comparison and makes no sunset decision, and it cannot delegate to this file
// because package blnk imports package config, not the reverse.
//
// The two therefore split the work cleanly: config decides whether a window is
// ADMISSIBLE, at startup, once, and fatally. This file decides whether the admissible
// window has ELAPSED, on every consultation. Neither duplicates the other.

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

// webhookSunsetResolution is the tri-state answer to "what does configuration say
// about the sunset?".
//
// A two-state answer is what made the previous behaviour unsafe. "Configured" and
// "not configured" collapsed two very different situations into one: a deployment
// that has no Kafka and therefore no migration to describe, and a deployment that IS
// publishing to Kafka but whose window is missing or mis-typed. Both used to resolve
// to "the sunset has not passed", which kept the deprecated HTTP transport running
// indefinitely in the second case with nothing failing anywhere to say so.
type webhookSunsetResolution int

const (
	// sunsetResolved means a sunset instant is configured and parses. The instant is
	// meaningful and the verdict is a straight comparison against it.
	sunsetResolved webhookSunsetResolution = iota

	// sunsetAbsentNoTransport means no window is configured AND no Kafka broker is
	// configured. There is nothing to migrate to, so there is no window to describe:
	// the legacy transport is simply the only transport and the sunset has not
	// passed. This is the graceful-degradation state every deployment without Kafka
	// runs in, and it stays an ordinary, quiet outcome.
	sunsetAbsentNoTransport

	// sunsetUnusableWithTransport means Kafka publishing IS configured but the window
	// is missing or will not parse. This FAILS CLOSED: the sunset is treated as
	// passed, so dual delivery stops and the deprecated webhook management routes
	// answer 410 Gone.
	//
	// Failing closed rather than open is the whole point of the tri-state. The
	// alternative — carrying on with legacy HTTP push indefinitely — keeps the
	// deprecated, less protected transport alive on the strength of a typo, and does
	// it invisibly. Retiring it early is loud, immediately visible to anyone still
	// consuming webhooks, and recoverable by correcting one variable.
	//
	// config.Configuration.resolveWebhookDeprecationWindow refuses to LOAD this
	// combination, so it is unreachable through the normal startup path. This arm is
	// what happens when configuration is published some other way — a test writing to
	// the store directly, or a future reload path — and is a second line of defence
	// rather than the primary one.
	sunsetUnusableWithTransport
)

// webhookSunsetInstant resolves the sunset instant, and what its absence means, from
// a configuration value.
//
// It is the only parse of the sunset date that feeds a decision. Whitespace is
// trimmed before parsing because a trailing newline from an environment file is not a
// malformed date, and a successfully parsed instant is normalised to UTC: time.Time
// comparisons are absolute, so the location cannot change any verdict, but
// normalising means the value handed back — to a log line, to a Sunset response
// header, to a test assertion — is always in one zone and reads unambiguously.
//
// A malformed value is reported, never raised as an error and never a panic: a
// mis-typed date must not take a ledger down. What it must ALSO not do is silently
// preserve the deprecated transport, which is why the returned resolution
// distinguishes "no transport to migrate to" from "publishing, but the window is
// unusable".
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read the sunset date from. May
//     be nil, which is reached when configuration has not been loaded.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//     Meaningful only alongside sunsetResolved.
//   - webhookSunsetResolution: which of the three situations applies.
func webhookSunsetInstant(cnf *config.Configuration) (time.Time, webhookSunsetResolution) {
	if cnf == nil {
		// No configuration at all means no brokers either, so there is no transport to
		// migrate to and nothing to fail closed about.
		return time.Time{}, sunsetAbsentNoTransport
	}

	publishing := len(cnf.Kafka.Brokers) > 0

	unusable := func() webhookSunsetResolution {
		if publishing {
			return sunsetUnusableWithTransport
		}

		return sunsetAbsentNoTransport
	}

	raw := strings.TrimSpace(cnf.WebhookDeprecationSunsetDate)
	if raw == "" {
		if publishing && sunsetParseWarnings.shouldWarn("") {
			logrus.Error(
				"webhook_deprecation_sunset_date is NOT SET while Kafka brokers are configured. " +
					"The dual-delivery window has no end, so the sunset is being treated as ALREADY " +
					"PASSED: legacy HTTP webhook delivery stops and the deprecated webhook " +
					"management routes answer 410 Gone. Set WEBHOOK_DEPRECATION_SUNSET_DATE, or " +
					"WEBHOOK_DEPRECATION_START_DATE and let the 30-day window derive it",
			)
		}

		return time.Time{}, unusable()
	}

	parsed, err := time.Parse(webhookSunsetLayout, raw)
	if err != nil {
		if sunsetParseWarnings.shouldWarn(raw) {
			entry := logrus.WithError(err).WithFields(logrus.Fields{
				"value":    raw,
				"expected": webhookSunsetLayout,
			})
			if publishing {
				entry.Error(
					"webhook_deprecation_sunset_date is not a valid RFC3339 instant while Kafka " +
						"brokers are configured. The sunset is being treated as ALREADY PASSED: " +
						"legacy HTTP webhook delivery stops and the deprecated webhook management " +
						"routes answer 410 Gone. Correct the value",
				)
			} else {
				entry.Warn(
					"webhook_deprecation_sunset_date is not a valid RFC3339 instant and is being " +
						"ignored; no Kafka broker is configured, so there is no dual-delivery " +
						"window to end and the deprecated webhook routes keep answering normally",
				)
			}
		}

		return time.Time{}, unusable()
	}

	return parsed.UTC(), sunsetResolved
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
// nothing is configured at all — no window and no brokers — which resolves to
// sunsetAbsentNoTransport, and is logged at debug level so the hot paths stay quiet.
// This mirrors Blnk.Config, which likewise degrades to an empty configuration rather
// than failing when the store is empty.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - webhookSunsetResolution: which of the three situations applies.
func resolveWebhookSunset() (time.Time, webhookSunsetResolution) {
	cnf, err := fetchConfiguration()
	if err != nil {
		logrus.WithError(err).Debug(
			"configuration is not loaded; treating the webhook sunset as not yet passed",
		)

		return time.Time{}, sunsetAbsentNoTransport
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
// Note that false does NOT imply the sunset has not passed. A deployment publishing
// to Kafka with an unusable window has no instant to report and yet IS past the
// sunset, because that state fails closed. Callers must take the verdict from
// WebhookSunsetPassed and use this only to describe a date they already know exists.
//
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - bool: true only when a sunset date is configured AND parses. When false, the
//     returned time is meaningless and must not be rendered.
func WebhookSunsetDate() (time.Time, bool) {
	instant, resolution := resolveWebhookSunset()

	return instant, resolution == sunsetResolved
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
// those routes answer 410 Gone. Because config.Configuration enforces a window of
// exactly config.WebhookDualDeliveryWindowDays, that interval is exactly 30 days.
//
// Both sides of the comparison are normalised to UTC, so a date written with an
// offset (2026-03-01T01:00:00+01:00) and the same instant written as Z
// (2026-03-01T00:00:00Z) are indistinguishable, as they must be.
//
// # What an absent or unusable window means, and why it depends on the transport
//
// This function FAILS CLOSED when Kafka publishing is configured and the window is
// missing or unparseable: it answers true, so dual delivery stops and the deprecated
// routes answer 410 Gone. It answers false only when there is no Kafka transport at
// all, where "the sunset has not passed" is simply the truth — there is nothing to
// have migrated to, so nothing can have been retired.
//
// The previous behaviour answered false in BOTH cases. That is the defect this
// distinction fixes: one mis-typed environment variable indefinitely preserved the
// legacy HTTP transport and the webhook management surface on a deployment that had
// already moved to Kafka, silently and with nothing to alert on. Configuration now
// refuses to load that combination outright, so reaching this arm at all means
// configuration arrived by some other route; the fail-closed answer is the second
// line of defence.
//
// Parameters:
//   - now time.Time: the instant to evaluate the sunset against. It is a parameter
//     rather than an internal time.Now() call so that callers — and the tests that
//     pin the boundary to the nanosecond — control the clock.
//
// Returns:
//   - bool: true when a sunset date is configured, parses, and now is at or after it;
//     true when Kafka publishing is configured and the window is unusable; false
//     otherwise.
func WebhookSunsetPassed(now time.Time) bool {
	sunset, resolution := resolveWebhookSunset()

	switch resolution {
	case sunsetResolved:
		return !now.UTC().Before(sunset)
	case sunsetUnusableWithTransport:
		return true
	case sunsetAbsentNoTransport:
		return false
	default:
		// Unreachable: the three constants above are exhaustive. Answering "passed"
		// keeps an unforeseen resolution on the fail-closed side.
		return true
	}
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
