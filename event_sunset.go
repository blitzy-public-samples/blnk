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
// # The window has TWO ends, and dual delivery is gated on both
//
// The sunset is one end of a window whose other end is DERIVED from it — exactly
// WebhookDualDeliveryWindowDays earlier — and requirement R-12 is about the SPAN
// between them: Kafka publishing and legacy HTTP delivery run concurrently for
// EXACTLY 30 days. A predicate that consulted the sunset alone could not express
// that span, so a deployment whose relay began publishing weeks before the window
// opened would run dual delivery for weeks longer than 30 days.
//
// THERE IS ONE CONFIGURABLE END, and it is WEBHOOK_DEPRECATION_SUNSET_DATE.
// Requirement R-10 freezes the deployment contract at eight variables, of which
// exactly one describes this window, so the opening instant carries no environment
// variable of its own and no message in this file may tell an operator to set one:
// the only actionable remedy for a window in the wrong place is to correct the
// SUNSET date. See config.WebhookDeprecationStartDate, which is a derived, read-only
// field.
//
// WebhookDualDeliveryActive is therefore the authoritative predicate for the LEGACY
// LEG, and it consults both ends: the window is the half-open interval
// [start, sunset). WebhookSunsetPassed remains the authoritative predicate for the
// HTTP 410 Gone guard, because a route's availability is a function of the sunset
// alone — a route that answered 410 before the window opened would refuse calls
// during a period in which webhooks were still being delivered.
//
// The asymmetry is not a divergence: both read the same resolved window from the same
// helper, and neither compares a clock against a raw configuration string. What they
// differ on is which BOUNDARY governs the behaviour each of them owns.
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
					"management routes answer 410 Gone. Set WEBHOOK_DEPRECATION_SUNSET_DATE to an " +
					"RFC3339 instant 30 days after this deployment begins publishing; the window's " +
					"opening instant is derived from it and has no variable of its own",
			)
		}

		return time.Time{}, unusable()
	}

	parsed, err := time.Parse(webhookSunsetLayout, raw)
	if err != nil {
		if sunsetParseWarnings.shouldWarn(raw) {
			entry := withLoggableCause(logrus.WithFields(logrus.Fields{
				"value":    sanitizeLogValue(raw, maxLoggedFilterLength),
				"expected": webhookSunsetLayout,
			}), err)
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
		withLoggableCause(nil, err).Debug(
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

	return webhookSunsetPassedFor(sunset, resolution, now)
}

// webhookSunsetPassedFor is the sunset comparison itself, separated from where the
// configuration came from.
//
// THIS IS THE ONLY PLACE IN THE CODEBASE THAT COMPARES AN INSTANT TO THE SUNSET. It is
// factored out so that a caller holding a configuration value — the event-capture gate,
// which must judge the deployment it was handed rather than the global store — reaches
// the same comparison as a caller reading live configuration, instead of writing a
// second one that can drift from it.
//
// Parameters:
//   - sunset time.Time: the resolved sunset instant. Meaningful only with sunsetResolved.
//   - resolution webhookSunsetResolution: which of the three situations applies.
//   - now time.Time: the instant to evaluate.
//
// Returns:
//   - bool: true when the sunset has passed, including the fail-closed arm.
func webhookSunsetPassedFor(
	sunset time.Time,
	resolution webhookSunsetResolution,
	now time.Time,
) bool {
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

// WebhookSunsetSnapshot is the sunset as ONE request saw it: the instant to describe
// and the verdict to act on, resolved together.
//
// # Why the two must come from one resolution
//
// The HTTP guard needs both — a Sunset header advertising the date, and a verdict
// deciding whether to answer 410 — and it used to obtain them from two independent
// calls, WebhookSunsetDate followed by WebhookSunsetPassed. Each re-reads the live
// configuration store, whose contents are replaced wholesale on reload, so a reload
// landing between them produced a single response advertising date A while deciding
// under date B. A client reading the header would be told it had until A when the
// refusal it just received was taken under B, and nothing in the response would
// disclose the disagreement.
//
// The mismatch is small and the window for it is narrow, which is precisely why it
// must be closed structurally rather than watched for: it cannot be reproduced on
// demand and would never be observed in testing.
//
// The fields are read-only once returned. There is no method that recomputes anything.
type WebhookSunsetSnapshot struct {
	// Date is the resolved sunset instant in UTC. Meaningful only when DateConfigured
	// is true; otherwise it is the zero time and must not be rendered.
	Date time.Time

	// DateConfigured reports whether there is an instant to DESCRIBE. It does not
	// answer whether the sunset has passed — a deployment publishing to Kafka with an
	// unusable window has no instant to advertise and yet IS past the sunset, because
	// that state fails closed. Take the verdict from Passed and nothing else.
	DateConfigured bool

	// Passed is the verdict, evaluated against the instant the caller supplied and
	// against the same resolution Date came from.
	Passed bool
}

// WebhookSunsetSnapshotAt resolves the sunset ONCE and answers every question about it.
//
// It reads the configuration store a single time, so the date it reports and the
// verdict it returns cannot describe different configurations. Callers that need both
// — the 410 guard being the one that does — must use this rather than pairing
// WebhookSunsetDate with WebhookSunsetPassed.
//
// The comparison is still webhookSunsetPassedFor's, so this adds no second reading of
// the boundary: it is the same predicate the relay's dual-delivery branch reaches, and
// the same fail-closed treatment of an unusable window.
//
// Parameters:
//   - now time.Time: the instant to evaluate the sunset against, injected for the same
//     reason WebhookSunsetPassed takes it — so tests pin the boundary exactly.
//
// Returns:
//   - WebhookSunsetSnapshot: the date, whether it is renderable, and the verdict.
func WebhookSunsetSnapshotAt(now time.Time) WebhookSunsetSnapshot {
	sunset, resolution := resolveWebhookSunset()

	return WebhookSunsetSnapshot{
		Date:           sunset,
		DateConfigured: resolution == sunsetResolved,
		Passed:         webhookSunsetPassedFor(sunset, resolution, now),
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

// ---------------------------------------------------------------------------
// THE DUAL-DELIVERY WINDOW — both ends of it
// ---------------------------------------------------------------------------

// WebhookWindowState is where a given instant falls relative to the dual-delivery
// window.
//
// It is four states rather than a boolean because the three ways dual delivery can be
// OFF call for three different operator responses, and collapsing them would make the
// most dangerous of them look like the most ordinary. "The window has not opened yet"
// is a misconfiguration to correct; "the window has closed" is the intended end state;
// "there is no usable window" is a failure that has been failed closed.
type WebhookWindowState int

const (
	// WebhookWindowActive means the instant is inside [start, sunset): both transports
	// run, driven from the same claimed outbox row.
	WebhookWindowActive WebhookWindowState = iota

	// WebhookWindowPending means the instant is BEFORE the configured start.
	//
	// It is a misconfiguration rather than a phase: reaching it means the process is
	// publishing to Kafka before the window it declared has opened, so the actual
	// concurrent-delivery period will be longer than the 30 days subscribers were told
	// about. The relay refuses to START in this state — see its startup obstacle —
	// which is what keeps the state from silently suppressing legacy deliveries to
	// subscribers who have not migrated yet.
	WebhookWindowPending

	// WebhookWindowClosed means the instant is AT OR AFTER the sunset. Kafka is the
	// only transport, and the deprecated webhook routes answer 410 Gone.
	WebhookWindowClosed

	// WebhookWindowUnavailable means configuration describes no usable window: either
	// nothing is configured at all (the graceful-degradation state of a deployment
	// without Kafka, where there is no window because there is no migration), or
	// publishing is configured and the window is missing or unparseable, which fails
	// closed.
	WebhookWindowUnavailable
)

// String renders the state for logs and error messages.
//
// Returns:
//   - string: the state's name, lower-case and stable, safe to use as a log field
//     value and to assert on.
func (s WebhookWindowState) String() string {
	switch s {
	case WebhookWindowActive:
		return "active"
	case WebhookWindowPending:
		return "pending"
	case WebhookWindowClosed:
		return "closed"
	case WebhookWindowUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

// webhookWindowStart resolves the instant the dual-delivery window opens.
//
// The configured start is used when it is present and parses. When it is absent or
// malformed the start is DERIVED as sunset minus the 30-day window, which is exactly
// what config.Configuration.resolveWebhookDeprecationWindow does when only the sunset
// is supplied. Deriving rather than failing is what makes the window computable from
// either end alone, and it is why a deployment — or a test — that configures only
// WEBHOOK_DEPRECATION_SUNSET_DATE still has a fully determined window rather than one
// with an open beginning.
//
// A malformed start is warned about once per distinct value, through the same guard
// the sunset parse uses, because this is consulted on the relay's hot path.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read. May be nil.
//   - sunset time.Time: the already-resolved sunset instant, used to derive the start.
//
// Returns:
//   - time.Time: the window's opening instant in UTC.
func webhookWindowStart(cnf *config.Configuration, sunset time.Time) time.Time {
	derived := sunset.Add(-config.WebhookDualDeliveryWindow())

	if cnf == nil {
		return derived
	}

	raw := strings.TrimSpace(cnf.WebhookDeprecationStartDate)
	if raw == "" {
		return derived
	}

	parsed, err := time.Parse(webhookSunsetLayout, raw)
	if err != nil {
		if startParseWarnings.shouldWarn(raw) {
			withLoggableCause(logrus.WithFields(logrus.Fields{
				"value":         sanitizeLogValue(raw, maxLoggedFilterLength),
				"expected":      webhookSunsetLayout,
				"derived_start": derived.Format(webhookSunsetLayout),
			}), err).Warn(
				"webhook_deprecation_start_date is not a valid RFC3339 instant and is being " +
					"ignored; the window start is derived from the sunset instead. Correct the value",
			)
		}

		return derived
	}

	return parsed.UTC()
}

// startParseWarnings guards the malformed-start-date warning, independently of the
// sunset's guard so that correcting one date does not suppress the warning about the
// other.
var startParseWarnings = &sunsetWarnGuard{}

// WebhookDualDeliveryWindowState reports where now falls relative to the window.
//
// It is the one resolution both dual-delivery callers share, and it re-reads live
// configuration on every call for the same reasons WebhookSunsetPassed does.
//
// Parameters:
//   - now time.Time: the instant to place. A parameter rather than an internal clock
//     read so callers, and the tests that pin the boundaries to the nanosecond,
//     control it.
//
// Returns:
//   - WebhookWindowState: which of the four states applies.
func WebhookDualDeliveryWindowState(now time.Time) WebhookWindowState {
	sunset, resolution := resolveWebhookSunset()
	if resolution != sunsetResolved {
		return WebhookWindowUnavailable
	}

	// The CLOSED end is decided by webhookSunsetPassedFor, not by a comparison written
	// here. This used to test `!now.Before(sunset)` inline, which was a second place in
	// the codebase comparing an instant to the sunset — correct today, and free to drift
	// from the predicate the 410 guard uses the moment either boundary rule changed. The
	// pair that would then disagree is exactly the one this file exists to keep in
	// step: "the legacy leg has stopped" and "the webhook routes answer 410".
	//
	// The resolution is already known to be sunsetResolved, so this reaches that
	// function's resolved arm and nothing else; the fail-closed arm is unreachable from
	// here and is handled above as WebhookWindowUnavailable.
	instant := now.UTC()
	if webhookSunsetPassedFor(sunset, resolution, instant) {
		return WebhookWindowClosed
	}

	cnf, err := fetchConfiguration()
	if err != nil {
		// The sunset resolved, so configuration was readable a moment ago; this can
		// only be a store that has since been emptied, which is not a reachable
		// production state. Deriving the start from the sunset keeps the verdict
		// determined rather than inventing a failure.
		cnf = nil
	}

	if instant.Before(webhookWindowStart(cnf, sunset)) {
		return WebhookWindowPending
	}

	return WebhookWindowActive
}

// WebhookDualDeliveryActive reports whether the LEGACY HTTP LEG must run for an event
// being dispatched at now.
//
// This is the authoritative predicate for the relay's dual-delivery branch, and it is
// the only one that answers the question requirement R-12 actually asks: are both
// transports supposed to be running at this instant? It is true only inside
// [start, sunset) — so it is false before the window opens, false from the sunset
// instant onwards, and false when no usable window is configured.
//
// It must be consulted IMMEDIATELY BEFORE each enqueue rather than once per batch. A
// batch claimed a second before the sunset takes time to publish, and a decision taken
// at the top of it would enqueue legacy deliveries after the boundary had passed — the
// one behaviour the sunset is defined to prevent.
//
// Parameters:
//   - now time.Time: the instant to evaluate.
//
// Returns:
//   - bool: true only when now is inside the configured window.
func WebhookDualDeliveryActive(now time.Time) bool {
	return WebhookDualDeliveryWindowState(now) == WebhookWindowActive
}

// WebhookDeprecationWindow returns the resolved window, for callers that need to
// DESCRIBE it rather than decide with it — a startup log line, or the error a relay
// refuses to start with.
//
// Returns:
//   - time.Time: the start instant in UTC.
//   - time.Time: the sunset instant in UTC.
//   - bool: true only when a sunset is configured and parses, in which case both
//     instants are meaningful. When false, both are zero and must not be rendered.
func WebhookDeprecationWindow() (time.Time, time.Time, bool) {
	sunset, resolution := resolveWebhookSunset()
	if resolution != sunsetResolved {
		return time.Time{}, time.Time{}, false
	}

	cnf, err := fetchConfiguration()
	if err != nil {
		cnf = nil
	}

	return webhookWindowStart(cnf, sunset), sunset, true
}

// WebhookWindowObstacle describes why a process that publishes to Kafka must not start on
// the window state the caller resolved, or nil when the state is a legitimate one to run in.
//
// SUNSET: goes with the legacy leg.
//
// TWO STATES ARE REFUSED and two are accepted, and the asymmetry is the whole point:
//
//   - WebhookWindowActive is the dual-delivery window itself. Run.
//   - WebhookWindowClosed is the intended END state — Kafka is the only transport. Run.
//   - WebhookWindowPending means the process would publish to Kafka BEFORE the window it
//     declared has opened, enqueuing no legacy webhooks while claiming a migration has not
//     started. Refuse; see WebhookWindowPendingObstacle for the message.
//   - WebhookWindowUnavailable, FOR A PUBLISHING PROCESS, means configuration describes no
//     usable window at all. The sunset predicate fails closed on it — WebhookSunsetPassed
//     answers true — so the legacy leg silently stops while configuration says nothing
//     about a retirement, which is exactly the disagreement the review's C-1 finding names.
//     Refuse, and name the variable.
//
// Callers must only pass a state they resolved for a process that HAS a Kafka transport. A
// process with no brokers also resolves to WebhookWindowUnavailable, legitimately — there is
// no window because there is no migration — and it has no reason to consult this function,
// because it publishes nothing.
//
// Parameters:
//   - state WebhookWindowState: the state the caller resolved.
//
// Returns:
//   - error: non-nil for WebhookWindowPending and WebhookWindowUnavailable.
func WebhookWindowObstacle(state WebhookWindowState) error {
	if state == WebhookWindowUnavailable {
		return fmt.Errorf(
			"configuration describes no usable dual-delivery window (%s), so the sunset resolves "+
				"fail-closed to ALREADY PASSED and this process would publish to Kafka while "+
				"enqueuing no legacy webhooks at all. Set WEBHOOK_DEPRECATION_SUNSET_DATE to the "+
				"RFC3339 instant the legacy transport retires; the %d-day window opens that many "+
				"days earlier",
			state, config.WebhookDualDeliveryWindowDays,
		)
	}

	return WebhookWindowPendingObstacle(state)
}

// WebhookWindowPendingObstacle describes why a process must not begin publishing to Kafka
// before the dual-delivery window opens, or nil when the state is anything else.
//
// SUNSET: goes with the legacy leg.
//
// # Why this lives here rather than at the call site
//
// The message quotes both ends of the window and names the one variable that moves them, and
// this file is the single owner of the window — including the vocabulary for explaining it.
// A relay that composed this message itself would be a second place that knew what the
// configured date is called, and the invariant test that keeps every raw read in one file
// would rightly reject it.
//
// The remedy it names is WEBHOOK_DEPRECATION_SUNSET_DATE, and only that. The opening instant
// is derived from the sunset and has no environment variable, so naming one would send an
// operator to set a key that does not exist — the message would look actionable and change
// nothing.
//
// # Why the state is a parameter
//
// The caller has already resolved it, through whichever seam it uses, and resolving it a
// second time here could produce a different answer on a clock boundary — so the caller's
// verdict is the one explained.
//
// Parameters:
//   - state WebhookWindowState: the state the caller resolved.
//
// Returns:
//   - error: non-nil only for WebhookWindowPending. The message names both ends of the
//     window, the one variable that moves them, and the two acceptable ways forward.
func WebhookWindowPendingObstacle(state WebhookWindowState) error {
	if state != WebhookWindowPending {
		return nil
	}

	start, sunset, resolved := WebhookDeprecationWindow()
	if !resolved {
		// Unreachable: WebhookWindowPending is only ever returned for a window that
		// resolved. Reported without dates rather than not reported at all.
		return errors.New(
			"the dual-delivery window has not opened yet, so publishing to Kafka now would " +
				"deliver no legacy webhooks: correct WEBHOOK_DEPRECATION_SUNSET_DATE",
		)
	}

	return fmt.Errorf(
		"the dual-delivery window opens at %s and has not opened yet, so publishing to Kafka now "+
			"would enqueue no legacy webhooks and would run the two transports concurrently for "+
			"longer than the %d days ending %s. Move the window by setting "+
			"WEBHOOK_DEPRECATION_SUNSET_DATE to %d days after this deployment actually begins "+
			"publishing — the opening instant is derived from it and has no variable of its own — "+
			"or do not publish until then",
		start.Format(webhookSunsetLayout),
		config.WebhookDualDeliveryWindowDays,
		sunset.Format(webhookSunsetLayout),
		config.WebhookDualDeliveryWindowDays,
	)
}
