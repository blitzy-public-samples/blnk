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

// webhookSunsetLayout is the layout the sunset date is written in. RFC3339 is the
// deployment contract for WEBHOOK_DEPRECATION_SUNSET_DATE, and it is what the
// configuration loader validates the value against.
const webhookSunsetLayout = time.RFC3339

// sunsetWarnGuard suppresses repeat warnings about the same malformed sunset date.
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

// reset forgets the last warned value so the next malformed value warns again.
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

// webhookSunsetResolution is the tri-state answer to "what does configuration say about
// the sunset?".
type webhookSunsetResolution int

const (
	// sunsetResolved means a sunset instant is configured and parses. The instant is
	// meaningful and the verdict is a straight comparison against it.
	sunsetResolved webhookSunsetResolution = iota

	// sunsetAbsentNoTransport means no window is configured AND no Kafka broker is
	// configured. There is nothing to migrate to, so there is no window to describe: the
	// legacy transport is simply the only transport and the sunset has not passed. This is
	// the graceful-degradation state every deployment without Kafka runs in, and it stays
	// an ordinary, quiet outcome.
	sunsetAbsentNoTransport

	// sunsetUnusableWithTransport means Kafka publishing IS configured but the window is
	// missing or will not parse. This FAILS CLOSED: the sunset is treated as passed, so
	// dual delivery stops and the deprecated webhook management routes answer 410 Gone.
	sunsetUnusableWithTransport

	// sunsetRetired means configuration DECLARES the legacy transport retired, with the
	// literal config.WebhookSunsetRetiredSentinel in place of an instant.
	sunsetRetired
)

// webhookSunsetInstant resolves the sunset instant, and what its absence means, from a
// configuration value.
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

	// THE DECLARED RETIREMENT, checked before the parse because it is not an instant and
	// must not be reported as a malformed one. Configuration has already normalised the
	// casing; EqualFold here so a value that reached this struct without passing through
	// validateAndAddDefaults — a hand-built Configuration in a test, say — reads the same.
	if strings.EqualFold(raw, config.WebhookSunsetRetiredSentinel) {
		return time.Time{}, sunsetRetired
	}

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
// Returns:
//   - time.Time: the sunset instant in UTC, or the zero time when none is configured.
//   - bool: true only when a sunset date is configured AND parses.
func WebhookSunsetDate() (time.Time, bool) {
	instant, resolution := resolveWebhookSunset()

	return instant, resolution == sunsetResolved
}

// WebhookSunsetPassed reports whether the legacy webhook sunset has passed as of now.
//
// Parameters:
//   - now time.Time: the instant to evaluate the sunset against.
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
func webhookSunsetPassedFor(
	sunset time.Time,
	resolution webhookSunsetResolution,
	now time.Time,
) bool {
	switch resolution {
	case sunsetResolved:
		return !now.UTC().Before(sunset)
	case sunsetUnusableWithTransport, sunsetRetired:
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
type WebhookSunsetSnapshot struct {
	// Date is the resolved sunset instant in UTC. Meaningful only when DateConfigured
	// is true; otherwise it is the zero time and must not be rendered.
	Date time.Time

	// DateConfigured reports whether there is an instant to DESCRIBE. It does not answer
	// whether the sunset has passed — a deployment publishing to Kafka with an unusable
	// window has no instant to advertise and yet IS past the sunset, because that state
	// fails closed. Take the verdict from Passed and nothing else.
	DateConfigured bool

	// WindowStart is the instant the dual-delivery window OPENED, in UTC, derived from the
	// same configuration read Date came from. Meaningful only when DateConfigured is true;
	// otherwise it is the zero time and must not be rendered.
	WindowStart time.Time

	// Passed is the verdict, evaluated against the instant the caller supplied and
	// against the same resolution Date came from.
	Passed bool
}

// WebhookSunsetSnapshotAt resolves the sunset ONCE and answers every question about it.
//
// Parameters:
//   - now time.Time: the instant to evaluate the sunset against, injected for the same
//     reason WebhookSunsetPassed takes it — so tests pin the boundary exactly.
//
// Returns:
//   - WebhookSunsetSnapshot: the date, whether it is renderable, and the verdict.
func WebhookSunsetSnapshotAt(now time.Time) WebhookSunsetSnapshot {
	start, sunset, resolution := resolveWebhookWindow()

	return WebhookSunsetSnapshot{
		Date:           sunset,
		DateConfigured: resolution == sunsetResolved,
		WindowStart:    start,
		Passed:         webhookSunsetPassedFor(sunset, resolution, now),
	}
}

// resolveWebhookWindow resolves BOTH ends of the retirement window and the resolution
// that produced them from ONE read of the configuration store.
func resolveWebhookWindow() (time.Time, time.Time, webhookSunsetResolution) {
	cnf, err := fetchConfiguration()
	if err != nil {
		withLoggableCause(nil, err).Debug(
			"configuration is not loaded; treating the webhook sunset as not yet passed",
		)

		return time.Time{}, time.Time{}, sunsetAbsentNoTransport
	}

	sunset, resolution := webhookSunsetInstant(cnf)
	if resolution != sunsetResolved {
		return time.Time{}, sunset, resolution
	}

	return webhookWindowStart(cnf, sunset), sunset, resolution
}

// WebhookSunsetPassedNow reports whether the webhook sunset has passed as of the
// current wall clock.
//
// Returns:
//   - bool: true when a sunset date is configured, parses, and the current instant is
//     at or after it.
func WebhookSunsetPassedNow() bool {
	return WebhookSunsetPassed(time.Now())
}

// ---------------------------------------------------------------------------
// THE DUAL-DELIVERY WINDOW — both ends of it
// ---------------------------------------------------------------------------

// WebhookWindowState is where a given instant falls relative to the dual-delivery
// window.
type WebhookWindowState int

const (
	// WebhookWindowActive means the instant is inside [start, sunset): both transports
	// run, driven from the same claimed outbox row.
	WebhookWindowActive WebhookWindowState = iota

	// WebhookWindowPending means the instant is BEFORE the configured start.
	WebhookWindowPending

	// WebhookWindowClosed means the instant is AT OR AFTER the sunset. Kafka is the
	// only transport, and the deprecated webhook routes answer 410 Gone.
	WebhookWindowClosed

	// WebhookWindowUnavailable means configuration describes no usable window: either
	// nothing is configured at all (the graceful-degradation state of a deployment without
	// Kafka, where there is no window because there is no migration), or publishing is
	// configured and the window is missing or unparseable, which fails closed.
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
// Parameters:
//   - now time.Time: the instant to place.
//
// Returns:
//   - WebhookWindowState: which of the four states applies.
func WebhookDualDeliveryWindowState(now time.Time) WebhookWindowState {
	start, sunset, resolution := resolveWebhookWindow()

	// A DECLARED retirement is the window's closed end, not an absent window. The
	// difference is what WebhookWindowObstacle acts on: closed is a legitimate state to
	// publish in, unavailable is not.
	if resolution == sunsetRetired {
		return WebhookWindowClosed
	}

	if resolution != sunsetResolved {
		return WebhookWindowUnavailable
	}

	// The CLOSED end is decided by webhookSunsetPassedFor, not by a comparison written
	// here. The pair that would then disagree is exactly the one this file exists to keep
	// in step: "the legacy leg has stopped" and "the webhook routes answer 410".
	instant := now.UTC()
	if webhookSunsetPassedFor(sunset, resolution, instant) {
		return WebhookWindowClosed
	}

	// start came from the SAME read as sunset, so the two ends cannot describe different
	// configurations and the PENDING boundary cannot contradict the CLOSED one. A resolved
	// resolution always carries a start — resolveWebhookWindow derives it from the sunset
	// when none is configured — so there is no second read to fall back to here.
	if instant.Before(start) {
		return WebhookWindowPending
	}

	return WebhookWindowActive
}

// WebhookDualDeliveryActive reports whether the LEGACY HTTP LEG must run for an event
// being dispatched at now.
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
//     instants are meaningful.
func WebhookDeprecationWindow() (time.Time, time.Time, bool) {
	// Delegated so that "the window" has ONE resolution in this package.
	start, sunset, resolution := resolveWebhookWindow()
	if resolution != sunsetResolved {
		return time.Time{}, time.Time{}, false
	}

	return start, sunset, true
}

// WebhookWindowObstacle describes why a process that publishes to Kafka must not start
// on the window state the caller resolved, or nil when the state is a legitimate one to
// run in.
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

// WebhookWindowPendingObstacle describes why a process must not begin publishing to
// Kafka before the dual-delivery window opens, or nil when the state is anything else.
//
// Parameters:
//   - state WebhookWindowState: the state the caller resolved.
//
// Returns:
//   - error: non-nil only for WebhookWindowPending.
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
