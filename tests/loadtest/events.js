/*
Copyright 2024 Blnk Finance Authors.
Apache 2.0
*/

/**
 * Load Test: Outbox-to-Kafka Event Streaming
 *
 * This scenario offers ledger mutations at a constant request rate and then decides two
 * acceptance criteria of the event-streaming pipeline:
 *
 *   V-1  500 events/sec sustained producer throughput, and a p99 outbox-to-Kafka publish
 *        latency under 2 seconds for non-retried events.
 *   V-3  under 0.1% of events reach a dead-letter topic over a 30-minute run at 500
 *        events/sec against a healthy broker.
 *
 * Three properties of the measurement are load-bearing, because each one is a way the
 * verdicts could otherwise be certified from the wrong number:
 *
 *   - SUSTAINED means every window, not the mean. A whole-run average of 500/s is also what
 *     a run that managed 1000/s for half its duration and stalled for the other half
 *     reports, and "sustained" is precisely the claim that distinguishes them. So a second,
 *     single-VU scenario samples the published counter on a bounded interval and the
 *     throughput verdict is the MINIMUM of those per-window rates. The whole-run average is
 *     still reported beside it, as a diagnostic rather than a verdict.
 *   - THE PENDING TAIL BELONGS INSIDE THE POPULATION. When the load stops, the relay is
 *     still working through whatever it has claimed and whatever is still pending. Reading
 *     /metrics at that instant excludes those events from the published counter, from the
 *     dead-letter ratio's population and from the latency histogram — which flatters V-3,
 *     since a dead-letter is precisely what a not-yet-terminal event may still become. So
 *     teardown WAITS, under a bounded budget, for `blnk_outbox_pending` to drain before it
 *     takes the readings the verdicts are computed from, and fails closed if the budget
 *     expires with rows still outstanding.
 *   - THE LATENCY VERDICT HAS EXACTLY ONE ADMISSIBLE SOURCE. V-1's p99 is read from
 *     capture-to-dispatch and from nothing else. `publish_duration` measures a strictly
 *     shorter interval, so certifying the target from it would be optimistic; when
 *     capture-to-dispatch is unavailable, V-1 is INVALIDATED rather than substituted.
 *
 * ALL THREE VERDICTS ARE READ FROM THE SERVER'S /metrics ENDPOINT, NEVER FROM k6's OWN
 * TIMINGS. That distinction is the whole point of this file, so it is worth stating why:
 *
 *   - k6's `http_req_duration` measures how long the API took to ACCEPT a request. The
 *     latency criterion is about how long the event then took to reach Kafka, measured from
 *     its capture inside the ledger transaction. They are different intervals, and reading
 *     the first one would certify a target the pipeline was missing.
 *   - k6's `http_reqs` rate is NOT the event rate, and NOT because of fan-out. One POST
 *     /transactions produces exactly ONE transaction event: `updateTransactionDetails`
 *     normalises the status before the event name is derived, so the whole lifecycle is
 *     announced once, as `transaction.applied`. What breaks the equality is the other
 *     direction — a request the executor could not start produces no event, a transaction the
 *     API rejects produces `transaction.rejected` instead, and provisioning's `ledger.created`
 *     and `balance.created` events belong to no request in the measured window. Reading
 *     iterations/sec as events/sec therefore reports the OFFERED pressure as though it were the
 *     measured result. Throughput is instead the
 *     DELTA of `blnk_events_published_total`, which is incremented ONCE PER EVENT at the
 *     transition that records the Kafka leg as durable. That transition is conditional on the
 *     claim token that authorised the publish and clears or consumes it, so it succeeds for
 *     one worker once in an event's whole life — which is what makes the delta a count of
 *     events rather than of writes even though delivery is at-least-once.
 *   - `blnk_events_broker_acknowledgements_total` is the WRITE count, and no verdict is
 *     computed from it. A republish after a crash increments it a second time for one event,
 *     so reading throughput from it inflates the figure by the redelivery rate and DEFLATES
 *     the dead-letter rate — both in the flattering direction, and both worst when the
 *     pipeline is least healthy. This harness does not scrape it; its ratio to the published
 *     counter is the real redelivery factor and the query is quoted below for an operator.
 *   - `blnk_events_dispatched_total` is per-event too, but it counts the row reaching the
 *     TERMINAL dispatched state, so for a row that still owes a legacy webhook it lags by
 *     that leg's remaining budget. It answers "how many events are completely settled", and
 *     it is reported as the settlement gap against the published count rather than being
 *     substituted for it. The two converge at the sunset.
 *
 * TWO THINGS DEGRADE A RUN RATHER THAN BEING PAPERED OVER, and both used to be reported
 * beside a verdict that still passed:
 *
 *   - THE LATENCY SERIES IS NOT SUBSTITUTABLE. V-1 is read from
 *     `blnk_events_capture_to_dispatch_duration_seconds` and from nothing else. The broker
 *     publish duration measures claim-to-acknowledgement, which excludes the outbox row's
 *     wait for the next poll tick — so a relay an hour behind reports the same sub-second p99
 *     as an idle one. It used to be a recorded fallback; the record made the substitution
 *     visible but the verdict still PASSED from the shorter interval. It is now a diagnostic
 *     that no verdict can read, and an absent capture series fails the run.
 *   - A RESTART INVALIDATES EVERY DELTA. Each figure is the difference between two readings of
 *     a monotonic counter, which means nothing unless both come from the same process.
 *     `process_start_time_seconds` is compared across the two scrapes, because comparing the
 *     counters alone (`end < start`) detects a restart only until the replacement counts past
 *     the old total — seconds, at the target rate. A restart, an unexplained reset, or an
 *     absent process marker all withhold the verdict gauges instead of recording a number
 *     computed across two populations.
 *
 * TWO PROPERTIES OF V-1 CANNOT BE ESTABLISHED FROM A SINGLE PAIR OF SCRAPES, and each has
 * its own machinery below:
 *
 *   - "SUSTAINED 500 events/sec" is not an average. A run that published nothing for fifteen
 *     minutes and then 1000/sec for fifteen minutes averages 500 and sustained it at no
 *     point. So a second scenario samples the published counter on a fixed cadence and each
 *     interval between samples is judged on its own, under a stated tolerance: a named
 *     fraction of the qualifying subwindows must individually reach the target. The
 *     whole-window figure is still reported, because it is the right number for the
 *     dead-letter ratio and a useful summary, but it no longer stands in for sustained.
 *   - THE PIPELINE IS ASYNCHRONOUS AT BOTH ENDS. Provisioning emits ledger.created and
 *     balance.created events that are still sitting in blnk.event_outbox when the load
 *     starts, and the load's own last events are still there when it stops. Publishing the
 *     first group inside the window inflates the numerator with work the load did not
 *     offer; excluding the second deflates it. Both ends are therefore gated on the outbox
 *     going quiet, under a bounded budget, and a pipeline that will not settle fails the run
 *     rather than being measured through.
 *
 * Every verdict carries its provenance — the exact Prometheus series it was computed from,
 * and for the latency figure the bucket boundary it landed in — into the summary JSON under
 * the top-level `blnk_event_streaming` key, so a reviewer can confirm at a glance which
 * series produced which number. The verdicts themselves are k6 thresholds on custom
 * metrics, which is what makes them render as pass/fail rows in the existing dashboard with
 * no change to anything under tools/.
 *
 * Two ways to run it, and the runner is the one to prefer because it forwards the credentials
 * ./.env already exports and writes the summary and the NDJSON stream where the dashboard looks
 * for them:
 *
 *   tests/loadtest/run_case.sh event-streaming
 *
 * `events` is accepted as a second spelling of the same case and normalises onto it, so either
 * invocation writes summary-event-streaming.json and run-event-streaming.ndjson — the two frozen
 * filenames this file names in its own report and the dashboard collects. The runner deliberately
 * restates NONE of the defaults below — it forwards RATE, DURATION, VUS and MAX_VUS only when the
 * caller set them — so a run invoked through it is judged against the criterion's own figures
 * rather than against the transaction cases' 300/s for 30s.
 *
 * Directly, when you want no runner in the way:
 *
 *   k6 run -e URL=http://localhost:5001/transactions \
 *          -e METRICS_URL=http://localhost:5001/metrics \
 *          tests/loadtest/events.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Gauge, Rate, Trend } from "k6/metrics";
// Used for exactly one thing: emitting the created fixtures in a form k6's own console cannot
// mangle. See the LEDGER_PAIRS line in logMeasurement.
import encoding from "k6/encoding";

// uuidv4 and textSummary are DELIBERATELY NOT IMPORTED. They were imported from the jslib CDN,
// and both imports are gone rather than pinned — see the note below for why, and for the local
// reimplementations that replace them. Restoring either import here would restore the finding:
// the two declarations below would also become duplicate lexical bindings, which is an early
// error the ESM parser reports and the CommonJS syntax check does not.

// --- Local helpers, deliberately not imported -----------------------------------------
//
// `uuidv4` and `textSummary` used to be imported from the jslib CDN. k6 resolves a remote
// import by FETCHING AND EXECUTING it in the same VU runtime as the rest of this file, so
// those two URLs were arbitrary third-party code running with everything this scenario
// holds. That matters more here than in the sibling scenarios: this is the only file in the
// directory carrying three credentials at once — API_KEY for the transaction endpoint,
// METRICS_BEARER_TOKEN for /metrics, and MASTER_KEY for the master-key-gated /events/stats
// probe. Nor is such an import pinned by content: the URL names a version, but nothing
// verifies that the bytes served under it today are the bytes that were reviewed.
//
// Both helpers are small and are reimplemented below, so there is no remote module left to
// pin and nothing is fetched at all. That also makes the scenario runnable on an isolated
// network, which a load generator sitting next to the ledger it measures often is.
//
// The sibling scenarios in this directory still import `uuidv4` remotely. They are outside
// this change and each carries only an API key, but the same reasoning applies to them.

/**
 * uuidv4 returns a random RFC 4122 version 4 UUID.
 *
 * Reimplements the k6-utils helper of the same name, including its use of Math.random
 * rather than a CSPRNG: these values are correlation identifiers for generated ledger
 * traffic, not secrets, and a collision would surface as a duplicate-reference request
 * failure rather than as a security property. The version and variant nibbles are set
 * explicitly so the result is a well-formed v4 UUID.
 *
 * @returns {string} a 36-character hyphenated UUID.
 */
function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0;
    var v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * has reports whether an object carries its own property of the given name.
 *
 * Used instead of a bare `in` by the renderers below, matching the style the rest of this
 * file uses, so an inherited key cannot be printed as though it were a metric.
 *
 * @param {Object} obj the object to test.
 * @param {string} key the property name.
 * @returns {boolean} true when the property is the object's own.
 */
function has(obj, key) {
  return obj !== null && obj !== undefined && Object.prototype.hasOwnProperty.call(obj, key);
}

/**
 * paint wraps text in an ANSI colour when colours are enabled and returns it untouched when
 * they are not, so one renderer serves both a terminal and a redirected file.
 *
 * @param {string} text the text to wrap.
 * @param {string} color one of "green", "red", "cyan" or "faint".
 * @param {boolean} enabled whether to emit escape codes at all.
 * @returns {string} the possibly-wrapped text.
 */
function paint(text, color, enabled) {
  if (!enabled) {
    return text;
  }

  var codes = {
    green: "\u001b[32m",
    red: "\u001b[31m",
    cyan: "\u001b[36m",
    faint: "\u001b[2m",
  };
  if (!has(codes, color)) {
    return text;
  }

  return codes[color] + text + "\u001b[0m";
}

/**
 * formatValue renders one metric number, choosing a unit from what the metric contains.
 *
 * k6 reports durations in milliseconds and everything else as a bare number. Sub-second
 * durations stay in milliseconds because that is the scale the V-1 publish-latency ceiling
 * is stated in; anything at or above a second is promoted so a long tail stays legible.
 *
 * @param {number} value the raw value from the summary.
 * @param {boolean} isTime whether the metric's `contains` is "time".
 * @returns {string} the formatted value with its unit.
 */
function formatValue(value, isTime) {
  if (typeof value !== "number" || isNaN(value)) {
    return "-";
  }

  if (!isTime) {
    if (value === Math.floor(value)) {
      return String(value);
    }
    return String(Math.round(value * 10000) / 10000);
  }

  if (Math.abs(value) >= 1000) {
    return (value / 1000).toFixed(2) + "s";
  }

  return value.toFixed(2) + "ms";
}

/**
 * metricSummaryLine renders the aggregates that are meaningful for one metric's type.
 *
 * Each k6 metric type reports a different set of values, and printing the wrong one is how
 * a summary misleads: a Trend has no `count` worth reading, a Gauge has no rate, and a
 * Rate's `rate` is a proportion rather than a per-second figure. Trend percentiles are
 * discovered from the values object rather than hardcoded, so whatever `summaryTrendStats`
 * asks for is what appears.
 *
 * @param {Object} metric the metric object from the summary.
 * @returns {string} a single-line rendering of the metric's aggregates.
 */
function metricSummaryLine(metric) {
  var values = (metric && metric.values) || {};
  var isTime = metric && metric.contains === "time";
  var type = metric && metric.type;
  var parts = [];
  var i;

  if (type === "counter") {
    parts.push("count=" + formatValue(values.count, false));
    if (typeof values.rate === "number") {
      parts.push("rate=" + formatValue(values.rate, false) + "/s");
    }
    return parts.join(" ");
  }

  if (type === "gauge") {
    parts.push("value=" + formatValue(values.value, isTime));
    if (typeof values.min === "number") {
      parts.push("min=" + formatValue(values.min, isTime));
    }
    if (typeof values.max === "number") {
      parts.push("max=" + formatValue(values.max, isTime));
    }
    return parts.join(" ");
  }

  if (type === "rate") {
    if (typeof values.rate === "number") {
      parts.push("rate=" + (values.rate * 100).toFixed(2) + "%");
    }
    if (typeof values.passes === "number") {
      parts.push("passes=" + formatValue(values.passes, false));
    }
    if (typeof values.fails === "number") {
      parts.push("fails=" + formatValue(values.fails, false));
    }
    return parts.join(" ");
  }

  var ordered = ["avg", "min", "med", "max"];
  var percentiles = [];
  for (var key in values) {
    if (has(values, key) && key.charAt(0) === "p") {
      percentiles.push(key);
    }
  }
  percentiles.sort();
  ordered = ordered.concat(percentiles);

  for (i = 0; i < ordered.length; i++) {
    if (typeof values[ordered[i]] === "number") {
      parts.push(ordered[i] + "=" + formatValue(values[ordered[i]], isTime));
    }
  }

  return parts.join(" ");
}

/**
 * renderThresholds appends one line per threshold expression on a metric.
 *
 * A summary that shows numbers but hides which ceilings they broke is the one thing this
 * renderer must not do, because the thresholds ARE this scenario's verdicts: buildOptions
 * expresses each acceptance ceiling as a threshold expression.
 *
 * @param {Object} metric the metric object from the summary.
 * @param {string} indent the indent unit.
 * @param {boolean} colors whether to emit colour.
 * @param {Array<string>} out the accumulating line buffer.
 */
function renderThresholds(metric, indent, colors, out) {
  var thresholds = metric && metric.thresholds;
  if (!thresholds) {
    return;
  }

  for (var expression in thresholds) {
    if (!has(thresholds, expression)) {
      continue;
    }

    // k6 has reported a threshold result both as a bare boolean and as an object carrying
    // an `ok` field. Accept either, and treat any other shape as a failure rather than a
    // pass, so an unrecognised result cannot read as green.
    var result = thresholds[expression];
    var ok = result === true || (result !== null && typeof result === "object" && result.ok === true);
    out.push(
      indent +
        indent +
        paint(ok ? "\u2713" : "\u2717", ok ? "green" : "red", colors) +
        " " +
        expression
    );
  }
}

/**
 * renderGroup renders one group's checks and then recurses into its subgroups.
 *
 * Both shapes k6 has used are handled: checks and groups are arrays in current releases and
 * were objects keyed by name in older ones.
 *
 * @param {Object} group a k6 summary group, starting from data.root_group.
 * @param {string} indent the indent unit.
 * @param {boolean} colors whether to emit colour.
 * @param {Array<string>} out the accumulating line buffer.
 * @param {string} prefix the accumulated indent for this depth.
 */
function renderGroup(group, indent, colors, out, prefix) {
  if (!group) {
    return;
  }

  var i;
  var name;
  var list = [];
  var checks = group.checks;

  if (checks) {
    if (Object.prototype.toString.call(checks) === "[object Array]") {
      list = checks;
    } else {
      for (name in checks) {
        if (has(checks, name)) {
          list.push(checks[name]);
        }
      }
    }

    for (i = 0; i < list.length; i++) {
      var c = list[i] || {};
      var passes = typeof c.passes === "number" ? c.passes : 0;
      var fails = typeof c.fails === "number" ? c.fails : 0;
      var ok = fails === 0 && passes > 0;
      out.push(
        prefix +
          indent +
          paint(ok ? "\u2713" : "\u2717", ok ? "green" : "red", colors) +
          " " +
          (c.name || "(unnamed check)") +
          paint("  " + passes + " passed, " + fails + " failed", "faint", colors)
      );
    }
  }

  var groups = group.groups;
  if (!groups) {
    return;
  }

  if (Object.prototype.toString.call(groups) === "[object Array]") {
    for (i = 0; i < groups.length; i++) {
      out.push(prefix + indent + paint((groups[i] || {}).name || "(unnamed group)", "cyan", colors));
      renderGroup(groups[i], indent, colors, out, prefix + indent);
    }
    return;
  }

  for (name in groups) {
    if (!has(groups, name)) {
      continue;
    }
    out.push(prefix + indent + paint(name, "cyan", colors));
    renderGroup(groups[name], indent, colors, out, prefix + indent);
  }
}

/**
 * textSummary renders a k6 end-of-test summary as plain text.
 *
 * Reimplements the k6-summary helper this file used to import. It is NOT a byte-for-byte
 * clone of that renderer's layout and does not try to be — it reports the same facts from
 * the same summary object: every check, every metric with the aggregates its type actually
 * defines, and every threshold with its pass or fail. handleSummary concatenates the result
 * with renderVerdictBlock, which is where this scenario's own verdicts are spelled out.
 *
 * Exporting handleSummary suppresses k6's built-in summary, so this is the only place the
 * standard metric table is printed. Dropping it rather than reimplementing it would have
 * left an operator reading verdicts with no measurements underneath them.
 *
 * @param {Object} data the summary object k6 passes to handleSummary.
 * @param {Object} [options] rendering options: `indent` (default one space) and
 *   `enableColors` (default false).
 * @returns {string} the rendered summary, newline-terminated.
 */
function textSummary(data, options) {
  var opts = options || {};
  var indent = typeof opts.indent === "string" ? opts.indent : " ";
  var colors = opts.enableColors === true;
  var out = [];
  var i;

  if (data && data.root_group) {
    renderGroup(data.root_group, indent, colors, out, "");
    out.push("");
  }

  var metrics = (data && data.metrics) || {};
  var names = [];
  for (var name in metrics) {
    if (has(metrics, name)) {
      names.push(name);
    }
  }
  names.sort();

  for (i = 0; i < names.length; i++) {
    var metric = metrics[names[i]] || {};
    var line = metricSummaryLine(metric);
    out.push(indent + paint(names[i], "cyan", colors) + (line === "" ? "" : ": " + line));
    renderThresholds(metric, indent, colors, out);
  }

  return out.join("\n") + "\n";
}

/**
 * numberFrom parses an environment value, falling back when it is absent or unusable.
 *
 * The `Number(__ENV.X || fallback)` idiom used elsewhere in this directory silently
 * discards a legitimate "0", because "0" is falsy. Every verdict ceiling here has to be
 * able to express zero — a smoke run relaxes `TARGET_EVENTS_PER_SEC` to 0, and
 * `MAX_DEAD_LETTER_RATIO=0` is the meaningful "no dead letters at all" setting — so the
 * parse is done explicitly instead.
 *
 * @param {string|undefined} raw the raw environment value.
 * @param {number} fallback the value to use when `raw` is absent or not a finite number.
 * @returns {number} the parsed value, or the fallback.
 */
function numberFrom(raw, fallback) {
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    return fallback;
  }

  var parsed = Number(raw);
  if (typeof parsed !== "number" || isNaN(parsed) || !isFinite(parsed)) {
    return fallback;
  }

  return parsed;
}

/**
 * parseDurationSeconds converts a k6 duration string to seconds.
 *
 * k6 accepts a concatenation of magnitude/unit pairs — "30m", "1h30m", "500ms" — and the
 * sustained-throughput sampler needs the run's length as a NUMBER so it can decide which
 * subwindows lie inside the steady-state region. Reading it off the same DURATION the
 * executor was given is the only way the two cannot disagree.
 *
 * A string this cannot parse returns the fallback rather than 0. Returning 0 would make every
 * subwindow fail its qualification test, the qualifying count would stay at zero, and the
 * run would fail for a reason that has nothing to do with the pipeline.
 *
 * @param {string} raw the duration string.
 * @param {number} fallback seconds to use when the string cannot be parsed.
 * @returns {number} the duration in seconds.
 */
function parseDurationSeconds(raw, fallback) {
  var text = String(raw || "").trim();
  if (text === "") {
    return fallback;
  }

  // A bare number is seconds, which is what k6 does with an unsuffixed value.
  if (/^[0-9]+(\.[0-9]+)?$/.test(text)) {
    return Number(text);
  }

  var units = {
    ns: 1e-9,
    us: 1e-6,
    ms: 1e-3,
    s: 1,
    m: 60,
    h: 3600,
  };
  // Longest units first, so "ms" is never read as "m" followed by a stray "s".
  var pattern = /([0-9]+(?:\.[0-9]+)?)(ns|us|ms|h|m|s)/g;
  var total = 0;
  var matched = false;
  var consumed = 0;
  var m;

  while ((m = pattern.exec(text)) !== null) {
    total += Number(m[1]) * units[m[2]];
    consumed += m[0].length;
    matched = true;
  }

  // Every character has to have been part of a magnitude/unit pair. Otherwise "30minutes"
  // would parse as 30 minutes and "3O0m" as 0, both silently.
  if (!matched || consumed !== text.length) {
    return fallback;
  }

  return total;
}

// --- Load generation -------------------------------------------------------------------

const URL = __ENV.URL || "http://localhost:5001/transactions";
const API_KEY = __ENV.API_KEY || __ENV.BLNK_API_KEY;
// The frozen artefact name, and the same one run_case.sh defaults to. A direct `k6 run` of this
// file must land in the file the acceptance record is filed under, or a run invoked without the
// runner produces the right numbers somewhere nothing looks for them.
// Deliberately ONE line: the runner's contract test reads this default out of the file to
// discover the artefact stem, and a wrapped declaration hid it — the runner and the scenario were
// then free to name the same run's output differently with nothing to catch it.
const SUMMARY_OUT = __ENV.SUMMARY_OUT || "tests/loadtest/summary-event-streaming.json";
const SCENARIO = __ENV.SCENARIO || "event_publish";
const DURATION = __ENV.DURATION || "30m"; // V-1 and V-3 are stated over 30 minutes
// The same duration as a number. Derived from DURATION rather than configured separately, so
// the two cannot drift, and it is the SAME NUMBER the throughput divisor uses — see LOAD_SECONDS
// below, which is this constant under the name the verdict arithmetic reads it by.
//
// THE FALLBACK IS ZERO, and deliberately not 1800. An unparseable DURATION used to fall back to
// an assumed thirty minutes here, which is a guess presented as a measurement: the divisor of
// V-1's throughput would then be a number nobody supplied, in whichever direction the real run
// length differed. Zero makes the run report REASON_LOAD_INTERVAL_UNKNOWN and certify nothing,
// which is the same doctrine every other unmeasured input follows in this file. The sampler
// reads it too, and zero disables the sampler rather than enabling it against a guess.
const DURATION_SECONDS = parseDurationSeconds(DURATION, 0);
// Required VUs are roughly arrival rate x response time, so 500/s needs ~150 at 300ms and
// ~500 at 1s. Both stay overridable because the right figure depends on the deployment.
const VUS = numberFrom(__ENV.VUS, 400);
const MAX_VUS = numberFrom(__ENV.MAX_VUS, 1600);
const AMOUNT_MIN = numberFrom(__ENV.AMOUNT_MIN, 100);
const AMOUNT_MAX = numberFrom(__ENV.AMOUNT_MAX, 1000);
const CURRENCY = __ENV.CURRENCY || "USD";

// How many independent aggregates the offered load is spread over, provisioned in setup().
//
// This is the single setting that decides whether the run measures the PIPELINE's throughput
// or one aggregate's serialisation ceiling, so it is worth stating why it exists rather than
// leaving it to look like a tuning knob.
//
// Events are keyed by their aggregate so that per-aggregate ordering holds, and the relay's
// claim enforces that by returning AT MOST ONE ROW PER PARTITION KEY PER BATCH — a later event
// of the same key is not claimable while an earlier one is pending or in flight, which is what
// makes ordering survive concurrent publishing. Within a batch the distinct keys are published
// together, and a tick chains up to 50 batches, so a key whose earlier row has already been
// dispatched can be claimed again later in the same tick. Throughput therefore scales with the
// number of DISTINCT KEYS in flight, not with a larger batch — but there is no 1-event-per-key
// -per-second ceiling.
//
// The `"@" + uuidv4()` shorthand the other scenarios in this directory use does NOT spread the
// load. It mints a fresh BALANCE, and every balance created that way lands in the same default
// ledger, so the whole run shares one partition key and the relay publishes that key's events
// strictly one at a time however much load is offered. Measured directly: 200 transactions
// offered at 20/s yielded 10 published events and 147 rows still pending.
//
// So setup() provisions this many ledgers, each with its own source and destination balance,
// and each iteration picks one pair. Set it to 0 to fall back to the shorthand deliberately —
// which is the right choice for measuring single-aggregate ordering, and the wrong one for
// measuring throughput.
const LEDGER_SPREAD = numberFrom(__ENV.LEDGER_SPREAD, 128);

// --- Measurement source ----------------------------------------------------------------

// /metrics is served on the API port (default 5001) and on the worker monitoring port
// (default 5004). The relay runs in the server role, so the server's endpoint is the one
// that carries the event-streaming instruments.
const METRICS_URL = __ENV.METRICS_URL || "http://localhost:5001/metrics";
// Optional. When server.secure is enabled, /metrics requires a bearer token and answers
// 403 Forbidden without one. The value is never logged and never written to the summary.
const METRICS_BEARER_TOKEN = __ENV.METRICS_BEARER_TOKEN || "";
// Optional corroboration only. GET /events/stats is master-key gated, so it is attempted
// solely when a master key is supplied, and the run never depends on its availability.
const MASTER_KEY = __ENV.MASTER_KEY || __ENV.BLNK_MASTER_KEY || "";
const EVENTS_STATS_URL =
  __ENV.EVENTS_STATS_URL ||
  siblingURL(METRICS_URL, "/metrics", "/events/stats");
// PERF-M02: the drain loop polls this endpoint once a second while waiting for the outbox to
// quiesce, and it wants COUNTS ONLY. `include_offsets=false` says so explicitly rather than
// relying on the endpoint's default: the posture also decides whether the endpoint runs an exact
// COUNT of the dispatched history — 43.2 million index entries a day at this target rate — so a
// poll that took the wrong posture would make the measurement a load source of its own.
//
// The consequence, stated here because it is not obvious: under this posture the response carries
// no `dispatched` key and reports `dispatched_history_counted: false`. Every count the drain loop
// and the quiescence gate actually read — pending, processing, failed, webhook_pending,
// dead_lettered, replaying — is exact and complete on this posture.
const EVENTS_STATS_COUNTS_URL = withQueryParam(
  EVENTS_STATS_URL,
  "include_offsets",
  "false",
);
const LEDGERS_URL =
  __ENV.LEDGERS_URL || siblingURL(URL, "/transactions", "/ledgers");
const BALANCES_URL =
  __ENV.BALANCES_URL || siblingURL(URL, "/transactions", "/balances");

// Refuse a credential-bearing endpoint before anything is logged, requested or summarised.
//
// This runs at module scope so it fires during init, before setup() has echoed a single URL
// and before the first request leaves the process. Authenticating through API_KEY, MASTER_KEY
// or METRICS_BEARER_TOKEN is the supported route: those are sent as headers and are never
// printed, whereas a URL is printed by design.
(function refuseCredentialBearingURLs() {
  var configured = [
    { name: "URL", value: URL },
    { name: "METRICS_URL", value: METRICS_URL },
    { name: "EVENTS_STATS_URL", value: EVENTS_STATS_URL },
    { name: "LEDGERS_URL", value: LEDGERS_URL },
    { name: "BALANCES_URL", value: BALANCES_URL },
  ];

  for (var i = 0; i < configured.length; i++) {
    if (urlCarriesCredential(configured[i].value)) {
      throw new Error(
        configured[i].name +
          " carries userinfo, a query string or a fragment. This script refuses it rather" +
          " than redacting it, because such a URL is written into" +
          " summary-event-streaming.json and CI" +
          " artifacts and is sent on the wire in a way you did not intend. Supply the endpoint" +
          " without credentials and authenticate with API_KEY, MASTER_KEY or" +
          " METRICS_BEARER_TOKEN, which are sent as headers and never printed. Received: " +
          redactURL(configured[i].value),
      );
    }
  }
})();


// The exact interval the load is offered over. V-1's throughput is published events divided by
// THIS, not by the wider scrape-to-scrape window: the window necessarily includes provisioning
// and the tail drain, so dividing by it reports a rate the load never had to sustain and does
// so in the failing direction.
// LOAD_SECONDS and DURATION_SECONDS are ONE quantity under two names, and the alias is kept
// because each name is read by a different half of the file: the verdict arithmetic divides by
// LOAD_SECONDS, the sampler's enablement test reads DURATION_SECONDS. They used to be parsed
// SEPARATELY, by two different helpers, with two different fallbacks — which is how the divisor
// of the throughput verdict and the sampler's idea of the run length came to be able to
// disagree about how long the run was.
const LOAD_SECONDS = DURATION_SECONDS;

// --- Verdict ceilings ------------------------------------------------------------------
// All three are overridable so that a five-second smoke run is not judged against
// thresholds calibrated for a thirty-minute one.

const TARGET_EVENTS_PER_SEC = numberFrom(__ENV.TARGET_EVENTS_PER_SEC, 500);
const MAX_P99_PUBLISH_SECONDS = numberFrom(__ENV.MAX_P99_PUBLISH_SECONDS, 2);
const MAX_DEAD_LETTER_RATIO = numberFrom(__ENV.MAX_DEAD_LETTER_RATIO, 0.001);

// --- Offered load, sized ABOVE the target on purpose ------------------------------------
//
// V-1 asks for 500 events/sec SUSTAINED and the verdict is `value>=500`, so offering exactly
// 500 arrivals/sec makes a flawless run land ON the boundary and any imperfection land below
// it. Every real effect pushes the same way: an arrival the executor could not start is a
// dropped iteration, a transaction the API rejects produces no event, and the measured window
// is never shorter than the load. There is no mechanism that pushes the other way.
//
// So the offered rate carries HEADROOM over the target. The verdict is still judged at 500 —
// the headroom buys margin, it does not move the bar. A run that needs the full 10% to clear
// the threshold is a run whose true sustained rate is close to the target, which is what the
// throughput figure and the offered/published ratio in the summary are for.
const LOAD_HEADROOM_RATIO = numberFrom(__ENV.LOAD_HEADROOM_RATIO, 1.1);
const RATE = numberFrom(
  __ENV.RATE,
  Math.ceil(TARGET_EVENTS_PER_SEC * LOAD_HEADROOM_RATIO),
);

// --- Run mode ---------------------------------------------------------------------------
//
// SMOKE=1 is the explicit opt-out from the acceptance contract: it permits the single-key
// fallback and a short spread, for a five-second shakeout that proves the script runs. It
// must never be set for a run whose numbers are quoted against V-1 or V-3, and the summary
// records which mode produced every figure.
const SMOKE = numberFrom(__ENV.SMOKE, 0) === 1;

// FIXTURES=1 RUNS THIS FILE AGAINST ITSELF instead of against a deployment.
//
// Everything between a /metrics response and the ALL CRITERIA row is pure computation — the
// exposition parser, the label matcher, the histogram collector and its quantile
// interpolation, the counter and bucket deltas, the settling-gate classifier, the summary
// readers and the verdict rules — and none of it was executable in a test. What existed was a
// Go suite that read this file as TEXT and asserted that certain source strings were present,
// which cannot tell a parser that works from one that does not, and could not have caught the
// defect that made `verdicts_all_hold` false on every run this file has ever produced.
//
// So this mode drives those functions over a table of fixtures with expected answers, in the
// real Goja runtime the acceptance run uses, and aborts the test when any of them disagrees.
// It contacts nothing: no broker, no database, no server, no /metrics endpoint. Run it with
//
//	k6 run -e FIXTURES=1 tests/loadtest/events.js
//
// FIXTURES_SELFCHECK=1 inverts one expectation on purpose, which is how the harness proves it
// is capable of failing. A harness that cannot be shown to fail is indistinguishable from one
// that asserts nothing, and event_loadtest_harness_test.go drives both directions.
const FIXTURES = numberFrom(__ENV.FIXTURES, 0) === 1;
const FIXTURES_SELFCHECK = numberFrom(__ENV.FIXTURES_SELFCHECK, 0) === 1;

// Blnk exposes NO delete endpoint for a ledger or a balance — the API has DELETE for
// identities, hooks, api-keys, balance-monitors, matching-rules and subscribers, and for
// nothing else. A run that provisions its own spread therefore changes the database
// PERMANENTLY, and every later run and benchmark sees the accumulated population.
//
// Cleanup cannot be written, so the contract is explicit instead, and one of the three must
// hold before provisioning happens:
//
//   LEDGER_PAIRS          pre-provisioned fixtures, supplied as JSON. Nothing is created.
//   ALLOW_FIXTURE_CREATION=1  an acknowledgement that this run permanently adds up to
//                         LEDGER_SPREAD ledgers and twice that many balances, which is
//                         appropriate for a disposable or per-run database.
//   SMOKE=1               a shakeout, where the fallback is acceptable anyway.
const ALLOW_FIXTURE_CREATION = numberFrom(__ENV.ALLOW_FIXTURE_CREATION, 0) === 1;
const LEDGER_PAIRS_RAW = __ENV.LEDGER_PAIRS || "";
// Fail closed by default: unless this is relaxed to 0, a run that could not read the
// verdict inputs at all fails rather than reporting three vacuous zeroes as a pass.
const REQUIRE_METRICS = numberFrom(__ENV.REQUIRE_METRICS, 1);
// API-acceptance ceilings, in milliseconds. Deliberately NOT the V-1 latency figure; see
// the comment on the threshold itself.
const MAX_API_P95_MS = numberFrom(__ENV.MAX_API_P95_MS, 500);
const MAX_API_P99_MS = numberFrom(__ENV.MAX_API_P99_MS, 1000);
// The floor for the per-response assertions. Matched to the http_req_failed ceiling of 0.001,
// because the two describe the same tolerance from opposite ends: at most one request in a
// thousand may fail, so at most one response in a thousand may be the wrong shape.
const MIN_CHECK_PASS_RATE = numberFrom(__ENV.MIN_CHECK_PASS_RATE, 0.999);
// The ceiling on dropped iterations, expressed as a FRACTION of the arrivals the run intends
// to offer and converted to a count for k6's Counter threshold. A proportion is the right way
// to state it: the same absolute number means something different over five seconds than over
// thirty minutes.
const MAX_DROPPED_ITERATION_RATIO = numberFrom(
  __ENV.MAX_DROPPED_ITERATION_RATIO,
  0.001,
);

/**
 * redactURL reduces a configured URL to the part that is safe to write down.
 *
 * Scheme, host, port and path are kept, because those are what makes a figure interpretable
 * months later. USERINFO, QUERY and FRAGMENT are dropped, because each is a place a
 * credential travels:
 *
 *   https://user:secret@host/metrics   userinfo carries a password
 *   http://host/metrics?token=abc123   a query token is a bearer token in a URL
 *   http://host/metrics#tok            a fragment is not sent to the server but IS logged
 *
 * This matters because these strings do not stay on a terminal. They are copied into
 * `summary-event-streaming.json`, which is committed to CI artifacts and read into dashboards, so a
 * token in a URL becomes a token in durable storage. The marker `(redacted)` is left in place
 * of anything removed rather than deleting it silently, so an operator debugging a 403 can see
 * that the value they set was received and deliberately not printed.
 *
 * The parse is deliberately string-based: k6's URL support varies by build, and a helper that
 * throws inside setup() would fail the run rather than protect it.
 *
 * @param {string} url the configured URL.
 * @returns {string} the URL with every credential-bearing component removed.
 */
function redactURL(url) {
  if (!url) {
    return "";
  }

  var text = String(url);
  var redacted = false;

  var fragmentAt = text.indexOf("#");
  if (fragmentAt >= 0) {
    text = text.substring(0, fragmentAt);
    redacted = true;
  }

  var queryAt = text.indexOf("?");
  if (queryAt >= 0) {
    text = text.substring(0, queryAt);
    redacted = true;
  }

  // Userinfo sits between "://" and the FIRST "/" after it, so a "@" later in the path is not
  // mistaken for a credential separator.
  var schemeAt = text.indexOf("://");
  if (schemeAt >= 0) {
    var authorityStart = schemeAt + 3;
    var pathAt = text.indexOf("/", authorityStart);
    var authorityEnd = pathAt < 0 ? text.length : pathAt;
    var authority = text.substring(authorityStart, authorityEnd);
    var atSign = authority.lastIndexOf("@");
    if (atSign >= 0) {
      text =
        text.substring(0, authorityStart) +
        text.substring(authorityStart + atSign + 1);
      redacted = true;
    }
  }

  return redacted ? text + " (redacted)" : text;
}

/**
 * urlCarriesCredential reports whether a URL has a component this script refuses to accept.
 *
 * Redaction protects the OUTPUT, and it is not sufficient on its own: a credential in a URL is
 * also sent on the wire in a way the operator did not intend, and it can reach places this
 * script does not control — a proxy access log, an error from the HTTP client, a k6 tag. The
 * URL is therefore REFUSED at start-up and the operator is directed at the dedicated
 * credential variables, which are the supported way to authenticate and are never printed.
 *
 * @param {string} url the configured URL.
 * @returns {boolean} true when the URL carries userinfo, a query or a fragment.
 */
function urlCarriesCredential(url) {
  if (!url) {
    return false;
  }

  return redactURL(url).indexOf(" (redacted)") >= 0;
}

// --- Sustained-rate sampling -----------------------------------------------------------
//
// The width of the windows the throughput verdict is stated over. A whole-run average
// cannot distinguish sustained load from a burst followed by a stall, so the published
// counter is sampled on this interval and the verdict is the minimum of the resulting
// per-window rates.
//
// 30 seconds is wide enough that ordinary jitter — a garbage collection pause, one slow
// scrape, a relay poll landing either side of a boundary — averages out inside a window
// instead of failing the run, and narrow enough that a thirty-minute run is judged on
// roughly its last 58 windows rather than on one number. It is also the floor the
// measurement imposes: a window shorter than the server's event-metrics collector tick
// would difference a counter against itself and report zero. Set it to 0 to switch the
// sampler off, which also withdraws its thresholds; the whole-run average then carries the
// throughput verdict on its own.
//
// ONE CADENCE, UNDER THREE ACCEPTED NAMES (PERF-M10). There used to be two constants for this
// single concept and they disagreed: RATE_WINDOW_SECONDS defaulted to 60 and SUBWINDOW_SECONDS
// to 30, while every sleep the sampler performs and every qualification decision it makes read
// the latter. Three things were therefore wrong at once, and none of them was visible in a
// passing run:
//
//   * THE ENABLE GATE used the width the sampler does not measure on. It requires
//     DURATION >= warmup + 2 windows, so at 60 it demanded 180 seconds — and a two-minute run,
//     which comfortably contains four 30-second windows, silently disabled the sampler and fell
//     back to the whole-run mean.
//   * THE SUMMARY reported window_seconds: 60 as "the width of the windows the throughput
//     verdict is stated over" beside subwindow_seconds: 30, and the verdict was stated over 30.
//     A reader was told the wrong width by the artifact whose purpose is provenance.
//   * THE WARM-UP defaulted to 60 and its comment says "one window's width", so it silently
//     excluded TWO measurement windows.
//
// 30 is kept rather than 60 because it is what the code measured all along: MIN_QUALIFYING_
// SUBWINDOWS, MIN_SUSTAINED_SUBWINDOW_RATIO and RAMP_EXCLUSION_SECONDS were all calibrated
// against it, so widening to 60 would halve the sample count and quietly change what the
// tolerance means. Fixing the reporting is a correction; changing the measurement would be a
// different verdict.
//
// SUBWINDOW_SECONDS and SAMPLE_INTERVAL_SECONDS remain accepted as aliases, in that precedence,
// so a runbook or CI job invoking either older name gets the cadence it asked for rather than
// the default.
const RATE_WINDOW_SECONDS = numberFrom(
  __ENV.RATE_WINDOW_SECONDS,
  numberFrom(__ENV.SUBWINDOW_SECONDS, numberFrom(__ENV.SAMPLE_INTERVAL_SECONDS, 30)),
);
// Windows that begin inside this much of the sampler's first reading are measured and
// reported but NOT counted towards the verdict. The arrival-rate executor needs a moment to
// reach its rate and the relay needs a moment to warm up, and neither is a throughput
// deficiency. One window's width by default — and now genuinely one, since there is one width.
const RATE_WINDOW_WARMUP_SECONDS = numberFrom(
  __ENV.RATE_WINDOW_WARMUP_SECONDS,
  RATE_WINDOW_SECONDS,
);

// --- Sustained-window policy -------------------------------------------------------------
//
// The per-subwindow family that decides the SUSTAINED half of V-1. Recovered here beside the
// sampling knobs above: every one of these constants was USED by the code below and declared
// nowhere, which is a ReferenceError at module evaluation rather than dead configuration.
//
// The cadence a subwindow is measured over — WHICH IS THE SAME CADENCE the sampler sleeps on
// and the same one the throughput windows are stated over. It is now an alias of
// RATE_WINDOW_SECONDS rather than a second knob (PERF-M10): two constants for one concept
// disagreed by a factor of two, and the disagreement reached the enable gate, the warm-up
// default and the summary's own provenance. Resolving the environment names happens once, up
// there, where the precedence RATE_WINDOW_SECONDS > SUBWINDOW_SECONDS > SAMPLE_INTERVAL_SECONDS
// is stated.
//
// Kept as a named constant, rather than replaced at its ~10 use sites, because the phrase
// "subwindow" is what the sustained-throughput family is called throughout this file, in the
// summary keys and in tests/loadtest/README.md. Renaming those would be churn; making them
// resolve to one number is the fix.
const SUBWINDOW_SECONDS = RATE_WINDOW_SECONDS;
// The tolerance. 0.95 says: at most one subwindow in twenty may miss the target. It is not 1
// because a single GC pause, a relay lease expiry or a checkpoint on the broker can cost one
// interval without the pipeline having failed to sustain anything; it is not 0.5 because
// that would re-admit exactly the averaging this replaces.
const MIN_SUSTAINED_SUBWINDOW_RATIO = numberFrom(
  __ENV.MIN_SUSTAINED_SUBWINDOW_RATIO,
  0.95,
);
// A floor on the evidence. Without it a run that produced one qualifying subwindow would
// report a 100% pass rate, and "sustained" would have been decided by a single sample.
const MIN_QUALIFYING_SUBWINDOWS = numberFrom(
  __ENV.MIN_QUALIFYING_SUBWINDOWS,
  3,
);
// Ramp exclusion. A constant-arrival-rate executor still has to allocate VUs, and the first
// and last subwindows of any run are partly outside the offered load. Excluding one
// subwindow at each end is what stops the ramp being judged as a throughput failure, and it
// is stated as a duration rather than a count of samples so that changing the cadence does
// not silently change how much of the run is excluded.
const RAMP_EXCLUSION_SECONDS = numberFrom(
  __ENV.RAMP_EXCLUSION_SECONDS,
  SUBWINDOW_SECONDS,
);

// --- Outbox drain ----------------------------------------------------------------------
//
// How often the unsettled depth is re-read while waiting. SETTLE_POLL_SECONDS is honoured as an
// alias.
const DRAIN_POLL_SECONDS = numberFrom(
  __ENV.DRAIN_POLL_SECONDS,
  numberFrom(__ENV.SETTLE_POLL_SECONDS, 5),
);
// --- Settling policy ---------------------------------------------------------------------
//
// The gates that make the measured window contain the load's events and only the load's
// events. Both ends poll the outbox until every row whose KAFKA OUTCOME IS STILL OWED has
// settled, then proceed.
//
// THE POPULATION IS THE TOTAL OF FOUR STATES, not the `pending` literal: pending, processing,
// failed-awaiting-its-dead-letter-write, and replaying. See probeEventStats for why each is in
// and why webhook_pending is out. A gate over `pending` alone reported quiet while the slow and
// FAILING tail was still outstanding, and that tail is enriched in dead letters — so excluding
// it flattered V-3 rather than merely shortening the count.
//
// Quiet has to be observed more than once. A single reading at or below the floor can be the
// gap between two claims rather than an empty outbox.
const DRAIN_STABLE_SAMPLES = numberFrom(__ENV.DRAIN_STABLE_SAMPLES, 3);
// The TOTAL unsettled depth that counts as quiet. Zero is the honest default: any non-zero
// remainder is an event whose terminal outcome — delivered or dead-lettered — is still
// undecided, and therefore missing from V-3's population. It is overridable because a shared
// development database carries other writers' rows, and on such a stack the floor is the
// resting depth rather than zero — but note that a non-zero floor and an acceptance verdict are
// in tension, since those rows are the same contamination REQUIRE_ISOLATION exists to refuse.
// DRAIN_TARGET_ROWS is honoured as an alias.
const DRAIN_FLOOR = numberFrom(
  __ENV.DRAIN_FLOOR,
  numberFrom(__ENV.DRAIN_TARGET_ROWS, 0),
);
// Bounded budgets, so a pipeline that never settles ends the gate instead of hanging. The
// post-load budget is larger because it has the load's own backlog to clear.
// The budgets have a FLOOR that depends on which source answers, and getting it wrong looks
// like a backlog when it is not. Without a master key the depth comes from
// blnk_outbox_pending, which the server republishes on its event-metrics collector tick
// (15s by default), and only ONE independent confirmation can be counted per tick — so the
// budget must be at least DRAIN_STABLE_SAMPLES times that tick, 45s at the defaults, plus
// however long the events themselves take to drain. 120s leaves room for both. With a master
// key the live /events/stats source is used, which needs no tick and settles in seconds.
//
// THE HISTORICAL NAMES ARE HONOURED AS ALIASES. Three generations of this gate each configured
// its budget under its own name — SETTLE_TIMEOUT_SECONDS for the pre-load reading,
// DRAIN_BUDGET_SECONDS and DRAIN_TIMEOUT_SECONDS for the closing one — and all three constants
// coexisted in this file alongside two implementations of the gate itself. A run invoked with any
// of those names must not silently fall back to the default, so each is read here rather than
// being dropped with the constant that used to hold it.
const PRE_DRAIN_BUDGET_SECONDS = numberFrom(
  __ENV.PRE_DRAIN_BUDGET_SECONDS,
  numberFrom(__ENV.SETTLE_TIMEOUT_SECONDS, 120),
);
const POST_DRAIN_BUDGET_SECONDS = numberFrom(
  __ENV.POST_DRAIN_BUDGET_SECONDS,
  numberFrom(
    __ENV.DRAIN_BUDGET_SECONDS,
    numberFrom(__ENV.DRAIN_TIMEOUT_SECONDS, 180),
  ),
);
// Fail closed on a pipeline that would not settle. Relaxing this to 0 keeps the gates and
// their reported figures but stops an unsettled pipeline — or a tail this run could not
// observe completely — from failing the run, which is only ever right for a smoke run, never
// for an acceptance one.
const REQUIRE_DRAIN = numberFrom(__ENV.REQUIRE_DRAIN, 1);

// --- Attribution: the measured population must belong to THIS run ------------------------
//
// EVERY FIGURE HERE IS PROCESS-GLOBAL. The verdicts are deltas of blnk_events_published_total,
// blnk_events_dead_lettered_total and a capture-to-dispatch histogram, all summed across every
// label set the exporting process holds. There is no run identifier, no workload dimension and
// no way to filter one out: a Blnk instance serving other traffic while this scenario runs
// contributes its events to the same counters and its latencies to the same histogram.
//
// The consequences are not symmetric annoyances, they are all in the flattering direction for
// two of the three verdicts. Foreign traffic INFLATES throughput, so V-1 can pass on work this
// run never offered. It enlarges the dead-letter DENOMINATOR, so V-3's ratio falls. And it
// shifts the latency population either way, so the p99 becomes a statement about a mixture.
// Adding a run label to the production instruments is the alternative and it is worse: a
// per-run label on a counter is unbounded cardinality in the server, paid for permanently to
// serve a benchmark.
//
// So the contract is a DEDICATED, ISOLATED Blnk instance, and it is enforced in two ways
// because neither alone is enough:
//
//   1. ISOLATED_INSTANCE=1 is an explicit acknowledgement by the operator. It is required for
//      an acceptance run and cannot be inferred — nothing observable distinguishes "no other
//      client is configured" from "no other client happens to be sending right now".
//   2. An EMPIRICAL IDLE PROBE, taken after the pre-load settling gate and before the baseline
//      scrape, with no load offered: two scrapes ISOLATION_PROBE_SECONDS apart. Any movement in
//      the terminal event counters over that interval is traffic this run did not produce.
//
// The probe's limit is stated rather than glossed: it can DISPROVE isolation and it cannot
// prove it, because a bursty foreign workload can be idle for the length of the probe. That is
// why the acknowledgement is required as well, and why the summary records both.
const ISOLATED_INSTANCE = numberFrom(__ENV.ISOLATED_INSTANCE, 0) === 1;
// Fail closed on an unattributable population, in the same shape as REQUIRE_DRAIN: 0 keeps the
// probe and its reported figures but stops them failing the run. Only ever right for a smoke
// run or a deliberately shared stack whose numbers are not quoted.
const REQUIRE_ISOLATION = numberFrom(__ENV.REQUIRE_ISOLATION, 1);
// How long the idle probe watches. Ten seconds catches any steady foreign traffic at the rates
// that matter — one event per second is 10 events, well above the tolerance — and costs 0.6% of
// a thirty-minute acceptance run. Set it to 0 to skip the probe, which leaves the
// acknowledgement as the only evidence and is recorded as such.
const ISOLATION_PROBE_SECONDS = numberFrom(__ENV.ISOLATION_PROBE_SECONDS, 10);
// How many foreign events the probe tolerates. ZERO is the honest default: the outbox is quiet
// when the probe runs — the pre-load gate has just proved it — so a terminal counter that moves
// at all is another workload's event. Raise it only to accept a known background producer, and
// know that doing so accepts the same amount of contamination in every verdict.
const ISOLATION_MAX_FOREIGN_EVENTS = numberFrom(
  __ENV.ISOLATION_MAX_FOREIGN_EVENTS,
  0,
);

// Where a settling gate read the outbox's pending depth from. Recorded because the two
// sources have different freshness: /events/stats is a live query, the gauge is refreshed on
// the collector's tick.
const DRAIN_SOURCE_NONE = 0;
const DRAIN_SOURCE_STATS = 1;
const DRAIN_SOURCE_GAUGE = 2;

// How a settling gate ended.
const DRAIN_SETTLED = 1;
const DRAIN_NOT_SETTLED = 0;


// The sampler is enabled only when it was asked for AND the run is long enough for the
// warm-up to elapse and counted windows to follow it. Without the second condition a
// deliberately short smoke run would fail the "at least one window was counted" guard it
// never had the duration to satisfy, and the operator would have to know to switch the
// sampler off by hand. The margin is two window widths past the warm-up, which is what makes
// the guard hold rather than merely be likely to.
const RATE_SAMPLER_ENABLED =
  RATE_WINDOW_SECONDS > 0 &&
  DURATION_SECONDS >= RATE_WINDOW_WARMUP_SECONDS + 2 * RATE_WINDOW_SECONDS;
const SAMPLER_SCENARIO = "event_publish_rate_sampler";

// How often the sampler scenario takes an interval throughput reading during the run. Short
// enough that a thirty-minute run yields enough observations for the median to be a statement
// about, long enough that each delta is large relative to the collector's own publish tick.
//
// AN ALIAS OF SUBWINDOW_SECONDS, not a second knob. The sampler sleeps SUBWINDOW_SECONDS between
// readings and the qualification arithmetic is stated in it, so a separate value here was simply
// a wrong number reported as the interval the samples were taken over — 15 in the summary beside
// intervals that were actually 30 seconds long, which is a factor of two on every figure a
// reader might recompute. Both environment names are accepted; see SUBWINDOW_SECONDS above.
const SAMPLE_INTERVAL_SECONDS = SUBWINDOW_SECONDS;

/**
 * siblingURL rewrites the trailing path of a configured URL, so the endpoints this scenario
 * needs alongside the one it was given require no extra environment variables in the common
 * case where one process serves them all. Each is still overridable on its own.
 *
 * @param {string} url the configured URL.
 * @param {string} marker the trailing path to replace.
 * @param {string} replacement the path to put in its place.
 * @returns {string} the rewritten URL, or "" when the marker is absent.
 */
function siblingURL(url, marker, replacement) {
  var trimmed = String(url || "");
  var at = trimmed.lastIndexOf(marker);
  if (at < 0) {
    return "";
  }

  return trimmed.substring(0, at) + replacement;
}

/**
 * withQueryParam appends one query parameter to a URL, honouring whatever query string the URL
 * already carries and leaving an existing value for the same name alone.
 *
 * It exists because EVENTS_STATS_URL is OVERRIDABLE. The probe must send `include_offsets=false`
 * so a drain poll cannot trigger a broker round trip and a history-sized count (PERF-M02), and
 * naive concatenation would produce `...?a=b?include_offsets=false` for an operator who supplied
 * their own query string. Leaving an existing value alone is deliberate too: an operator who
 * deliberately set a posture on the override has said what they want, and silently replacing it
 * would make the override a lie.
 *
 * An empty URL stays empty, because "" is how this scenario represents "no such endpoint" and
 * turning it into "?include_offsets=false" would make a missing endpoint look configured.
 *
 * @param {string} url the base URL, possibly already carrying a query string.
 * @param {string} name the parameter name.
 * @param {string} value the parameter value.
 * @returns {string} the URL with the parameter present.
 */
function withQueryParam(url, name, value) {
  var base = String(url || "");
  if (!base) {
    return "";
  }

  var at = base.indexOf("?");
  if (at < 0) {
    return base + "?" + name + "=" + value;
  }

  var query = base.substring(at + 1).split("&");
  for (var i = 0; i < query.length; i++) {
    if (query[i] === name || query[i].indexOf(name + "=") === 0) {
      return base;
    }
  }

  return base + "&" + name + "=" + value;
}

// ---------------------------------------------------------------------------------------
// The Prometheus series the verdicts are read from.
//
// Every name below is the frozen exposition name of an instrument declared in
// internal/metrics/metrics.go and catalogued in docs/metrics.md. None of them is invented
// here, and none is substituted for another when it is missing: a missing series is
// reported as missing.
//
// Each entry is a candidate list rather than a single string. The exporter's suffixing is a
// runtime property — a counter named `blnk.events.published.total` is exported as
// `blnk_events_published_total`, but the trailing `_total` is applied by the exporter and
// could legitimately be configured off — so the documented name is tried first and the
// bare form second. Whichever one matched is recorded as the verdict's provenance, which
// makes the tolerance visible instead of silent and doubles as the debugging aid when a
// name fails to resolve at all.
// ---------------------------------------------------------------------------------------

// The SETTLED count, reported as a supporting figure and used for NO verdict. It is
// incremented once per event, at the terminal dispatched transition — conditional on the
// claim token that authorised the publish, so it succeeds for one worker once in an event's
// life. It is per-event and cannot double-count; what makes it unsuitable for the verdicts is
// WHEN it moves. A row whose legacy webhook is still owed sits in webhook_pending and is not
// counted here until that leg settles, so during the dual-delivery window this count trails
// the published count by the population still owed a webhook. Its ratio to that count is the
// settlement gap, which is worth reporting; substituted for it, throughput would be
// understated by however much of the legacy leg was still outstanding at the closing scrape.
const SERIES_DISPATCHED = [
  "blnk_events_dispatched_total",
  "blnk_events_dispatched",
];
// The EVENT count, and the series every per-event verdict is computed from. It is incremented
// once per event, at the transition that records the Kafka leg as durably delivered — either
// MarkEventDispatched or MarkEventWebhookPending, both conditional on the claim token and both
// clearing or consuming it, so each succeeds for one worker once in an event's life. That
// uniqueness is what makes a throughput figure and a dead-letter rate derived from it mean
// events rather than writes.
//
// It is NOT the broker write count. That is blnk_events_broker_acknowledgements_total, which
// this harness deliberately does not read: it moves again on every republish, and substituted
// here it would over-report throughput and halve the apparent dead-letter rate.
const SERIES_PUBLISHED = [
  "blnk_events_published_total",
  "blnk_events_published",
];
const SERIES_DEAD_LETTERED = [
  "blnk_events_dead_lettered_total",
  "blnk_events_dead_lettered",
];
const SERIES_PUBLISH_ATTEMPTS = [
  "blnk_events_publish_attempts_total",
  "blnk_events_publish_attempts",
];
// V-1's latency series. capture_to_dispatch, NOT publish_duration: the target is stated
// over the interval a subscriber actually waits, which begins when the ledger transaction
// committed the outbox row. publish_duration's clock starts at the relay CLAIM, so it
// excludes the row waiting for the next poll tick, the poll interval and the claim query
// itself — a relay an hour behind would report the same sub-second p99 as an idle one.
const SERIES_CAPTURE_TO_DISPATCH = [
  "blnk_events_capture_to_dispatch_duration_seconds",
  "blnk_events_capture_to_dispatch_duration",
];
// The broker write in relative isolation. Reported ALONGSIDE the verdict and never AS it:
// it measures a strictly shorter interval, so substituting it for the absent
// capture_to_dispatch would certify V-1 from a number that cannot fail the way V-1 can. When
// capture_to_dispatch is missing, V-1 is invalidated instead — see the reason chain in
// teardown.
//
// It is reported because the gap between the two figures is what tells an operator whether a
// slow end-to-end reading is the broker or a relay backlog. Read that gap as an indication of
// where the time goes and not as a quantile of anything: the difference of two p99s is not
// the p99 of the difference, because the two are quantiles of different populations and the
// event at the 99th percentile of one need not be the event at the 99th percentile of the
// other.
const SERIES_PUBLISH_DURATION = [
  "blnk_events_publish_duration_seconds",
  "blnk_events_publish_duration",
];
const SERIES_OUTBOX_PENDING = ["blnk_outbox_pending"];
// THE REPAIR BACKLOGS, which blnk_outbox_pending does NOT include (PERF-C01).
//
// That gauge is pending plus processing and nothing else. Two further non-terminal states exist
// and are published only here, attributed by `leg`:
//
//   leg="dead_letter"    -- `failed`: the retry budget is spent and the .dlt write is STILL OWED
//   leg="legacy_webhook" -- `webhook_pending`: the Kafka leg is durable, the webhook is owed
//
// The first matters most to a verdict. An event in `failed` has not yet incremented
// blnk_events_dead_lettered_total, so treating the outbox as drained while such rows remain
// removes them from V-3's NUMERATOR — understating the dead-letter rate, which is the direction
// that lets a failing pipeline certify. Both legs fill during an outage, all at once, which is
// precisely when a drain gate must not conclude the run is finished.
//
// Summed across legs deliberately: the gate asks "is anything still owed", not which leg owes it.
const SERIES_REPAIR_BACKLOG = ["blnk_events_repair_backlog"];
// Not a verdict input. The settling gates need to know whether a pending reading is FRESH:
// blnk_outbox_pending is republished by the server's event-metrics collector on its own
// tick, so polling faster than that tick returns the same number repeatedly, and three
// identical readings would satisfy a naive stability rule without a single new observation
// having been made. This gauge resets towards zero on each successful collection, so a
// DECREASE in it is proof that a new collection happened between two polls.
const SERIES_COLLECTION_AGE = [
  "blnk_event_metrics_last_collection_age_seconds",
];
// THE PROCESS IDENTITY, and the only thing here that can positively identify a restart
// (PERF-P03). Every verdict below is a DELTA between two scrapes of a monotonic counter, and
// a delta is only meaningful while both readings come from the same process: a restart resets
// every counter to zero, so the baseline belongs to a process that no longer exists.
//
// Comparing the two readings cannot detect that reliably. `end < start` catches a restart
// only when the new process has not yet counted past the old one's total — and at 500 events
// a second it passes that point in seconds, after which the arithmetic silently produces a
// delta that is neither the old process's work nor the new one's. On a long run the likely
// case is the undetectable one.
//
// process_start_time_seconds is the Prometheus client's own standard process-collector gauge:
// the wall-clock instant this process started, constant for its whole life and different for
// any replacement. A CHANGE in it between the two scrapes is a restart, whatever the counters
// happen to show, and its ABSENCE means the run cannot rule one out.
const SERIES_PROCESS_START = ["process_start_time_seconds"];

// --- Measurement scope -------------------------------------------------------------------
//
// HOW MANY SERVER PROCESSES THE METRICS ENDPOINT FRONTS (PERF-M08).
//
// Every verdict here is a delta of a PER-PROCESS counter read from one URL. That is sound
// against a single process and unsound against several: behind a Kubernetes Service, or any
// load balancer, consecutive scrapes reach ARBITRARY backends, so the "delta" mixes counters
// that never belonged to one series. The failure is silent and biased — a scrape landing on a
// less busy replica looks like a throughput dip, and one landing on a busier replica looks
// like a burst — and the repository ships exactly that topology: server-deployment.yaml is
// horizontally scaled by server-hpa.yaml between 2 and 10 replicas.
//
// So the assumption is DECLARED rather than assumed. 1 says "this endpoint is one process",
// which is true of the Compose stack, of a port-forward to a single pod, and of a local
// binary. Anything greater says the endpoint fronts several, and the acceptance verdicts are
// then withheld with a reason naming this — because the correct instrument for a scaled
// deployment is a Prometheus query that aggregates across replicas, which is quoted in the
// summary's equivalent_promql for every verdict and is not something this harness can perform
// from a single scrape.
//
// Declaring 1 when it is false does not make the measurement sound; it makes the harness
// believe you. The per-sample identity check above is the backstop that catches it anyway.
const SERVER_REPLICAS = numberFrom(__ENV.SERVER_REPLICAS, 1);

// The closed `attempt`, `outcome` and `terminal` label domains, per docs/metrics.md.
// FIRST_ATTEMPT is the population both latency targets are stated over; DISPATCHED
// additionally narrows publish_duration to successful writes, without which that filter
// matches nothing at all.
//
// The outcome vocabulary is THREE values. "failed" is not one of them: a failed attempt
// reports outcome="retrying", and whether anything further will be tried is carried by the
// separate `terminal` dimension. Selecting outcome="failed" therefore matches nothing at
// all, which would silently read as "no events are stuck".
const FIRST_ATTEMPT = "1";
const OUTCOME_DISPATCHED = "dispatched";
const OUTCOME_RETRYING = "retrying";
const OUTCOME_DEAD_LETTERED = "dead_lettered";
const TERMINAL_TRUE = "true";
const TERMINAL_FALSE = "false";

// The quantile V-1 is stated at.
const TARGET_QUANTILE = 0.99;

// Numeric codes, recorded into gauges because a gauge carries a number and the strings they
// stand for are reconstituted in handleSummary. Each map is the single definition of its
// vocabulary.
const SERIES_CODE_ABSENT = 0;
const SERIES_CODE_DOCUMENTED_NAME = 1;
const SERIES_CODE_TOLERATED_NAME = 2;

// P99_SOURCE_* names WHICH INSTRUMENT a latency figure came from. Only
// CAPTURE_TO_DISPATCH is ever a verdict source; PUBLISH_DURATION exists solely to label the
// supporting broker-write figure, which is reported and never thresholded. NONE is what the
// verdict source becomes when the canonical histogram is absent, and it travels with
// REASON_NO_FIRST_ATTEMPT_SAMPLES rather than with a substituted number.
const P99_SOURCE_NONE = 0;
const P99_SOURCE_CAPTURE_TO_DISPATCH = 1;
// RESERVED AND NEVER EMITTED. publish_duration was once a fallback source for the V-1 p99;
// it is now a diagnostic that cannot certify anything, because its clock starts at the relay
// claim and so omits the queue wait. The code keeps its number so that an artefact from a run
// that did emit a 2 still decodes, and so the numbering of any code added later does not
// shift under a reader comparing two summaries.
const P99_SOURCE_PUBLISH_DURATION = 2;

const INTERPOLATION_NONE = 0;
const INTERPOLATION_LINEAR = 1;
const INTERPOLATION_HIGHEST_FINITE_BOUND = 2;
const INTERPOLATION_LOWEST_BUCKET = 3;

const REASON_AVAILABLE = 0;
const REASON_START_SCRAPE_UNAVAILABLE = 1;
const REASON_END_SCRAPE_UNAVAILABLE = 2;
const REASON_WINDOW_NOT_POSITIVE = 3;
// Named for the PUBLISHED series, which is the one every per-event verdict is read from. It
// used to be keyed on the dispatched series, and after the verdict source moved an absent
// dispatched series degraded a run that was perfectly measurable while an absent PUBLISHED
// series fell through to REASON_NO_TERMINAL_EVENTS and reported a no-op publisher. The numeric
// code is deliberately unchanged so an artefact from an earlier run still decodes to the same
// slot.
const REASON_PUBLISHED_SERIES_ABSENT = 4;
const REASON_NO_TERMINAL_EVENTS = 5;
const REASON_NO_FIRST_ATTEMPT_SAMPLES = 6;
// The dead-letter counter was ABSENT rather than zero, and no authenticated stats source
// corroborated it. An absent counter and a genuine zero are indistinguishable once absence is
// coerced to 0, and the coercion favours a pass: 0/N is a 0% dead-letter rate, which clears
// V-3 while proving nothing about it. A broken exporter must not certify a delivery target.
const REASON_DEAD_LETTER_UNMEASURED = 7;
// A required counter or histogram RESET inside the window, so the delta describes only the
// fraction of the run after the restart. A relay restarted twenty-nine minutes into a
// thirty-minute run would otherwise certify a sustained-throughput target from one minute.
// RESERVED AND NO LONGER EMITTED, superseded by REASON_COUNTER_RESET and
// REASON_EXPORTER_RESTARTED below, which say whether the reset had an attributable cause. The
// code keeps its number so an artefact from a run that did emit an 8 still decodes.
const REASON_SERIES_RESET = 8;
// The outbox backlog had not drained when the closing scrape was due. The events still in the
// outbox belong to the load that was offered, so counting the arrivals without them
// understates throughput, and the load interval is the divisor either way.
// RESERVED AND NO LONGER EMITTED, superseded by REASON_PIPELINE_NOT_SETTLED, which checks BOTH
// settling gates rather than only the closing one and honours REQUIRE_DRAIN. Emitting this one
// after that check would have defeated the knob: with REQUIRE_DRAIN=0 the run is meant to
// proceed on an unsettled pipeline and report the figures, not to be degraded anyway.
const REASON_BACKLOG_NOT_DRAINED = 9;
// The load interval could not be parsed, so V-1's divisor is unknown. Reporting a rate
// requires knowing what it is a rate over.
const REASON_LOAD_INTERVAL_UNKNOWN = 10;
// Acceptance mode was asked for without the aggregate spread it requires. With a single
// partition key the relay publishes one event per poll interval whatever the offered load, so
// the run measures one aggregate's serialisation ceiling and cannot speak to V-1 at all.
const REASON_SPREAD_INSUFFICIENT = 11;
// CODES 12 THROUGH 16 WERE USED BY THE CASCADE IN teardown AND DECLARED NOWHERE, which is a
// ReferenceError at module evaluation — the whole scenario failed to load rather than any one
// verdict failing. They are numbered from 12 rather than from the 7-to-10 range they were
// originally written in, because 7 through 11 above are already taken by other codes: a numeric
// vocabulary assembled from more than one source cannot preserve every source's numbering, and
// preserving the MEANINGS is what matters to a reader of a summary.
//
// Distinct from code 6, and the distinction leads an operator to a different place: 6 is "the
// pipeline produced no first-attempt samples", while this is "the series V-1 is defined over is
// not exported at all", which is a build or a configuration predating
// blnk_events_capture_to_dispatch_duration_seconds. The broker publish duration is NOT
// substituted for it — see the note at the verdict.
const REASON_CAPTURE_SERIES_ABSENT = 12;
// The exporter restarted between the two scrapes, so the baseline belongs to a process that no
// longer exists and every counter delta is arithmetic across two populations. Checked BEFORE the
// reset below, because a restart is the CAUSE and a backwards counter is its symptom.
const REASON_EXPORTER_RESTARTED = 13;
// A counter went BACKWARDS without an observed restart, so this run could not attribute the
// reset. Reported separately from the restart above so the two are distinguishable: one is a
// known cause, the other is an unexplained one.
const REASON_COUNTER_RESET = 14;
// process_start_time_seconds was absent from at least one scrape, so a restart can be neither
// confirmed nor ruled out. Fails closed rather than assuming continuity, because assuming it is
// exactly how an undetected restart certifies a target.
const REASON_PROCESS_IDENTITY_UNKNOWN = 15;
// The outbox would not go quiet inside its budget at one end of the window or the other, so the
// window's contents are not the load's events. This is the two-gate successor to code 9: it
// covers BOTH ends, and it honours REQUIRE_DRAIN.
const REASON_PIPELINE_NOT_SETTLED = 16;
// A settling gate could not read the WHOLE unsettled population, so quiescence was never
// established even though the depth it COULD read was at the floor. Distinguished from code 16
// because it sends an operator somewhere completely different: 16 is a relay that is behind,
// this is a measurement that cannot see the tail — blnk_outbox_pending carries pending plus
// processing only, so without a master key the failed-awaiting-dead-letter rows and any replay
// in flight are invisible, and those are precisely the rows whose exclusion flatters V-3. The
// remedy is a master key, not a bigger budget.
const REASON_SETTLEMENT_STATE_UNAVAILABLE = 17;
// The measured population cannot be attributed to THIS run: the idle probe saw terminal event
// counters move while no load was offered, or the isolated-instance contract was not
// established at all. Every verdict here is a delta of process-global counters, so foreign
// traffic inflates throughput, dilutes the dead-letter rate and mixes the latency population —
// all in the passing direction for two of the three verdicts.
const REASON_INSTANCE_NOT_ISOLATED = 18;
// The measurement was taken across MORE THAN ONE PROCESS (PERF-M08). Appended at the END of the
// range rather than inserted, because these codes are written into published artifacts and
// renumbering would change what an archived summary means.
const REASON_MEASUREMENT_NOT_SINGLE_PROCESS = 19;

// Where V-3's dead-letter figure came from. NONE is a failing state, not a zero: it is the
// difference between "no events were dead-lettered" and "nothing could tell us either way".
const DEAD_LETTER_SOURCE_NONE = 0;
const DEAD_LETTER_SOURCE_SERIES = 1;
const DEAD_LETTER_SOURCE_STATS_ENDPOINT = 2;

// A scrape that never reached the server has no HTTP status; k6 reports status 0 for a
// transport failure, and that is recorded verbatim rather than mapped to something else.
const STATUS_UNREACHABLE = 0;

// ---------------------------------------------------------------------------------------
// Custom metrics, registered in the init context.
//
// This is not a stylistic choice. handleSummary cannot make HTTP requests, and it cannot
// see module state assigned in setup() or teardown() — each lifecycle stage runs in its own
// context. A custom metric registered here is the only reliable channel from a
// teardown-time computation into the summary JSON, and a threshold on one is what turns the
// computed number into an auditable pass/fail row that the existing dashboard renders
// without any change under tools/.
//
// Every one is a Gauge because every one is a single end-of-run value. A Counter would also
// publish a k6-derived `rate` — count divided by the k6 run duration — and that second,
// differently-derived throughput number sitting beside the authoritative one is precisely
// the confusion this file exists to prevent.
// ---------------------------------------------------------------------------------------

// The three verdicts, plus the guard that says whether they mean anything.
const M_EVENTS_PER_SEC = "event_publish_events_per_second";
// The INTERVAL-AWARE throughput series, and the one V-1's sustained-rate claim is actually read
// from. See intervalEventsPerSecond for why a two-point average over the whole run is not
// evidence of a sustained rate.
const M_INTERVAL_EVENTS_PER_SEC = "event_publish_interval_events_per_second";
const M_P99_SECONDS = "event_publish_p99_seconds";
const M_DEAD_LETTER_RATIO = "event_publish_dead_letter_ratio";
const M_VERDICTS_AVAILABLE = "event_publish_verdicts_available";

// The sustained-rate verdict. A Trend rather than a Gauge because the claim is about EVERY
// window and a Gauge keeps only the last value; `min` over this series is the weakest window
// the run contained, which is what "sustained" means.
const M_WINDOW_EVENTS_PER_SEC = "event_publish_window_events_per_second";
const M_RATE_WINDOWS_COUNTED = "event_publish_rate_windows_counted";
const M_RATE_WINDOWS_SKIPPED = "event_publish_rate_windows_skipped";
const M_RATE_WINDOW_WIDTH = "event_publish_rate_window_seconds";

// Raw inputs, so the arithmetic behind each verdict can be re-done by hand from the JSON.
const M_WINDOW_SECONDS = "event_publish_window_seconds";
// Declared for the same reason as the two above: offeredWindowSeconds.add is called and the
// metric name it needs was lost.
const M_OFFERED_WINDOW_SECONDS = "event_publish_offered_window_seconds";
const M_DISPATCHED_START = "event_publish_dispatched_start";
const M_DISPATCHED_END = "event_publish_dispatched_end";
const M_DISPATCHED_DELTA = "event_publish_dispatched_delta";
const M_PUBLISHED_START = "event_publish_published_start";
const M_PUBLISHED_END = "event_publish_published_end";
const M_PUBLISHED_DELTA = "event_publish_published_delta";
const M_DEAD_LETTERED_START = "event_publish_dead_lettered_start";
const M_DEAD_LETTERED_END = "event_publish_dead_lettered_end";
const M_DEAD_LETTERED_DELTA = "event_publish_dead_lettered_delta";
const M_LATENCY_COUNT_START = "event_publish_latency_count_start";
const M_LATENCY_COUNT_END = "event_publish_latency_count_end";
const M_LATENCY_COUNT_DELTA = "event_publish_latency_count_delta";
const M_LATENCY_SUM_START = "event_publish_latency_sum_start";
const M_LATENCY_SUM_END = "event_publish_latency_sum_end";
const M_LATENCY_SUM_DELTA = "event_publish_latency_sum_delta";
const M_P99_BUCKET_LOWER = "event_publish_p99_bucket_lower_seconds";
const M_P99_BUCKET_UPPER = "event_publish_p99_bucket_upper_seconds";
const M_P99_INTERPOLATION_CODE = "event_publish_p99_interpolation_code";
const M_P99_SOURCE_CODE = "event_publish_p99_source_code";
const M_P99_SERIES_SPELLING_CODE = "event_publish_p99_series_spelling_code";
const M_BROKER_WRITE_P99 = "event_publish_broker_write_p99_seconds";
// NOT a percentile, and named so it cannot be read as one.
//
// It is the difference of two INDEPENDENTLY RANKED p99 values, and the p99 of a difference is
// not the difference of p99s. The two ranks come from different populations at different points
// in each distribution, so the figure can exceed the true p99 queue wait, understate it, or
// come out NEGATIVE when the broker-write histogram's coarser buckets place its p99 above the
// end-to-end one. It remains a useful ORDER-OF-MAGNITUDE diagnostic — a large positive value
// does mean backlog rather than broker — which is why it is kept and renamed rather than
// deleted. Neither the metric name nor the gauge nor the PromQL constant says "queue wait"
// any more; anything quoting a queue-wait percentile must instrument queue wait directly.
const M_P99_DIFFERENCE = "event_publish_p99_difference_seconds";
const M_ATTEMPTS_DISPATCHED = "event_publish_attempts_dispatched_delta";
const M_ATTEMPTS_RETRYING = "event_publish_attempts_retrying_delta";
const M_ATTEMPTS_TERMINAL = "event_publish_attempts_terminal_delta";
const M_ATTEMPTS_DEAD_LETTERED = "event_publish_attempts_dead_lettered_delta";
// The four rows that make the two new fail-closed conditions auditable rather than merely
// effective: a reader can see WHY a run reported as unavailable, and how long the wait took on a
// run that settled normally.
const M_BASELINE_COMPLETE = "event_publish_baseline_complete";
const M_BACKLOG_SETTLED = "event_publish_backlog_settled";
const M_BASELINE_SETTLE_SECONDS = "event_publish_baseline_settle_seconds";
const M_FINAL_SETTLE_SECONDS = "event_publish_final_settle_seconds";
const M_BACKLOG_PENDING_AT_SETTLE = "event_publish_backlog_pending_at_settle";

const M_OUTBOX_PENDING_START = "event_publish_outbox_pending_start";
const M_OUTBOX_PENDING_END = "event_publish_outbox_pending_end";
const M_DRAIN_WAIT_SECONDS = "event_publish_drain_wait_seconds";
const M_DRAIN_COMPLETE = "event_publish_drain_complete";
const M_DRAIN_POLLS = "event_publish_drain_polls";
// event_publish_settlement_gap_factor and event_publish_dispatched_series_code were USED by this
// script and not DECLARED in it, which is a ReferenceError at run time rather than a missing
// figure: `node --check` parses the file without resolving identifiers, so the gap survived
// every syntax gate. Both are restored here beside their siblings.
const M_SETTLEMENT_GAP_FACTOR = "event_publish_settlement_gap_factor";
const M_DISPATCHED_SERIES_CODE = "event_publish_dispatched_series_code";
const M_PUBLISHED_SERIES_CODE = "event_publish_published_series_code";
const M_DEAD_LETTERED_SERIES_CODE = "event_publish_dead_lettered_series_code";
const M_METRICS_STATUS_START = "event_publish_metrics_status_start";
const M_METRICS_STATUS_END = "event_publish_metrics_status_end";
const M_COUNTER_RESET = "event_publish_counter_reset";
// PERF-P03. The process identity at each end of the window, and whether it changed. Recorded
// as three separate figures rather than one boolean because the boolean alone leaves an
// operator unable to tell a restart from an absent marker: -1 on either side means the
// exporter did not publish process_start_time_seconds for that scrape, so continuity is
// UNKNOWN rather than intact.
const M_PROCESS_START_BEFORE = "event_publish_process_start_before";
const M_PROCESS_START_AFTER = "event_publish_process_start_after";
const M_EXPORTER_RESTARTED = "event_publish_exporter_restarted";
const M_DEGRADED_REASON_CODE = "event_publish_degraded_reason_code";
const M_STATS_STATUS = "event_publish_stats_endpoint_status";
const M_STATS_DISPATCHED = "event_publish_stats_dispatched";
const M_STATS_DEAD_LETTERED = "event_publish_stats_dead_lettered";
const M_STATS_PENDING = "event_publish_stats_pending";
// The rest of the outbox census, and the reason it is here rather than only in a log line: the
// settling gates are computed from the TOTAL of these states, so a reader who wants to check the
// gate's arithmetic needs the components. `failed` in particular is the row a run must not close
// its window over — its retry budget is spent and its dead-letter write is still owed, so it has
// reached neither terminal counter.
const M_STATS_PROCESSING = "event_publish_stats_processing";
const M_STATS_FAILED = "event_publish_stats_failed";
const M_STATS_REPLAYING = "event_publish_stats_replaying";
const M_STATS_UNSETTLED = "event_publish_stats_unsettled";
// V-3's numerator when the exported counter is absent: the census reading taken in setup, and
// the SAME-WINDOW delta against the final one. Recorded so the substitution is auditable — a
// provenance field that named the stats endpoint while the numerator silently stayed at zero is
// the defect these two rows close.
const M_STATS_DEAD_LETTERED_START = "event_publish_stats_dead_lettered_start";
const M_STATS_DEAD_LETTERED_DELTA = "event_publish_stats_dead_lettered_delta";
const M_PARTITION_KEYS = "event_publish_partition_keys";
// The exact interval the load was offered over, and the divisor V-1's throughput is stated
// over. Recorded so the rate in the summary can be recomputed by hand from the delta.
const M_LOAD_SECONDS = "event_publish_load_seconds";
// The offered arrival rate, which carries headroom over the target on purpose, and the ratio
// of published events to offered arrivals. A ratio far from 1 means the run measured something
// other than the load it thought it offered.
const M_OFFERED_RATE = "event_publish_offered_rate";
// 1 when the outbox backlog reached zero before the closing scrape, 0 when the drain timed out
// and the measurement is therefore short by whatever was still queued.
const M_BACKLOG_DRAINED = "event_publish_backlog_drained";
const M_DRAIN_SECONDS = "event_publish_drain_seconds";
// The dead-letter counter's provenance: whether V-3 rests on an explicit series, on the
// authenticated stats endpoint, or on nothing at all.
const M_DEAD_LETTER_SOURCE = "event_publish_dead_letter_source";
// How many sampler readings came from a different process than their predecessor (PERF-M08).
// A Gauge rather than a Counter because the useful figure is the final total for the run, and
// because it must publish 0 for a clean run rather than be absent.
const M_SAMPLER_IDENTITY_CHANGES = "event_publish_sampler_identity_changes";

// The sustained-throughput family. These are NOT Gauges: the whole point is that there is
// one observation per subwindow rather than one per run.
const M_SUBWINDOW_EVENTS_PER_SEC = "event_publish_subwindow_events_per_second";
const M_SUBWINDOW_MET_TARGET = "event_publish_subwindow_met_target";
const M_SUBWINDOWS_QUALIFYING = "event_publish_subwindows_qualifying";
const M_SUBWINDOWS_OBSERVED = "event_publish_subwindows_observed";
const M_SUBWINDOW_RESETS = "event_publish_subwindow_counter_resets";

// The settling gates. The two `_pending` rows carry the TOTAL unsettled depth at each gate's
// exit — pending plus processing plus failed-awaiting-dead-letter plus replaying — rather than
// the `pending` literal they were named for. The names are kept so an artefact produced by an
// earlier revision still decodes to the same slot; the summary states the population.
const M_PRE_DRAIN_SETTLED = "event_publish_pre_drain_settled";
const M_PRE_DRAIN_SECONDS = "event_publish_pre_drain_seconds";
const M_PRE_DRAIN_PENDING = "event_publish_pre_drain_pending";
const M_POST_DRAIN_SETTLED = "event_publish_post_drain_settled";
const M_POST_DRAIN_SECONDS = "event_publish_post_drain_seconds";
const M_POST_DRAIN_PENDING = "event_publish_post_drain_pending";
const M_DRAIN_SOURCE_CODE = "event_publish_drain_source_code";
// 1 when BOTH gates read the whole unsettled population, 0 when either could only read part of
// it. It is the row that separates "the tail had settled" from "the tail could not be seen",
// which the depth figures alone cannot express: an incomplete reading at the floor looks exactly
// like a settled one.
const M_SETTLEMENT_POPULATION_COMPLETE =
  "event_publish_settlement_population_complete";

// Attribution. Whether the counters the verdicts are computed from belong to this run alone.
const M_INSTANCE_ISOLATED = "event_publish_instance_isolated";
const M_ISOLATION_ACKNOWLEDGED = "event_publish_isolation_acknowledged";
const M_ISOLATION_FOREIGN_EVENTS = "event_publish_isolation_foreign_events";
const M_ISOLATION_PROBE_SECONDS = "event_publish_isolation_probe_seconds";

// --- The fixture harness (FIXTURES=1) --------------------------------------------------
const M_FIXTURE_CHECKS = "event_publish_fixture_checks";
const M_FIXTURE_FAILURES = "event_publish_fixture_failures";

const baselineComplete = new Gauge(M_BASELINE_COMPLETE);
const backlogSettled = new Gauge(M_BACKLOG_SETTLED);
const baselineSettleSeconds = new Gauge(M_BASELINE_SETTLE_SECONDS);
const finalSettleSeconds = new Gauge(M_FINAL_SETTLE_SECONDS);
const backlogPendingAtSettle = new Gauge(M_BACKLOG_PENDING_AT_SETTLE);

const eventsPerSecond = new Gauge(M_EVENTS_PER_SEC);

// A TREND rather than a Gauge, because it holds one observation per sampling interval and the
// verdict is a QUANTILE over them. Every other custom metric here is a Gauge precisely because it
// is a single end-of-run value; this one is the exception and the difference is the point.
const intervalEventsPerSecond = new Trend(M_INTERVAL_EVENTS_PER_SEC);
const p99Seconds = new Gauge(M_P99_SECONDS);
const deadLetterRatio = new Gauge(M_DEAD_LETTER_RATIO);
const verdictsAvailable = new Gauge(M_VERDICTS_AVAILABLE);

const windowEventsPerSecond = new Trend(M_WINDOW_EVENTS_PER_SEC);
const rateWindowsCounted = new Gauge(M_RATE_WINDOWS_COUNTED);
const rateWindowsSkipped = new Gauge(M_RATE_WINDOWS_SKIPPED);
const rateWindowWidth = new Gauge(M_RATE_WINDOW_WIDTH);

const windowSeconds = new Gauge(M_WINDOW_SECONDS);
const dispatchedStart = new Gauge(M_DISPATCHED_START);
const dispatchedEnd = new Gauge(M_DISPATCHED_END);
const dispatchedDelta = new Gauge(M_DISPATCHED_DELTA);
const publishedStart = new Gauge(M_PUBLISHED_START);
const publishedEnd = new Gauge(M_PUBLISHED_END);
const publishedDelta = new Gauge(M_PUBLISHED_DELTA);
const deadLetteredStart = new Gauge(M_DEAD_LETTERED_START);
const deadLetteredEnd = new Gauge(M_DEAD_LETTERED_END);
const deadLetteredDelta = new Gauge(M_DEAD_LETTERED_DELTA);
const latencyCountStart = new Gauge(M_LATENCY_COUNT_START);
const latencyCountEnd = new Gauge(M_LATENCY_COUNT_END);
const latencyCountDelta = new Gauge(M_LATENCY_COUNT_DELTA);
const latencySumStart = new Gauge(M_LATENCY_SUM_START);
const latencySumEnd = new Gauge(M_LATENCY_SUM_END);
const latencySumDelta = new Gauge(M_LATENCY_SUM_DELTA);
const p99BucketLower = new Gauge(M_P99_BUCKET_LOWER);
const p99BucketUpper = new Gauge(M_P99_BUCKET_UPPER);
const p99InterpolationCode = new Gauge(M_P99_INTERPOLATION_CODE);
const p99SourceCode = new Gauge(M_P99_SOURCE_CODE);
const p99SeriesSpellingCode = new Gauge(M_P99_SERIES_SPELLING_CODE);
const brokerWriteP99 = new Gauge(M_BROKER_WRITE_P99);
const p99Difference = new Gauge(M_P99_DIFFERENCE);
const attemptsDispatched = new Gauge(M_ATTEMPTS_DISPATCHED);
const attemptsRetrying = new Gauge(M_ATTEMPTS_RETRYING);
const attemptsTerminal = new Gauge(M_ATTEMPTS_TERMINAL);
const attemptsDeadLettered = new Gauge(M_ATTEMPTS_DEAD_LETTERED);
// The fixture harness's own counters, registered ONLY in fixture mode so an acceptance summary
// does not carry two permanently zero-valued rows that mean nothing to it.
const fixtureChecks = FIXTURES ? new Counter(M_FIXTURE_CHECKS) : null;
const fixtureFailures = FIXTURES ? new Counter(M_FIXTURE_FAILURES) : null;
const outboxPendingStart = new Gauge(M_OUTBOX_PENDING_START);
const outboxPendingEnd = new Gauge(M_OUTBOX_PENDING_END);
const drainWaitSeconds = new Gauge(M_DRAIN_WAIT_SECONDS);
const drainComplete = new Gauge(M_DRAIN_COMPLETE);
const drainPolls = new Gauge(M_DRAIN_POLLS);
const offeredWindowSeconds = new Gauge(M_OFFERED_WINDOW_SECONDS);
const settlementGapFactor = new Gauge(M_SETTLEMENT_GAP_FACTOR);
const dispatchedSeriesCode = new Gauge(M_DISPATCHED_SERIES_CODE);
const publishedSeriesCode = new Gauge(M_PUBLISHED_SERIES_CODE);
const deadLetteredSeriesCode = new Gauge(M_DEAD_LETTERED_SERIES_CODE);
const metricsStatusStart = new Gauge(M_METRICS_STATUS_START);
const metricsStatusEnd = new Gauge(M_METRICS_STATUS_END);
const counterReset = new Gauge(M_COUNTER_RESET);
const processStartBefore = new Gauge(M_PROCESS_START_BEFORE);
const processStartAfter = new Gauge(M_PROCESS_START_AFTER);
const exporterRestarted = new Gauge(M_EXPORTER_RESTARTED);
const degradedReasonCode = new Gauge(M_DEGRADED_REASON_CODE);
const statsEndpointStatus = new Gauge(M_STATS_STATUS);
const statsDispatched = new Gauge(M_STATS_DISPATCHED);
const statsDeadLettered = new Gauge(M_STATS_DEAD_LETTERED);
const statsPending = new Gauge(M_STATS_PENDING);
const statsProcessing = new Gauge(M_STATS_PROCESSING);
const statsFailed = new Gauge(M_STATS_FAILED);
const statsReplaying = new Gauge(M_STATS_REPLAYING);
const statsUnsettled = new Gauge(M_STATS_UNSETTLED);
const statsDeadLetteredStart = new Gauge(M_STATS_DEAD_LETTERED_START);
const statsDeadLetteredDelta = new Gauge(M_STATS_DEAD_LETTERED_DELTA);
const partitionKeys = new Gauge(M_PARTITION_KEYS);
const loadSecondsMetric = new Gauge(M_LOAD_SECONDS);
const offeredRate = new Gauge(M_OFFERED_RATE);
const backlogDrained = new Gauge(M_BACKLOG_DRAINED);
const drainSeconds = new Gauge(M_DRAIN_SECONDS);
const deadLetterSource = new Gauge(M_DEAD_LETTER_SOURCE);
const samplerIdentityChangeCount = new Gauge(M_SAMPLER_IDENTITY_CHANGES);

// The sustained-throughput instruments, and the one place in this file where a
// non-Gauge type is correct: each carries MANY observations, one per subwindow.
//
// A Trend for the distribution of per-subwindow rates, so the summary shows the min — the
// worst interval the pipeline had — beside the average that used to be the only figure.
const subwindowThroughput = new Trend(M_SUBWINDOW_EVENTS_PER_SEC);
// A Rate for the tolerance policy: the fraction of qualifying subwindows that individually
// reached the target.
const subwindowMetTarget = new Rate(M_SUBWINDOW_MET_TARGET);
// Counters for the evidence behind that fraction. `count>=N` on a k6 Counter is evaluated
// even when the metric received no samples at all, which is what makes the qualifying-count
// threshold the fail-closed guard for the whole family: a `rate>=x` threshold on an EMPTY
// Rate passes vacuously, so the Rate alone could never be trusted.
const subwindowsQualifying = new Counter(M_SUBWINDOWS_QUALIFYING);
const subwindowsObserved = new Counter(M_SUBWINDOWS_OBSERVED);
const subwindowResets = new Counter(M_SUBWINDOW_RESETS);

const preDrainSettled = new Gauge(M_PRE_DRAIN_SETTLED);
const preDrainSeconds = new Gauge(M_PRE_DRAIN_SECONDS);
const preDrainPending = new Gauge(M_PRE_DRAIN_PENDING);
const postDrainSettled = new Gauge(M_POST_DRAIN_SETTLED);
const postDrainSeconds = new Gauge(M_POST_DRAIN_SECONDS);
const postDrainPending = new Gauge(M_POST_DRAIN_PENDING);
const drainSourceCode = new Gauge(M_DRAIN_SOURCE_CODE);
const settlementPopulationComplete = new Gauge(
  M_SETTLEMENT_POPULATION_COMPLETE,
);

const instanceIsolated = new Gauge(M_INSTANCE_ISOLATED);
const isolationAcknowledged = new Gauge(M_ISOLATION_ACKNOWLEDGED);
const isolationForeignEvents = new Gauge(M_ISOLATION_FOREIGN_EVENTS);
const isolationProbeSeconds = new Gauge(M_ISOLATION_PROBE_SECONDS);

/**
 * dth builds the tag-scoped HTTP thresholds for a scenario, in the same shape the other
 * scripts in this directory use.
 *
 * Both are guards on the LOAD GENERATOR, not verdicts. Scoping them by scenario tag matters
 * for more than tidiness: the /metrics scrapes issued from setup() and teardown() are
 * outside every scenario, so a scenario-scoped threshold cannot be moved by an intentional
 * probe that returns 403 or refuses to connect.
 *
 * @param {string} scn the scenario tag value to scope the thresholds to.
 * @returns {object} a k6 thresholds map.
 */
function dth(scn) {
  var o = {};

  // A failing API produced no events at all, which would make every verdict below
  // meaningless. This is the guard that says the load actually landed.
  o["http_req_failed{scenario:" + scn + "}"] = ["rate<0.001"];

  // API ACCEPTANCE latency — how long the server took to accept a transaction. This is
  // emphatically NOT the V-1 publish latency, which is measured from the event's capture in
  // the transactional outbox to broker acknowledgement and lives on the custom metric
  // `event_publish_p99_seconds` below. Reading this row as the latency verdict is the single
  // most likely way V-1 gets falsely reported as passing.
  //
  // The numbers are not copied from script.js, which is calibrated for 300 arrivals/sec.
  // These are sized for 500/sec and stay overridable, because the right ceiling for
  // acceptance latency depends on the deployment rather than on the acceptance criteria.
  o[`http_req_duration{scenario:${scn},expected_response:true}`] = [
    "p(95)<" + MAX_API_P95_MS,
    "p(99)<" + MAX_API_P99_MS,
  ];

  // The per-response assertions, promoted from advisory to enforced.
  //
  // postTxn checks the status AND the returned identifier and status field, but a `check` that
  // fails only colours the console: k6 exits 0 unless a THRESHOLD fails. Without this row the
  // run went green while every transaction came back 200-with-no-id or 202-queued-forever —
  // responses that produce no applied ledger mutation and therefore no event, which is exactly
  // the state that makes the throughput verdict meaningless.
  //
  // Scoped to the scenario tag for the same reason the two rows above are: the /metrics and
  // /events/stats probes run outside every scenario and must not be able to move a verdict.
  o["checks{scenario:" + scn + "}"] = ["rate>" + MIN_CHECK_PASS_RATE];

  // Arrivals the executor could not start, because every VU was busy.
  //
  // A dropped iteration is offered load that never happened, so it silently lowers the events
  // the window can contain while the verdict still divides by the full load interval. It is
  // also the signal that maxVUs is too small for the offered rate — a configuration fault in
  // the harness rather than a finding about the system under test, and one that would otherwise
  // be reported as the system failing to sustain throughput.
  //
  // Counted rather than rated because k6 publishes dropped_iterations as a Counter and it is
  // untagged by scenario; with one scenario in this file that distinction does not matter.
  //
  // `count<=`, INCLUSIVE, and the distinction is not pedantic (PERF-m04). maxDroppedIterations
  // returns a MAXIMUM TOLERATED count and floors at 1, precisely so that the single iteration
  // k6's arrival-rate executor can drop while ramping VUs does not fail a thirty-minute run.
  // Expressed exclusively as `count<1`, that floor forbade the one drop it existed to allow: any
  // short run — every smoke run, and any run whose intended total puts the proportion below one
  // — demanded ZERO drops and failed on the ramp artefact the comment above calls noise. The
  // same off-by-one applies at every scale: a tolerance of N accepted only N-1.
  o["dropped_iterations"] = ["count<=" + maxDroppedIterations()];

  return o;
}

/**
 * verdictThresholds builds the thresholds that decide V-1 and V-3.
 *
 * Most are expressed against a Gauge, whose only supported aggregation is `value`: the p99 is
 * already computed from the server's histogram before it reaches the gauge, so k6 is not being
 * asked to compute a percentile of the LATENCY. The one exception is the sustained-throughput
 * row, which is a genuine Trend of per-interval observations and therefore carries a quantile
 * form — a `p(50)>=x` on a Gauge would be rejected, and a `value>=x` on that Trend would read
 * its last sample instead of its distribution.
 *
 * @returns {object} a k6 thresholds map.
 */
function verdictThresholds() {
  var o = {};

  // V-1, throughput. Computed as the delta of blnk_events_published_total, the per-event
  // counter incremented at the transition that records the Kafka leg as durable — NOT from
  // k6's http_reqs, which counts the requests OFFERED rather than the events produced, and NOT
  // from the broker acknowledgement counter, which counts writes and would over-report by the
  // redelivery rate.
  //
  // The numerator is read AFTER the outbox has drained, so it includes the tail the relay was
  // still working through when the load stopped, while the denominator stays the interval the
  // load was actually offered over. Both halves matter: crediting the tail to a window
  // lengthened by the drain wait would understate the rate, and dropping the tail entirely
  // would understate the event count.
  //
  // When the sampler is running this is a DIAGNOSTIC average and the verdict below is what
  // decides sustained throughput. It carries the verdict itself only when the sampler is off,
  // because then it is the only throughput figure the run produced.
  if (!RATE_SAMPLER_ENABLED) {
    o[M_EVENTS_PER_SEC] = ["value>=" + TARGET_EVENTS_PER_SEC];
  }

  // V-1, SUSTAINED throughput. `min` and not `avg`: the criterion is that the rate held, and
  // an average over the windows would readmit exactly the burst-then-stall run that the
  // whole-run average already fails to distinguish.
  //
  // EVERY SAMPLER-FED THRESHOLD IS REGISTERED HERE AND ONLY HERE (PERF-M10).
  //
  // Three of the five below used to be registered unconditionally, and that contradicted the
  // branch above outright. With the sampler off — which is what RATE_WINDOW_SECONDS=0 asks for,
  // and what any run too short to hold two windows gets — the whole-run mean was made the
  // verdict-carrying figure, and then these fired against metrics no scenario ever fed:
  //
  //   M_SUBWINDOWS_QUALIFYING  count>=3   on an empty Counter — EVALUATED AND FAILED
  //   M_SUBWINDOW_MET_TARGET   rate>=0.95 on an empty Rate    — passed VACUOUSLY
  //   M_INTERVAL_EVENTS_PER_SEC p(50)>=N  on an empty Trend    — failed
  //
  // So a deliberately unsampled run could not pass, whatever the pipeline did, and it failed
  // naming a subwindow count as the cause. Observed directly: a smoke run reported
  // `✗ count>=3` beside `✓ rate>=0.95` on the same absent measurement.
  //
  // The fail-closed intent behind those thresholds is real and is PRESERVED — it just belongs
  // inside the branch where the sampler is running. There, an empty Counter failing is exactly
  // the wanted behaviour: it is what stops a sampler that never read /metrics from certifying
  // sustained throughput by producing no evidence at all.
  if (RATE_SAMPLER_ENABLED) {
    o[M_WINDOW_EVENTS_PER_SEC] = ["min>=" + TARGET_EVENTS_PER_SEC];
    // Fail closed on the sampler itself. Without this, a sampler that could not read
    // /metrics for the whole run would contribute no samples, `min` would have nothing to
    // fail, and the absence of the measurement would read as a pass.
    o[M_RATE_WINDOWS_COUNTED] = ["value>=1"];

    // And the SUSTAINED rate, which is what the criterion actually says. The median of the
    // per-interval samples at or above the target means at least half the sampled intervals met
    // it; the run average alone passes for a pipeline that hit 1000/s for eight minutes and
    // 350/s for the rest, having sustained the target for a quarter of the run. Both thresholds
    // must hold, so neither statistic can certify the criterion on its own.
    o[M_INTERVAL_EVENTS_PER_SEC] = ["p(50)>=" + TARGET_EVENTS_PER_SEC];

    // V-1, SUSTAINED. The whole-window figure above is an average, and an average cannot tell
    // 500/sec throughout from nothing for half the run and 1000/sec for the other half. This
    // pair of thresholds is what "sustained" means here, and both are needed:
    //
    //   - the RATIO is the tolerance: at most one qualifying subwindow in twenty may miss the
    //     target at the default 0.95;
    //   - the COUNT is the evidence floor, and it is also the FAIL-CLOSED GUARD for the pair. A
    //     `rate>=x` threshold on a Rate that received NO samples passes vacuously in k6, so the
    //     ratio alone would certify sustained throughput for a run whose sampler never ran. A
    //     `count>=n` threshold on a Counter is evaluated at zero samples and fails, so the count
    //     is what makes an unsampled run fail rather than pass.
    o[M_SUBWINDOW_MET_TARGET] = ["rate>=" + MIN_SUSTAINED_SUBWINDOW_RATIO];
    o[M_SUBWINDOWS_QUALIFYING] = ["count>=" + MIN_QUALIFYING_SUBWINDOWS];
  }

  // V-1, latency. The p99 of blnk_events_capture_to_dispatch_duration_seconds_bucket
  // filtered to attempt="1", interpolated exactly as histogram_quantile would. That series
  // and no other: when it is absent the gauge is left unrecorded and the availability guard
  // below fails the run, rather than the shorter publish_duration figure being certified in
  // its place.
  o[M_P99_SECONDS] = ["value<" + MAX_P99_PUBLISH_SECONDS];

  // V-3, dead-letter rate. dead_lettered / (dead_lettered + published): the two counters
  // partition a captured event's terminal outcomes — an event is either delivered or
  // dead-lettered, never both — so their SUM is the population. Dividing by the delivered
  // count alone reports dead-letters as a fraction of successes, which overstates the rate and
  // diverges without bound as failures rise: every event failing gives a denominator of zero.
  //
  // The partition is what makes this a rate over EVENTS rather than over broker writes, and it
  // holds only because BOTH counters are incremented at their durable row transition —
  // dispatched or webhook-pending on one side, the acknowledged `.dlt` write recorded on the row
  // on the other. A duplicated broker write counted as a delivery would enlarge the denominator
  // without enlarging the numerator and understate the rate, which is the direction that lets a
  // failing pipeline pass.
  o[M_DEAD_LETTER_RATIO] = ["value<" + MAX_DEAD_LETTER_RATIO];

  // V-1, SUSTAINED. The whole-window figure above is an average, and an average cannot tell
  // 500/sec throughout from nothing for half the run and 1000/sec for the other half. This
  // pair of thresholds is what "sustained" means here, and both are needed:
  //
  //   - the RATIO is the tolerance: at most one qualifying subwindow in twenty may miss the
  //     target at the default 0.95;
  //   - the COUNT is the evidence floor, and it is also the FAIL-CLOSED GUARD for the pair. A
  //     `rate>=x` threshold on a Rate that received NO samples passes vacuously in k6, so the
  //     ratio alone would certify sustained throughput for a run whose sampler never ran. A
  //     `count>=n` threshold on a Counter is evaluated at zero samples and fails, so the count
  //     is what makes an unsampled run fail rather than pass.
  o[M_SUBWINDOW_MET_TARGET] = ["rate>=" + MIN_SUSTAINED_SUBWINDOW_RATIO];
  o[M_SUBWINDOWS_QUALIFYING] = ["count>=" + MIN_QUALIFYING_SUBWINDOWS];

  // Fail closed. This is 1 only when both scrapes succeeded, NO COUNTER RESET occurred, the
  // INSTANCE WAS ATTRIBUTABLE TO THIS RUN, BOTH SETTLING GATES HELD OVER THE WHOLE UNSETTLED
  // POPULATION, the series resolved, the window was positive, at least one terminal event was
  // observed, at least one first-attempt latency sample existed and V-3's numerator came from a
  // real window delta. Without it, a run against a stack whose observability was disabled would
  // report three zeroes, two of which pass their `<` thresholds, and certify criteria it never
  // measured.
  o[M_VERDICTS_AVAILABLE] = ["value>=" + REQUIRE_METRICS];

  return o;
}

/**
 * lifecycleTimeout sizes a setup or teardown timeout around the work that stage actually does.
 *
 * Both stages contain a bounded settling gate, so a fixed timeout would silently truncate a gate
 * that was configured to wait longer than it — k6 aborts the stage, and an aborted setup loses
 * the baseline entirely. Deriving the timeout from the budget is what keeps the two from
 * disagreeing.
 *
 * IT WAS DEFINED AND NEVER CALLED, which is how both budgets came to be sized on a gate other
 * than the one their stage runs: setup's allowed for provisioning and the baseline scrape but not
 * for the pre-load gate's 120 seconds, so a provisioning-bound run was killed inside the gate,
 * and teardown's was computed from a drain constant no surviving gate reads.
 *
 * @param {number} baseSeconds the stage's own work, excluding the gate.
 * @param {number} budgetSeconds the gate's budget.
 * @returns {number} the timeout in seconds.
 */
function lifecycleTimeout(baseSeconds, budgetSeconds) {
  // One extra poll interval of headroom: the gate checks its deadline before sleeping, so it
  // can return up to one poll after the budget.
  return Math.ceil(
    baseSeconds + Math.max(0, budgetSeconds) + DRAIN_POLL_SECONDS,
  );
}

// Simple merge without spread/rest (for Goja compatibility)
function merge() {
  var out = {};
  for (var i = 0; i < arguments.length; i++) {
    var src = arguments[i];
    if (!src) continue;
    for (var k in src) {
      if (Object.prototype.hasOwnProperty.call(src, k)) out[k] = src[k];
    }
  }
  return out;
}

export const options = buildOptions();

/**
 * requiredSpread is the smallest number of distinct partition keys acceptance mode accepts.
 *
 * Defaults to the full requested spread, because a partial spread is a silent reduction in the
 * concurrency the run is able to demonstrate. It is overridable so that an operator who
 * knowingly runs against a smaller fixture set can say so, rather than being pushed towards
 * SMOKE=1 and losing every other acceptance guard with it.
 *
 * @returns {number} the minimum acceptable number of provisioned pairs.
 */
function requiredSpread() {
  return numberFrom(__ENV.MIN_LEDGER_SPREAD, LEDGER_SPREAD);
}

/**
 * maxDroppedIterations converts the dropped-iteration tolerance into a k6 Counter threshold.
 *
 * At least 1 is always allowed: k6's arrival-rate executor can drop a single iteration while
 * ramping VUs at start-up, and failing a thirty-minute acceptance run on that would be noise
 * rather than a finding. A run so short that the proportion rounds below one is governed by
 * that floor.
 *
 * @returns {number} the maximum tolerated dropped-iteration count.
 */
function maxDroppedIterations() {
  var intended = RATE * LOAD_SECONDS;
  var allowed = Math.floor(intended * MAX_DROPPED_ITERATION_RATIO);

  return allowed > 1 ? allowed : 1;
}

/**
 * setupTimeoutSeconds sizes setup()'s budget to the work it actually performs.
 *
 * Provisioning is SEQUENTIAL — one ledger request each, then two balance requests per ledger —
 * so the cost is (3 x LEDGER_SPREAD) requests. Each is budgeted at the API p99 ceiling this
 * same file asserts, because that is the slowest a request may be while the run is still
 * considered healthy; sizing against an optimistic average is what produced a timeout that
 * looked generous and was not. The baseline scrape and parse are added on top.
 *
 * @returns {number} the setup timeout in whole seconds.
 */
function setupTimeoutSeconds() {
  var provisioningRequests = LEDGER_PAIRS_RAW ? 0 : 3 * Math.max(LEDGER_SPREAD, 0);
  var provisioningSeconds = (provisioningRequests * MAX_API_P99_MS) / 1000;
  // 60s covers the baseline scrape, the exposition parse and process start-up. The isolation
  // probe's own sleep is added because it is wall-clock time setup SPENDS: sized without it, a
  // long probe would be truncated by the stage timeout, and an aborted setup loses the baseline
  // entirely — the same defect lifecycleTimeout exists to prevent for the settling gate.
  var budget = lifecycleTimeout(
    Math.ceil(provisioningSeconds * 1.25) +
      60 +
      Math.max(0, ISOLATION_PROBE_SECONDS),
    PRE_DRAIN_BUDGET_SECONDS,
  );

  return numberFrom(__ENV.SETUP_TIMEOUT_SECONDS, budget);
}

/**
 * teardownTimeoutSeconds sizes teardown()'s budget.
 *
 * teardown waits for the outbox to drain, then scrapes, parses and computes every quantile, so
 * the drain deadline has to fit inside the stage's own timeout with room to spare — otherwise
 * k6 kills the stage mid-drain and the run reports nothing at all rather than reporting that
 * the backlog did not drain.
 *
 * @returns {number} the teardown timeout in whole seconds.
 */
function teardownTimeoutSeconds() {
  // 180 seconds for the closing scrape, the exposition parse, every quantile computation and the
  // optional /events/stats probe; the gate's own budget on top of that.
  return numberFrom(
    __ENV.TEARDOWN_TIMEOUT_SECONDS,
    lifecycleTimeout(180, POST_DRAIN_BUDGET_SECONDS),
  );
}

function buildOptions() {
  // THE FIXTURE HARNESS, dispatched before the load scenario so that a fixture run cannot
  // offer load, register a load threshold or reach a deployment even by accident. One VU, one
  // iteration, no HTTP: see the note on FIXTURES.
  //
  // The failure counter carries a `count==0` threshold so that a `k6 run` reader sees the
  // verdict in the familiar place, and verdictFixtures ALSO aborts the test when a check
  // fails. Both are deliberate: k6 skips a threshold on a metric that received no samples, so
  // the threshold alone would pass vacuously on a harness that never ran a single check, while
  // the abort is unconditional and sets a distinct exit code.
  if (FIXTURES) {
    var fixtureThresholds = {};
    fixtureThresholds[M_FIXTURE_FAILURES] = ["count==0"];

    return {
      scenarios: {
        event_publish_fixtures: {
          executor: "per-vu-iterations",
          vus: 1,
          iterations: 1,
          exec: "verdictFixtures",
        },
      },
      thresholds: fixtureThresholds,
      // Nothing here waits on a network, so the generous acceptance budgets would only delay
      // the report of a hung fixture.
      setupTimeout: "30s",
      teardownTimeout: "30s",
      discardResponseBodies: false,
    };
  }

  if (SCENARIO === "event_publish") {
    // THERE USED TO BE A SECOND `return` HERE, immediately, carrying only the scenarios map.
    // It made everything below it unreachable: the verdict thresholds, the computed setup and
    // teardown timeouts, discardResponseBodies and summaryTrendStats were all assembled and
    // then never returned. A run therefore offered the load, measured it, and was judged
    // against k6's defaults — no V-1, V-3 or sustained threshold registered at all, the
    // /metrics response bodies discarded so every verdict read as unavailable, and setup
    // killed at the default 60s in the middle of provisioning. It is the seam between two
    // generations of this function, one that returned early with scenarios alone and one that
    // built a `scenarios` local and returned it with everything else.
    var scenarios = {};

    // constant-arrival-rate rather than constant-vus, so the OFFERED pressure stays
    // fixed. A constant-VU executor would let the offered load sag exactly when the
    // system slowed down, which is the moment the measurement matters most. Note this
    // fixes the REQUEST rate; the resulting event rate is measured, not assumed.
    scenarios[SCENARIO] = {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: MAX_VUS,
      tags: { scenario: SCENARIO },
      exec: "publishEvents",
    };

    // The sustained-throughput sampler. It runs FOR THE SAME DURATION as the load, in
    // parallel with it, and reads the published counter on a fixed cadence so that each
    // interval can be judged on its own instead of being averaged into the whole window.
    //
    // constant-vus with EXACTLY ONE VU, and both properties matter. `vus: 1` is what makes
    // the readings a single sequential chain — the sampler's previous reading lives in
    // per-VU module state, so a second VU would difference the same counter against its
    // own earlier value and count every interval twice. constant-vus rather than
    // constant-arrival-rate because the cadence is enforced by the iteration's own sleep,
    // and an arrival-rate executor asked to keep a schedule it cannot meet would drop
    // iterations and leave gaps in the chain. A scrape that overruns its cadence therefore
    // costs nothing but a longer interval, because every rate is divided by the interval
    // that actually elapsed rather than by the nominal width.
    //
    // Its /metrics requests are tagged into their own scenario, which keeps them out of
    // the load's `http_req_failed` and `http_req_duration` thresholds: a scrape against a
    // secured endpoint legitimately answers 401, and that must not read as the API
    // failing.
    //
    // REGISTERED ONLY WHEN THE SAMPLER IS ENABLED, which is the same condition
    // verdictThresholds() gates its two sampler thresholds on and the same one the provenance
    // block reports as `sustained.enabled`. Registering the scenario unconditionally while
    // gating the thresholds is how a run too short to contain a counted window came to spawn a
    // sampler whose samples nothing judged. The exec name is `sampleThroughput`, which is the
    // function this file actually exports; a second copy of this block named a `sampleRate`
    // export that does not exist, and k6 refuses to start on an unknown exec.
    if (RATE_SAMPLER_ENABLED) {
      scenarios[SAMPLER_SCENARIO] = {
        executor: "constant-vus",
        vus: 1,
        duration: DURATION,
        tags: { scenario: SAMPLER_SCENARIO },
        exec: "sampleThroughput",
      };
    }

    return {
      scenarios: scenarios,
      thresholds: merge(dth(SCENARIO), verdictThresholds()),
      // The baseline and final scrapes each fetch and parse a full Prometheus exposition,
      // so both stages get more room than the defaults allow. teardown additionally does
      // the whole quantile computation, and now also waits for the outbox to drain.
      //
      // Both are COMPUTED rather than fixed, because a fixed 120s was not enough for the
      // work setup actually does: provisioning the default spread is 128 ledger requests
      // followed by 256 balance requests, all sequential, and at the API p99 this same file
      // asserts (1000ms) that is 384 seconds — three times the budget. The run then failed
      // in the worst possible way, by falling back to the single-key shorthand and carrying
      // on for thirty minutes measuring one aggregate.
      setupTimeout: setupTimeoutSeconds() + "s",
      teardownTimeout: teardownTimeoutSeconds() + "s",
      // Stated explicitly because the measurement depends on it: with response bodies
      // discarded, the /metrics payload would arrive empty and every verdict would read as
      // unavailable.
      discardResponseBodies: false,
      // THE SUSTAINED VERDICT IS STATED OVER p(50), AND p(50) IS NOT A DEFAULT.
      //
      // k6's default set is ["avg","min","med","max","p(90)","p(95)"], so `values["p(50)"]`,
      // `values["p(10)"]` and `values.count` are all absent from the summary unless asked for.
      // The THRESHOLD is computed independently of this list and was correct without it — which
      // is precisely why the omission was dangerous rather than merely untidy: the row rendered
      // its value as "n/a" beside a genuinely passing mark, which reads as a criterion met on
      // evidence nobody could see. Every k6 default is retained so the familiar rows are
      // unchanged; the three additions are the ones this file reports.
      summaryTrendStats: [
        "avg",
        "min",
        "med",
        "max",
        "p(10)",
        "p(50)",
        "p(90)",
        "p(95)",
        "count",
      ],
    };
  }

  throw new Error("Unknown SCENARIO=" + SCENARIO);
}

// ---------------------------------------------------------------------------------------
// Prometheus exposition parsing.
//
// The format is line oriented: `#`-prefixed comment lines, then samples shaped as
// `name{label="value",label="value"} value [timestamp]`. The parser is deliberately small,
// it does not take shortcuts on the four things that actually vary in real output: label
// ORDER (never assumed — labels go into a map), escaped characters inside label values,
// `+Inf` bucket boundaries, and scientific notation. The OpenTelemetry exporter also adds
// `otel_scope_name` and `otel_scope_version` labels to every series, which is exactly why
// order-independent parsing is not optional here.
// ---------------------------------------------------------------------------------------

// parseSampleValue parses a Prometheus sample value.
function parseSampleValue(token) {
  if (token === undefined || token === null || token === "") {
    return null;
  }

  var t = String(token);
  if (t === "+Inf" || t === "Inf" || t === "inf" || t === "+inf") {
    return Infinity;
  }
  if (t === "-Inf" || t === "-inf") {
    return -Infinity;
  }
  if (t === "NaN" || t === "nan") {
    return null;
  }

  var v = Number(t);
  if (typeof v !== "number" || isNaN(v)) {
    return null;
  }

  return v;
}

/**
 * findLabelSetEnd locates the `}` that closes a label set, ignoring braces that appear
 * inside quoted label values.
 *
 * A naive lastIndexOf("}") is wrong in two real cases: a label value containing a brace
 * (`unit="{event}"` is a shape the exporter genuinely produces elsewhere), and an
 * OpenMetrics exemplar appended after the value as `# {trace_id="abc"} 1.0`. Either one
 * would make the parser read a label set that runs past the sample value.
 *
 * @param {string} line the sample line.
 * @param {number} start the index just after the opening `{`.
 * @returns {number} the index of the closing `}`, or -1 when there is none.
 */
function findLabelSetEnd(line, start) {
  var inQuote = false;
  for (var i = start; i < line.length; i++) {
    var ch = line.charAt(i);
    if (inQuote) {
      if (ch === "\\") {
        i++;
        continue;
      }
      if (ch === '"') {
        inQuote = false;
      }
      continue;
    }
    if (ch === '"') {
      inQuote = true;
      continue;
    }
    if (ch === "}") {
      return i;
    }
  }

  return -1;
}

/**
 * parseLabels turns the inside of a label set into a map, so that callers never depend on
 * label order.
 *
 * @param {string} text the label set contents, without the surrounding braces.
 * @returns {object} label name to label value.
 */
function parseLabels(text) {
  // A prototype-less map, because a Prometheus label name is only constrained to
  // `[a-zA-Z_][a-zA-Z0-9_]*` and `__proto__` satisfies that. On an ordinary object literal,
  // assigning that key would reset the prototype instead of storing a label, and reading
  // `constructor` would return an inherited function rather than undefined.
  var labels = Object.create(null);
  var i = 0;
  var n = text.length;

  while (i < n) {
    while (
      i < n &&
      (text.charAt(i) === "," ||
        text.charAt(i) === " " ||
        text.charAt(i) === "\t")
    ) {
      i++;
    }
    if (i >= n) {
      break;
    }

    var eq = text.indexOf("=", i);
    if (eq < 0) {
      break;
    }

    var key = text.substring(i, eq).replace(/^[\s]+|[\s]+$/g, "");
    var j = eq + 1;
    while (j < n && (text.charAt(j) === " " || text.charAt(j) === "\t")) {
      j++;
    }
    if (text.charAt(j) !== '"') {
      // Malformed beyond recovery for this label set; keep whatever was already read.
      break;
    }
    j++;

    var value = "";
    var closed = false;
    while (j < n) {
      var ch = text.charAt(j);
      if (ch === "\\") {
        var next = text.charAt(j + 1);
        if (next === "n") {
          value += "\n";
        } else if (next === "\\") {
          value += "\\";
        } else if (next === '"') {
          value += '"';
        } else {
          value += next;
        }
        j += 2;
        continue;
      }
      if (ch === '"') {
        j++;
        closed = true;
        break;
      }
      value += ch;
      j++;
    }

    if (key !== "") {
      labels[key] = value;
    }
    if (!closed) {
      break;
    }
    i = j;
  }

  return labels;
}

// parseExposition indexes a Prometheus exposition payload by metric name.
function parseExposition(text) {
  // Prototype-less for the same reason as the label map: a metric may legally be named
  // `constructor`, and `index["constructor"]` on an object literal returns an inherited
  // function, so the `push` below would throw and abandon the whole scrape.
  var index = Object.create(null);
  var lines = String(text).split("\n");

  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].replace(/^[ \t]+|[ \t\r]+$/g, "");
    if (line.length === 0 || line.charAt(0) === "#") {
      continue;
    }

    var name;
    var labels;
    var rest;
    var brace = line.indexOf("{");
    // Whitespace, not just a space: the separator is a space in practice, but a tab is
    // whitespace too and costs nothing to accept.
    var space = line.search(/[ \t]/);

    if (brace >= 0 && (space < 0 || brace < space)) {
      var close = findLabelSetEnd(line, brace + 1);
      if (close < 0) {
        continue;
      }
      name = line.substring(0, brace);
      labels = parseLabels(line.substring(brace + 1, close));
      rest = line.substring(close + 1);
    } else {
      if (space < 0) {
        continue;
      }
      name = line.substring(0, space);
      labels = {};
      rest = line.substring(space);
    }

    // Only the first whitespace-separated token is the value; a trailing timestamp or
    // exemplar is deliberately ignored.
    var tokens = rest.replace(/^[ \t]+/, "").split(/[ \t]+/);
    var value = parseSampleValue(tokens[0]);
    if (value === null) {
      continue;
    }

    if (!Object.prototype.hasOwnProperty.call(index, name)) {
      index[name] = [];
    }
    index[name].push({ labels: labels, value: value });
  }

  return index;
}

/**
 * matchesAll reports whether a sample's labels contain every required name/value pair.
 * Labels the filter does not mention are ignored, which is what allows summing across
 * `topic` and tolerating the exporter's own `otel_scope_*` labels.
 *
 * @param {object} labels the sample's labels.
 * @param {object} filter the required name/value pairs.
 * @returns {boolean} true when every pair matches.
 */
function matchesAll(labels, filter) {
  for (var k in filter) {
    if (!Object.prototype.hasOwnProperty.call(filter, k)) {
      continue;
    }
    if (labels[k] !== filter[k]) {
      return false;
    }
  }

  return true;
}

/**
 * sumSeries adds up every sample of a metric whose labels satisfy the filter. This is the
 * in-script equivalent of a PromQL `sum` over a series' label dimensions.
 *
 * @param {object} index the parsed exposition.
 * @param {string} name the exact metric name.
 * @param {object} filter required label name/value pairs; pass `{}` to sum everything.
 * @returns {number|null} the sum, or null when the metric is absent.
 */
function sumSeries(index, name, filter) {
  var samples = index[name];
  if (!samples) {
    return null;
  }

  var total = 0;
  var matched = 0;
  for (var i = 0; i < samples.length; i++) {
    if (!matchesAll(samples[i].labels, filter)) {
      continue;
    }
    if (!isFinite(samples[i].value)) {
      continue;
    }
    total += samples[i].value;
    matched++;
  }

  if (matched === 0) {
    return null;
  }

  return total;
}

/**
 * bucketKey renders a bucket boundary as the stable string used to key a bucket map, so the
 * map survives the JSON round trip between setup() and teardown() unchanged.
 *
 * @param {number} le the boundary.
 * @returns {string} the key.
 */
function bucketKey(le) {
  if (le === Infinity) {
    return "+Inf";
  }

  return String(le);
}

/**
 * collectHistogram aggregates one histogram into `sum by (le)` form for a fixed label
 * filter, plus its `_sum` and `_count`.
 *
 * Prometheus bucket counts are cumulative, and summing cumulative counts across the `topic`
 * dimension keeps them cumulative, so the result is directly usable by bucketQuantile.
 *
 * @param {object} index the parsed exposition.
 * @param {string} base the histogram's base name, without `_bucket`/`_sum`/`_count`.
 * @param {object} filter required label name/value pairs.
 * @returns {object|null} `{buckets, sum, count}`, or null when the histogram is absent.
 */
function collectHistogram(index, base, filter) {
  var bucketSamples = index[base + "_bucket"];
  if (!bucketSamples) {
    return null;
  }

  var buckets = {};
  var matched = 0;
  for (var i = 0; i < bucketSamples.length; i++) {
    var sample = bucketSamples[i];
    if (!matchesAll(sample.labels, filter)) {
      continue;
    }
    var le = parseSampleValue(sample.labels.le);
    if (le === null) {
      continue;
    }
    var key = bucketKey(le);
    buckets[key] = (buckets[key] || 0) + sample.value;
    matched++;
  }

  if (matched === 0) {
    return null;
  }

  return {
    buckets: buckets,
    sum: sumSeries(index, base + "_sum", filter) || 0,
    count: sumSeries(index, base + "_count", filter) || 0,
  };
}

/**
 * resolveSeries picks the first candidate name that either snapshot actually observed, and
 * reports which candidate that was.
 *
 * Resolving across BOTH snapshots is what makes a series that only appeared partway through
 * the run — a counter is not exported until its first increment — resolve to a real name
 * with a baseline of zero, rather than reading as absent.
 *
 * @param {string[]} candidates the candidate names, most-documented first.
 * @param {object} startBucket the baseline snapshot section keyed by series name.
 * @param {object} endBucket the final snapshot section keyed by series name.
 * @returns {object} `{name, code}`; `code` is SERIES_CODE_ABSENT when nothing matched, and
 *   `name` is then the documented name so the provenance still says what was looked for.
 */
function resolveSeries(candidates, startBucket, endBucket) {
  for (var c = 0; c < candidates.length; c++) {
    var present =
      (startBucket &&
        Object.prototype.hasOwnProperty.call(startBucket, candidates[c])) ||
      (endBucket &&
        Object.prototype.hasOwnProperty.call(endBucket, candidates[c]));

    if (present) {
      return {
        name: candidates[c],
        code:
          c === 0 ? SERIES_CODE_DOCUMENTED_NAME : SERIES_CODE_TOLERATED_NAME,
      };
    }
  }

  return { name: candidates[0], code: SERIES_CODE_ABSENT };
}

// readNumber reads a finite number out of a snapshot section.
function readNumber(bucket, key) {
  if (!bucket || !Object.prototype.hasOwnProperty.call(bucket, key)) {
    return null;
  }

  var v = Number(bucket[key]);
  if (isNaN(v) || !isFinite(v)) {
    return null;
  }

  return v;
}

// The two histograms this scenario reads, each with the label filter its documented query
// uses. publish_duration needs `outcome="dispatched"` as well as `attempt="1"`: it carries
// three labels, and filtering on the attempt alone would match nothing at all while every
// dashboard still looked populated.
const HISTOGRAM_SPECS = [
  {
    candidates: SERIES_CAPTURE_TO_DISPATCH,
    filter: { attempt: FIRST_ATTEMPT },
  },
  {
    candidates: SERIES_PUBLISH_DURATION,
    filter: { attempt: FIRST_ATTEMPT, outcome: OUTCOME_DISPATCHED },
  },
];

/**
 * collectSnapshot reduces a parsed exposition to the compact set of numbers the verdicts
 * need.
 *
 * Aggregates are keyed by CANDIDATE NAME rather than by logical role. That is what lets
 * teardown resolve a series name against its own scrape and then read the matching baseline
 * for the very same candidate — otherwise a name that resolved differently between the two
 * scrapes would silently produce a delta computed from two different series.
 *
 * The result is deliberately small. It travels through setup()'s return value, which k6
 * serialises and copies to every VU and then embeds in the summary JSON, so the raw
 * exposition text is never carried along.
 *
 * @param {object} index the parsed exposition.
 * @returns {object} `{counters, attempts, histograms, gauges}`.
 */
function collectSnapshot(index) {
  var snapshot = { counters: {}, attempts: {}, histograms: {}, gauges: {} };
  var counterLists = [
    SERIES_DISPATCHED,
    SERIES_PUBLISHED,
    SERIES_DEAD_LETTERED,
  ];
  var i;
  var c;

  for (i = 0; i < counterLists.length; i++) {
    for (c = 0; c < counterLists[i].length; c++) {
      var total = sumSeries(index, counterLists[i][c], {});
      if (total !== null) {
        snapshot.counters[counterLists[i][c]] = total;
      }
    }
  }

  for (c = 0; c < SERIES_PUBLISH_ATTEMPTS.length; c++) {
    var attemptsName = SERIES_PUBLISH_ATTEMPTS[c];
    if (!index[attemptsName]) {
      continue;
    }
    // The four populations are DISJOINT and together they are every attempt: a failed
    // attempt is outcome="retrying" and is split by `terminal`, so summing all four still
    // gives the attempt total. Reading `retrying` without narrowing on terminal="false"
    // would double-count the terminal ones.
    snapshot.attempts[attemptsName] = {
      dispatched:
        sumSeries(index, attemptsName, { outcome: OUTCOME_DISPATCHED }) || 0,
      retrying:
        sumSeries(index, attemptsName, {
          outcome: OUTCOME_RETRYING,
          terminal: TERMINAL_FALSE,
        }) || 0,
      terminal:
        sumSeries(index, attemptsName, {
          outcome: OUTCOME_RETRYING,
          terminal: TERMINAL_TRUE,
        }) || 0,
      dead_lettered:
        sumSeries(index, attemptsName, { outcome: OUTCOME_DEAD_LETTERED }) || 0,
    };
  }

  for (i = 0; i < HISTOGRAM_SPECS.length; i++) {
    for (c = 0; c < HISTOGRAM_SPECS[i].candidates.length; c++) {
      var base = HISTOGRAM_SPECS[i].candidates[c];
      var collected = collectHistogram(index, base, HISTOGRAM_SPECS[i].filter);
      if (collected !== null) {
        snapshot.histograms[base] = collected;
      }
    }
  }

  for (c = 0; c < SERIES_OUTBOX_PENDING.length; c++) {
    var pending = sumSeries(index, SERIES_OUTBOX_PENDING[c], {});
    if (pending !== null) {
      snapshot.gauges[SERIES_OUTBOX_PENDING[c]] = pending;
    }
  }

  // The repair backlogs, summed across both legs (PERF-C01). Collected here so the gauge
  // fallback in readPendingDepth can add the two non-terminal states blnk_outbox_pending omits,
  // rather than declaring a run drained while half a million rows still owe a dead-letter write.
  for (c = 0; c < SERIES_REPAIR_BACKLOG.length; c++) {
    var owed = sumSeries(index, SERIES_REPAIR_BACKLOG[c], {});
    if (owed !== null) {
      snapshot.gauges[SERIES_REPAIR_BACKLOG[c]] = owed;
    }
  }

  // THE COLLECTION AGE, which was read by the drain gate and collected by nobody (PERF-M07).
  //
  // readPendingDepth looks this gauge up in snapshot.gauges to decide whether a pending reading
  // is FRESH, and the lookup could only ever miss: the series was declared, documented as the
  // freshness signal, and never put into the map. So `age` was unconditionally null, every
  // reading counted as a new observation, and DRAIN_STABLE_SAMPLES was satisfiable by three
  // polls of ONE collection — which is exactly the failure the constant exists to prevent, since
  // the server republishes this backlog on its own tick and polling faster returns the same
  // number repeatedly.
  for (c = 0; c < SERIES_COLLECTION_AGE.length; c++) {
    var collectedAt = sumSeries(index, SERIES_COLLECTION_AGE[c], {});
    if (collectedAt !== null) {
      snapshot.gauges[SERIES_COLLECTION_AGE[c]] = collectedAt;
    }
  }

  // The process-start marker (PERF-P03). Collected into the same gauge map, so the baseline
  // travels through setup()'s return value exactly as every other reading does and teardown
  // compares like with like.
  for (c = 0; c < SERIES_PROCESS_START.length; c++) {
    var startedAt = sumSeries(index, SERIES_PROCESS_START[c], {});
    if (startedAt !== null) {
      snapshot.gauges[SERIES_PROCESS_START[c]] = startedAt;
    }
  }

  return snapshot;
}

// shallowCopyNumbers copies a flat map of finite numbers.
function shallowCopyNumbers(source) {
  var out = {};
  if (!source) {
    return out;
  }

  for (var k in source) {
    if (!Object.prototype.hasOwnProperty.call(source, k)) {
      continue;
    }
    var v = Number(source[k]);
    if (!isNaN(v)) {
      out[k] = v;
    }
  }

  return out;
}

/**
 * deltaCounter differences two readings of a monotonic counter, detecting a reset.
 *
 * A reset means the exporter restarted mid-run. Two samples cannot recover what was lost, so
 * the post-reset reading is used as the delta — the closest honest figure available — and
 * the caller records the reset so the number is never read as if nothing had happened.
 *
 * A MISSING BASELINE is reported rather than absorbed. When the series is present at the end
 * and absent at the start there is no arithmetic available but "the end reading is the delta",
 * and that reading is the counter's whole LIFETIME — for a long-lived process an arbitrarily
 * large overstatement of the window. The function still returns that figure, because it is the
 * only number it has, and it now says so in `baselineMissing` so a caller cannot mistake a
 * lifetime total for a windowed one. Reporting it here rather than re-deriving it from the raw
 * readings at one call site is deliberate: the function is the only place that knows, and every
 * present and future caller gets the fact for free.
 *
 * @param {number|null} startValue the baseline reading, or null when the series was absent.
 * @param {number|null} endValue the final reading, or null when the series was absent.
 * @returns {object} `{value, reset, baselineMissing}`; `value` is null when the series was
 *   never present, and `baselineMissing` is true when `value` spans the counter's lifetime
 *   rather than the measured window.
 */
function deltaCounter(startValue, endValue) {
  if (endValue === null || endValue === undefined) {
    return { value: null, reset: false, baselineMissing: false };
  }
  if (startValue === null || startValue === undefined) {
    return { value: endValue, reset: false, baselineMissing: true };
  }
  if (endValue < startValue) {
    return { value: endValue, reset: true, baselineMissing: false };
  }

  return { value: endValue - startValue, reset: false, baselineMissing: false };
}

/**
 * deltaBuckets differences two cumulative bucket maps, producing the delta histogram the
 * quantile is computed over. This is the in-script equivalent of the `rate` inside PromQL's
 * `histogram_quantile(0.99, sum by (le) (rate(X[5m])))`: the window scaling cancels
 * out of a quantile, so a plain delta over the measured window is the same figure.
 *
 * @param {object} startMap the baseline cumulative counts, keyed by boundary.
 * @param {object} endMap the final cumulative counts, keyed by boundary.
 * @returns {object} `{buckets, reset}`.
 */
function deltaBuckets(startMap, endMap) {
  var end = shallowCopyNumbers(endMap);
  var start = shallowCopyNumbers(startMap);
  var infKey = bucketKey(Infinity);
  var startTotal = start[infKey] || 0;
  var endTotal = end[infKey] || 0;

  if (endTotal < startTotal) {
    return { buckets: end, reset: true };
  }

  var out = {};
  for (var k in end) {
    if (!Object.prototype.hasOwnProperty.call(end, k)) {
      continue;
    }
    var baseline = Object.prototype.hasOwnProperty.call(start, k)
      ? start[k]
      : 0;
    var d = end[k] - baseline;
    out[k] = d > 0 ? d : 0;
  }

  return { buckets: out, reset: false };
}

// sortedBuckets turns a bucket map into an ascending list of `{le, count}`.
function sortedBuckets(bucketMap) {
  var list = [];
  for (var k in bucketMap) {
    if (!Object.prototype.hasOwnProperty.call(bucketMap, k)) {
      continue;
    }
    var le = k === bucketKey(Infinity) ? Infinity : Number(k);
    if (isNaN(le)) {
      continue;
    }
    list.push({ le: le, count: Number(bucketMap[k]) });
  }

  list.sort(function (a, b) {
    if (a.le === b.le) {
      return 0;
    }
    return a.le < b.le ? -1 : 1;
  });

  return list;
}

/**
 * bucketQuantile computes a quantile from a delta histogram, reproducing Prometheus's
 * `histogram_quantile` arithmetic exactly so that the number reported here and the number a
 * dashboard would show for the documented PromQL agree.
 *
 * The three behaviours worth naming, because each one is a decision rather than an accident:
 *
 *   - Within a finite bucket the position is linearly interpolated between the bucket's
 *     bounds. `_sum / _count` is deliberately NOT used as a fallback anywhere: that is a
 *     mean, not a p99, and substituting it would report a comfortably passing figure for a
 *     distribution with a long tail.
 *   - When the rank falls in the `+Inf` bucket, the highest FINITE boundary is returned,
 *     which is what Prometheus does. It also keeps Infinity out of the result, and Infinity
 *     serialises to `null` in JSON, which would erase the very finding it represents.
 *   - A histogram without a `+Inf` bucket, or with no observations in the window, yields no
 *     value at all rather than a zero. Zero is indistinguishable from an instant publish and
 *     would pass the threshold.
 *
 * @param {number} quantile the quantile to compute, between 0 and 1.
 * @param {object} bucketMap cumulative delta counts keyed by boundary.
 * @returns {object} `{value, lower, upper, interpolation, total}`; `value` is null when the
 *   quantile is not computable.
 */
function bucketQuantile(quantile, bucketMap) {
  var result = {
    value: null,
    lower: null,
    upper: null,
    interpolation: INTERPOLATION_NONE,
    total: 0,
  };

  var list = sortedBuckets(bucketMap);
  if (list.length < 2) {
    return result;
  }

  var highest = list[list.length - 1];
  if (highest.le !== Infinity) {
    return result;
  }

  var total = highest.count;
  result.total = total;
  if (!(total > 0)) {
    return result;
  }

  var rank = quantile * total;
  var index = list.length - 1;
  for (var i = 0; i < list.length; i++) {
    if (list[i].count >= rank) {
      index = i;
      break;
    }
  }

  if (index === list.length - 1) {
    var highestFinite = list[list.length - 2];
    result.value = highestFinite.le;
    result.lower = highestFinite.le;
    result.upper = Infinity;
    result.interpolation = INTERPOLATION_HIGHEST_FINITE_BOUND;

    return result;
  }

  if (index === 0 && list[0].le <= 0) {
    result.value = list[0].le;
    result.lower = list[0].le;
    result.upper = list[0].le;
    result.interpolation = INTERPOLATION_LOWEST_BUCKET;

    return result;
  }

  var bucketStart = 0;
  var bucketEnd = list[index].le;
  var bucketCount = list[index].count;
  var positionInBucket = rank;

  if (index > 0) {
    bucketStart = list[index - 1].le;
    bucketCount -= list[index - 1].count;
    positionInBucket -= list[index - 1].count;
  }

  result.lower = bucketStart;
  result.upper = bucketEnd;
  result.interpolation = INTERPOLATION_LINEAR;
  result.value =
    bucketCount > 0
      ? bucketStart +
        (bucketEnd - bucketStart) * (positionInBucket / bucketCount)
      : bucketEnd;

  return result;
}

// ---------------------------------------------------------------------------------------
// Scraping.
// ---------------------------------------------------------------------------------------

/*
 * settleBacklog is RETIRED, superseded by waitForOutboxQuiescence.
 *
 * Both implementations of the settling gate were present in this file at once, and only one of
 * them could be the live one: teardown's drainHeld, preDrainSettled/postDrainSettled,
 * drainSourceCode and REASON_PIPELINE_NOT_SETTLED are all written against the two-phase
 * successor, while this one was still called from setup and returned a differently shaped result.
 *
 * The successor keeps every argument this function's documentation made — the biases at both
 * ends of the window, the bounded wait, the absent-series-is-settled rule and the absorption of
 * the exporter's own tick — and adds the two things this one could not do. It confirms quiet over
 * DRAIN_STABLE_SAMPLES readings whose FRESHNESS it verifies from
 * blnk_event_metrics_last_collection_age_seconds, so a single reading taken in the gap between
 * two relay claims cannot be mistaken for an empty outbox and three identical readings from one
 * collector tick cannot be mistaken for three observations. And it reads the depth from the live
 * GET /events/stats query when a master key is available, falling back to the gauge, recording
 * which source answered.
 */


/**
 * scrapeMetrics fetches and parses the server's Prometheus exposition.
 *
 * Every failure mode is a REPORTED CONDITION rather than an exception, because each one is a
 * legitimate state of a real deployment: observability can be switched off, `/metrics` can
 * be behind a bearer token, and a broker-less deployment is a supported steady state. A
 * throw here would abandon the run and lose the load-generation results along with the
 * measurement.
 *
 * The bearer token is sent as a header and is never echoed into the reason string, the
 * return value, the log line or any metric.
 *
 * @returns {object} `{available, status, reason, at, bodyBytes, snapshot}`.
 */
function scrapeMetrics() {
  var headers = {};
  if (METRICS_BEARER_TOKEN) {
    headers["Authorization"] = "Bearer " + METRICS_BEARER_TOKEN;
  }

  var res = null;
  var failure = "";
  try {
    res = http.get(METRICS_URL, {
      headers: headers,
      timeout: "60s",
      tags: { endpoint: "metrics" },
      // 401, 403 and 404 are expected outcomes of a probe against a secured or
      // observability-disabled deployment, so they are declared expected and do not inflate
      // the global http_req_failed rate the dashboard shows as a success rate.
      responseCallback: http.expectedStatuses(200, 401, 403, 404),
    });
  } catch (err) {
    failure = String(err && err.message ? err.message : err);
  }

  var at = Date.now();
  var status =
    res && typeof res.status === "number" ? res.status : STATUS_UNREACHABLE;
  var body = res && res.body ? String(res.body) : "";

  if (failure !== "") {
    return {
      available: false,
      status: status,
      reason: "the /metrics request could not be issued: " + failure,
      at: at,
      bodyBytes: 0,
      snapshot: null,
    };
  }

  if (status === STATUS_UNREACHABLE) {
    return {
      available: false,
      status: status,
      reason:
        "no response from " +
        redactURL(METRICS_URL) +
        " (the server is not listening, or the address is wrong)",
      at: at,
      bodyBytes: 0,
      snapshot: null,
    };
  }

  if (status === 401 || status === 403) {
    return {
      available: false,
      status: status,
      reason:
        "/metrics is secured and the bearer token was " +
        (METRICS_BEARER_TOKEN ? "rejected" : "not supplied") +
        " (set METRICS_BEARER_TOKEN to the server's metrics_bearer_token)",
      at: at,
      bodyBytes: 0,
      snapshot: null,
    };
  }

  if (status === 404) {
    return {
      available: false,
      status: status,
      reason:
        "/metrics is not served (observability is disabled; set enable_observability, or BLNK_ENABLE_OBSERVABILITY=true)",
      at: at,
      bodyBytes: 0,
      snapshot: null,
    };
  }

  if (status !== 200) {
    return {
      available: false,
      status: status,
      reason: "/metrics answered an unexpected status",
      at: at,
      bodyBytes: body.length,
      snapshot: null,
    };
  }

  if (body.length === 0) {
    return {
      available: false,
      status: status,
      reason:
        "/metrics answered 200 with an empty body (observability is disabled)",
      at: at,
      bodyBytes: 0,
      snapshot: null,
    };
  }

  return {
    available: true,
    status: status,
    reason: "",
    at: at,
    bodyBytes: body.length,
    snapshot: collectSnapshot(parseExposition(body)),
  };
}

/*
 * awaitOutboxDrain is RETIRED, superseded by waitForOutboxQuiescence.
 *
 * Its argument survives in the successor and in the comment on the closing gate in teardown: the
 * excluded tail is ENRICHED in the outcome V-3 is about, so dropping it flatters the criterion
 * rather than merely shortening the count. What does not survive is the shape — it returned the
 * scrape that proved the drain, and teardown now takes its closing scrape immediately after the
 * gate returns, which is the same guarantee reached without one function returning two unrelated
 * things.
 */


/**
 * pendingFromScrape reads the outbox backlog out of a scrape.
 *
 * WHAT IT CAN AND CANNOT SEE. blnk_outbox_pending is published by the server's event-metrics
 * collector as `pending + processing` — claimed-but-unacknowledged rows included, deliberately,
 * so a stalled relay holding every row under a lease cannot read as a drained backlog. It
 * contains NO other state, so it cannot see a `failed` row whose dead-letter write is still
 * owed, and it cannot see a replay in flight. Those rows are un-settled Kafka work that this
 * number reports as absent, which is why a gauge reading is never treated as a COMPLETE
 * settlement population; see readUnsettledDepth.
 *
 * @param {object} scrape a scrapeMetrics result.
 * @returns {number|null} the pending-plus-processing backlog, or null when it could not be read.
 */
function pendingFromScrape(scrape) {
  if (!scrape || !scrape.available || !scrape.snapshot) {
    return null;
  }

  var gauges = scrape.snapshot.gauges || {};
  var resolved = resolveSeries(SERIES_OUTBOX_PENDING, gauges, gauges);
  if (resolved.code === SERIES_CODE_ABSENT) {
    return null;
  }

  return readNumber(gauges, resolved.name);
}

/**
 * probeEventStats reads GET /events/stats: the per-status census of blnk.event_outbox.
 *
 * It serves TWO purposes, and the difference matters because one of them is load-bearing.
 *
 *   - CORROBORATION of the counter deltas (`dispatched`, `deadLettered`), which is optional
 *     in the sense that the endpoint is master-key gated and a run without a key simply does
 *     without it.
 *   - THE SETTLEMENT POPULATION (`unsettled`), which is not optional at all: it is the only
 *     source that can see the complete set of rows whose KAFKA outcome is still undecided,
 *     and the settling gates fail closed without it. See waitForOutboxQuiescence.
 *
 * # The settlement population, and why each state is in it or out of it
 *
 * `unsettled` is the sum of every state from which a Kafka publish is STILL OWED:
 *
 *   pending      captured and committed, not yet claimed.
 *   processing   claimed by a relay instance under a lease, not yet acknowledged.
 *   failed       the retry budget is spent and the DEAD-LETTER WRITE IS STILL OWED. This row
 *                has reached no terminal counter: it is absent from the published count and
 *                absent from the dead-lettered count, so a window that closes over it
 *                understates BOTH — and it understates V-3 specifically, because a failing
 *                row is the one most likely to become a dead letter.
 *   replaying    a dead-lettered row a replay has claimed. A publish is in flight and the
 *                dead-lettered census moves when it settles, which is exactly the figure
 *                V-3's stats-sourced numerator differences.
 *
 * `webhook_pending` is DELIBERATELY EXCLUDED and its exclusion is not an oversight. Such a
 * row's Kafka leg is acknowledged AND recorded — blnk_events_published_total has already
 * counted it — and what remains owed is the legacy HTTP leg, which no verdict here reads.
 * Waiting for it would make the acceptance window's length a function of the deprecated
 * transport's health, and at the sunset that state and its wait would both disappear.
 * `dispatched` and `dead_lettered` are terminal and are the outcomes being measured.
 *
 * # Completeness is reported, never assumed
 *
 * `unsettled` is null unless EVERY one of the four states was present in the response. A
 * response missing one of them cannot be summed into a population — the missing state's rows
 * would read as zero, which is precisely the "the tail was excluded" defect this exists to
 * close — so the gate treats a null as "the population could not be read" and refuses to
 * certify quiescence from it.
 *
 * IT ASKS FOR COUNTS ONLY (PERF-M02). See EVENTS_STATS_COUNTS_URL: the drain loop polls this once
 * a second, and the alternative postures additionally make a broker round trip and run an exact
 * COUNT of the dispatched history, which would make the measurement a load source of its own.
 *
 * `dispatched` is therefore expected to be null, and null here means NOT COUNTED rather than zero.
 * The endpoint states that itself in `dispatched_history_counted`, which is read and returned so a
 * caller can tell the two apart instead of inferring it from an absent key. Nothing this scenario
 * decides reads `dispatched`: it is published as a diagnostic gauge and nothing more, while every
 * count the quiescence gate reads is exact on this posture.
 *
 *
 * @returns {object} `{status, dispatched, dispatchedCounted, deadLettered, pending,
 *   processing, failed, replaying, unsettled}`; every count is null when it could not be
 *   read (and `dispatched` is null because it was NOT COUNTED under the counts-only
 *   posture, which `dispatchedCounted` states outright), and `unsettled` is null unless
 *   all four of its components were readable.
 */
function probeEventStats() {
  var unavailable = {
    status: STATUS_UNREACHABLE,
    dispatched: null,
    dispatchedCounted: false,
    deadLettered: null,
    pending: null,
    processing: null,
    failed: null,
    replaying: null,
    unsettled: null,
  };

  if (!MASTER_KEY || !EVENTS_STATS_URL) {
    return unavailable;
  }

  var res = null;
  try {
    res = http.get(EVENTS_STATS_COUNTS_URL, {
      headers: { "X-Blnk-Key": MASTER_KEY },
      timeout: "30s",
      tags: { endpoint: "events_stats" },
      responseCallback: http.expectedStatuses(200, 401, 403, 404, 503),
    });
  } catch (err) {
    return unavailable;
  }

  var status =
    res && typeof res.status === "number" ? res.status : STATUS_UNREACHABLE;
  // merge() over the unavailable shape rather than a hand-written literal, on every failing
  // return. The literals used to name four fields, so adding a fifth left the failure paths
  // handing back an object with `unsettled` UNDEFINED — and `undefined` is not `null`: a
  // completeness test written as `!== null` would have read a missing field as a real
  // population of NaN, which is the one reading a fail-closed gate must never make.
  if (status !== 200 || !res.body) {
    return merge(unavailable, { status: status });
  }

  var parsed = null;
  try {
    parsed = JSON.parse(String(res.body));
  } catch (err) {
    return merge(unavailable, { status: status });
  }

  // GET /events/stats returns the statistics object flat, which is the documented contract
  // and what this reads.
  var body = parsed;

  var pending = readOptionalNumber(body, "pending");
  var processing = readOptionalNumber(body, "processing");
  var failed = readOptionalNumber(body, "failed");
  var replaying = readOptionalNumber(body, "replaying");

  // ALL FOUR OR NOTHING. A response that carried three of them would sum to a population
  // short by the fourth, and a short population at or below the floor is indistinguishable
  // from a settled one — which is the whole defect. Null here makes the gate say "the
  // population could not be read" and withhold, rather than certify a partial sum.
  var unsettled =
    pending === null ||
    processing === null ||
    failed === null ||
    replaying === null
      ? null
      : pending + processing + failed + replaying;

  return {
    status: status,
    // Null under the counts-only posture, and dispatchedCounted says WHY: the key is absent
    // because nobody counted, not because nothing was dispatched. Reading the flag rather than
    // inferring from the absence is the whole reason the endpoint publishes it.
    dispatched: readOptionalNumber(body, "dispatched"),
    dispatchedCounted: body.dispatched_history_counted === true,
    deadLettered: readOptionalNumber(body, "dead_lettered"),
    pending: pending,
    processing: processing,
    failed: failed,
    replaying: replaying,
    unsettled: unsettled,
  };
}

// readOptionalNumber reads a numeric field from a decoded JSON object.
function readOptionalNumber(obj, key) {
  if (!obj || typeof obj !== "object") {
    return null;
  }

  var v = Number(obj[key]);
  if (obj[key] === undefined || obj[key] === null || isNaN(v) || !isFinite(v)) {
    return null;
  }

  return v;
}

// ---------------------------------------------------------------------------------------
// Aggregate provisioning.
// ---------------------------------------------------------------------------------------

// postJSON issues a JSON POST with the scenario's authentication, returning the decoded body.
function postJSON(url, body, endpointTag) {
  if (!url) {
    return null;
  }

  var headers = { "Content-Type": "application/json" };
  if (API_KEY) {
    headers["X-Blnk-Key"] = API_KEY;
  }

  var res = null;
  try {
    res = http.post(url, JSON.stringify(body), {
      headers: headers,
      timeout: "30s",
      tags: { endpoint: endpointTag },
    });
  } catch (err) {
    return null;
  }

  if (!res || (res.status !== 200 && res.status !== 201) || !res.body) {
    return null;
  }

  try {
    return JSON.parse(String(res.body));
  } catch (err) {
    return null;
  }
}

/**
 * readUnsettledDepth reads how much KAFKA WORK IS STILL OWED by blnk.event_outbox, preferring
 * the live census and falling back to the exported gauge.
 *
 * # The quantity, and why it is not "pending"
 *
 * This used to read the `pending` count alone, under the name readPendingDepth, and that is
 * the measurement defect it now exists to close. Three other states are un-settled Kafka work
 * and were all treated as quiet:
 *
 *   - `processing` — claimed under a lease, unacknowledged. It is not merely un-counted, it is
 *     the state a STALLED relay holds every row in.
 *   - `failed` — the retry budget is spent and the dead-letter write is still owed. Such a row
 *     has reached NO terminal counter: absent from the published count and absent from the
 *     dead-lettered count. Closing the window over it understates throughput and understates
 *     the DEAD-LETTER RATE, and it is the row most likely to become a dead letter — so the
 *     exclusion flatters V-3 by removing exactly its enriched tail.
 *   - `replaying` — a publish in flight whose completion moves the dead-lettered census that
 *     V-3's stats-sourced numerator is differenced from.
 *
 * `webhook_pending` is excluded: its Kafka leg is acknowledged and already counted, and only
 * the deprecated HTTP leg is owed. See probeEventStats for the full state-by-state rationale.
 *
 * # Two sources, deliberately ranked, and only one of them is COMPLETE
 *
 * GET /events/stats runs a live count against the outbox and returns every state, so it can
 * answer the settling question exactly; it is master-key gated, so it is only available when a
 * key was supplied. blnk_outbox_pending is always available once observability is armed, but
 * the server publishes it as `pending + processing` ONLY — so it is structurally BLIND to the
 * failed tail and to a replay in flight — and it is republished on the collector's tick, so a
 * reading also carries the collection age that says how stale it is.
 *
 * `complete` is what carries that difference to the caller. A gauge reading is a real number
 * and a useful diagnostic, and it is NOT a complete population: the gate reports it, refuses to
 * count it towards quiescence, and names the missing component in its reason. That is the
 * fail-closed half of the fix — "the tail could not be observed" must not read as "there was
 * no tail".
 *
 * @returns {object} `{unsettled, states, complete, source, fresh, age, status}`; `unsettled` is
 *   null when neither source answered, `states` is the per-state breakdown when the live census
 *   answered and null otherwise, and `complete` is true only for a reading that covers every
 *   state of the population.
 */
function readUnsettledDepth() {
  var stats = probeEventStats();
  if (stats.unsettled !== null) {
    return {
      unsettled: stats.unsettled,
      states: {
        pending: stats.pending,
        processing: stats.processing,
        failed: stats.failed,
        replaying: stats.replaying,
      },
      source: DRAIN_SOURCE_STATS,
      // A live query is fresh by construction.
      fresh: true,
      age: null,
      status: stats.status,
      // Complete BY CONSTRUCTION on this path: probeEventStats returns a non-null `unsettled`
      // only when every one of the four states was present in the response, so a live census
      // that answered at all is a census of the whole population.
      complete: true,
    };
  }

  var scrape = scrapeMetrics();
  if (!scrape.available || !scrape.snapshot) {
    return {
      unsettled: null,
      states: null,
      complete: false,
      source: DRAIN_SOURCE_NONE,
      fresh: false,
      age: null,
      status: scrape.status,
    };
  }

  // pendingFromScrape rather than a hand-rolled scan, and the two used to coexist — that helper
  // was defined, documented and never called while this function open-coded the same lookup. The
  // helper resolves the name through resolveSeries, which is the mechanism the rest of this file
  // uses to tolerate the exporter's optional suffixing; the open-coded loop tolerated it too but
  // by a second copy of the rule, which is how one of them comes to be updated and the other not.
  var gauges = scrape.snapshot.gauges || {};
  var pending = pendingFromScrape(scrape);
  var c;

  var age = null;
  for (c = 0; c < SERIES_COLLECTION_AGE.length; c++) {
    if (
      Object.prototype.hasOwnProperty.call(gauges, SERIES_COLLECTION_AGE[c])
    ) {
      age = Number(gauges[SERIES_COLLECTION_AGE[c]]);
      break;
    }
  }

  // THE REPAIR BACKLOGS, ADDED TO THE GAUGE DEPTH (PERF-C01). blnk_outbox_pending is pending
  // plus processing; blnk_events_repair_backlog carries `failed` and `webhook_pending` summed
  // across its two legs. Without this addition the fallback path reproduced the very defect the
  // stats path above was just fixed for, so a run whose master key was unset — which is all it
  // takes to reach this branch — would still declare a drained outbox with the dead-letter write
  // backlog untouched.
  //
  // `replaying` remains UNOBSERVABLE here: no gauge publishes it. The count is therefore a LOWER
  // BOUND on this path and says so through `complete`, rather than being presented as authoritative.
  var owed = null;
  for (c = 0; c < SERIES_REPAIR_BACKLOG.length; c++) {
    if (
      Object.prototype.hasOwnProperty.call(gauges, SERIES_REPAIR_BACKLOG[c])
    ) {
      var reading = Number(gauges[SERIES_REPAIR_BACKLOG[c]]);
      if (!isNaN(reading)) {
        owed = reading;
      }
      break;
    }
  }

  var usable = pending !== null && !isNaN(pending);
  var depth = usable ? pending + (owed === null ? 0 : owed) : null;

  return {
    // `depth` rather than `pending` alone: it folds in the repair backlog the gauge path can
    // see, so the diagnostic figure is the widest one this source can honestly report.
    unsettled: depth,
    // The gauge carries pending and processing FUSED into one number, so neither can be
    // reported on its own; the two states it cannot see at all are reported as null rather
    // than as zero, which is the same distinction `complete` makes one level up.
    states: usable
      ? {
          pending: null,
          processing: null,
          failed: null,
          replaying: null,
        }
      : null,
    source: usable ? DRAIN_SOURCE_GAUGE : DRAIN_SOURCE_NONE,
    fresh: false,
    age: age === null || isNaN(age) ? null : age,
    status: scrape.status,
    // Never complete on this path: `replaying` has no gauge, and the repair backlog may itself be
    // absent on a server whose collector has not published yet.
    complete: false,
  };
}

/**
 * waitForOutboxQuiescence blocks until the event outbox is quiet, or until a budget expires.
 *
 * WHY BOTH ENDS OF THE WINDOW NEED THIS. Every verdict here is a delta between two scrapes,
 * and the pipeline those deltas describe is asynchronous at both ends:
 *
 *   - BEFORE the load, provisioning has just emitted one ledger.created and two
 *     balance.created events per pair — 384 of them at the default spread of 128. Those rows
 *     are still pending when provisioning returns, so they are published DURING the measured
 *     window and counted as though the load produced them. Nothing bounds that error: it
 *     grows with LEDGER_SPREAD, and on a five-minute smoke run it can be most of the
 *     numerator.
 *   - AFTER the load, the last events offered are still pending, an event mid-retry has
 *     reached no terminal counter at all, and an event whose retry budget is SPENT is waiting
 *     for its dead-letter write with no terminal counter either. Ending the window there
 *     discards all three, which understates throughput and — the more serious direction —
 *     understates the DEAD-LETTER RATIO, because an event still working through its five
 *     attempts, or already past them, is precisely the one most likely to end up
 *     dead-lettered. V-3 is the criterion most flattered by stopping the clock early.
 *
 * Neither distortion is bounded, so neither can be dismissed as small. This gate closes both:
 * the baseline is taken once the provisioning events have drained, and the final scrape once
 * the load's have.
 *
 * # What quiescence is measured over
 *
 * The TOTAL of every state from which a Kafka publish is still owed — pending, processing,
 * failed-awaiting-its-dead-letter-write, and replaying — never the `pending` literal alone.
 * That distinction IS the fix: three of those four states used to read as quiet, and the
 * excluded rows are enriched in the outcome V-3 is about, so dropping them flattered the
 * criterion rather than merely shortening the count. readUnsettledDepth owns the arithmetic and
 * probeEventStats documents each state's membership.
 *
 * Quiescence requires DRAIN_STABLE_SAMPLES CONSECUTIVE readings whose TOTAL is at or below
 * DRAIN_FLOOR, and any reading above the floor restarts the run. A single low reading is as
 * likely to be the gap between two relay claims as an empty outbox. Both ends of the window run
 * this same gate, so both ends require the same number of confirmations by construction rather
 * than by two implementations agreeing.
 *
 * # It fails closed on an INCOMPLETE population as well as on a missing one
 *
 * A reading counts towards quiescence only when it covers every state. Without a master key the
 * only available source is blnk_outbox_pending, which the server publishes as pending plus
 * processing and which therefore cannot see the failed tail or a replay in flight — so such a
 * reading is reported, is never counted, and ends the gate with `state_unavailable` and a reason
 * naming what could not be observed. The caller then withholds every verdict. This is the same
 * principle the rest of the file applies to an absent series: a population nobody could observe
 * must read as unobserved, not as empty.
 *
 * When the reading comes from the gauge rather than the live endpoint, a reading counts
 * towards the run only once the collection age proves a NEW collection has happened since the
 * last counted one. Without that check, polling every two seconds against a sixty-second
 * collector tick would satisfy a three-sample stability rule having made ONE observation and
 * read it three times. That rule is retained even though a gauge reading can no longer settle
 * the gate, because the freshness figure is what tells an operator whether raising the budget
 * would help.
 *
 * @param {string} phase a label for the log line: "pre-load" or "post-load".
 * @param {number} budgetSeconds the wall-clock ceiling.
 * @returns {object} the gate's outcome, for the summary's conditions block. `pending` carries
 *   the TOTAL unsettled depth at the last reading, `states` its per-state breakdown when the
 *   live census answered, and `state_unavailable` says the population could not be read
 *   completely.
 */
function waitForOutboxQuiescence(phase, budgetSeconds) {
  var startedAt = Date.now();
  var deadline = startedAt + budgetSeconds * 1000;
  var stable = 0;
  var polls = 0;
  var lastPending = null;
  var lastStates = null;
  var source = DRAIN_SOURCE_NONE;
  var lastAge = null;
  var freshCollections = 0;
  var incompleteReadings = 0;
  // Gauge readings whose freshness could NOT be established because the collection-age series
  // was absent (PERF-M07). Reported rather than tolerated silently: a confirmation that could not
  // be proven to rest on a new collection is weaker evidence than one that could.
  var ageUnavailablePolls = 0;
  // Whether the depth readings were an authoritative all-states count (PERF-C01) or a lower
  // bound. The gauge path cannot see `replaying`, so it is always a lower bound there.
  var depthComplete = null;

  // A zero budget disables the gate EXPLICITLY. It must not loop zero times and then report
  // a settled pipeline it never looked at, because `settled` is what the caller fails on.
  if (!(budgetSeconds > 0)) {
    return {
      phase: phase,
      settled: false,
      skipped: true,
      pending: null,
      states: null,
      state_unavailable: true,
      polls: 0,
      seconds: 0,
      source: DRAIN_SOURCE_NONE,
      fresh_collections: 0,
      age_unavailable_polls: 0,
      depth_complete: false,
      reason: "the settling budget was zero, so the gate did not run",
    };
  }

  while (Date.now() < deadline) {
    var reading = readUnsettledDepth();
    polls++;

    if (reading.source !== DRAIN_SOURCE_NONE) {
      source = reading.source;
    }

    if (reading.unsettled === null) {
      // Neither source answered. That is not quiescence, and it is not a reason to keep
      // waiting for quiescence either — the gate says so and the caller fails closed.
      return {
        phase: phase,
        settled: false,
        skipped: false,
        pending: null,
        states: null,
        state_unavailable: true,
        polls: polls,
        seconds: (Date.now() - startedAt) / 1000,
        source: DRAIN_SOURCE_NONE,
        fresh_collections: freshCollections,
        age_unavailable_polls: ageUnavailablePolls,
        depth_complete: false,
        reason:
          "the unsettled outbox depth could not be read from /events/stats or from " +
          SERIES_OUTBOX_PENDING[0],
      };
    }

    lastPending = reading.unsettled;
    lastStates = reading.states;
    // Once any reading is incomplete the run's drain evidence is incomplete, so this latches.
    depthComplete =
      depthComplete === false ? false : reading.complete === true;

    // AN INCOMPLETE POPULATION IS A REFUSAL, NOT A LOWER CONFIDENCE. The reading is real and is
    // reported, and it cannot end the gate: the states it omits — a failed row owed a
    // dead-letter write, a replay in flight — are precisely the tail whose exclusion flatters
    // V-3, so counting a partial sum towards quiescence would reinstate the defect while
    // looking like a measurement. The loop keeps polling so the figures and the freshness
    // count are still gathered for the diagnostic below, and the budget then expires into the
    // state_unavailable branch.
    if (!reading.complete) {
      incompleteReadings++;
    }

    // Freshness, as described above: the live endpoint always counts, a gauge reading counts
    // only when a new collection has demonstrably occurred since the last counted one.
    //
    // THIS TEST WAS INERT UNTIL collectSnapshot COLLECTED THE AGE GAUGE (PERF-M07). The series
    // was declared and documented as the freshness signal, and never put into snapshot.gauges —
    // so `reading.age` was ALWAYS null, the first disjunct was always taken, `counts` was
    // unconditionally true, and three polls of a SINGLE collection satisfied
    // DRAIN_STABLE_SAMPLES. The gate then declared the outbox settled on one observation, which
    // is precisely what the constant exists to prevent: the server republishes this backlog on
    // its own collector tick, so polling faster than that tick re-reads one number.
    //
    // The `age === null` tolerance is KEPT — a deployment can legitimately lack the gauge — but
    // it is no longer silent. ageUnavailablePolls counts the readings that could not be proven
    // fresh, and it travels out with the result so a settled verdict cannot claim confirmations
    // it did not earn.
    var counts = reading.complete;
    if (reading.source === DRAIN_SOURCE_GAUGE && stable > 0) {
      if (reading.age === null) {
        ageUnavailablePolls++;
      } else if (lastAge !== null) {
        counts = counts && reading.age < lastAge;
      }
    }
    if (counts && reading.source === DRAIN_SOURCE_GAUGE) {
      freshCollections++;
    }
    lastAge = reading.age;

    if (reading.unsettled <= DRAIN_FLOOR) {
      if (counts) {
        stable++;
      }
      if (stable >= DRAIN_STABLE_SAMPLES) {
        return {
          phase: phase,
          settled: true,
          skipped: false,
          pending: reading.unsettled,
          states: reading.states,
          state_unavailable: false,
          polls: polls,
          confirmations: stable,
          seconds: (Date.now() - startedAt) / 1000,
          source: source,
          fresh_collections: freshCollections,
          age_unavailable_polls: ageUnavailablePolls,
          depth_complete: depthComplete === true,
          at_floor: true,
          reason: "",
        };
      }
    } else {
      stable = 0;
    }

    sleep(DRAIN_POLL_SECONDS);
  }

  // The budget expired. There are THREE reasons that can happen and they call for different
  // actions, so they are reported as different reasons rather than as one.
  //
  // The message this replaced said "the outbox still held N pending rows" in both cases. When
  // the depth was already at the floor and only the confirmations were short, that sentence
  // named the wrong cause: it sent a reader to look at a backlog that was not there, while the
  // actual fix was a larger budget or a master key. A diagnostic that points at the wrong
  // thing is worse than no diagnostic.
  //
  // The third case is the incomplete population, and it is FIRST because it subsumes the other
  // two: when the states are not all observable, neither "it drained" nor "the confirmations
  // were short" is a claim this gate is entitled to make about the tail at all.
  var atFloor = lastPending !== null && lastPending <= DRAIN_FLOOR;
  var stateUnavailable = incompleteReadings > 0 && stable < DRAIN_STABLE_SAMPLES;
  var reason;
  if (stateUnavailable) {
    reason =
      "the unsettled population could not be read COMPLETELY on " +
      incompleteReadings +
      " of " +
      polls +
      " poll(s), so quiescence was not established. " +
      SERIES_OUTBOX_PENDING[0] +
      " is published as pending plus processing only: it cannot see a `failed` row whose" +
      " dead-letter write is still owed, nor a replay in flight, and those are exactly the rows" +
      " whose exclusion flatters the dead-letter rate. The last complete-total reading was " +
      (lastPending === null ? "unknown" : lastPending) +
      ". Supply a master key so the live GET /events/stats census is used, which returns every" +
      " state — or set REQUIRE_DRAIN=0 to proceed on an unverified tail, which is only ever" +
      " right for a smoke run";
  } else if (atFloor) {
    reason =
      "the unsettled depth was at or below the floor of " +
      DRAIN_FLOOR +
      ", but only " +
      stable +
      " of the " +
      DRAIN_STABLE_SAMPLES +
      " required INDEPENDENT confirmations were obtained inside the " +
      budgetSeconds +
      "s budget" +
      (source === DRAIN_SOURCE_GAUGE
        ? ". " +
          SERIES_OUTBOX_PENDING[0] +
          " is republished on the server's event-metrics collector tick, so polling faster than that tick returns the same reading and only one confirmation per tick can be counted — the budget has to be at least DRAIN_STABLE_SAMPLES times that tick. " +
          freshCollections +
          " fresh collection(s) were seen over " +
          polls +
          " polls. Either raise the budget, lower DRAIN_STABLE_SAMPLES, or supply a master key so the live /events/stats source is used instead, which needs no tick" +
          (ageUnavailablePolls > 0
            ? ". " +
              ageUnavailablePolls +
              " reading(s) could not be proven fresh at all because " +
              SERIES_COLLECTION_AGE[0] +
              " was absent from the exposition, so freshness was not enforced for them"
            : "")
        : "");
  } else {
    reason =
      "the outbox still held " +
      (lastPending === null ? "an unknown number of" : lastPending) +
      " unsettled rows — pending, processing, failed-awaiting-dead-letter or replaying — when the " +
      budgetSeconds +
      "s budget expired, above the floor of " +
      DRAIN_FLOOR +
      " — the pipeline did not drain, so this is a backlog rather than a measurement problem";
  }

  return {
    phase: phase,
    settled: false,
    skipped: false,
    pending: lastPending,
    states: lastStates,
    state_unavailable: stateUnavailable,
    polls: polls,
    confirmations: stable,
    seconds: (Date.now() - startedAt) / 1000,
    source: source,
    fresh_collections: freshCollections,
    age_unavailable_polls: ageUnavailablePolls,
    depth_complete: depthComplete === true,
    at_floor: atFloor,
    reason: reason,
  };
}

/**
 * logDrain prints one settling gate's outcome.
 *
 * The TOTAL is printed with its per-state breakdown when the live census supplied one, because
 * the total alone cannot distinguish the two situations an operator would act on differently: a
 * queue the relay has not reached yet, and a set of rows that has exhausted its retries and is
 * waiting for a dead-letter write.
 *
 * @param {object} gate the gate's outcome.
 */
function logDrain(gate) {
  var states = gate.states;
  var breakdown = "";
  if (states) {
    breakdown =
      " (pending=" +
      (states.pending === null ? "?" : states.pending) +
      ", processing=" +
      (states.processing === null ? "?" : states.processing) +
      ", failed-awaiting-dlt=" +
      (states.failed === null ? "?" : states.failed) +
      ", replaying=" +
      (states.replaying === null ? "?" : states.replaying) +
      ")";
  }

  console.log(
    "[event_publish] " +
      gate.phase +
      " settling gate: " +
      (gate.skipped ? "SKIPPED" : gate.settled ? "settled" : "DID NOT SETTLE") +
      " after " +
      round(gate.seconds, 1) +
      "s over " +
      gate.polls +
      " poll(s), unsettled=" +
      (gate.pending === null ? "unknown" : gate.pending) +
      breakdown +
      ", confirmations=" +
      numberFrom(gate.confirmations, 0) +
      "/" +
      DRAIN_STABLE_SAMPLES +
      ", source=" +
      describeCode(DRAIN_SOURCE_LABELS, gate.source) +
      (gate.state_unavailable ? ", POPULATION INCOMPLETE" : "") +
      (gate.reason ? " — " + gate.reason : ""),
  );
}

/**
 * suppliedPairs reads pre-provisioned fixtures from LEDGER_PAIRS.
 *
 * This is the way to run the scenario repeatedly without growing the database, and it is the
 * only one available: Blnk has no delete endpoint for a ledger or a balance, so a run that
 * creates its own spread cannot undo it. Supplying the same pairs on every run makes the
 * population constant, which also makes successive runs comparable — an accumulating ledger
 * table changes query plans, and a benchmark whose fixtures grow is measuring two things.
 *
 * A malformed value THROWS rather than falling back. Falling back is what turns a typo into
 * thirty minutes of measuring one partition key.
 *
 * @returns {Array<object>} the supplied pairs, or an empty array when none were configured.
 */
function suppliedPairs() {
  if (!LEDGER_PAIRS_RAW) {
    return [];
  }

  var parsed;
  try {
    parsed = JSON.parse(LEDGER_PAIRS_RAW);
  } catch (err) {
    throw new Error(
      "LEDGER_PAIRS is not valid JSON. Expected an array of" +
        ' {"source":"bln_...","destination":"bln_..."} objects: ' +
        String(err),
    );
  }

  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error(
      "LEDGER_PAIRS must be a non-empty JSON array of" +
        ' {"source":"...","destination":"..."} objects',
    );
  }

  for (var i = 0; i < parsed.length; i++) {
    if (!parsed[i] || !parsed[i].source || !parsed[i].destination) {
      throw new Error(
        "LEDGER_PAIRS entry " +
          i +
          " is missing source or destination. Every entry needs both, because a pair is what" +
          " gives an iteration its own partition key.",
      );
    }
  }

  return parsed;
}

/**
 * provisionAggregates creates the ledgers and balance pairs the load is spread over.
 *
 * Provisioning happens in setup(), before the baseline scrape. That ORDER is necessary but
 * NOT SUFFICIENT for the events it emits to land on the baseline side of every delta: one
 * `ledger.created` and two `balance.created` events per pair are captured in the outbox
 * synchronously but published ASYNCHRONOUSLY by the relay, so at the instant provisioning
 * returns they are still pending and would be published inside the measured window. The
 * settling gate between provisioning and the baseline scrape is what actually puts them on
 * the baseline side; see waitForOutboxQuiescence.
 *
 * Failure is not fatal. A deployment that will not create a ledger — an unauthorised key, an
 * older build, a route that is not registered — still gets a usable run on the `"@" + uuid`
 * shorthand, and the load shape is recorded so the resulting numbers are interpretable rather
 * than merely lower.
 *
 * @returns {object[]} `{source, destination}` pairs; empty when spreading is off or failed.
 */
function provisionAggregates() {
  var pairs = [];
  var ledgerIDs = [];
  var i;

  // Pre-provisioned fixtures win: nothing is created, so nothing is left behind.
  var supplied = suppliedPairs();
  if (supplied.length > 0) {
    return supplied;
  }

  if (!(LEDGER_SPREAD > 0) || !LEDGERS_URL || !BALANCES_URL) {
    return pairs;
  }

  // The fixture-creation contract. Provisioning permanently adds up to LEDGER_SPREAD ledgers
  // and twice that many balances to a database this script cannot clean up, so it requires an
  // explicit acknowledgement rather than happening as a side effect of running the file.
  if (!ALLOW_FIXTURE_CREATION && !SMOKE) {
    throw new Error(
      "This run would create " +
        LEDGER_SPREAD +
        " ledgers and " +
        2 * LEDGER_SPREAD +
        " balances, and Blnk has no delete endpoint for either, so they are PERMANENT and" +
        " every later run and benchmark sees them. Choose one: pass LEDGER_PAIRS to reuse" +
        " existing fixtures, set ALLOW_FIXTURE_CREATION=1 if this database is disposable or" +
        " per-run, or set SMOKE=1 for a shakeout whose numbers are not quoted. Set" +
        " LEDGER_SPREAD=0 to measure single-aggregate ordering deliberately.",
    );
  }

  // Two passes, ledgers first and balances second, rather than one interleaved pass.
  //
  // Creating a balance immediately after its ledger races the search indexer: the ledger's
  // index write is queued asynchronously, and indexing a balance whose ledger document has not
  // landed yet is rejected. That rejection is reported as a system error, and every system
  // error shares ONE partition key, so a burst of them queues behind itself and lands in the
  // latency tail of the very histogram this scenario measures. Separating the passes lets the
  // ledger writes drain while the balances are being created, so the measurement is not
  // polluted by a subsystem this scenario has no interest in.
  for (i = 0; i < LEDGER_SPREAD; i++) {
    var ledger = postJSON(
      LEDGERS_URL,
      { name: "loadtest-events-" + uuidv4() },
      "ledgers",
    );
    if (!ledger || !ledger.ledger_id) {
      break;
    }
    ledgerIDs.push(ledger.ledger_id);
  }

  for (i = 0; i < ledgerIDs.length; i++) {
    var source = postJSON(
      BALANCES_URL,
      { ledger_id: ledgerIDs[i], currency: CURRENCY },
      "balances",
    );
    var destination = postJSON(
      BALANCES_URL,
      { ledger_id: ledgerIDs[i], currency: CURRENCY },
      "balances",
    );
    if (
      !source ||
      !source.balance_id ||
      !destination ||
      !destination.balance_id
    ) {
      break;
    }

    pairs.push({
      source: source.balance_id,
      destination: destination.balance_id,
    });
  }

  return pairs;
}

// ---------------------------------------------------------------------------------------
// Attribution.
// ---------------------------------------------------------------------------------------

/**
 * terminalEventTotal sums the two TERMINAL per-event counters out of a scrape.
 *
 * Both of them, not just the published one: an event that reaches a dead-letter topic is work
 * the pipeline did, and a foreign workload whose events are all failing would otherwise move
 * only the counter this did not read. The published counter is enough on a healthy stack and is
 * not enough on the stack whose isolation is most worth checking.
 *
 * @param {object} scrape a scrapeMetrics result.
 * @returns {number|null} the sum, or null when neither counter could be read.
 */
function terminalEventTotal(scrape) {
  if (!scrape || !scrape.available || !scrape.snapshot) {
    return null;
  }

  var counters = scrape.snapshot.counters || {};
  var total = null;
  var families = [SERIES_PUBLISHED, SERIES_DEAD_LETTERED];

  for (var f = 0; f < families.length; f++) {
    var resolved = resolveSeries(families[f], counters, counters);
    if (resolved.code === SERIES_CODE_ABSENT) {
      continue;
    }

    var value = readNumber(counters, resolved.name);
    if (value === null) {
      continue;
    }

    total = (total === null ? 0 : total) + value;
  }

  return total;
}

/**
 * verifyInstanceIsolation establishes whether the counters this run will difference belong to
 * this run alone.
 *
 * # Why it exists (the attribution problem)
 *
 * Every verdict is a delta of PROCESS-GLOBAL counters and a quantile over a process-global
 * histogram. There is no run dimension to filter on and adding one to the production
 * instruments would mean unbounded label cardinality in the server, paid for permanently to
 * serve a benchmark. So the population is attributable only if the instance is dedicated, and
 * an un-dedicated one distorts the results in the PASSING direction for two of the three
 * verdicts: foreign events inflate throughput and enlarge the dead-letter denominator.
 *
 * # What it actually checks
 *
 * Two independent things, because neither is sufficient alone:
 *
 *   - THE ACKNOWLEDGEMENT. ISOLATED_INSTANCE=1 states that the operator has given this run a
 *     dedicated deployment. Nothing observable can establish that on its own — an idle foreign
 *     client is indistinguishable from an absent one — so it must be asserted.
 *   - THE IDLE PROBE. Called after the pre-load settling gate has proved the outbox quiet and
 *     before the baseline scrape, with no load being offered. Two scrapes
 *     ISOLATION_PROBE_SECONDS apart: any movement in the terminal event counters over that
 *     interval is an event this run did not produce.
 *
 * The probe can DISPROVE isolation and cannot prove it, since a bursty producer can be idle for
 * the length of the probe. It is therefore evidence, not a substitute for the dedicated stack,
 * and the summary records which of the two was satisfied.
 *
 * # What it does about a failure
 *
 * Nothing here throws: the caller decides, so that the whole preflight refusal lives in one
 * place beside the spread check. `verified` is false whenever the contract was not established,
 * and `reason` says which half failed.
 *
 * @returns {object} `{acknowledged, probeSeconds, foreignEvents, probed, verified, reason}`.
 *   `foreignEvents` is null when the probe could not be taken at all.
 */
function verifyInstanceIsolation() {
  var outcome = {
    acknowledged: ISOLATED_INSTANCE,
    probeSeconds: ISOLATION_PROBE_SECONDS > 0 ? ISOLATION_PROBE_SECONDS : 0,
    foreignEvents: null,
    probed: false,
    verified: false,
    reason: "",
  };

  if (!ISOLATED_INSTANCE) {
    outcome.reason =
      "ISOLATED_INSTANCE was not set, so nothing states that this deployment is dedicated to" +
      " this run. Every verdict is a delta of process-global counters, so any other client's" +
      " events are counted as this run's: throughput is inflated, the dead-letter denominator is" +
      " enlarged and the latency population is a mixture. Point the run at a stack nobody else" +
      " is using and set ISOLATED_INSTANCE=1, or set REQUIRE_ISOLATION=0 to proceed with" +
      " results that are not attributable and must not be quoted";

    return outcome;
  }

  if (!(ISOLATION_PROBE_SECONDS > 0)) {
    // The probe is disabled EXPLICITLY. The acknowledgement stands on its own, and the summary
    // says the empirical half was not taken rather than implying it passed.
    outcome.verified = true;
    outcome.reason =
      "ISOLATION_PROBE_SECONDS=0, so the acknowledgement is the only evidence: no idle probe" +
      " was taken and foreign traffic would not have been detected";

    return outcome;
  }

  var before = terminalEventTotal(scrapeMetrics());
  sleep(ISOLATION_PROBE_SECONDS);
  var after = terminalEventTotal(scrapeMetrics());

  if (before === null || after === null) {
    // THE CONTRACT COULD NOT BE ESTABLISHED, which is not the same as being violated and is
    // reported as its own reason. It happens on a stack whose observability is off or whose
    // publisher is a no-op, and on such a stack no verdict is computable anyway.
    outcome.reason =
      "the idle probe could not read the terminal event counters at both ends, so foreign" +
      " traffic can be neither observed nor ruled out. Check that /metrics is reachable and" +
      " that observability is enabled";

    return outcome;
  }

  outcome.probed = true;
  // A NEGATIVE difference is a restart, not a negative amount of traffic, and it is reported as
  // unattributable for the same reason a counter reset invalidates the window: the two readings
  // belong to different processes.
  outcome.foreignEvents = after - before;
  if (outcome.foreignEvents < 0) {
    outcome.reason =
      "the terminal event counters read LOWER after the idle probe than before it, so the" +
      " exporting process restarted during the probe and neither reading can be attributed";

    return outcome;
  }

  if (outcome.foreignEvents > ISOLATION_MAX_FOREIGN_EVENTS) {
    outcome.reason =
      outcome.foreignEvents +
      " event(s) reached a terminal outcome during a " +
      ISOLATION_PROBE_SECONDS +
      "s window in which this run offered NO load, above the tolerance of " +
      ISOLATION_MAX_FOREIGN_EVENTS +
      ". Another workload is publishing to this deployment, so its events would be counted as" +
      " this run's — inflating throughput and diluting the dead-letter rate. Use a dedicated" +
      " instance, or raise ISOLATION_MAX_FOREIGN_EVENTS to accept that contamination knowingly";

    return outcome;
  }

  outcome.verified = true;

  return outcome;
}

// ---------------------------------------------------------------------------------------
// Lifecycle.
// ---------------------------------------------------------------------------------------

/**
 * setup takes the baseline reading, before a single transaction has been offered.
 *
 * The returned object is embedded verbatim in the summary JSON under `setup_data`, which
 * makes it the audit record for the left-hand side of every delta. It therefore contains no
 * credential of any kind — only whether each one was configured — and no raw exposition
 * text.
 *
 * @returns {object} the baseline snapshot and the configuration echo.
 */
export function setup() {
  // A fixture run contacts nothing, so it provisions nothing and takes no baseline. k6 calls
  // setup for every run regardless of which scenarios are registered, so the guard has to be
  // here rather than in the scenario map: without it, `-e FIXTURES=1` would provision ledgers
  // and balances against whatever deployment happened to be reachable.
  if (FIXTURES) {
    return {};
  }

  var pairs = provisionAggregates();
  console.log(
    "[event_publish] provisioned " +
      pairs.length +
      " of " +
      LEDGER_SPREAD +
      " requested ledger/balance pairs" +
      (pairs.length === 0
        ? " — falling back to the @uuid shorthand, which shares ONE partition key, so the relay will publish that key strictly one event at a time regardless of the offered load"
        : ""),
  );

  // Acceptance mode refuses to spend thirty minutes measuring something it cannot certify.
  //
  // Provisioning is sequential and can be cut short by a single failed request, and the old
  // behaviour was to carry on with whatever it got — including nothing, which meant the
  // shorthand fallback and a single partition key. The run then completed, produced a
  // throughput figure that was really one aggregate's serialisation ceiling, and reported it
  // against V-1. Failing here costs a minute; the alternative costs half an hour and produces
  // a number that looks like an answer.
  //
  // SMOKE=1 opts out, and LEDGER_SPREAD=0 is the deliberate single-aggregate measurement.
  if (!SMOKE && LEDGER_SPREAD > 0 && pairs.length < requiredSpread()) {
    throw new Error(
      "acceptance mode needs at least " +
        requiredSpread() +
        " distinct partition keys and provisioning yielded " +
        pairs.length +
        " of the " +
        LEDGER_SPREAD +
        " requested. Events are keyed by aggregate and the relay claims at most one row per" +
        " key per poll, so throughput comes from the number of distinct keys in flight: with" +
        " too few, this run would measure one aggregate's serialisation ceiling and report it" +
        " as V-1. Investigate the provisioning failures above, or set SMOKE=1 for a shakeout," +
        " or MIN_LEDGER_SPREAD to a spread you can justify.",
    );
  }

  // The baseline is taken AFTER provisioning on purpose: the ledger.created and
  // balance.created events provisioning emits belong on the baseline side of the deltas.
  //
  // "After provisioning" was not enough on its own, though, and the gap is the relay's poll
  // interval. Provisioning REQUESTS those events; the relay publishes them a poll or two later.
  // A baseline taken in between put them on the right-hand side of every delta, so they were
  // counted as the run's traffic and their end-to-end latencies — which include however long
  // provisioning's own batch waited — entered the population V-1's p99 is read from. Waiting for
  // the backlog to drain first is what actually puts them behind the baseline.
  // THE PRE-LOAD SETTLING GATE, and there is exactly one of it. Two generations of this gate
  // were both present: this call, and the `preDrain` the return block below hands to teardown
  // — which was never produced, so setup threw a ReferenceError on its own return statement
  // and no run ever reached the load. They are now one gate: the two-phase implementation is
  // the survivor because it is what teardown's drainHeld, preDrainSettled, drainSourceCode and
  // REASON_PIPELINE_NOT_SETTLED are all written against, and because it confirms quiet over
  // DRAIN_STABLE_SAMPLES readings whose freshness it verifies rather than trusting a single
  // reading that can be the gap between two claims.
  var preDrain = waitForOutboxQuiescence("pre-load", PRE_DRAIN_BUDGET_SECONDS);
  // The shape the console line and the two baseline_* summary fields below read. Derived rather
  // than measured separately, so the pre-load figure the summary reports and the pre-load gate
  // teardown judges cannot be two different waits.
  var baselineSettle = {
    settled: preDrain.settled,
    waitedSeconds: numberFrom(preDrain.seconds, 0),
    polls: numberFrom(preDrain.polls, 0),
  };
  logDrain(preDrain);
  console.log(
    "[event_publish] baseline backlog " +
      (baselineSettle.settled ? "settled" : "DID NOT SETTLE") +
      " after " +
      baselineSettle.waitedSeconds.toFixed(1) +
      "s (" +
      baselineSettle.polls +
      " polls)",
  );

  // ATTRIBUTION, checked here and not in teardown, for the same reason the spread check is here:
  // a thirty-minute run that cannot be attributed to itself is thirty minutes spent producing a
  // number nobody may quote, and the cost of finding out now is one idle probe.
  //
  // It runs AFTER the settling gate deliberately. The gate has just proved the outbox quiet, so
  // any terminal counter that moves during the probe is another workload's event rather than
  // this run's own provisioning still draining.
  var isolation = verifyInstanceIsolation();
  console.log(
    "[event_publish] instance isolation " +
      (isolation.verified ? "established" : "NOT ESTABLISHED") +
      ": acknowledged=" +
      (isolation.acknowledged ? "yes" : "no") +
      ", idle probe=" +
      (isolation.probed
        ? isolation.probeSeconds +
          "s saw " +
          isolation.foreignEvents +
          " foreign event(s)"
        : "not taken") +
      (isolation.reason ? " — " + isolation.reason : ""),
  );

  if (!SMOKE && REQUIRE_ISOLATION === 1 && !isolation.verified) {
    throw new Error(
      "acceptance mode requires a Blnk instance dedicated to this run, and that could not be" +
        " established: " +
        isolation.reason +
        " Every verdict this scenario produces is a delta of PROCESS-GLOBAL counters and a" +
        " quantile over a process-global histogram — there is no run dimension to filter on — so" +
        " concurrent traffic is counted as this run's work: it inflates throughput, enlarges the" +
        " dead-letter denominator and mixes the latency population, all in the direction that" +
        " passes. Run against a dedicated stack with ISOLATED_INSTANCE=1, or set" +
        " REQUIRE_ISOLATION=0 (or SMOKE=1) to accept results that are not attributable to this" +
        " invocation and must not be quoted against V-1 or V-3.",
    );
  }

  var scrape = scrapeMetrics();
  // THE BASELINE CENSUS, taken beside the baseline scrape and for one specific purpose: V-3's
  // numerator when blnk_events_dead_lettered_total is absent from the exposition.
  //
  // Without it, teardown had only the CUMULATIVE dead-letter count — every dead letter the
  // deployment has ever accumulated — which is not a window delta and cannot be used as one. The
  // consequence was worse than unusable: the endpoint's mere reachability selected it as the
  // numerator's provenance while the numerator itself stayed at the absent series' zero, so a
  // run with real dead letters could certify a 0% rate. Two readings differenced over the same
  // window is what makes the fallback an actual measurement.
  // THE OUTBOX-TABLE BASELINE, taken beside the metrics baseline (PERF-M01).
  //
  // V-3's numerator has two possible sources and only one of them was ever read. When
  // blnk_events_dead_lettered_total is ABSENT from the exposition, the run declared the stats
  // endpoint as its provenance — and then computed the numerator from the metrics delta anyway,
  // which is null for an absent series and became 0. So V-3 reported a 0.000000 dead-letter rate,
  // passed its `value<0.001` threshold, and named a source that had contributed nothing. A
  // criterion certified from an absent measurement is the exact failure this harness exists to
  // prevent, and it was doing it on the one verdict whose threshold is tightest.
  //
  // A rate needs two readings, so the fallback needs a baseline here: the counter is cumulative
  // and the endpoint's dead_lettered is a live table count, and neither is the run's own
  // contribution on its own. Taken AFTER the pre-load gate, so it excludes provisioning's events
  // exactly as the metrics baseline does.
  var statsBaseline = probeEventStats();
  console.log(
    "[event_publish] baseline outbox counts -> status " +
      statsBaseline.status +
      ", dead_lettered " +
      (statsBaseline.deadLettered === null
        ? "unavailable (V-3 has no fallback numerator if the metrics series is absent too)"
        : statsBaseline.deadLettered),
  );

  console.log(
    "[event_publish] baseline scrape " +
      redactURL(METRICS_URL) +
      " -> status " +
      scrape.status +
      ", " +
      scrape.bodyBytes +
      " bytes" +
      (scrape.available ? "" : " — " + scrape.reason),
  );

  return {
    startedAt: scrape.at,
    metricsStatus: scrape.status,
    metricsAvailable: scrape.available,
    metricsReason: scrape.reason,
    baseline: scrape.snapshot,
    baselineSettled: baselineSettle.settled,
    baselineSettleSeconds: baselineSettle.waitedSeconds,
    pairs: pairs,
    // The pre-load gate travels to teardown through setup data, because teardown is the only
    // stage that can write it into a metric and both gates have to be judged together.
    preDrain: preDrain,
    // The isolation outcome travels the same way, and for the same reason: teardown writes it
    // into a metric and folds it into the availability decision.
    isolation: isolation,
    // The left-hand side of V-3's stats-sourced numerator. Carries no credential — it is a
    // census of row counts — so it is safe in setup_data, which is embedded verbatim in the
    // summary.
    statsBaseline: statsBaseline,
    // A credential-free echo of what the run was told to do, so the numbers below can be
    // interpreted months later without the invoking command line.
    configuration: {
      // Redacted on the way into the summary, which is committed to CI artifacts and read
      // into dashboards. A credential-bearing URL is refused outright during init, so this is
      // defence in depth rather than the only guard.
      url: redactURL(URL),
      metrics_url: redactURL(METRICS_URL),
      events_stats_url: redactURL(EVENTS_STATS_URL),
      scenario: SCENARIO,
      rate: RATE,
      duration: DURATION,
      pre_allocated_vus: VUS,
      max_vus: MAX_VUS,
      target_events_per_sec: TARGET_EVENTS_PER_SEC,
      max_p99_publish_seconds: MAX_P99_PUBLISH_SECONDS,
      max_dead_letter_ratio: MAX_DEAD_LETTER_RATIO,
      require_metrics: REQUIRE_METRICS,
      pre_drain_budget_seconds: PRE_DRAIN_BUDGET_SECONDS,
      post_drain_budget_seconds: POST_DRAIN_BUDGET_SECONDS,
      sample_interval_seconds: SAMPLE_INTERVAL_SECONDS,
      ledger_spread_requested: LEDGER_SPREAD,
      ledger_spread_provisioned: pairs.length,
      subwindow_seconds: SUBWINDOW_SECONDS,
      min_sustained_subwindow_ratio: MIN_SUSTAINED_SUBWINDOW_RATIO,
      min_qualifying_subwindows: MIN_QUALIFYING_SUBWINDOWS,
      ramp_exclusion_seconds: RAMP_EXCLUSION_SECONDS,
      drain_floor: DRAIN_FLOOR,
      drain_stable_samples: DRAIN_STABLE_SAMPLES,
      drain_poll_seconds: DRAIN_POLL_SECONDS,
      require_drain: REQUIRE_DRAIN,
      // The states the settling gates require to be quiet, written out rather than implied, so
      // an artefact says which population its window was closed over.
      settlement_states: [
        "pending",
        "processing",
        "failed (dead-letter write owed)",
        "replaying",
      ],
      settlement_states_excluded: [
        "webhook_pending (its Kafka leg is acknowledged and counted; only the legacy HTTP leg is owed)",
      ],
      isolated_instance: ISOLATED_INSTANCE,
      require_isolation: REQUIRE_ISOLATION,
      isolation_probe_seconds: ISOLATION_PROBE_SECONDS,
      isolation_max_foreign_events: ISOLATION_MAX_FOREIGN_EVENTS,
      api_key_configured: API_KEY ? true : false,
      metrics_bearer_token_configured: METRICS_BEARER_TOKEN ? true : false,
      master_key_configured: MASTER_KEY ? true : false,
    },
  };
}

/**
 * postTxn offers one transaction to the ledger.
 *
 * The body, the header handling and the 30-second timeout are the same as the other
 * scenarios in this directory, so the offered work is comparable across cases and the events
 * this scenario measures are the ordinary ones the pipeline sees in production.
 *
 * @param {string} source the source balance identifier.
 * @param {string} destination the destination balance identifier.
 */
function postTxn(source, destination) {
  // Randomised, and actually SENT. Computing a random amount and then transmitting a
  // literal would make every transaction identical while reading as if it varied.
  var amount =
    Math.floor(Math.random() * (AMOUNT_MAX - AMOUNT_MIN + 1)) + AMOUNT_MIN;
  var payload = JSON.stringify({
    amount: amount,
    description: "event streaming load test",
    precision: 100,
    allow_overdraft: true,
    inflight: true,
    reference: uuidv4(),
    currency: CURRENCY,
    source: source,
    destination: destination,
  });

  var headers = { "Content-Type": "application/json" };
  if (API_KEY) {
    headers["X-Blnk-Key"] = API_KEY;
  }
  var res = http.post(URL, payload, {
    headers: headers,
    timeout: "30s",
    tags: { endpoint: "transactions" },
  });

  // The status alone is not enough to know an event will follow.
  //
  // 201 with no transaction_id, or a status this scenario does not expect, both mean the
  // pipeline did not do what the throughput verdict assumes it did — and both used to leave the
  // run green, because the response shape was never asserted and a failing `check` does not
  // fail k6 on its own. The `checks` threshold in dth() is what makes these enforced; these are
  // what make it meaningful.
  //
  // The accepted statuses are QUEUED and INFLIGHT. The request sets inflight:true, so an
  // accepted transaction is INFLIGHT once applied and QUEUED while it waits for the transaction
  // worker; both produce a captured event. APPLIED is accepted too, because a deployment
  // running with skip_queue would return it and the event is captured just the same. REJECTED
  // is NOT accepted: it is a 201 that produces a transaction.rejected event instead of the
  // applied one this scenario is offering load to produce.
  var body = null;
  try {
    body = res.json();
  } catch (err) {
    body = null;
  }

  check(res, {
    "is status 201": function (r) {
      return r.status === 201;
    },
    "carries a transaction id": function () {
      return body !== null && typeof body.transaction_id === "string" && body.transaction_id !== "";
    },
    "status is queued, inflight or applied": function () {
      if (body === null || typeof body.status !== "string") {
        return false;
      }
      var status = body.status.toUpperCase();

      return status === "QUEUED" || status === "INFLIGHT" || status === "APPLIED";
    },
  });
}

/**
 * publishEvents is the scenario body: one attempted ledger mutation per iteration.
 *
 * The pair is picked at random from the aggregates setup() provisioned, so the load is spread
 * across many partition keys over the run — not that successive iterations necessarily differ,
 * which random selection cannot promise. Spreading is what lets the relay publish in parallel:
 * its claim returns at most one row per key PER BATCH, and a tick chains up to 50 batches.
 *
 * With no provisioned pairs the scenario falls back to the `"@" + uuidv4()` shorthand the other
 * scenarios in this directory use. That still exercises the whole path end to end, but every
 * balance it mints belongs to the same default ledger, so the run measures one key's
 * serialisation rather than the pipeline's throughput. The fallback is recorded in the summary
 * for exactly that reason.
 *
 * @param {object} data the value setup returned.
 */
export function publishEvents(data) {
  var pairs = data && data.pairs ? data.pairs : null;
  if (pairs && pairs.length > 0) {
    var pair = pairs[Math.floor(Math.random() * pairs.length)];
    postTxn(pair.source, pair.destination);

    return;
  }

  var destination = "@" + uuidv4();
  postTxn(destination + "-source", destination);
}

// ---------------------------------------------------------------------------------------
// The sustained-throughput sampler.
//
// Module state, and the one place in this file where per-VU module state is load-bearing.
// Each k6 VU gets its own JavaScript runtime, so a module-scoped variable is private to one
// VU and persists across that VU's iterations. The sampler scenario is therefore pinned to
// EXACTLY ONE VU (`constant-vus` with `vus: 1`), which makes this a single sequential series
// of observations rather than several interleaved ones. With more than one VU each would hold
// its own `samplerPrevious` and difference the same counter against its own earlier reading,
// double-counting every interval.
// ---------------------------------------------------------------------------------------

var samplerPrevious = null;
// How many sampler readings came from a DIFFERENT process than the reading before them
// (PERF-M08). Non-zero means the endpoint is not a single process for the purposes of a counter
// delta — either it restarted, or the scrape is load-balanced across replicas.
var samplerIdentityChanges = 0;
// Counted and skipped INTERVALS, reported as gauges so the summary can say how much of the run
// the sustained claim actually rests on. They are cumulative counts rather than the module
// mutables of the earlier sampler generation, whose value/anchor/name trio this function no
// longer keeps: `samplerPrevious` above is the whole anchor.
var samplerCounted = 0;
var samplerSkipped = 0;

/**
 * sampleThroughput takes one reading of the published counter and judges the interval since
 * the previous reading on its own.
 *
 * WHY THIS SCENARIO EXISTS. V-1 asks for 500 events/sec SUSTAINED. A whole-window delta
 * divided by the window is an AVERAGE, and an average cannot distinguish a pipeline that held
 * 500/sec throughout from one that published nothing for half the run and 1000/sec for the
 * other half. The second pipeline has not sustained anything, and under the old measurement
 * it certified V-1.
 *
 * So the counter is read on a fixed cadence and each inter-sample interval — a SUBWINDOW — is
 * judged separately. The tolerance is explicit and stated in one place: at least
 * MIN_SUSTAINED_SUBWINDOW_RATIO of the qualifying subwindows must individually reach the
 * target, over at least MIN_QUALIFYING_SUBWINDOWS of them.
 *
 * A subwindow QUALIFIES only when it lies wholly inside the steady-state region — at least
 * RAMP_EXCLUSION_SECONDS after the load started and the same before it ends. The executor
 * has to allocate VUs at the start and drains in-flight iterations at the end, so the first
 * and last intervals are partly outside the offered load; judging them would report a
 * throughput failure for a ramp. Excluded subwindows are still measured and still reported —
 * they are only kept out of the pass/fail fraction.
 *
 * @param {object} data the value setup returned.
 */
export function sampleThroughput(data) {
  var now = Date.now();
  var scrape = scrapeMetrics();

  if (!scrape.available || !scrape.snapshot) {
    // A failed scrape breaks the CHAIN, so the next interval cannot be differenced against
    // this instant either: dropping the anchor is what stops a missed scrape from silently
    // producing one double-length subwindow whose rate is averaged over both.
    samplerPrevious = null;
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  var counters = scrape.snapshot.counters || {};
  var value = null;
  var name = "";
  var c;
  for (c = 0; c < SERIES_PUBLISHED.length; c++) {
    if (Object.prototype.hasOwnProperty.call(counters, SERIES_PUBLISHED[c])) {
      value = Number(counters[SERIES_PUBLISHED[c]]);
      name = SERIES_PUBLISHED[c];
      break;
    }
  }

  if (value === null || isNaN(value)) {
    samplerPrevious = null;
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  // THE PROCESS THIS READING CAME FROM (PERF-M08). Every sample is a delta of a per-process
  // counter, so two readings taken from DIFFERENT processes do not difference to anything. The
  // baseline-versus-final identity check cannot see this: it compares the two ends of the run and
  // never the samples in between, so a run whose baseline and final scrape happened to land on
  // one replica while the sampler rotated across the others passed it while every sustained-
  // throughput sample was arithmetic across unrelated counters.
  //
  // Behind a Service or an ingress this is exactly what happens — server-deployment.yaml is
  // horizontally scaled — and the symptom is not an error: it is a counter that appears to jump
  // up and down, which reads as a throughput dip or a reset rather than as a measurement fault.
  var identity = null;
  var identityGauges = scrape.snapshot.gauges || {};
  for (c = 0; c < SERIES_PROCESS_START.length; c++) {
    if (
      Object.prototype.hasOwnProperty.call(identityGauges, SERIES_PROCESS_START[c])
    ) {
      identity = Number(identityGauges[SERIES_PROCESS_START[c]]);
      break;
    }
  }

  var previous = samplerPrevious;
  samplerPrevious = { at: now, value: value, name: name, identity: identity };

  if (previous === null) {
    // The first reading is an anchor, not a subwindow.
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  // A DIFFERENT PROCESS ANSWERED. Dropped rather than measured, and counted so the run can
  // report how often it happened: a handful of changes is a restart, and a change on most
  // samples is a load-balanced endpoint that cannot be sampled at all.
  if (
    identity !== null &&
    previous.identity !== null &&
    identity !== previous.identity
  ) {
    samplerIdentityChanges++;
    samplerSkipped++;
    rateWindowsSkipped.add(samplerSkipped);
    samplerIdentityChangeCount.add(samplerIdentityChanges);
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  if (previous.name !== name) {
    // The series resolved to a different spelling between two readings, so the two numbers
    // are not the same counter and their difference is not a delta.
    samplerSkipped++;
    rateWindowsSkipped.add(samplerSkipped);
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  var elapsed = (now - previous.at) / 1000;
  if (!(elapsed > 0)) {
    samplerSkipped++;
    rateWindowsSkipped.add(samplerSkipped);
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  if (value < previous.value) {
    // A reset inside the run. The interval is unmeasurable and the count of them is reported,
    // because a restart mid-run is also what invalidates the whole-window verdict in teardown.
    subwindowResets.add(1);
    samplerSkipped++;
    rateWindowsSkipped.add(samplerSkipped);
    sleep(SUBWINDOW_SECONDS);

    return;
  }

  var rate = (value - previous.value) / elapsed;
  subwindowsObserved.add(1);
  subwindowThroughput.add(rate);

  // Qualification, decided from the load's own clock rather than from the sample index, so
  // changing the cadence does not silently change how much of the run is excluded.
  var loadStartedAt = numberFrom(data && data.startedAt, 0);
  var loadEndsAt =
    loadStartedAt > 0 ? loadStartedAt + DURATION_SECONDS * 1000 : 0;
  var qualifies =
    loadStartedAt > 0 &&
    loadEndsAt > 0 &&
    previous.at >= loadStartedAt + RAMP_EXCLUSION_SECONDS * 1000 &&
    now <= loadEndsAt - RAMP_EXCLUSION_SECONDS * 1000;

  if (qualifies) {
    subwindowsQualifying.add(1);
    subwindowMetTarget.add(rate >= TARGET_EVENTS_PER_SEC);
  }

  // THE SECOND FAMILY, RECORDED FROM THE SAME READING. event_publish_window_events_per_second
  // and its counted/skipped pair are read by the sampler's own thresholds and by the provenance
  // block, and they used to be fed by a SECOND sampler body spliced onto the end of this one —
  // which maintained five module-level mutables this function no longer has, differenced against
  // its own anchor, and applied its own warm-up rule. Two anchors over one counter is how two
  // reported rates for one run come to disagree; feeding both families from this interval makes
  // them the same measurement by construction. `counted` is the QUALIFYING interval rather than
  // a separately warmed-up one, for the same reason.
  if (qualifies) {
    samplerCounted++;
    rateWindowsCounted.add(samplerCounted);
    windowEventsPerSecond.add(rate);
    // THE THIRD READER OF THIS SAME NUMBER, and it carries the p(50) threshold the sustained
    // criterion is stated on. It was declared, thresholded UNCONDITIONALLY and reported with a
    // full distribution in the provenance block — and never recorded, so `p(50)` had no
    // population and the row rendered a threshold nobody could evaluate. Recorded from the
    // qualifying intervals, which is the population the sustained claim is about.
    intervalEventsPerSecond.add(rate);
  } else {
    // Measured, reported, and deliberately not counted: the interval was not wholly inside the
    // steady-state region.
    samplerSkipped++;
    rateWindowsSkipped.add(samplerSkipped);
  }
  rateWindowWidth.add(elapsed);

  sleep(SUBWINDOW_SECONDS);
}

/*
 * drainOutbox is RETIRED, superseded by waitForOutboxQuiescence.
 *
 * The third of three generations of the same gate to be present in this file simultaneously. Its
 * one distinct argument is preserved in the successor: an UNREADABLE backlog reports not-settled
 * rather than assuming zero, because an unreadable backlog is the same evidential state as a
 * large one.
 */


/**
 * teardown takes the final reading, computes the three verdicts and records them.
 *
 * This is where the measurement lives because it is the only stage that runs after the load
 * and can still emit metrics: handleSummary cannot make HTTP requests and cannot see state
 * assigned here, so a custom metric is the channel between the two.
 *
 * @param {object} data the value setup returned.
 */
export function teardown(data) {
  // Nothing was published, so there is no window to close, no backlog to drain and no closing
  // scrape to take. See the matching guard in setup.
  if (FIXTURES) {
    return;
  }

  // The instant the offered load stopped, captured BEFORE the settling gate runs. The gate
  // deliberately consumes wall-clock time, and that time belongs to neither the load nor the
  // throughput denominator; see the window computation below.
  var loadEndedAt = Date.now();
  var baseline = data || {};
  var startSnapshot = baseline.baseline || {};
  var startCounters = startSnapshot.counters || {};
  var startAttempts = startSnapshot.attempts || {};
  var startHistograms = startSnapshot.histograms || {};
  var startGauges = startSnapshot.gauges || {};
  var startedAt = numberFrom(baseline.startedAt, 0);
  var startStatus = numberFrom(baseline.metricsStatus, STATUS_UNREACHABLE);

  // The load has stopped, but the pipeline has not. Its last events are pending, an event
  // mid-retry has reached no terminal counter, and an event whose retry budget is spent is
  // waiting for its dead-letter write with no terminal counter either — so scraping now would
  // end the window in the middle of the work it is supposed to be measuring, understating
  // throughput and understating the dead-letter ratio by excluding exactly the events most
  // likely to be dead-lettered. The gate runs first, over the WHOLE unsettled population, and
  // the final scrape reads a settled pipeline.
  var postDrain = waitForOutboxQuiescence(
    "post-load",
    POST_DRAIN_BUDGET_SECONDS,
  );
  logDrain(postDrain);

  var preDrain = baseline.preDrain || {
    settled: false,
    skipped: true,
    pending: null,
    states: null,
    // A gate that was never recorded told us nothing about the population, which is exactly
    // what this flag means. Defaulting it to false would assert a completeness no gate observed.
    state_unavailable: true,
    polls: 0,
    seconds: 0,
    source: DRAIN_SOURCE_NONE,
    reason: "setup() recorded no pre-load settling gate",
  };

  preDrainSettled.add(preDrain.settled ? DRAIN_SETTLED : DRAIN_NOT_SETTLED);
  preDrainSeconds.add(numberFrom(preDrain.seconds, 0));
  preDrainPending.add(preDrain.pending === null ? -1 : preDrain.pending);
  postDrainSettled.add(postDrain.settled ? DRAIN_SETTLED : DRAIN_NOT_SETTLED);
  postDrainSeconds.add(numberFrom(postDrain.seconds, 0));
  postDrainPending.add(postDrain.pending === null ? -1 : postDrain.pending);
  // Whether the two gates read the WHOLE unsettled population or only the part
  // blnk_outbox_pending can see. Recorded as its own row because an incomplete reading at the
  // floor is indistinguishable from a settled one in the depth figures alone.
  var settlementStateUnavailable =
    preDrain.state_unavailable === true || postDrain.state_unavailable === true;
  settlementPopulationComplete.add(settlementStateUnavailable ? 0 : 1);
  // THE outbox_drain BLOCK's four inputs. Every one of them was declared, read by
  // buildProvenance and never recorded, and an unrecorded Gauge reads as 0 — so the summary
  // reported `completed: false` and `waited_seconds: 0` for a gate that had in fact settled, on
  // every run. They describe the CLOSING gate, which is the one that block is about.
  drainComplete.add(postDrain.settled === true ? 1 : 0);
  drainWaitSeconds.add(numberFrom(postDrain.seconds, 0));
  drainSeconds.add(numberFrom(postDrain.seconds, 0));
  drainPolls.add(numberFrom(postDrain.polls, 0));
  // The more informative of the two sources, so the summary says how the depth was read.
  drainSourceCode.add(
    postDrain.source !== DRAIN_SOURCE_NONE ? postDrain.source : preDrain.source,
  );

  // Both gates must have held for the window to contain the load's events and only those.
  // REQUIRE_DRAIN=0 keeps the gates and their figures but stops an unsettled pipeline — or a
  // tail this run could not observe completely — from failing the run, which is only ever right
  // for a smoke run.
  var drainHeld =
    REQUIRE_DRAIN !== 1 ||
    (preDrain.settled === true && postDrain.settled === true);
  // The one figure that is about BOTH gates, which is why it is recorded here rather than beside
  // the four above. Also previously never recorded, so `backlog_drained` read 0 — "the backlog
  // did not drain" — on every run including the ones where both gates settled.
  backlogDrained.add(drainHeld ? 1 : 0);

  // ATTRIBUTION, decided in setup and judged here. setup() refuses an acceptance run outright,
  // so a run that reaches teardown unisolated is one that relaxed the requirement — and its
  // figures are still recorded, exactly as REQUIRE_DRAIN=0 keeps the drain figures, with the
  // reason and these four rows saying what the numbers are worth.
  var isolation = baseline.isolation || {
    acknowledged: false,
    probeSeconds: 0,
    foreignEvents: null,
    probed: false,
    verified: false,
    reason: "setup() recorded no isolation check",
  };
  instanceIsolated.add(isolation.verified === true ? 1 : 0);
  isolationAcknowledged.add(isolation.acknowledged === true ? 1 : 0);
  isolationForeignEvents.add(
    isolation.foreignEvents === null || isolation.foreignEvents === undefined
      ? -1
      : isolation.foreignEvents,
  );
  isolationProbeSeconds.add(numberFrom(isolation.probeSeconds, 0));
  var isolationHeld = REQUIRE_ISOLATION !== 1 || isolation.verified === true;

  var scrape = scrapeMetrics();
  var endSnapshot = scrape.snapshot || {};
  var endCounters = endSnapshot.counters || {};
  var endAttempts = endSnapshot.attempts || {};
  var endHistograms = endSnapshot.histograms || {};
  var endGauges = endSnapshot.gauges || {};

  metricsStatusStart.add(startStatus);
  metricsStatusEnd.add(scrape.status);
  // How many distinct partition keys the load was spread over. Zero means the run used the
  // shorthand fallback and therefore measured one aggregate's serialisation ceiling.
  partitionKeys.add(baseline.pairs ? baseline.pairs.length : 0);

  // --- Resolve every series, recording which spelling matched -------------------------
  var dispatched = resolveSeries(SERIES_DISPATCHED, startCounters, endCounters);
  var published = resolveSeries(SERIES_PUBLISHED, startCounters, endCounters);
  var deadLettered = resolveSeries(
    SERIES_DEAD_LETTERED,
    startCounters,
    endCounters,
  );
  var attempts = resolveSeries(
    SERIES_PUBLISH_ATTEMPTS,
    startAttempts,
    endAttempts,
  );
  var captureSeries = resolveSeries(
    SERIES_CAPTURE_TO_DISPATCH,
    startHistograms,
    endHistograms,
  );
  var publishSeries = resolveSeries(
    SERIES_PUBLISH_DURATION,
    startHistograms,
    endHistograms,
  );
  var pendingSeries = resolveSeries(
    SERIES_OUTBOX_PENDING,
    startGauges,
    endGauges,
  );
  // The process identity (PERF-P03), resolved the same way as every other series so a
  // spelling that matched in one scrape and not the other cannot be silently compared.
  var processSeries = resolveSeries(
    SERIES_PROCESS_START,
    startGauges,
    endGauges,
  );

  dispatchedSeriesCode.add(dispatched.code);
  publishedSeriesCode.add(published.code);
  deadLetteredSeriesCode.add(deadLettered.code);

  // --- The measured window -------------------------------------------------------------
  //
  // THE NUMERATOR AND THE DENOMINATOR HAVE TO DESCRIBE THE SAME THING, and with settling
  // gates at both ends there are two candidate intervals:
  //
  //   - The SETTLED WINDOW spans baseline scrape to final scrape. Because each scrape sits on
  //     the quiet side of a gate, that span contains every event the load produced and none
  //     that it did not. It is the interval the DELTAS ACTUALLY ACCUMULATED OVER.
  //   - The OFFERED WINDOW spans baseline scrape to the moment the load stopped. It is the
  //     interval work was submitted over, and it EXCLUDES the post-load drain.
  //
  // BETWEEN THOSE TWO, THE SETTLED WINDOW IS THE HONEST ONE, and the choice is not arbitrary.
  // Events published during the drain are in the delta either way — the gate exists to include
  // them — so dividing by the offered window credits drain-period publishing to load-period
  // seconds. Measured directly on a backlogged pipeline: 153 events, 60.2s offered, 91.1s
  // settled. Almost all of the publishing happened after the load stopped, and the offered
  // denominator reported 2.54/sec against a real publish rate of 1.68/sec — a 51%
  // OVERSTATEMENT, in the direction that produces a false PASS. The settled window has the
  // opposite bias: a pipeline that kept up perfectly is still charged for the drain's few quiet
  // seconds, understating its rate slightly, and that is the safe direction — a false FAIL is
  // visible and investigated, a false PASS is not.
  //
  // THE RATE IS NEVERTHELESS DIVIDED BY NEITHER OF THEM. It is divided by LOAD_SECONDS, the
  // CONFIGURED load interval, which is what "500 events/sec sustained for 30 minutes" is a
  // statement about; see the divisor comment at the throughput computation for why. The settled
  // window is recorded as the audit figure for the delta and gates measurement soundness, and
  // the offered window is recorded beside it so the gap between the two stays readable — a
  // large gap IS the diagnosis that the relay fell behind during the load and caught up
  // afterwards. Neither is the divisor, and the question the offered window was reaching for
  // ("did the pipeline HOLD the rate while work was arriving") is answered by the per-subwindow
  // family rather than by any whole-window ratio.
  //
  var settledMillis = scrape.at - startedAt;
  var measuredWindow =
    startedAt > 0 && settledMillis > 0 ? settledMillis / 1000 : 0;
  var offeredMillis = loadEndedAt - startedAt;
  var offeredWindow =
    startedAt > 0 && offeredMillis > 0 ? offeredMillis / 1000 : 0;
  windowSeconds.add(measuredWindow);
  offeredWindowSeconds.add(offeredWindow);

  // Nothing derived from a delta means anything unless BOTH ends of it were read. With only
  // one end, `deltaCounter` legitimately falls back to the reading it has — which is the
  // right answer for a series that appeared partway through the run, and the wrong one for a
  // scrape that never happened, because it would present a lifetime total as a windowed
  // rate. The gate below is what keeps that number out of the verdicts.
  //
  // CONTINUITY IS PART OF THE GATE (PERF-P03), and it has to be here rather than only in the
  // reason code. This flag decides whether each verdict gauge is RECORDED, and an unrecorded
  // Gauge reads as 0 in the summary — where 0 passes both `value<x` thresholds. So a run whose
  // exporter restarted must not record them: reporting the reason while still recording a
  // number computed across two processes is how a reset gets reported alongside a pass, which
  // is exactly the defect. `restarted` and `resetSeen` are computed below, so this is assigned
  // there.
  var scrapesUsable =
    baseline.metricsAvailable === true &&
    scrape.available &&
    measuredWindow > 0;

  // --- Terminal outcome counters -------------------------------------------------------
  //
  // THE PUBLISHED COUNTER IS THE ONE THE VERDICTS ARE COMPUTED FROM. It moves once per
  // event, at the transition that records the Kafka leg as durable. The dispatched counter is
  // read too, but only so the settlement gap can be reported: it moves once per event as well,
  // at the TERMINAL state, so it trails the published count by however many rows are still
  // waiting in webhook_pending for a legacy webhook leg that has not settled yet.
  var dispatchedFrom = readNumber(startCounters, dispatched.name);
  var dispatchedTo = readNumber(endCounters, dispatched.name);
  var dispatchedChange = deltaCounter(dispatchedFrom, dispatchedTo);

  var publishedFrom = readNumber(startCounters, published.name);
  var publishedTo = readNumber(endCounters, published.name);
  var publishedChange = deltaCounter(publishedFrom, publishedTo);

  var deadLetteredFrom = readNumber(startCounters, deadLettered.name);
  var deadLetteredTo = readNumber(endCounters, deadLettered.name);
  var deadLetteredChange = deltaCounter(deadLetteredFrom, deadLetteredTo);

  dispatchedStart.add(dispatchedFrom === null ? 0 : dispatchedFrom);
  dispatchedEnd.add(dispatchedTo === null ? 0 : dispatchedTo);
  dispatchedDelta.add(
    dispatchedChange.value === null ? 0 : dispatchedChange.value,
  );
  publishedStart.add(publishedFrom === null ? 0 : publishedFrom);
  publishedEnd.add(publishedTo === null ? 0 : publishedTo);
  publishedDelta.add(
    publishedChange.value === null ? 0 : publishedChange.value,
  );
  deadLetteredStart.add(deadLetteredFrom === null ? 0 : deadLetteredFrom);
  deadLetteredEnd.add(deadLetteredTo === null ? 0 : deadLetteredTo);
  deadLetteredDelta.add(
    deadLetteredChange.value === null ? 0 : deadLetteredChange.value,
  );

  // --- Attempts by outcome, corroborating the dead-letter verdict -----------------------
  var attemptDeltas = attemptOutcomeDeltas(
    startAttempts[attempts.name],
    endAttempts[attempts.name],
  );
  attemptsDispatched.add(attemptDeltas.dispatched);
  attemptsRetrying.add(attemptDeltas.retrying);
  attemptsTerminal.add(attemptDeltas.terminal);
  attemptsDeadLettered.add(attemptDeltas.dead_lettered);

  // --- Relay backlog at both ends ------------------------------------------------------
  var pendingFrom = readNumber(startGauges, pendingSeries.name);
  var pendingTo = readNumber(endGauges, pendingSeries.name);
  outboxPendingStart.add(pendingFrom === null ? 0 : pendingFrom);
  outboxPendingEnd.add(pendingTo === null ? 0 : pendingTo);

  // --- The two latency histograms ------------------------------------------------------
  var capture = histogramDelta(
    startHistograms[captureSeries.name],
    endHistograms[captureSeries.name],
  );
  var brokerWrite = histogramDelta(
    startHistograms[publishSeries.name],
    endHistograms[publishSeries.name],
  );

  // V-1'S LATENCY IS READ FROM capture_to_dispatch OR FROM NOTHING. There is no fallback,
  // and removing the one that used to be here is the point.
  //
  // publish_duration measures a STRICTLY SHORTER INTERVAL: its clock starts at the relay's
  // claim, so it excludes the row sitting in blnk.event_outbox waiting for the next poll
  // tick, the poll interval itself, and the claim query. Those omitted parts are exactly
  // where a backlog lives. A relay an hour behind reports the same sub-second
  // publish_duration p99 as an idle one — so substituting it does not degrade the
  // measurement, it answers a DIFFERENT QUESTION and certifies V-1 from the answer.
  //
  // The old code recorded which series it used, and that was judged sufficient. It is not:
  // the substituted number still reached the thresholded gauge, the threshold still
  // evaluated, and the run still reported PASS. A marker beside a wrong verdict does not
  // stop the verdict being consumed — CI reads the threshold result, and a human reading
  // "PASS" does not go looking for a source annotation to disqualify it.
  //
  // So the canonical histogram being absent now FAILS CLOSED through
  // REASON_NO_FIRST_ATTEMPT_SAMPLES, and the broker-write p99 is retained as a supporting
  // figure only — recorded, reported, never thresholded.
  var latencySource =
    capture.quantile.value !== null
      ? P99_SOURCE_CAPTURE_TO_DISPATCH
      : P99_SOURCE_NONE;
  var chosen =
    latencySource === P99_SOURCE_CAPTURE_TO_DISPATCH ? capture : null;

  p99SourceCode.add(latencySource);
  p99InterpolationCode.add(
    chosen ? chosen.quantile.interpolation : INTERPOLATION_NONE,
  );
  // Only two spellings can be reported, because only one instrument can be the source. The
  // publish-duration arm this ternary used to carry was removed with the fallback itself
  // (PERF-P02): a branch that can never be taken reads as a live substitution to anyone
  // grepping for it.
  p99SeriesSpellingCode.add(
    latencySource === P99_SOURCE_CAPTURE_TO_DISPATCH
      ? captureSeries.code
      : SERIES_CODE_ABSENT,
  );

  // THE AUDIT TRAIL IS RECORDED WHETHER OR NOT A QUANTILE CAME OUT, and from `capture`
  // specifically rather than from `chosen`.
  //
  // These rows matter MOST on the runs where the verdict is unavailable, because they are what
  // separates its two causes: `latency_count_end` of zero says the instrument was never
  // populated, while a non-zero end with a zero delta says it was populated before this window
  // and stopped. Gating them on `chosen` would have withheld exactly that evidence at exactly
  // the moment it was needed — a real loss introduced by removing the publish_duration fallback,
  // since `chosen` used to be non-null in cases where it now is not. `histogramDelta` always
  // returns a fully-formed result, zeros included, so there is nothing to guard against.
  //
  // `latency_sum` and `latency_count` are for reading by hand; the p99 is never derived from
  // their ratio, which is a mean.
  latencyCountStart.add(capture.countFrom);
  latencyCountEnd.add(capture.countTo);
  latencyCountDelta.add(capture.countDelta);
  latencySumStart.add(capture.sumFrom);
  latencySumEnd.add(capture.sumTo);
  latencySumDelta.add(capture.sumDelta);

  // The bucket bounds, in contrast, only exist when a quantile was actually located, so they
  // stay gated: a recorded 0 would read as "the p99 fell in the lowest bucket".
  if (chosen) {
    p99BucketLower.add(
      chosen.quantile.lower === null ? 0 : chosen.quantile.lower,
    );
    // Infinity is never recorded: it serialises to null in JSON, which would erase the
    // finding. -1 marks "the rank fell in the +Inf bucket", and the value beside it is then
    // the highest finite boundary, which is a LOWER BOUND on the true p99.
    p99BucketUpper.add(
      chosen.quantile.upper === null || chosen.quantile.upper === Infinity
        ? -1
        : chosen.quantile.upper,
    );
  }

  // The broker write is reported on its own, and NOTHING IS SUBTRACTED FROM IT.
  //
  // What used to be here computed a "queue wait p99" as capture_p99 - brokerWrite_p99.
  // QUANTILES ARE NOT SUBTRACTIVE: the 99th percentile of a difference is not the
  // difference of the 99th percentiles, because the two figures are computed over
  // different populations and the slowest capture-to-dispatch event is generally not the
  // slowest broker write. The result has no interpretation as a latency, and it can come
  // out NEGATIVE — which it will whenever the two histograms' bucket boundaries put the
  // longer interval's rank in a lower bucket than the shorter one's, an ordinary outcome
  // when both p99s land inside one wide bucket.
  //
  // A negative "queue wait" is not merely odd. It is a figure an operator would read as
  // "the relay is ahead of the broker", which is meaningless, and it was published as a
  // supporting figure beside a certified verdict.
  //
  // The honest replacement is the two measurements side by side. A DIRECT capture-to-claim
  // histogram would answer the queue-wait question properly, but no such instrument exists
  // and inventing one in the load script — where it could only be estimated — would repeat
  // the original mistake in a new place. The report says so explicitly rather than
  // omitting the question.
  if (brokerWrite.quantile.value !== null) {
    brokerWriteP99.add(brokerWrite.quantile.value);
    if (capture.quantile.value !== null) {
      // A ROUGH DIAGNOSTIC of how much of the end-to-end figure is backlog rather than broker,
      // and deliberately not called a queue-wait p99.
      //
      // The difference of two quantiles is not the quantile of the difference. Each p99 is the
      // 99th percentile of its OWN population, and the event sitting at the 99th percentile of
      // end-to-end age is generally not the event sitting at the 99th percentile of broker
      // write time — so this number is not any event's queue wait, and it is not the 99th
      // percentile of the queue wait either. It can even come out negative when the two
      // populations differ enough, which is the clearest possible sign that it is not a
      // measurement of a duration.
      //
      // It is still worth reporting: a large positive value means the end-to-end figure is
      // dominated by something other than the broker write, which is the question an operator
      // actually asks first. Read it as that indication and nothing more. Measuring the queue
      // wait itself would take an instrument recording the per-event interval, which the
      // server does not currently export.
      p99Difference.add(capture.quantile.value - brokerWrite.quantile.value);
    }
  }

  // --- V-3's numerator and its provenance, resolved BEFORE the arithmetic ----------------
  //
  // The order is the fix. Provenance used to be resolved eighty lines BELOW the arithmetic,
  // which let the two disagree: `deadLetteredEvents` was assigned from the series delta and
  // nothing else, while the provenance code could select DEAD_LETTER_SOURCE_STATS_ENDPOINT on
  // the strength of any non-null cumulative census value. An absent counter therefore produced a
  // numerator of ZERO labelled as having come from the stats endpoint — a 0% dead-letter rate
  // that clears V-3 on a deployment with real dead letters, reported as a measurement. Resolving
  // the source first and deriving the numerator FROM it is what makes the label and the number
  // one decision.
  //
  // The census fallback is a SAME-WINDOW DELTA, never a cumulative value. `dead_lettered` counts
  // every dead letter the deployment has ever accumulated — the count is not window-bounded, and
  // dead-lettered rows are not purgeable by retention — so a cumulative reading substituted for
  // a window delta would charge this run with the whole history. A NEGATIVE delta is refused
  // rather than clamped to zero: it means rows left the state during the window (a replay, or an
  // operator resolving them), so the difference is not this window's dead-letter count and no
  // numerator can be formed from it.
  var stats = probeEventStats();
  var statsBaseline = baseline.statsBaseline || {
    status: STATUS_UNREACHABLE,
    dispatched: null,
    deadLettered: null,
    pending: null,
    processing: null,
    failed: null,
    replaying: null,
    unsettled: null,
  };

  var statsDeadLetteredWindow = null;
  if (stats.deadLettered !== null && statsBaseline.deadLettered !== null) {
    var censusDelta = stats.deadLettered - statsBaseline.deadLettered;
    statsDeadLetteredWindow = censusDelta < 0 ? null : censusDelta;
  }

  var deadLetterProvenance = DEAD_LETTER_SOURCE_NONE;
  if (deadLettered.code !== SERIES_CODE_ABSENT) {
    deadLetterProvenance = DEAD_LETTER_SOURCE_SERIES;
  } else if (statsDeadLetteredWindow !== null) {
    deadLetterProvenance = DEAD_LETTER_SOURCE_STATS_ENDPOINT;
  }
  deadLetterSource.add(deadLetterProvenance);
  statsDeadLetteredStart.add(
    statsBaseline.deadLettered === null ? -1 : statsBaseline.deadLettered,
  );
  statsDeadLetteredDelta.add(
    statsDeadLetteredWindow === null ? -1 : statsDeadLetteredWindow,
  );

  // --- The arithmetic behind the three verdicts -----------------------------------------
  var dispatchedEvents =
    dispatchedChange.value === null ? 0 : dispatchedChange.value;
  // The EVENT count from the published counter. It is named for what the counter now
  // measures: one increment per event whose Kafka leg became durable, not one per broker
  // write. It used to be called brokerWrites, and the rename was applied to the readers
  // and not to this declaration — which left publishedEvents used twice below and declared
  // nowhere, a ReferenceError that `node --check` cannot see because it parses without
  // resolving identifiers.
  var publishedEvents =
    publishedChange.value === null ? 0 : publishedChange.value;
  // Taken from whichever source the provenance above SELECTED, so the two cannot diverge. An
  // unmeasured numerator stays 0 and the availability gate below refuses to score V-3 from it.
  var deadLetteredEvents = 0;
  if (deadLetterProvenance === DEAD_LETTER_SOURCE_SERIES) {
    deadLetteredEvents =
      deadLetteredChange.value === null ? 0 : deadLetteredChange.value;
  } else if (deadLetterProvenance === DEAD_LETTER_SOURCE_STATS_ENDPOINT) {
    deadLetteredEvents = statsDeadLetteredWindow;
  }
  // The two counters partition a captured event's TERMINAL outcomes — delivered or
  // dead-lettered, never both — so their sum is the population the rate is stated over.
  //
  // Both are incremented at the durable outbox transition rather than at the broker write, so
  // each captured event contributes exactly ONE to this population however many times it was
  // written. That is what makes this a count of unique terminal outcomes; the stats
  // corroboration below reads the same population straight off the outbox table.
  var terminalEvents = publishedEvents + deadLetteredEvents;

  // THE DIVISOR IS THE LOAD INTERVAL, not the measured window.
  //
  // V-1 asks whether the pipeline sustained 500 events/sec while 500/sec was being offered. The
  // scrape-to-scrape window is necessarily LONGER than that: it starts at the baseline scrape,
  // which happens after provisioning, and ends after the tail drain. Dividing by it charges the
  // load for seconds during which no load was offered, so a pipeline that kept up perfectly
  // reports below target — and the error grows with the drain, meaning the better the run the
  // worse it looks. The window is still recorded, as the audit figure for the delta.
  //
  // This pairing is only sound because of the drain above: the numerator has to contain every
  // event the load produced, or dividing by the load interval understates it just as badly.
  //
  // Every division is guarded. A zero interval and a zero terminal population are both real
  // states of a real run, and neither may produce a NaN or an Infinity: JSON.stringify turns
  // both into `null`, which reads as "not measured" for a figure that was measured, or
  // worse, silently drops the finding.
  var throughput = LOAD_SECONDS > 0 ? publishedEvents / LOAD_SECONDS : 0;
  var ratio = terminalEvents > 0 ? deadLetteredEvents / terminalEvents : 0;
  // Published over dispatched, and it is NOT a redelivery factor. Both counters now move
  // ONCE PER EVENT: published at the transition that made the Kafka leg durable, dispatched
  // at the terminal state where nothing further is owed for the row. Their ratio is
  // therefore the SETTLEMENT GAP — how many events have been delivered to Kafka but still
  // owe the legacy webhook leg — and it converges to 1.0 at the sunset, when that leg and
  // the webhook_pending state are removed.
  //
  // The real redelivery factor needs blnk_events_broker_acknowledgements_total, which is the
  // only per-WRITE counter and which this harness does not scrape. Its query is published in
  // docs/metrics.md and is quoted as PROMQL_REDELIVERY_FACTOR below so an operator can run
  // it; deriving it from the two per-event counters here would report 1.0 for every run and
  // call it "no redeliveries".
  var settlementGap = dispatchedEvents > 0 ? publishedEvents / dispatchedEvents : 0;
  if (dispatchedEvents > 0 && publishedEvents > 0) {
    settlementGapFactor.add(settlementGap);
  }

  // --- Continuity: did these two scrapes come from the same process? (PERF-P03) ----------
  //
  // Every figure above is a delta between two readings of a monotonic counter, and a delta is
  // only meaningful while both readings belong to ONE process. The previous check was
  // `end < start`, which catches a restart only while the replacement has not yet counted past
  // the old total — at 500 events a second that window is seconds long, so on a 30-minute run
  // the LIKELY case was the undetectable one: a restart that grows past the baseline and
  // yields a delta that is neither process's work.
  //
  // process_start_time_seconds settles it. It is constant for a process's whole life and
  // different for any replacement, so a change in it IS a restart regardless of what the
  // counters show. Its ABSENCE is treated as unknown rather than as continuity, because
  // assuming continuity is precisely how an undetected restart certifies a target.
  var startedAtBefore = readNumber(startGauges, processSeries.name);
  var startedAtAfter = readNumber(endGauges, processSeries.name);
  var identityKnown = startedAtBefore !== null && startedAtAfter !== null;
  var restarted = identityKnown && startedAtBefore !== startedAtAfter;

  processStartBefore.add(startedAtBefore === null ? -1 : startedAtBefore);
  processStartAfter.add(startedAtAfter === null ? -1 : startedAtAfter);
  exporterRestarted.add(restarted ? 1 : 0);

  loadSecondsMetric.add(LOAD_SECONDS);
  offeredRate.add(RATE);

  var resetSeen =
    dispatchedChange.reset ||
    publishedChange.reset ||
    deadLetteredChange.reset ||
    capture.reset ||
    brokerWrite.reset;
  counterReset.add(resetSeen ? 1 : 0);

  // A SERIES PRESENT AT THE END AND ABSENT AT THE START — reported by the delta helpers
  // themselves rather than re-derived here, so a fifth series added later cannot forget to be
  // checked. Every such delta spans the exporter's whole LIFETIME instead of this window: for the
  // counters an arbitrarily large overstatement of the throughput and of the dead-letter
  // numerator, and for the histogram a confident p99 over the wrong population of samples. It
  // happens for real — an observability stack brought up mid-run, a server restarted between the
  // two scrapes, a series that only begins exporting once its first event exists.
  //
  // The same four series the reset guard covers, for the same reason: three of them feed a
  // verdict directly, and the fourth is the subtrahend of the reported p99 DIFFERENCE, which is
  // a diagnostic-only supporting figure and not a queue-wait percentile.
  var baselineIncomplete =
    publishedChange.baselineMissing ||
    deadLetteredChange.baselineMissing ||
    capture.baselineMissing ||
    brokerWrite.baselineMissing;
  baselineComplete.add(baselineIncomplete ? 0 : 1);

  // The closing gate under the name the three settle_* gauges below read it by. Derived from
  // postDrain rather than measured again: a second wait would report a pipeline that had
  // already been waited for, which is always settled and always zero seconds.
  var settled = {
    settled: postDrain.settled === true,
    waitedSeconds: numberFrom(postDrain.seconds, 0),
    pending: postDrain.pending === null ? null : postDrain.pending,
  };
  backlogSettled.add(settled.settled ? 1 : 0);
  // Two rows rather than one total, because they answer different questions: a long BASELINE wait
  // says provisioning produced more events than expected, a long FINAL wait says the relay was
  // behind the offered load — and a run that failed to settle needs to say which end failed.
  baselineSettleSeconds.add(numberFrom(baseline.baselineSettleSeconds, -1));
  finalSettleSeconds.add(settled.waitedSeconds);
  backlogPendingAtSettle.add(settled.pending === null ? -1 : settled.pending);

  // Where V-3's numerator came from is decided ABOVE, beside the arithmetic that uses it, so the
  // provenance and the number are one decision rather than two that can disagree. An ABSENT
  // dead-letter counter is not a zero: `deltaCounter` hands back 0 for a series that was never
  // exported, and 0/N is a 0% dead-letter rate, so a broken exporter used to certify the delivery
  // target it had stopped measuring. The authenticated census is accepted in its place ONLY as a
  // same-window delta, and when neither source yields one the verdict is unavailable rather than
  // zero.

  // --- Fail closed unless every input was genuinely present ----------------------------
  //
  // Ordered from the most fundamental missing input to the most specific, so the ONE reason
  // reported is the one an operator should act on first. Four of these conditions were
  // previously computed and recorded on gauges beside the verdicts while the verdicts
  // themselves were published anyway — and a gauge is not a threshold, so nothing failed:
  //
  //   - a COUNTER RESET meant the exporting process restarted, so the "delta" was a partial
  //     counter read as a whole window's traffic;
  //   - an INCOMPLETE BASELINE meant a series present at the end was missing at the start, so
  //     its delta was the counter's entire lifetime;
  //   - an UNSETTLED BACKLOG meant events this run captured were still unpublished, so they
  //     were absent from the counters and — being the slowest — from the latency histogram;
  //   - an ABSENT END-TO-END LATENCY SERIES used to substitute the shorter publish-duration
  //     histogram against the same threshold.
  //
  // Each is now a refusal to certify. That is the whole point of the guard: a criterion nobody
  // measured must read as unmeasured, not as met.
  var reason = REASON_AVAILABLE;
  if (baseline.metricsAvailable !== true) {
    reason = REASON_START_SCRAPE_UNAVAILABLE;
  } else if (!scrape.available) {
    reason = REASON_END_SCRAPE_UNAVAILABLE;
  } else if (!isolationHeld) {
    // ATTRIBUTION IS CHECKED BEFORE SETTLING, because it is the more fundamental failure: a
    // window whose edges are fuzzy still measures this pipeline, whereas a population containing
    // another workload's events is not this run's result at all. Naming the settling gate for
    // such a run would send an operator to tune a budget when what they have is a shared stack.
    reason = REASON_INSTANCE_NOT_ISOLATED;
  } else if (!drainHeld) {
    // Both settling gates must have held. See waitForOutboxQuiescence for what each end
    // distorts when it has not.
    //
    // TWO CAUSES, REPORTED APART. "The relay is behind" and "the tail could not be observed"
    // both leave the gate unsettled and lead an operator to different places: the first needs a
    // bigger budget or a faster relay, the second needs a master key so the live census — which
    // is the only source that can see a failed row awaiting its dead-letter write, or a replay
    // in flight — answers instead of blnk_outbox_pending. The more specific cause wins.
    reason = settlementStateUnavailable
      ? REASON_SETTLEMENT_STATE_UNAVAILABLE
      : REASON_PIPELINE_NOT_SETTLED;
  } else if (!(measuredWindow > 0)) {
    reason = REASON_WINDOW_NOT_POSITIVE;
  } else if (restarted) {
    // CONTINUITY IS CHECKED BEFORE SERIES PRESENCE, and the order is a diagnosis decision
    // rather than a preference (PERF-P03). A restart can MAKE a counter look absent: an OTel
    // counter is not exported until its first increment, so a replacement process that has not
    // yet dispatched anything exports no blnk_events_dispatched_total at all. Reported in the
    // other order, that run would say "the series was absent, so nothing has ever been
    // durably recorded" — sending an operator to look for a build or configuration fault that
    // does not exist, when the actual finding is that the process restarted mid-run.
    //
    // It also invalidates the population itself: the terminal count and every delta are
    // computed across two processes, so nothing below this line is worth reporting as a cause.
    reason = REASON_EXPORTER_RESTARTED;
  } else if (resetSeen) {
    // A counter went backwards without an observed restart. Distinguished from the case above
    // so an operator is not sent looking for a restart that did not happen.
    //
    // A RESET INVALIDATES THE WINDOW, it does not merely annotate it. Both scrapes succeeded, so
    // the earlier branches passed, but every counter restarted at zero and deltaCounter then
    // falls back to the end reading — which is right for a series that first appeared mid-run and
    // wrong here: it presents a post-restart partial total as a full-window delta, and that
    // number is LOWER than the truth for throughput and can go either way for the ratio. It used
    // to be recorded in `event_publish_counter_reset` and nothing more, on the view that a marker
    // was enough. It was not: the marker sat beside three gauges that were still written, still
    // thresholded and still reported PASS.
    //
    // THIS BRANCH USED TO BE UNREACHABLE. An identical `resetSeen` test sat above the restart
    // check, which is the merge of two cascades that each ordered these three causes for
    // themselves — and the earlier copy answered every reset before the restart check could
    // attribute one, so REASON_EXPORTER_RESTARTED was never emitted and the distinction this
    // branch exists to draw was never drawn.
    reason = REASON_COUNTER_RESET;
  } else if (!identityKnown) {
    reason = REASON_PROCESS_IDENTITY_UNKNOWN;
  } else if (SERVER_REPLICAS !== 1) {
    // THE DECLARED HALF (PERF-M08), and it catches what no measurement can: a run against a
    // Service fronting several replicas where every scrape happened to reach the same backend
    // still produces a number describing ONE replica's share of the work, and reporting that as
    // the pipeline's throughput understates it by the replica count.
    //
    // The OBSERVED half — the sampler watching the process identity change between consecutive
    // readings — is enforced in buildProvenance rather than here, and not by choice: k6 gives
    // each VU its own module context, so the sampler's counter does not exist in teardown's.
    // The sampler publishes it as a Gauge and the summary reads it back.
    reason = REASON_MEASUREMENT_NOT_SINGLE_PROCESS;
  } else if (published.code === SERIES_CODE_ABSENT) {
    // The PUBLISHED series, not the dispatched one. The dispatched counter's absence degrades
    // only the settlement gap, which is a supporting figure; no verdict reads it. Keying this
    // on the dispatched series is what let an absent supporting counter invalidate a
    // measurable run, and what let the absence of the counter the verdicts ARE read from fall
    // through to REASON_NO_TERMINAL_EVENTS — which reads as "the publisher is a no-op, a
    // legitimate steady state" and is exactly the wrong conclusion to hand an operator whose
    // exporter has stopped exposing the series.
    //
    // Reached only once continuity is established, so an absent series here really is absent
    // rather than a restart's after-effect.
    reason = REASON_PUBLISHED_SERIES_ABSENT;
  } else if (!(terminalEvents > 0)) {
    reason = REASON_NO_TERMINAL_EVENTS;
  } else if (captureSeries.code === SERIES_CODE_ABSENT) {
    // THE SERIES IS NOT EXPORTED AT ALL, which is a different finding from the series being
    // present and empty over the window, and it sends an operator somewhere different: this one
    // is a build or a configuration predating
    // blnk_events_capture_to_dispatch_duration_seconds, that one is a pipeline that published
    // nothing on a first attempt. The broker publish duration is NOT substituted for either —
    // its clock starts at the relay claim, so it measures a strictly shorter interval than the
    // one V-1 is stated over.
    reason = REASON_CAPTURE_SERIES_ABSENT;
  } else if (capture.quantile.value === null) {
    // Keyed on the capture histogram itself rather than on `latencySource`, so that
    // re-introducing any other source cannot quietly make the run certifiable again: V-1 has
    // no population without first-attempt capture-to-dispatch observations, whatever else was
    // scraped. This is the row that fails instead of a p99 borrowed from a shorter interval.
    reason = REASON_NO_FIRST_ATTEMPT_SAMPLES;
  } else if (deadLetterProvenance === DEAD_LETTER_SOURCE_NONE) {
    reason = REASON_DEAD_LETTER_UNMEASURED;
  } else if (!(LOAD_SECONDS > 0)) {
    reason = REASON_LOAD_INTERVAL_UNKNOWN;
  } else if (
    !SMOKE &&
    LEDGER_SPREAD > 0 &&
    (baseline.pairs ? baseline.pairs.length : 0) < requiredSpread()
  ) {
    // setup() refuses this case outright; the gate is repeated here because a resumed or
    // hand-assembled setup_data could reach teardown without having passed through it.
    reason = REASON_SPREAD_INSUFFICIENT;
  }

  degradedReasonCode.add(reason);
  verdictsAvailable.add(reason === REASON_AVAILABLE ? 1 : 0);

  // Each verdict gauge is recorded only when it is a genuine measurement. An unrecorded
  // Gauge reads as 0 in the summary, and 0 passes a `value<x` threshold — so recording a
  // figure derived from a scrape that never happened is the one way this file could report a
  // false pass. `event_publish_verdicts_available` is the row that fails instead, and the
  // three code gauges beside each number say why.
  // Each condition below mirrors one clause of the availability gate. A reset or an
  // undrained backlog invalidates the MEASUREMENT, not just its label, so the numbers derived
  // from it are not recorded at all — an unrecorded Gauge reads as 0 in the summary and 0
  // passes a `value<x` threshold, so recording them would be the one way this file could
  // report a false pass on a run it knows was compromised.
  // drainHeld, not a third drain object: it is the conjunction of BOTH settling gates and it
  // honours REQUIRE_DRAIN, so a smoke run that deliberately relaxed the gate still records its
  // figures while an acceptance run that did not settle records none.
  //
  // isolationHeld is in the conjunction for the same reason and at the same strength: a
  // population that includes another workload's events is not a measurement of this run, and
  // recording a throughput figure derived from it would be the flattering direction. It honours
  // REQUIRE_ISOLATION exactly as the drain honours REQUIRE_DRAIN.
  var measurementSound =
    scrapesUsable && !resetSeen && drainHeld && isolationHeld;

  if (measurementSound && published.code !== SERIES_CODE_ABSENT && LOAD_SECONDS > 0) {
    eventsPerSecond.add(throughput);
  }
  if (
    measurementSound &&
    terminalEvents > 0 &&
    deadLetterProvenance !== DEAD_LETTER_SOURCE_NONE
  ) {
    deadLetterRatio.add(ratio);
  }
  // Only ever the capture-to-dispatch quantile: `chosen` is null unless that series produced
  // it, and brokerWriteP99 above carries the corroborating figure under its own name.
  if (measurementSound && chosen) {
    p99Seconds.add(chosen.quantile.value);
  }

  // --- The outbox census -----------------------------------------------------------------
  //
  // Corroboration for the dispatched and dead-lettered totals, and — for the four unsettled
  // states — the components of the quantity the settling gates are computed from, so the gate's
  // arithmetic is checkable from the artefact. -1 means the figure was not read; the census is
  // master-key gated, so a run without one records -1 across the board and its settling gates
  // report an incomplete population.
  statsEndpointStatus.add(stats.status);
  statsDispatched.add(stats.dispatched === null ? -1 : stats.dispatched);
  statsDeadLettered.add(stats.deadLettered === null ? -1 : stats.deadLettered);
  statsPending.add(stats.pending === null ? -1 : stats.pending);
  statsProcessing.add(stats.processing === null ? -1 : stats.processing);
  statsFailed.add(stats.failed === null ? -1 : stats.failed);
  statsReplaying.add(stats.replaying === null ? -1 : stats.replaying);
  statsUnsettled.add(stats.unsettled === null ? -1 : stats.unsettled);

  reportToConsole({
    reason: reason,
    window: measuredWindow,
    loadSeconds: LOAD_SECONDS,
    offeredRate: RATE,
    // THE SETTLING GATES, as one group. There used to be two — this one and a `drain.*` group
    // thirty lines below reading an object that was never built — and duplicate keys in an
    // object literal are legal, so the later group silently won and every one of its four
    // values was a property read off `undefined`.
    //
    // `drained` is drainHeld, the conjunction of BOTH gates under REQUIRE_DRAIN, because that is
    // what the report is asserting when it says the window contained the load's events. The
    // per-gate figures are reported beside it, so a run that failed to settle says WHICH END
    // failed rather than only that one did.
    drained: drainHeld,
    drainSeconds: numberFrom(postDrain.seconds, 0),
    drainWaited: numberFrom(postDrain.seconds, 0),
    drainPolls: numberFrom(postDrain.polls, 0),
    drainBudgetExpired: postDrain.settled !== true && postDrain.skipped !== true,
    drainStates: postDrain.states || null,
    settlementStateUnavailable: settlementStateUnavailable,
    preDrainSettled: preDrain.settled === true,
    preDrainSeconds: numberFrom(preDrain.seconds, 0),
    // Attribution, so the terminal block says whether the numbers belong to this run.
    isolated: isolation.verified === true,
    isolationHeld: isolationHeld,
    isolationAcknowledged: isolation.acknowledged === true,
    isolationProbed: isolation.probed === true,
    isolationForeignEvents:
      isolation.foreignEvents === null || isolation.foreignEvents === undefined
        ? null
        : isolation.foreignEvents,
    isolationReason: isolation.reason || "",
    offeredWindow: offeredWindow,
    deadLetterSource: deadLetterProvenance,
    statsDeadLetteredWindow: statsDeadLetteredWindow,
    brokerWriteP99: brokerWrite.quantile.value,
    throughput: throughput,
    dispatchedEvents: dispatchedEvents,
    publishedEvents: publishedEvents,
    settlementGap: settlementGap,
    deadLetteredEvents: deadLetteredEvents,
    ratio: ratio,
    latencySource: latencySource,
    // ALWAYS the capture-to-dispatch series, because that is the only series V-1 may be read
    // from. The publish-duration figure travels beside it as a labelled diagnostic rather
    // than in this field, so nothing downstream can print it where the verdict belongs.
    latencySeries: captureSeries.name,
    diagnosticPublishSeries: publishSeries.name,
    diagnosticPublishP99:
      brokerWrite.quantile.value === null ? null : brokerWrite.quantile.value,
    publishedSeries: published.name,
    // Assigned because the console report reads it. The reversal that moved the verdicts onto
    // the published series renamed the readers and dropped this field, and a missing property
    // is not a ReferenceError — the provenance line simply printed "undefined" beside a real
    // number, which is worse than failing.
    dispatchedSeries: dispatched.name,
    deadLetteredSeries: deadLettered.name,
    quantile: chosen ? chosen.quantile : null,
    endScrapeReason: scrape.reason,
    startScrapeReason: baseline.metricsReason || "",
    reset: resetSeen,
    pendingTo: pendingTo,
    partitionKeys: baseline.pairs ? baseline.pairs.length : 0,
    // The fixtures themselves, so the report can print a paste-ready LEDGER_PAIRS value for the
    // next run. Only meaningful when THIS run created them: a run handed LEDGER_PAIRS already
    // has them, and echoing those back would suggest it had made more.
    pairs: baseline.pairs || [],
    createdFixtures: !LEDGER_PAIRS_RAW && (baseline.pairs || []).length > 0,
    // Not the sampler's window count: that lives in the sampler VU's own module state, which
    // teardown runs too late and in the wrong context to read. It reaches the report through
    // its metric, in handleSummary.
    samplerEnabled: RATE_SAMPLER_ENABLED,
  });
}

/**
 * attemptOutcomeDeltas differences the three publish-attempt outcome counters.
 *
 * @param {object|undefined} from the baseline per-outcome totals.
 * @param {object|undefined} to the final per-outcome totals.
 * @returns {object} the per-outcome deltas, zero where the series was absent.
 */
function attemptOutcomeDeltas(from, to) {
  var outcomes = [
    OUTCOME_DISPATCHED,
    OUTCOME_RETRYING,
    "terminal",
    OUTCOME_DEAD_LETTERED,
  ];
  var out = {};
  var i;

  for (i = 0; i < outcomes.length; i++) {
    out[outcomes[i]] = 0;
  }
  if (!to) {
    return out;
  }

  for (i = 0; i < outcomes.length; i++) {
    var before = from ? Number(from[outcomes[i]]) || 0 : 0;
    var after = Number(to[outcomes[i]]) || 0;
    var change = deltaCounter(before, after);
    out[outcomes[i]] = change.value === null ? 0 : change.value;
  }

  return out;
}

/**
 * histogramDelta differences two histogram readings and computes the target quantile over
 * the difference.
 *
 * A MISSING BASELINE matters more here than for a counter, and is easier to miss. With `from`
 * absent every bucket passes through undifferenced, so the quantile is computed over the
 * histogram's ENTIRE LIFETIME of samples and comes back NON-null and plausible-looking. It is
 * not a null the caller can refuse on; it is a confident number over the wrong population,
 * biased in whichever direction the process's history happens to lie. `baselineMissing` is what
 * lets the caller refuse it.
 *
 * @param {object|undefined} from the baseline `{buckets, sum, count}`.
 * @param {object|undefined} to the final `{buckets, sum, count}`.
 * @returns {object} the quantile result, the `_count`/`_sum` readings at both ends, `reset`,
 *   and `baselineMissing`.
 */
function histogramDelta(from, to) {
  var result = {
    quantile: {
      value: null,
      lower: null,
      upper: null,
      interpolation: INTERPOLATION_NONE,
      total: 0,
    },
    reset: false,
    baselineMissing: false,
    countFrom: 0,
    countTo: 0,
    countDelta: 0,
    sumFrom: 0,
    sumTo: 0,
    sumDelta: 0,
  };

  if (!to) {
    return result;
  }

  var delta = deltaBuckets(from ? from.buckets : {}, to.buckets);
  var countFrom = from ? Number(from.count) || 0 : 0;
  var countTo = Number(to.count) || 0;
  var sumFrom = from ? Number(from.sum) || 0 : 0;
  var sumTo = Number(to.sum) || 0;
  var countChange = deltaCounter(countFrom, countTo);
  var sumChange = deltaCounter(sumFrom, sumTo);

  result.quantile = bucketQuantile(TARGET_QUANTILE, delta.buckets);
  result.reset = delta.reset || countChange.reset;
  // `to` is non-null by the guard above, so a falsy `from` is precisely "the series was absent
  // from the baseline scrape" — the case the buckets above passed through undifferenced.
  result.baselineMissing = !from;
  result.countFrom = countFrom;
  result.countTo = countTo;
  result.countDelta = countChange.value === null ? 0 : countChange.value;
  result.sumFrom = sumFrom;
  result.sumTo = sumTo;
  result.sumDelta = sumChange.value === null ? 0 : sumChange.value;

  return result;
}

// round renders a number for human consumption without pulling in a formatter.
function round(value, places) {
  if (value === null || value === undefined || isNaN(value)) {
    return "n/a";
  }
  if (!isFinite(value)) {
    return value > 0 ? "+Inf" : "-Inf";
  }

  var factor = Math.pow(10, places);

  return String(Math.round(value * factor) / factor);
}

// verdictMark renders a pass/fail marker for a console line.
function verdictMark(ok) {
  // NULL IS A THIRD OUTCOME, not a falsy second one (PERF-M09). A row whose metrics carry no
  // registered threshold in this configuration is a diagnostic: it was measured and reported but
  // nothing was asserted about it. Rendering that as FAIL states the opposite of the truth — the
  // whole-run mean is a diagnostic whenever the sampler is running, and it would have printed
  // FAIL beside a perfectly healthy figure. Four characters, so the column stays aligned with
  // PASS and FAIL.
  if (ok === null || ok === undefined) {
    return "n/a ";
  }

  return ok ? "PASS" : "FAIL";
}

/**
 * latencySeriesLabel renders the fully-qualified series the latency p99 was computed from,
 * label filter included.
 *
 * One function builds this string for both the console lines and the summary's provenance
 * block, so the two can never disagree about which series produced the number.
 *
 * The filter differs between the two instruments: capture-to-dispatch carries `topic` and
 * `attempt`, while publish-duration carries `outcome` as well and needs it pinned to
 * `dispatched` or the filter matches nothing. Only the first can produce the V-1 figure now,
 * so only its filter is ever built for a fresh run; the publish-duration branch is retained
 * because handleSummary also renders a summary from an artefact of a version that emitted the
 * reserved source code, and mislabelling that run's series would misreport what it measured.
 *
 * @param {string|null} baseName the resolved histogram base name.
 * @returns {string|null} the series label, or null when nothing was resolved.
 */
function latencySeriesLabel(baseName, sourceCode) {
  if (!baseName) {
    return null;
  }

  // The V-1 series carries no outcome filter — only acknowledged publishes are recorded on it
  // at all — so the label is the same whichever spelling of the name resolved. sourceCode is
  // still read, because a verdict computed from no series must not be labelled as if it came
  // from one.
  if (sourceCode !== P99_SOURCE_CAPTURE_TO_DISPATCH) {
    return null;
  }

  return baseName + '_bucket{attempt="1"}';
}

/**
 * reportToConsole prints the three verdicts, with the series each came from, so an operator
 * watching a thirty-minute run learns the outcome without opening the summary JSON.
 *
 * No credential is printed: only the outcome, the arithmetic and the series names.
 *
 * @param {object} r the computed report.
 */
function reportToConsole(r) {
  var lines = [];
  var available = r.reason === REASON_AVAILABLE;

  lines.push(
    "[event_publish] verdict inputs " +
      (available ? "AVAILABLE" : "UNAVAILABLE — " + REASON_LABELS[r.reason]),
  );
  if (!available && r.startScrapeReason) {
    lines.push("[event_publish]   baseline scrape: " + r.startScrapeReason);
  }
  if (!available && r.endScrapeReason) {
    lines.push("[event_publish]   final scrape:    " + r.endScrapeReason);
  }
  lines.push(
    "[event_publish] load interval   " +
      round(r.loadSeconds, 3) +
      "s at " +
      r.offeredRate +
      " arrivals/sec offered (target " +
      TARGET_EVENTS_PER_SEC +
      "/sec, headroom on purpose) — THIS is the throughput divisor",
  );
  lines.push(
    "[event_publish] measured window " +
      round(r.window, 3) +
      "s settled (the throughput denominator, and the span the deltas cover); load was offered over " +
      round(r.offeredWindow, 3) +
      "s" +
      (r.window > 0 && r.offeredWindow > 0 && r.window > r.offeredWindow * 1.1
        ? " — the gap means the relay was still publishing after the load stopped, so it fell behind during it"
        : ""),
  );
  lines.push(
    "[event_publish] outbox drain    " +
      (r.drained ? "COMPLETE" : "INCOMPLETE") +
      "  after " +
      round(r.drainWaited, 1) +
      "s over " +
      round(r.drainPolls, 0) +
      " polls, unsettled " +
      round(r.pendingTo, 0) +
      (r.drainStates
        ? " (pending=" +
          (r.drainStates.pending === null ? "?" : r.drainStates.pending) +
          ", processing=" +
          (r.drainStates.processing === null ? "?" : r.drainStates.processing) +
          ", failed-awaiting-dlt=" +
          (r.drainStates.failed === null ? "?" : r.drainStates.failed) +
          ", replaying=" +
          (r.drainStates.replaying === null ? "?" : r.drainStates.replaying) +
          ")"
        : "") +
      (r.drained
        ? " — the tail the relay was still working through IS inside the figures below"
        : r.settlementStateUnavailable
          ? " — POPULATION INCOMPLETE: without a master key the depth comes from blnk_outbox_pending, which is pending plus processing only and cannot see a failed row awaiting its dead-letter write or a replay in flight. Those are the rows whose exclusion understates the dead-letter rate, so no verdict is certified from this window"
          : r.drainBudgetExpired
            ? " — BUDGET EXPIRED, so an unknown number of this run's events have no terminal outcome yet and are missing from both figures below (raise DRAIN_BUDGET_SECONDS, or fix the relay)"
            : " — the backlog could not be read, so it is unknown whether the figures below cover every event"),
  );
  lines.push(
    "[event_publish] attribution     " +
      (r.isolated ? "ISOLATED" : "NOT ISOLATED") +
      "  acknowledged=" +
      (r.isolationAcknowledged ? "yes" : "no") +
      ", idle probe=" +
      (r.isolationProbed
        ? round(r.isolationForeignEvents, 0) + " foreign event(s)"
        : "not taken") +
      (r.isolated
        ? " — every counter below is this run's work"
        : " — the counters below are PROCESS-GLOBAL and include any other workload's events, which inflates throughput and dilutes the dead-letter rate" +
          (r.isolationHeld
            ? " (REQUIRE_ISOLATION was relaxed, so the figures are still reported and must not be quoted)"
            : "")) +
      (r.isolationReason ? " — " + r.isolationReason : ""),
  );
  lines.push(
    "[event_publish] " +
      (r.samplerEnabled ? "average rate    " : "V-1 throughput  ") +
      verdictMark(available && r.throughput >= TARGET_EVENTS_PER_SEC) +
      "  " +
      round(r.throughput, 2) +
      " events/sec (target >= " +
      TARGET_EVENTS_PER_SEC +
      ")  from " +
      r.publishedSeries +
      " delta of " +
      round(r.publishedEvents, 0) +
      " — NOT from k6 http_reqs, and NOT from the broker acknowledgement counter",
  );
  lines.push(
    "[event_publish]   " +
      round(r.publishedEvents, 0) +
      " events durably delivered to Kafka against " +
      round(r.dispatchedEvents, 0) +
      " fully settled (" +
      r.dispatchedSeries +
      ") = settlement gap " +
      round(r.settlementGap, 4) +
      (r.settlementGap > 1
        ? " — the difference is events whose legacy webhook leg is still owed, which the sunset removes"
        : ""),
  );

  if (r.quantile) {
    lines.push(
      "[event_publish] V-1 p99 publish " +
        verdictMark(available && r.quantile.value < MAX_P99_PUBLISH_SECONDS) +
        "  " +
        round(r.quantile.value, 4) +
        "s (ceiling < " +
        MAX_P99_PUBLISH_SECONDS +
        ")  from " +
        latencySeriesLabel(r.latencySeries, r.latencySource) +
        " — NOT from k6 http_req_duration",
    );
    lines.push(
      "[event_publish]   bucket (" +
        round(r.quantile.lower, 4) +
        ", " +
        (r.quantile.upper === Infinity ? "+Inf" : round(r.quantile.upper, 4)) +
        "] over " +
        round(r.quantile.total, 0) +
        " first-attempt observations, " +
        INTERPOLATION_LABELS[r.quantile.interpolation],
    );
  } else if (r.latencySource === P99_SOURCE_PUBLISH_DURATION) {
    // Not "no observations": there were observations, on the series V-1 is not stated over.
    // Saying it the other way would send an operator looking for a load problem when what they
    // have is a missing instrument.
    lines.push(
      "[event_publish] V-1 p99 publish FAIL  INVALIDATED — " +
        SERIES_CAPTURE_TO_DISPATCH[0] +
        " was absent, and " +
        r.latencySeries +
        " is not admissible for V-1: its clock starts at the relay claim, so it omits the poll wait and the claim and cannot fail the way V-1 can. The figure is still reported as " +
        M_BROKER_WRITE_P99 +
        ", but no latency verdict was certified from it",
    );
  } else {
    // No borrowed figure and no PASS. publish_duration may well have observations, and it is
    // still printed among the supporting figures, but it cannot certify V-1 — so this run is
    // reported as unable to certify rather than as having met the target.
    lines.push(
      "[event_publish] V-1 p99 publish FAIL  not computable — no first-attempt observations on " +
        latencySeriesLabel(r.latencySeries, r.latencySource) +
        ". There is deliberately NO fallback: publish_duration starts its clock at the relay" +
        " claim, so it cannot certify a target stated from capture to acknowledgement.",
    );
    // Printed BECAUSE it is tempting, and printed as a non-verdict for the same reason. An
    // operator who sees "not computable" and then finds a healthy publish p99 in the summary
    // will otherwise reach for it; saying here what it measures and what it omits is what
    // stops that, and it is the only place the two numbers appear together.
    if (r.diagnosticPublishP99 !== null && r.diagnosticPublishP99 !== undefined) {
      lines.push(
        "[event_publish]   diagnostic only, NOT V-1: " +
          round(r.diagnosticPublishP99, 4) +
          "s p99 on " +
          latencySeriesLabel(
            r.diagnosticPublishSeries,
            P99_SOURCE_PUBLISH_DURATION,
          ) +
          " — claim to acknowledgement, which omits the outbox wait V-1 includes",
      );
    }
  }

  lines.push(
    "[event_publish] V-3 dead-letter " +
      verdictMark(available && r.ratio < MAX_DEAD_LETTER_RATIO) +
      "  " +
      round(r.ratio, 6) +
      " (ceiling < " +
      MAX_DEAD_LETTER_RATIO +
      ")  = " +
      round(r.deadLetteredEvents, 0) +
      " / (" +
      round(r.deadLetteredEvents, 0) +
      " + " +
      round(r.publishedEvents, 0) +
      ")  from " +
      r.deadLetteredSeries +
      "  [source: " +
      DEAD_LETTER_SOURCE_LABELS[r.deadLetterSource] +
      "]",
  );

  // Corroboration, reported under its own name so it cannot be mistaken for the verdict.
  if (r.brokerWriteP99 !== null && r.brokerWriteP99 !== undefined) {
    lines.push(
      "[event_publish]   broker write p99 " +
        round(r.brokerWriteP99, 4) +
        "s — CORROBORATION ONLY, a strictly shorter interval than V-1 (its clock starts at the" +
        " relay claim). Never the verdict.",
    );
  }

  lines.push(
    "[event_publish] load spread over " +
      round(r.partitionKeys, 0) +
      " partition keys" +
      (r.partitionKeys > 0
        ? ""
        : " — the @uuid shorthand fallback shares ONE key, so this run measures one aggregate's serialisation ceiling, not throughput"),
  );
  if (r.pendingTo !== null && r.pendingTo !== undefined) {
    lines.push(
      "[event_publish] relay backlog at the final scrape: " +
        round(r.pendingTo, 0) +
        " outbox rows still unpublished (blnk_outbox_pending: pending plus processing)" +
        (r.pendingTo > DRAIN_FLOOR
          ? " — ABOVE the settling floor of " +
            DRAIN_FLOOR +
            ", so the post-load gate did not reach quiescence and the window excludes work the load offered"
          : " — at or below the settling floor, so the window contains the load's events"),
    );
  }
  if (r.deadLetterSource === DEAD_LETTER_SOURCE_STATS_ENDPOINT) {
    // Said out loud because the numerator did not come from the series the line above names,
    // and a reader comparing the two would otherwise find them inconsistent.
    lines.push(
      "[event_publish]   V-3's numerator is the /events/stats census differenced over the run (" +
        round(r.statsDeadLetteredWindow, 0) +
        " dead letter(s)), because " +
        SERIES_DEAD_LETTERED[0] +
        " was absent from the exposition. A cumulative census value is never used on its own",
    );
  }
  if (r.reset) {
    lines.push(
      "[event_publish] FAIL a counter reset was detected — the exporting process restarted mid-run, so the baseline belongs to a process that no longer exists and NO delta across this window is meaningful. The verdicts are withheld, not annotated",
    );
  }

  // THE FIXTURES THIS RUN CREATED, printed as a paste-ready LEDGER_PAIRS value.
  //
  // Blnk has no delete endpoint for a ledger or a balance, so anything provisioned here is
  // PERMANENT. That makes reuse the only way to certify repeatedly without growing the database
  // without bound — and reuse needs the identifiers, which were otherwise reachable only by
  // querying the API for balances this run happened to create. They cannot be published in the
  // summary instead: k6 does not pass setup data to handleSummary, so this line is the channel.
  //
  // Printed only when the run created them. A run given LEDGER_PAIRS already has them, and a
  // run that created nothing has nothing to offer.
  if (r.createdFixtures && r.pairs && r.pairs.length > 0) {
    // BASE64, and not the JSON itself, because k6 renders every console line as logfmt: a value
    // containing a double quote is emitted with the quotes BACKSLASH-ESCAPED, on a terminal as
    // well as into a file. Printing the JSON directly therefore produces a line that looks
    // copy-pasteable and is not — pasting it yields `[{\"source\":...` and the next run dies with
    // "LEDGER_PAIRS is not valid JSON". Base64 has no quotes and no spaces, so logfmt passes it
    // through byte for byte, and the decode is one documented command.
    lines.push(
      "[event_publish] fixtures created and PERMANENT. Reuse them instead of creating more — decode the token below as tests/loadtest/README.md describes:",
    );
    // ONE TOKEN, no quotes and no spaces, so logfmt emits it verbatim. The decode command lives
    // in tests/loadtest/README.md rather than here for the same reason: any shell syntax printed
    // on this line would need a double quote, and logfmt escapes those.
    lines.push(
      "  LEDGER_PAIRS_B64=" + encoding.b64encode(JSON.stringify(r.pairs)),
    );
  }

  for (var i = 0; i < lines.length; i++) {
    console.log(lines[i]);
  }
}

// ---------------------------------------------------------------------------------------
// Reporting.
// ---------------------------------------------------------------------------------------

// The vocabularies the numeric codes stand for. A gauge carries a number, so the strings are
// reconstituted here, in the only stage that writes the summary.
const SERIES_CODE_LABELS = {};
SERIES_CODE_LABELS[SERIES_CODE_ABSENT] = "absent from the exposition";
SERIES_CODE_LABELS[SERIES_CODE_DOCUMENTED_NAME] = "matched the documented name";
SERIES_CODE_LABELS[SERIES_CODE_TOLERATED_NAME] =
  "matched the bare name, without the exporter's _total suffix";

const P99_SOURCE_LABELS = {};
P99_SOURCE_LABELS[P99_SOURCE_NONE] = "not computable";
P99_SOURCE_LABELS[P99_SOURCE_CAPTURE_TO_DISPATCH] =
  "capture-to-dispatch, the documented V-1 series: outbox capture to broker acknowledgement";
// RETAINED BUT UNREACHABLE AS A VERDICT SOURCE (PERF-P02). Nothing assigns this code any
// more: publish_duration measures claim-to-acknowledgement, a strictly shorter interval than
// V-1 is stated over, so certifying the target from it was optimistic even though the
// substitution was recorded. The label stays so a summary produced by an OLDER revision of
// this script remains readable, and so the reason it was removed is written down where the
// substitution used to be described.
P99_SOURCE_LABELS[P99_SOURCE_PUBLISH_DURATION] =
  "publish-duration: claim to broker acknowledgement, a strictly shorter interval than V-1 is stated over. NO LONGER EMITTED — it was once accepted as a fallback, which passed runs whose queue wait was the whole problem; a summary carrying this code was produced by that earlier version";

const INTERPOLATION_LABELS = {};
INTERPOLATION_LABELS[INTERPOLATION_NONE] = "no observations in the window";
INTERPOLATION_LABELS[INTERPOLATION_LINEAR] =
  "linearly interpolated between the bucket bounds, as histogram_quantile does";
INTERPOLATION_LABELS[INTERPOLATION_HIGHEST_FINITE_BOUND] =
  "the rank fell in the +Inf bucket, so the highest finite boundary is reported as a LOWER BOUND on the true p99";
INTERPOLATION_LABELS[INTERPOLATION_LOWEST_BUCKET] =
  "the rank fell in the lowest bucket, whose upper bound is not positive, so that bound is reported directly";

// Recovered because describeCode(DRAIN_SOURCE_LABELS, ...) reads it in the settling-gate log
// line and the declaration was dropped, which is a ReferenceError on the first gate rather than
// an unlabelled number.
const DRAIN_SOURCE_LABELS = {};
DRAIN_SOURCE_LABELS[DRAIN_SOURCE_NONE] =
  "no source answered, so the unsettled depth was never read";
DRAIN_SOURCE_LABELS[DRAIN_SOURCE_STATS] =
  "GET /events/stats, a live per-status census of blnk.event_outbox — the only source that covers the WHOLE unsettled population: pending, processing, failed-awaiting-its-dead-letter-write and replaying";
DRAIN_SOURCE_LABELS[DRAIN_SOURCE_GAUGE] =
  "the blnk_outbox_pending gauge, republished on the collector's tick, with freshness confirmed from the collection age. It is pending PLUS PROCESSING and nothing else, so it cannot see the failed tail or a replay in flight — an INCOMPLETE population, which this gate reports and refuses to certify quiescence from";

const REASON_LABELS = {};
REASON_LABELS[REASON_AVAILABLE] = "every verdict input was read successfully";
REASON_LABELS[REASON_START_SCRAPE_UNAVAILABLE] =
  "the baseline /metrics scrape did not succeed, so there is no left-hand side to any delta";
REASON_LABELS[REASON_END_SCRAPE_UNAVAILABLE] =
  "the final /metrics scrape did not succeed, so there is no right-hand side to any delta";
REASON_LABELS[REASON_WINDOW_NOT_POSITIVE] =
  "the measured window was not positive, so no rate can be computed from it";
REASON_LABELS[REASON_PUBLISHED_SERIES_ABSENT] =
  "blnk_events_published_total was absent, so no event's Kafka leg has ever been durably recorded by this process and the series every per-event verdict is read from does not exist. An absent blnk_events_dispatched_total does NOT reach here: it degrades only the settlement gap";
REASON_LABELS[REASON_CAPTURE_SERIES_ABSENT] =
  "blnk_events_capture_to_dispatch_duration_seconds was absent, and V-1 is stated over that interval — the broker publish duration measures a strictly shorter one and is NOT substituted for it";
REASON_LABELS[REASON_EXPORTER_RESTARTED] =
  "process_start_time_seconds changed between the two scrapes, so the exporter restarted and every counter delta spans two different processes";
REASON_LABELS[REASON_COUNTER_RESET] =
  "a counter read lower at the end than at the start, so it reset during the run and no delta over it is meaningful";
REASON_LABELS[REASON_MEASUREMENT_NOT_SINGLE_PROCESS] =
  "the metrics endpoint does not front a single process, so a counter delta read from it mixes counters that never belonged to one series. Either point METRICS_URL at ONE process — a port-forward to a single pod, or the Compose stack — or read the verdicts from Prometheus using the equivalent_promql each one publishes, which aggregates across replicas. Declaring SERVER_REPLICAS=1 does not make a load-balanced endpoint single-process; it only stops this check from noticing";
REASON_LABELS[REASON_PROCESS_IDENTITY_UNKNOWN] =
  "process_start_time_seconds was absent from a scrape, so a restart can be neither confirmed nor ruled out and continuity cannot be assumed";
REASON_LABELS[REASON_NO_TERMINAL_EVENTS] =
  "no event reached a terminal outcome in the window — with KAFKA_BROKERS unset the publisher is a no-op, which is a legitimate steady state and not a failure of the pipeline";
REASON_LABELS[REASON_NO_FIRST_ATTEMPT_SAMPLES] =
  'no first-attempt capture-to-dispatch observations were recorded, so V-1 has no population; the attempt="1" filter matched nothing. There is deliberately no fallback to publish_duration, which measures a shorter interval than the target is stated over';
REASON_LABELS[REASON_DEAD_LETTER_UNMEASURED] =
  "the dead-letter counter was absent and no authenticated /events/stats reading corroborated it, so V-3 cannot be distinguished from a broken exporter; an absent counter coerced to zero would have passed the ceiling while measuring nothing";
REASON_LABELS[REASON_PIPELINE_NOT_SETTLED] =
  "the event outbox would not go quiet inside its budget at one end of the window, so the window's contents are not the load's events: undrained provisioning events would be credited to the load, or the load's own pending, mid-retry and failed-awaiting-dead-letter events excluded from it";
REASON_LABELS[REASON_SETTLEMENT_STATE_UNAVAILABLE] =
  "a settling gate could not read the WHOLE unsettled population, so quiescence was never established: blnk_outbox_pending is pending plus processing only, so a failed row awaiting its dead-letter write and a replay in flight are invisible to it — and those are the rows whose exclusion understates the dead-letter rate. Supply a master key so the live GET /events/stats census answers instead";
REASON_LABELS[REASON_INSTANCE_NOT_ISOLATED] =
  "the measured counters cannot be attributed to this run: the idle probe observed terminal event traffic while no load was offered, or the isolated-instance contract was not established. Every verdict is a delta of PROCESS-GLOBAL counters, so foreign traffic inflates throughput and dilutes the dead-letter rate";
REASON_LABELS[REASON_SERIES_RESET] =
  "a required counter or histogram reset inside the window, so every delta describes only the fraction of the run after the restart rather than the run the target is stated over";
REASON_LABELS[REASON_BACKLOG_NOT_DRAINED] =
  "the outbox backlog had not drained when the closing scrape was due, so events the offered load captured are missing from the measured side of every delta";
REASON_LABELS[REASON_LOAD_INTERVAL_UNKNOWN] =
  "DURATION could not be parsed into seconds, so the throughput divisor is unknown; a rate cannot be reported without knowing what it is a rate over";
REASON_LABELS[REASON_SPREAD_INSUFFICIENT] =
  "fewer distinct partition keys were provisioned than acceptance mode requires, so the relay's one-row-per-key claim caps throughput at the spread rather than at the pipeline's capacity";

const DEAD_LETTER_SOURCE_LABELS = {};
DEAD_LETTER_SOURCE_LABELS[DEAD_LETTER_SOURCE_NONE] =
  "NOTHING — the counter was absent from the exposition and no same-window /events/stats delta could be formed, so V-3 is not measured";
DEAD_LETTER_SOURCE_LABELS[DEAD_LETTER_SOURCE_SERIES] =
  "the exported dead-letter counter, present with an explicit value, differenced over the measured window";
DEAD_LETTER_SOURCE_LABELS[DEAD_LETTER_SOURCE_STATS_ENDPOINT] =
  "the authenticated GET /events/stats census, differenced between the BASELINE reading taken in setup and the final reading taken in teardown — the counter being absent from the exposition. A cumulative census value is never used on its own: it counts every dead letter the deployment has ever accumulated, so substituting it for a window delta would report the whole history as this run's numerator";

// The canonical PromQL each figure is the in-script equivalent of, quoted from
// docs/metrics.md so that the documented query and the reported number cannot drift apart.
// sum(rate(...)), not rate(...). blnk_events_published_total carries `topic` and
// `event_type`, so a bare rate() returns ONE SERIES PER LABEL COMBINATION — currently eight
// topics times thirteen event types. The verdict is a single figure for the pipeline's total
// output, and this script computes it by summing the counter across every label set before
// differencing, so the equivalent query has to aggregate too. A bare rate() pasted into
// Prometheus produces a graph of many lines, none of which is the number reported here, and
// each of which is smaller — reading the largest as the throughput would understate it by
// most of an order of magnitude.
const PROMQL_THROUGHPUT = "sum(rate(blnk_events_published_total[5m]))";
// The sustained claim, expressed the way an operator would check it on a dashboard: the same
// aggregated rate, evaluated over the subwindow rather than a five-minute average, with the
// target as the comparison. min_over_time is what makes it a statement about every interval
// rather than about their mean.
const PROMQL_SUSTAINED =
  "min_over_time(sum(rate(blnk_events_published_total[" +
  SUBWINDOW_SECONDS +
  "s]))[30m:" +
  SUBWINDOW_SECONDS +
  "s])";
const PROMQL_P99 =
  'histogram_quantile(0.99, sum by (le) (rate(blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))';
const PROMQL_BROKER_WRITE =
  'histogram_quantile(0.99, sum by (le) (rate(blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))';
// The subtraction of the two p99 figures. NOT a queue-wait p99 — see the comment on
// M_P99_DIFFERENCE — so it is named and described as the difference it is.
const PROMQL_P99_DIFFERENCE = PROMQL_P99 + " - " + PROMQL_BROKER_WRITE;
// The sustained-rate verdict has no single-expression PromQL equivalent, because "every window
// held the rate" is a statement over a range of instants rather than a value at one. The
// closest reading in Prometheus is to graph the rate expression above and look at its minimum
// over the run window, which is what min_over_time does.
const PROMQL_SUSTAINED_THROUGHPUT =
  "min_over_time(rate(blnk_events_published_total[1m])[30m:1m])";
// Quoted from docs/metrics.md verbatim, denominator included. It used to read
// blnk_events_dispatched_total there, which is per-event and partitions the terminal outcomes
// just as validly — but it is not the counter this script differences, and a query that does
// not match the number printed beside it defeats the whole point of quoting one.
const PROMQL_DEAD_LETTER =
  "sum(rate(blnk_events_dead_lettered_total[30m])) / (sum(rate(blnk_events_dead_lettered_total[30m])) + sum(rate(blnk_events_published_total[30m])))";
// The settlement gap: events whose Kafka leg is durable over events whose row has reached the
// terminal state. Above 1.0 is the population still owed a legacy webhook, and it goes to 1.0
// at the sunset when that leg is removed.
const PROMQL_SETTLEMENT_GAP =
  "sum(rate(blnk_events_published_total[5m])) / sum(rate(blnk_events_dispatched_total[5m]))";
// THE REAL REDELIVERY FACTOR, quoted from docs/metrics.md and reported here only as a pointer:
// this harness does not scrape blnk_events_broker_acknowledgements_total, so it cannot compute
// the figure. It is the only per-WRITE counter, and it is filtered to original publishes
// because replays and dead-letter writes are acknowledged too and are deliberately never
// counted as deliveries. Deriving a redelivery factor from the two PER-EVENT counters instead
// would report 1.0 for every run and call it "nothing was republished".
const PROMQL_REDELIVERY_FACTOR =
  'sum(rate(blnk_events_broker_acknowledgements_total{purpose="original"}[5m])) / sum(rate(blnk_events_published_total[5m]))';

/**
 * promqlForLatencySource returns the query that matches the series a p99 actually came from.
 *
 * The provenance block used to quote the capture-to-dispatch query unconditionally, including
 * when the number beside it had been computed from publish_duration. A reader checking the
 * figure against Prometheus would then run a query over a different interval, get a different
 * answer, and have no way to tell which of the two was wrong. This file no longer substitutes
 * series at all, so the mismatch cannot arise from a fallback — but the query is still resolved
 * from the recorded source rather than assumed, because an assumption is what went wrong.
 *
 * @param {number|null} sourceCode the recorded P99_SOURCE_* value.
 * @returns {string} the PromQL equivalent, or a statement that there is none.
 */
function promqlForLatencySource(sourceCode) {
  if (sourceCode === P99_SOURCE_CAPTURE_TO_DISPATCH) {
    return PROMQL_P99;
  }
  if (sourceCode === P99_SOURCE_PUBLISH_DURATION) {
    return PROMQL_BROKER_WRITE;
  }

  return "none — no p99 was computed, so no query reproduces it";
}

// gaugeValue reads a custom gauge's end-of-run value out of the summary data.
function gaugeValue(metrics, name) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.values) {
    return null;
  }

  var v = Number(m.values.value);

  return isNaN(v) ? null : v;
}

/**
 * trendValue reads one aggregation off a custom Trend in the summary data.
 *
 * Separate from gaugeValue because a Trend publishes a map of aggregations — avg, min, max,
 * med, p(90), p(95) — and has no `value` key at all, so reading it as a Gauge yields null and
 * would report a measured window minimum as "not measured".
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @param {string} aggregation which aggregation to read, e.g. "min".
 * @returns {number|null} the value, or null when the metric or aggregation is absent.
 */
function trendValue(metrics, name, aggregation) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.values) {
    return null;
  }

  var v = Number(m.values[aggregation]);

  return isNaN(v) ? null : v;
}

/**
 * rateValue reads a k6 Rate metric's end-of-run fraction out of the summary data.
 *
 * A Rate that received no samples has no `rate` value, and that is returned as null rather
 * than as 0. The distinction carries weight here: 0 reads as "no subwindow met the target",
 * which is a measurement, whereas null is "no subwindow was ever judged", which is the
 * absence of one — and it is the state the qualifying-count threshold exists to fail on.
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @returns {number|null} the fraction, or null when the metric received no samples.
 */
function rateValue(metrics, name) {
  var m = metrics ? metrics[name] : null;
  if (
    !m ||
    !m.values ||
    m.values.rate === undefined ||
    m.values.rate === null
  ) {
    return null;
  }

  var v = Number(m.values.rate);

  return isNaN(v) ? null : v;
}

/**
 * counterTotal reads a k6 Counter's accumulated total out of the summary data.
 *
 * gaugeValue cannot be used for this: a Counter exposes `count` and a k6-derived `rate`, and
 * has no `value` at all, so reading it as a Gauge silently yields null for every counter in
 * the summary — which is how a subwindow count came out as "n/a" beside a fraction that had
 * been computed from it.
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @returns {number|null} the total, or null when the metric is absent.
 */
function counterTotal(metrics, name) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.values || m.values.count === undefined) {
    return null;
  }

  var v = Number(m.values.count);

  return isNaN(v) ? null : v;
}

/**
 * trendStat reads one aggregate off a k6 Trend metric.
 *
 * `min` is the aggregate that matters for a sustained claim: it is the worst interval the
 * pipeline had, which is precisely the figure an average is capable of hiding.
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @param {string} stat the aggregate to read — "min", "med", "avg", "max".
 * @returns {number|null} the value, or null when absent.
 */
function trendStat(metrics, name, stat) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.values || m.values[stat] === undefined) {
    return null;
  }

  var v = Number(m.values[stat]);

  return isNaN(v) ? null : v;
}

/**
 * thresholdVerdicts extracts a metric's threshold results as expression to pass/fail.
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @returns {object|null} expression to boolean, or null when there are no thresholds.
 */
function thresholdVerdicts(metrics, name) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.thresholds) {
    return null;
  }

  var out = {};
  for (var expression in m.thresholds) {
    if (!Object.prototype.hasOwnProperty.call(m.thresholds, expression)) {
      continue;
    }
    out[expression] = m.thresholds[expression]
      ? m.thresholds[expression].ok === true
      : false;
  }

  return out;
}

/**
 * verdictThresholdsFor resolves a verdict row against the thresholds ACTUALLY REGISTERED.
 *
 * # The defect this exists to remove (PERF-M09)
 *
 * The summary used to attach thresholds to rows by hand, and the hand-wiring drifted from what
 * `options` registered in two directions at once:
 *
 *   * A ROW SHOWED ANOTHER METRIC'S RESULT. `sustained_throughput_events_per_second` displayed
 *     min(window_events_per_second) and reported the pass/fail of interval_events_per_second's
 *     p(50). Those disagree whenever the distribution is skewed — a worst window of 300/s
 *     printed beside PASS because the median was 600 — and window_events_per_second's own
 *     registered `min>=` threshold appeared in the artifact nowhere at all, despite being one of
 *     the thresholds k6's exit code depends on.
 *   * `verdicts_all_hold` COULD NEVER BE TRUE. Every row was treated as certifying, and a row
 *     with no registered threshold fails `allThresholdsOk` by construction. With the sampler on,
 *     the whole-run mean row has no thresholds (it is a diagnostic then, by design); with the
 *     sampler off, the sustained row has a null value. Either way one row could not hold, so the
 *     headline "ALL CRITERIA" line read FAIL on every run ever made — including runs k6 itself
 *     exited 0 on. A verdict summary that cannot say PASS is not a conservative summary, it is
 *     an unread one.
 *
 * Both follow from the same root cause: two independent descriptions of one threshold graph. So
 * this reads the graph — the same verdictThresholds() that builds `options` — rather than
 * restating it. A row is CERTIFYING when at least one of the metrics it is certified by has a
 * registered threshold; otherwise it is a diagnostic, and a diagnostic reports null rather than
 * a false that would be indistinguishable from a real failure.
 *
 * EVERY threshold on EVERY metric the row is certified by is merged in, not just one of them. A
 * row reports ONE measured figure but may be gated by several thresholds, and the aggregate is
 * only sound if all of them are reflected somewhere. The windowed sustained verdict is the case
 * in point: verdictThresholds registers three for it — `min>=target` on the window trend,
 * `p(50)>=target` on the interval trend and `value>=1` on the counted-window gauge, the last
 * being its fail-closed guard — and the row used to report only the interval one. A sampler that
 * read /metrics for the first window and then failed for the rest would fail its own thresholds,
 * and k6's exit code, while `verdicts_all_hold` stayed true.
 *
 * @param {object|null} metrics k6's metrics map from the summary.
 * @param {string[]} names the metrics this row is certified by.
 * @returns {{certifying: boolean, thresholds: object|null}} the row's registered family.
 */
function verdictThresholdsFor(metrics, names) {
  var registered = verdictThresholds();
  var out = {};
  var certifying = false;

  for (var i = 0; i < names.length; i++) {
    if (!Object.prototype.hasOwnProperty.call(registered, names[i])) {
      continue;
    }
    certifying = true;

    var observed = thresholdVerdicts(metrics, names[i]);
    if (observed) {
      out = merge(out, observed);
    }
  }

  return { certifying: certifying, thresholds: certifying ? out : null };
}

// describeCode looks a numeric code up in its vocabulary.
function describeCode(vocabulary, code) {
  if (code === null || code === undefined) {
    return "not recorded";
  }
  if (Object.prototype.hasOwnProperty.call(vocabulary, code)) {
    return vocabulary[code];
  }

  return "unrecognised code " + code;
}

// resolvedSeriesName reconstructs the exposition name a spelling code stands for.
function resolvedSeriesName(candidates, code) {
  if (code === SERIES_CODE_DOCUMENTED_NAME) {
    return candidates[0];
  }
  if (code === SERIES_CODE_TOLERATED_NAME) {
    return candidates[1];
  }

  return null;
}

/**
 * buildProvenance assembles the audit record that accompanies the three verdicts.
 *
 * The point of this block is that a reviewer opening summary-event-streaming.json can
 * confirm, without reading this file, that the p99 came from the event pipeline's own
 * histogram and not from k6's HTTP timings, and that the throughput came from the PER-EVENT
 * dispatched counter — not from k6's request count, and not from the broker write counter
 * whose redeliveries would have inflated it. Each verdict therefore names the series it was
 * computed from, the PromQL it is equivalent to, and — for the latency — the bucket the rank
 * landed in and how it was interpolated.
 *
 * @param {object} metrics the summary's metrics map.
 * @returns {object} the provenance block.
 */
function buildProvenance(metrics) {
  var sourceCode = gaugeValue(metrics, M_P99_SOURCE_CODE);
  var spellingCode = gaugeValue(metrics, M_P99_SERIES_SPELLING_CODE);
  // The verdict's latency can only have come from capture-to-dispatch, so the series named
  // here is that one unconditionally. sourceCode is still read, and still reported, because
  // P99_SOURCE_NONE is a meaningful state: it says the histogram was absent and no figure
  // was certified.
  var latencyBase = resolvedSeriesName(
    SERIES_CAPTURE_TO_DISPATCH,
    spellingCode,
  );
  var bucketUpper = gaugeValue(metrics, M_P99_BUCKET_UPPER);
  var reason = gaugeValue(metrics, M_DEGRADED_REASON_CODE);
  var available = gaugeValue(metrics, M_VERDICTS_AVAILABLE);
  // THE OBSERVED HALF of the single-process requirement (PERF-M08). The sampler recorded how
  // often a reading came from a different process than the one before it; any change at all means
  // the per-interval deltas it produced span unrelated counters. Folded into availability here,
  // rather than in teardown, because a VU's module state is not visible to teardown — so this
  // gauge is the only channel the observation has.
  //
  // Downgraded to unavailable rather than merely annotated, for the reason every other guard in
  // this file is: a marker beside a published verdict does not stop the verdict being read.
  var samplerIdentityChangesObserved = gaugeValue(
    metrics,
    M_SAMPLER_IDENTITY_CHANGES,
  );

  // THE SUSTAINED ROW'S VERDICT FIGURE, computed once (PERF-M09).
  //
  // This row is a FRACTION of intervals rather than a single measurement against a bound, so it
  // never carried a `value` — and verdictHolds requires one, for a good reason: an unrecorded
  // Gauge reads as 0 in a k6 summary and 0 satisfies both `< ceiling` verdicts, so a figure that
  // is absent must not be treated as a figure that passed.
  //
  // The consequence was a THIRD structural reason verdicts_all_hold could never be true, on top
  // of the two the certifying/diagnostic split fixes: this row's thresholds could BOTH pass —
  // observed live at `rate>=0.95: true, count>=3: true` — and it still reported FAIL, because it
  // had no value to be non-null. So the aggregate stayed false with every threshold in the run
  // green.
  //
  // The fraction is the right figure: it is null exactly when NO subwindow was judged, which is
  // the unmeasured case the value check exists to catch, and a number whenever one was. Computed
  // here so `value` and `fraction_meeting_target` are the same expression rather than two.
  var qualifyingSubwindows = counterTotal(metrics, M_SUBWINDOWS_QUALIFYING);
  var subwindowFraction =
    qualifyingSubwindows > 0 ? rateValue(metrics, M_SUBWINDOW_MET_TARGET) : null;
  if (samplerIdentityChangesObserved !== null && samplerIdentityChangesObserved > 0) {
    available = 0;
  }

  var provenance = {
    generated_by: "tests/loadtest/events.js",
    scenario: SCENARIO,
    acceptance_criteria: {
      "V-1":
        "500 events/sec sustained, p99 outbox-to-Kafka publish latency under 2s for non-retried events",
      "V-3":
        "under 0.1% of events reach a dead-letter topic over 30 minutes at 500 events/sec",
    },
    measurement_source:
      "the blnk server's /metrics Prometheus exposition, on an instance dedicated to this run: a baseline scrape taken once every row whose Kafka outcome was still owed had settled, per-interval scrapes taken throughout the run by a dedicated sampler scenario, and a final scrape taken only once the same population had settled again",
    verdict_inputs_available: available === 1,
    // THE MEASUREMENT SCOPE (PERF-M08), so a reader of an archived summary can see what the
    // numbers describe rather than having to assume a single process.
    measurement_scope: {
      declared_server_replicas: SERVER_REPLICAS,
      sampler_process_identity_changes: samplerIdentityChangesObserved,
      single_process_required:
        "every verdict is a delta of a PER-PROCESS counter read from one URL, so the endpoint must front one process. Behind a Service or any load balancer consecutive scrapes reach arbitrary backends and the delta mixes counters that never formed a series — which looks like a throughput dip or a burst, never like an error. Declare SERVER_REPLICAS when the endpoint fronts more than one, and read the verdicts from Prometheus using each one's equivalent_promql instead",
      sampler_identity_note:
        "sampler_process_identity_changes counts sampler readings that came from a different process than the reading before them. Any non-zero value withholds the verdicts: it is direct evidence that the endpoint is not one process, whatever SERVER_REPLICAS declared",
    },
    verdict_inputs_condition: describeCode(REASON_LABELS, reason),
    verdicts: {
      sustained_throughput_subwindows: {
        metric: M_SUBWINDOW_MET_TARGET,
        subwindow_seconds: SUBWINDOW_SECONDS,
        qualifying_subwindows: qualifyingSubwindows,
        // The verdict figure for this row. See subwindowFraction above: without it the row could
        // not hold however green its thresholds were.
        value: subwindowFraction,
        observed_subwindows: counterTotal(metrics, M_SUBWINDOWS_OBSERVED),
        counter_resets_observed: counterTotal(metrics, M_SUBWINDOW_RESETS),
        // Null when NO qualifying subwindow was judged. k6 reports a Rate with zero
        // observations as 0.00%, which is indistinguishable from "every subwindow missed the
        // target" — a measurement — so the count decides which of the two this is.
        fraction_meeting_target: subwindowFraction,
        required_fraction: MIN_SUSTAINED_SUBWINDOW_RATIO,
        required_qualifying_subwindows: MIN_QUALIFYING_SUBWINDOWS,
        ramp_exclusion_seconds: RAMP_EXCLUSION_SECONDS,
        worst_subwindow_events_per_second: trendStat(
          metrics,
          M_SUBWINDOW_EVENTS_PER_SEC,
          "min",
        ),
        median_subwindow_events_per_second: trendStat(
          metrics,
          M_SUBWINDOW_EVENTS_PER_SEC,
          "med",
        ),
        method:
          "the published counter is read every subwindow_seconds by a single-VU sampler, and each interval between readings is judged on its own. A subwindow qualifies only when it lies wholly inside the steady-state region, ramp_exclusion_seconds in from each end of the load. THIS, not the whole-window average below, is what decides whether the target was SUSTAINED: an average cannot distinguish 500/sec throughout from nothing for half the run and 1000/sec for the other half",
        equivalent_promql: PROMQL_SUSTAINED,
        // The registered family, resolved from verdictThresholds() rather than restated here
        // (PERF-M09). Both metrics belong to this row: the ratio is the tolerance and the count
        // is its fail-closed guard, and neither certifies sustained throughput alone.
        certified_by: [M_SUBWINDOW_MET_TARGET, M_SUBWINDOWS_QUALIFYING],
        note: "the qualifying-subwindow count is the fail-closed guard: a rate>= threshold on an EMPTY k6 Rate passes vacuously, so without a count>= threshold beside it a run whose sampler never took a reading would certify sustained throughput",
      },
      throughput_events_per_second: {
        metric: M_EVENTS_PER_SEC,
        // WHETHER THIS ENTRY IS A VERDICT AT ALL, stated on exactly the condition
        // verdictThresholds() gates its threshold registration on. With the sampler running
        // this figure is the diagnostic mean beside the windowed verdict and carries no
        // threshold, so it is not scored; with the sampler off it is the only throughput figure
        // the run produced and it is.
        //
        // It has to be explicit rather than inferred from `thresholds === null`, or a threshold
        // registration accidentally dropped from verdictThresholds() would silently demote a
        // real verdict to "not applicable" and the aggregate would stay true without it. Stated
        // here, a dropped registration leaves this entry enabled with no thresholds, and
        // allThresholdsOk answers false.
        enabled: !RATE_SAMPLER_ENABLED,
        value: gaugeValue(metrics, M_EVENTS_PER_SEC),
        target: TARGET_EVENTS_PER_SEC,
        comparison: ">=",
        // SERIES_PUBLISHED, because that is the counter the value beside it was differenced
        // from: M_EVENTS_PER_SEC is publishedEvents / LOAD_SECONDS and is recorded only when
        // the PUBLISHED series resolved. It used to report the DISPATCHED series here, which
        // names a counter no part of this figure reads and contradicted the equivalent_promql
        // in this very block — the one defect a provenance field exists to make impossible,
        // and in the flattering direction too, since dispatched trails published by the
        // population still owed a legacy webhook. The dispatched counter's own resolution is
        // still auditable, on the event_publish_dispatched_series_code gauge and the
        // dispatched_delta row under raw_inputs.
        series:
          resolvedSeriesName(
            SERIES_PUBLISHED,
            gaugeValue(metrics, M_PUBLISHED_SERIES_CODE),
          ) || SERIES_PUBLISHED[0],
        series_resolution: describeCode(
          SERIES_CODE_LABELS,
          gaugeValue(metrics, M_PUBLISHED_SERIES_CODE),
        ),
        method:
          "the counter's delta over the measured window, summed over every topic and event_type label, divided by the LOAD INTERVAL rather than by the window. The window is necessarily longer than the load — it opens at the baseline scrape after provisioning and closes after the tail drain — so dividing by it would charge the load for seconds during which none was offered, and would do so in the failing direction",
        divisor_seconds: gaugeValue(metrics, M_LOAD_SECONDS),
        offered_arrival_rate: gaugeValue(metrics, M_OFFERED_RATE),
        offered_headroom_note:
          "the offered rate carries headroom over the target on purpose: at exactly the target a flawless run lands ON the >= boundary and every real effect — a dropped iteration, a rejected transaction, a window longer than the load — pushes below it, with nothing pushing the other way. The verdict is still judged at the target",
        backlog_drained: gaugeValue(metrics, M_BACKLOG_DRAINED) === 1,
        drain_seconds: gaugeValue(metrics, M_DRAIN_SECONDS),
        drain_note:
          "the numerator is only complete because teardown waits for every row whose Kafka outcome is still owed — pending, processing, failed-awaiting-its-dead-letter-write and replaying — to reach the floor before the closing scrape. A drain that does not finish, or a population this run could not read completely, makes every verdict unavailable rather than reporting a short count",
        attribution_note:
          "this delta is process-global. It is attributable to this run only because the run refuses to proceed without a dedicated instance; see supporting_figures.instance_isolation",
        equivalent_promql: PROMQL_THROUGHPUT,
        promql_note:
          "sum(rate(...)), not rate(...). The counter carries topic and event_type, so a bare rate() returns one series per label combination and none of them is this figure",
        // Certifying ONLY when the sampler is off, which is exactly when verdictThresholds()
        // registers this metric — so the two can no longer disagree about whether this row is a
        // verdict or a diagnostic. With the sampler on it reports verdict_holds: null.
        certified_by: [M_EVENTS_PER_SEC],
        role: RATE_SAMPLER_ENABLED
          ? "DIAGNOSTIC. A whole-run mean cannot distinguish sustained load from a burst followed by a stall, so the sustained verdict below decides V-1's throughput and this figure carries no threshold"
          : "VERDICT, by default of the sampler being disabled. This is a whole-run MEAN: a run that managed twice the target for half its duration and nothing for the other half reports the target here. Set RATE_WINDOW_SECONDS to enable the windowed verdict",
      },
      sustained_throughput_events_per_second: {
        metric: M_WINDOW_EVENTS_PER_SEC,
        enabled: RATE_SAMPLER_ENABLED,
        aggregation: "min",
        value: RATE_SAMPLER_ENABLED
          ? trendValue(metrics, M_WINDOW_EVENTS_PER_SEC, "min")
          : null,
        target: TARGET_EVENTS_PER_SEC,
        comparison: ">=",
        window_seconds: RATE_WINDOW_SECONDS,
        warmup_seconds: RATE_WINDOW_WARMUP_SECONDS,
        windows_counted: gaugeValue(metrics, M_RATE_WINDOWS_COUNTED),
        windows_skipped: gaugeValue(metrics, M_RATE_WINDOWS_SKIPPED),
        series:
          resolvedSeriesName(
            SERIES_PUBLISHED,
            gaugeValue(metrics, M_PUBLISHED_SERIES_CODE),
          ) || SERIES_PUBLISHED[0],
        sample_interval_seconds: SAMPLE_INTERVAL_SECONDS,
        samples: trendStat(metrics, M_INTERVAL_EVENTS_PER_SEC, "count"),
        // The shape is reported without being thresholded, so a reader can see a run that only
        // just held the target without a transient dip being allowed to fail the criterion.
        distribution: {
          min: trendStat(metrics, M_INTERVAL_EVENTS_PER_SEC, "min"),
          "p(10)": trendStat(metrics, M_INTERVAL_EVENTS_PER_SEC, "p(10)"),
          "p(90)": trendStat(metrics, M_INTERVAL_EVENTS_PER_SEC, "p(90)"),
          max: trendStat(metrics, M_INTERVAL_EVENTS_PER_SEC, "max"),
        },
        method:
          "the published counter's delta across each " +
          SAMPLE_INTERVAL_SECONDS +
          "-second interval, divided by the interval actually elapsed between the two scrapes. Sampled by a dedicated one-VU scenario DURING the load, because a sustained rate is a statement about the run while it is running and teardown can only ever see totals. The first sample is discarded as the ramp, an unreadable scrape records nothing rather than a zero, and an interval spanning a counter reset is dropped",
        // The SUSTAINED query, not the whole-window one. PROMQL_THROUGHPUT is a five-minute
        // average and this figure is the weakest interval, so quoting it here invited a reader to
        // check a windowed claim against an averaged query and get a different, larger number.
        equivalent_promql: PROMQL_SUSTAINED_THROUGHPUT,
        // ITS OWN FAMILY, and all of it (PERF-M09). This row used to display
        // min(window_events_per_second) while reporting interval_events_per_second's p(50)
        // result — a different statistic of a different metric, so the number shown and the
        // verdict beside it could disagree, and this row's own registered `min>=` threshold was
        // reported nowhere. All three sustained-throughput metrics are named because all three
        // are registered together and all three gate the same claim: the worst window, the
        // median interval, and the fail-closed count of windows actually measured.
        certified_by: [
          M_WINDOW_EVENTS_PER_SEC,
          M_INTERVAL_EVENTS_PER_SEC,
          M_RATE_WINDOWS_COUNTED,
        ],
      },
      publish_latency_p99_seconds: {
        metric: M_P99_SECONDS,
        value: gaugeValue(metrics, M_P99_SECONDS),
        // Whether the figure beside it is a measurement at all.
        //
        // An unrecorded Gauge is reported by k6 as 0, and `0 < 2` satisfies the ceiling — so
        // without this flag a run whose latency was never measured renders as the most
        // emphatic possible pass. It is true only for the one admissible instrument.
        measured: sourceCode === P99_SOURCE_CAPTURE_TO_DISPATCH,
        ceiling: MAX_P99_PUBLISH_SECONDS,
        comparison: "<",
        series: latencySeriesLabel(latencyBase, sourceCode),
        admissible_series: SERIES_CAPTURE_TO_DISPATCH[0],
        series_resolution: describeCode(SERIES_CODE_LABELS, spellingCode),
        instrument: describeCode(P99_SOURCE_LABELS, sourceCode),
        bucket_lower_seconds: gaugeValue(metrics, M_P99_BUCKET_LOWER),
        // -1 is the recorded stand-in for the +Inf boundary, because Infinity serialises to
        // null in JSON and would erase the finding it represents.
        bucket_upper_seconds: bucketUpper === -1 ? "+Inf" : bucketUpper,
        interpolation: describeCode(
          INTERPOLATION_LABELS,
          gaugeValue(metrics, M_P99_INTERPOLATION_CODE),
        ),
        first_attempt_observations: gaugeValue(metrics, M_LATENCY_COUNT_DELTA),
        // The query for the series that ACTUALLY produced the number. Labelling a
        // publish_duration figure with the capture-to-dispatch query is how a shorter interval
        // came to be presented as V-1; this file no longer substitutes, and the provenance
        // no longer asserts a query it did not use.
        equivalent_promql: promqlForLatencySource(sourceCode),
        no_fallback_note:
          "V-1 is derived from capture-to-dispatch and from nothing else. When that series has no first-attempt samples this verdict is UNAVAILABLE; publish_duration is corroboration under supporting_figures, because its clock starts at the relay claim and so measures a strictly shorter interval than the target is stated over",
        certified_by: [M_P99_SECONDS],
        source_policy:
          "read from capture-to-dispatch ONLY. publish_duration measures claim-to-acknowledgement and omits the outbox wait, so it is never substituted here: when capture-to-dispatch yields no first-attempt p99 the verdict inputs are reported UNAVAILABLE and V-1 is not scored. The publish figure is still reported, as broker_write_p99_seconds below",
        note: "2s is an explicit bucket boundary of this histogram, so 'is the p99 under 2s' is answered from bucket counts with no interpolation across the threshold",
      },
      dead_letter_ratio: {
        metric: M_DEAD_LETTER_RATIO,
        value: gaugeValue(metrics, M_DEAD_LETTER_RATIO),
        ceiling: MAX_DEAD_LETTER_RATIO,
        comparison: "<",
        numerator_series:
          resolvedSeriesName(
            SERIES_DEAD_LETTERED,
            gaugeValue(metrics, M_DEAD_LETTERED_SERIES_CODE),
          ) || SERIES_DEAD_LETTERED[0],
        series_resolution: describeCode(
          SERIES_CODE_LABELS,
          gaugeValue(metrics, M_DEAD_LETTERED_SERIES_CODE),
        ),
        method:
          "dead_lettered / (dead_lettered + published): the two counters partition a captured event's terminal outcomes, so their sum is the population. Dividing by published alone would overstate the rate and diverge without bound as failures rose",
        numerator_source: describeCode(
          DEAD_LETTER_SOURCE_LABELS,
          gaugeValue(metrics, M_DEAD_LETTER_SOURCE),
        ),
        numerator_source_note:
          "an ABSENT dead-letter counter is not a zero. Coercing absence to 0 yields a 0% rate, which clears this ceiling while measuring nothing, so a broken exporter would certify the delivery target it had stopped observing. This verdict requires either an explicit series delta or a SAME-WINDOW delta of the authenticated /events/stats census — the source is resolved before the arithmetic and the numerator is taken from whichever one it selected, so the label and the number cannot disagree. When neither yields a window delta the verdict is withheld",
        census_numerator: {
          baseline_dead_lettered: gaugeValue(
            metrics,
            M_STATS_DEAD_LETTERED_START,
          ),
          final_dead_lettered: gaugeValue(metrics, M_STATS_DEAD_LETTERED),
          window_delta: gaugeValue(metrics, M_STATS_DEAD_LETTERED_DELTA),
          note: "the census fallback, and only used when the counter is absent. -1 on any row means it was not read. dead_lettered is a WHOLE-TABLE count and dead-lettered rows are not purged by retention, so the difference of two readings is this window's dead-letter count; a NEGATIVE difference means rows left the state (a replay, or an operator resolving them) and is refused rather than clamped, because it is not a count of anything this window did",
        },
        equivalent_promql: PROMQL_DEAD_LETTER,
        certified_by: [M_DEAD_LETTER_RATIO],
      },
    },
    supporting_figures: {
      broker_write_p99_seconds: {
        metric: M_BROKER_WRITE_P99,
        value: gaugeValue(metrics, M_BROKER_WRITE_P99),
        instrument: describeCode(
          P99_SOURCE_LABELS,
          P99_SOURCE_PUBLISH_DURATION,
        ),
        equivalent_promql: PROMQL_BROKER_WRITE,
        note: "the broker write in relative isolation, reported ALONGSIDE the V-1 p99 and never in place of it. Its clock starts at the outbox claim, so it cannot see relay backlog: an hour-behind relay reports the same figure as an idle one",
      },
      p99_difference_seconds: {
        metric: M_P99_DIFFERENCE,
        value: gaugeValue(metrics, M_P99_DIFFERENCE),
        equivalent_promql: PROMQL_P99_DIFFERENCE,
        note: "the end-to-end p99 minus the broker-write p99. A ROUGH DIAGNOSTIC of how much of the end-to-end figure is something other than the broker write, and NOT a queue-wait p99: the difference of two quantiles is not the quantile of the difference, because each p99 is taken over its own population and the event at the 99th percentile of one is generally not the event at the 99th percentile of the other. A negative reading is possible and means only that the two populations differ, not that time ran backwards",
      },
      outbox_drain: {
        completed: gaugeValue(metrics, M_DRAIN_COMPLETE) === 1,
        waited_seconds: gaugeValue(metrics, M_DRAIN_WAIT_SECONDS),
        polls: gaugeValue(metrics, M_DRAIN_POLLS),
        budget_seconds: POST_DRAIN_BUDGET_SECONDS,
        target_rows: DRAIN_FLOOR,
        backlog_at_final_reading: gaugeValue(metrics, M_OUTBOX_PENDING_END),
        settlement_population: [
          "pending",
          "processing",
          "failed (dead-letter write owed)",
          "replaying",
        ],
        settlement_population_complete:
          gaugeValue(metrics, M_SETTLEMENT_POPULATION_COMPLETE) === 1,
        census_at_final_reading: {
          pending: gaugeValue(metrics, M_STATS_PENDING),
          processing: gaugeValue(metrics, M_STATS_PROCESSING),
          failed_awaiting_dead_letter: gaugeValue(metrics, M_STATS_FAILED),
          replaying: gaugeValue(metrics, M_STATS_REPLAYING),
          unsettled_total: gaugeValue(metrics, M_STATS_UNSETTLED),
          note: "-1 means the figure was not read, which is what a run without a master key records",
        },
        note: "teardown waits for the WHOLE unsettled population — pending, processing, failed-awaiting-its-dead-letter-write and replaying — to reach target_rows before taking the readings the verdicts are computed from, so the tail the relay was still working through when the load stopped is inside the population rather than excluded from it. Excluding it would flatter V-3 in particular, because a not-yet-terminal event is exactly the event that may still become a dead-letter, and a row whose retries are already spent is the likeliest of all. webhook_pending is deliberately not in the population: its Kafka leg is acknowledged and counted, and only the deprecated HTTP leg is owed. The wait is bounded, and it fails closed twice over: when the budget expires, and when the population could not be read completely — blnk_outbox_pending is pending plus processing only, so a run without a master key cannot observe the failed tail and records settlement_population_complete=false. In both cases verdict_inputs_available is false rather than the partial figures being reported as a result",
      },
      instance_isolation: {
        isolated: gaugeValue(metrics, M_INSTANCE_ISOLATED) === 1,
        acknowledged: gaugeValue(metrics, M_ISOLATION_ACKNOWLEDGED) === 1,
        idle_probe_seconds: gaugeValue(metrics, M_ISOLATION_PROBE_SECONDS),
        foreign_events_observed: gaugeValue(
          metrics,
          M_ISOLATION_FOREIGN_EVENTS,
        ),
        tolerated_foreign_events: ISOLATION_MAX_FOREIGN_EVENTS,
        required: REQUIRE_ISOLATION === 1,
        note: "EVERY figure in this document is a delta of PROCESS-GLOBAL counters and a quantile over a process-global histogram. There is no run or workload dimension to filter on, and adding one to the production instruments would mean unbounded label cardinality in the server, so attribution rests on the instance being dedicated to this run. That is asserted with ISOLATED_INSTANCE=1 and checked empirically by an idle probe taken after the pre-load settling gate, with no load offered: any terminal event counted during it belongs to another workload. foreign_events_observed is -1 when the probe was not taken. The probe can DISPROVE isolation and cannot prove it — a bursty producer can be idle for its duration — which is why the acknowledgement is required as well. Shared traffic invalidates certification: it inflates throughput, enlarges the dead-letter denominator and mixes the latency population",
      },
      settlement_gap_factor: {
        metric: M_SETTLEMENT_GAP_FACTOR,
        value: gaugeValue(metrics, M_SETTLEMENT_GAP_FACTOR),
        series:
          resolvedSeriesName(
            SERIES_PUBLISHED,
            gaugeValue(metrics, M_PUBLISHED_SERIES_CODE),
          ) || SERIES_PUBLISHED[0],
        equivalent_promql: PROMQL_SETTLEMENT_GAP,
        note: "events durably delivered to Kafka per event whose outbox row has reached the terminal dispatched state. Both counters are PER-EVENT, so this is not a redelivery factor: above 1.0 is the population whose Kafka leg is durable but whose legacy webhook leg is still owed, and it converges to 1.0 at the sunset when that leg and the webhook_pending state are removed. The real redelivery factor needs the only per-WRITE counter, blnk_events_broker_acknowledgements_total, which this harness does not scrape — its query is quoted as redelivery_factor_promql so an operator can run it. No verdict is computed from either",
        redelivery_factor_promql: PROMQL_REDELIVERY_FACTOR,
      },
    },
    load_shape: {
      partition_keys: gaugeValue(metrics, M_PARTITION_KEYS),
      required_partition_keys: SMOKE ? 0 : requiredSpread(),
      mode: SMOKE ? "smoke — acceptance guards relaxed, numbers not quotable" : "acceptance",
      fixtures: LEDGER_PAIRS_RAW
        ? "pre-provisioned via LEDGER_PAIRS; this run created nothing"
        : ALLOW_FIXTURE_CREATION
          ? "created by this run and PERMANENT: Blnk has no delete endpoint for a ledger or a balance, so ALLOW_FIXTURE_CREATION=1 was required to acknowledge it"
          : "none created",
      note: "the number of independent aggregates the offered load was spread over. The relay's claim returns at most ONE row per partition key per poll, so this is the ceiling on concurrent publishing: throughput comes from distinct keys, not from a larger batch. Zero means the @uuid shorthand fallback was used, whose balances all share one default ledger — such a run measures one aggregate's serialisation ceiling and cannot certify a throughput target",
    },
    raw_inputs: {
      load_seconds: gaugeValue(metrics, M_LOAD_SECONDS),
      offered_rate: gaugeValue(metrics, M_OFFERED_RATE),
      backlog_drained: gaugeValue(metrics, M_BACKLOG_DRAINED),
      drain_seconds: gaugeValue(metrics, M_DRAIN_SECONDS),
      settlement_population_complete: gaugeValue(
        metrics,
        M_SETTLEMENT_POPULATION_COMPLETE,
      ),
      instance_isolated: gaugeValue(metrics, M_INSTANCE_ISOLATED),
      isolation_foreign_events: gaugeValue(metrics, M_ISOLATION_FOREIGN_EVENTS),
      dead_letter_source: gaugeValue(metrics, M_DEAD_LETTER_SOURCE),
      stats_dead_lettered_start: gaugeValue(
        metrics,
        M_STATS_DEAD_LETTERED_START,
      ),
      stats_dead_lettered_delta: gaugeValue(
        metrics,
        M_STATS_DEAD_LETTERED_DELTA,
      ),
      measured_window_seconds: gaugeValue(metrics, M_WINDOW_SECONDS),
      dispatched_start: gaugeValue(metrics, M_DISPATCHED_START),
      dispatched_end: gaugeValue(metrics, M_DISPATCHED_END),
      dispatched_delta: gaugeValue(metrics, M_DISPATCHED_DELTA),
      published_start: gaugeValue(metrics, M_PUBLISHED_START),
      published_end: gaugeValue(metrics, M_PUBLISHED_END),
      published_delta: gaugeValue(metrics, M_PUBLISHED_DELTA),
      dead_lettered_start: gaugeValue(metrics, M_DEAD_LETTERED_START),
      dead_lettered_end: gaugeValue(metrics, M_DEAD_LETTERED_END),
      dead_lettered_delta: gaugeValue(metrics, M_DEAD_LETTERED_DELTA),
      latency_count_start: gaugeValue(metrics, M_LATENCY_COUNT_START),
      latency_count_end: gaugeValue(metrics, M_LATENCY_COUNT_END),
      latency_count_delta: gaugeValue(metrics, M_LATENCY_COUNT_DELTA),
      latency_sum_start: gaugeValue(metrics, M_LATENCY_SUM_START),
      latency_sum_end: gaugeValue(metrics, M_LATENCY_SUM_END),
      latency_sum_delta: gaugeValue(metrics, M_LATENCY_SUM_DELTA),
      publish_attempts_dispatched: gaugeValue(metrics, M_ATTEMPTS_DISPATCHED),
      publish_attempts_retrying: gaugeValue(metrics, M_ATTEMPTS_RETRYING),
      publish_attempts_terminal: gaugeValue(metrics, M_ATTEMPTS_TERMINAL),
      publish_attempts_dead_lettered: gaugeValue(
        metrics,
        M_ATTEMPTS_DEAD_LETTERED,
      ),
      outbox_pending_start: gaugeValue(metrics, M_OUTBOX_PENDING_START),
      outbox_pending_end: gaugeValue(metrics, M_OUTBOX_PENDING_END),
      counter_reset_detected: gaugeValue(metrics, M_COUNTER_RESET) === 1,
      // PERF-P03. The provenance of the continuity decision, so a reader can see WHY a run
      // was accepted or degraded rather than trusting the flag. -1 on either instant means the
      // exporter published no process_start_time_seconds for that scrape, and continuity is
      // then UNKNOWN rather than intact — which fails the run, because assuming continuity is
      // how an undetected restart certifies a target.
      exporter_restarted: gaugeValue(metrics, M_EXPORTER_RESTARTED) === 1,
      process_start_before: gaugeValue(metrics, M_PROCESS_START_BEFORE),
      process_start_after: gaugeValue(metrics, M_PROCESS_START_AFTER),
      continuity_method:
        "process_start_time_seconds compared across the two scrapes. A restart resets every counter, so a delta across one is arithmetic over two populations; comparing the counters alone (end < start) detects that only until the replacement counts past the old total, which at the target rate is seconds",
      note: "latency_sum and latency_count are recorded for auditing only. Their ratio is a MEAN and is never used as the p99: substituting it would report a comfortably passing figure for a distribution with a long tail",
    },
    conditions: {
      baseline_scrape_http_status: gaugeValue(metrics, M_METRICS_STATUS_START),
      final_scrape_http_status: gaugeValue(metrics, M_METRICS_STATUS_END),
      status_zero_means: "the request never reached a listener",
      backlog_settled: gaugeValue(metrics, M_BACKLOG_SETTLED) === 1,
      baseline_settle_seconds: gaugeValue(metrics, M_BASELINE_SETTLE_SECONDS),
      final_settle_seconds: gaugeValue(metrics, M_FINAL_SETTLE_SECONDS),
      outbox_pending_at_final_settle: gaugeValue(
        metrics,
        M_BACKLOG_PENDING_AT_SETTLE,
      ),
      pre_drain_budget_seconds: PRE_DRAIN_BUDGET_SECONDS,
      post_drain_budget_seconds: POST_DRAIN_BUDGET_SECONDS,
      backlog_settled_means:
        "the outbox held no rows whose KAFKA OUTCOME WAS STILL OWED at BOTH ends before the scrape was taken — pending, processing, failed-awaiting-its-dead-letter-write and replaying, all at or below the floor over the required number of fresh readings. Scraping the instant the load stops leaves the run's last events unpublished — absent from the counters, and absent from the latency histogram precisely because they are the slowest — which biases the p99 optimistically and understates the dead-letter rate. -1 for a settle-seconds row means it was not recorded; -1 for a depth means it was unreadable",
      events_stats_http_status: gaugeValue(metrics, M_STATS_STATUS),
      events_stats_dispatched: gaugeValue(metrics, M_STATS_DISPATCHED),
      events_stats_dead_lettered: gaugeValue(metrics, M_STATS_DEAD_LETTERED),
      events_stats_pending: gaugeValue(metrics, M_STATS_PENDING),
      events_stats_processing: gaugeValue(metrics, M_STATS_PROCESSING),
      events_stats_failed: gaugeValue(metrics, M_STATS_FAILED),
      events_stats_replaying: gaugeValue(metrics, M_STATS_REPLAYING),
      events_stats_unsettled: gaugeValue(metrics, M_STATS_UNSETTLED),
      events_stats_note:
        "the master-key-gated census, attempted solely when a master key is supplied; -1 means the figure was not read. The dispatched and dead-lettered rows are CORROBORATION and no verdict rests on them — but the four unsettled rows are NOT corroboration: their total is what the settling gates require to be quiet, and without them the gates cannot establish quiescence and every verdict is withheld. It is also V-3's numerator source when the dead-letter counter is absent, as a same-window delta events_stats_dispatched is EXPECTED to be -1: the probe sends include_offsets=false so a once-a-second drain poll cannot trigger a broker round trip and an exact count of the dispatched history, and under that posture the endpoint omits the dispatched key and reports dispatched_history_counted:false. -1 here therefore means NOT COUNTED, never zero",
      // The settling gates. These are NOT corroboration: an unsettled gate invalidates the
      // window and drops verdict_inputs_available to false.
      pre_load_settling_gate_held:
        gaugeValue(metrics, M_PRE_DRAIN_SETTLED) === DRAIN_SETTLED,
      pre_load_settling_seconds: gaugeValue(metrics, M_PRE_DRAIN_SECONDS),
      pre_load_pending_at_gate_exit: gaugeValue(metrics, M_PRE_DRAIN_PENDING),
      post_load_settling_gate_held:
        gaugeValue(metrics, M_POST_DRAIN_SETTLED) === DRAIN_SETTLED,
      post_load_settling_seconds: gaugeValue(metrics, M_POST_DRAIN_SECONDS),
      post_load_pending_at_gate_exit: gaugeValue(metrics, M_POST_DRAIN_PENDING),
      settling_depth_source: describeCode(
        DRAIN_SOURCE_LABELS,
        gaugeValue(metrics, M_DRAIN_SOURCE_CODE),
      ),
      settling_floor: DRAIN_FLOOR,
      settling_depth_counts:
        "the depth both gates compare against the floor is the UNSETTLED total — pending plus processing plus webhook_pending plus failed plus replaying — and not `pending` alone. Only dispatched and dead_lettered are terminal, so a row in any other state is work still owed. `failed` matters most: it has not yet incremented blnk_events_dead_lettered_total, so closing the window while such rows remain removes them from V-3's numerator and understates the dead-letter rate. On the gauge fallback the total is a LOWER BOUND, because `replaying` has no gauge; depth_complete says which of the two a run got",
      settling_stable_samples_required: DRAIN_STABLE_SAMPLES,
      settling_population: [
        "pending",
        "processing",
        "failed (dead-letter write owed)",
        "replaying",
      ],
      settling_population_complete:
        gaugeValue(metrics, M_SETTLEMENT_POPULATION_COMPLETE) === 1,
      settling_note:
        "the outbox is asynchronous at both ends of the window. Without the pre-load gate, provisioning's ledger.created and balance.created events would still be pending when the baseline is taken and would be published inside the window, crediting the load with work it never offered. Without the post-load gate, the load's own pending, mid-retry and failed-awaiting-dead-letter events would be excluded — which understates throughput and understates the DEAD-LETTER RATIO, because an event still working through its attempts, or already past them, is the one most likely to be dead-lettered. BOTH gate exits above carry the TOTAL of the four settling_population states, not the pending literal, and both require the same number of fresh confirmations at or below settling_floor. settling_population_complete is false when a gate could only read part of that population — blnk_outbox_pending is pending plus processing only — and a false reading withholds every verdict rather than certifying a window whose tail was never observed. -1 means the depth was never read",
      instance_isolated: gaugeValue(metrics, M_INSTANCE_ISOLATED) === 1,
      isolation_acknowledged:
        gaugeValue(metrics, M_ISOLATION_ACKNOWLEDGED) === 1,
      isolation_foreign_events: gaugeValue(
        metrics,
        M_ISOLATION_FOREIGN_EVENTS,
      ),
      isolation_note:
        "attribution, and it is not corroboration either: an unisolated population drops verdict_inputs_available to false under REQUIRE_ISOLATION. See supporting_figures.instance_isolation for what the probe does and does not prove",
      counter_reset_invalidates_the_window:
        "a monotonic counter going backwards means the exporting process restarted, so the baseline belongs to a process that no longer exists and no delta across the window is meaningful. It is not annotated and measured through: verdict_inputs_available goes to false and no verdict gauge is written",
    },
    verdicts_all_hold: null,
    deliberately_not_used_for_any_verdict: [
      "http_req_duration — API ACCEPTANCE latency. V-1 is about outbox-to-Kafka publish time, a different interval; reading this would certify a target the pipeline was missing",
      "http_reqs — REQUEST count and rate, which is the OFFERED pressure and not the measured result. One POST /transactions produces exactly one transaction event, so this is not a fan-out argument: what breaks the equality is that a dropped iteration produces no event at all, a rejected transaction produces a different one, and provisioning's events belong to no request in the window",
      "blnk_events_broker_acknowledgements_total — broker WRITES, not events. It is incremented on the acknowledgement, before the outbox row records the leg as durable, so a redelivery increments it twice for one event: it over-reports throughput and, as the dead-letter denominator, deflates the rate — both worst when the pipeline is least healthy. This harness does not scrape it at all; its query is quoted under supporting_figures so the redelivery factor stays reachable",
      "blnk_events_dispatched_total — per-event and unique, but counted at the TERMINAL state, so a row still owing a legacy webhook is not counted until that leg settles. Reading throughput from it understates the rate by the population in webhook_pending at the closing scrape, which is largest exactly while the dual-delivery window is open. Reported as settlement_gap_factor under supporting_figures instead",
      "blnk_events_publish_duration_seconds _sum / _count — a mean, not a quantile",
      "blnk_events_publish_duration_seconds as the V-1 latency — its clock starts at the relay CLAIM, so it cannot see the outbox backlog that V-1 is stated over. Reported as a supporting figure, never substituted for the verdict, and its absence never satisfies the verdict either",
      "capture_to_dispatch p99 MINUS publish_duration p99 as a queue wait — quantiles are not subtractive, and the difference of two p99s over different populations has no interpretation as a latency and can be negative",
      "the CUMULATIVE dead_lettered count from GET /events/stats as V-3's numerator — it counts every dead letter the deployment has ever accumulated, and dead-lettered rows are not purged by retention, so it is a lifetime total and not a window delta. Only the difference between the baseline and final census readings is used, and a negative difference is refused rather than clamped",
      "the pending count alone as the settling population — processing, failed-awaiting-its-dead-letter-write and replaying are un-settled Kafka work too, and the failed tail is enriched in the very outcome V-3 measures, so a gate over pending alone closes the window on the events most likely to be dead-lettered",
      "the whole-window average as evidence of SUSTAINED throughput — it is reported, and it is a verdict in its own right, but the sustained claim is decided by the per-subwindow family",
    ],
  };

  // Annotated after the fact so the JSON and the terminal block cannot disagree: both read
  // `verdict_holds`, computed once, rather than each applying its own idea of what a pass is.
  //
  // EACH ROW IS RESOLVED AGAINST THE REGISTERED THRESHOLD GRAPH (PERF-M09). Every row declares
  // the metrics it is `certified_by`; verdictThresholdsFor looks each one up in the same
  // verdictThresholds() that built `options`, so a row can neither report a family that was
  // never registered nor omit one that was.
  //
  // A row with no registered threshold is a DIAGNOSTIC for this configuration, not a failure.
  // The whole-run mean is the standing example: it carries the throughput verdict when the
  // sampler is off and is a diagnostic average when the sampler is on, and which of the two it
  // is was previously decided in one place and read in another. Diagnostics report
  // `verdict_holds: null` — distinguishable from false, which means measured and did not hold —
  // and are excluded from the aggregate.
  //
  // `verdicts_all_hold` is the single line to read: true only when the inputs were available AND
  // every CERTIFYING row holds. It also requires at least one certifying row, so a configuration
  // that somehow registered nothing cannot certify by vacuous agreement.
  var allHold = available === 1;
  var certifyingRows = 0;
  for (var key in provenance.verdicts) {
    if (!Object.prototype.hasOwnProperty.call(provenance.verdicts, key)) {
      continue;
    }

    var entry = provenance.verdicts[key];
    var family = verdictThresholdsFor(metrics, entry.certified_by || []);
    entry.thresholds = family.thresholds;
    entry.certifying = family.certifying;

    if (!family.certifying) {
      // Reported and not judged: this row is a diagnostic in this configuration.
      entry.verdict_holds = null;
      continue;
    }

    certifyingRows += 1;
    var holds = verdictHolds(entry, available === 1);
    entry.verdict_holds = holds;
    allHold = allHold && holds;
  }
  provenance.verdicts_all_hold = certifyingRows > 0 ? allHold : false;
  provenance.certifying_verdicts = certifyingRows;

  return provenance;
}

/**
 * renderVerdictBlock draws the three verdicts as a short text block, so that the outcome of a
 * thirty-minute run is the last thing on the terminal rather than something to be looked up.
 *
 * Each line names the series its number came from, because "p99 1.4s PASS" beside a k6
 * summary full of `http_req_duration` percentiles is exactly the ambiguity that lets the
 * wrong figure be read as the verdict.
 *
 * @param {object} provenance the block buildProvenance produced.
 * @returns {string} the rendered block.
 */
function renderVerdictBlock(provenance) {
  var v = provenance.verdicts;
  var sustained = v.sustained_throughput_events_per_second;
  var lines = [];
  var rows = [];

  // The sustained figure is the throughput VERDICT when the sampler ran, and the whole-run
  // mean is the diagnostic beside it. With the sampler off the mean is all there is, and the
  // row says so rather than implying a windowed measurement was taken.
  if (sustained && sustained.enabled) {
    rows.push({
      title: "V-1 sustained   ",
      entry: sustained,
      bound: ">= " + sustained.target + " events/sec, weakest of " +
        round(sustained.windows_counted, 0) + " windows",
      places: 2,
    });
    rows.push({
      title: "    mean rate   ",
      entry: v.throughput_events_per_second,
      bound: "diagnostic, not a verdict",
      places: 2,
    });
  } else {
    rows.push({
      title: "V-1 throughput  ",
      entry: v.throughput_events_per_second,
      bound: ">= " + v.throughput_events_per_second.target +
        " events/sec, whole-run mean only",
      places: 2,
    });
    rows.push({
      title: "V-1 sustained   ",
      entry: v.sustained_throughput_events_per_second,
      bound:
        "p(50) >= " +
        v.sustained_throughput_events_per_second.target +
        " events/sec per " +
        v.sustained_throughput_events_per_second.sample_interval_seconds +
        "s",
      places: 2,
    });
  }

  // OUTSIDE THE BRANCH, because both of these are reported whichever throughput shape was
  // used. They used to sit inside the `else` arm, which meant a run WITH the sampler enabled
  // — the acceptance configuration — rendered a block with no p99 row and no dead-letter row
  // at all, while the JSON summary still carried both. The array literal that arm was written
  // as also closed with `]` against a `rows.push(` opener, so the file did not parse: it is
  // the shape of two generations of this block fused at the seam, one that built `rows` as a
  // literal and one that pushes into it.
  rows.push(
    {
      title: "V-1 p99 publish ",
      entry: v.publish_latency_p99_seconds,
      bound: "< " + v.publish_latency_p99_seconds.ceiling + "s",
      places: 4,
    },
    {
      title: "V-3 dead-letter ",
      entry: v.dead_letter_ratio,
      bound: "< " + v.dead_letter_ratio.ceiling,
      places: 6,
    },
  );

  lines.push("");
  lines.push(
    "  event-streaming verdicts, read from " + provenance.measurement_source,
  );
  // A row is PASS only when its threshold passed AND the inputs behind it were available.
  // An unwritten Gauge reads as 0 in the summary and 0 satisfies both `< 2s` and `< 0.001`,
  // so a run that measured nothing at all would otherwise render two PASS rows above a
  // failing inputs row — and a reader who takes the row at face value has been told the
  // opposite of the truth. The threshold results are what set the exit code; this is what the
  // human reads, and the two must not disagree.
  for (var i = 0; i < rows.length; i++) {
    var entry = rows[i].entry;
    // ONE IDEA OF "PASS", read from `verdict_holds` (PERF-M09). Two more used to live here: an
    // `ok` local recomputing availability AND allThresholdsOk, and a `mark` local blanking the
    // no-threshold case — both computed on every row and then DISCARDED, because the line below
    // already rendered verdict_holds. Three definitions of a pass in one loop is how the JSON
    // and the terminal came to disagree in the first place, so the two dead ones are gone rather
    // than reconciled. buildProvenance computes verdict_holds once, against the registered
    // threshold graph, and includes the availability precondition this comment used to restate:
    // every verdict is a comparison against a Gauge, an unrecorded Gauge reads as 0, and 0
    // satisfies both `< ceiling` bounds — so a run that measured nothing must not render passes.
    lines.push(
      "   " +
        rows[i].title +
        verdictMark(entry.verdict_holds) +
        "  " +
        (entry.measured === false
          ? "NOT MEASURED (" + entry.admissible_series + " was absent)"
          : round(entry.value, rows[i].places)) +
        "  (" +
        rows[i].bound +
        ")  <- " +
        (entry.measured === false
          ? "no admissible series"
          : entry.series || entry.numerator_series || "series unresolved"),
    );
  }
  // The sustained row is rendered apart from the three above because it is not a single
  // value against a bound — it is a fraction of intervals, and the count behind that fraction
  // is as much of the verdict as the fraction is.
  var sustained = v.sustained_throughput_subwindows;
  lines.push(
    // `verdict_holds` here too, for the reason the loop above gives: this row recomputed the
    // judgement as `inputsOk && allThresholdsOk(...)`, which is a fourth definition of a pass and
    // renders FAIL for a row that is merely a diagnostic in this configuration (PERF-M09).
    "   V-1 sustained   " +
      verdictMark(sustained.verdict_holds) +
      "  " +
      (sustained.fraction_meeting_target === null
        ? "NO QUALIFYING SUBWINDOW WAS JUDGED — " +
          round(sustained.observed_subwindows, 0) +
          " subwindow(s) were measured, none of them wholly inside the steady-state region"
        : round(sustained.fraction_meeting_target * 100, 1) +
          "% of " +
          round(sustained.qualifying_subwindows, 0) +
          " qualifying " +
          sustained.subwindow_seconds +
          "s subwindows reached " +
          v.throughput_events_per_second.target +
          "/sec") +
      "  (>= " +
      round(sustained.required_fraction * 100, 1) +
      "% over >= " +
      sustained.required_qualifying_subwindows +
      ")",
  );
  if (sustained.worst_subwindow_events_per_second !== null) {
    lines.push(
      "     worst subwindow " +
        round(sustained.worst_subwindow_events_per_second, 2) +
        "/sec, median " +
        round(sustained.median_subwindow_events_per_second, 2) +
        "/sec — the worst interval is the figure an average hides",
    );
  }

  var conditions = provenance.conditions || {};
  lines.push(
    "   settling gates  " +
      verdictMark(
        conditions.pre_load_settling_gate_held === true &&
          conditions.post_load_settling_gate_held === true &&
          conditions.settling_population_complete === true,
      ) +
      "  pre-load " +
      (conditions.pre_load_settling_gate_held ? "settled" : "DID NOT SETTLE") +
      " in " +
      round(conditions.pre_load_settling_seconds, 1) +
      "s, post-load " +
      (conditions.post_load_settling_gate_held ? "settled" : "DID NOT SETTLE") +
      " in " +
      round(conditions.post_load_settling_seconds, 1) +
      "s" +
      (conditions.settling_population_complete === true
        ? " over pending+processing+failed+replaying"
        : " — POPULATION INCOMPLETE, the failed-awaiting-dead-letter tail was not observable"),
  );
  // ATTRIBUTION, as its own row. It is the question "are these numbers this run's work", and it
  // cannot be inferred from any row above: a perfectly settled window over a shared instance
  // reads as flawless while measuring somebody else's traffic too.
  var isolation = (provenance.supporting_figures || {}).instance_isolation || {};
  lines.push(
    "   attribution     " +
      verdictMark(isolation.isolated === true) +
      "  " +
      (isolation.isolated === true
        ? "dedicated instance: acknowledged, and the idle probe saw no foreign events"
        : "NOT ISOLATED — the counters are process-global, so any concurrent traffic is counted as this run's work (acknowledged=" +
          (isolation.acknowledged === true ? "yes" : "no") +
          ", foreign events observed=" +
          round(isolation.foreign_events_observed, 0) +
          ", -1 means the probe was not taken)"),
  );
  lines.push(
    "   verdict inputs  " +
      verdictMark(provenance.verdict_inputs_available) +
      "  " +
      provenance.verdict_inputs_condition,
  );
  // The bottom line, so that "did this run certify V-1 and V-3" is one row rather than an
  // inference across five. It is false whenever any figure was unreadable or any input missing,
  // which is the only reading that cannot be mistaken for a pass.
  lines.push(
    "   ALL CRITERIA    " +
      verdictMark(provenance.verdicts_all_hold) +
      "  " +
      (provenance.verdicts_all_hold
        ? "V-1 and V-3 certified by this run"
        : "NOT certified by this run"),
  );
  lines.push("");

  return lines.join("\n");
}

/**
 * verdictHolds reports whether a verdict entry may be presented as met.
 *
 * Three conditions, and none of them is redundant.
 *
 * A passing threshold alone is not enough, twice over. First, the threshold is evaluated by k6
 * against its own sink while the number shown beside it is read out of the summary, and the two
 * disagree when the statistic is absent from `summaryTrendStats` — a combination that once
 * rendered "PASS  n/a", a criterion asserted as met with no visible evidence. Second, and worse,
 * an UNRECORDED Gauge reads as 0 in the summary, and 0 passes both of the `value<x` thresholds
 * here: on a run whose inputs were missing, the p99 and the dead-letter rate both reported 0 and
 * both passed. So availability is not merely an aggregate concern to be checked once at the
 * bottom; it is a precondition of each individual figure meaning anything at all.
 *
 * @param {object|null} entry a verdict entry from buildProvenance.
 * @param {boolean} inputsAvailable whether the run's verdict inputs were all read.
 * @returns {boolean} true only when the inputs were available, every threshold passed, and the
 *   figure was readable.
 */
function verdictHolds(entry, inputsAvailable) {
  if (!verdictApplies(entry) || inputsAvailable !== true) {
    return false;
  }

  // `measured: false` is the p99 entry's statement that the figure beside it came from no
  // admissible instrument. The rendered block has always refused a PASS on it; this did not,
  // so the JSON and the terminal block could disagree about the same entry — the one defect
  // the whole provenance apparatus exists to make impossible.
  if (entry.measured === false) {
    return false;
  }

  var figure = verdictFigure(entry);

  return (
    allThresholdsOk(entry.thresholds) && figure !== null && figure !== undefined
  );
}

/**
 * verdictApplies reports whether a verdict entry is one this run's configuration can answer.
 *
 * Only an entry that carries `enabled` can answer no, and exactly one does:
 * sustained_throughput_events_per_second is the WINDOWED throughput figure, and it is not
 * registered at all on a run too short for the warm-up plus two window widths to elapse. On
 * such a run the whole-run mean is the throughput verdict — which is what the rendered block
 * already says — and the windowed entry measures nothing.
 *
 * Scoring it false was therefore reporting a criterion as MISSED by a run that never offered
 * to measure it, and because the aggregate is a conjunction over every entry, that false
 * propagated: `verdicts_all_hold` could not be true for any run with the sampler off. An entry
 * that does not apply is excluded from the conjunction instead, and is rendered "n/a".
 *
 * @param {object|null} entry a verdict entry from buildProvenance.
 * @returns {boolean} false when the entry is absent or explicitly disabled for this run.
 */
function verdictApplies(entry) {
  return !!entry && entry.enabled !== false;
}

/**
 * verdictFigure resolves the MEASURED FIGURE a verdict entry was judged on.
 *
 * Two shapes exist and both are legitimate. Most entries carry a scalar `value` compared
 * against a `target` or a `ceiling`. The sustained-subwindow entry instead carries
 * `fraction_meeting_target` — what proportion of qualifying subwindows reached the target —
 * beside `required_fraction`, because "was the target SUSTAINED" is a question about a
 * population of intervals rather than about one number.
 *
 * ONLY `value` USED TO BE RECOGNISED, and the consequence was total rather than partial:
 * verdictHolds requires a non-null figure, the subwindow entry has no `value` at all, and the
 * aggregate is a conjunction — so `verdicts_all_hold` and the ALL CRITERIA row were false on
 * every run this file has ever produced, including a perfect one. A thirty-minute acceptance
 * run could not certify V-1 and V-3 no matter what the pipeline did.
 *
 * The fraction is still allowed to be null, which is what keeps the fail-closed reading: a run
 * in which no qualifying subwindow was judged reports null rather than 0, and null does not
 * hold.
 *
 * @param {object|null} entry a verdict entry from buildProvenance.
 * @returns {number|null|undefined} the figure, or undefined when the entry declares none.
 */
function verdictFigure(entry) {
  if (!entry) {
    return undefined;
  }

  if (entry.value !== undefined) {
    return entry.value;
  }

  // Guarded on required_fraction rather than on the presence of the fraction itself, so that a
  // future entry which happens to report a fraction without being judged on one is not
  // silently promoted into a verdict.
  if (entry.required_fraction !== undefined) {
    return entry.fraction_meeting_target;
  }

  return undefined;
}

/**
 * allThresholdsOk reports whether every threshold on a metric passed.
 *
 * @param {object|null} thresholds expression to pass/fail, as buildProvenance recorded it.
 * @returns {boolean} true when there is at least one threshold and all of them passed.
 */
function allThresholdsOk(thresholds) {
  if (!thresholds) {
    return false;
  }

  var any = false;
  for (var expression in thresholds) {
    if (!Object.prototype.hasOwnProperty.call(thresholds, expression)) {
      continue;
    }
    any = true;
    if (thresholds[expression] !== true) {
      return false;
    }
  }

  return any;
}

// ---------------------------------------------------------------------------------------
// The fixture harness (FIXTURES=1).
//
// Everything below is reachable only in fixture mode. It contacts nothing and asserts on the
// SAME functions the acceptance run uses, in the same runtime, so a regression in the parser,
// a reducer, the quantile interpolation or a verdict rule fails here rather than being
// discovered by a thirty-minute run that quietly reported the wrong number.
//
// Each fixture states its inputs and its EXPECTED answer as data. Nothing is asserted as "not
// null" or "greater than zero": every expectation is the exact value, because an assertion that
// admits a range admits the wrong answer.
// ---------------------------------------------------------------------------------------

// fixtureOutcomes accumulates one record per check, and becomes the fixture run's summary.
var fixtureOutcomes = [];

/**
 * canonicalJSON renders a value with object keys in sorted order.
 *
 * Comparison is by rendered string, so key order would otherwise decide whether two equal
 * objects compare equal — and the objects under test are built by parsers whose key order is
 * insertion order and therefore input-dependent.
 *
 * @param {*} value any JSON-representable value.
 * @returns {string} the canonical rendering.
 */
function canonicalJSON(value) {
  if (value === null || value === undefined) {
    return String(value);
  }
  if (typeof value === "number") {
    // Infinity and NaN are not JSON, and both occur here: +Inf is a bucket boundary and NaN is
    // what an unparseable sample yields.
    return isFinite(value) ? String(value) : String(value);
  }
  if (typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Object.prototype.toString.call(value) === "[object Array]") {
    var parts = [];
    for (var i = 0; i < value.length; i++) {
      parts.push(canonicalJSON(value[i]));
    }

    return "[" + parts.join(",") + "]";
  }

  var keys = [];
  for (var key in value) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      keys.push(key);
    }
  }
  keys.sort();

  var fields = [];
  for (var k = 0; k < keys.length; k++) {
    fields.push(
      JSON.stringify(keys[k]) + ":" + canonicalJSON(value[keys[k]]),
    );
  }

  return "{" + fields.join(",") + "}";
}

/**
 * fixtureExpect records one check: an actual value against the exact value expected.
 *
 * FIXTURES_SELFCHECK inverts the FIRST check's expectation, which is what proves the harness
 * is capable of failing. Without it a harness that silently stopped asserting would report the
 * same clean run as one that asserted everything.
 *
 * @param {string} name what is being checked, named so a failure is diagnosable alone.
 * @param {*} actual the value the function under test produced.
 * @param {*} expected the exact value it must produce.
 */
function fixtureExpect(name, actual, expected) {
  var wanted = expected;
  if (FIXTURES_SELFCHECK && fixtureOutcomes.length === 0) {
    wanted = "__fixture selfcheck: this expectation is deliberately wrong__";
  }

  var got = canonicalJSON(actual);
  var want = canonicalJSON(wanted);
  var ok = got === want;

  fixtureOutcomes.push({ check: name, ok: ok, expected: want, actual: got });

  if (fixtureChecks) {
    fixtureChecks.add(1);
  }
  if (!ok) {
    if (fixtureFailures) {
      fixtureFailures.add(1);
    }
    console.error(
      "[fixture] FAIL " + name + "\n    expected " + want + "\n    actual   " + got,
    );
  }
}

/**
 * fixtureGauge builds the summary shape k6 publishes for a Gauge.
 *
 * @param {number} value the recorded value.
 * @param {object|undefined} thresholds expression to ok, as k6 reports them.
 * @returns {object} the metric entry.
 */
function fixtureGauge(value, thresholds) {
  var entry = { type: "gauge", values: { value: value } };
  if (thresholds) {
    entry.thresholds = thresholds;
  }

  return entry;
}

/**
 * fixtureCounter builds the summary shape k6 publishes for a Counter.
 *
 * @param {number} count the accumulated total.
 * @param {object|undefined} thresholds expression to ok.
 * @returns {object} the metric entry.
 */
function fixtureCounter(count, thresholds) {
  var entry = { type: "counter", values: { count: count, rate: count } };
  if (thresholds) {
    entry.thresholds = thresholds;
  }

  return entry;
}

/**
 * fixtureRate builds the summary shape k6 publishes for a Rate.
 *
 * A Rate that received no samples publishes no `rate` key at all, which is the state the
 * qualifying-count guard exists to fail on — pass null for it.
 *
 * @param {number|null} rate the fraction, or null for a Rate with no samples.
 * @param {object|undefined} thresholds expression to ok.
 * @returns {object} the metric entry.
 */
function fixtureRate(rate, thresholds) {
  var values = {};
  if (rate !== null) {
    values.rate = rate;
  }
  var entry = { type: "rate", values: values };
  if (thresholds) {
    entry.thresholds = thresholds;
  }

  return entry;
}

/**
 * fixtureTrend builds the summary shape k6 publishes for a Trend.
 *
 * @param {object} stats aggregation name to value.
 * @param {object|undefined} thresholds expression to ok.
 * @returns {object} the metric entry.
 */
function fixtureTrend(stats, thresholds) {
  var entry = { type: "trend", values: stats };
  if (thresholds) {
    entry.thresholds = thresholds;
  }

  return entry;
}

/**
 * fixturePass renders a threshold map in which every named expression passed.
 *
 * @param {string[]} expressions the threshold expressions.
 * @returns {object} the k6 threshold result map.
 */
function fixturePass(expressions) {
  var out = {};
  for (var i = 0; i < expressions.length; i++) {
    out[expressions[i]] = { ok: true };
  }

  return out;
}

/**
 * fixtureFail renders a threshold map in which every named expression failed.
 *
 * @param {string[]} expressions the threshold expressions.
 * @returns {object} the k6 threshold result map.
 */
function fixtureFail(expressions) {
  var out = {};
  for (var i = 0; i < expressions.length; i++) {
    out[expressions[i]] = { ok: false };
  }

  return out;
}

/**
 * passingMetrics builds the metrics map of a run that met every acceptance criterion.
 *
 * It is the harness's most important fixture, because until this file could produce one there
 * was no way to tell "the run failed" from "the aggregate verdict is unsatisfiable". Every
 * figure is comfortably inside its bound and every threshold this configuration registers is
 * reported as having passed.
 *
 * The thresholds are spelled exactly as verdictThresholds() registers them, from the same
 * constants, so a change to a bound cannot leave the fixture asserting against a stale one.
 *
 * @returns {object} a summary metrics map.
 */
function passingMetrics() {
  var metrics = {};

  metrics[M_VERDICTS_AVAILABLE] = fixtureGauge(
    1,
    fixturePass(["value>=" + REQUIRE_METRICS]),
  );

  // V-1 sustained, the subwindow family: every qualifying subwindow met the target, and there
  // were more of them than the evidence floor requires.
  metrics[M_SUBWINDOW_MET_TARGET] = fixtureRate(
    1,
    fixturePass(["rate>=" + MIN_SUSTAINED_SUBWINDOW_RATIO]),
  );
  metrics[M_SUBWINDOWS_QUALIFYING] = fixtureCounter(
    MIN_QUALIFYING_SUBWINDOWS + 7,
    fixturePass(["count>=" + MIN_QUALIFYING_SUBWINDOWS]),
  );
  metrics[M_SUBWINDOWS_OBSERVED] = fixtureCounter(MIN_QUALIFYING_SUBWINDOWS + 9);
  metrics[M_SUBWINDOW_RESETS] = fixtureCounter(0);
  metrics[M_SUBWINDOW_EVENTS_PER_SEC] = fixtureTrend({
    min: TARGET_EVENTS_PER_SEC + 3,
    med: TARGET_EVENTS_PER_SEC + 21,
    max: TARGET_EVENTS_PER_SEC + 44,
    count: MIN_QUALIFYING_SUBWINDOWS + 9,
  });

  // V-1 throughput, whole-run mean. A threshold is registered for it only when the sampler is
  // off, and the fixture follows that rule rather than asserting one unconditionally.
  metrics[M_EVENTS_PER_SEC] = fixtureGauge(
    TARGET_EVENTS_PER_SEC + 17,
    RATE_SAMPLER_ENABLED
      ? undefined
      : fixturePass(["value>=" + TARGET_EVENTS_PER_SEC]),
  );

  // V-1 sustained, the windowed family.
  metrics[M_WINDOW_EVENTS_PER_SEC] = fixtureTrend(
    { min: TARGET_EVENTS_PER_SEC + 5, med: TARGET_EVENTS_PER_SEC + 30 },
    RATE_SAMPLER_ENABLED
      ? fixturePass(["min>=" + TARGET_EVENTS_PER_SEC])
      : undefined,
  );
  metrics[M_INTERVAL_EVENTS_PER_SEC] = fixtureTrend(
    {
      min: TARGET_EVENTS_PER_SEC + 2,
      "p(10)": TARGET_EVENTS_PER_SEC + 6,
      "p(50)": TARGET_EVENTS_PER_SEC + 19,
      "p(90)": TARGET_EVENTS_PER_SEC + 38,
      max: TARGET_EVENTS_PER_SEC + 51,
      count: 58,
    },
    fixturePass(["p(50)>=" + TARGET_EVENTS_PER_SEC]),
  );
  metrics[M_RATE_WINDOWS_COUNTED] = fixtureGauge(
    27,
    RATE_SAMPLER_ENABLED ? fixturePass(["value>=1"]) : undefined,
  );
  metrics[M_RATE_WINDOWS_SKIPPED] = fixtureGauge(1);

  // V-1 latency. The source code is what makes `measured` true; a figure from any other
  // instrument is not admissible for this criterion however good it looks.
  metrics[M_P99_SECONDS] = fixtureGauge(
    MAX_P99_PUBLISH_SECONDS / 2,
    fixturePass(["value<" + MAX_P99_PUBLISH_SECONDS]),
  );
  metrics[M_P99_SOURCE_CODE] = fixtureGauge(P99_SOURCE_CAPTURE_TO_DISPATCH);
  metrics[M_P99_BUCKET_LOWER] = fixtureGauge(0.5);
  metrics[M_P99_BUCKET_UPPER] = fixtureGauge(1);
  metrics[M_LATENCY_COUNT_DELTA] = fixtureGauge(90000);

  // V-3 dead-letter rate.
  metrics[M_DEAD_LETTER_RATIO] = fixtureGauge(
    MAX_DEAD_LETTER_RATIO / 10,
    fixturePass(["value<" + MAX_DEAD_LETTER_RATIO]),
  );

  // The settling gates and the drain, so the rendered block reports a settled run rather than
  // one whose gates are "not recorded".
  metrics[M_PRE_DRAIN_SETTLED] = fixtureGauge(DRAIN_SETTLED);
  metrics[M_POST_DRAIN_SETTLED] = fixtureGauge(DRAIN_SETTLED);
  metrics[M_PRE_DRAIN_SECONDS] = fixtureGauge(3);
  metrics[M_POST_DRAIN_SECONDS] = fixtureGauge(4);
  metrics[M_BACKLOG_DRAINED] = fixtureGauge(1);
  metrics[M_LOAD_SECONDS] = fixtureGauge(LOAD_SECONDS || 1800);
  metrics[M_PARTITION_KEYS] = fixtureGauge(requiredSpread());

  return metrics;
}

/**
 * verdictSummary reduces a provenance block to the shape the fixtures assert on.
 *
 * The full block is thousands of characters of prose and provenance, none of which is the
 * verdict; this is the verdict.
 *
 * @param {object} provenance the block buildProvenance produced.
 * @returns {object} all_hold plus each entry's verdict_holds.
 */
function verdictSummary(provenance) {
  var holds = {};
  for (var key in provenance.verdicts) {
    if (!Object.prototype.hasOwnProperty.call(provenance.verdicts, key)) {
      continue;
    }
    holds[key] = provenance.verdicts[key].verdict_holds;
  }

  return {
    all_hold: provenance.verdicts_all_hold,
    inputs_available: provenance.verdict_inputs_available,
    holds: holds,
  };
}

/**
 * expectedHolds renders the per-entry expectation for a run in which exactly the named entries
 * failed.
 *
 * Entries the configuration does not apply are expected to be null rather than false, which is
 * the distinction the aggregate turns on.
 *
 * @param {string[]} failing the entry keys expected to be false.
 * @returns {object} entry key to true, false or null.
 */
function expectedHolds(failing) {
  var out = {
    sustained_throughput_subwindows: true,
    throughput_events_per_second: RATE_SAMPLER_ENABLED ? null : true,
    sustained_throughput_events_per_second: RATE_SAMPLER_ENABLED ? true : null,
    publish_latency_p99_seconds: true,
    dead_letter_ratio: true,
  };

  for (var i = 0; i < failing.length; i++) {
    // A disabled entry stays null: it cannot fail, because it is not scored.
    if (out[failing[i]] !== null) {
      out[failing[i]] = false;
    }
  }

  return out;
}

/**
 * runExpositionFixtures drives the Prometheus parser and its reducers.
 *
 * The fixture body carries, deliberately, every element of real exporter output that a naive
 * line-splitting parser gets wrong: HELP and TYPE comments, labels in an order no reader may
 * assume, the OpenTelemetry scope labels that appear on every series, an escaped quote and an
 * escaped backslash inside a label value, a `+Inf` bucket boundary, scientific notation, a
 * trailing timestamp, a metric name that is a strict prefix of another, and a sample whose
 * value is not a number.
 */
function runExpositionFixtures() {
  var body = [
    "# HELP blnk_events_published_total Events published to Kafka.",
    "# TYPE blnk_events_published_total counter",
    'blnk_events_published_total{event_type="transaction.applied",otel_scope_name="blnk",topic="blnk.transactions"} 1200',
    'blnk_events_published_total{topic="blnk.balances",event_type="balance.created",otel_scope_name="blnk"} 3.4e2',
    // A strict prefix of the name above. A parser matching by prefix reports 1540 for the
    // counter and the total is then wrong in the direction that flatters throughput.
    'blnk_events_published_total_by_ledger{topic="blnk.transactions"} 999999',
    'blnk_events_dead_lettered_total{topic="blnk.transactions.dlt",reason="broker refused \\"write\\"",path="a\\\\b"} 2',
    'blnk_outbox_pending 41 1717171717000',
    "blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt=\"1\",le=\"0.5\"} 100",
    'blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1",le="1"} 300',
    'blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1",le="2"} 995',
    'blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1",le="+Inf"} 1000',
    // A SECOND ATTEMPT'S BUCKETS, which the filter must exclude. V-1 is stated over
    // non-retried events, so a retry's latency counted into this histogram would inflate the
    // p99 with intervals the criterion does not describe.
    'blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="2",le="0.5"} 1',
    'blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="2",le="+Inf"} 7',
    'blnk_events_capture_to_dispatch_duration_seconds_sum{attempt="1"} 455.5',
    'blnk_events_capture_to_dispatch_duration_seconds_count{attempt="1"} 1000',
    'blnk_events_broken_total{topic="blnk.transactions"} NaN',
    "",
  ].join("\n");

  var index = parseExposition(body);

  fixtureExpect(
    "parseExposition sums a counter across its label sets and ignores the prefixed sibling",
    sumSeries(index, "blnk_events_published_total", {}),
    1540,
  );
  fixtureExpect(
    "parseExposition filters by label, order-independently",
    sumSeries(index, "blnk_events_published_total", {
      topic: "blnk.transactions",
    }),
    1200,
  );
  fixtureExpect(
    "parseExposition keeps an escaped quote and backslash in a label value intact",
    sumSeries(index, "blnk_events_dead_lettered_total", {
      reason: 'broker refused "write"',
      path: "a\\b",
    }),
    2,
  );
  fixtureExpect(
    "parseExposition ignores a trailing timestamp",
    sumSeries(index, "blnk_outbox_pending", {}),
    41,
  );
  fixtureExpect(
    "a non-numeric sample contributes nothing rather than NaN",
    sumSeries(index, "blnk_events_broken_total", {}),
    null,
  );
  fixtureExpect(
    "an absent series is null, never zero",
    sumSeries(index, "blnk_events_absent_total", {}),
    null,
  );
  fixtureExpect(
    "a filter no sample matches is null, never zero",
    sumSeries(index, "blnk_events_published_total", { topic: "acme.other" }),
    null,
  );

  var histogram = collectHistogram(
    index,
    "blnk_events_capture_to_dispatch_duration_seconds",
    { attempt: "1" },
  );
  fixtureExpect(
    "collectHistogram reads only the filtered attempt's buckets, sum and count",
    histogram,
    {
      buckets: { "0.5": 100, 1: 300, 2: 995, "+Inf": 1000 },
      sum: 455.5,
      count: 1000,
    },
  );

  var quantile = bucketQuantile(0.99, histogram.buckets);
  fixtureExpect(
    "the p99 interpolates INSIDE the bucket whose upper bound is the 2s criterion boundary",
    {
      value: quantile.value,
      lower: quantile.lower,
      upper: quantile.upper,
      total: quantile.total,
      interpolation: quantile.interpolation,
    },
    {
      // 1 + (990 - 300) / (995 - 300), which is what histogram_quantile computes for the same
      // buckets. Asserted to full precision on purpose: an off-by-one in the rank or in the
      // cumulative-to-per-bucket conversion moves this figure without moving the bucket, and a
      // rounded expectation would absorb it.
      value: 1.9928057553956835,
      lower: 1,
      upper: 2,
      total: 1000,
      interpolation: INTERPOLATION_LINEAR,
    },
  );
  fixtureExpect(
    "a p99 beyond the highest finite bound reports that bound rather than +Inf",
    bucketQuantile(0.99, { "0.5": 10, 2: 20, "+Inf": 1000 }),
    {
      value: 2,
      lower: 2,
      upper: Infinity,
      total: 1000,
      interpolation: INTERPOLATION_HIGHEST_FINITE_BOUND,
    },
  );

  fixtureExpect(
    "an empty bucket map yields no quantile rather than zero",
    bucketQuantile(0.99, {}).value,
    null,
  );
  fixtureExpect(
    "a histogram with no +Inf bucket is refused: its total is unknown",
    bucketQuantile(0.99, { "0.5": 10, 1: 30 }).value,
    null,
  );

  var resolved = resolveSeries(
    SERIES_CAPTURE_TO_DISPATCH,
    null,
    collectHistogramNames(index),
  );
  fixtureExpect(
    "the documented capture-to-dispatch spelling is the one this body carries",
    { name: resolved.name, code: resolved.code },
    {
      name: SERIES_CAPTURE_TO_DISPATCH[0],
      code: SERIES_CODE_DOCUMENTED_NAME,
    },
  );
}

/**
 * collectHistogramNames renders a parsed index as the presence map resolveSeries reads.
 *
 * resolveSeries takes the two bucket-name maps a snapshot holds rather than a parsed index, so
 * this bridges the fixture's index into that shape without duplicating either.
 *
 * @param {object} index the parsed exposition index.
 * @returns {object} base metric name to true, for every histogram present.
 */
function collectHistogramNames(index) {
  var present = {};
  for (var name in index) {
    if (!Object.prototype.hasOwnProperty.call(index, name)) {
      continue;
    }
    var suffix = "_bucket";
    if (name.length > suffix.length && name.indexOf(suffix, name.length - suffix.length) >= 0) {
      present[name.substring(0, name.length - suffix.length)] = true;
    }
  }

  return present;
}

/**
 * runDeltaFixtures drives the counter and bucket differencing, including the reset detection
 * that decides whether a run's verdict inputs are usable at all.
 */
function runDeltaFixtures() {
  fixtureExpect(
    "an ordinary counter delta is the difference",
    deltaCounter(100, 250),
    { value: 150, reset: false, baselineMissing: false },
  );
  fixtureExpect(
    "an absent baseline yields the end value and SAYS the baseline was missing",
    deltaCounter(null, 250),
    { value: 250, reset: false, baselineMissing: true },
  );
  fixtureExpect(
    "an end below the baseline is a RESET, not a negative delta",
    deltaCounter(300, 12),
    { value: 12, reset: true, baselineMissing: false },
  );
  fixtureExpect(
    "an absent end value is not measurable and is not zero",
    deltaCounter(300, null),
    { value: null, reset: false, baselineMissing: false },
  );
  fixtureExpect(
    "an unchanged counter is a delta of zero, which IS a measurement",
    deltaCounter(300, 300),
    { value: 0, reset: false, baselineMissing: false },
  );

  fixtureExpect(
    "bucket deltas are per-boundary and never negative",
    deltaBuckets(
      { "0.5": 5, 1: 8, "+Inf": 10 },
      { "0.5": 9, 1: 20, 2: 4, "+Inf": 30 },
    ),
    { buckets: { "0.5": 4, 1: 12, 2: 4, "+Inf": 20 }, reset: false },
  );
  fixtureExpect(
    "a shrunken +Inf bucket is a RESET, and the end map is taken whole",
    deltaBuckets({ "0.5": 5, "+Inf": 100 }, { "0.5": 1, "+Inf": 3 }),
    { buckets: { "0.5": 1, "+Inf": 3 }, reset: true },
  );
}

/**
 * runScrapeFixtures drives the backlog reading the settling gates and the drain wait are
 * decided from.
 *
 * The gates are what make a run's inputs admissible — a window that opens before the previous
 * run's backlog has drained measures the wrong population — and every one of their decisions
 * comes from this one reading.
 */
function runScrapeFixtures() {
  var gauges = {};
  gauges[SERIES_OUTBOX_PENDING[0]] = 41;

  fixtureExpect(
    "a successful scrape yields the pending backlog",
    pendingFromScrape({ available: true, snapshot: { gauges: gauges } }),
    41,
  );
  fixtureExpect(
    "a settled backlog reads as zero, which is a reading and not an absence",
    pendingFromScrape({
      available: true,
      snapshot: { gauges: zeroPendingGauges() },
    }),
    0,
  );
  fixtureExpect(
    "an unavailable scrape yields no reading rather than zero",
    pendingFromScrape({ available: false, snapshot: { gauges: gauges } }),
    null,
  );
  fixtureExpect(
    "a scrape with no backlog series yields no reading rather than zero",
    pendingFromScrape({ available: true, snapshot: { gauges: {} } }),
    null,
  );
}

/**
 * zeroPendingGauges renders a snapshot in which the backlog series is present and zero.
 *
 * @returns {object} the gauge map.
 */
function zeroPendingGauges() {
  var gauges = {};
  gauges[SERIES_OUTBOX_PENDING[0]] = 0;

  return gauges;
}

/**
 * runVerdictAssemblyFixtures drives buildProvenance, the verdict rules and the rendered block.
 *
 * The first fixture is the one this harness exists for: a run that met every criterion must
 * report `verdicts_all_hold` TRUE. It was false for every possible run before this, and no
 * source-string test could have seen that.
 *
 * The remainder are the fail-closed cases, one per way a run can fail to measure what it
 * claims. Each states the exact set of entries that must not hold, so a rule that fails the
 * wrong entry — or fails them all — is caught rather than absorbed.
 */
function runVerdictAssemblyFixtures() {
  var passing = buildProvenance(passingMetrics());
  fixtureExpect(
    "A RUN THAT MET EVERY CRITERION CERTIFIES V-1 AND V-3",
    verdictSummary(passing),
    { all_hold: true, inputs_available: true, holds: expectedHolds([]) },
  );

  var rendered = renderVerdictBlock(passing);
  fixtureExpect(
    "and the rendered block agrees with the summary rather than re-deriving it",
    rendered.indexOf("ALL CRITERIA    PASS") >= 0,
    true,
  );
  fixtureExpect(
    "the rendered block reports the certification in words too",
    rendered.indexOf("V-1 and V-3 certified by this run") >= 0,
    true,
  );

  // 1. The availability guard. Every figure is inside its bound and every threshold passed;
  //    only the guard says the inputs were not all readable.
  var unavailable = passingMetrics();
  unavailable[M_VERDICTS_AVAILABLE] = fixtureGauge(
    0,
    fixtureFail(["value>=" + REQUIRE_METRICS]),
  );
  var unavailableProvenance = buildProvenance(unavailable);
  fixtureExpect(
    "unavailable inputs fail EVERY applicable verdict, whatever the figures say",
    verdictSummary(unavailableProvenance),
    {
      all_hold: false,
      inputs_available: false,
      holds: expectedHolds([
        "sustained_throughput_subwindows",
        "throughput_events_per_second",
        "sustained_throughput_events_per_second",
        "publish_latency_p99_seconds",
        "dead_letter_ratio",
      ]),
    },
  );
  fixtureExpect(
    "and the rendered block says so on its bottom line",
    renderVerdictBlock(unavailableProvenance).indexOf(
      "ALL CRITERIA    FAIL",
    ) >= 0,
    true,
  );

  // 2. No qualifying subwindow was judged. The Rate has no samples, so its `rate>=` threshold
  //    passes vacuously in k6 — this is the case the qualifying COUNT exists to fail, and the
  //    fraction must read null rather than 0.
  var unsampled = passingMetrics();
  unsampled[M_SUBWINDOW_MET_TARGET] = fixtureRate(
    null,
    fixturePass(["rate>=" + MIN_SUSTAINED_SUBWINDOW_RATIO]),
  );
  unsampled[M_SUBWINDOWS_QUALIFYING] = fixtureCounter(
    0,
    fixtureFail(["count>=" + MIN_QUALIFYING_SUBWINDOWS]),
  );
  var unsampledProvenance = buildProvenance(unsampled);
  fixtureExpect(
    "a run whose sampler never judged a subwindow does not certify sustained throughput",
    verdictSummary(unsampledProvenance),
    {
      all_hold: false,
      inputs_available: true,
      holds: expectedHolds(["sustained_throughput_subwindows"]),
    },
  );
  fixtureExpect(
    "and the fraction is NULL rather than the 0 an empty k6 Rate reports",
    unsampledProvenance.verdicts.sustained_throughput_subwindows
      .fraction_meeting_target,
    null,
  );

  // 3. The latency figure came from an inadmissible instrument. The gauge holds a passing
  //    number and its threshold passed; `measured` is the only thing that is false.
  var wrongInstrument = passingMetrics();
  wrongInstrument[M_P99_SOURCE_CODE] = fixtureGauge(P99_SOURCE_PUBLISH_DURATION);
  fixtureExpect(
    "a p99 from publish_duration does not certify V-1, however small it is",
    verdictSummary(buildProvenance(wrongInstrument)),
    {
      all_hold: false,
      inputs_available: true,
      holds: expectedHolds(["publish_latency_p99_seconds"]),
    },
  );

  // 4. An unrecorded latency gauge. k6 reports an unrecorded Gauge as 0 and `0 < 2` passes, so
  //    the figure alone would be the most emphatic possible certification of a measurement
  //    that never happened.
  var noLatency = passingMetrics();
  delete noLatency[M_P99_SECONDS];
  noLatency[M_P99_SOURCE_CODE] = fixtureGauge(P99_SOURCE_NONE);
  fixtureExpect(
    "an unmeasured latency does not certify V-1",
    verdictSummary(buildProvenance(noLatency)),
    {
      all_hold: false,
      inputs_available: true,
      holds: expectedHolds(["publish_latency_p99_seconds"]),
    },
  );

  // 5. A genuine failure: the dead-letter rate exceeded its ceiling.
  var tooManyDeadLetters = passingMetrics();
  tooManyDeadLetters[M_DEAD_LETTER_RATIO] = fixtureGauge(
    MAX_DEAD_LETTER_RATIO * 4,
    fixtureFail(["value<" + MAX_DEAD_LETTER_RATIO]),
  );
  fixtureExpect(
    "a dead-letter rate over the ceiling fails V-3 and nothing else",
    verdictSummary(buildProvenance(tooManyDeadLetters)),
    {
      all_hold: false,
      inputs_available: true,
      holds: expectedHolds(["dead_letter_ratio"]),
    },
  );

  // 6. A THRESHOLD REGISTRATION THAT WENT MISSING. This is the fail-closed property that lets
  //    the not-applicable case be excluded from the aggregate at all: an entry the
  //    configuration DOES score, whose thresholds are absent, must fail rather than be
  //    silently treated as inapplicable.
  var droppedThreshold = passingMetrics();
  droppedThreshold[M_P99_SECONDS] = fixtureGauge(MAX_P99_PUBLISH_SECONDS / 2);
  fixtureExpect(
    "an entry that IS scored but registered no threshold fails rather than being skipped",
    verdictSummary(buildProvenance(droppedThreshold)),
    {
      all_hold: false,
      inputs_available: true,
      holds: expectedHolds(["publish_latency_p99_seconds"]),
    },
  );

  // 7. Nothing at all. The degenerate case, which must not throw and must not certify.
  var emptyProvenance = buildProvenance({});
  fixtureExpect(
    "a summary with no metrics certifies nothing and does not throw",
    verdictSummary(emptyProvenance).all_hold,
    false,
  );
  fixtureExpect(
    "and its block still renders",
    renderVerdictBlock(emptyProvenance).indexOf("ALL CRITERIA") >= 0,
    true,
  );
}

/**
 * runRunnerContractFixtures drives the helpers the shell runner and the summary depend on.
 *
 * The URL helpers matter for a reason beyond tidiness: a credential in a URL is written into
 * summary-event-streaming.json and into CI artifacts, and the sibling derivation is what decides which
 * deployment a run measures.
 */
function runRunnerContractFixtures() {
  fixtureExpect(
    "a sibling endpoint is derived by replacing the path, not by dirname",
    siblingURL("http://localhost:5001/transactions", "/transactions", "/metrics"),
    "http://localhost:5001/metrics",
  );
  fixtureExpect(
    "a URL that does not carry the marker path yields NOTHING rather than a guess",
    // Empty, not the input and not a dirname-style truncation. `dirname` is not URL-aware and
    // answers "http:" for a URL with no path, which is how METRICS_URL once became
    // "http:/metrics" and every scrape failed silently. An empty answer is what the shell
    // runner tests for before refusing the run and naming the variable to set.
    siblingURL("http://localhost:5001", "/transactions", "/metrics"),
    "",
  );
  fixtureExpect(
    "userinfo in a URL is recognised as a credential",
    urlCarriesCredential("http://user:secret@localhost:5001/transactions"),
    true,
  );
  fixtureExpect(
    "and a credential-free URL is not",
    urlCarriesCredential("http://localhost:5001/transactions"),
    false,
  );
  fixtureExpect(
    "a redacted URL carries no secret",
    redactURL("http://user:secret@localhost:5001/transactions").indexOf(
      "secret",
    ) >= 0,
    false,
  );

  fixtureExpect(
    "numberFrom falls back rather than yielding NaN",
    numberFrom("not-a-number", 7),
    7,
  );
  fixtureExpect("numberFrom accepts an explicit zero", numberFrom("0", 7), 0);
  fixtureExpect(
    "a duration is parsed into seconds",
    parseDurationSeconds("30m", 0),
    1800,
  );
  fixtureExpect(
    "a compound duration is parsed into seconds",
    parseDurationSeconds("1h30m10s", 0),
    5410,
  );
  fixtureExpect(
    "an unparseable duration falls back",
    parseDurationSeconds("soon", 42),
    42,
  );
}

/**
 * verdictFixtures is the fixture run's only iteration.
 *
 * It aborts the test when any check failed, rather than returning quietly: a fixture run whose
 * failures were visible only in a threshold row would be reported as a pass by any wrapper that
 * reads the exit code, which is every wrapper.
 */
export function verdictFixtures() {
  runExpositionFixtures();
  runDeltaFixtures();
  runScrapeFixtures();
  runVerdictAssemblyFixtures();
  runRunnerContractFixtures();

  var failed = 0;
  for (var i = 0; i < fixtureOutcomes.length; i++) {
    if (!fixtureOutcomes[i].ok) {
      failed++;
    }
  }

  console.log(
    "[fixture] " +
      (fixtureOutcomes.length - failed) +
      " of " +
      fixtureOutcomes.length +
      " checks held",
  );

  if (failed > 0) {
    exec.test.abort(
      failed +
        " of " +
        fixtureOutcomes.length +
        " event-streaming fixtures disagreed with their expected answer",
    );
  }
}

/**
 * fixtureSummary renders the fixture run's report.
 *
 * THE COUNTS COME FROM THE METRICS, NOT FROM fixtureOutcomes, and that is not a stylistic
 * choice: k6 evaluates handleSummary in a SEPARATE JavaScript runtime from the one the
 * iteration ran in, so every module-level variable is back at its initial value here. Reading
 * fixtureOutcomes reported "0 checks, 0 failures, PASS" for a run whose iteration had just
 * failed two of them — a clean bill of health assembled from an empty slate, which is exactly
 * the class of defect this harness exists to catch. The per-check detail is on stderr, written
 * as each failure occurred, and the abort names the total.
 *
 * @param {object} data the k6 end-of-test summary data.
 * @returns {object} the summary handleSummary returns in fixture mode.
 */
function fixtureSummary(data) {
  var metrics = data ? data.metrics : null;
  var checks = counterTotal(metrics, M_FIXTURE_CHECKS);
  var failures = counterTotal(metrics, M_FIXTURE_FAILURES);
  // A failure counter that is absent OR zero means no failure was recorded — k6 reports a
  // metric with no samples as absent unless a threshold is registered on it, and one is here,
  // so both spellings occur. An absent or empty CHECK counter is a different matter: it means
  // the iteration did not run, and a harness that asserted nothing must not report a pass.
  var held = checks !== null && checks > 0 && (failures === null || failures === 0);
  var report = {
    checks: checks,
    failures: failures === null ? 0 : failures,
    all_fixtures_held: held,
    selfcheck: FIXTURES_SELFCHECK,
  };

  var lines = [
    "",
    "  event-streaming FIXTURE run (no deployment was contacted)",
    "   checks     " + (checks === null ? "NONE RAN" : checks),
    "   failures   " + report.failures,
    "   verdict    " + verdictMark(held),
    "",
  ];

  var out = {};
  out.stdout = lines.join("\n");

  // WRITTEN ONLY WHERE THE CALLER ASKED. SUMMARY_OUT defaults to summary-event-streaming.json, which is
  // the acceptance run's artifact: a fixture run that fell back to it would overwrite a
  // thirty-minute result with a report about the parser.
  if (__ENV.SUMMARY_OUT) {
    out[SUMMARY_OUT] = JSON.stringify(
      { blnk_event_streaming_fixtures: report },
      null,
      2,
    );
  }

  return out;
}

// Optional: saves detailed stats to stdout + JSON file
export function handleSummary(data) {
  // A fixture run measured nothing, so a verdict block assembled from its (empty) metrics would
  // report five withheld criteria beside a passing fixture suite — two unrelated verdicts in one
  // report, the more prominent of which is meaningless.
  if (FIXTURES) {
    return fixtureSummary(data);
  }

  var provenance = buildProvenance(data ? data.metrics : null);
  // merge rather than spread, for Goja, and merge rather than mutate so that the top-level
  // `metrics` key survives untouched: tools/dashboard_parser.py classifies a JSON file as a
  // k6 summary only when that key is present, and silently ignores the file otherwise.
  // Sibling keys are safe — the parser reads only metrics, root_group and state.
  var enriched = merge(data, { blnk_event_streaming: provenance });

  return {
    stdout:
      textSummary(data, { indent: " ", enableColors: true }) +
      renderVerdictBlock(provenance),
    [SUMMARY_OUT]: JSON.stringify(enriched, null, 2),
  };
}
