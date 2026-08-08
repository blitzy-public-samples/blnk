/*
Copyright 2024 Blnk Finance Authors.
Apache 2.0
*/

/**
 * Load Test: Outbox-to-Kafka Event Streaming
 *
 * This scenario drives ledger mutations at a constant arrival rate and then decides two
 * acceptance criteria of the event-streaming pipeline:
 *
 *   V-1  500 events/sec sustained producer throughput, and a p99 outbox-to-Kafka publish
 *        latency under 2 seconds for non-retried events.
 *   V-3  under 0.1% of events reach a dead-letter topic over a 30-minute run at 500
 *        events/sec against a healthy broker.
 *
 * ALL THREE VERDICTS ARE READ FROM THE SERVER'S /metrics ENDPOINT, NEVER FROM k6's OWN
 * TIMINGS. That distinction is the whole point of this file, so it is worth stating why:
 *
 *   - k6's `http_req_duration` measures how long the API took to ACCEPT a request. The
 *     latency criterion is about how long the event then took to reach Kafka, measured from
 *     its capture inside the ledger transaction. They are different intervals, and reading
 *     the first one would certify a target the pipeline was missing.
 *   - k6's `http_reqs` rate is NOT the event rate. One POST /transactions produces at least
 *     `transaction.queued` and typically a second event once the worker applies it, so
 *     iterations/sec and events/sec are not the same number. Throughput is therefore the
 *     DELTA of `blnk_events_published_total` across the measured window.
 *
 * Every verdict carries its provenance — the exact Prometheus series it was computed from,
 * and for the latency figure the bucket boundary it landed in — into the summary JSON under
 * the top-level `blnk_event_streaming` key, so a reviewer can confirm at a glance which
 * series produced which number. The verdicts themselves are k6 thresholds on custom
 * metrics, which is what makes them render as pass/fail rows in the existing dashboard with
 * no change to anything under tools/.
 *
 * Run it directly:
 *
 *   k6 run -e URL=http://localhost:5001/transactions \
 *          -e METRICS_URL=http://localhost:5001/metrics \
 *          tests/loadtest/events.js
 *
 * or through the runner, which supplies the same variables:
 *
 *   bash tests/loadtest/run_case.sh event-streaming
 */

import http from "k6/http";
import { check } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.4/index.js";
import { Gauge } from "k6/metrics";

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

// --- Load generation -------------------------------------------------------------------

const URL = __ENV.URL || "http://localhost:5001/transactions";
const API_KEY = __ENV.API_KEY || __ENV.BLNK_API_KEY;
const SUMMARY_OUT = __ENV.SUMMARY_OUT || "tests/loadtest/summary-events.json";
// event_publish
const SCENARIO = __ENV.SCENARIO || "event_publish";
const RATE = numberFrom(__ENV.RATE, 500); // V-1: 500 events/sec of arrival pressure
const DURATION = __ENV.DURATION || "30m"; // V-1 and V-3 are stated over 30 minutes
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
// claim enforces that by returning AT MOST ONE ROW PER PARTITION KEY per poll — a later event
// of the same aggregate is not claimable while an earlier one is in flight, which is exactly
// what makes ordering survive concurrent publishing. Parallelism therefore comes from having
// MANY DISTINCT KEYS, not from a larger batch.
//
// The `"@" + uuidv4()` shorthand the other scenarios in this directory use does NOT produce
// distinct keys. It mints a fresh BALANCE, and every balance created that way lands in the
// same default ledger, so the whole run shares one partition key and the relay can publish
// one event per poll interval no matter how much load is offered. Measured directly: 200
// transactions offered at 20/s yielded 10 published events and 147 rows still pending.
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
const LEDGERS_URL =
  __ENV.LEDGERS_URL || siblingURL(URL, "/transactions", "/ledgers");
const BALANCES_URL =
  __ENV.BALANCES_URL || siblingURL(URL, "/transactions", "/balances");

// --- Verdict ceilings ------------------------------------------------------------------
// All three are overridable so that a five-second smoke run is not judged against
// thresholds calibrated for a thirty-minute one.

const TARGET_EVENTS_PER_SEC = numberFrom(__ENV.TARGET_EVENTS_PER_SEC, 500);
const MAX_P99_PUBLISH_SECONDS = numberFrom(__ENV.MAX_P99_PUBLISH_SECONDS, 2);
const MAX_DEAD_LETTER_RATIO = numberFrom(__ENV.MAX_DEAD_LETTER_RATIO, 0.001);
// Fail closed by default: unless this is relaxed to 0, a run that could not read the
// verdict inputs at all fails rather than reporting three vacuous zeroes as a pass.
const REQUIRE_METRICS = numberFrom(__ENV.REQUIRE_METRICS, 1);
// API-acceptance ceilings, in milliseconds. Deliberately NOT the V-1 latency figure; see
// the comment on the threshold itself.
const MAX_API_P95_MS = numberFrom(__ENV.MAX_API_P95_MS, 500);
const MAX_API_P99_MS = numberFrom(__ENV.MAX_API_P99_MS, 1000);

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
// The broker write in relative isolation. Reported alongside the verdict because the
// difference between the two is the queue wait, which is what tells an operator whether a
// slow end-to-end figure is the broker or a relay backlog. Also the recorded fallback for
// the verdict when capture_to_dispatch is absent.
const SERIES_PUBLISH_DURATION = [
  "blnk_events_publish_duration_seconds",
  "blnk_events_publish_duration",
];
const SERIES_OUTBOX_PENDING = ["blnk_outbox_pending"];

// The closed `attempt` and `outcome` label domains, per docs/metrics.md. FIRST_ATTEMPT is
// the population both latency targets are stated over; DISPATCHED additionally narrows
// publish_duration to successful writes, without which that filter matches nothing at all.
const FIRST_ATTEMPT = "1";
const OUTCOME_DISPATCHED = "dispatched";
const OUTCOME_RETRYING = "retrying";
const OUTCOME_FAILED = "failed";
const OUTCOME_DEAD_LETTERED = "dead_lettered";

// The quantile V-1 is stated at.
const TARGET_QUANTILE = 0.99;

// Numeric codes, recorded into gauges because a gauge carries a number and the strings they
// stand for are reconstituted in handleSummary. Each map is the single definition of its
// vocabulary.
const SERIES_CODE_ABSENT = 0;
const SERIES_CODE_DOCUMENTED_NAME = 1;
const SERIES_CODE_TOLERATED_NAME = 2;

const P99_SOURCE_NONE = 0;
const P99_SOURCE_CAPTURE_TO_DISPATCH = 1;
const P99_SOURCE_PUBLISH_DURATION = 2;

const INTERPOLATION_NONE = 0;
const INTERPOLATION_LINEAR = 1;
const INTERPOLATION_HIGHEST_FINITE_BOUND = 2;
const INTERPOLATION_LOWEST_BUCKET = 3;

const REASON_AVAILABLE = 0;
const REASON_START_SCRAPE_UNAVAILABLE = 1;
const REASON_END_SCRAPE_UNAVAILABLE = 2;
const REASON_WINDOW_NOT_POSITIVE = 3;
const REASON_PUBLISHED_SERIES_ABSENT = 4;
const REASON_NO_TERMINAL_EVENTS = 5;
const REASON_NO_FIRST_ATTEMPT_SAMPLES = 6;

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
const M_P99_SECONDS = "event_publish_p99_seconds";
const M_DEAD_LETTER_RATIO = "event_publish_dead_letter_ratio";
const M_VERDICTS_AVAILABLE = "event_publish_verdicts_available";

// Raw inputs, so the arithmetic behind each verdict can be re-done by hand from the JSON.
const M_WINDOW_SECONDS = "event_publish_window_seconds";
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
const M_QUEUE_WAIT_P99 = "event_publish_queue_wait_p99_seconds";
const M_ATTEMPTS_DISPATCHED = "event_publish_attempts_dispatched_delta";
const M_ATTEMPTS_RETRYING = "event_publish_attempts_retrying_delta";
const M_ATTEMPTS_FAILED = "event_publish_attempts_failed_delta";
const M_ATTEMPTS_DEAD_LETTERED = "event_publish_attempts_dead_lettered_delta";
const M_OUTBOX_PENDING_START = "event_publish_outbox_pending_start";
const M_OUTBOX_PENDING_END = "event_publish_outbox_pending_end";
const M_PUBLISHED_SERIES_CODE = "event_publish_published_series_code";
const M_DEAD_LETTERED_SERIES_CODE = "event_publish_dead_lettered_series_code";
const M_METRICS_STATUS_START = "event_publish_metrics_status_start";
const M_METRICS_STATUS_END = "event_publish_metrics_status_end";
const M_COUNTER_RESET = "event_publish_counter_reset";
const M_DEGRADED_REASON_CODE = "event_publish_degraded_reason_code";
const M_STATS_STATUS = "event_publish_stats_endpoint_status";
const M_STATS_DISPATCHED = "event_publish_stats_dispatched";
const M_STATS_DEAD_LETTERED = "event_publish_stats_dead_lettered";
const M_STATS_PENDING = "event_publish_stats_pending";
const M_PARTITION_KEYS = "event_publish_partition_keys";

const eventsPerSecond = new Gauge(M_EVENTS_PER_SEC);
const p99Seconds = new Gauge(M_P99_SECONDS);
const deadLetterRatio = new Gauge(M_DEAD_LETTER_RATIO);
const verdictsAvailable = new Gauge(M_VERDICTS_AVAILABLE);

const windowSeconds = new Gauge(M_WINDOW_SECONDS);
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
const queueWaitP99 = new Gauge(M_QUEUE_WAIT_P99);
const attemptsDispatched = new Gauge(M_ATTEMPTS_DISPATCHED);
const attemptsRetrying = new Gauge(M_ATTEMPTS_RETRYING);
const attemptsFailed = new Gauge(M_ATTEMPTS_FAILED);
const attemptsDeadLettered = new Gauge(M_ATTEMPTS_DEAD_LETTERED);
const outboxPendingStart = new Gauge(M_OUTBOX_PENDING_START);
const outboxPendingEnd = new Gauge(M_OUTBOX_PENDING_END);
const publishedSeriesCode = new Gauge(M_PUBLISHED_SERIES_CODE);
const deadLetteredSeriesCode = new Gauge(M_DEAD_LETTERED_SERIES_CODE);
const metricsStatusStart = new Gauge(M_METRICS_STATUS_START);
const metricsStatusEnd = new Gauge(M_METRICS_STATUS_END);
const counterReset = new Gauge(M_COUNTER_RESET);
const degradedReasonCode = new Gauge(M_DEGRADED_REASON_CODE);
const statsEndpointStatus = new Gauge(M_STATS_STATUS);
const statsDispatched = new Gauge(M_STATS_DISPATCHED);
const statsDeadLettered = new Gauge(M_STATS_DEAD_LETTERED);
const statsPending = new Gauge(M_STATS_PENDING);
const partitionKeys = new Gauge(M_PARTITION_KEYS);

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

  return o;
}

/**
 * verdictThresholds builds the thresholds that decide V-1 and V-3.
 *
 * Each is expressed against a Gauge, whose only supported aggregation is `value` — the
 * `p(99)<x` form belongs to a Trend and would be rejected here. The p99 is already computed
 * from the server's histogram before it reaches the gauge; k6 is not being asked to
 * compute a percentile of anything.
 *
 * @returns {object} a k6 thresholds map.
 */
function verdictThresholds() {
  var o = {};

  // V-1, throughput. Computed as the delta of blnk_events_published_total over the measured
  // window — NOT from k6's http_reqs, which counts requests rather than events and would
  // under-report by whatever fan-out each transaction has.
  o[M_EVENTS_PER_SEC] = ["value>=" + TARGET_EVENTS_PER_SEC];

  // V-1, latency. The p99 of blnk_events_capture_to_dispatch_duration_seconds_bucket
  // filtered to attempt="1", interpolated exactly as histogram_quantile would.
  o[M_P99_SECONDS] = ["value<" + MAX_P99_PUBLISH_SECONDS];

  // V-3, dead-letter rate. dead_lettered / (dead_lettered + published): the two counters
  // partition a captured event's terminal outcomes, so their sum is the population.
  // Dividing by published alone would overstate the rate and diverge without bound as
  // failures rose.
  o[M_DEAD_LETTER_RATIO] = ["value<" + MAX_DEAD_LETTER_RATIO];

  // Fail closed. This is 1 only when both scrapes succeeded, the series resolved, the
  // window was positive, at least one terminal event was observed and at least one
  // first-attempt latency sample existed. Without it, a run against a stack whose
  // observability was disabled would report three zeroes, two of which pass their `<`
  // thresholds, and certify criteria it never measured.
  o[M_VERDICTS_AVAILABLE] = ["value>=" + REQUIRE_METRICS];

  return o;
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

function buildOptions() {
  if (SCENARIO === "event_publish") {
    return {
      scenarios: {
        // constant-arrival-rate rather than constant-vus, because "500 events per second"
        // is only a meaningful statement about an arrival rate. A constant-VU executor
        // would let the offered load sag exactly when the system slowed down, which is the
        // moment the measurement matters most.
        [SCENARIO]: {
          executor: "constant-arrival-rate",
          rate: RATE,
          timeUnit: "1s",
          duration: DURATION,
          preAllocatedVUs: VUS,
          maxVUs: MAX_VUS,
          tags: { scenario: SCENARIO },
          exec: "publishEvents",
        },
      },
      thresholds: merge(dth(SCENARIO), verdictThresholds()),
      // The baseline and final scrapes each fetch and parse a full Prometheus exposition,
      // so both stages get more room than the defaults allow. teardown additionally does
      // the whole quantile computation.
      setupTimeout: "120s",
      teardownTimeout: "180s",
      // Stated explicitly because the measurement depends on it: with response bodies
      // discarded, the /metrics payload would arrive empty and every verdict would read as
      // unavailable.
      discardResponseBodies: false,
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

/**
 * parseSampleValue parses a Prometheus sample value.
 *
 * @param {string} token the raw value token.
 * @returns {number|null} the parsed value, or null when the token is unusable.
 */
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

/**
 * parseExposition indexes a Prometheus exposition payload by metric name.
 *
 * @param {string} text the raw response body.
 * @returns {object} metric name to an array of `{labels, value}` samples.
 */
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

/**
 * readNumber reads a finite number out of a snapshot section.
 *
 * @param {object} bucket the snapshot section.
 * @param {string} key the series name.
 * @returns {number|null} the value, or null when it is absent or unusable.
 */
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
  var counterLists = [SERIES_PUBLISHED, SERIES_DEAD_LETTERED];
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
    snapshot.attempts[attemptsName] = {
      dispatched:
        sumSeries(index, attemptsName, { outcome: OUTCOME_DISPATCHED }) || 0,
      retrying:
        sumSeries(index, attemptsName, { outcome: OUTCOME_RETRYING }) || 0,
      failed: sumSeries(index, attemptsName, { outcome: OUTCOME_FAILED }) || 0,
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

  return snapshot;
}

/**
 * shallowCopyNumbers copies a flat map of finite numbers.
 *
 * @param {object} source the map to copy.
 * @returns {object} the copy.
 */
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
 * @param {number|null} startValue the baseline reading, or null when the series was absent.
 * @param {number|null} endValue the final reading, or null when the series was absent.
 * @returns {object} `{value, reset}`; `value` is null when the series was never present.
 */
function deltaCounter(startValue, endValue) {
  if (endValue === null || endValue === undefined) {
    return { value: null, reset: false };
  }
  if (startValue === null || startValue === undefined) {
    return { value: endValue, reset: false };
  }
  if (endValue < startValue) {
    return { value: endValue, reset: true };
  }

  return { value: endValue - startValue, reset: false };
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

/**
 * sortedBuckets turns a bucket map into an ascending list of `{le, count}`.
 *
 * @param {object} bucketMap cumulative counts keyed by boundary.
 * @returns {object[]} the ascending list.
 */
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
        METRICS_URL +
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

/**
 * probeEventStats reads GET /events/stats for corroboration of the outbox side.
 *
 * Purely optional. The endpoint is master-key gated, so it is attempted only when a master
 * key is supplied, and every outcome is tolerated: the verdicts are computed from `/metrics`
 * and must not become contingent on a management endpoint being reachable.
 *
 * @returns {object} `{status, dispatched, deadLettered, pending}`; counts are null when
 *   unavailable.
 */
function probeEventStats() {
  var unavailable = {
    status: STATUS_UNREACHABLE,
    dispatched: null,
    deadLettered: null,
    pending: null,
  };

  if (!MASTER_KEY || !EVENTS_STATS_URL) {
    return unavailable;
  }

  var res = null;
  try {
    res = http.get(EVENTS_STATS_URL, {
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
  if (status !== 200 || !res.body) {
    return {
      status: status,
      dispatched: null,
      deadLettered: null,
      pending: null,
    };
  }

  var parsed = null;
  try {
    parsed = JSON.parse(String(res.body));
  } catch (err) {
    return {
      status: status,
      dispatched: null,
      deadLettered: null,
      pending: null,
    };
  }

  // The handler wraps its payload under `data` in some deployments and returns it flat in
  // others, so both shapes are accepted rather than assuming one.
  var body = parsed && parsed.data ? parsed.data : parsed;

  return {
    status: status,
    dispatched: readOptionalNumber(body, "dispatched"),
    deadLettered: readOptionalNumber(body, "dead_lettered"),
    pending: readOptionalNumber(body, "pending"),
  };
}

/**
 * readOptionalNumber reads a numeric field from a decoded JSON object.
 *
 * @param {object} obj the object.
 * @param {string} key the field name.
 * @returns {number|null} the value, or null when it is absent or not numeric.
 */
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

/**
 * postJSON issues a JSON POST with the scenario's authentication, returning the decoded body.
 *
 * @param {string} url the endpoint.
 * @param {object} body the request body.
 * @param {string} endpointTag the value for the `endpoint` metric tag.
 * @returns {object|null} the decoded response body on 200/201, otherwise null.
 */
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
 * provisionAggregates creates the ledgers and balance pairs the load is spread over.
 *
 * Provisioning happens in setup(), BEFORE the baseline scrape, so that the `ledger.created`
 * and `balance.created` events it necessarily produces sit on the baseline side of every
 * delta instead of inflating the measured throughput.
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

  if (!(LEDGER_SPREAD > 0) || !LEDGERS_URL || !BALANCES_URL) {
    return pairs;
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
  var pairs = provisionAggregates();
  console.log(
    "[event_publish] provisioned " +
      pairs.length +
      " of " +
      LEDGER_SPREAD +
      " requested ledger/balance pairs" +
      (pairs.length === 0
        ? " — falling back to the @uuid shorthand, which shares ONE partition key, so the relay will publish one event per poll interval regardless of the offered load"
        : ""),
  );

  // The baseline is taken AFTER provisioning on purpose: the ledger.created and
  // balance.created events provisioning emits belong on the baseline side of the deltas.
  var scrape = scrapeMetrics();

  console.log(
    "[event_publish] baseline scrape " +
      METRICS_URL +
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
    pairs: pairs,
    // A credential-free echo of what the run was told to do, so the numbers below can be
    // interpreted months later without the invoking command line.
    configuration: {
      url: URL,
      metrics_url: METRICS_URL,
      events_stats_url: EVENTS_STATS_URL,
      scenario: SCENARIO,
      rate: RATE,
      duration: DURATION,
      pre_allocated_vus: VUS,
      max_vus: MAX_VUS,
      target_events_per_sec: TARGET_EVENTS_PER_SEC,
      max_p99_publish_seconds: MAX_P99_PUBLISH_SECONDS,
      max_dead_letter_ratio: MAX_DEAD_LETTER_RATIO,
      require_metrics: REQUIRE_METRICS,
      ledger_spread_requested: LEDGER_SPREAD,
      ledger_spread_provisioned: pairs.length,
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

  check(res, {
    "is status 201": function (r) {
      return r.status === 201;
    },
  });
}

/**
 * publishEvents is the scenario body: one ledger mutation, which the pipeline turns into at
 * least one event.
 *
 * The pair is chosen from the aggregates setup() provisioned, so successive iterations land on
 * DIFFERENT partition keys. That is what lets the relay publish them concurrently: its claim
 * returns at most one row per key per poll, so throughput comes from the number of distinct
 * keys in flight and nothing else.
 *
 * With no provisioned pairs the scenario falls back to the `"@" + uuidv4()` shorthand the other
 * scenarios in this directory use. That still exercises the whole path end to end, but every
 * balance it mints belongs to the same default ledger, so the run measures one aggregate's
 * serialisation ceiling rather than the pipeline's throughput. The fallback is recorded in the
 * summary for exactly that reason.
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
  var baseline = data || {};
  var startSnapshot = baseline.baseline || {};
  var startCounters = startSnapshot.counters || {};
  var startAttempts = startSnapshot.attempts || {};
  var startHistograms = startSnapshot.histograms || {};
  var startGauges = startSnapshot.gauges || {};
  var startedAt = numberFrom(baseline.startedAt, 0);
  var startStatus = numberFrom(baseline.metricsStatus, STATUS_UNREACHABLE);

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

  publishedSeriesCode.add(published.code);
  deadLetteredSeriesCode.add(deadLettered.code);

  // --- The measured window -------------------------------------------------------------
  // Measured between the two scrape RESPONSES rather than from the scenario's own duration,
  // so it is the interval the deltas actually span. It is slightly longer than the load
  // itself, which makes the throughput figure a lower bound — the conservative direction.
  var windowMillis = scrape.at - startedAt;
  var measuredWindow =
    startedAt > 0 && windowMillis > 0 ? windowMillis / 1000 : 0;
  windowSeconds.add(measuredWindow);

  // Nothing derived from a delta means anything unless BOTH ends of it were read. With only
  // one end, `deltaCounter` legitimately falls back to the reading it has — which is the
  // right answer for a series that appeared partway through the run, and the wrong one for a
  // scrape that never happened, because it would present a lifetime total as a windowed
  // rate. The gate below is what keeps that number out of the verdicts.
  var scrapesUsable =
    baseline.metricsAvailable === true &&
    scrape.available &&
    measuredWindow > 0;

  // --- Terminal outcome counters -------------------------------------------------------
  var publishedFrom = readNumber(startCounters, published.name);
  var publishedTo = readNumber(endCounters, published.name);
  var publishedChange = deltaCounter(publishedFrom, publishedTo);

  var deadLetteredFrom = readNumber(startCounters, deadLettered.name);
  var deadLetteredTo = readNumber(endCounters, deadLettered.name);
  var deadLetteredChange = deltaCounter(deadLetteredFrom, deadLetteredTo);

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
  attemptsFailed.add(attemptDeltas.failed);
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

  // V-1 is read from capture-to-dispatch. publish_duration is the recorded fallback: it
  // measures a strictly shorter interval, so certifying the target from it would be
  // optimistic, and the substitution is therefore never silent — p99SourceCode says which
  // one produced the number.
  var latencySource = P99_SOURCE_NONE;
  var chosen = null;
  if (capture.quantile.value !== null) {
    latencySource = P99_SOURCE_CAPTURE_TO_DISPATCH;
    chosen = capture;
  } else if (brokerWrite.quantile.value !== null) {
    latencySource = P99_SOURCE_PUBLISH_DURATION;
    chosen = brokerWrite;
  }

  p99SourceCode.add(latencySource);
  p99InterpolationCode.add(
    chosen ? chosen.quantile.interpolation : INTERPOLATION_NONE,
  );
  p99SeriesSpellingCode.add(
    latencySource === P99_SOURCE_PUBLISH_DURATION
      ? publishSeries.code
      : latencySource === P99_SOURCE_CAPTURE_TO_DISPATCH
        ? captureSeries.code
        : SERIES_CODE_ABSENT,
  );

  // The histogram readings are recorded whatever happened, because they are the audit trail
  // for the quantile. `latency_sum` and `latency_count` are there to be checked by hand; the
  // p99 itself is never derived from their ratio, which is a mean.
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
    latencyCountStart.add(chosen.countFrom);
    latencyCountEnd.add(chosen.countTo);
    latencyCountDelta.add(chosen.countDelta);
    latencySumStart.add(chosen.sumFrom);
    latencySumEnd.add(chosen.sumTo);
    latencySumDelta.add(chosen.sumDelta);
  }

  if (brokerWrite.quantile.value !== null) {
    brokerWriteP99.add(brokerWrite.quantile.value);
    if (capture.quantile.value !== null) {
      // The queue wait: how much of the end-to-end figure is backlog rather than broker.
      queueWaitP99.add(capture.quantile.value - brokerWrite.quantile.value);
    }
  }

  // --- The arithmetic behind the three verdicts -----------------------------------------
  var publishedEvents =
    publishedChange.value === null ? 0 : publishedChange.value;
  var deadLetteredEvents =
    deadLetteredChange.value === null ? 0 : deadLetteredChange.value;
  // The two counters partition a captured event's TERMINAL outcomes — delivered or
  // dead-lettered, never both — so their sum is the population the rate is stated over.
  var terminalEvents = publishedEvents + deadLetteredEvents;

  // Every division is guarded. A zero window and a zero terminal population are both real
  // states of a real run, and neither may produce a NaN or an Infinity: JSON.stringify turns
  // both into `null`, which reads as "not measured" for a figure that was measured, or
  // worse, silently drops the finding.
  var throughput = measuredWindow > 0 ? publishedEvents / measuredWindow : 0;
  var ratio = terminalEvents > 0 ? deadLetteredEvents / terminalEvents : 0;

  var resetSeen =
    publishedChange.reset ||
    deadLetteredChange.reset ||
    capture.reset ||
    brokerWrite.reset;
  counterReset.add(resetSeen ? 1 : 0);

  // --- Fail closed unless every input was genuinely present ----------------------------
  var reason = REASON_AVAILABLE;
  if (baseline.metricsAvailable !== true) {
    reason = REASON_START_SCRAPE_UNAVAILABLE;
  } else if (!scrape.available) {
    reason = REASON_END_SCRAPE_UNAVAILABLE;
  } else if (!(measuredWindow > 0)) {
    reason = REASON_WINDOW_NOT_POSITIVE;
  } else if (published.code === SERIES_CODE_ABSENT) {
    reason = REASON_PUBLISHED_SERIES_ABSENT;
  } else if (!(terminalEvents > 0)) {
    reason = REASON_NO_TERMINAL_EVENTS;
  } else if (latencySource === P99_SOURCE_NONE) {
    reason = REASON_NO_FIRST_ATTEMPT_SAMPLES;
  }

  degradedReasonCode.add(reason);
  verdictsAvailable.add(reason === REASON_AVAILABLE ? 1 : 0);

  // Each verdict gauge is recorded only when it is a genuine measurement. An unrecorded
  // Gauge reads as 0 in the summary, and 0 passes a `value<x` threshold — so recording a
  // figure derived from a scrape that never happened is the one way this file could report a
  // false pass. `event_publish_verdicts_available` is the row that fails instead, and the
  // three code gauges beside each number say why.
  if (scrapesUsable && published.code !== SERIES_CODE_ABSENT) {
    eventsPerSecond.add(throughput);
  }
  if (scrapesUsable && terminalEvents > 0) {
    deadLetterRatio.add(ratio);
  }
  if (scrapesUsable && chosen) {
    p99Seconds.add(chosen.quantile.value);
  }

  // --- Optional corroboration ----------------------------------------------------------
  var stats = probeEventStats();
  statsEndpointStatus.add(stats.status);
  statsDispatched.add(stats.dispatched === null ? -1 : stats.dispatched);
  statsDeadLettered.add(stats.deadLettered === null ? -1 : stats.deadLettered);
  statsPending.add(stats.pending === null ? -1 : stats.pending);

  reportToConsole({
    reason: reason,
    window: measuredWindow,
    throughput: throughput,
    publishedEvents: publishedEvents,
    deadLetteredEvents: deadLetteredEvents,
    ratio: ratio,
    latencySource: latencySource,
    latencySeries:
      latencySource === P99_SOURCE_PUBLISH_DURATION
        ? publishSeries.name
        : captureSeries.name,
    publishedSeries: published.name,
    deadLetteredSeries: deadLettered.name,
    quantile: chosen ? chosen.quantile : null,
    endScrapeReason: scrape.reason,
    startScrapeReason: baseline.metricsReason || "",
    reset: resetSeen,
    pendingTo: pendingTo,
    partitionKeys: baseline.pairs ? baseline.pairs.length : 0,
  });
}

/**
 * attemptOutcomeDeltas differences the four publish-attempt outcome counters.
 *
 * @param {object|undefined} from the baseline per-outcome totals.
 * @param {object|undefined} to the final per-outcome totals.
 * @returns {object} the per-outcome deltas, zero where the series was absent.
 */
function attemptOutcomeDeltas(from, to) {
  var outcomes = [
    OUTCOME_DISPATCHED,
    OUTCOME_RETRYING,
    OUTCOME_FAILED,
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
 * @param {object|undefined} from the baseline `{buckets, sum, count}`.
 * @param {object|undefined} to the final `{buckets, sum, count}`.
 * @returns {object} the quantile result and the `_count`/`_sum` readings at both ends.
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
  result.countFrom = countFrom;
  result.countTo = countTo;
  result.countDelta = countChange.value === null ? 0 : countChange.value;
  result.sumFrom = sumFrom;
  result.sumTo = sumTo;
  result.sumDelta = sumChange.value === null ? 0 : sumChange.value;

  return result;
}

/**
 * round renders a number for human consumption without pulling in a formatter.
 *
 * @param {number|null} value the value.
 * @param {number} places decimal places to keep.
 * @returns {string} the rendered value, or "n/a".
 */
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

/**
 * verdictMark renders a pass/fail marker for a console line.
 *
 * @param {boolean} ok whether the verdict passed.
 * @returns {string} the marker.
 */
function verdictMark(ok) {
  return ok ? "PASS" : "FAIL";
}

/**
 * latencySeriesLabel renders the fully-qualified series the latency p99 was computed from,
 * label filter included.
 *
 * One function builds this string for both the console lines and the summary's provenance
 * block, so the two can never disagree about which series produced the number. The filter
 * differs between the two instruments: capture-to-dispatch carries `topic` and `attempt`,
 * while publish-duration carries `outcome` as well and needs it pinned to `dispatched` or the
 * filter matches nothing.
 *
 * @param {string|null} baseName the resolved histogram base name.
 * @param {number|null} sourceCode which instrument produced the figure.
 * @returns {string|null} the series label, or null when nothing was resolved.
 */
function latencySeriesLabel(baseName, sourceCode) {
  if (!baseName) {
    return null;
  }

  var filter =
    sourceCode === P99_SOURCE_PUBLISH_DURATION
      ? '{attempt="1",outcome="dispatched"}'
      : '{attempt="1"}';

  return baseName + "_bucket" + filter;
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
    "[event_publish] measured window " +
      round(r.window, 3) +
      "s between the two scrapes",
  );
  lines.push(
    "[event_publish] V-1 throughput  " +
      verdictMark(available && r.throughput >= TARGET_EVENTS_PER_SEC) +
      "  " +
      round(r.throughput, 2) +
      " events/sec (target >= " +
      TARGET_EVENTS_PER_SEC +
      ")  from " +
      r.publishedSeries +
      " delta of " +
      round(r.publishedEvents, 0) +
      " — NOT from k6 http_reqs",
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
        (r.latencySource === P99_SOURCE_PUBLISH_DURATION
          ? " — FALLBACK SERIES, see the provenance block"
          : "") +
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
  } else {
    lines.push(
      "[event_publish] V-1 p99 publish FAIL  not computable — no first-attempt observations on " +
        latencySeriesLabel(r.latencySeries, r.latencySource),
    );
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
      r.deadLetteredSeries,
  );

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
        " outbox rows still unpublished (the throughput figure is a lower bound by this much)",
    );
  }
  if (r.reset) {
    lines.push(
      "[event_publish] WARNING a counter reset was detected — the exporter restarted mid-run, so the deltas are post-reset readings",
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
P99_SOURCE_LABELS[P99_SOURCE_PUBLISH_DURATION] =
  "publish-duration FALLBACK: claim to broker acknowledgement, a strictly shorter interval than V-1 is stated over, used only because the capture-to-dispatch series was absent";

const INTERPOLATION_LABELS = {};
INTERPOLATION_LABELS[INTERPOLATION_NONE] = "no observations in the window";
INTERPOLATION_LABELS[INTERPOLATION_LINEAR] =
  "linearly interpolated between the bucket bounds, as histogram_quantile does";
INTERPOLATION_LABELS[INTERPOLATION_HIGHEST_FINITE_BOUND] =
  "the rank fell in the +Inf bucket, so the highest finite boundary is reported as a LOWER BOUND on the true p99";
INTERPOLATION_LABELS[INTERPOLATION_LOWEST_BUCKET] =
  "the rank fell in the lowest bucket, whose upper bound is not positive, so that bound is reported directly";

const REASON_LABELS = {};
REASON_LABELS[REASON_AVAILABLE] = "every verdict input was read successfully";
REASON_LABELS[REASON_START_SCRAPE_UNAVAILABLE] =
  "the baseline /metrics scrape did not succeed, so there is no left-hand side to any delta";
REASON_LABELS[REASON_END_SCRAPE_UNAVAILABLE] =
  "the final /metrics scrape did not succeed, so there is no right-hand side to any delta";
REASON_LABELS[REASON_WINDOW_NOT_POSITIVE] =
  "the measured window was not positive, so no rate can be computed from it";
REASON_LABELS[REASON_PUBLISHED_SERIES_ABSENT] =
  "blnk_events_published_total was absent, so no event has ever been published by this process";
REASON_LABELS[REASON_NO_TERMINAL_EVENTS] =
  "no event reached a terminal outcome in the window — with KAFKA_BROKERS unset the publisher is a no-op, which is a legitimate steady state and not a failure of the pipeline";
REASON_LABELS[REASON_NO_FIRST_ATTEMPT_SAMPLES] =
  'no first-attempt latency observations were recorded, so the p99 has no population; the attempt="1" filter matched nothing';

// The canonical PromQL each figure is the in-script equivalent of, quoted from
// docs/metrics.md so that the documented query and the reported number cannot drift apart.
const PROMQL_THROUGHPUT = "rate(blnk_events_published_total[5m])";
const PROMQL_P99 =
  'histogram_quantile(0.99, sum by (le) (rate(blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))';
const PROMQL_BROKER_WRITE =
  'histogram_quantile(0.99, sum by (le) (rate(blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))';
const PROMQL_QUEUE_WAIT = PROMQL_P99 + " - " + PROMQL_BROKER_WRITE;
const PROMQL_DEAD_LETTER =
  "sum(rate(blnk_events_dead_lettered_total[30m])) / (sum(rate(blnk_events_dead_lettered_total[30m])) + sum(rate(blnk_events_published_total[30m])))";

/**
 * gaugeValue reads a custom gauge's end-of-run value out of the summary data.
 *
 * @param {object} metrics the summary's metrics map.
 * @param {string} name the metric name.
 * @returns {number|null} the value, or null when the metric is absent.
 */
function gaugeValue(metrics, name) {
  var m = metrics ? metrics[name] : null;
  if (!m || !m.values) {
    return null;
  }

  var v = Number(m.values.value);

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
 * describeCode looks a numeric code up in its vocabulary.
 *
 * @param {object} vocabulary the code-to-string map.
 * @param {number|null} code the code.
 * @returns {string} the description.
 */
function describeCode(vocabulary, code) {
  if (code === null || code === undefined) {
    return "not recorded";
  }
  if (Object.prototype.hasOwnProperty.call(vocabulary, code)) {
    return vocabulary[code];
  }

  return "unrecognised code " + code;
}

/**
 * resolvedSeriesName reconstructs the exposition name a spelling code stands for.
 *
 * @param {string[]} candidates the candidate list the code indexes into.
 * @param {number|null} code the spelling code.
 * @returns {string|null} the matched name, or null when nothing matched.
 */
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
 * histogram and not from k6's HTTP timings, and that the throughput came from the published
 * counter and not from k6's request count. Each verdict therefore names the series it was
 * computed from, the PromQL it is equivalent to, and — for the latency — the bucket the rank
 * landed in and how it was interpolated.
 *
 * @param {object} metrics the summary's metrics map.
 * @returns {object} the provenance block.
 */
function buildProvenance(metrics) {
  var sourceCode = gaugeValue(metrics, M_P99_SOURCE_CODE);
  var spellingCode = gaugeValue(metrics, M_P99_SERIES_SPELLING_CODE);
  var latencyCandidates =
    sourceCode === P99_SOURCE_PUBLISH_DURATION
      ? SERIES_PUBLISH_DURATION
      : SERIES_CAPTURE_TO_DISPATCH;
  var latencyBase = resolvedSeriesName(latencyCandidates, spellingCode);
  var bucketUpper = gaugeValue(metrics, M_P99_BUCKET_UPPER);
  var reason = gaugeValue(metrics, M_DEGRADED_REASON_CODE);
  var available = gaugeValue(metrics, M_VERDICTS_AVAILABLE);

  return {
    generated_by: "tests/loadtest/events.js",
    scenario: SCENARIO,
    acceptance_criteria: {
      "V-1":
        "500 events/sec sustained, p99 outbox-to-Kafka publish latency under 2s for non-retried events",
      "V-3":
        "under 0.1% of events reach a dead-letter topic over 30 minutes at 500 events/sec",
    },
    measurement_source:
      "the blnk server's /metrics Prometheus exposition, scraped once before the load and once after",
    verdict_inputs_available: available === 1,
    verdict_inputs_condition: describeCode(REASON_LABELS, reason),
    verdicts: {
      throughput_events_per_second: {
        metric: M_EVENTS_PER_SEC,
        value: gaugeValue(metrics, M_EVENTS_PER_SEC),
        target: TARGET_EVENTS_PER_SEC,
        comparison: ">=",
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
          "the counter's delta across the measured window, summed over every topic and event_type label",
        equivalent_promql: PROMQL_THROUGHPUT,
        thresholds: thresholdVerdicts(metrics, M_EVENTS_PER_SEC),
      },
      publish_latency_p99_seconds: {
        metric: M_P99_SECONDS,
        value: gaugeValue(metrics, M_P99_SECONDS),
        ceiling: MAX_P99_PUBLISH_SECONDS,
        comparison: "<",
        series: latencySeriesLabel(latencyBase, sourceCode),
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
        equivalent_promql: PROMQL_P99,
        thresholds: thresholdVerdicts(metrics, M_P99_SECONDS),
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
        equivalent_promql: PROMQL_DEAD_LETTER,
        thresholds: thresholdVerdicts(metrics, M_DEAD_LETTER_RATIO),
      },
    },
    supporting_figures: {
      broker_write_p99_seconds: {
        metric: M_BROKER_WRITE_P99,
        value: gaugeValue(metrics, M_BROKER_WRITE_P99),
        equivalent_promql: PROMQL_BROKER_WRITE,
        note: "the broker write in relative isolation; not the V-1 figure, because its clock starts at the outbox claim",
      },
      queue_wait_p99_seconds: {
        metric: M_QUEUE_WAIT_P99,
        value: gaugeValue(metrics, M_QUEUE_WAIT_P99),
        equivalent_promql: PROMQL_QUEUE_WAIT,
        note: "how much of the end-to-end figure is relay backlog rather than broker latency",
      },
    },
    load_shape: {
      partition_keys: gaugeValue(metrics, M_PARTITION_KEYS),
      note: "the number of independent aggregates the offered load was spread over. The relay's claim returns at most ONE row per partition key per poll, so this is the ceiling on concurrent publishing: throughput comes from distinct keys, not from a larger batch. Zero means the @uuid shorthand fallback was used, whose balances all share one default ledger — such a run measures one aggregate's serialisation ceiling and cannot certify a throughput target",
    },
    raw_inputs: {
      measured_window_seconds: gaugeValue(metrics, M_WINDOW_SECONDS),
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
      publish_attempts_failed: gaugeValue(metrics, M_ATTEMPTS_FAILED),
      publish_attempts_dead_lettered: gaugeValue(
        metrics,
        M_ATTEMPTS_DEAD_LETTERED,
      ),
      outbox_pending_start: gaugeValue(metrics, M_OUTBOX_PENDING_START),
      outbox_pending_end: gaugeValue(metrics, M_OUTBOX_PENDING_END),
      counter_reset_detected: gaugeValue(metrics, M_COUNTER_RESET) === 1,
      note: "latency_sum and latency_count are recorded for auditing only. Their ratio is a MEAN and is never used as the p99: substituting it would report a comfortably passing figure for a distribution with a long tail",
    },
    conditions: {
      baseline_scrape_http_status: gaugeValue(metrics, M_METRICS_STATUS_START),
      final_scrape_http_status: gaugeValue(metrics, M_METRICS_STATUS_END),
      status_zero_means: "the request never reached a listener",
      events_stats_http_status: gaugeValue(metrics, M_STATS_STATUS),
      events_stats_dispatched: gaugeValue(metrics, M_STATS_DISPATCHED),
      events_stats_dead_lettered: gaugeValue(metrics, M_STATS_DEAD_LETTERED),
      events_stats_pending: gaugeValue(metrics, M_STATS_PENDING),
      events_stats_note:
        "corroboration only, master-key gated and attempted solely when a master key is supplied; -1 means the figure was not read. No verdict depends on it",
    },
    deliberately_not_used_for_any_verdict: [
      "http_req_duration — API ACCEPTANCE latency. V-1 is about outbox-to-Kafka publish time, a different interval; reading this would certify a target the pipeline was missing",
      "http_reqs — REQUEST count and rate. One POST /transactions produces at least two events, so iterations/sec is not events/sec",
      "blnk_events_publish_duration_seconds _sum / _count — a mean, not a quantile",
    ],
  };
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
  var lines = [];
  var rows = [
    {
      title: "V-1 throughput  ",
      entry: v.throughput_events_per_second,
      bound: ">= " + v.throughput_events_per_second.target + " events/sec",
      places: 2,
    },
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
  ];

  lines.push("");
  lines.push(
    "  event-streaming verdicts, read from " + provenance.measurement_source,
  );
  for (var i = 0; i < rows.length; i++) {
    lines.push(
      "   " +
        rows[i].title +
        verdictMark(allThresholdsOk(rows[i].entry.thresholds)) +
        "  " +
        round(rows[i].entry.value, rows[i].places) +
        "  (" +
        rows[i].bound +
        ")  <- " +
        (rows[i].entry.series ||
          rows[i].entry.numerator_series ||
          "series unresolved"),
    );
  }
  lines.push(
    "   verdict inputs  " +
      verdictMark(provenance.verdict_inputs_available) +
      "  " +
      provenance.verdict_inputs_condition,
  );
  lines.push("");

  return lines.join("\n");
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

// Optional: saves detailed stats to stdout + JSON file
export function handleSummary(data) {
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
