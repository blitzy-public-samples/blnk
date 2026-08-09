#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <case-name> [queue-mode]"
  echo "cases: hot-to-cold-no-shard | cold-to-hot-no-shard | hot-to-cold-shard | cold-to-hot-shard"
  echo "       events | event-streaming  (the event-streaming acceptance run: V-1 throughput"
  echo "                and p99, V-3 dead-letter rate. Both names run the same case)"
  echo "queue-mode: normal | spread | hot (default: normal, transaction cases only)"
  exit 1
fi

CASE_NAME="$1"
QUEUE_MODE="${2:-normal}"

# Hoisted above the event-streaming branch, because that branch derives METRICS_URL from it. It
# used to sit between two copies of that branch, so the first copy could not see it and the second
# could — which is how one of them echoed a metrics endpoint different from the one k6 received.
URL="${URL:-http://localhost:5001/transactions}"

# ---------------------------------------------------------------------------
# The event-streaming case
# ---------------------------------------------------------------------------
#
# Dispatched BEFORE the transaction defaults below are applied, and that ordering is the
# whole point of putting it here rather than in the case statement further down.
#
# The transaction cases default to RATE=300 for DURATION=30s. Acceptance criterion V-1 is
# stated at 500 events per second sustained for thirty minutes, and V-3's dead-letter rate is
# stated over the same window. Reaching events.js through the defaults above would therefore
# have produced a summary that reported PASS on all three verdicts after thirty seconds at
# 300/s — the run's own load parameters silently substituted for the criterion's, which is
# the same class of defect as certifying the latency target from the wrong histogram.
#
# So this case forwards RATE, DURATION, VUS and MAX_VUS ONLY when the caller set them, and
# otherwise lets events.js apply its own defaults, which ARE the criterion's figures. An
# operator who wants a two-minute smoke run passes DURATION=2m explicitly and can see in the
# artifact that they did.
#
# `event-streaming` IS AN ALIAS, NORMALISED ONTO THE ONE CASE NAME rather than dispatched by a
# second branch. Both names were once implemented as their own branch, forwarding a different set
# of variables and spelling the master key differently — one passed MASTER_KEY, the other
# BLNK_MASTER_KEY, and events.js accepts either, so both appeared to work while forwarding
# different things. An operator who read one document got one behaviour and an operator who read
# the other got the other. Normalising the name keeps `events` the single dispatch point, which is
# the name every document and the README use.
if [[ "${CASE_NAME}" == "event-streaming" ]]; then
  CASE_NAME="events"
fi

if [[ "${CASE_NAME}" == "events" ]]; then
  EVENTS_OUT_DIR="tests/loadtest"
  EVENTS_SUMMARY_OUT="${SUMMARY_OUT:-${EVENTS_OUT_DIR}/summary-events.json}"
  EVENTS_NDJSON_OUT="${NDJSON_OUT:-${EVENTS_OUT_DIR}/run-events.ndjson}"

  events_args=()

  # The verdicts are computed from Blnk's own /metrics, scraped before and after the run, so
  # the bearer token is not optional when metrics are protected. The master key is what reads
  # GET /events/stats. Both are taken from the same BLNK_* names ./.env already exports, so a
  # sourced environment needs no extra flags.
  # Derived from URL when it was not given, so a run against a non-default host needs one
  # variable rather than two. events.js derives it too; doing it here as well means the value the
  # runner ECHOES below is the value k6 receives.
  METRICS_URL="${METRICS_URL:-$(dirname "${URL}")/metrics}"
  events_args+=(-e "METRICS_URL=${METRICS_URL}")

  # The metrics endpoint is guarded by MetricsAuthHandler whenever server.secure is true, and an
  # unauthenticated scrape then collects NOTHING — which does not fail loudly. It produces a run
  # whose verdicts are all withheld and whose degraded reason is a scrape that did not succeed.
  if [[ -z "${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}" ]]; then
    echo "note: no METRICS_BEARER_TOKEN or BLNK_METRICS_BEARER_TOKEN is set."
    echo "      If this deployment has server.secure enabled, every /metrics scrape will be"
    echo "      refused and the run will report withheld verdicts rather than a pass or a fail."
  fi
  [[ -n "${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN:-}}" ]] &&
    events_args+=(-e "METRICS_BEARER_TOKEN=${BLNK_METRICS_BEARER_TOKEN:-${METRICS_BEARER_TOKEN}}")
  [[ -n "${BLNK_SERVER_SECRET_KEY:-${MASTER_KEY:-}}" ]] &&
    events_args+=(-e "MASTER_KEY=${BLNK_SERVER_SECRET_KEY:-${MASTER_KEY}}")
  [[ -n "${API_KEY:-${BLNK_API_KEY:-}}" ]] &&
    events_args+=(-e "API_KEY=${API_KEY:-${BLNK_API_KEY}}")
  events_args+=(-e "URL=${URL}")

  # Load shape: forwarded only when explicitly set, per the note above.
  for setting in RATE DURATION VUS MAX_VUS LEDGER_SPREAD TARGET_EVENTS_PER_SEC \
    MAX_DEAD_LETTER_RATIO REQUIRE_METRICS; do
    if [[ -n "${!setting:-}" ]]; then
      events_args+=(-e "${setting}=${!setting}")
    fi
  done

  echo "Running event-streaming acceptance case (${CASE_NAME})"
  echo "  transactions : ${URL}"
  echo "  metrics      : ${METRICS_URL}"
  echo "  load shape: $(
    [[ -n "${RATE:-}${DURATION:-}" ]] && echo "caller-specified" || echo "events.js defaults (V-1: 500/s for 30m)"
  )"

  # No queue benchmark. That tool measures Redis asynq depth for the transaction pipeline;
  # the event pipeline's backlog is blnk_outbox_pending on /metrics, which the scenario reads
  # itself. Requiring a Redis DSN here would block a run that has no use for one.
  k6 run \
    --out "json=${EVENTS_NDJSON_OUT}" \
    -e "SUMMARY_OUT=${EVENTS_SUMMARY_OUT}" \
    "${events_args[@]}" \
    tests/loadtest/events.js

  echo "Generated files:"
  echo "  ${EVENTS_SUMMARY_OUT}"
  echo "  ${EVENTS_NDJSON_OUT}"
  exit 0
fi

# THE EARLY EXIT ABOVE IS THE POINT rather than a shortcut (PERF-P14). events.js documented a
# `run_case.sh event-streaming` command while the runner had no such case, so it printed "unknown
# case" — the one instruction a reader of that file is most likely to follow. Both names now
# dispatch to the single branch above.
#
# It cannot reuse the pipeline below. That pipeline REQUIRES a Redis DSN and refuses to run
# without one, starts tools/queue_benchmark.go to measure asynq queue drain, and runs script.js.
# None of the three applies here: this scenario reads its verdicts from the server's /metrics
# endpoint, so it needs METRICS_URL and a bearer token instead of Redis, and a queue-drain
# benchmark would measure the transaction pipeline rather than the event pipeline.
#
# The branch above deliberately restates NONE of events.js's own defaults. DURATION, RATE, VUS,
# MAX_VUS and every threshold default live in that file — V-1 and V-3 are stated over 30 minutes
# at 500 events/second — so a second set of numbers here would be a second opinion able to
# disagree with the acceptance criteria. That is why every load-shape variable is forwarded ONLY
# when the caller set it, and why an empty credential is not forwarded as an empty value: `-e
# METRICS_BEARER_TOKEN=` overrides the file's own resolution with nothing.

DURATION="${DURATION:-30s}"
RATE="${RATE:-300}"
VUS="${VUS:-200}"
MAX_VUS="${MAX_VUS:-800}"
REDIS_DSN="${BLNK_REDIS_DNS:-${REDIS_DSN:-}}"

OUT_DIR="tests/loadtest"
SUMMARY_OUT="${OUT_DIR}/summary-${CASE_NAME}.json"
NDJSON_OUT="${OUT_DIR}/run-${CASE_NAME}.ndjson"
QUEUE_OUT="${OUT_DIR}/queue-summary-${CASE_NAME}.json"

SCENARIO=""
SOURCE_BUCKETS="1"
DESTINATION_BUCKETS="1"
QUEUE_PREFIXES="new:transaction_"

case "${CASE_NAME}" in
  hot-to-cold-no-shard)
    SCENARIO="hot_source"
    ;;
  cold-to-hot-no-shard)
    SCENARIO="hot_destination"
    ;;
  hot-to-cold-shard)
    SCENARIO="hot_source"
    SOURCE_BUCKETS="${SOURCE_BUCKETS_OVERRIDE:-8}"
    ;;
  cold-to-hot-shard)
    SCENARIO="hot_destination"
    DESTINATION_BUCKETS="${DESTINATION_BUCKETS_OVERRIDE:-8}"
    ;;
  *)
    echo "unknown case: ${CASE_NAME}"
    echo "queue cases: hot-to-cold-no-shard | cold-to-hot-no-shard | hot-to-cold-shard | cold-to-hot-shard"
    echo "event case:  events | event-streaming"
    exit 1
    ;;
esac

if [[ "${QUEUE_MODE}" == "hot" ]]; then
  QUEUE_PREFIXES="new:transaction_,hot_"
fi

if [[ -z "${REDIS_DSN}" ]]; then
  echo "BLNK_REDIS_DNS or REDIS_DSN is required for queue benchmarking"
  exit 1
fi

echo "Starting queue benchmark for ${CASE_NAME}"
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "${REDIS_DSN}" \
  -queue-prefixes "${QUEUE_PREFIXES}" \
  -wait \
  -out "${QUEUE_OUT}" &
QUEUE_BENCH_PID=$!

cleanup() {
  if kill -0 "${QUEUE_BENCH_PID}" >/dev/null 2>&1; then
    kill -INT "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
    wait "${QUEUE_BENCH_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

echo "Running ${CASE_NAME}"
k6 run \
  --out "json=${NDJSON_OUT}" \
  -e SUMMARY_OUT="${SUMMARY_OUT}" \
  -e URL="${URL}" \
  -e SCENARIO="${SCENARIO}" \
  -e DURATION="${DURATION}" \
  -e RATE="${RATE}" \
  -e VUS="${VUS}" \
  -e MAX_VUS="${MAX_VUS}" \
  -e SOURCE_BUCKETS="${SOURCE_BUCKETS}" \
  -e DESTINATION_BUCKETS="${DESTINATION_BUCKETS}" \
  tests/loadtest/script.js

echo "Waiting for queue drain benchmark to finish"
wait "${QUEUE_BENCH_PID}"

trap - EXIT

echo "Generated files:"
echo "  ${SUMMARY_OUT}"
echo "  ${NDJSON_OUT}"
echo "  ${QUEUE_OUT}"
