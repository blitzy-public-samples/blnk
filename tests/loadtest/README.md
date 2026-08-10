# Load Test Guide

This directory supports two kinds of measurement:

- server/API acceptance via k6
- queue drain time via the queue benchmark tool

Use both. A fast `summary.json` only tells you the API accepted work quickly. The queue summary tells you how long workers took to finish it.

## Open the dashboard

From the repo root:

```bash
python3 tests/loadtest/tools/dashboard.py
```

That starts the dashboard server, opens your browser, and scans `tests/loadtest/` for:

- `summary*.json`
- `queue-summary*.json`
- `run*.ndjson`

If you do not want it to open a browser automatically:

```bash
python3 tests/loadtest/tools/dashboard.py --no-open
```

Open:

```text
http://127.0.0.1:8765/dashboard/
```

## Test cases

These are the main flow tests to compare:

- `hot-to-cold-no-shard`
  - one hot source sends to many cold destinations
- `cold-to-hot-no-shard`
  - many cold sources send to one hot destination
- `hot-to-cold-shard`
  - same as above, but the hot source is split into source buckets first
- `cold-to-hot-shard`
  - same as above, but the hot destination is split into destination buckets first
- `events`
  - drives event publication at 500 events/sec for 30 minutes and measures throughput, p99
    publish latency and dead-letter rate from the server's `/metrics` endpoint. `event-streaming`
    is accepted as a second name for it. Not a queue-topology comparison, so read it against the
    acceptance criteria rather than against the four above — see the event-streaming acceptance
    run below

## What the k6 script supports

`tests/loadtest/script.js` now has explicit scenarios for:

- `hot_source`
- `hot_destination`

And supports optional sharding with:

- `SOURCE_BUCKETS`
- `DESTINATION_BUCKETS`

The `events` case does not run that script. It runs `tests/loadtest/events.js`, which recognises
exactly one scenario, `event_publish`, and takes its inputs from:

- `RATE`
- `DURATION`
- `VUS`
- `MAX_VUS`
- `METRICS_URL`
- `METRICS_BEARER_TOKEN`

`SOURCE_BUCKETS` and `DESTINATION_BUCKETS` mean nothing to it. Its equivalent is `LEDGER_SPREAD`,
the number of independent aggregates the offered load is spread over.

## Fastest way to run a case

Use the helper script from the repo root:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
```

That will:

1. start the queue benchmark
2. run the k6 load test
3. wait for the queue benchmark to finish
4. write three files into `tests/loadtest/`

The output files are:

- `summary-<case>.json`
- `run-<case>.ndjson`
- `queue-summary-<case>.json`

That is the artifact set for the four queue cases. The `events` case writes only two files,
`summary-events.json` and `run-events.ndjson`, because it has no asynq queue to drain and so runs
no queue benchmark and needs no Redis DSN. There is deliberately no `queue-summary-events.json`.
Its equivalent measurement is the outbox backlog and the publish-duration histogram, which it
reads from the server's `/metrics` endpoint itself.

Examples:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
bash tests/loadtest/run_case.sh cold-to-hot-no-shard
bash tests/loadtest/run_case.sh hot-to-cold-shard
bash tests/loadtest/run_case.sh cold-to-hot-shard
```

And the event-streaming case, under either of its two names:

```bash
bash tests/loadtest/run_case.sh events
bash tests/loadtest/run_case.sh event-streaming
```

Both dispatch the same case and write the same two files. Its defaults are the acceptance
criteria's own figures — 500 events/sec for 30 minutes — so override the load shape for a first
attempt. The runner forwards `RATE`, `DURATION`, `VUS` and `MAX_VUS` only when you set them, and
prints which of the two shapes it used. Override the ceilings too, or a ten-second run at rate 5
is judged against a target stated over thirty minutes at 500:

```bash
RATE=5 DURATION=10s VUS=5 MAX_VUS=10 \
  TARGET_EVENTS_PER_SEC=1 SMOKE=1 \
  bash tests/loadtest/run_case.sh events
```

`SMOKE=1` is the explicit opt-out from the acceptance contract: it permits the single-aggregate
fallback so a shakeout does not have to provision fixtures first. Never set it for a run whose
numbers are quoted against V-1 or V-3 — the summary records which mode produced every figure.

The `[queue-mode]` argument is accepted for this case and has no effect on it. There is no queue
benchmark for it to widen.

If the hot queue is enabled and you want the queue benchmark to include it:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard hot
```

That makes the queue benchmark watch both:

- `new:transaction_*`
- `hot_*`

## The event-streaming acceptance run

`tests/loadtest/events.js` is a different kind of scenario from the four above. It does not
compare queue topologies; it decides whether the Kafka event pipeline meets three stated
acceptance criteria, and it prints a verdict for each:

| Verdict metric | Criterion |
|----------------|-----------|
| `event_publish_events_per_second` | V-1 — 500 events/sec sustained |
| `event_publish_p99_seconds` | V-1 — p99 capture-to-dispatch latency under 2s, first attempts only |
| `event_publish_dead_letter_ratio` | V-3 — under 0.1% of events dead-lettered |
| `event_publish_verdicts_available` | the three above were actually measured |

Run it with:

```bash
set -a; . ./.env; set +a          # the master key and the metrics bearer token
bash tests/loadtest/run_case.sh events
```

Two files are written: `summary-events.json` and `run-events.ndjson`. No queue benchmark runs
and no Redis DSN is needed — the event pipeline's backlog is `blnk_outbox_pending`, which the
scenario reads from `/metrics` itself.

**The load shape defaults to the criterion's own figures — 500/s for 30 minutes — and the
runner does not substitute the transaction cases' defaults for them.** Pass `RATE` or
`DURATION` explicitly for a shorter smoke run, and the run announces that you did:

```bash
RATE=50 DURATION=2m bash tests/loadtest/run_case.sh events
```

A smoke run's verdicts are real for the load it offered, which is not the load the criteria are
stated over. Only a default run certifies V-1 and V-3.

### Reading the verdict honestly

Two things about the output are worth knowing before relying on it.

`event_publish_verdicts_available` is the row to check FIRST. A run against a stack whose
observability was off reports three zeroes, two of which satisfy their `<` thresholds — so this
flag is what stops a stack that measured nothing from reporting a clean sweep. It goes to 0, and
the summary names the reason, when either metrics scrape failed, the window was not positive, no
terminal events were seen, or **the first-attempt latency histogram had no observations**.

The p99 verdict is computed from `blnk_events_capture_to_dispatch_duration_seconds` and from
nothing else. `blnk_events_publish_duration_seconds` is reported beside it, and their difference
is reported as the queue wait, but neither is a threshold and neither can satisfy V-1: publish
duration's clock starts at the relay's claim, so it omits the queue wait and reports its
smallest figures for exactly the backlog the criterion exists to catch.

### Where each verdict comes from

All three verdicts are read from the server's `GET /metrics` endpoint, never from k6's own
timings. That is this guide's opening distinction applied to the event pipeline: a fast k6
summary proves the API accepted work quickly, and these three numbers are what the pipeline then
did with it.

- **Throughput**, in events/sec, is the delta of `blnk_events_published_total` over the measured
  window, divided by the interval the load was offered over. It is not k6's `http_reqs` rate: an
  arrival the executor could not start produces no event, a transaction the API rejects produces
  `transaction.rejected` instead, and provisioning's own `ledger.created` and `balance.created`
  events belong to no request in the window. **k6 iterations/sec is not events/sec.** Sustained is
  also a stronger claim than the average, so a second single-VU scenario samples the counter on a
  fixed cadence, and `event_publish_window_events_per_second` (`min`) and
  `event_publish_interval_events_per_second` (`p(50)`) are the rows that carry it.
  `event_publish_events_per_second` is the whole-run average, and it holds the verdict only on a
  run too short for the sampler to count a window.
- **p99 publish latency**, in seconds, is the 0.99 quantile of
  `blnk_events_capture_to_dispatch_duration_seconds_bucket` filtered to `attempt="1"`,
  interpolated the way `histogram_quantile` would be. It is not `http_req_duration`, which
  measures API acceptance, and it is not `blnk_events_publish_duration_seconds`, which starts its
  clock at the relay's claim. The gauge reporting the gap between those two is
  `event_publish_p99_difference_seconds`, and it is deliberately not named a queue wait: it is the
  difference of two independently ranked p99 values, so read it as an order-of-magnitude
  diagnostic and not as a percentile of anything.
- **Dead-letter rate** is `blnk_events_dead_lettered_total` divided by the sum of
  `blnk_events_dead_lettered_total` and `blnk_events_published_total` over the window. The sum is
  the denominator because those two counters partition a captured event's terminal outcomes;
  dividing by the published count alone would report dead-letters as a fraction of successes. An
  absent dead-letter series is **not** read as zero — the run corroborates from the master-key
  gated `GET /events/stats` instead, and withholds the verdict when neither source is available.

All three land in `summary-events.json` as custom metrics carrying k6 thresholds, with the series
each was computed from recorded as provenance under the top-level `blnk_event_streaming` key. That
is what makes the dashboard render them as pass/fail rows with no change to anything under
`tools/`, and what lets a reviewer confirm which series produced which number.

None of it is measured unless `/metrics` is reachable. That needs `"enable_observability": true`
in `blnk.json` (or `BLNK_ENABLE_OBSERVABILITY=true`), and the endpoint is served on the API port
(default `5001`) and the worker monitoring port (default `5004`) — the relay runs in the server
role, so the server's endpoint is the one carrying these instruments. With `server.secure` enabled
it answers `403 Forbidden` when Blnk itself has no token configured, and `401 Unauthorized` when a
scrape arrives without one, so set `BLNK_METRICS_BEARER_TOKEN` on Blnk and give the run the same
value; the runner forwards it from either that name or `METRICS_BEARER_TOKEN`. A refused scrape
does not fail loudly — it withholds the verdicts and names the scrape as the reason.
`docs/metrics.md` is the full catalogue and is not restated here.

Two readings look like failures and are not. With `KAFKA_BROKERS` empty the publisher is a no-op,
so a run reports zero published events and withholds its verdicts — a legitimate configuration
rather than a broken pipeline, and the summary names it as the reason. And the
`DeadLetterMessageStuck` alert in `alerts/blnk-kafka-alerts.yml` fires on
`blnk_dlt_oldest_message_age_seconds > 900`: that is an age alert about a triage backlog nobody is
clearing, which is a different measurement from this case's rate verdict.

## Manual run flow

If you want to run the tools manually instead of using `run_case.sh`, use two terminals.

Terminal 1, start the queue benchmark:

```bash
go run ./tests/loadtest/tools/queue_benchmark.go \
  -redis-dsn "$BLNK_REDIS_DNS" \
  -queue-prefixes "new:transaction_,hot_" \
  -wait \
  -out tests/loadtest/queue-summary.json
```

Terminal 2, run k6:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_source' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e SOURCE_BUCKETS='1' \
  tests/loadtest/script.js
```

For `cold-to-hot`:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_destination' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e DESTINATION_BUCKETS='1' \
  tests/loadtest/script.js
```

To rerun the same test with sharding:

- for `hot_source`, raise `SOURCE_BUCKETS`
- for `hot_destination`, raise `DESTINATION_BUCKETS`

Example:

```bash
k6 run \
  --out json=tests/loadtest/run.ndjson \
  -e SUMMARY_OUT=tests/loadtest/summary.json \
  -e URL='http://localhost:5001/transactions' \
  -e SCENARIO='hot_source' \
  -e DURATION='30s' \
  -e RATE='300' \
  -e VUS='200' \
  -e MAX_VUS='800' \
  -e SOURCE_BUCKETS='8' \
  tests/loadtest/script.js
```

## Suggested comparison order

Use this sequence and keep all generated files:

1. run `hot-to-cold-no-shard`
2. run `cold-to-hot-no-shard`
3. review queue drain time and keep the reports
4. shard and rerun the same two cases
5. compare the new reports against the no-shard runs
6. if you want to test queue optimizations on top, turn on worker concurrency, coalescing, or hot lane and rerun the sharded cases

## What to look at in the dashboard

For each case, inspect both:

- server summary
  - `summary-<case>.json`
- queue summary
  - `queue-summary-<case>.json`

Read them like this:

- `summary`
  - how fast the API accepted the transactions
- `queue-summary`
  - how fast the workers drained the queued work

The most useful comparison fields are:

- throughput
- p95 latency
- duration
- total processed

If the server numbers are good but queue duration is still high, the bottleneck is in worker drain, not API acceptance.
