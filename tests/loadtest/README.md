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

## What the k6 script supports

`tests/loadtest/script.js` now has explicit scenarios for:

- `hot_source`
- `hot_destination`

And supports optional sharding with:

- `SOURCE_BUCKETS`
- `DESTINATION_BUCKETS`

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

Examples:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
bash tests/loadtest/run_case.sh cold-to-hot-no-shard
bash tests/loadtest/run_case.sh hot-to-cold-shard
bash tests/loadtest/run_case.sh cold-to-hot-shard
```

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
