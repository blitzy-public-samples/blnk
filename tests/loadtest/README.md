# Load Test Guide

This directory supports two kinds of measurement:

- server/API acceptance via k6
- queue drain time via the queue benchmark tool

For the four queue-topology cases, use both. A fast `summary.json` only tells you the API accepted work quickly; the queue summary tells you how long workers took to finish it.

The `event-streaming` case is the exception, and the instruction above does not apply to it: it runs no queue benchmark and writes no `queue-summary` file at all, because the event pipeline has no asynq queue to drain. Its equivalent measurement is the outbox backlog and the capture-to-dispatch histogram, which it reads from the server's `/metrics` endpoint itself.

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
- `event-streaming`
  - offers event publication at 550 events/sec for 30 minutes, judged against V-1's 500/s,
    and measures throughput, p99
    capture-to-dispatch latency and dead-letter rate from the server's own instruments — the
    `/metrics` exposition, plus the `GET /events/stats` census where the dead-letter counter is
    not exported. Not a queue-topology
    comparison, so read it against the acceptance criteria rather than against the four above —
    see the event-streaming acceptance run below.
  - **`event-streaming` is the case name this guide uses throughout, and it is the name the two
    artifacts are called after.** `events` is accepted as a shorthand for the same case: the
    runner normalises it onto `event-streaming` and prints both the name you typed and the one it
    resolved to, so one run cannot produce a second pair of filenames.
  - It has two prerequisites the other four do not, and both are REFUSED rather than warned
    about: a Blnk instance **dedicated to the run** (`ISOLATED_INSTANCE=1`), because every figure
    it produces is a delta of process-global counters, and a **fixture decision**
    (`LEDGER_PAIRS=…` or `ALLOW_FIXTURE_CREATION=1`), because provisioning permanently adds
    ledgers and balances to a database with no delete endpoint for either. A master key is also
    needed for a certifiable run: it is what reads the outbox census the settling gates require.

## What the k6 script supports

`tests/loadtest/script.js` now has explicit scenarios for:

- `hot_source`
- `hot_destination`

And supports optional sharding with:

- `SOURCE_BUCKETS`
- `DESTINATION_BUCKETS`

### The four queue cases execute third-party code; the event-streaming case does not

`script.js` imports two modules from `https://jslib.k6.io` at run time — `k6-utils` and
`k6-summary`. That code is fetched over the network on every run and executed inside the VU
runtime, where it can read all of `__ENV`, and `script.js` itself reads `API_KEY`/`BLNK_API_KEY`
from there. k6 does not pin those modules by digest, so what executes is whatever that host
served, and the four queue cases inherit that trust boundary.

Treat it accordingly: run the queue cases against a disposable environment with a
least-privilege API key, and **never put a master key, a metrics bearer token, or any production
credential in the environment of a queue-case run**. Nothing in the queue arm needs one — the
runner forwards no credential to `script.js` at all.

`events.js` imports only k6 built-ins (`k6/http`, `k6`, `k6/metrics`) and fetches no remote
module, which is why the acceptance run is the one case that may legitimately be given the master
key and the metrics bearer token.

The `event-streaming` case does not run that script. It runs `tests/loadtest/events.js`, which recognises
exactly one scenario, `event_publish`, and is configured entirely through environment variables. The
ones that change what a run measures or whether it is allowed to start are grouped below with their
defaults; the rest are documented in the file itself, beside the constant each one feeds — which is
the authoritative inventory, since a count repeated here goes stale the first time a knob is added.
`grep -o '__ENV\.[A-Z_0-9]*' tests/loadtest/events.js | sort -u` lists them all.

**Load shape** — every default here is the criterion's own figure, so overriding any of them
produces a run whose numbers are real for the load it offered and not for the load V-1 is stated
over.

| Variable | Default | What it does |
|----------|---------|--------------|
| `TARGET_EVENTS_PER_SEC` | `500` | The rate the throughput verdict is judged against |
| `LOAD_HEADROOM_RATIO` | `1.1` | Multiplier applied to the target to get the offered rate |
| `RATE` | `550` (derived) | Offered arrival rate; `ceil(TARGET × HEADROOM)` unless set |
| `DURATION` | `30m` | V-1 and V-3 are both stated over this window |
| `VUS` / `MAX_VUS` | `400` / `1600` | Executor pool; too low and arrivals are dropped rather than slow |
| `LEDGER_SPREAD` | `128` | Independent aggregates the load is spread over. Replaces `SOURCE_BUCKETS`/`DESTINATION_BUCKETS`, which mean nothing here |
| `MIN_LEDGER_SPREAD` | see file | Floor below which acceptance mode refuses to measure |

**Verdict thresholds** — the pass/fail bars. Relaxing one does not make a run invalid, but the
summary records that you did, and a figure quoted against V-1 or V-3 must come from the defaults.

| Variable | Default | What it does |
|----------|---------|--------------|
| `MAX_P99_PUBLISH_SECONDS` | `2` | V-1's latency ceiling |
| `MAX_DEAD_LETTER_RATIO` | `0.001` | V-3's ceiling, i.e. 0.1% |
| `MAX_API_P95_MS`, `MAX_API_P99_MS` | see file | API-acceptance ceilings, separate from the pipeline verdicts |
| `MIN_CHECK_PASS_RATE`, `MAX_DROPPED_ITERATION_RATIO` | see file | Guard against certifying a run the executor could not actually drive |
| `REQUIRE_METRICS` | `1` | Fail closed when `/metrics` could not be scraped. `0` keeps the run but withholds verdicts |

**Endpoints and credentials** — see the run instructions below for how these reach the scenario.

| Variable | Default | What it does |
|----------|---------|--------------|
| `URL` | `http://localhost:5001/transactions` | Where load is offered |
| `METRICS_URL` | `http://localhost:5001/metrics` | Where all three verdicts are read from. The runner derives it from `URL` |
| `EVENTS_STATS_URL`, `LEDGERS_URL`, `BALANCES_URL` | derived from `URL` | Sibling endpoints for corroboration and fixtures |
| `METRICS_BEARER_TOKEN` | none | Required when `server.secure` is on, or every scrape is refused |
| `MASTER_KEY` (or `BLNK_MASTER_KEY`) | none | Reads the master-key gated `GET /events/stats` |
| `API_KEY` (or `BLNK_API_KEY`) | none | Authenticates the offered transactions |

**Fixtures** — these decide whether a run is permitted to create permanent ledgers and balances.
Read the fixture note under the acceptance run before using any of them.

| Variable | Default | What it does |
|----------|---------|--------------|
| `LEDGER_PAIRS` | none | Reuse existing fixtures instead of creating any |
| `ALLOW_FIXTURE_CREATION` | `0` | Authorises creation on a disposable database |
| `SMOKE` | `0` | Opt out of the acceptance contract; **also authorises fixture creation** |

**Settling, drain and sampling** — these control when measurement starts and stops, and are the
reason a verdict is about a settled pipeline rather than a snapshot mid-backlog:
`RAMP_EXCLUSION_SECONDS`, `SAMPLE_INTERVAL_SECONDS`, `SUBWINDOW_SECONDS`,
`RATE_WINDOW_SECONDS`, `RATE_WINDOW_WARMUP_SECONDS`, `MIN_SUSTAINED_SUBWINDOW_RATIO`,
`MIN_QUALIFYING_SUBWINDOWS`, `PRE_DRAIN_BUDGET_SECONDS`, `POST_DRAIN_BUDGET_SECONDS`,
`DRAIN_POLL_SECONDS`, `DRAIN_STABLE_SAMPLES`, `DRAIN_FLOOR`, `DRAIN_TARGET_ROWS`,
`SETTLE_POLL_SECONDS`, and `REQUIRE_DRAIN` (default `1`, fail closed on a pipeline that will not
settle). `SETUP_TIMEOUT_SECONDS` and `TEARDOWN_TIMEOUT_SECONDS` bound the fixture phases.

`SCENARIO` is **pinned** by the runner and is not an input you set — see below.

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

That is the artifact set for the four queue cases. The `event-streaming` case writes only
`summary-event-streaming.json` by default, because it has no asynq queue to drain and so runs no queue
benchmark and needs no Redis DSN, and because its raw `run-event-streaming.ndjson` stream is opt-in — see
`RAW_OUTPUT` below. There is deliberately no `queue-summary-event-streaming.json`.
Its equivalent measurement is the **unsettled outbox depth** — every row from which a Kafka
publish is still owed — which it reads from the master-key-gated `GET /events/stats` census, with
`blnk_outbox_pending` on `/metrics` as a partial fallback. `blnk_events_publish_duration_seconds`
is read too, but only as a diagnostic-only supporting figure: it is never a verdict, because its
clock starts at the relay's claim.

Examples:

```bash
bash tests/loadtest/run_case.sh hot-to-cold-no-shard
bash tests/loadtest/run_case.sh cold-to-hot-no-shard
bash tests/loadtest/run_case.sh hot-to-cold-shard
bash tests/loadtest/run_case.sh cold-to-hot-shard
```

And the event-streaming case. **A dedicated instance and a fixture
decision are both required** — the run refuses to start without either, for the reasons in the
acceptance section — so every command below carries `ISOLATED_INSTANCE=1` plus one of
`ALLOW_FIXTURE_CREATION=1` and `LEDGER_PAIRS`:

```bash
set -a; . ./.env; set +a          # the master key and the metrics bearer token

# Disposable or per-run database: this run creates the fixtures and they are PERMANENT.
ISOLATED_INSTANCE=1 ALLOW_FIXTURE_CREATION=1 \
  bash tests/loadtest/run_case.sh event-streaming

# Reusable fixtures: nothing is created, and successive runs are comparable. The list must hold
# at least LEDGER_SPREAD pairs — 128 by default — so a two-pair list needs the spread narrowed
# to match it, and a narrowed spread measures less concurrency than V-1 is stated over.
ISOLATED_INSTANCE=1 LEDGER_SPREAD=2 \
  LEDGER_PAIRS='[{"source":"bln_a1","destination":"bln_b1"},{"source":"bln_a2","destination":"bln_b2"}]' \
  bash tests/loadtest/run_case.sh event-streaming
```

The case's defaults are the acceptance criteria's own figures — 550/s offered for 30 minutes,
judged against V-1's 500/s — so override the load shape for a first attempt. The runner forwards
`RATE`, `DURATION`, `VUS` and `MAX_VUS` only when you set them, and prints which of the two shapes
it used. Override the ceilings too, or a ten-second run at rate 5 is judged against a target
stated over thirty minutes at 500:

```bash
RATE=5 DURATION=10s VUS=5 MAX_VUS=10 \
  TARGET_EVENTS_PER_SEC=1 SMOKE=1 \
  bash tests/loadtest/run_case.sh event-streaming
```

`SMOKE=1` is the explicit opt-out from the acceptance contract: it permits the single-aggregate
fallback so a shakeout does not have to provision fixtures first, and it relaxes the
dedicated-instance requirement. Never set it for a run whose numbers are quoted against V-1 or
V-3 — the summary records which mode produced every figure.

### Why those two variables are not optional, and why neither is in `.env`

**`ISOLATED_INSTANCE=1`** asserts that the Blnk deployment under test is serving nothing else.
Read "Attribution" below for what it buys; the short version is that the verdicts are deltas of
process-global counters, so another client's events would be counted as this run's.

**`LEDGER_PAIRS` or `ALLOW_FIXTURE_CREATION=1`** decides the fixtures. The scenario spreads its
load over `LEDGER_SPREAD` independent aggregates (128 by default) because the relay claims at most
one row per partition key per poll — with one key it would measure a single aggregate's
serialisation ceiling and report it as V-1. Provisioning those aggregates creates up to
`LEDGER_SPREAD` ledgers and twice as many balances, and **Blnk has no delete endpoint for either**,
so they are permanent and every later run and benchmark sees them. The scenario therefore refuses
to create them as a side effect of being run: pass `LEDGER_PAIRS` to reuse existing balance pairs,
or `ALLOW_FIXTURE_CREATION=1` to acknowledge the permanent addition on a disposable or per-run
database. (`LEDGER_SPREAD=0` is a third path: it opts into the single-aggregate measurement
deliberately, which is right for ordering work and wrong for throughput.)

Neither variable is in `.env.example` on purpose. That file is sourced by every run and by the
service itself, so a value parked in it would turn both of these into standing defaults — which
is precisely what makes them per-run acknowledgements rather than configuration. Put them on the
command line, where the artifact and the reader can both see that this run made the choice.

One file is written by default, `summary-event-streaming.json`, and it is the acceptance artifact: every
verdict is computed inside the scenario and recorded there. The raw k6 NDJSON stream is opt-in,
because a thirty-minute run at 500/s writes tens of millions of records and gigabytes from the
load generator while it is trying to measure sub-second latency — the artifact would degrade the
verdict it exists to evidence. Ask for it when you want per-request detail from a short run, and
prefer a `.gz` destination, which k6 compresses directly:

```bash
NDJSON_OUT=tests/loadtest/run-event-streaming.ndjson.gz \
  RATE=5 DURATION=10s VUS=5 MAX_VUS=10 TARGET_EVENTS_PER_SEC=1 SMOKE=1 \
  bash tests/loadtest/run_case.sh event-streaming
```

`RAW_OUTPUT=1` does the same at the default path. Naming `NDJSON_OUT` implies it.

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
| `event_publish_window_events_per_second` | V-1 — 500 events/sec sustained, weakest window |
| `event_publish_subwindow_met_target` | V-1 — the fraction of windows that met the target |
| `event_publish_events_per_second` | the whole-run mean. A **diagnostic** while the sampler runs; the verdict only when it is off |
| `event_publish_p99_seconds` | V-1 — p99 capture-to-dispatch latency under 2s, first attempts only |
| `event_publish_dead_letter_ratio` | V-3 — under 0.1% of events dead-lettered |
| `event_publish_verdicts_available` | the three above were actually measured |

### Before the first run: both process roles, and fixtures

**Two Blnk processes are required, not one.** The server hosts the API and the event relay; the
worker drains the asynq transaction queue. Start only the server and the load is accepted, queued,
and never processed — no ledger mutation happens, so no event is ever published, and the run
correctly reports that nothing reached a terminal outcome. That reads like a broken pipeline and is
a missing process:

```bash
go build -o blnk ./cmd/*.go     # NOT `go build -o blnk .` — the root package has no main
set -a; . ./.env; set +a
./blnk start &                  # API + event relay, port 5001
./blnk workers &                # transaction pipeline, monitoring port 5004
```

**Fixtures have to be decided before the run, because they cannot be undone.** Blnk exposes no
DELETE endpoint for a ledger or a balance, so a run that provisions its own spread changes the
database permanently and every later run sees the accumulated population. `events.js` refuses to
create them implicitly and **aborts during `setup()`** until one of three choices is stated:

| Choice | What it means | Use it when |
|--------|---------------|-------------|
| `LEDGER_PAIRS='[{"source":"bln_…","destination":"bln_…"}]'` | Reuse fixtures that already exist. Nothing is created. | Repeated acceptance runs against a long-lived database |
| `ALLOW_FIXTURE_CREATION=1` | Acknowledges that this run permanently adds up to `LEDGER_SPREAD` ledgers and twice that many balances | A disposable or per-run database |
| `SMOKE=1` | A shakeout whose numbers are not quoted; permits the single-aggregate fallback | First-time wiring checks only |

### The complete acceptance recipe

This is the whole command, with every variable it needs. It provisions its own spread, so run it
against a database you are willing to grow. Run it with:

```bash
set -a; . ./.env; set +a                  # master key (BLNK_SERVER_SECRET_KEY) + metrics bearer token
export API_KEY="$BLNK_SERVER_SECRET_KEY"  # REQUIRED: ./.env ships no API key of its own
ISOLATED_INSTANCE=1 LEDGER_SPREAD=64 SERVER_REPLICAS=1 \
  ALLOW_FIXTURE_CREATION=1 bash tests/loadtest/run_case.sh event-streaming
```

`API_KEY` is not optional and sourcing `./.env` does not provide it — that file ships the master
key under `BLNK_SERVER_SECRET_KEY` and no API key at all. Without one, `POST /ledgers`,
`POST /balances` and `POST /transactions` are all refused with `401`, and the run then aborts in
`setup()` complaining that provisioning yielded too few partition keys — several layers away from
the authentication error that caused it. The runner prints a note when no key is set. Reusing the
master key as above is appropriate for a local stack; issue a real API key for anything else.

Nothing else is needed: the load shape, the target, the latency ceiling and the dead-letter
ceiling all default to the criteria's own figures — 550/s offered for 30 minutes and judged
against V-1's 500/s, p99 under 2 seconds, dead-letter rate under 0.1%.

To certify repeatedly without growing the database, reuse the fixtures the first run created —
Blnk cannot delete them, so reuse is the only way to keep certifying without adding more. The run
prints them at the end as a single base64 token:

```text
[event_publish] fixtures created and PERMANENT. Reuse them instead of creating more — decode the
token below as tests/loadtest/README.md describes:
  LEDGER_PAIRS_B64=W3siZGVzdGluYXRpb24iOiJibG5f…
```

Base64 rather than the JSON itself, because k6 renders every console line as logfmt and escapes
the double quotes inside a value — on a terminal as well as into a file — so a JSON array printed
there would look copy-pasteable and would not be. Decode it into the next run:

```bash
ISOLATED_INSTANCE=1 \
  LEDGER_PAIRS="$(echo W3siZGVzdGluYXRpb24iOiJibG5f… | base64 -d)" \
  LEDGER_SPREAD=64 \
  SERVER_REPLICAS=1 \
  bash tests/loadtest/run_case.sh event-streaming
```

`LEDGER_SPREAD` must match the number of pairs you supply. Acceptance mode requires that many
distinct partition keys and refuses to run on fewer — events are keyed by aggregate and the relay
claims at most one row per key per poll, so a run on too few keys measures one aggregate's
serialisation ceiling and would report it as V-1. Supplying two pairs while the spread still says
64 aborts in `setup()` saying exactly that.

`SERVER_REPLICAS` declares how many server processes `METRICS_URL` fronts. Every verdict is a
delta of a **per-process** counter read from one URL, so the endpoint has to be one process:
behind a Kubernetes Service consecutive scrapes reach arbitrary backends and the delta mixes
counters that never formed a series, which looks like a throughput dip rather than an error.
Declare the real number — anything above 1 withholds the verdicts and says why, and the correct
instrument for a scaled deployment is a Prometheus query using the `equivalent_promql` each
verdict publishes. The run also watches `process_start_time_seconds` on every sample and withholds
the verdicts if it ever changes, so an endpoint that turns out to rotate is caught regardless of
what was declared.

One file is written by default: `summary-event-streaming.json`, with the raw NDJSON stream opt-in. No queue benchmark runs and no Redis DSN is needed.
The scenario reads the outbox depth from the master-key gated `GET /events/stats` when a key is
available and falls back to `blnk_outbox_pending` plus `blnk_events_repair_backlog` on `/metrics`
when it is not.

**On a shared database, set `DRAIN_FLOOR`.** Both ends of the window wait for the outbox to go
quiet, and "quiet" counts every row from which a KAFKA publish is still owed — `pending`, `processing`,
`failed` and `replaying`. `webhook_pending` is deliberately not in that population; see
"The settling gates: what \"settled\" covers" below for why. A database
another writer is also using has a resting depth that never reaches zero, so the gate will exhaust
its budget and the run will withhold its verdicts. Read the resting depth first and set the floor
above it:

```bash
curl -s -H "X-Blnk-Key: $BLNK_SERVER_SECRET_KEY" \
  "http://localhost:5001/events/stats?include_offsets=false"
```

Run it against a **dedicated** Blnk instance, with a fixture decision:

```bash
set -a; . ./.env; set +a          # the master key and the metrics bearer token

ISOLATED_INSTANCE=1 ALLOW_FIXTURE_CREATION=1 \
  bash tests/loadtest/run_case.sh event-streaming
```

or, to reuse fixtures from an earlier run instead of adding more:

```bash
set -a; . ./.env; set +a

ISOLATED_INSTANCE=1 \
  LEDGER_PAIRS="$(cat tests/loadtest/ledger-pairs.json)" \
  bash tests/loadtest/run_case.sh event-streaming
```

where `ledger-pairs.json` is a file you keep containing
`[{"source":"bln_…","destination":"bln_…"}, …]` — one entry per aggregate, and **at least
`LEDGER_SPREAD` of them (128 by default)**. A shorter list fails preflight rather than quietly
measuring a narrower spread, because throughput is bounded by the number of distinct partition
keys in flight: pass `LEDGER_SPREAD` to ask for the number you have, or `MIN_LEDGER_SPREAD` to
accept fewer than you asked for, and record why. Reusing the same pairs on every run keeps the
fixture population constant, which is what makes successive runs comparable: an accumulating
ledger table changes query plans, and a benchmark whose fixtures grow is measuring two things at
once.

No queue benchmark runs and no Redis DSN is needed. What replaces it is the outbox census: the run waits for every row from
which a Kafka publish is still owed before it takes each of its two scrapes, reading
`GET /events/stats` (master-key gated) and falling back to `blnk_outbox_pending` on `/metrics`.

**Both scrapes come from the SAME instance, and everything else on that instance counts too.**
That is why `ISOLATED_INSTANCE=1` is required and why the run also probes for foreign traffic
before it starts; see "Attribution" below.

**The load shape defaults to the criterion's own figures — 550/s offered for 30 minutes and
judged against V-1's 500/s — and the
runner does not substitute the transaction cases' defaults for them.** Pass `RATE` or
`DURATION` explicitly for a shorter run, and the run announces that you did. The two
prerequisites do not go away with the load shape — a two-minute run is still measured off the
same process-global counters and still needs fixtures — so they travel with it:

```bash
set -a; . ./.env; set +a

ISOLATED_INSTANCE=1 ALLOW_FIXTURE_CREATION=1 \
  RATE=50 DURATION=2m \
  bash tests/loadtest/run_case.sh event-streaming
```

A shorter run's verdicts are real for the load it offered, which is not the load the criteria are
stated over. Only a default run certifies V-1 and V-3. `SMOKE=1` is the one form that drops both
prerequisites, and it drops the claim with them.

### Reading the verdict honestly

Three things about the output are worth knowing before relying on it.

`event_publish_verdicts_available` is the row to check FIRST. A run against a stack whose
observability was off reports three zeroes, two of which satisfy their `<` thresholds — so this
flag is what stops a stack that measured nothing from reporting a clean sweep. It goes to 0, and
the summary names the reason, when any of the following holds:

- either metrics scrape failed, or the window was not positive;
- **the instance was not attributable to this run** — see "Attribution" below;
- **a settling gate did not reach quiescence, or could not read the whole unsettled population** —
  see "The settling gates" below;
- a counter reset or an exporter restart split the window across two processes;
- no terminal events were seen, or **the first-attempt latency histogram had no observations**;
- **V-3's numerator could not be measured over the window** at all.

The p99 verdict is computed from `blnk_events_capture_to_dispatch_duration_seconds` and from
nothing else. `blnk_events_publish_duration_seconds` is reported beside it as a diagnostic-only
supporting figure, and so is the difference between the two — neither carries a threshold and
neither can satisfy V-1: publish duration's clock starts at the relay's claim, so it excludes the
time a row spends waiting to be claimed and reports its smallest figures for exactly the backlog
the criterion exists to catch. The difference of two independently ranked p99 values is **not** a
queue-wait percentile; read it as an order-of-magnitude indication of where the time goes, and see
`event_publish_p99_difference_seconds` below.

A third row deserves reading rather than skimming. When the sustained-throughput sampler is
running, the whole-run mean is a **diagnostic and not a verdict**, and the summary marks it `n/a`
rather than PASS or FAIL — because nothing is asserted about it in that configuration. It carries
the throughput verdict only when the sampler is off, which is what `RATE_WINDOW_SECONDS=0` asks
for and what any run too short to hold two windows gets. `verdicts_all_hold` in
`summary-event-streaming.json` is the single line to read: it is true only when every *certifying* row
holds and the inputs behind them were available.

A PASS on a row is only a claim about the run when `event_publish_verdicts_available` is 1. The
terminal block prints `ALL CRITERIA` as the single line that combines the two.

### The settling gates: what "settled" covers

Both ends of the measured window wait for the outbox to go quiet, because the pipeline is
asynchronous at both ends. Before the load, provisioning's `ledger.created` and `balance.created`
events are still queued and would otherwise be published inside the window and credited to the
load. After the load, the run's own last events are still in flight.

**The population that has to go quiet is every state from which a Kafka publish is still owed**,
and each of the four is there for a reason:

| State | Why it is un-settled |
|-------|----------------------|
| `pending` | captured and committed, not yet claimed by the relay |
| `processing` | claimed under a lease, not yet acknowledged by the broker — and the state a *stalled* relay holds every row in |
| `failed` | the retry budget is spent and **the dead-letter write is still owed**: the row has reached NO terminal counter, so it is missing from both sides of V-3's ratio |
| `replaying` | a dead-lettered row whose republish is in flight, which moves the dead-letter census as it completes |

`webhook_pending` is deliberately **not** in that population: such a row's Kafka leg is
acknowledged and already counted, and only the deprecated HTTP leg is owed, so waiting for it
would tie the acceptance window's length to the transport being retired.

Getting this wrong is not a rounding error. The excluded rows are *enriched* in dead letters —
most of all the ones whose retries are already exhausted — so a gate that waited on `pending`
alone closed the window immediately before the dead letters were written and reported a
flatteringly low rate together with a flatteringly low p99.

**It fails closed twice over.** Quiescence needs `DRAIN_STABLE_SAMPLES` (3 by default) consecutive
fresh readings whose TOTAL is at or below `DRAIN_FLOOR` (0 by default), at BOTH ends. And a
reading only counts when it covers every state: `GET /events/stats` returns all four and needs a
master key, while `blnk_outbox_pending` is `pending + processing` only and is therefore blind to
the failed tail and to a replay in flight. **Without a master key the run reports
`settling_population_complete: false` and withholds all three verdicts** rather than certifying a
window whose failing tail it never observed. `REQUIRE_DRAIN=0` relaxes both checks and is only
ever right for a smoke run.

### Attribution: the numbers belong to a run, or to nothing

Every verdict is a delta of **process-global** counters and a quantile over a process-global
histogram. `blnk_events_published_total` and its siblings carry `topic` and `event_type` and
nothing that identifies a workload — and they should not: a per-run label on a production counter
is unbounded cardinality in the server, paid for permanently to serve a benchmark.

So a Blnk instance serving anything else during the run contributes its events to the same
counters, and the distortion is not neutral. Foreign events **inflate throughput**, **enlarge the
dead-letter denominator** (lowering the rate) and mix the latency population — the passing
direction for two of the three verdicts. **Traffic from a shared or production deployment
invalidates certification: the results are not attributable to the invocation.**

The requirement is therefore enforced, not advised, in two independent ways:

1. `ISOLATED_INSTANCE=1` is your assertion that the deployment is dedicated to this run. Both the
   runner and the scenario refuse an acceptance run without it, before any load is offered —
   nothing observable distinguishes an idle foreign client from an absent one, so it has to be
   asserted.
2. An **idle probe** runs after the pre-load settling gate and before the baseline scrape: two
   scrapes `ISOLATION_PROBE_SECONDS` apart (10 by default) with no load offered. Any movement in
   the terminal event counters over that interval is an event this run did not produce, and more
   than `ISOLATION_MAX_FOREIGN_EVENTS` (0 by default) of them fails the preflight.

The probe can **disprove** isolation; it cannot prove it, because a bursty producer can be idle
for its duration. That is exactly why the acknowledgement is required as well, and why
`summary-event-streaming.json` records both halves under `supporting_figures.instance_isolation`.
`REQUIRE_ISOLATION=0` relaxes the requirement for a deliberately shared stack — the figures are
then still reported, `event_publish_instance_isolated` reads 0, and they must not be quoted.

### Where each verdict comes from

All three verdicts are read from the server's own instruments, never from k6's own timings —
`GET /metrics` for every one of them, with the master-key-gated `GET /events/stats` census as the
dead-letter numerator's second source when the counter is not exported. That is this guide's
opening distinction applied to the event pipeline: a fast k6 summary proves the API accepted work
quickly, and these three numbers are what the pipeline then did with it.

They are the run's results only under the two conditions above: an instance dedicated to the run,
and both settling gates having gone quiet over the whole unsettled population. Neither source
carries a workload dimension — `/metrics` labels events by topic and type, and the census counts
rows by status — so nothing downstream can separate this run's contribution from anything else's
after the fact.

- **Throughput**, in events/sec, is the delta of `blnk_events_published_total` over the measured
  window, divided by the interval the load was offered over. It is not k6's `http_reqs` rate: an
  arrival the executor could not start produces no event, a transaction the API rejects produces
  `transaction.rejected` instead, and provisioning's own `ledger.created` and `balance.created`
  events belong to no request in the window. **k6 iterations/sec is not events/sec.** Sustained is
  also a stronger claim than the average, so a second single-VU scenario samples the counter on a
  fixed cadence, and `event_publish_window_events_per_second` (`min`) and
  `event_publish_interval_events_per_second` (`p(50)`) are the rows that carry it.
  `event_publish_events_per_second` is the whole-run average, and it holds the verdict only on a
  run too short for the sampler to count a window — where it is reported as a diagnostic instead.
  The cadence is `RATE_WINDOW_SECONDS`, 30 seconds by default, and it is one number: it is the
  interval the sampler sleeps, the width each window is judged over, and the figure the summary
  reports. `SUBWINDOW_SECONDS` and `SAMPLE_INTERVAL_SECONDS` are accepted as aliases of it.
  Setting it to 0 switches the sampler off and withdraws its thresholds.
- **p99 publish latency**, in seconds, is the 0.99 quantile of
  `blnk_events_capture_to_dispatch_duration_seconds_bucket` filtered to `attempt="1"`,
  interpolated the way `histogram_quantile` would be. It is not `http_req_duration`, which
  measures API acceptance, and it is not `blnk_events_publish_duration_seconds`, which starts its
  clock at the relay's claim — that one is a diagnostic-only supporting figure. So is the gauge
  reporting the gap between the two, `event_publish_p99_difference_seconds`: it is the difference
  of two independently ranked p99 values, which is not the p99 of any difference, so read it as an
  order-of-magnitude indication of whether the end-to-end figure is dominated by the broker or by
  backlog — it is not a percentile of anything and it is not a queue wait.
- **Dead-letter rate** is the dead-lettered count divided by the sum of the dead-lettered and
  published counts over the window. The sum is the denominator because those two partition a
  captured event's terminal outcomes; dividing by the published count alone would report
  dead-letters as a fraction of successes. The numerator comes from ONE of two sources, decided
  before the arithmetic so the reported provenance and the number cannot disagree:
  - `blnk_events_dead_lettered_total`, differenced over the window, when the series is exported.
  - otherwise the **same-window delta** of `dead_lettered` from the master-key-gated
    `GET /events/stats` census: a baseline reading taken in `setup()` beside the baseline scrape,
    subtracted from the final reading taken in `teardown()`. The cumulative census value is never
    used on its own — it counts every dead letter the deployment has ever accumulated, and
    dead-lettered rows are not purged by retention, so it is a lifetime total rather than this
    window's count. A negative delta (rows left the state because of a replay, or because an
    operator resolved them) is refused rather than clamped to zero.

  An absent counter is **not** read as zero: when neither source yields a window delta the verdict
  is withheld and `event_publish_verdicts_available` goes to 0. `summary-event-streaming.json` records the
  baseline reading, the final reading and the delta under
  `verdicts.dead_letter_ratio.census_numerator`, so the arithmetic can be redone by hand.

  The stats probes ask for counts only (`include_offsets=false`), so a once-a-second drain poll
  never costs a broker round trip; add `?include_offsets=true` yourself when you want the offset
  reconciliation.

All three land in `summary-event-streaming.json` as custom metrics carrying k6 thresholds, with
the series each was computed from recorded as provenance under the top-level
`blnk_event_streaming` key. That
is what makes the dashboard render them as pass/fail rows with no change to anything under
`tools/`, and what lets a reviewer confirm which series produced which number.

**`verdicts_all_hold` is the single line to read.** It is true only when every verdict this run's
configuration applies held *and* `verdict_inputs_available` was 1, so it cannot be true for a run
that measured nothing. A verdict the configuration does not apply — the windowed sustained figure
on a run too short for the sampler to count a window, or the whole-run mean on a run where the
sampler did — is recorded as `null` and rendered `n/a`, and is excluded from the conjunction
rather than failing it. Every verdict that IS applied must carry a threshold: an entry that is
scored and registered no threshold fails, so a threshold accidentally dropped from
`verdictThresholds()` cannot promote a criterion to "not applicable".

None of it is measured unless `/metrics` is reachable. That needs `"enable_observability": true`
in `blnk.json` (or `BLNK_ENABLE_OBSERVABILITY=true`), and the endpoint is served on the API port
(default `5001`) and the worker monitoring port (default `5004`) — the relay runs in the server
role, so the server's endpoint is the one carrying these instruments. With `server.secure` enabled
it answers `403 Forbidden` when Blnk itself has no token configured, and `401 Unauthorized` when a
scrape arrives without one, so set `BLNK_METRICS_BEARER_TOKEN` on Blnk and give the run the same
value; the runner reads it from either that name or `METRICS_BEARER_TOKEN` and **exports it into
k6's environment rather than passing it on the command line** — `/proc/<pid>/cmdline` is
world-readable and CI logs capture spawned command lines, while `/proc/<pid>/environ` is readable
only by the process owner. The same applies to the master key and the API key. A refused scrape
does not fail loudly — it withholds the verdicts and names the scrape as the reason.
`docs/metrics.md` is the full catalogue and is not restated here.

**A master key is equally load-bearing, and for a different reason.** It is not corroboration: it
is what reads the `GET /events/stats` census, the only source that reports all four un-settled
outbox states. Without it the settling gates fall back to `blnk_outbox_pending`, which cannot see
the failed tail, and the run withholds every verdict rather than certifying a window whose tail it
could not observe. The runner takes it from `BLNK_SERVER_SECRET_KEY` or `MASTER_KEY` and warns when
neither is set. Credentials travel to k6 in the environment rather than on its command line, so
they do not appear in `/proc/<pid>/cmdline`, in a process-audit log or in CI diagnostics; a URL
carrying userinfo, a query token or a fragment is refused outright, and every endpoint the runner
prints is redacted first.

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

### For the four queue cases

For each of them, inspect both:

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

### For the event-streaming case

There is one artifact to read, `summary-event-streaming.json`. A second file,
`run-event-streaming.ndjson`, holds the raw k6 series and exists **only when the run was asked for
it** — `RAW_OUTPUT=1`, or `NDJSON_OUT=PATH`, which implies it; see the `RAW_OUTPUT` note above. Its
absence after a default run is the documented outcome, not a truncated run.
**There is deliberately no `queue-summary-event-streaming.json`** — the event pipeline has no asynq
queue, so nothing runs the queue benchmark and there is no drain time to compare against. Looking
for one is the most likely way to conclude a healthy run was incomplete.

Read it differently from the four above, because the fields that decide anything are not the k6
timings:

- `event_publish_verdicts_available` — check this **first**. While it is 0 the other three rows are
  not verdicts, and a stack whose observability was off reports zeroes that satisfy their own
  thresholds.
- `event_publish_events_per_second`, `event_publish_p99_seconds` and
  `event_publish_dead_letter_ratio` — the three verdicts, each carrying a k6 threshold so the
  dashboard renders it as a pass/fail row.
- `event_publish_window_events_per_second` (`min`) and
  `event_publish_interval_events_per_second` (`p(50)`) — these are what carry the *sustained*
  claim; the whole-run average holds the verdict only on a run too short for the sampler.
- the top-level `blnk_event_streaming` key — provenance, recording which `/metrics` series produced
  each number and which mode the run used, so a reviewer can confirm a figure was not produced by a
  smoke run or a relaxed threshold.

The equivalent of "queue duration is still high" here is the outbox backlog: a rising
`blnk_outbox_pending` with acceptable API latency means the relay is behind, not the API.
