# Blnk Metrics Reference

Blnk exposes OpenTelemetry metrics via a Prometheus-compatible `/metrics` endpoint. This document lists all available metrics, their types, attributes, and what they tell you operationally.

## Prerequisites

- Set `"enable_observability": true` in `blnk.json` (or `BLNK_ENABLE_OBSERVABILITY=true`)
- Metrics are served on:
  - **Server**: `GET /metrics` on the API port (default `5001`)
  - **Worker**: `GET /metrics` on the monitoring port (default `5004`)

## Authentication

When `server.secure` is enabled, the `/metrics` endpoint requires a bearer token:

1. Set `BLNK_METRICS_BEARER_TOKEN` (or `"metrics_bearer_token"` in `blnk.json`) on Blnk itself.
2. Give Prometheus the same token **through a file**, not inline:

```yaml
scrape_configs:
  - job_name: 'blnk-server'
    authorization:
      type: Bearer
      # Read from a mounted secret. Never write the token inline in this file.
      credentials_file: /etc/prometheus/secrets/blnk-metrics-token
    static_configs:
      - targets: ['server:5001']
```

The file holds the token and nothing else — no trailing newline is required, and Prometheus trims
surrounding whitespace. Mount it read-only and restrict it to the scrape process:

```bash
install -m 0600 /dev/null /etc/prometheus/secrets/blnk-metrics-token
printf '%s' "$BLNK_METRICS_BEARER_TOKEN" > /etc/prometheus/secrets/blnk-metrics-token
chown prometheus:prometheus /etc/prometheus/secrets/blnk-metrics-token
```

**Why `credentials_file` rather than `credentials`.** `prometheus.yml` is configuration, not a
secret store: it is normally committed to version control, rendered by a config-management tool,
templated into a ConfigMap, and readable by anyone who can read the config. An inline `credentials`
value puts a live token into all of those places at once. A file reference keeps the token in
whatever secret mechanism you already trust — a Kubernetes Secret, a mounted vault file, a
mode-0600 file placed by your provisioner — and leaves the scrape config safe to commit. It also
makes rotation a matter of replacing one file rather than re-rendering and redeploying the config.

> On Kubernetes, mount the Secret and point `credentials_file` at the mount path. Do **not** put the
> token in the Prometheus ConfigMap; a ConfigMap is not a Secret and is not treated as one.

If secure mode is enabled without a token configured, the endpoint returns `403 Forbidden`.

### The master key, for the API examples below

A few examples in this document call Blnk's own API, which authenticates with the master key. The key
is passed through a **curl config file** rather than as a `-H` argument, because anything in argv is
world-readable in `/proc` for the life of the process and lands in shell history and in any audit log
that records command lines:

```bash
umask 077
export BLNK_API="${BLNK_API:-http://localhost:5001}"
export BLNK_CURL_CONFIG="$HOME/.blnk-curl"
printf 'header = "X-Blnk-Key: %s"\n' "$(cat /run/secrets/blnk-master-key)" > "$BLNK_CURL_CONFIG"
```

Every `curl` in this document then reads it with `--config "$BLNK_CURL_CONFIG"`. It is the same shell
environment [kafka-operations.md](kafka-operations.md#keep-credentials-out-of-process-arguments)
establishes, so one setup serves both documents.

## Export Modes

| Mode | How it works | When active |
|------|-------------|-------------|
| **Pull (Prometheus)** | Prometheus scrapes `/metrics` | Always (when observability is enabled) |
| **Push (OTLP HTTP)** | Periodically pushes to an OTel Collector | When `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

## Metrics

> **Naming convention**: OTel instrument names use dots (e.g., `blnk.transaction.total`). The Prometheus exporter converts these to underscores automatically (e.g., `blnk_transaction_total`).

> **A family documented here is absent from `/metrics` until something records it.** OpenTelemetry
> exports a series for the attribute sets that have actually been written, not for every instrument
> declared, so a counter nobody has incremented since start-up — `blnk_subscribers_obligations_settled_total`
> on a deployment that has never had a broker obligation to settle, say — appears in this table and
> not in the scrape. That is the export working as designed, and it is why a rule reading an absent
> series is a real operational hazard rather than a theoretical one: it never fires, and it reports
> no error while not firing. Confirm a family exists by making the thing it counts happen, not by
> scraping an idle process and concluding it was never implemented. The gauges the event-metrics
> collector publishes are the exception — it writes every one of them on every tick, zeros and all
> five `reason` labels included, precisely so their absence means the collector stopped.

### Transaction Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_transaction_total` | Counter | `status`, `currency` | Total transactions by final status and currency. Use to track throughput and status distribution. |
| `blnk_transaction_duration_seconds` | Histogram | `status` | Wall-clock time of `RecordTransaction()` in seconds. Use to monitor processing latency and set SLOs. |
| `blnk_transaction_rejected_total` | Counter | `reason` | Rejected transactions by reason. Use to alert on elevated rejection rates. |

**`status` values**: `APPLIED`, `REJECTED`, `INFLIGHT`, `VOID`, `COMMIT`

**`reason` values**: `insufficient_funds`, `overdraft_limit`, `lock_contention`, `max_retries`, `other`

**`currency` values**: Determined by the transactions processed (e.g., `USD`, `NGN`, `EUR`)

### Queue Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_queue_enqueued_total` | Counter | `queue_name` | Transactions enqueued for async processing. Use to monitor inflow rate per queue. |
| `blnk_queue_processing_duration_seconds` | Histogram | `result` | Time spent processing a transaction in the worker. Use to detect worker slowdowns. |

**`queue_name` values**: `new:transaction_1` through `new:transaction_N` (sharded queues), `hot_transactions` (hot lane)

**`result` values**: `success`

### Balance Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_balance_created_total` | Counter | — | Total balances created. Use to track account growth. |

### Inflight Transaction Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_inflight_commit_total` | Counter | — | Inflight transactions committed. |
| `blnk_inflight_void_total` | Counter | — | Inflight transactions voided. Use the commit/void ratio to monitor authorization patterns. |

### Batch Coalescing Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_transaction_batch_size` | Histogram | — | Number of transactions per coalesced batch. Use to evaluate batching efficiency. |
| `blnk_transaction_batch_total` | Counter | `result` | Coalescing attempts by outcome. |

**`result` values**: `success`, `failure`, `skipped`

### Hot Pairs / Lock Contention Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_hotpairs_contention_total` | Counter | — | Lock acquisition failures. Spikes indicate contention on specific balance pairs. |
| `blnk_hotpairs_lane_routed_total` | Counter | `lane` | Transactions routed to queue lanes. Use to verify hot lane is activating as expected. |

**`lane` values**: `normal`, `hot`

### Worker Metrics

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_worker_retries_total` | Counter | `reason` | Worker retry events. Sustained retries indicate systemic issues. |

**`reason` values**: `insufficient_funds`, `other`

### Event Streaming Metrics

The outbox-to-Kafka event pipeline. Every ledger mutation writes an event row inside its own
database transaction; the relay publishes those rows, retries with bounded backoff, and
dead-letters an event that cannot be delivered.

**An event reaches a dead-letter topic by one of two routes, and the difference is operational.**

| Route | What happened | Retry budget | Log field |
|-------|---------------|--------------|-----------|
| Budget spent | Every attempt in the schedule failed transiently — a broker unreachable, a leader election, a timeout. The last attempt exhausted `max_attempts`. | Fully spent, all five attempts made | `terminal_reason=budget_spent` |
| Permanent failure | One attempt failed in a way no further attempt could change — an authorisation failure, a topic outside the namespace Blnk owns, an envelope over the size ceiling, bytes that are not valid JSON. | **Unspent.** The row is exhausted on the attempt that failed, which may be the first | `terminal_reason=permanent_failure` |

Both routes end on the same `<topic>.dlt` sibling and both increment
`blnk_events_dead_lettered_total`, so the counter and the age gauge cover the two together. The
distinction matters when you triage: a permanent failure with four attempts unspent has already
told you the retry schedule is irrelevant and sends you to an ACL, a topic name or a message
size, whereas a spent budget sends you to broker availability over the whole window. The
`terminal_reason` field on the relay's warning line is what separates them, and the row's own
`attempts` column corroborates it — a row sitting at `attempts=1` in a dead-letter inventory
failed permanently, it did not run out of tries.

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_events_published_total` | Counter | `topic`, `event_type` | Ledger events whose delivery is DURABLY RECORDED: the broker acknowledged the write **and** the outbox row was moved out of the claimable set to say so. Counted once per event, by the relay, at whichever transition made the Kafka leg durable. Replays and dead-letter writes are excluded, so this stays a count of first deliveries. Use to track delivery throughput per topic, and as one half of the dead-letter rate below. |
| `blnk_events_broker_acknowledgements_total` | Counter | `topic`, `event_type`, `purpose` | Publishes the BROKER acknowledged, counted by the publisher on every acknowledgement and for every purpose. This is traffic accepted, not events delivered: a republished event is counted again here, which is exactly what makes it useful beside the row above. Use to see real broker traffic including replays and dead-letter writes, and to read the gap described below. |
| `blnk_events_dispatched_total` | Counter | `topic`, `event_type` | Events whose outbox row has reached the TERMINAL `dispatched` state — the Kafka leg is durable **and** nothing further is owed for the row. Also once per event, so it is not a second opinion on the row above: it answers "how many events are completely settled", which for a row that still owes a legacy webhook happens later, by that leg's remaining budget. The two converge at the sunset, when the legacy leg and the `webhook_pending` state are removed. Use it to reconcile the outbox backlog; use `blnk_events_published_total` for throughput and for the dead-letter rate. |
| `blnk_events_publish_attempts_total` | Counter | `outcome`, `terminal` | Individual publish attempts, retries included, with exactly one `outcome` recorded per attempted write — so summing the series gives the total number of attempts. Use to read retry pressure independently of delivery volume, and to tell an event that is still being retried from one that is genuinely stuck — which is what `terminal` carries: the `outcome` vocabulary is frozen at three values, so a failed attempt that will be retried and one that will not are both `outcome="retrying"`, and `{outcome="retrying",terminal="true"}` is the series that answers "how many events are actually stuck". Its domain is closed at the two literals. |
| `blnk_events_publish_duration_seconds` | Histogram | `topic`, `attempt`, `outcome` | Claim-to-acknowledgement latency — the broker write in relative isolation. **Not the figure the latency target is read from, and never a fallback for it.** Its clock starts at the relay's claim, so it excludes the row waiting for the next poll tick, the poll interval and the claim query — a relay an hour behind reports the same sub-second p99 as an idle one. When the series in the row below is absent, the correct answer is that the target was not measured, not a figure from this one. |
| `blnk_events_capture_to_dispatch_duration_seconds` | Histogram | `topic`, `attempt` | END-TO-END age of a delivered event, from its capture in the transactional outbox to broker acknowledgement. **This is the series the sub-2s p99 target is read from**, filtered to `attempt="1"`. Only acknowledged publishes are recorded. **Do not subtract the row above from this one**: quantiles are not subtractive, so the difference of the two p99s is not the queue wait, has no interpretation as a duration and can be negative. Read the two side by side — high here with a low broker write is a relay backlog, both high is the broker — and read `blnk_outbox_pending` for the backlog itself. |
| `blnk_kafka_consumer_lag_unmeasured_partitions` | Gauge | `subscriber`, `group`, `topic` | Partitions whose offsets could not be read when a subscriber's lag was last measured. Non-zero means `blnk_kafka_consumer_lag` WITHHOLDS that topic, so the lag alert cannot fire for it. |
| `blnk_events_dead_lettered_total` | Counter | `topic`, `event_type` | Events diverted to a `<topic>.dlt` sibling by either terminal route — a spent retry budget or a permanent failure with budget to spare — attributed by the ORIGINAL category topic rather than the sibling so it divides against `blnk_events_published_total` without a name mismatch. Incremented once the `.dlt` write is acknowledged, so an event whose dead-letter write is still owed is not yet counted here; `blnk_dlt_oldest_message_age_seconds` covers it in the meantime because it reads the outbox rather than the broker. Use as the numerator of the dead-letter rate, and to see which event types are failing rather than only how many. |
| `blnk_dlt_oldest_message_age_seconds` | Gauge | `topic` | Age of the oldest unresolved dead-letter entry, per dead-letter topic, over every outbox row in the `failed` or `dead_lettered` state — which is both terminal routes and, deliberately, the rows whose `.dlt` write has not landed yet — measured from its last publish attempt. Use to alert on a triage backlog nobody is clearing; zero is reported explicitly when a topic's inventory is empty, and zero is the reading that clears the alert. |
| `blnk_kafka_consumer_lag` | Gauge | `subscriber`, `group`, `topic` | Committed offset behind the log end offset, summed across a topic's partitions and measured in-process. Use to alert on a subscriber falling behind — but read `blnk_kafka_consumer_lag_unmeasured_partitions` beside it, because a topic that was only partly measured is withheld from this series rather than reported at its partial sum. |
| `blnk_outbox_pending` | Gauge | — | Outbox rows not yet published, counted as **pending plus processing only** — claimed-but-unacknowledged rows are included, so a stalled relay holding every row under a lease cannot read as a drained backlog. It contains **no** terminal rows, so it cannot show retained `dispatched`, `failed` or `dead_lettered` rows accumulating. Use it for one question: is the relay keeping up? A rising figure against a flat publish rate is backlog, not throughput. |
| `blnk_events_repair_backlog` | Gauge | `leg` | Rows a REPAIR leg still owes — work the ordinary publish claim cannot reach. `leg="dead_letter"` is rows whose retry budget is spent and whose `<topic>.dlt` write also failed, each the only copy of an event that reached no topic at all; `leg="legacy_webhook"` is rows whose Kafka leg finished and whose legacy webhook enqueue never succeeded. **Disjoint from `blnk_outbox_pending`, not a subset of it**: those statuses are `failed` and `webhook_pending`, and that gauge counts only `pending` plus `processing` — so a repair backlog of half a million rows reads as a drained outbox there. Normally zero; it fills during an outage, all at once. |
| `blnk_events_repair_completed_total` | Counter | `leg` | Rows a repair pass drove to their destination: a dead letter acknowledged on its `<topic>.dlt` sibling, or a legacy webhook enqueued. Incremented per ROW that succeeded, never per claim and never per attempt, so `rate()` over it is the **drain rate** in rows per second. Divide the backlog above by it for time-to-clear. |
| `blnk_events_repair_saturated` | Gauge | `leg` | 1 when the last tick spent its whole repair budget with a full batch still coming back, 0 when the leg drained. It is the reading the backlog and the drain rate cannot give you: both fall while a recovery is merely slow, so this is what separates "recovering as configured" from "configured too slowly to recover". A sustained 1 is the signal to raise `RELAY_REPAIR_MAX_BATCHES_PER_TICK` or `RELAY_REPAIR_BATCH_SIZE`. |
| `blnk_events_purged_total` | Counter | — | Terminal event rows deleted by the retention sweep. Read it **against eligibility**, not on its own: a flat counter most often means nothing is older than the retention cutoff yet, and only otherwise means the sweep is disabled or stuck. The sweep deletes in batches, so a step-shaped increase is its normal signature rather than evidence of a misconfigured cutoff. |
| `blnk_subscribers_revocation_pending` | Gauge | — | Subscribers whose broker-side credential revocation is still owed. Normally zero. |
| `blnk_subscribers_oldest_revocation_age_seconds` | Gauge | — | How long the oldest outstanding revocation has been owed. Zero when nothing is owed. |
| `blnk_kafka_consumer_lag_pass_age_seconds` | Gauge | — | How long the consumer-lag rotation took to get back round the whole subscriber registry. This is the series `SubscriberLagCoverageStale` reads. It matters because a reading EXPIRES after ten minutes: a subscriber the rotation does not return to inside that retention has its `blnk_kafka_consumer_lag` series stop being exported, and an absent series breaches no threshold — so `SubscriberConsumerLagHigh` goes quiet for exactly the subscribers it can no longer see. |
| `blnk_kafka_consumer_lag_covered_subscribers` | Gauge | — | How many subscribers currently have an exported lag reading. Read it against `blnk_subscribers_registered` to see what proportion of the registry the rotation is actually covering; the remedy for a shortfall is a larger `EVENT_METRICS_SUBSCRIBER_BUDGET`, or narrowing over-broad grants so each subscriber costs fewer round trips per tick. The collection interval is fixed at 15 seconds and is not operator-configurable, so it is not a lever. |
| `blnk_kafka_subscribers_unmeasured` | Gauge | `reason` | Registered subscribers the last collection did not measure lag for **at all**, attributed by why. Non-zero means those subscribers have NO `blnk_kafka_consumer_lag` series, so `SubscriberConsumerLagHigh` cannot fire for them however far behind they fall. Normally zero. The `reason` domain is closed at five values and every one of them is written on every tick, zeros included, so a healthy collection is distinguishable from a stopped one: `budget` (the rotation has not got back to them, which is the only reason a configuration change fixes), `unprovisioned` (the row has no authorised topics, or identifiers the registry did not generate), `measure_failed` (the broker refused the measurement), `registry_failed` (the registry enumeration itself failed) and `topic_missing` (a row authorises a topic that does not exist). Each subscriber is counted under exactly ONE reason, so `sum without(reason)(…)` never exceeds `blnk_subscribers_registered`. Sum the reasons away for the total; read them apart to know what to do. See the note below — this is not the same condition as `blnk_kafka_consumer_lag_unmeasured_partitions`. |
| `blnk_subscribers_registered` | Gauge | — | Subscribers the registry holds. Published so the row above reads as a proportion, and so the measurement budget's headroom is visible before it is exhausted rather than only after. |
| `blnk_subscribers_measurement_budget` | Gauge | — | Subscribers **one consumer-lag sweep may measure**, as configured — `EVENT_METRICS_SUBSCRIBER_BUDGET`, alias `RELAY_SUBSCRIBER_METRICS_BUDGET`, default 200. Exported so headroom is a difference of two series: `blnk_subscribers_measurement_budget - blnk_subscribers_registered`, which goes negative before any subscriber goes unmeasured. It is the figure the sweep actually stops at rather than the configured value re-read, so it cannot disagree with the behaviour. Published on every tick, like the row above, so a reconfiguration is visible and a stopped collector is not mistaken for a small registry. |
| `blnk_subscribers_settlement_outstanding` | Gauge | — | Subscribers with a broker obligation — a revocation, a grant reconciliation or a credential cleanup — still to be settled. Read with `blnk_subscribers_obligations_settled_total`: `SubscriberSettlementNotProgressing` fires on outstanding work whose settlement rate is flat **or absent**, which is the signature of a stalled settler rather than a busy one. |
| `blnk_subscribers_oldest_settlement_age_seconds` | Gauge | — | How long the oldest unsettled obligation has stood. This is the series `SubscriberSettlementOutstanding` reads. Zero when nothing is outstanding. |
| `blnk_subscribers_revocation_failures` | Gauge | — | Subscribers whose last revocation attempt was REFUSED by the broker, as opposed to merely still owed. The distinction is the remedy: a pending revocation needs time, a refused one needs an operator, because it will not clear by retrying. Normally zero. |
| `blnk_subscribers_oldest_revocation_failure_age_seconds` | Gauge | — | How long the oldest refused revocation has stood. This is the series `SubscriberRevocationRefused` reads, so a deployment without it has that rule evaluating an absent series and never firing. Zero when nothing is refused. |
| `blnk_subscribers_credential_orphans` | Gauge | — | Subscribers carrying a credential Blnk could NOT persist a reference for or could not revoke — a principal that may authenticate at the broker while the registry cannot name its credential. Normally zero, and a non-zero value is unaccounted broker access rather than a lagging counter. |
| `blnk_subscribers_oldest_credential_orphan_age_seconds` | Gauge | — | How long the oldest orphaned credential has been outstanding. This is the series `SubscriberCredentialOrphaned` reads. Zero when there are none. |
| `blnk_subscribers_credential_cleanup_pending` | Gauge | — | Subscribers whose broker-side credential still has to be deleted after deregistration. It is part of `blnk_subscribers_settlement_outstanding`, reported apart because the remedy differs from a grant reconciliation. Normally zero. |
| `blnk_subscribers_grant_reconcile_pending` | Gauge | — | Subscribers whose broker ACL bindings no longer match their authorised topics and have not been reconciled yet. Also part of `blnk_subscribers_settlement_outstanding`. Normally zero; a persistent value means the settlement pass is not progressing, which is what `SubscriberSettlementNotProgressing` fires on. |
| `blnk_subscribers_obligations_settled_total` | Counter | — | Broker obligations the settlement pass has discharged. Read as a RATE beside the two gauges above: a non-zero backlog with a zero rate is a stalled pass, and so is a non-zero backlog with **no series here at all** — this family does not exist until the first obligation is settled. `SubscriberSettlementNotProgressing` covers both, which is why it is written as a set difference (`unless rate(…) > 0`) rather than a conjunction with `== 0`; see the settlement notes below. |
| `blnk_kafka_consumer_lag_inventory_complete` | Gauge | — | 1 when the last sweep measured every registered subscriber and left nothing unmeasured by a failure, 0 otherwise. This is the series `SubscriberLagCoverageIncomplete` reads, so a deployment without it has that rule evaluating an absent series and never firing. Read `blnk_kafka_subscribers_unmeasured` for how much and why. |
| `blnk_event_metrics_last_collection_age_seconds` | Gauge | — | How long ago the collector last RAN, whether or not it succeeded. This is the series `EventMetricsCollectionStale` reads, and `EventMetricsCollectionAbsent` alerts on its absence scoped to `job="blnk-server"`. Every gauge on this page is refreshed by that collector, so a rising value means every one of them is stale. |
| `blnk_event_metrics_last_success_age_seconds` | Gauge | — | How long ago the collector last SUCCEEDED. The distinction from the row above is the whole point: a collector still ticking while every collection fails keeps one figure flat and the other rising, and this is the series `EventMetricsCollectionFailing` reads. |
| `blnk_event_metrics_collection_failures_total` | Counter | `collection` | Failed collections, attributed by which one. The `collection` domain is closed at seven values, one per independent collection plus the registry enumeration: `outbox_backlog`, `dead_letter_age`, `subscriber_revocations`, `subscriber_lag`, `subscriber_listing` (the registry enumeration itself, which fails as a whole rather than per subscriber), `subscriber_settlement` and `subscriber_access_residue`. No error text and no identifier ever reaches the attribute, so the series count is a property of the code rather than of whatever failed. Read it to see WHICH dependency is failing while the two age gauges say only that something is. |

**Why `blnk_events_published_total` is counted at the row transition and not at the broker
acknowledgement.** Delivery is at-least-once by construction: the relay publishes, then marks the
outbox row dispatched, and a crash or a database failure between those two steps deliberately leaves
the row claimable so the event is published *again* — losing it is unrecoverable, while a duplicate
is suppressed at the subscriber on `event_id`. Counted at the acknowledgement, that second write
increments the counter a second time for one event, so a series documented as "one per event"
silently becomes "one per successful write" exactly when the pipeline is having trouble. Because this
counter is simultaneously the V-1 throughput numerator and the V-3 dead-letter-rate denominator, that
error inflates throughput and understates the dead-letter rate together. The dispatched transition is
conditional on the claim token and clears it, so it completes for one worker once per event; the
webhook-pending transition, which records the Kafka leg for a row whose legacy webhook is still owed,
does the same and is counted too.

> The consequence to know when reading it: **it lags the wire by one bookkeeping statement**, and an
> event that reached Kafka but whose row could not be marked is not counted until the republish
> settles. That is the correct trade — the alternative over-counts.

**`outcome` values**: `dispatched`, `retrying`, `dead_lettered`

The vocabulary is closed at exactly these three values — requirement R-3 fixes it — and is
mutually exclusive: exactly one value per attempted write. `dispatched` is a broker
acknowledgement of the original write. `retrying` is **every** failed attempt, whether or not
another one follows: "retrying" describes the attempt's place in the sequence, and whether the
sequence continues is decided by the row's own budget in SQL rather than by the publisher.
`dead_lettered` is an acknowledged write to a `<topic>.dlt` sibling, which is the terminal
outcome of the original event.

> A fourth value, `failed`, was published here briefly to separate "failed, nothing further
> will be tried" from "failed, another attempt follows". It was **removed**: the label domain
> is a published interface, so widening it silently changes what every existing dashboard
> selection and alert rule matches. Any query that selected `outcome="failed"` should select
> `outcome="retrying"` instead, and read the terminal population from
> `blnk_events_dead_lettered_total` and the outbox statistics endpoint — which is where the
> answer to "how many events are actually stuck" has always been authoritative, because a
> stuck event is one whose ROW is stuck, not one whose last attempt happened to be
> unrecoverable.

**`attempt` values**: `1`, `2`, `3`, `4`, `5`, `over`, `replay`, `dead_letter`

A deliberately closed domain, because this attribute sits on a histogram and its cardinality
is multiplied by the bucket count. The numbers are the attempt number of an original publish,
capped at the retry budget — `RELAY_MAX_RETRY_ATTEMPTS` is clamped to 5, and a row whose
`max_attempts` was raised past that collapses to `over` rather than minting a new label. A
replay and a dead-letter write belong to no retry sequence, so they carry fixed tokens instead
of a number; that is what keeps them out of the `attempt="1"` population the latency target is
read from. `blnk_events_capture_to_dispatch_duration_seconds` uses the same domain minus
`dead_letter`, which is never an end-to-end delivery.

**`purpose` values**: `original`, `replay`, `dead_letter`

Carried only by `blnk_events_broker_acknowledgements_total`, and a closed set of three: a first
delivery of an event to its category topic, an operator-triggered re-publication of a
dead-lettered event, and a write to a `<topic>.dlt` sibling. An unstated purpose is recorded as
`original`, which is what the ordinary domain call site submits, and an unrecognised one is
collapsed to `original` with a warning naming it rather than being passed through — the purpose
selects a label value, so accepting arbitrary strings would reopen the domain it exists to
close. `original` is the only purpose the relay publishes under and therefore the only one that
can appear in `blnk_events_published_total`.

**`topic` values**: the five category topics `blnk.transactions`, `blnk.balances`,
`blnk.identities`, `blnk.ledgers`, `blnk.system`, and their five dead-letter siblings
`blnk.transactions.dlt`, `blnk.balances.dlt`, `blnk.identities.dlt`, `blnk.ledgers.dlt`,
`blnk.system.dlt`

Every one of those names derives from `KAFKA_TOPIC_PREFIX`, whose default is `blnk`; both the
un-prefixed and the `BLNK_`-prefixed form of that variable are accepted. Set a different prefix
and the whole inventory moves with it, labels included. A namespace declared in
`KAFKA_HISTORICAL_TOPIC_PREFIXES` is reported verbatim too, so a generation still draining after
a rename stays visible per topic rather than disappearing into the collapse label — which is
exactly the traffic an operator managing that migration is watching. The series count stays
bounded because that allowlist is bounded at four entries. A topic name Blnk does not own never
reaches a label: it collapses to `unowned` on the counters and histograms, and to `other` on
the two consumer-lag gauges. The series count therefore stays bounded while the anomaly stays
visible as a non-zero count on that one label, and the rejected name remains on the outbox row
and in the log line, which is where an operator triaging a single event looks.

Two instruments use `topic` in opposite directions, and both are deliberate.
`blnk_events_dead_lettered_total` carries the **original category** topic, so that it divides
against `blnk_events_published_total` cleanly; `blnk_dlt_oldest_message_age_seconds` carries
the **`.dlt` sibling**, because what it measures is what is waiting on a dead-letter topic.

### Three counters, three questions

`blnk_events_broker_acknowledgements_total`, `blnk_events_published_total` and
`blnk_events_dispatched_total` are easy to read as three spellings of one number. They are not.
Each is incremented at a different moment in one event's life, and the gaps between those
moments are where the diagnostics live.

```
publish ──► broker ack ──────► Kafka leg recorded ─────────► row terminal
            acknowledgements     published                    dispatched
            (every purpose)      (once per event)             (once per event)
```

- **acknowledgements** counts WRITES. Every purpose is counted — an original publish, a replay,
  a dead-letter write — because it measures traffic the broker accepted. A republished event is
  counted again, which is exactly what makes it useful beside the next one.
- **published** counts EVENTS, at the moment the claim-token-conditional statement recording the
  Kafka leg commits. That statement clears the token, so it succeeds for one worker once in an
  event's whole life: a republish after a crash increments acknowledgements again and cannot
  increment this. **Anything per-event is read from here** — the V-1 throughput verdict and the
  V-3 dead-letter rate both are.
- **dispatched** counts EVENTS reaching the terminal row state. For a row that owes a legacy
  webhook the Kafka leg is durable while the row sits in `webhook_pending`, so this one lags by
  that leg's remaining budget and is counted on the later pass that settles it — once, whether
  that pass delivers the webhook or abandons it. It answers "how many events are completely
  settled", which is the figure the outbox backlog is reconciled against.

So the difference between `published` and `dispatched` is the population still owed a webhook,
and it goes to zero at the sunset. The difference between `acknowledgements` and `published` is
re-delivery, and it is the one to alert on:

```promql
# Redelivery factor. 1.0 means every original write was a first delivery.
sum(rate(blnk_events_broker_acknowledgements_total{purpose="original"}[5m]))
  / sum(rate(blnk_events_published_total[5m]))
```

**`event_type` values**: the catalogued event names, reported verbatim

The catalogue — every event name, the topic it routes to and what each one means — is owned by
[event-streaming.md](event-streaming.md#event-types) and is not duplicated here. An event type
that catalogue does not recognise collapses to `unrecognised`; a non-zero count on that label
is the signal that something is publishing an uncatalogued event, and the publisher logs a
warning naming it.

**`subscriber` and `group` values**: a stable, truncated SHA-256 pseudonym of the subscriber
identifier and of its consumer-group namespace — never a customer-chosen name. A measurement
with no subject reports `unattributed`, and one whose subject the registry does not admit
reports `unregistered`; neither is hashed, because neither is anyone's name. See the note below
on why these are pseudonyms and how to resolve one back to a subscriber.

**An acknowledgement is not a delivery, and the gap between the two counters is a diagnostic.**
`blnk_events_broker_acknowledgements_total` moves when the broker accepts a write;
`blnk_events_published_total` moves when the outbox row is marked to say the delivery is
durable. Delivery to Kafka is at-least-once, so those are different facts separated by a
database round trip that can fail — and the counter was previously incremented on the
acknowledgement, which meant an event the broker accepted and a dying relay never marked was
published again and counted twice. Read them as a pair:

| Reading | What it means | What to do |
|---------|---------------|------------|
| acknowledgements ≈ published (filtered to `purpose="original"`) | Healthy. Nearly every accepted write is being recorded. | Nothing. |
| acknowledgements **above** published, and the difference growing | Rows are reaching Kafka and not being marked — a failing or slow outbox write path. Every one of those events **will be published again** when its lease expires, and subscribers are relying on `event_id` suppression to absorb it. | Look at the relay's `the event was published but its outbox row could not be marked dispatched` error line, and at database availability. |
| published **above** acknowledgements | Not a pipeline condition. It is an instrumentation defect: nothing can be durably recorded without having been acknowledged first. | Treat as a bug in the metric wiring. |

A modest standing difference is normal and is not the failure case: replays and dead-letter
writes are counted as acknowledgements and never as deliveries, so filter to
`purpose="original"` before comparing.

Do **not** substitute acknowledgements for published in the dead-letter rate. That rate is
stated over terminal outcomes, each event counted once; the acknowledgement counter counts
attempts the broker accepted, so a republished event or a triage afternoon would move the
denominator and make the rate depend on how much re-delivery happened rather than on how the
pipeline behaved.

**Every gauge here has a single owner.** `blnk_dlt_oldest_message_age_seconds`,
`blnk_outbox_pending`, `blnk_kafka_consumer_lag`, its unmeasured-partitions companion and the
two subscriber-coverage gauges are all fed by the periodic collector in the server role and by
nothing else, which re-reads them from authoritative state on every tick. A synchronous gauge retains its last value, so the
explicit zero the collector records is the measurement that clears an alert; omitting it is
what leaves a resolved incident firing for ever.

The two consumer-lag gauges are **asynchronous**, which changes what "clearing" means for
them. The exporter invokes a callback at collection time and publishes exactly the subscribers
that callback observes, so a subscriber that leaves the registry, has its grant narrowed or has
its consumer group reissued simply stops being exported rather than lingering at its last
reading. That is deliberate: subscriber churn would otherwise grow the series count without
bound, since there is no way to delete an attribute set from a synchronous gauge.

**Consumer lag is measured in-process, so no external lag exporter is required.** The collector
differences each subscriber group's committed offsets against the log end offsets it reads from
the broker, sums them per topic, and publishes the result as the inventory the two asynchronous
gauges are observed from. There is no `kafka_exporter`, no Burrow and no sidecar to deploy or
keep in step with the registry, so an absent lag series is never a missing exporter.

**Do not read an absent lag series as proof the collector is down.** The two asynchronous lag
gauges are legitimately absent in three healthy situations — no subscribers registered, no
brokers configured, and a sweep that did not reach a given subscriber — and absence cannot tell
those apart from a stopped collector. `blnk_event_metrics_last_collection_age_seconds` is the
signal that answers that question, and it is published unconditionally by any running collector,
so:

- The age gauge **present and near zero**, lag series absent → the collector is running and
  there is genuinely nothing to report, or coverage has not reached those subscribers. Read
  `blnk_kafka_consumer_lag_inventory_complete` to tell those two apart.
- The age gauge **present and rising** → the loop has stopped; every gauge in the table above is
  frozen at its last reading.
- The age gauge **absent from the `blnk-server` job** → no collector is running at all, and every
  rule in `alerts/blnk-kafka-alerts.yml` is evaluating a series that does not exist. That is what
  `EventMetricsCollectionAbsent` fires on. Absence from the `blnk-worker` job is expected and
  correct: the collector is server-only, because two processes writing the same gauges would each
  retire the other's series as stale.

**`blnk_outbox_pending` counts captured events only, and some events are captured one step
removed.** An event captured from an intent recorded atomically with its mutation — a bulk batch
coordinator row, or a balance-monitor handoff left over from a release that predates the
in-transaction alert capture — is OWED while that intent is outstanding: it exists in no outbox
status, and therefore in no gauge here. There is deliberately no instrument for it: an owed event
is not a backlog, and a gauge that summed the two would make a healthy poll interval of batch
finalisation look like relay lag. The outstanding counts are reported by `GET /events/stats` under
`producer_atomicity`, and
[kafka-operations.md](kafka-operations.md#producer_atomicity--events-that-are-owed-and-not-yet-in-the-table-at-all)
states which of them are actionable.

**Retention is off by default.** `blnk_events_purged_total` stays at zero until
`RELAY_EVENT_RETENTION_DAYS` is set to a positive number of days.

Diagnose accumulation with the right series. `blnk_outbox_pending` **cannot** show it: that gauge
holds only pending and processing rows, so retained `dispatched`, `failed` and `dead_lettered` rows
are invisible to it and it stays flat while the table grows. Query terminal status counts instead —
`GET /events/stats`, or `SELECT status, count(*) FROM blnk.event_outbox GROUP BY status;` — and
compare against the table's size on disk. That growth matters because every retained row holds a
verbatim copy of its webhook body.

The endpoint takes the `dispatched` count only when asked, because it is an exact `COUNT` over the
one population that grows without bound: send `GET /events/stats?include_offsets=best_effort`, and
read `dispatched_history_counted` before reading `dispatched`, since an absent key means "not
counted" rather than "none". The other statuses need no parameter — they are exact on every reading.

**Subscriber and group labels are pseudonyms, not names.** `blnk_kafka_consumer_lag` carries a
truncated SHA-256 of the subscriber identifier and of its consumer-group namespace rather than
the values themselves. A metric label is the most widely copied value in an observability
stack — it reaches Prometheus, every dashboard, every alert notification and whatever
long-term store the series are federated into — so publishing a tenant-ish name there spreads
it into all of them by default. The hash is stable, so a rate and a threshold behave
identically and distinct subscribers stay distinct.

**Resolving a pseudonym: one request, whatever the registry's size.** `GET /subscribers` accepts
the token as a filter and resolves it server-side, walking the whole registry rather than the
page the caller happens to ask for:

```bash
curl -sS "$BLNK_API/subscribers?subscriber_id_hash=$TOKEN" \
  --config "$BLNK_CURL_CONFIG" \
  | jq -r '.data[] | "\(.subscriber_id)\t\(.kafka_principal)\t\(.consumer_group_id)"'
```

Three answers, and they mean different things:

Every reading of `GET /subscribers` answers with an object carrying `data`, this one included, so
read `.data[]` rather than the body as an array. The keys beside `data` vary by reading: this one
carries `total_count` and `has_more` and no `next_cursor`, because it resolves the whole registry
rather than a page, which is also why its `total_count` needs no `include_count`.

| Response | Meaning |
| --- | --- |
| A one-element `data` | Resolved. |
| An empty `data` with **200** | The registry was searched to its end and holds no subscriber with that token. |
| **500** `GEN_INTERNAL` | The registry is larger than the resolver's bound (100 pages of 100 rows), so the answer is **unknown, not negative**. Query `blnk.event_subscribers` directly. |

`limit` and `cursor` are **refused** on this reading with `400 GEN_VALIDATION_ERROR`: a resolution
returns at most one row, so there is no page to bound or to resume from.

Do **not** resolve a token by fetching one page and hashing the rows locally. That was the
documented procedure before the filter existed and it is wrong in a way that produces no error:
it answers correctly only while the whole registry fits in the page requested, and past that it
returns "no match" for subscribers it never read.

Every subscriber row also carries its own `subscriber_id_hash`, so the mapping is readable
without recomputing anything. The rule, if you need it outside the API — the same one the
metric label, the log field and the response field all use — is SHA-256 over the exact
identifier bytes, hex, first 16 characters:

```bash
printf '%s' "$SUBSCRIBER_ID" | sha256sum | cut -c1-16
```

`printf` rather than `echo`: a trailing newline changes the digest, and `echo` adds one.

Service logs carry the same token in their `subscriber_id_hash` field and consumer-group
pseudonyms in `consumer_group_hash`, so a log line, a metric series and a registry row all
pivot on one value. The `topic` label is deliberately **not** hashed: it names one of the four
fixed, published, Blnk-owned category topics, so it identifies nobody.

**A non-zero `blnk_subscribers_revocation_pending` means a live credential is unaccounted
for.** Provisioning writes a subscriber's SCRAM credential before its ACL bindings, so a
binding failure is compensated by revoking the credential. When that compensation also fails,
a live SASL credential exists at the broker for a principal the registry records no issuance
for. That obligation is recorded on the subscriber row, and these two gauges are what make it
visible.

Alert on the **age**, not the count. A marker cleared within a minute by a retry is routine:
the automatic settlement paths — re-issuing a credential, which replaces the orphan by
construction, or deprovisioning the subscriber, which revokes it — are working. A marker
outstanding for hours means neither has been exercised and the principal has to be revoked by
hand with `kafka-configs --alter --delete-config SCRAM-SHA-512 --entity-type users
--entity-name <principal>`. The age is measured from when the obligation was **first**
recorded and is never reset by a later failed attempt, so it reports the age of the exposure
rather than the age of the last try.

**Finding the affected subscribers: ask the registry API.** The alert deliberately carries no
subscriber label — that would export a tenant identifier into every notification — so the
responder's first step is to ask which rows are affected. One master-key-gated request answers it:

```bash
curl -sS "$BLNK_API/subscribers?revocation_pending=true" --config "$BLNK_CURL_CONFIG"
```

Three properties of that reading matter during an incident:

- **It scans the whole registry, not a page.** `limit` and `cursor` are *refused* rather than
  ignored, because a truncated list of live unaccounted-for credentials reads exactly like a
  complete one — a responder acting on it would revoke those, close the incident, and leave the
  rest authenticating. The total in the envelope is therefore exact.
- **It is ordered oldest obligation first**, which is the same order the alert fires on, so you
  work the longest exposure first.
- **A walk that could not cover the registry is an error, never a shorter list.** It answers `500`
  with `GEN_INTERNAL` naming the reason, and *that* is when you fall back to the SQL below.

Every subscriber in the response carries the three markers directly — `revocation_pending` and
`revocation_pending_reason`, plus the instants `revocation_pending_at`, `revocation_failed_at` and
`credential_orphaned_at`, each present only when it is set. `kafka_principal` and
`consumer_group_id` are on the same row, so the response names the principal to revoke.

**The two other markers have no filter of their own.** `credential_orphaned_at` is stamped by a
failed *issuance* and `revocation_failed_at` by a broker that *refused* a revocation, and neither is
what `revocation_pending=true` selects. Read them from an ordinary `GET /subscribers` page, or query
them directly — which is also the fallback for the incomplete-walk case above:

```sql
SELECT subscriber_id, kafka_principal, consumer_group_id,
       revocation_pending_at, revocation_failed_at, credential_orphaned_at,
       now() - LEAST(
         COALESCE(revocation_pending_at,   'infinity'),
         COALESCE(revocation_failed_at,    'infinity'),
         COALESCE(credential_orphaned_at,  'infinity')
       ) AS outstanding_for
FROM blnk.event_subscribers
WHERE revocation_pending_at IS NOT NULL
   OR revocation_failed_at IS NOT NULL
   OR credential_orphaned_at IS NOT NULL
ORDER BY outstanding_for DESC;
```

The service logs carry the same information: the revocation-failure entry names the principal. The
full triage and hand-revocation procedure is in
[kafka-operations.md](kafka-operations.md#subscriberrevocationoutstanding), and the `$BLNK_API` and
`$BLNK_CURL_CONFIG` above are established by
[Keep credentials out of process arguments](kafka-operations.md#keep-credentials-out-of-process-arguments).

**The settlement gauges describe a registry that has drifted from the broker.** A subscriber's
state lives in two systems that cannot be written atomically: the registry row in PostgreSQL, and
the principal, credential and ACL bindings at the broker. A request that fails part-way leaves
them disagreeing — an authorization change that pruned the broker and then could not persist, a
credential written whose compensating revocation also failed, a credential revoked whose registry
record could not be cleared. Each of those is recorded as a durable obligation on the subscriber
row, and a background pass in the server role discharges them.

Read them in this order. `blnk_subscribers_credential_cleanup_pending` first, because it is the
security-relevant half: it means a credential may exist beyond what Blnk records. Then
`blnk_subscribers_grant_reconcile_pending`, which means a subscriber's enforced access may not
match its `authorized_topics`. The split matters precisely because the two need different
responses, which is why the total alone is not enough.

Alert on the **age**, for the same reason as the revocation gauges: an obligation discharged
within a minute is the mechanism working, and a count would alert on every one of those. An hour
means either the pass is not running or the broker has been unreachable throughout. The age is
measured from when the obligation was **first** recorded and is never reset by a later failed
attempt, so it is the age of the divergence rather than of the last try.

`blnk_subscribers_obligations_settled_total` answers the question the gauges cannot. A flat
backlog looks identical whether the pass is settling nothing or settling exactly as fast as new
obligations arrive; a rate over the counter separates those. A zero rate with a non-zero backlog
is a stuck pass — check that the server role logged `subscriber settlement processor started`,
which it declines to do when `KAFKA_BROKERS` is empty.

**No rate at all is the same finding, and `SubscriberSettlementNotProgressing` is written to say
so.** This counter is subject to the absent-family note at the top of this page: it has no series
until the pass discharges its first obligation, so a deployment whose settler has never worked
exports nothing here. The rule is therefore `blnk_subscribers_settlement_outstanding > 0 unless
rate(…) > 0` rather than `… and rate(…) == 0`. `and` against an absent series yields an empty
result, which left the rule unable to fire on exactly the deployment it exists for; `unless`
returns the left-hand side when the right has nothing to match, so an outstanding backlog alerts
before this counter's first sample. Matching is unchanged — `unless` pairs on the full label set,
so a backlog is still compared with its own process's settle rate — and the alert's `$value` is
still the backlog, because `unless` returns the left side.

**A non-zero `blnk_kafka_subscribers_unmeasured` means part of the lag signal does not exist.**
Measuring lag costs two broker round trips per authorised topic and produces a retained series
per subscriber-topic pair, so the collector applies a cardinality budget — default 200
subscribers per tick — and stops enumerating the registry when it is reached. A subscriber past
that point is not measured approximately: it has **no** `blnk_kafka_consumer_lag` series at all,
so `SubscriberConsumerLagHigh` has nothing to evaluate and the subscriber can fall arbitrarily
far behind while every dashboard reads clean. Silence and health being indistinguishable is the
worst property a monitoring system can have, which is why this is published as a metric rather
than left in a log line.

**That budget has TWO accepted environment names, and they are ONE knob.**
`EVENT_METRICS_SUBSCRIBER_BUDGET` and `RELAY_SUBSCRIBER_METRICS_BUDGET` both set it; both were
published, so both are honoured, and configuration reconciles them onto a single value before the
collector reads it. Set either. If you set both to DIFFERENT values,
`EVENT_METRICS_SUBSCRIBER_BUDGET` wins — it is the field the collector reads and the one the
supported ceiling clamps — and start-up logs a warning naming both variables and the value
applied. The alert remediations below and in `alerts/blnk-kafka-alerts.yml` name
`EVENT_METRICS_SUBSCRIBER_BUDGET`; `.env.example` and `blnk-config.yaml` ship the other. They are
the same ceiling, not two.

Read it beside `blnk_subscribers_registered`, which is why that gauge exists: three unmeasured
out of five is a different situation from three out of three thousand, and the pair shows the
budget's headroom **before** it is exhausted rather than only after.

It is **not** the same condition as `blnk_kafka_consumer_lag_unmeasured_partitions`, and the two
need different fixes. That one is per-subscriber and describes a subscriber the collector *did*
reach whose topic had unreadable partitions — cluster metadata is the thing to repair. This one
describes a subscriber with NO reading at all — and the `reason` attribute says which of five
causes it is, so read the series broken out rather than only summed. Each subscriber is counted
under exactly one reason, which is what makes the sum comparable with the registry size:

- **`budget`** — the rotation has not got back to this subscriber inside the reading's ten-minute
  retention. The usual case on a large registry, and the ONLY one a configuration change fixes:
  raise `EVENT_METRICS_SUBSCRIBER_BUDGET` (or its alias `RELAY_SUBSCRIBER_METRICS_BUDGET`) above
  `blnk_subscribers_registered` and restart the server role, remembering that each additional
  subscriber costs broker round trips per tick and a retained series per authorised topic.
- **`measure_failed`** — the broker refused the measurement. An ACL or connectivity fault; raising
  the budget does nothing. Look for `measuring consumer lag for subscriber` in the log.
- **`registry_failed`** — the enumeration itself failed, so the rows past the failure were never
  reached. Look for a `listing event subscribers` or `counting event subscribers` failure in the
  event-metrics log line.
- **`topic_missing`** — the row authorises a topic that does not exist. Provision it
  (`make kafka_provision`) or correct the row.
- **`unprovisioned`** — the row has no authorised topics, or carries identifiers the registry did
  not generate. This is the natural state immediately after `POST /subscribers` and needs a grant,
  not a budget.

**No broker configured at all** reports the whole registry under `measure_failed` or
`unprovisioned` depending on how far the sweep got, and that is the expected reading for a
deployment running without Kafka rather than a fault.

**Alerting on these series.** `alerts/blnk-kafka-alerts.yml` defines one rule group,
`blnk-kafka-alerts`, evaluated every 30 seconds:

| Alert | Expression | Dwell | Severity |
|-------|-----------|-------|----------|
| `DeadLetterMessageStuck` | `blnk_dlt_oldest_message_age_seconds > 900` | `0m` | critical |
| `SubscriberConsumerLagHigh` | `blnk_kafka_consumer_lag > 10000` | `2m` | warning |
| `SubscriberRevocationOutstanding` | `blnk_subscribers_oldest_revocation_age_seconds > 3600` | `0m` | critical |
| `SubscriberCredentialOrphaned` | `blnk_subscribers_oldest_credential_orphan_age_seconds > 3600` | `0m` | critical |
| `SubscriberRevocationRefused` | `blnk_subscribers_oldest_revocation_failure_age_seconds > 900` | `0m` | warning |
| `SubscriberSettlementNotProgressing` | `blnk_subscribers_settlement_outstanding > 0 unless rate(blnk_subscribers_obligations_settled_total[30m]) > 0` | `30m` | warning |
| `SubscriberSettlementOutstanding` | `blnk_subscribers_oldest_settlement_age_seconds > 3600` | `0m` | critical |
| `ConsumerLagMeasurementDegraded` | `blnk_kafka_consumer_lag_unmeasured_partitions > 0` | `5m` | warning |
| `SubscriberLagCoverageStale` | `max_over_time(blnk_kafka_consumer_lag_pass_age_seconds[30m]) > 600` | `0m` | warning |
| `SubscriberLagCoverageIncomplete` | `blnk_kafka_consumer_lag_inventory_complete == 0` | `30m` | warning |
| `EventMetricsCollectionStale` | `blnk_event_metrics_last_collection_age_seconds > 120` | `2m` | warning |
| `EventMetricsCollectionFailing` | `blnk_event_metrics_last_success_age_seconds > 300` | `0m` | warning |
| `EventMetricsCollectionAbsent` | `absent(blnk_event_metrics_last_collection_age_seconds{job="blnk-server"})` | `10m` | critical |

900 is the 15-minute dead-letter dwell threshold, written as a literal so that it is greppable,
and it carries the whole delay — which is why that rule's own dwell is `0m` rather than
omitted. The two lag rules are a pair: the threshold rule reads the gauge that carries only
complete measurements, and the degradation rule covers the gap withholding leaves, so a broker
or metadata fault surfaces as a measurement failure instead of silently resolving a firing
alert.

Thirteen rules, and they divide in two. The first SEVEN are CONDITION rules — they fire on
something being wrong in the pipeline. The last SIX are MEASURABILITY rules, and they exist
because every condition rule reads a gauge the collector publishes: if the collector is stale,
failing or absent, or if the lag rotation cannot get round the registry, every condition rule
goes quiet and quiet is indistinguishable from healthy. `EventMetricsCollectionAbsent` is the
outermost of them and is scoped to `job="blnk-server"`, because the relay and its collector run
in the server role — renaming that scrape job silences the rule permanently rather than making
it fire.

The count is worth stating because it is checkable: `alerts/blnk-kafka-alerts.yml` holds
thirteen rules in one group, and `http://localhost:9090/rules` must show thirteen under
`blnk-kafka-alerts` with a 30-second interval. Fewer means the file loaded partially, and a rule
that never loaded reports no error anywhere.

Every rule resolves to its OWN procedure. Each `runbook_url` is an absolute
[kafka-operations.md](kafka-operations.md) URL with a fragment naming the rule, so a responder
lands on that rule's section rather than at the top of the document. The base URL is
overridable: set `runbook_base_url` in `global.external_labels` in `prometheus.yml` to point at
an internal mirror, and every rule follows it.

> **A rule file is inert unless `prometheus.yml` lists it.** The `rule_files:` stanza is what arms
> the rules, not the mere presence of the file. It resolves a glob against the directory the
> Compose stack mounts `./alerts` into, so a rule file added there later needs no further edit
> here — but a deployment that mounts the directory elsewhere must adjust the glob. Verify at
> `http://localhost:9090/rules`, not at `/targets`: a target can show UP while the rules never
> loaded, and a rule that never loaded reports no error.
>
> **The scrape has to succeed for any of this to work.** With `metrics_bearer_token` set and no
> matching `authorization` block on the Prometheus job, every scrape is refused, every series
> above is absent, and each rule sits permanently unable to fire while reporting nothing wrong.
> `prometheus.yml` carries the two steps that enables. The Kubernetes deployment keeps a second
> copy of both this configuration and the rule file in
> `infrastructure/k8s-manifests/prometheus-configmap.yaml`; edit the two together, or one
> environment alerts and the other does not.
>
> **On Kubernetes there is one more thing to assert, and `/rules` cannot see it.** That copy
> discovers its targets from the API server (`kubernetes_sd_configs: role: pod`) instead of
> naming static addresses, because each role is autoscaled and a Service target load-balances —
> so a static address reaches one arbitrary replica per scrape and its per-process counters
> appear to reset. Discovery needs an identity: `prometheus-deployment.yaml` must carry
> `serviceAccountName: prometheus` **and** `automountServiceAccountToken: true` for the
> ServiceAccount and Role in `prometheus-rbac.yaml` to apply. With either missing, discovery
> fails at startup and Prometheus then serves normally with **zero targets** — pod healthy,
> rules loaded, nothing collected. After any deployment, assert
> `jq '.data.activeTargets | length'` over `/api/v1/targets` is greater than zero and every
> target reads `health: up`; [kafka-operations.md](kafka-operations.md#is-the-alert-armed-at-all)
> carries the commands.

## Example Prometheus Queries

```promql
# Transaction throughput (per second, 5 minute window)
rate(blnk_transaction_total[5m])

# Rejection rate by reason
rate(blnk_transaction_rejected_total[5m])

# P99 transaction latency
histogram_quantile(0.99, rate(blnk_transaction_duration_seconds_bucket[5m]))

# Queue enqueue rate
rate(blnk_queue_enqueued_total[5m])

# Hot lane traffic ratio
blnk_hotpairs_lane_routed_total{lane="hot"} / blnk_hotpairs_lane_routed_total

# Worker retry rate
rate(blnk_worker_retries_total[5m])

# Batch coalescing success rate
blnk_transaction_batch_total{result="success"} / blnk_transaction_batch_total

# Event publish throughput (per second, 5 minute window)
#
# sum(rate(...)), NOT rate(...). This counter carries `topic` and `event_type`, so a bare
# rate() returns ONE SERIES PER LABEL COMBINATION — five category topics against thirteen
# event types. Every one of those lines is a fraction of the pipeline's output, none of them is
# the throughput figure, and reading the largest as "the rate" understates the total by most of
# an order of magnitude. The 500 events/sec target is a statement about the pipeline's total
# output, so the query has to aggregate before it is compared to anything.
#
# Keep the per-series form only when the per-topic breakdown is what you want, and label it as
# such: sum by (topic) (rate(blnk_events_published_total[5m])).
sum(rate(blnk_events_published_total[5m]))

# SUSTAINED throughput, which is what the target actually asks for.
#
# The query above is a five-minute AVERAGE, and an average cannot distinguish a pipeline that
# held 500/sec throughout from one that published nothing for half the window and 1000/sec for
# the other half. min_over_time across a subquery evaluates the rate in each 30-second step and
# reports the WORST one, which is the figure an average hides. Compare THIS to the target when
# the question is whether the rate was sustained.
min_over_time(sum(rate(blnk_events_published_total[30s]))[30m:30s])

# Rows reaching Kafka but not being marked: the leading indicator of duplicate delivery.
#
# Filtered to original publishes, because replays and dead-letter writes are acknowledged and
# are deliberately never counted as deliveries. A small standing value is normal — the two
# increments are not simultaneous — but a value that grows means the outbox write path is
# failing and those events WILL be published a second time.
sum(rate(blnk_events_broker_acknowledgements_total{purpose="original"}[5m]))
  - sum(rate(blnk_events_published_total[5m]))

# Real broker traffic including re-delivery, which the throughput query above excludes by
# design. Break it out by purpose to see how much of it is triage.
sum by (purpose) (rate(blnk_events_broker_acknowledgements_total[5m]))

# Publish attempts by outcome: retry pressure, independent of how many events were delivered.
# A rising retrying share is a broker under strain. There is no `failed` outcome to read the
# stuck population from — see the note on the closed vocabulary above; read that from
# blnk_events_dead_lettered_total and GET /events/stats (its `failed` and `dead_lettered`
# counts need no parameter; only `dispatched` is taken on request).
rate(blnk_events_publish_attempts_total[5m])

# Attempts that gave up: events that will NOT be attempted again. This is the pair that
# replaced the retired outcome="failed" selection.
rate(blnk_events_publish_attempts_total{outcome="retrying",terminal="true"}[5m])

# P99 outbox-to-Kafka latency for FIRST-attempt publishes only.
#
# capture_to_dispatch, NOT publish_duration. The target is stated over the interval a
# subscriber actually waits, which begins when the ledger transaction committed the outbox
# row. publish_duration starts at the CLAIM, so it excludes the row waiting for the next
# poll tick, the poll interval and the claim query itself — a relay an hour behind reports
# the same sub-second p99 as an idle one, and the query would certify a target the system
# was missing.
histogram_quantile(0.99, sum by (le) (rate(
  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))

# The broker write in isolation: first-attempt SUCCESSFUL publishes only, which is the query
# that instrument's own declaration states. Useful on its own, and it is the second term of
# the next query.
histogram_quantile(0.99, sum by (le) (rate(
  blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))

# ROUGH DIAGNOSTIC ONLY — this difference is NOT a queue-wait p99, and must not be alerted on
# or reported as one.
#
# Two reasons, both fatal to reading it as a quantile:
#   1. Quantiles are not additive. p99(a) - p99(b) is not p99(a-b) for any distribution, so the
#      result is not the 99th percentile of anything.
#   2. The two populations are not paired. The end-to-end series covers every acknowledged
#      first-attempt publish; the broker series covers first-attempt SUCCESSFUL publishes. The
#      difference is taken across two differently-composed sets, not per event, and it can go
#      NEGATIVE when their shapes diverge.
#
# What it is good for: a large positive gap suggests time is being spent before the broker write
# rather than in it. Confirm that with blnk_outbox_pending, which measures backlog directly.
# For a real queue-wait distribution, the wait has to be instrumented as its own histogram.
histogram_quantile(0.99, sum by (le) (rate(
  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))
  - histogram_quantile(0.99, sum by (le) (rate(
      blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))

# Dead-letter rate INDICATOR, which stays below 0.001 — that is 0.1% — against a healthy broker.
#
# Both counters are per-EVENT and both are incremented at a durable row transition rather than
# at a broker write, so they partition unique events and this is the figure acceptance criterion
# V-3 is scored on. Do NOT substitute blnk_events_broker_acknowledgements_total for the
# denominator: that one counts writes, so a republished event or an afternoon of dead-letter
# triage would move it and the rate would depend on how much re-delivery happened rather than on
# how the pipeline behaved.
#
# The denominator is the SUM of both counters and not published alone. The two partition a
# captured event's terminal outcomes — delivered or dead-lettered, never both — so their sum
# is the population. Dividing by published alone reports dead-letters as a fraction of
# successes, which overstates the rate and diverges without bound as failures rise: every
# event failing gives a denominator of zero.
#
# The partition holds because BOTH counters are incremented at their durable row transition
# rather than at their broker write: the dispatched (or webhook-pending) transition for the
# numerator's complement, the acknowledged `.dlt` write recorded on the row for the numerator. A
# retried or republished event therefore contributes exactly one to this population, so the ratio
# is a rate over EVENTS and not over writes. See "Three counters, three questions" above.
sum(rate(blnk_events_dead_lettered_total[30m]))
  / (sum(rate(blnk_events_dead_lettered_total[30m]))
     + sum(rate(blnk_events_published_total[30m])))

# Dead-lettered events stuck longer than 15 minutes (the alert condition)
blnk_dlt_oldest_message_age_seconds > 900

# Subscriber consumer lag above the alert threshold
blnk_kafka_consumer_lag > 10000

# Topics whose lag could not be fully measured. Non-zero means the gauge above WITHHOLDS that
# topic, so the threshold rule cannot fire for it — the ConsumerLagMeasurementDegraded
# condition.
blnk_kafka_consumer_lag_unmeasured_partitions > 0

# Subscribers with no lag series at all: the SubscriberLagCoverageIncomplete condition. A
# different question from the query above — that one is a subscriber whose reading is a lower
# bound, this one is a subscriber with no reading. Attributed by `reason`, so read it broken
# out: only `budget` is answered by raising a number.
sum without(reason)(blnk_kafka_subscribers_unmeasured) > 0

# The same gap by cause, which is what decides the remedy: `measure_failed` needs the broker,
# `registry_failed` needs the database, `unprovisioned` needs a grant, `topic_missing` needs
# provisioning, and only `budget` needs EVENT_METRICS_SUBSCRIBER_BUDGET.
blnk_kafka_subscribers_unmeasured > 0

# The same gap as a PROPORTION of the registry, which is how to judge its severity: 3 of 3000
# is a rounding error on coverage, 3 of 5 means the lag signal is mostly absent. Each gap is
# attributed to exactly one reason, so this can never exceed 1.
sum without(reason)(blnk_kafka_subscribers_unmeasured) / clamp_min(blnk_subscribers_registered, 1)

# Measurement-budget headroom. Watch this rather than waiting for the shortfall above: it goes
# negative BEFORE any subscriber goes unmeasured. Both operands are series, so the query is true
# on every deployment — it used to read `200 - blnk_subscribers_registered`, naming the DEFAULT
# as a literal, which showed a deployment running a budget of 1000 as exhausted eight hundred
# subscribers early. The budget is set by EVENT_METRICS_SUBSCRIBER_BUDGET (alias
# RELAY_SUBSCRIBER_METRICS_BUDGET) and exported as the gauge below.
blnk_subscribers_measurement_budget - blnk_subscribers_registered

# Relay backlog: rows captured but not yet published, counted as pending plus processing. A
# rising figure against a flat publish rate is a relay that is not keeping up.
blnk_outbox_pending

# REPAIR backlog and its DRAIN RATE, per leg. These two are read together or neither is
# actionable: the first is what is owed, the second is how fast it is being paid, and their
# quotient is the time to clear. Note that blnk_outbox_pending above CANNOT answer this — the
# rows here are `failed` and `webhook_pending`, which that gauge does not count.
blnk_events_repair_backlog
rate(blnk_events_repair_completed_total[5m])

# Seconds to clear the repair backlog at the current drain rate, per leg. A result that climbs
# means the backlog is growing faster than the repair is clearing it.
blnk_events_repair_backlog
  / on(leg) rate(blnk_events_repair_completed_total[5m])

# Repair capacity is the binding constraint, not the broker. This is the condition to alert on
# when a recovery is taking too long: the per-tick budget ended the chain with work still
# coming back, so RELAY_REPAIR_MAX_BATCHES_PER_TICK or RELAY_REPAIR_BATCH_SIZE is the remedy
# rather than more brokers.
max_over_time(blnk_events_repair_saturated[15m]) == 1

# Retention sweep removal rate. Zero most often means NOTHING IS ELIGIBLE — no terminal row is
# older than the retention cutoff yet — and only otherwise means the sweep is disabled or stuck.
# Check the configured retention window before treating zero as a fault. The sweep deletes in
# batches, so expect a step-shaped series rather than a smooth rate.
rate(blnk_events_purged_total[1h])

# Retained terminal rows. blnk_outbox_pending CANNOT answer this — it holds only pending and
# processing rows — so query the outbox status counts instead:
#   GET /events/stats   -> per-status counts, including dispatched / failed / dead_lettered
# or, from SQL:
#   SELECT status, count(*) FROM blnk.event_outbox GROUP BY status;

# Any subscriber credential revocation still owed at the broker (normally zero)
blnk_subscribers_revocation_pending > 0

# An outstanding revocation nobody has settled for an hour: a live credential the registry
# records no issuance for, which needs revoking by hand
blnk_subscribers_oldest_revocation_age_seconds > 3600

# Subscribers whose lag is not being measured, and why. Read this whenever the completeness
# gauge below is zero: the reason decides the action, and 'budget' is the only one resolved by
# configuration rather than by fixing something.
blnk_kafka_subscribers_unmeasured > 0

# The lag inventory is not covering every registered subscriber — the
# SubscriberLagCoverageIncomplete condition. Expected transiently on a registry larger than one
# sweep's budget, since coverage rotates; sustained means the rotation cannot keep up or a
# measurement is failing.
blnk_kafka_consumer_lag_inventory_complete == 0

# The collector has stopped refreshing. Every event gauge above is frozen at its last reading
# and still being scraped as current while this holds, so no threshold rule over them can be
# trusted.
blnk_event_metrics_last_collection_age_seconds > 120

# The collector is ticking and failing. Invisible to the query above, whose value stays near
# zero throughout: a collector whose dependency is refusing connections keeps perfect time and
# publishes nothing.
blnk_event_metrics_last_success_age_seconds > 300

# WHICH collection is failing, which is what separates a database fault from a broker one
# without reading logs.
rate(blnk_event_metrics_collection_failures_total[5m])

# No collector at all in any scraped server process. Scoped to the server job deliberately —
# the collector is server-only, so the series is legitimately absent from every worker target
# and an unscoped absent() would fire for ever on a correct deployment.
absent(blnk_event_metrics_last_collection_age_seconds{job="blnk-server"})
```
