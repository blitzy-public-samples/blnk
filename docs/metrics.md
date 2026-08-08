# Blnk Metrics Reference

Blnk exposes OpenTelemetry metrics via a Prometheus-compatible `/metrics` endpoint. This document lists all available metrics, their types, attributes, and what they tell you operationally.

## Prerequisites

- Set `"enable_observability": true` in `blnk.json` (or `BLNK_ENABLE_OBSERVABILITY=true`)
- Metrics are served on:
  - **Server**: `GET /metrics` on the API port (default `5001`)
  - **Worker**: `GET /metrics` on the monitoring port (default `5004`)

## Authentication

When `server.secure` is enabled, the `/metrics` endpoint requires a bearer token:

1. Set `"metrics_bearer_token": "<your-token>"` in `blnk.json` (or `BLNK_METRICS_BEARER_TOKEN`)
2. Configure Prometheus to send the token:

```yaml
scrape_configs:
  - job_name: 'blnk-server'
    authorization:
      type: Bearer
      credentials: '<your-token>'
    static_configs:
      - targets: ['server:5001']
```

If secure mode is enabled without a token configured, the endpoint returns `403 Forbidden`.

## Export Modes

| Mode | How it works | When active |
|------|-------------|-------------|
| **Pull (Prometheus)** | Prometheus scrapes `/metrics` | Always (when observability is enabled) |
| **Push (OTLP HTTP)** | Periodically pushes to an OTel Collector | When `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

## Metrics

> **Naming convention**: OTel instrument names use dots (e.g., `blnk.transaction.total`). The Prometheus exporter converts these to underscores automatically (e.g., `blnk_transaction_total`).

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
dead-letters an event whose budget is spent.

| Prometheus Name | Type | Attributes | Description |
|----------------|------|------------|-------------|
| `blnk_events_published_total` | Counter | `topic`, `event_type` | Ledger events acknowledged by the broker and durably recorded as dispatched, counted once per ORIGINAL event — replays and dead-letter writes are deliberately excluded, so this stays a count of first deliveries. Use to track delivery throughput per topic, and as one half of the dead-letter rate below. |
| `blnk_events_publish_attempts_total` | Counter | `outcome` | Individual publish attempts, retries included, with exactly one `outcome` recorded per attempted write — so summing the series gives the total number of attempts. Use to read retry pressure independently of delivery volume, and to tell an event that is still being retried from one that is genuinely stuck. |
| `blnk_events_publish_duration_seconds` | Histogram | `topic`, `attempt`, `outcome` | Claim-to-acknowledgement latency — the broker write in relative isolation. Not the figure the latency target is read from; see the row below. |
| `blnk_events_capture_to_dispatch_duration_seconds` | Histogram | `topic`, `attempt` | END-TO-END age of a delivered event, from its capture in the transactional outbox to broker acknowledgement. **This is the series the sub-2s p99 target is read from**, filtered to `attempt="1"`. Only acknowledged publishes are recorded. Subtract the row above to get the queue wait, which is what tells you whether a slow figure is the broker or a relay backlog. |
| `blnk_kafka_consumer_lag_unmeasured_partitions` | Gauge | `subscriber`, `group`, `topic` | Partitions whose offsets could not be read when a subscriber's lag was last measured. Non-zero means `blnk_kafka_consumer_lag` WITHHOLDS that topic, so the lag alert cannot fire for it. |
| `blnk_events_dead_lettered_total` | Counter | `topic`, `event_type` | Events diverted to a `<topic>.dlt` sibling after exhausting their retry budget, attributed by the ORIGINAL category topic rather than the sibling so it divides against `blnk_events_published_total` without a name mismatch. Use as the numerator of the dead-letter rate, and to see which event types are failing rather than only how many. |
| `blnk_dlt_oldest_message_age_seconds` | Gauge | `topic` | Age of the oldest unresolved dead-letter entry, per dead-letter topic, over every outbox row in the `failed` or `dead_lettered` state and measured from its last publish attempt. Use to alert on a triage backlog nobody is clearing; zero is reported explicitly when a topic's inventory is empty, and zero is the reading that clears the alert. |
| `blnk_kafka_consumer_lag` | Gauge | `subscriber`, `group`, `topic` | Committed offset behind the log end offset, summed across a topic's partitions and measured in-process. Use to alert on a subscriber falling behind — but read `blnk_kafka_consumer_lag_unmeasured_partitions` beside it, because a topic that was only partly measured is withheld from this series rather than reported at its partial sum. |
| `blnk_outbox_pending` | Gauge | — | Outbox rows not yet published, counted as pending plus processing — claimed-but-unacknowledged rows are included, so a stalled relay holding every row under a lease cannot read as a drained backlog. Use to see whether the relay is keeping up: a rising figure against a flat publish rate is backlog, not throughput. |
| `blnk_events_purged_total` | Counter | — | Terminal event rows deleted by the retention sweep. Use to confirm the sweep is keeping up with the arrival rate — a flat counter means it is disabled or has stopped, and a step change means a cutoff was misconfigured. |
| `blnk_subscribers_revocation_pending` | Gauge | — | Subscribers whose broker-side credential revocation is still owed. Normally zero. |
| `blnk_subscribers_oldest_revocation_age_seconds` | Gauge | — | How long the oldest outstanding revocation has been owed. Zero when nothing is owed. |

**`outcome` values**: `dispatched`, `retrying`, `failed`, `dead_lettered`

The vocabulary is closed and mutually exclusive — exactly one value per attempted write.
`retrying` means the attempt failed and another is possible under the retry budget; `failed`
means the attempt failed and no further attempt will be made, either because the failure is
permanent or because the budget is spent; `dead_lettered` is an acknowledged write to a
`<topic>.dlt` sibling, which is the terminal outcome of the original event. Distinguishing
`failed` from `retrying` is what makes "how many events are actually stuck" answerable.

**`attempt` values**: `1`, `2`, `3`, `4`, `5`, `over`, `replay`, `dead_letter`

A deliberately closed domain, because this attribute sits on a histogram and its cardinality
is multiplied by the bucket count. The numbers are the attempt number of an original publish,
capped at the retry budget — `RELAY_MAX_RETRY_ATTEMPTS` is clamped to 5, and a row whose
`max_attempts` was raised past that collapses to `over` rather than minting a new label. A
replay and a dead-letter write belong to no retry sequence, so they carry fixed tokens instead
of a number; that is what keeps them out of the `attempt="1"` population the latency target is
read from. `blnk_events_capture_to_dispatch_duration_seconds` uses the same domain minus
`dead_letter`, which is never an end-to-end delivery.

**`topic` values**: the five category topics `blnk.transactions`, `blnk.balances`,
`blnk.identities`, `blnk.ledgers`, `blnk.system`, and their five dead-letter siblings
`blnk.transactions.dlt`, `blnk.balances.dlt`, `blnk.identities.dlt`, `blnk.ledgers.dlt`,
`blnk.system.dlt`

Every one of those names derives from `KAFKA_TOPIC_PREFIX`, whose default is `blnk`; both the
un-prefixed and the `BLNK_`-prefixed form of that variable are accepted. Set a different prefix
and the whole inventory moves with it, labels included. A topic name Blnk does not own never
reaches a label: it collapses to `unowned` on the counters and histograms, and to `other` on
the two consumer-lag gauges. The series count therefore stays bounded while the anomaly stays
visible as a non-zero count on that one label, and the rejected name remains on the outbox row
and in the log line, which is where an operator triaging a single event looks.

Two instruments use `topic` in opposite directions, and both are deliberate.
`blnk_events_dead_lettered_total` carries the **original category** topic, so that it divides
against `blnk_events_published_total` cleanly; `blnk_dlt_oldest_message_age_seconds` carries
the **`.dlt` sibling**, because what it measures is what is waiting on a dead-letter topic.

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

**Every gauge here has a single owner.** `blnk_dlt_oldest_message_age_seconds`,
`blnk_outbox_pending`, `blnk_kafka_consumer_lag` and its unmeasured-partitions companion are
all fed by the periodic collector in the server role and by nothing else, which re-reads them
from authoritative state on every tick. A synchronous gauge retains its last value, so the
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
keep in step with the registry — if the series are absent, the collector is not running, not
that a separate exporter is missing.

**Retention is off by default.** `blnk_events_purged_total` stays at zero until
`RELAY_EVENT_RETENTION_DAYS` is set to a positive number of days. A flat counter alongside a
rising `blnk_outbox_pending` is the signal that delivered events are accumulating
indefinitely — each one holding a verbatim copy of its webhook body.

**Subscriber and group labels are pseudonyms, not names.** `blnk_kafka_consumer_lag` carries a
truncated SHA-256 of the subscriber identifier and of its consumer-group namespace rather than
the values themselves. A metric label is the most widely copied value in an observability
stack — it reaches Prometheus, every dashboard, every alert notification and whatever
long-term store the series are federated into — so publishing a tenant-ish name there spreads
it into all of them by default. The hash is stable, so a rate and a threshold behave
identically and distinct subscribers stay distinct.

To resolve one: list the registry through `GET /subscribers` and hash each `subscriber_id` the
same way, or search the service logs for the matching `subscriber_id_hash` field — the log
fields and the metric labels use the same hash, so an operator can pivot between them. The
`topic` label is deliberately **not** hashed: it names one of the five fixed, published,
Blnk-owned category topics, so it identifies nobody.

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

Find the affected subscribers with `GET /subscribers`: a row that owes one carries
`revocation_pending`, `revocation_pending_reason` and `revocation_pending_at`.

**Alerting on these series.** `alerts/blnk-kafka-alerts.yml` defines one rule group,
`blnk-kafka-alerts`, evaluated every 30 seconds:

| Alert | Expression | Dwell | Severity |
|-------|-----------|-------|----------|
| `DeadLetterMessageStuck` | `blnk_dlt_oldest_message_age_seconds > 900` | `0m` | critical |
| `SubscriberConsumerLagHigh` | `blnk_kafka_consumer_lag > 10000` | `2m` | warning |
| `SubscriberRevocationOutstanding` | `blnk_subscribers_oldest_revocation_age_seconds > 3600` | `0m` | critical |
| `ConsumerLagMeasurementDegraded` | `blnk_kafka_consumer_lag_unmeasured_partitions > 0` | `5m` | warning |

900 is the 15-minute dead-letter dwell threshold, written as a literal so that it is greppable,
and it carries the whole delay — which is why that rule's own dwell is `0m` rather than
omitted. The two lag rules are a pair: the threshold rule reads the gauge that carries only
complete measurements, and the degradation rule covers the gap withholding leaves, so a broker
or metadata fault surfaces as a measurement failure instead of silently resolving a firing
alert. Every rule names [kafka-operations.md](kafka-operations.md) as its `runbook_url`; that
document holds the triage and replay procedure each one resolves to.

> **A rule file is inert unless `prometheus.yml` lists it.** The `rule_files:` stanza — which
> this repository did not have before the event pipeline landed — is what arms the rules, not
> the mere presence of the file. It resolves a glob against the directory the Compose stack
> mounts `./alerts` into, so a rule file added there later needs no further edit here. Verify at
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
rate(blnk_events_published_total[5m])

# Publish attempts by outcome: retry pressure, independent of how many events were delivered.
# A rising retrying share is a broker under strain; a rising failed share is events that will
# not be attempted again.
rate(blnk_events_publish_attempts_total[5m])

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

# The queue wait: how much of that end-to-end figure is backlog rather than broker.
histogram_quantile(0.99, sum by (le) (rate(
  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))
  - histogram_quantile(0.99, sum by (le) (rate(
      blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))

# Dead-letter rate, which stays below 0.001 — that is 0.1% — against a healthy broker.
#
# The denominator is the SUM of both counters and not published alone. The two partition a
# captured event's terminal outcomes — delivered or dead-lettered, never both — so their sum
# is the population. Dividing by published alone reports dead-letters as a fraction of
# successes, which overstates the rate and diverges without bound as failures rise: every
# event failing gives a denominator of zero.
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

# Relay backlog: rows captured but not yet published, counted as pending plus processing. A
# rising figure against a flat publish rate is a relay that is not keeping up.
blnk_outbox_pending

# Retention sweep removal rate (zero means retention is disabled or nothing is eligible)
rate(blnk_events_purged_total[1h])

# Any subscriber credential revocation still owed at the broker (normally zero)
blnk_subscribers_revocation_pending > 0

# An outstanding revocation nobody has settled for an hour: a live credential the registry
# records no issuance for, which needs revoking by hand
blnk_subscribers_oldest_revocation_age_seconds > 3600
```
