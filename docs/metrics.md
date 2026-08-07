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
| `blnk_events_published_total` | Counter | `topic`, `event_type` | Ledger events acknowledged by the broker. |
| `blnk_events_publish_attempts_total` | Counter | `outcome`, `attempt` | Individual publish attempts. Compare against `published_total` to see how much of the traffic is retries. |
| `blnk_events_publish_duration_seconds` | Histogram | `topic`, `attempt`, `outcome` | Claim-to-acknowledgement latency — the broker write in relative isolation. Not the figure the latency target is read from; see the row below. |
| `blnk_events_capture_to_dispatch_duration_seconds` | Histogram | `topic`, `attempt` | END-TO-END age of a delivered event, from its capture in the transactional outbox to broker acknowledgement. **This is the series the sub-2s p99 target is read from**, filtered to `attempt="1"`. Only acknowledged publishes are recorded. Subtract the row above to get the queue wait, which is what tells you whether a slow figure is the broker or a relay backlog. |
| `blnk_kafka_consumer_lag_unmeasured_partitions` | Gauge | `subscriber`, `group`, `topic` | Partitions whose offsets could not be read when a subscriber's lag was last measured. Non-zero means `blnk_kafka_consumer_lag` WITHHOLDS that topic, so the lag alert cannot fire for it. |
| `blnk_events_dead_lettered_total` | Counter | `topic`, `event_type` | Events diverted to a `<topic>.dlt` sibling after exhausting their retry budget. |
| `blnk_dlt_oldest_message_age_seconds` | Gauge | — | Age of the oldest unresolved dead-lettered event. Zero when the inventory is empty. |
| `blnk_kafka_consumer_lag` | Gauge | `subscriber`, `group`, `topic` | Committed offset behind the end offset, measured in-process. |
| `blnk_outbox_pending` | Gauge | — | Outbox rows not yet published, counted as pending plus processing. |
| `blnk_events_purged_total` | Counter | — | Terminal event rows deleted by the retention sweep. |
| `blnk_subscribers_revocation_pending` | Gauge | — | Subscribers whose broker-side credential revocation is still owed. Normally zero. |
| `blnk_subscribers_oldest_revocation_age_seconds` | Gauge | — | How long the oldest outstanding revocation has been owed. Zero when nothing is owed. |

**Two gauges have a single owner.** `blnk_dlt_oldest_message_age_seconds` and
`blnk_kafka_consumer_lag` are written only by the periodic collector in the server role,
which re-records them — including an explicit zero — on every tick. A gauge retains its last
value, so zero is the measurement that clears an alert.

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
`topic` label is deliberately **not** hashed: it names one of four fixed, published,
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

# The queue wait: how much of that end-to-end figure is backlog rather than broker.
histogram_quantile(0.99, sum by (le) (rate(
  blnk_events_capture_to_dispatch_duration_seconds_bucket{attempt="1"}[5m])))
  - histogram_quantile(0.99, sum by (le) (rate(
      blnk_events_publish_duration_seconds_bucket{attempt="1",outcome="dispatched"}[5m])))

# Dead-letter rate as a fraction of published events
rate(blnk_events_dead_lettered_total[30m]) / rate(blnk_events_published_total[30m])

# Dead-lettered events stuck longer than 15 minutes (the alert condition)
blnk_dlt_oldest_message_age_seconds > 900

# Subscriber consumer lag above the alert threshold
blnk_kafka_consumer_lag > 10000

# Retention sweep removal rate (zero means retention is disabled or nothing is eligible)
rate(blnk_events_purged_total[1h])

# Any subscriber credential revocation still owed at the broker (normally zero)
blnk_subscribers_revocation_pending > 0

# An outstanding revocation nobody has settled for an hour: a live credential the registry
# records no issuance for, which needs revoking by hand
blnk_subscribers_oldest_revocation_age_seconds > 3600
```
