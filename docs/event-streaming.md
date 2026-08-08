# Blnk Event Streaming Reference

Blnk publishes every ledger event to Kafka. Subscribers consume the topics below directly with
their own SASL/SCRAM credentials; there is no HTTP push to configure and no delivery endpoint to
keep online. This document is the subscriber-facing contract: the topics, the envelope, the
ordering and delivery guarantees, and the obligations those guarantees place on a consumer.

The legacy HTTP webhook transport runs alongside Kafka for a fixed 30-day window and is then
retired. The window and the retirement are both governed by one setting,
`WEBHOOK_DEPRECATION_SUNSET_DATE`: while the date is in the future the relay drives Kafka and the
HTTP leg from the same outbox row, so the two transports carry identical bytes; once it has passed
the HTTP leg is neither enqueued nor delivered and Kafka is the only transport.

## Prerequisites

- A Kafka 3.5+ cluster reachable from the Blnk server and worker roles.
- `KAFKA_BROKERS` configured on Blnk. **With no brokers configured Blnk publishes nothing to
  Kafka** — the event relay does not start, and a deployment that still has
  `BLNK_WEBHOOK_URL` (`notification.webhook.url`) set keeps receiving the legacy HTTP webhook instead. That is a supported
  steady state, not an error, and it is what lets a deployment migrate on its own schedule.
- Credentials issued for your subscriber. Each subscriber is a Kafka principal whose ACLs are
  scoped to its authorised topics and its consumer-group namespace.

## Topics

Every topic name is `<KAFKA_TOPIC_PREFIX>.<category>`, and the prefix defaults to `blnk`. Each
category topic has a dead-letter sibling named by appending `.dlt`.

| Topic | Dead-letter sibling | Carries | Subscriber-grantable |
|---|---|---|---|
| `blnk.transactions` | `blnk.transactions.dlt` | Transaction lifecycle events and bulk batch outcomes | Yes |
| `blnk.balances` | `blnk.balances.dlt` | Balance creation and balance monitor alerts | Yes |
| `blnk.identities` | `blnk.identities.dlt` | Identity events | Yes |
| `blnk.ledgers` | `blnk.ledgers.dlt` | Ledger events | Yes |
| `blnk.system` | `blnk.system.dlt` | Blnk-internal error events, and any event type this catalogue does not recognise | **No — internal** |

`blnk.system` is internal and is never granted to a subscriber. An event type that the catalogue
does not recognise routes there rather than being dropped, so an unrecognised producer cannot
deliver a balance or an identity record to an audience that never asked for it, while the event
stays durable, observable and replayable.

Topics are created with at least `KAFKA_MIN_PARTITIONS` partitions (default 6) and a replication
factor of `KAFKA_REPLICATION_FACTOR`. Use 1 for a single-broker local stack and 3 in production; a
single-broker cluster refuses topic creation at a factor of 3.

### The `<topic>.dlt` naming convention is Blnk-owned

`<topic>.dlt` names belong to Blnk. Blnk publishes an event to its category's dead-letter topic
when the relay's retry budget for that event is exhausted, and exposes that inventory for triage
and replay.

**Blnk does not implement, manage or observe subscriber-side dead-lettering.** If you build your
own consumer-side dead-letter topics, do not use the `<topic>.dlt` names above — they are already
taken and Blnk writes to them. Choose a namespace of your own, for example
`<your-service>.<topic>.dlq`.

Subscriber credentials are refused access to every `.dlt` topic.

## Event envelope

Every message value is one JSON object with exactly these members, in this order:

```json
{
  "event_id": "9f1b1a70-6c3e-4f7f-9a2e-6d5e2c0f1a11",
  "event_type": "transaction.applied",
  "aggregate_id": "txn_8f1c...",
  "occurred_at": "2026-02-11T09:14:22.417Z",
  "payload": { "event": "transaction.applied", "data": { "…": "…" } },
  "schema_version": 1
}
```

| Field | Type | Meaning |
|---|---|---|
| `event_id` | UUID string | Unique per event. **This is your idempotency key** — see [Delivery guarantees](#delivery-guarantees). |
| `event_type` | string | The event name, duplicated at the envelope level so you can route and filter without parsing the payload. |
| `aggregate_id` | string | The entity the event is about: a transaction, balance, ledger, identity or batch id. |
| `occurred_at` | RFC3339 timestamp (UTC) | When Blnk captured the event, at the moment its ledger transaction committed. |
| `payload` | object | The legacy webhook body, verbatim: `{"event": <event_type>, "data": <object>}`. |
| `schema_version` | integer | Envelope version, currently `1`. A new member may be ADDED at version 1 without notice; a removal or a type change raises the version. |

`payload` is byte-for-byte what the HTTP webhook body was, both keys included, so an existing
webhook body parser works unchanged and only the transport differs.

## Event types

| Event type | Topic | Emitted when |
|---|---|---|
| `transaction.applied` | `blnk.transactions` | A transaction settled |
| `transaction.inflight` | `blnk.transactions` | A transaction was recorded as inflight |
| `transaction.void` | `blnk.transactions` | An inflight transaction was voided |
| `transaction.rejected` | `blnk.transactions` | A transaction was rejected, for example for insufficient funds |
| `transaction.queued` | `blnk.transactions` | Reserved — see the note below |
| `transaction.scheduled` | `blnk.transactions` | Reserved — see the note below |
| `transaction.unknown` | `blnk.transactions` | Reserved — see the note below |
| `bulk_transaction.<status>` | `blnk.transactions` | A bulk batch finished, with the batch status as the suffix. The suffix set is open; match on the `bulk_transaction.` prefix rather than an enumeration |
| `balance.created` | `blnk.balances` | A balance was created |
| `balance.monitor` | `blnk.balances` | A balance monitor's condition was met |
| `identity.created` | `blnk.identities` | An identity was created |
| `ledger.created` | `blnk.ledgers` | A ledger was created |
| `system.error` | `blnk.system` | An internal error was escalated (internal topic) |

### Three names in the vocabulary are not currently reachable

`transaction.queued`, `transaction.scheduled` and `transaction.unknown` are part of the event
vocabulary but **no transaction currently produces them on either transport**. A transaction's
status is resolved to `APPLIED` before the event name is derived, so a queued or scheduled
transaction emits `transaction.applied`, and the `COMMIT` status falls through to
`transaction.unknown` at a point no code path reaches. This is exactly the pre-Kafka behaviour of
the HTTP webhook, preserved deliberately so the two transports carry identical payloads during
the dual-delivery window.

Do not build a handler that waits for one of these names. They are documented because they can
appear in a future release: correcting the `COMMIT` fall-through changes what BOTH transports
carry and therefore belongs in its own deliberate, announced change.

## Ordering

Events are keyed by **ledger id**, with a documented fallback for events whose payload carries no
ledger, and are routed with a stable hash (`murmur2`) balancer. Every event for one ledger
therefore lands on one partition and is consumed in publish order. Blnk's relay additionally
claims outbox rows in occurrence order and keeps at most one row per partition key in flight, so
ordering holds across concurrent relay instances as well as within one.

The key fallback chain, in order: the ledger supplied by the producer, then the payload's own
ledger, then the payload's aggregate (source balance for a transaction, balance for a monitor,
identity, batch id), then the event type, then a fixed sentinel. The chain never yields an empty
key, because an empty key lets Kafka scatter events round-robin and destroys ordering with nothing
in the data to show that it happened.

Two consequences worth planning for:

- **Ordering is per ledger, not global.** Two ledgers' events have no relative order.
- **A key's events are serialised across topics.** A stuck event holds up later events sharing its
  partition key even when they belong to another category. That is a deliberate
  correctness-over-throughput trade, bounded by the retry budget, and it is why a dead-letter age
  alert also implies delayed sibling events for the same ledger.

## Delivery guarantees

**Write side: exactly once.** Each event is written to a PostgreSQL outbox table inside the same
database transaction as the ledger mutation that produced it. The mutation and its event commit
together or not at all — with one narrow, documented exception, described in the next section.

**Wire: at least once.** The relay publishes a claimed row and then marks it dispatched. A crash
between the broker's acknowledgement and that mark redelivers the event on the next claim, and
the redelivered message is byte-identical to the first.

**Therefore: `event_id` idempotency is a subscriber obligation, not a recommendation.** Record the
`event_id` values you have processed and discard a repeat. Blnk cannot suppress the duplicate for
you, and no other field distinguishes the pair. The legacy HTTP leg carries the same value in the
`X-Blnk-Event-Id` header, so one idempotency store serves both transports during the window.

### The one exception: `balance.monitor` is at-most-once

`balance.monitor` is the single event type whose capture is **not** atomic with its mutation, and
the reason is structural rather than incidental. A monitor fires because a condition was met on a
balance that a transaction has ALREADY committed: by the time the alert exists there is no open
transaction left to enrol it in, and monitor conditions are evaluated after the commit by design.

What this means in practice:

- The insert of a `balance.monitor` event is retried on a transient database failure — a bounded
  budget of three attempts at 200ms then 400ms, with every attempt logged — and each attempt
  re-sends the same event id, so a retry cannot deliver the alert twice.
- If the whole budget is spent, or if the process dies in the window between the balance commit
  and the insert, **the alert is lost**. The balance movement stands; the notification does not
  exist and there is nothing to replay, because no row was ever written.
- A loss is never silent. It is logged at ERROR with the event id, the event type and the topic,
  and it is escalated as a `system.error` event, so an operator can detect a missed monitor
  alert rather than infer it.

Treat `balance.monitor` as an at-most-once alerting signal. If your use case cannot tolerate a
missed alert, poll the balance you care about — `GET /balances/:id` — rather than relying on the
event alone. Every other event type carries the full transactional guarantee above.

## Dead-lettering and replay

When the relay's retry budget for an event is exhausted, the event is published to its category's
`.dlt` topic with a `failure_metadata` object appended to the envelope, carrying
`original_topic`, `error_reason`, `attempt_count`, `first_attempted_at` and `last_attempted_at`.
The dead-lettered message is otherwise byte-identical to the message that would have been
published.

Blnk retains the outbox row so the event can be listed and replayed by an operator. A replay
re-publishes the ORIGINAL stored bytes to the original topic — the same `event_id`, the same
partition key, and no `failure_metadata` — so a replayed event is indistinguishable from the
original at a subscriber, and your `event_id` idempotency store collapses it if you had already
processed it.

Replay is operator-triggered and idempotent: an event already replayed, or one that was never
dead-lettered, is refused rather than published a second time.

## Consumer configuration

- **Consumer group.** Use the group id your credentials were issued with. ACLs are scoped to that
  group's namespace; joining another group is refused.
- **Topics.** Subscribe only to the topics your credentials list. Every other topic, and every
  `.dlt` topic, is refused with an authorisation error.
- **Offsets.** Commit offsets after your handler has recorded the `event_id`, so a redelivery
  after a crash is suppressed by your idempotency store rather than reprocessed.
- **Message size.** Blnk refuses to publish an event larger than 768 KiB, so a consumer's
  `max.partition.fetch.bytes` needs no unusual value.
- **Unknown fields.** Ignore envelope and payload members you do not recognise. New members may be
  added within `schema_version` 1.

## Related documents

- [metrics.md](./metrics.md) — the event publishing, outbox backlog, dead-letter age and consumer
  lag instruments, and the alert thresholds built on them.
