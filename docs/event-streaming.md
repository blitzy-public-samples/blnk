# Blnk Event Streaming Reference

Blnk publishes every ledger mutation to Kafka as a `LedgerEvent`. This document is the subscriber-facing contract: the topics you consume, the JSON envelope you receive, the guarantees Blnk makes, the obligations it leaves to you, and the topic names Blnk reserves for itself. It replaces the legacy HTTP webhook push, which is retired at the end of the migration window.

The legacy HTTP webhook transport runs alongside Kafka for a fixed 30-day window and is then
retired. The window and the retirement are both governed by one setting,
`WEBHOOK_DEPRECATION_SUNSET_DATE`: while the date is in the future the relay drives Kafka and the
HTTP leg from the same outbox row, so the two transports carry identical bytes; once it has passed
the HTTP leg is neither enqueued nor delivered and Kafka is the only transport.

## Prerequisites

- A Kafka 3.5+ cluster reachable from the Blnk server and worker roles.
- `KAFKA_BROKERS` configured on Blnk. **With no brokers configured Blnk publishes nothing to
  Kafka** — the event relay does not start, and a deployment that still has `BLNK_WEBHOOK_URL`
  (`notification.webhook.url`) set keeps receiving the legacy HTTP webhook instead. That is a
  supported steady state, not an error, and it is what lets a deployment migrate on its own
  schedule. See [Running without Kafka is supported](#running-without-kafka-is-supported).
- Credentials issued for your subscriber. Each subscriber is a Kafka principal whose ACLs are
  scoped to its authorised topics and its consumer-group namespace — see
  [Getting Access](#getting-access).

## Topic Catalogue

Events are grouped into five **category topics**, each with a **dead-letter sibling** named by appending `.dlt`. Ten names in total, and they are the complete inventory — Blnk writes to no other topic.

| Category topic | Dead-letter topic | Audience | Carries |
|---------------|-------------------|----------|---------|
| `blnk.transactions` | `blnk.transactions.dlt` | Subscribers may be granted it | Transaction lifecycle, including bulk batch progress |
| `blnk.balances` | `blnk.balances.dlt` | Subscribers may be granted it | Balance creation and balance monitor alerts |
| `blnk.identities` | `blnk.identities.dlt` | Subscribers may be granted it | Identity creation |
| `blnk.ledgers` | `blnk.ledgers.dlt` | Subscribers may be granted it | Ledger creation |
| `blnk.system` | `blnk.system.dlt` | **Internal — never granted to a subscriber** | Blnk's own error notifications, and any event type the catalogue does not recognise |

### The topic prefix

Every one of those ten names is composed as `<prefix>.<category>` and `<prefix>.<category>.dlt`, where the prefix is `KAFKA_TOPIC_PREFIX` and defaults to `blnk`. A deployment that sets `KAFKA_TOPIC_PREFIX=acme` therefore consumes `acme.transactions`, `acme.transactions.dlt`, `acme.balances`, and so on down the table. The category tokens themselves — `transactions`, `balances`, `identities`, `ledgers`, `system` — never change.

No topic name is spelled as a literal anywhere in Blnk. `model.EventCategory` resolves an event type to its category token and `event_topics.go` composes the topic name from that token and the configured prefix, so the whole namespace moves together when the prefix changes.

> A prefix written with a trailing dot (`KAFKA_TOPIC_PREFIX=acme.`) is trimmed rather than concatenated, so it yields `acme.transactions` and not `acme..transactions`. Kafka would happily accept the latter as a distinct topic that nothing is subscribed to.

### Why five categories

The three category topics the requirement names — transactions, balances, identities — do not cover everything Blnk emits. `ledger.created` and `system.error` belong to none of them, while the coverage rule admits no exceptions: every event type that reached the legacy webhook sender is published to Kafka. Two further categories close that gap, and they follow the identical naming convention, so nothing about the scheme is special-cased and each dead-letter sibling is derived by the same rule as every other.

They are two rather than one because coverage and *reachability* are different questions, and one shared topic could not answer both:

- **`blnk.ledgers` is subscriber-facing.** `ledger.created` is ordinary ledger data — a name, an id, a creation instant, the caller's own metadata — and every webhook subscriber receives it today. Publishing it to a topic no subscriber can be granted would have made the migration silently lose an event, which is the one thing it promises not to do.
- **`blnk.system` is internal.** `system.error`'s payload is a frozen legacy contract that carries verbatim error text, and error text names internal detail: database schemas, tables and routines, broker addresses. That body cannot be narrowed without breaking the payload guarantee below, so the disclosure is contained by audience instead. Being internal is also what makes it a safe catch-all: an event type Blnk adds later without extending the catalogue lands somewhere durable, observable and replayable, but not in a subscriber's feed.

Neither category is a placeholder. Folding these events into an unrelated topic would corrupt that topic's meaning for everyone filtering on it, and dropping them would breach coverage outright.

## The `LedgerEvent` Envelope

Every message value on every topic is a JSON object with exactly these six keys. None of them is omitted when its value is empty, so a subscriber can rely on all six being present on every message.

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string (UUID) | Uniquely identifies this event. **This is your idempotency key** — see [Delivery Guarantees](#delivery-guarantees-and-your-idempotency-obligation). |
| `event_type` | string | The event name, one of the values in [Event Types](#event-types). Duplicates the `event` name inside `payload` so you can route without parsing the payload. |
| `aggregate_id` | string | The entity the event is about, and the value to group by once messages arrive. Always populated. |
| `occurred_at` | string (RFC3339) | When the domain action happened. Parse it with a real RFC3339 parser rather than a fixed format string — see the note below. |
| `payload` | object | The legacy webhook body, verbatim — see [The Payload](#the-payload). |
| `schema_version` | integer | The envelope version. `1` today. |

### A worked example

A `transaction.applied` message on `blnk.transactions`:

```json
{
  "event_id": "9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b",
  "event_type": "transaction.applied",
  "aggregate_id": "txn_6c8e5f3a91d24b0e8f7a2c5d",
  "occurred_at": "2026-05-01T12:34:56.789012345Z",
  "payload": {
    "event": "transaction.applied",
    "data": {
      "transaction_id": "txn_6c8e5f3a91d24b0e8f7a2c5d",
      "parent_transaction": "",
      "source": "bln_2f1c9a4e77b3401d8e6a0b5c",
      "destination": "bln_8d3e0b6a12c74f5981ba7e4d",
      "reference": "invoice-2026-04-3391",
      "currency": "USD",
      "amount": 129.5,
      "precise_amount": 12950,
      "precision": 100,
      "status": "APPLIED",
      "hash": "4d1f6e0c9b7a25384fe1c0d6a9b8734e5f2c1d0a9b8e7f6c5d4a3b2c1d0e9f8a",
      "allow_overdraft": false,
      "inflight": false,
      "created_at": "2026-05-01T12:34:56.712004510Z",
      "meta_data": {
        "customer_tier": "gold"
      }
    }
  },
  "schema_version": 1
}
```

The `data` object above is abridged for readability. It is whatever the domain object marshals to, so its exact key set depends on the event type and on which optional fields the mutation populated — treat it as an open object and read the keys you need.

### What `aggregate_id` is, per event type

`aggregate_id` names the subject of the event. It is **not** the Kafka message key; the key controls partitioning and is derived separately, as [Partitioning and Ordering](#partitioning-and-ordering) explains. The two coincide for some event types and differ for others.

| Event type | `aggregate_id` |
|-----------|----------------|
| `transaction.*` | The transaction id |
| `bulk_transaction.<status>` | The batch id |
| `balance.created` | The balance id |
| `balance.monitor` | The id of the monitor that fired |
| `identity.created` | The identity id |
| `ledger.created` | The ledger id |
| `system.error` | No aggregate exists, so the field falls back to the message key, which for this event type is the event name itself |

### Parsing `occurred_at`

Timestamps are RFC3339 with sub-second precision, but **the number of fractional-second digits varies**: trailing zeros are omitted, so the same field can arrive as `2026-05-01T12:34:56.789012345Z` on one message and `2026-05-01T12:34:56.5Z` or `2026-05-01T12:34:56Z` on the next. An offset may be written as `Z` or numerically.

Use your language's RFC3339 or ISO 8601 parser. A hand-rolled format string with a fixed number of decimal places will parse most messages and fail on the rest, which is the worst failure mode available — intermittent, data-dependent and hard to reproduce.

### Versioning the envelope

`schema_version` starts at `1` and exists so you can branch on the envelope's shape rather than guess at it. An additive change that a version 1 consumer can safely ignore — a new sibling key, for instance — keeps the version at `1`. A breaking reshaping of the envelope increments it, letting old and new consumers run side by side during a migration.

Two consequences for your consumer, and both matter:

- **Tolerate unknown keys.** Do not fail a message because it carries a key you do not recognise. Dead-lettered messages already carry a seventh key, and additive envelope changes are explicitly permitted at the current version.
- **Read the version rather than assuming it.** Branch on `schema_version` if you depend on envelope shape at all, so a future increment is a decision you make rather than a parse error you discover in production.

### Durability and size

Blnk's producer sets `RequiredAcks` to *all in-sync replicas*, explicitly rather than by default. An acknowledged publish is therefore a durable acknowledgement from the replica set, not merely a successful socket write.

A serialised message is capped at 768 KiB. The cap sits below the 1 MiB default of both the client and a stock broker with room to spare, because a dead-lettered copy of the same event is strictly larger — it carries the failure metadata as well. The headroom is what guarantees that an event which can be published can also be dead-lettered, rather than becoming a row that fails every attempt forever.

## Event Types

Thirteen event strings, and this is the complete set. Every one of them reached the legacy HTTP webhook and every one of them is published to Kafka.

| Event type | Topic | Fires when |
|-----------|-------|-----------|
| `transaction.queued` | `blnk.transactions` | A transaction is accepted and queued for asynchronous processing. Not produced today — see [the note below](#three-names-in-the-vocabulary-are-not-currently-reachable). |
| `transaction.applied` | `blnk.transactions` | A transaction is committed to the ledger and balances have moved. |
| `transaction.scheduled` | `blnk.transactions` | A transaction is recorded for a future effective date rather than applied now. Not produced today — see [the note below](#three-names-in-the-vocabulary-are-not-currently-reachable). |
| `transaction.inflight` | `blnk.transactions` | A transaction is authorised and holding funds, awaiting commit or void. |
| `transaction.void` | `blnk.transactions` | An inflight transaction is voided and its hold released. |
| `transaction.rejected` | `blnk.transactions` | A transaction is refused — insufficient funds, an overdraft limit, or a terminal processing error. |
| `transaction.unknown` | `blnk.transactions` | A transaction reaches a status the event mapping has no name for. Not produced by any current code path — see [the note below](#three-names-in-the-vocabulary-are-not-currently-reachable) and [The `COMMIT` Status](#the-commit-status). |
| `bulk_transaction.<status>` | `blnk.transactions` | A bulk batch reaches an outcome. The suffix is the batch status, so this is a **family** of names, not one — see below. |
| `balance.created` | `blnk.balances` | A balance is created. |
| `balance.monitor` | `blnk.balances` | A balance monitor's condition is met. Fires on every occurrence, so the same monitor produces many of these. |
| `identity.created` | `blnk.identities` | An identity is created. |
| `ledger.created` | `blnk.ledgers` | A ledger is created. |
| `system.error` | `blnk.system` | Blnk raises an internal error notification. **Internal topic — not available to subscribers.** |

The seven `transaction.*` names are derived from the transaction's status by a single mapping, which is why a transaction's whole lifecycle appears under this one prefix.

### Three names in the vocabulary are not currently reachable

`transaction.queued`, `transaction.scheduled` and `transaction.unknown` are part of the event
vocabulary, and the mapping genuinely produces them, but **no transaction currently produces them
on either transport.** The reason is that the transaction's status is normalised *before* the event
name is derived from it: on the execution path `updateTransactionDetails` runs first and collapses
`QUEUED`, `SCHEDULED` and `COMMIT` onto `APPLIED`. A queued, scheduled or committed-inflight
transaction is therefore announced as `transaction.applied`, and the `COMMIT` fall-through to
`transaction.unknown` — described in [The `COMMIT` Status](#the-commit-status) — sits behind that
normalisation.

This is exactly the pre-Kafka behaviour of the HTTP webhook, preserved deliberately so that the two
transports carry identical payloads during the dual-delivery window.

What this means for your consumer:

- **Do not build a handler that waits for one of these three names.** Nothing will arrive.
- **Do tolerate them anyway.** They are documented because they can appear in a future release, and
  because an event type absent from the mapping still resolves to `transaction.unknown` rather than
  being dropped. Treat an unexpected `transaction.*` name as data to log, not as a fatal error.
- **Read `payload.data.status` when you need the authoritative status.** The status field says what
  happened; `event_type` says which mapping produced the message.

Correcting the `COMMIT` fall-through changes what BOTH transports carry, so it belongs in its own
deliberate, announced change rather than alongside a transport migration.

### `bulk_transaction.<status>` is matched by prefix, not by equality

It is the only event string with a variable suffix. The name is composed at runtime as `"bulk_transaction."` concatenated with the batch status, so the suffix set is open and no fixed list can enumerate it. Blnk itself matches the prefix and never the whole string, and **your consumer must do the same**.

A filter written as `event_type == "bulk_transaction.applied"` will silently miss every other batch outcome, and will miss any status added later. Match `event_type` against the `bulk_transaction.` prefix instead.

The statuses emitted today are `applied`, `inflight` and `failed`, giving `bulk_transaction.applied`, `bulk_transaction.inflight` and `bulk_transaction.failed`. Treat that as the current set rather than the contract; a new batch status routes correctly with no change on Blnk's side and no notice to you.

### An unrecognised event type is published, not dropped

If Blnk ever emits an event type absent from the table above, it is routed to `blnk.system` rather than refused or discarded. The outbox row is already committed by the time routing happens, so dropping it would lose a durable event, and `blnk.system` is the safe destination precisely because no subscriber can be granted it — a routing omission cannot deliver a payload to an audience that never asked for it. Blnk logs a warning when this happens; the resolution is always to catalogue the event type, never to rely on the fallback.

## The Payload

**`payload` is the legacy HTTP webhook body, unchanged.** Blnk's webhook body has always been a two-key object:

```json
{
  "event": "<event name>",
  "data": { }
}
```

That entire object — **both keys, verbatim** — is what `payload` carries. Nothing is unwrapped, renamed, filtered or reshaped. An existing webhook body parser therefore keeps working against `payload` with no changes at all: only the transport differs.

This is the strongest guarantee in the pipeline and the reason migrating off webhooks is cheap. If your handler today receives the HTTP body and reads `body["event"]` and `body["data"]`, point it at `message.payload` and you are done.

### What `data` contains

| Event type | `data` |
|-----------|--------|
| `transaction.*` | The transaction object |
| `bulk_transaction.<status>` | `batch_id`, `status` and `timestamp` always; plus `transaction_count` when the status is not `failed`, or `error` when it is `failed` and a message is available. A failed batch with no message carries only the first three. |
| `balance.created` | The balance object |
| `balance.monitor` | The balance monitor that fired |
| `identity.created` | The identity object |
| `ledger.created` | The ledger object |
| `system.error` | `error` and `time` |

These are the same objects the webhook carried, so this table describes what you already receive rather than anything new.

### Why `event_type` repeats the event name

The envelope's `event_type` and the payload's inner `event` always hold the same string, and the duplication is deliberate rather than an oversight. Hoisting the name to the envelope lets you route, filter and shard on `event_type` without parsing the payload at all — which for a large transaction body is the difference between reading one short string and decoding tens of kilobytes of JSON you may then discard.

The cost is one redundant string per message. That trade was made knowingly, and you can rely on the two values agreeing.

### The bytes are never re-encoded

`payload` is carried as raw JSON from capture to delivery. Blnk stores the marshaled body as opaque bytes and splices those same bytes into the envelope at publish time; it never decodes them into a map and re-encodes them.

That is a mechanism, not a nicety, and two guarantees depend on it:

- **Payload equivalence during the migration window.** While both transports are running, the Kafka message and the legacy HTTP body are produced from the same stored bytes, so they cannot drift apart.
- **Byte-faithful replay.** A replayed dead-lettered event matches the original byte for byte, because replay re-publishes the stored bytes rather than re-marshalling a struct.

Re-encoding would break both silently: a JSON round trip reorders object members, renormalises number literals and rewrites escape sequences, all without changing the value any parser sees.

> Blnk also keeps a parsed copy of the payload for SQL-side triage queries. That copy is never read as the message body, for exactly the reasons above.

## Delivery Guarantees and Your Idempotency Obligation

Read this section before you write a consumer. It states what Blnk guarantees, what it does not, and the one thing it needs you to do.

**1. Every event is recorded once, on the write side.** Blnk uses a transactional outbox: the event row is written inside the same database transaction as the ledger mutation that produced it. The mutation and its event commit together or neither commits, so an applied transaction cannot exist without its event and an event cannot exist for a mutation that rolled back. This is **write-side** exactly-once *capture*: a property of the database transaction, and the only sense in which that phrase applies anywhere in this pipeline.

**2. Kafka delivery is at-least-once.** That is the delivery guarantee, full stop. Blnk makes no stronger promise about delivery, and it does not use Kafka transactions or the idempotent-producer path to manufacture one.

**3. Duplicates happen, and they are expected.** A relay publishes an event and then marks its outbox row dispatched. Those are two operations against two systems with no transaction spanning them, so a crash in between leaves a row that was published but not marked — and the next relay to claim it publishes it **again**. That window is deliberately left open: marking the row first would lose events instead of duplicating them, and a duplicate is recoverable at your end while a loss is recoverable nowhere. A redelivery is normal operation, not a defect.

**4. `event_id` is your idempotency key, and deduplicating on it is your responsibility.** It is unique in Blnk's outbox table and it is stable across redeliveries of the same event. **Blnk does not deduplicate for you.**

During the dual-delivery window the legacy HTTP leg carries the same value in the
`X-Blnk-Event-Id` header, so **one idempotency store serves both transports** and an event you
already handled over the webhook is suppressed when it arrives over Kafka.

### What to do about it

Persist the `event_id` of every event you have finished processing, and skip any event whose id you have already recorded. A unique constraint on `event_id` in your own store, or a set with a retention window comfortably longer than your longest outage, is enough — the operation just needs to be atomic with whatever side effect you take, so that a crash cannot leave the effect applied and the id unrecorded.

Make your handlers idempotent as well where you can. Duplicate suppression protects you from Blnk redelivering; an idempotent handler protects you from your own retries too.

### Two event types take a fresh id every time

Most event ids are **derived** from the mutation's identity, so a mutation replayed after an ambiguous failure computes the same id and is caught by the unique index rather than becoming a second event.

Two are deliberately not, and you should know which:

- **`balance.monitor`** fires every time its condition is met. Deriving its id from the monitor would collapse every firing after the first into a duplicate the index rejects, and the alerts would silently stop.
- **`system.error`** is emitted per occurrence. Two identical messages a second apart are two events an operator needs to see twice.

These are repeatable by nature, so a fresh id is the correct answer for them. Your deduplication still works — it will simply never match, which is what you want. What it means in practice is that these two event types can legitimately deliver near-identical bodies, and you should not treat that as a bug in the pipeline.

### The one exception: `balance.monitor` is at-most-once

Guarantee 1 above — the event committing atomically with its mutation — holds for every event type
but one. `balance.monitor` is the single event type whose capture is **not** atomic with its
mutation, and the reason is structural rather than incidental. A monitor fires because a condition
was met on a balance that a transaction has ALREADY committed: by the time the alert exists there is
no open transaction left to enrol it in, and monitor conditions are evaluated after the commit by
design.

What this means in practice:

- The insert of a `balance.monitor` event is retried on a transient database failure — a bounded
  budget of three attempts at 200ms then 400ms, with every attempt logged — and each attempt
  re-sends the same event id, so a retry cannot deliver the alert twice.
- If the whole budget is spent, or if the process dies in the window between the balance commit and
  the insert, **the alert is lost**. The balance movement stands; the notification does not exist
  and there is nothing to replay, because no row was ever written.
- A loss is never silent. It is logged at ERROR with the event id, the event type and the topic, and
  it is escalated as a `system.error` event, so an operator can detect a missed monitor alert rather
  than infer it.

Treat `balance.monitor` as an at-most-once alerting signal. If your use case cannot tolerate a
missed alert, poll the balance you care about — `GET /balances/:id` — rather than relying on the
event alone. **Every other event type carries the full transactional guarantee above.**

## Partitioning and Ordering

Every message carries a **partition key**, and it is always set. The key is hashed by a stable balancer to select a partition, so all messages sharing a key land on one partition, and Kafka preserves order within a partition.

**Ordering is guaranteed per partition key. It is not guaranteed across a topic.** Two events with different keys carry no ordering relationship at all, even on the same topic and even if one was committed to the ledger before the other. Design your consumer around that: order within a key is something you can rely on, order between keys is something you must not.

### The key is not simply "the ledger id"

This is worth stating plainly because it is easy to assume otherwise. The key is whichever aggregate the event's ordering should follow, and for the highest-volume event type in the system — transactions — that is a **balance** id, not a ledger id.

| Payload | Partition key |
|---------|--------------|
| Transaction | The source balance, falling back to the destination balance, then the transaction id |
| Balance | The ledger, falling back to the balance id |
| Balance monitor | The watched balance, falling back to the monitor id |
| Identity | The identity id |
| Ledger | The ledger id |
| Bulk batch | The batch id |

If none of those yields a value, the key falls back to `aggregate_id`, then to the event type, then to a fixed sentinel. The chain exists so that a key is always present and always deterministic — an absent key would let Kafka scatter the message round-robin and destroy ordering with nothing in the data to show it. Falling back to the event type is also why `system.error`, which has no aggregate of any kind, gets a single partition and therefore a total order, which is what an error stream wants.

Two of those choices are deliberate and worth knowing:

- **A transaction keys on its source balance** because that is exactly what Blnk's internal transaction queue already shards on. Kafka partitioning and queue sharding therefore agree, and the ordering you observe matches the ordering the ledger itself imposes on that balance. The source is also stable across a transaction's whole lifecycle, so `transaction.queued`, `transaction.inflight` and `transaction.applied` for one transaction share a partition and can never be observed out of order.
- **A balance keys on its ledger**, so every balance event in one ledger is co-located and mutually ordered.

Because a key can group several aggregates — every balance of one ledger, every transaction against one balance — the guarantee is stated per partition key rather than per aggregate. That is the stronger, more useful reading: you get ordering across a whole related set, not merely within a single entity.

### The balancer

Blnk uses the Murmur2 balancer, which reproduces the Java client's default partitioner exactly. A message Blnk produces for a given key therefore lands on the partition a Java or librdkafka producer would have chosen for the same key. That matters if you reason about partition assignment from the key, or if anything other than Blnk ever writes to these topics.

A message with no key would be spread across partitions rather than pinned to one, which is also the Java behaviour — but in practice the fallback chain above means Blnk never publishes an unkeyed event.

### Ordering also depends on the relay

Keying is necessary but not sufficient. Blnk's relay claims outbox rows in **occurrence order** and publishes them in that order, so the sequence reaching a partition is the sequence in which the mutations happened. Ordering is therefore a property of the key *and* the claim, not the key alone.

The claim additionally returns **at most one row per partition key**, across all concurrent relay
instances rather than merely within one. Two events sharing a key can therefore never be in flight
simultaneously, which is what makes ordering hold when the relay is scaled out. One consequence is
worth planning for: a stuck event holds up later events sharing its partition key even when they
belong to another category. That is a deliberate correctness-over-throughput trade, bounded by the
retry budget, and it is why a dead-letter age alert also implies delayed sibling events for the same
key.

### Topic geometry

| Setting | Variable | Default |
|---------|---------|---------|
| Partitions per topic | `KAFKA_MIN_PARTITIONS` | `6` — the required minimum |
| Replication factor | `KAFKA_REPLICATION_FACTOR` | `3` |

**The replication factor is configuration, not a constant, and it has to be.** Production runs at 3. The single-broker local development stack runs at **1**, because a one-broker cluster cannot satisfy a replication factor of 3 — the broker rejects topic creation outright with an invalid-replication-factor error. Hard-coding 3 would make local bring-up impossible; hard-coding 1 would quietly ship a production cluster with no replicas. So it is a variable, and the local Compose stack sets it to 1 explicitly.

> Adding partitions to a topic that already holds messages changes which partition a key hashes to, which breaks ordering for keys already in flight. Blnk therefore requires explicit operator consent before it will grow a non-empty topic. Plan your partition count up front.

## Dead-Letter Topics and the `<topic>.dlt` Naming Convention

This section publishes a naming convention. Read it before you name a dead-letter topic of your own.

### `<topic>.dlt` names are Blnk-owned

The dead-letter sibling of a Blnk topic is that topic's name with `.dlt` appended. Blnk **creates, writes to and manages** all five:

- `blnk.transactions.dlt`
- `blnk.balances.dlt`
- `blnk.identities.dlt`
- `blnk.ledgers.dlt`
- `blnk.system.dlt`

The rule generalises: for any topic Blnk owns, Blnk also owns `<topic>.dlt`. If your deployment sets a different `KAFKA_TOPIC_PREFIX`, the owned set moves with it — `acme.transactions.dlt` and so on. The suffix is applied once and only once, so a name is never derived twice into `blnk.transactions.dlt.dlt`.

### Why this is published at all

**So that your own consumer-side dead-lettering does not collide with a Blnk-owned name.** That is the entire reason this convention is written down.

Consider what a collision costs. A subscriber that decides to route its own unprocessable messages to `blnk.transactions.dlt` would be writing into Blnk's dead-letter inventory. Blnk's operators triage that topic, count it, alert on the age of its oldest entry, and replay from it. Foreign messages there mean a triage queue full of records that are not Blnk's failures, alerts firing on a backlog nobody at Blnk can clear, and a replay surface polluted with events Blnk never published.

**Name your own dead-letter topics outside the `<topic>.dlt` namespace.** Anything unambiguous works — `myservice.blnk-transactions.failed`, `acme-consumer.dlq`, whatever fits your conventions — as long as it is not a Blnk topic name with `.dlt` appended.

### Blnk does not manage subscriber-side dead-lettering

Stated plainly, because the boundary matters: **Blnk publishes this naming convention and stops there.**

Blnk does not provide, and will not provide:

- a consumer or consumer-group library of any kind,
- subscriber-side dead-letter management — creating, reading, draining or replaying a dead-letter topic that you own,
- a consumer error-handling or poison-message framework.

Your consumption failures are yours to handle. Blnk's dead-letter topics hold events **Blnk** could not publish, which is a different problem from an event you could not process. Choose your own library, your own retry policy and your own dead-letter topic; the only constraint Blnk places on you is the name.

### What Blnk's dead-letter topics contain

When an event exhausts its publish retry budget, Blnk writes it to its category's dead-letter topic with a `failure_metadata` object attached, then records the outcome on the outbox row — `failed` once the budget is spent, and `dead_lettered` once the dead-letter write has succeeded. Only a `dead_lettered` event is eligible for replay.

`failure_metadata` is attached as an **additive sibling key** at the top level, alongside the six envelope keys. It is never nested inside `payload`, never replaces `payload`, and never rewrites, reorders or removes any envelope key — which is precisely what leaves the original event recoverable unchanged and makes a replay byte-faithful.

```json
{
  "event_id": "9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b",
  "event_type": "transaction.applied",
  "aggregate_id": "txn_6c8e5f3a91d24b0e8f7a2c5d",
  "occurred_at": "2026-05-01T12:34:56.789012345Z",
  "payload": {
    "event": "transaction.applied",
    "data": {
      "transaction_id": "txn_6c8e5f3a91d24b0e8f7a2c5d",
      "status": "APPLIED"
    }
  },
  "schema_version": 1,
  "failure_metadata": {
    "original_topic": "blnk.transactions",
    "error_reason": "write tcp 10.0.3.7:52344->10.0.3.9:9092: broken pipe",
    "attempt_count": 5,
    "last_attempted_at": "2026-05-01T12:35:11.884210773Z",
    "first_attempted_at": "2026-05-01T12:34:56.902117640Z"
  }
}
```

Exactly five metadata fields, and no more:

| Field | Meaning |
|-------|---------|
| `original_topic` | The topic the event was destined for, and the topic a replay sends it back to. |
| `error_reason` | Why the final attempt failed. |
| `attempt_count` | How many publish attempts were made before Blnk gave up. |
| `first_attempted_at` | When the first attempt was made. |
| `last_attempted_at` | When the final attempt was made. Together with `first_attempted_at` it bounds the window the failure persisted over, which is what distinguishes a momentary broker blip from a sustained outage. |

If you consume a dead-letter topic, note that the six envelope keys are byte-identical to the message that would have been published, so the same parser handles both — it simply sees one extra top-level key. This is the other reason to tolerate unknown keys.

Triage and replay are operator work. The runbook — how to list dead-lettered events, how to decide whether to replay, and how to replay one — lives in [kafka-operations.md](kafka-operations.md).

## The `COMMIT` Status

A known quirk in the event vocabulary, documented here because subscribers will encounter it.

Transaction event names are derived from the transaction's status by a single mapping, and that mapping has cases for six statuses:

| Status | Event |
|--------|-------|
| `QUEUED` | `transaction.queued` |
| `APPLIED` | `transaction.applied` |
| `SCHEDULED` | `transaction.scheduled` |
| `INFLIGHT` | `transaction.inflight` |
| `VOID` | `transaction.void` |
| `REJECTED` | `transaction.rejected` |
| anything else | `transaction.unknown` |

The `COMMIT` status is **not** in that list. It is a real status, genuinely assigned when an inflight transaction is committed, and because it has no case it falls through the default arm to `transaction.unknown`.

One qualification matters, and it is the reason this is a documented quirk rather than a bug you
will observe: on the execution path the status is normalised to `APPLIED` before the name is
derived, so a committed inflight transaction is announced today as `transaction.applied`. The
fall-through is real in the mapping and pinned by a test, but it sits behind that normalisation —
see [Three names in the vocabulary are not currently
reachable](#three-names-in-the-vocabulary-are-not-currently-reachable).

**This is pre-existing behaviour, not a regression introduced by the move to Kafka.** The same mapping produced the same event name over the HTTP webhook, so a subscriber migrating from webhooks sees no change here.

**It is preserved deliberately.** While both transports are running, Blnk asserts that the Kafka message and the legacy webhook body carry identical bytes for the same event. Adding a `transaction.commit` case would change one side of that comparison and fail it for a reason that has nothing to do with the transport. Behavioural parity is worth more during the migration than a tidier event name.

**It is written down here so it can be corrected as a separate, deliberate change** — one reviewed on its own terms, because introducing a new event name is a subscriber-facing change and deserves to be announced as one rather than slipped in alongside a transport migration.

### What this means for your consumer

Handle `transaction.unknown` without treating it as a failure, but do not build logic that waits
for it: no current code path emits it, because the status normalisation described above resolves
`COMMIT` to `APPLIED` first. A committed inflight transaction arrives as `transaction.applied`
today.

**Read `payload.data.status` whenever you need to know what actually happened.** A committed
inflight transaction carries `"COMMIT"` there even though the event name says `applied`. The
payload's status field is authoritative about the outcome; `event_type` tells you which topic and
which mapping produced the message. If a future release introduces `transaction.commit`, or removes
the normalisation so that `transaction.unknown` begins to arrive, the status field will not change —
which makes it the stable thing to branch on.

## Retry and Dead-Lettering Behaviour

When a publish fails, Blnk's relay retries it on a bounded exponential schedule before giving up.

| Setting | Variable | Default |
|---------|---------|---------|
| Maximum publish attempts | `RELAY_MAX_RETRY_ATTEMPTS` | `5` |
| Base delay | `RELAY_RETRY_BASE_BACKOFF_MS` | `1000` (1 second) |
| Maximum delay | `RELAY_RETRY_MAX_BACKOFF_MS` | `30000` (30 seconds) |

The delay doubles after each failure. At the defaults, five attempts are separated by **four waits — 1s, 2s, 4s and 8s — 15 seconds of backoff in total**.

Note what that means for the 30-second ceiling: **it is never reached at the default settings.** The delay after a fifth failure would be 16 seconds, but a fifth failure exhausts the budget and no sixth attempt consumes it. The ceiling only engages if the base delay or the attempt count is raised. It is stated here as the configured bound, not as a delay you will observe.

Every attempt is logged, including the first, with the attempt number, the budget, the error, the event id and the topic — so a retried publish is visible while it is happening rather than only once it has failed for the last time. Once the budget is spent, the event goes to its category's dead-letter topic as described above.

## Configuration

The variables a subscriber-facing or operator-facing reader needs. The complete set, including transport security and the producer and administrative credentials, is documented with commentary in `.env.example`.

| Variable | Purpose | Default |
|---------|---------|---------|
| `KAFKA_BROKERS` | The broker list Blnk itself publishes to. Empty disables publishing — see below. | *(empty)* |
| `KAFKA_TOPIC_PREFIX` | The namespace every topic name is composed under. | `blnk` |
| `KAFKA_MIN_PARTITIONS` | Partitions per topic. | `6` |
| `KAFKA_REPLICATION_FACTOR` | Replication factor per topic. Set to `1` on a single-broker cluster. | `3` |
| `RELAY_MAX_RETRY_ATTEMPTS` | Publish attempts before dead-lettering. | `5` |
| `RELAY_RETRY_BASE_BACKOFF_MS` | First retry delay, in milliseconds. | `1000` |
| `RELAY_RETRY_MAX_BACKOFF_MS` | Retry delay ceiling, in milliseconds. | `30000` |
| `WEBHOOK_DEPRECATION_SUNSET_DATE` | The instant the legacy HTTP webhook transport is retired. Until then the relay drives both transports from the same outbox row. | *(required once `KAFKA_BROKERS` is set)* |

Both the bare form and the `BLNK_`-prefixed form of each of these are accepted — `KAFKA_BROKERS` and `BLNK_KAFKA_BROKERS` both resolve. Set one form per deployment and you never have to think about it.

> The broker addresses a **subscriber** connects to are configured separately from `KAFKA_BROKERS` and are reported to you when your credentials are issued. `KAFKA_BROKERS` is an internal address inside the deployment; do not assume it is reachable from outside it.

### Running without Kafka is supported

**With `KAFKA_BROKERS` empty, Blnk publishes nothing to Kafka and runs normally.** The event publisher resolves to a no-op, the outbox relay does not start, and the ledger serves, records and processes transactions exactly as it does with Kafka configured. Events are delivered over the legacy webhook transport instead, so nothing is lost — it is simply the state every deployment is in before it opts into Kafka.

This is a legitimate steady state, not a misconfiguration, and Blnk reports it at info level once rather than warning about it. It also mirrors the behaviour the legacy webhook sender always had, which no-opped when no destination was configured. A deployment with no interest in event streaming needs no Kafka, and nothing about it degrades.

## Getting Access

Each subscriber is a Kafka principal with its own SASL/SCRAM credentials and ACLs scoped to the topics it is authorised for and to its own consumer-group namespace. Credentials are issued once, through `POST /subscribers/{id}/kafka-credentials`, which returns the broker endpoint, your topic list, your consumer group id and the credentials themselves. Provisioning, the ACL model and the exact request and response are documented in [kafka-operations.md](kafka-operations.md).

Only the four subscriber-facing categories can be granted: transactions, balances, identities and ledgers. `blnk.system` is internal, and no dead-letter topic is grantable to a subscriber.

## Observability

Publish throughput, per-attempt outcomes, end-to-end capture-to-dispatch latency, dead-letter counts, dead-letter age, consumer lag and outbox backlog are all exported as metrics. The full catalogue, with attribute values and example Prometheus queries, is in [metrics.md](metrics.md) — this document does not duplicate it.

## Consumer Configuration

A practical checklist. The reasoning behind each item is in the sections above.

- **Consumer group.** Use the group id your credentials were issued with. ACLs are scoped to that
  group's namespace; joining another group is refused.
- **Topics.** Subscribe only to the topics your credentials list. Every other topic, and every
  `.dlt` topic, is refused with an authorisation error.
- **Offsets.** Commit offsets after your handler has recorded the `event_id`, so a redelivery after
  a crash is suppressed by your idempotency store rather than reprocessed.
- **Message size.** Blnk refuses to publish an event larger than 768 KiB, so a consumer's
  `max.partition.fetch.bytes` needs no unusual value.
- **Unknown fields.** Ignore envelope and payload members you do not recognise. New members may be
  added within `schema_version` 1, and a dead-lettered message already carries a seventh key.
- **Timestamps.** Parse `occurred_at` with a real RFC3339 parser; the fractional-second precision
  varies between messages.

## Related Documents

| Document | Covers |
|----------|--------|
| [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md) | Migrating off HTTP webhooks: the dual-run timeline, the payload-equivalence guarantee, and what happens at the sunset |
| [kafka-operations.md](kafka-operations.md) | Provisioning topics and principals, the ACL model, dead-letter triage and replay, and the daily outbox-to-offset reconciliation |
| [metrics.md](metrics.md) | The metric catalogue and example queries |
