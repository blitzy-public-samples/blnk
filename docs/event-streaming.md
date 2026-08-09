# Blnk Event Streaming Reference

Blnk publishes every ledger mutation to Kafka as a `LedgerEvent`. This document is the subscriber-facing contract: the topics you consume, the JSON envelope you receive, the guarantees Blnk makes, the obligations it leaves to you, and the topic names Blnk reserves for itself. It replaces the legacy HTTP webhook push, which is retired at the end of the migration window.

The legacy HTTP webhook transport runs alongside Kafka for a fixed 30-day window and is then
retired. The window and the retirement are both governed by one setting,
`WEBHOOK_DEPRECATION_SUNSET_DATE`: while the date is in the future the relay drives Kafka and the
HTTP leg from the same outbox row, so the body a webhook receiver gets is byte-identical to the
`payload` member of the Kafka message; once it has passed the HTTP leg is neither enqueued nor
delivered and Kafka is the only transport.

**What the sunset instant does, and what it does not do.** Passing the date is a *runtime* change
only. Delivery stops, and the deprecated webhook-subscription routes begin answering `410 Gone`.
It does **not** remove any code: the legacy sender, its asynq handler and its configuration block
are all still present and are deleted in a later, manual release. Two consequences follow, and
both matter operationally:

- The change is reversible by configuration. Moving the date back into the future restores dual
  delivery, because nothing was removed.
- The retirement is not self-completing. Someone has to perform the source-removal release; until
  they do, the code is dormant rather than gone.

## Prerequisites

- A Kafka cluster reachable from the Blnk server and worker roles, on a release that is both
  **capable** and **patched**. Those are two different floors and both apply:
  - **Feature floor — 3.5.** `kafka-storage format --add-scram` arrived in Kafka 3.5, and SASL/SCRAM
    in KRaft mode cannot be bootstrapped without it. Nothing below 3.5 can run this pipeline.
  - **Supported floor — 3.9.2 on the 3.x line.** 3.5 being capable does not make it safe: the
    releases between 3.5 and 3.9.1 carry published Apache Kafka security advisories. Run an
    advisory-fixed release — 3.9.2 or later on 3.x, which is what this repository pins for its
    own broker (`apache/kafka:3.9.2` in the Compose stack and in the Kubernetes StatefulSet) — or
    a correspondingly patched 4.x release. Treat 3.5 as the oldest release the *feature* exists
    in, never as a version to deploy.
- `KAFKA_BROKERS` configured on Blnk. **With no brokers configured Blnk publishes nothing to
  Kafka** — the event relay does not start, and a deployment that still has `BLNK_WEBHOOK_URL`
  (`notification.webhook.url`) set keeps receiving the legacy HTTP webhook instead. That is a
  supported steady state, not an error, and it is what lets a deployment migrate on its own
  schedule. See [Running without Kafka is supported](#running-without-kafka-is-supported).
- Credentials issued for your subscriber. Each subscriber is a Kafka principal whose ACLs are
  scoped to its authorised topics and its consumer-group namespace — see
  [Getting Access](#getting-access).

## Topic Catalogue

Events are grouped into four **category topics**, each with a **dead-letter sibling** named by appending `.dlt`. Eight names in total, and they are the complete inventory — Blnk writes to no other topic.

| Category topic | Dead-letter topic | Audience | Carries |
|---------------|-------------------|----------|---------|
| `blnk.transactions` | `blnk.transactions.dlt` | Subscribers may be granted it | Transaction lifecycle, including bulk batch progress |
| `blnk.balances` | `blnk.balances.dlt` | Subscribers may be granted it | Balance creation and balance monitor alerts |
| `blnk.identities` | `blnk.identities.dlt` | Subscribers may be granted it | Identity creation |
| `blnk.system` | `blnk.system.dlt` | Subscribers may be granted it | Ledger creation, Blnk's own error notifications, and any event type the catalogue does not recognise |

All four categories are grantable. There is no internal-only topic in this inventory, so what a
given subscriber can read is decided entirely by the grant it was issued rather than by any
category being withheld from everyone. Read [what granting `blnk.system` discloses](#what-granting-blnksystem-discloses)
before granting it.

### The topic prefix

Every one of those eight names is composed as `<prefix>.<category>` and `<prefix>.<category>.dlt`, where the prefix is `KAFKA_TOPIC_PREFIX` and defaults to `blnk`. A deployment that sets `KAFKA_TOPIC_PREFIX=acme` therefore consumes `acme.transactions`, `acme.transactions.dlt`, `acme.balances`, and so on down the table. The category tokens themselves — `transactions`, `balances`, `identities`, `system` — never change.

Nothing Blnk writes to production routes on a literal topic name. `model.EventCategory` resolves an event type to its category token and `event_topics.go` composes the topic name from that token and the configured prefix, so the whole namespace moves together when the prefix changes. Literal names do appear where a name is being *described* rather than resolved — in this document, in the provisioning scripts, in operator commands and in test fixtures — so treat those as illustrations of the default prefix, not as the routing rule.

> A prefix written with a trailing dot (`KAFKA_TOPIC_PREFIX=acme.`) is trimmed rather than concatenated, so it yields `acme.transactions` and not `acme..transactions`. Kafka would happily accept the latter as a distinct topic that nothing is subscribed to.

### Renaming the topic namespace

Changing `KAFKA_TOPIC_PREFIX` on a deployment that already has events in flight requires one more variable, and leaving it out stops delivery of everything captured before the change.

An outbox row records its **fully-resolved destination topic when it is written**, which is what keeps a committed event bound to the topic it was always meant for. So after the rename the rows already in the table still name the previous generation — `blnk.transactions` — while the publisher, topic assurance and the ownership check all speak of the new one. A process that was already running published those rows without trouble, because it had built a writer for every old topic at start-up. The process that comes up next builds its inventory from the new configuration alone, and every stored old row is then refused: it stays claimable, each attempt logs the refusal, and nothing drains it. The dead-letter write of a failing old row and the replay of an already dead-lettered one are refused for the same reason.

Declare the previous namespace to close that gap:

```
KAFKA_TOPIC_PREFIX=acme
KAFKA_HISTORICAL_TOPIC_PREFIXES=blnk
```

Every name in that list is treated as owned exactly as `KAFKA_TOPIC_PREFIX` is — writers pre-created for its whole inventory, the ownership check admits it, topic assurance keeps its topics present, and its topic names appear verbatim in metrics rather than collapsing to the `unowned` label. It does **not** change where new events go: `KAFKA_TOPIC_PREFIX` alone decides that, so the old generation drains while the new one fills.

**Drain the list, do not accumulate it.** Remove a prefix once no non-terminal and no replayable row still names it; `GET /events/stats` and the dead-letter inventory are how you tell. At most four may be declared, because each one pre-creates a writer per topic on every process start.

If you rename and forget the variable, the server says so: it audits the outbox at start-up and logs, by name, any namespace its rows still name and the deployment no longer owns, together with the number of events waiting behind it and the value to add.

It is an explicit allowlist rather than "accept any prefix with a known category" on purpose. The looser rule would also admit `attacker.transactions`, which has exactly the same shape as a real topic name, and writer resolution is the one place a stored string becomes an outbound connection carrying Blnk's own producer credentials.

As a subscriber you are unaffected either way: you are granted topics under the **current** prefix, and a historical namespace is never granted. What you notice is that events captured before the rename arrive on the old topic you were already reading, rather than stopping.

### Why four categories, and what `blnk.system` costs you

The three category topics the requirement names — transactions, balances, identities — do not cover everything Blnk emits. `ledger.created` and `system.error` belong to none of them, while the coverage rule admits no exceptions: every event type that reached the legacy webhook sender is published to Kafka. One further category closes that gap, following the identical naming convention, so nothing about the scheme is special-cased and each dead-letter sibling is derived by the same rule as every other.

**`blnk.system` carries both of those event types, and that is what makes it the one category to grant deliberately.** `system.error`'s payload is a frozen legacy contract that carries verbatim error text, and error text names internal detail: database schemas, tables and routines, broker addresses. That body cannot be narrowed without breaking the payload guarantee below, so the disclosure is contained by AUDIENCE — by which subscribers are granted the topic — rather than by redaction. It is also the catalogue's catch-all: an event type Blnk adds later without extending the table lands somewhere durable, observable and replayable, and on the topic the fewest subscribers hold.

> **Read this if you consume `ledger.created` over webhooks today.** It shares `blnk.system` with `system.error`, so ask your operator to include `blnk.system` in your `authorized_topics` and you will receive it over Kafka exactly as you receive it over webhooks now. The grant is a deliberate decision rather than a default, because it also gives you `system.error` and any event type Blnk has not yet catalogued — so if you do not consume ledger creations, do not ask for it. A fifth, subscriber-facing `blnk.ledgers` topic was implemented to close the gap and then removed, because the topic catalogue is a published contract and adding to it obliges every subscriber that wants universal coverage to hold a grant it was never told about. Withholding the category instead was tried too, and it was worse: it turned a disclosure question into a lost event.

The system category is not a placeholder. Folding its events into an unrelated topic would corrupt that topic's meaning for everyone filtering on it, and dropping them would breach coverage outright.

## The `LedgerEvent` Envelope

Every message value on a **category topic** is a JSON object with exactly these six keys. None of them is omitted when its value is empty, so a subscriber can rely on all six being present on every message.

> **A dead-letter topic carries a seventh key.** A message on a `.dlt` sibling is this same six-key envelope with `failure_metadata` added at the top level — byte-identical in the six keys, plus one. If you consume both, size your parser for seven and see [What Blnk's dead-letter topics contain](#what-blnks-dead-letter-topics-contain).

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

`aggregate_id` names the subject of the event. It is **not** the Kafka message key; the key controls partitioning and is derived separately, as [Partitioning and Ordering](#partitioning-and-ordering) explains. **For most event types the two differ**, because the key is the event's ledger while the aggregate is the entity acted on — a `transaction.applied` names the transaction here and is keyed on the ledger. They coincide only where the entity *is* the ledger (`ledger.created`) or where there is no ledger to key on (`identity.created`, `bulk_transaction.<status>`). Group on `aggregate_id`; do not infer the key from it.

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

Thirteen catalogue entries, and this is the complete set. Twelve are fixed names; the thirteenth, `bulk_transaction.<status>`, is a family whose suffix is the batch's status, so the number of distinct strings on the wire is a little larger than the number of rows below. Every entry reached the legacy HTTP webhook and every entry is published to Kafka.

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
| `ledger.created` | `blnk.system` | A ledger is created. Ask your operator for `blnk.system` in your `authorized_topics` — it is granted deliberately rather than by default, because it shares the category with `system.error`. See [what granting `blnk.system` discloses](#what-granting-blnksystem-discloses). |
| `system.error` | `blnk.system` | Blnk raises an internal error notification. Grantable on the same terms, and the reason the grant is a deliberate one: this body carries Blnk's own error text verbatim. |

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

If Blnk ever emits an event type absent from the table above, it is routed to `blnk.system` rather than refused or discarded. The outbox row is already committed by the time routing happens, so dropping it would lose a durable event, and `blnk.system` is the narrowest destination in the catalogue — it is granted deliberately, per subscriber, and only to subscribers that need ledger events — so a routing omission cannot reach every subscriber at once. Blnk logs a warning when this happens; the resolution is always to catalogue the event type, never to rely on the fallback.

## The Payload

**`payload` is the legacy HTTP webhook body, unchanged.** Blnk's webhook body has always been a two-key object:

```json
{
  "event": "<event name>",
  "data": { }
}
```

That entire object — **both keys, verbatim** — is what `payload` carries. Nothing is unwrapped, renamed, filtered or reshaped. An existing webhook body parser therefore keeps working against `payload` with no changes at all: only the transport differs.

Be precise about what the guarantee covers. **`payload` is byte-identical to the HTTP body; the Kafka
message is not.** The message is the envelope — `payload` plus the five other keys — so a consumer
reads `message.payload` and hands *that* to its existing parser, rather than handing it the whole
message.

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

The outbox row holds two byte buffers, and knowing which is which is what keeps the equivalence claim below precise. One is the legacy webhook body exactly as it was marshaled; the other is the complete `LedgerEvent` envelope, with the first spliced into it as the nested `payload`. Kafka publishes the envelope; the legacy HTTP leg posts the body. So the two transports do **not** carry identical messages — an envelope is strictly larger than the body nested inside it — and the guarantee is narrower and exact:

That is a mechanism, not a nicety, and two guarantees depend on it:

- **Payload equivalence during the migration window.** While both transports are running, the
  Kafka message's `payload` member and the legacy HTTP body are spliced from the same stored bytes,
  so they cannot drift apart. Note the scope precisely: it is the `payload` **member** that equals
  the HTTP body, not the whole Kafka message, which additionally carries the five envelope keys the
  webhook never had.
- **Byte-faithful replay.** A replayed dead-lettered event matches the original byte for byte, because replay re-publishes the stored bytes rather than re-marshalling a struct.

Re-encoding would break both silently: a JSON round trip reorders object members, renormalises number literals and rewrites escape sequences, all without changing the value any parser sees.

> Blnk also keeps a parsed copy of the payload for SQL-side triage queries. That copy is never read as the message body, for exactly the reasons above.

## Delivery Guarantees and Your Idempotency Obligation

Read this section before you write a consumer. It states what Blnk guarantees, what it does not, and the one thing it needs you to do.

**1. Every event is recorded once, on the write side, and three of them have no mutation to be recorded with.** Blnk uses a transactional outbox: the event row is written inside the same database transaction as the ledger mutation that produced it, so the mutation and its event commit together or neither commits. An applied transaction cannot exist without its event, and an event cannot exist for a mutation that rolled back. This is **write-side** exactly-once *capture* — a property of the database transaction, and the only sense in which that phrase applies anywhere in this pipeline.

There is **no event type that is exempt** from that rule by category. What varies is whether a producing mutation exists to be atomic with at all: three event types are produced where none does, and they are named, with their exact residual window, in [Three event types are at-most-once](#three-event-types-are-at-most-once) below. Read that section before you rely on guarantee 1 for one of them.

**2. Kafka delivery is at-least-once.** That is the delivery guarantee, full stop. Blnk makes no stronger promise about delivery, and it does not use Kafka transactions or the idempotent-producer path to manufacture one.

**3. Duplicates happen, and they are expected.** A relay publishes an event and then marks its outbox row dispatched. Those are two operations against two systems with no transaction spanning them, so a crash in between leaves a row that was published but not marked — and the next relay to claim it publishes it **again**. That window is deliberately left open: marking the row first would lose events instead of duplicating them, and a duplicate is recoverable at your end while a loss is recoverable nowhere. A redelivery is normal operation, not a defect.

**4. `event_id` is your idempotency key, and deduplicating on it is your responsibility.** It is unique in Blnk's outbox table and it is stable across redeliveries of the same event. **Blnk does not deduplicate for you.**

During the dual-delivery window the legacy HTTP leg carries the same value in the
`X-Blnk-Event-Id` header, so **one idempotency store serves both transports** and an event you
already handled over the webhook is suppressed when it arrives over Kafka.

### What to do about it

Persist the `event_id` of every event you have finished processing, and skip any event whose id you have already recorded. A unique constraint on `event_id` in your own store, or a set with a retention window comfortably longer than your longest outage, is enough — the operation just needs to be atomic with whatever side effect you take, so that a crash cannot leave the effect applied and the id unrecorded.

Make your handlers idempotent as well where you can. Duplicate suppression protects you from Blnk redelivering; an idempotent handler protects you from your own retries too.

### Where an event id comes from

Most event ids are **derived** from the mutation's identity, so a mutation replayed after an ambiguous failure computes the same id and is caught by the unique index rather than becoming a second event.

Two are deliberately not, and you should know which:

- **`balance.monitor`** fires every time its condition is met. Deriving its id from the monitor would collapse every firing after the first into a duplicate the index rejects, and the alerts would silently stop.
- **`system.error`** is emitted per occurrence. Two identical messages a second apart are two events an operator needs to see twice.

These are repeatable by nature, so a fresh id is the correct answer for them. Your deduplication still works — it will simply never match, which is what you want. What it means in practice is that these two event types can legitimately deliver near-identical bodies, and you should not treat that as a bug in the pipeline.


### Three event types are at-most-once

Three event types are captured by a write that stands alone, for one shared reason: at the moment the event comes into existence there is **no producing mutation** for it to be enrolled in. That is a matter of scope rather than a carve-out — an event describing no open transaction is outside the outbox guarantee rather than excluded from it — which is also why the set is not expected to grow: a producer that has a mutation is capable of being atomic, and every one of them is.

The set is declared in the code, once, as `PostCommitEventCaptureContract`, and this section is asserted against it. Anything not listed here — every per-transaction status event, including those of a **coalesced** batch, and every `ledger.created`, `balance.created` and `identity.created` — is captured inside its mutation's transaction and carries guarantee 1 in full.

| Event type | Where the standalone write is reached | What is lost if it fails |
|-----------|--------------------------------------|--------------------------|
| `balance.monitor` | Only on a deployment with **no Kafka broker**. With a broker configured, the alert is captured inside the balance's own transaction — either evaluated before the write and committed with it, or from a durable handoff row committed with it and evaluated by the handoff processor, which writes the alerts and the handoff's completion in one transaction. | The threshold notification. The balance movement stands. |
| `bulk_transaction.<status>` | Only when the batch **coordinator row is absent**. Normally the summary is inserted in the same transaction as the coordinator's terminal transition, so the outcome and its event commit together. | The batch *summary* only. Every member transaction's own event is atomic with that member's mutation. |
| `system.error` | Always. It describes no mutation — it reports that something failed — so there has never been a transaction it could have joined. | The error notification. Nothing about the ledger. |

Two of the three spend a **bounded retry budget** on that standalone insert — `balance.monitor` and `bulk_transaction.<status>`, the two that describe ledger state — while `system.error` makes a single attempt. That asymmetry is deliberate: retrying is worth a database round trip when the event carries information about ledger state you might otherwise have to reconcile, and `system.error` carries none.

- The retried insert gets three attempts, at 200ms then 400ms, and every attempt is logged. Each attempt re-sends the **same** `event_id`, so a retry cannot deliver the event twice.
- If the budget is spent, if the single attempt fails, or if the process dies inside the window, the event is lost outright: **no row was ever written**, so there is nothing to relay, nothing to dead-letter and nothing to replay. Do not go looking for it in the dead-letter inventory — it can never appear there.
- A loss is never silent. It is logged at ERROR with the event id, the event type and the topic, and a lost `balance.monitor` or batch summary is additionally escalated as a `system.error` event.

#### `system.error` is at-most-once for a different reason, and it is not an exception to R-2

Requirement R-2 binds an event to the transaction of **the mutation that produced it**. `balance.monitor` and `bulk_transaction.<status>` both describe ledger state that a transaction did commit, so for those two the question "why was this not atomic?" has an answer, and a missing event leaves state behind that you may want to reconcile against.

`system.error` has no producing mutation at all — it reports that something failed. There was never a transaction it could have joined, and there is no ledger state behind it. So if you are enumerating the exceptions to R-2, **it is not in this set**: it is standalone by nature rather than by concession. Reconcile a missing `balance.monitor` against the balance and a missing batch summary against the batch; treat a missing `system.error` as a lost operator notification, not as ledger data to recover.

#### What this means for a consumer

Do not infer a batch outcome from the absence of a summary, and do not treat a missing `balance.monitor` as proof that no threshold was crossed — poll the balance you care about with `GET /balances/:id` if a missed alert is unacceptable to you. For everything else, the presence of the mutation implies the presence of the event.


## Partitioning and Ordering

Every message carries a **partition key**, and it is always set. The key is hashed by a stable balancer to select a partition, so all messages sharing a key land on one partition, and Kafka preserves order within a partition.

**Blnk partitions by **ledger id**.** That is the one dimension the ordering guarantee is built on: every event that belongs to a ledger is keyed on that ledger, so a ledger's events are pinned to a single partition and arrive in the order the mutations happened. The events that belong to no ledger reach a documented fallback chain instead: `identity.created`, because an identity is not ledger-scoped; `bulk_transaction.*`, because a batch is a runtime grouping that can span ledgers; and `system.error`, because it describes no ledger object at all. `transaction.rejected` is keyed on its ledger like every other transaction event — the ledger is resolved from the balance the transaction names — and reaches the fallback only when that balance cannot be read, which is itself one of the ordinary reasons a transaction is rejected.

**Ordering is guaranteed per partition key. It is not guaranteed across a topic.** Two events with different keys carry no ordering relationship at all, even on the same topic and even if one was committed to the ledger before the other. Design your consumer around that: order within a key is something you can rely on, order between keys is something you must not.

### The key is the ledger id wherever a ledger exists

**Every event that belongs to a ledger is keyed on that ledger.** That is the partitioning
dimension the design requires, and it is what the producers supply: transaction execution passes
the ledger of the loaded source balance (falling back to the destination's), the balance and ledger
post-action hooks pass the entity's own ledger, the monitor check passes the ledger of the balance
whose update triggered it, and a transaction rejection resolves the ledger from its source balance.
A ledger stated by the producer takes precedence over anything derivable from the payload, and it is
written to both the `ledger_id` column and the message key.

Three event types genuinely belong to no ledger, and for those the key is the aggregate the event
describes. That is not a degraded fallback — it is the only ordering domain those events have.

| Event type | Partition key | `ledger_id` |
|-----------|---------------|-------------|
| `transaction.*` (all seven) | The ledger — from the source balance, falling back to the destination balance | The same ledger |
| `bulk_transaction.<status>` | The **batch id** | `null` |
| `balance.created` | The ledger | The same ledger |
| `balance.monitor` | The ledger of the balance whose update met the condition | The same ledger |
| `identity.created` | The **identity id** | `null` |
| `ledger.created` | The ledger id | The same ledger |
| `system.error` | The **event type**, so the whole stream is one partition | `null` |

Why those three carry no ledger:

- **A bulk batch may span ledgers.** A single bulk request can name transactions whose balances sit
  in different ledgers, so "the ledger of this batch" is not a value that exists. Keying on the batch
  is what makes one batch's progress events mutually ordered, which is the guarantee a consumer of
  batch progress actually needs.
- **An identity is not a ledger-scoped entity.** The same party may hold balances in many ledgers or
  in none, so there is no authoritative ledger to record. Keying on the identity gives one identity's
  events a total order among themselves.
- **`system.error` has no aggregate at all.** Keying on the event type puts the whole error stream on
  one partition and therefore in total order, which is what an error stream wants.

If a payload somehow yields none of the above, the key falls back to `aggregate_id`, then to the
event type, then to a fixed sentinel. The chain exists so that a key is always present and always
deterministic — an absent key would let Kafka scatter the message round-robin and destroy ordering
with nothing in the data to show it.

**What keying on the ledger costs, and it is worth knowing.** Every event of one ledger lands on
**one partition**, so a deployment whose volume is concentrated in a single ledger reads that topic
through a single partition however many the topic has. That is the price of the strongest ordering
guarantee, and it is the guarantee the design asks for. Blnk's internal transaction queue shards on
the *source balance* instead, so Kafka partitioning and queue sharding do **not** agree for
transaction events — an earlier revision of this document claimed they did, and they do not.

Because a key can group several aggregates — every transaction, balance and monitor alert of one
ledger — the guarantee is stated per partition key rather than per aggregate. That is the stronger,
more useful reading: you get ordering across a whole related set, not merely within a single entity.
In particular, `transaction.queued`, `transaction.inflight` and `transaction.applied` for one
transaction share a partition and can never be observed out of order.

### The balancer

Blnk uses the Murmur2 balancer, which reproduces the Java client's default partitioner exactly. A message Blnk produces for a given key therefore lands on the partition a Java or librdkafka producer would have chosen for the same key. That matters if you reason about partition assignment from the key, or if anything other than Blnk ever writes to these topics.

A message with no key would be spread across partitions rather than pinned to one, which is also the Java behaviour — but in practice the fallback chain above means Blnk never publishes an unkeyed event.

### Ordering also depends on the relay

Keying is necessary but not sufficient. Blnk's relay claims outbox rows in **occurrence order** and publishes them in that order, so the sequence reaching a partition is the sequence in which the mutations happened. Ordering is therefore a property of the key *and* the claim, not the key alone.

The claim additionally returns **at most one row per partition key**, across all concurrent relay
instances rather than merely within one. Two events sharing a key can therefore never be in flight
simultaneously, which is what makes ordering hold when the relay is scaled out. One consequence is
worth planning for: while an event is still being retried, later events sharing its partition key
wait behind it even when they belong to another category. That is a deliberate
correctness-over-throughput trade, and it is bounded by the retry budget.

**Exhaustion releases the key.** The claim holds a candidate back only while an earlier row with the
same key is still active — pending or in flight. Once a row has spent its budget and been
dead-lettered it is no longer active, so its siblings proceed immediately. What a dead-letter
therefore produces is a **gap** in that key's sequence, not a permanent stall: the events after it
are delivered in order relative to each other, with the failed one missing until an operator replays
it, and a replay lands after everything published in the meantime. Design for a gap you may have to
reconcile, not for a queue that stops.

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

The dead-letter sibling of a Blnk topic is that topic's name with `.dlt` appended. Blnk **creates, writes to and manages** all four:

- `blnk.transactions.dlt`
- `blnk.balances.dlt`
- `blnk.identities.dlt`
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

An event reaches a dead-letter topic by **one of two routes**, and the distinction matters when you
read `attempt_count`:

- **Retry budget exhausted.** The publish failed repeatedly with errors worth retrying, and the
  attempt budget ran out. `attempt_count` equals the configured budget — 5 at the defaults.
- **Permanent failure, dead-lettered immediately.** The publish failed with an error that retrying
  cannot fix, so Blnk stops rather than spending the remaining budget on a foregone conclusion. A
  message larger than the broker will accept, or an authorization refusal, does not become
  publishable by waiting. **`attempt_count` is then well below the budget, and can be 1.**

A low `attempt_count` is therefore not evidence that the relay gave up early or that the budget is
misconfigured — it is the signature of a permanent failure. Read `error_reason` to tell the two
routes apart.

By either route, Blnk writes the event to its category's dead-letter topic with a
`failure_metadata` object attached, then records the outcome on the outbox row — `failed` once the
publish is abandoned, and `dead_lettered` once the dead-letter write has succeeded. Only a
`dead_lettered` event is eligible for replay.

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
    "first_attempted_at": "2026-05-01T12:34:56.902117640Z",
    "last_attempted_at": "2026-05-01T12:35:11.884210773Z"
  }
}
```

Exactly five metadata fields, and no more:

| Field | Meaning |
|-------|---------|
| `original_topic` | The topic the event was destined for, and the topic a replay sends it back to. |
| `error_reason` | Why the final attempt failed. |
| `attempt_count` | How many publish attempts were made before Blnk gave up. It equals the configured budget when the budget was exhausted, and is **lower than the budget — possibly 1 — when the failure was permanent** and retrying was pointless. |
| `first_attempted_at` | When the first attempt was made. |
| `last_attempted_at` | When the final attempt was made. **Equal or near-equal to `first_attempted_at` on a permanent failure**, because there was only one attempt; that is a valid record, not a missing one. Together with `first_attempted_at` it bounds the window the failure persisted over, which is what distinguishes a momentary broker blip from a sustained outage. |

If you consume a dead-letter topic, note that its six envelope keys are byte-identical to those of the message that would have been published, so the same parser handles both — it simply sees one extra top-level key. This is the other reason to tolerate unknown keys.

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

**It is preserved deliberately, for vocabulary compatibility rather than for transport parity.** Both
transports are produced from the same captured bytes, so adding a `transaction.commit` case could not
make them disagree with each other — the two sides of that comparison cannot drift apart by
construction. What a new name *would* change is the vocabulary itself: every existing receiver has
been consuming this event as `transaction.unknown`, and consumers that switch, filter or route on the
event name would begin seeing a name they have no branch for. Renaming an event that historical
webhook receivers already handle is a breaking change to the event contract, and the migration is
deliberately not the moment to make one. Compatibility with the established vocabulary is worth more
during the migration than a tidier event name.

**It is written down here so it can be corrected as a separate, deliberate change** — one reviewed on its own terms, because introducing a new event name is a subscriber-facing change and deserves to be announced as one rather than slipped in alongside a transport migration.

### What this means for your consumer

**Today a committed inflight transaction is `APPLIED` on both sides of the message.** The
normalisation runs before the transaction is persisted, so the recorded status is `APPLIED`, the
event is captured from that recorded status, and both `event_type` (`transaction.applied`) and
`payload.data.status` (`"APPLIED"`) say so. `COMMIT` does not reach a payload and
`transaction.unknown` does not reach a topic; the fall-through is latent mapping behaviour, pinned
by a test so that it cannot change unnoticed, not a message shape you will receive.

Two things follow for a consumer:

- **Handle `transaction.unknown` without treating it as a failure**, but do not build logic that
  waits for it. Nothing emits it today, and a consumer blocking on it would block for ever.
- **Read `payload.data.status` when you need the outcome rather than the routing.** It carries the
  status the transaction was persisted with, which is the authoritative record of what happened;
  `event_type` tells you which topic and which mapping produced the message. If a future release
  introduces `transaction.commit`, or removes the normalisation so that `COMMIT` survives into the
  payload, the status field is the field that will show it — which makes it the stable thing to
  branch on either way.

## Retry and Dead-Lettering Behaviour

When a publish fails, Blnk's relay retries it on a bounded exponential schedule before giving up.

| Setting | Variable | Default |
|---------|---------|---------|
| Maximum publish attempts | `RELAY_MAX_RETRY_ATTEMPTS` | `5` |
| Base delay | `RELAY_RETRY_BASE_BACKOFF_MS` | `1000` (1 second) |
| Maximum delay | `RELAY_RETRY_MAX_BACKOFF_MS` | `30000` (30 seconds) |

The delay doubles after each failure, so at the defaults the schedule is **1s, 2s, 4s, 8s, 16s** — one delay per attempt in the budget. Every one of those five is computed by the live publish path and stamped on the outbox row's `next_attempt_at`; the fifth appears on the exhausting attempt's log line as `retry_after=16s` beside `retry_after_waited=false`.

How many of them a retried event actually **waits** is a different number, and both are worth knowing. Five attempts have four gaps between them, so the waits are **1s, 2s, 4s and 8s — 15 seconds of backoff in total** — and the fifth delay is recorded rather than waited, because the fifth failure spends the budget and the event is dead-lettered instead of being published a sixth time. Raising `RELAY_MAX_RETRY_ATTEMPTS` turns the fifth delay into a wait as well.

Note what that means for the 30-second ceiling: **it is never reached at the default settings**, because the largest delay the schedule produces is 16 seconds. The ceiling only engages if the base delay or the attempt count is raised. It is stated here as the configured bound, not as a delay you will observe.

**The schedule above is only one of the two ways an event ends up dead-lettered.** It describes a chain of TRANSIENT failures. A **permanent** failure — an unauthorised principal, a topic outside the catalogue, bytes that will never serialise, a record over the size cap, or any failure the publisher does not recognise — is dead-lettered on the attempt that discovered it, with the budget deliberately unspent. So a dead-lettered event with `attempt_count: 1` and a delay schedule that never ran is the expected shape of that route, not evidence of a truncated retry. Which route an event took is recorded as `terminal_reason=budget_spent` or `terminal_reason=permanent_failure` on the relay's log line; the operator-facing triage difference between them is in [kafka-operations.md](kafka-operations.md#two-ways-an-event-becomes-terminal).

Every attempt is logged, including the first, with the attempt number, the budget, the error, the event id and the topic — so a retried publish is visible while it is happening rather than only once it has failed for the last time. That record is emitted **once per attempt**, by the publisher; the relay adds only the durable consequences of the attempt — the retry it scheduled, the budget it spent, and the dead-letter write — so counting attempt lines in the log gives the attempt count and not a multiple of it. Once the budget is spent, the event goes to its category's dead-letter topic as described above.

This schedule applies only to failures **worth** retrying. A permanent failure skips the remaining
schedule entirely and is dead-lettered on the spot, so the backoff you observe for such an event is
none at all — see [what Blnk's dead-letter topics contain](#what-blnks-dead-letter-topics-contain).

## Configuration

The variables a subscriber-facing or operator-facing reader needs. The complete set, including transport security and the producer and administrative credentials, is documented with commentary in `.env.example`.

| Variable | Purpose | Default |
|---------|---------|---------|
| `KAFKA_BROKERS` | The broker list Blnk itself publishes to. Empty disables publishing — see below. | *(empty)* |
| `KAFKA_TOPIC_PREFIX` | The namespace every topic name is composed under. | `blnk` |
| `KAFKA_HISTORICAL_TOPIC_PREFIXES` | Comma-separated namespaces this deployment used to own and must still publish to. Needed only after a `KAFKA_TOPIC_PREFIX` rename — see [Renaming the topic namespace](#renaming-the-topic-namespace). | *(empty)* |
| `KAFKA_MIN_PARTITIONS` | Partitions per topic. | `6` |
| `KAFKA_REPLICATION_FACTOR` | Replication factor per topic. Set to `1` on a single-broker cluster. | `3` |
| `RELAY_MAX_RETRY_ATTEMPTS` | Publish attempts before dead-lettering. | `5` |
| `RELAY_RETRY_BASE_BACKOFF_MS` | First retry delay, in milliseconds. | `1000` |
| `RELAY_RETRY_MAX_BACKOFF_MS` | Retry delay ceiling, in milliseconds. | `30000` |
| `WEBHOOK_DEPRECATION_SUNSET_DATE` | The instant the legacy HTTP webhook transport is retired. Until then the relay drives both transports from the same outbox row. | *(required once `KAFKA_BROKERS` is set)* |

Both the bare form and the `BLNK_`-prefixed form of each of these are accepted — `KAFKA_BROKERS` and `BLNK_KAFKA_BROKERS` both resolve. Set one form per deployment and you never have to think about it.

> The broker addresses a **subscriber** connects to are configured separately from `KAFKA_BROKERS` and are reported to you when your credentials are issued. `KAFKA_BROKERS` is an internal address inside the deployment; do not assume it is reachable from outside it.

### Running without Kafka is supported

**With `KAFKA_BROKERS` empty, Blnk publishes nothing to Kafka and runs normally.** The event publisher resolves to a no-op, the outbox relay does not start, and the ledger serves, records and processes transactions exactly as it does with Kafka configured.

Whether events are still *delivered* in that state depends on a second, independent setting — and
this is the part worth getting right:

| `KAFKA_BROKERS` | Sunset date | What happens to event delivery |
|---|---|---|
| empty | unset, or set to a future instant | Legacy webhook delivery continues exactly as before. Nothing is lost. |
| empty | **set to an instant that has passed** | **Neither transport delivers.** Kafka is not configured, and the legacy leg is retired by the sunset — queued webhook deliveries are dropped and the deprecated management routes answer `410 Gone`. |

The first row is the legitimate steady state: it is where every deployment sits before it opts into
Kafka, Blnk reports it at info level once rather than warning about it, and it mirrors the behaviour
the legacy webhook sender always had, which no-opped when no destination was configured. A deployment
with no interest in event streaming needs no Kafka, and nothing about it degrades.

**The second row is a silent hole, and it is reachable by configuration alone.** The sunset does not
check whether a replacement transport exists before retiring the old one. If you set a sunset date,
have Kafka running before it passes. If you are not migrating, leave the sunset date unset.

## Getting Access

Each subscriber is a Kafka principal with its own SASL/SCRAM credentials and ACLs scoped to the topics it is authorised for and to its own consumer-group namespace. Credentials are issued once, through `POST /subscribers/{subscriber_id}/kafka-credentials`, which returns the broker endpoint, your topic list, your consumer group id and the credentials themselves. Provisioning, the ACL model and the exact request and response are documented in [kafka-operations.md](kafka-operations.md).

All four categories can be granted: transactions, balances, identities and system. `blnk.system` is the one to grant deliberately rather than by default — it carries `ledger.created`, which is why it is grantable at all, and `system.error`, whose body carries Blnk's own error text verbatim — so ask for it only if you consume ledger events. No dead-letter topic is grantable to a subscriber.

### The topic grant is the isolation boundary

Two dimensions of a subscriber's access are enforced at the broker, and they are the two the credentials response enumerates in its `enforced_access` object:

- **`topic`** — literal `Read` and `Describe` on exactly the topics you were granted. Every other topic, including every `.dlt` topic, is refused by the broker rather than filtered by your client.
- **`consumer_group`** — a prefixed `Read` grant reserving your own consumer-group namespace. Joining a group outside it is refused.

All four categories can be granted, `blnk.system` included — it carries `ledger.created` as well as `system.error`, so withholding it would make a currently-delivered event unreachable. It is granted per subscriber rather than by default. No dead-letter topic is grantable to a subscriber.

### Your `partition_key_prefix` is yours to enforce

Your subscriber record may carry a `partition_key_prefix`, and your credential response reports it inside `enforced_access` next to two fields that tell you exactly what it is:

```json
"partition_key_prefix": "ldg_9f1c8a72",
"partition_key_prefix_enforced": false,
"partition_key_prefix_enforced_by": "consumer_side",
"not_enforced_by": ["partition_key"]
```

**Read those two fields before you design around the prefix.** Kafka's authorizer has no message-key dimension: an ACL grants `Read` on a *topic*, so your credential reads **every** record on every topic it is granted, whatever the keys are. The prefix is the scope you are expected to apply *in your consumer*, against the message key — `key.startsWith(prefix)`, a byte-exact prefix test on an opaque identifier, with no case folding and no trimming. Records outside it will be delivered to you; discarding them is your side of the contract.

If you need records outside your scope to be *unreachable* rather than filtered, that is a topic-grant question, not a key question — ask your operator to narrow `authorized_topics` or to publish your domain to a topic of its own. Both are real ACLs.

`partition_key_prefix_enforced_by` is `none` when no prefix is recorded, which means the topic and consumer-group grants are your whole boundary and there is nothing for you to filter. The field is always present, so you can branch on it without first testing whether the prefix is empty.

`not_enforced_by` says the same thing as a list, and it is the one to assert on if you want a test that fails when the contract changes: it carries `partition_key` for every subscriber, and together with `enforced_by` it enumerates every dimension this API names. Compare it against a fixed expectation rather than checking that `partition_key` is absent from `enforced_by` — an absence proves nothing, because a dimension the server never mentions reads identically to one it forgot.

## Observability

Publish throughput, per-attempt outcomes, end-to-end capture-to-dispatch latency, dead-letter counts, dead-letter age, consumer lag and outbox backlog are all exported as metrics. The full catalogue, with attribute values and example Prometheus queries, is in [metrics.md](metrics.md) — this document does not duplicate it.

### Distributed tracing: every record carries W3C trace headers

Every record Blnk publishes carries the standard [W3C Trace Context](https://www.w3.org/TR/trace-context/) headers, so your consumer's own instrumentation can continue the trace without knowing anything about Blnk:

| Header | Contents |
|--------|----------|
| `traceparent` | The trace and span identity of the **publish**, in version `00` format. |
| `tracestate` | Vendor state, when the originating request carried any. Often absent. |

Extract them with whatever your framework already uses — OpenTelemetry's `propagation.TraceContext`, or any library that implements the specification. There is nothing Blnk-specific to install.

**The header names the publish, not the ledger write that caused it.** Those are deliberately different spans, in different traces, joined by a *link* rather than a parent-child relationship. The reason is that they are separated in time: the request that produced the event commits and returns, and Blnk's relay publishes the record up to a poll interval later — and after as many as five retries spanning half a minute. Parenting the publish to the request would report a four-millisecond API call as one that lasted thirty seconds, and would append spans to a trace that had already been exported. So your consumer span attaches to the publish, the publish links back to the capture, and the whole chain is reachable while every duration stays truthful.

**A record may carry no headers at all.** That is not a fault and needs no special handling. It happens when the event was produced with tracing disabled or unsampled — a CLI-driven mutation, a worker-initiated rejection, or a deployment running without observability. Blnk sets no header rather than an empty one, precisely so your instrumentation sees *nothing to extract* instead of an invalid span context to choke on.

**The headers are never in the message value.** They travel as record headers only, which is what leaves the two byte-equality guarantees above intact: the payload bytes a replay reproduces, and the bytes the legacy webhook body is compared against during the migration window, are both unaffected by tracing.

## Consumer Configuration

A practical checklist. The reasoning behind each item is in the sections above.

- **Transport (required in production).** Connect over **`SASL_SSL`** and **validate the broker's
  certificate** — leave hostname verification enabled and do not disable certificate checks. Your
  SASL/SCRAM password authenticates *you to the broker*; only TLS authenticates the *broker to you*
  and encrypts the session. Over plaintext, SCRAM's exchange is observable to anyone on the path and
  nothing proves you are talking to Blnk's broker rather than an interposer. The credential response
  does **not** carry trust material: obtain the CA certificate or trust store from your Blnk
  operator through a separate channel, not from the credential endpoint. `SASL_PLAINTEXT` is for the
  local development stack only.
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
- **Trace headers.** Extract `traceparent` and `tracestate` with a standard W3C propagator if you
  trace. Treat their absence as "no trace to continue", not as an error — see the tracing note
  under Observability.

## Related Documents

| Document | Covers |
|----------|--------|
| [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md) | Migrating off HTTP webhooks: the dual-run timeline, the payload-equivalence guarantee, and what happens at the sunset |
| [kafka-operations.md](kafka-operations.md) | Provisioning topics and principals, the ACL model, dead-letter triage and replay, and the daily outbox-to-offset reconciliation |
| [metrics.md](metrics.md) | The metric catalogue and example queries |
