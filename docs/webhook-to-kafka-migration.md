# Migrating from Webhooks to Kafka

Blnk's HTTP webhook push is being retired and replaced by a Kafka event stream. This document is for an engineer who runs a working webhook receiver today and needs to move it before the retirement date: what you have to do, how long you have, why your existing payload parser keeps working unchanged, and exactly what stops working afterwards.

Read [event-streaming.md](event-streaming.md) first if you have not already. It is the subscriber-facing contract — the topic catalogue, the `LedgerEvent` envelope, the event types and your idempotency obligation — and this document deliberately does not restate any of it.

## What You Need To Do

Five steps. The first four can all be done while your webhook receiver is still running and still being delivered to, which is the entire point of the window.

1. **Get credentials.** Ask your Blnk operator to register you as a subscriber and issue your Kafka credentials — see [Getting Your Kafka Credentials](#getting-your-kafka-credentials). Capture the returned password immediately; it is shown once and cannot be recovered.
2. **Stand up a consumer.** Authenticate with SASL/SCRAM-SHA-512 against the broker endpoint you were given, join the consumer group you were issued, and subscribe to your authorised topics.
3. **Verify in parallel.** Both transports are live during the window, so compare what arrives on Kafka against what arrives at your webhook receiver. They carry byte-identical payloads by construction — see [Payload Equivalence](#payload-equivalence-and-why-it-holds-structurally) — so any difference you see is a bug in your consumer, not a difference between the transports.
4. **Deduplicate on `event_id`.** Delivery is at-least-once, and `event_id` is the deduplication key. This is not optional; [event-streaming.md](event-streaming.md#delivery-guarantees-and-your-idempotency-obligation) states the full obligation.
5. **Decommission the receiver, before the sunset date.** Then have your operator call `DELETE /subscribers/:subscriber_id/webhook-subscription`, which forgets your legacy URL and records your cutover.

> One idempotency store covers both transports. During the window every legacy HTTP delivery carries its event id in the `X-Blnk-Event-Id` header, and it is the **same** `event_id` the Kafka message carries — so an event you already handled over the webhook is suppressed when it arrives over Kafka, and you can migrate without double-processing anything.

## The Dual-Run Timeline

Kafka publishing and legacy HTTP webhook delivery run **concurrently, from the same outbox events, for exactly 30 days**. After that the legacy transport is gone: the deprecated webhook-subscription routes answer `410 Gone` on every request and every method, and the `ProcessWebhook` delivery code is deleted from the codebase.

The window is 30 days by construction rather than by configuration. There is exactly one input — `WEBHOOK_DEPRECATION_SUNSET_DATE`, the RFC3339 instant the legacy transport retires — and the window's opening instant is derived from it as *sunset minus 30 days*. There is no second variable to disagree with the first, so "exactly 30 days" is arithmetic and cannot be misconfigured.

### One decision point, consulted by everything

`WEBHOOK_DEPRECATION_SUNSET_DATE` is read into `config.WebhookDeprecationSunsetDate`, and `event_sunset.go` exposes a single predicate, `WebhookSunsetPassed(now)`, which is **the only place in the codebase that compares an instant to the sunset**. Both consumers of the decision ask it:

- the relay's dual-delivery branch, which decides whether the legacy HTTP leg is still owed for an event it has just published; and
- the API's sunset guard, which decides whether the deprecated webhook-subscription routes still answer.

That matters more than it looks. Split across two independent comparisons, the system could stop dual-writing while still accepting webhook management calls — telling operators the surface is live while subscribers silently stop being delivered to — or the reverse. Neither failure raises anything to notice. With one predicate, the two cannot form different opinions about when the window closes.

### The three states

| State | Kafka publishing | Legacy HTTP delivery | Webhook-subscription routes | `ProcessWebhook` handler |
|---|---|---|---|---|
| **During the window** (`[start, sunset)`) | Active | Active, driven from the same claimed outbox row | Answer normally, with advisory `Sunset` and `Deprecation` headers | Registered |
| **After the sunset** (at or after the instant) | Active — the only transport | Neither enqueued nor delivered | `410 Gone` on every request | Removed |
| **No brokers configured** (`KAFKA_BROKERS` empty) | Nothing published; the publisher is a no-op | Active, exactly as before this feature existed | Answer normally | Registered |

The boundary is inclusive: at exactly the configured instant, the sunset has passed.

**The third state is a legitimate steady state, not a misconfiguration.** With `KAFKA_BROKERS` empty the event publisher resolves to its no-op implementation, the relay does not start, and Blnk serves, records and processes transactions exactly as it did before Kafka existed. Events are delivered over the legacy webhook transport instead, so nothing is lost — it is simply the state every deployment is in before it opts into Kafka. Blnk reports it at info level once rather than warning about it. [event-streaming.md](event-streaming.md#running-without-kafka-is-supported) covers it in full.

### What happens when the date is missing or mis-typed

This is worth knowing precisely, because the behaviour is deliberately strict and one arm of it is not what you would guess.

| Configuration | Behaviour |
|---|---|
| A sunset that is not a valid RFC3339 instant | **Startup is refused.** The error names the variable and the expected layout. This is fatal with or without brokers. |
| No sunset, and `KAFKA_BROKERS` **is** set | **Startup is refused.** The error names the variable, the 30-day window length and how to compute the instant. |
| No sunset, and `KAFKA_BROKERS` is empty | Accepted quietly. There is no transport to migrate to, so there is no window, and the deprecated routes keep answering as they always did. |
| A valid sunset | Normalised to UTC. The window start is back-filled as *sunset minus 30 days*. |

Refusing to start is the point. The date governs two security-relevant behaviours — whether the deprecated, less protected HTTP transport still runs, and whether the deprecated management routes still answer — and an operator who states a retirement instant and mis-types it must not have it silently reinterpreted. Refusing happens before any traffic is served, and it is the only signal that cannot be overlooked.

Should a configuration carrying brokers but no usable window ever reach the runtime anyway — which normal startup now prevents, so this is a second line of defence rather than a routine outcome — `WebhookSunsetPassed` **fails closed** and reports the sunset as already passed. Dual delivery stops and the deprecated routes answer `410 Gone`. That is the safe direction: keeping a deprecated transport alive on the strength of a typo is worse than retiring it loudly, because retiring it early is immediately visible to anyone still consuming webhooks and is recoverable by correcting one variable.

> One state is rejected outright rather than tolerated: a process publishing to Kafka *before* its declared window has opened. The relay refuses to start, because running there would make the real concurrent-delivery period longer than the 30 days subscribers were told about.

### Configuration reference

Only the keys this document names. The complete set is documented with commentary in `.env.example`, and the subscriber-facing subset is in [event-streaming.md](event-streaming.md#configuration).

| Variable | Purpose |
|---|---|
| `WEBHOOK_DEPRECATION_SUNSET_DATE` | The RFC3339 instant the legacy HTTP transport retires. Required once `KAFKA_BROKERS` is set. The window opens 30 days earlier. |
| `KAFKA_BROKERS` | The brokers Blnk itself publishes to. Empty disables publishing. |
| `KAFKA_SUBSCRIBER_BROKERS` | The externally advertised brokers reported to a subscriber when credentials are issued. A **different list** from `KAFKA_BROKERS`, never a fallback for it. |
| `BLNK_WEBHOOK_URL` | The single global legacy webhook destination — see [What Never Existed](#what-never-existed). |

Both the bare form and the `BLNK_`-prefixed form of each Kafka and relay key are accepted: `KAFKA_BROKERS` and `BLNK_KAFKA_BROKERS` both resolve, as do `WEBHOOK_DEPRECATION_SUNSET_DATE` and `BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE`. Set one form per deployment and you never have to think about it.

## Getting Your Kafka Credentials

Each subscriber is a Kafka principal with its own SASL/SCRAM credentials, and its ACLs are scoped to the topics it is authorised for and to its own consumer-group namespace. Credentials are minted by one endpoint:

```text
POST /subscribers/:subscriber_id/kafka-credentials
```

Provisioning completes within a **5-second budget**, enforced both inside the registry and again at the HTTP boundary, so the request always ends inside the budget and always ends with a code that says whether retrying is sensible. It never hangs.

### This endpoint is master-key gated

Subscriber management follows the same privileged-endpoint pattern as hook management: the master key is checked **before any work is done**, and a caller who does not hold it receives `AUTH_MASTER_KEY_REQUIRED`. An ordinary scoped API key will not do. In practice this means credential issuance is an operator action, not something a subscriber performs for itself — so if you are the subscriber, this is the request you ask your Blnk operator to make on your behalf.

### The secret is shown once

Read this before you call the endpoint, because there is no second chance and the failure mode is self-inflicted lockout.

- **The password is returned one time only**, in the response to the issuing call.
- **It is never persisted.** The subscriber registry stores only a **non-reversible reference** and the **issuance instant**, and has no column capable of holding the secret. This mirrors Blnk's API-key posture, where the stored value is a bcrypt hash and the raw key is never kept.
- **No route can read it back** — including this one called again, which mints a **new** credential rather than returning the old one.
- **A lost credential is re-issued, never recovered.** That is the only remedy, and it is a normal, supported operation.

Re-issuing is supported and is **destructive to the previous secret**: Kafka stores one SCRAM credential per principal, so the upsert replaces it and a consumer still using the old password fails at its next handshake. The subscriber id, the principal and the consumer group are unchanged, all being derived from the immutable identifier — so re-issuing costs you one credential rotation, not a new identity.

Capture the password into your secret store in the same step as the call. Do not log the response body.

### The walkthrough

**1. Register the subscriber** (operator, master key). `name` is the only required field. `authorized_topics` must name Blnk-owned subscriber-facing category topics; `webhook_url` records the legacy endpoint you receive pushes on today, so that your cutover can be tracked.

```json
{
  "name": "acme-payments-service",
  "authorized_topics": ["blnk.transactions", "blnk.balances"],
  "webhook_url": "https://events.acme.example/blnk"
}
```

`POST /subscribers` returns the registered subscriber, including the `subscriber_id` to use in step 2 — supply your own canonical identifier or let the service generate one.

**2. Issue the credentials** (operator, master key).

```bash
curl -sS -X POST \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" \
  https://blnk.example.com/subscribers/sub_9f8d3c214b7a5e6f8a120c4d/kafka-credentials
```

The response carries everything a consumer needs — where the brokers are, which topics you may read, which group to read under, and the credential itself:

```json
{
  "brokers": ["kafka-0.blnk.example.com:9094", "kafka-1.blnk.example.com:9094"],
  "broker_endpoint": "kafka-0.blnk.example.com:9094,kafka-1.blnk.example.com:9094",
  "authorized_topics": ["blnk.transactions", "blnk.balances"],
  "consumer_group_id": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d.default",
  "enforced_access": {
    "enforced_by": ["topic", "consumer_group"],
    "topics": ["blnk.transactions", "blnk.balances"],
    "consumer_group_namespace": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d.",
    "partition_key_prefix_enforced": false
  },
  "username": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d",
  "password": "REDACTED_SASL_PASSWORD_SHOWN_ONCE",
  "mechanism": "SCRAM-SHA-512",
  "issued_at": "2026-05-01T12:34:56Z"
}
```

> The `password` above is a placeholder. **The real response carries the generated secret, once.** Everything else in the response is reproducible by re-reading the subscriber; the password is not.

**3. Note which brokers you were given.** `brokers` is the *subscriber-facing* list — the externally advertised addresses — and it is a different list from the one Blnk itself dials internally. Issuance is refused rather than falling back to the internal list, so a successful response never hands you an endpoint you cannot reach.

**4. Configure your consumer.** Authenticate with `SASL/SCRAM-SHA-512` using `username` and the captured password, join `consumer_group_id`, and subscribe to `authorized_topics`. Anything outside your grant is refused at the broker, not filtered by your client.

You are not restricted to the one group id. `enforced_access.consumer_group_namespace` is a **prefix** — note the trailing `.`, which is deliberate and is what keeps two subscribers' namespaces from ever overlapping — and any group id beginning with it is equally usable. `consumer_group_id` is simply the default leaf inside your own namespace, so running several consumer groups is a matter of choosing your own leaf rather than requesting anything.

**5. Deduplicate on `event_id`, and commit offsets afterwards.** Record the `event_id` of every event you have finished processing and skip ids you have already recorded, then commit the offset — so a redelivery after a crash is suppressed by your idempotency store rather than reprocessed.

### What the credential does and does not restrict

Two limits are worth stating outright, because the narrower one is the natural assumption and it is wrong:

- The credential grants **Read and Describe on your authorised topics** and **Read on your consumer-group namespace**, and nothing further. No dead-letter topic is ever granted to a subscriber, so no `<topic>.dlt` appears in `authorized_topics`.
- **Within an authorised topic there is no further restriction.** Kafka authorises at topic and consumer-group granularity and has no message-key dimension, so per-key filtering cannot be enforced by the broker and is not claimed anywhere. `enforced_access.partition_key_prefix_enforced` is always `false`, and it is reported explicitly rather than left to be inferred. If access needs to be narrower, narrow the topic grant — that is the dimension the broker can actually enforce.

The ACL model, the SCRAM parameters and the provisioning procedure are the operator's side of this and are documented in [kafka-operations.md](kafka-operations.md); they are not restated here.

## Payload Equivalence, and Why It Holds Structurally

During the window, the Kafka message payload and the legacy HTTP body for the same event are **identical**. Not equivalent, not compatible — identical, byte for byte.

That is the reason this migration is cheap. Your existing body parser already handles the payload you will receive over Kafka; only the transport changes.

### The bytes exist once

The guarantee is structural rather than a promise to be careful, and the distinction matters because a promise can be broken by a later change while a structure cannot.

Every event is captured once, into a single `blnk.event_outbox` row, inside the same database transaction as the ledger mutation that produced it. The relay then claims that row and drives **both** transports from it: it publishes the stored bytes to Kafka, and — while the window is open — enqueues the legacy HTTP delivery with **the same stored bytes** from the very row it just published.

No domain call site sends a webhook any more. The relay is the legacy transport's only caller, and it reads the payload rather than rebuilding it. The two transports therefore cannot diverge, **because there is only one set of bytes.** There is nothing to keep in step.

The bytes are also never re-encoded on either leg — Blnk splices the stored payload into the Kafka envelope and hands the same buffer to the HTTP delivery, and never decodes it into a map and re-encodes it. A JSON round trip would reorder object members, renormalise number literals and rewrite escape sequences without changing any value a parser sees, which is exactly the kind of drift that is invisible until someone is comparing hashes.

### What the payload is

The marshaled two-key object Blnk's webhook body has always been:

```json
{
  "event": "transaction.applied",
  "data": {}
}
```

Both keys, verbatim, nothing unwrapped or renamed. On Kafka it arrives as the `payload` member of the `LedgerEvent` envelope; over HTTP it arrives as the whole body. [event-streaming.md](event-streaming.md#the-payload) documents the envelope around it and what `data` contains per event type.

### It is verified, not merely asserted

`event_dual_delivery_test.go` holds the proof, and its test names are the claims: every event type carries identical bytes on both transports, neither transport reserialises the stored payload, both legs are driven from the same claimed row, the legacy wire contract is preserved over the shared bytes, and a relay restart does not enqueue the legacy delivery a second time.

That last one is worth spelling out, since a restart is the obvious way a dual-write scheme breaks. The relay records a `webhook_dispatched` marker on the row once the legacy task is enqueued, so a later claim skips the leg entirely. If the process dies after enqueuing but before the marker lands, the re-enqueue is suppressed anyway, because the task carries the event id as its identity and the queue refuses a duplicate — the marker makes the common case cheap, and the task identity makes the crash case correct.

Two further properties follow from the same design, and both are deliberate:

- **A failing webhook never affects the Kafka leg.** The legacy delivery has its own attempt budget. Once an event is on its Kafka topic it is never republished, whatever the HTTP leg does; if the webhook budget is exhausted, that is logged as an abandoned leg — the event is on Kafka and the webhook will never arrive — rather than being retried forever.
- **A row that reaches the sunset with a webhook still owed is settled, not stranded.** The window has ended, and the obligation with it.

## The Breaking Change

**The webhook HTTP contract is intentionally not preserved.** This is a deliberate breaking change, decided rather than overlooked, and it is the one part of this migration that cannot be absorbed transparently.

The payload survives. The transport does not.

### What is removed at the sunset

| Removed | Detail |
|---|---|
| Webhook subscription registration | The deprecated `/subscribers/:subscriber_id/webhook-subscription` routes answer `410 Gone` |
| Legacy URL storage | The recorded `webhook_url` is purged once a subscriber has migrated, and the column is dropped by a later migration |
| HTTP delivery | `webhooks.go` is deleted, taking `processHTTP`, `SendWebhook` and `ProcessWebhook` with it |
| The delivery handler registration | The `WebhookQueue → ProcessWebhook` mapping is unregistered — **the mapping only**, see [What Never Existed](#what-never-existed) |

**Subscribers must migrate to consuming Kafka directly. There is no compatibility shim, and none is planned.** No proxy re-emits Kafka events as HTTP pushes, and nothing accepts a webhook URL after the sunset.

### The `410 Gone` is a typed error code, not a bare status

Worth stating for anyone maintaining this: the sunset guard does not write a status. It aborts with the typed error code `GEN_GONE`, and the error catalog maps that code to HTTP `410`. The mapping is explicit, which is what makes the code and the status impossible to get out of step — the catalog is the single source of truth for every error code's default status, and an unmapped code would silently become a `500`.

So a post-sunset response body carries the same shape as every other error in this API: the flat `error` string alongside the structured `error_detail` object naming `GEN_GONE`. Branch on the code, not on prose.

While an instant is configured, the guarded routes also advertise it on **every** response, on both sides of the boundary, using the RFC 8594 fields:

```text
Sunset: Fri, 31 Jul 2026 00:00:00 GMT
Deprecation: true
```

Both are informational — the refusal is never a function of either — but they mean a client still inside the window is warned by the very responses it is succeeding with, so an integration can schedule its own migration without being told out of band.

### If your receiver verifies signatures, read this

This is the part most likely to be missed, because what replaces the check is not a header.

Today, when a server secret is configured, each delivery is signed: `X-Blnk-Timestamp` carries the unix second, and `X-Blnk-Signature` carries the hex-encoded HMAC-SHA256 of `timestamp + "." + body` under that secret. The timestamp is inside the signed data, so it cannot be tampered with independently, and Blnk warns when it is delivering unsigned because no secret is set.

After migration **there are no HTTP headers to verify, and no signature to check.** Authenticity comes from the connection instead:

- **Authentication** — your consumer proves who it is with SASL/SCRAM-SHA-512 against the broker, using the credential issued to your principal.
- **Authorization** — ACLs restrict that principal to your granted topics and your consumer-group namespace, enforced by the broker.

The trust model moves from *"this request body was signed by someone holding the shared secret"* to *"this record came from a broker I authenticated to, on a topic only Blnk can write to."* That is a stronger position than the HMAC gave you — a signature proves an individual payload's origin, whereas an authenticated broker session establishes the channel's origin once and covers everything on it — but it is a **different** check, in a different place. Do not port signature-verification code across; delete it, and configure SASL instead.

> Verify your consumer actually fails when it should. A misconfigured client that silently falls back to an unauthenticated connection would look identical to a working one for as long as the broker allows it.

### Two symbols outlive the file that holds them

`NewWebhook` and `getEventFromStatus` are relocated into the event package before `webhooks.go` is deleted, rather than being removed with it. This is not tidiness — those two symbols **are** the contract:

- `NewWebhook` is the two-key payload object. It defines the bytes every subscriber parses, on either transport.
- `getEventFromStatus` maps a transaction's status to its event name, and so defines the `transaction.*` event vocabulary.

Deleting them would delete the payload-equivalence guarantee and the event names along with the transport. That is why the event vocabulary outlives the mechanism that introduced it, and why nothing in [event-streaming.md](event-streaming.md#event-types)'s event catalogue changes at the sunset.

## What Never Existed

If you came here expecting to deregister yourself from a subscription API, there is something you should know first, because it changes what "migrating subscribers" can even mean.

**There was never a per-subscriber webhook subscription API.** The entire subscription surface was a single global configuration value — `WebhookConfig{Url, Headers}`, set through `BLNK_WEBHOOK_URL` and `BLNK_WEBHOOK_HEADERS` — and **every** event was POSTed to that **one** URL. There was no registration endpoint, no per-subscriber URL storage, and no way for Blnk to know who its subscribers were. Delivery was a no-op when the URL was unset, which is why a Blnk deployment has always been able to run with no notification sink at all.

So a deployment with several webhook consumers had them behind one endpoint of its own making — a fan-out proxy, a queue, a router — and Blnk knew nothing about that arrangement.

### What replaces it, and why the registry has legacy columns

The subscriber concept is genuinely new. It has to exist anyway, because a Kafka principal and its ACLs need something to be attached to, and `blnk.event_subscribers` is that registry. It carries two columns that exist **only** for this migration:

| Column | Purpose |
|---|---|
| `webhook_url` | The legacy HTTP endpoint a migrating subscriber receives pushes on today. Never required — a subscriber onboarded after the cutover never had one. |
| `migrated_at` | When that subscriber's cutover completed. This is what makes migration progress **queryable** rather than a matter of asking around. |

A small, **explicitly deprecated** management surface exposes them: `POST`, `GET`, `PUT` and `DELETE` on `/subscribers/:subscriber_id/webhook-subscription`. `DELETE` is the cutover action — it forgets the recorded URL and then stamps the migration instant, in that order, so a partial failure reports the subscriber as still awaiting migration rather than over-claiming progress.

That surface is also what makes the sunset behaviour observable at all. Without a subscription route there would be no request on which a `410` could ever be seen, and "the webhook REST API returns `410 Gone`" would be an untestable claim.

The two columns have different retention rules, deliberately. `webhook_url` is a third party's endpoint and an operational detail of somebody else's system, so it is purged once a subscriber has migrated — nulled, not deleted, since the subscriber is still a live subscriber and only the migration artefact expires. `migrated_at` is never purged: it is an audit fact about your own deployment, and it is what progress reporting counts.

### `/hooks` is a different feature and is not affected

This matters because the two share a word and nothing else.

`/hooks` implements `PRE_TRANSACTION` and `POST_TRANSACTION` **request-time callouts with a response contract** — Blnk calls out during transaction processing and the response can influence how that transaction proceeds. That is synchronous interception. What is being retired is asynchronous event *notification*, which tells you something already happened and has no response contract at all.

**`/hooks` stays fully functional. It is out of scope for this migration, it is not guarded by the sunset, and it answers exactly as it does today, before and after the date.** If you use hooks, nothing here requires any action from you.

> For operators: the asynq queue named by `BLNK_QUEUE_WEBHOOK` also **survives the sunset**, because hook execution and the search-index handlers are registered on the same queue and worker. Only the `ProcessWebhook` handler mapping is removed — removing the queue, its configuration key or its worker would silently disable transaction hooks and search indexing. [kafka-operations.md](kafka-operations.md) and the code own the detail.

## Your One Obligation

Kafka delivery is **at-least-once**, and `event_id` is the deduplication key: persist the id of every event you have finished processing and skip any id you have already recorded. [event-streaming.md](event-streaming.md#delivery-guarantees-and-your-idempotency-obligation) states the full obligation, including why duplicates are normal operation rather than a defect, and Blnk does not deduplicate for you.

During the window this costs you nothing extra. The same `event_id` travels on both transports — in the `X-Blnk-Event-Id` header on the legacy leg, in the envelope on Kafka — so **one idempotency store covers both**, and you can run the two side by side without processing anything twice. That also means the store you build for the migration is the store you keep afterwards.

> The legacy queue's own duplicate suppression is bounded and short-lived by design, and it was never intended to cover the migration. Your idempotency horizon is yours to choose against your own storage, which is strictly better than any window Blnk could pick on your behalf.

## Related Documents

| Document | Covers |
|----------|--------|
| [event-streaming.md](event-streaming.md) | The subscriber contract: the topic catalogue, the `LedgerEvent` envelope, the event types, partitioning and ordering, and your idempotency obligation |
| [kafka-operations.md](kafka-operations.md) | The operator side: provisioning topics and principals, the ACL model, dead-letter triage and replay |
| [metrics.md](metrics.md) | The metric catalogue and example queries, including publish throughput, dead-letter counts and consumer lag |
