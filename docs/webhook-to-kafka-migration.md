# Migrating from Webhooks to Kafka

Blnk's HTTP webhook push is being retired and replaced by a Kafka event stream. This document is for an engineer who runs a working webhook receiver today and needs to move it before the retirement date: what you have to do, how long you have, why your existing payload parser keeps working unchanged, and exactly what stops working afterwards.

Read [event-streaming.md](event-streaming.md) first if you have not already. It is the subscriber-facing contract — the topic catalogue, the `LedgerEvent` envelope, the event types and your idempotency obligation — and this document deliberately does not restate any of it.

## What You Need To Do

Seven steps. The first five can all be done while your webhook receiver is still running and still being delivered to, which is the entire point of the window.

1. **Get credentials.** Ask your Blnk operator to register you as a subscriber and issue your Kafka credentials — see [Getting Your Kafka Credentials](#getting-your-kafka-credentials). Capture the returned password immediately; it is shown once and cannot be recovered.
2. **Get the TLS trust material — separately.** The credential response carries your username, password, mechanism, brokers, topics and consumer group. It does **not** carry a security protocol, a CA certificate or any other trust material. Ask your operator for the broker's `SASL_SSL` endpoint and its CA certificate or trust store through a different channel; you cannot derive either from the response.
3. **Stand up a consumer.** Connect with **`SASL_SSL`** — not `SASL_PLAINTEXT` — authenticate with SASL/SCRAM-SHA-512, **validate the broker certificate** with hostname verification left on, join the consumer group you were issued, and subscribe to your authorised topics. SCRAM proves who *you* are to the broker; only TLS proves the broker is Blnk's and encrypts the session.
4. **Verify in parallel.** Both transports are live during the window, so compare what arrives on Kafka against what arrives at your webhook receiver. The Kafka message's `payload` member is byte-identical to the HTTP body by construction — see [Payload Equivalence](#payload-equivalence-and-why-it-holds-structurally) — so any difference you see there is a bug in your consumer, not a difference between the transports.
5. **Deduplicate on `event_id`.** Delivery is at-least-once, and `event_id` is the deduplication key. This is not optional; [event-streaming.md](event-streaming.md#delivery-guarantees-and-your-idempotency-obligation) states the full obligation.
6. **Stop the HTTP delivery itself.** This is the step that actually ends webhook traffic, and it is **not** the API call below. Legacy delivery is driven by the deployment-wide `BLNK_WEBHOOK_URL` (`notification.webhook.url`) and by whatever fan-out sits behind it — not by any per-subscriber record. To stop delivery you must either let the sunset date pass, or have your operator unset `BLNK_WEBHOOK_URL`, or remove your endpoint from the fan-out that forwards to you.
7. **Record the cutover.** Have your operator call `DELETE /subscribers/{subscriber_id}/webhook-subscription`. This forgets your recorded URL and timestamps your migration for tracking purposes. **It does not change delivery** — see [the webhook-subscription routes track migration only](#the-webhook-subscription-routes-track-migration-only).

> One idempotency store covers both transports. During the window every legacy HTTP delivery carries its event id in the `X-Blnk-Event-Id` header, and it is the **same** `event_id` the Kafka message carries — so an event you already handled over the webhook is suppressed when it arrives over Kafka, and you can migrate without double-processing anything.

## The Dual-Run Timeline

Kafka publishing and legacy HTTP webhook delivery run **concurrently, from the same outbox events, for exactly 30 days**. After that the legacy transport stops carrying anything: the deprecated webhook-subscription routes answer `410 Gone` on every request and every method, no legacy delivery is enqueued, and any task already queued is dropped rather than delivered.

**Two distinct things happen, at two different times, and conflating them will mislead you.**

| | What it is | What triggers it | What you have to do |
|---|---|---|---|
| **The runtime cutoff** | Behaviour changes: dual delivery stops, queued legacy tasks are dropped, the deprecated routes answer `410 Gone` | A clock passing `WEBHOOK_DEPRECATION_SUNSET_DATE` | Nothing. No deploy, no restart, no code change |
| **The terminal cleanup release** | Source changes: `webhooks.go` and its tests are deleted, the handler mapping is unregistered, the relay's dual-delivery branch is removed | A human merging that change, at some point after the cutoff | Ship a release |

Everything in the "What is removed at the sunset" table below belongs to the **second** row. Until that release lands, the code is all still present and still compiled in — it simply refuses to do anything. In particular `ProcessWebhook` stays registered on the webhook queue and returns without delivering, which is why a post-cutoff deployment needs no rebuild to be correct.

The window is 30 days by construction rather than by configuration. There is exactly one input — `WEBHOOK_DEPRECATION_SUNSET_DATE`, the RFC3339 instant the legacy transport retires — and the window's opening instant is derived from it as *sunset minus 30 days*. There is no second variable to disagree with the first, so "exactly 30 days" is arithmetic and cannot be misconfigured.

### One resolver, two predicates, and why it is not one predicate

`WEBHOOK_DEPRECATION_SUNSET_DATE` is read into `config.WebhookDeprecationSunsetDate`, and `event_sunset.go` holds `resolveWebhookSunset()` — **the only place in the codebase that turns configuration into a sunset instant**. Every decision below is derived from that one resolver, so no two of them can disagree about when the window closes.

What sits on top of it is **two** exported predicates, because the two consumers are asking genuinely different questions and a single predicate would answer one of them wrongly:

| Consumer | Predicate it calls | The question it is actually asking | True when |
|---|---|---|---|
| The relay's dual-delivery branch (`event_relay.go`) | `WebhookDualDeliveryActive(now)` | *Is the legacy HTTP leg still owed for this event?* | Only inside `[start, sunset)` |
| The API's sunset guard (`api/middleware/sunset.go`) | `WebhookSunsetPassed(now)` | *Must this route now refuse?* | At or after `sunset` — **and also** when Kafka is configured but no usable window is |

The asymmetry is deliberate, and it is the reason one predicate will not do. The two disagree in exactly the cases outside `[start, sunset)`, and they are *supposed* to:

- **Brokers configured, no usable window.** The API guard fails **closed** — the routes refuse — because a deployment that cannot say when the transport retires should not keep serving a surface that promises it still works. The relay's leg is simply inactive. One predicate would have to pick one of those and get the other backwards.
- **Before the window opens** (`WebhookWindowPending`). There is nothing to dual-deliver, so `WebhookDualDeliveryActive` is false, while `WebhookSunsetPassed` is also false because the sunset has not arrived. In practice a Kafka-publishing process **refuses to start** in this state rather than running with the leg switched off — a sunset more than 30 days out is a misconfiguration, not a mode — so it is a startup obstacle rather than a row in the table below.

That the two share a resolver still matters as much as it looks. Split across two independent *comparisons*, the system could stop dual-writing while still accepting webhook management calls — telling operators the surface is live while subscribers silently stop being delivered to — or the reverse. Neither failure raises anything to notice. Deriving both from `resolveWebhookSunset()` is what makes that impossible.

One further property of the relay's predicate is worth knowing when reading the code: it is consulted **immediately before each enqueue**, not once per batch. A batch claimed a second before the sunset takes time to publish, and a decision taken at the top of it would enqueue legacy deliveries after the boundary had passed.

### The four states

| State | Kafka publishing | Legacy HTTP delivery | Webhook-subscription routes | `ProcessWebhook` handler |
|---|---|---|---|---|
| **During the window** (`[start, sunset)`) | Active | Active, driven from the same claimed outbox row | Answer normally, with advisory `Sunset` and `Deprecation` headers | Registered, and delivers |
| **After the sunset** (at or after the instant) | Active — the only transport | Neither enqueued nor delivered | `410 Gone` on every request | Still registered, and drops every task it receives |
| **No brokers configured**, sunset unset or still future (`KAFKA_BROKERS` empty) | Nothing published; the publisher is a no-op | Active, exactly as before this feature existed | Answer normally | Registered, and delivers |
| **No brokers configured, and the sunset has passed** (`KAFKA_BROKERS` empty, sunset at or before now) | Nothing published; the publisher is a no-op | Neither enqueued nor delivered | `410 Gone` on every request | Still registered, and drops every task it receives |

These are RUNTIME states, all four reachable by the same binary. The handler is described as still registered after the sunset because it is: it checks the sunset itself and returns without delivering, so a task that was queued before the boundary and picked up after it is discarded rather than sent. Only the terminal cleanup release removes the registration, and until then this column reads "registered" no matter what the clock says.

The boundary is inclusive: at exactly the configured instant, the sunset has passed. Note that no row in this table describes code being added or removed — every cell is a runtime behaviour selected by configuration and the clock.

**The third state is a legitimate steady state, not a misconfiguration.** With `KAFKA_BROKERS` empty and no sunset that has passed, the event publisher resolves to its no-op implementation, the relay does not start, and Blnk serves, records and processes transactions exactly as it did before Kafka existed. Events are delivered over the legacy webhook transport instead, so nothing is lost — it is simply the state every deployment is in before it opts into Kafka. Blnk reports it at info level once rather than warning about it. [event-streaming.md](event-streaming.md#running-without-kafka-is-supported) covers it in full.

**The fourth state loses events, and configuration alone reaches it.** The sunset retires the legacy
transport without checking that a replacement exists, so an empty `KAFKA_BROKERS` plus a sunset date
that has passed leaves nothing delivering at all. If you set a sunset date, have Kafka running before
it passes; if you are not migrating, leave the date unset.

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
| `KAFKA_SUBSCRIBER_BROKERS` | The externally advertised brokers reported to a subscriber when credentials are issued. **Required for issuance, with no fallback**: left unset, `POST /subscribers/{id}/kafka-credentials` answers `503 SUBSCRIBER_BROKERS_NOT_CONFIGURED` and mints nothing, rather than publishing the internal `KAFKA_BROKERS` addresses. Set it to the same value as `KAFKA_BROKERS` if your subscribers really do run inside the deployment. |
| `BLNK_WEBHOOK_URL` | The single global legacy webhook destination — see [What Never Existed](#what-never-existed). |

#### Which names resolve, exactly

Two different consumers read this configuration, and they do **not** accept the same names.

**Blnk itself** accepts both the bare form and the `BLNK_`-prefixed form of every key in the table
above: `KAFKA_BROKERS` and `BLNK_KAFKA_BROKERS` both resolve, as do
`WEBHOOK_DEPRECATION_SUNSET_DATE` and `BLNK_WEBHOOK_DEPRECATION_SUNSET_DATE`. The prefixed form wins
if you set both. That dual acceptance is not automatic — each key is enumerated in the configuration
overlay — so it holds for the documented keys and should not be assumed for a key you find elsewhere
in the codebase.

**The provisioning scripts** (`scripts/kafka-bootstrap.sh`, `scripts/kafka-provision.sh`) read the
**bare names only**. `KAFKA_BROKERS`, `KAFKA_BOOTSTRAP_SERVER`, `KAFKA_TOPIC_PREFIX`,
`KAFKA_SASL_ADMIN_USER` and the rest must be exported unprefixed for those scripts. Exporting only
`BLNK_KAFKA_TOPIC_PREFIX` configures the server correctly and leaves the scripts on their defaults,
which is how a provisioned topic set ends up not matching the prefix the service publishes to.

> `WEBHOOK_DEPRECATION_START_DATE` is **not** an environment variable, and setting it — in either
> form — has no effect. The window's opening instant is derived as *sunset minus 30 days* and is not
> independently configurable. The requirement fixes the configuration surface at eight variables, and
> a test asserts this one stays out of it.

## Getting Your Kafka Credentials

Each subscriber is a Kafka principal with its own SASL/SCRAM credentials, and its ACLs are scoped to the topics it is authorised for and to its own consumer-group namespace. Credentials are minted by one endpoint:

```text
POST /subscribers/{subscriber_id}/kafka-credentials
```

**What the 5-second budget bounds: the whole request, success or failure.** There is **one absolute
deadline** of 5 seconds, and both the forward path and the cleanup live inside it. Every request ends
with a status code that says whether retrying is sensible; none hangs indefinitely.

The budget is split rather than extended. The forward path — the lookup, the provisioning fence, the
four broker round trips and the issuance record — is bounded by the deadline **minus a 1.25-second
reserve**, so roughly 3.75 seconds. That reserve is not spare capacity: it is held back precisely so
that the failure which most needs compensating, *the deadline expiring*, still has time left to
compensate in. Compensation then runs against the same absolute deadline — one window per request,
shared by every level of cleanup nested inside it — rather than starting a fresh budget of its own.

Why the cleanup matters enough to reserve time for it: the alternative is leaving a live SASL
credential at the broker for a principal the registry records no issuance for — an unaccounted
credential that has to be found and revoked by hand.

The one case that can overrun the deadline is a step that ignores its own context: a driver call that
does not honour cancellation, a blocking syscall, a stop-the-world pause. No deadline arithmetic
reaches a call that never looks at its deadline. When that happens the cleanup is still run, bounded
to a single 1.25-second window granted once per request, so the observable ceiling is roughly **6.25
seconds** — not a routine cost, and if you are seeing it, the thing to investigate is the step that
overran rather than the cleanup. Size client timeouts a little above the 5-second bound, and treat a
response near 6.25 seconds as a symptom to chase.

### This endpoint is master-key gated, and it requires a channel the deployment has established as confidential

Subscriber management follows the same privileged-endpoint pattern as hook management: the master key is checked **before any work is done**, and a caller who does not hold it receives `AUTH_MASTER_KEY_REQUIRED`. An ordinary scoped API key will not do. In practice this means credential issuance is an operator action, not something a subscriber performs for itself — so if you are the subscriber, this is the request you ask your Blnk operator to make on your behalf.

It is also the only endpoint in Blnk whose response contains a secret, so the **channel** is part of its contract. A request over a channel the deployment has not established as confidential is refused with `403` and `error_detail.code` of `SUBSCRIBER_INSECURE_TRANSPORT`, because a correct master key over plaintext HTTP is an authorised credential leak. The operator's side of that is one of: TLS terminated in Blnk (`BLNK_SERVER_SSL`), a declared TLS-terminating proxy (`BLNK_SERVER_TRUST_FORWARDED_PROTO`, with an ingress that sets `X-Forwarded-Proto`), or — on a host declared local-development with `BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE` — a loopback call from inside the container. Only the first is *proven* by this process; the other two are *declared* by the operator, and the loopback one needs a declaration because a same-host reverse proxy makes a public plaintext hop look like loopback from inside the process. See [The transport contract this endpoint requires](kafka-operations.md#the-transport-contract-this-endpoint-requires) in the operations runbook for the full contract, including why enabling the proxy boundary obliges you to make that proxy the only route to the process.

### The secret is shown once

Read this before you call the endpoint, because there is no second chance and the failure mode is self-inflicted lockout.

- **The password is returned one time only**, in the response to the issuing call.
- **It is never persisted.** The subscriber registry stores only a **non-reversible reference** and the **issuance instant**, and has no column capable of holding the secret. This mirrors Blnk's API-key posture, where the stored value is a bcrypt hash and the raw key is never kept.
- **No route can read it back** — including this one called again, which mints a **new** credential rather than returning the old one.
- **A lost credential is re-issued, never recovered.** That is the only remedy, and it is a normal, supported operation.

Re-issuing is supported and is **destructive to the previous secret**: Kafka stores one SCRAM credential per principal, so the upsert replaces it and a consumer still using the old password fails at its next handshake. The subscriber id, the principal and the consumer group are unchanged, all being derived from the immutable identifier — so re-issuing costs you one credential rotation, not a new identity.

Capture the password into your secret store **in the same step as the call**, and never let it reach
a terminal, a shell history, a log or a CI transcript. The walkthrough below shows how.

#### Keep the master key out of `argv`, too

Every example below reads the master key from a **protected curl configuration file** rather than
passing it with `-H`. A header written on the command line is expanded into the process's `argv`,
which is world-readable through `/proc` on Linux for the life of the call and is captured by shell
history and by most CI log collectors. Create the file once:

```bash
umask 077
export BLNK_API="${BLNK_API:-http://localhost:5001}"
export BLNK_CURL_CONFIG="$HOME/.blnk-curl"
printf 'header = "X-Blnk-Key: %s"\n' "$(cat /run/secrets/blnk-master-key)" > "$BLNK_CURL_CONFIG"
```

`umask 077` makes the file mode 0600 at creation, so the key is never briefly world-readable, and the
key is read from the secret store rather than typed — a value typed into an `export` sits in the
shell's history whatever the file's mode is. Pass it with `curl --config "$BLNK_CURL_CONFIG"`, which is
what every example below does; the inline `-H "X-Blnk-Key: ..."` form appears in none of them, so a
step followed verbatim cannot leak the key. It is the same shell environment
[kafka-operations.md](kafka-operations.md#keep-credentials-out-of-process-arguments) and
[metrics.md](metrics.md) establish, so one operator session serves all three documents.

### The walkthrough

**1. Register the subscriber** (operator, master key). `name` is the only required field. `authorized_topics` must name Blnk-owned category topics (any of the four; never a dead-letter topic).

```json
{
  "name": "acme-payments-service",
  "authorized_topics": ["blnk.transactions", "blnk.balances"]
}
```

```bash
curl -sS -X POST --config "$BLNK_CURL_CONFIG" \
  -H 'Content-Type: application/json' \
  -d '{"name":"acme-payments-service","authorized_topics":["blnk.transactions","blnk.balances"]}' \
  "$BLNK_API/subscribers"
```

`POST /subscribers` returns the registered subscriber, including the `subscriber_id` to use in the steps below — supply your own canonical identifier as `subscriber_id`, or omit it and let the service generate one.

**Do not put `webhook_url` in this body.** `POST /subscribers` accepts only `subscriber_id`, `name`,
`authorized_topics` and `partition_key_prefix`; a body naming any other field is refused with `400
GEN_VALIDATION_ERROR` naming it, rather than being accepted with the extra field quietly dropped. The
legacy URL is recorded by the separate call in step 1b, and that separation is deliberate — see
[The generic `/subscribers` routes have no `webhook_url` field at all](#what-replaces-it-and-why-the-registry-has-legacy-columns).

**1b. Record the legacy endpoint** (operator, master key) — **only if this subscriber receives HTTP pushes today.** Skip it entirely for a subscriber onboarded after the cutover; it never had an endpoint, and there is nothing to migrate it from.

```bash
curl -sS -X POST --config "$BLNK_CURL_CONFIG" \
  -H 'Content-Type: application/json' \
  -d '{"webhook_url":"https://events.acme.example/blnk"}' \
  "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f8a120c4d/webhook-subscription"
```

This records the endpoint you receive pushes on today so that your cutover can be tracked, and it clears `migrated_at` in the same statement — recording an address puts the subscriber back into the population awaiting migration. It **does not** start, stop or redirect any delivery: read [The webhook-subscription routes track migration only](#the-webhook-subscription-routes-track-migration-only) before you rely on it. The route is one of the four deprecated ones, so it answers `410 Gone` once the retirement instant has passed; by then there is nothing left to record.

**2. Issue the credentials** (operator, master key).

Write the response **straight to a mode-0600 file** and read the password out of that file. Do not
print it:

```bash
umask 077
resp="$(mktemp)"                 # created 0600 by umask, in the user's private temp dir
trap 'rm -f "$resp"' EXIT INT TERM   # removed even if the shell is interrupted

curl -sS -X POST --config "$BLNK_CURL_CONFIG" \
  -o "$resp" \
  https://blnk.example.com/subscribers/sub_9f8d3c214b7a5e6f8a120c4d/kafka-credentials

# Move the secret into your secret manager directly from the file. Nothing is echoed.
jq -re .password < "$resp" | vault kv put -mount=secret blnk/acme password=-
# or, for a Kubernetes Secret:
# umask 077 && jq -re .password < "$resp" > ./sasl-password
# kubectl create secret generic acme-blnk --from-file=password=./sasl-password
# shred -u ./sasl-password
# (--from-literal would make the secret a command argument: readable in /proc for the
#  life of the process, and recorded by shell history and command-logging audit tools.)

# Keep the non-secret half for your consumer configuration; it is safe to read.
jq '{brokers, consumer_group_id, authorized_topics, username, mechanism}' < "$resp"
```

Three properties of that snippet matter, and each closes a specific leak:

- **The secret never reaches stdout, a terminal, or shell history.** `-o` writes the body to the file;
  only the non-secret fields are printed at the end.
- **The file is 0600 from the moment it exists** — `umask 077` before `mktemp`, not `chmod` after,
  which would leave a readable window.
- **It is deleted even on failure.** The `EXIT INT TERM` trap disposes of the temporary data whether
  the command succeeds, fails or is interrupted.

If your secret manager can be fed by a pipe, prefer the piped form above over any variable: an
environment variable holding the password is visible to every child process and to `/proc`.

The response carries everything a consumer needs *from Blnk* — where the brokers are, which topics you may read, which group to read under, and the credential itself. It does **not** carry TLS trust material; see step 2 of [What You Need To Do](#what-you-need-to-do):

```json
{
  "brokers": ["kafka-0.blnk.example.com:9094", "kafka-1.blnk.example.com:9094"],
  "broker_endpoint": "kafka-0.blnk.example.com:9094,kafka-1.blnk.example.com:9094",
  "authorized_topics": ["blnk.transactions", "blnk.balances"],
  "consumer_group_id": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d.default",
  "enforced_access": {
    "enforced_by": ["topic", "consumer_group"],
    "not_enforced_by": [],
    "topics": ["blnk.transactions", "blnk.balances"],
    "consumer_group_namespace": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d.",
    "partition_key_prefix_enforced": false,
    "partition_key_prefix_enforced_by": "none",
    "gateway_delivery_required": false,
    "broker_record_access": true,
    "exclusive_grant_verified": true
  },
  "username": "blnk-sub-sub_9f8d3c214b7a5e6f8a120c4d",
  "password": "REDACTED_SASL_PASSWORD_SHOWN_ONCE",
  "mechanism": "SCRAM-SHA-512",
  "issued_at": "2026-05-01T12:34:56Z",
  "credential_fingerprint": "sha256:9c1185a5c5e9fc54612808977ee8f548b2258d31",
  "replaced": false
}
```

Every member above is present in every response, with one exception: `partition_key_prefix` is omitted
when no prefix is recorded for the subscriber, which is the case shown here — hence
`partition_key_prefix_enforced_by: "none"` and `gateway_delivery_required: false`. In particular
`not_enforced_by` is **empty for every subscriber**: every dimension the response names is enforced
somewhere, and a subscriber whose prefix nothing would enforce is refused a credential rather than
issued one with an unenforced dimension listed. `credential_fingerprint` is a non-reversible
reference you can compare against the registry to confirm which credential is live without ever
handling the secret; and `replaced` tells you whether this call superseded an existing credential —
`true` means any consumer still using the previous password is now failing authentication.

There is no `client_side_key_filtering_required` member. It existed while a key-scoped subscriber was
granted whole-topic `Read` and asked to discard what was not its own, and it was removed with that
design — cooperation by the party holding the credential is not an access boundary. Nothing replaced
it, because nothing now asks you to filter.

> The `password` above is a placeholder. **The real response carries the generated secret, once.** Everything else in the response is reproducible by re-reading the subscriber; the password is not — which is why the request above writes it to a private file instead of printing it, and why the only field a reviewer or a support ticket should ever see is the redacted projection.

**3. Note which brokers you were given, and check they are reachable from where you are.** `brokers` is
the *subscriber-facing* list, and it comes from **`KAFKA_SUBSCRIBER_BROKERS` alone**. There is no
fallback to `KAFKA_BROKERS`: those are the addresses Blnk dials from inside the deployment, they are
routinely unroutable from outside it, and advertising them would both hand you an endpoint that
cannot connect and publish the internal topology. When your operator has not set a subscriber-facing
list, issuance is **refused** with `503 SUBSCRIBER_BROKERS_NOT_CONFIGURED` before any credential is
minted — so a `200` from this endpoint means an operator deliberately published the address you were
given.

For a subscriber carrying a `partition_key_prefix` the address is different again: it is the
key-authorising component your operator declared (`KAFKA_KEY_SCOPE_GATEWAY_BROKERS`), not a Kafka
broker. Read `broker_endpoint` rather than assuming which of the two you have — see the
`gateway_delivery_required` bullet below.

**4. Configure your consumer.** The example above shows a subscriber with **no** `partition_key_prefix`, which is the ordinary case and the one this step describes: connect with **`security.protocol=SASL_SSL`**, authenticate with `sasl.mechanism=SCRAM-SHA-512` using `username` and the captured password, **validate the broker certificate** against the CA your operator supplied separately (leave hostname verification enabled), then join `consumer_group_id` and subscribe to `authorized_topics`. Anything outside your grant is refused at the broker, not filtered by your client.

**If `gateway_delivery_required` is `true`, stop and read the bullet below instead.** Your principal holds `Describe` and no `Read` on the Kafka brokers, so a consumer pointed at a broker will authenticate and then have every fetch refused with `TOPIC_AUTHORIZATION_FAILED`. Point it at the `broker_endpoint` in your response instead: that is the key-authorising component your operator declared, and it takes the same SASL/SCRAM credential.

`SASL_PLAINTEXT` appears only in Blnk's local development stack. Over plaintext the SCRAM exchange is
observable to anyone on the network path and nothing authenticates the broker to you, so a
man-in-the-middle can present itself as Blnk. The response's `mechanism` field names the SASL
mechanism only — it is **not** a security protocol, and it does not imply TLS.

You are not restricted to the one group id. `enforced_access.consumer_group_namespace` is a **prefix** — note the trailing `.`, which is deliberate and is what keeps two subscribers' namespaces from ever overlapping — and any group id beginning with it is equally usable. `consumer_group_id` is simply the default leaf inside your own namespace, so running several consumer groups is a matter of choosing your own leaf rather than requesting anything.

**5. Deduplicate on `event_id`, and commit offsets afterwards.** Record the `event_id` of every event you have finished processing and skip ids you have already recorded, then commit the offset — so a redelivery after a crash is suppressed by your idempotency store rather than reprocessed.

### What the credential does and does not restrict

Two limits are worth stating outright, because the narrower one is the natural assumption and it is wrong:

- The credential grants **Read and Describe on your authorised topics** and **Read on your consumer-group namespace**, and nothing further. No dead-letter topic is ever granted to a subscriber, so no `<topic>.dlt` appears in `authorized_topics`.
- **`ledger.created` HAS NO KAFKA ROUTE FOR SUBSCRIBERS, and it is the one event you lose at the cutover.** The topic catalogue has four categories, and `ledger.created` shares `blnk.system` with `system.error` — whose payload is a frozen legacy body carrying verbatim internal error text — and with every event type the catalogue does not yet recognise. `blnk.system` is therefore an operator topic: it cannot appear in any subscriber's `authorized_topics`, and a request that names it is refused. A webhook subscriber that consumes `ledger.created` today keeps receiving it over webhooks for the remainder of the window and must plan for the sunset now: read ledgers from the REST API, or agree with your operator how they will relay ledger creations to you. The event itself is not lost — it is captured, published to `blnk.system`, retained and replayable — but the audience is the operator, not you. See [why `blnk.system` is not grantable](event-streaming.md#why-four-categories-and-why-blnksystem-is-not-grantable).
- **Within an authorised topic the BROKER applies no further restriction.** Kafka authorises at topic and consumer-group granularity and has no message-key dimension, so per-key filtering cannot be enforced by the broker and is never claimed to be. That is why a key scope, when one is recorded, is kept *outside* the broker and why the broker-side grant for such a subscriber is narrowed to `Describe`.
- **`enforced_access.gateway_delivery_required` decides which endpoint you dial.** If the subscriber records a `partition_key_prefix`, its principal is granted `Describe` but **not** `Read` on its topics and the Kafka brokers refuse every fetch it attempts; its records arrive from the key-authorising component the deployment declared, whose address `broker_endpoint` carries, and `gateway_delivery_required` is `true` on every response about it. **Blnk does not ship that component and serves no records itself** — there is no read path under `/subscribers` — so where no component is declared such a subscriber is refused a credential with `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` rather than issued one it could not use. When no prefix is recorded the field is `false`, `broker_record_access` is `true`, and the subscriber consumes directly from the brokers confined by its `authorized_topics` alone — which means a granted topic is readable in full, including other ledgers' records on it. **Check this field before you point a consumer at a bootstrap address**: it is the difference between a working migration and a consumer whose every fetch is refused. See [event-streaming.md](event-streaming.md#your-partition_key_prefix-is-enforced-outside-the-broker-and-it-decides-whether-you-get-a-credential-at-all) for the contract and [kafka-operations.md](kafka-operations.md#the-partition-key-prefix-is-enforced-outside-the-broker) for the grant it implies.
- **To narrow what the BROKER enforces, narrow the topic grant** — that is the dimension the broker can actually check.

> **One of the three specified scopes is not an ACL, and cannot be.** The specification asks for ACLs
> scoped to the authorised topics, the consumer group **and a partition-key prefix**. The first two
> are enforced at the broker. The third has no ACL to bind: Kafka's authorizer evaluates cluster,
> topic, group, transactional id, delegation token and user resources, and a message key is none of
> them.
>
> So the third scope is kept **outside** the broker, by a component the deployment declares in
> `KAFKA_KEY_SCOPE_ENFORCEMENT` and addresses in `KAFKA_KEY_SCOPE_GATEWAY_BROKERS`. Blnk narrows the
> broker-side grant to `Describe` so that component is the only path such a subscriber's records can
> take, and it **refuses to mint a credential at all** — `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` — while
> no component is declared. **Blnk does not ship one, and Blnk serves no records itself.**
>
> Two earlier behaviours are worth naming, because each looks like this one and neither was a
> boundary. First, issuance was refused unconditionally and a database `CHECK` constraint made the
> combination unrepresentable, so a subscriber recording a prefix could never obtain a credential and
> clearing the prefix was the only escape; `sql/1781248930.sql` drops that constraint. Then the
> credential was issued with whole-topic `Read` and the response declared applying the prefix to be
> the consumer's own obligation — accurate prose about an absent boundary, since the party asked to
> filter was the party holding the credential.
>
> What the current behaviour keeps from each: the refusal, because a boundary that nothing enforces
> must not be reported as enforced; and two named remedies, because a permanent dead end is
> unacceptable. **Do not read `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` as "this deployment chose not to
> use key scoping."** Read it as "nothing here evaluates message keys yet", and take one of the two
> remedies: have a key-authorising component declared, or clear the prefix and narrow the topic grant
> instead. Separate deployments for data that must not be co-readable remain the third answer.
> Choosing between them is an architectural decision for the people who own the requirement, not
> something this document can settle. Clearing a prefix is never itself refused.

The ACL model, the SCRAM parameters and the provisioning procedure are the operator's side of this and are documented in [kafka-operations.md](kafka-operations.md); they are not restated here.

## Payload Equivalence, and Why It Holds Structurally

During the window, the Kafka message's **`payload` member** and the legacy HTTP body for the same event are **identical**. Not equivalent, not compatible — identical, byte for byte.

Note the scope precisely: it is that one member, not the whole Kafka message. The Kafka message is the
`LedgerEvent` envelope, which wraps the payload in five further keys the webhook never carried. A
consumer hands `message.payload` to its existing parser, not the whole message.

So the migration is cheap, but it is not nothing. Your existing body parser is reusable **unchanged** — you just have to give it the right bytes:

```
webhook receiver:  parse(httpRequestBody)
Kafka consumer:    parse(json.decode(kafkaMessage.value).payload)
```

One added step: decode the envelope, take `payload`, hand it to the parser you already have. What does *not* change is the parser itself, or any of the field names and types inside `data`. What does change is the transport, the envelope you unwrap first, and the deduplication you now owe on `event_id`.

### The payload bytes exist once

The guarantee is structural rather than a promise to be careful, and the distinction matters because a promise can be broken by a later change while a structure cannot.

Every event is captured once, into a single `blnk.event_outbox` row. For the state-change families —
`transaction.*`, `balance.created`, `identity.created`, `ledger.created` — that row is written inside
the same database transaction as the mutation, so the mutation and its event commit together. Other
families are captured just after their source outcome and can be lost before a row exists;
[event-streaming.md](event-streaming.md#three-event-types-are-at-most-once) carries the full
matrix. **Payload equivalence does not depend on which family an event belongs to** — it depends only
on there being one row, which there always is once the event exists at all.

The relay then claims that row and drives **both** transports from it: it publishes to Kafka, and —
while the window is open — enqueues the legacy HTTP delivery from the very row it just published.

No domain call site sends a webhook any more. The relay is the legacy transport's only caller, and it
reads the stored payload rather than rebuilding it. The two transports therefore cannot diverge,
**because there is one canonical payload sequence** and both legs are copies of it.

To be exact about the storage, because "one buffer" would be too loose: the row holds the canonical
payload sequence in `payload_raw`, the fully assembled Kafka envelope in `event_raw`, and a parsed
copy in `payload` for SQL-side triage queries. Three columns, not one buffer. What makes the guarantee
hold is not that a single buffer is shared but that **`event_raw`'s `payload` member is a byte copy of
`payload_raw`**, spliced in rather than re-marshalled, and that the HTTP body is `payload_raw` handed
over unchanged. The `payload` JSONB column is never read as a message body — only for triage. Neither leg ever decodes the payload into a map and
re-encodes it. A JSON round trip would reorder object members, renormalise number literals and rewrite
escape sequences without changing any value a parser sees, which is exactly the kind of drift that is
invisible until someone is comparing hashes.

### What the payload is

The marshaled two-key object Blnk's webhook body has always been:

```json
{
  "event": "transaction.applied",
  "data": {}
}
```

Both keys, verbatim, nothing unwrapped or renamed. On Kafka it arrives as the `payload` member of the `LedgerEvent` envelope — **the member, not the message**; over HTTP it arrives as the whole body. [event-streaming.md](event-streaming.md#the-payload) documents the envelope around it and what `data` contains per event type.

### It is verified, not merely asserted

`event_dual_delivery_test.go` holds the proof, and its test names are the claims: every event type carries identical bytes on both transports, neither transport reserialises the stored payload, both legs are driven from the same claimed row, the legacy wire contract is preserved over the shared bytes, and a relay restart does not enqueue the legacy delivery a second time.

That last one is worth spelling out, since a restart is the obvious way a dual-write scheme breaks. The relay records a `webhook_dispatched` marker on the row once the legacy task is enqueued, so a later claim skips the leg entirely. If the process dies after enqueuing but before the marker lands, the re-enqueue is suppressed anyway, because the task carries the event id as its identity and the queue refuses a duplicate — the marker makes the common case cheap, and the task identity makes the crash case correct.

**That suppression is bounded, and you must not rely on it.** The task identity only prevents a
duplicate while the completed task is still retained in Redis, which is **24 hours**. The bound is
deliberate: retaining task identities across the whole 30-day window would hold every legacy delivery
of those 30 days in Redis — on the order of a billion tasks at the pipeline's target rate — which
would threaten the queue that transaction processing and search indexing also depend on. Blnk trades a
duplicate you can discard in one line for not taking that risk.

So HTTP delivery, like Kafka delivery, is **at-least-once**. A redelivery more than 24 hours after the
original is possible on either transport. **The durable guarantee is your own deduplication on
`event_id`**, which every legacy delivery carries in the `X-Blnk-Event-Id` header — the same key the
Kafka message carries. That lets you hold an idempotency horizon as long as your storage allows,
chosen against your own retention rather than against Blnk's queue, which is strictly better than any
window Blnk could pick on your behalf.

Two further properties follow from the same design, and both are deliberate:

- **A failing webhook never affects the Kafka leg.** The legacy delivery has its own attempt budget. Once an event is on its Kafka topic it is never republished, whatever the HTTP leg does; if the webhook budget is exhausted, that is logged as an abandoned leg — the event is on Kafka and the webhook will never arrive — rather than being retried forever.
- **A row that reaches the sunset with a webhook still owed is settled, not stranded.** The window has ended, and the obligation with it.

## The Breaking Change

**The webhook HTTP contract is intentionally not preserved.** This is a deliberate breaking change, decided rather than overlooked, and it is the one part of this migration that cannot be absorbed transparently.

The payload survives. The transport does not.

### What is removed, and by which of the two events

Two different mechanisms are at work, and conflating them is how an operator ends up believing the
codebase changed itself. **Automatic** entries happen at the instant, by configuration alone.
**Manual** entries are a later release someone has to ship.

| Removed | When | Detail |
|---|---|---|
| Webhook subscription registration | **Automatic** | The deprecated `/subscribers/{subscriber_id}/webhook-subscription` routes answer `410 Gone` |
| Recording a legacy URL | **Automatic** | `POST` and `PUT /subscribers/{subscriber_id}/webhook-subscription` are the only write paths for it, and both answer `410 Gone`. Clearing one is `DELETE` on the same route, so it stops at the instant too |
| Reporting a legacy URL | **Automatic** | `GET /subscribers/{subscriber_id}/webhook-subscription` is the only read path, and it answers `410 Gone` |
| HTTP delivery | **Automatic** | No delivery is enqueued or attempted; a delivery still owed when the instant passes is dropped rather than left pending |
| `webhooks.go` and its functions | **Manual** | `processHTTP`, `SendWebhook` and `ProcessWebhook` are deleted in a later release — the one enumerated in [The terminal release](#the-terminal-release-the-deletion-checklist) below. The date does not do it. Its first step is **already complete**: the two payload-contract symbols have been relocated out, so what remains is a pure deletion |
| The delivery handler registration | **Manual** | The `WebhookQueue → ProcessWebhook` mapping is unregistered in that same release — **the mapping only**, never the queue, see [What Never Existed](#what-never-existed) |
| The `webhook_url` column | **Manual, not yet scheduled** | Recorded URLs can be purged (nulled) once subscribers have migrated. **No migration drops the column.** The statements to drop it and `migrated_at` sit **commented out** in `sql/1781248900.sql` as a documented future step, to be run by hand only after the sunset has passed and every subscriber shows migrated |

**Subscribers must migrate to consuming Kafka directly. There is no compatibility shim, and none is
planned.** No proxy re-emits Kafka events as HTTP pushes, and after the sunset no route accepts a new
webhook URL: the four deprecated routes answer `410 GEN_GONE`, and they are the only routes that ever
accepted or reported one.

**The generic `/subscribers` routes have no `webhook_url` field at all**, and that is what makes the
sunset enforceable rather than advisory. `CreateSubscriber` and `UpdateSubscriber` deliberately do
not declare it, so legacy webhook state can only be written through the guarded routes — a caller
cannot reach for the unguarded registry route and keep writing it after the guarded ones have begun
answering `410`. Sending `webhook_url` to `POST` or `PUT /subscribers` is refused as a field the
shape does not declare, with `400 GEN_VALIDATION_ERROR` naming it, and that is true **before and
after** the instant alike: it is not a sunset behaviour and it is not `GEN_GONE`. Register the
subscriber on `/subscribers`, then record its URL on
`/subscribers/{subscriber_id}/webhook-subscription`.

`SubscriberResponse` likewise never projects `webhook_url`, at any time — the recorded URL is read
only through the guarded `GET`, so the two reads cannot disagree about whether the legacy surface
still exists. `migrated_at` does stay on the subscriber body, before and after the sunset, because it
is migration *progress* rather than legacy webhook state and discloses no endpoint.

### The terminal release: the deletion checklist

The rows marked **Manual** above are not a promise to tidy up eventually. They are one release, and
this is its checklist. It is here rather than only in the source because it is the artifact the
deferral is answerable to: while the transport is still compiled in, every row below names something
that exists, and `TestWebhookTerminalRelease_ChecklistMatchesTheSurface` in `event_sunset_test.go`
asserts exactly that. So the release cannot be performed without editing this table, and this table
cannot be left describing a release that already happened.

**Trigger.** Do none of it until the deployed window has fully closed — `WebhookSunsetPassed` true,
`WebhookDualDeliveryActive` false, and 30 days elapsed since the window opened. Before then the
delivery code must stay compiled and reachable: the dual-delivery payload-equivalence check needs a
live second transport to compare against, and the `410` is a runtime decision, not a consequence of
deleted source. This ordering is the schedule the project's plan fixes, which places these deletions
as the feature's terminal step rather than as part of the release that introduced Kafka.

**Delete, in this order.** Order matters only for the first row, which is already discharged.

| Artifact | Where it lives today | Terminal action |
|---|---|---|
| `NewWebhook`, `getEventFromStatus` | `event_outbox.go`, `event_topics.go` | **ALREADY RELOCATED — do nothing.** They are the payload object and the event-string vocabulary, so they outlive the transport. Verify they are not in `webhooks.go` before deleting it |
| `processHTTP`, `processHTTPRaw`, `SendWebhook`, `EnqueueLegacyWebhookDelivery`, `ProcessWebhook`, `legacyWebhookTaskID`, `LegacyWebhookRetention`, `LegacyWebhookEventIDHeader` | `webhooks.go` | **DELETE the whole file.** Nothing else in it outlives the transport; confirm that with a build rather than by reading |
| The transport's own tests | `webhooks_test.go`, `webhooks_process_test.go`, `webhooks_destination_test.go`, `webhooks_logging_test.go` | **DELETE.** Their subject is the HTTP transport; the payload and vocabulary behaviours they also touch are covered where those two symbols now live |
| `mux.HandleFunc(cfg.Queue.WebhookQueue, b.blnk.ProcessWebhook)` | `cmd/workers.go` | **UNREGISTER THIS ONE LINE**, and nothing else on that mux. See the preserve table below — this is the row most likely to be over-applied |
| The dual-delivery branch and its state: `eventRelayLegacyTransport`, `dualDeliveryActive`, `MarkEventWebhookPending`, the `webhook_pending` status, `kafka_dispatched_at`, `webhook_attempts` | `event_relay.go` | **DELETE.** All of it exists to keep a failed legacy enqueue recoverable *during* the window. Deleting `webhooks.go` breaks the build at the enqueue call site, which is the intended reminder that the two go together |
| The delivery use of the webhook configuration block — `WebhookConfig`, reached as the `Webhook` member of the notification config | `config/config.go`, `webhooks.go` | **STOP READING IT for delivery.** The struct may stay so existing `blnk.json` files keep loading; nothing may deliver from it |
| The dual-delivery test | `event_dual_delivery_test.go` | **DELETE**, with the branch it tests — not with `webhooks.go`, since its subject is the branch |

**Preserve. Every row here is shared infrastructure that predates or outlives this transport, and
deleting any of it produces no compile error.**

| Artifact | Where it lives | Why it must survive |
|---|---|---|
| `cfg.Queue.WebhookQueue`, `initializeWebhookQueues`, `initializeWebhookWorkerServer` | `cmd/workers.go`, `config/config.go` | The queue is shared. `initializeWebhookQueues` returns the webhook queue **and** the index queue, and `internal/hooks/manager.go` enqueues `PRE_TRANSACTION`/`POST_TRANSACTION` work onto the webhook queue **by name**. Removing it silently disables transaction hooks and search indexing |
| `new:hook_execution`, `cfg.Queue.IndexQueue`, `new:index:batch` handlers | `cmd/workers.go` | Three of the four handlers on that mux belong to other features. Only the `ProcessWebhook` mapping is this transport's |
| `DeprecatedWebhookSubscriptionRoute` and its four registrations — `RegisterWebhookSubscription`, `GetWebhookSubscription`, `UpdateWebhookSubscription`, `DeleteWebhookSubscription` | `api/api.go`, `api/subscribers.go` | **The `410 Gone` is required *on every request* after the sunset, and a deleted route answers `404`.** The routes are what there is to answer with; the guard is what answers. Deleting them would replace the required refusal with "no such endpoint" and would leave a deployment still inside its own window with no management surface at all |
| `WebhookSunsetGuard`, `WebhookSunsetPreAuthGuard`, `IsDeprecatedWebhookSubscriptionPath` | `api/middleware/sunset.go` | The runtime retirement itself. It is a configuration decision that every deployment reaches on its own date |
| `ErrGenGone` and its `statusByCode` entry | `internal/apierror/codes.go` | Without the mapping the guard would emit `500`; an unmapped code defaults to it |
| `WebhookSunsetPassed`, `WebhookSunsetSnapshotAt`, `WebhookDeprecationWindow` | `event_sunset.go` | The single sunset decision point, still consulted by the guards |
| Every queueing dependency — `hibiken/asynq`, `hibiken/asynqmon`, `redis/go-redis` | `go.mod` | **Nothing leaves `go.mod`.** Deleting `ProcessWebhook` removes a handler registration, not a module: all three remain required by the transaction queue, the hooks subsystem and the index queue |

> This is the one place the checklist departs from a narrower reading of "delete the webhook REST
> API". The four routes and the guard are preserved *because* the requirement is that the API answer
> `410 Gone` on every request after the sunset, and only a registered route can. What is deleted is
> the delivery mechanism.

**Verify, after performing it.** `go build ./...` with zero errors is the check that nothing in
`webhooks.go` was still needed. Then: the four deprecated routes still answer `410 GEN_GONE` on every
method, transaction hooks still execute, and search indexing still runs — the three things the
preserve table exists to protect. `sql/1781248900.sql`'s commented-out column drops stay commented
out; they are a separate, hand-run step.

### The `410 Gone` is a typed error code, not a bare status

Worth stating for anyone maintaining this: the sunset guard does not write a status. It aborts with the typed error code `GEN_GONE`, and the error catalog maps that code to HTTP `410`. The mapping is explicit, which is what makes the code and the status impossible to get out of step — the catalog is the single source of truth for every error code's default status, and an unmapped code would silently become a `500`.

So a post-sunset response body carries the same shape as every other error in this API: the flat `error` string alongside the structured `error_detail` object naming `GEN_GONE`. Branch on the code, not on prose.

While an instant is configured, the guarded routes also advertise it on **every** response, on both
sides of the boundary. The two headers come from **two different specifications with two different
syntaxes**, which is the detail worth getting right:

```text
Sunset: Wed, 30 Sep 2026 00:00:00 GMT
Deprecation: @1788134400
```

- **`Sunset`** is defined by **RFC 8594** and carries an **HTTP-date**: the instant the resource
  becomes unavailable. Above, that is the configured sunset.
- **`Deprecation`** is defined by **RFC 9745** and carries a **Structured Field Date** (RFC 9651
  §3.3.7): an `@` followed by an integer number of seconds since the Unix epoch. It is the instant the
  resource *became deprecated*, which for Blnk is the **window start** — the sunset minus 30 days.
  `Deprecation: true` is **not** valid RFC 9745 syntax; a boolean was the pre-standard convention and
  no longer conforms.

RFC 9745 also requires that a `Sunset` date not precede the `Deprecation` date. Blnk satisfies that by
construction, since the deprecation instant is derived by subtracting the window from the sunset.

> The values above are illustrative but internally consistent: `@1788134400` is Mon, 31 Aug 2026
> 00:00:00 GMT, exactly 30 days before the `Sunset` date beside it. Substitute your deployment's own
> configured instant, and derive the epoch the same way rather than copying this one.

Both headers are informational — the refusal is never a function of either — but they mean a client
still inside the window is warned by the very responses it is succeeding with, so an integration can
schedule its own migration without being told out of band.

### If your receiver verifies signatures, read this

This is the part most likely to be missed, because what replaces the check is not a header.

Today, when a server secret is configured, each delivery is signed: `X-Blnk-Timestamp` carries the unix second, and `X-Blnk-Signature` carries the hex-encoded HMAC-SHA256 of `timestamp + "." + body` under that secret. The timestamp is inside the signed data, so it cannot be tampered with independently, and Blnk warns when it is delivering unsigned because no secret is set.

After migration **there are no HTTP headers to verify, and no signature to check.** Authenticity comes from the connection instead:

- **Authentication** — your consumer proves who it is with SASL/SCRAM-SHA-512 against the broker, using the credential issued to your principal.
- **Authorization** — ACLs restrict that principal to your granted topics and your consumer-group namespace, enforced by the broker.

The trust model moves from *"this request body was signed by someone holding the shared secret"* to
*"this record came from a broker I authenticated to."* Those are **different** properties, not a
straight upgrade, and the second one is **conditional**. It holds only if all of the following are
true:

- **You connect over `SASL_SSL` and validate the broker certificate.** Without TLS server
  verification, nothing authenticates the broker to you and the channel guarantee is void — you have
  authenticated yourself to an unverified peer.
- **Only Blnk can write to the topic.** Channel trust says the record came from the broker; it says
  nothing about who produced it. It substitutes for per-message authenticity only while producer ACLs
  on these topics are exclusive to Blnk. A second principal with write access breaks it silently, and
  nothing in the message would reveal that.
- **You trust the broker and its administrators.** An HMAC is verifiable end-to-end without trusting
  any intermediary: a broker operator who could alter a stored record could not forge its signature.
  Channel trust has no such property, so the broker and whoever administers its ACLs become part of
  your trusted base.

What is genuinely better: authentication is established once per session rather than per message, key
distribution is per subscriber rather than one shared secret, and authorization is enforced by the
broker rather than by your code. What is genuinely weaker: there is no per-message, independently
verifiable proof of origin that survives outside the connection.

Either way it is a check in a different place, so do not port signature-verification code across.
Delete it, configure SASL **and TLS**, and if you need per-message non-repudiation retained, raise it
as a requirement rather than assuming the channel supplies it.

- The HMAC authenticated an **individual message**, end to end, independently of how it reached you. It survives any number of intermediaries, and a proxy that altered a byte was detectable by the receiver.
- SASL, TLS and ACLs authenticate a **channel and its writers**. Once you are on it, you trust that everything the broker hands you was written by a principal the ACLs permit — so the broker itself becomes part of your trusted computing base in a way it was not before.

Getting the equivalent assurance therefore takes all four, not just the credential: **TLS** so the connection is authenticated and confidential rather than merely credentialed, **SASL** so your consumer is who it claims to be, **ACLs** so only Blnk can produce to your topics, and **operational integrity of the brokers** themselves, because a compromised broker can now insert a record no per-message signature would reject. In exchange you get replay, ordering, backpressure and offset control the HTTP push never offered. It is a trade with a different shape, not an upgrade along one axis.

Do not port signature-verification code across; there are no headers to verify. Delete it, and configure TLS and SASL instead.

> Verify your consumer actually fails when it should. A misconfigured client that silently falls back to an unauthenticated or unencrypted connection would look identical to a working one for as long as the broker allows it — which is why the broker should be configured to refuse, rather than the client trusted to insist.

### Two symbols had to outlive the file that held them — and they already do

This was the riskiest constraint on the stage-2 deletion above, because getting it wrong would break the payload contract long after the sunset had passed uneventfully. `NewWebhook` and `getEventFromStatus` were declared in `webhooks.go` and **have been relocated out of it already**, ahead of that file's deletion, because those two symbols **are** the contract:

| Symbol | Now declared in | Why it outlives the transport |
|---|---|---|
| `NewWebhook` | `event_outbox.go` | The two-key payload object. It defines the bytes every subscriber parses, on either transport, and is carried verbatim as the `payload` member of every `LedgerEvent`. Twelve surviving non-test files depend on it. |
| `getEventFromStatus` | `event_topics.go` | Maps a transaction's status to its event name, and so defines the `transaction.*` event vocabulary that decides which topic a transaction event is published to. Three surviving non-test files call it. |

Both were file moves inside package `blnk`: no import changed and no call site was edited. Doing it ahead of the deletion release rather than as that release's first act means **stage 2 is now a pure deletion** — nobody has to rescue two symbols from a 1,300-line file while removing it. Nothing else in `webhooks.go` outlives it; the checklist at the foot of that file says so and says to confirm it with a build rather than by reading.

That is why the event vocabulary outlives the mechanism that introduced it, and why nothing in [event-streaming.md](event-streaming.md#event-types)'s event catalogue changes at the sunset.

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

A small, **explicitly deprecated** management surface exposes them: `POST`, `GET`, `PUT` and `DELETE` on `/subscribers/{subscriber_id}/webhook-subscription`. `DELETE` is the cutover *record*, and it writes **both facts in one statement**: the recorded URL is forgotten and the migration instant is stamped together, so a subscriber is always on exactly one side of the migration report.

That single write matters for the report rather than for tidiness. Done as a clear followed by a stamp, a failure in between left the row with **neither** column set — no URL, so nothing still to migrate from, and no instant, so not counted as migrated. Such a row is invisible to both halves of the progress query, and nothing surfaces it: the endpoint no longer holds the URL that would identify it as owing a migration. One write removes that state rather than choosing which side of it to fail on.

**Clearing a URL is a different operation.** `migrated_at` is an audit fact, so stamping it for a subscriber that has not moved records a false one. To correct a mis-recorded endpoint, `PUT` the replacement; the cutover is for a subscriber that has actually finished moving.

#### The webhook-subscription routes track migration only

**These routes do not configure, start or stop HTTP delivery.** They read and write two columns on a
registry row, and nothing more. Say it plainly because the names invite the opposite assumption:

- `POST` / `PUT` **record** a URL for tracking. They do not cause Blnk to deliver anywhere. Blnk has
  never had per-subscriber delivery.
- `DELETE` **forgets** the recorded URL and stamps `migrated_at`. **It does not stop delivery.**
  Deliveries continue to the global destination exactly as before.
- `GET` reports what was recorded. It is not a view of where Blnk actually delivers.

Delivery is controlled by the deployment-wide `BLNK_WEBHOOK_URL` and by whatever fan-out sits behind
it. **Actually ending HTTP traffic to one consumer therefore requires a change outside this API** —
remove that consumer from your fan-out — and ending it for everyone requires either unsetting
`BLNK_WEBHOOK_URL` or letting the sunset pass. Calling `DELETE` and expecting the pushes to stop is
the single most likely mistake in this migration: the call will succeed, the tracking will look
complete, and the deliveries will carry on.

That surface is also what makes the sunset behaviour observable at all. Without a subscription route there would be no request on which a `410` could ever be seen, and "the webhook REST API returns `410 Gone`" would be an untestable claim.

The two columns have different retention rules, deliberately. `webhook_url` is a third party's endpoint and an operational detail of somebody else's system, so it is purged once a subscriber has migrated — nulled, not deleted, since the subscriber is still a live subscriber and only the migration artefact expires. `migrated_at` is never purged: it is an audit fact about your own deployment, and it is what progress reporting counts.

Clearing an address on its own is likewise **not** a migration. An erasure request or the retention purge forgets a URL and leaves `migrated_at` exactly as it stands, because deleting an address is no evidence that a subscriber moved to Kafka — conflating the two would have progress reports counting migrations that never happened.

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
