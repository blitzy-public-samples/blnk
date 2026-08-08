# Blnk Kafka Operations Runbook

This is the operator's runbook for Blnk's Kafka event pipeline: how to provision the broker, what the subscriber access model actually enforces, how to triage and replay a dead-lettered event, how to run the daily zero-loss reconciliation, and what to do when one of the alerts fires. It is written as steps to execute rather than as an overview — the subscriber-facing contract (the topic catalogue, the `LedgerEvent` envelope, the event vocabulary and the ordering guarantee) is owned by [event-streaming.md](event-streaming.md) and is referenced here, never restated.

## How to Use This Document

Every rule in `alerts/blnk-kafka-alerts.yml` names this file as its `runbook_url`. If you arrived from a notification, go straight to your alert:

| Alert | Response section |
|-------|-----------------|
| `DeadLetterMessageStuck` | [DeadLetterMessageStuck](#deadlettermessagestuck) |
| `SubscriberConsumerLagHigh` | [SubscriberConsumerLagHigh](#subscriberconsumerlaghigh) |
| `SubscriberRevocationOutstanding` | [SubscriberRevocationOutstanding](#subscriberrevocationoutstanding) |
| `ConsumerLagMeasurementDegraded` | [ConsumerLagMeasurementDegraded](#consumerlagmeasurementdegraded) |

If you arrived for routine work, the four procedures are [Provisioning](#provisioning), [The ACL Model](#the-acl-model), [Dead-Letter Triage and Replay](#dead-letter-triage-and-replay) and [The Daily Outbox-versus-Offset Reconciliation](#the-daily-outbox-versus-offset-reconciliation).

> **Two tables, two relays, and they are not the same thing.** `blnk.event_outbox` is the event pipeline's outbox and is served by the event relay. `blnk.lineage_outbox` is the fund-lineage feature's outbox and is served by its own processor; its behaviour is unchanged by anything in this document. They are separate tables with separate relays and are never merged. An investigation aimed at the wrong one will find a healthy table and conclude, wrongly, that nothing is stuck.

## Prerequisites

- **The master key.** Every event and subscriber endpoint in this runbook gates on it as its first act and answers `403` with `error_detail.code` of `AUTH_MASTER_KEY_REQUIRED` to anything else. A scoped API key cannot reach them. Pass it as `X-Blnk-Key`. The examples below assume `BLNK_MASTER_KEY` holds it and `BLNK_API` holds the API base URL, default `http://localhost:5001`.
- **Metrics.** `enable_observability` must be true for any gauge or counter named here to exist. See [metrics.md](metrics.md) for the catalogue, the attribute domains and example queries; this document does not duplicate them.
- **Broker access**, for the CLI steps only. The API-driven steps — listing, replaying and reconciling — need no broker access at all.

### Where to run the Kafka CLI

Blnk itself never shells out to the Kafka CLI: `event_admin.go` performs topic assurance, SCRAM provisioning, ACL grants and offset reads in process through `kafka-go`. The CLI is for you, and the two bootstrap scripts.

On the local Compose stack the broker container already holds an authenticated admin client configuration, written by its own entrypoint, so every command below is a one-liner:

```bash
# List the topic catalogue from inside the broker container.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --list
```

Three addressing facts matter and are easy to get wrong:

- **`kafka:9092`** is the in-network listener. It is what the server, the worker and the `kafka-init` one-shot dial, and it is what you use from inside a container on the Compose network.
- **`localhost:9092`** is the host listener. It is published from container port `29092` and bound to `127.0.0.1` — one listener cannot advertise two addresses, so there are two. Use it for host-run tests, a host-run `blnk start` and the k6 scenario.
- **`--command-config`** is not optional. Both client listeners require SASL, so a command without a client configuration fails the handshake and reports something that reads like a network fault.

In production, point `--bootstrap-server` at your brokers and `--command-config` at your own properties file. Do not reuse the local `SASL_PLAINTEXT` file: on that listener the SCRAM exchange and every ledger event travel in clear text, which is acceptable on a loopback-bound single-broker development stack and nowhere else.

## Provisioning

### What gets created

Five category topics and their five dead-letter siblings — **ten topics, and they are the complete inventory**. Blnk writes to no other topic.

| Category topic | Dead-letter topic | Grantable to a subscriber |
|---------------|-------------------|---------------------------|
| `blnk.transactions` | `blnk.transactions.dlt` | Yes |
| `blnk.balances` | `blnk.balances.dlt` | Yes |
| `blnk.identities` | `blnk.identities.dlt` | Yes |
| `blnk.ledgers` | `blnk.ledgers.dlt` | Yes |
| `blnk.system` | `blnk.system.dlt` | **No — internal** |

Every name is composed as `<prefix>.<category>` and `<prefix>.<category>.dlt`, where the prefix is `KAFKA_TOPIC_PREFIX` and defaults to `blnk`. Set `KAFKA_TOPIC_PREFIX=acme` and the whole inventory moves to `acme.transactions` and so on; the category tokens never change. What each topic carries, and why there are five categories rather than the three the requirement names, is in [event-streaming.md](event-streaming.md#topic-catalogue).

No dead-letter topic is ever granted to a subscriber, and neither is `blnk.system`. That leaves exactly four grantable names.

> Do not "tidy" the inventory to eight or twelve names. `model.EventCategory` routes events into exactly these five categories and `event_topics.go` composes exactly these ten names from them. A name provisioning does not create is a name the relay cannot publish to; a name it creates that no code writes to is dead weight in every environment.

### Partitions

`KAFKA_MIN_PARTITIONS`, **default 6**. It is a floor, not a target:

- A topic with **fewer** partitions is grown to the floor with `CreatePartitions` — but only while it is **empty**. See the warning below. `EnsureTopics` in `event_admin.go` is the Go path; `scripts/kafka-provision.sh` applies the identical rule with `kafka-topics --alter`, so the two are interchangeable.
- A topic with **more** partitions is left alone and reported. Kafka cannot reduce a partition count, and shrinking would be wrong even if it could: the partition a key lands on is `murmur2(key) mod partitionCount`, so a lower count would start routing one aggregate's events to a different partition from their predecessors.

> **Growing a non-empty topic is refused, and that refusal is correct.** Raising the count re-maps keys: a ledger that hashed into partition 2 of six lands somewhere else out of twelve, its history splits across two partitions with no ordering between them, and the per-aggregate ordering guarantee is broken permanently for every key already written — the records cannot be moved back. `EnsureTopics` therefore reports `ErrPartitionGrowthRefused` rather than growing a topic that holds messages, and `scripts/kafka-provision.sh` behaves the same way. Reaching a higher partition count on a live topic is a planned migration: provision a correctly shaped topic, move consumers, drain the old one. `KAFKA_ALLOW_PARTITION_GROWTH=true` exists for the operator who has planned that migration and wants the provisioning pass to perform the growth step. Set the count once, up front, and leave it alone.

Both the Go assurance pass and the provisioning script are **idempotent and safe to re-run**, including concurrently with themselves. An existing topic is not an error, and a topic that appears between the metadata probe and the create — the `TOPIC_ALREADY_EXISTS` race two server instances starting together will hit — is re-probed and treated as pre-existing.

### The replication factor

`KAFKA_REPLICATION_FACTOR`, **default 3**. Production runs at 3. The single-broker local stack runs at **1**, and the Compose `kafka-init` service sets it to 1 explicitly.

**It is configuration rather than a constant because it has to be.** A one-broker KRaft cluster cannot satisfy a factor of 3 — the broker rejects the creation outright with `INVALID_REPLICATION_FACTOR` — so a hard-coded 3 would make local bring-up impossible, while a hard-coded 1 would quietly ship a production cluster with no replicas. An unconfigured factor is refused with an actionable message rather than guessed at.

The factor of an **existing** topic is verified, not assumed: the minimum replica count observed across a topic's partitions is compared against the configured factor, and a shortfall returns `ErrReplicationFactorInadequate`. The minimum is used because durability is decided by the weakest partition. `scripts/kafka-provision.sh` applies the same rule and **fails the run** with reassignment guidance rather than recording an under-replicated topic as assured.

### Step 1 — Bootstrap the SCRAM admin credential, before the broker's first start

Run `scripts/kafka-bootstrap.sh`. It executes:

```text
kafka-storage format --add-scram 'SCRAM-SHA-512=[name=…,password=…,iterations=4096]' --ignore-formatted
```

**This is mandatory, not optional, and it cannot be done afterwards.** In KRaft mode SCRAM credentials live in the `__cluster_metadata` log rather than in ZooKeeper, and a broker cannot authenticate a SASL client until at least one credential is already there. The usual runtime command —

```bash
kafka-configs.sh --alter --add-config 'SCRAM-SHA-512=[password=…]' \
  --entity-type users --entity-name <user>
```

— needs an authenticated connection, so **it cannot create the first credential**. That is a genuine chicken-and-egg problem, and injecting the credential while the storage is being formatted is the only resolution. Your instinct will be to fix an unauthenticable broker by running `kafka-configs` against it; that will not work, and the time spent discovering so is the reason this paragraph exists.

The ordering is therefore fixed: **bootstrap, then broker, then provisioning.** Per-subscriber principals are not created here — they are added once the broker is up and this credential can authenticate, by `scripts/kafka-provision.sh` locally and by `event_admin.go`'s `AlterUserScramCredentials` in production.

#### The version floor is hard: Kafka 3.5 / Confluent Platform 7.5.0

The `--add-scram` flag of `kafka-storage format` was added in Kafka 3.5 (Confluent Platform 7.5.0). **Earlier releases simply do not have it.** An older image rejects the flag, the format either fails or completes with no credential in the metadata log, and the broker then starts but can authenticate nobody — which surfaces much later as what looks like a wrong password. This is a floor on the broker image tag, not a preference. The script asserts it by asking the CLI whether `format` accepts the flag, which is the last point at which a `KAFKA_IMAGE` override below the floor can still be diagnosed as itself.

The Compose stack pins `${KAFKA_IMAGE:-apache/kafka:3.9.2}`, comfortably above the floor. If you override it, stay above 3.5.

#### The script's inputs

| Variable | Required | Meaning |
|---------|----------|---------|
| `KAFKA_SASL_ADMIN_USER` | **Yes** | The administrative principal seeded into the metadata log. No default. |
| `KAFKA_SASL_ADMIN_SECRET` | **Yes** | That principal's password. No default, and it never will have one. |
| `KAFKA_SCRAM_ITERATIONS` | No | PBKDF2 iterations. Default and minimum `4096`. |
| `KAFKA_CLUSTER_ID` | No | Generated with `kafka-storage random-uuid` when unset. Not a secret; pin it to make a rebuild reproducible. |
| `KAFKA_KRAFT_CONFIG` | No | The `server.properties` to format against. Auto-detected from well-known image layouts when unset. |
| `KAFKA_LOG_DIRS` | No | Comma-separated log directories to probe for an existing format. Falls back to `log.dirs` from the resolved configuration, then `/var/lib/kafka/data`. |

The user and the secret are **one pair with one meaning**: both set means SASL/SCRAM as that principal, both empty means no SASL at all, and exactly one set is a misconfiguration refused by name. Because seeding a credential is this script's entire job, an empty pair is refused here. Neither value is defaulted — the username used to fall back to the literal `admin`, which made a single `.env` mean "authenticate as admin" to the scripts and "no SASL configured" to the Go clients.

`stack.sh --init` writes both into a mode-0600 `.env`, substituting the `{KAFKA_SASL_ADMIN_SECRET}` placeholder, which is the intended local route.

> **Both values must be drawn from the safe credential alphabet, and a value outside it is refused rather than escaped.** `--add-scram` takes a sentence in Kafka's own mini-grammar, `SCRAM-SHA-512=[name=<user>,password=<secret>,iterations=<n>]`, which Kafka parses by splitting on `,` and `=` inside the brackets. **That grammar has no escape sequence at all.** A password containing a comma or a bracket cannot be expressed in it: `a,b` parses as the end of the password followed by an unrecognised key, so a credential is seeded that is not the one you supplied and that nobody can authenticate with. Shell quoting does not help — quoting delivers the bytes intact and it is Kafka that then misreads them.

The script is **idempotent** and safe on every bring-up. `docker compose down && docker compose up` reuses the `kafka_data` volume, so from the second start onwards the storage is already formatted; that is detected via `meta.properties` in every configured log directory and skipped rather than treated as an error, and Kafka's own `--ignore-formatted` is passed as a complementary second mechanism.

It also accepts an optional trailing command to `exec` after a successful bootstrap, which is how a container entrypoint declares the sequence once:

```bash
scripts/kafka-bootstrap.sh                 # format, report, exit 0 — caller starts the broker
scripts/kafka-bootstrap.sh kafka-server-start.sh /etc/kafka/server.properties
```

### Step 2 — Provision the topics and principals

Run `scripts/kafka-provision.sh` against a **running** broker. It creates, in this order:

1. Every category topic and its dead-letter sibling — the ten names above, derived from `KAFKA_TOPIC_PREFIX`.
2. The **producer** principal (`KAFKA_SASL_USER`, falling back to `KAFKA_PRODUCER_USER`, default `blnk-producer`) with `Write` and `Describe` on the Blnk-owned topics and nothing else.
3. One **sample subscriber** principal (`KAFKA_SAMPLE_SUBSCRIBER_USER`, default `blnk-sample-subscriber`) with `Read` and `Describe` on the grantable topics and `Read` on its own prefixed consumer-group namespace.

The producer principal is load-bearing rather than a nicety: the configuration **refuses to publish as the administrator**, so a deployment with an administrative pair and no producer pair fails to construct its event publisher and neither the server nor the worker starts. The escape hatch is `KAFKA_ALLOW_ADMIN_PRODUCER=true`, which warns on every publisher construction and exists only for a deployment mid-upgrade.

It requires Step 1 to have already happened: it authenticates with the administrative credential, which can only have been created in the metadata log. Getting the order wrong does not produce a clear error of its own — it produces an authentication failure that reads like a wrong password, which is why the readiness wait names both causes when it times out.

**It is idempotent and exits 0 when nothing needs changing.** That is a hard requirement, not a nicety: the Compose `kafka-init` service is a one-shot with `restart: on-failure:3`, and the server and worker gate on it *completing*, so a non-zero exit on an already-provisioned broker would restart it until the cap and then fail the whole bring-up. Topic creation passes `--if-not-exists`, adding an existing ACL binding is a no-op, and **an existing SCRAM credential is left alone** — rotation is an explicit request through `KAFKA_ROTATE_PRODUCER_SECRET` or `KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET`, each needing a destination file. Rotating the producer secret out from under a running server and worker stops them authenticating, so a rotation with nowhere to deliver the new value refuses outright.

**A declared-but-empty `KAFKA_BROKERS` means "Kafka is not configured here" and provisioning skips entirely, exiting 0.** That is what makes the script safe to wire into an unconditional bring-up path. `KAFKA_BOOTSTRAP_SERVER` overrides the skip, which is how the `kafka-init` service provisions a broker for a deployment that has not yet turned publishing on.

> **No credential is ever printed.** A generated password is written to a mode-0600 file you nominate through `KAFKA_SASL_SECRET_FILE`, `KAFKA_PRODUCER_SECRET_FILE` or `KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE`, and only the **path** is reported; a supplied secret is not echoed. With neither a supplied secret nor a destination file the principal is skipped and the reason is printed. This is not fastidiousness: the script's usual home is the `kafka-init` service, whose stdout *is* a container log — written to disk, handed to anyone who can run `docker compose logs`, and forwarded to whatever collects the host's logs. "Shown once" is not a property a log line can have.

Geometry is **verified, not assumed**: every topic's partition count and replication factor are read back from the broker, and a geometry that cannot be read after three attempts fails the run rather than being recorded as unknown. The closing summary prints only observed values.

### How to run provisioning

Three routes, all reaching the same script:

```bash
# 1. The Compose one-shot. Runs automatically on bring-up, gated on the broker's
#    healthcheck. The kafka and kafka-init services are BOTH behind the "kafka" profile.
docker compose --profile kafka up -d
docker compose --profile kafka logs kafka-init

# 2. The makefile target, on the host. Sources .env, then lets the command line win over
#    it, then falls back to the documented local geometry (prefix blnk, 6 partitions,
#    replication factor 1).
make kafka_provision

# 3. stack.sh, which provisions Kafka and then brings the stack up.
./stack.sh --up
```

`make kafka_provision` and `stack.sh` run on the host, where a Kafka distribution is very likely absent. The script handles that by re-executing itself **inside the broker container** over `docker`, rather than shipping individual commands across, which keeps any temporary credential file on the side that has to read it. If neither route exists, the failure names all three remedies instead of surfacing as `command not found`.

Note that "the CLI is on `PATH`" is a different question from "the CLI is installed": Apache Kafka images install the tools in `/opt/kafka/bin` and do not add that directory to `PATH`, so the script probes `PATH` first and the well-known installation directories afterwards.

The broker is **opt-in**. Both Kafka services sit behind the `kafka` Compose profile, because Blnk's documented steady state is an empty `KAFKA_BROKERS` and a no-op publisher — see [Running Without Kafka](#running-without-kafka). Opting in means opting in to both halves:

```text
docker compose up                          -> no Kafka at all
docker compose --profile kafka up          -> broker + provisioning
COMPOSE_PROFILES=kafka docker compose up    -> the same, from .env
```

Selecting the profile without setting `KAFKA_BROKERS` gives a provisioned broker that Blnk ignores. Setting `KAFKA_BROKERS` without the profile gives a relay retrying against nothing. `stack.sh` enables the profile only when `KAFKA_BROKERS` is set, and always includes it on teardown so nothing is left behind.

Every one of these variables — and both bare `KAFKA_*` / `RELAY_*` names and their `BLNK_`-prefixed forms are accepted — is documented at its point of use in `.env.example`. The provisioning script publishes its own interface, which is how `stack.sh` builds its passthrough list:

```bash
scripts/kafka-provision.sh --print-interface-host   # one variable name per line
```

### Adding a runtime SCRAM user

Once the broker is up and the bootstrap credential can authenticate, further principals are ordinary runtime operations:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=<new-password>]' \
  --entity-type users --entity-name <principal>
```

**Distinguish this clearly from the bootstrap credential, which cannot be created this way** — see Step 1. This command needs an authenticated connection, so it works only *because* a bootstrap credential already exists.

Two cautions. The value must come from the safe credential alphabet, because `--add-config` shares the no-escape-sequence grammar described above, so a comma or a bracket is silently truncated. And a password typed on a command line lands in shell history and in the process table of whatever host runs it — prefer a `--command-config`-style properties file or the API path below, and clear your history afterwards if you do type one.

For a **subscriber**, do not run it by hand. Use `POST /subscribers/:subscriber_id/kafka-credentials`, which mints the credential, binds the ACLs, records the issuance and compensates a partial failure. Doing it by hand produces a principal with a credential and no bindings, which authenticates and can read nothing, and leaves no registry row for the reconciliation or the lag metrics to attribute.

### Verifying provisioning

```bash
# The ten topics, with their partition counts and replication factors.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --describe

# The sample subscriber principal exists and holds a SCRAM-SHA-512 credential.
# Prints only the mechanism and iteration count, e.g.
#   SCRAM credential configs for user-principal 'blnk-sample-subscriber' are SCRAM-SHA-512=iterations=4096
# Never a password: there is nothing stored that could print one.
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --describe --entity-type users --entity-name blnk-sample-subscriber
```

Expect ten topic names, six partitions each and a replication factor of 1 locally.

## The ACL Model

### The grant, exactly

For each authorised topic, and one binding for the consumer group:

| Resource | Pattern type | Operation | Permission |
|----------|-------------|-----------|------------|
| Topic `<authorised topic>` | `LITERAL` | `Read` | `Allow` |
| Topic `<authorised topic>` | `LITERAL` | `Describe` | `Allow` |
| Group `blnk-sub-<subscriber_id>.` | `PREFIXED` | `Read` | `Allow` |

**Never `Write`. Never a wildcard topic pattern.** A subscriber consumes; it does not produce, and a wildcard would grant every topic the prefix could ever cover, including the internal category and every dead-letter sibling.

The **group binding is `PREFIXED` on purpose**. Granting the group *id* literally would pin the subscriber to exactly one consumer group; granting the *namespace* with a prefixed pattern reserves everything beneath it and nothing beside it, so a subscriber wanting a second group — a replay group beside its live one — picks another leaf with no administrative round trip.

Kafka's own implication rules make `Read` imply `Describe` on the same resource, and the group `Read` binding already implies the group `Describe` that `FindCoordinator` and `OffsetFetch` require. The topic `Describe` binding is therefore technically redundant and is requested anyway, so the grant is auditable from the binding list alone without the reader having to know the implication table. It costs one binding per topic.

Only the **four subscriber-facing categories** may appear in a grant. `blnk.system` is internal and every `<topic>.dlt` is ungrantable, so no dead-letter name can ever appear in a subscriber's topic list.

### SCRAM parameters

- **Mechanism: `SCRAM-SHA-512`**, fixed rather than configurable. Kafka implements only SHA-256 and SHA-512, and keeping the administrative principal and every subscriber on one mechanism means the broker needs exactly one enabled.
- **Iterations: at least `4096`**, which is both Kafka's minimum and the default. A request below the minimum is raised to it with a warning rather than rejected; a request above it is honoured.
- **Pair SCRAM with TLS in production.** SCRAM authenticates; it does not encrypt. `SASL_PLAINTEXT` is acceptable for the loopback-bound local single-broker stack and nowhere else — both Kafka clients in Blnk **refuse to dial without TLS** unless `KAFKA_INSECURE_LOCAL_DEV` is explicitly set, which is what makes plaintext an opt-in rather than the accident of an unset variable. It is logged as a warning on every configuration load so its presence in a real deployment cannot go unnoticed. Configure `KAFKA_TLS_ENABLED` and the rest of the `KAFKA_TLS_*` block instead.

### The authorizer must be StandardAuthorizer

**In KRaft mode, `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` is required, and without it ACLs are accepted but never enforced.**

This is the most dangerous configuration mistake in the whole pipeline because **it fails silently and looks like success**. With no authorizer configured, Kafka permits every request. Every `CreateACLs` call still succeeds. `kafka-acls.sh --list` still shows the grants, exactly as it would on a correctly configured cluster. Nothing logs a warning. And yet nothing is restricted: any authenticated principal can read every topic, including `blnk.system` and every dead-letter sibling, and can join any consumer group.

The consequences are worth stating plainly:

- **The subscriber-isolation guarantee is void.** Every subscriber can read every other subscriber's events, and the internal error stream, whose payloads carry verbatim error text naming database schemas, tables and broker addresses.
- **The isolation test passes while proving nothing.** An automated check that provisions a principal and asserts it *can* read its own topics succeeds either way. Only a negative assertion — that a read outside the grant is refused — distinguishes the two, and only against a broker with the authorizer active.

The Compose broker sets it, alongside `super.users=User:${KAFKA_SASL_ADMIN_USER}`, in the `server.properties` its entrypoint renders. `scripts/kafka-bootstrap.sh` names the same class as its required authorizer.

#### Verify the authorizer is active rather than trusting it

Two checks. Do both — the first is cheap and the second is conclusive.

```bash
# 1. The static configuration the broker actually started with.
#    On the Compose stack the file is rendered by the kafka service's entrypoint.
docker compose exec kafka grep -E '^(authorizer\.class\.name|super\.users)=' \
  /tmp/blnk-kafka/server.properties
```

Expect exactly `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`. An empty result means no authorizer is configured and **no ACL on this cluster is being enforced**.

```bash
# 2. The behavioural proof: a principal reading OUTSIDE its grant must be refused.
#    blnk.system is granted to nobody, so an authorization failure here is the
#    correct and expected outcome. Use a SUBSCRIBER's own client properties file —
#    never the admin one, which is in super.users and is allowed everything by
#    design, so it would prove nothing. The path must be visible INSIDE the
#    container; mount the file or write it there first.
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --consumer.config /path/in/container/sample-subscriber.properties \
  --topic blnk.system --max-messages 1 --timeout-ms 10000
```

A `TopicAuthorizationException` is a **pass**. Expect output of this shape:

```text
WARN  ... reported a recoverable issue ... : {blnk.system=TOPIC_AUTHORIZATION_FAILED}
ERROR ... Topic authorization failed for topics [blnk.system]
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [blnk.system]
```

Records, or an empty topic reported without an authorization error, mean the authorizer is not enforcing and must be fixed before the cluster is trusted with more than one subscriber.

Pair it with the positive control, or a refusal proves only that the credential is broken: the same principal reading a topic it **is** granted, under a group inside its own namespace, must succeed.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --consumer.config /path/in/container/sample-subscriber.properties \
  --topic blnk.transactions --group blnk-sub-<subscriber_id>.default \
  --from-beginning --max-messages 3 --timeout-ms 20000
```

### Reconstructing a subscriber's principal and consumer group by hand

A subscriber's Kafka principal and consumer-group namespace are the two names its access boundary is expressed in — the SCRAM credential is minted for the principal, and every ACL binding names the principal and the namespace. Whoever chooses those strings chooses the boundary, so they are **derived from the subscriber's immutable identifier and never accepted from a request**. Both derivations are pure functions of it, which is exactly what lets you reconstruct them from a subscriber id alone:

```text
principal          = "blnk-sub-" + <subscriber_id>
group namespace    = "blnk-sub-" + <subscriber_id> + "."          <- the PREFIXED ACL resource
default group id   = "blnk-sub-" + <subscriber_id> + ".default"
ACL principal form = "User:" + principal
```

Worked example, for a subscriber whose `subscriber_id` is `sub_9f8d3c214b7a5e6f`:

| Value | Result |
|-------|--------|
| SASL username / Kafka principal | `blnk-sub-sub_9f8d3c214b7a5e6f` |
| ACL principal | `User:blnk-sub-sub_9f8d3c214b7a5e6f` |
| Consumer-group namespace (the `PREFIXED` ACL resource) | `blnk-sub-sub_9f8d3c214b7a5e6f.` |
| Default consumer group id | `blnk-sub-sub_9f8d3c214b7a5e6f.default` |

> **The trailing `.` on the namespace is significant and must not be trimmed.** It is the disjointness guarantee: the delimiter cannot occur inside a canonical identifier, so `blnk-sub-abc.` and `blnk-sub-abcd.` can never overlap. Without it, a subscriber whose id is a leading substring of another's would reserve the other's namespace and could join its consumer groups. A grant written against `blnk-sub-abc` rather than `blnk-sub-abc.` is a real isolation defect, not a cosmetic one.

Because the derivations depend only on the immutable identifier, a **credential rotation leaves the principal, the namespace and every ACL binding intact**. Re-issuing changes the password and nothing else.

The corresponding functions are `SubscriberKafkaPrincipal`, `SubscriberConsumerGroupNamespace` and `SubscriberConsumerGroupID`; the composition itself lives in `model/event.go` because the persistence layer's `CHECK` constraints have to agree with it byte for byte.

### There are no per-tenant topics

Every subscriber reads the **same** five category topics. There is no topic per tenant, per subscriber or per ledger, and looking for one is looking for something that does not exist.

Isolation is delivered by three things and three things only:

1. **The principal** — a distinct SASL/SCRAM identity per subscriber.
2. **The ACL bindings** — literal `Read`/`Describe` on the authorised topics, and nothing beyond them.
3. **The consumer group namespace** — a prefixed `Read` grant that reserves the subscriber's own group space and no one else's.

#### The partition-key prefix is recorded but not enforceable

A subscriber row may carry a `partition_key_prefix`, and it is worth knowing exactly what that means, because it is the one part of the access model Kafka cannot implement.

**Kafka's authorizer has no message-key dimension.** There is no ACL that restricts a consumer to a slice of a topic by key: a subscriber granted a topic can read every record on it. A non-empty `partition_key_prefix` therefore records an authorization *narrower than any credential this system can mint*, so **`POST /subscribers/:subscriber_id/kafka-credentials` refuses such a row** with `409` and `error_detail.code` of `SUBSCRIBER_ISOLATION_UNENFORCEABLE` rather than issuing a credential that quietly grants more than the registry claims.

The remedy is to decide which of the two you meant. Either clear the prefix and accept topic-level access, or split the data across separate deployments. The credentials response carries no partition-key prefix at all, and its `enforced_access` object states outright that key filtering is not enforced.

Note also that the prefix is **recorded rather than derived** — unlike the principal and the group. It could not be derived: a Kafka message key on Blnk's topics is the outbox row's stored partition key, which for the highest-volume event type is a *balance* id rather than a ledger id (see [event-streaming.md](event-streaming.md#the-key-is-not-simply-the-ledger-id)), so a prefix computed from the subscriber's own identifier would match no record ever produced and a subscriber filtering on it would silently discard its entire stream.

### Issuing credentials

```bash
curl -sS -X POST "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f/kafka-credentials" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY"
```

The `200` response carries everything the subscriber needs to start consuming, and nothing else:

| Field | Meaning |
|-------|---------|
| `brokers` | The subscriber-facing bootstrap list, from `KAFKA_SUBSCRIBER_BROKERS`. |
| `broker_endpoint` | The same list as one connection string, for convenience. |
| `authorized_topics` | The topics the credential may `Read` and `Describe`. Never `blnk.system`, never a `.dlt` name. |
| `consumer_group_id` | The derived default group, `blnk-sub-<subscriber_id>.default`. |
| `enforced_access` | What the broker actually enforces. Assembled by the model, never by the handler, and it states outright that key filtering is **not** enforced. |
| `username` | The derived principal, `blnk-sub-<subscriber_id>`. |
| `password` | **The plaintext, returned only on this response and never again.** Hand it to the subscriber and keep no copy Blnk can be asked for. |
| `mechanism` | `SCRAM-SHA-512`. |
| `issued_at` | The issuance instant, which is also what is persisted alongside the non-reversible reference. |

Provisioning completes **within five seconds**: the ceiling is applied by the service and again at the HTTP boundary, so an expiry is *answered* rather than waited out and the request always ends with a code that says whether retrying is sensible.

The endpoint reports `KAFKA_SUBSCRIBER_BROKERS`, which is a **different list** from `KAFKA_BROKERS` and does not fall back to it. `KAFKA_BROKERS` is what Blnk itself dials and is an address inside the deployment; a broker answers every client with the *advertised* address of the listener the connection arrived on, so handing a subscriber an internal address produces an unexplained connection timeout in the subscriber's logs days later, and publishes your internal topology for good measure. When `KAFKA_SUBSCRIBER_BROKERS` is empty, issuance is refused with `SUBSCRIBER_BROKERS_NOT_CONFIGURED` rather than falling back. A deployment whose subscribers really are in-cluster sets it to the same value as `KAFKA_BROKERS`, which is one line and makes the claim explicit.

The refusals worth recognising:

| Status | `error_detail.code` | What to do |
|--------|--------------------|------------|
| `400` | `GEN_MISSING_PARAMETER` | No identifier in the route. |
| `400` | `GEN_VALIDATION_ERROR` | No Kafka identity can be derived from that identifier. Fix the id. |
| `403` | `AUTH_MASTER_KEY_REQUIRED` | Use the master key. |
| `404` | `SUBSCRIBER_NOT_FOUND` | Register the subscriber first with `POST /subscribers`. |
| `409` | `SUBSCRIBER_GRANT_EMPTY` | The subscriber is authorised for no topics. Set `authorized_topics`. |
| `409` | `SUBSCRIBER_ISOLATION_UNENFORCEABLE` | A `partition_key_prefix` Kafka cannot enforce — see above. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | No broker configured, or the broker is down. |
| `503` | `SUBSCRIBER_BROKERS_NOT_CONFIGURED` | Set `KAFKA_SUBSCRIBER_BROKERS`. |
| `503` | `SUBSCRIBER_PROVISIONING_FAILED` | The broker refused the credential or its bindings. Check the admin credential and the authorizer. |
| `504` | `SUBSCRIBER_PROVISIONING_TIMEOUT` | The registry ran out of its five-second budget. Retry. |

### A lost credential is re-issued, never recovered

**There is no procedure in this runbook for looking up a subscriber's password, because none exists.** Only a non-reversible reference and the issuance instant are persisted, and `blnk.event_subscribers` has no column able to hold the secret. No endpoint returns it on a read, and re-calling the issuance endpoint mints a **new** credential rather than returning the old one.

So when a subscriber loses its password, the answer is to re-issue:

```bash
curl -sS -X POST "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f/kafka-credentials" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY"
```

**Re-issuing is destructive to the previous secret.** Kafka stores one SCRAM credential per principal, so the upsert replaces it and any consumer still using the old password fails at its next handshake. Coordinate the change with the subscriber. The subscriber id, the principal, the consumer group and every ACL binding are unchanged, all being derived from the immutable identifier.

### Verifying a subscriber's access

```bash
# Every ACL binding held by one principal. Expect two per authorised topic
# (Read, Describe, LITERAL) and one PREFIXED Read on the group namespace.
docker compose exec kafka /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --list --principal User:blnk-sub-sub_9f8d3c214b7a5e6f

# The principal holds a SCRAM-SHA-512 credential. Reports the mechanism and the
# iteration count only — the password is not stored and cannot be printed.
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --describe --entity-type users --entity-name blnk-sub-sub_9f8d3c214b7a5e6f

# The registry's own view, including the derived principal and group.
curl -sS "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f" -H "X-Blnk-Key: $BLNK_MASTER_KEY"
```

To confirm a credential **authenticates**, have the subscriber connect, or use a client properties file the subscriber already holds. Do not reconstruct one from a stored value — there is nothing stored to reconstruct it from.

### The legacy `webhook_url` on a subscriber row, and the one check that is yours

A subscriber row may carry a `webhook_url`, recorded during the migration window so an operator can tell which subscribers are still to be moved and which URL each is coming off. **Dual-run delivery does not read it** — the legacy leg goes to the single global webhook destination, which is the entire webhook subscription surface Blnk has ever had. Treating this column as a delivery sink would silently deliver nothing.

Registration validates what it can: a non-HTTPS scheme, and any **visibly internal** destination — loopback, link-local including the `169.254.169.254` cloud metadata address, private ranges and unqualified hostnames — is refused outright, because those are exactly what a server-side request forgery aims at.

> **One residual risk is not, and cannot be, caught at validation time, and it is recorded here as an obligation on whoever wires delivery.** A hostname that *resolves* to an internal address passes every check a validator running hours earlier can make — that is DNS rebinding, and the only place to catch it is at connect time, in the sender. So if you build anything that dials a URL out of this column, resolve the host and re-check the resulting address **immediately before connecting**, and refuse a private, loopback or link-local result then. Reviewing the stored strings is not a substitute; the string can be blameless and the address it resolves to at connect time internal.

Audit what is recorded:

```bash
curl -sS "$BLNK_API/subscribers?limit=100" -H "X-Blnk-Key: $BLNK_MASTER_KEY" \
| jq -r '.[] | select(.webhook_url != null)
             | [.subscriber_id, .webhook_url, (.migrated_at // "NOT MIGRATED")] | @tsv'
```

A row with a `webhook_url` and no `migrated_at` is a subscriber still to be moved. The timeline and the sunset behaviour are in [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md).

## Dead-Letter Triage and Replay

### Before you start: what already happened

An event only reaches a dead-letter topic after its publish retry budget is spent. At the defaults that is **five attempts separated by four waits — 1s, 2s, 4s and 8s, 15 seconds of backoff in total**:

| Setting | Variable | Default |
|---------|---------|---------|
| Maximum publish attempts | `RELAY_MAX_RETRY_ATTEMPTS` | `5` |
| Base delay | `RELAY_RETRY_BASE_BACKOFF_MS` | `1000` |
| Delay ceiling | `RELAY_RETRY_MAX_BACKOFF_MS` | `30000` |

The delay doubles after each failure. **The 30-second ceiling is never reached at the defaults** — the delay after a fifth failure would be 16 seconds, but a fifth failure exhausts the budget and no sixth attempt consumes it. The ceiling engages only if the base delay or the attempt count is raised. `RELAY_MAX_RETRY_ATTEMPTS` is additionally **clamped to 5**, with a loud warning, because the attempt number is an exported metric label and a value of 5000 would mint 5000 label values.

**Every attempt is logged, including the first**, with `event_id`, `event_type`, `topic`, `attempt` and `max_attempts`. So the log is a usable diagnostic trail while a publish is still being retried, not only once it has failed for the last time:

```bash
docker compose logs server | grep '"event_id":"<the event id>"'
```

### The two states, and why only one is replayable

Read the `status` before deciding anything. Both terminal failure states are listed by the endpoint below, and they need different actions:

| `status` | Meaning | Replayable |
|----------|---------|-----------|
| `failed` | The retry budget is spent, but the **dead-letter write itself has not completed** — `dlt_topic` is still null, so there may be no message on the dead-letter topic at all. Until it lands, `blnk.event_outbox` is the only copy of the event in existence. | **No.** Replay refuses with `409 EVENT_NOT_DEAD_LETTERED`. Restore broker reachability so the dead-letter write completes, then replay. The relay re-claims these rows on its own once it can. |
| `dead_lettered` | The event is on its `<topic>.dlt` sibling with failure metadata attached. | **Yes.** |

A third status, `replaying`, means another replay already holds the row. It is a lease, not a resting place: the row returns to `dead_lettered` when that replay completes or its lease expires.

### Step 1 — List the dead-lettered events

```bash
curl -sS "$BLNK_API/events/dead-letter?limit=20" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq .
```

It is **master-key gated**, following the same privileged-endpoint pattern as hook management: a non-master caller gets `403` with `error_detail.code` of `AUTH_MASTER_KEY_REQUIRED`. It reads `blnk.event_outbox` and never a Kafka topic, so **it answers with the broker down** — which is precisely when you want it.

Paged and filtered:

| Parameter | Effect |
|----------|--------|
| `limit` | Page size. Default `20`, maximum `100`; an out-of-range value resets to `20`, a non-numeric one is refused. |
| `offset` | Page offset. A negative value becomes `0`. |
| `event_type` | Exact match on the event name, e.g. `transaction.applied`. |
| `topic` | Exact match on the **original category** topic, e.g. `blnk.transactions`. |
| `dlt_topic` | The same filter expressed as the `.dlt` sibling, e.g. `blnk.transactions.dlt`. |
| `status` | `failed` or `dead_lettered`. Anything else is refused. |
| `include_count` | Adds a `{data, total_count}` envelope. Not supported together with `event_type` or `topic`, because no filter-aware count exists; drop the filter to get a total. |

Ordering is **fixed** at newest occurrence first, ties broken by descending id, which is what makes paging stable. `sort_by` and `sort_order` are accepted for client compatibility and change nothing. There is **no occurrence-window filter at any layer**, and one is refused rather than approximated — an operator who needs an arbitrary window pages the inventory, which is already ordered by occurrence, or queries `blnk.event_outbox` directly. Any other parameter is refused with `400 GEN_VALIDATION_ERROR` naming every offending name at once.

An empty inventory is `200` and `[]` — never `404` and never `null`. "No events are stuck" is a successful answer, and a script must be able to range over the result unconditionally.

Narrow to one topic's replayable backlog:

```bash
curl -sS "$BLNK_API/events/dead-letter?topic=blnk.transactions&status=dead_lettered&limit=100" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq '.[] | {event_id, event_type, status, attempts, failure_reason, last_attempted_at}'
```

Each item carries `event_id`, `event_type`, `aggregate_id`, `ledger_id`, `partition_key`, `occurred_at`, `schema_version`, `topic`, `dlt_topic`, `status`, `attempts`, `failure_reason`, `first_attempted_at`, `last_attempted_at` and `payload_bytes`. **The payload itself is never returned** — the projection is the single place the stored payload, the raw driver error text and the internal failure struct are dropped, so an operator triaging a backlog does not receive a copy of every event body.

### Step 2 — Read the failure metadata

The dead-letter *message* on the topic carries a `failure_metadata` object with **exactly five fields**, attached as an additive sibling key at the top level — never nested inside `payload`, never replacing it, and never reordering an envelope key. That is precisely what leaves the original event recoverable unchanged. The listing above surfaces the same information as `topic`, `failure_reason`, `attempts`, `first_attempted_at` and `last_attempted_at`.

| Field | What it tells you |
|-------|------------------|
| `original_topic` | **The replay destination.** The category topic the event was destined for — not the `.dlt` sibling it is sitting on. |
| `error_reason` | **The diagnosis.** Why the final attempt failed, verbatim. |
| `attempt_count` | **Confirmation that retries were exhausted.** At the defaults this is `5`. A lower number means the budget was configured smaller, not that Blnk gave up early. |
| `first_attempted_at` | When the first attempt was made. |
| `last_attempted_at` | When the final attempt was made. |

**The two timestamps together are the most useful field in the object**, because their difference bounds the window the failure persisted over, and that is how you tell a transient outage from a poison message:

- A gap of roughly **15 seconds** — the whole backoff schedule and nothing more — means every attempt failed back to back. The cause was continuous for the entire window: a broker that was down, a topic that does not exist, a credential that is wrong, an ACL that forbids the write. Fix the cause and replay; the event itself is fine.
- A gap **much longer than the schedule** means the attempts were spread by relay restarts or lease expiries. Look for a cause that came and went.
- Many events sharing a near-identical window are **one incident**, not many. Triage the cause once and replay them together.
- One event failing while its neighbours on the same topic succeeded is a **property of that event** — the size limit, or something the broker rejected about that specific record. Replaying it will fail again.

### Step 3 — Decide

| What `error_reason` looks like | Cause | Do this |
|-------------------------------|-------|---------|
| Connection refused, broken pipe, i/o timeout, `LEADER_NOT_AVAILABLE`, no available brokers | The broker was unavailable during the window | Restore the broker, confirm the healthcheck passes, then replay. Check `blnk_outbox_pending` is falling before you replay in bulk. |
| `UNKNOWN_TOPIC_OR_PARTITION` | The topic is missing — provisioning never ran, or `KAFKA_TOPIC_PREFIX` changed and the new namespace was never created | Re-run provisioning (`make kafka_provision`), verify the ten names with `kafka-topics.sh --describe`, then replay. |
| `INVALID_REPLICATION_FACTOR` while creating, or an under-partitioned topic | Topic geometry is wrong for this cluster | Fix `KAFKA_REPLICATION_FACTOR` for the cluster's broker count and re-run provisioning. A non-empty under-partitioned topic will be **refused**, not grown — see [Partitions](#partitions). Then replay. |
| `SaslAuthenticationException`, `TopicAuthorizationException`, `CLUSTER_AUTHORIZATION_FAILED` | The producer principal's credential or ACLs are wrong | Repair `KAFKA_SASL_USER`/`KAFKA_SASL_SECRET` and confirm the producer holds `Write` and `Describe` on the owned topics, then replay. **Do not** work around it with `KAFKA_ALLOW_ADMIN_PRODUCER`. |
| `MESSAGE_TOO_LARGE`, or a size refusal from Blnk | The event exceeds the 768 KiB publish limit | **Replay will fail again.** Investigate the producer: this is an oversized payload, not a transport fault. Capture the `event_id`, `event_type` and `payload_bytes` and raise it against the emitting code path. |
| A serialisation or encoding error naming this one event | A genuinely malformed event | **Replay will fail again.** Investigate the producer. Leave the row dead-lettered as evidence. |

For anything in the last two rows, do not loop on replay — each attempt costs a broker round trip and leaves the row exactly where it was.

### Step 4 — Replay

`POST /events/dead-letter/:event_id/replay`, where `:event_id` is the `event_id` from the listing — not the outbox row's numeric id, and not the `aggregate_id`. Master-key gated like the rest of the surface.

```bash
curl -sS -X POST \
  "$BLNK_API/events/dead-letter/9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b/replay" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq .
```

```json
{
  "event_id": "9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b",
  "topic": "blnk.transactions",
  "status": "dispatched",
  "replayed_at": "2026-05-01T14:02:11.334912Z"
}
```

`topic` is the **original category topic** the event went back to, never the dead-letter topic it was listed from.

| Status | `error_detail.code` | Meaning |
|--------|--------------------|---------|
| `200` | — | The broker acknowledged the re-publish. |
| `400` | `GEN_MISSING_PARAMETER` | No event id in the route. |
| `403` | `AUTH_MASTER_KEY_REQUIRED` | Use the master key. |
| `404` | `EVENT_NOT_FOUND` | No event with that id. |
| `409` | `EVENT_NOT_DEAD_LETTERED` | Not replayable: the row is `failed` rather than `dead_lettered`, already replayed, or a concurrent replay holds it. |
| `500` | `EVENT_REPLAY_FAILED` | The re-publish failed, or it succeeded and the outbox entry could not be cleared. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | No broker is configured, or the broker is down. Fix that first. |

Replaying a backlog is a loop over the listing. Keep it deliberate — one topic and one cause at a time:

```bash
curl -sS "$BLNK_API/events/dead-letter?topic=blnk.transactions&status=dead_lettered&limit=100" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" \
| jq -r '.[].event_id' \
| while read -r id; do
    curl -sS -X POST "$BLNK_API/events/dead-letter/$id/replay" \
      -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq -c '{event_id, topic, status}'
  done
```

#### Two facts that make replay trustworthy

- **It re-publishes the original stored bytes.** The service sends the payload as it was stored on the outbox row, stripping only the failure metadata the dead-letter copy added. Nothing decodes and re-encodes it, because a round trip through a struct would reorder JSON object keys and break the byte-for-byte guarantee. The acknowledgement deliberately does not echo the payload either, so nobody is tempted to diff the wrong pair of byte strings.
- **`event_id` is preserved.** A replay carries the same id and the same partition key as the original, so it lands on the same partition and cannot itself violate ordering, and a subscriber deduplicating on `event_id` absorbs it silently. Replaying an event that was in fact already delivered is therefore safe. Delivery remains at-least-once; the deduplication obligation is the subscriber's, as [event-streaming.md](event-streaming.md#delivery-guarantees-and-your-idempotency-obligation) states.

### Step 5 — Verify

Three checks, in increasing strength.

```bash
# 1. The outbox row left the dead-lettered state. The event id should no longer
#    appear in the inventory.
curl -sS "$BLNK_API/events/dead-letter?status=dead_lettered&limit=100" \
  -H "X-Blnk-Key: $BLNK_MASTER_KEY" \
| jq -r '.[].event_id' | grep -c '9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b' || echo "cleared"

# 2. The counts moved: dead_lettered down, dispatched up.
curl -sS "$BLNK_API/events/stats" -H "X-Blnk-Key: $BLNK_MASTER_KEY" \
| jq '{dispatched, failed, dead_lettered, replaying}'
```

```bash
# 3. The message landed. The original topic's end offset advanced by the number
#    of events you replayed.
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --topic blnk.transactions --time latest
```

Finally, watch `blnk_dlt_oldest_message_age_seconds` for the affected `.dlt` topic. It is re-read from authoritative state on every collector tick and **reports an explicit zero when a topic's inventory is empty**, so zero is the reading that clears the alert. A row still in `failed` keeps the gauge non-zero even after every `dead_lettered` row is replayed — that is the gauge working, not a stuck value.

## The Daily Outbox-versus-Offset Reconciliation

This is the zero-loss check. Run it once a day. It compares **outbox rows that claim to have been published** against **records the broker actually holds**, and it is the procedure acceptance criterion V-2 is scored on.

`CountEventOutboxByStatus` in `database/event_outbox.go` exists specifically to serve this check, and `GET /events/stats` exists to expose it. Neither is a general-purpose reporting API; do not build dashboards on them.

### Step 1 — Take the snapshot

```bash
curl -sS "$BLNK_API/events/stats" -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq . > recon-$(date -u +%Y%m%dT%H%M%SZ).json
cat recon-*.json | jq .
```

One call gets both sides. A realistic response:

```json
{
  "pending": 12,
  "processing": 3,
  "webhook_pending": 0,
  "dispatched": 1048571,
  "failed": 0,
  "dead_lettered": 4,
  "replaying": 0,
  "topic_end_offsets": {
    "blnk.transactions": 981204,
    "blnk.transactions.dlt": 3,
    "blnk.balances": 61118,
    "blnk.balances.dlt": 1,
    "blnk.identities": 5902,
    "blnk.identities.dlt": 0,
    "blnk.ledgers": 341,
    "blnk.ledgers.dlt": 0,
    "blnk.system": 12,
    "blnk.system.dlt": 0
  },
  "offsets_complete": true,
  "partitions_unavailable": 0,
  "offsets_measured_at": "2026-05-02T02:00:04.117Z",
  "generated_at": "2026-05-02T02:00:03.902Z",
  "reconciliation": {
    "terminal_events": 1048575,
    "confirmed_events": 1048575,
    "unconfirmed_events": 0,
    "duplicated_records": 0,
    "messages_written": 1048581,
    "overhead": 6,
    "loss_detected": false,
    "conclusive": true,
    "summary": "1048581 broker records against 1048575 terminal outbox rows: no loss detected, overhead 6.",
    "measured_at": "2026-05-02T02:00:04.117Z"
  }
}
```

`missing_topics` and `caveats` are absent here because both are omitted when empty, which on this response is the healthy reading.

The **seven per-status counts are explicit fields**, one per member of the outbox state machine, so a status with a count of zero reads as "none in that state" rather than "no such state":

| Field | Meaning | Counts as published? |
|-------|---------|---------------------|
| `pending` | Written and committed, not yet claimed | No — in flight |
| `processing` | Claimed by a relay instance, lease held | No — in flight |
| `webhook_pending` | Kafka leg acknowledged; only the legacy HTTP leg is still owed | **Yes** |
| `dispatched` | Broker acknowledged. Terminal | **Yes** |
| `failed` | Retry budget spent, dead-letter write still owed | No — not yet on any topic |
| `dead_lettered` | On its `.dlt` sibling. Terminal, and replayable | **Yes** |
| `replaying` | A replay holds the row | No — publish in flight |

Leave `include_offsets` off. Absent means **best effort**: the broker is read when one is configured, and a failure is logged and omitted, which is what this procedure wants. `include_offsets=true` makes the broker read *required* and answers `503 EVENT_KAFKA_UNAVAILABLE` on failure; `include_offsets=false` skips the broker entirely. There is deliberately no topic-narrowing parameter — the verdict compares the broker against **every** outbox row that claims a publication, so measuring a subset of topics would manufacture a shortfall and report loss that has not happened.

### Step 2 — Read the verdict, in this order

The server computes the verdict itself, so you do not re-implement the arithmetic. Branch on three fields, and **in this order**:

```bash
jq -r '
  if .reconciliation == null then
    "INCONCLUSIVE: no broker side was measured"
  elif (.reconciliation.conclusive | not) then
    "INCONCLUSIVE: " + ((.reconciliation.caveats // []) | join("; "))
  elif .reconciliation.loss_detected then
    "LOSS DETECTED: " + .reconciliation.summary
  else
    "PASS: " + .reconciliation.summary
  end' recon-*.json
```

1. **`reconciliation` absent** → no offsets could be read at all. Either no brokers are configured — a legitimate steady state, not an error — or the broker was unreachable. There is nothing to compare. Not a pass and not a failure.
2. **`conclusive` false** → counting cannot decide the matter today. Read `caveats`, which names every reason in plain words. **This is the field to check before reporting anything green**, and it is the one an eager script skips.
3. **`loss_detected` true** → the broker holds **fewer** records than the outbox has terminal rows: rows claiming a publication no record corresponds to. Escalate — Step 5.
4. **`conclusive` true and `loss_detected` false** → **pass**. Record `summary`.

The pass condition, stated as arithmetic:

```text
terminal_events  = dispatched + webhook_pending + dead_lettered   (rows claiming publication)
messages_written = SUM(topic_end_offsets)                         (the five topics + the five .dlt siblings)
overhead         = messages_written - terminal_events             (SIGNED, never clamped)

PASS  when  conclusive == true  AND  overhead >= 0
FAIL  when  conclusive == true  AND  overhead <  0     (loss_detected)
```

**The comparison is directional, and that is the whole design.** Every terminal row must have produced *at least one* broker record, so the broker side may legitimately exceed the outbox side and routinely does. Only a shortfall is evidence of loss. Do not diff the two totals and alert on any difference — you will alert constantly.

Two fields keep a surplus honest, and both make the result inconclusive when non-zero:

- **`unconfirmed_events`** — rows claiming a publication they cannot name a topic, partition and offset for. A surplus is otherwise *indistinguishable from compensated loss*: ten redeliveries plus ten lost events produce exactly the totals of a healthy pipeline. While any row is unconfirmed, the result is inconclusive rather than green.
- **`duplicated_records`** — confirmed rows sharing a coordinate with another row. **It should always be zero.** A partial unique index forbids two rows naming the same record, so a non-zero value reports a broken schema, not tolerable duplication.

`confirmed_events` is published so the verdict is auditable: it is how much of the claim is individually corroborated rather than inferred from a total.

### Step 3 — Cross-check the broker side from the CLI (optional)

Useful when you want the offsets independently of the API, or when the API's broker read is the thing you suspect:

```bash
# End offsets for all ten topics, one line per topic-partition.
for t in transactions balances identities ledgers system; do
  for topic in "blnk.$t" "blnk.$t.dlt"; do
    docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server kafka:9092 \
      --command-config /tmp/blnk-kafka/client-admin.properties \
      --topic "$topic" --time latest
  done
done
```

Each line is `topic:partition:offset`. Sum the third field to get `messages_written`:

```bash
… | awk -F: '{ total += $3 } END { print total }'
```

`--time latest` is the end offset, which is what the comparison uses. `--time earliest` gives the first retained offset, and the difference between the two is what retention has deleted — which is Step 4's first caveat.

`event_admin.go` reads the same numbers in process through `ListOffsets`, so the API and the CLI answer the same question. A disagreement between them larger than the traffic in the interval means one of the two could not read every partition; `partitions_unavailable` in the response says whether it was the API.

### Step 4 — Account for legitimate differences before declaring a discrepancy

Four differences are expected. Rule each one out before escalating, or this procedure will produce false alarms and stop being run.

1. **In-flight rows are legitimately not on a topic yet.** `pending` and `processing` rows have been captured and not yet published; `replaying` rows have a publish in flight; a `failed` row's dead-letter write is still owed. None of them is counted in `terminal_events`, and none of them is loss. A large `pending` figure is a relay throughput question, not a reconciliation one — check `blnk_outbox_pending`.
2. **A positive skew is normal, because delivery is at-least-once.** The relay publishes an event and *then* marks its row dispatched: two operations against two systems with no transaction spanning them, so a crash in between leaves a published-but-unmarked row that the next relay publishes again. Replays and dead-letter copies add records too. So a **small positive `overhead` is health, not a defect** — a healthy system's overhead is small and positive rather than zero. A **negative** overhead is the real signal. The outbox gives write-side exactly-once *capture*; it does not give exactly-once delivery, and this asymmetry is the visible consequence.
3. **The two sides are measured at different instants.** The counts come from PostgreSQL and the offsets from Kafka, in separate round trips — compare `generated_at` against `offsets_measured_at`. Under live traffic a difference proportional to that gap is expected. Take the snapshot when traffic is lowest, keep the two timestamps close, and **re-read before escalating**: a discrepancy that disappears on a second snapshot was a timing artefact.
4. **Retention deletes records while their outbox rows remain.** An expired record is gone from the end-offset reading, which manufactures a shortfall that is not loss. The server detects this and reports `conclusive: false` with a caveat naming it rather than reporting a false positive. Bound the comparison rather than fighting it:
   - **Run it daily**, well inside the topics' retention period, so no record in the window has expired.
   - **Compare the earliest retained offset against zero.** `kafka-get-offsets.sh --time earliest` returning a non-zero offset means records have already been deleted from that topic, so a whole-history comparison on it can never be conclusive.
   - **Bound the outbox side the same way** by setting `RELAY_EVENT_RETENTION_DAYS` to a period shorter than the broker's retention, so terminal rows are purged before their records expire and the two sides age together. It is `0` — retention disabled — by default, deliberately: deleting ledger-adjacent records is a decision only an operator can take, and only terminal rows are ever eligible. Confirm the sweep is running with `blnk_events_purged_total`.

Note the asymmetry when interpreting `offsets_complete`: a **missing topic** or an **unreadable partition** lowers the broker side without any event having been lost, so `missing_topics` and `partitions_unavailable` are why a shortfall may be an artefact. `offsets_complete` is the single field to branch on before comparing anything, and it is present on every response.

### Step 5 — Escalate

When `conclusive` is true and `loss_detected` is true, and a second snapshot agrees, treat it as event loss.

Capture, before anything is restarted:

1. **Both snapshots**, whole. `generated_at` and `offsets_measured_at` are part of the evidence.
2. **`reconciliation.summary`**, `terminal_events`, `messages_written`, `overhead`, `unconfirmed_events` and `duplicated_records`, verbatim.
3. **`blnk_outbox_pending`** at the time of the snapshot, and its trend over the preceding day — a backlog that dropped without a matching rise in `blnk_events_published_total` is the shape of rows leaving the table without reaching the broker.
4. **`blnk_events_published_total`** and **`blnk_events_dead_lettered_total`** over the same window.
5. **Relay logs from the server role**, which is where the relay runs. Search for the event-outbox relay's own lines and for mark failures — a publish that succeeded while the row could not be marked is exactly the window duplicates come from, and its inverse is the window to investigate here:

   ```bash
   docker compose logs server | grep -i 'event outbox relay'
   docker compose logs server | grep -iE 'dead.letter|mark|outbox' | tail -200
   ```

6. **The topics' earliest offsets**, to prove or exclude retention:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --topic 'blnk\..*' --time earliest
   ```

7. **`SELECT status, count(*) FROM blnk.event_outbox GROUP BY status;`** run directly, as an independent check that the endpoint and the table agree.

Then check the two things that would explain a shortfall without loss having occurred: whether the retention sweep is deleting rows faster than the comparison window (`RELAY_EVENT_RETENTION_DAYS` against the broker's `retention.ms`), and whether anything other than Blnk has deleted or recreated a topic. A recreated topic resets its offsets to zero, which reads as a shortfall of everything ever published to it.

If neither explains it, this is a defect in the relay's mark-after-publish path. Do not purge, replay in bulk, or truncate anything until the evidence above is captured — the outbox row is the only remaining record of an event whose message is gone.

### Step 6 — When counting is not enough: the per-`event_id` audit

Steps 1 to 5 compare **totals**, and totals have one blind spot that no amount of care in reading them removes: **a surplus is indistinguishable from compensated loss.** Ten redeliveries alongside ten lost events produce exactly the totals of a healthy pipeline. `unconfirmed_events` is what stops that reading as green, but proving the stronger property — that **every** `event_id` reached a topic at least once — needs the topics read and deduplicated by `event_id`, not counted.

**That requires a consumer, and Blnk implements no consumer by design.** `ConsumerLag` and `ListOffsets` count records; they cannot tell you which ones. So this step is operator work with your own tooling, and it is the audit to run when Step 2 returned a verdict you do not trust — after an incident, after a broker replacement, or when `unconfirmed_events` is persistently non-zero.

1. **Take the outbox side.** Every row that claims a publication, over a bounded window:

   ```bash
   psql "$BLNK_DATA_SOURCE_DNS" -At -F, -c "
     SELECT event_id
     FROM blnk.event_outbox
     WHERE status IN ('dispatched','webhook_pending','dead_lettered')
       AND occurred_at >= now() - interval '24 hours'
     ORDER BY event_id;" > /tmp/outbox-ids.txt
   wc -l /tmp/outbox-ids.txt
   ```

2. **Take the broker side.** Read every category topic and its `.dlt` sibling from the beginning of the window with a consumer that holds a grant over all ten — the administrative principal, or a purpose-made audit principal — and extract the envelope's `event_id`. Any consumer will do; the console consumer is enough for a one-off:

   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 \
     --consumer.config /tmp/blnk-kafka/client-admin.properties \
     --topic blnk.transactions --from-beginning --timeout-ms 60000 2>/dev/null \
   | jq -r .event_id
   ```

   Repeat per topic, concatenate, then `sort -u` — **the deduplication is the point of the exercise**, because a redelivered event legitimately appears more than once.

3. **Diff, in one direction only.** Ids present in the outbox and absent from the broker are the finding:

   ```bash
   comm -23 <(sort -u /tmp/outbox-ids.txt) <(sort -u /tmp/broker-ids.txt)
   ```

   Empty output is the pass. Any line is an event that claims a publication no record corresponds to, named individually — which is what Step 2 could only ever report as a number.

4. **Ignore the other direction.** Ids on the broker and not in the outbox are the expected residue of a retention purge that has already deleted the row (see Step 4), or of a window boundary. `comm -13` is not a finding.

Two caveats. The consumer must start from an offset **inside** the window, or retention will manufacture a shortfall exactly as it does for the counting check. And the window has to be closed: an event captured while the consumer was running may legitimately be in the outbox and not yet on a topic, so bound the outbox query to occurrences that predate the consumer's start.

## Alert Response

`alerts/blnk-kafka-alerts.yml` defines **one rule group, `blnk-kafka-alerts`, evaluated every 30 seconds**, holding four rules. Each names this document as its `runbook_url`. The sections below are in the same order as the file.

Before working any of them, satisfy yourself that the alert is *loaded* — see [Is the alert armed at all?](#is-the-alert-armed-at-all) — because "the alert did not fire" and "the alert was never evaluated" look identical from the outside.

The full metric catalogue, the attribute domains and example PromQL are in [metrics.md](metrics.md); they are not repeated here.

### DeadLetterMessageStuck

```text
expr:     blnk_dlt_oldest_message_age_seconds > 900
for:      0m
severity: critical
```

**900 seconds is the 15-minute threshold**, written as a literal so it is greppable. The dwell is deliberately `0m` rather than omitted: the threshold already carries the whole 15 minutes, and a `for` here would double-count it.

The gauge is an **age, not a count** — a single entry this old is enough to fire — and it is the age of the *oldest unresolved* entry per dead-letter topic, so one comparison covers the whole inventory. Its inventory is every outbox row in the `failed` **or** `dead_lettered` state, reported under the `.dlt` topic it was destined for, so an event whose dead-letter write itself failed is included.

1. **Identify the topic** from the alert's `topic` label. It is a `.dlt` name; the original category topic is that name with `.dlt` removed, and it is where a replay goes.
2. **Read the status first, because remediation depends on it.**

   ```bash
   curl -sS "$BLNK_API/events/stats" -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq '{failed, dead_lettered, replaying}'
   ```

   A non-zero `failed` count means at least one entry is **not replayable yet**: its retry budget is spent but the dead-letter write has not completed, so there may be no message on the topic at all, and replay refuses it with `EVENT_NOT_DEAD_LETTERED`. **Restore broker reachability so the dead-letter write completes**; the relay re-claims those rows itself. Only then replay.
3. **List the entries** for that topic — [Step 1](#step-1--list-the-dead-lettered-events).
4. **Read the failure metadata** and bound the failure window — [Step 2](#step-2--read-the-failure-metadata).
5. **Fix the underlying cause** — [Step 3](#step-3--decide). Replaying before the cause is fixed re-dead-letters the event and buys nothing.
6. **Replay** — [Step 4](#step-4--replay).
7. **Confirm the gauge returns toward zero.** The collector re-reads it from authoritative state on every tick and records an **explicit zero** when a topic's inventory is empty; **that zero is the reading that clears the alert.** A gauge stuck above the threshold after a successful replay means entries remain — most often rows still in `failed`.

### SubscriberConsumerLagHigh

```text
expr:     blnk_kafka_consumer_lag > 10000
for:      2m
severity: warning
```

A warning rather than a page: the events are durably in Kafka and a consumer catches up on its own, so this is degradation, not loss. The two-minute dwell is four consecutive evaluations — consumer lag is legitimately spiky, and a short dwell avoids paging on a burst that self-corrects.

**Blnk does not manage subscriber consumers.** More often than not the remedy is to contact the subscriber rather than to change anything in Blnk. Work the checks that are yours first, then hand it over with evidence.

1. **Identify the subscriber, group and topic** from the `subscriber`, `group` and `topic` labels. The first two are **pseudonyms** — a stable, truncated SHA-256 of the registry identifier — never customer-chosen names, because an annotation is rendered into notifications and incident tickets. Resolve one by listing the registry and hashing each `subscriber_id` the same way, or by searching the service logs for the matching `subscriber_id_hash` field, which uses the same hash:

   ```bash
   curl -sS "$BLNK_API/subscribers?limit=100" -H "X-Blnk-Key: $BLNK_MASTER_KEY" | jq -r '.[].subscriber_id'
   ```

   Three collapse tokens can appear instead of a pseudonym, and none of them is hashed because none is anyone's name: `unattributed` (the reading named no subscriber), `unregistered` (a value was named but Blnk did not issue it — worth investigating on its own), and `other` on the `topic` label (a topic Blnk does not own).
2. **Is the consumer running at all?** A group that has never committed reports **full lag from the earliest retained offset** rather than zero, by design — a subscriber that never started must not look healthy. So a lag figure close to a topic's whole retained volume usually means "not consuming", not "far behind".

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --describe --group blnk-sub-<subscriber_id>.default
   ```

   Read `CONSUMER-ID` and `HOST`: empty means no member is connected. A group perpetually in `PreparingRebalance` or `CompletingRebalance` is thrashing — usually a consumer whose processing exceeds `max.poll.interval.ms`, which is the subscriber's setting to fix.
3. **Can it still read?** A grant narrowed or revoked since the consumer last connected produces lag that will never drain. Confirm the bindings still exist — [Verifying a subscriber's access](#verifying-a-subscribers-access) — and that the group it is using is inside its `blnk-sub-<subscriber_id>.` namespace. Joining a group outside that namespace is refused.
4. **Is it consuming slower than Blnk publishes?** Compare the lag trend against `blnk_events_published_total` for that topic. Rising lag on a flat publish rate is the consumer; rising lag on a rising publish rate may simply be a burst.
5. **Partition count against consumer count.** A consumer group cannot use more consumers than the topic has partitions, so with `KAFKA_MIN_PARTITIONS=6` a seventh instance is idle and adds no throughput. If the subscriber has fewer consumers than partitions, more instances will help; if it already has six, more will not, and **the partition count cannot be raised on a live topic** — see [Partitions](#partitions).
6. **Escalate to the subscriber** with the resolved `subscriber_id`, the topic, the lag figure, the group's member list and the topic's retention. The deadline that matters is retention: lag is recoverable until the records the consumer has not read start expiring, after which they are gone for that consumer.

### SubscriberRevocationOutstanding

```text
expr:     blnk_subscribers_oldest_revocation_age_seconds > 3600
for:      0m
severity: critical
```

**This is a live credential nobody is accounted for, which is why it is critical.** Provisioning writes a subscriber's SCRAM credential *before* its ACL bindings, because a binding for a principal that does not exist is inert while a credential without bindings still **authenticates**. A binding failure is therefore compensated by revoking the credential — and when that compensation also fails, a means of authenticating to the event bus exists at the broker for a principal the registry records no issuance for.

It fires on the **age**, not the count, and the age is measured from when the obligation was *first* recorded and is never reset by a later failed attempt — so it reports the age of the exposure rather than of the last try. A marker cleared within a minute is routine: the automatic settlement paths are working. One outstanding for an hour means neither has been exercised.

1. **Find the affected rows.** They are not attributed in the alert on purpose — a subscriber label would export a tenant identifier into every notification — and the registry API does not project the marker either, so read it from the table. `revocation_pending_at` is the tombstone, and it names the principal that has to be revoked:

   ```bash
   psql "$BLNK_DATA_SOURCE_DNS" -c "
     SELECT subscriber_id, kafka_principal, consumer_group_id, revocation_pending_at,
            now() - revocation_pending_at AS outstanding_for
     FROM blnk.event_subscribers
     WHERE revocation_pending_at IS NOT NULL
     ORDER BY revocation_pending_at;"
   ```

   The row still exists precisely so the failure is recoverable: deregistration marks the row, revokes at the broker, and deletes the row only once the revocation is confirmed. A row carrying this timestamp therefore names a principal that may still authenticate. A pending row is not an active subscriber — credential issuance refuses for it.

2. **Try the automatic settlement paths first**, because both are safe and both settle the marker as a side effect. **Re-issuing** replaces the orphaned credential by construction; **deprovisioning** revokes it. Pick whichever matches the subscriber's actual status — re-issue if they should have access, deprovision if they should not:

   ```bash
   # Re-issue: the subscriber should keep access. Destructive to its previous secret.
   curl -sS -X POST "$BLNK_API/subscribers/<subscriber_id>/kafka-credentials" \
     -H "X-Blnk-Key: $BLNK_MASTER_KEY"

   # Or deprovision: the subscriber should have no access. Retrying this finishes a
   # revocation that failed halfway.
   curl -sS -X DELETE "$BLNK_API/subscribers/<subscriber_id>" \
     -H "X-Blnk-Key: $BLNK_MASTER_KEY" -o /dev/null -w '%{http_code}\n'
   ```

3. **Otherwise revoke by hand**, using the `kafka_principal` from the row above:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --alter --delete-config SCRAM-SHA-512 \
     --entity-type users --entity-name blnk-sub-<subscriber_id>
   ```

4. **Confirm the credential is gone** — the `--describe` form of the same command should report no SCRAM entry for the principal — and that `blnk_subscribers_revocation_pending` returns to zero, which is its normal reading.
5. **Then find out why the binding failed.** A `CLUSTER_AUTHORIZATION_FAILED` on `CreateACLs` means the administrative principal is not in `super.users` or has lost its grants; a transport error means the broker was unreachable mid-operation. Fix that before the next issuance, or the next one leaves the same marker.

### ConsumerLagMeasurementDegraded

```text
expr:     blnk_kafka_consumer_lag_unmeasured_partitions > 0
for:      5m
severity: warning
```

**This is not a lag alert. It fires when Blnk cannot tell what the lag is**, which is a distinct and more urgent condition, because **`SubscriberConsumerLagHigh` cannot fire for that topic while this holds.**

The reason is deliberate. Lag is a sum over partitions, so a partition the broker will not report contributes nothing and the total silently becomes a lower bound — which would *resolve* the threshold alert at exactly the moment the system is least able to say whether it should be firing. So an incompletely measured topic publishes **no lag at all** rather than a partial sum, and this rule covers the gap that withholding leaves. Any non-zero value fires: there is no acceptable number of unreadable partitions, because one is enough to make the topic's figure a lower bound.

The five-minute dwell is longer than the lag rule's on purpose. A partition is briefly unreadable during ordinary cluster events — a leader election, a broker restart, a partition reassignment — and those resolve without intervention well inside five minutes.

1. **Check broker and partition health first.** This is a cluster condition, not a subscriber one.

   ```bash
   # Partitions with no leader, or fewer in-sync replicas than replicas.
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --describe --unavailable-partitions

   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --describe --under-replicated-partitions
   ```

2. **Read the partial total from the log.** The event-metrics log line carries it, so you can see how far behind the measured partitions were even though the gauge withheld the topic.
3. **Rule out an ACL cause.** `OffsetFetch` and `ListOffsets` are performed by the administrative principal; a grant it has lost presents as unreadable partitions rather than as an authentication failure.
4. **While it holds, treat consumer lag as unknown for that topic** and read it from `kafka-consumer-groups.sh --describe` by hand if you need a figure. Do not conclude from a missing `blnk_kafka_consumer_lag` series that lag is zero.
5. **It clears by itself** once every partition is readable again: the two lag gauges are **asynchronous**, so the exporter publishes exactly the subscribers the callback observes on each collection and a resolved condition simply stops being exported rather than lingering at its last reading.

### How the lag figure is produced

**Consumer lag is measured in process, and no external lag exporter is part of this deployment.** `event_admin.go` differences each group's committed offsets, read with `OffsetFetch`, against the partition end offsets, read with `ListOffsets`, sums them per topic, and publishes the result on `blnk_kafka_consumer_lag`. There is no `kafka_exporter`, no Burrow and no sidecar to deploy or keep in step with the registry — **if the series are absent, the collector is not running; a separate exporter is not missing.** Do not go looking for one.

Two edge cases are decided explicitly rather than left to arithmetic:

- **No committed offset** (`OffsetFetch` reports `-1`) → the baseline becomes the partition's **earliest retained** offset, so the lag is every record still on the log. Not zero, which would make a subscriber that never started look perfectly healthy; and not zero-based, which would invent lag for records retention has already deleted.
- **A committed offset at or beyond the end** → clamped to zero. It is legitimately transient, since the two offsets are read in separate round trips and a commit can land in between, and the naive subtraction would produce a negative lag.

### Is the alert armed at all?

**A rule file is inert unless `prometheus.yml` lists it**, and this repository had no `rule_files:` stanza before the event pipeline landed. The stanza is what arms the rules; the mere presence of the file is not.

```yaml
rule_files:
  - '/etc/prometheus/alerts/*.yml'
```

That is a path **inside the Prometheus container**. The Compose `prometheus` service bind-mounts this repository's `./alerts` directory to `/etc/prometheus/alerts:ro`, and the glob resolves against it — so a rule file added there later needs no further edit, and the mount and the glob must be kept in step.

Verify, in this order:

```bash
docker compose --profile monitoring up -d prometheus
```

1. **Open `http://localhost:9090/rules`.** The group `blnk-kafka-alerts` must be listed with all four rules and a 30-second evaluation interval. **Check `/rules`, not `/targets`** — a target can show `UP` while the rules never loaded, and a rule that never loaded reports no error anywhere.
2. **Validate the file before you ship a change to it**, which also catches a bad glob:

   ```bash
   docker compose exec prometheus promtool check rules /etc/prometheus/alerts/blnk-kafka-alerts.yml
   docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
   ```

3. **Confirm the series exist.** A rule whose series is never recorded can never fire, and that is indistinguishable from a healthy system:

   ```bash
   curl -sS "http://localhost:9090/api/v1/query?query=blnk_dlt_oldest_message_age_seconds" | jq '.data.result | length'
   ```

> **If `metrics_bearer_token` is set, the scrape fails and every rule sits permanently unable to fire.** Prometheus shows the target down; the rules report nothing wrong. `prometheus.yml` carries the two commented `authorization:` blocks and the mount instructions that fix it — use `credentials_file`, never inline `credentials`, so the token stays out of the committed file and can be rotated without a restart.
>
> **The configuration exists twice.** Kubernetes has no bind mounts, so both `prometheus.yml` and the rule file travel as data in `infrastructure/k8s-manifests/prometheus-configmap.yaml`. **Edit both copies together** — two hand-maintained copies of an alerting configuration diverge silently, each staying valid and reading correctly in review, with one environment alerting and the other not. Tests compare the two, so a lone edit fails rather than shipping.

## Relay Operations

**The event relay runs in the server process role**, started immediately after the fund-lineage outbox processor it is modelled on — this repository's established home for an outbox relay, which also avoids standing up a fourth asynq server for one poll loop. There is no separate relay binary and no relay subcommand: `blnk start` *is* how the relay is run. `make run_relay` is an alias for the server role, provided so that "where does the relay run" is answerable without reading `cmd/server.go`, and so the relay can be run in isolation for a load test or while watching a backlog drain. It refuses to start when `KAFKA_BROKERS` is unset, because the failure it prevents is silent.

**The start is conditional on brokers being configured.** With an empty broker list the relay logs one info line and starts nothing — see [Running Without Kafka](#running-without-kafka).

| Parameter | Value | Why it matters operationally |
|-----------|-------|----------------------------|
| Batch size | 100 rows per claim | The unit of work. A backlog drains in multiples of this per tick. |
| Poll interval | 1 second | The idle latency floor. A row captured just after a tick waits up to a second before its first attempt, which is why the end-to-end latency series is read from `blnk_events_capture_to_dispatch_duration_seconds` and not from the broker-write series. |
| Lock duration | 30 seconds | The lease a claim takes on its rows. **This is the recovery mechanism.** |

Rows are claimed with a CTE using `FOR UPDATE SKIP LOCKED`, ordered by occurrence, which is what makes concurrent relay instances safe without losing FIFO order. The claim additionally returns **at most one row per partition key**, across all instances rather than merely within one, so two events sharing a key can never be in flight simultaneously — that is what makes per-aggregate ordering hold when the relay is scaled out. One consequence matters when you are triaging: **a stuck event holds up later events sharing its partition key**, even ones in another category. Correctness is chosen over throughput here, and the delay is bounded by the retry budget — so read `DeadLetterMessageStuck` as reporting delayed siblings for that key as well as one stuck event, and clear the backlog rather than only the entry that fired.

**The 30-second lease is how a crash recovers.** A relay that dies mid-batch leaves its claimed rows in `processing` with a `locked_until` in the near future; once that expires the rows become claimable again and the next instance picks them up. Nothing has to be reset by hand. The cost is the duplicate window described under [legitimate differences](#step-4--account-for-legitimate-differences-before-declaring-a-discrepancy): a crash between a successful publish and the row being marked leaves the row to be published again, which is exactly why `event_id` deduplication is a subscriber obligation.

Is the relay running and keeping up?

```bash
# One line at start-up naming the batch size, poll interval and lease.
docker compose logs server | grep -i 'event outbox relay'

# The backlog: pending plus processing. Rising against a flat publish rate is a
# relay that is not keeping up; it includes claimed-but-unacknowledged rows, so a
# stalled relay holding every row under a lease cannot read as a drained backlog.
curl -sS "http://localhost:5001/metrics" | grep '^blnk_outbox_pending'
```

> `blnk.event_outbox` and `blnk.lineage_outbox` are **separate tables served by separate relays**, and they are never merged. `NewLineageOutboxProcessor` handles fund lineage; the event relay handles events. The two share a shape — the same batch size, poll interval and lease, the same claim idiom — because the event relay was modelled on the lineage one, and that resemblance is exactly what makes them easy to confuse. Check which table you are looking at before drawing a conclusion.

Retention is a separate, optional sweep. `RELAY_EVENT_RETENTION_DAYS` is `0` — **disabled** — by default, and only terminal rows (`dispatched`, `dead_lettered`) are ever eligible. A `pending`, `processing`, `replaying` or `failed` row is still owed a delivery attempt and is never deleted however old it is; a `failed` row in particular is excluded because this table is the only copy of that event in existence. Bear in mind what an undeleted row holds: the payload is the webhook body verbatim, so a transaction event carries amounts and balance identifiers and an identity event carries names, email addresses, phone numbers, postal addresses and dates of birth. Confirm the sweep is running with `blnk_events_purged_total`; a flat counter alongside a rising `blnk_outbox_pending` means delivered events are accumulating indefinitely.

## The Local Stack

Bring-up order is enforced by Compose and is not optional:

```text
kafka          entrypoint renders server.properties + client-admin.properties,
               runs scripts/kafka-bootstrap.sh, then starts the broker
   |           healthcheck: a full SCRAM handshake plus an authorized metadata
   |           request — a listening socket says nothing about whether SASL works
   v           (interval 10s, timeout 15s, 5 retries, 60s start period)
kafka-init     scripts/kafka-provision.sh — depends_on kafka: service_healthy
   |           one-shot, restart: on-failure:3, exits 0 when nothing changed
   v
server         depends_on kafka: service_healthy AND
worker                    kafka-init: service_completed_successfully
```

The gates matter. Provisioning against a broker that is not yet answering authenticated requests fails in a way that reads like a wrong password, and a relay that starts before the topics exist dead-letters its first events for `UNKNOWN_TOPIC_OR_PARTITION`.

```bash
./stack.sh --init      # create or complete a mode-0600 .env, generating the
                       # admin and producer principals and their secrets
./stack.sh --up        # provision Kafka, then bring the stack up
```

`stack.sh --init` generates `KAFKA_SASL_ADMIN_USER=admin`, `KAFKA_SASL_ADMIN_SECRET`, `KAFKA_PRODUCER_USER=blnk-producer` and `KAFKA_PRODUCER_SECRET` into `.env`, substituting the `{KAFKA_SASL_ADMIN_SECRET}` placeholder the bootstrap path expects. It enables the `kafka` profile **only when `KAFKA_BROKERS` is set**, and always includes the profile on teardown so nothing is left behind. **A bring-up fails when this project's own broker cannot be verified** — it never became healthy, or its catalogue could not be provisioned; nothing is torn down, and the exit status is what says the stack cannot publish yet. An external broker or an empty `KAFKA_BROKERS` is reported and never fatal.

Confirm the stack:

```bash
# 1. The broker is healthy — meaning SASL works, not merely that a port is open.
docker compose ps kafka

# 2. The ten topics exist with the local geometry: 6 partitions, factor 1.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties --describe

# 3. The sample subscriber principal and its bindings exist.
docker compose exec kafka /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --list --principal User:blnk-sample-subscriber

# 4. Provisioning reported success and said what it observed.
docker compose logs kafka-init
```

Local-stack particulars worth knowing before you compare it with production:

- **Replication factor 1.** A single broker cannot do 3.
- **`SASL_PLAINTEXT` on both client listeners.** SCRAM authenticates but nothing is encrypted, which is why the host port is bound to `127.0.0.1` and why `KAFKA_INSECURE_LOCAL_DEV` has to be set for Blnk's clients to dial it at all.
- **The controller listener uses SASL/PLAIN**, not SCRAM. It has to: SCRAM credentials live in the metadata log the controller must read *before* they exist.
- **`auto.create.topics.enable=false`.** A topic created on first use would get the wrong partition count and no ACL, so a mistyped topic name fails loudly instead.
- **`super.users=User:<admin>`.** The administrative principal bypasses ACLs by design, which is why it must never be used to test isolation.
- **The `kafka_data` volume holds `__cluster_metadata`.** Removing it discards the cluster's entire authorization state — every SCRAM credential, the sample subscriber's password and every ACL — and forces a re-bootstrap. `./stack.sh --purge` deletes it.

## Running Without Kafka

**With `KAFKA_BROKERS` empty, the publisher resolves to a no-op, the relay does not start, and the ledger serves, records and processes transactions exactly as it does with Kafka configured.** This is a legitimate steady state, not an incident. It is reported at info level once, not warned about repeatedly, and it is the state every deployment is in before it opts into Kafka.

Consequences to expect, so that none of them is mistaken for a fault:

- **No events are published**, and nothing accumulates in `blnk.event_outbox` because nothing is captured for a publisher that does not exist.
- **`GET /events/stats` still answers `200`**, reporting the per-status counts it does know, with `offsets_complete: false`, the offset keys omitted rather than emitted as nulls, and no `reconciliation` object. That is the correct output, not a failure.
- **`GET /events/dead-letter` still answers**, because it reads PostgreSQL rather than a topic.
- **Replay answers `503 EVENT_KAFKA_UNAVAILABLE`**, since there is nowhere to publish to.
- **Credential issuance answers `503 EVENT_KAFKA_UNAVAILABLE`**, since there is no broker to provision against.
- **The event-pipeline gauges are absent**, not zero, because the collector that feeds them does not run.
- **`scripts/kafka-provision.sh` skips and exits 0**, which is what makes it safe on an unconditional bring-up path.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every publish fails with connection refused or no available brokers; `blnk_outbox_pending` climbs | Broker unreachable — wrong `KAFKA_BROKERS`, the `kafka` Compose profile not selected, or the broker down | Confirm `docker compose ps kafka` is **healthy** and that `KAFKA_BROKERS` matches the listener you can actually reach: `kafka:9092` from inside the network, `localhost:9092` from the host. Selecting the profile without setting `KAFKA_BROKERS` gives a broker Blnk ignores; setting the variable without the profile gives a relay retrying against nothing. |
| Topic creation rejected with `INVALID_REPLICATION_FACTOR` | `KAFKA_REPLICATION_FACTOR` exceeds the broker count | Set it to `1` on a single-broker stack, `3` on a replicated cluster. It is configuration precisely because no single literal is right for both. |
| `ErrReplicationFactorInadequate` on an existing topic | The topic sits at fewer replicas than configured | Reassign the topic's partitions to the configured factor, then re-run assurance. The **minimum** replica count across partitions is what is checked, because durability is decided by the weakest one. |
| `ErrPartitionGrowthRefused` | A non-empty topic has fewer partitions than `KAFKA_MIN_PARTITIONS` | Plan the migration: provision a correctly shaped topic, move consumers, drain the old one. `KAFKA_ALLOW_PARTITION_GROWTH=true` performs the growth step **and re-maps keys**, breaking ordering for every key already written — see [Partitions](#partitions). |
| Bootstrap fails, or the broker starts and authenticates nobody | The image predates Kafka 3.5 / Confluent Platform 7.5.0, so `kafka-storage format` has no `--add-scram` | Raise the image tag above the floor. The stack pins `apache/kafka:3.9.2`. Running `kafka-configs` afterwards **cannot** fix it — see [Step 1](#step-1--bootstrap-the-scram-admin-credential-before-the-brokers-first-start). |
| ACLs are listed by `kafka-acls --list` yet nothing is restricted | `authorizer.class.name` is unset, so ACLs are accepted and never enforced | Set `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` and restart the broker, then re-run the behavioural check in [Verify the authorizer is active](#verify-the-authorizer-is-active-rather-than-trusting-it). **This failure is silent** — assume nothing. |
| Every credential is rejected after `kafka_data` was destroyed | The volume held `__cluster_metadata`, so every SCRAM credential and ACL is gone | Re-bootstrap: bring the broker up so `scripts/kafka-bootstrap.sh` reformats and re-seeds the admin credential, re-run provisioning, then **re-issue** every subscriber credential. The old passwords cannot be restored — they were never stored — so every subscriber has to receive a new one. |
| `SCRAM authentication failed` for a credential you are sure is right | The password contains `,`, `=`, `[` or `]`, and Kafka's `--add-scram` / `--add-config` grammar has no escape sequence, so it was silently truncated | Re-issue with a value from the safe alphabet. The scripts refuse such a value up front by name; a credential set by hand outside them will not. |
| `kafka-init` restarts three times and the stack never comes up | Provisioning is refusing a configuration — a bad credential pair, an impossible replication factor, a rejected variable | Read `docker compose logs kafka-init`. The cap exists so a permanent failure is terminal: the container stays `Exited(1)`, the dependency gate fails fast, and you get one error to read instead of a log growing a fresh copy of itself every few seconds. |
| The publisher refuses to build; the server and worker will not start | Brokers and an administrative pair are configured but no producer pair is | Set `KAFKA_SASL_USER` and `KAFKA_SASL_SECRET`. Publishing as the administrator would make a leaked producer credential a compromise of the cluster's whole authorization state. `KAFKA_ALLOW_ADMIN_PRODUCER=true` is a documented, warned-about escape hatch for a deployment mid-upgrade, not a fix. |
| Both Kafka clients refuse to connect, naming TLS | `KAFKA_TLS_ENABLED` is off and `KAFKA_INSECURE_LOCAL_DEV` is not set | Configure the `KAFKA_TLS_*` block. Only set `KAFKA_INSECURE_LOCAL_DEV` for the local single-broker stack; it is warned about on every configuration load. |
| Credential issuance answers `409 SUBSCRIBER_ISOLATION_UNENFORCEABLE` | The subscriber row records a `partition_key_prefix`, which Kafka cannot enforce | Clear the prefix and accept topic-level access, or separate the data another way. See [the partition-key prefix](#the-partition-key-prefix-is-recorded-but-not-enforceable). |
| Replay answers `409 EVENT_NOT_DEAD_LETTERED` | The row is `failed` (its dead-letter write is still owed), already replayed, or another replay holds it | Restore broker reachability so the dead-letter write completes, then replay. See [the two states](#the-two-states-and-why-only-one-is-replayable). |
| A subscriber cannot join its consumer group | The group is outside its `blnk-sub-<subscriber_id>.` namespace, or the binding was written without the trailing delimiter | Use a leaf inside the namespace — `blnk-sub-<id>.default` is the issued default. Check the binding is `PREFIXED` on the dot-terminated namespace. |
| The alerts never fire, and nothing looks wrong | The rules are not loaded, or the scrape is refused | Check `http://localhost:9090/rules` — not `/targets` — and the bearer-token note in [Is the alert armed at all?](#is-the-alert-armed-at-all). |

## Related Documents

| Document | Covers |
|----------|--------|
| [event-streaming.md](event-streaming.md) | The subscriber contract: the topic catalogue, the `LedgerEvent` envelope, the event vocabulary, the payload, the delivery guarantees and the idempotency obligation, partitioning and ordering, and the `<topic>.dlt` naming convention |
| [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md) | Migrating off HTTP webhooks: the dual-run timeline, the payload-equivalence guarantee, and what happens at the sunset |
| [metrics.md](metrics.md) | The metric catalogue, the attribute domains and example Prometheus queries |
