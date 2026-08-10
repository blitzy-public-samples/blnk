# Blnk Kafka Operations Runbook

This is the operator's runbook for Blnk's Kafka event pipeline: how to provision the broker, what the subscriber access model actually enforces, how to triage and replay a dead-lettered event, how to run the daily zero-loss reconciliation, and what to do when one of the alerts fires. It is written as steps to execute rather than as an overview — the subscriber-facing contract (the topic catalogue, the `LedgerEvent` envelope, the event vocabulary and the ordering guarantee) is owned by [event-streaming.md](event-streaming.md) and is referenced here, never restated.

## How to Use This Document

Every rule in `alerts/blnk-kafka-alerts.yml` names this file as its `runbook_url`, and **all 13 of them are listed below** — the table is the complete set, not a selection. `TestKafkaAlertInventory_IsStatedOnceAndAgreesEverywhere` fails if a rule is added without a row here. If you arrived from a notification, go straight to your alert:

| Alert | Response section |
|-------|-----------------|
| `DeadLetterMessageStuck` | [DeadLetterMessageStuck](#deadlettermessagestuck) |
| `SubscriberConsumerLagHigh` | [SubscriberConsumerLagHigh](#subscriberconsumerlaghigh) |
| `SubscriberRevocationOutstanding` | [SubscriberRevocationOutstanding](#subscriberrevocationoutstanding) |
| `SubscriberCredentialOrphaned` | [SubscriberCredentialOrphaned](#subscribercredentialorphaned) |
| `SubscriberRevocationRefused` | [SubscriberRevocationRefused](#subscriberrevocationrefused) |
| `ConsumerLagMeasurementDegraded` | [ConsumerLagMeasurementDegraded](#consumerlagmeasurementdegraded) |
| `SubscriberLagCoverageStale` | [SubscriberLagCoverageStale](#subscriberlagcoveragestale) |
| `SubscriberLagCoverageIncomplete` | [SubscriberLagCoverageIncomplete](#subscriberlagcoverageincomplete) |
| `SubscriberSettlementNotProgressing` | [SubscriberSettlementNotProgressing](#subscribersettlementnotprogressing) |
| `SubscriberSettlementOutstanding` | [SubscriberSettlementOutstanding](#subscribersettlementoutstanding) |
| `EventMetricsCollectionStale` | [EventMetricsCollectionStale](#eventmetricscollectionstale) |
| `EventMetricsCollectionFailing` | [EventMetricsCollectionFailing](#eventmetricscollectionfailing) |
| `EventMetricsCollectionAbsent` | [EventMetricsCollectionAbsent](#eventmetricscollectionabsent) |

If you arrived for routine work, the four procedures are [Provisioning](#provisioning), [The ACL Model](#the-acl-model), [Dead-Letter Triage and Replay](#dead-letter-triage-and-replay) and [The Daily Outbox-versus-Offset Reconciliation](#the-daily-outbox-versus-offset-reconciliation).

> **Two tables, two relays, and they are not the same thing.** `blnk.event_outbox` is the event pipeline's outbox and is served by the event relay. `blnk.lineage_outbox` is the fund-lineage feature's outbox and is served by its own processor; its behaviour is unchanged by anything in this document. They are separate tables with separate relays and are never merged. An investigation aimed at the wrong one will find a healthy table and conclude, wrongly, that nothing is stuck.

## Prerequisites

- **The master key.** Every event and subscriber endpoint in this runbook gates on it as its first act and answers `403` with `error_detail.code` of `AUTH_MASTER_KEY_REQUIRED` to anything else. A scoped API key cannot reach them. It travels in the `X-Blnk-Key` header. The examples below assume `BLNK_API` holds the API base URL, default `http://localhost:5001`.

  **Keep it out of `argv`.** Every `curl` in this runbook reads the header from a protected
  configuration file rather than passing `-H` on the command line, because a command line is
  world-readable through `/proc` for the life of the process and is captured by shell history and by
  most CI log collectors. There is no local-development exception to that in this document: the
  inline `-H "X-Blnk-Key: ..."` form appears nowhere below, so an operator following a step verbatim
  cannot leak the key by accident.

- **Database access, when a procedure needs it.** Four commands in this runbook read PostgreSQL
  directly, and none of them puts a DSN on the command line — same `argv` exposure. They all run as
  `$BLNK_PSQL`, which carries the connection parameters and reads the password from a `PGPASSFILE`.

Both are established by one snippet, stated once, in
[Keep credentials out of process arguments](#keep-credentials-out-of-process-arguments) below. Run it
per operator shell before any step in this runbook. It used to be stated twice, here and there, with
two different file names and two different ways of reading the key — which is how thirteen examples
came to pass the master key on the command line while both copies claimed that none did.
- **Metrics.** `enable_observability` must be true for any gauge or counter named here to exist. See [metrics.md](metrics.md) for the catalogue, the attribute domains and example queries; this document does not duplicate them.
- **Broker access**, for the CLI steps only. The API-driven steps — listing, replaying and reconciling — need no broker access at all.

### Keep credentials out of process arguments

Anything passed as a command-line argument is world-readable in `/proc` for the life of the process, and it lands in shell history and in any audit log that records argv. That is true of the master key, of a database DSN with a password in it, and of a subscriber's SASL secret. Every example in this runbook is written to avoid it, and they all assume the environment this snippet establishes:

```bash
# Run once per operator shell. umask 077 means the files are never briefly world-readable.
umask 077

# The API base URL is not a secret; the master key is, so it goes in a curl config file.
export BLNK_API="${BLNK_API:-http://localhost:5001}"
export BLNK_CURL_CONFIG="$HOME/.blnk-curl"
printf 'header = "X-Blnk-Key: %s"\n' "$(cat /run/secrets/blnk-master-key)" > "$BLNK_CURL_CONFIG"

# PostgreSQL: the DSN's password goes in a password file, not in psql's argv.
# PGPASSFILE lines are host:port:database:user:password, and the file must be mode 0600.
export PGPASSFILE="$HOME/.pgpass"
export BLNK_PSQL="psql --no-psqlrc -X -h db.internal -p 5432 -U blnk -d blnk"
```

Every `curl` below then reads the header with `--config "$BLNK_CURL_CONFIG"`, and every `psql` below runs as `$BLNK_PSQL`, which carries no secret. Substitute a secret manager for the `cat` and the `.pgpass` file if you have one; the point is only that a secret reaches the tool through a private file rather than through `argv`. Adjust the connection parameters to your deployment.

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

Four category topics and their four dead-letter siblings — **eight topics, and they are the complete inventory**. Blnk writes to no other topic.

| Category topic | Dead-letter topic | Grantable to a subscriber |
|---------------|-------------------|---------------------------|
| `blnk.transactions` | `blnk.transactions.dlt` | Yes |
| `blnk.balances` | `blnk.balances.dlt` | Yes |
| `blnk.identities` | `blnk.identities.dlt` | Yes |
| `blnk.system` | `blnk.system.dlt` | Yes — but read the disclosure note first |

Every name is composed as `<prefix>.<category>` and `<prefix>.<category>.dlt`, where the prefix is `KAFKA_TOPIC_PREFIX` and defaults to `blnk`. Set `KAFKA_TOPIC_PREFIX=acme` and the whole inventory moves to `acme.transactions` and so on; the category tokens never change. What each topic carries, and why there are four categories rather than the three the requirement names, is in [event-streaming.md](event-streaming.md#topic-catalogue).

**No dead-letter topic is ever granted to a subscriber**, so the four `.dlt` names are operator-only. All four **category** topics are grantable, which leaves exactly four grantable names.

`blnk.system` carries `ledger.created` alongside `system.error`, and `system.error` includes verbatim error text that can name internal detail. Grant it to a subscriber that needs `ledger.created`; withhold it from one that should not read operational error text — see [what granting `blnk.system` discloses](event-streaming.md#what-granting-blnksystem-discloses).

> Do not "tidy" the inventory to a different count. `model.EventCategory` routes events into exactly these four categories and `event_topics.go` composes exactly these eight names from them. A name provisioning does not create is a name the relay cannot publish to; a name it creates that no code writes to is dead weight in every environment.

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
  --entity-type users --entity-name "$KAFKA_USER"
```

— needs an authenticated connection, so **it cannot create the first credential**. That is a genuine chicken-and-egg problem, and injecting the credential while the storage is being formatted is the only resolution. Your instinct will be to fix an unauthenticable broker by running `kafka-configs` against it; that will not work, and the time spent discovering so is the reason this paragraph exists.

The ordering is therefore fixed: **bootstrap, then broker, then provisioning.** Per-subscriber principals are not created here — they are added once the broker is up and this credential can authenticate, by `scripts/kafka-provision.sh` locally and by `event_admin.go`'s `AlterUserScramCredentials` in production.

#### Two version floors, and both apply

**Feature floor — Kafka 3.5 (Confluent Platform 7.5.0).** The `--add-scram` flag of `kafka-storage format` was added there, and **earlier releases simply do not have it.** An older image rejects the flag, the format either fails or completes with no credential in the metadata log, and the broker then starts but can authenticate nobody — which surfaces much later as what looks like a wrong password. The script asserts this floor by asking the CLI whether `format` accepts the flag, which is the last point at which a `KAFKA_IMAGE` override below it can still be diagnosed as itself.

**Supported floor — Kafka 3.9.2 on the 3.x line.** The feature floor is the oldest release that *can* run this pipeline; it is not a release to deploy. Kafka 3.5 through 3.9.1 carry published Apache Kafka security advisories, so running anything in that range means running a known-vulnerable broker that merely happens to boot. Deploy an advisory-fixed release: **3.9.2 or later on 3.x**, or a correspondingly patched 4.x release.

The Compose stack and the Kubernetes StatefulSet both pin `apache/kafka:3.9.2` for exactly this reason. If you override `KAFKA_IMAGE`, override it **upward from the supported floor**, and check the [Apache Kafka CVE list](https://kafka.apache.org/cve-list) before you pick a tag rather than assuming anything above 3.5 is safe.

> **The floor is a compatibility minimum, not a production recommendation.** 3.5 is the version at which
> `--add-scram` exists; it says nothing about whether a release is still maintained. **In production, run
> a release that is currently listed among Apache Kafka's supported releases and is patched.** The
> project maintains roughly the three most recent minor lines, so what qualifies changes over time and a
> version written down here would go stale — check the current list rather than trusting a number in this
> document. The pinned `3.9.2` above is a **local development** pin chosen for `--add-scram`
> compatibility, and at the time of writing it has already moved to Apache's archived releases; do not
> carry it into production on the strength of appearing here.

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

1. Every category topic and its dead-letter sibling — the eight names above, derived from `KAFKA_TOPIC_PREFIX`.
2. The **producer** principal (`KAFKA_SASL_USER`, falling back to `KAFKA_PRODUCER_USER`, default `blnk-producer`) with `Write` and `Describe` on the Blnk-owned topics and nothing else.
3. One **sample subscriber** principal (`KAFKA_SAMPLE_SUBSCRIBER_USER`, default `blnk-sample-subscriber`) with `Read` and `Describe` on the grantable topics and `Read` on its own prefixed consumer-group namespace.

The producer principal is load-bearing rather than a nicety: the configuration **refuses to publish as the administrator**, so a deployment with an administrative pair and no producer pair fails to construct its event publisher and the server does not start. The worker is unaffected: it captures events into the outbox and publishes none, so it receives no broker credential at all and resolves to the no-op publisher regardless. The escape hatch is `KAFKA_ALLOW_ADMIN_PRODUCER=true`, which warns on every publisher construction and exists only for a deployment mid-upgrade.

It requires Step 1 to have already happened: it authenticates with the administrative credential, which can only have been created in the metadata log. Getting the order wrong does not produce a clear error of its own — it produces an authentication failure that reads like a wrong password, which is why the readiness wait names both causes when it times out.

**It is idempotent and exits 0 when nothing needs changing.** That is a hard requirement, not a nicety: the Compose `kafka-init` service is a one-shot with `restart: on-failure:3`, and the server and worker gate on it *completing*, so a non-zero exit on an already-provisioned broker would restart it until the cap and then fail the whole bring-up. Topic creation passes `--if-not-exists`, adding an existing ACL binding is a no-op, and **an existing SCRAM credential is left alone** — rotation is an explicit request through `KAFKA_ROTATE_PRODUCER_SECRET` or `KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET`, each needing a destination file. Rotating the producer secret out from under a running server and worker stops them authenticating, so a rotation with nowhere to deliver the new value refuses outright.

**Preservation depends on the probe, so an indeterminate probe stops the run.** Leaving a credential alone requires knowing that one exists, and the script asks the broker with a `--describe` on the user entity. That question has three answers, not two: the credential exists, the broker says it does not, or *the broker did not answer* — a restart in progress, a timeout, an administrative principal without `DescribeConfigs` on user entities. Only the second licenses minting a password. Reading the third as absence is what turns a routine topic-assurance re-run into a silent rotation: control falls into the generate-and-upsert arm, a working credential is replaced under no rotation flag, every consumer and publishing process holding the old password stops authenticating, and the run still reports success.

So an indeterminate probe **aborts**, naming the principal and the broker's own reason with credential-bearing lines removed. Topics assured earlier in the run are unaffected and no credential is written. The usual fix is to re-run once the broker is ready, or to grant the administrative principal `DescribeConfigs`. If you already intend to write a specific password, supply it — an explicit value needs no probe and is applied idempotently. For a broker that can *never* answer a describe on users, `KAFKA_ALLOW_SCRAM_PROBE_FAILURE=1` accepts the risk deliberately and the log states what may be overwritten.

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

Each of these variables is documented at its point of use in `.env.example`. **Two consumers read them
and they do not accept the same names:**

- **Blnk itself** accepts the bare `KAFKA_*` / `RELAY_*` name and its `BLNK_`-prefixed form, with the
  prefixed form winning when both are set. That dual acceptance is enumerated key by key in the
  configuration overlay rather than being automatic, so it holds for the documented keys and should not
  be assumed for one found elsewhere.
- **The provisioning scripts** (`scripts/kafka-bootstrap.sh`, `scripts/kafka-provision.sh`) read the
  **bare names only**. Export `KAFKA_TOPIC_PREFIX`, not `BLNK_KAFKA_TOPIC_PREFIX`, before running them.
  Setting only the prefixed form configures the service correctly and silently leaves the scripts on
  their defaults — which is how a provisioned topic set ends up not matching the prefix the service
  publishes to.

`WEBHOOK_DEPRECATION_START_DATE` is **not** an environment variable in either form; the window start is
derived as sunset minus 30 days. The provisioning script publishes its own interface, which is how `stack.sh` builds its passthrough list:

```bash
scripts/kafka-provision.sh --print-interface-host   # one variable name per line
```

### Adding a runtime SCRAM user

Once the broker is up and the bootstrap credential can authenticate, further principals are ordinary runtime operations:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/blnk-kafka/client-admin.properties \
  --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=$NEW_PASSWORD]" \
  --entity-type users --entity-name "$PRINCIPAL"
```

**Distinguish this clearly from the bootstrap credential, which cannot be created this way** — see Step 1. This command needs an authenticated connection, so it works only *because* a bootstrap credential already exists.

Two cautions. The value must come from the safe credential alphabet, because `--add-config` shares the no-escape-sequence grammar described above, so a comma or a bracket is silently truncated. And a password typed on a command line lands in shell history and in the process table of whatever host runs it — prefer a `--command-config`-style properties file or the API path below, and clear your history afterwards if you do type one.

For a **subscriber**, do not run it by hand. Use `POST /subscribers/{subscriber_id}/kafka-credentials`, which mints the credential, binds the ACLs, records the issuance and compensates a partial failure. Doing it by hand produces a principal with a credential and no bindings, which authenticates and can read nothing, and leaves no registry row for the reconciliation or the lag metrics to attribute.

### Verifying provisioning

```bash
# The eight topics, with their partition counts and replication factors.
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

Expect eight topic names, six partitions each and a replication factor of 1 locally.

## The ACL Model

### The grant, exactly

For each authorised topic, and one binding for the consumer group:

| Resource | Pattern type | Operation | Permission |
|----------|-------------|-----------|------------|
| Topic `<authorised topic>` | `LITERAL` | `Read` | `Allow` |
| Topic `<authorised topic>` | `LITERAL` | `Describe` | `Allow` |
| Group `blnk-sub-<subscriber_id>.` | `PREFIXED` | `Read` | `Allow` |

**Never `Write`. Never a wildcard topic pattern.** A subscriber consumes; it does not produce, and a wildcard would grant every topic the prefix could ever cover, including every dead-letter sibling and any future category the subscriber was never authorised for.

The **group binding is `PREFIXED` on purpose**. Granting the group *id* literally would pin the subscriber to exactly one consumer group; granting the *namespace* with a prefixed pattern reserves everything beneath it and nothing beside it, so a subscriber wanting a second group — a replay group beside its live one — picks another leaf with no administrative round trip.

Kafka's own implication rules make `Read` imply `Describe` on the same resource, and the group `Read` binding already implies the group `Describe` that `FindCoordinator` and `OffsetFetch` require. The topic `Describe` binding is therefore technically redundant and is requested anyway, so the grant is auditable from the binding list alone without the reader having to know the implication table. It costs one binding per topic.

All **four categories** may appear in a grant, `blnk.system` included — it carries `ledger.created`, and withholding the category would make that event unreachable to every subscriber. Grant it only to subscribers that consume ledger events: it also carries `system.error`, whose body carries Blnk's own error text verbatim. Every `<topic>.dlt` remains ungrantable, so no dead-letter name can ever appear in a subscriber's topic list.

### Foreign ACL bindings, and why issuance refuses on them

Blnk reads a subscriber principal's **complete** ACL grant before it issues a credential, and it classifies every binding it did not itself provision:

| Foreign binding | What Blnk does | Why |
|-----------------|----------------|-----|
| An **`Allow`** of any shape Blnk does not provision — a `Write`, a `PREFIXED` topic pattern, a cluster or transactional-id resource, or a binding whose permission type the broker did not state | **Refuses.** Credential issuance fails with `SUBSCRIBER_PROVISIONING_FAILED`, the SCRAM credential written moments earlier is revoked, and **no password is returned**. Granting further access (`PUT /subscribers/{id}`) refuses too. | The binding grants access outside the boundary the registry describes, by an amount Blnk cannot bound. Issuing a credential would return one whose `enforced_access` declares a boundary the broker is not enforcing — and nothing in the response would say so. |
| A **`Deny`** | **Proceeds**, and reports it. | A `Deny` subtracts from what the `Allow` bindings grant, so the subscriber reads *less* than its authorization describes. That cannot be an isolation failure, and refusing would block a principal that is more restricted than Blnk requires. |

**Blnk never deletes a foreign binding.** ACL deletion has no undo, and an operator's deliberate binding is not Blnk's to remove — so the remedy is yours:

```bash
# See exactly what the principal holds.
kafka-acls.sh --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_CLIENT_CONFIG"   --list --principal "User:blnk-sub-<subscriber_id>"

# Remove the offending binding, then retry issuance.
kafka-acls.sh --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_CLIENT_CONFIG"   --remove --allow-principal "User:blnk-sub-<subscriber_id>"   --operation Write --topic blnk.transactions
```

The refusal names the bindings it found, so you do not have to describe them again to know which ones they are.

**One deliberate asymmetry: NARROWING is never blocked.** `PruneSubscriberAccess` — the first step of an authorization change — proceeds even when a foreign `Allow` is present, because refusing a narrowing would leave the subscriber with *more* access than you just asked for. It removes the obsolete Blnk-owned bindings and logs that the effective access is still broader than the registry records.

A successful credential response reports `enforced_access.exclusive_grant_verified: true`, which states that Blnk **read** the grant and found no foreign `Allow`. A subscriber *read* (`GET /subscribers/{id}`) reports it as `false`: that path makes no broker round trip, so it has observed nothing.

### Reserved principal identities — the two rules that are checked at start-up

Issuing credentials performs a SCRAM **upsert**: an existing credential for the derived principal is *replaced* with a freshly generated password, which the response returns. Two configuration rules follow, and both are validated when configuration loads:

1. **Neither `KAFKA_SASL_USER` nor `KAFKA_SASL_ADMIN_USER` may begin with `blnk-sub-`.** That namespace is Blnk's own and every principal in it is *derived* from a subscriber identifier, so an identity inside it is reachable by an ordinary authorized API call. A deployment whose admin username were `blnk-sub-admin` could be made to rotate and hand out its Kafka superuser credential: register a subscriber whose identifier derives that principal, call the credential endpoint, read the password out of the response body.
2. **The two must be distinct from each other.** They exist to separate publishing from administration — one holds `Write` on every Blnk-owned topic, the other can mint credentials and grant ACLs — and collapsing them onto one identity gives every process that only publishes the ability to provision.

With `KAFKA_BROKERS` set, a violation is **fatal**: the process refuses to start, because the credential endpoint is reachable and the escalation is live. With no brokers configured it is a warning and start-up continues, since nothing reads these identities and no credential can be issued. Comparison is case-sensitive, because Kafka principals are.

The write path checks again immediately before the SCRAM upsert, so a reloaded configuration cannot slip past the start-up gate. That refusal is deliberately vague to the caller — `"its derived principal is reserved by this deployment"` — because confirming which principal name is privileged would answer a question an API caller should not be able to ask; the specific identity is in the server log.

### SCRAM parameters

- **Mechanism: `SCRAM-SHA-512`**, fixed rather than configurable. Kafka implements only SHA-256 and SHA-512, and keeping the administrative principal and every subscriber on one mechanism means the broker needs exactly one enabled.
- **Iterations: at least `4096`**, which is both Kafka's minimum and the default. A request below the minimum is raised to it with a warning rather than rejected; a request above it is honoured.
- **Pair SCRAM with TLS in production.** SCRAM authenticates; it does not encrypt. `SASL_PLAINTEXT` is acceptable for the loopback-bound local single-broker stack and nowhere else — both Kafka clients in Blnk **refuse to dial without TLS** unless `KAFKA_INSECURE_LOCAL_DEV` is explicitly set, which is what makes plaintext an opt-in rather than the accident of an unset variable. It is logged as a warning on every configuration load so its presence in a real deployment cannot go unnoticed. Configure `KAFKA_TLS_ENABLED` and the rest of the `KAFKA_TLS_*` block instead.

### The authorizer must be StandardAuthorizer

**In KRaft mode, `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` is required. Without it there is no authorization boundary at all — and the ACL admin APIs stop working rather than quietly accepting your grants.**

Two distinct things go wrong, and it is worth separating them because they have opposite symptoms.

**1. Access becomes unrestricted.** With no authorizer configured, Kafka permits every request. Any
authenticated principal can read every topic — including every dead-letter sibling — and can join any
consumer group. Nothing about a *data* request looks unusual: no warning is logged, and a consumer that
should have been refused simply succeeds. This is the dangerous half, and it is invisible from the data
path.

**2. ACL administration fails outright.** The APIs that manage ACLs are *not* silently permissive.
Kafka answers `SECURITY_DISABLED` to `CreateACLs`, `DeleteACLs` and `DescribeACLs` when no authorizer
is configured, because there is no authorizer to record or report bindings. So:

- `CreateACLs` does **not** succeed — it returns an error. Grants are neither stored nor pretended.
- `kafka-acls.sh --list` does **not** show phantom grants — it reports the same error.
- **Blnk's credential issuance fails closed.** Provisioning probes for an active authorizer *before*
  writing anything and refuses to issue a credential when the probe says the authorizer is absent, so a
  misconfigured cluster cannot hand out credentials that would be unrestricted. A probe that cannot get
  a definitive answer is treated as a refusal too: "I am not allowed to ask" is not "yes".

The practical consequence for an operator: **you will notice this when provisioning refuses, not when a
subscriber over-reads.** Fix the broker configuration; do not work around the refusal.

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
#    A DEAD-LETTER topic is the right probe: no subscriber is ever granted one, so an
#    authorization failure is the correct and expected outcome. Do NOT probe
#    blnk.system — all four category topics are grantable and the sample principal
#    holds them, so that read SUCCEEDS and proves nothing. Use a SUBSCRIBER's own
#    client properties file —
#    never the admin one, which is in super.users and is allowed everything by
#    design, so it would prove nothing. The path must be visible INSIDE the
#    container; mount the file or write it there first.
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --consumer.config /path/in/container/sample-subscriber.properties \
  --topic blnk.transactions.dlt --max-messages 1 --timeout-ms 10000
```

A `TopicAuthorizationException` is a **pass**. The topic exists — `scripts/kafka-provision.sh`
creates every dead-letter sibling — so an authorization failure cannot be confused with a missing
topic. Expect output of this shape:

```text
WARN  ... reported a recoverable issue ... : {blnk.transactions.dlt=TOPIC_AUTHORIZATION_FAILED}
ERROR ... Topic authorization failed for topics [blnk.transactions.dlt]
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [blnk.transactions.dlt]
```

Records, or an empty topic reported without an authorization error, mean the authorizer is not enforcing and must be fixed before the cluster is trusted with more than one subscriber.

Pair it with the positive control, or a refusal proves only that the credential is broken: the same principal reading a topic it **is** granted, under a group inside its own namespace, must succeed.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --consumer.config /path/in/container/sample-subscriber.properties \
  --topic blnk.transactions --group "blnk-sub-$SUBSCRIBER_ID.default" \
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

There is no topic per tenant, per subscriber or per ledger — looking for one is looking for something
that does not exist. Every subscriber reads from the **same four grantable category topics**, and each
one is granted **only the subset it was authorised for**: its `authorized_topics`. Two subscribers can
therefore hold entirely different grants over one shared inventory, and no subscriber is ever granted a
`.dlt` topic.

Isolation is delivered by three things and three things only:

1. **The principal** — a distinct SASL/SCRAM identity per subscriber.
2. **The ACL bindings** — literal `Read`/`Describe` on the authorised topics, and nothing beyond them.
3. **The consumer group namespace** — a prefixed `Read` grant that reserves the subscriber's own group space and no one else's.

##### The operational consequence: a topic grant is a grant over every tenant's events on it

This follows directly from the two facts above and is the sentence to have in mind when you approve a grant.

A category topic carries **every** event of its category for the whole deployment. `blnk.transactions` holds every ledger's transaction events; `blnk.balances`, `blnk.identities` and `blnk.system` do the same for theirs. So granting `blnk.transactions` to a subscriber grants it read access to **every ledger's** transaction events and to the events of **every other subscriber** of that topic. Nothing narrows that: not the `partition_key_prefix`, not the consumer group, not the number of subscribers sharing the topic.

Approve a topic grant on that basis. The question to ask is not "which slice of this topic does the subscriber need?" — there is no mechanism that answers it — but **"is this subscriber trusted with the whole category?"** If the answer is no, the grant is the wrong instrument:

| What you need | The enforceable instrument | What it costs |
|---|---|---|
| A subscriber must not see a *category* | Omit that topic from `authorized_topics`. The broker refuses it outright. | Nothing. This is the intended mechanism. |
| A subscriber must not see *another tenant's records within a category* | **Separate the deployments.** A distinct Blnk deployment, with its own broker or its own topic namespace via `KAFKA_TOPIC_PREFIX`, is the only boundary that holds. | A second deployment to operate. |
| A subscriber should *process* only its own records, and is trusted with the rest | Record a `partition_key_prefix` and rely on the consumer to filter — see immediately below. | It is not a security boundary. Read the next section in full. |

Do not reach for a per-subscriber topic and a filtering republisher as a middle option; both are ruled out by design, and the reasons are in [Why not enforce it at the broker?](#the-partition-key-prefix-is-a-consumer-side-filtering-contract) below.

#### The partition-key prefix is a consumer-side filtering contract

A subscriber row may carry a `partition_key_prefix`. It is the third dimension of the access model, and it is the one the broker does not evaluate — so it is worth knowing exactly what it does and does not do.

**Kafka's authorizer has no message-key dimension.** There is no ACL that restricts a consumer to a slice of a topic by key: a subscriber granted a topic can read every record on it, whatever the keys are. A recorded prefix is therefore **enforced by the consumer**, over records the broker has already permitted it to read.

Credential issuance says so rather than guessing. The response carries the recorded prefix inside `enforced_access`, together with the two fields that qualify it:

```json
"enforced_access": {
  "enforced_by": ["topic", "consumer_group"],
  "not_enforced_by": ["partition_key"],
  "topics": ["blnk.transactions"],
  "consumer_group_namespace": "blnk-sub-sub_9f8d3c214b7a5e6f.",
  "partition_key_prefix_enforced": false,
  "partition_key_prefix": "ldg_9f1c8a72",
  "client_side_key_filtering_required": true,
  "partition_key_prefix_enforced_by": "consumer_side",
  "exclusive_grant_verified": true,
  "guidance": "Kafka authorises whole topics and consumer groups and has no message-key dimension, so a partition-key prefix is never enforced: a granted topic is readable in full, including records written for other ledgers and other subscribers. To confine a subscriber, narrow its authorized_topics, which the broker does enforce, or isolate the data at the deployment boundary."
}
```

`client_side_key_filtering_required` is the field a subscriber's client branches on: it is `true` exactly when a prefix is recorded, and it names the consumer as the component that applies the narrowing. `guidance` carries the remedy in the same object as the limitation, so an integrator who has just read that the key is not enforced also reads what to narrow instead; it is prose for a human and its wording may change, so nothing should match on it.

`enforced_by` and `not_enforced_by` together enumerate every dimension this API names, and they are disjoint — so the message-key dimension is stated as unenforced rather than left to be deduced from its absence, and a dimension moving between the two lists is a visible contract change. `not_enforced_by` carries `partition_key` for **every** subscriber, with or without a prefix recorded, because it describes what the broker can evaluate and not what the row configured. `partition_key_prefix_enforced` is **always** `false` and `partition_key_prefix_enforced_by` is `consumer_side` whenever a prefix is recorded, `none` when one is not. Issuance also logs a WARNING naming the prefix, the enforcement point and the topics the credential really covers, so the moment a key-scoped principal comes into existence is visible in the operator log.

Recording a prefix in the other order — onto a subscriber that **already** holds a credential — logs its own WARNING, carrying the same fields plus `credential_issued_at`. Both orders are disclosed because only one of them is reported by anything else: a prefix binds no ACL, so it produces no grant churn for the update's own log line to mention, and without this warning a live principal would quietly come to sit under a row describing something narrower than it is. The issuance instant is there to tell a row you have just provisioned apart from one whose principal has been reading whole topics for months.

**Do not build a tenancy boundary on the prefix.** If a subscriber must be unable to *reach* records outside its scope, the enforceable remedy is the topic grant: narrow `authorized_topics`, or publish the authorization domain to a topic of its own. That is a real ACL and the broker refuses everything outside it.

> **This used to be a refusal, and it was wrong.** `POST /subscribers/:subscriber_id/kafka-credentials` answered `409 SUBSCRIBER_ISOLATION_UNENFORCEABLE` for any row recording a prefix, recording one on a provisioned subscriber was refused too, and a `CHECK` constraint made the combination unrepresentable. The concern was legitimate — a registry row must not be readable as a boundary the broker keeps — but a subscriber that is refused a credential consumes **nothing**, which is the absence of a boundary rather than a narrower one. The refusals are gone, the constraint is dropped by `sql/1781248930.sql`, and the error code no longer exists. If your database predates that migration, a `PATCH` recording a prefix on a provisioned subscriber returns `500` naming the migration to apply.
>
> One caveat if you were running the refusal: `sql/1781248920.sql` **cleared** `partition_key_prefix` on every row that held it beside a credential, and those values were not retained anywhere. Re-record them with `PUT /subscribers/{id}` — the registry has no `PATCH` route, and an unregistered verb is answered by the router rather than the handler.


`partition_key_prefix` and `partition_key_prefix_enforced` live in the same object deliberately: the scope cannot be read without the statement that the broker does not keep it. **The subscriber's consumer must discard records whose key does not carry the prefix**, because nothing upstream of the consumer discards them. When no prefix is recorded, `partition_key_prefix` is **omitted** from the body and `partition_key_prefix_enforced_by` reads `none`: the topic grant is then the whole boundary and there is nothing for a consumer to filter. `partition_key_prefix_enforced` stays `false` in that case too — it is never `true`, because the broker has no message-key dimension to enforce with whether or not a prefix was asked for, and a `true` there would be the one wrong answer that is a disclosure bug. There is no `all-keys` sentinel on the wire either: this field is what a consumer compares record keys against, so any stand-in for "no restriction" would be a filter matching nothing, and a client applying it would silently discard its entire stream.

This is the same posture the event contract takes for duplicate suppression: delivery is at-least-once, so `event_id` idempotency is a documented subscriber obligation rather than a broker guarantee. A key scope is that pattern applied to authorization.

**Why not enforce it at the broker?** The two designs that could are both ruled out. A topic per key scope contradicts the model's own first rule — there are no per-tenant topics, which is what makes a new subscriber cost no new topics. An interposed filtering gateway that re-emits already-isolated streams is subscriber-side consumer machinery, which Blnk does not build. If broker-enforced record-level isolation is a hard requirement for your deployment, it needs one of those two designs and cannot come from this column.

##### Accepted deviation: "ACLs scoped to the partition-key prefix" is not delivered, and will not be

The subscriber access model was specified as *"ACLs scoped to its authorized topics, consumer group, and partition-key prefix"*. **Two of those three are delivered as ACLs. The third is not, and it is recorded here as an accepted deviation rather than as outstanding work**, because it is not implementable — a reader comparing the specification against the running system should find the discrepancy explained here instead of assuming a gap that a later release will close.

**What was verified.** A subscriber was registered with `partition_key_prefix` set, issued a credential, and used to read the granted topic from the first offset. It read tens of thousands of records, none of whose keys carried the prefix. That is the designed behaviour of the grant, and every response the subscriber received said so.

**Why no ACL can do better.** Kafka's authorizer evaluates a fixed set of resource types — cluster, topic, group, transactional id, delegation token, user. **A message key is not among them**, so there is no binding, pattern type or permission that narrows a principal to a subset of a topic's records. This is a property of Kafka, not of Blnk's provisioning code, and it holds for every Kafka release and every authorizer implementation that follows the standard resource model.

**Why the two workarounds are worse, not merely unbuilt.**

- *A topic per subscriber or per key scope* would satisfy the letter of the requirement and contradict its own first sentence — the access model exists to avoid per-tenant topics, which is what keeps a new subscriber from costing new topics, new partitions and new provisioning state. It also multiplies the write path by the number of subscribers.
- *A Blnk-owned filtering consumer that republishes in-prefix records* is subscriber-side consumer machinery, which Blnk deliberately does not build — the same boundary that leaves subscriber-side dead-lettering to subscribers. It would also make Blnk the availability and ordering bottleneck for every stream it re-emitted, and would need its own outbox to avoid being a new loss window.
- *Binding a `PREFIXED` topic pattern instead of a `LITERAL` one* deserves a specific warning, because it looks like the answer and is the most dangerous option on the list: topic-name prefixes and message-key prefixes are unrelated, so such a binding would **widen** the grant to every topic sharing the name prefix while appearing to narrow it. `NewSubscriberProvisioningRequest` refuses to construct it for exactly this reason.

**What is delivered instead, and it is not silence.** The prefix is accepted, recorded, and returned to the subscriber inside the same object that states it is unenforced, names the consumer as the enforcing party, carries `client_side_key_filtering_required`, and carries the remedy in `guidance`. Issuance logs a WARNING naming the prefix and the topics the credential really covers, and recording a prefix onto an already-provisioned subscriber logs its own. The obligation is documented for subscribers in [event-streaming.md](event-streaming.md#your-partition_key_prefix-is-yours-to-enforce--this-is-a-contract-not-a-hint) in the same voice as the `event_id` deduplication obligation.

**What an operator must do about it.** Treat the topic grant as the isolation boundary — see [the operational consequence](#the-operational-consequence-a-topic-grant-is-a-grant-over-every-tenants-events-on-it) above. Where records must be unreachable rather than filtered, separate the deployments. Do not represent the prefix to a subscriber as an isolation guarantee in a contract, a security questionnaire or an audit response: the API does not, and neither should the surrounding paperwork.

> **Changed behaviour.** Issuance used to **refuse** any row carrying a prefix with `409 SUBSCRIBER_ISOLATION_UNENFORCEABLE`, and the schema forbade the prefix alongside a credential. Both are gone: the refusal implemented no part of the third scope, it withheld the credential instead, so a key-scoped subscriber could not consume at all. `sql/1781249138.sql` drops `event_subscribers_key_scope_chk`, and the error code is retired rather than left unraisable. Recording a prefix on an already-provisioned subscriber is now accepted and **takes effect on the next issuance** — re-issue to hand the consumer its new boundary; the credential already in the field still carries the old one.

Note also that the prefix is **recorded rather than derived** — unlike the principal and the group. It could not be derived: a Kafka message key on Blnk's topics is the outbox row's stored partition key, which is the **ledger id** wherever the event's subject belongs to a ledger (see [event-streaming.md](event-streaming.md#the-key-is-the-ledger-id-wherever-a-ledger-exists)), and a prefix computed from the subscriber's own identifier bears no relation to any ledger id — so a subscriber filtering on it would silently discard its entire stream. A prefix is therefore only meaningful when the operator sets it from the ledger identifiers that subscriber is entitled to.

> This paragraph appeared twice, once here and once earlier in the section, with the two copies giving slightly different accounts of the same fact. The earlier copy has been removed.
### Issuing credentials

The response body contains a secret that exists nowhere else, so write it to a private file and never to a terminal. `--output` keeps it off stdout; `umask 077` keeps the file private from the moment it is created.

```bash
umask 077
resp="$(mktemp)"                       # 0600 at creation, in your private temp dir
trap 'rm -f "$resp"' EXIT INT TERM     # disposed even on failure or interrupt

curl -sS -X POST "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f/kafka-credentials" \
  --config "$BLNK_CURL_CONFIG" \
  -o "$resp"

# Move the one-time secret straight into your secret manager. It is never echoed.
jq -re .password < "$resp" | vault kv put -mount=secret blnk/sub_9f8d3c214b7a5e6f password=-

# The non-secret half is safe to read and is what the subscriber configures against.
jq '{brokers, consumer_group_id, authorized_topics, username, mechanism, enforced_access}' < "$resp"
```

**Never let the response body reach a terminal, a log or a shell history.** `-o` keeps it out of stdout;
`umask 077` makes the file unreadable to anyone else from the instant it exists; the `trap` disposes of it
whether the command succeeds, fails or is interrupted. Blnk cannot re-issue the same secret, so a leaked
one has to be rotated, and a lost one has to be replaced.

The `200` response carries everything the subscriber needs to start consuming, and nothing else:

| Field | Meaning |
|-------|---------|
| `brokers` | The subscriber-facing bootstrap list, from `KAFKA_SUBSCRIBER_BROKERS`. |
| `broker_endpoint` | The same list as one connection string, for convenience. |
| `authorized_topics` | The topics the credential may `Read` and `Describe` — the authorised subset of the four grantable category topics. **Never a `.dlt` name.** |
| `consumer_group_id` | The derived default group, `blnk-sub-<subscriber_id>.default`. |
| `enforced_access` | What the broker actually enforces, plus the recorded `partition_key_prefix` and its enforcement point. Assembled by the model, never by the handler, and it states outright that key filtering is **not** broker-enforced. |
| `username` | The derived principal, `blnk-sub-<subscriber_id>`. |
| `password` | **The plaintext, returned only on this response and never again.** Hand it to the subscriber and keep no copy Blnk can be asked for. |
| `mechanism` | `SCRAM-SHA-512`. |
| `issued_at` | The issuance instant, which is also what is persisted alongside the non-reversible reference. |

A **successful** provisioning completes within five seconds: the ceiling is applied by the service and
again at the HTTP boundary, so an expiry is *answered* rather than waited out and every request ends
with a code that says whether retrying is sensible.

**A failing provisioning can take considerably longer, and you should size timeouts for it.** When
provisioning fails partway, Blnk compensates synchronously before answering — revoking the credential it
had already written at the broker — on **fresh budgets that start after the primary five seconds have
already expired**: up to 5 seconds for the registry write and up to 10 for the broker call. Worst case
is therefore on the order of **20 seconds**, not 5. That is a deliberate trade: the alternative is
leaving a live SASL credential at the broker for a principal the registry records no issuance for, which
then has to be found and revoked by hand (see
[SubscriberRevocationOutstanding](#subscriberrevocationoutstanding)).

The endpoint reports `KAFKA_SUBSCRIBER_BROKERS`, which is a **different list** from `KAFKA_BROKERS` and does not fall back to it. `KAFKA_BROKERS` is what Blnk itself dials and is an address inside the deployment; a broker answers every client with the *advertised* address of the listener the connection arrived on, so handing a subscriber an internal address produces an unexplained connection timeout in the subscriber's logs days later, and publishes your internal topology for good measure. When `KAFKA_SUBSCRIBER_BROKERS` is empty, issuance is refused with `SUBSCRIBER_BROKERS_NOT_CONFIGURED` rather than falling back. A deployment whose subscribers really are in-cluster sets it to the same value as `KAFKA_BROKERS`, which is one line and makes the claim explicit.

The refusals worth recognising:

| Status | `error_detail.code` | What to do |
|--------|--------------------|------------|
| `400` | `GEN_MISSING_PARAMETER` | No identifier in the route. |
| `400` | `GEN_VALIDATION_ERROR` | No Kafka identity can be derived from that identifier. Fix the id. |
| `403` | `AUTH_MASTER_KEY_REQUIRED` | Use the master key. |
| `403` | `SUBSCRIBER_INSECURE_TRANSPORT` | The channel is not established as confidential — see [the transport contract](#the-transport-contract-this-endpoint-requires) directly below. |
| `404` | `SUBSCRIBER_NOT_FOUND` | Register the subscriber first with `POST /subscribers`. |
| `409` | `SUBSCRIBER_GRANT_EMPTY` | The subscriber is authorised for no topics. Set `authorized_topics`. |
| `409` | `GEN_CONFLICT` | A **concurrent issuance for the same subscriber superseded this one.** Another call won the race, so this request's credential is not the live one. Do not retry blindly: re-read the subscriber to see the issuance that landed, and re-issue only if you still need a credential of your own — a fresh issuance replaces whatever the other call created. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | No broker configured, or the broker is down. |
| `503` | `SUBSCRIBER_BROKERS_NOT_CONFIGURED` | Set `KAFKA_SUBSCRIBER_BROKERS`. |
| `503` | `SUBSCRIBER_PROVISIONING_FAILED` | The broker refused the credential or its bindings. Check the admin credential and the authorizer. |
| `504` | `SUBSCRIBER_PROVISIONING_TIMEOUT` | The registry ran out of the issuance budget. Retry. |

### The five-second bound covers the cleanup too

The five seconds above is a bound on **the whole request**, not on its forward path. That distinction is worth stating because it is easy to build the other thing by accident, and the other thing is what an operator notices.

Issuance touches the broker up to four times and the registry twice, and a failure part-way through leaves a **live SASL credential with ACL bindings that no registry row records** — unfindable through the API, so unrevokable by any means short of the Kafka CLI. So a failure is compensated before the call returns: the credential is deleted at the broker, the registry's credential record is cleared, and the provisioning fence is released.

That compensation has to survive the caller's cancellation, because *the deadline expiring is the commonest reason it is needed at all* — a cleanup running on the spent budget returns immediately and leaves exactly the residue it exists to remove. Detaching it from cancellation is what makes it run. But detaching from cancellation also detaches from the deadline, and a cleanup left with no bound will take one of its own.

So the deadline is **held onto deliberately and re-imposed on the cleanup**:

| | Bounded by | Why |
|---|---|---|
| Forward path — lookup, fence, four broker round trips, the issuance record | The deadline **minus 1.25 s** | The 1.25 s is what the cleanup needs: two broker round trips and up to two local writes. Held back rather than borrowed, so the one failure that most needs compensating — the deadline expiring — has time left to compensate in. |
| Compensation — revoke, clear, release fence | The **same** deadline | One window per request, shared by every level of cleanup nested inside it. A broker-side cleanup reached through a registry-side one inherits the instant rather than starting a second window. |

Two consequences to plan around:

- **A broker that needs more than 3.75 seconds for four round trips now fails where it would previously have succeeded at up to 5.** That is the deliberate cost of the reserve. It is not a tuning knob to widen; it is a broker to fix, and the `503` tells you to retry once you have.
- **The one case that can overrun the bound is a step that ignores its own context** — a driver call that does not honour cancellation, a blocking syscall, a stop-the-world pause. No deadline arithmetic reaches a call that never looks at its deadline. When that happens the compensation still runs, bounded to one 1.25-second window and no more, because the alternative is leaving the unaccounted credential behind. If you see issuance answering at roughly 6.25 seconds, that is this case, and the thing to investigate is the step that overran — not the cleanup.

If compensation itself cannot finish inside its window — a broker that is hanging rather than refusing — it is **abandoned and logged at ERROR with the principal named**. That is the manual-revocation case: find the principal in the log, delete it with `kafka-configs.sh --alter --delete-config 'SCRAM-SHA-512' --entity-type users --entity-name <principal>`, and remove its bindings as [The ACL Model](#the-acl-model) describes. The log names the principal precisely so this is possible; see [Reading the logs](#reading-the-logs-redacted-is-the-log-not-the-failure) for what else it will and will not tell you.

#### `503` and `504` are different failures, and the one you will actually see is `503`

Both appear in the refusal table above and it is worth knowing which to expect, because reaching for the wrong one wastes an incident.

**`503 SUBSCRIBER_PROVISIONING_FAILED` is the broker saying no, or not being there.** Every inducible broker fault produces it, and produces it fast — a refused connection or a broker that accepts and never answers is reported in tens of milliseconds, not after the budget expires, because the failure is observed rather than waited for. If issuance is failing, this is almost certainly the code, and the thing to check is the admin credential, the authorizer, and whether `KAFKA_BROKERS` names a reachable listener.

**`504 SUBSCRIBER_PROVISIONING_TIMEOUT` is Blnk saying it ran out of time**, which needs the work to be genuinely *slow* rather than broken — a broker answering but pathologically late, a registry write blocked behind a long-running transaction, or a host under enough pressure that the process does not get scheduled. It is rare by construction: the forward path is bounded well under the ceiling and a broker that is merely unreachable fails long before the clock runs out. Treat a `504` as a latency investigation, not an authorization one, and note that the request may have taken longer than five seconds to answer — see the bound table above for why.

Both are safe to retry, and neither leaves a live credential behind: whichever code you receive, the compensation described above has already run or has been logged at ERROR with the principal named for manual revocation. A `504` in particular does **not** mean "it may have half-worked" — that is the whole point of holding the reserve back.

### Rate limiting the credential endpoint

`POST /subscribers/{id}/kafka-credentials` is master-key-only, so rate limiting is not the control that keeps strangers out — the key is. It is worth setting anyway, for two reasons that have nothing to do with authentication.

**Issuance is destructive to the previous secret.** A loop that calls the endpoint repeatedly rotates the credential every time, and every rotation breaks whatever consumer is holding the old password. A limit turns a runaway script or a retry storm in an onboarding job into rejected requests rather than a rotation storm.

**The default is permissive, and deliberately so.** With neither variable set, Blnk applies **2000 requests per second with a burst of 4000, per client**, and says so at start-up. That is sized for the ledger's transaction endpoints, not for an endpoint an operator calls by hand a few times a week:

| Variable | What it sets | Default |
|---|---|---|
| `BLNK_RATE_LIMIT_RPS` | Sustained requests per second, per client | `2000` |
| `BLNK_RATE_LIMIT_BURST` | Burst allowance, per client | `4000` (twice the rps when only the rps is set) |
| `BLNK_SERVER_TRUSTED_PROXIES` | CIDR blocks or bare IPs whose forwarded-for header is believed | *(unset — forwarded headers are ignored)* |

**`BLNK_SERVER_TRUSTED_PROXIES` is the one that decides whether the limit means anything.** Until it is set, every request behind a load balancer keys to the *proxy's* address, so all your callers share one bucket: one noisy client can exhaust the allowance for every other, and a per-client limit is not what you have. Set it to the proxy's address range and each caller is limited separately.

The limiter is global middleware, so lowering it lowers it for the ledger endpoints too. If you want a tight bound on issuance specifically, put it at the ingress in front of Blnk — this endpoint is administrative and belongs behind a narrower path there anyway.

### A lost credential is re-issued, never recovered

**There is no procedure in this runbook for looking up a subscriber's password, because none exists.** Only a non-reversible reference and the issuance instant are persisted, and `blnk.event_subscribers` has no column able to hold the secret. No endpoint returns it on a read, and re-calling the issuance endpoint mints a **new** credential rather than returning the old one.

So when a subscriber loses its password, the answer is to re-issue — to a protected file, exactly as the first issuance did:

```bash
umask 077
curl -sS -X POST "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f/kafka-credentials" \
  -K "$BLNK_CURL"
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
curl -sS "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f" --config "$BLNK_CURL_CONFIG"
```

To confirm a credential **authenticates**, have the subscriber connect, or use a client properties file the subscriber already holds. Do not reconstruct one from a stored value — there is nothing stored to reconstruct it from.

### The legacy `webhook_url` on a subscriber row, and the one check that is yours

A subscriber row may carry a `webhook_url`, recorded during the migration window so an operator can tell which subscribers are still to be moved and which URL each is coming off. **Dual-run delivery does not read it** — the legacy leg goes to the single global webhook destination, which is the entire webhook subscription surface Blnk has ever had. Treating this column as a delivery sink would silently deliver nothing.

Registration validates what it can: a non-HTTPS scheme, and any **visibly internal** destination — loopback, link-local including the `169.254.169.254` cloud metadata address, private ranges and unqualified hostnames — is refused outright, because those are exactly what a server-side request forgery aims at.

> **One residual risk is not, and cannot be, caught at validation time, and it is recorded here as an obligation on whoever wires delivery.** A hostname that *resolves* to an internal address passes every check a validator running hours earlier can make — that is DNS rebinding, and the only place to catch it is at connect time, in the sender. So if you build anything that dials a URL out of this column, resolve the host and re-check the resulting address **immediately before connecting**, and refuse a private, loopback or link-local result then. Reviewing the stored strings is not a substitute; the string can be blameless and the address it resolves to at connect time internal.

#### Where the URL is readable, and where it deliberately is not

`webhook_url` is exposed by **one** route: `GET /subscribers/:subscriber_id/webhook-subscription`, which is fronted by the sunset guard and stops disclosing it — `410 Gone` — at the retirement instant.

The general subscriber routes do not carry it. `GET /subscribers` and `GET /subscribers/:subscriber_id` report `migrated_at` and no URL, and `POST`/`PUT /subscribers` accept none. Those routes are not deprecated and not guarded, so echoing the legacy address through them would keep it readable after the guarded route had begun refusing — two reads disagreeing about whether the legacy surface still exists.

So a **bulk** audit runs against the database, and a **per-subscriber** one against the deprecated route:

```bash
# Every subscriber still to be moved, in one query. This is the operator's audit.
$BLNK_PSQL -c "
  SELECT subscriber_id, webhook_url, created_at
  FROM blnk.event_subscribers
  WHERE webhook_url IS NOT NULL
  ORDER BY created_at;"

# Migration PROGRESS needs no database access — migrated_at is on the API. include_count is
# required: total_count is omitted from the envelope unless you ask for it.
#
# This counts subscribers with NO migration record, which is not the same as subscribers still
# to be moved: a subscriber onboarded after the cutover never had an endpoint and is counted
# here with nothing to migrate. For outstanding WORK, use the webhook_url query above.
curl -sS "$BLNK_API/subscribers?limit=100&include_count=true" --config "$BLNK_CURL_CONFIG" \
| jq -r '.total_count as $n | .data
         | map(select(.migrated_at == null)) as $unrecorded
         | "\($unrecorded | length) of \($n) subscribers carry no migration record",
           ($unrecorded[] | .subscriber_id)'

# One subscriber's recorded address, through the guarded route.
curl -sS "$BLNK_API/subscribers/$SUBSCRIBER_ID/webhook-subscription" \
  --config "$BLNK_CURL_CONFIG" | jq '{subscriber_id, webhook_url, migrated_at}'
```

A row with a `webhook_url` is a subscriber still to be moved, and the API will not let it also carry a `migrated_at`: recording an address clears the migration, completing the migration clears the address, and stamping a migration on a row that still holds an address is **refused** — each of the three in one statement. So for any row written through this API the two columns cannot disagree.

**That guarantee is application-enforced, not database-enforced, and the difference decides which query is a correct audit.** There is no CHECK constraint forbidding the pair, deliberately: the retention purge below is *defined* on `webhook_url IS NOT NULL AND migrated_at IS NOT NULL`, so a constraint would leave it with nothing it could ever match, and it would fail `blnk migrate up` on any database already holding such a row. A `psql` session, a bulk import or a restored backup can therefore still produce one — and if you find one, it came from outside the API and the purge is what clears it.

**`webhook_url IS NOT NULL` and `migrated_at IS NULL` are therefore NOT the same population**, and only one of them answers "who is still to be moved":

| Question | Query |
|---|---|
| Who is still to be moved? | `webhook_url IS NOT NULL` — the only column that says "this subscriber receives HTTP pushes today" |
| Who has been recorded as migrated? | `migrated_at IS NOT NULL` |

`migrated_at IS NULL` is **not** a synonym for the first. It counts every subscriber onboarded after the cutover as well — those never had an endpoint and have nothing to migrate from — so it **over-reports** the outstanding work, and on a row that came from outside the API and holds both values it **under-reports**, by excluding a subscriber whose endpoint is still recorded. Use it to answer what it actually measures: which subscribers carry no migration record.

The timeline and the sunset behaviour are in [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md).

## Dead-Letter Triage and Replay

### Before you start: what already happened

An event reaches a dead-letter topic for one of **two** reasons: a spent retry budget (`terminal_reason=budget_spent`), or a permanent failure recognised on the attempt it happened (`terminal_reason=permanent_failure`), which deliberately does not spend the budget at all. **The reason is a field on the relay's log line, not a field in `failure_metadata`** — the metadata carries exactly five fields and `terminal_reason` is not among them, so on the message itself the reason has to be inferred from `attempt_count`. Read [Two ways an event becomes terminal](#two-ways-an-event-becomes-terminal) first — **it decides what you do next**, and the retry schedule below only describes the first of the two.

### Two ways an event becomes terminal

**`attempts=1` on a dead-lettered event is not a bug, and it is the commonest case.** A permanent failure ends the event's life on the attempt that discovered it, with the retry budget deliberately unspent. An operator who assumes every dead letter followed budget exhaustion reads `attempts=1` as evidence of a broken relay and goes looking for a broker outage that never happened.

The two paths are separable by a field match on `terminal_reason`, not by parsing a message:

| `terminal_reason` | Typical `attempts` | What happened | What it means for you |
|-------------------|--------------------|---------------|-----------------------|
| `budget_spent` | `max_attempts` (5 at the defaults) | Every permitted attempt was made and each failed transiently. | The broker or the network was unavailable across the whole schedule. Five attempts have four gaps between them, so that is at least the 1s + 2s + 4s + 8s = **15 seconds** of waiting, plus the time the five attempts themselves took; `first_attempted_at` and `last_attempted_at` on the row give the actual span. **Replay is likely to succeed** once the dependency is back. Fix the dependency, then replay. |
| `permanent_failure` | `1` — but any attempt can be the one that discovers it | The publisher classified the failure as one no further attempt could change. | **Replay will fail identically until the cause is fixed.** Fix the cause first — the grant, the topic, the payload, the size — and only then replay. |

The log lines end in different clauses so the sentence matches what happened: `after exhausting its retry budget` versus `after a permanent publish failure, with its retry budget deliberately unspent`. Both carry `terminal_reason`, `dlt_topic`, `attempts` and `error` as fields, so the split is a field match:

```bash
# Everything dead-lettered because a dependency was down across the whole schedule.
docker compose logs server | grep '"terminal_reason":"budget_spent"'

# Everything dead-lettered because it can never succeed as it stands.
docker compose logs server | grep '"terminal_reason":"permanent_failure"'
```

`terminal_reason` is also on the two failure lines that fire when the dead-letter write itself does not complete — the row stays `failed` and stays in the inventory — so a terminal event is attributable even when it never reached its `.dlt` sibling.

**On the message, not in the log, the reason has to be inferred.** `failure_metadata` carries five fields and `terminal_reason` is not one of them, so a subscriber consuming a dead-letter topic reads `attempt_count`: equal to `RELAY_MAX_RETRY_ATTEMPTS` means the budget was spent, anything less means a permanent failure ended it early.

**What counts as permanent.** The classification is affirmative and conservative, and the direction matters in both places:

- **Transient**, so retried: anything the Kafka client itself reports as temporary or as a timeout; context deadline or cancellation, which says nothing about the broker's health; and the raw connection signatures of a broker restart — connection refused, connection reset, broken pipe, unexpected EOF. For a multi-record write error, **one** transient member makes the whole write transient.
- **Permanent**, so dead-lettered on the spot: an unauthorised principal, a destination outside the topic catalogue, bytes that will never serialise, a record over the broker's size limit — **and anything unrecognised**. Unrecognised failures are treated as permanent on purpose: retrying something that can never succeed, without bound, is worse than a dead-letter entry an operator can see and replay.
- **"No verdict" reads as NOT permanent.** The predicate requires an error, the failed status, *and* an explicit non-transient classification. The publisher is an interface seam, so a result nobody populated must fall through to the budgeted retry rather than ending an event's life on its first attempt.

**Which decision belongs where, and why it is split.** The permanence decision belongs to the publisher, because only it has seen the broker's error. The budget decision belongs to the DATABASE, inside `MarkEventFailed`'s `UPDATE`, because two relay instances racing on one row must not both conclude they were the last attempt. So a transient failure on the final permitted attempt is also terminal — as `budget_spent`, decided by the row, not by the publisher.

**Failure metadata is written the same way for both**, so nothing about triage or replay differs structurally: the original topic, the error reason, the attempt count, and the first- and last-attempted timestamps. On a permanent failure the attempt count is simply lower and the two timestamps are close together or identical. **A one-attempt entry with identical timestamps is a complete, valid record**, not a truncated one.

At the defaults the retry schedule is **five publish attempts and the delay sequence 1s, 2s, 4s, 8s, 16s**:

| Setting | Variable | Default |
|---------|---------|---------|
| Maximum publish attempts | `RELAY_MAX_RETRY_ATTEMPTS` | `5` |
| Base delay | `RELAY_RETRY_BASE_BACKOFF_MS` | `1000` |
| Delay ceiling | `RELAY_RETRY_MAX_BACKOFF_MS` | `30000` |

The delay doubles after each failure, and **every one of the five delays is computed and stamped on the row's `next_attempt_at`** — including the fifth, which appears on the exhausting attempt's log line as `retry_after=16s` alongside `retry_after_waited=false`. What differs is how many of them a retried event *waits*: five attempts have four gaps between them, so the waits are 1s, 2s, 4s and 8s — **15 seconds of backoff in total** — and the fifth delay is recorded rather than waited, because the budget is spent and the event is dead-lettered instead of published a sixth time. Raising `RELAY_MAX_RETRY_ATTEMPTS` is what turns it into a wait as well.

**The 30-second ceiling is never reached at the defaults**, since the largest scheduled delay is 16 seconds. The ceiling engages only if the base delay or the attempt count is raised. `RELAY_MAX_RETRY_ATTEMPTS` is additionally **clamped to 5**, with a loud warning, because the attempt number is an exported metric label and a value of 5000 would mint 5000 label values.

**Or the failure was permanent**, and then none of the above happened. A payload the broker refuses whatever is done to it, an unknown topic, an authorization refusal — no further attempt can change the answer, so the relay stops on the attempt it happened and leaves the remaining budget deliberately unspent. That is usually the **first** attempt, because a condition of this kind is normally present from the start rather than arriving mid-sequence. So a dead-lettered event is **not** by itself evidence of a broker outage, and `attempt_count: 1` is a perfectly ordinary reading.

**Establish which one from the log field, not from the count.** The dead-letter log line carries `terminal_reason`, whose value is exactly `budget_spent` or `permanent_failure`, so the two are separable with a field match rather than by reading a sentence:

```bash
docker compose logs server | grep '"terminal_reason":"permanent_failure"'
```

The remedies diverge completely. A spent budget points at the broker or the network and is often already resolved by the time you look; a permanent failure points at the event or the configuration and will recur on replay until the cause is fixed — replaying it unchanged just dead-letters it again.

**Every attempt is logged, including the first**, with `event_id`, `event_type`, `topic`, `attempt` and `max_attempts`. So the log is a usable diagnostic trail while a publish is still being retried, not only once it has failed for the last time:

```bash
docker compose logs server | grep '"event_id":"<the event id>"'
```

**A SUCCESSFUL publish logs at `debug`, not `info`.** At the 500 events per second this pipeline is built for, a line per published event saying "it worked" is volume rather than observability, so the successful-publish lines in the relay and the publisher, the metrics collector's per-tick summary, and the notice that a consumer-lag series has been retired are all emitted at debug. If the trail above shows failures but nothing about the events that are fine, that is the reason — raise the level:

```bash
# Compose: set it in .env and restart the two application services.
BLNK_LOG_LEVEL=debug docker compose up -d server worker

# Kubernetes: it is a key on the blnk-config ConfigMap, projected by both Deployments.
kubectl -n blnk patch configmap blnk-config --type merge -p '{"data":{"BLNK_LOG_LEVEL":"debug"}}'
kubectl -n blnk rollout restart deployment/server deployment/worker
```

Put it back to empty — which means `info` — when the investigation is over: debug is verbose in proportion to throughput. **Failure logging is unaffected either way**: every failed publish attempt is logged at `warning` with its attempt number and error reason at every level, so nothing above depends on having raised it.

### The two states, and why only one is replayable

Read the `status` before deciding anything. Both terminal failure states are listed by the endpoint below, and they need different actions:

| `status` | Meaning | Replayable |
|----------|---------|-----------|
| `failed` | The event is terminal — either reason, so check `terminal_reason` rather than assuming a spent budget — but the **dead-letter write itself has not completed** — `dlt_topic` is still null, so there may be no message on the dead-letter topic at all. Until it lands, `blnk.event_outbox` is the only copy of the event in existence. | **No.** Replay refuses with `409 EVENT_NOT_DEAD_LETTERED`. Restore broker reachability so the dead-letter write completes, then replay. The relay re-claims these rows on its own once it can. |
| `dead_lettered` | The event is on its `<topic>.dlt` sibling with failure metadata attached. | **Yes.** |

A third status, `replaying`, means another replay already holds the row. It is a lease, not a resting
place, and where the row goes next depends on the outcome:

| Replay outcome | Row moves to |
|---|---|
| The republish is acknowledged and recorded | **`dispatched`** — the row leaves the dead-letter inventory for good and is no longer replayable |
| The republish fails, or the result cannot be recorded | Back to **`dead_lettered`**, replayable again |
| The process holding the lease dies | Back to **`dead_lettered`** once the 2-minute lease expires |

A successful replay therefore does **not** return the row to `dead_lettered`; expecting it to is the
usual reason an operator believes a replay "did not take".

### Step 1 — List the dead-lettered events

```bash
curl -sS "$BLNK_API/events/dead-letter?limit=20" \
  --config "$BLNK_CURL_CONFIG" | jq .
```

It is **master-key gated**, following the same privileged-endpoint pattern as hook management: a non-master caller gets `403` with `error_detail.code` of `AUTH_MASTER_KEY_REQUIRED`. It reads `blnk.event_outbox` and never a Kafka topic, so **it answers with the broker down** — which is precisely when you want it.

Paged and filtered:

| Parameter | Effect |
|----------|--------|
| `limit` | Page size. Default `20`, maximum `100`; an out-of-range value resets to `20`, a non-numeric one is refused. |
| `cursor` | Keyset cursor naming the `(occurred_at, id)` coordinate of the last entry on the previous page. It is the **only** way to page — there is no `offset`, and sending one is refused — and it is the stable way: a row cannot be shown twice or skipped when new failures arrive between requests. |
| `event_type` | Exact match on the event name, e.g. `transaction.applied`. |
| `topic` | Exact match on the **original category** topic, e.g. `blnk.transactions`. |
| `dlt_topic` | The same filter expressed as the `.dlt` sibling, e.g. `blnk.transactions.dlt`. |
| `status` | `failed` or `dead_lettered`. Anything else is refused. |
| `occurred_from` | RFC3339 instant. **Inclusive** lower bound on `occurred_at`. |
| `occurred_to` | RFC3339 instant. **Inclusive** upper bound on `occurred_at`. |
| `include_count` | Adds `total_count` to the envelope. The key is **absent** when the option is not supplied — omitted, not zero — so a script must ask for it rather than read it. Supported alongside **every** filter in this table: the total is counted through the same predicate the page is selected by, **in the same database snapshot**, so the total and the page you are holding describe one population. It is not a fixed size to page towards — the inventory is live. |
| `sort_by` | Accepted and **inert**, for compatibility with a generic list-endpoint client. |
| `sort_order` | Accepted and **inert**, for the same reason. |

Ordering is **fixed** at newest occurrence first, ties broken by descending id, which is what makes paging stable — a client-chosen sort column would let a row be shown twice or skipped between pages, which is why `sort_by` and `sort_order` change nothing rather than being honoured. Any parameter not in the table above is refused with `400 GEN_VALIDATION_ERROR` naming every offending name at once.

#### The occurrence window

Either bound may be given alone, and both are **inclusive** — the timestamps you have to hand are `occurred_at` values copied out of a previous page, and a half-open window would silently drop the row you were looking at. The bound is `occurred_at`, when the event **happened**, which is also the axis the listing is ordered and paged by, so a window and the ordering describe the same axis.

Two values are refused rather than guessed at, and both refusals matter during an incident:

- A bound that is **not RFC3339** — a bare date such as `2026-08-01`, or a Unix epoch — is `400 GEN_VALIDATION_ERROR`. Guessing a time zone for a date is how a window ends up a day out, and a day is the difference between "this failed during the incident" and "this failed before it".
- An **inverted** window, `occurred_from` later than `occurred_to`, is `400 GEN_VALIDATION_ERROR`. It matches nothing, and answering it with an empty page would read as "nothing failed then" — the wrong answer to a question that was typed backwards.

The window is applied in **SQL**, served by the `(status, occurred_at)` index — as `event_type`, `topic` and `status` now are. That is what makes "what failed last Tuesday" answerable at all. The filters were once applied by an in-memory walk bounded by a scan limit, over a newest-first inventory, so an older window exhausted that budget before reaching the rows it asked for and returned an empty page with nothing to say the budget had run out. There is no walk and no budget left to reach.

```bash
# Everything that failed inside one incident window.
curl -sS "$BLNK_API/events/dead-letter?occurred_from=2026-08-01T13:00:00Z&occurred_to=2026-08-01T14:30:00Z&include_count=true&limit=100" \
  --config "$BLNK_CURL_CONFIG" | jq '{total: .total_count, events: [.data[] | {event_id, event_type, occurred_at, status}]}'
```

An offset is honoured, so `2026-08-01T14:00:00+01:00` and `2026-08-01T13:00:00Z` select the same instant.

**This listing always answers with the `{data, next_cursor, has_more, total_count?}` envelope**, so read `.data[]` and never the body as an array. That holds whether or not you asked for a total: cursor paging has to return the cursor somewhere, so there is no shape in which the body is a bare list. Page by handing `next_cursor` back as `?cursor=`, and stop when it is absent — not when a page comes back empty.

An empty inventory is `200` with `"data": []` — never `404`, and `data` is never `null`. "No events are stuck" is a successful answer, and a script must be able to range over `.data[]` unconditionally.

**Always page with `include_count=true` when you are investigating a loss.** The total is what tells a short page apart from a complete one, and this is not a theoretical concern: the filtered listing used to be assembled in memory and abandoned after five thousand scanned rows, so a filter whose matches lay past that point returned an empty page with nothing in the response saying so. It is applied in SQL now and there is no bound left to reach — but the total is still the check that would catch a future regression, and it costs one query.

Narrow to one topic's replayable backlog:

```bash
curl -sS "$BLNK_API/events/dead-letter?topic=blnk.transactions&status=dead_lettered&limit=100" \
  --config "$BLNK_CURL_CONFIG" | jq '.data[] | {event_id, event_type, status, attempts, failure_reason, last_attempted_at}'
```

Each item carries `event_id`, `event_type`, `aggregate_id`, `ledger_id`, `partition_key`, `occurred_at`, `schema_version`, `topic`, `dlt_topic`, `status`, `attempts`, `failure_reason`, `first_attempted_at`, `last_attempted_at` and `payload_bytes`. **The payload itself is never returned** — the projection is the single place the stored payload, the raw driver error text and the internal failure struct are dropped, so an operator triaging a backlog does not receive a copy of every event body.

### Step 2 — Read the failure metadata

The dead-letter *message* on the topic carries a `failure_metadata` object with **exactly five fields**, attached as an additive sibling key at the top level — never nested inside `payload`, never replacing it, and never reordering an envelope key. That is precisely what leaves the original event recoverable unchanged.

| Field | What it tells you |
|-------|------------------|
| `original_topic` | **The replay destination.** The category topic the event was destined for — not the `.dlt` sibling it is sitting on. |
| `error_reason` | **The diagnosis, verbatim.** This is the raw error text, and it is on the *message* — see the note below on where each form of the reason lives. |
| `attempt_count` | **How many attempts were made.** It equals the budget (`5` at the defaults) when the budget was exhausted, and is **lower — possibly `1` — when the failure was permanent** and retrying was pointless. A low number is not evidence that Blnk gave up early or that the budget is small. |
| `first_attempted_at` | When the first attempt was made. |
| `last_attempted_at` | When the final attempt was made. |

#### The API gives you `failure_reason`, not `error_reason` — and that is deliberate

The listing surfaces `topic`, `attempts`, `first_attempted_at` and `last_attempted_at` from this object directly. It does **not** surface `error_reason`. The verbatim text is a driver or Kafka-client string that renders with broker addresses, principal names and cluster topology, so the projection drops it and returns a **classification** in its place, under the different name `failure_reason`. Triage from `failure_reason` — it is the field you actually have — and reach for the verbatim text only when the classification is `unclassified` or when you need the exact words for an escalation.

`failure_reason` is drawn from a **closed set of seven values**. Anything else means the API changed:

| `failure_reason` | What it means |
|-----------------|---------------|
| `broker_unavailable` | The broker did not answer or dropped the connection. |
| `authorization_denied` | The producer's credential was refused, or it lacks `Write` on the topic. |
| `topic_missing` | The destination topic does not exist on the broker. |
| `message_too_large` | The event exceeds the configured publish maximum. |
| `timeout` | The attempt exceeded its deadline. |
| `persistence_failure` | The failure was Blnk's own database, not Kafka. The event is intact; the relay's bookkeeping failed. |
| `unclassified` | The stored text matched none of the above. This value says so rather than guessing — go read the verbatim text. |

The classification is derived from the stored text at the response boundary, and `authorization_denied` is checked before `broker_unavailable` because a broker can report both in one message and the authorization half is the actionable one: it will not clear on its own.

#### Retrieving the verbatim `error_reason`

Two places hold it, and neither is the API:

```bash
# From the outbox row — the authoritative record, and available with the broker down.
psql "$BLNK_POSTGRES_DSN" -c \
  "SELECT event_id, status, attempts, last_error
     FROM blnk.event_outbox
    WHERE event_id = '<event_id>'"
```

```bash
# From the dead-letter message itself, for a row whose status is dead_lettered.
kafka-console-consumer.sh --bootstrap-server "$KAFKA_BROKERS" \
  --topic blnk.transactions.dlt --from-beginning --max-messages 200 \
  --consumer.config "$KAFKA_CLIENT_CONFIG" \
  | jq -r 'select(.event_id == "<event_id>") | .failure_metadata.error_reason'
```

The outbox row's `last_error` column is the same text `failure_metadata.error_reason` was built from, so the two agree **at the moment the event was dead-lettered**. Prefer the row: it exists for a `failed` event too, which has no dead-letter message yet.

> **They stop agreeing after a failed replay, and knowing which is which is the difference between triaging the original fault and triaging your own retry.** A replay that fails writes its own reason into `last_error`, so the row — and therefore the API's `failure_reason`, which is classified from that column — now describes the **most recent** attempt. `failure_metadata`, on the row and in the dead-letter message alike, is never rewritten: it preserves the reason the event was dead-lettered in the first place. So read `failure_reason` for "what is wrong now" and `failure_metadata.error_reason` for "what went wrong originally", and when the two disagree, treat the disagreement as the useful signal — the original cause was fixed, or was never the cause, and something else is refusing the event now.

**The two timestamps together are the most useful field in the object**, because their difference bounds the window the failure persisted over, and that is how you tell a transient outage from a poison message:

- A gap of roughly **15 seconds** — the whole backoff schedule and nothing more — means every attempt failed back to back. The cause was continuous for the entire window: a broker that was down, a topic that does not exist, a credential that is wrong, an ACL that forbids the write. Fix the cause and replay; the event itself is fine.
- A gap **much longer than the schedule** means the attempts were spread by relay restarts or lease expiries. Look for a cause that came and went.
- Many events sharing a near-identical window are **one incident**, not many. Triage the cause once and replay them together.
- One event failing while its neighbours on the same topic succeeded is a **property of that event** — the size limit, or something the broker rejected about that specific record. Replaying it will fail again.

#### `failure_reason` on the API is a classification, not raw text

This distinction matters when you script triage, because the two forms are not interchangeable:

- **The API's `failure_reason`** (from `GET /events/dead-letter`) is a **bounded classification**,
  derived from the underlying error. Branch on it: the vocabulary is closed and stable.
- **The raw error text** is deliberately kept out of that projection. It lives on the outbox row's
  `last_error` column and in the dead-letter *message*'s `failure_metadata.error_reason`. Read it when
  you need the detail; do not pattern-match it in a script.

The classification vocabulary, in full:

| `failure_reason` | Means | Retrying alone will help? |
|---|---|---|
| `broker_unavailable` | The broker could not be reached or refused the connection | Yes, once the broker is back |
| `authorization_denied` | The producer principal is not permitted to write the topic | No — fix ACLs first |
| `message_too_large` | The serialised record exceeds what the broker accepts | No — fix broker/topic limits first |
| `topic_missing` | The destination topic does not exist | No — provision the topic first |
| `timeout` | The publish exceeded its deadline | Usually, once load or latency recovers |
| `persistence_failure` | The database write around the publish failed | Yes, once the database recovers |
| `unclassified` | The error matched none of the above | Read the raw text before deciding |

Two of these — `authorization_denied` and `message_too_large` — are the permanent failures that
dead-letter immediately, which is why you will see them alongside a low `attempt_count`.

**Replaying without fixing the cause re-fails.** For every row above marked "No", a replay attempt is
wasted work until the underlying condition is corrected.

### Step 3 — Decide

Branch on the `failure_reason` the listing gave you. It is the field the API exposes, so every row below is decidable from the response alone — no database access and no consumer required.

| `failure_reason` | Cause | Do this |
|-----------------|-------|---------|
| `broker_unavailable` | The broker did not answer, or dropped the connection, during the window | Restore the broker, confirm the healthcheck passes, then replay. Check `blnk_outbox_pending` is falling before you replay in bulk. |
| `timeout` | An attempt exceeded its deadline — usually load rather than a fault | Confirm the cluster is healthy and the backlog is draining, then replay. If it recurs at a steady rate, the cluster is undersized for the publish rate rather than broken. |
| `topic_missing` | Provisioning never ran, or `KAFKA_TOPIC_PREFIX` changed and the new namespace was never created | Re-run provisioning (`make kafka_provision`), verify the eight names with `kafka-topics.sh --describe`, then replay. |
| `authorization_denied` | The producer principal's credential or ACLs are wrong | Repair `KAFKA_SASL_USER`/`KAFKA_SASL_SECRET` and confirm the producer holds `Write` and `Describe` on the owned topics, then replay. **Do not** work around it with `KAFKA_ALLOW_ADMIN_PRODUCER`. |
| `message_too_large` | The event exceeds the 768 KiB publish limit | **Replay will fail again.** Investigate the producer: this is an oversized payload, not a transport fault. Capture the `event_id`, `event_type` and `payload_bytes` and raise it against the emitting code path. |
| `persistence_failure` | Blnk's own database failed, not Kafka. The event may well have reached the topic while the bookkeeping did not | Check PostgreSQL health first. Then read the row: if `kafka_topic`/`kafka_partition`/`kafka_offset` are populated the message is already on the topic, and replaying would publish a **second** copy that subscribers must deduplicate on `event_id`. |
| `unclassified` | The stored text matched no known signature | Read the verbatim `error_reason` as shown above and treat it as one of the cases below before replaying. |

Two verbatim signatures are worth recognising once you have the raw text, because neither is fixed by replaying:

| Verbatim `error_reason` contains | Cause | Do this |
|--------------------------------|-------|---------|
| `INVALID_REPLICATION_FACTOR` while creating, or an under-partitioned topic | Topic geometry is wrong for this cluster | Fix `KAFKA_REPLICATION_FACTOR` for the cluster's broker count and re-run provisioning. A non-empty under-partitioned topic will be **refused**, not grown — see [Partitions](#partitions). Then replay. |
| A serialisation or encoding error naming this one event | A genuinely malformed event | **Replay will fail again.** Investigate the producer. Leave the row dead-lettered as evidence. |

For `message_too_large`, for a malformed event, and for anything you have already replayed once without success, do not loop on replay — each attempt costs a broker round trip and leaves the row exactly where it was.

A useful shortcut when the classification is what you are triaging on: the listing's own filters cannot narrow by `failure_reason`, so group the page client-side.

```bash
curl -sS "$BLNK_API/events/dead-letter?limit=100" \
  --config "$BLNK_CURL_CONFIG" \
  | jq '.data | group_by(.failure_reason) | map({failure_reason: .[0].failure_reason, count: length})'
```

### Step 4 — Replay

`POST /events/dead-letter/{event_id}/replay`, where `{event_id}` is the `event_id` from the listing — not the outbox row's numeric id, and not the `aggregate_id`. Master-key gated like the rest of the surface.

The id must be a **canonical lowercase UUID**, which is the only form Blnk mints. It is validated before the lookup, so a mistyped, uppercased or braced spelling answers `400 GEN_VALIDATION_ERROR` rather than `404` — the distinction matters, because a `404` would send you looking for an event that is sitting right there. Surrounding whitespace is trimmed.

```bash
curl -sS -X POST \
  "$BLNK_API/events/dead-letter/9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b/replay" \
  --config "$BLNK_CURL_CONFIG" | jq .
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
| `400` | `GEN_VALIDATION_ERROR` | The id is not a canonical lowercase UUID, so it cannot be an event id. |
| `403` | `AUTH_MASTER_KEY_REQUIRED` | Use the master key. |
| `404` | `EVENT_NOT_FOUND` | No event with that id. |
| `409` | `EVENT_NOT_DEAD_LETTERED` | Not replayable: the row is `failed` rather than `dead_lettered`, already replayed, or a concurrent replay holds it. |
| `500` | `EVENT_REPLAY_FAILED` | The re-publish failed, or it succeeded and the outbox entry could not be cleared. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | No broker is configured, or the broker is down. Fix that first. |

Replaying a backlog is a loop over the listing. Keep it deliberate — one topic and one cause at a time:

```bash
curl -sS "$BLNK_API/events/dead-letter?topic=blnk.transactions&status=dead_lettered&limit=100" \
  --config "$BLNK_CURL_CONFIG" \
| jq -r '.data[].event_id' \
| while read -r id; do
    curl -sS -X POST "$BLNK_API/events/dead-letter/$id/replay" \
      --config "$BLNK_CURL_CONFIG" | jq -c '{event_id, topic, status}'
  done
```

#### Two facts that make replay trustworthy

- **It re-publishes the original stored bytes.** The service sends the payload as it was stored on the outbox row, stripping only the failure metadata the dead-letter copy added. Nothing decodes and re-encodes it, because a round trip through a struct would reorder JSON object keys and break the byte-for-byte guarantee. The acknowledgement deliberately does not echo the payload either, so nobody is tempted to diff the wrong pair of byte strings.
- **`event_id` is preserved.** A replay carries the same id and the same partition key as the original, so it lands on the same partition, and a subscriber deduplicating on `event_id` absorbs it silently. Replaying an event that was in fact already delivered is therefore safe. Delivery remains at-least-once; the deduplication obligation is the subscriber's, as [event-streaming.md](event-streaming.md#delivery-guarantees-and-your-idempotency-obligation) states.
- **A replay does NOT restore occurrence order.** This is the one property to be careful about. The
  record is appended at the **tail** of the partition, at the time of the replay — not at the position
  the original would have occupied. A consumer therefore sees the replayed event *after* newer events
  for the same aggregate that were published while it sat in the dead-letter topic. Same partition, so
  the per-key ordering guarantee is not broken for anything published afterwards; but the replayed event
  itself arrives late and out of sequence relative to its own aggregate's history.

  Consequence for consumers: a handler that assumes each aggregate's events arrive in occurrence order
  can compute a stale result from a replay — applying an older state change on top of a newer one. If
  your handler is order-sensitive rather than idempotent, check `occurred_at` against the state you
  already hold before applying, and ignore an event older than what you have.

### Step 5 — There is no resolve step

**A replay the broker acknowledges is the whole of the workflow.** There is no second call to record that you dealt with an entry, and the absence is deliberate.

A `POST /events/dead-letter/{event_id}/resolve` route existed and has been withdrawn. It recorded an operator's decision that an entry needed no further action, which then made the row eligible for the retention purge — and it could not compose with a replay in either order. Resolve first and the replay's success became unrecordable: marking the row `dispatched` violated the constraint that confined a resolution to the dead-lettered states, so the API answered `EVENT_REPLAY_FAILED` for a publish the broker had *accepted* and released the row as replayable again, inviting a duplicate on the topic. Replay first and the resolution was refused outright, because it required the `dead_lettered` state the replay had just left.

What follows from the withdrawal, in the terms of the two things it used to control:

- **The retention purge** now deletes `dispatched` rows only. A dead-lettered row is never removed by age, however old it is, so nothing can destroy the evidence of a loss. It becomes deletable by being *replayed*: the acknowledged re-publish makes it `dispatched`, and `RELAY_EVENT_RETENTION_DAYS` then applies to it as it does to every other receipt.
- **The dead-letter age gauge** counts every outstanding entry, with no exemption. `blnk_dlt_oldest_message_age_seconds` and `DeadLetterMessageStuck` therefore stay up until the event has actually reached a subscriber, which is stronger pressure than a note could be — an entry cannot be signed off without being delivered.

That leaves the two cases a resolution used to cover, and both have an honest answer:

| Situation | What to do |
|---|---|
| Replayed successfully | Nothing further. The row is `dispatched`, it has left the inventory and the gauge, and retention will remove it. |
| The subscriber is gone, a later event supersedes it, or the loss is accepted | The entry stays. Record the decision where decisions belong — the incident ticket — and, when the event genuinely must leave the inventory, replay it: the topic is Blnk's own, a subscriber deduplicates on `event_id`, and a decommissioned subscriber has no consumer to receive it. Deleting the row by hand in the database is the other option and it destroys the only record that the event went undelivered, so take it only with the same deliberation as any other manual write to a ledger-adjacent table. |

### Step 6 — Verify

Three checks, in increasing strength.

```bash
# 1. The outbox row left the dead-lettered state. The event id should no longer
#    appear in the inventory.
curl -sS "$BLNK_API/events/dead-letter?status=dead_lettered&limit=100" \
  --config "$BLNK_CURL_CONFIG" \
| jq -r '.data[].event_id' | grep -c '9f8d3c21-4b7a-5e6f-8a12-0c4d5e6f7a8b' || echo "cleared"

# 2. The counts moved: dead_lettered down, dispatched up.
curl -sS "$BLNK_API/events/stats" --config "$BLNK_CURL_CONFIG" \
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

## Retention: the two lifecycles

`RELAY_EVENT_RETENTION_DAYS` is a **data-protection control**, not a storage knob. Each outbox row's payload is the webhook body verbatim, so a transaction event carries amounts and balance identifiers and an identity event carries names, email addresses, phone numbers, postal addresses and dates of birth. Kept indefinitely, the delivery buffer becomes an unbounded second copy of the ledger's most sensitive data — with none of the access controls the primary tables have around it.

The sweep runs hourly in the server role, deletes in bounded batches so it never blocks the relay, and counts what it removes on `blnk.events.purged.total`. `0` disables it entirely.

**The eligibility rule, in full:**

| Row state | Deleted by age? | Why |
|---|---|---|
| `dispatched` | **Yes** | A receipt. The event reached the broker and a subscriber has had it; after the period the row says nothing anyone needs. |
| `dead_lettered` | **Never, however old** | It is the only record that a ledger event went undelivered, the only thing a replay can be driven from, and the only place the failure metadata lives. Replaying it makes it `dispatched`, and *then* the period applies. |
| `failed` | **Never** | Its `<topic>.dlt` write is still owed, so this table is the only copy of the event in existence. |
| `pending`, `processing`, `webhook_pending`, `replaying` | **Never** | A delivery attempt is still owed. |

That second row is what makes a finite period safe to configure. **The purge cannot destroy the evidence of a loss** — it can only remove events that reached the broker. The pressure to work through the backlog is the `DeadLetterMessageStuck` alert, which keeps firing until each entry has actually been delivered.

The practical consequence for an operator: **a dead-letter backlog grows without bound and the purge will not help you.** If `blnk_dlt_oldest_message_age_seconds` is high and the inventory is large, the answer is to work through it with Steps 1–4 above, not to shorten the retention period — shortening it changes nothing for those rows.

To size the backlog that is waiting on you:

```bash
curl -sS "$BLNK_API/events/dead-letter?include_count=true&limit=1" \
  --config "$BLNK_CURL_CONFIG" | jq '{outstanding: .total_count}'
```

## The Daily Outbox-versus-Offset Reconciliation

Run this once a day. It compares **outbox rows that claim to have been published** against
**records the broker actually holds**.

**Read what it is carefully: this is a net-shortfall SCREEN, not a proof of zero loss.** It counts
records; it does not identify them. A screen pass means "no shortfall was detected in the totals",
which is a useful daily signal and a genuine alarm when it fails — but it is *not* evidence that every
event reached a topic. Only the bounded per-`event_id` audit in
[Step 6](#step-6--when-counting-is-not-enough-the-per-event_id-audit) establishes event presence, and
**scoring acceptance criterion V-2 requires that audit.** The screen alone cannot score it.

Why counting cannot decide it, concretely — four independent reasons, each sufficient on its own:

- **Anything can compensate for a missing event.** A redelivery, a replay, or a dead-letter copy adds
  a record without adding an event. Ten of those alongside ten lost events produce exactly the totals
  of a healthy pipeline.
- **Distinct coordinates do not prove matching event ids.** `confirmed_events` counts rows whose
  stored topic/partition/offset are distinct. Distinct coordinates say two rows point at two different
  records; they do not say those records carry the event ids the rows claim.
- **Retention does not lower end offsets.** Kafka's latest offset never decreases when segments are
  deleted, so records aged out of a topic still count on the broker side while being unreadable.
- **The two sides do not age together.** Outbox retention prunes terminal rows on its own schedule,
  which lowers the outbox side while the broker's end offsets stay put. A long-running deployment
  therefore accumulates a permanent, growing surplus that hides a real shortfall inside it.

`CountEventOutboxByStatus` in `database/event_outbox.go` exists specifically to serve this check, and `GET /events/stats` exists to expose it. Neither is a general-purpose reporting API; do not build dashboards on them.

### Step 1 — Take the snapshot

```bash
curl -sS "$BLNK_API/events/stats" --config "$BLNK_CURL_CONFIG" | jq . > recon-$(date -u +%Y%m%dT%H%M%SZ).json
cat recon-*.json | jq .
```

One call gets both sides. The response below is a REAL one, taken verbatim from a running
deployment so that every member is one the API actually emits — the numbers are small because the
deployment was, and `measured_windows` is abridged to two of its 48 entries:

```json
{
  "pending": 0,
  "processing": 0,
  "webhook_pending": 0,
  "dispatched": 2,
  "failed": 0,
  "dead_lettered": 2,
  "replaying": 0,
  "producer_atomicity": {
    "monitor_handoff_pending": 0,
    "monitor_handoff_processing": 0,
    "monitor_handoff_completed": 0,
    "monitor_handoff_failed": 0,
    "unfinalized_batches": 0
  },
  "topic_end_offsets": {
    "blnk.transactions": 0,
    "blnk.transactions.dlt": 0,
    "blnk.balances": 0,
    "blnk.balances.dlt": 0,
    "blnk.identities": 2,
    "blnk.identities.dlt": 2,
    "blnk.system": 0,
    "blnk.system.dlt": 0
  },
  "offsets_complete": true,
  "partitions_unavailable": 0,
  "measured_windows": [
    { "topic": "blnk.identities", "partition": 4, "first_offset": 0, "end_offset": 1, "records": 1 },
    { "topic": "blnk.identities", "partition": 5, "first_offset": 0, "end_offset": 0, "records": 0 }
  ],
  "offsets_measured_at": "2026-08-10T01:10:12.645984448Z",
  "window_start": "2026-08-09T01:10:12.591082402Z",
  "window_seconds": 86400,
  "generated_at": "2026-08-10T01:10:12.592071725Z",
  "reconciliation": {
    "terminal_events": 4,
    "corroborated_events": 4,
    "unconfirmed_events": 0,
    "unmeasured_events": 0,
    "aged_out_events": 0,
    "beyond_end_events": 0,
    "duplicated_records": 0,
    "messages_written": 4,
    "records_retained": 4,
    "blnk_record_share": 4,
    "overhead": 0,
    "loss_detected": false,
    "conclusive": true,
    "window_start": "2026-08-09T01:10:12.591082402Z",
    "windowed": false,
    "covered_from": "2026-08-10T00:27:53.816226Z",
    "covered_to": "2026-08-10T00:47:02.967735Z",
    "oldest_terminal_at": "2026-08-10T00:27:53.816226Z",
    "summary": "NO LOSS DETECTED: every one of the 4 outbox rows published between 2026-08-10T00:27:53Z and 2026-08-10T00:47:02Z names the distinct broker record it produced, inside the measured offset window of its own partition, so no event this outbox still retains is missing from the broker. The broker's 4 record(s) are a CUMULATIVE total for the topics rather than a count inside a shared window, so the difference against the row count is not a surplus and no shortfall can be computed from it",
    "measured_at": "2026-08-10T01:10:12.645984448Z"
  }
}
```

`missing_topics` and `caveats` are absent here because both are omitted when empty, which on this
response is the healthy reading. So are `producer_atomicity.oldest_unfinalized_batch_at`, for the
same reason, and `reconciliation.covered_from` / `covered_to` when nothing was corroborated.

**FIVE FIELDS THAT USED TO BE DOCUMENTED HERE NO LONGER EXIST**, and a script asserting on them
reads a constant. They were `purged_events`, `all_time_terminal_events`, `verified_records`,
`unverifiable_records` and `missing_records`, and they belonged to a whole-history reconciliation
that corrected for retention with a purge log. The comparison is now drawn inside the
per-partition windows the offsets were measured in, so there is nothing to correct for, and each
question they answered has a better home: `corroborated_events` replaces `verified_records`,
`beyond_end_events` replaces `missing_records` and is the one signal that is unambiguous loss,
and `unverifiable_records` split into the two causes it used to fuse — `aged_out_events`, where
retention removed a record the stored offset still evidences, and `unmeasured_events`, where the
measurement did not cover that topic or partition.

**`windowed` is the field to read before believing `overhead`**, and on the response above it is
`false` while `window_start` is present: the request bounded the OUTBOX side to 24 hours, and the
broker's end offsets are cumulative for the life of each topic, so the two sides do not describe
one interval. The verdict is unchanged and rests on the per-row coordinate mapping rather than on
the totals — which is exactly what the summary says, and why it declines to call the difference a
surplus. A response whose two sides do share a window reports `windowed: true` and a summary that
names `overhead` as redelivery, replay and dead-letter copies.

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

#### `producer_atomicity` — events that are owed and not yet in the table at all

The seven counts above are a census of rows that **exist**. Two event families are captured from an *intent* recorded atomically with their mutation — a balance-monitor handoff and a bulk-batch coordinator row — and while an intent is outstanding, its event has not been captured yet and appears in **no** status. A reconciliation that read only the census would find it internally consistent while monitor alerts and batch summaries were still owed, so this object is reported alongside it.

| Field | Meaning | What to do |
|-------|---------|-----------|
| `monitor_handoff_pending` | Balance movements whose monitors have not been judged yet | Nothing. A small non-zero number is one poll interval of work |
| `monitor_handoff_processing` | Handoffs a processor currently holds | Nothing |
| `monitor_handoff_completed` | Handoffs judged, overwhelmingly "judged, nothing fired" — which is why it dwarfs the number of alerts ever published | Nothing |
| `monitor_handoff_failed` | **Evaluation budget spent.** Each one is a balance movement whose monitor conditions were never judged, so any alert it should have produced does not exist and never will without intervention | Investigate. `last_error` on the row names the cause; the ERROR log carries the handoff id, the balance and the attempt count |
| `unfinalized_batches` | Asynchronous bulk batches that began and never reported an outcome, past a grace period so batches still legitimately running are excluded | Investigate with `oldest_unfinalized_batch_at`. The member transactions are durable and carry the batch id, so this is a missing **summary**, never lost money |
| `oldest_unfinalized_batch_at` | When the oldest outstanding batch began. Omitted when there are none | Age is what separates a large batch still running from an abandoned one, so the count alone is not actionable and this is |

**The whole object is omitted when it could not be read.** That is deliberate and it is the reading to check for: zero means "nothing is outstanding", which is exactly the answer this check must not be given when the truth is "we could not tell". An absent `producer_atomicity` on a response that otherwise has counts means the owed-event side was not measured, and the day's reconciliation is incomplete in that dimension however green the verdict below reads.

These counts are a **separate signal from the loss verdict** and do not feed it. An owed event is not a lost one — its intent is durable, and the event still arrives when the handoff is evaluated or the batch finalises. Only `monitor_handoff_failed` and a stale `unfinalized_batches` describe an event that will never exist, and neither can be seen anywhere in the arithmetic of Step 2.

Leave `include_offsets` off. Absent means **best effort**: the broker is read when one is configured, and a failure is logged and omitted, which is what this procedure wants. `include_offsets=true` makes the broker read *required* and answers `503 EVENT_KAFKA_UNAVAILABLE` on failure; `include_offsets=false` skips the broker entirely. There is deliberately no topic-narrowing parameter — the verdict compares the broker against **every** outbox row that claims a publication, so measuring a subset of topics would manufacture a shortfall and report loss that has not happened.

### Step 2 — Read the verdict, in this order

The server computes the verdict itself, so you do not re-implement the arithmetic. Branch on three fields, and **in this order**:

```bash
jq -r '
  if .reconciliation == null then
    "INCONCLUSIVE: no broker side was measured"
  elif (.reconciliation.beyond_end_events // 0) > 0 then
    .reconciliation.summary
  elif .reconciliation.loss_detected then
    .reconciliation.summary
  elif (.reconciliation.conclusive | not) then
    "INCONCLUSIVE: " + ((.reconciliation.caveats // []) | join("; "))
  else
    .reconciliation.summary
  end' recon-*.json
```

**Three branches print `summary` unlabelled because `summary` is already the verdict line.** The
server writes it beginning `LOSS DETECTED:`, `NO LOSS DETECTED:` or `INCONCLUSIVE:`, so a script
that prepends its own label emits `LOSS DETECTED: LOSS DETECTED: …` and an operator reads a
stutter instead of a reason. The two branches that DO add a label are the two with no summary to
print: a `reconciliation` that is absent entirely, and the inconclusive branch, which deliberately
prints the `caveats` list rather than the summary because the caveats name every reason in plain
words. `grep -q '^LOSS DETECTED'` still works on the output either way.

**Loss is tested BEFORE inconclusiveness, and the order is load-bearing.** An incomplete measurement can only ever *understate* what the broker holds, so it cannot manufacture a shortfall — which means a shortfall on an inconclusive measurement is still a shortfall. Checking `conclusive` first would print `INCONCLUSIVE` and bury it, and an operator reading a caveat list would go looking for a measurement problem instead of missing events.

**The first branch reads `beyond_end_events`, and it is not redundant with `loss_detected`.** That field is the one unambiguous signal in the whole check — a row naming an offset at or above the end of its partition's log, which the broker itself once assigned, so the log reached it and does not now. `loss_detected` covers it too, and also covers the arithmetic shortfall that only a windowed comparison can produce; branching on the stronger signal first is what makes the printed line say *which* kind of loss without reading the summary. This branch used to read `missing_records`, a field the API no longer returns: it always evaluated to the `// 0` default, so the branch was dead and the whole verdict rested on the next one. It degraded safely — `loss_detected` still caught the loss — but a script that asserted on `missing_records` itself asserted on a constant.

1. **`reconciliation` absent** → no offsets could be read at all. Either no brokers are configured — a legitimate steady state, not an error — or the broker was unreachable. There is nothing to compare. Not a pass and not a failure.
2. **`conclusive` false** → counting cannot decide the matter today. Read `caveats`, which names every reason in plain words. **This is the field to check before reporting anything green**, and it is the one an eager script skips.
3. **`loss_detected` true** → the broker holds **fewer** records than the outbox has terminal rows: rows claiming a publication no record corresponds to. Escalate — Step 5.
4. **`conclusive` true and `loss_detected` false** → **the screen passed**. Record `summary`. This is
   not a zero-loss result: see the four reasons above, and run
   [Step 6](#step-6--when-counting-is-not-enough-the-per-event_id-audit) when you need the actual
   property rather than the daily signal.

The screen condition, stated as arithmetic:

```text
terminal_events  = dispatched + webhook_pending + dead_lettered   (rows claiming publication)
messages_written = SUM(topic_end_offsets)                         (the four topics + the four .dlt siblings)
overhead         = messages_written - terminal_events             (SIGNED, never clamped)

Every terminal row lands in exactly one bucket, and the five sum to terminal_events:

  corroborated_events  coordinate inside [first_offset, end_offset) of its partition
  unconfirmed_events   no coordinate at all
  unmeasured_events    names a topic or partition this reading did not cover
  aged_out_events      offset BELOW first_offset — written, then deleted by retention
  beyond_end_events    offset AT OR ABOVE end_offset — the log no longer reaches it

PASS  when  corroborated_events == terminal_events  AND  duplicated_records == 0
            AND missing_topics is empty AND partitions_unavailable == 0   (⇒ conclusive)
FAIL  when  beyond_end_events > 0                                          (⇒ loss_detected)
```

**Do not compare `messages_written` against `terminal_events`.** That comparison is reported for context and proves nothing, because the two describe different populations: `messages_written` is the cumulative end offset of shared topics — every redelivery, every replay, every dead-letter copy, and every record any *other* producer ever wrote — while the outbox counts the events it currently retains. It can exceed the event count arbitrarily, and a surplus is *indistinguishable from compensated loss*: ten redeliveries plus ten lost events produce exactly the totals of a healthy pipeline. `blnk_record_share` is the honest version of that question — how many retained records this verdict attributed to Blnk rows.

The five buckets are separate because the remedies are:

- **`unconfirmed_events`** — a claim nothing corroborates, and precisely what a surplus could be hiding. Usually a relay that published before coordinate recording existed, or a client that returned no coordinate. Inconclusive while any remain.
- **`unmeasured_events`** — the reading did not cover their partition: see `missing_topics` and `partitions_unavailable`. A broker problem, not an event problem.
- **`aged_out_events`** — Kafka retention has deleted the record. The write is *evidenced by the stored offset*, so this is not loss; but a subscriber that had not consumed the record by then never will. Bound the two retentions against each other — Step 4, item 4.
- **`beyond_end_events`** — **the one unambiguous signal.** The broker issued that offset when it accepted the write, so the log reached it once and does not now: the partition was truncated or the topic deleted and recreated. Escalate.
- **`duplicated_records`** — corroborated rows sharing a coordinate. **It should always be zero.** A partial unique index forbids two rows naming the same record, so a non-zero value reports a broken schema, not tolerable duplication.

`covered_from`, `covered_to` and `oldest_terminal_at` state the verdict's **scope**. A pass is a statement about the rows the outbox still holds and nothing earlier: if `oldest_terminal_at` is recent, retention has pruned rows and a green verdict covers only the window since. Record it with the result.

**A conclusive loss is a *conclusive* result.** `conclusive` reports whether the comparison could be trusted, not whether its answer is welcome, so a loss proven from a coordinate is reported with `conclusive: true` and `loss_detected: true`. Do not read `conclusive` as "everything is fine".

### Step 3 — Cross-check the broker side from the CLI (optional)

Useful when you want the offsets independently of the API, or when the API's broker read is the thing you suspect:

```bash
# End offsets for all eight topics, one line per topic-partition.
for t in transactions balances identities system; do
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
4. **Retention makes the verdict inconclusive — it does not manufacture a shortfall.** This one is easy to get backwards, so be precise about the mechanism: **deleting a segment advances the log-START offset, and never lowers the log-END offset.** An end offset is monotonically non-decreasing for the life of a topic. `messages_written` is the sum of END offsets, so it counts every record the broker ever accepted, *including ones retention has since deleted* — which is exactly why it is the reconciliation figure. Retention therefore cannot produce a negative `overhead`, and a negative `overhead` must never be explained away by it.
   What retention does change is what is still **on** the log. `retained_count` — the sum of end minus earliest across partitions — falls below `messages_written` by the number of deleted records, and the server reports that difference as a caveat and sets `conclusive: false`. Read that as it is meant: not "the comparison went wrong" but "records these offsets count are no longer consumable or replayable, so do not conclude anything about what a subscriber can still read". The counting comparison remains valid in the direction that matters.
   The outbox side is the one retention can move *downwards*. `RELAY_EVENT_RETENTION_DAYS` purges terminal rows, which lowers `terminal_events` and therefore **inflates** `overhead`. That is the safe direction — it cannot fake loss — but it does make `overhead` meaningless as a health figure across a period longer than the purge window, so compare over a window shorter than it. The setting is `0`, retention disabled, by default and deliberately: deleting ledger-adjacent records is a decision only an operator can take, and only terminal rows are ever eligible. Confirm the sweep with `blnk_events_purged_total`.
   So:
   - **Run it daily**, over a window shorter than both the broker's retention and `RELAY_EVENT_RETENTION_DAYS`, so neither side has aged and `conclusive` can actually be true.
   - **Read the `caveats` array.** When records have aged out, the response carries a caveat naming exactly how many were written that the broker no longer holds — that difference is the gap between what was published and what is still readable, and it is the number to know before promising anyone a replay. It is also why `conclusive` is false. The API reports it there rather than as a field of its own; from the CLI side the same fact is `kafka-get-offsets.sh --time earliest` returning non-zero on a topic.
   - **The mechanism that CAN lower the broker side is a topic being deleted and recreated**, because a new topic's offsets restart at zero. That is not retention, it produces a genuine negative `overhead`, and it is indistinguishable from loss by counting alone — so if a shortfall appears, establish that the topics are the original ones before treating the number as evidence.

Note what `offsets_complete` does and does not tell you now: a **missing topic** or an **unreadable partition** means no window could be measured for it, so the rows on it are counted as `unmeasured_events` rather than silently dropped or read as loss. `missing_topics` and `partitions_unavailable` say which part of the broker could not be read; `offsets_complete` is the single field to branch on before interpreting anything, and it is present on every response.

### Step 5 — Escalate

When `loss_detected` is true and a second snapshot agrees, treat it as event loss: specific records this outbox recorded are no longer on the log.

Capture, before anything is restarted:

1. **Both snapshots**, whole. `generated_at` and `offsets_measured_at` are part of the evidence.
2. **`reconciliation.summary`**, `terminal_events`, `messages_written`, `overhead`, `unconfirmed_events` and `duplicated_records`, verbatim.
3. **`blnk_outbox_pending`** at the time of the snapshot, and its trend over the preceding day — a backlog that dropped without a matching rise in `blnk_events_dispatched_total` is the shape of rows leaving the table without reaching a terminal state.
4. **`blnk_events_dispatched_total`** and **`blnk_events_dead_lettered_total`** over the same window. These are the per-event terminal counters, so their sum is directly comparable with a count of rows. Capture `blnk_events_published_total` alongside them: it counts acknowledged broker *writes*, so the amount by which it exceeds the dispatched count over the same window is the redelivery volume, which is itself evidence about how the relay was behaving.
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

Then confirm the cause. `beyond_end_events` says the log no longer reaches offsets Blnk recorded, and there are only three ways that happens: the topic was **deleted and recreated** (its offsets reset to zero, so every earlier coordinate is beyond the new end), a partition was **truncated** by an unclean leader election or a manual offset reset, or the snapshot raced a **write that landed after the windows were measured** — which a second snapshot rules out. Compare `measured_windows` against the coordinates the rows carry:

```bash
$BLNK_PSQL -c "
  SELECT kafka_topic, kafka_partition, min(kafka_offset), max(kafka_offset), count(*)
  FROM blnk.event_outbox
  WHERE kafka_offset IS NOT NULL
  GROUP BY kafka_topic, kafka_partition
  ORDER BY kafka_topic, kafka_partition;"
```

A `max(kafka_offset)` at or above that partition's `end_offset` is the row set in question.

If neither explains it, this is a defect in the relay's mark-after-publish path. Do not purge, replay in bulk, or truncate anything until the evidence above is captured — the outbox row is the only remaining record of an event whose message is gone.

### Step 6 — When counting is not enough: the per-`event_id` audit

**This step is REQUIRED to score acceptance criterion V-2. It is not an optional follow-up.**

Steps 1 to 5 compare **totals**, and totals cannot establish that any particular event is present — for
the four reasons given at the top of this section, of which the sharpest is that **a surplus is
indistinguishable from compensated loss.** Ten redeliveries alongside ten lost events produce exactly
the totals of a healthy pipeline. `unconfirmed_events` stops the most obvious version of that reading
as green, but proving the actual property — that **every** `event_id` reached a topic at least once —
requires the topics to be **read and deduplicated by `event_id`**, not counted.

**That requires a consumer, and Blnk implements no consumer by design.** `ConsumerLag` and
`ListOffsets` count records; they cannot tell you which ones. So this step is operator work with your
own tooling.

Run it:

- **whenever V-2 is being scored or attested** — the screen cannot substitute for it;
- after any incident, broker replacement or relay crash;
- whenever `unconfirmed_events` is persistently non-zero;
- on a routine sampled basis, on a bounded window, so the evidence exists before you need it.

The window keeps it bounded and therefore practical: audit a period short enough that both sides can
be read in full, rather than attempting the whole history.

1. **Take the outbox side.** Every row that claims a publication, over a bounded window:

   ```bash
   $BLNK_PSQL -At -F, -c "
     SELECT event_id
     FROM blnk.event_outbox
     WHERE status IN ('dispatched','webhook_pending','dead_lettered')
       AND occurred_at >= now() - interval '24 hours'
     ORDER BY event_id;" > /tmp/outbox-ids.txt
   wc -l /tmp/outbox-ids.txt
   ```

2. **Take the broker side.** Read every category topic and its `.dlt` sibling from the beginning of the window with a consumer that holds a grant over all eight — the administrative principal, or a purpose-made audit principal — and extract the envelope's `event_id`. Any consumer will do; the console consumer is enough for a one-off:

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

`alerts/blnk-kafka-alerts.yml` defines **one rule group, `blnk-kafka-alerts`, evaluated every 30 seconds**, holding **thirteen** rules. Seven are CONDITION rules — a dead-letter entry left unresolved, a subscriber falling behind, a credential awaiting revocation, an unaccounted credential, a refused revocation, and the two settlement rules — and six are MEASURABILITY rules, which fire when Blnk cannot tell whether a condition rule should. Each names this document as its `runbook_url`. The sections below are in the same order as the file.

Before working any of them, satisfy yourself that the alert is *loaded* — see [Is the alert armed at all?](#is-the-alert-armed-at-all) — because "the alert did not fire" and "the alert was never evaluated" look identical from the outside.

The full metric catalogue, the attribute domains and example PromQL are in [metrics.md](metrics.md); they are not repeated here.

### DeadLetterMessageStuck

```text
expr:     blnk_dlt_oldest_message_age_seconds > 900
for:      0m
severity: critical
```

**900 seconds is the 15-minute threshold**, written as a literal so it is greppable. The dwell is deliberately `0m` rather than omitted: the threshold already carries the whole 15 minutes, and a `for` here would double-count it.

The gauge is an **age, not a count** — a single entry this old is enough to fire — and it is the age of the *oldest outstanding* entry per dead-letter topic, so one comparison covers the whole inventory. Its inventory is every outbox row in the `failed` **or** `dead_lettered` state, reported under the `.dlt` topic it was destined for, so an event whose dead-letter write itself failed is included. There is no exemption: an entry leaves this gauge by being **replayed**, which makes it `dispatched`, and by nothing else.

1. **Identify the topic** from the alert's `topic` label. It is a `.dlt` name; the original category topic is that name with `.dlt` removed, and it is where a replay goes.
2. **Read the status first, because remediation depends on it.**

   ```bash
   curl -sS "$BLNK_API/events/stats" --config "$BLNK_CURL_CONFIG" | jq '{failed, dead_lettered, replaying}'
   ```

   A non-zero `failed` count means at least one entry is **not replayable yet**: its retry budget is spent but the dead-letter write has not completed, so there may be no message on the topic at all, and replay refuses it with `EVENT_NOT_DEAD_LETTERED`. **Restore broker reachability so the dead-letter write completes**; the relay re-claims those rows itself. Only then replay.
3. **List the entries** for that topic — [Step 1](#step-1--list-the-dead-lettered-events).
4. **Read the failure metadata** and bound the failure window — [Step 2](#step-2--read-the-failure-metadata).
5. **Fix the underlying cause** — [Step 3](#step-3--decide). Replaying before the cause is fixed re-dead-letters the event and buys nothing.
6. **Replay** — [Step 4](#step-4--replay). This is the step that clears the alert, and it is the only one that can: the replay is what takes the entry out of the gauge's inventory, and it does so by making the row `dispatched`. There is no follow-up call — see [Step 5](#step-5--there-is-no-resolve-step).
7. **Confirm the gauge returns toward zero.** The collector re-reads it from authoritative state on every tick and records an **explicit zero** when a topic's inventory is empty; **that zero is the reading that clears the alert.** A gauge still above the threshold after a successful replay means entries remain — either rows still in `failed`, whose dead-letter write has yet to land, or `dead_lettered` rows you have not replayed.

### SubscriberConsumerLagHigh

```text
expr:     blnk_kafka_consumer_lag > 10000
for:      2m
severity: warning
```

A warning rather than a page: the events are durably in Kafka and a consumer catches up on its own, so this is degradation, not loss. The two-minute dwell is four consecutive evaluations — consumer lag is legitimately spiky, and a short dwell avoids paging on a burst that self-corrects.

**Blnk does not manage subscriber consumers.** More often than not the remedy is to contact the subscriber rather than to change anything in Blnk. Work the checks that are yours first, then hand it over with evidence.

1. **Identify the subscriber, group and topic** from the `subscriber`, `group` and `topic` labels. The first two are **pseudonyms** — a stable, truncated SHA-256 of the registry identifier — never customer-chosen names, because an annotation is rendered into notifications and incident tickets. Resolve one with the registry's own resolver, which searches every page rather than the one you ask for:

   ```bash
   curl -sS "$BLNK_API/subscribers?subscriber_id_hash=$TOKEN" \
     --config "$BLNK_CURL_CONFIG" \
     | jq -r '.data[] | "\(.subscriber_id)\t\(.kafka_principal)\t\(.consumer_group_id)"'
   ```

   `GET /subscribers` answers with an **object carrying `data`** on every reading, including this one — never a bare array — so read `.data[]`. Which keys accompany `data` depends on the reading: this one carries `total_count` and `has_more` and **no** `next_cursor`, because it resolves the whole registry rather than a page. A one-element `data` resolved it; an empty `data` with **200** means the registry was searched to its end and holds no such subscriber. `total_count` is exact here, and is present without `include_count`, for that same reason — the resolution is complete rather than paged — and `limit` and `cursor` are **refused** on this reading, because there is no page to bound. **500** `GEN_INTERNAL` means the registry exceeds the resolver's bound of 100 pages × 100 rows, so the answer is **unknown rather than negative** — query the table directly in that case:

   ```bash
   $BLNK_PSQL -c "
     SELECT subscriber_id, kafka_principal, consumer_group_id
     FROM blnk.event_subscribers
     WHERE substring(encode(sha256(subscriber_id::bytea), 'hex') for 16) = '$TOKEN';"
   ```

   **Do not resolve a token by fetching `?limit=100` and hashing the rows yourself.** That was the procedure before the resolver existed, and it fails silently once the registry exceeds one page: it reports "no match" for subscribers it never read, and the wrong conclusion — that the alert names a subscriber which no longer exists — closes a live incident. To hash a candidate identifier outside the API, the rule is SHA-256 over the exact bytes, hex, first 16 characters: `printf '%s' "$SUBSCRIBER_ID" | sha256sum | cut -c1-16`. Use `printf`, not `echo`: a trailing newline changes the digest.

   Service logs carry the same token as `subscriber_id_hash`, consumer groups as `consumer_group_hash` and Kafka principals as `principal_hash`, so a log line, a metric series and a registry row all pivot on one value.

   Three collapse tokens can appear instead of a pseudonym on a **metric label**, and none of them is hashed because none is anyone's name: `unattributed` (the reading named no subscriber), `unregistered` (a value was named but Blnk did not issue it — worth investigating on its own), and `other` on the `topic` label (a topic Blnk does not own). A **log field** never collapses to `unregistered`: it hashes whatever it was given, so two unadmitted identifiers stay distinguishable in a log where they would share one label on a metric.
2. **Is the consumer running at all?** A group that has never committed reports **full lag from the earliest retained offset** rather than zero, by design — a subscriber that never started must not look healthy. So a lag figure close to a topic's whole retained volume usually means "not consuming", not "far behind".

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --describe --group "blnk-sub-$SUBSCRIBER_ID.default"
   ```

   Read `CONSUMER-ID` and `HOST`: empty means no member is connected. A group perpetually in `PreparingRebalance` or `CompletingRebalance` is thrashing — usually a consumer whose processing exceeds `max.poll.interval.ms`, which is the subscriber's setting to fix.
3. **Can it still read?** A grant narrowed or revoked since the consumer last connected produces lag that will never drain. Confirm the bindings still exist — [Verifying a subscriber's access](#verifying-a-subscribers-access) — and that the group it is using is inside its `blnk-sub-<subscriber_id>.` namespace. Joining a group outside that namespace is refused.
4. **Is it consuming slower than Blnk publishes?** Compare the lag trend against `blnk_events_published_total` for that topic — the *write* counter is the right one here, because lag is measured in records on the partition and a redelivery is a real extra record the consumer has to read. Rising lag on a flat publish rate is the consumer; rising lag on a rising publish rate may simply be a burst.
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

1. **Find the affected rows.** They are not attributed in the alert on purpose — a subscriber label would export a tenant identifier into every notification — so the first step is to ask the registry which subscribers are affected. The management API answers it in one request:

   ```bash
   curl -sS "$BLNK_API/subscribers?revocation_pending=true" \
     --config "$BLNK_CURL_CONFIG" \
     | jq -r '.data[] | "\(.revocation_pending_at)\t\(.subscriber_id)\t\(.kafka_principal)"'
   ```

   The scan covers the **whole** registry rather than the page you asked for, and returns the rows **oldest obligation first** — the same order the alert fires on, so the longest exposure is at the top. Each row carries `revocation_pending_at` (how long) and `kafka_principal` (what has to be revoked).

   **If it answers `500`, do not read that as "none".** The message says the registry is larger than the scan's bound. A partial list of live, unaccounted-for credentials reads exactly like a complete one, and acting on it would leave the rest authenticating while the incident looked closed — so the scan refuses rather than truncating. Fall back to the table:

   ```bash
   $BLNK_PSQL -c "
     SELECT subscriber_id, kafka_principal, consumer_group_id, revocation_pending_at,
            now() - revocation_pending_at AS outstanding_for
     FROM blnk.event_subscribers
     WHERE revocation_pending_at IS NOT NULL
     ORDER BY revocation_pending_at;"
   ```

   The row still exists precisely so the failure is recoverable: deregistration marks the row, revokes at the broker, and deletes the row only once the revocation is confirmed. A row carrying this timestamp therefore names a principal that may still authenticate. A pending row is not an active subscriber — credential issuance refuses for it.

2. **Try the automatic settlement paths first**, because both are safe and both settle the marker as a side effect. **Re-issuing** replaces the orphaned credential by construction; **deprovisioning** revokes it. Pick whichever matches the subscriber's actual status — re-issue if they should have access, deprovision if they should not:

   ```bash
   umask 077
   resp="$(mktemp)"; trap 'rm -f "$resp"' EXIT INT TERM

   # Re-issue: the subscriber should keep access. Destructive to its previous secret,
   # so capture the new one to a protected file rather than to the terminal.
   curl -sS -X POST "$BLNK_API/subscribers/$SUBSCRIBER_ID/kafka-credentials" \
     --config "$BLNK_CURL_CONFIG" -o "$resp"
   jq -re .password < "$resp" | vault kv put -mount=secret "blnk/$SUBSCRIBER_ID" password=-

   # Or deprovision: the subscriber should have no access. Retrying this finishes a
   # revocation that failed halfway.
   curl -sS -X DELETE "$BLNK_API/subscribers/$SUBSCRIBER_ID" \
     --config "$BLNK_CURL_CONFIG" -o /dev/null -w '%{http_code}\n'
   ```

3. **Otherwise revoke by hand**, using the `kafka_principal` from the row above:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --alter --delete-config SCRAM-SHA-512 \
     --entity-type users --entity-name "blnk-sub-$SUBSCRIBER_ID"
   ```

4. **Confirm the credential is gone** — the `--describe` form of the same command should report no SCRAM entry for the principal — and that `blnk_subscribers_revocation_pending` returns to zero, which is its normal reading.
5. **Then find out why the binding failed.** A `CLUSTER_AUTHORIZATION_FAILED` on `CreateACLs` means the administrative principal is not in `super.users` or has lost its grants; a transport error means the broker was unreachable mid-operation. Fix that before the next issuance, or the next one leaves the same marker.

### SubscriberCredentialOrphaned

```text
expr:     blnk_subscribers_oldest_credential_orphan_age_seconds > 3600
for:      0m
severity: critical
```

**A credential exists at the broker that Blnk neither recorded nor revoked.** Provisioning writes the SCRAM credential before the ACL bindings, because a binding for a principal that does not exist is inert while a credential with no bindings still authenticates. So a failure to record the issuance is compensated by revoking the credential — and when that compensation also fails, a means of authenticating to the event bus exists for a principal the registry records no issuance for.

**This is not `SubscriberRevocationOutstanding`, and the remedy is the opposite one.** There, the subscriber is on its way out and access must be taken away. Here the subscriber is still ACTIVE and its recorded fingerprint simply does not describe the credential that works.

1. **Find the rows.** `GET /subscribers` projects `credential_orphaned_at`.
2. **Settle it automatically, and prefer this.** Either replaces or removes the orphan without a CLI:
   - `POST /subscribers/{id}/kafka-credentials` — Kafka stores one credential per principal, so a new issuance replaces the orphan by construction. Choose this if the subscriber should keep access.
   - `DELETE /subscribers/{id}` — revokes it. Choose this if it should not.
3. **Only if neither can run**, revoke by hand:

   ```bash
   kafka-configs --bootstrap-server "$KAFKA_BROKERS" --command-config /tmp/admin.properties \
     --alter --delete-config 'SCRAM-SHA-512' --entity-type users --entity-name <kafka_principal>
   ```

4. **Verify** the marker clears on the next collection tick, then confirm the principal can no longer authenticate.

### SubscriberRevocationRefused

```text
expr:     blnk_subscribers_oldest_revocation_failure_age_seconds > 900
for:      0m
severity: warning
```

**The broker refused the last revocation attempt, and no attempt has been made since.** This is the fact `SubscriberRevocationOutstanding` cannot carry on its own: **retrying alone will not help.** The marker is cleared at the start of every new attempt, so a value here means the failure is the most recent thing that happened.

1. **Read why.** A `CLUSTER_AUTHORIZATION_FAILED` means the administrative principal is not in `super.users` or has lost its grants; a transport error means the broker is unreachable. The service log carries the classified reason.
2. **Fix that first.** Restore the admin principal's cluster authority, or broker reachability.
3. **Then retry** with `DELETE /subscribers/{id}`, which is idempotent at the broker.
4. **Expect the other alert too.** These rows also carry `revocation_pending_at`, so `SubscriberRevocationOutstanding` pages on them once the debt passes an hour. Clearing the cause clears both.

### SubscriberSettlementOutstanding

```text
expr:     blnk_subscribers_oldest_settlement_age_seconds > 3600
for:      0m
severity: warning
```

**A subscriber's Kafka state has drifted from its registry row and nothing has closed the gap.** A subscriber's state lives in two systems that cannot be written atomically: the row in PostgreSQL, and the principal, credential and ACL bindings at the broker. Three operations span both — issuing a credential, changing an authorization, deregistering — and a request that fails part-way leaves them disagreeing. Each such failure records a durable obligation on the row, and a background pass in the **server** role discharges them. This fires when that pass is not managing it.

It is `warning` rather than `critical` because `SubscriberRevocationOutstanding` above already pages on the credential exposure, and what remains is a correctness problem rather than an open door. It fires on the **age** for the same reason that rule does: an obligation discharged within a minute is the mechanism working.

1. **Read the security-relevant half first.** Two obligations exist and they need different responses. A credential cleanup means a SCRAM credential may exist that Blnk intended to destroy, or the row names one that no longer works. A grant reconciliation means the broker's ACL bindings may not match `authorized_topics`.

   ```promql
   blnk_subscribers_credential_cleanup_pending    # read this one first
   blnk_subscribers_grant_reconcile_pending
   ```

2. **Find the affected rows, with the reason each pass failed.** They are not attributed in the alert on purpose — a subscriber label would export a tenant identifier into every notification — and the registry API does not project the obligation columns, so read them from the table. `settlement_last_error` is the sanitized reason the last pass gave up:

   ```bash
   $BLNK_PSQL -c "
     SELECT subscriber_id,
            kafka_principal,
            grant_reconcile_pending_at,
            credential_cleanup_pending_at,
            settlement_attempts,
            settlement_last_attempt_at,
            settlement_last_error
     FROM blnk.event_subscribers
     WHERE grant_reconcile_pending_at IS NOT NULL
        OR credential_cleanup_pending_at IS NOT NULL
     ORDER BY LEAST(
       COALESCE(grant_reconcile_pending_at, credential_cleanup_pending_at),
       COALESCE(credential_cleanup_pending_at, grant_reconcile_pending_at));"
   ```

3. **Read `settlement_last_error` before doing anything by hand.** It is almost always the broker: a transport error means it was unreachable, and a `CLUSTER_AUTHORIZATION_FAILED` means the administrative principal has lost its grants or is no longer in `super.users`. Fixing that is usually the whole remedy — the next pass then discharges the backlog on its own, and no manual step is needed.

4. **Nothing here requires a manual broker change, and doing one is rarely the right move.** The pass is idempotent: it revokes-and-clears for a credential cleanup, and reconciles the broker to the row for a grant reconciliation. Both remedies are exactly what an operator would do by hand, and both are safe to repeat. Prefer letting it run.

   The two operator-facing actions that also settle an obligation, when one matches the subscriber's actual status:

   ```bash
   # Re-issue: the subscriber should keep access. This REPLACES the SCRAM credential, so it
   # satisfies a pending credential cleanup by construction. Destructive to its previous secret.
   curl -sS -X POST "$BLNK_API/subscribers/<subscriber_id>/kafka-credentials" \
     --config "$BLNK_CURL_CONFIG"

   # Re-apply the authorization: this performs the same prune-then-grant the pass would, so it
   # settles a pending grant reconciliation. Send the topic list the row already records.
   curl -sS -X PUT "$BLNK_API/subscribers/<subscriber_id>" \
     --config "$BLNK_CURL_CONFIG" -H 'Content-Type: application/json' \
     -d '{"authorized_topics":["blnk.transactions"]}'
   ```

5. **Confirm the gauges return to zero**, which is their normal reading, and that `blnk_subscribers_obligations_settled_total` moved — a backlog that cleared without the counter moving means the rows were edited rather than settled.

**One case is discharged without being acted on, deliberately.** A row carrying `revocation_pending_at` is being deregistered, so reconciling the broker *to* it would re-create the grants the deregistration is removing. The pass therefore clears the grant obligation and leaves the revocation tombstone as the outstanding work — that tombstone is itself durable and indexed, and it is what `SubscriberRevocationOutstanding` above reads.

### SubscriberSettlementNotProgressing

```text
expr:     blnk_subscribers_settlement_outstanding > 0
          and rate(blnk_subscribers_obligations_settled_total[30m]) == 0
for:      30m
severity: warning
```

**The settlement pass is not running, or cannot make progress.** This is the companion to the rule above and it fires half an hour earlier by design: that one needs an obligation to have gone stale, while this one fires as soon as work is outstanding and *nothing at all* is being discharged. So "the mechanism is broken" arrives before "this obligation is stale" rather than with it.

Both halves of the expression are required. A non-zero backlog alone is normal — an obligation raised a minute ago is expected to be outstanding — and a zero settlement rate alone is the healthy steady state, because most deployments never fail a subscriber operation at all.

1. **Check the pass started.** It runs in the **server** role, beside the relay and the retention sweeper, and it declines to start when `KAFKA_BROKERS` is empty. Look for one of these two lines at start-up:

   ```text
   subscriber settlement processor started
   subscriber settlement is inactive because no Kafka broker is configured; there is no
     broker-side subscriber state to reconcile
   ```

   The second line is a legitimate steady state — but not one to be in while subscribers hold credentials, which is precisely the situation this alert describes. If you see it, the server role is running without the broker list the subscribers were provisioned against.

   A third line means a broker *is* configured and the pass still could not start:

   ```text
   a Kafka broker is configured but the subscriber settlement processor could not start
   ```

2. **Check the pacing.** A pass leaves a failing row alone for five minutes between attempts, so a single stuck subscriber legitimately produces a settled rate of zero over shorter windows. Thirty minutes is six retry intervals, so a zero rate over that window is not pacing.

3. **Read `settlement_last_error` on the outstanding rows** with the query in the section above. If every row carries the same broker error, this is one fault rather than many, and fixing it clears the whole backlog.

4. **Confirm the pass is reaching the rows at all.** `settlement_attempts` incrementing with `settlement_last_error` populated means the pass is running and the broker is refusing. `settlement_attempts` staying at zero means the pass is not reaching them, which points back to step 1.

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

### SubscriberLagCoverageStale

```text
expr:     max_over_time(blnk_kafka_consumer_lag_pass_age_seconds[30m]) > 600
for:      0m
severity: warning
```

**The rotation is slower than the reading it refreshes.** Lag is measured for a rotating slice of the registry and each reading is retained for ten minutes, so a subscriber the rotation does not return to inside that window has its reading EXPIRE — its `blnk_kafka_consumer_lag` series stops being exported, and an absent series breaches no threshold. `SubscriberConsumerLagHigh` therefore reports nothing wrong for exactly the subscribers it can no longer see.

This is the quieter sibling of `SubscriberLagCoverageIncomplete`: there, a subscriber was never reached in this pass; here, it is reached, but too late for its reading to still be live when the next one lands.

`max_over_time` rather than the instantaneous value, because the gauge resets to zero whenever a rotation completes — the maximum over the window IS the rotation latency, and the instantaneous value is only wherever the last scrape happened to land.

1. **See how much of the registry is currently exported.** Compare `blnk_kafka_consumer_lag_covered_subscribers` with `blnk_subscribers_registered`.
2. **Measure more per tick**, which is the usual answer: raise `EVENT_METRICS_SUBSCRIBER_BUDGET` so one sweep covers more rows.
3. **Or tick more often**, if broker round trips rather than the wall clock are the constraint.
4. **Rule out failure as the cause.** A rotation that is slow because measurements are FAILING shows on `blnk_kafka_subscribers_unmeasured{reason="measure_failed"}` and needs the broker or the ACLs, not the budget.

### SubscriberLagCoverageIncomplete

```text
expr:     blnk_kafka_consumer_lag_inventory_complete == 0
for:      30m
severity: warning
```

**Some subscribers were not measured at all.** Measuring one subscriber's lag costs two broker round trips per authorised topic, so a sweep examines at most `EVENT_METRICS_SUBSCRIBER_BUDGET` registry rows (default 200). A registry larger than the budget is not permanently truncated — the next sweep resumes where the last one stopped, so coverage **rotates** — but while this fires, **absence of a subscriber's lag series means nothing**: it may be caught up, or it may simply not have been looked at.

`ConsumerLagMeasurementDegraded` is the different condition: there, a subscriber *was* measured and a partition would not answer. Here, the subscriber was never reached.

The thirty-minute dwell is the longest in this group deliberately. Rotation is the designed behaviour, so a single incomplete sweep is not a fault; what this catches is a registry that has outgrown the budget for long enough that a subscriber's lag is stale by more than a few sweeps.

1. **Decide whether it is size or failure.** Read the collector's own log line: it reports the subscribers examined, the budget, and where the cursor resumed. A registry comfortably inside the budget that still reports incomplete coverage is a failure, not a size problem — check `EventMetricsCollectionFailing` too.
2. **Raise the budget if the registry has genuinely grown**, remembering the cost is round trips per topic per subscriber per tick:

   ```bash
   # In .env, then restart the server role.
   EVENT_METRICS_SUBSCRIBER_BUDGET=500
   ```

   The value is clamped to a ceiling; a value above it is corrected with a warning rather than refused, so an over-large setting never prevents start-up.
3. **Or lengthen the collection interval** instead, if the broker round trips are the constraint rather than the wall clock.
4. **While it holds, read lag for an unmeasured subscriber by hand** with `kafka-consumer-groups.sh --describe`.

### EventMetricsCollectionStale

```text
expr:     blnk_event_metrics_last_collection_age_seconds > 120
for:      2m
severity: warning
```

**The collector has not completed a pass recently.** Every gauge in this group is refreshed from authoritative state on each tick; a synchronous gauge retains its last value, so a collector that has stopped ticking leaves **every gauge frozen at its last reading while looking perfectly healthy** — the dead-letter age stops rising, the outbox backlog stops moving, and each of the other rules quietly stops being able to fire. This rule is what makes that observable rather than inferred.

The interval is 15 seconds, so 120 is eight missed ticks: long enough that a slow pass or a single hung dependency does not fire it, short enough that a stopped collector is caught inside the dead-letter rule's own 15-minute window.

1. **Check whether the pass is slow or stopped.** The collector bounds each tick and each dependency call, so a hung dependency times out rather than freezing the loop. A rising age with the process alive means passes are exceeding the interval.
2. **Confirm the server role is running.** The collector lives in the server role only; the worker role does not run one.
3. **Look for the collection error series.** `EventMetricsCollectionFailing` fires when passes are running but failing, which is a different fix.
4. **Treat every other gauge in this group as UNRELIABLE while this fires.** That is the point of the rule.

### EventMetricsCollectionFailing

```text
expr:     blnk_event_metrics_last_success_age_seconds > 300
for:      0m
severity: warning
```

**Passes are running but not succeeding.** The distinction from `EventMetricsCollectionStale` is exact: there, the collector is not completing passes at all; here it is completing them and each one is failing, so the last *successful* refresh recedes while the last *attempt* stays current.

The success age is anchored at the collector's first attempt rather than left absent until the first success. Without that anchor, "failing since start-up" — a wrong DSN, an unreachable broker, a revoked administrative grant — was the one state this rule could never detect, because the series it matches on did not exist yet.

1. **Read the failure series** to see which dependency is refusing: the outbox read, the dead-letter age query, the registry listing, or the broker.
2. **Raise the log level to `debug`** to get the per-tick summary, which names the failing step:

   ```bash
   BLNK_LOG_LEVEL=debug
   ```

   For the dependency's own error text, `trace` — the raw text is deliberately not in the standard log, because a Kafka or PostgreSQL error names brokers, listeners, schema objects and constraints.
3. **Check the administrative grant** if only the lag portion fails: `OffsetFetch` and `ListOffsets` are performed by the administrative principal.

### EventMetricsCollectionAbsent

```text
expr:     absent(blnk_event_metrics_last_collection_age_seconds{job="blnk-server"})
for:      10m
severity: critical
```

**Nothing is reporting.** Every other rule in this group needs its series to exist in order to fire, so a scrape that returns nothing at all silences the whole group — and silence is indistinguishable from health. `absent()` is the only construction that alerts on the absence itself.

**It is scoped to `job="blnk-server"` deliberately, and that scoping is load-bearing**: the collector runs in the server role only, so an unscoped `absent()` would be satisfied by the worker's scrape and never fire. The consequence to know about: **renaming that job in `prometheus.yml` silences this rule permanently, with nothing to indicate it.** If you rename the job, rename it here too.

Critical rather than warning, because it means the pipeline's entire observability surface is dark.

1. **Is the target up?** `http://localhost:9090/targets` — a scrape failure is the common cause, and an authentication failure is the common scrape failure. See the note at the end of *[Is the alert armed at all?](#is-the-alert-armed-at-all)*: with `metrics_bearer_token` set and no matching `authorization:` block, the target is down and every rule here sits permanently unable to fire.
2. **Is observability armed?** The metrics endpoint is only registered when observability is enabled; a deployment that never set it exposes no `/metrics` at all.
3. **Is the job name still `blnk-server`?** See above.
4. **Is the server role running?** The worker role publishes no collector series.

### How the lag figure is produced

**Consumer lag is measured in process, and no external lag exporter is part of this deployment.** `event_admin.go` differences each group's committed offsets, read with `OffsetFetch`, against the partition end offsets, read with `ListOffsets`, sums them per topic, and publishes the result on `blnk_kafka_consumer_lag`. There is no `kafka_exporter`, no Burrow and no sidecar to deploy or keep in step with the registry, so do
not go looking for one.

**If the lag series are absent, work down this list in order — a stopped collector is the LAST
explanation, not the first.** Because the two lag gauges are asynchronous, three ordinary conditions omit
them entirely:

1. **Nothing to measure.** No subscriber is registered, or none holds a topic grant. Check
   `GET /subscribers`.
2. **The measurement was withheld.** A topic whose partitions could not all be read is withheld rather
   than reported at a partial sum. Check `blnk_kafka_consumer_lag_unmeasured_partitions` — non-zero
   confirms this case, and it is why that gauge exists.
3. **Kafka is not configured.** With an empty broker list there is no admin client to read offsets from,
   so only the two lag gauges disappear while the backlog, dead-letter-age and revocation gauges keep
   publishing.
4. **Only then, the collector.** Check the logs and whether observability is enabled at all.

The quick discriminator: **if other `blnk_` series are present and only the lag series are missing, it is
one of the first three.**

Two edge cases are decided explicitly rather than left to arithmetic:

- **No committed offset** (`OffsetFetch` reports `-1`) → the baseline becomes the partition's **earliest retained** offset, so the lag is every record still on the log. Not zero, which would make a subscriber that never started look perfectly healthy; and not zero-based, which would invent lag for records retention has already deleted.
- **A committed offset at or beyond the end** → clamped to zero. It is legitimately transient, since the two offsets are read in separate round trips and a commit can land in between, and the naive subtraction would produce a negative lag.

### Is the alert armed at all?

**A rule file is inert unless `prometheus.yml` lists it.** The `rule_files:` stanza is what arms the rules; the mere presence of the file is not.

```yaml
rule_files:
  - '/etc/prometheus/alerts/*.yml'
```

That is a path **inside the Prometheus container**. The Compose `prometheus` service bind-mounts this repository's `./alerts` directory to `/etc/prometheus/alerts:ro`, and the glob resolves against it — so a rule file added there later needs no further edit, and the mount and the glob must be kept in step.

Verify, in this order:

```bash
docker compose --profile monitoring up -d prometheus
```

1. **Open `http://localhost:9090/rules`.** The group `blnk-kafka-alerts` must be listed with all **thirteen** rules and a 30-second evaluation interval. **Check `/rules`, not `/targets`** — a target can show `UP` while the rules never loaded, and a rule that never loaded reports no error anywhere.
2. **Validate the file before you ship a change to it**, which also catches a bad glob:

   ```bash
   docker compose exec prometheus promtool check rules /etc/prometheus/alerts/blnk-kafka-alerts.yml
   docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
   ```

3. **Confirm the series exist.** A rule whose series is never recorded can never fire, and that is indistinguishable from a healthy system:

   ```bash
   curl -sS "http://localhost:9090/api/v1/query?query=blnk_dlt_oldest_message_age_seconds" | jq '.data.result | length'
   ```

4. **On Kubernetes, assert that Prometheus has targets at all.** This step has no Compose equivalent and it is the one that catches the failure the two steps above cannot see:

   ```bash
   kubectl -n blnk exec deploy/prometheus -- \
     wget -qO- http://localhost:9090/api/v1/targets | jq '.data.activeTargets | length'
   ```

   **The answer must be greater than zero, and every target's `health` must be `up`.** Kubernetes discovers its scrape targets from the API server rather than from static DNS names — the server and worker are autoscaled, and a Service target load-balances, so a static address would reach one arbitrary replica per scrape and collapse every per-process counter into one series that appears to reset. Discovery is therefore correct, and it costs a prerequisite: the Prometheus pod needs `serviceAccountName: prometheus` **and** `automountServiceAccountToken: true` (both are in `prometheus-deployment.yaml`) so that the ServiceAccount and Role in `prometheus-rbac.yaml` actually apply to it.

   With either missing, Prometheus logs `Cannot create service discovery` once at startup — or is refused by the API server with a 403 — and then serves normally with **zero** targets. The pod is `1/1 Running`, `/-/ready` is green, `/rules` lists all fourteen rules, and nothing is collected, so every rule evaluates against nothing and none can ever fire. **That is why both `/rules` and `/api/v1/targets` have to be checked on Kubernetes: a loaded rule over an absent series and a healthy system are the same observation.** Confirm the credential is present and sufficient with:

   ```bash
   kubectl -n blnk exec deploy/prometheus -- ls /var/run/secrets/kubernetes.io/serviceaccount/token
   kubectl -n blnk auth can-i list pods --as=system:serviceaccount:blnk:prometheus
   ```

> **If `metrics_bearer_token` is set, the scrape fails and every rule sits permanently unable to fire.** Prometheus shows the target down; the rules report nothing wrong. `prometheus.yml` carries the two commented `authorization:` blocks and the mount instructions that fix it — use `credentials_file`, never inline `credentials`, so the token stays out of the committed file and can be rotated without a restart.
>
> **The configuration exists twice.** Kubernetes has no bind mounts, so both `prometheus.yml` and the rule file travel as data in `infrastructure/k8s-manifests/prometheus-configmap.yaml`. **Edit both copies together** — two hand-maintained copies of an alerting configuration diverge silently, each staying valid and reading correctly in review, with one environment alerting and the other not. Tests compare the two, so a lone edit fails rather than shipping.

## Relay Operations

**The event relay runs in the server process role**, started immediately after the fund-lineage outbox processor it is modelled on — this repository's established home for an outbox relay, which also avoids standing up a fourth asynq server for one poll loop. There is no separate relay binary and no relay subcommand: `blnk start` *is* how the relay is run. `make run_relay` is an alias for the server role, provided so that "where does the relay run" is answerable without reading `cmd/server.go`, and so the relay can be run in isolation for a load test or while watching a backlog drain. It loads `.env` the way `make kafka_provision` does — the file supplies defaults, the caller's environment wins — and reports the broker list it resolved from `KAFKA_BROKERS`, the `BLNK_KAFKA_BROKERS` alias, or `blnk.json`. **Finding none is a refusal: the target prints the four places a broker list can be set and exits non-zero without starting anything.** It fails fast deliberately, because the alternative failure is silent — a server that comes up healthy, serves the API, captures events into `blnk.event_outbox` and publishes none of them. That is the one outcome worth a hard stop, and it is a stop only in this target: `blnk start` itself treats an empty broker list as the legitimate no-Kafka steady state described under [Running Without Kafka](#running-without-kafka), so nothing here narrows what the typed loader accepts. `config.Fetch` remains the authority on whether a *configured* value is valid.

**`make run_relay` reads `.env`, so the ordinary workflow needs nothing exported.** `./stack.sh --init` writes `KAFKA_BROKERS` and the producer pair into a mode-0600 `.env`, and the target sources that file — then replays the caller's own environment on top, so **anything you pass on the command line wins** and `.env` supplies only what you did not. Reading the file is what makes the refusal above honest: those assignments are not exported into your shell, so a target that consulted only the environment would refuse the operator who had just followed its own setup instruction.

```bash
make run_relay                              # broker list from .env
KAFKA_BROKERS=localhost:29092 make run_relay # this wins over .env, for one run
KAFKA_BROKERS= make run_relay                # deliberately empty: refused, not defaulted
```

Nothing else reads that file for you. Neither `make` nor the `blnk` binary loads `.env` on its own — configuration reaches the process through `envconfig`, which reads the environment and no file — so `./blnk start` invoked directly needs the variables exported yourself:

```bash
set -a; . ./.env; set +a
./blnk start
```

Skip that and the server comes up looking entirely healthy while publishing nothing, because the relay start is conditional on brokers being configured.

**The start is conditional on brokers being configured.** With an empty broker list the relay logs one info line and starts nothing — see [Running Without Kafka](#running-without-kafka).

| Parameter | Value | Why it matters operationally |
|-----------|-------|----------------------------|
| Batch size | 100 rows per claim | The unit of work. A backlog drains in multiples of this per tick. |
| Poll interval | 1 second | The idle latency floor. A row captured just after a tick waits up to a second before its first attempt, which is why the end-to-end latency series is read from `blnk_events_capture_to_dispatch_duration_seconds` and not from the broker-write series. |
| Lock duration | 30 seconds | The lease a claim takes on its rows. **This is the recovery mechanism.** |

Rows are claimed with a CTE using `FOR UPDATE SKIP LOCKED`, ordered by occurrence, which is what makes concurrent relay instances safe without losing FIFO order. The claim additionally returns **at most one row per partition key**, across all instances rather than merely within one, so two events sharing a key can never be in flight simultaneously — that is what makes per-aggregate ordering hold when the relay is scaled out.

One consequence matters when you are triaging, and its limit matters just as much. **An event still being retried holds up later events sharing its partition key**, even ones in another category, because the claim excludes a candidate while an earlier same-key row is `pending` or `processing`. **Exhaustion releases the key**: once the row has spent its budget and been dead-lettered it is no longer in either state, so its siblings publish immediately. A dead-letter therefore leaves a **gap** in that key's sequence rather than a stalled queue. Read `DeadLetterMessageStuck` accordingly — it reports one event needing triage and a hole in that key's history, not an accumulating backlog behind it. Check `blnk_outbox_pending` if you want to know whether anything is actually queued.

**The 30-second lease is how a crash recovers.** A relay that dies mid-batch leaves its claimed rows in `processing` with a `locked_until` in the near future; once that expires the rows become claimable again and the next instance picks them up. Nothing has to be reset by hand. The cost is the duplicate window described under [legitimate differences](#step-4--account-for-legitimate-differences-before-declaring-a-discrepancy): a crash between a successful publish and the row being marked leaves the row to be published again, which is exactly why `event_id` deduplication is a subscriber obligation.

Is the relay running and keeping up?

```bash
# One line at start-up naming the batch size, poll interval and lease.
docker compose logs server | grep -i 'event outbox relay'

# The backlog: pending plus processing. Rising against a flat publish rate is a
# relay that is not keeping up; it includes claimed-but-unacknowledged rows, so a
# stalled relay holding every row under a lease cannot read as a drained backlog.
# In secure mode /metrics requires the bearer token, so send it. Reading the token from a
# mode-0600 curl config keeps it out of argv and out of shell history.
#   umask 077; printf 'header = "Authorization: Bearer %s"\n' "$BLNK_METRICS_BEARER_TOKEN" > ~/.blnk-metrics
curl -sS --config ~/.blnk-metrics "http://localhost:5001/metrics" | grep '^blnk_outbox_pending'

# Without secure mode enabled the header is simply ignored, so the same command works either way.
# Alternatively, query Prometheus rather than the app: it already holds the token.
#   curl -sS 'http://localhost:9090/api/v1/query?query=blnk_outbox_pending'
```

### Reading the logs: `[redacted]` is the log, not the failure

A transport error in a **log line** has its network topology removed. `dial tcp 10.0.3.14:9092: connect: connection refused` is written as `dial [redacted] connect: [redacted]`-style text — the diagnosis survives, the addresses do not. It is not corruption, and it is not a truncated error.

Three places carry the same failure, and knowing which is which saves a triage:

| Where | Rendering | Why |
|-------|-----------|-----|
| Log line, at `info`/`warn`/`error` | Redacted, in the **`cause`** field | A log is retained, shipped onward and readable by more people than hold the master key. An address in one outlives the incident. |
| Log line, at `debug` | Verbatim, in the **`cause_verbatim`** field, alongside `cause` | An operator who set `BLNK_LOG_LEVEL=debug` has asked for exactly this detail. |
| `last_error` column and the DLT `failure_metadata.error_reason` | **Verbatim, always** | Both readers are already privileged: the column is behind the master-key-gated inventory, and `.dlt` topics cannot be granted to a subscriber. The address is the most useful part of a dead-letter triage, so it is kept. |

So: **triage from the dead-letter inventory, not from the log.** `GET /events/dead-letter` gives you the unredacted `failure_reason` without turning any log level up. Raise the level to `debug` only when the failure is not on a row — a start-up failure, an administrative call, a lease renewal:

```bash
# The verbatim cause for a transport failure that never reached a row.
# Restore the level afterwards: debug also enables a line per published event.
kubectl -n blnk set env deployment/server BLNK_LOG_LEVEL=debug
kubectl -n blnk logs deployment/server | grep cause_verbatim
```

The same rule governs the request log. It records the **route template** (`/transactions/:transaction_id`), never the requested path, so no ledger, transaction or identity identifier reaches it — group by `route` when you are counting endpoint traffic. The `client_ip` field is the peer that opened the connection; it believes `X-Forwarded-For` only from an address named in `BLNK_SERVER_TRUSTED_PROXIES`, which is empty by default. If your deployment sits behind an ingress and every request appears to come from one address, that is the setting to populate — with the ingress's own range, never `0.0.0.0/0`.

> `blnk.event_outbox` and `blnk.lineage_outbox` are **separate tables served by separate relays**, and they are never merged. `NewLineageOutboxProcessor` handles fund lineage; the event relay handles events. The two share a shape — the same batch size, poll interval and lease, the same claim idiom — because the event relay was modelled on the lineage one, and that resemblance is exactly what makes them easy to confuse. Check which table you are looking at before drawing a conclusion.

Retention is a separate, optional sweep. `RELAY_EVENT_RETENTION_DAYS` is `0` — **disabled** — by default, and only terminal rows (`dispatched`, `dead_lettered`) are ever eligible. A `pending`, `processing`, `replaying` or `failed` row is still owed a delivery attempt and is never deleted however old it is; a `failed` row in particular is excluded because this table is the only copy of that event in existence. Bear in mind what an undeleted row holds: the payload is the webhook body verbatim, so a transaction event carries amounts and balance identifiers and an identity event carries names, email addresses, phone numbers, postal addresses and dates of birth. Confirm the sweep is running with `blnk_events_purged_total`, but read it **against eligibility**: a flat
counter most often means nothing is past the retention cutoff yet, and only otherwise means the sweep is
disabled or stuck. Because the sweep deletes in batches, a step-shaped series is its normal signature.

**Do not diagnose accumulation with `blnk_outbox_pending`** — that gauge holds only `pending` and
`processing` rows, so the retained terminal rows this section is about are invisible to it and it stays
flat while the table grows. Measure the retained rows directly:

```bash
psql -X -f - <<'SQL'
SELECT status, count(*) AS rows,
       pg_size_pretty(pg_total_relation_size('blnk.event_outbox')) AS table_size
FROM blnk.event_outbox
GROUP BY status
ORDER BY rows DESC;
SQL
```

`GET /events/stats` reports the same per-status counts if you would rather not touch the database.

### Purge Capacity — Check It Against Your Arrival Rate

The retention *period* says how long a terminal row is kept. Two further settings say how fast that period is actually **enforced**, and a period configured without regard to them is not enforced at all. The sweep runs hourly, so:

```text
rows deleted per hour = RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP
                      x RELAY_EVENT_RETENTION_BATCH_SIZE
```

**Capacity below the arrival rate does not slow the table's growth, it permits it.** The sweeper never catches up, the oldest eligible rows are never reached, and `blnk.event_outbox` grows without bound however short the retention period is set. At the throughput this system is validated against — 500 events a second — rows arrive at **1,800,000 an hour**. The shipped defaults (2,000 batches of 1,000) give **2,000,000 an hour**, which clears that. Multiply your own peak rate out and compare before changing either value.

Two things report on this, so it does not have to be worked out from first principles:

- At start-up, whenever retention is enabled, the capacity in force is logged as a single `purge_capacity_rows_per_hour` field alongside its two factors.
- A sweep that exhausts its batch ceiling with rows still eligible logs a **warning** naming both variables. That log line means the period is not being enforced; treat it as the signal to raise capacity.

Which factor to raise is not arbitrary:

| Setting | What it controls | Guidance |
|---|---|---|
| `RELAY_EVENT_RETENTION_BATCH_SIZE` | Rows removed by **one** `DELETE`. The lock-footprint factor: each statement takes row locks and writes WAL for what it removes, on a table the relay is concurrently claiming from. | Keep it small. Raising it trades a longer pause for the relay against fewer statements. |
| `RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP` | How many such statements **one sweep** issues. The throughput factor. | The safer of the two to raise. This is the one to change when capacity is short. |

The ceiling exists rather than being unlimited by default because of one specific case: the **first** sweep after retention is enabled on a long-running deployment, which would otherwise try to delete the entire historical backlog in a single pass. With the ceiling, that backlog drains over successive hourly sweeps instead.

`0` means *unset* and takes the default — it does **not** mean unlimited, because an unset variable is indistinguishable from a deliberate zero and removing the bound is the more dangerous of the two readings. To remove the ceiling for a deliberate one-off catch-up, set it **negative**:

```bash
# One-off catch-up. Still bounded by the ten-minute sweep timeout, so this means
# "delete as much as you can each sweep", not "run until the backlog is gone".
RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP=-1
```

Both variables resolve from either the bare name above or the `BLNK_`-prefixed form, like every other setting in this feature.

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

# 2. The eight topics exist with the local geometry: 6 partitions, factor 1.
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
- **Only the Kafka-dependent gauges are absent — not all of them.** The metrics collector starts
  **unconditionally**, so the gauges it can compute from PostgreSQL alone keep publishing:

  | Gauge | With no brokers |
  |---|---|
  | `blnk_outbox_pending` | **Published** (reads the outbox) |
  | `blnk_dlt_oldest_message_age_seconds` | **Published** (reads the outbox) |
  | `blnk_subscribers_revocation_pending` | **Published** (reads the registry) |
  | `blnk_subscribers_oldest_revocation_age_seconds` | **Published** (reads the registry) |
  | `blnk_kafka_consumer_lag` | **Absent** — needs an admin client to read offsets |
  | `blnk_kafka_consumer_lag_unmeasured_partitions` | **Absent** — same reason |

  The publish and dead-letter **counters** simply never increment, because nothing is published. Do not
  read the absence of the two lag gauges as "the collector is not running"; see the decision tree above.
- **`scripts/kafka-provision.sh` skips and exits 0**, which is what makes it safe on an unconditional bring-up path.
- **The subscriber settlement pass does not run**, and says so once at info level: with no broker there is no broker-side subscriber state that could diverge from the registry. Note the one case where this matters: a deployment that provisioned subscribers against a broker and later started *without* the broker list has obligations that nothing will discharge, and `SubscriberSettlementNotProgressing` is the rule that catches it.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every publish fails with connection refused or no available brokers; `blnk_outbox_pending` climbs | Broker unreachable — wrong `KAFKA_BROKERS`, the `kafka` Compose profile not selected, or the broker down | Confirm `docker compose ps kafka` is **healthy** and that `KAFKA_BROKERS` matches the listener you can actually reach: `kafka:9092` from inside the network, `localhost:9092` from the host. Selecting the profile without setting `KAFKA_BROKERS` gives a broker Blnk ignores; setting the variable without the profile gives a relay retrying against nothing. |
| Topic creation rejected with `INVALID_REPLICATION_FACTOR` | `KAFKA_REPLICATION_FACTOR` exceeds the broker count | Set it to `1` on a single-broker stack, `3` on a replicated cluster. It is configuration precisely because no single literal is right for both. |
| `ErrReplicationFactorInadequate` on an existing topic | The topic sits at fewer replicas than configured | Reassign the topic's partitions to the configured factor, then re-run assurance. The **minimum** replica count across partitions is what is checked, because durability is decided by the weakest one. |
| `ErrPartitionGrowthRefused` | A non-empty topic has fewer partitions than `KAFKA_MIN_PARTITIONS` | Plan the migration: provision a correctly shaped topic, move consumers, drain the old one. `KAFKA_ALLOW_PARTITION_GROWTH=true` performs the growth step **and re-maps keys**, breaking ordering for every key already written — see [Partitions](#partitions). |
| Bootstrap fails, or the broker starts and authenticates nobody | The image predates Kafka 3.5 / Confluent Platform 7.5.0, so `kafka-storage format` has no `--add-scram` | Raise the image tag above the floor. The stack pins `apache/kafka:3.9.2`. Running `kafka-configs` afterwards **cannot** fix it — see [Step 1](#step-1--bootstrap-the-scram-admin-credential-before-the-brokers-first-start). |
| Nothing is restricted: a principal reads a topic it holds no grant for | `authorizer.class.name` is unset, so Kafka permits every request | Set `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` and restart the broker, then re-run the behavioural check in [Verify the authorizer is active](#verify-the-authorizer-is-active-rather-than-trusting-it). **From the data path this failure is silent** — verify behaviourally, assume nothing. |
| ACL commands or credential issuance fail with `SECURITY_DISABLED` | Same root cause: no authorizer is configured, so the ACL admin APIs have nothing to act on | Set the authorizer as above. Do **not** treat the refusal as a Blnk fault or bypass it — it is the guard that stops unrestricted credentials being issued. |
| Every credential is rejected after `kafka_data` was destroyed | The volume held `__cluster_metadata`, so every SCRAM credential and ACL is gone | Re-bootstrap: bring the broker up so `scripts/kafka-bootstrap.sh` reformats and re-seeds the admin credential, then re-run provisioning. **Recovery differs by how the principal was created** — see below. |

#### Which credentials can be recreated, and which are gone for good

The distinction matters after a broker rebuild, because two different mechanisms created the principals.

- **Script-managed local principals — recoverable.** The admin, producer and sample-subscriber
  credentials used by the local stack are generated by `scripts/kafka-bootstrap.sh` and
  `scripts/kafka-provision.sh` and **are persisted** — in the mode-0600 `.env` file and in the secret
  files those scripts write. Re-running provisioning re-seeds the *same* values, so nothing downstream
  has to be reconfigured.
- **API-issued subscriber credentials — unrecoverable.** A password minted by
  `POST /subscribers/{subscriber_id}/kafka-credentials` is returned once and never stored: the registry
  keeps only a non-reversible reference and the issuance instant. These cannot be restored and each
  affected subscriber must be **re-issued** a new credential and told the new value.

So "the old passwords were never stored" is true only of the API-issued ones. Do not delete `.env`
expecting the local principals to be irrecoverable, and do not expect a subscriber's password to be
recoverable from anywhere.
| `SCRAM authentication failed` for a credential you are sure is right | The password contains `,`, `=`, `[` or `]`, and Kafka's `--add-scram` / `--add-config` grammar has no escape sequence, so it was silently truncated | Re-issue with a value from the safe alphabet. The scripts refuse such a value up front by name; a credential set by hand outside them will not. |
| `kafka-init` restarts three times and the stack never comes up | Provisioning is refusing a configuration — a bad credential pair, an impossible replication factor, a rejected variable | Read `docker compose logs kafka-init`. The cap exists so a permanent failure is terminal: the container stays `Exited(1)`, the dependency gate fails fast, and you get one error to read instead of a log growing a fresh copy of itself every few seconds. |
| The publisher refuses to build; the server will not start | Brokers and an administrative pair are configured but no producer pair is | Set `KAFKA_SASL_USER` and `KAFKA_SASL_SECRET`. Publishing as the administrator would make a leaked producer credential a compromise of the cluster's whole authorization state. `KAFKA_ALLOW_ADMIN_PRODUCER=true` is a documented, warned-about escape hatch for a deployment mid-upgrade, not a fix. |
| Both Kafka clients refuse to connect, naming TLS | `KAFKA_TLS_ENABLED` is off and `KAFKA_INSECURE_LOCAL_DEV` is not set | Configure the `KAFKA_TLS_*` block. Only set `KAFKA_INSECURE_LOCAL_DEV` for the local single-broker stack; it is warned about on every configuration load. |
| A subscriber sees records outside its `partition_key_prefix` | Working as designed: the prefix is enforced **consumer-side** and no ACL evaluates a message key | Filter on the key in the consumer, or — if the records must be unreachable rather than filtered — narrow `authorized_topics`, which is a real ACL. See [the partition-key prefix](#the-partition-key-prefix-is-a-consumer-side-filtering-contract). |
| `PUT /subscribers/{id}` with a `partition_key_prefix` answers `500` naming a constraint | The database still carries `event_subscribers_key_scope_chk` | Apply the pending migrations; `sql/1781248930.sql` drops it. See [the partition-key prefix](#the-partition-key-prefix-is-a-consumer-side-filtering-contract). |
| Replay answers `409 EVENT_NOT_DEAD_LETTERED` | The row is `failed` (its dead-letter write is still owed), already replayed, or another replay holds it | Restore broker reachability so the dead-letter write completes, then replay. See [the two states](#the-two-states-and-why-only-one-is-replayable). |
| A subscriber cannot join its consumer group | The group is outside its `blnk-sub-<subscriber_id>.` namespace, or the binding was written without the trailing delimiter | Use a leaf inside the namespace — `blnk-sub-<id>.default` is the issued default. Check the binding is `PREFIXED` on the dot-terminated namespace. |
| The alerts never fire, and nothing looks wrong | The rules are not loaded, or the scrape is refused | Check `http://localhost:9090/rules` — not `/targets` — and the bearer-token note in [Is the alert armed at all?](#is-the-alert-armed-at-all). |

## Related Documents

| Document | Covers |
|----------|--------|
| [event-streaming.md](event-streaming.md) | The subscriber contract: the topic catalogue, the `LedgerEvent` envelope, the event vocabulary, the payload, the delivery guarantees and the idempotency obligation, partitioning and ordering, and the `<topic>.dlt` naming convention |
| [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md) | Migrating off HTTP webhooks: the dual-run timeline, the payload-equivalence guarantee, and what happens at the sunset |
| [metrics.md](metrics.md) | The metric catalogue, the attribute domains and example Prometheus queries |
