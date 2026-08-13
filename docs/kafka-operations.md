# Blnk Kafka Operations Runbook

This is the operator's runbook for Blnk's Kafka event pipeline: how to provision the broker, what the subscriber access model actually enforces, how to triage and replay a dead-lettered event, how to run the daily zero-loss reconciliation, and what to do when one of the alerts fires. It is written as steps to execute rather than as an overview — the subscriber-facing contract (the topic catalogue, the `LedgerEvent` envelope, the event vocabulary and the ordering guarantee) is owned by [event-streaming.md](event-streaming.md) and is referenced here, never restated.

## How to Use This Document

Every rule in `alerts/blnk-kafka-alerts.yml` names this file as its `runbook_url`, and **all 14 of them are listed below** — the table is the complete set, not a selection. `TestKafkaAlertInventory_IsStatedOnceAndAgreesEverywhere` fails if a rule is added without a row here. If you arrived from a notification, go straight to your alert:

| Alert | Response section |
|-------|-----------------|
| `DeadLetterMessageStuck` | [DeadLetterMessageStuck](#deadlettermessagestuck) |
| `EventRepairBacklogStuck` | [EventRepairBacklogStuck](#eventrepairbacklogstuck) |
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
| `EventRelayClaimTimingOut` | [EventRelayClaimTimingOut](#eventrelayclaimtimingout) |

**One further rule exists on Kubernetes only, and is deliberately not a row above.** The `prometheus-configmap.yaml` projection mounts a second group, `blnk-infra-alerts`, holding [KafkaBrokerVolumeFilling](#kafkabrokervolumefilling) — the broker's own disk, read from kubelet series this repository does not publish, so the rule has no Compose counterpart and no entry in `alerts/blnk-kafka-alerts.yml`. Its `runbook_url` resolves to that section. The table above stays exactly the thirteen of the rule file it indexes.

If you arrived for routine work, the four procedures are [Provisioning](#provisioning), [The ACL Model](#the-acl-model), [Dead-Letter Triage and Replay](#dead-letter-triage-and-replay) and [The Daily Outbox-versus-Offset Reconciliation](#the-daily-outbox-versus-offset-reconciliation). If you are deciding how to isolate subscribers from one another, read [Requirement Divergence — Partition-Key Scoping Has No Enforcement Point In This Repository](#requirement-divergence--partition-key-scoping-has-no-enforcement-point-in-this-repository) first: one of the three dimensions of the access model is not enforced by anything shipped here.

> **Two tables, two relays, and they are not the same thing.** `blnk.event_outbox` is the event pipeline's outbox and is served by the event relay. `blnk.lineage_outbox` is the fund-lineage feature's outbox and is served by its own processor; its behaviour is unchanged by anything in this document. They are separate tables with separate relays and are never merged. An investigation aimed at the wrong one will find a healthy table and conclude, wrongly, that nothing is stuck.

## Requirement Divergence — Partition-Key Scoping Has No Enforcement Point In This Repository

**Read this before you design a subscriber access model.** It is a statement about what Blnk does and does not deliver, not an operational procedure, and it is the one gap in the subscriber access model that shipped artefacts cannot close.

The requirement this pipeline was built to states that each subscriber gets a Kafka principal with SASL/SCRAM credentials and **ACLs scoped to its authorized topics, consumer group, and partition-key prefix**. Two of those three dimensions are enforced by the broker. The third is not, and cannot be.

| Dimension | Enforced by | Status |
|---|---|---|
| Authorized topics | The Kafka broker, from `LITERAL` topic ACLs Blnk creates | **Enforced.** A fetch outside the grant is refused with `TOPIC_AUTHORIZATION_FAILED`. |
| Consumer group | The Kafka broker, from a `PREFIXED` group ACL Blnk creates | **Enforced.** A join outside the namespace is refused with `GROUP_AUTHORIZATION_FAILED`. |
| Partition-key prefix | Nothing shipped in this repository | **Not enforced by any artefact here.** Kafka's authorizer has no message-key dimension, so no ACL can express it. |

**Why it cannot be an ACL.** A Kafka ACL names a resource type, a resource name, a principal, a host, an operation and a permission. The record key is not among them. `Read` on a topic is `Read` on every record in that topic, whatever the keys are. This is a property of Kafka's authorization model, not a limitation of the client library or of this implementation, and no configuration of the broker changes it.

**What Blnk does instead, and it is deliberately not a substitute.** Provisioning **withholds record-level `Read`** from any subscriber that records a `partition_key_prefix`, granting `Describe` only, and refuses to mint a credential at all unless the deployment has declared an external key-authorising component and that component has attested the exact binding. The full behaviour is in [The partition-key prefix is enforced outside the broker](#the-partition-key-prefix-is-enforced-outside-the-broker). Every path fails closed: a key-scoped subscriber in a deployment with no declared component receives `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` and holds nothing, and issuance additionally refuses if the principal's complete grant turns out to include topic `Read` or a foreign `ALLOW` binding.

**THE COMPONENT IS NOT IN THIS REPOSITORY.** Blnk ships a client for it — the attestation and withdrawal calls — and nothing else. There is no reference gateway, no proxy, no container image and no data-plane route: Blnk serves no records to subscribers under any configuration. So a deployment that sets `KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway` is declaring a component **it must supply and operate itself**, and until that component exists, key-scoped subscribers get no credential rather than a credential that reads too much.

**The consequence, stated plainly.** Partition-key isolation is **out of product scope**. If you need a subscriber restricted to a slice of a topic by record key, Blnk alone cannot give you one; you must build and run the key-authorising component against the contract in [The control endpoint](#the-control-endpoint-what-blnk-verifies-and-the-contract-your-component-implements). If you cannot, use the boundary the broker does enforce: **narrow `authorized_topics`**, or separate the deployments so the records are not in a shared namespace at all.

**What Blnk verifies even where a component exists is the control plane, never the data plane.** Blnk establishes that the broker will not serve that principal a record, that the component answered an authenticated call, and that it attested this principal against this exact prefix. It cannot observe whether the component then filters records. A component that attests correctly and forwards everything would satisfy every check Blnk makes. Instrument and test it on its own.

## Prerequisites

- **The master key.** Every event and subscriber endpoint in this runbook requires it: a scoped API key cannot reach them, whatever its scope. It travels in the `X-Blnk-Key` header. The examples below assume `BLNK_API` holds the API base URL, default `http://localhost:5001`.

  **The master-key gate is the last of three checks, not the first**, so a caller sees whichever one it fails first — and the code says which. Authentication runs first, then scope resolution for the route's resource, then the gate:

  | Caller | Status | `error_detail.code` |
  |---|---|---|
  | No `X-Blnk-Key` header | `401` | `AUTH_MISSING_API_KEY` |
  | A key that does not match any issued key | `401` | `AUTH_INVALID_API_KEY` |
  | A valid key whose scopes do not cover the route's resource | `403` | `AUTH_INSUFFICIENT_PERMISSIONS` |
  | A valid, correctly scoped key that is not the master key | `403` | `AUTH_MASTER_KEY_REQUIRED` |

  Only the last of those is the gate itself, which matters when you are triaging a `403`: `AUTH_INSUFFICIENT_PERMISSIONS` is a scope to widen on a key that will still be refused afterwards, while `AUTH_MASTER_KEY_REQUIRED` says the key is the wrong *kind* and no scope will help.

  **These endpoints need `BLNK_SERVER_SECURE=true`.** With secure mode off, authentication is skipped for the whole API — so nothing ever marks a request as the master key, and every route in this runbook answers `403 AUTH_MASTER_KEY_REQUIRED` no matter what you send. The ledger's own endpoints stay open in that mode, which is why a deployment can look healthy while none of this surface is reachable.

  **It is required whether or not secure mode is on, and that is deliberate.** `BLNK_SERVER_SECURE`
  decides whether the ordinary API routes authenticate at all — the Compose files ship with it unset,
  so a local stack serves `/ledgers` and friends to anyone — but it does **not** relax these
  endpoints. They resolve the presented `X-Blnk-Key` against `BLNK_SERVER_SECRET_KEY` themselves and
  refuse a caller that presents nothing or presents the wrong value, in either mode. So on a local
  stack `BLNK_SERVER_SECRET_KEY` must be set to run any procedure in this document, and the same key
  must reach the shell as described below; with secure mode on, an unauthenticated request is refused
  one layer earlier, with `401 AUTH_MISSING_API_KEY` rather than `403`.

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
per operator shell before any step in this runbook. Stating it once is deliberate: two copies, with
two different file names and two different ways of reading the key, are how a runbook ends up with
examples that pass the master key on the command line while both copies claim that none do.
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

- **`kafka:9092`** is the in-network listener. It is what the **server** and the `kafka-init` one-shot dial, and it is what you use from inside a container on the Compose network. The **worker** does not dial Kafka at all — it captures event rows into `blnk.event_outbox` and publishes none — so it holds no broker credential and its Compose service waits on nothing Kafka-related.
- **`localhost:9092`** is the host listener. It is published from container port `29092` and bound to `127.0.0.1` — one listener cannot advertise two addresses, so there are two. Use it for host-run tests, a host-run `blnk start` and the k6 scenario.
- **`--command-config`** is not optional. Both client listeners require SASL, so a command without a client configuration fails the handshake and reports something that reads like a network fault. `kafka-console-consumer.sh` takes the same flag on the pinned image; a CLI older than 4.0 spells it `--consumer.config`, which is still accepted on 4.x but prints `Option --consumer.config is deprecated…` **on stdout, ahead of the records**. That is why the two pipelines in this document that feed consumer output to `jq` — the [verbatim `error_reason`](#retrieving-the-verbatim-error_reason) lookup and [step 2 of the id-level audit](#the-daily-outbox-versus-offset-reconciliation) — put `grep '^{'` in front of it. Without the guard `jq` aborts on that one line, emits nothing, and the audit's own pass condition then reads as total message loss.

In production, point `--bootstrap-server` at your brokers and `--command-config` at your own properties file. Do not reuse the local `SASL_PLAINTEXT` file: on that listener the SCRAM exchange and every ledger event travel in clear text, which is acceptable on a loopback-bound single-broker development stack and nowhere else.

## Provisioning

### What gets created

Four category topics and their four dead-letter siblings — **eight topics, and they are the complete inventory**. Blnk writes to no other topic.

| Category topic | Dead-letter topic | Grantable to a subscriber |
|---------------|-------------------|---------------------------|
| `blnk.transactions` | `blnk.transactions.dlt` | Yes |
| `blnk.balances` | `blnk.balances.dlt` | Yes |
| `blnk.identities` | `blnk.identities.dlt` | Yes |
| `blnk.system` | `blnk.system.dlt` | **Only where you declare `KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true`.** Every `.dlt` is operator-only under all configurations |

Every name is composed as `<prefix>.<category>` and `<prefix>.<category>.dlt`, where the prefix is `KAFKA_TOPIC_PREFIX` and defaults to `blnk`. Set `KAFKA_TOPIC_PREFIX=acme` and the whole inventory moves to `acme.transactions` and so on; the category tokens never change. What each topic carries, and why there is a fourth category beyond the three the requirement names, is in [event-streaming.md](event-streaming.md#topic-catalogue).

**No dead-letter topic is ever granted to a subscriber**, under any configuration, so the four `.dlt` names are operator-only. **`blnk.system` is withheld by default too**: it carries `system.error`, whose frozen payload renders verbatim error text naming internal detail, and it is the catalogue's catch-all, so a grant of it also stands over every event type nobody has catalogued yet. That leaves **three grantable names by default** — the three tenant category topics.

`blnk.system` is grantable, but only with two declarations that no single action produces: you set `KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true` on the server, and the subscriber's own grant then names `<prefix>.system`. Either one missing and the create, the update and the credential issuance all refuse, naming the declaration that is absent. The provisioning script never grants it whatever the variable says — a bring-up script makes no entitlement decision — so the sample principal is granted the three tenant topics and nothing more. Grant it when a subscriber genuinely operates the deployment with you; withhold it otherwise, and read `system.error` yourself.

**`ledger.created` is on `blnk.system` too**, so a subscriber that needs ledger events needs that same privileged grant — the one operational consequence of the four-category catalogue you will actually field requests about. Granting it discloses `system.error`'s verbatim internal error text as well, so treat the request as the entitlement decision it is rather than a routine topic addition. See [why there is a fourth category, and what `blnk.system` costs](event-streaming.md#why-there-is-a-fourth-category-and-what-blnksystem-costs), which is the page a subscriber reads.

> Do not "tidy" the inventory to a different count, and do not add a category to it. `model.EventCategory` routes events into exactly these four categories and `event_topics.go` composes exactly these eight names from them. A name provisioning does not create is a name the relay cannot publish to; a name it creates that no code writes to is dead weight in every environment. Giving `ledger.created` a grantable home of its own would need a fifth category here, and that is a contract change to be agreed rather than a tidy-up: the catalogue is a published contract subscribers, grants and dashboards build against.

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

— needs an authenticated connection, so **it cannot create the first credential**. (It is shown here only to be ruled out. Do not copy its shape either: when you do have a running broker, use the file-based form under [Adding a runtime SCRAM user](#adding-a-runtime-scram-user), because `--add-config` places the credential in the process's `argv`.) That is a genuine chicken-and-egg problem, and injecting the credential while the storage is being formatted is the only resolution. Your instinct will be to fix an unauthenticable broker by running `kafka-configs` against it; that will not work, and the time spent discovering so is the reason this paragraph exists.

The ordering is therefore fixed: **bootstrap, then broker, then provisioning.** Per-subscriber principals are not created here — they are added once the broker is up and this credential can authenticate, by `scripts/kafka-provision.sh` locally and by `event_admin.go`'s `AlterUserScramCredentials` in production.

#### Two version floors, and both apply

**Feature floor — Kafka 3.5 (Confluent Platform 7.5.0).** The `--add-scram` flag of `kafka-storage format` was added there, and **earlier releases simply do not have it.** An older image rejects the flag, the format either fails or completes with no credential in the metadata log, and the broker then starts but can authenticate nobody — which surfaces much later as what looks like a wrong password. The script asserts this floor by asking the CLI whether `format` accepts the flag, which is the last point at which a `KAFKA_IMAGE` override below it can still be diagnosed as itself.

**Security floor — the pinned release itself.** The feature floor is the oldest release that *can* run this pipeline; it is not a release to deploy. Four advisories bear on this image and all four now carry an established range. Three are fixed at or below 4.2.0. The fourth, CVE-2026-41115, states an affected range of **4.0.0 through 4.3.0**, which puts the floor at the release pinned below and nowhere lower. Running under the floor means running a known-vulnerable broker that merely happens to boot. `.env.example` carries the per-advisory table at `KAFKA_IMAGE`: what each advisory does, and whether it reaches this deployment.

**The Compose stack and the Kubernetes StatefulSet pin the same release by the same immutable digest:**

```
apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
```

The **digest**, not the tag, is what carries the guarantee. A tag is a mutable pointer: the same `apache/kafka:4.3.1` can be repushed, so two clusters applying an identical manifest weeks apart can run different bytes with nothing in any `git diff` or `kubectl diff` to show for it. A digest is content-addressed, so it cannot. The tag is retained beside it only so the release is readable without resolving the hash — when both are present the runtime resolves the digest and treats the tag as commentary. Pinning production and local development to *one* digest is also what makes local bring-up a rehearsal of the deployed broker rather than an approximation of it.

If you override `KAFKA_IMAGE`, override it **upward**, pin a digest as well as a tag, and check the [Apache Kafka CVE list](https://kafka.apache.org/cve-list) before picking a release rather than assuming anything recent is safe.

> **The feature floor is a compatibility minimum, not a production recommendation.** 3.5 is the version
> at which `--add-scram` exists; it says nothing about whether a release is still maintained. **In
> production, run a release that is currently listed among Apache Kafka's supported releases and is
> patched.** The project maintains roughly the three most recent minor lines, so what qualifies changes
> over time — check the current list rather than trusting a number in this document, the pinned one
> included.

##### CVE-2026-41115: why the pin already clears it, and the ACL review it asks for

This one is singled out because it is the only advisory of the four that lands on the mechanism this
pipeline uses for isolation — per-subscriber GROUP ACLs — and because the remediation it asks for is
not a version bump.

**What it says.** `CONSUMER_GROUP_DESCRIBE` (API key 69) validates the **Describe** operation on the
`GROUP` resource, where Kafka's own documentation and KIP-848 both say **Read**. Two mismatches follow
from that discrepancy: an operator who reads the documentation grants `Read` where `Describe` would
have sufficed — and `Read` on a group is what permits joining and syncing it — while a principal
holding `Describe` but not `Read` can retrieve group metadata the documentation implies it cannot.

**What the ASF determined.** That `Describe` on `GROUP` is the *correct* permission, so the
implementation stands and **the documentation and the KIP are what get corrected**. There is
consequently no patched behaviour to wait for and nothing to contain in a release: the remediation the
announcement asks for is a review of existing group ACLs against least privilege.

**Where the pin sits.** The stated affected range is 4.0.0 through 4.3.0. The digest pinned above is
4.3.1, so this deployment is already above the range — the pin needs no move on account of it, and
this is the paragraph that records that determination rather than leaving the row in `.env.example`
blank.

**The ACL review, performed.** `SubscriberProvisioningRequest.aclEntries` in `event_admin.go` is the
single place a subscriber's bindings are expressed, so the review is a finite one:

| Resource | Pattern | Operations Blnk grants | Exposed by the discrepancy? |
|---|---|---|---|
| `GROUP` | `PREFIXED` on the subscriber's own group prefix | `Read` — exactly one binding, always | **No.** `Read` implies `Describe` in Kafka's ACL model, so the consumer describes its own group either way |
| `TOPIC` (ordinary subscriber) | `LITERAL`, per authorized topic | `Read`, `Describe` | Not a group resource |
| `TOPIC` (key-scoped subscriber) | `LITERAL`, per authorized topic | `Describe` only | Not a group resource |

The exposure the advisory describes requires a principal holding **`Describe` on a `GROUP` without
`Read`**. Blnk issues no such binding — there is one group binding per subscriber and its operation is
`Read`. The converse failure, a consumer with `Read` being refused because the broker checks
`Describe`, cannot occur either, because `Read` implies `Describe`. The `Describe`-only grant that
does exist is on **topics**, for key-scoped subscribers, and topics are not the resource this advisory
concerns.

**What this leaves for you.** Two things, and neither is optional if you bind ACLs outside
`event_admin.go`:

- **Do not grant `Describe`-only on a group expecting it to withhold group metadata, and do not grant
  `Read` on a group merely to permit a describe.** The first does not withhold; the second hands over
  join and sync.
- **Re-run the subscriber-isolation suite rather than reading `kafka-acls --list`.** Listing bindings
  proves what was accepted, not what is enforced — with `authorizer.class.name` unset the ACLs apply
  cleanly and grant nothing, and an isolation test then passes **vacuously**. The suite probes the
  broker for an active authorizer and fails closed when it finds none, which is the only reading of
  "least privilege here" that is worth anything.

#### Moving off the pin: the three checks, and what each proves

Swapping the image and watching the pod go ready proves almost nothing here, because each of the three things this deployment actually depends on fails either *silently* or *late*. Re-run all three before promoting a new release, and run them against the posture the manifest uses — uid 1000, read-only root filesystem — rather than a default container, because two of the three are sensitive to it.

| Check | How | What it proves | Failure signature if skipped |
|---|---|---|---|
| **Format seeds a credential** | `kafka-storage.sh format --add-scram 'SCRAM-SHA-512=[name=…,password=…,iterations=4096]'`, then look for `UserScramCredentialRecord` in the output | The credential reaches the metadata log. In KRaft there is no other way to create the *first* one | Format succeeds, broker starts, and authenticates nobody — which presents as a wrong password |
| **The broker authenticates SCRAM-SHA-512** | Any admin call with `security.protocol=SASL_PLAINTEXT` and `sasl.mechanism=SCRAM-SHA-512` | The listener, the mechanism and the seeded credential agree | Broker reports healthy; every client is rejected |
| **StandardAuthorizer *enforces*** | Bind an ACL, read it back with `kafka-acls --list`, and confirm `authorizer.class.name` is set | ACLs are enforced rather than merely accepted | ACLs apply cleanly and grant nothing. The subscriber-isolation test then passes **vacuously** |

A fourth behaviour is worth re-checking even though it is not a version floor: **`kafka-storage format` exits 1 on an already-formatted directory unless `--ignore-formatted` is passed.** That holds on both 3.9.2 and 4.3.1, and it is why `scripts/kafka-bootstrap.sh` and the StatefulSet's init container both pass the flag — it is what makes a pod restart idempotent rather than a crash loop. Do not remove it.

##### All four, re-run against the digest pinned above

Not against a convenient nearby tag. A local stack is easy to run on an *overridden* `KAFKA_IMAGE`
and then to describe as if the shipped default had been exercised, so the record below is what a
broker started from the pinned digest — no override — actually did. Reproduce it by bringing the
stack up with `KAFKA_IMAGE` unset and repeating each row.

| Check | Observed on the pinned digest |
|---|---|
| **Format seeds a credential** | The format output carried `UserScramCredentialRecord(name='admin', mechanism=2, …, iterations=4096)`, and the metadata log on disk decoded to three `USER_SCRAM_CREDENTIAL_RECORD` entries — the seeded administrator plus the producer and sample-subscriber principals the provisioning script adds afterwards |
| **The broker authenticates SCRAM-SHA-512** | The container's own healthcheck is a SASL/SCRAM admin call, and it reported healthy 20 seconds after start. Topic, ACL and SCRAM-describe calls all authenticated, as did a consumer using the sample subscriber's credential |
| **StandardAuthorizer *enforces*** | `authorizer.class.name` resolved to the KRaft `StandardAuthorizer` with `allow.everyone.if.no.acl.found=false`; `kafka-acls --list` returned 23 bindings rather than `SecurityDisabledException`; and the same subscriber credential that read a granted topic was refused `TOPIC_AUTHORIZATION_FAILED` on a `.dlt` and on `blnk.system`. That both arms were exercised is the point — a refusal alone is also what an unreachable broker produces |
| **`--ignore-formatted` idempotency** | Re-formatting an already-formatted directory exited 1 without the flag, reporting `already formatted`, and exited 0 with it |

Two conditions the table above depends on were held rather than assumed. The whole sequence — format,
broker start, SCRAM authentication, ACL bind and read-back — was repeated under the manifest's own
posture, uid 1000 with a genuinely read-only root filesystem and the compose-rendered broker
configuration, and it authenticated one second after the listener came up. And the bootstrap script
delivered the password through a mode-0600 argument file, so it never appeared in `argv`.

That repetition surfaced something worth carrying into the runbook, because it is the failure this
section exists to warn about. **`fsGroup: 1000` in the StatefulSet is load-bearing, and its absence
fails late and misleadingly.** Run the same posture with the data volume left root-owned and
`kafka-storage format` does not refuse — it reports `AccessDeniedException` on
`bootstrap.checkpoint.tmp` while still printing the `UserScramCredentialRecord` it intended to write,
and the broker then dies at startup with `No readable meta.properties files found`, which reads like
a corrupt volume rather than a permissions one. `fsGroup: 1000` with
`fsGroupChangePolicy: OnRootMismatch` is what makes the volume writable by uid 1000; do not drop
either when adapting the manifest. Note also that a format failure is easy to miss in a script,
because piping the command into `grep` replaces its exit status with `grep`'s — check
`${PIPESTATUS[0]}`, or do not pipe.

The pipeline was then exercised end to end on that broker: the relay assured all eight topics at 6
partitions, claimed a pending outbox row and dispatched it, and the record came back off
`blnk.transactions` keyed by its ledger ID with the envelope byte-identical to the stored one. The
event, ordering, recovery, isolation, dead-letter, dual-delivery, replay-fidelity and zero-loss
suites were re-run against it and reported no failures and **no skips** — a skip here is the failure
mode that matters, because these suites stand down quietly when they cannot reach a broker.

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

`stack.sh --init` writes both into a mode-0600 `.env`, which is the intended local route. It does so by *assignment*, not by placeholder substitution: `.env.example` ships `KAFKA_SASL_ADMIN_USER=` and `KAFKA_SASL_ADMIN_SECRET=` with empty values, and `--init` generates the pair and rewrites those assignments in place — the same mechanism it uses for every **secret** it writes, including the producer's and the sample subscriber's. No Kafka key in `.env.example` carries a `{...}` token for it to replace; `POSTGRES_PASSWORD` is the file's only one. The producer and sample-subscriber *principal names* are the other exception, because the template already carries them: an assignment `--init` finds non-empty is reported and left alone rather than rewritten. Note also what `--init` does **not** write: it sets credentials only, so `KAFKA_BROKERS` and `WEBHOOK_DEPRECATION_SUNSET_DATE` remain yours to set — [The Local Stack](#the-local-stack) lists what it writes, key by key.

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
3. One **sample subscriber** principal (`KAFKA_SAMPLE_SUBSCRIBER_USER`, default `blnk-sample-subscriber`) with `Read` and `Describe` on **three** category topics — `<prefix>.transactions`, `<prefix>.balances` and `<prefix>.identities` — and `Read` on its own prefixed consumer-group namespace.

   **Those three are the whole allowlist this script will grant from.** `<prefix>.system` is not on it — it carries `system.error`, whose payload is an internal error message, and it is the catch-all for any uncatalogued event type. `POST /subscribers/{id}/kafka-credentials` will grant it on a deployment that has declared `KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true`; **this script never will**, whatever that variable says, because a bring-up script makes no entitlement decision. `KAFKA_SAMPLE_SUBSCRIBER_TOPICS` narrows the sample's grant to a subset and refuses anything off the script's allowlist: the system topic, a dead-letter sibling, a category that does not exist, or a topic outside this stack's prefix. Setting the variable *replaces* the default rather than adding to it. Use it when you want a **grantable** topic outside the sample's grant to prove a denial on; a `.dlt` name and `<prefix>.system` are always outside it.

**Both principals' ACLs are RECONCILED, not merely added to.** Each run computes the grant the configuration asks for, then makes the broker hold exactly that: bindings the configuration no longer asks for are **revoked**, missing ones are created, and the end state is read back and compared. This matters because `kafka-acls --add` is idempotent without being convergent — it can only widen. Three ordinary changes therefore used to take no effect at all, each leaving the broker serving more than the configuration described while the run reported success:

- removing a category from `KAFKA_SAMPLE_SUBSCRIBER_TOPICS`, which left the previous topics' `Read` and `Describe` bindings in place while the summary printed the smaller list;
- changing `KAFKA_TOPIC_PREFIX`, which left both principals fully granted on the whole previous namespace;
- renaming the sample subscriber or its consumer group, which left a reserved group namespace nothing owned.

Three properties bound what the reconciliation may do:

- **Delete before create.** Any partial failure leaves a principal with fewer rights than the configuration describes, never more — the same order, for the same reason, as the Go path's `reconcileSubscriberACLs`.
- **Only owned shapes are removed.** A binding is the script's to revoke only if its shape is one the script provisions — `Read`/`Describe` on a `LITERAL` topic and `Read` on a `PREFIXED` group for a subscriber, `Write`/`Describe` on a `LITERAL` topic for the producer. Anything else on the principal is foreign, and a foreign binding that can **grant** aborts the run rather than being removed or ignored, exactly as credential issuance refuses on one. A foreign `DENY` is reported and left alone: it can only narrow the effective grant, and removing it would widen access as a side effect of provisioning.
- **The end state is verified.** A revoked binding still present after a successful remove is fatal, because the grant is then still too broad and the run must not claim otherwise. A granted binding still missing is reported as a warning: that direction fails safe and surfaces at the client's next connect.

Reconciliation applies only to the two principals this script manages. A principal provisioned through `POST /subscribers/{id}/kafka-credentials` is reconciled by the API, not by this script.

The producer principal is load-bearing rather than a nicety: the configuration **refuses to publish as the administrator**, so a deployment with an administrative pair and no producer pair fails to construct its event publisher and the server does not start. The worker is unaffected: it captures events into the outbox and publishes none, so it receives no broker credential at all and resolves to the no-op publisher regardless. The escape hatch is `KAFKA_ALLOW_ADMIN_PRODUCER=true`, which warns on every publisher construction and exists only for a deployment mid-upgrade.

It requires Step 1 to have already happened: it authenticates with the administrative credential, which can only have been created in the metadata log. Getting the order wrong does not produce a clear error of its own — it produces an authentication failure that reads like a wrong password, which is why the readiness wait names both causes when it times out.

**It is idempotent and exits 0 when nothing needs changing.** That is a hard requirement, not a nicety: the Compose `kafka-init` service is a one-shot with `restart: on-failure:3`, and the server and worker gate on it *completing*, so a non-zero exit on an already-provisioned broker would restart it until the cap and then fail the whole bring-up. Topic creation passes `--if-not-exists`, the ACL pass converges on the configured grant so a re-run with nothing to change writes nothing, and **an existing SCRAM credential is left alone** — rotation is an explicit request through `KAFKA_ROTATE_PRODUCER_SECRET` or `KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET`, each needing a destination file. Rotating the producer secret out from under a running server and worker stops them authenticating, so a rotation with nowhere to deliver the new value refuses outright.

**Preservation depends on the probe, so an indeterminate probe stops the run.** Leaving a credential alone requires knowing that one exists, and the script asks the broker with a `--describe` on the user entity. That question has three answers, not two: the credential exists, the broker says it does not, or *the broker did not answer* — a restart in progress, a timeout, an administrative principal without `DescribeConfigs` on user entities. Only the second licenses minting a password. Reading the third as absence is what turns a routine topic-assurance re-run into a silent rotation: control falls into the generate-and-upsert arm, a working credential is replaced under no rotation flag, every consumer and publishing process holding the old password stops authenticating, and the run still reports success.

So an indeterminate probe **leaves that principal's credential alone and writes nothing**, naming the principal and the broker's own reason with credential-bearing lines removed. Read the disposition in the closing summary: it reports `indeterminate` rather than `skipped`, because the two need different fixes — `skipped` means "configure a destination", `indeterminate` means "fix the probe".

**Refusing to guess and aborting are different reactions, and only the first is required — the run continues.** The principal's ACL bindings are still asserted, because those are idempotent and are usually the repair an operator re-running this script came for; topics assured earlier in the run are unaffected; and the script still exits 0 when nothing else needed changing. Aborting would throw that repair away, and it would turn a broker that is momentarily unauthorised to describe user configs into a bring-up that cannot start at all, because `kafka-init` is a one-shot the application services gate on.

The usual fix is to re-run once the broker is ready, or to grant the administrative principal `DescribeConfigs` on user entities. If you already intend to write a specific password, supply it — an explicit value needs no probe and is applied idempotently. For a broker that can *never* answer a describe on users, `KAFKA_ALLOW_SCRAM_PROBE_FAILURE=1` makes an indeterminate answer read as **absent**, which restores the generate-and-upsert path deliberately for the one deployment where that is correct; the log then states plainly what may be overwritten.

**A declared-but-empty `KAFKA_BROKERS` means "Kafka is not configured here" and provisioning skips entirely, exiting 0.** That is what makes the script safe to wire into an unconditional bring-up path. `KAFKA_BOOTSTRAP_SERVER` overrides the skip, which is how the `kafka-init` service provisions a broker for a deployment that has not yet turned publishing on.

> **No credential is ever printed.** A generated password is written to a mode-0600 file you nominate through `KAFKA_SASL_SECRET_FILE`, `KAFKA_PRODUCER_SECRET_FILE` or `KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE`, and only the **path** is reported; a supplied secret is not echoed. With neither a supplied secret nor a destination file the principal is skipped and the reason is printed. This is not fastidiousness: the script's usual home is the `kafka-init` service, whose stdout *is* a container log — written to disk, handed to anyone who can run `docker compose logs`, and forwarded to whatever collects the host's logs. "Shown once" is not a property a log line can have.

Geometry is **verified, not assumed**: every topic's partition count and replication factor are read back from the broker, and a geometry that cannot be read after three attempts fails the run rather than being recorded as unknown. The closing summary prints only observed values.

### How to run provisioning

Four routes, all reaching the same script. **Three of them are local and one is a mandatory step on Kubernetes** — see the box after the block, because nothing in the manifests performs it for you.

```bash
# 1. The Compose one-shot. Runs automatically on bring-up, gated on the broker's
#    healthcheck. The kafka and kafka-init services are BOTH behind the "kafka" profile.
#
#    NAME THE TWO SERVICES. Without them the command brings up the WHOLE project,
#    including the application services — and on a checkout where BLNK_IMAGE is still
#    unset that fails immediately on an unresolvable image, before the broker is
#    touched. Naming them provisions the broker on its own, which is what this step is
#    for; use ./stack.sh --up when you want the whole stack.
docker compose --profile kafka up -d kafka kafka-init
docker compose --profile kafka logs kafka-init

# 2. The makefile target, on the host. Sources .env, then lets the command line win over
#    it, then falls back to the documented local geometry (prefix blnk, 6 partitions,
#    replication factor 1).
make kafka_provision

# 3. stack.sh, which provisions Kafka and then brings the stack up.
./stack.sh --up

# 4. ON KUBERNETES, inside a broker pod, ONCE PER CLUSTER, after the StatefulSet is Ready
#    and before you rely on publishing. There is no Job or CronJob that does this: the
#    manifests deploy the brokers and the application, and provisioning is an operator
#    step. The script runs unmodified in the pod, where the CLI already is.
#
#    THE SCRIPT IS NOT IN THE BROKER IMAGE, so carry it in first:
kubectl -n blnk cp scripts/kafka-provision.sh kafka-0:/tmp/kafka-provision.sh

#    The bootstrap address is the CLIENT listener on 9092 — kafka:9092 through the
#    client-facing Service, or the per-pod name below. 9093 is the CONTROLLER listener and
#    is never a client address. Each broker pod also writes its own address to
#    /tmp/blnk-kafka/bootstrap-address, which is what its readiness probe reads.
kubectl -n blnk exec kafka-0 -- env \
  KAFKA_BOOTSTRAP_SERVER="$(kubectl -n blnk exec kafka-0 -- cat /tmp/blnk-kafka/bootstrap-address)" \
  KAFKA_SASL_ADMIN_USER="$ADMIN_USER" KAFKA_SASL_ADMIN_SECRET="$ADMIN_SECRET" \
  KAFKA_SASL_USER="$PRODUCER_USER" KAFKA_SASL_SECRET="$PRODUCER_SECRET" \
  KAFKA_TOPIC_PREFIX=blnk KAFKA_MIN_PARTITIONS=6 KAFKA_REPLICATION_FACTOR=3 \
  KAFKA_CLIENT_CONFIG=/tmp/blnk-kafka/client-admin.properties \
  bash /tmp/kafka-provision.sh
#    Pass the credentials from your Secrets rather than typing them; the pod's own
#    client-admin.properties is named so the run authenticates exactly as the broker's
#    readiness probe does, over the same protocol. `--print-interface-host` lists every
#    name the script reads.
```

> **On Kubernetes this step is REQUIRED, and skipping it fails silently.** The application creates its own topics at start-up with the **administrative** credential, so `kafka admin: event topics assured` appears in the log, every probe passes and the deployment reads as healthy. But Blnk **publishes as the producer principal**, which nothing has created yet — so every publish fails SASL authentication, the relay retries on its backoff schedule and events dead-letter while the cluster looks fine. There is no `Job` in `infrastructure/k8s-manifests/` that provisions the producer principal or the ACLs, and no manifest to enable: run route 4 once per cluster, and again after any change to `KAFKA_TOPIC_PREFIX`, to the principal names, or to a subscriber's authorised topics. The symptom to look for if it was missed is a growing `blnk_events_publish_attempts_total` with an authentication cause in the relay's failure log and nothing arriving on any topic. Per-**subscriber** principals are the exception and need no operator step: `POST /subscribers/{id}/kafka-credentials` creates and reconciles those through the Go admin client.

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
derived as sunset minus 30 days. Two consequences follow from that arithmetic and both are start-up
failures rather than warnings: **`WEBHOOK_DEPRECATION_SUNSET_DATE` is required once `KAFKA_BROKERS` is
set** — with brokers configured and no sunset the runtime treats the deployment as already past it, so
it refuses rather than silently stopping legacy delivery — and **the instant must be no more than 30
days ahead**, because a date further out leaves the window un-opened and the relay refuses to start.
`retired` is the value for a window that has already closed. The provisioning script publishes its own interface, which is how `stack.sh` builds its passthrough list:

```bash
scripts/kafka-provision.sh --print-interface-host   # one variable name per line
```

### Adding a runtime SCRAM user

Once the broker is up and the bootstrap credential can authenticate, further principals are ordinary runtime operations:

**Read the password from a file or a prompt and feed it in on stdin. Do not type it as an argument.**

```bash
# $PRINCIPAL is the principal name. The password is read from stdin, so it appears in
# no command line on this host - not this shell's, and not the docker client's.
read -rs NEW_PASSWORD < /dev/tty          # or: NEW_PASSWORD=$(cat /run/secrets/new-scram-password)

printf '%s\n' "$NEW_PASSWORD" | docker compose exec -T kafka sh -c '
  read -r pw
  exec /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server kafka:9092 \
    --command-config /tmp/blnk-kafka/client-admin.properties \
    --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=$pw]" \
    --entity-type users --entity-name "$1"
' sh "$PRINCIPAL"

unset NEW_PASSWORD
```

**Stdin is the whole point of that shape**, and it is a remedy rather than a mitigation: typing the credential into `--add-config` inline would place it in the CLI's `argv`, which is world-readable through `/proc/<pid>/cmdline` for the life of a JVM start and is captured by shell history and by anything sampling the process table. `scripts/kafka-provision.sh` reaches the same end by the *other* route Kafka offers — it writes a mode-0600 properties file and passes `--add-config-file`, which keeps the value out of argv on both sides of the container boundary. Either route is correct; they differ only in how the value is spelled, and [Why stdin, and not simply `--add-config` with the password inline](#why-stdin-and-not-simply---add-config-with-the-password-inline) below is where that difference is stated, because it is the one thing that makes a copied command fail.

**Distinguish this clearly from the bootstrap credential, which cannot be created this way** — see Step 1. This command needs an authenticated connection, so it works only *because* a bootstrap credential already exists.

#### Why stdin, and not simply `--add-config` with the password inline

Because a command line is not private. On Linux `/proc/<pid>/cmdline` is mode `444` — **every account on the host can read it**, for as long as the process lives — while `/proc/<pid>/environ` is mode `400`, readable only by the owner. The inline form this section used to show placed a broker password in the argv of *two* processes at once: the `docker` client on the host, and `kafka-configs.sh` in the container. Any local user running `ps` during those few seconds got it in full, and it landed in shell history besides.

The form above removes the host-side exposure entirely: the password crosses into the container on stdin, and the only argv that ever holds it belongs to `kafka-configs.sh` inside the container's own PID namespace. `-T` disables TTY allocation, which is what allows the pipe to be read. `read -rs` keeps it off the terminal and out of history; `unset` drops it from the shell afterwards.

**`--add-config-file` keeps a value out of argv too — but it does not accept the same spelling, and moving the inline value into a file unchanged is what fails.** The two options parse their input differently, and each rejects the other's form:

```
# A file holding SCRAM-SHA-512=[iterations=4096,password=...] — the spelling --add-config needs.
$ kafka-configs.sh --alter --add-config-file /tmp/scram.properties --entity-type users --entity-name someone
Invalid credential property SCRAM_SHA_512=[iterations=4096,password=...]

# The same value unbracketed, passed inline instead.
$ kafka-configs.sh --alter --add-config "SCRAM-SHA-512=iterations=4096,password=..." ...
requirement failed: Invalid entity config: all configs to be added must be in the format "key=val" or  "key=[val1,val2]" to group values which contain commas.
```

So the rule is: **`--add-config` takes the bracketed form, a properties file takes the unbracketed one.** A file holding `SCRAM-SHA-512=iterations=4096,password=...`, one assignment per line with no brackets, is accepted and creates the credential — that is exactly what `scripts/kafka-provision.sh` writes into a mode-0600 temporary file and removes the moment the broker has consumed it. Both spellings above were run against the pinned `apache/kafka:4.3.1`, and escaping the hyphens changes nothing in either direction.

Which route to use by hand is therefore a choice about what you have, not about which one works. Stdin is shown here because an interactive shell has a pipe and no file to create, secure and delete; a script has the opposite, which is why the script takes the other route.

One further caution that is unrelated to secrecy: the value must come from the safe credential alphabet, because `--add-config` shares the no-escape-sequence grammar described above, so a comma or a bracket is silently truncated.

**The argv question does not arise at all on the API path**, which is the one to prefer. `POST /subscribers/{id}/kafka-credentials` sends an `AlterUserScramCredentials` request over the Kafka protocol from inside the server process, so the password exists only in memory and in the TLS-protected request — there is no command line anywhere in the path.

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

The **three tenant categories** may appear in a grant unconditionally. `blnk.system` may appear only where you have declared `KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true`: it carries `system.error`, whose body renders Blnk's own error text verbatim, and it is the catch-all for any event type the catalogue does not yet recognise. Every `<topic>.dlt` is ungrantable outright, under every configuration, so a dead-letter name can never appear in a subscriber's topic list — the DTO, the persistence boundary and the ACL provisioner all read one allowlist, `model.SubscriberAuthorizableTopics`, resolved against that one declaration.

#### Granting the internal system topic

`<prefix>.system` is the one name whose grantability is a **deployment decision** rather than a fixed rule. Two declarations are required, and neither implies the other:

```bash
# 1. On the server (and the worker, if it issues credentials): permit the grant to exist at all.
KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true
```

```bash
# 2. Per subscriber: name the topic in that subscriber's own grant.
curl -X PUT "http://localhost:5001/subscribers/sub_9f8d3c214b7a5e6f" \
  -H "X-Blnk-Key: $BLNK_SERVER_SECRET_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"authorized_topics": ["blnk.transactions", "blnk.system"]}'
```

Default is `false`, and the default is deliberate: with the variable unset or false, the create, the update and the credential issuance each refuse a grant naming the topic, and the refusal names the variable so the remedy is readable without consulting the source. Setting the variable alone grants nobody anything — it only makes the second step possible.

**Four properties of this are worth knowing before you turn it on:**

- It is read in **every mode**, not only under `BLNK_SERVER_SECURE=true`. The `BLNK_`-prefixed alias `BLNK_KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS` also resolves, as with every other variable in this document.
- `scripts/kafka-provision.sh` **never** grants the topic, whatever the variable says. A bring-up script makes no entitlement decision, and a sample principal granted every category would make the subscriber-isolation criterion vacuous by leaving it no outside.
- A grant that passes is **logged as a warning** by the ACL provisioner, carrying the subscriber's id hash and the topic, so an entitlement this unusual leaves a record even when nobody was watching the request.
- Turning the variable back to `false` does **not** revoke a live principal on its own. Remove the topic from the subscriber's `authorized_topics` — the ACL grant is reconciled against the record on the next issuance, so the record is what has to change.

Grant it when a subscriber genuinely operates the deployment with you and needs to see Blnk's own failures. Withhold it otherwise: `system.error`'s body is a frozen legacy contract that cannot be narrowed, so there is no redacted version of this topic to offer instead.

### Foreign ACL bindings, and why issuance refuses on them

Blnk reads a subscriber principal's **complete** ACL grant before it issues a credential, and it classifies every binding it did not itself provision:

| Foreign binding | What Blnk does | Why |
|-----------------|----------------|-----|
| An **`Allow`** of any shape Blnk does not provision — a `Write`, a `PREFIXED` topic pattern, a cluster or transactional-id resource, or a binding whose permission type the broker did not state | **Refuses.** Credential issuance fails with `409` and `error_detail.code` of `SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION`, the SCRAM credential written moments earlier is revoked, and **no password is returned**. Granting further access (`PUT /subscribers/{id}`) refuses too. **It is not retryable**: the broker answered, provisioning completed, and the refusal is a judgement about the resulting boundary — an immediate retry re-reads the same broker state and refuses again. It becomes retryable once a human removes the bindings or records the access on the subscriber. | The binding grants access outside the boundary the registry describes, by an amount Blnk cannot bound. Issuing a credential would return one whose `enforced_access` declares a boundary the broker is not enforcing — and nothing in the response would say so. |
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
#    A DEAD-LETTER topic is the right probe: no subscriber is ever granted one under
#    any configuration, so an authorization failure is the correct and expected
#    outcome. blnk.system is only a valid probe on a deployment that has NOT declared
#    KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS; a .dlt name is the one that stays valid
#    whatever a deployment grants. Use a SUBSCRIBER's own
#    client properties file —
#    never the admin one, which is in super.users and is allowed everything by
#    design, so it would prove nothing. The path must be visible INSIDE the
#    container; mount the file or write it there first.
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --command-config /path/in/container/sample-subscriber.properties \
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
  --command-config /path/in/container/sample-subscriber.properties \
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
that does not exist. Every subscriber reads from the **same three grantable category topics**, and each
one is granted **only the subset it was authorised for**: its `authorized_topics`. Two subscribers can
therefore hold entirely different grants over one shared inventory, and no subscriber is ever granted a
`.dlt` topic.

Isolation is delivered by three things and three things only:

1. **The principal** — a distinct SASL/SCRAM identity per subscriber.
2. **The ACL bindings** — literal `Read`/`Describe` on the authorised topics, and nothing beyond them.
3. **The consumer group namespace** — a prefixed `Read` grant that reserves the subscriber's own group space and no one else's.

##### The operational consequence: a topic grant is a grant over every tenant's events on it

This follows directly from the two facts above and is the sentence to have in mind when you approve a grant.

A category topic carries **every** event of its category for the whole deployment. `blnk.transactions` holds every ledger's transaction events, and `blnk.balances` and `blnk.identities` do the same for theirs. So granting `blnk.transactions` to a subscriber grants it read access to **every ledger's** transaction events and to the events of **every other subscriber** of that topic. The consumer group does not narrow it, and neither does the number of subscribers sharing the topic. A `partition_key_prefix` **does** narrow it — but not at the broker, and not by Blnk: it is applied by a component the deployment declares in front of the brokers, and where none is declared such a subscriber is refused a credential rather than issued a wider one. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker).

Approve a topic grant on that basis. The question to ask is not "which slice of this topic does the subscriber need?" — there is no mechanism that answers it — but **"is this subscriber trusted with the whole category?"** If the answer is no, the grant is the wrong instrument:

| What you need | The enforceable instrument | What it costs |
|---|---|---|
| A subscriber must not see a *category* | Omit that topic from `authorized_topics`. The broker refuses it outright. | Nothing. This is the intended mechanism. |
| A subscriber must not see *another tenant's records within a category* | **Separate the deployments.** A distinct Blnk deployment, with its own broker or its own topic namespace via `KAFKA_TOPIC_PREFIX`, is the only boundary that holds. | A second deployment to operate. |
| A subscriber must not see *another subscriber's or another ledger's records within a category it is granted* | Record a `partition_key_prefix` **and declare a key-authorising component** (`KAFKA_KEY_SCOPE_ENFORCEMENT`). Blnk then grants `Describe` without `Read` so that component is the only path — see immediately below. | A component to run, which Blnk does not ship. Without one, issuance for that subscriber is refused with `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED`. |

A per-subscriber topic is ruled out by design — the access model exists to avoid per-tenant topics — so the middle option is the key scope, and the section below is what it costs and what it refuses: see [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker).

#### The partition-key prefix is enforced outside the broker

A subscriber row may carry a `partition_key_prefix`. It is the third dimension of the access model, and it is the one Kafka cannot evaluate — so it is kept outside the broker, and recording one changes both the subscriber's grant and whether it can be issued a credential at all.

**Kafka's authorizer has no message-key dimension.** There is no ACL that restricts a consumer to a slice of a topic by key: a principal granted topic `Read` reads every record on that topic, whatever the keys are. So rather than issue such a credential, provisioning **withholds record-level `Read`** from a key-scoped subscriber:

| The row | The ACL bindings created | How the subscriber consumes |
|---|---|---|
| No `partition_key_prefix` | `Read` + `Describe` on each authorised topic (`LITERAL`), `Read` on the group namespace (`PREFIXED`) | Directly from the brokers at `KAFKA_SUBSCRIBER_BROKERS`. Its `authorized_topics` are its whole boundary. |
| A `partition_key_prefix` | **`Describe` only** on each authorised topic, `Read` on the group namespace | Through the key-authorising component declared in `KAFKA_KEY_SCOPE_ENFORCEMENT`, at `KAFKA_KEY_SCOPE_GATEWAY_BROKERS`. A fetch against the brokers is refused with `TOPIC_AUTHORIZATION_FAILED`. |

**BLNK DOES NOT SHIP THAT COMPONENT, AND BLNK SERVES NO RECORDS ITSELF.** There is no data-plane route under `/subscribers` and no Blnk endpoint that returns records. What Blnk owns for a key scope is exactly three things: the narrowed grant above, the per-record rule the prefix means (`HasKeyAccess` in the model package, stated once so a declared component, a replay and an administrative export cannot disagree), and the refusal below.

##### So the shipped default REFUSES a key-scoped issuance

| `KAFKA_KEY_SCOPE_ENFORCEMENT` | `KAFKA_KEY_SCOPE_GATEWAY_BROKERS` | Attestation endpoint | `POST /subscribers/{id}/kafka-credentials` for a key-scoped row |
|---|---|---|---|
| `none` (default) | ignored | ignored | **`409 SUBSCRIBER_KEY_SCOPE_UNENFORCED`.** No SCRAM credential is created and nothing is recorded. |
| `broker_gateway` | unset, or identical to `KAFKA_BROKERS` | any | **`409 SUBSCRIBER_KEY_SCOPE_UNENFORCED`.** A declaration with no distinct endpoint enforces nothing; startup also logs a warning naming this. |
| `broker_gateway` | a distinct address list | unset, or missing its token | **`409 SUBSCRIBER_KEY_SCOPE_UNENFORCED`.** An unverifiable declaration is read as *no* declaration; startup logs a warning naming the two variables. |
| `broker_gateway` | a distinct address list | set, but the component does not attest the exact binding | **`409 SUBSCRIBER_KEY_SCOPE_UNATTESTED`.** Nothing is minted and nothing is recorded. |
| `broker_gateway` | a distinct address list | set, and the component attests | `200`. `broker_endpoint` carries **that component's** address, and `partition_key_prefix_enforced_by` reads `broker_gateway`. |

The refusal names the two remedies, and they are the only two: declare a component and its endpoint, or clear the prefix and narrow `authorized_topics` to what the broker enforces. **A subscriber with no prefix is never affected by *this* refusal** — it keeps the advertised broker list even where a component is declared, because it has no key scope to route through one. It is affected by a different one, immediately below.

##### The control endpoint: what Blnk verifies, and the contract your component implements

A mode and a bootstrap address are assertions a deployment makes about itself, and any address satisfies them. Blnk therefore requires a **control endpoint** it can call, and a declaration without one is treated as no declaration at all — which is why the third row of the table above refuses rather than issuing something unverified.

Set both:

```bash
KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL=https://keyscope-gateway.internal/key-scopes
KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN=<bearer credential>   # a SECRET
KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TIMEOUT_MS=2000             # optional; clamped to 5000
```

Plain `http` is accepted **only** for a loopback host. Anything else must be `https`: that token registers key-scope bindings, so whoever holds it decides which records a subscriber sees. Redirects are refused rather than followed, because following one forwards the token to wherever the redirect points.

The wire contract is three calls on that one URL. Implement all three; a component that answers only the first cannot be revoked from and reports no health.

| When | Call | Blnk sends | Your component must answer |
|---|---|---|---|
| Credential issuance, **before any secret exists** and before the broker is touched | `POST <url>` | `{"principal", "subscriber_id", "partition_key_prefix", "authorized_topics", "consumer_group_prefix"}` | `200` with `{"key_scope_enforced": true, "principal": <the same principal>, "partition_key_prefix": <the same prefix, byte for byte>}` |
| Deregistration, **after** the broker credential is revoked | `DELETE <url>?principal=<p>` | nothing | `200`, `204`, or `404`. A binding you never held is the state being asked for, so `404` counts as success. |
| Server start-up, once | `GET <url>` | nothing | `200` with `{"key_scope_enforced": true}` |

Every call carries `Authorization: Bearer <token>`.

**The comparison is exact and it is deliberate.** A component that echoes a *trimmed*, lower-cased or truncated prefix is enforcing a **wider** boundary than the registry records, and that is the dangerous direction: the subscriber is told it is isolated while receiving a superset. So a prefix differing by one byte, a different principal, `key_scope_enforced: false`, a non-`200`, or no answer inside the timeout all produce `409 SUBSCRIBER_KEY_SCOPE_UNATTESTED` with nothing minted. The error carries `retryable`: `true` where the component was unreachable or answered `5xx` — repeating the request can succeed — and `false` where it answered and answered wrongly, which no retry changes.

The start-up probe is **logged and never fatal**: a component that is briefly down must not stop the server, and issuance refuses on its own while it stays down. Look for one of these two lines at boot:

```
level=info  msg="cmd: the declared key-scope enforcement gateway answered the start-up health probe ..."
level=warning msg="cmd: the declared key-scope enforcement gateway did not answer the start-up health probe ..."
```

##### Under a declared key-scoped model, EVERY subscriber must carry a key scope

Withholding record `Read` from key-scoped principals protects *those* subscribers. It says nothing about a subscriber registered **without** a prefix — which is granted literal topic `Read` on every topic it is authorised for, meaning every ledger's records on a shared category topic, in a deployment whose declared model is that a subscriber sees only its own.

So while enforcement is active, all three of these are refused with **`409 SUBSCRIBER_KEY_SCOPE_REQUIRED`**:

- issuing a credential to a subscriber that records no `partition_key_prefix`;
- recording a subscriber with no prefix and then issuing (the same thing, reached in the other order);
- **clearing** a prefix on a subscriber that already holds one — the path that would otherwise widen a live principal back to whole-topic `Read` with issuance never running again.

The refusal names three remedies, and there is deliberately **no per-subscriber opt-out** — that would be the same gap with a field name:

1. Record the prefix that subscriber is entitled to.
2. Provision a genuine whole-topic consumer as an **operator-managed principal outside the subscriber registry**, which is what `scripts/kafka-provision.sh` does for the producer and the sample subscriber. Such a principal has no registry row, so no Blnk endpoint mints or revokes it, and it is yours to manage.
3. Stop declaring the key-scoped model, and acknowledge whole-topic access explicitly — the next section.

##### In secure mode, a deployment must declare which access model it is

Whole-topic subscriber access is **not a defect**. It is the mandated model: category topics, no per-tenant topics, an authorizer with no message-key dimension. For a single-tenant ledger, or a trusted internal consumer, a credential that reads every record on `blnk.transactions` is exactly right.

What was wrong is that it was the model a deployment arrived at by configuring **nothing**. So with `BLNK_SERVER_SECURE=true`, a deployment must declare one of the two models or issuance refuses with **`409 SUBSCRIBER_SHARED_TOPIC_ACCESS_UNACKNOWLEDGED`**:
**Every shipped configuration declares it, and declares whole-topic access.** `.env.example`, both Compose files and `infrastructure/k8s-manifests/blnk-config.yaml` all ship `KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true`. The Kubernetes ConfigMap in particular also sets `BLNK_SERVER_SECURE=true`, and shipping that pair as secure-plus-undeclared meant the reference production deployment answered `409` on the credential-issuance endpoint out of the box — the endpoint the requirement asks for, refused by the configuration meant to demonstrate it. Declaring the model in the reference configuration is not a relaxation of the check: it states the model the requirement mandates, on the deployment that implements it. **Set it to `false` on a cluster that has not decided**, and the refusal returns.


```bash
# Either: acknowledge that subscriber credentials read every record on each granted topic.
KAFKA_SUBSCRIBER_SHARED_TOPIC_ACCESS=true

# Or: declare the key-scoped model, and record a prefix on each subscriber.
KAFKA_KEY_SCOPE_ENFORCEMENT=broker_gateway
KAFKA_KEY_SCOPE_GATEWAY_BROKERS=keyscope-gateway.internal:9095
KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL=https://keyscope-gateway.internal/key-scopes
KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_TOKEN=<bearer credential>
```

**Outside secure mode nothing changes.** The local stack, both Compose files and the test suite issue credentials exactly as before; the declaration exists to make a production decision explicit, not to break `make run`.

`Describe` is retained deliberately in the key-scoped shape: without it a key-scoped consumer could not read its own topics' partition counts or offsets, and so could not measure its own lag.

Credential issuance states which shape was provisioned:

```json
"enforced_access": {
  "enforced_by": ["topic", "consumer_group", "partition_key"],
  "not_enforced_by": [],
  "topics": ["blnk.transactions"],
  "consumer_group_namespace": "blnk-sub-sub_9f8d3c214b7a5e6f.",
  "partition_key_prefix_enforced": true,
  "partition_key_prefix": "ldg_9f1c8a72",
  "partition_key_prefix_enforced_by": "broker_gateway",
  "gateway_delivery_required": true,
  "broker_record_access": false,
  "exclusive_grant_verified": true
}
```

`gateway_delivery_required` is the field a subscriber's client branches on — it is `true` exactly when `partition_key_prefix_enforced` is — and `broker_record_access` states whether a topic `Read` binding exists. `partition_key_prefix_enforced_by` uses the **same word** the deployment declares in `KAFKA_KEY_SCOPE_ENFORCEMENT`, so a response and the configuration that made it issuable cannot name two different components.

**Every one of those fields answers from the DEPLOYMENT, not from the column**, and `enforced_access.partition_key_scope_state` is where you read which:

| State | Prefix recorded? | Component declared? | Attested for this principal? |
|---|---|---|---|
| `not_requested` | no | — | — |
| `requested` | yes | **no** | — |
| `available` | yes | yes | not yet (registry reads make no round trip) |
| `attested` | yes | yes | yes — credential responses only |

`enforced_by` carries `partition_key` in the `available` and `attested` states; `not_enforced_by` carries it in `requested`, and is empty otherwise. That pairing is the correction: these fields previously derived from whether the prefix column was non-empty, so a row on the shipped default reported `partition_key_prefix_enforced: true` with `broker_gateway` named — while `POST /subscribers/{id}/kafka-credentials` refused the same row with `SUBSCRIBER_KEY_SCOPE_UNENFORCED`. If an audit or a dashboard asks "which subscribers actually have an enforced key boundary?", `partition_key_scope_state` is the field to group by.

`credential_issuance_blocked` on a subscriber read now predicts **every** refusal that is knowable without a broker round trip, in the order issuance applies them: a deregistration in flight, a recorded prefix with no component declared, a *missing* prefix under a declared component, an unacknowledged whole-topic model in secure mode, an empty topic grant, and unadvertised `KAFKA_SUBSCRIBER_BROKERS`. `credential_issuance_blocked_reason` names the field or variable to change. The one refusal it does not predict is `SUBSCRIBER_KEY_SCOPE_UNATTESTED`, because attestation is a live call to the component and not a property of the row or the configuration.

Issuance to a key-scoped subscriber logs an INFO line naming the prefix, the enforcement point and the topics, so the moment a key-scoped principal comes into existence is visible in the operator log. A **refused** issuance logs the refusal with the same identifiers, so the state is equally visible.

##### Verifying the narrowed grant

The grant is assertable rather than a matter of trust. Two checks, and the second is the one that matters:

```bash
# The bindings themselves: a key-scoped principal must show Describe and no Read on its topics.
docker compose exec kafka /opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 --command-config /tmp/admin.properties \
  --list --principal 'User:blnk-sub-<subscriber_id>'

# The behaviour: a fetch as that principal must be refused. This is the boundary; the listing above
# only describes it.
```

`ProvisionSubscriberPrincipal` performs the same check on every issuance and **refuses to return a password** when it cannot establish all three of: no topic `Read` binding for a key-scoped request, an authorizer actually enforcing, and no foreign `ALLOW` binding on the principal. A refusal is `409 SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION` and the credential it had written is revoked before the response is sent — so a key-scoped subscriber whose boundary could not be verified holds nothing, rather than holding a credential wider than its row describes.

##### Recording or clearing a prefix moves the grant, so it reaches the broker

An update that crosses between "has a prefix" and "has none" **reconciles the broker**, in the direction the change implies:

- **Recording** a prefix on a provisioned subscriber withdraws record-level `Read`. The existing credential still authenticates and can still `Describe`; its next direct fetch is refused. A WARNING is logged naming the prefix, the withdrawal and the two ways forward — re-issue, which delivers a response declaring the enforcing component (and which is itself refused where none is declared), or clear the prefix to restore direct consumption. That warning exists because the consequence reaches *backwards*: the response that credential was delivered with said `broker_record_access: true`, truthfully at the time, and it cannot be recalled.
- **Clearing** a prefix re-grants `Read` on every topic the subscriber already had. It is a **widening**, so it is fail-closed: if the broker cannot be reached, the update fails and the row keeps its prefix rather than recording access that was never granted.
- **Replacing** one prefix with another moves no binding — Kafka has no key dimension for it to move — so it does not touch the broker at all and does not fail when Kafka is down.

##### What the prefix still does not do

- **It does not narrow the topic dimension.** Within a granted topic the prefix is the only additional narrowing. A subscriber that must not see a whole *category* needs that topic omitted from `authorized_topics`.
- **It depends on the declared component being in the path, and on nothing widening the grant.** An operator who hands a subscriber a credential minted outside Blnk, or grants an extra `ALLOW` binding by hand, has widened it — which is why issuance reads the principal's complete grant and refuses when it finds one.
- **Blnk verifies the control plane, not the data plane, and this is the residual obligation that remains yours.** What Blnk establishes is that the broker will not serve that principal a record at all, that a component answered an authenticated call, and that it attested *this* principal against *this* exact prefix. What it cannot establish is that the component then **filters records accordingly** — that is data-plane behaviour in a process Blnk does not run. A component that attests correctly and forwards everything would satisfy every check here. Instrument and test that component on its own: the attestation is a binding contract, not a proof of enforcement.
- **Records outside the scope still exist on the shared topic.** They are unreachable by that subscriber, not absent. Where records must not be *present* in a shared namespace at all, separate the deployments.

> **This has been three different behaviours, and only the current one is a boundary.** First, `POST /subscribers/:subscriber_id/kafka-credentials` refused any row recording a prefix with a code of its own, and a `CHECK` constraint made the combination unrepresentable — which withheld the only credential such a subscriber could ever have and offered no remedy but clearing the prefix. Then the credential was issued with whole-topic `Read` and the response *declared* that applying the prefix was the consumer's own obligation — accurate prose about an absent boundary, since a subscriber that ignored it, or used any other Kafka client, read every record on the shared topic. Now the grant itself is narrower, the prefix is applied by a component the operator declares, and issuance refuses — with `SUBSCRIBER_KEY_SCOPE_UNENFORCED`, naming both remedies — while no component is declared. `sql/1781249138.sql` drops `event_subscribers_key_scope_chk`; the retired `SUBSCRIBER_ISOLATION_UNENFORCEABLE` code is gone, and `SUBSCRIBER_KEY_SCOPE_UNENFORCED` is the one code for this judgement in both orders (issuance, and recording a prefix on a row that already holds a credential).
>
> One caveat if you were running an earlier build: a `partition_key_prefix` recorded beside a credential has been **cleared** at two points in this history, and the cleared values were not retained anywhere. Both are places where the constraint gets *added*, because it cannot be added over a row that violates it: the original version of `sql/1781248920.sql`, and the `Down` section of `sql/1781249138.sql`, which repairs before it constrains. Neither the current up direction of any migration nor an ordinary forward upgrade clears anything — **it is a rollback that costs you these values.** Re-record them with `PUT /subscribers/{id}` — the registry has no `PATCH` route, and an unregistered verb is answered by the router rather than the handler. The `event_subscribers_key_scope_chk` CHECK constraint stays **dropped** (`sql/1781249138.sql`): the guard is in the service, so a row seeded directly into the database can still hold the combination, and every read of such a row describes it truthfully.
>
> **Three migrations drop this constraint and none of them adds it, and reading that as three attempts would be wrong.** `sql/1781248920.sql`, `sql/1781248930.sql` and `sql/1781249138.sql` each carry the same `DROP CONSTRAINT IF EXISTS` in their up direction. Nothing in the current tree creates the constraint on the way up, so on a fresh database all three are no-ops; it exists only in a database that applied the **original** version of `sql/1781248920.sql`, which created it before that file was rewritten in place. (It was rewritten rather than deleted because `sql-migrate` plans against the ledger in `blnk.gorp_migrations`, where an applied id with no source file aborts the entire run.) The repetition is then not redundancy but a consequence of the `Down` sections: `1781248930` and `1781249138` each **re-add** the constraint on the way down, so a rollback to either point reinstates it and the next up has to remove it again. A migration that assumed a predecessor had already removed it would leave the constraint in place on exactly those deployments. The applied history is left intact rather than squashed, for the same reason no applied migration is ever edited. **The final state is the only one to reason about: the constraint does not exist, and the guard lives in the service.**

##### Triage: a key scope has no Blnk-side metrics, because Blnk serves no records

There is no delivered/withheld counter pair to read, and there cannot be: the per-record filter runs in the declared component, which is not Blnk's process. **Instrument that component** if you need per-record evidence that the boundary is filtering; Blnk's `/metrics` cannot supply it.

What Blnk's own signals do tell you about a key-scoped subscriber:

| Signal | What it establishes |
|---|---|
| The INFO issuance line naming the prefix and enforcement point | A key-scoped principal exists, and the deployment had a component declared when it was minted. |
| `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` in the API log | A key-scoped subscriber was refused a credential. It holds nothing; it is not consuming. |
| `blnk_kafka_consumer_lag` for its group | Whether records are being consumed under its group at all. Flat non-zero lag on a granted topic means it is not consuming. |
| The `kafka-acls.sh --list` output above | `Describe` and no `Read` — the broker-side half of the boundary, which is the half Blnk owns. |

The one reading to act on immediately: a key-scoped principal that holds a topic `Read` binding. That is the boundary absent, whatever any metric says, and it is what `exclusive_grant_verified` and the verification above exist to make impossible through Blnk.

Note also that the prefix is **recorded rather than derived** — unlike the principal and the group. It could not be derived: a Kafka message key on Blnk's topics is the outbox row's stored partition key, which is the **ledger id** wherever the event's subject belongs to a ledger (see [event-streaming.md](event-streaming.md#the-key-is-the-ledger-id-wherever-a-ledger-exists)), and a prefix computed from the subscriber's own identifier bears no relation to any ledger id — so it would match no record ever produced. A prefix is therefore only meaningful when the operator sets it from the ledger identifiers that subscriber is entitled to.

### Issuing credentials

The response body contains a secret that exists nowhere else, so write it to a private file and never to a terminal. `--output` keeps it off stdout; `umask 077` keeps the file private from the moment it is created.

```bash
umask 077
resp="$(mktemp)"                       # a 0600 file in $TMPDIR (often the shared /tmp)
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
| `authorized_topics` | The topics the credential may `Read` and `Describe` — the authorised subset of the grantable category topics. **Never a `.dlt` name. Never `<prefix>.system` either, unless the deployment has declared `KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true`.** |
| `consumer_group_id` | The derived default group, `blnk-sub-<subscriber_id>.default`. |
| `enforced_access` | Each dimension of the access model and the component that enforces it: topic and consumer group at the broker's authorizer, the recorded `partition_key_prefix` at the key-authorising component the deployment declared. Assembled by the model, never by the handler, and it carries `gateway_delivery_required` so a client knows which endpoint to dial. |
| `username` | The derived principal, `blnk-sub-<subscriber_id>`. |
| `password` | **The plaintext, returned only on this response and never again.** Hand it to the subscriber and keep no copy Blnk can be asked for. |
| `mechanism` | `SCRAM-SHA-512`. |
| `issued_at` | The issuance instant, which is also what is persisted alongside the non-reversible reference. |
| `credential_fingerprint` | A short digest fragment of the stored non-reversible reference — **not** the reference and not the password. It answers one question and nothing else: "is this the same issuance I saw last time?" The same value appears on a subscriber *read*, so you can correlate an issuance you performed with the row that now records it. |
| `replaced` | `true` when this issuance overwrote an existing credential for the principal, `false` when the principal had none. This is the field that tells you a rotation just happened, and therefore that any consumer holding the previous password will fail at its next handshake. It is always present, so do not infer it from an absence. |

A **successful** provisioning completes within five seconds: the ceiling is applied by the service and
again at the HTTP boundary, so an expiry is *answered* rather than waited out and every request ends
with a code that says whether retrying is sensible.

**A failing provisioning answers inside the same five seconds — and its cleanup runs after the answer, on
a clock of its own.** When provisioning fails partway, Blnk owes a compensation: revoking the credential
it had already written at the broker, clearing the registry record, releasing the provisioning fence. On
the server role that work is SCHEDULED rather than awaited, so the refusal reaches you inside the budget
and the cleanup runs afterwards, bounded by its own five seconds measured from the moment it starts. One
window per cleanup, shared by every level nested within it — never a fresh budget per level.
**Read the response's `compensated` flag as the state at the moment of answering, not as a verdict on the
cleanup.** [The cleanup runs AFTER the response](#the-cleanup-runs-after-the-response-on-a-clock-of-its-own),
directly below, sets the arithmetic out in full, names the durable markers to inspect before retrying,
and explains why the cleanup is measured from when it starts rather than from when the request did.
Compensating at all is a deliberate trade: the alternative is leaving a live SASL credential at the
broker for a principal the registry records no issuance for, which then has to be found and revoked by
hand (see [SubscriberRevocationOutstanding](#subscriberrevocationoutstanding)).

The endpoint reports `KAFKA_SUBSCRIBER_BROKERS`, the externally advertised list, which is normally a **different list** from `KAFKA_BROKERS`. `KAFKA_BROKERS` is what Blnk itself dials and is an address inside the deployment; a broker answers every client with the *advertised* address of the listener the connection arrived on, so handing a subscriber an internal address produces an unexplained connection timeout in the subscriber's logs days later, and publishes your internal topology for good measure. Set it.

**It is REQUIRED for this one endpoint, and there is no fallback.** With `KAFKA_SUBSCRIBER_BROKERS` unset, issuance answers `503 SUBSCRIBER_BROKERS_NOT_CONFIGURED` naming the variable, before a secret is generated and before the broker is touched — so nothing needs cleaning up.

A fallback to `KAFKA_BROKERS` existed, with a WARNING on every issuance, and the warning was not a control: it landed in Blnk's log while the consequence landed on the subscriber. An internal address handed to an outside subscriber surfaces as an unexplained connection timeout in *their* logs days later, and because the SASL secret is shown exactly once, diagnosing it costs a reissue. Refusing is the cheaper failure — it is immediate, it names the variable to set, and it discloses no internal address.

Every other endpoint runs without this variable; only issuance requires it, and only because only an operator knows the externally advertised list. **A deployment whose subscribers really are in-cluster sets `KAFKA_SUBSCRIBER_BROKERS` to the same value as `KAFKA_BROKERS`** — one line that makes the claim explicit rather than implicit in a fallback. Start-up logs a warning when the two lists are identical, so that choice stays visible.

For a subscriber carrying a `partition_key_prefix` the reported list is different again: it is `KAFKA_KEY_SCOPE_GATEWAY_BROKERS`, the declared key-authorising component's own addresses, substituted in place of this list so the credential names the endpoint that keeps its scope. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker).

The refusals worth recognising:

| Status | `error_detail.code` | What to do |
|--------|--------------------|------------|
| `400` | `GEN_MISSING_PARAMETER` | No identifier in the route. |
| `400` | `GEN_VALIDATION_ERROR` | No Kafka identity can be derived from that identifier. Fix the id. |
| `403` | `AUTH_MASTER_KEY_REQUIRED` | Use the master key. |
| `403` | `SUBSCRIBER_INSECURE_TRANSPORT` | The channel is not established as confidential — see [the transport contract](#the-transport-contract-this-endpoint-requires) directly below. |
| `404` | `SUBSCRIBER_NOT_FOUND` | Register the subscriber first with `POST /subscribers`. |
| `409` | `SUBSCRIBER_GRANT_EMPTY` | The subscriber is authorised for no topics. Set `authorized_topics`. |
| `409` | `SUBSCRIBER_ACCESS_EXCEEDS_AUTHORIZATION` | The principal already holds foreign `Allow` bindings granting more than the registry records, so no credential was issued and the one written moments earlier was revoked. **Not retryable** until a human removes the bindings at the broker or records the access on the subscriber — see [Foreign ACL bindings](#foreign-acl-bindings-and-why-issuance-refuses-on-them). |
| `409` | `SUBSCRIBER_DEPROVISIONING`, *"This subscriber is being deregistered, so no credential will be issued for it"* | The row carries a revocation tombstone, so its broker-side access is being torn down and the cleanup has not finished. Minting a credential would re-arm a principal mid-removal, and the deregistration already in flight would then delete the row recording it. **Finish or reverse the deregistration first**; retrying unchanged will keep being refused. The same code answers `PUT /subscribers/{id}` on a tombstoned row, with the message naming the authorization change instead, and it is raised whether the tombstone was already there when the row was read or landed while the operation was in flight. |
| `409` | `GEN_CONFLICT`, *"No provisioning claim is held for this subscriber, so the operation was abandoned"* | A **concurrent issuance for the same subscriber superseded this one.** Another call won the race, so this request's credential is not the live one. Do not retry blindly: re-read the subscriber to see the issuance that landed, and re-issue only if you still need a credential of your own — a fresh issuance replaces whatever the other call created. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | **No broker configured** — `KAFKA_BROKERS` is empty, so there is nothing to provision against and the publisher is a no-op. A broker that *is* configured but unreachable is a different answer: it lands on `SUBSCRIBER_PROVISIONING_FAILED` two rows down. |
| `503` | `SUBSCRIBER_BROKERS_NOT_CONFIGURED` | `KAFKA_SUBSCRIBER_BROKERS` is not set. There is no fallback to `KAFKA_BROKERS`, whose addresses are internal to the deployment. Set the externally advertised list — to the same value as `KAFKA_BROKERS` if subscribers really are in-cluster. Nothing was minted. |
| `503` | `SUBSCRIBER_PROVISIONING_FAILED` | **Every broker-side fault, including a broker that is down**: refused the credential or its bindings, unreachable, or accepting and never answering — **or** the issuance budget ran out. See [what one code covering both means](#one-code-covers-both-a-refusal-and-a-spent-budget) below; `error_detail.details.retryable` is `false` for the broker faults and `true` for a spent budget, and the message tells you which happened. Check reachability first, then the admin credential and the authorizer. |

### The transport contract this endpoint requires

This is the only endpoint in Blnk whose response body contains a secret, and it contains it exactly once — the password is not stored and cannot be read back, so the response carrying it is the single opportunity to disclose it to anything on the wire. Authentication answers *who* is asking. It says nothing about whether the answer can be read by somebody else on the way back, and those are independent: **a correct master key over plaintext HTTP is a correctly authorised credential leak.**

So the endpoint refuses unless the request arrived over a channel this deployment has **established** as confidential, and answers `403` with `error_detail.code` of `SUBSCRIBER_INSECURE_TRANSPORT` when it has not. Three channels qualify, and each is something the process can establish rather than assume:

| Channel | How the process establishes it | What you configure |
|---|---|---|
| **TLS terminated in-process** | The request carries a completed TLS handshake *this process* performed. Proven, not asserted. | `BLNK_SERVER_SSL` |
| **A declared proxy boundary, entered through a declared proxy** | The deployment declares that a proxy in front of Blnk terminates TLS and sets `X-Forwarded-Proto`, **and** the request arrived from a socket peer that proxy list names; the header is then believed, and only a value of `https` qualifies. | `BLNK_SERVER_TRUST_FORWARDED_PROTO=true` **with** `BLNK_SERVER_TRUSTED_PROXIES` |
| **A declared local-development host with a loopback peer** | The far end of the accepted socket is in `127.0.0.0/8` or is `::1` **and** the deployment has declared that no proxy sits in front of it. Read from the socket, never from `X-Forwarded-For`. | `BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE=true` — **local development only** |

Anything else is refused. That is deny-by-default: a deployment that has declared nothing gets no secret on the wire — **including a loopback caller**, which is the one people are surprised by.

#### A loopback peer is not proof of anything on its own, which is why the third channel is a declaration

`RemoteAddr` is the far end of the accepted socket, so a loopback value establishes that **the last hop** stayed on this host. It establishes nothing about the hop before it, and the shape where that matters is completely ordinary: a reverse proxy on the same host — nginx, Caddy, an Envoy sidecar — accepts a request from the internet over plain `http` and forwards it to Blnk over `127.0.0.1`. From inside the process that request is **indistinguishable** from an operator running `curl` inside the container, and answering it puts a one-time SASL password on the public hop.

Blnk does not guess between them. Only the deployment knows which it is, so the loopback channel is a declaration exactly like the proxy one:

- **Set `BLNK_SERVER_ALLOW_LOOPBACK_CREDENTIAL_ISSUANCE=true` for local development**, where the plaintext listener and the loopback caller are the whole topology. The Compose stack sets it for that reason.
- **Leave it unset everywhere else**, including on a single-host production deployment with a proxy in front of Blnk. There, issue credentials over TLS or through the declared proxy; a loopback `curl` on that host is refused, and that refusal is correct rather than inconvenient.

There is no configuration under which a loopback peer is believed while a proxy is also declared to be in front of the process: the two are separate declarations, and each is evaluated on its own.

**`403` rather than `400` or `426`.** The request is well formed and the caller is authenticated and authorised; the server is declining to fulfil it, which is what `403` means. It shares that status with `AUTH_MASTER_KEY_REQUIRED`, so anything distinguishing "the transport was refused" from "the caller was refused" must read `error_detail.code` and never the status alone. `426 Upgrade Required` was considered and rejected: it mandates an `Upgrade` header describing an in-band protocol switch, which is not what a deployment behind an ingress needs to do about this.

#### If you enable `BLNK_SERVER_TRUST_FORWARDED_PROTO`, these two obligations come with it

`X-Forwarded-Proto` is a request header. **Any client can send it**, and no process can tell a proxy's value from a caller's — which is why Blnk believes it only when you declare the topology, once and explicitly, instead of letting every request assert its own. Declaring it moves the trust to your network, so your network has to hold it:

- **The proxy must SET the header, overwriting whatever the client sent — never append to it.** A proxy that appends leaves the client's value in place, and a caller can then supply `https` itself over a plaintext hop. Most ingress controllers do the right thing by default; confirm yours does rather than assuming, because the failure is silent and the consequence is a disclosed password.
- **No request path may reach the process or the pod directly.** Once the declaration is in force, a request that bypasses the proxy would be believed on the strength of a header it wrote itself. Enforce this in the network — a `NetworkPolicy` admitting only the ingress, a security group, a listener bound to the proxy's interface — not by convention. The loopback branch above is deliberately narrow for the same reason: it reads the peer from the accepted **socket**, never from `X-Forwarded-For`, so a remote caller cannot present itself as local.
- **And `BLNK_SERVER_TRUSTED_PROXIES` must name that proxy, because it is the half of the obligation above that Blnk itself checks.** The header is believed only on a request whose socket peer falls inside one of those ranges, so a caller arriving by any other route — a pod IP, a `kubectl port-forward`, a second Service, an ingress rule passing the client's header through — is refused however it sets the header. It is the same list that makes a forwarded *client address* trustworthy, deliberately: one proxy, one answer.

  ```bash
  # The production shape. Both, or the channel establishes nothing.
  BLNK_SERVER_TRUST_FORWARDED_PROTO=true
  BLNK_SERVER_TRUSTED_PROXIES=10.0.0.0/8          # the ranges your ingress dials from
  ```

  A **universal range** (`0.0.0.0/0`, `::/0`) is not an allowlist — it matches every peer, which is exactly the state the declaration replaces — so it is skipped, and a list of nothing else counts as empty. A malformed entry matches nothing.

  **With `BLNK_SERVER_SECURE=true`, the flag with no usable allowlist is refused at configuration load**, naming the variable to set. Outside secure mode it is a warning and the channel simply establishes nothing, which is what keeps the Compose stack and the test suite working. Blnk still cannot see the hop in front of the proxy, so the two obligations above remain yours; what this closes is the request that never went through the proxy at all.

A declaration plus a header reading anything other than `https` does not qualify: the proxy is reporting a plaintext client hop, and the request falls through to the loopback test — which, unless this host has also been declared local-development, refuses it.

> **One deliberate asymmetry.** The security-headers middleware takes `X-Forwarded-Proto` at face value for the same question, and that is not an inconsistency. Believing it there merely adds an HSTS header that a plaintext client ignores; believing it here would disclose a password. The stricter rule is applied where the cost of being wrong is a credential.


### The cleanup runs AFTER the response, on a clock of its own

Two facts, and the order between them is what an operator has to know:

1. **The response comes back inside the five seconds.** That is the contract.
2. **On the server role, the cleanup a failed issuance owes runs AFTER that response has been sent** — on its own goroutine, bounded by five seconds measured from when the cleanup starts, with the provisioning fence still held until it finishes.

Issuance touches the broker up to four times and the registry twice, and a failure part-way through leaves a **live SASL credential with ACL bindings that no registry row records** — unfindable through the API, so unrevokable by any means short of the Kafka CLI. Undoing that means deleting the credential at the broker, clearing the registry's credential record, and releasing the provisioning fence: two more broker round trips and up to two local writes. **None of it changes the caller's answer**, so making the caller wait for it is what turned a five-second contract into a ten-second one on success and a twenty-five-second one on failure. The work is therefore scheduled rather than awaited.

**Scheduled is not fire-and-forget**, and the difference is what makes the endpoint's answer trustworthy:

| | What happens | Why it is safe |
|---|---|---|
| The response | Returns as soon as the forward path is done or has failed | Its state flags describe the state **at the moment of answering**, not a prediction: a failed issuance whose cleanup has not run yet answers `credential_written: true`, `compensated: false`. |
| The cleanup | Runs after the response, detached from the caller's cancellation, bounded by its own five seconds measured from when it starts | Detached because *the deadline expiring is the commonest reason cleanup is needed at all*, and measured from its own start for the same reason: a cleanup handed a budget the request already spent begins with nothing left. Still bounded, so it cannot hang forever. |
| The fence | Held until the cleanup finishes, then released by the same task | A retry that arrives while cleanup is in flight is answered `409` rather than being allowed to mint a fresh credential this task would then revoke. |
| The record | A durable marker is written on the subscriber row before the task ends | The residue survives the process, so nothing depends on an operator having read a log line. |

**`credential_written: true` means "may exist", not "definitely exists", and that is deliberate.** A credential write whose reply never arrives — the ordinary shape of a deadline expiring against a congested broker — tells you nothing about whether the broker applied it. Blnk reads that as written, so the credential is revoked and an obligation is filed as though it had landed; revoking a credential that was never created is a no-op the broker reports as success, so the safe reading costs nothing. The one outcome reported as NOT written is a broker that answered about this principal and refused, because then there is genuinely nothing to undo — and revoking anyway would destroy a credential the subscriber may legitimately hold from an earlier issuance. Before that distinction existed, a 300-way burst left **145 principals at the broker that no log line, no registry column and no settlement sweep knew about**, against 18 that were correctly reported.

The forward path still does not get all five seconds, and the cleanup no longer depends on what is left of them:

| | Bounded by | Why |
|---|---|---|
| Forward path — lookup, fence, four broker round trips, the issuance record | The deadline **minus 1.25 s** | The reserve is what an INLINE cleanup spends. Every in-process caller outside the API server, and every test, runs the cleanup inline while the caller waits — so there the reserve is the only time the cleanup can have without breaking the five-second answer. |
| Compensation, scheduled (the server role) | **Its own five seconds**, measured from when it starts | The response has already been sent, so no caller is waiting and there is no promise left to protect. Measuring the cleanup against a promise that was already kept does not keep it a second time; it only guarantees the cleanup fails, because the commonest reason a cleanup is needed at all is that the budget ran out. |
| Compensation, inline (no scheduler) | The request's **own** deadline, as before | The caller IS waiting, so the endpoint must still answer inside its budget rather than inside its budget plus a fresh one per level of cleanup. |

Either way it is **one window per cleanup**, shared by every level nested inside it: a broker-side cleanup reached through a registry-side one inherits the instant rather than starting a second window.

**Why this changed, and what it cost before.** The scheduled cleanup used to be bounded by the request's own deadline, with a 1.25-second floor for the case where that deadline had already passed. Under concurrent issuance that floor was the normal case rather than the exception, and 1.25 seconds is not enough for two broker round trips against a broker congested enough to have caused the failure in the first place. Measured on a 300-way concurrent burst against that behaviour: **132 SCRAM credentials left at the broker that could not be revoked, 132 ACL grants that could not be removed, and 264 provisioning fences that could not be released** — every one of them an operator instruction to run `kafka-acls` by hand. The same burst after the change left **none of the three**.

**A shutdown waits for it.** Outstanding tasks are tracked, so a graceful stop drains them; a hard kill abandons them, which is exactly what the durable markers below are for. A panic inside one is recovered and logged rather than ending the process, because by then there is no caller to attribute it to.

> On a deployment with no scheduler installed — which is every in-process caller outside the API server, and every test — the same work runs inline instead. The outcome is identical; only the timing differs, and the response then waits for it.

#### What to inspect before you retry, and what to revoke by hand

**Do not read the response's `compensated: false` as "the cleanup failed".** It means the cleanup had not finished when you were answered, which is the normal case. The state a few seconds later is recorded on the subscriber row, and these are the three markers to look at:

| Marker | Meaning | Remedy |
|---|---|---|
| `credential_cleanup_pending_at` | A credential exists at the broker that the registry records no issuance for, **or** one was revoked and the registry still records it. The settlement processor owns it. | None by hand. Watch `blnk_subscribers_credential_cleanup_pending` return to zero. |
| `credential_orphaned_at` | The registry's credential reference may not describe what actually authenticates — a claim was superseded between provisioning and recording. | Issue once more. The next issuance replaces whatever is at the broker, because Kafka holds one SCRAM credential per principal. |
| `revocation_failed_at` | A deregistration could not remove the broker-side credential. | Settlement retries it; a marker that does not clear is the manual case below. |

`GET /subscribers` projects all three, and the settlement processor — which runs in the server role beside the relay — retries what they describe until the row is clean. **Retrying issuance is safe while any of them is set**: provisioning is idempotent per principal, so a retry re-establishes the same boundary rather than adding a second one.

**The manual case is narrow, and it is loud.** If the cleanup cannot even record its marker — the registry is unreachable as well as the broker — then the log line is the only record that exists, and it says so in those words, at ERROR, naming the principal. That is the one situation where nothing will reconcile the residue for you:

```bash
# Delete the orphaned SCRAM credential.
kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_PROPS" \
  --alter --delete-config 'SCRAM-SHA-512' --entity-type users --entity-name '<principal>'
```

Then remove its bindings as [The ACL Model](#the-acl-model) describes. Treat such an ERROR line as work to do rather than as a report of something already handled; see [Reading the logs](#reading-the-logs-redacted-is-the-log-not-the-failure) for what else it will and will not tell you.

Two consequences to plan around:

- **A broker that needs more than 3.75 seconds for four round trips now fails where it would previously have succeeded at up to 5.** That is the deliberate cost of the reserve. It is not a tuning knob to widen; it is a broker to fix, and the `503` tells you to retry once you have.
- **The one case that can overrun the answer is a step that ignores its own context** — a driver call that does not honour cancellation, a blocking syscall, a stop-the-world pause. No deadline arithmetic reaches a call that never looks at its deadline. If you see issuance answering later than five seconds, that is this case, and the thing to investigate is the step that overran — not the cleanup, which by then has not started. The compensation still runs afterwards, on its own five seconds, because the alternative is leaving the unaccounted credential behind.
- **Concurrent provisioning is bounded to 16 conversations with the broker, and the seventeenth waits.** Administrative work is not a request but a conversation of round trips to one controller, and the controller's service rate tops out below 40 issuances a second whatever is offered to it. Beyond that, extra concurrency buys latency and nothing else — and once the latency passes five seconds the requests fail HALF-PROVISIONED, each leaving a credential the registry does not record. So the queue is formed here, where a request that cannot be served is refused **before its first write**, rather than inside the broker where nothing can refuse it. Compensating work draws on a separate reserve of 4, so revoking a credential never queues behind the attempts to mint more of them.

If compensation itself cannot finish inside its window — a broker that is hanging rather than refusing — it is **abandoned, marked on the row, and logged at ERROR with the principal named**. The marker is what settlement then retries; the log line is what you read when even the marker could not be written. Both paths are described under [What to inspect before you retry](#what-to-inspect-before-you-retry-and-what-to-revoke-by-hand) above.

#### One code covers both a refusal and a spent budget

`SUBSCRIBER_PROVISIONING_FAILED` and its `503` is the answer to **two** conditions, and the message and `error_detail.details.retryable` are what tell them apart. One code is deliberate: the endpoint's published error taxonomy carries no separate timeout code, because a status a client's retry logic branches on is part of the public contract rather than an implementation detail. What the two conditions do **not** share is the reaction — a spent budget is worth retrying as it stands, a broker fault is not until the broker is fixed — and that is exactly what the flag says.

**The broker saying no, or not being there.** Every inducible broker fault produces it, and produces it fast — a refused connection or a broker that accepts and never answers is reported in tens of milliseconds, not after the budget expires, because the failure is observed rather than waited for. If issuance is failing, this is almost certainly what happened, and the thing to check is the admin credential, the authorizer, and whether `KAFKA_BROKERS` names a reachable listener. The message names the broker.

**Blnk running out of time**, which needs the work to be genuinely *slow* rather than broken — a broker answering but pathologically late, a registry write blocked behind a long-running transaction, or a host under enough pressure that the process does not get scheduled. It is rare by construction: the forward path is bounded well under the ceiling and a broker that is merely unreachable fails long before the clock runs out. The message says the budget was not met (*"did not complete within …"*, *"was cancelled before it completed"*, or the ceiling's own wording), and the same message appears in the log line. Treat it as a latency investigation, not an authorization one, and note that the request may have taken longer than five seconds to answer — see the bound table above for why.

**Branch on `retryable` first, then on the message, or better, on the log.** Every expiry in the issuance path — the broker half, the registry half and the request ceiling alike — reports this one code, so a client does not need to know which dependency consumed the budget in order to recognise the outcome, and it is not told a dependency is down when the truth is that time ran out. `error_detail.details.retryable` then separates the two conditions above, and it is the field to branch on:

| Condition | `retryable` | What the message says | What to do |
|---|---|---|---|
| The broker refused, is unreachable, or accepts and never answers | `false` | *"Provisioning the Kafka principal failed at the broker"* | Fix the broker, the admin credential or the authorizer. Retrying unchanged fails the same way, in milliseconds. |
| The issuance budget was spent, or the caller went away | `true` | *"did not complete within …"*, *"was cancelled before it completed"* | Retry as it stands. |

The asymmetry is the point: `false` here does not mean "unsafe to retry", it means "a retry will not succeed until something is fixed".

Both are safe to *issue* again — a retry cannot double-provision — and for both the compensation described above is **owed and scheduled, not completed, by the time the answer is sent** — a retry re-provisions the same boundary idempotently either way, and the provisioning fence makes an immediate retry wait rather than race the cleanup. What the code does not promise is that the compensation *succeeded*. Usually it does, and the revocation is confirmed: the broker is clean, the generated secret is dead, and the outcome is logged as a warning moments after your response. When the broker hangs rather than refusing, the compensation is abandoned inside its window and **a live SASL credential is left behind for a principal the registry records no issuance for** — recorded as `credential_cleanup_pending_at` for settlement to retry, and logged at ERROR naming the principal when even that record could not be written. So check the markers before concluding anything, and treat an ERROR line naming a principal as work to do; both procedures are above. A spent budget in particular does **not** mean "the forward path may have half-worked and no cleanup was tried" — that is what holding the reserve back prevents.

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
resp="$(mktemp)"                       # a 0600 file in $TMPDIR (often the shared /tmp)
trap 'rm -f "$resp"' EXIT INT TERM     # disposed even on failure or interrupt

curl -sS -X POST "$BLNK_API/subscribers/sub_9f8d3c214b7a5e6f/kafka-credentials" \
  --config "$BLNK_CURL_CONFIG" \
  -o "$resp"

# Straight into the secret manager. The password is never echoed and never stored by Blnk.
jq -re .password < "$resp" | vault kv put -mount=secret blnk/sub_9f8d3c214b7a5e6f password=-

# Confirm this was a rotation rather than a first issuance, and check the fingerprint changed.
jq '{replaced, credential_fingerprint, issued_at, username, brokers}' < "$resp"
```

**`-o` is not optional here, and `--config "$BLNK_CURL_CONFIG"` is the same file established in [Keep credentials out of process arguments](#keep-credentials-out-of-process-arguments).** Without `-o` the response body — which contains the one and only copy of the new password — is written to your terminal, where it lands in scrollback, in a terminal-recording session, and in the CI transcript if this is run from a job. Blnk cannot re-issue the same secret, so a leaked one has to be rotated again and every consumer reconfigured a second time.

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

**On the message, not in the log, the reason can only be partly inferred.** `failure_metadata` carries five fields — `original_topic`, `error_reason`, `attempt_count`, `first_attempted_at`, `last_attempted_at` — and `terminal_reason` is not one of them. So a consumer of a dead-letter topic reads `attempt_count`, and the inference works in one direction only:

- **`attempt_count` below `RELAY_MAX_RETRY_ATTEMPTS` proves a permanent failure ended it early.** The budget was deliberately left unspent, which is the only way a row can be terminal with attempts remaining.
- **`attempt_count` equal to the maximum is ambiguous.** A *permanent* failure on the final permitted attempt produces exactly the same count as a spent budget, and — as the split below explains — a *transient* failure on that attempt is also terminal, as `budget_spent`. The count cannot separate them.

To tell them apart, read `error_reason` on the message, which carries the broker's own words, or match on `terminal_reason` in the server log, which is where the two paths are separable by field rather than by arithmetic.

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

Put it back to empty — which means `info` — when the investigation is over: debug is verbose in proportion to throughput.

```bash
# Compose: clear the variable and restart the same two services.
BLNK_LOG_LEVEL= docker compose up -d server worker

# Kubernetes: set the ConfigMap key back to empty and roll.
kubectl -n blnk patch configmap blnk-config --type merge -p '{"data":{"BLNK_LOG_LEVEL":""}}'
kubectl -n blnk rollout restart deployment/server deployment/worker
```

**Failure logging is unaffected either way**: every failed publish attempt is logged at `warning` with its attempt number and error reason at every level, so nothing above depends on having raised it.

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
| `event_type` | Exact match on the event name, e.g. `transaction.applied`. Surrounding whitespace is ignored; a value carrying a NUL byte (`%00`) is refused, because no stored value can contain one. |
| `topic` | Exact match on the **original category** topic, e.g. `blnk.transactions`. Refused on a NUL byte, as `event_type` is. |
| `dlt_topic` | The same filter expressed as the `.dlt` sibling, e.g. `blnk.transactions.dlt`. Refused on a NUL byte, as `event_type` is. |
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
| `topic_missing` | The destination topic does not exist on the broker — **or it exists and the producer principal cannot see it**, because a principal holding no `Describe` is answered `UNKNOWN_TOPIC_OR_PARTITION` rather than a denial. |
| `message_too_large` | The event exceeds the configured publish maximum. |
| `timeout` | The attempt exceeded its deadline. |
| `persistence_failure` | The failure was Blnk's own database, not Kafka. The event is intact; the relay's bookkeeping failed. |
| `unclassified` | The stored text matched none of the above. This value says so rather than guessing — go read the verbatim text. |

The classification is derived from the stored text at the response boundary, and `authorization_denied` is checked before `broker_unavailable` because a broker can report both in one message and the authorization half is the actionable one: it will not clear on its own.

#### Retrieving the verbatim `error_reason`

Two places hold it, and neither is the API:

```bash
# From the outbox row — the authoritative record, and available with the broker down.
$BLNK_PSQL -c \
  "SELECT event_id, status, attempts, last_error
     FROM blnk.event_outbox
    WHERE event_id = '<event_id>'"
```

```bash
# From the dead-letter message itself, for a row whose status is dead_lettered.
# --timeout-ms is what makes this RETURN: --max-messages alone waits for the 200th
# record on a topic that may hold two. grep '^{' keeps anything that is not a record
# out of jq — see "Where to run the Kafka CLI" for the line it is guarding against.
kafka-console-consumer.sh --bootstrap-server "$KAFKA_BROKERS" \
  --topic blnk.transactions.dlt --from-beginning \
  --max-messages 200 --timeout-ms 15000 \
  --command-config "$KAFKA_CLIENT_CONFIG" 2>/dev/null \
  | grep '^{' \
  | jq -r 'select(.event_id == "<event_id>") | .failure_metadata.error_reason'
```

Both guards are load-bearing rather than tidy. A drained topic makes the consumer wait out its
timeout and then exit non-zero with a `TimeoutException` on **stderr** — expected, which is what
`2>/dev/null` is for — while a deprecation warning would arrive on **stdout** and take `jq` with it.

The outbox row's `last_error` column is the same text `failure_metadata.error_reason` was built from, so the two agree **at the moment the event was dead-lettered**. Prefer the row: it exists for a `failed` event too, which has no dead-letter message yet.

> **They stop agreeing after a failed replay, and knowing which is which is the difference between triaging the original fault and triaging your own retry.** A replay that fails writes its own reason into `last_error`, so the row — and therefore the API's `failure_reason`, which is classified from that column — now describes the **most recent** attempt. `failure_metadata`, on the row and in the dead-letter message alike, is never rewritten: it preserves the reason the event was dead-lettered in the first place. So read `failure_reason` for "what is wrong now" and `failure_metadata.error_reason` for "what went wrong originally", and when the two disagree, treat the disagreement as the useful signal — the original cause was fixed, or was never the cause, and something else is refusing the event now.

**The two timestamps together are the most useful field in the object**, because their difference bounds the window the failure persisted over, and that is how you tell a transient outage from a poison message:

- A gap of roughly **15 seconds** — the whole backoff schedule and nothing more — means every attempt failed back to back. The cause was continuous for the entire window: a broker that was down, a topic that does not exist, a credential that is wrong, an ACL that forbids the write. Fix the cause and replay; the event itself is fine.
- A gap **much longer than the schedule** has three explanations, and they are not equally likely. **Rule out same-key queuing first**, because it needs no fault at all: the relay claims at most **one row per effective key per tick** — a candidate is skipped while any earlier row sharing its key is still `pending` or `processing` — so a burst on one key drains at roughly one event per poll interval (1 s at the defaults), and a row whose predecessor is *failing* waits out that predecessor's whole retry ladder before its own first attempt. Five events seeded on one key dispatched at 1-second spacing for a 3.98 s span with no fault present. `system.error` is the worst case: its payload yields no aggregate, so both `partition_key` and `aggregate_id` fall back to the event type and **every system error in the deployment shares the single key `system.error`**. Only after that comes **relay restarts or lease expiries** — check for the lease-acquired and relay-started lines in the window — and then a **cause that came and went**. Distinguish them with one query: if `blnk.event_outbox` holds rows with the same `COALESCE(ledger_id, partition_key)` and an earlier `occurred_at`, the wait was the queue, not an outage.
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
| `topic_missing` | The destination topic does not exist, or is invisible to the producer's principal | No — provision the topic, or grant the principal `Describe` on it, first |
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
| `topic_missing` | Provisioning never ran, `KAFKA_TOPIC_PREFIX` changed and the new namespace was never created, **or the topic is there and the producer holds no `Describe` on it** — Kafka reports both the same way | List the topics with the **admin** credential first: absent means provisioning, so re-run `make kafka_provision` and verify the eight names with `kafka-topics.sh --describe`. Present means the ACL half, so read the producer's bindings back with `kafka-acls.sh --list --principal "User:$KAFKA_SASL_USER"` and repair the grant. Then replay. |
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
| `500` | `EVENT_REPLAY_FAILED` | The re-publish failed, or it succeeded and the outbox entry could not be cleared, **or** the request was abandoned before the broker acknowledged it — the caller disconnected, or its deadline expired. The message tells the three apart, and the abandoned case says the event is still dead-lettered, so the request can simply be repeated. |
| `503` | `EVENT_KAFKA_UNAVAILABLE` | No broker is configured, or the broker is down. Fix that first. An abandoned request is deliberately **not** reported here: the broker never stopped answering, and sending you to look at it would waste the incident. |

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
#
#    include_offsets=best_effort is REQUIRED here, and only because of `dispatched`.
#    Counting the dispatched population is an exact count over the one unbounded
#    table in the pipeline, so it is taken only when asked for; the default reading
#    omits the key and sets dispatched_history_counted:false. best_effort rather than
#    true so a deployment with no reachable broker still answers 200 with the counts.
curl -sS "$BLNK_API/events/stats?include_offsets=best_effort" --config "$BLNK_CURL_CONFIG" \
| jq '{dispatched, dispatched_history_counted, failed, dead_lettered, replaying}'
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

**Two defaults, and they differ deliberately.** The software default is `0` — disabled — because deleting ledger-adjacent records is a decision only an operator can take. The reference Kubernetes manifests ship `3`, because a manifest has to name a volume size and a retention period that agree with each other; see [Storage](#storage--size-the-volume-from-the-row-not-from-a-round-number) for the derivation and for what changing the period costs.

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
**scoring zero message loss requires that audit.** The screen alone cannot score it.

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

`CountEventOutboxByStatus` in `database/event_outbox.go` exists specifically to serve this check, and `GET /events/stats?include_offsets=true` exists to expose it. Neither is a general-purpose reporting API; **do not build dashboards on them, and the endpoint now makes that structural rather than advisory.**

The dispatched count is an exact `COUNT` over the only population in `blnk.event_outbox` that grows without bound — 43.2 million rows a day at the 500-events-per-second target — so it is taken **only for a request that asks for the broker side**. Every other count is exact, complete for all time, and taken on every request, because those are the populations you act on and any of them can legitimately be older than any window.

**A census is reused for one second, and `generated_at` is when it was taken rather than when you were answered.** Counting open work means visiting it, so the census is inherently proportional to the un-drained population — and repeating it per request made that cost proportional to the request rate as well. Measured on a 400,000-row open backlog, one census takes about 34 ms and two parallel query workers with it, so sixteen concurrent readers were costing 48 database backends to answer one question that has one answer. Concurrent readers now share the census already in progress, and a census is reused for one second afterwards: 48 requests at concurrency 16 cost **2 censuses instead of 51**, and 96 at concurrency 32 cost **5 instead of 132**.

The reuse window is deliberately far below anything that polls this endpoint — the load harness's settling gate polls every 5 seconds, the metrics collector reads the same census every 15 — and it is *reported* rather than hidden: `generated_at` is the instant the counts were read, which is what that field has always claimed to be. Compare it against your own clock if you need to know a reading's age. Nothing else in the response is reused: the interval audit is compared against offsets read in the same request, so serving it from a moment ago would compare two different instants.

| What you send | What you get | What it costs |
|---|---|---|
| nothing, or `include_offsets=false` | the exact unresolved inventory: `pending`, `processing`, `webhook_pending`, `failed`, `dead_lettered`, `replaying`, plus `producer_atomicity`. `dispatched` is **absent** and `dispatched_history_counted` is `false`. | one index-only aggregate over outstanding work. This is what a dashboard, a health check or a drain loop should use. |
| `include_offsets=best_effort` | the above **plus** `dispatched` over the window, plus the broker side and the verdict when a broker can be read. An unreadable broker omits the broker keys and still answers `200`. | the history count, and one broker round trip. |
| `include_offsets=true` | as `best_effort`, except an unreadable broker answers `503 EVENT_KAFKA_UNAVAILABLE`. | the same, and it fails loudly. **This is what this reconciliation uses.** |

`dispatched_history_counted` is on every response, including when it is `false`. Read it before reading `dispatched`: an absent `dispatched` key means "not counted", never "none were dispatched", and a script that treats the two as one reports total loss on a healthy pipeline.

The `window` parameter bounds the dispatched count **and nothing else**, and its ceiling is its default of `24h` — you may narrow it (`?window=15m` for a twenty-minute incident) but not widen it. A week's exact dispatched count is 302.4 million index entries at the target rate, which no request timeout survives; a longer retrospective comes from the dead-letter inventory and the per-`event_id` audit, both of which are bounded by retention rather than by this parameter.

### Step 1 — Take the snapshot

```bash
curl -sS "$BLNK_API/events/stats?include_offsets=true" --config "$BLNK_CURL_CONFIG" | jq . > recon-$(date -u +%Y%m%dT%H%M%SZ).json
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
  "dispatched_history_counted": true,
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

The seven counts above are a census of rows that **exist**. Some events are captured from an *intent* recorded atomically with their mutation rather than as an outbox row, and while an intent is outstanding its event has not been captured yet and appears in **no** status. A reconciliation that read only the census would find it internally consistent while those events were still owed, so this object is reported alongside it.

There are two such intents, and they are not equally common. A **bulk-batch coordinator row** is the ordinary route for every batch summary. A **balance-monitor handoff** is not: the atomic writers evaluate a moved balance's monitors inside their own transaction and insert the alert row there, so a movement made by any current release writes no handoff at all. The handoff counts therefore describe a finite, draining population — rows written by releases that predate the in-transaction capture, and rows written by a process with no alert capture registered. On a deployment that has finished draining them they sit at zero permanently, and a `monitor_handoff_pending` that starts climbing again after that is worth investigating rather than ignoring.

| Field | Meaning | What to do |
|-------|---------|-----------|
| `monitor_handoff_pending` | Balance movements whose monitors have not been judged yet. Only pre-capture rows reach this count | Nothing while the pre-capture population is draining. A number that climbs after it has reached zero means something is writing handoffs, so check that the process moving balances is one that registers the alert capture |
| `monitor_handoff_processing` | Handoffs a processor currently holds | Nothing |
| `monitor_handoff_completed` | Handoffs judged, overwhelmingly "judged, nothing fired" — which is why it dwarfs the number of alerts ever published | Nothing |
| `monitor_handoff_failed` | **Evaluation budget spent.** Each one is a balance movement whose monitor conditions were never judged, so any alert it should have produced does not exist and never will without intervention | Investigate. `last_error` on the row names the cause; the ERROR log carries the handoff id, the balance and the attempt count |
| `unfinalized_batches` | Asynchronous bulk batches that began and never reported an outcome, past a grace period so batches still legitimately running are excluded | Investigate with `oldest_unfinalized_batch_at`. The member transactions are durable and carry the batch id, so this is a missing **summary**, never lost money |
| `oldest_unfinalized_batch_at` | When the oldest outstanding batch began. Omitted when there are none | Age is what separates a large batch still running from an abandoned one, so the count alone is not actionable and this is |

**The whole object is omitted when it could not be read.** That is deliberate and it is the reading to check for: zero means "nothing is outstanding", which is exactly the answer this check must not be given when the truth is "we could not tell". An absent `producer_atomicity` on a response that otherwise has counts means the owed-event side was not measured, and the day's reconciliation is incomplete in that dimension however green the verdict below reads.

These counts are a **separate signal from the loss verdict** and do not feed it. An owed event is not a lost one — its intent is durable, and the event still arrives when the handoff is evaluated or the batch finalises. Only `monitor_handoff_failed` and a stale `unfinalized_batches` describe an event that will never exist, and neither can be seen anywhere in the arithmetic of Step 2.

**`include_offsets=true` is required here, and it must be spelled out.** Omitting the parameter — or
passing `false` — SKIPS the broker entirely and does not count `dispatched`, so the response carries
no offsets and no verdict: `offsets_complete` is `false` and there is nothing to reconcile against.
An earlier revision of this procedure said to leave the parameter off on the grounds that absent
meant best effort. It no longer does; see the posture table earlier in
[this procedure](#the-daily-outbox-versus-offset-reconciliation), which is authoritative. Following
the old instruction produces a snapshot that looks healthy because it measured nothing.

The three postures, and which one this check wants:

- **`true` — use this.** The broker read is REQUIRED, so a broker that cannot be read answers
  `503 EVENT_KAFKA_UNAVAILABLE`. A reconciliation that silently dropped half its input would report
  a verdict it did not compute, and this is the one procedure where failing loudly is the point.
- **`best_effort`** — attempts the broker, degrades to the counts and still answers `200`. Correct
  for a deployment with **no brokers configured at all**, where `true` would answer `503` for a
  reason that is not a fault; it is the only other posture that counts the dispatched history.
- **absent or `false`** — skips the broker and omits `dispatched`. Right for a dashboard, a health
  check or a drain loop, and wrong for this check.

There is deliberately no topic-narrowing parameter — the verdict compares the broker against
**every** outbox row that claims a publication, so measuring a subset of topics would manufacture a
shortfall and report loss that has not happened.

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
4. **`blnk_events_dispatched_total`** and **`blnk_events_dead_lettered_total`** over the same window. These are the per-event terminal counters, so their sum is directly comparable with a count of rows. Capture `blnk_events_broker_acknowledgements_total{purpose="original"}` and `blnk_events_published_total` alongside them: the first counts broker *writes* the broker acknowledged, the second counts *events* — once each, ever — so the amount by which the acknowledgements exceed the published count over the same window **is the redelivery volume**, which is itself evidence about how the relay was behaving. Do not compute that from `published` minus `dispatched`: that difference is the population still owed a legacy webhook, which is a different quantity and goes to zero at the sunset. The three counters and the questions they answer are set out in [metrics.md](metrics.md#three-counters-three-questions).
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

**This step is REQUIRED to score zero message loss. It is not an optional follow-up.**

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

- **whenever zero message loss is being scored or attested** — the screen cannot substitute for it;
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
   # grep '^{' is what keeps this honest: anything the CLI writes to stdout that is not a
   # record — a deprecation warning from an older flag spelling, for instance — aborts jq,
   # and an empty id list makes step 3 report every event as missing.
   docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 \
     --command-config /tmp/blnk-kafka/client-admin.properties \
     --topic blnk.transactions --from-beginning --timeout-ms 60000 2>/dev/null \
   | grep '^{' | jq -r .event_id
   ```

   **Check that this produced ids before going on.** `wc -l` the output. A count of zero on a
   deployment that has published anything is a fault in the pipeline above, not a finding — step 3
   compares set membership, so an empty broker side turns every outbox id into an apparent loss.

   Repeat per topic, concatenate, then `sort -u` — **the deduplication is the point of the exercise**, because a redelivered event legitimately appears more than once.

3. **Diff, in one direction only.** Ids present in the outbox and absent from the broker are the finding:

   ```bash
   comm -23 <(sort -u /tmp/outbox-ids.txt) <(sort -u /tmp/broker-ids.txt)
   ```

   Empty output is the pass. Any line is an event that claims a publication no record corresponds to, named individually — which is what Step 2 could only ever report as a number.

4. **Ignore the other direction.** Ids on the broker and not in the outbox are the expected residue of a retention purge that has already deleted the row (see Step 4), or of a window boundary. `comm -13` is not a finding.

**Run the consumer with `docker compose exec -T`, exactly as written.** Plain `docker exec` does not accept `-T` — it exits 125 with `unknown shorthand flag: 'T' in -T` and prints **nothing on stdout**, so piped into `jq -r .event_id` it contributes zero ids. The diff in step 3 then reports every outbox id as missing, or — with the operands the other way round — reports a clean audit over an empty broker side. Verified both ways on this stack: the documented form returned 6 event records, `docker exec -T` returned 0 with the error on stderr alone. If you must use plain `docker exec`, drop the `-T` and **check the exit status before believing the diff**.

Two further caveats. The consumer must start from an offset **inside** the window, or retention will manufacture a shortfall exactly as it does for the counting check. And the window has to be closed: an event captured while the consumer was running may legitimately be in the outbox and not yet on a topic, so bound the outbox query to occurrences that predate the consumer's start.

## Alert Response

`alerts/blnk-kafka-alerts.yml` defines **one rule group, `blnk-kafka-alerts`, evaluated every 30 seconds**, holding **fifteen** rules. Nine are CONDITION rules — a dead-letter entry left unresolved, a repair leg unable to clear its backlog, an outbox claim that is timing out so nothing is published, a subscriber falling behind, a credential awaiting revocation, an unaccounted credential, a refused revocation, and the two settlement rules — and six are MEASURABILITY rules, which fire when Blnk cannot tell whether a condition rule should. Each names this document as its `runbook_url`. Every rule has a row in the index above and a response section below; use the index rather than the reading order to reach one.

**Fifteen is this file's count, not the deployment's.** The Kubernetes projection mounts a second group, `blnk-infra-alerts`, carrying one further rule — [KafkaBrokerVolumeFilling](#kafkabrokervolumefilling) — which is absent under Compose because it reads a kubelet series nothing in this repository publishes. Its procedure sits with the Kubernetes material rather than below, and its `runbook_url` resolves there.

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
   # No include_offsets needed: all three of these are UNRESOLVED counts, which the
   # default reading takes exactly and in full. Adding it would buy an exact count of
   # the dispatched history you are not looking at.
   curl -sS "$BLNK_API/events/stats" --config "$BLNK_CURL_CONFIG" | jq '{failed, dead_lettered, replaying}'
   ```

   A non-zero `failed` count means at least one entry is **not replayable yet**: its retry budget is spent but the dead-letter write has not completed, so there may be no message on the topic at all, and replay refuses it with `EVENT_NOT_DEAD_LETTERED`. **Restore broker reachability so the dead-letter write completes**; the relay re-claims those rows itself. Only then replay.
3. **List the entries** for that topic — [Step 1](#step-1--list-the-dead-lettered-events).
4. **Read the failure metadata** and bound the failure window — [Step 2](#step-2--read-the-failure-metadata).
5. **Fix the underlying cause** — [Step 3](#step-3--decide). Replaying before the cause is fixed re-dead-letters the event and buys nothing.
6. **Replay** — [Step 4](#step-4--replay). This is the step that clears the alert, and it is the only one that can: the replay is what takes the entry out of the gauge's inventory, and it does so by making the row `dispatched`. There is no follow-up call — see [Step 5](#step-5--there-is-no-resolve-step).
7. **Confirm the gauge returns toward zero.** The collector re-reads it from authoritative state on every tick and records an **explicit zero** when a topic's inventory is empty; **that zero is the reading that clears the alert.** A gauge still above the threshold after a successful replay means entries remain — either rows still in `failed`, whose dead-letter write has yet to land, or `dead_lettered` rows you have not replayed.

**Which timestamp the age is measured from depends on the entry's state, and the difference matters when you are reading the number.** An entry already preserved on its `.dlt` sibling ages from its **last** publish attempt — the moment it was given up on. An entry whose `.dlt` write is still owed ages from its **first** attempt, because the repair pass re-claims exactly those rows every second and stamps `last_attempted_at` as it goes; measuring them from that column let the pass that was failing to preserve an event reset that event's own triage clock, and the entries that reached no topic at all were the only ones this alert could not fire for. So a `failed` entry's age is the age of the whole retry-and-preserve debt, which is what you want to know about it.

### EventRepairBacklogStuck

```text
expr:     blnk_events_repair_backlog > 0
for:      15m
severity: warning
```

**This is a count of rows, not an age**, and it is the companion to the rule above rather than a duplicate of it. `blnk_events_repair_backlog` is the only series either repair leg's outstanding work appears on: the dead-letter inventory the age rule reads covers the `dead_letter` leg's rows and **not** the `legacy_webhook` leg's, because those are `webhook_pending` — a state that inventory excludes and `blnk_outbox_pending` does not count either. It also fires on a `dead_letter` backlog that is filling quickly but has nothing in it fifteen minutes old yet, which is the earliest an operator can be told that events are reaching no topic.

**Zero is the healthy reading and it is published on every tick**, on every supported configuration, including a deployment running without Kafka. Both legs are self-limiting, which is why `> 0` is a legitimate comparison rather than a noisy one: the legacy leg abandons a row once it spends its enqueue budget, the relay settles rows left `webhook_pending` when the migration window closes, and a deployment with no webhook configured never enters that state at all. A repair pass runs on **every one-second poll tick**, so anything still owed fifteen minutes later is failing rather than queued.

Read the `leg` label first — the two legs need different actions.

1. **`leg="dead_letter"` is the urgent one.** Those rows spent their Kafka retry budget **and** their `<topic>.dlt` write also failed, so each is the only copy of an event that reached no Kafka topic at all. They appear in `GET /events/dead-letter` as `failed` rather than `dead_lettered`, and replay refuses them with `EVENT_NOT_DEAD_LETTERED` until the dead-letter write lands — see [The two states, and why only one is replayable](#the-two-states-and-why-only-one-is-replayable).

   ```bash
   curl -sS "$BLNK_API/events/stats" --config "$BLNK_CURL_CONFIG" | jq '{failed, dead_lettered}'
   ```

   Restore write access to the `.dlt` sibling, in this order, because each answer rules out the next: **broker reachability**, then **the sibling topic's existence** (`kafka-topics.sh --describe --topic <topic>.dlt`), then **the producer principal's `WRITE` ACL on it** (`kafka-acls.sh --list --topic <topic>.dlt`). A missing ACL is reported by the broker as `UNKNOWN_TOPIC_OR_PARTITION`, indistinguishable from a topic that does not exist, so check both — see [Reading the logs](#reading-the-logs-redacted-is-the-log-not-the-failure). The pass clears the backlog itself once the write succeeds; there is nothing to replay and no endpoint to call.
2. **`leg="legacy_webhook"` is rows whose Kafka leg is finished and whose legacy webhook enqueue keeps failing** inside the migration window. Kafka delivery is unaffected, so no subscriber consuming the topics is missing anything. Check Redis and the asynq queue. **Watch for this one clearing itself**: a row is abandoned once it spends its enqueue budget, which drops it out of the backlog with the webhook never delivered, so the log line naming the abandonment is the record of what a webhook subscriber missed.
3. **Check whether the pass is failing or merely too slow.** `blnk_events_repair_saturated{leg=...}` reads 1 when the last tick spent its whole repair budget with a full batch still coming back. A sustained 1 with a falling backlog is a recovery configured too slowly — raise `RELAY_REPAIR_MAX_BATCHES_PER_TICK` or `RELAY_REPAIR_BATCH_SIZE`. A 0 with a flat backlog is a failing pass, and the server log carries the cause on every attempt.
4. **Confirm the backlog returns to an explicit zero.** `blnk_events_repair_completed_total{leg=...}` is incremented per row driven to its destination, so `rate()` over it is the drain rate; divide the backlog by it for a time-to-clear.

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
          unless rate(blnk_subscribers_obligations_settled_total[30m]) > 0
for:      30m
severity: warning
```

**The settlement pass is not running, or cannot make progress.** This is the companion to the rule above and it fires half an hour earlier by design: that one needs an obligation to have gone stale, while this one fires as soon as work is outstanding and *nothing at all* is being discharged. So "the mechanism is broken" arrives before "this obligation is stale" rather than with it.

Both halves of the expression are required. A non-zero backlog alone is normal — an obligation raised a minute ago is expected to be outstanding — and a zero settlement rate alone is the healthy steady state, because most deployments never fail a subscriber operation at all.

**The second half is written `unless … > 0` rather than `and … == 0`, and the difference is what makes the rule able to fire at all.** `blnk_subscribers_obligations_settled_total` has no series until the pass discharges its first obligation, and `and` against an absent series yields an empty result — so the earlier form was silent on precisely the deployment it exists for: a pass that has never settled anything. `unless` returns the left-hand side when the right has nothing to match it, so a real backlog alerts before the counter's first sample. Vector matching is unchanged (`unless` matches on the full label set, so `instance` and `job` still pair a backlog with its own process's settle rate), and `$value` and `$labels` still describe the backlog because `unless` returns the left side. If you see this alert on a deployment where `blnk_subscribers_obligations_settled_total` returns nothing at all, that is the intended behaviour and not a broken query: read it as "no obligation has ever been settled here".

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
2. **Measure more per tick**, which is the usual answer: raise `RELAY_SUBSCRIBER_METRICS_BUDGET` so one sweep covers more rows (its alias `EVENT_METRICS_SUBSCRIBER_BUDGET` sets the same ceiling).
3. **Or tick more often**, if broker round trips rather than the wall clock are the constraint.
4. **Rule out failure as the cause.** A rotation that is slow because measurements are FAILING shows on `blnk_kafka_subscribers_unmeasured{reason="measure_failed"}` and needs the broker or the ACLs, not the budget.

### SubscriberLagCoverageIncomplete

```text
expr:     blnk_kafka_consumer_lag_inventory_complete == 0
for:      30m
severity: warning
```

**Some subscribers have no lag series at all.** Measuring one subscriber's lag costs two broker round trips per authorised topic, so a sweep examines at most `RELAY_SUBSCRIBER_METRICS_BUDGET` registry rows (default 200). A registry larger than the budget is not permanently truncated — the next sweep resumes where the last one stopped, so coverage **rotates** — but while this fires, **absence of a subscriber's lag series means nothing**: it may be caught up, or it may simply not have been looked at.

**The gauge measures the EXPORTED INVENTORY, not the last sweep, and the distinction is what makes this alert usable.** A rotating sweep never reaches the whole registry in one tick by design, and the readings of the subscribers it did not reach stay exported for ten minutes — so the inventory becomes complete several ticks before any single sweep does, and it is completeness of the inventory that decides whether a subscriber can be alerted on. The gauge previously published the *sweep's* figure, which is false on every tick of a rotation: measured with 400 subscribers against the default budget of 200, `blnk_kafka_consumer_lag_covered_subscribers` read 400, `blnk_subscribers_registered` read 400, every `blnk_kafka_subscribers_unmeasured` reason read 0 — and this alert fired anyway, permanently, with the instruction below telling the operator to read a breakdown that said nothing was wrong. It now agrees with those three figures by construction: if it is firing, at least one of them says why.

A subscriber authorised for **no topics** is not counted against completeness. There is no reading to take, so it appears under `blnk_kafka_subscribers_unmeasured{reason="unprovisioned"}` and is the row's own state rather than a gap in the collector.

`ConsumerLagMeasurementDegraded` is the different condition: there, a subscriber *was* measured and a partition would not answer. Here, the subscriber was never reached.

The thirty-minute dwell is the longest in this group deliberately. Rotation is the designed behaviour, so a single incomplete sweep is not a fault; what this catches is a registry that has outgrown the budget for long enough that a subscriber's lag is stale by more than a few sweeps.

**Read `blnk_kafka_subscribers_unmeasured` by `reason` before doing anything else** — the six values need six different actions, and only `budget` is answered by configuration. In particular, **`broker_unconfigured` means this deployment has no `KAFKA_BROKERS` at all**: those rows can never be measured, and the choice is to configure the brokers or to delete registry rows the deployment is not using. Raising the budget for them does nothing.

**A deployment running without Kafka and without subscriber rows does not reach this rule at all.** With no rows registered, the empty lag inventory covers the registry exactly, so `blnk_kafka_consumer_lag_inventory_complete` reads **1** and every reason reads zero. That is the documented no-Kafka steady state — see [Running Without Kafka](#running-without-kafka) — and it is a supported configuration rather than a fault.

1. **Decide whether it is size or failure.** Read the collector's own log line: it reports the subscribers examined, the budget, where the cursor resumed, `broker_unconfigured`, and both booleans — `sweep_complete` for "did this tick reach the end of the registry" and `inventory_complete` for "does every subscriber have a series". `sweep_complete=false` with `inventory_complete=true` is the ordinary rotation and is not this alert. `registry_count_failed=true` means the registry's SIZE could not be read, so coverage is unknowable rather than incomplete — that reports here because an unknown must never read as complete, and the remedy is the database rather than the budget. A registry comfortably inside the budget that still reports incomplete coverage is a failure, not a size problem — check `EventMetricsCollectionFailing` too.
2. **Raise the budget if the registry has genuinely grown**, remembering the cost is round trips per topic per subscriber per tick:

   ```bash
   # In .env, then restart the server role.
   RELAY_SUBSCRIBER_METRICS_BUDGET=500
   ```

   The value is clamped to a ceiling; a value above it is corrected with a warning rather than refused, so an over-large setting never prevents start-up.
3. **Do not reach for the collection interval.** It is fixed at 15 seconds and is not operator-configurable, and lengthening it would be the wrong direction even if it were: a longer interval means the rotation takes longer to come back round, so every subscriber's lag is *staler* — which is the condition this alert exists to report. If the broker round trips are genuinely the constraint, reduce the per-subscriber cost instead. The cost is round trips per **authorised topic** per subscriber per tick, so a registry of subscribers each granted every category costs several times one granted only what it consumes; narrowing over-broad grants is the lever that reduces the work rather than hiding it.
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

### EventRelayClaimTimingOut

```text
expr:     rate(blnk_events_relay_claims_total{outcome="timeout"}[10m]) > 0
for:      2m
severity: warning
```

**The relay is not claiming, so nothing is being published — and no other rule in this group can see it.** A claim cancelled at its budget returns no rows at all, so the relay publishes nothing for that tick. The events are still in the outbox, unharmed and un-attempted: no dead letter is written, no retry attempt is spent, no lag series moves. Every rule above stays quiet. The only symptoms are `blnk_outbox_pending` rising against a flat `blnk_events_published_total`, and this counter.

**Why `> 0` rather than a threshold.** The claim reads two bounded windows of the claim-order index, walks a bounded number of partition keys, and updates at most one batch of rows; its measured cost is single-digit milliseconds against a backlog of hundreds of thousands. A claim that runs for the whole 15 second budget is a plan that has gone wrong, so there is no acceptable rate of these. The 15 seconds is a constant in `event_relay.go`, not a setting — raising it would only lengthen the outage.

1. **How far has the claim drifted?** Read the p99 and compare it with the single-digit milliseconds a healthy claim takes:

   ```promql
   histogram_quantile(0.99, sum by (le) (rate(
     blnk_events_relay_claim_duration_seconds_bucket[5m])))
   ```

2. **How much work is each claim returning?** Rows per claim is the second half of the diagnosis. Near the batch size (100 by default) is healthy; near 1 means the claimable backlog is concentrated on very few partition keys, which is a key-spread problem rather than a claim problem — a single partition key is published strictly in order by one goroutine, so its throughput is bounded by the round trip to the broker however fast the claim is.

   ```promql
   sum(rate(blnk_events_relay_claimed_rows_total[5m]))
     / sum(rate(blnk_events_relay_claims_total{outcome="rows"}[5m]))
   ```

3. **Is the table's dead-tuple population ahead of autovacuum?** This is the common cause, and it is usually transient — a bulk import, a restore, or a very large backlog draining at once. `blnk.event_outbox` carries seventeen indexes, so every claim's `UPDATE` leaves seventeen dead index entries per row, and the claim's index scans have to step over them until vacuum reclaims them.

   ```sql
   SELECT n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
     FROM pg_stat_user_tables WHERE relname = 'event_outbox';
   ```

   `ANALYZE blnk.event_outbox;` first — a stale row estimate is enough on its own to move the planner off the index path. `VACUUM (ANALYZE) blnk.event_outbox;` if the dead-tuple count is large relative to the live one. Consider a more aggressive per-table autovacuum setting on a deployment that sustains a high publish rate.

4. **Is the claim still index-driven?** This is the check that separates a busy table from a regressed plan. `EXPLAIN (ANALYZE, BUFFERS)` on the claim must show index scans on `idx_event_outbox_effective_key_inflight`, `idx_event_outbox_claim_order`, `idx_event_outbox_status_open` and `event_outbox_pkey`, with **no `Seq Scan` on `event_outbox`** and a root-node buffer count in the low thousands. A sequential scan there is the regression; confirm the four indexes exist and are valid:

   ```sql
   SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE relname = 'event_outbox';
   SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;
   ```

5. **Is the database itself the constraint?** A claim cannot outrun the server it runs on. Check for a concurrent bulk load, a long-running transaction holding back vacuum (`pg_stat_activity` ordered by `xact_start`), or exhausted I/O.

**Nothing needs to be replayed afterwards.** A timed-out claim leaves every row exactly as it was, so the backlog drains on its own once the cause is cleared. The relay claims again on its next poll — recovery needs no operator action, which is precisely why this alert exists: without it, a relay in this state looks like a healthy relay with a growing queue.

6. **If the five checks above all look healthy, take a goroutine profile.** A counter can tell you the relay has stopped claiming; only a stack can tell you what it is blocked on. See [Profiling a wedged or slow relay](#profiling-a-wedged-or-slow-relay), directly below.

### Profiling a wedged or slow relay

**The runtime profiles are published under `/debug/pprof`, behind the same credential as `/metrics`.** They exist because the pipeline's worst failure is also its least diagnosable: a relay that has stopped draining leaves the process healthy, the queues healthy and the database idle, and the question that actually needs answering — *what is this goroutine waiting on* — has no answer anywhere in a metric. Measured during testing, a relay spent 504.8 seconds inside a single claim with no log line, no error and no counter, and the only way to learn anything about it was to kill the process, which destroys the evidence.

**The credential is the metrics bearer token, deliberately.** A profile discloses more than a counter does — a heap dump can carry payload bytes, a command line can carry a DSN — so the more sensitive surface must never be reachable where the less sensitive one is not. Both are gated by the same middleware, so the two cannot drift apart: in secure mode with `BLNK_METRICS_BEARER_TOKEN` unset, both refuse; outside secure mode with no token, both are open, which is the local-development posture the metrics endpoint already establishes. An unauthenticated request answers `401 AUTH_METRICS_TOKEN_REQUIRED`; a wrong token answers `401 AUTH_INVALID_BEARER_TOKEN`.

| Profile | Answers |
|---|---|
| `/debug/pprof/goroutine?debug=2` | Every goroutine's full stack, as text. **This is the one to take first for a wedged relay**: the claim's goroutine appears by name with the line it is blocked on. |
| `/debug/pprof/heap` | Live allocations by site — what unbounded memory is actually holding. |
| `/debug/pprof/allocs` | Everything allocated since start, which is what churns rather than what is retained. |
| `/debug/pprof/profile?seconds=30` | A CPU profile, for a relay that is busy rather than blocked. |
| `/debug/pprof/trace?seconds=5` | An execution trace, for scheduling and syscall latency. |
| `/debug/pprof/block`, `/debug/pprof/mutex` | Blocking and contention. **Both are empty** unless `runtime.SetBlockProfileRate` or `SetMutexProfileFraction` has been raised, which Blnk does not do: they carry a standing cost, and the two profiles above answer the questions this pipeline actually raises. |

```bash
# The stacks, as text, straight into a file you can attach to an incident.
curl -s -H "Authorization: Bearer $BLNK_METRICS_BEARER_TOKEN" \
  "http://blnk-server:5001/debug/pprof/goroutine?debug=2" > goroutines.txt

# Grep it for the relay first. A claim in flight shows database/sql in the stack;
# a publish in flight shows kafka-go.
grep -A 20 'EventRelayProcessor' goroutines.txt

# The heap, for go tool pprof. -http opens an interactive browser view.
curl -s -H "Authorization: Bearer $BLNK_METRICS_BEARER_TOKEN" \
  "http://blnk-server:5001/debug/pprof/heap" > heap.pb.gz
go tool pprof -top -nodecount=15 heap.pb.gz
```

**What a healthy relay's memory looks like, so a profile has something to be compared against.** Draining a 500,000-row backlog holds the process at a **90 MiB RSS plateau** with the in-use Go heap under 21 MiB, and the largest single allocation site is the in-process cache allocated once at start-up — not the claim and not the publish path. Memory that tracks the pending backlog is therefore a regression rather than a characteristic, and `go tool pprof -top` on the heap will name the site.

**Both roles set `GOMEMLIMIT`, and a deployment that overrides the memory limit must move it too.** Go's collector sizes the heap against `GOGC` alone unless told otherwise, so it does not know a cgroup limit exists: it grows until the kernel OOM-kills the container, and the pod restarts mid-backlog with no panic and no log line to explain it. The manifests set `GOMEMLIMIT` to roughly seven-eighths of each role's memory limit — 448MiB against the server's 512Mi, 672MiB against the worker's 768Mi — leaving the remainder for what the Go heap is not: the runtime's stacks and metadata, the SQL and Kafka drivers' buffers, and the binary's own pages. It is a *soft* limit, so approaching it makes the collector work harder rather than failing an allocation, which is the right trade when the alternative to a slower relay is a killed one.

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

1. **Open `http://localhost:9090/rules`.** The group `blnk-kafka-alerts` must be listed with all **fifteen** rules and a 30-second evaluation interval. **Check `/rules`, not `/targets`** — a target can show `UP` while the rules never loaded, and a rule that never loaded reports no error anywhere.
2. **The rules are gated in CI; this step is how you reproduce that gate locally.** One
   command runs everything:

   ```bash
   make alerts
   ```

   That is `promtool check rules` and `promtool test rules` on `alerts/blnk-kafka-alerts.yml`,
   then both again on the rule file as `infrastructure/k8s-manifests/prometheus-configmap.yaml`
   projects it. It uses a `promtool` on your `PATH` if you have one and otherwise runs the
   `prom/prometheus` image `docker-compose.yaml` already pins, so the engine that validates a
   rule is the engine that loads it. The `alerts` job in `.github/workflows/go.yml` runs the
   same target, so a rule cannot be merged untested.

   **`check rules` proves a rule PARSES; it does not prove a rule can FIRE.** Those are
   independent, and the gap is not hypothetical: `SubscriberSettlementNotProgressing` passed
   `check rules` throughout its life while being unable to fire on a settler that had never
   settled anything, because `and rate(counter[30m]) == 0` against a counter with no series
   yields an empty vector. `alerts/tests/blnk-kafka-alerts_test.yml` closes that by evaluating
   each rule **as loaded** — including its `for` duration — against synthetic series. Every one
   of the fifteen rules has a case that drives it past its own threshold and asserts the alert
   fires with its full rendered labels and annotations, and a case below the threshold or inside
   the dwell asserting silence. `TestAlertRules_AreGatedByCIRatherThanByARunbookStep` fails if a
   rule is added without both.

   To validate what a **running** Prometheus loaded, rather than what the repository holds:

   ```bash
   docker compose exec prometheus promtool check rules /etc/prometheus/alerts/blnk-kafka-alerts.yml
   docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
   docker compose exec -w /etc/prometheus/alerts/tests prometheus \
     promtool test rules blnk-kafka-alerts_test.yml
   ```

   The unit-test file lives in `alerts/tests/` rather than beside the rules **because the
   `rule_files` glob would otherwise match it**, and a promtool test file is not a rule file:
   Prometheus would refuse the whole configuration and load none of the fifteen rules. The glob
   does not recurse, so the subdirectory is inside the same mount and out of its reach.

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

   With either missing, Prometheus logs `Cannot create service discovery` once at startup — or is refused by the API server with a 403 — and then serves normally with **zero** targets. The pod is `1/1 Running`, `/-/ready` is green, `/rules` lists all fifteen rules, and nothing is collected, so every rule evaluates against nothing and none can ever fire. **That is why both `/rules` and `/api/v1/targets` have to be checked on Kubernetes: a loaded rule over an absent series and a healthy system are the same observation.** Confirm the credential is present and sufficient with:

   ```bash
   kubectl -n blnk exec deploy/prometheus -- ls /var/run/secrets/kubernetes.io/serviceaccount/token
   kubectl -n blnk auth can-i list pods --as=system:serviceaccount:blnk:prometheus
   ```

> **If `metrics_bearer_token` is set, the scrape fails and every rule sits permanently unable to fire.** Prometheus shows the target down; the rules report nothing wrong. `prometheus.yml` carries the two commented `authorization:` blocks and the mount instructions that fix it — use `credentials_file`, never inline `credentials`, so the token stays out of the committed file and can be rotated without a restart.
>
> **The configuration exists twice.** Kubernetes has no bind mounts, so both `prometheus.yml` and the rule file travel as data in `infrastructure/k8s-manifests/prometheus-configmap.yaml`. **Edit both copies together** — two hand-maintained copies of an alerting configuration diverge silently, each staying valid and reading correctly in review, with one environment alerting and the other not. Tests compare the two, so a lone edit fails rather than shipping.

## Relay Operations

**The event relay runs in the server process role**, started immediately after the fund-lineage outbox processor it is modelled on — this repository's established home for an outbox relay, which also avoids standing up a fourth asynq server for one poll loop. There is no separate relay binary and no relay subcommand: `blnk start` *is* how the relay is run. `make run_relay` is a CONVENIENCE ALIAS for the server role and nothing more — `make run_server_relay` is the same target under the name that says what starts. It exists so that "where does the relay run" is answerable without reading `cmd/server.go`, and because AAP §0.5.1 Group 7 names that target. It loads `.env` the way `make kafka_provision` does — the file supplies defaults, the caller's environment wins — and then execs `blnk start --config blnk.json --require-kafka`.

**IT IS NOT ISOLATED RELAY EXECUTION.** `blnk start` brings up the HTTP API on its port, the fund-lineage outbox processor, the balance-monitor handoff processor and the metrics collector alongside the event relay, and every one of them claims rows or serves traffic. That matters for two things operators reach for this target to do: a load test taken against it measures the whole server role rather than the relay, and a backlog you are watching drain is being drained by more than the relay. If you need the relay's own numbers, read `blnk.events.publish.duration` and `blnk_outbox_pending` rather than inferring them from process behaviour.

**`--require-kafka` is where the refusal lives, and it lives in the application on purpose.** The flag makes `blnk start` resolve its own configuration and refuse to start when that resolution yields no usable broker, printing every source and its precedence and exiting non-zero without serving. The target itself decides nothing about brokers.

That boundary is not tidiness. The target used to resolve the broker list in shell, and a shell approximation of the loader's precedence cannot agree with it — three ways, each of which made the target announce a relay the application then declined to start:

- It took the **first non-empty** of `KAFKA_BROKERS`, `BLNK_KAFKA_KAFKA_BROKERS` and `BLNK_KAFKA_BROKERS`, in that order, while the loader ranks them the other way at the top. So `BLNK_KAFKA_BROKERS= KAFKA_BROKERS=host:9092 make run_relay` was announced as configured from the bare name while the application resolved **no** brokers.
- It tested the configuration file with `grep '"brokers"'`, which reads `"brokers": []` as configured.
- No first-non-empty scan can express that an explicitly **empty** higher-precedence name *clears* what a lower one supplied — which is exactly how Kafka is turned off for a single run.

The precedence the application actually applies, highest first:

| Source | Notes |
|--------|-------|
| `BLNK_KAFKA_BROKERS` | the conventional prefixed name; applied as an overlay after the rest of the environment, so it wins over every other source |
| `KAFKA_BROKERS` | the deployment contract name |
| `BLNK_KAFKA_KAFKA_BROKERS` | the key envconfig derives for the nested field; still honoured |
| `kafka.brokers` in `blnk.json` | used when no environment name is set |
| a `.env` file in the working directory | not a fourth precedence level — `make run_relay` and `make kafka_provision` *source* it, so its assignments arrive as environment and rank by the names above. `./stack.sh --init` writes `KAFKA_BROKERS` there at mode 0600, with the `KAFKA_SASL_USER`/`KAFKA_SASL_SECRET` producer pair the publisher authenticates with. |

**A name that is set and empty is not absent.** `KAFKA_BROKERS= make run_relay` means no brokers for this run, and `--require-kafka` refuses it rather than falling back to `blnk.json`.

It fails fast deliberately, because the alternative failure is silent — a server that comes up healthy, serves the API, captures events into `blnk.event_outbox` and publishes none of them. That is the one outcome worth a hard stop, and it is a stop only when the flag is given: plain `blnk start` treats an empty broker list as the legitimate no-Kafka steady state described under [Running Without Kafka](#running-without-kafka), so the flag narrows nothing about what the typed loader accepts. `config.Fetch` remains the authority on whether a *configured* value is valid — `--require-kafka` answers only the one question that validation deliberately does not, because "none" is a legitimate answer for a deployment that has not migrated.

**`make run_relay` reads `.env`, so the ordinary workflow needs nothing exported** — once `KAFKA_BROKERS` is in that file. `./stack.sh --init` writes the **credentials** into a mode-0600 `.env` (the admin, producer and sample-subscriber pairs) and **not the broker list**, so set `KAFKA_BROKERS` there yourself; the target sources that file — then replays the caller's own environment on top, so **anything you pass on the command line wins** and `.env` supplies only what you did not. Reading the file is what makes the refusal above honest: those assignments are not exported into your shell, so a target that consulted only the environment would refuse the operator who had just followed its own setup instruction.

```bash
make run_relay                              # broker list from .env
KAFKA_BROKERS=localhost:9092 make run_relay  # this wins over .env, for one run
                                             # localhost:9092 is the HOST listener; 29092 is
                                             # the container-side port it is published from
KAFKA_BROKERS= make run_relay                # deliberately empty: refused, not defaulted
```

Nothing else reads that file for you. Neither `make` nor the `blnk` binary loads `.env` on its own — configuration reaches the process through `envconfig`, which reads the environment and no file — so `./blnk start` invoked directly needs the variables exported yourself:

```bash
set -a; . ./.env; set +a
./blnk start --require-kafka   # omit the flag for the no-Kafka steady state
```

Skip that and the server comes up looking entirely healthy while publishing nothing, because the relay start is conditional on brokers being configured.

**The start is conditional on brokers being configured.** With an empty broker list the relay logs one info line and starts nothing — see [Running Without Kafka](#running-without-kafka).

| Parameter | Value | Why it matters operationally |
|-----------|-------|----------------------------|
| Batch size | 100 rows per claim | The unit of work. A backlog drains in multiples of this per tick. |
| Poll interval | 250 milliseconds | The idle latency floor, and the reason it is shorter than the lineage relay's second. A row captured just after a tick waits until the next one before any work begins, so the interval contributes half of itself to the median event and nearly all of itself to the tail — which is why the end-to-end latency series is read from `blnk_events_capture_to_dispatch_duration_seconds` and not from the broker-write series. Measured at 550 events a second over 165,000 events, a one-second interval produced a capture-to-dispatch p50 of 0.548s against a claim-to-acknowledgement p50 of 0.014s, and a p99.9 of 2.247s — past the 2-second target with nothing behind and an empty outbox at the end. Raise it if you would rather have the idle quiet than the margin; `WithPollInterval` takes any positive value. |
| Lock duration | 30 seconds | The lease a claim takes on its rows. **This is the recovery mechanism.** |

Rows are claimed with a CTE using `FOR UPDATE SKIP LOCKED`, ordered by occurrence, which is what makes concurrent relay instances safe without losing FIFO order. The claim additionally admits **at most one claimant per message key**, across all instances rather than merely within one: a claimant enters a key by locking that key's oldest unfinished row, so while it holds that row no other claimant can take any row of the key. Within one claim a key may contribute **several rows** — its oldest, contiguous, and only as far as they are all claimable — and one relay goroutine publishes them in order, stopping at the first that does not settle. That is what makes per-aggregate ordering hold when the relay is scaled out, and it is also why a backlog concentrated on few keys still drains at a useful rate rather than one row per key per claim.

The key the claim serialises on is the **effective** key — the row's ledger where it has one, its stored `partition_key` where it does not — which is the same rule the publisher applies when it chooses a partition. Both are rendered from one expression, so the two cannot drift apart: the claim's predicate and the `idx_event_outbox_effective_key_inflight` expression index behind it are built from the same source as the Go resolver. Serialising on the `partition_key` column alone was unsound, because a row whose partition key was derived before its ledger was known carries a different stored key from its ledger-keyed siblings while Kafka still places all of them on one partition.

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
kubectl -n blnk patch configmap blnk-config --type merge -p '{"data":{"BLNK_LOG_LEVEL":"debug"}}'
kubectl -n blnk rollout restart deployment/server
kubectl -n blnk logs deployment/server | grep cause_verbatim

# Restore it. Debug also enables a line per published event, so do not leave it on.
kubectl -n blnk patch configmap blnk-config --type merge -p '{"data":{"BLNK_LOG_LEVEL":""}}'
kubectl -n blnk rollout restart deployment/server
```

Patch the ConfigMap rather than using `kubectl set env`: both Deployments project `BLNK_LOG_LEVEL` from `blnk-config` with a `configMapKeyRef`, and `set env` would *replace* that projection with a literal value on the Deployment. Undoing it with `set env BLNK_LOG_LEVEL-` would then delete the variable outright, leaving the Deployment permanently diverged from the manifests. The Compose equivalent is under [Two ways an event becomes terminal](#two-ways-an-event-becomes-terminal) above.

The same rule governs the request log. It records the **route template** (`/transactions/:transaction_id`), never the requested path, so no ledger, transaction or identity identifier reaches it — group by `route` when you are counting endpoint traffic. The `client_ip` field is the peer that opened the connection; it believes `X-Forwarded-For` only from an address named in `BLNK_SERVER_TRUSTED_PROXIES`, which is empty by default. If your deployment sits behind an ingress and every request appears to come from one address, that is the setting to populate — with the ingress's own range, never `0.0.0.0/0`.

#### `failure_class` and `failure_detail_class`: two classes on one line describe two different errors

The redacted `error_reason` is not the only thing a dead-letter record gives you. Every
dead-letter and replay record also carries a **closed classification**, and unlike the
verbatim text it is safe to group on, alert on and branch on in a script.

There are two such fields, they use the **same vocabulary**, and they classify **two
different errors**:

| Field | Classifies | The question it answers |
|-------|-----------|-------------------------|
| `failure_class` | `failure_metadata.error_reason` — the **original publish** failure | Why is this event being dead-lettered at all? |
| `failure_detail_class` | the error of **the step being logged** — the `.dlt` write, the row update that records it, or a replay's re-publish or bookkeeping | Why did the attempt to *preserve* or *replay* it fail? |

So `failure_class=authorization_denied failure_detail_class=broker_unavailable` on one line
is not a contradiction and not a bug: the event was refused a topic its principal may not
write, and the attempt to preserve it then hit a cluster that had gone away. **A record
carrying only `failure_class` is the normal case** — the dead-letter write succeeded, and
there is no second error to report.

The vocabulary is closed at eight values. Anything else means the classifier changed:

| Class | Means | Where to look |
|-------|-------|---------------|
| `broker_unavailable` | The broker did not answer, dropped the connection, had no leader, or the attempt timed out | The cluster. This class folds deadline failures in; the API reports those as `timeout`. |
| `authorization_denied` | A denial the broker was **willing to state** — SASL, or `TOPIC`/`GROUP`/`CLUSTER_AUTHORIZATION_FAILED` | The principal's credential and grants, not the cluster's health |
| `topic_missing_or_unauthorized` | The topic was not there **as far as this principal is concerned** | Provisioning first, then the `Describe` grant — see below |
| `message_too_large` | The record exceeds what Blnk or the broker accepts | The producer, and the topic/broker size limits. A replay re-fails. |
| `serialization` | Something **Blnk produced** could not be encoded, or Kafka rejected the topic **name** as illegal | A defect: the payload, or the topic-name construction. A replay re-fails unchanged. |
| `transport_closed` | The publish was attempted through a closed writer | Nothing, if it coincides with a restart — this is a shutdown, not a fault |
| `unrecorded` | No reason was recorded at all | The row: this is a bookkeeping gap in Blnk, not an answer from the broker |
| `other` | The text matched no signature | The verbatim text. A **rising** count here is the signal that a class is missing. |

**Why one class names two things.** A principal holding no `Describe` on a topic is never
told it was denied: Kafka answers `UNKNOWN_TOPIC_OR_PARTITION`, which is word for word what
a genuinely absent topic produces — `[3] Unknown Topic Or Partition: the request is for a
topic or partition that does not exist on this broker`. The broker does not distinguish the
two for us, so neither does the class. Check both, in this order, and use the **admin**
credential for the first so no ACL can mask the answer:

```bash
# 1. Does the topic exist at all? Admin credential, so absence here is real absence.
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --command-config /tmp/admin.properties \
  --list | grep '^blnk\.'

# 2. It exists, so the PRODUCER cannot see it. Read the grant back.
kafka-acls.sh --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_CLIENT_CONFIG" \
  --list --principal "User:$KAFKA_SASL_USER"
```

If step 1 shows the topic missing, run `make kafka_provision`. If step 1 shows it present
and step 2 shows no `Describe`/`Write` binding for the producer on it, the class was the ACL
half and provisioning would have changed nothing.

**The API's `failure_reason` is a third, independent classification of the same stored
text**, and it is deliberately narrower — it splits `timeout` and `persistence_failure` out
and has no serialisation value. Move between them with this table rather than by guesswork:

| `failure_class` (logs) | `failure_reason` (API) | Note |
|------------------------|------------------------|------|
| `broker_unavailable` | `broker_unavailable`, or `timeout` when the text names a deadline | The logs fold deadlines into the cluster class; the API separates them |
| `authorization_denied` | `authorization_denied` | Same condition, same name |
| `topic_missing_or_unauthorized` | `topic_missing` | Same condition. The log name states the ACL possibility the shorter API name leaves implicit — treat `topic_missing` on the API as meaning both. |
| `message_too_large` | `message_too_large` | Same condition, same name |
| `serialization` | `unclassified` | The API has no serialisation value. Read the verbatim `last_error`. |
| `transport_closed` | `unclassified` | As above |
| `unrecorded` | *(absent)* | `failure_reason` is omitted entirely when there is no text to classify |
| `other` | `unclassified` | Both are saying "no signature matched" |
| *(no equivalent)* | `persistence_failure` | **API-only.** Blnk's own database write around the publish failed. In the logs this appears as the `failure_detail_class` on the record whose message says the row could not be recorded — and its class will be whatever the driver's wording matches, commonly `other`. |

Both mappings are asserted in `TestClassifyDeadLetterFailure_ATopicTheProducerCannotSeeIsNeitherADefectNorACrashedCluster`,
so a change to either classifier that broke this table would fail the suite rather than
quietly leave the runbook wrong.

> `blnk.event_outbox` and `blnk.lineage_outbox` are **separate tables served by separate relays**, and they are never merged. `NewLineageOutboxProcessor` handles fund lineage; the event relay handles events. The two share a shape — the same batch size and lease, the same claim idiom — because the event relay was modelled on the lineage one, and that resemblance is exactly what makes them easy to confuse. Their poll intervals differ on purpose: the event relay polls every 250ms because it carries a stated end-to-end latency target and the interval is that target's floor, while the lineage relay keeps the house second. Check which table you are looking at before drawing a conclusion.

Retention is a separate, optional sweep, and **exactly one status is ever eligible for it: `dispatched`.** A dispatched row is a receipt — the event reached the broker and a subscriber has had it — so age alone governs it. Every other status is excluded, and for two different reasons. A `pending`, `processing` or `replaying` row is still owed a delivery attempt. A `dead_lettered` row is the record of an event **nobody received**, so age must never remove it: it leaves the table by being **replayed** through `POST /events/dead-letter/:event_id/replay`, and only once the broker acknowledges that re-publish — making the row `dispatched` — does this period begin to apply to it. A `failed` row is worse still, because its `.dlt` write has not landed, so this table is the only copy of that event in existence. That is what makes any finite retention period safe to set: the purge cannot destroy the evidence of a loss nobody has dealt with.

**The default depends on how you deploy.** `RELAY_EVENT_RETENTION_DAYS` defaults to `0` — **disabled** — in the Go configuration and in both Compose files, deliberately, because the period your jurisdiction, audit programme or legal hold requires is not something a default can guess. The shipped Kubernetes manifests take the other decision explicitly: `infrastructure/k8s-manifests/blnk-config.yaml` sets it to `"90"`, so a deployment applied from those manifests **is** purging dispatched rows older than ninety days unless you change it. Check the value your deployment actually has before concluding that nothing is being deleted, or that something is. Bear in mind what an undeleted row holds: the payload is the webhook body verbatim, so a transaction event carries amounts and balance identifiers and an identity event carries names, email addresses, phone numbers, postal addresses and dates of birth. Confirm the sweep is running with `blnk_events_purged_total`, but read it **against eligibility**: a flat
counter most often means nothing is past the retention cutoff yet, and only otherwise means the sweep is
disabled or stuck. Because the sweep deletes in batches, a step-shaped series is its normal signature.

**Do not diagnose accumulation with `blnk_outbox_pending`** — that gauge holds only `pending` and
`processing` rows, so the retained terminal rows this section is about are invisible to it and it stays
flat while the table grows. Measure the retained rows directly:

```bash
$BLNK_PSQL -f - <<'SQL'
SELECT status, count(*) AS rows,
       pg_size_pretty(pg_total_relation_size('blnk.event_outbox')) AS table_size
FROM blnk.event_outbox
GROUP BY status
ORDER BY rows DESC;
SQL
```

`GET /events/stats` reports the same per-status counts if you would rather not touch the database. Every status except `dispatched` comes back on the default reading; add `include_offsets=best_effort` if you also want the dispatched figure, which is the one count taken on request rather than always.

### Storage — Size The Volume From The Row, Not From A Round Number

Retention is a multiplier on a measured quantity, and the quantity is larger than it looks: **between 85% and 89% of every outbox row is the event body, held three times over.** `payload` is a queryable JSONB projection, `payload_raw` is the byte-exact webhook body the payload-preservation guarantee lives in, and `event_raw` is the byte-exact envelope the replay guarantee lives in. Each has a reason, stated at the column in `sql/1781248800.sql`.

**Measure it against your own payload.** Every figure below is one basis — bytes *as stored* after TOAST compression (`pg_column_size`), plus on-disk relation sizes (`pg_relation_size`, `pg_indexes_size`, `pg_total_relation_size`) — and logical (uncompressed) lengths are labelled where they appear. `sql/1781248800.sql` carries the exact query; clone the table with `CREATE TABLE ... (LIKE blnk.event_outbox INCLUDING ALL)`, load a representative sample, `ANALYZE`, and read it off.

Two shapes measured that way on PostgreSQL 16, 200,000 rows each — a minimally populated `transaction.applied` and a fully populated one, differing only in the webhook body, because the body is the caller's payload:

| Component | Minimal body | Populated body |
|---|---|---|
| webhook body (logical) | 492 | 899 |
| envelope (logical) | 706 | 1,113 |
| `payload` JSONB (stored) | 561 | 985 |
| `payload_raw` (stored) | 496 | 678 |
| `event_raw` (stored) | 517 | 773 |
| whole tuple (stored) | 1,861 | 2,726 |
| heap, on disk incl. page overhead | 2,048 | 2,048 |
| TOAST, on disk | 1 | 1,194 |
| all seventeen indexes, on disk | 379 | 377 |
| **total per row, on disk** | **2,427** | **3,619** |

On the populated shape the three body columns exceed the 2 KiB threshold together, so part of them is stored out of line — which is why TOAST is a real row there and all but absent on the minimal shape.

```text
GiB/day      = rate x 86400 x measured_bytes_per_row / 1024^3
steady state = GiB/day x RELAY_EVENT_RETENTION_DAYS x 1.5
```

The 1.5 is bloat and autovacuum headroom, and it is not padding: every row is `UPDATE`d at least twice on its way to a terminal state, `status` is indexed so neither update can be HOT, and each one leaves a dead tuple and rewrites index entries. WAL sits on top and is bounded separately by `max_wal_size`.

At 500 events a second that is **98–146 GiB a day** across those two shapes, so:

| Retention | Outbox steady state |
|---|---|
| 1 day | 146–218 GiB |
| **3 days (what `blnk-config.yaml` ships)** | **439–655 GiB** |
| 7 days | 1,025–1,529 GiB |

**"Shipped" here means the Kubernetes manifest, not the code default.** `infrastructure/k8s-manifests/blnk-config.yaml` sets `RELAY_EVENT_RETENTION_DAYS: "3"`, which is what the table above is sized against; the code and `.env.example` default is `0` — retention disabled — for the reason given under [the reconciliation's expected differences](#the-daily-outbox-versus-offset-reconciliation). A Compose or bare-metal deployment that never sets the variable therefore has **no** steady state in this table at all: terminal rows are never purged, so the outbox grows for as long as the deployment publishes. Read the table as the cost of the retention you choose, and the bold row as the cost of the one the manifests choose for you.

**Three settings are coupled and must move together.** `RELAY_EVENT_RETENTION_DAYS` is capped at the broker's `log.retention.hours` (168, so 7 days) because the reconciliation below can only compare a window both sides still hold; and the `pg-data` volume must hold the steady state above **plus the ledger's own tables plus WAL**.

**Size the `pg-data` volume yourself before production — the reference manifest does not size it for you.** `infrastructure/k8s-manifests/postgres-statefulset.yaml` ships the upstream development request of `10Gi`, which is deliberately left as it was: it is a starting point for a local cluster and it is *not* a capacity plan for any event rate. Take the populated figure from the table above — the one that has to fit — leave roughly a fifth of the volume for the ledger's own tables, WAL up to `max_wal_size` and the filesystem reserve, and set `spec.volumeClaimTemplates[0].spec.resources.requests.storage` to the result *before the first apply*. At the validated 500 events a second with the 3-day retention `blnk-config.yaml` ships that is 655 GiB of outbox, so a volume of roughly 900Gi; at 20 events a second it is 26 GiB and a volume two orders of magnitude smaller is right. Confirm the row cost against your own data either way.

Two properties of that edit decide when you can make it:

- **`volumeClaimTemplates` is immutable on an existing StatefulSet.** `kubectl apply` on a running `postgres` StatefulSet with a changed storage request is rejected by the API server. On an existing cluster the size is changed by `kubectl delete statefulset postgres --cascade=orphan` (which leaves the pod and the `pg-data-postgres-0` claim running), then applying the edited manifest, then deleting the pod so it is recreated against the new template — the same procedure documented for the Kafka StatefulSet below.
- **Growing the claim later needs a `StorageClass` with `allowVolumeExpansion: true`.** Arrange that before you need it: expanding a claim whose class forbids it means a restore, and the moment you need the space is the moment the ledger has stopped accepting writes.

There is also a standalone `pg-data` PersistentVolumeClaim in `infrastructure/k8s-manifests/pg-data-persistentvolumeclaim.yaml` requesting `100Mi`. It predates this work, it is referenced by no `claimName` in the manifest set, and it is not what postgres runs on — the StatefulSet's template generates `pg-data-postgres-0`. Ignore it, or delete it in a change of its own; do not size the outbox against it.

Two consequences worth stating plainly:

- **The outbox shares its volume with the ledger.** An over-generous retention period does not buy more history, it exhausts the volume the ledger writes to — availability loss of the whole system, with a retention setting as the cause.
- **The ledger's own growth is not bounded by any of this.** Transactions and balances are never purged. The band above is the part this feature is responsible for and the part that has a ceiling; project the rest from your own transaction rate before going to production.

Running well under the peak the load test drives? The demand scales linearly — at 20 events a second a populated event is 5.8 GiB a day, so three days is 26 GiB and a volume sized for the validated peak is two orders of magnitude larger than you need. Size down with the same arithmetic.

### Throughput — It Is Bound By Key Diversity, Not By Relay Tuning

**The 500 events a second this pipeline is validated at is a figure across many ledgers, and it does not transfer to a workload concentrated on one.** Plan capacity against that before you plan it against `RELAY_*`.

The relay's claim admits **at most one claimant per message key**, across every concurrent relay instance rather than merely within one. That is what delivers the per-aggregate ordering guarantee: no row is ever published ahead of an older row of its own key, so the sequence reaching a partition is the sequence the mutations happened in. The cost is that one key's events are published **strictly serially** — one publish round trip at a time — however many rows of that key a single claim took.

So throughput has a ceiling of roughly:

```
min(relay publish concurrency, distinct keys with work pending)
     ×  publish round trips per second on one key
```

and the key is the **ledger id** wherever the event's subject belongs to a ledger. A workload spread across many ledgers reaches the validated rate comfortably: measured against a local broker, 200,000 rows across 200,000 keys drained at **2,185 events a second**. A workload concentrated on **one** ledger cannot exceed one round trip at a time on it, which measured **484 events a second** against the same broker and would be materially fewer across a network — no matter how the relay is configured. Both figures were taken on a four-core host and are the shape of the curve rather than a promise about yours.

**Nothing raises the per-key limit**, and the batch size does not either. The three `RELAY_*` environment variables govern the retry schedule and nothing else; the batch size, poll interval and lease are code defaults a host can override through `WithBatchSize`, `WithPollInterval` and `WithLockDuration`, and they decide how many rows a claim returns and how often it runs. A claim will hand one key a large share of a batch when few keys have work, but those rows are still published one after another. Adding relay instances does not raise it either, because the one-claimant-per-key rule is enforced across instances by design — that is the point of it. Adding instances *does* raise the first term of the ceiling above, so it helps a many-key backlog and not a one-key one.

**If a single logical stream needs more throughput, the lever is key diversity, not tuning.** Spread the work across ledgers. There is deliberately no configuration that trades the serialisation away: it *is* the ordering guarantee, and a switch to disable it would be a switch to disable requirement R-6.

**What it looks like when you hit it.** `blnk_outbox_pending` climbs while `blnk_events_publish_duration` stays healthy and no dead-letter appears — rows are waiting for a key rather than failing on one. Group the backlog by key to confirm:

```bash
psql "$BLNK_DATA_SOURCE_DNS" -c \
  "SELECT COALESCE(NULLIF(btrim(ledger_id), ''), btrim(partition_key)) AS key, count(*)
     FROM blnk.event_outbox WHERE status IN ('pending','processing')
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10;"
```

A backlog concentrated in a handful of keys is this limit. A backlog spread evenly across many keys is a genuine relay or broker throughput question, and `RELAY_*` is then the right place to look.

### Purge Capacity — Check It Against Your Arrival Rate

The retention *period* says how long a terminal row is kept. Two further settings say how fast that period is actually **enforced**, and a period configured without regard to them is not enforced at all. The sweep runs hourly, so:

```text
rows deleted per hour = RELAY_EVENT_RETENTION_MAX_BATCHES_PER_SWEEP
                      x RELAY_EVENT_RETENTION_BATCH_SIZE
```

**Capacity below the arrival rate does not slow the table's growth, it permits it.** The sweeper never catches up, the oldest eligible rows are never reached, and `blnk.event_outbox` grows without bound however short the retention period is set. At the throughput this system is validated against — 500 events a second — rows arrive at **1,800,000 an hour**. The shipped defaults (4,000 batches of 1,000) give **4,000,000 an hour**, or 2.2x arrival. Multiply your own peak rate out and compare before changing either value.

The batch ceiling used to be 2,000, giving 1.11x arrival, and 11% is not headroom on a mechanism that cannot catch up once it falls behind: a single sweep cut short by the ten-minute timeout leaves a deficit that takes ten hours of steady state to work off, and the deficit is rows the retention period says should already be gone. It costs nothing to carry the larger ceiling, because **a sweep stops as soon as it runs out of eligible rows** — at 500 events a second it still deletes 1,800,000 and stops. The ceiling is only reached while catching up, which is exactly when you want it.

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
               kafka-init: service_completed_successfully
               (both required: false, so a stack with no Kafka profile still starts)

worker         depends_on redis, postgres, jaeger — and NOTHING Kafka.
               It captures event rows and publishes none, so it holds no broker
               credential and has nothing to wait for.
```

The gates matter, and they are on the **server** because that is the role that dials Kafka. Provisioning against a broker that is not yet answering authenticated requests fails in a way that reads like a wrong password, and a relay that starts before the topics exist dead-letters its first events for `UNKNOWN_TOPIC_OR_PARTITION`. The worker needs neither gate: an event it captures is a row committed to PostgreSQL inside the ledger's own transaction, which the server's relay publishes later, so a worker that starts before the broker exists loses nothing.

From a fresh checkout, in this order. Steps 2 and 3 are the ones `--init` does not do for you, and skipping either produces a stack that comes up healthy and publishes nothing:

```bash
# 1. Create or complete a mode-0600 .env, generating the admin, producer and
#    sample-subscriber principals and their secrets.
./stack.sh --init

# 2. Choose how the application images are obtained. Either name a published image:
#      BLNK_IMAGE=blnkfinance/blnk:<tag>
#    or build from source by switching the compose projection:
#      COMPOSE_FILE=docker-compose.dev.yaml
#    .env ships BLNK_IMAGE empty and COMPOSE_FILE=docker-compose.yaml, and the
#    server/worker services carry a deliberately unresolvable image default, so a
#    bring-up that skips this fails on the image before anything else is tried.

# 3. Turn Kafka on. THIS IS WHAT ENABLES THE PROFILE — stack.sh adds the "kafka"
#    profile only when a broker list is configured, and --init does not write one:
#      KAFKA_BROKERS=kafka:9092
#    Blnk dials that from inside the Compose network. Host-run tests and the k6
#    scenario use localhost:9092 instead.

# 4. REQUIRED, BECAUSE STEP 3 SET A BROKER LIST. --init does not write this either,
#    and there is no default: with brokers configured and this empty the server
#    refuses to start, because a Kafka deployment with no usable dual-delivery
#    window is treated as ALREADY past the sunset.
#
#    Set an RFC3339 instant NO MORE THAN 30 DAYS AHEAD. The window opens at
#    sunset minus 30 days and has no variable of its own, so a date further out
#    leaves it un-opened and the relay refuses to start rather than publish to
#    Kafka while delivering no legacy webhooks. Generate one that is always valid:
#      WEBHOOK_DEPRECATION_SUNSET_DATE=$(date -u -d '+30 days' +%Y-%m-%dT%H:%M:%SZ)
#
#    Use the literal "retired" instead if the legacy transport is already gone —
#    that is the closed-window value, and it is the only non-date this accepts.

# 5. Acknowledge the local transport. The single-broker stack listens on
#    SASL_PLAINTEXT, so the SCRAM exchange and every ledger event travel in clear
#    text — acceptable on a loopback-bound development stack and nowhere else:
#      KAFKA_INSECURE_LOCAL_DEV=true

# 6. Provision Kafka, then bring the stack up.
./stack.sh --up
```

> **Which projection satisfies "bring the stack up from a fresh checkout".** `docker-compose.dev.yaml` does, and it is the answer for a checkout with no published image: its `server` and `worker` services carry `build: .`, so `COMPOSE_FILE=docker-compose.dev.yaml ./stack.sh --up` compiles this commit and runs it against the local broker with no image reference to resolve. `docker-compose.yaml` is the deployment projection and deliberately has **no** usable image default — `${BLNK_IMAGE:-set-blnk-image-explicitly-see-env-example}` — so that a bring-up which never chose an image fails on the image rather than silently running a build from before this pipeline existed. Both projections declare the same Kafka service, the same `kafka-init` one-shot and the same `KAFKA_*` environment, so local parity is the same either way.

`stack.sh --init` writes exactly seven assignments into `.env`, and the run names each one as it goes: `KAFKA_SASL_ADMIN_USER=admin`, `KAFKA_SASL_ADMIN_SECRET`, `KAFKA_PRODUCER_SECRET`, `KAFKA_SASL_USER` and `KAFKA_SASL_SECRET` — the last two carrying the producer identity under the names the application reads it by — plus `KAFKA_SAMPLE_SUBSCRIBER_SECRET`, and `POSTGRES_PASSWORD` silently. All six Kafka keys ship as **empty** assignments in `.env.example` and are rewritten in place; `POSTGRES_PASSWORD` is the one key that ships as a `{POSTGRES_PASSWORD}` token and is substituted, and it is the only such token in the file. It also *offers* `KAFKA_PRODUCER_USER`, but `.env.example` already ships that one populated with `blnk-producer` — as it does `KAFKA_SAMPLE_SUBSCRIBER_USER` — so the run reports `KAFKA_PRODUCER_USER is already set in .env; left untouched` and changes nothing. **A principal name it finds set is never overwritten**, which is what lets you choose your own before the first `--init`. **It writes credentials and nothing else** — `KAFKA_BROKERS` and `WEBHOOK_DEPRECATION_SUNSET_DATE` are still empty afterwards — which is why steps 2 to 5 above are yours. It enables the `kafka` profile **only when `KAFKA_BROKERS` is set**, and always includes the profile on teardown so nothing is left behind. **A bring-up fails when this project's own broker cannot be verified** — it never became healthy, or its catalogue could not be provisioned; nothing is torn down, and the exit status is what says the stack cannot publish yet. An external broker or an empty `KAFKA_BROKERS` is reported and never fatal.

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

> **Where the containers get their PostgreSQL and Redis DSNs, and why it is not `blnk.json`.** Those two values are the only configuration Blnk *requires*, in every role including `blnk migrate up`, and both Compose projections now pass them to `server`, `worker` and (in the development projection) the one-shot `migration` service. When `BLNK_DATA_SOURCE_DNS` and `BLNK_REDIS_DNS` are empty — which is how `.env.example` ships them and how `stack.sh --init` leaves them — each service is handed a value derived from the very `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` and `REDIS_PASSWORD` keys that create those services, addressed by Compose service name. That is what makes step 6 above work on a clone with nothing else set, and it is why neither projection mounts `./blnk.json` any more: a bind-mount source that does not exist is *created* by Docker, as a directory, and all three services then exited on `read blnk.json: is a directory` — a path the operator never made, on the first bring-up after a clone. Two consequences to keep in mind. **A value you set wins, including a wrong one**: these keys are also read by host processes (`make run`, `make test`), where the endpoint is `localhost` and the published port, and a host-shaped value left in `.env` is forwarded verbatim into containers where `localhost` is the container itself. Set the in-Compose form (`@postgres:5432`, `redis://redis:6379`) or leave them empty. **And a password has to be expressible in a URL**: the derived value interpolates `POSTGRES_PASSWORD` into the DSN's userinfo, so `--init` generates it from an alphanumeric alphabet; a hand-written password containing `/`, `@`, `?` or `#` needs `BLNK_DATA_SOURCE_DNS` written out explicitly with those characters percent-encoded.

Local-stack particulars worth knowing before you compare it with production:

- **Replication factor 1.** A single broker cannot do 3.
- **`SASL_PLAINTEXT` on both client listeners.** SCRAM authenticates but nothing is encrypted, which is why the host port is bound to `127.0.0.1` and why `KAFKA_INSECURE_LOCAL_DEV` has to be set for Blnk's clients to dial it at all.
- **The controller listener uses SASL/PLAIN**, not SCRAM. It has to: SCRAM credentials live in the metadata log the controller must read *before* they exist.
- **`auto.create.topics.enable=false`.** A topic created on first use would get the wrong partition count and no ACL, so a mistyped topic name fails loudly instead.
- **`super.users=User:<admin>`.** The administrative principal bypasses ACLs by design, which is why it must never be used to test isolation.
- **The `kafka_data` volume holds `__cluster_metadata`.** Removing it discards the cluster's entire authorization state — every SCRAM credential, the sample subscriber's password and every ACL — and forces a re-bootstrap. `./stack.sh --purge` deletes it.

## Kubernetes Deployment

`infrastructure/k8s-manifests/` deploys the broker as a three-replica StatefulSet alongside the server and worker Deployments. Six things about it will bite you if you meet them for the first time during a deployment, so they are written down here rather than only in the manifests.

### Resolve the application image before you apply, not during

The committed manifests deliberately do **not** name a deployable application image:

```yaml
image: blnk:REPLACE_WITH_PINNED_DIGEST
```

That is not an oversight, and it must not be "fixed" by writing a tag there. It replaced a real published `jerryenebeli/blnk:0.13.3` — a build that predates this event-streaming pipeline — and while that reference was in place `kubectl apply` **succeeded**. Every probe passed. The cluster ran a binary with no relay and no `/events` endpoints, next to a ConfigMap full of `KAFKA_*` keys it had never heard of, and the only symptom was that no events ever arrived. A deployment that fails is recoverable in minutes; a deployment that succeeds against the wrong binary is discovered days later.

A placeholder alone, though, only makes the failure *certain* — not *early*. Kubernetes does not validate image references at admission: it accepts the string, admits the object, schedules the pod, and the kubelet discovers the problem when it tries to pull. So the feedback is an `ImagePullBackOff` several seconds after an apply that reported success, on a Deployment that is by then **partially rolled out**, old ReplicaSet scaling down and new one unable to start.

`scripts/k8s-preflight.sh` closes that gap. Its image checks contact no cluster and read no kubeconfig, so it is safe in CI:

```bash
# 1. Build and push this commit, then resolve its DIGEST.
docker build -t "${REGISTRY}/blnk:${GIT_SHA}" .
docker push "${REGISTRY}/blnk:${GIT_SHA}"
BLNK_IMAGE="$(docker inspect --format '{{index .RepoDigests 0}}' "${REGISTRY}/blnk:${GIT_SHA}")"

# 2. Render a COPY of the manifests with the image resolved, and check it.
make k8s_render BLNK_IMAGE="${BLNK_IMAGE}"      # -> ./rendered, gitignored

# 3. Apply the RENDERED tree, never the source tree.
kubectl apply -f ./rendered
```

`make k8s_preflight` on the committed tree is **expected to fail** — the placeholder is intentional in git. Rendering is a copy rather than an in-place edit for the same reason: an in-place substitution would leave a digest in the working tree, and the next `git status` would invite someone to commit it.

The gate also enforces that the `migrate` init container and the `server` container carry **one** image. Two builds there would migrate to one schema and serve against another, which presents as arbitrary runtime errors rather than as a failed deploy.

> **Third-party images.** The gate reports `postgres:16` and `typesense/typesense:29.0` as tag-only rather than digest-pinned, and **warns without failing**. Those references predate this work. Promoting them to errors would either block every run until an unrelated six-manifest change lands, or pressure whoever hits it into pinning images they were not reviewing. The warning keeps the gap visible and attributable; raising it is a deliberate follow-up.

### Create the Secrets before you apply, and let the preflight tell you which

The manifests project every credential from a `Secret`, never from the ConfigMap. A Secret that does not exist — or that exists under a different key name — fails in exactly the way an unresolved image does: the object is admitted, the pod is scheduled, and it then sits in `ContainerCreating` with the reason reachable only from `kubectl describe pod`.

`scripts/k8s-preflight.sh` derives the required inventory from the manifests themselves and checks it:

```bash
# What has to exist. Contacts no cluster; prints SECRET<TAB>KEY, one per line.
./scripts/k8s-preflight.sh --list-secrets

# Verify it against the cluster you are about to apply to. Fails, naming the Secret and
# the key, when one is absent -- and fails rather than warns if it cannot check at all.
./scripts/k8s-preflight.sh --require-secrets --namespace blnk ./rendered
```

The list is derived, not maintained here, so it stays correct as the manifests change. At the time of writing it is **eleven keys across six Secret objects**:

| Secret | Keys | Consumed by |
|---|---|---|
| `blnk-datasource` | `dsn` | server, worker |
| `blnk-redis` | `dns`, `password` | server, worker, redis |
| `blnk-secrets` | `server-secret-key`, `typesense-api-key` | server, worker |
| `blnk-metrics-token` | `metrics-bearer-token` | server, worker |
| `kafka-credentials` | `admin-secret`, `producer-secret` | server, kafka |
| `kafka-tls` | `keystore-password`, `truststore-password`, `key-password` | kafka |

Without a reachable cluster the check reports itself as **not run** and prints the inventory rather than failing, so the same script stays usable in a CI job with no kubeconfig. Pass `--require-secrets` on the run that is actually about to apply, where "could not verify" is not an acceptable answer.

### The brokers' external addresses are yours to supply

The StatefulSet runs with the in-cluster listener only until you give it addresses to advertise. That is a complete configuration — if every subscriber runs inside the cluster, leave it alone.

Subscribers connecting from outside need the external listener, and its addresses are the one value this repository cannot know: `kafka-service.yaml` provisions one `LoadBalancer` per broker on 29092, and the cloud provider assigns each address at apply time. Supply them as `KAFKA_EXTERNAL_ADVERTISED_HOSTS` on the `blnk-config` ConfigMap, where the key ships commented out:

```yaml
# infrastructure/k8s-manifests/blnk-config.yaml
KAFKA_EXTERNAL_ADVERTISED_HOSTS: "a.example.com,b.example.com,c.example.com"
```

Three properties of it are worth knowing before you set it:

- **One comma-separated entry per replica, in ordinal order**, matching the `kafka-external-<ordinal>` Services. A value present but short of one entry per broker makes that broker **refuse to start** rather than advertise an address it does not have — deliberately, because a broker advertising the wrong address is discovered by a subscriber, not by you.
- **It is the brokers' advertised listener, not the list handed to subscribers.** `POST /subscribers/{id}/kafka-credentials` reports `KAFKA_SUBSCRIBER_BROKERS`; set the same external addresses there too, or credentials will name addresses a subscriber cannot reach.
- **The external listener is `SASL_SSL`.** It is the only listener a subscriber uses, and it is TLS-protected on purpose: SCRAM over a plaintext external listener puts the credential on the wire.

### Re-pin the images on a cadence, not on an incident

Every image this work owns is pinned by digest, which is what makes two clusters applying the same manifest weeks apart run the same bytes. It has a cost that is easy to leave unattended: **a digest freezes the base image's content, including its unpatched CVEs.** A tag beside a digest is decoration — the digest wins — so the tag will still read `4.3.1` long after upstream has rebuilt it.

Review the pins on a schedule you actually keep, and at minimum whenever a base-image advisory affects one of them:

```bash
# For each pinned reference: resolve what the TAG points at now and compare.
docker buildx imagetools inspect apache/kafka:4.3.1 --format '{{.Manifest.Digest}}'

# Then update the pin in every place it appears and re-run the parity check, which
# asserts compose and the manifests carry the same digest for the same image.
grep -rn 'apache/kafka:' docker-compose.yaml docker-compose.dev.yaml .env.example \
  infrastructure/k8s-manifests/kafka-statefulset.yaml
go test -run TestKubernetesManifests_CarryTheSamePinsAsCompose ./...
```

The pinned references live in `docker-compose.yaml`, `docker-compose.dev.yaml`, `.env.example` and `infrastructure/k8s-manifests/`; a contract test asserts compose and the cluster manifests agree, so a partial re-pin fails rather than drifts. Record the re-pin in the release notes with the advisory that motivated it — a digest bump with no stated reason is indistinguishable from an accident.

### The Kafka storage claims are three, and applying them is optional

A reader coming from the PostgreSQL manifests will look for `kafka-data-persistentvolumeclaim.yaml`, because `pg-data-persistentvolumeclaim.yaml` sits beside `postgres-statefulset.yaml`. **There is a Kafka equivalent, and it holds three claims rather than one.**

`kafka-statefulset.yaml` declares a `volumeClaimTemplate` named `kafka-data` on a set named `kafka` with `replicas: 3`, and Kubernetes materialises one claim per pod from it under a **derived** name:

```text
<template name>-<StatefulSet name>-<ordinal>
kafka-data      -kafka             -0 / -1 / -2
```

Those three names are exactly what `kafka-data-persistentvolumeclaim.yaml` declares. **Adoption is by name and nothing else:** apply that file *before* the StatefulSet and the set adopts each claim rather than creating a second one; apply it *after*, or not at all, and the StatefulSet creates the same three itself. Both are valid — this file pre-provisions, it does not add anything the workload could not do alone.

**Apply it first when you need to control an individual broker's storage.** Because each claim exists before any pod does, you can bind a specific `PersistentVolume` to a specific ordinal — a named local NVMe disk, a cloud disk restored from a snapshot, or a different `StorageClass` for one broker — none of which a single template can express. Add `volumeName:` or `storageClassName:` to one entry and only that broker changes. It also makes the storage reviewable as an object: `kubectl get pvc -n blnk` before the brokers start tells you whether the cluster can really provision 3 × 200Gi, which is a far better moment to find out than during a cold start. **Skip it** when the cluster's default `StorageClass` is what you want for all three, which is the ordinary case.

**Why it is three claims and not one.** A file at this path once held a *single* claim named `kafka-data`, 10Gi, `ReadWriteOnce`, generated by Kompose — and it was replaced rather than corrected, because a singleton is worse than useless here. A PersistentVolumeClaim is one claim bound to one volume, and `ReadWriteOnce` means one node may mount it at a time, while **each broker needs its own KRaft metadata log and its own partition segments.** Three brokers sharing one claim would either fail to schedule onto separate nodes or — far worse — write concurrently into a single log directory, which corrupts the metadata log rather than reporting an error. Nothing ever mounted that singleton, so applying the folder simply provisioned an idle volume for someone to find later and reason about. Per-ordinal names are what make the difference: a claim the StatefulSet will adopt is a claim that gets used.

**Five properties must stay true, or the claims stop being adopted.** Kubernetes adopts a pre-existing claim *as it finds it* and never reconciles it against the template, so each of these is a silent-divergence hazard rather than a validation error — `kubectl apply` reports success either way.

| # | Property | Value | What breaks if it diverges |
|---|---|---|---|
| 1 | The names | `kafka-data-kafka-0/-1/-2` | Renaming the StatefulSet or its template orphans all three: the set creates its own claims under the new names and these sit unbound — the idle-volume defect with extra steps |
| 2 | The count | One entry per replica (3) | Raising `replicas` without adding an entry leaves the new broker provisioned from the template and its siblings from this file, differing in whatever the two disagree on |
| 3 | The size | `200Gi`, identical to the template | A smaller claim is adopted at the smaller size, silently, and the broker fills a disk the retention arithmetic said had room: 48 partitions × 2 GiB of `log.retention.bytes` = 96 GiB, 48% of 200Gi |
| 4 | The access mode | `ReadWriteOnce`, matching the template | A claim the scheduler treats differently from what the workload expects is a scheduling failure at the worst moment |
| 5 | The storage class | Omitted in both, so both take the cluster default | Setting it on one side only backs the adopted claim with storage the template never asked for. Name the same class in both places, or name it *only* here, deliberately, to give one broker different storage |

`TestManifests_KafkaDataClaimsMatchTheStatefulSetTemplate` derives all five from the StatefulSet rather than restating them, so a geometry change on one side alone fails the test instead of drifting.

**`Pending` is the expected state, not a fault.** On a cluster whose default `StorageClass` uses `WaitForFirstConsumer` — the norm for zonal block storage, and the right choice here — the three claims stay `Pending` until the brokers are scheduled. The volume is then provisioned in whichever zone the pod landed in, which is what keeps a broker able to reattach its own disk after rescheduling. With `Immediate` binding they provision at once and are still adopted.

> **`kubectl delete -f` on this file is destructive, not a cleanup.** The StatefulSet's default `persistentVolumeClaimRetentionPolicy` is `Retain`, so these claims outlive the StatefulSet deliberately — a broker returning with an empty volume has lost its KRaft metadata and its share of every partition. Deleting them on a running cluster deletes the cluster's metadata.

### Changing the broker's storage size is not a rolling update

`spec.volumeClaimTemplates` is **immutable after creation.** Editing the storage request and re-applying does not roll out — the API server rejects the update outright, with a `spec: Forbidden` error naming the small set of `StatefulSet` spec fields that *may* be updated (`replicas`, `template`, `updateStrategy` and a few more; **the exact list has grown across Kubernetes releases**, so match it against your own server version rather than against any list written down here). `volumeClaimTemplates` has never been among them.

Growing the volume therefore takes two independent steps, because the template governs **future** claims while the existing PVCs are separate objects that must be expanded in place. Do them in this order, and confirm the prerequisite first:

```bash
# 0. PREREQUISITE. Expansion is impossible without it, and this is the cheapest
#    moment to discover that. If it prints anything but "true", the volumes cannot
#    grow in place and the only path is a new StorageClass plus a data migration.
kubectl get storageclass "$(kubectl -n blnk get pvc kafka-data-kafka-0 \
  -o jsonpath='{.spec.storageClassName}')" \
  -o jsonpath='{.allowVolumeExpansion}{"\n"}'

# 1. Replace the StatefulSet OBJECT while leaving the pods running. --cascade=orphan
#    is what makes this safe: it deletes the controller, not the workload, so no
#    broker restarts and no PVC is released.
kubectl -n blnk delete statefulset kafka --cascade=orphan

# 2. Re-create it from the manifest carrying the new size. It adopts the running pods
#    by name, so this is not a restart. New ordinals get the new size.
kubectl apply -f ./rendered/kafka-statefulset.yaml

# 3. Expand each EXISTING claim. The template does not retroactively resize these.
for i in 0 1 2; do
  kubectl -n blnk patch pvc "kafka-data-kafka-${i}" --type merge \
    -p '{"spec":{"resources":{"requests":{"storage":"160Gi"}}}}'
done

# 4. Watch for FileSystemResizePending. Most CSI drivers finish the filesystem half
#    only when the volume is remounted, which means one rolling restart, ONE POD AT
#    A TIME, verifying quorum between each.
kubectl -n blnk get pvc -l io.kompose.service=kafka-data \
  -o custom-columns=NAME:.metadata.name,SIZE:.status.capacity.storage,PHASE:.status.phase
```

**Shrinking is not possible.** Kubernetes rejects a smaller request, and the only route is a new StatefulSet and a partition-by-partition migration. Size for growth at creation.

**Do the rolling restart in step 4 one pod at a time.** With `min.insync.replicas=2` across three brokers, taking two down together stops every `acks=all` produce in the cluster — so the relay can neither publish nor dead-letter, and events accumulate in the outbox instead. That is recoverable, but it is an outage while it lasts.

### KafkaBrokerVolumeFilling

**This is the fourteenth rule, and the only one that needs a metric this repository does not produce.** It exists on Kubernetes alone, which is why its procedure lives here rather than under "Alert Response" with the thirteen; its `runbook_url` points at this section.

`prometheus-configmap.yaml` ships two rule files. `blnk-kafka-alerts.yml` is a verbatim mirror of the repository-root `alerts/blnk-kafka-alerts.yml`, and every rule in it reads a `blnk_*` series this codebase publishes, so those load and evaluate identically under Compose and under Kubernetes.

`blnk-infra-alerts.yml` is different, and deliberately separate. Its single rule, `KafkaBrokerVolumeFilling`, reads `kubelet_volume_stats_available_bytes` and `kubelet_volume_stats_capacity_bytes` — **published by the kubelet, and by nothing in this repository.** Scraping them requires a job against the kubelet's `/metrics/resource` endpoint: cluster-scoped discovery plus a bearer token from a ServiceAccount with `nodes/metrics` access. `prometheus-rbac.yaml` grants neither, by design — it is a namespace `Role` with `pods` `get`/`list`/`watch`, which is all the pod discovery for `blnk-server` and `blnk-worker` needs, and widening it to a cluster-scoped `ClusterRole` for one alert would be a poor trade.

So the shipped behaviour is explicit:

- **Most clusters already run `kube-prometheus-stack` or an equivalent that scrapes the kubelet.** Where that is true, this rule evaluates as soon as the file is mounted — which it is, via a `subPath` mount in `prometheus-deployment.yaml`. Nothing further is needed.
- **Where it is not true, the rule loads and never fires.** It is not hidden, because a rule that cannot fire is indistinguishable from a system that is healthy — and this one is the last line of defence for a failure mode the retention policy bounds but cannot guarantee.

To confirm which situation you are in:

```bash
# Does the series exist at all? Empty result => nothing is scraping the kubelet.
kubectl -n blnk exec deploy/prometheus -- \
  wget -qO- 'http://localhost:9090/api/v1/query?query=kubelet_volume_stats_capacity_bytes' \
  | head -c 400

# Is the rule at least LOADED? An unmounted rule file is absent from here with no error.
kubectl -n blnk exec deploy/prometheus -- \
  wget -qO- 'http://localhost:9090/api/v1/rules' | grep -o 'KafkaBrokerVolumeFilling'
```

If the series is missing and you have no external monitoring stack, either point Prometheus at an existing kubelet scrape or delete the `blnk-infra-alerts.yml` mount so the gap is recorded as a deliberate omission rather than as a rule you believe is protecting you. **Do not leave it believed-active and unmonitored** — the failure it guards is a broker that fills its log directory, and a Kafka broker in that state does not shed old data to continue: it marks the directory offline and drops the partitions on it.

## Running Without Kafka

**With `KAFKA_BROKERS` empty, the publisher resolves to a no-op, the relay does not start, and the ledger serves, records and processes transactions exactly as it does with Kafka configured.** This is a legitimate steady state, not an incident. It is reported at info level once, not warned about repeatedly, and it is the state every deployment is in before it opts into Kafka.

Consequences to expect, so that none of them is mistaken for a fault:

- **No events are published**, and nothing accumulates in `blnk.event_outbox` because nothing is captured for a publisher that does not exist.
- **`GET /events/stats` still answers `200`**, reporting the per-status counts it does know, with `offsets_complete: false`, the offset keys omitted rather than emitted as nulls, and no `reconciliation` object. That is the correct output, not a failure. Use `include_offsets=best_effort` rather than `true` on a broker-less deployment: both count the dispatched history, and only `best_effort` still answers `200` when there is no broker to read.
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
- **Lag coverage reports itself as COMPLETE, and no alert fires.** With no subscriber rows registered
  the empty lag inventory covers the registry exactly, so `blnk_kafka_consumer_lag_inventory_complete`
  publishes **1** and all six `blnk_kafka_subscribers_unmeasured{reason=…}` series publish 0 —
  including `broker_unconfigured`. `SubscriberLagCoverageIncomplete` therefore cannot fire on this
  configuration. **If registry rows DO exist on a broker-less deployment**, coverage reads 0 and the
  whole registry is attributed to `broker_unconfigured`: that alert then fires after its 30-minute
  dwell, truthfully, and the remedy is to configure the brokers or delete the rows — not to raise
  `RELAY_SUBSCRIBER_METRICS_BUDGET`, which is what the shortfall used to be reported as.
- **`scripts/kafka-provision.sh` skips and exits 0**, which is what makes it safe on an unconditional bring-up path.
- **The subscriber settlement pass does not run**, and says so once at info level: with no broker there is no broker-side subscriber state that could diverge from the registry. Note the one case where this matters: a deployment that provisioned subscribers against a broker and later started *without* the broker list has obligations that nothing will discharge, and `SubscriberSettlementNotProgressing` is the rule that catches it.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Every publish fails with connection refused or no available brokers; `blnk_outbox_pending` climbs | Broker unreachable — wrong `KAFKA_BROKERS`, the `kafka` Compose profile not selected, or the broker down | Confirm `docker compose ps kafka` is **healthy** and that `KAFKA_BROKERS` matches the listener you can actually reach: `kafka:9092` from inside the network, `localhost:9092` from the host. Selecting the profile without setting `KAFKA_BROKERS` gives a broker Blnk ignores; setting the variable without the profile gives a relay retrying against nothing. |
| Topic creation rejected with `INVALID_REPLICATION_FACTOR` | `KAFKA_REPLICATION_FACTOR` exceeds the broker count | Set it to `1` on a single-broker stack, `3` on a replicated cluster. It is configuration precisely because no single literal is right for both. |
| `ErrReplicationFactorInadequate` on an existing topic | The topic sits at fewer replicas than configured | Reassign the topic's partitions to the configured factor, then re-run assurance. The **minimum** replica count across partitions is what is checked, because durability is decided by the weakest one. |
| `ErrPartitionGrowthRefused` | A non-empty topic has fewer partitions than `KAFKA_MIN_PARTITIONS` | Plan the migration: provision a correctly shaped topic, move consumers, drain the old one. `KAFKA_ALLOW_PARTITION_GROWTH=true` performs the growth step **and re-maps keys**, breaking ordering for every key already written — see [Partitions](#partitions). |
| Bootstrap fails, or the broker starts and authenticates nobody | The image predates Kafka 3.5 / Confluent Platform 7.5.0, so `kafka-storage format` has no `--add-scram` | Raise the image above the floor. The stack pins `apache/kafka:4.3.1` by digest. Running `kafka-configs` afterwards **cannot** fix it — see [Step 1](#step-1--bootstrap-the-scram-admin-credential-before-the-brokers-first-start). |
| Nothing is restricted: a principal reads a topic it holds no grant for | `authorizer.class.name` is unset, so Kafka permits every request | Set `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` and restart the broker, then re-run the behavioural check in [Verify the authorizer is active](#verify-the-authorizer-is-active-rather-than-trusting-it). **From the data path this failure is silent** — verify behaviourally, assume nothing. |
| ACL commands or credential issuance fail with `SECURITY_DISABLED` | Same root cause: no authorizer is configured, so the ACL admin APIs have nothing to act on | Set the authorizer as above. Do **not** treat the refusal as a Blnk fault or bypass it — it is the guard that stops unrestricted credentials being issued. |
| Every credential is rejected after `kafka_data` was destroyed | The volume held `__cluster_metadata`, so every SCRAM credential and ACL is gone | Re-bootstrap: bring the broker up so `scripts/kafka-bootstrap.sh` reformats and re-seeds the admin credential, then re-run provisioning. **Recovery differs by how the principal was created** — see below. |
| `SCRAM authentication failed` for a credential you are sure is right | The password contains `,`, `=`, `[` or `]`, and Kafka's `--add-scram` / `--add-config` grammar has no escape sequence, so it was silently truncated | Re-issue with a value from the safe alphabet. The scripts refuse such a value up front by name; a credential set by hand outside them will not. |
| `kafka-init` restarts three times and the stack never comes up | Provisioning is refusing a configuration — a bad credential pair, an impossible replication factor, a rejected variable | Read `docker compose logs kafka-init`. The cap exists so a permanent failure is terminal: the container stays `Exited(1)`, the dependency gate fails fast, and you get one error to read instead of a log growing a fresh copy of itself every few seconds. |
| The publisher refuses to build; the server will not start | Brokers and an administrative pair are configured but no producer pair is | Set `KAFKA_SASL_USER` and `KAFKA_SASL_SECRET`. Publishing as the administrator would make a leaked producer credential a compromise of the cluster's whole authorization state. `KAFKA_ALLOW_ADMIN_PRODUCER=true` is a documented, warned-about escape hatch for a deployment mid-upgrade, not a fix. |
| Both Kafka clients refuse to connect, naming TLS | `KAFKA_TLS_ENABLED` is off and `KAFKA_INSECURE_LOCAL_DEV` is not set | Configure the `KAFKA_TLS_*` block. Only set `KAFKA_INSECURE_LOCAL_DEV` for the local single-broker stack; it is warned about on every configuration load. |
| A subscriber sees records outside its `partition_key_prefix` | No ACL evaluates a message key, so the prefix is enforced by the component the deployment declares in front of the brokers — and if `KAFKA_KEY_SCOPE_ENFORCEMENT` is `none`, nothing enforces it and no credential should have been issued for that row | Declare the enforcing component, or — if the records must be unreachable rather than filtered — narrow `authorized_topics`, which is a real ACL. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker). |
| `PUT /subscribers/{id}` with a `partition_key_prefix` answers `500` naming a constraint | The database still carries `event_subscribers_key_scope_chk` | Apply **all** pending migrations, not just the next one: three of them drop this constraint and the last is `sql/1781249138.sql`. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker). |
| Replay answers `409 EVENT_NOT_DEAD_LETTERED` | The row is `failed` (its dead-letter write is still owed), already replayed, or another replay holds it | Restore broker reachability so the dead-letter write completes, then replay. See [the two states](#the-two-states-and-why-only-one-is-replayable). |
| A subscriber cannot join its consumer group | The group is outside its `blnk-sub-<subscriber_id>.` namespace, or the binding was written without the trailing delimiter | Use a leaf inside the namespace — `blnk-sub-<id>.default` is the issued default. Check the binding is `PREFIXED` on the dot-terminated namespace. |
| The alerts never fire, and nothing looks wrong | The rules are not loaded, or the scrape is refused | Check `http://localhost:9090/rules` — not `/targets` — and the bearer-token note in [Is the alert armed at all?](#is-the-alert-armed-at-all). |

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
| A subscriber sees records outside its `partition_key_prefix` | It is consuming **directly from the broker**, which evaluates no message key — so its row records no prefix, or its credential predates the prefix being recorded | Confirm the row carries the prefix, then re-issue: `enforced_access.gateway_delivery_required` must read `true` and the principal must hold no topic `Read` binding. Point the subscriber at the `broker_endpoint` the new response carries. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker). |
| `PUT /subscribers/{id}` with a `partition_key_prefix` answers `500` naming a constraint | The database still carries `event_subscribers_key_scope_chk` | Apply **all** pending migrations, not just the next one: three of them drop this constraint and the last is `sql/1781249138.sql`. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker). |
| `POST /subscribers/{id}/kafka-credentials` answers `409 SUBSCRIBER_KEY_SCOPE_UNENFORCED` | The row records a `partition_key_prefix` and no key-authorising component is declared, or `KAFKA_KEY_SCOPE_GATEWAY_BROKERS` is unset or identical to `KAFKA_BROKERS` | Take one of the two remedies the refusal names: declare a component and a distinct endpoint, or clear the prefix with `PUT /subscribers/{id}` and narrow `authorized_topics` instead. See [the partition-key prefix](#the-partition-key-prefix-is-enforced-outside-the-broker). |
| `POST /subscribers/{id}/kafka-credentials` answers `503 SUBSCRIBER_BROKERS_NOT_CONFIGURED` | `KAFKA_SUBSCRIBER_BROKERS` is unset. There is no fallback to `KAFKA_BROKERS`: those addresses are internal and would not resolve for the subscriber | Set the externally advertised broker list and retry. Nothing was minted, so no cleanup is needed. |
| Replay answers `409 EVENT_NOT_DEAD_LETTERED` | The row is `failed` (its dead-letter write is still owed), already replayed, or another replay holds it | Restore broker reachability so the dead-letter write completes, then replay. See [the two states](#the-two-states-and-why-only-one-is-replayable). |
| A subscriber cannot join its consumer group | The group is outside its `blnk-sub-<subscriber_id>.` namespace, or the binding was written without the trailing delimiter | Use a leaf inside the namespace — `blnk-sub-<id>.default` is the issued default. Check the binding is `PREFIXED` on the dot-terminated namespace. |
| The alerts never fire, and nothing looks wrong | The rules are not loaded, or the scrape is refused | Check `http://localhost:9090/rules` — not `/targets` — and the bearer-token note in [Is the alert armed at all?](#is-the-alert-armed-at-all). |

## Related Documents

| Document | Covers |
|----------|--------|
| [event-streaming.md](event-streaming.md) | The subscriber contract: the topic catalogue, the `LedgerEvent` envelope, the event vocabulary, the payload, the delivery guarantees and the idempotency obligation, partitioning and ordering, and the `<topic>.dlt` naming convention |
| [webhook-to-kafka-migration.md](webhook-to-kafka-migration.md) | Migrating off HTTP webhooks: the dual-run timeline, the payload-equivalence guarantee, and what happens at the sunset |
| [metrics.md](metrics.md) | The metric catalogue, the attribute domains and example Prometheus queries |
