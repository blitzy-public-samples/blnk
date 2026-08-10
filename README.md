![Blnk logo](https://res.cloudinary.com/dmxizylxw/image/upload/v1724847576/blnk_github_logo_eyy2lf.png)

<br/>

## Status at a Glance

![Build and Test Status](https://github.com/blnkfinance/blnk/actions/workflows/go.yml/badge.svg)
![Deploy to Docker Status](https://github.com/blnkfinance/blnk/actions/workflows/docker-publish.yml/badge.svg)
![Linter Status](https://github.com/blnkfinance/blnk/actions/workflows/lint.yml/badge.svg)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)

<br/>

## Open-Source Financial Ledger for Developers

Blnk is an open-source, double-entry ledger for teams building fintech products, wallets, banking infrastructure, payment systems, lending products, rewards programs, and other transaction-heavy financial applications.

It gives developers the core primitives needed to record transactions, manage balances, reconcile external records, and keep financial state accurate as products scale.

[Read the developer docs](https://docs.blnkfinance.com/home/install?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing) | [Deploy on Blnk Cloud](https://cloud.blnkfinance.com/auth/sign-up?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing) | [View Support plans](https://blnkfinance.com/pricing#support-plans?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing) 

<br/>

## Quick start

The fastest way to understand Blnk is to deploy a sandbox and follow the developer docs.

Start here:

- [Install Blnk locally](https://docs.blnkfinance.com/home/install) or [Deploy your sandbox](https://cloud.blnkfinance.com/auth/sign-up?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
- [Learn how ledgers, balances, and transactions fit together](https://docs.blnkfinance.com/ledgers/introduction?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
- [Explore Blnk tutorials](https://docs.blnkfinance.com/tutorials?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
- [Read the Core API reference](https://docs.blnkfinance.com/reference/overview?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
- [Set up Kafka event streaming](docs/kafka-operations.md), which is optional: the Docker Compose stack ships a single-broker Kafka with SASL/SCRAM behind an opt-in `kafka` profile and provisions the topics at startup, and with `KAFKA_BROKERS` unset the ledger runs exactly as before

<br/>

## Why teams use Blnk

Financial products need more than a database table of balances. They need a reliable ledger that can track every movement of value, preserve transaction history, support reconciliation, and help teams reason about correctness.

Blnk helps teams:

- Build on double-entry accounting principles from day one.
- Record transactions across ledgers, balances, and identities.
- Monitor balances and historical balance states.
- Handle inflight transactions, scheduled transactions, overdrafts, and bulk transaction workflows.
- Reconcile internal ledger records with external statements or provider data.
- Tokenize and manage identity data linked to balances and transactions.
- Move faster without hand-rolling critical financial infrastructure.

<br/>

## What you can build

Developers use Blnk for workflows such as:

1. [Wallet management](https://docs.blnkfinance.com/tutorials/quick-start/wallet-management?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
2. [Deposits and withdrawals](https://docs.blnkfinance.com/tutorials/digital-banking/deposits-withdrawals?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
3. [Order exchange](https://docs.blnkfinance.com/tutorials/crypto/order-exchange?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
4. [Lending](https://docs.blnkfinance.com/tutorials/digital-banking/lending?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
5. [Loyalty points systems](https://docs.blnkfinance.com/tutorials/quick-start/loyalty-points?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
6. [AI billing](https://docs.blnkfinance.com/tutorials/more/ai-billing?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)
7. [Escrow applications](https://docs.blnkfinance.com/tutorials/quick-start/escrow-payments?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)

<br/>

## Core capabilities

### Ledger

Blnk provides an open-source double-entry ledger for managing balances and recording transaction workflows. It supports balance monitoring, balance snapshots, historical balances, inflight transactions, scheduling, overdrafts, bulk transactions, and other ledger operations needed in production financial systems.

### Reconciliation

Blnk helps teams match external records, such as bank statements or payment processor exports, against internal ledger records using custom matching rules and reconciliation strategies.

### Identity management

Blnk lets teams create and manage identities, tokenize PII, and link identities to balances and transactions.

### Event streaming

Blnk publishes every ledger event to Kafka, so subscribers consume a stream directly instead of receiving HTTP pushes. An event produced by a ledger mutation is captured in a PostgreSQL transactional outbox inside that mutation's own database transaction, so the two commit together and the event is never lost because the broker was unavailable. That covers every transaction, ledger, balance and identity event, coalesced batches included. A relay then publishes the event, retries with bounded backoff, and dead-letters it if every attempt fails.

Events are grouped into four category topics, `blnk.transactions`, `blnk.balances`, `blnk.identities`, and `blnk.system`, each with a dead-letter sibling named by appending `.dlt`. Every event that belongs to a ledger is keyed by that ledger id, so those events land on one partition and arrive in the order the mutations happened. Three types belong to no ledger and are keyed on the aggregate they do describe: `bulk_transaction.<status>` by batch id, `identity.created` by identity id, and `system.error` by event type. Each subscriber is a Kafka principal with its own SASL/SCRAM credentials and ACLs scoped to the topics and consumer group it was granted.

The outbox gives exactly-once capture on the write side, while Kafka delivery itself is at-least-once, so `event_id` is the subscriber's idempotency key and deduplicating on it is the subscriber's responsibility. Three event types carry a weaker guarantee through one narrow window each — a `balance.monitor` alert whose pre-write evaluation could not read its monitors, a `bulk_transaction.<status>` summary whose finalising transaction never commits, and `system.error`, which describes no mutation and so has no transaction to join. The event streaming reference names each window and what it costs you. Blnk owns every `<topic>.dlt` name and does not implement or manage subscriber-side dead-lettering, so name your own dead-letter topics outside that namespace.

Read [the event streaming reference](docs/event-streaming.md) for the full topic catalogue, the event schema, and the idempotency guidance. If you run a webhook receiver today, [the migration guide](docs/webhook-to-kafka-migration.md) covers the move: HTTP webhook delivery and Kafka publishing run concurrently, from the same outbox events, for a 30-day window, after which the deprecated webhook management API answers `410 Gone` on every request. Provisioning, the ACL model, and the dead-letter triage and replay runbook are in [the Kafka operations runbook](docs/kafka-operations.md).

<br/>

## Managed hosting & support

Blnk is open source, so you can self-host and extend it for your own stack.

If your team wants a managed path, you can use [Blnk Cloud](https://cloud.blnkfinance.com/auth/sign-up?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing).

If you are integrating Blnk into a production product and want dedicated help from the team, you can [view Blnk Support plans](https://blnkfinance.com/pricing#support-plans?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing).

<br/>

## Community

If Blnk is useful to you:

- Star the repository so more developers can discover it.
- Join the community on Discord: [Accept Discord invite](https://discord.gg/7WNv94zPpx)
- Visit the website: [blnkfinance.com](https://www.blnkfinance.com?utm_source=github&utm_medium=readme_md&utm_campaign=oss_commercial_routing)

<br/>

## License

This project uses the [Apache License 2.0](LICENSE.md).
