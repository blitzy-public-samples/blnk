-- Copyright 2024 Blnk Finance Authors.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- +migrate Up

-- The event subscriber registry: who may consume Blnk's Kafka event topics, and
-- as which Kafka principal.
--
-- A row is the durable record of one consumer of the event stream — its Kafka
-- principal, the consumer group it reads under, the topics it is authorised for,
-- and the fact that a credential was issued to it and when. This table is the
-- persistence behind POST /subscribers/{id}/kafka-credentials: that endpoint
-- reads the row, provisions the SASL/SCRAM credential and the matching ACLs
-- against the broker, and returns the broker endpoint, the topic list, the
-- consumer group and the secret.
--
-- Nothing in this schema resembled a subscriber before this migration, so the
-- vocabulary fixed here is canonical rather than derived: the repository layer,
-- the subscriber service and the API DTOs are all written against these column
-- names, and a later synonym would be a second name for one thing rather than a
-- correction of a first.
--
-- THERE IS NO PER-TENANT TOPIC, and that single design fact is what makes the
-- columns below legible. Every subscriber reads the SAME shared category topics
-- (<prefix>.transactions, <prefix>.balances, <prefix>.identities and
-- <prefix>.system). Isolation is not achieved by giving each subscriber a topic
-- of its own; it is achieved by making each subscriber a distinct Kafka principal
-- and scoping that principal with ACLs — Read and Describe on exactly the topics
-- named in authorized_topics, Read on its own consumer group, and a
-- partition-key prefix narrowing which keys within those topics it is entitled
-- to. A reader expecting per-tenant topics will look here for a topic-name column
-- and not find one; that absence is deliberate.
--
-- THIS TABLE NEVER STORES A SECRET — see the credential group below, which states
-- the constraint in full. In short: the SASL password is generated at issuance,
-- returned to the caller exactly once and then discarded, and only a
-- non-reversible reference to it plus the issuance instant are persisted. That
-- mirrors blnk.api_keys, where `key` holds a bcrypt hash and the raw key is never
-- written anywhere.
--
-- Every timestamp below is TIMESTAMP WITH TIME ZONE, following blnk.api_keys and
-- the sibling blnk.event_outbox rather than the naive TIMESTAMP of the older
-- tables. Both timestamps here are facts about WHEN something happened that are
-- read outside the process that wrote them: credential_issued_at is the audit
-- record of a credential having been minted, and migrated_at drives
-- migration-progress reporting across the dual-delivery window. A naive timestamp
-- makes both silently wrong by a whole-hour offset on any deployment whose server
-- timezone is not UTC, with no error to notice.
--
-- The table carries NO FOREIGN KEY, by design. A subscriber is a Kafka consumer
-- principal, not a ledger participant: it has no owning identity, balance or
-- ledger to point at, and it is the counterparty to nothing. Referencing
-- blnk.identity or blnk.balances would model a relationship that does not exist
-- and would take a lock on a hot domain table on every subscriber write for no
-- benefit. It does not reference blnk.event_outbox either — a subscriber consumes
-- a topic, never an individual outbox row.
--
-- Finally, this table is deliberately small and read-rarely: it is consulted once
-- per credential issuance and during migration reporting, and never on the
-- transaction hot path. That is why the three indexes below are enough, and why a
-- fourth would be cost without a reader.
CREATE TABLE IF NOT EXISTS blnk.event_subscribers (
    -- Surrogate key, matching blnk.lineage_outbox and the sibling
    -- blnk.event_outbox. The key callers use is subscriber_id below; this one
    -- gives the row a stable, compact internal identity and lets an insert end
    -- RETURNING id.
    id                    BIGSERIAL                 PRIMARY KEY,

    -- ===================================================================
    -- Group 1: subscriber identity
    -- ===================================================================

    -- The business key, and the {id} in POST /subscribers/{id}/kafka-credentials.
    --
    -- TEXT rather than the uuid type. Every business key in this schema is a
    -- '<prefix>_<uuid>' string produced by model.GenerateUUIDWithSuffix — e.g.
    -- 'sub_9f1c8a72-...' — exactly as ledgers.ledger_id, identity.identity_id,
    -- balances.balance_id and api_keys.api_key_id already are. A uuid column
    -- would reject the prefix outright.
    subscriber_id         TEXT                      NOT NULL,

    -- The human label an operator recognises the subscriber by, matching
    -- api_keys.name. NOT NULL because an unnamed principal cannot be triaged, and
    -- being able to answer "who is this principal?" months later is most of the
    -- reason the registry exists.
    name                  TEXT                      NOT NULL,

    -- ===================================================================
    -- Group 2: the Kafka access model
    -- No per-tenant topics — see the note above the table. These four columns ARE
    -- the isolation boundary, and they are what a provisioning call translates
    -- into one SCRAM credential and a set of ACL bindings.
    -- ===================================================================

    -- The SASL/SCRAM username the ACLs are granted to: the Kafka principal this
    -- subscriber authenticates as. Every ACL binding provisioned for the
    -- subscriber names this value, which makes it the join key between a registry
    -- row and the broker's own authorization state.
    kafka_principal       TEXT                      NOT NULL,

    -- The consumer group the subscriber reads under, returned verbatim by the
    -- credential endpoint. The provisioned ACL grants Read on it with a PREFIXED
    -- pattern type, which reserves the subscriber's whole group namespace without
    -- having to enumerate every group it might one day create.
    consumer_group_id     TEXT                      NOT NULL,

    -- The topics this subscriber may Read and Describe: the list the credential
    -- endpoint returns, and the exact set the ACLs are granted over.
    --
    -- TEXT[] follows api_keys.scopes, the only other array column in this schema,
    -- and is read through lib/pq's pq.Array exactly as scopes is.
    --
    -- The DEFAULT '{}' is a security property rather than a convenience: a freshly
    -- registered subscriber is authorised for NOTHING instead of for NULL, so the
    -- registry FAILS CLOSED. A NULL here would make "no grant recorded"
    -- indistinguishable from "grant not yet known", and every authorisation check
    -- written against it would have to guess which was meant.
    --
    -- Deliberately NO CHECK constraint enumerating topic names. The topic prefix
    -- is configurable (KAFKA_TOPIC_PREFIX, default 'blnk'), so a hardcoded
    -- 'blnk.transactions'-style constraint would reject every write on any
    -- deployment using a non-default prefix.
    authorized_topics     TEXT[]                    NOT NULL DEFAULT '{}',

    -- The partition-key prefix the subscriber's grant is narrowed to, when it is
    -- narrowed at all.
    --
    -- Nullable, and the NULL carries meaning: it says the subscriber is entitled
    -- to whole topics rather than to a key-prefixed slice of them, which is a
    -- legitimate grant and not a missing value. A reader must treat NULL as "no
    -- key restriction" and never as "restrict to the empty prefix", which would
    -- invert the intent.
    partition_key_prefix  TEXT                      NULL,

    -- ===================================================================
    -- Group 3: the credential record — the security-critical part
    --
    -- READ THIS BEFORE ADDING A COLUMN HERE. This table records a NON-REVERSIBLE
    -- reference to the credential and the instant it was issued, and NEVER THE
    -- PASSWORD. The SASL secret is generated during provisioning, returned to the
    -- caller exactly once by the subscriber service, and is persisted neither here
    -- nor anywhere else: it cannot be recovered afterwards, only replaced by
    -- issuing a new one. This mirrors blnk.api_keys, where `key` holds a bcrypt
    -- hash and the raw key is never stored.
    --
    -- There is therefore deliberately NO column capable of holding a plaintext or
    -- reversibly-encrypted secret — no password, no secret, no sasl_password, no
    -- token, no encrypted_* variant, and no BYTEA blob standing in for one. The
    -- prohibition is enforced HERE, in the schema, rather than left to the
    -- application layer, because a column that exists will eventually be written
    -- to; and once a secret column has shipped and been populated, removing it is
    -- an incident rather than a migration.
    -- ===================================================================

    -- A non-reversible reference to the issued credential: enough to prove which
    -- credential a row corresponds to and to correlate an issuance with broker
    -- state, and not enough to authenticate with. Nullable because a subscriber
    -- legitimately exists in the registry before any credential has been issued to
    -- it — "registered, not yet provisioned" is a real state the registry has to be
    -- able to represent.
    credential_reference  TEXT                      NULL,

    -- When the credential was issued. Nullable, and set together with
    -- credential_reference; the two of them are the complete record of an
    -- issuance, and a reissue overwrites both. NULL here is the reliable test for
    -- "no credential has ever been issued to this subscriber".
    credential_issued_at  TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 4: dual-run migration tracking — TEMPORARY BY DESIGN
    --
    -- These two columns exist only for the 30-day window in which Kafka
    -- publishing and legacy HTTP webhook delivery run side by side, and they are
    -- the columns a post-sunset migration removes. Nothing else in this table is
    -- temporary: if a later migration drops columns from here, it should drop
    -- exactly these two and the index over migrated_at, and nothing more.
    --
    -- They exist because the requirement to migrate existing subscribers off the
    -- webhook subscription REST API meets a repository in which no such API
    -- exists. The entire subscription surface today is one global
    -- WebhookConfig{Url, Headers} value in the configuration, consumed by
    -- processHTTP; there is no per-subscriber URL storage and no registration
    -- endpoint anywhere. Recording a legacy URL per subscriber is what gives an
    -- existing subscriber somewhere to be recorded and migrated FROM, and it is
    -- what makes the 410 Gone sunset behaviour observable at all — without a
    -- subscription surface there is no route on which a 410 could ever be seen.
    -- ===================================================================

    -- The legacy HTTP webhook URL this subscriber received pushes on before moving
    -- to Kafka. Nullable: a subscriber onboarded after the cutover never had one.
    webhook_url           TEXT                      NULL,

    -- When the subscriber completed its move to Kafka consumption. NULL means NOT
    -- YET MIGRATED, which is exactly what migration-progress reporting counts and
    -- what idx_event_subscribers_migrated_at below exists to serve.
    migrated_at           TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 5: row bookkeeping
    -- Both NOT NULL DEFAULT NOW(), matching blnk.chain_state and blnk.api_keys.
    -- updated_at is maintained by the repository layer on write, as every other
    -- table in this schema does it — blnk has no updated_at trigger anywhere, and
    -- this table does not introduce the first one.
    -- ===================================================================

    created_at            TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW()
);

-- The business-key lookup, and the house guarantee every table in this schema
-- already gives its own business key: ledgers.ledger_id, identity.identity_id,
-- balances.balance_id, api_keys.api_key_id and lineage_outbox.transaction_id are
-- all unique. Two rows claiming one subscriber_id would leave
-- POST /subscribers/{id}/kafka-credentials ambiguous about which registry row it
-- is provisioning for, so the uniqueness is enforced here rather than assumed by
-- the caller.
--
-- It is also the index that lookup rides on. Credential provisioning has a
-- 5-second budget and this lookup sits on the request path, so it resolves
-- through this index instead of through a sequential scan whose cost would grow
-- with the registry.
CREATE UNIQUE INDEX IF NOT EXISTS event_subscribers_subscriber_id_uidx
    ON blnk.event_subscribers (subscriber_id);

-- The isolation guard, and the reason this index is UNIQUE rather than plain.
--
-- Two registry rows naming the same Kafka principal would mean two subscribers
-- sharing one SASL credential and one set of ACL bindings. Each would then be
-- able to read everything the other is authorised for, and the isolation the
-- access model promises would simply be gone — silently, because nothing would
-- fail, nothing would be logged, and each row would look perfectly correct on its
-- own. The database is the only place that can make that state impossible, so it
-- does.
--
-- It also serves the reverse lookup: given a principal observed in broker
-- authorization state or in a consumer-lag reading, which subscriber is that? That
-- is what makes the subscriber attribute on the consumer-lag gauge resolvable.
CREATE UNIQUE INDEX IF NOT EXISTS event_subscribers_kafka_principal_uidx
    ON blnk.event_subscribers (kafka_principal);

-- Migration-progress reporting across the dual-delivery window: how many
-- subscribers have moved to Kafka, and which have not.
--
-- Neither partial nor unique, and both choices are deliberate. The "not yet
-- migrated" query filters on migrated_at IS NULL, which a plain btree does serve
-- in PostgreSQL because NULLs are stored in the index; a partial index WHERE
-- migrated_at IS NULL would answer that one question and nothing else, whereas
-- this one also answers "migrated since <date>" and can order by migration time.
-- Unique would be wrong outright: any number of subscribers may migrate in the
-- same instant, and every unmigrated row holds NULL.
--
-- This index goes at sunset, together with the two dual-run columns.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_migrated_at
    ON blnk.event_subscribers (migrated_at);

-- +migrate Down

-- Indexes first, then the table, in reverse creation order. Dropping the table
-- would take its indexes with it, but naming them explicitly keeps the rollback
-- correct if a future migration ever detaches one of these objects from the
-- other, and it documents exactly what this migration created. Index names are
-- schema-qualified in the drop even though they are unqualified in the create;
-- that asymmetry is the house convention.
DROP INDEX IF EXISTS blnk.idx_event_subscribers_migrated_at;
DROP INDEX IF EXISTS blnk.event_subscribers_kafka_principal_uidx;
DROP INDEX IF EXISTS blnk.event_subscribers_subscriber_id_uidx;

DROP TABLE IF EXISTS blnk.event_subscribers;
