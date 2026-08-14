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

-- The event subscriber registry: who may consume Blnk's Kafka event topics, and as
-- which Kafka principal.
--
-- A row is the durable record of one consumer of the event stream — its Kafka
-- principal, the consumer group it reads under, the topics it is authorised for, and
-- the fact that a credential was issued to it and when. This table is the persistence
-- behind POST /subscribers/{id}/kafka-credentials: that endpoint reads the row,
-- provisions the SASL/SCRAM credential and the matching ACLs against the broker, and
-- returns the broker endpoint, the topic list, the consumer group and the secret.
--
-- Nothing in this schema resembled a subscriber before this migration, so the
-- vocabulary fixed here is canonical rather than derived: the repository layer, the
-- subscriber service and the API DTOs are all written against these column names, and a
-- later synonym would be a second name for one thing rather than a correction of a
-- first.
--
-- THERE IS NO PER-TENANT TOPIC, and that single design fact is what makes the columns
-- below legible. Every subscriber reads the SAME shared category topics
-- (<prefix>.transactions, <prefix>.balances, <prefix>.identities and <prefix>.system).
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
    -- balances.balance_id and api_keys.api_key_id already are.
    subscriber_id         TEXT                      NOT NULL,

    -- The human label an operator recognises the subscriber by, matching
    -- api_keys.name. NOT NULL because an unnamed principal cannot be triaged, and
    -- being able to answer "who is this principal?" months later is most of the
    -- reason the registry exists.
    name                  TEXT                      NOT NULL,

    -- ===================================================================
    -- Group 2: the Kafka access model
    -- No per-tenant topics — see the note above the table. THREE of the four columns
    -- here are the enforced boundary (kafka_principal, consumer_group_id,
    -- authorized_topics); the fourth, partition_key_prefix, is advisory and says so at
    -- its own declaration. The three are what a provisioning call translates into one
    -- SCRAM credential and a set of ACL bindings.
    -- ===================================================================

    -- The SASL/SCRAM username the ACLs are granted to: the Kafka principal this
    -- subscriber authenticates as. Every ACL binding provisioned for the subscriber
    -- names this value, which makes it the join key between a registry row and the
    -- broker's own authorization state.
    kafka_principal       TEXT                      NOT NULL,

    -- The consumer group the subscriber reads under, returned verbatim by the
    -- credential endpoint. The provisioned ACL grants Read on it with a PREFIXED
    -- pattern type, which reserves the subscriber's whole group namespace without
    -- having to enumerate every group it might one day create.
    consumer_group_id     TEXT                      NOT NULL,

    -- The topics this subscriber may Read and Describe: the list the credential
    -- endpoint returns, and the exact set the ACLs are granted over.
    --
    -- TEXT[] follows api_keys.scopes, the only other array column in this schema, and
    -- is read through lib/pq's pq.Array exactly as scopes is.
    authorized_topics     TEXT[]                    NOT NULL DEFAULT '{}',

    -- A ROUTING HINT THAT KAFKA CANNOT ENFORCE — NOT AN ACCESS BOUNDARY. See the
    -- boundary note above the table.
    --
    -- READ THIS BEFORE DECIDING TENANCY FROM THIS TABLE. A value here does NOT confine
    -- the subscriber to the records it names.
    partition_key_prefix  TEXT                      NULL,

    -- ===================================================================
    -- Group 3: the credential record — the security-critical part
    -- ===================================================================
    --
    -- READ THIS BEFORE ADDING A COLUMN HERE. This table records a NON-REVERSIBLE
    -- reference to the credential and the instant it was issued, and NEVER THE
    -- PASSWORD.

    -- A non-reversible reference to the issued credential: enough to prove which
    -- credential a row corresponds to and to correlate an issuance with broker state,
    -- and not enough to authenticate with. Nullable because a subscriber legitimately
    -- exists in the registry before any credential has been issued to it — "registered,
    -- not yet provisioned" is a real state the registry has to be able to represent.
    credential_reference  TEXT                      NULL,

    -- When the credential was issued. Nullable, and set together with
    -- credential_reference; the two of them are the complete record of an
    -- issuance, and a reissue overwrites both. NULL here is the reliable test for
    -- "no credential has ever been issued to this subscriber".
    credential_issued_at  TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 4: dual-run migration tracking — ONE COLUMN TEMPORARY, ONE PERMANENT
    -- webhook_url exists only for the dual-delivery window and is dropped with the
    -- legacy transport; migrated_at is permanent, because migration progress stays
    -- worth reporting after the cutover. Nothing else in this table depends on either.
    -- ===================================================================

    -- The legacy HTTP webhook URL this subscriber received pushes on before moving
    -- to Kafka. Nullable: a subscriber onboarded after the cutover never had one.
    webhook_url           TEXT                      NULL,

    -- When the subscriber completed its move to Kafka consumption. NULL means NOT
    -- YET MIGRATED, which is exactly what migration-progress reporting counts and
    -- what idx_event_subscribers_migrated_at below exists to serve.
    migrated_at           TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 4b: the revocation lifecycle
    -- ===================================================================
    --
    -- ENDING A SUBSCRIBER'S ACCESS IS TWO SYSTEMS' WORK AND CANNOT BE ONE STATEMENT.
    -- The broker holds the SCRAM credential and the ACL bindings; this table holds the
    -- principal and topic list that say WHICH credential and WHICH bindings.
    --
    -- When the revocation failed, the principal kept authenticating and kept reading,
    -- and the only record of which principal that was had just been destroyed —
    -- recoverable solely from a log line, if anyone read it. The residue was live
    -- access that nothing in Blnk could see.
    --
    -- So the row is TOMBSTONED first, the broker is revoked second, and the row is
    -- deleted only once that revocation has been confirmed. A row still carrying
    -- revocation_pending_at is the durable to-do item: it names the principal to
    -- revoke, and retrying the deregistration finishes the job.
    -- ===================================================================

    -- When deregistration began taking this subscriber's broker-side access away.
    --
    -- NULL for every ordinary subscriber. NON-NULL means "this subscriber is being
    -- taken out of service and its broker-side access may still be live": it is not an
    -- active subscriber, credential issuance refuses for it, and it disappears only
    -- when the revocation has succeeded.
    revocation_pending_at TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 4c: the provisioning fence
    -- ===================================================================
    --
    -- TWO CONCURRENT ISSUANCES FOR ONE SUBSCRIBER CANNOT BOTH BE RIGHT. Kafka stores
    -- ONE SCRAM credential per principal, so the second upsert replaces the first:
    -- after two overlapping issuances the broker holds one password while this table
    -- may hold a reference derived from the other, and the caller holding the recorded
    -- one cannot authenticate.
    --
    -- These two columns are the fence that makes the overlap impossible. An issuance
    -- claims the subscriber before it touches Kafka: the claim is an atomic conditional
    -- UPDATE that stamps a token and a lease, and it succeeds only when no live claim
    -- exists.
    --
    -- This is the SAME claim-token-and-lease shape blnk.event_outbox uses, and for the
    -- same two reasons: it is cross-process without a lock server, and a process that
    -- dies mid-issuance does not fence the subscriber for ever — the lease expires and
    -- the next attempt proceeds.
    -- ===================================================================

    -- The token of the issuance or revocation currently holding this subscriber.
    -- NULL when nothing holds it. Rotated on every claim, so a caller whose lease
    -- expired cannot release or complete a claim that has since been taken over.
    provisioning_token    TEXT                      NULL,

    -- When the current claim expires. A claim is live only while this is in the
    -- future, which is what makes a crashed issuance self-healing rather than a
    -- permanent block.
    provisioning_until    TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 5: row bookkeeping
    -- Both NOT NULL DEFAULT NOW(), matching blnk.chain_state and
    -- blnk.api_keys. updated_at is maintained by the repository layer on write, as
    -- every other table in this schema does it — blnk has no updated_at trigger
    -- anywhere, and this table does not introduce the first one.
    -- ===================================================================

    created_at            TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 6: canonical-identity constraints
    -- These live in the database as well as in the repository layer because a
    -- hand-written INSERT, a data fix or a future caller bypasses Go validation, and a
    -- principal or consumer group that is not derived from subscriber_id would grant
    -- access outside the boundary the ACLs were computed from.
    -- ===================================================================

    -- The identifier alphabet, matching model.CanonicalizeSubscriberIdentifier
    -- character for character: 3 to 128 characters, lowercase ASCII letters, digits,
    -- underscore and hyphen, and the first character a letter or digit.
    CONSTRAINT event_subscribers_subscriber_id_chk CHECK (
        subscriber_id ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
    ),

    -- The principal is DERIVED, not supplied. This is the constraint that makes that a
    -- property of the data.
    CONSTRAINT event_subscribers_principal_derived_chk CHECK (
        kafka_principal = 'blnk-sub-' || subscriber_id
    ),

    -- The consumer group must be a NON-EMPTY LEAF beneath the subscriber's own group
    -- namespace, 'blnk-sub-' || subscriber_id || '.', matching
    -- model.CanonicalConsumerGroupNamespace.
    CONSTRAINT event_subscribers_group_derived_chk CHECK (
        consumer_group_id ~ '^[a-z0-9][a-zA-Z0-9._-]*$'
        AND starts_with(consumer_group_id, 'blnk-sub-' || subscriber_id || '.')
        AND length(consumer_group_id) > length('blnk-sub-' || subscriber_id || '.')
    ),

    -- The prefix-independent half of the grantable-topic rule. The prefix-AWARE half —
    -- that every topic is a subscriber-facing Blnk category topic under the configured
    -- prefix — lives in the repository layer, which can read KAFKA_TOPIC_PREFIX.
    CONSTRAINT event_subscribers_topics_shape_chk CHECK (
        array_to_string(authorized_topics, ',')
            ~ '^([a-zA-Z0-9][a-zA-Z0-9._-]*)?(,[a-zA-Z0-9][a-zA-Z0-9._-]*)*$'
        AND array_to_string(authorized_topics, ',') !~ '(^|,)[^,]*[.]dlt(,|$)'
        AND NOT ('' = ANY (authorized_topics))
    ),

    -- The credential reference and its issuance instant are ONE record and must be
    -- written or cleared together.
    CONSTRAINT event_subscribers_credential_pair_chk CHECK (
        (credential_reference IS NULL AND credential_issued_at IS NULL)
        OR (credential_reference IS NOT NULL AND credential_issued_at IS NOT NULL)
    ),

    -- The provisioning fence is ONE claim and must be written or cleared as one, for
    -- the same reason the credential record is.
    CONSTRAINT event_subscribers_fence_pair_chk CHECK (
        (provisioning_token IS NULL AND provisioning_until IS NULL)
        OR (provisioning_token IS NOT NULL AND provisioning_until IS NOT NULL)
    )
);

-- The business-key lookup, and the house guarantee every table in this schema already
-- gives its own business key: ledgers.ledger_id, identity.identity_id,
-- balances.balance_id, api_keys.api_key_id and lineage_outbox.transaction_id are all
-- unique. Two rows claiming one subscriber_id would leave POST
-- /subscribers/{id}/kafka-credentials ambiguous about which registry row it is
-- provisioning for, so the uniqueness is enforced here rather than assumed by the
-- caller.
CREATE UNIQUE INDEX IF NOT EXISTS event_subscribers_subscriber_id_uidx
    ON blnk.event_subscribers (subscriber_id);

-- The isolation guard, and the reason this index is UNIQUE rather than plain.
--
-- It also serves the reverse lookup: given a principal observed in broker authorization
-- state or in a consumer-lag reading, which subscriber is that? That is what makes the
-- subscriber attribute on the consumer-lag gauge resolvable.
CREATE UNIQUE INDEX IF NOT EXISTS event_subscribers_kafka_principal_uidx
    ON blnk.event_subscribers (kafka_principal);

-- Migration-progress reporting across the dual-delivery window: how many subscribers
-- have moved to Kafka, and which have not.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_migrated_at
    ON blnk.event_subscribers (migrated_at);

-- The listing order, matched exactly.
--
-- The registry is read newest first, ORDER BY created_at DESC, id DESC. Without an
-- index in that order PostgreSQL has to read every subscriber and sort the whole set
-- before it can return the first page, so the cost of page one grows with the size of
-- the registry and a large enough registry spills the sort to disk.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_created_at
    ON blnk.event_subscribers (created_at DESC, id DESC);

-- +migrate Down

-- Indexes first, then the table, in reverse creation order. Dropping the table would
-- take its indexes with it, but naming them explicitly keeps the rollback correct if a
-- future migration ever detaches one of these objects from the other, and it documents
-- exactly what this migration created. Index names are schema-qualified in the drop
-- even though they are unqualified in the create; that asymmetry is the house
-- convention.
DROP INDEX IF EXISTS blnk.idx_event_subscribers_created_at;
DROP INDEX IF EXISTS blnk.idx_event_subscribers_migrated_at;
DROP INDEX IF EXISTS blnk.event_subscribers_kafka_principal_uidx;
DROP INDEX IF EXISTS blnk.event_subscribers_subscriber_id_uidx;

DROP TABLE IF EXISTS blnk.event_subscribers;
