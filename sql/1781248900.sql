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
-- and scoping that principal with ACLs. A reader expecting per-tenant topics will
-- look here for a topic-name column and not find one; that absence is deliberate.
--
-- THE ENFORCED BOUNDARY IS EXACTLY TWO THINGS, and stating it precisely matters
-- because the rest of this file is read as a description of who can see what:
--
--   1. Read and Describe ACLs on exactly the topics named in authorized_topics,
--      granted to kafka_principal.
--   2. A Read ACL on kafka_principal's own consumer group namespace.
--
-- Those two are the whole of it, because those two are what the broker evaluates
-- on every request. partition_key_prefix IS NOT PART OF THE BOUNDARY. Kafka
-- authorises at topic and group granularity only — no ACL operation restricts a
-- principal to a subset of a topic's partitions, or to records carrying a
-- particular key — so a principal permitted to read a topic can read EVERY record
-- in that topic no matter what that column holds. The column is an advisory
-- consumer-side filter and is documented as one where it is declared below.
--
-- The distinction is not pedantry. Describing the key prefix as isolation would
-- mean believing that two subscribers authorised for one topic cannot see each
-- other's events. They can. Anyone making a tenancy decision on the strength of
-- that belief would be granting a shared topic to parties who must not see one
-- another's data, and the registry would look correct while they did it.
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
    -- No per-tenant topics — see the note above the table. THREE of the four
    -- columns here are the enforced boundary (kafka_principal, consumer_group_id,
    -- authorized_topics); the fourth, partition_key_prefix, is advisory and says
    -- so at its own declaration. The three are what a provisioning call translates
    -- into one SCRAM credential and a set of ACL bindings.
    --
    -- kafka_principal and consumer_group_id are DERIVED FROM subscriber_id, never
    -- supplied. The CHECK constraints at the end of this table are what make that
    -- true of the stored data rather than merely true of the code path that
    -- normally writes it — see the constraint block for why.
    -- ===================================================================

    -- The SASL/SCRAM username the ACLs are granted to: the Kafka principal this
    -- subscriber authenticates as. Every ACL binding provisioned for the
    -- subscriber names this value, which makes it the join key between a registry
    -- row and the broker's own authorization state.
    --
    -- Always 'blnk-sub-' || subscriber_id, enforced by event_subscribers_principal_derived_chk.
    kafka_principal       TEXT                      NOT NULL,

    -- The consumer group the subscriber reads under, returned verbatim by the
    -- credential endpoint. The provisioned ACL grants Read on it with a PREFIXED
    -- pattern type, which reserves the subscriber's whole group namespace without
    -- having to enumerate every group it might one day create.
    --
    -- Always a non-empty leaf beneath 'blnk-sub-' || subscriber_id || '.', enforced
    -- by event_subscribers_group_derived_chk. The trailing '.' is load-bearing: it
    -- is what stops a PREFIXED grant on one subscriber's namespace from also
    -- covering another whose identifier merely starts with the same characters.
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
    -- Deliberately NO CHECK constraint enumerating topic NAMES. The topic prefix
    -- is configurable (KAFKA_TOPIC_PREFIX, default 'blnk'), so a hardcoded
    -- 'blnk.transactions'-style constraint would reject every write on any
    -- deployment using a non-default prefix. The prefix-aware allowlist —
    -- subscriber-facing category topics only — is enforced in the repository layer,
    -- which can read the configured prefix and this file cannot.
    --
    -- What IS constrained here is everything that holds regardless of prefix, in
    -- event_subscribers_topics_shape_chk below: each element must be a syntactically
    -- valid Kafka topic name, no element may end in '.dlt', and no element may be
    -- empty. Those three are prefix-independent facts, so the database can hold
    -- them, and each of them is a thing an ACL would otherwise be granted over.
    authorized_topics     TEXT[]                    NOT NULL DEFAULT '{}',

    -- An ADVISORY CONSUMER-SIDE FILTER. NOT an authorization boundary, and nothing
    -- may treat it as one. See the boundary note above the table.
    --
    -- Its honest purpose: a hint the subscriber may use to discard records it does
    -- not care about, and a place to record which slice of a shared topic is
    -- *intended* for it. Blnk keys every event by ledger ID, so a subscriber
    -- interested in one ledger can filter on that key rather than processing the
    -- whole topic.
    --
    -- What it CANNOT do: keep the subscriber from reading anything. Kafka has no
    -- ACL that restricts a principal to a key range or a partition subset, so a
    -- principal with Read on a topic reads all of it. The filter runs in the
    -- consumer, on records the broker has already handed over, and a subscriber
    -- that simply ignores the hint sees everything on every topic it is authorised
    -- for. Enforcement is topic-level and group-level ACLs; this column has no part
    -- in it.
    --
    -- Nullable, and the NULL carries meaning: no advisory narrowing is recorded.
    -- A reader must treat NULL as "no filter suggested" and never as "filter to the
    -- empty prefix", which would invert the intent.
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
    -- # THE RETENTION CONTRACT FOR THESE COLUMNS (RETAIN-01)
    --
    -- webhook_url is a THIRD PARTY'S ENDPOINT. It is an operational detail of
    -- somebody else's system, and it is a destination that becomes a request Blnk
    -- makes the moment any sender is wired to it. Once a subscriber has migrated
    -- it has no remaining purpose, so keeping it is retention without a reason.
    --
    -- It is therefore PURGED, not kept: PurgeMigratedSubscriberWebhookURLs sets it
    -- to NULL for every subscriber whose migrated_at precedes a caller-supplied
    -- cut-off. The URL is nulled rather than the row deleted, because the
    -- subscriber is still a live subscriber; only the migration artefact expires.
    --
    -- migrated_at is DELIBERATELY EXEMPT and is never purged. It is an audit fact
    -- — when this subscriber's cutover completed — it is neither personal nor
    -- third-party data, and it is what migration-progress reporting counts. That
    -- exemption is the audit exception this table's retention policy carries; it
    -- has no others.
    --
    -- # THE POST-SUNSET MIGRATION, SPECIFIED BUT DELIBERATELY NOT SHIPPED
    --
    -- No migration in this repository drops these columns, and that is a decision
    -- rather than an omission. sql-migrate applies every pending migration
    -- unconditionally on `blnk migrate up`: a drop committed today would run on the
    -- next deployment and remove the dual-run columns DURING the 30-day window,
    -- ending dual delivery early and destroying the URLs the window exists to
    -- migrate away from. The drop is a post-sunset operator action, so it is
    -- specified here and authored then:
    --
    --     -- +migrate Up
    --     DROP INDEX IF EXISTS blnk.idx_event_subscribers_migrated_at;
    --     ALTER TABLE blnk.event_subscribers DROP COLUMN IF EXISTS webhook_url;
    --     ALTER TABLE blnk.event_subscribers DROP COLUMN IF EXISTS migrated_at;
    --
    -- Run it only after WEBHOOK_DEPRECATION_SUNSET_DATE has passed AND after the
    -- migration report shows every subscriber migrated, because dropping
    -- migrated_at destroys the evidence that it did. Nothing else in this table is
    -- affected.
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
    updated_at            TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 6: canonical-identity constraints
    --
    -- WHY THESE ARE IN THE DATABASE AND NOT ONLY IN THE REPOSITORY LAYER.
    --
    -- The repository canonicalizes the identifier, derives the principal and the
    -- consumer group from it, and refuses a supplied value that differs. That is
    -- the right place for the primary check — it can return a useful error, and it
    -- runs before anything reaches the broker. But it protects only writes that go
    -- through it. A migration backfilling rows, a fix applied by hand during an
    -- incident, an admin tool, or a second code path added later all reach this
    -- table directly, and any of them can write a row the derivation would have
    -- refused.
    --
    -- What such a row costs is the reason the constraints are worth their weight.
    -- The principal and group on the row are what provisioning grants ACLs to and
    -- what revocation takes them away from. A row whose principal is not derived
    -- from its subscriber_id means the boundary that gets granted is not the
    -- boundary the registry appears to describe — and it fails in the dangerous
    -- direction, silently, because both the row and the ACL look internally
    -- consistent. Two subscribers whose identifiers differ only in case or
    -- surrounding whitespace collapse onto ONE principal, sharing one credential
    -- and one grant, each able to read everything the other is authorised for.
    --
    -- These are CHECK constraints rather than triggers because the derivation is a
    -- pure function of columns in the same row, which is exactly what CHECK
    -- expresses; and because blnk has no trigger anywhere in its schema, so a
    -- trigger here would be the first and would surprise every reader.
    -- ===================================================================

    -- The identifier alphabet, matching model.CanonicalizeSubscriberIdentifier
    -- character for character: 3 to 128 characters, lowercase ASCII letters,
    -- digits, underscore and hyphen, and the first character a letter or digit.
    --
    -- Lowercase-only is the anti-collapse rule. Kafka principals are compared
    -- byte-for-byte, so 'Acme' and 'acme' are different principals — but they are
    -- the same subscriber to every human who reads them, and admitting both means
    -- an operator revoking one leaves the other live. Refusing uppercase outright
    -- removes the ambiguity instead of trying to resolve it later.
    --
    -- Excluding whitespace does the same work for a subtler case: ' acme' and
    -- 'acme' would produce two principals that are indistinguishable in every log
    -- line and every dashboard label they ever appear in.
    --
    -- The first-character rule keeps '_acme' and '-acme' out. A leading hyphen is
    -- read as a flag by the Kafka CLI tooling an operator reaches for during an
    -- incident, which is the worst possible moment for a principal name to be
    -- unusable.
    CONSTRAINT event_subscribers_subscriber_id_chk CHECK (
        subscriber_id ~ '^[a-z0-9][a-z0-9_-]{2,127}$'
    ),

    -- The principal is DERIVED, not supplied. This is the constraint that makes
    -- that a property of the data.
    --
    -- Byte-for-byte equality against 'blnk-sub-' || subscriber_id, matching
    -- model.CanonicalKafkaPrincipal. The 'blnk-sub-' namespace keeps subscriber
    -- principals from colliding with the administrative principal or with any
    -- other principal an operator creates on the same cluster, so a subscriber can
    -- never be provisioned onto an identity that already means something else.
    CONSTRAINT event_subscribers_principal_derived_chk CHECK (
        kafka_principal = 'blnk-sub-' || subscriber_id
    ),

    -- The consumer group must be a NON-EMPTY LEAF beneath the subscriber's own
    -- group namespace, 'blnk-sub-' || subscriber_id || '.', matching
    -- model.CanonicalConsumerGroupNamespace.
    --
    -- starts_with() rather than LIKE, deliberately: '_' is a legal identifier
    -- character AND a LIKE single-character wildcard, so a LIKE pattern built from
    -- subscriber_id would silently match identifiers that merely differ at the
    -- underscore positions. starts_with() is a literal prefix test with no pattern
    -- language, so there is nothing to escape and nothing to get wrong.
    --
    -- The length test is what forces the leaf to be non-empty. Without it, the
    -- bare namespace 'blnk-sub-acme.' would qualify, and the PREFIXED ACL granted
    -- over it would cover the namespace itself rather than a group inside it.
    --
    -- The trailing '.' is the disjointness guarantee. A PREFIXED group ACL on
    -- 'blnk-sub-acme' — no terminator — also matches 'blnk-sub-acmecorp', so
    -- subscriber 'acme' would be able to join subscriber 'acmecorp's consumer
    -- groups and take its partition assignments. Requiring the terminator makes
    -- every subscriber's group namespace provably disjoint from every other's.
    --
    -- The alphabet test rejects control characters and the '*' wildcard. A newline
    -- in a group ID splits a log line in two and forges a second entry; a '*'
    -- reaching ACL-filter construction turns a single-group grant into a
    -- match-everything one.
    CONSTRAINT event_subscribers_group_derived_chk CHECK (
        consumer_group_id ~ '^[a-z0-9][a-zA-Z0-9._-]*$'
        AND starts_with(consumer_group_id, 'blnk-sub-' || subscriber_id || '.')
        AND length(consumer_group_id) > length('blnk-sub-' || subscriber_id || '.')
    ),

    -- The prefix-independent half of the grantable-topic rule (SEC-03). The
    -- prefix-AWARE half — that every topic is a subscriber-facing Blnk category
    -- topic under the configured prefix — lives in the repository layer, which can
    -- read KAFKA_TOPIC_PREFIX.
    --
    -- The elements are joined with ',' and matched as a comma-separated list of
    -- valid Kafka topic names. ',' is not a legal topic character, so it cannot
    -- appear inside an element and be mistaken for a separator, and the empty
    -- string matches the empty-array case correctly.
    --
    -- Uppercase is permitted in an element because KAFKA_TOPIC_PREFIX is
    -- operator-supplied and may legitimately be mixed-case; the CATEGORY part is
    -- always lowercase, but this file cannot tell prefix from category. Whitespace
    -- and control characters are excluded because they are not legal in a topic
    -- name and their only route in is a copy-paste accident or an injection
    -- attempt.
    --
    -- No element may END IN '.dlt'. Dead-letter topics hold events that already
    -- failed, together with failure_metadata naming broker addresses and internal
    -- error reasons; they are Blnk's operational surface, read through
    -- GET /events/dead-letter under the master key, and never a subscriber's.
    -- Granting one to a subscriber would hand it other subscribers' failed events.
    -- Only the SUFFIX is reserved, so a legitimately named topic containing 'dlt'
    -- elsewhere is unaffected.
    --
    -- No element may be EMPTY. An empty topic name cannot be granted meaningfully,
    -- and its presence in the list is always a bug in whatever assembled it —
    -- worth failing on rather than storing.
    CONSTRAINT event_subscribers_topics_shape_chk CHECK (
        array_to_string(authorized_topics, ',')
            ~ '^([a-zA-Z0-9][a-zA-Z0-9._-]*)?(,[a-zA-Z0-9][a-zA-Z0-9._-]*)*$'
        AND array_to_string(authorized_topics, ',') !~ '(^|,)[^,]*[.]dlt(,|$)'
        AND NOT ('' = ANY (authorized_topics))
    ),

    -- The credential reference and its issuance instant are ONE record and must be
    -- written or cleared together.
    --
    -- A reference without a timestamp cannot be audited — nobody can say when the
    -- credential was minted or whether it predates a rotation. A timestamp without
    -- a reference asserts that an issuance happened while retaining nothing that
    -- identifies what was issued, so it can neither be correlated with broker state
    -- nor revoked with confidence. Both halves being NULL is the legitimate
    -- "registered, not yet provisioned" state, and is what
    -- ClearSubscriberCredential returns a row to.
    CONSTRAINT event_subscribers_credential_pair_chk CHECK (
        (credential_reference IS NULL AND credential_issued_at IS NULL)
        OR (credential_reference IS NOT NULL AND credential_issued_at IS NOT NULL)
    )
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

-- The listing order, matched exactly.
--
-- The registry is read newest first, ORDER BY created_at DESC, id DESC. Without an
-- index in that order PostgreSQL has to read every subscriber and sort the whole
-- set before it can return the first page, so the cost of page one grows with the
-- size of the registry and a large enough registry spills the sort to disk. With
-- it, the scan starts at the newest row and stops once LIMIT is satisfied, so the
-- cost is proportional to the PAGE.
--
-- The column order is the ORDER BY, term for term, including both DESC markers.
-- That is what lets the index be read forwards to produce the required order
-- directly. It is also what makes the keyset cursor work: the paging predicate is
-- the row comparison (created_at, id) < (:created_at, :id), and a leading-column
-- match on the same pair is what the planner positions the scan from instead of
-- filtering after the fact.
--
-- The id tie-break is not decoration. created_at is stamped in Go, so two
-- subscribers registered in the same microsecond share an instant and their
-- relative order would otherwise be whatever the scan happened to produce —
-- unstable between two executions of the same query, which is precisely how a
-- paginated walk both repeats and skips rows at a page boundary.
--
-- A btree can be scanned in either direction, so this one also serves an
-- oldest-first read of the same pair; the explicit DESC is about matching the
-- default listing without a reverse scan, not about making the other direction
-- possible.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_created_at
    ON blnk.event_subscribers (created_at DESC, id DESC);

-- +migrate Down

-- Indexes first, then the table, in reverse creation order. Dropping the table
-- would take its indexes with it, but naming them explicitly keeps the rollback
-- correct if a future migration ever detaches one of these objects from the
-- other, and it documents exactly what this migration created. Index names are
-- schema-qualified in the drop even though they are unqualified in the create;
-- that asymmetry is the house convention.
DROP INDEX IF EXISTS blnk.idx_event_subscribers_created_at;
DROP INDEX IF EXISTS blnk.idx_event_subscribers_migrated_at;
DROP INDEX IF EXISTS blnk.event_subscribers_kafka_principal_uidx;
DROP INDEX IF EXISTS blnk.event_subscribers_subscriber_id_uidx;

DROP TABLE IF EXISTS blnk.event_subscribers;
