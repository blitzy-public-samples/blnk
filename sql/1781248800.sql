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

-- The event outbox: the durable hand-off between a ledger mutation and the Kafka event
-- announcing it.
--
-- For almost every event, a row is inserted INSIDE THE SAME DATABASE TRANSACTION as the
-- mutation that produced it, immediately before the commit. The mutation and its event
-- therefore commit or roll back together and can never disagree: there is no window in
-- which a balance moved but the event was lost, and none in which an event describes
-- work that was rolled back.
--
-- THREE EVENT CLASSES ARE CAPTURED AFTER THEIR MUTATION COMMITS, and this table cannot
-- tell you which rows those were, so it is stated here: balance.monitor,
-- bulk_transaction.<status>, and the transaction.* events of a COALESCED write. Each
-- has a structural reason — no transaction remains open to enrol them in — and each is
-- retried on a transient failure, but a process death between the commit and the insert
-- loses the event and NO ROW IS EVER WRITTEN for it.
--
-- What that buys for every other event is exactly-once ON THE WRITE SIDE ONLY, and the
-- distinction matters enough to state here rather than leave to documentation. Each
-- such event is recorded exactly once — that is what the unique index on event_id
-- enforces — but Kafka delivery downstream remains AT-LEAST-ONCE for all of them.
CREATE TABLE IF NOT EXISTS blnk.event_outbox (
    -- Surrogate key, and not decoration. The in-transaction insert ends
    -- RETURNING id, mirroring InsertLineageOutboxInTx, and the claim query
    -- selects ids in its inner FOR UPDATE SKIP LOCKED subquery and updates by
    -- them.
    id                  BIGSERIAL                 PRIMARY KEY,

    -- ===================================================================
    -- Group 1: the event envelope
    -- These columns ARE the LedgerEvent that goes on the wire, plus the routing
    -- information the relay needs to send it. They map one-to-one onto
    -- model.EventOutbox, whose JSON tags are these column names.
    -- ===================================================================

    -- The event UUID, and simultaneously the subscriber idempotency key.
    -- TEXT rather than the uuid type: every business key in this schema is TEXT
    -- (ledgers.ledger_id, identity.identity_id, balances.balance_id,
    -- api_keys.api_key_id, lineage_outbox.transaction_id), and this value is
    -- carried verbatim as a JSON string on the wire, never compared as a uuid.
    event_id            TEXT                      NOT NULL,

    -- The event name, e.g. 'transaction.applied' or 'balance.monitor'. It duplicates
    -- the event name already inside payload, hoisted to the top level so the relay and
    -- any SQL-side triage can route and filter without parsing the payload.
    event_type          TEXT                      NOT NULL,

    -- The aggregate the event belongs to — the transaction, balance, identity or
    -- ledger the mutation acted on. This is what a consumer groups by, and what
    -- the per-aggregate ordering verification queries on.
    aggregate_id        TEXT                      NOT NULL,

    -- The FALLBACK message key: what the event is keyed on when it belongs to no ledger.
    --
    -- NEITHER THIS COLUMN NOR ledger_id IS THE KEY ON ITS OWN. The Kafka message key, and
    -- the expression the relay serialises publishes on, are both the EFFECTIVE key:
    --
    --     COALESCE(NULLIF(btrim(ledger_id), ''), btrim(partition_key))
    --
    -- Three code sites resolve it by that one rule — model.EventOutbox.EffectiveKey, the
    -- publisher's message key, and the claim query's per-key anti-join — and
    -- sql/1781252000.sql creates the expression index that makes the anti-join cheap. A
    -- change to the keying rule therefore belongs in all four places, not in this column.
    --
    -- It remains its own column because it is never NULL, so every row has a key even when
    -- it has no ledger: this one answers "where does this message go if no ledger says",
    -- while ledger_id answers "which ledger is this about".
    partition_key       TEXT                      NOT NULL,

    -- The AUTHORITATIVE ledger this event belongs to, or NULL when the event genuinely
    -- has no ledger. It is the FIRST term of the effective key above, so for the eleven
    -- of thirteen event types that belong to a ledger it IS what the event is keyed and
    -- serialised on; partition_key only decides the key for the ones that do not.
    --
    -- NULLABLE, deliberately, and this is the point of the split: NULL states "this
    -- event has no ledger" as a fact. The previous NOT NULL column with '' as its
    -- no-ledger value could not distinguish that from "nobody looked".
    ledger_id           TEXT                      NULL,

    -- The fully-resolved destination topic, recorded at insert time so the relay never
    -- re-derives it and so a stored row stays replayable to its ORIGINAL destination
    -- even if the topic-naming configuration changes later.
    topic               TEXT                      NOT NULL,

    -- The envelope version, starting at 1 to match the SchemaVersionV1 constant.
    -- A subscriber branches on this rather than guessing the envelope's shape.
    schema_version      INT                       NOT NULL DEFAULT 1,

    -- The event body, held TWICE and deliberately so. Read both comments before
    -- touching either column: which one a reader picks decides whether the
    -- payload-preservation guarantee holds.
    payload             JSONB                     NOT NULL,

    -- THE PAYLOAD-PRESERVATION GUARANTEE LIVES IN THIS COLUMN.
    --
    -- payload_raw holds the EXACT bytes json.Marshal produced for the NewWebhook value
    -- at the producer call site — the same bytes SendWebhook would have put on the wire
    -- as the HTTP body, with the producer's member order, spelling and number literals
    -- intact. BYTEA, not TEXT and not JSON, for two reasons: bytea stores an opaque
    -- byte string, so nothing about it can validate, reject, transcode or re-render the
    -- payload; and this INSERT runs inside the caller's ledger transaction, where a
    -- column that could reject its input would let a notification defect abort a
    -- financially valid mutation.
    --
    -- EVERY READER OF THE LEGACY WEBHOOK BODY TAKES IT FROM HERE, never from payload:
    -- the legacy webhook leg during the dual-delivery window reads these bytes, and so
    -- does the payload member spliced into the canonical envelope in event_raw below.
    -- That is what makes the dual-delivery guarantee structural rather than a matter of
    -- careful coding — during the window the Kafka message and the legacy webhook body
    -- carry the same payload because both come from this one column.
    --
    -- Reading it by eye: SELECT convert_from(payload_raw, 'UTF8') renders the stored
    -- body as text without touching what is stored.
    payload_raw         BYTEA                     NOT NULL,

    -- THE CANONICAL EVENT VALUE, and the column the replay guarantee lives in.
    --
    -- BYTEA for the same reasons payload_raw is: an opaque byte string cannot validate,
    -- reject, transcode or re-render what it was given, and this INSERT runs inside the
    -- caller's ledger transaction, where a column that could reject its input would let
    -- a notification defect abort a financially valid mutation.
    event_raw           BYTEA                     NOT NULL,

    -- ===================================================================
    -- WHAT THE THREE COPIES ABOVE COST
    -- ===================================================================
    --
    -- The three columns above hold the event body three times over, each for the
    -- reason stated at it. The price of holding it three times is stated here,
    -- because it is the number the retention period and the PostgreSQL volume must
    -- both be sized from.
    --
    -- Every figure below is ONE basis: bytes AS STORED, after TOAST compression, as
    -- reported by pg_column_size, plus on-disk relation sizes from pg_relation_size /
    -- pg_indexes_size / pg_total_relation_size. Logical (uncompressed) lengths are
    -- called out where they are quoted, and are never mixed with stored sizes.
    --
    --     CREATE TABLE blnk.event_outbox_sizing
    --         (LIKE blnk.event_outbox INCLUDING ALL);
    --     -- insert or copy a representative sample, then:
    --     ANALYZE blnk.event_outbox_sizing;
    --     SELECT round(avg(pg_column_size(payload)))      AS payload_jsonb,
    --            round(avg(pg_column_size(payload_raw)))  AS payload_raw,
    --            round(avg(pg_column_size(event_raw)))    AS event_raw,
    --            round(avg(pg_column_size(t)))            AS whole_tuple,
    --            round(avg(octet_length(event_raw)))      AS event_raw_logical
    --       FROM blnk.event_outbox_sizing t;
    --     SELECT round(pg_relation_size('blnk.event_outbox_sizing')::numeric
    --                  / count(*))                        AS heap_bytes_row,
    --            round((pg_total_relation_size('blnk.event_outbox_sizing')
    --                   - pg_relation_size('blnk.event_outbox_sizing')
    --                   - pg_indexes_size('blnk.event_outbox_sizing'))::numeric
    --                  / count(*))                        AS toast_bytes_row,
    --            round(pg_indexes_size('blnk.event_outbox_sizing')::numeric
    --                  / count(*))                        AS index_bytes_row,
    --            round(pg_total_relation_size('blnk.event_outbox_sizing')::numeric
    --                  / count(*))                        AS total_bytes_row
    --       FROM blnk.event_outbox_sizing;

    -- When the domain action happened; RFC3339 on the wire.
    --
    -- THIS, never created_at, is the ordering column throughout this table. The relay
    -- claims rows in ascending occurred_at order so that FIFO holds within a partition
    -- key, and every index below that carries an ordering column carries this one.
    occurred_at         TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 2: the relay state machine
    -- Progress from pending to a terminal state, plus the bounded-retry
    -- bookkeeping the retry schedule and the dead-letter record are built from.
    -- ===================================================================

    -- The row's durable state. The canonical vocabulary, fixed here because this DDL is
    -- what the repository layer's queries are written against:
    --
    --   pending ──claimed──▶ processing ──broker ack──▶ dispatched
    --      │                     │        │            (dispatched_at set)
    --      │                     │        └──legacy leg still owed──▶ webhook_pending
    --      │                     │                                        │
    --      │                     │            (re-claimed; Kafka NOT republished)
    --      └──budget exhausted───┴──▶ failed ──written to <topic>.dlt──▶ dead_lettered
    status              TEXT                      NOT NULL DEFAULT 'pending',

    -- Publish attempts made so far, and the budget for them. The default of 5
    -- matches the default of RELAY_MAX_RETRY_ATTEMPTS, so a row inserted without
    -- an explicit budget still gets the configured retry behaviour. The budget
    -- is per-row rather than global so an operator can extend it for a single
    -- stuck event without reconfiguring the relay.
    attempts            INT                       NOT NULL DEFAULT 0,
    max_attempts        INT                       NOT NULL DEFAULT 5,

    -- The instant this row is next DUE for a publish attempt, which is what makes the
    -- configured exponential backoff real rather than nominal: the delay survives a
    -- relay restart and is honoured by every instance, because it is persisted here
    -- rather than held in memory or slept through in-process. The claim predicate is
    -- next_attempt_at <= NOW(), so a row that is not yet due is simply not claimed.
    next_attempt_at     TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- The most recent publish failure reason, kept for operator triage and
    -- copied into the dead-letter failure metadata's error_reason field.
    last_error          TEXT                      NULL,

    -- The attempt window. These two are load-bearing rather than diagnostic:
    -- they become two of the five fields of the failure metadata attached to a
    -- dead-lettered event, and together they distinguish a momentary broker blip
    -- from a sustained outage. Nullable because a row that has never been
    -- attempted has neither.
    first_attempted_at  TIMESTAMP WITH TIME ZONE  NULL,
    last_attempted_at   TIMESTAMP WITH TIME ZONE  NULL,

    -- When the broker acknowledged the publish. Nullable, and set together with
    -- the dispatched status.
    dispatched_at       TIMESTAMP WITH TIME ZONE  NULL,

    -- The lease held by the relay instance that claimed this row (30 seconds).
    -- Concurrent relay instances skip locked rows, and an EXPIRED lease makes a row
    -- claimable again, which is what lets a crashed relay's in-flight work be picked up
    -- rather than stranded.
    locked_until        TIMESTAMP WITH TIME ZONE  NULL,

    -- The token identifying the CLAIM the row is currently under, and what makes every
    -- state transition safe against a worker whose lease expired.
    --
    -- NULL on a freshly inserted row and set back to NULL at a terminal state, so a
    -- non-NULL value means "some worker holds this row right now".
    claim_token         TEXT                      NULL,

    -- ===================================================================
    -- Group 3: the dual-delivery marker
    -- ===================================================================

    -- Records that the legacy HTTP webhook leg was dispatched for this row.
    --
    -- During the 30-day dual-delivery window the relay publishes to Kafka AND enqueues
    -- the legacy webhook task from the SAME claimed row. This flag makes that second
    -- leg individually idempotent, so a row republished to Kafka after a relay restart
    -- does not also re-enqueue a duplicate webhook.
    webhook_dispatched  BOOLEAN                   NOT NULL DEFAULT FALSE,

    -- When the BROKER acknowledged the Kafka publish, recorded independently of
    -- dispatched_at — and the column that makes the two legs' fates independent.
    --
    -- Set on the ordinary success path too, not only in the webhook_pending case, so
    -- the column answers "was this event published to Kafka, and when" for every row
    -- rather than only for the ones that took the unusual path.
    kafka_dispatched_at TIMESTAMP WITH TIME ZONE  NULL,

    -- Legacy webhook ENQUEUE attempts made so far, counted separately from attempts.
    --
    -- Separate because the two legs must not spend each other's budget. A webhook
    -- receiver being down, or the queue being unreachable, must never consume a Kafka
    -- retry attempt — that would let the deprecated transport dead-letter events on the
    -- new one, which is precisely backwards.
    webhook_attempts    INT                       NOT NULL DEFAULT 0,

    -- ===================================================================
    -- Group 3b: the broker coordinate
    -- WHERE this row's record actually landed. All three are nullable and are
    -- written and cleared TOGETHER, which the all-or-nothing check below enforces.
    -- ===================================================================

    -- The topic the record is on. Recorded explicitly rather than inferred from
    -- status, because the destination differs by outcome: a dispatched row's record
    -- is on `topic` and a dead-lettered row's is on `dlt_topic`. Storing the name
    -- makes the coordinate self-describing, so an operator can paste it straight
    -- into a console consumer.
    kafka_topic         TEXT                      NULL,

    -- The partition the broker assigned. For a keyed message this is determined by
    -- partition_key, so it is stable across redeliveries of the same event.
    kafka_partition     INT                       NULL,

    -- The record's offset within that partition. The coordinate is stored because the
    -- zero-loss reconciliation compares this table against the broker, and a stored
    -- topic/partition/offset triple makes a row's record locatable without a scan.
    kafka_offset        BIGINT                    NULL,

    -- ===================================================================
    -- Group 4: the dead-letter record
    -- Both nullable: the overwhelming majority of rows never dead-letter, and
    -- paying for these on the hot path would be wrong.
    -- ===================================================================

    -- The dead-letter topic the event was written to once its retry budget was
    -- spent — the '<topic>.dlt' sibling of topic.
    dlt_topic           TEXT                      NULL,

    -- The marshaled failure metadata attached to the dead-lettered event, carrying
    -- exactly five fields: the original topic, the error reason, the attempt count, and
    -- the first- and last-attempted instants. Stored as raw JSON so the dead-letter API
    -- hands back the bytes as they were written, and attached to the dead-letter
    -- message as a sibling top-level key so the original envelope is left untouched and
    -- stays replayable.
    failure_metadata    JSONB                     NULL,

    -- The physical row-insert time, part of the lineage vocabulary and kept for
    -- operational forensics.
    created_at          TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 5: the invariants the database itself enforces
    -- Each is written to be satisfied by every row the current code inserts, so the
    -- migration applies to an existing table without a data fix.
    -- ===================================================================

    -- A blank partition key would let Kafka scatter the event round-robin, so
    -- per-aggregate ordering would be silently lost for that event with nothing in
    -- the row to show it. btrim rather than <> '' because a whitespace-only key is
    -- just as unusable and hashes to a different partition than the empty string.
    CONSTRAINT event_outbox_partition_key_not_blank
        CHECK (btrim(partition_key) <> ''),

    -- A blank event_id would insert a row whose idempotency key is the empty
    -- string. The first such row succeeds and every later one collides on the
    -- unique index, so the failure surfaces far from its cause.
    CONSTRAINT event_outbox_event_id_not_blank
        CHECK (btrim(event_id) <> ''),

    -- A blank event_type cannot be routed and cannot be filtered on by a
    -- subscriber, and a blank topic cannot be published to at all — Kafka rejects
    -- an empty topic, so the row would be retried until its budget was spent and
    -- then dead-lettered for a reason that was decided at insert time.
    CONSTRAINT event_outbox_event_type_not_blank
        CHECK (btrim(event_type) <> ''),
    CONSTRAINT event_outbox_topic_not_blank
        CHECK (btrim(topic) <> ''),

    -- aggregate_id is what a consumer groups by and what the ordering verification
    -- queries on; blank makes both meaningless.
    CONSTRAINT event_outbox_aggregate_id_not_blank
        CHECK (btrim(aggregate_id) <> ''),

    -- ledger_id is nullable and means "no ledger" when NULL. The empty string
    -- would be a second spelling of the same thing, and two spellings of one
    -- meaning is how a query that filters on one of them silently misses rows.
    CONSTRAINT event_outbox_ledger_id_not_blank_when_present
        CHECK (ledger_id IS NULL OR btrim(ledger_id) <> ''),

    -- A zero or negative schema version reaches subscribers on the wire, where it
    -- reads as an unknown envelope shape.
    CONSTRAINT event_outbox_schema_version_positive
        CHECK (schema_version >= 1),

    -- A non-positive retry budget means the row is dead-lettered without ever
    -- being attempted, and a negative attempt count makes the exhaustion
    -- comparison in the failure transition meaningless.
    CONSTRAINT event_outbox_attempts_non_negative
        CHECK (attempts >= 0),
    CONSTRAINT event_outbox_max_attempts_positive
        CHECK (max_attempts >= 1),
    -- The legacy leg's own counter is bounded by the same per-row budget, so a
    -- permanently unreachable queue cannot keep a row claimable forever.
    CONSTRAINT event_outbox_webhook_attempts_non_negative
        CHECK (webhook_attempts >= 0),

    -- The broker coordinate is all-or-nothing. A half-written coordinate is worse
    -- than none: a reader testing only the offset would treat a row with no topic as
    -- confirmed and then have nothing to look the record up with, so the audit's
    -- confirmed count would include rows it cannot actually match.
    CONSTRAINT event_outbox_broker_record_complete
        CHECK (
            (kafka_topic IS NULL AND kafka_partition IS NULL AND kafka_offset IS NULL)
            OR (kafka_topic IS NOT NULL AND kafka_partition IS NOT NULL AND kafka_offset IS NOT NULL)
        ),
    -- Kafka numbers partitions from zero and offsets from zero, and both are
    -- assigned by the broker, so a negative value can only come from a bug or from
    -- one of kafka-go's own sentinels (-1 means "no offset") reaching a write path
    -- that should have treated it as absent.
    CONSTRAINT event_outbox_kafka_partition_non_negative
        CHECK (kafka_partition IS NULL OR kafka_partition >= 0),
    CONSTRAINT event_outbox_kafka_offset_non_negative
        CHECK (kafka_offset IS NULL OR kafka_offset >= 0),
    CONSTRAINT event_outbox_kafka_topic_not_blank_when_present
        CHECK (kafka_topic IS NULL OR length(btrim(kafka_topic)) > 0),

    -- The status column drives the claim predicate, the partial indexes and every
    -- transition, so an unrecognised literal is not a cosmetic problem: a row in a
    -- state nothing selects for is a permanently invisible event. The list is the
    -- model.EventOutboxStatus* vocabulary, in state-machine order.
    CONSTRAINT event_outbox_status_known
        CHECK (status IN ('pending', 'processing', 'webhook_pending', 'dispatched', 'failed',
                          'dead_lettered', 'replaying'))
);

-- The write-side exactly-once guard, load-bearing in two distinct ways.
--
-- First, it makes "one event is recorded once" an invariant the schema enforces rather
-- than a property the code intends: a retried or duplicated insert is rejected by the
-- database, inside the caller's transaction.
CREATE UNIQUE INDEX IF NOT EXISTS event_outbox_event_id_uidx
    ON blnk.event_outbox (event_id);

-- The relay's steady-state poll: the pending backlog, in occurrence order.
-- Partial, so it holds only rows still awaiting a first dispatch — a few seconds
-- of rows in steady state — and the poll therefore does not degrade as the table
-- grows. Keyed on occurred_at, never created_at, because occurrence time is what
-- the relay orders by.
CREATE INDEX IF NOT EXISTS idx_event_outbox_pending
    ON blnk.event_outbox (status, occurred_at)
    WHERE status = 'pending';

-- The dead-letter inventory behind GET /events/dead-letter, and the operator's view of
-- stalled events.
CREATE INDEX IF NOT EXISTS idx_event_outbox_failed
    ON blnk.event_outbox (status, occurred_at)
    WHERE status IN ('failed', 'dead_lettered');

-- The index the FIFO claim query drives, and the reason relay polling stays viable at
-- 500 events per second. The query, run once per poll interval:
--
--   WITH claimed AS (
--       UPDATE blnk.event_outbox
--       SET status = $1, locked_until = NOW() + $2::interval
--       WHERE id IN (
--           SELECT id FROM blnk.event_outbox
--           WHERE status IN ('pending', 'processing')
--             AND (locked_until IS NULL OR locked_until < NOW())
--             AND attempts < max_attempts
--             AND next_attempt_at <= NOW()
--           ORDER BY occurred_at ASC
--           LIMIT $3
--           FOR UPDATE SKIP LOCKED
--       )
--       RETURNING <all columns>
--   )
--   SELECT * FROM claimed ORDER BY occurred_at ASC
CREATE INDEX IF NOT EXISTS idx_event_outbox_claim
    ON blnk.event_outbox (status, locked_until, attempts, next_attempt_at, occurred_at)
    WHERE status IN ('pending', 'processing', 'webhook_pending');

-- Per-aggregate history in occurrence order. This is what makes the ordering
-- guarantee auditable after the fact: given an aggregate, replay its events in
-- the order they were published and compare against what a consumer saw. Not
-- partial — the audit is most useful precisely on rows that have already reached
-- a terminal state.
CREATE INDEX IF NOT EXISTS idx_event_outbox_aggregate
    ON blnk.event_outbox (aggregate_id, occurred_at);

-- The index for the claim's per-key predicate AS THIS MIGRATION LEFT IT.
--
-- SUPERSEDED BY sql/1781252000.sql, which drops this index and creates
-- idx_event_outbox_effective_key_inflight on the effective key expression instead —
-- COALESCE(NULLIF(btrim(ledger_id), ''), btrim(partition_key)) — because that is the
-- expression the claim's anti-join actually compares. It is declared here because this is
-- the migration that created it and a rollback of that one restores it; on a current
-- schema it does not exist, so do not tune the claim against it.
CREATE INDEX IF NOT EXISTS idx_event_outbox_partition_key_inflight
    ON blnk.event_outbox (partition_key, occurred_at, id)
    WHERE status IN ('pending', 'processing');

-- The retention index, and the retention contract it serves.
--
-- The retention primitive is Datasource.PurgeTerminalEventsBefore, which deletes rows
-- in a TERMINAL state whose occurrence is older than a cutoff. This index is what lets
-- it delete a bounded slice cheaply instead of scanning the table.
CREATE INDEX IF NOT EXISTS idx_event_outbox_terminal_retention
    ON blnk.event_outbox (occurred_at)
    WHERE status IN ('dispatched', 'dead_lettered');

-- The broker coordinate is UNIQUE, and that is a correctness guarantee rather than a
-- performance one.
CREATE UNIQUE INDEX IF NOT EXISTS event_outbox_broker_record_uidx
    ON blnk.event_outbox (kafka_topic, kafka_partition, kafka_offset)
    WHERE kafka_offset IS NOT NULL;

-- The outbox side of the daily zero-loss reconciliation, which counts rows claiming a
-- Kafka record and how many of those name one.
--
-- The predicate is deliberately WIDER than the terminal statuses. A webhook_pending row
-- HAS been published to Kafka — its Kafka leg completed and kafka_dispatched_at is
-- stamped; what is outstanding is the deprecated HTTP leg.
CREATE INDEX IF NOT EXISTS idx_event_outbox_published_audit
    ON blnk.event_outbox (kafka_dispatched_at)
    INCLUDE (kafka_topic, kafka_partition, kafka_offset)
    WHERE kafka_dispatched_at IS NOT NULL OR status = 'dead_lettered';

-- +migrate Down

-- Indexes first, then the table. Dropping the table would take its indexes with it, but
-- naming them explicitly keeps the rollback correct if a future migration ever detaches
-- one of these objects from the other, and it documents exactly what this migration
-- created. Index names are schema-qualified in the drop even though they are
-- unqualified in the create; that asymmetry is the house convention.
DROP INDEX IF EXISTS blnk.idx_event_outbox_published_audit;
DROP INDEX IF EXISTS blnk.event_outbox_broker_record_uidx;
DROP INDEX IF EXISTS blnk.idx_event_outbox_terminal_retention;
DROP INDEX IF EXISTS blnk.idx_event_outbox_partition_key_inflight;
DROP INDEX IF EXISTS blnk.idx_event_outbox_aggregate;
DROP INDEX IF EXISTS blnk.idx_event_outbox_claim;
DROP INDEX IF EXISTS blnk.idx_event_outbox_failed;
DROP INDEX IF EXISTS blnk.idx_event_outbox_pending;
DROP INDEX IF EXISTS blnk.event_outbox_event_id_uidx;

DROP TABLE IF EXISTS blnk.event_outbox;
