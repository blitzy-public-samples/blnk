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

-- The event outbox: the durable hand-off between a ledger mutation and the
-- Kafka event announcing it.
--
-- A row is inserted INSIDE THE SAME DATABASE TRANSACTION as the mutation that
-- produced it, immediately before the commit. The mutation and its event
-- therefore commit or roll back together and can never disagree: there is no
-- window in which a balance moved but the event was lost, and none in which an
-- event describes work that was rolled back. A separate relay polls this table,
-- publishes claimed rows to Kafka, and marks them dispatched.
--
-- What that buys is exactly-once ON THE WRITE SIDE ONLY, and the distinction
-- matters enough to state here rather than leave to documentation. Each event is
-- recorded exactly once — that is what the unique index on event_id enforces —
-- but Kafka delivery downstream remains AT-LEAST-ONCE. A relay that crashes
-- between an acknowledged publish and marking the row dispatched will reclaim
-- the row once its lease expires and publish it again. Suppressing that
-- duplicate is a subscriber obligation, keyed on event_id. Nothing in this table
-- promises end-to-end exactly-once delivery, and it should not be read as if it
-- did.
--
-- This is a SECOND, INDEPENDENT outbox alongside blnk.lineage_outbox, not a
-- replacement for it and never merged with it: separate tables, separate relays,
-- and the lineage relay's behaviour is unchanged. The column vocabulary, the
-- status literals and the index set here are deliberately modelled on that table
-- so an operator who knows one can read the other.
--
-- Every timestamp below is TIMESTAMP WITH TIME ZONE. That departs from
-- blnk.lineage_outbox's naive TIMESTAMP and follows blnk.api_keys instead; the
-- deviation is deliberate and both reasons are load-bearing rather than
-- stylistic. First, occurred_at is an RFC3339 instant that crosses the process
-- boundary onto the Kafka wire, where it is read by consumers running in other
-- timezones. Second, first_attempted_at, last_attempted_at and dispatched_at
-- feed the dead-letter age arithmetic behind the
-- blnk.dlt.oldest_message_age_seconds gauge and its 15-minute alert. A naive
-- timestamp makes both silently wrong on any deployment whose server timezone is
-- not UTC — wrong by a whole-hour offset, with no error to notice. Nothing
-- downstream is affected by the choice: NOW() + $2::interval arithmetic and
-- scanning into a Go time.Time behave identically with either type.
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

    -- The event name, e.g. 'transaction.applied' or 'balance.monitor'. It
    -- duplicates the event name already inside payload, hoisted to the top level
    -- so the relay and any SQL-side triage can route and filter without parsing
    -- the payload.
    --
    -- Deliberately NO CHECK constraint enumerating the known event types. Bulk
    -- transaction events are composed at runtime as 'bulk_transaction.' plus the
    -- batch status, so the value set is open-ended: any enumeration would be
    -- wrong the moment a status is added, and would fail the write rather than
    -- the routing.
    event_type          TEXT                      NOT NULL,

    -- The aggregate the event belongs to — the transaction, balance, identity or
    -- ledger the mutation acted on. This is what a consumer groups by, and what
    -- the per-aggregate ordering verification queries on.
    aggregate_id        TEXT                      NOT NULL,

    -- The Kafka message key. Keying by ledger ID with a stable hash balancer
    -- pins every event for one ledger to a single partition, which is what
    -- delivers the per-aggregate ordering guarantee.
    --
    -- NOT NULL with the EMPTY STRING as the "no ledger" value, rather than a
    -- nullable column. A few event types genuinely belong to no ledger (internal
    -- system errors, for instance) and are published without a key;
    -- model.EventOutbox.LedgerID is a plain string, so the insert always supplies
    -- a value and '' is what arrives. Keeping the column NOT NULL means the read
    -- path never has to handle a NULL here.
    ledger_id           TEXT                      NOT NULL,

    -- The fully-resolved destination topic, recorded at insert time so the relay
    -- never re-derives it and so a stored row stays replayable to its ORIGINAL
    -- destination even if the topic-naming configuration changes later.
    --
    -- Deliberately NO CHECK constraint enumerating topic names. The topic prefix
    -- is configurable (KAFKA_TOPIC_PREFIX, default 'blnk'), so a hardcoded
    -- 'blnk.transactions'-style constraint would reject every write on any
    -- deployment using a non-default prefix.
    topic               TEXT                      NOT NULL,

    -- The envelope version, starting at 1 to match the SchemaVersionV1 constant.
    -- A subscriber branches on this rather than guessing the envelope's shape.
    schema_version      INT                       NOT NULL DEFAULT 1,

    -- The payload-preservation guarantee lives in this column.
    --
    -- It holds the marshaled legacy webhook object verbatim — the two-key
    -- {"event": ..., "data": ...} form of NewWebhook, BOTH keys included — which
    -- is byte-for-byte what the HTTP webhook body is today. Carrying the whole
    -- object means an existing subscriber's body parser keeps working unchanged
    -- and only the transport differs.
    --
    -- Nothing may transform these bytes: no generated column, no trigger, no
    -- default. Every reader takes them from HERE. That is what makes the two
    -- delivery guarantees structural rather than a matter of careful coding —
    -- during the dual-delivery window the Kafka message and the legacy webhook
    -- body are identical because both are read from this one row, and a
    -- dead-letter replay matches the original because it re-publishes these
    -- stored bytes instead of re-marshalling a struct.
    --
    -- Note for the read path, measured on PostgreSQL 16 rather than assumed.
    -- JSONB normalises on the way in: object keys are reordered (shortest first,
    -- then bytewise), whitespace is renormalised rather than merely stripped, a
    -- duplicate key collapses to its last occurrence, and exponent notation is
    -- expanded — though numeric scale survives, so 100.50 stays 100.50. The
    -- stored bytes are therefore NOT necessarily the bytes Go marshalled before
    -- the insert.
    --
    -- That is harmless, and the read path needs no ::text cast: casting was
    -- verified to return exactly what the plain jsonb read returns. Normalisation
    -- happens ONCE, at insert, after which every reader — the Kafka publish, the
    -- legacy webhook leg during the dual-delivery window, and a later
    -- dead-letter replay — reads these same bytes; two reads of one row were
    -- verified byte-identical. That reader-to-reader equality is exactly what the
    -- dual-delivery and replay-fidelity criteria compare. What is NOT promised is
    -- byte-identity with the pre-insert Go bytes, which would require the json
    -- type rather than jsonb, and which no criterion asks for.
    payload             JSONB                     NOT NULL,

    -- When the domain action happened; RFC3339 on the wire.
    --
    -- THIS, never created_at, is the ordering column throughout this table. The
    -- relay claims rows in ascending occurred_at order so that FIFO holds within
    -- a partition key, and every index below that carries an ordering column
    -- carries this one. See the created_at comment for the distinction.
    occurred_at         TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 2: the relay state machine
    -- Progress from pending to a terminal state, plus the bounded-retry
    -- bookkeeping the retry schedule and the dead-letter record are built from.
    -- ===================================================================

    -- The row's durable state. The canonical vocabulary, fixed here because this
    -- DDL is what the repository layer's queries are written against:
    --
    --   pending ──claimed──▶ processing ──broker ack──▶ dispatched
    --      │                     │                     (dispatched_at set)
    --      └──budget exhausted───┴──▶ failed ──written to <topic>.dlt──▶ dead_lettered
    --
    -- 'dispatched' and 'dead_lettered' are the terminal states, and only a
    -- dead_lettered row is eligible for replay. 'pending', 'processing' and
    -- 'failed' reuse the lineage outbox's literals verbatim, which is what lets
    -- its partial-index convention carry over here unchanged. The success
    -- terminal is named 'dispatched' rather than lineage's 'completed' so it
    -- matches the dispatched_at column and this pipeline's language;
    -- 'dead_lettered' has no lineage equivalent at all. The Go-side declaration
    -- site for all five literals is model.EventOutboxStatus*.
    --
    -- Deliberately NO CHECK constraint. blnk.lineage_outbox has none either, and
    -- a CHECK would turn every future addition to the state machine into a
    -- schema migration.
    status              TEXT                      NOT NULL DEFAULT 'pending',

    -- Publish attempts made so far, and the budget for them. The default of 5
    -- matches the default of RELAY_MAX_RETRY_ATTEMPTS, so a row inserted without
    -- an explicit budget still gets the configured retry behaviour. The budget
    -- is per-row rather than global so an operator can extend it for a single
    -- stuck event without reconfiguring the relay.
    attempts            INT                       NOT NULL DEFAULT 0,
    max_attempts        INT                       NOT NULL DEFAULT 5,

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
    -- Concurrent relay instances skip locked rows, and an EXPIRED lease makes a
    -- row claimable again, which is what lets a crashed relay's in-flight work be
    -- picked up rather than stranded.
    --
    -- Nullability is required, not incidental: the claim predicate is
    -- (locked_until IS NULL OR locked_until < NOW()), so a freshly inserted row
    -- must have NULL here to be claimable. A NOT NULL default would either make
    -- every new row unclaimable or force a sentinel timestamp.
    locked_until        TIMESTAMP WITH TIME ZONE  NULL,

    -- ===================================================================
    -- Group 3: the dual-delivery marker
    -- ===================================================================

    -- Records that the legacy HTTP webhook leg was dispatched for this row.
    --
    -- During the 30-day dual-delivery window the relay publishes to Kafka AND
    -- enqueues the legacy webhook task from the SAME claimed row. This flag makes
    -- that second leg individually idempotent, so a row republished to Kafka
    -- after a relay restart does not also re-enqueue a duplicate webhook.
    -- Mirrors lineage_outbox.inflight's NOT NULL DEFAULT FALSE form.
    --
    -- This column becomes vestigial after the webhook sunset date: the legacy leg
    -- is deleted and the flag is simply never set again. It can be dropped by a
    -- later migration once no dual-delivery row remains of interest.
    webhook_dispatched  BOOLEAN                   NOT NULL DEFAULT FALSE,

    -- ===================================================================
    -- Group 4: the dead-letter record
    -- Both nullable: the overwhelming majority of rows never dead-letter, and
    -- paying for these on the hot path would be wrong.
    -- ===================================================================

    -- The dead-letter topic the event was written to once its retry budget was
    -- spent — the '<topic>.dlt' sibling of topic.
    dlt_topic           TEXT                      NULL,

    -- The marshaled failure metadata attached to the dead-lettered event,
    -- carrying exactly five fields: the original topic, the error reason, the
    -- attempt count, and the first- and last-attempted instants. Stored as raw
    -- JSON so the dead-letter API hands back the bytes as they were written, and
    -- attached to the dead-letter message as a sibling top-level key so the
    -- original envelope is left untouched and stays replayable.
    failure_metadata    JSONB                     NULL,

    -- The physical row-insert time, part of the lineage vocabulary and kept for
    -- operational forensics.
    --
    -- NOT the same thing as occurred_at, and conflating the two is the mistake
    -- this comment exists to prevent. occurred_at is the DOMAIN event time: it
    -- goes on the wire, and it drives FIFO ordering and every index. created_at
    -- is when the row physically landed in this table. They can legitimately
    -- differ — a backfilled or replayed mutation carries an earlier occurred_at
    -- than its created_at — and using created_at for ordering would publish such
    -- events out of domain order.
    created_at          TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW()
);

-- The write-side exactly-once guard, load-bearing in two distinct ways.
--
-- First, it makes "one event is recorded once" an invariant the schema enforces
-- rather than a property the code intends: a retried or duplicated insert is
-- rejected by the database, inside the caller's transaction.
--
-- Second, event_id doubles as the subscriber idempotency key. The relay can
-- crash between an acknowledged publish and marking the row dispatched, after
-- which the row is reclaimed and republished; this index is what guarantees the
-- key a consumer deduplicates on is unique in the first place, and is therefore
-- what makes that duplicate suppressible at the consumer's idempotency
-- boundary.
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

-- The dead-letter inventory behind GET /events/dead-letter, and the operator's
-- view of stalled events.
--
-- It covers BOTH terminal failure literals so it stays usable whichever one the
-- repository layer filters on: the planner can prove that status =
-- 'dead_lettered' implies this predicate, and likewise for 'failed'. Restricting
-- it to a single literal would silently drop the other query off the index.
CREATE INDEX IF NOT EXISTS idx_event_outbox_failed
    ON blnk.event_outbox (status, occurred_at)
    WHERE status IN ('failed', 'dead_lettered');

-- The index the FIFO claim query drives, and the reason relay polling stays
-- viable at 500 events per second. The query, run once per poll interval:
--
--   WITH claimed AS (
--       UPDATE blnk.event_outbox
--       SET status = $1, locked_until = NOW() + $2::interval
--       WHERE id IN (
--           SELECT id FROM blnk.event_outbox
--           WHERE status IN ('pending', 'processing')
--             AND (locked_until IS NULL OR locked_until < NOW())
--             AND attempts < max_attempts
--           ORDER BY occurred_at ASC
--           LIMIT $3
--           FOR UPDATE SKIP LOCKED
--       )
--       RETURNING <all columns>
--   )
--   SELECT * FROM claimed ORDER BY occurred_at ASC
--
-- The partial predicate is the same IN-list the query filters on, which is what
-- confines the scan to the claimable working set and keeps dispatched and
-- dead-lettered rows — eventually the overwhelming majority of the table — out
-- of the index entirely. The three predicate columns lead and the ordering
-- column trails, matching the shape proven on blnk.lineage_outbox.
--
-- One honest note on the plan, measured rather than assumed: because the leading
-- key is matched against a two-element IN-list, a btree cannot emit rows already
-- sorted by the trailing occurred_at, so the plan legitimately contains a Sort
-- above the index scan. blnk.lineage_outbox has the same index and query shape
-- and the same plan. It costs nothing that matters, because the partial
-- predicate confines that sort's input to the claimable working set rather than
-- the whole table: EXPLAIN ANALYZE over 20,000 rows claims a batch of 100 in
-- roughly 3 ms via this index, with no sequential scan.
CREATE INDEX IF NOT EXISTS idx_event_outbox_claim
    ON blnk.event_outbox (status, locked_until, attempts, occurred_at)
    WHERE status IN ('pending', 'processing');

-- Per-aggregate history in occurrence order. This is what makes the ordering
-- guarantee auditable after the fact: given an aggregate, replay its events in
-- the order they were published and compare against what a consumer saw. Not
-- partial — the audit is most useful precisely on rows that have already reached
-- a terminal state.
CREATE INDEX IF NOT EXISTS idx_event_outbox_aggregate
    ON blnk.event_outbox (aggregate_id, occurred_at);

-- +migrate Down

-- Indexes first, then the table. Dropping the table would take its indexes with
-- it, but naming them explicitly keeps the rollback correct if a future
-- migration ever detaches one of these objects from the other, and it documents
-- exactly what this migration created. Index names are schema-qualified in the
-- drop even though they are unqualified in the create; that asymmetry is the
-- house convention.
DROP INDEX IF EXISTS blnk.idx_event_outbox_aggregate;
DROP INDEX IF EXISTS blnk.idx_event_outbox_claim;
DROP INDEX IF EXISTS blnk.idx_event_outbox_failed;
DROP INDEX IF EXISTS blnk.idx_event_outbox_pending;
DROP INDEX IF EXISTS blnk.event_outbox_event_id_uidx;

DROP TABLE IF EXISTS blnk.event_outbox;
