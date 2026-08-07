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

    -- The Kafka message key, and the column the relay serialises on.
    --
    -- Keying with a stable hash balancer pins every event sharing a key to a
    -- single partition, which is what delivers the per-aggregate ordering
    -- guarantee. NOT NULL AND NON-BLANK, enforced by the CHECK below: an empty
    -- key would let Kafka scatter the event round-robin and destroy that ordering
    -- with nothing in the data to show it, so "no key" is not a representable
    -- state. The construction path guarantees a value through a documented
    -- fallback chain ending in a sentinel.
    --
    -- THIS IS NOT THE LEDGER ID, and the two used to be one column. That column
    -- was called ledger_id and held, depending on the event, a ledger ID, a source
    -- or destination balance ID, an identity ID, a monitor ID, a batch ID or the
    -- event type — so the name was wrong for most of its values, and any
    -- subscriber-facing claim that a key prefix identifies a ledger was unfounded.
    -- Splitting them means each column means one thing.
    partition_key       TEXT                      NOT NULL,

    -- The AUTHORITATIVE ledger this event belongs to, or NULL when the event
    -- genuinely has no ledger. It takes no part in partitioning or in ordering.
    --
    -- Populated only from a payload that actually carries a ledger identifier: a
    -- ledger, or a balance, which belongs to exactly one ledger. NULL for
    -- transactions (model.Transaction has no ledger field — a transaction's ledger
    -- association is indirect, through the balances it moves value between, and
    -- resolving it would cost a database read on the capture path), for balance
    -- monitors and identities (neither carries a ledger field), for bulk
    -- transaction batches (a runtime grouping, not a ledger object) and for
    -- system.error (no aggregate of any kind).
    --
    -- NULLABLE, deliberately, and this is the point of the split: NULL states
    -- "this event has no ledger" as a fact. The previous NOT NULL column with ''
    -- as its no-ledger value could not distinguish that from "nobody looked".
    ledger_id           TEXT                      NULL,

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

    -- The event body, held TWICE and deliberately so. Read both comments before
    -- touching either column: which one a reader picks decides whether the
    -- payload-preservation guarantee holds.
    --
    -- Both columns hold the marshaled legacy webhook object — the two-key
    -- {"event": ..., "data": ...} form of NewWebhook, BOTH keys included — which
    -- is byte-for-byte what the HTTP webhook body is today. Carrying the whole
    -- object means an existing subscriber's body parser keeps working unchanged
    -- and only the transport differs.
    --
    -- payload is the QUERYABLE projection. JSONB is a parsed representation, and
    -- that is precisely what makes it useful here: containment (@>), member
    -- extraction (->, ->>) and a future expression index all work against it, so
    -- an operator triaging a stuck event can ask questions of the body in SQL
    -- rather than exporting it first.
    --
    -- What JSONB CANNOT do is return the bytes it was given. Measured on
    -- PostgreSQL 16 rather than assumed: object keys are reordered (shortest
    -- first, then bytewise), whitespace is renormalised rather than merely
    -- stripped, a duplicate key collapses to its last occurrence, and exponent
    -- notation is expanded — though numeric scale survives, so 100.50 stays
    -- 100.50. A ::text cast does not recover the input either; it renders the
    -- parsed form. So JSONB alone cannot carry a byte contract, which is why the
    -- next column exists.
    payload             JSONB                     NOT NULL,

    -- THE PAYLOAD-PRESERVATION GUARANTEE LIVES IN THIS COLUMN.
    --
    -- payload_raw holds the EXACT bytes json.Marshal produced for the NewWebhook
    -- value at the producer call site — the same bytes SendWebhook would have put
    -- on the wire as the HTTP body, with the producer's member order, spelling and
    -- number literals intact. BYTEA, not TEXT and not JSON, for two reasons: bytea
    -- stores an opaque byte string, so nothing about it can validate, reject,
    -- transcode or re-render the payload; and this INSERT runs inside the caller's
    -- ledger transaction, where a column that could reject its input would let a
    -- notification defect abort a financially valid mutation.
    --
    -- EVERY READER OF THE EVENT BODY TAKES IT FROM HERE, never from payload: the
    -- Kafka publish, the legacy webhook leg during the dual-delivery window, the
    -- dead-letter write and a later dead-letter replay. That is what makes the two
    -- delivery guarantees structural rather than a matter of careful coding —
    -- during the window the Kafka message and the legacy webhook body are
    -- identical because both are read from this one column, and a dead-letter
    -- replay matches the original byte for byte because it re-publishes these
    -- stored bytes instead of re-marshalling a struct. Acceptance criteria V-8 and
    -- V-9 compare exactly these bytes.
    --
    -- The two columns cannot drift apart, and that is a property of the write path
    -- rather than a convention: eventOutboxInsertArgs binds both from ONE
    -- in-memory slice, and no statement anywhere UPDATEs either of them. Any
    -- future write must set both from the same slice. Nothing may transform these
    -- bytes: no generated column, no trigger, no default.
    --
    -- Reading it by eye: SELECT convert_from(payload_raw, 'UTF8') renders the
    -- stored body as text without touching what is stored.
    payload_raw         BYTEA                     NOT NULL,

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
    --      │                     │        │            (dispatched_at set)
    --      │                     │        └──legacy leg still owed──▶ webhook_pending
    --      │                     │                                        │
    --      │                     │            (re-claimed; Kafka NOT republished)
    --      └──budget exhausted───┴──▶ failed ──written to <topic>.dlt──▶ dead_lettered
    --
    -- 'dispatched' and 'dead_lettered' are the terminal states, and only a
    -- dead_lettered row is eligible for replay. 'pending', 'processing' and
    -- 'failed' reuse the lineage outbox's literals verbatim, which is what lets
    -- its partial-index convention carry over here unchanged. The success
    -- terminal is named 'dispatched' rather than lineage's 'completed' so it
    -- matches the dispatched_at column and this pipeline's language;
    -- 'dead_lettered' has no lineage equivalent at all. The Go-side declaration
    -- site for every literal is model.EventOutboxStatus*.
    --
    -- 'webhook_pending' is the DUAL-DELIVERY state and, like the two columns in
    -- group 3, it disappears at the sunset. It means: the Kafka leg is published
    -- and recorded in kafka_dispatched_at, and the legacy HTTP leg is still owed.
    -- Without it the two legs shared one terminal state, so a row whose Kafka
    -- publish succeeded was marked dispatched even when its webhook enqueue had
    -- failed — and because the claim predicate excludes dispatched rows, that
    -- webhook was never retried and never delivered. The state is claimable and,
    -- because kafka_dispatched_at is set, a re-claim delivers ONLY the webhook leg.
    --
    -- The CHECK below enumerates the vocabulary. It is the one place a typo'd
    -- literal is caught, and it matters more here than on blnk.lineage_outbox
    -- because this state machine has more states and every query selects on
    -- specific ones: a row in an unrecognised state is an event nothing will ever
    -- look at again. Adding a state means editing the constraint, which is the
    -- intended friction.
    status              TEXT                      NOT NULL DEFAULT 'pending',

    -- Publish attempts made so far, and the budget for them. The default of 5
    -- matches the default of RELAY_MAX_RETRY_ATTEMPTS, so a row inserted without
    -- an explicit budget still gets the configured retry behaviour. The budget
    -- is per-row rather than global so an operator can extend it for a single
    -- stuck event without reconfiguring the relay.
    attempts            INT                       NOT NULL DEFAULT 0,
    max_attempts        INT                       NOT NULL DEFAULT 5,

    -- The instant this row is next DUE for a publish attempt, and the reason the
    -- configured exponential backoff is real rather than nominal.
    --
    -- Without it, a failed row simply had its lease cleared and became claimable
    -- again on the very next poll. At the relay's 1-second poll interval the
    -- configured schedule of 1s, 2s, 4s, 8s, 16s collapsed to five attempts inside
    -- about five seconds — the whole retry budget spent on a broker that had barely
    -- begun to fail, and every attempt hammering it while it was already struggling.
    -- The alternative, sleeping in the relay process, is worse: the schedule sums to
    -- 31 seconds, which OUTLIVES the 30-second claim lease, so a second instance
    -- reclaims the row mid-sleep and publishes it twice.
    --
    -- Persisting the decision removes both options. The claim predicate is
    -- next_attempt_at <= NOW(), so a row that is not yet due is simply not claimed,
    -- by any instance, and nothing has to sleep.
    --
    -- THE SCHEDULE ITSELF IS NOT HERE. RELAY_RETRY_BASE_BACKOFF_MS, the doubling and
    -- the RELAY_RETRY_MAX_BACKOFF_MS cap are configuration; computing them in SQL
    -- would freeze them into a migration and put them beyond the reach of the
    -- configuration meant to govern them. This column records only the instant the
    -- caller decided on.
    --
    -- NOT NULL DEFAULT NOW() so a freshly inserted row is due immediately: a first
    -- publish is not a retry and must not wait. Distinct from locked_until, and the
    -- two are not interchangeable — locked_until answers "is somebody publishing
    -- this row right now", next_attempt_at answers "may anybody publish it yet".
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
    -- Concurrent relay instances skip locked rows, and an EXPIRED lease makes a
    -- row claimable again, which is what lets a crashed relay's in-flight work be
    -- picked up rather than stranded.
    --
    -- Nullability is required, not incidental: the claim predicate is
    -- (locked_until IS NULL OR locked_until < NOW()), so a freshly inserted row
    -- must have NULL here to be claimable. A NOT NULL default would either make
    -- every new row unclaimable or force a sentinel timestamp.
    locked_until        TIMESTAMP WITH TIME ZONE  NULL,

    -- The token identifying the CLAIM the row is currently under, and what makes
    -- every state transition safe against a worker whose lease expired.
    --
    -- A fresh token is minted on each claim. Every transition — dispatched,
    -- failed, dead-lettered, webhook-dispatched — then names the token it believes
    -- it holds, and its UPDATE matches on that token as well as on the row id and
    -- the expected status. A relay that stalled past its lease, and whose row has
    -- since been claimed and moved on by another instance, therefore matches no
    -- row and is told its claim was lost.
    --
    -- Without it, every transition matched on id alone. Three concrete failures
    -- followed, all silent: a stalled worker overwrote the newer state of a row
    -- another instance had already dispatched; two workers each recorded a failed
    -- attempt against the same claim, double-incrementing attempts and spending
    -- the retry budget at twice the intended rate; and a terminal row could be
    -- moved back out of its terminal state by a call that arrived late.
    --
    -- NULL on a freshly inserted row and set back to NULL at a terminal state, so
    -- a non-NULL value means "some worker holds this row right now".
    claim_token         TEXT                      NULL,

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

    -- When the BROKER acknowledged the Kafka publish, recorded independently of
    -- dispatched_at — and the column that makes the two legs' fates independent.
    --
    -- dispatched_at means "this row is finished". kafka_dispatched_at means "the
    -- Kafka leg of this row is finished", which during the dual-delivery window is
    -- a strictly weaker statement, because the legacy leg may still be owed. They
    -- were once the same column, and the consequence was severe: a row whose Kafka
    -- publish succeeded and whose webhook enqueue failed was marked dispatched
    -- anyway, leaving the webhook leg undeliverable forever with only a warning to
    -- show for it.
    --
    -- Its second job is to make a re-claim safe. A claimed row carrying a non-NULL
    -- value here has ALREADY been published, so the relay skips the publish and
    -- delivers only the outstanding webhook — which is what stops the retry of a
    -- deprecated-transport failure from putting a duplicate on the Kafka topic.
    --
    -- Set on the ordinary success path too, not only in the webhook_pending case,
    -- so the column answers "was this event published to Kafka, and when" for
    -- every row rather than only for the ones that took the unusual path.
    --
    -- Vestigial after the sunset, exactly like webhook_dispatched.
    kafka_dispatched_at TIMESTAMP WITH TIME ZONE  NULL,

    -- Legacy webhook ENQUEUE attempts made so far, counted separately from
    -- attempts.
    --
    -- Separate because the two legs must not spend each other's budget. A webhook
    -- receiver being down, or the queue being unreachable, must never consume a
    -- Kafka retry attempt — that would let the deprecated transport dead-letter
    -- events on the new one, which is precisely backwards. Bounded by the row's
    -- own max_attempts so that a permanently unreachable queue cannot keep a row
    -- claimable indefinitely: once the budget is spent the Kafka delivery is
    -- recorded as final, the legacy leg is abandoned, and the reason is written to
    -- last_error where an operator can find it.
    --
    -- Vestigial after the sunset, exactly like webhook_dispatched.
    webhook_attempts    INT                       NOT NULL DEFAULT 0,

    -- ===================================================================
    -- Group 3b: the broker coordinate (OBS-02)
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

    -- The record's offset within that partition.
    --
    -- WHY THE COORDINATE IS STORED AT ALL. The zero-loss criterion (V-2) reconciles
    -- this table against the broker. Done by COUNTING — records written against rows
    -- claiming a publication — it cannot detect loss that duplicate surplus happens
    -- to offset: ten lost events plus ten redeliveries produce exactly the totals of
    -- a healthy pipeline, and the reconciliation reports no loss while ten events are
    -- genuinely missing. Storing the coordinate replaces that subtraction with a
    -- MAPPING: every row that claims a publication names the record it produced, the
    -- unique index below refuses two rows the same record, and a row claiming a
    -- publication with no coordinate is visible as unconfirmed rather than absorbed
    -- into the surplus.
    kafka_offset        BIGINT                    NULL,

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
    created_at          TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),

    -- ===================================================================
    -- Group 5: the invariants the database itself enforces
    --
    -- These are here rather than in Go because application-level validation
    -- protects only the code paths that remember to call it, while a constraint
    -- protects the table. A row that violates one of these is not a row with a bad
    -- value in it; it is a row that breaks a delivery guarantee, and the cheapest
    -- place to make it unrepresentable is the schema.
    --
    -- Each is written to be satisfied by every row the current code inserts, so
    -- the migration applies to an existing table without a data fix.
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
    --
    -- dlt_pending is the state a row enters the instant its retry budget is spent,
    -- and it exists because "the budget is spent" and "the event is preserved
    -- somewhere durable" are two different facts. Moving straight to failed made
    -- the first imply the second: failed is outside the claim predicate, so a row
    -- whose dead-letter write then failed — a broker outage, a cancelled context, a
    -- process killed between the two — was terminal with the event existing NOWHERE
    -- but this row, and no relay poll and no replay path could ever pick it up
    -- again. dlt_pending is retryable by design: the hand-off is re-claimable once
    -- the holder's lease expires, and only MarkEventDeadLettered — which runs after
    -- the `<topic>.dlt` write is acknowledged — may declare the row terminal.
    CONSTRAINT event_outbox_status_known
        CHECK (status IN ('pending', 'processing', 'webhook_pending', 'dispatched', 'failed',
                          'dead_lettered', 'replaying'))
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
--
-- IT HAS A SECOND CONSUMER, and narrowing it would take that one off the index
-- too. ClaimExhaustedEventsForDeadLetter sweeps for rows left `failed` with the
-- retry budget spent and dlt_topic still NULL — events whose dead-letter write
-- never completed, which no other claim in the repository can reach — and it
-- drives this index by (status, occurred_at), taking the remaining columns as a
-- filter. That sweep runs on every relay tick, so an index scan here rather than
-- a sequential one is what keeps it free on an outbox that has accumulated
-- terminal rows.
CREATE INDEX IF NOT EXISTS idx_event_outbox_failed
    ON blnk.event_outbox (status, occurred_at)
    WHERE status IN ('dlt_pending', 'failed', 'dead_lettered');

-- The index behind the DEAD-LETTER HAND-OFF RECOVERY claim, which is what makes
-- dlt_pending retryable rather than merely differently-named.
--
-- The query, run once per relay tick:
--
--   UPDATE blnk.event_outbox
--   SET claim_token = $1, locked_until = NOW() + $2::interval, last_attempted_at = NOW()
--   WHERE id IN (
--       SELECT id FROM blnk.event_outbox
--       WHERE status = 'dlt_pending'
--         AND (locked_until IS NULL OR locked_until < NOW())
--       ORDER BY occurred_at ASC
--       LIMIT $3
--       FOR UPDATE SKIP LOCKED
--   )
--   RETURNING <all columns>
--
-- The lease is what stops this claim racing the worker that is still performing the
-- hand-off: a row is only re-claimable once the lease its original claim took has
-- expired, so two workers cannot both write the same event to its dead-letter topic.
-- Without the lease in the predicate, the fresh claim token this claim stamps would
-- invalidate the original worker's token AFTER it had already published, and the
-- duplicate would be undetectable.
--
-- It is partial on a state that is empty in steady state — a dlt_pending row means a
-- dead-letter write is in flight or has just failed — so this index costs almost
-- nothing to maintain and the recovery poll never touches the rest of the table.
CREATE INDEX IF NOT EXISTS idx_event_outbox_dlt_pending
    ON blnk.event_outbox (status, locked_until, occurred_at)
    WHERE status = 'dlt_pending';

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
--             AND next_attempt_at <= NOW()
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
--
-- next_attempt_at joins the predicate columns because the due check runs on every
-- candidate on every poll. A row inside its backoff window is the common case
-- during an outage — the entire backlog is not yet due — so leaving that column
-- off the index would make the poll read and discard the whole retrying set on
-- every lap, which is exactly the load the backoff exists to shed.
-- 'webhook_pending' is in the predicate because it is part of the CLAIMABLE set: a
-- row whose Kafka leg is done but whose legacy leg is still owed has to be picked up
-- again, and a claim predicate the index did not cover would degrade the poll into a
-- sequential scan during exactly the incident that produced those rows.
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

-- The index that makes PER-KEY ORDERING enforceable rather than merely intended,
-- and the most consequential index in this migration.
--
-- The claim query used to be FOR UPDATE SKIP LOCKED over a globally ordered
-- window, which is safe against two relays claiming the SAME row and unsafe
-- against something subtler: SKIP LOCKED means relay B skips the earlier row that
-- relay A holds and claims a LATER row — possibly one with the same partition
-- key. Kafka preserves append order, not occurred_at, so relay B's message can be
-- appended first and a subscriber sees transaction.applied before
-- transaction.queued for the same aggregate. No error, no log line, and the
-- ordering guarantee the whole partitioning scheme exists to provide is gone.
--
-- The fix is a NOT EXISTS predicate in the claim: a row is claimable only when no
-- EARLIER row with the same partition_key is still pending or processing. That
-- predicate runs once per candidate row, so it needs an index keyed on
-- partition_key with the ordering columns trailing, restricted to exactly the
-- states that block — otherwise the check degrades into a scan of the table's
-- entire history per candidate and the relay's throughput collapses as the table
-- grows.
--
-- The partial predicate is the set of BLOCKING states, and it is deliberately
-- narrower than the claimable set: only pending and processing rows hold a key
-- back. A row that has exhausted its budget (failed) or been preserved on its
-- dead-letter topic (dead_lettered) does NOT block its key forever — the trade is
-- explicit. Strict ordering would demand it block, but a single permanently
-- undeliverable event would then stall every subsequent event for that aggregate
-- indefinitely, which is a worse failure than a gap. Retries DO preserve order,
-- because a retrying row returns to pending.
--
-- 'webhook_pending' is deliberately NOT here, and the asymmetry with the claim index
-- above is the point. Ordering is a property of the KAFKA topic, and a webhook_pending
-- row has already been published to it — its position in the partition is fixed and
-- nothing it does subsequently can change it. Blocking its key would let a failing
-- LEGACY enqueue stall Kafka delivery for the whole aggregate, which is the deprecated
-- transport interfering with the new one. So such a row is claimable without being
-- blocking: the outstanding webhook is retried while later events keep flowing.
CREATE INDEX IF NOT EXISTS idx_event_outbox_partition_key_inflight
    ON blnk.event_outbox (partition_key, occurred_at, id)
    WHERE status IN ('pending', 'processing');

-- The retention index, and the retention contract it serves.
--
-- WHAT IS STORED HERE IS SENSITIVE. payload is the webhook body verbatim, so a
-- transaction event carries amounts and balance identifiers and an identity event
-- carries names, email addresses, phone numbers, postal addresses and dates of
-- birth. last_error and failure_metadata carry broker and driver text. None of it
-- has any operational value once the event has been delivered, and keeping it
-- indefinitely turns a delivery buffer into an unbounded secondary copy of the
-- ledger's most sensitive data — with none of the access controls the primary
-- tables have around them.
--
-- The retention primitive is Datasource.PurgeTerminalEventsBefore, which deletes
-- rows in a TERMINAL state whose occurrence is older than a cutoff. This index is
-- what lets it delete a bounded slice cheaply instead of scanning the table.
--
-- Terminal only, and that is the safety property: a pending, processing,
-- replaying or failed row is still owed a delivery attempt, and nothing here can
-- delete one however old it is. Choosing the cutoff, and honouring any legal or
-- audit hold that requires a longer one, is an operator decision — this schema
-- provides the mechanism and takes no view on the period.
CREATE INDEX IF NOT EXISTS idx_event_outbox_terminal_retention
    ON blnk.event_outbox (occurred_at)
    WHERE status IN ('dispatched', 'dead_lettered');

-- The broker coordinate is UNIQUE, and that is a correctness guarantee rather
-- than a performance one (OBS-02).
--
-- One record is produced by one acknowledged write of one row, so two rows can
-- never legitimately name the same coordinate. Making the database refuse it turns
-- the audit's "confirmed rows equals distinct records" check into a statement about
-- an enforced invariant rather than a hopeful comparison: a duplicate would
-- otherwise let two rows share one record's corroboration, which is the same
-- double-counting the reconciliation exists to eliminate.
--
-- PARTIAL, so the overwhelming majority of rows — everything not yet published, and
-- everything published before this column existed — cost nothing and collide with
-- nothing. Postgres treats NULLs as distinct in a unique index anyway; the
-- predicate makes the index small as well as correct.
CREATE UNIQUE INDEX IF NOT EXISTS event_outbox_broker_record_uidx
    ON blnk.event_outbox (kafka_topic, kafka_partition, kafka_offset)
    WHERE kafka_offset IS NOT NULL;

-- The outbox side of the daily zero-loss reconciliation, which counts rows claiming
-- a Kafka record and how many of those name one.
--
-- The predicate is deliberately WIDER than the terminal statuses. A webhook_pending
-- row HAS been published to Kafka — its Kafka leg completed and kafka_dispatched_at
-- is stamped; what is outstanding is the deprecated HTTP leg. Counting only
-- dispatched and dead_lettered rows would leave those records unaccounted for on the
-- broker side, inflating the apparent surplus and loosening the reconciliation during
-- exactly the window it matters most.
--
-- kafka_offset is carried as an INCLUDE column so the confirmed/unconfirmed split is
-- answerable from the index alone.
CREATE INDEX IF NOT EXISTS idx_event_outbox_published_audit
    ON blnk.event_outbox (kafka_dispatched_at)
    INCLUDE (kafka_topic, kafka_partition, kafka_offset)
    WHERE kafka_dispatched_at IS NOT NULL OR status = 'dead_lettered';

-- +migrate Down

-- Indexes first, then the table. Dropping the table would take its indexes with
-- it, but naming them explicitly keeps the rollback correct if a future
-- migration ever detaches one of these objects from the other, and it documents
-- exactly what this migration created. Index names are schema-qualified in the
-- drop even though they are unqualified in the create; that asymmetry is the
-- house convention.
DROP INDEX IF EXISTS blnk.idx_event_outbox_published_audit;
DROP INDEX IF EXISTS blnk.event_outbox_broker_record_uidx;
DROP INDEX IF EXISTS blnk.idx_event_outbox_terminal_retention;
DROP INDEX IF EXISTS blnk.idx_event_outbox_partition_key_inflight;
DROP INDEX IF EXISTS blnk.idx_event_outbox_aggregate;
DROP INDEX IF EXISTS blnk.idx_event_outbox_claim;
DROP INDEX IF EXISTS blnk.idx_event_outbox_dlt_pending;
DROP INDEX IF EXISTS blnk.idx_event_outbox_failed;
DROP INDEX IF EXISTS blnk.idx_event_outbox_pending;
DROP INDEX IF EXISTS blnk.event_outbox_event_id_uidx;

DROP TABLE IF EXISTS blnk.event_outbox;
