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

-- blnk.event_outbox_purge_log: the durable record of what retention DELETED from
-- blnk.event_outbox, and the thing that makes the daily zero-loss reconciliation
-- able to reach a sound verdict at all.
--
-- # The problem it solves, stated precisely
--
-- The reconciliation compares two quantities:
--
--   outbox side  — the number of event_outbox rows in a terminal state
--   broker side  — the sum of per-partition END OFFSETS across the owned topics
--
-- An end offset counts every record ever appended to that partition, from offset
-- zero. It does not go down. Retention removing the underlying log segments does
-- not lower it, and deleting the outbox row that produced a record certainly does
-- not.
--
-- So the moment retention deletes its first terminal row, the two sides stop
-- measuring the same interval. The outbox side now counts a SUFFIX of history while
-- the broker side still counts ALL of it, and the difference — which the verdict
-- reads as expected "overhead" from redeliveries, replays and dead-letter copies —
-- silently absorbs every purged row as well.
--
-- That matters because the surplus is the only thing the arithmetic can hide loss
-- behind. The comparison is directional: records are a lower bound on events, so a
-- shortfall proves loss and a surplus is expected. A purge-inflated surplus can
-- therefore offset a genuine shortfall exactly, and the endpoint returns a
-- CONCLUSIVE "no loss detected" while events are missing. Nothing about the numbers
-- hints at it, which is the property that makes it dangerous rather than merely
-- wrong: the answer is confident and unverifiable.
--
-- There is no way to detect this after the fact. A deleted row leaves no trace, so
-- "have any terminal rows been purged, and how many" is unanswerable unless the
-- purge itself records it. This table is that record, and writing it is what turns
-- an unknowable quantity into a known one.
--
-- # Why it is a log rather than a counter
--
-- A single running total would answer the reconciliation's question, and it would
-- answer nothing else. A row per purge batch additionally supports the operational
-- questions that arise the moment a reconciliation is inconclusive: when did
-- retention last run, how much is it removing per sweep, has it been keeping up, and
-- was a particular window purged before or after the offsets were measured. Those
-- are exactly the questions an operator asks while deciding whether an inconclusive
-- verdict is benign.
--
-- It is small by construction: one row per purge BATCH, not per deleted event, and
-- the retention sweep runs on an interval measured in hours.
CREATE TABLE IF NOT EXISTS blnk.event_outbox_purge_log (
    id                    BIGSERIAL PRIMARY KEY,

    -- The retention cutoff the sweep used: rows whose occurred_at was strictly
    -- earlier than this were eligible. Recorded so that an operator can tell which
    -- WINDOW of history has been purged, not merely how much of it.
    cutoff                TIMESTAMPTZ NOT NULL,

    -- How many rows the DELETE removed in this batch. This is the quantity the
    -- reconciliation adds back to reach an all-time terminal count commensurable
    -- with all-time end offsets.
    --
    -- A batch that removed nothing is NOT recorded — see the repository, which skips
    -- the insert on a zero-row delete. The column is still constrained to be
    -- positive so that a zero can never be written by another path and then be
    -- indistinguishable from a real batch in the aggregate.
    rows_removed          BIGINT NOT NULL CHECK (rows_removed > 0),

    -- How many of the removed rows CARRIED A BROKER COORDINATE (kafka_offset was not
    -- null), and therefore how many of the records the broker still counts have lost
    -- the row that named them.
    --
    -- It is recorded separately from rows_removed because the two answer different
    -- questions. rows_removed restores the terminal count; this one restores the
    -- CONFIRMED count, which is what the row-to-record mapping is measured against.
    -- Conflating them would make a purge of unconfirmed rows look like a purge of
    -- confirmed ones and vice versa, and the mapping is the part of the verdict that
    -- carries its soundness.
    confirmed_removed     BIGINT NOT NULL DEFAULT 0 CHECK (confirmed_removed >= 0),

    -- The oldest and newest occurrence instants among the removed rows, so the
    -- purged interval is known exactly rather than bounded only from above by the
    -- cutoff. Nullable because a future purge path may not be able to compute them,
    -- and a missing bound must read as "unknown" rather than as the zero instant.
    oldest_occurred_at    TIMESTAMPTZ NULL,
    newest_occurred_at    TIMESTAMPTZ NULL,

    -- When the batch was committed.
    purged_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- confirmed_removed can never exceed rows_removed: it counts a subset of the
    -- same rows. A violation would mean the two were computed over different sets,
    -- which would corrupt the baseline in a way the arithmetic could not reveal.
    CONSTRAINT event_outbox_purge_log_confirmed_within_removed
        CHECK (confirmed_removed <= rows_removed),

    -- The interval bounds are both present or both absent, and ordered. A single
    -- bound describes no interval, and a reversed pair is a computation error rather
    -- than a narrow window.
    CONSTRAINT event_outbox_purge_log_interval_coherent
        CHECK (
            (oldest_occurred_at IS NULL AND newest_occurred_at IS NULL)
            OR (
                oldest_occurred_at IS NOT NULL
                AND newest_occurred_at IS NOT NULL
                AND oldest_occurred_at <= newest_occurred_at
            )
        )
);

-- The reconciliation reads one aggregate over the whole table — SUM(rows_removed),
-- SUM(confirmed_removed) — and an operator reads the most recent batches. The index
-- serves the second: newest first, which is the order "when did retention last run"
-- is asked in. The aggregate is a full scan by nature and needs no index, and the
-- table is one row per batch so that scan stays trivial.
CREATE INDEX IF NOT EXISTS idx_event_outbox_purge_log_recent
    ON blnk.event_outbox_purge_log (purged_at DESC, id DESC);

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_event_outbox_purge_log_recent;
DROP TABLE IF EXISTS blnk.event_outbox_purge_log;
