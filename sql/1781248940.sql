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
-- blnk.event_outbox, and the thing that makes the daily zero-loss reconciliation able
-- to reach a sound verdict at all.
--
-- The reconciliation compares two quantities:
--
--   outbox side  — the number of event_outbox rows in a terminal state
--   broker side  — the sum of per-partition END OFFSETS across the owned topics
--
-- An end offset counts every record ever appended to that partition, from offset zero.
-- It does not go down.
--
-- That matters because the surplus is the only thing the arithmetic can hide loss
-- behind. The comparison is directional: records are a lower bound on events, so a
-- shortfall proves loss and a surplus is expected.
CREATE TABLE IF NOT EXISTS blnk.event_outbox_purge_log (
    id                    BIGSERIAL PRIMARY KEY,

    -- The retention cutoff the sweep used: rows whose occurred_at was strictly
    -- earlier than this were eligible. Recorded so that an operator can tell which
    -- WINDOW of history has been purged, not merely how much of it.
    cutoff                TIMESTAMPTZ NOT NULL,

    -- How many rows the DELETE removed in this batch. This is the quantity the
    -- reconciliation adds back to reach an all-time terminal count commensurable with
    -- all-time end offsets.
    rows_removed          BIGINT NOT NULL CHECK (rows_removed > 0),

    -- How many of the removed rows CARRIED A BROKER COORDINATE (kafka_offset was not
    -- null), and therefore how many of the records the broker still counts have lost
    -- the row that named them.
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
