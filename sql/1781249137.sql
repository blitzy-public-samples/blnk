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

-- The W3C trace context of the request that CAPTURED an event, carried on the row.
--
-- Why the row and not a side table: an outbox row is the only artefact that survives
-- between the request that produced the event and the relay that publishes it. The two
-- are decoupled by design — the request commits and returns, the relay claims the row
-- up to a poll interval later, possibly in a different process — so a trace that is not
-- written down here cannot be recovered afterwards from anything.
--
-- Both columns are NULLABLE and stay null for every event captured with no active trace
-- — a CLI-driven mutation, a worker-initiated rejection, or any deployment running
-- without observability enabled. Null is the correct reading of "there was no trace to
-- record", and the publish path treats it as such rather than as a defect.
ALTER TABLE blnk.event_outbox
    ADD COLUMN IF NOT EXISTS traceparent TEXT NULL,
    ADD COLUMN IF NOT EXISTS tracestate  TEXT NULL;

-- NO INDEX, deliberately.
--
-- Nothing queries by trace: the trace context is read only after a row has already been
-- claimed by primary key, and correlation in the other direction happens in the tracing
-- backend rather than in SQL. An index here would cost every insert on the ledger's hottest
-- table to serve no query at all.

-- The two columns are BOUNDED, and the bound is the point rather than defensiveness.
--
-- These values arrive from an inbound HTTP header, so they are caller-influenced data
-- on a table that carries one row per ledger mutation at 500 events per second. Without
-- a ceiling a caller could append arbitrary bytes to every event row in the ledger, and
-- the cost would appear as table and index bloat on the relay's hottest table rather
-- than as a rejected request.
--
-- Guarded by a catalogue lookup because PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS,
-- and the columns above ARE idempotent — so a re-run would otherwise fail here having
-- succeeded there, which is the worst of both. sql/1781248920.sql guards its constraint
-- the same way, and for the same reason. The StatementBegin/StatementEnd markers are
-- required rather than decorative: sql-migrate splits on semicolons and knows nothing
-- about dollar quoting, so without them the block is cut at its first internal
-- semicolon.
-- +migrate StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'event_outbox_traceparent_bounded'
          AND conrelid = 'blnk.event_outbox'::regclass
    ) THEN
        ALTER TABLE blnk.event_outbox
            ADD CONSTRAINT event_outbox_traceparent_bounded
            CHECK (traceparent IS NULL OR length(traceparent) <= 255);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'event_outbox_tracestate_bounded'
          AND conrelid = 'blnk.event_outbox'::regclass
    ) THEN
        ALTER TABLE blnk.event_outbox
            ADD CONSTRAINT event_outbox_tracestate_bounded
            CHECK (tracestate IS NULL OR length(tracestate) <= 512);
    END IF;
END
$$;
-- +migrate StatementEnd


-- +migrate Down

ALTER TABLE blnk.event_outbox
    DROP CONSTRAINT IF EXISTS event_outbox_tracestate_bounded,
    DROP CONSTRAINT IF EXISTS event_outbox_traceparent_bounded;

ALTER TABLE blnk.event_outbox
    DROP COLUMN IF EXISTS tracestate,
    DROP COLUMN IF EXISTS traceparent;
