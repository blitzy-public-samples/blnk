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

-- WITHDRAW THE DEAD-LETTER RESOLUTION COLUMNS, because the state machine they created
-- could not be completed and the endpoint that wrote them was never part of the agreed
-- API surface.
--
-- # The defect this closes
--
-- sql/1781249007.sql added resolved_at, resolution_note and two CHECK constraints so that
-- an operator could record "this dead-lettered event has been dealt with" and retention
-- could then delete the row. One of those constraints,
-- event_outbox_resolution_state_chk, confined a resolution to the dead_lettered and
-- replaying states. That is a correct-looking rule with an impossible consequence:
--
--   1. An operator resolves a dead-lettered event. resolved_at is set.
--   2. The broker recovers and the same event is REPLAYED. The replay claims the row
--      (status becomes replaying), publishes the stored bytes, and the broker
--      acknowledges them.
--   3. MarkEventDispatched then moves the row to dispatched — and dispatched is not one
--      of the two states the constraint permits a resolution in, so PostgreSQL REJECTS
--      the only write that records the successful publish.
--   4. The API answers EVENT_REPLAY_FAILED for a publish that in fact succeeded, and the
--      row is released still dead_lettered — that is, still replayable. Every retry
--      publishes the event to the topic again.
--
-- The reverse order was refused outright: once a replay had made the row dispatched, a
-- resolution could no longer be recorded at all, because the write required the
-- dead_lettered state. So the two operations composed in neither direction, and one of
-- the orderings manufactured duplicate ledger events on a topic subscribers consume.
--
-- # Why the columns go rather than the constraint
--
-- Dropping the constraint alone would make the schema permissive enough for the sequence
-- above to commit, and would leave the feature in place. The feature itself is the
-- problem: POST /events/dead-letter/:event_id/resolve was a fourteenth management route
-- on a surface the plan fixes at thirteen, and its purpose — telling retention which
-- dead-letter rows are safe to delete — is served correctly and with no extra state by
-- the workflow that was already approved:
--
--   * A DEAD-LETTERED row is now NEVER deleted by age. It is the only record that a
--     ledger event went undelivered, the only inventory triage reads, and the only place
--     the bytes a replay is driven from and the failure metadata explaining the loss
--     exist. Nothing may remove it on a timer.
--   * REPLAY is what ends its life. A re-publish the broker acknowledges makes the row
--     dispatched — a receipt for an event a subscriber has now had — and a receipt is
--     exactly what the retention sweep is for.
--
-- The dead-letter age gauge and the DeadLetterMessageStuck alert therefore keep counting
-- an entry until it has actually reached a subscriber, which is stronger pressure than a
-- resolution that could be recorded without anything being delivered.
--
-- No data is lost that anything reads: nothing in the shipped code writes these columns
-- any more, and the endpoint that used to is gone in the same change.

-- The state constraint that made a broker-acknowledged replay uncommittable. Dropped
-- FIRST, before the columns it references, so the drop order is legible rather than
-- relying on the cascade a column drop performs.
ALTER TABLE blnk.event_outbox
    DROP CONSTRAINT IF EXISTS event_outbox_resolution_state_chk;

ALTER TABLE blnk.event_outbox
    DROP CONSTRAINT IF EXISTS event_outbox_resolution_pair_chk;

ALTER TABLE blnk.event_outbox
    DROP COLUMN IF EXISTS resolution_note;

ALTER TABLE blnk.event_outbox
    DROP COLUMN IF EXISTS resolved_at;

-- THE PURGE INDEX, replaced rather than adjusted, because its predicate IS the purge's
-- eligibility rule and a partial index is only usable when its predicate is implied by
-- the query's. The sweep now selects `status = 'dispatched'` alone; an index whose
-- predicate still named resolved_at would reference a dropped column and could not be
-- created at all, so the replacement is not optional.
DROP INDEX IF EXISTS blnk.idx_event_outbox_purgeable;

CREATE INDEX IF NOT EXISTS idx_event_outbox_purgeable
    ON blnk.event_outbox (occurred_at)
    WHERE status = 'dispatched';

-- The unresolved-inventory index goes with the concept it indexed. The dead-letter age
-- gauge now aggregates every outstanding entry, and idx_event_outbox_failed — created by
-- sql/1781248800.sql over (status, occurred_at) for {failed, dead_lettered} — is the
-- index that serves it, exactly as it did before the resolution subset existed.
DROP INDEX IF EXISTS blnk.idx_event_outbox_unresolved_dead_letter;

-- +migrate Down

-- Restored exactly as sql/1781249007.sql left the schema, so rolling this migration back
-- reinstates the state the previous one describes — including the constraint whose
-- consequence is documented above. Rolling back therefore also requires rolling back
-- 1781249007 before the resolution endpoint could be reintroduced safely.
ALTER TABLE blnk.event_outbox
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

ALTER TABLE blnk.event_outbox
    ADD COLUMN IF NOT EXISTS resolution_note TEXT;

-- The StatementBegin/StatementEnd markers are required, not decorative: sql-migrate
-- splits a migration on semicolons and knows nothing about dollar quoting, so without
-- them the block is cut at its first internal semicolon and the migration fails with
-- "unterminated dollar-quoted string". sql/1781249007.sql brackets the same two guarded
-- constraint additions the same way for the same reason.
-- +migrate StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'event_outbox_resolution_pair_chk'
          AND conrelid = 'blnk.event_outbox'::regclass
    ) THEN
        ALTER TABLE blnk.event_outbox
            ADD CONSTRAINT event_outbox_resolution_pair_chk
            CHECK (resolution_note IS NULL OR resolved_at IS NOT NULL);
    END IF;
END
$$;
-- +migrate StatementEnd

-- +migrate StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'event_outbox_resolution_state_chk'
          AND conrelid = 'blnk.event_outbox'::regclass
    ) THEN
        ALTER TABLE blnk.event_outbox
            ADD CONSTRAINT event_outbox_resolution_state_chk
            CHECK (resolved_at IS NULL OR status IN ('dead_lettered', 'replaying'));
    END IF;
END
$$;
-- +migrate StatementEnd

DROP INDEX IF EXISTS blnk.idx_event_outbox_purgeable;

CREATE INDEX IF NOT EXISTS idx_event_outbox_purgeable
    ON blnk.event_outbox (occurred_at)
    WHERE status = 'dispatched'
       OR (status = 'dead_lettered' AND resolved_at IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_event_outbox_unresolved_dead_letter
    ON blnk.event_outbox (occurred_at, id)
    INCLUDE (dlt_topic)
    WHERE status IN ('failed', 'dead_lettered') AND resolved_at IS NULL;
