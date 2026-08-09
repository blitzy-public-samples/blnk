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

-- AN EXPLICIT RESOLUTION FOR A DEAD-LETTERED EVENT, so retention can delete the
-- evidence of a failure someone has dealt with WITHOUT deleting the evidence of one
-- nobody has looked at.
--
-- # The defect this closes
--
-- The retention purge selected on status alone, over the set {dispatched,
-- dead_lettered}. dispatched belongs there: the event reached the broker, the
-- subscriber has had it, and the row is a receipt. dead_lettered did not. A
-- dead-lettered row is the record of an event NO SUBSCRIBER EVER RECEIVED — it is the
-- only inventory an operator triages from, the only thing a replay can be driven from,
-- and the only place the failure metadata that explains the loss exists. Deleting it on
-- an age timer destroys, unrecoverably and without a trace, the evidence that a ledger
-- event went undelivered.
--
-- The two populations therefore need SEPARATE LIFECYCLES, and status cannot express
-- that on its own because "dealt with" is not a delivery state — it is an operator
-- decision about a row that is already in its final delivery state. So it is recorded
-- as its own fact.
--
-- # Why a column and not a new status literal
--
-- A 'resolved' status would have been the smaller diff and the wrong design. Three
-- things read the dead_lettered literal and would all have had to change in step, and
-- one of them is a correctness property rather than a convenience:
--
--   * THE ZERO-LOSS RECONCILIATION (acceptance criterion V-2) passes when the
--     dispatched plus dead-lettered row counts equal the sum of the main-topic and
--     dead-letter-topic end offsets. A resolved event still has its message on the
--     `.dlt` topic, so moving it out of the dead_lettered count would make the identity
--     fail for a resolution that changed nothing about the broker.
--   * idx_event_outbox_published_audit and the broker-coordinate projection select on
--     the literal.
--   * A replay requires the dead_lettered state, so a resolved row would stop being
--     replayable — and "I have dealt with this" must not mean "and I can never resend
--     it".
--
-- A nullable timestamp is additive against all three: the status is untouched, the
-- counts are untouched, replay is untouched, and the only behaviour that changes is the
-- one that must.
ALTER TABLE blnk.event_outbox
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

COMMENT ON COLUMN blnk.event_outbox.resolved_at IS
    'When an operator recorded that this dead-lettered event has been dealt with. NULL means '
    'unresolved: the event reached no subscriber and nobody has accounted for it, so retention '
    'must never delete the row and the dead-letter age gauge must keep counting it. Set only '
    'through the resolve endpoint, which requires the dead_lettered state.';

-- The operator's own words about WHY it is resolved, which is the part no timestamp can
-- carry. "Replayed after the broker came back", "duplicate of the event in ticket 4182",
-- "the subscriber was decommissioned and does not want it" — these are the difference
-- between an audit trail and a boolean. Bounded because it is operator-supplied free
-- text on a row the API projects: an unbounded column here becomes an unbounded field
-- in every listing response.
ALTER TABLE blnk.event_outbox
    ADD COLUMN IF NOT EXISTS resolution_note TEXT;

COMMENT ON COLUMN blnk.event_outbox.resolution_note IS
    'Optional operator note recorded with resolved_at, explaining why the dead-lettered event '
    'needs no further action. Bounded and sanitised at the service layer before it is stored.';

-- A note without a resolution is a note about nothing, and the pair is what the audit
-- trail is. Enforced here rather than only in Go because the column is writable by
-- anything holding the connection, and a half-written pair would read as an unresolved
-- row carrying an explanation.
-- The StatementBegin/StatementEnd markers are required, not decorative: sql-migrate
-- splits a migration on semicolons and knows nothing about dollar quoting, so without
-- them the block is cut at its first internal semicolon and the migration fails with
-- "unterminated dollar-quoted string". sql/1781248920.sql brackets its guarded
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

-- A resolution may only be recorded against a row that actually has a dead-letter
-- record. failed is deliberately excluded: its `<topic>.dlt` write is still OWED, so the
-- outbox row is the only copy of the event in existence and "dealt with" cannot be true
-- of it yet. replaying is included because it is a transient state a dead_lettered row
-- passes through and back out of — refusing it would make a resolution fail for the
-- duration of an unrelated replay.
-- The StatementBegin/StatementEnd markers are required, not decorative: sql-migrate
-- splits a migration on semicolons and knows nothing about dollar quoting, so without
-- them the block is cut at its first internal semicolon and the migration fails with
-- "unterminated dollar-quoted string". sql/1781248920.sql brackets its guarded
-- constraint additions the same way for the same reason.
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

-- THE PURGE INDEX, replaced rather than added to, because its predicate IS the purge's
-- eligibility rule and the two must be identical or the purge stops being index-driven.
--
-- idx_event_outbox_terminal_retention covered {dispatched, dead_lettered} — the old,
-- wrong set. A partial index is usable only when its predicate is implied by the
-- query's, so leaving it in place while the purge narrowed its WHERE clause would have
-- left the narrower query falling back to a sequential scan of the whole table on every
-- sweep. It is dropped and replaced in one migration for that reason.
DROP INDEX IF EXISTS blnk.idx_event_outbox_terminal_retention;

CREATE INDEX IF NOT EXISTS idx_event_outbox_purgeable
    ON blnk.event_outbox (occurred_at)
    WHERE status = 'dispatched'
       OR (status = 'dead_lettered' AND resolved_at IS NOT NULL);

-- THE UNRESOLVED DEAD-LETTER INVENTORY, which is what an operator triages and what the
-- age gauge behind the DeadLetterMessageStuck alert is computed from.
--
-- idx_event_outbox_failed covers {failed, dead_lettered} on (status, occurred_at) and
-- still serves the unfiltered listing. It cannot serve the unresolved subset: adding
-- `resolved_at IS NULL` to a query's WHERE clause does not make that index any more
-- selective, so once a deployment has accumulated resolved rows every poll of the age
-- gauge would sift them again. Keyed on occurred_at ascending-friendly order because the
-- gauge wants the OLDEST entry, and carrying dlt_topic so the per-topic maximum can be
-- built from the index alone.
CREATE INDEX IF NOT EXISTS idx_event_outbox_unresolved_dead_letter
    ON blnk.event_outbox (occurred_at, id)
    INCLUDE (dlt_topic)
    WHERE status IN ('failed', 'dead_lettered') AND resolved_at IS NULL;

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_event_outbox_unresolved_dead_letter;
DROP INDEX IF EXISTS blnk.idx_event_outbox_purgeable;

-- Restored exactly as 1781248800.sql created it, so rolling this migration back leaves
-- the purge index the previous schema expects.
CREATE INDEX IF NOT EXISTS idx_event_outbox_terminal_retention
    ON blnk.event_outbox (occurred_at)
    WHERE status IN ('dispatched', 'dead_lettered');

ALTER TABLE blnk.event_outbox
    DROP CONSTRAINT IF EXISTS event_outbox_resolution_state_chk;

ALTER TABLE blnk.event_outbox
    DROP CONSTRAINT IF EXISTS event_outbox_resolution_pair_chk;

ALTER TABLE blnk.event_outbox
    DROP COLUMN IF EXISTS resolution_note;

ALTER TABLE blnk.event_outbox
    DROP COLUMN IF EXISTS resolved_at;
