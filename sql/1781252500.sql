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

-- PIN THE TWO KEY-SPACE LOOKUPS THE RELAY CLAIM DEPENDS ON TO THE INDEX THAT ANSWERS THEM
-- IN ONE SEEK, because the planner reliably picks a different index and the wrong choice
-- turns a slow drain into a self-reinforcing one.
--
-- The claim in database/event_outbox_claim.go asks the key space two questions, both of
-- which idx_event_outbox_effective_key_inflight — btree on
-- (COALESCE(NULLIF(btrim(ledger_id),''), btrim(partition_key)), occurred_at, id) WHERE
-- status IN ('pending','processing') — answers by seeking to one entry and stopping:
--
--   1. "what is the next distinct effective key after this cursor?" — the rotation walk
--      that keeps a key from starving behind a larger one.
--   2. "what is this key's oldest unfinished row?" — the head, which is the gate every
--      claimant must pass through for per-key ordering to hold.
--
-- The planner does not pick that index. It picks idx_event_outbox_claim_order, on
-- (occurred_at, id) WHERE status IN ('pending','processing','webhook_pending'), and applies
-- the key as a FILTER — reading the whole claimable population to answer a question about
-- one key. It is not a mis-costing of the indexes but of the PREDICATE: the number of rows
-- matching status IN ('pending','processing') is bimodal in an outbox, zero for long
-- stretches and thousands during a burst, so ANALYZE has almost always last run on an empty
-- backlog and the planner values the claimable set at ONE ROW. At one row the age-ordered
-- scan is estimated at 6.68 and the seek at 8.38, and the scan wins by that margin however
-- many rows are really there.
--
-- Measured on a 600,000-row table with statistics gathered while nothing was claimable,
-- one claim of 100 rows across distinct keys:
--
--        pending rows | claim without these functions |     with them
--               5,000 |    380ms to 424ms,  75k pages | 7.6ms,  4.5k pages
--              25,000 |            2,215ms, 1.8M pages | 9.4ms,  4.6k pages
--              50,000 |            4,853ms, 7.3M pages | 10.1ms, 4.7k pages
--             150,000 |           12,838ms, 11.6M pages | 9.5ms,  4.8k pages
--
-- The left column is the feedback loop, and it was observed in production shape before it
-- was understood here: claim latency climbing 26ms, 171ms, 654ms, 1.9s, 3.2s, 4.5s while
-- the pending backlog climbed 11, 1,900, 8,386, 16,088, 23,886 rows. The claim gets slower
-- because the backlog grew, and the backlog grows because the claim got slower. It only
-- broke when autoanalyze happened to run; at 12.8 seconds it was also approaching the
-- relay's own claim timeout, at which point claims would have started being abandoned.
--
-- WHY A FUNCTION WITH A SET CLAUSE, and not any of the cheaper things. A function whose
-- definition carries SET is planned with that setting applied and is never inlined into the
-- calling query, which is exactly the isolation wanted: the setting shapes these two
-- lookups and nothing else in the claim. Both bodies are phrased so that ORDER BY matches
-- idx_event_outbox_effective_key_inflight's own column order, so under enable_sort = off
-- the age-ordered alternative is not merely more expensive but unusable — it would have to
-- sort, and sorting is what has been switched off. Nothing here overrides a cost estimate;
-- it removes a plan from consideration for two statements whose correct plan is not in
-- doubt.
--
-- Every alternative was measured first, and each was rejected on evidence:
--
--   * random_page_cost = 1.1 moves both index paths together and does not flip the choice
--     (191.9ms against 196.3ms).
--   * ALTER COLUMN status SET STATISTICS 0 leaves the estimate at one row (318ms, 293ms).
--   * Reordering the lookup's ORDER BY, and dropping the claimable predicate from it,
--     change nothing.
--   * ANALYZE often enough to keep the estimate honest costs 1.26s to 1.59s per run on a
--     1,174MB table — more than the claims it would protect, and it cannot be scheduled
--     for the moment a burst arrives, which is the only moment it would help.
--   * Dropping idx_event_outbox_claim_order gives the right plan (1.967ms), and is not an
--     option: the age-ordered window scans and the legacy leg are what it is for.
--
-- Both functions are STABLE rather than IMMUTABLE — they read the table — and neither takes
-- a lock or writes anything. They are called from the claim only; nothing else in the
-- codebase depends on them, and dropping them makes the claim fail to execute rather than
-- silently regress, which is deliberate.

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.event_outbox_next_effective_key(after_key text)
RETURNS text
LANGUAGE sql
STABLE
SET enable_sort = 'off'
AS $fn$
    SELECT COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key))
    FROM blnk.event_outbox candidate
    WHERE candidate.status IN ('pending', 'processing')
      AND COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) > after_key
    ORDER BY COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) ASC
    LIMIT 1
$fn$;
-- +migrate StatementEnd

COMMENT ON FUNCTION blnk.event_outbox_next_effective_key(text) IS
'Next distinct effective partition key after the given cursor among unfinished event_outbox rows, or NULL at the end of the key space. Pinned to idx_event_outbox_effective_key_inflight by SET enable_sort = off; see migration 1781252500.';

-- +migrate StatementBegin
CREATE OR REPLACE FUNCTION blnk.event_outbox_key_head(key_value text)
RETURNS TABLE (id bigint, occurred_at timestamptz)
LANGUAGE sql
STABLE
SET enable_sort = 'off'
AS $fn$
    SELECT candidate.id, candidate.occurred_at
    FROM blnk.event_outbox candidate
    WHERE candidate.status IN ('pending', 'processing')
      AND COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) = key_value
    ORDER BY COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) ASC,
             candidate.occurred_at ASC, candidate.id ASC
    LIMIT 1
$fn$;
-- +migrate StatementEnd

COMMENT ON FUNCTION blnk.event_outbox_key_head(text) IS
'The oldest unfinished event_outbox row for one effective partition key — the head a claimant must take before any newer row of that key — or no row when the key has none. Returns the head whatever state it is in, so a leased head blocks the key rather than being skipped. Pinned to idx_event_outbox_effective_key_inflight by SET enable_sort = off; see migration 1781252500.';

-- +migrate Down

DROP FUNCTION IF EXISTS blnk.event_outbox_key_head(text);
DROP FUNCTION IF EXISTS blnk.event_outbox_next_effective_key(text);
