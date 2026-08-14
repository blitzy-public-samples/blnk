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

-- CLOSE THE HOLE IN THE KEY-HEAD PIN: MAKE THE AGE-ORDERED PLAN UNUSABLE RATHER THAN
-- MERELY DEARER.
--
-- sql/1781252500.sql pinned the claim's two key-space lookups to
-- idx_event_outbox_effective_key_inflight by giving each function SET enable_sort = 'off'
-- and phrasing its ORDER BY in that index's own column order, so the age-ordered
-- alternative would have to sort and sorting was switched off. That reasoning holds for
-- blnk.event_outbox_next_effective_key. It does NOT hold for blnk.event_outbox_key_head,
-- and this migration is the correction.
--
-- WHY IT DID NOT HOLD. The head lookup selects one key with an EQUALITY clause:
--
--     WHERE effective_key = key_value
--     ORDER BY effective_key, occurred_at, id
--
-- An equality against a constant puts that expression into an equivalence class, and
-- PostgreSQL then treats the first ORDER BY column as CONSTANT: any plan that returns rows
-- in (occurred_at, id) order already satisfies the whole ORDER BY, with no sort node.
-- idx_event_outbox_claim_order is exactly such a plan. enable_sort = off therefore
-- excluded nothing, and at a one-row estimate for the claimable population — which is what
-- ANALYZE reports for most of an outbox's life, because the claimable set is zero for long
-- stretches — the age-ordered scan is costed 6.04 against the seek's 8.38 and wins. The key
-- becomes a FILTER, and the lookup reads the claimable population to answer a question
-- about one key.
--
-- Measured on this schema, empty table with fresh statistics, enable_sort = off in force:
--
--     = key_value                Index Scan using idx_event_outbox_claim_order
--                                  Filter: (... = 'probe'::text)          cost 0.12..5.91
--     >= key_value AND <= key_value
--                                Index Scan using idx_event_outbox_effective_key_inflight
--                                  Index Cond: (... >= 'probe' AND ... <= 'probe')
--                                                                         cost 0.12..7.99
--
-- And on 25,000 claimable rows across 250 keys, EXPLAIN (ANALYZE, BUFFERS):
-- both forms reach the effective-key index there, at 3 shared buffers, so the range form
-- costs nothing in the state where the old form was already correct.
--
-- WHY A RANGE INSTEAD OF AN EQUALITY. `>= x AND <= x` selects exactly the rows `= x`
-- selects, for every value including NULL (all three comparisons are NULL, so no row
-- qualifies either way). What it does not do is create an equivalence class: with no
-- equality clause the planner cannot treat the leading ORDER BY column as constant, so the
-- ordering must come from an index that actually provides it — and
-- idx_event_outbox_effective_key_inflight is the only one that does. The age-ordered plan
-- would need a sort, and sorting is off. The plan is removed from consideration rather than
-- out-priced, which is what "pinned" was meant to mean.
--
-- The SET enable_sort = 'off' stays and is still load-bearing: without it the planner is
-- free to sort, and on an empty table it chooses a sequential scan plus a sort over either
-- index. The two mechanisms together are what make the correct plan the only representable
-- one.
--
-- Nothing else about the function changes: same signature, same returned columns, same
-- rows, same STABLE volatility, no lock, no write. The claim calls it through
-- CROSS JOIN LATERAL and is unaffected. blnk.event_outbox_next_effective_key is left
-- exactly as 1781252500 created it — it carries no equality clause, so it never had this
-- hole, and its plan was verified unchanged after this migration.

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
      AND COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) >= key_value
      AND COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) <= key_value
    ORDER BY COALESCE(NULLIF(btrim(candidate.ledger_id), ''), btrim(candidate.partition_key)) ASC,
             candidate.occurred_at ASC, candidate.id ASC
    LIMIT 1
$fn$;
-- +migrate StatementEnd

COMMENT ON FUNCTION blnk.event_outbox_key_head(text) IS
'The oldest unfinished event_outbox row for one effective partition key — the head a claimant must take before any newer row of that key — or no row when the key has none. Returns the head whatever state it is in, so a leased head blocks the key rather than being skipped. The key is selected by a RANGE rather than an equality so no equivalence class forms and the leading ORDER BY column cannot be folded to a constant; with SET enable_sort = off that leaves idx_event_outbox_effective_key_inflight as the only plan able to satisfy the ordering. See migration 1781252600.';

-- +migrate Down

-- Restore the equality form 1781252500 created. This reopens the mis-plan described above:
-- the head lookup becomes free to read the claimable population per key, and the claim's
-- cost then follows the backlog it is draining.
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
