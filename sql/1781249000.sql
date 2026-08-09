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

-- READ-PATH indexes for blnk.event_outbox and blnk.event_subscribers.
--
-- Every index here exists because a read that operators and collectors run on a
-- schedule was O(the whole history) rather than O(the answer). At the target rate of
-- 500 events per second blnk.event_outbox gains 43.2 million rows a day, so a
-- statement whose cost tracks the table size is one that works in staging and
-- degrades without bound in production — and the readings affected are exactly the
-- ones an operator reaches for when something is already wrong: the status counts,
-- the zero-loss reconciliation, the dead-letter inventory and the dead-letter age
-- gauge the 15-minute alert fires from.
--
-- All five are additive. No column, constraint or existing index is altered, so this
-- migration is safe to apply to a database already carrying 1781248800.sql,
-- 1781248900.sql, 1781248910.sql and 1781248920.sql. No status literal is
-- introduced: the event_outbox_status_known CHECK constraint enumerates the state
-- machine and stays exactly as it is, and every predicate below is carved out of
-- states that already exist.

-- The NON-DELIVERED working set, counted exactly.
--
-- CountEventOutboxByStatus splits its aggregate in two: every status except
-- 'dispatched' is counted exactly and completely, and only 'dispatched' is bounded by
-- the caller's window. The split follows the cost rather than the calendar, because
-- the counts an operator acts on — the pending backlog, the rows in flight, the
-- dead-letter inventory — can each legitimately be older than any window you would
-- pick, and a pending row stuck for three days must not vanish from a one-day reading
-- of the backlog.
--
-- This index is what makes the exact half cheap. The population it covers is bounded
-- BY OPERATION, not by time: it is the working set plus the dead-letter inventory,
-- which acceptance criterion V-3 holds below 0.1% of throughput. Indexing status
-- alone, restricted to that population, gives PostgreSQL an index-only scan over just
-- those rows — so the aggregate reads thousands of index entries instead of tens of
-- millions of heap tuples.
--
-- The predicate must be spelled exactly as the query spells it. A partial index is
-- usable only when the query's WHERE clause implies the index's predicate, so
-- `status <> 'dispatched'` here and `status <> $1` bound to 'dispatched' there is the
-- pairing; writing this as a NOT IN over the terminal states would leave the query
-- unable to use it.
CREATE INDEX IF NOT EXISTS idx_event_outbox_status_open
    ON blnk.event_outbox (status)
    WHERE status <> 'dispatched';

-- The CLAIM's candidate order, and why this index is REQUIRED rather than an extra.
--
-- idx_event_outbox_status_open leads on `status`, so PostgreSQL can also use it to
-- satisfy the relay claim's `status IN ('pending','processing','webhook_pending')` — and
-- measured on a 300,000-row table, it does. That is a REGRESSION, because this index's
-- partial set deliberately includes 'failed' and 'dead_lettered': the terminal rows an
-- operator has not triaged yet. Reading them on every poll makes the claim's cost grow
-- with the dead-letter inventory, which is precisely the property the claim index exists
-- to prevent. Measured, with 150,000 dead-lettered rows present: the candidate scan's
-- estimated cost rose from 25.72 to 115.26 and gained a Sort.
--
-- This index removes the choice by being strictly better. Keyed on (occurred_at, id) —
-- the claim's own ORDER BY — over a partial set that is EXACTLY the blocking states, it
-- returns candidates already ordered, so the planner stops at the LIMIT instead of
-- sorting the whole candidate set, and never reads a terminal row at all. Same
-- measurement, with this index present: cost 8.16, no Sort, and the plan is chosen at
-- both 300,000 rows and 150,000 dead-lettered rows.
--
-- It complements idx_event_outbox_claim rather than replacing it. That index leads on
-- the state and the lease, which is what the anti-join's build side wants; this one leads
-- on the order, which is what the LIMIT wants. Both are partial on the same blocking
-- set, so under either plan the cold history is excluded from the index entirely — the
-- guarantee TestClaimPendingEventOutbox_UsesClaimIndex_RealDB asserts.
CREATE INDEX IF NOT EXISTS idx_event_outbox_claim_order
    ON blnk.event_outbox (occurred_at, id)
    WHERE status IN ('pending', 'processing', 'webhook_pending');

-- DELIVERED rows, by occurrence, so the windowed half of the same aggregate is a
-- range scan.
--
-- idx_event_outbox_terminal_retention already indexes occurred_at for the retention
-- sweep, but it covers 'dispatched' and 'dead_lettered' TOGETHER: a count restricted
-- to 'dispatched' would have to read and discard every dead-lettered entry in the
-- range, and the retention sweep's predicate is the one that index exists to serve.
-- One status per index keeps both statements index-only.
--
-- occurred_at rather than dispatched_at because occurred_at is NOT NULL and is the
-- column the statistics window is expressed in — an operator asking "how many events
-- from today" is asking about when they happened, not about when the relay got to
-- them.
CREATE INDEX IF NOT EXISTS idx_event_outbox_dispatched_occurred
    ON blnk.event_outbox (occurred_at)
    WHERE status = 'dispatched';

-- DEAD-LETTERED rows by their hand-off instant, for the windowed reconciliation.
--
-- AuditTerminalEventRecords bounds its population by publication instant so that the
-- outbox side and the broker side describe the same window. A row published to its
-- category topic carries kafka_dispatched_at and is reached through
-- idx_event_outbox_published_audit, which already indexes that column and INCLUDEs the
-- whole broker coordinate.
--
-- A DEAD-LETTERED row has no kafka_dispatched_at: its record is on the `<topic>.dlt`
-- sibling and was written by the hand-off, whose instant the row records as
-- last_attempted_at. That arm needs its own index or the audit degrades to a scan
-- exactly as it did before — and dropping the arm instead is not an option, because
-- excluding dead-lettered rows understates the outbox side and manufactures an
-- apparent surplus in the direction that hides loss.
--
-- The same index serves the dead-letter age aggregate's MIN over
-- COALESCE(last_attempted_at, occurred_at) for preserved rows.
CREATE INDEX IF NOT EXISTS idx_event_outbox_dead_lettered_attempted
    ON blnk.event_outbox (last_attempted_at)
    WHERE status = 'dead_lettered';

-- The DEAD-LETTER INVENTORY, keyed for filtering and keyset paging in one index.
--
-- ListDeadLetterInventory applies its filters in SQL and pages by keyset rather than
-- by offset, and this is the index both of those depend on. The leading columns are
-- the ordering key — (occurred_at DESC, id DESC) — because that is what the cursor
-- predicate `(occurred_at, id) < ($cursor_instant, $cursor_id)` ranges over, and a
-- keyset page is only constant-cost when the ordering key leads.
--
-- event_type and topic are INCLUDEd rather than indexed. They are equality filters
-- whose selectivity varies from "one of thirteen" to "everything", so leading with
-- them would help one shape of request and force a sort on every other; carrying them
-- as payload lets an unfiltered page stay index-only and a filtered one apply its
-- predicate without a heap fetch per candidate row.
--
-- idx_event_outbox_failed covers the same two statuses on (status, occurred_at), but
-- leads with status: a listing that admits BOTH terminal states — which is the default,
-- because a row that spent its retries and whose dead-letter write also failed is the
-- one most in need of attention — cannot get a single ordered range out of it and has
-- to merge and sort two. That index stays: the per-status count above reads it.
CREATE INDEX IF NOT EXISTS idx_event_outbox_dead_letter_inventory
    ON blnk.event_outbox (occurred_at DESC, id DESC)
    INCLUDE (event_type, topic)
    WHERE status IN ('failed', 'dead_lettered');

-- OUTSTANDING subscriber revocations.
--
-- Deregistration revokes the credential at the broker and only then deletes the
-- registry row, so a row still carrying revocation_pending_at names a principal that
-- may still be able to authenticate while nothing in Blnk records an issuance for it.
-- The metrics collector reads the count and the oldest instant every collection
-- interval, and the alert on that age is the only thing that surfaces the debt.
--
-- Unindexed, that COUNT and MIN was a sequential scan of the whole registry to answer
-- a question whose healthy answer is zero — the pathological shape for a periodic
-- reading. Indexing the column and restricting the index to non-NULL values makes the
-- index hold only the rows that actually owe something, so the aggregate reads nothing
-- at all in the healthy case and the answer arrives from an index whose size is the
-- size of the backlog rather than of the registry.
--
-- The MIN comes from the same index without a sort, because a b-tree is ordered.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_revocation_pending
    ON blnk.event_subscribers (revocation_pending_at)
    WHERE revocation_pending_at IS NOT NULL;

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_event_subscribers_revocation_pending;
DROP INDEX IF EXISTS blnk.idx_event_outbox_dead_letter_inventory;
DROP INDEX IF EXISTS blnk.idx_event_outbox_dead_lettered_attempted;
DROP INDEX IF EXISTS blnk.idx_event_outbox_dispatched_occurred;
DROP INDEX IF EXISTS blnk.idx_event_outbox_claim_order;
DROP INDEX IF EXISTS blnk.idx_event_outbox_status_open;
