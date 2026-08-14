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
-- 500 events per second blnk.event_outbox gains 43.2 million rows a day, so a statement
-- whose cost tracks the table size is one that works in staging and degrades without
-- bound in production — and the readings affected are exactly the ones an operator
-- reaches for when something is already wrong: the status counts, the zero-loss
-- reconciliation, the dead-letter inventory and the dead-letter age gauge the 15-minute
-- alert fires from.
--
-- All five are additive. No column, constraint or existing index is altered, so this
-- migration is safe to apply to a database already carrying 1781248800.sql,
-- 1781248900.sql, 1781248910.sql and 1781248920.sql.

-- The NON-DELIVERED working set, counted exactly.
--
-- This index is what makes the exact half cheap. The population it covers is bounded BY
-- OPERATION, not by time: it is the working set plus the dead-letter inventory.
CREATE INDEX IF NOT EXISTS idx_event_outbox_status_open
    ON blnk.event_outbox (status)
    WHERE status <> 'dispatched';

-- The CLAIM's candidate order, and why this index is REQUIRED rather than an extra.
--
-- idx_event_outbox_status_open leads on `status`, so PostgreSQL can also use it to
-- satisfy the relay claim's `status IN ('pending','processing','webhook_pending')` —
-- and measured on a 300,000-row table, it does. That is a REGRESSION, because this
-- index's partial set deliberately includes 'failed' and 'dead_lettered': the terminal
-- rows an operator has not triaged yet.
CREATE INDEX IF NOT EXISTS idx_event_outbox_claim_order
    ON blnk.event_outbox (occurred_at, id)
    WHERE status IN ('pending', 'processing', 'webhook_pending');

-- DELIVERED rows, by occurrence, so the windowed half of the same aggregate is a range
-- scan.
--
-- occurred_at rather than dispatched_at because occurred_at is NOT NULL and is the
-- column the statistics window is expressed in — an operator asking "how many events
-- from today" is asking about when they happened, not about when the relay got to them.
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
CREATE INDEX IF NOT EXISTS idx_event_outbox_dead_lettered_attempted
    ON blnk.event_outbox (last_attempted_at)
    WHERE status = 'dead_lettered';

-- The DEAD-LETTER INVENTORY, keyed for filtering and keyset paging in one index.
--
-- ListDeadLetterInventory applies its filters in SQL and pages by keyset rather than by
-- offset, and this is the index both of those depend on. The leading columns are the
-- ordering key — (occurred_at DESC, id DESC) — because that is what the cursor
-- predicate `(occurred_at, id) < ($cursor_instant, $cursor_id)` ranges over, and a
-- keyset page is only constant-cost when the ordering key leads.
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
