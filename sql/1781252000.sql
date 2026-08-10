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

-- SERIALISE THE RELAY CLAIM ON THE KEY KAFKA ACTUALLY PARTITIONS BY (PERF-C02).
--
-- # The defect this closes
--
-- The claim's earlier-same-key exclusion is the statement "at most one row per KAFKA
-- PARTITION KEY may be in flight at a time", and it is the only thing that makes
-- per-aggregate ordering survive more than one relay replica. It was written against the
-- partition_key COLUMN, and the column is not the key.
--
-- Requirement R-6 partitions by ledger id, so the publish path keys by ledger_id wherever a
-- row carries one and falls back to partition_key only where it does not:
-- model.EffectivePartitionKey, applied by model.EventOutbox.EffectiveKey, by the publisher
-- and by the relay's per-key grouping. The two values agree on almost every row, because
-- PrepareEventOutbox derives the partition key FROM the ledger when a ledger is known — and
-- they diverge on exactly the rows this schema permits them to:
--
--   * a row written before the ledger was threaded through its call site, whose
--     partition_key holds a source balance while ledger_id holds the ledger; and
--   * a row whose partition_key was derived from the payload before the ledger was
--     resolved.
--
-- Two such rows sharing ONE ledger have DIFFERENT stored partition keys. The old exclusion
-- therefore did not hold them apart, so two relay replicas could claim them in the same
-- instant — while the publisher hashed both to ONE Kafka partition, because both key on the
-- ledger. Whichever replica's write was appended first won, and a subscriber then observed
-- the later event before the earlier one for the same aggregate. Silently: no error, no log
-- line, no row state, and the guarantee the whole partitioning scheme exists to provide
-- gone on precisely the rows an operator would later be investigating.
--
-- # What replaces it
--
-- The claim now compares the EFFECTIVE key on both sides of the anti-join, and this index is
-- what keeps that comparison an index probe rather than a scan of the key's history. It is
-- an EXPRESSION index, and the expression is the exact SQL rendering of the Go rule:
--
--     COALESCE(NULLIF(btrim(ledger_id), ''), btrim(partition_key))
--
-- Term for term: btrim is strings.TrimSpace over SQL whitespace, NULLIF turns a blank
-- ledger into NULL so COALESCE falls through to the partition key, and a NULL ledger falls
-- through directly. Every function in it is IMMUTABLE, which is what makes the index legal.
--
-- The result can never be NULL, and that matters more than it looks: two CHECK constraints
-- from sql/1781248800.sql — event_outbox_partition_key_not_blank and
-- event_outbox_ledger_id_not_blank_when_present — guarantee a non-blank fallback on every
-- row. An expression that could yield NULL would make the anti-join's equality NULL rather
-- than true, and a NOT EXISTS over a never-true predicate excludes nothing at all: the
-- serialisation would appear to be in place and hold nothing back.
--
-- THE QUERY AND THIS INDEX MUST SPELL THE EXPRESSION IDENTICALLY. A partial expression
-- index is usable only when the query's expression matches it, so a difference here does
-- not produce a wrong answer — it produces a sequential scan of the whole table on every
-- poll, which is the same outage by a slower route. database/event_outbox.go builds both the
-- predicate and this expression from one function, eventOutboxEffectiveKeySQL, so there is
-- exactly one spelling to keep.
--
-- The partial predicate is the BLOCKING set and is deliberately narrower than the claimable
-- set, unchanged from the index it replaces: only pending and processing rows hold a key
-- back. A row that has spent its retry budget (failed), one preserved on its dead-letter
-- topic (dead_lettered), and one that owes only a legacy webhook enqueue (webhook_pending)
-- do NOT block their key — the first two because one permanently undeliverable event would
-- otherwise stall its aggregate for ever, and the third because its position in the Kafka
-- partition is already fixed so nothing it does subsequently can reorder anything.
--
-- The trailing (occurred_at, id) are the columns the exclusion's tuple comparison reads, so
-- the probe is satisfied from the index alone.
CREATE INDEX IF NOT EXISTS idx_event_outbox_effective_key_inflight
    ON blnk.event_outbox (
        (COALESCE(NULLIF(btrim(ledger_id), ''), btrim(partition_key))),
        occurred_at,
        id
    )
    WHERE status IN ('pending', 'processing');

-- AND THE COLUMN INDEX GOES, because nothing reads it any more.
--
-- idx_event_outbox_partition_key_inflight existed for one predicate — the old
-- partition_key equality — and that predicate no longer exists. Left in place it would be a
-- pure cost: every INSERT and every status transition on the hottest table in this schema
-- would maintain a second key it is never probed by, and a reader comparing the two indexes
-- would have to work out which of them the claim depends on. Dropped, the answer is the one
-- above.
--
-- Ordered AFTER the CREATE deliberately. The two are applied in one transaction by
-- sql-migrate, so the ordering is not about a window in which neither exists; it is so that
-- a migration halted between statements leaves the new index present rather than the table
-- with neither.
DROP INDEX IF EXISTS blnk.idx_event_outbox_partition_key_inflight;

-- +migrate Down

-- Restore the column index exactly as sql/1781248800.sql declared it, so a rollback returns
-- the claim's old predicate to an indexed state rather than to a sequential scan.
CREATE INDEX IF NOT EXISTS idx_event_outbox_partition_key_inflight
    ON blnk.event_outbox (partition_key, occurred_at, id)
    WHERE status IN ('pending', 'processing');

DROP INDEX IF EXISTS blnk.idx_event_outbox_effective_key_inflight;
