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

-- SERIALISE THE RELAY CLAIM ON THE KEY KAFKA ACTUALLY PARTITIONS BY.
--
-- The claim's earlier-same-key exclusion is the statement "at most one row per KAFKA
-- PARTITION KEY may be in flight at a time", and it is the only thing that makes
-- per-aggregate ordering survive more than one relay replica. It was written against
-- the partition_key COLUMN, and the column is not the key.
--
-- Kafka partitioning is by ledger id, so the publish path keys by ledger_id
-- wherever a row carries one and falls back to partition_key only where it does not:
-- model.EffectivePartitionKey, applied by model.EventOutbox.EffectiveKey, by the
-- publisher and by the relay's per-key grouping. The two values agree on almost every
-- row, because PrepareEventOutbox derives the partition key FROM the ledger when a
-- ledger is known — and they diverge on exactly the rows this schema permits them to:
--
--   * a row written before the ledger was threaded through its call site, whose
--     partition_key holds a source balance while ledger_id holds the ledger; and
--   * a row whose partition_key was derived from the payload before the ledger was
--     resolved.
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
-- partition_key equality — and that predicate no longer exists. Left in place it would
-- be a pure cost: every INSERT and every status transition on the hottest table in this
-- schema would maintain a second key it is never probed by, and a reader comparing the
-- two indexes would have to work out which of them the claim depends on.
DROP INDEX IF EXISTS blnk.idx_event_outbox_partition_key_inflight;

-- +migrate Down

-- Restore the column index exactly as sql/1781248800.sql declared it, so a rollback returns
-- the claim's old predicate to an indexed state rather than to a sequential scan.
CREATE INDEX IF NOT EXISTS idx_event_outbox_partition_key_inflight
    ON blnk.event_outbox (partition_key, occurred_at, id)
    WHERE status IN ('pending', 'processing');

DROP INDEX IF EXISTS blnk.idx_event_outbox_effective_key_inflight;
