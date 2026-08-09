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

-- blnk.bulk_transaction_batches: the durable coordinator record for an asynchronous
-- bulk transaction batch, and the row the batch's outcome event is atomic with.
--
-- # Why this table exists
--
-- `bulk_transaction.<status>` was the second event type that could not honour
-- requirement R-2, and for a different reason from `balance.monitor`. A bulk request is
-- executed one transaction at a time, each under its own database transaction, with
-- compensating void or refund as its rollback. There is no batch-spanning transaction,
-- so at the moment the batch's OUTCOME becomes known every mutation it describes has
-- already committed separately and there is no row the summary event could be atomic
-- with.
--
-- The consequence was that the outcome existed only in the local variables of the
-- goroutine that computed it. If the capture failed — a transient database error that
-- outlived a three-attempt budget, or a process that died — the summary was gone
-- permanently, and it was unreconstructable in any automatic way: the member
-- transactions were durable and carried the batch id, but whether the batch as a whole
-- had been declared applied, inflight or failed, and with what rollback detail, was
-- knowable only from a log line.
--
-- This table gives the outcome somewhere to be. A row is inserted with status
-- 'processing' BEFORE any member transaction runs, so the batch is durable and
-- enumerable from the moment it begins. When the outcome is known, ONE database
-- transaction moves this row to its terminal status AND inserts the outcome event into
-- blnk.event_outbox. Those two facts therefore commit together:
--
--   * the event can never be missing while the outcome is recorded, and
--   * the outcome can never be recorded while the event is missing.
--
-- A crash before that transaction leaves the row in 'processing' — which is not silent
-- loss but a visible, queryable, counted state that says "this batch began and never
-- reported an outcome". That is the honest residue, and it is a different thing
-- entirely from an outcome that was declared and then evaporated.
--
-- # Why the terminal transition is conditional
--
-- The finalising UPDATE is guarded on the row still being non-terminal. That makes the
-- whole finalise idempotent, which matters because it is retried: a transient failure
-- retries the SAME prepared event row, so a first attempt whose commit succeeded but
-- whose acknowledgement was lost is recognised on the second rather than producing a
-- second, differently-identified event for one batch outcome.
--
-- # Why the synchronous path has no row here
--
-- Only the asynchronous path emits `bulk_transaction.<status>`; the synchronous path
-- returns its outcome to the caller in the HTTP response and has always emitted no
-- event. A coordinator row there would have no event to be atomic with, and inventing
-- one would add an event type to the catalogue that no subscriber has ever received.
-- The sync path is therefore deliberately absent from this table, and that absence is
-- documented rather than incidental.
CREATE TABLE IF NOT EXISTS blnk.bulk_transaction_batches
(
    batch_id          TEXT PRIMARY KEY,
    status            TEXT        NOT NULL DEFAULT 'processing',
    transaction_count INT         NOT NULL DEFAULT 0,
    error_message     TEXT,
    atomic            BOOLEAN     NOT NULL DEFAULT FALSE,
    inflight          BOOLEAN     NOT NULL DEFAULT FALSE,
    event_id          TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finalized_at      TIMESTAMPTZ,
    CONSTRAINT bulk_transaction_batches_status_chk
        CHECK (status IN ('processing', 'applied', 'inflight', 'failed')),
    -- A terminal row must carry the instant it became terminal, and a non-terminal row
    -- must not. Without this a partially-applied finalise — status moved, timestamp
    -- not — would be representable, and the stuck-batch query below would then miss a
    -- batch that never finished or report one that did.
    CONSTRAINT bulk_transaction_batches_finalized_chk
        CHECK ((status = 'processing') = (finalized_at IS NULL))
);

-- The stuck-batch query: batches that began and never reported an outcome, oldest
-- first. This is the operational handle for the one window the coordinator cannot
-- close, so it is indexed rather than left to a scan — an operator reaches for it
-- precisely when the table is large.
CREATE INDEX IF NOT EXISTS idx_bulk_transaction_batches_unfinalized
    ON blnk.bulk_transaction_batches (created_at)
    WHERE status = 'processing';

-- Supports "which batches finished in this window, and how did they end", the query
-- behind both the outcome summary and a reconciliation of coordinator rows against
-- published outcome events.
CREATE INDEX IF NOT EXISTS idx_bulk_transaction_batches_finalized
    ON blnk.bulk_transaction_batches (status, finalized_at);

-- +migrate Down
DROP INDEX IF EXISTS blnk.idx_bulk_transaction_batches_finalized;
DROP INDEX IF EXISTS blnk.idx_bulk_transaction_batches_unfinalized;
DROP TABLE IF EXISTS blnk.bulk_transaction_batches;
