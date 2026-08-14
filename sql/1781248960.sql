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

-- blnk.balance_monitor_handoff: the durable intent that a balance moved and its
-- monitors have not been evaluated yet.
--
-- The durability contract puts every event in the same database transaction as the ledger
-- mutation that produced it. `balance.monitor` was the one event type that could not
-- honour that, and the reason was structural rather than incidental: a monitor fires
-- because a CONDITION was met on a balance a transaction has already committed, so by
-- the time the alert exists there is no open transaction left to enrol it in.
--
-- A row in this table is written INSIDE the balance's own transaction. That is the
-- whole mechanism.
--
-- It is also what lets the evaluator run in a different process from the writer. The
-- row carries everything the condition needs.
CREATE TABLE IF NOT EXISTS blnk.balance_monitor_handoff
(
    id               BIGSERIAL PRIMARY KEY,
    handoff_id       TEXT        NOT NULL,
    balance_id       TEXT        NOT NULL,
    ledger_id        TEXT,
    balance_snapshot JSONB       NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending',
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,
    last_error       TEXT,
    events_captured  INT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ,
    locked_until     TIMESTAMPTZ,
    CONSTRAINT balance_monitor_handoff_status_chk
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    CONSTRAINT balance_monitor_handoff_balance_chk
        CHECK (btrim(balance_id) <> '')
);

-- The business key. It is generated per handoff rather than derived from the balance,
-- because one balance legitimately produces many handoffs — one per movement — and a
-- derived key would collapse them into the first.
CREATE UNIQUE INDEX IF NOT EXISTS idx_balance_monitor_handoff_handoff_id
    ON blnk.balance_monitor_handoff (handoff_id);

-- The polled statuses, each with its own partial index so the poll never touches a
-- completed row. Completed rows are the overwhelming majority in steady state.
CREATE INDEX IF NOT EXISTS idx_balance_monitor_handoff_pending
    ON blnk.balance_monitor_handoff (status, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_balance_monitor_handoff_failed
    ON blnk.balance_monitor_handoff (status, created_at)
    WHERE status = 'failed';

-- The claim index. Its column order matches the claim query's predicate order —
-- status, then lease expiry, then the attempt budget, then FIFO — so the CTE that
-- selects a batch FOR UPDATE SKIP LOCKED resolves entirely from the index.
CREATE INDEX IF NOT EXISTS idx_balance_monitor_handoff_claim
    ON blnk.balance_monitor_handoff (status, locked_until, attempts, created_at)
    WHERE status IN ('pending', 'processing');

-- Supports the operational question "what is outstanding for this balance", which is
-- the first thing asked when an expected alert did not arrive.
CREATE INDEX IF NOT EXISTS idx_balance_monitor_handoff_balance
    ON blnk.balance_monitor_handoff (balance_id, created_at);

-- An index on blnk.balance_monitors(balance_id), which has never existed.
--
-- PostgreSQL does not index a foreign-key column automatically, so every lookup of "the
-- monitors for this balance" has always been a sequential scan. That was tolerable
-- while the lookup happened once per balance in a post-commit goroutine and was cached
-- for five minutes.
--
-- This is an additive, idempotent index on an existing table. It changes no monitor
-- semantics, no constraint and no column; it makes an existing access pattern use an
-- index instead of a scan, and it is what makes the guard cheap enough to sit where it
-- has to sit.
CREATE INDEX IF NOT EXISTS idx_balance_monitors_balance_id
    ON blnk.balance_monitors (balance_id);

-- +migrate Down
--
-- Reverse order of the Up section above, and ONE Down section rather than two. The file
-- carried a second `-- +migrate Up` and a second `-- +migrate Down`, which is how two
-- separately written migrations come to share one timestamped file: sql-migrate toggles
-- direction on each marker so the statements did run in the right order, but a reader
-- checking that a file's Down reverses its Up had two of each to reconcile.
DROP INDEX IF EXISTS blnk.idx_balance_monitors_balance_id;
DROP INDEX IF EXISTS blnk.idx_balance_monitor_handoff_balance;
DROP INDEX IF EXISTS blnk.idx_balance_monitor_handoff_claim;
DROP INDEX IF EXISTS blnk.idx_balance_monitor_handoff_failed;
DROP INDEX IF EXISTS blnk.idx_balance_monitor_handoff_pending;
DROP INDEX IF EXISTS blnk.idx_balance_monitor_handoff_handoff_id;
DROP TABLE IF EXISTS blnk.balance_monitor_handoff;
