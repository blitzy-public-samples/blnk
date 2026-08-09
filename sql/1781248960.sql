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
-- # Why this table exists
--
-- Requirement R-2 puts every event in the same database transaction as the ledger
-- mutation that produced it. `balance.monitor` was the one event type that could not
-- honour that, and the reason was structural rather than incidental: a monitor fires
-- because a CONDITION was met on a balance a transaction has already committed, so by
-- the time the alert exists there is no open transaction left to enrol it in. The
-- capture was therefore a standalone insert taken after the commit, and a process that
-- died in the window between the two — or a database outage that outlasted a small
-- retry budget — destroyed the alert outright. The balance movement stood; the
-- low-balance or overdraft notification an operator relies on simply ceased to exist,
-- and nothing was left to replay because no row had ever been written.
--
-- A row in this table is written INSIDE the balance's own transaction. That is the
-- whole mechanism. The intent "these balances moved to this state, evaluate their
-- monitors" becomes durable at the same instant as the movement itself, so the two
-- cannot disagree: a committed balance always carries its pending evaluation, and a
-- rolled-back balance carries none.
--
-- The evaluation then happens outside that transaction, and its result — zero or more
-- `balance.monitor` event rows — is written in ONE transaction with this row's
-- transition to a terminal status. So the alert and the record that the evaluation is
-- finished commit together, and a crash anywhere in the sequence leaves the handoff
-- claimable rather than the alert lost. That is at-least-once evaluation feeding a
-- transactional capture, which is the strongest guarantee available for a condition
-- that can only be judged after its balance is durable.
--
-- # Why the balance is SNAPSHOTTED rather than re-read
--
-- balance_snapshot holds the balance exactly as the transaction wrote it. Re-reading
-- the balance at evaluation time would evaluate a DIFFERENT state — later transactions
-- may have moved it again — so a threshold that was crossed by this mutation and
-- uncrossed by the next would produce no alert at all, and a retry after a transient
-- failure could reach a different verdict than the attempt before it. The snapshot
-- makes the evaluation deterministic and makes it describe the movement it belongs to.
--
-- It is also what lets the evaluator run in a different process from the writer. The
-- row carries everything the condition needs.
--
-- # Why a row is written only when a monitor exists
--
-- The overwhelming majority of balances carry no monitor at all, and a handoff row per
-- balance per transaction would be pure write amplification on the money path — two
-- rows per transaction at the system's throughput target, every one of them destined to
-- be evaluated to "nothing fired" and deleted. The insert is therefore guarded by an
-- EXISTS against blnk.balance_monitors in the same statement, so a deployment with no
-- monitors configured writes nothing and pays one cheap indexed probe. See
-- insertBalanceMonitorHandoffsInTx.
--
-- That guard is only sound because a monitor cannot be created for a balance
-- retroactively: a monitor registered AFTER a movement was never intended to fire on
-- it, which is the behaviour the post-commit evaluation had as well, so nothing is lost
-- by deciding at write time.
--
-- # Index set
--
-- The columns and index shapes follow blnk.lineage_outbox and blnk.event_outbox rather
-- than inventing a third vocabulary: a unique business key, a partial index per polled
-- status, and a composite claim index restricted to the two statuses a claim can
-- select. The relay-polling shape is what keeps the claim query an index scan over the
-- pending rows alone instead of a scan of the whole table.
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
-- PostgreSQL does not index a foreign-key column automatically, so every lookup of
-- "the monitors for this balance" has always been a sequential scan. That was tolerable
-- while the lookup happened once per balance in a post-commit goroutine and was cached
-- for five minutes. It is not tolerable now: the EXISTS guard described above runs
-- INSIDE the money-path transaction, on every persistence of every transaction, and a
-- sequential scan there would put a table scan on the hot path and hold the balance
-- locks for its duration.
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
