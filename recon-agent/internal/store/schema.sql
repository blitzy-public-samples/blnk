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

-- schema.sql defines recon-agent's OWN persistence, created in a dedicated
-- "agent" schema inside the shared blnk database. It follows Blnk's migrate
-- file convention (see sql/1721922047.sql) but is applied INDEPENDENTLY by
-- store.Migrate at boot. Only the Up section is ever executed by the agent;
-- the Down section documents the teardown and is never run in normal operation.
--
-- Rule 5.1: every object lives under the agent schema; the agent never reads or
-- writes any blnk.* table.
-- Rule 5.5: agent.agent_audit is append-only (INSERT and SELECT only; never
-- UPDATE or DELETE).

-- +migrate Up
CREATE SCHEMA IF NOT EXISTS agent;

-- agent.agent_break holds one row per external break under management.
CREATE TABLE IF NOT EXISTS agent.agent_break (
    external_txn_id TEXT PRIMARY KEY,
    root_cause      TEXT NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL DEFAULT 0,
    regulated       BOOLEAN NOT NULL DEFAULT FALSE,
    status          TEXT NOT NULL,
    proposed_rule   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- agent.agent_audit is the append-only action ledger (Rule 5.5). No UPDATE or
-- DELETE statement may ever target this table; only INSERT and SELECT.
CREATE TABLE IF NOT EXISTS agent.agent_audit (
    event_id        UUID PRIMARY KEY,
    external_txn_id TEXT NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    "timestamp"     TIMESTAMPTZ NOT NULL,
    rationale       TEXT NOT NULL DEFAULT '',
    confidence      DOUBLE PRECISION NOT NULL DEFAULT 0,
    provenance      JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- agent.agent_hitl_queue holds breaks awaiting a human decision (low
-- confidence, regulated, or fail-closed). It is drained by the HITL handlers.
CREATE TABLE IF NOT EXISTS agent.agent_hitl_queue (
    external_txn_id TEXT PRIMARY KEY,
    reason          TEXT NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_break_status ON agent.agent_break (status);
CREATE INDEX IF NOT EXISTS idx_agent_audit_external_txn_id ON agent.agent_audit (external_txn_id);
CREATE INDEX IF NOT EXISTS idx_agent_audit_timestamp ON agent.agent_audit ("timestamp");

-- +migrate Down
DROP INDEX IF EXISTS agent.idx_agent_audit_timestamp;
DROP INDEX IF EXISTS agent.idx_agent_audit_external_txn_id;
DROP INDEX IF EXISTS agent.idx_agent_break_status;
DROP TABLE IF EXISTS agent.agent_hitl_queue CASCADE;
DROP TABLE IF EXISTS agent.agent_audit CASCADE;
DROP TABLE IF EXISTS agent.agent_break CASCADE;
DROP SCHEMA IF EXISTS agent CASCADE;
