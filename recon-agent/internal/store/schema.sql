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
--
-- rationale carries the classifier's short natural-language justification so
-- the break row is self-describing on the HITL status page (the full rationale
-- is also captured on every audit event). created_rule_id records the id of the
-- Blnk matching rule the agent created while auto-remediating this break, so a
-- retry after a partial failure can reuse the already-created rule instead of
-- creating a duplicate one in Blnk (deterministic-arbiter side-effect safety).
-- The amount/currency/reference/description/txn_date columns persist the
-- matchable fields of the ORIGINAL external transaction alongside the classifier
-- verdict. A queued break is later re-driven from the HITL surface, and the
-- native Blnk HTTP contract exposes no route that lists an unmatched
-- transaction's fields (Rule 5.1 forbids reading Blnk-owned tables directly), so
-- the agent must remember the transaction it was managing in order to rebuild
-- the single-transaction dry-run for a re_drive. They are nullable so a
-- previously-migrated database upgrades cleanly (finding L2).
CREATE TABLE IF NOT EXISTS agent.agent_break (
    external_txn_id TEXT PRIMARY KEY,
    root_cause      TEXT NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL DEFAULT 0,
    regulated       BOOLEAN NOT NULL DEFAULT FALSE,
    status          TEXT NOT NULL,
    proposed_rule   JSONB,
    rationale       TEXT NOT NULL DEFAULT '',
    created_rule_id TEXT,
    amount          DOUBLE PRECISION,
    currency        TEXT,
    reference       TEXT,
    description     TEXT,
    txn_date        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Upgrade path: add the newer columns to a pre-existing agent_break table.
-- ADD COLUMN IF NOT EXISTS is a no-op when the column is already present, so
-- Migrate stays idempotent on both fresh and previously-migrated databases.
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS rationale TEXT NOT NULL DEFAULT '';
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS created_rule_id TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS amount DOUBLE PRECISION;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS currency TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS reference TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS txn_date TIMESTAMPTZ;

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

-- Rule 5.5 (defense-in-depth): enforce the append-only invariant at the
-- database boundary, not merely by application convention. A BEFORE trigger
-- fires for EVERY role including the table owner and a superuser (unlike a
-- REVOKE, which superusers bypass), so no connection can mutate or remove a
-- recorded audit event. The trigger raises on any row-level UPDATE or DELETE;
-- INSERT and SELECT are unaffected, so the writer (INSERT) and status page
-- (SELECT) work normally.
CREATE OR REPLACE FUNCTION agent.agent_audit_reject_mutation() RETURNS trigger AS $reject$
BEGIN
    RAISE EXCEPTION 'agent.agent_audit is append-only (Rule 5.5); % is not permitted', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$reject$ LANGUAGE plpgsql;

-- CREATE TRIGGER has no IF NOT EXISTS form, so guard it in a DO block that
-- swallows duplicate_object. This keeps Migrate idempotent and safe under
-- concurrent invocations (the losing racer catches the duplicate and no-ops).
DO $ensure_trigger$
BEGIN
    CREATE TRIGGER agent_audit_no_mutate
        BEFORE UPDATE OR DELETE ON agent.agent_audit
        FOR EACH ROW EXECUTE FUNCTION agent.agent_audit_reject_mutation();
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$ensure_trigger$;

-- +migrate Down
DROP TRIGGER IF EXISTS agent_audit_no_mutate ON agent.agent_audit;
DROP FUNCTION IF EXISTS agent.agent_audit_reject_mutation();
DROP INDEX IF EXISTS agent.idx_agent_audit_timestamp;
DROP INDEX IF EXISTS agent.idx_agent_audit_external_txn_id;
DROP INDEX IF EXISTS agent.idx_agent_break_status;
DROP TABLE IF EXISTS agent.agent_hitl_queue CASCADE;
DROP TABLE IF EXISTS agent.agent_audit CASCADE;
DROP TABLE IF EXISTS agent.agent_break CASCADE;
DROP SCHEMA IF EXISTS agent CASCADE;
