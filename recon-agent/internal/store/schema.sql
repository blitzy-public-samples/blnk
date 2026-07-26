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
-- UPDATE, DELETE, or TRUNCATE).

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
-- source/upload_id/main_recon_id persist the break's durable provenance and
-- correlation identity (finding M-13): the external-statement source label, the
-- Blnk upload batch it arrived in, and the batch reconciliation run that
-- surfaced it. Persisting source in particular fixes the re-drive bug where the
-- rebuilt transaction carried an empty Source. resolved_recon_id records the
-- confirming Blnk dry-run reconciliation id when (and only when) the break is
-- auto-resolved; the agent_break_resolved_proof CHECK below makes an
-- auto-resolved row without that proof impossible (Rule 5.3, defense-in-depth
-- alongside the audit trail's own resolved-proof CHECK).
--
-- status_version is a monotonically-increasing optimistic-concurrency counter
-- bumped on every status transition, and claimed_by/lease_expires_at implement
-- a durable processing lease (finding M-15) so that at most one agent instance
-- processes a given break at a time — replacing reliance on a process-local
-- mutex, which cannot coordinate across instances.
CREATE TABLE IF NOT EXISTS agent.agent_break (
    external_txn_id   TEXT PRIMARY KEY,
    root_cause        TEXT NOT NULL,
    confidence        DOUBLE PRECISION NOT NULL DEFAULT 0,
    regulated         BOOLEAN NOT NULL DEFAULT FALSE,
    status            TEXT NOT NULL,
    proposed_rule     JSONB,
    rationale         TEXT NOT NULL DEFAULT '',
    created_rule_id   TEXT,
    amount            DOUBLE PRECISION,
    currency          TEXT,
    reference         TEXT,
    description       TEXT,
    txn_date          TIMESTAMPTZ,
    source            TEXT,
    upload_id         TEXT,
    main_recon_id     TEXT,
    resolved_recon_id TEXT,
    status_version    BIGINT NOT NULL DEFAULT 0,
    claimed_by        TEXT,
    lease_expires_at  TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
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
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS upload_id TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS main_recon_id TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS resolved_recon_id TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS status_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS claimed_by TEXT;
ALTER TABLE agent.agent_break ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- Domain / finiteness / resolved-proof integrity for agent_break (finding
-- M-13). CHECK constraints are added via guarded DO blocks because Postgres has
-- no "ADD CONSTRAINT IF NOT EXISTS": the block adds the constraint once and
-- swallows duplicate_object on every subsequent (idempotent) Migrate. The
-- confidence bound rejects NaN/±Inf as well as out-of-range values because in
-- Postgres NaN sorts greater than every value (so NaN <= 1 is false) and ±Inf
-- fails one of the bounds. The resolved-proof constraint (defined further below
-- via a DROP-then-ADD converge, because its definition grew in finding F16)
-- makes a CLEARED break — 'auto-resolved' or 're_driven' — without a confirming
-- reconciliation id impossible.
DO $agent_break_checks$
BEGIN
    ALTER TABLE agent.agent_break
        ADD CONSTRAINT agent_break_confidence_range
        CHECK (confidence >= 0 AND confidence <= 1);
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_break_checks$;

DO $agent_break_root_cause$
BEGIN
    ALTER TABLE agent.agent_break
        ADD CONSTRAINT agent_break_root_cause_domain
        CHECK (root_cause IN ('timing', 'amount_drift', 'reference_mismatch',
                              'duplicate', 'missing_internal', 'currency_mismatch', 'unknown'));
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_break_root_cause$;

DO $agent_break_status$
BEGIN
    ALTER TABLE agent.agent_break
        ADD CONSTRAINT agent_break_status_domain
        CHECK (status IN ('classified', 'auto-resolved', 'queued', 'accepted', 're_driven', 'rejected'));
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_break_status$;

-- Finding F16 extends the resolved-proof constraint to also require the
-- confirming reconciliation id on a 're_driven' break — a re_drive that Blnk
-- confirmed cleared, exactly like an 'auto-resolved' break (Rule 5.3,
-- defense-in-depth behind MarkReDrivenClearedFromQueuedTx, which only ever sets
-- 're_driven' together with a non-empty proof). It is (re)established via
-- DROP-then-ADD ... NOT VALID rather than the guarded ADD-EXCEPTION pattern the
-- other agent_break constraints use, for the same reason the audit action-domain
-- is: the constraint's definition GREW (from auto-resolved-only to also cover
-- re_driven), and a plain guarded ADD would silently keep the older, narrower
-- definition on any database migrated before it expanded. Dropping first makes
-- Migrate converge it to the CURRENT definition on every boot. NOT VALID skips
-- the existence scan of pre-existing rows, so Migrate can never fail on a legacy
-- re_driven row written before F16 (when the clearance proof was recorded only
-- in the audit event's provenance, not on the break row); the constraint is
-- still enforced on every future INSERT/UPDATE. This is safe under concurrency
-- because Migrate runs the whole Up section inside one transaction holding the
-- migration advisory lock.
ALTER TABLE agent.agent_break DROP CONSTRAINT IF EXISTS agent_break_resolved_proof;
ALTER TABLE agent.agent_break
    ADD CONSTRAINT agent_break_resolved_proof
    CHECK (status NOT IN ('auto-resolved', 're_driven')
           OR (resolved_recon_id IS NOT NULL AND length(btrim(resolved_recon_id)) > 0)) NOT VALID;

-- agent.agent_audit is the append-only action ledger (Rule 5.5). No UPDATE,
-- DELETE, or TRUNCATE statement may ever target this table; only INSERT and
-- SELECT.
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

-- Domain / evidence integrity for the append-only audit ledger (finding M-13),
-- enforced at the database boundary as defense-in-depth behind the application
-- validator in internal/audit. Each constraint is added via a guarded DO block
-- for idempotency (no "ADD CONSTRAINT IF NOT EXISTS" exists). Because agent_audit
-- is append-only (its trigger blocks UPDATE/DELETE) the constraints are added
-- NOT VALID: they are enforced on every future INSERT but the initial existence
-- scan of any pre-existing rows is skipped, so Migrate can never fail on legacy
-- data it is forbidden to rewrite. The action domain matches the closed set in
-- internal/audit; the resolved-proof constraint refuses any 'resolved' event
-- without a confirming reconciliation id in its provenance (Rule 5.3); the
-- rationale constraint requires a non-empty justification on every event.
-- The action domain is (re)established via DROP-then-ADD rather than the guarded
-- ADD ... EXCEPTION-WHEN-duplicate pattern used by the other constraints. The
-- closed action set grew over time (finding M-11 added 'rule_compensated' to
-- durably record orphaned-rule cleanup outcomes), and a plain guarded ADD would
-- silently keep a stale, narrower domain on any database migrated before the set
-- expanded. Dropping first makes Migrate converge the constraint to the CURRENT
-- domain on every boot. This is safe under concurrency because Migrate runs the
-- whole Up section inside one transaction holding the migration advisory lock, so
-- no two migrations execute this DDL at the same time; and it is still added
-- NOT VALID so the scan of any pre-existing rows is skipped (agent_audit is
-- append-only and must never be rewritten).
-- Finding F16 grew the closed set again with the finer-grained re_drive outcome
-- actions ('re_drive_attempted' / 're_drive_cleared' / 're_drive_unmatched' /
-- 're_drive_failed'), so the re_drive HANDLER records cleared / still-unmatched /
-- failed attempts as distinct immutable actions rather than one 're_driven'
-- value. 're_driven' is retained in the domain: it remains the break STATUS of a
-- confirmed-clearing re_drive and stays valid for any historical audit rows.
ALTER TABLE agent.agent_audit DROP CONSTRAINT IF EXISTS agent_audit_action_domain;
ALTER TABLE agent.agent_audit
    ADD CONSTRAINT agent_audit_action_domain
    CHECK (action IN ('classified', 'rule_proposed', 'rule_created', 'probed',
                      'resolved', 'escalated', 'accepted', 're_driven', 'rejected',
                      'rule_compensated',
                      're_drive_attempted', 're_drive_cleared', 're_drive_unmatched',
                      're_drive_failed')) NOT VALID;

DO $agent_audit_confidence$
BEGIN
    ALTER TABLE agent.agent_audit
        ADD CONSTRAINT agent_audit_confidence_range
        CHECK (confidence >= 0 AND confidence <= 1) NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_audit_confidence$;

DO $agent_audit_rationale$
BEGIN
    ALTER TABLE agent.agent_audit
        ADD CONSTRAINT agent_audit_rationale_nonempty
        CHECK (length(btrim(rationale)) > 0) NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_audit_rationale$;

DO $agent_audit_resolved_proof$
BEGIN
    ALTER TABLE agent.agent_audit
        ADD CONSTRAINT agent_audit_resolved_proof
        CHECK (action <> 'resolved'
               OR ((provenance ->> 'recon_id') IS NOT NULL
                   AND length(btrim(provenance ->> 'recon_id')) > 0)) NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_audit_resolved_proof$;

-- agent.agent_hitl_queue holds breaks awaiting a human decision (low
-- confidence, regulated, or fail-closed). It is drained by the HITL handlers.
CREATE TABLE IF NOT EXISTS agent.agent_hitl_queue (
    external_txn_id TEXT PRIMARY KEY,
    reason          TEXT NOT NULL,
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- agent.agent_rule_outbox is the durable compensation ledger for the one
-- external, non-transactional side effect the agent performs: creating a Blnk
-- matching rule while auto-remediating a break (findings M-12 / M-11). Creating
-- the rule is an HTTP POST that cannot participate in a database transaction, so
-- a crash in the window between "rule created in Blnk" and "break durably
-- resolved" would otherwise strand an orphaned rule the agent has forgotten. To
-- make compensation crash-safe, the remediator records a 'pending' outbox row in
-- the SAME transaction that persists the created rule id and its rule_created
-- audit event; it flips the row to 'confirmed' in the SAME transaction that
-- marks the break auto-resolved (the rule legitimately did its job), or — when
-- the attempt aborts — deletes the rule from Blnk and marks the row
-- 'compensated'. On startup the remediator scans 'pending' rows (whose owning
-- attempt never confirmed) and compensates them, recording last_error/attempts
-- and surfacing a persistent failure as 'compensation_failed'. Unlike
-- agent_audit this table is intentionally mutable (Rule 5.5 protects only the
-- audit ledger); every compensation OUTCOME is additionally written to the
-- append-only audit trail.
CREATE TABLE IF NOT EXISTS agent.agent_rule_outbox (
    id              BIGSERIAL PRIMARY KEY,
    external_txn_id TEXT NOT NULL,
    rule_id         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Constrain the outbox status to its closed lifecycle domain. Added via a
-- guarded DO block (Postgres lacks "ADD CONSTRAINT IF NOT EXISTS") so Migrate
-- stays idempotent.
DO $agent_rule_outbox_status$
BEGIN
    ALTER TABLE agent.agent_rule_outbox
        ADD CONSTRAINT agent_rule_outbox_status_domain
        CHECK (status IN ('pending', 'confirmed', 'compensated', 'compensation_failed'));
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_rule_outbox_status$;

-- agent.agent_run is the durable per-INVOCATION run ledger (findings M-15, F17).
-- It cleanly separates two identities that finding F17 showed must not be
-- conflated:
--
--   * run_id (PRIMARY KEY) — the INVOCATION identity. Every distinct pipeline
--     invocation gets its OWN row under its own run_id. This is what lets a
--     legitimate re-invocation of the SAME fixture (an operator re-running the
--     demo after applying HITL decisions, or re-running after restoring a failed
--     LLM) start a FRESH run, safely reprocess eligible work under a fresh
--     run-scoped id set, and report current-run-only counters — instead of being
--     silently refused as "already completed" and emitting a stale summary.
--
--   * fixture_key — the IMMUTABLE fixture identity: a deterministic content hash
--     of the ingested CSV plus its source label. It is recorded on every run for
--     provenance/observability (which fixture a run processed) and is
--     deliberately NON-UNIQUE: many runs may share one fixture over time.
--
-- BeginRun inserts a 'running' row under the caller's run_id; CompleteRun flips
-- THAT row (by run_id) to 'completed'. Idempotency is now scoped to the
-- invocation, not the fixture content: a retry WITHIN one invocation reuses the
-- same run_id (ON CONFLICT (run_id) DO NOTHING), so an in-process M-16 retry
-- converges on the one run rather than forking duplicate history; a genuinely
-- NEW invocation supplies a fresh run_id and therefore always gets a new run.
-- Because serve mode no longer processes the fixture on boot by default (finding
-- F01; gated behind AGENT_RUN_ON_BOOT), a plain container restart does not
-- reprocess anything, so the previous fixture-keyed dedup is no longer needed to
-- prevent restart duplication. The row is intentionally mutable (Rule 5.5
-- protects only agent_audit); every break state change it gates is still written
-- through the append-only audit trail.
CREATE TABLE IF NOT EXISTS agent.agent_run (
    run_id      TEXT PRIMARY KEY,
    fixture_key TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Look up all runs of a given fixture (provenance / operator inspection). Not
-- unique: finding F17 requires many invocations to share one fixture identity.
CREATE INDEX IF NOT EXISTS idx_agent_run_fixture_key ON agent.agent_run (fixture_key);

-- Constrain the run status to its closed lifecycle domain. Guarded DO block for
-- idempotency (Postgres lacks "ADD CONSTRAINT IF NOT EXISTS").
DO $agent_run_status$
BEGIN
    ALTER TABLE agent.agent_run
        ADD CONSTRAINT agent_run_status_domain
        CHECK (status IN ('running', 'completed'));
EXCEPTION WHEN duplicate_object THEN NULL;
END;
$agent_run_status$;

CREATE INDEX IF NOT EXISTS idx_agent_break_status ON agent.agent_break (status);
CREATE INDEX IF NOT EXISTS idx_agent_audit_external_txn_id ON agent.agent_audit (external_txn_id);
CREATE INDEX IF NOT EXISTS idx_agent_audit_timestamp ON agent.agent_audit ("timestamp");
-- Partial index over just the 'pending' outbox rows: startup compensation
-- recovery scans only this small, transient set, never the confirmed/compensated
-- history that accumulates over time.
CREATE INDEX IF NOT EXISTS idx_agent_rule_outbox_pending
    ON agent.agent_rule_outbox (created_at) WHERE status = 'pending';

-- Rule 5.5 (defense-in-depth): enforce the append-only invariant at the
-- database boundary, not merely by application convention. A BEFORE trigger
-- fires for EVERY role including the table owner and a superuser (unlike a
-- REVOKE, which superusers bypass), so no connection can mutate or remove a
-- recorded audit event. Two triggers share one reject function: a row-level
-- trigger raises on UPDATE or DELETE, and a statement-level trigger raises on
-- TRUNCATE (a distinct event that row-level triggers never fire for, so without
-- it the whole trail could be wiped in one statement despite the row-level
-- guard). INSERT and SELECT are unaffected, so the writer (INSERT) and status
-- page (SELECT) work normally.
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

-- TRUNCATE does not fire row-level triggers, so it needs a dedicated
-- STATEMENT-level BEFORE trigger (reusing the same reject function, which raises
-- for any TG_OP including 'TRUNCATE'). Guarded in a DO block that swallows
-- duplicate_object for the same idempotency/concurrency reasons as above.
DO $ensure_truncate_trigger$
BEGIN
    CREATE TRIGGER agent_audit_no_truncate
        BEFORE TRUNCATE ON agent.agent_audit
        FOR EACH STATEMENT EXECUTE FUNCTION agent.agent_audit_reject_mutation();
EXCEPTION
    WHEN duplicate_object THEN NULL;
END;
$ensure_truncate_trigger$;

-- +migrate Down
DROP TRIGGER IF EXISTS agent_audit_no_truncate ON agent.agent_audit;
DROP TRIGGER IF EXISTS agent_audit_no_mutate ON agent.agent_audit;
DROP FUNCTION IF EXISTS agent.agent_audit_reject_mutation();
DROP INDEX IF EXISTS agent.idx_agent_rule_outbox_pending;
DROP INDEX IF EXISTS agent.idx_agent_audit_timestamp;
DROP INDEX IF EXISTS agent.idx_agent_audit_external_txn_id;
DROP INDEX IF EXISTS agent.idx_agent_break_status;
DROP TABLE IF EXISTS agent.agent_run CASCADE;
DROP TABLE IF EXISTS agent.agent_rule_outbox CASCADE;
DROP TABLE IF EXISTS agent.agent_hitl_queue CASCADE;
DROP TABLE IF EXISTS agent.agent_audit CASCADE;
DROP TABLE IF EXISTS agent.agent_break CASCADE;
DROP SCHEMA IF EXISTS agent CASCADE;
