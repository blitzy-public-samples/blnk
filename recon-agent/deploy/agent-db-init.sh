#!/bin/sh
# Copyright 2024 Blnk Finance Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# agent-db-init.sh — provisions the PostgreSQL roles the recon-agent uses
# (findings M-13 and F05).
#
# WHY THIS EXISTS: the recon-agent must NOT connect as the shared Postgres
# superuser. If the agent (or its DB connection) were compromised, a superuser
# connection would defeat the intended schema boundary and expose every blnk.*
# table. This script bounds the runtime blast radius to the agent's own additive
# `agent` schema.
#
# TWO-ROLE LEAST PRIVILEGE (finding F05): when AGENT_MIGRATOR_USER and
# AGENT_MIGRATOR_PASSWORD are provided (the shipped docker-compose default), this
# provisions TWO roles with a strict privilege separation:
#   * the MIGRATOR/OWNER role owns the `agent` schema and every object
#     store.Migrate creates in it (including the append-only audit trigger), and
#     holds database CREATE. It is used ONLY to run the boot migration.
#   * the RUNTIME role — the one the agent connects as for all normal work — owns
#     NOTHING, has NO database CREATE, and cannot ALTER (hence cannot disable the
#     append-only trigger) or TRUNCATE the audit ledger. Its exact table
#     privileges (SELECT+INSERT on the append-only agent_audit; the specific DML
#     the mutable tables need) are granted by store.MigrateAndGrant AS THE
#     MIGRATOR after the tables exist, so this script only has to grant it
#     CONNECT + schema USAGE here.
# This makes the F05 "runtime owner can disable the audit trigger / mutate audit
# history / create unrelated schemas" attack impossible for the runtime role.
#
# SINGLE-ROLE FALLBACK (dev/test): when the migrator variables are absent, this
# degrades to the historical single-role model — one role that owns the `agent`
# schema and runs the migration itself (main.go uses st.Migrate, not
# MigrateAndGrant, because AGENT_MIGRATE_DATABASE_URL is unset). Convenient for a
# local psql/dev database; the two-role model is the recommended and default
# production posture.
#
# WHERE IT RUNS: mounted into the postgres container's
# /docker-entrypoint-initdb.d/ (docker-compose). The official postgres image runs
# every *.sh/*.sql there exactly once, on FIRST database initialization (empty
# data volume), as the bootstrap superuser. The agent therefore never receives or
# holds superuser credentials — only the restricted runtime role's DSN. For an
# already-initialized volume this init does not re-run; an operator provisioning
# the roles after the fact can run the same statements manually (they are
# idempotent).
#
# SECURITY OF THIS SCRIPT: role names and passwords arrive as psql client
# variables and are injected server-side exclusively through format('%I', ...)
# (identifier-quoted) and format('%L', ...) (literal-quoted), so neither can be
# used for SQL injection regardless of their contents.

set -eu

: "${AGENT_POSTGRES_USER:?AGENT_POSTGRES_USER must be set for recon-agent runtime role provisioning}"
: "${AGENT_POSTGRES_PASSWORD:?AGENT_POSTGRES_PASSWORD must be set for recon-agent runtime role provisioning}"

# The migrator variables are OPTIONAL. When BOTH are present we provision the
# two-role least-privilege model (finding F05); otherwise we fall back to the
# single-role dev model. Default to empty so `set -u` does not abort.
AGENT_MIGRATOR_USER="${AGENT_MIGRATOR_USER:-}"
AGENT_MIGRATOR_PASSWORD="${AGENT_MIGRATOR_PASSWORD:-}"

if [ -n "${AGENT_MIGRATOR_USER}" ] && [ -n "${AGENT_MIGRATOR_PASSWORD}" ]; then
    # ---- TWO-ROLE LEAST-PRIVILEGE MODEL (finding F05) ----
    # POSTGRES_USER / POSTGRES_DB are exported by the official postgres entrypoint.
    psql -v ON_ERROR_STOP=1 --no-psqlrc \
         --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
         -v runtime_user="${AGENT_POSTGRES_USER}" \
         -v runtime_pw="${AGENT_POSTGRES_PASSWORD}" \
         -v migrator_user="${AGENT_MIGRATOR_USER}" \
         -v migrator_pw="${AGENT_MIGRATOR_PASSWORD}" <<'EOSQL'
-- Restricted RUNTIME login role (idempotent create; keep password in sync).
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'runtime_user', :'runtime_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'runtime_user', :'runtime_pw')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')
\gexec

-- OWNER/MIGRATOR login role (idempotent create; keep password in sync).
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'migrator_user', :'migrator_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'migrator_user')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'migrator_user', :'migrator_pw')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'migrator_user')
\gexec

-- The MIGRATOR owns the `agent` schema; store.Migrate (run as the migrator)
-- then creates every agent.* object as that owner. AUTHORIZATION sets the owner
-- on first create; the explicit ALTER ... OWNER re-asserts it on a pre-existing
-- schema (e.g. a single-role volume being reprovisioned two-role by hand).
SELECT format('CREATE SCHEMA IF NOT EXISTS agent AUTHORIZATION %I', :'migrator_user')
\gexec
SELECT format('ALTER SCHEMA agent OWNER TO %I', :'migrator_user')
\gexec

-- MIGRATOR privileges: connect, database-level CREATE (store.Migrate runs an
-- idempotent CREATE SCHEMA IF NOT EXISTS agent at boot and Postgres evaluates
-- CREATE-on-database BEFORE the IF-NOT-EXISTS short-circuit), and USAGE/CREATE
-- within `agent`.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'migrator_user')
\gexec
SELECT format('GRANT CREATE ON DATABASE %I TO %I', current_database(), :'migrator_user')
\gexec
SELECT format('GRANT USAGE, CREATE ON SCHEMA agent TO %I', :'migrator_user')
\gexec

-- RUNTIME privileges: ONLY connect to the database and use (reference) the
-- `agent` schema. It gets NO database CREATE (so it cannot create unrelated
-- schemas) and NO ownership (so it cannot ALTER/DISABLE the append-only trigger
-- or TRUNCATE the audit ledger). Its per-table DML privileges are granted later,
-- as the migrator, by store.MigrateAndGrant once the tables exist (finding F05,
-- Rule 5.5). The REVOKE of CREATE-on-database is explicit defense in depth.
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user')
\gexec
SELECT format('REVOKE CREATE ON DATABASE %I FROM %I', current_database(), :'runtime_user')
\gexec
SELECT format('GRANT USAGE ON SCHEMA agent TO %I', :'runtime_user')
\gexec

-- Neither role may touch the `public` schema (Rule 5.1: the agent never uses
-- Blnk's data; its objects live only under `agent`).
SELECT format('REVOKE ALL ON SCHEMA public FROM %I', :'runtime_user')
\gexec
SELECT format('REVOKE ALL ON SCHEMA public FROM %I', :'migrator_user')
\gexec
EOSQL

    echo "recon-agent: provisioned TWO-ROLE model — migrator '${AGENT_MIGRATOR_USER}' owns schema 'agent'; restricted runtime role '${AGENT_POSTGRES_USER}' has no ownership/CREATE (finding F05)"
else
    # ---- SINGLE-ROLE DEV FALLBACK (historical behavior) ----
    psql -v ON_ERROR_STOP=1 --no-psqlrc \
         --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
         -v agent_user="${AGENT_POSTGRES_USER}" \
         -v agent_pw="${AGENT_POSTGRES_PASSWORD}" <<'EOSQL'
-- Create the restricted login role if it does not already exist (idempotent).
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'agent_user', :'agent_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'agent_user')
\gexec

-- Keep the role's password in sync with AGENT_POSTGRES_PASSWORD on re-runs.
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'agent_user', :'agent_pw')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'agent_user')
\gexec

-- In single-role mode the one role owns the `agent` schema and runs the
-- migration itself (main.go uses st.Migrate because AGENT_MIGRATE_DATABASE_URL
-- is unset). AUTHORIZATION sets the owner on first create; the ALTER re-asserts
-- ownership on a pre-existing schema.
SELECT format('CREATE SCHEMA IF NOT EXISTS agent AUTHORIZATION %I', :'agent_user')
\gexec
SELECT format('ALTER SCHEMA agent OWNER TO %I', :'agent_user')
\gexec

-- Minimal privileges for the single dev role: connect, own/use/create within
-- `agent`, and database-level CREATE (needed because store.Migrate runs an
-- idempotent CREATE SCHEMA IF NOT EXISTS agent at boot and Postgres evaluates
-- CREATE-on-database BEFORE the IF-NOT-EXISTS short-circuit). It grants NO
-- access to Blnk's data: `public` is revoked below and the role holds no
-- privileges on any blnk.* object (Rule 5.1).
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'agent_user')
\gexec
SELECT format('GRANT CREATE ON DATABASE %I TO %I', current_database(), :'agent_user')
\gexec
SELECT format('GRANT USAGE, CREATE ON SCHEMA agent TO %I', :'agent_user')
\gexec
SELECT format('REVOKE ALL ON SCHEMA public FROM %I', :'agent_user')
\gexec
EOSQL

    echo "recon-agent: provisioned SINGLE-ROLE dev model — role '${AGENT_POSTGRES_USER}' owns schema 'agent' (set AGENT_MIGRATOR_USER/PASSWORD for the two-role least-privilege model, finding F05)"
fi
