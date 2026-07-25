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

# agent-db-init.sh — provisions the DEDICATED, least-privilege PostgreSQL role
# the recon-agent connects as at runtime (finding M-13).
#
# WHY THIS EXISTS: the recon-agent must NOT connect as the shared Postgres
# superuser. If the agent (or its DB connection) were compromised, a superuser
# connection would defeat the intended schema boundary and expose every blnk.*
# table. This script creates a role that OWNS only the `agent` schema and can do
# nothing in `public` or in Blnk's tables, so the runtime blast radius is bounded
# to the agent's own additive objects.
#
# WHERE IT RUNS: mounted into the postgres container's
# /docker-entrypoint-initdb.d/ (docker-compose). The official postgres image runs
# every *.sh/*.sql there exactly once, on FIRST database initialization (empty
# data volume), as the bootstrap superuser. The agent therefore never receives or
# holds superuser credentials — only the restricted role's DSN. For an
# already-initialized volume this init does not re-run; an operator provisioning
# the role after the fact can run the same statements manually (they are
# idempotent).
#
# SECURITY OF THIS SCRIPT: the role name and password arrive as psql client
# variables and are injected server-side exclusively through format('%I', ...)
# (identifier-quoted) and format('%L', ...) (literal-quoted), so neither can be
# used for SQL injection regardless of their contents.

set -eu

: "${AGENT_POSTGRES_USER:?AGENT_POSTGRES_USER must be set for recon-agent role provisioning}"
: "${AGENT_POSTGRES_PASSWORD:?AGENT_POSTGRES_PASSWORD must be set for recon-agent role provisioning}"

# POSTGRES_USER / POSTGRES_DB are exported by the official postgres entrypoint.
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

-- The agent owns ONLY the `agent` schema; store.Migrate then creates every
-- agent.* object as that owner. AUTHORIZATION sets the owner on first create;
-- the explicit ALTER ... OWNER re-asserts ownership on a pre-existing schema.
SELECT format('CREATE SCHEMA IF NOT EXISTS agent AUTHORIZATION %I', :'agent_user')
\gexec
SELECT format('ALTER SCHEMA agent OWNER TO %I', :'agent_user')
\gexec

-- Minimal privileges: connect to the database and use/create within `agent`
-- only. Revoke any ambient rights on `public` so the role is confined to its
-- own schema (Rule 5.1: the agent never touches blnk.* tables).
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'agent_user')
\gexec
SELECT format('GRANT USAGE, CREATE ON SCHEMA agent TO %I', :'agent_user')
\gexec
SELECT format('REVOKE ALL ON SCHEMA public FROM %I', :'agent_user')
\gexec
EOSQL

echo "recon-agent: provisioned restricted role '${AGENT_POSTGRES_USER}' owning schema 'agent'"
