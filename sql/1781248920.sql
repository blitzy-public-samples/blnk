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

-- Ensure event_subscribers_key_scope_chk is absent, idempotently.
--
-- The constraint this drops forbade "partition_key_prefix IS NOT NULL AND
-- credential_reference IS NOT NULL". That combination MUST be representable: a
-- partition key prefix is a ROUTING HINT rather than an access boundary — Kafka's ACL
-- resource types are Topic, Group, Cluster, TransactionalId and DelegationToken, none of
-- them a message key — so forbidding it withholds no narrower credential and instead
-- leaves a subscriber that legitimately records a prefix permanently unable to obtain
-- credentials at all. Every credential response and subscriber read states the boundary
-- the broker really keeps in enforced_access, and sql/1781248900.sql documents the
-- column in those terms.
--
-- The file is a DROP rather than a deletion because sql-migrate plans against the ledger
-- in blnk.gorp_migrations: an id recorded there with no source file aborts the whole run
-- with "unknown migration in database", so any database that applied the original could
-- never migrate again.
ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_key_scope_chk;

-- +migrate Down

-- Deliberately a NO-OP.
--
-- The state this migration's up direction finds is "the constraint is absent" — on a
-- fresh database because nothing creates it, and on a database that applied the
-- original version because the up direction has just removed it. Re-adding it on the
-- way down would not restore a previous state; it would reinstate the defect described
-- above on any database that rolled back.
SELECT 1;
