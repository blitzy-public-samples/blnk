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

-- SEC-05, THIRD BARRIER: make "records a key scope AND holds a credential"
-- unrepresentable in blnk.event_subscribers.
--
-- # The state, and why a comment could not prevent it
--
-- partition_key_prefix records that a subscriber is authorised only for the records
-- whose key carries that prefix. Kafka's authorizer has NO message-key dimension:
-- an ACL grants Read on a TOPIC, so a principal holding one reads all of it. A row
-- carrying a prefix therefore describes an authorization NARROWER THAN ANY
-- CREDENTIAL BLNK CAN MINT, and the two facts together — prefix recorded, credential
-- live — mean the registry states a tenancy boundary that does not exist.
--
-- EventSubscriberService refuses that combination from both directions:
-- requireProvisionableKeyScope refuses to issue against a row that records a prefix,
-- and requireRecordableKeyScope refuses to record a prefix on a row that holds a
-- credential. The second of those was missing, and its absence was reachable by one
-- ordinary API call — register, issue, then update with a prefix — which is how a
-- live credential came to read six records outside the prefix its row advertised.
--
-- Two service guards are the right place to answer a caller, and they are still not
-- enough on their own, because this state is UNDETECTABLE by anyone reading the
-- registry: nothing fails, nothing is logged, and each column looks correct alone. A
-- CHECK is the only barrier that also covers a psql session, a data migration, a
-- restored backup and any future code path that writes these columns without
-- knowing the rule. It is the same reasoning as
-- event_subscribers_credential_pair_chk and event_subscribers_fence_pair_chk, which
-- make their own two-column invariants unrepresentable rather than merely intended.
--
-- # Why this is a separate migration rather than an edit of 1781248900.sql
--
-- That file has already been applied wherever this feature has been exercised, and
-- sql-migrate records a migration by filename: editing its body would leave every
-- existing database without the constraint while reporting the migration as applied.
-- 1781248910.sql was added for the same reason and says so. This migration is
-- additive and idempotent: it touches no column, no index and no other constraint,
-- so it is safe on a database that already carries 1781248900.sql and a no-op on one
-- created after it.

-- STEP 1 — REPAIR the rows that already hold the combination, or the constraint
-- below cannot be added at all.
--
-- The PREFIX is cleared and the credential record is deliberately left alone, and
-- the direction is the whole point. Clearing the prefix makes the row describe the
-- access that ACTUALLY EXISTS: a live credential with Read on whole topics. Clearing
-- credential_reference instead would make the registry report "registered, never
-- provisioned" while a principal that authenticates is still live at the broker —
-- the registry would then UNDER-report real access, which is the dangerous direction
-- and the opposite of what SEC-05 is for.
--
-- An operator who wanted the narrower boundary still has the two enforceable
-- remedies the service names: narrow authorized_topics, or revoke the credential and
-- then record the prefix. Neither is silently chosen here — what is chosen is that
-- the row stops making a claim nothing backs.
UPDATE blnk.event_subscribers
SET partition_key_prefix = NULL,
    updated_at           = NOW()
WHERE partition_key_prefix IS NOT NULL
  AND credential_reference IS NOT NULL;

-- STEP 2 — the constraint.
--
-- Added through a guarded DO block because PostgreSQL has no ADD CONSTRAINT IF NOT
-- EXISTS, and a bare ALTER would fail the whole migration on any database that
-- already carries the constraint — including one restored from a dump taken after
-- this migration ran.
--
-- NOT declared NOT VALID. Step 1 has just removed every violating row, so the
-- validating scan cannot fail, and a NOT VALID constraint would leave the existing
-- rows permanently unchecked for a table whose whole purpose is to be read as an
-- authoritative statement about access.
--
-- The StatementBegin/StatementEnd markers are required, not decorative: sql-migrate
-- splits a migration on semicolons and knows nothing about dollar quoting, so without
-- them the block is cut at its first internal semicolon and the migration fails with
-- "unterminated dollar-quoted string". sql/1780963300.sql brackets its function bodies
-- the same way for the same reason.
-- +migrate StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'event_subscribers_key_scope_chk'
          AND conrelid = 'blnk.event_subscribers'::regclass
    ) THEN
        ALTER TABLE blnk.event_subscribers
            ADD CONSTRAINT event_subscribers_key_scope_chk
            CHECK (partition_key_prefix IS NULL OR credential_reference IS NULL);
    END IF;
END
$$;
-- +migrate StatementEnd

-- +migrate Down

-- Dropping the constraint restores the state the service still refuses, which is the
-- correct asymmetry for a down migration: it undoes what this file did to the schema
-- and nothing else. The repaired rows are NOT restored — the prefixes cleared above
-- were claims about access that never existed, and re-inventing them on the way down
-- would recreate the defect deliberately.
ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_key_scope_chk;
