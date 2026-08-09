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

-- DROP event_subscribers_key_scope_chk: a subscriber MAY record a partition key
-- prefix while holding a Kafka credential.
--
-- # What the constraint asserted, and why it has to go
--
-- 1781248920.sql made "partition_key_prefix recorded AND credential_reference
-- recorded" unrepresentable, as the third barrier behind two service-layer refusals.
-- All three implemented one policy: because Kafka's authorizer has no message-key
-- dimension, a recorded prefix was treated as an authorization that must never
-- coexist with a credential, and credential issuance was REFUSED while one was
-- present.
--
-- That policy contradicts the access model this feature is required to deliver. A
-- subscriber's boundary is defined as its authorised TOPICS, its CONSUMER GROUP and
-- its partition-key prefix, and the credential endpoint is required to provision
-- against that record — the prefix included — and return the broker endpoint, the
-- topic list, the group id and the SASL credentials. Refusing to issue whenever a
-- prefix is present does not implement a narrower boundary; it implements NO
-- boundary, because the subscriber gets no credential at all and therefore consumes
-- nothing. A subscriber that cannot consume is not isolated, it is absent.
--
-- The three ACL dimensions that ARE broker-enforced remain exactly as they were:
-- literal Read/Describe bindings on each authorised topic, and a PREFIXED Read
-- binding reserving the subscriber's consumer-group namespace. Nothing about
-- topic-level or group-level isolation is relaxed by dropping this constraint, and no
-- credential gains access to a topic, a dead-letter topic or a group it did not
-- already have.
--
-- # The prefix is a CONSUMER-SIDE contract, and that is now said out loud
--
-- The original concern was real: a row recording a prefix beside a live credential
-- must not be readable as "the broker confines this subscriber to those keys". The
-- answer is disclosure rather than refusal. The credential response states the
-- recorded prefix, states that its enforcement is consumer-side, and states that the
-- broker grants whole topics; issuance logs the same at WARNING; and the operator
-- documentation says it in both the subscriber guide and the operations runbook. The
-- registry therefore no longer claims a boundary that does not exist — it records the
-- filtering contract the subscriber is expected to apply, which is what the column
-- always meant.
--
-- An operator who needs a HARD boundary still has the enforceable remedy the service
-- has always named: narrow authorized_topics, which is a real ACL. That remedy is
-- unchanged and is documented alongside the disclosure.
--
-- # A PREFIX 1781248920.sql ALREADY CLEARED CANNOT BE RECOVERED HERE
--
-- That migration's STEP 1 set partition_key_prefix to NULL on every row that held it
-- beside a credential, so on any database where it ran, those prefixes are gone and
-- this file cannot restore them: the pre-repair values were not retained anywhere.
-- Nothing is fabricated to fill the gap — a guessed prefix is exactly the false
-- assurance both migrations exist to avoid.
--
-- Operators who had recorded a prefix before 1781248920.sql ran must re-record it:
--
--     PATCH /subscribers/{id}   {"partition_key_prefix": "<prefix>"}
--
-- which now succeeds on a provisioned subscriber. Rows registered after this
-- migration are unaffected, and a row that never held both columns was never touched
-- by the repair.
--
-- # Why a new file rather than an edit of 1781248920.sql
--
-- sql-migrate records a migration by FILENAME. Editing that file's body would leave
-- every database on which it has already run still carrying the constraint while
-- reporting the migration as applied — the same reasoning 1781248910.sql and
-- 1781248920.sql each give for existing separately. This migration is additive and
-- idempotent: IF EXISTS makes it a no-op on a database that never had the constraint.
ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_key_scope_chk;

-- +migrate Down

-- Re-adding the constraint restores the schema 1781248920.sql established, which is
-- what a down migration owes: it undoes this file's change to the schema and nothing
-- else.
--
-- IT CAN FAIL, and failing is correct. Once issuance stopped refusing, rows recording
-- a prefix beside a live credential are legitimate and expected, so the validating
-- scan will reject any database that holds one. There is no repair step here on
-- purpose: silently clearing a prefix an operator recorded deliberately — which is
-- what 1781248920.sql's STEP 1 did, when the prefix was believed to be a false claim
-- — would destroy a real statement of intent on the way DOWN, where the caller has
-- asked only to reverse a schema change.
--
-- Whoever needs this migration to succeed must first decide what those rows should
-- say, using the enforceable remedy or an explicit clear:
--
--     SELECT subscriber_id, partition_key_prefix
--     FROM blnk.event_subscribers
--     WHERE partition_key_prefix IS NOT NULL
--       AND credential_reference IS NOT NULL;
--
-- The guarded DO block, and the StatementBegin/StatementEnd markers around it, are
-- required for the same reasons 1781248920.sql gives: PostgreSQL has no ADD
-- CONSTRAINT IF NOT EXISTS, and sql-migrate splits on semicolons with no knowledge of
-- dollar quoting, so an unbracketed block is cut at its first internal semicolon.
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
