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

-- Numbered 1781249138 rather than 1781249137: that timestamp is taken by the trace-context
-- migration, which adds blnk.event_outbox.traceparent and .tracestate. Two migrations cannot
-- share an identifier — sql-migrate keys applied state on the file name — and the trace columns
-- were applied first, so this one takes the next slot. Ordering is otherwise irrelevant here:
-- the two touch different tables.

-- Make the partition-key scope a DELIVERED scope of the subscriber access model
-- instead of a state the registry refuses to hold.
--
-- # What this supersedes
--
-- sql/1781248920.sql added event_subscribers_key_scope_chk, forbidding a row from
-- recording partition_key_prefix while it also held credential_reference, and
-- sql/1781248900.sql's column commentary describes that barrier as part of the
-- schema's contract. Both statements are superseded here. The reasoning behind them
-- was sound as far as it went and its premise is unchanged: Kafka's authorizer has
-- no message-key dimension, so a principal granted Read on a shared category topic
-- reads every record on it whatever the key. What was wrong was the conclusion.
--
-- The access model this feature implements scopes a subscriber's credential by three
-- things: its authorised topics, its consumer group, and its partition-key prefix.
-- The constraint made the third one unusable — a subscriber that recorded a key scope
-- could never hold a credential, so the scope existed only on rows that could not
-- consume. That is not an implementation of the third scope; it is a refusal of it.
-- And the refusal bought nothing a reader could see: the row still recorded the
-- prefix, the registry still described the boundary, and the only thing that changed
-- was that the subscriber had no access at all.
--
-- The two designs that COULD move the boundary to the broker are both closed off
-- deliberately, which is why disclosure is what is left. A resource per authorization
-- domain — a topic per key scope — contradicts the model's own first rule that there
-- are no per-tenant topics. An interposed filtering gateway is subscriber-side
-- consumer machinery, which Blnk explicitly does not build.
--
-- # What replaces it
--
-- The scope is issued, and it is DELIVERED to the one party that can apply it. The
-- credential endpoint returns it inside its enforced-access declaration, in the same
-- object as partition_key_prefix_enforced — which is false — so it is structurally
-- impossible to receive the prefix without receiving the statement that the broker
-- does not check it. model.EventSubscriber.HasKeyAccess is the rule the prefix means,
-- and EffectiveKeyScope is the pair that reports the scope together with who enforces
-- it, so no response can carry one without the other.
--
-- This is the posture the event contract already takes for duplicate suppression:
-- Kafka delivery is at-least-once, so event_id is a documented subscriber obligation
-- rather than a promise the broker keeps. A key scope is that pattern applied to
-- authorization.
--
-- # STEP 1 — drop the constraint
--
-- IF EXISTS because a database provisioned before sql/1781248920.sql ran, or one
-- restored from a dump taken before it, does not carry the constraint and must not
-- fail here.
--
-- NOTE ON WHAT IS NOT RECOVERABLE. sql/1781248920.sql's own step 1 NULLED the prefix
-- on every row that held a credential, and those values are gone. Nothing here
-- reinvents them: a prefix is a statement of intent only its author can make, and
-- guessing one would attach an authorization boundary to a subscriber that never
-- asked for it. Operators who recorded a prefix before that migration ran must record
-- it again and re-issue, which is also what hands the consumer its scope.
ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_key_scope_chk;

-- STEP 2 — restate the column.
--
-- The comment is a live schema object and it is the thing an operator actually reads
-- when they inspect the table, so leaving it describing a refusal that no longer
-- happens would be the same defect in a different place. It now names the enforcement
-- point, which is the single fact a reader of this column needs and the one they would
-- otherwise assume wrongly.
COMMENT ON COLUMN blnk.event_subscribers.partition_key_prefix IS
    'The third scope of the subscriber access model: the subscriber is entitled only to '
    'records whose message key carries this prefix. Because every Blnk event is keyed by '
    'ledger id, that is a ledger boundary. ENFORCED AT THE CONSUMER, NOT AT THE BROKER: '
    'Kafka ACLs are topic-level and group-level and the authorizer has no message-key '
    'dimension, so a credential granted a shared category topic can read all of it. The '
    'credential endpoint returns this prefix together with partition_key_prefix_enforced '
    '= false, so the subscriber receives the boundary and the statement of who keeps it '
    'in one object. NULL means no key scope, reported as all-keys. Superseded '
    'sql/1781248920.sql, which forbade this column alongside credential_reference.';

-- +migrate Down

-- Restore the constraint, so a rollback returns the schema this file changed and
-- nothing else.
--
-- STEP 1 — repair before constraining, exactly as sql/1781248920.sql had to. Any row
-- that now legitimately records a prefix beside a credential — which is the state this
-- migration exists to permit, so on a live database there will be some — would fail the
-- validating scan. Clearing the PREFIX rather than the credential is the same choice
-- made there and for the same reason: the registry is left describing the access that
-- actually exists at the broker, whereas clearing credential_reference would leave it
-- reporting "registered, never provisioned" while a principal that authenticates is
-- still live, which is the dangerous direction.
UPDATE blnk.event_subscribers
SET partition_key_prefix = NULL,
    updated_at           = NOW()
WHERE partition_key_prefix IS NOT NULL
  AND credential_reference IS NOT NULL;

-- STEP 2 — the constraint, through a guarded DO block because PostgreSQL has no
-- ADD CONSTRAINT IF NOT EXISTS and a bare ALTER would fail the whole migration on a
-- database that already carries it.
--
-- The StatementBegin/StatementEnd markers are required, not decorative: sql-migrate
-- splits on semicolons and knows nothing about dollar quoting, so without them the
-- block is cut at its first internal semicolon and the migration fails with
-- "unterminated dollar-quoted string".
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

-- STEP 3 — restore the previous column commentary, so the down direction leaves no
-- statement describing behaviour the restored schema does not have.
COMMENT ON COLUMN blnk.event_subscribers.partition_key_prefix IS
    'A key-scoped authorization constraint Kafka cannot enforce. Recording one alongside '
    'credential_reference is forbidden by event_subscribers_key_scope_chk, and credential '
    'issuance refuses for any row that carries one.';
