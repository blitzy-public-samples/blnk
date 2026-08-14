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

-- Make the partition-key scope a DELIVERED scope of the subscriber access model instead
-- of a state the registry refuses to hold.
--
-- sql/1781248920.sql added event_subscribers_key_scope_chk, forbidding a row from
-- recording partition_key_prefix while it also held credential_reference, and
-- sql/1781248900.sql's column commentary describes that barrier as part of the schema's
-- contract. Both statements are superseded here.
--
-- The access model this feature implements scopes a subscriber's credential by three
-- things: its authorised topics, its consumer group, and its partition-key prefix. The
-- constraint made the third one unusable — a subscriber that recorded a key scope could
-- never hold a credential, so the scope existed only on rows that could not consume.
--
-- This is the posture the event contract already takes for duplicate suppression: Kafka
-- delivery is at-least-once, so event_id is a documented subscriber obligation rather
-- than a promise the broker keeps. A key scope is that pattern applied to
-- authorization.
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
-- validating scan.
UPDATE blnk.event_subscribers
SET partition_key_prefix = NULL,
    updated_at           = NOW()
WHERE partition_key_prefix IS NOT NULL
  AND credential_reference IS NOT NULL;

-- STEP 2 — the constraint, through a guarded DO block because PostgreSQL has no ADD
-- CONSTRAINT IF NOT EXISTS and a bare ALTER would fail the whole migration on a
-- database that already carries it.
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
