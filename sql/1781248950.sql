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

-- ORPHAN-01: give the two ways broker-side access outlives its record a DURABLE
-- state, and bound the one free-text column on the table.
--
-- =====================================================================
-- PART 1 — credential_orphaned_at: a live credential nothing records
-- =====================================================================
--
-- Provisioning writes a subscriber's SCRAM credential BEFORE its ACL bindings,
-- because a binding for a principal that does not exist is inert while a credential
-- with no bindings still AUTHENTICATES. So when the registry write that records an
-- issuance fails, the credential already exists at the broker and the compensation
-- is to revoke it.
--
-- When that compensation ALSO fails, a means of authenticating to the event bus
-- exists for a principal the registry records no issuance for — and until now the
-- only trace of it was a log line. Nothing counted it, so the alert on outstanding
-- revocation could not see it: that rule reads revocation_pending_at, which this
-- path never sets. An exposure whose only representation is a log line is an
-- exposure nobody is watching, which is precisely the condition the alert exists
-- for.
--
-- credential_orphaned_at is that state, made durable. NON-NULL means "a credential
-- for this row's principal may exist at the broker that Blnk could neither record
-- nor revoke". It is settled automatically by either of the two operations that
-- necessarily resolve it — a successful issuance REPLACES the orphaned credential
-- (Kafka stores one per principal), and a successful revocation REMOVES it — and by
-- nothing else, so a marker that persists is a marker that genuinely needs a human.
--
-- It is deliberately NOT the same column as revocation_pending_at, and the
-- distinction is operational rather than tidy. The two demand OPPOSITE remedies:
--
--   * revocation_pending_at means a deregistration is in flight, so the row is NOT
--     an active subscriber and issuance is refused for it. Its remedy is to RETRY
--     THE DEREGISTRATION.
--   * credential_orphaned_at means the row IS an active subscriber whose recorded
--     credential is untrustworthy. Its remedy is to RE-ISSUE, which replaces the
--     orphan by construction, or to deprovision if the subscriber should have no
--     access at all.
--
-- Collapsing them into one column would have made the documented "re-issue to
-- settle it" remedy unreachable, because issuance refuses a row marked pending
-- revocation.
--
-- =====================================================================
-- PART 2 — revocation_failed_at: the attempt failed, not merely started
-- =====================================================================
--
-- revocation_pending_at is stamped BEFORE the broker is touched, so it says only
-- that a deregistration began. That covers two materially different situations
-- with one value: the broker REFUSED the revocation, or the process died between
-- the stamp and the attempt. The first needs the broker's authorization or reach
-- fixed before any retry can work; the second needs nothing but a retry.
--
-- revocation_failed_at separates them. It is set when a revocation attempt actually
-- failed at the broker, and CLEARED at the start of every new attempt — so it always
-- describes the MOST RECENT attempt rather than accumulating history. Meanwhile
-- revocation_pending_at keeps the FIRST instant, because the quantity an operator
-- alerts on is the age of the exposure, not the age of the last try.
--
-- Their combination is readable:
--
--   pending set, failed NULL  -> in flight, or the row could not be deleted after a
--                                successful revocation. Retry.
--   pending set, failed set   -> the broker refused. Fix the broker side first.
--
-- =====================================================================
-- PART 3 — a maximum length for name
-- =====================================================================
--
-- name is the only free-text, caller-supplied column on this table. Nothing bounded
-- it, so the effective limit was the global 5 MiB request body cap — and the value
-- is stored, returned in every registry response and written into log fields. A
-- multi-megabyte name is therefore not merely untidy: it is amplified by every read
-- of the registry and by every log line that names the subscriber.
--
-- The service and the DTO both bound it now, and this CHECK is the barrier that also
-- covers a psql session, a data migration, a restored backup and any future code
-- path that writes the column without going through them — the same reasoning
-- sql/1781248920.sql gives for the key-scope constraint.
--
-- 256 characters is chosen to be generous for a human label in any script while
-- staying far below anything that could matter for storage, response size or log
-- volume. It is measured with char_length on the TRIMMED value, matching what the
-- service stores, so trailing whitespace cannot be used to approach the bound.

ALTER TABLE blnk.event_subscribers
    ADD COLUMN IF NOT EXISTS credential_orphaned_at TIMESTAMP WITH TIME ZONE NULL;

ALTER TABLE blnk.event_subscribers
    ADD COLUMN IF NOT EXISTS revocation_failed_at TIMESTAMP WITH TIME ZONE NULL;

-- Guarded by a catalogue lookup rather than IF NOT EXISTS, because ADD CONSTRAINT
-- has no such clause and sql-migrate applies every file's Up unconditionally on
-- `blnk migrate up`. sql/1781248920.sql brackets its guard the same way and for the
-- same reason; the StatementBegin/End markers are required because sql-migrate
-- splits on semicolons and knows nothing about dollar quoting.
-- +migrate StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'event_subscribers_name_length_chk'
          AND conrelid = 'blnk.event_subscribers'::regclass
    ) THEN
        ALTER TABLE blnk.event_subscribers
            ADD CONSTRAINT event_subscribers_name_length_chk
            CHECK (char_length(btrim(name)) <= 256);
    END IF;
END
$$;
-- +migrate StatementEnd

-- PARTIAL INDEXES, matching the shape sql/1781248800.sql uses for the outbox's
-- terminal states: the predicate is the same one the aggregate uses, so the index
-- holds only the rows that are outstanding — which in the healthy steady state is
-- none of them, making both indexes empty and free.
--
-- The aggregate they serve is COUNT(*) plus MIN(<marker>), so the marker leads the
-- key and an index-only scan answers both figures without touching the heap.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_credential_orphaned
    ON blnk.event_subscribers (credential_orphaned_at)
    WHERE credential_orphaned_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_subscribers_revocation_failed
    ON blnk.event_subscribers (revocation_failed_at)
    WHERE revocation_failed_at IS NOT NULL;

-- +migrate Down

-- The reversal undoes what this file did to the schema and nothing else.
--
-- DROPPING credential_orphaned_at DESTROYS THE RECORD OF EVERY UNSETTLED ORPHAN. The
-- markers name rows whose principal may still hold a credential at the broker, and
-- once the column is gone the only remaining trace is the log line that this
-- migration exists to replace. Read the two aggregates — or the rows themselves —
-- and settle anything outstanding BEFORE rolling this back. The same applies to
-- revocation_failed_at, whose loss silently merges "the broker refused" back into
-- "a deregistration began".
DROP INDEX IF EXISTS blnk.idx_event_subscribers_revocation_failed;
DROP INDEX IF EXISTS blnk.idx_event_subscribers_credential_orphaned;

ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_name_length_chk;

ALTER TABLE blnk.event_subscribers
    DROP COLUMN IF EXISTS revocation_failed_at;

ALTER TABLE blnk.event_subscribers
    DROP COLUMN IF EXISTS credential_orphaned_at;
