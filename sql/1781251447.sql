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

-- DURABLE SETTLEMENT OBLIGATIONS for blnk.event_subscribers.
--
-- Every column here exists because a subscriber's broker-side state and its
-- registry row can be left disagreeing by a failure that outlives the request
-- which caused it, and until now the ONLY record of that was a log line. A log
-- line cannot be queried, cannot be retried and cannot be alerted on per
-- subscriber, so the divergence persisted until a human happened to read it.
--
-- Additive only: no existing column, constraint or index is touched, so this is
-- safe to apply to a database already carrying 1781248900.sql and its followers.
--
-- # The two obligations, and why they are separate columns
--
-- They are separate because their REMEDIES are different, and a settlement pass
-- that could not tell them apart would have to guess. One is answered by
-- reconciling the broker's ACL bindings to the row; the other by destroying a
-- credential and forgetting it. A single "needs attention" flag would make the
-- worker's action ambiguous exactly when it matters.

ALTER TABLE blnk.event_subscribers
    -- THE GRANT MAY NOT MATCH THE ROW.
    --
    -- An authorization change is three steps: prune at the broker, persist the row,
    -- grant at the broker. The order is deliberate and fail-closed — a failure
    -- always leaves the subscriber with LESS access than the registry claims rather
    -- than more — but fail-closed is not the same as consistent. After a failure
    -- between any two steps the broker is behind the row, and a subscriber that
    -- silently stopped receiving events looks, from the registry, exactly like one
    -- that is working.
    --
    -- The ROW is the source of truth in both directions, so the remedy is uniform:
    -- reconcile the broker to the row. That is what makes one column enough to
    -- cover a failure at either boundary.
    ADD COLUMN IF NOT EXISTS grant_reconcile_pending_at TIMESTAMPTZ,

    -- A CREDENTIAL EXISTS THAT BLNK INTENDED TO DESTROY, or the row claims one that
    -- does not work.
    --
    -- Two failures converge on this single obligation because one remedy answers
    -- both. When provisioning is refused after the credential was written, the
    -- credential is compensated away; if that compensation FAILS, a principal can
    -- authenticate with no boundary. If it SUCCEEDS but clearing the row's
    -- credential_reference then fails, the registry reports the subscriber as
    -- provisioned while no secret works, which every read repeats.
    --
    -- Settlement revokes at the broker and then clears the row. Revoking a
    -- credential that is already gone is a no-op, so the same pass fixes either
    -- case without being told which one it is facing.
    --
    -- IT IS SELF-SATISFYING ON RE-ISSUE, which is the subtle part. A later
    -- successful issuance upserts the SCRAM credential, REPLACING whatever orphan
    -- this obligation was raised against, and records a fresh reference. The
    -- obligation is therefore genuinely discharged rather than merely stale, and the
    -- issuance write clears it in the same statement. Leaving it set would point a
    -- settlement pass at a credential the subscriber is legitimately using and have
    -- it destroy a working one.
    ADD COLUMN IF NOT EXISTS credential_cleanup_pending_at TIMESTAMPTZ,

    -- How many settlement passes this row has cost, and what the last one said.
    --
    -- The same vocabulary blnk.event_outbox uses for the relay, for the same reason:
    -- an obligation that can never be discharged has to be DIAGNOSABLE rather than
    -- merely visible. Without the error text, a permanently stuck subscriber offers
    -- an operator nothing but a non-null timestamp.
    ADD COLUMN IF NOT EXISTS settlement_attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS settlement_last_error TEXT,

    -- When the last pass ran, which is what PACES the retries.
    --
    -- Settlement talks to the same broker that just failed, so a worker that
    -- retried every poll would hammer it and fill the log with one subscriber. The
    -- scan orders by this column with NULLS FIRST, so a newly raised obligation is
    -- always attempted ahead of one that has already been tried.
    ADD COLUMN IF NOT EXISTS settlement_last_attempt_at TIMESTAMPTZ;

-- The scan index.
--
-- In a healthy deployment this set is EMPTY while the table holds every subscriber
-- ever registered, so the partial predicate is what makes a periodic poll
-- affordable: without it every pass would scan the whole registry to find nothing.
--
-- The predicate must stay identical to the one in the settlement query. A partial
-- index is usable only when the query's WHERE clause implies the index's, and the
-- planner proves that by comparing the expressions it can see while planning — so a
-- query that spelled this condition differently would silently fall back to a
-- sequential scan.
--
-- settlement_last_attempt_at leads the key with NULLS FIRST ordering so an obligation
-- that has never been attempted sorts ahead of one that has, and id breaks ties to
-- make the order total and therefore stable across passes.
CREATE INDEX IF NOT EXISTS idx_event_subscribers_settlement_owed
    ON blnk.event_subscribers (settlement_last_attempt_at NULLS FIRST, id)
    WHERE grant_reconcile_pending_at IS NOT NULL
       OR credential_cleanup_pending_at IS NOT NULL;

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_event_subscribers_settlement_owed;

ALTER TABLE blnk.event_subscribers
    DROP COLUMN IF EXISTS settlement_last_attempt_at,
    DROP COLUMN IF EXISTS settlement_last_error,
    DROP COLUMN IF EXISTS settlement_attempts,
    DROP COLUMN IF EXISTS credential_cleanup_pending_at,
    DROP COLUMN IF EXISTS grant_reconcile_pending_at;
