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

-- Two RECOVERY indexes for blnk.event_outbox.
--
-- Both support work the relay OWES a row after its main publish attempt has
-- finished, and neither existed while that work had no way of being reclaimed at
-- all. They are additive: no column, constraint or existing index is touched, so
-- this migration is safe to apply to a database that already carries
-- 1781248800.sql.
--
-- No new status literal is introduced. The event_outbox_status_known CHECK
-- constraint enumerates the state machine and stays exactly as it is; both sets
-- below are carved out of states that already exist.

-- Rows that OWE THEIR DEAD-LETTER HAND-OFF.
--
-- ClaimFailedEventOutboxForDeadLetter reads this set, so the hand-off is retried rather
-- than stranded. dlt_topic IS NULL is what makes the set self-clearing: once the
-- dead-letter record lands, the row leaves it for good. 'processing' is included
-- because a relay can die between that claim and the record, and the same predicate
-- must recover it.
CREATE INDEX IF NOT EXISTS idx_event_outbox_dead_letter_owed
    ON blnk.event_outbox (next_attempt_at, occurred_at, id)
    WHERE status IN ('failed', 'processing') AND dlt_topic IS NULL;

-- Rows whose LEGACY WEBHOOK LEG is still owed.
--
-- SUNSET NOTICE: this index exists only for the 30-day dual-delivery window and is
-- DROPPED together with the webhook_dispatched and webhook_attempts columns,
-- MarkWebhookDispatched, MarkEventLegacyWebhookAttempted, ClaimPendingWebhookDeliveries
-- and the relay's dual-delivery branch when the webhook sunset arrives. Nothing outside
-- that window depends on it.
--
-- During the window the relay enqueues the legacy delivery and publishes to Kafka from
-- the same claimed row. A failed enqueue is deliberately swallowed — a webhook receiver
-- being down must not consume a Kafka retry attempt, still less dead-letter an event on
-- the new transport — but the row then reaches a state the main claim never revisits
-- and its claim token is cleared, so nothing would ever return to the outstanding
-- webhook.
--
-- ALL THREE KAFKA END STATES are candidates, not 'dispatched' alone. The relay enqueues
-- the webhook BEFORE it publishes, so when both legs fail on the attempt that spends
-- the retry budget the row travels failed → dead_lettered with its webhook still owed.
--
-- The partial predicate is what makes it affordable. In a healthy window the set is
-- empty, while 'dispatched' is the largest status in the table — so an unindexed poll
-- of it would scan every event ever delivered, once a second.
CREATE INDEX IF NOT EXISTS idx_event_outbox_webhook_pending
    ON blnk.event_outbox (occurred_at, id)
    WHERE status IN ('dispatched', 'failed', 'dead_lettered') AND webhook_dispatched = FALSE;

-- +migrate Down

DROP INDEX IF EXISTS blnk.idx_event_outbox_webhook_pending;
DROP INDEX IF EXISTS blnk.idx_event_outbox_dead_letter_owed;
