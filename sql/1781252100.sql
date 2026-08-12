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

-- CAPTURE THE MONITOR DEFINITIONS IN THE MUTATION'S OWN TRANSACTION.
--
-- blnk.balance_monitor_handoff carried the BALANCE as the transaction wrote it, and
-- nothing else. The evaluator then re-read blnk.balance_monitors when it drained the
-- row, so one half of the decision was snapshotted and the other half was read live —
-- from a table an operator can edit, and does, through PUT and DELETE
-- /balance-monitors.
--
-- Two consequences followed, and neither was observable from the outbox:
--
--   * WHICH EVENTS EXIST could change after the mutation committed. A monitor deleted
--     between the commit and the drain produced no alert for a movement that had
--     already crossed its threshold; a monitor created in that window produced an alert
--     for a movement it was never registered to watch; an edited threshold produced an
--     alert for a condition that was not the condition in force.
--   * A RE-EVALUATION could disagree with the first attempt. The claim lease can lapse
--     and a row can be retried, and BalanceMonitorEventIdentity makes those attempts
--     collide on purpose so the second is idempotent — but only if the second reaches
--     the same verdict.
ALTER TABLE blnk.balance_monitor_handoff
    ADD COLUMN IF NOT EXISTS monitor_snapshot JSONB;

COMMENT ON COLUMN blnk.balance_monitor_handoff.monitor_snapshot IS
    'The monitor definitions in force when the balance''s transaction committed, read inside that transaction. NULL only on rows written before this column existed, which the evaluator drains by falling back to a live read.';

-- +migrate Down

ALTER TABLE blnk.balance_monitor_handoff
    DROP COLUMN IF EXISTS monitor_snapshot;
