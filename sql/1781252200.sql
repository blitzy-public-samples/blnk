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

-- WITHDRAW THE INTERNAL SYSTEM TOPIC FROM EVERY SUBSCRIBER GRANT.
--
-- model.SubscriberGrantableEventCategories no longer offers the `system` category. The
-- topic is still created and still published to — it carries `ledger.created`,
-- `system.error` and every event type the catalogue does not recognise — but no
-- subscriber principal may hold an ACL over it, because `system.error`'s payload is
-- frozen by the requirement and renders Blnk's error text verbatim (a PostgreSQL error
-- names schema, table, column and routine; a broker error names internal addresses),
-- and because a grant of the catch-all category would also stand over event types
-- nobody has catalogued yet.
--
-- The refusal is enforced at every door a new grant can arrive through: the request
-- DTO, this schema's own repository guard, and the ACL provisioner. None of them
-- touches a row that ALREADY records the topic.
--
--   1. REMOVES every `<anything>.system` entry from authorized_topics.
--   2. MARKS GRANT RECONCILIATION PENDING on every row it changed, by setting
--      grant_reconcile_pending_at.

UPDATE blnk.event_subscribers
SET authorized_topics = ARRAY(
        SELECT topic
        FROM unnest(authorized_topics) AS topic
        WHERE topic NOT LIKE '%.system'
    ),
    grant_reconcile_pending_at = COALESCE(grant_reconcile_pending_at, NOW()),
    updated_at                 = NOW()
WHERE EXISTS (
    SELECT 1
    FROM unnest(authorized_topics) AS topic
    WHERE topic LIKE '%.system'
);

-- +migrate Down

-- IRREVERSIBLE BY DESIGN, and the reason is recorded rather than left as an empty
-- section.
--
-- A deployment that genuinely rolls back to a build where `<prefix>.system` was
-- grantable and wants a subscriber to hold it again re-records the topic through PUT
-- /subscribers/{subscriber_id}, which validates the grant and reconciles the broker.
-- That is one deliberate request per subscriber, which is the correct cost for
-- restoring access to a topic carrying verbatim internal error text.

SELECT 1;
