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

-- WITHDRAW THE INTERNAL SYSTEM TOPIC FROM EVERY SUBSCRIBER GRANT (SEC-02).
--
-- # What changed above this migration
--
-- model.SubscriberGrantableEventCategories no longer offers the `system` category. The
-- topic is still created and still published to — it carries `ledger.created`,
-- `system.error` and every event type the catalogue does not recognise — but no
-- subscriber principal may hold an ACL over it, because `system.error`'s payload is
-- frozen by requirement R-8 and renders Blnk's error text verbatim (a PostgreSQL error
-- names schema, table, column and routine; a broker error names internal addresses), and
-- because a grant of the catch-all category would also stand over event types nobody has
-- catalogued yet.
--
-- # Why code alone does not finish the job
--
-- The refusal is enforced at every door a new grant can arrive through: the request DTO,
-- this schema's own repository guard, and the ACL provisioner. None of them touches a row
-- that ALREADY records the topic. Such a row keeps a live ACL binding at the broker, so
-- the credential issued for it keeps reading `<prefix>.system` — the exposure the change
-- exists to close, surviving in exactly the population that has been running longest.
--
-- Leaving it to an operator was the alternative and it is not one: nothing would surface
-- the rows, the topic name varies with KAFKA_TOPIC_PREFIX, and the remedy (a PUT that
-- re-states the whole topic list) is easy to get wrong under time pressure.
--
-- # What this does, in one statement each
--
--   1. REMOVES every `<anything>.system` entry from authorized_topics. Matched by suffix
--      rather than against a literal `blnk.system`, because the prefix is configuration
--      and a deployment running KAFKA_TOPIC_PREFIX=acme stores `acme.system`. The match
--      cannot over-reach: authorized_topics has only ever accepted names from Blnk's own
--      grantable allowlist, so a `%.system` entry in this column is necessarily the
--      internal category topic of this deployment's own namespace.
--   2. MARKS GRANT RECONCILIATION PENDING on every row it changed, by setting
--      grant_reconcile_pending_at. That is the obligation column the subscriber
--      settlement processor drains (sql/1781251447.sql): its next pass takes the
--      subscriber's provisioning claim, prunes the broker bindings the row no longer
--      implies — which is precisely the `<prefix>.system` Read and Describe — and clears
--      the marker only once the broker has confirmed. So the registry and the broker
--      converge without an operator issuing a single request, and a broker that is down
--      delays convergence rather than losing it.
--
-- COALESCE, not an unconditional assignment: a row that already owes a reconciliation
-- keeps its ORIGINAL pending instant, so this migration cannot reset the age of an
-- obligation that has been outstanding — and cannot therefore hide a backlog from the
-- settlement metrics that are read by age.
--
-- WHAT IS NOT DONE HERE. Nothing is written to Kafka: a migration cannot, and the
-- settlement processor is the component that owns broker reconciliation. Nothing is
-- deleted from the row beyond the one topic entry — the principal, the consumer group,
-- the credential reference and the issuance instant are untouched, so a subscriber keeps
-- the credential it already holds and simply stops being authorised for one topic.

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

-- IRREVERSIBLE BY DESIGN, and the reason is recorded rather than left as an empty section.
--
-- The removed entries were not retained anywhere: this is a grant withdrawal, so keeping a
-- copy would keep a record of an authorization the deployment has decided nobody may hold.
-- Re-adding the topic on a rollback would also be a WIDENING applied without any of the
-- checks a grant normally passes, and it would re-create the very ACL binding the
-- settlement pass has by then revoked.
--
-- A deployment that genuinely rolls back to a build where `<prefix>.system` was grantable
-- and wants a subscriber to hold it again re-records the topic through
-- PUT /subscribers/{subscriber_id}, which validates the grant and reconciles the broker.
-- That is one deliberate request per subscriber, which is the correct cost for restoring
-- access to a topic carrying verbatim internal error text.

SELECT 1;
