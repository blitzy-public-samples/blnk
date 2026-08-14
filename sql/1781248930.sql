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

-- DROP event_subscribers_key_scope_chk: a subscriber MAY record a partition key prefix
-- while holding a Kafka credential.
--
-- 1781248920.sql made "partition_key_prefix recorded AND credential_reference recorded"
-- unrepresentable, as the third barrier behind two service-layer refusals. All three
-- implemented one policy: because Kafka's authorizer has no message-key dimension, a
-- recorded prefix was treated as an authorization that must never coexist with a
-- credential, and credential issuance was REFUSED while one was present.
--
-- That policy contradicts the access model this feature is required to deliver. A
-- subscriber's boundary is defined as its authorised TOPICS, its CONSUMER GROUP and its
-- partition-key prefix, and the credential endpoint is required to provision against
-- that record — the prefix included — and return the broker endpoint, the topic list,
-- the group id and the SASL credentials.
ALTER TABLE blnk.event_subscribers
    DROP CONSTRAINT IF EXISTS event_subscribers_key_scope_chk;

-- +migrate Down

-- Re-adding the constraint restores the schema 1781248920.sql established, which is
-- what a down migration owes: it undoes this file's change to the schema and nothing
-- else.
--
-- IT CAN FAIL, and failing is correct. Once issuance stopped refusing, rows recording a
-- prefix beside a live credential are legitimate and expected, so the validating scan
-- will reject any database that holds one.
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
