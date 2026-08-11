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

-- RESTATE THE KEY-SCOPE COLUMN COMMENT FOR THE GATEWAY MODEL (MAJ-1).
--
-- # What this corrects
--
-- sql/1781249138.sql set this comment when the consumer-side reading of a partition key
-- prefix was still the shipped model. It says the prefix is "ENFORCED AT THE CONSUMER,
-- NOT AT THE BROKER" and that the credential endpoint returns it "together with
-- partition_key_prefix_enforced = false".
--
-- Both statements have since been superseded, and both now describe behaviour the code
-- does not have:
--
--   * Consumer-side filtering was withdrawn as an authorization boundary. The party asked
--     to apply the filter was the party holding the credential, and any other Kafka
--     client read the whole topic — so cooperation was doing the work an access control
--     was credited with. A key-scoped principal is now granted Describe and NO topic
--     Read, so the broker refuses every direct fetch it attempts, and the records reach
--     it through the key-authorising component the deployment declared in
--     KAFKA_KEY_SCOPE_ENFORCEMENT.
--   * partition_key_prefix_enforced is therefore no longer hard-coded false. It is TRUE
--     for a recorded prefix in a deployment that declares such a component, and false
--     where none is declared — in which case credential issuance refuses the row outright
--     rather than emitting a response about it.
--
-- # Why a migration rather than a code change
--
-- The comment is a LIVE SCHEMA OBJECT. It is what an operator reads from `\d+
-- blnk.event_subscribers` or from information_schema during an incident, and it is the
-- one description of this column that no amount of editing Go doc comments reaches. A
-- correction that stopped at the source would leave the database itself asserting a model
-- withdrawn two releases ago — which is the same defect the previous migration's own STEP
-- 2 was written to avoid, arriving one revision later.
--
-- It is a comment and nothing else: no table, index, constraint or datum is touched, so
-- this migration cannot fail on data and needs no repair step.
COMMENT ON COLUMN blnk.event_subscribers.partition_key_prefix IS
    'The third scope of the subscriber access model: the subscriber is entitled only to '
    'records whose message key carries this prefix. Because every Blnk event is keyed by '
    'ledger id, that is a ledger boundary. ENFORCED BY THE COMPONENT THE DEPLOYMENT '
    'DECLARES IN KAFKA_KEY_SCOPE_ENFORCEMENT, not by the broker: Kafka ACLs are '
    'topic-level and group-level and the authorizer has no message-key dimension, so a '
    'key-scoped principal is granted Describe and NO topic Read and the broker refuses '
    'its direct fetches. Where the deployment declares no such component, credential '
    'issuance REFUSES a row carrying this column (SUBSCRIBER_KEY_SCOPE_UNENFORCED) rather '
    'than minting one whose declared boundary nothing keeps; recording the column stays '
    'legitimate, and every read of the row reports the scope as requested rather than '
    'enforced. Where one IS declared, issuance additionally requires it to attest this '
    'principal and this prefix before a secret is generated, and the response reports '
    'partition_key_prefix_enforced = true with partition_key_scope_state = attested. NULL '
    'means no key scope. Superseded sql/1781248920.sql, which forbade this column '
    'alongside credential_reference, and sql/1781249138.sql, which described the '
    'withdrawn consumer-side filtering model.';

-- +migrate Down

-- Restore the previous statement verbatim, so the down direction leaves the schema
-- describing exactly what sql/1781249138.sql left it describing and nothing else.
--
-- Rolling back the WORDING does not roll back the behaviour, and that asymmetry is
-- inherent to a documentation object: the code in the binary decides what happens, and a
-- rollback of this file restores the comment its predecessor set. That is the correct
-- down direction regardless — this migration's whole content is the comment, so undoing
-- it means putting the old comment back.
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
