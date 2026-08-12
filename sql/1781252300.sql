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

-- RESTATE THE KEY-SCOPE COLUMN COMMENT FOR THE GATEWAY MODEL.
--
-- This replaces the comment sql/1781249138.sql set on the column, which described the
-- prefix as "ENFORCED AT THE CONSUMER, NOT AT THE BROKER" with
-- partition_key_prefix_enforced hard-coded false. Neither describes the shipped model:
-- consumer-side filtering asked the credential holder to police its own boundary while
-- any other client read the whole topic, so cooperation stood in for an access control.
--
-- The comment below states the gateway model instead — the deployment names the component
-- that enforces the key scope in KAFKA_KEY_SCOPE_ENFORCEMENT, a key-scoped principal is
-- granted Describe and no topic Read, and issuance refuses a key-scoped row where no such
-- component is declared.
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
