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

-- GIVE THE EVENT OUTBOX THE VACUUM POSTURE OF A QUEUE TABLE, because the cluster-wide
-- defaults are sized for a table that is mostly read.
--
-- Every row in this table is written once and then UPDATED at least twice more before it
-- reaches a terminal state: the claim stamps status, locked_until and claim_token; the
-- legacy leg stamps webhook_dispatched; the terminal transition stamps status,
-- dispatched_at and the broker coordinate. Each of those updates leaves a dead tuple
-- behind, and because status and locked_until are themselves indexed, none of them can be
-- a heap-only update — so each one also leaves a dead entry in every index that covers the
-- row. At 500 events a second that is on the order of 1,500 dead tuples a second, and an
-- index scan must walk the dead entries until a vacuum removes them.
--
-- The cluster defaults cannot keep up with that, and the failure is not a gradual
-- slowdown: it is a feedback loop. autovacuum_vacuum_cost_delay defaults to 2ms with a
-- cost limit of 200, which throttles a vacuum worker to a few megabytes a second, and
-- autovacuum_vacuum_scale_factor defaults to 0.2, so a table of half a million rows waits
-- for a hundred thousand dead tuples before a worker even starts. MEASURED on this
-- schema at 550 events a second offered through the API: the relay's claim began at 6ms,
-- and as dead index entries accumulated it rose monotonically through 131ms, 517ms,
-- 1,036ms, 2,551ms to 4,695ms, at which point the relay was completing one batch every
-- five seconds — 48 events a second against an offered 550. A slower claim means a larger
-- backlog, a larger backlog means more rows updated per unit of drained work, and more
-- updates mean more dead tuples: the loop tightens until autovacuum finally catches up,
-- and then it starts again.
--
-- These parameters are per-table, so they change the vacuum posture of THIS table and of
-- nothing else in the schema.
--
--   autovacuum_vacuum_cost_delay = 0
--       Do not throttle a vacuum of this table. This is the single most consequential
--       setting: with the default 2ms delay a worker sleeps far more than it works, and it
--       cannot remove dead entries as fast as the relay creates them.
--
--   autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 2000
--       Start a vacuum at two thousand dead tuples plus two percent of the live rows
--       rather than twenty percent. Each pass is then small and cheap, and the dead
--       population an index scan has to walk stays bounded by seconds of churn instead of
--       by a fifth of the table.
--
--   autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 1000
--       The claim's plan depends on the planner knowing roughly how many rows are
--       claimable, and that number swings between zero and the whole backlog within a
--       minute. Statistics that describe the table as it was ten minutes ago are how a
--       claim that should walk a handful of key heads chooses to scan a population
--       instead.
--
--   autovacuum_vacuum_insert_scale_factor = 0.05,
--   autovacuum_vacuum_insert_threshold = 5000
--       Insert-triggered vacuums maintain the visibility map, which is what allows the
--       claim's index-only scans to stay index-only. An append-heavy table with the
--       default 0.2 leaves large stretches of the heap unmarked, and every index-only scan
--       over them becomes a heap fetch.
--
--   toast.autovacuum_vacuum_cost_delay = 0
--       payload, payload_raw and event_raw are wide enough that a large event is stored
--       out of line, so the TOAST table churns with the same 1,500 updates a second and
--       needs the same freedom from throttling.
--
-- Down resets each parameter to the cluster default rather than restating a number, so a
-- rollback leaves the table exactly as it was before this migration and a later change to
-- the cluster defaults is inherited.
ALTER TABLE blnk.event_outbox SET (
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold = 1000,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_vacuum_insert_threshold = 5000,
    toast.autovacuum_vacuum_cost_delay = 0
);

-- THE SUBSCRIBER REGISTRY IS NOT GIVEN THE SAME TREATMENT, deliberately. It is written
-- once per subscriber and updated on issuance and revocation — thousands of rows over a
-- deployment's life, not thousands a second — so the cluster defaults describe it
-- correctly and a second set of exceptions would only be another thing to keep true.

-- +migrate Down

ALTER TABLE blnk.event_outbox RESET (
    autovacuum_vacuum_cost_delay,
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold,
    autovacuum_vacuum_insert_scale_factor,
    autovacuum_vacuum_insert_threshold,
    toast.autovacuum_vacuum_cost_delay
);
