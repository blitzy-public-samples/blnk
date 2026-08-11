#!/usr/bin/env bash
# Copyright 2024 Blnk Finance Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Provisions a RUNNING Kafka broker for Blnk event streaming. Three things are created, in
# this order: every category topic and its dead-letter sibling, the steady-state PRODUCER
# principal with its Write grant, and one sample SUBSCRIBER principal with its Read grant.
# That is the whole job.
#
# TWO PRINCIPALS, NEITHER OF THEM THE ADMINISTRATOR
#
# The producer is what the SERVER publishes as - the outbox relay and the dead-letter writer
# are the only producers in Blnk and both run in the server role, so the worker presents no
# broker credential at all. It is the load-bearing principal:
# config.KafkaConfig REFUSES to publish as the administrative principal, so with an
# administrative pair configured and no producer pair Blnk's event publisher fails to
# construct and the server does not start. The administrator is a cluster superuser - it creates
# topics, mints SCRAM credentials and rewrites ACLs - so publishing as it would make a leaked
# producer credential a compromise of the cluster's authorization state rather than the
# ability to publish events.
#
# The producer's grant is Write and Describe on the Blnk-owned topics and nothing else: no
# Read, no consumer group, no cluster operation. The subscriber's is the mirror image - Read
# and Describe on the topics it may consume, and Read on its own consumer-group namespace -
# and it is granted only the topics model.SubscriberAuthorizableTopics allows, which excludes
# every dead-letter sibling. The sample principal's default grant is narrower still: the four
# tenant category topics, without the internal system topic a real subscriber may be granted
# where the deployment has declared KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS.
#
# NO CREDENTIAL IS EVER PRINTED. A generated password is written to a mode-0600 file the
# operator nominates through KAFKA_PRODUCER_SECRET_FILE or
# KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE, and only the PATH is reported; a supplied one is not
# echoed. With neither a supplied secret nor a destination file, the principal is skipped and
# the reason is printed - because this script runs as the compose kafka-init service, whose
# stdout is a container log that would retain the credential indefinitely, hand it to anyone
# who can run "docker compose logs", and forward it to whatever collects the host's logs.
# "Shown once" is not a property a log line can have. stack.sh --init generates both secrets
# into a mode-0600 .env, which is the intended local route and needs no file at all.
#
# WHAT IS CREATED
#
# Every category topic and its dead-letter sibling, all names derived from
# KAFKA_TOPIC_PREFIX (default "blnk"):
#
#     <prefix>.transactions        <prefix>.transactions.dlt
#     <prefix>.balances            <prefix>.balances.dlt
#     <prefix>.identities          <prefix>.identities.dlt
#     <prefix>.ledgers             <prefix>.ledgers.dlt
#     <prefix>.system              <prefix>.system.dlt
#
# Only the CATEGORY names are written down below; each dead-letter name is derived by
# appending ".dlt", exactly as event_topics.go's DLTFor does. Deriving rather than listing
# is what structurally prevents the two halves of the catalogue drifting apart.
#
# There are TWO categories more than the three named in the requirement because two real event
# types - ledger.created and system.error - belong to none of transactions, balances or
# identities, while the requirement also demands that every event formerly delivered by webhook
# be published AND, once the webhook transport retires, still be reachable by a subscriber
# credential. They get a category each, split by audience: <prefix>.ledgers is tenant data and is
# granted like any other tenant topic, and <prefix>.system holds the internal error stream and
# doubles as the catch-all for an event type the catalogue does not recognise, so no event type
# is silently dropped.
#
# <prefix>.system IS CREATED AND THIS SCRIPT GRANTS IT TO NOBODY. It is an operator topic: it is
# absent from SUBSCRIBER_GRANTABLE_CATEGORIES below — mirroring
# model.SubscriberGrantableEventCategories — because system.error's payload is frozen by R-8 and
# renders Blnk's own error text verbatim, and because the category is the catch-all, so a grant
# of it would stand over every event type nobody has catalogued yet. A deployment that needs a
# subscriber reading it declares KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS and grants the topic
# through the subscriber API, against a named subscriber; the sample principal this script
# creates for a local walkthrough must not teach that grant as a default.
#
# Do not "correct" this to eight or twelve topics: model.EventCategory routes events into exactly
# these five and event_topics.go composes exactly these ten names from them. A name this
# script does not create is a name the relay cannot publish to, and a name it creates that
# the code never writes to is dead weight in every environment.
#
# ORDERING: BOOTSTRAP, THEN BROKER, THEN THIS
#
# This script talks to a broker that is already up, over an authenticated SASL/SCRAM
# connection, so both of those must already be true when it runs:
#
#   1. scripts/kafka-bootstrap.sh has formatted the broker's storage and seeded the
#      administrative SCRAM credential into the KRaft metadata log. In KRaft mode a broker
#      cannot authenticate any SASL client until at least one credential is in that log, so
#      the credential this script authenticates with can only have been created there.
#   2. The broker is listening and has finished loading that metadata.
#
# Getting the order wrong does not produce a clear error on its own; it produces an
# authentication failure that looks like a wrong password. The readiness wait below names
# both causes when it times out for exactly that reason.
#
# IDEMPOTENCY, AND WHY THE EXIT CODE MATTERS
#
# Safe to run on every bring-up, and specified to be. Topic creation passes
# --if-not-exists, ACL addition is a no-op when the binding already exists, and the sample
# subscriber's SCRAM CREDENTIAL IS LEFT ALONE when it already exists. A topic with too FEW
# partitions is grown; a topic with MORE is left alone with a warning, because Kafka cannot
# reduce a partition count and doing so would move keys between partitions and break the
# per-aggregate ordering guarantee that keying by ledger ID exists to provide.
#
# Preserving the credential matters as much as the exit code, and it applies to BOTH
# principals. This script used to mint and upsert a NEW sample password on every run, so the
# routine bring-up an operator performs to assure topics silently invalidated the credential
# their local consumer was already using, and the only copy of the replacement was in that
# run's console output. Rotation is now an explicit request:
# KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET=1 or KAFKA_ROTATE_PRODUCER_SECRET=1, each needing a
# destination file, or supply the secret directly. For the producer the stakes are higher than
# a broken local consumer: rotating it out from under a running server stops it
# authenticating, which is why a rotation with nowhere to deliver the new value refuses
# outright rather than proceeding.
#
# NO CREDENTIAL IS EVER PRINTED. A generated sample password goes to the mode-0600 file named
# by KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE and the console is told only the path; without that
# variable, and without an explicit KAFKA_SAMPLE_SUBSCRIBER_SECRET, the run is refused before
# the broker is touched. Printing it to stdout - which under the compose kafka-init service
# IS the container log, written to disk and shipped onward by any collector - was CWE-532
# whatever the surrounding banner claimed about being "shown once, not stored".
#
# GEOMETRY IS VERIFIED, NOT ASSUMED. Every topic's partition count AND replication factor
# are read back from the broker. A factor below KAFKA_REPLICATION_FACTOR fails the run with
# reassignment guidance, and a geometry that cannot be read after three attempts fails too
# rather than being recorded as "unknown" - a run that verified nothing must not report
# success. The closing summary prints only observed values.
#
# A re-run that changes nothing therefore exits 0 and says so. That is a hard requirement,
# not a nicety: the compose kafka-init service is a one-shot with "restart: on-failure",
# following the migration service in docker-compose.dev.yaml, so a non-zero exit on an
# already-provisioned broker would restart this script forever.
#
# WHERE THIS RUNS: THREE CONTEXTS, ONLY ONE WITH THE CLI ON PATH
#
#   1. The compose "kafka-init" one-shot, inside a Kafka image. The CLI tools are present,
#      the broker answers on its compose service name, and ./scripts is mounted read-only
#      at /scripts - so nothing may ever be written inside this directory.
#   2. The root makefile's "kafka_provision" target, on the HOST, where the Kafka CLI tools
#      are very likely absent.
#   3. stack.sh's fallback on --up / --build / --restart, also on the host.
#
# Contexts 2 and 3 are handled by re-executing this whole script inside the broker
# container over docker, rather than by shipping each individual command across, which
# keeps the temporary credential file on the side that has to read it. See
# delegate_to_container. If neither route exists the failure names all three remedies
# instead of surfacing as "command not found".
#
# An operator-supplied KAFKA_CLIENT_CONFIG is NOT silently dropped when delegating. A host
# path is meaningless inside the container, so the delegation either uses
# KAFKA_CLIENT_CONFIG_CONTAINER_PATH - verified readable there first - or refuses. Writing a
# fresh configuration in the container instead would discard the operator's CA, truststore
# and mechanism choices and could downgrade a SASL_SSL connection to SASL_PLAINTEXT without
# a word. See delegate_client_config.
#
# Note that "the CLI is on PATH" is not the same question as "the CLI is installed": Apache
# Kafka images install the tools in /opt/kafka/bin and do not add that directory to PATH,
# so PATH is probed first and the well-known installation directories after it.
#
# WHAT IS DELIBERATELY NOT HERE
#
# This script and scripts/kafka-bootstrap.sh are the only two places in Blnk that use the
# Kafka CLI. Application code never shells out: kafka-go exposes CreateTopics,
# CreatePartitions, CreateACLs, AlterUserScramCredentials, DescribeUserScramCredentials,
# OffsetFetch and ListOffsets natively, and event_admin.go performs every one of these
# operations in process at runtime. These two scripts exist only because bootstrapping has
# to happen before any broker - and therefore before any Go process - can be reached.
#
# So nothing is added here that event_admin.go already does at runtime. What IS here is
# kept behaviourally identical to it, because operators will use both interchangeably:
# same grow-never-shrink rule for partitions, same SCRAM-SHA-512 with an iteration floor of
# 4096, same grant shape of Read plus Describe on topics with a literal pattern and Read on
# the consumer group with a prefixed pattern.
#
# CONFIGURATION
#
# Every value comes from the environment; there are no required arguments and no options to
# parse. Each variable is documented at its point of use in the configuration block below.
# In summary, with defaults:
#
#   KAFKA_BROKERS                         (unset)               comma-separated broker list;
#                                                               DECLARED-BUT-EMPTY means
#                                                               "Kafka not configured" and
#                                                               provisioning is skipped
#   KAFKA_BOOTSTRAP_SERVER                (unset)               explicit override, wins over
#                                                               KAFKA_BROKERS and the skip
#   KAFKA_TOPIC_PREFIX                    blnk
#   KAFKA_MIN_PARTITIONS                  6                     a lower value is RAISED to 6
#   KAFKA_REPLICATION_FACTOR              1                     1 locally, 3 in production;
#                                                               verified per topic, and a
#                                                               shortfall fails the run
#   KAFKA_SASL_ADMIN_USER                 (no default)          required WITH the secret
#   KAFKA_SASL_ADMIN_SECRET               (no default)          required WITH the user
#   KAFKA_SECURITY_PROTOCOL               SASL_PLAINTEXT        SASL_SSL in production;
#                                                               PLAINTEXT/SSL with no
#                                                               credential pair
#   KAFKA_CLIENT_CONFIG                   (unset)               use this properties file
#                                                               verbatim instead of writing
#                                                               a temporary one; makes the
#                                                               credential pair unnecessary
#   KAFKA_CLIENT_CONFIG_CONTAINER_PATH    (unset)               that file's path INSIDE the
#                                                               broker container, needed
#                                                               only when delegating
#   KAFKA_SCRAM_ITERATIONS                4096                  the SCRAM minimum
#   KAFKA_PROVISION_TIMEOUT_SECONDS       60                    readiness budget
#   KAFKA_PROVISION_POLL_INTERVAL_SECONDS 2                     readiness poll interval
#   KAFKA_SASL_USER                       blnk-producer         the STEADY-STATE PUBLISHER
#                                                               principal, so the publisher
#                                                               need not authenticate as the
#                                                               cluster administrator
#   KAFKA_SASL_SECRET                     (unset)               supply one, or name a file
#                                                               below; never printed
#   KAFKA_SASL_SECRET_FILE                (unset)               where a generated producer
#                                                               password is written, mode 0600
#   KAFKA_ROTATE_PRODUCER_SECRET          (unset)               truthy replaces an existing
#                                                               producer password; off by
#                                                               default so a re-run does not
#                                                               stop the running publisher
#   KAFKA_SAMPLE_SUBSCRIBER_USER          blnk-sample-subscriber
#   KAFKA_SAMPLE_SUBSCRIBER_SECRET        (unset)               supply one, or name a file
#                                                               below to receive a generated
#                                                               one; a generated password is
#                                                               NEVER printed
#   KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE   (unset)               where a generated password is
#                                                               written, mode 0600. Required
#                                                               whenever no explicit secret
#                                                               is supplied
#   KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET (unset)               truthy replaces an existing
#                                                               password; off by default so
#                                                               re-runs do not break the
#                                                               consumer already using it
#   KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX  (derived)             leave it unset: the namespace
#                                                               follows the principal. A
#                                                               supplied value is only
#                                                               cross-checked against it,
#                                                               with or without the trailing
#                                                               '.' delimiter
#   KAFKA_SAMPLE_SUBSCRIBER_TOPICS        (the grantable category topics)
#   KAFKA_SKIP_SAMPLE_SUBSCRIBER          (unset)               truthy skips the principal
#                                                               and its ACLs entirely
#   KAFKA_PRODUCER_USER                   blnk-producer         the identity Blnk's relay
#                                                               publishes as; Write and
#                                                               Describe on every owned
#                                                               topic, nothing else
#   KAFKA_PRODUCER_SECRET                 (unset)               generated into the 0600 file
#                                                               named by *_SECRET_FILE and
#                                                               never printed; no file and
#                                                               no secret skips the principal
#   KAFKA_ROTATE_PRODUCER_SECRET          (unset)               truthy replaces an existing
#                                                               password
#   KAFKA_SKIP_PRODUCER                   (unset)               truthy skips the producer's
#                                                               validation, principal and
#                                                               ACLs entirely. The retired
#                                                               KAFKA_SKIP_PRODUCER_PRINCIPAL
#                                                               is refused with this name
#   KAFKA_CONTAINER                       kafka                 for docker exec delegation
#   KAFKA_COMPOSE_SERVICE                 kafka                 for docker compose exec
#   KAFKA_PROVISION_CONTAINER_SCRIPT      /scripts/kafka-provision.sh
#
# The administrative username and secret are ONE PAIR: both set means SASL/SCRAM, both empty
# means no SASL (and then KAFKA_SECURITY_PROTOCOL must not be a SASL_* protocol), and
# exactly one set is refused by name. Neither is defaulted - the username used to fall back
# to the literal principal "admin", which made a single .env mean "authenticate as admin"
# here and "no SASL at all" to the Go clients. Both values must also be drawn from the safe
# credential alphabet documented further down, because Kafka's --add-config grammar and the
# JAAS properties value have no escape sequence between them.
#
# REQUIREMENTS
#
# bash 4.4 or later, plus EITHER the Kafka CLI tools (kafka-topics, kafka-configs,
# kafka-acls, in unsuffixed or .sh form) OR a docker that can reach a running Kafka
# container. The bash floor is not decorative: this script runs under "set -u", and
# expanding an empty array - which the delegation path does - was only made safe in 4.4.
# Every image in the stack is well past that.
#
# Nothing is written inside the repository. The single temporary file this script creates
# lives under TMPDIR and is removed on every exit path.
#
# USAGE
#
#   scripts/kafka-provision.sh              provision, using the environment above
#   scripts/kafka-provision.sh -h|--help    print the usage summary and exit 0
#
# No other arguments, in any context, and an unexpected one is REJECTED before any side
# effect rather than ignored. "make kafka_provision" invokes it bare.

# Error handling
set -euo pipefail

# Colours, declared on one line as in stack.sh, and emptied when stderr is not a terminal
# so that container logs and CI transcripts stay free of escape sequences.
if [[ -t 2 ]]; then
    GRN=$'\033[0;32m' && YEL=$'\033[1;33m' && RED=$'\033[0;31m' && BLU=$'\033[0;34m' && NC=$'\033[0m'
else
    GRN='' && YEL='' && RED='' && BLU='' && NC=''
fi

# ---------------------------------------------------------------------------------------
# Configuration
#
# Every tunable is read here, once, and nowhere else in the body.
#
# .env.example is the source of truth for the six names it declares under "Kafka event
# streaming" - KAFKA_BROKERS, KAFKA_TOPIC_PREFIX, KAFKA_SASL_ADMIN_USER,
# KAFKA_SASL_ADMIN_SECRET, KAFKA_MIN_PARTITIONS and KAFKA_REPLICATION_FACTOR - and the
# defaults below track it. The rest are provisioning-only knobs that no other component
# reads, so they are deliberately absent from .env.example rather than duplicated into it.
# ---------------------------------------------------------------------------------------

# Whether KAFKA_BROKERS was DECLARED, captured before anything can overwrite it.
#
# The distinction between unset and set-but-empty is load-bearing and cannot be made with
# the usual ${VAR:-default} form, which treats the two identically. An operator who has
# deliberately emptied KAFKA_BROKERS - which is what .env.example ships, and a legitimate
# steady state in which the event publisher resolves to its no-op implementation - must get
# a clean skip, not a provisioning attempt against a broker that is not there. The ${VAR+x}
# form answers the question actually being asked: is the name declared at all?
KAFKA_BROKERS_DECLARED="${KAFKA_BROKERS+declared}"
KAFKA_BROKERS="${KAFKA_BROKERS:-}"

# The broker to provision. An explicit KAFKA_BOOTSTRAP_SERVER wins over KAFKA_BROKERS, and
# also overrides the skip above, which is how the compose kafka-init service provisions a
# broker while .env still carries an empty KAFKA_BROKERS for the application's benefit.
#
# KAFKA_BROKERS is comma-separated, which is exactly the form --bootstrap-server accepts,
# so it passes straight through with no reformatting. The default is the compose service
# name and its SASL port; a broker published on the host is reached with
# KAFKA_BOOTSTRAP_SERVER=localhost:9092.
KAFKA_BOOTSTRAP_SERVER_EXPLICIT="${KAFKA_BOOTSTRAP_SERVER:+explicit}"
KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-${KAFKA_BROKERS:-kafka:9092}}"

# The namespace every topic name is derived from. Must match config.Kafka.TopicPrefix, or
# this script provisions topics the relay never publishes to - a failure with no symptom
# beyond events piling up in the outbox.
KAFKA_TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX:-blnk}"

# Partitions per topic. Six is the required minimum, and a lower value is RAISED to it
# rather than applied - see require_valid_geometry. An EMPTY under-partitioned topic is grown
# to the resolved count; one that holds records is not, unless
# KAFKA_ALLOW_PARTITION_GROWTH permits it. Nothing is ever shrunk.
KAFKA_MIN_PARTITIONS="${KAFKA_MIN_PARTITIONS:-6}"

# Consent to grow a topic that ALREADY HOLDS RECORDS.
#
# Growing partitions is not a safe no-op on a live topic. Kafka assigns a record to a
# partition by hashing its key modulo the partition COUNT, so raising the count re-maps
# existing keys to different partitions - and for Blnk that key is the ledger id, which is the
# whole mechanism behind the per-aggregate ordering guarantee. Events for one ledger written
# before the change sit on one partition and events written after it on another, and a
# consumer reading partitions independently can then observe them out of order. No error is
# raised anywhere; the guarantee simply stops holding for keys already in flight.
#
# So growth is gated on the topic being EMPTY, which is the case that cannot break ordering
# because there is nothing to re-map. A non-empty topic is left alone with a warning, and the
# run still succeeds - an under-partitioned topic is a throughput limit, not an outage, and
# failing a bring-up over one would be worse than reporting it.
#
# Setting this to a truthy value overrides the refusal, for the operator who has judged the
# re-mapping acceptable. It mirrors config.KafkaConfig.AllowPartitionGrowth and
# event_admin.go's partitionGrowthDecision, which apply the identical rule at runtime; the two
# must agree, or provisioning and start-up would disagree about the same topic.
KAFKA_ALLOW_PARTITION_GROWTH="${KAFKA_ALLOW_PARTITION_GROWTH:-}"

# Consent to proceed when the broker CANNOT SAY whether a principal already holds a
# SCRAM-SHA-512 credential.
#
# Unset, an indeterminate probe leaves THAT PRINCIPAL alone — no credential is written, the
# disposition records "indeterminate" rather than "skipped" so the summary can say why, and the
# principal's ACLs are still asserted because those are idempotent. That is the safe direction
# and it is not theoretical: the preservation decision is the only thing standing between a
# routine re-run and a silent password replacement, and "cannot tell" used to be recorded as
# "absent", which sent control into the generate-and-upsert arm. A live credential was then
# overwritten with a freshly generated one, under no rotation flag, and every consumer
# authenticating with the old password failed at once — while this script reported a successful
# provisioning run.
#
# The run itself CONTINUES. Refusing to guess and aborting are different reactions and only the
# first is required: aborting would also discard the idempotent ACL repair an operator re-running
# this came for, and would turn a broker that is briefly unauthorised to describe user configs
# into a compose bring-up that cannot start at all, because kafka-init is a one-shot the
# application services wait on.
#
# Set, an indeterminate probe is treated as ABSENT, restoring that behaviour deliberately for
# the one deployment where it is correct: a broker that does not permit describe on user
# entities, where the probe can never succeed and the alternative is a principal that can never
# be provisioned. The log says plainly what may be overwritten.
KAFKA_ALLOW_SCRAM_PROBE_FAILURE="${KAFKA_ALLOW_SCRAM_PROBE_FAILURE:-}"

# Replication factor, configuration-driven with a LOCAL default of 1. Deliberately not 3,
# and deliberately not hard-coded either way.
#
# A single-broker KRaft cluster cannot satisfy a replication factor of 3: the broker
# answers CreateTopics with INVALID_REPLICATION_FACTOR and topic creation fails outright.
# So a hard-coded 3 would make local bring-up impossible, while a hard-coded 1 would
# silently discard the durability requirement in production. The production Kubernetes
# manifests supply 3 through this same variable; .env.example pins 1 for the local stack.
# Please do not "fix" the default to 3.
KAFKA_REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

# The SASL/SCRAM administrative principal this script authenticates as.
#
# NO DEFAULT, DELIBERATELY. This used to fall back to the literal principal "admin", which
# made one .env mean two different things: the scripts authenticated as "admin" while the
# Go clients read the same empty value as "no SASL configured" and connected anonymously.
#
# The username and the secret are ONE PAIR with a single meaning, identical here, in
# scripts/kafka-bootstrap.sh, and in config.KafkaConfig.SASLAdminCredentials:
#
#     both set          SASL/SCRAM-SHA-512 as the named principal
#     both empty        no SASL. Supported, and it means the broker must be reached over a
#                       non-SASL protocol - set KAFKA_SECURITY_PROTOCOL=PLAINTEXT or SSL,
#                       because asking for SASL_* with no credential is a contradiction
#                       rather than a default
#     exactly one set   a misconfiguration, refused by name
#
# The pair is not needed at all when KAFKA_CLIENT_CONFIG supplies a client-properties file:
# that file carries whatever authentication the broker needs, and require_admin_credentials
# says so. See F-4 in the resolution notes and prepare_client_config below.
KAFKA_SASL_ADMIN_USER="${KAFKA_SASL_ADMIN_USER:-}"

# That principal's password. Deliberately has no default and never will: a credential must
# not be guessable from source. Validated by require_admin_credentials, written once into
# the client-properties file, and never printed.
KAFKA_SASL_ADMIN_SECRET="${KAFKA_SASL_ADMIN_SECRET:-}"

# Security protocol for the admin connection. SASL_PLAINTEXT is right for the local
# single-broker stack; production pairs SCRAM with TLS, so set SASL_SSL there (and supply
# the truststore settings through KAFKA_CLIENT_CONFIG). Set PLAINTEXT or SSL when running
# against a broker with no SASL listener, which is the both-credentials-empty mode above.
KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-SASL_PLAINTEXT}"

# An operator-supplied client-properties file. When set it is used verbatim: nothing is
# generated, nothing is deleted, and no credential is read from the environment. This is
# the way to add TLS material, a different mechanism, or any other CLI client setting this
# script does not model.
#
# It is a path on the machine that will RUN the CLI. When this script has to delegate into
# the broker container, a host path is meaningless there, so the delegation does not
# silently drop it or regenerate a different file - it either uses
# KAFKA_CLIENT_CONFIG_CONTAINER_PATH or refuses. See delegate_to_container.
KAFKA_CLIENT_CONFIG="${KAFKA_CLIENT_CONFIG:-}"

# Where the operator-supplied client configuration can be read INSIDE the broker container.
#
# Only consulted when KAFKA_CLIENT_CONFIG is set and this script has to delegate. Set it to
# the in-container path of a mounted properties file - for example, mount
# ./kafka-client.properties at /etc/blnk/kafka-client.properties and point this at that -
# and the delegated run uses exactly the settings you chose. It is verified as readable
# inside the container before anything is provisioned, so a wrong path fails immediately
# rather than authenticating with something else.
KAFKA_CLIENT_CONFIG_CONTAINER_PATH="${KAFKA_CLIENT_CONFIG_CONTAINER_PATH:-}"

# PBKDF2 iteration count for the sample subscriber's SCRAM credential. 4096 is the minimum
# Kafka accepts and the default here, matching event_admin.go's DefaultScramIterations.
KAFKA_SCRAM_ITERATIONS="${KAFKA_SCRAM_ITERATIONS:-4096}"

# Readiness budget. Compose gates kafka-init on the broker healthcheck, but the makefile
# target and stack.sh's fallback do not, so the wait cannot be skipped - and it cannot be
# unbounded either, or a broker that never comes up hangs the bring-up instead of failing
# it.
KAFKA_PROVISION_TIMEOUT_SECONDS="${KAFKA_PROVISION_TIMEOUT_SECONDS:-60}"
KAFKA_PROVISION_POLL_INTERVAL_SECONDS="${KAFKA_PROVISION_POLL_INTERVAL_SECONDS:-2}"

# Per-call bound on every Kafka CLI invocation. Distinct from the readiness budget above,
# which bounds only the initial wait: once the broker has answered once, every later call would
# otherwise run unbounded, so ONE hung operation hangs the entire run with no deadline to end
# it. See kafka_cli_timeout.
#
# 30 seconds is generous for a single administrative operation against a reachable broker -
# topic creation, a SCRAM upsert and an ACL write are all sub-second in practice - and short
# enough that a hang is reported while an operator is still watching. The kill grace gives the
# JVM a window to exit on TERM before SIGKILL, so a CLI that is merely slow to shut down is not
# reported as unkillable.
KAFKA_CLI_TIMEOUT_SECONDS="${KAFKA_CLI_TIMEOUT_SECONDS:-30}"
KAFKA_CLI_KILL_GRACE_SECONDS="${KAFKA_CLI_KILL_GRACE_SECONDS:-5}"

# THE STEADY-STATE PRODUCER PRINCIPAL, and why it has to exist at all.
#
# The event publisher inside the SERVER process authenticates as
# KAFKA_SASL_USER / KAFKA_SASL_SECRET. Leaving that pair unset makes it fall back to the
# ADMINISTRATIVE principal, which is a super.user on this broker: it can create and delete
# topics, mint and revoke SCRAM credentials for every subscriber, and rewrite every ACL. The
# publisher is the busiest and most exposed component in the deployment, so publishing every
# ledger event as that identity turns a leaked producer credential into full control of the
# cluster's authorization state rather than the ability to write events. It also makes the
# broker's audit trail useless, because routine publishing and administration arrive as the
# same principal.
#
# event_publisher.go already prefers this pair and logs an excess-privilege warning whenever
# it falls back - but nothing created the principal, so the local stack took the fallback on
# every run and the warning became background noise. This script now provisions it: a SCRAM
# credential, plus Write and Describe on the topics Blnk owns and nothing else.
#
# The grant is deliberately NOT symmetric with the subscriber's. A producer needs Write, must
# not have Read (it never consumes), and needs the dead-letter topics as well as the category
# topics, because the relay writes an exhausted event to its category's .dlt sibling and the
# replay endpoint writes it back to the original topic.
#
# The secret is delivered exactly as the sample subscriber's is: supply it, or name a
# mode-0600 file to receive a generated one. It is never printed.
# DEFAULTED TO EMPTY, not to blnk-producer, because empty here means "not stated" and the
# fallback belongs in one place. require_valid_producer resolves the producer identity as
# KAFKA_SASL_USER, then KAFKA_PRODUCER_USER, then the default - which is EXACTLY the precedence
# the compose files apply when they hand the same pair to the server
# (KAFKA_SASL_USER: ${KAFKA_SASL_USER:-${KAFKA_PRODUCER_USER:-}}). The two cannot diverge, which
# is the whole point: the principal this script MINTS has to be the principal the relay
# PRESENTS, and a separate default here is how those two quietly become different names.
KAFKA_SASL_USER="${KAFKA_SASL_USER:-}"
KAFKA_SASL_SECRET="${KAFKA_SASL_SECRET:-}"
KAFKA_SASL_SECRET_FILE="${KAFKA_SASL_SECRET_FILE:-}"

# Rotate the producer's password on this run. Off by default, for the same reason the
# subscriber's is: a re-run that silently invalidated the credential the running server and
# worker authenticate with would stop event publishing until both were restarted with the
# new value.
KAFKA_ROTATE_PRODUCER_SECRET="${KAFKA_ROTATE_PRODUCER_SECRET:-}"

# KAFKA_SKIP_PRODUCER_PRINCIPAL IS RETIRED, and is deliberately NOT declared here.
#
# Defaulting it the way every live variable above is defaulted would make it declared on every
# run, and require_no_retired_variables tests DECLARATION - so the refusal would fire
# unconditionally and no run could ever provision anything. The check reads the raw
# environment instead, which is the only place the distinction between "the operator set this"
# and "the script defaulted this" survives.
#
# There were two variables, and they gated DIFFERENT HALVES of the same decision:
# KAFKA_SKIP_PRODUCER_PRINCIPAL skipped the producer VALIDATION, KAFKA_SKIP_PRODUCER skipped
# the broker MUTATION. Neither entry point could express "skip the producer" completely,
# because the forwarding was split too - stack.sh forwarded only the first and the compose
# kafka-init service only the second. So a compose stack that set the flag had its principal
# left alone but was still refused for, say, an empty KAFKA_SASL_USER it had no intention of
# using; and a stack.sh run that set it passed validation and then created the very principal
# it had asked to be left alone. Both directions are silent, and each looks like a bug in the
# other half.
#
# KAFKA_SKIP_PRODUCER is the survivor: it is the name compose already forwards, the name that
# gates the actual mutation, and the symmetric partner of KAFKA_SKIP_SAMPLE_SUBSCRIBER. See
# KAFKA_PROVISION_RETIRED_VARIABLES.

# The sample subscriber principal, its consumer-group namespace, and the topics it may
# read. See ensure_sample_subscriber and grant_subscriber_acls for the grant shape and for
# why the dead-letter topics are excluded from the default.
KAFKA_SAMPLE_SUBSCRIBER_USER="${KAFKA_SAMPLE_SUBSCRIBER_USER:-blnk-sample-subscriber}"
KAFKA_SAMPLE_SUBSCRIBER_SECRET="${KAFKA_SAMPLE_SUBSCRIBER_SECRET:-}"
# NOT DEFAULTED, deliberately. The consumer-group namespace is DERIVED from the principal by
# require_valid_subscriber, so there is no default for this variable to supply — and giving it
# one would make "was it overridden?" indistinguishable from "was it left alone", so the
# override refusal would fire on every run. It is read only to detect a stale override and
# tell the operator it is no longer honoured.
KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX="${KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX:-}"
KAFKA_SAMPLE_SUBSCRIBER_TOPICS="${KAFKA_SAMPLE_SUBSCRIBER_TOPICS:-}"

# Where a GENERATED credential is delivered, and the reason a destination is required rather
# than optional.
#
# A generated password used to be printed to stdout. In the compose stack this script runs as
# the kafka-init service, so stdout IS the container log: the credential was retained for the
# lifetime of the container, readable by anyone who could run "docker compose logs", shipped
# to whatever log collector the host has, and impossible to redact after the fact. A password
# printed once into a permanent log is not a password shown once.
#
# So generation now needs somewhere permissioned to put the result. Set this to a path on a
# WRITABLE mount and the credential is written there with mode 0600 and the path - never the
# value - is reported. Leave it unset and the credential is not generated at all: an
# explicitly supplied KAFKA_*_SECRET is used if present, otherwise the principal is skipped
# with an explanation. stack.sh --init generates both secrets into the mode-0600 .env, which
# is the intended local route and needs no file at all.
KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE="${KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE:-}"

# The STEADY-STATE PRODUCER principal: the identity the relay and every publishing process
# authenticate as, and the reason it exists separately from the administrative one.
#
# The administrative principal is a cluster superuser - it creates topics, mints SCRAM
# credentials and rewrites ACLs. Publishing every ledger event as that principal means a
# leaked producer credential is not "someone can publish events", it is "someone can rewrite
# the cluster's authorization state". config.KafkaConfig therefore refuses to publish as the
# administrator: with an administrative pair configured and no producer pair, the event
# publisher fails to construct at start-up rather than falling back. This is the principal
# that satisfies it, and its grant is Write and Describe on the Blnk-owned topics and nothing
# else - no Read, no group, no cluster operation.
KAFKA_PRODUCER_USER="${KAFKA_PRODUCER_USER:-blnk-producer}"
KAFKA_PRODUCER_SECRET="${KAFKA_PRODUCER_SECRET:-}"
KAFKA_PRODUCER_SECRET_FILE="${KAFKA_PRODUCER_SECRET_FILE:-}"

# Rotate the sample subscriber's password on this run.
#
# Off by default, and that default is the whole point. Re-running this script used to mint
# and upsert a new sample password every single time, so the routine bring-up an operator
# performs to assure topics silently invalidated the credential their local consumer was
# already using - and the only copy of the new one was in that run's console output. The
# credential is now PRESERVED when the principal already holds a SCRAM-SHA-512 credential,
# and rotation is an explicit request: set this truthy, or supply
# KAFKA_SAMPLE_SUBSCRIBER_SECRET to set a known password. See ensure_sample_subscriber.
KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET="${KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET:-}"

# Escape hatch for a shared or production broker, where minting a sample credential is
# exactly the wrong thing to do. Set it truthy and the principal and its ACLs are skipped;
# the topics are still assured.
KAFKA_SKIP_SAMPLE_SUBSCRIBER="${KAFKA_SKIP_SAMPLE_SUBSCRIBER:-}"

# The PRODUCER principal: the identity Blnk's own relay publishes ledger events as.
#
# It exists because the alternative was the administrator. Before this principal, the local
# stack configured only KAFKA_SASL_ADMIN_USER, so the event publisher authenticated as the
# principal that creates topics, alters SCRAM credentials and grants ACLs - and a leaked
# producer credential handed over the cluster's authorization state rather than the ability
# to publish. The Go side now REFUSES that fallback unless KAFKA_ALLOW_ADMIN_PRODUCER is set
# deliberately, which means a local stack without this principal cannot publish at all. So
# provisioning it is not an optional extra here; it is what makes the default configuration
# work.
#
# Its grant is Write and Describe on every Blnk-owned topic, category and dead-letter alike:
# the relay produces to the category topics and the dead-letter writer produces to the .dlt
# siblings, so a grant covering only the former fails exactly when an event needs
# dead-lettering - the worst possible moment to discover a missing binding. It is granted no
# Read, no Create, no Alter, and nothing at cluster scope. See ensure_producer_principal.

# THE ONE escape hatch for a broker where the producer principal is managed elsewhere - by a
# platform team, a secrets operator or an existing deployment's own tooling. Set it truthy
# and the producer's VALIDATION, its principal and its ACLs are all skipped; the topics are
# still assured.
#
# One variable, one behaviour, one declaration. It was declared twice in this block and
# shared its job with a second variable, KAFKA_SKIP_PRODUCER_PRINCIPAL, that gated the
# validation half - see that name above for what the split cost. Everything that skips
# anything about the producer now reads THIS name: require_valid_producer,
# require_rotation_destination, ensure_producer_principal, generate_password's guidance and
# the closing summary.
KAFKA_SKIP_PRODUCER="${KAFKA_SKIP_PRODUCER:-}"

# Delegation targets, overridable because a stack may name its broker anything. The
# defaults are the compose service name, which .env.example also pins as KAFKA_CONTAINER.
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_COMPOSE_SERVICE="${KAFKA_COMPOSE_SERVICE:-kafka}"

# Where this script is expected to be found inside the broker container. The compose
# read-only mount of ./scripts guarantees this path in the compose stack; when it is absent
# the script is streamed in instead. See delegate_to_container.
KAFKA_PROVISION_CONTAINER_SCRIPT="${KAFKA_PROVISION_CONTAINER_SCRIPT:-/scripts/kafka-provision.sh}"

# Recursion guard. Set when this script re-executes itself inside the broker container, so
# that a container which somehow also lacks the CLI reports the real problem instead of
# delegating into itself forever.
BLNK_KAFKA_PROVISION_IN_CONTAINER="${BLNK_KAFKA_PROVISION_IN_CONTAINER:-}"

# ---------------------------------------------------------------------------------------
# THE PROVISIONING INTERFACE — ONE DECLARATION, READ BY EVERY INVOKER
#
# Every variable an invoker of this script may set, in one list. It is not documentation: it
# is the list this script forwards when it re-executes itself inside the broker container, it
# is what "--print-interface" emits, and it is therefore what stack.sh forwards and what the
# compose kafka-init blocks are asserted against.
#
# WHY IT IS DECLARED HERE AND NOWHERE ELSE. There are four invocation paths - this script run
# directly, this script delegating into the broker container, ./stack.sh running it as the host
# fallback, and the compose kafka-init one-shot - and each used to carry its OWN hand-written
# list. They had drifted in every direction at once: stack.sh omitted the CLI timeout pair, the
# producer secret file and the real skip flag; both compose blocks omitted those plus partition
# growth, the readiness budget, the subscriber skip and rotation flags, and the client
# configuration.
#
# A NAME MISSING FROM AN INVOKER'S LIST FAILS SILENTLY AND PLAUSIBLY. The delegated or
# one-shot run does not error on an absent variable; it falls back to a DEFAULT and provisions
# something slightly different from what was asked for, then reports success. A missing
# KAFKA_PRODUCER_USER creates the principal under the default name. A missing
# KAFKA_PRODUCER_SECRET preserves an existing credential instead of applying the supplied one.
# A missing KAFKA_ALLOW_PARTITION_GROWTH refuses a growth the operator consented to. Which
# path ran - and therefore which settings survived - depended on whether the host happened to
# have the Kafka CLI, so the difference was invisible and unreproducible.
#
# So the list moved here and the invokers now READ it: stack.sh calls "--print-interface", and
# TestKafkaProvisionScript_HandsOffEverySupportedSetting compares both compose blocks against
# the same output. Adding a variable to the configuration block above means adding it here,
# once, and every invoker picks it up.
#
# EXCLUSIONS ARE DELIBERATE AND EACH HAS A REASON:
#
#   BLNK_KAFKA_PROVISION_IN_CONTAINER   this script's own already-delegated marker. It is set
#                                       by the delegation itself; an invoker setting it would
#                                       tell a host-side run it must not delegate.
#   KAFKA_BROKERS                       resolved into KAFKA_BOOTSTRAP_SERVER before anything
#                                       is provisioned, and it is the RESOLVED value that must
#                                       cross a boundary. Forwarding the raw one would let a
#                                       stale value win over the decision already made.
#   KAFKA_CONTAINER                     delegation TARGETS. They name where to delegate TO, so
#   KAFKA_COMPOSE_SERVICE               they are meaningful to a host-side invoker and
#   KAFKA_PROVISION_CONTAINER_SCRIPT    meaningless inside the container - see
#   KAFKA_CLIENT_CONFIG_CONTAINER_PATH  KAFKA_PROVISION_HOST_ONLY_INTERFACE below.
# ---------------------------------------------------------------------------------------
readonly KAFKA_PROVISION_INTERFACE=(
    KAFKA_BOOTSTRAP_SERVER
    KAFKA_TOPIC_PREFIX
    KAFKA_MIN_PARTITIONS
    KAFKA_REPLICATION_FACTOR
    # Consent to a key-remapping partition growth. It has to cross every boundary or two
    # invocation contexts answer differently for one .env: run inside the broker container it
    # would grow a non-empty topic on request, and delegated from the host it would refuse.
    # Dropping it fails safe rather than open, which is precisely why its absence went
    # unnoticed.
    KAFKA_ALLOW_PARTITION_GROWTH
    # Consent to proceed on an indeterminate credential probe, and it crosses the boundary for
    # exactly the reason above: the describe permission that makes the probe fail belongs to
    # the broker and the client configuration, not to the invocation context, so a host-side
    # run and a delegated run must reach the same verdict from one .env.
    KAFKA_ALLOW_SCRAM_PROBE_FAILURE
    KAFKA_SECURITY_PROTOCOL
    KAFKA_SCRAM_ITERATIONS
    KAFKA_SASL_ADMIN_USER
    KAFKA_SASL_ADMIN_SECRET
    KAFKA_SASL_USER
    KAFKA_SASL_SECRET
    # A path, and one an invoker's filesystem may not share. Forwarded anyway, deliberately:
    # stage_generated_secret then fails naming that exact directory, which tells the operator
    # to mount it, whereas dropping the variable fails with the generic "no delivery channel
    # is configured" and sends them looking for the wrong thing. That failure also happens
    # BEFORE the broker is altered, so a mis-mounted path costs a re-run and nothing else.
    KAFKA_SASL_SECRET_FILE
    KAFKA_PRODUCER_USER
    KAFKA_PRODUCER_SECRET
    KAFKA_PRODUCER_SECRET_FILE
    KAFKA_ROTATE_PRODUCER_SECRET
    KAFKA_SKIP_PRODUCER
    KAFKA_SAMPLE_SUBSCRIBER_USER
    KAFKA_SAMPLE_SUBSCRIBER_SECRET
    KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE
    KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX
    KAFKA_SAMPLE_SUBSCRIBER_TOPICS
    KAFKA_SKIP_SAMPLE_SUBSCRIBER
    KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET
    KAFKA_PROVISION_TIMEOUT_SECONDS
    KAFKA_PROVISION_POLL_INTERVAL_SECONDS
    KAFKA_CLI_TIMEOUT_SECONDS
    KAFKA_CLI_KILL_GRACE_SECONDS
    KAFKA_CLIENT_CONFIG
)

# The names an invoker running this script ON A HOST may additionally set, and which this
# script must NOT forward into the container.
#
# They are the delegation targets: where to delegate to, and where this script will be found
# once there. Inside the container they are either meaningless or actively wrong -
# KAFKA_CLIENT_CONFIG_CONTAINER_PATH is consumed by delegate_client_config on the way in and
# becomes KAFKA_CLIENT_CONFIG on the other side.
#
# Kept as a SEPARATE list rather than folded into the one above so the asymmetry is stated
# instead of being a discrepancy a reader has to explain. "--print-interface --host" emits
# both, which is what stack.sh forwards.
readonly KAFKA_PROVISION_HOST_ONLY_INTERFACE=(
    KAFKA_CLIENT_CONFIG_CONTAINER_PATH
    KAFKA_CONTAINER
    KAFKA_COMPOSE_SERVICE
    KAFKA_PROVISION_CONTAINER_SCRIPT
)

# Variables that USED to control something and no longer do.
#
# A retired name is not harmless. An operator who set it was asking for a behaviour, and after
# the rename the value is read by nothing: the request is silently dropped and the run does the
# opposite of what was asked. require_no_retired_variables refuses instead, naming the
# replacement, so the migration is a one-line fix rather than a mystery.
readonly KAFKA_PROVISION_RETIRED_VARIABLES=(
    "KAFKA_SKIP_PRODUCER_PRINCIPAL=KAFKA_SKIP_PRODUCER"
)

# Fixed rather than configurable. Blnk standardises on SCRAM-SHA-512: event_admin.go
# provisions subscriber credentials with kafka.ScramMechanismSha512 and
# scripts/kafka-bootstrap.sh seeds the administrative principal with the same mechanism, so
# the broker needs exactly one enabled.
readonly SCRAM_MECHANISM="SCRAM-SHA-512"

# The three answers a credential probe can give, as EXIT CODES rather than as words on stdout.
#
# The distinction between the second and the third is the whole point: "the broker says this
# principal has no SHA-512 credential" licenses minting one, and "the broker did not answer"
# licenses nothing at all. Collapsing them into a single non-zero return is what allowed a
# transient describe failure to be read as absence and a live secret to be replaced.
#
# They are exit codes because the probe is used as a CONDITION, and a previous version that
# printed its answer to stdout returned 0 unconditionally — so every caller read "exists"
# whatever the broker said, and the word appeared in the operator's console mid-sentence.
readonly SCRAM_PROBE_EXISTS=0
readonly SCRAM_PROBE_ABSENT=1
readonly SCRAM_PROBE_UNKNOWN=2
readonly MIN_SCRAM_ITERATIONS=4096

# The required partition floor, quoted in diagnostics so the message explains the rule
# rather than just restating the configured value.
readonly REQUIRED_MIN_PARTITIONS=6

# ---------------------------------------------------------------------------------------
# THE SAFE CREDENTIAL ALPHABET
#
# Two allow-lists, shared character-for-character with scripts/kafka-bootstrap.sh. Every
# credential this script handles - the administrative pair and the sample subscriber's -
# is checked against one of them before it is placed into a Kafka command or a properties
# file, and a value outside the alphabet is REFUSED rather than escaped.
#
# WHY AN ALLOW-LIST RATHER THAN ESCAPING
#
# This script writes credentials into TWO grammars, and neither can be escaped safely.
#
#   1. Kafka's config value grammar, used by kafka-configs --add-config:
#          SCRAM-SHA-512=[iterations=<n>,password=<secret>]
#      Kafka parses it by splitting on ',' and '=' inside '[' ... ']'. THERE IS NO ESCAPE
#      SEQUENCE - no backslash form, no quoting form, no length prefix. A password
#      containing a comma simply cannot be expressed, and is silently truncated into a
#      credential nobody can authenticate with.
#
#   2. The JAAS login-module value inside a Java properties file:
#          sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
#              required username="<user>" password="<secret>";
#      Here '"' ends the value, '\' is an escape in BOTH the JAAS syntax and the Java
#      Properties file format, ';' terminates the entry, and a space or a newline changes
#      how Properties reads the line. Escaping correctly would mean implementing two
#      nested escape schemes in bash and getting both right, forever.
#
# Shell quoting does not help with either. Quoting delivers the bytes intact; it is Kafka
# and the JAAS parser that then misread them. An allow-list turns that whole class of
# silent corruption into an immediate, named refusal.
#
# THE PRINCIPAL LIST IS STRICTER, AND FOR AN ADDITIONAL REASON
#
# A principal name reaches ACL bindings as "User:<name>", where '*' is Kafka's wildcard. A
# principal of '*' would not be a corrupted grant but a grant to EVERY principal - which is
# exactly the isolation failure the sample subscriber's ACLs exist to prevent. So the
# principal alphabet excludes it along with every grammar character, leaving the
# conventional identifier set that real Kafka principals use.
#
# RELATIONSHIP TO THE GO SIDE
#
# event_admin.go's validateSCRAMPassword requires printable ASCII (0x21-0x7E). This
# alphabet is a strict SUBSET, and deliberately so rather than inconsistently: the Go path
# sends the password over the Kafka protocol as a length-prefixed field where no grammar
# applies, while this script must put it through two CLI grammars that cannot express those
# bytes at all. A credential accepted here is always accepted there.
#
# The excluded printable characters, and what each one breaks:
#   , = [ ]   Kafka's --add-config / --add-scram value grammar
#   " \       the JAAS value, and Java Properties escaping
#   ;         terminates a JAAS login-module entry
#   ' ` $     shell metacharacters; excluded as defence in depth, not because quoting here
#             is wrong, but so that no credential depends on it being right
#   space     Java Properties line handling, and word splitting in any consumer
# Control characters and non-ASCII are excluded by construction.
# ---------------------------------------------------------------------------------------
readonly CREDENTIAL_SAFE_ERE='^[A-Za-z0-9!#%&()*+./:<>?@_{|}~^-]+$'
readonly PRINCIPAL_SAFE_ERE='^[A-Za-z0-9._@+-]+$'
readonly CREDENTIAL_SAFE_DESCRIPTION="letters, digits and ! # % & ( ) * + - . / : < > ? @ ^ _ { | } ~"
readonly PRINCIPAL_SAFE_DESCRIPTION="letters, digits and . _ @ + -"

# Kafka requires this prefix on a principal name in an ACL binding, and "*" is its
# any-host pattern. Both match event_admin.go's kafkaPrincipalPrefix and ACLHostAny.
readonly ACL_PRINCIPAL_PREFIX="User:"
readonly ACL_HOST_ANY="*"

# The KRaft authorizer that has to be active for an ACL to mean anything. Named here only
# so the readiness-failure diagnosis can point at it.
readonly REQUIRED_AUTHORIZER="org.apache.kafka.metadata.authorizer.StandardAuthorizer"

# THE topic catalogue. Only the categories are written down; the dead-letter names are
# derived. This order is the canonical one - it is eventCategoryOrder in model/event.go and
# it fixes the order of event_topics.go's AllTopics, AllDeadLetterTopics and
# AllTopicsWithDeadLetters, which provisioning is compared against.
#
# FIVE CATEGORIES AND TEN TOPICS is the published catalogue. 'ledgers' exists so that
# ledger.created - ordinary tenant data - has a subscriber-grantable home of its own instead of
# sharing the internal category with system.error: while the two shared a topic, granting one
# meant disclosing the other and withholding it left ledger.created with no subscriber route at
# all after the webhook transport retires. model.EventCategoryLedgers records that reasoning.
#
# 'system' is INTERNAL and is provisioned anyway, which is not a contradiction. It is not in the
# default grant set - a subscriber may hold it only where the deployment has declared
# KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS - but Blnk publishes system.error to it, and it is also
# where an event type the mapping table does not recognise is routed, precisely so the "every
# event is published, with zero exceptions" guarantee survives a producer that forgot to extend
# the table. A topic that
# does not exist cannot receive one: against a broker with auto.create.topics.enable=false
# the publish fails, the row retries until its budget is spent, and the dead-letter write
# then fails too because <prefix>.system.dlt is missing as well - so the very events the
# internal category exists to keep durable are the ones that get stranded.
# It is asserted against model.AllEventCategories by TestKafkaProvisionScript_ProvisionsEveryCategoryTheCodeOwns.
readonly EVENT_CATEGORIES=(transactions balances identities ledgers system)

# There is NO list of ungrantable categories here, and the absence is deliberate.
#
# One stood in this position, mirroring an internalEventCategories set in model/event.go, and
# both are gone. The distinction that matters is expressed positively instead, by
# SUBSCRIBER_GRANTABLE_CATEGORIES below: it is the default grant set, 'system' is outside it, and
# a deployment that has declared KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS may still grant that
# topic per subscriber through authorized_topics. What such a grant discloses is documented at
# model.EventCategorySystem.

# The categories a SUBSCRIBER may be granted, in that same canonical order: this is
# model.SubscriberGrantableEventCategories written in shell, and
# TestKafkaProvisionScript_GrantsOnlyTheCategoriesTheCodeAllows asserts the two lists are
# identical. A grant list that drifts from the Go one is a grant this script applies and every
# Go layer refuses, or worse, the reverse.
#
# PROVISIONED IS NOT THE SAME AS GRANTABLE, and conflating the two is what this list exists to
# prevent. Every category in EVENT_CATEGORIES is created; only these may appear in an ACL
# binding. The default grant used to be every category topic, which handed the sample principal
# Blnk's operational internals — and was self-defeating as well as wrong, because the
# subscriber-isolation acceptance criterion is proved by showing a principal CANNOT read outside
# its grant, and a principal granted every category has no outside.
#
# What a grant of each category carries, so the per-subscriber decision is made with the facts:
#
#   ledgers  carries ledger.created: a ledger's name, id, creation instant and the caller's own
#            metadata. Ordinary tenant data, granted like any other tenant category.
#   system   carries system.error, whose payload is the FROZEN legacy body, so it carries
#            Blnk's own error text verbatim - a driver message naming schema, table and
#            column, a broker error naming internal addresses. It is also the catalogue's
#            CATCH-ALL, where an event type no mapping recognises is routed. It is NOT in this
#            default list: a subscriber holds it only where the deployment has declared
#            KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS and its authorized_topics names it.
#   *.dlt    holds events that already failed, together with failure metadata naming broker
#            addresses and internal error reasons. Granting one would hand a subscriber every
#            OTHER subscriber's failed events. Read through GET /events/dead-letter under the
#            master key. The dead-letter names are excluded by not being category names at
#            all, so no entry here can ever produce one.
#
# THE FOUR TENANT CATEGORIES ARE GRANTABLE BY DEFAULT. 'system' IS NOT, and that exclusion is a
# deliberate authorization decision rather than an oversight.
#
# <prefix>.system carries system.error, whose payload is frozen by requirement R-8 and therefore
# renders Blnk's error text verbatim — a PostgreSQL error names schema, table, column and
# routine; a broker error names internal addresses — and it is the catalogue's CATCH-ALL, so a
# grant of it would also stand over every event type nobody has catalogued yet. It is an
# OPERATOR topic, in the same class as every .dlt sibling, and this script grants no principal
# an ACL over it: the sample subscriber a local walkthrough uses must not be the thing that
# teaches that grant as a default.
#
# It is reachable, and only deliberately: a deployment that sets
# KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS may name the topic in a subscriber's authorized_topics
# through POST/PUT /subscribers, and Blnk's own provisioning path then binds the ACL and logs
# the grant. That is a decision recorded at deployment level rather than an operator rule one
# request can walk around. See model.SubscriberPrivilegedEventCategories and
# docs/event-streaming.md.
#
# Kept as a separate list rather than derived by filtering EVENT_CATEGORIES because the
# distinction is a POLICY, not a naming rule, and because provisioning every category while
# granting a subset must remain expressible: conflating the two is how a topic that events are
# routed to goes uncreated. model.SubscriberGrantableEventCategories states the same policy on
# the Go side, and TestKafkaProvisionScript_ProvisionsEveryCategoryTheCodeOwns asserts the two
# agree name for name and in the same order.
readonly SUBSCRIBER_GRANTABLE_CATEGORIES=(transactions balances identities ledgers)

# What the SAMPLE principal is granted when KAFKA_SAMPLE_SUBSCRIBER_TOPICS is unset.
#
# IT WAS REFERENCED AND NEVER DEFINED. Two loops expanded it to build the sample's default
# topic list, and under `set -u` bash expands an undefined ARRAY to nothing rather than
# failing, so the list came out EMPTY and the empty case is not the one that is validated:
# resolve_subscriber_topics only refuses an empty result on the OVERRIDE path. The script
# therefore reported a provisioned sample subscriber that could read nothing, and the first
# symptom was a demo consumer receiving no records with every ACL apparently in place.
#
# IT IS A SUBSET OF SUBSCRIBER_GRANTABLE_CATEGORIES, and the two stay separate for the reason
# stated above: what a subscriber MAY be granted is a policy, what the sample IS granted is a
# default. It currently equals the allowlist, because every category in it is tenant data and a
# local walkthrough consumer has nobody on this stack to be isolated from; the one category that
# makes the two differ — `system` — is outside the default allowlist, so the narrowing that
# matters lives in the allowlist itself.
#
# `system` must never appear here. The allowlist check would refuse it anyway, and stating it
# again costs one assertion and catches the edit that adds it to both lists at once. A
# deployment that really wants a subscriber reading the internal topic declares
# KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS and grants it through the subscriber API, where the
# decision is recorded against a named subscriber rather than baked into a sample.
#
# Ordered as a subset of the allowlist so the two lists read against each other.
readonly SAMPLE_SUBSCRIBER_DEFAULT_CATEGORIES=(transactions balances identities ledgers)

# The suffix that forms a dead-letter sibling. This is the published <topic>.dlt naming
# convention and it must equal event_topics.go's DeadLetterTopicSuffix. Blnk owns the .dlt
# sibling of every topic it owns; subscribers building their own consumer-side
# dead-lettering must choose names outside that space.
readonly DEAD_LETTER_SUFFIX=".dlt"

# The character that TERMINATES a consumer-group namespace, equal to
# model.SubscriberGroupTerminator.
#
# It exists because a PREFIXED ACL reaches further than its value reads: a grant on
# "blnk-sample-subscriber" also matches "blnk-sample-subscriber-evil". Terminating the
# namespace confines the grant to that principal's own group tree, which is the difference
# between a namespace and a prefix that happens to look like one.
readonly SUBSCRIBER_GROUP_TERMINATOR="."

# Kafka's own ceiling on a topic name, and the prefix budget derived from it.
#
# MAX_TOPIC_NAME_LENGTH is the broker's limit; it refuses CreateTopics for anything longer.
# MAX_TOPIC_PREFIX_LENGTH reserves room for the longest suffix this script appends,
# ".transactions" plus the dead-letter sibling, so that a prefix which passes validation can
# compose a legal name for EVERY category rather than only the shortest one.
#
# Both must equal config.MaxKafkaTopicNameLength and config.MaxKafkaTopicPrefixLength. Blnk
# refuses the same values at configuration load, and this script is the side an operator runs
# first, so a disagreement here surfaces as "the topics exist but Blnk will not start".
readonly MAX_TOPIC_NAME_LENGTH=249
readonly MAX_TOPIC_PREFIX_LENGTH=232

# The strength floor every secret in this script is held to (S6-07).
#
# Before this existed the only test a secret faced was the ALPHABET one, which a
# one-character password passes: "a" is drawn entirely from the allowed set. So the script
# would happily seed the broker's administrative principal, or a subscriber's, with a
# credential brute-forced in one guess — and then report success.
#
# Two numbers rather than one, because length alone is not strength. "aaaaaaaaaaaaaaaa..."
# is 32 characters and one character of entropy, and a padded or repeated value is exactly
# what a hurried operator produces. Requiring 16 DISTINCT characters rejects that class
# without rejecting anything a generator produces: 32 characters drawn uniformly from the
# 62-character alphanumeric set contain 16 or more distinct characters with overwhelming
# probability.
#
# 32 is not arbitrary either. It is the length of the value this script GENERATES, and it is
# what .env.example and the failure messages here recommend, so the floor an operator is held
# to is the same one the tooling meets. A SCRAM-SHA-512 credential derived from fewer than
# 32 printable characters is the weakest link in a chain whose other end is PBKDF2 at 4096
# iterations.
readonly MIN_SECRET_LENGTH=32
readonly MIN_SECRET_DISTINCT=16

# Resolved during main; declared here so the data flow between the steps is visible.
TOPICS_CLI=""
CONFIGS_CLI=""
ACLS_CLI=""
# OPTIONAL, unlike the three above: it is needed only to decide whether an
# under-partitioned topic is empty, and an environment without it still provisions
# correctly - it just cannot prove emptiness, so it declines to grow rather than guessing.
# See topic_record_state.
OFFSETS_CLI=""
CLI_FLAVOUR=""

# The absolute path to 'timeout', or empty when the host has none. Resolved once by
# detect_cli_timeout so the absence is announced a single time rather than per call.
CLI_TIMEOUT=""
CLIENT_CONFIG=""
GENERATED_CLIENT_CONFIG=""
# Every mode-0600 scratch file this run creates, so the EXIT trap removes each one. A list
# rather than a single path because the SCRAM delivery below writes one per credential
# (Q4-20), and a file holding a password must not outlive the command that consumed it.
GENERATED_SECRET_FILES=()
# Where the generated sample credential was recorded, if it was. Printed as a PATH; the
# credential itself is never printed (Q4-20).
SUBSCRIBER_SECRET_ARTIFACT=""
CATEGORY_TOPICS=()
# The topics a subscriber MAY be granted: every TENANT category topic, and never the internal
# system topic or a dead-letter sibling. It is the shell's copy of
# model.SubscriberGrantableTopics and it is the allowlist any override is checked against.
GRANTABLE_TOPICS=()
# The category topics that are created and published to but never granted to a subscriber.
# DERIVED as CATEGORY_TOPICS minus GRANTABLE_TOPICS, so it cannot disagree with the allowlist,
# and reported in the summary so "created" and "grantable" are visibly different questions.
INTERNAL_TOPICS=()
# What the sample principal is granted when no override is supplied: a least-privilege subset
# of the allowlist. See SAMPLE_SUBSCRIBER_DEFAULT_CATEGORIES.
SAMPLE_DEFAULT_TOPICS=()
DEAD_LETTER_TOPICS=()
ALL_TOPICS=()
SUMMARY_TOPICS=()
# Topics left under-partitioned because they hold records and growth was not permitted.
# Reported in the summary so a deliberate refusal is visible rather than only warned about
# in passing, since a throughput limit nobody notices is one nobody fixes.
SUMMARY_GROWTH_REFUSED=()
# What this run actually DID to the catalogue, as opposed to what the catalogue now looks
# like. Both are needed and they answer different questions: the geometry table says the
# layout is correct, and these say whether it was correct when the run started.
#
# Without them a run that silently rebuilt a lost catalogue was indistinguishable from a run
# that found everything in place - both printed ten topics with the right partition counts
# and nothing else. That distinction is the whole value of the output as an audit trail, and
# it is what the daily outbox-versus-offset reconciliation in docs/kafka-operations.md needs:
# a topic recreated between two runs has offsets that start again from zero.
#
# The vocabulary is event_admin.go's, deliberately - created / grown / unchanged - so the
# shell provisioner and the Go assurance path can be read against each other.
#
# EVERY TOPIC LANDS IN EXACTLY ONE OF THESE FOUR, and each count is the length of its own array
# rather than a subtraction. The counts used to be three arrays and an arithmetic remainder, which
# could go NEGATIVE: created and grown were appended independently, so a topic this run created and
# then grew was counted in both and subtracted twice.
#
# SUMMARY_RACED is the class that was missing. Provisioning is deliberately concurrent-safe - the
# compose one-shot and a manual `make kafka_provision` can run together - so "another provisioner
# did it" is an ordinary outcome, and so is "the broker could not be asked". Neither is a creation,
# a growth or a no-change, and reporting either as one of those makes the summary say something
# this run does not know. An operator reading `created: 8` treats it as a rebuilt catalogue with
# restarted offsets; that reading has to be earned.
SUMMARY_CREATED=()
SUMMARY_GROWN=()
SUMMARY_UNCHANGED=()
SUMMARY_RACED=()
SUMMARY_PARTITIONS=()
# The inputs to reconcile_principal_acls, rebuilt by each caller immediately before it runs.
#
# ACL_DESIRED holds the canonical descriptors the configuration asks for; ACL_OWNED_SHAPES
# holds the (resource type, pattern type, operation) triples this script provisions for that
# principal, which is what lets a stale grant of ours be told apart from somebody else's
# binding. Globals rather than parameters because bash cannot pass an array by value, and this
# script already passes its lists this way - SUBSCRIBER_TOPICS and ALL_TOPICS are read the
# same way by the functions that grant on them.
ACL_DESIRED=()
ACL_OWNED_SHAPES=()
SUBSCRIBER_TOPICS=()
SUBSCRIBER_USER=""
SUBSCRIBER_GROUP_PREFIX=""
SUBSCRIBER_PROVISIONED="no"
PRODUCER_USER=""
PRODUCER_PROVISIONED="no"
PRODUCER_SECRET_DISPOSITION=""
# generate_password's return channel. A variable rather than stdout so that no line of this
# script writes a credential to output - see generate_password for why that matters.
# The path stage_generated_secret staged a credential at, for commit_staged_secret to
# consume. A GLOBAL rather than a printed value, and that is load-bearing rather than
# stylistic: a printed path has to be read with "$(...)", which runs the function in a
# SUBSHELL, and a subshell cannot append to GENERATED_SECRET_FILES in the parent. The
# staged file would then never be registered for cleanup, so an abort between staging and
# the commit would leave a credential on disk - the exact guarantee staging is supposed to
# provide. generate_password returns GENERATED_PASSWORD the same way, for the same reason.
STAGED_SECRET_PATH=""
GENERATED_PASSWORD=""
CONTAINER_RUNTIME=()
CONTAINER_STDIN_FLAG=()
CONTAINER_TARGET=""
CONTAINER_RUNTIME_LABEL=""

# The partition count actually applied, after the six-partition floor has been imposed on
# KAFKA_MIN_PARTITIONS. Kept separate from the configured value so that every message and
# every request uses the number in force rather than the number requested.
TARGET_PARTITIONS=""

# Whether the administrative credential pair is in use. "yes" when both values are set,
# "no" for a broker with no SASL listener, and "config" when KAFKA_CLIENT_CONFIG supplies
# the authentication instead. Diagnostics read this rather than guessing a principal name.
ADMIN_SASL_MODE=""

# The OBSERVED replication factor per topic, parallel to SUMMARY_TOPICS. The summary prints
# this rather than KAFKA_REPLICATION_FACTOR, because printing the requested factor would
# report a durability that was never confirmed.
SUMMARY_REPLICATION=()

# What happened to each principal's password. SIX values, and the summary has a case for
# every one of them:
#
#   supplied      - the operator gave an explicit value, so no probe was needed
#   rotated       - a replacement was generated because a rotation was requested
#   generated     - none existed, so one was generated and delivered to its file
#   preserved     - one already exists and was deliberately left alone
#   indeterminate - the credential's state could NOT be established, so nothing was written
#   skipped       - none exists and there is nowhere to deliver a generated one
#
# "indeterminate" is distinct from "skipped" on purpose. Both leave the credential untouched,
# but they need different remedies and reporting them alike sends an operator looking in the
# wrong place: skipped means "configure a destination", indeterminate means "the probe itself
# failed — fix broker reachability or the admin principal's authorisation".
#
# Read by the closing summary and by the once-only credential print, which exists for the
# producer too: a generated password nobody can read is a credential nobody can put into .env.
SUBSCRIBER_SECRET_DISPOSITION=""
PRODUCER_SECRET_DISPOSITION=""

# The resolved producer principal, and what happened to it: "yes" when provisioned,
# "skipped" when KAFKA_SKIP_PRODUCER is set, and "no-secret" when no secret was
# supplied to provision it with. The last of those is reported prominently rather than
# quietly, because a deployment whose application is configured with KAFKA_SASL_ADMIN_USER
# now REFUSES to start without this principal.
PRODUCER_USER=""
PRODUCER_PROVISIONED="no"

# ---------------------------------------------------------------------------------------
# Diagnostics
#
# In every one of these, the first argument is the headline and any further arguments are
# printed as indented continuation lines. Keeping continuation lines as separate arguments
# is what lets the failure messages below be genuinely actionable without embedding
# newlines and stray indentation in string literals.
#
# Note what is absent: there is no shell tracing anywhere in this script, and no helper that
# can print KAFKA_SASL_ADMIN_SECRET. "set -x" would echo the assembled SCRAM configuration
# and the client-properties contents into the container log, which is why it appears
# nowhere - not even commented out.
# ---------------------------------------------------------------------------------------

_continuation() {
    local line
    for line in "$@"; do
        printf '%s\n' "    ${line}"
    done
}

# Progress, on stdout.
log() {
    printf '%s\n' "${BLU}==>${NC} ${1}"
    shift || true
    _continuation "$@"
}

# Success, on stdout.
ok() {
    printf '%s\n' "${GRN}==>${NC} ${1}"
    shift || true
    _continuation "$@"
}

# Advisory, on stderr. Never fatal.
warn() {
    printf '%s\n' "${YEL}==> warning:${NC} ${1}" >&2
    shift || true
    _continuation "$@" >&2
}

# Fatal, on stderr.
die() {
    printf '%s\n' "${RED}==> error:${NC} ${1}" >&2
    shift || true
    _continuation "$@" >&2
    exit 1
}

# ---------------------------------------------------------------------------------------
# Small pure-bash utilities
# ---------------------------------------------------------------------------------------

# Strip leading and trailing whitespace. Used for entries in delimited lists, where
# surrounding spaces are legal and routinely present in a hand-edited .env.
trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

# Split a delimited list into one trimmed, non-empty entry per line. Written with parameter
# expansion rather than by reassigning IFS so that no global state is mutated and no
# word-splitting surprises are possible.
split_list() {
    local rest="$1" delimiter="$2" entry
    while [[ -n "$rest" ]]; do
        entry="${rest%%"${delimiter}"*}"
        if [[ "$rest" == *"${delimiter}"* ]]; then
            rest="${rest#*"${delimiter}"}"
        else
            rest=""
        fi
        entry="$(trim "$entry")"
        if [[ -n "$entry" ]]; then
            printf '%s\n' "$entry"
        fi
    done
}

# Join the remaining arguments with ", " for a one-line diagnostic.
join_commas() {
    local result="" entry
    for entry in "$@"; do
        if [[ -z "$result" ]]; then
            result="$entry"
        else
            result="${result}, ${entry}"
        fi
    done
    printf '%s' "$result"
}

# Interpret a flag-shaped variable. Empty and the conventional negatives are false;
# anything else is true, so that "1", "true", "yes", "on" and a bare "please" all enable
# the flag rather than silently doing nothing because the operator guessed a spelling this
# script did not anticipate.
is_truthy() {
    local value
    value="$(trim "${1:-}")"
    case "${value,,}" in
        "" | 0 | false | no | n | off) return 1 ;;
        *) return 0 ;;
    esac
}

# Remove anything that could carry a credential from output that is about to be shown.
#
# Used only where a Kafka CLI failure is echoed to help diagnosis. The CLI does not print
# the JAAS password in practice, but "in practice" is not a guarantee worth betting a
# credential on, and a client-configuration parse error is precisely the class of failure
# that quotes the offending line back.
redact() {
    grep -v -i -E 'password|jaas|sasl\.jaas\.config' || true
}

# ---------------------------------------------------------------------------------------
# Preconditions
#
# Everything here runs before the broker is contacted, and each failure names the variable
# or the fix the operator needs rather than leaving them to infer it.
# ---------------------------------------------------------------------------------------

# "No Kafka configured" is a legitimate steady state, not an error.
#
# With no brokers the event publisher resolves to its no-op implementation and Blnk starts,
# serves and processes transactions exactly as it did before Kafka existed - the same
# graceful degradation the legacy webhook path had when its URL was unset. .env.example
# ships KAFKA_BROKERS empty for exactly that reason, so this is the common local case
# rather than an exotic one, and it must exit 0 with an explanation instead of failing.
#
# An explicit KAFKA_BOOTSTRAP_SERVER overrides the skip. That is what lets the compose
# kafka-init service provision a broker while .env still carries an empty KAFKA_BROKERS for
# the application's benefit.
skip_when_kafka_unconfigured() {
    if [[ -n "${KAFKA_BOOTSTRAP_SERVER_EXPLICIT:-}" ]]; then
        return 0
    fi

    if [[ -n "$KAFKA_BROKERS_DECLARED" && -z "$KAFKA_BROKERS" ]]; then
        log "KAFKA_BROKERS is declared but empty, so Kafka is not configured here." \
            "Nothing to provision, and this is not an error: with no brokers the event" \
            "publisher resolves to its no-op implementation and Blnk runs exactly as it" \
            "did before Kafka existed." \
            "To provision anyway, set KAFKA_BROKERS to the broker list, or set" \
            "KAFKA_BOOTSTRAP_SERVER to override this check for one run."
        exit 0
    fi
}

# A whole number of at least one. Kafka rejects zero and negative values for both the
# partition count and the replication factor, but with an error that names neither the
# variable nor the file it came from.
require_positive_int() {
    local name="$1" value="$2"
    # The regex is checked first and short-circuits, so the arithmetic below never sees a
    # non-numeric value. 10# forces base 10 so that a padded "06" is not read as octal.
    if [[ ! "$value" =~ ^[0-9]+$ ]] || ((10#$value < 1)); then
        die "${name} must be a positive integer, but is '${value}'." \
            "Fix: unset it to accept the default, or set a whole number of at least 1."
    fi
}

require_valid_geometry() {
    require_positive_int "KAFKA_MIN_PARTITIONS" "$KAFKA_MIN_PARTITIONS"
    require_positive_int "KAFKA_REPLICATION_FACTOR" "$KAFKA_REPLICATION_FACTOR"
    require_positive_int "KAFKA_PROVISION_TIMEOUT_SECONDS" "$KAFKA_PROVISION_TIMEOUT_SECONDS"
    require_positive_int "KAFKA_PROVISION_POLL_INTERVAL_SECONDS" "$KAFKA_PROVISION_POLL_INTERVAL_SECONDS"

    TARGET_PARTITIONS="$((10#$KAFKA_MIN_PARTITIONS))"

    # A value below the floor is RAISED to it, not merely complained about and then applied.
    #
    # It used to be warned about and used, which broke the requirement two ways at once: the
    # topics really did get fewer partitions than the design calls for, and the script
    # disagreed with event_admin.go's resolveTopicPartitions, which raises the same input to
    # the same floor. An operator who provisioned here and then let the service assure
    # topics on start-up would see the service grow every topic on its first run, undoing by
    # surprise what this script had just done deliberately.
    #
    # Raising rather than refusing is the behaviour chosen because it matches the Go path
    # exactly, and because the two must never diverge again.
    if ((TARGET_PARTITIONS < REQUIRED_MIN_PARTITIONS)); then
        warn "KAFKA_MIN_PARTITIONS is ${KAFKA_MIN_PARTITIONS}, below the required minimum of ${REQUIRED_MIN_PARTITIONS}." \
            "It is being RAISED to ${REQUIRED_MIN_PARTITIONS}, which is what" \
            "event_admin.go's resolveTopicPartitions does with the same value - the two" \
            "must agree, or the service would grow every topic on its next start-up and" \
            "undo what this run did." \
            "Fewer partitions than the minimum would cap how far a subscriber's consumer" \
            "group can scale out. Ordering is unaffected either way: messages are keyed by" \
            "ledger ID, so events for one aggregate share a partition at any count." \
            "Fix: unset KAFKA_MIN_PARTITIONS to accept the default of ${REQUIRED_MIN_PARTITIONS}."
        TARGET_PARTITIONS="$REQUIRED_MIN_PARTITIONS"
    fi
}

# Refuse a credential that the Kafka config or JAAS grammars cannot carry.
#
# Takes the variable NAME and its value, and prints only the name - never the value, since
# the value is a secret in every call. See the safe-alphabet block above for why a refusal
# is the only correct response to a character neither grammar has an escape for.
require_safe_credential() {
    local name="$1" value="$2"
    if [[ ! "$value" =~ $CREDENTIAL_SAFE_ERE ]]; then
        die "${name} contains a character that Kafka's credential grammars cannot carry." \
            "Allowed: ${CREDENTIAL_SAFE_DESCRIPTION}" \
            "The value is not echoed. It is refused rather than escaped because it has to" \
            "pass through two grammars that cannot express it: kafka-configs parses" \
            "'${SCRAM_MECHANISM}=[iterations=...,password=...]' by splitting on ',' and '='" \
            "with no escape sequence at all, and the JAAS properties value ends at the" \
            "first unescaped '\"' and terminates at ';'. Either would silently produce a" \
            "credential that is not the one you supplied." \
            "Fix: use a secret drawn from the allowed set. For example:" \
            "  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c 32" \
            "Nothing has been provisioned."
    fi
}

# Count the distinct characters in a value, without ever printing any of them.
#
# fold -w1 puts one character per line, sort -u collapses duplicates, wc -l counts what is
# left. Every stage is a pipe, so no intermediate ever reaches a command line or a log.
# LC_ALL=C keeps "distinct" byte-wise rather than locale-dependent, which is what makes the
# count reproducible across the three environments this script runs in.
count_distinct_characters() {
    local value="$1"
    printf '%s' "$value" | LC_ALL=C fold -w1 | LC_ALL=C sort -u | wc -l | tr -d '[:space:]'
}

# Refuse a secret that is too short or too repetitive to be worth deriving a credential from
# (S6-07).
#
# Neither the value nor any part of it is printed - only its LENGTH and its DISTINCT COUNT,
# which is what the operator needs in order to fix it and is not enough to guess it. A length
# is not a secret; a prefix would be.
require_strong_credential() {
    local name="$1" value="$2" distinct

    if ((${#value} < MIN_SECRET_LENGTH)); then
        die "${name} is ${#value} characters, below the ${MIN_SECRET_LENGTH}-character minimum." \
            "The alphabet check this replaces let a ONE-character password through: a single" \
            "letter is drawn entirely from the allowed set, so the only test a secret used to" \
            "face said nothing at all about its strength." \
            "The value is not echoed, only its length." \
            "Fix: generate one from the allowed set. For example:" \
            "  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c ${MIN_SECRET_LENGTH}" \
            "Nothing has been provisioned."
    fi

    distinct="$(count_distinct_characters "$value")"
    if ((distinct < MIN_SECRET_DISTINCT)); then
        die "${name} uses only ${distinct} distinct characters, below the ${MIN_SECRET_DISTINCT} required." \
            "It is long enough but not varied enough, which is what a padded or repeated" \
            "value looks like - a 32-character run of one letter has 32 characters and one" \
            "character of entropy. A randomly generated secret of ${MIN_SECRET_LENGTH}" \
            "alphanumerics clears this comfortably; a hand-typed one usually does not." \
            "Neither the value nor any part of it is echoed." \
            "Fix: generate one from the allowed set. For example:" \
            "  openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c ${MIN_SECRET_LENGTH}" \
            "Nothing has been provisioned."
    fi
}

# Refuse a principal name that Kafka's grammar or its ACL wildcard cannot carry.
#
# The name IS printed here: a principal is an identifier rather than a credential, and the
# operator needs to see which value was rejected.
require_safe_principal() {
    local name="$1" value="$2"
    if [[ ! "$value" =~ $PRINCIPAL_SAFE_ERE ]]; then
        die "${name} is '${value}', which is not a usable Kafka principal name." \
            "Allowed: ${PRINCIPAL_SAFE_DESCRIPTION}" \
            "A principal name goes into the SCRAM credential grammar, which has no escape" \
            "sequence, into the JAAS username, and into ACL bindings as 'User:<name>' -" \
            "where '*' is the wildcard that matches EVERY principal, so a name of '*' would" \
            "not corrupt a grant but hand every topic to everybody." \
            "Fix: use a plain identifier." \
            "Nothing has been provisioned."
    fi
}

# Refuse an unsubstituted {PLACEHOLDER} left over from a template.
#
# A literal placeholder is worse than an empty value: it authenticates or provisions with a
# password nobody chose, and fails indistinguishably from a wrong one a long way from here.
require_no_placeholder() {
    local name="$1" value="$2"
    if [[ "$value" == "{"*"}" ]]; then
        die "${name} still holds an unsubstituted {PLACEHOLDER} value." \
            "It was copied from a template and never replaced with a real secret. Using it" \
            "would provision or authenticate with a literal placeholder as the password," \
            "which fails indistinguishably from a wrong password." \
            "Fix: replace it with a real value, or unset it. Nothing has been provisioned" \
            "and the value is not echoed."
    fi
}

# Resolve and validate the administrative credential PAIR, and decide which of the three
# authentication modes this run uses.
#
# Sets ADMIN_SASL_MODE to one of:
#
#   config   KAFKA_CLIENT_CONFIG supplies the authentication. The environment pair is not
#            needed and is not required - the operator's file carries whatever the broker
#            wants, which may not even be SCRAM. This is the F-4 fix: the secret used to be
#            demanded unconditionally, so a perfectly complete client-properties file could
#            not be used without also exporting a redundant secret that nothing then read.
#   yes      Both values set: SASL/SCRAM as the named principal.
#   no       Both values empty: no SASL. Only coherent with a non-SASL security protocol,
#            which is checked here, because asking for SASL_PLAINTEXT with no credential is
#            a contradiction rather than something to guess at.
#
# Exactly one value set is refused in either direction. The username is never defaulted:
# it used to fall back to the literal "admin", which silently disagreed with the Go clients
# reading the same empty value as "no SASL at all".
#
# This mirrors scripts/kafka-bootstrap.sh and config.KafkaConfig.SASLAdminCredentials
# deliberately: all three authenticate as the same principal, so they must agree about what
# a partly-filled pair means, or an operator debugging one learns the wrong rule about the
# others.
#
# No branch prints the secret. .env.example ships both keys EMPTY rather than carrying a
# {PLACEHOLDER}, deliberately: stack.sh --init substitutes {POSTGRES_PASSWORD} and nothing else
# with a global sed, so any other brace placeholder would survive into .env as a literal
# password. --init then GENERATES both of these keys - the principal and a random 32-character
# password - through set_env_value rather than through that sed, and does so for an .env that
# already exists as well as for one it creates, which is why they can be empty in the template
# and still correct in a generated .env.
#
# So the empty case reaching this function means .env was hand-written, or it predates the Kafka
# keys and --init has not been run since. The placeholder check remains as defence in depth for
# a hand-edited .env.
require_admin_credentials() {
    local user secret
    # Trimmed before every test, so a value carrying a trailing newline from a secret store
    # reads as absence rather than as a principal nobody created. This is what
    # config.KafkaConfig.SASLAdminCredentials does in Go.
    user="$(trim "$KAFKA_SASL_ADMIN_USER")"
    secret="$(trim "$KAFKA_SASL_ADMIN_SECRET")"

    if [[ -n "$KAFKA_CLIENT_CONFIG" ]]; then
        ADMIN_SASL_MODE="config"

        # A half-filled pair alongside a client-configuration file is harmless - nothing
        # reads it - but it is still a sign the operator expected it to be used, so it is
        # pointed out rather than passed over in silence.
        if [[ -n "$user" && -z "$secret" ]] || [[ -z "$user" && -n "$secret" ]]; then
            warn "the KAFKA_SASL_ADMIN_USER / KAFKA_SASL_ADMIN_SECRET pair is half-configured." \
                "It is not used at all on this run: KAFKA_CLIENT_CONFIG is set, so" \
                "'${KAFKA_CLIENT_CONFIG}' supplies the authentication verbatim." \
                "Set both keys or neither, so the file and the environment do not appear to" \
                "disagree."
        fi

        # Still validated when present, because the delegation path may propagate them.
        if [[ -n "$user" ]]; then
            require_safe_principal "KAFKA_SASL_ADMIN_USER" "$user"
        fi
        if [[ -n "$secret" ]]; then
            require_no_placeholder "KAFKA_SASL_ADMIN_SECRET" "$secret"
            require_safe_credential "KAFKA_SASL_ADMIN_SECRET" "$secret"
        fi

        KAFKA_SASL_ADMIN_USER="$user"
        KAFKA_SASL_ADMIN_SECRET="$secret"

        return 0
    fi

    if [[ -z "$user" && -z "$secret" ]]; then
        # No SASL. Coherent only against a broker with no SASL listener, so the protocol has
        # to say so; a SASL_* protocol with no credential could only ever fail the handshake.
        case "$KAFKA_SECURITY_PROTOCOL" in
            SASL_*)
                die "KAFKA_SECURITY_PROTOCOL is '${KAFKA_SECURITY_PROTOCOL}' but neither KAFKA_SASL_ADMIN_USER nor KAFKA_SASL_ADMIN_SECRET is set." \
                    "A SASL protocol needs a credential; there is nothing to authenticate" \
                    "with, and no principal is invented for you - the username used to fall" \
                    "back to 'admin', which silently disagreed with the Go clients reading" \
                    "the same empty value as 'no SASL at all'." \
                    "Fix, whichever is true:" \
                    "  1. you meant to authenticate: set BOTH keys, to the principal and" \
                    "     password scripts/kafka-bootstrap.sh seeded. .env.example ships" \
                    "     both empty and './stack.sh --init' generates BOTH into .env -" \
                    "     into an .env that already exists as well as one it creates." \
                    "  2. the broker has no SASL listener: set" \
                    "     KAFKA_SECURITY_PROTOCOL=PLAINTEXT (or SSL)." \
                    "  3. the broker needs settings this script does not model: point" \
                    "     KAFKA_CLIENT_CONFIG at a properties file you manage." \
                    "Nothing has been provisioned."
                ;;
        esac

        ADMIN_SASL_MODE="no"
        KAFKA_SASL_ADMIN_USER=""
        KAFKA_SASL_ADMIN_SECRET=""

        log "no administrative SASL credential is configured" \
            "KAFKA_SECURITY_PROTOCOL is ${KAFKA_SECURITY_PROTOCOL}, so the CLI will connect" \
            "without authenticating. This is the same reading the event publisher and the" \
            "Go admin client apply to an empty credential pair."

        return 0
    fi

    if [[ -z "$user" ]]; then
        die "KAFKA_SASL_ADMIN_SECRET is set but KAFKA_SASL_ADMIN_USER is empty." \
            "SASL/SCRAM needs both: without a principal the secret cannot be used, and the" \
            "connection would be anonymous while looking configured. The username is never" \
            "defaulted, so no principal is invented for you." \
            "Fix: set KAFKA_SASL_ADMIN_USER to the principal" \
            "scripts/kafka-bootstrap.sh seeded, or clear the secret too and set" \
            "KAFKA_SECURITY_PROTOCOL=PLAINTEXT for a broker with no SASL listener." \
            "The secret is not echoed and nothing has been provisioned."
    fi

    if [[ -z "$secret" ]]; then
        die "KAFKA_SASL_ADMIN_USER is set to '${user}' but KAFKA_SASL_ADMIN_SECRET is empty." \
            "The broker cannot be provisioned without authenticating, and there is no" \
            "default by design: a credential must not be guessable from source." \
            "Fix: run './stack.sh --init'. It generates this key into .env - creating the" \
            "file when it is absent and filling in only the keys an existing one is missing," \
            "so it is safe to run against an .env already in service. Otherwise set" \
            "KAFKA_SASL_ADMIN_SECRET by hand, or export it for this process." \
            "It must be the same password scripts/kafka-bootstrap.sh seeded the" \
            "'${user}' principal with." \
            "Nothing has been provisioned."
    fi

    require_no_placeholder "KAFKA_SASL_ADMIN_SECRET" "$secret"
    require_safe_principal "KAFKA_SASL_ADMIN_USER" "$user"
    require_safe_credential "KAFKA_SASL_ADMIN_SECRET" "$secret"
    # S6-07. The administrative principal is a super-user on this cluster: it can create any
    # topic, mint any subscriber's credential and rewrite any ACL. Holding it to a weaker
    # standard than the sample subscriber's would be exactly backwards.
    require_strong_credential "KAFKA_SASL_ADMIN_SECRET" "$secret"

    ADMIN_SASL_MODE="yes"

    # Publish the trimmed values so the JAAS line, the delegation pass-through and every
    # diagnostic work from exactly what was validated.
    KAFKA_SASL_ADMIN_USER="$user"
    KAFKA_SASL_ADMIN_SECRET="$secret"
}

# admin_identity_label names the principal for a diagnostic, without ever guessing one.
#
# Diagnostics used to interpolate KAFKA_SASL_ADMIN_USER unconditionally, which read as
# "authenticated as " with nothing after it once the "admin" default was removed, and was
# simply wrong when a client-configuration file supplied a different principal.
admin_identity_label() {
    case "$ADMIN_SASL_MODE" in
        yes) printf '%s' "$KAFKA_SASL_ADMIN_USER" ;;
        config) printf 'the principal in %s' "$CLIENT_CONFIG" ;;
        *) printf 'an unauthenticated client' ;;
    esac
}

# The iteration count must be a whole number at or above the SCRAM minimum; Kafka rejects
# anything lower outright, with an UNACCEPTABLE_CREDENTIAL error that reads like a bad
# password.
require_valid_iterations() {
    if [[ ! "$KAFKA_SCRAM_ITERATIONS" =~ ^[0-9]+$ ]]; then
        die "KAFKA_SCRAM_ITERATIONS must be a positive integer, but is '${KAFKA_SCRAM_ITERATIONS}'." \
            "Fix: unset it to accept the default of ${MIN_SCRAM_ITERATIONS}, or set a whole number."
    fi

    # 10# forces base 10 so that a padded value such as 08192 is not read as invalid octal.
    if ((10#$KAFKA_SCRAM_ITERATIONS < MIN_SCRAM_ITERATIONS)); then
        die "KAFKA_SCRAM_ITERATIONS is ${KAFKA_SCRAM_ITERATIONS}, below the SCRAM minimum of ${MIN_SCRAM_ITERATIONS}." \
            "Kafka implements SCRAM-SHA-256 and SCRAM-SHA-512 only and rejects a lower" \
            "iteration count outright. event_admin.go raises a low request to the minimum" \
            "rather than failing; this script refuses it so the two never disagree about" \
            "what was actually provisioned." \
            "Fix: unset it to accept the default of ${MIN_SCRAM_ITERATIONS}, or raise the value."
    fi
}

# ---------------------------------------------------------------------------------------
# The topic catalogue
# ---------------------------------------------------------------------------------------

# Derive every topic name from EVENT_CATEGORIES and the configured prefix.
#
# The prefix is trimmed of whitespace and of a leading or trailing separator, matching
# event_topics.go's topicPrefixTrimCutset, so that KAFKA_TOPIC_PREFIX=acme. and
# KAFKA_TOPIC_PREFIX=acme resolve identically instead of producing acme..transactions. A
# prefix that trims away to nothing falls back to the default for the same reason that file
# does: an empty prefix would yield names beginning with a bare dot.
#
# The resulting order is deliberate and matches AllTopicsWithDeadLetters: every category
# topic, then every dead-letter sibling.
resolve_topics() {
    local prefix category topic

    # Trimmed in a loop rather than once, because Go's strings.Trim removes EVERY leading and
    # trailing character in the cutset. "acme.." and " .acme. " both have to resolve to
    # "acme", or the two sides disagree for an input a human could plausibly type.
    prefix="$KAFKA_TOPIC_PREFIX"
    while :; do
        local before="$prefix"
        prefix="$(trim "$prefix")"
        prefix="${prefix#.}"
        prefix="${prefix%.}"
        if [[ "$prefix" == "$before" ]]; then
            break
        fi
    done

    if [[ -z "$prefix" ]]; then
        warn "KAFKA_TOPIC_PREFIX resolved to an empty prefix; falling back to 'blnk'." \
            "This matches event_topics.go, which falls back to DefaultTopicPrefix for the" \
            "same input, so the two still agree on every name."
        prefix="blnk"
    fi

    # THE SAME REFUSAL config/config.go APPLIES, for the same reason and with the same rule.
    #
    # Blnk's configuration load now REJECTS a prefix carrying a character Kafka does not
    # permit in a topic name, rather than warning and using it: the application would
    # otherwise capture outbox rows naming topics the broker will never create, and their
    # dead-letter names would be equally illegal, so events would be recorded and never
    # delivered. This script has to apply the identical rule or the two disagree about what
    # is provisionable — and this is the side an operator runs first, so a mismatch here is
    # discovered as "the topics exist but Blnk will not start".
    #
    # Kafka's legal set is letters, digits, '.', '_' and '-'. The length ceiling is 249 for
    # the COMPOSED name, and the longest suffix appended below is ".transactions.dlt" (17
    # characters), so the prefix budget is 232 — identical to
    # config.MaxKafkaTopicPrefixLength.
    if [[ "$prefix" =~ [^a-zA-Z0-9._-] ]]; then
        die "KAFKA_TOPIC_PREFIX contains characters Kafka does not permit in a topic name." \
            "Only letters, digits, '.', '_' and '-' are legal, so every topic composed from" \
            "'${prefix}' would be refused by the broker." \
            "Blnk's own configuration load refuses the same value, so provisioning with it" \
            "would create nothing usable and the application would not start either." \
            "Fix: set ${BLU}KAFKA_TOPIC_PREFIX${NC} to a legal namespace, for example ${BLU}blnk${NC}" \
            "or ${BLU}blnk-eu${NC}."
    fi

    if (( ${#prefix} > MAX_TOPIC_PREFIX_LENGTH )); then
        die "KAFKA_TOPIC_PREFIX is ${#prefix} characters, longer than the ${MAX_TOPIC_PREFIX_LENGTH} a prefix may be." \
            "Kafka refuses any topic name over ${MAX_TOPIC_NAME_LENGTH} characters, and the longest name" \
            "composed here is \"<prefix>.transactions${DEAD_LETTER_SUFFIX}\"." \
            "Blnk's configuration load applies the identical ceiling."
    fi

    # ONE pass per list, and each list composed from the same prefix in the same call, so no
    # two of them can be a different rendering of the same names. There were briefly four
    # successive rebuilds of GRANTABLE_TOPICS here, each overwriting the last: harmless only
    # because they agreed, and exactly the shape in which a divergence hides.
    CATEGORY_TOPICS=()
    DEAD_LETTER_TOPICS=()
    for category in "${EVENT_CATEGORIES[@]}"; do
        topic="${prefix}.${category}"
        CATEGORY_TOPICS+=("$topic")
        DEAD_LETTER_TOPICS+=("${topic}${DEAD_LETTER_SUFFIX}")
    done

    ALL_TOPICS=("${CATEGORY_TOPICS[@]}" "${DEAD_LETTER_TOPICS[@]}")

    # The subscriber-facing allowlist, composed from SUBSCRIBER_GRANTABLE_CATEGORIES and the
    # same prefix, so the two lists cannot disagree about a name. It is derived from its own
    # category list rather than filtered out of CATEGORY_TOPICS, so a category that must not
    # be granted cannot reach a subscriber grant by being forgotten in a filter. This is the
    # exact analogue of event_topics.go's SubscriberGrantableTopics, which adds the configured
    # prefix to model.SubscriberGrantableTopics for the same reason.
    GRANTABLE_TOPICS=()
    for category in "${SUBSCRIBER_GRANTABLE_CATEGORIES[@]}"; do
        GRANTABLE_TOPICS+=("${prefix}.${category}")
    done

    # The category topics no subscriber may be granted, derived rather than listed: a category
    # that is published to and not on the allowlist is internal by definition, so this stays
    # correct without a second policy list to keep in step.
    INTERNAL_TOPICS=()
    for topic in "${CATEGORY_TOPICS[@]}"; do
        if ! is_grantable_topic "$topic"; then
            INTERNAL_TOPICS+=("$topic")
        fi
    done

    # What the SAMPLE principal is granted by default: a subset of the allowlist, for the
    # reasons on SAMPLE_SUBSCRIBER_DEFAULT_CATEGORIES.
    SAMPLE_DEFAULT_TOPICS=()
    for category in "${SAMPLE_SUBSCRIBER_DEFAULT_CATEGORIES[@]}"; do
        SAMPLE_DEFAULT_TOPICS+=("${prefix}.${category}")
    done
}

# Report whether a fully-qualified topic name is one a subscriber may be granted.
#
# EXACT membership of GRANTABLE_TOPICS, matching model.IsSubscriberGrantableTopicName. It is
# deliberately not a prefix or suffix test: "blnk.transactions.dlt" starts with a grantable
# name and must still be refused, and a topic under some other prefix is not this stack's to
# grant at all.
#
# Returns 0 when the topic is grantable, 1 otherwise.
is_grantable_topic() {
    local candidate="$1" grantable

    for grantable in "${GRANTABLE_TOPICS[@]}"; do
        if [[ "$candidate" == "$grantable" ]]; then
            return 0
        fi
    done

    return 1
}

# The topics the sample subscriber is authorised to read.
#
# THE DEFAULT IS THE THREE DOMAIN CATEGORY TOPICS, which is a strict subset of what the API
# would allow. Two things are excluded, each for its own reason:
#
#   1. THE SYSTEM TOPIC, which a real subscriber CAN be granted and a sample principal is not.
#      It carries system.error, whose frozen payload includes Blnk's error text verbatim, so
#      granting it is a deliberate decision about one subscriber's entitlement - and a
#      provisioning script has made no such decision. Excluding it also gives the
#      subscriber-isolation criterion something to prove: a principal granted every category
#      has no outside, so the isolation assertion would be vacuous. ledger.created needs none of
#      this: it is on <prefix>.ledgers, a tenant topic the sample principal is granted by
#      default.
#   2. NO DEAD-LETTER SIBLING, and this one is not a policy but a rule the API enforces too. A
#      subscriber consumes events; a dead-letter topic holds events Blnk failed to publish and
#      is operator-facing, triaged and replayed through the internal events API. A principal
#      that could read the DLTs would make the isolation assertion vacuous from the other side.
#
# KAFKA_SAMPLE_SUBSCRIBER_TOPICS overrides the default, and it may name any topic on this
# script's allowlist - the four tenant category topics - but nothing outside it: the internal
# system topic, a dead-letter sibling, a category that does not exist, or a topic outside this
# stack's prefix is refused, because an override that could name any topic could grant a
# principal this stack minted something the API would have rejected. The internal topic is
# granted through the subscriber API in a deployment that has declared
# KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS, never from here.
resolve_subscriber_topics() {
    local entry

    SUBSCRIBER_TOPICS=()
    if [[ -z "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_TOPICS")" ]]; then
        SUBSCRIBER_TOPICS=("${SAMPLE_DEFAULT_TOPICS[@]}")
        return 0
    fi

    while IFS= read -r entry; do
        if ! is_grantable_topic "$entry"; then
            die "KAFKA_SAMPLE_SUBSCRIBER_TOPICS names a topic this script may not grant: '${entry}'." \
                "Grantable topics under prefix '$(trim "$KAFKA_TOPIC_PREFIX")':" \
                "  $(join_commas "${GRANTABLE_TOPICS[@]}")" \
                "This is the same default allowlist model.SubscriberGrantableTopics enforces," \
                "so a grant refused here is one POST /subscribers/{id}/kafka-credentials" \
                "would also refuse by default. Three kinds of name are commonly attempted and" \
                "all three are excluded on purpose:" \
                "  - any '${DEAD_LETTER_SUFFIX}' sibling, which is operator-facing and is triaged and" \
                "    replayed through the internal events API rather than consumed. No" \
                "    subscriber may be granted one, from here or from the API;" \
                "  - the internal category topic$( ((${#INTERNAL_TOPICS[@]} > 0)) && printf ' %s' "$(join_commas "${INTERNAL_TOPICS[@]}")"), which carries operator diagnostics." \
                "    A subscriber CAN be granted it, but only through the subscriber API on a" \
                "    deployment that has declared KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true," \
                "    because that grant is a deliberate entitlement decision and a bring-up" \
                "    script has made no such decision;" \
                "  - any name that is not one of the category topics above, including a" \
                "    category this deployment does not have." \
                "Fix: list a subset of the grantable topics above, or unset the variable to" \
                "take the sample default: $(join_commas "${SAMPLE_DEFAULT_TOPICS[@]}")."
        fi
        SUBSCRIBER_TOPICS+=("$entry")
    done < <(split_list "$KAFKA_SAMPLE_SUBSCRIBER_TOPICS" ",")

    if ((${#SUBSCRIBER_TOPICS[@]} == 0)); then
        die "KAFKA_SAMPLE_SUBSCRIBER_TOPICS is set but contains no usable topic name." \
            "Value: '${KAFKA_SAMPLE_SUBSCRIBER_TOPICS}'" \
            "Fix: give a comma-separated list of grantable topic names, or unset it to take" \
            "the sample default: $(join_commas "${SAMPLE_DEFAULT_TOPICS[@]}")."
    fi

    for entry in "${SUBSCRIBER_TOPICS[@]}"; do
        if is_grantable_topic "$entry"; then
            continue
        fi

        die "KAFKA_SAMPLE_SUBSCRIBER_TOPICS names '${entry}', which is not a grantable topic." \
            "Only Blnk-owned tenant category topics may be granted from here, and the" \
            "comparison is exact - a wildcard, a dead-letter topic, an internal category" \
            "topic, a broker-internal topic or a differently-cased or whitespace-padded name" \
            "is refused here for the same reason event_topics.go's IsSubscriberGrantableTopic" \
            "refuses it." \
            "Grantable (${#GRANTABLE_TOPICS[@]}): $(join_commas "${GRANTABLE_TOPICS[@]}")" \
            "Refused because it is a dead-letter sibling, if it looked familiar:" \
            "$(join_commas "${DEAD_LETTER_TOPICS[@]}")" \
            "Refused because it is internal, if it looked familiar:" \
            "$(join_commas "${INTERNAL_TOPICS[@]}")"
    done
}

# Resolve and validate the sample principal's identity before the broker is touched.
#
# Deliberately a precondition rather than a check inside ensure_sample_subscriber: an empty
# principal name is a configuration error, and finding it only after every topic has been
# assured means the operator reads a failure at the end of an otherwise successful run.
# Skipped entirely when the sample principal is skipped, because then neither value is used.
# Validate the producer principal, and refuse the two names that would do damage.
#
# Runs before the broker is touched, like every other decision in this group, so a bad value
# costs nothing. Nothing here needs the network.
require_valid_producer() {
    # THE SAME FLAG ensure_producer_principal reads. It used to be a different one, so a stack
    # that skipped the mutation could still be refused here for a producer value it was never
    # going to use, and a stack that skipped this check went on to create the principal anyway.
    if is_truthy "$KAFKA_SKIP_PRODUCER"; then
        return 0
    fi

    # KAFKA_SASL_USER first — the name the application resolves — then KAFKA_PRODUCER_USER,
    # which is what the compose kafka-init one-shot is given, then the shipped default. One
    # chain, in one place, identical to the compose files' own precedence.
    PRODUCER_USER="$(trim "$KAFKA_SASL_USER")"
    if [[ -z "$PRODUCER_USER" ]]; then
        PRODUCER_USER="$(trim "$KAFKA_PRODUCER_USER")"
    fi

    if [[ -z "$PRODUCER_USER" ]]; then
        die "neither KAFKA_SASL_USER nor KAFKA_PRODUCER_USER names a producer principal." \
            "This is the principal the server publishes events as, and it has to be" \
            "created before it can. Blnk will not let a publisher authenticate as" \
            "KAFKA_SASL_ADMIN_USER instead - config.ProducerSASL refuses, and the publisher" \
            "fails to build with a named error - so an empty producer principal on a stack" \
            "that has administrative credentials means events are never published at all." \
            "Fix: set KAFKA_SASL_USER to the principal the server will present," \
            "or leave both unset to accept the shipped default of blnk-producer, or set" \
            "KAFKA_SKIP_PRODUCER=1 if the cluster's owner manages this principal." \
            "Nothing has been provisioned."
    fi

    # The name reaches the SCRAM grammar, --entity-name and an ACL binding, so it is held to
    # the same alphabet as every other principal - including the exclusion of '*', which as an
    # ACL principal would grant Write on every Blnk topic to every authenticated client.
    require_safe_principal "the producer principal" "$PRODUCER_USER"

    # A SUPPLIED password is held to the full floor. A GENERATED one is checked inside
    # generate_password, so there is no branch here where an unchecked value is upserted.
    if [[ -n "$KAFKA_SASL_SECRET" ]]; then
        require_safe_credential "KAFKA_SASL_SECRET" "$KAFKA_SASL_SECRET"
        require_strong_credential "KAFKA_SASL_SECRET" "$KAFKA_SASL_SECRET"
    fi

    # THE PRODUCER MAY NOT BE THE ADMINISTRATOR (S6-06, PRIV-01).
    #
    # Two independent reasons, either sufficient. First, the upsert below would REWRITE the
    # administrative principal's password to a value nobody recorded, which is the same
    # unrecoverable cluster lock-out described under the sample subscriber - the broker's own
    # JAAS configuration holds the old password literally. Second, and the reason this check
    # exists even for an operator who supplied a password and would not lose anything:
    # creating the producer AS the administrator is precisely the excess-privilege posture
    # that config.ProducerSASL was changed to refuse. Accepting it here would provision, at
    # the broker, exactly the arrangement the application declines to use.
    local admin_user
    admin_user="$(trim "$KAFKA_SASL_ADMIN_USER")"
    if [[ -n "$admin_user" && "${PRODUCER_USER,,}" == "${admin_user,,}" ]]; then
        die "KAFKA_SASL_USER is '${PRODUCER_USER}', which is the ADMINISTRATIVE principal." \
            "The event publisher must not be the administrator. A leaked producer credential" \
            "would then carry authority to create topics, mint SCRAM credentials and rewrite" \
            "ACLs, and the broker's audit trail could not tell routine publishing from" \
            "cluster administration. config.ProducerSASL refuses that pair outright, so a" \
            "principal provisioned this way could not be used by Blnk anyway." \
            "Provisioning it would also rewrite the administrator's password to a value" \
            "nobody recorded, locking the cluster out on its next restart." \
            "Fix: unset KAFKA_SASL_USER to accept the default of blnk-producer, or name any" \
            "principal that is not the administrator." \
            "Nothing has been provisioned."
    fi

    # THE PRODUCER MAY NOT BE A SUBSCRIBER.
    #
    # A subscriber is granted Read; the producer is granted Write. One principal holding both
    # is a subscriber that can forge events into the topics it consumes, which makes the
    # isolation criterion meaningless in the one direction that matters most. The two
    # credential upserts would also contend, so whichever ran last would decide the password
    # and the other identity would silently stop authenticating.
    if ! is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER"; then
        local sample_user
        sample_user="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_USER")"
        if [[ -n "$sample_user" && "${PRODUCER_USER,,}" == "${sample_user,,}" ]]; then
            die "KAFKA_SASL_USER is '${PRODUCER_USER}', which is also KAFKA_SAMPLE_SUBSCRIBER_USER." \
                "One principal cannot be both. The producer is granted Write on every Blnk" \
                "topic and a subscriber is granted Read; merging them produces a subscriber" \
                "that can forge events into the topics it consumes, and the two credential" \
                "upserts would fight over the password so one of the two identities would" \
                "stop authenticating without saying so." \
                "Fix: give them different names - blnk-producer and blnk-sample-subscriber are" \
                "the defaults, and they differ." \
                "Nothing has been provisioned."
        fi
    fi
}

require_valid_subscriber() {
    if is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER"; then
        return 0
    fi

    SUBSCRIBER_USER="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_USER")"

    if [[ -z "$SUBSCRIBER_USER" ]]; then
        die "KAFKA_SAMPLE_SUBSCRIBER_USER is empty." \
            "A principal needs a name." \
            "Fix: unset it to accept the default of blnk-sample-subscriber, or set" \
            "KAFKA_SKIP_SAMPLE_SUBSCRIBER=1 to skip the sample principal entirely."
    fi

    # THE GROUP NAMESPACE IS DERIVED FROM THE PRINCIPAL AND TERMINATED WITH THE DELIMITER.
    # It is no longer an independent input, and both halves of that matter.
    #
    # Derived, because a group prefix is not a name a caller wants — it is a BOUNDARY the
    # caller would be selecting. The grant is a PREFIXED pattern, so whoever chooses the
    # prefix chooses how far the grant reaches: a value naming another subscriber's namespace
    # lets this principal join their groups and take their partition assignments.
    # api/model/event.go removed the equivalent field from the real API for exactly this
    # reason, and leaving it configurable here left the local stack able to demonstrate a
    # boundary the API forbids.
    #
    # Terminated, because an UNTERMINATED prefix reaches further than it reads. A PREFIXED
    # grant on "blnk-sample-subscriber" also matches "blnk-sample-subscriber-evil" and
    # "blnk-sample-subscriberX" — any group whose name merely STARTS with it — so a second
    # principal could be given a namespace that silently overlaps the first's. Appending the
    # delimiter confines the grant to "<principal>." and its descendants, which is what
    # model.CanonicalConsumerGroupNamespace does with SubscriberGroupTerminator on the Go
    # side; the two now express the same rule.
    SUBSCRIBER_GROUP_PREFIX="${SUBSCRIBER_USER}${SUBSCRIBER_GROUP_TERMINATOR}"

    # A SUPPLIED VALUE IS A CROSS-CHECK, NOT A CHOICE - AND THE TERMINATOR IS OPTIONAL IN IT.
    #
    # The variable is kept only so that an operator who believes they are choosing the
    # namespace is told they are not, instead of watching their value be ignored. So the
    # comparison has to answer one question - "does this name the same principal's
    # namespace?" - and nothing else.
    #
    # It used to compare for exact equality against the TERMINATED form, which made the
    # trailing '.' a required part of a value no shipped surface carried: .env.example, both
    # compose files' kafka-init fallbacks, this script's own usage output and its comment
    # table all named the bare principal. Every default local bring-up therefore died here,
    # kafka-init restarted on a loop, and because the server gates on that service
    # completing, the whole stack stayed at "created" with zero topics on a healthy broker
    # (auto-creation is off, so the relay would have had nothing to publish to either). The
    # remedy the message offered - "unset the variable" - could not work: Compose re-injected
    # the same rejected literal through its own ${VAR:-default}.
    #
    # Stripping ONE optional trailing terminator before comparing accepts both spellings of
    # the derived namespace and nothing else. The security property is untouched: the value
    # is discarded either way, SUBSCRIBER_GROUP_PREFIX stays derived, and every other
    # value - another principal's namespace, a broadening prefix like "blnk" - still stops
    # the run here. "blnk-sample-subscriber.." is rejected too, because only one terminator
    # is removed and the remainder must equal the principal exactly.
    local supplied_group_prefix
    supplied_group_prefix="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX")"

    if [[ -n "$supplied_group_prefix" ]] &&
        [[ "${supplied_group_prefix%"$SUBSCRIBER_GROUP_TERMINATOR"}" != "$SUBSCRIBER_USER" ]]; then
        die "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX is set to '${supplied_group_prefix}', which is not the namespace derived from the principal." \
            "Derived: ${SUBSCRIBER_GROUP_PREFIX} (the principal '${SUBSCRIBER_USER}' is also accepted," \
            "with or without the trailing '${SUBSCRIBER_GROUP_TERMINATOR}')" \
            "The consumer-group namespace is no longer chosen. It is granted as a PREFIXED" \
            "pattern, so choosing it means choosing how far the grant reaches - a value" \
            "naming another principal's namespace would let this one join their groups and" \
            "take their partition assignments. api/model/event.go removed the same field" \
            "from the real API for the same reason." \
            "Fix: unset the variable, or leave it empty - the namespace follows the" \
            "principal. If you need a different namespace, rename the principal with" \
            "KAFKA_SAMPLE_SUBSCRIBER_USER and the namespace follows it." \
            "Nothing has been provisioned."
    fi

    # The principal goes into the SCRAM grammar, the --entity-name flag and an ACL binding,
    # so it is held to the same alphabet as the administrative principal - including the
    # exclusion of '*', which as an ACL principal would grant every topic to everybody and
    # make the subscriber-isolation criterion vacuous.
    require_safe_principal "KAFKA_SAMPLE_SUBSCRIBER_USER" "$SUBSCRIBER_USER"

    # A group prefix reaches an ACL binding as a prefixed resource pattern. Same alphabet,
    # same reason.
    require_safe_principal "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX" "$SUBSCRIBER_GROUP_PREFIX"

    # THE SAMPLE PRINCIPAL MAY NOT BE THE ADMINISTRATOR (S6-06).
    #
    # KAFKA_SAMPLE_SUBSCRIBER_USER=admin used to be accepted, and the consequences compound.
    # The credential upsert would REWRITE the administrative principal's password to a
    # generated value nobody recorded, so:
    #
    #   - the broker's own inter-broker and controller JAAS configuration, which holds the old
    #     password as a literal, stops authenticating: the cluster locks itself out on restart;
    #   - Blnk's KAFKA_SASL_ADMIN_SECRET stops working, so topic assurance and subscriber
    #     credential issuance both fail;
    #   - this very script cannot authenticate on its next run, so the damage is not repairable
    #     by re-running it.
    #
    # And a "sample subscriber" that IS a super-user has no isolation to demonstrate: super
    # users bypass the authorizer entirely, so every ACL grant below becomes decorative and the
    # isolation criterion passes against a principal that could read everything anyway.
    #
    # The comparison is case-insensitive because Kafka principal matching is exact but operator
    # intent is not - "Admin" is the same mistake as "admin" and deserves the same answer.
    local admin_user
    admin_user="$(trim "$KAFKA_SASL_ADMIN_USER")"
    if [[ -n "$admin_user" ]]; then
        if [[ "${SUBSCRIBER_USER,,}" == "${admin_user,,}" ]]; then
            die "KAFKA_SAMPLE_SUBSCRIBER_USER is '${SUBSCRIBER_USER}', which is the ADMINISTRATIVE principal." \
                "Provisioning it would rewrite the administrator's password to a generated" \
                "value, and that is not recoverable by re-running this script: the broker's own" \
                "JAAS configuration holds the old password literally, so the cluster would lock" \
                "itself out on its next restart and Blnk's KAFKA_SASL_ADMIN_SECRET would stop" \
                "working at the same moment." \
                "It is also pointless as a sample: an administrator is in the broker's" \
                "super.users, super users bypass the authorizer, and every ACL granted below" \
                "would therefore have no effect at all." \
                "Fix: unset KAFKA_SAMPLE_SUBSCRIBER_USER to accept the default of" \
                "blnk-sample-subscriber, or name any principal that is not the administrator." \
                "Nothing has been provisioned."
        fi
    fi

    # THE GROUP NAMESPACE MAY NOT COVER OTHER SUBSCRIBERS (S6-06).
    #
    # The group grant is a PREFIXED pattern, so it matches every consumer group whose name
    # starts with the prefix. A prefix of "blnk" therefore covers blnk-sub-acme,
    # blnk-sub-globex and every other subscriber's group namespace - which means the sample
    # principal can join a real subscriber's consumer group, read from its partitions and
    # COMMIT OFFSETS INTO IT, silently advancing another tenant's consumer past events it
    # never received. That is worse than reading data it should not: it destroys the other
    # subscriber's position.
    #
    # model.SubscriberPrincipalNamespace ("blnk-sub-") is the namespace real subscriber groups
    # live in, and the rule is a containment test in the direction that matters: a prefix which
    # is a PREFIX OF that namespace covers all of it. "blnk", "blnk-", "blnk-s" all do;
    # "blnk-sub-acme" does not, because it can only cover one subscriber's groups - itself.
    local subscriber_namespace="blnk-sub-"
    if [[ "$subscriber_namespace" == "$SUBSCRIBER_GROUP_PREFIX"* ]]; then
        die "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX is '${SUBSCRIBER_GROUP_PREFIX}', which covers every subscriber's consumer groups." \
            "The group ACL is a PREFIXED pattern, and '${SUBSCRIBER_GROUP_PREFIX}' is a prefix" \
            "of '${subscriber_namespace}' - the namespace real subscriber groups are named in" \
            "(model.CanonicalConsumerGroupNamespace). So this grant would let the sample" \
            "principal join ANY subscriber's consumer group." \
            "That is not only a read of data it should not see. A consumer that joins a group" \
            "COMMITS OFFSETS into it, so the sample principal would advance a real" \
            "subscriber's position past events that subscriber never received - silent," \
            "unrecoverable data loss for them, with nothing in either system to indicate it." \
            "Fix: leave KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX empty so the namespace is" \
            "derived from the principal, or rename the principal so its derived namespace" \
            "covers it alone." \
            "Nothing has been provisioned."
    fi

    # An operator-supplied sample secret had NO validation at all: no placeholder check and
    # no alphabet check, unlike the administrative secret it sits beside. It goes into the
    # same --add-config grammar, so it gets the same two checks.
    local sample_secret
    sample_secret="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET")"
    if [[ -n "$sample_secret" ]]; then
        require_no_placeholder "KAFKA_SAMPLE_SUBSCRIBER_SECRET" "$sample_secret"
        require_safe_credential "KAFKA_SAMPLE_SUBSCRIBER_SECRET" "$sample_secret"
        # S6-07, and the same floor as the administrative secret. A sample principal is
        # still a principal that can read a ledger event stream.
        require_strong_credential "KAFKA_SAMPLE_SUBSCRIBER_SECRET" "$sample_secret"
        KAFKA_SAMPLE_SUBSCRIBER_SECRET="$sample_secret"
    else
        # Whitespace reads as absence, so a value of spaces generates a password rather
        # than provisioning a credential made of whitespace.
        KAFKA_SAMPLE_SUBSCRIBER_SECRET=""
    fi

    KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE="$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE")"

    # A GENERATION NEEDS A DELIVERY CHANNEL, AND THE ABSENCE OF ONE IS NOT AN ERROR.
    #
    # With neither a supplied secret nor a destination file, this script will not mint a
    # sample password at all, because the only remaining way to hand it over would be to
    # print it - and under the compose kafka-init service stdout IS the container log, which
    # retains the credential for the container's lifetime, hands it to anyone who can run
    # "docker compose logs", and forwards it to whatever collects the host's logs. "Shown
    # once" is true of a banner and false of a log.
    #
    # SO THAT STATE SKIPS THE SAMPLE PRINCIPAL RATHER THAN FAILING THE RUN, and the decision
    # is made in ensure_sample_subscriber where the broker's answer is known. Failing here
    # instead would refuse a bring-up that carries no .env at all - the topic catalogue would
    # never be assured, kafka-init would exit non-zero, and under the compose gate the server
    # and worker would not start. The sample principal is a local-development convenience;
    # the topics are what the relay actually needs. Losing the convenience is the cheaper
    # outcome by a wide margin, and the skip says exactly what to set to get it back.
    #
    # A REQUESTED ROTATION IS DIFFERENT and is still refused up front, by
    # require_rotation_destination: asking for a new password with nowhere to deliver it is
    # an instruction that cannot be carried out, rather than a convenience left unbuilt.
}

# ---------------------------------------------------------------------------------------
# Locating the Kafka CLI, or delegating to a container that has it
#
# Only one of this script's three invocation contexts has the CLI available: the compose
# kafka-init one-shot, which runs inside a Kafka image. The makefile target and stack.sh's
# fallback both run on the host, where it is very likely absent. A script that simply
# assumed PATH would work in one context and break the other two silently.
# ---------------------------------------------------------------------------------------

# Locate kafka-topics, kafka-configs and kafka-acls, all three from the same place.
#
# Two naming conventions exist: Confluent Platform images expose unsuffixed wrappers, while
# Apache Kafka distributions ship the .sh forms. Whichever convention answers first is used
# for all three tools, so a half-Confluent half-Apache invocation is impossible.
#
# PATH is probed first, then the well-known installation directories - and that fallback is
# not belt-and-braces. Apache Kafka images install the tools in /opt/kafka/bin WITHOUT
# adding that directory to PATH, so a PATH-only probe fails inside the very container this
# script is meant to run in, and would send it off to delegate into itself. Verified
# against apache/kafka, whose PATH carries the JDK and nothing else.
#
# Returns 0 with TOPICS_CLI, CONFIGS_CLI, ACLS_CLI and CLI_FLAVOUR set, or 1 when no
# complete set was found. It never dies, because failing to find the CLI is the normal case
# on a host and the caller's answer to it is delegation, not an error.
detect_kafka_cli() {
    local suffix directory topics configs acls

    for suffix in "" ".sh"; do
        if command -v "kafka-topics${suffix}" >/dev/null 2>&1 &&
            command -v "kafka-configs${suffix}" >/dev/null 2>&1 &&
            command -v "kafka-acls${suffix}" >/dev/null 2>&1; then
            TOPICS_CLI="$(command -v "kafka-topics${suffix}")"
            CONFIGS_CLI="$(command -v "kafka-configs${suffix}")"
            ACLS_CLI="$(command -v "kafka-acls${suffix}")"
            # Best-effort, and deliberately not part of the guard above: a missing offsets
            # tool must not make an otherwise usable environment unusable.
            OFFSETS_CLI="$(command -v "kafka-get-offsets${suffix}" 2>/dev/null || true)"
            CLI_FLAVOUR="PATH, kafka-*${suffix}"
            return 0
        fi
    done

    for directory in \
        /opt/kafka/bin \
        /usr/bin \
        /opt/bitnami/kafka/bin \
        /usr/local/kafka/bin; do
        for suffix in ".sh" ""; do
            topics="${directory}/kafka-topics${suffix}"
            configs="${directory}/kafka-configs${suffix}"
            acls="${directory}/kafka-acls${suffix}"
            if [[ -x "$topics" && -x "$configs" && -x "$acls" ]]; then
                TOPICS_CLI="$topics"
                CONFIGS_CLI="$configs"
                ACLS_CLI="$acls"
                if [[ -x "${directory}/kafka-get-offsets${suffix}" ]]; then
                    OFFSETS_CLI="${directory}/kafka-get-offsets${suffix}"
                fi
                CLI_FLAVOUR="${directory}, kafka-*${suffix}"
                return 0
            fi
        done
    done

    return 1
}

# Locate a working container runtime for delegation, preferring a plain "docker exec" on the
# named container and falling back to "docker compose exec -T" on the named service.
#
# The probe is a real call rather than a "command -v docker", because a docker binary that
# cannot reach a daemon is worse than no docker at all: it would take this branch and then
# fail with a message about a socket rather than about Kafka.
#
# Returns 0 with CONTAINER_RUNTIME, CONTAINER_TARGET, CONTAINER_STDIN_FLAG and
# CONTAINER_RUNTIME_LABEL set, or 1 when neither route works. The runtime is kept as an
# array rather than a string because "docker compose exec" is several words and rebuilding
# it by splitting a string would be one quoting bug away from a different command.
detect_container_runtime() {
    if ! command -v docker >/dev/null 2>&1; then
        return 1
    fi

    if docker exec "$KAFKA_CONTAINER" true >/dev/null 2>&1; then
        CONTAINER_RUNTIME=(docker exec)
        # docker exec does not attach stdin unless asked; docker compose exec does.
        CONTAINER_STDIN_FLAG=(-i)
        CONTAINER_TARGET="$KAFKA_CONTAINER"
        CONTAINER_RUNTIME_LABEL="docker exec ${KAFKA_CONTAINER}"
        return 0
    fi

    if docker compose ps --quiet "$KAFKA_COMPOSE_SERVICE" >/dev/null 2>&1 &&
        docker compose exec -T "$KAFKA_COMPOSE_SERVICE" true >/dev/null 2>&1; then
        CONTAINER_RUNTIME=(docker compose exec -T)
        CONTAINER_STDIN_FLAG=()
        CONTAINER_TARGET="$KAFKA_COMPOSE_SERVICE"
        CONTAINER_RUNTIME_LABEL="docker compose exec -T ${KAFKA_COMPOSE_SERVICE}"
        return 0
    fi

    return 1
}

# Re-execute this entire script inside the broker container.
#
# The whole script is delegated rather than each individual command, which keeps the
# temporary client-properties file on the side that has to read it - shipping commands
# across one at a time would mean either recreating that file per call or passing the
# credential on a command line once per command.
#
# Environment is propagated with docker's bare "-e NAME" form, which copies the value from
# this process's environment instead of placing it in the docker command line. That detail
# matters for exactly one variable: KAFKA_SASL_ADMIN_SECRET would otherwise be visible in
# the host's process table for the lifetime of the exec.
#
# The script source inside the container is the compose read-only mount at
# KAFKA_PROVISION_CONTAINER_SCRIPT when that path is readable, which is the documented
# arrangement. When it is not - a hand-started broker, or a stack that mounts nothing - the
# script is streamed in over stdin instead, so that "make kafka_provision" still works
# against a broker that was not started by compose.
#
# BLNK_KAFKA_PROVISION_IN_CONTAINER is set on the way in. If the container also lacks the
# CLI, the guard makes it report that plainly instead of delegating into itself forever.
# Decide what an operator-supplied client configuration becomes inside the container, and
# refuse rather than substitute something else.
#
# Called from delegate_to_container once a runtime has been found, so that the readability
# probe can run inside the container the delegated script will actually use.
#
# Three cases:
#
#   1. No KAFKA_CLIENT_CONFIG. Nothing to carry; the delegated run writes its own temporary
#      file from the credential pair, exactly as before.
#   2. KAFKA_CLIENT_CONFIG_CONTAINER_PATH is set. It is probed with "test -r" inside the
#      container and then becomes KAFKA_CLIENT_CONFIG for the delegated run, so the settings
#      in force are the ones the operator chose. An unreadable path fails here, before
#      anything is provisioned, rather than being silently replaced.
#   3. KAFKA_CLIENT_CONFIG is set and no container path is given. REFUSED. The file cannot
#      be read from inside the container, and the alternatives are both unacceptable:
#      regenerating a different configuration discards the operator's TLS material and can
#      downgrade a SASL_SSL connection to SASL_PLAINTEXT without a word, and copying the
#      file in would put a credential-bearing file into a container image's writable layer.
#      The refusal names both remedies.
delegate_client_config() {
    if [[ -z "$KAFKA_CLIENT_CONFIG" ]]; then
        return 0
    fi

    if [[ -n "$KAFKA_CLIENT_CONFIG_CONTAINER_PATH" ]]; then
        if ! "${CONTAINER_RUNTIME[@]}" "$CONTAINER_TARGET" \
            test -r "$KAFKA_CLIENT_CONFIG_CONTAINER_PATH" >/dev/null 2>&1; then
            die "KAFKA_CLIENT_CONFIG_CONTAINER_PATH is '${KAFKA_CLIENT_CONFIG_CONTAINER_PATH}', which is not readable inside '${CONTAINER_TARGET}'." \
                "This script is delegating into that container because the Kafka CLI is not" \
                "available here, so the client configuration has to be readable THERE." \
                "It is checked now rather than used blindly: authenticating with a file that" \
                "turned out to be something else - or silently falling back to a generated" \
                "SASL_PLAINTEXT configuration - could send your credential over an" \
                "unencrypted connection." \
                "Fix: mount the properties file into the container and point" \
                "KAFKA_CLIENT_CONFIG_CONTAINER_PATH at the path it appears on inside it," \
                "for example:" \
                "  volumes: ['./kafka-client.properties:/etc/blnk/kafka-client.properties:ro']" \
                "  KAFKA_CLIENT_CONFIG_CONTAINER_PATH=/etc/blnk/kafka-client.properties" \
                "Nothing has been provisioned."
        fi

        log "carrying the operator-supplied client configuration into the container" \
            "host path      : ${KAFKA_CLIENT_CONFIG}" \
            "container path : ${KAFKA_CLIENT_CONFIG_CONTAINER_PATH} (verified readable)"

        KAFKA_CLIENT_CONFIG="$KAFKA_CLIENT_CONFIG_CONTAINER_PATH"

        return 0
    fi

    die "KAFKA_CLIENT_CONFIG is set to '${KAFKA_CLIENT_CONFIG}', but this run has to delegate into '${CONTAINER_TARGET}' where that path does not apply." \
        "The Kafka CLI is not available here, so the commands run inside the broker" \
        "container - and a host path is meaningless there." \
        "This is refused rather than worked around, because both workarounds are worse than" \
        "an error. Writing a fresh configuration in the container would silently discard" \
        "everything you put in that file - a custom CA, a truststore, a non-SCRAM mechanism," \
        "hostname verification - and would replace a SASL_SSL connection with a" \
        "SASL_PLAINTEXT one, sending your credential in the clear against a broker that" \
        "accepts both. Copying the file in would leave a credential-bearing file inside the" \
        "container." \
        "Fix, either one:" \
        "  1. mount the file into the broker container and name its path there:" \
        "       volumes: ['./kafka-client.properties:/etc/blnk/kafka-client.properties:ro']" \
        "       KAFKA_CLIENT_CONFIG_CONTAINER_PATH=/etc/blnk/kafka-client.properties" \
        "  2. run this script inside the container, where the file already is:" \
        "       docker exec -it ${KAFKA_CONTAINER} bash ${KAFKA_PROVISION_CONTAINER_SCRIPT}" \
        "Nothing has been provisioned."
}

delegate_to_container() {
    if ! detect_container_runtime; then
        die "the Kafka CLI tools are not available here, and no Kafka container could be reached." \
            "This script needs kafka-topics, kafka-configs and kafka-acls. Searched PATH for" \
            "both the unsuffixed and the .sh forms, then /opt/kafka/bin, /usr/bin," \
            "/opt/bitnami/kafka/bin and /usr/local/kafka/bin; then tried to delegate to" \
            "container '${KAFKA_CONTAINER}' and compose service '${KAFKA_COMPOSE_SERVICE}'." \
            "Any one of these fixes it:" \
            "  1. bring the stack up so the broker container exists - 'docker compose up -d" \
            "     ${KAFKA_COMPOSE_SERVICE}' - then re-run 'make kafka_provision';" \
            "  2. run this script inside the broker container, where the CLI already is:" \
            "     docker exec -it ${KAFKA_CONTAINER} bash ${KAFKA_PROVISION_CONTAINER_SCRIPT};" \
            "  3. install a Kafka distribution on this host so the CLI tools are present," \
            "     and point KAFKA_BOOTSTRAP_SERVER at the broker." \
            "If the container is named something else, set KAFKA_CONTAINER or" \
            "KAFKA_COMPOSE_SERVICE."
    fi

    # Only names, never values, and the names come from THE canonical interface rather than
    # from a copy of it maintained here. See KAFKA_PROVISION_INTERFACE for why a list per
    # invocation path was the defect: four paths each carried their own, they had drifted in
    # every direction, and a missing name falls back to a default and reports success.
    #
    # The recursion marker is prepended because it is the one name the delegation itself owns.
    # The host-only names are deliberately absent: they say where to delegate TO and are
    # meaningless on the far side.
    local passthrough=(
        BLNK_KAFKA_PROVISION_IN_CONTAINER
        "${KAFKA_PROVISION_INTERFACE[@]}"
    )

    # An operator-supplied client configuration must NOT be silently dropped.
    #
    # It used to be: the path was deliberately left out of the pass-through because a host
    # path is unlikely to exist in the container, and a fresh file was written there
    # instead. That quietly discarded everything the operator had chosen - a custom CA and
    # truststore, a non-SCRAM mechanism, SSL hostname-verification settings, timeouts - and
    # replaced it with SASL_PLAINTEXT SCRAM assembled from the environment. Against a
    # SASL_SSL broker that fails as a handshake error with no hint that the configuration
    # was substituted; worse, against a broker that accepts both it would SUCCEED while
    # sending the credential over an unencrypted connection.
    #
    # So the choice is made explicitly instead. KAFKA_CLIENT_CONFIG_CONTAINER_PATH names the
    # file inside the container, it is verified as readable there before anything is
    # provisioned, and it is what the delegated run uses. Without it, the delegation refuses.
    delegate_client_config

    # The resolved values are exported so that the bare "-e NAME" form has something to copy,
    # and the export is DRIVEN BY THE SAME LIST rather than written out beside it. A
    # hand-written export block is a third copy of the interface and it had already fallen
    # behind: it exported the retired skip flag and named one variable twice, so a name could
    # appear in the pass-through and never be exported - which is the silent-default failure
    # the pass-through exists to prevent, reintroduced one line lower down.
    #
    # KAFKA_BOOTSTRAP_SERVER is exported after resolution rather than as it arrived, so the
    # container provisions the broker this process decided on.
    export BLNK_KAFKA_PROVISION_IN_CONTAINER=1
    local exported
    for exported in "${KAFKA_PROVISION_INTERFACE[@]}"; do
        export "${exported?}"
    done

    local env_flags=() name
    for name in "${passthrough[@]}"; do
        if [[ -n "${!name+declared}" ]]; then
            env_flags+=(-e "$name")
        fi
    done

    if "${CONTAINER_RUNTIME[@]}" "$CONTAINER_TARGET" \
        test -r "$KAFKA_PROVISION_CONTAINER_SCRIPT" >/dev/null 2>&1; then
        log "no Kafka CLI here; delegating to '${CONTAINER_RUNTIME_LABEL}'" \
            "running ${KAFKA_PROVISION_CONTAINER_SCRIPT} inside the container" \
            "broker: ${KAFKA_BOOTSTRAP_SERVER}"
        exec "${CONTAINER_RUNTIME[@]}" "${env_flags[@]}" "$CONTAINER_TARGET" \
            bash "$KAFKA_PROVISION_CONTAINER_SCRIPT"
    fi

    local source_path="${BASH_SOURCE[0]:-}"
    if [[ -z "$source_path" || ! -r "$source_path" ]]; then
        die "cannot delegate into '${CONTAINER_TARGET}': this script is not readable from disk." \
            "${KAFKA_PROVISION_CONTAINER_SCRIPT} does not exist in the container either, so" \
            "there is nothing to run there." \
            "Fix: mount ./scripts into the broker container read-only, as the compose stack" \
            "does, or set KAFKA_PROVISION_CONTAINER_SCRIPT to where the script already is."
    fi

    log "no Kafka CLI here; delegating to '${CONTAINER_RUNTIME_LABEL}'" \
        "${KAFKA_PROVISION_CONTAINER_SCRIPT} is not present in the container, so this" \
        "script is streamed in over stdin instead" \
        "broker: ${KAFKA_BOOTSTRAP_SERVER}"

    # "bash -s" makes the container's stdin this script's own source text. Every Kafka CLI
    # invocation therefore reads from /dev/null - see kafka_cli - because a child that
    # consumed stdin would eat the rest of the script and the run would end mid-function.
    exec "${CONTAINER_RUNTIME[@]}" "${CONTAINER_STDIN_FLAG[@]}" "${env_flags[@]}" \
        "$CONTAINER_TARGET" bash -s <"$source_path"
}

# ---------------------------------------------------------------------------------------
# The client-properties file, and its disposal
#
# The Kafka CLI tools have no inline credential flag. Authentication settings can only be
# supplied through a properties file named by --command-config, so one has to exist for the
# duration of the run and must not exist afterwards.
# ---------------------------------------------------------------------------------------

# Remove the properties file this script created, on every exit path.
#
# Create an empty file that only this user can read, and register it for removal.
#
# Extracted from the client-config writer below, which had this logic inline, because Q4-20
# needs the identical guarantees for a second and third kind of file: the SCRAM properties
# handed to "kafka-configs --add-config-file", and the one-time artifact the generated sample
# credential is recorded in. Three copies of "umask 077, mktemp, chmod 600, remember to
# delete it" would be three chances to get one of them wrong.
#
# The umask is set and restored around the creation rather than left changed, so nothing
# else this script writes inherits it by accident.
#
# Parameters:
#   $1 - a short label used in the filename and in any failure message.
# Prints the path on stdout. Dies if no writable location can be found.
new_secret_file() {
    local label="$1"
    local directory="${TMPDIR:-/tmp}"

    if [[ ! -d "$directory" || ! -w "$directory" ]]; then
        die "the temporary directory '${directory}' is not a writable directory." \
            "A ${label} file has to be written somewhere outside the repository, because" \
            "./scripts is mounted read-only in the compose stack and the Kafka CLI has no" \
            "way to accept this material other than from a file." \
            "Fix: set TMPDIR to a writable directory."
    fi

    local previous_umask path=""
    previous_umask="$(umask)"
    umask 077

    if command -v mktemp >/dev/null 2>&1; then
        path="$(mktemp "${directory%/}/blnk-kafka-${label}-XXXXXX" 2>/dev/null || true)"
    fi

    # Fallback for an image without mktemp. O_EXCL is approximated with an existence test
    # plus "set -o noclobber" on the redirection, which fails rather than truncating if the
    # path was created between the test and the write.
    if [[ -z "$path" ]]; then
        path="${directory%/}/blnk-kafka-${label}-$$-${RANDOM}"
        if ! (set -o noclobber && : >"$path") 2>/dev/null; then
            umask "$previous_umask"
            die "could not create a ${label} file in '${directory}'." \
                "Tried mktemp and then '${path}'." \
                "Fix: set TMPDIR to a writable directory."
        fi
    fi

    umask "$previous_umask"

    # Belt and braces on top of the umask: an inherited-directory oddity or a fallback path
    # that already existed must not leave the contents readable.
    chmod 600 "$path" 2>/dev/null || true

    # Registered BEFORE it is written, so a failure part-way through still leaves the trap a
    # path to remove.
    GENERATED_SECRET_FILES+=("$path")

    printf '%s' "$path"
}

# GENERATED_CLIENT_CONFIG is set only when this script wrote the file, so an
# operator-supplied KAFKA_CLIENT_CONFIG is never touched - deleting a file the operator
# manages would be a genuinely destructive surprise.
#
# It cannot fail and cannot change the exit status. Both matter: this runs from an EXIT trap,
# and a cleanup that returned non-zero on an otherwise successful run could turn a clean
# provisioning into a failure - which, for the compose kafka-init one-shot with
# "restart: on-failure", would mean an endless restart loop over a deleted temporary file.
cleanup() {
    if [[ -n "$GENERATED_CLIENT_CONFIG" && -f "$GENERATED_CLIENT_CONFIG" ]]; then
        rm -f "$GENERATED_CLIENT_CONFIG" || true
    fi

    # Every scratch file that held credential material, removed here for the same reasons and
    # with the same guarantees (Q4-20). The one-time artifacts recording a GENERATED
    # credential - the sample subscriber's and the producer's - are deliberately NOT in this
    # list: they are the operator's copies and outliving the run is their entire purpose.
    #
    # "${arr[@]+...}" guards the expansion, because an empty array under "set -u" is an
    # unbound-variable error on the bash versions this script has to run under.
    local file
    for file in ${GENERATED_SECRET_FILES[@]+"${GENERATED_SECRET_FILES[@]}"}; do
        if [[ -n "$file" && -f "$file" \
              && "$file" != "$SUBSCRIBER_SECRET_ARTIFACT" \
              && "$file" != "$PRODUCER_SECRET_ARTIFACT" ]]; then
            rm -f "$file" || true
        fi
    done

    return 0
}

# on_signal turns a received signal into a TERMINATION with the conventional status.
#
# The previous arrangement - 'trap cleanup EXIT INT TERM' - removed the credential file but
# did not stop the script. A bash trap handler RETURNS when it finishes, and execution then
# resumes at the point the signal interrupted, so a Ctrl-C mid-run deleted the
# client-properties file and then carried on provisioning against a file that no longer
# existed: every subsequent CLI call failed on a missing --command-config, and the run ended
# in a confusing cascade rather than at the interruption. It also exited 0 on a run the
# operator had cancelled.
#
# Cleanup therefore stays on EXIT alone, and each signal gets a handler that exits with the
# status a shell is expected to report for it - 128 + the signal number, so 130 for INT and
# 143 for TERM. Calling exit is what fires the EXIT trap, so the file is still removed
# exactly once, by the one handler responsible for it.
#
# The handler reports on descriptor 9 rather than on stderr, and that is not a stylistic
# choice. A trapped signal is not dispatched when it arrives: bash defers it until the command
# in progress returns. When that command carries redirections and is a function or a builtin -
# 'kafka_topics --list >/dev/null 2>&1' in wait_for_broker is the one an operator is most
# likely to interrupt, since it is where the run can sit for the whole readiness budget - bash
# applies those redirections to the shell itself and restores them afterwards, and the deferred
# handler runs INSIDE that window. Writing to stderr there sends the report to /dev/null: the
# run still exits 130 and still removes its credential file, but the operator who pressed
# Ctrl-C is told nothing whatsoever, which is the one thing this handler exists to do.
# Descriptor 9 is a duplicate of the original stderr taken once, below, before any such window
# can exist, so no later redirection can reach it. A fixed descriptor is used rather than
# bash's automatic '{var}>&2' allocation because that syntax needs bash 4.1, and /bin/bash is
# still 3.2 on macOS; 9 is free here, as this script opens no other descriptor.
#
# The duplication is allowed to fail: a caller that starts this script with stderr closed
# ('2>&-') has nowhere for the report to go, and that is a reason to stay quiet, not a reason to
# refuse to provision. The handler below tolerates a descriptor that was never opened.
exec 9>&2 || true
readonly SIGNAL_REPORT_FD=9

on_signal() {
    local name="$1" status="$2"

    # The report is best-effort; the exit status is not. Any failure to write - stderr closed
    # at launch, a full disk - is swallowed here, because under "set -e" a failed write would
    # abort the handler before the exit below and the run would then report that write error
    # instead of the conventional signal status, which is the one thing this handler must get
    # right.
    {
        printf '%s\n' "${YEL}==> interrupted:${NC} received SIG${name}; provisioning stopped."
        _continuation \
            "Any temporary client-properties file is removed on the way out." \
            "Whatever was already created on the broker is left in place - topic creation, ACL" \
            "addition and the SCRAM upsert are each idempotent, so re-running this script" \
            "finishes the job rather than duplicating it."
    } 1>&"$SIGNAL_REPORT_FD" 2>/dev/null || true

    exit "$status"
}

trap cleanup EXIT
trap 'on_signal INT 130' INT
trap 'on_signal TERM 143' TERM

# Resolve the properties file the CLI will authenticate with.
#
# An operator-supplied KAFKA_CLIENT_CONFIG is used verbatim: nothing is generated, nothing
# is deleted, and this script adds no settings to it. That is the supported way to configure
# TLS material for a SASL_SSL broker, or anything else this script does not model.
#
# Otherwise a file is written, and where and how matter:
#
#   - Under TMPDIR (or /tmp), never inside the repository. ./scripts is mounted READ-ONLY in
#     the compose stack, so writing beside this script would fail there; and a credential
#     file inside a working tree is one "git add ." away from being committed.
#   - Mode 0600, set by umask before creation rather than chmod after it, so the file is
#     never even briefly group- or world-readable. The umask is restored immediately, since
#     it is process-global state and the sample-credential generator runs later.
#   - Removed by the trap above on success, on failure and on Ctrl-C.
prepare_client_config() {
    if [[ -n "$KAFKA_CLIENT_CONFIG" ]]; then
        if [[ ! -r "$KAFKA_CLIENT_CONFIG" ]]; then
            die "KAFKA_CLIENT_CONFIG is set to '${KAFKA_CLIENT_CONFIG}', which is not readable." \
                "Fix: point it at a Kafka client properties file carrying security.protocol," \
                "sasl.mechanism and sasl.jaas.config, or unset it to have one written for" \
                "this run from KAFKA_SASL_ADMIN_USER and KAFKA_SASL_ADMIN_SECRET."
        fi
        CLIENT_CONFIG="$KAFKA_CLIENT_CONFIG"
        log "using the operator-supplied client configuration ${CLIENT_CONFIG}" \
            "It is used verbatim and will not be modified or removed."
        return 0
    fi

    # Created by the shared helper, which owns the umask, the mktemp fallback, the chmod and
    # the cleanup registration. GENERATED_CLIENT_CONFIG is still set separately because the
    # log line below distinguishes a file this script wrote from an operator-supplied one.
    local path
    path="$(new_secret_file provision)"
    GENERATED_CLIENT_CONFIG="$path"
    CLIENT_CONFIG="$path"

    # The one and only place the secret is written. Assembled with printf into a file whose
    # mode is already 0600; no echo of any part of it, and no intermediate command line.
    #
    # BOTH INTERPOLATED VALUES HAVE ALREADY BEEN HELD TO THE SAFE ALPHABET by
    # require_admin_credentials, which is what makes the JAAS line below correct rather than
    # merely conventional: the value ends at the first unescaped '"' and terminates at ';',
    # and a backslash is an escape in both JAAS and the Java Properties format, so a
    # credential containing any of those would produce a file the client misreads.
    #
    # The no-SASL mode writes the protocol alone. A sasl.mechanism with no jaas.config is a
    # client that announces SASL and then has nothing to authenticate with, which fails the
    # handshake with an error about the mechanism rather than about the missing credential.
    local written
    if [[ "$ADMIN_SASL_MODE" == "yes" ]]; then
        printf '%s\n' \
            "security.protocol=${KAFKA_SECURITY_PROTOCOL}" \
            "sasl.mechanism=${SCRAM_MECHANISM}" \
            "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${KAFKA_SASL_ADMIN_USER}\" password=\"${KAFKA_SASL_ADMIN_SECRET}\";" \
            >"$path" && written=yes
    else
        printf '%s\n' "security.protocol=${KAFKA_SECURITY_PROTOCOL}" >"$path" && written=yes
    fi

    if [[ "${written:-}" != "yes" ]]; then
        die "could not write the client-properties file '${path}'." \
            "Fix: check that ${directory} is writable, or pass KAFKA_CLIENT_CONFIG pointing" \
            "at a properties file you manage yourself."
    fi

    if [[ "$ADMIN_SASL_MODE" == "yes" ]]; then
        log "wrote a temporary client configuration for this run" \
            "path      : ${path} (mode 0600, removed on exit)" \
            "protocol  : ${KAFKA_SECURITY_PROTOCOL}" \
            "mechanism : ${SCRAM_MECHANISM}" \
            "principal : ${KAFKA_SASL_ADMIN_USER}"
    else
        log "wrote a temporary client configuration for this run" \
            "path      : ${path} (mode 0600, removed on exit)" \
            "protocol  : ${KAFKA_SECURITY_PROTOCOL}" \
            "mechanism : none - no administrative SASL credential is configured"
    fi
}

# ---------------------------------------------------------------------------------------
# Kafka CLI wrappers
#
# Every broker call goes through one of these three, so the bootstrap server and the
# credential file are applied in exactly one place and cannot be forgotten at a call site.
#
# stdin is redirected from /dev/null on all of them, for two reasons. It guarantees these
# tools can never wait for input and hang a one-shot container - kafka-acls prompts before a
# --remove, and a future flag might prompt for something else. And on the delegation path
# that streams this script over stdin, a child that read stdin would consume the script's
# own remaining source text.
# ---------------------------------------------------------------------------------------

# EVERY CLI call is bounded, and the reason is that nothing else bounds it.
#
# KAFKA_PROVISION_TIMEOUT_SECONDS bounds the READINESS WAIT and nothing more: once the broker
# has answered one authenticated request, every subsequent call ran unbounded. A single hung
# operation - a topic create against a broker that has lost its controller, a SCRAM upsert
# that never returns, an ACL write blocked on metadata - therefore hung the whole run
# indefinitely. In the compose kafka-init one-shot that is a container that never exits and a
# 'docker compose up' that never completes; under 'make kafka_provision' it is a terminal that
# never returns. Either way the outcome is worse than a failure, because a failure is
# actionable and a hang is not.
#
# The bound is applied HERE, in the three wrappers every call goes through, rather than at each
# call site. That is deliberate: a bound applied per call site is a bound the next call site
# added will not have.
#
# 'timeout' is used when it exists and silently skipped when it does not. It is in GNU
# coreutils and present in every image this script realistically runs in, but a busybox or
# macOS host may lack it, and refusing to provision because a timing safeguard is unavailable
# would trade a rare hang for a certain failure. The degradation is announced once by
# detect_cli_timeout rather than assumed.
#
# --kill-after gives the JVM a window to exit on TERM before it is killed, so a CLI that is
# merely slow to shut down is not reported as unkillable.
detect_cli_timeout() {
    require_positive_int "KAFKA_CLI_TIMEOUT_SECONDS" "$KAFKA_CLI_TIMEOUT_SECONDS"
    require_positive_int "KAFKA_CLI_KILL_GRACE_SECONDS" "$KAFKA_CLI_KILL_GRACE_SECONDS"

    if CLI_TIMEOUT="$(command -v timeout 2>/dev/null)" && [[ -n "$CLI_TIMEOUT" ]]; then
        log "bounding every Kafka CLI call at ${KAFKA_CLI_TIMEOUT_SECONDS}s" \
            "A hung administrative operation is stopped rather than allowed to hang the run." \
            "Raise KAFKA_CLI_TIMEOUT_SECONDS on a slow or heavily loaded broker."

        return 0
    fi

    # Announced once, and only a warning. Refusing to provision because a timing safeguard is
    # unavailable would trade a rare hang for a certain failure, but leaving it unsaid would
    # mean an operator whose run hangs has no way to know the bound was never in effect.
    CLI_TIMEOUT=""
    warn "'timeout' is not available on this host, so Kafka CLI calls are NOT bounded" \
        "A single hung administrative operation will hang this run with no deadline to end" \
        "it - in the compose kafka-init one-shot that is a container that never exits." \
        "Everything else proceeds normally." \
        "Fix, if you want the bound: install GNU coreutils (Debian and Ubuntu ship" \
        "'timeout' in coreutils; on macOS 'brew install coreutils' provides gtimeout), or" \
        "run this script inside the broker container, where it is present."
}

kafka_cli_timeout() {
    if [[ -z "$CLI_TIMEOUT" ]]; then
        return 0
    fi

    printf '%s\n' "$CLI_TIMEOUT" "--kill-after=${KAFKA_CLI_KILL_GRACE_SECONDS}" "${KAFKA_CLI_TIMEOUT_SECONDS}"
}

kafka_topics() {
    local bound=()
    while IFS= read -r part; do bound+=("$part"); done < <(kafka_cli_timeout)

    "${bound[@]}" "$TOPICS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

kafka_configs() {
    local bound=()
    while IFS= read -r part; do bound+=("$part"); done < <(kafka_cli_timeout)

    "${bound[@]}" "$CONFIGS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

kafka_acls() {
    local bound=()
    while IFS= read -r part; do bound+=("$part"); done < <(kafka_cli_timeout)

    "${bound[@]}" "$ACLS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

kafka_get_offsets() {
    "$OFFSETS_CLI" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
        --command-config "$CLIENT_CONFIG" "$@" </dev/null
}

# offsets_unavailable_reason explains why emptiness could not be established, so the warning
# names something actionable instead of stating a bare unknown.
#
# Outputs:
#   a single explanatory sentence on stdout.
offsets_unavailable_reason() {
    if [[ -z "$OFFSETS_CLI" ]]; then
        printf '%s' "No kafka-get-offsets tool was found beside the CLI tools in use" \
            "(${CLI_FLAVOUR}), so partition offsets could not be read."
        return 0
    fi

    printf '%s' "Reading partition offsets with ${OFFSETS_CLI} failed or returned no usable" \
        "value; $(admin_identity_label) may lack Describe on the topic."
}

# topic_record_state reports whether a topic holds any records.
#
# It sums the LATEST offset of every partition. A topic whose every partition is at offset zero
# has never been written to, which is the only state in which raising the partition count cannot
# re-map an existing key to a different partition.
#
# Retention makes this conservative in the right direction: a topic whose records have all
# aged out still reports a non-zero latest offset, so it is treated as holding records. That
# refuses a growth which would in fact have been harmless, which is the safe way to be wrong.
#
# Arguments:
#   $1 - the topic name.
# Outputs:
#   "empty", "records", or "unknown" on stdout.
topic_record_state() {
    local topic="$1" output line offset total=0

    if [[ -z "$OFFSETS_CLI" ]]; then
        printf '%s\n' "unknown"
        return 0
    fi

    if ! output="$(kafka_get_offsets --topic "$topic" --time -1 2>/dev/null)"; then
        printf '%s\n' "unknown"
        return 0
    fi

    if [[ -z "$(trim "$output")" ]]; then
        printf '%s\n' "unknown"
        return 0
    fi

    # Each line is topic:partition:offset. A partition with no leader prints an empty
    # offset field, which is not proof of emptiness, so it makes the whole answer unknown.
    while IFS= read -r line; do
        [[ -z "$(trim "$line")" ]] && continue
        offset="${line##*:}"
        offset="$(trim "$offset")"
        if [[ ! "$offset" =~ ^[0-9]+$ ]]; then
            printf '%s\n' "unknown"
            return 0
        fi
        total=$((total + 10#$offset))
    done <<<"$output"

    if ((total > 0)); then
        printf '%s\n' "records"
    else
        printf '%s\n' "empty"
    fi
}

# Wait for the broker to answer an AUTHENTICATED request, within a bound.
#
# The probe is "kafka-topics --list", which is cheap and, crucially, exercises the whole
# path this script depends on: TCP, the SASL handshake, SCRAM verification and authorization.
# A TCP-only check would report success against a broker that then refuses every command.
#
# The loop is bounded because compose gates kafka-init on the broker healthcheck but the
# makefile target and stack.sh's fallback do not: without a wait those two race the broker,
# and without a bound a broker that never arrives hangs the bring-up instead of failing it.
# Every attempt is guarded so that a failure cannot abort the script under "set -e".
wait_for_broker() {
    local deadline=$((SECONDS + 10#$KAFKA_PROVISION_TIMEOUT_SECONDS))
    local attempt=0

    log "waiting for ${KAFKA_BOOTSTRAP_SERVER} to accept an authenticated request" \
        "budget: ${KAFKA_PROVISION_TIMEOUT_SECONDS}s, polling every ${KAFKA_PROVISION_POLL_INTERVAL_SECONDS}s"

    while :; do
        attempt=$((attempt + 1))
        if kafka_topics --list >/dev/null 2>&1; then
            ok "broker reachable, connected as $(admin_identity_label) (attempt ${attempt})"
            return 0
        fi

        if ((SECONDS >= deadline)); then
            break
        fi

        # Guarded like every other external call here: a sleep that somehow failed must not
        # abort the run under "set -e". The deadline check above is what bounds the loop, so
        # losing a sleep costs a busier poll, not correctness.
        sleep "$KAFKA_PROVISION_POLL_INTERVAL_SECONDS" || true
    done

    # One last attempt with its output captured, so the diagnosis can carry the broker's own
    # words. Filtered through redact first: a client-configuration parse error is exactly the
    # class of failure that quotes the offending line back, and that line holds the password.
    local final_output
    final_output="$(kafka_topics --list 2>&1 | redact || true)"
    if [[ -n "$final_output" ]]; then
        printf '%s\n' "$final_output" >&2
    fi

    die "gave up waiting for ${KAFKA_BOOTSTRAP_SERVER} after ${KAFKA_PROVISION_TIMEOUT_SECONDS}s and ${attempt} attempts." \
        "Any broker output above is the CLI's own, with credential-bearing lines removed." \
        "There are three usual causes, in the order worth checking:" \
        "  1. the broker is not up, or not up at this address. Check 'docker compose ps'" \
        "     and that KAFKA_BOOTSTRAP_SERVER names a reachable host and port. Inside the" \
        "     compose network that is the service name; from the host it is localhost and" \
        "     the published port." \
        "  2. authentication is failing because scripts/kafka-bootstrap.sh never seeded" \
        "     $(admin_identity_label) as a principal, or seeded it with a different" \
        "     password." \
        "     In KRaft mode SCRAM credentials live in the metadata log and can only be" \
        "     created there while storage is formatted, so a broker whose storage was" \
        "     formatted without --add-scram can authenticate nobody at all." \
        "  3. authorization is failing because $(admin_identity_label) is not in the" \
        "     broker's super.users while this authorizer is active:" \
        "       ${REQUIRED_AUTHORIZER}" \
        "     The credential is then valid but every topic and ACL operation is denied," \
        "     which is the failure mode hardest to guess at because the password is not" \
        "     the problem. Add that principal to super.users in the broker configuration."
}

# ---------------------------------------------------------------------------------------
# Step one: the topic catalogue
#
# Create if absent, grow if under-partitioned, leave alone and warn if over-partitioned.
# Never fail on a topic that already exists, and never attempt a shrink.
#
# This is the same rule event_admin.go's EnsureTopics applies at runtime through
# CreateTopics and CreatePartitions, and the two are kept identical on purpose: an operator
# who provisions with this script and then lets the service assure topics on start-up must
# not see the two disagree.
# ---------------------------------------------------------------------------------------

# Read a topic's OBSERVED geometry: its partition count and its replication factor.
#
# Prints "<partitions> <replication-factor>" on success and nothing at all when the topic's
# summary line could not be read. Both numbers come from the same --describe summary line,
# which every Kafka 2.x-and-later release emits in the form:
#
#     Topic: blnk.transactions  TopicId: ...  PartitionCount: 6  ReplicationFactor: 3  Configs: ...
#
# Reading the replication factor here is what closes the durability gap. This script used to
# read only the partition count, so the configured factor was applied to topics it CREATED
# and never checked against topics that already existed - a topic created by hand, or
# created while the deployment was still a single broker, stayed at one replica on a
# replicated cluster indefinitely and every run reported success. event_admin.go's
# EnsureTopics now performs the same check from the broker's replica assignment, and the two
# must agree.
#
# --describe exits non-zero with a Java stack trace for a topic that does not exist, so the
# call is guarded and its output discarded; an unreadable answer is a legitimate outcome here
# and the caller decides what it means.
topic_geometry() {
    local topic="$1" output partitions="" replication=""
    output="$(kafka_topics --describe --topic "$topic" 2>/dev/null || true)"

    if [[ "$output" =~ PartitionCount:[[:space:]]*([0-9]+) ]]; then
        partitions="${BASH_REMATCH[1]}"
    fi
    if [[ "$output" =~ ReplicationFactor:[[:space:]]*([0-9]+) ]]; then
        replication="${BASH_REMATCH[1]}"
    fi

    if [[ -z "$partitions" ]]; then
        return 0
    fi

    printf '%s %s' "$partitions" "${replication:-0}"
}

# Report whether a topic exists, distinguishing "absent" from "could not tell".
#
# # Why this is not topic_geometry
#
# topic_geometry answers empty for BOTH an absent topic and one whose --describe momentarily
# returns nothing, and the create/grow/unchanged summary was derived from that answer. So a topic
# that existed but was unreadable for a few hundred milliseconds — a broker mid-election, a
# metadata refresh, a Describe the admin principal is not granted — was reported as CREATED BY
# THIS RUN. That is the one thing the summary exists to be trusted about: "created: 8" on a
# deployment that has been publishing for weeks is read as "the catalogue was lost and rebuilt,
# and every offset with it", which is an incident. Inferring it from an ambiguous silence made the
# line unusable as the audit trail the daily outbox-versus-offset reconciliation depends on.
#
# --list is used rather than --describe because it answers the question being asked. A topic that
# appears in the list exists, whatever its geometry, and the list either succeeds or fails as a
# whole — so a failure is reportable as a failure instead of looking like an empty catalogue.
# The retry is the same three-attempts-a-second pattern require_topic_geometry uses, for the same
# reason: a metadata refresh is not an answer.
#
# Arguments:
#   $1 - the topic name.
# Outputs:
#   "yes", "no", or "unknown" on stdout. "unknown" means the broker could not be asked, and every
#   caller must treat it as such rather than as either of the other two.
topic_presence() {
    local topic="$1" attempt output status

    for attempt in 1 2 3; do
        status=0
        output="$(kafka_topics --list 2>/dev/null)" || status=$?

        if ((status == 0)); then
            if printf '%s\n' "$output" | grep -Fxq -- "$topic"; then
                printf '%s' "yes"
            else
                printf '%s' "no"
            fi

            return 0
        fi

        if ((attempt < 3)); then
            sleep 1 || true
        fi
    done

    printf '%s' "unknown"
}

# Read a topic's geometry, retrying briefly, and fail if it cannot be read at all.
#
# An unreadable geometry used to be accepted: the topic was recorded as "unknown" and the
# run reported success, which meant a run could complete having verified nothing at all
# about the layout it exists to guarantee - and the summary still printed a replication
# factor beside it. That is precisely the outcome this script is supposed to make
# impossible, so it is now a failure.
#
# The retry is what makes failing safe rather than flaky: --describe legitimately answers
# nothing for a few hundred milliseconds after a create, while the metadata propagates. Three
# attempts a second apart distinguish "still settling" from "cannot be read".
require_topic_geometry() {
    local topic="$1" attempt geometry=""

    for attempt in 1 2 3; do
        geometry="$(topic_geometry "$topic")"
        if [[ -n "$geometry" ]]; then
            printf '%s' "$geometry"
            return 0
        fi
        if ((attempt < 3)); then
            sleep 1 || true
        fi
    done

    die "topic '${topic}' exists but its geometry could not be read after 3 attempts." \
        "The partition count and replication factor are what this script exists to" \
        "guarantee, so a run that cannot read them has verified nothing and must not report" \
        "success - it used to record the topic as 'unknown' and carry on." \
        "The two usual causes:" \
        "  1. $(admin_identity_label) lacks Describe authority on the topic. Add the" \
        "     principal to the broker's super.users, or grant Describe on the topic." \
        "  2. the broker is mid-election or its metadata has not settled. Re-run this" \
        "     script; it is idempotent, so nothing is duplicated." \
        "Nothing further has been provisioned."
}

# Create one topic if it is absent, then verify its geometry: reconcile the partition count
# upwards only, and REFUSE a replication factor below the requirement.
#
# Records the OBSERVED partition count and replication factor in SUMMARY_PARTITIONS and
# SUMMARY_REPLICATION so that the closing summary reports what is actually on the broker
# rather than what was requested.
#
# CLASSIFIES THE TOPIC INTO EXACTLY ONE OF created / grown / unchanged, in the local variable
# "state", and appends to SUMMARY_CREATED or SUMMARY_GROWN once, at the very end, only after
# the transition has been CONFIRMED against the broker. Three properties follow from that, and
# each of them was previously violated:
#
#   1. The two buckets are MUTUALLY EXCLUSIVE. They used to be independent appends, so a topic
#      that appeared during the run and was then grown landed in both - and the summary's
#      "unchanged" line, computed as total minus created minus grown, printed a NEGATIVE
#      number. When they collide, created wins: "it was not there and now it is" is the fact
#      that matters, and it is reported alone rather than alongside a growth of the topic that
#      same run had just brought into existence.
#   2. Growth is counted only when THIS RUN's alter succeeded. The append used to happen
#      BEFORE the alter was issued, so a run whose alter lost a race to a peer provisioner -
#      the compose one-shot against a manual "make kafka_provision" - reported a growth it did
#      not perform, and so did a run whose alter failed outright before the re-read decided
#      whether that mattered.
#   3. Creation is counted only when the topic was CONFIRMED ABSENT beforehand. See
#      topic_presence: an unreadable probe is "unknown" and is counted as nothing, because a
#      created count is this script's loudest signal and must never be manufactured by a
#      transient metadata read.
#
# event_admin.go's EnsureTopics reports the same three words from the same kind of evidence,
# probing partition counts into partitionsBefore ahead of its create pass, and the two outputs
# are meant to be readable against each other.
ensure_topic() {
    local topic="$1" target="$2"
    local output geometry current replication existed_before
    # Evidence collected as this function proceeds, and read ONCE at the tail to choose exactly
    # one outcome. Nothing is appended to a summary array before the evidence for it exists.
    local grew_confirmed="no" grown_elsewhere="no" growth_incomplete="no"
    # Declared here rather than inside the under-target branch, because the classification at
    # the tail reads it on every path and `set -u` makes an unset name fatal.
    local grow_permitted="yes" record_state=""

    # PROBED BEFORE THE CREATE, so this run can say what it actually DID. --if-not-exists makes
    # creation idempotent by making it indistinguishable - an existing topic and a freshly
    # created one both leave exit 0 - so without this probe the equal-partition arm below would
    # report every topic as "already correct", including one it had created seconds earlier.
    #
    # --if-not-exists makes creation idempotent by making it indistinguishable: an existing
    # topic and a freshly created one both leave exit 0 and both fall into the same
    # reconciliation below. Without this probe the equal-partition arm reported EVERY topic as
    # "already correct", including one it had just created seconds earlier - so a bring-up whose
    # catalogue had been lost and silently rebuilt produced output identical to one where
    # nothing had changed, and the run could not be used as evidence for the outbox-versus-
    # offset reconciliation in docs/kafka-operations.md.
    #
    # THE PROBE IS topic_presence, WHICH HAS THREE ANSWERS. It used to be `topic_geometry`,
    # which answers empty for an absent topic AND for one whose geometry is momentarily
    # unreadable, so an existing topic the broker could not describe for a moment was reported
    # as created by this run. "created" is the strongest claim this summary makes - it says the
    # catalogue was rebuilt and its offsets restarted - and it must not rest on an ambiguous
    # silence. An "unknown" answer is now carried through to its own outcome at the tail rather
    # than being resolved by guessing.
    #
    # event_admin.go's EnsureTopics resolves this the same way, probing partition counts into
    # partitionsBefore ahead of its create pass and reporting created/grown/unchanged from it.
    existed_before="$(topic_presence "$topic")"

    # --if-not-exists is Kafka's own creation idempotency: an existing topic is a no-op with
    # exit 0, whatever its partition count, so the reconciliation below is reached either way.
    #
    # Output is captured and shown only on failure. On success it carries nothing but Kafka's
    # standing advisory that topic names mixing '.' and '_' can collide in metric names -
    # which every name here triggers, by design, since the catalogue is dot-separated and
    # event_topics.go composes it that way. One copy of that warning per topic would bury
    # the lines that matter.
    if ! output="$(kafka_topics --create --if-not-exists --topic "$topic" \
        --partitions "$target" --replication-factor "$KAFKA_REPLICATION_FACTOR" 2>&1)"; then
        printf '%s\n' "$output" | redact >&2
        die "could not create topic '${topic}'." \
            "The broker's own output is above. The two usual causes:" \
            "  1. KAFKA_REPLICATION_FACTOR is ${KAFKA_REPLICATION_FACTOR} but the cluster has" \
            "     fewer brokers than that, which Kafka refuses with" \
            "     INVALID_REPLICATION_FACTOR. A single-broker local stack needs 1; only a" \
            "     multi-broker cluster can satisfy 3." \
            "  2. $(admin_identity_label) lacks Create authority on the cluster. Add the" \
            "     principal to the broker's super.users, or grant Create on the cluster" \
            "     resource."
    fi

    geometry="$(require_topic_geometry "$topic")"
    current="${geometry%% *}"
    replication="${geometry##* }"

    if ((10#$current < 10#$target)); then
        # THE GROWTH GATE. Raising a partition count re-maps existing keys, and the key here is
        # the ledger id that carries the per-aggregate ordering guarantee, so an under-partitioned
        # topic that already holds records is left alone unless an operator has consented.
        #
        # Reported and skipped rather than fatal: an under-partitioned topic is a throughput
        # limit, not an outage, and failing a bring-up over one would be the worse outcome.
        record_state="$(topic_record_state "$topic")"

        if [[ "$record_state" != "empty" ]] && ! is_truthy "$KAFKA_ALLOW_PARTITION_GROWTH"; then
            grow_permitted="no"
            case "$record_state" in
                records)
                    warn "'${topic}' has ${current} partitions, fewer than the ${target} configured," \
                        "and it HOLDS RECORDS, so it was left alone." \
                        "Growing it would re-map keys already written to different partitions:" \
                        "Blnk keys each event by its ledger id, so events for one ledger would be" \
                        "split across two partitions and a consumer could observe them out of" \
                        "order, with nothing failing to say so." \
                        "To grow it anyway, set KAFKA_ALLOW_PARTITION_GROWTH=true and accept that" \
                        "the ordering guarantee does not hold for keys already in flight." \
                        "To keep ${current} deliberately, set KAFKA_MIN_PARTITIONS=${current}."
                    ;;
                *)
                    warn "'${topic}' has ${current} partitions, fewer than the ${target} configured," \
                        "and whether it holds records COULD NOT BE DETERMINED, so it was left" \
                        "alone rather than grown on an assumption." \
                        "$(offsets_unavailable_reason)" \
                        "To grow it anyway, set KAFKA_ALLOW_PARTITION_GROWTH=true."
                    ;;
            esac
        fi

        # Deliberately a SKIP of the alter rather than an early return: the replication check
        # and the summary entry at the tail of this function must still run, so a topic left
        # under-partitioned is still verified and still reported with its real geometry.
        if [[ "$grow_permitted" == "no" ]]; then
            # Classified at the tail with every other disposition, so nothing is appended to a
            # summary array from two places.
            :
        elif [[ "$record_state" != "empty" ]]; then
            warn "growing '${topic}' from ${current} to ${target} partitions even though it is not" \
                "known to be empty, because KAFKA_ALLOW_PARTITION_GROWTH is set." \
                "Keys already written will re-map to different partitions, so the per-aggregate" \
                "ordering of existing events is not preserved. This matches what" \
                "event_admin.go's partitionGrowthDecision does with the same setting."
        fi

        if [[ "$grow_permitted" == "yes" ]]; then
            # NOT RECORDED AS GROWN HERE. The summary used to append the topic at this point,
            # before the alter had been attempted, so it stayed counted as grown by this run even
            # when the alter FAILED and the re-read showed that another provisioner had grown it -
            # the race the block below exists to tolerate. What follows sets the evidence flags
            # instead, and the tail of this function turns them into exactly one outcome.
            log "growing '${topic}' from ${current} to ${target} partitions"
            if ! output="$(kafka_topics --alter --topic "$topic" --partitions "$target" 2>&1)"; then
                # Re-read before deciding this is a failure. Two provisioners racing each other -
                # the compose one-shot and a manual "make kafka_provision" - both try to grow, and
                # the loser is told the topic already has that many partitions. The topic is
                # correct, so that is success, not an error.
                geometry="$(require_topic_geometry "$topic")"
                current="${geometry%% *}"
                replication="${geometry##* }"
                if ((10#$current >= 10#$target)); then
                    grown_elsewhere="yes"
                    log "'${topic}' already has ${current} partitions; another provisioner grew it"
                else
                    printf '%s\n' "$output" | redact >&2
                    die "could not grow topic '${topic}' to ${target} partitions." \
                        "The broker's own output is above. Kafka increases a partition count with" \
                        "an alter and cannot decrease one, so this is a genuine failure rather" \
                        "than a no-op." \
                        "Fix: give $(admin_identity_label) Alter authority on the topic - add the" \
                        "principal to the broker's super.users, or grant Alter on the topic" \
                        "resource. If the existing partition count is deliberate, set" \
                        "KAFKA_MIN_PARTITIONS to it instead so no alter is attempted."
                fi
            else
                # Re-read rather than assuming the target was reached, so the summary reports
                # what the broker has and not what was asked for. The re-read is also what
                # CONFIRMS the growth: an alter that exits 0 is the broker accepting a request,
                # and this line is the broker agreeing it took effect.
                geometry="$(require_topic_geometry "$topic")"
                current="${geometry%% *}"
                replication="${geometry##* }"
                if ((10#$current >= 10#$target)); then
                    grew_confirmed="yes"
                else
                    # Accepted and yet not there. Neither "grown" nor "unchanged" is true of this
                    # topic, so it is neither: it is reported as indeterminate and the geometry
                    # table below still shows the count the broker actually has.
                    growth_incomplete="yes"
                    warn "'${topic}' still reports ${current} partitions after an alter to" \
                        "${target} was accepted, so this run cannot claim to have grown it." \
                        "The geometry below is the broker's own. Re-run this script to converge;" \
                        "it is idempotent, and a partition count only ever increases."
                fi
            fi
        fi
    elif ((10#$current == 10#$target)); then
        case "$existed_before" in
            no)
                log "created '${topic}' with ${current} partitions"
                ;;
            yes)
                log "'${topic}' exists with ${current} partitions, already correct"
                ;;
            *)
                log "'${topic}' has ${current} partitions, already correct; whether it existed" \
                    "before this run could not be determined, so it is reported as unchanged"
                ;;
        esac
    else
        # More partitions than configured. Left alone, reported, and NOT an error - matching
        # event_admin.go, which counts this as a refused shrink rather than a failure.
        #
        # Kafka cannot reduce a partition count at all, so attempting it can only fail. The
        # deeper reason not to want to is that per-aggregate ordering depends on a stable
        # key-to-partition mapping: the partition a ledger ID hashes to is a function of the
        # partition count, so reducing it would start landing an aggregate's events on a
        # different partition from their predecessors.
        warn "'${topic}' has ${current} partitions, more than the configured ${target}." \
            "Left exactly as it is. Kafka cannot reduce a partition count, and reducing one" \
            "would move keys between partitions and break the per-aggregate ordering guarantee" \
            "that keying by ledger ID exists to provide." \
            "If ${target} is what you meant, raise KAFKA_MIN_PARTITIONS to ${current} to make" \
            "configuration and reality agree; the extra partitions are harmless."
    fi

    require_topic_replication "$topic" "$replication"

    # ONE OUTCOME PER TOPIC, FROM THE EVIDENCE GATHERED ABOVE.
    #
    # The four classes are mutually exclusive and every topic lands in exactly one, which is what
    # makes the counts add up to the catalogue size. They previously did not: "created" and
    # "grown" were appended independently, so a topic that this run created AND then grew was
    # counted twice and the unchanged figure - computed by SUBTRACTING both from the total - came
    # out negative. Nothing here subtracts; each count is the length of its own array.
    #
    #   created            absent before this run, and present after it. The strongest claim,
    #                      because it says the catalogue was rebuilt and its offsets restarted
    #                      from zero.
    #   grown              present before, below the target, and THIS RUN's own alter was
    #                      confirmed to have taken effect. An alter another provisioner won
    #                      leaves the catalogue in the same state and is NOT this: the summary
    #                      is what an operator reads to learn what this invocation did, and a
    #                      run that claims an alter it did not issue makes the audit trail
    #                      unusable for the one question it answers. Such a topic is `raced`,
    #                      which names the uncertainty instead of resolving it, and the
    #                      per-topic line above says "another provisioner grew it".
    #   under-partitioned  below the target and deliberately left there: it holds records, or
    #                      could not be proven empty, and growth was not consented to. ITS OWN
    #                      class, not a kind of `unchanged` - "unchanged" says the catalogue
    #                      needed nothing, this says it needs a decision, and folded together
    #                      they print "created=0 grown=0 unchanged=8" for a catalogue stuck at 3
    #                      partitions against a configured 6.
    #   raced              the truth is not this run's to report: the pre-state could not be
    #                      read, an accepted alter has not converged, or this run created the
    #                      topic and then found it with fewer partitions than it asked for,
    #                      which can only mean another provisioner created it first. Named
    #                      rather than folded into one of the others, because a summary that
    #                      guesses is worse than one that abstains.
    #   unchanged          present before, at or above the target, with a geometry this run did
    #                      not change.
    if [[ "$existed_before" == "unknown" || "$growth_incomplete" == "yes" || "$grown_elsewhere" == "yes" ]]; then
        SUMMARY_RACED+=("$topic")
    elif [[ "$existed_before" == "no" ]]; then
        if [[ "$grew_confirmed" == "yes" ]]; then
            SUMMARY_RACED+=("$topic")
        else
            SUMMARY_CREATED+=("$topic")
        fi
    elif [[ "$grew_confirmed" == "yes" ]]; then
        SUMMARY_GROWN+=("$topic")
    elif [[ "$grow_permitted" == "no" ]]; then
        SUMMARY_GROWTH_REFUSED+=("$topic")
    else
        SUMMARY_UNCHANGED+=("$topic")
    fi

    SUMMARY_TOPICS+=("$topic")
    SUMMARY_PARTITIONS+=("$current")
    SUMMARY_REPLICATION+=("$replication")
}

# Refuse a topic whose OBSERVED replication factor is below the configured requirement.
#
# Fatal, and deliberately so. An under-replicated topic works: it accepts messages, serves
# consumers and reports nothing wrong, right up to the moment a broker is lost, at which
# point the ledger events on it are unavailable or gone. There is no earlier symptom to
# alert on, so refusing here is the only point at which the defect is visible at all.
#
# The reassignment is NOT performed. Raising a replication factor is a partition
# reassignment, which copies every partition's whole log from its leader - a bulk data
# movement that needs throttling and a maintenance window, which is why Kafka exposes it
# through kafka-reassign-partitions rather than through an alter. Doing it unasked from a
# provisioning script would be a genuinely dangerous surprise. event_admin.go's EnsureTopics
# reaches the same verdict from the broker's replica assignment and gives the same guidance.
require_topic_replication() {
    local topic="$1" observed="$2"

    if ((10#$observed >= 10#$KAFKA_REPLICATION_FACTOR)); then
        return 0
    fi

    die "topic '${topic}' has a replication factor of ${observed}, below the configured KAFKA_REPLICATION_FACTOR of ${KAFKA_REPLICATION_FACTOR}." \
        "Events on its partitions would be lost if a broker were lost. This is checked" \
        "because the configured factor only ever applied to topics this script CREATED - an" \
        "existing topic's real factor was never looked at, so a topic created by hand, or" \
        "created while this was still a single-broker stack, could sit at one replica on a" \
        "replicated cluster indefinitely with every run reporting success." \
        "It is reported rather than fixed here: raising a replication factor is a partition" \
        "reassignment that copies every partition's log, so it needs throttling and a" \
        "maintenance window." \
        "Fix, whichever is true:" \
        "  1. the topic should be more durable: raise it with kafka-reassign-partitions," \
        "     then re-run this script to confirm." \
        "  2. the cluster genuinely has fewer brokers than the configured factor: set" \
        "     KAFKA_REPLICATION_FACTOR to the broker count - 1 for the single-broker local" \
        "     stack - so configuration and reality agree."
}

# Assure every topic in the catalogue, category topics first and then their dead-letter
# siblings. The count follows EVENT_CATEGORIES rather than being written down, so adding a
# category does not leave a stale number behind.
ensure_topics() {
    local topic

    log "assuring ${#ALL_TOPICS[@]} topics" \
        "partitions         : ${TARGET_PARTITIONS}" \
        "replication factor : ${KAFKA_REPLICATION_FACTOR} (verified per topic, not assumed)"

    SUMMARY_TOPICS=()
    SUMMARY_PARTITIONS=()
    SUMMARY_REPLICATION=()
    SUMMARY_CREATED=()
    SUMMARY_GROWN=()
    SUMMARY_UNCHANGED=()
    SUMMARY_RACED=()
    SUMMARY_GROWTH_REFUSED=()
    for topic in "${ALL_TOPICS[@]}"; do
        ensure_topic "$topic" "$TARGET_PARTITIONS"
    done

    # Counts, not just a total, so the line records what this run CHANGED. "created=0 grown=0
    # unchanged=8" is a catalogue that was already right; "created=8" on a deployment that has
    # been publishing for weeks says the catalogue was lost and rebuilt, and every offset with
    # it. The same words in the same order as event_admin.go's assurance log.
    #
    # EVERY COUNT IS AN ARRAY LENGTH. The unchanged figure used to be the total minus the other
    # two, which made it a function of their correctness rather than an observation, and let it
    # print a negative number when a topic was counted in both. Each topic is now classified once
    # in ensure_topic, so the four lengths sum to the catalogue size by construction.
    #
    # The raced line is printed only when it is non-empty, because an empty class every run is a
    # line that stops being read - and this one has to be noticed on the run where it appears.
    local raced_line="" refused_line=""
    if ((${#SUMMARY_RACED[@]} > 0)); then
        raced_line="raced     : ${#SUMMARY_RACED[@]} (${SUMMARY_RACED[*]}) - another provisioner acted, or the broker could not be asked; re-run to confirm"
    fi

    # The deliberate refusals, which the array beside SUMMARY_TOPICS was declared to report and
    # nothing ever printed. A topic left under-partitioned because it holds records is a standing
    # throughput limit that only a warning mid-run mentioned, and a warning several screens above
    # a success line is one nobody acts on. It is its OWN disposition rather than a kind of
    # `unchanged`, so this line and the count agree with each other and with the four figures
    # above summing to the catalogue.
    if ((${#SUMMARY_GROWTH_REFUSED[@]} > 0)); then
        refused_line="under-partitioned : ${#SUMMARY_GROWTH_REFUSED[@]} (${SUMMARY_GROWTH_REFUSED[*]}) - holding records, or not provably empty, and growth was not consented to; see the warnings above"
    fi

    ok "all ${#SUMMARY_TOPICS[@]} topics are present with a verified geometry" \
        "created   : ${#SUMMARY_CREATED[@]}${SUMMARY_CREATED[*]:+ (${SUMMARY_CREATED[*]})}" \
        "grown     : ${#SUMMARY_GROWN[@]}${SUMMARY_GROWN[*]:+ (${SUMMARY_GROWN[*]})}" \
        "unchanged : ${#SUMMARY_UNCHANGED[@]}" \
        ${raced_line:+"$raced_line"} \
        ${refused_line:+"$refused_line"}
}

# ---------------------------------------------------------------------------------------
# Step two: the steady-state producer principal
#
# The identity the server publishes ledger events as. Without it the publisher
# authenticates as the cluster administrator, which is the excess privilege described at
# KAFKA_SASL_USER near the top of this script.
#
# Its grant is Write and Describe on every topic Blnk owns, and NOTHING ELSE - see
# grant_producer_acls for why the dead-letter siblings are included and why Read is not.
# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# Step three: the sample subscriber principal
#
# One SCRAM-SHA-512 credential, so that a developer can point a consumer at the local stack
# without first learning how to mint a principal. Production subscribers are provisioned by
# event_admin.go's ProvisionSubscriberPrincipal through AlterUserScramCredentials, in
# process, and never come through here.
# ---------------------------------------------------------------------------------------

# Generate a password, and return it in GENERATED_PASSWORD rather than on stdout.
#
# THE RETURN CHANNEL IS THE POINT, not an eccentricity. This used to end in
# "printf '%s' \"$password\"" and be called through a command substitution, which is the
# ordinary bash idiom - and it was the one construct in this file that made the rule "no line
# of this script writes a credential to output" impossible to state, let alone verify. A
# reviewer scanning for a leak had to recognise that one printf as a value return and every
# other as suspect, and an automated check could only be written with a carve-out for exactly
# the line a real leak would hide behind. Returning through a named variable removes the
# exception, so the rule is absolute and mechanically checkable.
#
# openssl is the primary source, with a /dev/urandom fallback for an image without it. Both
# outputs are filtered to alphanumerics, which is not cosmetic: kafka-configs parses
# --add-config as a comma-separated list with '[' and ']' delimiting a value, so a password
# containing any of those characters would be misparsed rather than rejected. Restricting the
# alphabet makes that impossible instead of merely unlikely, and it stays inside the
# printable-ASCII range where SASLprep normalisation is a no-op - the same restriction
# event_admin.go's validateSCRAMPassword enforces on the Go side.
generate_password() {
    # The role this password is for, used only to make the failure message name the two
    # variables that resolve it. Defaulted so existing call sites read unchanged.
    local role="${1:-sample subscriber}"
    local supply_variable="${2:-KAFKA_SAMPLE_SUBSCRIBER_SECRET}"
    local skip_variable="${3:-KAFKA_SKIP_SAMPLE_SUBSCRIBER}"
    local password=""

    GENERATED_PASSWORD=""

    if command -v openssl >/dev/null 2>&1; then
        password="$(openssl rand -base64 64 2>/dev/null | LC_ALL=C tr -dc 'A-Za-z0-9' | head -c "$MIN_SECRET_LENGTH" || true)"
    fi

    if [[ ${#password} -lt MIN_SECRET_LENGTH && -r /dev/urandom ]]; then
        # head closes the pipe, so tr is killed by SIGPIPE and the pipeline reports failure
        # under "set -o pipefail". Guarded, because that failure is the expected outcome.
        password="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c "$MIN_SECRET_LENGTH" || true)"
    fi

    # Short output means the filter ate almost everything, which means the source produced
    # almost nothing. Refusing beats provisioning a guessable credential.
    if ((${#password} < 16)); then
        die "could not generate a password for the ${role} principal." \
            "Tried 'openssl rand' and then /dev/urandom; neither yielded enough entropy." \
            "Fix: set ${supply_variable} to a password of your own, or set" \
            "${skip_variable}=1 to skip that principal entirely."
    fi

    GENERATED_PASSWORD="$password"
}

# stage_secret_atomically is RETIRED. Its properties live in the stage/commit PAIR below.
#
# SEC-15 was answered twice. This was the single-shot form — stage beside the destination, write,
# rename, all in one call — and stage_generated_secret + commit_staged_secret is the two-phase
# form, which stages BEFORE the broker is altered and commits only once the broker has accepted
# the credential. The two-phase form is the one every call site uses, and it is strictly the
# better shape: the single-shot form publishes a secret file for a credential that may still fail
# to be created, leaving an operator holding a password the broker never accepted.
#
# Every property this function carried is kept, and each is stated where it now lives:
#
#   * REFUSE A SYMLINK AT THE DESTINATION (CWE-59), before anything is created — moved verbatim
#     into stage_generated_secret. `>"$destination"` and `chmod 600 "$destination"` both resolve a
#     link, so a link pre-placed in a shared bind mount received the credential and the 0600
#     landed on the LINK TARGET rather than on the path this script chose. A dangling link is the
#     worse case: it is a request to create a file wherever it points.
#   * CREATE EXCLUSIVELY, BESIDE THE DESTINATION — stage_generated_secret, under `umask 077` and
#     `set -o noclobber`. Beside it rather than in TMPDIR, because rename(2) is atomic only within
#     one filesystem and TMPDIR is a different one in every container this runs in.
#   * ESTABLISH THE MODE BEFORE ANY BYTE OF THE CREDENTIAL EXISTS, and pass the value by
#     redirection so it reaches no log and no process-table entry — stage_generated_secret.
#   * RENAME OVER THE DESTINATION (CWE-367) — commit_staged_secret. rename(2) replaces the path in
#     one step and does not write through a symlink at it, so a concurrent reader sees either the
#     whole previous credential or the whole new one, never nothing and never half.
#   * CLEAN UP ON EVERY FAILURE — the EXIT trap, which the staged path is registered with BEFORE
#     it is written. That covers a signal part-way through, which an explicit `rm -f` per failure
#     arm does not. The single exception is a commit that fails AFTER the broker accepted the
#     credential: there the staged file holds the only copy of a live password, so it is taken
#     back off the trap's list deliberately and the operator is told where it is.
# Write a generated credential to a permissioned file, and report only the PATH.
#
# This is the "deliberately permissioned mechanism" that replaced printing to stdout. Three
# properties make it one rather than a rename of the same disclosure, and the pair below
# delivers all three between them:
#
#   1. THE MODE IS SET BEFORE THE CONTENT EXISTS, on a file created with O_EXCL, so there is
#      neither an instant in which the credential sits at a wider mode nor a symlink through
#      which the mode could apply to some other file.
#   2. NOTHING IS APPENDED, AND NOTHING IS HALF-WRITTEN. The destination is replaced by an
#      atomic rename, so a rotation is one visible transition from the whole old value to the
#      whole new one rather than a window of empty or partial content.
#   3. THE VALUE NEVER REACHES A LOG. Only the path is reported, and the write goes through a
#      redirection rather than a command argument, so the credential appears in no process
#      table entry either.
#
# Parameters:
#   $1 destination : path to write. Must be non-empty; the caller decides that.
#   $2 credential  : the value to write.
#   $3 label       : what the credential is for, used in diagnostics only.
#
# Either half dies when it cannot do its part, because a generated credential that was not
# delivered anywhere is a principal nobody can authenticate as.
#
# ---------------------------------------------------------------------------
#
# Delivering a generated credential is TWO steps, and they sit either side of the broker
# mutation. stage_generated_secret runs BEFORE it; commit_staged_secret runs after it
# succeeds. Nothing calls them in one breath, and that separation is the whole point.
#
# # Why staging has to precede the broker call
#
# A single "alter the broker, then write the file" function has an unrecoverable failure
# mode: the credential is live at the broker and the only copy of the password is gone.
# Nothing can retrieve it afterwards, because SCRAM stores a salted verifier and not the
# password, so the broker cannot be asked what it now accepts. The subscriber is locked
# out, and the fix is another rotation - which is exactly the operation an operator was
# trying to avoid performing blind. Every reason a write fails is knowable in advance: a
# missing directory, a read-only mount, a wrong owner inside a container, no space. So
# they are established first, on a real file with real permissions, and the broker is
# only touched once delivery is known to work.
#
# # Why a staged file rather than writing the destination directly
#
# Writing the destination in place has a second, quieter defect. '>' TRUNCATES an
# existing file and KEEPS ITS MODE - umask applies only to files being created - so a
# destination that already existed at 0644 receives the password and stays world-readable
# for the window before any chmod lands. A staged path is created fresh with noclobber,
# so it cannot be a pre-existing file with a mode somebody else chose, and 0600 is
# established BEFORE a single byte of credential is written to it.
#
# The staged file is registered with GENERATED_SECRET_FILES before it is written, so
# every abort path - a die anywhere below, Ctrl-C, SIGTERM - removes it through the EXIT
# trap. There is no cleanup to remember at the call sites and none to forget.
#
# Parameters:
#   $1 destination - the final path the operator nominated.
#   $2 credential  - the generated password.
#   $3 label       - what the credential is for, used in messages.
#
# Sets STAGED_SECRET_PATH to the staged path, for commit_staged_secret to consume. It does
# NOT print it: see that variable's declaration for why a printed value would break cleanup.
stage_generated_secret() {
    local destination="$1" credential="$2" label="$3"
    local directory="" staged="" previous_umask=""

    # The directory check is stated as its own diagnostic rather than being left to the
    # creation failure below, because this is the case an operator actually hits: the
    # *_SECRET_FILE variable named a path whose parent is not mounted, and the fix is about the
    # mount rather than about the write.
    # REFUSED FIRST, BEFORE A TEMPORARY FILE EXISTS, so a refusal leaves nothing behind. -L is
    # true for a symlink whether or not its target exists, which is what makes it the right test:
    # a DANGLING link is the more dangerous of the two, because it is a request to create a file
    # wherever it points. The commit below is a rename and would not write THROUGH a link, but a
    # link at this path is either an operator indirection this would silently destroy or someone
    # else's doing in a shared bind mount — and delivering a password into a directory somebody
    # else is manipulating is precisely what must not happen. Refusing says so instead of
    # guessing which it was.
    if [[ -L "$destination" ]]; then
        die "refusing to deliver the generated ${label} credential: '${destination}' is a symbolic link." \
            "A credential is never written through a link. In a shared directory a link at this" \
            "path is either an indirection this would destroy or someone else's doing, and" \
            "neither is safe to write a password into." \
            "Fix: remove the link and let this script create the file, or point the" \
            "*_SECRET_FILE variable at a real path." \
            "THE BROKER HAS NOT BEEN TOUCHED: staging runs before the credential is created, so" \
            "nothing needs undoing and re-running after fixing the path is safe."
    fi

    directory="$(dirname -- "$destination")"
    if [[ ! -d "$directory" ]]; then
        die "cannot deliver the generated ${label} credential: '${directory}' is not a directory." \
            "Destination requested: ${destination}" \
            "Fix: mount a writable directory at that path, point the *_SECRET_FILE variable" \
            "somewhere that exists, or supply the credential explicitly instead of having one" \
            "generated." \
            "THE BROKER HAS NOT BEEN TOUCHED: no credential was created, so nothing needs" \
            "undoing and re-running after fixing the path is safe."
    fi

    # Beside the destination, never in TMPDIR. A rename is atomic only within one
    # filesystem, and TMPDIR is routinely a different one - under compose it is very often
    # a tmpfs. Staging elsewhere would turn the commit into a copy, which reintroduces
    # precisely the partially-written, wrongly-moded destination that staging exists to
    # prevent.
    staged="${destination}.blnk-staged.$$"

    previous_umask="$(umask)"
    umask 077

    # noclobber, so a path that already exists is an error rather than something to
    # truncate. That is what guarantees the mode below is one this function established.
    if ! (set -o noclobber && : >"$staged") 2>/dev/null; then
        umask "$previous_umask"
        die "cannot deliver the generated ${label} credential: '${staged}' could not be created." \
            "Fix: check that '${directory}' is writable by the user this script runs as." \
            "Inside a container that usually means the mount is read-only or owned by" \
            "another user. If a file of that name is left over from an interrupted run," \
            "remove it." \
            "THE BROKER HAS NOT BEEN TOUCHED: no credential was created."
    fi

    umask "$previous_umask"

    # Registered before anything is written to it, so an abort between here and the commit
    # cannot leave a credential behind on disk.
    GENERATED_SECRET_FILES+=("$staged")

    # Belt and braces over the umask, and FATAL rather than ignored. A credential must not
    # be written to a file whose permissions could not be established - that was the
    # original defect, where the chmod was 'chmod 600 ... || true' and a failure to secure
    # the file simply proceeded to fill it with a password.
    if ! chmod 600 "$staged" 2>/dev/null; then
        die "cannot deliver the generated ${label} credential: the mode of '${staged}' could not be set to 0600." \
            "Refusing to write a credential to a file whose permissions are unknown." \
            "THE BROKER HAS NOT BEEN TOUCHED: no credential was created."
    fi

    if ! printf '%s\n' "$credential" >"$staged" 2>/dev/null; then
        die "cannot deliver the generated ${label} credential: writing '${staged}' failed." \
            "Fix: check the free space and the permissions on '${directory}'." \
            "THE BROKER HAS NOT BEEN TOUCHED: no credential was created."
    fi

    STAGED_SECRET_PATH="$staged"
}

# retain_secret_file takes a path back off the EXIT trap's removal list.
#
# It exists for exactly one situation, and it is a situation where the trap's usual
# instinct is wrong. The trap deletes scratch credential files because leaving a password
# on disk is a disclosure. But if a credential is already LIVE at the broker and the
# staged file holds the only copy of its password, deleting that file does not prevent a
# disclosure - it destroys the sole means of using an account that now exists, and SCRAM
# stores a salted verifier rather than the password, so nothing can recover it afterwards.
# Between "a 0600 file the operator has just been told about" and "an unusable live
# credential", the file is plainly the lesser harm.
#
# Parameters:
#   $1 - the path to stop tracking.
retain_secret_file() {
    local keep="$1" remaining=() file=""

    for file in ${GENERATED_SECRET_FILES[@]+"${GENERATED_SECRET_FILES[@]}"}; do
        if [[ "$file" != "$keep" ]]; then
            remaining+=("$file")
        fi
    done

    GENERATED_SECRET_FILES=(${remaining[@]+"${remaining[@]}"})
}

# commit_staged_secret moves a staged credential into place once the broker has accepted
# it.
#
# The rename is what makes the destination's contents and its mode change together. A
# reader either sees the previous credential or the new one, never a half-written file,
# and never the new password inside a file still carrying an old wide mode: rename
# replaces the destination inode outright, so the 0600 established during staging is the
# mode the destination ends up with, whatever it was before.
#
# Parameters:
#   $1 staged      - the path returned by stage_generated_secret.
#   $2 destination - the final path.
#   $3 label       - what the credential is for, used in messages.
commit_staged_secret() {
    local staged="$1" destination="$2" label="$3"

    if ! mv -f -- "$staged" "$destination" 2>/dev/null; then
        # Take it off the trap's list FIRST. Otherwise the die below exits, the trap runs,
        # and the one copy of a live credential's password is removed by the very cleanup
        # meant to protect it - making the message that follows a lie.
        retain_secret_file "$staged"

        die "the ${label} credential was created at the broker but could not be moved into place." \
            "Staged file: ${staged}" \
            "Destination: ${destination}" \
            "THE CREDENTIAL IS LIVE AT THE BROKER. The password is in the staged file above," \
            "at mode 0600 - move it into place yourself, and delete it once you have. The" \
            "staged file is NOT removed on this path precisely because it holds the only copy;" \
            "a SCRAM verifier cannot be read back, so losing it means rotating again."
    fi

    log "wrote the generated ${label} credential to a mode-0600 file" \
        "path: ${destination}" \
        "The value is NOT printed here and is not in this run's output: read it from that" \
        "file. It is replaced wholesale on a rotation, never appended to."
}

# Deliver a generated credential to the file an operator named, and nowhere else.
#
# THE ORDER OF OPERATIONS IS THE SECURITY PROPERTY. The file is created empty under a 0077
# umask and then chmod 600 before a single byte of the password is written to it, so there is
# no instant at which the credential exists in a file that a wider mode would have let
# another user read. Creating it and then tightening the mode afterwards leaves exactly that
# window open, which is CWE-732 and is the shape of the defect this whole change closes; it
# is not a theoretical window either, because the compose bind mount this normally writes
# through is a host directory shared with every other process on the host.
#
# Truncated rather than appended, so a rotation replaces the previous value instead of
# leaving a file that holds both and gives no indication which one the broker has.
#
# Nothing about the value is logged, here or by the caller: the path is reported, the
# password is not. The variable is passed as an argument rather than read from the
# environment so that this function has exactly one input and cannot pick up a stale one.
#
# Parameters:
#   $1 - path to write. Its parent directory must already exist and be writable.
#   $2 - a short label naming what the credential is for, used only in messages.
#   $3 - the credential. Never logged.

# Report whether a principal already holds a SCRAM-SHA-512 credential.
#
# Named for the CREDENTIAL rather than for the subscriber because both principals this script
# provisions - the sample subscriber and the steady-state producer - ask the same question of
# the same broker in the same way.
#
# This is the probe that makes preservation possible: without it the only options are to
# upsert blindly on every run, or to skip provisioning even when there is nothing to keep.
#
# "kafka-configs --describe --entity-type users --entity-name <user>" prints one line per
# configured credential, naming the mechanism, and prints nothing for a principal with no
# credentials. Matching on the mechanism name rather than on the presence of any output is
# what keeps a principal that holds only a SHA-256 credential from being mistaken for one
# that can authenticate here - Blnk standardises on SHA-512 and the broker needs exactly
# that one enabled.
#
# The describe is guarded, and A FAILURE IS ITS OWN ANSWER. "Cannot tell" is returned as
# SCRAM_PROBE_UNKNOWN and is NOT folded into "absent", because the two license different
# actions: absence licenses minting a credential, and silence licenses nothing.
#
# That fold was a real defect with a concrete cost. A transient describe failure - a broker
# mid-restart, a momentary timeout, a client configuration briefly missing its administrative
# credentials - was recorded as absence, so the disposition chain skipped its `preserved` arm
# and fell into generate-and-upsert. The broker then replaced a WORKING credential with a newly
# generated password, under no rotation flag, and every consumer and every publishing process
# authenticating with the old one began failing immediately - while this script printed a
# successful provisioning run. The operator had asked only for topics to be assured.
#
# IT IS A PREDICATE, AND IT HAS TO BE ONE, because both call sites use it as the condition
# of an `elif`. It printed its answer to STDOUT as "exists" / "absent" / "unknown" and
# returned 0 unconditionally, so every caller read "yes" whatever the broker said, and the
# word itself was emitted into the operator's console mid-sentence. The consequences differed
# by caller and both were bad: the producer branch took its `preserved` short-circuit and the
# run reported SUCCESS with no credential on the broker at all - a green bring-up followed by
# a SASL failure the server could not explain - while the subscriber branch fell
# through to the credential upsert with an EMPTY password, which the broker refuses, failing
# a run that had nothing wrong with it. It also made two of the subscriber branches
# unreachable, so the documented one-time secret-file delivery never happened on a first run.
#
# So: no stdout, one return code, and ONE round-trip to the broker (the original issued the
# describe twice and discarded the first answer). `status` is declared local, because as a
# global it leaked this probe's exit status into every later caller of it.
#
# Returns 0 when the credential exists, 1 when it does not or cannot be determined.
scram_credential_is_present() {
    local user="$1"

    # `|| true` IS LOAD-BEARING, not defensive noise. The probe reports absent and unknown as
    # non-zero exit codes, and this script runs under `set -e`: a bare call would take the
    # whole run down on the single most ordinary outcome there is — a fresh broker with no
    # credential yet — before this predicate could answer anything.
    scram_credential_state "$user" || true

    [[ "$SCRAM_CREDENTIAL_STATE" == "present" ]]
}

# SCRAM_CREDENTIAL_STATE is scram_credential_state's answer: present, absent or unknown.
#
# It is a global rather than stdout because the probe writes the broker's own diagnostic to
# stderr, and mixing a machine-readable verdict into a stream a human also reads is how the
# verdict ends up being parsed out of a log line.
SCRAM_CREDENTIAL_STATE="unknown"

# scram_credential_state answers whether a principal holds a ${SCRAM_MECHANISM} credential
# with THREE outcomes rather than two: present, absent, or unknown.
#
# # Why the third state has to exist
#
# The probe is a "kafka-configs --describe" against the broker, and that call can fail for
# reasons that say NOTHING about the credential: the broker is unreachable, the admin
# principal is not authorised to describe user configs, the CLI is missing, a TLS handshake
# fails. A two-valued probe has to fold every one of those onto a boolean, and the previous
# implementation folded them onto ABSENT.
#
# Absent is not the safe direction here, and calling it safe was the defect. Downstream, the
# "credential is absent" branch is what GENERATES a new password and upserts it whenever a
# secret-file destination is configured. So an unreachable broker or an unauthorised admin
# silently ROTATED a working producer or subscriber credential — no KAFKA_ROTATE_* asked for
# it, every consumer already holding the old password stopped authenticating, and the run
# reported success. The failure is invisible precisely because the upsert succeeds: the
# broker accepts the new credential and nothing anywhere compares it with the old one.
#
# So an indeterminate probe now says so, and every caller must decide what to do about it
# rather than being handed a wrong fact. The one thing no caller may do is write a
# credential: see require_determinate_credential_state.
#
# Arguments:
#   $1 - the principal to probe.
#
# Sets:
#   SCRAM_CREDENTIAL_STATE - present | absent | unknown.
scram_credential_state() {
    local user="$1" output status

    SCRAM_CREDENTIAL_STATE="unknown"

    # stderr is folded into the capture rather than discarded, so a probe that failed for an
    # administrative reason can be reported to the operator in the broker's own words, with
    # credential-bearing lines removed, instead of vanishing into a bare "no".
    output="$(kafka_configs --describe --entity-type users --entity-name "$user" 2>&1)" && status=0 || status=$?

    if ((status != 0)); then
        printf '%s\n' "$output" | redact >&2
        warn "could not determine whether '${user}' already holds a ${SCRAM_MECHANISM} credential" \
            "The broker's own answer is above, with credential-bearing lines removed." \
            "This run treats the state as UNKNOWN and will NOT write a credential for this" \
            "principal, because the alternative — assuming absence — generates a new password" \
            "and upserts it, which silently rotates a working credential and breaks every" \
            "consumer already using the current one." \
            "Fix the probe (broker reachability, admin authorisation) and re-run, or set the" \
            "principal's password explicitly, which needs no probe."

        # THE GLOBAL AND THE EXIT CODE ARE SET TOGETHER, ON EVERY PATH, BY THIS FUNCTION ALONE.
        # Two channels for one fact is only safe while one function writes both of them in the
        # same breath: a caller then cannot read a verdict this probe did not reach. The global
        # is what the predicate and the write-gate read, because it survives the `||` a
        # non-zero return has to be invoked through; the code is what lets a caller distinguish
        # the three answers without touching a global at all.
        SCRAM_CREDENTIAL_STATE="unknown"

        return "$SCRAM_PROBE_UNKNOWN"
    fi

    # Matching on the MECHANISM rather than on the presence of any output is what keeps a
    # principal holding only a SHA-256 credential from being mistaken for one that can
    # authenticate here: Blnk standardises on SHA-512 and the broker needs exactly that one.
    if [[ "$output" == *"${SCRAM_MECHANISM}"* ]]; then
        SCRAM_CREDENTIAL_STATE="present"

        return "$SCRAM_PROBE_EXISTS"
    fi

    SCRAM_CREDENTIAL_STATE="absent"

    return "$SCRAM_PROBE_ABSENT"
}

# require_determinate_credential_state reports whether a credential WRITE may proceed for a
# principal whose probe has just run.
#
# It exists so the rule "never write a credential on an indeterminate probe" is stated once
# rather than repeated at each call site, where one omission reinstates the silent rotation
# this whole tri-state exists to prevent.
#
# # Why this REFUSES the principal rather than aborting the run
#
# Refusing to guess and aborting are not the same thing, and only the first is required. The
# finding is that a credential must not be written on an answer the broker never gave; leaving
# the principal untouched satisfies that completely. Aborting additionally throws away the
# idempotent ACL assertion that follows — which is precisely the repair an operator re-running
# this script during an incident came for — and, in the compose stack, turns a broker that is
# briefly unauthorised to describe user configs into a bring-up that cannot start at all,
# because kafka-init is a one-shot the application services wait on.
#
# So the principal is skipped, the disposition records WHY it was skipped, and the run's
# summary says plainly that the password is unchanged and not because it was preserved. That
# distinction is the whole reporting contract: "skipped" tells an operator to configure a
# destination, "indeterminate" tells them to fix the probe.
#
# KAFKA_ALLOW_SCRAM_PROBE_FAILURE is the one documented escape hatch, for a broker that can
# NEVER answer a describe on user entities and where the alternative is a principal that can
# never be provisioned. Set, an unknown state is treated as absent — deliberately accepting
# that a live credential may be replaced — and the log says so in those words.
#
# Arguments:
#   $1 - the principal, for the diagnostic.
#
# Returns:
#   0 when SCRAM_CREDENTIAL_STATE is present or absent, or when the operator has consented in
#   advance; 1 when it is unknown and they have not.
require_determinate_credential_state() {
    local user="$1"

    if [[ "$SCRAM_CREDENTIAL_STATE" != "unknown" ]]; then
        return 0
    fi

    if is_truthy "$KAFKA_ALLOW_SCRAM_PROBE_FAILURE"; then
        warn "proceeding on an UNKNOWN credential state for '${user}' because KAFKA_ALLOW_SCRAM_PROBE_FAILURE is set" \
            "This run will treat the credential as ABSENT and may therefore REPLACE one that" \
            "already exists. Every consumer or publishing process holding the current password" \
            "will stop authenticating, and nothing here can tell you whether that happened." \
            "Unset the variable to have this principal skipped instead."

        return 0
    fi

    warn "not writing a credential for '${user}': its current state is unknown" \
        "REFUSING TO GUESS. A generated password would be upserted over whatever the broker" \
        "already holds, rotating a credential nobody asked to rotate. The principal is left" \
        "exactly as it is, and its ACLs are still asserted below because those are idempotent" \
        "and cannot invalidate an existing credential." \
        "Fix, whichever applies:" \
        "  - the usual cause is a broker that is not ready or not reachable yet. Re-run." \
        "  - if the administrative principal lacks DescribeConfigs on user entities, grant it," \
        "    or point KAFKA_SASL_ADMIN_USER/KAFKA_SASL_ADMIN_SECRET at one that has it." \
        "  - to write the credential you already intend to write, set the password explicitly;" \
        "    an explicit value needs no probe and is applied idempotently." \
        "  - if this broker can NEVER answer a describe on users, set" \
        "    KAFKA_ALLOW_SCRAM_PROBE_FAILURE=1 to accept the risk deliberately."

    return 1
}

# scram_credential_exists is RETIRED, and deliberately not replaced.
#
# It was the yes/no wrapper the disposition chains used before the state was a tri-state, and
# it existed in two mutually exclusive forms: one built on a `scram_credential_probe` helper
# that no longer exists, and one that read scram_credential_state's exit code and ABORTED the
# run on the third answer. Neither was reachable — both chains call scram_credential_is_present
# and then require_determinate_credential_state — and the first would have failed with
# "command not found" the moment anything did call it.
#
# The two properties they carried are kept, at the places that own them: refusing to write a
# credential on an answer the broker never gave is require_determinate_credential_state, and
# the deliberate escape hatch for a broker that can never answer is the
# KAFKA_ALLOW_SCRAM_PROBE_FAILURE branch inside it. What is NOT kept is the abort, because the
# run has idempotent ACL work left to do that an operator re-running this script came for.


# Report whether the broker knows this principal at all.
#
# A DELIBERATELY WEAKER QUESTION than scram_credential_state, and the difference is the
# point. That one asks "can this principal authenticate the way Blnk needs?"; this one asks
# "does the broker hold any configuration for this name?" - a SHA-256-only credential, a
# quota, or a SHA-512 credential. Both matter, in different places: the credential probe
# decides whether a password may be preserved, and this decides whether a bindings-only
# repair is worth making for a principal that cannot currently authenticate.
#
# "kafka-configs --describe --entity-type users --entity-name <user>" prints nothing at all
# for a name the broker has never heard of, so a non-blank answer is the test. A failed
# describe is reported as "not known", which keeps the caller from granting bindings on the
# strength of a probe that told it nothing; the credential probe has already surfaced the
# broker's own words by the time this runs, so it stays quiet rather than repeating them.
#
# Returns 0 when the broker holds configuration for the principal, 1 when it does not or
# cannot be determined.
principal_is_known() {
    local user="$1" output status

    output="$(kafka_configs --describe --entity-type users --entity-name "$user" 2>/dev/null)" && status=0 || status=$?

    if ((status != 0)); then
        return 1
    fi

    if [[ -n "$(trim "$output")" ]]; then
        return 0
    fi

    return 1
}

# ---------------------------------------------------------------------------------------
# ACL reconciliation
#
# WHY RECONCILIATION AND NOT JUST "--add"
#
# Every ACL grant in this script used to be a bare "kafka-acls --add", which is idempotent
# and was described as needing no existence check. Idempotent is not the same as
# CONVERGENT: --add can only ever widen. So this script could grow a grant and never narrow
# one, and narrowing is the direction that matters for an authorization control.
#
# Three ordinary operations narrowed a grant and were then silently ignored:
#
#   1. Removing a category from KAFKA_SAMPLE_SUBSCRIBER_TOPICS. The re-run added the smaller
#      set, every previous topic's Read and Describe binding stayed, and the summary printed
#      the SMALL list while the broker still served the large one.
#   2. Changing KAFKA_TOPIC_PREFIX. Both principals kept full grants on the whole previous
#      namespace, which is a live grant on topics the deployment no longer considers its own.
#   3. Renaming the sample subscriber or its consumer group. The old prefixed GROUP binding
#      survived, leaving a reserved namespace nothing owns.
#
# Each of those is a configuration change an operator would reasonably expect to take
# effect, and in each case the effective grant stayed broader than the configuration - with
# a run that reported success. That is the same defect class as a stale credential, and it
# is fixed the same way: converge to the desired set rather than append to whatever is
# there.
#
# THE ORDER IS DELETE-THEN-CREATE
#
# Deliberately, and it is the safe order. Every partial failure must leave a principal with
# FEWER rights than the configuration describes, never more. Deleting first satisfies that:
# a reconciliation that dies between the two steps has removed access it was about to
# re-grant, which the next run restores. Creating first and then failing to delete leaves
# live access nothing describes. event_admin.go's reconcileSubscriberACLs orders its two
# round trips the same way and for the same reason, and this script is the local-stack
# counterpart of that function - the two must not disagree about what a principal's grant is.
#
# WHAT COUNTS AS "OURS" TO REMOVE
#
# Only bindings whose SHAPE this script provisions, where a shape is the triple
# (resource type, pattern type, operation) - Read and Describe on a LITERAL topic and Read
# on a PREFIXED group for a subscriber; Write and Describe on a LITERAL topic for the
# producer. Those are the dimensions this script owns, and a binding of an owned shape whose
# resource is not in the desired set is surplus and is removed.
#
# Everything else on the principal is FOREIGN and is never removed by this script, because
# this script did not create it and cannot know what it is for. Foreign bindings are instead
# classified the way blnkManagedACLBinding and foreignACLBindingWidens classify them:
#
#   - An explicit DENY is reported and left alone. It can only narrow the effective grant,
#     so it is safe to keep - and removing it would WIDEN access, which no reconciliation
#     here may do as a side effect.
#   - Anything that can GRANT - an ALLOW of an unowned shape, or a permission type the
#     broker did not state as Allow or Deny - is FATAL. The effective grant is then broader
#     than the configuration by an amount this script cannot bound, so it refuses rather
#     than reporting a grant it cannot state. Fail closed, exactly as issuance does.
#
# THE THREE PROHIBITIONS STILL HOLD, and reconciliation does not relax them: no wildcard
# topic pattern, no Write for a subscriber, and no log line that can interpolate the
# administrative password. What it adds is a fourth: no grant survives a configuration that
# no longer asks for it.
# ---------------------------------------------------------------------------------------

# The canonical descriptor of one ACL binding, as a single "|"-delimited field list.
#
# Written once, here, so the desired set and the observed set are built by the same code and
# can be compared by string equality. A mismatch in field order or letter case between the
# two constructions would read as "every binding is surplus, and every desired binding is
# missing" - a reconciliation that deletes a correct grant and immediately re-creates it,
# which is both alarming in the log and a real, if brief, loss of access.
#
# Parameters:
#   $1 resource type  - TOPIC or GROUP, upper case, as the broker reports it.
#   $2 resource name  - the topic name or the group prefix, verbatim.
#   $3 pattern type   - LITERAL or PREFIXED, upper case.
#   $4 operation      - READ, DESCRIBE or WRITE, upper case.
#   $5 host           - the ACL host, normally "*".
acl_descriptor() {
    printf '%s|%s|%s|%s|%s' "$1" "$2" "$3" "$4" "$5"
}

# The shape of a binding: what it is, without which resource it names.
#
# This is the predicate for ownership. Two bindings of the same shape are provisioned by the
# same code path here, so a binding of an owned shape on an unexpected resource is a stale
# grant of ours rather than somebody else's binding.
acl_shape() {
    local descriptor="$1"
    local type name pattern operation

    IFS='|' read -r type name pattern operation _ <<<"$descriptor"

    printf '%s|%s|%s' "$type" "$pattern" "$operation"
}

# Read every ACL binding the broker currently holds for one principal.
#
# Prints one canonical descriptor per line with the permission type appended as a sixth
# field, sorted, deduplicated:
#
#     TOPIC|blnk.transactions|LITERAL|READ|*|ALLOW
#
# The parse is anchored on the two line shapes "kafka-acls --list" emits and ignores
# everything else, which is what makes it safe to merge the CLI's own stderr into the input:
# a JVM warning matches neither shape. Resource names in Kafka cannot contain a comma, so the
# comma-delimited field extraction is exact rather than heuristic.
#
# A FAILED READ IS FATAL. It has to be: reconciliation removes bindings, and an empty answer
# that actually means "could not tell" would be read as "this principal has nothing", which
# converges to the desired set by creating everything and removing nothing. That is the
# harmless direction, but the same empty answer with a SMALLER desired set would report a
# successful narrowing that never happened - the exact defect this function exists to fix.
describe_principal_acls() {
    local principal="$1" output status

    output="$(kafka_acls --list --principal "$principal" 2>&1)" && status=0 || status=$?

    if ((status != 0)); then
        printf '%s\n' "$output" | redact >&2
        die "could not read the ACL bindings currently held by ${principal}." \
            "The broker's own output is above. This is fatal rather than treated as 'no" \
            "bindings' because the next step REMOVES the bindings this run does not want, and" \
            "an unreadable answer would let it report a narrowing it had not performed." \
            "The two usual causes:" \
            "  1. $(admin_identity_label) lacks Describe authority on the cluster - add the" \
            "     principal to the broker's super.users;" \
            "  2. no authorizer is configured, so the broker has no ACL store to read." \
            "     KRaft needs ${REQUIRED_AUTHORIZER}."
    fi

    printf '%s\n' "$output" | awk '
        /^Current ACLs for resource/ {
            rt = ""; rn = ""; pt = ""
            if (match($0, /resourceType=[A-Z_]+/))   rt = substr($0, RSTART + 13, RLENGTH - 13)
            if (match($0, /name=[^,]+/))             rn = substr($0, RSTART + 5,  RLENGTH - 5)
            if (match($0, /patternType=[A-Z_]+/))    pt = substr($0, RSTART + 12, RLENGTH - 12)
            next
        }
        /\(principal=/ {
            if (rt == "") next
            op = ""; pm = ""; hs = ""
            if (match($0, /operation=[A-Z_]+/))      op = substr($0, RSTART + 10, RLENGTH - 10)
            if (match($0, /permissionType=[A-Z_]+/)) pm = substr($0, RSTART + 15, RLENGTH - 15)
            if (match($0, /host=[^,]+/))             hs = substr($0, RSTART + 5,  RLENGTH - 5)
            if (op == "" || pm == "") next
            print rt "|" rn "|" pt "|" op "|" hs "|" pm
        }
    ' | LC_ALL=C sort -u
}

# Add or remove exactly one binding, named field by field.
#
# One binding per call rather than a batch. It is more round trips, and it buys the thing that
# matters when a reconciliation goes wrong: the failure message names the single binding that
# could not be written, instead of a batch whose partial application has to be inferred.
#
# Parameters:
#   $1 action     - "--add" or "--remove".
#   $2 principal  - the full "User:<name>" form.
#   $3 descriptor - a canonical descriptor from acl_descriptor.
#
# Returns 0 on success. On failure it prints the broker's redacted output to stderr and
# returns non-zero; the caller decides whether that is fatal.
apply_acl_binding() {
    local action="$1" principal="$2" descriptor="$3"
    local type name pattern operation host output resource_flag=()

    IFS='|' read -r type name pattern operation host <<<"$descriptor"

    case "$type" in
        TOPIC) resource_flag=(--topic "$name") ;;
        GROUP) resource_flag=(--group "$name") ;;
        *)
            # Unreachable through either caller: the desired sets are built from TOPIC and
            # GROUP shapes only, and a foreign shape is refused before anything is applied.
            # Guarded anyway, because the alternative to a named refusal here is a
            # kafka-acls invocation with no resource flag, which Kafka would apply to
            # something other than what was asked for.
            die "internal: cannot ${action#--} an ACL binding on resource type '${type}'." \
                "This script provisions TOPIC and GROUP bindings only."
            ;;
    esac

    # --force suppresses the interactive confirmation --remove would otherwise prompt for.
    # Without it a removal run under the compose one-shot reads EOF from /dev/null and
    # abandons the removal, which would leave the stale binding in place while the run
    # reported success.
    if ! output="$(kafka_acls "$action" --force \
        --allow-principal "$principal" \
        --allow-host "$host" \
        --operation "$operation" \
        "${resource_flag[@]}" \
        --resource-pattern-type "$pattern" 2>&1)"; then
        printf '%s\n' "$output" | redact >&2

        return 1
    fi

    return 0
}

# Make the broker's owned bindings for one principal EXACTLY the desired set.
#
# Reads two global arrays, which is this script's established way of passing a list into a
# function:
#
#   ACL_DESIRED        canonical descriptors the configuration asks for.
#   ACL_OWNED_SHAPES   shapes this script provisions for this principal, so a binding of an
#                      owned shape on an undesired resource is recognised as stale.
#
# Verifies the end state by re-reading it, rather than trusting that a successful --add and a
# successful --remove leave the broker where they were asked to. A grant is the thing this
# script exists to establish; "the commands returned zero" is not the same claim as "the
# broker now holds exactly this".
#
# Parameters:
#   $1 principal - the full "User:<name>" form.
#   $2 label     - what to call this principal in the output, e.g. "subscriber".
reconcile_principal_acls() {
    local principal="$1" label="$2"
    local observed=() surplus=() missing=() foreign_allow=() foreign_deny=()
    local line descriptor shape wanted found permission

    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        observed+=("$line")
    done < <(describe_principal_acls "$principal")

    # Classify what is there. Every observed binding lands in exactly one of four buckets, so
    # nothing is silently unaccounted for.
    for line in "${observed[@]}"; do
        permission="${line##*|}"
        descriptor="${line%|*}"
        shape="$(acl_shape "$descriptor")"

        if [[ "$permission" != "ALLOW" ]]; then
            if [[ "$permission" == "DENY" ]]; then
                foreign_deny+=("${descriptor} (${permission})")
            else
                # Neither ALLOW nor DENY: UNKNOWN, or the ANY filter value leaking into a
                # described binding. Counted as widening, because a permission the broker did
                # not state definitively cannot be shown to narrow anything, and "I could not
                # tell" must never be recorded as "it is safe".
                foreign_allow+=("${descriptor} (${permission})")
            fi

            continue
        fi

        found=no
        for wanted in "${ACL_OWNED_SHAPES[@]}"; do
            if [[ "$shape" == "$wanted" ]]; then
                found=yes
                break
            fi
        done

        if [[ "$found" == "no" ]]; then
            foreign_allow+=("$descriptor")

            continue
        fi

        found=no
        for wanted in "${ACL_DESIRED[@]}"; do
            if [[ "$descriptor" == "$wanted" ]]; then
                found=yes
                break
            fi
        done

        if [[ "$found" == "no" ]]; then
            surplus+=("$descriptor")
        fi
    done

    if ((${#foreign_deny[@]} > 0)); then
        warn "${principal} carries DENY ACL bindings this script does not provision, and they" \
            "are being LEFT IN PLACE: $(join_commas "${foreign_deny[@]}")." \
            "A deny can only narrow what the grant allows, so removing one would widen access" \
            "as a side effect of provisioning - which this script will not do." \
            "The ${label} may therefore be able to do LESS than the grant below suggests."
    fi

    # REFUSED BEFORE ANYTHING IS WRITTEN, so a principal whose effective grant this script
    # cannot state is left exactly as it was found. Matching reconcileSubscriberACLs, which
    # refuses issuance on the same condition rather than minting a credential under a boundary
    # the broker is not enforcing.
    if ((${#foreign_allow[@]} > 0)); then
        die "${principal} carries ACL bindings this script did not provision and that GRANT" \
            "access: $(join_commas "${foreign_allow[@]}")." \
            "Its effective grant is therefore broader than this configuration describes, by an" \
            "amount this script cannot bound - so nothing has been added or removed and the" \
            "broker is exactly as it was found." \
            "Blnk's own credential issuance refuses on the same condition, so a subscriber in" \
            "this state cannot be provisioned through the API either." \
            "Fix: remove the bindings above by hand with 'kafka-acls --remove', then re-run." \
            "If one of them is deliberate, it belongs on a principal this script does not" \
            "manage - this one is reconciled to the configuration on every run."
    fi

    for descriptor in "${ACL_DESIRED[@]}"; do
        found=no
        for line in "${observed[@]}"; do
            if [[ "${line%|*}" == "$descriptor" && "${line##*|}" == "ALLOW" ]]; then
                found=yes
                break
            fi
        done

        if [[ "$found" == "no" ]]; then
            missing+=("$descriptor")
        fi
    done

    # DELETE FIRST. See the section header: every partial failure must leave the principal
    # narrower than the configuration, never broader.
    for descriptor in "${surplus[@]}"; do
        log "revoking a grant the configuration no longer asks for" \
            "principal: ${principal}" \
            "binding  : ${descriptor}"

        if ! apply_acl_binding --remove "$principal" "$descriptor"; then
            die "could not revoke the obsolete ACL binding '${descriptor}' from ${principal}." \
                "The broker's own output is above. The binding is still in place, so the" \
                "${label}'s effective grant remains broader than this configuration describes." \
                "The two usual causes:" \
                "  1. $(admin_identity_label) lacks Alter authority on the cluster - add the" \
                "     principal to the broker's super.users;" \
                "  2. no authorizer is configured, so the broker has no ACL store to write to." \
                "     KRaft needs ${REQUIRED_AUTHORIZER}."
        fi
    done

    for descriptor in "${missing[@]}"; do
        if ! apply_acl_binding --add "$principal" "$descriptor"; then
            die "could not grant the ACL binding '${descriptor}' to ${principal}." \
                "The broker's own output is above. The two usual causes:" \
                "  1. $(admin_identity_label) lacks Alter authority on the cluster - add the" \
                "     principal to the broker's super.users;" \
                "  2. no authorizer is configured, so the broker has nowhere to store an ACL." \
                "     KRaft needs ${REQUIRED_AUTHORIZER}; without it ACLs are meaningless and" \
                "     isolation cannot be enforced at all."
        fi
    done

    # VERIFIED, not assumed. Re-read and compare both directions, so the run's success claim is
    # a statement about the broker rather than about the exit statuses of the commands above.
    #
    # RETRIED, because an ACL write is committed to the metadata log before the CLI returns but
    # a subsequent read can be served by a broker whose authorizer cache has not caught up.
    # Three attempts a second apart distinguish "still propagating" from "did not happen", the
    # same way require_topic_geometry distinguishes them for a freshly created topic.
    #
    # THE TWO OUTCOMES ARE NOT SYMMETRIC, and they are handled differently on purpose:
    #
    #   - A surplus binding STILL PRESENT after a successful remove means the principal's grant
    #     is still broader than the configuration. That is the direction this whole function
    #     exists to prevent, and it is FATAL: a run must not report a narrowing it cannot see.
    #   - A desired binding STILL MISSING after a successful add means the grant is NARROWER
    #     than intended. That fails safely and announces itself the moment the client connects,
    #     so it is reported loudly and does not abort a run that may have other work to finish -
    #     aborting would leave a half-provisioned deployment over the less dangerous of the two
    #     divergences.
    if ((${#surplus[@]} > 0 || ${#missing[@]} > 0)); then
        local attempt remaining=() still_present=() still_missing=()

        for attempt in 1 2 3; do
            remaining=()
            still_present=()
            still_missing=()

            while IFS= read -r line; do
                [[ -n "$line" ]] || continue
                remaining+=("$line")
            done < <(describe_principal_acls "$principal")

            for descriptor in "${surplus[@]}"; do
                for line in "${remaining[@]}"; do
                    if [[ "${line%|*}" == "$descriptor" ]]; then
                        still_present+=("$descriptor")
                        break
                    fi
                done
            done

            for descriptor in "${ACL_DESIRED[@]}"; do
                found=no
                for line in "${remaining[@]}"; do
                    if [[ "${line%|*}" == "$descriptor" && "${line##*|}" == "ALLOW" ]]; then
                        found=yes
                        break
                    fi
                done
                if [[ "$found" == "no" ]]; then
                    still_missing+=("$descriptor")
                fi
            done

            if ((${#still_present[@]} == 0 && ${#still_missing[@]} == 0)); then
                break
            fi

            if ((attempt < 3)); then
                sleep 1 || true
            fi
        done

        if ((${#still_present[@]} > 0)); then
            die "the broker still holds ACL bindings this run revoked from ${principal}:" \
                "$(join_commas "${still_present[@]}")." \
                "Every remove reported success, so the grant is still broader than this" \
                "configuration describes and the run must not report otherwise - most often" \
                "because another provisioner is writing the same principal's ACLs concurrently." \
                "Re-run this script; it converges, so a second run either succeeds or fails the" \
                "same way with the same list."
        fi

        if ((${#still_missing[@]} > 0)); then
            warn "${principal} is missing ACL bindings this run granted it:" \
                "$(join_commas "${still_missing[@]}")." \
                "Every add reported success, so either the broker's authorizer cache has not" \
                "caught up after three attempts, or the grant did not take." \
                "This fails SAFE - the principal can do less than intended, not more - and it" \
                "will surface as an authorization failure the moment a client connects." \
                "Re-run this script to converge; it is idempotent."
        fi
    fi

    ok "reconciled the ${label} grant for ${principal}" \
        "desired : ${#ACL_DESIRED[@]} binding(s)" \
        "granted : ${#missing[@]} added" \
        "revoked : ${#surplus[@]} removed (grants the configuration no longer asks for)" \
        "left    : ${#foreign_deny[@]} foreign deny binding(s) untouched"
}

# Grant the producer exactly Write and Describe, on every topic Blnk publishes to.
#
# WHY DESCRIBE AS WELL AS WRITE. A Kafka producer fetches topic metadata before it can choose
# a partition, and that fetch is authorized as Describe. Without it the writer authenticates
# and then fails on every send with UNKNOWN_TOPIC_OR_PARTITION, which reads like a missing
# topic rather than a missing grant - one of the more expensive ways to lose an afternoon.
#
# WHY THE DEAD-LETTER TOPICS TOO. event_dlt.go publishes exhausted events to
# blnk.<category>.dlt through the same publisher and therefore the same principal. A grant
# covering only the category topics would work perfectly until the first retry exhaustion,
# and would then strand the event it was supposed to quarantine.
#
# WHAT IS DELIBERATELY NOT GRANTED: Create on the cluster, because topic assurance is the
# administrative client's job and EnsureTopics runs as the administrative principal in
# cmd/server.go; Read on anything, because a producer consumes nothing; and any group grant,
# because a producer joins no consumer group. Each omission is what keeps a leaked producer
# credential to the smallest possible blast radius - it can write events, and nothing else.
#
# RECONCILED, NOT APPENDED. The grant converges on ALL_TOPICS rather than being added to it,
# which is what makes a KAFKA_TOPIC_PREFIX change take effect: without it the producer kept
# full Write and Describe on the whole previous namespace indefinitely. See the ACL
# reconciliation header above.
grant_producer_acls() {
    local user="$1"
    local principal="${ACL_PRINCIPAL_PREFIX}${user}"
    local topic

    log "reconciling ACLs for ${principal}" \
        "topics : $(join_commas "${ALL_TOPICS[@]}")" \
        "grants : Write, Describe (no Read, no group, no cluster authority)" \
        "host   : ${ACL_HOST_ANY}"

    # Literal patterns, one binding per topic and operation, rather than one prefixed pattern
    # on the topic prefix. A prefixed grant on "blnk" would also cover any future topic whose
    # name begins that way, including ones this feature does not own, so the narrower form is
    # used even though it costs one binding per topic instead of one in total.
    ACL_DESIRED=()
    ACL_OWNED_SHAPES=(
        "TOPIC|LITERAL|WRITE"
        "TOPIC|LITERAL|DESCRIBE"
    )

    for topic in "${ALL_TOPICS[@]}"; do
        ACL_DESIRED+=(
            "$(acl_descriptor TOPIC "$topic" LITERAL WRITE "$ACL_HOST_ANY")"
            "$(acl_descriptor TOPIC "$topic" LITERAL DESCRIBE "$ACL_HOST_ANY")"
        )
    done

    # Every failure path inside the reconciler is fatal and names the binding it could not
    # write. The one worth spelling out for this principal: without a Write binding the relay
    # authenticates and then fails every publish, which surfaces as events accumulating in
    # blnk.event_outbox with a TOPIC_AUTHORIZATION_FAILED last_error rather than as anything
    # that looks like a permissions problem.
    reconcile_principal_acls "$principal" "producer"
}

# Refuse a rotation that has nowhere to deliver the new password.
#
# A PURE CONFIGURATION DECISION, so it belongs here with the other preconditions rather than
# beside the upsert that would consume it. The script's stated order is that everything
# decidable without the network is decided first, and this is the clearest case for it: the
# alternative is an operator reading "there is nowhere to deliver the new password" after a
# successful topic run and a thirty-second broker wait.
#
# WHY A ROTATION REFUSES RATHER THAN SKIPPING. Elsewhere in this script an absent credential
# and an absent destination mean "skip the principal", which is a safe default for something
# nobody asked for. A rotation is different: the operator asked for a new credential in so
# many words. Quietly doing nothing would leave them believing the old password had been
# replaced — and for the PRODUCER it is worse still, because a rotation that half-succeeded
# would leave the running server presenting a password nobody holds.
#
# The generated value cannot simply be printed instead. This script runs as the compose
# kafka-init service, whose stdout is a container log that retains the credential for the
# container's lifetime, hands it to anyone who can run "docker compose logs", and forwards it
# to whatever collects the host's logs.
require_rotation_destination() {
    if is_truthy "$KAFKA_ROTATE_PRODUCER_SECRET" &&
        ! is_truthy "$KAFKA_SKIP_PRODUCER" &&
        [[ -z "$KAFKA_PRODUCER_SECRET" ]] &&
        [[ -z "$(trim "$KAFKA_PRODUCER_SECRET_FILE")" ]]; then
        die "KAFKA_ROTATE_PRODUCER_SECRET is set but there is nowhere to deliver the new password." \
            "Rotating the producer credential without delivering it would leave the server and" \
            "worker authenticating with a password nobody holds, so this refuses rather than" \
            "proceeding. A generated credential is never printed: this script runs as the" \
            "kafka-init service, whose stdout is a container log that would retain it." \
            "Fix, either one:" \
            "  - set KAFKA_PRODUCER_SECRET to the value you want, and put the same value in" \
            "    KAFKA_SASL_SECRET so the publishing processes present it; or" \
            "  - set KAFKA_PRODUCER_SECRET_FILE to a path on a writable mount, and the" \
            "    generated value is written there with mode 0600."
    fi

    if is_truthy "$KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET" &&
        ! is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER" &&
        [[ -z "$KAFKA_SAMPLE_SUBSCRIBER_SECRET" ]] &&
        [[ -z "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE")" ]]; then
        die "KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET is set but there is nowhere to deliver the new password." \
            "A generated credential is never printed: this script runs as the kafka-init" \
            "service, whose stdout is a container log that would retain it indefinitely." \
            "Fix, either one:" \
            "  - set KAFKA_SAMPLE_SUBSCRIBER_SECRET to the value you want, which is applied" \
            "    idempotently and never echoed; or" \
            "  - set KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE to a path on a writable mount, and" \
            "    the generated value is written there with mode 0600."
    fi
}

# Create or update the sample principal's SCRAM credential, then grant its ACLs.
#
# THE CREDENTIAL IS PRESERVED BY DEFAULT. The upsert is only performed when there is no
# existing SCRAM-SHA-512 credential, when the operator supplied a password, or when a
# rotation was explicitly requested - see the disposition logic below for why. The ACLs are
# asserted on every run regardless, because ACL addition is idempotent and a missing binding
# is worth repairing.
ensure_sample_subscriber() {
    if is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER"; then
        log "skipping the sample subscriber principal and its ACLs" \
            "KAFKA_SKIP_SAMPLE_SUBSCRIBER is set. The topics above were still assured." \
            "This is the right setting for a shared or production broker, where subscriber" \
            "principals are provisioned per subscriber through" \
            "POST /subscribers/{id}/kafka-credentials rather than minted by a script."
        SUBSCRIBER_PROVISIONED="skipped"
        return 0
    fi

    # Resolved and validated by require_valid_subscriber, before the broker was touched.
    local user="$SUBSCRIBER_USER" group_prefix="$SUBSCRIBER_GROUP_PREFIX"

    # Decide what to do with the password BEFORE touching the credential, because the
    # default is now to leave an existing one alone.
    #
    # Every re-run used to mint a new password and upsert it, which meant the routine
    # bring-up an operator performs to assure topics silently invalidated the credential
    # their local consumer was already using - and the only copy of the replacement was in
    # that run's console output. Since "docker compose up" runs this on every start, a
    # developer's consumer broke on a schedule for no reason they could see.
    #
    # So: an existing SCRAM-SHA-512 credential is PRESERVED unless the operator asks
    # otherwise, either by supplying a password of their own or by requesting a rotation.
    #
    # GENERATION NOW REQUIRES A DESTINATION. The generated value used to be printed to
    # stdout, which in the compose stack is the kafka-init container's log: the credential
    # was retained for the container's lifetime, readable through "docker compose logs",
    # forwarded to whatever collects the host's logs, and unredactable afterwards. So a
    # generated credential is delivered to a mode-0600 file and nowhere else, and with no
    # file configured nothing is generated at all - an explicit secret is used if there is
    # one, otherwise the principal is skipped and the reason is printed.
    local password="" generated="no"
    if [[ -n "$KAFKA_SAMPLE_SUBSCRIBER_SECRET" ]]; then
        # An explicit password is always applied. It is idempotent by nature - the same
        # value upserted twice leaves the same credential - so re-running with it set cannot
        # invalidate anything, and it needs no probe.
        password="$KAFKA_SAMPLE_SUBSCRIBER_SECRET"
        SUBSCRIBER_SECRET_DISPOSITION="supplied"
    elif is_truthy "$KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET"; then
        # The destination was proved present by require_rotation_destination, before the
        # broker was touched: a rotation with nowhere to put the result is a configuration
        # error, and finding it here would mean reporting it after a successful topic run.
        generate_password
        password="$GENERATED_PASSWORD"
        generated="yes"
        SUBSCRIBER_SECRET_DISPOSITION="rotated"
    elif scram_credential_is_present "$user"; then
        SUBSCRIBER_SECRET_DISPOSITION="preserved"
        log "keeping the existing SCRAM credential for '${user}'" \
            "It already holds a ${SCRAM_MECHANISM} credential, so this run does not touch" \
            "it: re-provisioning would mint a new password and break whatever consumer is" \
            "already using the current one." \
            "The ACLs below are still asserted, because those are idempotent." \
            "To replace the password deliberately, set" \
            "KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET=1, or set" \
            "KAFKA_SAMPLE_SUBSCRIBER_SECRET to a value of your own."
    elif ! require_determinate_credential_state "$user"; then
        # THE PROBE WAS INDETERMINATE, so nothing is written. Generating here — which is what
        # the next arm does — would upsert a new password over whatever the broker already
        # holds and rotate a credential nobody asked to rotate. The disposition records the
        # reason rather than reporting a generation that did not happen.
        SUBSCRIBER_SECRET_DISPOSITION="indeterminate"
        SUBSCRIBER_PROVISIONED="skipped"
    elif [[ -n "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE")" ]]; then
        generate_password
        password="$GENERATED_PASSWORD"
        generated="yes"
        SUBSCRIBER_SECRET_DISPOSITION="generated"
    else
        # No credential, no supplied value, and nowhere to deliver a generated one. SKIPPED
        # rather than failed, deliberately: the sample principal is a local-development
        # convenience, and the topics above are what the relay actually needs. Failing here
        # would mean a bring-up with no .env could not start the broker at all, which is a
        # worse outcome than starting without a sample consumer credential.
        SUBSCRIBER_SECRET_DISPOSITION="skipped"
        SUBSCRIBER_PROVISIONED="skipped"
        log "skipping the sample subscriber principal: no password to give it" \
            "A generated password is no longer printed to this run's output, because in the" \
            "compose stack that output is the kafka-init container log and would retain the" \
            "credential indefinitely. The topics above were still assured, so Blnk publishes" \
            "normally; only the sample CONSUMER credential is absent." \
            "To create it, any one of:" \
            "  - run 'stack.sh --init', which generates KAFKA_SAMPLE_SUBSCRIBER_SECRET into" \
            "    a mode-0600 .env and is the intended local route;" \
            "  - set KAFKA_SAMPLE_SUBSCRIBER_SECRET yourself;" \
            "  - set KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE to a path on a writable mount and a" \
            "    password is generated into it with mode 0600." \
            "Real subscriber credentials are issued by" \
            "POST /subscribers/{id}/kafka-credentials and never by this script."

        # The ACLs are still asserted for a principal that already EXISTS on the broker, and
        # existence here means "the broker knows this user", not "it can authenticate with
        # SCRAM-SHA-512" - by construction it cannot, or this branch would not have been
        # reached. That is why it asks principal_is_known rather than the SHA-512 probe: a
        # principal holding only a SHA-256 credential, or one whose credential was deleted
        # while its grant survived, is exactly the case where a bindings-only repair is worth
        # making. The bindings are idempotent and carry no secret.
        #
        # (This called an undefined function, which under `set -u`/`set -e` in an `if`
        # condition evaluated as false after printing "command not found" - so the repair
        # never happened and the run said nothing about it. It was unreachable at the time,
        # because the credential probe above always answered "yes".)
        if principal_is_known "$user"; then
            grant_subscriber_acls "$user" "$group_prefix"
        fi

        return 0
    fi

    # PRESERVED MEANS DO NOT WRITE A CREDENTIAL, and it has to return before the upsert below
    # rather than fall into it. The branch that sets it leaves $password EMPTY on purpose - the
    # whole point is that the stored credential is left alone - so continuing would compose
    # "SCRAM-SHA-512=iterations=4096,password=" and hand that to kafka-configs, which answers
    # with a hard error. The consequence was not cosmetic: it failed the SECOND provisioning
    # run against any broker that already held this principal, which is every re-run of
    # "docker compose --profile kafka up" on a stack whose volume survived, and it failed AFTER
    # the topics had been assured, so the run looked like a broker problem rather than a script
    # one. The producer leg has had this guard all along; this is the same guard, and the
    # asymmetry was the defect.
    # AN INDETERMINATE PROBE MUST ALSO RETURN BEFORE THE UPSERT, and for a sharper reason than
    # "preserved" does. The branch that set it leaves $password EMPTY, so falling through
    # composes an empty SCRAM value — but worse, if a password HAD been generated the upsert
    # would rotate a credential whose existence was never established. Either way the write is
    # wrong, and only a return prevents it.
    #
    # This is the second half of finding F-11's fix: setting the disposition is not enough on
    # its own, because the upsert below is reached by fall-through rather than by an explicit
    # decision. The ACLs are still asserted, because they are idempotent and carry no secret,
    # and repairing a drifted binding is exactly what an operator re-running this wants even
    # when the credential cannot be inspected.
    if [[ "$SUBSCRIBER_SECRET_DISPOSITION" == "indeterminate" ]]; then
        if principal_is_known "$user"; then
            grant_subscriber_acls "$user" "$group_prefix"
        fi

        return 0
    fi

    if [[ "$SUBSCRIBER_SECRET_DISPOSITION" == "preserved" ]]; then
        grant_subscriber_acls "$user" "$group_prefix"
        SUBSCRIBER_PROVISIONED="yes"

        return 0
    fi

    log "provisioning the sample subscriber principal" \
        "principal  : ${ACL_PRINCIPAL_PREFIX}${user}" \
        "mechanism  : ${SCRAM_MECHANISM}, ${KAFKA_SCRAM_ITERATIONS} iterations" \
        "password   : ${SUBSCRIBER_SECRET_DISPOSITION}"

    # THE PASSWORD NEVER REACHES A COMMAND LINE (Q4-20).
    #
    # It used to: "--add-config SCRAM-SHA-512=[iterations=N,password=SECRET]" put the
    # credential in the CLI's argv, and argv is world-readable through /proc/<pid>/cmdline
    # for the whole life of a JVM start. Any process in the same namespace could read it, and
    # so could anything sampling the process table - a monitoring agent, an audit tool, a
    # container-metrics collector - which is how a local credential ends up in a log nobody
    # meant to write.
    #
    # kafka-configs accepts "--add-config-file", pointing at a Java properties file, and that
    # is a complete remedy rather than a mitigation: the secret is written to a file created
    # at mode 0600 by new_secret_file, consumed from there, and removed by the EXIT trap.
    #
    # THE VALUE HAS NO SQUARE BRACKETS, and that is not a stylistic choice. The brackets in
    # the --add-config form exist to protect the commas from the OPTION parser; in a
    # properties file everything after the first '=' is already the value, so brackets become
    # part of it and Kafka answers "Invalid credential property". Verified against
    # apache/kafka 3.9: the bracketed form is rejected, the bare form creates a credential
    # that then authenticates.

    # DELIVERY IS PROVED BEFORE THE BROKER IS TOUCHED (F-04).
    #
    # A generated password has exactly one copy, and it is in this shell. If the broker
    # accepted the credential and the file could not then be written, the account would be
    # live with an unrecoverable password: SCRAM keeps a salted verifier, so the broker
    # cannot be asked what it now accepts, and the subscriber is locked out until somebody
    # rotates blind. Staging first turns every one of those failures - absent directory,
    # read-only mount, wrong owner, no space, unsettable mode - into a clean abort that
    # leaves the broker exactly as it was and is safe to re-run.
    local staged_secret=""
    if [[ "$generated" == "yes" ]]; then
        stage_generated_secret \
            "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE")" "$password" "sample subscriber"
        staged_secret="$STAGED_SECRET_PATH"
    fi

    local scram_file output
    scram_file="$(new_secret_file scram)"
    if ! printf '%s\n' \
        "${SCRAM_MECHANISM}=iterations=${KAFKA_SCRAM_ITERATIONS},password=${password}" \
        >"$scram_file"; then
        die "could not write the SCRAM credential file for '${user}'." \
            "Fix: check that ${TMPDIR:-/tmp} is writable."
    fi

    if ! output="$(kafka_configs --alter --add-config-file "$scram_file" \
        --entity-type users --entity-name "$user" 2>&1)"; then
        printf '%s\n' "$output" | redact >&2
        die "could not provision the SCRAM credential for '${user}'." \
            "The broker's own output is above, with credential-bearing lines removed. The" \
            "three usual causes:" \
            "  1. $(admin_identity_label) lacks Alter authority on the cluster - add the" \
            "     principal to the broker's super.users;" \
            "  2. the broker does not have ${SCRAM_MECHANISM} among its enabled mechanisms." \
            "     Kafka implements SCRAM-SHA-256 and SCRAM-SHA-512 only, and Blnk" \
            "     standardises on SHA-512 so the broker needs exactly one enabled;" \
            "  3. the CLI is older than Kafka 3.5 and has no '--add-config-file'. That flag" \
            "     is how the password is kept out of the process table, and there is no" \
            "     older equivalent - upgrade KAFKA_IMAGE rather than passing it inline."
    fi

    # Removed the moment it has been consumed, rather than left to the EXIT trap. The trap is
    # the guarantee; this is the window, and on a run that goes on to grant ACLs and print a
    # summary the window is otherwise the rest of the run.
    rm -f "$scram_file" || true

    grant_subscriber_acls "$user" "$group_prefix"

    SUBSCRIBER_PROVISIONED="yes"

    # THE PASSWORD IS NOT PRINTED, IN EITHER BRANCH. A generated one goes to the mode-0600
    # file the operator nominated and only the PATH is reported; a supplied one is not echoed
    # because the operator already holds it.
    #
    # What was here before printed the generated value in a banner, on the reasoning that a
    # credential nobody can read is a credential nobody can use. That reasoning was sound and
    # the mechanism was not: this script runs as the kafka-init service, so its stdout is a
    # container log that keeps the credential for the container's lifetime, hands it to anyone
    # who can run "docker compose logs", forwards it to whatever collects the host's logs, and
    # cannot be redacted after the fact. "Shown once" was true of the banner and false of the
    # log. The file keeps the credential readable to the operator without making it readable
    # to everything that reads logs.
    if [[ "$generated" == "yes" ]]; then
        # The broker has accepted it, so the staged file becomes the destination in one
        # atomic rename. The mode travels with the rename, which is what leaves the
        # destination at 0600 even if it previously existed with a wider one.
        commit_staged_secret \
            "$staged_secret" "$(trim "$KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE")" "sample subscriber"

        log "provisioned the sample subscriber credential" \
            "user     : ${user}" \
            "mechanism: ${SCRAM_MECHANISM} (${KAFKA_SCRAM_ITERATIONS} iterations)" \
            "group    : ${group_prefix}* (prefixed)" \
            "Re-running this script will NOT change it: an existing credential is preserved," \
            "so the consumer you point at it keeps working. To replace it deliberately, set" \
            "KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET=1 with a destination file configured, or" \
            "set KAFKA_SAMPLE_SUBSCRIBER_SECRET to a value of your own." \
            "NEVER use it in production, and never commit it: real subscriber credentials are" \
            "issued by POST /subscribers/{id}/kafka-credentials, which returns the secret once" \
            "and persists only a non-reversible reference to it."
    else
        log "used the KAFKA_SAMPLE_SUBSCRIBER_SECRET you supplied; it is not echoed or written anywhere"
    fi
}

# ---------------------------------------------------------------------------------------
# The steady-state producer principal
#
# WHY THIS PRINCIPAL EXISTS AT ALL
#
# Before it, the local stack shipped one Kafka identity: the administrative superuser. The
# server and the worker were handed that pair and published every ledger event as it. A
# superuser can create and delete topics, mint and revoke SCRAM credentials for any principal
# and rewrite any ACL, so the blast radius of a leaked producer credential was not "an
# attacker can publish events" but "an attacker owns the cluster's authorization state" -
# including the power to grant themselves Read on every subscriber's topics and to revoke the
# credentials of every real subscriber.
#
# config.KafkaConfig now refuses that arrangement outright: with an administrative pair
# configured and no producer pair, the event publisher FAILS TO CONSTRUCT at start-up with a
# message naming KAFKA_SASL_USER. A refusal with nothing to satisfy it is only half a fix, so
# this is the other half - the principal that makes the correct configuration the easy one.
#
# THE GRANT, AND WHY IT IS EXACTLY THIS
#
#     Topic  <each category topic>     LITERAL   Write     Allow   - publish ledger events
#     Topic  <each category topic>     LITERAL   Describe  Allow   - resolve partitions
#     Topic  <each dead-letter topic>  LITERAL   Write     Allow   - preserve dead letters
#     Topic  <each dead-letter topic>  LITERAL   Describe  Allow   - resolve partitions
#
# FOUR PROHIBITIONS, HELD BY HAND
#
#   1. NEVER Read. The relay publishes and never consumes. A producer that could read would be
#      able to see every ledger event of every subscriber, which is the exact confidentiality
#      boundary the per-subscriber ACLs exist to draw.
#   2. NEVER a consumer-group binding. Nothing here joins a group; the offset reads behind
#      reconciliation are performed by the ADMINISTRATIVE client in the server process, not by
#      the publisher.
#   3. NEVER a wildcard or prefixed topic pattern. Every topic is named with a literal
#      pattern, so the grant grows only when the catalogue does.
#   4. NEVER a cluster operation - no Alter, no Create, no DescribeConfigs. Topic assurance is
#      an administrative act performed by the server's admin client with the administrative
#      pair; a producer that could create topics could create one outside the catalogue with
#      the wrong partition count and no ACL at all.
#
# THE DEAD-LETTER TOPICS ARE INCLUDED, and that is not a widening of the grant by accident.
# Dead-lettering is a PUBLISH performed by the same process on the same connection: when an
# event exhausts its retry budget the relay writes it to <topic>.dlt. Without Write on the
# dead-letter siblings, the one path that exists to keep an unpublishable event durable is the
# one path that is unauthorized, and the event is stranded in the outbox instead.
# ---------------------------------------------------------------------------------------

# Create or update the producer's SCRAM credential, then grant its Write and Describe bindings.
#
# The credential discipline is the sample subscriber's, for the same reason: a routine
# bring-up must not invalidate the credential a running server is authenticating
# with. An existing SCRAM-SHA-512 credential is preserved unless a value is supplied or a
# rotation is explicitly requested, and a generated value is delivered to a mode-0600 file
# rather than printed.
#
# The BINDINGS are asserted on every run regardless of what happened to the credential,
# because ACL addition is idempotent and a missing binding is worth repairing - a producer
# whose Write on one dead-letter topic went missing fails only for the events that need that
# topic, which is the hardest kind of failure to attribute.
ensure_producer_principal() {
    if is_truthy "$KAFKA_SKIP_PRODUCER"; then
        log "skipping the steady-state producer principal and its grant" \
            "KAFKA_SKIP_PRODUCER is set. The topics above were still assured." \
            "This is the right setting for a broker whose principals are managed elsewhere." \
            "Remember that Blnk REFUSES TO PUBLISH as the administrative principal: the" \
            "server still needs KAFKA_SASL_USER and KAFKA_SASL_SECRET naming a" \
            "principal with Write and Describe on the topics above."
        PRODUCER_PROVISIONED="skipped"

        return 0
    fi

    # Resolved and validated by require_valid_producer, before the broker was touched.
    local user="$PRODUCER_USER"
    local password="" generated="no"

    # The same precedence the identity follows: the application's own name first, the
    # provisioning alias second. Anything else lets the script mint one password while the
    # relay presents another, and the symptom is an authentication failure with two
    # correct-looking configurations.
    local supplied="$KAFKA_SASL_SECRET"
    if [[ -z "$supplied" ]]; then
        supplied="$KAFKA_PRODUCER_SECRET"
    fi

    if [[ -n "$supplied" ]]; then
        password="$supplied"
        PRODUCER_SECRET_DISPOSITION="supplied"
    elif is_truthy "$KAFKA_ROTATE_PRODUCER_SECRET"; then
        # Destination proved present by require_rotation_destination, before the broker was
        # touched. See there for why a rotation with nowhere to deliver refuses outright.
        #
        # The role and its two variables are passed EXPLICITLY: generate_password defaults
        # them to the sample subscriber's, so a bare call here would tell an operator whose
        # producer password could not be generated to set KAFKA_SAMPLE_SUBSCRIBER_SECRET.
        generate_password "producer" "KAFKA_PRODUCER_SECRET" "KAFKA_SKIP_PRODUCER"
        password="$GENERATED_PASSWORD"
        generated="yes"
        PRODUCER_SECRET_DISPOSITION="rotated"
    elif scram_credential_is_present "$user"; then
        PRODUCER_SECRET_DISPOSITION="preserved"
        log "keeping the existing SCRAM credential for '${user}'" \
            "It already holds a ${SCRAM_MECHANISM} credential, so this run does not touch it:" \
            "replacing it would stop the running server from authenticating." \
            "The grant below is still asserted, because ACL addition is idempotent." \
            "To replace the password deliberately, set KAFKA_PRODUCER_SECRET, or set" \
            "KAFKA_ROTATE_PRODUCER_SECRET=1 with KAFKA_PRODUCER_SECRET_FILE configured."
    elif ! require_determinate_credential_state "$user"; then
        # As for the sample subscriber, and the consequence here is larger: rotating this
        # credential stops the running server from authenticating, so an
        # indeterminate probe must never reach the generation arm below.
        PRODUCER_SECRET_DISPOSITION="indeterminate"
        PRODUCER_PROVISIONED="skipped"
    elif [[ -n "$(trim "$KAFKA_PRODUCER_SECRET_FILE")" ]]; then
        # Named for the same reason as the rotation arm above.
        generate_password "producer" "KAFKA_PRODUCER_SECRET" "KAFKA_SKIP_PRODUCER"
        password="$GENERATED_PASSWORD"
        generated="yes"
        PRODUCER_SECRET_DISPOSITION="generated"
    else
        # SKIPPED rather than failed, matching the sample subscriber: the topics are what the
        # relay needs, and failing here would mean a bring-up with no .env could not start the
        # broker at all. The message is emphatic because the consequence is not cosmetic - the
        # server will refuse to construct its publisher without this pair.
        PRODUCER_SECRET_DISPOSITION="skipped"
        PRODUCER_PROVISIONED="skipped"
        log "skipping the steady-state producer principal: no password to give it" \
            "A generated password is no longer printed to this run's output, because in the" \
            "compose stack that output is the kafka-init container log and would retain the" \
            "credential indefinitely." \
            "THIS MATTERS MORE THAN THE SAMPLE SUBSCRIBER: with an administrative pair" \
            "configured and no producer pair, Blnk's event publisher REFUSES TO CONSTRUCT and" \
            "the server does not start. Blnk will not publish as the administrator." \
            "To create it, any one of:" \
            "  - run 'stack.sh --init', which generates KAFKA_PRODUCER_SECRET and the matching" \
            "    KAFKA_SASL_USER and KAFKA_SASL_SECRET into a mode-0600 .env;" \
            "  - set KAFKA_PRODUCER_SECRET yourself, and the same value in KAFKA_SASL_SECRET;" \
            "  - set KAFKA_PRODUCER_SECRET_FILE to a path on a writable mount and a password" \
            "    is generated into it with mode 0600."

        return 0
    fi

    # AN INDETERMINATE PROBE MUST ALSO RETURN BEFORE THE UPSERT, and for a sharper reason than
    # "preserved" does. The branch that set it leaves $password EMPTY, so falling through
    # composes an empty SCRAM value — but worse, if a password HAD been generated the upsert
    # would rotate a credential whose existence was never established. Either way the write is
    # wrong, and only a return prevents it.
    #
    # This is the second half of finding F-11's fix: setting the disposition is not enough on
    # its own, because the upsert below is reached by fall-through rather than by an explicit
    # decision. The ACLs are still asserted, because they are idempotent and carry no secret,
    # and repairing a drifted binding is exactly what an operator re-running this wants even
    # when the credential cannot be inspected.
    if [[ "$PRODUCER_SECRET_DISPOSITION" == "indeterminate" ]]; then
        if principal_is_known "$user"; then
            grant_producer_acls "$user"
        fi

        return 0
    fi

    if [[ "$PRODUCER_SECRET_DISPOSITION" == "preserved" ]]; then
        grant_producer_acls "$user"
        PRODUCER_PROVISIONED="yes"

        return 0
    fi

    log "provisioning the steady-state producer principal" \
        "principal  : ${ACL_PRINCIPAL_PREFIX}${user}" \
        "mechanism  : ${SCRAM_MECHANISM}, ${KAFKA_SCRAM_ITERATIONS} iterations" \
        "password   : ${PRODUCER_SECRET_DISPOSITION}"

    # THROUGH A FILE, NOT THROUGH ARGV - the same remedy the sample subscriber's upsert uses,
    # and for a credential that matters more than that one.
    #
    # This leg used to pass "--add-config SCRAM-SHA-512=[...,password=SECRET]", putting the
    # password in the CLI's argv where /proc/<pid>/cmdline exposes it for the whole life of the
    # JVM start - to any process in the same namespace, and to anything that samples the process
    # table, which is how a credential reaches a log nobody meant to write. The comment here
    # justified it as "the CLI offers no alternative", and the subscriber leg forty lines above
    # disproves that in the same file: --add-config-file is available and is described there as
    # a complete remedy rather than a mitigation. Leaving the STEADY-STATE PRODUCER - the
    # long-lived identity the server authenticates as - on the weaker of two paths
    # this file already implements was indefensible.
    #
    # No square brackets, for the reason recorded at the subscriber's upsert: in a properties
    # file everything after the first '=' is the value, so brackets become part of the password
    # and Kafka answers "Invalid credential property".
    # DELIVERY IS PROVED BEFORE THE BROKER IS TOUCHED (F-04).
    #
    # A generated password has exactly one copy, and it is in this shell. If the broker
    # accepted the credential and the file could not then be written, the account would be
    # live with an unrecoverable password: SCRAM keeps a salted verifier, so the broker
    # cannot be asked what it now accepts, and the server - which authenticates as
    # this very principal - would refuse to construct its publisher and fail to start.
    # Staging first turns every one of those failures - absent directory,
    # read-only mount, wrong owner, no space, unsettable mode - into a clean abort that
    # leaves the broker exactly as it was and is safe to re-run.
    local staged_secret=""
    if [[ "$generated" == "yes" ]]; then
        stage_generated_secret \
            "$(trim "$KAFKA_PRODUCER_SECRET_FILE")" "$password" "producer"
        staged_secret="$STAGED_SECRET_PATH"
    fi

    local scram_file output
    scram_file="$(new_secret_file scram)"
    if ! printf '%s\n' \
        "${SCRAM_MECHANISM}=iterations=${KAFKA_SCRAM_ITERATIONS},password=${password}" \
        >"$scram_file"; then
        die "could not write the SCRAM credential file for '${user}'." \
            "Fix: check that ${TMPDIR:-/tmp} is writable."
    fi

    if ! output="$(kafka_configs --alter --add-config-file "$scram_file" \
        --entity-type users --entity-name "$user" 2>&1)"; then
        printf '%s\n' "$output" | redact >&2
        die "could not provision the SCRAM credential for '${user}'." \
            "The broker's own output is above, with credential-bearing lines removed. The" \
            "three usual causes:" \
            "  1. $(admin_identity_label) lacks Alter authority on the cluster - add the" \
            "     principal to the broker's super.users;" \
            "  2. the broker does not have ${SCRAM_MECHANISM} among its enabled mechanisms." \
            "     Kafka implements SCRAM-SHA-256 and SCRAM-SHA-512 only, and Blnk" \
            "     standardises on SHA-512 so the broker needs exactly one enabled;" \
            "  3. the CLI is older than Kafka 3.5 and has no '--add-config-file'. That flag" \
            "     is how the password is kept out of the process table, and there is no" \
            "     older equivalent - upgrade KAFKA_IMAGE rather than passing it inline."
    fi

    # Removed the moment it has been consumed, matching the subscriber leg: the EXIT trap is the
    # guarantee, and this is the window - which on a run that goes on to grant ACLs and print a
    # summary would otherwise be the rest of the run.
    rm -f "$scram_file" || true

    grant_producer_acls "$user"

    PRODUCER_PROVISIONED="yes"

    if [[ "$generated" == "yes" ]]; then
        # The broker has accepted it, so the staged file becomes the destination in one
        # atomic rename, carrying its 0600 mode with it.
        commit_staged_secret \
            "$staged_secret" "$(trim "$KAFKA_PRODUCER_SECRET_FILE")" "producer"

        log "provisioned the producer credential" \
            "user     : ${user}" \
            "mechanism: ${SCRAM_MECHANISM} (${KAFKA_SCRAM_ITERATIONS} iterations)" \
            "Put this user and the password from that file into KAFKA_SASL_USER and" \
            "KAFKA_SASL_SECRET for the server, or it will refuse to construct" \
            "its event publisher rather than publish as the administrator."
    else
        log "used the KAFKA_PRODUCER_SECRET you supplied; it is not echoed"
    fi
}

# ---------------------------------------------------------------------------------------
# Step three: the subscriber's ACLs
#
# THE GRANT, AND WHY IT IS EXACTLY THIS
#
#     Topic  <each authorised topic>   LITERAL   Read      Allow   - consume the topic
#     Topic  <each authorised topic>   LITERAL   Describe  Allow   - see its partitions
#     Group  <consumer group prefix>   PREFIXED  Read      Allow   - join and commit
#
# Binding for binding, that is what event_admin.go's aclEntries builds. Kafka's implication
# rules make Read imply Describe on the same resource, so the topic Describe binding is
# technically redundant; it is requested explicitly anyway so that the grant is auditable
# from "kafka-acls --list" alone, without the reader having to know the implication table.
#
# THE PREFIXED GROUP PATTERN
#
# A prefixed pattern reserves the subscriber's whole consumer-group namespace without
# enumerating groups, so a subscriber can run as many consumer groups as it likes under its
# own prefix and none of them needs a new ACL - while it still cannot join anybody else's.
#
# FOUR PROHIBITIONS, HELD BY HAND
#
# event_admin_test.go asserts the equivalents for the Go path. There is no equivalent
# automated test for a shell script, which is exactly why they are written down here:
#
#   1. NEVER a wildcard topic pattern. No --topic '*', no wildcard resource-pattern type.
#      Every topic is named explicitly with a literal pattern. A wildcard grant is
#      indistinguishable from no isolation at all.
#   2. NEVER Write. Subscribers consume; only Blnk's relay produces. A subscriber that could
#      produce to a category topic could forge ledger events.
#   3. NEVER surface the administrative password. It exists only inside the 0600
#      client-properties file, and no log line in this script can interpolate it.
#   4. NEVER leave a grant the configuration no longer asks for. The three bindings above are
#      the WHOLE grant, not a minimum: reconcile_principal_acls removes any owned binding
#      outside them, because an --add-only provisioner can widen a grant and can never narrow
#      one. See the ACL reconciliation header further up.
# ---------------------------------------------------------------------------------------

grant_subscriber_acls() {
    local user="$1" group_prefix="$2"
    local principal="${ACL_PRINCIPAL_PREFIX}${user}"
    local topic

    log "reconciling ACLs for ${principal}" \
        "topics : $(join_commas "${SUBSCRIBER_TOPICS[@]}")" \
        "group  : ${group_prefix} (prefixed)" \
        "host   : ${ACL_HOST_ANY}"

    # THE GRANT AS A SET, so removing a category from KAFKA_SAMPLE_SUBSCRIBER_TOPICS or
    # renaming the principal's consumer group actually narrows what the broker serves. Adding
    # the smaller set and leaving the previous bindings in place - which is all --add can do -
    # left the summary printing the small list while the broker still served the large one.
    ACL_DESIRED=()
    ACL_OWNED_SHAPES=(
        "TOPIC|LITERAL|READ"
        "TOPIC|LITERAL|DESCRIBE"
        "GROUP|PREFIXED|READ"
    )

    for topic in "${SUBSCRIBER_TOPICS[@]}"; do
        ACL_DESIRED+=(
            "$(acl_descriptor TOPIC "$topic" LITERAL READ "$ACL_HOST_ANY")"
            "$(acl_descriptor TOPIC "$topic" LITERAL DESCRIBE "$ACL_HOST_ANY")"
        )
    done

    # Without this binding the principal can authenticate and describe its topics but cannot
    # join a consumer group or commit an offset, which looks like a consumer that starts and
    # then does nothing.
    ACL_DESIRED+=("$(acl_descriptor GROUP "$group_prefix" PREFIXED READ "$ACL_HOST_ANY")")

    reconcile_principal_acls "$principal" "subscriber"
}

# ---------------------------------------------------------------------------------------
# Step four: the producer principal
#
# WHY THIS PRINCIPAL EXISTS
#
# Before it, the local stack configured only KAFKA_SASL_ADMIN_USER, so Blnk's event
# publisher authenticated as the administrator - the identity that creates topics, alters
# SCRAM credentials and grants or revokes ACLs. Every ledger event was produced at the
# privilege level that controls the cluster's access model, which turned a leaked producer
# credential into a full compromise of that model rather than the ability to publish events,
# and left the broker's audit trail unable to tell routine publishing from administration.
#
# The Go side now REFUSES to borrow the administrative credential unless
# KAFKA_ALLOW_ADMIN_PRODUCER is set deliberately. That refusal is only a real improvement if
# there is a producer principal to use instead, which is what this provisions.
#
# THE GRANT, AND WHY IT IS EXACTLY THIS
#
#     Topic  <every Blnk-owned topic>   LITERAL   Write     Allow   - produce the event
#     Topic  <every Blnk-owned topic>   LITERAL   Describe  Allow   - see its partitions
#
# EVERY owned topic means the category topics AND their .dlt siblings. The relay produces to
# the category topics; the dead-letter writer produces to the siblings. A grant covering only
# the former would fail at exactly the moment an event needs dead-lettering - the worst
# possible time to discover a missing binding, because the event that triggered it is already
# on its last attempt.
#
# Describe is requested explicitly even though Kafka's implication rules make Write imply it,
# so that the grant is auditable from "kafka-acls --list" alone. The publisher genuinely
# needs partition metadata: it keys by ledger ID and lets the murmur2 balancer choose a
# partition, which cannot be done without knowing how many there are.
#
# FOUR PROHIBITIONS, HELD BY HAND
#
#   1. NEVER Read. This principal publishes; it never consumes. Read would let a leaked
#      producer credential exfiltrate every ledger event and identity record in the cluster.
#   2. NEVER Create, Alter, Delete or DescribeConfigs. Topic assurance is the administrator's
#      work and runs from cmd/server.go's admin client, not from the publisher.
#   3. NEVER anything at Cluster scope, and no consumer-group binding. The publisher joins no
#      group, and kafka-go's Writer performs no idempotent or transactional produce, so
#      IdempotentWrite is not needed either - granting it would be privilege with no use.
#   4. NEVER a wildcard topic pattern. Every topic is named with a literal pattern; a
#      wildcard Write grant is indistinguishable from letting anything forge ledger events.
# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# The closing summary
#
# What makes a successful run verifiable at a glance, and the only place the resolved
# catalogue is shown as a whole. The topic list is what to compare against
# event_topics.go's AllTopicsWithDeadLetters when a subscriber reports seeing no events.
# ---------------------------------------------------------------------------------------

# print_producer_summary WAS RETIRED FROM HERE, and removed rather than wired in.
#
# It was a third rendering of the producer's summary section, never called by anything. Its own
# comment said it was extracted from print_summary "because print_summary is already long" — but
# the extraction was never finished: the inline block it was copied from is still there, still
# live, and still covers all five PRODUCER_SECRET_DISPOSITION cases.
#
# WIRING IT IN WOULD HAVE BROKEN THE SCRIPT. It referenced ${PRODUCER_SECRET_LOCATION}, which is
# assigned nowhere. Unlike an undefined ARRAY, an undefined SCALAR under `set -u` is fatal, so the
# rotated and generated paths — the two that matter most, because they are the runs where the
# operator must copy a new secret — would have aborted the summary after provisioning had already
# succeeded. It also told the reader that a supplied password was "the KAFKA_SASL_SECRET you
# supplied", conflating this script's own input (KAFKA_PRODUCER_SECRET) with the variable the
# application reads.
#
# So the live inline block in print_summary is the only producer rendering, and it is the correct
# one. Note for a maintainer: print_summary still prints a SECOND, less complete producer section
# further down, covering only the rotated and supplied dispositions. That duplication is
# cosmetic — neither block is wrong — and is left alone here because no finding covers it.

print_summary() {
    local index
    # For the sample-subscriber branch: the allowlist entries this run did not grant.
    local grantable_topic granted_topic granted
    local withheld_topics=()

    printf '%s\n' ""
    ok "Kafka is provisioned for Blnk event streaming"
    printf '%s\n' ""
    printf '%s\n' "Broker:"
    case "$ADMIN_SASL_MODE" in
        yes)
            printf '%s\n' "  ${KAFKA_BOOTSTRAP_SERVER} (${KAFKA_SECURITY_PROTOCOL}, ${SCRAM_MECHANISM} as ${KAFKA_SASL_ADMIN_USER})"
            ;;
        config)
            printf '%s\n' "  ${KAFKA_BOOTSTRAP_SERVER} (settings from ${CLIENT_CONFIG})"
            ;;
        *)
            printf '%s\n' "  ${KAFKA_BOOTSTRAP_SERVER} (${KAFKA_SECURITY_PROTOCOL}, no SASL)"
            ;;
    esac
    printf '%s\n' ""
    # Both numbers are what the broker reported for each topic, never what was requested.
    # This column used to print KAFKA_REPLICATION_FACTOR for every row, which presented a
    # requested factor as an observed one - so an RF1 topic on an RF3 deployment was
    # reported as RF3 by the very output an operator would check.
    printf '%s\n' "Topics (name, partitions, replication factor - all as OBSERVED on the broker):"
    for index in "${!SUMMARY_TOPICS[@]}"; do
        printf '  %-34s %-4s %s\n' \
            "${SUMMARY_TOPICS[index]}" \
            "${SUMMARY_PARTITIONS[index]}" \
            "${SUMMARY_REPLICATION[index]}"
    done
    printf '%s\n' ""

    # A REFUSED GROWTH IS NAMED HERE TOO, and not only in the disposition line above it.
    #
    # The table shows each topic's real partition count, so the information is technically
    # present - but reading it requires the operator to compare eight numbers against
    # KAFKA_MIN_PARTITIONS by eye, and the run exited zero, which is the signal most people
    # act on. A topic left under-partitioned is a throughput ceiling on the ledger's event
    # stream that nothing else will ever report: no alert fires, no consumer errors, and the
    # next run refuses it again just as quietly. The closing summary is where an operator
    # looks once, so it says so once, with the remedy attached.
    if ((${#SUMMARY_GROWTH_REFUSED[@]} > 0)); then
        printf '%s\n' "Left under-partitioned (fewer than the configured ${TARGET_PARTITIONS} partitions):"
        for index in "${!SUMMARY_GROWTH_REFUSED[@]}"; do
            printf '  %s\n' "${SUMMARY_GROWTH_REFUSED[index]}"
        done
        printf '%s\n' "  Each holds records, or could not be proven empty, so growing it was refused:"
        printf '%s\n' "  raising a partition count re-maps existing keys, and the key here is the ledger"
        printf '%s\n' "  id that carries the per-aggregate ordering guarantee."
        printf '%s\n' "  To grow anyway: KAFKA_ALLOW_PARTITION_GROWTH=true, accepting that ordering does"
        printf '%s\n' "  not hold for keys already in flight. To keep the current count deliberately,"
        printf '%s\n' "  set KAFKA_MIN_PARTITIONS to it so configuration and reality agree."
        printf '%s\n' ""
    fi

    case "$PRODUCER_PROVISIONED" in
        yes)
            printf '%s\n' "Steady-state producer (what the server publishes as):"
            printf '%s\n' "  principal            ${ACL_PRINCIPAL_PREFIX}${PRODUCER_USER}"
            printf '%s\n' "  writable topics      every topic listed above, including the .dlt siblings"
            printf '%s\n' "  granted operations   Write, Describe"
            printf '%s\n' "  NOT granted          Create, Alter, Read anywhere, no consumer group, no"
            printf '%s\n' "                       cluster operation"
            case "$PRODUCER_SECRET_DISPOSITION" in
                preserved)
                    printf '%s\n' "  password             unchanged - the existing credential was kept, so a"
                    printf '%s\n' "                       running server keeps authenticating"
                    ;;
                rotated)
                    printf '%s\n' "  password             ROTATED into the file named above. Update"
                    printf '%s\n' "                       KAFKA_SASL_SECRET or the publishing processes will"
                    printf '%s\n' "                       stop authenticating"
                    ;;
                generated)
                    printf '%s\n' "  password             generated into the mode-0600 file named above"
                    printf '%s\n' "                       (never printed, and not in this run's output)"
                    ;;
                supplied)
                    printf '%s\n' "  password             the KAFKA_PRODUCER_SECRET you supplied"
                    ;;
                indeterminate)
                    printf '%s\n' "  password             UNCHANGED, and not because it was preserved: this run could"
                    printf '%s\n' "                       not determine whether a credential exists, so it wrote none."
                    printf '%s\n' "                       Writing one anyway would have rotated a live credential and"
                    printf '%s\n' "                       stopped the server authenticating."
                    printf '%s\n' "                       Fix broker reachability or the admin principal's authorisation"
                    printf '%s\n' "                       to describe user configs, then re-run"
                    ;;
            esac
            printf '%s\n' "  REQUIRED BY BLNK     set KAFKA_SASL_USER and KAFKA_SASL_SECRET to this"
            printf '%s\n' "                       principal. Blnk refuses to publish as the"
            printf '%s\n' "                       administrator and will not start without them"
            ;;
        skipped)
            printf '%s\n' "Steady-state producer:"
            if is_truthy "$KAFKA_SKIP_PRODUCER"; then
                printf '%s\n' "  skipped - KAFKA_SKIP_PRODUCER is set. The server still needs"
                printf '%s\n' "  KAFKA_SASL_USER and KAFKA_SASL_SECRET naming a principal with Write and"
                printf '%s\n' "  Describe on the topics above"
            else
                printf '%s\n' "  skipped - no password to give it. Blnk will REFUSE TO START with an"
                printf '%s\n' "  administrative pair configured and no producer pair: run 'stack.sh --init',"
                printf '%s\n' "  or set KAFKA_PRODUCER_SECRET, or set KAFKA_PRODUCER_SECRET_FILE"
            fi
            ;;
        *)
            printf '%s\n' "Steady-state producer:"
            printf '%s\n' "  NOT provisioned. Blnk REFUSES to publish as the administrator, so a server"
            printf '%s\n' "  configured with an administrative pair and no producer pair will not start."
            printf '%s\n' "  Run './stack.sh --init', or set KAFKA_PRODUCER_SECRET, so that publishing"
            printf '%s\n' "  does not carry authority to create topics, mint credentials and rewrite ACLs"
            ;;
    esac

    case "$SUBSCRIBER_PROVISIONED" in
        yes)
            for grantable_topic in "${GRANTABLE_TOPICS[@]}"; do
                granted=no
                for granted_topic in "${SUBSCRIBER_TOPICS[@]}"; do
                    if [[ "$grantable_topic" == "$granted_topic" ]]; then
                        granted=yes
                        break
                    fi
                done
                if [[ "$granted" == "no" ]]; then
                    withheld_topics+=("$grantable_topic")
                fi
            done

            printf '%s\n' "Sample subscriber:"
            printf '%s\n' "  principal            ${ACL_PRINCIPAL_PREFIX}${SUBSCRIBER_USER}"
            printf '%s\n' "  consumer group       ${SUBSCRIBER_GROUP_PREFIX}* (prefixed)"
            printf '%s\n' "  readable topics      $(join_commas "${SUBSCRIBER_TOPICS[@]}")"
            printf '%s\n' "  granted operations   Read, Describe on those topics; Read on that group namespace"
            printf '%s\n' "  NOT granted          Write anywhere, and no .dlt topic - those are Blnk's"
            printf '%s\n' "                       own internals, triaged through the events API"
            if ((${#INTERNAL_TOPICS[@]} > 0)); then
                printf '%s\n' "  created, never       $(join_commas "${INTERNAL_TOPICS[@]}")"
                printf '%s\n' "  granted              published to, and granted by this script to nobody: the"
                printf '%s\n' "                       system topic carries system.error's verbatim error text and"
                printf '%s\n' "                       every uncatalogued event type. Grant it through the"
                printf '%s\n' "                       subscriber API where KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS"
                printf '%s\n' "                       is declared"
            fi
            # WITHHELD IS COMPUTED, not written out, so this line describes the grant that was
            # actually made and keeps describing it if the default set changes. It reports only
            # GRANTABLE topics the sample did not receive; the internal topic is reported above,
            # because this script withholds it from every principal it creates rather than from
            # this one in particular.
            if ((${#withheld_topics[@]} > 0)); then
                printf '%s\n' "  grantable, withheld  $(join_commas "${withheld_topics[@]}")"
                printf '%s\n' "                       on the allowlist and not in this run's grant - name"
                printf '%s\n' "                       it in KAFKA_SAMPLE_SUBSCRIBER_TOPICS to include it"
            fi
            case "$SUBSCRIBER_SECRET_DISPOSITION" in
                preserved)
                    printf '%s\n' "  password             unchanged - the existing credential was kept"
                    printf '%s\n' "                       (KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET=1 to replace it)"
                    ;;
                rotated)
                    printf '%s\n' "  password             ROTATED into the file named above. Any consumer using"
                    printf '%s\n' "                       the previous password must be updated"
                    ;;
                generated)
                    printf '%s\n' "  password             generated into the mode-0600 file named above"
                    printf '%s\n' "                       (never printed, and not in this run's output)"
                    ;;
                supplied)
                    printf '%s\n' "  password             the KAFKA_SAMPLE_SUBSCRIBER_SECRET you supplied"
                    ;;
                indeterminate)
                    printf '%s\n' "  password             UNCHANGED, and not because it was preserved: this run could"
                    printf '%s\n' "                       not determine whether a credential exists, so it wrote none"
                    printf '%s\n' "                       rather than risk rotating one a consumer is using."
                    printf '%s\n' "                       Fix broker reachability or the admin principal's authorisation"
                    printf '%s\n' "                       to describe user configs, then re-run"
                    ;;
            esac
            ;;
        skipped)
            printf '%s\n' "Sample subscriber:"
            if is_truthy "$KAFKA_SKIP_SAMPLE_SUBSCRIBER"; then
                printf '%s\n' "  skipped - KAFKA_SKIP_SAMPLE_SUBSCRIBER is set"
            else
                printf '%s\n' "  skipped - no password to give it, and a generated one is no longer printed."
                printf '%s\n' "  Run 'stack.sh --init', or set KAFKA_SAMPLE_SUBSCRIBER_SECRET, or set"
                printf '%s\n' "  KAFKA_SAMPLE_SUBSCRIBER_SECRET_FILE. Blnk publishes normally either way"
            fi
            ;;
        *)
            printf '%s\n' "Sample subscriber:"
            printf '%s\n' "  not provisioned"
            ;;
    esac
    printf '%s\n' ""

    case "$PRODUCER_PROVISIONED" in
        yes)
            printf '%s\n' "Producer principal (what the server publishes as):"
            printf '%s\n' "  principal            ${ACL_PRINCIPAL_PREFIX}${PRODUCER_USER}"
            printf '%s\n' "  writable topics      $(join_commas "${ALL_TOPICS[@]}")"
            printf '%s\n' "  granted operations   Write, Describe on those topics"
            printf '%s\n' "  NOT granted          Create, Alter, Read, any consumer group, any cluster operation"
            case "$PRODUCER_SECRET_DISPOSITION" in
                rotated)
                    printf '%s\n' "  password             ROTATED into the mode-0600 file named above, and"
                    printf '%s\n' "                       never printed. Put it in .env as KAFKA_SASL_SECRET"
                    printf '%s\n' "                       and restart the server"
                    ;;
                supplied)
                    printf '%s\n' "  password             the KAFKA_SASL_SECRET you supplied"
                    ;;
            esac
            ;;
        skipped)
            printf '%s\n' "Producer principal:"
            printf '%s\n' "  skipped - KAFKA_SKIP_PRODUCER is set"
            ;;
        *)
            printf '%s\n' "Producer principal:"
            printf '%s\n' "  NOT provisioned - the publisher will authenticate as the administrator."
            printf '%s\n' "  Set KAFKA_SASL_USER and KAFKA_SASL_SECRET (./stack.sh --init generates them)"
            printf '%s\n' "  so that publishing does not carry authority to create topics, mint"
            printf '%s\n' "  credentials and rewrite ACLs."
            ;;
    esac
    printf '%s\n' ""
}

# ---------------------------------------------------------------------------------------
# Entry point
#
# The order is not arbitrary. Everything that can be decided without touching the network is
# decided first, so that a misconfiguration fails immediately and with a named variable
# rather than as a timeout thirty seconds later. Delegation comes after validation for the
# same reason: it is better to reject a missing credential here than to hand the problem to
# a container and read the answer back out of its log.
# ---------------------------------------------------------------------------------------

usage() {
    printf '%s\n' "Usage: scripts/kafka-provision.sh [-h|--help] [--print-interface[-host]]"
    printf '%s\n' ""
    printf '%s\n' "Provisions a RUNNING Kafka broker for Blnk event streaming: the category topics,"
    printf '%s\n' "their dead-letter siblings, the producer principal Blnk publishes as, and one"
    printf '%s\n' "sample subscriber principal - each with its ACLs. Safe to run on every bring-up."
    printf '%s\n' ""
    printf '%s\n' "--print-interface emits every environment variable name this script reads, one"
    printf '%s\n' "per line, and exits without touching anything. It is the machine-readable contract"
    printf '%s\n' "./stack.sh and the compose kafka-init blocks are built from and checked against."
    printf '%s\n' "--print-interface-host adds the delegation targets a host-side invoker may set."
    printf '%s\n' ""
    printf '%s\n' "Otherwise this script takes NO arguments. Everything is"
    printf '%s\n' "configured through the environment; the header of this file documents every"
    printf '%s\n' "variable at its point of use. In summary, with defaults:"
    printf '%s\n' ""
    printf '  %-39s %s\n' "KAFKA_BROKERS" "(unset) comma-separated; declared-but-empty skips"
    printf '  %-39s %s\n' "KAFKA_BOOTSTRAP_SERVER" "(unset) overrides KAFKA_BROKERS and the skip"
    printf '  %-39s %s\n' "KAFKA_TOPIC_PREFIX" "blnk"
    printf '  %-39s %s\n' "KAFKA_MIN_PARTITIONS" "6 (a lower value is raised to 6)"
    printf '  %-39s %s\n' "KAFKA_REPLICATION_FACTOR" "1 locally, 3 in production; verified per topic"
    printf '  %-39s %s\n' "KAFKA_SASL_ADMIN_USER" "(required with the secret; no default)"
    printf '  %-39s %s\n' "KAFKA_SASL_ADMIN_SECRET" "(required with the user; no default)"
    printf '  %-39s %s\n' "KAFKA_SASL_USER" "blnk-producer (what Blnk publishes as)"
    printf '  %-39s %s\n' "KAFKA_SASL_SECRET" "(unset) generated on first run"
    printf '  %-39s %s\n' "KAFKA_ROTATE_PRODUCER_SECRET" "(unset) truthy replaces an existing password"
    printf '  %-39s %s\n' "KAFKA_SECURITY_PROTOCOL" "SASL_PLAINTEXT"
    printf '  %-39s %s\n' "KAFKA_CLIENT_CONFIG" "(unset) use this properties file verbatim"
    printf '  %-39s %s\n' "KAFKA_CLIENT_CONFIG_CONTAINER_PATH" "(unset) its path inside the broker container"
    printf '  %-39s %s\n' "KAFKA_SCRAM_ITERATIONS" "4096 (the SCRAM minimum)"
    printf '  %-39s %s\n' "KAFKA_PROVISION_TIMEOUT_SECONDS" "60"
    printf '  %-39s %s\n' "KAFKA_PROVISION_POLL_INTERVAL_SECONDS" "2"
    printf '  %-39s %s\n' "KAFKA_CLI_TIMEOUT_SECONDS" "30 (per-call bound on every CLI call)"
    printf '  %-39s %s\n' "KAFKA_CLI_KILL_GRACE_SECONDS" "5"
    printf '  %-39s %s\n' "KAFKA_SAMPLE_SUBSCRIBER_USER" "blnk-sample-subscriber"
    printf '  %-39s %s\n' "KAFKA_SAMPLE_SUBSCRIBER_SECRET" "(unset) generated on first run"
    printf '  %-39s %s\n' "KAFKA_ROTATE_SAMPLE_SUBSCRIBER_SECRET" "(unset) truthy replaces an existing password"
    printf '  %-39s %s\n' "KAFKA_SAMPLE_SUBSCRIBER_GROUP_PREFIX" "(derived) leave unset; only cross-checked"
    printf '  %-39s %s\n' "KAFKA_SAMPLE_SUBSCRIBER_TOPICS" "(the grantable category topics)"
    printf '  %-39s %s\n' "KAFKA_SKIP_SAMPLE_SUBSCRIBER" "(unset) truthy skips the principal and ACLs"
    printf '  %-39s %s\n' "KAFKA_PRODUCER_USER" "blnk-producer (what Blnk publishes as)"
    printf '  %-39s %s\n' "KAFKA_PRODUCER_SECRET" "(unset) generated on first run"
    printf '  %-39s %s\n' "KAFKA_ROTATE_PRODUCER_SECRET" "(unset) truthy replaces an existing password"
    printf '  %-39s %s\n' "KAFKA_SKIP_PRODUCER" "(unset) truthy skips validation, principal and ACLs"
    printf '  %-39s %s\n' "KAFKA_CONTAINER" "kafka"
    printf '  %-39s %s\n' "KAFKA_COMPOSE_SERVICE" "kafka"
    printf '  %-39s %s\n' "KAFKA_PROVISION_CONTAINER_SCRIPT" "/scripts/kafka-provision.sh"
    printf '%s\n' ""
}

# Emit the provisioning interface, one variable name per line, and nothing else.
#
# THIS IS THE MACHINE-READABLE CONTRACT every other invoker reads, which is why the output is
# bare names on stdout with no header, no alignment and no colour: ./stack.sh reads it into an
# array, and TestKafkaProvisionScript_HandsOffEverySupportedSetting compares the compose
# kafka-init environment blocks against it. usage() is the human form of the same list and is
# free to stay pretty.
#
# It touches nothing. parse_arguments runs before every side effect, so this reads no
# configuration, contacts no broker and provisions nothing - which is what makes it safe for
# stack.sh to call on every bring-up and for a test to call with no environment at all.
#
# Parameters:
#   $1: "--host" to include the host-only delegation targets. Omitted, only the names that
#       cross into a container are emitted.
print_interface() {
    printf '%s\n' "${KAFKA_PROVISION_INTERFACE[@]}"

    if [[ "${1:-}" == "--host" ]]; then
        printf '%s\n' "${KAFKA_PROVISION_HOST_ONLY_INTERFACE[@]}"
    fi
}

# Refuse a run that sets a variable this script no longer reads.
#
# The alternative is silence, and silence here does the OPPOSITE of what was asked. An operator
# who sets a retired skip flag is asking for something to be skipped; with the name unread, the
# thing is done instead - a principal created on a broker whose owner manages principals, or a
# validation refusal for a value that was never going to be used. Both look like a bug in this
# script rather than a stale variable name in an .env.
#
# It runs among the pure decisions, before the broker is touched, so a stale name costs nothing
# but the message. The check is on DECLARATION rather than on truthiness, deliberately: a
# retired name set to an explicit "0" or "" still records an intent that this script can no
# longer honour, and telling the operator once is cheaper than leaving it in their .env to be
# rediscovered after the next rename.
require_no_retired_variables() {
    local entry retired replacement
    for entry in "${KAFKA_PROVISION_RETIRED_VARIABLES[@]}"; do
        retired="${entry%%=*}"
        replacement="${entry#*=}"

        if [[ -n "${!retired+declared}" ]]; then
            die "${retired} is set, and this script no longer reads it." \
                "Use ${replacement} instead - it is the one flag that governs the whole of what" \
                "${retired} governed half of." \
                "There were two variables and they gated different halves of the same decision:" \
                "one skipped the producer's VALIDATION and the other skipped the broker" \
                "MUTATION, and the forwarding was split too, so no entry point could express" \
                "'skip the producer' completely." \
                "Fix: rename ${retired} to ${replacement} wherever it is set - your .env, your" \
                "shell, your CI configuration or your compose override." \
                "Refusing rather than ignoring it, because ignoring it would silently do the" \
                "opposite of what you asked for." \
                "Nothing has been provisioned."
        fi
    done
}

# Parse the command line before anything else happens.
#
# There is nothing to parse but the help flags, and that is exactly why this exists: the
# script used to accept "$@" and ignore it silently, so "kafka-provision.sh --help"
# PROVISIONED A BROKER instead of printing usage, and a mistyped flag or a stray argument
# from a makefile was swallowed without a word. Neither is acceptable in a script whose job
# is to create topics and mint a credential.
#
# It runs before every side effect, including the unconfigured-skip check, so a bad argument
# costs nothing.
parse_arguments() {
    local argument
    for argument in "$@"; do
        case "$argument" in
            -h | --help)
                usage
                exit 0
                ;;
            --print-interface)
                print_interface
                exit 0
                ;;
            --print-interface-host)
                print_interface --host
                exit 0
                ;;
            *)
                usage >&2
                die "unexpected argument '${argument}'." \
                    "This script takes no arguments other than -h/--help and the" \
                    "--print-interface pair; everything is configured through the environment," \
                    "and the usage above lists every variable." \
                    "Nothing has been provisioned."
                ;;
        esac
    done
}

main() {
    # Before anything else, including the skip check: a bad argument must cost nothing.
    parse_arguments "$@"

    log "provisioning Kafka topics, the producer and sample subscriber principals, and their ACLs"

    # Cheapest possible exit, and the common local case: no Kafka configured at all.
    skip_when_kafka_unconfigured

    # Before any decision that reads a variable: a stale name must be reported as a stale name,
    # not as whatever the unread value would have prevented.
    require_no_retired_variables

    # Pure decisions, no network.
    #
    # require_admin_credentials runs BEFORE require_valid_subscriber, and the order is load
    # bearing rather than tidy: the subscriber check refuses a sample principal that collides
    # with the ADMINISTRATIVE one (S6-06), and it can only compare against a value that has
    # already been trimmed and published. Run the other way round it would compare against the
    # raw environment string, so an administrative user carrying a trailing newline from a
    # secret store would slip past the collision test and the script would rewrite the
    # administrator's password.
    require_valid_geometry
    require_valid_iterations
    resolve_topics
    require_admin_credentials
    # Both of these run AFTER require_admin_credentials, for the reason given above and for
    # exactly one more: each refuses a principal that COLLIDES with the administrative one
    # (S6-06), and each can only compare against a value that has already been trimmed and
    # published. Run before it, they would compare against the raw environment string, so an
    # administrative user carrying a trailing newline from a secret store would slip past the
    # collision test and this script would rewrite the administrator's password.
    #
    # Each call appears exactly ONCE. Repeating them cost a second identical set of checks
    # and, worse, made the order they document unreadable.
    require_valid_producer
    # BEFORE the subscriber check, and the order is observable rather than cosmetic. A rotation
    # with nowhere to deliver the result is a configuration error about ROTATION, and it has to
    # be reported as one: run after require_valid_subscriber, a stack that asked to rotate the
    # producer's password would instead be told it had no sample-subscriber delivery channel —
    # true, unrelated, and no help at all in finding the variable that actually caused the
    # refusal. It reads only environment variables, so it has no ordering dependency of its own.
    require_rotation_destination
    resolve_subscriber_topics
    require_valid_subscriber

    log "resolved the topic catalogue from prefix '$(trim "$KAFKA_TOPIC_PREFIX")'" \
        "category    : $(join_commas "${CATEGORY_TOPICS[@]}")" \
        "dead-letter : $(join_commas "${DEAD_LETTER_TOPICS[@]}")"

    # Either the CLI is here, or the whole script moves to where it is. delegate_to_container
    # ends in an exec or a die, so nothing below it runs on the host in that case.
    if ! detect_kafka_cli; then
        if [[ -n "$BLNK_KAFKA_PROVISION_IN_CONTAINER" ]]; then
            die "the Kafka CLI tools are missing inside the container this script delegated to." \
                "Searched PATH for both the unsuffixed and the .sh forms of kafka-topics," \
                "kafka-configs and kafka-acls, then /opt/kafka/bin, /usr/bin," \
                "/opt/bitnami/kafka/bin and /usr/local/kafka/bin." \
                "Delegating again would loop, so this stops here." \
                "Fix: point KAFKA_CONTAINER at a container built from a Kafka image - the" \
                "stack uses apache/kafka - or run this script somewhere the CLI exists."
        fi
        delegate_to_container
    fi

    log "using the Kafka CLI from ${CLI_FLAVOUR}"

    # Resolved before the first call, so the bound covers every one of them including the
    # readiness probe.
    detect_cli_timeout

    prepare_client_config
    wait_for_broker

    ensure_topics
    # The producer BEFORE the sample subscriber, because it is the one the application cannot
    # start without: with an administrative pair configured and no producer pair, Blnk's event
    # publisher refuses to construct. A failure provisioning it should therefore be the first
    # thing an operator reads, not something below a successful subscriber grant.
    ensure_producer_principal
    ensure_sample_subscriber

    print_summary
}

main "$@"
